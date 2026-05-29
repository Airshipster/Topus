import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import gspread
import requests

import config
from sheets import (
    ROW_INSERT_INHERIT_BUFFER,
    TARGET_WORKSHEET_ROWS,
    authenticate_google_sheets,
    clean_sheet_value,
    format_timestamp,
    get_values_with_quota_retry,
    last_used_row,
    normalize_timestamp,
    parse_datetime_value,
)


LEGACY_SHEET_NAME = 'Бот данные'
SHEET_NAME = 'Боты'
EXTRA_SHEET_NAMES = ['Бот подписки', 'Бот free']

HEADERS = [
    'Project Code',
    'Bot',
    'User ID',
    'Username',
    'First Name',
    'Access',
    'Access From GMT+4',
    'Access Until GMT+4',
    'Role',
    'Subscription Mode',
    'Included Channel IDs',
    'Excluded Channel IDs',
    'Subscribed Count',
    'Total Channels',
    'Free Note',
    'Cloudflare Updated At',
    'Sheet Synced At',
    'Sync Action',
    'Access History',
]

DEPRECATED_HEADERS = {
    'Payment Method',
    'Boost Count',
    'Boost Checked At GMT+4',
    'Boost Expires At GMT+4',
    'Hide Inactive >1y',
}

ROLE_VALUES = {'admin', 'user'}
SUBSCRIPTION_MODE_VALUES = {'all', 'custom', 'все', 'всё'}
INVALID_ACCESS_UNTIL_VALUES = ROLE_VALUES | SUBSCRIPTION_MODE_VALUES
TRUE_VALUES = {'1', 'true', 'yes', 'y', 'да', 'истина', '✅', 'on', 'active', 'free', 'paid'}
FALSE_VALUES = {'0', 'false', 'no', 'n', 'нет', 'ложь', '❌', 'off', 'inactive', 'none'}
ACCESS_VALUES = {'free', 'paid', 'none', 'trial', 'booster'}
CLOUDFLARE_MONTHLY_REQUEST_LIMIT = 100000
CLOUDFLARE_GRAPHQL_URL = 'https://api.cloudflare.com/client/v4/graphql'
DEFAULT_CLOUDFLARE_ACCOUNT_ID = '8460cfa72309d5c869775d6c38ca41dd'
DEFAULT_CLOUDFLARE_WORKER_SCRIPT = 'topus-telegram-subscriptions'
DEFAULT_BOT_STATUS_COLUMN_INDEX = len(HEADERS) + 1


def bool_from_sheet(value, default=False):
    text = str(clean_sheet_value(value) or '').strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    return default


def bool_to_sheet(value):
    return 'TRUE' if bool(value) else 'FALSE'


def split_ids(value):
    text = str(clean_sheet_value(value) or '').strip()
    if not text or text.lower() in {'all', 'все', 'всё'}:
        return set()
    return {item.strip() for item in text.replace('\n', ',').split(',') if item.strip()}


def join_ids(values):
    return ', '.join(sorted(values))


def max_timestamp(*values):
    filtered = [str(value or '').strip() for value in values if str(value or '').strip()]
    return max(filtered) if filtered else ''


def display_timestamp(value):
    return normalize_timestamp(value) if value else ''


def display_access_until(value):
    parsed = parse_iso_datetime(value)
    if not parsed:
        return display_timestamp(value)
    return format_timestamp(parsed.astimezone(ZoneInfo('Asia/Baku')))


def parse_iso_datetime(value):
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00'))
    except ValueError:
        return None


def is_future(value):
    parsed = parse_iso_datetime(value)
    return bool(parsed and parsed > datetime.now(timezone.utc))


def add_months(start, months):
    month = start.month - 1 + months
    year = start.year + month // 12
    month = month % 12 + 1
    days = [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return start.replace(year=year, month=month, day=min(start.day, days[month - 1]))


def parse_access_until(value, existing_value=''):
    text = str(clean_sheet_value(value) or '').strip()
    if not text:
        return existing_value or ''
    if text.lower() in INVALID_ACCESS_UNTIL_VALUES:
        return existing_value or ''
    if text.lower() in {'forever', 'permanent', 'навсегда', 'вечный', 'вечно'}:
        return ''
    text = re.sub(r'^(?:до|until|till)\s+', '', text, flags=re.IGNORECASE).strip()
    if re.fullmatch(r'(?:[1-9]|1[0-2])', text):
        return add_months(datetime.now(timezone.utc), int(text)).isoformat().replace('+00:00', 'Z')
    parsed = parse_datetime_value(text)
    if parsed:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    return text


def parse_access_spec(access_value, access_until_value='', existing_expiry=''):
    text = str(clean_sheet_value(access_value) or '').strip().lower()
    if not text:
        return '', ''
    parts = [part.strip() for part in re.split(r'[,;]', text, maxsplit=1)]
    access = normalize_access(parts[0])
    inline_until = parts[1] if len(parts) > 1 else ''
    if access == 'trial':
        access = 'free'
        if not inline_until and not access_until_value:
            inline_until = '1'
    if access == 'booster':
        return access, ''
    if access == 'paid':
        expiry_source = inline_until or access_until_value
        return access, parse_access_until(expiry_source, existing_expiry)
    if access != 'free':
        return access, ''
    expiry_source = inline_until or access_until_value
    return access, parse_access_until(expiry_source, existing_expiry)


def access_kind(value):
    text = str(value or '').strip().lower()
    return normalize_access(re.split(r'[,;]', text, maxsplit=1)[0].strip())


def bot_display(project):
    return str(project.get('bot_username') or project.get('name') or project.get('code') or '').strip()


def worker_bool(record, key):
    return bool(int(record.get(key) or 0))


def row_key(project_code, user_id):
    return f'{project_code}:{user_id}'


def get_cell(row, headers, name):
    index = headers.get(name)
    if index is None or index >= len(row):
        return ''
    return clean_sheet_value(row[index])


def get_first_cell(row, headers, names):
    for name in names:
        value = get_cell(row, headers, name)
        if value:
            return value
    return ''


def a1_column(column_index):
    return re.sub(r'\d+', '', gspread.utils.rowcol_to_a1(1, column_index))


def find_bot_status_column(worksheet):
    values = get_values_with_quota_retry(worksheet, '1:2')
    first_row = values[0] if values else []
    second_row = values[1] if len(values) > 1 else []
    max_columns = max(worksheet.col_count, len(first_row), len(second_row))
    start_column = len(HEADERS) + 1

    for column in range(start_column, max_columns + 1):
        top = str(first_row[column - 1] if column <= len(first_row) else '').strip()
        bottom = str(second_row[column - 1] if column <= len(second_row) else '').strip()
        combined = f'{top}\n{bottom}'
        if re.search(r'Cloudflare sync|Bot Cloudflare sync|remaining|source:', combined, re.IGNORECASE):
            return column

    for column in range(start_column, max_columns + 1):
        top = str(first_row[column - 1] if column <= len(first_row) else '').strip()
        bottom = str(second_row[column - 1] if column <= len(second_row) else '').strip()
        if not top and not bottom:
            return column

    target_column = max(max_columns + 1, start_column)
    if worksheet.col_count < target_column:
        worksheet.add_cols(target_column - worksheet.col_count)
    return target_column


def write_bot_sync_status(worksheet, top_text='', bottom_text=''):
    column = find_bot_status_column(worksheet)
    column_letter = a1_column(column)
    current = get_values_with_quota_retry(worksheet, f'{column_letter}1:{column_letter}2')
    current_top = current[0][0] if current and current[0] else ''
    current_bottom = current[1][0] if len(current) > 1 and current[1] else ''
    worksheet.update(
        range_name=f'{column_letter}1:{column_letter}2',
        values=[[top_text or current_top], [bottom_text or current_bottom]],
        value_input_option='USER_ENTERED',
    )


def cloudflare_status_text(usage):
    if not usage:
        return 'Cloudflare sync OK: unknown usage'
    month = usage.get('month') or datetime.now(timezone.utc).strftime('%Y-%m')
    remaining = usage.get('remaining')
    source = usage.get('source') or 'unknown'
    return '\n'.join([
        f'Cloudflare sync OK: {format_timestamp()}',
        f'remaining {month}: {remaining}',
        f'source: {source}',
    ])


def rename_legacy_sheet(sheet):
    try:
        return sheet.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        pass

    try:
        worksheet = sheet.worksheet(LEGACY_SHEET_NAME)
        worksheet.update_title(SHEET_NAME)
        return worksheet
    except gspread.exceptions.WorksheetNotFound:
        return sheet.add_worksheet(SHEET_NAME, rows=TARGET_WORKSHEET_ROWS, cols=len(HEADERS))


def delete_extra_sheets(sheet):
    return


def ensure_bot_worksheet(sheet):
    worksheet = rename_legacy_sheet(sheet)
    delete_extra_sheets(sheet)
    ensure_bot_row_count(sheet, worksheet)
    values = get_values_with_quota_retry(worksheet, '1:1')
    current_headers = [str(cell).strip() for cell in values[0]] if values else []
    if current_headers != HEADERS:
        migrate_bot_sheet_columns(worksheet, current_headers)
        values = get_values_with_quota_retry(worksheet, '1:1')
        current_headers = [str(cell).strip() for cell in values[0]] if values else []
    worksheet = delete_deprecated_bot_columns(sheet, worksheet, current_headers)
    values = get_values_with_quota_retry(worksheet, '1:1')
    current_headers = [str(cell).strip() for cell in values[0]] if values else []
    if current_headers != HEADERS:
        migrate_bot_sheet_columns(worksheet, current_headers)
    return worksheet


def migrate_bot_sheet_columns(worksheet, current_headers):
    values = get_values_with_quota_retry(worksheet)
    if not values:
        worksheet.update(range_name='A1', values=[HEADERS], value_input_option='USER_ENTERED')
        return

    source_headers = [str(cell).strip() for cell in values[0]]
    source_indexes = {
        header: index
        for index, header in enumerate(source_headers)
        if header
    }
    source_indexes.setdefault('Role', source_indexes.get('Status'))
    source_indexes.setdefault('Access Until GMT+4', source_indexes.get('Access Until'))

    migrated_rows = []
    for raw_row in values[1:]:
        migrated_rows.append([
            clean_sheet_value(raw_row[source_indexes[header]])
            if header in source_indexes and source_indexes[header] is not None and source_indexes[header] < len(raw_row)
            else ''
            for header in HEADERS
        ])

    payload = [HEADERS] + migrated_rows
    last_col = a1_column(len(HEADERS))
    worksheet.update(
        range_name=f'A1:{last_col}{len(payload)}',
        values=payload,
        value_input_option='USER_ENTERED',
    )
    if worksheet.col_count > len(HEADERS):
        worksheet.batch_clear([f'{a1_column(len(HEADERS) + 1)}1:{a1_column(worksheet.col_count)}{worksheet.row_count}'])
    print('  🧭 Migrated bot sheet columns by header names')


def delete_deprecated_bot_columns(sheet, worksheet, headers):
    indexes = [
        index + 1
        for index, header in enumerate(headers)
        if str(header).strip() in DEPRECATED_HEADERS
    ]
    if not indexes:
        return worksheet
    requests = []
    for column_index in sorted(indexes, reverse=True):
        requests.append({
            'deleteDimension': {
                'range': {
                    'sheetId': worksheet.id,
                    'dimension': 'COLUMNS',
                    'startIndex': column_index - 1,
                    'endIndex': column_index,
                }
            }
        })
    sheet.batch_update({'requests': requests})
    print(f"  🧹 Removed deprecated bot columns: {len(indexes)}")
    return sheet.worksheet(SHEET_NAME)


def ensure_bot_row_count(sheet, worksheet):
    current_rows = worksheet.row_count
    try:
        used_rows = last_used_row(get_values_with_quota_retry(worksheet))
    except Exception:
        used_rows = current_rows
    target_rows = max(TARGET_WORKSHEET_ROWS, used_rows)
    if current_rows == target_rows:
        return

    if current_rows < target_rows:
        start_index = max(1, current_rows - ROW_INSERT_INHERIT_BUFFER)
        request = {
            'insertDimension': {
                'range': {
                    'sheetId': worksheet.id,
                    'dimension': 'ROWS',
                    'startIndex': start_index,
                    'endIndex': start_index + (target_rows - current_rows),
                },
                'inheritFromBefore': True,
            }
        }
    else:
        request = {
            'deleteDimension': {
                'range': {
                    'sheetId': worksheet.id,
                    'dimension': 'ROWS',
                    'startIndex': target_rows,
                    'endIndex': current_rows,
                }
            }
        }
    sheet.batch_update({'requests': [request]})
    print(f"  📐 Normalized bot sheet row count: {target_rows}")


def request_worker(method, path, worker_url, admin_secret, **kwargs):
    response = requests.request(
        method,
        worker_url.rstrip('/') + path,
        headers={'x-admin-secret': admin_secret},
        timeout=45,
        **kwargs,
    )
    response.raise_for_status()
    return response.json()


def fetch_worker_state(worker_url, admin_secret):
    state = request_worker('GET', '/admin/sheet-state', worker_url, admin_secret)
    if not state.get('ok'):
        raise RuntimeError(f'Worker state export failed: {state}')
    return state


def month_bounds_utc(now=None):
    now = now or datetime.now(timezone.utc)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start.isoformat().replace('+00:00', 'Z'), now.isoformat().replace('+00:00', 'Z')


def fetch_cloudflare_analytics_usage():
    token = os.environ.get('CLOUDFLARE_API_TOKEN', '').strip()
    account_id = os.environ.get('CLOUDFLARE_ACCOUNT_ID', DEFAULT_CLOUDFLARE_ACCOUNT_ID).strip()
    script_name = os.environ.get('CLOUDFLARE_WORKER_SCRIPT_NAME', DEFAULT_CLOUDFLARE_WORKER_SCRIPT).strip()
    if not token:
        return None

    datetime_start, datetime_end = month_bounds_utc()
    query = """
      query GetWorkersAnalytics(
        $accountTag: string,
        $datetimeStart: string,
        $datetimeEnd: string,
        $scriptName: string
      ) {
        viewer {
          accounts(filter: {accountTag: $accountTag}) {
            workersInvocationsAdaptive(limit: 100, filter: {
              scriptName: $scriptName,
              datetime_geq: $datetimeStart,
              datetime_leq: $datetimeEnd
            }) {
              sum {
                requests
              }
            }
          }
        }
      }
    """
    response = requests.post(
        CLOUDFLARE_GRAPHQL_URL,
        headers={
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        },
        json={
            'query': query,
            'variables': {
                'accountTag': account_id,
                'datetimeStart': datetime_start,
                'datetimeEnd': datetime_end,
                'scriptName': script_name,
            },
        },
        timeout=45,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get('errors'):
        raise RuntimeError(f"Cloudflare Analytics query failed: {payload.get('errors')}")

    accounts = (((payload.get('data') or {}).get('viewer') or {}).get('accounts') or [])
    rows = accounts[0].get('workersInvocationsAdaptive') if accounts else []
    used = sum(int((row.get('sum') or {}).get('requests') or 0) for row in (rows or []))
    limit = CLOUDFLARE_MONTHLY_REQUEST_LIMIT
    month = datetime_start[:7]
    return {
        'limit': limit,
        'used': used,
        'remaining': max(0, limit - used),
        'month': month,
        'source': 'Cloudflare Analytics',
    }


def resolve_usage(worker_usage):
    analytics_usage = fetch_cloudflare_analytics_usage()
    if analytics_usage:
        return analytics_usage
    usage = dict(worker_usage or {})
    usage.setdefault('limit', CLOUDFLARE_MONTHLY_REQUEST_LIMIT)
    usage.setdefault('source', 'Worker counter since deploy')
    return usage


def build_state(state):
    projects = {}
    users = {}
    allowlist = {}
    channels_by_project = defaultdict(list)
    subscription_rows = defaultdict(dict)
    subscription_updated_at = defaultdict(str)

    for project in state.get('projects') or []:
        code = str(project.get('code') or '').strip()
        if code:
            projects[code] = {
                'code': code,
                'name': str(project.get('name') or code).strip(),
                'bot_username': str(project.get('bot_username') or '').strip(),
            }

    for channel in state.get('channels') or []:
        project_code = str(channel.get('project_code') or '').strip()
        channel_id = str(channel.get('channel_id') or '').strip()
        if project_code and channel_id:
            channels_by_project[project_code].append(channel_id)

    for user in state.get('users') or []:
        project_code = str(user.get('project_code') or '').strip()
        user_id = str(user.get('user_id') or '').strip()
        if not project_code or not user_id:
            continue
        users[row_key(project_code, user_id)] = {
            'project_code': project_code,
            'user_id': user_id,
            'username': user.get('username') or '',
            'first_name': user.get('first_name') or '',
            'is_paid': worker_bool(user, 'is_paid'),
            'is_allowlisted': worker_bool(user, 'is_allowlisted'),
            'is_admin': worker_bool(user, 'is_admin'),
            'access_expires_at': user.get('access_expires_at') or '',
            'access_source': user.get('access_source') or '',
            'payment_method': user.get('payment_method') or '',
            'boost_count': int(user.get('boost_count') or 0),
            'boost_checked_at': user.get('boost_checked_at') or '',
            'boost_expires_at': user.get('boost_expires_at') or '',
            'star_paid_until': user.get('star_paid_until') or '',
            'access_started_at': user.get('access_started_at') or '',
            'access_history': user.get('access_history') or '',
            'hide_inactive_year': worker_bool(user, 'hide_inactive_year'),
            'updated_at': user.get('updated_at') or user.get('created_at') or '',
        }

    for entry in state.get('allowlist') or []:
        project_code = str(entry.get('project_code') or '').strip()
        user_id = str(entry.get('user_id') or '').strip()
        if not project_code or not user_id:
            continue
        allowlist[row_key(project_code, user_id)] = {
            'note': entry.get('note') or '',
            'updated_at': entry.get('updated_at') or entry.get('created_at') or '',
        }

    for subscription in state.get('subscriptions') or []:
        project_code = str(subscription.get('project_code') or '').strip()
        user_id = str(subscription.get('user_id') or '').strip()
        channel_id = str(subscription.get('channel_id') or '').strip()
        if not project_code or not user_id or not channel_id:
            continue
        key = row_key(project_code, user_id)
        subscription_rows[key][channel_id] = worker_bool(subscription, 'active')
        subscription_updated_at[key] = max_timestamp(
            subscription_updated_at[key],
            subscription.get('updated_at') or subscription.get('created_at') or '',
        )
        users.setdefault(key, {
            'project_code': project_code,
            'user_id': user_id,
            'username': '',
            'first_name': '',
            'is_paid': False,
            'is_allowlisted': False,
            'is_admin': False,
            'access_expires_at': '',
            'access_started_at': '',
            'access_history': '',
            'updated_at': '',
        })

    return {
        'projects': projects,
        'users': users,
        'allowlist': allowlist,
        'channels_by_project': dict(channels_by_project),
        'subscription_rows': dict(subscription_rows),
        'subscription_updated_at': dict(subscription_updated_at),
        'usage': state.get('usage') or {},
    }


def read_sheet_rows(worksheet):
    values = get_values_with_quota_retry(worksheet)
    if not values:
        return {'rows': [], 'order': [], 'by_key': {}}
    headers = {str(cell).strip(): index for index, cell in enumerate(values[0]) if str(cell).strip()}
    rows = []
    order = []
    by_key = {}
    for raw_row in values[1:]:
        project_code = str(get_cell(raw_row, headers, 'Project Code') or '').strip()
        user_id = str(get_cell(raw_row, headers, 'User ID') or '').strip()
        if not project_code or not user_id:
            continue
        key = row_key(project_code, user_id)
        row = {
            'key': key,
            'project_code': project_code,
            'user_id': user_id,
            'username': str(get_cell(raw_row, headers, 'Username') or '').strip(),
            'first_name': str(get_cell(raw_row, headers, 'First Name') or '').strip(),
            'access': str(get_cell(raw_row, headers, 'Access') or '').strip().lower(),
            'access_from': str(get_first_cell(raw_row, headers, ['Access From GMT+4', 'Access From']) or '').strip(),
            'access_until': str(get_first_cell(raw_row, headers, ['Access Until GMT+4', 'Access Until']) or '').strip(),
            'role': str(get_first_cell(raw_row, headers, ['Role', 'Status']) or '').strip().lower(),
            'mode': str(get_cell(raw_row, headers, 'Subscription Mode') or '').strip().lower(),
            'included': get_cell(raw_row, headers, 'Included Channel IDs'),
            'excluded': get_cell(raw_row, headers, 'Excluded Channel IDs'),
            'free_note': str(get_cell(raw_row, headers, 'Free Note') or '').strip(),
            'access_history': str(get_cell(raw_row, headers, 'Access History') or '').strip(),
            'action': str(get_cell(raw_row, headers, 'Sync Action') or '').strip().lower(),
        }
        rows.append(row)
        if key not in by_key:
            order.append(key)
            by_key[key] = row
    return {'rows': rows, 'order': order, 'by_key': by_key}


def bot_sheet_rows_look_corrupted(sheet_rows):
    suspicious = 0
    for row in sheet_rows['rows']:
        access_until = str(row.get('access_until') or '').strip().lower()
        role = str(row.get('role') or '').strip().lower()
        mode = str(row.get('mode') or '').strip().lower()
        if access_until in ROLE_VALUES or role in SUBSCRIPTION_MODE_VALUES or mode.startswith('uc'):
            suspicious += 1
        if suspicious >= 3:
            return True
    return False


def read_action_rows(sheet_rows, compact_state):
    users = compact_state['users']
    allowlist = compact_state['allowlist']
    rows = []
    for row in sheet_rows['rows']:
        action = row['action']
        project_code = row['project_code']
        user_id = row['user_id']
        access = row['access']
        key = row_key(project_code, user_id)
        existing_access = user_access(users.get(key, {}), allowlist.get(key))
        existing_access_kind = access_kind(existing_access)
        existing_role = user_role(users.get(key, {}))
        existing_expiry = str(users.get(key, {}).get('access_expires_at') or '').strip()
        requested_access, requested_expiry = parse_access_spec(access, row.get('access_until'), existing_expiry)
        requested_role = normalize_role(row.get('role'))
        is_new_free_user = key not in users and requested_access == 'free'
        access_changed = requested_access in ACCESS_VALUES and requested_access != existing_access_kind
        expiry_changed = requested_access == 'free' and requested_expiry != existing_expiry
        role_changed = requested_role != existing_role
        if action not in {'push', 'delete'} and not is_new_free_user and not access_changed and not expiry_changed and not role_changed:
            continue
        if (is_new_free_user or access_changed or expiry_changed or role_changed) and action not in {'push', 'delete'}:
            action = 'push'
        action_row = dict(row)
        action_row['action'] = action
        action_row['access'] = requested_access
        action_row['access_until'] = requested_expiry
        rows.append(action_row)
    return rows


def desired_subscriptions(row, all_channel_ids):
    mode = row['mode'] or 'custom'
    included = split_ids(row['included'])
    excluded = split_ids(row['excluded'])
    all_channels = set(all_channel_ids)
    if mode in {'all', 'все', 'всё'}:
        return all_channels - excluded
    return included & all_channels


def collect_changes(action_rows, compact_state):
    payload = {'users': [], 'subscriptions': [], 'allowlist': []}
    users = compact_state['users']
    allowlist = compact_state['allowlist']
    channels_by_project = compact_state['channels_by_project']
    current_sub_rows = compact_state['subscription_rows']

    for row in action_rows:
        project_code = row['project_code']
        user_id = row['user_id']
        key = row_key(project_code, user_id)
        existing_user = users.get(key, {})

        if row['action'] == 'delete':
            desired = set()
            payload['allowlist'].append({'projectCode': project_code, 'userId': user_id, 'delete': True})
            is_paid = False
            is_allowlisted = False
            is_admin = False
            access_expires_at = None
        else:
            desired = desired_subscriptions(row, channels_by_project.get(project_code, []))
            existing_access = user_access(existing_user, allowlist.get(key))
            requested_access, requested_expiry = parse_access_spec(row['access'], row.get('access_until'), existing_user.get('access_expires_at') or '')
            requested_access = requested_access or existing_access
            requested_role = normalize_role(row.get('role'))
            access_expires_at = requested_expiry if requested_access in {'free', 'paid'} and requested_expiry else None
            is_paid = requested_access == 'paid'
            is_allowlisted = requested_access == 'free'
            is_admin = requested_role == 'admin'
            if requested_access == 'free' and not access_expires_at:
                payload['allowlist'].append({
                    'projectCode': project_code,
                    'userId': user_id,
                    'note': row['free_note'],
                    'active': True,
                })
            elif key in allowlist:
                payload['allowlist'].append({'projectCode': project_code, 'userId': user_id, 'delete': True})

        payload['users'].append({
            'projectCode': project_code,
            'userId': user_id,
            'username': row['username'] or existing_user.get('username') or None,
            'firstName': row['first_name'] or existing_user.get('first_name') or None,
            'isPaid': is_paid,
            'isAllowlisted': is_allowlisted,
            'isAdmin': is_admin,
            'accessExpiresAt': access_expires_at,
            'accessSource': requested_access if requested_access in {'free', 'paid', 'booster', 'none'} else None,
            'paymentMethod': existing_user.get('payment_method') or ('boost' if requested_access == 'booster' else 'manual' if requested_access in {'free', 'paid'} else None),
            'boostCount': 0 if requested_access != 'booster' else int(existing_user.get('boost_count') or 0),
            'boostCheckedAt': existing_user.get('boost_checked_at') or None,
            'boostExpiresAt': existing_user.get('boost_expires_at') or None,
            'starPaidUntil': existing_user.get('star_paid_until') or None,
            'accessStartedAt': existing_user.get('access_started_at') or None,
            'accessHistory': row.get('access_history') or existing_user.get('access_history') or None,
            'hideInactiveYear': bool(existing_user.get('hide_inactive_year')),
        })

        current = {channel_id for channel_id, active in current_sub_rows.get(key, {}).items() if active}
        for channel_id in sorted(current | desired):
            if (channel_id in current) == (channel_id in desired):
                continue
            payload['subscriptions'].append({
                'projectCode': project_code,
                'userId': user_id,
                'channelId': channel_id,
                'active': channel_id in desired,
            })

    return {key: value for key, value in payload.items() if value}


def user_access(user, allowlist_entry):
    access_source = str(user.get('access_source') or '').strip().lower()
    if access_source == 'booster' and int(user.get('boost_count') or 0) >= 3:
        return 'booster'
    if allowlist_entry:
        return 'free'
    if user.get('is_allowlisted'):
        expires_at = user.get('access_expires_at') or ''
        if not expires_at:
            return 'free'
        if is_future(expires_at):
            return 'free'
        return 'none'
    if user.get('is_paid'):
        expires_at = user.get('access_expires_at') or ''
        if not expires_at or is_future(expires_at):
            return 'paid'
        return 'none'
    return 'none'


def normalize_access(value):
    text = str(value or '').strip().lower()
    if text in {'пробный', 'trial', 'test'}:
        return 'trial'
    if text in ACCESS_VALUES:
        return text
    return ''


def normalize_role(value):
    text = str(value or '').strip().lower()
    return 'admin' if text == 'admin' else 'user'


def user_role(user):
    return 'admin' if user.get('is_admin') else 'user'


def compact_subscription_cells(all_channel_ids, current_subscriptions):
    all_channels = set(all_channel_ids)
    selected = {channel_id for channel_id, active in current_subscriptions.items() if active and channel_id in all_channels}
    if all_channels and selected == all_channels:
        return 'all', 'all', '', len(selected), len(all_channels)
    if all_channels and len(selected) > len(all_channels) / 2:
        return 'all', 'all', join_ids(all_channels - selected), len(selected), len(all_channels)
    return 'custom', join_ids(selected), '', len(selected), len(all_channels)


def compact_access_history(user, allowlist_entry, existing_row):
    existing = str(existing_row.get('access_history') or '').strip()
    entries = []
    source = str(user.get('access_source') or '').strip()
    method = str(user.get('payment_method') or '').strip()
    boost_count = int(user.get('boost_count') or 0)
    boost_checked = display_timestamp(user.get('boost_checked_at') or '')
    boost_expires = display_timestamp(user.get('boost_expires_at') or '')
    star_until = display_timestamp(user.get('star_paid_until') or '')
    if source or method:
        entries.append(f'access={source or user_access(user, allowlist_entry)} method={method or "manual"}')
    if boost_count or boost_checked or boost_expires:
        entries.append(f'boosts={boost_count} checked={boost_checked or "-"} expires={boost_expires or "-"}')
    if star_until:
        entries.append(f'stars_until={star_until}')
    if allowlist_entry and allowlist_entry.get('note'):
        entries.append(f'free_note={allowlist_entry.get("note")}')
    summary = ' | '.join(item for item in entries if item)
    if not existing:
        return summary
    if not summary or summary in existing:
        return existing[-500:]
    return f'{summary}\n{existing}'[-500:]


def write_rows(worksheet, rows):
    existing_values = get_values_with_quota_retry(worksheet)
    payload = [HEADERS] + rows
    last_col = a1_column(len(HEADERS))
    worksheet.update(
        range_name=f'A1:{last_col}{len(payload)}',
        values=payload,
        value_input_option='USER_ENTERED',
    )
    if len(existing_values) > len(payload):
        worksheet.batch_clear([f'A{len(payload) + 1}:{last_col}{len(existing_values)}'])


def write_single_sheet(worksheet, compact_state, sheet_rows):
    sync_time = format_timestamp()
    projects = compact_state['projects']
    users = compact_state['users']
    allowlist = compact_state['allowlist']
    channels_by_project = compact_state['channels_by_project']
    subscription_rows = compact_state['subscription_rows']
    subscription_updated_at = compact_state['subscription_updated_at']

    rows = []
    all_user_keys = set(users) | set(subscription_rows) | set(allowlist)
    ordered_keys = [key for key in sheet_rows['order'] if key in all_user_keys]
    ordered_keys.extend(sorted(all_user_keys - set(ordered_keys)))
    existing_by_key = sheet_rows['by_key']

    for key in ordered_keys:
        project_code, user_id = key.split(':', 1)
        existing_row = existing_by_key.get(key, {})
        user = users.get(key, {
            'username': '',
            'first_name': '',
            'is_paid': False,
            'is_allowlisted': False,
            'is_admin': False,
            'access_expires_at': '',
            'access_started_at': '',
            'access_history': '',
            'updated_at': '',
        })
        allowlist_entry = allowlist.get(key)
        mode, included, excluded, selected_count, total_count = compact_subscription_cells(
            channels_by_project.get(project_code, []),
            subscription_rows.get(key, {}),
        )
        updated_at = max_timestamp(
            user.get('updated_at'),
            allowlist_entry.get('updated_at') if allowlist_entry else '',
            subscription_updated_at.get(key, ''),
        )
        rows.append([
            project_code,
            bot_display(projects.get(project_code, {'code': project_code})),
            user_id,
            user.get('username') or existing_row.get('username', ''),
            user.get('first_name') or existing_row.get('first_name', ''),
            user_access(user, allowlist_entry),
            display_timestamp(user.get('access_started_at') or existing_row.get('access_from', '')),
            display_access_until(user.get('access_expires_at') or ''),
            user_role(user),
            mode,
            included,
            excluded,
            selected_count,
            total_count if selected_count else '',
            allowlist_entry.get('note', '') if allowlist_entry else existing_row.get('free_note', ''),
            display_timestamp(updated_at),
            sync_time,
            '',
            compact_access_history(user, allowlist_entry, existing_row),
        ])

    write_rows(worksheet, rows)


def main():
    worker_url = os.environ.get('TOPUS_WORKER_URL', '').strip()
    admin_secret = os.environ.get('TOPUS_WORKER_ADMIN_SECRET', '').strip()
    if not worker_url:
        raise ValueError('TOPUS_WORKER_URL is required')
    if not admin_secret:
        raise ValueError('TOPUS_WORKER_ADMIN_SECRET is required')

    client = authenticate_google_sheets()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = ensure_bot_worksheet(sheet)
    write_bot_sync_status(
        worksheet,
        bottom_text=f'Bot Cloudflare sync running in GitHub Actions: {format_timestamp()}',
    )
    state = fetch_worker_state(worker_url, admin_secret)
    compact_state = build_state(state)
    sheet_rows = read_sheet_rows(worksheet)
    if bot_sheet_rows_look_corrupted(sheet_rows):
        print('  ⚠️  Bot sheet rows look column-shifted; skipping sheet-to-Worker writes and rewriting from Worker state')
        changes = {}
    else:
        changes = collect_changes(read_action_rows(sheet_rows, compact_state), compact_state)

    applied_count = 0
    if changes:
        result = request_worker('POST', '/admin/sheet-state', worker_url, admin_secret, json=changes)
        applied_count = int(result.get('applied') or 0)
        print(f'  Applied bot sheet changes to Worker: {result}')
        state = fetch_worker_state(worker_url, admin_secret)
        compact_state = build_state(state)
        sheet_rows = read_sheet_rows(worksheet)
    else:
        print('  No bot sheet changes to push')

    write_single_sheet(worksheet, compact_state, sheet_rows)
    usage = resolve_usage(compact_state.get('usage'))
    write_bot_sync_status(
        worksheet,
        top_text=cloudflare_status_text(usage),
        bottom_text=f'Bot Cloudflare sync finished: {format_timestamp()}; applied={applied_count}',
    )
    print(f"  Synced one-sheet bot state: {len(compact_state['users'])} users")


if __name__ == '__main__':
    main()
