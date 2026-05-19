import os
import re
from collections import defaultdict
from datetime import datetime, timezone

import gspread
import requests

import config
from sheets import authenticate_google_sheets, clean_sheet_value, format_timestamp, get_values_with_quota_retry, normalize_timestamp


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
    'Subscription Mode',
    'Included Channel IDs',
    'Excluded Channel IDs',
    'Subscribed Count',
    'Total Channels',
    'Free Note',
    'Cloudflare Updated At',
    'Sheet Synced At',
    'Sync Action',
]

TRUE_VALUES = {'1', 'true', 'yes', 'y', 'да', 'истина', '✅', 'on', 'active', 'free', 'paid'}
FALSE_VALUES = {'0', 'false', 'no', 'n', 'нет', 'ложь', '❌', 'off', 'inactive', 'none'}
CLOUDFLARE_MONTHLY_REQUEST_LIMIT = 100000
CLOUDFLARE_GRAPHQL_URL = 'https://api.cloudflare.com/client/v4/graphql'
DEFAULT_CLOUDFLARE_ACCOUNT_ID = '8460cfa72309d5c869775d6c38ca41dd'
DEFAULT_CLOUDFLARE_WORKER_SCRIPT = 'topus-telegram-subscriptions'


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


def a1_column(column_index):
    return re.sub(r'\d+', '', gspread.utils.rowcol_to_a1(1, column_index))


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
        return sheet.add_worksheet(SHEET_NAME, rows=1000, cols=len(HEADERS))


def delete_extra_sheets(sheet):
    return


def ensure_bot_worksheet(sheet):
    worksheet = rename_legacy_sheet(sheet)
    delete_extra_sheets(sheet)
    values = get_values_with_quota_retry(worksheet, '1:1')
    current_headers = [str(cell).strip() for cell in values[0]] if values else []
    if current_headers != HEADERS:
        worksheet.update(range_name='A1', values=[HEADERS], value_input_option='USER_ENTERED')
    return worksheet


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
            'mode': str(get_cell(raw_row, headers, 'Subscription Mode') or '').strip().lower(),
            'included': get_cell(raw_row, headers, 'Included Channel IDs'),
            'excluded': get_cell(raw_row, headers, 'Excluded Channel IDs'),
            'free_note': str(get_cell(raw_row, headers, 'Free Note') or '').strip(),
            'action': str(get_cell(raw_row, headers, 'Sync Action') or '').strip().lower(),
        }
        rows.append(row)
        if key not in by_key:
            order.append(key)
            by_key[key] = row
    return {'rows': rows, 'order': order, 'by_key': by_key}


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
        is_new_free_user = key not in users and access == 'free'
        access_changed = access == 'free' and existing_access != 'free'
        if action not in {'push', 'delete'} and not is_new_free_user and not access_changed:
            continue
        if (is_new_free_user or access_changed) and action not in {'push', 'delete'}:
            action = 'push'
        action_row = dict(row)
        action_row['action'] = action
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
        else:
            desired = desired_subscriptions(row, channels_by_project.get(project_code, []))
            existing_access = user_access(existing_user, allowlist.get(key))
            requested_access = row['access'] or existing_access
            is_paid = requested_access == 'paid'
            is_allowlisted = requested_access == 'free'
            if is_allowlisted:
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
    if user.get('is_allowlisted') or allowlist_entry:
        return 'free'
    if user.get('is_paid'):
        return 'paid'
    return 'none'


def compact_subscription_cells(all_channel_ids, current_subscriptions):
    all_channels = set(all_channel_ids)
    selected = {channel_id for channel_id, active in current_subscriptions.items() if active and channel_id in all_channels}
    if all_channels and selected == all_channels:
        return 'all', 'all', '', len(selected), len(all_channels)
    if all_channels and len(selected) > len(all_channels) / 2:
        return 'all', 'all', join_ids(all_channels - selected), len(selected), len(all_channels)
    return 'custom', join_ids(selected), '', len(selected), len(all_channels)


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


def write_cloudflare_status(worksheet, user_count, applied_count, usage):
    limit = usage.get('limit') or CLOUDFLARE_MONTHLY_REQUEST_LIMIT
    used = usage.get('used') or 0
    remaining = usage.get('remaining')
    if remaining is None:
        remaining = max(0, int(limit) - int(used))
    month = usage.get('month') or ''
    source = usage.get('source') or 'unknown'
    status = (
        f"Cloudflare sync OK: {format_timestamp()}; users={user_count}; "
        f"applied={applied_count}; requests {month}: {used}/{limit}; "
        f"remaining={remaining}; source={source}"
    )
    worksheet.update(range_name='S1', values=[[status]], value_input_option='USER_ENTERED')


def write_operation_status(worksheet, status):
    worksheet.update(range_name='S2', values=[[status]], value_input_option='USER_ENTERED')


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
            mode,
            included,
            excluded,
            selected_count,
            total_count if selected_count else '',
            allowlist_entry.get('note', '') if allowlist_entry else existing_row.get('free_note', ''),
            display_timestamp(updated_at),
            sync_time,
            '',
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
    write_operation_status(worksheet, f"Bot Cloudflare sync running in GitHub Actions: {format_timestamp()}")

    state = fetch_worker_state(worker_url, admin_secret)
    compact_state = build_state(state)
    sheet_rows = read_sheet_rows(worksheet)
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
    usage = resolve_usage(compact_state.get('usage') or {})
    write_cloudflare_status(worksheet, len(compact_state['users']), applied_count, usage)
    write_operation_status(
        worksheet,
        f"Bot Cloudflare sync finished: {format_timestamp()}; users={len(compact_state['users'])}; applied={applied_count}",
    )
    print(f"  Synced one-sheet bot state: {len(compact_state['users'])} users")


if __name__ == '__main__':
    main()
