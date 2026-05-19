import os
from collections import defaultdict

import gspread
import requests

import config
from sheets import authenticate_google_sheets, clean_sheet_value, format_timestamp, get_values_with_quota_retry, normalize_timestamp


LEGACY_SHEET_NAME = 'Бот данные'
USERS_SHEET_NAME = 'Боты'
SUBSCRIPTIONS_SHEET_NAME = 'Бот подписки'
FREE_SHEET_NAME = 'Бот free'

USER_HEADERS = [
    'Project Code',
    'Bot',
    'User ID',
    'Username',
    'First Name',
    'Is Paid',
    'Is Allowlisted',
    'Access',
    'Cloudflare Updated At',
    'Sheet Synced At',
    'Sync Action',
]

SUBSCRIPTION_HEADERS = [
    'Project Code',
    'Bot',
    'User ID',
    'Username',
    'First Name',
    'Mode',
    'Included Channel IDs',
    'Excluded Channel IDs',
    'Subscribed Count',
    'Total Channels',
    'Cloudflare Updated At',
    'Sheet Synced At',
    'Sync Action',
]

FREE_HEADERS = [
    'Project Code',
    'Bot',
    'User ID',
    'Username',
    'First Name',
    'Active',
    'Note',
    'Source',
    'Is Paid',
    'Is Allowlisted',
    'Cloudflare Updated At',
    'Sheet Synced At',
    'Sync Action',
]

TRUE_VALUES = {'1', 'true', 'yes', 'y', 'да', 'истина', '✅', 'on', 'active'}
FALSE_VALUES = {'0', 'false', 'no', 'n', 'нет', 'ложь', '❌', 'off', 'inactive'}


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
    if not text:
        return set()
    return {item.strip() for item in text.replace('\n', ',').split(',') if item.strip()}


def join_ids(values):
    return ','.join(sorted(values))


def max_timestamp(*values):
    filtered = [str(value or '').strip() for value in values if str(value or '').strip()]
    return max(filtered) if filtered else ''


def display_timestamp(value):
    return normalize_timestamp(value) if value else ''


def worker_bool(record, key):
    return bool(int(record.get(key) or 0))


def row_key(project_code, user_id):
    return f'{project_code}:{user_id}'


def get_cell(row, headers, name):
    index = headers.get(name)
    if index is None or index >= len(row):
        return ''
    return clean_sheet_value(row[index])


def ensure_worksheet(sheet, name, headers, rows=1000):
    try:
        worksheet = sheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(name, rows=rows, cols=len(headers))
        worksheet.update(range_name='A1', values=[headers], value_input_option='USER_ENTERED')
        return worksheet

    values = get_values_with_quota_retry(worksheet, '1:1')
    current_headers = [str(cell).strip() for cell in values[0]] if values else []
    if current_headers != headers:
        worksheet.update(range_name='A1', values=[headers], value_input_option='USER_ENTERED')
    if worksheet.col_count < len(headers):
        worksheet.resize(rows=worksheet.row_count, cols=len(headers))
    return worksheet


def compact_legacy_sheet(sheet):
    try:
        worksheet = sheet.worksheet(LEGACY_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        return
    worksheet.clear()
    worksheet.update(
        range_name='A1',
        values=[['Статус'], ['Подробное зеркало заменено компактными листами: Боты, Бот подписки, Бот free.']],
        value_input_option='USER_ENTERED',
    )
    if worksheet.row_count > 20:
        worksheet.resize(rows=20, cols=2)


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
            projects[code] = str(project.get('name') or code).strip()

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
        key = row_key(project_code, user_id)
        allowlist[key] = {
            'project_code': project_code,
            'user_id': user_id,
            'note': entry.get('note') or '',
            'active': True,
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
    }


def read_action_rows(worksheet):
    values = get_values_with_quota_retry(worksheet)
    if not values:
        return []
    headers = {str(cell).strip(): index for index, cell in enumerate(values[0]) if str(cell).strip()}
    rows = []
    for raw_row in values[1:]:
        action = str(get_cell(raw_row, headers, 'Sync Action') or '').strip().lower()
        if action not in {'push', 'delete'}:
            continue
        project_code = str(get_cell(raw_row, headers, 'Project Code') or '').strip()
        user_id = str(get_cell(raw_row, headers, 'User ID') or '').strip()
        if not project_code or not user_id:
            continue
        rows.append({
            'project_code': project_code,
            'user_id': user_id,
            'username': str(get_cell(raw_row, headers, 'Username') or '').strip(),
            'first_name': str(get_cell(raw_row, headers, 'First Name') or '').strip(),
            'is_paid': get_cell(raw_row, headers, 'Is Paid'),
            'is_allowlisted': get_cell(raw_row, headers, 'Is Allowlisted'),
            'mode': str(get_cell(raw_row, headers, 'Mode') or '').strip().lower(),
            'included': get_cell(raw_row, headers, 'Included Channel IDs'),
            'excluded': get_cell(raw_row, headers, 'Excluded Channel IDs'),
            'active': get_cell(raw_row, headers, 'Active'),
            'note': str(get_cell(raw_row, headers, 'Note') or '').strip(),
            'action': action,
        })
    return rows


def desired_subscriptions(row, all_channel_ids):
    mode = row['mode'] or 'custom'
    included = split_ids(row['included'])
    excluded = split_ids(row['excluded'])
    all_channels = set(all_channel_ids)
    if mode == 'all':
        return all_channels - excluded
    return included & all_channels


def collect_changes(user_rows, subscription_rows, free_rows, compact_state):
    payload = {'users': [], 'subscriptions': [], 'allowlist': []}
    users = compact_state['users']
    allowlist = compact_state['allowlist']
    channels_by_project = compact_state['channels_by_project']
    current_sub_rows = compact_state['subscription_rows']

    for row in user_rows:
        if row['action'] == 'delete':
            continue
        existing = users.get(row_key(row['project_code'], row['user_id']), {})
        payload['users'].append({
            'projectCode': row['project_code'],
            'userId': row['user_id'],
            'username': row['username'] or existing.get('username') or None,
            'firstName': row['first_name'] or existing.get('first_name') or None,
            'isPaid': bool_from_sheet(row['is_paid'], existing.get('is_paid', False)),
            'isAllowlisted': bool_from_sheet(row['is_allowlisted'], existing.get('is_allowlisted', False)),
        })

    for row in subscription_rows:
        project_code = row['project_code']
        user_id = row['user_id']
        key = row_key(project_code, user_id)
        all_channel_ids = channels_by_project.get(project_code, [])
        current = {channel_id for channel_id, active in current_sub_rows.get(key, {}).items() if active}
        desired = set() if row['action'] == 'delete' else desired_subscriptions(row, all_channel_ids)
        for channel_id in sorted(current | desired):
            if (channel_id in current) == (channel_id in desired):
                continue
            payload['subscriptions'].append({
                'projectCode': project_code,
                'userId': user_id,
                'channelId': channel_id,
                'active': channel_id in desired,
            })

    for row in free_rows:
        key = row_key(row['project_code'], row['user_id'])
        existing_user = users.get(key, {})
        if row['action'] == 'delete':
            payload['allowlist'].append({
                'projectCode': row['project_code'],
                'userId': row['user_id'],
                'delete': True,
            })
            payload['users'].append({
                'projectCode': row['project_code'],
                'userId': row['user_id'],
                'username': row['username'] or existing_user.get('username') or None,
                'firstName': row['first_name'] or existing_user.get('first_name') or None,
                'isPaid': bool_from_sheet(row['is_paid'], existing_user.get('is_paid', False)),
                'isAllowlisted': False,
            })
            continue
        payload['allowlist'].append({
            'projectCode': row['project_code'],
            'userId': row['user_id'],
            'note': row['note'],
            'active': bool_from_sheet(row['active'], True),
        })
        payload['users'].append({
            'projectCode': row['project_code'],
            'userId': row['user_id'],
            'username': row['username'] or existing_user.get('username') or None,
            'firstName': row['first_name'] or existing_user.get('first_name') or None,
            'isPaid': bool_from_sheet(row['is_paid'], existing_user.get('is_paid', False)),
            'isAllowlisted': True,
        })

    return {key: value for key, value in payload.items() if value}


def user_access(user, allowlist_entry):
    if user.get('is_allowlisted') or allowlist_entry:
        return 'free'
    if user.get('is_paid'):
        return 'paid'
    return 'none'


def compact_subscription_row(project_code, bot_name, user_id, user, all_channel_ids, current_subscriptions, updated_at):
    all_channels = set(all_channel_ids)
    selected = {channel_id for channel_id, active in current_subscriptions.items() if active and channel_id in all_channels}
    if all_channels and selected == all_channels:
        mode = 'all'
        included = ''
        excluded = ''
    elif all_channels and len(selected) > len(all_channels) / 2:
        mode = 'all'
        included = ''
        excluded = join_ids(all_channels - selected)
    else:
        mode = 'custom'
        included = join_ids(selected)
        excluded = ''
    return [
        project_code,
        bot_name,
        user_id,
        user.get('username', ''),
        user.get('first_name', ''),
        mode,
        included,
        excluded,
        len(selected),
        len(all_channels),
        display_timestamp(updated_at),
    ]


def write_rows(worksheet, headers, rows):
    target_rows = max(1000, len(rows) + 20)
    if worksheet.row_count < target_rows or worksheet.col_count < len(headers):
        worksheet.resize(rows=target_rows, cols=len(headers))
    worksheet.clear()
    worksheet.update(range_name='A1', values=[headers] + rows, value_input_option='USER_ENTERED')


def write_compact_sheets(worksheets, compact_state):
    sync_time = format_timestamp()
    projects = compact_state['projects']
    users = compact_state['users']
    allowlist = compact_state['allowlist']
    channels_by_project = compact_state['channels_by_project']
    subscription_rows = compact_state['subscription_rows']
    subscription_updated_at = compact_state['subscription_updated_at']

    all_user_keys = set(users) | set(subscription_rows) | set(allowlist)

    user_sheet_rows = []
    subscription_sheet_rows = []
    free_sheet_rows = []

    for key in sorted(all_user_keys):
        project_code, user_id = key.split(':', 1)
        bot_name = projects.get(project_code, project_code)
        user = users.get(key, {
            'project_code': project_code,
            'user_id': user_id,
            'username': '',
            'first_name': '',
            'is_paid': False,
            'is_allowlisted': False,
            'updated_at': '',
        })
        allowlist_entry = allowlist.get(key)
        access = user_access(user, allowlist_entry)
        user_sheet_rows.append([
            project_code,
            bot_name,
            user_id,
            user.get('username', ''),
            user.get('first_name', ''),
            bool_to_sheet(user.get('is_paid')),
            bool_to_sheet(user.get('is_allowlisted') or bool(allowlist_entry)),
            access,
            display_timestamp(max_timestamp(user.get('updated_at'), allowlist_entry.get('updated_at') if allowlist_entry else '')),
            sync_time,
            '',
        ])

        subscription_sheet_rows.append(
            compact_subscription_row(
                project_code,
                bot_name,
                user_id,
                user,
                channels_by_project.get(project_code, []),
                subscription_rows.get(key, {}),
                subscription_updated_at.get(key, ''),
            ) + [sync_time, '']
        )

        if access == 'free':
            free_sheet_rows.append([
                project_code,
                bot_name,
                user_id,
                user.get('username', ''),
                user.get('first_name', ''),
                bool_to_sheet(True),
                allowlist_entry.get('note', '') if allowlist_entry else 'user.is_allowlisted',
                'allowlist' if allowlist_entry else 'user flag',
                bool_to_sheet(user.get('is_paid')),
                bool_to_sheet(user.get('is_allowlisted') or bool(allowlist_entry)),
                display_timestamp(max_timestamp(user.get('updated_at'), allowlist_entry.get('updated_at') if allowlist_entry else '')),
                sync_time,
                '',
            ])

    write_rows(worksheets['users'], USER_HEADERS, user_sheet_rows)
    write_rows(worksheets['subscriptions'], SUBSCRIPTION_HEADERS, subscription_sheet_rows)
    write_rows(worksheets['free'], FREE_HEADERS, free_sheet_rows)


def main():
    worker_url = os.environ.get('TOPUS_WORKER_URL', '').strip()
    admin_secret = os.environ.get('TOPUS_WORKER_ADMIN_SECRET', '').strip()
    if not worker_url:
        raise ValueError('TOPUS_WORKER_URL is required')
    if not admin_secret:
        raise ValueError('TOPUS_WORKER_ADMIN_SECRET is required')

    client = authenticate_google_sheets()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    compact_legacy_sheet(sheet)
    worksheets = {
        'users': ensure_worksheet(sheet, USERS_SHEET_NAME, USER_HEADERS),
        'subscriptions': ensure_worksheet(sheet, SUBSCRIPTIONS_SHEET_NAME, SUBSCRIPTION_HEADERS),
        'free': ensure_worksheet(sheet, FREE_SHEET_NAME, FREE_HEADERS),
    }

    state = fetch_worker_state(worker_url, admin_secret)
    compact_state = build_state(state)
    changes = collect_changes(
        read_action_rows(worksheets['users']),
        read_action_rows(worksheets['subscriptions']),
        read_action_rows(worksheets['free']),
        compact_state,
    )

    if changes:
        result = request_worker('POST', '/admin/sheet-state', worker_url, admin_secret, json=changes)
        print(f'  Applied compact sheet changes to Worker: {result}')
        state = fetch_worker_state(worker_url, admin_secret)
        compact_state = build_state(state)
    else:
        print('  No compact sheet changes to push')

    write_compact_sheets(worksheets, compact_state)
    print(
        f"  Synced compact bot state: "
        f"{len(compact_state['users'])} users, "
        f"{len(compact_state['subscription_rows'])} subscription rows, "
        f"{len(compact_state['allowlist'])} free rows"
    )


if __name__ == '__main__':
    main()
