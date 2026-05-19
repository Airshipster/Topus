import os
from datetime import datetime, timezone

import gspread
import requests

import config
from sheets import authenticate_google_sheets, clean_sheet_value, get_values_with_quota_retry


SHEET_NAME = 'Бот данные'
HEADERS = [
    'Record Key',
    'Type',
    'Project Code',
    'User ID',
    'Username',
    'First Name',
    'Channel ID',
    'Channel Title',
    'Active',
    'Is Paid',
    'Is Allowlisted',
    'Note',
    'Cloudflare Updated At',
    'Sheet Synced At',
    'Sync Action',
]

TRUE_VALUES = {'1', 'true', 'yes', 'y', 'да', 'истина', '✅', 'on', 'active'}
FALSE_VALUES = {'0', 'false', 'no', 'n', 'нет', 'ложь', '❌', 'off', 'inactive'}


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def bool_from_sheet(value, default=False):
    text = str(clean_sheet_value(value) or '').strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    return default


def bool_to_sheet(value):
    return 'TRUE' if bool(value) else 'FALSE'


def get_cell(row, headers, name):
    index = headers.get(name)
    if index is None or index >= len(row):
        return ''
    return clean_sheet_value(row[index])


def ensure_bot_state_worksheet(sheet):
    try:
        worksheet = sheet.worksheet(SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(SHEET_NAME, rows=1000, cols=len(HEADERS))
        worksheet.update(range_name='A1', values=[HEADERS], value_input_option='USER_ENTERED')
        return worksheet

    values = get_values_with_quota_retry(worksheet, '1:1')
    current_headers = [str(cell).strip() for cell in values[0]] if values else []
    if current_headers != HEADERS:
        worksheet.update(range_name='A1', values=[HEADERS], value_input_option='USER_ENTERED')
    if worksheet.col_count < len(HEADERS):
        worksheet.resize(rows=worksheet.row_count, cols=len(HEADERS))
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
        raise RuntimeError(f"Worker state export failed: {state}")
    return state


def normalize_worker_bool(record, key):
    return bool(int(record.get(key) or 0))


def build_worker_records(state):
    records = {}
    for row in state.get('users') or []:
        project_code = str(row.get('project_code') or '').strip()
        user_id = str(row.get('user_id') or '').strip()
        if not project_code or not user_id:
            continue
        key = f'user:{project_code}:{user_id}'
        records[key] = {
            'Record Key': key,
            'Type': 'user',
            'Project Code': project_code,
            'User ID': user_id,
            'Username': row.get('username') or '',
            'First Name': row.get('first_name') or '',
            'Channel ID': '',
            'Channel Title': '',
            'Active': '',
            'Is Paid': bool_to_sheet(normalize_worker_bool(row, 'is_paid')),
            'Is Allowlisted': bool_to_sheet(normalize_worker_bool(row, 'is_allowlisted')),
            'Note': '',
            'Cloudflare Updated At': row.get('updated_at') or row.get('created_at') or '',
            'Sync Action': '',
        }

    for row in state.get('subscriptions') or []:
        project_code = str(row.get('project_code') or '').strip()
        user_id = str(row.get('user_id') or '').strip()
        channel_id = str(row.get('channel_id') or '').strip()
        if not project_code or not user_id or not channel_id:
            continue
        key = f'subscription:{project_code}:{user_id}:{channel_id}'
        records[key] = {
            'Record Key': key,
            'Type': 'subscription',
            'Project Code': project_code,
            'User ID': user_id,
            'Username': '',
            'First Name': '',
            'Channel ID': channel_id,
            'Channel Title': row.get('channel_title') or '',
            'Active': bool_to_sheet(normalize_worker_bool(row, 'active')),
            'Is Paid': '',
            'Is Allowlisted': '',
            'Note': '',
            'Cloudflare Updated At': row.get('updated_at') or row.get('created_at') or '',
            'Sync Action': '',
        }

    for row in state.get('allowlist') or []:
        project_code = str(row.get('project_code') or '').strip()
        user_id = str(row.get('user_id') or '').strip()
        if not project_code or not user_id:
            continue
        key = f'allowlist:{project_code}:{user_id}'
        records[key] = {
            'Record Key': key,
            'Type': 'allowlist',
            'Project Code': project_code,
            'User ID': user_id,
            'Username': '',
            'First Name': '',
            'Channel ID': '',
            'Channel Title': '',
            'Active': bool_to_sheet(True),
            'Is Paid': '',
            'Is Allowlisted': '',
            'Note': row.get('note') or '',
            'Cloudflare Updated At': row.get('updated_at') or row.get('created_at') or '',
            'Sync Action': '',
        }

    return records


def read_sheet_records(worksheet):
    values = get_values_with_quota_retry(worksheet)
    if not values:
        return {}

    headers = {str(cell).strip(): index for index, cell in enumerate(values[0]) if str(cell).strip()}
    records = {}
    for row_number, row in enumerate(values[1:], start=2):
        key = str(get_cell(row, headers, 'Record Key') or '').strip()
        if not key:
            continue
        records[key] = {
            'row_number': row_number,
            'type': str(get_cell(row, headers, 'Type') or '').strip(),
            'project_code': str(get_cell(row, headers, 'Project Code') or '').strip(),
            'user_id': str(get_cell(row, headers, 'User ID') or '').strip(),
            'channel_id': str(get_cell(row, headers, 'Channel ID') or '').strip(),
            'active': get_cell(row, headers, 'Active'),
            'is_paid': get_cell(row, headers, 'Is Paid'),
            'is_allowlisted': get_cell(row, headers, 'Is Allowlisted'),
            'note': str(get_cell(row, headers, 'Note') or '').strip(),
            'cloudflare_updated_at': str(get_cell(row, headers, 'Cloudflare Updated At') or '').strip(),
            'sync_action': str(get_cell(row, headers, 'Sync Action') or '').strip().lower(),
        }
    return records


def sheet_row_changed(sheet_record, worker_record):
    row_type = sheet_record['type'] or str(worker_record.get('Type') or '').strip()
    if row_type == 'user':
        return (
            bool_from_sheet(sheet_record['is_paid']) != bool_from_sheet(worker_record.get('Is Paid'))
            or bool_from_sheet(sheet_record['is_allowlisted']) != bool_from_sheet(worker_record.get('Is Allowlisted'))
        )
    if row_type == 'subscription':
        return bool_from_sheet(sheet_record['active']) != bool_from_sheet(worker_record.get('Active'))
    if row_type == 'allowlist':
        return (
            bool_from_sheet(sheet_record['active'], default=True) != bool_from_sheet(worker_record.get('Active'), default=True)
            or sheet_record['note'] != str(worker_record.get('Note') or '').strip()
        )
    return False


def user_change_from_sheet(record):
    if not record['project_code'] or not record['user_id']:
        return None
    return {
        'projectCode': record['project_code'],
        'userId': record['user_id'],
        'isPaid': bool_from_sheet(record['is_paid']),
        'isAllowlisted': bool_from_sheet(record['is_allowlisted']),
    }


def subscription_change_from_sheet(record, delete=False):
    if not record['project_code'] or not record['user_id'] or not record['channel_id']:
        return None
    return {
        'projectCode': record['project_code'],
        'userId': record['user_id'],
        'channelId': record['channel_id'],
        'active': bool_from_sheet(record['active']),
        'delete': delete,
    }


def allowlist_change_from_sheet(record, delete=False):
    if not record['project_code'] or not record['user_id']:
        return None
    return {
        'projectCode': record['project_code'],
        'userId': record['user_id'],
        'note': record['note'],
        'active': bool_from_sheet(record['active'], default=True),
        'delete': delete,
    }


def collect_sheet_changes(sheet_records, worker_records):
    payload = {'users': [], 'subscriptions': [], 'allowlist': []}

    for key, sheet_record in sheet_records.items():
        action = sheet_record['sync_action']
        worker_record = worker_records.get(key)
        row_type = sheet_record['type'] or (worker_record or {}).get('Type') or key.split(':', 1)[0]
        cloudflare_changed = bool(worker_record) and (
            sheet_record['cloudflare_updated_at'] != str(worker_record.get('Cloudflare Updated At') or '')
        )

        if action == 'delete':
            if row_type == 'subscription':
                change = subscription_change_from_sheet(sheet_record, delete=True)
                if change:
                    payload['subscriptions'].append(change)
            elif row_type == 'allowlist':
                change = allowlist_change_from_sheet(sheet_record, delete=True)
                if change:
                    payload['allowlist'].append(change)
            continue

        should_push = action == 'push'
        if worker_record and not cloudflare_changed and sheet_row_changed(sheet_record, worker_record):
            should_push = True
        if not worker_record and action == 'push':
            should_push = True
        if not should_push:
            continue

        if row_type == 'user':
            change = user_change_from_sheet(sheet_record)
            if change:
                payload['users'].append(change)
        elif row_type == 'subscription':
            change = subscription_change_from_sheet(sheet_record)
            if change:
                payload['subscriptions'].append(change)
        elif row_type == 'allowlist':
            change = allowlist_change_from_sheet(sheet_record)
            if change:
                payload['allowlist'].append(change)

    return payload


def compact_payload(payload):
    return {key: value for key, value in payload.items() if value}


def write_records_to_sheet(worksheet, records):
    sync_time = now_iso()
    rows = []
    for key in sorted(records):
        record = records[key]
        rows.append([
            record.get('Record Key', ''),
            record.get('Type', ''),
            record.get('Project Code', ''),
            record.get('User ID', ''),
            record.get('Username', ''),
            record.get('First Name', ''),
            record.get('Channel ID', ''),
            record.get('Channel Title', ''),
            record.get('Active', ''),
            record.get('Is Paid', ''),
            record.get('Is Allowlisted', ''),
            record.get('Note', ''),
            record.get('Cloudflare Updated At', ''),
            sync_time,
            '',
        ])

    target_rows = max(1000, len(rows) + 20)
    if worksheet.row_count < target_rows or worksheet.col_count < len(HEADERS):
        worksheet.resize(rows=target_rows, cols=len(HEADERS))

    worksheet.clear()
    if rows:
        worksheet.update(range_name='A1', values=[HEADERS] + rows, value_input_option='USER_ENTERED')
    else:
        worksheet.update(range_name='A1', values=[HEADERS], value_input_option='USER_ENTERED')


def main():
    worker_url = os.environ.get('TOPUS_WORKER_URL', '').strip()
    admin_secret = os.environ.get('TOPUS_WORKER_ADMIN_SECRET', '').strip()
    if not worker_url:
        raise ValueError('TOPUS_WORKER_URL is required')
    if not admin_secret:
        raise ValueError('TOPUS_WORKER_ADMIN_SECRET is required')

    client = authenticate_google_sheets()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = ensure_bot_state_worksheet(sheet)

    state = fetch_worker_state(worker_url, admin_secret)
    worker_records = build_worker_records(state)
    sheet_records = read_sheet_records(worksheet)
    changes = compact_payload(collect_sheet_changes(sheet_records, worker_records))

    if changes:
        result = request_worker('POST', '/admin/sheet-state', worker_url, admin_secret, json=changes)
        print(f"  Applied sheet changes to Worker: {result}")
        state = fetch_worker_state(worker_url, admin_secret)
        worker_records = build_worker_records(state)
    else:
        print('  No sheet changes to push')

    write_records_to_sheet(worksheet, worker_records)
    print(f"  Synced {len(worker_records)} bot state rows to '{SHEET_NAME}'")


if __name__ == '__main__':
    main()
