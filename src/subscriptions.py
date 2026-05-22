import time
from datetime import datetime, timedelta

import gspread
import requests

import config
from sheets import (
    channel_id_from_link,
    clean_sheet_value,
    delete_rows_batch,
    find_setting_row,
    format_timestamp,
    get_all_active_channels,
    get_values_with_quota_retry,
    is_sheets_quota_error,
    parse_datetime_value,
    sheet_datetime_value,
    update_setting_value,
    update_project_provisioning_statuses,
    update_project_provisioning_status_map,
)


SUBSCRIPTION_SYNC_SETTING = 'last_subscription_sync'
SUBSCRIPTION_SYNC_INTERVAL_SECONDS = 86400
SUBSCRIPTIONS_SHEET_NAME = 'Подписки'
SUBSCRIPTIONS_HEADERS = ['Projects', 'Project Count', 'Channel ID', 'Subscribed At', 'Last Renewed', 'Status']
SUBSCRIPTIONS_READ_RANGE = 'A1:ZZ'
SUBSCRIPTIONS_TARGET_ROWS = 10000


def run_with_quota_retry(operation, description, attempts=5, delay_seconds=20):
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except Exception as error:
            if not is_sheets_quota_error(error) or attempt >= attempts:
                raise
            print(f"  ⚠️  Sheets quota busy while {description}; retry {attempt}/{attempts - 1} in {delay_seconds}s")
            time.sleep(delay_seconds)
            delay_seconds *= 2


def base_subscription_header(value):
    return str(value or '').splitlines()[0].strip()


def subscription_header_indexes(headers):
    return {base_subscription_header(header): index for index, header in enumerate(headers)}


def subscription_col(indexes, header):
    index = indexes.get(header)
    return index + 1 if index is not None else None


def subscription_row_for_headers(headers, values_by_header):
    return [
        clean_sheet_value(values_by_header.get(base_subscription_header(header), ''))
        for header in headers
    ]


def subscription_column_range(header, row_index, indexes):
    col = subscription_col(indexes, header)
    if not col:
        return None
    return gspread.utils.rowcol_to_a1(row_index, col)


def ensure_subscription_headers(worksheet, headers):
    stripped = [base_subscription_header(cell) for cell in headers]
    missing = [header for header in SUBSCRIPTIONS_HEADERS if header not in stripped]
    if not missing:
        return stripped

    start_col = len(stripped) + 1
    worksheet.update(
        range_name=f'{column_letter(start_col)}1:{column_letter(start_col + len(missing) - 1)}1',
        values=[missing],
        value_input_option='USER_ENTERED',
    )
    print(f"  ➕ Added missing subscription columns: {', '.join(missing)}")
    return stripped + missing


def normalize_subscription_channel_id(value):
    cleaned = clean_sheet_value(value).strip()
    return channel_id_from_link(cleaned) or cleaned


def subscription_channel_url(channel_id):
    return f'https://www.youtube.com/channel/{channel_id}/videos'


def subscription_channel_formula(channel_id):
    normalized = normalize_subscription_channel_id(channel_id)
    if not normalized:
        return ''
    return f'=HYPERLINK("{subscription_channel_url(normalized)}";"{normalized}")'


def get_subscription_sync_state(sheet):
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        existing, _ = find_setting_row(values, SUBSCRIPTION_SYNC_SETTING)
        if existing:
            return {
                'row_index': existing['row_number'],
                'last_sync': parse_datetime_value(existing.get('value', '')),
            }
    except Exception as e:
        print(f"  ⚠️  Error reading subscription sync state: {e}")

    return {
        'row_index': None,
        'last_sync': None,
    }


def should_run_subscription_sync(sheet, force=False):
    if force:
        return True

    state = get_subscription_sync_state(sheet)
    last_sync = state.get('last_sync')
    if not last_sync:
        return True

    return (datetime.utcnow() - last_sync).total_seconds() >= SUBSCRIPTION_SYNC_INTERVAL_SECONDS


def update_subscription_sync_state(sheet):
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        update_setting_value(
            worksheet,
            SUBSCRIPTION_SYNC_SETTING,
            format_timestamp(),
            'Последняя полная синхронизация YouTube push-подписок',
        )
    except Exception as e:
        print(f"  ⚠️  Error updating subscription sync state: {e}")


def get_subscribed_channels(sheet):
    """Получение списка подписанных каналов"""
    try:
        worksheet = sheet.worksheet(SUBSCRIPTIONS_SHEET_NAME)
        records = worksheet.get_all_records()
        return set(
            normalize_subscription_channel_id(row.get('Channel ID', ''))
            for row in records
            if normalize_subscription_channel_id(row.get('Channel ID', ''))
        )
    except:
        return set()

def get_subscription_records(sheet):
    """Получение подписок вместе со строками и датой обновления"""
    try:
        worksheet = get_or_create_subscriptions_worksheet(sheet)
        values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
        headers = [str(cell).strip() for cell in values[0]] if values else []
        indexes = subscription_header_indexes(headers)
        records = {}

        for i, row in enumerate(values[1:], start=2):
            channel_col = indexes.get('Channel ID', 2)
            renewed_col = indexes.get('Last Renewed', 4)
            projects_col = indexes.get('Projects', 0)
            count_col = indexes.get('Project Count', 1)
            status_col = indexes.get('Status', 5)

            raw_channel_id = clean_sheet_value(row[channel_col]).strip() if len(row) > channel_col else ''
            channel_id = normalize_subscription_channel_id(raw_channel_id)
            if not channel_id:
                continue

            records[channel_id] = {
                'row_index': i,
                'raw_channel_id': raw_channel_id,
                'last_renewed': clean_sheet_value(row[renewed_col]).strip() if len(row) > renewed_col else '',
                'projects': clean_sheet_value(row[projects_col]).strip() if len(row) > projects_col else '',
                'project_count': clean_sheet_value(row[count_col]).strip() if len(row) > count_col else '',
                'status': clean_sheet_value(row[status_col]).strip() if len(row) > status_col else '',
            }

        return records
    except Exception as e:
        print(f"  ⚠️  Error reading subscription records: {type(e).__name__}: {e}")
        return None


def rewrite_subscriptions_values(worksheet):
    values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
    if len(values) < 2:
        return 0

    headers = ensure_subscription_headers(worksheet, [str(cell).strip() for cell in values[0]])
    indexes = subscription_header_indexes(headers)
    channel_col = subscription_col(indexes, 'Channel ID')
    if not channel_col:
        return 0

    updates = []
    for row_index, row in enumerate(values[1:], start=2):
        channel_value = clean_sheet_value(row[channel_col - 1]).strip() if len(row) >= channel_col else ''
        if not channel_value:
            continue
        updates.append({
            'range': gspread.utils.rowcol_to_a1(row_index, channel_col),
            'values': [[subscription_channel_formula(channel_value)]],
        })

    for i in range(0, len(updates), config.BATCH_SIZE):
        worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
        time.sleep(0.2)

    return len(updates)


def subscription_status_header(values):
    counts = {'✅': 0, '❌': 0, '⚠️': 0}
    if values:
        headers = [str(cell).strip() for cell in values[0]]
        indexes = subscription_header_indexes(headers)
        status_col = indexes.get('Status', 5)
        for row in values[1:]:
            status = clean_sheet_value(row[status_col]).strip() if len(row) > status_col else ''
            for marker in counts:
                if marker in status:
                    counts[marker] += 1

    summary = ', '.join(f'{marker}{count}' for marker, count in counts.items() if count)
    return f'Status\n{summary}' if summary else 'Status'


def update_subscription_status_header(worksheet, values=None):
    try:
        if values is None:
            values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
        if not values:
            return
        headers = ensure_subscription_headers(worksheet, [str(cell).strip() for cell in values[0]])
        indexes = subscription_header_indexes(headers)
        status_col = subscription_col(indexes, 'Status')
        if not status_col:
            return
        header = subscription_status_header(values)
        current = str(values[0][status_col - 1]).strip() if len(values[0]) >= status_col else ''
        if current == header:
            return
        worksheet.update(range_name=gspread.utils.rowcol_to_a1(1, status_col), values=[[header]], value_input_option='USER_ENTERED')
    except Exception as e:
        print(f"  ⚠️  Error updating subscription status header: {e}")


def ensure_subscription_row_count(sheet, worksheet):
    if worksheet.row_count == SUBSCRIPTIONS_TARGET_ROWS:
        return
    sheet.batch_update({
        'requests': [{
            'updateSheetProperties': {
                'properties': {
                    'sheetId': worksheet.id,
                    'gridProperties': {'rowCount': SUBSCRIPTIONS_TARGET_ROWS},
                },
                'fields': 'gridProperties.rowCount',
            }
        }]
    })
    print(f"  📐 Restored subscriptions row count: {SUBSCRIPTIONS_TARGET_ROWS}")


def normalize_subscriptions_columns(sheet, worksheet, headers, values=None):
    """Keep required subscription columns present without forcing their order."""
    stripped = ensure_subscription_headers(worksheet, headers)
    update_subscription_status_header(worksheet, values)
    return stripped

def get_or_create_subscriptions_worksheet(sheet):
    delay_seconds = 5
    for attempt in range(1, 4):
        try:
            worksheet = sheet.worksheet(SUBSCRIPTIONS_SHEET_NAME)
            break
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(SUBSCRIPTIONS_SHEET_NAME, rows=SUBSCRIPTIONS_TARGET_ROWS, cols=len(SUBSCRIPTIONS_HEADERS))
            worksheet.append_row(SUBSCRIPTIONS_HEADERS, value_input_option='USER_ENTERED')
            return worksheet
        except Exception as error:
            if not is_sheets_quota_error(error) or attempt >= 3:
                raise
            print(f"  ⚠️  Sheets quota busy while opening {SUBSCRIPTIONS_SHEET_NAME}; retry {attempt}/2 in {delay_seconds}s")
            time.sleep(delay_seconds)
            delay_seconds *= 2

    ensure_subscription_row_count(sheet, worksheet)

    values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
    if not values:
        worksheet.append_row(SUBSCRIPTIONS_HEADERS, value_input_option='USER_ENTERED')
        update_subscription_status_header(worksheet)
        return worksheet

    headers = [str(cell).strip() for cell in values[0]]
    normalize_subscriptions_columns(sheet, worksheet, headers, values)

    return worksheet

def column_letter(column_index):
    letters = ''
    while column_index:
        column_index, remainder = divmod(column_index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters

def parse_subscription_date(value):
    """Парсинг дат подписок из таблицы"""
    if not value:
        return None

    return parse_datetime_value(value)

def get_stale_subscriptions(subscription_records, active_channels):
    """Каналы, подписку на которые нужно продлить"""
    cutoff = datetime.utcnow() - timedelta(days=config.SUBSCRIPTION_RENEW_AFTER_DAYS)
    stale = set()

    for channel_id in active_channels:
        record = subscription_records.get(channel_id)
        if not record:
            continue

        last_renewed = parse_subscription_date(record.get('last_renewed', ''))
        if not last_renewed or last_renewed < cutoff:
            stale.add(channel_id)

    return stale

def get_failed_subscriptions(subscription_records, active_channels):
    """Каналы с последней ошибкой subscribe/renew, которые стоит повторить точечно."""
    failed = set()

    for channel_id in active_channels:
        record = subscription_records.get(channel_id)
        if not record:
            continue
        status = str(record.get('status', '')).strip()
        if status.startswith('❌'):
            failed.add(channel_id)

    return failed

def subscription_status_body(status):
    text = str(status or '').strip()
    if not text:
        return ''
    if text.startswith('⚠️'):
        text = text[2:].strip()
    elif text[0] in {'✅', '❌', '⚠'}:
        text = text[1:].strip()
    if text.lower().startswith('бот:'):
        text = text.split(':', 1)[1].strip()
    return text

def is_bot_only_subscription(active_channels_dict, channel_id):
    channel_info = active_channels_dict.get(channel_id, {}).get('channel_info', {})
    return bool(channel_info.get('bot_only'))

def format_subscription_status(status, active_channels_dict=None, channel_id=''):
    text = str(status or '').strip()
    if not text:
        return text
    active_channels_dict = active_channels_dict or {}
    icon = ''
    body = text
    if text.startswith('⚠️'):
        icon = '⚠️'
        body = text[2:].strip()
    elif text[0] in {'✅', '❌', '⚠'}:
        icon = text[0]
        body = text[1:].strip()
    if body.lower().startswith('бот:'):
        body = body.split(':', 1)[1].strip()
    if is_bot_only_subscription(active_channels_dict, channel_id):
        body = f'бот: {body}'
    return f'{icon} {body}'.strip() if icon else body

def format_channel_projects(active_channels_dict, channel_id):
    projects = sorted(set(active_channels_dict.get(channel_id, {}).get('projects', [])))
    return ', '.join(projects)

def save_subscribed_channels_batch(sheet, channel_ids, active_channels_dict):
    """Сохранение подписок на каналы"""
    worksheet = get_or_create_subscriptions_worksheet(sheet)
    
    timestamp = format_timestamp()
    values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
    headers = ensure_subscription_headers(worksheet, [str(cell).strip() for cell in values[0]]) if values else SUBSCRIPTIONS_HEADERS
    rows = []
    for channel_id in channel_ids:
        row_values = {
            'Projects': format_channel_projects(active_channels_dict, channel_id),
            'Project Count': len(set(active_channels_dict.get(channel_id, {}).get('projects', []))),
            'Channel ID': subscription_channel_formula(channel_id),
            'Subscribed At': sheet_datetime_value(timestamp),
            'Last Renewed': sheet_datetime_value(timestamp),
            'Status': format_subscription_status('✅ subscribed', active_channels_dict, channel_id),
        }
        rows.append(subscription_row_for_headers(headers, row_values))
    
    if rows:
        delay_seconds = 5
        for attempt in range(1, 4):
            try:
                worksheet.append_rows(rows, value_input_option='USER_ENTERED')
                break
            except Exception as error:
                if not is_sheets_quota_error(error) or attempt >= 3:
                    raise
                print(f"  ⚠️  Sheets quota busy while saving subscriptions; retry {attempt}/2 in {delay_seconds}s")
                time.sleep(delay_seconds)
                delay_seconds *= 2

def update_subscription_renewals_batch(sheet, subscription_records, channel_ids, active_channels_dict=None):
    """Обновление времени продления существующих подписок"""
    if not channel_ids:
        return

    try:
        worksheet = get_or_create_subscriptions_worksheet(sheet)
        values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
        headers = ensure_subscription_headers(worksheet, [str(cell).strip() for cell in values[0]]) if values else SUBSCRIPTIONS_HEADERS
        indexes = subscription_header_indexes(headers)
        renewed_col = subscription_col(indexes, 'Last Renewed')
        status_col = subscription_col(indexes, 'Status')
        channel_col = subscription_col(indexes, 'Channel ID')
        timestamp = format_timestamp()
        updates = []
        clear_rows = []

        for channel_id in channel_ids:
            record = subscription_records.get(channel_id)
            if not record:
                continue

            if renewed_col:
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(record["row_index"], renewed_col),
                    'values': [[sheet_datetime_value(timestamp)]]
                })
            if status_col:
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(record["row_index"], status_col),
                    'values': [[format_subscription_status('✅ renewed', active_channels_dict, channel_id)]]
                })
            clear_rows.append(record['row_index'])

        if updates:
            run_with_quota_retry(
                lambda: worksheet.batch_update(updates, value_input_option='USER_ENTERED'),
                'updating subscription renewals',
            )
        if clear_rows and channel_col:
            requests = [{
                'repeatCell': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': row_index - 1,
                        'endRowIndex': row_index,
                        'startColumnIndex': channel_col - 1,
                        'endColumnIndex': channel_col,
                    },
                    'cell': {'userEnteredFormat': {}},
                    'fields': 'userEnteredFormat.backgroundColor',
                }
            } for row_index in clear_rows]
            for i in range(0, len(requests), config.BATCH_SIZE):
                run_with_quota_retry(
                    lambda chunk=requests[i:i + config.BATCH_SIZE]: sheet.batch_update({'requests': chunk}),
                    'clearing subscription error formatting',
                )
                time.sleep(0.2)
    except Exception as e:
        print(f"  ⚠️  Error updating subscription renewals: {e}")


def update_subscription_statuses(sheet, subscription_records, status_by_channel, active_channels_dict=None):
    if not status_by_channel:
        return
    try:
        worksheet = get_or_create_subscriptions_worksheet(sheet)
        values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
        headers = ensure_subscription_headers(worksheet, [str(cell).strip() for cell in values[0]]) if values else SUBSCRIPTIONS_HEADERS
        indexes = subscription_header_indexes(headers)
        status_col = subscription_col(indexes, 'Status')
        channel_col = subscription_col(indexes, 'Channel ID')
        if not status_col:
            return
        updates = []
        red_rows = []
        clear_rows = []
        for channel_id, status in status_by_channel.items():
            record = subscription_records.get(channel_id)
            if not record:
                continue
            status = format_subscription_status(status, active_channels_dict, channel_id)
            row_index = record['row_index']
            updates.append({'range': gspread.utils.rowcol_to_a1(row_index, status_col), 'values': [[status]]})
            if str(status).startswith('❌'):
                red_rows.append(row_index)
            else:
                clear_rows.append(row_index)

        for i in range(0, len(updates), config.BATCH_SIZE):
            run_with_quota_retry(
                lambda chunk=updates[i:i + config.BATCH_SIZE]: worksheet.batch_update(chunk, value_input_option='USER_ENTERED'),
                'updating subscription statuses',
            )
            time.sleep(0.2)

        requests = []
        for row_index in red_rows:
            if not channel_col:
                continue
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': row_index - 1,
                        'endRowIndex': row_index,
                        'startColumnIndex': channel_col - 1,
                        'endColumnIndex': channel_col,
                    },
                    'cell': {'userEnteredFormat': {'backgroundColor': {'red': 1.0, 'green': 0.8, 'blue': 0.8}}},
                    'fields': 'userEnteredFormat.backgroundColor',
                }
            })
        for row_index in clear_rows:
            if not channel_col:
                continue
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': row_index - 1,
                        'endRowIndex': row_index,
                        'startColumnIndex': channel_col - 1,
                        'endColumnIndex': channel_col,
                    },
                    'cell': {'userEnteredFormat': {}},
                    'fields': 'userEnteredFormat.backgroundColor',
                }
            })
        for i in range(0, len(requests), config.BATCH_SIZE):
            run_with_quota_retry(
                lambda chunk=requests[i:i + config.BATCH_SIZE]: sheet.batch_update({'requests': chunk}),
                'updating subscription status formatting',
            )
            time.sleep(0.2)
        update_subscription_status_header(worksheet)
    except Exception as e:
        print(f"  ⚠️  Error updating subscription statuses: {e}")


def normalize_subscription_status_formatting(sheet, subscription_records, active_channels_dict=None):
    """Keep red Channel ID highlighting only for rows with current ❌ status."""
    try:
        worksheet = get_or_create_subscriptions_worksheet(sheet)
        values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
        headers = ensure_subscription_headers(worksheet, [str(cell).strip() for cell in values[0]]) if values else SUBSCRIPTIONS_HEADERS
        indexes = subscription_header_indexes(headers)
        channel_col = subscription_col(indexes, 'Channel ID')
        status_col = subscription_col(indexes, 'Status')
        if not channel_col:
            return
        status_updates = []
        requests = [{
            'repeatCell': {
                'range': {
                    'sheetId': worksheet.id,
                    'startRowIndex': 1,
                    'endRowIndex': SUBSCRIPTIONS_TARGET_ROWS,
                    'startColumnIndex': channel_col - 1,
                    'endColumnIndex': channel_col,
                },
                'cell': {'userEnteredFormat': {}},
                'fields': 'userEnteredFormat.backgroundColor',
            }
        }]

        for channel_id, record in subscription_records.items():
            current_status = str(record.get('status', ''))
            normalized_status = format_subscription_status(current_status, active_channels_dict, channel_id)
            if status_col and normalized_status != current_status:
                status_updates.append({
                    'range': gspread.utils.rowcol_to_a1(record['row_index'], status_col),
                    'values': [[normalized_status]],
                })
                record['status'] = normalized_status
            if not normalized_status.startswith('❌'):
                continue
            row_index = record['row_index']
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': row_index - 1,
                        'endRowIndex': row_index,
                        'startColumnIndex': channel_col - 1,
                        'endColumnIndex': channel_col,
                    },
                    'cell': {'userEnteredFormat': {'backgroundColor': {'red': 1.0, 'green': 0.8, 'blue': 0.8}}},
                    'fields': 'userEnteredFormat.backgroundColor',
                }
            })

        for i in range(0, len(status_updates), config.BATCH_SIZE):
            run_with_quota_retry(
                lambda chunk=status_updates[i:i + config.BATCH_SIZE]: worksheet.batch_update(chunk, value_input_option='USER_ENTERED'),
                'normalizing subscription status text',
            )
            time.sleep(0.2)
        run_with_quota_retry(
            lambda: sheet.batch_update({'requests': requests}),
            'normalizing subscription formatting',
        )
        print("  🎨 Normalized subscription status formatting")
    except Exception as e:
        print(f"  ⚠️  Error normalizing subscription formatting: {e}")


def split_project_names(projects_text):
    return {
        name.strip()
        for name in str(projects_text or '').split(',')
        if name.strip()
    }


def update_subscription_inventory_warnings(sheet, subscription_records, failed_project_names, active_channels_dict=None):
    statuses = {}
    if not failed_project_names:
        for channel_id, record in subscription_records.items():
            status = str(record.get('status', ''))
            body = subscription_status_body(status)
            if not status or body.startswith('project read failed') or body == 'project read ok':
                statuses[channel_id] = '✅ renewed' if record.get('last_renewed') else '✅ subscribed'
        if statuses:
            update_subscription_statuses(sheet, subscription_records, statuses, active_channels_dict)
            print(f"  ✅ Cleared subscription inventory warnings: {len(statuses)}")
        return

    for channel_id, record in subscription_records.items():
        channel_projects = split_project_names(record.get('projects', ''))
        failed_for_channel = sorted(channel_projects & failed_project_names)
        if not failed_for_channel:
            continue
        project_text = ', '.join(failed_for_channel[:2])
        if len(failed_for_channel) > 2:
            project_text += f" +{len(failed_for_channel) - 2}"
        statuses[channel_id] = f"⚠️ project read failed: {project_text}"

    if statuses:
        update_subscription_statuses(sheet, subscription_records, statuses, active_channels_dict)
        print(f"  ⚠️  Marked subscription inventory warnings: {len(statuses)}")


def update_subscription_project_links(sheet, subscription_records, active_channels_dict):
    try:
        worksheet = get_or_create_subscriptions_worksheet(sheet)
        values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
        headers = ensure_subscription_headers(worksheet, [str(cell).strip() for cell in values[0]]) if values else SUBSCRIPTIONS_HEADERS
        indexes = subscription_header_indexes(headers)
        projects_range = lambda row_index: subscription_column_range('Projects', row_index, indexes)
        count_range = lambda row_index: subscription_column_range('Project Count', row_index, indexes)
        channel_range = lambda row_index: subscription_column_range('Channel ID', row_index, indexes)
        updates = []
        updated_rows = set()

        for channel_id, record in subscription_records.items():
            if channel_id not in active_channels_dict:
                continue

            projects = sorted(set(active_channels_dict[channel_id].get('projects', [])))
            projects_text = ', '.join(projects)
            project_count = len(projects)

            row_updates = []
            if record.get('raw_channel_id') != channel_id:
                target_range = channel_range(record["row_index"])
                if target_range:
                    row_updates.append({'range': target_range, 'values': [[subscription_channel_formula(channel_id)]]})

            if record.get('projects') != projects_text or str(record.get('project_count')) != str(project_count):
                target_range = projects_range(record["row_index"])
                if target_range:
                    row_updates.append({'range': target_range, 'values': [[projects_text]]})
                target_range = count_range(record["row_index"])
                if target_range:
                    row_updates.append({'range': target_range, 'values': [[project_count]]})

            if row_updates:
                updates.extend(row_updates)
                updated_rows.add(record['row_index'])

        for i in range(0, len(updates), config.BATCH_SIZE):
            worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
            time.sleep(0.2)

        if updates:
            print(f"  ✅ Updated project links for {len(updated_rows)} subscriptions")
    except Exception as e:
        print(f"  ⚠️  Error updating subscription project links: {e}")


def deduplicate_subscription_rows(sheet):
    try:
        worksheet = get_or_create_subscriptions_worksheet(sheet)
        values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
        if len(values) < 3:
            return 0

        headers = [str(cell).strip() for cell in values[0]]
        indexes = subscription_header_indexes(headers)
        channel_col = indexes.get('Channel ID', 2)
        seen = set()
        rows_to_delete = []

        for row_index, row in enumerate(values[1:], start=2):
            raw_channel_id = clean_sheet_value(row[channel_col]).strip() if len(row) > channel_col else ''
            channel_id = normalize_subscription_channel_id(raw_channel_id)
            if not channel_id:
                continue
            if channel_id in seen:
                rows_to_delete.append(row_index)
                continue
            seen.add(channel_id)

        deleted = delete_rows_batch(sheet, worksheet, rows_to_delete)
        ensure_subscription_row_count(sheet, worksheet)
        update_subscription_status_header(worksheet)
        if deleted:
            print(f"  🧹 Removed duplicate subscription rows: {deleted}")
        return deleted
    except Exception as e:
        print(f"  ⚠️  Error deduplicating subscriptions: {type(e).__name__}: {e}")
        return 0

def subscribe_channel(channel_id, return_error=False):
    """Подписка на push-уведомления"""
    hub_url = "https://pubsubhubbub.appspot.com/subscribe"
    topic_url = f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={channel_id}"
    
    data = {
        'hub.callback': config.CALLBACK_URL,
        'hub.topic': topic_url,
        'hub.mode': 'subscribe',
        'hub.verify': 'async'
    }
    
    try:
        response = requests.post(hub_url, data=data, timeout=10)
        ok = response.status_code in [202, 204]
        if ok:
            return (True, '') if return_error else True
        response_text = clean_sheet_value(response.text)[:120]
        reason = f'HTTP {response.status_code}'
        if response_text:
            reason += f': {response_text}'
        return (False, reason) if return_error else False
    except Exception as e:
        reason = f'{type(e).__name__}: {e}'
        return (False, reason) if return_error else False

def unsubscribe_channel(channel_id):
    """Отписка от push-уведомлений"""
    hub_url = "https://pubsubhubbub.appspot.com/subscribe"
    topic_url = f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={channel_id}"
    
    data = {
        'hub.callback': config.CALLBACK_URL,
        'hub.topic': topic_url,
        'hub.mode': 'unsubscribe',
        'hub.verify': 'async'
    }
    
    try:
        response = requests.post(hub_url, data=data, timeout=10)
        return response.status_code in [202, 204]
    except:
        return False

def remove_subscribed_channels(sheet, channel_ids):
    """Удаление подписок из таблицы"""
    try:
        worksheet = sheet.worksheet(SUBSCRIPTIONS_SHEET_NAME)
        all_values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
        
        rows_to_delete = []
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            headers = [str(cell).strip() for cell in all_values[0]] if all_values else []
            indexes = subscription_header_indexes(headers)
            channel_col = indexes.get('Channel ID', 2)
            if len(row) > channel_col and normalize_subscription_channel_id(row[channel_col]) in channel_ids:
                rows_to_delete.append(i + 1)
        
        delete_rows_batch(sheet, worksheet, rows_to_delete)
        ensure_subscription_row_count(sheet, worksheet)
        update_subscription_status_header(worksheet)
    except Exception as e:
        print(f"  ❌ Error removing subscriptions: {e}")

def sync_subscriptions(client, master_sheet, projects, force=False, active_channels_dict=None):
    """Синхронизация push-подписок"""
    print("\n📡 Syncing subscriptions...")
    result = {
        'ok': True,
        'partial': False,
        'reason': '',
    }

    if active_channels_dict is None:
        active_channels_dict = get_all_active_channels(client, projects)
    channel_load_errors = [
        f"{project.get('name')}: {project.get('channels_error')}"
        for project in projects
        if project.get('channels_error')
    ]
    inventory_complete = not channel_load_errors
    if not inventory_complete:
        result.update({
            'ok': False,
            'partial': True,
            'reason': f"channel load errors: {len(channel_load_errors)}",
        })
        print(f"  ⚠️  Active channel inventory is incomplete ({len(channel_load_errors)} project errors)")
        print("  🛑 Inactive unsubscribe/removal is disabled for this run")
    active_channels = set(active_channels_dict.keys())
    subscription_records = get_subscription_records(master_sheet)
    if subscription_records is None:
        print("  ⚠️  Subscription sync skipped: could not read existing subscriptions")
        result.update({'ok': False, 'partial': True, 'reason': 'could not read subscriptions'})
        return result
    if not inventory_complete:
        failed_project_names = {
            str(project.get('name', '')).strip()
            for project in projects
            if project.get('channels_error') and str(project.get('name', '')).strip()
        }
    else:
        failed_project_names = set()
    update_subscription_inventory_warnings(master_sheet, subscription_records, failed_project_names, active_channels_dict)
    subscribed_channels = set(subscription_records.keys())
    to_subscribe = active_channels - subscribed_channels
    to_unsubscribe = set() if not inventory_complete else subscribed_channels - active_channels

    if not should_run_subscription_sync(master_sheet, force=force):
        print("  ⏭️  Subscribe/renew skipped (last full sync < 24h)")
        changed_subscription_rows = False
        failed_channels = get_failed_subscriptions(subscription_records, active_channels)
        if to_subscribe:
            print(f"  Subscribing to {len(to_subscribe)} new channels despite recent full sync...")
            subscribed = []
            for channel_id in to_subscribe:
                if subscribe_channel(channel_id):
                    subscribed.append(channel_id)
                time.sleep(0.1)

            if subscribed:
                save_subscribed_channels_batch(master_sheet, subscribed, active_channels_dict)
                changed_subscription_rows = True
                print(f"  ✅ Successfully subscribed: {len(subscribed)}")

        if failed_channels:
            print(f"  Retrying {len(failed_channels)} failed push subscriptions despite recent full sync...")
            renewed = []
            renewal_errors = {}
            for channel_id in sorted(failed_channels):
                ok, error_text = subscribe_channel(channel_id, return_error=True)
                if ok:
                    renewed.append(channel_id)
                else:
                    renewal_errors[channel_id] = error_text
                time.sleep(0.1)

            if renewed:
                update_subscription_renewals_batch(master_sheet, subscription_records, renewed, active_channels_dict)
                changed_subscription_rows = True
                print(f"  ✅ Successfully retried: {len(renewed)}")
            if renewal_errors:
                update_subscription_statuses(
                    master_sheet,
                    subscription_records,
                    {channel_id: f'❌ subscribe/renew failed: {error_text}' for channel_id, error_text in renewal_errors.items()},
                    active_channels_dict,
                )
                changed_subscription_rows = True
                print(f"  ❌ Subscription retry failed: {len(renewal_errors)}")

        if to_unsubscribe:
            print(f"  Unsubscribing/removing {len(to_unsubscribe)} inactive subscriptions...")
            for channel_id in to_unsubscribe:
                unsubscribe_channel(channel_id)
                time.sleep(0.1)
            remove_subscribed_channels(master_sheet, to_unsubscribe)
            changed_subscription_rows = True
        if changed_subscription_rows:
            subscription_records = get_subscription_records(master_sheet)
            if subscription_records is None:
                print("  ⚠️  Project link update skipped: could not re-read subscriptions")
                result.update({'ok': False, 'partial': True, 'reason': 'could not re-read subscriptions'})
                return result
        update_subscription_project_links(master_sheet, subscription_records, active_channels_dict)
        normalize_subscription_status_formatting(master_sheet, subscription_records, active_channels_dict)
        return result

    if force:
        print("  🔁 Forced subscription sync requested")

    stale_channels = get_stale_subscriptions(subscription_records, active_channels)

    if force:
        to_renew = set(active_channels)
        print("  🔁 Force mode: renewing all active push subscriptions")
    else:
        to_renew = active_channels & stale_channels
    
    print(f"  Active channels: {len(active_channels)}")
    print(f"  Already subscribed: {len(subscribed_channels)}")
    print(f"  New to subscribe: {len(to_subscribe)}")
    print(f"  To renew: {len(to_renew)}")
    print(f"  To unsubscribe: {len(to_unsubscribe)}")
    
    if len(to_subscribe) > 0:
        print(f"  Subscribing to {len(to_subscribe)} new channels...")
        subscribed = []
        for channel_id in to_subscribe:
            if subscribe_channel(channel_id):
                subscribed.append(channel_id)
            time.sleep(0.1)
        
        if subscribed:
            save_subscribed_channels_batch(master_sheet, subscribed, active_channels_dict)
            print(f"  ✅ Successfully subscribed: {len(subscribed)}")

    if len(to_renew) > 0:
        print(f"  Renewing {len(to_renew)} existing subscriptions...")
        renewed = []
        renewal_errors = {}
        to_renew_list = sorted(to_renew)
        project_renew_totals = {}
        project_renew_done = {}
        if force:
            for channel_id in to_renew_list:
                for project_name in active_channels_dict.get(channel_id, {}).get('projects', []):
                    project_renew_totals[project_name] = project_renew_totals.get(project_name, 0) + 1

        for channel_id in to_renew_list:
            if force:
                touched_projects = active_channels_dict.get(channel_id, {}).get('projects', [])
                status_updates = {}
                for project_name in touched_projects:
                    total = project_renew_totals.get(project_name, 0)
                    if not total:
                        continue
                    done = project_renew_done.get(project_name, 0) + 1
                    project_renew_done[project_name] = done
                    if done == 1 or done == total or done % 10 == 0:
                        status_updates[project_name] = f'checking subscriptions {done}/{total}'
                update_project_provisioning_status_map(
                    master_sheet,
                    projects,
                    status_updates,
                    'renewing push subscriptions',
                )
            ok, error_text = subscribe_channel(channel_id, return_error=True)
            if ok:
                renewed.append(channel_id)
            elif error_text:
                renewal_errors[channel_id] = error_text
            time.sleep(0.1)

        if renewed:
            update_subscription_renewals_batch(master_sheet, subscription_records, renewed, active_channels_dict)
            update_subscription_statuses(
                master_sheet,
                subscription_records,
                {channel_id: '✅ renewed' for channel_id in renewed},
                active_channels_dict,
            )
            print(f"  ✅ Successfully renewed: {len(renewed)}")
        failed_renewals = sorted(set(to_renew) - set(renewed))
        if failed_renewals:
            update_subscription_statuses(
                master_sheet,
                subscription_records,
                {
                    channel_id: f"❌ subscribe/renew failed: {renewal_errors.get(channel_id, 'unknown')}"
                    for channel_id in failed_renewals
                },
                active_channels_dict,
            )
            print(f"  ❌ Subscription renew failed: {len(failed_renewals)}")
    
    if len(to_unsubscribe) > 0:
        print(f"  Unsubscribing from {len(to_unsubscribe)} inactive channels...")
        unsubscribed = []
        for channel_id in to_unsubscribe:
            if unsubscribe_channel(channel_id):
                unsubscribed.append(channel_id)
            time.sleep(0.1)
        
        remove_subscribed_channels(master_sheet, to_unsubscribe)
        print(f"  ✅ Removed inactive subscriptions from sheet: {len(to_unsubscribe)}")
        if unsubscribed:
            print(f"  ✅ PubSub unsubscribe accepted: {len(unsubscribed)}")
    
    if len(to_subscribe) == 0 and len(to_renew) == 0 and len(to_unsubscribe) == 0:
        print("  ✅ No changes needed")

    subscription_records = get_subscription_records(master_sheet)
    if subscription_records is None:
        print("  ⚠️  Final project link update skipped: could not re-read subscriptions")
        result.update({'ok': False, 'partial': True, 'reason': 'could not re-read subscriptions'})
        return result
    update_subscription_project_links(master_sheet, subscription_records, active_channels_dict)
    normalize_subscription_status_formatting(master_sheet, subscription_records, active_channels_dict)

    if inventory_complete:
        update_subscription_sync_state(master_sheet)
    else:
        print("  ⚠️  Full subscription sync timestamp not updated because channel inventory was incomplete")

    return result
