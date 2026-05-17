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
    parse_datetime_value,
    sheet_datetime_value,
    update_setting_value,
)


SUBSCRIPTION_SYNC_SETTING = 'last_subscription_sync'
SUBSCRIPTION_SYNC_INTERVAL_SECONDS = 86400
SUBSCRIPTIONS_SHEET_NAME = 'Подписки'
SUBSCRIPTIONS_HEADERS = ['Projects', 'Project Count', 'Channel ID', 'Subscribed At', 'Last Renewed', 'Status']
SUBSCRIPTIONS_READ_RANGE = 'A1:F'


def normalize_subscription_channel_id(value):
    cleaned = clean_sheet_value(value).strip()
    return channel_id_from_link(cleaned) or cleaned


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
        indexes = {header: index for index, header in enumerate(headers)}
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

    updates = []
    for row_index, row in enumerate(values[1:], start=2):
        cleaned = [clean_sheet_value(cell) for cell in row[:len(SUBSCRIPTIONS_HEADERS)]]
        if len(cleaned) < len(SUBSCRIPTIONS_HEADERS):
            cleaned.extend([''] * (len(SUBSCRIPTIONS_HEADERS) - len(cleaned)))
        cleaned[2] = normalize_subscription_channel_id(cleaned[2])
        updates.append({
            'range': f'A{row_index}:{column_letter(len(SUBSCRIPTIONS_HEADERS))}{row_index}',
            'values': [cleaned],
        })

    for i in range(0, len(updates), config.BATCH_SIZE):
        worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
        time.sleep(0.2)

    return len(updates)


def normalize_subscriptions_columns(sheet, worksheet, headers):
    """Move Projects/Project Count to the front with column moves so formatting follows."""
    stripped = [str(cell).strip() for cell in headers]
    desired = SUBSCRIPTIONS_HEADERS
    if stripped[:len(desired)] == desired:
        return stripped

    try:
        projects_index = stripped.index('Projects')
        count_index = stripped.index('Project Count')
    except ValueError:
        worksheet.update(
            range_name=f'A1:{column_letter(len(desired))}1',
            values=[desired],
            value_input_option='USER_ENTERED',
        )
        return desired

    if projects_index == 0 and count_index == 1:
        worksheet.update(
            range_name=f'A1:{column_letter(len(desired))}1',
            values=[desired],
            value_input_option='USER_ENTERED',
        )
        return desired

    if count_index == projects_index + 1:
        sheet.batch_update({
            'requests': [{
                'moveDimension': {
                    'source': {
                        'sheetId': worksheet.id,
                        'dimension': 'COLUMNS',
                        'startIndex': projects_index,
                        'endIndex': count_index + 1,
                    },
                    'destinationIndex': 0,
                }
            }]
        })
    else:
        sheet.batch_update({
            'requests': [
                {
                    'moveDimension': {
                        'source': {
                            'sheetId': worksheet.id,
                            'dimension': 'COLUMNS',
                            'startIndex': count_index,
                            'endIndex': count_index + 1,
                        },
                        'destinationIndex': 0,
                    }
                },
                {
                    'moveDimension': {
                        'source': {
                            'sheetId': worksheet.id,
                            'dimension': 'COLUMNS',
                            'startIndex': projects_index + 1 if projects_index < count_index else projects_index,
                            'endIndex': projects_index + 2 if projects_index < count_index else projects_index + 1,
                        },
                        'destinationIndex': 0,
                    }
                },
            ]
        })

    worksheet.update(
        range_name=f'A1:{column_letter(len(desired))}1',
        values=[desired],
        value_input_option='USER_ENTERED',
    )
    print("  ↔️  Moved subscription Projects columns to the front")
    return desired

def get_or_create_subscriptions_worksheet(sheet):
    try:
        worksheet = sheet.worksheet(SUBSCRIPTIONS_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(SUBSCRIPTIONS_SHEET_NAME, rows=10000, cols=len(SUBSCRIPTIONS_HEADERS))
        worksheet.append_row(SUBSCRIPTIONS_HEADERS, value_input_option='USER_ENTERED')
        return worksheet

    values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
    if not values:
        worksheet.append_row(SUBSCRIPTIONS_HEADERS, value_input_option='USER_ENTERED')
        return worksheet

    headers = [str(cell).strip() for cell in values[0]]
    normalize_subscriptions_columns(sheet, worksheet, headers)

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

def format_channel_projects(active_channels_dict, channel_id):
    projects = sorted(set(active_channels_dict.get(channel_id, {}).get('projects', [])))
    return ', '.join(projects)

def save_subscribed_channels_batch(sheet, channel_ids, active_channels_dict):
    """Сохранение подписок на каналы"""
    worksheet = get_or_create_subscriptions_worksheet(sheet)
    
    timestamp = format_timestamp()
    rows = [
        [
            format_channel_projects(active_channels_dict, channel_id),
            len(set(active_channels_dict.get(channel_id, {}).get('projects', []))),
            channel_id,
            sheet_datetime_value(timestamp),
            sheet_datetime_value(timestamp),
            '✅ subscribed',
        ]
        for channel_id in channel_ids
    ]
    
    if rows:
        worksheet.append_rows(rows, value_input_option='USER_ENTERED')

def update_subscription_renewals_batch(sheet, subscription_records, channel_ids):
    """Обновление времени продления существующих подписок"""
    if not channel_ids:
        return

    try:
        worksheet = get_or_create_subscriptions_worksheet(sheet)
        timestamp = format_timestamp()
        updates = []

        for channel_id in channel_ids:
            record = subscription_records.get(channel_id)
            if not record:
                continue

            updates.append({
                'range': f'E{record["row_index"]}',
                'values': [[sheet_datetime_value(timestamp)]]
            })
            updates.append({
                'range': f'F{record["row_index"]}',
                'values': [['✅ renewed']]
            })

        if updates:
            worksheet.batch_update(updates, value_input_option='USER_ENTERED')
    except Exception as e:
        print(f"  ⚠️  Error updating subscription renewals: {e}")


def update_subscription_statuses(sheet, subscription_records, status_by_channel):
    if not status_by_channel:
        return
    try:
        worksheet = get_or_create_subscriptions_worksheet(sheet)
        updates = []
        red_rows = []
        clear_rows = []
        for channel_id, status in status_by_channel.items():
            record = subscription_records.get(channel_id)
            if not record:
                continue
            row_index = record['row_index']
            updates.append({'range': f'F{row_index}', 'values': [[status]]})
            if str(status).startswith('❌'):
                red_rows.append(row_index)
            else:
                clear_rows.append(row_index)

        for i in range(0, len(updates), config.BATCH_SIZE):
            worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
            time.sleep(0.2)

        requests = []
        for row_index in red_rows:
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': row_index - 1,
                        'endRowIndex': row_index,
                        'startColumnIndex': 2,
                        'endColumnIndex': 3,
                    },
                    'cell': {'userEnteredFormat': {'backgroundColor': {'red': 1.0, 'green': 0.8, 'blue': 0.8}}},
                    'fields': 'userEnteredFormat.backgroundColor',
                }
            })
        for row_index in clear_rows:
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': row_index - 1,
                        'endRowIndex': row_index,
                        'startColumnIndex': 2,
                        'endColumnIndex': 3,
                    },
                    'cell': {'userEnteredFormat': {}},
                    'fields': 'userEnteredFormat.backgroundColor',
                }
            })
        for i in range(0, len(requests), config.BATCH_SIZE):
            sheet.batch_update({'requests': requests[i:i + config.BATCH_SIZE]})
            time.sleep(0.2)
    except Exception as e:
        print(f"  ⚠️  Error updating subscription statuses: {e}")


def split_project_names(projects_text):
    return {
        name.strip()
        for name in str(projects_text or '').split(',')
        if name.strip()
    }


def update_subscription_inventory_warnings(sheet, subscription_records, failed_project_names):
    statuses = {}
    if not failed_project_names:
        for channel_id, record in subscription_records.items():
            if str(record.get('status', '')).startswith('⚠️ project read failed'):
                statuses[channel_id] = '✅ project read ok'
        if statuses:
            update_subscription_statuses(sheet, subscription_records, statuses)
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
        update_subscription_statuses(sheet, subscription_records, statuses)
        print(f"  ⚠️  Marked subscription inventory warnings: {len(statuses)}")


def update_subscription_project_links(sheet, subscription_records, active_channels_dict):
    try:
        worksheet = get_or_create_subscriptions_worksheet(sheet)
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
                row_updates.append({'range': f'C{record["row_index"]}', 'values': [[channel_id]]})

            if record.get('projects') != projects_text or str(record.get('project_count')) != str(project_count):
                row_updates.extend([
                    {'range': f'A{record["row_index"]}', 'values': [[projects_text]]},
                    {'range': f'B{record["row_index"]}', 'values': [[project_count]]},
                ])

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
        indexes = {header: index for index, header in enumerate(headers)}
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
        if deleted:
            print(f"  🧹 Removed duplicate subscription rows: {deleted}")
        return deleted
    except Exception as e:
        print(f"  ⚠️  Error deduplicating subscriptions: {type(e).__name__}: {e}")
        return 0

def subscribe_channel(channel_id):
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
        return response.status_code in [202, 204]
    except:
        return False

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
            indexes = {header: index for index, header in enumerate(headers)}
            channel_col = indexes.get('Channel ID', 2)
            if len(row) > channel_col and normalize_subscription_channel_id(row[channel_col]) in channel_ids:
                rows_to_delete.append(i + 1)
        
        delete_rows_batch(sheet, worksheet, rows_to_delete)
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
    update_subscription_inventory_warnings(master_sheet, subscription_records, failed_project_names)
    subscribed_channels = set(subscription_records.keys())
    to_subscribe = active_channels - subscribed_channels
    to_unsubscribe = set() if not inventory_complete else subscribed_channels - active_channels

    if not should_run_subscription_sync(master_sheet, force=force):
        print("  ⏭️  Subscribe/renew skipped (last full sync < 24h)")
        if to_subscribe:
            print(f"  Subscribing to {len(to_subscribe)} new channels despite recent full sync...")
            subscribed = []
            for channel_id in to_subscribe:
                if subscribe_channel(channel_id):
                    subscribed.append(channel_id)
                time.sleep(0.1)

            if subscribed:
                save_subscribed_channels_batch(master_sheet, subscribed, active_channels_dict)
                print(f"  ✅ Successfully subscribed: {len(subscribed)}")

        if to_unsubscribe:
            print(f"  Unsubscribing/removing {len(to_unsubscribe)} inactive subscriptions...")
            for channel_id in to_unsubscribe:
                unsubscribe_channel(channel_id)
                time.sleep(0.1)
            remove_subscribed_channels(master_sheet, to_unsubscribe)
        subscription_records = get_subscription_records(master_sheet)
        if subscription_records is None:
            print("  ⚠️  Project link update skipped: could not re-read subscriptions")
            result.update({'ok': False, 'partial': True, 'reason': 'could not re-read subscriptions'})
            return result
        update_subscription_project_links(master_sheet, subscription_records, active_channels_dict)
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
        for channel_id in to_renew:
            if subscribe_channel(channel_id):
                renewed.append(channel_id)
            time.sleep(0.1)

        if renewed:
            update_subscription_renewals_batch(master_sheet, subscription_records, renewed)
            update_subscription_statuses(
                master_sheet,
                subscription_records,
                {channel_id: '✅ renewed' for channel_id in renewed},
            )
            print(f"  ✅ Successfully renewed: {len(renewed)}")
        failed_renewals = sorted(set(to_renew) - set(renewed))
        if failed_renewals:
            update_subscription_statuses(
                master_sheet,
                subscription_records,
                {channel_id: '❌ subscribe/renew failed' for channel_id in failed_renewals},
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

    if inventory_complete:
        update_subscription_sync_state(master_sheet)
    else:
        print("  ⚠️  Full subscription sync timestamp not updated because channel inventory was incomplete")

    return result
