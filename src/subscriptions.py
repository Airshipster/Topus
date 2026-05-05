import time
from datetime import datetime, timedelta

import requests

import config
from sheets import get_all_active_channels


SUBSCRIPTION_SYNC_SETTING = 'last_subscription_sync'
SUBSCRIPTION_SYNC_INTERVAL_SECONDS = 86400


def get_subscription_sync_state(sheet):
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()

        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == SUBSCRIPTION_SYNC_SETTING:
                last_sync = None
                if len(row) > 1 and row[1]:
                    try:
                        last_sync = datetime.fromisoformat(row[1].replace('Z', ''))
                    except:
                        pass
                return {
                    'row_index': i + 1,
                    'last_sync': last_sync,
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
        state = get_subscription_sync_state(sheet)
        timestamp = datetime.utcnow().isoformat() + 'Z'

        if state.get('row_index'):
            worksheet.update_cell(state['row_index'], 2, timestamp)
        else:
            worksheet.append_row([
                SUBSCRIPTION_SYNC_SETTING,
                timestamp,
                'Последняя полная синхронизация YouTube push-подписок',
            ])
    except Exception as e:
        print(f"  ⚠️  Error updating subscription sync state: {e}")


def get_subscribed_channels(sheet):
    """Получение списка подписанных каналов"""
    try:
        worksheet = sheet.worksheet('Подписки')
        records = worksheet.get_all_records()
        return set(row.get('Channel ID', '') for row in records if row.get('Channel ID'))
    except:
        return set()

def get_subscription_records(sheet):
    """Получение подписок вместе со строками и датой обновления"""
    try:
        worksheet = sheet.worksheet('Подписки')
        values = worksheet.get_all_values()
        records = {}

        for i, row in enumerate(values):
            if i == 0:
                continue

            channel_id = row[0].strip() if len(row) > 0 else ''
            if not channel_id:
                continue

            records[channel_id] = {
                'row_index': i + 1,
                'last_renewed': row[2].strip() if len(row) > 2 else ''
            }

        return records
    except:
        return {}

def parse_subscription_date(value):
    """Парсинг дат подписок из таблицы"""
    if not value:
        return None

    try:
        return datetime.fromisoformat(value.replace('Z', ''))
    except:
        return None

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

def save_subscribed_channels_batch(sheet, channel_ids):
    """Сохранение подписок на каналы"""
    try:
        worksheet = sheet.worksheet('Подписки')
    except:
        worksheet = sheet.add_worksheet('Подписки', rows=5000, cols=3)
        worksheet.append_row(['Channel ID', 'Subscribed At', 'Last Renewed'])
    
    timestamp = datetime.utcnow().isoformat()
    rows = [[channel_id, timestamp, timestamp] for channel_id in channel_ids]
    
    if rows:
        worksheet.append_rows(rows)

def update_subscription_renewals_batch(sheet, subscription_records, channel_ids):
    """Обновление времени продления существующих подписок"""
    if not channel_ids:
        return

    try:
        worksheet = sheet.worksheet('Подписки')
        timestamp = datetime.utcnow().isoformat()
        updates = []

        for channel_id in channel_ids:
            record = subscription_records.get(channel_id)
            if not record:
                continue

            updates.append({
                'range': f'C{record["row_index"]}',
                'values': [[timestamp]]
            })

        if updates:
            worksheet.batch_update(updates)
    except Exception as e:
        print(f"  ⚠️  Error updating subscription renewals: {e}")

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
        worksheet = sheet.worksheet('Подписки')
        all_values = worksheet.get_all_values()
        
        rows_to_delete = []
        for i, row in enumerate(all_values):
            if i == 0:
                continue
            if len(row) > 0 and row[0] in channel_ids:
                rows_to_delete.append(i + 1)
        
        for row_index in sorted(rows_to_delete, reverse=True):
            worksheet.delete_rows(row_index)
    except Exception as e:
        print(f"  ❌ Error removing subscriptions: {e}")

def sync_subscriptions(client, master_sheet, projects, force=False):
    """Синхронизация push-подписок"""
    print("\n📡 Syncing subscriptions...")

    if not should_run_subscription_sync(master_sheet, force=force):
        print("  ⏭️  Subscription sync skipped (last full sync < 24h)")
        return

    if force:
        print("  🔁 Forced subscription sync requested")
    
    active_channels_dict = get_all_active_channels(client, projects)
    active_channels = set(active_channels_dict.keys())
    subscription_records = get_subscription_records(master_sheet)
    subscribed_channels = set(subscription_records.keys())
    stale_channels = get_stale_subscriptions(subscription_records, active_channels)
    
    to_subscribe = active_channels - subscribed_channels
    to_renew = active_channels & stale_channels
    to_unsubscribe = subscribed_channels - active_channels
    
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
            save_subscribed_channels_batch(master_sheet, subscribed)
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
            print(f"  ✅ Successfully renewed: {len(renewed)}")
    
    if len(to_unsubscribe) > 0:
        print(f"  Unsubscribing from {len(to_unsubscribe)} inactive channels...")
        unsubscribed = []
        for channel_id in to_unsubscribe:
            if unsubscribe_channel(channel_id):
                unsubscribed.append(channel_id)
            time.sleep(0.1)
        
        if unsubscribed:
            remove_subscribed_channels(master_sheet, unsubscribed)
            print(f"  ✅ Successfully unsubscribed: {len(unsubscribed)}")
    
    if len(to_subscribe) == 0 and len(to_renew) == 0 and len(to_unsubscribe) == 0:
        print("  ✅ No changes needed")

    update_subscription_sync_state(master_sheet)
