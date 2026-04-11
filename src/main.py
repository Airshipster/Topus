import gspread
import requests
import json
import time
import re
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from config import *


youtube_api_calls = 0


def authenticate_google_sheets():
    """Аутентификация в Google Sheets через Service Account"""
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not found")
    
    credentials_dict = json.loads(SERVICE_ACCOUNT_JSON)
    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    client = gspread.authorize(credentials)
    return client


def acquire_lock(sheet):
    """Получить блокировку для предотвращения одновременных запусков"""
    try:
        worksheet = sheet.worksheet(SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        
        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == 'lock_status':
                lock_row = i + 1
                lock_value = row[1].strip() if len(row) > 1 else ''
                
                if lock_value == 'locked':
                    lock_time_str = row[2] if len(row) > 2 else ''
                    if lock_time_str:
                        try:
                            lock_time = datetime.fromisoformat(lock_time_str.replace('Z', ''))
                            if (datetime.utcnow() - lock_time).total_seconds() > 900:
                                print("  ⚠️  Stale lock detected (>15min), removing...")
                            else:
                                print("  ❌ Another process is running! Exiting...")
                                return False
                        except:
                            pass
                
                current_time = datetime.utcnow().isoformat() + 'Z'
                worksheet.update_cell(lock_row, 2, 'locked')
                worksheet.update_cell(lock_row, 3, current_time)
                print(f"  🔒 Lock acquired at {current_time}")
                return True
        
        current_time = datetime.utcnow().isoformat() + 'Z'
        worksheet.append_row(['lock_status', 'locked', current_time])
        print(f"  🔒 Lock created and acquired")
        return True
        
    except Exception as e:
        print(f"  ❌ Error acquiring lock: {e}")
        return False


def release_lock(sheet):
    """Освободить блокировку"""
    try:
        worksheet = sheet.worksheet(SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        
        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == 'lock_status':
                lock_row = i + 1
                worksheet.update_cell(lock_row, 2, 'unlocked')
                worksheet.update_cell(lock_row, 3, datetime.utcnow().isoformat() + 'Z')
                print(f"  🔓 Lock released")
                return
    except Exception as e:
        print(f"  ⚠️  Error releasing lock: {e}")


def save_videos_batch(sheet, videos_data):
    """
    БАТЧЕВОЕ сохранение видео в таблицу с защитой от потери
    videos_data = [(video, project, pub_date, tg_msg_id, error), ...]
    Возвращает список video_id которые удалось сохранить
    """
    if not videos_data:
        return []
    
    try:
        try:
            worksheet = sheet.worksheet(SHEET_NAME_VIDEOS)
        except:
            worksheet = sheet.add_worksheet(SHEET_NAME_VIDEOS, rows=10000, cols=13)
            headers = [
                'Video ID', 'Название видео', 'Ссылка', 'Название канала', 
                'Channel ID', 'Проект', 'Дата публикации UTC', 'Дата обработки UTC',
                'Опубл. в TG', 'TG message_id', 'Дата публикации TG', 
                'Системный статус', 'Ошибка'
            ]
            worksheet.append_row(headers)
            print(f"  📋 Created 'Глобальные видео' worksheet")
        
        rows = []
        for video, project, video_published_date, tg_message_id, error in videos_data:
            rows.append([
                video['video_id'],
                video.get('title', ''),
                video.get('url', ''),
                video.get('channel', ''),
                video.get('channel_id', ''),
                project.get('name', ''),
                video_published_date,
                datetime.utcnow().isoformat(),
                '1' if tg_message_id else '0',
                str(tg_message_id) if tg_message_id else '',
                datetime.utcnow().isoformat() if tg_message_id else '',
                'published' if tg_message_id else 'pending',
                error or ''
            ])
        
        saved_video_ids = []
        
        # Дробим на батчи по BATCH_SIZE
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i+BATCH_SIZE]
            batch_video_ids = [vd[0]['video_id'] for vd in videos_data[i:i+BATCH_SIZE]]
            
            try:
                worksheet.append_rows(batch, value_input_option='USER_ENTERED')
                saved_video_ids.extend(batch_video_ids)
                print(f"  💾 Saved batch {i//BATCH_SIZE + 1}: {len(batch)} videos")
            except Exception as e:
                print(f"  ⚠️  Batch failed, saving one by one: {e}")
                # Если батч упал - сохраняем по одной (защита от потери)
                for row, video_id in zip(batch, batch_video_ids):
                    try:
                        worksheet.append_row(row, value_input_option='USER_ENTERED')
                        saved_video_ids.append(video_id)
                    except Exception as e2:
                        print(f"  ❌ Failed to save {video_id}: {e2}")
            
            time.sleep(0.5)  # Небольшая пауза между батчами
        
        return saved_video_ids
        
    except Exception as e:
        print(f"  ❌ Critical error in save_videos_batch: {e}")
        return []


def log_events_batch(sheet, log_entries):
    """Пакетная запись логов"""
    if not log_entries:
        return
    
    try:
        try:
            worksheet = sheet.worksheet('Логи')
        except:
            worksheet = sheet.add_worksheet('Логи', rows=10000, cols=6)
            worksheet.append_row(['Timestamp', 'Проект', 'Событие', 'Video ID', 'Детали', 'Статус'])
        
        # Дробим на батчи
        for i in range(0, len(log_entries), BATCH_SIZE):
            batch = log_entries[i:i+BATCH_SIZE]
            try:
                worksheet.append_rows(batch, value_input_option='USER_ENTERED')
            except:
                # Fallback - по одной
                for entry in batch:
                    try:
                        worksheet.append_row(entry)
                    except:
                        pass
        
        print(f"  📝 Logged {len(log_entries)} events")
    except Exception as e:
        print(f"  ⚠️  Error batch logging: {e}")


def cleanup_old_records(sheet):
    """Очистка старых записей (>7 дней)"""
    try:
        worksheet_settings = sheet.worksheet(SHEET_NAME_SETTINGS)
        values = worksheet_settings.get_all_values()
        
        last_cleanup = None
        last_cleanup_row = None
        
        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == 'last_cleanup':
                last_cleanup_row = i + 1
                if len(row) > 1 and row[1]:
                    try:
                        last_cleanup = datetime.fromisoformat(row[1].replace('Z', ''))
                    except:
                        pass
                break
        
        if last_cleanup and (datetime.utcnow() - last_cleanup).total_seconds() < 86400:
            print(f"  ⏭️  Cleanup skipped (last run: {last_cleanup.isoformat()}Z)")
            return
        
        print("\n🧹 Cleaning up old records...")
        
        cutoff_date = datetime.utcnow() - timedelta(days=CLEANUP_AFTER_DAYS)
        print(f"  Removing records older than: {cutoff_date.isoformat()}Z")
        
        deleted_videos = 0
        try:
            worksheet = sheet.worksheet(SHEET_NAME_VIDEOS)
            values = worksheet.get_all_values()
            
            rows_to_delete = []
            for i, row in enumerate(values):
                if i == 0:
                    continue
                
                if len(row) > 7:
                    date_str = row[7]
                    try:
                        record_date = datetime.fromisoformat(date_str.replace('Z', ''))
                        if record_date < cutoff_date:
                            rows_to_delete.append(i + 1)
                    except:
                        pass
            
            for row_index in sorted(rows_to_delete, reverse=True):
                worksheet.delete_rows(row_index)
                deleted_videos += 1
            
            if deleted_videos > 0:
                print(f"  ✅ Deleted {deleted_videos} old videos")
        except Exception as e:
            print(f"  ⚠️  Error cleaning videos: {e}")
        
        deleted_logs = 0
        try:
            worksheet = sheet.worksheet('Логи')
            values = worksheet.get_all_values()
            
            rows_to_delete = []
            for i, row in enumerate(values):
                if i == 0:
                    continue
                
                if len(row) > 0:
                    date_str = row[0]
                    try:
                        record_date = datetime.fromisoformat(date_str.replace('Z', ''))
                        if record_date < cutoff_date:
                            rows_to_delete.append(i + 1)
                    except:
                        pass
            
            for row_index in sorted(rows_to_delete, reverse=True):
                worksheet.delete_rows(row_index)
                deleted_logs += 1
            
            if deleted_logs > 0:
                print(f"  ✅ Deleted {deleted_logs} old logs")
        except Exception as e:
            print(f"  ⚠️  Error cleaning logs: {e}")
        
        current_time = datetime.utcnow().isoformat() + 'Z'
        if last_cleanup_row:
            worksheet_settings.update_cell(last_cleanup_row, 2, current_time)
        else:
            worksheet_settings.append_row(['last_cleanup', current_time, 'Последняя очистка старых записей'])
        
        print(f"  ✅ Cleanup completed: {deleted_videos} videos, {deleted_logs} logs")
        
    except Exception as e:
        print(f"  ❌ Cleanup error: {e}")


def load_settings(sheet):
    """Загрузка настроек"""
    try:
        worksheet = sheet.worksheet(SHEET_NAME_SETTINGS)
        records = worksheet.get_all_records()
        
        settings = {}
        for row in records:
            key = row.get('Параметр', '')
            value = row.get('Значение', '')
            
            if isinstance(key, str):
                key = key.strip()
            else:
                key = str(key).strip()
            
            if isinstance(value, str):
                value = value.strip()
            else:
                value = str(value).strip()
            
            if key and value:
                settings[key] = value
        
        global YOUTUBE_API_KEY, MAX_VIDEO_AGE_HOURS, DEFAULT_MESSAGE_TEMPLATE
        if 'youtube_api_key' in settings:
            YOUTUBE_API_KEY = settings['youtube_api_key']
            print(f"  ✅ YouTube API key loaded")
        
        if 'max_video_age_hours' in settings:
            MAX_VIDEO_AGE_HOURS = int(settings['max_video_age_hours'])
            print(f"  ✅ Max video age: {MAX_VIDEO_AGE_HOURS}h ({MAX_VIDEO_AGE_HOURS//24}d)")
        
        if 'default_template' in settings:
            DEFAULT_MESSAGE_TEMPLATE = settings['default_template']
            print(f"  ✅ Default template loaded")
        
        return settings
    except Exception as e:
        print(f"  ❌ Error loading settings: {e}")
        return {}


def update_youtube_quota(sheet, calls_used):
    """Обновление счётчика YouTube API квоты"""
    if calls_used == 0:
        return
    
    try:
        worksheet = sheet.worksheet(SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        
        quota_row = None
        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == 'youtube_quota_used':
                quota_row = i + 1
                current_quota = int(row[1]) if (len(row) > 1 and row[1].isdigit()) else 0
                new_quota = current_quota + calls_used
                worksheet.update_cell(quota_row, 2, str(new_quota))
                print(f"  📊 YouTube API quota updated: {current_quota} + {calls_used} = {new_quota}")
                return
        
        if quota_row is None:
            worksheet.append_row(['youtube_quota_used', str(calls_used), 'Счётчик использованных единиц YouTube API'])
            print(f"  ✅ Created 'youtube_quota_used' with value: {calls_used}")
            
    except Exception as e:
        print(f"  ❌ Error updating quota: {e}")


def update_last_run(sheet):
    """Обновление времени последнего запуска"""
    try:
        worksheet = sheet.worksheet(SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        
        last_run_row = None
        current_time = datetime.utcnow().isoformat() + 'Z'
        
        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == 'last_run':
                last_run_row = i + 1
                worksheet.update_cell(last_run_row, 2, current_time)
                print(f"  🕐 Last run updated: {current_time}")
                return
        
        if last_run_row is None:
            worksheet.append_row(['last_run', current_time, 'Последний запуск обработки'])
            print(f"  ✅ Created 'last_run': {current_time}")
            
    except Exception as e:
        print(f"  ❌ Error updating last_run: {e}")


def load_projects(sheet):
    """Загрузка активных проектов"""
    worksheet = sheet.worksheet(SHEET_NAME_PROJECTS)
    records = worksheet.get_all_records()
    
    projects = []
    for row in records:
        status = row.get('Активен', '')
        if status == '🟢':
            sheet_url = row.get('Ссылка на документ проекта', '')
            sheet_id = ''
            if '/d/' in sheet_url:
                sheet_id = sheet_url.split('/d/')[1].split('/')[0]
            
            stop_words_str = row.get('Стоп-слова (через запятую)', '').strip()
            stop_words = [w.strip().lower() for w in stop_words_str.split(',') if w.strip()] if stop_words_str else []
            
            tg_channel = row.get('Telegram канал', '').strip()
            
            projects.append({
                'code': row.get('Код проекта'),
                'name': row.get('Название'),
                'sheet_id': sheet_id,
                'bot_token': row.get('Telegram bot token'),
                'channel_id': str(row.get('Telegram канал ID')),
                'tg_channel': tg_channel,
                'default_template': row.get('Шаблон по умолчанию', DEFAULT_MESSAGE_TEMPLATE),
                'stop_words': stop_words
            })
    
    print(f"  ✅ Loaded {len(projects)} active projects")
    return projects


def load_youtube_channels(client, project):
    """Загрузка активных YouTube каналов проекта"""
    try:
        sheet = client.open_by_key(project['sheet_id'])
        worksheet = sheet.worksheet('Список. YouTube')
        
        values = worksheet.get_all_values()
        
        channels = {}
        for i, row in enumerate(values):
            if i == 0:
                continue
            
            if len(row) < 8:
                continue
            
            status = row[6].strip() if len(row) > 6 else ''
            
            if status == '🔵':
                break
            
            if status == '🟢':
                channel_id = row[4].strip() if len(row) > 4 else ''
                channel_name = row[3].strip() if len(row) > 3 else ''
                channel_template = row[20].strip() if len(row) > 20 else ''
                tg_channel_link = row[21].strip() if len(row) > 21 else ''
                
                if channel_id and channel_id.startswith('UC'):
                    channels[channel_id] = {
                        'name': channel_name,
                        'template': channel_template,
                        'tg_channel': tg_channel_link
                    }
        
        return channels
    except Exception as e:
        print(f"  ❌ Error loading channels for {project['name']}: {e}")
        return {}


def get_all_active_channels(client, projects):
    """Получение всех уникальных активных каналов"""
    all_channels = {}
    
    for project in projects:
        channels = load_youtube_channels(client, project)
        for ch_id, ch_info in channels.items():
            if ch_id not in all_channels:
                all_channels[ch_id] = ch_info
    
    return all_channels


def get_subscribed_channels(sheet):
    """Получение списка подписанных каналов"""
    try:
        worksheet = sheet.worksheet('Подписки')
        records = worksheet.get_all_records()
        return set(row.get('Channel ID', '') for row in records if row.get('Channel ID'))
    except:
        return set()


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


def subscribe_channel(channel_id):
    """Подписка на push-уведомления"""
    hub_url = "https://pubsubhubbub.appspot.com/subscribe"
    topic_url = f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={channel_id}"
    
    data = {
        'hub.callback': CALLBACK_URL,
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
        'hub.callback': CALLBACK_URL,
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


def get_video_info_from_api(video_id):
    """Получение информации о видео через YouTube Data API v3"""
    global youtube_api_calls
    
    if not YOUTUBE_API_KEY:
        return None
    
    try:
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            'part': 'snippet,contentDetails,liveStreamingDetails',
            'id': video_id,
            'key': YOUTUBE_API_KEY
        }
        
        response = requests.get(url, params=params, timeout=10)
        youtube_api_calls += 1
        
        if response.status_code != 200:
            return None
        
        data = response.json()
        if not data.get('items'):
            return None
        
        item = data['items'][0]
        snippet = item['snippet']
        content_details = item.get('contentDetails', {})
        live_details = item.get('liveStreamingDetails', {})
        
        is_short = False
        duration_seconds = 0
        duration_str = content_details.get('duration', '')
        if duration_str:
            match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
            if match:
                hours = int(match.group(1) or 0)
                minutes = int(match.group(2) or 0)
                seconds = int(match.group(3) or 0)
                duration_seconds = hours * 3600 + minutes * 60 + seconds
                if duration_seconds <= 60:
                    is_short = True
        
        is_live = live_details.get('actualStartTime') is not None
        is_upcoming = snippet.get('liveBroadcastContent') == 'upcoming'
        
        return {
            'title': snippet['title'],
            'channel': snippet['channelTitle'],
            'channel_id': snippet['channelId'],
            'published': snippet['publishedAt'],
            'is_short': is_short,
            'is_live': is_live,
            'is_upcoming': is_upcoming,
            'duration': duration_str,
            'duration_seconds': duration_seconds
        }
    except Exception as e:
        return None


def check_rss_feed(channel_id):
    """Проверка RSS фида канала"""
    try:
        time.sleep(0.2)
        
        url = f"{CLOUDFLARE_WORKER_URL}/?channel={channel_id}"
        response = requests.get(url, timeout=15)
        
        if response.status_code != 200:
            return []
        
        if len(response.content) == 0:
            return []
        
        from xml.etree import ElementTree as ET
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError:
            return []
        
        ns = {
            'atom': 'http://www.w3.org/2005/Atom',
            'yt': 'http://www.youtube.com/xml/schemas/2015'
        }
        
        entries = root.findall('atom:entry', ns)
        
        videos = []
        cutoff_time = datetime.utcnow() - timedelta(hours=MAX_VIDEO_AGE_HOURS)
        
        for entry in entries:
            video_id_elem = entry.find('yt:videoId', ns)
            title_elem = entry.find('atom:title', ns)
            published_elem = entry.find('atom:published', ns)
            author_elem = entry.find('atom:author/atom:name', ns)
            
            if video_id_elem is None or title_elem is None or published_elem is None:
                continue
            
            if not video_id_elem.text or not title_elem.text or not published_elem.text:
                continue
            
            video_id = video_id_elem.text
            title = title_elem.text
            published_str = published_elem.text
            channel_name = author_elem.text if author_elem is not None and author_elem.text else 'Unknown'
            
            try:
                if published_str.endswith('Z'):
                    published = datetime.fromisoformat(published_str.replace('Z', '+00:00')).replace(tzinfo=None)
                else:
                    published = datetime.fromisoformat(published_str).replace(tzinfo=None)
            except:
                continue
            
            if published > cutoff_time:
                videos.append({
                    'video_id': video_id,
                    'title': title,
                    'url': f"https://www.youtube.com/watch?v={video_id}",
                    'channel': channel_name,
                    'channel_id': channel_id,
                    'published': published.isoformat()
                })
        
        return videos
    except:
        return []


def sync_subscriptions(client, master_sheet, projects):
    """Синхронизация push-подписок"""
    print("\n📡 Syncing subscriptions...")
    
    active_channels_dict = get_all_active_channels(client, projects)
    active_channels = set(active_channels_dict.keys())
    subscribed_channels = get_subscribed_channels(master_sheet)
    
    to_subscribe = active_channels - subscribed_channels
    to_unsubscribe = subscribed_channels - active_channels
    
    print(f"  Active channels: {len(active_channels)}")
    print(f"  Already subscribed: {len(subscribed_channels)}")
    print(f"  New to subscribe: {len(to_subscribe)}")
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
    
    if len(to_subscribe) == 0 and len(to_unsubscribe) == 0:
        print("  ✅ No changes needed")


def rss_fallback_check(client, project, published_videos):
    """RSS fallback для конкретного проекта"""
    print(f"\n  📡 RSS fallback for {project['name']}...")
    
    project_channels = load_youtube_channels(client, project)
    
    print(f"    Checking {len(project_channels)} channels")
    print(f"    Time window: {MAX_VIDEO_AGE_HOURS}h ({MAX_VIDEO_AGE_HOURS//24}d)")
    
    new_videos = []
    videos_found_count = 0
    
    for i, (channel_id, channel_info) in enumerate(project_channels.items()):
        videos = check_rss_feed(channel_id)
        videos_found_count += len(videos)
        
        if i > 0 and i % 10 == 0:
            print(f"    Progress: {i}/{len(project_channels)} channels (Found: {videos_found_count} videos, New: {len(new_videos)})")
        
        for video in videos:
            if video['video_id'] not in published_videos:
                video['project'] = project
                video['channel_info'] = channel_info
                new_videos.append(video)
    
    print(f"    ✅ RSS scan complete: {videos_found_count} videos total, {len(new_videos)} new")
    
    return new_videos


def get_published_videos(sheet):
    """Получение списка уже опубликованных видео"""
    try:
        worksheet = sheet.worksheet(SHEET_NAME_VIDEOS)
        records = worksheet.get_all_records()
        published = set(row.get('Video ID', '') for row in records if row.get('Video ID'))
        print(f"  📋 Found {len(published)} published videos in table")
        return published
    except Exception as e:
        print(f"  ⚠️  Error loading published videos: {e}")
        return set()


def get_push_events(sheet):
    """Получение необработанных push-событий"""
    try:
        worksheet = sheet.worksheet(SHEET_NAME_PUSH_EVENTS)
        values = worksheet.get_all_values()
        
        events = []
        for i, row in enumerate(values):
            if i == 0:
                continue
            
            if len(row) < 4:
                continue
            
            status = row[3] if len(row) > 3 else ''
            if status == '' or status == '❌':
                video_id = row[1] if len(row) > 1 else ''
                channel_id = row[2] if len(row) > 2 else ''
                if video_id and channel_id:
                    events.append({
                        'row_index': i + 1,
                        'video_id': video_id,
                        'channel_id': channel_id
                    })
        
        return events
    except Exception as e:
        print(f"❌ Error loading push events: {e}")
        return []


def mark_push_event_processed(sheet, row_index, project_name):
    """Отметка push-события как обработанного"""
    try:
        worksheet = sheet.worksheet(SHEET_NAME_PUSH_EVENTS)
        worksheet.update_cell(row_index, 4, '✅')
        
        current_projects = worksheet.cell(row_index, 5).value or ''
        if project_name not in current_projects:
            new_projects = (current_projects + ', ' + project_name).strip(', ')
            worksheet.update_cell(row_index, 5, new_projects)
    except Exception as e:
        print(f"  ⚠️  Error marking event: {e}")


def should_filter_video(video_info, project):
    """Проверка нужно ли фильтровать видео"""
    if not video_info:
        return False, ""
    
    if FILTER_SHORTS and video_info.get('is_short'):
        return True, f"Short video ({video_info.get('duration_seconds', 0)}s)"
    
    if FILTER_LIVE and video_info.get('is_live'):
        return True, "Live stream"
    
    if video_info.get('is_upcoming'):
        return True, "Upcoming/Premiere"
    
    if project.get('stop_words'):
        title_lower = video_info['title'].lower()
        for stop_word in project['stop_words']:
            if stop_word and stop_word in title_lower:
                return True, f"Stop word: {stop_word}"
    
    return False, ""


def format_message(template, video, channel_info, project):
    """Форматирование сообщения"""
    if not template:
        template = DEFAULT_MESSAGE_TEMPLATE
    
    channel_name = video.get('channel', channel_info.get('name', 'Unknown'))
    video_title = video['title']
    video_url = video['url']
    
    message = template.replace('{channel_title}', channel_name)
    message = message.replace('{video_title}', video_title)
    message = message.replace('{video_url}', video_url)
    message = message.replace('{video_title_link}', f'<a href="{video_url}">{video_title}</a>')
    
    tg_channel_name = project.get('tg_channel', '')
    message = message.replace('{TG_channel}', tg_channel_name)
    
    tg_channel_link = channel_info.get('tg_channel', '').strip()
    
    if tg_channel_link and not tg_channel_link.startswith('-'):
        def replace_brackets(match):
            text = match.group(1)
            return f'<a href="{tg_channel_link}">{text}</a>'
        
        message = re.sub(r'\[([^\]]+)\]', replace_brackets, message)
    else:
        message = re.sub(r'\[([^\]]+)\]', r'\1', message)
    
    invisible_link = f'<a href="{video_url}">\u200b</a>'
    message = invisible_link + message
    
    return message


def send_to_telegram(bot_token, channel_id, message):
    """Отправка сообщения в Telegram"""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': channel_id,
        'text': message,
        'parse_mode': 'HTML',
        'disable_web_page_preview': False
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        result = response.json()
        return result.get('result', {}).get('message_id')
    except Exception as e:
        print(f"  ❌ Telegram error: {e}")
        return None


def main():
    print("="*60)
    print("TOPUS - YouTube to Telegram Publisher")
    print("="*60)
    print(f"Started: {datetime.utcnow().isoformat()}Z\n")
    
    master_sheet = None
    
    try:
        client = authenticate_google_sheets()
        master_sheet = client.open_by_key(SPREADSHEET_ID)
        
        # ПРОВЕРКА БЛОКИРОВКИ
        if not acquire_lock(master_sheet):
            print("\n❌ Cannot acquire lock. Another process is running. Exiting.")
            return
        
        # Автоочистка старых записей
        cleanup_old_records(master_sheet)
        
        print("\n⚙️  Loading settings...")
        settings = load_settings(master_sheet)
        
        print("\n📂 Loading projects...")
        projects = load_projects(master_sheet)
        
        sync_subscriptions(client, master_sheet, projects)
        
        published_videos = get_published_videos(master_sheet)
        
        push_events = get_push_events(master_sheet)
        print(f"📬 Unprocessed push events: {len(push_events)}")
        
        total_found = 0
        total_published = 0
        total_filtered = 0
        total_failed = 0
        
        # Аккумуляторы для батчевой записи
        videos_to_save = []
        log_entries = []
        
        for project in projects:
            print(f"\n{'='*60}")
            print(f"📁 Project: {project['name']}")
            print(f"{'='*60}")
            
            yt_channels = load_youtube_channels(client, project)
            print(f"  📺 Active channels: {len(yt_channels)}")
            
            # Process push events
            for event in push_events:
                if event['channel_id'] not in yt_channels:
                    continue
                
                if event['video_id'] in published_videos:
                    mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                    continue
                
                total_found += 1
                
                video_info_api = get_video_info_from_api(event['video_id'])
                
                if not video_info_api:
                    mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                    continue
                
                channel_info = yt_channels[event['channel_id']]
                
                video = {
                    'video_id': event['video_id'],
                    'title': video_info_api['title'],
                    'url': f"https://www.youtube.com/watch?v={event['video_id']}",
                    'channel': video_info_api['channel'],
                    'channel_id': event['channel_id']
                }
                
                video_published_date = video_info_api['published']
                
                should_filter, filter_reason = should_filter_video(video_info_api, project)
                if should_filter:
                    print(f"  🚫 Filtered: {video['title'][:50]} ({filter_reason})")
                    timestamp = datetime.utcnow().isoformat()
                    log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], filter_reason, 'filtered'])
                    published_videos.add(video['video_id'])
                    mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                    total_filtered += 1
                    continue
                
                # СНАЧАЛА добавляем в батч для сохранения
                videos_to_save.append((video, project, video_published_date, None, None))
                published_videos.add(video['video_id'])
                
                print(f"  📝 Queued: {video['title'][:50]}...")
                
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
            
            # RSS fallback check
            rss_videos = rss_fallback_check(client, project, published_videos)
            
            for video in rss_videos:
                channel_info = video['channel_info']
                
                total_found += 1
                
                video_info_api = get_video_info_from_api(video['video_id'])
                
                if video_info_api:
                    video_published_date = video_info_api['published']
                    
                    should_filter, filter_reason = should_filter_video(video_info_api, project)
                    
                    if should_filter:
                        print(f"    🚫 Filtered (RSS): {video['title'][:50]} ({filter_reason})")
                        timestamp = datetime.utcnow().isoformat()
                        log_entries.append([timestamp, project['name'], 'Video filtered', video['video_id'], f"RSS: {filter_reason}", 'filtered'])
                        published_videos.add(video['video_id'])
                        total_filtered += 1
                        continue
                else:
                    video_published_date = video.get('published', datetime.utcnow().isoformat())
                
                # СНАЧАЛА добавляем в батч для сохранения
                videos_to_save.append((video, project, video_published_date, None, None))
                published_videos.add(video['video_id'])
                
                print(f"    📝 Queued (RSS): {video['title'][:50]}...")
        
        # СОХРАНЯЕМ ВСЕ ВИДЕО БАТЧАМИ
        print(f"\n💾 Saving {len(videos_to_save)} videos to table...")
        saved_video_ids = save_videos_batch(master_sheet, videos_to_save)
        print(f"  ✅ Saved {len(saved_video_ids)} videos")
        
        # ТЕПЕРЬ ПУБЛИКУЕМ В TELEGRAM
        print(f"\n📤 Publishing to Telegram...")
        videos_with_msg_ids = []
        
        for video, project, video_published_date, _, _ in videos_to_save:
            if video['video_id'] not in saved_video_ids:
                print(f"  ⚠️  Skipping {video['video_id']} - not saved")
                continue
            
            channel_info = video.get('channel_info', {})
            
            template = channel_info.get('template') or project['default_template']
            message = format_message(template, video, channel_info, project)
            
            print(f"  📤 Publishing: {video['title'][:50]}...")
            
            tg_message_id = send_to_telegram(
                project['bot_token'],
                project['channel_id'],
                message
            )
            
            if tg_message_id:
                print(f"    ✅ Published (msg: {tg_message_id})")
                timestamp = datetime.utcnow().isoformat()
                log_entries.append([timestamp, project['name'], 'Video published', video['video_id'], f"Telegram msg: {tg_message_id}", 'success'])
                videos_with_msg_ids.append((video, project, video_published_date, tg_message_id, None))
                total_published += 1
            else:
                print(f"    ❌ Failed to publish")
                timestamp = datetime.utcnow().isoformat()
                log_entries.append([timestamp, project['name'], 'Publish failed', video['video_id'], 'Telegram error', 'error'])
                total_failed += 1
            
            time.sleep(1 / TELEGRAM_RATE_LIMIT)
        
        # ОБНОВЛЯЕМ message_id БАТЧАМИ (если есть)
        if videos_with_msg_ids:
            print(f"\n🔄 Updating {len(videos_with_msg_ids)} message IDs...")
            # TODO: можно оптимизировать через batch update
        
        # СОХРАНЯЕМ ЛОГИ БАТЧЕМ
        if log_entries:
            print(f"\n📝 Saving logs...")
            log_events_batch(master_sheet, log_entries)
        
        # Обновление метаданных
        print("\n📝 Updating metadata...")
        if youtube_api_calls > 0:
            update_youtube_quota(master_sheet, youtube_api_calls)
        update_last_run(master_sheet)
        
        # Final summary
        print(f"\n{'='*60}")
        print("📊 SUMMARY")
        print(f"{'='*60}")
        print(f"Videos found: {total_found}")
        print(f"  ✅ Published: {total_published}")
        print(f"  🚫 Filtered: {total_filtered}")
        print(f"  ❌ Failed: {total_failed}")
        print(f"  📊 YouTube API calls: {youtube_api_calls}")
        print(f"\nFinished: {datetime.utcnow().isoformat()}Z")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\n❌❌❌ FATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        if master_sheet:
            release_lock(master_sheet)


if __name__ == "__main__":
    main()
