import gspread
import requests
import json
import time
import re
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from config import *

def authenticate_google_sheets():
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not found")
    
    credentials_dict = json.loads(SERVICE_ACCOUNT_JSON)
    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    client = gspread.authorize(credentials)
    return client

def log_to_sheet(sheet, project_name, event, video_id='', details='', status='info'):
    try:
        try:
            worksheet = sheet.worksheet('Логи')
        except:
            worksheet = sheet.add_worksheet('Логи', rows=10000, cols=6)
            worksheet.append_row(['Timestamp', 'Проект', 'Событие', 'Video ID', 'Детали', 'Статус'])
        
        timestamp = datetime.utcnow().isoformat()
        worksheet.append_row([timestamp, project_name, event, video_id, details, status])
    except Exception as e:
        print(f"    Error logging: {e}")

def load_settings(sheet):
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
            print(f"  YouTube API key loaded")
        
        if 'max_video_age_hours' in settings:
            MAX_VIDEO_AGE_HOURS = int(settings['max_video_age_hours'])
            print(f"  Max video age: {MAX_VIDEO_AGE_HOURS} hours ({MAX_VIDEO_AGE_HOURS//24} days)")
        
        if 'default_template' in settings:
            DEFAULT_MESSAGE_TEMPLATE = settings['default_template']
            print(f"  Default template loaded")
        
        return settings
    except Exception as e:
        print(f"  Error loading settings: {e}")
        return {}

def load_projects(sheet):
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
    
    print(f"Projects loaded: {len(projects)}")
    return projects

def load_youtube_channels(client, project):
    try:
        sheet = client.open_by_key(project['sheet_id'])
        worksheet = sheet.worksheet('Список. YouTube')
        
        # Получаем все значения (отображаемые, не формулы)
        values = worksheet.get_all_values()
        
        channels = {}
        for i, row in enumerate(values):
            if i == 0:  # Пропускаем заголовок
                continue
            
            if len(row) < 8:
                continue
            
            # Колонка G (индекс 6) - Активен
            status = row[6].strip() if len(row) > 6 else ''
            
            # 🔵 означает конец активных каналов
            if status == '🔵':
                break
            
            # Только активные 🟢
            if status == '🟢':
                # E (4) - Channel ID
                channel_id = row[4].strip() if len(row) > 4 else ''
                # D (3) - Channel Title
                channel_name = row[3].strip() if len(row) > 3 else ''
                # U (20) - Шаблон
                channel_template = row[20].strip() if len(row) > 20 else ''
                # V (21) - Telegram канал для шаблона
                tg_channel_link = row[21].strip() if len(row) > 21 else ''
                
                # Проверяем что это валидный YouTube канал
                if channel_id and channel_id.startswith('UC'):
                    channels[channel_id] = {
                        'name': channel_name,
                        'template': channel_template,
                        'tg_channel': tg_channel_link
                    }
        
        print(f"  Loaded {len(channels)} active channels for {project['name']}")
        return channels
    except Exception as e:
        print(f"  Error loading channels for {project['name']}: {e}")
        return {}

def get_all_active_channels(client, projects):
    all_channels = {}
    
    for project in projects:
        channels = load_youtube_channels(client, project)
        for ch_id, ch_info in channels.items():
            if ch_id not in all_channels:
                all_channels[ch_id] = ch_info
    
    return all_channels

def get_subscribed_channels(sheet):
    try:
        worksheet = sheet.worksheet('Подписки')
        records = worksheet.get_all_records()
        return set(row.get('Channel ID', '') for row in records if row.get('Channel ID'))
    except:
        return set()

def save_subscribed_channels_batch(sheet, channel_ids):
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
        print(f"  Error removing subscriptions: {e}")

def get_video_info_from_api(video_id):
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
        duration_str = content_details.get('duration', '')
        if duration_str:
            match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
            if match:
                hours = int(match.group(1) or 0)
                minutes = int(match.group(2) or 0)
                seconds = int(match.group(3) or 0)
                total_seconds = hours * 3600 + minutes * 60 + seconds
                # Шортсы - это видео длительностью до 60 секунд
                if total_seconds <= 60:
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
            'duration_seconds': hours * 3600 + minutes * 60 + seconds if duration_str else 0
        }
    except Exception as e:
        print(f"  YouTube API error: {e}")
        return None

def check_rss_feed(channel_id):
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
    print("\nSyncing subscriptions...")
    
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
            print(f"  Successfully subscribed: {len(subscribed)}")
            log_to_sheet(master_sheet, 'System', 'Push subscriptions', '', f'Subscribed to {len(subscribed)} channels', 'success')
    
    if len(to_unsubscribe) > 0:
        print(f"  Unsubscribing from {len(to_unsubscribe)} inactive channels...")
        unsubscribed = []
        for channel_id in to_unsubscribe:
            if unsubscribe_channel(channel_id):
                unsubscribed.append(channel_id)
            time.sleep(0.1)
        
        if unsubscribed:
            remove_subscribed_channels(master_sheet, unsubscribed)
            print(f"  Successfully unsubscribed: {len(unsubscribed)}")
            log_to_sheet(master_sheet, 'System', 'Push unsubscriptions', '', f'Unsubscribed from {len(unsubscribed)} channels', 'success')
    
    if len(to_subscribe) == 0 and len(to_unsubscribe) == 0:
        print("  No changes needed")

def rss_fallback_check(client, projects, published_videos):
    print("\nRSS fallback check...")
    
    all_channels = get_all_active_channels(client, projects)
    
    print(f"  Checking {len(all_channels)} channels")
    print(f"  Time window: {MAX_VIDEO_AGE_HOURS}h ({MAX_VIDEO_AGE_HOURS//24}d)")
    
    new_videos = []
    videos_found_count = 0
    
    for i, (channel_id, channel_info) in enumerate(all_channels.items()):
        videos = check_rss_feed(channel_id)
        videos_found_count += len(videos)
        
        if i > 0 and i % 20 == 0:
            print(f"  Progress: {i}/{len(all_channels)} (Videos: {videos_found_count})")
        
        for video in videos:
            if video['video_id'] not in published_videos:
                # Найдём проект, которому принадлежит этот канал
                for project in projects:
                    project_channels = load_youtube_channels(client, project)
                    if channel_id in project_channels:
                        video['project'] = project
                        video['channel_info'] = project_channels[channel_id]  # Берём из проектного листа!
                        new_videos.append(video)
                        break
    
    print(f"  Found {videos_found_count} videos total")
    print(f"  New unpublished: {len(new_videos)}")
    
    return new_videos

def get_published_videos(sheet):
    try:
        worksheet = sheet.worksheet(SHEET_NAME_VIDEOS)
        records = worksheet.get_all_records()
        return set(row.get('Video ID', '') for row in records if row.get('Video ID'))
    except:
        return set()

def get_push_events(sheet):
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
        print(f"Error loading push events: {e}")
        return []

def mark_push_event_processed(sheet, row_index, project_name):
    try:
        worksheet = sheet.worksheet(SHEET_NAME_PUSH_EVENTS)
        worksheet.update_cell(row_index, 4, '✅')
        
        current_projects = worksheet.cell(row_index, 5).value or ''
        if project_name not in current_projects:
            new_projects = (current_projects + ', ' + project_name).strip(', ')
            worksheet.update_cell(row_index, 5, new_projects)
    except Exception as e:
        print(f"  Error marking event: {e}")

def save_video_to_global(sheet, video, project, video_published_date, tg_message_id=None, error=None):
    try:
        worksheet = sheet.worksheet(SHEET_NAME_VIDEOS)
        
        row = [
            video['video_id'],
            video['title'],
            video['url'],
            video.get('channel', ''),
            video['channel_id'],
            project['name'],
            video_published_date,
            datetime.utcnow().isoformat(),
            '1' if tg_message_id else '0',
            str(tg_message_id) if tg_message_id else '',
            datetime.utcnow().isoformat() if tg_message_id else '',
            'published' if tg_message_id else 'failed',
            error or ''
        ]
        
        worksheet.append_row(row)
    except Exception as e:
        print(f"    Error saving to global: {e}")

def should_filter_video(video_info, project):
    """
    Проверяет, нужно ли фильтровать видео.
    Возвращает (True/False, причина)
    """
    if not video_info:
        return False, ""
    
    # ФИЛЬТР ШОРТСОВ - проверяем через API
    if FILTER_SHORTS and video_info.get('is_short'):
        return True, f"Short video ({video_info.get('duration_seconds', 0)}s)"
    
    # ФИЛЬТР СТРИМОВ
    if FILTER_LIVE and video_info.get('is_live'):
        return True, "Live stream"
    
    # ФИЛЬТР ПРЕМЬЕР/UPCOMING
    if video_info.get('is_upcoming'):
        return True, "Upcoming/Premiere"
    
    # ФИЛЬТР СТОП-СЛОВ
    if project.get('stop_words'):
        title_lower = video_info['title'].lower()
        for stop_word in project['stop_words']:
            if stop_word and stop_word in title_lower:
                return True, f"Stop word: {stop_word}"
    
    return False, ""

def format_message(template, video, channel_info, project):
    """
    Форматирование сообщения с поддержкой:
    - {channel_title} - название канала
    - {video_title} - название видео
    - {video_url} - URL видео
    - {video_title_link} - название с гиперссылкой <a href="url">title</a>
    - {TG_channel} - Telegram канал проекта из колонки E
    - [текст] - гиперссылка на Telegram канал из колонки V (если не начинается с -)
    """
    if not template:
        template = DEFAULT_MESSAGE_TEMPLATE
    
    channel_name = video.get('channel', channel_info.get('name', 'Unknown'))
    video_title = video['title']
    video_url = video['url']
    
    # Базовые замены
    message = template.replace('{channel_title}', channel_name)
    message = message.replace('{video_title}', video_title)
    message = message.replace('{video_url}', video_url)
    message = message.replace('{video_title_link}', f'<a href="{video_url}">{video_title}</a>')
    
    # {TG_channel} из проекта
    tg_channel_name = project.get('tg_channel', '')
    message = message.replace('{TG_channel}', tg_channel_name)
    
    # Обработка [текст] → гиперссылка на Telegram канал
    tg_channel_link = channel_info.get('tg_channel', '').strip()
    
    # Если tg_channel_link НЕ начинается с дефиса и не пустой
    if tg_channel_link and not tg_channel_link.startswith('-'):
        # Находим все [текст] и заменяем на <a href="tg_channel_link">текст</a>
        def replace_brackets(match):
            text = match.group(1)
            return f'<a href="{tg_channel_link}">{text}</a>'
        
        message = re.sub(r'\[([^\]]+)\]', replace_brackets, message)
    else:
        # Если начинается с дефиса или пусто - просто убираем скобки
        message = re.sub(r'\[([^\]]+)\]', r'\1', message)
    
    return message

def send_to_telegram(bot_token, channel_id, message):
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
        print(f"  Telegram error: {e}")
        return None

def main():
    print("="*60)
    print("TOPUS - YouTube to Telegram Publisher")
    print("="*60)
    print(f"Started: {datetime.utcnow().isoformat()}Z\n")
    
    client = authenticate_google_sheets()
    master_sheet = client.open_by_key(SPREADSHEET_ID)
    
    print("Loading settings...")
    settings = load_settings(master_sheet)
    
    projects = load_projects(master_sheet)
    
    sync_subscriptions(client, master_sheet, projects)
    
    published_videos = get_published_videos(master_sheet)
    print(f"\nAlready published: {len(published_videos)} videos")
    
    push_events = get_push_events(master_sheet)
    print(f"Unprocessed push events: {len(push_events)}")
    
    total_found = 0
    total_published = 0
    total_filtered = 0
    total_failed = 0
    
    for project in projects:
        print(f"\n{'='*60}")
        print(f"Project: {project['name']}")
        print(f"{'='*60}")
        
        yt_channels = load_youtube_channels(client, project)
        print(f"  Active channels: {len(yt_channels)}")
        
        # Process push events
        for event in push_events:
            # ВАЖНО: проверяем что канал принадлежит ЭТОМУ проекту
            if event['channel_id'] not in yt_channels:
                continue
            
            if event['video_id'] in published_videos:
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                continue
            
            total_found += 1
            
            # ОБЯЗАТЕЛЬНО получаем информацию через API для проверки на шортсы!
            video_info_api = get_video_info_from_api(event['video_id'])
            
            if not video_info_api:
                print(f"  ⚠️  Failed to get video info from API: {event['video_id']}")
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
            
            # ПРОВЕРКА ФИЛЬТРОВ (в т.ч. шортсы!)
            should_filter, filter_reason = should_filter_video(video_info_api, project)
            if should_filter:
                print(f"  🚫 Filtered: {video['title'][:50]} ({filter_reason})")
                log_to_sheet(master_sheet, project['name'], 'Video filtered', video['video_id'], filter_reason, 'filtered')
                published_videos.add(video['video_id'])
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                total_filtered += 1
                continue
            
            print(f"  📤 Publishing: {video['title'][:50]}...")
            
            template = channel_info.get('template') or project['default_template']
            message = format_message(template, video, channel_info, project)
            
            tg_message_id = send_to_telegram(
                project['bot_token'],
                project['channel_id'],
                message
            )
            
            if tg_message_id:
                print(f"    ✅ Published (msg: {tg_message_id})")
                log_to_sheet(master_sheet, project['name'], 'Video published', video['video_id'], f"Telegram msg: {tg_message_id}", 'success')
                save_video_to_global(master_sheet, video, project, video_published_date, tg_message_id)
                published_videos.add(video['video_id'])
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                total_published += 1
            else:
                print(f"    ❌ Failed to publish")
                log_to_sheet(master_sheet, project['name'], 'Publish failed', video['video_id'], 'Telegram error', 'error')
                save_video_to_global(master_sheet, video, project, video_published_date, error="Telegram send failed")
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                total_failed += 1
            
            time.sleep(1 / TELEGRAM_RATE_LIMIT)
    
    # RSS fallback check
    rss_videos = rss_fallback_check(client, projects, published_videos)
    
    for video in rss_videos:
        project = video['project']
        channel_info = video['channel_info']
        
        total_found += 1
        
        # ОБЯЗАТЕЛЬНО получаем информацию через API для проверки на шортсы!
        video_info_api = get_video_info_from_api(video['video_id'])
        
        if video_info_api:
            video_published_date = video_info_api['published']
            
            # ПРОВЕРКА ФИЛЬТРОВ (в т.ч. шортсы!)
            should_filter, filter_reason = should_filter_video(video_info_api, project)
            
            if should_filter:
                print(f"  🚫 Filtered (RSS): {video['title'][:50]} ({filter_reason})")
                log_to_sheet(master_sheet, project['name'], 'Video filtered', video['video_id'], f"RSS: {filter_reason}", 'filtered')
                published_videos.add(video['video_id'])
                total_filtered += 1
                continue
        else:
            video_published_date = video.get('published', datetime.utcnow().isoformat())
        
        print(f"  📤 Publishing (RSS): {video['title'][:50]}...")
        
        template = channel_info.get('template') or project['default_template']
        message = format_message(template, video, channel_info, project)
        
        tg_message_id = send_to_telegram(
            project['bot_token'],
            project['channel_id'],
            message
        )
        
        if tg_message_id:
            print(f"    ✅ Published (msg: {tg_message_id})")
            log_to_sheet(master_sheet, project['name'], 'Video published', video['video_id'], f"RSS → Telegram msg: {tg_message_id}", 'success')
            save_video_to_global(master_sheet, video, project, video_published_date, tg_message_id)
            published_videos.add(video['video_id'])
            total_published += 1
        else:
            print(f"    ❌ Failed to publish")
            log_to_sheet(master_sheet, project['name'], 'Publish failed', video['video_id'], 'RSS → Telegram error', 'error')
            save_video_to_global(master_sheet, video, project, video_published_date, error="Telegram send failed")
            total_failed += 1
        
        time.sleep(1 / TELEGRAM_RATE_LIMIT)
    
    # Final summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Videos found: {total_found}")
    print(f"  ✅ Published: {total_published}")
    print(f"  🚫 Filtered: {total_filtered}")
    print(f"  ❌ Failed: {total_failed}")
    print(f"\nFinished: {datetime.utcnow().isoformat()}Z")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
