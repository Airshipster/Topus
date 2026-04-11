import gspread
import requests
import json
import time
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
            key = row.get('Параметр', '').strip()
            value = row.get('Значение', '').strip()
            if key and value:
                settings[key] = value
        
        global YOUTUBE_API_KEY, MAX_VIDEO_AGE_HOURS
        if 'youtube_api_key' in settings:
            YOUTUBE_API_KEY = settings['youtube_api_key']
            print(f"  YouTube API key loaded: {YOUTUBE_API_KEY[:10]}...")
        
        if 'max_video_age_hours' in settings:
            MAX_VIDEO_AGE_HOURS = int(settings['max_video_age_hours'])
            print(f"  Max video age: {MAX_VIDEO_AGE_HOURS} hours ({MAX_VIDEO_AGE_HOURS//24} days)")
        
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
            
            projects.append({
                'code': row.get('Код проекта'),
                'name': row.get('Название'),
                'sheet_id': sheet_id,
                'bot_token': row.get('Telegram bot token'),
                'channel_id': str(row.get('Telegram канал ID')),
                'default_template': row.get('Шаблон по умолчанию', DEFAULT_MESSAGE_TEMPLATE),
                'stop_words': stop_words
            })
    
    print(f"Projects loaded: {len(projects)}")
    return projects

def load_youtube_channels(client, project):
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
                
                if channel_id and channel_id.startswith('UC'):
                    channels[channel_id] = {
                        'name': channel_name,
                        'template': channel_template
                    }
        
        return channels
    except Exception as e:
        print(f"  Error loading channels: {e}")
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
            print(f"    API error {response.status_code}: {response.text[:100]}")
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
            import re
            match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
            if match:
                hours = int(match.group(1) or 0)
                minutes = int(match.group(2) or 0)
                seconds = int(match.group(3) or 0)
                total_seconds = hours * 3600 + minutes * 60 + seconds
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
            'duration': duration_str
        }
    except Exception as e:
        print(f"    API exception: {e}")
        return None

def check_rss_feed(channel_id):
    try:
        time.sleep(0.2)
        
        url = f"{CLOUDFLARE_WORKER_URL}/?channel={channel_id}"
        response = requests.get(url, timeout=15)
        
        if response.status_code != 200:
            print(f"    RSS error for {channel_id}: HTTP {response.status_code}")
            return []
        
        if len(response.content) == 0:
            print(f"    RSS error for {channel_id}: Empty response")
            return []
        
        from xml.etree import ElementTree as ET
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as e:
            print(f"    RSS parse error for {channel_id}: {e}")
            print(f"    Response preview: {response.text[:200]}")
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
            
            if not all([video_id_elem, title_elem, published_elem]):
                continue
            
            video_id = video_id_elem.text
            title = title_elem.text
            published_str = published_elem.text
            channel_name = author_elem.text if author_elem is not None else 'Unknown'
            
            try:
                published = datetime.fromisoformat(published_str.replace('Z', '+00:00')).replace(tzinfo=None)
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
    except requests.Timeout:
        print(f"    RSS timeout for {channel_id}")
        return []
    except requests.RequestException as e:
        print(f"    RSS request error for {channel_id}: {e}")
        return []
    except Exception as e:
        print(f"    RSS unexpected error for {channel_id}: {type(e).__name__}: {e}")
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
        print(f"\n  Subscribing to {len(to_subscribe)} new channels...")
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
        print(f"\n  Unsubscribing from {len(to_unsubscribe)} inactive channels...")
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
    
    print(f"  Checking {len(all_channels)} channels via Cloudflare Worker...")
    print(f"  Looking for videos from last {MAX_VIDEO_AGE_HOURS} hours ({MAX_VIDEO_AGE_HOURS//24} days)")
    
    new_videos = []
    success_count = 0
    failed_count = 0
    videos_found_count = 0
    
    for i, (channel_id, channel_info) in enumerate(all_channels.items()):
        if i > 0 and i % 10 == 0:
            print(f"  Progress: {i}/{len(all_channels)} (Success: {success_count}, Videos: {videos_found_count})")
        
        videos = check_rss_feed(channel_id)
        
        if videos is not None:
            if len(videos) > 0:
                success_count += 1
                videos_found_count += len(videos)
            else:
                success_count += 1
        else:
            failed_count += 1
        
        for video in videos:
            if video['video_id'] not in published_videos:
                for project in projects:
                    project_channels = load_youtube_channels(client, project)
                    if channel_id in project_channels:
                        video['project'] = project
                        video['channel_info'] = channel_info
                        new_videos.append(video)
                        break
    
    print(f"\n  RSS Check Results:")
    print(f"    Channels checked: {len(all_channels)}")
    print(f"    Successful responses: {success_count}")
    print(f"    Failed: {failed_count}")
    print(f"    Total videos found in feeds: {videos_found_count}")
    print(f"    New unpublished videos: {len(new_videos)}")
    
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

def save_video_to_global(sheet, video, project, tg_message_id=None, error=None):
    try:
        worksheet = sheet.worksheet(SHEET_NAME_VIDEOS)
        
        row = [
            video['video_id'],
            video['title'],
            video['url'],
            video.get('channel', ''),
            video['channel_id'],
            project['name'],
            datetime.utcnow().isoformat(),
            '',
            '',
            '',
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
    if not video_info:
        return False, "API info unavailable"
    
    if FILTER_SHORTS and video_info.get('is_short'):
        return True, "Filtered: Short video"
    
    if FILTER_LIVE and video_info.get('is_live'):
        return True, "Filtered: Live stream"
    
    if video_info.get('is_upcoming'):
        return True, "Filtered: Upcoming/Premiere"
    
    if project.get('stop_words'):
        title_lower = video_info['title'].lower()
        for stop_word in project['stop_words']:
            if stop_word and stop_word in title_lower:
                return True, f"Filtered: Stop word '{stop_word}'"
    
    return False, ""

def format_message(template, video, channel_info):
    if not template:
        template = DEFAULT_MESSAGE_TEMPLATE
    
    channel_name = video.get('channel', channel_info.get('name', 'Unknown'))
    video_title = video['title']
    video_url = video['url']
    
    message = template.replace('{channel_title}', channel_name)
    message = message.replace('{video_title}', video_title)
    message = message.replace('{video_url}', video_url)
    message = message.replace('{video_title_link}', f'<a href="{video_url}">{video_title}</a>')
    
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
    print(f"Started at: {datetime.utcnow().isoformat()}Z")
    
    client = authenticate_google_sheets()
    master_sheet = client.open_by_key(SPREADSHEET_ID)
    
    print("\nLoading settings...")
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
        print(f"Processing project: {project['name']}")
        print(f"{'='*60}")
        
        yt_channels = load_youtube_channels(client, project)
        print(f"  Active channels: {len(yt_channels)}")
        
        for event in push_events:
            if event['channel_id'] not in yt_channels:
                continue
            
            if event['video_id'] in published_videos:
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                continue
            
            total_found += 1
            
            video_info_api = get_video_info_from_api(event['video_id'])
            
            if not video_info_api:
                print(f"  Could not get API info for {event['video_id']}")
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
            
            should_filter, filter_reason = should_filter_video(video_info_api, project)
            if should_filter:
                print(f"  Skipped: {video['title'][:50]}...")
                print(f"     Reason: {filter_reason}")
                log_to_sheet(master_sheet, project['name'], 'Video filtered', video['video_id'], filter_reason, 'filtered')
                save_video_to_global(master_sheet, video, project, error=filter_reason)
                published_videos.add(video['video_id'])
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                total_filtered += 1
                continue
            
            print(f"  Publishing (Push): {video['title'][:50]}...")
            
            template = channel_info.get('template') or project['default_template']
            message = format_message(template, video, channel_info)
            
            tg_message_id = send_to_telegram(
                project['bot_token'],
                project['channel_id'],
                message
            )
            
            if tg_message_id:
                print(f"     Success! Message ID: {tg_message_id}")
                log_to_sheet(master_sheet, project['name'], 'Video published', video['video_id'], f"TG msg: {tg_message_id}", 'success')
                save_video_to_global(master_sheet, video, project, tg_message_id)
                published_videos.add(video['video_id'])
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                total_published += 1
            else:
                print(f"     Failed to publish")
                log_to_sheet(master_sheet, project['name'], 'Publish failed', video['video_id'], 'Telegram API error', 'error')
                save_video_to_global(master_sheet, video, project, error="Telegram send failed")
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                total_failed += 1
    
    if len(push_events) == 0:
        rss_videos = rss_fallback_check(client, projects, published_videos)
        
        for video in rss_videos:
            total_found += 1
            
            project = video['project']
            channel_info = video['channel_info']
            
            video_info_api = get_video_info_from_api(video['video_id'])
            
            if video_info_api:
                video['title'] = video_info_api['title']
                video['channel'] = video_info_api['channel']
                
                should_filter, filter_reason = should_filter_video(video_info_api, project)
                if should_filter:
                    print(f"  Skipped: {video['title'][:50]}...")
                    print(f"     Reason: {filter_reason}")
                    log_to_sheet(master_sheet, project['name'], 'Video filtered', video['video_id'], filter_reason, 'filtered')
                    save_video_to_global(master_sheet, video, project, error=filter_reason)
                    published_videos.add(video['video_id'])
                    total_filtered += 1
                    continue
            
            print(f"  Publishing (RSS): {video['title'][:50]}...")
            
            template = channel_info.get('template') or project['default_template']
            message = format_message(template, video, channel_info)
            
            tg_message_id = send_to_telegram(
                project['bot_token'],
                project['channel_id'],
                message
            )
            
            if tg_message_id:
                print(f"     Success! Message ID: {tg_message_id}")
                log_to_sheet(master_sheet, project['name'], 'Video published', video['video_id'], f"TG msg: {tg_message_id}", 'success')
                save_video_to_global(master_sheet, video, project, tg_message_id)
                published_videos.add(video['video_id'])
                total_published += 1
            else:
                print(f"     Failed to publish")
                log_to_sheet(master_sheet, project['name'], 'Publish failed', video['video_id'], 'Telegram API error', 'error')
                save_video_to_global(master_sheet, video, project, error="Telegram send failed")
                total_failed += 1
    
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Videos found: {total_found}")
    print(f"  Published: {total_published}")
    print(f"  Filtered: {total_filtered}")
    print(f"  Failed: {total_failed}")
    print(f"{'='*60}")
    print("\nDone!")

if __name__ == "__main__":
    main()
