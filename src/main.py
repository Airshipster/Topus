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
            
            projects.append({
                'code': row.get('Код проекта'),
                'name': row.get('Название'),
                'sheet_id': sheet_id,
                'bot_token': row.get('Telegram bot token'),
                'channel_id': str(row.get('Telegram канал ID')),
                'default_template': row.get('Шаблон по умолчанию', DEFAULT_MESSAGE_TEMPLATE),
                'stop_words': row.get('Стоп-слова (через запятую)', '').split(',')
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

def check_rss_feed(channel_id):
    try:
        time.sleep(0.2)
        
        url = f"{CLOUDFLARE_WORKER_URL}/?channel={channel_id}"
        response = requests.get(url, timeout=15)
        
        if response.status_code != 200:
            return []
        
        from xml.etree import ElementTree as ET
        root = ET.fromstring(response.content)
        
        ns = {
            'atom': 'http://www.w3.org/2005/Atom',
            'yt': 'http://www.youtube.com/xml/schemas/2015'
        }
        
        entries = root.findall('atom:entry', ns)
        
        videos = []
        cutoff_time = datetime.utcnow() - timedelta(days=7)
        
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
            
            published = datetime.fromisoformat(published_str.replace('Z', '+00:00')).replace(tzinfo=None)
            
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
        print(f"\n  Subscribing to {len(to_subscribe)} new channels...")
        subscribed = []
        for channel_id in to_subscribe:
            if subscribe_channel(channel_id):
                subscribed.append(channel_id)
            time.sleep(0.1)
        
        if subscribed:
            save_subscribed_channels_batch(master_sheet, subscribed)
            print(f"  Successfully subscribed: {len(subscribed)}")
    
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
    
    if len(to_subscribe) == 0 and len(to_unsubscribe) == 0:
        print("  No changes needed")

def rss_fallback_check(client, projects, published_videos):
    print("\nRSS fallback check...")
    
    all_channels = get_all_active_channels(client, projects)
    
    print(f"  Checking ALL {len(all_channels)} channels via Cloudflare Worker...")
    
    new_videos = []
    success_count = 0
    failed_count = 0
    
    for i, (channel_id, channel_info) in enumerate(all_channels.items()):
        if i > 0 and i % 10 == 0:
            print(f"  Progress: {i}/{len(all_channels)}")
        
        videos = check_rss_feed(channel_id)
        
        if videos:
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
    
    print(f"  Checked {len(all_channels)} channels:")
    print(f"    Success: {success_count}")
    print(f"    Failed: {failed_count}")
    print(f"    Found {len(new_videos)} new videos")
    
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

def get_video_info(video_id):
    try:
        oembed_url = f"https://www.youtube.com/oembed?url=https://www.youtube.com/watch?v={video_id}&format=json"
        response = requests.get(oembed_url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return {
                'title': data.get('title', 'Unknown'),
                'channel': data.get('author_name', 'Unknown')
            }
    except:
        pass
    
    return {
        'title': f"Video {video_id}",
        'channel': 'Unknown'
    }

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
    print("Starting...")
    
    client = authenticate_google_sheets()
    master_sheet = client.open_by_key(SPREADSHEET_ID)
    
    projects = load_projects(master_sheet)
    
    sync_subscriptions(client, master_sheet, projects)
    
    published_videos = get_published_videos(master_sheet)
    print(f"\nAlready published: {len(published_videos)} videos")
    
    push_events = get_push_events(master_sheet)
    print(f"Unprocessed push events: {len(push_events)}")
    
    total_found = 0
    total_published = 0
    
    for project in projects:
        print(f"\nProcessing: {project['name']}")
        
        yt_channels = load_youtube_channels(client, project)
        print(f"  Channels found: {len(yt_channels)}")
        
        for event in push_events:
            if event['channel_id'] not in yt_channels:
                continue
            
            if event['video_id'] in published_videos:
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                continue
            
            total_found += 1
            
            video_info = get_video_info(event['video_id'])
            channel_info = yt_channels[event['channel_id']]
            
            video = {
                'video_id': event['video_id'],
                'title': video_info['title'],
                'url': f"https://www.youtube.com/watch?v={event['video_id']}",
                'channel': video_info['channel'],
                'channel_id': event['channel_id']
            }
            
            print(f"  Publishing: {video['title'][:50]}...")
            
            template = channel_info.get('template') or project['default_template']
            message = format_message(template, video, channel_info)
            
            tg_message_id = send_to_telegram(
                project['bot_token'],
                project['channel_id'],
                message
            )
            
            if tg_message_id:
                print(f"    Published!")
                save_video_to_global(master_sheet, video, project, tg_message_id)
                published_videos.add(video['video_id'])
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
                total_published += 1
            else:
                print(f"    Failed!")
                save_video_to_global(master_sheet, video, project, error="Telegram send failed")
                mark_push_event_processed(master_sheet, event['row_index'], project['name'])
    
    if len(push_events) == 0:
        rss_videos = rss_fallback_check(client, projects, published_videos)
        
        for video in rss_videos:
            total_found += 1
            
            project = video['project']
            channel_info = video['channel_info']
            
            print(f"  Publishing (RSS): {video['title'][:50]}...")
            
            template = channel_info.get('template') or project['default_template']
            message = format_message(template, video, channel_info)
            
            tg_message_id = send_to_telegram(
                project['bot_token'],
                project['channel_id'],
                message
            )
            
            if tg_message_id:
                print(f"    Published!")
                save_video_to_global(master_sheet, video, project, tg_message_id)
                published_videos.add(video['video_id'])
                total_published += 1
            else:
                print(f"    Failed!")
                save_video_to_global(master_sheet, video, project, error="Telegram send failed")
    
    print(f"\nSummary:")
    print(f"  Videos found: {total_found}")
    print(f"  Published: {total_published}")
    print("\
