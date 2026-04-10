import gspread
import requests
import json
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
                'template': row.get('Шаблон по умолчанию'),
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
                if channel_id and channel_id.startswith('UC'):
                    channels[channel_id] = channel_name
        
        return channels
    except Exception as e:
        print(f"  Error loading channels: {e}")
        return {}

def get_published_videos(sheet):
    try:
        worksheet = sheet.worksheet(SHEET_NAME_VIDEOS)
        records = worksheet.get_all_records()
        return set(row.get('Video ID', '') for row in records if row.get('Video ID'))
    except:
        return set()

def get_push_events(sheet):
    try:
        worksheet = sheet.worksheet('Push events')
        values = worksheet.get_all_values()
        
        events = []
        for i, row in enumerate(values):
            if i == 0:
                continue
            
            if len(row) < 4:
                continue
            
            status = row[3] if len(row) > 3 else ''
            if status == '❌':
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

def mark_push_event_processed(sheet, row_index):
    try:
        worksheet = sheet.worksheet('Push events')
        worksheet.update_cell(row_index, 4, '✅')
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
        print(f"    Saved to global sheet")
    except Exception as e:
        print(f"    Error saving to global: {e}")

def get_video_info(video_id):
    try:
        import urllib.parse
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
    
    published_videos = get_published_videos(master_sheet)
    print(f"Already published: {len(published_videos)} videos")
    
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
                mark_push_event_processed(master_sheet, event['row_index'])
                continue
            
            total_found += 1
            
            video_info = get_video_info(event['video_id'])
            
            video = {
                'video_id': event['video_id'],
                'title': video_info['title'],
                'url': f"https://www.youtube.com/watch?v={event['video_id']}",
                'channel': video_info['channel'],
                'channel_id': event['channel_id']
            }
            
            print(f"  Publishing: {video['title'][:50]}...")
            
            message = f"🎥 <b>{video['title']}</b>\n\n" \
                     f"📺 {video['channel']}\n" \
                     f"🔗 {video['url']}"
            
            tg_message_id = send_to_telegram(
                project['bot_token'],
                project['channel_id'],
                message
            )
            
            if tg_message_id:
                print(f"    Published!")
                save_video_to_global(master_sheet, video, project, tg_message_id)
                published_videos.add(video['video_id'])
                mark_push_event_processed(master_sheet, event['row_index'])
                total_published += 1
            else:
                print(f"    Failed!")
                save_video_to_global(master_sheet, video, project, error="Telegram send failed")
    
    print(f"\nSummary:")
    print(f"  Videos found: {total_found}")
    print(f"  Published: {total_published}")
    print("\nDone")

if __name__ == "__main__":
    main()
