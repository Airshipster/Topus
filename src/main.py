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
        
        channels = []
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
                if channel_id and channel_id.startswith('UC'):
                    channels.append(channel_id)
        
        return channels
    except Exception as e:
        print(f"  Error loading channels: {e}")
        return []

def get_published_videos(sheet):
    try:
        worksheet = sheet.worksheet(SHEET_NAME_VIDEOS)
        records = worksheet.get_all_records()
        return set(row.get('Video ID', '') for row in records if row.get('Video ID'))
    except:
        return set()

def save_video_to_global(sheet, video, project, tg_message_id=None, error=None):
    try:
        worksheet = sheet.worksheet(SHEET_NAME_VIDEOS)
        
        row = [
            video['video_id'],
            video['title'],
            video['url'],
            video['channel'],
            video['channel_id'],
            project['name'],
            video['published'],
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

def check_rss_feed(channel_id):
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    
    try:
        time.sleep(0.3)
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
        }
        
        response = requests.get(rss_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            print(f"    RSS: HTTP {response.status_code}")
            return []
        
        from xml.etree import ElementTree as ET
        root = ET.fromstring(response.content)
        
        ns = {
            'atom': 'http://www.w3.org/2005/Atom',
            'yt': 'http://www.youtube.com/xml/schemas/2015',
            'media': 'http://search.yahoo.com/mrss/'
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
        
        if len(entries) > 0:
            print(f"    RSS: {len(entries)} total, {len(videos)} new")
        
        return videos
        
    except Exception as e:
        print(f"    RSS error: {e}")
        return []

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
    
    total_found = 0
    total_published = 0
    
    for project in projects:
        print(f"\nProcessing: {project['name']}")
        
        yt_channels = load_youtube_channels(client, project)
        print(f"  Channels found: {len(yt_channels)}")
        
        for yt_channel in yt_channels:
            print(f"  Checking: {yt_channel}")
            videos = check_rss_feed(yt_channel)
            
            for video in videos:
                total_found += 1
                
                if video['video_id'] in published_videos:
                    print(f"    Skipped (duplicate): {video['title'][:50]}...")
                    continue
                
                message = f"🎥 <b>{video['title']}</b>\n\n" \
                         f"📺 {video['channel']}\n" \
                         f"🔗 {video['url']}"
                
                tg_message_id = send_to_telegram(
                    project['bot_token'],
                    project['channel_id'],
                    message
                )
                
                if tg_message_id:
                    print(f"    Published: {video['title'][:50]}...")
                    save_video_to_global(master_sheet, video, project, tg_message_id)
                    published_videos.add(video['video_id'])
                    total_published += 1
                else:
                    print(f"    Failed: {video['title'][:50]}...")
                    save_video_to_global(master_sheet, video, project, error="Telegram send failed")
    
    print(f"\nSummary:")
    print(f"  Videos found: {total_found}")
    print(f"  Published: {total_published}")
    print("\nDone")

if __name__ == "__main__":
    main()
