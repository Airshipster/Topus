import gspread
import feedparser
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
            sheet_id = row.get('Ссылка на документ проекта', '')
            if '/d/' in sheet_id:
                sheet_id = sheet_id.split('/d/')[1].split('/')[0]
            
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
    sheet = client.open_by_key(project['sheet_id'])
    worksheet = sheet.worksheet('Список. YouTube')
    values = worksheet.get_all_values()
    
    channels = []
    for row in values[1:]:
        if len(row) < 8:
            continue
        
        status = row[7]
        if status == '🔵':
            break
        if status == '🟢':
            channel_id = row[4] if len(row) > 4 else ''
            if channel_id:
                channels.append(channel_id.strip())
    
    return channels

def check_rss_feed(channel_id):
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    
    try:
        feed = feedparser.parse(rss_url)
        videos = []
        
        cutoff_time = datetime.now() - timedelta(hours=MAX_VIDEO_AGE_HOURS)
        
        for entry in feed.entries:
            published = datetime(*entry.published_parsed[:6])
            
            if published > cutoff_time:
                videos.append({
                    'title': entry.title,
                    'url': entry.link,
                    'channel': feed.feed.title,
                    'published': published.isoformat()
                })
        
        return videos
    except Exception as e:
        print(f"RSS error for {channel_id}: {e}")
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
        return True
    except Exception as e:
        print(f"Telegram error: {e}")
        return False

def main():
    print("Starting...")
    
    client = authenticate_google_sheets()
    master_sheet = client.open_by_key(SPREADSHEET_ID)
    
    projects = load_projects(master_sheet)
    
    for project in projects:
        print(f"\nProcessing: {project['name']}")
        
        yt_channels = load_youtube_channels(client, project)
        print(f"  Channels found: {len(yt_channels)}")
        
        for yt_channel in yt_channels:
            print(f"  Checking: {yt_channel}")
            videos = check_rss_feed(yt_channel)
            
            for video in videos:
                message = f"🎥 <b>{video['title']}</b>\n\n" \
                         f"📺 {video['channel']}\n" \
                         f"🔗 {video['url']}"
                
                success = send_to_telegram(
                    project['bot_token'],
                    project['channel_id'],
                    message
                )
                
                if success:
                    print(f"    Published: {video['title'][:50]}...")
                else:
                    print(f"    Failed: {video['title'][:50]}...")
    
    print("\nDone")

if __name__ == "__main__":
    main()
