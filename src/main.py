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
        if row.get('Статус') == 'Активен':
            projects.append({
                'name': row.get('Название проекта'),
                'bot_token': row.get('Токен бота'),
                'channel_id': row.get('Channel ID'),
                'youtube_channels': row.get('YouTube каналы', '').split(',')
            })
    
    print(f"Projects loaded: {len(projects)}")
    return projects

def check_rss_feed(channel_id):
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id.strip()}"
    
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
    sheet = client.open_by_key(SPREADSHEET_ID)
    
    projects = load_projects(sheet)
    
    for project in projects:
        print(f"\nProcessing: {project['name']}")
        
        for yt_channel in project['youtube_channels']:
            if not yt_channel.strip():
                continue
                
            print(f"  Checking: {yt_channel.strip()}")
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
