"""
Topus: YouTube → Telegram агрегатор
Главный скрипт для GitHub Actions
"""
import gspread
import feedparser
import requests
import json
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
from config import *

def authenticate_google_sheets():
    """Аутентификация в Google Sheets через Service Account"""
    if not SERVICE_ACCOUNT_JSON:
        raise ValueError("❌ GOOGLE_SERVICE_ACCOUNT_JSON не найден в secrets")
    
    credentials_dict = json.loads(SERVICE_ACCOUNT_JSON)
    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    client = gspread.authorize(credentials)
    return client

def load_projects(sheet):
    """Загрузка проектов из листа 'Проекты'"""
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
    
    print(f"✅ Загружено проектов: {len(projects)}")
    return projects

def check_rss_feed(channel_id):
    """Проверка RSS-ленты YouTube канала"""
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
        print(f"⚠️ Ошибка RSS для {channel_id}: {e}")
        return []

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
        return True
    except Exception as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")
        return False

def main():
    """Основной цикл обработки"""
    print("🚀 Запуск Topus агрегатора...")
    
    # 1. Подключение к Google Sheets
    client = authenticate_google_sheets()
    sheet = client.open_by_key(SPREADSHEET_ID)
    
    # 2. Загрузка проектов
    projects = load_projects(sheet)
    
    # 3. Проверка каналов и публикация
    for project in projects:
        print(f"\n📂 Обработка проекта: {project['name']}")
        
        for yt_channel in project['youtube_channels']:
            if not yt_channel.strip():
                continue
                
            print(f"  🔍 Проверка канала: {yt_channel.strip()}")
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
                    print(f"    ✅ Опубликовано: {video['title'][:50]}...")
                else:
                    print(f"    ❌ Ошибка публикации: {video['title'][:50]}...")
    
    print("\n✅ Работа завершена!")

if __name__ == "__main__":
    main()
