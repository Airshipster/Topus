import os

SPREADSHEET_ID = "19E8OWIYgAoR-PYrtlyPd0HdoBHWXg7nC_bxB_RVZhKI"
SHEET_NAME_PROJECTS = "Проекты"
SHEET_NAME_VIDEOS = "Глобальные видео"
SHEET_NAME_SETTINGS = "Настройки"
SHEET_NAME_PUSH_EVENTS = "Push события"

SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

CALLBACK_URL = "https://script.google.com/macros/s/AKfycbwqySnqlEYAMTTQNkcUy9RU-B6UkikW9o-v5lzLxtthnpOE_52XRZThoe2b1xjIj1Zm/exec"

YOUTUBE_API_KEY = None

FILTER_SHORTS = True
FILTER_LIVE = True
MAX_VIDEO_AGE_HOURS = 24

TELEGRAM_RATE_LIMIT = 20

DEFAULT_MESSAGE_TEMPLATE = '🎥 <b>{video_title}</b>\n\n📺 {channel_title}\n🔗 {video_url}'
