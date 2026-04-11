import os

# Google Sheets
SPREADSHEET_ID = "19E8OWIYgAoR-PYrtlyPd0HdoBHWXg7nC_bxB_RVZhKI"
SHEET_NAME_PROJECTS = "Проекты"
SHEET_NAME_VIDEOS = "Глобальные видео"
SHEET_NAME_SETTINGS = "Настройки"
SHEET_NAME_PUSH_EVENTS = "Push события"

# Service Account
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

# PubSubHubbub
CALLBACK_URL = "https://script.google.com/macros/s/AKfycbwqySnqlEYAMTTQNkcUy9RU-B6UkikW9o-v5lzLxtthnpOE_52XRZThoe2b1xjIj1Zm/exec"

# Cloudflare Worker (RSS proxy)
CLOUDFLARE_WORKER_URL = "https://aged-unit-b8f6.elman-ahmadbayov.workers.dev"

# YouTube API (загружается из настроек)
YOUTUBE_API_KEY = None
MAX_VIDEO_AGE_HOURS = 168  # 7 дней

# Cleanup settings
CLEANUP_AFTER_DAYS = 7  # Удалять записи старше 7 дней

# Filters
FILTER_SHORTS = True
FILTER_LIVE = True

# Telegram
TELEGRAM_RATE_LIMIT = 20

# Default template
DEFAULT_MESSAGE_TEMPLATE = '🎥 <b>{video_title}</b>\n\n📺 {channel_title}\n🔗 {video_url}'
