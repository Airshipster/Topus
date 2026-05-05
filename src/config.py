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
SUBSCRIPTION_RENEW_AFTER_DAYS = 4

# Cloudflare Worker (RSS proxy)
CLOUDFLARE_WORKER_URL = "https://aged-unit-b8f6.elman-ahmadbayov.workers.dev"

# YouTube API
YOUTUBE_API_KEY = None
MAX_VIDEO_AGE_HOURS = 168
MAX_PUBLISH_AGE_HOURS = 24
RSS_FALLBACK_AGE_HOURS = 24

# Cleanup settings
CLEANUP_AFTER_DAYS = 14

# Filters
FILTER_SHORTS = True
FILTER_LIVE = True

# Telegram
TELEGRAM_RATE_LIMIT = 20

# Batch settings
BATCH_SIZE = 20  # Размер батча для записи в Google Sheets

# Default template
DEFAULT_MESSAGE_TEMPLATE = '🎥 <b>{video_title}</b>\n\n📺 {channel_title}\n🔗 {video_url}'
