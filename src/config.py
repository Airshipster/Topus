"""
Конфигурация для Topus YouTube → Telegram агрегатора
"""
import os

# Google Sheets
SPREADSHEET_ID = "19E8OWIYgAoR-PYrtlyPd0HdoBHWXg7nC_bxB_RVZhKI"
SHEET_NAME_PROJECTS = "Проекты"
SHEET_NAME_VIDEOS = "Глобальные видео"
SHEET_NAME_SETTINGS = "Настройки"

# Service Account JSON (из GitHub Secrets)
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

# YouTube API
YOUTUBE_API_KEY = None  # Будет загружен из Google Sheets

# Фильтры
FILTER_SHORTS = True  # Пропускать Shorts
FILTER_LIVE = True    # Пропускать Live/Premiere
MAX_VIDEO_AGE_HOURS = 24  # Только новые видео (24 часа)

# Telegram
TELEGRAM_RATE_LIMIT = 20  # Сообщений в минуту
