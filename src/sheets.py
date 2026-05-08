import json
import re
import time
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials

import config


PROJECT_STATUS_COLUMNS = [
    'Provisioning status',
    'Provisioning error',
    'Provisioned at',
]


def extract_sheet_id(sheet_url):
    if not sheet_url:
        return ''

    if '/d/' in sheet_url:
        return sheet_url.split('/d/')[1].split('/')[0]

    return sheet_url.strip()


def normalize_project_row(headers, row):
    return {
        header: row[i].strip() if i < len(row) and isinstance(row[i], str) else (row[i] if i < len(row) else '')
        for i, header in enumerate(headers)
        if header
    }


def ensure_project_status_columns(worksheet, headers):
    missing = [column for column in PROJECT_STATUS_COLUMNS if column not in headers]
    if not missing:
        return headers

    start_col = len(headers) + 1
    worksheet.update(
        range_name=f'{chr(64 + start_col)}1:{chr(64 + start_col + len(missing) - 1)}1',
        values=[missing],
    )
    return headers + missing


def validate_project_row(row):
    errors = []
    sheet_id = extract_sheet_id(row.get('Ссылка на документ проекта', ''))
    bot_token = str(row.get('Telegram bot token', '')).strip()
    channel_id = str(row.get('Telegram канал ID', '')).strip()

    if not str(row.get('Код проекта', '')).strip():
        errors.append('missing project code')

    if not str(row.get('Название', '')).strip():
        errors.append('missing project name')

    if not sheet_id:
        errors.append('missing project sheet URL')

    if bot_token and not re.match(r'^\d+:[A-Za-z0-9_-]+$', bot_token):
        errors.append('invalid Telegram bot token format')

    if channel_id and not re.match(r'^-?\d+$', channel_id):
        errors.append('invalid Telegram channel ID format')

    if not bot_token:
        errors.append('missing Telegram bot token')

    if not channel_id:
        errors.append('missing Telegram channel ID')

    return sheet_id, errors


def update_project_statuses(worksheet, headers, status_updates):
    if not status_updates:
        return

    status_col = headers.index('Provisioning status') + 1
    error_col = headers.index('Provisioning error') + 1
    at_col = headers.index('Provisioned at') + 1
    timestamp = datetime.utcnow().isoformat() + 'Z'
    updates = []

    for row_index, status, error_text in status_updates:
        updates.extend([
            {'range': gspread.utils.rowcol_to_a1(row_index, status_col), 'values': [[status]]},
            {'range': gspread.utils.rowcol_to_a1(row_index, error_col), 'values': [[error_text]]},
            {'range': gspread.utils.rowcol_to_a1(row_index, at_col), 'values': [[timestamp]]},
        ])

    worksheet.batch_update(updates)


def update_project_runtime_status(sheet, project, status, error_text=''):
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_PROJECTS)
        values = worksheet.get_all_values()
        if not values:
            return

        headers = ensure_project_status_columns(worksheet, values[0])
        code_col = headers.index('Код проекта') if 'Код проекта' in headers else None
        name_col = headers.index('Название') if 'Название' in headers else None
        target_row = None

        for row_index, row in enumerate(values[1:], start=2):
            row_code = row[code_col].strip() if code_col is not None and len(row) > code_col else ''
            row_name = row[name_col].strip() if name_col is not None and len(row) > name_col else ''
            if row_code == str(project.get('code', '')).strip() or row_name == str(project.get('name', '')).strip():
                target_row = row_index
                break

        if target_row:
            update_project_statuses(worksheet, headers, [(target_row, status, error_text)])
    except Exception as e:
        print(f"  ⚠️  Error updating project runtime status for {project.get('name')}: {type(e).__name__}: {e}")


def authenticate_google_sheets():
    """Аутентификация в Google Sheets через Service Account"""
    if not config.SERVICE_ACCOUNT_JSON:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not found")
    
    credentials_dict = json.loads(config.SERVICE_ACCOUNT_JSON)
    print(f"  🔐 Google service account: {credentials_dict.get('client_email', 'unknown')}")
    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    client = gspread.authorize(credentials)
    return client

def acquire_lock(sheet):
    """Получить блокировку для предотвращения одновременных запусков"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        
        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == 'lock_status':
                lock_row = i + 1
                lock_value = row[1].strip() if len(row) > 1 else ''
                
                if lock_value == 'locked':
                    lock_time_str = row[2] if len(row) > 2 else ''
                    if lock_time_str:
                        try:
                            lock_time = datetime.fromisoformat(lock_time_str.replace('Z', ''))
                            if (datetime.utcnow() - lock_time).total_seconds() > 900:
                                print("  ⚠️  Stale lock detected (>15min), removing...")
                            else:
                                print("  ❌ Another process is running! Exiting...")
                                return False
                        except:
                            pass
                
                current_time = datetime.utcnow().isoformat() + 'Z'
                worksheet.update_cell(lock_row, 2, 'locked')
                worksheet.update_cell(lock_row, 3, current_time)
                print(f"  🔒 Lock acquired at {current_time}")
                return True
        
        current_time = datetime.utcnow().isoformat() + 'Z'
        worksheet.append_row(['lock_status', 'locked', current_time])
        print(f"  🔒 Lock created and acquired")
        return True
        
    except Exception as e:
        print(f"  ❌ Error acquiring lock: {e}")
        return False

def release_lock(sheet):
    """Освободить блокировку"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        
        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == 'lock_status':
                lock_row = i + 1
                worksheet.update_cell(lock_row, 2, 'unlocked')
                worksheet.update_cell(lock_row, 3, datetime.utcnow().isoformat() + 'Z')
                print(f"  🔓 Lock released")
                return
    except Exception as e:
        print(f"  ⚠️  Error releasing lock: {e}")

def save_videos_batch(sheet, videos_data):
    """
    БАТЧЕВОЕ сохранение видео в таблицу с защитой от потери
    videos_data = [(video, project, pub_date, tg_msg_id, error), ...]
    Возвращает список video_id которые удалось сохранить
    """
    if not videos_data:
        return []
    
    try:
        try:
            worksheet = sheet.worksheet(config.SHEET_NAME_VIDEOS)
        except:
            worksheet = sheet.add_worksheet(config.SHEET_NAME_VIDEOS, rows=10000, cols=13)
            headers = [
                'Video ID', 'Название видео', 'Ссылка', 'Название канала', 
                'Channel ID', 'Проект', 'Дата публикации UTC', 'Дата обработки UTC',
                'Опубл. в TG', 'TG message_id', 'Дата публикации TG', 
                'Системный статус', 'Ошибка'
            ]
            worksheet.append_row(headers)
            print(f"  📋 Created 'Глобальные видео' worksheet")
        
        existing_rows = {}
        try:
            values = worksheet.get_all_values()
            for row_index, row in enumerate(values[1:], start=2):
                video_id = row[0].strip() if len(row) > 0 else ''
                project_name = row[5].strip() if len(row) > 5 else ''
                status = row[11].strip().lower() if len(row) > 11 else ''
                if video_id and project_name:
                    existing_rows[(video_id, project_name)] = {
                        'row_index': row_index,
                        'status': status,
                    }
        except Exception as e:
            print(f"  ⚠️  Could not load existing video rows: {e}")

        rows = []
        rows_video_ids = []
        saved_video_ids = []
        for video, project, video_published_date, tg_message_id, error in videos_data:
            key = (video['video_id'], project.get('name', ''))
            existing = existing_rows.get(key)
            is_filtered = str(error or '').startswith('FILTERED: ')
            row_status = 'filtered' if is_filtered else ('published' if tg_message_id else 'pending')
            row_error = str(error or '').replace('FILTERED: ', '', 1) if is_filtered else (error or '')

            if existing:
                if existing['status'] in ('published', 'filtered'):
                    continue

                saved_video_ids.append(video['video_id'])
                continue

            rows.append([
                video['video_id'],
                video.get('title', ''),
                video.get('url', ''),
                video.get('channel', ''),
                video.get('channel_id', ''),
                project.get('name', ''),
                video_published_date,
                datetime.utcnow().isoformat(),
                '1' if tg_message_id else '0',
                str(tg_message_id) if tg_message_id else '',
                datetime.utcnow().isoformat() if tg_message_id else '',
                row_status,
                row_error
            ])
            rows_video_ids.append(video['video_id'])
        
        # Дробим на батчи по config.BATCH_SIZE
        for i in range(0, len(rows), config.BATCH_SIZE):
            batch = rows[i:i+config.BATCH_SIZE]
            batch_video_ids = rows_video_ids[i:i+config.BATCH_SIZE]
            
            try:
                worksheet.append_rows(batch, value_input_option='USER_ENTERED')
                saved_video_ids.extend(batch_video_ids)
                print(f"  💾 Saved batch {i//config.BATCH_SIZE + 1}: {len(batch)} videos")
            except Exception as e:
                print(f"  ⚠️  Batch failed, saving one by one: {e}")
                # Если батч упал - сохраняем по одной (защита от потери)
                for row, video_id in zip(batch, batch_video_ids):
                    try:
                        worksheet.append_row(row, value_input_option='USER_ENTERED')
                        saved_video_ids.append(video_id)
                    except Exception as e2:
                        print(f"  ❌ Failed to save {video_id}: {e2}")
            
            time.sleep(0.5)  # Небольшая пауза между батчами
        
        return saved_video_ids
        
    except Exception as e:
        print(f"  ❌ Critical error in save_videos_batch: {e}")
        return []

def update_video_publication_status(sheet, video_id, project_name, tg_message_id=None, status='published', error=''):
    """Обновление статуса публикации существующей строки видео"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_VIDEOS)
        values = worksheet.get_all_values()
        target_row = None

        for row_index, row in enumerate(values[1:], start=2):
            row_video_id = row[0].strip() if len(row) > 0 else ''
            row_project_name = row[5].strip() if len(row) > 5 else ''
            if row_video_id == video_id and row_project_name == project_name:
                target_row = row_index

        if not target_row:
            print(f"  ⚠️  Could not find video row to update: {video_id} / {project_name}")
            return False

        timestamp = datetime.utcnow().isoformat()
        updates = [
            {'range': gspread.utils.rowcol_to_a1(target_row, 9), 'values': [['1' if tg_message_id else '0']]},
            {'range': gspread.utils.rowcol_to_a1(target_row, 10), 'values': [[str(tg_message_id) if tg_message_id else '']]},
            {'range': gspread.utils.rowcol_to_a1(target_row, 11), 'values': [[timestamp if tg_message_id else '']]},
            {'range': gspread.utils.rowcol_to_a1(target_row, 12), 'values': [[status]]},
            {'range': gspread.utils.rowcol_to_a1(target_row, 13), 'values': [[error or '']]},
        ]
        worksheet.batch_update(updates)
        return True
    except Exception as e:
        print(f"  ⚠️  Error updating publication status for {video_id}: {e}")
        return False

def log_events_batch(sheet, log_entries):
    """Пакетная запись логов"""
    if not log_entries:
        return
    
    try:
        try:
            worksheet = sheet.worksheet('Логи')
        except:
            worksheet = sheet.add_worksheet('Логи', rows=10000, cols=6)
            worksheet.append_row(['Timestamp', 'Проект', 'Событие', 'Video ID', 'Детали', 'Статус'])
        
        # Дробим на батчи
        for i in range(0, len(log_entries), config.BATCH_SIZE):
            batch = log_entries[i:i+config.BATCH_SIZE]
            try:
                worksheet.append_rows(batch, value_input_option='USER_ENTERED')
            except:
                # Fallback - по одной
                for entry in batch:
                    try:
                        worksheet.append_row(entry)
                    except:
                        pass
        
        print(f"  📝 Logged {len(log_entries)} events")
    except Exception as e:
        print(f"  ⚠️  Error batch logging: {e}")

def group_contiguous_ranges(row_indexes):
    """Преобразует номера строк в непрерывные диапазоны для batch delete"""
    if not row_indexes:
        return []

    ranges = []
    start = row_indexes[0]
    end = row_indexes[0]

    for row_index in row_indexes[1:]:
        if row_index == end + 1:
            end = row_index
        else:
            ranges.append((start, end))
            start = row_index
            end = row_index

    ranges.append((start, end))
    return ranges

def delete_rows_batch(spreadsheet, worksheet, row_indexes):
    """Удаляет строки батчами через Sheets API, чтобы не упираться в per-minute write quota"""
    if not row_indexes:
        return 0

    ranges = group_contiguous_ranges(sorted(row_indexes))
    requests = []

    for start_row, end_row in sorted(ranges, reverse=True):
        requests.append({
            'deleteDimension': {
                'range': {
                    'sheetId': worksheet.id,
                    'dimension': 'ROWS',
                    'startIndex': start_row - 1,
                    'endIndex': end_row,
                }
            }
        })

    deleted_rows = len(row_indexes)
    for i in range(0, len(requests), config.BATCH_SIZE):
        spreadsheet.batch_update({'requests': requests[i:i + config.BATCH_SIZE]})
        time.sleep(0.5)

    return deleted_rows

def cleanup_old_records(sheet):
    """Очистка старых записей"""
    try:
        worksheet_settings = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = worksheet_settings.get_all_values()
        
        last_cleanup = None
        last_cleanup_row = None
        last_cleanup_retention_days = None
        last_cleanup_retention_row = None
        
        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == 'last_cleanup':
                last_cleanup_row = i + 1
                if len(row) > 1 and row[1]:
                    try:
                        last_cleanup = datetime.fromisoformat(row[1].replace('Z', ''))
                    except:
                        pass
            elif len(row) > 0 and row[0].strip() == 'last_cleanup_retention_days':
                last_cleanup_retention_row = i + 1
                if len(row) > 1 and row[1]:
                    try:
                        last_cleanup_retention_days = int(row[1])
                    except:
                        pass
        
        retention_changed = last_cleanup_retention_days != config.CLEANUP_AFTER_DAYS
        if last_cleanup and not retention_changed and (datetime.utcnow() - last_cleanup).total_seconds() < 86400:
            print(f"  ⏭️  Cleanup skipped (last run: {last_cleanup.isoformat()}Z)")
            return
        if retention_changed:
            print(f"  🔁 Cleanup retention changed: {last_cleanup_retention_days} -> {config.CLEANUP_AFTER_DAYS} days")
        
        print("\n🧹 Cleaning up old records...")
        
        cutoff_date = datetime.utcnow() - timedelta(days=config.CLEANUP_AFTER_DAYS)
        print(f"  Removing records older than: {cutoff_date.isoformat()}Z")
        
        deleted_videos = 0
        try:
            worksheet = sheet.worksheet(config.SHEET_NAME_VIDEOS)
            values = worksheet.get_all_values()
            
            rows_to_delete = []
            for i, row in enumerate(values):
                if i == 0:
                    continue
                
                if len(row) > 7:
                    date_str = row[7]
                    try:
                        record_date = datetime.fromisoformat(date_str.replace('Z', ''))
                        if record_date < cutoff_date:
                            rows_to_delete.append(i + 1)
                    except:
                        pass
            
            deleted_videos = delete_rows_batch(sheet, worksheet, rows_to_delete)
            
            if deleted_videos > 0:
                print(f"  ✅ Deleted {deleted_videos} old videos")
        except Exception as e:
            print(f"  ⚠️  Error cleaning videos: {e}")
        
        deleted_logs = 0
        try:
            worksheet = sheet.worksheet('Логи')
            values = worksheet.get_all_values()
            
            rows_to_delete = []
            for i, row in enumerate(values):
                if i == 0:
                    continue
                
                if len(row) > 0:
                    date_str = row[0]
                    try:
                        record_date = datetime.fromisoformat(date_str.replace('Z', ''))
                        if record_date < cutoff_date:
                            rows_to_delete.append(i + 1)
                    except:
                        pass
            
            deleted_logs = delete_rows_batch(sheet, worksheet, rows_to_delete)
            
            if deleted_logs > 0:
                print(f"  ✅ Deleted {deleted_logs} old logs")
        except Exception as e:
            print(f"  ⚠️  Error cleaning logs: {e}")

        deleted_push_events = 0
        try:
            worksheet = sheet.worksheet(config.SHEET_NAME_PUSH_EVENTS)
            values = worksheet.get_all_values()

            rows_to_delete = []
            for i, row in enumerate(values):
                if i == 0:
                    continue

                if len(row) > 0:
                    date_str = row[0]
                    try:
                        record_date = datetime.fromisoformat(date_str.replace('Z', ''))
                        if record_date < cutoff_date:
                            rows_to_delete.append(i + 1)
                    except:
                        pass

            deleted_push_events = delete_rows_batch(sheet, worksheet, rows_to_delete)

            if deleted_push_events > 0:
                print(f"  ✅ Deleted {deleted_push_events} old push events")
        except Exception as e:
            print(f"  ⚠️  Error cleaning push events: {e}")
        
        current_time = datetime.utcnow().isoformat() + 'Z'
        if last_cleanup_row:
            worksheet_settings.update_cell(last_cleanup_row, 2, current_time)
        else:
            worksheet_settings.append_row(['last_cleanup', current_time, 'Последняя очистка старых записей'])

        if last_cleanup_retention_row:
            worksheet_settings.update_cell(last_cleanup_retention_row, 2, str(config.CLEANUP_AFTER_DAYS))
        else:
            worksheet_settings.append_row([
                'last_cleanup_retention_days',
                str(config.CLEANUP_AFTER_DAYS),
                'Retention window used by the last cleanup run',
            ])
        
        print(f"  ✅ Cleanup completed: {deleted_videos} videos, {deleted_logs} logs, {deleted_push_events} push events")
        
    except Exception as e:
        print(f"  ❌ Cleanup error: {e}")

def load_settings(sheet):
    """Загрузка настроек"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        records = worksheet.get_all_records()
        
        settings = {}
        for row in records:
            key = row.get('Параметр', '')
            value = row.get('Значение', '')
            
            if isinstance(key, str):
                key = key.strip()
            else:
                key = str(key).strip()
            
            if isinstance(value, str):
                value = value.strip()
            else:
                value = str(value).strip()
            
            if key and value:
                settings[key] = value
        
        if 'youtube_api_key' in settings:
            config.YOUTUBE_API_KEY = settings['youtube_api_key']
            print(f"  ✅ YouTube API key loaded")
        
        if 'max_video_age_hours' in settings:
            config.MAX_VIDEO_AGE_HOURS = int(settings['max_video_age_hours'])
            print(f"  ✅ Max video age: {config.MAX_VIDEO_AGE_HOURS}h ({config.MAX_VIDEO_AGE_HOURS//24}d)")

        if 'max_publish_age_hours' in settings:
            config.MAX_PUBLISH_AGE_HOURS = int(settings['max_publish_age_hours'])
            print(f"  ✅ Max publish age: {config.MAX_PUBLISH_AGE_HOURS}h")

        if 'rss_fallback_age_hours' in settings:
            config.RSS_FALLBACK_AGE_HOURS = int(settings['rss_fallback_age_hours'])
            print(f"  ✅ RSS fallback age: {config.RSS_FALLBACK_AGE_HOURS}h")
        
        if 'default_template' in settings:
            config.DEFAULT_MESSAGE_TEMPLATE = settings['default_template']
            print(f"  ✅ Default template loaded")
        
        return settings
    except Exception as e:
        print(f"  ❌ Error loading settings: {e}")
        return {}

def update_youtube_quota(sheet, calls_used):
    """Обновление счётчика YouTube API квоты"""
    if calls_used == 0:
        return
    
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        
        quota_row = None
        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == 'youtube_quota_used':
                quota_row = i + 1
                current_quota = int(row[1]) if (len(row) > 1 and row[1].isdigit()) else 0
                new_quota = current_quota + calls_used
                worksheet.update_cell(quota_row, 2, str(new_quota))
                print(f"  📊 YouTube API quota updated: {current_quota} + {calls_used} = {new_quota}")
                return
        
        if quota_row is None:
            worksheet.append_row(['youtube_quota_used', str(calls_used), 'Счётчик использованных единиц YouTube API'])
            print(f"  ✅ Created 'youtube_quota_used' with value: {calls_used}")
            
    except Exception as e:
        print(f"  ❌ Error updating quota: {e}")

def update_last_run(sheet):
    """Обновление времени последнего запуска"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        
        last_run_row = None
        current_time = datetime.utcnow().isoformat() + 'Z'
        
        for i, row in enumerate(values):
            if i == 0:
                continue
            if len(row) > 0 and row[0].strip() == 'last_run':
                last_run_row = i + 1
                worksheet.update_cell(last_run_row, 2, current_time)
                print(f"  🕐 Last run updated: {current_time}")
                return
        
        if last_run_row is None:
            worksheet.append_row(['last_run', current_time, 'Последний запуск обработки'])
            print(f"  ✅ Created 'last_run': {current_time}")
            
    except Exception as e:
        print(f"  ❌ Error updating last_run: {e}")

def load_projects(sheet):
    """Загрузка активных проектов"""
    worksheet = sheet.worksheet(config.SHEET_NAME_PROJECTS)
    values = worksheet.get_all_values()

    if not values:
        return []

    headers = ensure_project_status_columns(worksheet, values[0])
    projects = []
    status_updates = []

    def queue_status(row_index, row, status, error_text):
        current_status = str(row.get('Provisioning status', '')).strip()
        current_error = str(row.get('Provisioning error', '')).strip()
        if current_status != status or current_error != error_text:
            status_updates.append((row_index, status, error_text))

    for row_index, raw_row in enumerate(values[1:], start=2):
        if any(str(cell).strip() == '🔵' for cell in raw_row):
            break

        if not any(str(cell).strip() for cell in raw_row):
            continue

        row = normalize_project_row(headers, raw_row)
        status = row.get('Активен', '')
        sheet_id, errors = validate_project_row(row)

        if status == '🟢':
            if errors:
                error_text = '; '.join(errors)
                queue_status(row_index, row, 'error', error_text)
                print(f"  ⚠️  Project row {row_index} skipped: {error_text}")
                continue

            stop_words_str = str(row.get('Стоп-слова (через запятую)', '')).strip()
            stop_words = [w.strip().lower() for w in stop_words_str.split(',') if w.strip()] if stop_words_str else []
            tg_channel = str(row.get('Telegram канал @', '') or row.get('Telegram канал', '')).strip()
            channels_sheet_name = str(row.get('Название листа', '')).strip()

            projects.append({
                'code': row.get('Код проекта'),
                'name': row.get('Название'),
                'sheet_id': sheet_id,
                'channels_sheet_name': channels_sheet_name,
                'bot_token': row.get('Telegram bot token'),
                'channel_id': str(row.get('Telegram канал ID')),
                'tg_channel': tg_channel,
                'default_template': row.get('Шаблон по умолчанию', config.DEFAULT_MESSAGE_TEMPLATE),
                'stop_words': stop_words
            })
            queue_status(row_index, row, 'ready', '')
        else:
            queue_status(row_index, row, 'inactive', '')

    update_project_statuses(worksheet, headers, status_updates)
    
    print(f"  ✅ Loaded {len(projects)} active projects")
    return projects

def load_youtube_channels(client, project):
    """Загрузка активных YouTube каналов проекта"""
    project.pop('channels_error', None)
    try:
        sheet = client.open_by_key(project['sheet_id'])
        configured_name = project.get('channels_sheet_name', '')
        preferred_names = [configured_name] if configured_name else []
        candidate_worksheets = []
        seen_sheet_ids = set()

        for name in [name for name in preferred_names if name]:
            try:
                candidate = sheet.worksheet(name)
                if candidate.id not in seen_sheet_ids:
                    candidate_worksheets.append(candidate)
                    seen_sheet_ids.add(candidate.id)
            except Exception as e:
                print(f"  ⚠️  Channels sheet '{name}' not available for {project['name']}: {type(e).__name__}")
                continue

        if not candidate_worksheets:
            try:
                worksheets = sheet.worksheets()
                if worksheets:
                    candidate_worksheets.append(worksheets[0])
            except Exception as e:
                print(f"  ⚠️  Could not load first worksheet for {project['name']}: {type(e).__name__}: {e}")

        for worksheet in candidate_worksheets:
            channels = parse_youtube_channels_worksheet(worksheet, project)
            if channels:
                return channels

        print(f"  ⚠️  No active channels parsed for {project['name']}")
        project['channels_error'] = 'no active channels parsed'
        return {}
    except Exception as e:
        error_text = f"{type(e).__name__}: {e}"
        print(f"  ❌ Error loading channels for {project['name']}: {error_text}")
        project['channels_error'] = error_text
        return {}

def parse_youtube_channels_worksheet(worksheet, project):
    try:
        print(f"  📄 Channels sheet: {worksheet.title}")
        values = worksheet.get_all_values()
        channels = {}
        for i, row in enumerate(values):
            if i == 0:
                continue

            normalized = [str(cell).strip() for cell in row]
            if not any(normalized):
                continue

            if any(cell == '🔵' for cell in normalized):
                break

            if '🟢' not in normalized:
                continue

            channel_id = extract_youtube_channel_id_from_row(normalized)
            if not channel_id:
                print(f"  ⚠️  Active row {i + 1} has no YouTube channel ID")
                continue

            channel_name = infer_channel_name(normalized, channel_id)
            channel_template = normalized[20] if len(normalized) > 20 else ''
            tg_channel_link = normalized[21] if len(normalized) > 21 else ''

            channels[channel_id] = {
                'name': channel_name,
                'template': channel_template,
                'tg_channel': tg_channel_link
            }

        return channels
    except Exception as e:
        print(f"  ⚠️  Error reading channels sheet '{worksheet.title}' for {project['name']}: {type(e).__name__}: {e}")
        return {}

def extract_youtube_channel_id_from_row(row):
    for cell in row:
        match = re.search(r'(UC[0-9A-Za-z_-]{20,})', cell)
        if match:
            return match.group(1)
    return ''

def infer_channel_name(row, channel_id):
    ignored = {'🟢', '🔴', '🔵', channel_id}
    for cell in row:
        if not cell or cell in ignored:
            continue
        if 'youtube.com' in cell or 'youtu.be' in cell:
            continue
        if channel_id in cell:
            continue
        return cell
    return channel_id

def get_all_active_channels(client, projects):
    """Получение всех уникальных активных каналов"""
    all_channels = {}
    
    for project in projects:
        channels = load_youtube_channels(client, project)
        for ch_id, ch_info in channels.items():
            if ch_id not in all_channels:
                all_channels[ch_id] = {
                    'channel_info': ch_info,
                    'projects': [],
                }
            all_channels[ch_id]['projects'].append(project['name'])
    
    return all_channels

def get_published_videos(sheet):
    """Получение списка уже опубликованных видео"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_VIDEOS)
        records = worksheet.get_all_records()
        terminal_statuses = {'published', 'filtered'}
        published = set(
            row.get('Video ID', '')
            for row in records
            if row.get('Video ID') and str(row.get('Системный статус', '')).strip().lower() in terminal_statuses
        )
        print(f"  📋 Found {len(published)} terminal videos in table")
        return published
    except Exception as e:
        print(f"  ⚠️  Error loading published videos: {e}")
        return set()

def get_push_events(sheet):
    """Получение необработанных push-событий"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_PUSH_EVENTS)
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
        print(f"❌ Error loading push events: {e}")
        return []

def mark_push_event_processed(sheet, row_index, project_name):
    """Отметка push-события как обработанного"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_PUSH_EVENTS)
        worksheet.update_cell(row_index, 4, '✅')
        
        current_projects = worksheet.cell(row_index, 5).value or ''
        if project_name not in current_projects:
            new_projects = (current_projects + ', ' + project_name).strip(', ')
            worksheet.update_cell(row_index, 5, new_projects)
    except Exception as e:
        print(f"  ⚠️  Error marking event: {e}")
