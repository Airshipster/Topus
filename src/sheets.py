import json
import os
import re
import time
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials

import config


PROJECT_STATUS_COLUMNS = [
    'RSS delete limit',
    'Provisioning status',
    'Provisioning error',
    'Provisioned at',
]

GLOBAL_VIDEOS_HEADERS = [
    'Video ID', 'Название видео', 'Ссылка', 'Название канала',
    'Channel ID', 'Проект', 'Дата публикации UTC', 'Дата обработки UTC',
    'Опубл. в TG', 'TG message_id', 'Дата публикации TG',
    'Системный статус', 'Ошибка'
]

SETTINGS_MARKER = 'Настройки'

NUMERIC_CLEANUP_HEADERS = {
    'просмотры',
    'просмотров',
    'лайки',
    'комменты',
    'комментарии',
    'видео',
    'подписчики',
    'опубл. в tg',
    'project count',
}

TIMESTAMP_CLEANUP_HEADERS = {
    'timestamp',
    'timestamp (utc)',
    'дата публикации utc',
    'дата обработки utc',
    'дата публикации tg',
    'provisioned at',
    'subscribed at',
    'last renewed',
}


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


def parse_list_setting(value):
    """Split comma- and newline-separated settings into normalized values."""
    if value is None:
        return []

    return [
        item.strip()
        for item in re.split(r'[,\n\r]+', str(value))
        if item.strip()
    ]


def format_timestamp(dt=None):
    if dt is None:
        dt = datetime.utcnow()
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def parse_datetime_value(value):
    if not value:
        return None

    text = str(value).strip().lstrip("'")
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(text.replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return None


def column_letter(column_index):
    letters = ''
    while column_index:
        column_index, remainder = divmod(column_index - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def find_settings_table(values):
    marker_index = None
    for index, row in enumerate(values):
        if any(str(cell).strip() == SETTINGS_MARKER for cell in row):
            marker_index = index
            break

    if marker_index is None:
        return None

    header_index = None
    for index in range(marker_index + 1, min(len(values), marker_index + 6)):
        normalized = [str(cell).strip() for cell in values[index]]
        if 'Параметр' in normalized and 'Значение' in normalized:
            header_index = index
            break

    if header_index is None:
        return None

    headers = [str(cell).strip() for cell in values[header_index]]
    return {
        'marker_index': marker_index,
        'header_index': header_index,
        'key_col': headers.index('Параметр') + 1,
        'value_col': headers.index('Значение') + 1,
        'description_col': headers.index('Описание') + 1 if 'Описание' in headers else None,
        'first_data_row': header_index + 2,
    }


def iter_settings_rows(values, table):
    if not table:
        return

    key_index = table['key_col'] - 1
    value_index = table['value_col'] - 1
    description_index = table['description_col'] - 1 if table.get('description_col') else None

    for row_number, row in enumerate(values[table['first_data_row'] - 1:], start=table['first_data_row']):
        key = str(row[key_index]).strip() if len(row) > key_index else ''
        value = str(row[value_index]).strip() if len(row) > value_index else ''
        description = str(row[description_index]).strip() if description_index is not None and len(row) > description_index else ''
        if key:
            yield row_number, key, value, description


def find_setting_row(values, key):
    table = find_settings_table(values)
    if not table:
        return None, None

    for row_number, row_key, value, description in iter_settings_rows(values, table):
        if row_key == key:
            return {
                'row_number': row_number,
                'value': value,
                'description': description,
            }, table

    return None, table


def update_setting_value(worksheet, key, value, description=''):
    values = worksheet.get_all_values()
    existing, table = find_setting_row(values, key)

    if not table:
        worksheet.append_row([key, value, description])
        return

    if existing:
        worksheet.update_cell(existing['row_number'], table['value_col'], value)
        if description and table.get('description_col') and not existing.get('description'):
            worksheet.update_cell(existing['row_number'], table['description_col'], description)
        return

    append_row = [''] * max(table['description_col'] or 0, table['value_col'])
    append_row[table['key_col'] - 1] = key
    append_row[table['value_col'] - 1] = value
    if description and table.get('description_col'):
        append_row[table['description_col'] - 1] = description
    worksheet.append_row(append_row, value_input_option='USER_ENTERED')


def ensure_global_videos_worksheet(sheet):
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_VIDEOS)
    except Exception:
        worksheet = sheet.add_worksheet(config.SHEET_NAME_VIDEOS, rows=10000, cols=len(GLOBAL_VIDEOS_HEADERS))
        worksheet.append_row(GLOBAL_VIDEOS_HEADERS)
        print(f"  📋 Created 'Глобальные видео' worksheet")
        return worksheet, GLOBAL_VIDEOS_HEADERS

    values = worksheet.get_all_values()
    if not values:
        worksheet.append_row(GLOBAL_VIDEOS_HEADERS)
        return worksheet, GLOBAL_VIDEOS_HEADERS

    current_headers = values[0]
    target_headers = GLOBAL_VIDEOS_HEADERS + [''] * max(0, len(current_headers) - len(GLOBAL_VIDEOS_HEADERS))
    if current_headers[:len(target_headers)] != target_headers:
        worksheet.update(
            range_name=f'A1:{column_letter(len(target_headers))}1',
            values=[target_headers],
        )
    return worksheet, GLOBAL_VIDEOS_HEADERS


def header_index(headers, name):
    return headers.index(name) if name in headers else None


def strip_leading_apostrophe(value):
    text = str(value)
    if not text.startswith("'"):
        return value

    stripped = text[1:].strip()
    if re.fullmatch(r'-?\d+(?:[.,]\d+)?', stripped):
        return stripped.replace(',', '.')

    return value


def clean_numeric_text_values(worksheet):
    """Remove Sheets text-prefix apostrophes only from known numeric columns."""
    try:
        values = worksheet.get_all_values()
        if len(values) < 2:
            return 0

        headers = [str(cell).strip().lower() for cell in values[0]]
        numeric_cols = {
            index
            for index, header in enumerate(headers)
            if header in NUMERIC_CLEANUP_HEADERS
        }
        if not numeric_cols:
            return 0

        updates = []
        for row_index, row in enumerate(values[1:], start=2):
            for col_index in numeric_cols:
                if col_index >= len(row):
                    continue
                cleaned = strip_leading_apostrophe(row[col_index])
                if cleaned != row[col_index]:
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(row_index, col_index + 1),
                        'values': [[cleaned]],
                    })

        for i in range(0, len(updates), config.BATCH_SIZE):
            worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
            time.sleep(0.2)

        return len(updates)
    except Exception as e:
        print(f"  ⚠️  Error cleaning numeric values in {worksheet.title}: {e}")
        return 0


def clean_timestamp_text_values(worksheet):
    """Rewrite ISO timestamps to YYYY-MM-DD HH:MM:SS in known timestamp columns."""
    try:
        values = worksheet.get_all_values()
        if len(values) < 2:
            return 0

        headers = [str(cell).strip().lower() for cell in values[0]]
        timestamp_cols = {
            index
            for index, header in enumerate(headers)
            if header in TIMESTAMP_CLEANUP_HEADERS
        }
        if not timestamp_cols:
            return 0

        updates = []
        for row_index, row in enumerate(values[1:], start=2):
            for col_index in timestamp_cols:
                if col_index >= len(row):
                    continue

                value = str(row[col_index]).strip()
                normalized_value = value.lstrip("'")
                if not normalized_value:
                    continue

                parsed = parse_datetime_value(value)
                if not parsed:
                    continue

                cleaned = format_timestamp(parsed)
                if cleaned != normalized_value:
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(row_index, col_index + 1),
                        'values': [[cleaned]],
                    })

        for i in range(0, len(updates), config.BATCH_SIZE):
            worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
            time.sleep(0.2)

        return len(updates)
    except Exception as e:
        print(f"  ⚠️  Error cleaning timestamp values in {worksheet.title}: {e}")
        return 0


def clean_master_numeric_text_values(sheet):
    """Clean numeric prefixes and timestamp text in master sheets without touching formatting."""
    cleaned_total = 0
    for worksheet in sheet.worksheets():
        cleaned_total += clean_numeric_text_values(worksheet)
        cleaned_total += clean_timestamp_text_values(worksheet)

    if cleaned_total:
        print(f"  ✅ Cleaned text-formatted values: {cleaned_total} cells")


def update_project_statuses(worksheet, headers, status_updates):
    if not status_updates:
        return

    status_col = headers.index('Provisioning status') + 1
    error_col = headers.index('Provisioning error') + 1
    at_col = headers.index('Provisioned at') + 1
    timestamp = format_timestamp()
    updates = []

    for row_index, status, error_text in status_updates:
        updates.extend([
            {'range': gspread.utils.rowcol_to_a1(row_index, status_col), 'values': [[status]]},
            {'range': gspread.utils.rowcol_to_a1(row_index, error_col), 'values': [[error_text]]},
            {'range': gspread.utils.rowcol_to_a1(row_index, at_col), 'values': [[timestamp]]},
        ])

    worksheet.batch_update(updates)


def update_project_default_values(worksheet, headers, default_updates):
    if not default_updates:
        return

    updates = []
    for row_index, column_name, value in default_updates:
        if column_name not in headers:
            continue
        updates.append({
            'range': gspread.utils.rowcol_to_a1(row_index, headers.index(column_name) + 1),
            'values': [[value]],
        })

    if updates:
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
        
        existing, table = find_setting_row(values, 'lock_status')
        if existing and table:
                lock_row = existing['row_number']
                lock_value = existing['value']
                
                if lock_value == 'locked':
                    lock_time_str = existing.get('description', '')
                    if lock_time_str:
                        try:
                            lock_time = parse_datetime_value(lock_time_str)
                            if (datetime.utcnow() - lock_time).total_seconds() > 900:
                                print("  ⚠️  Stale lock detected (>15min), removing...")
                            else:
                                print("  ❌ Another process is running! Exiting...")
                                return False
                        except:
                            pass
                
                current_time = format_timestamp()
                worksheet.update_cell(lock_row, table['value_col'], 'locked')
                if table.get('description_col'):
                    worksheet.update_cell(lock_row, table['description_col'], current_time)
                print(f"  🔒 Lock acquired at {current_time}")
                return True
        
        current_time = format_timestamp()
        update_setting_value(worksheet, 'lock_status', 'locked', current_time)
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
        
        existing, table = find_setting_row(values, 'lock_status')
        if existing and table:
            worksheet.update_cell(existing['row_number'], table['value_col'], 'unlocked')
            if table.get('description_col'):
                worksheet.update_cell(existing['row_number'], table['description_col'], format_timestamp())
            print(f"  🔓 Lock released")
            return
    except Exception as e:
        print(f"  ⚠️  Error releasing lock: {e}")

def save_videos_batch(sheet, videos_data):
    """
    БАТЧЕВОЕ сохранение видео в таблицу с защитой от потери
    videos_data = [(video, project, pub_date, tg_msg_id, error), ...]
    Возвращает список (video_id, project_name), которые были созданы в этом запуске
    """
    if not videos_data:
        return []
    
    try:
        worksheet, headers = ensure_global_videos_worksheet(sheet)
        video_id_col = header_index(headers, 'Video ID')
        project_col = header_index(headers, 'Проект')
        status_col = header_index(headers, 'Системный статус')
        
        existing_rows = {}
        try:
            values = worksheet.get_all_values()
            for row_index, row in enumerate(values[1:], start=2):
                video_id = row[video_id_col].strip() if video_id_col is not None and len(row) > video_id_col else ''
                project_name = row[project_col].strip() if project_col is not None and len(row) > project_col else ''
                status = row[status_col].strip().lower() if status_col is not None and len(row) > status_col else ''
                if video_id and project_name:
                    existing_rows[(video_id, project_name)] = {
                        'row_index': row_index,
                        'status': status,
                    }
        except Exception as e:
            print(f"  ⚠️  Could not load existing video rows: {e}")

        rows = []
        rows_publication_keys = []
        saved_publication_keys = []
        for video, project, video_published_date, tg_message_id, error in videos_data:
            project_name = project.get('name', '')
            key = (video['video_id'], project_name)
            existing = existing_rows.get(key)
            is_filtered = str(error or '').startswith('FILTERED: ')
            row_status = 'filtered' if is_filtered else ('published' if tg_message_id else 'pending')
            row_error = str(error or '').replace('FILTERED: ', '', 1) if is_filtered else (error or '')

            if existing:
                print(f"  ⏭️  Already tracked: {video['video_id']} / {project_name} ({existing['status'] or 'no status'})")
                continue

            rows.append([
                video['video_id'],
                video.get('title', ''),
                video.get('url', ''),
                video.get('channel', ''),
                video.get('channel_id', ''),
                project_name,
                video_published_date,
                format_timestamp(),
                '1' if tg_message_id else '0',
                str(tg_message_id) if tg_message_id else '',
                format_timestamp() if tg_message_id else '',
                row_status,
                row_error
            ])
            rows_publication_keys.append(key)
        
        # Дробим на батчи по config.BATCH_SIZE
        for i in range(0, len(rows), config.BATCH_SIZE):
            batch = rows[i:i+config.BATCH_SIZE]
            batch_publication_keys = rows_publication_keys[i:i+config.BATCH_SIZE]
            
            try:
                worksheet.append_rows(batch, value_input_option='USER_ENTERED')
                saved_publication_keys.extend(batch_publication_keys)
                print(f"  💾 Saved batch {i//config.BATCH_SIZE + 1}: {len(batch)} videos")
            except Exception as e:
                print(f"  ⚠️  Batch failed, saving one by one: {e}")
                # Если батч упал - сохраняем по одной (защита от потери)
                for row, key in zip(batch, batch_publication_keys):
                    try:
                        worksheet.append_row(row, value_input_option='USER_ENTERED')
                        saved_publication_keys.append(key)
                    except Exception as e2:
                        print(f"  ❌ Failed to save {key[0]} / {key[1]}: {e2}")
            
            time.sleep(0.5)  # Небольшая пауза между батчами
        
        return saved_publication_keys
        
    except Exception as e:
        print(f"  ❌ Critical error in save_videos_batch: {e}")
        return []

def update_video_publication_status(sheet, video_id, project_name, tg_message_id=None, status='published', error=''):
    """Обновление статуса публикации существующей строки видео"""
    try:
        worksheet, headers = ensure_global_videos_worksheet(sheet)
        values = worksheet.get_all_values()
        target_row = None
        video_id_col = header_index(headers, 'Video ID')
        project_col = header_index(headers, 'Проект')
        published_col = header_index(headers, 'Опубл. в TG')
        message_col = header_index(headers, 'TG message_id')
        published_at_col = header_index(headers, 'Дата публикации TG')
        status_col = header_index(headers, 'Системный статус')
        error_col = header_index(headers, 'Ошибка')

        for row_index, row in enumerate(values[1:], start=2):
            row_video_id = row[video_id_col].strip() if video_id_col is not None and len(row) > video_id_col else ''
            row_project_name = row[project_col].strip() if project_col is not None and len(row) > project_col else ''
            if row_video_id == video_id and row_project_name == project_name:
                target_row = row_index

        if not target_row:
            print(f"  ⚠️  Could not find video row to update: {video_id} / {project_name}")
            return False

        timestamp = format_timestamp()
        updates = [
            {'range': gspread.utils.rowcol_to_a1(target_row, published_col + 1), 'values': [['1' if tg_message_id else '0']]},
            {'range': gspread.utils.rowcol_to_a1(target_row, message_col + 1), 'values': [[str(tg_message_id) if tg_message_id else '']]},
            {'range': gspread.utils.rowcol_to_a1(target_row, published_at_col + 1), 'values': [[timestamp if tg_message_id else '']]},
            {'range': gspread.utils.rowcol_to_a1(target_row, status_col + 1), 'values': [[status]]},
            {'range': gspread.utils.rowcol_to_a1(target_row, error_col + 1), 'values': [[error or '']]},
        ]
        worksheet.batch_update(updates)
        return True
    except Exception as e:
        print(f"  ⚠️  Error updating publication status for {video_id}: {e}")
        return False


def get_recent_published_video_rows(sheet, project_name, hours=24):
    """Строки опубликованных видео за последние часы для сверки с RSS."""
    try:
        worksheet, headers = ensure_global_videos_worksheet(sheet)
        values = worksheet.get_all_values()
        cutoff = datetime.utcnow() - timedelta(hours=hours)

        indexes = {
            name: header_index(headers, name)
            for name in (
                'Video ID', 'Channel ID', 'Проект', 'Дата публикации UTC',
                'Дата обработки UTC', 'Опубл. в TG', 'TG message_id', 'Системный статус'
            )
        }

        rows = []
        for row_index, row in enumerate(values[1:], start=2):
            row_project = row[indexes['Проект']].strip() if indexes['Проект'] is not None and len(row) > indexes['Проект'] else ''
            if row_project != project_name:
                continue

            status = row[indexes['Системный статус']].strip().lower() if indexes['Системный статус'] is not None and len(row) > indexes['Системный статус'] else ''
            published_flag = row[indexes['Опубл. в TG']].strip() if indexes['Опубл. в TG'] is not None and len(row) > indexes['Опубл. в TG'] else ''
            message_id = row[indexes['TG message_id']].strip() if indexes['TG message_id'] is not None and len(row) > indexes['TG message_id'] else ''
            if status != 'published' or published_flag != '1' or not message_id:
                continue

            date_value = ''
            for date_col in ('Дата публикации UTC', 'Дата обработки UTC'):
                col = indexes[date_col]
                if col is not None and len(row) > col and row[col]:
                    date_value = row[col]
                    break
            record_date = parse_datetime_value(date_value)
            if not record_date or record_date < cutoff:
                continue

            video_id = row[indexes['Video ID']].strip() if indexes['Video ID'] is not None and len(row) > indexes['Video ID'] else ''
            channel_id = row[indexes['Channel ID']].strip() if indexes['Channel ID'] is not None and len(row) > indexes['Channel ID'] else ''
            if video_id and channel_id:
                rows.append({
                    'row_index': row_index,
                    'video_id': video_id,
                    'channel_id': channel_id,
                    'message_id': message_id,
                })

        return rows
    except Exception as e:
        print(f"  ⚠️  Error loading recent published rows: {e}")
        return []

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

        last_cleanup_row, _ = find_setting_row(values, 'last_cleanup')
        last_cleanup = parse_datetime_value(last_cleanup_row.get('value', '')) if last_cleanup_row else None
        last_cleanup_retention_days = None
        retention_row, _ = find_setting_row(values, 'last_cleanup_retention_days')
        if retention_row:
            try:
                last_cleanup_retention_days = int(retention_row.get('value', ''))
            except:
                pass
        
        retention_changed = last_cleanup_retention_days != config.CLEANUP_AFTER_DAYS
        if last_cleanup and not retention_changed and (datetime.utcnow() - last_cleanup).total_seconds() < 86400:
            print(f"  ⏭️  Cleanup skipped (last run: {format_timestamp(last_cleanup)})")
            return
        if retention_changed:
            print(f"  🔁 Cleanup retention changed: {last_cleanup_retention_days} -> {config.CLEANUP_AFTER_DAYS} days")
        
        print("\n🧹 Cleaning up old records...")
        
        cutoff_date = datetime.utcnow() - timedelta(days=config.CLEANUP_AFTER_DAYS)
        print(f"  Removing records older than: {format_timestamp(cutoff_date)}")
        
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
                        record_date = parse_datetime_value(date_str)
                        if record_date and record_date < cutoff_date:
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
                        record_date = parse_datetime_value(date_str)
                        if record_date and record_date < cutoff_date:
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
                        record_date = parse_datetime_value(date_str)
                        if record_date and record_date < cutoff_date:
                            rows_to_delete.append(i + 1)
                    except:
                        pass

            deleted_push_events = delete_rows_batch(sheet, worksheet, rows_to_delete)

            if deleted_push_events > 0:
                print(f"  ✅ Deleted {deleted_push_events} old push events")
        except Exception as e:
            print(f"  ⚠️  Error cleaning push events: {e}")
        
        update_setting_value(worksheet_settings, 'last_cleanup', format_timestamp(), 'Последняя очистка старых записей')
        update_setting_value(
            worksheet_settings,
            'last_cleanup_retention_days',
            str(config.CLEANUP_AFTER_DAYS),
            'Retention window used by the last cleanup run',
        )
        
        print(f"  ✅ Cleanup completed: {deleted_videos} videos, {deleted_logs} logs, {deleted_push_events} push events")
        
    except Exception as e:
        print(f"  ❌ Cleanup error: {e}")

def load_settings(sheet):
    """Загрузка настроек"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        table = find_settings_table(values)
        
        settings = {}
        if not table:
            print("  ⚠️  Settings block marker not found")
            return settings

        for _, key, value, _ in iter_settings_rows(values, table):
            if key and value:
                settings[key] = value
        
        api_keys_value = settings.get('youtube_api_keys') or settings.get('youtube_api_key') or ''
        api_keys = parse_list_setting(api_keys_value or '')
        env_api_keys = parse_list_setting(os.environ.get('YOUTUBE_API_KEYS') or os.environ.get('YOUTUBE_API_KEY') or '')
        if env_api_keys:
            api_keys = env_api_keys

        config.YOUTUBE_API_KEYS = api_keys
        config.YOUTUBE_API_KEY = api_keys[0] if api_keys else None
        if api_keys:
            print(f"  ✅ YouTube API keys loaded: {len(api_keys)}")
        
        if 'max_video_age_hours' in settings:
            config.MAX_VIDEO_AGE_HOURS = int(settings['max_video_age_hours'])
            print(f"  ✅ Max video age: {config.MAX_VIDEO_AGE_HOURS}h ({config.MAX_VIDEO_AGE_HOURS//24}d)")

        if 'max_publish_age_hours' in settings:
            config.MAX_PUBLISH_AGE_HOURS = int(settings['max_publish_age_hours'])
            print(f"  ✅ Max publish age: {config.MAX_PUBLISH_AGE_HOURS}h")

        if 'rss_fallback_age_hours' in settings:
            config.RSS_FALLBACK_AGE_HOURS = int(settings['rss_fallback_age_hours'])
            print(f"  ✅ RSS fallback age: {config.RSS_FALLBACK_AGE_HOURS}h")

        if 'rss_workers' in settings:
            config.RSS_WORKERS = max(1, int(settings['rss_workers']))
            print(f"  ✅ RSS workers: {config.RSS_WORKERS}")
        
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
        
        existing, _ = find_setting_row(values, 'youtube_quota_used')
        current_quota = int(existing['value']) if existing and existing.get('value', '').isdigit() else 0
        new_quota = current_quota + calls_used
        update_setting_value(worksheet, 'youtube_quota_used', str(new_quota), 'Счётчик использованных единиц YouTube API')
        print(f"  📊 YouTube API quota updated: {current_quota} + {calls_used} = {new_quota}")
            
    except Exception as e:
        print(f"  ❌ Error updating quota: {e}")

def update_last_run(sheet):
    """Обновление времени последнего запуска"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        current_time = format_timestamp()
        update_setting_value(worksheet, 'last_run', current_time, 'Последний запуск обработки')
        print(f"  🕐 Last run updated: {current_time}")
            
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
    default_updates = []

    def queue_status(row_index, row, status, error_text):
        current_status = str(row.get('Provisioning status', '')).strip()
        current_error = str(row.get('Provisioning error', '')).strip()
        if current_status != status or current_error != error_text:
            status_updates.append((row_index, status, error_text))

    for row_index, raw_row in enumerate(values[1:], start=2):
        if any(str(cell).strip() == SETTINGS_MARKER for cell in raw_row):
            break

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
            stop_words = [w.lower() for w in parse_list_setting(stop_words_str)]
            shorts_value = str(row.get('Шортсы', '')).strip()
            allow_shorts = shorts_value == '🟢'
            tg_channel = str(row.get('Telegram канал @', '') or row.get('Telegram канал', '')).strip()
            channels_sheet_name = str(row.get('Название листа', '')).strip()
            rss_delete_limit_raw = str(row.get('RSS delete limit', '')).strip()
            if not rss_delete_limit_raw:
                rss_delete_limit_raw = '5'
                default_updates.append((row_index, 'RSS delete limit', rss_delete_limit_raw))
            try:
                rss_delete_limit = max(0, int(rss_delete_limit_raw))
            except ValueError:
                rss_delete_limit = 5

            projects.append({
                'code': row.get('Код проекта'),
                'name': row.get('Название'),
                'sheet_id': sheet_id,
                'channels_sheet_name': channels_sheet_name,
                'bot_token': row.get('Telegram bot token'),
                'channel_id': str(row.get('Telegram канал ID')),
                'tg_channel': tg_channel,
                'default_template': row.get('Шаблон по умолчанию', config.DEFAULT_MESSAGE_TEMPLATE),
                'stop_words': stop_words,
                'allow_shorts': allow_shorts,
                'rss_delete_limit': rss_delete_limit,
            })
            queue_status(row_index, row, 'ready', '')
        else:
            queue_status(row_index, row, 'inactive', '')

    update_project_statuses(worksheet, headers, status_updates)
    update_project_default_values(worksheet, headers, default_updates)
    
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
        headers = [str(cell).strip() for cell in values[0]] if values else []
        header_indexes = {header: index for index, header in enumerate(headers) if header}
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

            channel_id = get_row_value(normalized, header_indexes, 'ID') or extract_youtube_channel_id_from_row(normalized)
            if not channel_id:
                print(f"  ⚠️  Active row {i + 1} has no YouTube channel ID")
                continue

            channel_name = get_row_value(normalized, header_indexes, 'Название') or infer_channel_name(normalized, channel_id)
            channel_template = get_row_value(normalized, header_indexes, 'Шаблон')

            channels[channel_id] = {
                'name': channel_name,
                'template': channel_template,
                'tg_channel': ''
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


def get_row_value(row, header_indexes, header):
    index = header_indexes.get(header)
    if index is None or index >= len(row):
        return ''
    return row[index].strip()

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
    """Получение списка уже заведённых публикаций.

    Любая строка в журнале блокирует повторную отправку этого video_id в тот же
    проект. Иначе сбой после успешной отправки в Telegram, но до записи
    message_id, превращает status=pending в повторный пост на следующем запуске.
    """
    try:
        worksheet, _ = ensure_global_videos_worksheet(sheet)
        records = worksheet.get_all_records()
        tracked = set()
        for row in records:
            video_id = str(row.get('Video ID', '')).strip()
            project_name = str(row.get('Проект', '')).strip()
            if video_id and project_name:
                tracked.add((video_id, project_name))
        print(f"  📋 Found {len(tracked)} tracked video publications in table")
        return tracked
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
