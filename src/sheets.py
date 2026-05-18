import json
import os
import re
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

import config


PROJECT_STATUS_COLUMNS = [
    'Push API',
    'RSS feed',
    'Кол. 🟢 каналов',
    'Кол. 🔴 каналов',
    'Стримы',
    'Премьеры',
    'Возраст видео, ч',
    'RSS delete limit',
    'Provisioning status',
    'Provisioning error',
    'Provisioned at',
]

CHANNEL_TEMPLATE_HEADERS = [
    'Шаблон',
    'Шаблон сообщения',
    'Шаблон поста',
    'Шаблон публикации',
    'Шаблон для канала',
    'Шаблон для каждого канала',
]

CHANNEL_TG_HEADERS = [
    'TG-каналы партнёров',
    'TG-каналы партнеров',
    'TG канал партнёра',
    'TG канал партнера',
    'Telegram канал партнёра',
    'Telegram канал партнера',
    'Telegram-папка',
]

CHANNEL_NAME_HEADERS = [
    'Название',
    'Название канала',
    'Канал',
    'YouTube канал',
]

VIDEO_HEADERS = [
    'Проект',
    'Название канала',
    'Ссылка на канал',
    'Название видео',
    'Ссылка на видео',
    'Дата публикации YT GMT+4',
    'Дата обработки GMT+4',
    'Разница в минутах',
    'Дата публикации TG GMT+4',
    'TG message_id',
    'Системный статус',
]

LOG_HEADERS = ['Проект', 'Timestamp GMT+4', 'Video ID', 'Событие']

_LOCK_ROW_INFO = None
_RUN_STATUS_ROW = None
_SETTINGS_VALUES_CACHE = None
TARGET_WORKSHEET_ROWS = 10000
TARGET_SETTINGS_ROWS = 300
ROW_INSERT_INHERIT_BUFFER = 2
PUSH_EVENT_ROW_HEIGHT_PIXELS = 21
SETTINGS_READ_RANGE = 'A1:D300'
PROJECTS_READ_RANGE = 'A1:ZZ500'
PENDING_RETRY_WINDOW_HOURS = 24


def clean_sheet_value(value):
    if value is None:
        return ''
    if isinstance(value, str):
        return value.lstrip("'").strip()
    return value


def clean_sheet_row(row):
    return [clean_sheet_value(value) for value in row]


def clean_row(row):
    return clean_sheet_row(row)


def is_sheets_quota_error(error):
    return isinstance(error, gspread.exceptions.APIError) and '[429]' in str(error)


def get_values_with_quota_retry(worksheet, range_name=None, attempts=3, value_render_option=None):
    delay_seconds = 5
    for attempt in range(1, attempts + 1):
        try:
            if range_name:
                kwargs = {'value_render_option': value_render_option} if value_render_option else {}
                return worksheet.get(range_name, **kwargs)
            if value_render_option:
                return worksheet.get_all_values(value_render_option=value_render_option)
            return worksheet.get_all_values()
        except Exception as error:
            if not is_sheets_quota_error(error) or attempt >= attempts:
                raise
            print(f"  ⚠️  Sheets quota busy while reading {worksheet.title}; retry {attempt}/{attempts - 1} in {delay_seconds}s")
            time.sleep(delay_seconds)
            delay_seconds *= 2


def batch_update_with_quota_retry(worksheet, updates, value_input_option='USER_ENTERED', attempts=3):
    delay_seconds = 5
    for attempt in range(1, attempts + 1):
        try:
            return worksheet.batch_update(updates, value_input_option=value_input_option)
        except Exception as error:
            if not is_sheets_quota_error(error) or attempt >= attempts:
                raise
            print(f"  ⚠️  Sheets quota busy while writing {worksheet.title}; retry {attempt}/{attempts - 1} in {delay_seconds}s")
            time.sleep(delay_seconds)
            delay_seconds *= 2


def get_settings_values(worksheet, force_refresh=False):
    """Read settings once per run; lock/status/settings all use the same small range."""
    global _SETTINGS_VALUES_CACHE
    if force_refresh or _SETTINGS_VALUES_CACHE is None:
        _SETTINGS_VALUES_CACHE = get_values_with_quota_retry(worksheet, SETTINGS_READ_RANGE)
    return _SETTINGS_VALUES_CACHE


def display_timezone():
    return getattr(config, 'DISPLAY_TIMEZONE', 'Asia/Baku') or 'Asia/Baku'


def timezone_name():
    value = display_timezone().strip()
    if value.upper() in ('GMT+4', 'UTC+4', 'GMT+04:00', 'UTC+04:00'):
        return 'Asia/Baku'
    return value


def timezone_label():
    value = display_timezone().strip()
    if value == 'Asia/Baku':
        return 'GMT+4'
    return value


def now_iso():
    return datetime.now(ZoneInfo(timezone_name())).replace(microsecond=0).isoformat()


def current_local_datetime():
    return datetime.now(ZoneInfo(timezone_name())).replace(tzinfo=None, microsecond=0)


def parse_table_datetime(value):
    return parse_datetime_value(value)


def video_id_from_url(url):
    value = str(url or '')
    match = re.search(r'(?:v=|youtu\.be/|shorts/)([0-9A-Za-z_-]{6,})', value)
    return match.group(1) if match else ''


def channel_link(channel_id_or_link):
    value = clean_sheet_value(channel_id_or_link)
    if not value:
        return ''
    if str(value).startswith('http'):
        return bare_url(value)
    return f'youtube.com/channel/{value}'


def channel_id_from_link(link):
    match = re.search(r'(UC[0-9A-Za-z_-]{20,})', str(link or ''))
    return match.group(1) if match else ''


def a1_column(column_index):
    return re.sub(r'\d+', '', gspread.utils.rowcol_to_a1(1, column_index))


def publication_delay_minutes(yt_published, tg_published):
    yt_dt = parse_table_datetime(yt_published)
    tg_dt = parse_table_datetime(tg_published)
    if not yt_dt or not tg_dt:
        return ''
    return round((tg_dt - yt_dt).total_seconds() / 60)


def normalize_timestamp(value):
    parsed = parse_datetime_value(value)
    return format_timestamp(parsed) if parsed else clean_sheet_value(value)


def bare_url(value):
    value = str(clean_sheet_value(value) or '').strip()
    return re.sub(r'^https?://(?:www\.)?', '', value)


def tg_channel_url(project):
    value = str(project.get('tg_channel', '') or '').strip()
    if not value:
        return ''
    if value.startswith('http'):
        return value
    if value.startswith('@'):
        return f'https://t.me/{value[1:]}'
    if value.startswith('t.me/'):
        return f'https://{value}'
    return ''


def hyperlink_formula(url, text):
    if not url:
        return text
    safe_url = str(url).replace('"', '""')
    safe_text = str(text).replace('"', '""')
    # Google Sheets in this account uses a Russian locale, so formulas need semicolon separators.
    return f'=HYPERLINK("{safe_url}";"{safe_text}")'


def partner_tg_link(value):
    text = str(clean_sheet_value(value) or '').strip()
    if not text:
        return ''

    match = re.search(r'(https?://t\.me/[^\s,;]+|t\.me/[^\s,;]+|@[A-Za-z0-9_]{5,})', text)
    if not match:
        return ''

    before = text[:match.start()].rstrip()
    if before.endswith('-') or before.endswith('–') or before.endswith('—'):
        return ''

    link = match.group(1).rstrip(').,;')
    if link.startswith('@'):
        return f'https://t.me/{link[1:]}'
    if link.startswith('t.me/'):
        return f'https://{link}'
    return link


def append_tg_message_id(url, tg_message_id):
    message_id = str(clean_sheet_value(tg_message_id) or '').strip()
    if not url or not message_id:
        return url
    base_url = re.sub(r'/\d+$', '', str(url).rstrip('/'))
    return f'{base_url}/{message_id}'


def project_link_formula(project_name, project, tg_message_id=None):
    url = tg_channel_url(project)
    return hyperlink_formula(append_tg_message_id(url, tg_message_id), project_name)


def project_post_link_formula_from_cell(project_cell, project_name, tg_message_id):
    project_cell = str(clean_sheet_value(project_cell) or '').strip()
    match = re.match(r'=(?:HYPERLINK|ГИПЕРССЫЛКА)\("([^"]+)"[;,]\s*"[^"]+"\)', project_cell, flags=re.IGNORECASE)
    if not match:
        return project_cell or project_name
    return hyperlink_formula(append_tg_message_id(match.group(1), tg_message_id), project_name)


def project_name_from_cell(value):
    value = str(clean_sheet_value(value) or '').strip()
    match = re.match(r'=(?:HYPERLINK|ГИПЕРССЫЛКА)\("[^"]+"[;,]\s*"([^"]+)"\)', value, flags=re.IGNORECASE)
    return match.group(1) if match else value


def status_method_from_text(value):
    text = str(clean_sheet_value(value) or '').strip()
    match = re.match(r'^(Push|RSS):\s*(.*)$', text, flags=re.IGNORECASE)
    if not match:
        return '', text
    method = match.group(1).strip().lower()
    method = 'RSS' if method == 'rss' else 'Push'
    return method, match.group(2).strip()


def status_name_from_text(value):
    _, text = status_method_from_text(value)
    return text.split('.', 1)[0].strip().lower()


def combined_status(status, error, method=''):
    status = str(clean_sheet_value(status) or '').strip()
    error = str(clean_sheet_value(error) or '').strip()
    existing_method, bare_status = status_method_from_text(status)
    method = str(method or existing_method or '').strip()
    value = bare_status
    if error:
        value = f'{bare_status}. {error}'
    return f'{method}: {value}' if method else value


SETTINGS_MARKER = 'Настройки'

GLOBAL_VIDEOS_HEADERS = VIDEO_HEADERS

TIMESTAMP_CLEANUP_HEADERS = {
    'timestamp',
    'timestamp (utc)',
    'timestamp (asia/baku)',
    'timestamp gmt+4',
    'дата публикации yt utc',
    'дата публикации yt gmt+4',
    'дата обработки asia/baku',
    'дата обработки gmt+4',
    'дата публикации tg asia/baku',
    'дата публикации tg gmt+4',
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


def normalize_header(header):
    return re.sub(r'\s+', ' ', str(header or '').strip()).lower()


def find_column_index(headers, candidates):
    normalized_headers = [normalize_header(header) for header in headers]
    normalized_candidates = [normalize_header(candidate) for candidate in candidates]

    for candidate in normalized_candidates:
        if candidate in normalized_headers:
            return normalized_headers.index(candidate)

    for i, header in enumerate(normalized_headers):
        if any(candidate and candidate in header for candidate in normalized_candidates):
            return i

    return None


def cell_value(row, index):
    if index is None or index >= len(row):
        return ''
    return row[index]


def column_value(row, headers, candidates, fallback_index=None):
    index = find_column_index(headers, candidates)
    value = cell_value(row, index)
    if value or fallback_index is None:
        return value
    return cell_value(row, fallback_index)


def ensure_project_status_columns(worksheet, headers):
    missing = [column for column in PROJECT_STATUS_COLUMNS if column not in headers]
    if not missing:
        return headers

    start_col = len(headers) + 1
    worksheet.update(
        range_name=f'{a1_column(start_col)}1:{a1_column(start_col + len(missing) - 1)}1',
        values=[missing],
        value_input_option='USER_ENTERED',
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
        dt = datetime.now(ZoneInfo(timezone_name()))
    elif dt.tzinfo:
        dt = dt.astimezone(ZoneInfo(timezone_name()))
    # User-facing spreadsheet timestamps use Russian/Baku display style.
    return f'{dt.day:02}.{dt.month:02}.{dt.year} {dt.hour}:{dt.minute:02}:{dt.second:02}'


def sheets_datetime_serial(dt):
    if dt.tzinfo:
        dt = dt.astimezone(ZoneInfo(timezone_name())).replace(tzinfo=None)
    epoch = datetime(1899, 12, 30)
    return (dt - epoch).total_seconds() / 86400


def sheet_datetime_value(value):
    parsed = parse_datetime_value(value)
    if not parsed:
        return clean_sheet_value(value)
    return sheets_datetime_serial(parsed)


def sheet_numeric_value(value):
    cleaned = clean_sheet_value(value)
    text = str(cleaned or '').strip()
    if not text:
        return ''
    if re.fullmatch(r'-?\d+', text):
        return int(text)
    if re.fullmatch(r'-?\d+[.,]\d+', text):
        return float(text.replace(',', '.'))
    return cleaned


def parse_datetime_value(value):
    if not value:
        return None

    if isinstance(value, (int, float)):
        try:
            return datetime(1899, 12, 30) + timedelta(days=float(value))
        except Exception:
            return None

    text = str(value).strip().lstrip("'")
    for fmt in (
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%d.%m.%Y %H:%M:%S',
        '%d.%m.%Y %H:%M',
    ):
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
    for index in range(marker_index, min(len(values), marker_index + 6)):
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
    values = get_settings_values(worksheet)
    existing, table = find_setting_row(values, key)

    if not table:
        worksheet.append_row(clean_sheet_row([key, value, description]), value_input_option='USER_ENTERED')
        return

    if existing:
        updates = [{
            'range': gspread.utils.rowcol_to_a1(existing['row_number'], table['value_col']),
            'values': [[clean_sheet_value(value)]],
        }]
        if description and table.get('description_col') and not existing.get('description'):
            updates.append({
                'range': gspread.utils.rowcol_to_a1(existing['row_number'], table['description_col']),
                'values': [[clean_sheet_value(description)]],
            })
        worksheet.batch_update(updates, value_input_option='USER_ENTERED')
        return

    append_row = [''] * max(table['description_col'] or 0, table['value_col'])
    append_row[table['key_col'] - 1] = clean_sheet_value(key)
    append_row[table['value_col'] - 1] = clean_sheet_value(value)
    if description and table.get('description_col'):
        append_row[table['description_col'] - 1] = clean_sheet_value(description)
    worksheet.append_row(clean_sheet_row(append_row), value_input_option='USER_ENTERED')


def deduplicate_settings_rows(sheet):
    """Keep one row per setting key in the settings table, preserving the latest row."""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = worksheet.get_all_values()
        table = find_settings_table(values)
        if not table:
            return 0

        latest_rows = {}
        duplicate_rows = []
        legacy_status_rows = []
        key_index = table['key_col'] - 1
        for row_number, row in enumerate(values, start=1):
            normalized = [str(cell).strip() for cell in row]
            key = normalized[key_index] if len(normalized) > key_index else ''
            if 'Статус run-ов' in normalized and key != 'Статус run-ов':
                legacy_status_rows.append(row_number)

        for row_number, key, _, _ in iter_settings_rows(values, table):
            if key in latest_rows:
                duplicate_rows.append(latest_rows[key])
            latest_rows[key] = row_number

        duplicate_rows.extend(legacy_status_rows)
        duplicate_rows = sorted(set(duplicate_rows))
        if not duplicate_rows:
            return 0

        delete_rows_batch(sheet, worksheet, duplicate_rows)
        get_settings_values(worksheet, force_refresh=True)
        print(f"  🧹 Removed duplicate settings rows: {len(duplicate_rows)}")
        return len(duplicate_rows)
    except Exception as e:
        print(f"  ⚠️  Error deduplicating settings rows: {e}")
        return 0


def ensure_global_videos_worksheet(sheet):
    return ensure_videos_worksheet(sheet), GLOBAL_VIDEOS_HEADERS


def header_index(headers, name):
    return headers.index(name) if name in headers else None


def ensure_master_timestamp_formats(sheet):
    requests = []
    timestamp_headers = {normalize_header(header) for header in TIMESTAMP_CLEANUP_HEADERS}
    for worksheet in sheet.worksheets():
        try:
            values = get_values_with_quota_retry(worksheet, '1:1')
        except Exception:
            continue
        if not values:
            continue
        headers = [normalize_header(header) for header in values[0]]
        for index, header in enumerate(headers):
            if header not in timestamp_headers:
                continue
            requests.append({
                'repeatCell': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': 1,
                        'startColumnIndex': index,
                        'endColumnIndex': index + 1,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'numberFormat': {
                                'type': 'DATE_TIME',
                                'pattern': 'dd.mm.yyyy h:mm:ss',
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.numberFormat',
                }
            })

    for i in range(0, len(requests), config.BATCH_SIZE):
        sheet.batch_update({'requests': requests[i:i + config.BATCH_SIZE]})
        time.sleep(0.2)

    if requests:
        print(f"  🕒 Applied timestamp number formats: {len(requests)} columns")


def normalize_settings_datetime_values(sheet):
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = get_values_with_quota_retry(worksheet, SETTINGS_READ_RANGE)
        table = find_settings_table(values)
        if not table:
            return 0

        timestamp_value_keys = {'last_run', 'last_cleanup', 'last_subscription_sync'}
        timestamp_description_keys = {'lock_status'}
        updates = []
        for row_number, key, value, description in iter_settings_rows(values, table):
            if key in timestamp_value_keys:
                parsed = parse_datetime_value(value)
                if parsed:
                    formatted = format_timestamp(parsed)
                    if formatted != value:
                        updates.append({
                            'range': gspread.utils.rowcol_to_a1(row_number, table['value_col']),
                            'values': [[formatted]],
                        })
            if key in timestamp_description_keys and table.get('description_col'):
                parsed = parse_datetime_value(description)
                if parsed:
                    formatted = format_timestamp(parsed)
                    if formatted != description:
                        updates.append({
                            'range': gspread.utils.rowcol_to_a1(row_number, table['description_col']),
                            'values': [[formatted]],
                        })

        for i in range(0, len(updates), config.BATCH_SIZE):
            worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
            time.sleep(0.2)
        if updates:
            print(f"  🕒 Normalized settings timestamp values: {len(updates)}")
        return len(updates)
    except Exception as e:
        print(f"  ⚠️  Error normalizing settings timestamp values: {e}")
        return 0


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
            {'range': gspread.utils.rowcol_to_a1(row_index, at_col), 'values': [[sheet_datetime_value(timestamp)]]},
        ])

    worksheet.batch_update(updates, value_input_option='USER_ENTERED')


def update_project_provisioning_statuses(sheet, projects, status, error_text=''):
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_PROJECTS)
        cached_updates = []
        timestamp = format_timestamp()
        for project in projects:
            row_index = project.get('_settings_row')
            status_col = project.get('_provisioning_status_col')
            error_col = project.get('_provisioning_error_col')
            at_col = project.get('_provisioned_at_col')
            if not row_index or not status_col or not error_col or not at_col:
                cached_updates = []
                break
            cached_updates.extend([
                {'range': gspread.utils.rowcol_to_a1(row_index, status_col), 'values': [[status]]},
                {'range': gspread.utils.rowcol_to_a1(row_index, error_col), 'values': [[error_text]]},
                {'range': gspread.utils.rowcol_to_a1(row_index, at_col), 'values': [[sheet_datetime_value(timestamp)]]},
            ])

        if cached_updates:
            worksheet.batch_update(cached_updates, value_input_option='USER_ENTERED')
            print(f"  ℹ️  Updated project provisioning statuses: {status} ({len(projects)})")
            return

        values = get_values_with_quota_retry(worksheet)
        if not values:
            return

        headers = ensure_project_status_columns(worksheet, values[0])
        code_col = headers.index('Код проекта') if 'Код проекта' in headers else None
        name_col = headers.index('Название') if 'Название' in headers else None
        projects_by_code = {str(project.get('code', '')).strip(): project for project in projects if project.get('code')}
        projects_by_name = {str(project.get('name', '')).strip(): project for project in projects if project.get('name')}
        status_updates = []

        for row_index, row in enumerate(values[1:], start=2):
            if any(str(cell).strip() == SETTINGS_MARKER for cell in row):
                break
            row_code = row[code_col].strip() if code_col is not None and len(row) > code_col else ''
            row_name = row[name_col].strip() if name_col is not None and len(row) > name_col else ''
            if projects_by_code.get(row_code) or projects_by_name.get(row_name):
                status_updates.append((row_index, status, error_text))

        update_project_statuses(worksheet, headers, status_updates)
        if status_updates:
            print(f"  ℹ️  Updated project provisioning statuses: {status} ({len(status_updates)})")
    except Exception as e:
        print(f"  ⚠️  Error setting project provisioning statuses: {type(e).__name__}: {e}")


def update_project_provisioning_status_map(sheet, projects, status_by_project, error_text=''):
    if not status_by_project:
        return
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_PROJECTS)
        timestamp = format_timestamp()
        updates = []

        for project in projects:
            project_name = str(project.get('name', '')).strip()
            status = status_by_project.get(project_name)
            if not status:
                continue
            row_index = project.get('_settings_row')
            status_col = project.get('_provisioning_status_col')
            error_col = project.get('_provisioning_error_col')
            at_col = project.get('_provisioned_at_col')
            if not row_index or not status_col or not error_col or not at_col:
                continue
            updates.extend([
                {'range': gspread.utils.rowcol_to_a1(row_index, status_col), 'values': [[status]]},
                {'range': gspread.utils.rowcol_to_a1(row_index, error_col), 'values': [[error_text]]},
                {'range': gspread.utils.rowcol_to_a1(row_index, at_col), 'values': [[sheet_datetime_value(timestamp)]]},
            ])

        for i in range(0, len(updates), config.BATCH_SIZE):
            worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
            time.sleep(0.2)
        if updates:
            print(f"  ℹ️  Updated per-project provisioning progress: {len(status_by_project)}")
    except Exception as e:
        print(f"  ⚠️  Error setting per-project provisioning progress: {type(e).__name__}: {e}")


def update_project_channel_counts(sheet, projects, update_counts=True):
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_PROJECTS)
        cached_updates = []
        timestamp = format_timestamp()
        for project in projects:
            row_index = project.get('_settings_row')
            status_col = project.get('_provisioning_status_col')
            error_col = project.get('_provisioning_error_col')
            at_col = project.get('_provisioned_at_col')
            count_col = project.get('_channel_count_col')
            disabled_count_col = project.get('_disabled_channel_count_col')
            if not row_index or not status_col or not error_col or not at_col:
                cached_updates = []
                break
            status = 'error' if project.get('channels_error') else 'ready'
            error_text = project.get('channels_error', '')
            if update_counts and not project.get('channels_error'):
                if not count_col or not disabled_count_col:
                    cached_updates = []
                    break
                cached_updates.extend([
                    {'range': gspread.utils.rowcol_to_a1(row_index, count_col), 'values': [[project.get('channel_count', 0)]]},
                    {'range': gspread.utils.rowcol_to_a1(row_index, disabled_count_col), 'values': [[project.get('disabled_channel_count', 0)]]},
                ])
            cached_updates.extend([
                {'range': gspread.utils.rowcol_to_a1(row_index, status_col), 'values': [[status]]},
                {'range': gspread.utils.rowcol_to_a1(row_index, error_col), 'values': [[error_text]]},
                {'range': gspread.utils.rowcol_to_a1(row_index, at_col), 'values': [[sheet_datetime_value(timestamp)]]},
            ])

        if cached_updates:
            for i in range(0, len(cached_updates), config.BATCH_SIZE):
                worksheet.batch_update(cached_updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
                time.sleep(0.2)
            action = 'project channel counts' if update_counts else 'project provisioning statuses'
            print(f"  🟢 Updated {action}: {len(cached_updates)}")
            return

        values = get_values_with_quota_retry(worksheet)
        if not values:
            return

        headers = ensure_project_status_columns(worksheet, values[0])
        count_col = headers.index('Кол. 🟢 каналов') + 1
        disabled_count_col = headers.index('Кол. 🔴 каналов') + 1
        status_col = headers.index('Provisioning status') + 1
        error_col = headers.index('Provisioning error') + 1
        at_col = headers.index('Provisioned at') + 1
        code_col = headers.index('Код проекта') if 'Код проекта' in headers else None
        name_col = headers.index('Название') if 'Название' in headers else None
        projects_by_code = {str(project.get('code', '')).strip(): project for project in projects if project.get('code')}
        projects_by_name = {str(project.get('name', '')).strip(): project for project in projects if project.get('name')}
        updates = []

        for row_index, row in enumerate(values[1:], start=2):
            if any(str(cell).strip() == SETTINGS_MARKER for cell in row):
                break
            row_code = row[code_col].strip() if code_col is not None and len(row) > code_col else ''
            row_name = row[name_col].strip() if name_col is not None and len(row) > name_col else ''
            project = projects_by_code.get(row_code) or projects_by_name.get(row_name)
            if not project:
                continue
            status = 'error' if project.get('channels_error') else 'ready'
            error_text = project.get('channels_error', '')
            provisioned_at = format_timestamp()
            row_updates = []
            if update_counts and not project.get('channels_error'):
                channel_count = str(project.get('channel_count', 0))
                current = str(row[count_col - 1]).strip() if len(row) >= count_col else ''
                if current != channel_count:
                    row_updates.append({
                        'range': gspread.utils.rowcol_to_a1(row_index, count_col),
                        'values': [[channel_count]],
                    })
                disabled_channel_count = str(project.get('disabled_channel_count', 0))
                current_disabled = str(row[disabled_count_col - 1]).strip() if len(row) >= disabled_count_col else ''
                if current_disabled != disabled_channel_count:
                    row_updates.append({
                        'range': gspread.utils.rowcol_to_a1(row_index, disabled_count_col),
                        'values': [[disabled_channel_count]],
                    })
            current_status = str(row[status_col - 1]).strip() if len(row) >= status_col else ''
            current_error = str(row[error_col - 1]).strip() if len(row) >= error_col else ''
            if current_status != status or current_error != error_text:
                row_updates.extend([
                    {'range': gspread.utils.rowcol_to_a1(row_index, status_col), 'values': [[status]]},
                    {'range': gspread.utils.rowcol_to_a1(row_index, error_col), 'values': [[error_text]]},
                ])
            row_updates.append({
                'range': gspread.utils.rowcol_to_a1(row_index, at_col),
                'values': [[sheet_datetime_value(provisioned_at)]],
            })
            updates.extend(row_updates)

        for i in range(0, len(updates), config.BATCH_SIZE):
            worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
            time.sleep(0.2)
        if updates:
            action = 'project channel counts' if update_counts else 'project provisioning statuses'
            print(f"  🟢 Updated {action}: {len(updates)}")
    except Exception as e:
        print(f"  ⚠️  Error updating project channel counts: {e}")


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
        worksheet.batch_update(updates, value_input_option='USER_ENTERED')


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

def acquire_lock(sheet, stale_after_seconds=900):
    """Получить блокировку для предотвращения одновременных запусков"""
    global _LOCK_ROW_INFO
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = get_settings_values(worksheet, force_refresh=True)
        
        existing, table = find_setting_row(values, 'lock_status')
        if existing and table:
                lock_row = existing['row_number']
                lock_value = existing['value']
                
                if lock_value == 'locked':
                    lock_time_str = existing.get('description', '')
                    stale_lock = False
                    if lock_time_str:
                        try:
                            lock_time = parse_datetime_value(lock_time_str)
                            if not lock_time:
                                raise ValueError('invalid lock time')
                            stale_lock = (current_local_datetime() - lock_time).total_seconds() > stale_after_seconds
                        except:
                            stale_lock = True
                    else:
                        stale_lock = True

                    if stale_lock:
                        print("  ⚠️  Stale lock detected, removing...")
                    else:
                        print("  ❌ Another process is running! Exiting...")
                        return False
                
                current_time = format_timestamp()
                updates = [{
                    'range': gspread.utils.rowcol_to_a1(lock_row, table['value_col']),
                    'values': [['locked']],
                }]
                if table.get('description_col'):
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(lock_row, table['description_col']),
                        'values': [[current_time]],
                    })
                worksheet.batch_update(updates, value_input_option='USER_ENTERED')
                _LOCK_ROW_INFO = {
                    'row_number': lock_row,
                    'value_col': table['value_col'],
                    'description_col': table.get('description_col'),
                }
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
    global _LOCK_ROW_INFO
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        if _LOCK_ROW_INFO:
            updates = [{
                'range': gspread.utils.rowcol_to_a1(_LOCK_ROW_INFO['row_number'], _LOCK_ROW_INFO['value_col']),
                'values': [['unlocked']],
            }]
            if _LOCK_ROW_INFO.get('description_col'):
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(_LOCK_ROW_INFO['row_number'], _LOCK_ROW_INFO['description_col']),
                    'values': [[format_timestamp()]],
                })
            worksheet.batch_update(updates, value_input_option='USER_ENTERED')
            print(f"  🔓 Lock released")
            _LOCK_ROW_INFO = None
            return

        values = get_values_with_quota_retry(worksheet, SETTINGS_READ_RANGE)
        _, table = find_setting_row(values, 'lock_status')
        if table:
            updates = []
            for row_number, key, _, _ in iter_settings_rows(values, table):
                if key != 'lock_status':
                    continue
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_number, table['value_col']),
                    'values': [['unlocked']],
                })
                if table.get('description_col'):
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(row_number, table['description_col']),
                        'values': [[format_timestamp()]],
                    })
            if updates:
                worksheet.batch_update(updates, value_input_option='USER_ENTERED')
                print(f"  🔓 Lock released")
            return
    except Exception as e:
        print(f"  ⚠️  Error releasing lock: {e}")


def row_as_dict(headers, row):
    data = {
        str(header).strip(): clean_sheet_value(row[i]) if i < len(row) else ''
        for i, header in enumerate(headers)
        if str(header).strip()
    }
    for index, header in enumerate(headers):
        canonical = canonical_header_name(header)
        if canonical and canonical != str(header).strip():
            data[canonical] = clean_sheet_value(row[index]) if index < len(row) else ''
    return data


def canonical_header_name(header):
    normalized = normalize_header(header)
    for canonical in ('Системный статус', 'Событие', 'Разница в минутах'):
        if normalize_header(canonical) in normalized:
            return canonical
    return str(header).strip()


def first_value(row, names):
    for name in names:
        value = row.get(name, '')
        if value not in ('', None):
            return value
    return ''


def header_indexes(headers):
    indexes = {}
    for index, header in enumerate(headers):
        name = canonical_header_name(header)
        if name:
            indexes[name] = index
    return indexes


def row_for_headers(headers, values_by_header):
    return [
        clean_sheet_value(values_by_header.get(canonical_header_name(header), ''))
        for header in headers
    ]


def migrate_video_row(headers, row):
    data = row_as_dict(headers, row)
    video_url = first_value(data, ['Ссылка на видео', 'Ссылка'])
    video_id = first_value(data, ['Video ID'])
    if not video_url and video_id:
        video_url = f'youtube.com/watch?v={video_id}'

    tg_published = normalize_timestamp(first_value(data, ['Дата публикации TG GMT+4', 'Дата публикации TG Asia/Baku', 'Дата публикации TG']))
    yt_published = normalize_timestamp(first_value(data, ['Дата публикации YT GMT+4', 'Дата публикации YT UTC', 'Дата публикации UTC']))
    status = combined_status(
        first_value(data, ['Системный статус']),
        first_value(data, ['Ошибка']),
    )

    return clean_row([
        first_value(data, ['Проект']),
        first_value(data, ['Название канала']),
        channel_link(first_value(data, ['Ссылка на канал', 'Channel ID'])),
        first_value(data, ['Название видео']),
        bare_url(video_url),
        yt_published,
        normalize_timestamp(first_value(data, ['Дата обработки GMT+4', 'Дата обработки Asia/Baku', 'Дата обработки UTC'])),
        first_value(data, ['Разница в минутах']) or publication_delay_minutes(yt_published, tg_published),
        tg_published,
        first_value(data, ['TG message_id']),
        status,
    ])


def ensure_videos_worksheet(sheet):
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_VIDEOS)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(config.SHEET_NAME_VIDEOS, rows=10000, cols=len(VIDEO_HEADERS))
        worksheet.append_row(VIDEO_HEADERS, value_input_option='USER_ENTERED')
        print(f"  📋 Created '{config.SHEET_NAME_VIDEOS}' worksheet")
        return worksheet

    header_values = get_values_with_quota_retry(worksheet, '1:1')
    if not header_values:
        worksheet.append_row(VIDEO_HEADERS, value_input_option='USER_ENTERED')
        return worksheet

    headers = [str(value).strip() for value in header_values[0]]
    existing_header_keys = set(header_indexes(headers).keys())
    if all(header in existing_header_keys for header in VIDEO_HEADERS):
        return worksheet

    if headers[:len(VIDEO_HEADERS)] == VIDEO_HEADERS and len(headers) == len(VIDEO_HEADERS):
        return worksheet

    values = get_values_with_quota_retry(worksheet)
    migrated_rows = [migrate_video_row(headers, row) for row in values[1:] if any(str(cell).strip() for cell in row)]
    worksheet.clear()
    worksheet.update(range_name='A1', values=[VIDEO_HEADERS] + migrated_rows, value_input_option='USER_ENTERED')
    if worksheet.col_count > len(VIDEO_HEADERS):
        sheet.batch_update({
            'requests': [{
                'deleteDimension': {
                    'range': {
                        'sheetId': worksheet.id,
                        'dimension': 'COLUMNS',
                        'startIndex': len(VIDEO_HEADERS),
                        'endIndex': worksheet.col_count,
                    }
                }
            }]
        })
    print(f"  🔁 Migrated '{config.SHEET_NAME_VIDEOS}' columns")
    return worksheet


def save_videos_batch(sheet, videos_data):
    """
    БАТЧЕВОЕ сохранение видео в таблицу с защитой от потери
    videos_data = [(video, project, pub_date, tg_msg_id, error), ...]
    Возвращает список (video_id, project_name), которые были созданы в этом запуске
    """
    if not videos_data:
        return []
    
    try:
        worksheet = ensure_videos_worksheet(sheet)
        header_values = get_values_with_quota_retry(worksheet, '1:1')
        headers = [str(value).strip() for value in header_values[0]] if header_values else VIDEO_HEADERS
        
        existing_rows = {}
        try:
            values = worksheet.get_all_values()
            existing_headers = [str(value).strip() for value in values[0]] if values else headers
            for row_index, row in enumerate(values[1:], start=2):
                data = row_as_dict(existing_headers, row)
                project_name = project_name_from_cell(first_value(data, ['Проект']))
                video_id = video_id_from_url(first_value(data, ['Ссылка на видео', 'Video ID']))
                status = status_name_from_text(first_value(data, ['Системный статус']))
                if video_id and project_name:
                    key = (video_id, project_name)
                    current = existing_rows.get(key)
                    existing = {
                        'row_index': row_index,
                        'status': status,
                    }
                    if not current or row_status_blocks_retry(status) or not row_status_blocks_retry(current.get('status')):
                        existing_rows[key] = existing
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
            is_pending_hold = str(error or '').startswith('PENDING: ')
            row_status = 'filtered' if is_filtered else ('published' if tg_message_id else 'pending')
            row_error = str(error or '')
            if is_filtered:
                row_error = row_error.replace('FILTERED: ', '', 1)
            elif is_pending_hold:
                row_error = row_error.replace('PENDING: ', '', 1)
            source_method = str(video.get('source_method') or '').strip()
            if source_method.lower() == 'rss' and row_error.startswith('RSS: '):
                row_error = row_error.replace('RSS: ', '', 1)
            project_display = project_link_formula(project_name, project, tg_message_id)
            processed_at = format_timestamp()
            yt_published_at = normalize_timestamp(video_published_date)
            tg_published_at = format_timestamp() if tg_message_id else ''

            if existing:
                if existing['status'] == 'pending' or str(existing['status']).startswith('deleted'):
                    if str(existing['status']).startswith('deleted'):
                        video['restored_from_status'] = existing['status']
                    print(f"  🔁 Retrying {existing['status'] or 'tracked'}: {video['video_id']} / {project_name}")
                    saved_publication_keys.append(key)
                    continue
                print(f"  ⏭️  Already tracked: {video['video_id']} / {project_name} ({existing['status'] or 'no status'})")
                continue

            row_values = {
                'Проект': project_display,
                'Название канала': video.get('channel', ''),
                'Ссылка на канал': channel_link(video.get('channel_id', '')),
                'Название видео': video.get('title', ''),
                'Ссылка на видео': bare_url(video.get('url', '')),
                'Дата публикации YT GMT+4': sheet_datetime_value(yt_published_at),
                'Дата обработки GMT+4': sheet_datetime_value(processed_at),
                'Разница в минутах': publication_delay_minutes(yt_published_at, tg_published_at),
                'Дата публикации TG GMT+4': sheet_datetime_value(tg_published_at) if tg_published_at else '',
                'TG message_id': sheet_numeric_value(tg_message_id) if tg_message_id else '',
                'Системный статус': combined_status(row_status, row_error, source_method),
            }
            rows.append(row_for_headers(headers, row_values))
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


def row_status_blocks_retry(status):
    status = str(status or '').strip().lower()
    return bool(status and status != 'pending' and not status.startswith('deleted'))

def update_video_publication_status(sheet, video_id, project_name, tg_message_id=None, status='published', error=''):
    """Обновление статуса публикации существующей строки видео"""
    try:
        worksheet = ensure_videos_worksheet(sheet)
        values = get_values_with_quota_retry(worksheet)
        headers = [str(value).strip() for value in values[0]] if values else []
        indexes = {header: index + 1 for header, index in header_indexes(headers).items()}
        status_index = find_column_index(headers, ['Системный статус'])
        if status_index is not None:
            indexes['Системный статус'] = status_index + 1
        target_row = None
        target_data = {}

        for row_index, row in enumerate(values[1:], start=2):
            data = row_as_dict(headers, row)
            row_project_name = project_name_from_cell(first_value(data, ['Проект']))
            row_video_id = video_id_from_url(first_value(data, ['Ссылка на видео', 'Video ID']))
            if row_video_id == video_id and row_project_name == project_name:
                target_row = row_index
                target_data = data

        if not target_row:
            print(f"  ⚠️  Could not find video row to update: {video_id} / {project_name}")
            return False

        timestamp = format_timestamp()
        yt_published = first_value(target_data, ['Дата публикации YT GMT+4', 'Дата публикации YT UTC'])
        method, _ = status_method_from_text(first_value(target_data, ['Системный статус']))
        column_updates = {
            'Проект': project_post_link_formula_from_cell(first_value(target_data, ['Проект']), project_name, tg_message_id) if tg_message_id else first_value(target_data, ['Проект']),
            'TG message_id': sheet_numeric_value(tg_message_id) if tg_message_id else '',
            'Дата публикации TG GMT+4': sheet_datetime_value(timestamp) if tg_message_id else '',
            'Разница в минутах': publication_delay_minutes(yt_published, timestamp) if tg_message_id else '',
            'Системный статус': combined_status(status, error, method),
        }
        updates = [
            {
                'range': gspread.utils.rowcol_to_a1(target_row, indexes[header]),
                'values': [[value]],
            }
            for header, value in column_updates.items()
            if header in indexes
        ]
        batch_update_with_quota_retry(worksheet, updates, value_input_option='USER_ENTERED')
        return True
    except Exception as e:
        print(f"  ⚠️  Error updating publication status for {video_id}: {e}")
        return False


def reconcile_pending_published_videos(sheet):
    """Repair rows left as pending when Telegram succeeded but Sheets update hit quota."""
    try:
        videos_worksheet = ensure_videos_worksheet(sheet)
        logs_worksheet = ensure_logs_worksheet(sheet)
        video_values = videos_worksheet.get_all_values()
        log_values = logs_worksheet.get_all_values()
        if len(video_values) < 2 or len(log_values) < 2:
            return 0
        video_headers = [str(value).strip() for value in video_values[0]]
        video_indexes = {header: index + 1 for header, index in header_indexes(video_headers).items()}
        status_index = find_column_index(video_headers, ['Системный статус'])
        if status_index is not None:
            video_indexes['Системный статус'] = status_index + 1
        log_headers = [str(value).strip() for value in log_values[0]]

        published_logs = {}
        for row in log_values[1:]:
            data = row_as_dict(log_headers, row)
            project_name = project_name_from_cell(first_value(data, ['Проект']))
            timestamp = normalize_timestamp(first_value(data, ['Timestamp GMT+4', 'Timestamp']))
            video_id = first_value(data, ['Video ID'])
            event = first_value(data, ['Событие'])
            match = re.search(r'Telegram msg:\s*(\d+)', event)
            if project_name and video_id and match:
                published_logs[(video_id, project_name)] = (timestamp, match.group(1))

        updates = []
        for row_index, row in enumerate(video_values[1:], start=2):
            data = row_as_dict(video_headers, row)
            project_name = project_name_from_cell(first_value(data, ['Проект']))
            video_id = video_id_from_url(first_value(data, ['Ссылка на видео', 'Video ID']))
            status_text = first_value(data, ['Системный статус'])
            method, _ = status_method_from_text(status_text)
            status = status_name_from_text(status_text)
            message_id = first_value(data, ['TG message_id'])
            tg_published_current = first_value(data, ['Дата публикации TG GMT+4', 'Дата публикации TG Asia/Baku', 'Дата публикации TG'])
            delay_current = first_value(data, ['Разница в минутах'])
            if status == 'published' and message_id and tg_published_current and delay_current:
                continue

            log_entry = published_logs.get((video_id, project_name))
            if not log_entry and message_id and (not tg_published_current or not delay_current):
                fallback_published_at = first_value(data, ['Дата обработки GMT+4', 'Дата обработки Asia/Baku', 'Дата обработки UTC'])
                if fallback_published_at:
                    log_entry = (normalize_timestamp(fallback_published_at), message_id)
            if not log_entry:
                continue

            published_at, tg_message_id = log_entry
            yt_published = first_value(data, ['Дата публикации YT GMT+4', 'Дата публикации YT UTC'])
            for header, value in {
                'Разница в минутах': publication_delay_minutes(yt_published, published_at),
                'Дата публикации TG GMT+4': sheet_datetime_value(published_at),
                'TG message_id': tg_message_id,
                'Системный статус': combined_status('published', '', method),
            }.items():
                if header in video_indexes:
                    updates.append({
                        'range': gspread.utils.rowcol_to_a1(row_index, video_indexes[header]),
                        'values': [[value]],
                    })

        for i in range(0, len(updates), config.BATCH_SIZE):
            videos_worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
            time.sleep(0.2)
        if updates:
            fixed = len(updates) // 4
            print(f"  🧩 Reconciled pending published rows: {fixed}")
            return fixed
        return 0
    except Exception as e:
        print(f"  ⚠️  Error reconciling pending published rows: {e}")
        return 0


def delete_stale_unpublished_video_rows(sheet):
    """Mark non-actionable pending videos as filtered instead of leaving them forever."""
    try:
        worksheet = ensure_videos_worksheet(sheet)
        values = get_values_with_quota_retry(worksheet)
        if len(values) < 2:
            return 0

        headers = [str(value).strip() for value in values[0]]
        updates = []
        now = current_local_datetime()
        blocking_keys = set()

        for row in values[1:]:
            data = row_as_dict(headers, row)
            status_name = status_name_from_text(first_value(data, ['Системный статус']))
            if not row_status_blocks_retry(status_name):
                continue
            project_name = project_name_from_cell(first_value(data, ['Проект']))
            video_id = video_id_from_url(first_value(data, ['Ссылка на видео', 'Video ID']))
            if video_id and project_name:
                blocking_keys.add((video_id, project_name))

        for row_index, row in enumerate(values[1:], start=2):
            data = row_as_dict(headers, row)
            status_text = first_value(data, ['Системный статус'])
            method, _ = status_method_from_text(status_text)
            status_name = status_name_from_text(status_text)
            if status_name == 'published':
                continue
            if status_name not in ('pending', ''):
                continue

            status_index = find_column_index(headers, ['Системный статус'])
            status_col = status_index + 1 if status_index is not None else None
            if not status_col:
                continue

            project_name = project_name_from_cell(first_value(data, ['Проект']))
            video_id = video_id_from_url(first_value(data, ['Ссылка на видео', 'Video ID']))
            if video_id and project_name and (video_id, project_name) in blocking_keys:
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_index, status_col),
                    'values': [[combined_status('filtered', 'Duplicate already published/tracked', method)]],
                })
                continue

            yt_published = parse_datetime_value(first_value(data, ['Дата публикации YT GMT+4', 'Дата публикации YT UTC']))
            if not yt_published:
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_index, status_col),
                    'values': [[combined_status('filtered', 'Missing/invalid YT publication date', method)]],
                })
                continue

            age_hours = (now - yt_published).total_seconds() / 3600
            if age_hours > config.MAX_PUBLISH_AGE_HOURS:
                reason = f"Stale video ({age_hours:.1f}h old, limit {config.MAX_PUBLISH_AGE_HOURS}h)"
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_index, status_col),
                    'values': [[combined_status('filtered', reason, method)]],
                })
                continue

            processed_at = parse_datetime_value(first_value(data, ['Дата обработки GMT+4', 'Дата обработки Asia/Baku', 'Дата обработки UTC']))
            pending_age_hours = (now - processed_at).total_seconds() / 3600 if processed_at else age_hours
            if pending_age_hours > PENDING_RETRY_WINDOW_HOURS:
                reason = f"Pending retry window expired ({pending_age_hours:.1f}h old)"
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_index, status_col),
                    'values': [[combined_status('filtered', reason, method)]],
                })

        for i in range(0, len(updates), config.BATCH_SIZE):
            batch_update_with_quota_retry(worksheet, updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
            time.sleep(0.2)
        if updates:
            print(f"  🧹 Marked non-actionable pending rows filtered: {len(updates)}")
        return len(updates)
    except Exception as e:
        print(f"  ⚠️  Error marking stale unpublished video rows: {e}")
        return 0


def get_recent_published_video_rows(sheet, project_name, hours=24):
    """Строки опубликованных видео за последние часы для сверки с RSS."""
    try:
        worksheet = ensure_videos_worksheet(sheet)
        values = worksheet.get_all_values()
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        headers = [str(value).strip() for value in values[0]] if values else []

        rows = []
        for row_index, row in enumerate(values[1:], start=2):
            data = row_as_dict(headers, row)
            row_project = project_name_from_cell(first_value(data, ['Проект']))
            if row_project != project_name:
                continue

            status = status_name_from_text(first_value(data, ['Системный статус']))
            message_id = first_value(data, ['TG message_id'])
            if status != 'published' or not message_id:
                continue

            date_value = first_value(data, ['Дата публикации YT GMT+4', 'Дата обработки GMT+4'])
            record_date = parse_datetime_value(date_value)
            if not record_date or record_date < cutoff:
                continue

            video_id = video_id_from_url(first_value(data, ['Ссылка на видео', 'Video ID']))
            channel_id = channel_id_from_link(first_value(data, ['Ссылка на канал', 'Channel ID']))
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


def merge_log_event(event, details, status=''):
    event = str(clean_sheet_value(event) or '').strip()
    details = str(clean_sheet_value(details) or '').strip()
    status = str(clean_sheet_value(status) or '').strip()
    method = ''
    method_match = re.match(r'^(Push|RSS):\s*(.*)$', event, flags=re.IGNORECASE)
    if method_match:
        method = 'RSS' if method_match.group(1).lower() == 'rss' else 'Push'
        event = method_match.group(2).strip()
    details_method_match = re.match(r'^(Push|RSS):\s*(.*)$', details, flags=re.IGNORECASE)
    if details_method_match:
        method = 'RSS' if details_method_match.group(1).lower() == 'rss' else 'Push'
        details = details_method_match.group(2).strip()
    if method and event:
        event = f'{method}: {event}'
    if event and details:
        return f'{event}. {details}'
    return event or details or status


def migrate_log_row(headers, row):
    data = row_as_dict(headers, row)
    return clean_row([
        first_value(data, ['Проект']),
        normalize_timestamp(first_value(data, ['Timestamp GMT+4', 'Timestamp'])),
        first_value(data, ['Video ID']),
        merge_log_event(first_value(data, ['Событие']), first_value(data, ['Детали']), first_value(data, ['Статус'])),
    ])


def ensure_logs_worksheet(sheet):
    try:
        worksheet = sheet.worksheet('Логи')
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet('Логи', rows=10000, cols=len(LOG_HEADERS))
        worksheet.append_row(LOG_HEADERS, value_input_option='USER_ENTERED')
        return worksheet

    header_values = get_values_with_quota_retry(worksheet, '1:1')
    if not header_values:
        worksheet.append_row(LOG_HEADERS, value_input_option='USER_ENTERED')
        return worksheet

    headers = [str(value).strip() for value in header_values[0]]
    existing_header_keys = set(header_indexes(headers).keys())
    if all(header in existing_header_keys for header in LOG_HEADERS) and len(headers) == len(LOG_HEADERS):
        return worksheet

    values = get_values_with_quota_retry(worksheet)
    migrated_rows = [migrate_log_row(headers, row) for row in values[1:] if any(str(cell).strip() for cell in row)]
    worksheet.clear()
    worksheet.update(range_name='A1', values=[LOG_HEADERS] + migrated_rows, value_input_option='USER_ENTERED')
    if worksheet.col_count > len(LOG_HEADERS):
        sheet.batch_update({
            'requests': [{
                'deleteDimension': {
                    'range': {
                        'sheetId': worksheet.id,
                        'dimension': 'COLUMNS',
                        'startIndex': len(LOG_HEADERS),
                        'endIndex': worksheet.col_count,
                    }
                }
            }]
        })
    print("  🔁 Migrated 'Логи' columns")
    return worksheet


def strip_apostrophes_in_worksheet(worksheet):
    values = get_values_with_quota_retry(worksheet)
    updates = []
    for row_index, row in enumerate(values, start=1):
        cleaned = clean_row(row)
        if cleaned != row:
            updates.append({
                'range': f'A{row_index}:{a1_column(len(cleaned))}{row_index}',
                'values': [cleaned],
            })

    for i in range(0, len(updates), config.BATCH_SIZE):
        worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
        time.sleep(0.2)

    return len(updates)


def last_used_row(values):
    for row_index in range(len(values), 0, -1):
        if any(str(cell).strip() for cell in values[row_index - 1]):
            return row_index
    return 0


def row_count_target(worksheet, used_rows):
    base_target = TARGET_SETTINGS_ROWS if worksheet.title == config.SHEET_NAME_SETTINGS else TARGET_WORKSHEET_ROWS
    return max(base_target, used_rows)


def sheet_row_count_requests(worksheet, target_rows):
    current_rows = worksheet.row_count
    if current_rows == target_rows:
        return []

    if current_rows < target_rows:
        insert_count = target_rows - current_rows
        start_index = max(1, current_rows - ROW_INSERT_INHERIT_BUFFER)
        return [{
            'insertDimension': {
                'range': {
                    'sheetId': worksheet.id,
                    'dimension': 'ROWS',
                    'startIndex': start_index,
                    'endIndex': start_index + insert_count,
                },
                'inheritFromBefore': True,
            }
        }]

    return [{
        'deleteDimension': {
            'range': {
                'sheetId': worksheet.id,
                'dimension': 'ROWS',
                'startIndex': target_rows,
                'endIndex': current_rows,
            }
        }
    }]


def extend_conditional_format_ranges(sheet):
    try:
        metadata = sheet.fetch_sheet_metadata(params={
            'includeGridData': False,
            'fields': 'sheets(properties(sheetId,gridProperties(rowCount)),conditionalFormats)',
        })
    except Exception as e:
        print(f"  ⚠️  Error reading conditional format metadata: {e}")
        return 0

    requests = []
    for sheet_info in metadata.get('sheets', []):
        properties = sheet_info.get('properties', {})
        sheet_id = properties.get('sheetId')
        row_count = properties.get('gridProperties', {}).get('rowCount')
        if sheet_id is None or not row_count:
            continue

        for rule_index, rule in enumerate(sheet_info.get('conditionalFormats', [])):
            ranges = rule.get('ranges', [])
            updated_ranges = []
            changed = False
            for grid_range in ranges:
                updated_range = dict(grid_range)
                if updated_range.get('sheetId') == sheet_id and updated_range.get('endRowIndex') != row_count:
                    updated_range['endRowIndex'] = row_count
                    changed = True
                updated_ranges.append(updated_range)

            if changed:
                updated_rule = dict(rule)
                updated_rule['ranges'] = updated_ranges
                requests.append({
                    'updateConditionalFormatRule': {
                        'sheetId': sheet_id,
                        'index': rule_index,
                        'rule': updated_rule,
                    }
                })

    for i in range(0, len(requests), config.BATCH_SIZE):
        sheet.batch_update({'requests': requests[i:i + config.BATCH_SIZE]})
        time.sleep(0.2)

    if requests:
        print(f"  🎨 Extended conditional formatting ranges: {len(requests)}")
    return len(requests)


def ensure_workbook_row_counts(sheet):
    requests = []
    for worksheet in sheet.worksheets():
        try:
            used_rows = last_used_row(get_values_with_quota_retry(worksheet))
        except Exception:
            used_rows = worksheet.row_count
        requests.extend(sheet_row_count_requests(worksheet, row_count_target(worksheet, used_rows)))

    for i in range(0, len(requests), config.BATCH_SIZE):
        sheet.batch_update({'requests': requests[i:i + config.BATCH_SIZE]})
        time.sleep(0.2)

    if requests:
        print(f"  📐 Normalized workbook row counts: {len(requests)}")

    extend_conditional_format_ranges(sheet)


def ensure_non_settings_sheet_row_counts(sheet):
    ensure_workbook_row_counts(sheet)


def format_push_events_sheet(sheet, clean_rows=False):
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_PUSH_EVENTS)
        values = get_values_with_quota_retry(worksheet, '1:1')
        if values:
            headers = list(values[0])
            headers = ['Timestamp GMT+4' if str(h).strip().lower().startswith('timestamp') else h for h in headers]
            worksheet.update(range_name=f'A1:{a1_column(len(headers))}1', values=[headers], value_input_option='USER_ENTERED')

            rows_to_delete = []
            timestamp_updates = []
            if clean_rows:
                values = get_values_with_quota_retry(worksheet)
                for row_index, row in enumerate(values[1:], start=2):
                    cleaned = clean_row(row)
                    video_id = cleaned[1] if len(cleaned) > 1 else ''
                    channel_value = cleaned[2] if len(cleaned) > 2 else ''
                    channel_id = channel_id_from_link(channel_value) or channel_value
                    if not video_id or not channel_id:
                        rows_to_delete.append(row_index)
                        continue
                    timestamp = cleaned[0] if cleaned else ''
                    normalized_timestamp = normalize_timestamp(timestamp)
                    if normalized_timestamp and normalized_timestamp != timestamp:
                        timestamp_updates.append({
                            'range': gspread.utils.rowcol_to_a1(row_index, 1),
                            'values': [[sheet_datetime_value(normalized_timestamp)]],
                        })

            if timestamp_updates:
                for i in range(0, len(timestamp_updates), config.BATCH_SIZE):
                    worksheet.batch_update(timestamp_updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
                    time.sleep(0.2)
            if rows_to_delete:
                deleted = delete_rows_batch(sheet, worksheet, rows_to_delete)
                print(f"  🧹 Removed invalid push event rows: {deleted}")

        requests = [{
            'repeatCell': {
                'range': {
                    'sheetId': worksheet.id,
                    'startRowIndex': 1,
                    'startColumnIndex': 0,
                    'endColumnIndex': 6,
                },
                'cell': {
                    'userEnteredFormat': {
                        'wrapStrategy': 'CLIP',
                    }
                },
                'fields': 'userEnteredFormat.wrapStrategy',
            }
        }]
        if worksheet.row_count > 1:
            requests.append({
                'updateDimensionProperties': {
                    'range': {
                        'sheetId': worksheet.id,
                        'dimension': 'ROWS',
                        'startIndex': 1,
                        'endIndex': worksheet.row_count,
                    },
                    'properties': {'pixelSize': PUSH_EVENT_ROW_HEIGHT_PIXELS},
                    'fields': 'pixelSize',
                }
            })
        sheet.batch_update({'requests': requests})
    except Exception as e:
        print(f"  ⚠️  Error formatting push events: {e}")


def maintain_workbook_layout(sheet, clean_apostrophes=False):
    ensure_non_settings_sheet_row_counts(sheet)
    ensure_master_timestamp_formats(sheet)
    normalize_settings_datetime_values(sheet)

    changed_rows = 0
    if clean_apostrophes:
        for worksheet_name in [
            config.SHEET_NAME_PROJECTS,
            config.SHEET_NAME_VIDEOS,
            'Логи',
            config.SHEET_NAME_PUSH_EVENTS,
            'Подписки',
            config.SHEET_NAME_SETTINGS,
        ]:
            try:
                worksheet = sheet.worksheet(worksheet_name)
                changed_rows += strip_apostrophes_in_worksheet(worksheet)
            except Exception:
                pass

    ensure_videos_worksheet(sheet)
    ensure_logs_worksheet(sheet)
    format_push_events_sheet(sheet)

    if changed_rows:
        print(f"  🧹 Removed leading apostrophes from {changed_rows} rows")


def update_video_project_links(sheet, projects):
    try:
        worksheet = ensure_videos_worksheet(sheet)
        values = get_values_with_quota_retry(worksheet)
        if not values:
            return
        project_formulas = get_values_with_quota_retry(worksheet, 'A:A', value_render_option='FORMULA')
        headers = [str(value).strip() for value in values[0]]
        project_map = {str(project.get('name', '')).strip(): project for project in projects}
        updates = []
        for row_index, row in enumerate(values[1:], start=2):
            data = row_as_dict(headers, row)
            current_formula = cell_value(project_formulas[row_index - 1], 0) if row_index - 1 < len(project_formulas) else ''
            current = current_formula if str(current_formula).strip().startswith('=') else first_value(data, ['Проект'])
            project_name = project_name_from_cell(current)
            project = project_map.get(project_name)
            if not project:
                continue
            linked = project_link_formula(project_name, project, first_value(data, ['TG message_id']))
            if linked != current:
                updates.append({'range': f'A{row_index}', 'values': [[linked]]})
        for i in range(0, len(updates), config.BATCH_SIZE):
            worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
            time.sleep(0.2)
        if updates:
            print(f"  🔗 Updated project links in videos: {len(updates)}")
    except Exception as e:
        print(f"  ⚠️  Error updating video project links: {e}")


def normalize_log_entry(entry):
    if len(entry) >= 7:
        timestamp = normalize_timestamp(entry[0])
        method = str(clean_sheet_value(entry[6]) or '').strip()
        event = f'{method}: {entry[2]}' if method and not re.match(r'^(Push|RSS):', str(entry[2]), flags=re.IGNORECASE) else entry[2]
        return clean_row([entry[1], sheet_datetime_value(timestamp), entry[3], merge_log_event(event, entry[4], entry[5])])
    if len(entry) >= 6:
        timestamp = normalize_timestamp(entry[0])
        return clean_row([entry[1], sheet_datetime_value(timestamp), entry[3], merge_log_event(entry[2], entry[4], entry[5])])
    if len(entry) == 4:
        timestamp = normalize_timestamp(entry[0])
        return clean_row([entry[1], sheet_datetime_value(timestamp), entry[2], entry[3]])
    return clean_row((entry + [''] * len(LOG_HEADERS))[:len(LOG_HEADERS)])

def log_events_batch(sheet, log_entries):
    """Пакетная запись логов"""
    if not log_entries:
        return
    
    try:
        worksheet = ensure_logs_worksheet(sheet)
        log_entries = [normalize_log_entry(entry) for entry in log_entries]
        
        # Дробим на батчи
        for i in range(0, len(log_entries), config.BATCH_SIZE):
            batch = log_entries[i:i+config.BATCH_SIZE]
            try:
                worksheet.append_rows(batch, value_input_option='USER_ENTERED')
            except:
                # Fallback - по одной
                for entry in batch:
                    try:
                        worksheet.append_row(clean_sheet_row(entry), value_input_option='USER_ENTERED')
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

def load_settings(sheet):
    """Загрузка настроек"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        values = get_settings_values(worksheet)
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
            config.MAX_PUBLISH_AGE_HOURS = min(int(settings['max_publish_age_hours']), 24)
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

        if 'timezone' in settings:
            if settings['timezone'] == 'Asia/Baku':
                update_setting_value(worksheet, 'timezone', 'GMT+4', 'Часовой пояс для отображаемых дат')
                settings['timezone'] = 'GMT+4'
            config.DISPLAY_TIMEZONE = settings['timezone']
            print(f"  ✅ Display timezone: {config.DISPLAY_TIMEZONE}")
        else:
            worksheet.append_row(['timezone', config.DISPLAY_TIMEZONE, 'Часовой пояс для отображаемых дат'], value_input_option='USER_ENTERED')
            settings['timezone'] = config.DISPLAY_TIMEZONE
            print(f"  ✅ Created timezone setting: {config.DISPLAY_TIMEZONE}")
        
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


def update_run_status(sheet, status, details=''):
    """Write the current publisher status inside the settings table."""
    global _RUN_STATUS_ROW
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_SETTINGS)
        if _RUN_STATUS_ROW:
            values = get_settings_values(worksheet)
            _, table = find_setting_row(values, 'Статус run-ов')
            row_values = [''] * max(table['description_col'] or 0, table['value_col']) if table else [
                'Статус run-ов',
                clean_sheet_value(status),
                f"{clean_sheet_value(details)} | {format_timestamp()}".strip(),
            ]
            if table:
                row_values[table['key_col'] - 1] = 'Статус run-ов'
                row_values[table['value_col'] - 1] = clean_sheet_value(status)
                if table.get('description_col'):
                    row_values[table['description_col'] - 1] = f"{clean_sheet_value(details)} | {format_timestamp()}".strip()
            worksheet.update(
                range_name=f'A{_RUN_STATUS_ROW}:{a1_column(len(row_values))}{_RUN_STATUS_ROW}',
                values=[row_values],
                value_input_option='USER_ENTERED',
            )
            print(f"  ℹ️  Run status updated: {status}")
            return

        values = get_settings_values(worksheet)
        existing, table = find_setting_row(values, 'Статус run-ов')
        status_details = f"{clean_sheet_value(details)} | {format_timestamp()}".strip()

        if not table:
            update_setting_value(worksheet, 'Статус run-ов', status, status_details)
            print(f"  ℹ️  Run status updated: {status}")
            return

        target_row = existing['row_number'] if existing else table['first_data_row']
        if not existing:
            worksheet.insert_row([''], index=target_row, value_input_option='USER_ENTERED')

        row_values = [''] * max(table['description_col'] or 0, table['value_col'])
        row_values[table['key_col'] - 1] = 'Статус run-ов'
        row_values[table['value_col'] - 1] = clean_sheet_value(status)
        if table.get('description_col'):
            row_values[table['description_col'] - 1] = status_details

        worksheet.update(
            range_name=f'A{target_row}:{a1_column(len(row_values))}{target_row}',
            values=[row_values],
            value_input_option='USER_ENTERED',
        )
        _RUN_STATUS_ROW = target_row
        print(f"  ℹ️  Run status updated: {status}")
    except Exception as e:
        print(f"  ⚠️  Error updating run status: {e}")

def load_projects(sheet, update_status=True):
    """Загрузка активных проектов"""
    worksheet = sheet.worksheet(config.SHEET_NAME_PROJECTS)
    values = get_values_with_quota_retry(worksheet, PROJECTS_READ_RANGE)

    if not values:
        return []

    headers = ensure_project_status_columns(worksheet, values[0])
    projects = []
    status_updates = []
    default_updates = []

    def queue_status(row_index, row, status, error_text):
        if not update_status:
            return
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
            allow_streams = is_enabled_marker(row.get('Стримы'), default=False)
            allow_premieres = is_enabled_marker(row.get('Премьеры'), default=False)
            max_publish_age_hours = parse_positive_int_setting(
                row.get('Возраст видео, ч'),
                config.MAX_PUBLISH_AGE_HOURS,
            )
            if str(row.get('Push API', '')).strip() == '':
                default_updates.append((row_index, 'Push API', '🟢'))
            if str(row.get('RSS feed', '')).strip() == '':
                default_updates.append((row_index, 'RSS feed', '🟢'))
            tg_channel = str(row.get('Telegram канал @', '') or row.get('Telegram канал', '')).strip()
            channels_sheet_name = str(row.get('Название листа', '')).strip()
            rss_delete_limit_raw = str(row.get('RSS delete limit', '')).strip()
            if not rss_delete_limit_raw:
                rss_delete_limit_raw = '5'
                if update_status:
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
                'allow_streams': allow_streams,
                'allow_premieres': allow_premieres,
                'max_publish_age_hours': max_publish_age_hours,
                'push_api_enabled': is_enabled_marker(row.get('Push API'), default=True),
                'rss_feed_enabled': is_enabled_marker(row.get('RSS feed'), default=True),
                'rss_delete_limit': rss_delete_limit,
                'channel_count': 0,
                'disabled_channel_count': 0,
                '_settings_row': row_index,
                '_channel_count_col': headers.index('Кол. 🟢 каналов') + 1,
                '_disabled_channel_count_col': headers.index('Кол. 🔴 каналов') + 1,
                '_provisioning_status_col': headers.index('Provisioning status') + 1,
                '_provisioning_error_col': headers.index('Provisioning error') + 1,
                '_provisioned_at_col': headers.index('Provisioned at') + 1,
            })
            queue_status(row_index, row, 'ready', '')
        else:
            queue_status(row_index, row, 'inactive', '')

    if update_status:
        update_project_statuses(worksheet, headers, status_updates)
        update_project_default_values(worksheet, headers, default_updates)
    
    print(f"  ✅ Loaded {len(projects)} active projects")
    return projects

def load_youtube_channels(client, project):
    """Загрузка активных YouTube каналов проекта"""
    project.pop('channels_error', None)
    project['disabled_channel_count'] = 0
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
            if project.get('channels_error'):
                return {}

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
        values = get_values_with_quota_retry(worksheet, 'A:V')
        if not values:
            return {}

        headers = [str(cell).strip() for cell in values[0]]
        header_indexes = {header: index for index, header in enumerate(headers) if header}
        template_col = find_column_index(headers, CHANNEL_TEMPLATE_HEADERS)
        tg_col = find_column_index(headers, CHANNEL_TG_HEADERS)
        template_col_text = template_col + 1 if template_col is not None else 'fallback 21'
        tg_col_text = tg_col + 1 if tg_col is not None else 'not found'
        print(f"  📌 Channel columns: template={template_col_text}, tg_partner={tg_col_text}")
        channels = {}
        disabled_channel_ids = set()
        for i, row in enumerate(values):
            if i == 0:
                continue

            normalized = [str(cell).strip() for cell in row]
            if not any(normalized):
                continue

            if any(cell == '🔵' for cell in normalized):
                break

            channel_id = get_row_value(normalized, header_indexes, 'ID') or extract_youtube_channel_id_from_row(normalized)
            if '🔴' in normalized:
                if channel_id:
                    disabled_channel_ids.add(channel_id)
                continue

            if '🟢' not in normalized:
                continue

            if not channel_id:
                print(f"  ⚠️  Active row {i + 1} has no YouTube channel ID")
                continue

            channel_name = column_value(normalized, headers, CHANNEL_NAME_HEADERS) or get_row_value(normalized, header_indexes, 'Название') or infer_channel_name(normalized, channel_id)
            channel_template = column_value(normalized, headers, CHANNEL_TEMPLATE_HEADERS, fallback_index=20)
            tg_channel = partner_tg_link(column_value(normalized, headers, CHANNEL_TG_HEADERS))

            channels[channel_id] = {
                'name': channel_name,
                'template': channel_template,
                'tg_channel': tg_channel
            }

        project['disabled_channel_count'] = len(disabled_channel_ids)
        return channels
    except Exception as e:
        error_text = f"{type(e).__name__}: {e}"
        print(f"  ⚠️  Error reading channels sheet '{worksheet.title}' for {project['name']}: {error_text}")
        project['channels_error'] = error_text
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


def is_enabled_marker(value, default=True):
    text = str(value or '').strip().lower()
    if not text:
        return default
    if text in ('🟢', 'yes', 'true', '1', 'on', 'да', 'вкл'):
        return True
    if text in ('🔴', 'no', 'false', '0', 'off', 'нет', 'выкл'):
        return False
    return default


def parse_positive_int_setting(value, default):
    text = str(clean_sheet_value(value) or '').strip()
    if not text:
        return default
    try:
        parsed = int(float(text.replace(',', '.')))
        return parsed if parsed > 0 else default
    except ValueError:
        return default


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
            status = status_name_from_text(row.get('Системный статус', ''))
            if status == 'pending' or str(status).startswith('deleted'):
                continue
            video_id = video_id_from_url(row.get('Ссылка на видео', '')) or str(row.get('Video ID', '')).strip()
            project_name = project_name_from_cell(row.get('Проект', ''))
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
        if not values:
            return []

        headers = [str(value).strip() for value in values[0]]
        indexes = {header: index for index, header in enumerate(headers)}
        video_col = indexes.get('Video ID')
        channel_col = indexes.get('Ссылка на канал')
        status_col = indexes.get('Обработано')
        projects_col = indexes.get('Проекты')
        if video_col is None or channel_col is None or status_col is None:
            print("❌ Push events headers missing required columns")
            return []
        
        events = []
        for i, row in enumerate(values[1:], start=2):
            row = clean_row(row)
            status = row[status_col] if len(row) > status_col else ''
            if status == '' or status == '❌':
                video_id = row[video_col] if len(row) > video_col else ''
                channel_value = row[channel_col] if len(row) > channel_col else ''
                channel_id = channel_id_from_link(channel_value) or channel_value
                if video_id and channel_id:
                    events.append({
                        'row_index': i,
                        'video_id': video_id,
                        'channel_id': channel_id,
                        'projects': row[projects_col] if projects_col is not None and len(row) > projects_col else '',
                    })
        
        return events
    except Exception as e:
        print(f"❌ Error loading push events: {e}")
        return []

def mark_push_event_processed(sheet, row_index, project_name, current_projects=''):
    """Отметка push-события как обработанного"""
    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_PUSH_EVENTS)
        current_projects = clean_sheet_value(current_projects)
        if project_name not in current_projects:
            new_projects = (current_projects + ', ' + project_name).strip(', ')
        else:
            new_projects = current_projects
        headers = get_values_with_quota_retry(worksheet, '1:1')
        header_row = [str(value).strip() for value in headers[0]] if headers else []
        indexes = {header: index + 1 for index, header in enumerate(header_row)}
        status_col = indexes.get('Обработано')
        projects_col = indexes.get('Проекты')
        if not status_col or not projects_col:
            raise ValueError('Push events headers missing Обработано/Проекты')
        worksheet.batch_update([
            {'range': gspread.utils.rowcol_to_a1(row_index, status_col), 'values': [['✅']]},
            {'range': gspread.utils.rowcol_to_a1(row_index, projects_col), 'values': [[new_projects]]},
        ], value_input_option='USER_ENTERED')
        return new_projects
    except Exception as e:
        print(f"  ⚠️  Error marking event: {e}")
        return current_projects


def mark_push_events_processed_batch(sheet, tracked_events):
    """Mark processed push events in batches instead of one Sheets request per event."""
    if not tracked_events:
        return

    try:
        worksheet = sheet.worksheet(config.SHEET_NAME_PUSH_EVENTS)
        headers = get_values_with_quota_retry(worksheet, '1:1')
        header_row = [str(value).strip() for value in headers[0]] if headers else []
        indexes = {header: index + 1 for index, header in enumerate(header_row)}
        status_col = indexes.get('Обработано')
        projects_col = indexes.get('Проекты')
        if not status_col or not projects_col:
            raise ValueError('Push events headers missing Обработано/Проекты')
        updates = []
        for tracked in tracked_events:
            current_projects = clean_sheet_value(tracked.get('projects', ''))
            project_names = tracked.get('project_names') or []
            project_set = {
                value.strip()
                for value in str(current_projects).split(',')
                if value.strip()
            }
            project_set.update(str(value).strip() for value in project_names if str(value).strip())
            new_projects = ', '.join(sorted(project_set))
            row_index = tracked['row_index']
            updates.extend([
                {'range': gspread.utils.rowcol_to_a1(row_index, status_col), 'values': [['✅']]},
                {'range': gspread.utils.rowcol_to_a1(row_index, projects_col), 'values': [[new_projects]]},
            ])

        for i in range(0, len(updates), 100):
            worksheet.batch_update(updates[i:i + 100], value_input_option='USER_ENTERED')
            time.sleep(0.2)
    except Exception as e:
        print(f"  ⚠️  Error marking push events batch: {e}")
