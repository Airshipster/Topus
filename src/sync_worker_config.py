import argparse
import hashlib
import os
import re
from datetime import datetime

import requests

import config
from sheets import (
    authenticate_google_sheets,
    clean_sheet_value,
    column_value,
    extract_sheet_id,
    extract_youtube_channel_id_from_row,
    find_column_index,
    get_row_value,
    infer_channel_name,
    is_enabled_marker,
    normalize_project_row,
    parse_datetime_value,
)


BOT_COLUMN_NAMES = ['Бот', 'Индивидуальный бот', 'Персональный бот']
TELEGRAM_BOT_COLUMN_NAMES = ['Telegram-бот', 'Telegram бот', 'Telegram bot', 'Telegram Bot']
CATEGORY_COLUMN_NAMES = ['Категория', 'Категории', 'Category', 'Раздел']
CHANNEL_NAME_HEADERS = ['Название', 'Название канала', 'Канал', 'YouTube канал']
LAST_VIDEO_HEADERS = [
    'Посл. вид.',
    'Посл. видео',
    'Последнее видео',
    'Год послед. видео',
    'Last video',
    'Last Video',
]
CHANNEL_STATS_SHEET_NAMES = ['Стат. Каналы', 'Статистика каналов', 'Статистика', 'Channel Stats', 'Channels Stats']
CATEGORY_MARKER = '🟡'
BOT_DESCRIPTION = (
    'Бот SciTopus помогает собрать личную ленту уведомлений по научпоп YouTube-каналам. '
    'Выбирайте отдельные каналы, категории или весь список SciTopus. '
    'Для использования нужна подписка на основной Telegram-канал.'
)
BOT_SHORT_DESCRIPTION = 'Личная лента уведомлений по научпоп YouTube-каналам SciTopus.'


def slug(value):
    digest = hashlib.sha1(str(value or '').encode('utf-8')).hexdigest()[:16]
    return f'cat_{digest}'


def webhook_secret(project_code, admin_secret):
    return hashlib.sha256(f'{admin_secret}:{project_code}'.encode('utf-8')).hexdigest()[:32]


def split_category_path(value):
    text = str(clean_sheet_value(value) or '').strip()
    if not text:
        return []
    return [item.strip() for item in re.split(r'\s*(?:/|>|→|\|)\s*', text) if item.strip()]


def infer_category_title(row, headers, category_col):
    if category_col is not None and len(row) > category_col and clean_sheet_value(row[category_col]):
        return clean_sheet_value(row[category_col])

    title = column_value(row, headers, CATEGORY_COLUMN_NAMES + CHANNEL_NAME_HEADERS)
    if title:
        return title

    ignored = {'🟢', '🔴', '🟡', '🔵'}
    for cell in row:
        value = clean_sheet_value(cell)
        if not value or value in ignored:
            continue
        if 'youtube.com' in str(value) or 'youtu.be' in str(value):
            continue
        if re.search(r'UC[0-9A-Za-z_-]{20,}', str(value)):
            continue
        return value

    return 'Без названия'


def read_bot_projects(master_sheet):
    worksheet = master_sheet.worksheet(config.SHEET_NAME_PROJECTS)
    values = worksheet.get_all_values()
    if not values:
        return []

    headers = [str(cell).strip() for cell in values[0]]
    projects = []

    for raw_row in values[1:]:
        if any(str(cell).strip() == 'Настройки' for cell in raw_row):
            break
        if any(str(cell).strip() == '🔵' for cell in raw_row):
            break
        if not any(str(cell).strip() for cell in raw_row):
            continue

        row = normalize_project_row(headers, raw_row)
        if row.get('Активен') != '🟢':
            continue
        if not is_enabled_marker(row.get('Бот') or row.get('Индивидуальный бот') or row.get('Персональный бот'), default=False):
            continue

        sheet_id = extract_sheet_id(row.get('Ссылка на документ проекта', ''))
        project_code = str(row.get('Код проекта') or '').strip()
        bot_token = str(row.get('Telegram bot token') or '').strip()
        bot_username = telegram_channel_ref(column_value(raw_row, headers, TELEGRAM_BOT_COLUMN_NAMES) or row.get('Telegram-бот') or '')
        main_channel = telegram_channel_ref(
            row.get('Telegram канал @') or row.get('Telegram канал') or row.get('Telegram канал ID') or ''
        )
        if not sheet_id or not project_code or not bot_token:
            print(f"  ⚠️  Bot project skipped: missing sheet/code/token ({row.get('Название', project_code)})")
            continue

        projects.append({
            'code': project_code,
            'name': str(row.get('Название') or project_code).strip(),
            'sheet_id': sheet_id,
            'channels_sheet_name': str(row.get('Название листа') or '').strip(),
            'bot_token': bot_token,
            'bot_username': bot_username,
            'main_channel': main_channel,
        })

    return projects


def telegram_channel_ref(value):
    text = str(clean_sheet_value(value) or '').strip()
    if not text:
        return ''
    match = re.search(r'(?:https?://)?t\.me/([A-Za-z0-9_]+)', text)
    if match:
        return '@' + match.group(1)
    if text.startswith('@') or re.fullmatch(r'-?\d+', text):
        return text
    if re.fullmatch(r'[A-Za-z0-9_]{5,}', text):
        return '@' + text
    return text


def category_id_for_path(parts, depth):
    return slug(' / '.join(parts[:depth]))


def normalize_header(value):
    return re.sub(r'\s+', ' ', str(value or '').strip()).lower()


def flexible_column_value(row, headers, candidates):
    index = flexible_column_index(headers, candidates)
    if index is not None and index < len(row):
        return row[index]
    return ''


def flexible_column_index(headers, candidates):
    normalized_candidates = {normalize_header(candidate) for candidate in candidates}
    for index, header in enumerate(headers):
        if normalize_header(header) in normalized_candidates:
            return index
    return None


def last_video_timestamp(row, headers):
    value = flexible_column_value(row, headers, LAST_VIDEO_HEADERS) or column_value(row, headers, LAST_VIDEO_HEADERS)
    text = str(clean_sheet_value(value) or '').strip()
    year_match = re.fullmatch(r'(19|20)\d{2}', text)
    if year_match:
        return f'{text}-12-31T23:59:59'
    for fmt in ('%d.%m.%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(text, fmt).isoformat()
        except ValueError:
            pass
    parsed = parse_datetime_value(value)
    return parsed.isoformat() if parsed else ''


def channel_id_from_stats_row(row, headers):
    header_indexes = {header: index for index, header in enumerate(headers) if header}
    return (
        get_row_value(row, header_indexes, 'ID')
        or flexible_column_value(row, headers, ['Channel ID', 'ID канала', 'ID', 'Ссылка на канал', '/channel/'])
        or column_value(row, headers, ['Channel ID', 'ID канала', 'ID', 'Ссылка на канал', '/channel/'])
        or extract_youtube_channel_id_from_row(row)
    )


def find_header_row(values):
    for index, row in enumerate(values[:20]):
        normalized = [normalize_header(cell) for cell in row]
        has_channel = any(value in {'id', 'channel id', 'id канала', 'ссылка на канал', '/channel/'} for value in normalized)
        has_last_video = any(value in {normalize_header(item) for item in LAST_VIDEO_HEADERS} for value in normalized)
        if has_channel and has_last_video:
            return index
    return 0


def read_channel_stats_last_videos(sheet):
    worksheet = None
    selected_name = ''
    for sheet_name in CHANNEL_STATS_SHEET_NAMES:
        try:
            worksheet = sheet.worksheet(sheet_name)
            selected_name = sheet_name
            break
        except Exception:
            pass
    if worksheet is None:
        print("  ℹ️  Channel stats sheet not found; inactive-channel hiding will use channel-list dates only")
        return {}

    values = worksheet.get_all_values()
    if not values:
        return {}

    header_row = find_header_row(values)
    headers = [str(cell).strip() for cell in values[header_row]]
    stats = {}
    for raw_row in values[header_row + 1:]:
        row = [str(cell).strip() for cell in raw_row]
        channel_id = channel_id_from_stats_row(row, headers)
        if not channel_id:
            continue
        timestamp = last_video_timestamp(row, headers)
        if timestamp:
            stats[channel_id] = timestamp
    print(f"  📊 Channel stats dates loaded from '{selected_name}': {len(stats)}")
    return stats


def read_project_channels(client, project):
    sheet = client.open_by_key(project['sheet_id'])
    stats_last_videos = read_channel_stats_last_videos(sheet)
    worksheets = []
    if project.get('channels_sheet_name'):
        try:
            worksheets.append(sheet.worksheet(project['channels_sheet_name']))
        except Exception as error:
            print(f"  ⚠️  Configured channels sheet unavailable for {project['name']}: {error}")
    if not worksheets:
        worksheets = sheet.worksheets()[:1]

    categories_by_id = {}
    channels = []

    for worksheet in worksheets:
        values = worksheet.get_all_values()
        if not values:
            continue
        headers = [str(cell).strip() for cell in values[0]]
        header_indexes = {header: index for index, header in enumerate(headers) if header}
        last_video_col = flexible_column_index(headers, LAST_VIDEO_HEADERS)
        if last_video_col is None:
            print(f"  ⚠️  Last-video column not found in '{worksheet.title}'. Headers: {headers}")
        else:
            print(f"  📌 Last-video column in '{worksheet.title}': {headers[last_video_col]} ({last_video_col + 1})")
        category_col = find_column_index(headers, CATEGORY_COLUMN_NAMES)
        current_category_id = 'root'
        raw_last_video_count = 0
        parsed_last_video_count = 0

        for sort_order, raw_row in enumerate(values[1:], start=1):
            normalized = [str(cell).strip() for cell in raw_row]
            if not any(normalized):
                continue
            if any(cell == '🔵' for cell in normalized):
                break
            if CATEGORY_MARKER in normalized:
                title = infer_category_title(normalized, headers, category_col)
                category_parts = split_category_path(title) or [title]
                parent_id = 'root'
                for depth, part in enumerate(category_parts, start=1):
                    category_id = category_id_for_path(category_parts, depth)
                    if category_id not in categories_by_id:
                        categories_by_id[category_id] = {
                            'id': category_id,
                            'parentId': parent_id,
                            'title': part,
                            'sortOrder': sort_order,
                        }
                    parent_id = category_id
                current_category_id = parent_id
                continue
            if '🟢' not in normalized and '🔴' not in normalized:
                continue

            channel_id = get_row_value(normalized, header_indexes, 'ID') or extract_youtube_channel_id_from_row(normalized)
            if not channel_id:
                continue

            category_parts = split_category_path(normalized[category_col] if category_col is not None and len(normalized) > category_col else '')
            parent_id = 'root'
            category_id = current_category_id
            for depth, title in enumerate(category_parts, start=1):
                category_id = category_id_for_path(category_parts, depth)
                if category_id not in categories_by_id:
                    categories_by_id[category_id] = {
                        'id': category_id,
                        'parentId': parent_id,
                        'title': title,
                        'sortOrder': len(categories_by_id) + 1,
                    }
                parent_id = category_id

            channel_name = (
                column_value(normalized, headers, CHANNEL_NAME_HEADERS)
                or get_row_value(normalized, header_indexes, 'Название')
                or infer_channel_name(normalized, channel_id)
            )
            raw_last_video = normalized[last_video_col] if last_video_col is not None and last_video_col < len(normalized) else ''
            parsed_last_video = stats_last_videos.get(channel_id) or last_video_timestamp(normalized, headers)
            if raw_last_video:
                raw_last_video_count += 1
            if parsed_last_video:
                parsed_last_video_count += 1
            channels.append({
                'id': channel_id,
                'title': channel_name,
                'categoryId': category_id,
                'status': 'green' if '🟢' in normalized else 'red',
                'sortOrder': sort_order,
                'lastVideoAt': parsed_last_video,
            })

        print(
            f"  📊 Channel-list last-video values in '{worksheet.title}': "
            f"raw={raw_last_video_count}, parsed={parsed_last_video_count}"
        )

    return list(categories_by_id.values()), channels


def build_payload(client, master_sheet, admin_secret):
    projects = []
    for project in read_bot_projects(master_sheet):
        categories, channels = read_project_channels(client, project)
        projects.append({
            'code': project['code'],
            'name': project['name'],
            'botToken': project['bot_token'],
            'botUsername': project.get('bot_username', ''),
            'webhookSecret': webhook_secret(project['code'], admin_secret),
            'mainChannel': project.get('main_channel', ''),
            'active': True,
            'categories': categories,
            'channels': channels,
        })
        print(f"  ✅ Prepared {project['name']}: {len(categories)} categories, {len(channels)} channels")

    return {'projects': projects, 'generatedAt': datetime.utcnow().isoformat() + 'Z'}


def post_sync(worker_url, admin_secret, payload):
    response = requests.post(
        worker_url.rstrip('/') + '/admin/sync',
        json=payload,
        headers={'x-admin-secret': admin_secret},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def set_telegram_webhooks(worker_url, payload):
    for project in payload['projects']:
        webhook_url = f"{worker_url.rstrip()}/telegram/{project['code']}/{project['webhookSecret']}"
        response = requests.post(
            f"https://api.telegram.org/bot{project['botToken']}/setWebhook",
            json={
                'url': webhook_url,
                'allowed_updates': ['message', 'callback_query', 'pre_checkout_query', 'chat_boost', 'removed_chat_boost'],
            },
            timeout=15,
        )
        response.raise_for_status()
        result = response.json()
        print(f"  {'✅' if result.get('ok') else '⚠️ '} Webhook {project['code']}: {result}")


def set_telegram_bot_descriptions(payload):
    for project in payload['projects']:
        for method, text in (
            ('setMyDescription', BOT_DESCRIPTION),
            ('setMyShortDescription', BOT_SHORT_DESCRIPTION),
        ):
            response = requests.post(
                f"https://api.telegram.org/bot{project['botToken']}/{method}",
                json={'description' if method == 'setMyDescription' else 'short_description': text},
                timeout=15,
            )
            response.raise_for_status()
            result = response.json()
            print(f"  {'✅' if result.get('ok') else '⚠️ '} {method} {project['code']}: {result}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--set-webhooks', action='store_true')
    args = parser.parse_args()

    worker_url = os.environ.get('TOPUS_WORKER_URL', '').strip()
    admin_secret = os.environ.get('TOPUS_WORKER_ADMIN_SECRET', '').strip()
    if not worker_url:
        raise ValueError('TOPUS_WORKER_URL is required')
    if not admin_secret:
        raise ValueError('TOPUS_WORKER_ADMIN_SECRET is required')

    client = authenticate_google_sheets()
    master_sheet = client.open_by_key(config.SPREADSHEET_ID)
    payload = build_payload(client, master_sheet, admin_secret)
    result = post_sync(worker_url, admin_secret, payload)
    print(f"  ✅ Worker sync result: {result}")
    if args.set_webhooks:
        set_telegram_webhooks(worker_url, payload)
        set_telegram_bot_descriptions(payload)


if __name__ == '__main__':
    main()
