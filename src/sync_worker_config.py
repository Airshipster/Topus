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
)


BOT_COLUMN_NAMES = ['Бот', 'Индивидуальный бот', 'Персональный бот']
CATEGORY_COLUMN_NAMES = ['Категория', 'Категории', 'Category', 'Раздел']
CHANNEL_NAME_HEADERS = ['Название', 'Название канала', 'Канал', 'YouTube канал']


def slug(value):
    text = str(value or '').strip().lower()
    text = re.sub(r'[^0-9a-zа-яё]+', '-', text, flags=re.IGNORECASE).strip('-')
    digest = hashlib.sha1(str(value or '').encode('utf-8')).hexdigest()[:8]
    return f'{text[:40] or "category"}-{digest}'


def webhook_secret(project_code, admin_secret):
    return hashlib.sha256(f'{admin_secret}:{project_code}'.encode('utf-8')).hexdigest()[:32]


def split_category_path(value):
    text = str(clean_sheet_value(value) or '').strip()
    if not text:
        return []
    return [item.strip() for item in re.split(r'\s*(?:/|>|→|\|)\s*', text) if item.strip()]


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
        if not sheet_id or not project_code or not bot_token:
            print(f"  ⚠️  Bot project skipped: missing sheet/code/token ({row.get('Название', project_code)})")
            continue

        projects.append({
            'code': project_code,
            'name': str(row.get('Название') or project_code).strip(),
            'sheet_id': sheet_id,
            'channels_sheet_name': str(row.get('Название листа') or '').strip(),
            'bot_token': bot_token,
        })

    return projects


def category_id_for_path(parts, depth):
    return slug(' / '.join(parts[:depth]))


def read_project_channels(client, project):
    sheet = client.open_by_key(project['sheet_id'])
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
        category_col = find_column_index(headers, CATEGORY_COLUMN_NAMES)

        for sort_order, raw_row in enumerate(values[1:], start=1):
            normalized = [str(cell).strip() for cell in raw_row]
            if not any(normalized):
                continue
            if any(cell == '🔵' for cell in normalized):
                break
            if '🟢' not in normalized and '🔴' not in normalized:
                continue

            channel_id = get_row_value(normalized, header_indexes, 'ID') or extract_youtube_channel_id_from_row(normalized)
            if not channel_id:
                continue

            category_parts = split_category_path(normalized[category_col] if category_col is not None and len(normalized) > category_col else '')
            parent_id = 'root'
            category_id = 'root'
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
            channels.append({
                'id': channel_id,
                'title': channel_name,
                'categoryId': category_id,
                'status': 'green' if '🟢' in normalized else 'red',
                'sortOrder': sort_order,
            })

    return list(categories_by_id.values()), channels


def build_payload(client, master_sheet, admin_secret):
    projects = []
    for project in read_bot_projects(master_sheet):
        categories, channels = read_project_channels(client, project)
        projects.append({
            'code': project['code'],
            'name': project['name'],
            'botToken': project['bot_token'],
            'webhookSecret': webhook_secret(project['code'], admin_secret),
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
                'allowed_updates': ['message', 'callback_query'],
            },
            timeout=15,
        )
        response.raise_for_status()
        result = response.json()
        print(f"  {'✅' if result.get('ok') else '⚠️ '} Webhook {project['code']}: {result}")


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


if __name__ == '__main__':
    main()
