import re
import time

import gspread

import config
from sheets import (
    authenticate_google_sheets,
    batch_update_with_quota_retry,
    effective_youtube_publication_timestamp,
    find_column_index,
    first_value,
    get_values_with_quota_retry,
    header_indexes,
    load_settings,
    project_name_from_cell,
    publication_delay_minutes,
    row_as_dict,
    sheet_datetime_value,
    status_method_from_text,
    status_name_from_text,
    video_id_from_url,
)


def main():
    client = authenticate_google_sheets()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    load_settings(sheet)

    videos = sheet.worksheet(config.SHEET_NAME_VIDEOS)
    logs = sheet.worksheet('Логи')
    video_values = get_values_with_quota_retry(videos)
    log_values = get_values_with_quota_retry(logs)
    if len(video_values) < 2 or len(log_values) < 2:
        print('No rows to repair')
        return

    video_headers = [str(value).strip() for value in video_values[0]]
    video_indexes = {header: index + 1 for header, index in header_indexes(video_headers).items()}
    for header in ('Системный статус', 'Разница в минутах'):
        index = find_column_index(video_headers, [header])
        if index is not None:
            video_indexes[header] = index + 1
    log_headers = [str(value).strip() for value in log_values[0]]

    bot_logs = {}
    for row in log_values[1:]:
        data = row_as_dict(log_headers, row)
        project = project_name_from_cell(first_value(data, ['Проект']))
        video_id = first_value(data, ['Video ID'])
        event = first_value(data, ['Событие'])
        timestamp = first_value(data, ['Timestamp GMT+4', 'Timestamp'])
        match = re.search(r'Bot subscribers queued:\s*(\d+)', str(event), flags=re.IGNORECASE)
        if project and video_id and timestamp and match:
            bot_logs[(video_id, project)] = (timestamp, f'bot:{match.group(1)}')

    updates = []
    repaired = 0
    missing_logs = 0
    for row_index, row in enumerate(video_values[1:], start=2):
        data = row_as_dict(video_headers, row)
        status_text = first_value(data, ['Системный статус'])
        method, _ = status_method_from_text(status_text)
        if not method.lower().startswith('bot:') or status_name_from_text(status_text) != 'published':
            continue

        project = project_name_from_cell(first_value(data, ['Проект']))
        video_id = video_id_from_url(first_value(data, ['Ссылка на видео', 'Video ID']))
        current_tg = first_value(data, ['Дата публикации TG GMT+4', 'Дата публикации TG Asia/Baku', 'Дата публикации TG'])
        current_delay = first_value(data, ['Разница в минутах'])
        current_message_id = first_value(data, ['TG message_id'])
        if current_tg and current_delay and current_message_id:
            continue

        log_entry = bot_logs.get((video_id, project))
        if not log_entry:
            missing_logs += 1
            print(f'Missing bot log for row {row_index}: {project} / {video_id}')
            continue

        published_at, bot_message_id = log_entry
        yt_published = effective_youtube_publication_timestamp(
            None,
            first_value(data, ['Дата публикации YT GMT+4', 'Дата публикации YT UTC']),
        )
        delay = publication_delay_minutes(yt_published, published_at)
        values_by_header = {
            'Дата публикации TG GMT+4': sheet_datetime_value(published_at),
            'Разница в минутах': delay,
            'TG message_id': bot_message_id,
        }
        for header, value in values_by_header.items():
            column = video_indexes.get(header)
            if column:
                updates.append({
                    'range': gspread.utils.rowcol_to_a1(row_index, column),
                    'values': [[value]],
                })
        repaired += 1
        print(f'Repaired row {row_index}: {project} / {video_id} -> {published_at}, {delay}, {bot_message_id}')

    for i in range(0, len(updates), config.BATCH_SIZE):
        batch_update_with_quota_retry(videos, updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
        time.sleep(0.2)

    print(f'Bot-only timing repair complete: repaired={repaired}, missing_logs={missing_logs}, updates={len(updates)}')


if __name__ == '__main__':
    main()
