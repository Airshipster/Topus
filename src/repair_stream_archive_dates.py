import os
import time

import gspread

import config
from sheets import (
    authenticate_google_sheets,
    batch_update_with_quota_retry,
    find_column_index,
    first_value,
    get_values_with_quota_retry,
    load_settings,
    parse_datetime_value,
    publication_delay_minutes,
    row_as_dict,
    sheet_datetime_value,
    status_name_from_text,
    video_id_from_url,
)
from youtube_client import get_last_youtube_api_error, get_video_info_from_api


def same_timestamp(left, right):
    left_dt = parse_datetime_value(left)
    right_dt = parse_datetime_value(right)
    if not left_dt or not right_dt:
        return str(left or '').strip() == str(right or '').strip()
    return abs((left_dt - right_dt).total_seconds()) < 1


def main():
    dry_run = os.environ.get('TOPUS_DRY_RUN', '').lower() in ('1', 'true', 'yes')
    client = authenticate_google_sheets()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    load_settings(sheet)

    worksheet = sheet.worksheet(config.SHEET_NAME_VIDEOS)
    values = get_values_with_quota_retry(worksheet)
    if len(values) < 2:
        print('No video rows')
        return

    headers = [str(value).strip() for value in values[0]]
    yt_col = find_column_index(headers, ['Дата публикации YT GMT+4', 'Дата публикации YT UTC'])
    tg_col = find_column_index(headers, ['Дата публикации TG GMT+4', 'Дата публикации TG Asia/Baku', 'Дата публикации TG'])
    delay_col = find_column_index(headers, ['Разница в минутах'])
    status_col = find_column_index(headers, ['Системный статус'])
    if yt_col is None or tg_col is None or delay_col is None or status_col is None:
        raise ValueError('Required Global videos columns are missing')

    updates = []
    checked = 0
    live_rows = 0
    changed = 0
    skipped_no_end = 0
    skipped_api = 0

    for row_index, row in enumerate(values[1:], start=2):
        data = row_as_dict(headers, row)
        status_text = first_value(data, ['Системный статус'])
        status_name = status_name_from_text(status_text)
        if status_name not in ('published', 'filtered', 'pending') or 'stream' not in str(status_text).lower():
            continue

        video_id = video_id_from_url(first_value(data, ['Ссылка на видео', 'Video ID']))
        if not video_id:
            continue

        checked += 1
        video_info = get_video_info_from_api(video_id)
        if not video_info:
            skipped_api += 1
            print(f"API skipped row {row_index}: {video_id} ({get_last_youtube_api_error() or 'not found'})")
            continue
        if not video_info.get('was_live'):
            continue

        live_rows += 1
        actual_end = video_info.get('live_actual_end')
        if not actual_end:
            skipped_no_end += 1
            print(f"No actualEndTime row {row_index}: {video_id}")
            continue

        current_yt = first_value(data, ['Дата публикации YT GMT+4', 'Дата публикации YT UTC'])
        tg_published = first_value(data, ['Дата публикации TG GMT+4', 'Дата публикации TG Asia/Baku', 'Дата публикации TG'])
        new_delay = publication_delay_minutes(actual_end, tg_published) if tg_published else ''
        row_changed = False

        if not same_timestamp(current_yt, actual_end):
            updates.append({
                'range': gspread.utils.rowcol_to_a1(row_index, yt_col + 1),
                'values': [[sheet_datetime_value(actual_end)]],
            })
            row_changed = True
        if tg_published and str(new_delay) != str(first_value(data, ['Разница в минутах'])).strip():
            updates.append({
                'range': gspread.utils.rowcol_to_a1(row_index, delay_col + 1),
                'values': [[new_delay]],
            })
            row_changed = True

        if row_changed:
            changed += 1
            print(f"Repair row {row_index}: {video_id} -> YT {actual_end}, delay {new_delay}")

        if len(updates) >= config.BATCH_SIZE:
            if dry_run:
                updates.clear()
            else:
                batch_update_with_quota_retry(worksheet, updates, value_input_option='USER_ENTERED')
                updates.clear()
                time.sleep(0.2)

    if updates and not dry_run:
        batch_update_with_quota_retry(worksheet, updates, value_input_option='USER_ENTERED')

    print(
        f"Stream archive date repair complete: checked={checked}, live_rows={live_rows}, "
        f"changed={changed}, skipped_no_end={skipped_no_end}, skipped_api={skipped_api}, dry_run={dry_run}"
    )


if __name__ == '__main__':
    main()
