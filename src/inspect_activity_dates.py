from collections import Counter

import config
from sheets import (
    authenticate_google_sheets,
    cell_value,
    current_local_datetime,
    find_column_index,
    get_values_with_quota_retry,
    parse_datetime_value,
)


TARGETS = [
    ('Логи', ['Timestamp GMT+4', 'Timestamp']),
    (config.SHEET_NAME_PUSH_EVENTS, ['Timestamp GMT+4', 'Timestamp']),
    (config.SHEET_NAME_VIDEOS, ['Дата обработки GMT+4', 'Дата обработки Asia/Baku', 'Дата обработки UTC']),
]


def main():
    client = authenticate_google_sheets()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    now = current_local_datetime()
    print(f'Now: {now:%Y-%m-%d %H:%M:%S}')
    print(f'Retention days: {config.ACTIVITY_RETENTION_DAYS}')

    for worksheet_name, date_headers in TARGETS:
        worksheet = sheet.worksheet(worksheet_name)
        values = get_values_with_quota_retry(worksheet)
        headers = [str(value).strip() for value in values[0]] if values else []
        date_col = find_column_index(headers, date_headers)
        if date_col is None:
            print(f'\n{worksheet_name}: date column not found')
            continue

        dates = []
        blank_date_rows = 0
        for row in values[1:]:
            if not any(str(cell).strip() for cell in row):
                continue
            parsed = parse_datetime_value(cell_value(row, date_col))
            if parsed:
                dates.append(parsed)
            else:
                blank_date_rows += 1

        print(f'\n{worksheet_name}: rows with date={len(dates)}, rows without parsed date={blank_date_rows}')
        if not dates:
            continue
        print(f'  min={min(dates):%Y-%m-%d %H:%M:%S}')
        print(f'  max={max(dates):%Y-%m-%d %H:%M:%S}')
        counts = Counter(date.strftime('%Y-%m-%d') for date in dates)
        for day in sorted(counts):
            print(f'  {day}: {counts[day]}')


if __name__ == '__main__':
    main()
