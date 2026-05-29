from sync_bot_state_sheet import HEADERS, SHEET_NAME
from sheets import authenticate_google_sheets, get_values_with_quota_retry
import config


def is_date_like(value):
    text = str(value or '').strip()
    if not text:
        return False
    return any(separator in text for separator in ['.', '-', ':']) and any(char.isdigit() for char in text)


def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(SHEET_NAME)
    values = get_values_with_quota_retry(worksheet)
    headers = values[0] if values else []
    print(f'rows={len(values)} cols={len(headers)}')
    for index, header in enumerate(headers[:30], start=1):
        expected = HEADERS[index - 1] if index <= len(HEADERS) else ''
        marker = 'OK' if header == expected else 'DIFF'
        print(f'header {index}: actual={header!r} expected={expected!r} {marker}')

    bad_total = []
    nonempty_excluded = []
    rows_with_dates_near_counts = []
    for row_number, row in enumerate(values[1:], start=2):
        row += [''] * 30
        total_channels = str(row[13]).strip()
        excluded = str(row[11]).strip()
        if total_channels and not total_channels.isdigit():
            bad_total.append((row_number, row[0], row[2], row[9], row[10], row[11], row[12], row[13], row[14], row[15]))
        if excluded:
            nonempty_excluded.append((row_number, row[0], row[2], excluded[:140]))
        if any(is_date_like(row[index]) for index in [10, 11, 12, 13, 14]):
            rows_with_dates_near_counts.append((row_number, row[0], row[2], row[10], row[11], row[12], row[13], row[14]))

    print(f'bad_total_channels={len(bad_total)}')
    for item in bad_total[:12]:
        print('bad_total_sample=' + repr(item))
    print(f'nonempty_excluded={len(nonempty_excluded)}')
    for item in nonempty_excluded[:12]:
        print('excluded_sample=' + repr(item))
    print(f'rows_with_dates_K_to_O={len(rows_with_dates_near_counts)}')
    for item in rows_with_dates_near_counts[:12]:
        print('date_near_count_sample=' + repr(item))


if __name__ == '__main__':
    main()
