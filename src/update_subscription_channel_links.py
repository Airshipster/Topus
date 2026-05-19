import time

import config
from sheets import authenticate_google_sheets, clean_sheet_value, get_values_with_quota_retry
from subscriptions import (
    SUBSCRIPTIONS_READ_RANGE,
    SUBSCRIPTIONS_SHEET_NAME,
    normalize_subscription_channel_id,
    subscription_channel_formula,
    subscription_header_indexes,
)


def main():
    client = authenticate_google_sheets()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = sheet.worksheet(SUBSCRIPTIONS_SHEET_NAME)
    values = get_values_with_quota_retry(worksheet, SUBSCRIPTIONS_READ_RANGE)
    if not values:
        print('Subscriptions sheet is empty')
        return

    headers = [str(cell).strip() for cell in values[0]]
    indexes = subscription_header_indexes(headers)
    channel_col = indexes.get('Channel ID', 2)

    updates = []
    for row_index, row in enumerate(values[1:], start=2):
        raw_value = clean_sheet_value(row[channel_col]).strip() if len(row) > channel_col else ''
        channel_id = normalize_subscription_channel_id(raw_value)
        if not channel_id:
            continue
        updates.append({
            'range': f'C{row_index}',
            'values': [[subscription_channel_formula(channel_id)]],
        })

    for i in range(0, len(updates), config.BATCH_SIZE):
        worksheet.batch_update(updates[i:i + config.BATCH_SIZE], value_input_option='USER_ENTERED')
        time.sleep(0.2)

    print(f'Updated subscription channel links: {len(updates)}')


if __name__ == '__main__':
    main()
