import time

from sheets import authenticate_google_sheets, clean_sheet_value, get_values_with_quota_retry, is_sheets_quota_error
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

    formulas = []
    for row_index, row in enumerate(values[1:], start=2):
        raw_value = clean_sheet_value(row[channel_col]).strip() if len(row) > channel_col else ''
        channel_id = normalize_subscription_channel_id(raw_value)
        formulas.append([subscription_channel_formula(channel_id) if channel_id else ''])

    if not formulas:
        print('No subscription channel links to update')
        return

    delay_seconds = 10
    target_range = f'C2:C{len(formulas) + 1}'
    for attempt in range(1, 5):
        try:
            worksheet.update(range_name=target_range, values=formulas, value_input_option='USER_ENTERED')
            break
        except Exception as error:
            if not is_sheets_quota_error(error) or attempt >= 4:
                raise
            print(f'Sheets quota busy while updating channel links; retry {attempt}/3 in {delay_seconds}s')
            time.sleep(delay_seconds)
            delay_seconds *= 2

    print(f'Updated subscription channel links: {len(formulas)}')


if __name__ == '__main__':
    main()
