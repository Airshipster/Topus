import os

import gspread

import config
from sheets import authenticate_google_sheets, a1_column, find_column_index, get_values_with_quota_retry


def status_summary_formula(column_letter):
    status_range = f'{column_letter}2:{column_letter}'
    return (
        '="Системный статус"&СИМВОЛ(10)&'
        f'"Push: "&СЧЁТЕСЛИ({status_range};"Push:*")&" | "'
        f'&"RSS: "&СЧЁТЕСЛИ({status_range};"RSS:*")&" | "'
        f'&"Bot Push: "&СЧЁТЕСЛИ({status_range};"Bot: Push:*")&" | "'
        f'&"Bot RSS: "&СЧЁТЕСЛИ({status_range};"Bot: RSS:*")&СИМВОЛ(10)&'
        f'"published: "&СЧЁТЕСЛИ({status_range};"*published*")&" | "'
        f'&"filtered: "&СЧЁТЕСЛИ({status_range};"*filtered*")&" | "'
        f'&"pending: "&СЧЁТЕСЛИ({status_range};"*pending*")&" | "'
        f'&"failed: "&СЧЁТЕСЛИ({status_range};"*failed*")'
    )


def main():
    repair = os.environ.get('TOPUS_REPAIR', '').lower() in ('1', 'true', 'yes')
    client = authenticate_google_sheets()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = sheet.worksheet(config.SHEET_NAME_VIDEOS)

    display_headers = get_values_with_quota_retry(worksheet, '1:1')
    formula_headers = get_values_with_quota_retry(worksheet, '1:1', value_render_option='FORMULA')
    headers = [str(value).strip() for value in display_headers[0]] if display_headers else []
    formulas = [str(value).strip() for value in formula_headers[0]] if formula_headers else []
    status_index = find_column_index(headers, ['Системный статус'])
    if status_index is None:
        raise ValueError('Системный статус column not found')

    column = a1_column(status_index + 1)
    current = formulas[status_index] if status_index < len(formulas) else ''
    formula = status_summary_formula(column)
    print(f'Status column: {column}')
    print(f'Current header formula: {current}')
    print(f'Repair formula: {formula}')

    if repair:
        worksheet.update(
            range_name=gspread.utils.rowcol_to_a1(1, status_index + 1),
            values=[[formula]],
            value_input_option='USER_ENTERED',
        )
        print('Status header formula restored')


if __name__ == '__main__':
    main()
