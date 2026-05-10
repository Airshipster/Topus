import json

import gspread
from google.oauth2.service_account import Credentials

import config


def main():
    credentials = Credentials.from_service_account_info(
        json.loads(config.SERVICE_ACCOUNT_JSON),
        scopes=['https://www.googleapis.com/auth/spreadsheets'],
    )
    sheet = gspread.authorize(credentials).open_by_key(config.SPREADSHEET_ID)
    sheet.batch_update({
        'requests': [{
            'findReplace': {
                'find': '","',
                'replacement': '";"',
                'allSheets': True,
                'matchCase': True,
            }
        }]
    })
    print('fixed hyperlink separators')


if __name__ == '__main__':
    main()
