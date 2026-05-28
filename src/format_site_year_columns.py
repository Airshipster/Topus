import config
from sheets import authenticate_google_sheets


def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet("Сайт")
    worksheet.format("E:F", {"numberFormat": {"type": "NUMBER", "pattern": "0"}})
    print("Formatted Сайт!E:F as plain numeric years")


if __name__ == "__main__":
    main()
