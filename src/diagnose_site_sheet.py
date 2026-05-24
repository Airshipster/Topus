import config
from sheets import authenticate_google_sheets


def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet("Сайт")
    values = worksheet.get("A1:G12", value_render_option="FORMATTED_VALUE")
    formulas = worksheet.get("A1:G1", value_render_option="FORMULA")
    print(f"A1:G12: {values}")
    print(f"A1:G1 formulas: {formulas}")


if __name__ == "__main__":
    main()
