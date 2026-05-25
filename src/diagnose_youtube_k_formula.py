import config
from sheets import authenticate_google_sheets


def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet("Список. YouTube")
    formulas = worksheet.get("K1:O1", value_render_option="FORMULA")
    values = worksheet.get("K1:O6", value_render_option="FORMATTED_VALUE")
    markers = worksheet.get("K:O", value_render_option="FORMATTED_VALUE")
    first_marker = None
    for index, row in enumerate(markers, start=1):
        text = " ".join(str(cell) for cell in row)
        if "🔵" in text or "𐍈" in text:
            first_marker = index
            break
    print(f"K1:O1 formulas: {formulas}")
    print(f"K1:O6 values: {values}")
    print(f"First marker row in K:O: {first_marker}")


if __name__ == "__main__":
    main()
