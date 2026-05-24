import config
from sheets import authenticate_google_sheets

SOURCE_SPREADSHEET_ID = "1m67OLnwzOLCjnLCj_xZG_eT6R90yXwLWLXOxkTrjDuY"


def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet("Сайт")
    values = worksheet.get("A1:G12", value_render_option="FORMATTED_VALUE")
    formulas = worksheet.get("A1:G1", value_render_option="FORMULA")
    print(f"A1:G12: {values}")
    print(f"A1:G1 formulas: {formulas}")
    metadata = spreadsheet.fetch_sheet_metadata(params={
        "includeGridData": "true",
        "ranges": "Сайт!A1:G12",
    })
    rows = metadata["sheets"][0].get("data", [{}])[0].get("rowData", [])
    for row_index, row in enumerate(rows[:12], start=1):
        cells = row.get("values", [])
        for col_index, cell in enumerate(cells[:7], start=1):
            effective = cell.get("effectiveValue", {})
            if "errorValue" in effective:
                print(f"Error R{row_index}C{col_index}: {effective['errorValue']}")
    source = client.open_by_key(SOURCE_SPREADSHEET_ID)
    source_list = source.worksheet("Список. YouTube").get("A1:W12")
    source_update = source.worksheet("Стат. Каналы").get("A2")
    print(f"Source direct A1:W12 rows: {len(source_list)}")
    print(f"Source direct first row: {source_list[0] if source_list else []}")
    print(f"Source direct Стат. Каналы!A2: {source_update}")


if __name__ == "__main__":
    main()
