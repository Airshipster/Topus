import json
from urllib.parse import urlencode

import gspread
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials

import config
from sheets import authenticate_google_sheets


def compact(value, limit=500):
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet("Сайт")

    display = worksheet.get("A1:G20", value_render_option="FORMATTED_VALUE")
    formulas = worksheet.get("A1:G20", value_render_option="FORMULA")
    tail = worksheet.get("A998:G1000", value_render_option="FORMATTED_VALUE")

    print("Site sheet diagnostics")
    print(f"Spreadsheet: {spreadsheet.title}")
    print(f"Worksheet: {worksheet.title}; id={worksheet.id}; rows={worksheet.row_count}; cols={worksheet.col_count}")
    print(f"A1000:G1000: {tail[-1] if tail else []}")
    print(f"A1:G8 display: {display[:8]}")

    errors = []
    for row_index, row in enumerate(display, start=1):
        for column_index, value in enumerate(row, start=1):
            if str(value or "").strip().startswith("#"):
                errors.append(f"{gspread.utils.rowcol_to_a1(row_index, column_index)}={value}")
    print(f"A1:G20 error cells: {errors or 'none'}")

    for row_index, row in enumerate(formulas, start=1):
        for column_index, formula in enumerate(row, start=1):
            if formula:
                print(f"formula {gspread.utils.rowcol_to_a1(row_index, column_index)}: {compact(formula, 900)}")

    credentials = Credentials.from_service_account_info(
        json.loads(config.SERVICE_ACCOUNT_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    session = AuthorizedSession(credentials)
    query = urlencode(
        {
            "ranges": "'Сайт'!A1:G20",
            "includeGridData": "true",
            "fields": "sheets(data(rowData(values(effectiveValue,formattedValue,note))))",
        }
    )
    response = session.get(f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet.id}?{query}", timeout=30)
    print(f"Detailed API status: {response.status_code}")
    if response.ok:
        grid = response.json().get("sheets", [{}])[0].get("data", [{}])[0].get("rowData", [])
        detailed = []
        for row_index, row in enumerate(grid, start=1):
            for column_index, cell in enumerate(row.get("values", []), start=1):
                error = cell.get("effectiveValue", {}).get("errorValue")
                if error:
                    detailed.append(
                        f"{gspread.utils.rowcol_to_a1(row_index, column_index)}="
                        f"{error.get('type', '')}: {compact(error.get('message', ''), 300)}"
                    )
        print(f"Detailed errors: {detailed or 'none'}")
    else:
        print(compact(response.text, 1000))

    source_id = tail[-1][0].strip() if tail and tail[-1] else ""
    if source_id:
        try:
            source = client.open_by_key(source_id)
            print(f"Source spreadsheet: {source.title}")
            source_ws = source.worksheet("Список. YouTube")
            source_values = source_ws.get("A1:Z60", value_render_option="FORMATTED_VALUE")
            print(f"Source worksheet rows={source_ws.row_count}; cols={source_ws.col_count}")
            print(f"Source first row: {source_values[0] if source_values else []}")
            marker_rows = [
                idx
                for idx, row in enumerate(source_values, start=1)
                if any("🔵" in str(cell or "") for cell in row)
            ]
            print(f"Blue marker rows in source A1:Z60: {marker_rows or 'none'}")
        except Exception as error:
            print(f"Source read failed: {type(error).__name__}: {compact(error, 1000)}")


if __name__ == "__main__":
    main()
