import config
from sheets import authenticate_google_sheets
import re


SPREADSHEET_ID_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9-_]+)|^([a-zA-Z0-9-_]{30,})$")


def extract_spreadsheet_id(value):
    value = str(value or "").strip()
    match = SPREADSHEET_ID_RE.search(value)
    if not match:
        return ""
    return match.group(1) or match.group(2)


def main():
    client = authenticate_google_sheets()
    master = client.open_by_key(config.SPREADSHEET_ID)
    settings = master.worksheet("Настройки")
    settings_values = settings.get("A1:Z6", value_render_option="FORMATTED_VALUE")
    print(f"Settings A1:Z6: {settings_values}")
    first_project = settings_values[1] if len(settings_values) > 1 else []
    project_id = ""
    for cell in first_project:
        project_id = extract_spreadsheet_id(cell)
        if project_id:
            break
    if not project_id:
        raise RuntimeError("Could not find first project spreadsheet ID in Настройки row 2")
    spreadsheet = client.open_by_key(project_id)
    print(f"Project spreadsheet: {spreadsheet.title}")
    print(f"Worksheets: {[sheet.title for sheet in spreadsheet.worksheets()]}")
    worksheet = next(sheet for sheet in spreadsheet.worksheets() if "YouTube" in sheet.title or "Список" in sheet.title)
    print(f"Selected worksheet: {worksheet.title}")
    formulas = worksheet.get("K1:O1", value_render_option="FORMULA")
    values = worksheet.get("K1:O3", value_render_option="FORMATTED_VALUE")
    print(f"K1:O1 formulas: {formulas}")
    print(f"K1:O3 values: {values}")


if __name__ == "__main__":
    main()
