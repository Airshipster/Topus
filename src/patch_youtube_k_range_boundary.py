import re

import config
from sheets import authenticate_google_sheets


SPREADSHEET_ID_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9-_]+)|^([a-zA-Z0-9-_]{30,})$")
IDS_RANGE_RE = re.compile(r"ids\s*;\s*E3:E\d+\s*;", re.IGNORECASE)


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
    headers = settings.get("A1:Z1", value_render_option="FORMATTED_VALUE")[0]
    first_project = settings.get("A2:Z2", value_render_option="FORMATTED_VALUE")[0]

    project_id = ""
    worksheet_name = "Список. YouTube"
    for index, header in enumerate(headers):
        header = str(header).strip()
        if header == "Название листа" and index < len(first_project) and first_project[index]:
            worksheet_name = first_project[index]
        if header == "Ссылка на документ проекта" and index < len(first_project):
            project_id = extract_spreadsheet_id(first_project[index])
    if not project_id:
        raise RuntimeError("Could not find first project spreadsheet ID")

    spreadsheet = client.open_by_key(project_id)
    worksheet = spreadsheet.worksheet(worksheet_name)
    formula_rows = worksheet.get("K1", value_render_option="FORMULA")
    if not formula_rows or not formula_rows[0] or not formula_rows[0][0]:
        raise RuntimeError("K1 formula is empty")

    formula = formula_rows[0][0]
    replacement = (
        'blueRow; IFERROR(MATCH("*🔵*"; E3:E; 0)+2; MATCH(2; 1/(E:E<>""))+2);\n'
        '  ids; E3:INDEX(E:E; blueRow-2);'
    )
    updated = IDS_RANGE_RE.sub(replacement, formula, count=1)
    if updated == formula:
        raise RuntimeError(f"Could not find fixed ids range in K1 formula: {formula[:300]}")

    worksheet.update("K1", [[updated]], value_input_option="USER_ENTERED")
    print(f"Patched K1 ids range boundary in {spreadsheet.title} / {worksheet_name}")


if __name__ == "__main__":
    main()
