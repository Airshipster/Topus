import re

import config
from sheets import authenticate_google_sheets


SPREADSHEET_ID_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9-_]+)|^([a-zA-Z0-9-_]{30,})$")


def extract_spreadsheet_id(value):
    value = str(value or "").strip()
    match = SPREADSHEET_ID_RE.search(value)
    if not match:
        return ""
    return match.group(1) or match.group(2)

SEP = "\\"
HEADER_ARRAY = "{" + SEP.join(['"Посл. вид."', '"Просмотры"', '"Подписчики 1▼"', '"Видео"', '"Создан"']) + "}"
BLANK_ARRAY = "{" + SEP.join(['""'] * 5) + "}"
STAT_ARRAY = "{" + SEP.join([
    "INDEX('Стат. Каналы'!L:L; rowNumber)",
    "INDEX('Стат. Каналы'!F:F; rowNumber)",
    "INDEX('Стат. Каналы'!E:E; rowNumber)",
    "INDEX('Стат. Каналы'!D:D; rowNumber)",
    "INDEX('Стат. Каналы'!K:K; rowNumber)",
]) + "}"

FORMULA = f'''=LET(
  markerRow; IFERROR(MIN(FILTER(ROW(E3:M); BYROW(E3:M; LAMBDA(row; REGEXMATCH(TEXTJOIN(""; TRUE; row); "𐍈|🔵")))); MATCH(2; 1/(E:E<>""))+1);
  ids; E3:INDEX(E:E; markerRow-1);
  keys; 'Стат. Каналы'!M:M;
  VSTACK(
    {HEADER_ARRAY};
    {BLANK_ARRAY};
    MAP(ids; LAMBDA(id;
      IF(id=""; {BLANK_ARRAY};
        IFERROR(
          LET(
            rowNumber; MATCH(id; keys; 0);
            {STAT_ARRAY}
          );
          {BLANK_ARRAY}
        )
      )
    ))
  )
)'''


def main():
    client = authenticate_google_sheets()
    master = client.open_by_key(config.SPREADSHEET_ID)
    settings = master.worksheet("Настройки")
    first_project = settings.get("A2:Z2", value_render_option="FORMATTED_VALUE")[0]
    project_id = ""
    worksheet_name = "Список. YouTube"
    headers = settings.get("A1:Z1", value_render_option="FORMATTED_VALUE")[0]
    for index, header in enumerate(headers):
        if str(header).strip() == "Название листа" and index < len(first_project) and first_project[index]:
            worksheet_name = first_project[index]
        if str(header).strip() == "Ссылка на документ проекта" and index < len(first_project):
            project_id = extract_spreadsheet_id(first_project[index])
    if not project_id:
        raise RuntimeError("Could not find first project spreadsheet ID")

    spreadsheet = client.open_by_key(project_id)
    worksheet = spreadsheet.worksheet(worksheet_name)
    worksheet.update("K1", [[FORMULA]], value_input_option="USER_ENTERED")
    print(f"Installed dynamic K1 formula in {spreadsheet.title} / {worksheet_name}")


if __name__ == "__main__":
    main()
