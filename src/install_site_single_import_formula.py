import config
from sheets import authenticate_google_sheets


SOURCE_SPREADSHEET_ID = "1m67OLnwzOLCjnLCj_xZG_eT6R90yXwLWLXOxkTrjDuY"


def formula_for_column(label: str, header: str, transform: str | None = None, append_timestamp: bool = False) -> str:
    value_expr = "filtered"
    if transform == "youtube_url":
        value_expr = 'ARRAYFORMULA(IF(REGEXMATCH(filtered;"https://www\\\\.");REGEXREPLACE(filtered;"https://www\\\\.";"");filtered))'
    elif transform == "partner":
        value_expr = 'ARRAYFORMULA(IF(ARRAYFORMULA(IFERROR(FIND("🐙";filtered;1)>0;FALSE));"🐙";""))'
    elif transform == "year":
        value_expr = "ARRAYFORMULA(IF(ISNUMBER(filtered);YEAR(filtered);filtered))"

    tail = ""
    if append_timestamp:
        tail = f';"";"";"";IMPORTRANGE("{SOURCE_SPREADSHEET_ID}";"\'Стат. Каналы\'!A2")'

    return (
        f'=LET(sourceId;"{SOURCE_SPREADSHEET_ID}";'
        f'data;IMPORTRANGE(sourceId;"Список. YouTube!A1:W2100");'
        f'headers;INDEX(data;1;);'
        f'label;"{label}";'
        f'position;MATCH(label;headers;0);'
        f'column;INDEX(data;;position);'
        f'rowCount;ROWS(data);'
        f'startRow;MATCH(label;column;0)+1;'
        f'endRow;MATCH("🔵";column;0)-1;'
        f'filtered;FILTER(column;(SEQUENCE(rowCount)>=startRow)*(SEQUENCE(rowCount)<=endRow));'
        f'VSTACK("{header}";{value_expr}{tail}))'
    )


FORMULAS = [[
    formula_for_column("Название", "Название" + chr(10) + "проекта", append_timestamp=True),
    formula_for_column("/channel/", "Ссылка " + chr(10) + "на канал", "youtube_url"),
    formula_for_column("3▼ Партнёр", "Партнёр " + chr(10) + "SciTopus", "partner"),
    formula_for_column("Видео", "Кол." + chr(10) + " видео"),
    formula_for_column("Посл. вид.", "Год послед." + chr(10) + " видео", "year"),
    formula_for_column("Создан", "Год создания" + chr(10) + " канала", "year"),
    formula_for_column("TG-каналы партнёров", "TG-каналы" + chr(10) + " партнёров"),
]]


def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet("Сайт")
    worksheet.batch_clear(["A1:G1000", "A1000"])
    worksheet.update("A1:G1", FORMULAS, value_input_option="USER_ENTERED")
    print("Installed live Сайт formulas with embedded source ID and cleared A1000")


if __name__ == "__main__":
    main()
