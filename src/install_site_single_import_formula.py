import config
from sheets import authenticate_google_sheets


SOURCE_SPREADSHEET_ID = "1m67OLnwzOLCjnLCj_xZG_eT6R90yXwLWLXOxkTrjDuY"
BLANK_ROW = "\\".join(['""'] * 7)
UPDATED_ROW = "\\".join(["updated"] + ['""'] * 6)

FORMULA = f'''=LET(
  sourceId;"{SOURCE_SPREADSHEET_ID}";
  data;IMPORTRANGE(sourceId;"Список. YouTube!A1:W2100");
  updated;IMPORTRANGE(sourceId;"'Стат. Каналы'!A2");
  headers;INDEX(data;1;);
  rowCount;ROWS(data);
  nameCol;INDEX(data;;MATCH("Название";headers;0));
  startRow;MATCH("Название";nameCol;0)+1;
  endRow;MATCH("🔵";nameCol;0)-1;
  mask;(SEQUENCE(rowCount)>=startRow)*(SEQUENCE(rowCount)<=endRow);
  names;FILTER(nameCol;mask);
  rawLinks;FILTER(INDEX(data;;MATCH("/channel/";headers;0));mask);
  rawPartners;FILTER(INDEX(data;;MATCH("3▼ Партнёр";headers;0));mask);
  videos;FILTER(INDEX(data;;MATCH("Видео";headers;0));mask);
  rawLast;FILTER(INDEX(data;;MATCH("Посл. вид.";headers;0));mask);
  rawCreated;FILTER(INDEX(data;;MATCH("Создан";headers;0));mask);
  tg;FILTER(INDEX(data;;MATCH("TG-каналы партнёров";headers;0));mask);
  links;ARRAYFORMULA(IF(REGEXMATCH(rawLinks;"https://www\\.");REGEXREPLACE(rawLinks;"https://www\\.";"");rawLinks));
  partners;ARRAYFORMULA(IF(IFERROR(FIND("🐙";rawPartners;1)>0;FALSE);"🐙";""));
  lastYears;ARRAYFORMULA(IF(ISNUMBER(rawLast);YEAR(rawLast);rawLast));
  createdYears;ARRAYFORMULA(IF(ISNUMBER(rawCreated);YEAR(rawCreated);rawCreated));
  {{
    "Название"&CHAR(10)&"проекта"\\"Ссылка "&CHAR(10)&"на канал"\\"Партнёр "&CHAR(10)&"SciTopus"\\"Кол."&CHAR(10)&" видео"\\"Год послед."&CHAR(10)&" видео"\\"Год создания"&CHAR(10)&" канала"\\"TG-каналы"&CHAR(10)&" партнёров";
    names\\links\\partners\\videos\\lastYears\\createdYears\\tg;
    {BLANK_ROW};
    {BLANK_ROW};
    {BLANK_ROW};
    {UPDATED_ROW}
  }}
)'''


def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet("Сайт")
    worksheet.batch_clear(["A1:G1000"])
    worksheet.update("A1", [[FORMULA]], value_input_option="USER_ENTERED")
    print("Installed compact live import formula in Сайт!A1 and cleared A1:G1000")


if __name__ == "__main__":
    main()
