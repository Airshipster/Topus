import config
from sheets import authenticate_google_sheets


SOURCE_SPREADSHEET_ID = "1m67OLnwzOLCjnLCj_xZG_eT6R90yXwLWLXOxkTrjDuY"
BLANK_ROW = "\\".join(['""'] * 8)
UPDATED_ROW = "\\".join(["updated"] + ['""'] * 7)

FORMULA = f'''=LET(
  sourceId;"{SOURCE_SPREADSHEET_ID}";
  data;IMPORTRANGE(sourceId;"Список. YouTube!A:W");
  updated;IMPORTRANGE(sourceId;"'Стат. Каналы'!A2");
  headers;INDEX(data;1;);
  rowCount;ROWS(data);
  nameCol;INDEX(data;;MATCH("Название";headers;0));
  startRow;MATCH("Название";nameCol;0)+1;
  endRow;MATCH("🔵";nameCol;0)-1;
  rowNums;SEQUENCE(rowCount);
  names;FILTER(nameCol;rowNums>=startRow;rowNums<=endRow);
  rawLinks;FILTER(INDEX(data;;MATCH("/channel/";headers;0));rowNums>=startRow;rowNums<=endRow);
  rawPartners;FILTER(INDEX(data;;MATCH("3▼ Партнёр";headers;0));rowNums>=startRow;rowNums<=endRow);
  videos;FILTER(INDEX(data;;MATCH("Видео";headers;0));rowNums>=startRow;rowNums<=endRow);
  rawLast;FILTER(INDEX(data;;MATCH("Посл. вид.";headers;0));rowNums>=startRow;rowNums<=endRow);
  rawCreated;FILTER(INDEX(data;;MATCH("Создан";headers;0));rowNums>=startRow;rowNums<=endRow);
  tg;FILTER(INDEX(data;;MATCH("TG-каналы партнёров";headers;0));rowNums>=startRow;rowNums<=endRow);
  links;ARRAYFORMULA(IF(REGEXMATCH(rawLinks;"https://www\\.");REGEXREPLACE(rawLinks;"https://www\\.";"");rawLinks));
  partners;ARRAYFORMULA(IF(IFERROR(FIND("🐙";rawPartners;1)>0;FALSE);"🐙";""));
  lastYears;ARRAYFORMULA(IF(ISNUMBER(rawLast);YEAR(rawLast);rawLast));
  lastDates;ARRAYFORMULA(IF(ISNUMBER(rawLast);TEXT(rawLast;"yyyy-mm-dd");rawLast));
  createdYears;ARRAYFORMULA(IF(ISNUMBER(rawCreated);YEAR(rawCreated);rawCreated));
  cleanTg;ARRAYFORMULA(IF(REGEXMATCH(TO_TEXT(tg);"^\\s*-");"";tg));
  {{
    "Название"&CHAR(10)&"проекта"\\"Ссылка "&CHAR(10)&"на канал"\\"Партнёр "&CHAR(10)&"SciTopus"\\"Кол."&CHAR(10)&" видео"\\"Год послед."&CHAR(10)&" видео"\\"Год создания"&CHAR(10)&" канала"\\"TG-каналы"&CHAR(10)&" партнёров"\\"Дата послед."&CHAR(10)&" видео";
    names\\links\\partners\\videos\\lastYears\\createdYears\\cleanTg\\lastDates;
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
    worksheet.batch_clear(["A1:H1000", "A1000"])
    worksheet.update("A1", [[FORMULA]], value_input_option="USER_ENTERED")
    print("Installed single live Сайт formula with dynamic blue-marker boundary")


if __name__ == "__main__":
    main()
