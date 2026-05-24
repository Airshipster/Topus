import config
from sheets import authenticate_google_sheets


SOURCE_SPREADSHEET_ID = "1m67OLnwzOLCjnLCj_xZG_eT6R90yXwLWLXOxkTrjDuY"

FORMULA = f'''=LET(
  ID;"{SOURCE_SPREADSHEET_ID}";
  данные;IMPORTRANGE(ID;"Список. YouTube!A1:W2100");
  заголовки;INDEX(данные;1;);
  количество;ROWS(данные);
  столбецНазваний;INDEX(данные;;MATCH("Название";заголовки;0));
  начало;MATCH("Название";столбецНазваний;0)+1;
  конец;MATCH("🔵";столбецНазваний;0)-1;
  маска;(SEQUENCE(количество)>=начало)*(SEQUENCE(количество)<=конец);
  взять;LAMBDA(метка;FILTER(INDEX(данные;;MATCH(метка;заголовки;0));маска));
  названия;взять("Название");
  ссылки;ARRAYFORMULA(IF(REGEXMATCH(взять("/channel/");"https://www\\.");REGEXREPLACE(взять("/channel/");"https://www\\.";"");взять("/channel/")));
  партнеры;ARRAYFORMULA(IF(IFERROR(FIND("🐙";взять("3▼ Партнёр");1)>0;FALSE);"🐙";""));
  видео;взять("Видео");
  последние;ARRAYFORMULA(IF(ISNUMBER(взять("Посл. вид."));YEAR(взять("Посл. вид."));взять("Посл. вид.")));
  создан;ARRAYFORMULA(IF(ISNUMBER(взять("Создан"));YEAR(взять("Создан"));взять("Создан")));
  тг;взять("TG-каналы партнёров");
  VSTACK(
    {{"Название"&CHAR(10)&"проекта"\\"Ссылка "&CHAR(10)&"на канал"\\"Партнёр "&CHAR(10)&"SciTopus"\\"Кол."&CHAR(10)&" видео"\\"Год послед."&CHAR(10)&" видео"\\"Год создания"&CHAR(10)&" канала"\\"TG-каналы"&CHAR(10)&" партнёров"}};
    HSTACK(названия;ссылки;партнеры;видео;последние;создан;тг)
  )
)'''


def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(config.SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet("Сайт")
    worksheet.batch_clear(["A1:G1000"])
    worksheet.update("A1", [[FORMULA]], value_input_option="USER_ENTERED")
    print("Installed single IMPORTRANGE formula in Сайт!A1 and cleared A1:G1000")


if __name__ == "__main__":
    main()
