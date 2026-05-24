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
  названия;FILTER(столбецНазваний;маска);
  ссылкиСырые;FILTER(INDEX(данные;;MATCH("/channel/";заголовки;0));маска);
  партнерыСырые;FILTER(INDEX(данные;;MATCH("3▼ Партнёр";заголовки;0));маска);
  видео;FILTER(INDEX(данные;;MATCH("Видео";заголовки;0));маска);
  последниеСырые;FILTER(INDEX(данные;;MATCH("Посл. вид.";заголовки;0));маска);
  созданСырые;FILTER(INDEX(данные;;MATCH("Создан";заголовки;0));маска);
  тг;FILTER(INDEX(данные;;MATCH("TG-каналы партнёров";заголовки;0));маска);
  ссылки;ARRAYFORMULA(IF(REGEXMATCH(ссылкиСырые;"https://www\\.");REGEXREPLACE(ссылкиСырые;"https://www\\.";"");ссылкиСырые));
  партнеры;ARRAYFORMULA(IF(IFERROR(FIND("🐙";партнерыСырые;1)>0;FALSE);"🐙";""));
  последние;ARRAYFORMULA(IF(ISNUMBER(последниеСырые);YEAR(последниеСырые);последниеСырые));
  создан;ARRAYFORMULA(IF(ISNUMBER(созданСырые);YEAR(созданСырые);созданСырые));
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
