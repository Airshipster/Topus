from __future__ import annotations

import config
from sheets import authenticate_google_sheets, clean_sheet_value, get_values_with_quota_retry


TARGET_SHEET_NAME = "Сайт"
SOURCE_SHEET_NAME = "Список. YouTube"
SOURCE_STATS_SHEET_NAME = "Стат. Каналы"
TARGET_ROWS_BEFORE_META = 999
TARGET_COLUMNS = 7

HEADER = [
    "Название\nпроекта",
    "Ссылка \nна канал",
    "Партнёр \nSciTopus",
    "Кол.\n видео",
    "Год послед.\n видео",
    "Год создания\n канала",
    "TG-каналы\n партнёров",
]


def normalize_cell(value):
    return str(clean_sheet_value(value) or "").strip()


def column_index(headers, label):
    for index, value in enumerate(headers):
        if normalize_cell(value) == label:
            return index
    raise RuntimeError(f"Source header not found: {label}")


def extract_column(values, headers, label):
    index = column_index(headers, label)
    column = [normalize_cell(row[index]) if index < len(row) else "" for row in values]
    try:
        start = column.index(label) + 1
    except ValueError as error:
        raise RuntimeError(f"Source column start marker not found: {label}") from error

    end = len(column)
    for row_index, value in enumerate(column):
        if value == "🔵":
            end = row_index
            break
    return column[start:end]


def normalize_youtube_url(value):
    value = normalize_cell(value)
    for prefix in ("https://www.", "http://www.", "https://", "http://"):
        if value.startswith(prefix):
            value = value[len(prefix):]
            break
    return value.strip().rstrip("/")


def partner_mark(value):
    return "🐙" if "🐙" in normalize_cell(value) else ""


def year_value(value):
    return normalize_cell(value)


def build_site_rows(source_values, channel_folder):
    if not source_values:
        raise RuntimeError("Source sheet is empty")

    headers = source_values[0]
    columns = [
        extract_column(source_values, headers, "Название") + ["", "", "", normalize_cell(channel_folder)],
        [normalize_youtube_url(value) for value in extract_column(source_values, headers, "/channel/")],
        [partner_mark(value) for value in extract_column(source_values, headers, "3▼ Партнёр")],
        [normalize_cell(value) for value in extract_column(source_values, headers, "Видео")],
        [year_value(value) for value in extract_column(source_values, headers, "Посл. вид.")],
        [year_value(value) for value in extract_column(source_values, headers, "Создан")],
        [normalize_cell(value) for value in extract_column(source_values, headers, "TG-каналы партнёров")],
    ]

    max_rows = max(len(column) for column in columns) + 1
    if max_rows > TARGET_ROWS_BEFORE_META:
        raise RuntimeError(f"Site sheet output has {max_rows} rows; target supports {TARGET_ROWS_BEFORE_META}")

    rows = [HEADER]
    for row_index in range(max_rows - 1):
        rows.append([
            columns[column_index][row_index] if row_index < len(columns[column_index]) else ""
            for column_index in range(TARGET_COLUMNS)
        ])

    while len(rows) < TARGET_ROWS_BEFORE_META:
        rows.append([""] * TARGET_COLUMNS)
    return rows


def main():
    client = authenticate_google_sheets()
    master = client.open_by_key(config.SPREADSHEET_ID)
    target = master.worksheet(TARGET_SHEET_NAME)

    meta_row = target.get("A1000:G1000", value_render_option="FORMATTED_VALUE")
    source_id = normalize_cell(meta_row[0][0]) if meta_row and meta_row[0] else ""
    if not source_id:
        raise RuntimeError("Source spreadsheet id is missing in Сайт!A1000")

    source = client.open_by_key(source_id)
    source_values = get_values_with_quota_retry(source.worksheet(SOURCE_SHEET_NAME), "A:Z", attempts=5)
    channel_folder = ""
    try:
        stats_values = get_values_with_quota_retry(source.worksheet(SOURCE_STATS_SHEET_NAME), "A2:A2", attempts=3)
        channel_folder = stats_values[0][0] if stats_values and stats_values[0] else ""
    except Exception as error:
        print(f"  ⚠️  Could not read {SOURCE_STATS_SHEET_NAME}!A2: {type(error).__name__}: {error}")

    rows = build_site_rows(source_values, channel_folder)
    target.update(f"A1:G{TARGET_ROWS_BEFORE_META}", rows, value_input_option="USER_ENTERED")
    print(f"Updated {TARGET_SHEET_NAME}: {len(rows)} rows from {source.title}/{SOURCE_SHEET_NAME}")


if __name__ == "__main__":
    main()
