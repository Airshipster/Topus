from collections import Counter, defaultdict

import config
from sheets import (
    authenticate_google_sheets,
    clean_sheet_value,
    extract_sheet_id,
    get_values_with_quota_retry,
)
from subscriptions import SUBSCRIPTIONS_SHEET_NAME, normalize_subscription_channel_id


def base_header(value):
    return str(value or '').split('\n', 1)[0].strip()


def header_indexes(headers):
    return {base_header(header): index for index, header in enumerate(headers)}


def row_value(row, indexes, name):
    index = indexes.get(name)
    if index is None or index >= len(row):
        return ''
    return str(clean_sheet_value(row[index]) or '').strip()


def is_green_status(status):
    return '✅' in str(status or '')


def compact(value, limit=140):
    text = ' '.join(str(value or '').split())
    if len(text) <= limit:
        return text
    return text[:limit - 3] + '...'


def inspect_subscriptions(sheet):
    worksheet = sheet.worksheet(SUBSCRIPTIONS_SHEET_NAME)
    values = get_values_with_quota_retry(worksheet)
    if not values:
        print('Subscriptions sheet is empty')
        return

    indexes = header_indexes(values[0])
    status_counts = Counter()
    problem_rows = []

    for row_number, row in enumerate(values[1:], start=2):
        channel_id = normalize_subscription_channel_id(row_value(row, indexes, 'Channel ID'))
        status = row_value(row, indexes, 'Status')
        if not channel_id and not status:
            continue

        status_key = status or '(empty)'
        status_counts[status_key] += 1
        if not is_green_status(status):
            problem_rows.append({
                'row': row_number,
                'projects': row_value(row, indexes, 'Projects'),
                'project_count': row_value(row, indexes, 'Project Count'),
                'channel_id': channel_id or row_value(row, indexes, 'Channel ID'),
                'last_renewed': row_value(row, indexes, 'Last Renewed'),
                'status': status or '(empty)',
            })

    print(f'Subscriptions rows with non-green status: {len(problem_rows)}')
    print('Status summary:')
    for status, count in status_counts.most_common():
        print(f'  {count:>4}  {compact(status)}')

    if problem_rows:
        print('\nNon-green subscription rows:')
        for item in problem_rows:
            print(
                f"  row {item['row']}: {compact(item['status'], 180)} | "
                f"channel={item['channel_id']} | projects={compact(item['projects'], 90)} | "
                f"project_count={item['project_count']} | last_renewed={item['last_renewed']}"
            )

    grouped = defaultdict(int)
    for item in problem_rows:
        grouped[item['status']] += 1
    if grouped:
        print('\nNon-green status groups:')
        for status, count in sorted(grouped.items(), key=lambda pair: (-pair[1], pair[0])):
            print(f'  {count:>4}  {compact(status, 180)}')


def inspect_project_statuses(sheet):
    worksheet = sheet.worksheet(config.SHEET_NAME_PROJECTS)
    values = get_values_with_quota_retry(worksheet, config.PROJECTS_READ_RANGE if hasattr(config, 'PROJECTS_READ_RANGE') else None)
    if not values:
        print('Projects/settings sheet is empty')
        return

    indexes = header_indexes(values[0])
    problem_rows = []
    row_15 = None

    for row_number, row in enumerate(values[1:], start=2):
        if any(str(cell).strip() == 'Настройки' for cell in row):
            break
        if not any(str(cell).strip() for cell in row):
            continue

        status = row_value(row, indexes, 'Provisioning status')
        error = row_value(row, indexes, 'Provisioning error')
        active = row_value(row, indexes, 'Активен')
        name = (
            row_value(row, indexes, 'Название')
            or row_value(row, indexes, 'Проект')
            or row_value(row, indexes, 'Name')
        )
        code = row_value(row, indexes, 'Код проекта') or row_value(row, indexes, 'Код') or row_value(row, indexes, 'Code')
        sheet_url = (
            row_value(row, indexes, 'Ссылка на документ проекта')
            or row_value(row, indexes, 'Ссылка на таблицу')
            or row_value(row, indexes, 'Google Sheet')
        )
        sheet_id = extract_sheet_id(sheet_url) if sheet_url else ''

        item = {
            'row': row_number,
            'active': active,
            'name': name,
            'code': code,
            'status': status,
            'error': error,
            'sheet_id': sheet_id,
        }

        if row_number == 15:
            row_15 = item

        status_ok = status in ('', 'ready', 'ok', '✅ ready')
        if error or not status_ok:
            problem_rows.append(item)

    if row_15:
        print('\nProject row 15:')
        print(
            f"  active={row_15['active']} | name={compact(row_15['name'], 90)} | "
            f"code={row_15['code']} | status={compact(row_15['status'])} | "
            f"error={compact(row_15['error'], 180)} | sheet_id={row_15['sheet_id']}"
        )

    print(f'\nProject rows with provisioning status/error to inspect: {len(problem_rows)}')
    for item in problem_rows:
        print(
            f"  row {item['row']}: active={item['active']} | name={compact(item['name'], 90)} | "
            f"code={item['code']} | status={compact(item['status'])} | "
            f"error={compact(item['error'], 180)} | sheet_id={item['sheet_id']}"
        )


def main():
    client = authenticate_google_sheets()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    inspect_subscriptions(sheet)
    inspect_project_statuses(sheet)


if __name__ == '__main__':
    main()
