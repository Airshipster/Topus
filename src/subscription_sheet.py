"""Keep the subscription view consistent with confirmed inventory and status."""
import json
import os
from pathlib import Path
import time


def obsolete_rows(values, leases, channels, complete):
    if not complete or not channels or not values:
        return []
    headers = [str(v).splitlines()[0].strip() for v in values[0]]
    column = headers.index('Channel ID')
    return [n for n, row in enumerate(values[1:], 2)
            if len(row) > column and row[column] in leases
            and not leases[row[column]]['enabled'] and row[column] not in channels]


def reconcile_view(sheet, worksheet, values, leases, channels, complete):
    removed = obsolete_rows(values, leases, channels, complete)
    if removed:
        backup = Path(os.environ.get('TOPUS_SHEET_BACKUP_DIR', '/data/sheet-backups'))
        backup.mkdir(mode=0o700, parents=True, exist_ok=True)
        target = backup / ('subscriptions-' + str(time.time_ns()) + '.json')
        with target.open('x', encoding='utf-8') as saved:
            json.dump({'sheet_id': worksheet.id, 'rows': values, 'removed': removed}, saved)
        os.chmod(target, 0o600)
        sheet.batch_update({'requests': [
            {'deleteDimension': {'range': {'sheetId': worksheet.id, 'dimension': 'ROWS',
                                          'startIndex': n - 1, 'endIndex': n}}}
            for n in reversed(removed)]})
        values = worksheet.get_all_values()
        print('Removed disabled subscription rows: ' + str(len(removed)), flush=True)
    return values


def clear_legacy_error_fill(sheet, worksheet, column, row_count):
    import gspread
    from channel_availability import unavailable
    missing = unavailable()
    start = gspread.utils.rowcol_to_a1(2, column + 1)
    end = gspread.utils.rowcol_to_a1(max(2, row_count), column + 1)
    metadata = sheet.fetch_sheet_metadata(params={
        'ranges': ["'" + worksheet.title.replace("'", "''") + "'!" + start + ':' + end],
        'includeGridData': 'true',
        'fields': 'sheets(data(startRow,rowData(values(formattedValue,userEnteredFormat))))'})
    requests = []
    for tab in metadata.get('sheets', []):
        for grid in tab.get('data', []):
            for row, data in enumerate(grid.get('rowData', []), grid.get('startRow', 1)):
                for cell in data.get('values', []):
                    color = cell.get('userEnteredFormat', {}).get('backgroundColor', {})
                    pink = all(abs(color.get(k, 0) - v) < .001 for k, v in
                               (('red', 1), ('green', .8), ('blue', .8)))
                    desired = cell.get('formattedValue') in missing
                    if pink != desired:
                        requests.append({'repeatCell': {
                            'range': {'sheetId': worksheet.id, 'startRowIndex': row,
                                      'endRowIndex': row + 1, 'startColumnIndex': column,
                                      'endColumnIndex': column + 1},
                            'cell': {'userEnteredFormat': {'backgroundColor': {'red': 1, 'green': .8, 'blue': .8}} if desired else {}},
                            'fields': 'userEnteredFormat.backgroundColor,userEnteredFormat.backgroundColorStyle'}})
    if requests:
        sheet.batch_update({'requests': requests})
        print('Updated channel availability fills: ' + str(len(requests)), flush=True)
