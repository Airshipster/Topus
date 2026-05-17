function repairTopusWorkbookMaintenance() {
  var ss = SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
  ensureWorkbookRows_(ss);
  repairPushEventsLayout_(ss);
  moveSubscriptionProjectColumns_(ss);
  repairKnownDateColumns_(ss);
  repairKnownNumericColumns_(ss);
}

function repairTopusFastLayout() {
  var ss = SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
  ensureWorkbookRows_(ss);
  repairPushEventsLayout_(ss);
  moveSubscriptionProjectColumns_(ss);
}

function ensureWorkbookRows_(ss) {
  ss.getSheets().forEach(function(sheet) {
    if (sheet.getName() === 'Настройки') {
      return;
    }
    ensureSheetRows_(sheet, 10000);
  });
}

function repairPushEventsLayout_(ss) {
  var sheet = ss.getSheetByName(PUSH_EVENTS_SHEET_NAME);
  if (!sheet) {
    return;
  }
  ensureSheetRows_(sheet, 10000);
  sheet.getRange(1, 1, Math.max(sheet.getMaxRows(), 1), PUSH_EVENTS_HEADERS.length)
    .setWrapStrategy(SpreadsheetApp.WrapStrategy.CLIP);
  if (sheet.getMaxRows() > 1) {
    sheet.setRowHeightsForced(2, sheet.getMaxRows() - 1, 21);
  }
}

function moveSubscriptionProjectColumns_(ss) {
  var sheet = ss.getSheetByName('Подписки');
  if (!sheet) {
    return;
  }
  var desiredHeaders = ['Projects', 'Project Count', 'Channel ID', 'Subscribed At', 'Last Renewed'];
  var headers = sheet.getRange(1, 1, 1, Math.max(sheet.getLastColumn(), desiredHeaders.length)).getValues()[0]
    .map(function(value) { return String(value || '').trim(); });
  if (headers.slice(0, desiredHeaders.length).join('\u0001') === desiredHeaders.join('\u0001')) {
    return;
  }

  var projectsCol = headers.indexOf('Projects') + 1;
  var countCol = headers.indexOf('Project Count') + 1;
  if (projectsCol > 0 && countCol === projectsCol + 1) {
    sheet.moveColumns(sheet.getRange(1, projectsCol, sheet.getMaxRows(), 2), 1);
  }
  sheet.getRange(1, 1, 1, desiredHeaders.length).setValues([desiredHeaders]);
}

function repairKnownDateColumns_(ss) {
  [
    {sheet: 'Настройки', headers: ['Provisioned at']},
    {sheet: 'Глобальные видео', headers: ['Дата публикации TG GMT+4']},
    {sheet: PUSH_EVENTS_SHEET_NAME, headers: ['Timestamp GMT+4']},
    {sheet: 'Подписки', headers: ['Subscribed At', 'Last Renewed']}
  ].forEach(function(target) {
    var sheet = ss.getSheetByName(target.sheet);
    if (!sheet) {
      return;
    }
    target.headers.forEach(function(header) {
      repairDateColumnByHeader_(sheet, header);
    });
  });
}

function repairDateColumnByHeader_(sheet, headerName) {
  var lastColumn = sheet.getLastColumn();
  var lastRow = sheet.getLastRow();
  if (lastColumn < 1 || lastRow < 2) {
    return;
  }
  var headers = sheet.getRange(1, 1, 1, lastColumn).getValues()[0];
  var column = headers.indexOf(headerName) + 1;
  if (column < 1) {
    return;
  }

  var range = sheet.getRange(2, column, lastRow - 1, 1);
  var values = range.getValues();
  var changed = false;
  var repaired = values.map(function(row) {
    var parsed = parseSheetDateValue_(row[0]);
    if (parsed) {
      changed = true;
      return [parsed];
    }
    return row;
  });

  if (changed) {
    range.setValues(repaired);
    range.setNumberFormat('yyyy-mm-dd hh:mm:ss');
  }
}

function repairKnownNumericColumns_(ss) {
  [
    {sheet: 'Глобальные видео', headers: ['TG message_id', 'Разница в минутах']}
  ].forEach(function(target) {
    var sheet = ss.getSheetByName(target.sheet);
    if (!sheet) {
      return;
    }
    target.headers.forEach(function(header) {
      repairNumericColumnByHeader_(sheet, header);
    });
  });
}

function repairNumericColumnByHeader_(sheet, headerName) {
  var lastColumn = sheet.getLastColumn();
  var lastRow = sheet.getLastRow();
  if (lastColumn < 1 || lastRow < 2) {
    return;
  }
  var headers = sheet.getRange(1, 1, 1, lastColumn).getValues()[0];
  var column = headers.indexOf(headerName) + 1;
  if (column < 1) {
    return;
  }

  var range = sheet.getRange(2, column, lastRow - 1, 1);
  var values = range.getValues();
  var changed = false;
  var repaired = values.map(function(row) {
    var parsed = parseSheetNumericValue_(row[0]);
    if (parsed !== null) {
      changed = true;
      return [parsed];
    }
    return row;
  });

  if (changed) {
    range.setValues(repaired);
    range.setNumberFormat('0');
  }
}

function parseSheetDateValue_(value) {
  if (!value) {
    return null;
  }
  if (Object.prototype.toString.call(value) === '[object Date]' && !isNaN(value.getTime())) {
    return value;
  }
  if (typeof value === 'number') {
    return new Date(Math.round((value - 25569) * 86400000));
  }

  var text = String(value).replace(/^'+/, '').trim();
  var match = text.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/);
  if (!match) {
    return null;
  }
  return new Date(
    Number(match[1]),
    Number(match[2]) - 1,
    Number(match[3]),
    Number(match[4]),
    Number(match[5]),
    Number(match[6] || 0)
  );
}

function parseSheetNumericValue_(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }
  if (typeof value === 'number') {
    return value;
  }
  var text = String(value).replace(/^'+/, '').trim().replace(',', '.');
  if (!/^-?\d+(\.\d+)?$/.test(text)) {
    return null;
  }
  return Number(text);
}
