function appendPushEvent_(timestamp, videoId, channelId, rawXml) {
  if (!videoId || !channelId) {
    console.warn('Push event not appended: missing videoId or channelId');
    return;
  }

  var ss = SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
  var sheet = ss.getSheetByName(PUSH_EVENTS_SHEET_NAME) ||
              ss.insertSheet(PUSH_EVENTS_SHEET_NAME);

  ensurePushEventsHeader_(sheet);
  var columns = pushEventsHeaderMap_(sheet);
  var nextRow = sheet.getLastRow() + 1;
  if (nextRow > sheet.getMaxRows()) {
    sheet.insertRowsAfter(sheet.getMaxRows(), nextRow - sheet.getMaxRows());
  }
  setPushEventCell_(sheet, nextRow, columns, 'Timestamp GMT+4', timestamp);
  setPushEventCell_(sheet, nextRow, columns, 'Video ID', stripLeadingApostrophe_(videoId));
  setPushEventCell_(sheet, nextRow, columns, 'Ссылка на канал', channelLink_(channelId));
  setPushEventCell_(sheet, nextRow, columns, 'Обработано', '❌');
  setPushEventCell_(sheet, nextRow, columns, 'Проекты', '');
  setPushEventCell_(sheet, nextRow, columns, 'Raw XML', stripLeadingApostrophe_(rawXml));

  sheet.getRange(nextRow, 1, 1, Math.max(sheet.getLastColumn(), PUSH_EVENTS_HEADERS.length))
    .setWrapStrategy(SpreadsheetApp.WrapStrategy.CLIP);
  sheet.setRowHeightsForced(nextRow, 1, 21);
}

function ensurePushEventsHeader_(sheet) {
  ensureSheetRows_(sheet, 10000);
  var headerWidth = Math.max(sheet.getLastColumn(), PUSH_EVENTS_HEADERS.length);
  var current = sheet.getRange(1, 1, 1, headerWidth).getValues()[0];
  var hasHeader = current.some(function(value) {
    return String(value || '').trim() !== '';
  });

  if (!hasHeader) {
    sheet.getRange(1, 1, 1, PUSH_EVENTS_HEADERS.length).setValues([PUSH_EVENTS_HEADERS]);
  }

  sheet.getRange(1, 1, Math.max(sheet.getMaxRows(), 1), Math.max(sheet.getLastColumn(), PUSH_EVENTS_HEADERS.length))
    .setWrapStrategy(SpreadsheetApp.WrapStrategy.CLIP);
  if (sheet.getLastRow() > 1) {
    sheet.setRowHeightsForced(2, sheet.getLastRow() - 1, 21);
  }
  sheet.setFrozenRows(1);
}

function pushEventsHeaderMap_(sheet) {
  var headerWidth = Math.max(sheet.getLastColumn(), PUSH_EVENTS_HEADERS.length);
  var headers = sheet.getRange(1, 1, 1, headerWidth).getValues()[0];
  var columns = {};
  headers.forEach(function(header, index) {
    var name = String(header || '').trim();
    if (name) {
      columns[name] = index + 1;
    }
  });
  return columns;
}

function setPushEventCell_(sheet, rowIndex, columns, headerName, value) {
  var column = columns[headerName];
  if (!column) {
    throw new Error('Push events header not found: ' + headerName);
  }
  sheet.getRange(rowIndex, column).setValue(value);
}

function ensureSheetRows_(sheet, targetRows) {
  var currentRows = sheet.getMaxRows();
  var desiredRows = Math.max(targetRows, sheet.getLastRow());
  if (currentRows < targetRows) {
    var insertBefore = Math.max(2, currentRows - 9);
    sheet.insertRowsBefore(insertBefore, desiredRows - currentRows);
  } else if (currentRows > desiredRows) {
    sheet.deleteRows(desiredRows + 1, currentRows - desiredRows);
  }
}

function channelLink_(channelId) {
  var value = stripLeadingApostrophe_(channelId);
  if (!value) {
    return '';
  }
  if (value.indexOf('http') === 0) {
    return value;
  }
  return 'https://www.youtube.com/channel/' + value;
}

function stripLeadingApostrophe_(value) {
  return String(value || '').replace(/^'+/, '');
}
