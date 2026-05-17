function appendPushEvent_(timestamp, videoId, channelId, rawXml) {
  if (!videoId || !channelId) {
    console.warn('Push event not appended: missing videoId or channelId');
    return;
  }

  var ss = SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
  var sheet = ss.getSheetByName(PUSH_EVENTS_SHEET_NAME) ||
              ss.insertSheet(PUSH_EVENTS_SHEET_NAME);

  ensurePushEventsHeader_(sheet);
  var nextRow = sheet.getLastRow() + 1;
  if (nextRow > sheet.getMaxRows()) {
    sheet.insertRowsAfter(sheet.getMaxRows(), nextRow - sheet.getMaxRows());
  }
  sheet.getRange(nextRow, 1, 1, PUSH_EVENTS_HEADERS.length)
    .setValues([[timestamp, stripLeadingApostrophe_(videoId), channelLink_(channelId), '❌', '', stripLeadingApostrophe_(rawXml)]]);
  sheet.getRange(nextRow, 1).setNumberFormat('yyyy-mm-dd hh:mm:ss');
  sheet.getRange(nextRow, 1, 1, PUSH_EVENTS_HEADERS.length).setWrapStrategy(SpreadsheetApp.WrapStrategy.CLIP);
  sheet.setRowHeight(nextRow, 21);
}

function ensurePushEventsHeader_(sheet) {
  ensureSheetRows_(sheet, 10000);
  var current = sheet.getRange(1, 1, 1, PUSH_EVENTS_HEADERS.length).getValues()[0];
  var hasHeader = current.some(function(value) {
    return String(value || '').trim() !== '';
  });

  if (!hasHeader) {
    sheet.getRange(1, 1, 1, PUSH_EVENTS_HEADERS.length).setValues([PUSH_EVENTS_HEADERS]);
  }

  sheet.getRange(2, 1, Math.max(sheet.getMaxRows() - 1, 1), 1)
    .setNumberFormat('yyyy-mm-dd hh:mm:ss');
  sheet.getRange(1, 1, Math.max(sheet.getMaxRows(), 1), PUSH_EVENTS_HEADERS.length)
    .setWrapStrategy(SpreadsheetApp.WrapStrategy.CLIP);
  if (sheet.getLastRow() > 1) {
    sheet.setRowHeightsForced(2, sheet.getLastRow() - 1, 21);
  }
  sheet.setFrozenRows(1);
}

function ensureSheetRows_(sheet, targetRows) {
  var currentRows = sheet.getMaxRows();
  var desiredRows = Math.max(targetRows, sheet.getLastRow());
  if (currentRows < targetRows) {
    sheet.insertRowsAfter(currentRows, desiredRows - currentRows);
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
