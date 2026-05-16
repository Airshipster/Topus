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
  sheet.getRange(nextRow, 1, 1, PUSH_EVENTS_HEADERS.length)
    .setValues([[timestamp, stripLeadingApostrophe_(videoId), channelLink_(channelId), '❌', '', stripLeadingApostrophe_(rawXml)]]);
  sheet.getRange(nextRow, 1).setNumberFormat('yyyy-mm-dd hh:mm:ss');
  sheet.getRange(nextRow, 6).setWrapStrategy(SpreadsheetApp.WrapStrategy.CLIP);
  sheet.setRowHeight(nextRow, 21);
}

function ensurePushEventsHeader_(sheet) {
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
    sheet.setRowHeights(2, sheet.getLastRow() - 1, 21);
  }
  sheet.setFrozenRows(1);
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
