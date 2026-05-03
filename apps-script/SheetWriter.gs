function appendPushEvent_(timestamp, videoId, channelId, rawXml) {
  var ss = SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
  var sheet = ss.getSheetByName(PUSH_EVENTS_SHEET_NAME) ||
              ss.insertSheet(PUSH_EVENTS_SHEET_NAME);

  ensurePushEventsHeader_(sheet);
  sheet.appendRow([timestamp, videoId, channelId, '❌', '', rawXml]);
}

function ensurePushEventsHeader_(sheet) {
  var current = sheet.getRange(1, 1, 1, PUSH_EVENTS_HEADERS.length).getValues()[0];
  var hasHeader = current.some(function(value) {
    return String(value || '').trim() !== '';
  });

  if (!hasHeader) {
    sheet.getRange(1, 1, 1, PUSH_EVENTS_HEADERS.length).setValues([PUSH_EVENTS_HEADERS]);
  }
}
