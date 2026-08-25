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
  if (hasRecentPushEvent_(sheet, columns, videoId, channelId, timestamp)) {
    console.log('Push event ignored as duplicate in retention window: ' + videoId + ' / ' + channelId);
    return false;
  }

  var nextRow = sheet.getLastRow() + 1;
  if (nextRow > sheet.getMaxRows()) {
    sheet.insertRowsAfter(sheet.getMaxRows(), nextRow - sheet.getMaxRows());
  }
  var width = Math.max(sheet.getLastColumn(), PUSH_EVENTS_HEADERS.length);
  var row = new Array(width).fill('');
  row[columns['Timestamp GMT+4'] - 1] = timestamp;
  row[columns['Video ID'] - 1] = stripLeadingApostrophe_(videoId);
  row[columns['Ссылка на канал'] - 1] = channelLink_(channelId);
  row[columns['Обработано'] - 1] = '❌';
  row[columns['Проекты'] - 1] = '';
  row[columns['Raw XML'] - 1] = stripLeadingApostrophe_(rawXml);
  sheet.getRange(nextRow, 1, 1, width).setValues([row]);
  sheet.getRange(nextRow, columns['Timestamp GMT+4']).setNumberFormat('dd.mm.yyyy h:mm:ss');
  sheet.getRange(nextRow, 1, 1, width).setWrapStrategy(SpreadsheetApp.WrapStrategy.CLIP);
  sheet.setRowHeightsForced(nextRow, 1, 21);
  rememberRecentPushEvent_(videoId, channelId);
  return true;
}

function ensurePushEventsHeader_(sheet) {
  var headerWidth = Math.max(sheet.getLastColumn(), PUSH_EVENTS_HEADERS.length);
  var current = sheet.getRange(1, 1, 1, headerWidth).getValues()[0];
  var hasHeader = current.some(function(value) {
    return String(value || '').trim() !== '';
  });

  if (!hasHeader) {
    sheet.getRange(1, 1, 1, PUSH_EVENTS_HEADERS.length).setValues([PUSH_EVENTS_HEADERS]);
    sheet.getRange(1, 1, 1, PUSH_EVENTS_HEADERS.length)
      .setWrapStrategy(SpreadsheetApp.WrapStrategy.CLIP);
    sheet.setFrozenRows(1);
  }
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
  var range = sheet.getRange(rowIndex, column);
  range.setValue(value);
  if (headerName === 'Timestamp GMT+4') {
    range.setNumberFormat('dd.mm.yyyy h:mm:ss');
  }
}

function hasRecentPushEvent_(sheet, columns, videoId, channelId, timestamp) {
  var timestampCol = columns['Timestamp GMT+4'];
  var videoCol = columns['Video ID'];
  var channelCol = columns['Ссылка на канал'];
  if (!timestampCol || !videoCol || !channelCol) {
    return false;
  }

  var cache = CacheService.getScriptCache();
  if (cache.get(pushEventCacheKey_(videoId, channelId))) {
    return true;
  }

  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) {
    return false;
  }

  var cutoffMs = timestamp.getTime() - PUSH_EVENT_DEDUP_DAYS * 24 * 60 * 60 * 1000;
  var channelValue = channelLink_(channelId);
  var firstColumn = Math.min(timestampCol, videoCol, channelCol);
  var lastColumn = Math.max(timestampCol, videoCol, channelCol);
  var chunkSize = 250;

  for (var endRow = lastRow; endRow >= 2; endRow -= chunkSize) {
    var startRow = Math.max(2, endRow - chunkSize + 1);
    var rows = sheet.getRange(startRow, firstColumn, endRow - startRow + 1, lastColumn - firstColumn + 1).getValues();
    for (var index = rows.length - 1; index >= 0; index--) {
      var row = rows[index];
      var rowTimestamp = pushEventTimestampMs_(row[timestampCol - firstColumn]);
      if (rowTimestamp && rowTimestamp < cutoffMs) {
        return false;
      }
      var rowVideoId = stripLeadingApostrophe_(row[videoCol - firstColumn]);
      var rowChannelId = channelIdFromLink_(row[channelCol - firstColumn]) || stripLeadingApostrophe_(row[channelCol - firstColumn]);
      if (rowVideoId === videoId && rowChannelId === channelId) {
        return true;
      }
      if (rowVideoId === videoId && stripLeadingApostrophe_(row[channelCol - firstColumn]) === channelValue) {
        return true;
      }
    }
  }
  return false;
}

function pushEventCacheKey_(videoId, channelId) {
  return 'topus-push-event:' + videoId + ':' + channelId;
}

function rememberRecentPushEvent_(videoId, channelId) {
  // Cache is only a fast-path for retried WebSub deliveries. The Sheet keeps
  // the seven-day durable deduplication window above.
  CacheService.getScriptCache().put(pushEventCacheKey_(videoId, channelId), '1', 21600);
}

function pushEventTimestampMs_(value) {
  if (value instanceof Date) {
    return value.getTime();
  }
  if (typeof value === 'number') {
    return (value - 25569) * 86400000;
  }
  var parsed = new Date(value);
  return isNaN(parsed.getTime()) ? 0 : parsed.getTime();
}

function ensureSheetRows_(sheet, targetRows) {
  var currentRows = sheet.getMaxRows();
  var desiredRows = Math.max(targetRows, sheet.getLastRow());
  if (currentRows < desiredRows) {
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

function channelIdFromLink_(value) {
  var match = String(value || '').match(/(UC[0-9A-Za-z_-]{20,})/);
  return match ? match[1] : '';
}

function stripLeadingApostrophe_(value) {
  return String(value || '').replace(/^'+/, '');
}
