function repairTopusFastLayout() {
  var ss = SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
  ensureWorkbookRows_(ss);
  migrateLogsChannelIdColumn_(ss);
  repairPushEventsLayout_(ss);
  moveSubscriptionProjectColumns_(ss);
}

function migrateLogsChannelIdColumn() {
  var ss = SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
  migrateLogsChannelIdColumn_(ss);
  return 'ok';
}

function migrateLogsChannelIdColumn_(ss) {
  var logs = ss.getSheetByName('Логи');
  if (!logs) {
    return;
  }

  ensureSheetRows_(logs, 10000);

  var headerWidth = Math.max(logs.getLastColumn(), 5);
  var headers = logs.getRange(1, 1, 1, headerWidth).getDisplayValues()[0]
    .map(function(value) { return String(value || '').trim(); });
  var eventCol = findHeaderColumn_(headers, 'Событие');
  var channelCol = findHeaderColumn_(headers, 'Channel ID');

  if (!channelCol) {
    var insertBefore = eventCol || 4;
    logs.insertColumnBefore(insertBefore);
    channelCol = insertBefore;
    eventCol = insertBefore + 1;
  } else {
    eventCol = findHeaderColumn_(headers, 'Событие') || channelCol + 1;
  }

  logs.getRange(1, channelCol).setValue('Channel ID');
  logs.getRange(1, eventCol).setFormula('="Событие"&CHAR(10)&"Push: "&COUNTIF(OFFSET(INDEX(A:ZZ;ROW()+1;COLUMN());0;0;10000;1);"*Push:*")&", RSS: "&COUNTIF(OFFSET(INDEX(A:ZZ;ROW()+1;COLUMN());0;0;10000;1);"*RSS:*")');

  var lastRow = logs.getLastRow();
  if (lastRow <= 1) {
    return;
  }

  var videoCol = findHeaderColumn_(logs.getRange(1, 1, 1, logs.getLastColumn()).getDisplayValues()[0], 'Video ID') || 3;
  var projectCol = findHeaderColumn_(logs.getRange(1, 1, 1, logs.getLastColumn()).getDisplayValues()[0], 'Проект') || 1;
  var channelLookup = buildVideoChannelLookup_(ss);
  var rows = logs.getRange(2, 1, lastRow - 1, logs.getLastColumn()).getDisplayValues();
  var channelValues = rows.map(function(row) {
    var current = String(row[channelCol - 1] || '').trim();
    if (current) {
      return [extractChannelId_(current) || current];
    }
    var videoId = extractVideoId_(row[videoCol - 1]);
    var project = String(row[projectCol - 1] || '').trim();
    return [channelLookup[videoId + '\u0001' + project] || channelLookup[videoId + '\u0001'] || ''];
  });
  logs.getRange(2, channelCol, channelValues.length, 1).setValues(channelValues);
}

function buildVideoChannelLookup_(ss) {
  var lookup = {};
  var videos = ss.getSheetByName('Глобальные видео');
  if (!videos || videos.getLastRow() <= 1) {
    return lookup;
  }

  var headers = videos.getRange(1, 1, 1, videos.getLastColumn()).getDisplayValues()[0];
  var projectCol = findHeaderColumn_(headers, 'Проект') || 1;
  var videoCol = findHeaderColumn_(headers, 'Ссылка на видео') || findHeaderColumn_(headers, 'Video ID') || 5;
  var channelCol = findHeaderColumn_(headers, 'Ссылка на канал') || findHeaderColumn_(headers, 'Channel ID') || 3;
  var rows = videos.getRange(2, 1, videos.getLastRow() - 1, videos.getLastColumn()).getDisplayValues();

  rows.forEach(function(row) {
    var videoId = extractVideoId_(row[videoCol - 1]);
    var channelId = extractChannelId_(row[channelCol - 1]);
    var project = String(row[projectCol - 1] || '').trim();
    if (!videoId || !channelId) {
      return;
    }
    lookup[videoId + '\u0001' + project] = channelId;
    if (!lookup[videoId + '\u0001']) {
      lookup[videoId + '\u0001'] = channelId;
    }
  });
  return lookup;
}

function findHeaderColumn_(headers, target) {
  var normalizedTarget = String(target || '').toLowerCase();
  for (var i = 0; i < headers.length; i++) {
    var value = String(headers[i] || '').trim();
    if (value === target || value.toLowerCase() === normalizedTarget) {
      return i + 1;
    }
    if (target === 'Событие' && value.indexOf('Событие') !== -1) {
      return i + 1;
    }
  }
  return 0;
}

function extractVideoId_(value) {
  var text = String(value || '').trim();
  var match = text.match(/[?&]v=([^&]+)/) || text.match(/youtu\.be\/([^?&]+)/);
  return match ? match[1] : text;
}

function extractChannelId_(value) {
  var text = String(value || '').trim();
  var match = text.match(/channel\/([^/?#]+)/);
  return match ? match[1] : text;
}

function ensureWorkbookRows_(ss) {
  ss.getSheets().forEach(function(sheet) {
    if (sheet.getName() === 'Настройки') {
      ensureSheetRows_(sheet, 300);
      return;
    }
    ensureSheetRows_(sheet, 10000);
  });
  extendConditionalFormattingToSheetEnd_(ss);
}

function repairPushEventsLayout_(ss) {
  var sheet = ss.getSheetByName(PUSH_EVENTS_SHEET_NAME);
  if (!sheet) {
    return;
  }
  ensureSheetRows_(sheet, 10000);
  sheet.getRange(1, 1, Math.max(sheet.getMaxRows(), 1), Math.max(sheet.getLastColumn(), PUSH_EVENTS_HEADERS.length))
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

function extendConditionalFormattingToSheetEnd_(ss) {
  ss.getSheets().forEach(function(sheet) {
    var rowCount = sheet.getMaxRows();
    var rules = sheet.getConditionalFormatRules();
    var changed = false;
    var updatedRules = rules.map(function(rule) {
      var ranges = rule.getRanges();
      var updatedRanges = ranges.map(function(range) {
        if (range.getSheet().getSheetId() !== sheet.getSheetId()) {
          return range;
        }
        if (range.getNumRows() === rowCount - range.getRow() + 1) {
          return range;
        }
        changed = true;
        return sheet.getRange(
          range.getRow(),
          range.getColumn(),
          rowCount - range.getRow() + 1,
          range.getNumColumns()
        );
      });
      return rule.copy().setRanges(updatedRanges).build();
    });
    if (changed) {
      sheet.setConditionalFormatRules(updatedRules);
    }
  });
}
