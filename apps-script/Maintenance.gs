function repairTopusFastLayout() {
  var ss = SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
  ensureWorkbookRows_(ss);
  repairPushEventsLayout_(ss);
  moveSubscriptionProjectColumns_(ss);
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
