function onOpen(e) {
  addTopusMenu_();
}

function addTopusMenu_() {
  SpreadsheetApp.getUi()
    .createMenu('Topus')
    .addItem('Забрать обновления сейчас', 'runTopusManualRefresh')
    .addItem('Проверить push-подписки', 'runTopusSubscriptionRenew')
    .addItem('Синхронизировать ботов с Cloudflare', 'runTopusBotCloudflareSync')
    .addToUi();
}

function runTopusManualRefresh() {
  triggerPublisher_('', '', {syncOnly: true});
  SpreadsheetApp.getActiveSpreadsheet().toast('Запуск Topus отправлен в GitHub Actions', 'Topus', 5);
}

function runTopusSubscriptionRenew() {
  triggerPublisher_('', '', {syncOnly: true, forceSubscriptionSync: true});
  SpreadsheetApp.getActiveSpreadsheet().toast('Проверка push-подписок отправлена в GitHub Actions', 'Topus', 5);
}

function runTopusBotCloudflareSync() {
  writeTopusBotSyncStatus_('Bot Cloudflare sync: dispatching to GitHub Actions at ' + topusStatusTimestamp_());
  var result = triggerPublisher_('', '', {syncBotState: true});

  if (result && result.ok) {
    writeTopusBotSyncStatus_('Bot Cloudflare sync: GitHub Actions accepted dispatch at ' + topusStatusTimestamp_() + '; status=' + result.status);
    SpreadsheetApp.getActiveSpreadsheet().toast('Синхронизация ботов с Cloudflare отправлена в GitHub Actions', 'Topus', 5);
    return;
  }

  var message = result && result.message ? result.message : 'unknown error';
  writeTopusBotSyncStatus_('Bot Cloudflare sync: dispatch failed at ' + topusStatusTimestamp_() + '; ' + message);
  SpreadsheetApp.getActiveSpreadsheet().toast('Синхронизация ботов с Cloudflare не отправлена', 'Topus', 8);
}

function writeTopusBotSyncStatus_(text) {
  var ss = SpreadsheetApp.getActiveSpreadsheet() || SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
  var sheet = ss.getSheetByName('Боты') || ss.getActiveSheet();
  var statusColumn = topusBotStatusColumn_(sheet);
  clearTopusBotStatusDuplicates_(sheet, statusColumn);
  sheet.getRange(2, statusColumn).setValue(text);
  clearTopusBotStatusDuplicates_(sheet, statusColumn);
}

function topusStatusTimestamp_() {
  return Utilities.formatDate(new Date(), DISPLAY_TIMEZONE, 'dd.MM.yyyy HH:mm:ss');
}

function topusBotStatusColumn_(sheet) {
  var headers = sheet.getRange(1, 1, 1, Math.max(1, sheet.getLastColumn())).getValues()[0];
  for (var index = 0; index < headers.length; index++) {
    if (String(headers[index]).trim() === 'Sync Action') {
      return index + 2;
    }
  }
  for (var last = headers.length - 1; last >= 0; last--) {
    if (String(headers[last]).trim()) {
      return last + 2;
    }
  }
  return 18;
}

function clearTopusBotStatusDuplicates_(sheet, statusColumn) {
  var firstStatusColumn = Math.max(18, statusColumn);
  var lastColumn = Math.max(sheet.getLastColumn(), firstStatusColumn + 8);
  var values = sheet.getRange(1, firstStatusColumn, 2, lastColumn - firstStatusColumn + 1).getValues();
  for (var offset = 0; offset < values[0].length; offset++) {
    var column = firstStatusColumn + offset;
    if (column === statusColumn) {
      continue;
    }
    var row1 = String(values[0][offset] || '').trim();
    var row2 = String(values[1][offset] || '').trim();
    if (isTopusBotServiceStatus_(row1) || isTopusBotServiceStatus_(row2)) {
      sheet.getRange(1, column, 2, 1).clearContent();
    }
  }
}

function isTopusBotServiceStatus_(value) {
  return value.indexOf('Cloudflare sync ') === 0 || value.indexOf('Bot Cloudflare sync') === 0;
}

function installTopusMasterMenuTrigger() {
  var triggers = ScriptApp.getProjectTriggers();

  triggers.forEach(function(trigger) {
    if (trigger.getHandlerFunction() === 'onOpen') {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  ScriptApp.newTrigger('onOpen')
    .forSpreadsheet(MASTER_SPREADSHEET_ID)
    .onOpen()
    .create();

  return 'ok';
}
