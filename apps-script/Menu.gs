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
  clearTopusBotStatusTail_(sheet, statusColumn);
  sheet.getRange(2, statusColumn).setValue(text);
  clearTopusBotStatusTail_(sheet, statusColumn);
}

function topusStatusTimestamp_() {
  return Utilities.formatDate(new Date(), DISPLAY_TIMEZONE, 'dd.MM.yyyy HH:mm:ss');
}

function topusBotStatusColumn_(sheet) {
  // Bot data is currently A:Q; R is the single service status column.
  return 18;
}

function clearTopusBotStatusTail_(sheet, statusColumn) {
  var firstDuplicateColumn = statusColumn + 1;
  var lastColumn = Math.max(sheet.getLastColumn(), 26);
  if (lastColumn >= firstDuplicateColumn) {
    sheet.getRange(1, firstDuplicateColumn, 2, lastColumn - firstDuplicateColumn + 1).clearContent();
  }
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
