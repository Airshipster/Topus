function onOpen(e) {
  addTopusMenu_();
}

function addTopusMenu_() {
  SpreadsheetApp.getUi()
    .createMenu('Topus')
    .addItem('Забрать обновления сейчас', 'runTopusManualRefresh')
    .addItem('Проверить push-подписки', 'runTopusSubscriptionRenew')
    .addItem('Синхронизировать меню ботов', 'runTopusWorkerConfigSync')
    .addItem('Синхронизировать ботов с Cloudflare', 'runTopusBotCloudflareSyncV2')
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

function runTopusWorkerConfigSync() {
  writeTopusBotSyncStatus_('Bot menu sync: GitHub Actions dispatching at ' + topusStatusTimestamp_());
  var result = triggerRepositoryDispatch_(GITHUB_WORKER_CONFIG_DISPATCH_EVENT_TYPE, {});

  if (result && result.ok) {
    writeTopusBotSyncStatus_('Bot menu sync: GitHub Actions accepted at ' + topusStatusTimestamp_() + '; status=' + result.status);
    SpreadsheetApp.getActiveSpreadsheet().toast('Синхронизация меню ботов отправлена в GitHub Actions', 'Topus', 5);
    return;
  }

  var message = result && result.message ? result.message : 'unknown error';
  writeTopusBotSyncStatus_('Bot menu sync: dispatch failed at ' + topusStatusTimestamp_() + '; ' + message);
  SpreadsheetApp.getActiveSpreadsheet().toast('Синхронизация меню ботов не отправлена', 'Topus', 8);
}

function runTopusBotCloudflareSyncV2() {
  writeTopusBotSyncStatus_('Bot Cloudflare sync: GitHub Actions dispatching at ' + topusStatusTimestamp_());
  var result = triggerPublisher_('', '', {syncBotState: true});

  if (result && result.ok) {
    writeTopusBotSyncStatus_('Bot Cloudflare sync: GitHub Actions accepted at ' + topusStatusTimestamp_() + '; status=' + result.status);
    SpreadsheetApp.getActiveSpreadsheet().toast('Синхронизация ботов с Cloudflare отправлена в GitHub Actions', 'Topus', 5);
    return;
  }

  var message = result && result.message ? result.message : 'unknown error';
  writeTopusBotSyncStatus_('Bot Cloudflare sync: dispatch failed at ' + topusStatusTimestamp_() + '; ' + message);
  SpreadsheetApp.getActiveSpreadsheet().toast('Синхронизация ботов с Cloudflare не отправлена', 'Topus', 8);
}

function runTopusBotCloudflareSync() {
  return runTopusBotCloudflareSyncV2();
}

function writeTopusBotSyncStatus_(text) {
  var ss = SpreadsheetApp.getActiveSpreadsheet() || SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
  var sheet = ss.getSheetByName('Боты') || ss.getActiveSheet();
  var column = findTopusBotStatusColumn_(sheet);
  sheet.getRange(2, column).setValue(text);
}

function findTopusBotStatusColumn_(sheet) {
  var maxColumns = sheet.getMaxColumns();
  var firstRow = sheet.getRange(1, 1, 1, maxColumns).getDisplayValues()[0];
  var secondRow = sheet.getRange(2, 1, 1, maxColumns).getDisplayValues()[0];
  var syncActionIndex = firstRow.indexOf('Sync Action');
  var startColumn = syncActionIndex >= 0 ? syncActionIndex + 2 : 18;

  for (var column = startColumn; column <= maxColumns; column++) {
    var top = String(firstRow[column - 1] || '').trim();
    var bottom = String(secondRow[column - 1] || '').trim();
    var combined = top + '\n' + bottom;
    if (/Cloudflare sync|Bot Cloudflare sync|Bot menu sync|remaining|source:/i.test(combined)) {
      return column;
    }
  }

  for (var emptyColumn = startColumn; emptyColumn <= maxColumns; emptyColumn++) {
    var topValue = String(firstRow[emptyColumn - 1] || '').trim();
    var bottomValue = String(secondRow[emptyColumn - 1] || '').trim();
    if (!topValue && !bottomValue) {
      return emptyColumn;
    }
  }

  sheet.insertColumnAfter(maxColumns);
  return maxColumns + 1;
}

function topusStatusTimestamp_() {
  return Utilities.formatDate(new Date(), DISPLAY_TIMEZONE, 'dd.MM.yyyy HH:mm:ss');
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
