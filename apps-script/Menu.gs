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
  sheet.getRange('S2').setValue(text);
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
