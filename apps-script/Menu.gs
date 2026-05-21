function onOpen(e) {
  addTopusMenu_();
}

function addTopusMenu_() {
  SpreadsheetApp.getUi()
    .createMenu('Topus')
    .addItem('Забрать обновления сейчас', 'runTopusManualRefresh')
    .addItem('Проверить push-подписки', 'runTopusSubscriptionRenew')
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

function runTopusBotCloudflareSyncV2() {
  var result = triggerPublisher_('', '', {syncBotState: true});

  if (result && result.ok) {
    SpreadsheetApp.getActiveSpreadsheet().toast('Синхронизация ботов с Cloudflare отправлена в GitHub Actions', 'Topus', 5);
    return;
  }

  var message = result && result.message ? result.message : 'unknown error';
  SpreadsheetApp.getActiveSpreadsheet().toast('Синхронизация ботов с Cloudflare не отправлена', 'Topus', 8);
}

function runTopusBotCloudflareSync() {
  return runTopusBotCloudflareSyncV2();
}

function writeTopusBotSyncStatus_(text) {
  return;
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
