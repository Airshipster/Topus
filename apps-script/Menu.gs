function onOpen(e) {
  addTopusMenu_();
}

function addTopusMenu_() {
  SpreadsheetApp.getUi()
    .createMenu('Topus')
    .addItem('Забрать обновления сейчас', 'runTopusManualRefresh')
    .addToUi();
}

function runTopusManualRefresh() {
  triggerPublisher_('', '');
  SpreadsheetApp.getActiveSpreadsheet().toast('Запуск Topus отправлен в GitHub Actions', 'Topus', 5);
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
