function topusQueueHealth_() {
  var cache = CacheService.getScriptCache();
  var cached = cache.get('topus-queue-health-v1');
  if (cached) return ContentService.createTextOutput(cached).setMimeType(ContentService.MimeType.JSON);
  var result = {kind: 'topus-push-queue-read', ok: false, checkedAt: Date.now()};
  try {
    var sheet = SpreadsheetApp.openById(MASTER_SPREADSHEET_ID).getSheetByName(PUSH_EVENTS_SHEET_NAME);
    if (!sheet) throw new Error('Queue missing');
    sheet.getRange(1, 1).getValue();
    result.ok = true;
  } catch (error) {
    result.error = 'QUEUE_READ_FAILED';
  }
  var body = JSON.stringify(result);
  cache.put('topus-queue-health-v1', body, result.ok ? 240 : 30);
  return ContentService.createTextOutput(body).setMimeType(ContentService.MimeType.JSON);
}
