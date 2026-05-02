var MASTER_SPREADSHEET_ID = '19E8OWIYgAoR-PYrtlyPd0HdoBHWXg7nC_bxB_RVZhKI';
var PUSH_EVENTS_SHEET_NAME = 'Push события';

function doPost(e) {
  var timestamp = new Date();
  var rawXml = e && e.postData && e.postData.contents ? e.postData.contents : '';

  var videoId = '';
  var channelId = '';

  try {
    var doc = XmlService.parse(rawXml);
    var atom = XmlService.getNamespace('http://www.w3.org/2005/Atom');
    var yt = XmlService.getNamespace('yt', 'http://www.youtube.com/xml/schemas/2015');
    var entry = doc.getRootElement().getChild('entry', atom);

    if (!entry) {
      throw new Error('Missing Atom entry');
    }

    videoId = entry.getChild('videoId', yt).getText();
    channelId = entry.getChild('channelId', yt).getText();
  } catch (err) {
    console.error('Failed to parse push payload: ' + err);
  }

  var lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    var ss = SpreadsheetApp.openById(MASTER_SPREADSHEET_ID);
    var sheet = ss.getSheetByName(PUSH_EVENTS_SHEET_NAME) ||
                ss.insertSheet(PUSH_EVENTS_SHEET_NAME);

    ensurePushEventsHeader_(sheet);
    sheet.appendRow([timestamp, videoId, channelId, '❌', '', rawXml]);
  } finally {
    lock.releaseLock();
  }

  return ContentService.createTextOutput('ok')
                       .setMimeType(ContentService.MimeType.TEXT);
}

function doGet(e) {
  var challenge = e && e.parameter ? e.parameter['hub.challenge'] : '';

  return ContentService.createTextOutput(challenge || 'alive')
                       .setMimeType(ContentService.MimeType.TEXT);
}

function ensurePushEventsHeader_(sheet) {
  var expected = ['Timestamp (UTC)', 'Video ID', 'Channel ID', 'Обработано', 'Проекты', 'Raw XML'];
  var current = sheet.getRange(1, 1, 1, expected.length).getValues()[0];
  var hasHeader = current.some(function(value) {
    return String(value || '').trim() !== '';
  });

  if (!hasHeader) {
    sheet.getRange(1, 1, 1, expected.length).setValues([expected]);
  }
}
