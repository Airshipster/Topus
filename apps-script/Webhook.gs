function doPost(e) {
  var timestamp = formatTimestamp_(new Date());
  var rawXml = e && e.postData && e.postData.contents ? e.postData.contents : '';
  var payload = parsePushPayload_(rawXml);

  var lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    appendPushEvent_(timestamp, payload.videoId, payload.channelId, rawXml);
    triggerPublisher_(payload.videoId, payload.channelId);
  } finally {
    lock.releaseLock();
  }

  return textOutput_('ok');
}

function doGet(e) {
  var challenge = e && e.parameter ? e.parameter['hub.challenge'] : '';

  return textOutput_(challenge || 'alive');
}
