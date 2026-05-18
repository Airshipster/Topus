function doPost(e) {
  var timestamp = new Date();
  var rawXml = e && e.postData && e.postData.contents ? e.postData.contents : '';
  var payload = parsePushPayload_(rawXml);

  var lock = LockService.getScriptLock();
  lock.waitLock(30000);

  try {
    if (!payload.videoId || !payload.channelId) {
      console.warn('Push payload ignored: missing videoId or channelId');
      return textOutput_('ignored: missing videoId or channelId');
    }
    if (!appendPushEvent_(timestamp, payload.videoId, payload.channelId, rawXml)) {
      return textOutput_('ignored: duplicate push event');
    }
    triggerPublisher_(payload.videoId, payload.channelId);
  } finally {
    lock.releaseLock();
  }

  return textOutput_('ok');
}

function doGet(e) {
  var maintenance = e && e.parameter ? e.parameter.maintenance : '';
  if (maintenance === 'fastLayout') {
    repairTopusFastLayout();
    return textOutput_('ok: fastLayout');
  }

  var challenge = e && e.parameter ? e.parameter['hub.challenge'] : '';

  return textOutput_(challenge || 'alive');
}
