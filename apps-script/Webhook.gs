function doPost(e) {
  var startedAt = Date.now();
  var timestamp = new Date();
  var rawXml = e && e.postData && e.postData.contents ? e.postData.contents : '';
  var payload = parsePushPayload_(rawXml);

  var lock = LockService.getScriptLock();
  // A slow callback causes YouTube to retry and turns one burst into hundreds
  // of simultaneous Apps Script executions. The critical section below is
  // intentionally small; if it is genuinely busy, fail the delivery so the
  // hub can retry instead of holding another execution for 30 seconds.
  if (!lock.tryLock(5000)) {
    throw new Error('Push queue is temporarily busy; delivery should be retried');
  }

  var accepted = false;
  try {
    if (!payload.videoId || !payload.channelId) {
      console.warn('Push payload ignored: missing videoId or channelId');
      return textOutput_('ignored: missing videoId or channelId');
    }
    if (!appendPushEvent_(timestamp, payload.videoId, payload.channelId, rawXml)) {
      return textOutput_('ignored: duplicate push event');
    }
    accepted = true;
  } finally {
    lock.releaseLock();
  }

  // The durable Sheet row is now committed. Do not make the WebSub callback
  // wait for an outbound GitHub request while it owns the queue lock.
  if (accepted) {
    triggerPushPublisher_(payload.videoId, payload.channelId);
    console.log('Push event accepted in ' + (Date.now() - startedAt) + 'ms: ' + payload.videoId);
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
