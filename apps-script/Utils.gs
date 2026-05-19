function parsePushPayload_(rawXml) {
  var payload = {
    videoId: '',
    channelId: ''
  };

  try {
    var doc = XmlService.parse(rawXml);
    var atom = XmlService.getNamespace('http://www.w3.org/2005/Atom');
    var yt = XmlService.getNamespace('yt', 'http://www.youtube.com/xml/schemas/2015');
    var entry = doc.getRootElement().getChild('entry', atom);

    if (!entry) {
      throw new Error('Missing Atom entry');
    }

    var videoId = entry.getChild('videoId', yt);
    var channelId = entry.getChild('channelId', yt);
    payload.videoId = videoId ? videoId.getText().trim() : '';
    payload.channelId = channelId ? channelId.getText().trim() : '';
  } catch (err) {
    console.error('Failed to parse push payload: ' + err);
  }

  return payload;
}

function textOutput_(text) {
  return ContentService.createTextOutput(text)
                       .setMimeType(ContentService.MimeType.TEXT);
}

function formatTimestamp_(date) {
  return (date.getTime() + 4 * 60 * 60 * 1000) / 86400000 + 25569;
}

function triggerPublisher_(videoId, channelId, options) {
  var token = PropertiesService.getScriptProperties().getProperty('GITHUB_DISPATCH_TOKEN');
  options = options || {};

  if (!token) {
    var missingTokenMessage = 'GITHUB_DISPATCH_TOKEN is not set; publisher will run on cron fallback';
    console.warn(missingTokenMessage);
    return {ok: false, status: 0, message: missingTokenMessage};
  }

  if (videoId || channelId) {
    var properties = PropertiesService.getScriptProperties();
    var now = Date.now();
    var lastDispatch = Number(properties.getProperty('TOPUS_LAST_PUSH_DISPATCH_MS') || 0);
    if (lastDispatch && now - lastDispatch < 60000) {
      var skippedMessage = 'GitHub dispatch skipped: recent push dispatch already queued';
      console.log(skippedMessage);
      return {ok: true, status: 0, message: skippedMessage};
    }
    properties.setProperty('TOPUS_LAST_PUSH_DISPATCH_MS', String(now));
  }

  var url = 'https://api.github.com/repos/' + GITHUB_OWNER + '/' + GITHUB_REPO + '/dispatches';
  var payload = {
    event_type: GITHUB_DISPATCH_EVENT_TYPE,
    client_payload: {
      video_id: videoId || '',
      channel_id: channelId || '',
      force_subscription_sync: options.forceSubscriptionSync ? 'true' : 'false',
      sync_only: options.syncOnly ? 'true' : 'false',
      sync_bot_state: options.syncBotState ? 'true' : 'false'
    }
  };

  var response = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    headers: {
      Authorization: 'Bearer ' + token,
      Accept: 'application/vnd.github+json'
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  var status = response.getResponseCode();
  if (status < 200 || status >= 300) {
    var failureMessage = 'Failed to trigger GitHub dispatch: ' + status + ' ' + response.getContentText();
    console.error(failureMessage);
    return {ok: false, status: status, message: failureMessage};
  }

  return {ok: true, status: status, message: 'GitHub dispatch accepted'};
}
