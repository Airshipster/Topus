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

    payload.videoId = entry.getChild('videoId', yt).getText();
    payload.channelId = entry.getChild('channelId', yt).getText();
  } catch (err) {
    console.error('Failed to parse push payload: ' + err);
  }

  return payload;
}

function textOutput_(text) {
  return ContentService.createTextOutput(text)
                       .setMimeType(ContentService.MimeType.TEXT);
}

function triggerPublisher_(videoId, channelId) {
  var token = PropertiesService.getScriptProperties().getProperty('GITHUB_DISPATCH_TOKEN');

  if (!token) {
    console.warn('GITHUB_DISPATCH_TOKEN is not set; publisher will run on cron fallback');
    return;
  }

  var url = 'https://api.github.com/repos/' + GITHUB_OWNER + '/' + GITHUB_REPO + '/dispatches';
  var payload = {
    event_type: GITHUB_DISPATCH_EVENT_TYPE,
    client_payload: {
      video_id: videoId || '',
      channel_id: channelId || ''
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
    console.error('Failed to trigger GitHub dispatch: ' + status + ' ' + response.getContentText());
  }
}
