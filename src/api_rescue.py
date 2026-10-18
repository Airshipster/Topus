"""Quota-bounded official API fallback. Never treats RSS failure as recovery."""
import json
import os
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests
from push_store import database, queue_event


class BudgetExhausted(RuntimeError):
    pass


class InvalidPageToken(RuntimeError):
    pass


class UpstreamError(RuntimeError):
    def __init__(self, status, reasons):
        self.status = status
        self.reasons = tuple(reasons)
        super().__init__('API_RESCUE_HTTP_' + str(status))


class ApiPage(list):
    def __init__(self, items, next_token=''):
        super().__init__(items)
        self.next_token = next_token


def initialize():
    with database() as db:
        db.executescript('''
          CREATE TABLE IF NOT EXISTS api_rescue_budget (
            day TEXT PRIMARY KEY, used INTEGER NOT NULL, blocked INTEGER NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS api_rescue_channels (
            channel_id TEXT PRIMARY KEY, uploads TEXT, attempted REAL NOT NULL DEFAULT 0,
            checked REAL NOT NULL DEFAULT 0, payload TEXT, error TEXT NOT NULL DEFAULT '');
          CREATE TABLE IF NOT EXISTS api_rescue_videos (
            channel_id TEXT NOT NULL, video_id TEXT NOT NULL, payload TEXT NOT NULL,
            received REAL NOT NULL, PRIMARY KEY(channel_id,video_id));
          CREATE TABLE IF NOT EXISTS api_rescue_state (
            key TEXT PRIMARY KEY, value REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS api_rescue_progress (
            channel_id TEXT PRIMARY KEY, checkpoint TEXT NOT NULL DEFAULT '',
            head TEXT NOT NULL DEFAULT '', token TEXT NOT NULL DEFAULT '', cutoff TEXT NOT NULL DEFAULT '');
        ''')


def quota_day(now=None):
    return datetime.fromtimestamp(time.time() if now is None else now,
                                  ZoneInfo('America/Los_Angeles')).date().isoformat()


def reserve(now=None):
    initialize()
    limit = max(0, min(4000, int(os.environ.get('TOPUS_API_RESCUE_DAILY_LIMIT', '4000'))))
    day = quota_day(now)
    with database() as db:
        db.execute('BEGIN IMMEDIATE')
        db.execute('INSERT OR IGNORE INTO api_rescue_budget(day,used) VALUES (?,0)', (day,))
        row = db.execute('SELECT used,blocked FROM api_rescue_budget WHERE day=?', (day,)).fetchone()
        if row['blocked'] or row['used'] >= limit:
            raise BudgetExhausted('API_RESCUE_BUDGET_EXHAUSTED')
        db.execute('UPDATE api_rescue_budget SET used=used+1 WHERE day=?', (day,))
        db.execute('DELETE FROM api_rescue_budget WHERE day<?',
                   ((datetime.fromisoformat(day)-timedelta(days=7)).date().isoformat(),))


def request(endpoint, params, key):
    reserve()
    response = requests.get('https://www.googleapis.com/youtube/v3/'+endpoint,
                            params={**params, 'key':key}, timeout=(5,15))
    body = response.json()
    if response.status_code != 200:
        reasons = [e.get('reason') for e in body.get('error', {}).get('errors', [])]
        if any(r in ('quotaExceeded', 'dailyLimitExceeded') for r in reasons):
            with database() as db:
                db.execute('UPDATE api_rescue_budget SET blocked=1 WHERE day=?', (quota_day(),))
            raise BudgetExhausted('YOUTUBE_QUOTA_EXHAUSTED')
        if response.status_code == 400 and 'invalidPageToken' in reasons:
            raise InvalidPageToken('API_RESCUE_PAGE_TOKEN_EXPIRED')
        raise UpstreamError(response.status_code, reasons)
    expected = {'channels':'youtube#channelListResponse', 'playlistItems':'youtube#playlistItemListResponse'}
    items = body.get('items')
    if items is None and body.get('pageInfo', {}).get('totalResults') == 0:
        items = []
    if body.get('kind') != expected[endpoint] or not isinstance(items,list):
        raise ValueError('API_RESCUE_INVALID_RESPONSE')
    return ApiPage(items, body.get('nextPageToken', ''))


def scan_uploads(row, key, cutoff, consume):
    """Refresh the head, then resume a bounded tail without advancing an unfinished checkpoint."""
    channel = row['channel_id']
    with database() as db:
        progress = db.execute('SELECT * FROM api_rescue_progress WHERE channel_id=?', (channel,)).fetchone()
    progress = dict(progress) if progress else {}
    checkpoint = progress.get('checkpoint', '')
    if not checkpoint and row.get('payload'):
        previous = json.loads(row['payload'])
        checkpoint = previous[0].get('video_id', '') if previous else ''
    params = {'part': 'snippet,contentDetails', 'playlistId': row['uploads'], 'maxResults': 50}
    first = request('playlistItems', params, key)
    head = next((item.get('contentDetails', {}).get('videoId') for item in first
                 if item.get('contentDetails', {}).get('videoId')), '')
    scan_head = progress.get('head') or head
    scan_cutoff = progress.get('cutoff') or cutoff.isoformat()

    def apply(page):
        if checkpoint:
            boundary = next((i + 1 for i, item in enumerate(page)
                if item.get('contentDetails', {}).get('videoId') == checkpoint), len(page))
            consume(page[:boundary])
        else:
            # Initial seeding is age-bounded; catch-up to a known checkpoint is not.
            consume([item for item in page if item.get('contentDetails', {}).get('videoPublishedAt')
                and datetime.fromisoformat(item['contentDetails']['videoPublishedAt'].replace('Z', '+00:00'))
                >= datetime.fromisoformat(scan_cutoff)])
        ids = [item.get('contentDetails', {}).get('videoId') for item in page]
        if checkpoint:
            return checkpoint in ids or not getattr(page, 'next_token', '')
        stamps = [item.get('contentDetails', {}).get('videoPublishedAt') for item in page]
        valid = [stamp for stamp in stamps if stamp]
        return not getattr(page, 'next_token', '') or bool(valid and all(
            datetime.fromisoformat(stamp.replace('Z', '+00:00')) < datetime.fromisoformat(scan_cutoff)
            for stamp in valid))

    complete = apply(first)
    token = '' if complete else progress.get('token') or getattr(first, 'next_token', '')

    def save():
        with database() as db:
            db.execute('INSERT INTO api_rescue_progress VALUES (?,?,?,?,?) ON CONFLICT(channel_id) '
                       'DO UPDATE SET checkpoint=excluded.checkpoint,head=excluded.head,token=excluded.token,cutoff=excluded.cutoff',
                       (channel, (head if not progress.get('token') else scan_head) if complete else checkpoint,
                        '' if complete else scan_head, '' if complete else token, '' if complete else scan_cutoff))
    save()
    # Additional pages use the same durable daily budget; unfinished work resumes next pass.
    for _ in range(2):
        if complete or not token:
            break
        try:
            page = request('playlistItems', {**params, 'pageToken': token}, key)
        except InvalidPageToken:
            # A new scan restarts from the head, preserving the known checkpoint and received events.
            token = ''
            save()
            raise
        complete = apply(page)
        token = '' if complete else getattr(page, 'next_token', '')
        save()
    return complete


def select_channels(channels, limit):
    initialize()
    with database() as db:
        db.executemany('INSERT OR IGNORE INTO api_rescue_channels(channel_id) VALUES (?)',
                       [(c,) for c in channels])
        rows = db.execute('SELECT * FROM api_rescue_channels ORDER BY attempted,channel_id').fetchall()
    return [dict(r) for r in rows if r['channel_id'] in channels][:limit]


def claim_pass(now=None):
    initialize()
    current = time.time() if now is None else now
    with database() as db:
        db.execute('BEGIN IMMEDIATE')
        previous = db.execute("SELECT value FROM api_rescue_state WHERE key='last_pass'").fetchone()
        if previous and current - previous['value'] < 1800:
            return False
        db.execute("INSERT INTO api_rescue_state VALUES ('last_pass',?) ON CONFLICT(key) "
                   'DO UPDATE SET value=excluded.value', (current,))
    return True


def read_result(channel):
    initialize()
    with database() as db:
        rows = db.execute('SELECT payload FROM api_rescue_videos WHERE channel_id=? AND received>=?',
                          (channel,time.time()-7*86400)).fetchall()
        row = db.execute('SELECT checked,payload,error FROM api_rescue_channels WHERE channel_id=?',
                         (channel,)).fetchone()
    if rows:
        return [json.loads(r['payload']) for r in rows]
    if not row or row['error'] or not row['payload'] or not 0 <= time.time()-row['checked'] <= 2100:
        return None
    return json.loads(row['payload'])


def run(channels):
    import config
    from sheets import format_timestamp
    keys = config.YOUTUBE_API_KEYS or ([config.YOUTUBE_API_KEY] if config.YOUTUBE_API_KEY else [])
    if not keys:
        # Cached inventories and hot scans do not call load_inventory/load_settings.
        try:
            from sheets import authenticate_google_sheets, load_settings
            client = authenticate_google_sheets()
            load_settings(client.open_by_key(config.SPREADSHEET_ID))
        except Exception as exc:
            print('API_RESCUE_CONFIGURATION_' + type(exc).__name__, flush=True)
            return
        keys = config.YOUTUBE_API_KEYS or ([config.YOUTUBE_API_KEY] if config.YOUTUBE_API_KEY else [])
    if not keys:
        print('API_RESCUE_NO_KEY', flush=True)
        return
    if not channels or not claim_pass():
        print('API_RESCUE_THROTTLED', flush=True)
        return
    # At most 80 uploads reads + two mapping reads per half-hour, within 4000/day.
    selected = select_channels(channels, 80)
    recovered = 0
    for row in selected:
        channel = row['channel_id']
        with database() as db:
            db.execute('UPDATE api_rescue_channels SET attempted=? WHERE channel_id=?', (time.time(),channel))
        try:
            uploads = row['uploads']
            if not uploads and not row.get('_mapped'):
                batch = [r['channel_id'] for r in selected if not r['uploads']][:50]
                items = request('channels', {'part':'contentDetails','id':','.join(batch)}, keys[0])
                found = {i['id']:i['contentDetails']['relatedPlaylists']['uploads'] for i in items}
                with database() as db:
                    for r in selected:
                        if r['channel_id'] in batch:
                            r['_mapped'] = True
                        if r['channel_id'] in found:
                            r['uploads'] = found[r['channel_id']]
                            db.execute('UPDATE api_rescue_channels SET uploads=? WHERE channel_id=?',
                                       (r['uploads'],r['channel_id']))
                uploads = row['uploads']
            if not uploads:
                raise RuntimeError('API_CHANNEL_UNAVAILABLE')
            videos = []
            cutoff = datetime.now(timezone.utc)-timedelta(hours=config.RSS_FALLBACK_AGE_HOURS)
            def consume(items):
                page_videos = []
                for item in items:
                    details, snippet = item.get('contentDetails',{}), item.get('snippet',{})
                    published = details.get('videoPublishedAt')
                    if not published or not details.get('videoId'):
                        continue
                    stamp = datetime.fromisoformat(published.replace('Z','+00:00'))
                    page_videos.append({'video_id':details['videoId'], 'channel_id':channel,
                        'title':snippet.get('title',''), 'channel':snippet.get('channelTitle',''),
                        'url':'https://www.youtube.com/watch?v='+details['videoId'],
                        'published':format_timestamp(stamp), 'discovery_method':'YouTube API backup'})
                with database() as db:
                    db.executemany('INSERT OR IGNORE INTO api_rescue_videos VALUES (?,?,?,?)',
                                   [(channel,v['video_id'],json.dumps(v),time.time()) for v in page_videos])
                    for video in page_videos:
                        queue_event(video['video_id'], channel, source='YouTube API backup', connection=db)
                videos.extend(page_videos)
            complete = scan_uploads({**row, 'uploads': uploads}, keys[0], cutoff, consume)
            with database() as db:
                db.execute('DELETE FROM api_rescue_videos WHERE received<?', (time.time()-7*86400,))
                db.execute("UPDATE api_rescue_channels SET checked=?,payload=?,error=? WHERE channel_id=?",
                           (time.time(),json.dumps(videos),'' if complete else 'API_RESCUE_HISTORY_PENDING',channel))
            recovered += int(complete)
        except BudgetExhausted as exc:
            print(str(exc),flush=True)
            break
        except UpstreamError as exc:
            # Keep useful API status without persisting request URLs or credentials.
            label = 'API_RESCUE_HTTP_' + str(exc.status)
            if 'playlistNotFound' in exc.reasons:
                label += '_playlistNotFound'
            with database() as db:
                db.execute('UPDATE api_rescue_channels SET error=? WHERE channel_id=?', (label, channel))
        except Exception as exc:
            # Never log requests exceptions: their URL can contain the API key.
            with database() as db:
                db.execute('UPDATE api_rescue_channels SET error=? WHERE channel_id=?',
                           (type(exc).__name__,channel))
    print(f'API_RESCUE checked={len(selected)} recovered={recovered}',flush=True)
