"""Quota-bounded official API fallback. Never treats RSS failure as recovery."""
import json
import os
import time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import requests
from push_store import database


class BudgetExhausted(RuntimeError):
    pass


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
        raise RuntimeError('API_RESCUE_HTTP_'+str(response.status_code))
    expected = {'channels':'youtube#channelListResponse', 'playlistItems':'youtube#playlistItemListResponse'}
    if body.get('kind') != expected[endpoint] or not isinstance(body.get('items'),list):
        raise ValueError('API_RESCUE_INVALID_RESPONSE')
    return body['items']


def select_channels(channels, limit):
    initialize()
    with database() as db:
        db.executemany('INSERT OR IGNORE INTO api_rescue_channels(channel_id) VALUES (?)',
                       [(c,) for c in channels])
        rows = db.execute('SELECT * FROM api_rescue_channels ORDER BY attempted,channel_id').fetchall()
    return [dict(r) for r in rows if r['channel_id'] in channels][:limit]


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
        print('API_RESCUE_NO_KEY', flush=True)
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
            items = request('playlistItems', {'part':'snippet,contentDetails','playlistId':uploads,
                                             'maxResults':50}, keys[0])
            videos = []
            cutoff = datetime.now(timezone.utc)-timedelta(hours=config.RSS_FALLBACK_AGE_HOURS)
            for item in items:
                details, snippet = item.get('contentDetails',{}), item.get('snippet',{})
                published = details.get('videoPublishedAt')
                if not published or not details.get('videoId'):
                    continue
                stamp = datetime.fromisoformat(published.replace('Z','+00:00'))
                if stamp < cutoff:
                    continue
                videos.append({'video_id':details['videoId'], 'channel_id':channel,
                    'title':snippet.get('title',''), 'channel':snippet.get('channelTitle',''),
                    'url':'https://www.youtube.com/watch?v='+details['videoId'],
                    'published':format_timestamp(stamp), 'discovery_method':'YouTube API backup'})
            with database() as db:
                db.executemany('INSERT OR IGNORE INTO api_rescue_videos VALUES (?,?,?,?)',
                               [(channel,v['video_id'],json.dumps(v),time.time()) for v in videos])
                db.execute('DELETE FROM api_rescue_videos WHERE received<?', (time.time()-7*86400,))
                db.execute("UPDATE api_rescue_channels SET checked=?,payload=?,error='' WHERE channel_id=?",
                           (time.time(),json.dumps(videos),channel))
            recovered += 1
        except BudgetExhausted as exc:
            print(str(exc),flush=True)
            break
        except Exception as exc:
            # Never log requests exceptions: their URL can contain the API key.
            with database() as db:
                db.execute('UPDATE api_rescue_channels SET error=? WHERE channel_id=?',
                           (type(exc).__name__,channel))
    print(f'API_RESCUE checked={len(selected)} recovered={recovered}',flush=True)
