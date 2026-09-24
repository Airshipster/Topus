"""Collect RSS without acquiring the publication lease; persist results atomically."""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from push_store import database

HOT_RETENTION_SECONDS = 7 * 86400
PUSH_GRACE_SECONDS = 10 * 60


def initialize():
    with database() as db:
        db.execute('CREATE TABLE IF NOT EXISTS rss_discovery ('
                   'channel_id TEXT PRIMARY KEY, checked REAL NOT NULL, '
                   'payload TEXT NOT NULL, error TEXT NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS rss_hotset ('
                   'channel_id TEXT PRIMARY KEY, last_video REAL NOT NULL, '
                   'last_checked REAL NOT NULL DEFAULT 0)')
        db.execute('CREATE TABLE IF NOT EXISTS rss_push_gaps ('
                   'video_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, '
                   'first_seen REAL NOT NULL, push_received REAL NOT NULL DEFAULT 0)')


def _video_ids(videos):
    return {video.get('video_id') for video in videos or []
            if isinstance(video, dict) and video.get('video_id')}


def save_result(channel, videos, error, now=None, track_push_gap=False):
    initialize()
    checked = time.time() if now is None else now
    with database() as db:
        previous_row = db.execute(
            'SELECT payload FROM rss_discovery WHERE channel_id=?', (channel,)
        ).fetchone()
        try:
            previous = json.loads(previous_row['payload']) if previous_row else []
        except (TypeError, ValueError):
            previous = []
        db.execute('INSERT INTO rss_discovery VALUES (?,?,?,?) ON CONFLICT(channel_id) '
                   'DO UPDATE SET checked=excluded.checked,payload=excluded.payload,error=excluded.error',
                   (channel, checked, json.dumps(videos), error))
        if not error and isinstance(videos, list) and videos:
            db.execute('INSERT INTO rss_hotset(channel_id,last_video,last_checked) VALUES (?,?,?) '
                       'ON CONFLICT(channel_id) DO UPDATE SET '
                       'last_video=excluded.last_video,last_checked=excluded.last_checked',
                       (channel, checked, checked))
        elif not error:
            db.execute('UPDATE rss_hotset SET last_checked=? WHERE channel_id=?',
                       (checked, channel))
        if track_push_gap and not error and isinstance(videos, list):
            new_ids = _video_ids(videos) - _video_ids(previous)
            for video in videos:
                video_id = video.get('video_id') if isinstance(video, dict) else None
                if video_id not in new_ids:
                    continue
                push_row = db.execute(
                    'SELECT max(received) received FROM events WHERE video_id=? AND channel_id=?',
                    (video_id, channel),
                ).fetchone()
                push_received = float(push_row['received'] or 0)
                db.execute('INSERT OR IGNORE INTO rss_push_gaps('
                           'video_id,channel_id,first_seen,push_received) VALUES (?,?,?,?)',
                           (video_id, channel, checked, push_received))


def hot_channels(now=None):
    initialize()
    cutoff = (time.time() if now is None else now) - HOT_RETENTION_SECONDS
    with database() as db:
        return {row['channel_id'] for row in db.execute(
            'SELECT h.channel_id FROM rss_hotset h JOIN leases l ON l.channel_id=h.channel_id '
            'WHERE h.last_video>=? AND l.enabled=1', (cutoff,)
        )}


def push_gap_health(now=None):
    initialize()
    current = time.time() if now is None else now
    with database() as db:
        db.execute('DELETE FROM rss_push_gaps WHERE first_seen<?',
                   (current - HOT_RETENTION_SECONDS,))
        row = db.execute(
            'SELECT count(*) total, min(first_seen) oldest FROM rss_push_gaps '
            'WHERE push_received=0 AND first_seen<=?',
            (current - PUSH_GRACE_SECONDS,),
        ).fetchone()
    return {'open': int(row['total'] or 0), 'oldest': row['oldest'],
            'grace_seconds': PUSH_GRACE_SECONDS}


def report_push_gap_health():
    from control_client import configured, ControlClient
    result = push_gap_health()
    if configured():
        try:
            ControlClient().request('/incident', {
                'kind': 'push-delivery-gap',
                'active': bool(result['open']),
                'summary': ('Push не доставил события для видео, найденных резервным RSS: '
                            f"{result['open']}" if result['open'] else
                            'Доставка Push снова подтверждается событиями.'),
            })
        except Exception as exc:
            result['report_error'] = type(exc).__name__
    return result


def read_result(channel, now=None):
    initialize()
    with database() as db:
        row = db.execute('SELECT * FROM rss_discovery WHERE channel_id=?', (channel,)).fetchone()
    if row is None:
        return None, 'DISCOVERY_PENDING'
    age = (time.time() if now is None else now) - row['checked']
    if age < -5 or age > 2100:
        return None, 'DISCOVERY_STALE'
    if row['error']:
        return None, row['error']
    value = json.loads(row['payload'])
    if not isinstance(value, list):
        return None, 'DISCOVERY_INVALID'
    return value, ''


def run():
    import config
    from sheets import authenticate_google_sheets, load_settings, load_projects, load_youtube_channels
    from rss import check_rss_feed, failure_reasons
    mode = os.environ.get('TOPUS_RSS_DISCOVERY_MODE', 'full').strip().lower()
    if mode == 'hot':
        channels = hot_channels()
    else:
        client = authenticate_google_sheets()
        sheet = client.open_by_key(config.SPREADSHEET_ID)
        load_settings(sheet)
        projects = load_projects(sheet, update_status=False)
        channels = set()
        for project in projects:
            if project.get('rss_feed_enabled', True):
                channels.update(load_youtube_channels(client, project,
                                include_disabled=bool(project.get('bot_enabled'))))
        if not channels:
            raise RuntimeError('RSS_DISCOVERY_INVENTORY_EMPTY')
        from channel_availability import check as check_availability
        check_availability(channels)
    initialize()
    if not channels:
        print('RSS_DISCOVERY mode=hot completed=0 failed=0', flush=True)
        report_push_gap_health()
        return
    failures = 0
    failed = set()
    with ThreadPoolExecutor(max_workers=max(1, min(12, int(config.RSS_WORKERS)))) as pool:
        futures = {pool.submit(check_rss_feed, channel): channel for channel in channels}
        for future in as_completed(futures):
            channel = futures[future]
            try:
                videos = future.result()
                error = failure_reasons.get(channel, 'RSS_DISCOVERY_FAILED') if videos is None else ''
            except Exception as exc:
                videos, error = None, type(exc).__name__
            save_result(channel, videos, error, track_push_gap=(mode == 'hot'))
            failures += bool(error)
            if error:
                failed.add(channel)
    if failed:
        from api_rescue import run as rescue
        rescue(failed)
    with database() as db:
        db.execute('DELETE FROM rss_discovery WHERE checked<?', (time.time()-7*86400,))
    if mode == 'hot':
        report_push_gap_health()
    print(f'RSS_DISCOVERY mode={mode} completed={len(channels)} failed={failures}', flush=True)
    if failures:
        raise RuntimeError(f'RSS_DISCOVERY_FAILED_{failures}')


if __name__ == '__main__':
    run()
