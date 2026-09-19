"""Collect RSS without acquiring the publication lease; persist results atomically."""
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from push_store import database


def initialize():
    with database() as db:
        db.execute('CREATE TABLE IF NOT EXISTS rss_discovery ('
                   'channel_id TEXT PRIMARY KEY, checked REAL NOT NULL, '
                   'payload TEXT NOT NULL, error TEXT NOT NULL)')


def save_result(channel, videos, error, now=None):
    initialize()
    with database() as db:
        db.execute('INSERT INTO rss_discovery VALUES (?,?,?,?) ON CONFLICT(channel_id) '
                   'DO UPDATE SET checked=excluded.checked,payload=excluded.payload,error=excluded.error',
                   (channel, time.time() if now is None else now, json.dumps(videos), error))


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
    initialize()
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
            save_result(channel, videos, error)
            failures += bool(error)
            if error:
                failed.add(channel)
    if failed:
        from api_rescue import run as rescue
        rescue(failed)
    with database() as db:
        db.execute('DELETE FROM rss_discovery WHERE checked<?', (time.time()-7*86400,))
    print(f'RSS_DISCOVERY completed={len(channels)} failed={failures}', flush=True)
    if failures:
        raise RuntimeError(f'RSS_DISCOVERY_FAILED_{failures}')


if __name__ == '__main__':
    run()
