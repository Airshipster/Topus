"""Unavailable to the official API is distinct from a failed Push subscription."""
import time
from push_store import database


def initialize():
    with database() as db:
        db.execute('CREATE TABLE IF NOT EXISTS channel_availability ('
                   'channel_id TEXT PRIMARY KEY, checked REAL NOT NULL, missing_since REAL NOT NULL, '
                   'missing_count INTEGER NOT NULL)')


def record(channel, available, now):
    initialize()
    with database() as db:
        previous = db.execute('SELECT * FROM channel_availability WHERE channel_id=?', (channel,)).fetchone()
        if previous and now - previous['checked'] < 86400 and not available:
            return
        since = 0 if available else (previous['missing_since'] if previous and previous['missing_since'] else now)
        count = 0 if available else (previous['missing_count'] + 1 if previous else 1)
        db.execute('INSERT OR REPLACE INTO channel_availability VALUES (?,?,?,?)',
                   (channel, now, since, count))


def unavailable(now=None):
    initialize()
    now = time.time() if now is None else now
    with database() as db:
        return {r['channel_id'] for r in db.execute(
            'SELECT channel_id FROM channel_availability WHERE missing_count>=2 '
            'AND missing_since<=? AND checked>=?', (now - 86400, now - 172800))}


def check(channels):
    import config
    from api_rescue import request
    initialize()
    keys = config.YOUTUBE_API_KEYS or ([config.YOUTUBE_API_KEY] if config.YOUTUBE_API_KEY else [])
    if not keys:
        return
    now = time.time()
    with database() as db:
        checked = {r['channel_id']: r['checked'] for r in db.execute('SELECT channel_id,checked FROM channel_availability')}
    due = sorted((c for c in channels if now - checked.get(c, 0) >= 86400), key=lambda c: checked.get(c, 0))
    deadline = time.monotonic() + 45
    for start in range(0, len(due), 50):
        if time.monotonic() >= deadline:
            break
        batch = due[start:start + 50]
        try:
            items = request('channels', {'part': 'id', 'id': ','.join(batch)}, keys[0])
            found = {item['id'] for item in items}
            if not found.issubset(set(batch)):
                raise ValueError('UNEXPECTED_CHANNEL_RESPONSE')
        except Exception as exc:
            print('CHANNEL_AVAILABILITY_UNKNOWN: ' + type(exc).__name__, flush=True)
            break
        for channel in batch:
            record(channel, channel in found, now)
    print('CHANNEL_AVAILABILITY confirmed=' + str(len(unavailable())), flush=True)
