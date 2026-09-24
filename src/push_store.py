"""Durable WebSub ingress, confirmed leases, and bounded Sheets mirroring."""
import hashlib
import hmac
import os
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from contextlib import contextmanager


@contextmanager
def database():
    db = sqlite3.connect(os.environ.get('TOPUS_PUSH_DB', '/data/push.sqlite3'), timeout=15)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.executescript('''
      CREATE TABLE IF NOT EXISTS events (
        fingerprint TEXT PRIMARY KEY, video_id TEXT NOT NULL, channel_id TEXT NOT NULL,
        received REAL NOT NULL, mirrored INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS leases (
        channel_id TEXT PRIMARY KEY, requested REAL NOT NULL DEFAULT 0,
        verified REAL NOT NULL DEFAULT 0, expires REAL NOT NULL DEFAULT 0,
        error TEXT NOT NULL DEFAULT '', enabled INTEGER NOT NULL DEFAULT 1);
      CREATE TABLE IF NOT EXISTS jobs (
        name TEXT PRIMARY KEY, started REAL, completed REAL, success REAL,
        error TEXT NOT NULL DEFAULT '');
      CREATE TABLE IF NOT EXISTS callback_health (
        id INTEGER PRIMARY KEY CHECK (id=1), requests INTEGER NOT NULL DEFAULT 0,
        accepted INTEGER NOT NULL DEFAULT 0, rejected INTEGER NOT NULL DEFAULT 0,
        entries INTEGER NOT NULL DEFAULT 0, new_events INTEGER NOT NULL DEFAULT 0,
        ignored_unsubscribed INTEGER NOT NULL DEFAULT 0, duplicate_events INTEGER NOT NULL DEFAULT 0,
        invalid_entries INTEGER NOT NULL DEFAULT 0, last_received REAL,
        last_accepted REAL, last_rejected REAL, last_rejection_code TEXT NOT NULL DEFAULT '');
      CREATE TABLE IF NOT EXISTS rss_hotset (
        channel_id TEXT PRIMARY KEY, last_video REAL NOT NULL, last_checked REAL NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS rss_push_gaps (
        video_id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, first_seen REAL NOT NULL,
        push_received REAL NOT NULL DEFAULT 0);
    ''')
    try:
        yield db
        db.commit()
    finally:
        db.close()


def verify_key(channel_id):
    return hmac.new(os.environ['TOPUS_HUB_SECRET'].encode(), channel_id.encode(), hashlib.sha256).hexdigest()


def confirm(channel_id, supplied, lease):
    if not re.fullmatch(r'UC[\w-]{22}', channel_id) or not hmac.compare_digest(verify_key(channel_id), supplied):
        return False
    with database() as db:
        row = db.execute('SELECT requested,enabled FROM leases WHERE channel_id=?', (channel_id,)).fetchone()
        if not row or not row['enabled'] or time.time() - row['requested'] > 3600:
            return False
        db.execute("UPDATE leases SET verified=?,expires=?,error='' WHERE channel_id=?",
                   (time.time(), time.time() + min(lease, 864000), channel_id))
    return True


def accept_xml(body, signature):
    algorithm, _, digest = signature.partition('=')
    if algorithm not in ('sha1', 'sha256', 'sha384', 'sha512'):
        raise PermissionError('Missing or unsupported signature')
    expected = hmac.new(os.environ['TOPUS_HUB_SECRET'].encode(), body, algorithm).hexdigest()
    if not hmac.compare_digest(expected, digest):
        raise PermissionError('Signature mismatch')
    if len(body) > 1024 * 1024 or b'<!DOCTYPE' in body.upper() or b'<!ENTITY' in body.upper():
        raise ValueError('Invalid XML')
    root = ET.fromstring(body)
    ns = {'a': 'http://www.w3.org/2005/Atom', 'y': 'http://www.youtube.com/xml/schemas/2015'}
    entries = root.findall('a:entry', ns)
    if root.tag == '{http://www.w3.org/2005/Atom}entry':
        entries = [root]
    counts = {'entries': 0, 'new_events': 0, 'ignored_unsubscribed': 0,
              'duplicate_events': 0, 'invalid_entries': 0}
    with database() as db:
        for entry in entries:
            counts['entries'] += 1
            vid = entry.findtext('y:videoId', '', ns)
            channel = entry.findtext('y:channelId', '', ns)
            if not re.fullmatch(r'[\w-]{11}', vid):
                counts['invalid_entries'] += 1
                continue
            known = db.execute('SELECT enabled FROM leases WHERE channel_id=?', (channel,)).fetchone()
            if not known or not known['enabled']:
                counts['ignored_unsubscribed'] += 1
                continue
            updated = entry.findtext('a:updated', '', ns)
            fingerprint = hashlib.sha256(f'{channel}:{vid}:{updated}'.encode()).hexdigest()
            cursor = db.execute('INSERT OR IGNORE INTO events(fingerprint,video_id,channel_id,received) VALUES (?,?,?,?)',
                                (fingerprint, vid, channel, time.time()))
            db.execute('UPDATE rss_push_gaps SET push_received=? '
                       'WHERE video_id=? AND channel_id=? AND push_received=0',
                       (time.time(), vid, channel))
            if cursor.rowcount:
                counts['new_events'] += 1
            else:
                counts['duplicate_events'] += 1
    return counts


def record_callback(outcome, *, counts=None, rejection_code=''):
    """Store aggregate callback health only; never persist request bodies or secrets."""
    now = time.time()
    counts = counts or {}
    with database() as db:
        db.execute('INSERT OR IGNORE INTO callback_health(id) VALUES (1)')
        if outcome == 'accepted':
            db.execute('UPDATE callback_health SET requests=requests+1, accepted=accepted+1, '
                       'entries=entries+?, new_events=new_events+?, '
                       'ignored_unsubscribed=ignored_unsubscribed+?, '
                       'duplicate_events=duplicate_events+?, invalid_entries=invalid_entries+?, '
                       'last_received=?, last_accepted=? WHERE id=1',
                       tuple(max(0, int(counts.get(key, 0))) for key in (
                           'entries', 'new_events', 'ignored_unsubscribed',
                           'duplicate_events', 'invalid_entries')) + (now, now))
        else:
            db.execute('UPDATE callback_health SET requests=requests+1, rejected=rejected+1, '
                       'last_received=?, last_rejected=?, last_rejection_code=? WHERE id=1',
                       (now, now, str(rejection_code)[:40]))


def mirror_events(sheet):
    from datetime import datetime, timezone
    from sheets import format_timestamp, channel_link
    import config
    from control_client import configured, ControlClient
    with database() as db:
        events = [dict(r) for r in db.execute('SELECT * FROM events WHERE mirrored=0 ORDER BY received LIMIT 100')]
    local_events = events
    control = ControlClient() if configured() else None
    if control:
        for start in range(0, len(local_events), 25):
            control.request('/events', {'events': [{**e, 'event_id': e['fingerprint'], 'source': 'server'}
                                                  for e in local_events[start:start + 25]]})
        events = control.request('/events/pending', {}).get('events', [])
    if not events:
        return
    worksheet = sheet.worksheet(config.SHEET_NAME_PUSH_EVENTS)
    values = worksheet.get_all_values()
    headers = values[0]
    # Retry an uncertain append by checking its durable video/channel key first.
    vi, ci = headers.index('Video ID'), headers.index('Ссылка на канал')
    existing = {(r[vi], r[ci]) for r in values[1:] if len(r) > max(vi, ci)}
    rows = []
    for event in events:
        key = (event['video_id'], channel_link(event['channel_id']))
        if key not in existing:
            record = {'Timestamp GMT+4': format_timestamp(datetime.fromtimestamp(event['received'], timezone.utc)),
                      'Video ID': key[0], 'Ссылка на канал': key[1], 'Обработано': '❌', 'Проекты': ''}
            rows.append([record.get(h, '') for h in headers])
            existing.add(key)
    if rows:
        worksheet.append_rows(rows, value_input_option='USER_ENTERED')
    if control:
        for start in range(0, len(events), 25):
            control.request('/events/ack', {'owner': os.environ['TOPUS_PUBLISHER_OWNER'],
                'lease': os.environ['TOPUS_PUBLISHER_LEASE'], 'keys': [e['key'] for e in events[start:start + 25]]})
    with database() as db:
        db.executemany('UPDATE events SET mirrored=1 WHERE fingerprint=?', [(e['fingerprint'],) for e in local_events])
        db.execute('DELETE FROM events WHERE mirrored=1 AND received<?', (time.time() - 7 * 86400,))


def health():
    with database() as db:
        callback = db.execute('SELECT * FROM callback_health WHERE id=1').fetchone()
        return {
            'pending_ingress': db.execute('SELECT count(*) FROM events WHERE mirrored=0').fetchone()[0],
            'last_push_at': db.execute('SELECT max(received) FROM events').fetchone()[0],
            'callbacks': dict(callback) if callback else {
                'requests': 0, 'accepted': 0, 'rejected': 0, 'entries': 0,
                'new_events': 0, 'ignored_unsubscribed': 0,
                'duplicate_events': 0, 'invalid_entries': 0,
                'last_received': None, 'last_accepted': None, 'last_rejected': None,
                'last_rejection_code': ''},
            'subscriptions': dict(db.execute('SELECT count(*) total, sum(expires>?) verified_active, '
                                            'sum(expires<=?) expired FROM leases WHERE enabled=1',
                                            (time.time(), time.time())).fetchone()),
            'jobs': {r['name']: dict(r) for r in db.execute('SELECT * FROM jobs')},
        }
