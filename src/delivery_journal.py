"""Durable send receipts. An unknown Telegram outcome is never blindly retried."""
import json
import os
import sqlite3
import time
from contextlib import contextmanager


def path():
    return os.environ.get('TOPUS_DELIVERY_DB', '/data/publication.sqlite3')


@contextmanager
def connection():
    db = sqlite3.connect(path(), timeout=20)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.execute('CREATE TABLE IF NOT EXISTS deliveries (key TEXT PRIMARY KEY, state TEXT NOT NULL, '
               'message_id TEXT, updated REAL NOT NULL, error TEXT, attempts INTEGER NOT NULL DEFAULT 0)')
    try:
        yield db
        db.commit()
    finally:
        db.close()


def claim(key):
    with connection() as db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT * FROM deliveries WHERE key=?', (key,)).fetchone()
        if row and row['state'] in ('sent', 'sending', 'uncertain'):
            return dict(row)
        if row and time.time() - row['updated'] < min(3600, 30 * 2 ** min(row['attempts'], 7)):
            return dict(row)
        db.execute("INSERT INTO deliveries(key,state,updated,attempts) VALUES (?,'sending',?,1) "
                   "ON CONFLICT(key) DO UPDATE SET state='sending',updated=excluded.updated,attempts=deliveries.attempts+1",
                   (key, time.time()))
        return {'state': 'claimed'}


def finish(key, state, message_id=None, error=''):
    with connection() as db:
        db.execute('UPDATE deliveries SET state=?,message_id=?,updated=?,error=? WHERE key=?',
                   (state, str(message_id) if message_id is not None else None, time.time(), error[:200], key))


def summary():
    with connection() as db:
        return {r['state']: r['n'] for r in db.execute('SELECT state,count(*) n FROM deliveries GROUP BY state')}


def send_public(bot_token, channel_id, message, project, video_id, published_at=None):
    from control_client import configured, ControlClient
    if configured():
        return ControlClient().send(project, video_id, channel_id, message, published_at)
    import requests
    key = json.dumps(['public', project, str(channel_id), video_id], separators=(',', ':'))
    receipt = claim(key)
    if receipt['state'] == 'sent':
        return receipt['message_id']
    if receipt['state'] != 'claimed':
        return None
    try:
        response = requests.post(f'https://api.telegram.org/bot{bot_token}/sendMessage', json={
            'chat_id': channel_id, 'text': message, 'parse_mode': 'HTML', 'disable_web_page_preview': False,
        }, timeout=(10, 30))
        body = response.json()
        message_id = body.get('result', {}).get('message_id')
        if response.ok and body.get('ok') and message_id:
            finish(key, 'sent', message_id)
            return message_id
        # A documented negative API response has no message to duplicate.
        if body.get('ok') is False and response.status_code < 500:
            finish(key, 'retry', error=f"Telegram rejected: {body.get('error_code', response.status_code)}")
        else:
            finish(key, 'uncertain', error=f'Telegram outcome unknown: HTTP {response.status_code}')
    except requests.ConnectTimeout:
        finish(key, 'retry', error='ConnectTimeout before sending')
    except Exception as exc:
        finish(key, 'uncertain', error=type(exc).__name__)
    return None
