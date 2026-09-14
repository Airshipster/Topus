import os

import requests
import json
import time
from delivery_journal import connection


def worker_message_id(result):
    deliveries = result.get('deliveries') if isinstance(result, dict) else None
    if not deliveries:
        queued = result.get('queued', 0) if isinstance(result, dict) else 0
        return f'bot:{queued}'

    ids = [
        str(delivery.get('messageId'))
        for delivery in deliveries
        if delivery.get('messageId') is not None
    ]
    if not ids:
        return 'bot:0'
    if len(ids) == 1:
        return ids[0]
    return ','.join(ids)


def notify_worker_subscribers(project, video, message):
    worker_url = os.environ.get('TOPUS_WORKER_URL', '').strip()
    admin_secret = os.environ.get('TOPUS_WORKER_ADMIN_SECRET', '').strip()
    project_code = str(project.get('code') or '').strip()
    channel_id = str(video.get('channel_id') or '').strip()

    routed_projects = {v.strip() for v in os.environ.get('TOPUS_WORKER_PROJECTS', 'SciTopus').split(',')}
    if project_code not in routed_projects:
        return None

    if not worker_url or not admin_secret or not project_code or not channel_id:
        return None

    from sheets import parse_datetime_value
    youtube_published = parse_datetime_value(video.get('published'))
    payload = {
        'projectCode': project_code, 'channelId': channel_id,
        'videoId': str(video.get('video_id') or video.get('videoId') or '').strip(),
        'text': message, 'parseMode': 'HTML',
        'youtubePublishedAt': youtube_published.isoformat() if youtube_published else None,
    }
    key = json.dumps([project_code, payload['videoId']], separators=(',', ':'))
    from control_client import configured, ControlClient
    if configured():
        from sheets import parse_datetime_value
        published = parse_datetime_value(video.get('live_actual_end') or video.get('published'))
        control = ControlClient()
        queued = control.request('/notifications/put', {'owner': os.environ['TOPUS_PUBLISHER_OWNER'],
            'lease': os.environ['TOPUS_PUBLISHER_LEASE'], 'payload': payload,
            'published_at': published.timestamp() if published else None})
        if queued['state'] == 'sent':
            return queued['result']
        return deliver_remote(control, queued['key'], worker_url, admin_secret)
    with connection() as db:
        db.execute('CREATE TABLE IF NOT EXISTS notify_outbox (key TEXT PRIMARY KEY,payload TEXT NOT NULL,sent INTEGER DEFAULT 0,updated REAL DEFAULT 0,error TEXT)')
        db.execute('INSERT OR IGNORE INTO notify_outbox(key,payload) VALUES (?,?)', (key, json.dumps(payload)))
        row = db.execute('SELECT sent FROM notify_outbox WHERE key=?', (key,)).fetchone()
        if row['sent']:
            return {'ok': True, 'queued': 0, 'sent': 0, 'deduplicated': True}
    return deliver_outbox(key, payload, worker_url, admin_secret)


def deliver_outbox(key, payload, worker_url, admin_secret):
    try:
        response = requests.post(
            worker_url.rstrip('/') + '/admin/notify',
            json=payload,
            headers={'x-admin-secret': admin_secret},
            timeout=(10, 60),
        )
        response.raise_for_status()
        result = response.json()
        if not result.get('ok'):
            raise RuntimeError('Partial personal delivery')
        with connection() as db:
            db.execute('UPDATE notify_outbox SET sent=1,updated=?,error=NULL WHERE key=?', (time.time(), key))
        return result
    except Exception as e:
        with connection() as db:
            db.execute('UPDATE notify_outbox SET updated=?,error=? WHERE key=?', (time.time(), type(e).__name__, key))
        print(f"    Worker notification pending retry: {type(e).__name__}")
        return None


def retry_outbox():
    from control_client import configured, ControlClient
    if configured():
        control = ControlClient()
        items = control.request('/notifications/pending', {}).get('items', [])
        failed = 0
        for item in items:
            if deliver_remote(control, item['key'], os.environ['TOPUS_WORKER_URL'], os.environ['TOPUS_WORKER_ADMIN_SECRET']) is None:
                failed += 1
        control.heartbeat('personal', failed == 0, 'PERSONAL_RETRY_PENDING' if failed else '')
        if failed:
            raise RuntimeError('Personal notification retries pending: ' + str(failed))
        return
    with connection() as db:
        db.execute('CREATE TABLE IF NOT EXISTS notify_outbox (key TEXT PRIMARY KEY,payload TEXT NOT NULL,sent INTEGER DEFAULT 0,updated REAL DEFAULT 0,error TEXT)')
        rows = list(db.execute('SELECT key,payload FROM notify_outbox WHERE sent=0 AND updated<? ORDER BY updated LIMIT 5', (time.time()-120,)))
    failed = 0
    for row in rows:
        if deliver_outbox(row['key'], json.loads(row['payload']), os.environ['TOPUS_WORKER_URL'], os.environ['TOPUS_WORKER_ADMIN_SECRET']) is None:
            failed += 1
    if failed:
        raise RuntimeError(f'Personal notification retries failed: {failed}')


def deliver_remote(control, key, worker_url, admin_secret):
    claim = control.request('/notifications/claim', {'key': key})
    if not claim.get('claim'):
        return None
    result = None
    error = ''
    try:
        response = requests.post(worker_url.rstrip('/') + '/admin/notify', json=claim['payload'],
            headers={'x-admin-secret': admin_secret, 'User-Agent': 'Topus-Control/1.0'},
            timeout=(5, 60), allow_redirects=False)
        if response.status_code != 200:
            raise RuntimeError('Personal endpoint rejected request')
        result = response.json()
        if not result.get('ok'):
            raise RuntimeError('Partial personal delivery')
    except Exception as exc:
        error = type(exc).__name__
    control.request('/notifications/finish', {'key': key, 'claim': claim['claim'], 'ok': not error,
        'queued': result.get('queued', 0) if result else 0, 'sent': result.get('sent', 0) if result else 0, 'error': error})
    return result if not error else None


if __name__ == '__main__':
    retry_outbox()
