"""Bounded WebSub maintenance, independent of the publisher lock."""
import os
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import requests
import config
from push_store import database, verify_key
from sheets import authenticate_google_sheets, load_settings, load_projects, get_all_active_channels, format_timestamp

TOPIC_PATH = '/feeds/videos.xml'
LEGACY_TOPIC_PATH = '/xml/feeds/videos.xml'

def topic_for_renewal(lease, now=None):
    now = time.time() if now is None else now
    if lease['topic_path'] == LEGACY_TOPIC_PATH and lease['expires'] < now+86400:
        return LEGACY_TOPIC_PATH
    return TOPIC_PATH


def hub_backoff(status, retry_after='', now=None):
    now = time.time() if now is None else now
    if status not in (429, 503):
        return
    try:
        delay = max(120, min(3600, int(retry_after)))
    except (ValueError, TypeError):
        delay = 600 if status == 429 else 120
    with database() as db:
        db.execute('CREATE TABLE IF NOT EXISTS hub_backoff (id INTEGER PRIMARY KEY, until REAL NOT NULL)')
        db.execute('INSERT INTO hub_backoff VALUES (1,?) ON CONFLICT(id) DO UPDATE '
                   'SET until=max(until,excluded.until)', (now + delay,))


def hub_ready(now=None):
    now = time.time() if now is None else now
    with database() as db:
        db.execute('CREATE TABLE IF NOT EXISTS hub_backoff (id INTEGER PRIMARY KEY, until REAL NOT NULL)')
        row = db.execute('SELECT until FROM hub_backoff WHERE id=1').fetchone()
    return not row or row['until'] <= now


def reconcile_inventory(channels, complete):
    if not channels:
        raise RuntimeError('Subscription inventory incomplete; keeping existing leases')
    with database() as db:
        if complete:
            db.execute('UPDATE leases SET enabled=0')
        db.executemany('INSERT INTO leases(channel_id) VALUES (?) ON CONFLICT(channel_id) DO UPDATE SET enabled=1',
                       [(c,) for c in channels])


def require_verified_coverage():
    # HTTP acceptance is not a confirmed lease; callbacks commit verification.
    with database() as db:
        missing = db.execute('SELECT count(*) FROM leases WHERE enabled=1 AND '
                             '(expires<=? OR topic_path NOT IN (?,?))',
                             (time.time(), TOPIC_PATH, LEGACY_TOPIC_PATH)).fetchone()[0]
    if missing:
        raise RuntimeError(f'WEBSUB_UNVERIFIED_{missing}')

def renewal_pause(now=None):
    now = time.time() if now is None else now
    if not hub_ready(now):
        return 120
    with database() as db:
        migrating = db.execute('SELECT 1 FROM leases WHERE enabled=1 AND topic_path!=? AND requested<? LIMIT 1',
                               (TOPIC_PATH, now-300)).fetchone()
    return 5 if migrating else 120


def run():
    callback = os.environ.get('TOPUS_WEBSUB_URL', '').strip()
    secret = os.environ.get('TOPUS_HUB_SECRET', '')
    if not callback.startswith('https://') or not secret:
        raise RuntimeError('WEBSUB_CONFIGURATION_MISSING: callback URL or signing secret')
    client = authenticate_google_sheets()
    sheet = client.open_by_key(config.SPREADSHEET_ID)
    with database() as db:
        db.execute('CREATE TABLE IF NOT EXISTS inventory_cache (id INTEGER PRIMARY KEY, updated REAL, payload TEXT)')
        cached = db.execute('SELECT updated,payload FROM inventory_cache WHERE id=1').fetchone()
    incomplete = 0
    if cached and time.time() - cached['updated'] < 3600:
        channels = json.loads(cached['payload'])
    else:
        load_settings(sheet)
        projects = load_projects(sheet, update_status=False)
        channels = get_all_active_channels(client, projects)
        incomplete = sum(bool(p.get('channels_error')) for p in projects)
        if channels and not incomplete:
            minimal = {key: {'projects': value.get('projects', [])} for key, value in channels.items()}
            with database() as db:
                db.execute('INSERT INTO inventory_cache VALUES (1,?,?) ON CONFLICT(id) DO UPDATE SET updated=excluded.updated,payload=excluded.payload',
                           (time.time(), json.dumps(minimal)))
    reconcile_inventory(channels, complete=not incomplete)
    with database() as db:
        due = [dict(r) for r in db.execute('SELECT channel_id,topic_path,expires FROM leases WHERE enabled=1 '
                    'AND (expires<? OR topic_path!=?) '
                    'AND requested<? ORDER BY (expires<?) DESC,requested,channel_id LIMIT 10',
                    (time.time() + 86400, TOPIC_PATH, time.time() - 300, time.time()+86400)) if r['channel_id'] in channels]

    pacing = threading.Lock()
    last_request = [0.0]

    def renew(lease):
        channel = lease['channel_id']
        topic_path = topic_for_renewal(lease)
        with pacing:
            if not hub_ready():
                return None
            wait = 1 - (time.monotonic() - last_request[0])
            if wait > 0:
                time.sleep(wait)
            last_request[0] = time.monotonic()
            requested = time.time()
            with database() as db:
                db.execute("UPDATE leases SET requested=?,requested_topic=?,error='awaiting verification' WHERE channel_id=?",
                           (requested, topic_path, channel))
        try:
            response = requests.post('https://pubsubhubbub.appspot.com/subscribe', data={
                'hub.callback': callback + '?verify=' + verify_key(channel),
                'hub.topic': 'https://www.youtube.com' + topic_path + '?channel_id=' + channel,
                'hub.mode': 'subscribe', 'hub.verify': 'async', 'hub.lease_seconds': '432000',
                'hub.secret': secret,
            }, timeout=(5, 30))
            hub_backoff(response.status_code, response.headers.get('Retry-After', ''))
            error = '' if response.status_code in (202, 204) else f'HTTP {response.status_code}'
        except requests.RequestException as exc:
            error = type(exc).__name__
        if error:
            with database() as db:
                db.execute('UPDATE leases SET error=? WHERE channel_id=? AND verified<requested', (error, channel))
        return not error

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(renew, due))
    worksheet = sheet.worksheet('Подписки')
    values = worksheet.get_all_values()
    from subscription_sheet import reconcile_view
    with database() as db:
        leases = {r['channel_id']: dict(r) for r in db.execute('SELECT * FROM leases')}
    values = reconcile_view(sheet, worksheet, values, leases, channels, not incomplete)
    headers = [h.splitlines()[0].strip() for h in values[0]]
    ci, ri, si = (headers.index(h) for h in ('Channel ID', 'Last Renewed', 'Status'))
    import gspread
    from sheets import channel_id_from_link
    from datetime import datetime, timezone
    with database() as db:
        leases = {r['channel_id']: dict(r) for r in db.execute('SELECT * FROM leases')}
    updates = []
    seen = set()
    for n, row in enumerate(values[1:], 2):
        channel = channel_id_from_link(row[ci]) or row[ci] if len(row) > ci else ''
        lease = leases.get(channel)
        seen.add(channel)
        if not lease:
            continue
        if lease['verified'] and lease['expires'] > time.time() and lease['topic_path'] == TOPIC_PATH:
            status = '✅ server lease verified'
            renewed = format_timestamp(datetime.fromtimestamp(lease['verified'], timezone.utc))
            if len(row) <= ri or row[ri] != renewed:
                updates.append({'range': gspread.utils.rowcol_to_a1(n, ri+1), 'values': [[renewed]]})
        else:
            status = '⚠️ server: ' + (lease['error'] or 'official topic verification queued')
            if lease['expires'] > time.time() and lease['topic_path'] == LEGACY_TOPIC_PATH:
                status = '⚠️ server: legacy lease verified; official topic pending'
        if len(row) <= si or row[si] != status:
            updates.append({'range': gspread.utils.rowcol_to_a1(n, si+1), 'values': [[status]]})
    if updates:
        worksheet.batch_update(updates, value_input_option='USER_ENTERED')
    from subscription_sheet import clear_legacy_error_fill
    clear_legacy_error_fill(sheet, worksheet, ci, len(values))
    new_rows = []
    for channel in channels.keys() - seen:
        names = channels[channel].get('projects', [])
        record = {'Projects': ', '.join(names), 'Project Count': len(names), 'Channel ID': channel,
                  'Subscribed At': format_timestamp(), 'Last Renewed': '', 'Status': '⚠️ server: renewal queued'}
        new_rows.append([record.get(h, '') for h in headers])
    if new_rows:
        worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
    attempted = [result for result in results if result is not None]
    verified = sum(r['enabled'] and r['expires']>time.time() and r['topic_path'] in (TOPIC_PATH,LEGACY_TOPIC_PATH) for r in leases.values())
    official = sum(r['enabled'] and r['expires']>time.time() and r['topic_path']==TOPIC_PATH for r in leases.values())
    print(f'Subscription batch: requested={len(attempted)}, accepted={sum(attempted)}, verified={verified}, official={official}', flush=True)
    if attempted and not any(attempted):
        raise RuntimeError('All subscription requests failed')
    if incomplete:
        raise RuntimeError('Partial subscription inventory; accessible channels renewed, existing leases retained')
    require_verified_coverage()


if __name__ == '__main__':
    run()
