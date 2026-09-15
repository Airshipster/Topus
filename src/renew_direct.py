"""Bounded WebSub maintenance, independent of the publisher lock."""
import os
import json
import time
from concurrent.futures import ThreadPoolExecutor
import requests
import config
from push_store import database, verify_key
from sheets import authenticate_google_sheets, load_settings, load_projects, get_all_active_channels, format_timestamp


def reconcile_inventory(channels, complete):
    if not channels:
        raise RuntimeError('Subscription inventory incomplete; keeping existing leases')
    with database() as db:
        if complete:
            db.execute('UPDATE leases SET enabled=0')
        db.executemany('INSERT INTO leases(channel_id) VALUES (?) ON CONFLICT(channel_id) DO UPDATE SET enabled=1',
                       [(c,) for c in channels])


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
        due = [r['channel_id'] for r in db.execute('SELECT channel_id FROM leases WHERE enabled=1 '
                    'AND expires<? AND requested<? ORDER BY requested,channel_id LIMIT 100',
                    (time.time() + 86400, time.time() - 300)) if r['channel_id'] in channels]
        db.executemany("UPDATE leases SET requested=?,error='awaiting verification' WHERE channel_id=?",
                       [(time.time(), c) for c in due])

    def renew(channel):
        try:
            response = requests.post('https://pubsubhubbub.appspot.com/subscribe', data={
                'hub.callback': callback + '?verify=' + verify_key(channel),
                'hub.topic': 'https://www.youtube.com/xml/feeds/videos.xml?channel_id=' + channel,
                'hub.mode': 'subscribe', 'hub.verify': 'async', 'hub.lease_seconds': '432000',
                'hub.secret': secret,
            }, timeout=(5, 12))
            error = '' if response.status_code in (202, 204) else f'HTTP {response.status_code}'
        except requests.RequestException as exc:
            error = type(exc).__name__
        if error:
            with database() as db:
                db.execute('UPDATE leases SET error=? WHERE channel_id=?', (error, channel))
        return not error

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(renew, due))
    worksheet = sheet.worksheet('Подписки')
    values = worksheet.get_all_values()
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
        if lease['verified'] and lease['expires'] > time.time():
            status = '✅ server lease verified'
            renewed = format_timestamp(datetime.fromtimestamp(lease['verified'], timezone.utc))
            if len(row) <= ri or row[ri] != renewed:
                updates.append({'range': gspread.utils.rowcol_to_a1(n, ri+1), 'values': [[renewed]]})
        else:
            status = '⚠️ server: ' + (lease['error'] or 'renewal queued')
        if len(row) <= si or row[si] != status:
            updates.append({'range': gspread.utils.rowcol_to_a1(n, si+1), 'values': [[status]]})
    if updates:
        worksheet.batch_update(updates, value_input_option='USER_ENTERED')
    new_rows = []
    for channel in channels.keys() - seen:
        names = channels[channel].get('projects', [])
        record = {'Projects': ', '.join(names), 'Project Count': len(names), 'Channel ID': channel,
                  'Subscribed At': format_timestamp(), 'Last Renewed': '', 'Status': '⚠️ server: renewal queued'}
        new_rows.append([record.get(h, '') for h in headers])
    if new_rows:
        worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
    print(f'Subscription batch: requested={len(due)}, accepted={sum(results)}, verified={sum(r["expires"]>time.time() for r in leases.values())}', flush=True)
    if results and not any(results):
        raise RuntimeError('All subscription requests failed')
    if incomplete:
        raise RuntimeError('Partial subscription inventory; accessible channels renewed, existing leases retained')


if __name__ == '__main__':
    run()
