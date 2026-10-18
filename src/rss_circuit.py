"""Bound repeated all-404 RSS outages without turning them into channel deletions."""
import time

from push_store import database

PROBE_SIZE = 12
MIN_INVENTORY = 64
RETRY_SECONDS = 1800
PROBE_LEASE_SECONDS = 90


def initialize():
    with database() as db:
        db.execute('CREATE TABLE IF NOT EXISTS rss_upstream_circuit ('
                   'id INTEGER PRIMARY KEY CHECK (id=1), blocked_until REAL NOT NULL, '
                   'probe_until REAL NOT NULL DEFAULT 0)')


def claim(now=None):
    initialize()
    current = time.time() if now is None else now
    with database() as db:
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT * FROM rss_upstream_circuit WHERE id=1').fetchone()
        if not row:
            return 'normal'
        if max(row['blocked_until'], row['probe_until']) > current:
            return 'skip'
        db.execute('UPDATE rss_upstream_circuit SET probe_until=? WHERE id=1',
                   (current + PROBE_LEASE_SECONDS,))
        return 'probe'


def is_open(now=None):
    initialize()
    current = time.time() if now is None else now
    with database() as db:
        row = db.execute('SELECT blocked_until,probe_until FROM rss_upstream_circuit WHERE id=1').fetchone()
    return bool(row and max(row['blocked_until'], row['probe_until']) > current)


def defer(now=None):
    initialize()
    current = time.time() if now is None else now
    with database() as db:
        db.execute('INSERT INTO rss_upstream_circuit VALUES (1,?,0) '
                   'ON CONFLICT(id) DO UPDATE SET blocked_until=excluded.blocked_until,probe_until=0',
                   (current + RETRY_SECONDS,))


def recover():
    initialize()
    with database() as db:
        db.execute('DELETE FROM rss_upstream_circuit WHERE id=1')


def ordered_channels(channels, now=None):
    values = sorted(channels)
    if not values:
        return values
    current = time.time() if now is None else now
    offset = (int(current // RETRY_SECONDS) * PROBE_SIZE) % len(values)
    return values[offset:] + values[:offset]
