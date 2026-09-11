"""One request budget shared by publisher and renewal processes."""
import os
from pathlib import Path
import sqlite3
import time

from gspread.http_client import HTTPClient
from gspread.exceptions import APIError


def reserve(path, now):
    with sqlite3.connect(path, timeout=10) as db:
        db.execute('CREATE TABLE IF NOT EXISTS budget (id INTEGER PRIMARY KEY, next REAL NOT NULL)')
        db.execute('BEGIN IMMEDIATE')
        row = db.execute('SELECT next FROM budget WHERE id=1').fetchone()
        slot = max(now, min(row[0], now + 60)) if row else now
        db.execute('INSERT INTO budget VALUES (1,?) ON CONFLICT(id) DO UPDATE SET next=excluded.next', (slot + 1.25,))
    return slot - now


class CoordinatedSheetsClient(HTTPClient):
    def request(self, *args, **kwargs):
        path = os.environ.get('TOPUS_GOOGLE_RATE_DB') or str(Path(os.environ.get('TOPUS_PUSH_DB', '/data/push.sqlite3')).with_name('sheets-rate.sqlite3'))
        for attempt in range(3):
            time.sleep(reserve(path, time.time()))
            try:
                return super().request(*args, **kwargs)
            except APIError as error:
                if error.response.status_code != 429 or attempt == 2:
                    raise
                time.sleep(30 * (attempt + 1))
