import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from event_batches import prioritize_events
import push_store


class Worksheet:
    def __init__(self):
        self.values = [['Timestamp GMT+4', 'Video ID', 'Ссылка на канал',
                        'Обработано', 'Проекты', 'Источник']]
        self.append_count = 0
        self.fail_after_append = False

    def get_all_values(self):
        return [row[:] for row in self.values]

    def append_rows(self, rows, **_kwargs):
        self.values.extend(rows)
        self.append_count += 1
        if self.fail_after_append:
            self.fail_after_append = False
            raise RuntimeError('uncertain sheet response')


class Coordinator:
    def __init__(self):
        self.events = {}
        self.fail_ack = False

    def add(self, event):
        key = json.dumps([event['channel_id'], event['video_id'], event['event_id']],
                         ensure_ascii=False, separators=(',', ':'))
        self.events.setdefault(key, {**event, 'key': key})
        return key

    def request(self, path, body=None):
        if path == '/events':
            for event in body['events']:
                self.add(event)
            return {'ok': True}
        if path == '/events/pending':
            events = sorted(self.events.values(), key=lambda event: event['received'])
            return {'events': events[:body['limit']]}
        if path == '/events/ack':
            if self.fail_ack:
                self.fail_ack = False
                return {'ok': False}
            for key in body['keys']:
                self.events.pop(key, None)
            return {'ok': True}
        raise AssertionError(path)


class Sheet:
    def __init__(self, worksheet):
        self.page = worksheet

    def worksheet(self, _name):
        return self.page


class EventBatchTests(unittest.TestCase):
    channel = 'UC' + 'a' * 22

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {
            'TOPUS_PUSH_DB': str(Path(self.tmp.name) / 'push.sqlite3'),
            'TOPUS_PUBLISHER_OWNER': 'server', 'TOPUS_PUBLISHER_LEASE': 'fixture',
        })
        self.env.start()
        self.control = Coordinator()
        self.configured = patch('control_client.configured', return_value=True)
        self.client = patch('control_client.ControlClient', return_value=self.control)
        self.configured.start()
        self.client.start()
        self.sheet = Sheet(Worksheet())

    def tearDown(self):
        self.client.stop()
        self.configured.stop()
        self.env.stop()
        self.tmp.cleanup()

    def test_fresh_push_before_old_events_without_starving_history(self):
        old = [{'video_id': str(i), 'source': 'YouTube API backup'} for i in range(303)]
        fresh = [{'video_id': 'fresh', 'source': 'Push · server'}]
        selected = prioritize_events(old + fresh, 100)
        self.assertEqual(selected[0]['video_id'], 'fresh')
        self.assertEqual(len(selected), 100)
        selected = prioritize_events(old + fresh * 200, 100)
        self.assertEqual(sum(event['source'] == 'YouTube API backup' for event in selected), 25)
        self.assertEqual(prioritize_events(old, 0), [])

    def test_science_push_has_priority_even_beyond_first_page(self):
        with push_store.database() as db:
            db.execute('CREATE TABLE inventory_cache(id, payload)')
            science = 'UC' + 's' * 22
            db.execute('INSERT INTO inventory_cache VALUES (1, ?)',
                       (json.dumps({science: {'projects': ['SciTopus']}}),))
            db.executemany('INSERT INTO events VALUES (?, ?, ?, ?, 0, ?)', [
                (str(i), f'{i:011d}', self.channel, float(i), 'Push · server')
                for i in range(110)
            ])
            db.execute('INSERT INTO events VALUES (?, ?, ?, ?, 0, ?)',
                       ('science', 'science0001', science, 1000, 'Push · server'))
            selected = push_store.prioritized_pending_events(db, limit=100)
        self.assertEqual(selected[0]['fingerprint'], 'science')

    def test_new_local_push_bypasses_remote_history_and_is_not_duplicated(self):
        now = time.time()
        for i in range(600):
            self.control.add({'event_id': str(i), 'video_id': f'{i:011d}',
                              'channel_id': self.channel, 'received': now - 1000 + i,
                              'source': 'YouTube API backup'})
        push_store.queue_event('fresh000001', self.channel, received=now, fingerprint='fresh')
        self.assertTrue(push_store.mirror_events(self.sheet))
        self.assertEqual(self.sheet.page.values[1][1], 'fresh000001')
        with push_store.database() as db:
            self.assertEqual(db.execute("SELECT mirrored FROM events WHERE fingerprint='fresh'").fetchone()[0], 1)
        for _ in range(10):
            if not push_store.mirror_events(self.sheet):
                break
        self.assertEqual(len(self.control.events), 0)
        ids = [row[1] for row in self.sheet.page.values[1:]]
        self.assertEqual(len(ids), 601)
        self.assertEqual(len(set(ids)), 601)

    def test_uncertain_sheet_append_is_not_acked_and_retry_does_not_duplicate(self):
        push_store.queue_event('fresh000001', self.channel, received=1000, fingerprint='fresh')
        self.sheet.page.fail_after_append = True
        with self.assertRaisesRegex(RuntimeError, 'uncertain sheet'):
            push_store.mirror_events(self.sheet)
        self.assertEqual(len(self.control.events), 1)
        with push_store.database() as db:
            self.assertEqual(db.execute('SELECT mirrored FROM events').fetchone()[0], 0)
        self.assertFalse(push_store.mirror_events(self.sheet))
        self.assertEqual(len(self.control.events), 0)
        self.assertEqual(self.sheet.page.append_count, 1)
        self.assertEqual(len(self.sheet.page.values), 2)

    def test_failed_ack_keeps_local_event_and_retry_does_not_duplicate(self):
        push_store.queue_event('fresh000001', self.channel, received=1000, fingerprint='fresh')
        self.control.fail_ack = True
        with self.assertRaisesRegex(RuntimeError, 'CONTROL_EVENT_ACK_FAILED'):
            push_store.mirror_events(self.sheet)
        with push_store.database() as db:
            self.assertEqual(db.execute('SELECT mirrored FROM events').fetchone()[0], 0)
        push_store.mirror_events(self.sheet)
        self.assertEqual(self.sheet.page.append_count, 1)
        self.assertEqual(len(self.control.events), 0)

    def test_malformed_remote_response_cannot_ack_or_mark_events(self):
        push_store.queue_event('fresh000001', self.channel, received=1000, fingerprint='fresh')
        with patch.object(self.control, 'request', side_effect=lambda path, body=None:
                          {'events': [None]} if path == '/events/pending' else {'ok': True}):
            with self.assertRaisesRegex(RuntimeError, 'CONTROL_EVENTS_INVALID'):
                push_store.mirror_events(self.sheet)
        with push_store.database() as db:
            self.assertEqual(db.execute('SELECT mirrored FROM events').fetchone()[0], 0)
        self.assertEqual(len(self.sheet.page.values), 1)


if __name__ == '__main__':
    unittest.main()
