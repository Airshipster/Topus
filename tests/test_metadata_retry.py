import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import sheets


class Worksheet:
    def __init__(self, rows):
        self.rows = rows
        self.updates = []

    def get_all_values(self):
        return [row[:] for row in self.rows]

    def batch_update(self, updates, **_kwargs):
        self.updates.extend(updates)
        for update in updates:
            row = int(update['range'][1:]) - 1
            col = ord(update['range'][0]) - ord('A')
            self.rows[row][col] = update['values'][0][0]


class Sheet:
    def __init__(self, worksheet):
        self.page = worksheet

    def worksheet(self, _name):
        return self.page


class MetadataRetryTests(unittest.TestCase):
    channel = 'UC' + 'a' * 22
    headers = ['Timestamp GMT+4', 'Video ID', 'Ссылка на канал', 'Обработано', 'Проекты', 'Источник']

    def setUp(self):
        self.now = datetime(2026, 11, 9, tzinfo=timezone.utc)
        self.row = ['09.11.2026 04:00:00', 'fresh000001',
                    'https://www.youtube.com/channel/' + self.channel, '❌', '', 'Push · server']
        self.ws = Worksheet([self.headers[:], self.row[:]])
        self.sheet = Sheet(self.ws)
        self.event = {'video_id': 'fresh000001', 'channel_id': self.channel,
                      'timestamp': '09.11.2026 04:00:00'}

    def test_backoff_is_shared_and_increases(self):
        self.assertEqual(sheets.defer_unavailable_push_events(self.sheet, [self.event], self.now), 1)
        self.assertIn('[api-retry=1]', self.ws.rows[1][3])
        self.assertIn('2026-11-09T00:30:00Z', self.ws.rows[1][3])
        self.assertEqual(sheets.defer_unavailable_push_events(self.sheet, [self.event], self.now), 1)
        self.assertIn('[api-retry=2]', self.ws.rows[1][3])
        self.assertIn('2026-11-09T01:00:00Z', self.ws.rows[1][3])

    def test_reader_skips_future_retries_and_keeps_due_retries(self):
        now = datetime.now(timezone.utc)
        future = (now + timedelta(hours=1)).isoformat()
        past = (now - timedelta(hours=1)).isoformat()
        self.ws.rows[1][3] = f'❌ [api-retry=1] [retry-after={future}]'
        self.assertEqual(sheets.get_push_events(self.sheet), [])
        self.ws.rows[1][3] = f'❌ [api-retry=1] [retry-after={past}]'
        self.assertEqual([event['video_id'] for event in sheets.get_push_events(self.sheet)], ['fresh000001'])

    def test_confirmed_row_is_never_reopened_by_deferral(self):
        self.ws.rows[1][3] = '✅'
        self.assertEqual(sheets.defer_unavailable_push_events(self.sheet, [self.event], self.now), 0)
        self.assertEqual(self.ws.rows[1][3], '✅')

    def test_duplicate_identity_fails_closed(self):
        self.ws.rows.append(self.ws.rows[1][:])
        with self.assertRaisesRegex(RuntimeError, 'PUSH_DEFER_IDENTITY_UNRESOLVED'):
            sheets.defer_unavailable_push_events(self.sheet, [self.event], self.now)
        self.assertEqual(self.ws.updates, [])

    def test_old_missing_metadata_is_retained_without_further_polling(self):
        sheets.defer_unavailable_push_events(self.sheet, [self.event], self.now + timedelta(days=8))
        self.assertIn('manual review', self.ws.rows[1][3])
        self.assertEqual(sheets.get_push_events(self.sheet), [])


if __name__ == '__main__':
    unittest.main()
