import datetime
import os
import sys
import types
import unittest
from unittest.mock import patch

import worker_notifications


class AdminVideoAgeTests(unittest.TestCase):
    def test_real_biomolecula_timestamp_keeps_baku_offset(self):
        from youtube_client import format_youtube_timestamp
        original = '2026-09-14T21:00:34Z'
        sheet_value = format_youtube_timestamp(original)
        parsed = worker_notifications.publication_datetime(sheet_value)
        self.assertEqual(parsed.isoformat(), '2026-09-15T01:00:34+04:00')
        now = datetime.datetime.fromisoformat('2026-09-14T21:18:34+00:00')
        self.assertEqual((now - parsed).total_seconds() / 60, 18)

    def test_explicit_offsets_are_not_discarded(self):
        for value in ('2026-09-14T21:00:34Z', '2026-09-15T01:00:34+04:00'):
            self.assertEqual(worker_notifications.publication_datetime(value).timestamp(), 1789419634)

    def test_legacy_naive_iso_is_baku_time(self):
        self.assertEqual(worker_notifications.publication_datetime('2026-09-15T01:00:34').isoformat(),
                         '2026-09-15T01:00:34+04:00')

    def test_invalid_or_missing_dates_remain_unknown(self):
        for value in (None, '', 'invalid'):
            self.assertIsNone(worker_notifications.publication_datetime(value))

    def test_original_publication_survives_queue_payload(self):
        published = datetime.datetime(2026, 9, 14, 19, 32, tzinfo=datetime.timezone.utc)
        captured = []
        class Control:
            def request(self, path, payload):
                captured.append(payload)
                return {'state': 'pending', 'key': 'key'}
        ended = published + datetime.timedelta(hours=2)
        sheets = types.SimpleNamespace(parse_datetime_value=lambda value: {'original': published, 'later': ended}.get(value))
        control = types.SimpleNamespace(configured=lambda: True, ControlClient=Control)
        with patch.dict(sys.modules, {'sheets': sheets, 'control_client': control}), patch.dict(os.environ, {
            'TOPUS_WORKER_URL': 'https://example.invalid', 'TOPUS_WORKER_ADMIN_SECRET': 'test',
            'TOPUS_PUBLISHER_OWNER': 'test', 'TOPUS_PUBLISHER_LEASE': 'test', 'TOPUS_WORKER_PROJECTS': 'SciTopus',
        }), patch.object(worker_notifications, 'deliver_remote', return_value={}):
            worker_notifications.notify_worker_subscribers({'code':'SciTopus'},
                {'channel_id':'channel','video_id':'video','published':'original','live_actual_end':'later'}, 'text')
        self.assertEqual(captured[0]['payload']['youtubePublishedAt'], published.isoformat())
        self.assertEqual(captured[0]['payload']['youtubeLiveEndedAt'], ended.isoformat())
        self.assertEqual(captured[0]['payload']['text'], 'text')


if __name__ == '__main__':
    unittest.main()
