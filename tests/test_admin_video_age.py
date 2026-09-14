import datetime
import os
import sys
import types
import unittest
from unittest.mock import patch

import worker_notifications


class AdminVideoAgeTests(unittest.TestCase):
    def test_original_publication_survives_queue_payload(self):
        published = datetime.datetime(2026, 9, 14, 19, 32, tzinfo=datetime.timezone.utc)
        captured = []
        class Control:
            def request(self, path, payload):
                captured.append(payload)
                return {'state': 'pending', 'key': 'key'}
        sheets = types.SimpleNamespace(parse_datetime_value=lambda value: published if value == 'original' else None)
        control = types.SimpleNamespace(configured=lambda: True, ControlClient=Control)
        with patch.dict(sys.modules, {'sheets': sheets, 'control_client': control}), patch.dict(os.environ, {
            'TOPUS_WORKER_URL': 'https://example.invalid', 'TOPUS_WORKER_ADMIN_SECRET': 'test',
            'TOPUS_PUBLISHER_OWNER': 'test', 'TOPUS_PUBLISHER_LEASE': 'test', 'TOPUS_WORKER_PROJECTS': 'SciTopus',
        }), patch.object(worker_notifications, 'deliver_remote', return_value={}):
            worker_notifications.notify_worker_subscribers({'code':'SciTopus'},
                {'channel_id':'channel','video_id':'video','published':'original','live_actual_end':'later'}, 'text')
        self.assertEqual(captured[0]['payload']['youtubePublishedAt'], published.isoformat())
        self.assertEqual(captured[0]['payload']['text'], 'text')


if __name__ == '__main__':
    unittest.main()
