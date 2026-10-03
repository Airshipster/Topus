import json
import os
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

import api_rescue as api
from push_store import database


class RescueTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        env = patch.dict(os.environ, {'TOPUS_PUSH_DB':directory.name+'/push.db',
                                     'TOPUS_API_RESCUE_DAILY_LIMIT':'2'})
        env.start()
        self.addCleanup(env.stop)

    def test_budget_survives_reopen_and_stops_before_http(self):
        api.reserve()
        api.reserve()
        with patch.object(api.requests,'get') as get:
            with self.assertRaises(api.BudgetExhausted):
                api.request('channels', {}, 'fixture')
            get.assert_not_called()

    def test_quota_error_blocks_remaining_day(self):
        response = Mock(status_code=403)
        response.json.return_value = {'error':{'errors':[{'reason':'quotaExceeded'}]}}
        with patch.object(api.requests,'get',return_value=response) as get:
            with self.assertRaises(api.BudgetExhausted):
                api.request('channels',{},'fixture')
            with self.assertRaises(api.BudgetExhausted):
                api.request('channels',{},'fixture')
            self.assertEqual(get.call_count,1)

    def test_rotation_does_not_starve_channels(self):
        chosen = api.select_channels({'a','b','c'},2)
        self.assertEqual([r['channel_id'] for r in chosen],['a','b'])
        with database() as db:
            db.execute("UPDATE api_rescue_channels SET attempted=1 WHERE channel_id IN ('a','b')")
        self.assertEqual(api.select_channels({'a','b','c'},1)[0]['channel_id'],'c')

    def test_hot_and_full_scans_share_a_durable_half_hour_pass(self):
        self.assertTrue(api.claim_pass(now=100))
        self.assertFalse(api.claim_pass(now=101))
        self.assertFalse(api.claim_pass(now=1899))
        self.assertTrue(api.claim_pass(now=1900))

    def test_concurrent_scans_cannot_both_claim_the_api_pass(self):
        api.initialize()
        with ThreadPoolExecutor(max_workers=4) as pool:
            claims = list(pool.map(lambda _: api.claim_pass(now=100), range(4)))
        self.assertEqual(sum(claims), 1)

    def test_missing_runtime_key_uses_existing_master_settings(self):
        fixture = Mock(YOUTUBE_API_KEYS=[], YOUTUBE_API_KEY='', SPREADSHEET_ID='master-fixture')
        sheets = Mock(format_timestamp=lambda value: value.isoformat())
        def load_settings(sheet):
            fixture.YOUTUBE_API_KEYS = ['assigned-fixture']
        sheets.load_settings.side_effect = load_settings
        with patch.dict(sys.modules, {'config': fixture, 'sheets': sheets}), \
             patch.object(api, 'select_channels', return_value=[]):
            api.run({'channel'})
        sheets.authenticate_google_sheets.return_value.open_by_key.assert_called_once_with('master-fixture')
        sheets.load_settings.assert_called_once()
        self.assertEqual(fixture.YOUTUBE_API_KEYS, ['assigned-fixture'])

    def test_existing_runtime_key_is_not_replaced(self):
        fixture = Mock(YOUTUBE_API_KEYS=['assigned-fixture'], YOUTUBE_API_KEY='')
        sheets = Mock(format_timestamp=lambda value: value.isoformat())
        with patch.dict(sys.modules, {'config': fixture, 'sheets': sheets}), \
             patch.object(api, 'select_channels', return_value=[]):
            api.run({'channel'})
        sheets.authenticate_google_sheets.assert_not_called()
        sheets.load_settings.assert_not_called()

    def test_configuration_failure_does_not_spend_budget_or_claim_pass(self):
        fixture = Mock(YOUTUBE_API_KEYS=[], YOUTUBE_API_KEY='', SPREADSHEET_ID='master-fixture')
        sheets = Mock(format_timestamp=lambda value: value.isoformat())
        sheets.authenticate_google_sheets.side_effect = RuntimeError('unavailable')
        with patch.dict(sys.modules, {'config': fixture, 'sheets': sheets}), \
             patch.object(api, 'request') as request:
            api.run({'channel'})
        request.assert_not_called()
        self.assertTrue(api.claim_pass(now=100))

    def test_candidate_survives_source_failure_and_is_deduplicated(self):
        api.initialize()
        with database() as db:
            db.execute("INSERT INTO api_rescue_channels(channel_id,error) VALUES ('a','source-error')")
            for _ in range(2):
                db.execute('INSERT OR IGNORE INTO api_rescue_videos VALUES (?,?,?,?)',
                           ('a','video',json.dumps({'video_id':'video'}),time.time()))
        self.assertEqual(api.read_result('a'),[{'video_id':'video'}])

    def test_quota_day_is_pacific_not_server_timezone(self):
        self.assertEqual(api.quota_day(1789794000),'2026-09-18')

    def test_rescued_video_is_queued_immediately_and_only_once(self):
        channel = 'UC' + 'a' * 22
        api.initialize()
        with database() as db:
            db.execute('INSERT INTO api_rescue_channels(channel_id,uploads) VALUES (?,?)', (channel, 'uploads'))
        item = {'contentDetails': {'videoId': 'abcdefghijk',
                'videoPublishedAt': '2026-10-02T00:00:00Z'},
                'snippet': {'title': 'Fixture', 'channelTitle': 'Fixture'}}
        fixture = Mock(YOUTUBE_API_KEYS=['fixture'], YOUTUBE_API_KEY='', RSS_FALLBACK_AGE_HOURS=999999)
        sheets = Mock(format_timestamp=lambda value: value.isoformat())
        with patch.dict(sys.modules, {'config': fixture, 'sheets': sheets}), \
             patch.object(api, 'request', return_value=[item]):
            api.run({channel})
            api.run({channel})
        with database() as db:
            rows = db.execute('SELECT video_id,source,mirrored FROM events').fetchall()
        self.assertEqual([tuple(row) for row in rows], [('abcdefghijk', 'YouTube API backup', 0)])

    def test_invalid_success_is_not_accepted(self):
        response = Mock(status_code=200)
        response.json.return_value = {'items':[]}
        with patch.object(api.requests,'get',return_value=response):
            with self.assertRaises(ValueError): api.request('channels',{},'fixture')
