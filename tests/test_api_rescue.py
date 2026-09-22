import json
import os
import sys
import tempfile
import time
import unittest
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

    def test_invalid_success_is_not_accepted(self):
        response = Mock(status_code=200)
        response.json.return_value = {'items':[]}
        with patch.object(api.requests,'get',return_value=response):
            with self.assertRaises(ValueError): api.request('channels',{},'fixture')
