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

    def test_explicit_zero_results_without_items_is_a_valid_empty_page(self):
        response = Mock(status_code=200)
        response.json.return_value = {'kind': 'youtube#playlistItemListResponse',
                                     'pageInfo': {'totalResults': 0}}
        with patch.object(api.requests, 'get', return_value=response):
            self.assertEqual(api.request('playlistItems', {}, 'fixture'), [])

    def test_missing_items_with_nonzero_results_is_not_accepted(self):
        response = Mock(status_code=200)
        response.json.return_value = {'kind': 'youtube#playlistItemListResponse',
                                     'pageInfo': {'totalResults': 1}}
        with patch.object(api.requests, 'get', return_value=response):
            with self.assertRaises(ValueError):
                api.request('playlistItems', {}, 'fixture')

    def test_upstream_status_and_reason_are_preserved_without_request_data(self):
        response = Mock(status_code=404)
        response.json.return_value = {'error': {'errors': [{'reason': 'playlistNotFound'}]}}
        with patch.object(api.requests, 'get', return_value=response):
            with self.assertRaises(api.UpstreamError) as raised:
                api.request('playlistItems', {}, 'private-fixture-key')
        self.assertEqual(raised.exception.status, 404)
        self.assertEqual(raised.exception.reasons, ('playlistNotFound',))
        self.assertNotIn('private-fixture-key', str(raised.exception))

    def test_catch_up_keeps_old_gap_videos_and_stops_at_checkpoint(self):
        api.initialize()
        row = {'channel_id': 'a', 'uploads': 'uploads', 'payload': json.dumps([{'video_id': 'known'}])}
        item = lambda video: {'contentDetails': {'videoId': video, 'videoPublishedAt': '2026-09-01T00:00:00Z'}}
        collected = []
        with patch.object(api, 'request', return_value=api.ApiPage([item('missed'), item('known'), item('older')], 'tail')) as request:
            self.assertTrue(api.scan_uploads(row, 'fixture', datetime_cutoff(), collected.extend))
        self.assertEqual([i['contentDetails']['videoId'] for i in collected], ['missed', 'known'])
        self.assertEqual(request.call_count, 1)

    def test_initial_seed_keeps_only_recent_videos(self):
        api.initialize()
        row = {'channel_id': 'a', 'uploads': 'uploads'}
        items = [{'contentDetails': {'videoId': 'recent', 'videoPublishedAt': '2026-10-04T00:00:00Z'}},
                 {'contentDetails': {'videoId': 'old', 'videoPublishedAt': '2026-09-01T00:00:00Z'}}]
        collected = []
        with patch.object(api, 'request', return_value=api.ApiPage(items)):
            self.assertTrue(api.scan_uploads(row, 'fixture', datetime_cutoff(), collected.extend))
        self.assertEqual([i['contentDetails']['videoId'] for i in collected], ['recent'])

    def test_old_catch_up_video_reaches_durable_event_queue(self):
        channel = 'UC' + 'a' * 22
        api.initialize()
        with database() as db:
            db.execute('INSERT INTO api_rescue_channels(channel_id,uploads,payload) VALUES (?,?,?)',
                       (channel, 'uploads', json.dumps([{'video_id': 'known-video'}])))
        item = {'contentDetails': {'videoId': 'abcdefghijk', 'videoPublishedAt': '2020-01-01T00:00:00Z'},
                'snippet': {'title': 'Fixture', 'channelTitle': 'Fixture'}}
        fixture = Mock(YOUTUBE_API_KEYS=['fixture'], YOUTUBE_API_KEY='', RSS_FALLBACK_AGE_HOURS=168)
        sheets = Mock(format_timestamp=lambda value: value.isoformat())
        with patch.dict(sys.modules, {'config': fixture, 'sheets': sheets}), \
             patch.object(api, 'request', return_value=api.ApiPage([item])):
            api.run({channel})
        with database() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM events WHERE video_id=?', ('abcdefghijk',)).fetchone()[0], 1)

    def test_upload_pagination_reaches_checkpoint_and_retains_all_pages(self):
        api.initialize()
        row = {'channel_id': 'a', 'uploads': 'uploads', 'payload': json.dumps([{'video_id': 'known'}])}
        item = lambda video: {'contentDetails': {'videoId': video, 'videoPublishedAt': '2026-10-04T00:00:00Z'}}
        collected = []
        with patch.object(api, 'request', side_effect=[api.ApiPage([item('new')], 'second'), api.ApiPage([item('known')])]) as request:
            self.assertTrue(api.scan_uploads(row, 'fixture', datetime_cutoff(), lambda page: collected.extend(page)))
        self.assertEqual(request.call_args_list[1].args[1]['pageToken'], 'second')
        self.assertEqual([i['contentDetails']['videoId'] for i in collected], ['new', 'known'])
        with database() as db:
            progress = db.execute('SELECT checkpoint,token FROM api_rescue_progress').fetchone()
        self.assertEqual(tuple(progress), ('new', ''))

    def test_unfinished_scan_resumes_tail_and_does_not_skip_intervening_uploads(self):
        api.initialize()
        row = {'channel_id': 'a', 'uploads': 'uploads', 'payload': json.dumps([{'video_id': 'known'}])}
        item = lambda video: {'contentDetails': {'videoId': video, 'videoPublishedAt': '2026-10-04T00:00:00Z'}}
        with patch.object(api, 'request', side_effect=[api.ApiPage([item('head')], 'one'),
             api.ApiPage([item('b')], 'two'), api.ApiPage([item('c')], 'three')]):
            self.assertFalse(api.scan_uploads(row, 'fixture', datetime_cutoff(), lambda page: None))
        with database() as db:
            progress = db.execute('SELECT checkpoint,token FROM api_rescue_progress').fetchone()
        self.assertEqual(tuple(progress), ('known', 'three'))
        with patch.object(api, 'request', side_effect=[api.ApiPage([item('newer')], 'one'),
             api.ApiPage([item('known')])]) as request:
            self.assertTrue(api.scan_uploads(row, 'fixture', datetime_cutoff(), lambda page: None))
        self.assertEqual(request.call_args_list[1].args[1]['pageToken'], 'three')
        with database() as db:
            checkpoint = db.execute('SELECT checkpoint FROM api_rescue_progress').fetchone()[0]
        self.assertEqual(checkpoint, 'head')

    def test_page_failure_never_advances_checkpoint_or_loses_successful_head(self):
        api.initialize()
        row = {'channel_id': 'a', 'uploads': 'uploads', 'payload': json.dumps([{'video_id': 'known'}])}
        collected = []
        with patch.object(api, 'request', side_effect=[api.ApiPage([{'contentDetails': {'videoId': 'new'}}], 'next'),
             api.BudgetExhausted('limit')]):
            with self.assertRaises(api.BudgetExhausted):
                api.scan_uploads(row, 'fixture', datetime_cutoff(), lambda page: collected.extend(page))
        self.assertEqual(len(collected), 1)
        with database() as db:
            progress = db.execute('SELECT checkpoint,token FROM api_rescue_progress').fetchone()
        self.assertEqual(tuple(progress), ('known', 'next'))

    def test_expired_token_restarts_tail_without_advancing_checkpoint(self):
        api.initialize()
        row = {'channel_id': 'a', 'uploads': 'uploads', 'payload': json.dumps([{'video_id': 'known'}])}
        first = api.ApiPage([{'contentDetails': {'videoId': 'new'}}], 'next')
        with patch.object(api, 'request', side_effect=[first, api.InvalidPageToken('expired')]):
            with self.assertRaises(api.InvalidPageToken):
                api.scan_uploads(row, 'fixture', datetime_cutoff(), lambda page: None)
        with database() as db:
            progress = db.execute('SELECT checkpoint,token FROM api_rescue_progress').fetchone()
        self.assertEqual(tuple(progress), ('known', ''))
        with patch.object(api, 'request', side_effect=[first, api.ApiPage([{'contentDetails': {'videoId': 'known'}}])]) as request:
            self.assertTrue(api.scan_uploads(row, 'fixture', datetime_cutoff(), lambda page: None))
        self.assertEqual(request.call_args_list[1].args[1]['pageToken'], 'next')


def datetime_cutoff():
    from datetime import datetime, timezone
    return datetime(2026, 10, 1, tzinfo=timezone.utc)
