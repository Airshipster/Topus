import os
import tempfile
import unittest
from unittest.mock import patch

from rss_discovery import hot_channels, push_gap_health, read_result, save_result
from push_store import database, queue_event
import rss


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        env = patch.dict(os.environ, {'TOPUS_PUSH_DB': directory.name+'/push.db', 'TOPUS_RSS_CACHE_ONLY':'true'})
        env.start()
        self.addCleanup(env.stop)

    def test_missing_stale_and_failure_are_not_empty_success(self):
        self.assertEqual(read_result('channel', now=100), (None, 'DISCOVERY_PENDING'))
        save_result('channel', [], '', now=100)
        self.assertEqual(read_result('channel', now=200), ([], ''))
        self.assertEqual(read_result('channel', now=2300), (None, 'DISCOVERY_STALE'))
        save_result('channel', None, 'HTTP_404', now=2400)
        self.assertEqual(read_result('channel', now=2401), (None, 'HTTP_404'))

    def test_publisher_uses_persisted_results_without_network(self):
        videos = [{'video_id':'fixture'}]
        save_result('channel', videos, '')
        with patch.object(rss, '_check_rss_feed_once') as network:
            self.assertEqual(rss.check_rss_feed('channel'), videos)
            self.assertIsNone(rss.check_rss_feed('missing'))
            network.assert_not_called()

    def test_success_clears_prior_error(self):
        save_result('channel', None, 'HTTP_500')
        self.assertIsNone(rss.check_rss_feed('channel'))
        save_result('channel', [], '')
        self.assertEqual(rss.check_rss_feed('channel'), [])
        self.assertNotIn('channel', rss.failure_reasons)

    def test_recent_active_verified_channel_enters_hotset(self):
        with database() as db:
            db.execute("INSERT INTO leases(channel_id,enabled) VALUES ('channel',1)")
        save_result('channel', [{'video_id':'video-one'}], '', now=100)
        self.assertEqual(hot_channels(now=101), {'channel'})
        self.assertEqual(hot_channels(now=100 + 8 * 86400), set())

    def test_hot_scan_records_only_new_videos_as_push_gaps(self):
        channel = 'UC' + 'a' * 22
        save_result(channel, [{'video_id':'oldvideo123'}], '', now=100)
        queued = save_result(channel, [{'video_id':'oldvideo123'}, {'video_id':'newvideo123'}], '',
                             now=200, track_push_gap=True)
        self.assertEqual(queued, 1)
        self.assertEqual(push_gap_health(now=200 + 601)['open'], 1)
        with database() as db:
            event = db.execute("SELECT video_id,source FROM events").fetchone()
        self.assertEqual((event['video_id'], event['source']), ('newvideo123', 'RSS · server'))

    def test_hot_scan_does_not_queue_rss_when_push_arrived_first(self):
        channel = 'UC' + 'b' * 22
        save_result(channel, [{'video_id':'oldvideo123'}], '', now=100)
        queue_event('newvideo123', channel, received=150, source='Push · server')

        queued = save_result(channel, [
            {'video_id':'oldvideo123'}, {'video_id':'newvideo123'}
        ], '', now=200, track_push_gap=True)

        self.assertEqual(queued, 0)
        with database() as db:
            sources = [row['source'] for row in db.execute(
                "SELECT source FROM events WHERE video_id='newvideo123'"
            )]
        self.assertEqual(sources, ['Push · server'])
