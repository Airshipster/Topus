import os
import tempfile
import unittest
from unittest.mock import patch

from rss_discovery import read_result, save_result
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
