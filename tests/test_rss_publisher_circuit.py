import os
import tempfile
import unittest
from unittest.mock import patch

import rss
import rss_circuit as circuit


class PublisherCircuitTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        env = patch.dict(os.environ, {'TOPUS_PUSH_DB': directory.name + '/push.db',
                                      'TOPUS_RSS_CACHE_ONLY': 'false'})
        env.start()
        self.addCleanup(env.stop)
        rss.failure_reasons.clear()
        rss.failure_retry_at.clear()
        rss.failed_channels.clear()
        self.channels = {'UC' + str(i).zfill(22): {} for i in range(100)}
        self.project = {'name': 'fixture'}

    def missing(self, channel):
        rss.failure_reasons[channel] = 'HTTP_404'
        return None

    def scan(self, channels=None, cache=None):
        return rss.rss_fallback_check(None, self.project, set(),
            project_channels=self.channels if channels is None else channels,
            return_seen=True, rss_cache=cache)

    def test_backup_publisher_stops_partial_outage_and_keeps_verified_items(self):
        first = next(iter(self.channels))
        video = {'video_id': 'abcdefghijk'}
        with patch.object(rss, 'check_rss_feed', side_effect=lambda c: [video] if c == first else self.missing(c)) as fetch:
            videos, seen = self.scan()
        self.assertEqual(fetch.call_count, 12)
        self.assertEqual([item['video_id'] for item in videos], ['abcdefghijk'])
        self.assertEqual(seen, {first: {'abcdefghijk'}})
        self.assertEqual(len(rss.failed_channels), 99)
        self.assertTrue(circuit.is_open())

    def test_next_project_reuses_shared_pause_without_new_source_requests(self):
        cache = {}
        with patch.object(rss, 'check_rss_feed', side_effect=self.missing) as fetch:
            self.scan(cache=cache)
            self.scan(channels={'other': {}}, cache=cache)
        self.assertEqual(fetch.call_count, 12)
        self.assertEqual(rss.failure_reasons['other'], 'RSS_UPSTREAM_COOLDOWN')

    def test_cached_success_remains_usable_without_a_new_network_request(self):
        circuit.defer()
        video = {'video_id': 'abcdefghijk'}
        with patch.object(rss, 'check_rss_feed') as fetch:
            videos, seen = self.scan(channels={'cached': {}, 'uncached': {}}, cache={'cached': [video]})
        fetch.assert_not_called()
        self.assertEqual(len(videos), 1)
        self.assertEqual(seen, {'cached': {'abcdefghijk'}})
        self.assertNotIn('cached', rss.failed_channels)
        self.assertIn('uncached', rss.failed_channels)

    def test_cache_only_primary_does_not_recover_or_acquire_network_probe(self):
        with patch.dict(os.environ, {'TOPUS_RSS_CACHE_ONLY': 'true'}), \
                patch.object(circuit, 'claim') as claim, \
                patch.object(rss, 'check_rss_feed', return_value=[]) as fetch:
            self.scan()
        claim.assert_not_called()
        self.assertEqual(fetch.call_count, 100)

    def test_backup_stops_on_actual_retry_after_even_for_small_inventory(self):
        def limited(channel):
            rss.failure_reasons[channel] = 'HTTP_429'
            rss.failure_retry_at[channel] = 9999999999
            return None
        with patch.object(rss, 'check_rss_feed', side_effect=limited) as fetch:
            self.scan(channels={'one': {}, 'two': {}})
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(circuit.claim(now=9999999998), 'skip')


if __name__ == '__main__':
    unittest.main()
