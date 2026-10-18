import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import api_rescue
import rss
import rss_circuit as circuit
import rss_discovery as discovery


class CircuitTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        env = patch.dict(os.environ, {'TOPUS_PUSH_DB': directory.name + '/push.db'})
        env.start()
        self.addCleanup(env.stop)
        self.channels = {'UC' + str(index).zfill(22) for index in range(100)}
        self.config = Mock(RSS_WORKERS=12)
        patcher = patch.object(discovery, 'report_push_gap_health')
        patcher.start()
        self.addCleanup(patcher.stop)

    def missing(self, channel):
        rss.failure_reasons[channel] = 'HTTP_404'
        return None

    def test_mass_404_checks_twelve_and_keeps_all_rescue_candidates(self):
        with patch.object(rss, 'check_rss_feed', side_effect=self.missing) as fetch, \
             patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                discovery.collect(self.channels, self.config, 'full')
            self.assertEqual(fetch.call_count, 12)
            rescue.assert_called_once_with(self.channels)
        self.assertTrue(circuit.is_open())

    def test_cooldown_survives_reopen_and_does_no_rss_http(self):
        circuit.defer()
        with patch.object(rss, 'check_rss_feed') as fetch, patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                discovery.collect(self.channels, self.config, 'full')
            fetch.assert_not_called()
            rescue.assert_called_once_with(self.channels)

    def test_one_success_keeps_full_scan_and_does_not_open_circuit(self):
        first = circuit.ordered_channels(self.channels)[0]
        with patch.object(rss, 'check_rss_feed', side_effect=lambda c: [] if c == first else self.missing(c)) as fetch, \
             patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'RSS_DISCOVERY_FAILED_99'):
                discovery.collect(self.channels, self.config, 'full')
            self.assertEqual(fetch.call_count, 100)
            self.assertEqual(rescue.call_args.args[0], self.channels - {first})
        self.assertFalse(circuit.is_open())

    def test_small_inventory_does_not_assert_host_wide_failure(self):
        with patch.object(rss, 'check_rss_feed', side_effect=self.missing) as fetch, patch.object(api_rescue, 'run'):
            with self.assertRaisesRegex(RuntimeError, 'RSS_DISCOVERY_FAILED_12'):
                discovery.collect(set(sorted(self.channels)[:12]), self.config, 'full')
            self.assertEqual(fetch.call_count, 12)
        self.assertFalse(circuit.is_open())

    def test_only_one_probe_claim_after_expiry(self):
        circuit.defer(now=100)
        self.assertEqual(circuit.claim(now=1899), 'skip')
        with ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(lambda _: circuit.claim(now=1900), range(4)))
        self.assertEqual(values.count('probe'), 1)
        self.assertEqual(values.count('skip'), 3)
        self.assertTrue(circuit.is_open(now=1901))

    def test_probe_rotates_across_inventory(self):
        before = circuit.ordered_channels(self.channels, now=1800)[:12]
        after = circuit.ordered_channels(self.channels, now=3600)[:12]
        self.assertTrue(set(before).isdisjoint(after))

    def test_recovery_clears_persisted_cooldown(self):
        circuit.defer(now=100)
        circuit.recover()
        self.assertFalse(circuit.is_open(now=101))
        self.assertEqual(circuit.claim(now=102), 'normal')

    def test_expired_circuit_recovers_and_completes_full_scan(self):
        circuit.defer(now=0)
        with patch.object(rss, 'check_rss_feed', return_value=[]) as fetch, \
             patch.object(api_rescue, 'run') as rescue:
            discovery.collect(self.channels, self.config, 'full')
            self.assertEqual(fetch.call_count, 100)
            rescue.assert_not_called()
        self.assertFalse(circuit.is_open())

    def test_expired_circuit_failure_defers_without_full_scan(self):
        circuit.defer(now=0)
        with patch.object(rss, 'check_rss_feed', side_effect=self.missing) as fetch, \
             patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                discovery.collect(self.channels, self.config, 'full')
            self.assertEqual(fetch.call_count, 12)
            rescue.assert_called_once_with(self.channels)
        self.assertTrue(circuit.is_open())

    def test_cached_feed_is_not_relabelled_fresh_during_cooldown(self):
        discovery.save_result('channel', [], '', now=100)
        circuit.defer(now=101)
        self.assertEqual(discovery.read_result('channel', now=102), (None, 'RSS_UPSTREAM_COOLDOWN'))
        circuit.recover()
        self.assertEqual(discovery.read_result('channel', now=102), ([], ''))

    def test_non_404_failures_do_not_start_mass_404_circuit(self):
        def failed(channel):
            rss.failure_reasons[channel] = 'HTTP_500'
            return None
        with patch.object(rss, 'check_rss_feed', side_effect=failed) as fetch, patch.object(api_rescue, 'run'):
            with self.assertRaisesRegex(RuntimeError, 'RSS_DISCOVERY_FAILED_100'):
                discovery.collect(self.channels, self.config, 'full')
            self.assertEqual(fetch.call_count, 100)
        self.assertFalse(circuit.is_open())


if __name__ == '__main__':
    unittest.main()
