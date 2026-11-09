import os
import tempfile
import time
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
        rss.failure_reasons.clear()
        rss.failure_retry_at.clear()
        self.channels = {'UC' + str(index).zfill(22) for index in range(100)}
        self.config = Mock(RSS_WORKERS=12)
        pacing = patch.object(discovery.RequestStartPacer, 'wait')
        pacing.start()
        self.addCleanup(pacing.stop)
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

    def test_one_success_does_not_allow_ninety_nine_more_requests(self):
        first = circuit.ordered_channels(self.channels)[0]
        with patch.object(rss, 'check_rss_feed', side_effect=lambda c: [] if c == first else self.missing(c)) as fetch, \
             patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                discovery.collect(self.channels, self.config, 'full')
            self.assertEqual(fetch.call_count, 12)
            self.assertEqual(rescue.call_args.args[0], self.channels - {first})
        self.assertTrue(circuit.is_open())

    def test_mixed_partial_404_and_500_outage_stops_after_first_batch(self):
        ordered = sorted(self.channels)
        errors = dict(zip(ordered[:8], ['HTTP_404'] * 4 + ['HTTP_500'] * 4))
        def result(channel):
            rss.failure_reasons[channel] = errors.get(channel, '')
            return None if channel in errors else []
        with patch.object(circuit, 'ordered_channels', return_value=ordered), \
                patch.object(rss, 'check_rss_feed', side_effect=result) as fetch, \
                patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                discovery.collect(self.channels, self.config, 'full')
        self.assertEqual(fetch.call_count, 12)
        self.assertEqual(rescue.call_args.args[0], set(ordered[:8] + ordered[12:]))

    def test_outage_after_healthy_probe_does_not_enqueue_the_whole_inventory(self):
        ordered = sorted(self.channels)
        healthy = set(ordered[:12])
        with patch.object(circuit, 'ordered_channels', return_value=ordered), \
                patch.object(rss, 'check_rss_feed', side_effect=lambda c: [] if c in healthy else self.missing(c)) as fetch, \
                patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                discovery.collect(self.channels, self.config, 'full')
        self.assertEqual(fetch.call_count, 24)
        self.assertEqual(rescue.call_args.args[0], self.channels - healthy)

    def test_isolated_channel_failure_does_not_block_healthy_feeds(self):
        first = sorted(self.channels)[0]
        with patch.object(rss, 'check_rss_feed', side_effect=lambda c: self.missing(c) if c == first else []) as fetch, \
                patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'RSS_DISCOVERY_FAILED_1'):
                discovery.collect(self.channels, self.config, 'full')
        self.assertEqual(fetch.call_count, 100)
        rescue.assert_called_once_with({first})
        self.assertFalse(circuit.is_open())

    def test_explicit_rate_limit_persists_actual_wait_and_stops_future_batches(self):
        deadline = time.time() + 7200
        first = sorted(self.channels)[0]
        def result(channel):
            if channel == first:
                rss.failure_reasons[channel] = 'HTTP_429'
                rss.failure_retry_at[channel] = deadline
                return None
            return []
        with patch.object(circuit, 'ordered_channels', return_value=sorted(self.channels)), \
                patch.object(rss, 'check_rss_feed', side_effect=result) as fetch, \
                patch.object(api_rescue, 'run'):
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                discovery.collect(self.channels, self.config, 'full')
        self.assertEqual(fetch.call_count, 12)
        self.assertEqual(circuit.claim(now=deadline - 1), 'skip')
        self.assertEqual(circuit.claim(now=deadline), 'probe')

    def test_shorter_pause_cannot_reduce_an_existing_retry_after(self):
        circuit.defer(now=100, retry_at=10000)
        circuit.defer(now=200)
        self.assertEqual(circuit.claim(now=9999), 'skip')
        self.assertEqual(circuit.claim(now=10000), 'probe')

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

    def test_mass_500_failure_opens_circuit_instead_of_amplifying_outage(self):
        def failed(channel):
            rss.failure_reasons[channel] = 'HTTP_500'
            return None
        with patch.object(rss, 'check_rss_feed', side_effect=failed) as fetch, patch.object(api_rescue, 'run'):
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                discovery.collect(self.channels, self.config, 'full')
            self.assertEqual(fetch.call_count, 12)
        self.assertTrue(circuit.is_open())

    def hot_snapshot(self, channel, *, checked=None, error='', payload=None, enabled=1):
        now = time.time() if checked is None else checked
        discovery.initialize()
        with discovery.database() as db:
            db.execute('INSERT OR REPLACE INTO leases(channel_id,enabled) VALUES (?,?)', (channel, enabled))
            db.execute('INSERT OR REPLACE INTO rss_hotset(channel_id,last_video,last_checked) VALUES (?,?,?)',
                       (channel, now, now))
            db.execute('INSERT OR REPLACE INTO rss_discovery(channel_id,checked,payload,error) VALUES (?,?,?,?)',
                       (channel, now, '[]' if payload is None else payload, error))
        return now

    def test_full_scan_reuses_recent_success_without_relabeling_freshness(self):
        channel = sorted(self.channels)[0]
        observed_at = self.hot_snapshot(channel, checked=time.time() - 60)
        with patch.object(rss, 'check_rss_feed', return_value=[]) as fetch, \
                patch.object(api_rescue, 'run') as rescue:
            discovery.collect(self.channels, self.config, 'full')
        self.assertEqual(fetch.call_count, len(self.channels) - 1)
        self.assertNotIn(channel, {call.args[0] for call in fetch.call_args_list})
        rescue.assert_not_called()
        with discovery.database() as db:
            self.assertEqual(db.execute('SELECT checked FROM rss_discovery WHERE channel_id=?',
                                        (channel,)).fetchone()['checked'], observed_at)

    def test_reuse_requires_success_valid_payload_and_enabled_hot_lease(self):
        channels = sorted(self.channels)[:5]
        self.hot_snapshot(channels[0])
        self.hot_snapshot(channels[1], checked=time.time() - discovery.FULL_REUSE_MAX_AGE_SECONDS - 5)
        self.hot_snapshot(channels[2], error='HTTP_404')
        self.hot_snapshot(channels[3], payload='{invalid')
        self.hot_snapshot(channels[4], enabled=0)
        with patch.object(rss, 'check_rss_feed', return_value=[]) as fetch:
            discovery.collect(set(channels), self.config, 'full')
        self.assertEqual({call.args[0] for call in fetch.call_args_list}, set(channels[1:]))

    def test_hot_scan_does_not_reuse_its_own_snapshot(self):
        channel = sorted(self.channels)[0]
        self.hot_snapshot(channel)
        with patch.object(rss, 'check_rss_feed', return_value=[]) as fetch:
            discovery.collect({channel}, self.config, 'hot')
        fetch.assert_called_once_with(channel)

    def test_recovery_probe_never_uses_a_cached_success(self):
        channel = sorted(self.channels)[0]
        self.hot_snapshot(channel)
        circuit.defer(now=0)
        with patch.object(rss, 'check_rss_feed', return_value=[]) as fetch:
            discovery.collect(self.channels, self.config, 'full')
        self.assertEqual(fetch.call_count, len(self.channels))

    def test_new_outage_keeps_previously_reused_channels_in_api_recovery(self):
        channel = sorted(self.channels)[0]
        self.hot_snapshot(channel)
        with patch.object(rss, 'check_rss_feed', side_effect=self.missing) as fetch, \
                patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                discovery.collect(self.channels, self.config, 'full')
        self.assertEqual(fetch.call_count, 12)
        rescue.assert_called_once_with(self.channels)

    def test_hot_discovery_queues_new_video_before_full_reuses_it(self):
        channel = sorted(self.channels)[0]
        self.hot_snapshot(channel, payload='[]')
        videos = [{'video_id': 'newvideo123'}]
        self.assertEqual(discovery.save_result(channel, videos, '', track_push_gap=True), 1)
        with patch.object(rss, 'check_rss_feed', return_value=[]) as fetch:
            discovery.collect({channel}, self.config, 'full')
        fetch.assert_not_called()
        with discovery.database() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM events WHERE video_id=?',
                                        ('newvideo123',)).fetchone()[0], 1)


if __name__ == '__main__':
    unittest.main()
