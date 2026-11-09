import os
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import api_rescue
import rss
import rss_circuit
import rss_discovery
from push_store import database


A, B = 'UC' + 'a' * 22, 'UC' + 'b' * 22


class ResumeRescueTests(unittest.TestCase):
    def setUp(self):
        root = tempfile.TemporaryDirectory()
        self.addCleanup(root.cleanup)
        env = patch.dict(os.environ, {'TOPUS_PUSH_DB': root.name + '/push.sqlite3'})
        env.start()
        self.addCleanup(env.stop)

    def progress(self, channel, *, head='', token='', checkpoint='known'):
        api_rescue.initialize()
        with database() as db:
            db.execute('INSERT INTO api_rescue_progress VALUES (?,?,?,?,?)',
                       (channel, checkpoint, head, token, '2026-10-01T00:00:00+00:00'))

    def test_new_healthy_inventory_does_not_create_rescue_schema(self):
        self.assertEqual(rss_discovery.rescue_targets({A}, set()), set())
        with database() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='api_rescue_progress'").fetchone())

    def test_failures_are_still_rescued(self):
        self.assertEqual(rss_discovery.rescue_targets({A, B}, {A}), {A})

    def test_continuation_is_resumed_when_rss_is_healthy(self):
        self.progress(A, head='new', token='next')
        self.assertEqual(rss_discovery.rescue_targets({A}, set()), {A})

    def test_head_without_token_remains_unfinished(self):
        self.progress(A, head='new')
        self.assertEqual(rss_discovery.rescue_targets({A}, set()), {A})

    def test_completed_checkpoint_is_not_rescanned(self):
        self.progress(A)
        self.assertEqual(rss_discovery.rescue_targets({A}, set()), set())

    def test_removed_channels_are_not_resumed(self):
        self.progress(B, head='new', token='next')
        self.assertEqual(rss_discovery.rescue_targets({A}, {B}), set())

    def test_failure_and_continuation_are_deduplicated(self):
        self.progress(A, head='new', token='next')
        self.assertEqual(rss_discovery.rescue_targets({A, B}, {A, B}), {A, B})

    def errors(self, error):
        api_rescue.initialize()
        with database() as db:
            db.execute('INSERT INTO api_rescue_channels(channel_id,error) VALUES (?,?)', (A, error))

    def test_connection_failure_is_retried_after_rss_recovery(self):
        self.errors('ConnectionError')
        self.assertEqual(rss_discovery.rescue_targets({A}, set()), {A})

    def test_retryable_upstream_error_is_retried(self):
        self.errors('API_RESCUE_HTTP_503_backendError')
        self.assertEqual(rss_discovery.rescue_targets({A}, set()), {A})

    def test_absent_playlist_is_not_an_unbounded_retry_target(self):
        self.errors('API_RESCUE_HTTP_404_playlistNotFound')
        self.assertEqual(rss_discovery.rescue_targets({A}, set()), set())

    def test_outage_recovery_keeps_unchecked_channels_until_caught_up(self):
        api_rescue.initialize()
        with database() as db:
            db.executemany('INSERT INTO api_rescue_channels(channel_id,checked) VALUES (?,?)',
                           [(A, 150), (B, 250)])
        rss_discovery.note_outage(now=100)
        rss_discovery.note_outage(now=140)
        with database() as db:
            self.assertEqual(db.execute('SELECT started FROM rss_recovery_episode').fetchone()[0], 100)
        rss_discovery.note_recovered(now=200)
        self.assertEqual(rss_discovery.rescue_targets({A, B}, set()), {A})
        with database() as db:
            db.execute('UPDATE api_rescue_channels SET checked=251 WHERE channel_id=?', (A,))
        self.assertEqual(rss_discovery.rescue_targets({A, B}, set()), set())
        with database() as db:
            self.assertEqual(db.execute('SELECT complete FROM rss_recovery_episode').fetchone()[0], 1)
        rss_discovery.note_outage(now=300)
        with database() as db:
            row = db.execute('SELECT active,started,ended,complete FROM rss_recovery_episode').fetchone()
            self.assertEqual(tuple(row), (1, 300, 0, 0))

    def test_outage_recovery_ignores_a_known_missing_uploads_playlist(self):
        self.errors('API_RESCUE_HTTP_404_playlistNotFound')
        rss_discovery.note_outage(now=100)
        rss_discovery.note_recovered(now=200)
        self.assertEqual(rss_discovery.rescue_targets({A}, set()), set())

    def test_outage_recovery_adds_channels_without_prior_api_state(self):
        rss_discovery.note_outage(now=100)
        rss_discovery.note_recovered(now=200)
        self.assertEqual(rss_discovery.rescue_targets({A, B}, set()), {A, B})

    def test_healthy_hot_scan_ends_outage_and_full_scan_continues_recovery(self):
        rss_discovery.note_outage(now=100)
        self.collect('hot').assert_not_called()
        with database() as db:
            self.assertEqual(db.execute('SELECT active FROM rss_recovery_episode').fetchone()[0], 0)
        self.collect('full').assert_called_once_with({A})

    def collect(self, mode):
        config = Mock(RSS_WORKERS=1)
        with patch.object(rss, 'check_rss_feed', return_value=[]), \
             patch.object(rss_circuit, 'claim', return_value='normal'), \
             patch.object(rss_circuit, 'ordered_channels', side_effect=lambda group: sorted(group)), \
             patch.object(rss_discovery, 'report_push_gap_health'), \
             patch.object(api_rescue, 'run') as rescue:
            rss_discovery.collect({A}, config, mode)
            return rescue

    def test_real_collect_includes_unfinished_healthy_channel(self):
        self.progress(A, head='new', token='next')
        self.collect('full').assert_called_once_with({A})

    def test_hot_scan_does_not_add_another_history_schedule(self):
        self.progress(A, head='new', token='next')
        self.collect('hot').assert_not_called()

    def test_cached_inventory_requires_a_recent_valid_list(self):
        with database() as db:
            db.execute('CREATE TABLE rss_inventory (id INTEGER PRIMARY KEY, updated REAL, payload TEXT)')
            db.execute('INSERT INTO rss_inventory VALUES (1,?,?)', (100, json.dumps([A, B])))
        self.assertEqual(rss_discovery.cached_inventory(now=200), {A, B})
        self.assertEqual(rss_discovery.cached_inventory(now=3700), {A, B})
        self.assertEqual(rss_discovery.cached_inventory(now=7299), {A, B})
        self.assertEqual(rss_discovery.cached_inventory(now=7300), set())
        self.assertEqual(rss_discovery.cached_inventory(now=99), set())
        with database() as db:
            db.execute("UPDATE rss_inventory SET payload='{bad' WHERE id=1")
        self.assertEqual(rss_discovery.cached_inventory(now=200), set())

    def test_hot_outage_rescues_full_inventory_without_extra_passes(self):
        with patch.object(rss_circuit, 'claim', return_value='skip'), \
             patch.object(rss_discovery, 'cached_inventory', return_value={A, B}), \
             patch.object(rss_discovery, 'report_push_gap_health'), \
             patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                rss_discovery.collect({A}, Mock(RSS_WORKERS=1), 'hot')
        rescue.assert_called_once_with({A, B}, priority_channels={A})

    def test_hot_outage_without_recent_inventory_keeps_existing_scope(self):
        with patch.object(rss_circuit, 'claim', return_value='skip'), \
             patch.object(rss_discovery, 'cached_inventory', return_value=set()), \
             patch.object(rss_discovery, 'report_push_gap_health'), \
             patch.object(api_rescue, 'run') as rescue:
            with self.assertRaisesRegex(RuntimeError, 'UPSTREAM_COOLDOWN'):
                rss_discovery.collect({A}, Mock(RSS_WORKERS=1), 'hot')
        rescue.assert_called_once_with({A})


if __name__ == '__main__':
    unittest.main()
