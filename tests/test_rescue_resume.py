import os
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


if __name__ == '__main__':
    unittest.main()
