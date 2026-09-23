import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).parents[1] / 'src'))
import config
import sheets
import main
from retry_queue import pending_events, retry_deferred


class QueueContractTests(unittest.TestCase):
    def test_permission_error_is_not_busy(self):
        book = Mock()
        book.worksheet.side_effect = PermissionError('403')
        with self.assertRaises(PermissionError):
            sheets.acquire_lock(book)

    def test_failed_is_retryable_but_sent_is_not(self):
        self.assertFalse(sheets.row_status_blocks_retry('failed'))
        self.assertFalse(sheets.row_status_blocks_retry('pending'))
        self.assertTrue(sheets.row_status_blocks_retry('published'))

    def test_manual_replay_bypasses_age_but_regular_retry_does_not(self):
        self.assertEqual(main.get_stale_reason('2020-01-01', {}, {'manual_replay': True}), '')
        self.assertNotEqual(main.get_stale_reason('2020-01-01', {}, {'retry_accepted': True}), '')

    def test_push_read_error_is_not_empty_success(self):
        book = Mock()
        book.worksheet.side_effect = PermissionError('403')
        with self.assertRaises(PermissionError):
            sheets.get_push_events(book)

    def test_pending_stream_replay_is_scoped_to_project(self):
        values = [['Проект', 'Ссылка на видео', 'Ссылка на канал', 'Системный статус', 'TG message_id'],
                  ['SciTopus', 'https://www.youtube.com/watch?v=abcdefghijk',
                   'https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa/videos', 'RSS: pending', '']]
        with patch('retry_queue.get_values_with_quota_retry', return_value=values):
            result = pending_events(Mock())
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['retry_project'], 'SciTopus')
        self.assertTrue(result[0]['retry_accepted'])

    def test_future_premiere_retry_is_deferred_until_schedule(self):
        now = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)
        future_status = 'Push: pending. Awaiting premiere publication [retry-after=2026-09-25T14:00:00Z]'
        self.assertTrue(retry_deferred(future_status, now=now))

        values = [['Проект', 'Ссылка на видео', 'Ссылка на канал', 'Системный статус', 'TG message_id'],
                  ['SciTopus', 'https://www.youtube.com/watch?v=abcdefghijk',
                   'https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa/videos',
                   'Push: pending. Awaiting premiere publication [retry-after=2999-01-01T00:00:00Z]', '']]
        with patch('retry_queue.get_values_with_quota_retry', return_value=values):
            self.assertEqual(pending_events(Mock()), [])

    def test_due_premiere_retry_reenters_queue(self):
        values = [['Проект', 'Ссылка на видео', 'Ссылка на канал', 'Системный статус', 'TG message_id'],
                  ['SciTopus', 'https://www.youtube.com/watch?v=abcdefghijk',
                   'https://www.youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa/videos',
                   'Push: pending. Awaiting premiere publication [retry-after=2020-01-01T00:00:00Z]', '']]
        with patch('retry_queue.get_values_with_quota_retry', return_value=values):
            result = pending_events(Mock())
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['video_id'], 'abcdefghijk')
