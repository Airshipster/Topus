import os
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).parents[1] / 'src'))
import config
import sheets
import main
from retry_queue import pending_events


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

    def test_pending_recovery_is_timely_but_not_unbounded(self):
        now = datetime(2026, 9, 22, 12, 0, tzinfo=main.current_local_datetime().tzinfo)
        with patch('main.current_local_datetime', return_value=now):
            self.assertEqual(main.get_stale_reason('2026-09-22 11:30:00', {}, {'retry_accepted': True}), '')
            self.assertIn('60-minute', main.get_stale_reason('2026-09-22 10:59:00', {}, {'retry_accepted': True}))

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
