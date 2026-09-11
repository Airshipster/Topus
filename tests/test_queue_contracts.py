import os
import sys
import unittest
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

    def test_pending_not_lost_after_discovery_age_window(self):
        self.assertEqual(main.get_stale_reason('2020-01-01', {}, {'retry_accepted': True}), '')
        self.assertNotEqual(main.get_stale_reason('2020-01-01', {}, {}), '')

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
