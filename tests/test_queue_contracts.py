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

    def test_pending_status_refresh_uses_one_batch_write(self):
        worksheet = Mock()
        headers = sheets.VIDEO_HEADERS
        worksheet.get_all_values.return_value = [headers, [
            'SciTopus', 'Test channel', 'https://youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa',
            'Premiere', 'youtube.com/watch?v=abcdefghijk', '23.09.2026 12:00:00',
            '23.09.2026 12:01:00', '', '', '',
            'Push: pending. Awaiting premiere publication [retry-after=2026-09-23T14:00:00Z]',
        ]]
        sheet = Mock()
        video = {
            'video_id': 'abcdefghijk', 'url': 'https://www.youtube.com/watch?v=abcdefghijk',
            'channel_id': 'UCaaaaaaaaaaaaaaaaaaaaaa', 'channel': 'Test channel', 'source_method': 'Push',
        }
        pending = 'Awaiting premiere publication [retry-after=2026-09-23T15:00:00Z]'

        with patch('sheets.ensure_videos_worksheet', return_value=worksheet), \
             patch('sheets.get_values_with_quota_retry', return_value=[headers]), \
             patch('sheets.update_video_publication_status') as single_row_update:
            result = sheets.save_videos_batch(
                sheet, [(video, {'name': 'SciTopus'}, '23.09.2026 12:00:00', None, f'PENDING: {pending}')]
            )

        self.assertEqual(result, [('abcdefghijk', 'SciTopus')])
        single_row_update.assert_not_called()
        worksheet.batch_update.assert_called_once()
        self.assertEqual(worksheet.batch_update.call_args.args[0][0]['range'], 'K2')
        self.assertIn('retry-after=2026-09-23T15:00:00Z',
                      worksheet.batch_update.call_args.args[0][0]['values'][0][0])

    def test_publication_reconciliation_restores_source_from_event_log(self):
        video_ws, logs_ws, book = Mock(), Mock(), Mock()
        video_ws.get_all_values.return_value = [sheets.VIDEO_HEADERS, [
            'SciTopus', 'Test channel', 'https://youtube.com/channel/UCaaaaaaaaaaaaaaaaaaaaaa',
            'Test title', 'https://youtube.com/watch?v=abcdefghijk', '23.09.2026 12:00:00',
            '23.09.2026 12:01:00', '', '', '', 'pending',
        ]]
        logs_ws.get_all_values.return_value = [sheets.LOG_HEADERS, [
            'SciTopus', '23.09.2026 12:05:00', 'abcdefghijk', 'UCaaaaaaaaaaaaaaaaaaaaaa',
            'Push: Video published. Telegram msg: 71719',
        ]]
        with patch('sheets.ensure_videos_worksheet', return_value=video_ws), \
             patch('sheets.ensure_logs_worksheet', return_value=logs_ws):
            self.assertEqual(sheets.reconcile_pending_published_videos(book), 1)
        updates = video_ws.batch_update.call_args.args[0]
        status_update = next(update for update in updates if update['range'] == 'K2')
        self.assertEqual(status_update['values'], [['Push: published']])
