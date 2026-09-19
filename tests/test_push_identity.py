import unittest
from unittest.mock import Mock, patch
import sheets


class PushIdentityTest(unittest.TestCase):
    def test_shifted_event_is_marked_by_identity(self):
        headers = ['Video ID', 'Ссылка на канал', 'Timestamp GMT+4', 'Обработано', 'Проекты']
        values = [headers, ['abcdefghijk', 'UCaaaaaaaaaaaaaaaaaaaaaa', '19.09.2026 8:00:00', '', '']]
        book = Mock()
        tracked = {'row_index': 17, 'video_id': 'abcdefghijk', 'channel_id': 'UCaaaaaaaaaaaaaaaaaaaaaa',
                   'timestamp': '19.09.2026 8:00:00', 'project_names': {'SciTopus'}}
        with patch('sheets.get_values_with_quota_retry', return_value=values), patch('sheets.time.sleep'):
            sheets.mark_push_events_processed_batch(book, [tracked])
        updates = book.worksheet.return_value.batch_update.call_args.args[0]
        self.assertEqual(updates[0]['range'], 'D2')

    def test_missing_identity_never_marks_old_row(self):
        book = Mock()
        with patch('sheets.get_values_with_quota_retry', return_value=[['Video ID', 'Ссылка на канал', 'Timestamp GMT+4', 'Обработано', 'Проекты']]):
            sheets.mark_push_events_processed_batch(book, [{'row_index': 17}])
        book.worksheet.return_value.batch_update.assert_not_called()
