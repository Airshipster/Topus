import json
import unittest
from unittest.mock import Mock, patch

import requests
import gspread
from sheets import get_values_with_quota_retry, load_youtube_channels


def api_error(status):
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps({'error': {'code': status, 'message': 'test'}}).encode()
    return gspread.exceptions.APIError(response)


class TransientReadTests(unittest.TestCase):
    def test_metadata_recovers_without_losing_channels(self):
        client = Mock()
        spreadsheet = Mock()
        client.open_by_key.side_effect = [api_error(500), spreadsheet]
        spreadsheet.worksheet.side_effect = [api_error(503), Mock(id=1)]
        project = {'sheet_id': 'test', 'name': 'test', 'channels_sheet_name': 'Channels'}
        with patch('sheets.time.sleep'), patch('sheets.parse_youtube_channels_worksheet', return_value={'channel': {}}):
            self.assertEqual(load_youtube_channels(client, project), {'channel': {}})
        self.assertNotIn('channels_error', project)
        self.assertEqual(client.open_by_key.call_count, 2)
        self.assertEqual(spreadsheet.worksheet.call_count, 2)
        spreadsheet.worksheets.assert_not_called()

    def test_metadata_failure_must_not_select_another_sheet(self):
        client = Mock()
        spreadsheet = client.open_by_key.return_value
        spreadsheet.worksheet.side_effect = api_error(503)
        project = {'sheet_id': 'test', 'name': 'test', 'channels_sheet_name': 'Channels'}
        with patch('sheets.time.sleep'):
            self.assertEqual(load_youtube_channels(client, project), {})
        self.assertIn('channels_error', project)
        self.assertEqual(spreadsheet.worksheet.call_count, 3)
        spreadsheet.worksheets.assert_not_called()

    def test_recovers_service_unavailable(self):
        sheet = Mock(title='test')
        sheet.get.side_effect = [api_error(503), [['ok']]]
        with patch('sheets.time.sleep'):
            self.assertEqual(get_values_with_quota_retry(sheet, 'A1'), [['ok']])
        self.assertEqual(sheet.get.call_count, 2)

    def test_does_not_retry_permission_errors(self):
        sheet = Mock(title='test')
        sheet.get.side_effect = api_error(403)
        with patch('sheets.time.sleep') as sleep:
            with self.assertRaises(gspread.exceptions.APIError):
                get_values_with_quota_retry(sheet, 'A1')
        sleep.assert_not_called()

    def test_persistent_error_is_bounded(self):
        sheet = Mock(title='test')
        sheet.get.side_effect = api_error(503)
        with patch('sheets.time.sleep'):
            with self.assertRaises(gspread.exceptions.APIError):
                get_values_with_quota_retry(sheet, 'A1', attempts=3)
        self.assertEqual(sheet.get.call_count, 3)
