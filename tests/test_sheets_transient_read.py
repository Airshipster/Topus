import json
import unittest
from unittest.mock import Mock, patch

import requests
import gspread
from sheets import get_values_with_quota_retry


def api_error(status):
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps({'error': {'code': status, 'message': 'test'}}).encode()
    return gspread.exceptions.APIError(response)


class TransientReadTests(unittest.TestCase):
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
