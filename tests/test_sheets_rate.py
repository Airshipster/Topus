import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch
from gspread.exceptions import APIError
from sheets_rate import reserve, CoordinatedSheetsClient


class SheetsRateTests(unittest.TestCase):
    def test_independent_clients_share_slots(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'rate.db')
            reserve(path, 90)
            with ThreadPoolExecutor(max_workers=8) as pool:
                delays = list(pool.map(lambda _: reserve(path, 100), range(8)))
            self.assertEqual(sorted(delays), [i * 1.25 for i in range(8)])

    def test_permission_error_is_not_retried(self):
        response = Mock(status_code=403)
        response.json.return_value = {'error': {'code': 403, 'message': 'permission', 'status': 'PERMISSION_DENIED'}}
        with patch('sheets_rate.reserve', return_value=0), patch('sheets_rate.time.sleep'), patch('gspread.http_client.HTTPClient.request', side_effect=APIError(response)) as call:
            with self.assertRaises(APIError):
                CoordinatedSheetsClient.__new__(CoordinatedSheetsClient).request('GET', 'https://example.test')
        self.assertEqual(call.call_count, 1)
