import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).parents[1] / 'src'))
from rss_diagnostics import http_failure_evidence, log_http_failure
import rss


class RssDiagnosticsTests(unittest.TestCase):
    def response(self, body, status=404, headers=None):
        return SimpleNamespace(content=body, status_code=status,
                               headers=headers or {'Content-Type': 'text/html; charset=utf-8'})

    def test_not_found_classification_without_body_or_headers_in_output(self):
        response = self.response(b'<html>404 The requested URL /private?key=SECRET was not found on this server.</html>',
                                 headers={'Content-Type': 'text/html', 'Set-Cookie': 'SECRET',
                                          'Location': 'https://example.org/?key=SECRET'})
        evidence = http_failure_evidence(response, direct=True)
        self.assertEqual(evidence['bodyKind'], 'not-found-page')
        self.assertEqual(evidence['transport'], 'direct')
        self.assertNotIn('SECRET', json.dumps(evidence))
        self.assertNotIn('private', json.dumps(evidence))
        self.assertNotIn('Set-Cookie', evidence)

    def test_bounded_hash_and_content_type_validation(self):
        evidence = http_failure_evidence(self.response(b'x' * 100000,
            headers={'Content-Type': 'unsafe\nSECRET'}), direct=False)
        self.assertEqual(evidence['sampleBytes'], 4096)
        self.assertEqual(len(evidence['sampleSha256']), 64)
        self.assertEqual(evidence['contentType'], 'unknown')

    def test_quota_and_challenge_have_distinct_classes(self):
        for body, kind in ((b'{"reason":"quotaExceeded"}', 'quota-error'),
                           (b'Sign in to confirm unusual traffic', 'access-challenge')):
            with self.subTest(kind=kind):
                self.assertEqual(http_failure_evidence(self.response(body), direct=True)['bodyKind'], kind)

    def test_retry_after_seconds_and_date(self):
        self.assertEqual(http_failure_evidence(self.response(b'error', headers={'Retry-After': '120'}),
                                               direct=True)['retryAfterSeconds'], 120)
        result = http_failure_evidence(self.response(b'error', headers={
            'Retry-After': 'Thu, 08 Oct 2026 08:00:00 GMT'}), direct=True, now=1791446370)
        self.assertEqual(result['retryAfterSeconds'], 30)

    def test_invalid_diagnostics_cannot_change_request_outcome(self):
        with patch('builtins.print', side_effect=RuntimeError('fixture')):
            self.assertIsNone(log_http_failure(self.response(b'error'), direct=True))

    def test_404_still_makes_one_request_and_never_calls_proxy(self):
        response = self.response(b'<html>404 was not found on this server</html>')
        with patch('rss.requests.get', return_value=response) as get, patch('rss.time.sleep'), patch('builtins.print') as log:
            self.assertIsNone(rss.check_rss_feed('fixture'))
            self.assertEqual(get.call_count, 1)
            self.assertEqual(rss.failure_reasons['fixture'], 'HTTP_404')
            self.assertTrue(any(call.args[0].startswith('RSS_HTTP_EVIDENCE ') for call in log.call_args_list))

    def test_mock_response_metadata_is_optional(self):
        response = Mock(status_code=503)
        self.assertEqual(http_failure_evidence(response, direct=True)['bodyKind'], 'empty-or-unavailable')


if __name__ == '__main__':
    unittest.main()
