import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
import requests
sys.path.insert(0, str(Path(__file__).parents[1] / 'src'))
import rss

class RssRetryTests(unittest.TestCase):
    def test_static_discovery_document_is_not_success(self):
        response = Mock(status_code=200, content=b'<feed xmlns="http://www.w3.org/2005/Atom"><title>YouTube video feed</title></feed>')
        with patch('rss.requests.get', return_value=response), patch('rss.time.sleep'):
            self.assertIsNone(rss.check_rss_feed('fixture'))
            self.assertEqual(rss.failure_reasons['fixture'], 'STATIC_DISCOVERY_DOCUMENT')
    def setUp(self):
        rss.failure_reasons.clear()
        rss.failure_retry_at.clear()
        rss.failed_channels.clear()

    def test_proxy_fallback_recovers_transport_failure(self):
        good = Mock(status_code=200, content=b'<feed xmlns="http://www.w3.org/2005/Atom"/>')
        with patch('rss.requests.get', side_effect=[requests.ConnectionError(), good]) as get, patch('rss.time.sleep'):
            self.assertEqual(rss.check_rss_feed('fixture'), [])
            self.assertIn('www.youtube.com/', get.call_args_list[0].args[0])
            self.assertEqual(get.call_count, 2)
            self.assertNotIn('fixture', rss.failure_reasons)

    def test_denied_source_is_not_bypassed(self):
        with patch('rss.requests.get', return_value=Mock(status_code=403)) as get, patch('rss.time.sleep'):
            self.assertIsNone(rss.check_rss_feed('fixture'))
            self.assertEqual(get.call_count, 1)

    def test_explicit_upstream_failures_do_not_call_proxy_or_retry(self):
        for status in (404, 429, 500, 502, 503, 504):
            with self.subTest(status=status), patch('rss.requests.get', return_value=Mock(status_code=status)) as get, \
                    patch('rss.time.sleep'), patch('builtins.print') as log:
                self.assertIsNone(rss.check_rss_feed('fixture'))
                self.assertEqual(get.call_count, 1)
                self.assertEqual(rss.failure_reasons['fixture'], 'HTTP_' + str(status))
                self.assertIn('attempts=1', log.call_args.args[0])

    def test_retry_after_is_retained_and_does_not_trigger_another_transport(self):
        response = Mock(status_code=503, content=b'unavailable', headers={'Retry-After': '7200'})
        with patch('rss.requests.get', return_value=response) as get, patch('rss.time.sleep'), \
                patch('rss.time.time', return_value=100):
            self.assertIsNone(rss.check_rss_feed('fixture'))
        self.assertEqual(get.call_count, 1)
        self.assertEqual(rss.failure_retry_at['fixture'], 7300)

    def test_challenge_with_status_200_stops_instead_of_parsing_or_retrying(self):
        response = Mock(status_code=200, content=b'<html>unusual traffic captcha</html>', headers={})
        with patch('rss.requests.get', return_value=response) as get, patch('rss.time.sleep'):
            self.assertIsNone(rss.check_rss_feed('fixture'))
        self.assertEqual(get.call_count, 1)
        self.assertEqual(rss.failure_reasons['fixture'], 'RSS_ACCESS_CHALLENGE')
        self.assertIn('fixture', rss.failure_retry_at)

    def test_fresh_success_is_not_discarded_because_of_a_previous_denial(self):
        rss.failure_reasons['fixture'] = 'HTTP_429'
        rss.failure_retry_at['fixture'] = 1
        response = Mock(status_code=200, content=b'<feed xmlns="http://www.w3.org/2005/Atom"/>')
        with patch('rss.requests.get', return_value=response) as get, patch('rss.time.sleep'):
            self.assertEqual(rss.check_rss_feed('fixture'), [])
        self.assertEqual(get.call_count, 1)
        self.assertNotIn('fixture', rss.failure_reasons)
        self.assertNotIn('fixture', rss.failure_retry_at)

    def test_invalid_html_is_not_an_empty_success(self):
        html = Mock(status_code=200, content=b'<html/>')
        with patch('rss.requests.get', return_value=html) as get, patch('rss.time.sleep'), patch('builtins.print'):
            self.assertIsNone(rss.check_rss_feed('fixture'))
            self.assertEqual(get.call_count, 3)
            self.assertEqual(rss.failure_reasons['fixture'], 'NOT_ATOM_FEED')

    def test_recovery_clears_prior_failure(self):
        rss.failed_channels.add('fixture')
        with patch('rss._check_rss_feed_once', side_effect=[None, None, []]), patch('rss.time.sleep'):
            self.assertEqual(rss.check_rss_feed('fixture'), [])
            self.assertNotIn('fixture', rss.failed_channels)
