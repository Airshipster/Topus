import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0, str(Path(__file__).parents[1] / 'src'))
import rss

class RssRetryTests(unittest.TestCase):
    def setUp(self):
        rss.failure_reasons.clear()
        rss.failed_channels.clear()

    def test_direct_fallback_recovers_proxy_failure(self):
        good = Mock(status_code=200, content=b'<feed xmlns="http://www.w3.org/2005/Atom"/>')
        with patch('rss.requests.get', side_effect=[Mock(status_code=503), good]) as get, patch('rss.time.sleep'):
            self.assertEqual(rss.check_rss_feed('fixture'), [])
            self.assertIn('youtube.com/feeds/videos.xml', get.call_args_list[1].args[0])
            self.assertEqual(get.call_count, 2)
            self.assertNotIn('fixture', rss.failure_reasons)

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
