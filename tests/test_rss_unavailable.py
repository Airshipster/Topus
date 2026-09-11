import unittest
import sys
from pathlib import Path
from unittest.mock import Mock,patch
sys.path.insert(0,str(Path(__file__).parents[1]/'src'))
import rss


class RssUnavailableTests(unittest.TestCase):
    def test_only_valid_api_response_confirms_absence(self):
        response=Mock(status_code=200)
        response.json.return_value={'kind':'youtube#channelListResponse','items':[{'id':'live'}]}
        with patch('rss.config.YOUTUBE_API_KEYS',['fixture']),patch('rss.requests.get',return_value=response):
            self.assertEqual(rss.confirmed_unavailable_channels({'live','missing'}),{'missing'})
            response.json.return_value={'kind':'youtube#channelListResponse','pageInfo':{'totalResults':0}}
            self.assertEqual(rss.confirmed_unavailable_channels({'missing'}),{'missing'})
            response.json.return_value={'kind':'youtube#channelListResponse'}
            self.assertEqual(rss.confirmed_unavailable_channels({'live'}),set())
            response.json.return_value={}
            self.assertEqual(rss.confirmed_unavailable_channels({'live'}),set())
            response.status_code=403
            self.assertEqual(rss.confirmed_unavailable_channels({'live'}),set())
