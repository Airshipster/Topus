import sys
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

import config
import youtube_client
from sheets import first_value, row_as_dict, status_name_from_text


class YouTubeClientTests(unittest.TestCase):
    def test_long_landscape_video_does_not_use_shorts_html_fallback(self):
        config.YOUTUBE_API_KEYS = ['test-key']
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            'items': [{
                'snippet': {
                    'title': 'Вы ЕДИТЕ ЭТО каждый день! Как устроены ароматизаторы?',
                    'channelTitle': 'Test channel',
                    'channelId': 'channel',
                    'publishedAt': '2026-09-12T08:05:44Z',
                    'liveBroadcastContent': 'none',
                },
                'contentDetails': {'duration': 'PT1H5M36S'},
                'player': {'embedHtml': '<iframe width="480" height="270"></iframe>'},
            }]
        }

        with patch.object(youtube_client.requests, 'get', return_value=response), patch.object(
            youtube_client, 'detect_shorts_from_web', side_effect=AssertionError('must not be called')
        ):
            video = youtube_client.get_video_info_from_api('b6Ky9Pz1olM')

        self.assertEqual(video['duration_seconds'], 3936)
        self.assertFalse(video['is_short'])

    def test_embed_viewport_never_classifies_video_as_short(self):
        config.YOUTUBE_API_KEYS = ['test-key']
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            'items': [{
                'snippet': {'title': 'Long video', 'channelTitle': 'Test', 'channelId': 'channel',
                            'publishedAt': '2026-09-12T08:05:44Z', 'liveBroadcastContent': 'none'},
                'contentDetails': {'duration': 'PT1H5M36S'},
                'player': {'embedHtml': '<iframe width="270" height="480"></iframe>'},
            }]
        }
        with patch.object(youtube_client.requests, 'get', return_value=response):
            video = youtube_client.get_video_info_from_api('b6Ky9Pz1olM')
        self.assertFalse(video['is_short'])

    def test_short_form_candidate_uses_canonical_shorts_check(self):
        config.YOUTUBE_API_KEYS = ['test-key']
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            'items': [{
                'snippet': {'title': 'Короткий научный ролик', 'channelTitle': 'Test', 'channelId': 'channel',
                            'publishedAt': '2026-09-12T08:05:44Z', 'liveBroadcastContent': 'none'},
                'contentDetails': {'duration': 'PT2M5S'},
                'player': {},
            }]
        }
        with patch.object(youtube_client.requests, 'get', return_value=response), patch.object(
            youtube_client, 'detect_shorts_from_web', return_value=True
        ):
            video = youtube_client.get_video_info_from_api('shortcandidate')
        self.assertTrue(video['is_short'])
        self.assertEqual('YouTube Shorts canonical', video['short_reason'])

    def test_formula_status_header_preserves_retry_state(self):
        headers = ['Проект', 'Системный статус\nPush: ✅1, RSS: ✅2']
        row = ['SciTopus', 'RSS: retry. Requeued after classifier fix']
        data = row_as_dict(headers, row)

        self.assertEqual(status_name_from_text(first_value(data, ['Системный статус'])), 'retry')
