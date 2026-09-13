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
                'player': {'embedWidth': '1280', 'embedHeight': '720'},
            }]
        }

        with patch.object(youtube_client.requests, 'get', return_value=response), patch.object(
            youtube_client, 'detect_shorts_from_web', side_effect=AssertionError('must not be called')
        ):
            video = youtube_client.get_video_info_from_api('b6Ky9Pz1olM')

        self.assertEqual(video['duration_seconds'], 3936)
        self.assertFalse(video['is_short'])

    def test_vertical_format_is_a_short_even_for_a_longer_video(self):
        config.YOUTUBE_API_KEYS = ['test-key']
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            'items': [{
                'snippet': {'title': 'Long video', 'channelTitle': 'Test', 'channelId': 'channel',
                            'publishedAt': '2026-09-12T08:05:44Z', 'liveBroadcastContent': 'none'},
                'contentDetails': {'duration': 'PT1H5M36S'},
                'player': {'embedWidth': '270', 'embedHeight': '480'},
            }]
        }
        with patch.object(youtube_client.requests, 'get', return_value=response):
            video = youtube_client.get_video_info_from_api('b6Ky9Pz1olM')
        self.assertTrue(video['is_short'])
        self.assertEqual('vertical 270x480', video['short_reason'])

    def test_short_form_candidate_uses_canonical_shorts_check_when_duration_is_missing(self):
        config.YOUTUBE_API_KEYS = ['test-key']
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            'items': [{
                'snippet': {'title': 'Короткий научный ролик', 'channelTitle': 'Test', 'channelId': 'channel',
                            'publishedAt': '2026-09-12T08:05:44Z', 'liveBroadcastContent': 'none'},
                'contentDetails': {},
                'player': {},
            }]
        }
        with patch.object(youtube_client.requests, 'get', return_value=response), patch.object(
            youtube_client, 'detect_shorts_from_web', return_value=True
        ):
            video = youtube_client.get_video_info_from_api('shortcandidate')
        self.assertTrue(video['is_short'])
        self.assertEqual('YouTube Shorts canonical', video['short_reason'])

    def test_duration_up_to_182_seconds_is_a_short(self):
        config.YOUTUBE_API_KEYS = ['test-key']
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            'items': [{
                'snippet': {'title': 'Короткий научный ролик', 'channelTitle': 'Test', 'channelId': 'channel',
                            'publishedAt': '2026-09-12T08:05:44Z', 'liveBroadcastContent': 'none'},
                'contentDetails': {'duration': 'PT3M2S'},
                'player': {},
            }]
        }
        with patch.object(youtube_client.requests, 'get', return_value=response):
            video = youtube_client.get_video_info_from_api('shortduration')
        self.assertTrue(video['is_short'])
        self.assertEqual('duration 182s', video['short_reason'])

    def test_vertical_and_square_formats_are_shorts(self):
        config.YOUTUBE_API_KEYS = ['test-key']
        for dimensions, expected in ((('270', '480'), 'vertical 270x480'), (('480', '480'), 'square 480x480')):
            with self.subTest(dimensions=dimensions):
                response = Mock()
                response.status_code = 200
                response.json.return_value = {
                    'items': [{
                        'snippet': {'title': 'Формат ролика', 'channelTitle': 'Test', 'channelId': 'channel',
                                    'publishedAt': '2026-09-12T08:05:44Z', 'liveBroadcastContent': 'none'},
                        'contentDetails': {'duration': 'PT10M'},
                        'player': {'embedWidth': dimensions[0], 'embedHeight': dimensions[1]},
                    }]
                }
                with patch.object(youtube_client.requests, 'get', return_value=response):
                    video = youtube_client.get_video_info_from_api('shortformat')
                self.assertTrue(video['is_short'])
                self.assertEqual(expected, video['short_reason'])

    def test_formula_status_header_preserves_retry_state(self):
        headers = ['Проект', 'Системный статус\nPush: ✅1, RSS: ✅2']
        row = ['SciTopus', 'RSS: retry. Requeued after classifier fix']
        data = row_as_dict(headers, row)

        self.assertEqual(status_name_from_text(first_value(data, ['Системный статус'])), 'retry')

    def test_default_iframe_is_not_aspect_evidence(self):
        self.assertEqual(youtube_client.parse_video_dimensions({
            'embedHtml': '<iframe width="480" height="480"></iframe>'
        }), (None, None))

    def test_invalid_structured_dimensions(self):
        for player in ({}, {'embedWidth': None}, {'embedWidth': 'x'},
                       {'embedWidth': 10, 'embedHeight': 0}):
            self.assertEqual(youtube_client.parse_video_dimensions(player), (None, None))
