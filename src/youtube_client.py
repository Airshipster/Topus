import re
from datetime import datetime

import requests

import config

youtube_api_calls = 0


def format_youtube_timestamp(value):
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return value


def parse_video_dimensions(player):
    embed_html = player.get('embedHtml', '') if player else ''
    width_match = re.search(r'\bwidth="(\d+)"', embed_html)
    height_match = re.search(r'\bheight="(\d+)"', embed_html)

    if not width_match or not height_match:
        return None, None

    return int(width_match.group(1)), int(height_match.group(1))


def get_video_info_from_api(video_id):
    """Получение информации о видео через YouTube Data API v3"""
    global youtube_api_calls
    
    api_keys = config.YOUTUBE_API_KEYS or ([config.YOUTUBE_API_KEY] if config.YOUTUBE_API_KEY else [])
    if not api_keys:
        return None
    
    for api_key in api_keys:
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            'part': 'snippet,contentDetails,liveStreamingDetails,player',
            'id': video_id,
            'key': api_key
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            youtube_api_calls += 1

            if response.status_code in (403, 429, 500, 502, 503, 504):
                continue

            if response.status_code != 200:
                return None

            data = response.json()
            if not data.get('items'):
                return None

            item = data['items'][0]
            snippet = item['snippet']
            content_details = item.get('contentDetails', {})
            live_details = item.get('liveStreamingDetails', {})
            width, height = parse_video_dimensions(item.get('player', {}))

            is_short = False
            short_reasons = []
            duration_seconds = 0
            duration_str = content_details.get('duration', '')
            if duration_str:
                match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
                if match:
                    hours = int(match.group(1) or 0)
                    minutes = int(match.group(2) or 0)
                    seconds = int(match.group(3) or 0)
                    duration_seconds = hours * 3600 + minutes * 60 + seconds
                    if duration_seconds <= 60:
                        is_short = True
                        short_reasons.append(f"duration {duration_seconds}s")

            if width and height:
                if height > width:
                    is_short = True
                    short_reasons.append(f"vertical {width}x{height}")
                elif height == width:
                    is_short = True
                    short_reasons.append(f"square {width}x{height}")

            is_live = live_details.get('actualStartTime') is not None
            is_upcoming = snippet.get('liveBroadcastContent') == 'upcoming'

            return {
                'title': snippet['title'],
                'channel': snippet['channelTitle'],
                'channel_id': snippet['channelId'],
                'published': format_youtube_timestamp(snippet['publishedAt']),
                'is_short': is_short,
                'short_reason': ', '.join(short_reasons),
                'is_live': is_live,
                'is_upcoming': is_upcoming,
                'duration': duration_str,
                'duration_seconds': duration_seconds,
                'width': width,
                'height': height,
            }
        except Exception:
            continue

    return None


def get_youtube_api_calls():
    return youtube_api_calls
