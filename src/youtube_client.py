import re
from datetime import datetime

import requests

import config

youtube_api_calls = 0
last_youtube_api_error = None


def mask_api_key(api_key):
    value = str(api_key or '')
    if len(value) <= 8:
        return '***'
    return f'{value[:4]}...{value[-4:]}'


def extract_youtube_error(response):
    try:
        payload = response.json()
    except Exception:
        return f'HTTP {response.status_code}'

    error = payload.get('error', {}) if isinstance(payload, dict) else {}
    errors = error.get('errors') or []
    reason = ''
    message = error.get('message') or ''
    if errors and isinstance(errors[0], dict):
        reason = errors[0].get('reason') or ''
        message = errors[0].get('message') or message

    detail = reason or message or f'HTTP {response.status_code}'
    if reason and message and reason not in message:
        detail = f'{reason}: {message}'
    return f'HTTP {response.status_code} {detail}'.strip()


def is_retryable_youtube_status(status_code):
    return status_code in (403, 429, 500, 502, 503, 504)


def detect_shorts_from_web(video_id):
    """Best-effort Shorts check without spending YouTube Data API quota."""
    try:
        response = requests.get(
            f'https://www.youtube.com/watch?v={video_id}',
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=5,
        )
        if response.status_code != 200:
            return False
        html = response.text
        return (
            f'href="https://www.youtube.com/shorts/{video_id}"' in html
            or f'content="https://www.youtube.com/shorts/{video_id}"' in html
            or '"isShortsEligible":true' in html
        )
    except Exception:
        return False


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
    global youtube_api_calls, last_youtube_api_error
    last_youtube_api_error = None
    
    api_keys = config.YOUTUBE_API_KEYS or ([config.YOUTUBE_API_KEY] if config.YOUTUBE_API_KEY else [])
    if not api_keys:
        last_youtube_api_error = 'YouTube API keys are not configured'
        return None

    errors = []
    
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

            if is_retryable_youtube_status(response.status_code):
                error = extract_youtube_error(response)
                errors.append(f'{mask_api_key(api_key)}: {error}')
                print(f"  ⚠️  YouTube API key {mask_api_key(api_key)} failed: {error}")
                continue

            if response.status_code != 200:
                last_youtube_api_error = f'{mask_api_key(api_key)}: {extract_youtube_error(response)}'
                return None

            data = response.json()
            if not data.get('items'):
                last_youtube_api_error = None
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

            if width and height and is_short:
                if height > width:
                    short_reasons.append(f"vertical {width}x{height}")
                elif height == width:
                    short_reasons.append(f"square {width}x{height}")

            if not is_short and detect_shorts_from_web(video_id):
                is_short = True
                short_reasons.append("YouTube Shorts canonical")

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
        except Exception as e:
            errors.append(f'{mask_api_key(api_key)}: request failed: {e}')
            print(f"  ⚠️  YouTube API key {mask_api_key(api_key)} request failed: {e}")
            continue

    if errors:
        last_youtube_api_error = '; '.join(errors)
    return None


def get_youtube_api_calls():
    return youtube_api_calls


def get_last_youtube_api_error():
    return last_youtube_api_error
