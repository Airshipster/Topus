import re
from html import unescape
from datetime import datetime
from zoneinfo import ZoneInfo

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
    """Best-effort Shorts check when structured metadata is inconclusive.

    Never request the Shorts route itself: YouTube can render that route with a
    Shorts canonical URL even for an ordinary video ID, which turns the route
    into a false-positive detector.
    """
    urls = [f'https://www.youtube.com/watch?v={video_id}']
    patterns = [
        rf'https://www\.youtube\.com/shorts/{re.escape(video_id)}',
        rf'"canonicalUrl"\s*:\s*"https://www\.youtube\.com/shorts/{re.escape(video_id)}"',
    ]
    for url in urls:
        try:
            response = requests.get(
                url,
                headers={'User-Agent': 'Mozilla/5.0'},
                timeout=10,
            )
            if response.status_code != 200:
                continue
            html = unescape(response.text).replace('\\/', '/')
            if any(re.search(pattern, html) for pattern in patterns):
                return True
        except Exception as error:
            print(f"  ⚠️  Shorts web check failed for {video_id}: {error}")
            continue
    return False


def format_youtube_timestamp(value):
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(ZoneInfo('Asia/Baku'))
        return f'{dt.day:02}.{dt.month:02}.{dt.year} {dt.hour}:{dt.minute:02}:{dt.second:02}'
    except Exception:
        return value


def parse_video_dimensions(player):
    # These fields are returned only when YouTube knows the aspect ratio.
    # Default iframe markup may describe a fallback player, not the video.
    try:
        width = int((player or {}).get('embedWidth', 0))
        height = int((player or {}).get('embedHeight', 0))
    except (TypeError, ValueError):
        return None, None
    return (width, height) if width > 0 and height > 0 else (None, None)


def _video_info_from_item(item, video_id=None):
    video_id = video_id or item.get('id', '')
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
            if duration_seconds <= 182:
                is_short = True
                short_reasons.append(f"duration {duration_seconds}s")

    if width and height:
        if height > width:
            is_short = True
            short_reasons.append(f"vertical {width}x{height}")
        elif height == width:
            is_short = True
            short_reasons.append(f"square {width}x{height}")

    if not is_short and not duration_seconds and video_id and detect_shorts_from_web(video_id):
        is_short = True
        short_reasons.append("YouTube Shorts canonical")

    is_live = (
        live_details.get('actualStartTime') is not None
        and live_details.get('actualEndTime') is None
    )
    was_live = live_details.get('actualStartTime') is not None
    is_upcoming = snippet.get('liveBroadcastContent') == 'upcoming'

    return {
        'title': snippet['title'],
        'channel': snippet['channelTitle'],
        'channel_id': snippet['channelId'],
        'published': format_youtube_timestamp(snippet['publishedAt']),
        'is_short': is_short,
        'short_reason': ', '.join(short_reasons),
        'is_live': is_live,
        'was_live': was_live,
        'is_upcoming': is_upcoming,
        'duration': duration_str,
        'duration_seconds': duration_seconds,
        'live_actual_start': format_youtube_timestamp(live_details.get('actualStartTime')) if live_details.get('actualStartTime') else '',
        'live_actual_end': format_youtube_timestamp(live_details.get('actualEndTime')) if live_details.get('actualEndTime') else '',
        'scheduled_start': live_details.get('scheduledStartTime', ''),
        'width': width,
        'height': height,
    }


def get_videos_info_from_api(video_ids):
    """Fetch at most 50 video records per quota unit where possible."""
    global youtube_api_calls, last_youtube_api_error
    last_youtube_api_error = None
    ids = list(dict.fromkeys(str(video_id).strip() for video_id in video_ids if str(video_id).strip()))
    results = {video_id: (None, None) for video_id in ids}
    if not ids:
        return results

    api_keys = config.YOUTUBE_API_KEYS or ([config.YOUTUBE_API_KEY] if config.YOUTUBE_API_KEY else [])
    if not api_keys:
        last_youtube_api_error = 'YouTube API keys are not configured'
        return {video_id: (None, last_youtube_api_error) for video_id in ids}

    for start in range(0, len(ids), 50):
        batch = ids[start:start + 50]
        errors = []
        success = False
        for api_key in api_keys:
            try:
                response = requests.get(
                    'https://www.googleapis.com/youtube/v3/videos',
                    params={
                        'part': 'snippet,contentDetails,liveStreamingDetails,player',
                        'maxHeight': 720,
                        'id': ','.join(batch),
                        'key': api_key,
                    },
                    timeout=10,
                )
                youtube_api_calls += 1
                if is_retryable_youtube_status(response.status_code):
                    error = extract_youtube_error(response)
                    errors.append(f'{mask_api_key(api_key)}: {error}')
                    print(f"  ⚠️  YouTube API key {mask_api_key(api_key)} failed: {error}")
                    continue
                if response.status_code != 200:
                    errors.append(f'{mask_api_key(api_key)}: {extract_youtube_error(response)}')
                    continue

                items = response.json().get('items', [])
                found = {str(item.get('id')): item for item in items if item.get('id')}
                if len(batch) == 1 and not found and len(items) == 1:
                    found[batch[0]] = items[0]
                for video_id in batch:
                    item = found.get(video_id)
                    results[video_id] = (_video_info_from_item(item, video_id), None) if item else (None, None)
                success = True
                break
            except Exception as error:
                errors.append(f'{mask_api_key(api_key)}: request failed: {error}')
                print(f"  ⚠️  YouTube API key {mask_api_key(api_key)} request failed: {error}")

        if not success and errors:
            batch_error = '; '.join(errors)
            last_youtube_api_error = batch_error
            for video_id in batch:
                results[video_id] = (None, batch_error)
            if any('quotaExceeded' in error for error in errors):
                for video_id in ids[start + len(batch):]:
                    results[video_id] = (None, batch_error)
                break

    return results


def get_video_info_from_api(video_id):
    """Получение информации о видео через YouTube Data API v3"""
    return get_videos_info_from_api([video_id]).get(str(video_id), (None, None))[0]


def get_youtube_api_calls():
    return youtube_api_calls


def get_last_youtube_api_error():
    return last_youtube_api_error
