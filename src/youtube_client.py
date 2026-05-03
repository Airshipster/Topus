import re

import requests

import config

youtube_api_calls = 0


def get_video_info_from_api(video_id):
    """Получение информации о видео через YouTube Data API v3"""
    global youtube_api_calls
    
    if not config.YOUTUBE_API_KEY:
        return None
    
    try:
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            'part': 'snippet,contentDetails,liveStreamingDetails',
            'id': video_id,
            'key': config.YOUTUBE_API_KEY
        }
        
        response = requests.get(url, params=params, timeout=10)
        youtube_api_calls += 1
        
        if response.status_code != 200:
            return None
        
        data = response.json()
        if not data.get('items'):
            return None
        
        item = data['items'][0]
        snippet = item['snippet']
        content_details = item.get('contentDetails', {})
        live_details = item.get('liveStreamingDetails', {})
        
        is_short = False
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
        
        is_live = live_details.get('actualStartTime') is not None
        is_upcoming = snippet.get('liveBroadcastContent') == 'upcoming'
        
        return {
            'title': snippet['title'],
            'channel': snippet['channelTitle'],
            'channel_id': snippet['channelId'],
            'published': snippet['publishedAt'],
            'is_short': is_short,
            'is_live': is_live,
            'is_upcoming': is_upcoming,
            'duration': duration_str,
            'duration_seconds': duration_seconds
        }
    except Exception as e:
        return None


def get_youtube_api_calls():
    return youtube_api_calls
