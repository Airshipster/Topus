import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import requests

import config
from sheets import format_timestamp, load_youtube_channels

failed_channels = set()
failure_reasons = {}


def confirmed_unavailable_channels(channel_ids):
    """Distinguish an unavailable source from a failed scan, without deleting it."""
    import youtube_client
    unavailable = set()
    ids = sorted(channel_ids)
    keys = config.YOUTUBE_API_KEYS or ([config.YOUTUBE_API_KEY] if config.YOUTUBE_API_KEY else [])
    for start in range(0, len(ids), 50):
        batch = ids[start:start + 50]
        for key in keys[:3]:
            try:
                youtube_client.youtube_api_calls += 1
                response = requests.get('https://www.googleapis.com/youtube/v3/channels',
                    params={'part':'snippet', 'id':','.join(batch), 'key':key}, timeout=(5, 15))
                if response.status_code != 200:
                    continue
                payload = response.json()
                if payload.get('kind') != 'youtube#channelListResponse':
                    continue
                items = payload.get('items')
                if items is None and payload.get('pageInfo', {}).get('totalResults') == 0:
                    items = []
                if not isinstance(items, list):
                    continue
                present = {item['id'] for item in items if isinstance(item, dict) and item.get('id')}
                unavailable.update(set(batch) - present)
                break
            except (requests.RequestException, ValueError, TypeError):
                continue
    return unavailable


def _check_rss_feed_once(channel_id, direct=False):
    """Проверка RSS фида канала"""
    try:
        time.sleep(0.05)
        
        url = (f'https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}' if direct
               else f"{config.CLOUDFLARE_WORKER_URL}/?channel={channel_id}")
        response = requests.get(url, timeout=(5, 15))
        
        if response.status_code != 200:
            failure_reasons[channel_id] = 'HTTP_' + str(response.status_code)
            return None
        
        if len(response.content) == 0:
            failure_reasons[channel_id] = 'EMPTY_BODY'
            return None
        
        from xml.etree import ElementTree as ET
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError:
            failure_reasons[channel_id] = 'INVALID_XML'
            return None
        if root.tag != '{http://www.w3.org/2005/Atom}feed':
            failure_reasons[channel_id] = 'NOT_ATOM_FEED'
            return None
        
        ns = {
            'atom': 'http://www.w3.org/2005/Atom',
            'yt': 'http://www.youtube.com/xml/schemas/2015'
        }
        
        entries = root.findall('atom:entry', ns)
        if not entries and root.findtext('atom:title', '', ns) == 'YouTube video feed':
            failure_reasons[channel_id] = 'STATIC_DISCOVERY_DOCUMENT'
            return None
        
        videos = []
        cutoff_time = datetime.utcnow() - timedelta(hours=config.RSS_FALLBACK_AGE_HOURS)
        
        for entry in entries:
            video_id_elem = entry.find('yt:videoId', ns)
            title_elem = entry.find('atom:title', ns)
            published_elem = entry.find('atom:published', ns)
            author_elem = entry.find('atom:author/atom:name', ns)
            
            if video_id_elem is None or title_elem is None or published_elem is None:
                continue
            
            if not video_id_elem.text or not title_elem.text or not published_elem.text:
                continue
            
            video_id = video_id_elem.text
            title = title_elem.text
            published_str = published_elem.text
            channel_name = author_elem.text if author_elem is not None and author_elem.text else 'Unknown'
            
            try:
                published_display = None
                if published_str.endswith('Z'):
                    published_display = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
                    published = published_display.replace(tzinfo=None)
                else:
                    published = datetime.fromisoformat(published_str).replace(tzinfo=None)
                    published_display = published
            except:
                continue
            
            if published > cutoff_time:
                videos.append({
                    'video_id': video_id,
                    'title': title,
                    'url': f"https://www.youtube.com/watch?v={video_id}",
                    'channel': channel_name,
                    'channel_id': channel_id,
                    'published': format_timestamp(published_display)
                })
        
        return videos
    except Exception as error:
        failure_reasons[channel_id] = type(error).__name__
        return None


def check_rss_feed(channel_id):
    """Recover transient/proxy failures without dropping a failed source as empty."""
    for attempt, direct in enumerate((True, False, True)):
        if attempt:
            time.sleep(0.5 * attempt)
        videos = _check_rss_feed_once(channel_id, direct=direct)
        if failure_reasons.get(channel_id) in ('HTTP_401', 'HTTP_403'):
            break
        if videos is not None:
            failure_reasons.pop(channel_id, None)
            failed_channels.discard(channel_id)
            return videos
    print(f'RSS_SOURCE_FAILED channel={channel_id} reason={failure_reasons.get(channel_id, "UNKNOWN")} attempts=3', flush=True)
    return None

def rss_fallback_check(client, project, published_videos, project_channels=None, return_seen=False, rss_cache=None):
    """RSS fallback для конкретного проекта"""
    print(f"\n  📡 RSS fallback for {project['name']}...")
    
    if project_channels is None:
        project_channels = load_youtube_channels(client, project)
    
    print(f"    Checking {len(project_channels)} channels")
    print(f"    Time window: {config.RSS_FALLBACK_AGE_HOURS}h")
    
    new_videos = []
    videos_found_count = 0
    seen_by_channel = {}
    rss_cache = rss_cache if rss_cache is not None else {}
    
    def load_channel(channel_id):
        if channel_id not in rss_cache:
            rss_cache[channel_id] = check_rss_feed(channel_id)
        return channel_id, rss_cache[channel_id]

    channel_items = list(project_channels.items())
    with ThreadPoolExecutor(max_workers=max(1, int(config.RSS_WORKERS))) as executor:
        futures = {
            executor.submit(load_channel, channel_id): (i, channel_id, channel_info)
            for i, (channel_id, channel_info) in enumerate(channel_items)
        }

        for future in as_completed(futures):
            i, channel_id, channel_info = futures[future]
            try:
                _, videos = future.result()
            except Exception:
                videos = None

            if videos is None:
                failed_channels.add(channel_id)
                continue

            videos_found_count += len(videos)
            seen_by_channel[channel_id] = {video['video_id'] for video in videos}

            if i > 0 and i % 10 == 0:
                print(f"    Progress: {i}/{len(project_channels)} channels (Found: {videos_found_count} videos, New: {len(new_videos)})")

            for video in videos:
                if (video['video_id'], project['name']) not in published_videos:
                    video['project'] = project
                    video['channel_info'] = channel_info
                    new_videos.append(video)
    
    print(f"    ✅ RSS scan complete: {videos_found_count} videos total, {len(new_videos)} new")
    
    if return_seen:
        return new_videos, seen_by_channel

    return new_videos
