import time
from datetime import datetime, timedelta

import requests

import config
from sheets import load_youtube_channels


def check_rss_feed(channel_id):
    """Проверка RSS фида канала"""
    try:
        time.sleep(0.2)
        
        url = f"{config.CLOUDFLARE_WORKER_URL}/?channel={channel_id}"
        response = requests.get(url, timeout=15)
        
        if response.status_code != 200:
            return []
        
        if len(response.content) == 0:
            return []
        
        from xml.etree import ElementTree as ET
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError:
            return []
        
        ns = {
            'atom': 'http://www.w3.org/2005/Atom',
            'yt': 'http://www.youtube.com/xml/schemas/2015'
        }
        
        entries = root.findall('atom:entry', ns)
        
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
                if published_str.endswith('Z'):
                    published = datetime.fromisoformat(published_str.replace('Z', '+00:00')).replace(tzinfo=None)
                else:
                    published = datetime.fromisoformat(published_str).replace(tzinfo=None)
            except:
                continue
            
            if published > cutoff_time:
                videos.append({
                    'video_id': video_id,
                    'title': title,
                    'url': f"https://www.youtube.com/watch?v={video_id}",
                    'channel': channel_name,
                    'channel_id': channel_id,
                    'published': published.isoformat()
                })
        
        return videos
    except:
        return []

def rss_fallback_check(client, project, published_videos):
    """RSS fallback для конкретного проекта"""
    print(f"\n  📡 RSS fallback for {project['name']}...")
    
    project_channels = load_youtube_channels(client, project)
    
    print(f"    Checking {len(project_channels)} channels")
    print(f"    Time window: {config.RSS_FALLBACK_AGE_HOURS}h")
    
    new_videos = []
    videos_found_count = 0
    
    for i, (channel_id, channel_info) in enumerate(project_channels.items()):
        videos = check_rss_feed(channel_id)
        videos_found_count += len(videos)
        
        if i > 0 and i % 10 == 0:
            print(f"    Progress: {i}/{len(project_channels)} channels (Found: {videos_found_count} videos, New: {len(new_videos)})")
        
        for video in videos:
            if (video['video_id'], project['name']) not in published_videos:
                video['project'] = project
                video['channel_info'] = channel_info
                new_videos.append(video)
    
    print(f"    ✅ RSS scan complete: {videos_found_count} videos total, {len(new_videos)} new")
    
    return new_videos
