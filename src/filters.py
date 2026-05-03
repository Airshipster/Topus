import config


def should_filter_video(video_info, project):
    """Проверка нужно ли фильтровать видео"""
    if not video_info:
        return False, ""
    
    if config.FILTER_SHORTS and video_info.get('is_short'):
        return True, f"Short video ({video_info.get('duration_seconds', 0)}s)"
    
    if config.FILTER_LIVE and video_info.get('is_live'):
        return True, "Live stream"
    
    if video_info.get('is_upcoming'):
        return True, "Upcoming/Premiere"
    
    if project.get('stop_words'):
        title_lower = video_info['title'].lower()
        for stop_word in project['stop_words']:
            if stop_word and stop_word in title_lower:
                return True, f"Stop word: {stop_word}"
    
    return False, ""
