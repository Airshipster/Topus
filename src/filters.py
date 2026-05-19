import config


def normalize_stop_text(value):
    return str(value or '').casefold().replace('ё', 'е')


def should_filter_video(video_info, project):
    """Проверка нужно ли фильтровать видео"""
    if not video_info:
        return False, ""
    
    if config.FILTER_SHORTS and not project.get('allow_shorts') and video_info.get('is_short'):
        reason = video_info.get('short_reason') or f"{video_info.get('duration_seconds', 0)}s"
        return True, f"Short video ({reason})"
    
    if config.FILTER_LIVE and video_info.get('is_live') and not project.get('allow_streams'):
        return True, "Live stream"
    
    if video_info.get('is_upcoming') and not project.get('allow_premieres'):
        return True, "Upcoming/Premiere"
    
    if project.get('stop_words'):
        title_text = normalize_stop_text(video_info['title'])
        for stop_word in project['stop_words']:
            normalized_stop_word = normalize_stop_text(stop_word).strip()
            if normalized_stop_word and normalized_stop_word in title_text:
                return True, f"Stop word: {stop_word}"
    
    return False, ""
