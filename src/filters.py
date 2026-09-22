import re
import unicodedata

import config


def normalize_stop_text(value):
    return str(value or '').casefold().replace('ё', 'е')


def contains_cyrillic_letter(value):
    for character in str(value or ''):
        if unicodedata.category(character).startswith('L') and 'CYRILLIC' in unicodedata.name(character, ''):
            return True
    return False


def contains_latin_letter(value):
    for character in str(value or ''):
        if unicodedata.category(character).startswith('L') and 'LATIN' in unicodedata.name(character, ''):
            return True
    return False


def contains_whole_word(text, word):
    normalized_text = normalize_stop_text(text)
    normalized_word = normalize_stop_text(word).strip()
    if not normalized_word:
        return False
    if normalized_word.endswith('*'):
        prefix = normalized_word[:-1].strip()
        if not prefix:
            return False
        return re.search(rf'(?<!\w){re.escape(prefix)}\w*', normalized_text) is not None
    return re.search(rf'(?<!\w){re.escape(normalized_word)}(?!\w)', normalized_text) is not None


def should_filter_video(video_info, project, channel_info=None):
    """Проверка нужно ли фильтровать видео"""
    if not video_info:
        return False, ""
    
    # Shorts never enter the public feed or personal bot notifications.  This
    # is global policy, so a stale per-project sheet flag cannot opt a project
    # back in accidentally.
    if config.FILTER_SHORTS and video_info.get('is_short'):
        reason = video_info.get('short_reason') or f"{video_info.get('duration_seconds', 0)}s"
        return True, f"Short video ({reason})"
    
    if config.FILTER_LIVE and video_info.get('is_live') and not project.get('allow_streams'):
        return True, "Live stream"
    
    if video_info.get('is_upcoming') and not project.get('allow_premieres'):
        return True, "Upcoming/Premiere"

    title = video_info.get('title', '')
    latin_only = bool(project.get('only_latin'))
    cyrillic_only = bool(project.get('only_cyrillic'))
    if cyrillic_only and not latin_only and not contains_cyrillic_letter(title):
        return True, "Title has no Cyrillic letters"
    if latin_only and not cyrillic_only and contains_cyrillic_letter(title):
        return True, "Title contains Cyrillic letters"
    if str(project.get('name', '')).strip() == 'SciTopus' and not contains_cyrillic_letter(title):
        return True, "Title has no Cyrillic letters"
    
    if project.get('stop_words'):
        title_text = normalize_stop_text(video_info['title'])
        for stop_word in project['stop_words']:
            normalized_stop_word = normalize_stop_text(stop_word).strip()
            if normalized_stop_word and normalized_stop_word in title_text:
                return True, f"Stop word: {stop_word}"

    category = str((channel_info or {}).get('category') or '').strip()
    category_rules = project.get('category_stop_words') or {}
    category_words = category_rules.get(normalize_stop_text(category).strip(), [])
    for stop_word in category_words:
        if contains_whole_word(video_info.get('title', ''), stop_word):
            return True, f"Category stop word ({category}): {stop_word}"
    
    return False, ""
