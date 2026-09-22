"""Replay accepted work independently of the YouTube discovery age window."""
from sheets import (get_values_with_quota_retry, row_as_dict, status_name_from_text,
                    first_value, video_id_from_url, project_name_from_cell, channel_id_from_link)
import config


def pending_events(sheet):
    values = get_values_with_quota_retry(sheet.worksheet(config.SHEET_NAME_VIDEOS))
    if not values:
        return []
    events = []
    blocked = set()
    for row in values[1:]:
        item = row_as_dict(values[0], row)
        key = (video_id_from_url(first_value(item, ['Ссылка на видео', 'Video ID'])),
               project_name_from_cell(first_value(item, ['Проект'])))
        if status_name_from_text(first_value(item, ['Системный статус'])) == 'published' or first_value(item, ['TG message_id']):
            blocked.add(key)
    for i, row in enumerate(values[1:], 2):
        item = row_as_dict(values[0], row)
        status = status_name_from_text(first_value(item, ['Системный статус']))
        video_id = video_id_from_url(first_value(item, ['Ссылка на видео', 'Video ID']))
        project = project_name_from_cell(first_value(item, ['Проект']))
        channel = channel_id_from_link(first_value(item, ['Ссылка на канал']))
        manual_replay = 'Manual language recovery' in str(first_value(item, ['Системный статус']))
        if status in ('pending', 'failed', 'retry') and video_id and channel and (video_id, project) not in blocked:
            events.append({'row_index': -i, 'video_id': video_id, 'channel_id': channel,
                           'projects': '', 'retry_project': project, 'retry_accepted': True,
                           'manual_replay': manual_replay})
    return events
