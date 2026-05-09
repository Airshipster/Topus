import re

import requests

import config


def format_message(template, video, channel_info, project):
    """Форматирование сообщения"""
    if not template:
        template = config.DEFAULT_MESSAGE_TEMPLATE
    
    channel_name = video.get('channel', channel_info.get('name', 'Unknown'))
    video_title = video['title']
    video_url = video['url']
    
    message = template.replace('{channel_title}', channel_name)
    message = message.replace('{video_title}', video_title)
    message = message.replace('{video_url}', video_url)
    message = message.replace('{video_title_link}', f'<a href="{video_url}">{video_title}</a>')
    
    tg_channel_name = project.get('tg_channel', '')
    message = message.replace('{TG_channel}', tg_channel_name)
    
    tg_channel_link = channel_info.get('tg_channel', '').strip()
    
    if tg_channel_link and not tg_channel_link.startswith('-'):
        def replace_brackets(match):
            text = match.group(1)
            return f'<a href="{tg_channel_link}">{text}</a>'
        
        message = re.sub(r'\[([^\]]+)\]', replace_brackets, message)
    else:
        message = re.sub(r'\[([^\]]+)\]', r'\1', message)
    
    invisible_link = f'<a href="{video_url}">\u200b</a>'
    message = invisible_link + message
    
    return message

def send_to_telegram(bot_token, channel_id, message):
    """Отправка сообщения в Telegram"""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': channel_id,
        'text': message,
        'parse_mode': 'HTML',
        'disable_web_page_preview': False
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        result = response.json()
        return result.get('result', {}).get('message_id')
    except Exception as e:
        print(f"  ❌ Telegram error: {e}")
        return None


def delete_telegram_message(bot_token, channel_id, message_id):
    """Удаление сообщения из Telegram-канала."""
    url = f"https://api.telegram.org/bot{bot_token}/deleteMessage"
    payload = {
        'chat_id': channel_id,
        'message_id': message_id,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return bool(response.json().get('ok'))
    except Exception as e:
        print(f"  ❌ Telegram delete error: {e}")
        return False
