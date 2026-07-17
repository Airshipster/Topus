import os

import requests


def worker_message_id(result):
    deliveries = result.get('deliveries') if isinstance(result, dict) else None
    if not deliveries:
        queued = result.get('queued', 0) if isinstance(result, dict) else 0
        return f'bot:{queued}'

    ids = [
        str(delivery.get('messageId'))
        for delivery in deliveries
        if delivery.get('messageId') is not None
    ]
    if not ids:
        return 'bot:0'
    if len(ids) == 1:
        return ids[0]
    return ','.join(ids)


def notify_worker_subscribers(project, video, message):
    worker_url = os.environ.get('TOPUS_WORKER_URL', '').strip()
    admin_secret = os.environ.get('TOPUS_WORKER_ADMIN_SECRET', '').strip()
    project_code = str(project.get('code') or '').strip()
    channel_id = str(video.get('channel_id') or '').strip()

    if not worker_url or not admin_secret or not project_code or not channel_id:
        return None

    try:
        response = requests.post(
            worker_url.rstrip('/') + '/admin/notify',
            json={
                'projectCode': project_code,
                'channelId': channel_id,
                'videoId': str(video.get('video_id') or video.get('videoId') or '').strip(),
                'text': message,
                'parseMode': 'HTML',
            },
            headers={'x-admin-secret': admin_secret},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"    ⚠️  Worker subscriber notification skipped: {e}")
        return None
