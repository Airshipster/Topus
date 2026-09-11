"""Bounded authenticated access to shared ownership and delivery receipts."""
import os
from urllib.parse import urlsplit

import requests


class ControlUnavailable(RuntimeError):
    pass


def configured():
    return bool(os.environ.get('TOPUS_CONTROL_URL') or os.environ.get('TOPUS_CONTROL_REQUIRED') == 'true')


class ControlClient:
    def __init__(self, url=None, token=None, session=None):
        self.url = (url if url is not None else os.environ.get('TOPUS_CONTROL_URL', '')).rstrip('/')
        self.token = token if token is not None else os.environ.get('TOPUS_CONTROL_TOKEN', '')
        parsed = urlsplit(self.url)
        if parsed.scheme != 'https' or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment or not self.token:
            raise ControlUnavailable('CONTROL_CONFIG_INVALID')
        self.session = session or requests.Session()

    def request(self, path, body=None):
        try:
            response = self.session.request('GET' if body is None else 'POST', self.url + path,
                headers={'Authorization': 'Bearer ' + self.token, 'User-Agent': 'Topus-Control/1.0'}, json=body,
                timeout=(5, 20), allow_redirects=False)
            if response.status_code != 200:
                raise ControlUnavailable('CONTROL_HTTP_' + str(response.status_code))
            data = response.json()
            if not isinstance(data, dict):
                raise ControlUnavailable('CONTROL_INVALID_RESPONSE')
            return data
        except ControlUnavailable:
            raise
        except Exception:
            # requests errors may embed URLs; never expose auth/configuration.
            raise ControlUnavailable('CONTROL_UNAVAILABLE') from None

    def heartbeat(self, name, ok, error=''):
        return self.request('/beat', {'name': name, 'ok': ok, 'error': error[:100]})

    def send(self, project, video_id, destination, text, published_at=None):
        owner = os.environ.get('TOPUS_PUBLISHER_OWNER', '')
        lease = os.environ.get('TOPUS_PUBLISHER_LEASE', '')
        if owner not in ('server', 'github') or not lease:
            raise ControlUnavailable('CONTROL_LEASE_MISSING')
        result = self.request('/send', {'owner': owner, 'lease': lease, 'project': project,
            'video_id': video_id, 'destination': str(destination), 'text': text,
            'published_at': published_at})
        return result.get('message_id') if result.get('state') == 'sent' else None

    def register_projects(self, projects):
        settings = {'project:' + p['name']: {'token': p['bot_token'], 'destination': str(p['channel_id'])}
                    for p in projects if p.get('bot_token') and p.get('channel_id')}
        items = list(settings.items())
        for start in range(0, len(items), 25):
            self.request('/configure', dict(items[start:start + 25]))
