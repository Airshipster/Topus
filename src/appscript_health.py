"""Independent, bounded check of Apps Script execution and queue-read access."""
import json
import os
from pathlib import Path
import time
import urllib.request

PROBE_URL = 'https://script.google.com/macros/s/AKfycbwqySnqlEYAMTTQNkcUy9RU-B6UkikW9o-v5lzLxtthnpOE_52XRZThoe2b1xjIj1Zm/exec?health=queue-v1'


def valid_health(body, now):
    checked = body.get('checkedAt')
    return (body.get('kind') == 'topus-push-queue-read' and body.get('ok') is True
            and isinstance(checked, (int, float)) and -5 <= now - checked / 1000 <= 300)


def stale_component(beat, limit):
    age = beat.get('success_minutes')
    if age is None:
        age = beat.get('seen_minutes')
    return isinstance(age, (int, float)) and age >= limit


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    settings = dict(os.environ)
    if not settings.get('TOPUS_CONTROL_TOKEN'):
        settings.update(line.split('=', 1) for line in Path('/root/scitopus-secrets/topus-control.env').read_text().splitlines() if '=' in line)
    base = settings['TOPUS_CONTROL_URL'].rstrip('/')
    if base != 'https://topus-publication-control.scitopus.workers.dev':
        raise RuntimeError('CONTROL_URL_INVALID')
    opener = urllib.request.build_opener(NoRedirect)
    def control(path, body=None):
        req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
            headers={'Authorization':'Bearer '+settings['TOPUS_CONTROL_TOKEN'], 'Content-Type':'application/json', 'User-Agent':'Topus-Control/1.0'})
        with opener.open(req, timeout=25) as response:
            return json.loads(response.read(32768))
    snapshot = control('/status')
    beats = snapshot.get('beats', {})
    for name, limit in [('server-publisher',15),('github',45),('rss',45),('renewal',30)]:
        if name in beats:
            control('/incident', {'kind':name+'-stale', 'active':stale_component(beats[name],limit),
                'summary':f'{name}: нет успешного выполнения задачи более {limit} минут'})
    reachable = False
    try:
        with urllib.request.urlopen('https://scitopus.com/topus-watchdog/healthz',timeout=10) as response:
            reachable = response.status == 200
    except Exception:
        pass
    control('/beat', {'name':'server-guard','ok':reachable,'error':'' if reachable else 'HTTP_PROBE_FAILED'})
    control('/incident', {'kind':'server-http','active':not reachable and stale_component(beats.get('server-guard',beats.get('server-http',{})),5),
        'summary':'Сервер: нет подтверждённой доступности более 5 минут'})
    previous = beats.get('appscript', {})
    if previous.get('error') == '' and previous.get('seen_minutes', 999) < 4:
        print('Apps Script queue check already fresh')
        return
    ok = False
    try:
        with urllib.request.urlopen(PROBE_URL, timeout=30) as response:
            ok = response.status == 200 and valid_health(json.loads(response.read(16384)), time.time())
    except Exception:
        pass
    control('/beat', {'name':'appscript', 'ok':ok, 'error':'' if ok else 'QUEUE_READ_CHECK_FAILED'})
    stale = not ok and previous.get('success_minutes', 999) >= 15
    control('/incident', {'kind':'appscript-queue-health', 'active':stale,
        'summary':'Apps Script: не подтверждено выполнение обработчика и чтение очереди. Код AS_QUEUE_READ.'})
    print('Apps Script queue read: ' + ('OK' if ok else 'FAILED'))
    if not ok:
        raise SystemExit(1)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print('AppScript monitor error: ' + type(error).__name__)
        raise SystemExit(1)
