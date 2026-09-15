"""One persistent publisher, independent timers, signed durable WebSub ingress."""
import fcntl
import hmac
import json
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).parent / 'src'))
from push_store import database, accept_xml, confirm, health
from delivery_journal import summary
from control_client import ControlClient, configured

ACTIVE = os.environ.get('TOPUS_WATCHDOG_ACTIVE', 'false').lower() == 'true'
TOKEN = os.environ.get('TOPUS_WATCHDOG_TOKEN', '')
wake = threading.Event()
requested_rss = threading.Event()
running = {'publisher': None, 'renewal': False, 'tick': time.time()}
lock = threading.Lock()


def run_job(name, mode=None):
    with database() as db:
        db.execute("INSERT INTO jobs(name,started,error) VALUES (?,?,'running') ON CONFLICT(name) "
                   "DO UPDATE SET started=excluded.started,error='running'", (name, time.time()))
    env = os.environ.copy()
    env.update({'GOOGLE_SERVICE_ACCOUNT_JSON': Path(env['GOOGLE_SERVICE_ACCOUNT_JSON_FILE']).read_text(),
                'TOPUS_PUSH_ONLY': 'true' if mode == 'push' else 'false',
                'TOPUS_SYNC_ONLY': 'false', 'TOPUS_RUN_SOURCE': 'server-controller',
                'TOPUS_PUBLISHER_OWNER': 'server', 'PYTHONUNBUFFERED': '1'})
    # Preserve configured age filters: no silent 72-hour publication override.
    env.pop('TOPUS_MAX_PUBLISH_AGE_HOURS_OVERRIDE', None)
    env.pop('TOPUS_RSS_FALLBACK_AGE_HOURS_OVERRIDE', None)
    script = {'renewal': 'renew_direct.py', 'notifications': 'worker_notifications.py'}.get(name, 'main.py')
    if script == 'main.py' and env.get('TOPUS_CONTROL_REQUIRED') == 'true':
        script = 'coordinated_run.py'
    error = ''
    code = None
    try:
        child = subprocess.Popen([sys.executable, '/app/src/' + script], env=env, start_new_session=True)
        try:
            code = child.wait(timeout=240 if name == 'renewal' else 1200)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
            code = 124
        if code:
            error = f'exit {code}'
    except Exception as exc:
        error = type(exc).__name__
    if name in ('renewal', 'notifications') and configured():
        try:
            ControlClient().heartbeat('renewal' if name == 'renewal' else 'personal', not error, error)
        except Exception as exc:
            error = error or type(exc).__name__
    with database() as db:
        if name == 'rss' and code == 75:
            # Lease contention did not scan anything; retry in one minute.
            db.execute('UPDATE jobs SET started=? WHERE name=?', (time.time() - 1740, name))
        db.execute('UPDATE jobs SET completed=?,success=CASE WHEN ?=\'\' THEN ? ELSE success END,error=? WHERE name=?',
                   (time.time(), error, time.time(), error, name))


def publisher_loop():
    # OS lock survives HTTP signals and serializes all publisher modes.
    with open('/data/publisher.lock', 'a') as guard:
        fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            running['tick'] = time.time()
            if not ACTIVE:
                wake.wait(10)
                wake.clear()
                continue
            with database() as db:
                jobs = {r['name']: dict(r) for r in db.execute('SELECT * FROM jobs')}
            now = time.time()
            rss = jobs.get('rss', {})
            push = jobs.get('push', {})
            # Cadence is measured from start, never postponed by an unrelated success.
            if requested_rss.is_set() or now - (rss.get('started') or 0) >= 1800:
                requested_rss.clear()
                mode = 'rss'
            elif now - (push.get('started') or 0) >= 120 or wake.is_set():
                mode = 'push'
            else:
                wake.wait(5)
                continue
            wake.clear()
            running['publisher'] = mode
            try:
                run_job(mode, mode)
            except Exception as exc:
                print('Publisher scheduler error: ' + type(exc).__name__, flush=True)
                time.sleep(10)
            running['publisher'] = None


def renewal_loop():
    while True:
        if ACTIVE:
            running['renewal'] = True
            try:
                run_job('renewal')
            except Exception as exc:
                print('Renewal scheduler error: ' + type(exc).__name__, flush=True)
            running['renewal'] = False
        time.sleep(120)


def notifications_loop():
    while True:
        if ACTIVE:
            try:
                run_job('notifications')
            except Exception as exc:
                print('Notification scheduler error: ' + type(exc).__name__, flush=True)
        time.sleep(120)


def restart_guard(threads):
    while True:
        time.sleep(60)
        if any(not thread.is_alive() for thread in threads) or time.time() - running['tick'] > 1250:
            print('Scheduler stalled; exiting for Docker restart policy', flush=True)
            os._exit(1)


def status():
    data = health()
    data.update({'active': ACTIVE, 'owner': 'server', 'running': dict(running), 'deliveries': summary()})
    now = time.time()
    issues = []
    for name, limit in [('rss', 2700), ('push', 900), ('renewal', 900), ('notifications', 900)]:
        job = data['jobs'].get(name, {})
        if not job.get('success') or now - job['success'] > limit:
            issues.append(name + ': no recent successful pass')
        if job.get('error') not in ('', 'running', None):
            issues.append(name + ': ' + job['error'])
    if data['subscriptions'].get('expired'):
        issues.append('expired/unverified subscriptions')
    if data['deliveries'].get('uncertain') or data['deliveries'].get('sending'):
        issues.append('delivery outcomes require reconciliation')
    data['issues'] = issues
    data['ok'] = ACTIVE and not issues
    return data


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, payload, text=False):
        body = str(payload).encode() if text else json.dumps(payload).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'text/plain; charset=utf-8' if text else 'application/json')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlsplit(self.path)
        if url.path == '/websub':
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            topic = urlsplit(q.get('hub.topic', ''))
            channel = parse_qs(topic.query).get('channel_id', [''])[0]
            try:
                valid = (topic.scheme == 'https' and topic.netloc == 'www.youtube.com' and
                         topic.path == '/xml/feeds/videos.xml' and q.get('hub.mode') == 'subscribe' and
                         0 < int(q.get('hub.lease_seconds', '0')) and
                         confirm(channel, q.get('verify', ''), int(q['hub.lease_seconds'])))
            except (ValueError, KeyError):
                valid = False
            self.reply(200 if valid else 403, q.get('hub.challenge', '') if valid else 'rejected', text=True)
        elif url.path == '/healthz':
            # Liveness differs from end-to-end business readiness.
            alive = time.time() - running['tick'] < 1250
            self.reply(200 if alive else 503, {'alive': alive})
        elif url.path in ('/', '/status'):
            data = status()
            self.reply(200 if data['ok'] else 503, data)
        else:
            self.reply(404, {'error': 'not_found'})

    def do_POST(self):
        route = urlsplit(self.path).path
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 <= length <= 1024 * 1024:
                self.reply(413, {'error': 'too_large'})
                return
            if route == '/websub':
                count = accept_xml(self.rfile.read(length), self.headers.get('X-Hub-Signature', ''))
                if count:
                    wake.set()
                self.reply(204, '', text=True)
                return
            if route not in ('/run', '/heartbeat') or not TOKEN or not hmac.compare_digest(
                    self.headers.get('Authorization', ''), 'Bearer ' + TOKEN):
                self.reply(401, {'error': 'unauthorized'})
                return
            raw = self.rfile.read(length)
            if route == '/run':
                options = json.loads(raw) if raw else {}
                if not isinstance(options, dict) or options.get('mode', 'push') not in ('push', 'rss'):
                    raise ValueError('invalid mode')
                if options.get('mode') == 'rss':
                    requested_rss.set()
            wake.set()
            self.reply(202, {'accepted': True, 'owner': 'server'})
        except PermissionError:
            self.reply(403, {'error': 'bad_signature'})
        except (ValueError, KeyError):
            self.reply(400, {'error': 'invalid_request'})
        except Exception:
            self.reply(503, {'error': 'temporary_failure'})

    def log_message(self, *args):
        pass  # Callback query parameters and authorization never enter logs.


if __name__ == '__main__':
    with database():
        pass
    threads = [threading.Thread(target=target, daemon=True) for target in
               (publisher_loop, renewal_loop, notifications_loop)]
    for thread in threads:
        thread.start()
    threading.Thread(target=restart_guard, args=(threads,), daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0', int(os.environ.get('PORT', '8090'))), Handler).serve_forever()
