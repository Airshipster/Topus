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
from push_store import database, accept_xml, confirm, health, record_callback
from rss_discovery import push_gap_health
from delivery_journal import summary
from control_client import ControlClient, configured

ACTIVE = os.environ.get('TOPUS_WATCHDOG_ACTIVE', 'false').lower() == 'true'
TOKEN = os.environ.get('TOPUS_WATCHDOG_TOKEN', '')
wake = threading.Event()
requested_rss = threading.Event()
requested_hot_rss = threading.Event()
running = {'publisher': None, 'renewal': False, 'tick': time.time()}
lock = threading.Lock()


def schedule_hot_retry(result, timer_factory=threading.Timer):
    if result != 75:
        return False
    timer = timer_factory(45, requested_hot_rss.set)
    timer.daemon = True
    timer.start()
    return True


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
    script = {'renewal': 'renew_direct.py', 'notifications': 'worker_notifications.py',
              'rss-discovery': 'rss_discovery.py',
              'rss-hot-discovery': 'rss_discovery.py'}.get(name, 'main.py')
    env['TOPUS_RSS_DISCOVERY_MODE'] = 'hot' if name == 'rss-hot-discovery' else 'full'
    env['TOPUS_RSS_HOT_ONLY'] = 'true' if mode == 'rss-hot' else 'false'
    env['TOPUS_RSS_CACHE_ONLY'] = 'true' if script == 'main.py' else 'false'
    if script == 'main.py' and env.get('TOPUS_CONTROL_REQUIRED') == 'true':
        script = 'coordinated_run.py'
    error = ''
    code = None
    preempted = False
    try:
        child = subprocess.Popen([sys.executable, '/app/src/' + script], env=env, start_new_session=True)
        timeout = 240 if name == 'renewal' else 1200
        deadline = time.monotonic() + timeout
        while child.poll() is None:
            try:
                child.wait(timeout=min(2, max(0.1, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                if name in ('rss', 'rss-hot') and wake.is_set():
                    # A callback is time-sensitive. coordinated_run.py releases
                    # the shared lease before exiting, so a Push pass can follow.
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait()
                    preempted = True
                    error = 'preempted by push'
                    break
                if time.monotonic() >= deadline:
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait()
                    code = 124
                    break
        if not preempted and code is None:
            code = child.returncode
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
        if name in ('rss', 'rss-hot') and (code == 75 or preempted):
            # Lease contention did not scan anything; retry in one minute.
            retry_age = 1740 if name == 'rss' else 240
            db.execute('UPDATE jobs SET started=? WHERE name=?', (time.time() - retry_age, name))
        db.execute('UPDATE jobs SET completed=?,success=CASE WHEN ?=\'\' THEN ? ELSE success END,error=? WHERE name=?',
                   (time.time(), error, time.time(), error, name))
    return code


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
                requested_hot_rss.clear()
                mode = 'rss'
            elif now - (push.get('started') or 0) >= 120 or wake.is_set():
                mode = 'push'
            elif requested_hot_rss.is_set():
                requested_hot_rss.clear()
                mode = 'rss-hot'
            else:
                wake.wait(5)
                continue
            wake.clear()
            running['publisher'] = mode
            try:
                result = run_job(mode, mode)
                if mode == 'rss-hot':
                    schedule_hot_retry(result)
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


def discovery_loop():
    while True:
        with database() as db:
            full = db.execute("SELECT started FROM jobs WHERE name='rss-discovery'").fetchone()
            hot = db.execute("SELECT started FROM jobs WHERE name='rss-hot-discovery'").fetchone()
        if ACTIVE:
            now = time.time()
            if not full or now - (full['started'] or 0) >= 1800:
                try:
                    run_job('rss-discovery')
                except Exception as exc:
                    print('RSS discovery scheduler error: ' + type(exc).__name__, flush=True)
                finally:
                    # Publish completed source results even if other sources failed.
                    requested_rss.set()
                    wake.set()
            elif not hot or now - (hot['started'] or 0) >= 300:
                try:
                    run_job('rss-hot-discovery')
                except Exception as exc:
                    print('Hot RSS discovery scheduler error: ' + type(exc).__name__, flush=True)
                finally:
                    requested_hot_rss.set()
        time.sleep(5)


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
    data.update({'active': ACTIVE, 'owner': 'server', 'running': dict(running),
                 'deliveries': summary(), 'push_delivery': push_gap_health()})
    now = time.time()
    issues = []
    for name, limit in [('rss', 2700), ('push', 900), ('renewal', 900), ('notifications', 900)]:
        job = data['jobs'].get(name, {})
        if not job.get('success') or now - job['success'] > limit:
            issues.append(name + ': no recent successful pass')
        if job.get('error') not in ('', 'running', None, 'preempted by push'):
            issues.append(name + ': ' + job['error'])
    if data['subscriptions'].get('expired'):
        issues.append('expired/unverified subscriptions')
    if data['push_delivery'].get('open'):
        issues.append('push delivery gaps: ' + str(data['push_delivery']['open']))
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
                         topic.path in ('/feeds/videos.xml', '/xml/feeds/videos.xml') and q.get('hub.mode') == 'subscribe' and
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
                try:
                    counts = accept_xml(self.rfile.read(length), self.headers.get('X-Hub-Signature', ''))
                except PermissionError:
                    record_callback('rejected', rejection_code='bad_signature')
                    raise
                except (ValueError, KeyError):
                    record_callback('rejected', rejection_code='invalid_payload')
                    raise
                except Exception:
                    record_callback('rejected', rejection_code='internal_error')
                    raise
                record_callback('accepted', counts=counts)
                if counts['new_events']:
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
               (publisher_loop, renewal_loop, notifications_loop, discovery_loop)]
    for thread in threads:
        thread.start()
    threading.Thread(target=restart_guard, args=(threads,), daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0', int(os.environ.get('PORT', '8090'))), Handler).serve_forever()
