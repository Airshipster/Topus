"""Run one fenced publisher; terminate it if shared ownership cannot renew."""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from control_client import ControlClient, ControlUnavailable


def stop_child(child):
    if child.poll() is not None:
        return
    if os.name == 'posix':
        os.killpg(child.pid, signal.SIGTERM)
    else:
        child.terminate()
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if os.name == 'posix':
            os.killpg(child.pid, signal.SIGKILL)
        else:
            child.kill()
        child.wait(timeout=5)


def run(client=None, spawn=subprocess.Popen, clock=time.monotonic):
    client = client or ControlClient()
    owner = os.environ.get('TOPUS_PUBLISHER_OWNER', '')
    if owner not in ('server', 'github'):
        raise ControlUnavailable('CONTROL_OWNER_INVALID')
    name = 'server-publisher' if owner == 'server' else 'github'
    if owner == 'github':
        # This inbox is external, so runner teardown cannot discard pending work.
        from worker_notifications import retry_outbox
        if os.environ.get('TOPUS_WORKER_URL'):
            try:
                retry_outbox()
            except Exception:
                client.heartbeat('personal', False, 'PERSONAL_RETRY_PENDING')
    client.heartbeat(name, False, 'running')
    token = client.request('/lease/acquire', {'owner': owner}).get('token')
    if not token:
        if owner == 'github':
            client.heartbeat('github', True)
        print('CONTROL_BUSY: no publication performed', flush=True)
        return 75
    child = None
    payload = {'owner': owner, 'lease': token}
    def interrupted(_signum, _frame):
        raise ControlUnavailable('PUBLISHER_STOPPED')
    previous_handler = signal.signal(signal.SIGTERM, interrupted)
    try:
        env = os.environ.copy()
        env.update(TOPUS_PUBLISHER_LEASE=token, TOPUS_CONTROL_REQUIRED='true')
        maintenance = any(env.get(key, '').lower() == 'true' for key in
            ('TOPUS_MAINTENANCE_ONLY', 'TOPUS_UNLOCK_ONLY', 'TOPUS_SYNC_ONLY', 'TOPUS_REPAIR_PENDING_ONLY', 'TOPUS_FORCE_SUBSCRIPTION_SYNC'))
        if maintenance:
            env['TOPUS_PUSH_ONLY'] = 'false'
        elif env.get('TOPUS_PUSH_ONLY', '') == 'auto':
            rss = client.request('/status').get('beats', {}).get('rss', {})
            env['TOPUS_PUSH_ONLY'] = 'true' if (rss.get('success_minutes') is not None and rss['success_minutes'] < 30) else 'false'
        child = spawn([sys.executable, str(Path(__file__).with_name('main.py'))],
                      env=env, start_new_session=True)
        deadline = clock() + 1200
        while True:
            try:
                code = child.wait(timeout=20)
                break
            except subprocess.TimeoutExpired:
                if clock() >= deadline:
                    raise ControlUnavailable('PUBLISHER_TIMEOUT')
                if not client.request('/lease/renew', payload).get('ok'):
                    raise ControlUnavailable('CONTROL_LEASE_LOST')
        client.heartbeat(name, code == 0, '' if code == 0 else 'PUBLISHER_FAILED')
        if code == 0 and not maintenance and env.get('TOPUS_PUSH_ONLY') != 'true':
            client.heartbeat('rss', True)
        return code
    except Exception:
        if child is not None:
            stop_child(child)
        try:
            client.heartbeat(name, False, 'PUBLISHER_INTERRUPTED')
        except ControlUnavailable:
            pass
        raise
    finally:
        signal.signal(signal.SIGTERM, previous_handler)
        try:
            client.request('/lease/release', payload)
        except ControlUnavailable:
            pass  # The remote lease expires; do not fall back to a local lock.


if __name__ == '__main__':
    try:
        result = run()
        # A healthy primary owning work is a normal GitHub standby outcome.
        raise SystemExit(0 if result == 75 and os.environ.get('TOPUS_PUBLISHER_OWNER') == 'github' else result)
    except ControlUnavailable as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
