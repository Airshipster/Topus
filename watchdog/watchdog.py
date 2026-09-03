import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

GITHUB_API = os.getenv(
    "TOPUS_GITHUB_RUNS_URL",
    "https://api.github.com/repos/Airshipster/Topus/actions/workflows/check_videos.yml/runs",
)
CHECK_SECONDS = max(30, int(os.getenv("TOPUS_WATCHDOG_CHECK_SECONDS", "60")))
STALE_SECONDS = max(600, int(os.getenv("TOPUS_WATCHDOG_STALE_SECONDS", "1200")))
FALLBACK_COOLDOWN_SECONDS = max(900, int(os.getenv("TOPUS_WATCHDOG_FALLBACK_COOLDOWN_SECONDS", "1800")))
RUN_TIMEOUT_SECONDS = max(300, int(os.getenv("TOPUS_WATCHDOG_RUN_TIMEOUT_SECONDS", "1500")))
FAILURES_BEFORE_FALLBACK = max(2, int(os.getenv("TOPUS_WATCHDOG_FAILURES_BEFORE_FALLBACK", "3")))
RSS_RECOVERY_HOURS = max(24, int(os.getenv("TOPUS_WATCHDOG_RSS_RECOVERY_HOURS", "72")))
ACTIVE = os.getenv("TOPUS_WATCHDOG_ACTIVE", "false").lower() in {"1", "true", "yes"}
DB_PATH = Path(os.getenv("TOPUS_WATCHDOG_DB", "/data/topus-watchdog.sqlite3"))
SERVICE_ACCOUNT_FILE = Path(
    os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_FILE", "/run/secrets/google-service-account.json")
)
TOPUS_MAIN = Path(os.getenv("TOPUS_MAIN", "/app/src/main.py"))
PORT = int(os.getenv("PORT", "8090"))
HEARTBEAT_TOKEN = os.getenv("TOPUS_WATCHDOG_TOKEN", "").strip()

state_lock = threading.Lock()
state = {
    "ok": True,
    "active": ACTIVE,
    "last_check_at": None,
    "last_github_success_at": None,
    "last_heartbeat_at": None,
    "github_success_age_seconds": None,
    "github_active_run": False,
    "consecutive_api_failures": 0,
    "last_fallback_at": None,
    "last_fallback_exit_code": None,
    "fallback_running": False,
    "decision": "starting",
    "error": None,
}


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat(timespec="seconds")


def parse_github_time(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def inspect_runs(payload, now=None):
    now = now or utc_now()
    latest_success = None
    active = False
    for run in payload.get("workflow_runs") or []:
        created = parse_github_time(run.get("created_at"))
        status = str(run.get("status") or "")
        conclusion = str(run.get("conclusion") or "")
        if status in {"queued", "in_progress", "waiting", "requested", "pending"}:
            if (now - created).total_seconds() <= RUN_TIMEOUT_SECONDS:
                active = True
        if status == "completed" and conclusion == "success":
            completed = parse_github_time(run.get("updated_at") or run.get("created_at"))
            if latest_success is None or completed > latest_success:
                latest_success = completed
    age = None if latest_success is None else max(0, (now - latest_success).total_seconds())
    return latest_success, age, active


def should_fallback(success_age, active_run, api_failures, last_fallback_age):
    if active_run:
        return False, "github-run-active"
    if last_fallback_age is not None and last_fallback_age < FALLBACK_COOLDOWN_SECONDS:
        return False, "fallback-cooldown"
    if success_age is not None and success_age >= STALE_SECONDS:
        return True, "github-success-stale"
    if success_age is None and api_failures >= FAILURES_BEFORE_FALLBACK:
        return True, "github-api-unavailable"
    return False, "github-healthy"


def connect_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH)
    db.execute(
        "CREATE TABLE IF NOT EXISTS watchdog_events ("
        "id INTEGER PRIMARY KEY, created_at TEXT NOT NULL, event TEXT NOT NULL, details TEXT NOT NULL)"
    )
    db.commit()
    return db


def record_event(event, details):
    with connect_db() as db:
        db.execute(
            "INSERT INTO watchdog_events(created_at, event, details) VALUES (?, ?, ?)",
            (iso_now(), event, json.dumps(details, ensure_ascii=True, sort_keys=True)),
        )
        db.execute("DELETE FROM watchdog_events WHERE created_at < datetime('now', '-7 days')")
        db.commit()


def read_last_fallback():
    with connect_db() as db:
        row = db.execute(
            "SELECT created_at FROM watchdog_events WHERE event='fallback_finished' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return parse_github_time(row[0]) if row else None


def read_last_successful_heartbeat():
    with connect_db() as db:
        row = db.execute(
            "SELECT created_at FROM watchdog_events WHERE event='github_heartbeat_success' "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return parse_github_time(row[0]) if row else None


def github_snapshot():
    query = urllib.parse.urlencode({"branch": "main", "per_page": 20})
    request = urllib.request.Request(
        f"{GITHUB_API}?{query}",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "topus-publication-watchdog",
        },
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def fallback_environment():
    if not SERVICE_ACCOUNT_FILE.is_file():
        raise RuntimeError(f"service account file is missing: {SERVICE_ACCOUNT_FILE}")
    env = os.environ.copy()
    env.update(
        {
            "GOOGLE_SERVICE_ACCOUNT_JSON": SERVICE_ACCOUNT_FILE.read_text(encoding="utf-8"),
            "GITHUB_EVENT_NAME": "server_watchdog",
            "GITHUB_RUN_ID": f"server-{int(time.time())}",
            "TOPUS_RUN_SOURCE": "server-watchdog",
            "TOPUS_MAX_PUBLISH_AGE_HOURS_OVERRIDE": str(RSS_RECOVERY_HOURS),
            "TOPUS_RSS_FALLBACK_AGE_HOURS_OVERRIDE": str(RSS_RECOVERY_HOURS),
            "PYTHONUNBUFFERED": "1",
        }
    )
    return env


def run_fallback(reason):
    with state_lock:
        state["fallback_running"] = True
        state["decision"] = f"fallback:{reason}"
    started = iso_now()
    record_event("fallback_started", {"reason": reason, "active": ACTIVE})
    exit_code = None
    error = None
    try:
        if not ACTIVE:
            return
        result = subprocess.run(
            [sys.executable, str(TOPUS_MAIN)],
            cwd=str(TOPUS_MAIN.parent),
            env=fallback_environment(),
            timeout=RUN_TIMEOUT_SECONDS,
            check=False,
        )
        exit_code = result.returncode
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        exit_code = -1
    finally:
        finished = iso_now()
        record_event(
            "fallback_finished",
            {"reason": reason, "started_at": started, "finished_at": finished, "exit_code": exit_code, "error": error},
        )
        with state_lock:
            state["last_fallback_at"] = finished
            state["last_fallback_exit_code"] = exit_code
            state["fallback_running"] = False
            state["error"] = error


def monitor_once():
    now = utc_now()
    latest_success = read_last_successful_heartbeat()
    success_age = None if latest_success is None else max(0, (now - latest_success).total_seconds())
    active_run = False
    api_error = None
    with state_lock:
        failures = int(state["consecutive_api_failures"])
    # A fresh signed heartbeat is authoritative and avoids consuming the
    # shared unauthenticated GitHub API quota. The API is only a stale-path
    # secondary check that can detect a currently running job.
    if success_age is None or success_age >= STALE_SECONDS:
        try:
            api_success, api_age, active_run = inspect_runs(github_snapshot(), now)
            if api_success is not None and (latest_success is None or api_success > latest_success):
                latest_success = api_success
                success_age = api_age
            failures = 0
        except Exception as exc:
            failures += 1
            api_error = f"{type(exc).__name__}: {exc}"
    else:
        failures = 0

    last_fallback = read_last_fallback()
    last_fallback_age = None if last_fallback is None else max(0, (now - last_fallback).total_seconds())
    trigger, decision = should_fallback(success_age, active_run, failures, last_fallback_age)
    with state_lock:
        state.update(
            {
                "ok": api_error is None or failures < FAILURES_BEFORE_FALLBACK,
                "last_check_at": now.isoformat(timespec="seconds"),
                "last_github_success_at": latest_success.isoformat(timespec="seconds") if latest_success else None,
                "last_heartbeat_at": latest_success.isoformat(timespec="seconds") if latest_success else None,
                "github_success_age_seconds": round(success_age, 1) if success_age is not None else None,
                "github_active_run": active_run,
                "consecutive_api_failures": failures,
                "decision": decision,
                "error": api_error,
            }
        )
        already_running = bool(state["fallback_running"])
    if trigger and not already_running:
        run_fallback(decision)


def monitor_loop():
    while True:
        try:
            monitor_once()
        except Exception as exc:
            with state_lock:
                state["ok"] = False
                state["error"] = f"monitor failure: {type(exc).__name__}: {exc}"
            record_event("monitor_failed", {"error": str(exc)})
        time.sleep(CHECK_SECONDS)


class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in {"/", "/healthz", "/status"}:
            self.send_error(404)
            return
        with state_lock:
            payload = dict(state)
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(200 if payload["ok"] else 503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/heartbeat":
            self.send_error(404)
            return
        supplied = self.headers.get("Authorization", "")
        if not HEARTBEAT_TOKEN or supplied != f"Bearer {HEARTBEAT_TOKEN}":
            self.send_error(401)
            return
        length = min(4096, max(0, int(self.headers.get("Content-Length", "0"))))
        try:
            payload = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self.send_error(400)
            return
        status = str(payload.get("status") or "unknown")[:32]
        details = {
            "status": status,
            "run_id": str(payload.get("run_id") or "")[:64],
            "event": str(payload.get("event") or "")[:64],
            "sha": str(payload.get("sha") or "")[:40],
        }
        record_event("github_heartbeat_success" if status == "success" else "github_heartbeat_failed", details)
        with state_lock:
            state["last_heartbeat_at"] = iso_now()
            if status == "success":
                state["last_github_success_at"] = state["last_heartbeat_at"]
                state["github_success_age_seconds"] = 0
                state["consecutive_api_failures"] = 0
                state["decision"] = "github-heartbeat"
                state["error"] = None
                state["ok"] = True
        body = b'{"ok":true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format_string, *args):
        return


if __name__ == "__main__":
    connect_db().close()
    threading.Thread(target=monitor_loop, name="watchdog", daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORT), StatusHandler).serve_forever()
