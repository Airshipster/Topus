import importlib.util
import os
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


os.environ.setdefault("TOPUS_WATCHDOG_STALE_SECONDS", "1200")
os.environ.setdefault("TOPUS_WATCHDOG_FALLBACK_COOLDOWN_SECONDS", "1800")
module_path = Path(__file__).parents[1] / "watchdog" / "watchdog.py"
spec = importlib.util.spec_from_file_location("topus_watchdog", module_path)
watchdog = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = watchdog
spec.loader.exec_module(watchdog)


class WatchdogDecisionTests(unittest.TestCase):
    def test_healthy_github_does_not_fallback(self):
        self.assertEqual(
            watchdog.should_fallback(300, False, 0, None),
            (False, "github-healthy"),
        )

    def test_stale_github_triggers_fallback(self):
        self.assertEqual(
            watchdog.should_fallback(1300, False, 0, None),
            (True, "github-success-stale"),
        )

    def test_active_run_blocks_fallback(self):
        self.assertEqual(
            watchdog.should_fallback(1300, True, 0, None),
            (False, "github-run-active"),
        )

    def test_cooldown_blocks_duplicate_fallback(self):
        self.assertEqual(
            watchdog.should_fallback(1300, False, 0, 120),
            (False, "fallback-cooldown"),
        )

    def test_repeated_api_failures_trigger_fallback(self):
        self.assertEqual(
            watchdog.should_fallback(None, False, watchdog.FAILURES_BEFORE_FALLBACK, None),
            (True, "github-api-unavailable"),
        )

    def test_snapshot_uses_latest_success_and_active_run(self):
        now = datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)
        payload = {
            "workflow_runs": [
                {
                    "status": "completed",
                    "conclusion": "success",
                    "created_at": (now - timedelta(minutes=8)).isoformat(),
                    "updated_at": (now - timedelta(minutes=7)).isoformat(),
                },
                {
                    "status": "in_progress",
                    "conclusion": None,
                    "created_at": (now - timedelta(minutes=2)).isoformat(),
                    "updated_at": (now - timedelta(minutes=1)).isoformat(),
                },
            ]
        }
        latest, age, active = watchdog.inspect_runs(payload, now)
        self.assertEqual(latest, now - timedelta(minutes=7))
        self.assertEqual(age, 420)
        self.assertTrue(active)


if __name__ == "__main__":
    unittest.main()
