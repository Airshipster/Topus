"""Run retention from the existing serialized publisher, including push-only runs."""
import os
import time
from pathlib import Path


def marker_path():
    return Path(os.environ.get('TOPUS_ACTIVITY_RETENTION_MARKER', '/data/activity-retention.ok'))


def retention_due(now=None):
    try:
        return (time.time() if now is None else now) - marker_path().stat().st_mtime >= 21600
    except FileNotFoundError:
        return True


def mark_retention_completed():
    path = marker_path()
    if path.parent.is_dir():
        path.touch()
