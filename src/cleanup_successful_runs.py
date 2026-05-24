from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone


def request_json(url: str, token: str, method: str = "GET") -> dict:
    request = urllib.request.Request(
        url,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "topus-cleanup-successful-runs/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status == 204:
            return {}
        body = response.read()
        return json.loads(body) if body else {}


def delete_run(url: str, token: str) -> None:
    request = urllib.request.Request(
        url,
        method="DELETE",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "topus-cleanup-successful-runs/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        if response.status not in {202, 204}:
            raise RuntimeError(f"Unexpected delete status: {response.status}")


def main() -> int:
    token = os.environ["GITHUB_TOKEN"]
    repository = os.environ["GITHUB_REPOSITORY"]
    workflow_id = os.environ.get("WORKFLOW_ID", "sync_site_channel_list.yml")
    keep_days = int(os.environ.get("KEEP_SUCCESSFUL_RUN_DAYS", "30"))
    cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)

    base = f"https://api.github.com/repos/{repository}/actions/workflows/{urllib.parse.quote(workflow_id)}/runs"
    page = 1
    deleted = 0

    while True:
        url = f"{base}?status=completed&per_page=100&page={page}"
        data = request_json(url, token)
        runs = data.get("workflow_runs", [])
        if not runs:
            break

        for run in runs:
            if run.get("conclusion") != "success":
                continue
            created_at = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
            if created_at >= cutoff:
                continue
            delete_run(run["url"], token)
            deleted += 1

        page += 1

    print(f"Deleted {deleted} successful workflow runs older than {keep_days} days.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except urllib.error.HTTPError as error:
        print(f"GitHub cleanup failed: HTTP {error.code} {error.read().decode('utf-8', errors='replace')}", file=sys.stderr)
        raise SystemExit(1)
