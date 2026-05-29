from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from sheets import authenticate_google_sheets, clean_sheet_value, get_values_with_quota_retry
from sync_site_channels_to_gitlab import (
    clone_repo,
    configure_git,
    optional_env,
    push_repo,
    run,
)


SPREADSHEET_ID = "1m67OLnwzOLCjnLCj_xZG_eT6R90yXwLWLXOxkTrjDuY"


def parse_year(value: object) -> int | None:
    text = str(clean_sheet_value(value) or "")
    for token in text.replace("-", ".").replace("/", ".").split("."):
        token = token.strip()
        if len(token) == 4 and token.isdigit() and token.startswith(("19", "20")):
            return int(token)
    return None


def parse_source_updated(value: object) -> str:
    text = str(clean_sheet_value(value) or "").strip()
    if not text:
        return ""

    match = re.search(r"(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2}):(\d{2})", text)
    if not match:
        return ""

    day, month, year, hour, minute, second = match.groups()
    return datetime(
        int(year),
        int(month),
        int(day),
        int(hour),
        int(minute),
        int(second),
        tzinfo=ZoneInfo("Asia/Baku"),
    ).isoformat()


def read_graph_payload() -> dict:
    spreadsheet_id = optional_env("TOPUS_MASTER_SPREADSHEET_ID", SPREADSHEET_ID)
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(spreadsheet_id)

    video_rows = get_values_with_quota_retry(
        spreadsheet.worksheet("Стат. Видео"),
        "E2:N",
        attempts=5,
        value_render_option="FORMATTED_VALUE",
    )
    counts: Counter[int] = Counter()
    for row in video_rows:
        year = parse_year(row[0] if row else "")
        if year:
            counts[year] += 1
    if len(counts) < 5:
        raise RuntimeError(
            "Graph source returned too few yearly buckets; refusing to overwrite site graph data."
        )

    source_update = get_values_with_quota_retry(
        spreadsheet.worksheet("Стат. Каналы"),
        "A2",
        attempts=5,
        value_render_option="FORMATTED_VALUE",
    )
    source_updated = parse_source_updated(source_update[0][0] if source_update and source_update[0] else "")
    now = datetime.now(ZoneInfo("Asia/Baku")).replace(microsecond=0)

    return {
        "title": "Количество русскоязычных научпоп-видео\nпо годам",
        "subtitle": "Все научно-популярные видео на русском языке",
        "brand": "SciTopus",
        "lastUpdated": now.isoformat(),
        "sourceUpdated": source_updated,
        "source": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?gid=1303884430#gid=1303884430",
        "years": [
            {
                "year": year,
                "value": counts[year],
                **({"partial": True} if year >= now.year else {}),
            }
            for year in sorted(counts)
        ],
    }


def main() -> int:
    gitlab_repo_url = optional_env("GITLAB_REPO_URL", "git@gitlab.com:scitopus/scitopus-site.git")
    gitlab_token = optional_env("GITLAB_TOKEN", "")
    gitlab_username = optional_env("GITLAB_USERNAME", "oauth2")
    branch = optional_env("GITLAB_BRANCH", "main")
    graph_data_path = Path(optional_env("GRAPH_DATA_PATH", "public/scitopus-graph/data.json"))
    payload = read_graph_payload()

    import tempfile

    with tempfile.TemporaryDirectory(prefix="topus-site-graph-sync-") as temp_dir:
        repo_dir = Path(temp_dir) / "website"
        clone_repo(gitlab_repo_url, gitlab_username, gitlab_token, branch, repo_dir)
        configure_git(repo_dir)

        output_path = repo_dir / graph_data_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        status = run(["git", "status", "--porcelain"], cwd=repo_dir)
        if not status:
            print("No graph data changes detected.")
            return 0

        run(["git", "add", str(graph_data_path)], cwd=repo_dir)
        updated_at = payload["lastUpdated"]
        run(["git", "commit", "-m", f"chore: sync graph data {updated_at}"], cwd=repo_dir)
        push_repo(gitlab_repo_url, gitlab_username, gitlab_token, branch, repo_dir)
        print(f"Updated graph data: {updated_at}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
