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


def read_graph_payload(now: datetime | None = None) -> dict:
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
    now = now or datetime.now(ZoneInfo("Asia/Baku")).replace(microsecond=0)

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


def current_month_was_synced(output_path: Path, now: datetime) -> bool:
    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
        updated_at = datetime.fromisoformat(str(payload.get("lastUpdated", "")))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return False

    return updated_at.year == now.year and updated_at.month == now.month


def should_skip_sync(output_path: Path, now: datetime) -> bool:
    event_name = optional_env("GITHUB_EVENT_NAME", "")
    force_sync = optional_env("FORCE_SITE_GRAPH_SYNC", "").lower() in {"1", "true", "yes"}
    if force_sync or event_name == "workflow_dispatch":
        return False

    if event_name == "schedule" and current_month_was_synced(output_path, now):
        print(f"Graph data already synced for {now:%Y-%m}; skipping scheduled retry.")
        return True

    return False


def with_embedded_initial_graph_data(index_html: str, payload: dict) -> str:
    json_payload = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</script", "<\\/script")
    script = f'<script id="initialGraphData" type="application/json">{json_payload}</script>'
    pattern = r'\s*<script id="initialGraphData" type="application/json">.*?</script>\s*'
    if re.search(pattern, index_html, flags=re.S):
        return re.sub(pattern, f"\n    {script}\n", index_html, count=1, flags=re.S)
    return re.sub(
        r'(\s*)<script src="scitopus-graph\.js"></script>',
        rf'\1{script}\n\1<script src="scitopus-graph.js"></script>',
        index_html,
        count=1,
    )


def write_graph_payload(repo_dir: Path, graph_data_path: Path, payload: dict) -> None:
    output_path = repo_dir / graph_data_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    index_path = output_path.parent / "index.html"
    if index_path.exists():
        index_path.write_text(
            with_embedded_initial_graph_data(index_path.read_text(encoding="utf-8"), payload),
            encoding="utf-8",
        )


def main() -> int:
    gitlab_repo_url = optional_env("GITLAB_REPO_URL", "git@gitlab.com:scitopus/scitopus-site.git")
    gitlab_token = optional_env("GITLAB_TOKEN", "")
    gitlab_username = optional_env("GITLAB_USERNAME", "oauth2")
    branch = optional_env("GITLAB_BRANCH", "main")
    graph_data_path = Path(optional_env("GRAPH_DATA_PATH", "public/scitopus-graph/data.json"))
    now = datetime.now(ZoneInfo("Asia/Baku")).replace(microsecond=0)

    import tempfile

    with tempfile.TemporaryDirectory(prefix="topus-site-graph-sync-") as temp_dir:
        repo_dir = Path(temp_dir) / "website"
        clone_repo(gitlab_repo_url, gitlab_username, gitlab_token, branch, repo_dir)
        configure_git(repo_dir)

        output_path = repo_dir / graph_data_path
        if should_skip_sync(output_path, now):
            return 0

        payload = read_graph_payload(now)
        write_graph_payload(repo_dir, graph_data_path, payload)

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
