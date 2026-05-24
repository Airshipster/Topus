from __future__ import annotations

import base64
import csv
import os
import re
import subprocess
import sys
import tempfile
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import config
from sheets import authenticate_google_sheets, clean_sheet_value, get_values_with_quota_retry


CANONICAL_HEADER = [
    "Название\nпроекта",
    "Ссылка \nна канал",
    "Партнёр \nSciTopus",
    "Кол.\n видео",
    "Год послед.\n видео",
    "Год создания\n канала",
    "TG-канал",
]

SOURCE_TIMESTAMP_PATTERN = re.compile(r"Обновление завершено:\s*(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2}):(\d{2})")
SOURCE_PROGRESS_PATTERN = re.compile(r"^\[\d{2}:\d{2}:\d{2}\]\s+")


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def optional_env(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def run(args: list[str], cwd: Path | None = None) -> str:
    try:
        completed = subprocess.run(
            args,
            cwd=cwd,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        return completed.stdout.strip()
    except subprocess.CalledProcessError as error:
        output = (error.stdout or "").strip()
        if output:
            print(output, file=sys.stderr)
        raise


def is_https_git_url(repo_url: str) -> bool:
    parsed = urllib.parse.urlparse(repo_url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def is_ssh_git_url(repo_url: str) -> bool:
    parsed = urllib.parse.urlparse(repo_url)
    return parsed.scheme == "ssh" or repo_url.startswith("git@")


def validate_git_url(repo_url: str) -> None:
    if not (is_https_git_url(repo_url) or is_ssh_git_url(repo_url)):
        raise RuntimeError("GITLAB_REPO_URL must be an HTTPS or SSH Git URL")


def clone_repo(repo_url: str, username: str, token: str, branch: str, repo_dir: Path) -> None:
    validate_git_url(repo_url)
    if is_ssh_git_url(repo_url):
        run(["git", "clone", "--depth", "1", "--branch", branch, repo_url, str(repo_dir)])
        return

    if not token:
        raise RuntimeError("Missing required environment variable for HTTPS Git URL: GITLAB_TOKEN")

    credentials = base64.b64encode(f"{username}:{token}".encode("utf-8")).decode("ascii")
    run([
        "git",
        "-c",
        f"http.https://gitlab.com/.extraheader=AUTHORIZATION: Basic {credentials}",
        "clone",
        "--depth",
        "1",
        "--branch",
        branch,
        repo_url,
        str(repo_dir),
    ])


def push_repo(repo_url: str, username: str, token: str, branch: str, repo_dir: Path) -> None:
    if is_ssh_git_url(repo_url):
        run(["git", "push", "origin", f"HEAD:{branch}"], cwd=repo_dir)
        return

    if not token:
        raise RuntimeError("Missing required environment variable for HTTPS Git URL: GITLAB_TOKEN")

    credentials = base64.b64encode(f"{username}:{token}".encode("utf-8")).decode("ascii")
    run([
        "git",
        "-c",
        f"http.https://gitlab.com/.extraheader=AUTHORIZATION: Basic {credentials}",
        "push",
        "origin",
        f"HEAD:{branch}",
    ], cwd=repo_dir)


def normalize_youtube_url(value: str) -> str:
    value = value.strip()
    for prefix in ("https://www.", "http://www.", "https://", "http://"):
        if value.startswith(prefix):
            value = value[len(prefix):]
            break
    return value.strip().rstrip("/")


def normalize_telegram_url(value: str) -> str:
    value = value.strip()
    if value.startswith("-"):
        return ""
    return value


def should_skip_row(row: list[str]) -> bool:
    non_empty = [cell for cell in row if cell]
    if not non_empty:
        return True

    project_name = row[0] if row else ""
    youtube_url = row[1] if len(row) > 1 else ""
    if not youtube_url and len(non_empty) == 1 and SOURCE_PROGRESS_PATTERN.match(project_name):
        return True

    if not youtube_url and len(non_empty) == 1 and len(project_name) >= 20:
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
        return all(char in allowed for char in project_name)

    return False


def parse_source_updated_at(row: list[str], target_utc_offset_hours: int, timezone_label: str) -> str | None:
    text = " ".join(cell for cell in row if cell).strip()
    match = SOURCE_TIMESTAMP_PATTERN.search(text)
    if not match:
        return None

    day, month, year, hour, minute, second = map(int, match.groups())
    source_timezone = timezone(timedelta(hours=4))
    target_timezone = timezone(timedelta(hours=target_utc_offset_hours))
    source_time = datetime(year, month, day, hour, minute, second, tzinfo=source_timezone)
    return source_time.astimezone(target_timezone).strftime(f"%Y-%m-%d %H:%M:%S {timezone_label}")


def read_source_values() -> list[list[str]]:
    spreadsheet_id = optional_env("TOPUS_MASTER_SPREADSHEET_ID", config.SPREADSHEET_ID)
    worksheet_name = optional_env("TOPUS_SITE_WORKSHEET_NAME", "Сайт")
    client = authenticate_google_sheets()
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(worksheet_name)
    return get_values_with_quota_retry(worksheet, "A:Z", attempts=5)


def normalize_rows(values: list[list[str]], target_utc_offset_hours: int, timezone_label: str) -> tuple[list[list[str]], str | None]:
    if not values:
        raise RuntimeError("The source sheet is empty")

    output: list[list[str]] = [CANONICAL_HEADER]
    seen_links: set[str] = set()
    source_updated_at: str | None = None

    for line_number, raw_row in enumerate(values[1:], start=2):
        row = [str(clean_sheet_value(cell) or "").strip() for cell in raw_row]
        parsed_updated_at = parse_source_updated_at(row, target_utc_offset_hours, timezone_label)
        if parsed_updated_at:
            source_updated_at = parsed_updated_at
            continue

        if not any(row):
            continue
        if len(row) < 7:
            row.extend([""] * (7 - len(row)))
        row = row[:7]
        row[1] = normalize_youtube_url(row[1])
        row[6] = normalize_telegram_url(row[6])

        if should_skip_row(row):
            continue

        if row[1]:
            key = row[1].lower()
            if key in seen_links:
                raise RuntimeError(f"Duplicate channel link in source sheet at row {line_number}: {row[1]}")
            seen_links.add(key)

        output.append(row)

    if len(output) <= 1:
        raise RuntimeError("The source sheet has no data rows")
    return output, source_updated_at


def write_csv(path: Path, rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerows(rows)


def configure_git(repo: Path) -> None:
    run(["git", "config", "user.name", optional_env("GIT_AUTHOR_NAME", "topus-site-list-update")], cwd=repo)
    run(["git", "config", "user.email", optional_env("GIT_AUTHOR_EMAIL", "topus-site-list-update@users.noreply.github.com")], cwd=repo)


def main() -> int:
    gitlab_repo_url = optional_env("GITLAB_REPO_URL", "git@gitlab.com:scitopus/scitopus-site.git")
    gitlab_token = optional_env("GITLAB_TOKEN", "")
    gitlab_username = optional_env("GITLAB_USERNAME", "oauth2")
    branch = optional_env("GITLAB_BRANCH", "main")
    csv_path = Path(optional_env("CSV_PATH", "assets/channels.csv"))
    updated_at_path = Path(optional_env("UPDATED_AT_PATH", "assets/channels-updated-at.txt"))
    utc_offset_hours = int(optional_env("APP_UTC_OFFSET_HOURS", "3"))
    timezone_label = optional_env("APP_TIMEZONE_LABEL", "UTC+3")

    source_rows, source_updated_at = normalize_rows(read_source_values(), utc_offset_hours, timezone_label)
    display_timezone = timezone(timedelta(hours=utc_offset_hours))
    updated_at = source_updated_at or datetime.now(display_timezone).strftime(f"%Y-%m-%d %H:%M:%S {timezone_label}")

    with tempfile.TemporaryDirectory(prefix="topus-site-list-sync-") as temp_dir:
        repo_dir = Path(temp_dir) / "website"
        clone_repo(gitlab_repo_url, gitlab_username, gitlab_token, branch, repo_dir)
        configure_git(repo_dir)

        write_csv(repo_dir / csv_path, source_rows)
        (repo_dir / updated_at_path).parent.mkdir(parents=True, exist_ok=True)
        (repo_dir / updated_at_path).write_text(updated_at + "\n", encoding="utf-8")

        status = run(["git", "status", "--porcelain"], cwd=repo_dir)
        if not status:
            print("No website changes detected.")
            return 0

        run(["git", "add", str(csv_path), str(updated_at_path)], cwd=repo_dir)
        run(["git", "commit", "-m", f"chore: sync channel list {updated_at}"], cwd=repo_dir)
        push_repo(gitlab_repo_url, gitlab_username, gitlab_token, branch, repo_dir)
        print(f"Updated channel list and timestamp: {updated_at}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Sync failed: {error}", file=sys.stderr)
        raise SystemExit(1)
