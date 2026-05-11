"""
Replay a previously-extracted issue into pydantic/httpx2.

Usage:
    uv run scripts/issue-migration/replay.py scripts/issue-migration/data/issue-1234.json [--dry-run]

Requires `gh` CLI authenticated with repo write access on pydantic/httpx2.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

TARGET_REPO = "pydantic/httpx2"
SOURCE_REPO = "encode/httpx"

MENTION_RE = re.compile(r"(?<![A-Za-z0-9_`])@([A-Za-z0-9](?:[A-Za-z0-9-]{0,38}))")


def escape_mentions(text: str | None) -> str | None:
    if text is None:
        return None
    return MENTION_RE.sub(r"`@\1`", text)


def link_user(login: str | None) -> str:
    if not login:
        return "unknown"
    return f"[`@{login}`](https://github.com/{login})"


def gh_api(method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    cmd = ["gh", "api", "--method", method, path]
    if body is not None:
        cmd += ["--input", "-"]
    result = subprocess.run(
        cmd,
        input=json.dumps(body) if body is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        raise SystemExit(result.returncode)
    return json.loads(result.stdout) if result.stdout.strip() else {}


def format_header(author: str | None, created_at: str | None, source_url: str | None) -> str:
    who = link_user(author)
    when = created_at or "unknown date"
    where = source_url or f"https://github.com/{SOURCE_REPO}"
    return f"> _Originally opened by {who} on {when} in [{SOURCE_REPO}]({where})_\n\n"


def format_comment_header(author: str | None, created_at: str | None) -> str:
    who = link_user(author)
    when = created_at or "unknown date"
    return f"> _Originally posted by {who} on {when}_\n\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("issue_file", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    issue = json.loads(args.issue_file.read_text())

    title = issue["title"] or f"(no title) #{issue['number']}"
    body = format_header(issue.get("author"), issue.get("created_at"), issue.get("url"))
    body += escape_mentions(issue.get("body")) or "_(no body)_"

    print(f"Issue: {title}")
    print(f"  State: {issue['state']}, comments: {len(issue['comments'])}")

    if args.dry_run:
        print("--- BODY ---")
        print(body)
        for i, c in enumerate(issue["comments"], 1):
            print(f"--- COMMENT {i} ---")
            print(format_comment_header(c.get("author"), c.get("created_at")) + (escape_mentions(c.get("body")) or ""))
        return

    created = gh_api("POST", f"/repos/{TARGET_REPO}/issues", {"title": title, "body": body})
    new_number = created["number"]
    print(f"Created {TARGET_REPO}#{new_number} -> {created['html_url']}")

    for c in issue["comments"]:
        comment_body = format_comment_header(c.get("author"), c.get("created_at")) + (escape_mentions(c.get("body")) or "")
        gh_api(
            "POST",
            f"/repos/{TARGET_REPO}/issues/{new_number}/comments",
            {"body": comment_body},
        )
    print(f"Posted {len(issue['comments'])} comments")

    if issue["state"] == "closed":
        gh_api("PATCH", f"/repos/{TARGET_REPO}/issues/{new_number}", {"state": "closed"})
        print("Closed.")


if __name__ == "__main__":
    main()
