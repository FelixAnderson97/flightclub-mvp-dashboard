"""
HGEM MVP Dashboard — GitHub Pages Deploy

SUMMARY
-------
Pushes Outputs/dashboard.html to the configured GitHub repo. GitHub Pages
auto-redeploys within 30-90 seconds, giving the team a fresh URL each run.

Usage:
  Set GITHUB_PAT in env (or via secrets.enc file once Phase 3 is set up).
  python deploy_to_pages.py [--message "custom commit msg"]

Config:
  GITHUB_OWNER  = "FelixAnderson97"
  GITHUB_REPO   = "flightclub-mvp-dashboard"
  GITHUB_BRANCH = "main"
  GITHUB_PATH   = "index.html"   # dashboard becomes the repo's index
"""
from __future__ import annotations
import argparse
import base64
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError

GITHUB_OWNER  = "FelixAnderson97"
GITHUB_REPO   = "flightclub-mvp-dashboard"
GITHUB_BRANCH = "main"
GITHUB_PATH   = "index.html"     # dashboard.html lives at repo root, served as index by Pages

API = "https://api.github.com"


def gh_request(method: str, url: str, token: str, body: dict | None = None) -> dict:
    """Make a GitHub REST API call. Returns the parsed JSON response."""
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "flightclub-mvp-dashboard-deployer",
    }
    data = json.dumps(body).encode("utf-8") if body else None
    if data:
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"GitHub API error {e.code} for {method} {url}\n{body}")


def get_existing_sha(token: str) -> str | None:
    """Return the SHA of the existing index.html, or None if it doesn't exist yet."""
    url = f"{API}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{GITHUB_PATH}?ref={GITHUB_BRANCH}"
    try:
        resp = gh_request("GET", url, token)
        return resp.get("sha")
    except SystemExit:
        return None  # file doesn't exist yet (404)


def push_dashboard(token: str, commit_message: str, dashboard_path: Path) -> str:
    """Upload dashboard.html to the repo as index.html. Returns the commit URL."""
    if not dashboard_path.exists():
        raise SystemExit(f"Dashboard file not found: {dashboard_path}")
    content_b64 = base64.b64encode(dashboard_path.read_bytes()).decode("ascii")

    existing_sha = get_existing_sha(token)
    body = {
        "message":  commit_message,
        "content":  content_b64,
        "branch":   GITHUB_BRANCH,
    }
    if existing_sha:
        body["sha"] = existing_sha

    url = f"{API}/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{GITHUB_PATH}"
    resp = gh_request("PUT", url, token, body)

    commit_url = resp.get("commit", {}).get("html_url", "")
    pages_url = f"https://{GITHUB_OWNER.lower()}.github.io/{GITHUB_REPO}/"
    print(f"  pushed {dashboard_path.name} -> {GITHUB_OWNER}/{GITHUB_REPO}/{GITHUB_PATH}")
    print(f"  commit: {commit_url}")
    print(f"  live URL (allow 30-90s for Pages to rebuild):")
    print(f"  {pages_url}")
    return pages_url


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--message", default=None, help="Commit message")
    ap.add_argument("--dashboard", default=None, help="Path to dashboard.html")
    args = ap.parse_args()

    # Look up token
    token = os.environ.get("GITHUB_PAT")
    if not token:
        # Try the encrypted creds file (Phase 3 will populate this)
        try:
            from creds import get_credential
            token = get_credential("github_pat")
        except Exception:
            pass
    if not token:
        raise SystemExit(
            "No GitHub PAT found. Set GITHUB_PAT env var or save it to the creds store."
        )

    project_dir = Path(__file__).resolve().parent.parent
    dashboard_path = Path(args.dashboard) if args.dashboard else project_dir / "Outputs" / "dashboard.html"
    msg = args.message or f"Auto-deploy dashboard {datetime.now().strftime('%Y-%m-%d %H:%M')}"

    push_dashboard(token, msg, dashboard_path)


if __name__ == "__main__":
    main()
