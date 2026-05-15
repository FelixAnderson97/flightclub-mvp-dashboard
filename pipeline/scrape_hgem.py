"""
HGEM Hub — Headless Scraper

SUMMARY
-------
Logs into hub.hgem.com via Azure B2C, navigates to the data export, downloads
the latest CSV, and saves it to the data/ folder with the conventional
filename pattern (Hub_Data_Export__YYYY-MM-DD__HH-MM.csv).

This is designed to run in GitHub Actions on ubuntu-latest. Requires:
  - playwright (headless chromium)
  - HGEM_USERNAME and HGEM_PASSWORD environment variables

The script writes diagnostic screenshots to Outputs/scrape_screenshots/ on
every step + on failure, so we can see what went wrong if HGEM redesigns or
changes the flow.

Usage:
  HGEM_USERNAME=... HGEM_PASSWORD=... python scrape_hgem.py
"""
from __future__ import annotations
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Playwright import is local so the rest of the pipeline can run without it
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    raise SystemExit(
        "playwright not installed. Run: pip install playwright && playwright install chromium"
    )


HGEM_URL = "https://hub.hgem.com/"
PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_DIR / "data"
SHOT_DIR = PROJECT_DIR / "Outputs" / "scrape_screenshots"
SHOT_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)


def shot(page, name: str):
    """Save a screenshot for diagnostic purposes."""
    path = SHOT_DIR / f"{datetime.now().strftime('%H%M%S')}_{name}.png"
    try:
        page.screenshot(path=str(path), full_page=True)
        print(f"  [shot] {path.name}")
    except Exception as e:
        print(f"  [shot] failed: {e}")


def login(page, username: str, password: str):
    """Walk through Azure B2C login. Iteratively try common selectors."""
    print(f"  navigating to {HGEM_URL}")
    page.goto(HGEM_URL, wait_until="domcontentloaded", timeout=30000)
    shot(page, "01_landing")

    # Wait for redirect to login.hgem.com
    page.wait_for_url("**/login.hgem.com/**", timeout=30000)
    print(f"  redirected to: {page.url}")
    shot(page, "02_login_page")

    # Azure B2C usually has an email field — try common selectors
    email_selectors = [
        'input[type="email"]',
        'input[name="username"]',
        'input[name="signInName"]',
        'input[id="signInName"]',
        'input[id="email"]',
        'input[autocomplete="username"]',
    ]
    email_filled = False
    for sel in email_selectors:
        try:
            page.wait_for_selector(sel, timeout=5000)
            page.fill(sel, username)
            print(f"  filled email via selector: {sel}")
            email_filled = True
            break
        except PWTimeout:
            continue
    if not email_filled:
        shot(page, "03_email_field_missing")
        raise SystemExit("Could not find email/username field on login page. See screenshot.")

    # Password field
    password_selectors = [
        'input[type="password"]',
        'input[name="password"]',
        'input[id="password"]',
        'input[autocomplete="current-password"]',
    ]
    pw_filled = False
    for sel in password_selectors:
        try:
            page.wait_for_selector(sel, timeout=5000)
            page.fill(sel, password)
            print(f"  filled password via selector: {sel}")
            pw_filled = True
            break
        except PWTimeout:
            continue

    if not pw_filled:
        # Some B2C flows are 2-step: enter email, click "Next", then password appears
        next_selectors = [
            'input[type="submit"][value*="ext" i]',
            'button:has-text("Next")',
            'button:has-text("Continue")',
            'button[type="submit"]',
        ]
        for sel in next_selectors:
            try:
                page.click(sel, timeout=3000)
                print(f"  clicked Next via: {sel}")
                page.wait_for_load_state("networkidle", timeout=15000)
                break
            except PWTimeout:
                continue
        # Try password selectors again after clicking Next
        for sel in password_selectors:
            try:
                page.wait_for_selector(sel, timeout=10000)
                page.fill(sel, password)
                pw_filled = True
                print(f"  filled password via selector (post-Next): {sel}")
                break
            except PWTimeout:
                continue

    if not pw_filled:
        shot(page, "04_password_field_missing")
        raise SystemExit("Could not find password field. See screenshot.")

    shot(page, "05_credentials_filled")

    # Submit
    submit_selectors = [
        'button:has-text("Sign in")',
        'button:has-text("Log in")',
        'button:has-text("Login")',
        'input[type="submit"]',
        'button[type="submit"]',
    ]
    submitted = False
    for sel in submit_selectors:
        try:
            page.click(sel, timeout=3000)
            print(f"  clicked submit via: {sel}")
            submitted = True
            break
        except PWTimeout:
            continue
    if not submitted:
        raise SystemExit("Could not find sign-in button.")

    # Wait for return to hub.hgem.com
    try:
        page.wait_for_url("**/hub.hgem.com/**", timeout=30000)
        print(f"  logged in, now at: {page.url}")
        shot(page, "06_post_login")
    except PWTimeout:
        shot(page, "06_post_login_timeout")
        # Log what page we ended up on
        print(f"  WARNING: did not redirect back to hub.hgem.com. Current URL: {page.url}")


def find_and_download_export(page) -> Path | None:
    """
    Navigate to the data export view and trigger download.
    THIS IS A BEST-GUESS — we don't know the exact UI yet. The script will
    save screenshots so the actual flow can be wired in next iteration.
    """
    print(f"  current page: {page.url}")
    shot(page, "10_hub_landing")

    # Wait for the SPA to load
    try:
        page.wait_for_load_state("networkidle", timeout=20000)
    except PWTimeout:
        pass

    shot(page, "11_hub_loaded")

    # Best-guess: look for an "Export" / "Download" link or button.
    # If this fails, the screenshot will show us where to point next.
    export_candidates = [
        'a:has-text("Export")',
        'button:has-text("Export")',
        'a:has-text("Download")',
        'a:has-text("CSV")',
        'a[href*="export"]',
        'a[href*="download"]',
    ]

    # Set up download handler
    print(f"  scanning for export controls...")
    with page.expect_download(timeout=60000) as download_info:
        clicked = False
        for sel in export_candidates:
            try:
                if page.locator(sel).count() > 0:
                    print(f"  clicking export via: {sel}")
                    page.locator(sel).first.click()
                    clicked = True
                    break
            except Exception as e:
                print(f"    selector {sel} failed: {e}")
                continue
        if not clicked:
            shot(page, "12_no_export_button")
            print("  no export button found on current page. See screenshot.")
            print("  TODO: navigate manually to the export view and find the correct selector.")
            return None

        download = download_info.value

    # Save with HGEM's filename convention
    target = DATA_DIR / f"Hub_Data_Export__{datetime.now().strftime('%Y-%m-%d__%H-%M')}.csv"
    download.save_as(str(target))
    print(f"  saved CSV: {target}")
    return target


def main():
    username = os.environ.get("HGEM_USERNAME")
    password = os.environ.get("HGEM_PASSWORD")
    if not username or not password:
        raise SystemExit("HGEM_USERNAME and HGEM_PASSWORD env vars must be set.")

    print(f"[HGEM scrape] starting at {datetime.now().isoformat(timespec='seconds')}")
    print(f"             user: {username}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            accept_downloads=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
            viewport={"width": 1400, "height": 900},
        )
        page = context.new_page()

        try:
            login(page, username, password)
            path = find_and_download_export(page)
            if path:
                print(f"\n[HGEM scrape] success — CSV at {path}")
                return 0
            else:
                print(f"\n[HGEM scrape] no CSV downloaded — review screenshots in {SHOT_DIR}")
                return 1
        except Exception as e:
            shot(page, "99_failure")
            print(f"\n[HGEM scrape] FAILED: {e}")
            print(f"  screenshots saved in {SHOT_DIR}")
            return 2
        finally:
            browser.close()


if __name__ == "__main__":
    sys.exit(main())
