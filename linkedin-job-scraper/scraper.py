import os
import random
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import gspread
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from gspread.exceptions import WorksheetNotFound
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

SHEET_ID = os.getenv("SHEET_ID", "").strip()
CREDS_FILE = os.getenv("CREDS_FILE", "service_account.json").strip()
WORKSHEET_NAME = "Jobs"

RESULTS_PER_PAGE = 25
MAX_PAGES_PER_KEYWORD = 3
MAX_POST_AGE_DAYS = 14
PAGE_DELAY_RANGE_SECONDS = (2, 4)
KEYWORD_DELAY_RANGE_SECONDS = (3, 6)
HEARTBEAT_INTERVAL_SECONDS = 5
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_KEYWORDS = [
    "Robotics Software Engineer Intern",
    "Robotics Engineer Coop",
    "Embedded Systems Intern",
    "Autonomous Systems Intern",
    "ROS Developer Intern",
    "Computer Vision Intern",
    "Motion Planning Intern",
    "Controls Engineer Coop",
    "Mechatronics Intern",
    "SLAM Engineer Intern",
    "Machine Learning Robotics Intern",
]
# Optional env override from GitHub variable KEYWORDS:
# "Keyword A|Keyword B|Keyword C"
KEYWORDS_ENV = os.getenv("KEYWORDS", "").strip()
KEYWORDS = [k.strip() for k in KEYWORDS_ENV.split("|") if k.strip()] if KEYWORDS_ENV else DEFAULT_KEYWORDS

HEADERS = [
    "Job Title",
    "Company",
    "Location",
    "Date Posted",
    "Job URL",
    "Apply URL",
    "Keyword",
    "Application Date",
    "Date Added",
]
APPLY_LINK_SELECTORS = [
    "a[data-tracking-control-name='public_jobs_topcard-apply']",
    "a.top-card-layout__cta--primary",
    "a.topcard__link",
    "a[href*='linkedin.com/jobs/view/'][href*='apply']",
]

RELATIVE_POSTED_PATTERN = re.compile(
    r"(?P<number>\d+)\+?\s*(?P<unit>hour|day|week|month|year)s?\s*ago", re.IGNORECASE
)


def log(message: str) -> None:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{timestamp}] {message}", flush=True)


def run_with_heartbeat(action: str, func, *args, **kwargs):
    done = threading.Event()

    def heartbeat() -> None:
        elapsed = 0
        while not done.wait(HEARTBEAT_INTERVAL_SECONDS):
            elapsed += HEARTBEAT_INTERVAL_SECONDS
            log(f"Heartbeat: still {action}... ({elapsed}s elapsed)")

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    started = time.monotonic()
    try:
        return func(*args, **kwargs)
    finally:
        done.set()
        thread.join(timeout=0.2)
        log(f"Finished {action} in {time.monotonic() - started:.1f}s")


def _sleep_random(delay_range: tuple[int, int], reason: str) -> None:
    sleep_seconds = random.uniform(*delay_range)
    log(f"Sleeping {sleep_seconds:.2f}s ({reason})")
    time.sleep(sleep_seconds)


def _build_search_url(keyword: str, start: int) -> str:
    keyword_encoded = quote_plus(keyword)
    return (
        "https://www.linkedin.com/jobs/search/"
        f"?keywords={keyword_encoded}&sortBy=DD&start={start}"
    )


def _normalize_job_url(url: str) -> str:
    return url.split("?", 1)[0].rstrip("/")


def _normalize_apply_url(apply_href: str, job_url: str) -> str:
    cleaned_href = apply_href.strip()
    if not cleaned_href or cleaned_href.startswith("javascript:") or cleaned_href == "#":
        return job_url

    absolute_url = urljoin(job_url, cleaned_href)
    parsed = urlparse(absolute_url)
    query = parse_qs(parsed.query)

    # LinkedIn sometimes wraps outbound links in query parameters.
    for key in ("url", "redirect", "redirectUrl", "target"):
        if key in query and query[key]:
            return unquote(query[key][0]).strip()

    return absolute_url


def resolve_apply_url(detail_page, job_url: str) -> str:
    try:
        run_with_heartbeat(
            action=f"resolving apply URL for job {job_url}",
            func=detail_page.goto,
            url=job_url,
            wait_until="domcontentloaded",
            timeout=45000,
        )
    except PlaywrightTimeoutError:
        log(f"Timed out opening job detail page for apply URL: {job_url}")
        return job_url

    for selector in APPLY_LINK_SELECTORS:
        link = detail_page.locator(selector).first
        try:
            if link.count() == 0:
                continue
            href = link.get_attribute("href", timeout=1500)
            if href:
                return _normalize_apply_url(href, job_url)
        except Exception:
            continue

    return job_url


def _parse_posted_datetime(posted_value: str, now_utc: datetime) -> datetime | None:
    if not posted_value:
        return None

    value = " ".join(posted_value.strip().split())
    iso_candidate = value
    if iso_candidate.endswith("Z"):
        iso_candidate = iso_candidate[:-1] + "+00:00"

    try:
        parsed_iso = datetime.fromisoformat(iso_candidate)
        if parsed_iso.tzinfo is not None:
            return parsed_iso.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed_iso
    except ValueError:
        pass

    try:
        parsed_date = date.fromisoformat(value[:10])
        return datetime.combine(parsed_date, datetime.min.time())
    except ValueError:
        pass

    normalized = value.lower().replace("reposted", "").replace("posted", "")
    normalized = " ".join(normalized.split())
    if "just now" in normalized or "today" in normalized:
        return now_utc
    if "yesterday" in normalized:
        return now_utc - timedelta(days=1)

    match = RELATIVE_POSTED_PATTERN.search(normalized)
    if not match:
        return None

    number = int(match.group("number"))
    unit = match.group("unit").lower()

    if unit == "hour":
        delta = timedelta(hours=number)
    elif unit == "day":
        delta = timedelta(days=number)
    elif unit == "week":
        delta = timedelta(weeks=number)
    elif unit == "month":
        delta = timedelta(days=number * 30)
    elif unit == "year":
        delta = timedelta(days=number * 365)
    else:
        return None

    return now_utc - delta


def _is_recent_enough(posted_value: str, now_utc: datetime) -> bool:
    posted_dt = _parse_posted_datetime(posted_value, now_utc)
    if not posted_dt:
        return False

    age = now_utc - posted_dt
    return timedelta(0) <= age <= timedelta(days=MAX_POST_AGE_DAYS)


def _safe_text(card, selectors: List[str]) -> str:
    for selector in selectors:
        locator = card.locator(selector).first
        if locator.count() == 0:
            continue
        try:
            text = locator.inner_text(timeout=1500).strip()
            if text:
                return " ".join(text.split())
        except Exception:
            continue
    return ""


def _safe_attr(card, selectors: List[str], attr_name: str) -> str:
    for selector in selectors:
        locator = card.locator(selector).first
        if locator.count() == 0:
            continue
        try:
            value = locator.get_attribute(attr_name, timeout=1500)
            if value:
                return value.strip()
        except Exception:
            continue
    return ""


def scrape_keyword_jobs(page, keyword: str, now_utc: datetime) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    skipped_not_recent = 0
    skipped_missing_url = 0
    log(f"=== Scraping keyword: {keyword} ===")

    for page_index in range(MAX_PAGES_PER_KEYWORD):
        if page_index > 0:
            _sleep_random(PAGE_DELAY_RANGE_SECONDS, "between page requests")

        start = page_index * RESULTS_PER_PAGE
        search_url = _build_search_url(keyword, start)
        log(f"Loading page {page_index + 1}/{MAX_PAGES_PER_KEYWORD}: {search_url}")

        try:
            run_with_heartbeat(
                action=f"loading page {page_index + 1} for keyword '{keyword}'",
                func=page.goto,
                url=search_url,
                wait_until="domcontentloaded",
                timeout=60000,
            )
        except PlaywrightTimeoutError:
            log(f"Timed out loading search page for keyword '{keyword}', start={start}")
            continue

        try:
            log(f"Waiting for job cards on page {page_index + 1} for keyword '{keyword}'")
            run_with_heartbeat(
                action=f"waiting for job cards on page {page_index + 1} for keyword '{keyword}'",
                func=page.wait_for_selector,
                selector="ul.jobs-search__results-list li",
                timeout=15000,
            )
        except PlaywrightTimeoutError:
            log(f"No job list found for keyword '{keyword}', page {page_index + 1}")
            break

        cards = page.locator("ul.jobs-search__results-list li")
        card_count = cards.count()
        log(f"Found {card_count} cards on page {page_index + 1}")
        if card_count == 0:
            break

        for i in range(card_count):
            if i > 0 and i % 10 == 0:
                log(
                    f"Parsed {i}/{card_count} cards on page {page_index + 1} for keyword '{keyword}'"
                )

            card = cards.nth(i)
            title = _safe_text(card, ["h3.base-search-card__title", "h3"])
            company = _safe_text(
                card,
                ["h4.base-search-card__subtitle", ".base-search-card__subtitle", "h4"],
            )
            location = _safe_text(card, [".job-search-card__location", ".job-search-card__location-text"])
            posted_date = _safe_attr(card, ["time"], "datetime") or _safe_text(
                card, ["time", ".job-search-card__listdate", ".job-search-card__listdate--new"]
            )
            if not _is_recent_enough(posted_date, now_utc):
                skipped_not_recent += 1
                continue

            raw_url = _safe_attr(card, ["a.base-card__full-link", "a[href*='/jobs/view/']"], "href")
            if not raw_url:
                skipped_missing_url += 1
                continue

            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "date_posted": posted_date,
                    "job_url": _normalize_job_url(raw_url),
                    "keyword": keyword,
                }
            )

        if card_count < RESULTS_PER_PAGE:
            log("Less than 25 results on page; stopping pagination for this keyword.")
            break

    log(
        f"Collected {len(jobs)} recent rows for '{keyword}'. "
        f"Skipped old/unknown-date: {skipped_not_recent}, missing URL: {skipped_missing_url}"
    )
    return jobs


def get_gspread_client():
    creds_path = Path(CREDS_FILE)
    if not creds_path.exists():
        raise FileNotFoundError(f"Google credentials file not found: {CREDS_FILE}")
    log(f"Loading Google credentials from: {creds_path.as_posix()}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = service_account.Credentials.from_service_account_file(
        creds_path.as_posix(), scopes=scopes
    )
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    log("Google Sheets client authorized.")
    return gspread.authorize(credentials)


def get_spreadsheet(client):
    if not SHEET_ID:
        raise ValueError("SHEET_ID is not set. Provide it via environment variable.")

    log(f"Opening spreadsheet with SHEET_ID: {SHEET_ID}")
    return client.open_by_key(SHEET_ID)


def get_jobs_worksheet(spreadsheet):
    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        log(f"Using existing worksheet: {WORKSHEET_NAME}")
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=20)
        log(f"Created worksheet: {WORKSHEET_NAME}")
    return worksheet


def ensure_headers(worksheet) -> None:
    existing_values = worksheet.get_all_values()
    if not existing_values:
        worksheet.append_row(HEADERS, value_input_option="RAW")
        log(f"Header row initialized for tab '{worksheet.title}'.")
        return

    first_row = existing_values[0]
    if first_row[: len(HEADERS)] != HEADERS:
        worksheet.update("A1:I1", [HEADERS], value_input_option="RAW")
        log(f"Header row updated for tab '{worksheet.title}'.")


def get_existing_job_urls(worksheet) -> set[str]:
    log(f"Loading existing rows from tab '{worksheet.title}' for deduplication.")
    rows = worksheet.get_all_values()
    existing_urls = set()
    for row in rows[1:]:
        if len(row) >= 5 and row[4].strip():
            existing_urls.add(_normalize_job_url(row[4].strip()))
    log(f"Loaded {len(existing_urls)} existing URLs from tab '{worksheet.title}'")
    return existing_urls


def main() -> None:
    log("Scraper started.")
    log(
        f"Keyword source: {'KEYWORDS env var' if KEYWORDS_ENV else 'DEFAULT_KEYWORDS list'}. "
        f"Total keywords: {len(KEYWORDS)}"
    )
    log(f"Date filter: jobs posted within the last {MAX_POST_AGE_DAYS} days.")

    client = get_gspread_client()
    spreadsheet = get_spreadsheet(client)
    worksheet = get_jobs_worksheet(spreadsheet)
    ensure_headers(worksheet)
    existing_urls = get_existing_job_urls(worksheet)
    date_added = date.today().isoformat()
    now_utc = datetime.utcnow()

    total_new_rows = 0
    total_duplicates_skipped = 0

    with sync_playwright() as playwright:
        log("Launching Playwright Chromium in headless mode.")
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1366, "height": 768},
        )
        page = context.new_page()
        detail_page = context.new_page()

        for idx, keyword in enumerate(KEYWORDS):
            log(f"Starting keyword {idx + 1}/{len(KEYWORDS)}: {keyword}")

            keyword_jobs = scrape_keyword_jobs(page, keyword, now_utc)
            keyword_rows_to_append: List[List[str]] = []
            keyword_duplicates_skipped = 0

            for job in keyword_jobs:
                job_url = job["job_url"]
                if job_url in existing_urls:
                    keyword_duplicates_skipped += 1
                    continue

                existing_urls.add(job_url)
                apply_url = resolve_apply_url(detail_page, job_url)
                keyword_rows_to_append.append(
                    [
                        job["title"],
                        job["company"],
                        job["location"],
                        job["date_posted"],
                        job_url,
                        apply_url,
                        keyword,
                        "",
                        date_added,
                    ]
                )

            if keyword_rows_to_append:
                log(
                    f"Appending {len(keyword_rows_to_append)} rows to tab '{worksheet.title}' "
                    f"for keyword '{keyword}'."
                )
                worksheet.append_rows(keyword_rows_to_append, value_input_option="RAW")
                log(f"Appended {len(keyword_rows_to_append)} rows to tab '{worksheet.title}'.")
            else:
                log(f"No new rows to append for keyword '{keyword}'.")

            total_new_rows += len(keyword_rows_to_append)
            total_duplicates_skipped += keyword_duplicates_skipped
            log(
                f"Keyword complete: {keyword}. New rows: {len(keyword_rows_to_append)}. "
                f"Duplicates skipped: {keyword_duplicates_skipped}"
            )

            if idx < len(KEYWORDS) - 1:
                _sleep_random(KEYWORD_DELAY_RANGE_SECONDS, "between keywords")

        context.close()
        browser.close()
        log("Browser session closed.")

    log(
        f"Scraper finished. Total new rows: {total_new_rows}. "
        f"Total duplicates skipped: {total_duplicates_skipped}"
    )


if __name__ == "__main__":
    main()
