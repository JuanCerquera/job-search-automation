import random
import time
from datetime import date
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote_plus

import gspread
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from gspread.exceptions import WorksheetNotFound
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

SHEET_ID = "YOUR_GOOGLE_SHEET_ID"
CREDS_FILE = "service_account.json"

WORKSHEET_NAME = "Jobs"
RESULTS_PER_PAGE = 25
MAX_PAGES_PER_KEYWORD = 3
PAGE_DELAY_RANGE_SECONDS = (2, 4)
KEYWORD_DELAY_RANGE_SECONDS = (3, 6)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

KEYWORDS = [
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

HEADERS = [
    "Job Title",
    "Company",
    "Location",
    "Date Posted",
    "Job URL",
    "Keyword",
    "Application Status",
    "Date Added",
]


def _sleep_random(delay_range: tuple[int, int], reason: str) -> None:
    sleep_seconds = random.uniform(*delay_range)
    print(f"Sleeping {sleep_seconds:.2f}s ({reason})")
    time.sleep(sleep_seconds)


def _build_search_url(keyword: str, start: int) -> str:
    keyword_encoded = quote_plus(keyword)
    return (
        "https://www.linkedin.com/jobs/search/"
        f"?keywords={keyword_encoded}&sortBy=DD&start={start}"
    )


def _normalize_job_url(url: str) -> str:
    return url.split("?", 1)[0].rstrip("/")


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


def scrape_keyword_jobs(page, keyword: str) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    print(f"\n=== Scraping keyword: {keyword} ===")

    for page_index in range(MAX_PAGES_PER_KEYWORD):
        if page_index > 0:
            _sleep_random(PAGE_DELAY_RANGE_SECONDS, "between page requests")

        start = page_index * RESULTS_PER_PAGE
        search_url = _build_search_url(keyword, start)
        print(f"Loading page {page_index + 1}/{MAX_PAGES_PER_KEYWORD}: {search_url}")

        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
        except PlaywrightTimeoutError:
            print(f"Timed out loading search page for keyword '{keyword}', start={start}")
            continue

        try:
            page.wait_for_selector("ul.jobs-search__results-list li", timeout=15000)
        except PlaywrightTimeoutError:
            print(f"No job list found for keyword '{keyword}', page {page_index + 1}")
            break

        cards = page.locator("ul.jobs-search__results-list li")
        card_count = cards.count()
        print(f"Found {card_count} cards on this page")
        if card_count == 0:
            break

        for i in range(card_count):
            card = cards.nth(i)
            title = _safe_text(
                card,
                [
                    "h3.base-search-card__title",
                    "h3",
                ],
            )
            company = _safe_text(
                card,
                [
                    "h4.base-search-card__subtitle",
                    ".base-search-card__subtitle",
                    "h4",
                ],
            )
            location = _safe_text(
                card,
                [
                    ".job-search-card__location",
                    ".job-search-card__location-text",
                ],
            )
            posted_date = _safe_attr(card, ["time"], "datetime") or _safe_text(
                card, ["time", ".job-search-card__listdate", ".job-search-card__listdate--new"]
            )
            raw_url = _safe_attr(card, ["a.base-card__full-link", "a[href*='/jobs/view/']"], "href")
            if not raw_url:
                continue
            job_url = _normalize_job_url(raw_url)
            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "date_posted": posted_date,
                    "job_url": job_url,
                    "keyword": keyword,
                }
            )

        if card_count < RESULTS_PER_PAGE:
            print("Less than 25 results on page; stopping pagination for this keyword.")
            break

    print(f"Collected {len(jobs)} rows for keyword '{keyword}'")
    return jobs


def get_gspread_client():
    creds_path = Path(CREDS_FILE)
    if not creds_path.exists():
        raise FileNotFoundError(f"Google credentials file not found: {CREDS_FILE}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = service_account.Credentials.from_service_account_file(
        creds_path.as_posix(), scopes=scopes
    )
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    return gspread.authorize(credentials)


def get_jobs_worksheet(client):
    if SHEET_ID == "YOUR_GOOGLE_SHEET_ID":
        raise ValueError("Please set SHEET_ID at the top of scraper.py before running.")

    spreadsheet = client.open_by_key(SHEET_ID)
    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=20)
    return worksheet


def ensure_headers(worksheet) -> None:
    existing_values = worksheet.get_all_values()
    if not existing_values:
        worksheet.append_row(HEADERS, value_input_option="RAW")
        print("Header row initialized.")
        return

    first_row = existing_values[0]
    if first_row[: len(HEADERS)] != HEADERS:
        worksheet.update("A1:H1", [HEADERS], value_input_option="RAW")
        print("Header row updated to expected format.")


def get_existing_job_urls(worksheet) -> set[str]:
    rows = worksheet.get_all_values()
    existing_urls = set()
    for row in rows[1:]:
        if len(row) >= 5 and row[4].strip():
            existing_urls.add(_normalize_job_url(row[4].strip()))
    print(f"Loaded {len(existing_urls)} existing URLs from sheet")
    return existing_urls


def main() -> None:
    client = get_gspread_client()
    worksheet = get_jobs_worksheet(client)
    ensure_headers(worksheet)
    existing_urls = get_existing_job_urls(worksheet)

    date_added = date.today().isoformat()
    rows_to_append: List[List[str]] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            viewport={"width": 1366, "height": 768},
        )
        page = context.new_page()

        for idx, keyword in enumerate(KEYWORDS):
            keyword_jobs = scrape_keyword_jobs(page, keyword)

            for job in keyword_jobs:
                job_url = job["job_url"]
                if job_url in existing_urls:
                    continue

                existing_urls.add(job_url)
                rows_to_append.append(
                    [
                        job["title"],
                        job["company"],
                        job["location"],
                        job["date_posted"],
                        job_url,
                        keyword,
                        "",
                        date_added,
                    ]
                )

            if idx < len(KEYWORDS) - 1:
                _sleep_random(KEYWORD_DELAY_RANGE_SECONDS, "between keywords")

        context.close()
        browser.close()

    if rows_to_append:
        worksheet.append_rows(rows_to_append, value_input_option="RAW")
        print(f"Appended {len(rows_to_append)} new jobs to the sheet.")
    else:
        print("No new jobs to append.")


if __name__ == "__main__":
    main()
