import os
import random
import re
import threading
import time
import json
import traceback
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Tuple, Any
from urllib.parse import parse_qs, quote, quote_plus, urljoin, urlparse

import gspread
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from gspread.exceptions import WorksheetNotFound
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

SHEET_ID = os.getenv("SHEET_ID", "").strip()
CREDS_FILE = os.getenv("CREDS_FILE", "service_account.json").strip()
WORKSHEET_NAME = "Jobs"
RUN_SUMMARY_FILE = os.getenv("RUN_SUMMARY_FILE", "scraper_run_summary.json").strip()
SOURCE_NAME = "linkedin"

RESULTS_PER_PAGE = 25


def _parse_positive_int_env(name: str, default: int, minimum: int = 1) -> int:
    raw_value = os.getenv(name, "").strip()
    if not raw_value:
        return default
    try:
        parsed_value = int(raw_value)
    except ValueError:
        print(
            f"[config] Invalid integer for {name}='{raw_value}'. Using default {default}.",
            flush=True,
        )
        return default
    if parsed_value < minimum:
        print(
            f"[config] {name} must be >= {minimum}, got {parsed_value}. Using default {default}.",
            flush=True,
        )
        return default
    return parsed_value


def _parse_range_env(name: str, default: Tuple[float, float]) -> Tuple[float, float]:
    raw_value = os.getenv(name, "").strip()
    if not raw_value:
        return default

    normalized = raw_value.replace(" ", "")
    for separator in (",", "-"):
        if separator in normalized:
            parts = normalized.split(separator)
            if len(parts) != 2:
                break
            try:
                low = float(parts[0])
                high = float(parts[1])
            except ValueError:
                break
            if low <= 0 or high < low:
                break
            return (low, high)

    print(
        f"[config] Invalid range for {name}='{raw_value}'. "
        f"Expected 'min,max'. Using default {default[0]},{default[1]}.",
        flush=True,
    )
    return default


MAX_PAGES_PER_KEYWORD = _parse_positive_int_env("MAX_PAGES_PER_KEYWORD", 3)
MAX_POST_AGE_DAYS = _parse_positive_int_env("MAX_POST_AGE_DAYS", 14)
PAGE_DELAY_RANGE_SECONDS = _parse_range_env("PAGE_DELAY_RANGE_SECONDS", (2.0, 4.0))
KEYWORD_DELAY_RANGE_SECONDS = _parse_range_env("KEYWORD_DELAY_RANGE_SECONDS", (3.0, 6.0))
HEARTBEAT_INTERVAL_SECONDS = _parse_positive_int_env("HEARTBEAT_INTERVAL_SECONDS", 5)
TITLE_FILTER_EXPRESSION = os.getenv("TITLE_FILTER_EXPRESSION", "").strip()
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
SUPPORTED_SOURCES = ["linkedin", "indeed", "ziprecruiter", "usajobs", "adzuna", "jooble"]

USAJOBS_API_KEY = os.getenv("USAJOBS_API_KEY", "").strip()
USAJOBS_USER_AGENT_EMAIL = os.getenv("USAJOBS_USER_AGENT_EMAIL", "").strip()
USAJOBS_RESULTS_PER_PAGE = _parse_positive_int_env("USAJOBS_RESULTS_PER_PAGE", 25)
USAJOBS_MAX_PAGES = _parse_positive_int_env("USAJOBS_MAX_PAGES", 2)

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID", "").strip()
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "").strip()
ADZUNA_COUNTRY = os.getenv("ADZUNA_COUNTRY", "us").strip().lower() or "us"
ADZUNA_RESULTS_PER_PAGE = _parse_positive_int_env("ADZUNA_RESULTS_PER_PAGE", 50)
ADZUNA_MAX_PAGES = _parse_positive_int_env("ADZUNA_MAX_PAGES", 2)

JOOBLE_API_KEY = os.getenv("JOOBLE_API_KEY", "").strip()
JOOBLE_LOCATION = os.getenv("JOOBLE_LOCATION", "").strip()
JOOBLE_MAX_PAGES = _parse_positive_int_env("JOOBLE_MAX_PAGES", 2)

SCRAPE_LOCATION = os.getenv("SCRAPE_LOCATION", "United States").strip() or "United States"
INDEED_ENABLED = os.getenv("INDEED_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
INDEED_MAX_PAGES = _parse_positive_int_env("INDEED_MAX_PAGES", 1)
ZIPRECRUITER_ENABLED = os.getenv("ZIPRECRUITER_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
ZIPRECRUITER_MAX_PAGES = _parse_positive_int_env("ZIPRECRUITER_MAX_PAGES", 1)

HEADERS = [
    "Job URL",
    "Application Date",
    "Job Title",
    "Company",
    "Location",
    "Date Posted",
    "Keyword",
    "Date Added",
    "Source",
    "Source Job ID",
    "Canonical Key",
    "Seen On",
    "First Seen Date",
    "Last Seen Date",
]
JOB_CARD_SELECTOR = "li:has(a.base-card__full-link)"
NO_RESULTS_SELECTOR = (
    ".jobs-search-no-results-banner, "
    ".jobs-search-no-results__image, "
    "h1:has-text('No matching jobs found')"
)

RELATIVE_POSTED_PATTERN = re.compile(
    r"(?P<number>\d+)\+?\s*(?P<unit>hour|day|week|month|year)s?\s*ago", re.IGNORECASE
)


def log(message: str) -> None:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{timestamp}] {message}", flush=True)


def write_run_summary(summary: Dict[str, Any]) -> None:
    summary_path = Path(RUN_SUMMARY_FILE)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log(f"Run summary written to {summary_path.as_posix()}")


def http_get_json(url: str, headers: Dict[str, str] | None = None, timeout: int = 45):
    request = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def http_post_json(url: str, payload: Dict[str, Any], headers: Dict[str, str] | None = None, timeout: int = 45):
    body = json.dumps(payload).encode("utf-8")
    merged_headers = {"Content-Type": "application/json"}
    if headers:
        merged_headers.update(headers)
    request = urllib.request.Request(url, data=body, headers=merged_headers, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


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


def _sleep_random(delay_range: tuple[float, float], reason: str) -> None:
    sleep_seconds = random.uniform(*delay_range)
    log(f"Sleeping {sleep_seconds:.2f}s ({reason})")
    time.sleep(sleep_seconds)


def _build_search_url(keyword: str, start: int) -> str:
    keyword_encoded = quote_plus(keyword)
    max_age_seconds = MAX_POST_AGE_DAYS * 24 * 60 * 60
    return (
        "https://www.linkedin.com/jobs/search/"
        f"?keywords={keyword_encoded}&sortBy=DD&f_TPR=r{max_age_seconds}&start={start}"
    )


def _normalize_job_url(url: str) -> str:
    return url.split("?", 1)[0].rstrip("/")


def _normalize_for_fingerprint(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return " ".join(cleaned.split())


def _build_canonical_key(title: str, company: str, location: str) -> str:
    title_norm = _normalize_for_fingerprint(title)
    company_norm = _normalize_for_fingerprint(company)
    location_norm = _normalize_for_fingerprint(location)
    return f"{title_norm}|{company_norm}|{location_norm}"


def _extract_linkedin_job_id(job_url: str) -> str:
    # Supports URLs like /jobs/view/<slug>-1234567890 and /jobs/view/1234567890
    match = re.search(r"/jobs/view/(?:[^/?#]*-)?(\d+)", job_url)
    return match.group(1) if match else ""


def _column_letter(column_index_1_based: int) -> str:
    result = ""
    value = column_index_1_based
    while value > 0:
        value, remainder = divmod(value - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _join_sources(existing_seen_on: str, new_source: str) -> str:
    existing_parts = [part.strip() for part in existing_seen_on.split(",") if part.strip()]
    existing_set = {part.lower() for part in existing_parts}
    if new_source and new_source.lower() not in existing_set:
        existing_parts.append(new_source)
    return ", ".join(existing_parts)


def _normalized_text_for_term_matching(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _term_matches_title(term: str, title_lower: str, title_compact: str) -> bool:
    term_lower = term.lower().strip()
    if not term_lower:
        return False
    if term_lower in title_lower:
        return True
    return _normalized_text_for_term_matching(term_lower) in title_compact


def _tokenize_filter_expression(expression: str) -> List[str]:
    tokens: List[str] = []
    i = 0
    while i < len(expression):
        ch = expression[i]
        if ch.isspace():
            i += 1
            continue
        if ch in ("(", ")", "&", "|"):
            tokens.append(ch)
            i += 1
            continue
        if ch in ("'", '"'):
            quote = ch
            i += 1
            start = i
            while i < len(expression) and expression[i] != quote:
                i += 1
            if i >= len(expression):
                raise ValueError("Unclosed quote in TITLE_FILTER_EXPRESSION")
            tokens.append(expression[start:i].strip())
            i += 1
            continue

        start = i
        while i < len(expression) and (not expression[i].isspace()) and expression[i] not in "()&|":
            i += 1
        tokens.append(expression[start:i].strip())

    return [t for t in tokens if t]


class _ExprParser:
    def __init__(self, tokens: List[str]):
        self.tokens = tokens
        self.pos = 0

    def _peek(self) -> str:
        if self.pos >= len(self.tokens):
            return ""
        return self.tokens[self.pos]

    def _consume(self, expected: str | None = None) -> str:
        token = self._peek()
        if not token:
            raise ValueError("Unexpected end of TITLE_FILTER_EXPRESSION")
        if expected is not None and token != expected:
            raise ValueError(
                f"Expected '{expected}' in TITLE_FILTER_EXPRESSION, found '{token}'"
            )
        self.pos += 1
        return token

    def parse(self):
        node = self._parse_or()
        if self._peek():
            raise ValueError(
                f"Unexpected token '{self._peek()}' in TITLE_FILTER_EXPRESSION"
            )
        return node

    def _parse_or(self):
        node = self._parse_and()
        while self._peek() == "|":
            self._consume("|")
            right = self._parse_and()
            node = ("or", node, right)
        return node

    def _parse_and(self):
        node = self._parse_primary()
        while self._peek() == "&":
            self._consume("&")
            right = self._parse_primary()
            node = ("and", node, right)
        return node

    def _parse_primary(self):
        token = self._peek()
        if token == "(":
            self._consume("(")
            node = self._parse_or()
            self._consume(")")
            return node
        if token in ("", ")", "&", "|"):
            raise ValueError(
                f"Invalid token '{token or '<end>'}' in TITLE_FILTER_EXPRESSION"
            )
        term = self._consume()
        return ("term", term)


def _evaluate_filter_ast(node, title_lower: str, title_compact: str) -> bool:
    node_type = node[0]
    if node_type == "term":
        return _term_matches_title(node[1], title_lower, title_compact)
    if node_type == "and":
        return _evaluate_filter_ast(node[1], title_lower, title_compact) and _evaluate_filter_ast(
            node[2], title_lower, title_compact
        )
    if node_type == "or":
        return _evaluate_filter_ast(node[1], title_lower, title_compact) or _evaluate_filter_ast(
            node[2], title_lower, title_compact
        )
    raise ValueError(f"Unknown expression node type: {node_type}")


@lru_cache(maxsize=1)
def _get_title_filter_ast():
    if not TITLE_FILTER_EXPRESSION:
        return None
    tokens = _tokenize_filter_expression(TITLE_FILTER_EXPRESSION)
    if not tokens:
        raise ValueError("TITLE_FILTER_EXPRESSION is set but empty after tokenization.")
    parser = _ExprParser(tokens)
    return parser.parse()


def _title_matches_term_filters(title: str) -> bool:
    title_lower = title.lower()
    title_compact = _normalized_text_for_term_matching(title)
    expression_ast = _get_title_filter_ast()
    if expression_ast is None:
        return True
    return _evaluate_filter_ast(expression_ast, title_lower, title_compact)


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


def _is_recent_enough(posted_value: str, now_utc: datetime) -> tuple[bool, bool]:
    posted_dt = _parse_posted_datetime(posted_value, now_utc)
    if not posted_dt:
        # Keep rows with unknown posted text because LinkedIn search URL already
        # applies the f_TPR recency filter.
        return True, True

    age = now_utc - posted_dt
    return timedelta(0) <= age <= timedelta(days=MAX_POST_AGE_DAYS), False


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


def scrape_keyword_jobs(page, keyword: str, now_utc: datetime) -> tuple[List[Dict[str, str]], Dict[str, Any]]:
    jobs: List[Dict[str, str]] = []
    skipped_old = 0
    kept_unknown_date = 0
    skipped_missing_url = 0
    skipped_term_filter = 0
    no_results_pages = 0
    page_timeouts = 0
    pages_scanned = 0
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
            page_timeouts += 1
            continue

        card_wait_succeeded = False
        no_results_found = False
        for attempt in range(2):
            try:
                log(f"Waiting for job cards on page {page_index + 1} for keyword '{keyword}'")
                run_with_heartbeat(
                    action=f"waiting for job cards on page {page_index + 1} for keyword '{keyword}'",
                    func=page.wait_for_selector,
                    selector=JOB_CARD_SELECTOR,
                    timeout=15000,
                )
                card_wait_succeeded = True
                break
            except PlaywrightTimeoutError:
                if page.locator(NO_RESULTS_SELECTOR).count() > 0:
                    log(f"No matching jobs found for keyword '{keyword}', page {page_index + 1}")
                    no_results_found = True
                    break
                if attempt == 0:
                    log(
                        f"Card wait timed out for keyword '{keyword}', page {page_index + 1}. "
                        "Reloading once."
                    )
                    try:
                        run_with_heartbeat(
                            action=(
                                f"reloading page {page_index + 1} "
                                f"for keyword '{keyword}' after card wait timeout"
                            ),
                            func=page.reload,
                            wait_until="domcontentloaded",
                            timeout=60000,
                        )
                    except PlaywrightTimeoutError:
                        break

        if no_results_found:
            no_results_pages += 1
            break
        if not card_wait_succeeded:
            log(f"No job list found for keyword '{keyword}', page {page_index + 1}")
            page_timeouts += 1
            continue

        cards = page.locator(JOB_CARD_SELECTOR)
        card_count = cards.count()
        pages_scanned += 1
        log(f"Found {card_count} cards on page {page_index + 1}")
        if card_count == 0:
            break

        page_seen_urls = set()
        for i in range(card_count):
            if i > 0 and i % 10 == 0:
                log(
                    f"Parsed {i}/{card_count} cards on page {page_index + 1} for keyword '{keyword}'"
                )

            card = cards.nth(i)
            title = _safe_text(card, ["h3.base-search-card__title", "h3"])
            if not _title_matches_term_filters(title):
                skipped_term_filter += 1
                continue
            company = _safe_text(
                card,
                ["h4.base-search-card__subtitle", ".base-search-card__subtitle", "h4"],
            )
            location = _safe_text(card, [".job-search-card__location", ".job-search-card__location-text"])
            posted_date = _safe_attr(card, ["time"], "datetime") or _safe_text(
                card, ["time", ".job-search-card__listdate", ".job-search-card__listdate--new"]
            )
            is_recent, is_unknown_date = _is_recent_enough(posted_date, now_utc)
            if is_unknown_date:
                kept_unknown_date += 1
            if not is_recent:
                skipped_old += 1
                continue

            raw_url = _safe_attr(card, ["a.base-card__full-link", "a[href*='/jobs/view/']"], "href")
            if not raw_url:
                skipped_missing_url += 1
                continue

            normalized_job_url = _normalize_job_url(raw_url)
            if normalized_job_url in page_seen_urls:
                continue
            page_seen_urls.add(normalized_job_url)

            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "date_posted": posted_date,
                    "job_url": normalized_job_url,
                    "keyword": keyword,
                    "source": SOURCE_NAME,
                    "source_job_id": _extract_linkedin_job_id(normalized_job_url),
                    "canonical_key": _build_canonical_key(title, company, location),
                }
            )

        if card_count < RESULTS_PER_PAGE:
            log("Less than 25 results on page; stopping pagination for this keyword.")
            break

    log(
        f"Collected {len(jobs)} recent rows for '{keyword}'. "
        f"Skipped old: {skipped_old}, kept unknown-date: {kept_unknown_date}, "
        f"missing URL: {skipped_missing_url}, title-term-filter: {skipped_term_filter}"
    )
    return jobs, {
        "keyword": keyword,
        "source": SOURCE_NAME,
        "pages_scanned": pages_scanned,
        "page_timeouts": page_timeouts,
        "no_results_pages": no_results_pages,
        "rows_collected": len(jobs),
        "skipped_old": skipped_old,
        "kept_unknown_date": kept_unknown_date,
        "missing_url": skipped_missing_url,
        "skipped_term_filter": skipped_term_filter,
    }


def _page_is_likely_bot_blocked(page) -> bool:
    try:
        body_text = page.locator("body").inner_text(timeout=2000).lower()
    except Exception:
        return False
    markers = [
        "captcha",
        "verify you are human",
        "security check",
        "access denied",
        "unusual traffic",
    ]
    return any(marker in body_text for marker in markers)


def _jsonld_collect_job_postings(node, collected: List[Dict[str, Any]]) -> None:
    if isinstance(node, list):
        for item in node:
            _jsonld_collect_job_postings(item, collected)
        return

    if not isinstance(node, dict):
        return

    raw_type = node.get("@type")
    type_values = []
    if isinstance(raw_type, str):
        type_values = [raw_type]
    elif isinstance(raw_type, list):
        type_values = [str(value) for value in raw_type]

    if any(value.lower() == "jobposting" for value in type_values):
        collected.append(node)

    for value in node.values():
        if isinstance(value, (dict, list)):
            _jsonld_collect_job_postings(value, collected)


def _extract_location_from_jobposting(job_posting: Dict[str, Any]) -> str:
    raw_location = job_posting.get("jobLocation")
    if isinstance(raw_location, list) and raw_location:
        raw_location = raw_location[0]

    if isinstance(raw_location, dict):
        address = raw_location.get("address", raw_location)
        if isinstance(address, dict):
            parts = [
                str(address.get("addressLocality") or "").strip(),
                str(address.get("addressRegion") or "").strip(),
                str(address.get("addressCountry") or "").strip(),
            ]
            return ", ".join(part for part in parts if part)
        if isinstance(address, str):
            return address.strip()

    if isinstance(raw_location, str):
        return raw_location.strip()
    return ""


def _extract_source_job_id_from_jsonld(job_posting: Dict[str, Any]) -> str:
    identifier = job_posting.get("identifier")
    if isinstance(identifier, dict):
        return str(identifier.get("value") or identifier.get("@value") or "").strip()
    if isinstance(identifier, list) and identifier:
        first = identifier[0]
        if isinstance(first, dict):
            return str(first.get("value") or first.get("@value") or "").strip()
        return str(first).strip()
    if identifier:
        return str(identifier).strip()
    return ""


def _extract_job_postings_from_jsonld(page, base_url: str, source: str, keyword: str) -> List[Dict[str, str]]:
    jobs: List[Dict[str, str]] = []
    seen_urls = set()
    scripts = page.locator("script[type='application/ld+json']")
    script_count = min(scripts.count(), 40)
    all_job_postings: List[Dict[str, Any]] = []

    for index in range(script_count):
        script = scripts.nth(index)
        try:
            raw_text = script.inner_text(timeout=800).strip()
            if not raw_text:
                continue
            json_payload = json.loads(raw_text)
            _jsonld_collect_job_postings(json_payload, all_job_postings)
        except Exception:
            continue

    for job_posting in all_job_postings:
        title = str(job_posting.get("title") or "").strip()
        job_url_raw = str(job_posting.get("url") or "").strip()
        if not title or not job_url_raw:
            continue
        job_url = _normalize_job_url(urljoin(base_url, job_url_raw))
        if job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        hiring_org = job_posting.get("hiringOrganization")
        if isinstance(hiring_org, dict):
            company = str(hiring_org.get("name") or "").strip()
        else:
            company = str(hiring_org or "").strip()

        source_job_id = _extract_source_job_id_from_jsonld(job_posting)
        jobs.append(
            {
                "title": title,
                "company": company,
                "location": _extract_location_from_jobposting(job_posting),
                "date_posted": str(job_posting.get("datePosted") or "").strip(),
                "job_url": job_url,
                "keyword": keyword,
                "source": source,
                "source_job_id": source_job_id,
                "canonical_key": _build_canonical_key(title, company, _extract_location_from_jobposting(job_posting)),
            }
        )
    return jobs


def scrape_indeed_keyword_jobs(page, keyword: str, now_utc: datetime) -> tuple[List[Dict[str, str]], Dict[str, Any]]:
    jobs: List[Dict[str, str]] = []
    seen_urls = set()
    skipped_old = 0
    kept_unknown_date = 0
    skipped_missing_url = 0
    skipped_term_filter = 0
    page_timeouts = 0
    pages_scanned = 0
    no_results_pages = 0

    for page_index in range(INDEED_MAX_PAGES):
        if page_index > 0:
            _sleep_random(PAGE_DELAY_RANGE_SECONDS, "between indeed pages")

        start = page_index * 10
        url = (
            f"https://www.indeed.com/jobs?q={quote_plus(keyword)}&l={quote_plus(SCRAPE_LOCATION)}"
            f"&sort=date&fromage={MAX_POST_AGE_DAYS}&start={start}"
        )
        log(f"Loading Indeed page {page_index + 1}/{INDEED_MAX_PAGES}: {url}")
        try:
            run_with_heartbeat(
                action=f"loading indeed page {page_index + 1} for '{keyword}'",
                func=page.goto,
                url=url,
                wait_until="domcontentloaded",
                timeout=60000,
            )
        except PlaywrightTimeoutError:
            page_timeouts += 1
            continue

        if _page_is_likely_bot_blocked(page):
            log(f"Indeed appears blocked for keyword '{keyword}'")
            page_timeouts += 1
            break

        pages_scanned += 1
        page_jobs = _extract_job_postings_from_jsonld(page, "https://www.indeed.com", "indeed", keyword)

        if not page_jobs:
            cards = page.locator("div.job_seen_beacon, div[data-jk]")
            card_count = min(cards.count(), 120)
            if card_count == 0:
                no_results_pages += 1
                break

            for i in range(card_count):
                card = cards.nth(i)
                title = _safe_text(card, ["h2.jobTitle span[title]", "h2.jobTitle span", "a[data-jk] span"])
                if not title:
                    continue
                if not _title_matches_term_filters(title):
                    skipped_term_filter += 1
                    continue
                company = _safe_text(card, ["span[data-testid='company-name']", "span.companyName"])
                location = _safe_text(card, ["div[data-testid='text-location']", "div.companyLocation"])
                posted_date = _safe_text(card, ["span.date", "[data-testid='myJobsStateDate']"])
                is_recent, is_unknown_date = _is_recent_enough(posted_date, now_utc)
                if is_unknown_date:
                    kept_unknown_date += 1
                if not is_recent:
                    skipped_old += 1
                    continue

                link = card.locator("h2.jobTitle a, a[data-jk]").first
                href = ""
                try:
                    href = link.get_attribute("href", timeout=1000) or ""
                except Exception:
                    href = ""
                if not href:
                    skipped_missing_url += 1
                    continue
                job_url = _normalize_job_url(urljoin("https://www.indeed.com", href))
                if job_url in seen_urls:
                    continue
                seen_urls.add(job_url)

                data_jk = ""
                try:
                    data_jk = (card.get_attribute("data-jk", timeout=300) or "").strip()
                except Exception:
                    data_jk = ""
                if not data_jk:
                    data_jk = (parse_qs(urlparse(job_url).query).get("jk", [""])[0] or "").strip()

                jobs.append(
                    {
                        "title": title,
                        "company": company,
                        "location": location,
                        "date_posted": posted_date,
                        "job_url": job_url,
                        "keyword": keyword,
                        "source": "indeed",
                        "source_job_id": data_jk,
                        "canonical_key": _build_canonical_key(title, company, location),
                    }
                )
        else:
            for item in page_jobs:
                title = item["title"]
                if not _title_matches_term_filters(title):
                    skipped_term_filter += 1
                    continue
                posted_date = item.get("date_posted", "")
                is_recent, is_unknown_date = _is_recent_enough(posted_date, now_utc)
                if is_unknown_date:
                    kept_unknown_date += 1
                if not is_recent:
                    skipped_old += 1
                    continue
                job_url = item.get("job_url", "")
                if not job_url:
                    skipped_missing_url += 1
                    continue
                if job_url in seen_urls:
                    continue
                seen_urls.add(job_url)
                jobs.append(item)

    return jobs, {
        "keyword": keyword,
        "source": "indeed",
        "pages_scanned": pages_scanned,
        "page_timeouts": page_timeouts,
        "no_results_pages": no_results_pages,
        "rows_collected": len(jobs),
        "skipped_old": skipped_old,
        "kept_unknown_date": kept_unknown_date,
        "missing_url": skipped_missing_url,
        "skipped_term_filter": skipped_term_filter,
    }


def scrape_ziprecruiter_keyword_jobs(page, keyword: str, now_utc: datetime) -> tuple[List[Dict[str, str]], Dict[str, Any]]:
    jobs: List[Dict[str, str]] = []
    seen_urls = set()
    skipped_old = 0
    kept_unknown_date = 0
    skipped_missing_url = 0
    skipped_term_filter = 0
    page_timeouts = 0
    pages_scanned = 0
    no_results_pages = 0

    for page_number in range(1, ZIPRECRUITER_MAX_PAGES + 1):
        if page_number > 1:
            _sleep_random(PAGE_DELAY_RANGE_SECONDS, "between ziprecruiter pages")

        url = (
            "https://www.ziprecruiter.com/jobs-search"
            f"?search={quote_plus(keyword)}&location={quote_plus(SCRAPE_LOCATION)}"
            f"&days={MAX_POST_AGE_DAYS}&page={page_number}"
        )
        log(f"Loading ZipRecruiter page {page_number}/{ZIPRECRUITER_MAX_PAGES}: {url}")
        try:
            run_with_heartbeat(
                action=f"loading ziprecruiter page {page_number} for '{keyword}'",
                func=page.goto,
                url=url,
                wait_until="domcontentloaded",
                timeout=60000,
            )
        except PlaywrightTimeoutError:
            page_timeouts += 1
            continue

        if _page_is_likely_bot_blocked(page):
            log(f"ZipRecruiter appears blocked for keyword '{keyword}'")
            page_timeouts += 1
            break

        pages_scanned += 1
        page_jobs = _extract_job_postings_from_jsonld(
            page, "https://www.ziprecruiter.com", "ziprecruiter", keyword
        )

        if not page_jobs:
            links = page.locator("a[href*='/jobs/']")
            link_count = min(links.count(), 120)
            if link_count == 0:
                no_results_pages += 1
                break

            for i in range(link_count):
                link = links.nth(i)
                try:
                    href = (link.get_attribute("href", timeout=800) or "").strip()
                except Exception:
                    href = ""
                if not href:
                    continue
                job_url = _normalize_job_url(urljoin("https://www.ziprecruiter.com", href))
                if job_url in seen_urls:
                    continue
                title = ""
                try:
                    title = (link.inner_text(timeout=800) or "").strip()
                except Exception:
                    title = ""
                if not title:
                    try:
                        title = (link.get_attribute("title", timeout=300) or "").strip()
                    except Exception:
                        title = ""
                if not title:
                    continue
                if not _title_matches_term_filters(title):
                    skipped_term_filter += 1
                    continue

                seen_urls.add(job_url)
                jobs.append(
                    {
                        "title": title,
                        "company": "",
                        "location": "",
                        "date_posted": "",
                        "job_url": job_url,
                        "keyword": keyword,
                        "source": "ziprecruiter",
                        "source_job_id": "",
                        "canonical_key": _build_canonical_key(title, "", ""),
                    }
                )
                kept_unknown_date += 1
        else:
            for item in page_jobs:
                title = item["title"]
                if not _title_matches_term_filters(title):
                    skipped_term_filter += 1
                    continue
                posted_date = item.get("date_posted", "")
                is_recent, is_unknown_date = _is_recent_enough(posted_date, now_utc)
                if is_unknown_date:
                    kept_unknown_date += 1
                if not is_recent:
                    skipped_old += 1
                    continue
                job_url = item.get("job_url", "")
                if not job_url:
                    skipped_missing_url += 1
                    continue
                if job_url in seen_urls:
                    continue
                seen_urls.add(job_url)
                jobs.append(item)

    return jobs, {
        "keyword": keyword,
        "source": "ziprecruiter",
        "pages_scanned": pages_scanned,
        "page_timeouts": page_timeouts,
        "no_results_pages": no_results_pages,
        "rows_collected": len(jobs),
        "skipped_old": skipped_old,
        "kept_unknown_date": kept_unknown_date,
        "missing_url": skipped_missing_url,
        "skipped_term_filter": skipped_term_filter,
    }


def scrape_usajobs_keyword_jobs(keyword: str, now_utc: datetime) -> tuple[List[Dict[str, str]], Dict[str, Any]]:
    jobs: List[Dict[str, str]] = []
    skipped_old = 0
    kept_unknown_date = 0
    skipped_missing_url = 0
    skipped_term_filter = 0
    page_timeouts = 0
    pages_scanned = 0

    if not USAJOBS_API_KEY or not USAJOBS_USER_AGENT_EMAIL:
        log("Skipping USAJobs: missing USAJOBS_API_KEY or USAJOBS_USER_AGENT_EMAIL.")
        return jobs, {
            "keyword": keyword,
            "source": "usajobs",
            "pages_scanned": 0,
            "page_timeouts": 0,
            "no_results_pages": 0,
            "rows_collected": 0,
            "skipped_old": 0,
            "kept_unknown_date": 0,
            "missing_url": 0,
            "skipped_term_filter": 0,
        }

    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": USAJOBS_USER_AGENT_EMAIL,
        "Authorization-Key": USAJOBS_API_KEY,
    }
    seen_urls = set()
    no_results_pages = 0

    for page_number in range(1, USAJOBS_MAX_PAGES + 1):
        query = (
            f"https://data.usajobs.gov/api/search?Keyword={quote(keyword)}"
            f"&Page={page_number}&ResultsPerPage={USAJOBS_RESULTS_PER_PAGE}"
            "&SortField=OpenDate&SortDirection=Desc"
        )
        try:
            payload = http_get_json(query, headers=headers, timeout=45)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            log(f"USAJobs fetch failed for keyword '{keyword}', page {page_number}: {exc}")
            page_timeouts += 1
            continue

        pages_scanned += 1
        items = (
            payload.get("SearchResult", {}).get("SearchResultItems", [])
            if isinstance(payload, dict)
            else []
        )
        if not items:
            no_results_pages += 1
            break

        for item in items:
            descriptor = item.get("MatchedObjectDescriptor", {}) if isinstance(item, dict) else {}
            title = (descriptor.get("PositionTitle") or "").strip()
            if not _title_matches_term_filters(title):
                skipped_term_filter += 1
                continue

            company = (descriptor.get("OrganizationName") or "USAJobs").strip()
            location = (descriptor.get("PositionLocationDisplay") or "").strip()
            posted_date = (descriptor.get("PublicationStartDate") or "").strip()
            is_recent, is_unknown_date = _is_recent_enough(posted_date, now_utc)
            if is_unknown_date:
                kept_unknown_date += 1
            if not is_recent:
                skipped_old += 1
                continue

            job_url = (descriptor.get("PositionURI") or "").strip()
            if not job_url:
                skipped_missing_url += 1
                continue
            normalized_job_url = _normalize_job_url(job_url)
            if normalized_job_url in seen_urls:
                continue
            seen_urls.add(normalized_job_url)

            position_ids = descriptor.get("PositionID") if isinstance(descriptor.get("PositionID"), list) else []
            source_job_id = str(position_ids[0]).strip() if position_ids else ""
            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "date_posted": posted_date,
                    "job_url": normalized_job_url,
                    "keyword": keyword,
                    "source": "usajobs",
                    "source_job_id": source_job_id,
                    "canonical_key": _build_canonical_key(title, company, location),
                }
            )

    return jobs, {
        "keyword": keyword,
        "source": "usajobs",
        "pages_scanned": pages_scanned,
        "page_timeouts": page_timeouts,
        "no_results_pages": no_results_pages,
        "rows_collected": len(jobs),
        "skipped_old": skipped_old,
        "kept_unknown_date": kept_unknown_date,
        "missing_url": skipped_missing_url,
        "skipped_term_filter": skipped_term_filter,
    }


def scrape_adzuna_keyword_jobs(keyword: str, now_utc: datetime) -> tuple[List[Dict[str, str]], Dict[str, Any]]:
    jobs: List[Dict[str, str]] = []
    skipped_old = 0
    kept_unknown_date = 0
    skipped_missing_url = 0
    skipped_term_filter = 0
    page_timeouts = 0
    pages_scanned = 0
    no_results_pages = 0

    if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
        log("Skipping Adzuna: missing ADZUNA_APP_ID or ADZUNA_APP_KEY.")
        return jobs, {
            "keyword": keyword,
            "source": "adzuna",
            "pages_scanned": 0,
            "page_timeouts": 0,
            "no_results_pages": 0,
            "rows_collected": 0,
            "skipped_old": 0,
            "kept_unknown_date": 0,
            "missing_url": 0,
            "skipped_term_filter": 0,
        }

    seen_urls = set()
    for page_number in range(1, ADZUNA_MAX_PAGES + 1):
        query = (
            f"https://api.adzuna.com/v1/api/jobs/{quote(ADZUNA_COUNTRY)}/search/{page_number}"
            f"?app_id={quote(ADZUNA_APP_ID)}&app_key={quote(ADZUNA_APP_KEY)}"
            f"&results_per_page={ADZUNA_RESULTS_PER_PAGE}&sort_by=date&what={quote(keyword)}"
        )
        try:
            payload = http_get_json(query, timeout=45)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            log(f"Adzuna fetch failed for keyword '{keyword}', page {page_number}: {exc}")
            page_timeouts += 1
            continue

        pages_scanned += 1
        results = payload.get("results", []) if isinstance(payload, dict) else []
        if not results:
            no_results_pages += 1
            break

        for item in results:
            title = (item.get("title") or "").strip()
            if not _title_matches_term_filters(title):
                skipped_term_filter += 1
                continue

            company = ""
            raw_company = item.get("company")
            if isinstance(raw_company, dict):
                company = (raw_company.get("display_name") or "").strip()
            company = company or "Unknown Company"

            location = ""
            raw_location = item.get("location")
            if isinstance(raw_location, dict):
                location = (raw_location.get("display_name") or "").strip()

            posted_date = (item.get("created") or "").strip()
            is_recent, is_unknown_date = _is_recent_enough(posted_date, now_utc)
            if is_unknown_date:
                kept_unknown_date += 1
            if not is_recent:
                skipped_old += 1
                continue

            job_url = (item.get("redirect_url") or "").strip()
            if not job_url:
                skipped_missing_url += 1
                continue
            normalized_job_url = _normalize_job_url(job_url)
            if normalized_job_url in seen_urls:
                continue
            seen_urls.add(normalized_job_url)

            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "date_posted": posted_date,
                    "job_url": normalized_job_url,
                    "keyword": keyword,
                    "source": "adzuna",
                    "source_job_id": str(item.get("id") or "").strip(),
                    "canonical_key": _build_canonical_key(title, company, location),
                }
            )

    return jobs, {
        "keyword": keyword,
        "source": "adzuna",
        "pages_scanned": pages_scanned,
        "page_timeouts": page_timeouts,
        "no_results_pages": no_results_pages,
        "rows_collected": len(jobs),
        "skipped_old": skipped_old,
        "kept_unknown_date": kept_unknown_date,
        "missing_url": skipped_missing_url,
        "skipped_term_filter": skipped_term_filter,
    }


def scrape_jooble_keyword_jobs(keyword: str, now_utc: datetime) -> tuple[List[Dict[str, str]], Dict[str, Any]]:
    jobs: List[Dict[str, str]] = []
    skipped_old = 0
    kept_unknown_date = 0
    skipped_missing_url = 0
    skipped_term_filter = 0
    page_timeouts = 0
    pages_scanned = 0
    no_results_pages = 0

    if not JOOBLE_API_KEY:
        log("Skipping Jooble: missing JOOBLE_API_KEY.")
        return jobs, {
            "keyword": keyword,
            "source": "jooble",
            "pages_scanned": 0,
            "page_timeouts": 0,
            "no_results_pages": 0,
            "rows_collected": 0,
            "skipped_old": 0,
            "kept_unknown_date": 0,
            "missing_url": 0,
            "skipped_term_filter": 0,
        }

    endpoint = f"https://jooble.org/api/{quote(JOOBLE_API_KEY)}"
    seen_urls = set()

    for page_number in range(1, JOOBLE_MAX_PAGES + 1):
        payload = {"keywords": keyword, "location": JOOBLE_LOCATION, "page": page_number}
        try:
            response = http_post_json(endpoint, payload=payload, timeout=45)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            log(f"Jooble fetch failed for keyword '{keyword}', page {page_number}: {exc}")
            page_timeouts += 1
            continue

        pages_scanned += 1
        results = response.get("jobs", []) if isinstance(response, dict) else []
        if not results:
            no_results_pages += 1
            break

        for item in results:
            title = (item.get("title") or "").strip()
            if not _title_matches_term_filters(title):
                skipped_term_filter += 1
                continue

            company = (item.get("company") or item.get("companyName") or "").strip() or "Unknown Company"
            location = (item.get("location") or "").strip()
            posted_date = (
                item.get("updated")
                or item.get("updatedAt")
                or item.get("created")
                or item.get("posted")
                or ""
            )
            posted_date = str(posted_date).strip()
            is_recent, is_unknown_date = _is_recent_enough(posted_date, now_utc)
            if is_unknown_date:
                kept_unknown_date += 1
            if not is_recent:
                skipped_old += 1
                continue

            job_url = (item.get("link") or item.get("url") or "").strip()
            if not job_url:
                skipped_missing_url += 1
                continue
            normalized_job_url = _normalize_job_url(job_url)
            if normalized_job_url in seen_urls:
                continue
            seen_urls.add(normalized_job_url)

            jobs.append(
                {
                    "title": title,
                    "company": company,
                    "location": location,
                    "date_posted": posted_date,
                    "job_url": normalized_job_url,
                    "keyword": keyword,
                    "source": "jooble",
                    "source_job_id": str(item.get("id") or item.get("jobId") or "").strip(),
                    "canonical_key": _build_canonical_key(title, company, location),
                }
            )

    return jobs, {
        "keyword": keyword,
        "source": "jooble",
        "pages_scanned": pages_scanned,
        "page_timeouts": page_timeouts,
        "no_results_pages": no_results_pages,
        "rows_collected": len(jobs),
        "skipped_old": skipped_old,
        "kept_unknown_date": kept_unknown_date,
        "missing_url": skipped_missing_url,
        "skipped_term_filter": skipped_term_filter,
    }


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
        last_col = _column_letter(len(HEADERS))
        worksheet.update(range_name=f"A1:{last_col}1", values=[HEADERS], value_input_option="RAW")
        log(f"Header row updated for tab '{worksheet.title}'.")


def _normalize_existing_row_for_schema(row: List[str]) -> List[str]:
    normalized = list(row[: len(HEADERS)]) + [""] * max(0, len(HEADERS) - len(row))

    # Backward compatibility for historical layouts:
    # - current schema keeps Job URL in column A
    # - some older rows may still have it in column E
    if not normalized[0].strip() and len(row) >= 5 and "/jobs/view/" in row[4]:
        normalized[0] = row[4].strip()

    normalized[0] = _normalize_job_url(normalized[0]) if normalized[0].strip() else ""
    normalized[8] = normalized[8].strip() or SOURCE_NAME
    normalized[9] = normalized[9].strip() or _extract_linkedin_job_id(normalized[0])

    canonical_key = normalized[10].strip()
    if not canonical_key:
        canonical_key = _build_canonical_key(normalized[2], normalized[3], normalized[4])
    normalized[10] = canonical_key

    normalized[11] = _join_sources(normalized[11], normalized[8])
    normalized[12] = normalized[12].strip() or normalized[7].strip()
    normalized[13] = normalized[13].strip() or normalized[12].strip() or normalized[7].strip()

    return normalized


def _join_unique_values(existing_value: str, new_value: str, separator: str = " | ") -> str:
    existing_parts = [part.strip() for part in existing_value.split(separator) if part.strip()]
    existing_set = {part.lower() for part in existing_parts}
    clean_new = new_value.strip()
    if clean_new and clean_new.lower() not in existing_set:
        existing_parts.append(clean_new)
    return separator.join(existing_parts)


def load_existing_row_index(worksheet):
    log(f"Loading existing rows from tab '{worksheet.title}' for deduplication and merge indexing.")
    rows = worksheet.get_all_values()
    rows_by_number: Dict[int, List[str]] = {}
    by_job_url: Dict[str, int] = {}
    by_canonical_key: Dict[str, int] = {}

    for row_number, raw_row in enumerate(rows[1:], start=2):
        normalized_row = _normalize_existing_row_for_schema(raw_row)
        rows_by_number[row_number] = normalized_row

        job_url = normalized_row[0].strip()
        if job_url and job_url not in by_job_url:
            by_job_url[job_url] = row_number

        canonical_key = normalized_row[10].strip()
        if canonical_key and canonical_key != "||" and canonical_key not in by_canonical_key:
            by_canonical_key[canonical_key] = row_number

    log(
        f"Loaded {len(rows_by_number)} existing rows, "
        f"{len(by_job_url)} URL keys, {len(by_canonical_key)} canonical keys."
    )
    return rows_by_number, by_job_url, by_canonical_key


def merge_job_into_existing_row(existing_row: List[str], job: Dict[str, str], keyword: str, today: str) -> List[str]:
    row = list(existing_row[: len(HEADERS)]) + [""] * max(0, len(HEADERS) - len(existing_row))

    if not row[0].strip():
        row[0] = job["job_url"]
    if not row[2].strip():
        row[2] = job["title"]
    if not row[3].strip():
        row[3] = job["company"]
    if not row[4].strip():
        row[4] = job["location"]
    if not row[5].strip():
        row[5] = job["date_posted"]

    row[6] = _join_unique_values(row[6], keyword)
    row[7] = row[7].strip() or today
    row[8] = row[8].strip() or job.get("source", SOURCE_NAME)
    row[9] = row[9].strip() or job.get("source_job_id", "")
    row[10] = row[10].strip() or job.get("canonical_key", "")
    row[11] = _join_sources(row[11], job.get("source", SOURCE_NAME))
    row[12] = row[12].strip() or row[7]
    row[13] = today

    return row


def write_row_updates(worksheet, row_updates: Dict[int, List[str]]) -> None:
    if not row_updates:
        return

    last_col = _column_letter(len(HEADERS))
    data = []
    for row_number in sorted(row_updates.keys()):
        normalized_row = row_updates[row_number][: len(HEADERS)] + [""] * max(
            0, len(HEADERS) - len(row_updates[row_number])
        )
        data.append(
            {
                "range": f"A{row_number}:{last_col}{row_number}",
                "values": [normalized_row],
            }
        )

    worksheet.batch_update(data, value_input_option="RAW")
    log(f"Updated {len(data)} existing rows in tab '{worksheet.title}'.")


def build_source_summary(keyword_summaries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    aggregated: Dict[str, Dict[str, Any]] = {}
    tracked_fields = [
        "rows_collected",
        "new_rows_appended",
        "duplicates_skipped",
        "merged_existing_rows",
        "merged_by_job_url",
        "merged_by_canonical_key",
        "canonical_duplicates_in_run",
        "canonical_duplicates_against_existing",
        "skipped_old",
        "skipped_term_filter",
        "page_timeouts",
    ]

    for item in keyword_summaries:
        source_name = str(item.get("source", "unknown"))
        if source_name not in aggregated:
            aggregated[source_name] = {"source": source_name, "batches": 0}
            for field in tracked_fields:
                aggregated[source_name][field] = 0

        aggregated[source_name]["batches"] += 1
        for field in tracked_fields:
            aggregated[source_name][field] += int(item.get(field, 0))

    return [aggregated[key] for key in sorted(aggregated.keys())]


def get_next_empty_row(worksheet) -> int:
    # Column A always contains Job URL for populated rows.
    # Using col_values keeps writes contiguous within the existing table block.
    column_a_values = worksheet.col_values(1)
    return max(len(column_a_values) + 1, 2)


def write_rows_to_next_empty_range(worksheet, rows: List[List[str]]) -> None:
    if not rows:
        return

    start_row = get_next_empty_row(worksheet)
    end_row = start_row + len(rows) - 1
    if end_row > worksheet.row_count:
        worksheet.add_rows(end_row - worksheet.row_count)
        log(
            f"Expanded worksheet by {end_row - worksheet.row_count} rows "
            f"to fit write range ending at row {end_row}."
        )

    last_col = _column_letter(len(HEADERS))
    range_name = f"A{start_row}:{last_col}{end_row}"
    worksheet.update(range_name=range_name, values=rows, value_input_option="RAW")
    log(f"Wrote {len(rows)} rows to range {range_name} in tab '{worksheet.title}'.")


def main() -> None:
    started_at = datetime.utcnow()

    enabled_sources = ["linkedin"]
    disabled_sources: Dict[str, str] = {}
    if INDEED_ENABLED:
        enabled_sources.append("indeed")
    else:
        disabled_sources["indeed"] = "disabled by INDEED_ENABLED"
    if ZIPRECRUITER_ENABLED:
        enabled_sources.append("ziprecruiter")
    else:
        disabled_sources["ziprecruiter"] = "disabled by ZIPRECRUITER_ENABLED"
    if USAJOBS_API_KEY and USAJOBS_USER_AGENT_EMAIL:
        enabled_sources.append("usajobs")
    else:
        disabled_sources["usajobs"] = "missing USAJOBS_API_KEY or USAJOBS_USER_AGENT_EMAIL"
    if ADZUNA_APP_ID and ADZUNA_APP_KEY:
        enabled_sources.append("adzuna")
    else:
        disabled_sources["adzuna"] = "missing ADZUNA_APP_ID or ADZUNA_APP_KEY"
    if JOOBLE_API_KEY:
        enabled_sources.append("jooble")
    else:
        disabled_sources["jooble"] = "missing JOOBLE_API_KEY"

    total_batches = len(KEYWORDS) * len(enabled_sources)
    summary: Dict[str, Any] = {
        "phase": "phase3_multi_source",
        "status": "running",
        "started_at_utc": started_at.isoformat() + "Z",
        "finished_at_utc": "",
        "duration_seconds": 0.0,
        "sheet_id": SHEET_ID,
        "worksheet_name": WORKSHEET_NAME,
        "keyword_source": "KEYWORDS env var" if KEYWORDS_ENV else "DEFAULT_KEYWORDS list",
        "keywords_total": total_batches,
        "keywords_processed": 0,
        "runtime_config": {
            "MAX_PAGES_PER_KEYWORD": MAX_PAGES_PER_KEYWORD,
            "MAX_POST_AGE_DAYS": MAX_POST_AGE_DAYS,
            "PAGE_DELAY_RANGE_SECONDS": list(PAGE_DELAY_RANGE_SECONDS),
            "KEYWORD_DELAY_RANGE_SECONDS": list(KEYWORD_DELAY_RANGE_SECONDS),
            "HEARTBEAT_INTERVAL_SECONDS": HEARTBEAT_INTERVAL_SECONDS,
            "TITLE_FILTER_EXPRESSION": TITLE_FILTER_EXPRESSION,
            "SUPPORTED_SOURCES": SUPPORTED_SOURCES,
            "ENABLED_SOURCES": enabled_sources,
            "DISABLED_SOURCES": disabled_sources,
            "INDEED_MAX_PAGES": INDEED_MAX_PAGES,
            "ZIPRECRUITER_MAX_PAGES": ZIPRECRUITER_MAX_PAGES,
            "SCRAPE_LOCATION": SCRAPE_LOCATION,
            "USAJOBS_MAX_PAGES": USAJOBS_MAX_PAGES,
            "ADZUNA_MAX_PAGES": ADZUNA_MAX_PAGES,
            "JOOBLE_MAX_PAGES": JOOBLE_MAX_PAGES,
        },
        "totals": {
            "rows_collected_before_dedupe": 0,
            "new_rows_appended": 0,
            "duplicates_skipped": 0,
            "merged_existing_rows": 0,
            "merged_by_job_url": 0,
            "merged_by_canonical_key": 0,
            "canonical_duplicates_in_run": 0,
            "canonical_duplicates_against_existing": 0,
            "skipped_old": 0,
            "kept_unknown_date": 0,
            "missing_url": 0,
            "skipped_term_filter": 0,
            "page_timeouts": 0,
            "no_results_pages": 0,
        },
        "keywords": [],
        "sources": [],
        "error": "",
        "traceback": "",
    }

    log("Scraper started.")
    if TITLE_FILTER_EXPRESSION:
        # Fail fast if expression is malformed instead of silently running with bad filtering.
        _get_title_filter_ast()
        log(f"Title filter expression: {TITLE_FILTER_EXPRESSION}")
    log(
        f"Keyword source: {'KEYWORDS env var' if KEYWORDS_ENV else 'DEFAULT_KEYWORDS list'}. "
        f"Total keywords: {len(KEYWORDS)}"
    )
    log(f"Enabled job sources: {enabled_sources}")
    if disabled_sources:
        log(f"Skipped sources due missing config: {disabled_sources}")
    log(f"Date filter: jobs posted within the last {MAX_POST_AGE_DAYS} days.")
    log(
        "Runtime config: "
        f"MAX_PAGES_PER_KEYWORD={MAX_PAGES_PER_KEYWORD}, "
        f"PAGE_DELAY_RANGE_SECONDS={PAGE_DELAY_RANGE_SECONDS[0]}-{PAGE_DELAY_RANGE_SECONDS[1]}, "
        f"KEYWORD_DELAY_RANGE_SECONDS={KEYWORD_DELAY_RANGE_SECONDS[0]}-{KEYWORD_DELAY_RANGE_SECONDS[1]}, "
        f"HEARTBEAT_INTERVAL_SECONDS={HEARTBEAT_INTERVAL_SECONDS}, "
        f"TITLE_FILTER_EXPRESSION={TITLE_FILTER_EXPRESSION or '<not set>'}"
    )
    try:
        client = get_gspread_client()
        spreadsheet = get_spreadsheet(client)
        worksheet = get_jobs_worksheet(spreadsheet)
        ensure_headers(worksheet)
        rows_by_number, by_job_url, by_canonical_key = load_existing_row_index(worksheet)
        date_added = date.today().isoformat()
        now_utc = datetime.utcnow()

        total_new_rows = 0
        total_duplicates_skipped = 0
        total_merged_existing_rows = 0
        total_merged_by_job_url = 0
        total_merged_by_canonical = 0
        total_canonical_dupes_in_run = 0
        total_canonical_dupes_against_existing = 0
        run_seen_canonical_keys = set()
        staged_new_job_urls = set()
        staged_new_canonical_keys = set()

        with sync_playwright() as playwright:
            log("Launching Playwright Chromium in headless mode.")
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=USER_AGENT,
                locale="en-US",
                viewport={"width": 1366, "height": 768},
            )
            page = context.new_page()
            def process_jobs_batch(
                batch_label: str, batch_jobs: List[Dict[str, str]], batch_stats: Dict[str, Any]
            ) -> None:
                nonlocal total_new_rows
                nonlocal total_duplicates_skipped
                nonlocal total_merged_existing_rows
                nonlocal total_merged_by_job_url
                nonlocal total_merged_by_canonical
                nonlocal total_canonical_dupes_in_run
                nonlocal total_canonical_dupes_against_existing

                batch_rows_to_append: List[List[str]] = []
                batch_duplicates_skipped = 0
                batch_merged_existing_rows = 0
                batch_merged_by_job_url = 0
                batch_merged_by_canonical = 0
                batch_canonical_dupes_in_run = 0
                batch_canonical_dupes_against_existing = 0
                batch_row_updates: Dict[int, List[str]] = {}

                for job in batch_jobs:
                    job_url = job["job_url"]
                    canonical_key = job.get("canonical_key", "")

                    if canonical_key:
                        if canonical_key in run_seen_canonical_keys:
                            batch_canonical_dupes_in_run += 1
                        else:
                            run_seen_canonical_keys.add(canonical_key)

                        if canonical_key in by_canonical_key:
                            batch_canonical_dupes_against_existing += 1

                    # Deduplicate against rows staged for append in this run.
                    if job_url in staged_new_job_urls:
                        batch_duplicates_skipped += 1
                        continue
                    if canonical_key and canonical_key in staged_new_canonical_keys:
                        batch_duplicates_skipped += 1
                        continue

                    matched_row_number = None
                    if job_url in by_job_url:
                        matched_row_number = by_job_url[job_url]
                        batch_merged_by_job_url += 1
                    elif canonical_key and canonical_key in by_canonical_key:
                        matched_row_number = by_canonical_key[canonical_key]
                        batch_merged_by_canonical += 1

                    row_keyword = job.get("keyword", batch_label)

                    if matched_row_number is not None:
                        batch_duplicates_skipped += 1
                        batch_merged_existing_rows += 1
                        existing_row = rows_by_number.get(matched_row_number, [""] * len(HEADERS))
                        merged_row = merge_job_into_existing_row(existing_row, job, row_keyword, date_added)
                        rows_by_number[matched_row_number] = merged_row
                        batch_row_updates[matched_row_number] = merged_row
                        by_job_url[job_url] = matched_row_number
                        canonical_after_merge = merged_row[10].strip()
                        if canonical_after_merge and canonical_after_merge not in by_canonical_key:
                            by_canonical_key[canonical_after_merge] = matched_row_number
                        continue

                    new_row = [
                        job_url,
                        "",
                        job["title"],
                        job["company"],
                        job["location"],
                        job["date_posted"],
                        row_keyword,
                        date_added,
                        job.get("source", SOURCE_NAME),
                        job.get("source_job_id", ""),
                        canonical_key,
                        job.get("source", SOURCE_NAME),
                        date_added,
                        date_added,
                    ]
                    batch_rows_to_append.append(new_row)
                    staged_new_job_urls.add(job_url)
                    if canonical_key:
                        staged_new_canonical_keys.add(canonical_key)

                if batch_row_updates:
                    write_row_updates(worksheet, batch_row_updates)

                if batch_rows_to_append:
                    log(
                        f"Appending {len(batch_rows_to_append)} rows to tab '{worksheet.title}' "
                        f"for batch '{batch_label}'."
                    )
                    write_rows_to_next_empty_range(worksheet, batch_rows_to_append)
                    log(f"Appended {len(batch_rows_to_append)} rows to tab '{worksheet.title}'.")
                else:
                    log(f"No new rows to append for batch '{batch_label}'.")

                total_new_rows += len(batch_rows_to_append)
                total_duplicates_skipped += batch_duplicates_skipped
                total_merged_existing_rows += batch_merged_existing_rows
                total_merged_by_job_url += batch_merged_by_job_url
                total_merged_by_canonical += batch_merged_by_canonical
                total_canonical_dupes_in_run += batch_canonical_dupes_in_run
                total_canonical_dupes_against_existing += batch_canonical_dupes_against_existing
                source_name = str(
                    batch_stats.get("source")
                    or (batch_jobs[0].get("source") if batch_jobs else SOURCE_NAME)
                )
                summary["keywords"].append(
                    {
                        **batch_stats,
                        "source": source_name,
                        "new_rows_appended": len(batch_rows_to_append),
                        "duplicates_skipped": batch_duplicates_skipped,
                        "merged_existing_rows": batch_merged_existing_rows,
                        "merged_by_job_url": batch_merged_by_job_url,
                        "merged_by_canonical_key": batch_merged_by_canonical,
                        "canonical_duplicates_in_run": batch_canonical_dupes_in_run,
                        "canonical_duplicates_against_existing": batch_canonical_dupes_against_existing,
                    }
                )
                summary["keywords_processed"] = len(summary["keywords"])

                log(
                    f"Batch complete: {batch_label}. New rows: {len(batch_rows_to_append)}. "
                    f"Duplicates skipped: {batch_duplicates_skipped}. "
                    f"Merged existing rows: {batch_merged_existing_rows} "
                    f"(url={batch_merged_by_job_url}, canonical={batch_merged_by_canonical}). "
                    f"Canonical dupes in run: {batch_canonical_dupes_in_run}. "
                    f"Canonical dupes vs existing: {batch_canonical_dupes_against_existing}"
                )

            for source_index, source_name in enumerate(enabled_sources):
                for keyword_index, keyword in enumerate(KEYWORDS):
                    log(
                        f"Starting source '{source_name}' keyword "
                        f"{keyword_index + 1}/{len(KEYWORDS)}: {keyword}"
                    )

                    if source_name == "linkedin":
                        source_jobs, source_stats = scrape_keyword_jobs(page, keyword, now_utc)
                    elif source_name == "indeed":
                        source_jobs, source_stats = scrape_indeed_keyword_jobs(page, keyword, now_utc)
                    elif source_name == "ziprecruiter":
                        source_jobs, source_stats = scrape_ziprecruiter_keyword_jobs(page, keyword, now_utc)
                    elif source_name == "usajobs":
                        source_jobs, source_stats = scrape_usajobs_keyword_jobs(keyword, now_utc)
                    elif source_name == "adzuna":
                        source_jobs, source_stats = scrape_adzuna_keyword_jobs(keyword, now_utc)
                    else:
                        source_jobs, source_stats = scrape_jooble_keyword_jobs(keyword, now_utc)

                    process_jobs_batch(f"{source_name}:{keyword}", source_jobs, source_stats)

                    is_last_keyword = keyword_index == len(KEYWORDS) - 1
                    is_last_source = source_index == len(enabled_sources) - 1
                    if not (is_last_keyword and is_last_source):
                        _sleep_random(KEYWORD_DELAY_RANGE_SECONDS, "between source batches")

            context.close()
            browser.close()
            log("Browser session closed.")

        summary["totals"] = {
            "rows_collected_before_dedupe": sum(int(item["rows_collected"]) for item in summary["keywords"]),
            "new_rows_appended": total_new_rows,
            "duplicates_skipped": total_duplicates_skipped,
            "merged_existing_rows": total_merged_existing_rows,
            "merged_by_job_url": total_merged_by_job_url,
            "merged_by_canonical_key": total_merged_by_canonical,
            "canonical_duplicates_in_run": total_canonical_dupes_in_run,
            "canonical_duplicates_against_existing": total_canonical_dupes_against_existing,
            "skipped_old": sum(int(item["skipped_old"]) for item in summary["keywords"]),
            "kept_unknown_date": sum(int(item["kept_unknown_date"]) for item in summary["keywords"]),
            "missing_url": sum(int(item["missing_url"]) for item in summary["keywords"]),
            "skipped_term_filter": sum(int(item["skipped_term_filter"]) for item in summary["keywords"]),
            "page_timeouts": sum(int(item["page_timeouts"]) for item in summary["keywords"]),
            "no_results_pages": sum(int(item["no_results_pages"]) for item in summary["keywords"]),
        }
        summary["sources"] = build_source_summary(summary["keywords"])
        summary["status"] = "success"

        log(
            f"Scraper finished. Total new rows: {total_new_rows}. "
            f"Total duplicates skipped: {total_duplicates_skipped}. "
            f"Merged existing rows: {total_merged_existing_rows} "
            f"(url={total_merged_by_job_url}, canonical={total_merged_by_canonical}). "
            f"Canonical dupes in run: {total_canonical_dupes_in_run}. "
            f"Canonical dupes vs existing: {total_canonical_dupes_against_existing}"
        )
    except Exception as exc:
        summary["status"] = "failed"
        summary["error"] = str(exc)
        summary["traceback"] = traceback.format_exc()
        summary["sources"] = build_source_summary(summary["keywords"])
        log(f"Scraper failed: {exc}")
        raise
    finally:
        finished_at = datetime.utcnow()
        summary["finished_at_utc"] = finished_at.isoformat() + "Z"
        summary["duration_seconds"] = round((finished_at - started_at).total_seconds(), 2)
        write_run_summary(summary)


if __name__ == "__main__":
    main()
