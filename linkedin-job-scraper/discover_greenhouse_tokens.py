import json
import os
import re
import urllib.error
import urllib.request
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple
from urllib.parse import quote

import gspread
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from gspread.exceptions import WorksheetNotFound

SHEET_ID = os.getenv("SHEET_ID", "").strip()
CREDS_FILE = os.getenv("CREDS_FILE", "service_account.json").strip()
JOBS_WORKSHEET_NAME = os.getenv("JOBS_WORKSHEET_NAME", "Jobs").strip()
TOKENS_WORKSHEET_NAME = os.getenv("GREENHOUSE_TOKENS_WORKSHEET", "GreenhouseTokens").strip()
DISCOVERY_ENABLED = os.getenv("GREENHOUSE_DISCOVERY_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
MAX_URLS_TO_SCAN = int(os.getenv("GREENHOUSE_DISCOVERY_MAX_URLS", "400"))
HTTP_TIMEOUT_SECONDS = int(os.getenv("GREENHOUSE_DISCOVERY_HTTP_TIMEOUT", "20"))
MAX_FETCH_BYTES = int(os.getenv("GREENHOUSE_DISCOVERY_MAX_FETCH_BYTES", "200000"))
VALIDATE_TOKENS = os.getenv("GREENHOUSE_DISCOVERY_VALIDATE", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
SUMMARY_FILE = os.getenv("GREENHOUSE_DISCOVERY_SUMMARY_FILE", "greenhouse_discovery_summary.json")

HEADERS = [
    "Token",
    "Status",
    "First Seen Date",
    "Last Seen Date",
    "Last Checked Date",
    "Source URLs",
    "Notes",
]

TOKEN_REGEX = re.compile(r"https?://(?:job-boards|boards)\.greenhouse\.io/([a-z0-9-]+)", re.IGNORECASE)
GREENHOUSE_LINK_REGEX = re.compile(r"https?://(?:job-boards|boards)\.greenhouse\.io/[a-z0-9-]+", re.IGNORECASE)
HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def log(message: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {message}", flush=True)


def write_summary(summary: Dict) -> None:
    target = Path(SUMMARY_FILE)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log(f"Discovery summary written to {target.as_posix()}")


def get_gspread_client():
    if not SHEET_ID:
        raise ValueError("SHEET_ID is not set.")

    creds_path = Path(CREDS_FILE)
    if not creds_path.exists():
        raise FileNotFoundError(f"Credentials file not found: {CREDS_FILE}")

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


def ensure_headers(worksheet) -> None:
    existing = worksheet.get_all_values()
    if not existing:
        worksheet.append_row(HEADERS, value_input_option="RAW")
        return
    if existing[0][: len(HEADERS)] != HEADERS:
        worksheet.update(range_name="A1:G1", values=[HEADERS], value_input_option="RAW")


def get_or_create_worksheet(spreadsheet, worksheet_name: str):
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=2000, cols=10)
    ensure_headers(worksheet)
    return worksheet


def get_job_urls(worksheet, max_urls: int) -> List[str]:
    rows = worksheet.get_all_values()
    # Current schema keeps Job URL in column A (index 0).
    urls = [row[0].strip() for row in rows[1:] if row and row[0].strip()]
    # Prefer scanning the latest rows first.
    latest_first = list(reversed(urls))
    return latest_first[:max_urls]


def extract_tokens_from_text(value: str) -> Set[str]:
    return {match.group(1).lower() for match in TOKEN_REGEX.finditer(value or "")}


def fetch_url_context(url: str) -> Tuple[str, str]:
    request = urllib.request.Request(url, headers={"User-Agent": HTTP_USER_AGENT})
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        final_url = response.geturl()
        content_type = (response.headers.get("Content-Type") or "").lower()
        if "text/html" in content_type:
            body = response.read(MAX_FETCH_BYTES).decode("utf-8", errors="ignore")
        else:
            body = ""
    return final_url, body


def discover_tokens_from_urls(job_urls: List[str]) -> Dict[str, Set[str]]:
    tokens_to_sources: Dict[str, Set[str]] = {}
    scanned = 0
    fetch_failures = 0

    for url in job_urls:
        scanned += 1
        direct_tokens = extract_tokens_from_text(url)
        for token in direct_tokens:
            tokens_to_sources.setdefault(token, set()).add(url)

        try:
            final_url, html_body = fetch_url_context(url)
            all_text = " ".join([final_url, " ".join(GREENHOUSE_LINK_REGEX.findall(html_body))])
            discovered_tokens = extract_tokens_from_text(all_text)
            for token in discovered_tokens:
                tokens_to_sources.setdefault(token, set()).add(url)
        except Exception:
            fetch_failures += 1
            continue

    log(
        f"Scanned {scanned} URLs, discovered {len(tokens_to_sources)} unique Greenhouse token candidates, "
        f"fetch failures: {fetch_failures}"
    )
    return tokens_to_sources


def validate_token(token: str) -> bool:
    url = f"https://boards-api.greenhouse.io/v1/boards/{quote(token)}/jobs"
    try:
        request = urllib.request.Request(url, headers={"User-Agent": HTTP_USER_AGENT})
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return isinstance(payload, dict) and "jobs" in payload
    except Exception:
        return False


def load_existing_tokens(worksheet) -> Dict[str, Tuple[int, List[str]]]:
    rows = worksheet.get_all_values()
    result: Dict[str, Tuple[int, List[str]]] = {}
    for row_number, row in enumerate(rows[1:], start=2):
        if not row:
            continue
        token = row[0].strip().lower() if len(row) >= 1 else ""
        if token:
            normalized = list(row[: len(HEADERS)]) + [""] * max(0, len(HEADERS) - len(row))
            result[token] = (row_number, normalized)
    return result


def merge_source_urls(existing_urls: str, discovered_sources: Set[str]) -> str:
    items = [part.strip() for part in existing_urls.split(" | ") if part.strip()]
    existing = {item.lower() for item in items}
    for source in sorted(discovered_sources):
        if source.lower() not in existing:
            items.append(source)
    joined = " | ".join(items)
    # Keep cell content bounded.
    return joined[:30000]


def main() -> None:
    summary = {
        "started_at_utc": datetime.utcnow().isoformat() + "Z",
        "status": "running",
        "urls_scanned": 0,
        "candidate_tokens": 0,
        "validated_tokens": 0,
        "new_tokens_added": 0,
        "existing_tokens_updated": 0,
        "validation_failures": 0,
        "error": "",
    }

    if not DISCOVERY_ENABLED:
        log("Greenhouse discovery disabled by GREENHOUSE_DISCOVERY_ENABLED.")
        summary["status"] = "disabled"
        summary["finished_at_utc"] = datetime.utcnow().isoformat() + "Z"
        write_summary(summary)
        return

    try:
        client = get_gspread_client()
        spreadsheet = client.open_by_key(SHEET_ID)
        jobs_ws = get_or_create_worksheet(spreadsheet, JOBS_WORKSHEET_NAME)
        tokens_ws = get_or_create_worksheet(spreadsheet, TOKENS_WORKSHEET_NAME)

        job_urls = get_job_urls(jobs_ws, MAX_URLS_TO_SCAN)
        summary["urls_scanned"] = len(job_urls)
        tokens_to_sources = discover_tokens_from_urls(job_urls)
        summary["candidate_tokens"] = len(tokens_to_sources)

        existing_tokens = load_existing_tokens(tokens_ws)
        today = date.today().isoformat()
        row_updates = []
        rows_to_append = []

        for token in sorted(tokens_to_sources.keys()):
            is_valid = validate_token(token) if VALIDATE_TOKENS else True
            if is_valid:
                summary["validated_tokens"] += 1
            else:
                summary["validation_failures"] += 1

            status = "active" if is_valid else "invalid"
            source_urls = merge_source_urls("", tokens_to_sources[token])
            notes = ""

            if token in existing_tokens:
                row_number, existing_row = existing_tokens[token]
                first_seen = existing_row[2].strip() or today
                merged_sources = merge_source_urls(existing_row[5], tokens_to_sources[token])
                row_updates.append(
                    {
                        "range": f"A{row_number}:G{row_number}",
                        "values": [[token, status, first_seen, today, today, merged_sources, notes]],
                    }
                )
                summary["existing_tokens_updated"] += 1
            else:
                rows_to_append.append([token, status, today, today, today, source_urls, notes])
                summary["new_tokens_added"] += 1

        if row_updates:
            tokens_ws.batch_update(row_updates, value_input_option="RAW")
            log(f"Updated {len(row_updates)} existing token rows.")

        if rows_to_append:
            tokens_ws.append_rows(rows_to_append, value_input_option="RAW")
            log(f"Appended {len(rows_to_append)} new token rows.")

        summary["status"] = "success"
    except Exception as exc:
        summary["status"] = "failed"
        summary["error"] = str(exc)
        log(f"Greenhouse discovery failed: {exc}")
        raise
    finally:
        summary["finished_at_utc"] = datetime.utcnow().isoformat() + "Z"
        write_summary(summary)


if __name__ == "__main__":
    main()
