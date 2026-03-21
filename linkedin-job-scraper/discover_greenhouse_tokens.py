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

def _env_or_default(name: str, default: str) -> str:
    value = os.getenv(name, "")
    value = value.strip() if value is not None else ""
    return value if value else default


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "")
    raw = raw.strip().lower() if raw is not None else ""
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _parse_int_env(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name, "")
    raw = raw.strip() if raw is not None else ""
    if not raw:
        return default
    try:
        parsed = int(raw)
        return parsed if parsed >= minimum else default
    except ValueError:
        return default


SHEET_ID = _env_or_default("SHEET_ID", "")
CREDS_FILE = _env_or_default("CREDS_FILE", "service_account.json")
JOBS_WORKSHEET_NAME = _env_or_default("JOBS_WORKSHEET_NAME", "Jobs")
TOKENS_WORKSHEET_NAME = _env_or_default("GREENHOUSE_TOKENS_WORKSHEET", "GreenhouseTokens")
DISCOVERY_ENABLED = _parse_bool_env("GREENHOUSE_DISCOVERY_ENABLED", True)
MAX_URLS_TO_SCAN = _parse_int_env("GREENHOUSE_DISCOVERY_MAX_URLS", 400, minimum=1)
HTTP_TIMEOUT_SECONDS = _parse_int_env("GREENHOUSE_DISCOVERY_HTTP_TIMEOUT", 20, minimum=1)
MAX_FETCH_BYTES = _parse_int_env("GREENHOUSE_DISCOVERY_MAX_FETCH_BYTES", 200000, minimum=50000)
VALIDATE_TOKENS = _parse_bool_env("GREENHOUSE_DISCOVERY_VALIDATE", True)
SUMMARY_FILE = _env_or_default("GREENHOUSE_DISCOVERY_SUMMARY_FILE", "greenhouse_discovery_summary.json")

HEADERS = [
    "Token",
    "Status",
    "First Seen Date",
    "Last Seen Date",
    "Last Checked Date",
    "Board URL",
    "Greenhouse URLs",
    "Source URLs",
    "Notes",
]

TOKEN_REGEX = re.compile(r"https?://(?:job-boards|boards)\.greenhouse\.io/([a-z0-9-]+)", re.IGNORECASE)
GREENHOUSE_LINK_REGEX = re.compile(
    r"https?://(?:job-boards|boards)\.greenhouse\.io/[a-z0-9-]+(?:/[^\s\"'<>)]*)?",
    re.IGNORECASE,
)
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
        worksheet.update(range_name="A1:I1", values=[HEADERS], value_input_option="RAW")


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


def extract_greenhouse_urls_from_text(value: str) -> Set[str]:
    urls = set()
    for match in GREENHOUSE_LINK_REGEX.finditer(value or ""):
        urls.add(match.group(0).strip())
    return urls


def canonical_board_url(token: str) -> str:
    return f"https://boards.greenhouse.io/{token}"


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


def discover_tokens_from_urls(job_urls: List[str]) -> Tuple[Dict[str, Dict[str, Set[str]]], int]:
    token_map: Dict[str, Dict[str, Set[str]]] = {}
    scanned = 0
    fetch_failures = 0

    for url in job_urls:
        scanned += 1
        direct_tokens = extract_tokens_from_text(url)
        direct_greenhouse_urls = extract_greenhouse_urls_from_text(url)

        for token in direct_tokens:
            token_entry = token_map.setdefault(
                token, {"source_urls": set(), "greenhouse_urls": set()}
            )
            token_entry["source_urls"].add(url)
            token_entry["greenhouse_urls"].add(canonical_board_url(token))

        for gh_url in direct_greenhouse_urls:
            for token in extract_tokens_from_text(gh_url):
                token_entry = token_map.setdefault(
                    token, {"source_urls": set(), "greenhouse_urls": set()}
                )
                token_entry["source_urls"].add(url)
                token_entry["greenhouse_urls"].add(gh_url)

        try:
            final_url, html_body = fetch_url_context(url)
            discovered_greenhouse_urls = set()
            discovered_greenhouse_urls |= extract_greenhouse_urls_from_text(final_url)
            discovered_greenhouse_urls |= extract_greenhouse_urls_from_text(html_body)

            discovered_tokens = extract_tokens_from_text(final_url + "\n" + html_body)
            for token in discovered_tokens:
                token_entry = token_map.setdefault(
                    token, {"source_urls": set(), "greenhouse_urls": set()}
                )
                token_entry["source_urls"].add(url)
                token_entry["greenhouse_urls"].add(canonical_board_url(token))
            for gh_url in discovered_greenhouse_urls:
                for token in extract_tokens_from_text(gh_url):
                    token_entry = token_map.setdefault(
                        token, {"source_urls": set(), "greenhouse_urls": set()}
                    )
                    token_entry["source_urls"].add(url)
                    token_entry["greenhouse_urls"].add(gh_url)
        except Exception:
            fetch_failures += 1
            continue

    log(
        f"Scanned {scanned} URLs, discovered {len(token_map)} unique Greenhouse token candidates, "
        f"fetch failures: {fetch_failures}"
    )
    return token_map, fetch_failures


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


def merge_values(existing_urls: str, discovered_sources: Set[str]) -> str:
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
        "fetch_failures": 0,
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
        token_map, fetch_failures = discover_tokens_from_urls(job_urls)
        summary["candidate_tokens"] = len(token_map)
        summary["fetch_failures"] = fetch_failures

        existing_tokens = load_existing_tokens(tokens_ws)
        today = date.today().isoformat()
        row_updates = []
        rows_to_append = []

        for token in sorted(token_map.keys()):
            is_valid = validate_token(token) if VALIDATE_TOKENS else True
            if is_valid:
                summary["validated_tokens"] += 1
            else:
                summary["validation_failures"] += 1

            status = "active" if is_valid else "invalid"
            source_urls = merge_values("", token_map[token]["source_urls"])
            greenhouse_urls = merge_values("", token_map[token]["greenhouse_urls"])
            board_url = canonical_board_url(token)
            notes = ""

            if token in existing_tokens:
                row_number, existing_row = existing_tokens[token]
                first_seen = existing_row[2].strip() or today
                merged_greenhouse_urls = merge_values(existing_row[6], token_map[token]["greenhouse_urls"])
                merged_sources = merge_values(existing_row[7], token_map[token]["source_urls"])
                row_updates.append(
                    {
                        "range": f"A{row_number}:I{row_number}",
                        "values": [
                            [
                                token,
                                status,
                                first_seen,
                                today,
                                today,
                                board_url,
                                merged_greenhouse_urls,
                                merged_sources,
                                notes,
                            ]
                        ],
                    }
                )
                summary["existing_tokens_updated"] += 1
            else:
                rows_to_append.append(
                    [token, status, today, today, today, board_url, greenhouse_urls, source_urls, notes]
                )
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
