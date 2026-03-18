import json
import os
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape
from pathlib import Path
from typing import Any, Dict, List

RUN_SUMMARY_FILE = os.getenv("RUN_SUMMARY_FILE", "scraper_run_summary.json").strip()
RUN_STATUS = os.getenv("RUN_STATUS", "").strip().lower()

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = os.getenv("SMTP_PORT", "465").strip()
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "").strip()
SMTP_TO_EMAIL = os.getenv("SMTP_TO_EMAIL", "").strip()
SMTP_USE_STARTTLS = os.getenv("SMTP_USE_STARTTLS", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _required(value: str, name: str) -> str:
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _to_recipients(raw: str) -> List[str]:
    normalized = raw.replace(";", ",")
    return [item.strip() for item in normalized.split(",") if item.strip()]


def _load_summary(path: str) -> Dict[str, Any]:
    summary_path = Path(path)
    if not summary_path.exists():
        return {
            "status": RUN_STATUS or "unknown",
            "started_at_utc": "",
            "finished_at_utc": datetime.utcnow().isoformat() + "Z",
            "duration_seconds": 0,
            "keywords_total": 0,
            "keywords_processed": 0,
            "totals": {},
            "keywords": [],
            "error": f"Summary file not found: {summary_path.as_posix()}",
            "traceback": "",
        }
    return json.loads(summary_path.read_text(encoding="utf-8"))


def _run_url() -> str:
    server = os.getenv("GITHUB_SERVER_URL", "https://github.com").strip()
    repo = os.getenv("GITHUB_REPOSITORY", "").strip()
    run_id = os.getenv("GITHUB_RUN_ID", "").strip()
    if repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return ""


def _build_subject(summary: Dict[str, Any]) -> str:
    status = str(summary.get("status", RUN_STATUS or "unknown")).upper()
    totals = summary.get("totals", {}) or {}
    new_rows = int(totals.get("new_rows_appended", 0))
    scraped_rows = int(totals.get("rows_collected_before_dedupe", 0))
    return f"[LinkedIn Scraper] {status} | {new_rows} new / {scraped_rows} collected"


def _build_keyword_table_rows(summary: Dict[str, Any]) -> str:
    rows = []
    for item in summary.get("keywords", []) or []:
        rows.append(
            "<tr>"
            f"<td>{escape(str(item.get('keyword', '')))}</td>"
            f"<td>{int(item.get('rows_collected', 0))}</td>"
            f"<td>{int(item.get('new_rows_appended', 0))}</td>"
            f"<td>{int(item.get('duplicates_skipped', 0))}</td>"
            f"<td>{int(item.get('skipped_old', 0))}</td>"
            f"<td>{int(item.get('page_timeouts', 0))}</td>"
            "</tr>"
        )
    return "".join(rows) if rows else "<tr><td colspan='6'>No keyword details available</td></tr>"


def _build_html(summary: Dict[str, Any]) -> str:
    totals = summary.get("totals", {}) or {}
    status = escape(str(summary.get("status", RUN_STATUS or "unknown")).upper())
    run_url = _run_url()
    run_link_html = (
        f"<p><a href='{escape(run_url)}'>Open GitHub Actions run</a></p>" if run_url else ""
    )
    error = summary.get("error", "")
    error_html = f"<p><b>Error:</b> {escape(str(error))}</p>" if error else ""

    return f"""
<html>
  <body style="font-family: Arial, sans-serif; font-size: 13px; color: #222;">
    <h3 style="margin: 0 0 8px 0;">LinkedIn Job Scraper Run: {status}</h3>
    <table cellpadding="6" cellspacing="0" border="1" style="border-collapse: collapse; margin-bottom: 10px;">
      <tr><td><b>Started (UTC)</b></td><td>{escape(str(summary.get("started_at_utc", "")))}</td></tr>
      <tr><td><b>Finished (UTC)</b></td><td>{escape(str(summary.get("finished_at_utc", "")))}</td></tr>
      <tr><td><b>Duration (s)</b></td><td>{escape(str(summary.get("duration_seconds", "")))}</td></tr>
      <tr><td><b>Keywords</b></td><td>{int(summary.get("keywords_processed", 0))}/{int(summary.get("keywords_total", 0))}</td></tr>
      <tr><td><b>Collected (pre-dedupe)</b></td><td>{int(totals.get("rows_collected_before_dedupe", 0))}</td></tr>
      <tr><td><b>New Rows Appended</b></td><td>{int(totals.get("new_rows_appended", 0))}</td></tr>
      <tr><td><b>Duplicates Skipped</b></td><td>{int(totals.get("duplicates_skipped", 0))}</td></tr>
      <tr><td><b>Old Skipped</b></td><td>{int(totals.get("skipped_old", 0))}</td></tr>
      <tr><td><b>Unknown Date Kept</b></td><td>{int(totals.get("kept_unknown_date", 0))}</td></tr>
      <tr><td><b>Page Timeouts</b></td><td>{int(totals.get("page_timeouts", 0))}</td></tr>
    </table>
    {run_link_html}
    {error_html}
    <h4 style="margin: 12px 0 6px 0;">Per-keyword summary</h4>
    <table cellpadding="5" cellspacing="0" border="1" style="border-collapse: collapse;">
      <tr>
        <th>Keyword</th>
        <th>Collected</th>
        <th>New</th>
        <th>Dupes</th>
        <th>Old</th>
        <th>Timeouts</th>
      </tr>
      {_build_keyword_table_rows(summary)}
    </table>
  </body>
</html>
""".strip()


def _build_text(summary: Dict[str, Any]) -> str:
    totals = summary.get("totals", {}) or {}
    lines = [
        f"LinkedIn Job Scraper Run: {str(summary.get('status', RUN_STATUS or 'unknown')).upper()}",
        f"Started: {summary.get('started_at_utc', '')}",
        f"Finished: {summary.get('finished_at_utc', '')}",
        f"Duration (s): {summary.get('duration_seconds', '')}",
        f"Keywords: {int(summary.get('keywords_processed', 0))}/{int(summary.get('keywords_total', 0))}",
        f"Collected (pre-dedupe): {int(totals.get('rows_collected_before_dedupe', 0))}",
        f"New Rows Appended: {int(totals.get('new_rows_appended', 0))}",
        f"Duplicates Skipped: {int(totals.get('duplicates_skipped', 0))}",
        f"Old Skipped: {int(totals.get('skipped_old', 0))}",
        f"Unknown Date Kept: {int(totals.get('kept_unknown_date', 0))}",
        f"Page Timeouts: {int(totals.get('page_timeouts', 0))}",
    ]
    run_url = _run_url()
    if run_url:
        lines.append(f"Run URL: {run_url}")
    if summary.get("error"):
        lines.append(f"Error: {summary.get('error')}")

    lines.append("")
    lines.append("Per-keyword summary:")
    for item in summary.get("keywords", []) or []:
        lines.append(
            "- "
            f"{item.get('keyword', '')}: "
            f"collected={int(item.get('rows_collected', 0))}, "
            f"new={int(item.get('new_rows_appended', 0))}, "
            f"dupes={int(item.get('duplicates_skipped', 0))}, "
            f"old={int(item.get('skipped_old', 0))}, "
            f"timeouts={int(item.get('page_timeouts', 0))}"
        )
    return "\n".join(lines)


def _send_email(summary: Dict[str, Any]) -> None:
    host = _required(SMTP_HOST, "SMTP_HOST")
    from_email = _required(SMTP_FROM_EMAIL, "SMTP_FROM_EMAIL")
    recipients = _to_recipients(_required(SMTP_TO_EMAIL, "SMTP_TO_EMAIL"))
    if not recipients:
        raise ValueError("SMTP_TO_EMAIL did not contain any valid recipient addresses.")

    try:
        port = int(_required(SMTP_PORT, "SMTP_PORT"))
    except ValueError as exc:
        raise ValueError(f"SMTP_PORT must be an integer, got '{SMTP_PORT}'") from exc

    subject = _build_subject(summary)
    text_body = _build_text(summary)
    html_body = _build_html(summary)

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = from_email
    message["To"] = ", ".join(recipients)
    message.attach(MIMEText(text_body, "plain", "utf-8"))
    message.attach(MIMEText(html_body, "html", "utf-8"))

    if SMTP_USE_STARTTLS:
        with smtplib.SMTP(host, port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            if SMTP_USERNAME or SMTP_PASSWORD:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(from_email, recipients, message.as_string())
    else:
        with smtplib.SMTP_SSL(host, port, timeout=30) as server:
            if SMTP_USERNAME or SMTP_PASSWORD:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(from_email, recipients, message.as_string())


def main() -> None:
    summary = _load_summary(RUN_SUMMARY_FILE)
    if summary.get("status") == "running":
        summary["status"] = RUN_STATUS or "unknown"
    _send_email(summary)
    print(f"Notification email sent to: {SMTP_TO_EMAIL}", flush=True)


if __name__ == "__main__":
    main()
