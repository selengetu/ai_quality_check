"""
alerting.py
-----------
Two responsibilities:
  1. POST a formatted Slack message to the SLACK_WEBHOOK_URL.
  2. Write the full incident record to the MotherDuck incidents table.

Both operations are best-effort — a Slack or DB failure must never mask the
original data quality failure that triggered them.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "")
MD_DATABASE = "dq_monitor"
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://localhost:8501")

# Slack severity → emoji mapping
_SEVERITY_EMOJI = {
    "critical": ":red_circle:",
    "warning":  ":large_yellow_circle:",
    "info":     ":large_blue_circle:",
}
_CONFIDENCE_EMOJI = {
    "high":   ":white_check_mark:",
    "medium": ":question:",
    "low":    ":warning:",
}


# ── DB connection ─────────────────────────────────────────────────────────────

def _get_conn():
    """Return a DuckDB connection (MotherDuck if token present, else local)."""
    import duckdb

    if MOTHERDUCK_TOKEN:
        return duckdb.connect(
            f"md:{MD_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}"
        )
    return duckdb.connect("/tmp/dq_monitor.duckdb")


# ── Incidents table bootstrap ─────────────────────────────────────────────────

_CREATE_INCIDENTS_TABLE = """
CREATE TABLE IF NOT EXISTS incidents (
    incident_id        VARCHAR,
    run_id             VARCHAR,
    check_name         VARCHAR,
    check_type         VARCHAR,
    column_name        VARCHAR,
    failure_rate       DOUBLE,
    root_cause         VARCHAR,
    confidence         VARCHAR,
    suggested_fix      VARCHAR,
    severity           VARCHAR,
    needs_human_review BOOLEAN,
    created_at         TIMESTAMP
)
"""


def _ensure_incidents_table(conn) -> None:
    """Create the incidents table if it doesn't already exist."""
    conn.execute(_CREATE_INCIDENTS_TABLE)


# ── Slack formatter ───────────────────────────────────────────────────────────

def _build_slack_payload(
    ctx: dict[str, Any],
    diagnosis: dict[str, Any],
    incident_id: str,
    run_id: str,
) -> dict[str, Any]:
    """
    Build a Slack Block Kit payload for the incident alert.

    Uses sections + dividers for readability in both desktop and mobile.
    """
    severity: str = diagnosis.get("severity", "warning")
    confidence: str = diagnosis.get("confidence", "low")
    sev_emoji: str = _SEVERITY_EMOJI.get(severity, ":white_circle:")
    conf_emoji: str = _CONFIDENCE_EMOJI.get(confidence, ":question:")

    check_name: str = ctx.get("check_name", "unknown")
    column_name: str = ctx.get("column_name", "unknown")
    failure_pct: str = ctx.get("failure_rate_pct", "N/A")
    root_cause: str = diagnosis.get("root_cause", "")
    suggested_fix: str = diagnosis.get("suggested_fix", "")
    needs_review: bool = diagnosis.get("needs_human_review", False)

    review_line = ":rotating_light: *Human review required*\n" if needs_review else ""

    return {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{sev_emoji} Data Quality Incident — {severity.upper()}",
                    "emoji": True,
                },
            },
            {"type": "divider"},
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Check:*\n`{check_name}`"},
                    {"type": "mrkdwn", "text": f"*Column:*\n`{column_name}`"},
                    {"type": "mrkdwn", "text": f"*Failure rate:*\n{failure_pct}"},
                    {"type": "mrkdwn", "text": f"*Confidence:*\n{conf_emoji} {confidence}"},
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Root Cause:*\n{root_cause}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Suggested Fix:*\n```{suggested_fix}```",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"{review_line}"
                        f"*Run ID:* `{run_id}`  |  *Incident ID:* `{incident_id}`"
                    ),
                },
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Dashboard", "emoji": True},
                        "url": DASHBOARD_URL,
                        "style": "primary" if severity == "critical" else "default",
                    }
                ],
            },
        ]
    }


# ── Public API ────────────────────────────────────────────────────────────────

def send_alert(
    ctx: dict[str, Any],
    diagnosis: dict[str, Any],
    run_id: str,
) -> str:
    """
    Send a Slack alert and persist the incident record to MotherDuck.

    Args:
        ctx:       Context dict from context_builder.build_context().
        diagnosis: Diagnosis dict from claude_diagnosis.diagnose().
        run_id:    Airflow run_id for correlation.

    Returns:
        The generated incident_id UUID string.
    """
    incident_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc)

    # 1. Persist to MotherDuck (do this first — Slack is nice-to-have)
    _write_incident(ctx, diagnosis, incident_id, run_id, created_at)

    # 2. Send Slack alert
    _send_slack(ctx, diagnosis, incident_id, run_id)

    return incident_id


def _write_incident(
    ctx: dict[str, Any],
    diagnosis: dict[str, Any],
    incident_id: str,
    run_id: str,
    created_at: datetime,
) -> None:
    """Write the incident record to the MotherDuck incidents table."""
    conn = _get_conn()
    try:
        _ensure_incidents_table(conn)
        conn.execute(
            """
            INSERT INTO incidents (
                incident_id, run_id, check_name, check_type, column_name,
                failure_rate, root_cause, confidence, suggested_fix, severity,
                needs_human_review, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                incident_id,
                run_id,
                ctx.get("check_name", ""),
                ctx.get("check_type", ""),
                ctx.get("column_name", ""),
                ctx.get("failure_rate", 0.0),
                diagnosis.get("root_cause", ""),
                diagnosis.get("confidence", "low"),
                diagnosis.get("suggested_fix", ""),
                diagnosis.get("severity", "warning"),
                diagnosis.get("needs_human_review", True),
                created_at,
            ],
        )
        log.info("Incident %s written to MotherDuck", incident_id)
    except Exception as exc:
        log.error("Failed to write incident to MotherDuck: %s", exc)
    finally:
        conn.close()


def _send_slack(
    ctx: dict[str, Any],
    diagnosis: dict[str, Any],
    incident_id: str,
    run_id: str,
) -> None:
    """POST the Slack Block Kit payload to the webhook URL."""
    if not SLACK_WEBHOOK_URL:
        log.warning("SLACK_WEBHOOK_URL not set — skipping Slack alert")
        return

    payload = _build_slack_payload(ctx, diagnosis, incident_id, run_id)

    try:
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        log.info("Slack alert sent for incident %s (status=%d)", incident_id, resp.status_code)
    except requests.exceptions.Timeout:
        log.error("Slack webhook timed out for incident %s", incident_id)
    except requests.exceptions.HTTPError as exc:
        log.error("Slack webhook HTTP error for incident %s: %s", incident_id, exc)
    except requests.exceptions.RequestException as exc:
        log.error("Slack webhook request failed for incident %s: %s", incident_id, exc)
