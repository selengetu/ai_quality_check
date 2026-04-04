"""
claude_diagnosis.py
-------------------
Calls the Claude API with a structured failure context and returns a parsed
incident diagnosis.

Model: claude-sonnet-4-20250514
Response schema (JSON):
    root_cause         str   — max 2 sentences
    confidence         str   — "high" | "medium" | "low"
    suggested_fix      str   — specific dbt or pipeline code change
    severity           str   — "critical" | "warning" | "info"
    needs_human_review bool
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import anthropic

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 1024
TEMPERATURE = 0  # deterministic for reproducible diagnoses

SYSTEM_PROMPT = """\
You are a senior data engineer reviewing a data quality failure in a production \
NYC Taxi data pipeline built on dbt + DuckDB. Be concise and technical. \
Return ONLY valid JSON — no markdown fences, no commentary outside the JSON object.\
"""

# Fallback used when the API call fails entirely
_FALLBACK_DIAGNOSIS: dict[str, Any] = {
    "root_cause": "Claude API unavailable. Manual investigation required.",
    "confidence": "low",
    "suggested_fix": "Check Anthropic API key and network connectivity, then re-run the pipeline.",
    "severity": "warning",
    "needs_human_review": True,
}

# Valid enum values — used for response validation
_VALID_CONFIDENCE = {"high", "medium", "low"}
_VALID_SEVERITY = {"critical", "warning", "info"}


# ── Prompt builder ────────────────────────────────────────────────────────────

def _build_user_prompt(ctx: dict[str, Any]) -> str:
    """
    Render the structured failure context into a clear, information-dense
    prompt that gives Claude everything it needs to diagnose the issue.
    """
    trend_section = ""
    if ctx.get("trend_available"):
        trend_rows = "\n".join(
            f"  {t['date']}: {t['avg_failure_rate']:.4f}"
            for t in ctx["recent_trend_7d"]
        )
        trend_section = f"\n7-DAY NULL-RATE TREND (same column + check):\n{trend_rows}"
    else:
        trend_section = "\n7-DAY TREND: No history yet (first occurrence or new column)."

    sample_section = ""
    if ctx["sample_bad_rows"]:
        sample_section = (
            f"\nSAMPLE BAD ROWS ({ctx['sample_bad_rows_count']} shown):\n"
            + json.dumps(ctx["sample_bad_rows"], indent=2, default=str)
        )
    else:
        sample_section = "\nSAMPLE BAD ROWS: none available."

    return f"""\
DATA QUALITY FAILURE REPORT
============================
Run ID:        {ctx['pipeline_run_id']}
Timestamp:     {ctx['run_timestamp']}
Check name:    {ctx['check_name']}
Check type:    {ctx['check_type']}
Column:        {ctx['column_name']}
Failure rate:  {ctx['failure_rate_pct']} ({ctx['failure_rate']:.6f})
{trend_section}
{sample_section}

DBT MODEL SQL (most relevant model for this column):
----------------------------------------------------
{ctx['dbt_model_sql']}

----------------------------------------------------
Diagnose this failure and respond with ONLY a JSON object matching this schema:
{{
  "root_cause": "<2 sentences max — what caused this failure>",
  "confidence": "<high|medium|low>",
  "suggested_fix": "<specific actionable change to dbt SQL, pipeline config, or source system>",
  "severity": "<critical|warning|info>",
  "needs_human_review": <true|false>
}}

Severity guide:
  critical — pipeline produces wrong numbers downstream; dashboards or SLAs affected
  warning  — data quality degraded but pipeline still runs; monitor closely
  info     — minor anomaly, within acceptable tolerance
"""


# ── Response validator ────────────────────────────────────────────────────────

def _validate_diagnosis(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Validate and coerce the Claude response to the expected schema.

    Fills in defaults for missing/invalid fields rather than raising, so a
    partial response is still actionable.
    """
    diagnosis: dict[str, Any] = {}

    diagnosis["root_cause"] = str(raw.get("root_cause", "Unknown root cause."))[:500]

    confidence = str(raw.get("confidence", "low")).lower()
    diagnosis["confidence"] = confidence if confidence in _VALID_CONFIDENCE else "low"

    diagnosis["suggested_fix"] = str(raw.get("suggested_fix", "No fix suggested."))[:1000]

    severity = str(raw.get("severity", "warning")).lower()
    diagnosis["severity"] = severity if severity in _VALID_SEVERITY else "warning"

    hr = raw.get("needs_human_review", True)
    diagnosis["needs_human_review"] = bool(hr) if isinstance(hr, bool) else str(hr).lower() == "true"

    return diagnosis


# ── Public API ────────────────────────────────────────────────────────────────

def diagnose(ctx: dict[str, Any]) -> dict[str, Any]:
    """
    Call Claude API with the failure context and return a validated diagnosis.

    Args:
        ctx: Structured context dict from context_builder.build_context().

    Returns:
        Validated diagnosis dict with keys:
            root_cause, confidence, suggested_fix, severity, needs_human_review.
        Falls back to _FALLBACK_DIAGNOSIS on any API or parse error.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set — returning fallback diagnosis")
        return dict(_FALLBACK_DIAGNOSIS)

    client = anthropic.Anthropic(api_key=api_key)
    user_prompt = _build_user_prompt(ctx)

    log.info(
        "Calling Claude (%s) for check=%s column=%s",
        MODEL,
        ctx.get("check_name"),
        ctx.get("column_name"),
    )

    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            temperature=TEMPERATURE,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
    except anthropic.AuthenticationError as exc:
        log.error("Claude API authentication failed: %s", exc)
        return dict(_FALLBACK_DIAGNOSIS)
    except anthropic.RateLimitError as exc:
        log.error("Claude API rate limit hit: %s", exc)
        fallback = dict(_FALLBACK_DIAGNOSIS)
        fallback["root_cause"] = "Claude API rate limit exceeded. Retry after cooldown."
        return fallback
    except anthropic.APIStatusError as exc:
        log.error("Claude API error (status=%s): %s", exc.status_code, exc.message)
        return dict(_FALLBACK_DIAGNOSIS)
    except anthropic.APIConnectionError as exc:
        log.error("Claude API connection error: %s", exc)
        return dict(_FALLBACK_DIAGNOSIS)

    raw_text: str = message.content[0].text.strip()
    log.debug("Raw Claude response: %s", raw_text[:500])

    # Strip markdown code fences if Claude added them despite the prompt
    if raw_text.startswith("```"):
        lines = raw_text.splitlines()
        raw_text = "\n".join(
            line for line in lines
            if not line.startswith("```")
        ).strip()

    try:
        raw_json: dict = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        log.error("Claude returned non-JSON response: %s\nRaw: %s", exc, raw_text[:300])
        fallback = dict(_FALLBACK_DIAGNOSIS)
        fallback["root_cause"] = (
            f"AI diagnosis returned unparseable response. Raw: {raw_text[:200]}"
        )
        return fallback

    diagnosis = _validate_diagnosis(raw_json)

    log.info(
        "Diagnosis complete: severity=%s  confidence=%s  needs_review=%s",
        diagnosis["severity"],
        diagnosis["confidence"],
        diagnosis["needs_human_review"],
    )

    return diagnosis
