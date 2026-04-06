"""
context_builder.py
------------------
Assembles a rich, structured context dict from a raw GE or dbt failure dict.

The context is what gets sent to Claude — the more signal here, the better
the diagnosis. We pull:
  - failure metadata from the upstream task (check name, column, failure rate)
  - the dbt model SQL that produced the failing table
  - the last 7 days of null-rate trend for this column from the incidents table
  - up to 10 sample bad rows
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).parent.parent
DBT_MODELS_DIR = _REPO_ROOT / "dbt" / "models"

# ── Connection ────────────────────────────────────────────────────────────────
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "")
MD_DATABASE = "dq_monitor"
LOCAL_INCIDENTS_DB = "/tmp/dq_incidents.duckdb"  # separate file from dbt db


def _get_conn():
    """Return a DuckDB connection to the incidents store."""
    import duckdb

    if MOTHERDUCK_TOKEN:
        return duckdb.connect(
            f"md:{MD_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}"
        )
    return duckdb.connect(LOCAL_INCIDENTS_DB)


# ── SQL reader ────────────────────────────────────────────────────────────────

def _read_model_sql(column_name: str, check_type: str) -> str:
    """
    Return the SQL of the dbt model most likely to own the failing column.

    Heuristic: search all .sql files under dbt/models for a column reference.
    Falls back to the staging model if no specific match is found.
    """
    col_lower = column_name.lower()
    candidates: list[tuple[int, Path]] = []

    for sql_file in DBT_MODELS_DIR.rglob("*.sql"):
        try:
            content = sql_file.read_text()
            # Score by how many times the column name appears
            score = content.lower().count(col_lower)
            if score > 0:
                candidates.append((score, sql_file))
        except OSError:
            continue

    if not candidates:
        # Fallback: return the staging model
        fallback = DBT_MODELS_DIR / "staging" / "stg_taxi_trips.sql"
        if fallback.exists():
            return fallback.read_text()
        return "(dbt model SQL not found)"

    # Highest score wins
    candidates.sort(key=lambda t: t[0], reverse=True)
    best_path = candidates[0][1]
    log.info("Using dbt model SQL from: %s", best_path)
    return best_path.read_text()


# ── Trend reader ──────────────────────────────────────────────────────────────

def _fetch_recent_trend(column_name: str, check_name: str) -> list[dict[str, Any]]:
    """
    Query the incidents table for the last 7 days of failure_rate for this
    column + check combination.

    Returns a list of {date, failure_rate} dicts (empty if table doesn't exist
    yet or no history found).
    """
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT
                CAST(created_at AS DATE) AS date,
                AVG(failure_rate)        AS avg_failure_rate
            FROM incidents
            WHERE column_name = ?
              AND check_name  = ?
              AND created_at >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY 1
            ORDER BY 1
            """,
            [column_name, check_name],
        ).fetchall()

        return [
            {"date": str(r[0]), "avg_failure_rate": round(r[1], 6)}
            for r in rows
        ]
    except Exception as exc:
        # incidents table may not exist on first run — that's fine
        log.debug("Could not fetch trend data (expected on first run): %s", exc)
        return []
    finally:
        conn.close()


# ── Public API ────────────────────────────────────────────────────────────────

def build_context(failure: dict[str, Any], run_id: str) -> dict[str, Any]:
    """
    Build a structured context dict for the Claude AI engine.

    Args:
        failure: A failure dict produced by the GE checkpoint or dbt task.
                 Expected keys: check_name, check_type, column_name,
                 failure_rate, sample_bad_rows.
        run_id:  Airflow run_id string for this pipeline execution.

    Returns:
        A dict with all signal needed for Claude to diagnose the failure.
    """
    check_name: str = failure.get("check_name", "unknown")
    check_type: str = failure.get("check_type", "unknown")
    column_name: str = failure.get("column_name", "unknown")
    failure_rate: float = failure.get("failure_rate", 0.0)
    sample_bad_rows: list[dict] = failure.get("sample_bad_rows", [])

    log.info(
        "Building context for: check=%s  column=%s  failure_rate=%.4f",
        check_name,
        column_name,
        failure_rate,
    )

    # Serialize sample rows — truncate long string values to keep prompt tight
    sanitized_rows: list[dict] = []
    for row in sample_bad_rows[:10]:
        sanitized_rows.append(
            {
                k: (str(v)[:120] if isinstance(v, str) else v)
                for k, v in row.items()
            }
        )

    dbt_model_sql = _read_model_sql(column_name, check_type)
    recent_trend = _fetch_recent_trend(column_name, check_name)

    ctx: dict[str, Any] = {
        "pipeline_run_id": run_id,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "check_name": check_name,
        "check_type": check_type,
        "column_name": column_name,
        "failure_rate": round(failure_rate, 6),
        "failure_rate_pct": f"{failure_rate * 100:.2f}%",
        "sample_bad_rows": sanitized_rows,
        "sample_bad_rows_count": len(sanitized_rows),
        "dbt_model_sql": dbt_model_sql,
        "recent_trend_7d": recent_trend,
        "trend_available": len(recent_trend) > 0,
    }

    log.debug("Context built: %s", json.dumps(ctx, default=str)[:500])
    return ctx
