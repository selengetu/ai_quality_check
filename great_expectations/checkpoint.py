"""
checkpoint.py
-------------
Runs the taxi_trips_suite.json expectations against the staging table in
MotherDuck (or local DuckDB in dev mode) and returns a structured result
dict consumable by the AI engine.

Can be executed standalone:
    python great_expectations/checkpoint.py

Or imported and called from the Airflow DAG task.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_HERE = Path(__file__).parent
SUITE_PATH = _HERE / "expectations" / "taxi_trips_suite.json"

# ── Connection ────────────────────────────────────────────────────────────────
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "")
MD_DATABASE = "dq_monitor"
STAGING_TABLE = "staging.stg_taxi_trips"
SAMPLE_LIMIT = 50_000  # rows pulled for in-memory validation


def _get_conn():
    """Return a DuckDB connection (MotherDuck in prod, local file in dev)."""
    import duckdb

    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(
            f"md:{MD_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}"
        )
        log.info("GE checkpoint: connected to MotherDuck md:%s", MD_DATABASE)
    else:
        local_path = "/tmp/dq_monitor.duckdb"
        conn = duckdb.connect(local_path)
        log.info("GE checkpoint: connected to local DuckDB %s", local_path)

    return conn


# ── Expectation evaluators ────────────────────────────────────────────────────

def _eval_not_null(
    df: Any, col: str, mostly: float
) -> dict[str, Any]:
    null_mask = df[col].isna()
    null_rate = float(null_mask.mean())
    failure_rate = null_rate
    success = null_rate <= (1.0 - mostly)
    bad_rows = df[null_mask].head(10).to_dict(orient="records")
    return {
        "success": success,
        "failure_rate": round(failure_rate, 6),
        "observed_value": round(null_rate, 6),
        "sample_bad_rows": bad_rows,
    }


def _eval_between(
    df: Any, col: str, min_value: float | None, max_value: float | None, mostly: float
) -> dict[str, Any]:
    import pandas as pd

    mask = pd.Series([False] * len(df), index=df.index)
    if min_value is not None:
        mask |= df[col] < min_value
    if max_value is not None:
        mask |= df[col] > max_value
    # also flag nulls as out-of-range
    mask |= df[col].isna()

    failure_rate = float(mask.mean())
    success = failure_rate <= (1.0 - mostly)
    bad_rows = df[mask].head(10).to_dict(orient="records")
    return {
        "success": success,
        "failure_rate": round(failure_rate, 6),
        "observed_value": round(failure_rate, 6),
        "sample_bad_rows": bad_rows,
    }


def _eval_row_count(df: Any, min_value: int) -> dict[str, Any]:
    count = len(df)
    success = count >= min_value
    return {
        "success": success,
        "failure_rate": 0.0 if success else 1.0,
        "observed_value": count,
        "sample_bad_rows": [],
    }


def _eval_unique_proportion(
    df: Any, col: str, min_value: float
) -> dict[str, Any]:
    proportion = df[col].nunique() / max(len(df), 1)
    success = proportion >= min_value
    return {
        "success": success,
        "failure_rate": 0.0 if success else round(1.0 - proportion, 6),
        "observed_value": round(proportion, 6),
        "sample_bad_rows": [],
    }


_EVALUATORS = {
    "expect_column_values_to_not_be_null": lambda df, kw: _eval_not_null(
        df, kw["column"], kw.get("mostly", 1.0)
    ),
    "expect_column_values_to_be_between": lambda df, kw: _eval_between(
        df,
        kw["column"],
        kw.get("min_value"),
        kw.get("max_value"),
        kw.get("mostly", 1.0),
    ),
    "expect_table_row_count_to_be_between": lambda df, kw: _eval_row_count(
        df, kw.get("min_value", 0)
    ),
    "expect_column_proportion_of_unique_values_to_be_between": lambda df, kw: _eval_unique_proportion(
        df, kw["column"], kw.get("min_value", 0.0)
    ),
}


# ── Main checkpoint runner ────────────────────────────────────────────────────

def run_checkpoint(run_id: str = "manual") -> dict[str, Any]:
    """
    Run all expectations in taxi_trips_suite.json against the staging table.

    Args:
        run_id: Airflow run_id (or "manual" for standalone execution).

    Returns:
        {
            "success": bool,
            "run_id": str,
            "table": str,
            "total_expectations": int,
            "passed": int,
            "failed": int,
            "failures": [
                {
                    "check_name": str,
                    "check_type": "great_expectations",
                    "column_name": str,
                    "failure_rate": float,
                    "observed_value": float | int,
                    "sample_bad_rows": list[dict],
                },
                ...
            ],
        }
    """
    import pandas as pd

    if not SUITE_PATH.exists():
        raise FileNotFoundError(f"GE suite not found: {SUITE_PATH}")

    with open(SUITE_PATH) as f:
        suite = json.load(f)

    conn = _get_conn()
    try:
        log.info("Loading %s (limit %d rows) for GE validation", STAGING_TABLE, SAMPLE_LIMIT)
        try:
            df: pd.DataFrame = conn.execute(
                f"SELECT * FROM {STAGING_TABLE} LIMIT {SAMPLE_LIMIT}"
            ).df()
        except Exception as exc:
            raise RuntimeError(
                f"Could not query {STAGING_TABLE}. "
                "Run dbt first to create the staging view. "
                f"Error: {exc}"
            ) from exc

        log.info("Loaded %d rows for validation", len(df))

        failures: list[dict[str, Any]] = []
        passed = 0

        for expectation in suite.get("expectations", []):
            exp_type: str = expectation["expectation_type"]
            kwargs: dict = expectation.get("kwargs", {})
            col: str = kwargs.get("column", "table")

            evaluator = _EVALUATORS.get(exp_type)
            if evaluator is None:
                log.warning("No evaluator for expectation type '%s' — skipping", exp_type)
                continue

            try:
                result = evaluator(df, kwargs)
            except Exception as exc:
                log.error("Error evaluating %s on column '%s': %s", exp_type, col, exc)
                continue

            if result["success"]:
                log.info("PASS  %s  column=%s", exp_type, col)
                passed += 1
            else:
                log.warning(
                    "FAIL  %s  column=%s  failure_rate=%.4f",
                    exp_type,
                    col,
                    result["failure_rate"],
                )
                failures.append(
                    {
                        "check_name": exp_type,
                        "check_type": "great_expectations",
                        "column_name": col,
                        "failure_rate": result["failure_rate"],
                        "observed_value": result["observed_value"],
                        "sample_bad_rows": result["sample_bad_rows"],
                        "run_id": run_id,
                    }
                )

        total = passed + len(failures)
        overall_success = len(failures) == 0

        log.info(
            "GE checkpoint complete: %d/%d passed, %d failed",
            passed,
            total,
            len(failures),
        )

        return {
            "success": overall_success,
            "run_id": run_id,
            "table": STAGING_TABLE,
            "total_expectations": total,
            "passed": passed,
            "failed": len(failures),
            "failures": failures,
        }

    finally:
        conn.close()


# ── Standalone entry point ────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
    )

    result = run_checkpoint(run_id="manual")

    print(json.dumps(result, indent=2, default=str))

    if not result["success"]:
        print(f"\n{result['failed']} expectation(s) failed.", file=sys.stderr)
        sys.exit(1)
    else:
        print("\nAll expectations passed.")
