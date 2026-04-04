"""
dq_monitor_dag.py
-----------------
Main Airflow DAG for the AI-powered data quality monitor.

Pipeline:
  1. Download NYC Taxi parquet from S3 (public bucket, no auth required)
  2. Load raw data into MotherDuck raw schema
  3. Run dbt transformations (staging → marts)
  4. Run Great Expectations validation suite
  5. On any failure → AI diagnosis → Slack alert → store incident

Schedule: daily at 06:00 UTC
Idempotent: all steps check-before-insert / replace existing data.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
DAG_ID = "dq_monitor"
TAXI_S3_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2024-01.parquet"
)
# MotherDuck uses the md: URI scheme; local dev uses a file path
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "")
MD_DATABASE = "dq_monitor"
RAW_TABLE = "raw.taxi_trips"
DBT_DIR = Path("/opt/airflow/dbt")
GE_DIR = Path("/opt/airflow/great_expectations")

default_args: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ── Helper: get DuckDB connection ─────────────────────────────────────────────

def _get_conn(use_motherduck: bool = True):
    """
    Return a duckdb.DuckDBPyConnection.

    In prod (use_motherduck=True) connects to MotherDuck cloud.
    In dev falls back to a local file so the DAG is runnable without a token.
    """
    import duckdb  # imported here so Airflow workers with duckdb installed work

    if use_motherduck and MOTHERDUCK_TOKEN:
        conn = duckdb.connect(f"md:{MD_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}")
        log.info("Connected to MotherDuck: md:%s", MD_DATABASE)
    else:
        local_path = "/tmp/dq_monitor.duckdb"
        conn = duckdb.connect(local_path)
        log.info("Connected to local DuckDB: %s", local_path)

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL parquet; LOAD parquet;")
    return conn


# ── Task 1: ingest_raw ────────────────────────────────────────────────────────

def ingest_raw(**context: Any) -> None:
    """
    Download the NYC Taxi parquet file and load it into the raw schema.

    Uses a temp file to avoid persisting the ~500 MB parquet locally.
    Replaces the table on each run (idempotent via CREATE OR REPLACE).
    """
    import urllib.request

    run_id: str = context["run_id"]
    log.info("Run ID: %s — starting raw ingestion", run_id)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        log.info("Downloading taxi data from: %s", TAXI_S3_URL)
        urllib.request.urlretrieve(TAXI_S3_URL, tmp_path)  # noqa: S310
        log.info("Download complete: %s", tmp_path)

        conn = _get_conn()
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw;")
            conn.execute(
                f"""
                CREATE OR REPLACE TABLE {RAW_TABLE} AS
                SELECT
                    *,
                    '{run_id}' AS _run_id,
                    NOW()       AS _loaded_at
                FROM read_parquet('{tmp_path}')
                """
            )
            row_count = conn.execute(f"SELECT COUNT(*) FROM {RAW_TABLE}").fetchone()[0]
            log.info("Loaded %d rows into %s", row_count, RAW_TABLE)

            # Push row count to XCom for downstream tasks
            context["ti"].xcom_push(key="raw_row_count", value=row_count)
        finally:
            conn.close()
    except Exception as exc:
        log.error("Raw ingestion failed: %s", exc)
        raise
    finally:
        Path(tmp_path).unlink(missing_ok=True)


# ── Task 2: run_dbt ───────────────────────────────────────────────────────────

def run_dbt(**context: Any) -> None:
    """
    Execute `dbt run` followed by `dbt test` inside the dbt project directory.

    Failures surface as subprocess.CalledProcessError and are caught to trigger
    the AI diagnosis branch via XCom.
    """
    dbt_target = os.environ.get("DBT_TARGET", "dev")
    log.info("Running dbt with target: %s", dbt_target)

    failures: list[dict[str, Any]] = []

    for dbt_cmd in [["dbt", "run", "--target", dbt_target],
                    ["dbt", "test", "--target", dbt_target]]:
        try:
            result = subprocess.run(
                dbt_cmd,
                cwd=str(DBT_DIR),
                capture_output=True,
                text=True,
                check=True,
                env={**os.environ},
            )
            log.info("dbt output:\n%s", result.stdout)
        except subprocess.CalledProcessError as exc:
            log.error("dbt command failed: %s\n%s", " ".join(dbt_cmd), exc.stdout)
            failures.append(
                {
                    "check_name": " ".join(dbt_cmd),
                    "check_type": "dbt",
                    "error_output": exc.stdout[-3000:],  # last 3 k chars
                    "column_name": "unknown",
                    "failure_rate": 1.0,
                    "sample_bad_rows": [],
                }
            )

    context["ti"].xcom_push(key="dbt_failures", value=failures)

    if failures:
        raise RuntimeError(f"dbt had {len(failures)} failure(s). AI diagnosis queued.")


# ── Task 3: run_great_expectations ───────────────────────────────────────────

def run_great_expectations(**context: Any) -> None:
    """
    Run the Great Expectations checkpoint against the MotherDuck/local staging table.

    Delegates to great_expectations/checkpoint.py which owns all expectation
    evaluation logic. Pushes structured failure dicts to XCom for the AI engine.
    """
    sys.path.insert(0, "/opt/airflow")
    try:
        from great_expectations.checkpoint import run_checkpoint  # type: ignore[import]
    except ImportError as exc:
        log.error("Could not import GE checkpoint module: %s", exc)
        context["ti"].xcom_push(key="ge_failures", value=[])
        return

    run_id: str = context["run_id"]

    try:
        result = run_checkpoint(run_id=run_id)
    except RuntimeError as exc:
        # Staging table not yet created (e.g. dbt failed upstream)
        log.warning("GE checkpoint skipped: %s", exc)
        context["ti"].xcom_push(key="ge_failures", value=[])
        return

    failures = result.get("failures", [])
    context["ti"].xcom_push(key="ge_failures", value=failures)

    log.info(
        "GE checkpoint: %d passed, %d failed (total=%d)",
        result["passed"],
        result["failed"],
        result["total_expectations"],
    )

    if not result["success"]:
        raise RuntimeError(f"Great Expectations had {result['failed']} failure(s).")


# ── Task 4: handle_failures ───────────────────────────────────────────────────

def handle_failures(**context: Any) -> None:
    """
    Collect failures from dbt and GE XCom values, call the AI engine for each,
    send Slack alerts, and persist incidents to MotherDuck.

    This task runs regardless of upstream failure (trigger_rule=ALL_DONE) so
    that we capture failures even when upstream tasks raise exceptions.
    """
    ti = context["ti"]
    run_id: str = context["run_id"]

    dbt_failures: list[dict] = ti.xcom_pull(task_ids="run_dbt", key="dbt_failures") or []
    ge_failures: list[dict] = ti.xcom_pull(task_ids="run_great_expectations", key="ge_failures") or []

    all_failures = dbt_failures + ge_failures

    if not all_failures:
        log.info("No failures detected — pipeline run clean.")
        return

    log.info("Processing %d failure(s) through AI engine", len(all_failures))

    # Import AI engine modules (mounted at /opt/airflow/ai_engine)
    sys.path.insert(0, "/opt/airflow")
    from ai_engine.context_builder import build_context  # type: ignore[import]
    from ai_engine.claude_diagnosis import diagnose  # type: ignore[import]
    from ai_engine.alerting import send_alert  # type: ignore[import]

    for failure in all_failures:
        try:
            ctx = build_context(failure, run_id=run_id)
            diagnosis = diagnose(ctx)
            send_alert(ctx, diagnosis, run_id=run_id)
            log.info(
                "Incident processed: %s | severity=%s",
                failure["check_name"],
                diagnosis.get("severity", "unknown"),
            )
        except Exception as exc:
            log.error("Failed to process incident for '%s': %s", failure["check_name"], exc)


# ── DAG definition ─────────────────────────────────────────────────────────────

with DAG(
    dag_id=DAG_ID,
    description="AI-powered data quality monitor for NYC Taxi data",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # daily at 06:00 UTC
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "ai", "dbt", "great-expectations"],
) as dag:

    t_ingest = PythonOperator(
        task_id="ingest_raw",
        python_callable=ingest_raw,
    )

    t_dbt = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt,
    )

    t_ge = PythonOperator(
        task_id="run_great_expectations",
        python_callable=run_great_expectations,
    )

    t_handle = PythonOperator(
        task_id="handle_failures",
        python_callable=handle_failures,
        trigger_rule="all_done",  # runs even if upstream tasks fail
    )

    t_ingest >> t_dbt >> t_ge >> t_handle
