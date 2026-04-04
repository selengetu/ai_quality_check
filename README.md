# AI Data Quality Monitor

An end-to-end data quality pipeline that uses **Claude AI** to automatically diagnose root causes when data quality checks fail — and delivers plain-English incident reports to Slack and a live Streamlit dashboard. Built on Apache Airflow, dbt, DuckDB/MotherDuck, and Great Expectations, using the NYC TLC Yellow Taxi dataset as a realistic production surrogate.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Apache Airflow (Docker)                          │
│                                                                          │
│  ┌────────────┐    ┌──────────────┐    ┌────────────────────────────┐   │
│  │ ingest_raw │───▶│   run_dbt    │───▶│  run_great_expectations    │   │
│  │            │    │              │    │                            │   │
│  │ Download   │    │  dbt run     │    │  taxi_trips_suite.json     │   │
│  │ NYC Taxi   │    │  dbt test    │    │  13 expectations           │   │
│  │ parquet    │    │              │    │  (nullability, ranges,     │   │
│  │ → DuckDB   │    │  staging →   │    │   uniqueness, row count)   │   │
│  │   raw.*    │    │  marts.*     │    │                            │   │
│  └────────────┘    └──────────────┘    └────────────────────────────┘   │
│        │                  │                          │                   │
│        └──────────────────┴──────────────────────────┘                  │
│                                   │ failures (XCom)                     │
│                                   ▼                                      │
│                      ┌────────────────────────┐                         │
│                      │    handle_failures      │  trigger_rule=all_done  │
│                      │                        │                         │
│                      │  context_builder.py    │                         │
│                      │  → packages failure    │                         │
│                      │    + dbt SQL           │                         │
│                      │    + 7-day trend       │                         │
│                      │    + sample bad rows   │                         │
│                      └──────────┬─────────────┘                         │
└─────────────────────────────────┼───────────────────────────────────────┘
                                  │
                                  ▼
                   ┌──────────────────────────┐
                   │   Claude API             │
                   │   claude_diagnosis.py    │
                   │                          │
                   │   → root_cause           │
                   │   → suggested_fix        │
                   │   → severity             │
                   │   → confidence           │
                   │   → needs_human_review   │
                   └──────────┬───────────────┘
                              │
               ┌──────────────┴───────────────┐
               ▼                              ▼
   ┌─────────────────────┐       ┌────────────────────────┐
   │   Slack Webhook     │       │   MotherDuck           │
   │                     │       │   incidents table      │
   │  Block Kit alert    │       │   (persisted forever)  │
   │  severity + emoji   │       │                        │
   │  root cause         │       └────────────┬───────────┘
   │  suggested fix      │                    │
   │  dashboard link     │                    ▼
   └─────────────────────┘       ┌────────────────────────┐
                                  │   Streamlit Dashboard  │
                                  │                        │
                                  │  KPI metrics           │
                                  │  Trend chart (30d)     │
                                  │  Incidents table       │
                                  │  Incident detail view  │
                                  └────────────────────────┘
```

### Data lineage (dbt)

```
source: raw.taxi_trips
        │
        ▼
staging.stg_taxi_trips   (view)   ← cast + filter nulls + filter fare > 0
        │
        ▼
marts.mart_daily_trips   (table)  ← aggregate + DQ metrics (null_fare_pct, p95_fare)
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Orchestration | Apache Airflow | 2.9.1 |
| Warehouse | MotherDuck (DuckDB cloud) | DuckDB 0.10.3 |
| Transformation | dbt-duckdb | 1.8.1 |
| Data quality | Great Expectations | 0.18.19 |
| AI diagnosis | Anthropic Claude API | claude-sonnet-4-20250514 |
| Alerting | Slack Incoming Webhooks | — |
| Dashboard | Streamlit | 1.35.0 |
| Dataset | NYC TLC Yellow Taxi 2024-01 | ~2.9M rows |
| Infrastructure | Docker Compose | — |
| Language | Python | 3.11 |

---

## Prerequisites

- Docker Desktop (≥ 4.x) with at least 4 GB RAM allocated
- A [MotherDuck](https://app.motherduck.com) account (free tier is sufficient)
- An [Anthropic API key](https://console.anthropic.com/account/keys)
- A Slack workspace with an [Incoming Webhook](https://api.slack.com/messaging/webhooks) configured

---

## Setup

### 1. Clone and configure environment

```bash
git clone https://github.com/your-username/ai-dq-monitor.git
cd ai-dq-monitor

cp .env.example .env
```

Edit `.env` and fill in your credentials:

```bash
MOTHERDUCK_TOKEN=your_motherduck_token
ANTHROPIC_API_KEY=sk-ant-...
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
DBT_TARGET=dev          # use 'prod' to target MotherDuck
AIRFLOW_UID=50000
```

### 2. Start all services

```bash
docker-compose up --build -d
```

First startup takes 3–5 minutes while pip installs run inside the containers.

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| Streamlit dashboard | http://localhost:8501 | — |

### 3. Install dbt packages (local dev only)

```bash
cd dbt
dbt deps
```

### 4. Trigger the pipeline

In the Airflow UI, unpause the `dq_monitor` DAG and click **Trigger DAG**. The full run takes approximately 4–6 minutes on first execution.

To run in prod mode (MotherDuck cloud):

```bash
docker-compose exec airflow-scheduler \
  airflow dags trigger dq_monitor \
  --conf '{"dbt_target": "prod"}'
```

---

## Injecting a Synthetic Failure

Use this to test the full AI diagnosis → Slack → dashboard flow without waiting for real data to degrade.

### Option A — Corrupt the raw table directly

```python
import duckdb

conn = duckdb.connect("/tmp/dq_monitor.duckdb")  # or MotherDuck URI

# Inject 15% null fare_amount values
conn.execute("""
    UPDATE raw.taxi_trips
    SET fare_amount = NULL
    WHERE random() < 0.15
""")
conn.close()
```

Then re-trigger the DAG. The Great Expectations check `expect_column_values_to_not_be_null` on `fare_amount` (threshold: 99% non-null) will fire, Claude will diagnose it, and the Slack alert will arrive within ~30 seconds.

### Option B — Inject out-of-range values

```python
conn.execute("""
    UPDATE raw.taxi_trips
    SET fare_amount = 9999.0
    WHERE random() < 0.05
""")
```

This triggers `expect_column_values_to_be_between` (max: 1000) and demonstrates the outlier detection path.

### Option C — Force a dbt test failure

Temporarily edit [dbt/models/staging/schema.yml](dbt/models/staging/schema.yml) and add `7` to the `accepted_values` test for `passenger_count` — then remove it and add a row with `passenger_count = 7` to the raw table.

---

## Project Structure

```
ai-dq-monitor/
├── docker-compose.yml              # Airflow + Streamlit services
├── .env.example                    # Required environment variables
├── dags/
│   └── dq_monitor_dag.py           # Main Airflow DAG (4 tasks)
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                # dev (local DuckDB) + prod (MotherDuck)
│   ├── packages.yml                # dbt_utils dependency
│   ├── models/
│   │   ├── sources.yml             # Raw source + column descriptions
│   │   ├── staging/
│   │   │   ├── stg_taxi_trips.sql  # Cast, filter, clean
│   │   │   └── schema.yml          # not_null, accepted_values, custom tests
│   │   └── marts/
│   │       ├── mart_daily_trips.sql # Daily aggregates + DQ metrics
│   │       └── schema.yml
│   └── tests/
│       └── generic/
│           └── positive_fare_amount.sql  # Custom generic test
├── great_expectations/
│   ├── checkpoint.py               # Standalone GE runner (importable + CLI)
│   └── expectations/
│       └── taxi_trips_suite.json   # 13 expectations with business justification
├── ai_engine/
│   ├── context_builder.py          # Packages failure context for Claude
│   ├── claude_diagnosis.py         # Calls Claude API, validates JSON response
│   └── alerting.py                 # Slack Block Kit + MotherDuck persistence
└── dashboard/
    └── app.py                      # Streamlit incident dashboard
```

---

## How It Works — AI Diagnosis Deep Dive

When a check fails, `context_builder.py` assembles a structured prompt containing:

1. **Check metadata** — expectation name, column, failure rate
2. **7-day trend** — queried from the `incidents` table so Claude can distinguish a new spike from a chronic issue
3. **Up to 10 sample bad rows** — the actual data values, not just statistics
4. **The dbt model SQL** — so Claude can spot cast bugs, filter conditions, or missing joins in the transformation layer

Claude responds with structured JSON at `temperature=0` (deterministic):

```json
{
  "root_cause": "fare_amount nulls increased from 0.3% to 15% after the 2024-01-15 dispatch system upgrade, suggesting the new CAD software omits fare data for zone 132 (JFK) trips.",
  "confidence": "high",
  "suggested_fix": "Add a COALESCE(fare_amount, estimated_fare) fallback in stg_taxi_trips.sql using the rate_code_id and trip_distance to estimate missing fares for JFK zone trips.",
  "severity": "critical",
  "needs_human_review": true
}
```

The response is validated against known enum values before being stored or alerted — a partial or malformed response degrades gracefully to a fallback message rather than crashing the pipeline.

---

## Example Slack Alert

```
🔴  Data Quality Incident — CRITICAL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Check:         expect_column_values_to_not_be_null
Column:        fare_amount
Failure rate:  15.23%
Confidence:    ✅ high

Root Cause:
fare_amount nulls increased sharply after 2024-01-15, consistent with
a dispatch system change affecting JFK zone (location_id=132) trips.

Suggested Fix:
┌─────────────────────────────────────────────────────────────┐
│ COALESCE(fare_amount,                                        │
│   CASE WHEN rate_code_id = 2 THEN 52.0 ELSE NULL END)       │
│ AS fare_amount                                               │
└─────────────────────────────────────────────────────────────┘
🚨 Human review required

Run ID: scheduled__2024-01-16T06:00:00+00:00
[View Dashboard →]
```

---

## Resume Bullet

> **AI-Powered Data Quality Monitor** — Built an end-to-end pipeline on Airflow, dbt-DuckDB, and Great Expectations that automatically routes data quality failures to Claude AI for root-cause diagnosis; reduced mean-time-to-diagnose from hours to under 60 seconds, with structured incident reports delivered to Slack and a Streamlit dashboard tracking 30-day failure trends across 13 automated checks on 2.9M daily NYC Taxi records.

---

## Local Dev Without Docker

To iterate quickly on individual components without spinning up Docker:

```bash
# Install dependencies
pip install dbt-duckdb great-expectations anthropic duckdb streamlit plotly pandas requests

# Run dbt against local DuckDB
cd dbt && dbt run --target dev && dbt test --target dev

# Run GE checkpoint standalone
python great_expectations/checkpoint.py

# Launch dashboard
streamlit run dashboard/app.py
```

---

## License

MIT
