"""
app.py
------
Streamlit dashboard for the AI Data Quality Monitor.

Displays incidents written by the AI engine to the MotherDuck incidents table.
Connects to MotherDuck in prod (MOTHERDUCK_TOKEN set) or local DuckDB in dev.

Run locally:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="AI Data Quality Monitor",
    page_icon=":mag:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Constants ─────────────────────────────────────────────────────────────────
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "")
MD_DATABASE = "dq_monitor"
LOCAL_DB = "/tmp/dq_monitor.duckdb"

SEVERITY_COLORS = {
    "critical": "#FF4B4B",
    "warning":  "#FFA500",
    "info":     "#4B9EFF",
}
CONFIDENCE_COLORS = {
    "high":   "#21C354",
    "medium": "#FFA500",
    "low":    "#FF4B4B",
}

# ── Styles ────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    .metric-card {
        background: #1E1E2E;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
    }
    .severity-critical { color: #FF4B4B; font-weight: bold; }
    .severity-warning  { color: #FFA500; font-weight: bold; }
    .severity-info     { color: #4B9EFF; font-weight: bold; }
    .stDataFrame { font-size: 13px; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ── DB connection (cached for session) ────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a cached DuckDB connection for the session."""
    if MOTHERDUCK_TOKEN:
        conn = duckdb.connect(
            f"md:{MD_DATABASE}?motherduck_token={MOTHERDUCK_TOKEN}"
        )
    else:
        conn = duckdb.connect(LOCAL_DB)
    return conn


# ── Data loader ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner="Loading incidents...")
def load_incidents() -> pd.DataFrame:
    """
    Load all rows from the incidents table.

    Returns an empty DataFrame with the correct schema if the table doesn't
    exist yet (first run before any failures have occurred).
    """
    _schema = {
        "incident_id": pd.StringDtype(),
        "run_id": pd.StringDtype(),
        "check_name": pd.StringDtype(),
        "check_type": pd.StringDtype(),
        "column_name": pd.StringDtype(),
        "failure_rate": float,
        "root_cause": pd.StringDtype(),
        "confidence": pd.StringDtype(),
        "suggested_fix": pd.StringDtype(),
        "severity": pd.StringDtype(),
        "needs_human_review": bool,
        "created_at": "datetime64[ns]",
    }

    try:
        conn = get_connection()
        df = conn.execute(
            """
            SELECT
                incident_id,
                run_id,
                check_name,
                check_type,
                column_name,
                ROUND(failure_rate * 100, 2)  AS failure_rate_pct,
                failure_rate,
                root_cause,
                confidence,
                suggested_fix,
                severity,
                needs_human_review,
                created_at
            FROM incidents
            ORDER BY created_at DESC
            """
        ).df()
        df["created_at"] = pd.to_datetime(df["created_at"])
        return df
    except Exception:
        # Table doesn't exist yet
        return pd.DataFrame(columns=list(_schema.keys()) + ["failure_rate_pct"])


# ── Sidebar filters ───────────────────────────────────────────────────────────

def render_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    """Render sidebar filter controls and return the filtered DataFrame."""
    st.sidebar.title("Filters")

    # Date range
    today = date.today()
    default_start = today - timedelta(days=30)
    date_range = st.sidebar.date_input(
        "Date range",
        value=(default_start, today),
        min_value=date(2024, 1, 1),
        max_value=today,
    )

    # Severity multi-select
    all_severities = ["critical", "warning", "info"]
    severities = st.sidebar.multiselect(
        "Severity",
        options=all_severities,
        default=all_severities,
    )

    # Check type multi-select
    all_check_types = sorted(df["check_type"].dropna().unique().tolist()) if not df.empty else []
    check_types = st.sidebar.multiselect(
        "Check type",
        options=all_check_types,
        default=all_check_types,
    )

    # Needs human review toggle
    human_review_only = st.sidebar.checkbox("Needs human review only", value=False)

    st.sidebar.markdown("---")
    st.sidebar.caption("Data refreshes every 60 seconds.")
    if st.sidebar.button("Force refresh"):
        st.cache_data.clear()
        st.rerun()

    # Apply filters
    if df.empty:
        return df

    filtered = df.copy()

    if len(date_range) == 2:
        start_dt = pd.Timestamp(date_range[0])
        end_dt = pd.Timestamp(date_range[1]) + pd.Timedelta(days=1)
        filtered = filtered[
            (filtered["created_at"] >= start_dt) &
            (filtered["created_at"] < end_dt)
        ]

    if severities:
        filtered = filtered[filtered["severity"].isin(severities)]

    if check_types:
        filtered = filtered[filtered["check_type"].isin(check_types)]

    if human_review_only:
        filtered = filtered[filtered["needs_human_review"] == True]  # noqa: E712

    return filtered


# ── Top metrics row ───────────────────────────────────────────────────────────

def render_metrics(df: pd.DataFrame) -> None:
    """Render the four KPI metric cards at the top of the page."""
    total = len(df)
    critical_count = int((df["severity"] == "critical").sum()) if not df.empty else 0

    if not df.empty and "confidence" in df.columns:
        conf_map = {"high": 3, "medium": 2, "low": 1}
        avg_conf_num = df["confidence"].map(conf_map).mean()
        avg_conf = (
            "high" if avg_conf_num >= 2.5
            else "medium" if avg_conf_num >= 1.5
            else "low"
        )
    else:
        avg_conf = "N/A"

    last_run = (
        df["created_at"].max().strftime("%Y-%m-%d %H:%M UTC")
        if not df.empty
        else "No runs yet"
    )

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Incidents", total)
    with c2:
        st.metric("Critical", critical_count, delta=None)
    with c3:
        st.metric("Avg AI Confidence", avg_conf.capitalize() if avg_conf != "N/A" else "N/A")
    with c4:
        st.metric("Last Run", last_run)


# ── Trend chart ───────────────────────────────────────────────────────────────

def render_trend_chart(df: pd.DataFrame) -> None:
    """Bar chart: incidents per day over last 30 days, stacked by severity."""
    st.subheader("Incidents per Day (last 30 days)")

    if df.empty:
        st.info("No incidents in this period.")
        return

    trend = (
        df.assign(date=df["created_at"].dt.date)
        .groupby(["date", "severity"])
        .size()
        .reset_index(name="count")
    )

    # Ensure all severity levels appear in the legend
    all_dates = pd.date_range(
        end=date.today(), periods=30, freq="D"
    ).date
    base = pd.MultiIndex.from_product(
        [all_dates, ["critical", "warning", "info"]],
        names=["date", "severity"],
    )
    trend = (
        trend.set_index(["date", "severity"])
        .reindex(base, fill_value=0)
        .reset_index()
    )
    trend["date"] = pd.to_datetime(trend["date"])

    fig = px.bar(
        trend,
        x="date",
        y="count",
        color="severity",
        color_discrete_map=SEVERITY_COLORS,
        barmode="stack",
        labels={"count": "Incidents", "date": "Date", "severity": "Severity"},
        height=320,
    )
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )
    st.plotly_chart(fig, use_container_width=True)


# ── Incidents table ───────────────────────────────────────────────────────────

def _color_severity(val: str) -> str:
    colors = {"critical": "#FF4B4B", "warning": "#FFA500", "info": "#4B9EFF"}
    color = colors.get(str(val).lower(), "#FFFFFF")
    return f"color: {color}; font-weight: bold"


def _color_confidence(val: str) -> str:
    colors = {"high": "#21C354", "medium": "#FFA500", "low": "#FF4B4B"}
    color = colors.get(str(val).lower(), "#FFFFFF")
    return f"color: {color}"


def render_incidents_table(df: pd.DataFrame) -> int | None:
    """
    Render the sortable incidents table.

    Returns the index of the selected row (for detail expansion), or None.
    """
    st.subheader(f"Incidents ({len(df)} total)")

    if df.empty:
        st.info("No incidents match the current filters.")
        return None

    display_cols = [
        "created_at", "severity", "check_type", "check_name",
        "column_name", "failure_rate_pct", "confidence", "needs_human_review",
    ]
    display_df = df[display_cols].copy()
    display_df["created_at"] = display_df["created_at"].dt.strftime("%Y-%m-%d %H:%M")
    display_df["failure_rate_pct"] = display_df["failure_rate_pct"].astype(str) + "%"
    display_df["needs_human_review"] = display_df["needs_human_review"].map(
        {True: "Yes", False: "No"}
    )
    display_df.columns = [
        "Timestamp", "Severity", "Check Type", "Check Name",
        "Column", "Failure %", "Confidence", "Needs Review",
    ]

    styled = display_df.style.applymap(
        _color_severity, subset=["Severity"]
    ).applymap(
        _color_confidence, subset=["Confidence"]
    )

    # Sort controls
    sort_col, sort_dir = st.columns([2, 1])
    with sort_col:
        sort_by = st.selectbox(
            "Sort by",
            options=["Timestamp", "Severity", "Failure %", "Confidence"],
            index=0,
            label_visibility="collapsed",
        )
    with sort_dir:
        ascending = st.selectbox(
            "Direction",
            options=["Newest first", "Oldest first"],
            index=0,
            label_visibility="collapsed",
        ) == "Oldest first"

    sev_order = {"critical": 0, "warning": 1, "info": 2}
    if sort_by == "Severity":
        df = df.copy()
        df["_sev_order"] = df["severity"].map(sev_order)
        df = df.sort_values("_sev_order", ascending=ascending).drop(columns="_sev_order")
    elif sort_by == "Failure %":
        df = df.sort_values("failure_rate", ascending=ascending)
    elif sort_by == "Confidence":
        conf_order = {"high": 0, "medium": 1, "low": 2}
        df = df.copy()
        df["_conf_order"] = df["confidence"].map(conf_order)
        df = df.sort_values("_conf_order", ascending=ascending).drop(columns="_conf_order")
    else:
        df = df.sort_values("created_at", ascending=ascending)

    st.dataframe(
        df[display_cols].rename(columns={
            "created_at": "Timestamp", "severity": "Severity",
            "check_type": "Check Type", "check_name": "Check Name",
            "column_name": "Column", "failure_rate_pct": "Failure %",
            "confidence": "Confidence", "needs_human_review": "Needs Review",
        }),
        use_container_width=True,
        hide_index=True,
        height=360,
    )

    return df


# ── Incident detail expander ──────────────────────────────────────────────────

def render_incident_detail(df: pd.DataFrame) -> None:
    """
    Selectbox → expander showing full incident detail for the chosen row.
    """
    if df.empty:
        return

    st.subheader("Incident Detail")

    options = [
        f"{row['created_at'].strftime('%Y-%m-%d %H:%M')}  |  "
        f"{row['severity'].upper()}  |  {row['check_name']}  |  col: {row['column_name']}"
        for _, row in df.iterrows()
    ]
    selected_idx = st.selectbox(
        "Select an incident to inspect:",
        options=range(len(options)),
        format_func=lambda i: options[i],
        label_visibility="collapsed",
    )

    row = df.iloc[selected_idx]
    sev_color = SEVERITY_COLORS.get(row["severity"], "#FFFFFF")
    conf_color = CONFIDENCE_COLORS.get(row["confidence"], "#FFFFFF")

    with st.expander("Full incident report", expanded=True):
        m1, m2, m3, m4 = st.columns(4)
        m1.markdown(
            f"**Severity**\n\n"
            f"<span style='color:{sev_color};font-weight:bold'>{row['severity'].upper()}</span>",
            unsafe_allow_html=True,
        )
        m2.markdown(
            f"**Confidence**\n\n"
            f"<span style='color:{conf_color};font-weight:bold'>{row['confidence'].upper()}</span>",
            unsafe_allow_html=True,
        )
        m3.markdown(f"**Failure rate**\n\n{row['failure_rate_pct']}%")
        m4.markdown(
            f"**Needs review**\n\n{'Yes' if row['needs_human_review'] else 'No'}"
        )

        st.markdown("---")

        col_l, col_r = st.columns(2)
        with col_l:
            st.markdown("**Root Cause**")
            st.info(row["root_cause"])

        with col_r:
            st.markdown("**Suggested Fix**")
            st.code(row["suggested_fix"], language="sql")

        st.markdown("---")
        meta1, meta2, meta3 = st.columns(3)
        meta1.markdown(f"**Check name**\n\n`{row['check_name']}`")
        meta2.markdown(f"**Column**\n\n`{row['column_name']}`")
        meta3.markdown(f"**Check type**\n\n`{row['check_type']}`")

        st.markdown(f"**Run ID:** `{row['run_id']}`")
        st.markdown(f"**Incident ID:** `{row['incident_id']}`")
        st.markdown(f"**Timestamp:** {row['created_at'].strftime('%Y-%m-%d %H:%M:%S UTC')}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    """Entry point for the Streamlit dashboard."""
    st.title("AI Data Quality Monitor")
    st.caption(
        "Powered by Apache Airflow · dbt · Great Expectations · Claude AI · MotherDuck"
    )

    raw_df = load_incidents()
    filtered_df = render_sidebar(raw_df)

    st.markdown("---")
    render_metrics(filtered_df)

    st.markdown("---")
    render_trend_chart(filtered_df)

    st.markdown("---")
    render_incidents_table(filtered_df)

    st.markdown("---")
    render_incident_detail(filtered_df)


if __name__ == "__main__":
    main()
