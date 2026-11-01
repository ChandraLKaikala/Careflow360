"""Synthetic hospital operations command center for Databricks Apps.

Only warehouse-backed aggregate results are rendered. No patient-level records
are fetched; this demo is not clinical decision support.
"""

from __future__ import annotations

import logging
import os

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

from app_helpers import AppSettings, build_queries, warehouse_hostname


LOG = logging.getLogger(__name__)
CACHE_TTL_SECONDS = 120
LOOKBACK_DAYS = 7
ROW_LIMIT = 100

st.set_page_config(
    page_title="CareFlow360 | Command Center",
    page_icon="🏥",
    layout="wide",
)
st.title("CareFlow360")
st.subheader("Hospital Operations Command Center")
st.caption(
    "Synthetic/public portfolio data only · Refreshed by a Databricks job, "
    "not continuous monitoring · Not clinical decision support"
)

try:
    settings = AppSettings.from_env(os.environ)
    cfg = Config()
    hostname = warehouse_hostname(cfg.host)
except Exception:
    LOG.exception("CareFlow360 App configuration failed")
    st.error(
        "App resources or workspace authentication are not configured. "
        "Check the app bindings and service-principal permissions."
    )
    st.stop()

queries = build_queries(settings, lookback_days=LOOKBACK_DAYS, row_limit=ROW_LIMIT)


@st.cache_data(ttl=CACHE_TTL_SECONDS, max_entries=32, show_spinner=False)
def query(statement: str, server_hostname: str, warehouse_id: str) -> pd.DataFrame:
    """Cache bounded result data, never SQL connections or authentication objects."""
    with sql.connect(
        server_hostname=server_hostname,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)
            columns = [column[0] for column in (cursor.description or [])]
            return pd.DataFrame.from_records(cursor.fetchall(), columns=columns)


def load_section(name: str, statement: str) -> pd.DataFrame | None:
    try:
        return query(statement, hostname, settings.warehouse_id)
    except Exception:
        LOG.exception("Failed to load dashboard section: %s", name)
        st.warning(
            f"{name} is temporarily unavailable. "
            "Check the SQL warehouse and bound table permissions."
        )
        return None


def count_label(value: object) -> str:
    """Display nullable Spark aggregate counts without throwing on missing values."""
    if value is None:
        return "0"
    try:
        if pd.isna(value):
            return "0"
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return "—"
    return f"{number:,}" if number >= 0 else "—"


top_left, top_right = st.columns([4, 1])
with top_left:
    st.caption(
        f"Recent event windows: {LOOKBACK_DAYS} days · "
        f"Up to {ROW_LIMIT} aggregated groups per section · "
        f"SQL query cache: {CACHE_TTL_SECONDS} seconds"
    )
with top_right:
    if st.button("Refresh dashboard", use_container_width=True):
        query.clear()
        st.caption("Cached query results cleared.")

st.subheader("Current operational snapshot")
kpi = load_section("Operational snapshot", queries.kpi)
if kpi is not None and not kpi.empty:
    latest = kpi.iloc[0]
    metrics = (
        ("active_patients", "Active patients"),
        ("ed_patients", "ED patients"),
        ("icu_patients", "ICU patients"),
        ("operational_alert_patients", "Operational alert patients"),
    )
    for col, (field, label) in zip(st.columns(4), metrics):
        with col:
            st.metric(label, count_label(latest.get(field)))

    snapshot = pd.to_datetime(latest.get("snapshot_ts"), errors="coerce", utc=True)
    if pd.isna(snapshot):
        st.warning("The KPI snapshot timestamp is missing or invalid.")
    else:
        age_hours = (pd.Timestamp.now(tz="UTC") - snapshot).total_seconds() / 3600
        st.caption(
            f"Snapshot updated: {snapshot:%Y-%m-%d %H:%M UTC} "
            f"({age_hours:.1f} hours ago)"
        )
        if age_hours < -1:
            st.warning("Snapshot timestamp is in the future. Verify workspace clocks.")
        elif age_hours > 30:
            st.warning("KPI snapshot is over 30 hours old; values may be stale.")
elif kpi is not None:
    st.info("No operational KPI snapshot has been created yet.")

st.subheader("Current department census")
census = load_section("Department census", queries.census)
if census is not None:
    if census.empty:
        st.info("No currently admitted patients in the synthetic dataset.")
    else:
        census = census.copy()
        census["patients"] = pd.to_numeric(census["patients"], errors="coerce").fillna(0)
        st.bar_chart(census.set_index("department")["patients"])

left, right = st.columns(2)
with left:
    st.subheader("Synthetic operational alerts")
    st.caption(f"Latest {ROW_LIMIT} hourly/source groups in the past {LOOKBACK_DAYS} days.")
    alerts = load_section("Alert activity", queries.alerts)
    if alerts is not None:
        if alerts.empty:
            st.info("No synthetic alert records in the selected time window.")
        else:
            alerts = alerts.copy()
            alerts["event_hour"] = pd.to_datetime(
                alerts["event_hour"], errors="coerce", utc=True
            )
            alerts["alert_records"] = pd.to_numeric(
                alerts["alert_records"], errors="coerce"
            ).fillna(0)
            hourly = (
                alerts.dropna(subset=["event_hour"])
                .groupby("event_hour")["alert_records"]
                .sum()
                .sort_index()
            )
            if not hourly.empty:
                st.line_chart(hourly)
            with st.expander("View aggregated alert records"):
                st.dataframe(alerts, use_container_width=True, hide_index=True)

with right:
    st.subheader("ADT patient flow")
    st.caption(f"Latest {ROW_LIMIT} hourly/event groups in the past {LOOKBACK_DAYS} days.")
    flow = load_section("ADT patient flow", queries.flow)
    if flow is not None:
        if flow.empty:
            st.info("No ADT flow records in the selected time window.")
        else:
            flow = flow.copy()
            flow["event_hour"] = pd.to_datetime(
                flow["event_hour"], errors="coerce", utc=True
            )
            flow["events"] = pd.to_numeric(flow["events"], errors="coerce").fillna(0)
            hourly_flow = flow.dropna(subset=["event_hour"]).pivot_table(
                index="event_hour",
                columns="event_type",
                values="events",
                aggfunc="sum",
                fill_value=0,
            ).sort_index()
            if not hourly_flow.empty:
                st.bar_chart(hourly_flow)
            with st.expander("View aggregated flow records"):
                st.dataframe(flow, use_container_width=True, hide_index=True)

st.divider()
st.caption(
    "Portfolio demonstration only. Trends reflect the latest returned aggregate "
    "groups, not an exhaustive clinical monitoring feed."
)
