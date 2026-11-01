import os
import re
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

cfg = Config()
warehouse_id = os.environ["DATABRICKS_WAREHOUSE_ID"]
command_center_table = os.environ["COMMAND_CENTER_TABLE"]
patient_360_table = os.environ["PATIENT_360_TABLE"]
alerts_table = os.environ["ALERTS_TABLE"]
flow_table = os.environ["FLOW_TABLE"]

# Environment-supplied SQL identifiers must be three-part Unity Catalog names.
safe_table = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){2}$")
for table_name in (command_center_table, patient_360_table, alerts_table, flow_table):
    if not safe_table.fullmatch(table_name):
        raise ValueError("Expected a three-part Unity Catalog table name")


def connection():
    host = cfg.host.replace("https://", "").replace("http://", "")
    return sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
        _use_arrow_native_complex_types=False,
    )


def query(statement: str) -> pd.DataFrame:
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement)
            # Row-based SQL results work without optional PyArrow wheels.
            # This app queries bounded aggregate results (not patient-level data).
            columns = [col[0] for col in (cursor.description or [])]
            return pd.DataFrame.from_records(cursor.fetchall(), columns=columns)


st.set_page_config(page_title="CareFlow360", layout="wide")
st.title("CareFlow360 — Hospital Operations Command Center")
st.caption("Synthetic/public portfolio data only. Operational demo — not clinical decision support.")

try:
    kpi = query(f"SELECT * FROM {command_center_table} ORDER BY snapshot_ts DESC LIMIT 1")
    if not kpi.empty:
        r = kpi.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Active patients", int(r.get("active_patients", 0) or 0))
        c2.metric("ED patients", int(r.get("ed_patients", 0) or 0))
        c3.metric("ICU patients", int(r.get("icu_patients", 0) or 0))
        c4.metric("Operational alerts", int(r.get("operational_alert_patients", 0) or 0))
        snapshot = pd.to_datetime(r.get("snapshot_ts"), errors="coerce", utc=True)
        if pd.notna(snapshot):
            hours_old = (pd.Timestamp.now(tz="UTC") - snapshot).total_seconds() / 3600
            st.caption(f"Last KPI snapshot: {snapshot} ({hours_old:.1f} hours old)")
            if hours_old > 30:
                st.warning("KPI snapshot is stale (>30 hours). Do not treat counts as live.")

    st.subheader("Current department census")
    census = query(f"""
        SELECT COALESCE(current_department, 'Unknown') AS department,
               COUNT(*) AS patients
        FROM {patient_360_table}
        WHERE is_currently_admitted
        GROUP BY COALESCE(current_department, 'Unknown')
        ORDER BY patients DESC
    """)
    if not census.empty:
        st.bar_chart(census.set_index("department"))
    else:
        st.info("No active census rows yet.")

    left, right = st.columns(2)
    with left:
        st.subheader("Synthetic alert activity by hour (aggregated)")
        alerts = query(f"""
            SELECT DATE_TRUNC('HOUR', event_ts) AS event_hour,
                   source, COUNT(*) AS alert_records,
                   SUM(CASE WHEN fall_detected THEN 1 ELSE 0 END) AS fall_records
            FROM {alerts_table}
            GROUP BY DATE_TRUNC('HOUR', event_ts), source
            ORDER BY event_hour DESC
            LIMIT 100
        """)
        st.dataframe(alerts, use_container_width=True, hide_index=True)

    with right:
        st.subheader("Recent ADT flow")
        flow = query(f"""
            SELECT event_hour, event_type, SUM(event_count) AS events
            FROM {flow_table}
            GROUP BY event_hour, event_type
            ORDER BY event_hour DESC
            LIMIT 100
        """)
        st.dataframe(flow, use_container_width=True, hide_index=True)

except Exception as exc:
    st.error("Could not query CareFlow360 tables. Verify the SQL warehouse is running and app resources are bound.")
    # Detailed exceptions should stay in protected application logs.
