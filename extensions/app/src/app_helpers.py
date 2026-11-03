"""Validated configuration and bounded read-only SQL for the demo dashboard.

This module has no Streamlit or Databricks dependencies so it can be tested
offline. Only aggregate synthetic data is supported.
"""
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Mapping
from urllib.parse import urlsplit


_SAFE_TABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){2}\Z")
_SAFE_WAREHOUSE = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")


@dataclass(frozen=True)
class AppSettings:
    warehouse_id: str
    command_center_table: str
    patient_360_table: str
    alerts_table: str
    flow_table: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "AppSettings":
        names = (
            "DATABRICKS_WAREHOUSE_ID",
            "COMMAND_CENTER_TABLE",
            "PATIENT_360_TABLE",
            "ALERTS_TABLE",
            "FLOW_TABLE",
        )
        missing = [name for name in names if not env.get(name, "").strip()]
        if missing:
            raise ValueError(f"Missing app resource bindings: {', '.join(missing)}")
        warehouse_id = env["DATABRICKS_WAREHOUSE_ID"].strip()
        if not _SAFE_WAREHOUSE.fullmatch(warehouse_id):
            raise ValueError("Invalid SQL warehouse ID")
        tables = [env[name].strip() for name in names[1:]]
        if not all(_SAFE_TABLE.fullmatch(table) for table in tables):
            raise ValueError("Expected safe three-part Unity Catalog table names")
        return cls(warehouse_id, *tables)


def warehouse_hostname(host: str) -> str:
    """Derive the SQL hostname from a plain Databricks HTTPS workspace URL."""
    parsed = urlsplit(host or "")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Invalid Databricks host") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Expected a plain HTTPS Databricks workspace URL")
    return parsed.hostname


@dataclass(frozen=True)
class DashboardQueries:
    kpi: str
    census: str
    alerts: str
    flow: str


def build_queries(
    settings: AppSettings, *, lookback_days: int = 7, row_limit: int = 100
) -> DashboardQueries:
    """Return read-only aggregate SQL with bounded result sets/time windows."""
    if type(lookback_days) is not int or not 1 <= lookback_days <= 30:
        raise ValueError("lookback_days must be an integer from 1 to 30")
    if type(row_limit) is not int or not 1 <= row_limit <= 500:
        raise ValueError("row_limit must be an integer from 1 to 500")

    return DashboardQueries(
        kpi=f"""
            SELECT active_patients, ed_patients, icu_patients,
                   operational_alert_patients, snapshot_ts
            FROM {settings.command_center_table}
            ORDER BY snapshot_ts DESC
            LIMIT 1
        """,
        census=f"""
            SELECT COALESCE(current_department, 'Unknown') AS department,
                   COUNT(*) AS patients
            FROM {settings.patient_360_table}
            WHERE is_currently_admitted = TRUE
            GROUP BY COALESCE(current_department, 'Unknown')
            ORDER BY patients DESC, department ASC
            LIMIT {row_limit}
        """,
        alerts=f"""
            SELECT DATE_TRUNC('HOUR', event_ts) AS event_hour,
                   source, COUNT(*) AS alert_records,
                   SUM(CASE WHEN fall_detected THEN 1 ELSE 0 END) AS fall_records
            FROM {settings.alerts_table}
            WHERE event_ts >= CURRENT_TIMESTAMP() - INTERVAL {lookback_days} DAYS
            GROUP BY DATE_TRUNC('HOUR', event_ts), source
            ORDER BY event_hour DESC, source ASC
            LIMIT {row_limit}
        """,
        flow=f"""
            SELECT event_hour, event_type, SUM(event_count) AS events
            FROM {settings.flow_table}
            WHERE event_hour >= CURRENT_TIMESTAMP() - INTERVAL {lookback_days} DAYS
            GROUP BY event_hour, event_type
            ORDER BY event_hour DESC, event_type ASC
            LIMIT {row_limit}
        """,
    )
