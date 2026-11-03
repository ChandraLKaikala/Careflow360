"""Offline regression tests for safe, bounded Databricks App queries."""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest


HELPERS = (
    pathlib.Path(__file__).resolve().parents[1]
    / "extensions/app/src/app_helpers.py"
)
spec = importlib.util.spec_from_file_location("app_helpers", HELPERS)
app_helpers = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app_helpers
spec.loader.exec_module(app_helpers)

AppSettings = app_helpers.AppSettings
build_queries = app_helpers.build_queries
warehouse_hostname = app_helpers.warehouse_hostname


class AppDashboardContracts(unittest.TestCase):
    def setUp(self):
        self.env = {
            "DATABRICKS_WAREHOUSE_ID": "9a2b_01-warehouse",
            "COMMAND_CENTER_TABLE": "workspace.careflow360_gold.hospital_command_center",
            "PATIENT_360_TABLE": "workspace.careflow360_gold.patient_360",
            "ALERTS_TABLE": "workspace.careflow360_gold.live_alerts",
            "FLOW_TABLE": "workspace.careflow360_gold.department_hourly",
        }

    def test_accepts_bound_resources_and_trims_whitespace(self):
        env = {key: f" {value} " for key, value in self.env.items()}
        settings = AppSettings.from_env(env)
        self.assertEqual(settings.warehouse_id, self.env["DATABRICKS_WAREHOUSE_ID"])
        self.assertEqual(settings.patient_360_table, self.env["PATIENT_360_TABLE"])

    def test_missing_resource_binding_fails_closed(self):
        for key in self.env:
            with self.subTest(key=key):
                env = dict(self.env)
                del env[key]
                with self.assertRaisesRegex(ValueError, "Missing app resource bindings"):
                    AppSettings.from_env(env)

    def test_rejects_sql_identifier_injection_and_partial_names(self):
        bad = (
            "workspace.schema.table;DROP TABLE x",
            "workspace.schema",
            "workspace.schema.table extra",
            "workspace.schema.table--",
            "workspace..table",
        )
        for key in ("COMMAND_CENTER_TABLE", "PATIENT_360_TABLE", "ALERTS_TABLE", "FLOW_TABLE"):
            for identifier in bad:
                with self.subTest(key=key, identifier=identifier):
                    env = dict(self.env)
                    env[key] = identifier
                    with self.assertRaises(ValueError):
                        AppSettings.from_env(env)

    def test_rejects_untrusted_warehouse_paths(self):
        for value in ("warehouse/../../test", "id?param=1", "", "bad id", "x" * 129):
            with self.subTest(value=value):
                env = dict(self.env)
                env["DATABRICKS_WAREHOUSE_ID"] = value
                with self.assertRaises(ValueError):
                    AppSettings.from_env(env)

    def test_workspace_hostname_requires_https_and_no_path_or_userinfo(self):
        self.assertEqual(
            warehouse_hostname("https://adb-123.cloud.databricks.com/"),
            "adb-123.cloud.databricks.com",
        )
        for host in (
            "http://adb.example.com",
            "https://user:secret@adb.example.com",
            "https://adb.example.com/other/path",
            "https://adb.example.com?query=1",
            "https://adb.example.com:444",
            "adb.example.com",
        ):
            with self.subTest(host=host):
                with self.assertRaises(ValueError):
                    warehouse_hostname(host)

    def test_queries_are_aggregate_only_and_time_bounded(self):
        queries = build_queries(AppSettings.from_env(self.env))
        for statement in (queries.kpi, queries.census, queries.alerts, queries.flow):
            self.assertIn("LIMIT ", statement)
            self.assertNotIn("SELECT *", statement.upper())
        self.assertIn("LIMIT 1", queries.kpi)
        self.assertIn("is_currently_admitted = TRUE", queries.census)
        self.assertIn("COUNT(*) AS patients", queries.census)
        self.assertIn("INTERVAL 7 DAYS", queries.alerts)
        self.assertIn("INTERVAL 7 DAYS", queries.flow)
        self.assertIn("LIMIT 100", queries.alerts)
        self.assertIn("GROUP BY", queries.alerts)
        self.assertIn("GROUP BY", queries.flow)

    def test_limit_parameters_reject_out_of_range_and_noninteger_values(self):
        settings = AppSettings.from_env(self.env)
        for bad in (0, 31, -1, True, "7", 1.5):
            with self.subTest(days=bad):
                with self.assertRaises(ValueError):
                    build_queries(settings, lookback_days=bad)
        for bad in (0, 501, -1, True, "100", 1.5):
            with self.subTest(limit=bad):
                with self.assertRaises(ValueError):
                    build_queries(settings, row_limit=bad)
        queries = build_queries(settings, lookback_days=2, row_limit=25)
        self.assertIn("INTERVAL 2 DAYS", queries.flow)
        self.assertIn("LIMIT 25", queries.census)


if __name__ == "__main__":
    unittest.main()
