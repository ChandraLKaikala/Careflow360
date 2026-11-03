"""Regression tests for the ML release sequence and encounter lifecycle.

Covers the externally audited P0 corrections:
  * candidate vs Champion compared on the SAME holdout data (never via
    metrics logged by different historical runs),
  * rejected candidates never score new patients — the approved model
    (incumbent Champion) is used instead, and promotion happens only after
    the required checks succeed,
  * alias-not-found handling returns None only for genuine first installs
    while registry permission/service errors propagate,
  * capacity-aware bed allocation and encounter closure rules in the
    generator (no bed ever holds two concurrently-admitted patients).
"""
from __future__ import annotations

import ast
import pathlib
import types
import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd
from sklearn.metrics import balanced_accuracy_score, f1_score

from test_end_to_end_contracts import FakeDbutils, FakeFs, FakeSpark, execute_code, event_rows

ROOT = pathlib.Path(__file__).resolve().parents[1]
ML_PATH = ROOT / "src/notebooks/04_train_triage_model.py"
GENERATOR_PATH = ROOT / "src/notebooks/01_generate_sample_data.py"


def nodes_from_source(path, wanted, extra_globals=None):
    """Extract named functions/classes from a notebook source file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in wanted:
            found[node.name] = node
    assert set(found) == set(wanted), (wanted, sorted(found))
    scope = dict(extra_globals or {})
    exec(compile(ast.Module(body=list(found.values()), type_ignores=[]), str(path), "exec"), scope)
    return scope


class FakeMlflowException(Exception):
    def __init__(self, message, error_code=""):
        super().__init__(message)
        self.error_code = error_code


class ChampionLookupTests(unittest.TestCase):
    def lookup(self, behavior):
        calls = []

        class Client:
            def get_model_version_by_alias(self, name, alias):
                calls.append((name, alias))
                return behavior()

        scope = nodes_from_source(
            ML_PATH, {"find_champion_version"},
            {"MlflowException": FakeMlflowException},
        )
        return scope["find_champion_version"](Client(), "model", "Champion"), calls

    def test_missing_model_or_alias_returns_none(self):
        for code in ("RESOURCE_DOES_NOT_EXIST", "RESOURCE_NOT_FOUND"):
            def raise_missing(code=code):
                raise FakeMlflowException("not found", code)
            result, calls = self.lookup(raise_missing)
            self.assertIsNone(result)
            self.assertEqual(calls, [("model", "Champion")])

    def test_invalid_parameter_with_alias_returns_none(self):
        def raise_invalid():
            raise FakeMlflowException("No such alias: Champion", "INVALID_PARAMETER_VALUE")
        self.assertIsNone(self.lookup(raise_invalid)[0])

    def test_permission_and_service_errors_propagate(self):
        for code in ("PERMISSION_DENIED", "INVALID_STATE", "INTERNAL_ERROR"):
            def raise_registry(code=code):
                raise FakeMlflowException("registry unavailable", code)
            with self.assertRaises(FakeMlflowException):
                self.lookup(raise_registry)

    def test_non_mlflow_errors_propagate(self):
        def raise_other():
            raise PermissionError("denied")
        with self.assertRaises(PermissionError):
            self.lookup(raise_other)


class SameHoldoutDecisionTests(unittest.TestCase):
    def test_candidate_wins_only_when_both_metrics_hold(self):
        scope = nodes_from_source(ML_PATH, {"candidate_wins_same_holdout"})
        wins = scope["candidate_wins_same_holdout"]
        champion = {"f1_macro": 0.5, "balanced_accuracy": 0.6}
        self.assertTrue(wins({"f1_macro": 0.5, "balanced_accuracy": 0.6}, champion))
        self.assertTrue(wins({"f1_macro": 0.6, "balanced_accuracy": 0.7}, champion))
        self.assertFalse(wins({"f1_macro": 0.49, "balanced_accuracy": 0.9}, champion))
        self.assertFalse(wins({"f1_macro": 0.9, "balanced_accuracy": 0.59}, champion))

    def test_champion_is_rescored_on_this_runs_holdout_not_logged_metrics(self):
        source = ML_PATH.read_text(encoding="utf-8")
        # The incumbent Champion must be LOADED and re-predicted on this
        # run's X_test/y_test — never compared via metrics from another run.
        self.assertIn("def evaluate_version_on_holdout", source)
        self.assertIn("loaded.predict(X_test)", source)
        self.assertIn("f1_score(y_test, preds, average=", source)
        self.assertNotIn("data.metrics.get", source)

    def test_evaluate_version_on_holdout_rescores_loaded_model(self):
        X = pd.DataFrame({"f": [1.0, 2.0, 3.0, 4.0, 6.0, 5.0]})
        y = pd.Series([0, 0, 1, 1, 1, 0])
        y_pred = pd.Series([0, 0, 1, 1, 0, 1])  # deliberately imperfect

        class FakeLoaded:
            def predict(self, X_test):
                assert len(X_test) == len(y_pred)
                return y_pred.to_numpy()

        def load_model(uri):
            assert uri == "models:/demo_model/3"
            return FakeLoaded()

        fake_mlflow = types.SimpleNamespace(
            sklearn=types.SimpleNamespace(load_model=load_model)
        )
        scope = nodes_from_source(
            ML_PATH, {"evaluate_version_on_holdout"},
            {"mlflow": fake_mlflow, "f1_score": f1_score,
             "balanced_accuracy_score": balanced_accuracy_score},
        )
        result = scope["evaluate_version_on_holdout"]("models:/demo_model/3", X, y)
        self.assertEqual(
            result["f1_macro"],
            float(f1_score(y, y_pred, average="macro", zero_division=0)),
        )
        self.assertEqual(
            result["balanced_accuracy"],
            float(balanced_accuracy_score(y, y_pred)),
        )


class ApprovedModelScoringTests(unittest.TestCase):
    def test_rejected_candidate_never_scores_new_patients(self):
        source = ML_PATH.read_text(encoding="utf-8")
        # When the candidate loses, the approved pipeline is the LOADED
        # incumbent Champion, and new patients are scored with it only.
        self.assertIn("approved_pipeline = mlflow.sklearn.load_model(", source)
        self.assertIn("approved_pipeline.predict(score_pdf[features])", source)
        self.assertNotIn("= pipeline.predict(score_pdf[features])", source)
        # The alias moves only AFTER the same-holdout comparison succeeded.
        decision = source.index("candidate_wins_same_holdout(candidate_metrics, champion_metrics)")
        alias_set = source.index(
            'client.set_registered_model_alias(full_model_name, "Champion", registered.version)'
        )
        self.assertGreater(alias_set, decision,
                          "alias must be set only after the release decision")


class BedAllocatorTests(unittest.TestCase):
    def allocator(self, pools=None, preoccupied=()):
        scope = nodes_from_source(
            GENERATOR_PATH, {"BedAllocator"},
            {"datetime": datetime, "timezone": timezone, "timedelta": timedelta},
        )
        pools = pools or {"Emergency": ("ED", 2), "ICU": ("ICU", 1)}
        return scope["BedAllocator"](pools, preoccupied)

    def test_never_assigns_same_bed_to_overlapping_stays(self):
        a = self.allocator()
        t0 = datetime(2026, 11, 3, tzinfo=timezone.utc)
        self.assertEqual(a.allocate("ICU", t0, t0 + timedelta(hours=2)), "ICU-001")
        # Pool of one is busy: boarding (None), never a shared bed.
        self.assertIsNone(
            a.allocate("ICU", t0 + timedelta(hours=1), t0 + timedelta(hours=3))
        )

    def test_bed_is_released_after_interval_ends(self):
        a = self.allocator()
        t0 = datetime(2026, 11, 3, tzinfo=timezone.utc)
        self.assertEqual(a.allocate("Emergency", t0, t0 + timedelta(hours=1)), "ED-001")
        self.assertEqual(
            a.allocate("Emergency", t0 + timedelta(hours=2), t0 + timedelta(hours=3)),
            "ED-001",
        )

    def test_open_ended_stay_holds_bed_for_whole_run(self):
        a = self.allocator()
        t0 = datetime(2026, 11, 3, tzinfo=timezone.utc)
        # Two still-admitted patients take both ED beds and never free them;
        # a third overlapping request must board (None), never share a bed.
        self.assertEqual(a.allocate("Emergency", t0, None), "ED-001")
        self.assertEqual(a.allocate("Emergency", t0 + timedelta(hours=1), None), "ED-002")
        self.assertIsNone(a.allocate("Emergency", t0 + timedelta(hours=10), None))

    def test_preoccupied_live_beds_are_never_reused(self):
        a = self.allocator(preoccupied={"ED-001"})
        t0 = datetime(2026, 11, 3, tzinfo=timezone.utc)
        self.assertEqual(a.allocate("Emergency", t0, None), "ED-002")


class EncounterLifecycleTests(unittest.TestCase):
    @staticmethod
    def generate(run_id="lifecycle01", n="120", batches="4"):
        fs = FakeFs()
        params = {
            "catalog": "workspace", "schema": "careflow360", "volume": "landing",
            "num_patients": n, "num_batches": batches, "run_id": run_id,
            "reset_events": "false", "seconds_between_batches": "0",
        }
        execute_code(GENERATOR_PATH, FakeSpark(), FakeDbutils(params, fs))
        return event_rows(fs, "adt")

    @staticmethod
    def parse_ts(value):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def test_no_bed_ever_holds_two_concurrently_admitted_patients(self):
        rows = self.generate()
        per_encounter = {}
        for row in rows:
            per_encounter.setdefault((row["patient_id"], row["encounter_id"]), []).append(row)
        intervals = {}
        order = ["A01_ADMIT", "A02_TRANSFER", "A03_DISCHARGE"]
        for events in per_encounter.values():
            events.sort(key=lambda r: r["event_ts"])
            kinds = [r["event_type"] for r in events]
            self.assertEqual(kinds[0], "A01_ADMIT")
            self.assertEqual(kinds, sorted(kinds, key=order.index),
                             "ADT events must follow admit -> transfer -> discharge")
            admit, *rest = events
            transfer = next((r for r in rest if r["event_type"] == "A02_TRANSFER"), None)
            discharge = next((r for r in rest if r["event_type"] == "A03_DISCHARGE"), None)
            admit_ts = self.parse_ts(admit["event_ts"])
            ed_end = (
                self.parse_ts(transfer["event_ts"]) if transfer
                else self.parse_ts(discharge["event_ts"]) if discharge
                else None
            )
            if admit["bed_id"]:
                intervals.setdefault(admit["bed_id"], []).append((admit_ts, ed_end))
            if transfer and transfer["bed_id"]:
                start = self.parse_ts(transfer["event_ts"])
                end = self.parse_ts(discharge["event_ts"]) if discharge else None
                intervals.setdefault(transfer["bed_id"], []).append((start, end))
        never = datetime.max.replace(tzinfo=timezone.utc)
        conflicts = 0
        for stays in intervals.values():
            for i in range(len(stays)):
                for j in range(i + 1, len(stays)):
                    a_start, a_end = stays[i]
                    b_start, b_end = stays[j]
                    if (a_start < (b_end or never)) and (b_start < (a_end or never)):
                        conflicts += 1
        self.assertEqual(conflicts, 0,
                         "a bed must never hold two concurrently-admitted patients")

    def test_assigned_beds_match_department_pool_and_allow_boarding(self):
        rows = self.generate(run_id="lifecycle02")
        pools = {"ED": 80, "OBS": 60, "MED": 60, "SUR": 60, "ICU": 40, "TEL": 40}
        assigned = 0
        for row in rows:
            bed = row["bed_id"]
            if bed is None:
                continue
            prefix, _, number = bed.partition("-")
            self.assertIn(prefix, pools, bed)
            self.assertTrue(1 <= int(number) <= pools[prefix], bed)
            assigned += 1
        # Capacity pressure is realistic: some patients board without a bed,
        # but nobody ever shares one.
        self.assertGreater(assigned, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
