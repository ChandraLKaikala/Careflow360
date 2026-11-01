"""Offline smoke test for CareFlow360's ML feature and prediction output contract.

No Databricks MLflow registry or Spark execution is covered by this test.
"""
from __future__ import annotations

import ast
import pathlib
import unittest

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from test_end_to_end_contracts import FakeDbutils, FakeFs, FakeSpark, execute_code, event_rows

ROOT = pathlib.Path(__file__).resolve().parents[1]


def model_feature_lists():
    source = ROOT / "src/notebooks/04_train_triage_model.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    lists = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in ("numeric", "categorical")
            for t in node.targets
        ):
            value = ast.literal_eval(node.value)
            for target in node.targets:
                if isinstance(target, ast.Name):
                    lists[target.id] = value
    return lists["numeric"], lists["categorical"]


def scoring_coverage_guard():
    source = ROOT / "src/notebooks/04_train_triage_model.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    fn = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and node.name == "ensure_current_patient_coverage"
    )
    namespace = {}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(source), "exec"), namespace)
    return namespace["ensure_current_patient_coverage"]


def model_hyperparameters():
    """Keep the offline scikit-learn smoke test tied to production model source."""
    source = ROOT / "src/notebooks/04_train_triage_model.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    model_calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "RandomForestClassifier"
    ]
    assert len(model_calls) == 1
    return {kw.arg: ast.literal_eval(kw.value) for kw in model_calls[0].keywords}


class ModelOutputContractTests(unittest.TestCase):
    def test_current_patient_scoring_coverage_and_uniqueness(self):
        guard = scoring_coverage_guard()
        current = pd.DataFrame({
            "patient_id": ["P1", "P2"],
            "encounter_id": ["E1_new", "E2_new"],
        })
        guard(current, 2)  # one current encounter per patient is valid
        with self.assertRaisesRegex(RuntimeError, "Expected 3 current patients"):
            guard(current, 3)
        with self.assertRaisesRegex(RuntimeError, "duplicate current patient"):
            guard(pd.DataFrame({
                "patient_id": ["P1", "P1"],
                "encounter_id": ["E1_old", "E1_new"],
            }), 2)
        with self.assertRaisesRegex(RuntimeError, "null patient or encounter"):
            guard(pd.DataFrame({
                "patient_id": ["P1", "P2"],
                "encounter_id": ["E1_new", None],
            }), 2)
        with self.assertRaisesRegex(RuntimeError, "no current patient"):
            guard(current.iloc[:0], 0)
        source = (ROOT / "src/notebooks/04_train_triage_model.py").read_text()
        self.assertIn('df.join(new_keys, ["patient_id", "encounter_id"], "inner")', source)
        self.assertIn('current_keys.join(existing_ids, "patient_id", "left_anti")', source)
        self.assertIn('spark.table(current_patient_table)', source)
        self.assertIn('ensure_current_patient_coverage(score_pdf, current_patient_count)', source)

    def test_synthetic_model_features_train_and_predict(self):
        fs = FakeFs()
        params = {
            "catalog": "workspace", "schema": "careflow360", "volume": "landing",
            "num_patients": "300", "num_batches": "3", "run_id": "modeltest01",
            "reset_events": "false", "seconds_between_batches": "0",
        }
        execute_code(ROOT / "src/notebooks/01_generate_sample_data.py",
                     FakeSpark(), FakeDbutils(params, fs))
        df = pd.DataFrame(event_rows(fs, "triage"))
        df["shock_index"] = df["heart_rate"] / df["systolic_bp"]
        numeric, categorical = model_feature_lists()
        features = numeric + categorical
        self.assertEqual(set(features) - set(df.columns), set())

        X = df[features]
        y = df["triage_acuity"].astype(int)
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.25, random_state=42,
            stratify=y if y.value_counts().min() >= 2 else None,
        )
        pre = ColumnTransformer([
            ("numeric", Pipeline([("imputer", SimpleImputer(strategy="median"))]), numeric),
            ("categorical", Pipeline([
                ("imputer", SimpleImputer(strategy="most_frequent")),
                ("onehot", OneHotEncoder(handle_unknown="ignore")),
            ]), categorical),
        ])
        model = Pipeline([
            ("preprocessor", pre),
            ("model", RandomForestClassifier(**model_hyperparameters())),
        ])
        params = model_hyperparameters()
        self.assertEqual(params["n_estimators"], 96)
        self.assertEqual(params["n_jobs"], 2)
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)
        self.assertEqual(len(predictions), len(X_test))
        self.assertEqual(set(predictions).difference({1, 2, 3, 4, 5}), set())
        self.assertGreaterEqual(accuracy_score(y_test, predictions), 0.0)
        self.assertLessEqual(accuracy_score(y_test, predictions), 1.0)
        self.assertGreaterEqual(f1_score(y_test, predictions, average="macro", zero_division=0), 0.0)
        self.assertEqual(len(X_train) + len(X_test), 300)


if __name__ == "__main__":
    unittest.main(verbosity=2)
