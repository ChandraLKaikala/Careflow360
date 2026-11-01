"""Source and CPU-only regression checks for the Free Edition notebook audit.

All executable notebook cells are also AST-checked in test_pipeline_structure.
These tests do not claim to run Spark/Lakeflow or MLflow in Databricks.
"""
import ast
import pathlib
import unittest

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score, f1_score
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = pathlib.Path(__file__).resolve().parents[1]
GOLD = ROOT / "src/pipelines/03_gold.py"
MIRROR_GOLD = ROOT / "CareFlow360_Databricks_Free_Edition/core/src/pipelines/03_gold.py"
BENCH = ROOT / "src/notebooks/09_compare_free_edition_models.py"
MIRROR_BENCH = (
    ROOT / "CareFlow360_Databricks_Free_Edition/core/src/notebooks/"
    "09_compare_free_edition_models.py"
)
NUMERIC = [
    "age", "systolic_bp", "diastolic_bp", "heart_rate",
    "respiratory_rate", "temperature_c", "spo2", "gcs_total",
    "pain_score", "num_prior_visits", "arrival_hour", "shock_index",
]
CATEGORICAL = ["arrival_mode", "arrival_day", "sex"]


def extract_functions(path, names, context):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert len(selected) == len(names), [n.name for n in selected]
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), context)
    return context


class GoldEncounterSafetyTests(unittest.TestCase):
    def test_gold_live_patient_data_joins_by_patient_and_encounter(self):
        source = GOLD.read_text(encoding="utf-8")
        self.assertEqual(source, MIRROR_GOLD.read_text(encoding="utf-8"))
        tree = ast.parse(source)
        gold = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                    and n.name == "gold_patient_360")
        calls = [n for n in ast.walk(gold)
                 if isinstance(n, ast.Call) and
                 isinstance(n.func, ast.Attribute) and n.func.attr == "join"]
        join_keys = [
            [x.value for x in n.args[1].elts]
            for n in calls if len(n.args) >= 2 and isinstance(n.args[1], ast.List)
        ]
        self.assertEqual(join_keys.count(["patient_id", "encounter_id"]), 2)
        self.assertEqual(len(calls), 3)
        self.assertIn('F.col("encounter_id") == F.col("location_encounter_id")', source)
        self.assertIn('Window.partitionBy("patient_id", "encounter_id")', source)
        self.assertIn('partition_cols=("patient_id", "encounter_id")', source)
        self.assertNotIn('.join(vitals, "patient_id", "left")', source)
        self.assertNotIn('.join(complaints, "patient_id", "left")', source)

    def test_live_alerts_are_from_current_admitted_encounter(self):
        source = GOLD.read_text(encoding="utf-8")
        tree = ast.parse(source)
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                  and n.name == "gold_live_alerts")
        body = ast.get_source_segment(source, fn)
        self.assertIn('spark.read.table("gold_patient_360")', body)
        self.assertIn('F.col("is_currently_admitted")', body)
        self.assertIn('.join(admitted, ["patient_id", "encounter_id"], "inner")', body)

    def test_reference_example_prevents_cross_encounter_contamination(self):
        # One returning patient has a current visit E2, but a delayed vitals
        # event from E1. An ID-only join would incorrectly attach it to E2.
        triage = pd.DataFrame([{"patient_id": "P1", "encounter_id": "E2"}])
        vitals = pd.DataFrame([
            {"patient_id": "P1", "encounter_id": "E1", "heart_rate": 140},
            {"patient_id": "P1", "encounter_id": "E2", "heart_rate": 88},
        ])
        safe = triage.merge(vitals, on=["patient_id", "encounter_id"], how="left")
        unsafe = triage.merge(vitals, on=["patient_id"], how="left")
        self.assertEqual(len(safe), 1)
        self.assertEqual(safe["heart_rate"].tolist(), [88])
        self.assertEqual(len(unsafe), 2)


class FreeEditionModelComparisonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.namespace = extract_functions(
            BENCH,
            {"three_way_patient_split", "preprocessing", "candidate_models", "measure"},
            {
                "GroupShuffleSplit": GroupShuffleSplit,
                "ColumnTransformer": ColumnTransformer,
                "Pipeline": Pipeline,
                "SimpleImputer": SimpleImputer,
                "OneHotEncoder": OneHotEncoder,
                "StandardScaler": StandardScaler,
                "LogisticRegression": LogisticRegression,
                "RandomForestClassifier": RandomForestClassifier,
                "ExtraTreesClassifier": ExtraTreesClassifier,
                "HistGradientBoostingClassifier": HistGradientBoostingClassifier,
                "f1_score": f1_score,
                "balanced_accuracy_score": balanced_accuracy_score,
                "numeric": NUMERIC,
                "categorical": CATEGORICAL,
            },
        )

    def test_every_comparison_cell_and_mirror_are_identical(self):
        self.assertEqual(BENCH.read_bytes(), MIRROR_BENCH.read_bytes())
        for i, cell in enumerate(BENCH.read_text().split("# COMMAND ----------")):
            ast.parse(cell, filename=f"model_compare:{i}")
        source = BENCH.read_text()
        self.assertNotIn("mlflow.register_model(", source)
        self.assertNotIn("set_registered_model_alias(", source)
        self.assertIn('max_local_rows", "2000"', source)
        self.assertIn('GroupShuffleSplit(n_splits=1', source)
        self.assertIn('selection": "validation_macro_f1_patient_disjoint"', source)

    def test_three_way_patient_groups_are_non_overlapping(self):
        patients = pd.Series([f"P{i:04d}" for i in range(70) for _ in range(3)])
        features = pd.DataFrame({"age": list(range(210))})
        labels = pd.Series([(i // 3) % 5 + 1 for i in range(210)])
        fn = self.namespace["three_way_patient_split"]
        train, valid, test = fn(features, labels, patients)
        sets = [set(patients.iloc[indices]) for indices in (train, valid, test)]
        self.assertFalse(sets[0] & sets[1] or sets[0] & sets[2] or sets[1] & sets[2])
        self.assertEqual(sum(len(x) for x in (train, valid, test)), 210)
        self.assertEqual(sets, [
            set(patients.iloc[indices])
            for indices in fn(features, labels, patients)
        ])

    def test_cpu_candidates_fit_and_predict_synthetic_inputs(self):
        X = pd.DataFrame({
            **{name: [float((i * 7 + j) % 45 + 1) for i in range(120)]
               for j, name in enumerate(NUMERIC)},
            "arrival_mode": ["Walk-in", "Ambulance"] * 60,
            "arrival_day": ["Monday", "Tuesday"] * 60,
            "sex": ["F", "M"] * 60,
        })
        y = pd.Series([i % 5 + 1 for i in range(120)])
        candidates = self.namespace["candidate_models"]()
        self.assertEqual(set(candidates), {
            "logistic_regression", "random_forest", "extra_trees", "hist_gradient_boosting"
        })
        self.assertEqual(candidates["random_forest"].named_steps["model"].n_estimators, 96)
        self.assertEqual(candidates["random_forest"].named_steps["model"].n_jobs, 2)
        self.assertEqual(candidates["hist_gradient_boosting"].named_steps["model"].max_iter, 100)
        self.assertEqual(candidates["extra_trees"].named_steps["model"].n_estimators, 80)
        self.assertEqual(candidates["extra_trees"].named_steps["model"].n_jobs, 2)
        for model in candidates.values():
            model.fit(X, y)
            prediction = model.predict(X.iloc[:10])
            self.assertEqual(len(prediction), 10)
            self.assertTrue(set(prediction) <= {1, 2, 3, 4, 5})


if __name__ == "__main__":
    unittest.main(verbosity=2)
