"""Executable patient-disjoint ML evaluation tests plus Free Edition source contracts.

These do not run Databricks serverless, MLflow registration, or Spark SQL.
"""
import ast
import pathlib
import unittest

import pandas as pd
from sklearn.model_selection import GroupShuffleSplit

ROOT = pathlib.Path(__file__).resolve().parents[1]
ML = ROOT / "src/notebooks/04_train_triage_model.py"
ML_MIRROR = ROOT / "CareFlow360_Databricks_Free_Edition/core/src/notebooks/04_train_triage_model.py"
JOB = ROOT / "resources/03_jobs.yml"


def get_group_split():
    tree = ast.parse(ML.read_text(encoding="utf-8"))
    fn = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "split_patient_holdout"
    )
    namespace = {"GroupShuffleSplit": GroupShuffleSplit}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(ML), "exec"), namespace)
    return namespace["split_patient_holdout"]


class FreeEditionMlTests(unittest.TestCase):
    @staticmethod
    def multiple_encounters():
        # Fifty patients with four different encounters and all 5 acuity
        # classes; a naive row split would leak patient IDs into both sets.
        ids = [f"P{i:04d}" for i in range(50) for _ in range(4)]
        X = pd.DataFrame({"age": [i % 65 for i in range(200)],
                          "systolic_bp": [110 + i % 30 for i in range(200)]})
        y = pd.Series([(i // 4) % 5 + 1 for i in range(200)])
        patients = pd.Series(ids)
        return X, y, patients

    def test_patient_groups_never_leak_across_train_test(self):
        X, y, patients = self.multiple_encounters()
        fn = get_group_split()
        result = fn(X, y, patients)
        a, b, y_a, y_b, train_ids, test_ids = result
        self.assertEqual(len(a) + len(b), 200)
        self.assertEqual(len(y_a) + len(y_b), 200)
        self.assertEqual(train_ids & test_ids, set())
        self.assertEqual(train_ids | test_ids, set(patients))
        self.assertEqual(set(patients.iloc[a.index]), train_ids)
        self.assertEqual(set(patients.iloc[b.index]), test_ids)
        self.assertGreater(len(test_ids), 0)
        # Using the same fixed seed gives exactly the same held-out patients.
        self.assertEqual(result[5], fn(X, y, patients)[5])

    def test_too_few_training_classes_fails(self):
        X = pd.DataFrame({"age": list(range(20))})
        y = pd.Series([3] * 20)
        patients = pd.Series([f"P{i}" for i in range(20)])
        with self.assertRaisesRegex(ValueError, "diversity"):
            get_group_split()(X, y, patients)

    def test_source_is_bounded_and_scores_only_new_patients(self):
        source = ML.read_text(encoding="utf-8")
        self.assertEqual(source, ML_MIRROR.read_text(encoding="utf-8"))
        self.assertIn('widget("max_local_rows", "4000")', source)
        self.assertIn('F.xxhash64("patient_id", "encounter_id")', source)
        self.assertIn(".limit(max_local_rows + 1)", source)
        self.assertIn("training_capped", source)
        self.assertIn('current_keys.join(existing_ids, "patient_id", "left_anti")', source)
        self.assertIn("if new_patient_count:", source)
        self.assertIn("ensure_current_patient_coverage(score_pdf, new_patient_count)", source)
        self.assertIn('n_jobs=2,', source)
        self.assertIn('DummyClassifier(strategy="most_frequent")', source)
        self.assertIn("balanced_accuracy_score(y_test, pred)", source)
        self.assertIn('train_test_strategy": "patient_group_shuffle_split"', source)
        self.assertNotIn('model_selection import train_test_split', source)
        self.assertNotIn('.mode("overwrite")', source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
