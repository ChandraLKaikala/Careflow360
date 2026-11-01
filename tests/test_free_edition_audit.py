"""Offline regression checks for Free Edition quota and ML release safeguards.

No Databricks serverless pipeline, model registry, or live job is executed.
"""
from __future__ import annotations

import ast
import math
from pathlib import Path
import unittest

from test_end_to_end_contracts import FakeDbutils, FakeFs, FakeSpark, execute_code


ROOT = Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "src/notebooks/01_generate_sample_data.py"
ML = ROOT / "src/notebooks/04_train_triage_model.py"
VALIDATOR = ROOT / "src/notebooks/06_validate.py"
BENCHMARK = ROOT / "src/notebooks/09_compare_free_edition_models.py"


def model_baseline_gate():
    tree = ast.parse(ML.read_text(encoding="utf-8"))
    definitions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "beats_majority_baseline"
    ]
    if len(definitions) != 1:
        raise AssertionError("Expected exactly one ML promotion guard")
    scope = {"math": math}
    exec(compile(ast.Module(body=definitions, type_ignores=[]), str(ML), "exec"), scope)
    return scope["beats_majority_baseline"]


class FreeEditionAuditTests(unittest.TestCase):
    def test_model_release_gate_rejects_underperforming_or_nan_candidates(self):
        guard = model_baseline_gate()
        scores = {
            "f1_macro": 0.60,
            "dummy_f1_macro": 0.25,
            "balanced_accuracy": 0.70,
            "dummy_balanced_accuracy": 0.20,
        }
        self.assertTrue(guard(scores))
        for changed in (
            {"f1_macro": 0.20},
            {"f1_macro": 0.25},
            {"balanced_accuracy": 0.19},
            {"f1_macro": float("nan")},
            {"balanced_accuracy": float("inf")},
        ):
            with self.subTest(changed=changed):
                self.assertFalse(guard({**scores, **changed}))
        # Equal balanced accuracy is tolerated only with strictly higher F1.
        self.assertTrue(guard({**scores, "balanced_accuracy": 0.20}))

    def test_model_release_gate_precedes_registry_alias_update(self):
        script = ML.read_text(encoding="utf-8")
        self.assertLess(
            script.index("if not beats_majority_baseline(metrics):"),
            script.index("mlflow.register_model("),
        )
        self.assertLess(
            script.index("mlflow.register_model("),
            script.index("client.set_registered_model_alias("),
        )
        self.assertIn(
            'pipeline.predict(score_pdf[features]).astype(int)', script
        )
        self.assertNotIn("mlflow.pyfunc.load_model(", script)
        self.assertIn('.select("patient_id", "encounter_id", "triage_acuity", *features)', script)
        self.assertIn('"dummy_balanced_accuracy"', script)

    def test_generator_caps_large_interactive_workloads_before_writing(self):
        defaults = {
            "catalog": "workspace", "schema": "careflow360",
            "volume": "landing", "num_patients": "100",
            "num_batches": "5", "run_id": "quota-contract",
            "reset_events": "false", "seconds_between_batches": "0",
        }
        for overrides, message in (
            ({"num_patients": "2001"}, "between 50 and 2000"),
            ({"num_batches": "51"}, "between 1 and 50"),
        ):
            with self.subTest(overrides=overrides):
                fs = FakeFs()
                with self.assertRaisesRegex(ValueError, message):
                    execute_code(
                        GENERATOR, FakeSpark(),
                        FakeDbutils({**defaults, **overrides}, fs),
                    )
                self.assertFalse(fs.files, "Rejected replay must write no landing events")

    def test_validation_uses_bounded_existence_probes(self):
        source = VALIDATOR.read_text(encoding="utf-8")
        self.assertIn("spark.table(name).limit(1).count() > 0", source)
        self.assertNotIn("row_count = spark.table(name).count()", source)
        self.assertIn('freshness["row_count"] != 1', source)
        self.assertIn('if age < -1:', source)
        # Complete per-stream counts are still required on the new run.
        self.assertIn("expected_stream_counts_from_manifest(", source)
        self.assertIn("verify_new_patient_inserts(", source)

    def test_cpu_comparison_remains_optional_and_disjoint(self):
        source = BENCHMARK.read_text(encoding="utf-8")
        self.assertIn("ExtraTreesClassifier(", source)
        self.assertIn("baseline_test_score = measure(baseline, X_test, y_test)", source)
        self.assertIn('"beats_validation_dummy"', source)
        self.assertIn("Validation/test groups have fewer than two labels", source)
        self.assertNotIn("mlflow.register_model(", source)
        self.assertNotIn("set_registered_model_alias(", source)


if __name__ == "__main__":
    unittest.main()
