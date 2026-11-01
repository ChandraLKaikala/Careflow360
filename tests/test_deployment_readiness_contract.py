"""End-to-end offline manifest/medallion and policy refresh integration contracts.

These mocks exercise actual synthetic files and actual validator logic;
a Databricks workspace run is still required for Spark, Auto Loader and MLflow.
"""
import json
import pathlib
import sys
import types
import unittest
from unittest.mock import patch

from test_end_to_end_contracts import (
    FakeDbutils, FakeFs, FakeSpark, execute_code, event_rows,
)
from test_incremental_insert_contract import (
    FakeSpark as LayerSpark, Functions, function_from_notebook,
)

ROOT = pathlib.Path(__file__).resolve().parents[1]
GENERATOR = ROOT / "src/notebooks/01_generate_sample_data.py"
VALIDATOR = ROOT / "src/notebooks/06_validate.py"
POLICY = ROOT / "src/notebooks/05_create_policy_docs.py"


def generate_run(run_id="contract-run-01", patients=50):
    fs = FakeFs()
    dbutils = FakeDbutils({
        "catalog": "workspace", "schema": "careflow360",
        "volume": "landing", "num_patients": str(patients),
        "num_batches": "3", "run_id": run_id,
        "reset_events": "false", "seconds_between_batches": "0",
    }, fs)
    execute_code(GENERATOR, FakeSpark(), dbutils)
    return fs, dbutils


def read_manifest(dbutils, run_id="contract-run-01", patients=50):
    parser = function_from_notebook(
        VALIDATOR, "expected_stream_counts_from_manifest",
        {"dbutils": dbutils, "json": json},
    )
    return parser(run_id, patients, "workspace", "careflow360", "landing")


class RealGeneratorToValidatorTests(unittest.TestCase):
    def test_real_generator_10_layer_stream_contract(self):
        run_id = "contract-run-01"
        fs, dbutils = generate_run(run_id)
        counts = read_manifest(dbutils, run_id)
        streams = ("triage", "vitals", "adt", "chief_complaints")
        self.assertEqual(counts["triage"], 50)
        self.assertEqual(counts["chief_complaints"], 50)
        self.assertGreaterEqual(counts["adt"], 50)
        self.assertGreaterEqual(counts["vitals"], 50)
        data = {}
        assets = []
        older = [{"patient_id": f"P{i:06d}"} for i in range(1, 601)]
        for stream in streams:
            rows = event_rows(fs, stream)
            self.assertEqual(len(rows), counts[stream])
            for layer in ("bronze", "silver"):
                name = f"{layer}_{stream}"
                data[name] = older + rows
                assets.append((name, name, counts[stream]))
        new_patients = [{"patient_id": f"P{run_id}-{i:06d}"} for i in range(1, 51)]
        for name in ("gold_patient_360", "gold_triage_predictions"):
            data[name] = older + new_patients
            assets.append((name, name, 50))
        validate = function_from_notebook(
            VALIDATOR, "verify_new_patient_inserts",
            {"spark": LayerSpark(data), "F": Functions},
        )
        self.assertEqual(len(assets), 10)
        self.assertEqual(validate(run_id, 50, assets), [])
        # An entire missing ADT batch must make the run fail, even if triage
        # and Patient 360 each received all their expected new patients.
        data["silver_adt"].pop()
        errors = validate(run_id, 50, assets)
        self.assertEqual(len(errors), 1)
        self.assertIn("silver_adt", errors[0])

    def test_immutable_manifest_rejects_wrong_run_or_missing_records(self):
        run_id = "contract-run-02"
        fs, dbutils = generate_run(run_id)
        path = f"/Volumes/workspace/careflow360/landing/manifests/{run_id}.json"
        saved = fs.files[path]
        self.assertEqual(read_manifest(dbutils, run_id)["triage"], 50)
        with self.assertRaisesRegex(RuntimeError, "disagrees"):
            read_manifest(dbutils, run_id, 51)
        manifest = json.loads(saved)
        manifest["rows"]["vitals"] = 10
        fs.files[path] = json.dumps(manifest)
        with self.assertRaisesRegex(RuntimeError, "invalid vitals count"):
            read_manifest(dbutils, run_id)
        fs.files[path] = saved
        manifest = json.loads(saved)
        manifest["run_id"] = "unexpected"
        fs.files[path] = json.dumps(manifest)
        with self.assertRaisesRegex(RuntimeError, "disagrees"):
            read_manifest(dbutils, run_id)

    def test_validation_and_job_source_use_same_landing_schema(self):
        job = (ROOT / "resources/03_jobs.yml").read_text()
        self.assertIn("schema: ${resources.schemas.pipeline_schema.name}", job)
        self.assertIn("landing_volume: ${resources.volumes.landing_volume.name}", job)
        validation = VALIDATOR.read_text()
        self.assertIn("expected_stream_counts_from_manifest(", validation)
        self.assertIn('("triage", "vitals", "adt", "chief_complaints")', validation)
        self.assertIn("len(asset) == 2", validation)


class PolicyMergeContractTests(unittest.TestCase):
    def test_policy_refresh_is_non_destructive_and_changes_only_modified_rows(self):
        policy = POLICY.read_text()
        self.assertNotIn('.mode("overwrite")', policy)
        self.assertIn(".whenMatchedUpdateAll(condition=", policy)
        self.assertIn(".whenNotMatchedInsertAll()", policy)
        self.assertIn("delta.enableChangeDataFeed", policy)

        state = {"POL-001-C01": {
            "chunk_id": "POL-001-C01", "document_id": "POL-001",
            "title": "Old", "text": "Original.", "source": "synthetic_demo_policy",
        }}
        class ExistingTable:
            columns = ["chunk_id", "document_id", "title", "text", "source"]
        class Source:
            def __init__(self, rows):
                self.rows = rows
            def alias(self, name):
                self.alias_name = name
                return self
        class Builder:
            def __init__(self):
                self.condition = None
                self.when_changed = False
                self.when_inserted = False
            def merge(self, incoming, key):
                self.incoming = incoming
                self.condition = key
                return self
            def whenMatchedUpdateAll(self, condition=None):
                self.when_changed = bool(condition)
                return self
            def whenNotMatchedInsertAll(self):
                self.when_inserted = True
                return self
            def execute(self):
                assert self.condition == "target.chunk_id = incoming.chunk_id"
                assert self.when_changed and self.when_inserted
                for row in self.incoming.rows:
                    if row["chunk_id"] not in state or state[row["chunk_id"]] != row:
                        state[row["chunk_id"]] = dict(row)
        class DeltaTable:
            @classmethod
            def forName(cls, spark, name):
                return cls()
            def alias(self, name):
                assert name == "target"
                return Builder()
        delta_tables = types.ModuleType("delta.tables")
        delta_tables.DeltaTable = DeltaTable
        delta = types.ModuleType("delta")
        delta.tables = delta_tables
        fake_spark = types.SimpleNamespace(
            catalog=types.SimpleNamespace(tableExists=lambda _: True),
            table=lambda _: ExistingTable(),
        )
        merge = function_from_notebook(
            POLICY, "upsert_policy_chunks", {},
        )
        incoming = Source([
            {"chunk_id":"POL-001-C01","document_id":"POL-001",
             "title":"New","text":"Revised.","source":"synthetic_demo_policy"},
            {"chunk_id":"POL-002-C01","document_id":"POL-002",
             "title":"Second","text":"New.","source":"synthetic_demo_policy"},
        ])
        with patch.dict(sys.modules, {"delta":delta,"delta.tables":delta_tables}):
            self.assertEqual(merge(fake_spark, "workspace.careflow360_gold.policy_chunks", incoming), "merged")
            self.assertEqual(len(state), 2)
            self.assertEqual(state["POL-001-C01"]["text"], "Revised.")
            snapshot = {k: dict(v) for k,v in state.items()}
            self.assertEqual(merge(fake_spark, "workspace.careflow360_gold.policy_chunks", incoming), "merged")
            self.assertEqual(state, snapshot)


if __name__ == "__main__":
    unittest.main()
