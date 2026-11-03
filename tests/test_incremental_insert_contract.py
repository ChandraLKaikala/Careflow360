"""Executable offline contracts for insert-new-patients and insert-only Delta MERGE.

These mocks do not replace a Databricks/Lakeflow runtime integration run.
"""
import ast
import pathlib
import types
import unittest

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
VALIDATION_PATH = ROOT / "src/notebooks/06_validate.py"
ML_PATH = ROOT / "src/notebooks/04_train_triage_model.py"
GENERATOR_PATH = ROOT / "src/notebooks/01_generate_sample_data.py"


def function_from_notebook(path, name, globals_):
    tree = ast.parse(path.read_text(encoding="utf8"))
    fns = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name]
    assert len(fns) == 1, (path, name, fns)
    scope = dict(globals_)
    exec(compile(ast.Module(body=fns, type_ignores=[]), str(path), "exec"), scope)
    return scope[name]


class Column:
    def __init__(self, name):
        self.name = name

    def startswith(self, prefix):
        return ("prefix", self.name, prefix)

    def __gt__(self, value):
        return ("gt", self.name, value)


class Aggregate:
    def __init__(self, expression):
        self.expression = expression

    def alias(self, alias):
        return self


class Functions:
    @staticmethod
    def col(name):
        return Column(name)

    @staticmethod
    def count(_):
        return Aggregate("count")

    @staticmethod
    def countDistinct(_):
        return Aggregate("countDistinct")


class FakeTable:
    def __init__(self, rows):
        self.rows = rows
        self.columns = ["patient_id", "encounter_id", "predicted_triage_acuity"]

    def where(self, predicate):
        operation, column, value = predicate
        if operation == "prefix":
            return FakeTable([r for r in self.rows if r[column].startswith(value)])
        if operation == "gt":
            return FakeTable([r for r in self.rows if r[column] > value])
        raise ValueError(predicate)

    def agg(self, *args):
        rows = self.rows
        return types.SimpleNamespace(
            first=lambda: {"rows": len(rows), "unique_patients": len({r["patient_id"] for r in rows})}
        )

    def groupBy(self, *names):
        groups = {}
        for row in self.rows:
            key = tuple(row[name] for name in names) if len(names) > 1 else row[names[0]]
            groups[key] = groups.get(key, 0) + 1
        return FakeGrouped(groups)

    def limit(self, n):
        return FakeTable(self.rows[:n])

    def count(self):
        return len(self.rows)


class FakeGrouped:
    def __init__(self, groups):
        self.groups = groups

    def count(self):
        rows = []
        for key, n in self.groups.items():
            if isinstance(key, tuple):
                row = dict(zip(("patient_id", "encounter_id"), key))
            else:
                row = {"patient_id": key}
            row["count"] = n
            rows.append(row)
        return FakeTable(rows)


class FakeSpark:
    def __init__(self, datasets):
        self.datasets = datasets
        self.catalog = types.SimpleNamespace(tableExists=lambda name: name in self.datasets)

    def table(self, name):
        return FakeTable(self.datasets[name])


class FakeDeltaBuilder:
    def __init__(self, target):
        self.target = target
        self.source = None
        self.key = None
        self.insert_only = False

    def merge(self, source, condition):
        self.source = source
        self.key = condition
        return self

    def whenNotMatchedInsertAll(self):
        self.insert_only = True
        return self

    def execute(self):
        assert self.key == ("target.patient_id = new.patient_id "
                            "AND target.encounter_id = new.encounter_id")
        assert self.insert_only
        existing = {(r["patient_id"], r["encounter_id"]) for r in self.target.rows}
        for row in self.source.rows:
            pair = (row["patient_id"], row["encounter_id"])
            if pair not in existing:
                self.target.rows.append(dict(row))
                existing.add(pair)


class FakeDeltaTable:
    target = None

    @classmethod
    def forName(cls, spark, name):
        cls.target = spark.table(name)
        return cls

    @classmethod
    def alias(cls, name):
        assert name == "target"
        return FakeDeltaBuilder(cls.target)


class FakeSource:
    def __init__(self, rows):
        self.rows = rows
        self.columns = ["patient_id", "encounter_id", "predicted_triage_acuity"]

    def alias(self, name):
        assert name == "new"
        return self


class InsertValidationTests(unittest.TestCase):
    def contract(self, data):
        spark = FakeSpark(data)
        return function_from_notebook(VALIDATION_PATH, "verify_new_patient_inserts", {"spark": spark, "F": Functions})

    @staticmethod
    def datasets(n=100):
        old = [{"patient_id": f"P{i:06d}"} for i in range(1, 601)]
        new = [{"patient_id": f"Pjob123-{i:06d}"} for i in range(1, n + 1)]
        return {
            "bronze": old + new,
            "silver": old + new,
            "patient_360": old + new,
            "predictions": old + new,
        }

    @staticmethod
    def assets():
        return [("bronze_triage", "bronze"), ("silver_triage", "silver"),
                ("gold_patient_360", "patient_360"),
                ("gold_triage_predictions", "predictions")]

    def test_600_existing_plus_100_new_in_every_layer(self):
        data = self.datasets()
        func = self.contract(data)
        self.assertEqual(func("job123", 100, self.assets()), [])
        self.assertEqual(len(data["patient_360"]), 700)

    def test_silent_refresh_without_new_gold_rows_is_rejected(self):
        data = self.datasets()
        data["patient_360"] = data["patient_360"][:600]
        errors = self.contract(data)("job123", 100, self.assets())
        self.assertEqual(len(errors), 1)
        self.assertIn("gold_patient_360", errors[0])
        self.assertIn("got rows=0", errors[0])

    def test_duplicates_and_incomplete_predictions_are_rejected(self):
        data = self.datasets()
        data["predictions"] = data["predictions"][:-1]
        data["silver"].append({"patient_id": "Pjob123-000001"})
        errors = self.contract(data)("job123", 100, self.assets())
        self.assertEqual(len(errors), 2)
        self.assertTrue(any("silver_triage" in err and "rows=101" in err for err in errors))
        self.assertTrue(any("gold_triage_predictions" in err and "distinct=99" in err for err in errors))

    def test_missing_source_table_fails(self):
        data = self.datasets()
        del data["bronze"]
        errors = self.contract(data)("job123", 100, self.assets())
        self.assertEqual(len(errors), 1)
        self.assertIn("missing table", errors[0])


class PredictionMergeTests(unittest.TestCase):
    def test_merge_preserves_old_predictions_and_adds_new_only(self):
        rows = [{"patient_id": "P000001", "encounter_id": "old", "predicted_triage_acuity": 2}]
        target_dataset = FakeTable(rows)
        spark = FakeSpark({"predictions": rows})
        spark.table = lambda _: target_dataset
        FakeDeltaTable.target = target_dataset
        source = FakeSource([
            {"patient_id": "P000001", "encounter_id": "old", "predicted_triage_acuity": 5},
            {"patient_id": "P000001", "encounter_id": "new", "predicted_triage_acuity": 4},
            {"patient_id": "Pjob123-000001", "encounter_id": "new", "predicted_triage_acuity": 1},
        ])
        guard = function_from_notebook(ML_PATH, "insert_new_patient_predictions",
                                       {"DeltaTable": FakeDeltaTable, "F": Functions})
        self.assertEqual(guard(spark, "predictions", source), "insert_only_merge")
        # Existing pair untouched (insert-only); the returning patient's NEW
        # encounter and the new patient's encounter are appended: history kept.
        self.assertEqual(len(target_dataset.rows), 3)
        self.assertEqual(target_dataset.rows[0]["predicted_triage_acuity"], 2)
        self.assertEqual(target_dataset.rows[1]["encounter_id"], "new")
        self.assertEqual(target_dataset.rows[2]["patient_id"], "Pjob123-000001")

    def test_existing_duplicate_patient_encounter_pairs_are_rejected(self):
        spark = FakeSpark({"predictions": [
            {"patient_id": "P1", "encounter_id": "E1", "predicted_triage_acuity": 2},
            {"patient_id": "P1", "encounter_id": "E1", "predicted_triage_acuity": 3},
        ]})
        guard = function_from_notebook(ML_PATH, "insert_new_patient_predictions",
                                       {"DeltaTable": FakeDeltaTable, "F": Functions})
        with self.assertRaisesRegex(RuntimeError, r"duplicate \(patient_id, encounter_id\) pairs"):
            guard(spark, "predictions", FakeSource([]))

    def test_returning_patient_history_rows_are_not_duplicates(self):
        # Encounter grain: two rows for one patient with DIFFERENT encounters
        # are legitimate history, not a duplicate-pair violation.
        spark = FakeSpark({"predictions": [
            {"patient_id": "P1", "encounter_id": "E1", "predicted_triage_acuity": 2},
            {"patient_id": "P1", "encounter_id": "E2", "predicted_triage_acuity": 3},
        ]})
        guard = function_from_notebook(ML_PATH, "insert_new_patient_predictions",
                                       {"DeltaTable": FakeDeltaTable, "F": Functions})
        self.assertEqual(
            guard(spark, "predictions", FakeSource([
                {"patient_id": "P2", "encounter_id": "E9", "predicted_triage_acuity": 1},
            ])),
            "insert_only_merge",
        )


class JobParameterTests(unittest.TestCase):
    def test_schedule_uses_unique_run_id_and_verified_insert_counts(self):
        job = yaml.safe_load((ROOT / "resources/03_jobs.yml").read_text())["resources"]["jobs"]["careflow360_job"]
        by = {task["task_key"]: task for task in job["tasks"]}
        gen = by["generate_sample_events"]["notebook_task"]["base_parameters"]
        validation = by["validate_project"]["notebook_task"]["base_parameters"]
        self.assertEqual(gen["run_id"], "{{job.run_id}}")
        self.assertEqual(gen["num_patients"], "random")
        self.assertEqual(gen["reset_events"], "false")
        self.assertEqual(validation["expected_run_id"], "{{tasks.generate_sample_events.values.run_id}}")
        self.assertEqual(validation["expected_new_patients"], "{{tasks.generate_sample_events.values.num_patients}}")
        self.assertIn('taskValues.set(key="run_id"', GENERATOR_PATH.read_text())
        self.assertIn('taskValues.set(key="num_patients"', GENERATOR_PATH.read_text())


if __name__ == "__main__":
    unittest.main(verbosity=2)
