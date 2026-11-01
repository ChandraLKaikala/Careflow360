"""Static per-cell compilation and Spark Declarative Pipeline dependency audit.

This verifies source contracts; it does NOT execute a Spark or Databricks pipeline.
"""
import ast
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
PIPELINES = ROOT / "src/pipelines"
NOTEBOOKS = [ROOT / "src/notebooks",
             ROOT / "CareFlow360_Databricks_Free_Edition/core/src/notebooks",
             ROOT / "presentations"]


def constant_string(node):
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def keyword(call, name):
    return next((constant_string(v.value) for v in call.keywords if v.arg == name), None)


class CellAndPipelineTests(unittest.TestCase):
    def test_every_databricks_source_cell_compiles(self):
        cells_checked = 0
        for root in NOTEBOOKS:
            for path in root.glob("*.py"):
                source = path.read_text(encoding="utf-8")
                sections = source.split("# COMMAND ----------")
                for cell_index, cell in enumerate(sections):
                    ast.parse(cell, filename=f"{path}:{cell_index}")
                    cells_checked += 1
        self.assertGreater(cells_checked, 30)

    def test_declared_pipeline_tables_and_input_dependencies(self):
        declared = set()
        referenced = set()
        collected = {}
        for path in sorted(PIPELINES.glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)]
            for function in functions:
                for decorator in function.decorator_list:
                    if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute) and decorator.func.attr in ("table", "materialized_view"):
                        name = keyword(decorator, "name")
                        if name is None and decorator.args:
                            name = constant_string(decorator.args[0])
                        if name:
                            self.assertNotIn(name, declared, f"Duplicate pipeline table {name}")
                            declared.add(name)
                            collected[name] = path.name
            for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
                if isinstance(call.func, ast.Attribute) and call.func.attr == "create_streaming_table":
                    name = keyword(call, "name") or (constant_string(call.args[0]) if call.args else None)
                    if name:
                        self.assertNotIn(name, declared, f"Duplicate pipeline table {name}")
                        declared.add(name)
                if isinstance(call.func, ast.Attribute) and call.func.attr == "table" and call.args:
                    name = constant_string(call.args[0])
                    if name:
                        referenced.add(name)
                if isinstance(call.func, ast.Name) and call.func.id == "latest_per_patient" and call.args:
                    name = constant_string(call.args[0])
                    if name:
                        referenced.add(name)
                if isinstance(call.func, ast.Attribute) and call.func.attr == "create_auto_cdc_flow":
                    target = keyword(call, "target")
                    source = keyword(call, "source")
                    self.assertEqual(target, "silver_patient_location_history")
                    self.assertEqual(source, "silver_adt")
                    self.assertEqual(keyword(call, "stored_as_scd_type"), "2")
        self.assertEqual(len(declared), 15)
        self.assertEqual(referenced - declared, set(), f"Pipeline depends on undefined tables: {referenced - declared}")
        self.assertTrue({
            "bronze_triage", "silver_triage", "silver_patient_location_history",
            "gold_patient_360", "gold_hospital_command_center", "gold_triage_training",
        }.issubset(declared))

    def test_all_materialized_gold_views_have_unique_names(self):
        path = PIPELINES / "03_gold.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("gold_"):
                names.append(node.name)
                self.assertTrue(any(
                    isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute)
                    and d.func.attr == "materialized_view"
                    for d in node.decorator_list
                ), f"Gold source {node.name} has no materialized_view decorator")
        self.assertEqual(len(names), 5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
