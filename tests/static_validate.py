"""Offline CareFlow360 repository integrity checks (no Databricks connection)."""
from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def check_python():
    errors = []
    for path in REPO.rglob("*.py"):
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            errors.append(f"Python syntax error in {path.relative_to(REPO)}: {exc}")
    return errors


def check_notebooks():
    errors = []
    for root in (REPO / "src/notebooks",):
        for path in root.glob("*.py"):
            text = path.read_text(encoding="utf-8")
            if not text.startswith("# Databricks notebook source"):
                errors.append(f"Missing notebook source header: {path.relative_to(REPO)}")
    for path in REPO.rglob("*.ipynb"):
        try:
            notebook = json.loads(path.read_text(encoding="utf-8"))
            if notebook.get("nbformat") != 4:
                errors.append(f"Expected notebook format 4: {path.relative_to(REPO)}")
            for index, cell in enumerate(notebook.get("cells", [])):
                if cell["cell_type"] == "code":
                    code = "".join(cell.get("source", []))
                    if not code.strip():
                        errors.append(f"Empty code cell: {path.relative_to(REPO)} cell {index}")
                    ast.parse(code, filename=f"{path}:{index}")
        except (ValueError, KeyError, SyntaxError) as exc:
            errors.append(f"Notebook invalid: {path.relative_to(REPO)}: {exc}")
    return errors


def check_incremental_contract():
    errors = []
    script = (REPO / "src/notebooks/01_generate_sample_data.py").read_text(encoding="utf-8")
    if 'reset_events = widget("reset_events", "false")' not in script:
        errors.append("Generator must default to non-destructive replay")
    if 'f"P{run_id}-{i:06d}"' not in script:
        errors.append("Patient IDs must be unique per replay run")
    if 'dbutils.fs.put(f"{manifest_dir}/{run_id}.json"' not in script:
        errors.append("A per-run replay manifest must be preserved")
    job = (REPO / "resources/03_jobs.yml").read_text(encoding="utf-8")
    if 'reset_events: "false"' not in job or 'num_patients: "random"' not in job:
        errors.append("DAB job must use safe daily append parameters")
    model = (REPO / "src/notebooks/04_train_triage_model.py").read_text(encoding="utf-8")
    if "presentation_source" not in model or "prediction_table" not in model:
        errors.append("ML notebook must handle split-schema table names")
    return errors


def check_yaml_and_payloads():
    errors = []
    try:
        import yaml
    except ImportError:
        errors.append("PyYAML required for full YAML syntax validation")
        return errors
    for path in list(REPO.rglob("*.yml")) + list(REPO.rglob("*.yaml")):
        try:
            yaml.safe_load(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            errors.append(f"Invalid YAML in {path.relative_to(REPO)}: {exc}")
    bad_names = {"train.csv", "test.csv", "chief_complaints.csv", "sample_submission.csv"}
    for path in REPO.rglob("*"):
        if path.is_file() and path.name in bad_names:
            errors.append(f"Unexpected raw Kaggle dataset: {path.relative_to(REPO)}")
    return errors


def main():
    errors = []
    for task in (check_python, check_notebooks,
                 check_incremental_contract, check_yaml_and_payloads):
        errors.extend(task())
    if errors:
        print("CAREFlow360 STATIC VALIDATION FAILED")
        for error in errors:
            print("FAIL:", error)
        return 1
    print("CAREFlow360 STATIC VALIDATION PASSED")
    print("Python AST, notebook cells, YAML, and replay contracts verified.")
    print("Databricks runtime, schedules, permissions and deployment are not tested here.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
