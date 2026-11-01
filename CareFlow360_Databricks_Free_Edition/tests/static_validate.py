"""Offline CareFlow360 repository integrity checks (no Databricks connection)."""
from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]
REPO = PACKAGE.parent


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
    for root in (REPO / "src/notebooks", PACKAGE / "core/src/notebooks"):
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


def check_mirrors():
    errors = []
    mirrored = [
        "databricks.yml", "resources/01_uc.yml", "resources/02_pipeline.yml",
        "resources/03_jobs.yml",
        "src/notebooks/00_preflight.py", "src/notebooks/01_generate_sample_data.py",
        "src/notebooks/03_create_views.py", "src/notebooks/04_train_triage_model.py",
        "src/notebooks/05_create_policy_docs.py", "src/notebooks/06_validate.py",
        "src/pipelines/01_bronze.py", "src/pipelines/02_silver.py",
        "src/pipelines/03_gold.py",
    ]
    for relative in mirrored:
        left = REPO / relative
        right = PACKAGE / "core" / relative
        if not left.is_file() or not right.is_file():
            errors.append(f"Mirror missing: {relative}")
        elif left.read_bytes() != right.read_bytes():
            errors.append(f"Repository and Free Edition package differ: {relative}")
    notebook = PACKAGE / "core/src/notebooks/03_create_views.ipynb"
    source = (PACKAGE / "core/src/notebooks/03_create_views.py").read_text(encoding="utf-8")
    if notebook.is_file():
        actual = json.loads(notebook.read_text(encoding="utf-8"))
        cells = [c for c in actual["cells"] if c["cell_type"] == "code"]
        if len(cells) != 1 or "".join(cells[0]["source"]) != source.removeprefix("# Databricks notebook source\n"):
            errors.append("03_create_views.ipynb code cell differs from .py script")
    return errors


def check_incremental_contract():
    errors = []
    script = (REPO / "src/notebooks/01_generate_sample_data.py").read_text(encoding="utf-8")
    if 'reset_events = widget("reset_events", "false")' not in script:
        errors.append("Generator must default to non-destructive replay")
    if 'f"P{run_id}-{i:06d}"' not in script:
        errors.append("Patient IDs must be unique per replay run")
    if 'f"{base}/manifests/{run_id}.json"' not in script:
        errors.append("A per-run replay manifest must be preserved")
    job = (REPO / "resources/03_jobs.yml").read_text(encoding="utf-8")
    if 'reset_events: "false"' not in job or 'num_patients: "100"' not in job:
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
    for task in (check_python, check_notebooks, check_mirrors,
                 check_incremental_contract, check_yaml_and_payloads):
        errors.extend(task())
    if errors:
        print("CAREFlow360 STATIC VALIDATION FAILED")
        for error in errors:
            print("FAIL:", error)
        return 1
    print("CAREFlow360 STATIC VALIDATION PASSED")
    print("Python AST, notebook cells, YAML, mirror, and replay contracts verified.")
    print("Databricks runtime, schedules, permissions and deployment are not tested here.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
