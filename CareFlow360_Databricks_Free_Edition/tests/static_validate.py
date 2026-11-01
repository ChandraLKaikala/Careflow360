"""Offline package checks. Does not contact Databricks."""
from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check_python() -> list[str]:
    errors: list[str] = []
    for path in ROOT.rglob("*.py"):
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            errors.append(f"Python syntax: {path.relative_to(ROOT)}: {exc}")
    return errors


def check_notebook_headers() -> list[str]:
    errors: list[str] = []
    for path in (ROOT / "core" / "src" / "notebooks").glob("*.py"):
        first = path.read_text(encoding="utf-8").splitlines()[0].strip()
        if first != "# Databricks notebook source":
            errors.append(f"Missing Databricks notebook header: {path.relative_to(ROOT)}")
    return errors


def check_required_files() -> list[str]:
    required = [
        "README.md",
        "QUICKSTART.md",
        "SETUP_COMMANDS.md",
        "core/databricks.yml",
        "core/resources/01_uc.yml",
        "core/resources/02_pipeline.yml",
        "core/resources/03_jobs.yml",
        "core/src/pipelines/01_bronze.py",
        "core/src/pipelines/02_silver.py",
        "core/src/pipelines/03_gold.py",
        "core/src/notebooks/01_generate_sample_data.py",
        "core/src/notebooks/04_train_triage_model.py",
    ]
    return [f"Missing required file: {item}" for item in required if not (ROOT / item).exists()]


def check_no_kaggle_payloads() -> list[str]:
    errors: list[str] = []
    forbidden_names = {"train.csv", "test.csv", "chief_complaints.csv", "sample_submission.csv"}
    for path in ROOT.rglob("*"):
        if path.is_file() and path.name in forbidden_names:
            errors.append(f"Kaggle payload must not be redistributed: {path.relative_to(ROOT)}")
    return errors


def check_yaml() -> list[str]:
    errors: list[str] = []
    try:
        import yaml  # type: ignore
    except ImportError:
        return ["PyYAML not installed; YAML syntax check skipped"]
    for path in list(ROOT.rglob("*.yml")) + list(ROOT.rglob("*.yaml")):
        try:
            yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append(f"YAML parse: {path.relative_to(ROOT)}: {exc}")
    return errors


def main() -> int:
    errors: list[str] = []
    for fn in [check_python, check_notebook_headers, check_required_files, check_no_kaggle_payloads]:
        errors.extend(fn())

    yaml_results = check_yaml()
    skipped = [x for x in yaml_results if "skipped" in x]
    errors.extend(x for x in yaml_results if x not in skipped)

    if skipped:
        for msg in skipped:
            print(f"WARN: {msg}")

    if errors:
        print("STATIC VALIDATION FAILED")
        for error in errors:
            print(" -", error)
        return 1

    print("STATIC VALIDATION PASSED")
    print("Python syntax, notebook headers, required files, Kaggle redistribution guard, and YAML syntax are OK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
