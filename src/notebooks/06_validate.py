# Databricks notebook source
# MAGIC %md
# MAGIC # CareFlow360 — Deployment validation
# MAGIC Read-only aggregate checks. This is a synthetic portfolio project, not a clinical system.

# COMMAND ----------

import json
import math
import re
from pyspark.sql import functions as F


def widget(name, default):
    try:
        dbutils.widgets.text(name, str(default), name)
    except Exception:
        pass
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return str(default)


def safe_identifier(raw):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", raw):
        raise ValueError(f"Invalid Unity Catalog identifier: {raw!r}")
    return raw


catalog = safe_identifier(widget("catalog", spark.sql("SELECT current_catalog() AS c").first()["c"]))
schema = safe_identifier(widget("schema", "careflow360"))
landing_volume = safe_identifier(widget("landing_volume", "landing"))
bronze_schema = safe_identifier(widget("bronze_schema", "careflow360_bronze"))
silver_schema = safe_identifier(widget("silver_schema", "careflow360_silver"))
gold_schema = safe_identifier(widget("gold_schema", "careflow360_gold"))
layout = widget("layout", "auto").lower()
max_snapshot_age_hours = float(widget("max_snapshot_age_hours", "36"))
expected_run_id = widget("expected_run_id", "").strip()
expected_new_patients_raw = widget("expected_new_patients", "").strip()
# Both values must arrive together. Missing upstream task values must fail,
# not quietly disable the insert checks.
if bool(expected_run_id) != bool(expected_new_patients_raw):
    raise ValueError("expected_run_id and expected_new_patients must both be supplied")
if expected_run_id:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", expected_run_id):
        raise ValueError(f"Invalid expected_run_id: {expected_run_id!r}")
    if not expected_new_patients_raw.isdigit() or int(expected_new_patients_raw) < 1:
        raise ValueError("expected_new_patients must be a positive integer")
    expected_new_patients = int(expected_new_patients_raw)
else:
    expected_new_patients = None
if layout not in {"auto", "split", "unified"}:
    raise ValueError("layout must be auto, split, or unified")
if not math.isfinite(max_snapshot_age_hours) or max_snapshot_age_hours <= 0:
    raise ValueError("max_snapshot_age_hours must be finite and positive")


def table(schema_name, name):
    return f"{catalog}.{schema_name}.{name}"


def verify_prediction_pair_grain(prediction_table_name):
    """Prediction grain contract: one row per unique non-null encounter pair.

    triage_predictions is append-only at (patient_id, encounter_id) grain.
    Duplicate pairs or null keys fan out every downstream join
    (triage_model_quality, triage_predictions_current) and would silently
    defeat the composite-key insert-only merge in the ML notebook, so
    surface them as hard validation failures here.
    """
    failures = []
    duplicates = (
        spark.table(prediction_table_name)
        .groupBy("patient_id", "encounter_id")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicates:
        failures.append(
            "Batch predictions contain duplicate (patient_id, encounter_id) rows"
        )
    null_keys = (
        spark.table(prediction_table_name)
        .where(F.col("patient_id").isNull() | F.col("encounter_id").isNull())
        .limit(1)
        .count()
    )
    if null_keys:
        failures.append(
            "Batch predictions contain null patient_id or encounter_id keys"
        )
    return failures


# Auto-detect the presentation views that the DAB creates.
if layout == "auto":
    layout = "split" if spark.catalog.tableExists(table(gold_schema, "patient_360")) else "unified"

bronze = ["triage", "vitals", "adt", "chief_complaints"]
silver = ["triage", "triage_quarantine", "vitals", "adt", "chief_complaints", "patient_location_history"]
gold = [
    "patient_360", "hospital_command_center", "department_hourly", "live_alerts",
    "triage_training", "triage_predictions", "policy_chunks",
]

if layout == "split":
    required = (
        [table(bronze_schema, name) for name in bronze]
        + [table(silver_schema, name) for name in silver]
        + [table(gold_schema, name) for name in gold]
    )
    triage = table(silver_schema, "triage")
    patient = table(gold_schema, "patient_360")
    command = table(gold_schema, "hospital_command_center")
    prediction = table(gold_schema, "triage_predictions")
else:
    required = (
        [table(schema, "bronze_" + name) for name in bronze]
        + [table(schema, "silver_" + name) for name in silver]
        + [table(schema, "gold_" + name) for name in gold]
    )
    triage = table(schema, "silver_triage")
    patient = table(schema, "gold_patient_360")
    command = table(schema, "gold_hospital_command_center")
    prediction = table(schema, "gold_triage_predictions")

print(f"Validating CareFlow360 {layout} layout, catalog={catalog}")
errors = []
for name in required:
    if not spark.catalog.tableExists(name):
        errors.append(f"MISSING TABLE: {name}")
        continue
    # Check presence with a bounded read instead of COUNT(*) on every
    # growing pipeline table; exact per-run counts are checked below.
    has_rows = spark.table(name).limit(1).count() > 0
    print(f"{name}: {'nonempty' if has_rows else 'empty'}")
    # Quarantine and alerts can legitimately have zero rows.
    if not has_rows and not name.endswith(("triage_quarantine", "live_alerts", "gold_live_alerts")):
        errors.append(f"EMPTY REQUIRED TABLE: {name}")

# COMMAND ----------

if spark.catalog.tableExists(triage):
    invalid = spark.table(triage).where(
        F.col("triage_acuity").isNull() | (~F.col("triage_acuity").between(1, 5))
    ).count()
    if invalid:
        errors.append(f"Silver triage invalid/missing acuity: {invalid}")

if spark.catalog.tableExists(patient):
    counts = spark.table(patient).agg(
        F.count("*").alias("rows"),
        F.countDistinct("patient_id").alias("distinct_ids"),
    ).first()
    print(f"Patient 360: {counts['rows']} rows, {counts['distinct_ids']} distinct patients")
    if counts["rows"] != counts["distinct_ids"]:
        errors.append("Patient 360 must contain exactly one row per non-null patient_id")

if spark.catalog.tableExists(prediction):
    invalid = spark.table(prediction).where(
        F.col("predicted_triage_acuity").isNull()
        | (~F.col("predicted_triage_acuity").between(1, 5))
    ).count()
    if invalid:
        errors.append(f"Invalid batch prediction labels: {invalid}")
    pair_failures = verify_prediction_pair_grain(prediction)
    if pair_failures:
        errors.extend(pair_failures)
    else:
        print("Prediction grain: one row per unique non-null (patient_id, encounter_id)")

if spark.catalog.tableExists(prediction) and spark.catalog.tableExists(patient):
    # Current-encounter prediction coverage: every (patient_id, encounter_id)
    # pair in Patient 360 must have its own stored prediction row. The
    # prediction table is append-only per encounter, so a returning patient's
    # earlier encounters keep their history while the CURRENT encounter must
    # still be scored by its own row.
    uncovered = (
        spark.table(patient).select("patient_id", "encounter_id").distinct()
        .join(
            spark.table(prediction).select("patient_id", "encounter_id").distinct(),
            ["patient_id", "encounter_id"], "left_anti",
        )
        .limit(1).count()
    )
    if uncovered:
        errors.append(
            "Prediction coverage gap: current Patient 360 encounters without their own stored prediction"
        )
    else:
        print("Prediction coverage: every current encounter has its own stored prediction")

    # Bed-occupancy observation: two currently-admitted patients sharing one
    # bed is physically impossible. Reported as a WARNING (not fatal) because
    # legacy pre-refactor synthetic encounters violate this rule and age out
    # over time; new generator runs enforce unique bed occupancy per bed.
    bed_conflicts = (
        spark.table(patient)
        .where(F.col("is_currently_admitted") & F.col("current_bed_id").isNotNull())
        .groupBy("current_department", "current_bed_id")
        .agg(F.countDistinct("patient_id").alias("patients"))
        .where(F.col("patients") > 1)
    )
    conflict_rows = bed_conflicts.count()
    if conflict_rows:
        conflict_patients = bed_conflicts.agg(F.sum("patients").alias("p")).first()["p"]
        print(
            f"WARNING: {conflict_rows} beds currently hold more than one admitted "
            f"patient ({conflict_patients} patients involved). Expected only in legacy "
            f"synthetic data; new generator runs enforce unique bed occupancy."
        )
    else:
        print("Bed occupancy: at most one currently-admitted patient per bed")

if spark.catalog.tableExists(command):
    freshness = spark.table(command).agg(
        F.count("*").alias("row_count"),
        F.max("snapshot_ts").alias("latest_ts"),
        ((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("snapshot_ts"))) / 3600.0).alias("age_hours"),
    ).first()
    if freshness["row_count"] != 1:
        errors.append("Command-center KPI must contain exactly one snapshot row")
    if freshness["latest_ts"] is None:
        errors.append("Missing command-center KPI snapshot")
    else:
        age = float(freshness["age_hours"])
        print(f"Snapshot age: {age:.2f} hours")
        if age > max_snapshot_age_hours:
            errors.append(f"KPI snapshot older than {max_snapshot_age_hours} hours")
        if age < -1:
            errors.append("Command-center KPI snapshot is unexpectedly future-dated")

# COMMAND ----------

def expected_stream_counts_from_manifest(run_id, patient_count, catalog_name, schema_name, volume_name):
    """Read the immutable generator manifest and verify the exact run contract."""
    manifest_path = (
        f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/manifests/{run_id}.json"
    )
    manifest = json.loads(dbutils.fs.head(manifest_path, 65536))
    if manifest.get("run_id") != run_id or manifest.get("num_patients") != patient_count:
        raise RuntimeError(
            f"Generator manifest disagrees with job task values for run {run_id}"
        )
    counts = manifest.get("rows")
    names = ("triage", "vitals", "adt", "chief_complaints")
    if not isinstance(counts, dict):
        raise RuntimeError("Generator manifest has no stream row counts")
    for name in names:
        value = counts.get(name)
        if type(value) is not int or value < patient_count:
            raise RuntimeError(f"Generator manifest has invalid {name} count: {value!r}")
    if counts["triage"] != patient_count or counts["chief_complaints"] != patient_count:
        raise RuntimeError("Generator manifest is not one triage/complaint per new patient")
    return {name: counts[name] for name in names}


def verify_new_patient_inserts(expected_run_id, expected_new_patients, assets):
    """Fail if this scheduled run did not add one unique row per patient.

    A new run ID makes patient IDs non-overlapping across job executions.
    Auto Loader/Gold materialized views then INSERT these as new patients,
    while SCD2 continues to track changes to existing encounters.
    """
    prefix = f"P{expected_run_id}-"
    failures = []
    for asset in assets:
        if len(asset) == 2:
            layer, name = asset
            expected_rows = expected_new_patients
        else:
            layer, name, expected_rows = asset
        if not spark.catalog.tableExists(name):
            failures.append(f"{layer}: missing table {name}")
            continue
        counts = (
            spark.table(name)
            .where(F.col("patient_id").startswith(prefix))
            .agg(
                F.count("*").alias("rows"),
                F.countDistinct("patient_id").alias("unique_patients"),
            )
            .first()
        )
        rows = int(counts["rows"])
        unique_patients = int(counts["unique_patients"])
        print(f"Run {expected_run_id}: {layer} inserted rows={rows}, distinct_patients={unique_patients}")
        if unique_patients != expected_new_patients or rows != expected_rows:
            failures.append(
                f"{layer}: expected rows={expected_rows}, patients={expected_new_patients} "
                f"for run {expected_run_id}, got rows={rows}, distinct={unique_patients}"
            )
    return failures


if expected_new_patients is not None:
    # Check the real generator manifest, not only that a patient_id prefix exists:
    # a run may ingest all triage rows while losing its ADT/vitals or complaints.
    expected_rows = expected_stream_counts_from_manifest(
        expected_run_id, expected_new_patients, catalog, schema, landing_volume
    )
    streams = ("triage", "vitals", "adt", "chief_complaints")
    if layout == "split":
        insert_assets = [
            (f"bronze_{name}", table(bronze_schema, name), expected_rows[name])
            for name in streams
        ] + [
            (f"silver_{name}", table(silver_schema, name), expected_rows[name])
            for name in streams
        ]
    else:
        insert_assets = [
            (f"bronze_{name}", table(schema, "bronze_" + name), expected_rows[name])
            for name in streams
        ] + [
            (f"silver_{name}", table(schema, "silver_" + name), expected_rows[name])
            for name in streams
        ]
    insert_assets += [
        ("gold_patient_360", patient, expected_new_patients),
        ("gold_triage_predictions", prediction, expected_new_patients),
    ]
    errors.extend(verify_new_patient_inserts(
        expected_run_id, expected_new_patients, insert_assets
    ))

# COMMAND ----------

if errors:
    for error in errors:
        print("FAIL:", error)
    raise RuntimeError(f"CareFlow360 failed {len(errors)} validation checks")

print("PASS: required assets, Patient 360 grain, acuity ranges and KPI freshness")
print("These engineering tests do not establish clinical safety or production reliability.")