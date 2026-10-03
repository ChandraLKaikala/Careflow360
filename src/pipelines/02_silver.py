"""
CareFlow360 Silver Layer — Cleansed & Enriched Events
======================================================

Medallion Architecture: Bronze → Silver → Gold

The SILVER layer transforms raw bronze events into cleansed, deduplicated,
enriched, and SCD2-tracked streaming tables. Key operations:

  - Deduplication via watermark + dropDuplicates on event_id
  - Data quality validation (expect_or_drop on acuity, age ranges, event types)
  - Derived columns: shock_index, pulse_pressure, operational_alert_flag
  - Quarantine table captures rejected records for DQ investigation
  - AUTO CDC with SCD Type 2 for patient location history

All tables are created in the SDP pipeline working schema (careflow360_dab).
Presentation views in workspace.careflow360_silver expose these to consumers.

Author: CareFlow360 Team
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Silver triage — validated, deduplicated, enriched ED triage events
# ---------------------------------------------------------------------------

@dp.table(name="silver_triage", comment="Validated and enriched ED triage events.")
@dp.expect_or_drop("valid_acuity", "triage_acuity BETWEEN 1 AND 5")
@dp.expect_or_drop("age_in_demo_range", "age BETWEEN 0 AND 120")
def silver_triage():
    df = (
        spark.readStream.table("bronze_triage")
        .withWatermark("event_ts", "1 day")
        .dropDuplicates(["event_id"])
    )
    return (
        df
        .withColumn("shock_index", F.when(F.col("systolic_bp") > 0, F.col("heart_rate") / F.col("systolic_bp")))
        .withColumn("pulse_pressure", F.col("systolic_bp") - F.col("diastolic_bp"))
        .withColumn("arrival_date", F.to_date("event_ts"))
        .withColumn("arrival_hour_event", F.hour("event_ts"))
        .withColumn("data_quality_status", F.lit("accepted"))
    )


# ---------------------------------------------------------------------------
# Quarantine — rejected triage records with rejection reasons for DQ ops
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="silver_triage_quarantine",
    comment="Rejected triage records retained for data-quality investigation rather than silently discarded.",
)
def silver_triage_quarantine():
    df = spark.read.table("bronze_triage")
    invalid = (
        F.col("event_id").isNull()
        | F.col("patient_id").isNull()
        | F.col("event_ts").isNull()
        | F.col("triage_acuity").isNull()
        | (~F.col("triage_acuity").between(1, 5))
        | F.col("age").isNull()
        | (~F.col("age").between(0, 120))
    )
    return (
        df.where(invalid)
        .withColumn(
            "rejection_reason",
            F.when(F.col("event_id").isNull(), F.lit("missing_event_id"))
            .when(F.col("patient_id").isNull(), F.lit("missing_patient_id"))
            .when(F.col("event_ts").isNull(), F.lit("missing_event_ts"))
            .when(F.col("triage_acuity").isNull(), F.lit("missing_triage_acuity"))
            .when(~F.col("triage_acuity").between(1, 5), F.lit("invalid_triage_acuity"))
            .when(F.col("age").isNull(), F.lit("missing_age"))
            .when(~F.col("age").between(0, 120), F.lit("invalid_age"))
            .otherwise(F.lit("unknown")),
        )
    )


# ---------------------------------------------------------------------------
# Silver vitals — deduplicated bedside monitoring with operational alert flags
# ---------------------------------------------------------------------------

@dp.table(name="silver_vitals", comment="Deduplicated bedside vitals with operational demo flags.")
@dp.expect_or_drop("vital_ids", "event_id IS NOT NULL AND patient_id IS NOT NULL")
def silver_vitals():
    df = (
        spark.readStream.table("bronze_vitals")
        .withWatermark("event_ts", "1 day")
        .dropDuplicates(["event_id"])
    )
    # These broad thresholds are for a synthetic operations demo only; they are not medical guidance.
    return (
        df
        .withColumn(
            "operational_alert_flag",
            (F.col("spo2") < 90)
            | (F.col("heart_rate") > 130)
            | (F.col("heart_rate") < 45)
            | (F.col("systolic_bp") < 85)
            | (F.col("temperature_c") > 39.5)
            | (F.col("fall_detected") == F.lit(True)),
        )
        .withColumn("vital_date", F.to_date("event_ts"))
    )


# ---------------------------------------------------------------------------
# Silver ADT — deduplicated admission/transfer/discharge events
# ---------------------------------------------------------------------------

@dp.table(name="silver_adt", comment="Deduplicated hospital admission/transfer/discharge event stream.")
@dp.expect_or_drop("valid_adt_event", "event_type IN ('A01_ADMIT','A02_TRANSFER','A03_DISCHARGE')")
def silver_adt():
    return (
        spark.readStream.table("bronze_adt")
        .withWatermark("event_ts", "1 day")
        .dropDuplicates(["event_id"])
        .withColumn("event_date", F.to_date("event_ts"))
    )


# ---------------------------------------------------------------------------
# Silver chief complaints — cleaned ED chief-complaint text
# ---------------------------------------------------------------------------

@dp.table(name="silver_chief_complaints", comment="Cleaned ED chief-complaint text.")
@dp.expect_or_drop("complaint_present", "chief_complaint_raw IS NOT NULL AND length(trim(chief_complaint_raw)) > 0")
def silver_chief_complaints():
    return (
        spark.readStream.table("bronze_chief_complaints")
        .withWatermark("event_ts", "1 day")
        .dropDuplicates(["event_id"])
        .withColumn("chief_complaint_clean", F.lower(F.trim("chief_complaint_raw")))
    )


# ---------------------------------------------------------------------------
# AUTO CDC — SCD Type 2 patient location/event history
# Tracks changes to patient location (department, bed) over time.
# ---------------------------------------------------------------------------

dp.create_streaming_table(
    name="silver_patient_location_history",
    comment="SCD Type 2 patient location/event history built with AUTO CDC.",
)

dp.create_auto_cdc_flow(
    target="silver_patient_location_history",
    source="silver_adt",
    keys=["patient_id", "encounter_id"],
    sequence_by=F.col("event_ts"),
    ignore_null_updates=False,
    except_column_list=["operation", "_ingest_ts", "_source_file", "event_date"],
    stored_as_scd_type="2",
)
