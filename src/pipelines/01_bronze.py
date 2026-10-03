"""
CareFlow360 Bronze Layer — Raw Event Ingestion
================================================

Medallion Architecture: Bronze → Silver → Gold

This module implements the BRONZE layer of the CareFlow360 hospital analytics
lakehouse. Bronze tables are append-only, schema-enforced, and DQ-gated raw
events ingested via Auto Loader from JSON files in the landing volume.

All tables are created in the SDP pipeline's working schema (careflow360_dab).
Presentation views in workspace.careflow360_bronze expose these tables to
downstream consumers with proper access control.

Data Quality:
  - expect_or_drop on event_id, patient_id, event_ts (non-null)
  - Schema enforced via StructType — no schema drift

Sources:
  - /Volumes/{catalog}/{schema}/landing/events/{triage,vitals,adt,chief_complaints}/*.json

Author: CareFlow360 Team
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Pipeline configuration — defaults match DAB pipeline settings
CATALOG = spark.conf.get("careflow.catalog", "workspace")
SCHEMA = spark.conf.get("careflow.schema", "careflow360")
VOLUME = spark.conf.get("careflow.landing_volume", "landing")
ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/events"

# ---------------------------------------------------------------------------
# Schema definitions — explicit StructType enforces contract at ingest time
# ---------------------------------------------------------------------------

triage_schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("patient_id", T.StringType()),
    T.StructField("encounter_id", T.StringType()),
    T.StructField("event_ts", T.TimestampType()),
    T.StructField("arrival_mode", T.StringType()),
    T.StructField("arrival_hour", T.IntegerType()),
    T.StructField("arrival_day", T.StringType()),
    T.StructField("age", T.IntegerType()),
    T.StructField("sex", T.StringType()),
    T.StructField("systolic_bp", T.IntegerType()),
    T.StructField("diastolic_bp", T.IntegerType()),
    T.StructField("heart_rate", T.IntegerType()),
    T.StructField("respiratory_rate", T.IntegerType()),
    T.StructField("temperature_c", T.DoubleType()),
    T.StructField("spo2", T.IntegerType()),
    T.StructField("gcs_total", T.IntegerType()),
    T.StructField("pain_score", T.IntegerType()),
    T.StructField("num_prior_visits", T.IntegerType()),
    T.StructField("triage_acuity", T.IntegerType()),
    T.StructField("source", T.StringType()),
])

vitals_schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("patient_id", T.StringType()),
    T.StructField("encounter_id", T.StringType()),
    T.StructField("event_ts", T.TimestampType()),
    T.StructField("heart_rate", T.IntegerType()),
    T.StructField("spo2", T.IntegerType()),
    T.StructField("systolic_bp", T.IntegerType()),
    T.StructField("diastolic_bp", T.IntegerType()),
    T.StructField("temperature_c", T.DoubleType()),
    T.StructField("respiratory_rate", T.IntegerType()),
    T.StructField("fall_detected", T.BooleanType()),
    T.StructField("source", T.StringType()),
])

adt_schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("patient_id", T.StringType()),
    T.StructField("encounter_id", T.StringType()),
    T.StructField("event_ts", T.TimestampType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("department", T.StringType()),
    T.StructField("bed_id", T.StringType()),
    T.StructField("operation", T.StringType()),
    T.StructField("source", T.StringType()),
])

chief_schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("patient_id", T.StringType()),
    T.StructField("encounter_id", T.StringType()),
    T.StructField("event_ts", T.TimestampType()),
    T.StructField("chief_complaint_raw", T.StringType()),
    T.StructField("source", T.StringType()),
])


def autoload(path, schema):
    """Auto Loader streaming read with schema enforcement and ingest metadata."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .schema(schema)
        .load(path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ---------------------------------------------------------------------------
# Bronze streaming tables — DQ-gated with expect_or_drop
# ---------------------------------------------------------------------------

@dp.table(name="bronze_triage", comment="Raw ED triage events from synthetic/Kaggle replay sources.")
@dp.expect_or_drop("triage_has_ids", "event_id IS NOT NULL AND patient_id IS NOT NULL AND event_ts IS NOT NULL")
def bronze_triage():
    return autoload(f"{ROOT}/triage", triage_schema)


@dp.table(name="bronze_vitals", comment="Raw bedside monitoring event stream.")
@dp.expect_or_drop("vitals_has_ids", "event_id IS NOT NULL AND patient_id IS NOT NULL AND event_ts IS NOT NULL")
def bronze_vitals():
    return autoload(f"{ROOT}/vitals", vitals_schema)


@dp.table(name="bronze_adt", comment="Raw admission, transfer and discharge events.")
@dp.expect_or_drop("adt_has_ids", "event_id IS NOT NULL AND patient_id IS NOT NULL AND event_ts IS NOT NULL")
def bronze_adt():
    return autoload(f"{ROOT}/adt", adt_schema)


@dp.table(name="bronze_chief_complaints", comment="Raw ED chief-complaint text events.")
@dp.expect_or_drop("complaint_has_ids", "event_id IS NOT NULL AND patient_id IS NOT NULL AND event_ts IS NOT NULL")
def bronze_chief_complaints():
    return autoload(f"{ROOT}/chief_complaints", chief_schema)
