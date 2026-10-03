"""
CareFlow360 Gold Layer — Analytics-Ready Aggregates & ML Features
==================================================================

Medallion Architecture: Bronze → Silver → Gold

The GOLD layer creates business-ready materialized views for dashboards,
Genie, ML training, and AI Search. All gold tables are materialized views
built from silver-layer streaming tables.

Key outputs:
  - gold_patient_360: Latest operational snapshot per patient
  - gold_hospital_command_center: Single-row KPI summary for dashboards
  - gold_department_hourly: Hourly ADT event counts by department
  - gold_live_alerts: Patients with active operational vital alerts
  - gold_triage_training: Curated feature table for ML model training

Note: gold_triage_predictions and gold_policy_chunks are created by
notebooks (04_train_triage_model, 05_create_policy_docs) as physical
tables in the workspace.careflow360_gold presentation schema.

All pipeline tables are in the SDP working schema (careflow360_dab).
Presentation views in workspace.careflow360_gold expose them to consumers.

Author: CareFlow360 Team
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def latest_per_patient(table_name, selected=None):
    """Return the most recent record per patient_id using row_number window."""
    df = spark.read.table(table_name)
    w = Window.partitionBy("patient_id").orderBy(F.col("event_ts").desc(), F.col("event_id").desc())
    out = df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    return out.select(*selected) if selected else out


# ---------------------------------------------------------------------------
# Patient 360 — latest triage, vitals, complaints, and location per patient
# ---------------------------------------------------------------------------

@dp.materialized_view(name="gold_patient_360", comment="Latest operational Patient 360 assembled from triage, vitals, complaints and SCD2 location history.")
def gold_patient_360():
    triage = latest_per_patient(
        "silver_triage",
        [
            "patient_id", "encounter_id", F.col("event_ts").alias("triage_ts"), "age", "sex",
            "arrival_mode", "triage_acuity", "pain_score", "num_prior_visits", "shock_index",
        ],
    )
    vitals = latest_per_patient(
        "silver_vitals",
        [
            "patient_id", F.col("event_ts").alias("latest_vital_ts"),
            F.col("heart_rate").alias("latest_heart_rate"),
            F.col("spo2").alias("latest_spo2"),
            F.col("systolic_bp").alias("latest_systolic_bp"),
            F.col("diastolic_bp").alias("latest_diastolic_bp"),
            F.col("temperature_c").alias("latest_temperature_c"),
            F.col("operational_alert_flag").alias("latest_operational_alert_flag"),
        ],
    )
    complaints = latest_per_patient(
        "silver_chief_complaints",
        ["patient_id", "chief_complaint_clean"],
    )
    location_window = Window.partitionBy("patient_id").orderBy(F.col("__START_AT").desc())
    location = (
        spark.read.table("silver_patient_location_history")
        .where(F.col("__END_AT").isNull())
        .withColumn("_rn", F.row_number().over(location_window))
        .where("_rn = 1")
        .select(
            "patient_id",
            F.col("encounter_id").alias("location_encounter_id"),
            F.col("department").alias("current_department"),
            F.col("bed_id").alias("current_bed_id"),
            F.col("event_type").alias("current_adt_event"),
            F.col("event_ts").alias("location_event_ts"),
        )
    )
    return (
        triage
        .join(vitals, "patient_id", "left")
        .join(complaints, "patient_id", "left")
        .join(location, "patient_id", "left")
        .withColumn("is_currently_admitted", F.coalesce(F.col("current_adt_event") != F.lit("A03_DISCHARGE"), F.lit(False)))
    )


# ---------------------------------------------------------------------------
# Hospital Command Center — single-row KPI snapshot for dashboards
# ---------------------------------------------------------------------------

@dp.materialized_view(name="gold_hospital_command_center", comment="Single-row hospital operations KPI snapshot for AI/BI dashboards.")
def gold_hospital_command_center():
    p = spark.read.table("gold_patient_360")
    return p.agg(
        F.sum(F.col("is_currently_admitted").cast("int")).alias("active_patients"),
        F.sum(((F.col("current_department") == "Emergency") & F.col("is_currently_admitted")).cast("int")).alias("ed_patients"),
        F.sum(((F.col("current_department") == "ICU") & F.col("is_currently_admitted")).cast("int")).alias("icu_patients"),
        F.sum((F.col("latest_operational_alert_flag") & F.col("is_currently_admitted")).cast("int")).alias("operational_alert_patients"),
        F.avg(F.when(F.col("is_currently_admitted"), F.col("triage_acuity"))).alias("avg_triage_acuity_active"),
    ).withColumn("snapshot_ts", F.current_timestamp())


# ---------------------------------------------------------------------------
# Department Hourly — ADT event counts by department for patient-flow analytics
# ---------------------------------------------------------------------------

@dp.materialized_view(name="gold_department_hourly", comment="Hourly ADT event counts by department for patient-flow analytics.")
def gold_department_hourly():
    a = spark.read.table("silver_adt")
    return (
        a.withColumn("event_hour", F.date_trunc("hour", "event_ts"))
        .groupBy("event_hour", "department", "event_type")
        .agg(F.count("*").alias("event_count"), F.countDistinct("patient_id").alias("patient_count"))
    )


# ---------------------------------------------------------------------------
# Live Alerts — patients with active operational vital threshold alerts
# Note: These are synthetic operational thresholds, NOT clinical alerts.
# ---------------------------------------------------------------------------

@dp.materialized_view(name="gold_live_alerts", comment="Latest synthetic operational vital threshold alerts; not clinical alerts.")
def gold_live_alerts():
    latest = latest_per_patient("silver_vitals")
    return (
        latest.where(F.col("operational_alert_flag") == True)
        .select(
            "patient_id", "encounter_id", "event_ts", "heart_rate", "spo2", "systolic_bp",
            "diastolic_bp", "temperature_c", "respiratory_rate", "fall_detected",
            "operational_alert_flag", "source",
        )
    )


# ---------------------------------------------------------------------------
# Triage Training — curated feature table for ML model training
# ---------------------------------------------------------------------------

@dp.materialized_view(name="gold_triage_training", comment="Curated feature table for the portfolio ED triage ML experiment.")
def gold_triage_training():
    t = spark.read.table("silver_triage")
    return t.select(
        "patient_id", "encounter_id", "arrival_mode", "arrival_hour", "arrival_day", "age", "sex",
        "systolic_bp", "diastolic_bp", "heart_rate", "respiratory_rate", "temperature_c", "spo2",
        "gcs_total", "pain_score", "num_prior_visits", "shock_index", "triage_acuity", "source",
    ).where(F.col("triage_acuity").isNotNull())
