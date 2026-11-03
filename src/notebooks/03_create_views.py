# Databricks notebook source
"""
CareFlow360 — Create Presentation Views
=======================================

Creates views in the bronze, silver, and gold presentation schemas that
point to the SDP pipeline tables in the pipeline working schema.

This notebook runs as a job task after the pipeline refresh completes.

Parameters (via widgets):
  catalog:          Unity Catalog catalog name (e.g., workspace)
  pipeline_schema:  SDP pipeline working schema (e.g., careflow360)
  bronze_schema:    Bronze presentation schema (e.g., careflow360_bronze)
  silver_schema:    Silver presentation schema (e.g., careflow360_silver)
  gold_schema:      Gold presentation schema (e.g., careflow360_gold)
"""

import re

# Read widget parameters
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("pipeline_schema", "careflow360")
dbutils.widgets.text("bronze_schema", "careflow360_bronze")
dbutils.widgets.text("silver_schema", "careflow360_silver")
dbutils.widgets.text("gold_schema", "careflow360_gold")

CATALOG = dbutils.widgets.get("catalog")
PIPELINE_SCHEMA = dbutils.widgets.get("pipeline_schema")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")

# Validate identifiers before interpolating them into DDL.
for value in [CATALOG, PIPELINE_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe Unity Catalog identifier: {value!r}")

# Ensure presentation schemas exist
for schema, comment in [
    (BRONZE_SCHEMA, "Bronze layer: raw ingested hospital events. Access restricted to data engineering."),
    (SILVER_SCHEMA, "Silver layer: cleansed, deduplicated, enriched hospital events. Access for analysts and ML engineers."),
    (GOLD_SCHEMA, "Gold layer: analytics-ready aggregates, ML features, and policy docs. Access for all consumers."),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")
    print(f"✓ Schema: {CATALOG}.{schema}")

# Bronze views
bronze_tables = ["bronze_triage", "bronze_vitals", "bronze_adt", "bronze_chief_complaints"]
for t in bronze_tables:
    view_name = t.replace("bronze_", "")
    spark.sql(
        f"CREATE OR REPLACE VIEW {CATALOG}.{BRONZE_SCHEMA}.{view_name} "
        f"COMMENT 'Bronze view -> {CATALOG}.{PIPELINE_SCHEMA}.{t}' "
        f"AS SELECT * FROM {CATALOG}.{PIPELINE_SCHEMA}.{t}"
    )
    print(f"  ✓ {BRONZE_SCHEMA}.{view_name}")

# Silver views
silver_tables = ["silver_triage", "silver_triage_quarantine", "silver_vitals", "silver_adt", "silver_chief_complaints", "silver_patient_location_history"]
for t in silver_tables:
    view_name = t.replace("silver_", "")
    spark.sql(
        f"CREATE OR REPLACE VIEW {CATALOG}.{SILVER_SCHEMA}.{view_name} "
        f"COMMENT 'Silver view -> {CATALOG}.{PIPELINE_SCHEMA}.{t}' "
        f"AS SELECT * FROM {CATALOG}.{PIPELINE_SCHEMA}.{t}"
    )
    print(f"  ✓ {SILVER_SCHEMA}.{view_name}")

# Gold views (pipeline tables only; predictions and policy_chunks are physical tables)
gold_tables = ["gold_patient_360", "gold_hospital_command_center", "gold_department_hourly", "gold_live_alerts", "gold_triage_training"]
for t in gold_tables:
    view_name = t.replace("gold_", "")
    spark.sql(
        f"CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.{view_name} "
        f"COMMENT 'Gold view -> {CATALOG}.{PIPELINE_SCHEMA}.{t}' "
        f"AS SELECT * FROM {CATALOG}.{PIPELINE_SCHEMA}.{t}"
    )
    print(f"  ✓ {GOLD_SCHEMA}.{view_name}")

# ---------------------------------------------------------------------------
# Gold analytics views on top of notebook-produced physical tables.
# The CareFlow360 Hospital Operations Dashboard depends on these.
# ---------------------------------------------------------------------------

# 1) Model quality + current-encounter prediction views.
#    Pure SQL over physical tables: no AI-function quota cost per run.
#    Prediction-dependent views are CONDITIONAL: on a fresh install this
#    task runs before the first ML training write, so triage_predictions
#    does not exist yet and creating these views would fail the whole task.
#    The next daily run after the ML task creates them.
predictions_table = f"{CATALOG}.{GOLD_SCHEMA}.triage_predictions"
if spark.catalog.tableExists(predictions_table):
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.triage_model_quality
        COMMENT 'Per-patient predicted vs actual triage acuity agreement for the live model. Includes in-sample patients; run notebook 09 for holdout-only evaluation.'
        AS SELECT p.patient_id,
                  p.predicted_triage_acuity,
                  t.triage_acuity AS actual_triage_acuity,
                  CAST(p.predicted_triage_acuity = t.triage_acuity AS INT) AS correct
        FROM {CATALOG}.{GOLD_SCHEMA}.triage_predictions p
        JOIN {CATALOG}.{GOLD_SCHEMA}.triage_training t
          ON p.patient_id = t.patient_id AND p.encounter_id = t.encounter_id
        """
    )
    print(f"  ✓ {GOLD_SCHEMA}.triage_model_quality")

    # 1b) Current-encounter prediction view. triage_predictions is
    #     append-only per (patient_id, encounter_id), so a returning
    #     patient keeps predictions for earlier encounters. This view
    #     keeps only predictions matching the patient's current encounter
    #     in Patient 360; triage_model_quality above remains the historical
    #     (all-encounter, in-sample) agreement view.
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.triage_predictions_current
        COMMENT 'Predictions matching each patient current encounter in Patient 360; obsolete outcomes from earlier encounters are excluded. Historical agreement: see triage_model_quality.'
        AS SELECT
                  p.patient_id,
                  p.encounter_id,
                  p.predicted_triage_acuity,
                  t.triage_acuity AS actual_triage_acuity,
                  CAST(p.predicted_triage_acuity = t.triage_acuity AS INT) AS correct
        FROM {CATALOG}.{GOLD_SCHEMA}.triage_predictions p
        JOIN {CATALOG}.{GOLD_SCHEMA}.patient_360 t
          ON p.patient_id = t.patient_id AND p.encounter_id = t.encounter_id
        """
    )
    print(f"  ✓ {GOLD_SCHEMA}.triage_predictions_current")
else:
    print(
        f"  - {GOLD_SCHEMA}.triage_model_quality and triage_predictions_current "
        f"deferred: {predictions_table} does not exist yet (fresh install). "
        f"The next daily run after the ML task creates them."
    )

# 2) AI complaint categories: physical table, created ONCE with ai_classify.
#    Re-running classification daily would burn Free Edition AI-function
#    quota, so this only builds when the table is missing. Delete the table
#    to force a one-off re-classification of new complaints.
categories_table = f"{CATALOG}.{GOLD_SCHEMA}.chief_complaint_categories"
if not spark.catalog.tableExists(categories_table):
    spark.sql(
        f"""
        CREATE TABLE {categories_table}
        COMMENT 'AI-classified clinical category for each distinct ED chief complaint (ai_classify). Synthetic demo data.'
        AS SELECT chief_complaint_clean,
                  ai_classify(
                      chief_complaint_clean,
                      ARRAY('trauma', 'cardiac', 'respiratory',
                            'neurological', 'gastrointestinal', 'general')
                  ) AS clinical_category
        FROM (SELECT DISTINCT chief_complaint_clean
              FROM {CATALOG}.{SILVER_SCHEMA}.chief_complaints)
        """
    )
    print(f"  ✓ {GOLD_SCHEMA}.chief_complaint_categories (created)")
else:
    print(f"  ✓ {GOLD_SCHEMA}.chief_complaint_categories (already present)")

print("\nAll presentation views created successfully!")
