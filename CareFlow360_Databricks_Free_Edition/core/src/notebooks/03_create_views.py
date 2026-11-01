"""
CareFlow360 — Create Presentation Views
=======================================

Creates views in the bronze, silver, and gold presentation schemas that
point to the SDP pipeline tables in the pipeline working schema.

This notebook runs as a job task after the pipeline refresh completes.

Parameters (via widgets):
  catalog:          Unity Catalog catalog name (e.g., workspace)
  pipeline_schema:  SDP pipeline working schema (e.g., careflow360_dab)
  bronze_schema:    Bronze presentation schema (e.g., careflow360_bronze)
  silver_schema:    Silver presentation schema (e.g., careflow360_silver)
  gold_schema:      Gold presentation schema (e.g., careflow360_gold)
"""

import json

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

print("\nAll presentation views created successfully!")
