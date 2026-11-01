-- Databricks notebook source
-- CareFlow360 v2.2.0 — Free Edition monitoring query pack.
-- Replace <CATALOG> with your Unity Catalog catalog. Run in SQL Editor.
-- The default schema below is for the deployed DAB presentation views.
-- For the single-schema quickstart, change schema to careflow360_dev
-- and prefix all Gold table names with gold_.
-- No individual patient identifiers or vital-sign records are returned.

USE CATALOG `<CATALOG>`;
USE SCHEMA careflow360_gold;

-- COMMAND ----------
-- Data quality: all output is aggregate.
SELECT
  COUNT(*) AS patient_rows,
  COUNT(DISTINCT patient_id) AS distinct_patients,
  SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) AS null_patient_ids,
  SUM(CASE WHEN triage_acuity IS NULL OR triage_acuity NOT BETWEEN 1 AND 5 THEN 1 ELSE 0 END) AS invalid_triage_rows,
  SUM(CASE WHEN is_currently_admitted AND current_department IS NULL THEN 1 ELSE 0 END) AS admitted_missing_department
FROM patient_360;

-- COMMAND ----------
-- Last observed event timestamps; not pipeline execution timestamps.
SELECT
  'KPI snapshot' AS source_name,
  MAX(snapshot_ts) AS observed_timestamp
FROM hospital_command_center
UNION ALL
SELECT 'Hourly ADT events', MAX(event_hour) FROM department_hourly
UNION ALL
SELECT 'Synthetic operational alerts', MAX(event_ts) FROM live_alerts;

-- COMMAND ----------
-- Aggregate operational workload: sums event_count, not table row counts.
SELECT event_type, SUM(event_count) AS event_total
FROM department_hourly
GROUP BY event_type
ORDER BY event_type;

-- COMMAND ----------
-- Batch model output distribution, no identifier display.
SELECT
  predicted_triage_acuity,
  COUNT(*) AS prediction_rows
FROM triage_predictions
GROUP BY predicted_triage_acuity
ORDER BY predicted_triage_acuity;

-- COMMAND ----------
-- Alert provenance (dataset uses synthetic source labels).
SELECT source, COUNT(*) AS alert_records
FROM live_alerts
GROUP BY source
ORDER BY source;
