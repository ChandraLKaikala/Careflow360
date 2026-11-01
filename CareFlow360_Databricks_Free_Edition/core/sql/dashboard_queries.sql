-- Databricks notebook source
-- CareFlow360 AI/BI dashboard query pack.
-- Replace <CATALOG> before using these datasets in an AI/BI dashboard.

USE CATALOG `<CATALOG>`;
USE SCHEMA careflow360_dev;

-- COMMAND ----------
-- KPI cards
SELECT
  active_patients,
  ed_patients,
  icu_patients,
  operational_alert_patients,
  ROUND(avg_triage_acuity_active, 2) AS avg_triage_acuity_active,
  snapshot_ts
FROM gold_hospital_command_center;

-- COMMAND ----------
-- Patient flow over time
SELECT
  event_hour,
  department,
  event_type,
  event_count,
  patient_count
FROM gold_department_hourly
ORDER BY event_hour, department, event_type;

-- COMMAND ----------
-- Current department census
SELECT
  COALESCE(current_department, 'Unknown') AS department,
  COUNT(*) AS patient_count,
  SUM(CASE WHEN latest_operational_alert_flag THEN 1 ELSE 0 END) AS operational_alert_count
FROM gold_patient_360
WHERE is_currently_admitted
GROUP BY COALESCE(current_department, 'Unknown')
ORDER BY patient_count DESC;

-- COMMAND ----------
-- Current synthetic operational alerts
SELECT
  patient_id,
  encounter_id,
  event_ts,
  heart_rate,
  spo2,
  systolic_bp,
  diastolic_bp,
  temperature_c,
  respiratory_rate,
  fall_detected
FROM gold_live_alerts
ORDER BY event_ts DESC
LIMIT 200;

-- COMMAND ----------
-- Triage mix
SELECT
  triage_acuity,
  COUNT(*) AS patients
FROM gold_patient_360
GROUP BY triage_acuity
ORDER BY triage_acuity;

-- COMMAND ----------
-- Model prediction distribution
SELECT
  predicted_triage_acuity,
  COUNT(*) AS predictions
FROM gold_triage_predictions
GROUP BY predicted_triage_acuity
ORDER BY predicted_triage_acuity;
