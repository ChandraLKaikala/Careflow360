-- Databricks notebook source
-- Replace <CATALOG> before running.
USE CATALOG `<CATALOG>`;
USE SCHEMA careflow360_dev;

SHOW TABLES;

SELECT 'bronze_triage' AS asset, COUNT(*) AS rows FROM bronze_triage
UNION ALL SELECT 'bronze_vitals', COUNT(*) FROM bronze_vitals
UNION ALL SELECT 'bronze_adt', COUNT(*) FROM bronze_adt
UNION ALL SELECT 'silver_triage', COUNT(*) FROM silver_triage
UNION ALL SELECT 'silver_vitals', COUNT(*) FROM silver_vitals
UNION ALL SELECT 'gold_patient_360', COUNT(*) FROM gold_patient_360
UNION ALL SELECT 'gold_live_alerts', COUNT(*) FROM gold_live_alerts;

SELECT * FROM gold_hospital_command_center;
