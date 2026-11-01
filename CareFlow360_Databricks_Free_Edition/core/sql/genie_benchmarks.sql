-- Databricks notebook source
-- Queries you can use to benchmark Genie responses after configuring the agent.
-- Replace <CATALOG>.

USE CATALOG `<CATALOG>`;
USE SCHEMA careflow360_dev;

-- Q: How many patients are currently in the emergency department?
SELECT COUNT(*) AS ed_patients
FROM gold_patient_360
WHERE is_currently_admitted AND current_department = 'Emergency';

-- Q: Which department currently has the most admitted patients?
SELECT current_department, COUNT(*) AS patients
FROM gold_patient_360
WHERE is_currently_admitted
GROUP BY current_department
ORDER BY patients DESC;

-- Q: Show the latest operational alerts.
SELECT patient_id, event_ts, heart_rate, spo2, systolic_bp, temperature_c
FROM gold_live_alerts
ORDER BY event_ts DESC
LIMIT 50;

-- Q: How many admission, transfer and discharge events happened by hour?
SELECT event_hour, event_type, SUM(event_count) AS events
FROM gold_department_hourly
GROUP BY event_hour, event_type
ORDER BY event_hour, event_type;
