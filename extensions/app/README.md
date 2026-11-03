# Databricks App Extension — CareFlow360 Command Center

A Streamlit synthetic hospital-operations dashboard backed by Databricks SQL
Warehouse and App service-principal authentication. It displays aggregated
KPI, department census, alert and patient-flow data. **It is not clinical
decision support or a continuous live-monitoring system. Do not use PHI.**

## Optional deployment

The core Gold tables must exist, the App service principal needs read
permissions on the bound tables, and the SQL warehouse must be available.

```bash
cd extensions/app

databricks bundle validate \
  --var="catalog=<CATALOG>" \
  --var="warehouse_id=<WAREHOUSE_ID>"

databricks bundle deploy \
  --var="catalog=<CATALOG>" \
  --var="warehouse_id=<WAREHOUSE_ID>"

databricks bundle run careflow_command_center \
  --var="catalog=<CATALOG>" \
  --var="warehouse_id=<WAREHOUSE_ID>"
```

The app receives the warehouse ID and four three-part Unity Catalog table
names through resource-bound environment variables in app.yaml. These values
are validated before use in SQL. No access token is embedded in source code.

## Dashboard behavior

- SQL results are aggregate-only and explicitly limited.
- Alert and ADT flow queries read the last **7 days** of event timestamps,
  returning at most **100 hourly groups** per section. Charts represent the
  returned groups, not necessarily every event in that window.
- The app caches query results for **120 seconds**, not connections or
  credentials. **Refresh dashboard** clears this cache and re-queries.
  It does **not** trigger or deploy the Databricks ingestion pipeline.
- Sections fail independently with a safe message; detailed exceptions
  are written to protected app logs rather than rendered to users.
- Missing, future-dated or older-than-**30-hours** snapshots are flagged.

## Offline regression checks

```bash
python -m unittest discover -s tests -p 'test_*.py' -v
python tests/static_validate.py
```

Offline tests cover SQL query construction, resource validation and repository
contracts. They **do not** test Databricks deployment, live SQL permissions,
warehouse performance or any clinical use case.
