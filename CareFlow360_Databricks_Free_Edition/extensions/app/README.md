# Databricks App Extension

This is a Streamlit Databricks App using app-service-principal authentication and resource bindings. No personal access token is hard-coded.

Prerequisites: core Gold tables exist and you know the SQL warehouse ID.

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

The app receives table names and the warehouse ID through Databricks App resources / `valueFrom` environment variables.
