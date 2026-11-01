# Genie Agent Extension

Prerequisites:

1. Core CareFlow360 pipeline is deployed and populated.
2. Your SQL warehouse is running/available.
3. Use a Databricks CLI version that supports Genie bundle resources (1.3+ direct deployment engine).

Find your warehouse ID in SQL Warehouses, then:

```bash
cd extensions/genie

databricks bundle validate \
  --var="catalog=<CATALOG>" \
  --var="warehouse_id=<WAREHOUSE_ID>"

databricks bundle deploy \
  --var="catalog=<CATALOG>" \
  --var="warehouse_id=<WAREHOUSE_ID>"
```

After deployment, open the Genie Agent and test the sample questions. Verify answers independently with `core/sql/genie_benchmarks.sql`.
