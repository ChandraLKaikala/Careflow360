# CareFlow360 Command Reference

Replace `<CATALOG>`, `<WORKSPACE_HOST>` and other angle-bracket placeholders before running commands.

## SQL bootstrap

```sql
SELECT current_catalog();

USE CATALOG `<CATALOG>`;

CREATE SCHEMA IF NOT EXISTS careflow360_dev
COMMENT 'CareFlow360 synthetic healthcare lakehouse';

CREATE VOLUME IF NOT EXISTS careflow360_dev.landing
COMMENT 'Landing area for synthetic/public hospital events';

SHOW SCHEMAS IN `<CATALOG>`;
SHOW VOLUMES IN `<CATALOG>`.careflow360_dev;
```

## Databricks CLI authentication

Install a current Databricks CLI, then authenticate:

```bash
databricks auth login --host <WORKSPACE_HOST> --profile careflow-free
```

Check authentication:

```bash
databricks current-user me --profile careflow-free
```

You can either make that profile the active/default profile or append `--profile careflow-free` to CLI commands.

## Core DAB

From the `core/` directory:

```bash
databricks bundle validate --var="catalog=<CATALOG>"

databricks bundle deploy -t dev --var="catalog=<CATALOG>"

databricks bundle summary -t dev --var="catalog=<CATALOG>"

databricks bundle run -t dev careflow360_job --var="catalog=<CATALOG>"
```

Run only the Lakeflow pipeline:

```bash
databricks bundle run -t dev hospital_pipeline --var="catalog=<CATALOG>"
```

Destroy resources created by the bundle when finished:

```bash
databricks bundle destroy -t dev --var="catalog=<CATALOG>"
```

Use `destroy` carefully: inspect the bundle plan first and do not delete data you want to keep.

## Upload Kaggle files to the managed volume

After manually downloading the files under Kaggle's license, upload through Catalog Explorer or use CLI file copy commands if available in your environment.

Expected paths:

```text
/Volumes/<CATALOG>/careflow360_dev/landing/kaggle/triagegeist/train.csv
/Volumes/<CATALOG>/careflow360_dev/landing/kaggle/triagegeist/chief_complaints.csv
/Volumes/<CATALOG>/careflow360_dev/landing/kaggle/monitoring/healthcare_monitoring.csv
```

Then run `core/src/notebooks/02_kaggle_normalize.py`.

## Core SQL inspection

```sql
USE CATALOG `<CATALOG>`;
USE SCHEMA careflow360_dev;

SHOW TABLES;

SELECT COUNT(*) FROM bronze_triage;
SELECT COUNT(*) FROM silver_triage;
SELECT COUNT(*) FROM gold_patient_360;
SELECT * FROM gold_hospital_command_center;
SELECT * FROM gold_live_alerts ORDER BY event_ts DESC LIMIT 100;
```

## Reprocess from scratch

For portfolio/demo reset only:

```sql
USE CATALOG `<CATALOG>`;
DROP SCHEMA IF EXISTS careflow360_dev CASCADE;
CREATE SCHEMA careflow360_dev;
CREATE VOLUME careflow360_dev.landing;
```

Then regenerate events and rerun the pipeline.

## Optional extensions

Every extension is a separate bundle to keep the core reproducible when a Free Edition feature or quota is temporarily unavailable.

Examples:

```bash
cd extensions/genie
databricks bundle validate --var="catalog=<CATALOG>" --var="warehouse_id=<WAREHOUSE_ID>"

databricks bundle deploy --var="catalog=<CATALOG>" --var="warehouse_id=<WAREHOUSE_ID>"
```

See each extension README for required variables.
