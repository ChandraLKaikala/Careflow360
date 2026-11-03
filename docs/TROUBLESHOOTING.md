# Troubleshooting

## `catalog does not exist` or permission error

Run:

```sql
SELECT current_catalog();
```

Use the exact returned value as the DAB `catalog` variable. Your workspace catalog is not guaranteed to be named `workspace`.

## Cannot create schema

Use a schema where you have `CREATE TABLE`, `CREATE VOLUME`, and model privileges, or ask the workspace owner/admin to create `careflow360_dev` for you. In a personal Free Edition workspace, the workspace owner normally has sufficient control over the workspace catalog.

## Pipeline sees no files

Confirm:

```text
/Volumes/<CATALOG>/careflow360_dev/landing/events/triage/
```

contains JSON files. Rerun `01_generate_sample_data.py`.

## `pyspark.pipelines` import fails in a normal notebook

That API is evaluated by Lakeflow pipeline runtime. The preflight notebook catches this condition. The pipeline source files should be attached to a Lakeflow pipeline, not executed as ordinary notebooks.

## DAB validation reports an unknown property

Use a current Databricks CLI and run:

```bash
databricks -v
databricks bundle schema
```

Databricks adds bundle resource types frequently; older CLIs can reject current configuration.

## Model training fails on package import

Serverless environments can change package versions. Inspect the current environment. The project intentionally avoids installing packages from the public internet during the core path.

## AI Search or Model Serving quota error

Free Edition has tight quotas. Delete unused endpoints or use the FULL_TEXT AI Search configuration rather than creating a separate custom embedding endpoint.

## Genie bundle fails

Genie bundle resources require a recent CLI and the direct deployment engine. The extension already declares `engine: direct`.
