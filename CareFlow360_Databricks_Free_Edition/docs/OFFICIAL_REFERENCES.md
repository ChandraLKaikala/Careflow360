# Official references used for CareFlow360

Checked 2026-10-02. Product documentation evolves; verify the current pages before production use.

## Databricks Free Edition

- https://docs.databricks.com/aws/en/getting-started/free-edition
- https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

## Lakeflow Spark Declarative Pipelines

- https://docs.databricks.com/aws/en/ldp/
- https://docs.databricks.com/aws/en/ldp/developer/python-dev
- https://docs.databricks.com/aws/en/ldp/cdc
- https://docs.databricks.com/aws/en/ldp/expectations

Current Python examples use `from pyspark import pipelines as dp`.

## Declarative Automation Bundles

- https://docs.databricks.com/aws/en/dev-tools/bundles/
- https://docs.databricks.com/aws/en/dev-tools/bundles/resources

Databricks Asset Bundles were renamed Declarative Automation Bundles. Existing configuration still uses `databricks.yml` and the `databricks bundle ...` CLI workflow.

## ML / serving

- https://docs.databricks.com/aws/en/mlflow/
- https://docs.databricks.com/aws/en/machine-learning/manage-model-lifecycle/
- https://docs.databricks.com/aws/en/machine-learning/model-serving/

## AI / BI / Genie / AI Search

- https://docs.databricks.com/aws/en/dashboards/
- https://docs.databricks.com/aws/en/genie-agents/
- https://docs.databricks.com/aws/en/generative-ai/vector-search

The product formerly called Vector Search is now AI Search, although bundle/API resource keys can still use `vector_search_*` names.

## Databricks Apps and Lakebase

- https://docs.databricks.com/aws/en/dev-tools/databricks-apps/
- https://docs.databricks.com/aws/en/oltp/

## Kaggle / source data

CareFlow360 does not redistribute Kaggle competition files. Download the datasets yourself and upload them to the project landing volume. See `docs/KAGGLE_DATA.md`.
