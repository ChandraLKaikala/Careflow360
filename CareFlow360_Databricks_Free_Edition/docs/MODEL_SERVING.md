# Model Serving

The training notebook registers:

```text
<CATALOG>.careflow360_gold.triage_acuity_model
```

and assigns the `Champion` alias to the newly registered version.

The split-schema DAB core job registers the model in `careflow360_gold`; the unified quickstart may use a different schema and requires an explicit `schema` override when deploying the serving bundle. Actual registered model versions and serving quotas must be verified in the workspace.

The optional bundle under `extensions/model_serving/` requires the exact numeric model version because the Model Serving resource API uses `entity_version`.

Discover versions with MLflow UI or:

```python
from mlflow.tracking import MlflowClient
c = MlflowClient()
for v in c.search_model_versions("name='<CATALOG>.careflow360_gold.triage_acuity_model'"):
    print(v.version, v.status)
```

Then deploy:

```bash
cd extensions/model_serving
databricks bundle validate --var="catalog=<CATALOG>" --var="model_version=<VERSION>"
databricks bundle deploy --var="catalog=<CATALOG>" --var="model_version=<VERSION>"
```

The endpoint scales to zero. Free Edition quotas can prevent creation if another custom serving endpoint is already consuming the allowance.
