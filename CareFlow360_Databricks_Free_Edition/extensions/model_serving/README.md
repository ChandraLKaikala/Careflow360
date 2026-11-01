# Model Serving Extension

Run the core ML training notebook first. Find the registered model version, then:

```bash
cd extensions/model_serving

databricks bundle validate \
  --var="catalog=<CATALOG>" \
  --var="model_version=<VERSION>"

databricks bundle deploy \
  --var="catalog=<CATALOG>" \
  --var="model_version=<VERSION>"
```

The endpoint is CPU/Small and configured to scale to zero. It is a portfolio technical demo, not a clinical service.
