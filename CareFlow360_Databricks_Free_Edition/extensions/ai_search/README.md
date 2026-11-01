# AI Search Extension

The split-schema DAB core job creates `policy_chunks` in `careflow360_gold` and enables Delta Change Data Feed. Subsequent daily runs use an idempotent Delta MERGE rather than recreating the table, preserving Delta Sync table identity and CDF continuity. The unified quickstart uses `gold_policy_chunks` in its single schema. The AI Search source table honors `table_prefix` (empty by default; use `gold_` for unified quickstart).

## Default deployment: semantic + keyword hybrid retrieval

The default bundle creates one **STANDARD** AI Search endpoint and a triggered **HYBRID Delta Sync index** over the synthetic hospital-policy corpus. It uses the Databricks-hosted embedding endpoint/model service configured by `embedding_model_endpoint`. The default is:

```text
databricks-qwen3-embedding-0-6b
```

Because Foundation Model availability can vary by region/workspace, confirm the model is visible in your workspace before deployment and override the variable if needed.

```bash
cd extensions/ai_search

databricks bundle validate \
  --var="catalog=<CATALOG>" \
  --var="embedding_model_endpoint=databricks-qwen3-embedding-0-6b"

databricks bundle deploy \
  --var="catalog=<CATALOG>" \
  --var="embedding_model_endpoint=databricks-qwen3-embedding-0-6b"
```

This extension is optional. The core lakehouse does not depend on AI Search. Free Edition allows only a limited AI Search footprint, so delete or reuse any existing AI Search endpoint before deploying another one.

## Optional dedicated full-text search

Databricks also supports a dedicated `FULL_TEXT` index, but that feature is Beta and requires a **STORAGE_OPTIMIZED** endpoint plus triggered sync. A reference resource is included at `examples/ai_search_full_text_preview.yml`. Use it only if that Preview is enabled in your workspace.
