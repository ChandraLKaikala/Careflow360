# AI Search + RAG

`core/src/notebooks/05_create_policy_docs.py` creates `policy_chunks` in the **deployed split Gold schema** (`careflow360_gold`) and enables Delta Change Data Feed. The optional single-schema quickstart uses `gold_policy_chunks` instead. AI Search resources and both examples use `${var.table_prefix}policy_chunks`, with an empty prefix for split and `gold_` for unified.

The default extension at `extensions/ai_search` creates a **HYBRID Delta Sync AI Search index** on a STANDARD endpoint. It combines semantic retrieval with keyword matching and uses a Databricks-hosted embedding endpoint/model service. The bundle defaults to `databricks-qwen3-embedding-0-6b`; override that variable if your workspace exposes a different supported embedding model.

Deploy it only after the core job has created `policy_chunks` in the split Gold schema (or set `table_prefix=gold_` for the unified quickstart). Check that the embedding endpoint is available in your Free Edition workspace before attempting an optional index:

```bash
cd extensions/ai_search
databricks bundle validate \
  --var="catalog=<CATALOG>" \
  --var="embedding_model_endpoint=databricks-qwen3-embedding-0-6b"

databricks bundle deploy \
  --var="catalog=<CATALOG>" \
  --var="embedding_model_endpoint=databricks-qwen3-embedding-0-6b"
```

Suggested RAG questions:

- What does the demo policy say to review during ICU capacity escalation?
- What should happen to records with invalid timestamps?
- What operational dependencies are tracked before discharge?

## Dedicated full-text alternative

`extensions/ai_search/examples/ai_search_full_text_preview.yml` shows the dedicated `FULL_TEXT` subtype. Databricks currently documents that dedicated full-text indexes are Beta, require **STORAGE_OPTIMIZED** endpoints, and support triggered sync only. Do not copy that example over the default resource unless the Preview is enabled in your workspace.

The policy corpus is fabricated for this project and must not be treated as a hospital's real policy library.
