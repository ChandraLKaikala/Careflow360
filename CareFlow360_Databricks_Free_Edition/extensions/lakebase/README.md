# Lakebase Autoscaling Extension

This extension demonstrates OLTP state separate from the Delta Lake analytical layer.

```bash
cd extensions/lakebase

databricks bundle validate
databricks bundle deploy
```

It creates one project, one main branch, one small autoscaling read-write endpoint, an owner role and the `careflow_app` database.

After creation, connect through the Lakebase UI/SQL editor and execute `sql/schema.sql`.

Free Edition has tight Lakebase limits. If you already created a Lakebase project, reuse it instead of deploying another.
