# Lakebase

The Lakehouse remains the analytical system of record. Lakebase demonstrates OLTP state that should not be modeled as a high-frequency Delta overwrite workflow, such as:

- operations alerts acknowledged by users
- discharge-task ownership
- command-center comments
- operational case status

Deploy `extensions/lakebase/`, then use the included `sql/schema.sql` in the Lakebase SQL editor/connection.

Because Free Edition permits only a small Lakebase footprint, the extension intentionally creates one project, one branch, one read-write endpoint, one owner role and one database.
