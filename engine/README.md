# tributary-engine

A config-driven, plugin-based PySpark engine. Pipeline specs come from YAML, JSON or the API. The engine compiles them, plans them and runs them in batch, streaming or preview mode.

## Commands

| Command | What it does |
|---|---|
| `tributary validate FILE [-e ENV] [--deep]` | Checks the spec schema, references, DAG cycles and the table names used in SQL. `--deep` also checks columns and types with Spark, using empty frames (no data is read). |
| `tributary plan FILE` | Shows the physical plan (step order, cache points, quality gates) and the chosen Spark resources. |
| `tributary preview FILE --node ID [--limit N] [--sample SRC=path] [--write] [--json]` | Shows the rows a node would produce from bounded reads. Sinks are a dry run unless `--write` is given. |
| `tributary run FILE [-e ENV]` | Runs the pipeline. Streaming pipelines keep running until stopped. |
| `tributary schema-export [-o FILE]` | Writes the JSON Schema of the spec and the component catalog (this is what the UI uses). |
| `tributary list-components` | Lists every registered plugin. |
| `tributary convert A.yaml B.json` | Converts a pipeline between YAML and JSON. |

The same operations are available from Python: `tributary.compile_pipeline / plan / preview / analyze / run / export_schema`.

## How it works

```
document ─▶ compile ─────────────────────────────▶ plan ──────────────────▶ execute
            env vars + secrets, connections,        quality gates spliced,    batch: once
            spec model built from the registry,     fan-out counted,          streaming: per micro-batch
            references / cycles / SQL tables        persist decided,          preview: needed steps only,
                                                    sinks ordered last        bounded reads
```

- **Plugins everywhere.** Sources, transforms, sinks, checks, metrics sinks, schema resolvers and secret providers are all plugins, discovered through entry points (`tributary.<category>s`). The spec model, its JSON Schema and the UI catalog are generated from the registry. Adding a connector never touches engine code.
- **SQL inputs.** Each entry in `inputs` becomes a temp view named by its `as` alias, defaulting to the node id. At compile time `sqlglot` checks that the query is a single SELECT that reads only those aliases or its own CTEs, and suggests the closest name when a table is unknown. Catalog tables must be listed in `allow_catalog_tables`.
- **Quality gates.**
  - **Attaching:** `attach_to` checks a node's output for all of its consumers; `before` checks only what flows into one sink. Sinks keep pointing at the business node, and the planner rewires them through the gate.
  - **One pass:** all checks run in a single aggregate. Row checks become flag columns; dataset checks become aggregate expressions.
  - **Actions:**
    - `fail` raises an error before any sink writes.
    - `drop` filters out failing rows.
    - `quarantine` also writes the failing rows, with a `_dq_failed_checks` column, to `quarantine_sink`.
  - **Results:** one row per check is sent to each entry in `results_sinks` (console, file, JDBC/Postgres, Prometheus Pushgateway).
- **Automatic persist.** The planner counts how often each step's output is read. A pass-through gate (observe/fail) counts as one read and also passes its consumers' reads up to its input. A step read twice or more is persisted; a single-reader step is not. You can override this per node with `cache: auto|always|never` and `storage_level`. For drop/quarantine gates, the flagged frame they split is persisted. Cached frames are released when the run or micro-batch ends.
- **Streaming.** A streaming pipeline is one streaming query with `foreachBatch`. Each micro-batch is persisted once, and the whole plan (static lookup sources, transforms, gates and every sink) runs on it. The source is read once per batch no matter how many sinks there are. Sinks get `ctx.batch_id` for idempotent writes.
- **Resources.**
  - AQE and dynamic allocation are always on.
  - `resources: auto` picks an S/M/L/XL profile from the estimated input size (local file listing) and the number of joins and aggregations.
  - Explicit `resources` fields override the profile, and raw `spark_conf` overrides everything.
  - Only the Spark packages of the plugins a pipeline uses are added (for example spark-sql-kafka, spark-avro, the Postgres JDBC driver).

## Development

```bash
python3.11 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/pytest
```

## Known limits (v0.1)

- Streaming uses foreachBatch with exactly one stream source. Stateful streaming operations across micro-batches (windowed aggregations with watermarks, stream-stream joins) are not supported yet. Static sources are re-read on every micro-batch.
- Size estimation only covers local paths. Remote and streaming sources default to profile M.
- The Kafka source (JSON values) and the Postgres and Prometheus metrics sinks were checked by hand against the `deploy/docker-compose.yml` stack. Automated tests for them don't exist yet, and the Avro/Schema Registry path hasn't been run.
- In the Schema Registry resolver, only Avro is supported (JSON Schema and Protobuf are not yet).
