# Tributary

**Draw it. Preview it. Ship it.** An open-source platform for visual, config-driven data pipelines on Apache Spark.

You draw a pipeline as sources → transforms → sinks, with data-quality gates attached wherever you need them, and test every step on sample data as you go. The canvas produces a declarative spec; the engine validates it, plans it and runs it on Spark in batch or streaming. Phase 2 adds an AI assistant that draws pipelines from chat.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/editor-dark.png">
  <img alt="The Tributary editor: a pipeline joining orders with customers, a quality block attached to the join, the SQL node's inputs and editor, and a preview of its output on sample data" src="docs/images/editor.png">
</picture>

## Features

- **Draw, don't code.** Drag sources, transforms, quality checks and sinks onto a canvas and connect them. Every node's settings form is generated from its plugin, so new connectors show up in the UI without frontend work.
- **Test every step.** Press *Test* on any node to see its output on real or uploaded sample data. Validation runs as you type, down to column names in your SQL.
- **Data quality anywhere.** Attach checks to any node or in front of any sink. Then observe, fail the run, drop bad rows, or quarantine them with the checks they failed.

  ![Testing a quality block: failing rows first, each tagged with the check it fails](docs/images/quality-test.png)

- **Versions and runs.** Drafts autosave; runs use immutable, validated versions. Each run gets its own Spark driver, with live logs and its data-quality history.

  ![A finished run with its data-quality results](docs/images/runs.png)

- **Lakehouse and operational connectors.** Read from and write to files, Kafka (source), Apache Iceberg, Delta Lake, Apache Hudi, Elasticsearch, MongoDB and REST APIs (source and per-row enrichment), and profile or verify data with an [Amazon Deequ](https://github.com/awslabs/deequ) node. Sources can stand in with pasted dummy data, so a pipeline can be drawn and tested before the real system is reachable. See [`engine/README.md`](engine/README.md) for each connector and [`examples/`](examples/) for a pipeline per connector.
- **One engine, many places.** The same spec runs from the CLI, the API or the UI: batch or streaming, locally or on a cluster, with Spark resources sized automatically.

| Component | Status |
|---|---|
| [`engine/`](engine/): PySpark engine + CLI | **Phase 1a: usable** |
| [`backend/`](backend/): FastAPI, pipelines, connections, previews, runs | **Phase 1b: usable** |
| [`ui/`](ui/): React canvas, schema-driven forms, live validation, per-node preview, runs | **Phase 1c: usable** |

## Quickstart (UI)

```bash
docker compose -f deploy/docker-compose.yml up -d postgres
(cd engine && python3.11 -m venv .venv && .venv/bin/pip install -e .)
(cd backend && python3.11 -m venv .venv && .venv/bin/pip install -e ../engine -e . && .venv/bin/tributary-backend serve) &
(cd ui && npm install && npm run dev)     # open http://localhost:5173 and import examples/csv_to_parquet.yaml
```

## Quickstart (CLI only)

```bash
cd engine && python3.11 -m venv .venv && .venv/bin/pip install -e ".[dev]" && cd ..
engine/.venv/bin/tributary validate examples/csv_to_parquet.yaml
engine/.venv/bin/tributary plan examples/csv_to_parquet.yaml
engine/.venv/bin/tributary preview examples/csv_to_parquet.yaml --node orders_clean --limit 5
engine/.venv/bin/tributary run examples/csv_to_parquet.yaml
```

Requires Java 8/11/17 and Python 3.10–3.12, which is what PySpark 3.5 supports. The Iceberg runtime defaults to 1.6.1 for the same reason (newer releases need Java 11+; pick one with `TRIBUTARY_ICEBERG_VERSION`).

**Working locally:** every pipeline can have a `local` environment, where each source can stand in with uploaded dummy data (JSON Lines, up to 1,000 records) and each sink can be a dummy that writes nothing, so a pipeline previews end to end with nothing running. For real systems, `python3 scripts/local_services.py up mongodb kafka` (or `up all`, or `up --pipeline FILE`) starts only the Docker services those connectors need and prints their connection settings. See [`docs/local-development.md`](docs/local-development.md). Connector guides: [MongoDB](docs/mongodb.md), [Elasticsearch](docs/ES_AS_A_SOURCE.md), [Delta](docs/DELTA_AS_A_SOURCE.md), [Hudi](docs/HUDI_AS_A_SOURCE.md), [Iceberg](docs/iceberg-source.md), [REST](docs/REST_AS_A_SOURCE.md).

## A pipeline

```yaml
pipeline: { id: orders_daily, mode: batch }
environments:
  dev: { vars: { out: /tmp/out } }
sources:
  - id: orders_src
    type: file
    options: { path: data/orders.csv, format: csv, read_options: { header: true } }
    schema: { kind: ddl, ddl: "order_id INT, amount DOUBLE, country STRING" }
transforms:
  - id: orders_clean
    type: sql
    inputs: [{ node: orders_src, as: orders }]          # "orders" is the table name in SQL
    options: { sql: "SELECT order_id, amount, upper(country) AS country FROM orders" }
quality:
  - id: orders_dq
    attach_to: orders_clean                             # or `before: <sink>` to guard one sink
    action: quarantine                                  # observe | fail | drop | quarantine
    checks:
      - { type: not_null, column: order_id }
      - { type: range, column: amount, min: 0 }
    results_sinks: [{ type: console }]
    quarantine_sink: { type: file, options: { path: "${env.out}/bad" } }
sinks:
  - id: orders_out
    type: file
    input: orders_clean                                 # sinks read the business node, not the gate
    options: { path: "${env.out}/orders" }
```

See [`engine/README.md`](engine/README.md) for how it works, and [`docs/writing-a-plugin.md`](docs/writing-a-plugin.md) to add a connector.

The screenshots above are generated: with the backend and UI running, `node scripts/capture-screenshots.mjs` recreates them (it needs Google Chrome).
