# Tributary

**Draw it. Preview it. Ship it.** An open-source platform for visual, config-driven data pipelines on Apache Spark.

You draw a pipeline as sources → transforms → sinks, with data-quality gates attached wherever you need them, and test every step on sample data as you go. The canvas produces a declarative spec; the engine validates it, plans it and runs it on Spark in batch or streaming. Phase 2 adds an AI assistant that draws pipelines from chat.

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

Requires Java 8/11/17 and Python 3.10–3.12, which is what PySpark 3.5 supports.

For Kafka, Postgres, MinIO and the Prometheus Pushgateway:
`docker compose -f deploy/docker-compose.yml up -d`, then see [`examples/kafka_to_parquet.yaml`](examples/kafka_to_parquet.yaml).

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
