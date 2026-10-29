# Testing Hudi as a source locally

Yes, you can test the Hudi source end to end on your machine with the stack you already use
(`docker compose -f deploy/docker-compose.yml up -d postgres`). Hudi tables are plain
directories with a `.hoodie` folder, so no extra service is needed: the tables live on local
disk, or on MinIO if you want to test against S3-style storage (optional, section 8).

This document is about testing with real tables and the real UI, API and CLI. The automated
tests (`engine/tests/test_hudi_source.py`, `backend/tests/test_hudi_source.py`) are described
at the end only for completeness.

Everything below was run on Linux with Java 21 and Python 3.11; sections 1–7 and 9–10 were
executed as written. Section 8 (MinIO) follows Spark and Hadoop conventions but could not be
run in the environment this was written in (no Docker there); it is marked as such.

---

## 1. Prerequisites

| Need | Notes |
|---|---|
| Java 8, 11 or 17 | What Spark 3.5 supports. Java 21 also worked for all of this, with harmless `Serviceability Agent` warnings from Hudi. `java -version` to check. |
| Python 3.10–3.12 | `python3.11` recommended. |
| Docker | For Postgres (the backend's metadata DB). Only needed for the UI/API part, not for the CLI. |
| Node 18+ | For the UI. |
| Maven Central reachable once | The first run that touches Hudi downloads the Spark bundle (~110 MB, `org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0`) into `~/.ivy2`. After that everything works offline. |

Two version pins worth knowing about (both are environment issues, not Hudi ones):

- Install `pyspark==3.5.7` in the backend virtualenv. PySpark 3.5.8/3.5.9 have no matching
  `spark-sql-kafka` artifact on Maven Central, and the backend's shared preview session loads
  every installed plugin's packages, Kafka included.
- If the backend fails at startup with `TypeError: cannot use a string pattern on a bytes-like
  object`, pin `psycopg[binary]<3.3` (incompatible with the pinned SQLAlchemy).

## 2. Start the stack and install

```bash
docker compose -f deploy/docker-compose.yml up -d postgres

# engine + CLI
(cd engine && python3.11 -m venv .venv && .venv/bin/pip install -e ".[dev]")

# backend (API + preview session + run workers)
(cd backend && python3.11 -m venv .venv \
  && .venv/bin/pip install -e ../engine -e ".[dev]" "pyspark==3.5.7" "psycopg[binary]<3.3")

# UI
(cd ui && npm install)
```

Check that the source is registered:

```bash
engine/.venv/bin/tributary list-components | grep -A3 '^source'
#   file   Files on local disk or any Hadoop-compatible object store. [batch, streaming]
#   hudi   Apache Hudi tables: snapshot, read-optimized, incremental or time-travel reads, and streams of commits. [batch, streaming]
#   kafka  ...
```

## 3. Get a Hudi table to read

Any of the three. The source expects the table at `<base_path>/<database>/<table>` (or
`<base_path>/<table>` when the table name has no database part), so `base_path` is simply the
directory *above* the table's directory.

### 3a. Write one with Tributary's own Hudi sink (quickest)

`examples/csv_to_hudi.yaml` turns `examples/data/orders.csv` into two tables under
`/tmp/tributary/hudi`:

```bash
engine/.venv/bin/tributary run examples/csv_to_hudi.yaml
ls /tmp/tributary/hudi/sales/orders          # country=GB  country=IN  country=US ...  .hoodie
```

Run it a second time to get a second commit (the upsert changes nothing, but it is a commit on
the timeline). To make real changes between runs, edit a few amounts in
`examples/data/orders.csv` and run again.

For a **merge-on-read** table, a **CDC-enabled** table (needed for `incremental_format: cdc`)
or **soft deletes**, copy the example and change the sink, e.g.:

```yaml
sinks:
  - id: orders_upsert
    type: hudi
    connection: lake
    options:
      table: sales.orders_mor
      table_type: merge_on_read
      record_key: [order_id]
      precombine_field: order_ts
      partition_by: [country]
      write_options:
        hoodie.table.cdc.enabled: true       # records every change for cdc reads
```

A row whose boolean `_hoodie_is_deleted` column is `true` is deleted on upsert (Hudi's soft
delete marker); `operation: delete` removes the keys in the data.

### 3b. Use a table you already have

Point `base_path` at the directory above it. A table written by Spark at
`/data/lake/warehouse/orders` is `base_path: /data/lake/warehouse`, `table: orders`. Tables
written by Hudi 0.x (table version ≤ 6) are read by the default bundle (0.15.0); tables written
by Hudi 1.x (table version 8, `hoodie.table.version=8` in `.hoodie/hoodie.properties`) need the
1.x bundle: `export TRIBUTARY_HUDI_VERSION=1.0.2` for every command and for the backend.

### 3c. Write one with plain PySpark + Hudi

```bash
engine/.venv/bin/pyspark --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
```
```python
df = spark.createDataFrame([(1, 10.0, "IN", 1), (2, 20.0, "US", 1)], "id INT, amount DOUBLE, country STRING, version INT")
(df.write.format("hudi")
   .option("hoodie.table.name", "events")
   .option("hoodie.datasource.write.recordkey.field", "id")
   .option("hoodie.datasource.write.precombine.field", "version")
   .option("hoodie.datasource.write.partitionpath.field", "country")
   .option("hoodie.datasource.write.hive_style_partitioning", "true")
   .mode("append").save("/tmp/tributary/hudi/raw/events"))
```
→ `base_path: /tmp/tributary/hudi`, `table: raw.events`.

## 4. Test from the CLI

`examples/hudi_to_parquet.yaml` reads `sales.orders` incrementally with progress tracking and
writes Parquet. With the table from 3a in place:

```bash
T=engine/.venv/bin/tributary
$T validate examples/hudi_to_parquet.yaml            # spec, references, SQL table names
$T validate examples/hudi_to_parquet.yaml --deep     # + the table's real columns, through Spark
$T plan examples/hudi_to_parquet.yaml                # steps, cache points, Spark profile
$T preview examples/hudi_to_parquet.yaml --node orders_changes --limit 5     # rows, nothing written
$T preview examples/hudi_to_parquet.yaml --node orders_enriched --limit 5    # after the SQL
$T -v run examples/hudi_to_parquet.yaml               # -v shows what the source decided
```

What to look for in the `-v run` output:

```
INFO tributary: reading hudi table sales.orders (copy_on_write) as incremental at /tmp/tributary/hudi/sales/orders
INFO tributary: hudi table sales.orders: reading commits after earliest up to 20261028091846597 (no progress recorded yet: starting at begin_instant)
INFO tributary: writing sink orders_out: parquet -> /tmp/tributary/out/orders_changes (overwrite)
INFO tributary: hudi table sales.orders: progress recorded up to commit 20261028091846597
✓ run ...: wrote orders_out; 2 checks, 0 failed
```

Then exercise progress tracking:

```bash
cat /tmp/tributary/checkpoints/orders_changes/offset.json   # the commit the run read up to
$T -v run examples/hudi_to_parquet.yaml                      # "reading commits after X up to X": nothing new
$T run examples/csv_to_hudi.yaml                             # (after editing orders.csv) a new commit
$T -v run examples/hudi_to_parquet.yaml                      # only the changed rows this time
```

Progress is recorded only after every sink has written: a failing sink (point `out` at an
unwritable path once) or a preview leaves `offset.json` unchanged. To start over, delete the
checkpoint directory.

Other query types, by editing the source's options in a copy of the example:

| Want | Options |
|---|---|
| The whole table | `query_type: snapshot` (the default) |
| The table as it was | `query_type: snapshot`, `as_of_instant: "2026-10-28 09:00:00"` (or an instant, or a date) |
| MOR base files only | `query_type: read_optimized` (on a copy-on-write table this is the snapshot; the log says so) |
| Changes since a commit | `query_type: incremental`, `begin_instant: <instant>` (an `_hoodie_commit_time` value; `include_metadata_columns: true` keeps that column on the rows) |
| A window | add `end_instant` |
| A change feed | `incremental_format: cdc` on a table written with `hoodie.table.cdc.enabled` → columns `op` (i/u/d), `ts_ms`, `before`, `after` (structs) |
| Keep `_hoodie_*` columns | `include_metadata_columns: true` |
| Only some partitions | a `WHERE country = 'IN'` in the SQL prunes partitions; for incremental reads also `read_options: { hoodie.datasource.read.incr.path.glob: "country=IN/*" }` |

Hudi's instants are visible in `.hoodie/` (`ls /tmp/tributary/hudi/sales/orders/.hoodie | grep commit$`)
or in the `_hoodie_commit_time` column of a preview with `include_metadata_columns: true`.

## 5. Test in the UI (draw it, test it live, run it)

```bash
(cd backend && .venv/bin/tributary-backend serve) &     # :8000, applies migrations
(cd ui && npm run dev)                                   # :5173
```

1. **Connections → New connection.** Name `lake`, type `hudi`, environment `dev`, one field
   `base_path` = `/tmp/tributary/hudi`. Save. (Hudi sinks can use the same connection; its
   `catalog_sync`/`catalog_uri` fields, if you add them, are ignored by sources.)
2. **Pipelines → Import**, choose `examples/hudi_to_parquet.yaml` — or **New pipeline** and
   drag **Apache Hudi** from *Sources* onto the canvas.
3. Select the source node. In the panel: Connection `lake`, Table `sales.orders`, Query Type.
   Mandatory fields carry a red star; fields that apply only to one query type appear when
   that type is chosen (`Begin Instant*`, `End Instant`, `Incremental Format`,
   `Checkpoint Location` for incremental; `As Of Instant` for snapshot). Settings that don't
   fit the query type are reported under the field.
4. Press **Test** on the source: the preview drawer shows the table's rows. Press **Test** on
   the SQL node: its input chips list the Hudi columns (after the automatic deep check) and
   the drawer shows the transformed rows with the quality results.
5. Switch Query Type to `incremental`, set Begin Instant to `earliest` (or an instant), tick
   **Include Metadata Columns** under *More options*, **Test** again: `_hoodie_commit_time`
   appears. With a Checkpoint Location set, **Test** shows what the next run would read and
   records nothing.
6. **Save version → Run (env dev)**. Runs page: live logs (the same `reading hudi table …`
   lines as the CLI), status, data-quality results. Run it twice with a checkpoint location to
   see the second run read nothing new.

Problems (missing table, a `cdc` read of a table that does not record changes, a column the
SQL names that the table lacks) show up in the **Problems** drawer and on the node.

## 6. Test through the API

Same operations the UI calls, useful for scripting:

```bash
engine/.venv/bin/tributary convert examples/hudi_to_parquet.yaml /tmp/hudi_src.json

curl -s -X POST localhost:8000/api/v1/connections -H 'content-type: application/json' \
  -d '{"name":"lake","type":"hudi","environments":{"dev":{"base_path":"/tmp/tributary/hudi"}}}'

# deep validation: per-node schemas (the source's columns come from the table)
jq '{spec: ., env: "dev", deep: true}' /tmp/hudi_src.json \
  | curl -s -X POST localhost:8000/api/v1/validate -H 'content-type: application/json' -d @- | jq .nodes.orders_changes

# preview the source node
jq '{spec: ., env: "dev", node_id: "orders_changes", limit: 5}' /tmp/hudi_src.json \
  | curl -s -X POST localhost:8000/api/v1/preview -H 'content-type: application/json' -d @- | jq .rows
```

`GET /api/v1/components` lists the source with its options schema (what the UI form is built
from). A pipeline created with `POST /api/v1/pipelines`, versioned with
`POST /api/v1/pipelines/{id}/versions`, runs with `POST /api/v1/pipelines/{id}/runs {"env":"dev"}`
in its own worker process; `GET /api/v1/runs/{id}/logs` has the worker's log.

## 7. Test streaming (a Hudi table as a stream of commits)

```yaml
# /tmp/hudi_stream.yaml
version: 1
pipeline:
  id: orders_stream
  mode: streaming
  streaming: { checkpoint_location: /tmp/tributary/chk/orders_stream, trigger: available_now }
environments:
  dev: { connections: { lake: { type: hudi, base_path: /tmp/tributary/hudi } } }
sources:
  - id: orders
    type: hudi
    connection: lake
    options: { table: sales.orders, start_offset: earliest, include_metadata_columns: true }
transforms:
  - id: big
    type: sql
    inputs: [orders]
    options: { sql: "SELECT order_id, amount, country, _hoodie_commit_time AS committed FROM orders WHERE amount > 40" }
sinks:
  - id: out
    type: file
    input: big
    options: { path: /tmp/tributary/out/orders_stream, format: json }
```

```bash
$T run /tmp/hudi_stream.yaml          # processes every commit so far, then stops (available_now)
$T run examples/csv_to_hudi.yaml      # a new commit (edit orders.csv first to change rows)
$T run /tmp/hudi_stream.yaml          # resumes from its checkpoint: only the new commit
cat /tmp/tributary/out/orders_stream/*.json
```

`start_offset: latest` starts a fresh stream at the commits made after it starts. With
`trigger: processing_time` the query keeps running and picks up each new commit within the
interval; stop it with Ctrl-C. A preview of a streaming source shows a snapshot.

## 8. Optional: tables on S3-compatible storage (MinIO) — *not run in the environment this was written in*

The stack has a MinIO profile:

```bash
docker compose -f deploy/docker-compose.yml --profile s3 up -d minio   # console at :9001, user tributary / tributary123
```

Spark needs the S3A connector and the endpoint settings.

**Endpoint settings** — give them to every Spark session Tributary starts (CLI, backend
previews, run workers) with a Spark conf directory (PySpark reads `spark-defaults.conf` from
`SPARK_CONF_DIR`; this part was verified):

```bash
mkdir -p /tmp/spark-conf && cat > /tmp/spark-conf/spark-defaults.conf <<'EOF'
spark.hadoop.fs.s3a.endpoint               http://localhost:9000
spark.hadoop.fs.s3a.access.key             tributary
spark.hadoop.fs.s3a.secret.key             tributary123
spark.hadoop.fs.s3a.path.style.access      true
spark.hadoop.fs.s3a.connection.ssl.enabled false
EOF
export SPARK_CONF_DIR=/tmp/spark-conf     # set before starting the CLI or the backend
```

**The connector jar** — not through `spark-defaults.conf`: the engine sets
`spark.jars.packages` itself (the plugins' packages), and a value set on the session builder
replaces the one from the defaults file. Add it where the engine merges packages:

```yaml
# in the pipeline: CLI runs and backend run workers
runtime:
  dev:
    master: "local[*]"
    resources: auto
    spark_conf: { spark.jars.packages: "org.apache.hadoop:hadoop-aws:3.3.4" }
```
```bash
# the backend's shared preview/validation session
export TRIBUTARY_PREVIEW_EXTRA_PACKAGES='["org.apache.hadoop:hadoop-aws:3.3.4"]'
```

Then create a bucket in the MinIO console and use `base_path: s3a://lake/hudi` in the
connection, for sinks and sources alike; a `checkpoint_location: s3a://lake/checkpoints/x`
works the same way (progress files are written through Hadoop's FileSystem).

## 9. Hudi 1.x

```bash
export TRIBUTARY_HUDI_VERSION=1.0.2      # CLI and backend alike
```

Everything in this document also passes against 1.0.2 (tables written by 1.0.2 are table
version 8 and cannot be read by the 0.15 bundle). Incremental reads keep the same meaning on
both lines: the source asks 1.x for its version-6 incremental relation, because 1.x's own one
makes `begin_instant` inclusive of a commit's completion time (and, in 1.0.2, returns rows of
untouched commits in open-ended reads).

## 10. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `hudi table sales.orders not found at …/sales/orders (no .hoodie/hoodie.properties there)` | `base_path` must be the directory above the table; check `ls <base_path>/<db>/<table>/.hoodie`. |
| `does not record changes (hoodie.table.cdc.enabled is off)` | `cdc` needs a table written with `hoodie.table.cdc.enabled=true` (sink `write_options`); use `latest_state` otherwise. |
| `time travel is a snapshot read, not 'incremental'` / `only incremental reads start at an instant` | Settings of another query type: remove them or change `query_type`. |
| Second run reads the same commits again | Expected after a failed run (progress is recorded only after the sinks write). If a run was killed mid-write, check `pending.json` vs `offset.json` in the checkpoint directory. |
| Progress should start over | Delete the checkpoint directory (or `offset.json`). |
| `Failed to find the data source: hudi` | The bundle is not on the session: the first run needs Maven Central; check `~/.ivy2/jars` for `org.apache.hudi_hudi-spark3.5-bundle_2.12-0.15.0.jar`. |
| `needs spark.serializer=…KryoSerializer` | Only Hudi **sinks** need Kryo; reading does not. Runs add it automatically when a sink is present. |
| Reading a 1.x table with the default bundle fails | `export TRIBUTARY_HUDI_VERSION=1.0.2`. |
| `Method getTimestamp([]) does not exist` | Mixed Hudi versions in one session (a stale jar in `spark.jars`); use one bundle. |
| Snapshot columns in a different order than incremental ones | Hudi lists partition columns last on snapshot reads; select columns by name in SQL. |

## 11. The automated tests, for reference

```bash
cd engine && .venv/bin/pytest tests/test_hudi_source.py -q        # 26 tests, ~8 min; fetches the bundle once, skips when offline
TRIBUTARY_HUDI_VERSION=1.0.2 .venv/bin/pytest tests/test_hudi_source.py -q
cd ../backend && .venv/bin/pytest tests/test_hudi_source.py -q    # through the API, needs the Postgres container
```
