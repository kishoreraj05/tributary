# Delta Lake as a source: using and testing it locally

Tributary reads Delta Lake tables with the built-in **`delta` source** (type `delta` in a
pipeline, "Delta Lake" in the editor's palette). This is the guide to using it and to testing
it on your machine with a Python venv, as in the main README.

## What the source does

| | |
|---|---|
| **Batch** | the current table, or a past version with `version` / `timestamp` (time travel) |
| **Streaming** | the table as it is, then every commit; `starting_version` / `starting_timestamp` move the start (`latest` = new commits only); `on_changes` decides what happens on commits that update or delete rows; `max_files_per_trigger` bounds a micro-batch |
| **Change data feed** | `change_feed: true` reads the *changes* instead of the rows, with `_change_type`, `_commit_version`, `_commit_timestamp` columns (the table must record them: `delta.enableChangeDataFeed`, which the Delta sink sets via `write_options` when it creates a table) |
| **Schema** | optional: the table has one, and the editor / `validate --deep` read it from the table's log. A declared `schema:` is a contract instead (those columns, cast to those types) and lets a pipeline be drawn before the table exists |
| **Dummy data** | in the `local` environment only: with the node's **Use Dummy Data** toggle on, it produces the uploaded JSON Lines records (up to 1000) and never contacts the table, in batch, preview and streaming ([local development](local-development.md)) |

Tables are addressed by `path` (local, `s3a://`, `gs://`, `abfss://`, `hdfs://`). Like the
Delta sink, the source needs only Delta's session catalog, which it sets itself; no SQL
extension. Everything it needs ships with the engine (`delta-spark` and its jar).

Three example pipelines exercise it end to end; they share a lake directory
(`/tmp/tributary/lake`):

| Example | What it shows |
|---|---|
| `examples/csv_to_delta.yaml` | *produces* the `orders` table (Delta sink, merge; change data feed on) |
| `examples/delta_to_parquet.yaml` | batch read, time travel in comments, dummy data for `-e local` |
| `examples/delta_changes_to_delta.yaml` | streaming change data capture: the feed of `orders` keeps an `orders_mirror` table current, deletes included |

## Running it locally (no Docker needed for Delta)

Delta tables are files, so nothing has to run in Docker for the source itself; Postgres is only
needed for the UI.

```bash
(cd engine && python3.11 -m venv .venv && .venv/bin/pip install -e ".[dev]")
T=engine/.venv/bin/tributary

$T run      examples/csv_to_delta.yaml                       # creates /tmp/tributary/lake/orders (fetches the Delta jars once)
$T validate examples/delta_to_parquet.yaml -e dev --deep     # columns read from the table's log
$T preview  examples/delta_to_parquet.yaml -e dev --node orders_src --limit 5
$T run      examples/delta_to_parquet.yaml -e dev            # -> /tmp/tributary/delta_to_parquet/customer_totals
$T run      examples/delta_to_parquet.yaml -e local          # the same on the source's dummy data, no table read
$T preview  examples/delta_changes_to_delta.yaml --node orders_changes     # the last commit's changes
$T run      examples/delta_changes_to_delta.yaml             # mirror = orders (one commit per micro-batch)
$T run      examples/csv_to_delta.yaml                       # change the source table again ...
$T run      examples/delta_changes_to_delta.yaml             # ... only the new changes are applied
```

For the UI: `docker compose -f deploy/docker-compose.yml up -d postgres`, then the backend and
the UI as in the main README, and continue at "Testing in the UI" below.

The engine's tests of the source (21, local disk, no services) and the API tests:

```bash
(cd engine && .venv/bin/pytest tests/test_delta_source.py -q)
(cd backend && .venv/bin/pytest tests/test_delta_source.py -q)     # needs the docker-compose Postgres
```

## Testing in the UI

1. **Pipelines → Import** `examples/delta_to_parquet.yaml` (or draw: drag **Delta Lake** from the
   *Sources* palette, set `path` to `/tmp/tributary/lake/orders`).
2. Validation runs as you draw. With no schema block, the deep validation reads the table's
   columns from its log and the SQL editor autocompletes them; with a `ddl` schema, nothing is
   contacted and the pipeline validates before the table exists.
3. Press **Test** on the source node: a bounded read. Open *More options* to try `version: 0`
   (time travel) or `change_feed` (the changes of the last commit appear in the sample).
4. **Run**: a version is created, a worker runs it; the run page shows logs and the quality
   results.
5. **Dummy data**: select the `local` environment, toggle **Use Dummy Data** on the source node
   and upload a JSON Lines file (one record per line, up to 1000), e.g.
   ```json
   {"order_id": 1, "customer_id": 100, "amount": 25.5, "country": "IN", "order_ts": "2026-10-01 10:00:00"}
   {"order_id": 2, "customer_id": 101, "amount": 99.99, "country": "US", "order_ts": "2026-10-01 11:30:00"}
   ```
   Test any downstream node: it runs on those records; the `path` is never touched. Any other
   environment ignores the toggle and reads the real table. A sink's toggle writes nothing and
   lets its Preview show the rows it would receive.
6. Streaming: set the pipeline mode to *streaming* (Pipeline settings: checkpoint location,
   trigger), mark the Delta node as the streaming source, and run; with `available_now` the run
   processes what is there and stops; with `processing_time` it keeps following commits.

Mandatory fields carry a red star; `merge_keys` of the Delta sink gets one as soon as its mode
is `merge`.

## Options

| Option | Default | What it is |
|---|---|---|
| `path` | required | Table directory. |
| `change_feed` | `false` | Read the change data feed instead of the rows. |
| `version`, `timestamp` | – | Time travel (batch only; not with a change feed). |
| `starting_version` | – | Streaming: version to start from, or `latest`. Change-feed batch reads: first version (required unless `starting_timestamp`). |
| `starting_timestamp` | – | Same, as a time (`2024-05-01 12:00:00`). |
| `on_changes` | `fail` | Streaming, commits that update/delete: `fail`, `ignore_deletes`, `ignore_changes` (re-emit rewritten files; pair with a merge sink), `skip_commits`. |
| `max_files_per_trigger` | – | Streaming: files per micro-batch. |
| `read_options` | – | Any other Delta reader option (`maxBytesPerTrigger`, `withEventTimeOrder`, ...). |

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Path does not exist` / `is not a Delta table` in the editor or a preview | Nothing at `path` yet: run the producing pipeline first, or declare a `schema:` (or use dummy data in the `local` environment) to work ahead of it. In Docker, the path is the container's: `/tmp/tributary/...` maps to `.lake/` on the host. |
| `DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG` | Delta's session catalog is missing from a session the source did not build. The source sets it on any session it runs in, so this only appears if another setting holds `spark.sql.catalog.spark_catalog`; see the error message. |
| `DELTA_MISSING_CHANGE_DATA` / `change data was not recorded` | `change_feed: true` on a table that does not record it. Create it with `write_options: { delta.enableChangeDataFeed: true }` (as `csv_to_delta.yaml` does) or `ALTER TABLE ... SET TBLPROPERTIES (delta.enableChangeDataFeed = true)` for commits from now on. |
| `reading a change feed in batch needs starting_version or starting_timestamp` | Batch change-feed reads need a start; a stream does not. |
| `Cannot time travel Delta table to version N` | The version does not exist (or was vacuumed); `DESCRIBE HISTORY` / the table's `_delta_log` shows what is there. |
| A stream stops with `data update ... not supported` / `skipChangeCommits` | The source table had an update or delete after the stream's start; choose `on_changes`, or read the change feed instead. |
| `starting_version: latest` with `available_now` never picks anything up | Nothing was processed on the first run, so no offset was checkpointed, and the next run resolves `latest` again. Use it with a `processing_time` trigger (a query that keeps running). |
| First run hangs on `resolving dependencies` (native) | Maven Central is not reachable; allow it once (the jars are cached in `~/.ivy2`). |
| Files in `.lake/` are owned by root (Linux) | The container runs as root; `sudo chown -R $USER .lake` or point `TRIBUTARY_LAKE_DIR` at a directory you do not mind. |

## What was verified, and where

| Checked | How |
|---|---|
| Batch reads, time travel, declared-schema contract, change feed in batch, previews and deep validation, streams (start positions, `latest`, `on_changes`), change-feed CDC into a merge table | `engine/tests/test_delta_source.py` (21 tests, real Delta tables on local disk) |
| The same through the API, including a run in a worker process whose session is built from what the source asks for | `backend/tests/test_delta_source.py` (6 tests) |
| The three examples end to end through the CLI (native) | run while building this |
