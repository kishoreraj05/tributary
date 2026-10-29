# Apache Iceberg as a source

How to read Iceberg tables in Tributary, feature by feature, and how to test each feature
yourself: from the UI (the node's **Test** button), from the CLI, and with the automated
tests that ship with the engine.

Every scenario below is also an automated test; the [coverage matrix](#test-coverage) at the end
maps features to tests. Run them with:

```bash
cd engine && .venv/bin/pytest tests/test_iceberg_source.py tests/test_iceberg_source_scenarios.py -v
```

They need the Iceberg runtime jar (fetched once from Maven Central into `~/.cache/tributary/jars`;
offline they skip) and write to a Hadoop catalog in a temporary directory.

## Setup: a connection and a table to read

The catalog is described once, as a connection of type `iceberg`. The same connection serves
the Iceberg sink, so a pipeline can read and write the same lakehouse.

| Field | hadoop | hive | rest | glue / jdbc / nessie |
|---|---|---|---|---|
| `catalog_type` | `hadoop` | `hive` | `rest` | `glue`, `jdbc`, `nessie` (their jars must be on the session) |
| `warehouse` | required (local path, `s3a://`, `gs://`, `abfss://`, `hdfs://`) | optional | optional | required |
| `uri` | – | `thrift://host:9083` | `http(s)://host:8181` | required |
| `credential`, `token` | – | – | optional, secrets | – |
| `catalog_name` | optional: the name in Spark SQL (`lake.sales.orders`) | | | |
| `catalog_properties` | extra `spark.sql.catalog.<name>.<key>` settings (`io-impl`, `s3.endpoint`, ...) | | | |

To get a table to experiment with, run the sink example, which writes `sales.orders` and
`sales.revenue_by_country` into `/tmp/tributary/warehouse` (run it twice to get several snapshots):

```bash
engine/.venv/bin/tributary run examples/csv_to_iceberg.yaml
```

**UI:** Connections → New connection: name `lake`, type `iceberg`, fields `catalog_type=hadoop`,
`warehouse=/tmp/tributary/warehouse`, `catalog_name=lake`. In a pipeline, drag **Apache Iceberg**
from *Sources*, pick the connection, type the `table` (`sales.orders`) and press **Test**.

**CLI / YAML:** [`examples/iceberg_to_parquet.yaml`](../examples/iceberg_to_parquet.yaml) (batch)
and [`examples/iceberg_stream_to_parquet.yaml`](../examples/iceberg_stream_to_parquet.yaml) (streaming):

```yaml
sources:
  - id: orders_src
    type: iceberg
    connection: lake
    options:
      table: sales.orders
```

```bash
tributary validate examples/iceberg_to_parquet.yaml --deep     # columns and types, read from the catalog
tributary preview  examples/iceberg_to_parquet.yaml --node orders_src --limit 5
tributary plan     examples/iceberg_to_parquet.yaml --explain  # Spark's physical plans (section 4)
tributary run      examples/iceberg_to_parquet.yaml
```

### All options

| Option | Meaning |
|---|---|
| `table` | `namespace.table` inside the catalog |
| `filter` | SQL condition applied at the source and pushed down into Iceberg's scan |
| `columns` | Read only these columns |
| `snapshot_id` | Time travel: the table as of this snapshot (a string of digits) |
| `as_of_timestamp` | Time travel: the snapshot current at this time (ISO 8601, UTC unless an offset is given, or epoch milliseconds) |
| `branch`, `tag` | Time travel: the head of a branch, or the snapshot a tag points to |
| `start_snapshot_id`, `end_snapshot_id` | Incremental read: rows appended after `start` (exclusive) up to `end` (inclusive, default: current) |
| `metadata_table` | Read a metadata table instead: `snapshots`, `history`, `refs`, `files`, `partitions`, `manifests`, ... |
| `stream_from_timestamp` | Streaming: skip snapshots committed before this time |
| `streaming_skip_delete_snapshots`, `streaming_skip_overwrite_snapshots` | Streaming: get past snapshots that are not appends |
| `max_files_per_trigger`, `max_rows_per_trigger` | Streaming: throttle each micro-batch |
| `read_options` | Anything else Iceberg's reader accepts (`split-size`, `vectorization-enabled`, `locality`, ...) |

The node's optional **schema** block is not a reader schema (Iceberg does not take one): when
given, it selects exactly those columns and casts them to the declared types (section 3).

Options that contradict each other are rejected on the field that introduces the conflict
(`snapshot_id` with `as_of_timestamp`, `tag` with `branch`, an increment with `tag`, `end_snapshot_id`
without `start_snapshot_id`, a metadata table with time travel), so the UI marks the right field.

---

## 1. Snapshots and time travel

Every commit to an Iceberg table is a snapshot. The source reads the current one by default; the
time travel options read an older one.

### Finding snapshots, tags and branches

Set `metadata_table: snapshots` on a source node (it is under *More options* in the UI) and press
**Test**: each row is a commit with its `snapshot_id`, `committed_at` and `operation` (`append`,
`overwrite`, `delete`, `replace`). `refs` lists branches and tags with the snapshot they point to,
`history` the lineage of the current snapshot, `partitions` and `files` what is stored where.

Snapshot ids are 64-bit numbers. Previews show them as strings of digits (a browser would round
them otherwise), and `snapshot_id` takes them as strings, so they can be pasted straight from the
preview into the option. In YAML a plain number is fine too.

From SQL (any Spark session with the catalog configured, see section 4 for `tributary preview`):

```sql
SELECT snapshot_id, committed_at, operation FROM lake.sales.orders.snapshots ORDER BY committed_at;
SELECT name, type, snapshot_id FROM lake.sales.orders.refs;
```

### Reading a snapshot

```yaml
options: { table: sales.orders, snapshot_id: "1259856710711799087" }
options: { table: sales.orders, as_of_timestamp: 2024-05-01T00:00:00Z }   # or epoch millis: "1714521600000"
options: { table: sales.orders, tag: v1 }
options: { table: sales.orders, branch: audit }
```

**How to test it (UI):** with `metadata_table: snapshots`, note the oldest id and commit time.
Clear `metadata_table`, paste the id into *Snapshot Id*, press **Test**: the rows are the table's
first version (after one `csv_to_iceberg.yaml` run that is 6 rows; the merge of a second run
changes nothing, an append adds rows). Try *As Of Timestamp* with a time between two commits.
Put in an id that does not exist: the node shows `snapshot_id 12345 is not a snapshot of
lake.sales.orders; the latest are <ids with their times>`. The same for a tag or branch: `has no
tag 'v9' (tags: none)`, `has no branch 'dev' (branches: main)`; for a time before the first commit:
`has no snapshot at or before ...; the oldest is from ...`.

**How to test it (CLI):** edit the options in `examples/iceberg_to_parquet.yaml` (the commented
lines) and run `tributary preview examples/iceberg_to_parquet.yaml --node orders_src`, or run the
pipeline and look at the Parquet output.

Tags and branches are created with Iceberg's SQL extensions (`ALTER TABLE ... CREATE TAG v1`) or
its Java API; the tests use the API because the sessions Tributary builds do not load the
extensions (see the engine README).

Notes:
- A snapshot is read with the schema it had at the time (section 3).
- `branch` can be combined with an incremental read (`start_snapshot_id` walks that branch); a
  pinned `snapshot_id`, `as_of_timestamp` or `tag` cannot.
- Time travel options are batch only: a stream is a stream of appends on the main branch and refuses
  them (Iceberg would silently ignore them).


---

## 2. Incremental and streaming ingestion

### Incremental batch reads: the bookmark pattern

A scheduled batch job that should only process what is new keeps the id of the last snapshot it
handled and reads the appends after it:

```yaml
options: { table: sales.orders, start_snapshot_id: "1259856710711799087" }            # everything appended since
options: { table: sales.orders, start_snapshot_id: "...", end_snapshot_id: "..." }    # a closed range
```

- Only append snapshots count: rows written by an `overwrite`, `overwrite_partitions` or `merge`
  in between are not part of an increment (Iceberg's incremental scan is append-only; use a stream
  with `streaming_skip_overwrite_snapshots` or a full read if you need them).
- When nothing was appended since `start_snapshot_id`, the read returns no rows with the table's
  schema (Iceberg itself would fail with "not a parent ancestor"), so downstream SQL still compiles
  and the job ends cleanly.
- Filters and column pruning apply to increments as to any read (section 4).

**How to test it:** with `metadata_table: snapshots` note the id of the second snapshot; set it as
`start_snapshot_id` and **Test**: only rows appended after it. Run `csv_to_iceberg.yaml` again with
new rows in `examples/data/orders.csv` (a merge is an overwrite, so instead append: in the UI,
build a small CSV → Iceberg pipeline with `mode: append`) and test again. Set `start_snapshot_id` to
the latest id: 0 rows, no error.


---

## 3. Schema evolution

Iceberg evolves schemas in metadata (`ALTER TABLE ... ADD COLUMNS / RENAME COLUMN / DROP COLUMN`,
or the sink's `write_options: { merge-schema: true }` on a table with
`write.spark.accept-any-schema=true`). The source follows:

| Change | What the source reads |
|---|---|
| column added | present on the next read; `null` for rows written before it existed |
| column renamed | the new name, data intact (Iceberg tracks columns by id, not by name) |
| column dropped | gone; SQL that still uses it fails on that node in deep validation |
| type widened (int → long, float → double, decimal precision) | the new type |
| time travel to an older snapshot | the schema *of that snapshot* |
| incremental read | the current schema |
| a running stream | keeps the schema it started with; the next run/restart uses the current one |

Every read asks the catalog for the latest metadata (`REFRESH TABLE`), so a change made by
another process (a run, a different tool) is visible immediately, including in the shared
session the UI uses for **Test**, where Iceberg's catalog cache could otherwise lag by 30 s.

### Pinning a contract with a declared schema

Put a schema on the node (UI: *Schema* → `ddl`) to make the pipeline independent of additions:

```yaml
sources:
  - id: orders_src
    type: iceberg
    connection: lake
    options: { table: sales.orders }
    schema: { kind: ddl, ddl: "order_id BIGINT, amount DECIMAL(10,2), country STRING" }
```

Only those columns are read, cast to those types; new table columns do not leak into the
pipeline; a column the table no longer has is an error on the source node. Deep validation and
the UI's column lists use the declared schema without touching the catalog.

**How to test it:** with the shared Spark session of a `tributary preview` or from `pyspark` with
the catalog configured (`spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog`,
`spark.sql.catalog.lake.type=hadoop`, `spark.sql.catalog.lake.warehouse=/tmp/tributary/warehouse`),
run `ALTER TABLE lake.sales.orders ADD COLUMNS (note STRING)` and
`INSERT INTO lake.sales.orders VALUES (...)`; then **Test** the node: the new column is there with
nulls for old rows. Put a snapshot id from before the change into *Snapshot Id*: the column is
gone. Add a `ddl` schema without it: it stays hidden. Run `tributary validate --deep` on a
pipeline whose SQL still uses a dropped column: the error lands on the transform.


---

## 4. Predicate pushdown and partition pruning

Spark pushes predicates and projections down to Iceberg's scan. Iceberg then skips whole
partitions (by the partition spec: `country`, `days(order_ts)`, `bucket(16, id)`, ...) and whole
files (by the min/max statistics of every column in every data file), and reads only the columns
asked for from the Parquet files. This happens:

- for the source's own `filter` and `columns`, in runs **and** in previews;
- for `WHERE` clauses and projections of downstream SQL transforms, in runs (in a preview,
  sources are read bounded by a `LIMIT`, which stops predicates at the source: use `filter` to
  preview a subset);
- in batch reads, time travel and increments. A stream applies the filter per micro-batch instead
  (Iceberg's streaming scan does not take predicates).

```yaml
options:
  table: sales.orders
  filter: "country = 'IN' AND order_ts >= '2024-01-01'"
  columns: [order_id, amount, order_ts]
```

### Seeing what reached Iceberg

Three ways, all showing Spark's physical plan. In it, the `BatchScan` node lists the pushed-down
predicates as `[filters=country = 'IN', order_ts >= ..., groupedBy=]` and the columns it reads as
`Output [3]: [order_id, amount, order_ts]`:

1. **UI:** press **Test** and open *Spark plan* under the rows.
2. **CLI, per node:** `tributary preview FILE --node orders_src --explain`.
3. **CLI, the whole batch pipeline as a run builds it:** `tributary plan FILE --explain` prints the
   plan of every source and transform (reads metadata only). This is the one that shows a
   transform's `WHERE` arriving in the scan:

```
== Spark plan: big_orders ==
(1) BatchScan lake.sales.orders
Output [5]: [order_id#0, customer_id#1, amount#2, country#3, order_ts#4]
lake.sales.orders (branch=null) [filters=amount IS NOT NULL, amount >= 20.0, country IS NOT NULL, groupedBy=]
```

How many files were skipped shows in the Spark UI's SQL tab on the `BatchScan` node
(`resultDataFiles`, `skippedDataFiles`, `skippedDataManifests`); the tests read the same metrics
from the executed plan. To predict it, `metadata_table: files` (with its `partition` and
`lower_bounds`/`upper_bounds` columns) shows which files a predicate can skip.

**How to test it:** in the UI, set `filter: country = 'IN'` and press **Test**: fewer rows, and the
plan shows `filters=... country = 'IN'`. Set `columns: [order_id, country]`: `Output [2]`. Run
`tributary plan examples/iceberg_to_parquet.yaml --explain`: the `big_orders` transform's
`WHERE o.amount >= 20` is in the scan's filters although the source has none. Partition pruning
needs a partitioned table: `csv_to_iceberg.yaml` partitions `sales.orders` by `country` and
`days(order_ts)`, so `filter: country = 'IN'` reads only the `country=IN` files (check
`metadata_table: partitions`).


---

## Limits

- Catalog types other than `hadoop` are configured the same way but have not been run in this
  project's tests; `glue`, `jdbc` and `nessie` need their own jars (`runtime.spark_conf:
  { spark.jars.packages: ... }`).
- Changelog reads (updates and deletes as rows, Iceberg's `changes` procedure) are not supported;
  streams and increments are append-only.
- Tags and branches are read, not created: creating them needs Iceberg's SQL extensions or API.
- Previews (UI **Test**, `tributary preview`) use a bounded read of the source, so a transform's
  predicate is not pushed down there; `filter` on the source is.

---

## Test coverage

All tests need the Iceberg runtime jar (fetched once into `~/.cache/tributary/jars`; offline they
skip) and run on a Hadoop catalog in a temporary directory. `S` = `engine/tests/test_iceberg_source.py`,
`X` = `engine/tests/test_iceberg_source_scenarios.py`, `B` = `backend/tests/test_iceberg.py`.

```bash
cd engine  && .venv/bin/pytest tests/test_iceberg_source.py tests/test_iceberg_source_scenarios.py -v
cd backend && .venv/bin/pytest tests/test_iceberg.py -v        # needs Postgres (deploy/docker-compose.yml)
```

### Plugin, options and session (no data read)

| What | Test |
|---|---|
| Contract, capabilities (`batch`, `streaming`), JSON Schema | S `test_static_contract` |
| Conflicting options rejected on the field that introduces them (`snapshot_id`/`as_of_timestamp`, `branch`/`tag`, increments with a pinned read, `end` without `start`, metadata table with time travel, column names, bad timestamps) | S `test_conflicting_options_are_reported_on_the_field_that_introduces_them`, S `test_issues_land_on_the_source_field` |
| Timestamps: ISO 8601 (with/without zone), epoch millis → reader options | S `test_timestamps_accept_iso_8601_or_epoch_millis` |
| Streaming options → Iceberg reader options; pinned reads refused for streams | S `test_streaming_options_map_to_the_reader_and_pinned_reads_are_refused` |
| Source and sink share catalog settings; one `iceberg` connection serves both | S `test_source_and_sink_share_the_catalog_settings`, S `test_connection_supplies_the_catalog_to_a_source` |
| Session gets the Iceberg package and one catalog for source + sink; conflicting catalogs rejected | S `test_session_gets_the_package_and_one_catalog_for_source_and_sink` |
| Input size for resource sizing (local Hadoop warehouse only) | S `test_input_size_is_known_for_a_local_hadoop_warehouse` |

### 1. Snapshots and time travel

| What | Test |
|---|---|
| `metadata_table`: snapshots, refs, history, partitions from a node/preview; ids pasted back into `snapshot_id`; no time travel on metadata tables | X `test_snapshots_tags_and_branches_are_discoverable_from_a_source_node` |
| `snapshot_id`, `as_of_timestamp` (ISO, ISO+Z, epoch ms, between commits), `tag`, `branch` (incl. `main`) through full runs; `describe()` | X `test_every_time_travel_option_reads_the_table_as_it_was`, S `test_reads_the_current_table_time_travel_and_increments` |
| Time travel in previews and deep validation | X `test_time_travel_in_a_preview_and_in_deep_validation` |
| Unknown snapshot / start snapshot / tag / branch, time before the first commit → clear errors listing what exists, also via preview | X `test_wrong_snapshot_tag_branch_or_time_is_a_clear_error` |
| Missing table → clear error | S `test_missing_table_is_a_clear_error` |
| API: metadata preview returns exact 64-bit ids as strings; time travel preview with one of them | B `test_iceberg_source_from_connection_preview_and_run` |

### 2. Incremental and streaming ingestion

| What | Test |
|---|---|
| Bookmark pattern: `start_snapshot_id` over several snapshots, closed ranges, nothing new → empty frame with the table's schema, through runs | X `test_incremental_reads_walk_the_snapshots_with_a_bookmark` |
| Increments are append-only (overwrite/merge rows excluded) | X `test_incremental_reads_only_see_appends` |
| Stream follows appends across runs from one checkpoint; overwrite snapshots stop it unless skipped | S `test_streaming_follows_appends` |
| `stream_from_timestamp` skips history; nothing replayed; fresh checkpoint starts with the whole table | X `test_stream_from_timestamp_skips_history_and_each_run_continues_from_its_checkpoint` |
| Merge-on-read deletes stop a stream (also a fresh one) unless `streaming_skip_delete_snapshots`; no retraction | X `test_streams_stop_at_deletes_unless_told_to_skip_them` |
| Throttling (`max_files_per_trigger`) with `processing_time`; refused with `available_now` on Iceberg < 1.10 | S `test_rate_limits_throttle_micro_batches` |
| Iceberg → Iceberg stream: exactly once across restarts, micro-batches tagged in snapshot summaries | X `test_streaming_iceberg_to_iceberg_is_exactly_once_across_restarts` |
| Iceberg → Iceberg batch through one connection | S `test_iceberg_to_iceberg_through_one_connection` |

### 3. Schema evolution

| What | Test |
|---|---|
| Added (nulls for old rows), renamed, dropped, type-promoted columns on the next read; deep validation reflects them and flags SQL on a dropped column | X `test_added_renamed_and_dropped_columns_show_up_on_the_next_read` |
| Time travel uses the snapshot's schema; increments use the current one | X `test_time_travel_reads_use_the_schema_of_that_snapshot` |
| Declared `schema` pins columns and casts types; new columns hidden; a lost column is an error | X `test_a_declared_schema_pins_the_contract`, S `test_declared_schema_selects_and_casts_columns` |
| Columns added through the sink (`merge-schema`) readable | X `test_columns_added_through_the_sink_are_readable` |
| A stream picks up new columns when it restarts | X `test_a_stream_picks_up_new_columns_when_it_restarts` |
| Table's own schema in previews and deep validation without a declared schema | S `test_preview_and_deep_validation_read_the_table` |

### 4. Predicate pushdown and partition pruning

| What | Test |
|---|---|
| Source `filter` pushed into the scan; partition pruning (files read/skipped from scan metrics); min/max pruning on a non-partition column; pushdown kept with increments | X `test_source_filter_is_pushed_down_and_prunes_partitions` |
| SQL `WHERE` reaches the scan in a run (`explain`), not in a bounded preview; `preview --explain` shows the source `filter` | X `test_sql_predicates_reach_the_scan_in_a_run_but_not_in_a_bounded_preview` |
| Column pruning via `columns` and via downstream SQL; filter on a column not in the output | X `test_column_pruning` |
| File statistics prune within a partition (`>=`, `BETWEEN`) | X `test_file_statistics_prune_within_a_partition` |
| API: preview `explain` returns the plan with the pushed-down filter | B `test_iceberg_source_from_connection_preview_and_run` |

### End to end through the backend and UI

| What | Test |
|---|---|
| Stored connection → deep validate (table columns) → source and sink previews → wrong table reported on the node → versioned run in a worker writes Parquet | B `test_iceberg_source_from_connection_preview_and_run` |
| Shared preview session has the Iceberg runtime and no SQL extensions | B `test_preview_session_has_the_iceberg_runtime_but_no_extensions` |
| UI (manual, scripted with a headless browser during development): connection form, palette tile, fields filled from the connection, field-level validation, Test, metadata/time-travel previews, Spark plan in the drawer, Save version → Run | see the walkthroughs in sections 1–4 |
