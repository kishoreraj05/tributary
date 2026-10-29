# MongoDB in Tributary: using and testing it locally

Tributary can write to MongoDB (the `mongodb` **sink**) and read from it (the `mongodb`
**source**, which can also stand in for a collection with pasted dummy data). Both go through
the official [MongoDB Spark Connector](https://www.mongodb.com/docs/spark-connector/) (10.5.0,
fetched from Maven Central on first use) and need nothing else: no catalog, no SQL extension,
no session setting.

This page is the local how-to: start a MongoDB, write to it from the CLI and from the UI,
check the documents, and run the tests. For the option-by-option reference see the
[engine README](../engine/README.md) ("MongoDB source" / "MongoDB sink").

## 1. Start MongoDB

The dev stack in [`deploy/docker-compose.yml`](../deploy/docker-compose.yml) has a `mongodb`
service (`mongo:7`, standalone, no authentication, data kept in the `mongodb-data` volume):

```bash
docker compose -f deploy/docker-compose.yml up -d postgres mongodb
docker compose -f deploy/docker-compose.yml ps          # both should be "healthy"
```

From the host it is `mongodb://localhost:27017`. A shell into it:

```bash
docker compose -f deploy/docker-compose.yml exec mongodb mongosh tributary
```

> Already have a MongoDB (Atlas, a replica set, one with auth)? Use its URI instead, e.g.
> `mongodb+srv://cluster0.example.net/?retryWrites=true`, and put the user name and password
> in `username`/`password` (in the UI: a connection, so the password is stored encrypted).

## 2. Write to MongoDB from the CLI

[`examples/csv_to_mongodb.yaml`](../examples/csv_to_mongodb.yaml) reads the sample orders CSV,
cleans it with SQL, drops rows that fail the quality checks, upserts the orders into
`tributary.orders` and overwrites a per-country revenue collection:

```bash
cd engine && python3.11 -m venv .venv && .venv/bin/pip install -e ".[dev]" && cd ..
engine/.venv/bin/tributary validate examples/csv_to_mongodb.yaml
engine/.venv/bin/tributary plan examples/csv_to_mongodb.yaml
engine/.venv/bin/tributary preview examples/csv_to_mongodb.yaml --node orders_upsert   # dry run, shows the rows
engine/.venv/bin/tributary run examples/csv_to_mongodb.yaml                            # the first run downloads the connector
```

Check the result:

```bash
docker compose -f deploy/docker-compose.yml exec mongodb mongosh tributary --quiet --eval '
  print("orders:", db.orders.countDocuments());
  printjson(db.orders.find({}, {_id: 0}).sort({order_id: 1}).limit(3).toArray());
  printjson(db.revenue_by_country.find().sort({_id: 1}).toArray());'
```

Expected: 6 orders (8 in the CSV, minus one with no `order_id` and one with a negative
amount), and one revenue document per country keyed by the country code (the SQL produces an
`_id` column, which becomes the document id). **Run it again**: the counts stay the same,
because `orders_upsert` matches on `order_id` and `revenue_out` overwrites.

The sink's options in that file:

```yaml
- id: orders_upsert
  type: mongodb
  input: orders_clean
  options:
    connection_uri: mongodb://localhost:27017
    database: tributary
    collection: orders
    mode: upsert            # append | overwrite | upsert
    merge_keys: [order_id]  # upsert only: what identifies a document
```

| Mode | What happens | Re-running the same data |
|---|---|---|
| `append` | inserts the rows | adds them again (each run is a new batch); a *replayed* streaming micro-batch or a *retried* write does **not** duplicate, thanks to `deterministic_ids` |
| `overwrite` | drops the collection, then inserts | same result |
| `upsert` | `on_match: replace` swaps the matching document for the row, `on_match: update` sets only the row's columns; unmatched rows are inserted | same result |

## 3. Write to MongoDB from the UI

Start the backend and the UI (or use the Docker image, section 6):

```bash
(cd backend && python3.11 -m venv .venv && .venv/bin/pip install -e ../engine -e . && .venv/bin/tributary-backend serve) &
(cd ui && npm install && npm run dev)      # http://localhost:5173
```

1. **Connections → New connection.** Name `mongo_local`, type `mongodb`, environment `dev`
   with the fields `connection_uri` = `mongodb://localhost:27017` and `database` = `tributary`.
   (For a server with auth, add `username` and `password`; `password` is detected as a secret
   and encrypted.) Save.
2. **Pipelines → New pipeline**, e.g. "Orders to MongoDB". On the canvas:
   - Add a **Files** source: path `<repo>/examples/data/orders.csv`, format `csv`,
     read option `header` = `true`, schema `ddl`:
     `order_id INT, customer_id INT, amount DOUBLE, country STRING, order_ts TIMESTAMP`.
   - Add a **SQL** transform, connect the source to it, and write for instance
     `SELECT order_id, customer_id, amount, upper(country) AS country, order_ts FROM file_source_1 WHERE order_id IS NOT NULL`
     (the table name is the source's node id, shown in the *Inputs* chip).
   - Add a **MongoDB** sink (Sinks group), connect the transform to it, pick the connection
     `mongo_local` (its fields show "from mongo_local" instead of a red star), set
     `collection` = `orders`, open *More options*, `mode` = `upsert`, `merge_keys` = `order_id`.
     Mandatory fields carry a red star; `merge_keys` becomes mandatory when the mode is `upsert`.
3. The toolbar shows **Valid** once the live validation (including column checks on a shared
   Spark session) passes. Press **Test** on the sink for a dry run of what it would write.
4. **Run** → *Save as v1 & run*. The *Runs* drawer follows the worker's log; the summary
   ends with `"sinks_written": ["<your sink id>"]`. Check the collection as in section 2.

Import instead of drawing: **Import** on the pipelines page takes `examples/csv_to_mongodb.yaml`
as is (its `connection_uri` is inline; no connection needed).

## 4. Read from MongoDB, or pretend to

The **MongoDB** source (Sources group) reads a collection with the same connection fields
plus `database`, `collection`, an optional aggregation `pipeline` (JSON array of stages, run
on the server), `on_malformed` (what to do with a document that does not fit the declared
schema: `null` the field, `drop` the document, `fail`) and `read_options`.

Tick **Use Dummy Data** and the node never connects: paste a JSON array of up to 100
documents in *Dummy Data* and they flow downstream as if read from a collection (nested
documents become structs, so `customer.tier` works in SQL). This is how to draw and test a
pipeline before the real collection exists. [`examples/mongodb_to_parquet.yaml`](../examples/mongodb_to_parquet.yaml)
runs entirely on dummy data: `engine/.venv/bin/tributary run examples/mongodb_to_parquet.yaml`.

## 5. Run the tests

The MongoDB tests in the engine run against whatever server `TRIBUTARY_TEST_MONGODB_URI` points
at (default `mongodb://localhost:27017`, i.e. the compose service) and skip when none is
reachable or the connector jars cannot be fetched:

```bash
docker compose -f deploy/docker-compose.yml up -d mongodb
cd engine
.venv/bin/pytest tests/test_mongodb.py tests/test_mongodb_source.py tests/test_mongodb_resilience.py -v
```

| File | What it covers |
|---|---|
| `test_mongodb.py` | sink options and form hints, credentials never in logs, writer options per mode, design-time checks, real writes for append/overwrite/upsert (replace and update), `_id` columns, nested data, streaming micro-batches |
| `test_mongodb_source.py` | source options, dummy data (limits, JSON parsing, schema typing, analysis), real reads with an aggregation pipeline |
| `test_mongodb_resilience.py` | **idempotency** (a micro-batch replayed after a crash, a partial write retried, batch re-runs), **schema** (new fields, type drift flagged at design time, `on_malformed` policies), **resiliency** (writes that wait for a server that comes back, retry budgets, a running stream surviving an outage, through a cut-able TCP proxy) |

They use their own database, `tributary_tests`, with a fresh collection per test. The whole
engine suite is `.venv/bin/pytest` (Iceberg and Delta tests fetch their jars the same way).

## 6. Behaviour worth knowing

- **Idempotency.** Appends get a deterministic `_id` (sha1 of the pipeline, the micro-batch or
  run, and the row) and are written as upserts, so at-least-once delivery never duplicates:
  a micro-batch replayed after a crash, or a write retried after a network drop, rewrites the
  same documents. Turn `deterministic_ids` off to get MongoDB ObjectIds (and duplicates on
  replay). To reprocess a *batch* pipeline over the same data without copies, use `upsert`.
- **Retries.** Transient errors (server unreachable, connection reset, primary stepping down,
  retryable server codes) are retried with exponential backoff: `retries` (3) and
  `retry_backoff_seconds` (5, doubling). Only idempotent writes are retried. The driver's own
  settings still apply through the URI (`serverSelectionTimeoutMS`, `retryWrites`, ...).
- **Schema.** Documents are schemaless: new columns simply appear. While you draw, the sink
  samples the existing collection and flags a column whose type changed (int → string, ...),
  and a changed *merge key* in particular, because upserts on it would stop matching.
- **Field names.** MongoDB refuses dots and a leading `$`; the sink flags such columns so you
  can rename them in the SQL.

## 7. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `unresolved dependency: org.mongodb.spark#mongo-spark-connector_2.12;10.5.0` | Maven Central not reachable from the machine. Fetch once with network, or pick another 10.x release with `TRIBUTARY_MONGODB_CONNECTOR_VERSION`. |
| `Timed out after 30000 ms while waiting for a server` | Wrong host/port, or `localhost` used from inside a container (use `mongodb://mongodb:27017`). Design-time checks use a 3 s timeout; add `serverSelectionTimeoutMS=...` to the URI to change the runtime one. |
| `Command failed with error 18 (AuthenticationFailed)` | Check `username`/`password` and `authSource` in the URI (`?authSource=admin` for users created in `admin`). |
| Upsert adds copies instead of updating | The merge key's type differs from the documents already there (e.g. `"1"` vs `1`); the sink flags this at design time. Cast in the SQL, or index and migrate the collection. |
| Reads fail with `Partitioner calling collStats command failed` | The server does not support `$collStats` (some MongoDB-compatible services). Set `read_options: { partitioner: com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner }`. |
| `Java gateway process exited` in previews | The shared preview session could not start; look at the backend log. Java 8/11/17 is required for PySpark 3.5. |
