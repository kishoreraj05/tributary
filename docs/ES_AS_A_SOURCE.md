# Testing Elasticsearch as a source, locally

Tributary can read an Elasticsearch index as a pipeline source and write the result to
MongoDB. This guide walks through testing that on your machine, with the same Docker Compose
stack you already use, in three ways:

1. [With a local Elasticsearch and MongoDB in Docker, through the UI](#2-test-through-the-ui-with-docker-elasticsearch-and-mongodb) (the main path)
2. [The same pipeline from the CLI](#3-the-same-test-from-the-cli)
3. [Without any Elasticsearch at all, using dummy data](#4-test-without-elasticsearch-dummy-data)

Everything below was run end to end against Elasticsearch 8.13 and MongoDB; the numbers in
the "expected result" boxes are what you should see.

## 0. What you need

- Docker with Compose, Java 8/11/17 (21 also works with Spark 3.5), Python 3.10–3.12, Node 18+.
- Network access to Maven Central the first time: the Spark session downloads the two
  connectors (`org.elasticsearch:elasticsearch-spark-30_2.12:8.13.2` and
  `org.mongodb.spark:mongo-spark-connector_2.12:10.5.0`, ~10 MB) into `~/.ivy2` on first use.
- About 1.5 GB of free RAM for the extra containers (Elasticsearch runs with a 512 MB heap).

Ports used: Postgres 5432, Elasticsearch **9200**, MongoDB **27017**, backend 8000, UI 5173.

## 1. Start the stack

Your usual commands, with two more services on the `docker compose` line:

```bash
# from the repository root
docker compose -f deploy/docker-compose.yml up -d postgres elasticsearch mongodb

(cd engine  && python3.11 -m venv .venv && .venv/bin/pip install -e .)
(cd backend && python3.11 -m venv .venv && .venv/bin/pip install -e ../engine -e . && .venv/bin/tributary-backend serve) &
(cd ui      && npm install && npm run dev)     # http://localhost:5173
```

> Already had the engine/backend installed before this change? Re-run the two `pip install -e`
> lines once: the new source and sink are registered as package entry points at install time.

Wait for Elasticsearch (it takes 20–40 s), then load the sample orders into an index called
`orders`:

```bash
curl -s localhost:9200/_cluster/health      # "status":"green" or "yellow" means it is up
deploy/seed_elasticsearch.sh                # -> indexed 8 documents into orders
```

The seed script indexes `examples/data/orders.csv` (8 orders: one without `order_id`, one
with a negative amount, one with a 3-letter country, all on purpose so a quality gate has
something to catch). Check it yourself:

```bash
curl -s 'localhost:9200/orders/_search?size=2&pretty'
```

Quick health check of the whole setup before opening the UI:

```bash
curl -s localhost:8000/api/v1/components | grep -o '"type":"elasticsearch"\|"type":"mongodb"' | sort -u
# "type":"elasticsearch"
# "type":"mongodb"
docker compose -f deploy/docker-compose.yml exec mongodb mongosh --quiet --eval 'db.version()'
```

## 2. Test through the UI, with Docker Elasticsearch and MongoDB

Open http://localhost:5173. There are two ways to get the pipeline: import the ready-made
example (fastest), or draw it yourself (what a user does).

### 2a. Import the example

1. **Pipelines → Import**, name it, pick (or paste) `examples/elasticsearch_to_mongodb.yaml`,
   **Import**.
2. The canvas shows `orders_src` (Elasticsearch) → `orders_clean` (SQL) → a quality block →
   `orders_out` / `revenue_out` (MongoDB) + a console sink. The Elasticsearch and MongoDB
   settings are inline connections in the spec (`es_main`, `mongo_main`, both pointing at
   localhost), so nothing else is needed.
3. Click **`orders_src`** and press **Test**: the 8 documents from the index appear, typed
   from the index mapping (`order_ts` as a timestamp, `_metadata` with the document id).
4. Click **`orders_clean`** → **Test**: the cleaned rows (`doc_id`, upper-cased country).
5. Click **`orders_dq`** → **Test**: the 3 failing rows first, each tagged with the check it fails.
6. Press **Run** → **Save as v1 & run**. The *Runs* drawer opens; the run ends in
   `succeeded` with `sinks_written: orders_out, revenue_out, revenue_console` and 3 failed checks.
7. Look at MongoDB:

```bash
docker compose -f deploy/docker-compose.yml exec mongodb mongosh --quiet tributary --eval '
  print("orders:", db.orders.countDocuments(), " quarantine:", db.orders_quarantine.countDocuments(), " revenue:", db.revenue_by_country.countDocuments());
  db.orders.find({}, {_id: 0}).sort({order_id: 1}).forEach(d => print(JSON.stringify(d)))'
```

> **Expected:** `orders: 5  quarantine: 3  revenue: 3`. The 5 clean orders (1, 2, 4, 7, 8),
> the 3 bad ones in `orders_quarantine` with a `_dq_failed_checks` array, and one revenue
> document per country. Run it again: the counts stay the same (`orders_out` upserts on
> `doc_id`, the other two collections are overwritten).

### 2b. Draw it yourself

This is the flow to exercise when testing the feature as a user.

**Connections page** (top bar → *Connections*), **New connection**, twice:

| Name         | Type            | Field (env `dev`) | Value                       |
|--------------|-----------------|-------------------|-----------------------------|
| `es_main`    | `elasticsearch` | `nodes`           | `localhost:9200`            |
| `mongo_main` | `mongodb`       | `uri`             | `mongodb://localhost:27017` |

(For a secured cluster add `user` and `password` fields; `password` is stored encrypted. For
Elasticsearch 8 with security on, also `ssl` = `true` and `ssl_allow_self_signed` = `true`.)

**Pipelines page → New pipeline**, name it, **Create and open**. Then on the canvas:

1. From the palette on the left click **Elasticsearch** (Sources), **SQL** (Transforms) and
   **MongoDB** (Sinks). Press the *Tidy layout* button (bottom-left controls) to line them up.
2. Wire them: drag from the right-hand handle of the Elasticsearch node to the left handle of
   the SQL node, then from SQL to MongoDB.
3. Click the **Elasticsearch node**. In the panel: *Connection* = `es_main` (the *Nodes*
   field now says "from connection es_main"), *Index* = `orders`. Open *More options* and tick
   *Include Metadata* (gives you `_metadata['_id']`, the document id). Optionally rename the
   node id to `orders_src`.
4. Click the **SQL node**. The *Inputs* chip lists the columns found in the index mapping
   (`amount, country, customer_id, order_id, order_ts, _metadata`). Enter, using your source
   node's id as the table name:

   ```sql
   SELECT _metadata['_id'] AS doc_id, order_id, customer_id, amount, upper(country) AS country, order_ts
   FROM orders_src
   WHERE order_id IS NOT NULL AND amount >= 0
   ```

5. Click the **MongoDB node**: *Connection* = `mongo_main`, *Database* = `tributary`,
   *Collection* = `orders_from_ui`, *Mode* = `upsert`, *Id Fields* = `doc_id` (the field
   appears once the mode is `upsert`, and is checked against the SQL's columns as you type).
6. The status in the top bar turns **Valid** (it validates column names with Spark as you
   edit). Press **Test** on the SQL node: live rows from Elasticsearch. **Test** on the MongoDB
   node shows the same rows as a dry run (`dry run → tributary.orders_from_ui @ … (upsert on doc_id)`),
   nothing is written yet.
7. **Run** → **Save as v1 & run**, wait for `succeeded` in the *Runs* drawer, then:

```bash
docker compose -f deploy/docker-compose.yml exec mongodb mongosh --quiet tributary --eval 'db.orders_from_ui.find({}, {_id: 0}).sort({order_id: 1}).forEach(d => print(JSON.stringify(d)))'
```

> **Expected:** 6 documents (orders 1, 2, 4, 6, 7, 8; the order without an id and the
> negative one were filtered by the `WHERE`), e.g.
> `{"doc_id":"1","order_id":1,"customer_id":100,"amount":25.5,"country":"IN","order_ts":"2026-10-01T10:00:00.000Z"}`.

The same clicks are scripted in `scripts/e2e-elasticsearch-mongodb.mjs` (headless Chrome over
the DevTools protocol, no npm dependencies); with the stack up,
`node scripts/e2e-elasticsearch-mongodb.mjs` replays them and leaves a screenshot of every
step in `$TMPDIR/tributary-ui-e2e`.

### Things to try on the source node

All of these are options on the Elasticsearch node (most under *More options*):

- **Query**: `?q=country:us` (URI query) or `{"query": {"range": {"amount": {"gte": 10}}}}`
  (Query DSL) – only matching documents are read.
- **Fields**: `order_id, amount` – read only those `_source` fields.
- **Array Fields**: list every field that holds arrays (e.g. `tags`). Mappings do not say
  which fields are arrays, so an unlisted array field fails the read with
  `JListWrapper is not a valid external type`.
- **Schema** (the *Schema* section, kind `ddl`), e.g.
  `order_id INT, amount DECIMAL(10,2), country STRING, order_ts TIMESTAMP`: the columns are
  selected and cast to these types, and the editor knows them without asking the cluster.
- **Wan Only** is on by default so a cluster in Docker or the cloud works (Spark only talks to
  the addresses you list, not the ones the cluster publishes). Turn it off when Spark runs
  inside the cluster's network.

## 3. The same test from the CLI

No UI or backend needed, just the engine venv and the two containers:

```bash
engine/.venv/bin/tributary validate examples/elasticsearch_to_mongodb.yaml -e dev
engine/.venv/bin/tributary plan     examples/elasticsearch_to_mongodb.yaml -e dev
engine/.venv/bin/tributary preview  examples/elasticsearch_to_mongodb.yaml -e dev --node orders_clean --limit 5
engine/.venv/bin/tributary run      examples/elasticsearch_to_mongodb.yaml -e dev
```

> **Expected** from `run`: `✓ run …: wrote orders_out, revenue_out, revenue_console; 3 checks, 3 failed`
> and the same MongoDB counts as above (`orders: 5  quarantine: 3  revenue: 3`).

## 4. Test without Elasticsearch (dummy data)

For drawing and testing a pipeline before the index exists, the Elasticsearch node has a
**Use Dummy Data** switch. When it is on, the node emits the documents you paste into
**Dummy Data** (a JSON array, 100 documents at most) in previews *and* runs, never contacts
a cluster, and does not even need the connector jar; the *Nodes*/*Index* fields stop being
required. You can keep Elasticsearch stopped for this:

```bash
docker compose -f deploy/docker-compose.yml stop elasticsearch
```

In the UI: add an Elasticsearch node, tick **Use Dummy Data**, paste for example

```json
[
  {"_id": "d1", "order_id": 1, "customer_id": 100, "amount": 25.5, "country": "in"},
  {"_id": "d2", "order_id": 2, "customer_id": 101, "amount": 99.99, "country": "us"},
  {"order_id": 3, "customer_id": 102, "amount": -5, "country": "de"}
]
```

tick *Include Metadata* if you want `_metadata['_id']` (a document's `"_id"` becomes it),
wire a SQL node and a sink, and **Test** / **Run** as before. Types are inferred from the
values like an index mapping would (numbers, strings, booleans, nested objects, arrays);
add a *Schema* to the node to fix them (dates stay strings otherwise). Switch it off again
and pick a connection to read the real index; the pasted documents are kept but ignored.

From the CLI, `examples/elasticsearch_dummy_data.yaml` is the same thing with no services:

```bash
engine/.venv/bin/tributary run examples/elasticsearch_dummy_data.yaml
```

## 5. Automated tests

```bash
(cd engine  && .venv/bin/pip install -e ".[dev]" && .venv/bin/pytest tests/test_elasticsearch.py tests/test_mongodb.py)
(cd backend && .venv/bin/pip install -e ".[dev]" && .venv/bin/pytest tests/test_elasticsearch_mongodb.py)
```

The tests that talk to a server use `localhost:9200` and `mongodb://localhost:27017` (override
with `TRIBUTARY_TEST_ES_NODES` / `TRIBUTARY_TEST_MONGO_URI`) and skip when nothing is
listening; the dummy-data and option tests always run. The backend tests also need the
docker-compose Postgres.

## 6. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| The palette has no Elasticsearch/MongoDB tile | The engine was installed before this change: re-run `pip install -e` for `engine` and `backend`, restart `tributary-backend serve`. |
| First preview/run is slow | Maven download of the connectors (once). Check `~/.ivy2/jars` afterwards. |
| `unresolved dependency: org.apache.spark#spark-sql-kafka-0-10_2.12;3.5.x: not found` at session start | Your `pyspark` is newer than the Spark artifacts on Maven Central (seen with pyspark 3.5.9 vs 3.5.7 on Central). `pip install "pyspark==3.5.7"` in both venvs. |
| `Connection refused` / `Cannot detect ES version` from the source | Elasticsearch not up yet or wrong port: `curl localhost:9200`. With security on, set `user`/`password` (+ `ssl`). Keep *Wan Only* on for Docker. |
| `JListWrapper is not a valid external type for schema of string` | A field holds arrays: add it to *Array Fields* on the source node. |
| `_metadata` not found in SQL | Tick *Include Metadata* on the source node (under *More options*). |
| Elasticsearch container exits right away | Usually memory: it needs ~1 GB; on Linux also `sysctl -w vm.max_map_count=262144`. |
| `id_fields not in the data` on the MongoDB node | The upsert key must be a column of the incoming SQL; the editor shows this under *Id Fields* while you type. |
| Run log says `uses dummy data … no cluster is contacted` but you expected real data | *Use Dummy Data* is still on for that node. |

## 7. Clean up

```bash
docker compose -f deploy/docker-compose.yml exec mongodb mongosh --quiet tributary --eval 'db.dropDatabase()'
curl -s -XDELETE localhost:9200/orders
docker compose -f deploy/docker-compose.yml down        # add -v to drop the volumes too
```
