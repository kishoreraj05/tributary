# Elasticsearch as a sink: using and testing it locally

The `elasticsearch` sink writes one JSON document per row to an index, an alias or a data
stream, through the official Spark connector (ES-Hadoop, `elasticsearch-spark-30`). It works
with Elasticsearch 7.x and 8.x clusters. This page is the local walkthrough: start a cluster,
draw or import a pipeline, run it, check the documents, run the test suites.

```
 CSV / Kafka ─▶ SQL ─▶ quality gate ─▶ elasticsearch sink ─▶ bulk API ─▶ index / write alias
```

## What is in the box

| File | Role |
|---|---|
| `engine/src/tributary/plugins/sinks/elasticsearch.py` | The sink plugin (options, design-time checks, write) |
| `engine/pyproject.toml` | Registers it: `[project.entry-points."tributary.sinks"] elasticsearch = ...` |
| `deploy/elasticsearch/Dockerfile` | A single-node cluster without security for local use |
| `deploy/docker-compose.yml` | The `elasticsearch` service built from that Dockerfile (opt-in) |
| `examples/csv_to_elasticsearch.yaml` | A batch pipeline: CSV → SQL → quality gate → two indices |
| `engine/tests/test_elasticsearch.py` | Options, session settings, writes, preview, streaming replay |
| `engine/tests/test_elasticsearch_ops.py` | Idempotency, mapping conflicts, 429s, rollover |
| `engine/tests/es_proxy.py` | A tampering proxy used by the 429 tests |
| `backend/tests/test_elasticsearch.py` | The same through the API: catalog, connection, validation, preview, runs |
| `ui/src/components/icons.tsx` | The sink's and the connection type's icons (the form itself is generated) |

The connector jar (about 2 MB) is fetched from Maven Central by Spark on the first run
(`spark.jars.packages`); nothing to install by hand.

## 1. Start a cluster

**With Docker Compose** (from the repository root):

```bash
docker compose -f deploy/docker-compose.yml up -d elasticsearch     # builds deploy/elasticsearch/Dockerfile
curl localhost:9200                                                 # {"name": ..., "version": {"number": "8.13.4", ...}}
```

**With the Dockerfile alone:**

```bash
docker build -t tributary/elasticsearch deploy/elasticsearch
docker run -d --name tributary-es -p 9200:9200 tributary/elasticsearch
```

**Without Docker** (a JDK 17+ on the machine): the `integ-test-zip` distribution on Maven
Central is a full server without the bundled JDK. Take a release up to 8.6.x, the last ones
whose `GET /` (which the connector calls first) is built in rather than a module the zip
leaves out:

```bash
curl -LO https://repo1.maven.org/maven2/org/elasticsearch/distribution/integ-test-zip/elasticsearch/8.6.2/elasticsearch-8.6.2.zip
unzip -q elasticsearch-8.6.2.zip && cd elasticsearch-8.6.2
printf 'discovery.type: single-node\nxpack.security.enabled: false\nnetwork.host: 127.0.0.1\n' >> config/elasticsearch.yml
ES_JAVA_HOME=$JAVA_HOME ES_JAVA_OPTS="-Xms512m -Xmx1g" bin/elasticsearch
```

All three give `http://localhost:9200`, no TLS, no credentials, which is what the example and
the tests expect. A secured cluster only needs the connection settings below (`ssl`,
`user`/`password` or `api_key`).

## 2. Describe the cluster once: a connection

In the UI, **Connections → New connection**, type `elasticsearch`, environment `dev`:

| Field | Value | Notes |
|---|---|---|
| `nodes` | `localhost:9200` | Comma separated `host[:port]`; Elastic Cloud: `my-deployment.es.region.aws.found.io:9243` |
| `port` | `9200` | Default for nodes without a port |
| `ssl` | `false` | `true` for HTTPS; `ssl_allow_self_signed: true` for the Docker image's own certificate |
| `user` / `password` | | Basic auth. `password` is detected as a secret: encrypted, never shown again |
| `api_key` | | Sent instead of user/password (the base64 `encoded` value) |
| `wan_only` | `true` | Talk to the listed nodes only. Keep it on for Docker, cloud and proxies |

The same block inline in a YAML pipeline (what the example does):

```yaml
environments:
  dev:
    connections:
      search: { type: elasticsearch, nodes: localhost:9200 }
```

## 3. The sink's options

Everything below `index` is optional; the form stars what is mandatory.

| Option | Default | What it does |
|---|---|---|
| `index` ★ | | Index, alias or data stream; lowercase. `logs-{country}` or `events-{ts\|yyyy.MM.dd}` routes rows by a column (values must be lowercase too) |
| `mode` | `append` | `append`: index every row (same id → replaced). `overwrite`: delete the index's documents first (fixed index names only). `upsert`: update the columns given for documents that exist, add the others |
| `id_column` | | Column used as the document `_id`. With it, re-runs, retried tasks and replayed micro-batches replace documents instead of adding copies. Mandatory for `upsert` |
| `timestamp_format` | `iso` | TIMESTAMP/DATE columns as ISO-8601 text (a new index maps them as `date`) or `epoch_millis` numbers |
| `auto_create_index` | `true` | Let Elasticsearch create the index with a dynamic mapping. Turn off for rollover series and for indices that must come from a template |
| `batch_size_entries` | `1000` | Documents per bulk request, per task |
| `retry_count` / `retry_wait` | `3` / `10s` | Documents a shard rejects with 429 are resent, only those, after the wait; `-1` retries until they get in |
| `rejected_documents` | `fail` | `skip`: log documents the mapping refuses (`tributary.elasticsearch.rejected` in the run log) and carry on |
| `write_options` | | Any other connector setting, e.g. `es.mapping.routing`, `es.batch.size.bytes`, `es.net.proxy.http.host`; the `es.` prefix may be omitted |

## 4. Run the example

**From the CLI:**

```bash
(cd engine && python3.11 -m venv .venv && .venv/bin/pip install -e ".[dev]")
engine/.venv/bin/tributary validate examples/csv_to_elasticsearch.yaml --deep   # asks the cluster about the targets
engine/.venv/bin/tributary preview examples/csv_to_elasticsearch.yaml --node orders_search
engine/.venv/bin/tributary run examples/csv_to_elasticsearch.yaml
engine/.venv/bin/tributary run examples/csv_to_elasticsearch.yaml             # again: same 6 documents, no copies
curl 'localhost:9200/_cat/indices/orders,revenue_by_country?v&h=index,docs.count'
curl 'localhost:9200/orders/_search?pretty&size=1'
curl 'localhost:9200/orders/_mapping?pretty'                                  # order_ts is a date field
```

**From the UI** (the usual quickstart, plus the cluster):

```bash
docker compose -f deploy/docker-compose.yml up -d postgres elasticsearch
(cd engine && python3.11 -m venv .venv && .venv/bin/pip install -e .)
(cd backend && python3.11 -m venv .venv && .venv/bin/pip install -e ../engine -e . && .venv/bin/tributary-backend serve) &
(cd ui && npm install && npm run dev)       # http://localhost:5173
```

1. **Connections → New connection**: name `search`, type `elasticsearch`, field `nodes` = `localhost:9200`. Save.
2. **Pipelines → Import**, pick `examples/csv_to_elasticsearch.yaml`. (Its inline `search`
   connection is used as is; delete that block from the YAML first if you want the stored one.)
   Or draw it: drag a *file* source, a *sql* transform and an *Elasticsearch* sink from the
   palette, connect them, choose the connection on the sink and fill `index` and `id_column`.
3. Select a sink node: its form shows `index` ★ (and `id_column` ★ once `mode` is `upsert`);
   `nodes` shows as *from search*. Validation runs as you type and reads the cluster's
   mapping: a column that cannot fit an existing field, a new column on a `strict` mapping
   or an alias without a write index are reported under the node before anything runs.
4. **Test** on the sink shows the rows it would write without writing them.
5. **Publish** a version, **Run** with environment `dev`, and watch the log
   (`writing sink orders_search: elasticsearch -> orders at localhost:9200 (append)`).
   Run it twice: `orders` stays at 6 documents, `revenue_by_country` is rebuilt.

## 5. Run the tests

The Elasticsearch tests skip themselves when no cluster answers at `TRIBUTARY_TEST_ES_URL`
(default `http://localhost:9200`) or when the connector jar cannot be fetched from Maven
Central (it is cached under `~/.cache/tributary/jars` after the first time). They create
indices named `tributary-test-*` / `tributary-backend-*` and remove them afterwards.

```bash
# engine: 40 tests, about a minute with a local cluster
(cd engine && .venv/bin/pytest tests/test_elasticsearch.py tests/test_elasticsearch_ops.py -rs)

# backend: needs Postgres too (docker compose ... up -d postgres); 6 tests, runs spawn Spark workers
(cd backend && .venv/bin/pip install -e ".[dev]" && .venv/bin/pytest tests/test_elasticsearch.py -rs)

# ui: the generated form, including which fields carry the red star
(cd ui && npx vitest run)
```

What they cover, beyond the basics (modes, patterns, dates, preview, streaming):

| Scenario | Tests | Outcome |
|---|---|---|
| Idempotency | re-runs, a replayed micro-batch, a write that fails half-way | With `id_column` documents are replaced (`_version` grows, count does not); without it every attempt adds copies. Null ids get generated ids: the one leak |
| Mapping conflicts | a value that does not fit, `dynamic: strict`, `dynamic: false`, detected dates | Refused per document at run time; the same conflicts reported in the form beforehand |
| Bulk rejections (429) | `es_proxy.py` refuses items, whole requests, or never recovers | Rejected items are resent in place and indexed once; a request-level 429 fails the task (Spark retries it; safe with an id); exhausted retries fail with the rest of the bulk already in |
| Rollover | write alias, `_rollover` mid-write, replay after a rollover | Nothing lost or duplicated across a rollover during a 9,000-row write; a document written again after a rollover goes to the new index (rollover is for append-only streams) |

`_rollover` is what ILM calls; ILM policies and data streams themselves are not part of the
minimal distribution used without Docker and have not been run.

## 6. Things to know

- **Index names are lowercase**, and so must be the values of a column used in an index
  pattern (`lower(country)` in the SQL).
- **Decimals and `timestamp_ntz`** cannot be serialised by the connector: cast them upstream
  (the form tells you).
- **Connection errors** (`Cannot detect ES version ... es.nodes.wan.only`) mean the address in
  `nodes` is not reachable from Spark, or a proxy in between strips the
  `X-elastic-product` header the connector checks.
- **Rollover / ILM**: write to the alias, create the first index with `is_write_index: true`
  yourself and set `auto_create_index: false`; otherwise the first write creates a plain index
  by the alias' name that can never roll over. Data streams additionally need
  `write_options: { es.write.operation: create }` and a `@timestamp` column.
- **Java**: the connector runs on Java 8+ like Spark 3.5; the Elasticsearch server itself
  needs Java 17+ (bundled in the Docker image).
- **Another connector version**: `TRIBUTARY_ELASTICSEARCH_VERSION=8.x.y` before starting the
  backend or the CLI.
- **Stopping the cluster**: `docker compose -f deploy/docker-compose.yml down` (add `-v` to
  drop the data), or `docker rm -f tributary-es`.
