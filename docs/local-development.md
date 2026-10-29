# Running pipelines locally

Every pipeline can have an environment named **`local`** for working on your own machine. New pipelines get one
automatically, and the editor opens in it. For an existing pipeline, use **Add local environment** in *Pipeline
settings* (click an empty spot on the canvas), or add `local: {}` under `environments:` in the YAML.

In `local` you have two ways to give a pipeline the systems it talks to, and you can mix them node by node:

| | What you need | Good for |
|---|---|---|
| **A. Real services in Docker** | Docker Desktop (or Docker Engine + Compose v2) | testing against real MongoDB, Kafka, Elasticsearch, ... |
| **B. Dummy data** | nothing | drawing and previewing a pipeline before any system is reachable |

---

## Option A: real services in Docker

`scripts/local_services.py` starts only the services behind the connectors you name, and prints the connection
settings to use for them. It needs nothing but `python3` and Docker.

```bash
python3 scripts/local_services.py list                     # connectors and the services behind them
python3 scripts/local_services.py up mongodb kafka         # start just these
python3 scripts/local_services.py up all                   # start everything
engine/.venv/bin/python scripts/local_services.py up --pipeline examples/csv_to_mongodb.yaml   # whatever the pipeline uses
python3 scripts/local_services.py status                   # which ports answer
python3 scripts/local_services.py down                     # stop what the script started (--remove to delete the containers)
```

`--pipeline` reads the pipeline's source, transform and sink types (and its quality blocks' results and quarantine
sinks). YAML files need PyYAML, which the engine's virtualenv has. Add `--dry-run` to any command to see the
`docker compose` command without running it.

### Connectors and their services

| Connector (`type:`) | Service | Address on your machine | Credentials |
|---|---|---|---|
| `kafka` | `redpanda` | Kafka `localhost:19092`, Schema Registry `http://localhost:18081` | none |
| `mongodb` | `mongodb` | `mongodb://localhost:27017` | none |
| `elasticsearch` | `elasticsearch` | `http://localhost:9200` | none (security off) |
| `rest_api`, `rest_api_enrichment` | `mock-api` | `http://localhost:8765` | bearer token `local-token` |
| `jdbc`, `postgres` (quality results) | `postgres` | `jdbc:postgresql://localhost:5432/tributary` | `tributary` / `tributary` |
| `prometheus` (quality results) | `pushgateway` | `http://localhost:9091` | none |
| `s3` (files, Delta, Hudi, Iceberg on S3) | `minio` | `http://localhost:9000` (console `:9001`) | `tributary` / `tributary123` |
| `file`, `delta`, `hudi`, `iceberg`, `console`, `sql`, `deequ` | none | local disk / runs in Spark | — |

Aliases work too: `es`, `mongo`, `rest`, `redpanda`, `minio`, `postgresql`.

The services run in the Docker Compose project `tributary` (from `deploy/docker-compose.yml`). If a service's port
already answers, for example because another checkout's stack or a server of your own is running, the script
reports it as *already answering, reusing it* and does not start a second copy. `down` only stops the `tributary`
project's containers, never a reused server.

### Sample data

```bash
python3 scripts/local_services.py seed              # kafka, mongodb and elasticsearch
python3 scripts/local_services.py seed mongodb      # just one
```

It loads `examples/data/orders.csv`: 8 orders, a few of them deliberately bad so the quality examples have something
to catch. They go to the `orders` topic in Kafka, the `tributary.orders` collection in MongoDB, and the `orders`
index in Elasticsearch.

### Wiring services into a pipeline

`up` (and `env`) print the connection settings for the connectors you named, ready to paste into the pipeline:

```bash
python3 scripts/local_services.py env mongodb kafka
```

```yaml
environments:
  local:
    connections:
      local_mongodb:
        type: "mongodb"
        connection_uri: "mongodb://localhost:27017"
      local_kafka:
        type: "kafka"
        brokers: "localhost:19092"
```

The nodes then name the connection and keep only what is specific to them:

```yaml
sources:
  - id: orders
    type: kafka
    connection: local_kafka
    options: { topic: orders, starting_offsets: earliest }
sinks:
  - id: orders_out
    type: mongodb
    connection: local_mongodb
    input: orders_clean
    options: { database: tributary, collection: orders, mode: upsert, merge_keys: [order_id] }
```

**In the UI**, you can instead create the connection on the **Connections** page: pick the type, add an
environment named `local` with the settings above, and select the connection in the node's *Connection* field.
Other environments (`dev`, `prod`) of the same connection keep their own settings, so the pipeline needs no change
when you switch environment.

Then run it:

```bash
engine/.venv/bin/tributary preview my_pipeline.yaml -e local --node orders_out
engine/.venv/bin/tributary run my_pipeline.yaml -e local
```

---

## Option B: dummy data

In the `local` environment, any source can stand in with records you upload, and any sink can be a dummy that
writes nothing. Neither needs its connection or options, so a pipeline can be drawn and previewed end to end before
anything is reachable.

### In the editor

1. Pick **local** in the toolbar's *Env* selector.
2. Select a source and turn on **Use dummy data**. Upload a `.jsonl` file (one JSON object per line, up to **1,000**
   records; a `.json` file holding an array works too), or paste or edit the lines in the box below. The count
   updates as you type, and a bad line is pointed out by number.
3. Columns are inferred from the records the way Spark reads JSON: whole numbers become `bigint`, nested objects
   become structs, and timestamps stay strings. To set exact types, pick a schema in the node's *Schema* section
   (DDL such as `order_id INT, amount DECIMAL(10,2), order_ts TIMESTAMP`).
4. Select a sink and turn on **Use dummy data**. It now writes nothing; **Test** (or the drawer's *Preview*) shows
   the rows it would have received.
5. Dummy nodes carry an amber **DUMMY** tag on the canvas, and their *Connection* and *Options* sections are hidden
   while the switch is on. Switching it off brings them back; the uploaded records are kept, ready for next time.

In any other environment the switch isn't offered. A node with dummy data turned on shows a note and uses its real
settings there.

### In YAML

```yaml
environments:
  local: {}
  dev:
    connections: { mongo: { type: mongodb, connection_uri: "mongodb://mongo.dev:27017" } }

sources:
  - id: orders
    type: mongodb
    connection: mongo
    options: { database: shop, collection: orders }
    dummy:
      enabled: true
      data: |
        {"order_id": 1, "amount": 25.5, "country": "IN"}
        {"order_id": 2, "amount": 9.0, "country": "US", "meta": {"channel": "web"}}
sinks:
  - id: orders_out
    type: elasticsearch
    input: orders
    options: { index: orders }
    dummy: { enabled: true }
```

```bash
engine/.venv/bin/tributary preview pipeline.yaml -e local --node orders_out   # the rows the sink would get
engine/.venv/bin/tributary run pipeline.yaml -e local                         # dummy sinks log a row count, write nothing
engine/.venv/bin/tributary run pipeline.yaml -e dev                           # the real MongoDB and Elasticsearch
```

`data` also accepts a JSON array, or a YAML list of objects.

### How it behaves

- **Local only.** When the selected environment is `local`, a node with `dummy.enabled` becomes Tributary's built-in
  dummy source or sink. In every other environment the setting is ignored, with a warning, and the node needs its
  real options. So the same pipeline runs locally on dummy data and for real everywhere else.
- **Sources** hand their records downstream in previews, deep validation and runs. In a streaming pipeline they
  arrive as one micro-batch. A declared schema types them; without one they're inferred.
- **Sinks** write nothing. Preview shows their input; a run logs `dummy <type> sink: N rows not written`. Real sinks
  in the same pipeline still write as usual, so a dummy source can feed a real local MongoDB from Option A.
- **Limits:** 1,000 records per source; every line must be a JSON object.
- **Saving a version.** A version is checked in every environment the pipeline defines. Dummy nodes are the
  exception: if their real settings are still missing in another environment (say `dev`), the version is saved
  anyway, and the editor says which environments still need them. Running there fails until they're filled in;
  running in `local` works right away. Any other problem still blocks the save, with its environment shown
  (`[dev] orders: ...`).
- **Dummy data vs. sample data.** The *Sample data* upload on a source only replaces what Previews read, works in any
  environment, and isn't saved in the pipeline. Dummy data is part of the pipeline, works for runs too, and only
  in `local`.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `already answering on localhost:<port>, reusing it`, but it's not the server you expect | Something else holds the port; stop it, or point the connection at the server that's there |
| `docker: command not found` / cannot connect to the Docker daemon | Start Docker Desktop |
| `up` waits a long time on the first run | Images are being pulled; later starts take seconds |
| `<file> is YAML: run this script with the engine's python` | Use `engine/.venv/bin/python scripts/local_services.py ... --pipeline file.yaml`, or name the connectors instead |
| The *Use dummy data* switch is missing | Select the `local` environment in the toolbar (add it in *Pipeline settings* if the pipeline has none) |
| `dummy data is only used in the 'local' environment` warning | Expected outside `local`: the node runs with its real settings there |
| `sources[0].dummy.data: line 3 is not JSON` | Fix that line; each line must hold one complete JSON object |
