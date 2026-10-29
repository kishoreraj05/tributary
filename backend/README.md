# tributary-backend

FastAPI service between the UI and the engine. It stores pipelines (a mutable draft plus immutable versions) and per-environment connections with encrypted secrets. It answers design-time requests (validate, plan, preview) on a shared Spark session, and runs pipelines in isolated worker processes.

## Run it

```bash
docker compose -f ../deploy/docker-compose.yml up -d postgres
python3.11 -m venv .venv && .venv/bin/pip install -e ../engine -e ".[dev]"
.venv/bin/tributary-backend serve           # applies migrations, then serves on :8000
```

OpenAPI docs are at http://localhost:8000/docs.

Settings come from `TRIBUTARY_*` environment variables (or `.env`):

| Variable | Default | Purpose |
|---|---|---|
| `TRIBUTARY_DATABASE_URL` | `postgresql+psycopg://tributary:tributary@localhost:5432/tributary` | Metadata DB |
| `TRIBUTARY_SECRET_KEY` | generated into `data_dir/secret.key` | Fernet key for connection secrets. **Set it in production.** |
| `TRIBUTARY_DATA_DIR` | `.tributary-data` | Run logs, uploaded samples |
| `TRIBUTARY_SPARK_MASTER` | `local[*]` | Preview session master |
| `TRIBUTARY_SPARK_REMOTE` | unset | Spark Connect URL for previews (needs `pyspark[connect]`; untested) |
| `TRIBUTARY_CORS_ORIGINS` | `["http://localhost:5173"]` | UI origins |
| `TRIBUTARY_UI_DIR` | unset | A built UI (`ui/dist`) to serve at `/` next to the API, as the Docker image does (`deploy/Dockerfile`) |

## API (`/api/v1`)

| Area | Endpoints |
|---|---|
| Catalog | `GET /components`: spec JSON Schema plus every plugin's options schema (drives the UI palette and forms) |
| Pipelines | `GET/POST /pipelines`, `GET/PUT/DELETE /pipelines/{id}` (the draft is saved unvalidated), `POST /pipelines/import`, `GET /pipelines/{id}/export?version=&format=yaml\|json` |
| Versions | `POST /pipelines/{id}/versions` validates the draft for every env it defines, then freezes it. `GET .../versions`, `GET .../versions/{v}`, `POST .../versions/{v}/restore` |
| Connections | `GET/POST /connections`, `GET/PUT/DELETE /connections/{name}`. See "Connections" below. |
| Design | `POST /validate` (`deep` adds per-node schemas/column errors), `POST /plan`, `POST /preview` (compiles only the node and its upstream, so unfinished nodes elsewhere do not block a test), `POST /samples` (upload a CSV/JSON/Parquet sample to stand in for a source) |
| Runs | `POST /pipelines/{id}/runs {env, version?}`, `GET /runs`, `GET /runs/{id}`, `POST /runs/{id}/cancel`, `GET /runs/{id}/logs`, `GET /runs/{id}/quality`, `WS /runs/{id}/events` |
| Quality | `GET /quality?pipeline_id=&check_id=&passed=`: DQ history across runs |

**Validate, plan and preview take the spec in the request body**, so the canvas can call them on unsaved changes. Validation always returns 200, with `issues[]` that each carry the `node_id` they belong to, so the UI can mark the node.

## Connections

- Settings are stored per environment (`{env: {field: value}}`). Sources, sinks and the transforms that declare it (the REST enrichment takes a `rest_api` connection) copy them into their options.
- Fields named like `password`, `token` or `secret` (or listed in `secret_fields`) are encrypted at rest and returned as `********`.
- Sending `********` back in an update keeps the stored value.

## How runs work

1. **Starting a run** validates the chosen version for the environment. The run record keeps a resolved spec with secrets masked.
2. **The worker:** `python -m tributary_backend.worker <run_id>` starts in its own process group, with its own Spark driver.
   - It decrypts secrets in memory only.
   - It adds a backend metrics sink to every quality block, and to every node that embeds results sinks in its options (the deequ node), so all DQ results land in `quality_results`.
   - It reports status directly to Postgres.
3. **Cancelling** sends SIGTERM to the whole process group (worker plus JVM), then SIGKILL after 10 s.
4. **If a worker dies without reporting**, its run is marked failed the next time anyone reads it, and also on API startup.

## Tests

```bash
.venv/bin/pytest     # needs the docker-compose Postgres; recreates a `tributary_test` database
```

## Known limits (v0.1)

- **Authentication:** none yet (planned: OIDC + role-based access). Don't expose this service publicly.
- **Where runs execute:** only on the API host, as local processes. A `spark-submit` / Kubernetes / Databricks runner is next.
- **Spark Connect:** wired up for previews, but not tested.
- **Connection test button:** not implemented yet. Use a source preview instead.
