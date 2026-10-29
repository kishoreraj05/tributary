# Testing REST as a source, locally

Yes, you can test the REST API source end to end on your machine with the stack you already
start with Docker. The only extra piece is an API to call. This guide ships one: a small mock
"university" REST API that logs every request it receives, so you can see exactly what the
pipeline does (how many calls, which pages, which headers). It runs either as a plain Python
script or as a service in the same `docker-compose.yml` (profile `rest`).

Everything below calls the API for real. Dummy data is not used here: the source's comes from
the `local` environment (toggle **Use Dummy Data** on the node and upload a JSON Lines file of
up to 1000 records; see [local development](local-development.md)), and the enrichment
transform keeps its own `useDummyData` / `dummyData` (see `engine/README.md`).

What you will test:

1. the **REST API source** (`rest_api`) from the CLI: JSON array, wrapped object, pagination,
   bearer token, declared vs inferred schema, the error paths;
2. the same in the **UI**: draw the node, watch validation, press *Test*, run it;
3. the **DataFrame-driven** variant (`rest_api_enrichment`): one call per row, and bulk calls;
4. the automated test suite.

---

## 0. Prerequisites

| Need | Why |
|---|---|
| Docker (compose v2) | Postgres for the backend; optionally the mock API |
| Java 8, 11 or 17 | PySpark 3.5 (`java -version`) |
| Python 3.10–3.12 | engine + backend (`python3.11 --version`) |
| Node 18+ | the UI |

All commands run from the repository root.

## 1. Start the mock REST API

Pick one of the two. Both listen on **http://localhost:8765** and require the bearer token
`local-token` (so that authentication is part of the test).

**A. With your Docker command** (the `rest` profile adds the mock to the usual stack):

```bash
docker compose -f deploy/docker-compose.yml --profile rest up -d postgres mock-api
docker compose -f deploy/docker-compose.yml logs -f mock-api      # keep this open: one line per request
```

**B. Without Docker** (standard library only):

```bash
python3 scripts/mock_rest_api.py --token local-token               # Ctrl-C to stop
```

Check it answers:

```bash
curl -s http://localhost:8765/health
curl -s -H 'Authorization: Bearer local-token' 'http://localhost:8765/students?page=1&size=2'
curl -s -H 'Authorization: Bearer local-token' http://localhost:8765/students/1/grades
curl -s http://localhost:8765/students                              # -> 401 without the token
```

Endpoints (see the script's docstring for all of them):

| Endpoint | Shape | Good for |
|---|---|---|
| `GET /students` | JSON array; `?dept=CS`; `?page=N&size=M` or `?offset=N&limit=M` | `responseType: JSONArray`, pagination |
| `GET /students/wrapped` | `{"status": 200, "data": {"students": [...], "count": 7}}` | `JSONObject` + `recordExtractionPath` |
| `GET /students/lines` | NDJSON | `JSONLines` |
| `GET /students/{roll_no}/grades` | `{"status": 200, "data": {"roll_no": 1, "semesters": [...]}}`, 404 if unknown | enrichment, `pathParams` |
| `POST /grades/bulk` | `{"ids": [1, 2]}` → array of records | bulk enrichment |
| `ANY /echo` | echoes method, query, body, headers | checking what you send |
| `GET /flaky`, `GET /slow?seconds=2`, `GET /empty` | 503 twice then 200; slow; `[]` | retries, timeouts, empty responses |

Options: `--delay 0.5` makes every request take half a second (to watch concurrency), `--port`,
`--fail-first N`. Leave the log visible in a terminal while you test.

## 2. The REST source from the CLI

Install the engine once:

```bash
cd engine && python3.11 -m venv .venv && .venv/bin/pip install -e ".[dev]" && cd ..
export PATH="$PWD/engine/.venv/bin:$PATH"
```

The token is read from an environment variable through `${secret:UNIVERSITY_TOKEN}`:

```bash
export TRIBUTARY_SECRET_UNIVERSITY_TOKEN=local-token
```

`examples/rest_api_local_test.yaml` reads `/students?dept=CS`, two records per page with a
bearer token and a declared schema, plus `/students/wrapped` through a JSONPath:

```bash
tributary validate examples/rest_api_local_test.yaml
tributary plan examples/rest_api_local_test.yaml
tributary preview examples/rest_api_local_test.yaml --node students --limit 3
tributary run examples/rest_api_local_test.yaml
```

What to expect:

- `preview` prints three CS students; the mock log shows **two** requests
  (`page=1&size=2`, `page=2&size=2`): a preview stops paging as soon as it has enough rows.
- `run` prints the `active_students` table (3 rows, names upper-cased, `enrolled_year` from the
  `DATE` column the schema declared), the quality results (3 checks, 0 failed), the wrapped
  endpoint's 7 rows, and writes `/tmp/tributary/rest_local_test/students/` as Parquet:

  ```bash
  tributary preview examples/rest_api_local_test.yaml --node students_out   # reads the sink's input
  ```
- The log shows `page=2` was the last request: the page came back short, so no third call.

Now change things and re-run `preview` to see the behaviour:

| Try | Expect |
|---|---|
| remove the `schema:` block | same rows, columns inferred by Spark's JSON reader (alphabetical order, `enrolled` becomes a string) |
| `pagination.type: offset`, `pageParam: offset`, `sizeParam: limit` | requests `offset=0`, `offset=2`, … |
| `pagination: { type: none }` | one request, all 7 records (the `dept` filter still applies) |
| `url: .../students/wrapped` with `responseType: JSONArray` | error: *`... is a JSON object, not a JSON array; set responseType`* |
| `responseType: JSONObject`, `recordExtractionPath: "$.data.count"` | one row with a `value` column = 7 |
| `url: .../students/lines`, `responseType: JSONLines` | 7 rows |
| `token: wrong` | error: *`HTTP 401 Unauthorized: {"error": "missing or wrong bearer token"}`* |
| `url: .../students/99/grades` | error: *`HTTP 404 Not Found: {"error": "no student 99"}`* |
| `url: .../flaky`, `retryBackoffSeconds: 0` | succeeds; the log shows 503, 503, 200 (retries) |
| `url: .../slow`, `timeoutSeconds: 0.5` | error: *`... Read timed out`* |
| `url: .../empty` | zero rows (with `recordSchema`, zero rows with the declared columns) |

## 3. The REST source in the UI

Start the backend and the UI (Postgres must be up):

```bash
docker compose -f deploy/docker-compose.yml up -d postgres
(cd backend && python3.11 -m venv .venv && .venv/bin/pip install -e ../engine -e . \
   && TRIBUTARY_SECRET_UNIVERSITY_TOKEN=local-token .venv/bin/tributary-backend serve) &
(cd ui && npm install && npm run dev)
```

The backend process must see the token variable, because previews and runs resolve
`${secret:...}` there (alternatively store the token in a connection, step 3c).

Open http://localhost:5173.

### 3a. Import the example and test it

1. *Pipelines → Import*, choose `examples/rest_api_local_test.yaml`.
2. The canvas shows `students` and `grades_wrapped` (REST API sources, globe icon), the SQL
   node, the quality block and the sinks. The status in the header reads *Checking columns…*
   then *Valid*; during that check the mock log shows **one** request per source that has no
   schema (`/students/wrapped`): deep validation calls the API once to learn the columns, and
   not at all for `students`, whose schema is declared.
3. Click `students`. The right panel shows the options form: *Endpoint\** (URL, method,
   *More options* for headers, timeout, retries), *Response parsing*, *Pagination*, *Auth type*
   with *Token\**. Mandatory fields carry a red star.
4. Press **Test**. The preview drawer lists the rows (limit 50); the log shows the paged
   requests.
5. Click the SQL node, press **Test**: the rows are upper-cased; the SQL editor autocompletes
   the columns the REST source produced.
6. Press **Run** (*Save version* first if asked). The run opens in the *Runs* tab with live
   logs; the mock log shows the worker's requests; the quality results appear under the run.

### 3b. Draw a REST source from scratch

1. *Pipelines → New*, then click **REST API** in the palette (Sources), **SQL** (Transforms)
   and **Console** (Sinks); drag from the right handle of each node to the left handle of the
   next.
2. Select the REST node. Fill *Endpoint → Url* with `http://localhost:8765/students`, open
   *More options* and set *Auth type* = `bearer`, *Token* = `local-token` (or `${secret:UNIVERSITY_TOKEN}`).
3. Optional: *Response parsing → Response type* (`JSONArray` is the default), *Pagination →
   Type* = `page`, then *More options* inside it: *Size param* = `size`, *Page size* = `3`.
4. The node's red badge disappears once the URL is set; *Checking columns…* makes one call
   and the downstream SQL editor now knows `active, dept, enrolled, name, roll_no`.
5. In the SQL node type `SELECT dept, count(*) AS students FROM rest_api_source_1 GROUP BY dept`
   and press **Test**.

### 3c. Keep the token in a connection

1. *Connections → New*: name `university_api`, type `rest_api`, environment `dev`, fields
   `baseUrl = http://localhost:8765`, `authType = bearer`, `token = local-token`. `token` is
   detected as a secret: it is encrypted at rest and shown as `********`.
2. On the REST node choose *Connection = university_api*; the form marks *Auth type* and
   *Token* as *from university_api*, and the URL can now be relative: `/students`.
3. **Test** again: the request carries the bearer token, although the pipeline spec never
   contains it.

## 4. DataFrame-driven calls (REST enrichment) against the mock

`examples/rest_api_enrichment_local_test.yaml` reads `examples/data/students.csv` and calls
`/students/{roll_no}/grades` once per student (2 tasks, 5 calls/s each), then does the same
with one `POST /grades/bulk` per 3 students:

```bash
tributary preview examples/rest_api_enrichment_local_test.yaml --node grades
tributary run examples/rest_api_enrichment_local_test.yaml
```

Expect 7 rows from 5 students (Asha and Eli have two semesters; Chen has none and keeps a row
with null grade columns). The mock log shows five `GET /students/N/grades` lines spaced by
the rate limit, then two `POST /grades/bulk` lines (`3 ids`, `2 ids`). In the UI, import the
file, select `grades`, press **Test**: only the previewed rows are called for.

Things to try: `concurrency.maxConcurrentCalls: 1` with the mock started with `--delay 0.5`
(the calls become strictly sequential, visible in the timestamps); add a student `99` to
`students.csv` and set `dynamicInput.onError: null` (the row survives with nulls; with `fail`
the run stops on the 404); remove `recordSchema` (the columns are then inferred from the
responses; deep validation in the UI asks for a schema or the enrichment's `dummyData`, previews still work).

## 5. The backend API directly (optional)

The same operations the UI performs, with `curl` (backend on :8000):

```bash
PY=engine/.venv/bin/python
$PY -c 'import json,yaml;print(json.dumps({"spec": yaml.safe_load(open("examples/rest_api_local_test.yaml")), "env": "dev", "node_id": "students", "limit": 3}))' \
  | curl -s -X POST http://localhost:8000/api/v1/preview -H 'Content-Type: application/json' -d @- | $PY -m json.tool | head -40
$PY -c 'import json,yaml;print(json.dumps({"spec": yaml.safe_load(open("examples/rest_api_local_test.yaml")), "env": "dev", "deep": True}))' \
  | curl -s -X POST http://localhost:8000/api/v1/validate -H 'Content-Type: application/json' -d @- | $PY -m json.tool | grep -E '"valid"|"name"'
```

## 6. The automated tests

The engine's REST tests start their own instance of the same mock (random port), so they need
no setup and no network:

```bash
cd engine && .venv/bin/pytest tests/test_rest_api.py -q           # 47 tests: source, enrichment, helpers
cd ui && npm test                                                  # includes the REST form tests
cd backend && .venv/bin/pytest tests/test_rest_api.py -q           # needs the docker-compose Postgres
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `HTTP 401 Unauthorized` | the token is not reaching the API: `TRIBUTARY_SECRET_UNIVERSITY_TOKEN` not exported in the shell that runs `tributary` / the backend, or the connection is for another environment |
| `secret 'UNIVERSITY_TOKEN' not found` | same variable missing (`validate` without `--deep` does not need it; `preview`/`run` do) |
| `Connection refused` on `127.0.0.1:8765` | the mock is not running, or Docker published another port (`docker compose ps`) |
| `Java gateway process exited` | Java missing or 21+; use Java 17 (`JAVA_HOME`) |
| `PYTHON_VERSION_MISMATCH` in a Spark task | you drive Spark from your own script with another Python than the executors'; `export PYSPARK_PYTHON=$PWD/engine/.venv/bin/python` (the CLI and the backend set it themselves) |
| `ModuleNotFoundError: requests` on an executor | the engine's Python environment must be the executors' too (`PYSPARK_PYTHON=engine/.venv/bin/python`); on a cluster install `tributary-engine` on the workers |
| the enrichment node shows *"columns of the API records are not known without calling the API"* | deep validation runs without rows: add `parsing.recordSchema` (or dummy records); previews and runs are not affected |
| previews are slow the first time | the shared preview Spark session starts on the first request (≈10 s) |
| the mock API in Docker cannot be reached from a pipeline **running inside another container** | use `http://mock-api:8765` on the compose network instead of `localhost` |
