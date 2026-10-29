"""The REST API source and the REST API enrichment transform, against a local HTTP server."""

from __future__ import annotations

import pytest

from conftest import pipeline
from rest_server import GRADES, STUDENTS, start
from tributary import SpecError, analyze, compile_pipeline, preview, run
from tributary.core import RunContext
from tributary.errors import TributaryError
from tributary.plugins._rest import RestApiError
from tributary.plugins.sources.rest_api import RestApiSource
from tributary.plugins.transforms.rest_api_enrichment import RestApiEnrichment
from tributary.registry import default_registry
from tributary.testing import assert_source_contract, static_contract_issues


@pytest.fixture(scope="module")
def api():
    server = start()
    yield server
    server.stop()


@pytest.fixture
def calls(api):
    api.state.reset()
    return api.state


def ctx(spark, limit: int = 20, preview: bool = False) -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id="t", env="test", preview=preview, preview_limit=limit)


def source_opts(api, path="/students", **extra) -> RestApiSource.Options:
    return RestApiSource.Options.model_validate({"endpoint": {"url": api.url + path}, **extra})


def messages(exc: SpecError) -> str:
    return "\n".join(str(i) for i in exc.issues)


# --- the standalone source -------------------------------------------------------------


def test_contracts(spark, api, calls):
    assert static_contract_issues(RestApiSource) == []
    assert static_contract_issues(RestApiEnrichment) == []
    assert_source_contract(spark, RestApiSource(), source_opts(api), limit=2)


def test_json_array_is_a_table_of_records(spark, api, calls):
    df = RestApiSource().read(ctx(spark), source_opts(api), None)
    assert df.columns == ["active", "dept", "name", "roll_no"]  # inferred by Spark's JSON reader (sorted)
    assert sorted(r.name for r in df.collect()) == ["Asha", "Bo", "Chen", "Dee", "Eli"]
    assert calls.paths() == ["/students"]


def test_declared_record_schema_gives_typed_columns(spark, api, calls):
    opts = source_opts(api, parsing={"recordSchema": "roll_no INT, name STRING, missing STRING"})
    df = RestApiSource().read(ctx(spark), opts, None)
    assert [(f.name, f.dataType.simpleString()) for f in df.schema.fields] == [("roll_no", "int"), ("name", "string"), ("missing", "string")]
    assert {r.roll_no: r.missing for r in df.collect()} == {1: None, 2: None, 3: None, 4: None, 5: None}


def test_json_object_with_extraction_path(spark, api, calls):
    opts = source_opts(api, "/students/wrapped", parsing={"responseType": "JSONObject", "recordExtractionPath": "$.data.students"})
    assert RestApiSource().read(ctx(spark), opts, None).count() == 5

    opts = source_opts(api, "/students/wrapped", parsing={"responseType": "JSONObject", "recordExtractionPath": "$.data", "explodeArray": False})
    [row] = RestApiSource().read(ctx(spark), opts, None).collect()
    assert row["count"] == 5 and len(row.students) == 5  # one row: the extracted object, flattened

    opts = source_opts(api, "/students/wrapped", parsing={"responseType": "JSONObject", "recordExtractionPath": "$.data.count"})
    assert RestApiSource().read(ctx(spark), opts, None).collect()[0].value == 5  # a scalar lands in `value`


def test_json_lines(spark, api, calls):
    opts = source_opts(api, "/students/lines", parsing={"responseType": "JSONLines"})
    assert RestApiSource().read(ctx(spark), opts, None).count() == 5


def test_wrong_response_type_is_reported(spark, api, calls):
    with pytest.raises(RestApiError, match="is a JSON object, not a JSON array; set responseType"):
        RestApiSource().read(ctx(spark), source_opts(api, "/students/wrapped"), None)
    with pytest.raises(RestApiError, match="not JSONArray"):
        RestApiSource().read(ctx(spark), source_opts(api, "/broken"), None)


def test_page_pagination_stops_at_a_short_page(spark, api, calls):
    opts = source_opts(api, pagination={"type": "page", "pageParam": "page", "sizeParam": "size", "pageSize": 2})
    df = RestApiSource().read(ctx(spark), opts, None)
    assert sorted(r.roll_no for r in df.collect()) == [1, 2, 3, 4, 5]
    assert [c.query["page"] for c in calls.calls] == [["1"], ["2"], ["3"]]  # the third page is short: no fourth call
    assert all(c.query["size"] == ["2"] for c in calls.calls)


def test_offset_pagination_without_a_size_parameter_stops_at_an_empty_page(spark, api, calls):
    opts = source_opts(api, pagination={"type": "offset", "pageParam": "offset", "pageSize": 2})
    assert RestApiSource().read(ctx(spark), opts, None).count() == 5
    assert [c.query["offset"] for c in calls.calls] == [["0"], ["2"], ["4"], ["6"]]


def test_max_pages_caps_the_requests(spark, api, calls):
    opts = source_opts(api, pagination={"type": "page", "sizeParam": "size", "pageSize": 1, "maxPages": 2})
    assert RestApiSource().read(ctx(spark), opts, None).count() == 2
    assert len(calls.calls) == 2


def test_preview_fetches_only_the_pages_it_needs(spark, api, calls):
    opts = source_opts(api, pagination={"type": "page", "sizeParam": "size", "pageSize": 2})
    df = RestApiSource().read_preview(ctx(spark), opts, None, limit=3)
    assert df.count() == 3
    assert len(calls.calls) == 2


def test_static_query_params_and_auth(spark, api, calls):
    opts = source_opts(api, "/echo", endpoint={"url": api.url + "/echo", "queryParams": {"dept": "CS", "active": True}},
                       parsing={"responseType": "JSONObject"}, authType="bearer", token="t0k")
    [row] = RestApiSource().read(ctx(spark), opts, None).collect()
    assert row.query.asDict() == {"dept": ["CS"], "active": ["true"]} and row.auth == "Bearer t0k"

    opts = source_opts(api, "/echo", parsing={"responseType": "JSONObject"}, auth_type="api_key", token="k3y")
    assert RestApiSource().read(ctx(spark), opts, None).collect()[0].api_key == "k3y"

    opts = source_opts(api, "/echo", parsing={"responseType": "JSONObject"}, authType="basic", username="u", password="p")
    assert RestApiSource().read(ctx(spark), opts, None).collect()[0].auth == "Basic dTpw"


def test_auth_fields_are_required_by_auth_type():
    with pytest.raises(ValueError, match="required for authType 'bearer'"):
        RestApiSource.Options.model_validate({"endpoint": {"url": "http://x"}, "authType": "bearer"})


def test_post_with_static_body(spark, api, calls):
    opts = source_opts(api, endpoint={"url": api.url + "/echo", "method": "POST", "body": {"q": [1, 2]}}, parsing={"responseType": "JSONObject"})
    [row] = RestApiSource().read(ctx(spark), opts, None).collect()
    assert row.method == "POST" and row.body.q == [1, 2]
    assert calls.calls[0].headers["Content-Type"] == "application/json"


def test_http_errors_are_retried_then_reported(spark, api, calls):
    calls.reset(fail_first=2)
    opts = source_opts(api, "/flaky", endpoint={"url": api.url + "/flaky", "retryBackoffSeconds": 0})
    assert RestApiSource().read(ctx(spark), opts, None).collect()[0].ok is True
    assert len(calls.calls) == 3  # two 503s, then a 200

    calls.reset(fail_first=5)
    with pytest.raises(RestApiError, match="HTTP 503"):
        RestApiSource().read(ctx(spark), opts, None)
    assert len(calls.calls) == 4  # the first try plus maxRetries

    with pytest.raises(RestApiError, match="HTTP 404 .*no student 99"):
        RestApiSource().read(ctx(spark), source_opts(api, "/students/99/grades"), None)


def test_local_dummy_data_stands_in_for_the_api(spark, api, calls, tmp_path):
    """No endpoint at all: in the local environment the node's dummy data is what the
    pipeline reads, and the API is never called."""
    doc = pipeline(tmp_path, tmp_path / "unused.csv")
    doc["environments"] = {"local": doc["environments"]["dev"]}
    doc["sources"] = [{"id": "students", "type": "rest_api", "options": {},
                       "dummy": {"enabled": True, "data": '{"roll_no": 7, "name": "Dummy"}\n{"roll_no": 8, "name": "Data"}'}}]
    doc["transforms"][0] = {"id": "upper", "type": "sql", "inputs": ["students"], "options": {"sql": "SELECT roll_no, upper(name) AS name FROM students"}}
    doc["sinks"][0]["input"] = "upper"
    report = analyze(compile_pipeline(doc, env="local"), spark=spark)
    assert all(a.ok for a in report.values())
    run(doc, env="local", spark=spark)
    rows = spark.read.parquet(str(tmp_path / "out" / "orders")).collect()
    assert sorted((r.roll_no, r.name) for r in rows) == [(7, "DUMMY"), (8, "DATA")]
    assert calls.calls == []
    assert preview(doc, "students", env="local", spark=spark, limit=1).rows == [{"name": "Dummy", "roll_no": 7}]


def test_only_the_enrichment_has_its_own_dummy_data():
    with pytest.raises(ValueError, match=r"endpoint\n  Field required"):
        RestApiSource.Options.model_validate({})
    with pytest.raises(ValueError, match=r"useDummyData\n  Extra inputs are not permitted"):
        RestApiSource.Options.model_validate({"endpoint": {"url": "http://x"}, "useDummyData": True})
    assert RestApiEnrichment.Options.model_validate(
        {"useDummyData": True, "dummyData": [{"a": 1}], "dynamicInput": {"parameterMapping": {}}}
    ).endpoint is None


def test_source_in_a_pipeline_with_deep_validation(spark, api, calls, tmp_path):
    doc = pipeline(tmp_path, tmp_path / "unused.csv")
    doc["sources"] = [{"id": "students", "type": "rest_api", "options": {"endpoint": {"url": api.url + "/students"}},
                       "schema": {"kind": "ddl", "ddl": "roll_no INT, name STRING, dept STRING"}}]
    doc["transforms"][0] = {"id": "cs", "type": "sql", "inputs": ["students"],
                            "options": {"sql": "SELECT roll_no, upper(name) AS name FROM students WHERE dept = 'CS'"}}
    doc["sinks"][0]["input"] = "cs"
    report = analyze(compile_pipeline(doc), spark=spark)
    assert all(a.ok for a in report.values()), {k: a.error for k, a in report.items()}
    assert [f["name"] for f in report["students"].schema["fields"]] == ["roll_no", "name", "dept"]
    assert calls.calls == []  # a declared schema: deep validation never calls the API

    del doc["sources"][0]["schema"]
    report = analyze(compile_pipeline(doc), spark=spark)
    assert report["cs"].ok and calls.paths() == ["/students"]  # nothing declared: one call to learn the columns

    run(doc, spark=spark)
    rows = spark.read.parquet(str(tmp_path / "out" / "orders")).collect()
    assert sorted((r.roll_no, r.name) for r in rows) == [(1, "ASHA"), (3, "CHEN")]


def test_declared_schemas_take_precedence_and_bad_ddl_is_reported(spark, api, calls):
    from tributary.core import ResolvedSchema
    from tributary.plugins.schemas.basic import parse_ddl

    node_schema = ResolvedSchema(struct=parse_ddl(spark, "roll_no INT, name STRING"))
    opts = source_opts(api, parsing={"recordSchema": "name STRING"})
    df = RestApiSource().read(ctx(spark), opts, node_schema)
    assert df.columns == ["roll_no", "name"] and df.count() == 5  # the node's schema: block wins over parsing.recordSchema

    # deep validation (limit 0) with a recordSchema: the columns come from the DDL, no request
    df = RestApiSource().read_preview(ctx(spark, limit=0, preview=True), opts, None, limit=0)
    assert df.columns == ["name"] and df.count() == 0 and calls.paths() == ["/students"]

    with pytest.raises(TributaryError, match="parsing.recordSchema is not valid Spark DDL"):
        RestApiSource().read(ctx(spark), source_opts(api, parsing={"recordSchema": "this is not ddl"}), None)


def test_empty_responses(spark, api, calls):
    df = RestApiSource().read(ctx(spark), source_opts(api, "/empty"), None)
    assert df.columns == [] and df.count() == 0
    df = RestApiSource().read(ctx(spark), source_opts(api, "/empty", parsing={"recordSchema": "id INT"}), None)
    assert df.columns == ["id"] and df.count() == 0
    # an extraction path that points nowhere yields no records rather than an error
    opts = source_opts(api, "/students/wrapped", parsing={"responseType": "JSONObject", "recordExtractionPath": "$.data.nope"})
    assert RestApiSource().read(ctx(spark), opts, None).count() == 0


def test_pagination_can_start_anywhere(spark, api, calls):
    opts = source_opts(api, pagination={"type": "page", "sizeParam": "size", "pageSize": 2, "start": 2})
    assert sorted(r.roll_no for r in RestApiSource().read(ctx(spark), opts, None).collect()) == [3, 4, 5]
    assert [c.query["page"] for c in calls.calls] == [["2"], ["3"]]
    calls.reset()
    opts = source_opts(api, pagination={"type": "offset", "pageParam": "offset", "sizeParam": "limit", "pageSize": 2, "start": 3})
    assert sorted(r.roll_no for r in RestApiSource().read(ctx(spark), opts, None).collect()) == [4, 5]
    assert [c.query["offset"] for c in calls.calls] == [["3"], ["5"]]  # a full page: one more request finds the end


def test_request_construction(spark, api, calls, monkeypatch, tmp_path):
    # relative URL under baseUrl, static headers (with a secret), a JSON body given as text
    monkeypatch.setenv("TRIBUTARY_SECRET_API_TOKEN", "s3cret")
    doc = pipeline(tmp_path, tmp_path / "unused.csv")
    doc["sources"] = [{"id": "echo", "type": "rest_api", "options": {
        "baseUrl": api.url + "/", "endpoint": {"url": "/echo", "method": "PUT", "headers": {"X-Custom": "yes", "Authorization": "Bearer ${secret:API_TOKEN}"},
                                               "body": '{"q": 1}'},
        "parsing": {"responseType": "JSONObject"},
    }}]
    doc["transforms"] = []
    doc["sinks"][0]["input"] = "echo"
    [row] = preview(doc, "echo", spark=spark).rows
    assert row["method"] == "PUT" and row["body"] == {"q": 1} and row["content_type"] == "application/json"
    assert row["custom"] == "yes" and row["auth"] == "Bearer s3cret"
    assert calls.paths() == ["/echo"]
    assert RestApiSource().estimate_input_bytes(RestApiSource.Options.model_validate(doc["sources"][0]["options"])) is None


def test_client_errors(spark, api, calls):
    calls.reset(fail_first=1)
    opts = source_opts(api, "/flaky", endpoint={"url": api.url + "/flaky", "maxRetries": 0})
    with pytest.raises(RestApiError, match="HTTP 503"):
        RestApiSource().read(ctx(spark), opts, None)
    assert len(calls.calls) == 1  # no retries asked for

    opts = source_opts(api, "/slow", endpoint={"url": api.url + "/slow", "queryParams": {"seconds": 2}, "timeoutSeconds": 0.3, "maxRetries": 0})
    with pytest.raises(RestApiError, match="timed out"):
        RestApiSource().read(ctx(spark), opts, None)

    unreachable = RestApiSource.Options.model_validate({"endpoint": {"url": "http://127.0.0.1:1/nothing", "maxRetries": 0}})
    with pytest.raises(RestApiError, match="GET http://127.0.0.1:1/nothing: .*[Cc]onnection"):
        RestApiSource().read(ctx(spark), unreachable, None)


def test_parser_rejects_bodies_that_do_not_match_the_response_type(spark, api, calls):
    with pytest.raises(RestApiError, match="is a JSON array, not a JSON object; set responseType"):
        RestApiSource().read(ctx(spark), source_opts(api, parsing={"responseType": "JSONObject"}), None)
    with pytest.raises(RestApiError, match="is not JSONLines"):
        RestApiSource().read(ctx(spark), source_opts(api, "/bad-lines", parsing={"responseType": "JSONLines"}), None)
    with pytest.raises(RestApiError, match="unsupported JSONPath"):
        RestApiSource().read(ctx(spark), source_opts(api, "/students/wrapped", parsing={"responseType": "JSONObject", "recordExtractionPath": "$..students"}), None)


def test_source_is_a_static_lookup_in_a_streaming_pipeline(spark, api, calls, tmp_path):
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"roll_no": 1, "score": 10}\n{"roll_no": 3, "score": 7}\n')
    out = tmp_path / "out"
    doc = {
        "pipeline": {"id": "stream_lookup", "mode": "streaming",
                     "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now"}},
        "sources": [
            {"id": "events", "type": "file", "stream": True, "options": {"path": str(incoming), "format": "json"},
             "schema": {"kind": "ddl", "ddl": "roll_no INT, score INT"}},
            {"id": "students", "type": "rest_api", "options": {"endpoint": {"url": api.url + "/students"}},
             "schema": {"kind": "ddl", "ddl": "roll_no INT, name STRING"}},
        ],
        "transforms": [{"id": "named", "type": "sql", "inputs": ["events", "students"],
                        "options": {"sql": "SELECT e.roll_no, e.score, s.name FROM events e JOIN students s ON e.roll_no = s.roll_no"}}],
        "sinks": [{"id": "sink", "type": "file", "input": "named", "options": {"path": str(out)}}],
    }
    query = run(doc, spark=spark)
    assert not query.isActive
    assert sorted((r.roll_no, r.name, r.score) for r in spark.read.parquet(str(out)).collect()) == [(1, "Asha", 10), (3, "Chen", 7)]
    assert calls.paths() == ["/students"]  # one micro-batch: read once


def test_camel_and_snake_case_keys_are_both_accepted_and_typos_rejected(tmp_path, api):
    doc = pipeline(tmp_path, tmp_path / "unused.csv")
    doc["sources"] = [{"id": "s", "type": "rest_api", "options": {
        "endpoint": {"url": api.url, "query_params": {"a": 1}},
        "parsing": {"response_type": "JSONObject", "recordExtractionPath": "$.data"},
    }}]
    doc["transforms"] = []
    doc["sinks"][0]["input"] = "s"
    opts = compile_pipeline(doc).spec.sources[0].options
    assert opts.endpoint.query_params == {"a": 1} and opts.parsing.response_type == "JSONObject"

    doc["sources"][0]["options"]["parsing"]["responseTyp"] = "JSONArray"
    with pytest.raises(SpecError) as err:
        compile_pipeline(doc)
    assert "sources[0].options.parsing.responseTyp" in messages(err.value)


# --- the enrichment transform ----------------------------------------------------------


def enrichment_doc(api, tmp_path, **options) -> dict:
    """students.csv -> rest_api_enrichment (grades per student) -> parquet; the sample
    configuration of the specification, with a local API."""
    students = tmp_path / "students.csv"
    students.write_text("student_roll_num,name\n" + "".join(f"{s['roll_no']},{s['name']}\n" for s in STUDENTS))
    doc = pipeline(tmp_path, students)
    doc["sources"][0]["schema"] = {"kind": "ddl", "ddl": "student_roll_num INT, name STRING"}
    doc["transforms"] = [{
        "id": "grades", "type": "rest_api_enrichment", "inputs": [{"node": "orders_src", "as": "student_enrollment_df"}],
        "options": {
            "endpoint": {"url": api.url + "/students/{roll_no}/grades", "method": "GET"},
            "dynamicInput": {
                "inputStrategy": "DataFrameDriven",
                "inputDataFrameRef": "student_enrollment_df",
                "parameterMapping": {"pathParams": {"roll_no": "student_roll_num"}},
                "bulkRequestSupport": False,
            },
            "parsing": {"responseType": "JSONObject", "recordExtractionPath": "$.data.semesters", "explodeArray": True},
            "concurrency": {"maxConcurrentCalls": 2, "rateLimitPerExecutor": 50, "connectionPoolSize": 2},
            **options,
        },
    }]
    doc["sinks"][0]["input"] = "grades"
    return doc


def test_enrichment_explodes_each_response_next_to_its_row(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path)
    run(doc, spark=spark)
    df = spark.read.parquet(str(tmp_path / "out" / "orders"))
    assert df.columns == ["student_roll_num", "name", "gpa", "semester"]
    got = {(r.student_roll_num, r.semester): r.gpa for r in df.collect()}
    expected = {(roll, g["semester"]): g["gpa"] for roll, grades in GRADES.items() for g in grades}
    expected[(3, None)] = None  # a student without grades keeps the row (explode_outer)
    assert got == expected
    assert sorted(calls.paths()) == sorted(f"/students/{s['roll_no']}/grades" for s in STUDENTS)  # one call per row, once


def test_enrichment_with_declared_schema_needs_no_inference(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, parsing={"responseType": "JSONObject", "recordExtractionPath": "$.data.semesters",
                                                  "recordSchema": "semester INT, gpa DECIMAL(3,1)"})
    result = preview(doc, "grades", spark=spark, limit=10)
    assert [f["name"] for f in result.schema["fields"]] == ["student_roll_num", "name", "semester", "gpa"]
    assert result.schema["fields"][3]["type"] == "decimal(3,1)"
    assert len(result.rows) == 7


def test_preview_limits_the_rows_that_call_the_api(spark, api, calls, tmp_path):
    result = preview(enrichment_doc(api, tmp_path), "grades", spark=spark, limit=2)
    assert len(calls.calls) == 2 and len(result.rows) <= 4


def test_deep_validation_needs_a_schema_or_dummy_data(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path)
    report = analyze(compile_pipeline(doc), spark=spark)
    assert not report["grades"].ok and "parsing.recordSchema or give dummyData" in report["grades"].error
    assert "skipped" in report["orders_out"].error

    doc["transforms"][0]["options"]["dummyData"] = [{"semester": 1, "gpa": 3.0}]
    report = analyze(compile_pipeline(doc), spark=spark)
    assert report["grades"].ok
    assert [f["name"] for f in report["grades"].schema["fields"]] == ["student_roll_num", "name", "gpa", "semester"]

    doc["transforms"][0]["options"]["parsing"]["recordSchema"] = "semester INT, gpa DOUBLE"
    report = analyze(compile_pipeline(doc), spark=spark)
    assert report["grades"].ok and report["orders_out"].ok
    assert [f["name"] for f in report["grades"].schema["fields"]] == ["student_roll_num", "name", "semester", "gpa"]
    assert calls.calls == []


def test_dummy_data_replaces_every_call(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, useDummyData=True, dummyData=[{"semester": 9, "gpa": 1.0}])
    result = preview(doc, "grades", spark=spark, limit=10)
    assert len(result.rows) == 5 and {r["semester"] for r in result.rows} == {9}
    assert calls.calls == []


def test_query_params_and_body_template_from_columns(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, endpoint={"url": api.url + "/echo", "method": "POST"}, parsing={"responseType": "JSONObject"},
                         dynamicInput={"parameterMapping": {
                             "queryParams": {"student_id": "student_roll_num"},
                             "bodyTemplate": {"request_id": "${student_roll_num}", "action": "fetch", "note": "student ${name}"},
                         }})
    result = preview(doc, "grades", spark=spark, limit=10)
    by_roll = {r["student_roll_num"]: r for r in result.rows}
    assert by_roll[1]["query"] == {"student_id": ["1"]}
    assert by_roll[1]["body"] == {"request_id": 1, "action": "fetch", "note": "student Asha"}  # typed where the value is the whole string
    assert by_roll[1]["method"] == "POST"


def test_bulk_requests_batch_rows_and_match_records_back(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, endpoint={"url": api.url + "/grades/bulk", "method": "POST"}, parsing={"responseType": "JSONArray"},
                         dynamicInput={
                             "parameterMapping": {"bodyTemplate": {"ids": "${student_roll_num}"}},
                             "bulkRequestSupport": True, "bulkBatchSize": 3,
                             "bulkResponseMatch": {"responseField": "roll_no", "inputColumn": "student_roll_num"},
                         }, concurrency={"maxConcurrentCalls": 1})
    run(doc, spark=spark)
    assert len(calls.calls) == 2  # 5 rows, 3 per call
    assert sorted(len(c.body["ids"]) for c in calls.calls) == [2, 3]
    rows = spark.read.parquet(str(tmp_path / "out" / "orders")).collect()
    got = {(r.student_roll_num, r.semester): (r.name, r.gpa) for r in rows}
    assert got[(1, 2)] == ("Asha", 3.7) and got[(5, 1)] == ("Eli", 3.9) and got[(3, None)] == ("Chen", None)
    assert len(rows) == 7


def test_bulk_records_without_a_match_have_null_input_columns(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, endpoint={"url": api.url + "/grades/bulk", "method": "POST"}, parsing={"responseType": "JSONArray"},
                         dynamicInput={"parameterMapping": {"bodyTemplate": "${student_roll_num}"}, "bulkRequestSupport": True, "bulkBatchSize": 10},
                         concurrency={"maxConcurrentCalls": 1})
    result = preview(doc, "grades", spark=spark, limit=10)
    assert calls.calls[0].body == [1, 2, 3, 4, 5]  # the template is the list itself
    assert len(result.rows) == 6 and all(r["student_roll_num"] is None for r in result.rows)
    assert sorted(r["roll_no"] for r in result.rows) == [1, 1, 2, 4, 5, 5]


def test_on_error_null_keeps_rows_whose_call_failed(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, dynamicInput={"parameterMapping": {"pathParams": {"roll_no": "student_roll_num"}}, "onError": "null"})
    sample = tmp_path / "students.csv"
    sample.write_text("student_roll_num,name\n1,Asha\n99,Ghost\n")
    result = preview(doc, "grades", spark=spark, limit=10)
    assert {(r["student_roll_num"], r["semester"]) for r in result.rows} == {(1, 1), (1, 2), (99, None)}

    doc["transforms"][0]["options"]["dynamicInput"]["onError"] = "fail"
    with pytest.raises(Exception, match="HTTP 404"):
        preview(doc, "grades", spark=spark, limit=10)


def test_concurrency_is_bounded_by_max_concurrent_calls(spark, api, calls, tmp_path):
    calls.reset(delay=0.15)
    doc = enrichment_doc(api, tmp_path, concurrency={"maxConcurrentCalls": 1})
    preview(doc, "grades", spark=spark, limit=10)
    assert len(calls.calls) == 5 and calls.max_in_flight() == 1  # the test session has 2 cores; one task was allowed


def test_rate_limit_paces_calls_within_a_task(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, concurrency={"maxConcurrentCalls": 1, "rateLimitPerExecutor": 10})
    preview(doc, "grades", spark=spark, limit=10)
    started = sorted(c.started for c in calls.calls)
    assert len(started) == 5
    assert all(b - a >= 0.09 for a, b in zip(started, started[1:]))  # >= 100 ms apart (10/s)


def test_column_clashes_need_a_prefix(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, useDummyData=True, dummyData=[{"name": "clash", "gpa": 1.0}])
    with pytest.raises(Exception, match="record column\\(s\\) name clash with input columns"):
        preview(doc, "grades", spark=spark, limit=10)
    doc["transforms"][0]["options"]["dynamicInput"]["responseColumnPrefix"] = "grade_"
    result = preview(doc, "grades", spark=spark, limit=10)
    assert [f["name"] for f in result.schema["fields"]] == ["student_roll_num", "name", "grade_gpa", "grade_name"]
    doc["transforms"][0]["options"]["dynamicInput"]["includeInputColumns"] = False
    result = preview(doc, "grades", spark=spark, limit=10)
    assert [f["name"] for f in result.schema["fields"]] == ["grade_gpa", "grade_name"]


def test_missing_columns_are_reported_before_any_call(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, dynamicInput={"parameterMapping": {"pathParams": {"roll_no": "roll"}, "queryParams": {"x": "nope"}}})
    with pytest.raises(Exception, match="pathParams.roll_no -> 'roll', queryParams.x -> 'nope'"):
        preview(doc, "grades", spark=spark, limit=10)
    assert calls.calls == []


def test_compile_time_checks(api, tmp_path):
    doc = enrichment_doc(api, tmp_path, dynamicInput={"inputDataFrameRef": "students_df"})
    with pytest.raises(SpecError, match="inputDataFrameRef 'students_df' is not an input of this node"):
        compile_pipeline(doc)

    doc = enrichment_doc(api, tmp_path, dynamicInput={"parameterMapping": {"bodyTemplate": {"id": "${student_roll_num}"}}})
    with pytest.raises(SpecError, match="bodyTemplate is set but the method is GET"):
        compile_pipeline(doc)

    doc = enrichment_doc(api, tmp_path, dynamicInput={"parameterMapping": {"pathParams": {"roll_no": "student_roll_num"}}})
    doc["sources"].append({"id": "other", "type": "file", "options": {"path": "x.csv"}})
    doc["transforms"][0]["inputs"] = ["orders_src", "other"]
    with pytest.raises(SpecError, match="connect exactly one input, or name the one that drives the calls"):
        compile_pipeline(doc)


def test_column_placeholders_survive_compilation_but_variables_are_still_checked(api, tmp_path):
    doc = enrichment_doc(api, tmp_path, endpoint={"url": api.url + "/echo", "method": "POST"},
                         dynamicInput={"parameterMapping": {"bodyTemplate": {"id": "${student_roll_num}", "out": "${env.out}"}}})
    options = compile_pipeline(doc).spec.transforms[0].options
    assert options.dynamic_input.parameter_mapping.body_template == {"id": "${student_roll_num}", "out": str(tmp_path / "out")}

    doc["transforms"][0]["options"]["dynamicInput"]["parameterMapping"]["bodyTemplate"]["out"] = "${out}"
    with pytest.raises(SpecError, match="names the variable 'out': write \\$\\{env.out\\}"):
        compile_pipeline(doc)
    doc["transforms"][0]["options"]["dynamicInput"]["parameterMapping"]["bodyTemplate"]["out"] = "${evn.out}"
    with pytest.raises(SpecError, match="unknown placeholder"):
        compile_pipeline(doc)


def test_transform_takes_base_url_and_credentials_from_a_connection(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, endpoint={"url": "/echo"}, parsing={"responseType": "JSONObject"},
                         dynamicInput={"parameterMapping": {"queryParams": {"id": "student_roll_num"}}})
    doc["environments"]["dev"]["connections"] = {"grades_api": {"type": "rest_api", "baseUrl": api.url, "authType": "bearer", "token": "abc"}}
    doc["transforms"][0]["connection"] = "grades_api"
    result = preview(doc, "grades", spark=spark, limit=1)
    assert result.rows[0]["auth"] == "Bearer abc" and calls.paths() == ["/echo"]


def test_enrichment_runs_per_micro_batch_in_streaming(spark, api, calls, tmp_path):
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"student_roll_num": 1}\n{"student_roll_num": 2}\n')
    out = tmp_path / "out"
    doc = {
        "pipeline": {"id": "stream_enrich", "mode": "streaming",
                     "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now"}},
        "sources": [{"id": "events", "type": "file", "options": {"path": str(incoming), "format": "json"},
                     "schema": {"kind": "ddl", "ddl": "student_roll_num INT"}}],
        "transforms": [{"id": "grades", "type": "rest_api_enrichment", "inputs": ["events"], "options": {
            "endpoint": {"url": api.url + "/students/{roll_no}/grades"},
            "dynamicInput": {"parameterMapping": {"pathParams": {"roll_no": "student_roll_num"}}},
            "parsing": {"responseType": "JSONObject", "recordExtractionPath": "$.data.semesters", "recordSchema": "semester INT, gpa DOUBLE"},
        }}],
        "sinks": [{"id": "sink", "type": "file", "input": "grades", "options": {"path": str(out)}}],
    }
    query = run(doc, spark=spark)
    assert not query.isActive
    rows = spark.read.parquet(str(out)).collect()
    assert sorted((r.student_roll_num, r.semester) for r in rows) == [(1, 1), (1, 2), (2, 1)]


# --- the shared helpers, on the driver (inside Spark tasks they are out of coverage's sight) ---


def test_jsonpath_subset():
    from tributary.plugins._rest import jsonpath

    doc = {"data": {"students": [{"a": 1, "b-c": 2}, {"a": 3}], "count": 2}, "status": 200}
    assert jsonpath(doc, "$.data.count") == 2
    assert jsonpath(doc, "$.data.students[0]['b-c']") == 2
    assert jsonpath(doc, "$.data.students[-1].a") == 3
    assert jsonpath(doc, "$.data.students[*].a") == [1, 3]
    assert jsonpath(doc, "$.data.*") == [[{"a": 1, "b-c": 2}, {"a": 3}], 2]
    assert jsonpath(doc, "$.nope.deeper") is None and jsonpath(doc, "$.data.students[9]") is None
    assert jsonpath(doc, "$") is doc and jsonpath([1, 2], "[1]") == 2
    with pytest.raises(RestApiError, match="unsupported JSONPath"):
        jsonpath(doc, "$.data[?(@.a > 1)]")


def test_templating():
    import datetime as dt
    import decimal

    from tributary.plugins._rest import body_as_json, param_text, render_template, render_url, template_columns

    row = {"roll": 42, "name": "Asha", "when": dt.date(2024, 1, 2), "amount": decimal.Decimal("1.50"), "ids": [1, 2], "flag": True, "none": None}
    assert render_template({"request_id": "${roll}", "note": "student ${name} on ${when}", "n": 1, "l": ["${ids}"]}, row) == {
        "request_id": 42, "note": "student Asha on 2024-01-02", "n": 1, "l": [[1, 2]],
    }
    assert render_template("${ids}", row) == [1, 2]  # the whole body is the value
    assert render_template('{"ids": "${ids}", "x": "${none}"}', row) == {"ids": [1, 2], "x": None}  # JSON text keeps types
    assert render_template("id=${roll}&amount=${amount}&f=${flag}", row) == "id=42&amount=1.50&f=true"
    with pytest.raises(RestApiError, match="bodyTemplate refers to column 'nope'"):
        render_template({"a": "${nope}"}, row)
    assert template_columns({"a": "${x}", "b": ["${y}", {"c": "${z} ${x}"}], "d": 1}) == {"x", "y", "z"}
    assert template_columns(None) == set()

    assert render_url("https://api/students/{roll_no}/{name}", row, {"roll_no": "roll"}) == "https://api/students/42/Asha"
    assert render_url("https://api/{name}", {"name": "a b/c"}, {}) == "https://api/a%20b%2Fc"
    with pytest.raises(RestApiError, match="URL placeholder {id} needs column 'student_id'"):
        render_url("https://api/{id}", row, {"id": "student_id"})

    assert param_text(True) == "true" and param_text(None) == "" and param_text([1, "a", None]) == "1,a,"
    assert param_text(dt.datetime(2024, 1, 2, 3, 4, 5)) == "2024-01-02T03:04:05" and param_text({"a": 1}) == '{"a": 1}'
    assert body_as_json(" [1, 2] ") == [1, 2] and body_as_json("plain text") == "plain text" and body_as_json({"a": 1}) == {"a": 1}
    assert body_as_json("{not json") == "{not json" and render_template("${blob}", {"blob": b"\xe9"}) == "\u00e9"


def test_token_bucket_paces_without_bursting():
    import time

    from tributary.plugins._rest import TokenBucket

    bucket = TokenBucket(rate=50)  # 20 ms apart
    started = time.monotonic()
    for _ in range(6):
        bucket.acquire()
    assert time.monotonic() - started >= 5 * 0.02 * 0.9  # the first is free, the next five wait


def test_parse_records_shapes():
    from tributary.plugins._rest import ParsingConfig, parse_records

    assert parse_records("", ParsingConfig()) == []
    assert parse_records("[1, {\"a\": 2}]", ParsingConfig()) == [{"value": 1}, {"a": 2}]
    assert parse_records("[1, 2]", ParsingConfig(explode=False)) == [{"value": [1, 2]}]
    assert parse_records('{"a": 1}', ParsingConfig("JSONObject")) == [{"a": 1}]
    assert parse_records('{"a": 1}\n\n{"a": 2}\n', ParsingConfig("JSONLines", "$[*].a")) == [{"value": 1}, {"value": 2}]
    with pytest.raises(RestApiError, match="page 3 is not JSONArray"):
        parse_records("{", ParsingConfig(), "page 3")


def test_enrichment_pretends_without_an_endpoint(spark, api, calls, tmp_path):
    doc = enrichment_doc(api, tmp_path, useDummyData=True, dummyData=[{"semester": 1, "gpa": 3.0}])
    del doc["transforms"][0]["options"]["endpoint"]
    del doc["transforms"][0]["options"]["parsing"]
    result = preview(doc, "grades", spark=spark, limit=10)
    assert len(result.rows) == 5 and all(r["gpa"] == 3.0 for r in result.rows)
    assert calls.calls == []
