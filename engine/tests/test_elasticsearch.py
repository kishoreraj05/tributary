"""The Elasticsearch sink: options, connector settings, session configuration, design-time
checks, and real writes to a live cluster.

Tests that write need the connector jar (``elasticsearch_jar`` in conftest) and an
Elasticsearch at TRIBUTARY_TEST_ES_URL (the docker-compose one at localhost:9200 by
default); the others run without Spark.
"""

from __future__ import annotations

import json
import urllib.request
import uuid
from pathlib import Path

import pytest
from pydantic import ValidationError
from pyspark.sql.types import StructType

from conftest import ES_URL, pipeline
from tributary import SpecError, compile_pipeline, preview, run
from tributary.core import RunContext
from tributary.plugins.sinks.elasticsearch import (
    ELASTICSEARCH_EXCLUDES,
    ELASTICSEARCH_VERSION,
    EXCLUDES_KEY,
    ElasticsearchSink,
    ElasticsearchSinkOptions,
    supported,
    with_iso_timestamps,
)
from tributary.registry import default_registry
from tributary.runtime import decide_resources, required_conf, required_packages, session_conf
from tributary.testing import assert_sink_contract, static_contract_issues

NODES = ES_URL.split("://", 1)[-1]
PACKAGE = f"org.elasticsearch:elasticsearch-spark-30_2.12:{ELASTICSEARCH_VERSION}"


def es_sink(index: str, **options) -> dict:
    return {"id": "orders_out", "type": "elasticsearch", "input": "orders_clean", "options": {"nodes": NODES, "index": index, **options}}


def es_pipeline(tmp_path, orders_csv, index: str, **options) -> dict:
    return pipeline(tmp_path, orders_csv, sinks=[es_sink(index, **options)])


def options(**overrides) -> ElasticsearchSinkOptions:
    return ElasticsearchSinkOptions(**{"nodes": NODES, "index": "orders", **overrides})


def ctx(spark, batch_id=None) -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id="es_test", env="dev", batch_id=batch_id)


# --- talking to the cluster directly, to check what was written -------------------------


def es(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(f"{ES_URL}{path}", data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        return json.load(exc)


def docs(index: str) -> dict[str, dict]:
    """{_id: _source} of every document in the index (or pattern), after a refresh."""
    es("POST", f"/{index}/_refresh")
    hits = es("GET", f"/{index}/_search?size=1000").get("hits", {}).get("hits", [])
    return {h["_id"]: h["_source"] for h in hits}


def field_types(index: str) -> dict[str, str]:
    mapping = es("GET", f"/{index}/_mapping")[index]["mappings"].get("properties", {})
    return {name: prop.get("type", "object") for name, prop in mapping.items()}


@pytest.fixture
def index():
    """A fresh index name for each test, removed afterwards (with anything routed from it)."""
    name = f"tributary-test-{uuid.uuid4().hex[:8]}"
    yield name
    es("DELETE", f"/{name}*")


# --- no Spark needed ---------------------------------------------------------------------


def test_registered_with_its_contract_met():
    assert default_registry().get("sink", "elasticsearch") is ElasticsearchSink
    assert static_contract_issues(ElasticsearchSink) == []
    assert ElasticsearchSink.spark_packages == (f"org.elasticsearch:elasticsearch-spark-30_{{scala_version}}:{ELASTICSEARCH_VERSION}",)
    # No session settings besides keeping the POM's stale Spark/Scala dependencies out.
    assert ElasticsearchSink.spark_conf == {EXCLUDES_KEY: ",".join(ELASTICSEARCH_EXCLUDES)}
    assert "org.apache.spark:spark-yarn_2.12" in ELASTICSEARCH_EXCLUDES


def test_options_are_validated():
    ok = options(nodes=" es1:9200 , es2 ", index=" logs-{country} ", mode="upsert", id_column="id")
    assert ok.nodes == "es1:9200,es2" and ok.index == "logs-{country}"
    for bad, message in [
        ({"index": "orders"}, "nodes"),
        ({"nodes": NODES}, "index"),
        ({"nodes": " , ", "index": "orders"}, "at least one node"),
        ({"nodes": NODES, "index": "Orders"}, "must be lowercase"),
        ({"nodes": NODES, "index": "my orders"}, "cannot contain"),
        ({"nodes": NODES, "index": "a/b"}, "cannot contain"),
        ({"nodes": NODES, "index": "_orders"}, "cannot start with"),
        ({"nodes": NODES, "index": ".."}, "an index name is required"),
        ({"nodes": NODES, "index": "logs-{country"}, "unbalanced"),
        ({"nodes": NODES, "index": "orders", "mode": "upsert"}, r"id_column\n  Value error, required for mode 'upsert'"),
        ({"nodes": NODES, "index": "orders", "mode": "merge"}, "mode"),
        ({"nodes": NODES, "index": "logs-{country}", "mode": "overwrite"}, "overwrite needs a fixed index name"),
        ({"nodes": NODES, "index": "orders", "port": 0}, "port"),
        ({"nodes": NODES, "index": "orders", "batch_size_entries": 0}, "batch_size_entries"),
        ({"nodes": NODES, "index": "orders", "path": "/x"}, "extra"),
    ]:
        with pytest.raises(ValidationError, match=message):
            ElasticsearchSinkOptions(**bad)


def test_fields_say_what_they_depend_on():
    """The UI stars id_column for mode upsert and shows the self-signed switch only with ssl on."""
    schema = ElasticsearchSinkOptions.model_json_schema()
    assert schema["required"] == ["nodes", "index"]
    props = schema["properties"]
    assert props["id_column"]["x-when"] == {"field": "mode", "required_in": ["upsert"]}
    assert props["ssl_allow_self_signed"]["x-when"] == {"field": "ssl", "in": [True], "required_in": []}
    assert props["mode"]["enum"] == ["append", "overwrite", "upsert"]
    assert props["timestamp_format"]["enum"] == ["iso", "epoch_millis"]


def test_index_patterns_name_the_columns_they_read():
    assert options(index="orders").index_columns() == []
    assert options(index="logs-{country}-{event_ts|yyyy.MM.dd}").index_columns() == ["country", "event_ts"]
    assert options(index="logs-{meta.region}").index_columns() == ["meta"]


def test_connector_settings():
    plain = options().connector_options()
    assert plain == {
        "es.nodes": NODES,
        "es.port": "9200",
        "es.nodes.wan.only": "true",
        "es.net.ssl": "false",
        "es.index.auto.create": "true",
        "es.batch.size.entries": "1000",
        "es.batch.write.retry.count": "3",
        "es.batch.write.retry.wait": "10s",
        "es.write.operation": "index",
    }
    assert options(mode="overwrite").connector_options()["es.write.operation"] == "index"
    assert options(mode="upsert", id_column="id").connector_options()["es.write.operation"] == "upsert"

    secured = options(port=9243, ssl=True, ssl_allow_self_signed=True, user="elastic", password="pw", wan_only=False)
    conf = secured.connector_options()
    assert conf["es.port"] == "9243" and conf["es.nodes.wan.only"] == "false"
    assert conf["es.net.ssl"] == "true" and conf["es.net.ssl.cert.allow.self.signed"] == "true"
    assert conf["es.net.http.auth.user"] == "elastic" and conf["es.net.http.auth.pass"] == "pw"
    assert "es.net.ssl.cert.allow.self.signed" not in options(ssl_allow_self_signed=True).connector_options()  # ssl off

    # An API key replaces basic auth; write_options come last and may omit the es. prefix.
    keyed = options(user="elastic", password="pw", api_key="abc==", id_column="id", auto_create_index=False, batch_size_entries=50,
                    write_options={"mapping.routing": "country", "es.batch.size.bytes": "5mb", "es.nodes.wan.only": False})
    conf = keyed.connector_options()
    assert conf["es.net.http.header.Authorization"] == "ApiKey abc==" and "es.net.http.auth.user" not in conf
    assert conf["es.mapping.id"] == "id" and conf["es.index.auto.create"] == "false" and conf["es.batch.size.entries"] == "50"
    assert conf["es.mapping.routing"] == "country" and conf["es.batch.size.bytes"] == "5mb" and conf["es.nodes.wan.only"] == "false"


def test_describe():
    assert ElasticsearchSink().describe(options(index="logs-{country}", mode="upsert", id_column="id")) == f"elasticsearch -> logs-{{country}} at {NODES} (upsert)"


def test_issues_are_reported_on_their_field(tmp_path, orders_csv):
    doc = es_pipeline(tmp_path, orders_csv, "Orders", mode="upsert")
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    assert {(i.path, i.message) for i in exc.value.issues} == {
        ("sinks[0].options.index", "index names must be lowercase"),
        ("sinks[0].options.id_column", "required for mode 'upsert'"),
    }


def test_session_gets_the_jar_and_the_exclusions_but_nothing_else(tmp_path, orders_csv):
    compiled = compile_pipeline(es_pipeline(tmp_path, orders_csv, "orders"))
    assert required_packages(compiled) == [PACKAGE]
    assert required_conf(compiled) == {EXCLUDES_KEY: ",".join(ELASTICSEARCH_EXCLUDES)}
    conf = session_conf(compiled, decide_resources(compiled))
    assert conf["spark.jars.packages"].endswith(PACKAGE)
    assert "spark.sql.extensions" not in conf and not any(k.startswith("spark.sql.catalog") for k in conf)

    # The user's own exclusions add up with the sink's instead of replacing them.
    doc = es_pipeline(tmp_path, orders_csv, "orders")
    doc["runtime"] = {"dev": {"spark_conf": {EXCLUDES_KEY: "org.example:noisy,org.slf4j:slf4j-api"}}}
    conf = session_conf(compile_pipeline(doc), decide_resources(compile_pipeline(doc)))
    assert conf[EXCLUDES_KEY] == ",".join(ELASTICSEARCH_EXCLUDES) + ",org.example:noisy"

    # Next to a Delta sink: both jars, Delta's catalog, the exclusions.
    doc = es_pipeline(tmp_path, orders_csv, "orders")
    doc["sinks"].append({"id": "lake", "type": "delta", "input": "orders_clean", "options": {"path": str(tmp_path / "lake")}})
    conf = session_conf(compile_pipeline(doc), decide_resources(compile_pipeline(doc)))
    assert PACKAGE in conf["spark.jars.packages"] and "delta-spark" in conf["spark.jars.packages"]
    assert conf["spark.sql.catalog.spark_catalog"].endswith("DeltaCatalog") and conf[EXCLUDES_KEY] == ",".join(ELASTICSEARCH_EXCLUDES)


def test_design_time_checks_against_the_upstream_schema(spark):
    def ddl(text: str) -> StructType:
        return spark.createDataFrame([], text).schema

    schema = ddl("order_id INT, amount DECIMAL(10,2), country STRING, ts TIMESTAMP_NTZ, tags ARRAY<STRING>")
    sink = ElasticsearchSink()
    assert sink.validate_input(None, ddl("order_id INT, country STRING"), options(id_column="order_id", index="o-{country}")) == []
    issues = sink.validate_input(None, schema, options(id_column="id", index="orders-{region}-{ts|yyyy.MM}"))
    assert issues == [
        ("id_column", "not in the data: id (columns: order_id, amount, country, ts, tags)"),
        ("index", "refers to column(s) not in the data: region (columns: order_id, amount, country, ts, tags)"),
        ("", "the connector cannot send amount (decimal(10,2)), ts (timestamp_ntz): cast decimals to double or string "
             "and timestamp_ntz to timestamp in the SQL upstream"),
    ]
    assert not supported(ddl("a ARRAY<DECIMAL(3,1)>").fields[0].dataType)
    assert not supported(ddl("a STRUCT<b: MAP<STRING, TIMESTAMP_NTZ>>").fields[0].dataType)
    assert supported(ddl("a STRUCT<b: MAP<STRING, ARRAY<TIMESTAMP>>>").fields[0].dataType)


# --- real writes (a live cluster) ----------------------------------------------------------


def test_sink_contract(elasticsearch_spark, index):
    assert_sink_contract(elasticsearch_spark, ElasticsearchSink(), options(index=index), elasticsearch_spark.range(3))
    assert sorted(d["id"] for d in docs(index).values()) == [0, 1, 2]


def test_append_by_id_replaces_documents_on_rerun(elasticsearch_spark, tmp_path, orders_csv, index):
    doc = es_pipeline(tmp_path, orders_csv, index, id_column="order_id")
    assert run(doc, spark=elasticsearch_spark).sinks_written == ["orders_out"]
    first = docs(index)
    assert first == {
        "1": {"order_id": 1, "amount": 10.0, "country": "IN"},
        "2": {"order_id": 2, "amount": -1.0, "country": "US"},
        "4": {"order_id": 4, "amount": 7.5, "country": "USA"},
        "5": {"order_id": 5, "amount": 3.0, "country": "GB"},
        # the row without an order_id gets a generated id
        **{k: v for k, v in first.items() if k not in ("1", "2", "4", "5")},
    }
    assert len(first) == 5 and [v for k, v in first.items() if k not in ("1", "2", "4", "5")] == [{"amount": 5.0, "country": "DE"}]

    run(doc, spark=elasticsearch_spark)
    second = docs(index)
    assert len(second) == 6  # the keyed rows were replaced, the keyless one was added again
    assert {k: v for k, v in second.items() if k in ("1", "2", "4", "5")} == {k: v for k, v in first.items() if k in ("1", "2", "4", "5")}


def test_without_id_column_every_run_adds_documents(elasticsearch_spark, tmp_path, orders_csv, index):
    doc = es_pipeline(tmp_path, orders_csv, index)
    run(doc, spark=elasticsearch_spark)
    run(doc, spark=elasticsearch_spark)
    assert len(docs(index)) == 10


def test_overwrite_replaces_the_index_contents(elasticsearch_spark, tmp_path, orders_csv, index):
    run(es_pipeline(tmp_path, orders_csv, index, id_column="order_id"), spark=elasticsearch_spark)
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n1,99.0,in\n6,1.0,fr\n")
    run(pipeline(tmp_path, update, sinks=[es_sink(index, mode="overwrite", id_column="order_id")]), spark=elasticsearch_spark)
    assert docs(index) == {"1": {"order_id": 1, "amount": 99.0, "country": "IN"}, "6": {"order_id": 6, "amount": 1.0, "country": "FR"}}
    # An overwrite of an index that does not exist yet simply creates it.
    run(pipeline(tmp_path, update, sinks=[es_sink(f"{index}-new", mode="overwrite")]), spark=elasticsearch_spark)
    assert len(docs(f"{index}-new")) == 2


def test_upsert_updates_the_given_columns_and_inserts_the_rest(elasticsearch_spark, tmp_path, orders_csv, index):
    run(es_pipeline(tmp_path, orders_csv, index, id_column="order_id"), spark=elasticsearch_spark)
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n2,20.0,us\n6,60.0,fr\n")
    doc = pipeline(tmp_path, update, sinks=[es_sink(index, mode="upsert", id_column="order_id")])
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amount FROM orders"  # no country: left as it is
    run(doc, spark=elasticsearch_spark)
    after = docs(index)
    assert after["2"] == {"order_id": 2, "amount": 20.0, "country": "US"}
    assert after["6"] == {"order_id": 6, "amount": 60.0}
    assert after["1"] == {"order_id": 1, "amount": 10.0, "country": "IN"} and len(after) == 6


def test_index_pattern_routes_rows_by_column(elasticsearch_spark, tmp_path, orders_csv, index):
    doc = es_pipeline(tmp_path, orders_csv, f"{index}-{{country}}", id_column="order_id")
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amount, lower(country) AS country FROM orders"
    run(doc, spark=elasticsearch_spark)
    assert {d["country"] for d in docs(f"{index}-in").values()} == {"in"}
    assert {d["order_id"] for d in docs(f"{index}-usa").values()} == {4}
    assert len(docs(f"{index}-*")) == 5


def test_timestamps_become_dates_unless_asked_otherwise(elasticsearch_spark, index):
    df = elasticsearch_spark.sql(
        "SELECT 1 AS id, timestamp'2024-05-01 12:00:00.5' AS ts, date'2024-05-01' AS d, CAST(NULL AS TIMESTAMP) AS missing, "
        "named_struct('at', timestamp'2024-05-01 12:00:00') AS nested"
    )
    iso = with_iso_timestamps(df).collect()[0]
    assert (iso.ts, iso.d, iso.missing) == ("2024-05-01T12:00:00.500Z", "2024-05-01", None)  # the test session runs in UTC

    ElasticsearchSink().write(ctx(elasticsearch_spark), df, options(index=index, id_column="id"))
    assert docs(index)["1"] == {"id": 1, "ts": "2024-05-01T12:00:00.500Z", "d": "2024-05-01", "nested": {"at": 1714564800000}}
    assert field_types(index) == {"id": "long", "ts": "date", "d": "date", "nested": "object"}

    ElasticsearchSink().write(ctx(elasticsearch_spark), df, options(index=f"{index}-raw", id_column="id", timestamp_format="epoch_millis"))
    assert docs(f"{index}-raw")["1"] == {"id": 1, "ts": 1714564800500, "d": 1714521600000, "nested": {"at": 1714564800000}}
    assert field_types(f"{index}-raw")["ts"] == "long"


def test_auto_create_off_fails_on_a_missing_index(elasticsearch_spark, tmp_path, orders_csv, index):
    doc = es_pipeline(tmp_path, orders_csv, index, auto_create_index=False)
    with pytest.raises(Exception, match=f"(?s)Target index \\[{index}\\] does not exist.*auto-creation is disabled"):
        run(doc, spark=elasticsearch_spark)
    es("PUT", f"/{index}", {"mappings": {"properties": {"country": {"type": "keyword"}}}})
    run(doc, spark=elasticsearch_spark)
    assert len(docs(index)) == 5 and field_types(index)["country"] == "keyword"


def test_unsupported_column_types_fail_the_write_with_the_connector_message(elasticsearch_spark, index):
    df = elasticsearch_spark.sql("SELECT 1 AS id, CAST(1.5 AS DECIMAL(10, 2)) AS amount")
    with pytest.raises(Exception, match="Decimal types are not supported"):
        ElasticsearchSink().write(ctx(elasticsearch_spark), df, options(index=index))


def test_unreachable_cluster_is_reported(elasticsearch_spark, index):
    with pytest.raises(Exception, match="(?s)Cannot detect ES version.*Connection error"):
        ElasticsearchSink().write(ctx(elasticsearch_spark), elasticsearch_spark.range(1), options(index=index, nodes="localhost:1"))


def test_preview_is_a_dry_run_unless_asked_to_write(elasticsearch_spark, tmp_path, orders_csv, index):
    doc = es_pipeline(tmp_path, orders_csv, index, id_column="order_id")
    result = preview(doc, "orders_out", spark=elasticsearch_spark, limit=2)
    assert result.kind == "sink" and len(result.rows) == 2 and result.written is False
    assert result.target == f"elasticsearch -> {index} at {NODES} (append)"
    assert es("GET", f"/{index}")["error"]["type"] == "index_not_found_exception"
    written = preview(doc, "orders_out", spark=elasticsearch_spark, limit=2, write_sinks=True)
    assert written.written is True and len(docs(index)) == 2


def test_streaming_micro_batches_and_a_replay(elasticsearch_spark, tmp_path, index):
    """Each micro-batch is one bulk write. A batch replayed after a crash between the write
    and the checkpoint commit writes the same ids again, so nothing is duplicated."""
    from test_delta_streaming import crash_before_checkpoint_commit, drop_file, stream_doc

    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5}, {"id": 2, "amount": 7})
    drop_file(tmp_path, "b1.json", {"id": 2, "amount": 8}, {"id": 3, "amount": 1})
    sink = {"id": "search", "type": "elasticsearch", "input": "clean", "options": {"nodes": NODES, "index": index, "id_column": "id"}}
    doc = stream_doc(tmp_path, sink)
    query = run(doc, spark=elasticsearch_spark)
    assert not query.isActive
    assert {k: v["amount"] for k, v in docs(index).items()} == {"1": 5, "2": 8, "3": 1}

    crash_before_checkpoint_commit(tmp_path, 1)
    run(doc, spark=elasticsearch_spark)  # replays batch 1
    assert {k: v["amount"] for k, v in docs(index).items()} == {"1": 5, "2": 8, "3": 1}
    drop_file(tmp_path, "b2.json", {"id": 3, "amount": 2})
    run(doc, spark=elasticsearch_spark)
    assert {k: v["amount"] for k, v in docs(index).items()} == {"1": 5, "2": 8, "3": 2}


def test_example_pipeline_validates(tmp_path: Path):
    from conftest import EXAMPLES

    compiled = compile_pipeline(EXAMPLES / "csv_to_elasticsearch.yaml")
    assert [(s.type, s.options.index, s.options.mode) for s in compiled.spec.sinks] == [
        ("elasticsearch", "orders", "append"), ("elasticsearch", "revenue_by_country", "overwrite"),
    ]
    assert compiled.spec.sinks[0].options.nodes == "localhost:9200"  # from the connection
    assert required_packages(compiled) == [PACKAGE]
