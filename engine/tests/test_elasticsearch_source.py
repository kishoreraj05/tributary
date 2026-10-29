"""The Elasticsearch source: options, connector settings and reads from a real cluster.

Tests that read need the ES-Hadoop jar and a cluster at TRIBUTARY_TEST_ES_NODES (see
``es_spark`` in conftest); the others run without Spark.
"""

from __future__ import annotations

import json
import urllib.request
import uuid

import pytest
from pydantic import ValidationError

from conftest import ES_NODES, pipeline
from tributary import analyze, compile_pipeline, preview, run
from tributary.core import ResolvedSchema, RunContext
from tributary.errors import SpecError
from tributary.plugins.sources.elasticsearch import (
    ES_HADOOP_EXCLUDES,
    ES_HADOOP_VERSION,
    ElasticsearchSource,
    ElasticsearchSourceOptions,
    reader_options,
)
from tributary.registry import default_registry
from tributary.runtime import decide_resources, required_conf, required_packages, session_conf
from tributary.testing import assert_source_contract, static_contract_issues

ORDERS = [
    {"order_id": 1, "amount": 10.0, "country": "in", "order_ts": "2026-10-01T10:00:00Z", "tags": ["new", "web"]},
    {"order_id": 2, "amount": -1.0, "country": "us", "order_ts": "2026-10-01T11:30:00Z", "tags": ["web"]},
    {"amount": 5.0, "country": "de", "order_ts": "2026-10-02T10:45:00Z", "tags": []},
    {"order_id": 4, "amount": 7.5, "country": "USA", "order_ts": "2026-10-02T13:20:00Z", "tags": ["app"]},
    {"order_id": 5, "amount": 3.0, "country": "gb", "order_ts": "2026-10-03T16:40:00Z", "tags": ["web", "promo"]},
]
MAPPING = {
    "properties": {
        "order_id": {"type": "integer"},
        "amount": {"type": "double"},
        "country": {"type": "keyword"},
        "order_ts": {"type": "date"},
        "tags": {"type": "keyword"},
    }
}


def es_source(index: str, **options) -> dict:
    return {"id": "orders_src", "type": "elasticsearch", "options": {"nodes": ES_NODES, "index": index, **options}}


def es_pipeline(tmp_path, index: str, **options) -> dict:
    return pipeline(tmp_path, tmp_path, sources=[es_source(index, **options)])


def _http(method: str, path: str, body=None) -> dict:
    base = ES_NODES if "://" in ES_NODES else f"http://{ES_NODES}"
    data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    content_type = "application/x-ndjson" if isinstance(body, bytes) else "application/json"
    request = urllib.request.Request(f"{base}{path}", data=data, method=method, headers={"Content-Type": content_type})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


@pytest.fixture
def orders_index(es_spark):
    """A fresh index holding ORDERS, dropped afterwards."""
    name = f"tributary_test_{uuid.uuid4().hex[:8]}"
    _http("PUT", f"/{name}", {"mappings": MAPPING})
    lines = []
    for doc in ORDERS:
        action = {"index": {"_index": name, **({"_id": str(doc["order_id"])} if "order_id" in doc else {})}}
        lines += [json.dumps(action), json.dumps(doc)]
    result = _http("POST", "/_bulk?refresh=true", ("\n".join(lines) + "\n").encode())
    assert not result["errors"], result
    yield name
    _http("DELETE", f"/{name}")


# --- no Spark needed ---------------------------------------------------------------------


def test_registered_with_its_contract_met():
    assert default_registry().get("source", "elasticsearch") is ElasticsearchSource
    assert static_contract_issues(ElasticsearchSource) == []
    assert ElasticsearchSource.capabilities() == ["batch"]  # the connector has no streaming reader


def test_options_are_validated():
    ok = ElasticsearchSourceOptions(nodes=" es1:9200, es2:9200 ", index="orders-*", query=' {"query": {"match_all": {}}} ')
    assert ok.nodes == "es1:9200, es2:9200" and ok.query == '{"query": {"match_all": {}}}'
    assert ElasticsearchSourceOptions(nodes="es", index="o", query="  ").query is None
    assert ElasticsearchSourceOptions(nodes="${secret:es/nodes}", index="o", query="${env.query}").query == "${env.query}"
    for bad, message in [
        ({"index": "orders"}, "nodes\n  Field required"),
        ({"nodes": "es"}, "index\n  Field required"),
        ({"nodes": " ", "index": "o"}, "nodes\n  Value error, required"),
        ({"nodes": "es", "index": "o", "use_dummy_data": True}, "Extra inputs are not permitted"),
        ({"nodes": "es", "index": "o", "query": "country:us"}, "must be a URI query starting with"),
        ({"nodes": "es", "index": "o", "query": '{"query": }'}, "not valid JSON"),
        ({"nodes": "es", "index": "o", "scroll_size": 0}, "greater than or equal to 1"),
        ({"nodes": "es", "index": "o", "port": 70000}, "less than or equal to 65535"),
        ({"nodes": "es", "index": "o", "es_query": "x"}, "Extra inputs are not permitted"),
    ]:
        with pytest.raises(ValidationError, match=message):
            ElasticsearchSourceOptions(**bad)


def test_every_option_maps_to_a_connector_setting():
    opts = ElasticsearchSourceOptions(
        nodes="es1,es2", port=9243, user="elastic", password="pw", ssl=True, ssl_allow_self_signed=True, wan_only=False,
        index="orders", query="?q=country:us", fields=["order_id", "amount"], array_fields=["tags"],
        include_metadata=True, scroll_size=500,
        read_options={"read.field.exclude": "secret", "es.mapping.date.rich": False, "es.scroll.size": 50},
    )
    assert reader_options(opts) == {
        "es.nodes": "es1,es2",
        "es.port": "9243",
        "es.net.http.auth.user": "elastic",
        "es.net.http.auth.pass": "pw",
        "es.net.ssl": "true",
        "es.net.ssl.cert.allow.self.signed": "true",
        "es.nodes.wan.only": "false",
        "es.query": "?q=country:us",
        "es.read.field.include": "order_id,amount",
        "es.read.field.as.array.include": "tags",
        "es.read.metadata": "true",
        "es.scroll.size": "50",  # read_options win, and get the es. prefix when it is missing
        "es.read.field.exclude": "secret",
        "es.mapping.date.rich": "false",
    }
    minimal = reader_options(ElasticsearchSourceOptions(nodes="es", index="o"))
    assert minimal == {"es.nodes": "es", "es.nodes.wan.only": "true", "es.net.ssl": "false", "es.read.metadata": "false", "es.scroll.size": "1000"}


def test_output_schema_is_known_only_when_declared():
    from pyspark.sql.types import IntegerType, MapType, StringType, StructField, StructType

    source = ElasticsearchSource()
    assert source.output_schema(ElasticsearchSourceOptions(nodes="es", index="o"), None) is None
    declared = ResolvedSchema(struct=StructType([StructField("order_id", IntegerType())]))
    assert source.output_schema(ElasticsearchSourceOptions(nodes="es", index="o"), declared) == declared.struct
    with_meta = source.output_schema(ElasticsearchSourceOptions(nodes="es", index="o", include_metadata=True), declared)
    assert with_meta == StructType([StructField("order_id", IntegerType()), StructField("_metadata", MapType(StringType(), StringType()))])


def test_issues_are_reported_on_their_field(tmp_path):
    with pytest.raises(SpecError) as exc:
        compile_pipeline(es_pipeline(tmp_path, "orders", query="country:us"))
    assert [(i.path, i.message) for i in exc.value.issues] == [
        ("sources[0].options.query", "must be a URI query starting with '?' (e.g. ?q=country:us) or a JSON Query DSL body")
    ]


def test_connection_of_type_elasticsearch_supplies_the_cluster(tmp_path):
    doc = es_pipeline(tmp_path, "orders")
    doc["sources"][0] = {"id": "orders_src", "type": "elasticsearch", "connection": "es_main", "options": {"index": "orders"}}
    doc["environments"]["dev"]["connections"] = {"es_main": {"type": "elasticsearch", "nodes": "es:9200", "user": "u", "password": "p", "ssl": True}}
    opts = compile_pipeline(doc).spec.sources[0].options
    assert (opts.nodes, opts.user, opts.password, opts.ssl, opts.index) == ("es:9200", "u", "p", True, "orders")
    doc["environments"]["dev"]["connections"]["es_main"]["type"] = "kafka"
    with pytest.raises(SpecError, match="connection 'es_main' is 'kafka', node type is 'elasticsearch'"):
        compile_pipeline(doc)


def test_session_gets_the_connector_and_drops_its_spark_33_baggage(tmp_path):
    compiled = compile_pipeline(es_pipeline(tmp_path, "orders"))
    assert required_packages(compiled) == [f"org.elasticsearch:elasticsearch-spark-30_2.12:{ES_HADOOP_VERSION}"]
    assert required_conf(compiled) == {"spark.jars.excludes": ",".join(ES_HADOOP_EXCLUDES)}
    conf = session_conf(compiled, decide_resources(compiled))
    assert f"elasticsearch-spark-30_2.12:{ES_HADOOP_VERSION}" in conf["spark.jars.packages"]
    assert "org.apache.spark:spark-yarn_2.12" in conf["spark.jars.excludes"]

    # The user's own excludes add to the plugin's instead of replacing them.
    doc = es_pipeline(tmp_path, "orders")
    doc["runtime"] = {"dev": {"spark_conf": {"spark.jars.excludes": "com.example:noisy"}}}
    compiled = compile_pipeline(doc)
    excludes = session_conf(compiled, decide_resources(compiled))["spark.jars.excludes"].split(",")
    assert excludes == [*ES_HADOOP_EXCLUDES, "com.example:noisy"]


# --- reads from a cluster ------------------------------------------------------------------


def test_source_contract(es_spark, orders_index):
    assert_source_contract(es_spark, ElasticsearchSource(), ElasticsearchSourceOptions(nodes=ES_NODES, index=orders_index), limit=2)


def test_reads_documents_with_the_mapping_as_schema(es_spark, orders_index):
    ctx = RunContext(spark=es_spark, registry=default_registry(), pipeline_id="t", env="test")
    df = ElasticsearchSource().read(ctx, ElasticsearchSourceOptions(nodes=ES_NODES, index=orders_index, array_fields=["tags"]), None)
    assert dict(df.dtypes) == {"amount": "double", "country": "string", "order_id": "int", "order_ts": "timestamp", "tags": "array<string>"}
    rows = {r.order_id: (r.amount, r.country, r.tags) for r in df.collect()}
    assert rows == {1: (10.0, "in", ["new", "web"]), 2: (-1.0, "us", ["web"]), None: (5.0, "de", []), 4: (7.5, "USA", ["app"]), 5: (3.0, "gb", ["web", "promo"])}


def test_query_fields_and_metadata(es_spark, orders_index):
    ctx = RunContext(spark=es_spark, registry=default_registry(), pipeline_id="t", env="test")
    opts = ElasticsearchSourceOptions(
        nodes=ES_NODES, index=orders_index, fields=["order_id", "amount"], include_metadata=True,
        query='{"query": {"range": {"amount": {"gte": 5}}}}',
    )
    df = ElasticsearchSource().read(ctx, opts, None)
    assert df.columns == ["amount", "order_id", "_metadata"]
    rows = {r._metadata["_id"]: (r.order_id, r.amount) for r in df.collect()}
    assert rows == {"1": (1, 10.0), "4": (4, 7.5)} | {k: v for k, v in rows.items() if v == (None, 5.0)}
    assert len(rows) == 3 and all(r["_index"] == orders_index for r in df.select("_metadata").rdd.map(lambda r: r[0]).collect())

    uri = ElasticsearchSource().read(ctx, ElasticsearchSourceOptions(nodes=ES_NODES, index=orders_index, query="?q=country:us", array_fields=["tags"]), None)
    assert [r.order_id for r in uri.collect()] == [2]


def test_declared_schema_replaces_the_mapping(es_spark, tmp_path, orders_index):
    """A schema block fixes names and types without a mapping lookup; the metadata column is
    still added when asked for, which is what design-time analysis predicts."""
    doc = es_pipeline(tmp_path, orders_index, include_metadata=True)
    doc["sources"][0]["schema"] = {"kind": "ddl", "ddl": "order_id LONG, amount DECIMAL(10,2), country STRING"}
    doc["transforms"][0]["options"]["sql"] = "SELECT _metadata['_id'] AS doc_id, order_id, amount, upper(country) AS country FROM orders"
    analysis = analyze(doc, spark=es_spark)
    assert analysis["orders_src"].ok and [f["name"] for f in analysis["orders_src"].schema["fields"]] == ["order_id", "amount", "country", "_metadata"]
    assert [f["name"] for f in analysis["orders_clean"].schema["fields"]] == ["doc_id", "order_id", "amount", "country"]

    result = preview(doc, "orders_clean", spark=es_spark, limit=10)
    assert {f["name"]: f["type"] for f in result.schema["fields"]} == {"doc_id": "string", "order_id": "long", "amount": "decimal(10,2)", "country": "string"}
    assert {r["doc_id"]: (r["order_id"], r["amount"], r["country"]) for r in result.rows if r["doc_id"] == "4"} == {"4": (4, "7.50", "USA")}


def test_pipeline_from_elasticsearch_to_parquet_with_a_quality_gate(es_spark, tmp_path, orders_index):
    doc = es_pipeline(tmp_path, orders_index)
    doc["quality"] = [{
        "id": "dq", "attach_to": "orders_clean", "action": "quarantine",
        "checks": [{"type": "not_null", "column": "order_id"}, {"type": "range", "column": "amount", "min": 0}, {"type": "expression", "expr": "length(country) = 2"}],
        "quarantine_sink": {"type": "file", "options": {"path": str(tmp_path / "out" / "bad"), "mode": "overwrite"}},
    }]
    summary = run(doc, spark=es_spark)
    assert summary.sinks_written == ["orders_out"]
    assert {r.order_id for r in es_spark.read.parquet(str(tmp_path / "out" / "orders")).collect()} == {1, 5}
    bad = {r.order_id: r._dq_failed_checks for r in es_spark.read.parquet(str(tmp_path / "out" / "bad")).collect()}
    assert bad == {2: ["range_2"], None: ["not_null_1"], 4: ["expression_3"]}


def test_preview_is_bounded(es_spark, tmp_path, orders_index):
    result = preview(es_pipeline(tmp_path, orders_index, array_fields=["tags"]), "orders_src", spark=es_spark, limit=2)
    assert len(result.rows) == 2 and set(result.rows[0]) == {"amount", "country", "order_id", "order_ts", "tags"}


# --- dummy data in the local environment: no cluster at all --------------------------------

DUMMY = [
    {"order_id": 1, "amount": 10.0, "country": "in", "tags": ["new", "web"]},
    {"order_id": 2, "amount": -1.0, "country": "us", "tags": ["web"]},
    {"amount": 5.0, "country": "de", "tags": []},
    {"order_id": 4, "amount": 7.5, "country": "USA"},
    {"order_id": 5, "amount": 3.0, "country": "gb"},
]


def dummy_pipeline(tmp_path) -> dict:
    """An Elasticsearch source with no cluster settings at all, standing in with DUMMY."""
    doc = pipeline(tmp_path, tmp_path, sources=[{
        "id": "orders_src", "type": "elasticsearch", "options": {},
        "schema": {"kind": "ddl", "ddl": "order_id INT, amount DOUBLE, country STRING"},
        "dummy": {"enabled": True, "data": "\n".join(json.dumps(d) for d in DUMMY)},
    }])
    doc["environments"] = {"local": doc["environments"]["dev"]}
    return doc


def test_dummy_data_needs_no_connector(tmp_path):
    compiled = compile_pipeline(dummy_pipeline(tmp_path), env="local")
    assert required_packages(compiled) == [] and required_conf(compiled) == {}


def test_pipeline_runs_on_dummy_data_without_any_cluster(spark, tmp_path):
    doc = dummy_pipeline(tmp_path)
    doc["quality"] = [{
        "id": "dq", "attach_to": "orders_clean", "action": "quarantine",
        "checks": [{"type": "not_null", "column": "order_id"}, {"type": "range", "column": "amount", "min": 0}, {"type": "expression", "expr": "length(country) = 2"}],
        "quarantine_sink": {"type": "file", "options": {"path": str(tmp_path / "out" / "bad"), "mode": "overwrite"}},
    }]
    summary = run(doc, env="local", spark=spark)
    assert summary.sinks_written == ["orders_out"]
    assert {r.order_id for r in spark.read.parquet(str(tmp_path / "out" / "orders")).collect()} == {1, 5}
    bad = {r.order_id: r._dq_failed_checks for r in spark.read.parquet(str(tmp_path / "out" / "bad")).collect()}
    assert bad == {2: ["range_2"], None: ["not_null_1"], 4: ["expression_3"]}
