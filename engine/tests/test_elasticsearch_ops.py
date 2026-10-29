"""The Elasticsearch sink under operational conditions, against a live cluster:

1. idempotency: re-runs, replays, task failures half-way, and where duplicates come from;
2. mapping conflicts and dynamic mappings, at run time and ahead of time;
3. bulk requests the cluster rejects (429) or refuses for good, and partial success;
4. rollover behind a write alias (what ILM does), including while a write is in flight.

The cluster is the one the other Elasticsearch tests use (``elasticsearch_spark``); the
429 scenarios go through a tampering proxy (``es_proxy``) because a single-node cluster
cannot be made to reject bulk requests on cue.
"""

from __future__ import annotations

import threading
import time

import pytest

from conftest import ES_URL, pipeline
from es_proxy import FlakyElasticsearch
from test_elasticsearch import ctx, docs, es, es_pipeline, es_sink, field_types, options
from test_elasticsearch import index  # noqa: F401 - a fixture, used by name
from tributary import run
from tributary.plugins.sinks.elasticsearch import ElasticsearchSink, ElasticsearchSinkOptions, describe_target, mapping_issues


def versions(name: str) -> dict[str, int]:
    """{_id: _version}: a version above 1 means the document was indexed more than once."""
    es("POST", f"/{name}/_refresh")
    return {h["_id"]: h["_version"] for h in es("GET", f"/{name}/_search?size=10000&version=true")["hits"]["hits"]}


def write(spark, df, **opts) -> None:
    ElasticsearchSink().write(ctx(spark), df, options(**opts))


# --- 1. idempotency and duplicates ---------------------------------------------------------


def test_rerun_with_an_id_column_replaces_without_one_it_duplicates(elasticsearch_spark, tmp_path, orders_csv, index):
    keyed, keyless = es_pipeline(tmp_path, orders_csv, index, id_column="order_id"), es_pipeline(tmp_path, orders_csv, f"{index}-nokey")
    for _ in range(3):
        run(keyed, spark=elasticsearch_spark)
        run(keyless, spark=elasticsearch_spark)
    keyed_docs = versions(index)
    assert {k: v for k, v in keyed_docs.items() if k in ("1", "2", "4", "5")} == {"1": 3, "2": 3, "4": 3, "5": 3}  # replaced each time
    assert len(keyed_docs) == 4 + 3  # the row with a null order_id gets a generated id every time: the one leak
    assert len(versions(f"{index}-nokey")) == 15


def test_upsert_is_idempotent_and_partial(elasticsearch_spark, index):
    first = elasticsearch_spark.createDataFrame([(1, "a", 1.0), (2, "b", 2.0)], "id INT, name STRING, score DOUBLE")
    write(elasticsearch_spark, first, index=index, id_column="id")
    change = elasticsearch_spark.createDataFrame([(2, 2.5), (3, 3.0)], "id INT, score DOUBLE")
    for _ in range(2):
        write(elasticsearch_spark, change, index=index, id_column="id", mode="upsert")
    assert docs(index) == {"1": {"id": 1, "name": "a", "score": 1.0}, "2": {"id": 2, "name": "b", "score": 2.5}, "3": {"id": 3, "score": 3.0}}
    assert versions(index)["2"] == 2  # the second upsert changed nothing: Elasticsearch detected the no-op, no new version


def test_a_write_that_fails_half_way_leaves_a_partial_index_that_a_rerun_completes(elasticsearch_spark, index):
    """Bulk requests are independent: the ones before the failing one are indexed. With an
    id_column the re-run rewrites exactly the same documents; without one it adds copies."""
    good = elasticsearch_spark.range(10).selectExpr("id", "cast(id as double) AS amount").coalesce(1)
    write(elasticsearch_spark, good, index=index, id_column="id", batch_size_entries=2)
    poison = elasticsearch_spark.range(10).selectExpr("id", "if(id = 7, 'seven', cast(id as string)) AS amount").coalesce(1)
    with pytest.raises(Exception, match="failed to parse field \\[amount\\] of type \\[float\\] in document with id '7'"):
        write(elasticsearch_spark, poison, index=index, id_column="id", batch_size_entries=2)
    after = versions(index)
    assert {k: v for k, v in after.items() if k in ("0", "1", "2", "3", "4", "5", "6")} == {str(i): 2 for i in range(7)}  # the bulks before
    assert after["7"] == 1 and after["8"] == 1 and after["9"] == 1  # 7 was refused, 8/9 never sent
    write(elasticsearch_spark, good, index=index, id_column="id", batch_size_entries=2)
    assert len(versions(index)) == 10 and {d["amount"] for d in docs(index).values()} == {float(i) for i in range(10)}

    nokey = f"{index}-nokey"
    write(elasticsearch_spark, good.withColumnRenamed("id", "n"), index=nokey, batch_size_entries=2)
    with pytest.raises(Exception, match="seven"):
        write(elasticsearch_spark, poison.withColumnRenamed("id", "n"), index=nokey, batch_size_entries=2)
    write(elasticsearch_spark, good.withColumnRenamed("id", "n"), index=nokey, batch_size_entries=2)
    assert len(versions(nokey)) == 10 + 7 + 10  # the failed attempt's 7 documents stay as copies: why id_column matters


# --- 2. mapping conflicts and dynamic mappings ---------------------------------------------


def test_a_value_that_does_not_fit_the_mapping_fails_the_run(elasticsearch_spark, index):
    write(elasticsearch_spark, elasticsearch_spark.sql("SELECT 1 AS id, 1.5D AS amount, true AS flag, named_struct('a', 1) AS meta"), index=index, id_column="id")
    assert field_types(index) == {"id": "long", "amount": "float", "flag": "boolean", "meta": "object"}
    for sql, message in [
        ("SELECT 2 AS id, 'abc' AS amount", "failed to parse field \\[amount\\] of type \\[float\\]"),
        ("SELECT 2 AS id, 1 AS flag", "failed to parse field \\[flag\\] of type \\[boolean\\]"),
        ("SELECT 2 AS id, 'x' AS meta", "object mapping for \\[meta\\] tried to parse field \\[meta\\] as object, but found a concrete value"),
        ("SELECT 2 AS id, named_struct('a', 1) AS amount", "Cannot parse object as number"),
    ]:
        with pytest.raises(Exception, match=message):
            write(elasticsearch_spark, elasticsearch_spark.sql(sql), index=index, id_column="id")
    # Elasticsearch coerces what it can parse: numeric text into a number, "true" into a boolean.
    write(elasticsearch_spark, elasticsearch_spark.sql("SELECT 3 AS id, '2.5' AS amount, 'true' AS flag"), index=index, id_column="id")
    assert docs(index)["3"] == {"id": 3, "amount": "2.5", "flag": "true"}  # stored as sent, indexed as float/boolean


def test_dynamic_mapping_adds_fields_strict_refuses_them_false_ignores_them(elasticsearch_spark, index):
    write(elasticsearch_spark, elasticsearch_spark.sql("SELECT 1 AS id"), index=index, id_column="id")
    write(elasticsearch_spark, elasticsearch_spark.sql("SELECT 2 AS id, 'x' AS extra"), index=index, id_column="id")
    assert field_types(index) == {"id": "long", "extra": "text"}

    es("PUT", f"/{index}-strict", {"mappings": {"dynamic": "strict", "properties": {"id": {"type": "long"}}}})
    with pytest.raises(Exception, match="strict_dynamic_mapping_exception: mapping set to strict, dynamic introduction of \\[extra\\]"):
        write(elasticsearch_spark, elasticsearch_spark.sql("SELECT 2 AS id, 'x' AS extra"), index=f"{index}-strict", id_column="id")

    es("PUT", f"/{index}-false", {"mappings": {"dynamic": False, "properties": {"id": {"type": "long"}}}})
    write(elasticsearch_spark, elasticsearch_spark.sql("SELECT 2 AS id, 'x' AS extra"), index=f"{index}-false", id_column="id")
    assert docs(f"{index}-false")["2"] == {"id": 2, "extra": "x"} and field_types(f"{index}-false") == {"id": "long"}  # stored, not searchable


def test_detected_dates_are_the_classic_trap(elasticsearch_spark, index):
    """Dynamic mapping turns a text column whose first value looks like a date into a date
    field; a later value that does not parse is refused. Declare the mapping, or send dates
    as dates (the sink does, for TIMESTAMP/DATE columns)."""
    write(elasticsearch_spark, elasticsearch_spark.sql("SELECT 1 AS id, '2024-01-01' AS note"), index=index, id_column="id")
    assert field_types(index)["note"] == "date"
    with pytest.raises(Exception, match="failed to parse date field \\[yesterday\\]"):
        write(elasticsearch_spark, elasticsearch_spark.sql("SELECT 2 AS id, 'yesterday' AS note"), index=index, id_column="id")


def test_mapping_conflicts_are_reported_ahead_of_time(elasticsearch_spark, index):
    """Deep validation reads the target's mapping with the sink's credentials and applies
    the same rules, so the form shows the problem before a run (or a micro-batch) hits it."""
    write(elasticsearch_spark, elasticsearch_spark.sql(
        "SELECT 1 AS id, 1.5D AS amount, true AS flag, named_struct('a', 1) AS meta, 'x' AS tag, timestamp'2024-01-01 00:00:00' AS ts"
    ), index=index, id_column="id")
    sink = ElasticsearchSink()
    same = elasticsearch_spark.sql("SELECT 1 AS id, 2.5D AS amount, false AS flag, named_struct('a', 2) AS meta, 'y' AS tag, timestamp'2024-01-02 00:00:00' AS ts").schema
    assert sink.validate_input(None, same, options(index=index, id_column="id")) == []
    widened = elasticsearch_spark.sql("SELECT CAST(1 AS BIGINT) AS id, '2.5' AS amount, 'true' AS flag, array(1, 2) AS ids").schema
    assert sink.validate_input(None, widened, options(index=index, id_column="id")) == []  # text is coerced; new fields are fine

    bad = elasticsearch_spark.sql(
        "SELECT 1 AS id, 'abc' AS amount, 1 AS flag, 'x' AS meta, named_struct('a', 1) AS tag, true AS ts"
    ).schema
    [(field, message)] = sink.validate_input(None, bad, options(index=index, id_column="id"))
    assert field == ""
    assert message == (
        f"flag is int in the data but boolean in index {index}; meta is string in the data but object in index {index}; "
        f"tag is struct<a:int> in the data but text in index {index}; ts is boolean in the data but date in index {index}; "
        "cast it in the SQL upstream or reindex with another mapping (Elasticsearch never changes a field's type)"
    )
    # As epoch milliseconds a timestamp is a number: fine for a date field, wrong for a boolean one.
    epoch = options(index=index, id_column="id", timestamp_format="epoch_millis")
    assert sink.validate_input(None, elasticsearch_spark.sql("SELECT 1 AS id, timestamp'2024-01-01 00:00:00' AS ts").schema, epoch) == []
    assert sink.validate_input(None, elasticsearch_spark.sql("SELECT 1 AS id, timestamp'2024-01-01 00:00:00' AS flag").schema, epoch) == [
        ("", f"flag is timestamp in the data but boolean in index {index}; cast it in the SQL upstream or reindex with another mapping (Elasticsearch never changes a field's type)")
    ]

    es("PUT", f"/{index}-strict", {"mappings": {"dynamic": "strict", "properties": {"id": {"type": "long"}}}})
    es("PUT", f"/{index}-false", {"mappings": {"dynamic": False, "properties": {"id": {"type": "long"}}}})
    wider = elasticsearch_spark.sql("SELECT 1 AS id, 'x' AS extra, 2 AS more").schema
    assert sink.validate_input(None, wider, options(index=f"{index}-strict")) == [
        ("", f"index {index}-strict has no field(s) extra, more and refuses new ones (dynamic: strict): add them to the mapping or drop them upstream")
    ]
    assert sink.validate_input(None, wider, options(index=f"{index}-false")) == [
        ("", f"index {index}-false has no field(s) extra, more and does not index new ones (dynamic: false): they would be stored but not searchable")
    ]
    # A missing target is only a problem when the sink may not create it; an unreachable cluster is not reported here.
    assert sink.validate_input(None, wider, options(index=f"{index}-missing")) == []
    assert sink.validate_input(None, wider, options(index=f"{index}-missing", auto_create_index=False)) == [
        ("index", f"'{index}-missing' does not exist and auto_create_index is off: create the index (for rollover, the "
                  "first index with the write alias) or turn auto_create_index on")
    ]
    assert sink.validate_input(None, wider, options(index=index, nodes="localhost:1")) == []
    assert sink.validate_input(None, wider, options(index=f"{index}-{{extra}}")) == []  # patterns: nothing to look up


def test_mapping_rules_without_a_cluster():
    from pyspark.sql.types import DoubleType, LongType, StructField, StructType

    opts = ElasticsearchSinkOptions(nodes="x", index="t")
    schema = StructType([StructField("a", LongType()), StructField("b", DoubleType())])
    assert mapping_issues(schema, {}, opts, "t") == []
    assert mapping_issues(schema, {"properties": {"a": {"type": "keyword"}, "b": {"type": "date"}}}, opts, "t") == []  # numbers fit text and dates
    assert mapping_issues(schema, {"properties": {"a": {"type": "boolean"}, "b": {"type": "ip"}}}, opts, "t") == [
        "a is bigint in the data but boolean in index t; cast it in the SQL upstream or reindex with another mapping (Elasticsearch never changes a field's type)"
    ]
    assert mapping_issues(schema, {"dynamic": "strict", "properties": {"a": {"type": "long"}}}, opts, "t") == [
        "index t has no field(s) b and refuses new ones (dynamic: strict): add them to the mapping or drop them upstream"
    ]


# --- 3. bulk rejections (429) and partial success -------------------------------------------


def test_rejected_documents_are_retried_in_place_and_indexed_once(elasticsearch_spark, index):
    """A shard that is busy rejects some documents of a bulk request with 429; the connector
    resends just those after retry_wait. Every document ends at version 1."""
    df = elasticsearch_spark.range(20).selectExpr("id", "concat('v', id) AS v").coalesce(1)
    with FlakyElasticsearch(ES_URL, ["items429", "items429", "ok"]) as cluster:
        write(elasticsearch_spark, df, nodes=cluster.nodes, index=index, id_column="id", batch_size_entries=10, retry_wait="1s")
        assert [n for _, n in cluster.bulks] == [10, 5, 2, 10]  # 10 sent, 5 refused and resent, 2 of those again, then the rest
    assert versions(index) == {str(i): 1 for i in range(20)}


def test_rejections_that_do_not_stop_fail_the_task_after_the_retries(elasticsearch_spark, index):
    df = elasticsearch_spark.range(20).selectExpr("id", "concat('v', id) AS v").coalesce(1)
    with FlakyElasticsearch(ES_URL, ["items429"] * 20) as cluster:
        with pytest.raises(Exception, match="(?s)Could not write all entries for bulk operation \\[1/10\\].*es_rejected_execution_exception"):
            write(elasticsearch_spark, df, nodes=cluster.nodes, index=index, id_column="id", batch_size_entries=10, retry_count=2, retry_wait="1s")
        assert [n for _, n in cluster.bulks] == [10, 5, 2]  # the first attempt and two retries, then the task gave up
    partial = versions(index)
    assert len(partial) == 9 and set(partial.values()) == {1}  # what got in stayed in, once
    # Recovery is a re-run (or Spark's task retry): with an id_column it completes the index in place.
    write(elasticsearch_spark, df, index=index, id_column="id", batch_size_entries=10)
    assert len(versions(index)) == 20


def test_a_whole_request_refused_with_429_fails_the_task_at_once(elasticsearch_spark, index):
    """The coordinating node's queue being full refuses the request itself (not its items):
    the connector does not retry that. The bulk requests before it are in, so the retry
    Spark makes (spark.task.maxFailures) or a re-run needs an id_column to be harmless."""
    df = elasticsearch_spark.range(20).selectExpr("id", "concat('v', id) AS v").coalesce(1)
    with FlakyElasticsearch(ES_URL, ["ok", "429"]) as cluster:
        with pytest.raises(Exception, match="es_rejected_execution_exception: rejected execution of coordinating operation"):
            write(elasticsearch_spark, df, nodes=cluster.nodes, index=index, id_column="id", batch_size_entries=10, retry_wait="1s")
        assert cluster.bulks == [("ok", 10), ("429", 10)]
    assert len(versions(index)) == 10
    write(elasticsearch_spark, df, index=index, id_column="id", batch_size_entries=10)
    assert versions(index) == {**{str(i): 2 for i in range(10)}, **{str(i): 1 for i in range(10, 20)}}


def test_documents_refused_for_good_can_be_skipped(elasticsearch_spark, index, caplog):
    write(elasticsearch_spark, elasticsearch_spark.sql("SELECT 0 AS id, 1.0D AS amount"), index=index, id_column="id")
    poison = elasticsearch_spark.range(1, 11).selectExpr("id", "if(id % 4 = 0, 'bad', cast(id as string)) AS amount").coalesce(1)
    with pytest.raises(Exception, match="Could not write all entries for bulk operation \\[1/5\\]"):
        write(elasticsearch_spark, poison, index=index, id_column="id", batch_size_entries=5)
    assert sorted(versions(index)) == ["0", "1", "2", "3", "5"]  # the first bulk minus its bad document; the second never sent

    write(elasticsearch_spark, poison, index=index, id_column="id", batch_size_entries=5, rejected_documents="skip")
    assert sorted(versions(index), key=int) == ["0", "1", "2", "3", "5", "6", "7", "9", "10"]  # 4 and 8 skipped, the rest in


def test_retry_settings_are_validated_and_passed_on():
    conf = options(retry_count=-1, retry_wait="500ms", rejected_documents="skip").connector_options()
    assert conf["es.batch.write.retry.count"] == "-1" and conf["es.batch.write.retry.wait"] == "500ms"
    assert conf["es.write.rest.error.handlers"] == "log" and conf["es.write.rest.error.handler.log.logger.name"] == "tributary.elasticsearch.rejected"
    assert "es.write.rest.error.handlers" not in options().connector_options()
    for bad in [{"retry_count": -2}, {"retry_wait": "soon"}, {"retry_wait": "10 s"}, {"rejected_documents": "ignore"}]:
        with pytest.raises(ValueError):
            options(**bad)


# --- 4. rollover behind a write alias --------------------------------------------------------


def bootstrap(alias: str) -> str:
    """The first index of a rollover series, with the write alias: what ILM's rollover
    action needs to exist before the first write."""
    first = f"{alias}-000001"
    es("PUT", f"/{first}", {"aliases": {alias: {"is_write_index": True}}})
    return first


def rollover(alias: str) -> str:
    body = es("POST", f"/{alias}/_rollover", {})
    assert body.get("rolled_over"), body
    return body["new_index"]


def per_index(alias: str) -> dict[str, int]:
    """Documents behind the alias, by backing index."""
    es("POST", f"/{alias}/_refresh")
    body = es("POST", f"/{alias}/_search", {"size": 0, "aggs": {"by_index": {"terms": {"field": "_index", "size": 1000}}}})
    return {b["key"]: b["doc_count"] for b in body["aggregations"]["by_index"]["buckets"]}


@pytest.fixture
def alias(index):
    """An alias name; the index fixture removes every index starting with it."""
    return index


def test_writes_follow_the_write_alias_across_rollovers(elasticsearch_spark, tmp_path, orders_csv, alias):
    first = bootstrap(alias)
    doc = es_pipeline(tmp_path, orders_csv, alias, id_column="order_id", auto_create_index=False)
    run(doc, spark=elasticsearch_spark)
    second = rollover(alias)
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n6,1.0,fr\n7,2.0,de\n")
    run(pipeline(tmp_path, update, sinks=[es_sink(alias, id_column="order_id", auto_create_index=False)]), spark=elasticsearch_spark)
    assert per_index(alias) == {first: 5, second: 2}
    assert ElasticsearchSink().validate_input(None, elasticsearch_spark.range(1).schema, options(index=alias, auto_create_index=False)) == []
    assert describe_target(options(index=alias))["write_index"] == second


def test_rollover_while_a_write_is_in_flight_loses_nothing(elasticsearch_spark, alias):
    """ILM rolls the alias over whenever its conditions are met, in the middle of a Spark
    write as likely as not. The switch is atomic on the cluster: each bulk request lands
    whole in whichever index is the write index at that moment."""
    bootstrap(alias)
    rows = 9_000  # within one search page (max_result_window), so versions() sees every document
    df = elasticsearch_spark.range(rows).selectExpr("id", "concat('event-', id) AS event").repartition(2)
    stop, rolled = threading.Event(), []

    def keep_rolling():
        while not stop.is_set():
            time.sleep(0.15)
            rolled.append(rollover(alias))

    roller = threading.Thread(target=keep_rolling)
    roller.start()
    try:
        write(elasticsearch_spark, df, index=alias, id_column="id", auto_create_index=False, batch_size_entries=100)
    finally:
        stop.set()
        roller.join()
    counts = per_index(alias)
    assert sum(counts.values()) == rows and len(counts) > 2, counts  # nothing lost, and the rollovers did happen mid-write
    assert versions(alias) == {str(i): 1 for i in range(rows)}  # nothing written twice


def test_after_a_rollover_a_replay_lands_in_the_new_index(elasticsearch_spark, alias):
    """Idempotency by id holds within one index: a document written again after a rollover
    goes to the new write index, next to its old copy. Rollover is for append-only streams;
    do not count on replays (or upserts) after one."""
    first = bootstrap(alias)
    df = elasticsearch_spark.createDataFrame([(1, "a"), (2, "b")], "id INT, v STRING")
    write(elasticsearch_spark, df, index=alias, id_column="id", auto_create_index=False)
    write(elasticsearch_spark, df, index=alias, id_column="id", auto_create_index=False)
    assert per_index(alias) == {first: 2}
    second = rollover(alias)
    write(elasticsearch_spark, df, index=alias, id_column="id", auto_create_index=False)
    assert per_index(alias) == {first: 2, second: 2}
    # The same by upsert: the alias routes it to the write index, which has no copy yet.
    write(elasticsearch_spark, df, index=alias, id_column="id", auto_create_index=False, mode="upsert")
    assert per_index(alias) == {first: 2, second: 2} and versions(second) == {"1": 1, "2": 1}  # found there: a no-op


def test_alias_without_a_write_index_is_refused_and_reported(elasticsearch_spark, alias):
    es("PUT", f"/{alias}-a", {"aliases": {alias: {}}})
    es("PUT", f"/{alias}-b", {"aliases": {alias: {}}})
    with pytest.raises(Exception, match=f"Attempting to write to alias \\[{alias}\\], but detected multiple indices \\[2\\] with no write index"):
        write(elasticsearch_spark, elasticsearch_spark.range(1), index=alias, auto_create_index=False)
    assert ElasticsearchSink().validate_input(None, elasticsearch_spark.range(1).schema, options(index=alias)) == [
        ("index", f"'{alias}' is an alias of {alias}-a, {alias}-b and none of them is its write index: writes are refused. "
                  "Give one is_write_index: true, or write to an index")
    ]


def test_auto_create_would_take_the_alias_name(elasticsearch_spark, alias):
    """Without the bootstrap index, auto-creation makes a plain index by the alias' name and
    ILM can never roll it over: rollover series want auto_create_index off."""
    write(elasticsearch_spark, elasticsearch_spark.range(1), index=alias)
    assert es("GET", f"/_alias/{alias}").get("status") == 404 and es("GET", f"/{alias}")[alias]["aliases"] == {}
    assert "index_already_exists" in es("PUT", f"/{alias}-000001", {"aliases": {alias: {"is_write_index": True}}})["error"]["type"] or \
        "invalid_alias_name_exception" in es("PUT", f"/{alias}-000001", {"aliases": {alias: {"is_write_index": True}}})["error"]["type"]
