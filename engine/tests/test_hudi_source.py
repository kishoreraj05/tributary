"""The Hudi source: options, session configuration and real reads of tables on local disk.

Tables are written with the Hudi sink. Tests that read need the Hudi Spark bundle (see
``hudi_jar`` in conftest); the others run without Spark.
"""

from __future__ import annotations

import copy
import json

import pytest
from pydantic import ValidationError

from conftest import pipeline
from tributary import SpecError, TributaryError, analyze, compile_pipeline, preview, run
from tributary.core import ResolvedSchema, RunContext
from tributary.plugins._hudi import commits, to_instant
from tributary.plugins.sinks.hudi import HUDI_VERSION, HudiSink, HudiSinkOptions
from tributary.plugins.sources.hudi import HudiSource, HudiSourceOptions, check_table
from tributary.registry import default_registry
from tributary.runtime import required_conf, required_packages
from tributary.testing import assert_source_contract, static_contract_issues

ROWS = "id INT, amount DOUBLE, country STRING, version INT"


def ctx_for(spark, **overrides) -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id="p", env="dev", **overrides)


def write(spark, base_path, table, rows, **options) -> HudiSinkOptions:
    """One commit on a keyed table (upsert on id, the larger version winning)."""
    opts = HudiSinkOptions(base_path=str(base_path), table=table, record_key=["id"], precombine_field="version", **options)
    HudiSink().write(ctx_for(spark), spark.createDataFrame(rows, ROWS), opts)
    return opts


def read(spark, base_path, table, **options):
    opts = HudiSourceOptions(base_path=str(base_path), table=table, **options)
    return HudiSource().read(ctx_for(spark), opts, None)


def rows(df) -> list[tuple]:
    return sorted(tuple(r) for r in df.collect())


def hudi_source(base_path, table="sales.orders", **options) -> dict:
    return {"id": "orders_src", "type": "hudi", "options": {"base_path": str(base_path), "table": table, **options}}


def source_pipeline(tmp_path, base_path, sql="SELECT * FROM orders", **source_options) -> dict:
    """hudi source -> sql -> parquet file sink."""
    doc = pipeline(tmp_path, tmp_path / "unused.csv", quality=[])
    doc["sources"] = [hudi_source(base_path, **source_options)]
    doc["transforms"][0]["options"]["sql"] = sql
    return doc


def parquet_rows(spark, tmp_path, *columns) -> list[tuple]:
    return rows(spark.read.parquet(str(tmp_path / "out" / "orders")).select(*columns))


# --- no Spark needed ---------------------------------------------------------------------


def test_static_contract():
    assert static_contract_issues(HudiSource) == []
    assert HudiSource.capabilities() == ["batch", "streaming"]


def test_instants_accept_hudi_timestamps_dates_and_times():
    assert to_instant("20240604080000123") == "20240604080000123"
    assert to_instant(" 20240604080000 ") == "20240604080000"
    assert to_instant("2024-06-04") == "20240604000000"
    assert to_instant("2024-06-04 08:00:00") == "20240604080000"
    assert to_instant("2024-06-04T08:00:00.5") == "20240604080000500"
    for bad in ["", "yesterday", "2024/06/04", "2024-06-04 08:00", "2024-06-04 08:00:00 UTC"]:
        with pytest.raises(ValueError, match="is not a Hudi instant"):
            to_instant(bad)


def test_options_are_validated():
    ok = HudiSourceOptions(base_path="/lake/", table="sales.orders", query_type="incremental", begin_instant=" Earliest ",
                           end_instant=" 2024-06-04 ", start_offset="LATEST")
    assert (ok.begin_instant, ok.end_instant, ok.start_offset) == ("Earliest", "2024-06-04", "latest")
    assert ok.location == "/lake/sales/orders" and ok.database == "sales" and ok.table_name == "orders"
    flat = HudiSourceOptions(base_path="s3a://bucket", table="orders")
    assert flat.location == "s3a://bucket/orders" and flat.database is None and flat.query_type == "snapshot"
    assert not flat.include_metadata_columns and flat.start_offset == "earliest"
    for bad, message in [
        ({"table": "orders"}, r"base_path\n  Field required"),
        ({"base_path": "/l", "table": "a.b.c"}, "pattern"),
        ({"base_path": "/l", "table": "orders", "query_type": "incremental"}, r"begin_instant\n  Value error, required for query_type 'incremental'"),
        ({"base_path": "/l", "table": "orders", "query_type": "incremental", "begin_instant": " "}, "required for query_type 'incremental'"),
        ({"base_path": "/l", "table": "orders", "query_type": "incremental", "begin_instant": "yesterday"}, r"begin_instant\n  Value error, 'yesterday' is not a Hudi instant"),
        ({"base_path": "/l", "table": "orders", "query_type": "incremental", "begin_instant": "earliest", "end_instant": "later"}, r"end_instant\n  Value error, 'later' is not"),
        ({"base_path": "/l", "table": "orders", "as_of_instant": "2024/06/04"}, r"as_of_instant\n  Value error, '2024/06/04' is not"),
        ({"base_path": "/l", "table": "orders", "start_offset": "now"}, r"start_offset\n  Value error, 'now' is not"),
        ({"base_path": "/l", "table": "orders", "query_type": "changes"}, "query_type"),
        # Settings of another query type are refused rather than ignored.
        ({"base_path": "/l", "table": "orders", "query_type": "incremental", "begin_instant": "earliest", "as_of_instant": "2024-06-04"},
         r"as_of_instant\n  Value error, time travel is a snapshot read, not 'incremental'"),
        ({"base_path": "/l", "table": "orders", "query_type": "read_optimized", "as_of_instant": "2024-06-04"}, "not 'read_optimized'"),
        ({"base_path": "/l", "table": "orders", "begin_instant": "earliest"}, r"begin_instant\n  Value error, only incremental reads start at an instant \(query_type is 'snapshot'\)"),
        ({"base_path": "/l", "table": "orders", "query_type": "read_optimized", "end_instant": "2024-06-04"}, r"end_instant\n  Value error, only incremental reads stop"),
        ({"base_path": "/l", "table": "orders", "nope": 1}, "extra"),
    ]:
        with pytest.raises(ValidationError, match=message):
            HudiSourceOptions(**bad)
    # Not incremental: begin_instant is not needed, and 'latest' is only a streaming start
    HudiSourceOptions(base_path="/l", table="orders", query_type="read_optimized")
    blank = HudiSourceOptions(base_path="/l", table="orders", query_type="incremental", begin_instant="earliest", end_instant="  ")
    assert blank.end_instant is None and HudiSourceOptions(base_path="/l", table="orders", as_of_instant=" ").as_of_instant is None
    HudiSourceOptions(base_path="/l", table="orders", start_offset="latest")


def test_each_setting_says_when_it_applies():
    """The UI shows a field only for the values in 'in' and stars it for 'required_in'."""
    schema = HudiSourceOptions.model_json_schema()
    props = schema["properties"]
    assert props["begin_instant"]["x-when"] == {"field": "query_type", "in": ["incremental"], "required_in": ["incremental"]}
    assert props["end_instant"]["x-when"] == {"field": "query_type", "in": ["incremental"], "required_in": []}
    assert props["incremental_format"]["x-when"] == {"field": "query_type", "in": ["incremental"], "required_in": []}
    assert props["as_of_instant"]["x-when"] == {"field": "query_type", "in": ["snapshot"], "required_in": []}
    assert props["query_type"]["enum"] == ["snapshot", "read_optimized", "incremental"]
    assert props["query_type"]["default"] == "snapshot"
    assert props["incremental_format"]["enum"] == ["latest_state", "cdc"]
    # Always mandatory: the storage root (unless a connection supplies it) and the table.
    assert schema["required"] == ["base_path", "table"]
    assert "x-when" not in props["table"] and "x-when" not in props["base_path"]


def test_reader_options_cover_every_query_type():
    def hoodie(**options):
        return HudiSourceOptions(base_path="/l", table="orders", **options).hoodie_options()

    assert hoodie() == {"hoodie.datasource.query.type": "snapshot"}
    assert hoodie(as_of_instant="2024-06-04 08:00:00") == {"hoodie.datasource.query.type": "snapshot", "as.of.instant": "20240604080000"}
    assert hoodie(query_type="read_optimized") == {"hoodie.datasource.query.type": "read_optimized"}
    # Everything since the beginning, in its latest state, is the table itself.
    assert hoodie(query_type="incremental", begin_instant="earliest") == {"hoodie.datasource.query.type": "snapshot"}
    assert hoodie(query_type="incremental", begin_instant="Earliest", end_instant="2024-06-05") == {
        "hoodie.datasource.query.type": "snapshot", "as.of.instant": "20240605000000",
    }
    assert hoodie(query_type="incremental", begin_instant="earliest", incremental_format="cdc") == {
        "hoodie.datasource.query.type": "incremental",
        "hoodie.datasource.read.begin.instanttime": "000",
        "hoodie.datasource.query.incremental.format": "cdc",
        "hoodie.datasource.read.incr.fallback.fulltablescan.enable": "true",
        "hoodie.datasource.read.incr.table.version": "6",
    }
    window = hoodie(query_type="incremental", begin_instant="20240604080000123", end_instant="2024-06-05", incremental_format="cdc",
                    read_options={"hoodie.enable.data.skipping": True, "hoodie.datasource.query.type": "snapshot"})
    assert window == {
        "hoodie.datasource.query.type": "snapshot",  # read_options win over derived settings
        "hoodie.datasource.read.begin.instanttime": "20240604080000123",
        "hoodie.datasource.read.end.instanttime": "20240605000000",
        "hoodie.datasource.query.incremental.format": "cdc",
        "hoodie.datasource.read.incr.fallback.fulltablescan.enable": "true",
        "hoodie.datasource.read.incr.table.version": "6",
        "hoodie.enable.data.skipping": "true",
    }

    # A recorded checkpoint and a pinned latest commit replace the configured bounds.
    tracked = HudiSourceOptions(base_path="/l", table="orders", query_type="incremental", begin_instant="earliest", checkpoint_location="/chk")
    assert tracked.hoodie_options(end="20240605000000000") == {"hoodie.datasource.query.type": "snapshot", "as.of.instant": "20240605000000000"}
    assert tracked.hoodie_options(begin="20240604080000123", end="20240605000000000") == {
        "hoodie.datasource.query.type": "incremental",
        "hoodie.datasource.read.begin.instanttime": "20240604080000123",
        "hoodie.datasource.read.end.instanttime": "20240605000000000",
        "hoodie.datasource.query.incremental.format": "latest_state",
        "hoodie.datasource.read.incr.fallback.fulltablescan.enable": "true",
        "hoodie.datasource.read.incr.table.version": "6",
    }
    props = HudiSourceOptions.model_json_schema()["properties"]
    assert props["checkpoint_location"]["x-when"] == {"field": "query_type", "in": ["incremental"], "required_in": []}

    def stream(**options):
        return HudiSourceOptions(base_path="/l", table="orders", **options).stream_options()

    assert stream() == {
        "hoodie.datasource.streaming.startOffset": "earliest",
        "hoodie.datasource.query.incremental.format": "latest_state",
        "hoodie.datasource.read.streaming.table.version": "6",
    }
    assert stream(start_offset="latest")["hoodie.datasource.streaming.startOffset"] == "latest"
    assert stream(start_offset="2024-06-04")["hoodie.datasource.streaming.startOffset"] == "20240604000000"
    # Batch settings stay out of the stream; read_options go in.
    assert "hoodie.datasource.query.type" not in stream(query_type="incremental", begin_instant="earliest", end_instant="2024-06-04")
    assert stream(read_options={"hoodie.datasource.read.incr.path.glob": "country=IN/*"})["hoodie.datasource.read.incr.path.glob"] == "country=IN/*"


def test_missing_settings_are_reported_on_their_field(tmp_path):
    """The UI shows an issue under the field its path names."""
    doc = source_pipeline(tmp_path, tmp_path / "lake", query_type="incremental")
    del doc["sources"][0]["options"]["base_path"]
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    assert {(i.path, i.message) for i in exc.value.issues} == {
        ("sources[0].options.base_path", "Field required"),
        ("sources[0].options.begin_instant", "required for query_type 'incremental' (a Hudi instant, a timestamp, a date or 'earliest')"),
    }


def test_one_hudi_connection_serves_sources_and_sinks(tmp_path):
    """A connection of type hudi carries the storage root and the sinks' metastore sync
    settings; a source takes the root from it and accepts (ignores) the rest."""
    doc = source_pipeline(tmp_path, tmp_path / "lake")
    doc["environments"]["dev"]["connections"] = {
        "lake": {"type": "hudi", "base_path": str(tmp_path / "lake"), "catalog_sync": "hms", "catalog_uri": "thrift://h:9083"}
    }
    doc["sources"][0] = {"id": "orders_src", "type": "hudi", "connection": "lake", "options": {"table": "sales.orders"}}
    doc["sinks"].append({"id": "copy", "type": "hudi", "input": "orders_clean", "connection": "lake",
                         "options": {"table": "sales.orders_copy", "operation": "insert"}})
    compiled = compile_pipeline(doc)
    source, copy = compiled.spec.sources[0].options, compiled.spec.sinks[1].options
    assert source.location == f"{tmp_path}/lake/sales/orders" and source.hoodie_options() == {"hoodie.datasource.query.type": "snapshot"}
    assert copy.location == f"{tmp_path}/lake/sales/orders_copy" and copy.catalog_sync == "hms"

    doc["sources"][0]["options"]["base_path"] = "/elsewhere"  # node options win
    assert compile_pipeline(doc).spec.sources[0].options.location == "/elsewhere/sales/orders"

    doc["environments"]["dev"]["connections"]["lake"]["type"] = "kafka"
    with pytest.raises(SpecError, match="connection 'lake' is 'kafka', node type is 'hudi'"):
        compile_pipeline(doc)


def test_session_gets_the_bundle_but_needs_no_kryo(tmp_path):
    """Reading needs the Hudi bundle on the classpath and nothing else: Kryo serialization
    is only required for writes, so a pipeline that reads Hudi into files gets no static
    session settings from it."""
    compiled = compile_pipeline(source_pipeline(tmp_path, tmp_path / "lake"))
    assert required_packages(compiled) == [f"org.apache.hudi:hudi-spark3.5-bundle_2.12:{HUDI_VERSION}"]
    assert required_conf(compiled) == {}


def test_input_size_counts_data_files_only(tmp_path):
    table = tmp_path / "lake" / "sales" / "orders"
    (table / ".hoodie").mkdir(parents=True)
    (table / ".hoodie" / "hoodie.properties").write_text("x" * 100)
    (table / "country=IN").mkdir()
    (table / "country=IN" / "a.parquet").write_bytes(b"x" * 300)
    (table / "country=IN" / ".a.parquet.crc").write_bytes(b"x" * 50)
    (table / "country=US").mkdir()
    (table / "country=US" / "b.parquet").write_bytes(b"x" * 200)
    assert HudiSource().estimate_input_bytes(HudiSourceOptions(base_path=str(tmp_path / "lake"), table="sales.orders")) == 500
    assert HudiSource().estimate_input_bytes(HudiSourceOptions(base_path=str(tmp_path / "lake"), table="nope")) is None
    assert HudiSource().estimate_input_bytes(HudiSourceOptions(base_path="s3a://bucket/lake", table="sales.orders")) is None


# --- real reads (tables on local disk) -----------------------------------------------------


def test_source_contract(hudi_spark, tmp_path):
    write(hudi_spark, tmp_path, "contract.numbers", [(i, float(i), "IN", 1) for i in range(5)])
    opts = HudiSourceOptions(base_path=str(tmp_path), table="contract.numbers")
    assert_source_contract(hudi_spark, HudiSource(), opts)


def test_snapshot_read_drops_hudi_metadata_unless_asked(hudi_spark, tmp_path):
    write(hudi_spark, tmp_path, "sales.orders", [(1, 10.0, "IN", 1), (2, 20.0, "US", 1)], partition_by=["country"])
    write(hudi_spark, tmp_path, "sales.orders", [(2, 25.0, "US", 2), (3, 30.0, "DE", 1)], partition_by=["country"])

    df = read(hudi_spark, tmp_path, "sales.orders")
    assert sorted(df.columns) == ["amount", "country", "id", "version"]  # Hudi lists partition columns last
    assert rows(df.select("id", "amount", "country", "version")) == [(1, 10.0, "IN", 1), (2, 25.0, "US", 2), (3, 30.0, "DE", 1)]

    full = read(hudi_spark, tmp_path, "sales.orders", include_metadata_columns=True)
    assert full.columns[:5] == ["_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name"]
    first, second = [c.timestamp for c in commits(hudi_spark, f"{tmp_path}/sales/orders")]
    assert rows(full.select("id", "_hoodie_commit_time", "_hoodie_partition_path")) == [
        (1, first, "country=IN"), (2, second, "country=US"), (3, second, "country=DE"),
    ]


def test_schema_block_selects_and_casts_the_named_columns(hudi_spark, tmp_path):
    write(hudi_spark, tmp_path, "typed", [(1, 10.0, "IN", 1), (2, 20.5, "US", 1)])
    opts = HudiSourceOptions(base_path=str(tmp_path), table="typed")
    struct = hudi_spark.createDataFrame([], "amount DECIMAL(10,2), id STRING, _hoodie_record_key STRING").schema

    df = HudiSource().read(ctx_for(hudi_spark), opts, ResolvedSchema(struct=struct))
    assert df.schema == struct  # the declared schema is what flows downstream, metadata columns included when named
    assert rows(df) == [(10.0, "1", "1"), (20.5, "2", "2")]
    assert HudiSource().output_schema(opts, ResolvedSchema(struct=struct)) == struct
    assert HudiSource().output_schema(opts, None) is None

    nope = hudi_spark.createDataFrame([], "id INT, customer STRING, region STRING").schema
    with pytest.raises(TributaryError, match="hudi source typed: schema names columns 'customer', 'region' that the table does not have"):
        HudiSource().read(ctx_for(hudi_spark), opts, ResolvedSchema(struct=nope))


def test_missing_table_is_reported_up_front(hudi_spark, tmp_path):
    opts = HudiSourceOptions(base_path=str(tmp_path / "lake"), table="sales.orders")
    message = f"hudi table sales.orders not found at {tmp_path}/lake/sales/orders \\(no .hoodie/hoodie.properties there\\)"
    with pytest.raises(TributaryError, match=message):
        check_table(hudi_spark, opts)
    with pytest.raises(TributaryError, match=message):
        HudiSource().read(ctx_for(hudi_spark), opts, None)
    with pytest.raises(TributaryError, match=message):
        HudiSource().read_stream(ctx_for(hudi_spark), opts, None)
    # Deep validation reports it on the source node and skips what depends on it.
    report = analyze(compile_pipeline(source_pipeline(tmp_path, tmp_path / "lake")), spark=hudi_spark)
    assert not report["orders_src"].ok and "not found at" in report["orders_src"].error
    assert report["orders_clean"].error == "skipped: upstream orders_src has errors"


def test_pipeline_reads_hudi_into_parquet_and_preview_is_bounded(hudi_spark, tmp_path):
    lake = tmp_path / "lake"
    write(hudi_spark, lake, "sales.orders", [(1, 10.0, "IN", 1), (2, -1.0, "US", 1), (3, 30.0, "de", 1)])
    doc = source_pipeline(tmp_path, lake, sql="SELECT id AS order_id, amount, upper(country) AS country FROM orders")

    report = analyze(compile_pipeline(doc), spark=hudi_spark)
    assert all(r.ok for r in report.values()), report
    assert sorted(f["name"] for f in report["orders_src"].schema["fields"]) == ["amount", "country", "id", "version"]

    result = preview(doc, "orders_src", spark=hudi_spark, limit=2)
    assert len(result.rows) == 2 and set(result.rows[0]) == {"id", "amount", "country", "version"}

    summary = run(doc, spark=hudi_spark)
    assert summary.sinks_written == ["orders_out"]
    assert parquet_rows(hudi_spark, tmp_path, "order_id", "amount", "country") == [(1, 10.0, "IN"), (2, -1.0, "US"), (3, 30.0, "DE")]


def test_incremental_reads_follow_the_commits(hudi_spark, tmp_path):
    """Each read sees only what the commits after begin_instant changed: the latest state
    of those rows, unpartitioned or not. 'earliest' is every commit, and a window is closed
    with end_instant."""
    lake = tmp_path / "lake"
    write(hudi_spark, lake, "sales.orders", [(1, 10.0, "IN", 1), (2, 20.0, "US", 1)], partition_by=["country"])
    write(hudi_spark, lake, "sales.orders", [(2, 25.0, "US", 2), (3, 30.0, "DE", 1)], partition_by=["country"])
    write(hudi_spark, lake, "sales.orders", [(4, 40.0, "FR", 1)], partition_by=["country"])
    first, second, third = [c.timestamp for c in commits(hudi_spark, f"{lake}/sales/orders")]

    def changed(**options):
        df = read(hudi_spark, lake, "sales.orders", query_type="incremental", include_metadata_columns=True, **options)
        return rows(df.select("id", "amount", "_hoodie_commit_time"))

    assert changed(begin_instant="earliest") == [(1, 10.0, first), (2, 25.0, second), (3, 30.0, second), (4, 40.0, third)]
    assert changed(begin_instant=first) == [(2, 25.0, second), (3, 30.0, second), (4, 40.0, third)]
    assert changed(begin_instant=second) == [(4, 40.0, third)]
    assert changed(begin_instant=third) == []
    assert changed(begin_instant=first, end_instant=second) == [(2, 25.0, second), (3, 30.0, second)]
    assert changed(begin_instant="earliest", end_instant=second) == [(1, 10.0, first), (2, 25.0, second), (3, 30.0, second)]
    # A timestamp (in the timeline's time zone) stands in for an instant.
    stamp = f"{first[:4]}-{first[4:6]}-{first[6:8]} {first[8:10]}:{first[10:12]}:{first[12:14]}"
    assert changed(begin_instant=stamp) == changed(begin_instant=first[:14])
    # Without metadata columns, the changed rows look like any other read of the table.
    plain = read(hudi_spark, lake, "sales.orders", query_type="incremental", begin_instant=first)
    assert sorted(plain.columns) == ["amount", "country", "id", "version"] and rows(plain.select("id")) == [(2,), (3,), (4,)]

    # The usual loop: a run processes the commits since the previous run's last commit.
    doc = source_pipeline(tmp_path, lake, sql="SELECT id, amount FROM orders", query_type="incremental", begin_instant="${env.since}")
    doc["environments"]["dev"]["vars"]["since"] = first
    run(doc, spark=hudi_spark)
    assert parquet_rows(hudi_spark, tmp_path, "id", "amount") == [(2, 25.0), (3, 30.0), (4, 40.0)]


def test_cdc_format_gives_one_row_per_change(hudi_spark, tmp_path):
    cdc = {"write_options": {"hoodie.table.cdc.enabled": True}}
    write(hudi_spark, tmp_path, "cdc.orders", [(1, 10.0, "IN", 1), (2, 20.0, "US", 1)], **cdc)
    write(hudi_spark, tmp_path, "cdc.orders", [(2, 25.0, "US", 2), (3, 30.0, "DE", 1)], **cdc)
    HudiSink().write(ctx_for(hudi_spark), hudi_spark.createDataFrame([(1, 0.0, "IN", 9)], ROWS),
                     HudiSinkOptions(base_path=str(tmp_path), table="cdc.orders", operation="delete", record_key=["id"], **cdc))
    first, second, third = [c.timestamp for c in commits(hudi_spark, f"{tmp_path}/cdc/orders")]

    df = read(hudi_spark, tmp_path, "cdc.orders", query_type="incremental", begin_instant="earliest", incremental_format="cdc")
    assert df.columns == ["op", "ts_ms", "before", "after"]
    assert df.schema["before"].dataType == df.schema["after"].dataType
    assert df.schema["after"].dataType.simpleString() == "struct<id:int,amount:double,country:string,version:int>"
    changes = sorted((r.op, r.ts_ms, r.before and tuple(r.before), r.after and tuple(r.after)) for r in df.collect())
    assert changes == [
        ("d", third, (1, 10.0, "IN", 1), None),
        ("i", first, None, (1, 10.0, "IN", 1)),
        ("i", first, None, (2, 20.0, "US", 1)),
        ("i", second, None, (3, 30.0, "DE", 1)),
        ("u", second, (2, 20.0, "US", 1), (2, 25.0, "US", 2)),
    ]
    later = read(hudi_spark, tmp_path, "cdc.orders", query_type="incremental", begin_instant=second, incremental_format="cdc")
    assert [(r.op, r.ts_ms, r.after) for r in later.collect()] == [("d", third, None)]

    # SQL downstream reaches into the structs.
    doc = source_pipeline(tmp_path, tmp_path, sql="SELECT op, after.id AS id, after.amount AS amount FROM orders WHERE op <> 'd'",
                          table="cdc.orders", query_type="incremental", begin_instant="earliest", incremental_format="cdc")
    run(doc, spark=hudi_spark)
    assert parquet_rows(hudi_spark, tmp_path, "op", "id", "amount") == [("i", 1, 10.0), ("i", 2, 20.0), ("i", 3, 30.0), ("u", 2, 25.0)]


def test_read_optimized_and_time_travel_on_a_merge_on_read_table(hudi_spark, tmp_path):
    mor = {"table_type": "merge_on_read"}
    write(hudi_spark, tmp_path, "mor.orders", [(1, 10.0, "IN", 1), (2, 20.0, "US", 1)], **mor)
    write(hudi_spark, tmp_path, "mor.orders", [(2, 25.0, "US", 2)], **mor)  # an update: goes to a log file
    first, second = [c.timestamp for c in commits(hudi_spark, f"{tmp_path}/mor/orders")]
    assert [c.action for c in commits(hudi_spark, f"{tmp_path}/mor/orders")] == ["deltacommit", "deltacommit"]

    def amounts(**options):
        return {r.id: r.amount for r in read(hudi_spark, tmp_path, "mor.orders", **options).collect()}

    assert amounts() == {1: 10.0, 2: 25.0}  # snapshot: base files merged with log files
    assert amounts(query_type="read_optimized") == {1: 10.0, 2: 20.0}  # base files only: the update is not compacted yet
    assert amounts(as_of_instant=first) == {1: 10.0, 2: 20.0}  # time travel
    assert amounts(as_of_instant=second) == {1: 10.0, 2: 25.0}
    assert amounts(as_of_instant="2099-01-01") == {1: 10.0, 2: 25.0}
    assert amounts(as_of_instant="2000-01-01") == {}
    # Incremental reads merge the log files too.
    assert amounts(query_type="incremental", begin_instant=first) == {2: 25.0}
    assert amounts(query_type="incremental", begin_instant="earliest") == {1: 10.0, 2: 25.0}


def test_hudi_to_hudi_copies_the_changes_since_a_commit(hudi_spark, tmp_path):
    """Source and sink share one connection: the changes of the main table since a commit
    are upserted into another table."""
    lake = tmp_path / "lake"
    write(hudi_spark, lake, "sales.orders", [(1, 10.0, "IN", 1), (2, 20.0, "US", 1)])
    write(hudi_spark, lake, "sales.orders", [(2, 25.0, "US", 2), (3, 30.0, "DE", 1)])
    first, _ = [c.timestamp for c in commits(hudi_spark, f"{lake}/sales/orders")]
    doc = {
        "pipeline": {"id": "orders_replica", "mode": "batch"},
        "environments": {"dev": {"connections": {"lake": {"type": "hudi", "base_path": str(lake)}}}},
        "sources": [{"id": "changes", "type": "hudi", "connection": "lake",
                     "options": {"table": "sales.orders", "query_type": "incremental", "begin_instant": first}}],
        "sinks": [{"id": "replica", "type": "hudi", "connection": "lake", "input": "changes",
                   "options": {"table": "sales.orders_replica", "record_key": ["id"], "precombine_field": "version"}}],
    }
    assert run(doc, spark=hudi_spark).sinks_written == ["replica"]
    assert rows(read(hudi_spark, lake, "sales.orders_replica")) == [(2, 25.0, "US", 2), (3, 30.0, "DE", 1)]


def test_streaming_reads_new_commits_per_micro_batch(hudi_spark, tmp_path):
    """A streaming pipeline follows the table's commits: each run picks up the commits made
    since its checkpoint, and start_offset 'latest' skips what is already there."""
    lake = tmp_path / "lake"
    write(hudi_spark, lake, "events", [(1, 10.0, "IN", 1), (2, 20.0, "US", 1)])
    write(hudi_spark, lake, "events", [(2, 25.0, "US", 2)])
    out = tmp_path / "out"

    def stream_doc(checkpoint: str, **source_options) -> dict:
        return {
            "pipeline": {"id": "events_stream", "mode": "streaming",
                         "streaming": {"checkpoint_location": str(tmp_path / checkpoint), "trigger": "available_now"}},
            "sources": [{"id": "events", "type": "hudi", "options": {"base_path": str(lake), "table": "events", **source_options}}],
            "transforms": [{"id": "big", "type": "sql", "inputs": ["events"],
                            "options": {"sql": "SELECT id, amount, _hoodie_commit_time AS committed FROM events WHERE amount >= 20"}}],
            "sinks": [{"id": "out", "type": "file", "input": "big", "options": {"path": str(out), "format": "json"}}],
        }

    doc = stream_doc("chk", include_metadata_columns=True)
    # A preview of a streaming source is a bounded snapshot read.
    assert sorted(r["id"] for r in preview(doc, "events", spark=hudi_spark, limit=5).rows) == [1, 2]

    assert not run(doc, spark=hudi_spark).isActive
    _, second = [c.timestamp for c in commits(hudi_spark, f"{lake}/events")]
    assert rows(hudi_spark.read.json(str(out)).select("id", "amount", "committed")) == [(2, 25.0, second)]

    write(hudi_spark, lake, "events", [(3, 30.0, "DE", 1), (4, 4.0, "FR", 1)])
    assert not run(doc, spark=hudi_spark).isActive  # resumes from the checkpoint: only the new commit
    third = commits(hudi_spark, f"{lake}/events")[-1].timestamp
    assert rows(hudi_spark.read.json(str(out)).select("id", "amount", "committed")) == [(2, 25.0, second), (3, 30.0, third)]

    # A fresh stream can also start after a given commit (its _hoodie_commit_time).
    after_second = stream_doc("chk_after_second", start_offset=second)
    after_second["sinks"][0]["options"]["path"] = str(tmp_path / "out_after_second")
    after_second["transforms"][0]["options"]["sql"] = "SELECT id, amount FROM events WHERE amount >= 20"
    assert not run(after_second, spark=hudi_spark).isActive
    assert rows(hudi_spark.read.json(str(tmp_path / "out_after_second")).select("id")) == [(3,)]

    # A stream asked for the change feed: Hudi 0.x streams the latest state regardless (the
    # rows come through as they are; 1.x delivers change rows), so nothing is parsed.
    as_cdc = stream_doc("chk_cdc", incremental_format="cdc")
    as_cdc["sinks"][0]["options"]["path"] = str(tmp_path / "out_cdc")
    as_cdc["transforms"][0]["options"]["sql"] = "SELECT * FROM events"
    assert not run(as_cdc, spark=hudi_spark).isActive
    streamed = hudi_spark.read.json(str(tmp_path / "out_cdc"))
    assert {"id", "amount"} <= set(streamed.columns) or {"op", "before", "after"} <= set(streamed.columns)

    # A fresh stream starting at 'latest' sees nothing until the next commit.
    latest = stream_doc("chk_latest", include_metadata_columns=True, start_offset="latest")
    latest["sinks"][0]["options"]["path"] = str(tmp_path / "out_latest")
    assert not run(latest, spark=hudi_spark).isActive
    assert not (tmp_path / "out_latest").exists() or hudi_spark.read.json(str(tmp_path / "out_latest")).count() == 0
    write(hudi_spark, lake, "events", [(5, 50.0, "GB", 1)])
    assert not run(latest, spark=hudi_spark).isActive
    assert rows(hudi_spark.read.json(str(tmp_path / "out_latest")).select("id")) == [(5,)]


# --- the scenarios an incremental pipeline meets ----------------------------------------------


def test_query_type_is_checked_against_the_table(hudi_spark, tmp_path, caplog):
    """A change feed needs a table that records changes; a read_optimized view of a
    copy-on-write table is its snapshot (noted, not refused)."""
    write(hudi_spark, tmp_path, "plain", [(1, 10.0, "IN", 1)])
    with pytest.raises(TributaryError, match="hudi table plain does not record changes .hoodie.table.cdc.enabled is off."):
        read(hudi_spark, tmp_path, "plain", query_type="incremental", begin_instant="earliest", incremental_format="cdc")
    assert check_table(hudi_spark, HudiSourceOptions(base_path=str(tmp_path), table="plain"))["hoodie.table.type"] == "COPY_ON_WRITE"
    with caplog.at_level("INFO", logger="tributary"):
        assert rows(read(hudi_spark, tmp_path, "plain", query_type="read_optimized")) == rows(read(hudi_spark, tmp_path, "plain"))
    assert "reading hudi table plain (copy_on_write) as read_optimized (the snapshot: a copy-on-write table has no log files to skip)" in caplog.text


def test_checkpoint_tracks_where_each_run_stopped(hudi_spark, tmp_path, caplog):
    """With a checkpoint_location, every run reads the commits since the previous successful
    run, whatever lands in between; a failed run or a preview leaves the progress alone."""
    lake, chk = tmp_path / "lake", tmp_path / "chk"
    write(hudi_spark, lake, "sales.orders", [(1, 10.0, "IN", 1), (2, 20.0, "US", 1)])
    doc = source_pipeline(tmp_path, lake, sql="SELECT id, amount FROM orders", query_type="incremental",
                          begin_instant="earliest", checkpoint_location=str(chk))
    first = commits(hudi_spark, f"{lake}/sales/orders")[-1].timestamp

    # A preview shows what the next run would read, and records nothing.
    assert sorted(r["id"] for r in preview(doc, "orders_src", spark=hudi_spark, limit=10).rows) == [1, 2]
    assert not chk.exists()

    run(doc, spark=hudi_spark)  # first run: everything (begin_instant), progress recorded at the latest commit
    assert parquet_rows(hudi_spark, tmp_path, "id") == [(1,), (2,)]
    assert json.loads((chk / "offset.json").read_text())["instant"] == first

    run(doc, spark=hudi_spark)  # nothing new: an empty read, progress unchanged
    assert parquet_rows(hudi_spark, tmp_path, "id") == []
    assert json.loads((chk / "offset.json").read_text())["instant"] == first

    write(hudi_spark, lake, "sales.orders", [(2, 25.0, "US", 2), (3, 30.0, "DE", 1)])
    write(hudi_spark, lake, "sales.orders", [(4, 40.0, "FR", 1)])
    third = commits(hudi_spark, f"{lake}/sales/orders")[-1].timestamp
    assert sorted(r["id"] for r in preview(doc, "orders_src", spark=hudi_spark, limit=10).rows) == [2, 3, 4]
    with caplog.at_level("INFO", logger="tributary"):
        run(doc, spark=hudi_spark)
    assert parquet_rows(hudi_spark, tmp_path, "id", "amount") == [(2, 25.0), (3, 30.0), (4, 40.0)]
    assert f"reading commits after {first} up to {third} (recorded progress)" in caplog.text
    assert f"progress recorded up to commit {third}" in caplog.text

    # A run whose sink fails does not move the progress: the next run reads the same commits.
    write(hudi_spark, lake, "sales.orders", [(5, 50.0, "GB", 1)])
    broken = copy.deepcopy(doc)
    broken["sinks"][0]["options"]["path"] = "/proc/nope/cannot/write"
    with pytest.raises(Exception):  # noqa: B017 - the sink's own failure
        run(broken, spark=hudi_spark)
    assert json.loads((chk / "offset.json").read_text())["instant"] == third
    run(doc, spark=hudi_spark)
    assert parquet_rows(hudi_spark, tmp_path, "id") == [(5,)]
    assert json.loads((chk / "offset.json").read_text())["instant"] == commits(hudi_spark, f"{lake}/sales/orders")[-1].timestamp

    # Someone else's pending note is not committed by this run.
    (chk / "pending.json").write_text(json.dumps({"instant": "20990101000000000", "run_id": "other"}))
    with caplog.at_level("WARNING", logger="tributary"):
        HudiSource().commit_read(ctx_for(hudi_spark, run_id="mine"), compile_pipeline(doc).spec.sources[0].options)
    assert "noted by run other, not this one (mine)" in caplog.text
    assert json.loads((chk / "offset.json").read_text())["instant"] != "20990101000000000"
    # A pending note older than the committed progress is not a step back; none at all is nothing to do.
    committed = json.loads((chk / "offset.json").read_text())["instant"]
    (chk / "pending.json").write_text(json.dumps({"instant": "000", "run_id": "mine"}))
    with caplog.at_level("WARNING", logger="tributary"):
        HudiSource().commit_read(ctx_for(hudi_spark, run_id="mine"), compile_pipeline(doc).spec.sources[0].options)
    assert f"progress already past 000 (at {committed}); keeping it" in caplog.text
    (chk / "pending.json").unlink()
    HudiSource().commit_read(ctx_for(hudi_spark, run_id="mine"), compile_pipeline(doc).spec.sources[0].options)
    assert json.loads((chk / "offset.json").read_text())["instant"] == committed
    # An end_instant the progress is already past reads nothing new (and says so).
    with caplog.at_level("INFO", logger="tributary"):
        nothing = read(hudi_spark, lake, "sales.orders", query_type="incremental", begin_instant="earliest",
                       end_instant=first, checkpoint_location=str(chk))
    assert nothing.count() == 0 and f"end_instant {first} is already recorded as read; nothing new" in caplog.text
    # Progress files are validated before they are trusted.
    (chk / "offset.json").write_text(json.dumps({"table": "sales.orders"}))
    with pytest.raises(TributaryError, match="offset.json is not a progress record written by the hudi source"):
        read(hudi_spark, lake, "sales.orders", query_type="incremental", begin_instant="earliest", checkpoint_location=str(chk))
    (chk / "offset.json").write_text("not json")
    with pytest.raises(TributaryError, match="offset.json is not a progress record"):
        read(hudi_spark, lake, "sales.orders", query_type="incremental", begin_instant="earliest", checkpoint_location=str(chk))


def test_checkpoint_with_a_change_feed(hudi_spark, tmp_path):
    """The same tracking for cdc reads: each run gets the changes of the commits since its predecessor."""
    lake, chk = tmp_path / "lake", tmp_path / "chk"
    cdc = {"write_options": {"hoodie.table.cdc.enabled": True}}
    write(hudi_spark, lake, "events", [(1, 10.0, "IN", 1)], **cdc)
    doc = source_pipeline(tmp_path, lake, table="events", sql="SELECT op, after.id AS id FROM orders", query_type="incremental",
                          begin_instant="earliest", incremental_format="cdc", checkpoint_location=str(chk))
    run(doc, spark=hudi_spark)
    assert parquet_rows(hudi_spark, tmp_path, "op", "id") == [("i", 1)]
    write(hudi_spark, lake, "events", [(1, 11.0, "IN", 2), (2, 20.0, "US", 1)], **cdc)
    run(doc, spark=hudi_spark)
    assert parquet_rows(hudi_spark, tmp_path, "op", "id") == [("i", 2), ("u", 1)]
    run(doc, spark=hudi_spark)
    assert parquet_rows(hudi_spark, tmp_path, "op", "id") == []


def test_soft_deletes_vanish_from_reads_and_show_in_the_change_feed(hudi_spark, tmp_path):
    """A row upserted with _hoodie_is_deleted = true is deleted (Hudi's soft delete marker,
    honoured by the sink's payload), like one removed by operation 'delete': snapshot and
    latest-state reads no longer have it, and the change feed reports a 'd'. Rows whose
    columns are merely set to null stay rows."""
    cdc = {"write_options": {"hoodie.table.cdc.enabled": True}}
    write(hudi_spark, tmp_path, "sd", [(1, 10.0, "IN", 1), (2, 20.0, "US", 1), (3, 30.0, "DE", 1)], **cdc)
    ctx = ctx_for(hudi_spark)
    flagged = hudi_spark.createDataFrame([(2, 20.0, "US", 2, True), (3, None, None, 2, False)],
                                         "id INT, amount DOUBLE, country STRING, version INT, _hoodie_is_deleted BOOLEAN")
    HudiSink().write(ctx, flagged, HudiSinkOptions(base_path=str(tmp_path), table="sd", record_key=["id"], precombine_field="version", **cdc))
    first, second = [c.timestamp for c in commits(hudi_spark, f"{tmp_path}/sd")]

    snapshot = read(hudi_spark, tmp_path, "sd")
    assert snapshot.columns == ["id", "amount", "country", "version"]  # the marker is a _hoodie_ column: dropped like the others
    assert rows(snapshot) == [(1, 10.0, "IN", 1), (3, None, None, 2)]
    assert rows(read(hudi_spark, tmp_path, "sd", include_metadata_columns=True).select("id", "_hoodie_is_deleted")) == [(1, None), (3, False)]
    assert rows(read(hudi_spark, tmp_path, "sd", query_type="incremental", begin_instant=first).select("id")) == [(3,)]
    changes = read(hudi_spark, tmp_path, "sd", query_type="incremental", begin_instant=first, incremental_format="cdc")
    assert sorted((r.op, r.before.id, r.after and r.after.id) for r in changes.collect()) == [("d", 2, None), ("u", 3, 3)]


def test_partition_pruning_and_partition_restricted_incremental_reads(hudi_spark, tmp_path):
    """A filter on a partition column downstream prunes the partitions Hudi lists (no
    files of the others are read); incremental reads take a partition path glob."""
    from tributary.plugins.sources.hudi import HudiSourceOptions as Options

    data = [(i, float(i), country, 1) for i, country in enumerate(["IN", "US", "DE", "FR"] * 5)]
    write(hudi_spark, tmp_path, "part", data, partition_by=["country"])
    first = commits(hudi_spark, f"{tmp_path}/part")[-1].timestamp
    write(hudi_spark, tmp_path, "part", [(100, 1.0, "IN", 1), (101, 1.0, "US", 1)], partition_by=["country"])

    def scan(df) -> str:
        return df._jdf.queryExecution().executedPlan().toString()

    pruned = read(hudi_spark, tmp_path, "part").filter("country = 'IN'")
    assert "PartitionFilters: [isnotnull(country" in scan(pruned) and "(country" in scan(pruned).split("PartitionFilters: ")[1]
    assert sorted(r.id for r in pruned.collect()) == [0, 4, 8, 12, 16, 100]
    # The same through a pipeline's SQL.
    doc = source_pipeline(tmp_path, tmp_path, table="part", sql="SELECT id FROM orders WHERE country = 'DE'")
    run(doc, spark=hudi_spark)
    assert parquet_rows(hudi_spark, tmp_path, "id") == [(2,), (6,), (10,), (14,), (18,)]

    since = read(hudi_spark, tmp_path, "part", query_type="incremental", begin_instant=first)
    assert sorted(r.id for r in since.collect()) == [100, 101]
    assert sorted(r.id for r in since.filter("country = 'IN'").collect()) == [100]
    only_in = Options(base_path=str(tmp_path), table="part", query_type="incremental", begin_instant=first,
                      read_options={"hoodie.datasource.read.incr.path.glob": "country=IN/*"})
    assert [r.id for r in HudiSource().read(ctx_for(hudi_spark), only_in, None).collect()] == [100]


def test_reads_follow_the_table_schema_as_it_evolves(hudi_spark, tmp_path):
    """After the sink added a column and widened a type, every read uses the table's latest
    schema: rows written before have null in the new column, the widened type applies to
    all of them, a change feed parses old and new rows alike, and time travel shows the
    schema of that time. A schema block pins what flows downstream."""
    write(hudi_spark, tmp_path, "evo", [(1, 10.0, "IN", 1), (2, 20.0, "US", 1)])
    first = commits(hudi_spark, f"{tmp_path}/evo")[-1].timestamp
    ctx = ctx_for(hudi_spark)
    wider = hudi_spark.createDataFrame([(3, 30.0, "DE", 1, "new"), (2, 25.0, "US", 2, None)],
                                       "id INT, amount DOUBLE, country STRING, version INT, note STRING")
    HudiSink().write(ctx, wider, HudiSinkOptions(base_path=str(tmp_path), table="evo", record_key=["id"], precombine_field="version"))
    wider_type = hudi_spark.createDataFrame([(4, 40.0, "FR", 1, "x")], "id LONG, amount DOUBLE, country STRING, version INT, note STRING")
    HudiSink().write(ctx, wider_type, HudiSinkOptions(base_path=str(tmp_path), table="evo", record_key=["id"], precombine_field="version"))

    latest = "struct<id:bigint,amount:double,country:string,version:int,note:string>"
    snapshot = read(hudi_spark, tmp_path, "evo")
    assert snapshot.schema.simpleString() == latest
    assert rows(snapshot) == [(1, 10.0, "IN", 1, None), (2, 25.0, "US", 2, None), (3, 30.0, "DE", 1, "new"), (4, 40.0, "FR", 1, "x")]
    since_first = read(hudi_spark, tmp_path, "evo", query_type="incremental", begin_instant=first)
    assert since_first.schema.simpleString() == latest and rows(since_first.select("id")) == [(2,), (3,), (4,)]
    assert read(hudi_spark, tmp_path, "evo", query_type="incremental", begin_instant="earliest").schema.simpleString() == latest
    as_of = read(hudi_spark, tmp_path, "evo", as_of_instant=first)
    assert as_of.schema.simpleString() == "struct<id:int,amount:double,country:string,version:int>"
    assert rows(as_of) == [(1, 10.0, "IN", 1), (2, 20.0, "US", 1)]

    # A declared schema keeps downstream steady whatever the table grows into.
    declared = hudi_spark.createDataFrame([], "id INT, amount DECIMAL(10,2)").schema
    pinned = HudiSource().read(ctx, HudiSourceOptions(base_path=str(tmp_path), table="evo"), ResolvedSchema(struct=declared))
    assert pinned.schema == declared and rows(pinned.select("id")) == [(1,), (2,), (3,), (4,)]

    # Deep validation reports the evolved schema (it is what the SQL editor offers).
    report = analyze(compile_pipeline(source_pipeline(tmp_path, tmp_path, table="evo")), spark=hudi_spark)
    assert [f["name"] for f in report["orders_src"].schema["fields"]] == ["id", "amount", "country", "version", "note"]


def test_change_feed_across_schema_evolution(hudi_spark, tmp_path):
    cdc = {"write_options": {"hoodie.table.cdc.enabled": True}}
    write(hudi_spark, tmp_path, "evo_cdc", [(1, 10.0, "IN", 1)], **cdc)
    wider = hudi_spark.createDataFrame([(1, 11.0, "IN", 2, "note")], "id INT, amount DOUBLE, country STRING, version INT, note STRING")
    HudiSink().write(ctx_for(hudi_spark), wider, HudiSinkOptions(base_path=str(tmp_path), table="evo_cdc", record_key=["id"], precombine_field="version", **cdc))
    changes = read(hudi_spark, tmp_path, "evo_cdc", query_type="incremental", begin_instant="earliest", incremental_format="cdc")
    assert changes.schema["after"].dataType.simpleString() == "struct<id:int,amount:double,country:string,version:int,note:string>"
    assert sorted((r.op, r.before and tuple(r.before), tuple(r.after)) for r in changes.collect()) == [
        ("i", None, (1, 10.0, "IN", 1, None)),  # written before the column existed
        ("u", (1, 10.0, "IN", 1, None), (1, 11.0, "IN", 2, "note")),
    ]
