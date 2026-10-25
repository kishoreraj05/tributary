from __future__ import annotations

from conftest import pipeline
from tributary import compile_pipeline
from tributary.runtime import decide_resources, required_packages
from tributary.runtime.resources import GB, _auto_profile


def test_local_run_keeps_adaptive_settings(tmp_path, orders_csv):
    decision = decide_resources(compile_pipeline(pipeline(tmp_path, orders_csv)))
    assert decision.master == "local[*]"
    assert decision.profile == "S"
    assert decision.conf["spark.sql.adaptive.enabled"] == "true"
    assert "spark.executor.memory" not in decision.conf


def test_cluster_profile_and_overrides(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv, runtime={"dev": {
        "master": "yarn",
        "resources": {"profile": "L", "max_executors": 12},
        "spark_conf": {"spark.executor.memory": "20g"},
    }})
    conf = decide_resources(compile_pipeline(doc)).conf
    assert conf["spark.dynamicAllocation.maxExecutors"] == "12"
    assert conf["spark.dynamicAllocation.minExecutors"] == "4"
    assert conf["spark.executor.memory"] == "20g"  # raw spark_conf wins


def test_auto_profile_thresholds():
    assert _auto_profile(500 * 1024**2, 0, "batch", []) == "S"
    assert _auto_profile(5 * GB, 0, "batch", []) == "M"
    assert _auto_profile(5 * GB, 4, "batch", []) == "L"  # complex SQL bumps a level
    assert _auto_profile(None, 0, "streaming", []) == "M"
    assert _auto_profile(500 * GB, 9, "batch", []) == "XL"


def test_packages_come_from_used_plugins_only(tmp_path, orders_csv):
    assert required_packages(compile_pipeline(pipeline(tmp_path, orders_csv))) == []
    doc = pipeline(tmp_path, orders_csv)
    doc["sources"][0] = {"id": "orders_src", "type": "kafka", "options": {"brokers": "x:1", "topic": "t", "value_format": "avro"},
                         "schema": {"kind": "registry", "subject": "t-value", "url": "http://r"}}
    packages = required_packages(compile_pipeline(doc))
    assert any("spark-sql-kafka-0-10_2.12" in p for p in packages)
    assert any("spark-avro_2.12" in p for p in packages)
