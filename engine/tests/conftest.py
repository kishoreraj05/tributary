from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("tributary-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def orders_csv(tmp_path: Path) -> Path:
    path = tmp_path / "orders.csv"
    path.write_text(
        "order_id,amount,country\n"
        "1,10.0,IN\n"
        "2,-1.0,US\n"
        ",5.0,DE\n"
        "4,7.5,USA\n"
        "5,3.0,GB\n"
    )
    return path


def pipeline(tmp_path: Path, source_path: Path, **overrides) -> dict:
    """A small batch pipeline; tests tweak pieces of it via keyword overrides."""
    doc = {
        "version": 1,
        "pipeline": {"id": "test_pipeline", "mode": "batch"},
        "environments": {"dev": {"vars": {"out": str(tmp_path / "out")}}},
        "sources": [
            {
                "id": "orders_src",
                "type": "file",
                "options": {"path": str(source_path), "format": "csv", "read_options": {"header": True}},
                "schema": {"kind": "ddl", "ddl": "order_id INT, amount DOUBLE, country STRING"},
            }
        ],
        "transforms": [
            {
                "id": "orders_clean",
                "type": "sql",
                "inputs": [{"node": "orders_src", "as": "orders"}],
                "options": {"sql": "SELECT order_id, amount, upper(country) AS country FROM orders"},
            }
        ],
        "quality": [],
        "sinks": [
            {
                "id": "orders_out",
                "type": "file",
                "input": "orders_clean",
                "options": {"path": "${env.out}/orders", "format": "parquet", "mode": "overwrite"},
            }
        ],
    }
    doc.update(overrides)
    return doc


ROW_CHECKS = [
    {"type": "not_null", "column": "order_id"},
    {"type": "range", "column": "amount", "min": 0},
    {"type": "expression", "expr": "length(country) = 2"},
]
