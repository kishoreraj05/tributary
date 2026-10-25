"""Tests run against a real Postgres (the docker-compose one by default).

TRIBUTARY_TEST_ADMIN_URL points at a server where the 'tributary_test' database can be
(re)created; tests are skipped when it is unreachable.
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

import psycopg
import pytest

ADMIN_URL = os.environ.get("TRIBUTARY_TEST_ADMIN_URL", "postgresql://tributary:tributary@localhost:5432/tributary")
TEST_DB = "tributary_test"
DATA_DIR = Path(tempfile.mkdtemp(prefix="tributary-backend-tests-"))

os.environ["TRIBUTARY_DATABASE_URL"] = ADMIN_URL.replace("postgresql://", "postgresql+psycopg://").rsplit("/", 1)[0] + f"/{TEST_DB}"
os.environ["TRIBUTARY_DATA_DIR"] = str(DATA_DIR)
os.environ["TRIBUTARY_SPARK_MASTER"] = "local[2]"
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def _recreate_database() -> None:
    with psycopg.connect(ADMIN_URL, autocommit=True, connect_timeout=3) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        conn.execute(f"CREATE DATABASE {TEST_DB}")


@pytest.fixture(scope="session", autouse=True)
def database():
    try:
        _recreate_database()
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable at {ADMIN_URL}: {exc}")
    from tributary_backend.migrate import upgrade

    upgrade()
    yield


@pytest.fixture(autouse=True)
def clean_tables(database):
    yield
    from sqlalchemy import text

    from tributary_backend.db import get_engine

    with get_engine().begin() as conn:
        conn.execute(text("TRUNCATE quality_results, runs, pipeline_versions, pipelines, connections CASCADE"))


@pytest.fixture(scope="session")
def client(database):
    from fastapi.testclient import TestClient

    from tributary_backend.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def orders_csv(tmp_path: Path) -> Path:
    path = tmp_path / "orders.csv"
    path.write_text("order_id,amount,country\n1,10.0,IN\n2,-1.0,US\n,5.0,DE\n4,7.5,GB\n")
    return path


def spec(source: Path, out: Path, **overrides) -> dict:
    doc = {
        "pipeline": {"id": "orders", "mode": "batch"},
        "environments": {"dev": {"vars": {"out": str(out)}}},
        "sources": [{
            "id": "orders_src",
            "type": "file",
            "options": {"path": str(source), "format": "csv", "read_options": {"header": True}},
            "schema": {"kind": "ddl", "ddl": "order_id INT, amount DOUBLE, country STRING"},
        }],
        "transforms": [{
            "id": "orders_clean",
            "type": "sql",
            "inputs": [{"node": "orders_src", "as": "orders"}],
            "options": {"sql": "SELECT order_id, amount, upper(country) AS country FROM orders"},
        }],
        "quality": [{
            "id": "dq",
            "attach_to": "orders_clean",
            "action": "drop",
            "checks": [{"type": "not_null", "column": "order_id"}, {"type": "range", "column": "amount", "min": 0}],
        }],
        "sinks": [{
            "id": "orders_out",
            "type": "file",
            "input": "orders_clean",
            "options": {"path": "${env.out}/orders", "mode": "overwrite"},
        }],
    }
    doc.update(overrides)
    return doc


def create_pipeline(client, doc: dict, name: str = "orders") -> dict:
    response = client.post("/api/v1/pipelines", json={"name": name, "spec": doc})
    assert response.status_code == 201, response.text
    return response.json()


def wait_for(client, run_id: str, statuses: tuple[str, ...], timeout: float = 180) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = client.get(f"/api/v1/runs/{run_id}").json()
        if run["status"] in statuses:
            return run
        time.sleep(1)
    logs = client.get(f"/api/v1/runs/{run_id}/logs").json()["lines"]
    raise AssertionError(f"run stayed {run['status']}; last log lines:\n" + "\n".join(logs[-40:]))
