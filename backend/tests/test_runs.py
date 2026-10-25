from __future__ import annotations

import os
import time

from conftest import create_pipeline, spec, wait_for

from tributary_backend.db import session_factory
from tributary_backend.models import Run


def _versioned(client, doc, name="orders"):
    pid = create_pipeline(client, doc, name)["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    return pid


def test_batch_run_records_status_quality_and_logs(client, orders_csv, tmp_path):
    out = tmp_path / "out"
    client.post("/api/v1/connections", json={
        "name": "lake", "type": "file", "secret_fields": ["path"],
        "environments": {"dev": {"path": str(out / "orders"), "mode": "overwrite"}},
    })
    doc = spec(orders_csv, out)
    doc["sinks"][0] = {"id": "orders_out", "type": "file", "input": "orders_clean", "connection": "lake"}
    pid = _versioned(client, doc)

    started = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"})
    assert started.status_code == 202, started.text
    run = started.json()
    assert run["status"] in ("queued", "running")
    detail = client.get(f"/api/v1/runs/{run['id']}").json()
    with session_factory()() as session:
        snapshot = session.get(Run, run["id"]).resolved_spec
    assert snapshot["environments"]["dev"]["connections"]["lake"]["path"] == "********"  # never stored in clear

    done = wait_for(client, run["id"], ("succeeded", "failed"))
    assert done["status"] == "succeeded", done
    assert done["summary"]["sinks_written"] == ["orders_out"]
    assert sorted(done["summary"]["failed_checks"]) == ["not_null_1", "range_2"]
    assert (out / "orders" / "_SUCCESS").exists()  # path came from the decrypted secret

    quality = client.get(f"/api/v1/runs/{run['id']}/quality").json()
    assert {(q["check_id"], q["failed_rows"]) for q in quality} == {("not_null_1", 1), ("range_2", 1)}
    history = client.get("/api/v1/quality", params={"pipeline_id": pid, "passed": False}).json()
    assert len(history) == 2
    assert any("succeeded" in line for line in client.get(f"/api/v1/runs/{run['id']}/logs").json()["lines"])
    assert detail["id"] == run["id"]


def test_quality_fail_marks_run_failed(client, orders_csv, tmp_path):
    doc = spec(orders_csv, tmp_path / "out")
    doc["quality"][0]["action"] = "fail"
    pid = _versioned(client, doc)
    run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).json()
    done = wait_for(client, run["id"], ("succeeded", "failed"))
    assert done["status"] == "failed" and "quality 'dq' failed" in done["error"]
    assert not (tmp_path / "out" / "orders").exists()


def test_streaming_run_can_be_cancelled_and_streams_events(client, tmp_path):
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"id": 1}\n')
    doc = {
        "pipeline": {"id": "events", "mode": "streaming",
                     "streaming": {"checkpoint_location": str(tmp_path / "chk"), "interval": "1 second"}},
        "sources": [{"id": "src", "type": "file", "options": {"path": str(incoming), "format": "json"},
                     "schema": {"kind": "ddl", "ddl": "id INT"}}],
        "sinks": [{"id": "out", "type": "file", "input": "src", "options": {"path": str(tmp_path / "out")}}],
    }
    pid = _versioned(client, doc, "events")
    run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "default"}).json()
    wait_for(client, run["id"], ("running",))
    deadline = time.monotonic() + 120
    while not (tmp_path / "out" / "_SUCCESS").exists() and time.monotonic() < deadline:
        time.sleep(1)
    assert (tmp_path / "out" / "_SUCCESS").exists(), "first micro-batch was not written"

    with client.websocket_connect(f"/api/v1/runs/{run['id']}/events") as ws:
        first = ws.receive_json()
        assert first == {"type": "status", "run": first["run"]} and first["run"]["status"] == "running"

        cancelled = client.post(f"/api/v1/runs/{run['id']}/cancel").json()
        assert cancelled["status"] == "cancelled"
        statuses = [first["run"]["status"]]
        while True:
            try:
                message = ws.receive_json()
            except Exception:  # noqa: BLE001 - server closed the socket after the final status
                break
            if message["type"] == "status":
                statuses.append(message["run"]["status"])
        assert statuses[-1] == "cancelled"

    with session_factory()() as session:
        pid_ = session.get(Run, run["id"]).pid
    try:
        os.kill(pid_, 0)
        alive = True
    except ProcessLookupError:
        alive = False
    assert not alive, "worker process survived cancel"


def test_dead_worker_is_reconciled(client, orders_csv, tmp_path):
    pid = _versioned(client, spec(orders_csv, tmp_path / "out"))
    with session_factory()() as session:
        run = Run(pipeline_id=pid, version=1, env="dev", mode="batch", status="running", pid=2**22 - 1,
                  summary={}, resolved_spec={})
        session.add(run)
        session.commit()
        run_id = run.id
    got = client.get(f"/api/v1/runs/{run_id}").json()
    assert got["status"] == "failed" and "exited unexpectedly" in got["error"]
