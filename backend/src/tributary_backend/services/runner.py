"""Runs pipelines in a separate worker process each, so a run never blocks or crashes the API.

The worker (``python -m tributary_backend.worker RUN_ID``) owns its own Spark driver and
reports status straight to the database. This runner starts, inspects and cancels it.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import update
from sqlalchemy.orm import Session

from ..models import Run
from ..settings import get_settings

ACTIVE = ("queued", "running")
TERMINAL = ("succeeded", "failed", "cancelled")
_procs: dict[str, subprocess.Popen] = {}


def _child_env() -> dict[str, str]:
    settings = get_settings()
    env = dict(os.environ)
    env["TRIBUTARY_DATABASE_URL"] = settings.database_url
    env["TRIBUTARY_DATA_DIR"] = str(settings.data_dir.resolve())
    if settings.secret_key:
        env["TRIBUTARY_SECRET_KEY"] = settings.secret_key
    env.setdefault("PYSPARK_PYTHON", sys.executable)
    return env


def launch(session: Session, run: Run) -> Run:
    """``run`` must already be committed (status queued) so the worker can load it."""
    logs = get_settings().logs_dir
    logs.mkdir(parents=True, exist_ok=True)
    log_path = (logs / f"{run.id}.log").resolve()
    with open(log_path, "ab") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "tributary_backend.worker", run.id],
            stdout=log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,  # own process group: cancel kills the Spark JVM too
            env=_child_env(),
        )
    _procs[run.id] = proc
    run.pid = proc.pid
    run.log_path = str(log_path)
    session.commit()
    return run


def _alive(run: Run) -> bool:
    proc = _procs.get(run.id)
    if proc is not None:
        return proc.poll() is None
    if not run.pid:
        return False
    try:
        os.kill(run.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def refresh(session: Session, run: Run) -> Run:
    """Marks a run failed if its worker died without reporting (crash, OOM kill, reboot)."""
    if run.status not in ACTIVE or _alive(run):
        return run
    session.refresh(run)
    if run.status in ACTIVE:
        proc = _procs.get(run.id)
        code = proc.returncode if proc is not None else None
        _finish(session, run.id, "failed", error=f"worker process exited unexpectedly (exit code {code})")
        session.refresh(run)
    _procs.pop(run.id, None)
    return run


def cancel(session: Session, run: Run, grace_seconds: float = 10.0) -> Run:
    if run.status not in ACTIVE:
        return run
    if run.pid and _alive(run):
        _signal_group(run.pid, signal.SIGTERM)
        deadline = time.monotonic() + grace_seconds
        while _alive(run) and time.monotonic() < deadline:
            time.sleep(0.2)
        if _alive(run):
            _signal_group(run.pid, signal.SIGKILL)
    _finish(session, run.id, "cancelled", error="cancelled by user")
    _procs.pop(run.id, None)
    session.refresh(run)
    return run


def _signal_group(pid: int, sig: int) -> None:
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def _finish(session: Session, run_id: str, status: str, **fields) -> None:
    """Conditional update: whoever reaches a terminal state first wins."""
    session.execute(
        update(Run)
        .where(Run.id == run_id, Run.status.in_(ACTIVE))
        .values(status=status, finished_at=datetime.now(timezone.utc), **fields)
    )
    session.commit()


def tail(path: str | None, lines: int) -> list[str]:
    if not path or not Path(path).exists():
        return []
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        f.seek(max(0, size - 256 * 1024))
        data = f.read().decode(errors="replace").splitlines()
    return data[-lines:]


def read_from(path: str | None, offset: int) -> tuple[list[str], int]:
    """New complete lines since ``offset`` (used by the events WebSocket)."""
    if not path or not Path(path).exists():
        return [], offset
    with open(path, "rb") as f:
        f.seek(offset)
        chunk = f.read()
    end = chunk.rfind(b"\n")
    if end < 0:
        return [], offset
    return chunk[: end + 1].decode(errors="replace").splitlines(), offset + end + 1
