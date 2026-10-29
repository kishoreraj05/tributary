"""A small REST API on localhost for the REST source/enrichment tests.

Serves a student directory in every shape the plugins understand (array, wrapped object,
NDJSON, paginated), per-student grades (with a 404 for unknown students), a bulk grades
endpoint, an echo endpoint and a flaky one. Every request is logged with its timing so
tests can assert how many calls were made, how concurrent they were and how fast.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

STUDENTS = [
    {"roll_no": 1, "name": "Asha", "dept": "CS", "active": True},
    {"roll_no": 2, "name": "Bo", "dept": "EE", "active": False},
    {"roll_no": 3, "name": "Chen", "dept": "CS", "active": True},
    {"roll_no": 4, "name": "Dee", "dept": "ME", "active": True},
    {"roll_no": 5, "name": "Eli", "dept": "EE", "active": True},
]
GRADES = {
    1: [{"semester": 1, "gpa": 3.5}, {"semester": 2, "gpa": 3.7}],
    2: [{"semester": 1, "gpa": 2.9}],
    3: [],
    4: [{"semester": 1, "gpa": 3.1}],
    5: [{"semester": 1, "gpa": 3.9}, {"semester": 2, "gpa": 4.0}],
}


@dataclass
class Call:
    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]
    body: Any
    started: float
    in_flight: int  # requests being served when this one started (including itself)


@dataclass
class ApiState:
    calls: list[Call] = field(default_factory=list)
    delay: float = 0.0  # seconds each request takes (to observe concurrency)
    fail_first: int = 0  # /flaky: how many requests still fail with 503
    lock: threading.Lock = field(default_factory=threading.Lock)
    in_flight: int = 0

    def reset(self, *, delay: float = 0.0, fail_first: int = 0) -> None:
        with self.lock:
            self.calls.clear()
            self.delay, self.fail_first, self.in_flight = delay, fail_first, 0

    def paths(self) -> list[str]:
        return [c.path for c in self.calls]

    def max_in_flight(self) -> int:
        return max((c.in_flight for c in self.calls), default=0)


class _Handler(BaseHTTPRequestHandler):
    state: ApiState  # set by start()

    def log_message(self, *args: Any) -> None:  # quiet
        pass

    def _record(self) -> tuple[Call, str, dict[str, list[str]]]:
        url = urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            body = raw.decode()
        with self.state.lock:
            self.state.in_flight += 1
            call = Call(self.command, url.path, parse_qs(url.query), dict(self.headers), body, time.monotonic(), self.state.in_flight)
            self.state.calls.append(call)
        return call, url.path, call.query

    def _send(self, status: int, payload: Any, *, raw: str | None = None) -> None:
        if self.state.delay:
            time.sleep(self.state.delay)
        text = raw if raw is not None else json.dumps(payload)
        data = text.encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        with self.state.lock:
            self.state.in_flight -= 1

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def do_PUT(self) -> None:
        self._handle()

    def _handle(self) -> None:
        call, path, query = self._record()
        state = self.state
        if path == "/students":
            return self._students(query)
        if path == "/students/wrapped":
            return self._send(200, {"status": 200, "data": {"students": STUDENTS, "count": len(STUDENTS)}})
        if path == "/students/lines":
            return self._send(200, None, raw="".join(json.dumps(s) + "\n" for s in STUDENTS))
        if path.startswith("/students/") and path.endswith("/grades"):
            try:
                roll = int(path.split("/")[2])
            except ValueError:
                return self._send(400, {"error": "bad roll number"})
            if roll not in GRADES:
                return self._send(404, {"error": f"no student {roll}"})
            return self._send(200, {"status": 200, "data": {"roll_no": roll, "semesters": GRADES[roll]}})
        if path == "/grades/bulk":
            ids = call.body["ids"] if isinstance(call.body, dict) else call.body
            records = [{"roll_no": i, **g} for i in ids if i in GRADES for g in GRADES[i]]
            return self._send(200, records)
        if path == "/echo":
            return self._send(200, {"method": self.command, "query": query, "body": call.body, "auth": self.headers.get("Authorization"),
                                    "api_key": self.headers.get("X-API-Key"), "custom": self.headers.get("X-Custom"),
                                    "content_type": self.headers.get("Content-Type")})
        if path == "/slow":
            time.sleep(float(query.get("seconds", ["1"])[0]))
            return self._send(200, [{"slow": True}])
        if path == "/empty":
            return self._send(200, [])
        if path == "/bad-lines":
            return self._send(200, None, raw='{"a": 1}\nnot json\n')
        if path == "/flaky":
            with state.lock:
                failing = state.fail_first > 0
                state.fail_first -= 1
            if failing:
                return self._send(503, {"error": "try again"})
            return self._send(200, [{"ok": True}])
        if path == "/broken":
            return self._send(200, None, raw="this is not json")
        return self._send(404, {"error": "not found"})

    def _students(self, query: dict[str, list[str]]) -> None:
        rows = STUDENTS
        if "dept" in query:
            rows = [s for s in rows if s["dept"] == query["dept"][0]]
        if "page" in query:
            size = int(query.get("size", ["2"])[0])
            page = int(query["page"][0])
            rows = rows[(page - 1) * size : page * size]
        elif "offset" in query:
            size = int(query.get("limit", ["2"])[0])
            offset = int(query["offset"][0])
            rows = rows[offset : offset + size]
        self._send(200, rows)


@dataclass
class Api:
    url: str
    state: ApiState
    server: ThreadingHTTPServer

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def start() -> Api:
    state = ApiState()
    handler = type("Handler", (_Handler,), {"state": state})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return Api(f"http://127.0.0.1:{server.server_address[1]}", state, server)
