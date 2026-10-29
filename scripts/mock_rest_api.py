#!/usr/bin/env python3
"""A small "university" REST API to test the REST source and enrichment against, locally.

Standard library only. Every request is logged to stdout (method, path, status, auth),
so you can watch what the pipeline does: how many calls, which pages, which students.

    python3 scripts/mock_rest_api.py                  # http://localhost:8765
    python3 scripts/mock_rest_api.py --port 9000 --token s3cret --delay 0.2

Endpoints (all JSON):
  GET  /students                      JSON array of students; ?dept=CS filters;
                                      ?page=N&size=M (page numbers from 1) or ?offset=N&limit=M paginate
  GET  /students/wrapped              {"status": 200, "data": {"students": [...], "count": N}}
  GET  /students/lines                the same students as NDJSON (one object per line)
  GET  /students/{roll_no}/grades     {"status": 200, "data": {"roll_no": N, "semesters": [{semester, gpa}, ...]}}
                                      404 for an unknown student; student 3 has no grades
  POST /grades/bulk                   body {"ids": [1, 2]} or [1, 2] -> [{"roll_no", "semester", "gpa"}, ...]
  ANY  /echo                          echoes method, query, body and a few headers back
  GET  /flaky                         503 for the first --fail-first requests, then 200
  GET  /slow?seconds=2                answers after a delay
  GET  /empty                         []
  GET  /health                        {"ok": true}

With --token, every endpoint but /health requires "Authorization: Bearer <token>" (401 otherwise).
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

STUDENTS = [
    {"roll_no": 1, "name": "Asha", "dept": "CS", "active": True, "enrolled": "2022-08-15"},
    {"roll_no": 2, "name": "Bo", "dept": "EE", "active": False, "enrolled": "2021-08-16"},
    {"roll_no": 3, "name": "Chen", "dept": "CS", "active": True, "enrolled": "2023-08-14"},
    {"roll_no": 4, "name": "Dee", "dept": "ME", "active": True, "enrolled": "2022-08-15"},
    {"roll_no": 5, "name": "Eli", "dept": "EE", "active": True, "enrolled": "2020-08-17"},
    {"roll_no": 6, "name": "Fay", "dept": "CS", "active": True, "enrolled": "2023-08-14"},
    {"roll_no": 7, "name": "Gus", "dept": "ME", "active": False, "enrolled": "2019-08-19"},
]
GRADES = {
    1: [{"semester": 1, "gpa": 3.5}, {"semester": 2, "gpa": 3.7}],
    2: [{"semester": 1, "gpa": 2.9}],
    3: [],
    4: [{"semester": 1, "gpa": 3.1}],
    5: [{"semester": 1, "gpa": 3.9}, {"semester": 2, "gpa": 4.0}],
    6: [{"semester": 1, "gpa": 3.3}],
    7: [{"semester": 1, "gpa": 2.5}, {"semester": 2, "gpa": 2.8}, {"semester": 3, "gpa": 3.0}],
}


class Handler(BaseHTTPRequestHandler):
    token: str | None = None
    delay: float = 0.0
    fail_first: int = 0
    lock = threading.Lock()

    def log_message(self, *args: Any) -> None:  # replaced by our own line per request
        pass

    def _log(self, status: int, note: str = "") -> None:
        auth = self.headers.get("Authorization")
        who = "" if not auth else " auth=" + (auth[:14] + "…" if len(auth) > 14 else auth)
        print(f"{datetime.now():%H:%M:%S.%f}"[:-3], f"{self.command} {self.path} -> {status}{who} {note}".rstrip(), flush=True)

    def _send(self, status: int, payload: Any = None, *, raw: str | None = None, note: str = "") -> None:
        if self.delay:
            time.sleep(self.delay)
        data = (raw if raw is not None else json.dumps(payload)).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json" if raw is None else "application/x-ndjson")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        self._log(status, note)

    def _body(self) -> Any:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            return json.loads(raw) if raw else None
        except json.JSONDecodeError:
            return raw.decode(errors="replace")

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def do_PUT(self) -> None:
        self._handle()

    def do_PATCH(self) -> None:
        self._handle()

    def _handle(self) -> None:
        url = urlparse(self.path)
        path, query = url.path.rstrip("/") or "/", parse_qs(url.query)
        body = self._body()
        if path == "/health":
            return self._send(200, {"ok": True})
        if self.token and self.headers.get("Authorization") != f"Bearer {self.token}":
            return self._send(401, {"error": "missing or wrong bearer token"})

        if path == "/students":
            rows = STUDENTS
            if "dept" in query:
                rows = [s for s in rows if s["dept"] == query["dept"][0]]
            if "page" in query:
                size, page = int(query.get("size", ["3"])[0]), int(query["page"][0])
                return self._send(200, rows[(page - 1) * size : page * size], note=f"page {page} size {size}")
            if "offset" in query:
                size, offset = int(query.get("limit", ["3"])[0]), int(query["offset"][0])
                return self._send(200, rows[offset : offset + size], note=f"offset {offset} limit {size}")
            return self._send(200, rows, note=f"{len(rows)} students")
        if path == "/students/wrapped":
            return self._send(200, {"status": 200, "data": {"students": STUDENTS, "count": len(STUDENTS)}})
        if path == "/students/lines":
            return self._send(200, raw="".join(json.dumps(s) + "\n" for s in STUDENTS))
        if path.startswith("/students/") and path.endswith("/grades"):
            try:
                roll = int(path.split("/")[2])
            except ValueError:
                return self._send(400, {"error": "roll number must be an integer"})
            if roll not in GRADES:
                return self._send(404, {"error": f"no student {roll}"})
            return self._send(200, {"status": 200, "data": {"roll_no": roll, "semesters": GRADES[roll]}}, note=f"{len(GRADES[roll])} semesters")
        if path == "/grades/bulk":
            ids = body.get("ids", []) if isinstance(body, dict) else (body or [])
            records = [{"roll_no": i, **g} for i in ids if i in GRADES for g in GRADES[i]]
            return self._send(200, records, note=f"{len(ids)} ids -> {len(records)} records")
        if path == "/echo":
            return self._send(200, {
                "method": self.command, "path": path, "query": query, "body": body,
                "headers": {k: v for k, v in self.headers.items() if k.lower() in ("authorization", "x-api-key", "content-type", "x-custom")},
            })
        if path == "/flaky":
            with self.lock:
                failing = self.fail_first > 0
                if failing:
                    Handler.fail_first -= 1
            return self._send(503, {"error": "try again"}) if failing else self._send(200, [{"ok": True}])
        if path == "/slow":
            time.sleep(float(query.get("seconds", ["2"])[0]))
            return self._send(200, [{"slow": True}])
        if path == "/empty":
            return self._send(200, [])
        return self._send(404, {"error": f"no route {path}; see the script's docstring"})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--token", help="require this bearer token")
    parser.add_argument("--delay", type=float, default=0.0, help="seconds each request takes (to watch concurrency/rate limits)")
    parser.add_argument("--fail-first", type=int, default=2, help="how many /flaky requests fail with 503 before it recovers")
    args = parser.parse_args()
    Handler.token, Handler.delay, Handler.fail_first = args.token, args.delay, args.fail_first
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"mock university API on http://{args.host}:{args.port}  (token: {'required' if args.token else 'none'}, delay: {args.delay}s)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        sys.stdout.flush()


if __name__ == "__main__":
    main()
