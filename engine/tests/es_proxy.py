"""An HTTP proxy in front of a real Elasticsearch that tampers with bulk requests, so tests
can see what the connector does when the cluster pushes back: a whole request refused
with 429 (thread pool full), some items of a request rejected with 429 (partial success),
or a cluster that never recovers. Everything else is forwarded untouched."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REJECTED = {"type": "es_rejected_execution_exception", "reason": "rejected execution of coordinating operation: queue capacity reached"}


class FlakyElasticsearch:
    """``plan`` is consumed one entry per bulk request: "ok" (forward), "429" (refuse the
    request), "items429" (forward every other document, reject the rest per item). After
    the plan runs out every request is forwarded. ``bulks`` records (entry, documents)."""

    def __init__(self, upstream: str, plan: list[str] | None = None):
        self.upstream = upstream.rstrip("/")
        self.plan = list(plan or [])
        self.bulks: list[tuple[str, int]] = []
        self.lock = threading.Lock()
        proxy = self
        upstream_url = self.upstream

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):  # quiet
                pass

            def _body(self) -> bytes:
                length = int(self.headers.get("Content-Length") or 0)
                return self.rfile.read(length) if length else b""

            def _reply(self, status: int, body: bytes, content_type: str = "application/json") -> None:
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("X-elastic-product", "Elasticsearch")  # the connector checks it is talking to Elasticsearch
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)

            def _forward(self, body: bytes) -> tuple[int, bytes, str]:
                request = urllib.request.Request(upstream_url + self.path, data=body or None, method=self.command)
                for name in ("Content-Type", "Authorization"):
                    if self.headers.get(name):
                        request.add_header(name, self.headers[name])
                try:
                    with urllib.request.urlopen(request, timeout=30) as response:
                        return response.status, response.read(), response.headers.get("Content-Type", "application/json")
                except urllib.error.HTTPError as exc:
                    return exc.code, exc.read(), exc.headers.get("Content-Type", "application/json")

            def _handle(self) -> None:
                body = self._body()
                if "_bulk" not in self.path:
                    self._reply(*self._forward(body))
                    return
                with proxy.lock:
                    entry = proxy.plan.pop(0) if proxy.plan else "ok"
                lines = body.decode().splitlines()
                pairs = [(lines[i], lines[i + 1]) for i in range(0, len(lines), 2)]
                proxy.bulks.append((entry, len(pairs)))
                if entry == "429":
                    self._reply(429, json.dumps({"error": REJECTED, "status": 429}).encode())
                elif entry == "items429":
                    kept = [p for i, p in enumerate(pairs) if i % 2 == 0]
                    forwarded = "".join(f"{a}\n{d}\n" for a, d in kept).encode()
                    status, response, _ = self._forward(forwarded)
                    upstream = json.loads(response)["items"] if status == 200 else []
                    items, it = [], iter(upstream)
                    for i, (action, _doc) in enumerate(pairs):
                        if i % 2 == 0:
                            items.append(next(it))
                        else:
                            (op, meta), = json.loads(action).items()
                            items.append({op: {"_index": meta.get("_index"), "_id": meta.get("_id"), "status": 429, "error": REJECTED}})
                    self._reply(200, json.dumps({"took": 1, "errors": True, "items": items}).encode())
                else:
                    self._reply(*self._forward(body))

            do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = _handle

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def nodes(self) -> str:
        host, port = self.server.server_address
        return f"{host}:{port}"

    def __enter__(self) -> FlakyElasticsearch:
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.server.shutdown()
        self.server.server_close()
