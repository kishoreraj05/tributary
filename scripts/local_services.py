#!/usr/bin/env python3
"""Start, stop and seed the local Docker services behind Tributary's sources and sinks.

Name the connectors a pipeline uses (or point at the pipeline) and only their services
start; ``all`` starts every one. Connectors that need no server (files, Delta, Hudi and
Iceberg on local disk, the console sink, SQL and Deequ transforms) are reported as such.

    scripts/local_services.py list
    scripts/local_services.py up mongodb kafka
    scripts/local_services.py up --pipeline examples/csv_to_mongodb.yaml
    scripts/local_services.py up all
    scripts/local_services.py env mongodb elasticsearch     # connections for environments.local
    scripts/local_services.py seed mongodb                  # sample orders into each service
    scripts/local_services.py status
    scripts/local_services.py down                          # stop what this script started

Services run in the Docker Compose project ``tributary`` (deploy/docker-compose.yml). A
service whose port already answers (another copy of the stack, a server of your own) is
reused rather than started twice. Standard library only: runs with any python3.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import socket
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
COMPOSE_FILE = ROOT / "deploy" / "docker-compose.yml"
PROJECT = "tributary"
ORDERS_CSV = ROOT / "examples" / "data" / "orders.csv"


@dataclass(frozen=True)
class Service:
    name: str  # the docker compose service
    port: int  # the host port it answers on
    what: str


SERVICES = {
    "redpanda": Service("redpanda", 19092, "Kafka API on localhost:19092, Schema Registry on localhost:18081"),
    "mongodb": Service("mongodb", 27017, "MongoDB 7, no auth, on localhost:27017"),
    "elasticsearch": Service("elasticsearch", 9200, "Elasticsearch 8, security off, on http://localhost:9200"),
    "postgres": Service("postgres", 5432, "PostgreSQL 16 (tributary/tributary) on localhost:5432"),
    "mock-api": Service("mock-api", 8765, "Mock REST API (bearer token local-token) on http://localhost:8765"),
    "pushgateway": Service("pushgateway", 9091, "Prometheus Pushgateway on http://localhost:9091"),
    "minio": Service("minio", 9000, "MinIO (S3) on http://localhost:9000, console :9001, tributary/tributary123"),
}

# Connector (plugin type, as written in a pipeline) -> the services it needs.
CONNECTORS: dict[str, tuple[str, ...]] = {
    "kafka": ("redpanda",),
    "mongodb": ("mongodb",),
    "elasticsearch": ("elasticsearch",),
    "rest_api": ("mock-api",),
    "rest_api_enrichment": ("mock-api",),
    "jdbc": ("postgres",),
    "postgres": ("postgres",),
    "prometheus": ("pushgateway",),
    "s3": ("minio",),
}
# Connectors that run without any service locally.
NO_SERVICE = {
    "file": "local disk (or s3 with the 's3' service)",
    "delta": "local disk (a path)",
    "hudi": "local disk (base_path)",
    "iceberg": "local disk (catalog_type: hadoop with a local warehouse)",
    "console": "prints to the run log",
    "dummy": "the local environment's dummy data",
    "sql": "runs in Spark",
    "deequ": "runs in Spark (the jar is fetched on first use)",
}
ALIASES = {"es": "elasticsearch", "mongo": "mongodb", "rest": "rest_api", "minio": "s3", "redpanda": "kafka", "postgresql": "postgres"}

# What to put in environments.local.connections for each connector, so pipeline nodes only
# name the connection (connection: local_mongodb) and keep their database/index/topic.
CONNECTION_SETTINGS = {
    "kafka": ("local_kafka", {"type": "kafka", "brokers": "localhost:19092"}),
    "mongodb": ("local_mongodb", {"type": "mongodb", "connection_uri": "mongodb://localhost:27017"}),
    "elasticsearch": ("local_elasticsearch", {"type": "elasticsearch", "nodes": "localhost:9200"}),
    "rest_api": ("local_rest", {"type": "rest_api", "baseUrl": "http://localhost:8765", "authType": "bearer", "token": "local-token"}),
    "jdbc": ("local_postgres", {"type": "jdbc", "url": "jdbc:postgresql://localhost:5432/tributary", "user": "tributary", "password": "tributary"}),
    "prometheus": ("local_pushgateway", {"type": "prometheus", "url": "http://localhost:9091"}),
}


def canonical(name: str) -> str:
    name = name.strip().lower()
    return ALIASES.get(name, name)


def answers(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), timeout=0.5):
            return True
    except OSError:
        return False


def compose(*args: str, check: bool = True, dry_run: bool = False) -> subprocess.CompletedProcess | None:
    cmd = ["docker", "compose", "-p", PROJECT, "-f", str(COMPOSE_FILE), *args]
    if dry_run:
        print("would run:", " ".join(cmd))
        return None
    return subprocess.run(cmd, check=check)


def pipeline_types(path: Path) -> list[str]:
    """The source, sink, transform and metrics-sink types a pipeline file uses."""
    text = path.read_text()
    try:
        doc = json.loads(text)
    except json.JSONDecodeError:
        try:
            import yaml  # the engine's virtualenv has it
        except ImportError:
            sys.exit(f"{path} is YAML: run this script with the engine's python (engine/.venv/bin/python), which has PyYAML")
        doc = yaml.safe_load(text)
    types: list[str] = []

    def add(node):
        if isinstance(node, dict) and isinstance(node.get("type"), str):
            types.append(node["type"])

    for key in ("sources", "transforms", "sinks"):
        for node in doc.get(key) or []:
            add(node)
    for block in doc.get("quality") or []:
        for target in block.get("results_sinks") or []:
            add(target)
        add(block.get("quarantine_sink"))
    return list(dict.fromkeys(types))


def resolve(names: list[str], pipelines: list[Path]) -> tuple[list[str], list[str], list[str]]:
    """-> (connectors, services to start, unknown names)."""
    connectors = [canonical(n) for n in names]
    for path in pipelines:
        connectors += pipeline_types(path)
    if "all" in connectors:
        return sorted(CONNECTORS), list(SERVICES), []
    connectors = list(dict.fromkeys(connectors))
    services, unknown = [], []
    for c in connectors:
        if c in CONNECTORS:
            services += [s for s in CONNECTORS[c] if s not in services]
        elif c not in NO_SERVICE:
            unknown.append(c)
    return connectors, services, unknown


def cmd_list(_args) -> None:
    print("Connectors and the local services they need:\n")
    for c, svcs in sorted(CONNECTORS.items()):
        print(f"  {c:21} -> {', '.join(svcs):14} {SERVICES[svcs[0]].what}")
    print()
    for c, how in NO_SERVICE.items():
        print(f"  {c:21} -> {'(none)':14} {how}")
    print("\nAliases:", ", ".join(f"{a}={c}" for a, c in sorted(ALIASES.items())))


def cmd_up(args) -> None:
    connectors, services, unknown = resolve(args.names, args.pipeline)
    if unknown:
        sys.exit(f"unknown connector(s): {', '.join(unknown)} (see: {Path(sys.argv[0]).name} list)")
    for c in connectors:
        if c in NO_SERVICE:
            print(f"  {c}: nothing to start ({NO_SERVICE[c]})")
    if not services:
        print("No services needed.")
        return
    reuse = [s for s in services if answers(SERVICES[s].port)]
    start = [s for s in services if s not in reuse]
    for s in reuse:
        print(f"  {s}: already answering on localhost:{SERVICES[s].port}, reusing it")
    if start:
        if not shutil.which("docker"):
            sys.exit("docker is not installed or not on PATH")
        print(f"Starting {', '.join(start)} ...")
        compose("up", "-d", *([] if args.no_wait else ["--wait"]), *start, dry_run=args.dry_run)
    for s in services:
        print(f"  {s}: {SERVICES[s].what}")
    wanted = [c for c in connectors if c in CONNECTION_SETTINGS]
    if wanted and not args.dry_run:
        print("\nConnections for the pipeline's local environment (also: env " + " ".join(wanted) + "):\n")
        print(env_snippet(wanted))


def cmd_down(args) -> None:
    _, services, unknown = resolve(args.names or ["all"], [])
    if unknown:
        sys.exit(f"unknown connector(s): {', '.join(unknown)}")
    # Only the 'tributary' project's containers are touched: reused servers keep running.
    compose("stop", *services, dry_run=args.dry_run)
    if args.remove:
        compose("rm", "-f", *services, dry_run=args.dry_run)


def cmd_status(_args) -> None:
    for s in SERVICES.values():
        print(f"  {s.name:14} localhost:{s.port:<6} {'up' if answers(s.port) else 'down'}")


def env_snippet(connectors: list[str]) -> str:
    lines = ["environments:", "  local:", "    connections:"]
    for c in connectors:
        if c not in CONNECTION_SETTINGS:
            continue
        name, settings = CONNECTION_SETTINGS[c]
        lines.append(f"      {name}:")
        lines += [f"        {k}: {json.dumps(v)}" for k, v in settings.items()]
    return "\n".join(lines)


def cmd_env(args) -> None:
    connectors, _, unknown = resolve(args.names, args.pipeline)
    if unknown:
        sys.exit(f"unknown connector(s): {', '.join(unknown)}")
    wanted = [c for c in connectors if c in CONNECTION_SETTINGS] or [c for c in CONNECTION_SETTINGS if not args.names]
    print(env_snippet(wanted))


def orders() -> list[dict]:
    def number(v: str):
        try:
            return int(v)
        except ValueError:
            try:
                return float(v)
            except ValueError:
                return v or None

    with ORDERS_CSV.open() as f:
        return [{k: number(v) for k, v in row.items()} for row in csv.DictReader(f)]


def seed_kafka(dry_run: bool) -> None:
    records = "\n".join(json.dumps(r) for r in orders()) + "\n"
    if dry_run:
        print("would produce", len(orders()), "orders to topic 'orders'")
        return
    subprocess.run(["docker", "exec", container_for("redpanda"), "rpk", "topic", "create", "orders"], check=False, capture_output=True)
    subprocess.run(["docker", "exec", "-i", container_for("redpanda"), "rpk", "topic", "produce", "orders"], input=records.encode(), check=True)


def seed_mongodb(dry_run: bool) -> None:
    script = f"db = db.getSiblingDB('tributary'); db.orders.drop(); db.orders.insertMany({json.dumps(orders())}); print(db.orders.countDocuments())"
    if dry_run:
        print("would insert", len(orders()), "orders into tributary.orders")
        return
    subprocess.run(["docker", "exec", "-i", container_for("mongodb"), "mongosh", "--quiet", "--eval", script], check=True)


def seed_elasticsearch(dry_run: bool) -> None:
    script = ROOT / "deploy" / "seed_elasticsearch.sh"
    if dry_run:
        print("would run", script)
        return
    subprocess.run([str(script)], check=True)


SEEDERS = {"kafka": seed_kafka, "mongodb": seed_mongodb, "elasticsearch": seed_elasticsearch}


def container_for(service: str) -> str:
    """The running container of a service: this project's, or a reused one from another copy."""
    out = subprocess.run(
        ["docker", "ps", "--filter", f"label=com.docker.compose.service={service}", "--format", "{{.Names}}"],
        capture_output=True, text=True, check=True,
    ).stdout.split()
    if not out:
        sys.exit(f"no running container for {service}: start it first (up {service})")
    ours = [n for n in out if n.startswith(f"{PROJECT}-")]
    return (ours or out)[0]


def cmd_seed(args) -> None:
    names = [canonical(n) for n in args.names] or list(SEEDERS)
    for n in names:
        if n not in SEEDERS:
            sys.exit(f"no sample data for {n}; seedable: {', '.join(SEEDERS)}")
        print(f"Seeding {n} with examples/data/orders.csv ...")
        SEEDERS[n](args.dry_run)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    def with_names(p, pipeline: bool = True):
        p.add_argument("names", nargs="*", help="connector names (kafka, mongodb, elasticsearch, rest_api, jdbc, prometheus, s3) or 'all'")
        if pipeline:
            p.add_argument("--pipeline", "-p", type=Path, action="append", default=[], help="start what this pipeline file uses")
        p.add_argument("--dry-run", action="store_true", help="print the docker commands instead of running them")
        return p

    sub.add_parser("list", help="connectors and their services").set_defaults(func=cmd_list)
    up = with_names(sub.add_parser("up", help="start the services of the named connectors"))
    up.add_argument("--no-wait", action="store_true", help="do not wait for health checks")
    up.set_defaults(func=cmd_up)
    down = with_names(sub.add_parser("down", help="stop services this script started (default: all)"), pipeline=False)
    down.add_argument("--remove", action="store_true", help="also remove the stopped containers")
    down.set_defaults(func=cmd_down)
    sub.add_parser("status", help="which service ports answer").set_defaults(func=cmd_status)
    with_names(sub.add_parser("env", help="print environments.local connections for the connectors")).set_defaults(func=cmd_env)
    with_names(sub.add_parser("seed", help="load examples/data/orders.csv into kafka, mongodb, elasticsearch"), pipeline=False).set_defaults(func=cmd_seed)

    args = parser.parse_args(argv)
    if args.command in ("up",) and not args.names and not args.pipeline:
        parser.error("name the connectors to start (or 'all'), or pass --pipeline FILE")
    args.func(args)


if __name__ == "__main__":
    main()
