from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"

# The sessions below use UTC as Spark's session time zone, and several tests compare
# timestamps read back through Python or a connector (Elasticsearch dates, MongoDB dates,
# Iceberg snapshot times) against UTC wall-clock values. Pin the process, and so the Spark
# JVM it starts, to UTC as well, so the suite gives the same answer on any host time zone.
os.environ["TZ"] = "UTC"
time.tzset()


def maven_jar(coordinate: str) -> Path | None:
    """A jar by Maven coordinate, so the shared test session can write Iceberg and Delta tables.

    Looked up in Spark's Ivy cache (a previous run of any pipeline using the plugin put it
    there), then a local cache filled from Maven Central. Without network access the tests
    of that sink are skipped; nothing else needs the jars."""
    group, artifact, version = coordinate.split(":")
    candidates = [
        Path.home() / ".ivy2" / "jars" / f"{group}_{artifact}-{version}.jar",
        Path.home() / ".cache" / "tributary" / "jars" / f"{artifact}-{version}.jar",
    ]
    for path in candidates:
        if path.exists():
            return path
    target = candidates[-1]
    url = f"https://repo1.maven.org/maven2/{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.jar"
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(url, timeout=60) as response, open(f"{target}.part", "wb") as out:
            while chunk := response.read(1 << 20):
                out.write(chunk)
        os.replace(f"{target}.part", target)
        return target
    except Exception:  # noqa: BLE001 - offline: the sink's tests skip themselves
        return None


def iceberg_jar() -> Path | None:
    """The Iceberg Spark runtime: TRIBUTARY_ICEBERG_JAR, or fetched like any other jar."""
    from tributary.plugins.sinks.iceberg import ICEBERG_PACKAGE
    from tributary.runtime import format_package

    env = os.environ.get("TRIBUTARY_ICEBERG_JAR")
    if env:
        return Path(env) if Path(env).exists() else None
    return maven_jar(format_package(ICEBERG_PACKAGE))


def delta_jars() -> list[Path] | None:
    """delta-spark and delta-storage (its one dependency outside Spark), both or nothing."""
    from tributary.plugins.sinks.delta import DeltaSink
    from tributary.runtime import format_package

    jars = [maven_jar(format_package(pkg)) for pkg in DeltaSink.spark_packages]
    return None if any(j is None for j in jars) else [j for j in jars if j is not None]


def deequ_jar() -> Path | None:
    """Deequ itself is one jar; everything else it needs (breeze, scala-reflect) ships with Spark."""
    from tributary.plugins.transforms.deequ import DEEQU_PACKAGE
    from tributary.runtime import format_package

    env = os.environ.get("TRIBUTARY_DEEQU_JAR")
    if env:
        return Path(env) if Path(env).exists() else None
    return maven_jar(format_package(DEEQU_PACKAGE))


def hudi_jar() -> Path | None:
    """The Hudi Spark bundle: TRIBUTARY_HUDI_JAR, or fetched like any other jar."""
    from tributary.plugins.sinks.hudi import HUDI_PACKAGE
    from tributary.runtime import format_package

    env = os.environ.get("TRIBUTARY_HUDI_JAR")
    if env:
        return Path(env) if Path(env).exists() else None
    return maven_jar(format_package(HUDI_PACKAGE))


def elasticsearch_jar() -> Path | None:
    """The Elasticsearch Spark connector (self-contained; its POM's Spark/Scala dependencies
    are already in the session)."""
    from tributary.plugins.sinks.elasticsearch import ELASTICSEARCH_PACKAGE
    from tributary.runtime import format_package

    return maven_jar(format_package(ELASTICSEARCH_PACKAGE))


ES_URL = os.environ.get("TRIBUTARY_TEST_ES_URL", "http://localhost:9200").rstrip("/")


def elasticsearch_reachable() -> str | None:
    """Why the Elasticsearch at ES_URL (the docker-compose one by default) cannot be used, or None."""
    try:
        with urllib.request.urlopen(ES_URL, timeout=3) as response:
            info = json.load(response)
    except Exception as exc:  # noqa: BLE001
        return f"Elasticsearch not reachable at {ES_URL}: {exc}"
    if "version" not in info:
        return f"{ES_URL} does not look like Elasticsearch: {info}"
    return None
# The MongoDB Java driver the connector declares as a version range; pinned here only
# because the shared test session takes plain jars (a run resolves the range through Ivy).
MONGODB_DRIVER_VERSION = "5.1.4"
MONGODB_DRIVER_PACKAGES = tuple(
    f"org.mongodb:{artifact}:{MONGODB_DRIVER_VERSION}"
    for artifact in ("mongodb-driver-sync", "mongodb-driver-core", "bson", "bson-record-codec")
)
MONGODB_URI = os.environ.get("TRIBUTARY_TEST_MONGODB_URI", "mongodb://localhost:27017")


def mongodb_jars() -> list[Path] | None:
    """The MongoDB Spark Connector and the driver jars it needs, all or nothing."""
    from tributary.plugins.sinks.mongodb import MongoDbSink
    from tributary.runtime import format_package

    jars = [maven_jar(format_package(pkg)) for pkg in (*MongoDbSink.spark_packages, *MONGODB_DRIVER_PACKAGES)]
    return None if any(j is None for j in jars) else [j for j in jars if j is not None]


def mongodb_reachable(uri: str = MONGODB_URI) -> bool:
    """A TCP connection to the first host of the URI: enough to know whether a server
    (``docker compose up -d mongodb``, or TRIBUTARY_TEST_MONGODB_URI) is there."""
    import socket

    authority = uri.split("://", 1)[1].split("/", 1)[0].split("?", 1)[0].rsplit("@", 1)[-1]
    host, _, port = authority.split(",")[0].partition(":")
    try:
        with socket.create_connection((host, int(port or 27017)), timeout=2):
            return True
    except OSError:
        return False


def es_hadoop_jar() -> Path | None:
    """The ES-Hadoop Spark connector; its other dependencies are already in Spark's jars."""
    from tributary.plugins.sources.elasticsearch import ES_HADOOP_PACKAGE
    from tributary.runtime import format_package

    return maven_jar(format_package(ES_HADOOP_PACKAGE))


def reachable(host: str, port: int) -> bool:
    import socket

    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


# Integration tests against real services: TRIBUTARY_TEST_ES_NODES (host:port) and
# TRIBUTARY_TEST_MONGO_URI, defaulting to the deploy/docker-compose.yml ports. They skip
# when the service is not listening.
ES_NODES = os.environ.get("TRIBUTARY_TEST_ES_NODES", "localhost:9200")
MONGO_URI = os.environ.get("TRIBUTARY_TEST_MONGO_URI", "mongodb://localhost:27017")


def _host_port(address: str, default_port: int) -> tuple[str, int]:
    address = address.split("://", 1)[-1].split("/", 1)[0].split("@")[-1].split(",")[0]
    host, _, port = address.partition(":")
    return host, int(port or default_port)


@pytest.fixture(scope="session")
def iceberg_runtime() -> Path | None:
    return iceberg_jar()


@pytest.fixture(scope="session")
def elasticsearch_runtime() -> Path | None:
    return elasticsearch_jar()


@pytest.fixture(scope="session")
def es_hadoop_runtime() -> Path | None:
    return es_hadoop_jar()


@pytest.fixture(scope="session")
def mongo_runtime(mongodb_runtime) -> list[Path] | None:
    return mongodb_runtime


@pytest.fixture(scope="session")
def deequ_runtime() -> Path | None:
    return deequ_jar()


@pytest.fixture(scope="session")
def hudi_runtime() -> Path | None:
    return hudi_jar()


@pytest.fixture(scope="session")
def delta_runtime() -> list[Path] | None:
    return delta_jars()


@pytest.fixture(scope="session")
def mongodb_runtime() -> list[Path] | None:
    return mongodb_jars()


@pytest.fixture(scope="session")
def spark(iceberg_runtime, delta_runtime, deequ_runtime, hudi_runtime, elasticsearch_runtime, es_hadoop_runtime, mongodb_runtime):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("tributary-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    # Only the jars: the sinks must work without Iceberg's or Delta's SQL extensions.
    jars = [j for j in (iceberg_runtime, *(delta_runtime or []), deequ_runtime, hudi_runtime, elasticsearch_runtime, es_hadoop_runtime, *(mongodb_runtime or [])) if j]
    jars = list(dict.fromkeys(jars))  # the Elasticsearch source and sink use the same connector
    if jars:
        builder = builder.config("spark.jars", ",".join(str(j) for j in jars))
    if hudi_runtime is not None:
        # The one static setting Hudi insists on (the sink declares it in spark_conf).
        builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def iceberg_spark(spark, iceberg_runtime):
    if iceberg_runtime is None:
        pytest.skip("Iceberg runtime jar not available (offline); set TRIBUTARY_ICEBERG_JAR or connect to Maven Central")
    return spark


@pytest.fixture(scope="session")
def deequ_spark(spark, deequ_runtime):
    if deequ_runtime is None:
        pytest.skip("deequ jar not available (offline); set TRIBUTARY_DEEQU_JAR or connect to Maven Central")
    return spark


@pytest.fixture(scope="session")
def hudi_spark(spark, hudi_runtime):
    if hudi_runtime is None:
        pytest.skip("Hudi bundle jar not available (offline); set TRIBUTARY_HUDI_JAR or connect to Maven Central")
    return spark


@pytest.fixture(scope="session")
def delta_spark(spark, delta_runtime):
    if delta_runtime is None:
        pytest.skip("Delta jars not available (offline); connect to Maven Central once to cache them")
    return spark


@pytest.fixture(scope="session")
def elasticsearch_spark(spark, elasticsearch_runtime):
    """The shared session, for tests that write to a live Elasticsearch (docker-compose's,
    or TRIBUTARY_TEST_ES_URL); skipped without the connector jar or without a cluster."""
    if elasticsearch_runtime is None:
        pytest.skip("Elasticsearch Spark connector jar not available (offline); connect to Maven Central once to cache it")
    reason = elasticsearch_reachable()
    if reason:
        pytest.skip(reason + " (docker compose -f deploy/docker-compose.yml up -d elasticsearch)")
    return spark


@pytest.fixture(scope="session")
def es_spark(spark, es_hadoop_runtime):
    if es_hadoop_runtime is None:
        pytest.skip("ES-Hadoop jar not available (offline); connect to Maven Central once to cache it")
    if not reachable(*_host_port(ES_NODES, 9200)):
        pytest.skip(f"no Elasticsearch at {ES_NODES} (docker compose up -d elasticsearch, or set TRIBUTARY_TEST_ES_NODES)")
    return spark


@pytest.fixture(scope="session")
def mongodb_spark(spark, mongodb_runtime):
    if mongodb_runtime is None:
        pytest.skip("MongoDB Spark Connector jars not available (offline); connect to Maven Central once to cache them")
    if not mongodb_reachable():
        pytest.skip(f"no MongoDB server at {MONGODB_URI}; start one (docker compose up -d mongodb) or set TRIBUTARY_TEST_MONGODB_URI")
    return spark


@pytest.fixture(scope="session")
def mongo_spark(spark, mongo_runtime):
    if mongo_runtime is None:
        pytest.skip("MongoDB connector jars not available (offline); connect to Maven Central once to cache them")
    if not reachable(*_host_port(MONGO_URI, 27017)):
        pytest.skip(f"no MongoDB at {MONGO_URI} (docker compose up -d mongodb, or set TRIBUTARY_TEST_MONGO_URI)")
    return spark


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


class FlakyProxy:
    """A TCP proxy in front of a MongoDB server that a test can cut and restore, to play a
    server that goes away for a while: ``cut()`` closes the listener and every connection
    (clients get "connection refused" / "connection reset"), ``restore()`` brings the same
    port back. Runs on daemon threads; ``close()`` stops it."""

    def __init__(self, target: str = MONGODB_URI):
        import socket

        authority = target.split("://", 1)[1].split("/", 1)[0].split("?", 1)[0].rsplit("@", 1)[-1]
        host, _, port = authority.split(",")[0].partition(":")
        self.target = (host, int(port or 27017))
        self.port = self._free_port()
        self.up = False
        self._listener: socket.socket | None = None
        self._connections: list[socket.socket] = []
        self._lock = __import__("threading").Lock()

    @staticmethod
    def _free_port() -> int:
        import socket

        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    @property
    def uri(self) -> str:
        # Short timeouts, so that a test sees the failure and the retry within seconds.
        return f"mongodb://127.0.0.1:{self.port}/?serverSelectionTimeoutMS=2000&connectTimeoutMS=2000&socketTimeoutMS=5000"

    def restore(self) -> "FlakyProxy":
        import socket
        import threading

        listener = socket.socket()
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.port))
        listener.listen(16)
        self._listener, self.up = listener, True
        threading.Thread(target=self._accept, args=(listener,), daemon=True).start()
        return self

    start = restore

    def cut(self) -> None:
        self.up = False
        listener, self._listener = self._listener, None
        if listener is not None:
            listener.close()
        with self._lock:
            connections, self._connections = self._connections, []
        for s in connections:
            try:
                s.shutdown(__import__("socket").SHUT_RDWR)
            except OSError:
                pass
            s.close()

    close = cut

    def _accept(self, listener) -> None:
        import socket
        import threading

        while self.up and self._listener is listener:
            try:
                client, _ = listener.accept()
            except OSError:
                return
            try:
                upstream = socket.create_connection(self.target, timeout=5)
            except OSError:
                client.close()
                continue
            upstream.settimeout(None)
            with self._lock:
                self._connections += [client, upstream]
            threading.Thread(target=self._pump, args=(client, upstream), daemon=True).start()
            threading.Thread(target=self._pump, args=(upstream, client), daemon=True).start()

    @staticmethod
    def _pump(src, dst) -> None:
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.close()
                except OSError:
                    pass


@pytest.fixture
def flaky_mongodb(mongodb_spark):
    """A cut-able proxy to the test server (see FlakyProxy)."""
    proxy = FlakyProxy().start()
    yield proxy
    proxy.close()
