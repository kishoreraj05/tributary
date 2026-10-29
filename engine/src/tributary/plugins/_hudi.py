"""What the Hudi source and sink share: the Spark bundle, how a table is located under a
connection's ``base_path``, and read access to a table's properties and timeline.

A Hudi table is a directory (``<base_path>/<database>/<table>``) with a ``.hoodie``
metadata folder; no catalog is needed to read or write one.
"""

from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass, field

import pyspark
from pyspark.sql.types import StructType

from ..errors import TributaryError

# 0.15.0 is the last 0.x line and, like Spark 3.5 itself, runs on Java 8/11/17; 1.0.x (new table
# format, tested with 1.0.2) can be selected with TRIBUTARY_HUDI_VERSION.
HUDI_VERSION = os.environ.get("TRIBUTARY_HUDI_VERSION", "0.15.0")
# Hudi bundles are built per Spark minor ("spark3.5"); the engine fills in the Scala version.
_SPARK_MINOR = ".".join(pyspark.__version__.split(".")[:2])
HUDI_PACKAGE = f"org.apache.hudi:hudi-spark{_SPARK_MINOR}-bundle_{{scala_version}}:{HUDI_VERSION}"
HUDI_AWS_PACKAGE = f"org.apache.hudi:hudi-aws-bundle:{HUDI_VERSION}"
KRYO_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"

# Hudi adds these to every row it stores: commit time, sequence number, record key,
# partition path and file name.
META_COLUMN_PREFIX = "_hoodie_"
COMMIT_TIME_COLUMN = "_hoodie_commit_time"

# database.table, or just table
TABLE_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$"

BASE_PATH_DESCRIPTION = (
    "Root directory for tables (local path, s3a://, gs://, abfss://, hdfs://): the table is at "
    "<base_path>/<database>/<table>"
)

# Hudi names a commit by its instant time: yyyyMMddHHmmss (0.x, older tables) or
# yyyyMMddHHmmssSSS. A date or timestamp is turned into one textually, in the table's
# timeline time zone (the local one unless hoodie.table.timeline.timezone says otherwise).
_INSTANT = re.compile(r"^\d{1,17}$")
_TIMESTAMP = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})(?:[ T](?P<time>\d{2}:\d{2}:\d{2})(?:\.(?P<millis>\d{1,3}))?)?$"
)


def to_instant(value: str) -> str:
    """'2024-06-04 08:00:00' -> '20240604080000', '2024-06-04' -> '20240604000000',
    '2024-06-04T08:00:00.123' -> '20240604080000123'; an instant is returned as is.
    Raises ValueError for anything else."""
    value = value.strip()
    if _INSTANT.match(value):
        return value
    match = _TIMESTAMP.match(value)
    if not match:
        raise ValueError(
            f"'{value}' is not a Hudi instant (digits, as in _hoodie_commit_time, e.g. 20240604080000123), "
            "a timestamp (2024-06-04 08:00:00) or a date (2024-06-04)"
        )
    date = match.group("date").replace("-", "")
    time = (match.group("time") or "00:00:00").replace(":", "")
    millis = match.group("millis")
    return f"{date}{time}{millis.ljust(3, '0') if millis else ''}"


def split_table(table: str) -> tuple[str | None, str]:
    """'sales.orders' -> ('sales', 'orders'); 'orders' -> (None, 'orders')."""
    parts = table.split(".")
    return (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])


def table_location(base_path: str, table: str) -> str:
    return "/".join([base_path.rstrip("/"), *table.split(".")])


def _jvm(spark):
    """The driver JVM, or None where it is not reachable (Spark Connect)."""
    try:
        jvm = spark._jvm
        return jvm if jvm is not None else None
    except Exception:  # noqa: BLE001
        return None


def table_exists(spark, location: str) -> bool | None:
    """Whether a Hudi table is already at ``location``; None when that cannot be checked."""
    jvm = _jvm(spark)
    if jvm is None:
        return None
    path = jvm.org.apache.hadoop.fs.Path(location, ".hoodie/hoodie.properties")
    return bool(path.getFileSystem(spark._jsc.hadoopConfiguration()).exists(path))


def table_properties(spark, location: str) -> dict[str, str]:
    """The table's ``hoodie.properties`` (type, key and partition fields as they were fixed
    when it was created), empty when the JVM is out of reach."""
    jvm = _jvm(spark)
    if jvm is None:
        return {}
    path = jvm.org.apache.hadoop.fs.Path(location, ".hoodie/hoodie.properties")
    stream = path.getFileSystem(spark._jsc.hadoopConfiguration()).open(path)
    try:
        props = jvm.java.util.Properties()
        props.load(stream)
        return {str(k): str(v) for k, v in dict(props).items()}
    finally:
        stream.close()


def table_schema(spark, location: str) -> StructType:
    """The table's columns and types, without Hudi's ``_hoodie_*`` metadata columns (Hudi
    takes the schema from the latest commit; no data is read)."""
    schema = spark.read.format("hudi").load(location).schema
    return StructType([f for f in schema.fields if not f.name.startswith(META_COLUMN_PREFIX)])


@dataclass
class HudiCommit:
    """A completed write on the table's timeline."""

    timestamp: str  # the instant time, as in _hoodie_commit_time
    action: str  # commit (copy-on-write), deltacommit (merge-on-read), replacecommit (overwrites)
    operation: str  # UPSERT, INSERT, BULK_INSERT, INSERT_OVERWRITE, INSERT_OVERWRITE_TABLE, DELETE
    metadata: dict[str, str] = field(default_factory=dict)  # extraMetadata: the sink's run/batch tags


# Table services Hudi may run inline right after a write; they commit too, without the
# write's tags, so they are skipped when looking for writes.
MAINTENANCE_OPERATIONS = ("COMPACT", "LOG_COMPACT", "CLUSTER")


def _meta_client(spark, location: str):
    jvm = _jvm(spark)
    hadoop_conf = spark._jsc.hadoopConfiguration()
    builder = jvm.org.apache.hudi.common.table.HoodieTableMetaClient.builder()
    try:  # 0.15+ wraps the Hadoop configuration in its own StorageConfiguration
        builder = builder.setConf(jvm.org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf(hadoop_conf))
    except Exception:  # noqa: BLE001 - older releases take the Hadoop Configuration itself
        builder = builder.setConf(hadoop_conf)
    return builder.setBasePath(location).build()


def _instant_time(instant) -> str:
    """The instant's time, as written in ``_hoodie_commit_time``: 1.x calls it the requested
    time (a commit also has a completion time there), 0.x has only the timestamp."""
    try:
        return str(instant.requestedTime())
    except Exception:  # noqa: BLE001 - Py4J: the method does not exist on 0.x
        return str(instant.getTimestamp())


def _iter_commits(spark, location: str, *, newest_first: bool):
    """The table's completed writes (compactions and clustering are skipped), read through
    Hudi's own timeline classes so it works on every storage and both the 0.x and 1.x
    timeline layouts. Nothing when the table does not exist or the JVM is out of reach."""
    jvm = _jvm(spark)
    if jvm is None or not table_exists(spark, location):
        return
    timeline = _meta_client(spark, location).getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
    get_metadata = jvm.org.apache.hudi.common.table.timeline.TimelineUtils.getCommitMetadata
    instants = list(timeline.getInstants())
    for instant in reversed(instants) if newest_first else instants:
        metadata = get_metadata(instant, timeline)
        operation = str(metadata.getOperationType())
        if operation in MAINTENANCE_OPERATIONS:
            continue
        yield HudiCommit(
            timestamp=_instant_time(instant),
            action=str(instant.getAction()),
            operation=operation,
            metadata={str(k): str(v) for k, v in dict(metadata.getExtraMetadata()).items()},
        )


def latest_instant(spark, location: str) -> str | None:
    """The time of the table's latest completed commit of any kind (a compaction or
    clustering included: nothing is written after it), or None for a table without one
    or when the JVM is out of reach. What an incremental read up to "now" ends at."""
    jvm = _jvm(spark)
    if jvm is None or not table_exists(spark, location):
        return None
    last = _meta_client(spark, location).getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant()
    return _instant_time(last.get()) if last.isPresent() else None


def _filesystem(spark, path: str, what: str):
    """The Hadoop FileSystem for ``path`` and the path itself. On local disk, the raw one:
    the checksummed wrapper keeps ``.crc`` sidecars that make a file edited by hand (an
    operator resetting progress) unreadable."""
    jvm = _jvm(spark)
    if jvm is None:
        raise TributaryError(f"cannot {what} {path}: the driver JVM is out of reach (Spark Connect)")
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(spark._jsc.hadoopConfiguration())
    try:
        fs = fs.getRawFileSystem()  # ChecksumFileSystem (local disk) only
    except Exception:  # noqa: BLE001 - Py4J: other file systems have no such method
        pass
    return fs, target


def read_file(spark, path: str) -> str | None:
    """The text at ``path`` on any Hadoop-compatible storage; None when it is not there."""
    jvm = _jvm(spark)
    fs, target = _filesystem(spark, path, "read")
    if not fs.exists(target):
        return None
    stream = fs.open(target)
    try:
        return bytes(jvm.org.apache.commons.io.IOUtils.toByteArray(stream)).decode("utf-8")
    finally:
        stream.close()


def write_file(spark, path: str, text: str) -> None:
    """Writes ``text`` at ``path`` (parent directories created), through a temporary file
    and a rename so a reader never sees a half-written file."""
    jvm = _jvm(spark)
    fs, target = _filesystem(spark, path, "write")
    fs.mkdirs(target.getParent())
    temp = jvm.org.apache.hadoop.fs.Path(target.getParent(), f".{target.getName()}.{uuid.uuid4().hex}.tmp")
    stream = fs.create(temp, True)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()
    if fs.exists(target):
        fs.delete(target, False)
    if not fs.rename(temp, target):
        fs.delete(temp, False)
        raise TributaryError(f"could not write {path}")


def commits(spark, location: str) -> list[HudiCommit]:
    """The table's completed writes, oldest first (see ``_iter_commits``)."""
    return list(_iter_commits(spark, location, newest_first=False))


def latest_commit(spark, location: str) -> HudiCommit | None:
    """The table's latest completed write; None when there is none."""
    return next(_iter_commits(spark, location, newest_first=True), None)


def latest_commit_metadata(spark, location: str) -> dict[str, str] | None:
    """The extraMetadata of the latest completed commit (see ``latest_commit``)."""
    commit = latest_commit(spark, location)
    return None if commit is None else commit.metadata
