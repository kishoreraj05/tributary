"""What the Delta source and the Delta sink share: the jar coordinates, the session catalog
Delta needs, and a few questions about a table at a path.

Delta refuses any operation on a table by path (reads and streams as well as writes) unless
the session catalog (``spark.sql.catalog.spark_catalog``) is its ``DeltaCatalog``. Both
plugins ask for it at session build through ``required_conf``, and set it lazily on
sessions they did not build (``configure_session``), such as the one the backend shares
for previews: Spark loads the session catalog implementation on first use, so the setting
takes effect on a running session. The catalog delegates everything that is not a Delta
table to Spark's own, so other pipelines on the session are not affected.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from pyspark.sql.types import LongType, StringType, StructField, TimestampType

from ..errors import TributaryError

# The jar must match the delta-spark Python package (a thin wrapper over the JVM API), so
# the Maven coordinate follows the installed package, as delta's own
# configure_spark_with_delta_pip does. Delta 3.x is the line for Spark 3.5 (Java 8+).
try:
    DELTA_VERSION = version("delta-spark")
except PackageNotFoundError:  # an engine dependency; the fallback only keeps the catalog importable
    DELTA_VERSION = "3.3.2"
DELTA_PACKAGE = f"io.delta:delta-spark_{{scala_version}}:{DELTA_VERSION}"
DELTA_STORAGE_PACKAGE = f"io.delta:delta-storage:{DELTA_VERSION}"  # transitive, listed for direct-jar setups
DELTA_PACKAGES = (DELTA_PACKAGE, DELTA_STORAGE_PACKAGE)
SESSION_CATALOG_KEY = "spark.sql.catalog.spark_catalog"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

# The columns a change data feed read adds to the table's own.
CHANGE_FEED_FIELDS = (
    StructField("_change_type", StringType()),
    StructField("_commit_version", LongType()),
    StructField("_commit_timestamp", TimestampType()),
)
CHANGE_FEED_COLUMNS = tuple(f.name for f in CHANGE_FEED_FIELDS)


def configure_session(spark) -> None:
    """Makes Delta's catalog the session catalog unless it already is. Spark loads the
    session catalog implementation lazily, so this works on any running session."""
    current = spark.conf.get(SESSION_CATALOG_KEY, None)
    if current is None:
        spark.conf.set(SESSION_CATALOG_KEY, DELTA_CATALOG)
    elif current != DELTA_CATALOG:
        raise TributaryError(
            f"the session catalog ({SESSION_CATALOG_KEY}) is '{current}' in this session, but Delta needs "
            f"{DELTA_CATALOG}; remove the other setting or write Delta from another pipeline"
        )


def is_delta_table(spark, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def table_version(spark, path: str) -> int:
    """The current (latest committed) version of the table."""
    from delta.tables import DeltaTable

    return DeltaTable.forPath(spark, path).history(1).collect()[0].version
