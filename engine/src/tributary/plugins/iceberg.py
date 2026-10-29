"""What the Iceberg source and sink share: the catalog settings and how they reach Spark.

An Iceberg table is always reached through a Spark catalog, described by the options
below (usually coming from a connection of type ``iceberg``) and registered in the session
as ``spark.sql.catalog.<catalog_name>``: at session build through ``required_conf`` for
runs, and lazily at read/write time for shared sessions such as the one the backend uses
for previews.

Neither plugin asks for Iceberg's SQL extensions: Spark 3.5 runs MERGE INTO natively on
Iceberg tables, and the extensions would load Iceberg classes into every session state,
including sessions that never touch Iceberg. Users who want the extra DDL and procedures
can add them in ``runtime.spark_conf``.
"""

from __future__ import annotations

import hashlib
import os
import re
from typing import Literal

import pyspark
from pydantic import Field, ValidationInfo, field_validator

from ..core import PluginOptions
from ..errors import TributaryError
from ._util import when

# 1.6.1 is the last Iceberg release that runs on Java 8, which Spark 3.5 (and this project)
# support. Newer releases need Java 11+, and a newer jar on an older JVM breaks *every* read
# in the session (Spark scans all data sources on the classpath). Override with
# TRIBUTARY_ICEBERG_VERSION on Java 11+ to get a newer runtime (tested with 1.10.1).
ICEBERG_VERSION = os.environ.get("TRIBUTARY_ICEBERG_VERSION", "1.6.1")
_SPARK_MINOR = ".".join(pyspark.__version__.split(".")[:2])
ICEBERG_PACKAGE = f"org.apache.iceberg:iceberg-spark-runtime-{_SPARK_MINOR}_{{scala_version}}:{ICEBERG_VERSION}"
SPARK_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"


def iceberg_version() -> tuple[int, ...]:
    """The runtime's version as a comparable tuple: (1, 6, 1) for "1.6.1"."""
    return tuple(int(part) for part in re.findall(r"\d+", ICEBERG_VERSION)[:3])


CatalogType = Literal["hadoop", "hive", "rest", "glue", "jdbc", "nessie"]
IDENT = r"^[A-Za-z_][A-Za-z0-9_]*$"
TABLE = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$"

# Which catalog settings apply to which catalog type. One table drives both the validators
# and the "x-when" hints (``when`` in _util) the UI uses to show only the relevant fields and star the
# mandatory ones (see SchemaForm in the UI).
CATALOG_TYPES = ("hadoop", "hive", "rest", "glue", "jdbc", "nessie")
WAREHOUSE_REQUIRED = ("hadoop", "glue", "jdbc", "nessie")
URI_APPLIES = ("hive", "rest", "jdbc", "nessie")
URI_REQUIRED = URI_APPLIES  # a hive-site.xml fallback exists in Iceberg, but nothing here provides one
REST_ONLY = ("rest",)


class IcebergCatalogOptions(PluginOptions):
    """Catalog settings plus the table, common to the source and the sink.

    Field validators (not a model validator) so each error is reported on its own field,
    which is where the UI shows it. ``info.data`` holds the fields declared above."""

    # --- catalog: typically provided by a connection of type 'iceberg' -------------------
    catalog_type: CatalogType = Field(
        "hadoop",
        description="hadoop: tables under a warehouse path, no service needed. hive: a Hive metastore. "
        "rest: an Iceberg REST catalog. glue, jdbc and nessie need their own jars on the session",
    )
    warehouse: str | None = Field(
        None,
        validate_default=True,
        description="Warehouse root (local path, s3a://, gs://, abfss://, hdfs://). Optional for hive "
        "(the metastore's default warehouse is used) and rest (the server supplies locations)",
        json_schema_extra=when("catalog_type", required=WAREHOUSE_REQUIRED),
    )
    uri: str | None = Field(
        None,
        validate_default=True,
        description="hive: thrift://host:9083. rest: http(s)://host:8181. jdbc: jdbc:postgresql://... "
        "nessie: http://host:19120/api/v2",
        json_schema_extra=when("catalog_type", applies=URI_APPLIES, required=URI_REQUIRED),
    )
    credential: str | None = Field(
        None,
        description="OAuth2 client credential (client_id:secret). Use ${secret:...}",
        json_schema_extra=when("catalog_type", applies=REST_ONLY),
    )
    token: str | None = Field(
        None,
        description="Bearer token. Use ${secret:...}",
        json_schema_extra=when("catalog_type", applies=REST_ONLY),
    )
    catalog_name: str | None = Field(
        None,
        pattern=IDENT,
        description="Name of the catalog in Spark SQL (spark.sql.catalog.<name>). "
        "Defaults to a name derived from the catalog settings, so different catalogs never clash",
    )
    catalog_properties: dict[str, str] = Field(
        default_factory=dict,
        description="Extra catalog settings, each set as spark.sql.catalog.<name>.<key> "
        "(io-impl, s3.endpoint, jdbc.user, catalog-impl, ...)",
    )
    # --- table ---------------------------------------------------------------------------
    table: str = Field(..., pattern=TABLE, description="namespace.table inside the catalog, e.g. sales.orders")

    @field_validator("warehouse", mode="after")
    @classmethod
    def _warehouse_when_needed(cls, value: str | None, info: ValidationInfo) -> str | None:
        catalog_type = info.data.get("catalog_type", "hadoop")
        if not value and catalog_type in WAREHOUSE_REQUIRED:
            raise ValueError(f"required for a {catalog_type} catalog")
        return value

    @field_validator("uri", mode="after")
    @classmethod
    def _uri_when_needed(cls, value: str | None, info: ValidationInfo) -> str | None:
        catalog_type = info.data.get("catalog_type", "hadoop")
        if not value and catalog_type in URI_REQUIRED:
            raise ValueError(f"required for a {catalog_type} catalog")
        return value

    # -- derived ---------------------------------------------------------------------------

    @property
    def catalog(self) -> str:
        if self.catalog_name:
            return self.catalog_name
        fingerprint = hashlib.sha1(repr(sorted(self._catalog_settings().items())).encode()).hexdigest()[:8]
        return f"iceberg_{fingerprint}"

    @property
    def location(self) -> str:
        return self.uri or self.warehouse or self.catalog_type

    def _catalog_settings(self) -> dict[str, str]:
        """Catalog keys relative to spark.sql.catalog.<name>."""
        settings: dict[str, str] = {}
        if "catalog-impl" not in self.catalog_properties:  # Iceberg rejects 'type' next to 'catalog-impl'
            settings["type"] = self.catalog_type
        for key in ("warehouse", "uri", "credential", "token"):
            value = getattr(self, key)
            if value:
                settings[key] = value
        settings.update({k: str(v) for k, v in self.catalog_properties.items()})
        return settings

    def catalog_conf(self) -> dict[str, str]:
        prefix = f"spark.sql.catalog.{self.catalog}"
        return {prefix: SPARK_CATALOG_IMPL, **{f"{prefix}.{k}": v for k, v in self._catalog_settings().items()}}

    def qualified_table(self) -> str:
        return ".".join(f"`{part}`" for part in (self.catalog, *self.table.split(".")))

    def qualified_namespace(self) -> str:
        return ".".join(f"`{part}`" for part in (self.catalog, *self.table.split(".")[:-1]))


def configure_catalog(spark, opts: IcebergCatalogOptions) -> None:
    """Registers the catalog on a running session unless it is already there with the same
    settings. Spark reads ``spark.sql.catalog.*`` lazily, the first time a catalog is used,
    so this works on any session, including one shared by several pipelines."""
    for key, value in opts.catalog_conf().items():
        current = spark.conf.get(key, None)
        if current is None:
            spark.conf.set(key, value)
        elif current != value:
            setting = key.removeprefix(f"spark.sql.catalog.{opts.catalog}").lstrip(".") or "implementation"
            raise TributaryError(
                f"Spark catalog '{opts.catalog}' is already configured in this session with a different "
                f"'{setting}'; pick another catalog_name or restart the session"
            )


def local_table_bytes(opts: IcebergCatalogOptions) -> int | None:
    """Size of a table's data files when the warehouse is a local directory (a Hadoop
    catalog lays tables out as <warehouse>/<namespace...>/<table>), for resource sizing.
    None when the table is somewhere a listing would need a Spark session."""
    if opts.catalog_type != "hadoop" or not opts.warehouse:
        return None
    root = opts.warehouse.removeprefix("file://")
    if "://" in root:
        return None
    data = os.path.join(root, *opts.table.split("."), "data")
    if not os.path.isdir(data):
        return None
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, files in os.walk(data) for f in files)
