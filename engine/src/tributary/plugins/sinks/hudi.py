"""Apache Hudi tables, written with the Hudi Spark data source.

A Hudi table is a directory (``<base_path>/<database>/<table>``): no catalog is needed to
write one. The storage root and an optional metastore sync usually come from a connection
of type ``hudi``; the node names the table and how rows are written (``operation``).

Hudi insists on Kryo as ``spark.serializer``, a static setting that cannot be changed on a
running session, so the sink declares it in ``spark_conf``: the engine applies it when it
builds a session, and the backend applies it to its shared preview session. Nothing else
Hudi-specific is wired into the session: the SQL extension and ``HoodieCatalog`` are only
needed for Spark SQL DDL/DML on Hudi tables, which this sink does not use, and the Kryo
registrator loads Hudi classes into every session state; add them in ``runtime.spark_conf``
if you want them.
"""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import Field, ValidationInfo, field_validator
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from ...core import PluginOptions, Sink
from ...errors import TributaryError
from .._hudi import (  # noqa: F401 - re-exported: these names are part of this module's API
    BASE_PATH_DESCRIPTION,
    HUDI_AWS_PACKAGE,
    HUDI_PACKAGE,
    HUDI_VERSION,
    KRYO_SERIALIZER,
    MAINTENANCE_OPERATIONS,
    META_COLUMN_PREFIX,
    TABLE_PATTERN,
    HudiCommit,
    latest_commit,
    latest_commit_metadata,
    split_table,
    table_exists,
    table_location,
    table_properties,
    table_schema,
)
from .._util import OptionValue, column_errors, format_errors, spark_options, when

GLUE_SYNC_TOOL = "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool"
# Merges by the precombine column against stored rows too. Hudi 0.x's default payload only
# deduplicates the incoming batch by it and then lets the latest write win.
ORDERED_PAYLOAD = "org.apache.hudi.common.model.DefaultHoodieRecordPayload"
# A global index looks a key up across every partition, so a row whose partition value
# changes is moved rather than duplicated.
GLOBAL_INDEX_OPTIONS = {"hoodie.index.type": "GLOBAL_BLOOM", "hoodie.bloom.index.update.partition.path": True}
SCHEMA_EVOLUTION_OPTIONS: dict[str, dict[str, Any]] = {
    "add_columns": {},  # Hudi's default: new columns are added, types widened, missing columns rejected
    "reconcile": {"hoodie.datasource.write.reconcile.schema": True},
    "drop_columns": {"hoodie.datasource.write.schema.allow.auto.evolution.column.drop": True},
    "strict": {},  # checked by the sink before the write
}

# Commit metadata keys: which pipeline/run/micro-batch produced a commit. Hudi copies every
# writer option starting with "_" (hoodie.datasource.write.commitmeta.key.prefix) into the
# commit's extraMetadata.
PIPELINE_PROP = "_tributary.pipeline"
RUN_PROP = "_tributary.run"
BATCH_PROP = "_tributary.batch"

TableType = Literal["copy_on_write", "merge_on_read"]
SchemaEvolution = Literal["add_columns", "reconcile", "drop_columns", "strict"]
Operation = Literal["upsert", "insert", "bulk_insert", "insert_overwrite", "insert_overwrite_table", "delete"]
CatalogSync = Literal["none", "hms", "jdbc", "glue"]

# Which settings apply to which operation / sync mode. One table drives both the validators
# and the "x-when" hints the UI uses to show only the relevant fields and star the mandatory
# ones (see SchemaForm in the UI).
KEY_REQUIRED = ("upsert", "delete")  # the others can let Hudi generate keys (0.14+)
PRECOMBINE_REQUIRED = ("upsert",)  # upsert reads the ordering column of every incoming row
SYNC_WITH_URI = ("hms", "jdbc")
JDBC_ONLY = ("jdbc",)

_TABLE = TABLE_PATTERN
_FIELD = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)*$")  # a column, or a nested one (a.b)
_PARTITION = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)*(:(simple|timestamp))?$")  # CustomKeyGenerator suffixes


class HudiSinkOptions(PluginOptions):
    # --- storage: typically provided by a connection of type 'hudi' ---------------------
    base_path: str = Field(..., description=BASE_PATH_DESCRIPTION)
    catalog_sync: CatalogSync = Field(
        "none",
        description="Register the table in a metastore after every write. hms: a Hive metastore (thrift). "
        "jdbc: HiveServer2. glue: AWS Glue (adds the hudi-aws bundle). none: the table is only on storage",
    )
    catalog_uri: str | None = Field(
        None,
        validate_default=True,
        description="hms: thrift://host:9083. jdbc: jdbc:hive2://host:10000",
        json_schema_extra=when("catalog_sync", applies=SYNC_WITH_URI, required=SYNC_WITH_URI),
    )
    catalog_user: str | None = Field(
        None, description="HiveServer2 user", json_schema_extra=when("catalog_sync", applies=JDBC_ONLY)
    )
    catalog_password: str | None = Field(
        None, description="HiveServer2 password. Use ${secret:...}", json_schema_extra=when("catalog_sync", applies=JDBC_ONLY)
    )
    # --- table ---------------------------------------------------------------------------
    table: str = Field(
        ...,
        pattern=_TABLE,
        description="database.table (or just table, in database 'default'), e.g. sales.orders. Names the table "
        "directory under base_path (<base_path>/sales/orders) and the metastore table when catalog_sync is on",
    )
    table_type: TableType = Field(
        "copy_on_write",
        description="copy_on_write: every write rewrites the files it touches; fastest reads. "
        "merge_on_read: writes go to log files merged on read or by compaction; fastest writes. "
        "Fixed once the table exists",
    )
    operation: Operation = Field(
        "upsert",
        description="upsert: update rows whose record_key exists, insert the others. insert: add rows (keys may "
        "repeat). bulk_insert: like insert but faster for initial loads, no small-file handling. "
        "insert_overwrite: replace only the partitions present in the data. insert_overwrite_table: "
        "replace the whole table. delete: remove the rows whose keys are in the data",
    )
    record_key: list[str] = Field(
        default_factory=list,
        validate_default=True,
        description="Columns that identify a row (nested columns as a.b). Required for upsert and delete; "
        "without it, the other operations let Hudi generate keys. Fixed once the table exists",
        json_schema_extra=when("operation", required=KEY_REQUIRED),
    )
    precombine_field: str | None = Field(
        None,
        validate_default=True,
        description="Column whose largest value wins when several incoming rows, or an incoming and a stored "
        "row, share a key (a timestamp, a version): a replayed older batch never overwrites newer data. "
        "Required for upsert",
        json_schema_extra=when("operation", required=PRECOMBINE_REQUIRED),
    )
    partition_by: list[str] = Field(
        default_factory=list,
        description="Partition columns, in order; empty for an unpartitioned table. Fixed once the table exists. "
        "A col:timestamp or col:simple suffix is passed through for Hudi's CustomKeyGenerator (set "
        "hoodie.datasource.write.keygenerator.class and its settings in write_options)",
    )
    hive_style_partitioning: bool = Field(
        True, description="Partition directories named country=IN (as Spark and Hive do) rather than IN"
    )
    global_index: bool = Field(
        False,
        description="Record keys are unique across the whole table rather than within a partition: a row whose "
        "partition column changes moves to the new partition instead of being duplicated. Costs an index "
        "lookup over every partition, so leave it off when keys never change partition",
    )
    schema_evolution: SchemaEvolution = Field(
        "add_columns",
        description="What to do when the data's columns differ from the table's. add_columns: new columns are "
        "added and types widened, but a batch missing a column is rejected. reconcile: the table's schema is "
        "kept and columns missing from the data are written as null. drop_columns: columns missing from the "
        "data are also dropped from the table. strict: the data must have exactly the table's columns and "
        "types, or the write is refused before it starts",
    )
    create_if_missing: bool = Field(True, description="Create the table on first write")
    write_options: dict[str, OptionValue] = Field(
        default_factory=dict,
        description="Any other hoodie.* setting, applied on top of the ones derived above: hoodie.index.type, "
        "hoodie.clean.commits.retained, hoodie.compact.inline.max.delta.commits, hoodie.datasource.hive_sync.*, ...",
    )
    skip_replayed_batches: bool = Field(
        True,
        description="Streaming commits are tagged with their micro-batch id, and a batch the table already "
        "has is skipped, so a restart never duplicates rows. Turn off when replaying a stream from a fresh "
        "checkpoint into an existing table",
    )

    # Field validators (not a model validator) so each error is reported on its own field,
    # which is where the UI shows it. ``info.data`` holds the fields declared above.

    @field_validator("catalog_uri", mode="after")
    @classmethod
    def _uri_when_syncing(cls, value: str | None, info: ValidationInfo) -> str | None:
        sync = info.data.get("catalog_sync", "none")
        if not value and sync in SYNC_WITH_URI:
            raise ValueError(f"required for catalog_sync '{sync}'")
        return value

    @field_validator("record_key", mode="after")
    @classmethod
    def _record_key_when_needed(cls, value: list[str], info: ValidationInfo) -> list[str]:
        operation = info.data.get("operation", "upsert")
        if not value and operation in KEY_REQUIRED:
            raise ValueError(f"required for operation '{operation}'")
        bad = [k for k in value if not _FIELD.match(k.strip())]
        if bad:
            raise ValueError(f"invalid column name(s): {', '.join(bad)}")
        return [k.strip() for k in value]

    @field_validator("precombine_field", mode="after")
    @classmethod
    def _precombine_when_needed(cls, value: str | None, info: ValidationInfo) -> str | None:
        operation = info.data.get("operation", "upsert")
        if not value and operation in PRECOMBINE_REQUIRED:
            raise ValueError(f"required for operation '{operation}'")
        if value and not _FIELD.match(value.strip()):
            raise ValueError(f"invalid column name: {value}")
        return value.strip() if value else value

    @field_validator("partition_by")
    @classmethod
    def _valid_partitions(cls, value: list[str]) -> list[str]:
        bad = [p for p in value if not _PARTITION.match(p.strip())]
        if bad:
            raise ValueError(f"invalid partition field(s): {', '.join(bad)}; use a column, or col:simple / col:timestamp")
        return [p.strip() for p in value]

    # -- derived ---------------------------------------------------------------------------

    @property
    def database(self) -> str | None:
        return split_table(self.table)[0]

    @property
    def table_name(self) -> str:
        return split_table(self.table)[1]

    @property
    def location(self) -> str:
        return table_location(self.base_path, self.table)

    def hoodie_options(self) -> dict[str, str]:
        """The writer options derived from the fields, then ``write_options`` on top."""
        opts: dict[str, Any] = {
            "hoodie.table.name": self.table_name,
            "hoodie.datasource.write.table.type": self.table_type.upper(),
            "hoodie.datasource.write.operation": self.operation,
            "hoodie.datasource.write.hive_style_partitioning": self.hive_style_partitioning,
        }
        if self.database:
            opts["hoodie.database.name"] = self.database
        if self.record_key:
            opts["hoodie.datasource.write.recordkey.field"] = ",".join(self.record_key)
        if self.precombine_field:
            opts["hoodie.datasource.write.precombine.field"] = self.precombine_field
            opts["hoodie.datasource.write.payload.class"] = ORDERED_PAYLOAD
        if self.partition_by:
            opts["hoodie.datasource.write.partitionpath.field"] = ",".join(self.partition_by)
        if self.global_index:
            opts.update(GLOBAL_INDEX_OPTIONS)
        opts.update(SCHEMA_EVOLUTION_OPTIONS[self.schema_evolution])
        opts.update(self._sync_options())
        opts.update(self.write_options)
        return spark_options(opts)

    def _sync_options(self) -> dict[str, Any]:
        if self.catalog_sync == "none":
            return {}
        opts: dict[str, Any] = {
            "hoodie.datasource.hive_sync.database": self.database or "default",
            "hoodie.datasource.hive_sync.table": self.table_name,
        }
        if self.partition_by:
            opts["hoodie.datasource.hive_sync.partition_fields"] = ",".join(p.split(":")[0] for p in self.partition_by)
        if self.catalog_sync == "glue":
            opts["hoodie.datasource.meta.sync.enable"] = True
            opts["hoodie.meta.sync.client.tool.class"] = GLUE_SYNC_TOOL
            return opts
        opts["hoodie.datasource.hive_sync.enable"] = True
        opts["hoodie.datasource.hive_sync.mode"] = self.catalog_sync
        if self.catalog_sync == "hms":
            opts["hoodie.datasource.hive_sync.metastore.uris"] = self.catalog_uri
        else:
            opts["hoodie.datasource.hive_sync.jdbcurl"] = self.catalog_uri
            if self.catalog_user:
                opts["hoodie.datasource.hive_sync.username"] = self.catalog_user
            if self.catalog_password:
                opts["hoodie.datasource.hive_sync.password"] = self.catalog_password
        return opts


class HudiSink(Sink):
    """Apache Hudi tables: upsert, insert, bulk insert, partition or table overwrite, delete."""

    type_name = "hudi"
    display_name = "Apache Hudi"
    icon = "table"
    Options = HudiSinkOptions
    spark_packages = (HUDI_PACKAGE,)
    spark_conf = {"spark.serializer": KRYO_SERIALIZER}

    def required_packages(self, opts: HudiSinkOptions) -> list[str]:
        packages = list(self.spark_packages)
        if opts.catalog_sync == "glue":
            packages.append(HUDI_AWS_PACKAGE)
        return packages

    def describe(self, opts: HudiSinkOptions) -> str:
        sync = f", synced to {opts.catalog_sync}" if opts.catalog_sync != "none" else ""
        return f"hudi {opts.table_type} -> {opts.table} ({opts.operation}{sync}) at {opts.location}"

    def validate_input(self, ctx, schema: StructType, opts: HudiSinkOptions) -> list[tuple[str, str]]:
        """The record key, precombine column and partition columns must be columns of the input."""
        return column_errors(schema, {
            "record_key": opts.record_key,
            "precombine_field": [opts.precombine_field] if opts.precombine_field else [],
            "partition_by": [p.split(":")[0] for p in opts.partition_by],
        })

    # -- write -----------------------------------------------------------------------------

    def write(self, ctx, df: DataFrame, opts: HudiSinkOptions) -> None:
        spark = ctx.spark
        check_serializer(spark)
        if errors := self.validate_input(ctx, df.schema, opts):
            raise TributaryError(f"hudi sink {opts.table}: {format_errors(errors)}")
        exists = table_exists(spark, opts.location)  # None: not checkable (Spark Connect)
        if exists is False:
            if not opts.create_if_missing:
                raise TributaryError(f"hudi table {opts.table} does not exist at {opts.location} (create_if_missing is off)")
            ctx.log.info("creating hudi table %s at %s", opts.table, opts.location)
        if exists:
            check_existing_table(spark, opts)
            if opts.schema_evolution == "strict":
                check_schema_unchanged(spark, df.schema, opts)
        if exists and ctx.batch_id is not None and opts.skip_replayed_batches and self._has_batch(ctx, opts):
            ctx.log.warning(
                "hudi table %s already has micro-batch %s of pipeline %s; skipping it",
                opts.table, ctx.batch_id, ctx.pipeline_id,
            )
            return
        options = {**opts.hoodie_options(), RUN_PROP: ctx.run_id}
        if ctx.batch_id is not None:
            options[PIPELINE_PROP] = ctx.pipeline_id
            options[BATCH_PROP] = str(ctx.batch_id)
        # Always Spark's append mode: what happens to existing rows is decided by the Hudi
        # operation (overwrite mode would drop and re-create the whole table directory).
        df.write.format("hudi").options(**options).mode("append").save(opts.location)

    def _has_batch(self, ctx, opts: HudiSinkOptions) -> bool:
        """Whether the table's latest commit was produced by this micro-batch (a replay after a restart)."""
        commit = latest_commit(ctx.spark, opts.location)
        if commit is None:
            return False
        return commit.metadata.get(PIPELINE_PROP) == ctx.pipeline_id and commit.metadata.get(BATCH_PROP) == str(ctx.batch_id)


def check_serializer(spark) -> None:
    """Hudi refuses to write unless Spark serializes with Kryo. The sink asks for it in
    ``spark_conf``; a session built elsewhere (a notebook, a test) may still lack it."""
    try:
        serializer = spark.conf.get("spark.serializer", None)
    except Exception:  # noqa: BLE001 - some sessions (Spark Connect) hide non-SQL settings
        return
    if serializer != KRYO_SERIALIZER:
        raise TributaryError(
            f"the hudi sink needs spark.serializer={KRYO_SERIALIZER} (got {serializer or 'the default'}); "
            "set it when the session is created, for example in runtime.spark_conf"
        )


def _fields(value: str | None) -> list[str]:
    """'country:simple,day' -> ['country', 'day']: the columns, without key-generator suffixes."""
    return [f.split(":")[0].strip() for f in (value or "").split(",") if f.strip()]


def check_existing_table(spark, opts: HudiSinkOptions) -> None:
    """Hudi fixes the table type, the record key and the partitioning when a table is
    created; a write with other settings fails deep inside Hudi (or, for the table type,
    with an error about compaction), so say what differs up front."""
    props = table_properties(spark, opts.location)
    if not props:
        return
    expected = {
        "table_type": (opts.table_type.upper(), props.get("hoodie.table.type", "").upper()),
        "record_key": (opts.record_key, _fields(props.get("hoodie.table.recordkey.fields"))),
        "partition_by": ([p.split(":")[0] for p in opts.partition_by], _fields(props.get("hoodie.table.partition.fields"))),
    }
    if not opts.record_key:
        expected.pop("record_key")  # no key given: Hudi generates keys or reuses the table's own
    mismatches = [
        f"{name} is {_show(wanted)} but the table was created with {_show(actual)}"
        for name, (wanted, actual) in expected.items()
        if wanted != actual
    ]
    if mismatches:
        raise TributaryError(
            f"hudi table {opts.table} at {opts.location}: {'; '.join(mismatches)} "
            "(these are fixed once the table exists; write to another path to change them)"
        )


def _show(value: str | list[str]) -> str:
    if isinstance(value, list):
        return ", ".join(value) if value else "no partitioning"
    return value.lower()


def check_schema_unchanged(spark, incoming: StructType, opts: HudiSinkOptions) -> None:
    """``schema_evolution: strict``: the data must have exactly the table's columns and types."""
    stored = {f.name: f.dataType.simpleString() for f in table_schema(spark, opts.location).fields}
    data = {f.name: f.dataType.simpleString() for f in incoming.fields}
    problems = []
    if added := sorted(set(data) - set(stored)):
        problems.append(f"not in the table: {', '.join(added)}")
    if missing := sorted(set(stored) - set(data)):
        problems.append(f"missing from the data: {', '.join(missing)}")
    if changed := sorted(f"{c} ({stored[c]} -> {data[c]})" for c in set(stored) & set(data) if stored[c] != data[c]):
        problems.append(f"type changed: {', '.join(changed)}")
    if problems:
        raise TributaryError(
            f"hudi table {opts.table}: the data's schema differs from the table's ({'; '.join(problems)}) "
            "and schema_evolution is 'strict'"
        )
