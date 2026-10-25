from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from ...core import BatchReadable, PluginOptions, Source, StreamReadable
from ...errors import TributaryError
from .._util import OptionValue, spark_options

_KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
_AVRO_PKG = "org.apache.spark:spark-avro_{scala_version}:{spark_version}"
_META = (
    ("key", "_kafka_key"),
    ("topic", "_kafka_topic"),
    ("partition", "_kafka_partition"),
    ("offset", "_kafka_offset"),
    ("timestamp", "_kafka_timestamp"),
)


class KafkaSourceOptions(PluginOptions):
    brokers: str = Field(..., description="Bootstrap servers, comma separated (host:port)")
    topic: str | None = None
    topic_pattern: str | None = Field(None, description="Regex of topics to subscribe to")
    starting_offsets: str = Field("latest", description="earliest | latest | JSON offsets")
    ending_offsets: str = Field("latest", description="Batch reads only")
    max_offsets_per_trigger: int | None = Field(None, ge=1)
    value_format: Literal["json", "avro", "string", "raw"] = "json"
    key_format: Literal["string", "raw"] = "string"
    include_metadata: bool = Field(False, description="Add _kafka_key/_topic/_partition/_offset/_timestamp")
    schema_registry_url: str | None = None
    preview_starting_offsets: str = Field("earliest", description="Where previews start reading")
    kafka_options: dict[str, OptionValue] = Field(
        default_factory=dict, description="Extra Kafka client settings, passed as kafka.<key>"
    )

    @model_validator(mode="after")
    def _one_subscription(self) -> KafkaSourceOptions:
        if bool(self.topic) == bool(self.topic_pattern):
            raise ValueError("set exactly one of 'topic' or 'topic_pattern'")
        return self


class KafkaSource(Source, BatchReadable, StreamReadable):
    """Apache Kafka topics (JSON, Avro via Schema Registry, or raw)."""

    type_name = "kafka"
    display_name = "Kafka"
    icon = "stream"
    Options = KafkaSourceOptions
    spark_packages = (_KAFKA_PKG,)

    def required_packages(self, opts: KafkaSourceOptions) -> list[str]:
        return [_KAFKA_PKG, _AVRO_PKG] if opts.value_format == "avro" else [_KAFKA_PKG]

    def _subscribe(self, reader, opts: KafkaSourceOptions):
        reader = reader.format("kafka").option("kafka.bootstrap.servers", opts.brokers)
        reader = reader.option("subscribe", opts.topic) if opts.topic else reader.option("subscribePattern", opts.topic_pattern)
        for key, value in spark_options(opts.kafka_options).items():
            reader = reader.option(f"kafka.{key}", value)
        return reader

    def read(self, ctx, opts: KafkaSourceOptions, schema):
        raw = (
            self._subscribe(ctx.spark.read, opts)
            .option("startingOffsets", opts.starting_offsets)
            .option("endingOffsets", opts.ending_offsets)
            .load()
        )
        return self._decode(raw, opts, schema)

    def read_stream(self, ctx, opts: KafkaSourceOptions, schema):
        reader = self._subscribe(ctx.spark.readStream, opts).option("startingOffsets", opts.starting_offsets)
        if opts.max_offsets_per_trigger:
            reader = reader.option("maxOffsetsPerTrigger", opts.max_offsets_per_trigger)
        return self._decode(reader.load(), opts, schema)

    def read_preview(self, ctx, opts: KafkaSourceOptions, schema, limit: int):
        # A bounded batch read, so streaming pipelines can be previewed too.
        raw = (
            self._subscribe(ctx.spark.read, opts)
            .option("startingOffsets", opts.preview_starting_offsets)
            .option("endingOffsets", "latest")
            .load()
            .limit(limit)
        )
        return self._decode(raw, opts, schema)

    def _decode(self, raw, opts: KafkaSourceOptions, schema):
        meta = []
        if opts.include_metadata:
            for column, alias in _META:
                col = F.col(column).cast("string") if column == "key" and opts.key_format == "string" else F.col(column)
                meta.append(col.alias(alias))
        meta_names = [alias for _, alias in _META] if opts.include_metadata else []

        if opts.value_format == "string":
            return raw.select(F.col("value").cast("string").alias("value"), *meta)
        if opts.value_format == "raw":
            return raw.select(F.col("value"), *meta)
        if opts.value_format == "json":
            if schema is None or schema.struct is None:
                raise TributaryError("kafka value_format 'json' needs a schema (kind: ddl or static)")
            parsed = raw.select(F.from_json(F.col("value").cast("string"), schema.struct).alias("__v"), *meta)
            return parsed.select("__v.*", *meta_names)

        from pyspark.sql.avro.functions import from_avro

        if schema is None or schema.avro is None:
            raise TributaryError("kafka value_format 'avro' needs a schema of kind 'registry'")
        payload = (
            F.expr("substring(value, 6, length(value) - 5)") if schema.wire_format == "confluent" else F.col("value")
        )
        parsed = raw.select(from_avro(payload, schema.avro).alias("__v"), *meta)
        return parsed.select("__v.*", *meta_names)

    def output_schema(self, opts: KafkaSourceOptions, schema):
        meta = []
        if opts.include_metadata:
            key_type = StringType() if opts.key_format == "string" else BinaryType()
            meta = [
                StructField("_kafka_key", key_type),
                StructField("_kafka_topic", StringType()),
                StructField("_kafka_partition", IntegerType()),
                StructField("_kafka_offset", LongType()),
                StructField("_kafka_timestamp", TimestampType()),
            ]
        if opts.value_format == "string":
            return StructType([StructField("value", StringType()), *meta])
        if opts.value_format == "raw":
            return StructType([StructField("value", BinaryType()), *meta])
        if opts.value_format == "json" and schema is not None and schema.struct is not None:
            return StructType([*schema.struct.fields, *meta])
        return None
