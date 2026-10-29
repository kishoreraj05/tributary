"""Built-in plugins.

Each one is also declared as an entry point in pyproject.toml, like a third-party plugin
would be. The registry loads the list below regardless, so a built-in never goes missing
when the installed package metadata predates it (an editable install that was not
re-run after a new plugin was added, for example).
"""

from __future__ import annotations

BUILTIN: dict[str, dict[str, str]] = {
    "source": {
        "dummy": "tributary.plugins.sources.dummy:DummySource",
        "file": "tributary.plugins.sources.file:FileSource",
        "kafka": "tributary.plugins.sources.kafka:KafkaSource",
        "mongodb": "tributary.plugins.sources.mongodb:MongoDbSource",
        "delta": "tributary.plugins.sources.delta:DeltaSource",
        "elasticsearch": "tributary.plugins.sources.elasticsearch:ElasticsearchSource",
        "rest_api": "tributary.plugins.sources.rest_api:RestApiSource",
        "hudi": "tributary.plugins.sources.hudi:HudiSource",
        "iceberg": "tributary.plugins.sources.iceberg:IcebergSource",
    },
    "transform": {
        "sql": "tributary.plugins.transforms.sql:SqlTransform",
        "rest_api_enrichment": "tributary.plugins.transforms.rest_api_enrichment:RestApiEnrichment",
        "deequ": "tributary.plugins.transforms.deequ:DeequTransform",
    },
    "sink": {
        "file": "tributary.plugins.sinks.file:FileSink",
        "console": "tributary.plugins.sinks.console:ConsoleSink",
        "dummy": "tributary.plugins.sinks.dummy:DummySink",
        "iceberg": "tributary.plugins.sinks.iceberg:IcebergSink",
        "delta": "tributary.plugins.sinks.delta:DeltaSink",
        "elasticsearch": "tributary.plugins.sinks.elasticsearch:ElasticsearchSink",
        "mongodb": "tributary.plugins.sinks.mongodb:MongoDbSink",
        "hudi": "tributary.plugins.sinks.hudi:HudiSink",
    },
    "check": {
        "not_null": "tributary.plugins.checks.row:NotNullCheck",
        "range": "tributary.plugins.checks.row:RangeCheck",
        "regex": "tributary.plugins.checks.row:RegexCheck",
        "accepted_values": "tributary.plugins.checks.row:AcceptedValuesCheck",
        "expression": "tributary.plugins.checks.row:ExpressionCheck",
        "unique": "tributary.plugins.checks.dataset:UniqueCheck",
        "row_count": "tributary.plugins.checks.dataset:RowCountCheck",
    },
    "metrics_sink": {
        "console": "tributary.plugins.metrics_sinks.console:ConsoleMetricsSink",
        "file": "tributary.plugins.metrics_sinks.file:FileMetricsSink",
        "jdbc": "tributary.plugins.metrics_sinks.jdbc:JdbcMetricsSink",
        "postgres": "tributary.plugins.metrics_sinks.jdbc:PostgresMetricsSink",
        "prometheus": "tributary.plugins.metrics_sinks.prometheus:PrometheusMetricsSink",
    },
    "schema": {
        "ddl": "tributary.plugins.schemas.basic:DdlSchema",
        "static": "tributary.plugins.schemas.basic:StaticSchema",
        "infer": "tributary.plugins.schemas.basic:InferSchema",
        "registry": "tributary.plugins.schemas.registry:SchemaRegistrySchema",
    },
    "secret_provider": {
        "env": "tributary.plugins.secrets.env:EnvSecretProvider",
    },
}
