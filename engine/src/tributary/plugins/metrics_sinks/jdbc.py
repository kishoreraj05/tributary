from __future__ import annotations

from pydantic import Field

from ...core import MetricsSink, PluginOptions
from ...core.metrics_sink import results_frame


class JdbcMetricsOptions(PluginOptions):
    url: str = Field(..., description="JDBC URL, e.g. jdbc:postgresql://host:5432/db")
    table: str = "dq_results"
    user: str | None = None
    password: str | None = Field(None, description="Use ${secret:...}")
    driver: str | None = None
    properties: dict[str, str] = Field(default_factory=dict)


class JdbcMetricsSink(MetricsSink):
    """Appends data-quality results to a database table over JDBC."""

    type_name = "jdbc"
    display_name = "JDBC table"
    icon = "database"
    Options = JdbcMetricsOptions
    accepts_connections = ("*",)

    def emit(self, ctx, results, opts: JdbcMetricsOptions) -> None:
        props = dict(opts.properties)
        for key in ("user", "password", "driver"):
            value = getattr(opts, key)
            if value is not None:
                props[key] = value
        results_frame(ctx, results).write.mode("append").jdbc(opts.url, opts.table, properties=props)


class PostgresMetricsOptions(JdbcMetricsOptions):
    driver: str | None = "org.postgresql.Driver"


class PostgresMetricsSink(JdbcMetricsSink):
    """Appends data-quality results to a PostgreSQL table."""

    type_name = "postgres"
    display_name = "PostgreSQL table"
    Options = PostgresMetricsOptions
    spark_packages = ("org.postgresql:postgresql:42.7.4",)
    accepts_connections = ("jdbc",)
