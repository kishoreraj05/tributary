from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from ...core import PluginOptions, ResolvedSchema, SchemaResolver


def parse_ddl(spark, ddl: str):
    return spark.createDataFrame([], schema=ddl).schema


class DdlSchemaOptions(PluginOptions):
    ddl: str = Field(..., description="Spark DDL, e.g. 'id INT, name STRING, ts TIMESTAMP'")


class DdlSchema(SchemaResolver):
    """Schema written as Spark DDL."""

    type_name = "ddl"
    Options = DdlSchemaOptions

    def resolve(self, ctx, opts: DdlSchemaOptions, source_opts) -> ResolvedSchema:
        return ResolvedSchema(struct=parse_ddl(ctx.spark, opts.ddl))


class StaticField(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    type: str = Field(..., description="Spark SQL type, e.g. INT, STRING, DECIMAL(10,2)")
    nullable: bool = True


class StaticSchemaOptions(PluginOptions):
    fields: list[StaticField] = Field(..., min_length=1)


class StaticSchema(SchemaResolver):
    """Schema as a list of fields (what the UI's schema editor produces)."""

    type_name = "static"
    Options = StaticSchemaOptions

    def resolve(self, ctx, opts: StaticSchemaOptions, source_opts) -> ResolvedSchema:
        ddl = ", ".join(f"`{f.name}` {f.type}{'' if f.nullable else ' NOT NULL'}" for f in opts.fields)
        return ResolvedSchema(struct=parse_ddl(ctx.spark, ddl))


class InferSchema(SchemaResolver):
    """Let Spark infer the schema from the data."""

    type_name = "infer"
    Options = PluginOptions

    def resolve(self, ctx, opts, source_opts) -> ResolvedSchema:
        return ResolvedSchema()
