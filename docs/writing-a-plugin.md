# Writing a plugin

Every connector, transform and check is a small class. Engine code is never edited.

## 1. Implement the family interface

```python
from pydantic import Field
from tributary.core import BatchReadable, PluginOptions, Source


class JdbcSourceOptions(PluginOptions):        # extra keys are rejected automatically
    url: str = Field(..., description="JDBC URL")
    table: str
    user: str | None = None
    password: str | None = Field(None, description="Use ${secret:...}")


class JdbcSource(Source, BatchReadable):       # add StreamReadable if it can stream
    """Reads a table over JDBC."""

    type_name = "jdbc"                         # the value users write in `type:`
    Options = JdbcSourceOptions                # becomes the UI form via JSON Schema
    spark_packages = ("org.postgresql:postgresql:42.7.4",)

    def read(self, ctx, opts, schema):
        props = {"user": opts.user or "", "password": opts.password or ""}
        return ctx.spark.read.jdbc(opts.url, opts.table, properties=props)
```

Families and their main methods:

| Category | Base class | Method(s) you implement |
|---|---|---|
| source | `Source` + `BatchReadable` / `StreamReadable` | `read`, `read_stream`; optionally `read_preview`, `output_schema`, `estimate_input_bytes` |
| transform | `Transform` | `apply(ctx, inputs, opts)`; optionally `validate`, `complexity_hint` |
| sink | `Sink` | `write(ctx, df, opts)` (`ctx.batch_id` is set in streaming); optionally `describe` |
| check | `RowCheck` / `DatasetCheck` | `failure_condition` / `metric` + `passed` |
| metrics_sink | `MetricsSink` | `emit(ctx, results, opts)` |
| schema | `SchemaResolver` | `resolve(ctx, opts, source_opts)` |
| secret_provider | `SecretProvider` | `get(path)` |

## 2. Register it

In your package's `pyproject.toml`:

```toml
[project.entry-points."tributary.sources"]
jdbc = "my_package.jdbc:JdbcSource"
```

After `pip install`, the new type appears in `tributary list-components`, in `schema-export` (and so in the UI), and in spec validation. For quick experiments you can register a plugin in-process instead with `tributary.registry.register`.

## 3. Test it against the contract

```python
from tributary.testing import assert_source_contract, static_contract_issues

def test_contract(spark):
    assert static_contract_issues(JdbcSource) == []
    assert_source_contract(spark, JdbcSource(), JdbcSource.Options(url=..., table="t"))
```
