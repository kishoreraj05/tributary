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

Plugins can also ask for Spark session settings. `spark_packages` lists Maven coordinates; `spark_conf` lists static settings every session needs; `required_conf(opts)` adds settings that depend on the options (the Iceberg sink registers its `spark.sql.catalog.<name>` there). Be conservative with both: the backend builds one shared session for previews out of every installed plugin's packages and `spark_conf`, so anything that loads classes eagerly (SQL extensions, for instance) affects pipelines that never use your plugin. The engine applies them when it builds a session; `spark.sql.extensions` contributions are merged, and the user's `runtime.spark_conf` still wins for everything else. When a plugin may run on a session it did not build (the backend shares one session for previews), it should also be able to apply what it needs at runtime, as the Iceberg sink does with `spark.conf.set` before its first write.

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

### Fields that depend on another field

When an option only makes sense for some values of another option, say so in the schema and the UI form follows: the field is shown only for those values, gets a red star where it is mandatory, and is cleared when the other field changes. Validate the same rule with a field validator so the error lands on that field:

```python
uri: str | None = Field(
    None, validate_default=True,
    json_schema_extra={"x-when": {"field": "catalog_type", "in": ["hive", "rest"], "required_in": ["rest"]}},
)

@field_validator("uri")
@classmethod
def _uri_when_needed(cls, value, info: ValidationInfo):
    if not value and info.data.get("catalog_type") == "rest":
        raise ValueError("required for a rest catalog")
    return value
```

`in` lists the values for which the field applies (omit it for "always"); `required_in` the values for which it is mandatory. The Iceberg sink uses this for its catalog settings and for `merge_keys`; the deequ node drives every constraint field from its `type` this way.

### Transforms that only observe their input

A transform that returns its input unchanged (a verification node) should say so, so the planner counts its read of the input together with its consumers' reads and persists the input instead of recomputing it:

```python
def is_passthrough(self, opts) -> bool:
    return opts.output == "data"
```

A transform that must not be chained with another node of its type (a verification node checks the node it reads, not another verification node) says so with `chainable = False`; the spec is then rejected when one is upstream of another, and the canvas refuses the edge.

### Reporting check results from any plugin

Quality blocks are not the only place checks can run. A plugin that evaluates checks of its own builds `CheckResult` rows (`tributary.core.CheckResult`, the same shape every metrics sink takes) and hands them to the context:

```python
def apply(self, ctx, inputs, opts):
    results = [...]                      # one CheckResult per check; ctx.node_id is this node's id
    ctx.report(results)                  # -> run summary, preview results, the CLI's report
    if not ctx.preview:
        for target in opts.results_sinks:    # see below
            ctx.registry.create("metrics_sink", target.type).emit(ctx, results, target.options)
        if failing: raise QualityFailure(ctx.node_id, results)
```

Sending them to metrics sinks (and skipping that in previews) stays the plugin's job. Raising `QualityFailure` aborts the run before any sink writes, like a `fail` gate.

### Embedding metrics sinks (or sinks) in the options

Declare a `list[Target]` field with `targets("metrics_sink")` (or `targets("sink")`):

```python
from tributary.core import Target, targets

class MyOptions(PluginOptions):
    results_sinks: list[Target] = Field(default_factory=list, json_schema_extra=targets("metrics_sink"))
```

The engine then treats the entries like a quality block's `results_sinks`: each `{type, connection?, options}` is validated against the registered plugins of that category (issues land on `...options.results_sinks[i].options.<field>`), a `connection` is merged into it, its Spark packages and settings are loaded, and the UI renders the target picker for the field. At runtime the entries are typed (`target.type`, `target.options` is the sink's Options model).

## 2. Register it

In your package's `pyproject.toml`:

```toml
[project.entry-points."tributary.sources"]
jdbc = "my_package.jdbc:JdbcSource"
```

After `pip install` (re-run it for an editable install too: entry points are written into the package metadata at install time), the new type appears in `tributary list-components`, in `schema-export` (and so in the UI), and in spec validation. For quick experiments you can register a plugin in-process instead with `tributary.registry.register`.

## 3. Test it against the contract

```python
from tributary.testing import assert_source_contract, static_contract_issues

def test_contract(spark):
    assert static_contract_issues(JdbcSource) == []
    assert_source_contract(spark, JdbcSource(), JdbcSource.Options(url=..., table="t"))
```
