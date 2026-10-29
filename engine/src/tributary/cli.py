from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import typer

from . import api
from .errors import QualityFailure, SpecError, TributaryError, short_error
from .planner import build_plan
from .runtime import decide_resources
from .spec import dump_document, load_document, restrict_to

app = typer.Typer(
    name="tributary",
    help="Config-driven Spark pipelines.",
    no_args_is_help=True,
    add_completion=False,
    pretty_exceptions_enable=False,
)

FileArg = typer.Argument(..., exists=True, dir_okay=False, help="Pipeline file (.yaml or .json)")
EnvOpt = typer.Option(None, "--env", "-e", help="Environment to use, e.g. dev or prod")


@app.callback()
def _setup(verbose: bool = typer.Option(False, "--verbose", "-v")) -> None:
    logging.basicConfig(level=logging.INFO if verbose else logging.WARNING, format="%(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)


def _compile(file: Path, env: Optional[str], *, secrets: bool = False, only_node: Optional[str] = None):
    """``only_node`` compiles just that node and its upstream (what a preview needs)."""
    try:
        doc = load_document(file)
        if only_node:
            doc = restrict_to(doc, only_node)
        compiled = api.compile_pipeline(doc, env, resolve_secrets=secrets)
    except SpecError as exc:
        typer.secho(f"✗ {file} is invalid:", fg="red", err=True)
        for issue in exc.issues:
            typer.echo(f"  - {issue}", err=True)
        raise typer.Exit(1) from None
    for warning in compiled.warnings:
        typer.secho(f"! {warning}", fg="yellow", err=True)
    return compiled


def _quiet(spark) -> None:
    spark.sparkContext.setLogLevel("WARN")


@app.command()
def validate(
    file: Path = FileArg,
    env: Optional[str] = EnvOpt,
    deep: bool = typer.Option(False, "--deep", help="Also check columns and types with Spark (no data is read)"),
) -> None:
    """Validate a pipeline: schema, references, DAG, SQL tables (and with --deep, columns)."""
    compiled = _compile(file, env, secrets=deep)
    if deep:
        from .runtime import create_session

        # The pipeline's own session, as for preview and run: plugins whose schema comes from
        # the data system (a Delta table's log, an existing sink table) need their jars and settings.
        spark = create_session(compiled, decide_resources(compiled))
        _quiet(spark)
        failures = [a for a in api.analyze(compiled, spark=spark).values() if not a.ok]
        for a in failures:
            for message in a.messages:
                typer.secho(f"✗ {a.node_id}: {message}", fg="red", err=True)
        if failures:
            raise typer.Exit(1)
    typer.secho(f"✓ {compiled.pipeline_id} is valid (env={compiled.env})", fg="green")


@app.command("plan")
def plan_cmd(file: Path = FileArg, env: Optional[str] = EnvOpt) -> None:
    """Show the physical plan: step order, cache points, quality gates and resources."""
    compiled = _compile(file, env)
    typer.echo(build_plan(compiled).explain())
    typer.echo()
    typer.echo(decide_resources(compiled).describe())


@app.command()
def run(file: Path = FileArg, env: Optional[str] = EnvOpt) -> None:
    """Run a pipeline (streaming pipelines run until stopped)."""
    compiled = _compile(file, env, secrets=True)
    try:
        from .runtime import create_session

        spark = create_session(compiled, decide_resources(compiled))
        _quiet(spark)
        summary = api.run(compiled, spark=spark)
    except QualityFailure as exc:
        typer.secho(f"✗ {exc}", fg="red", err=True)
        raise typer.Exit(2) from None
    except TributaryError as exc:
        typer.secho(f"✗ {exc}", fg="red", err=True)
        raise typer.Exit(1) from None
    except Exception as exc:  # noqa: BLE001 - Spark errors: show the gist, full trace with -v
        if logging.getLogger().isEnabledFor(logging.INFO):
            raise
        typer.secho(f"✗ {short_error(exc)}  (rerun with -v for details)", fg="red", err=True)
        raise typer.Exit(1) from None
    if isinstance(summary, api.RunSummary):
        failed = [r for r in summary.quality if not r.passed]
        typer.secho(
            f"✓ run {summary.run_id}: wrote {', '.join(summary.sinks_written) or 'nothing'}; "
            f"{len(summary.quality)} checks, {len(failed)} failed",
            fg="green",
        )


@app.command()
def preview(
    file: Path = FileArg,
    node: str = typer.Option(..., "--node", "-n", help="Node to preview"),
    env: Optional[str] = EnvOpt,
    limit: int = typer.Option(20, "--limit", "-l"),
    sample: list[str] = typer.Option([], "--sample", help="SOURCE_ID=path to use a sample file for a source"),
    write: bool = typer.Option(False, "--write", help="For sinks: actually write the sample"),
    as_json: bool = typer.Option(False, "--json", help="Print the result as JSON"),
) -> None:
    """Preview a node's output on a bounded sample (only the node and its upstream need to be valid)."""
    compiled = _compile(file, env, secrets=True, only_node=node)
    samples = {}
    for item in sample:
        source_id, sep, path = item.partition("=")
        if not sep:
            raise typer.BadParameter(f"expected SOURCE_ID=path, got '{item}'")
        samples[source_id] = path
    try:
        from .runtime import create_session

        spark = create_session(compiled, decide_resources(compiled))
        _quiet(spark)
        kept = {s.id for s in compiled.spec.sources}  # sources left out of the preview need no sample
        samples = {k: v for k, v in samples.items() if k in kept}
        result = api.preview(compiled, node, spark=spark, limit=limit, sample_data=samples, write_sinks=write)
    except TributaryError as exc:
        typer.secho(f"✗ {exc}", fg="red", err=True)
        raise typer.Exit(1) from None
    except Exception as exc:  # noqa: BLE001 - Spark errors: show the gist, full trace with -v
        if logging.getLogger().isEnabledFor(logging.INFO):
            raise
        typer.secho(f"✗ {short_error(exc)}  (rerun with -v for details)", fg="red", err=True)
        raise typer.Exit(1) from None
    if as_json:
        typer.echo(json.dumps(result.to_dict(), indent=2, default=str))
        return
    columns = [f["name"] for f in result.schema["fields"]]
    typer.echo(_table(columns, result.rows))
    if result.target:
        typer.echo(f"\ntarget: {result.target} ({'written' if result.written else 'dry run'})")
    for r in result.quality:
        status = "PASS" if r["passed"] else "FAIL"
        detail = f"failed={r['failed_rows']}/{r['total_rows']}" if r["level"] == "row" else f"observed={r['observed_value']}"
        typer.echo(f"[{status}] {r['quality_id']}.{r['check_id']}: {detail}")


@app.command("schema-export")
def schema_export(output: Optional[Path] = typer.Option(None, "--output", "-o")) -> None:
    """Export the JSON Schema of the pipeline spec and the component catalog."""
    text = json.dumps(api.export_schema(), indent=2)
    if output:
        output.write_text(text + "\n")
        typer.echo(f"wrote {output}")
    else:
        typer.echo(text)


@app.command("list-components")
def list_components() -> None:
    """List available sources, transforms, sinks, checks and more."""
    for category, plugins in api.list_components().items():
        typer.secho(category, bold=True)
        for p in plugins:
            caps = f" [{', '.join(p['capabilities'])}]" if p["capabilities"] else ""
            typer.echo(f"  {p['type']:<16} {p['description']}{caps}")


@app.command()
def convert(src: Path = FileArg, dst: Path = typer.Argument(..., help="Output .yaml or .json")) -> None:
    """Convert a pipeline between YAML and JSON."""
    dst.write_text(dump_document(load_document(src), "json" if dst.suffix.lower() == ".json" else "yaml"))
    typer.echo(f"wrote {dst}")


def _table(columns: list[str], rows: list[dict], width: int = 28) -> str:
    def cell(v) -> str:
        text = "null" if v is None else str(v)
        return text if len(text) <= width else text[: width - 1] + "…"

    widths = [max(len(c), *(len(cell(r.get(c))) for r in rows)) if rows else len(c) for c in columns]
    line = " | ".join(c.ljust(w) for c, w in zip(columns, widths))
    sep = "-+-".join("-" * w for w in widths)
    body = [" | ".join(cell(r.get(c)).ljust(w) for c, w in zip(columns, widths)) for r in rows]
    return "\n".join([line, sep, *body, f"({len(rows)} rows)"])


if __name__ == "__main__":
    app()
