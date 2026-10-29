from __future__ import annotations

import os
import sys
from typing import Any

import pyspark
from pyspark.sql import SparkSession

from ..core.targets import embedded_targets
from ..errors import TributaryError
from ..spec import CompiledPipeline
from .resources import ResourceDecision

EXTENSIONS_KEY = "spark.sql.extensions"
EXCLUDES_KEY = "spark.jars.excludes"
# Comma-separated lists: contributions from several plugins (and the user) add up.
LIST_KEYS = (EXTENSIONS_KEY, EXCLUDES_KEY)


def _versions() -> tuple[str, str]:
    spark_version = pyspark.__version__.split(".dev")[0]
    scala = "2.13" if int(spark_version.split(".")[0]) >= 4 else "2.12"
    return spark_version, scala


def format_package(pkg: str) -> str:
    """Fills "{spark_version}" / "{scala_version}" in a plugin's Maven coordinate."""
    spark_version, scala = _versions()
    return pkg.format(spark_version=spark_version, scala_version=scala)


def _used_plugins(compiled: CompiledPipeline) -> list[tuple[Any, Any]]:
    """(plugin instance, options) for every plugin the pipeline actually uses."""
    registry, spec = compiled.registry, compiled.spec
    used: list[tuple[str, str, Any]] = []
    used += [("source", s.type, s.options) for s in spec.sources]
    used += [("schema", s.schema_.kind, s.schema_) for s in spec.sources if s.schema_ is not None]
    used += [("transform", t.type, t.options) for t in spec.transforms]
    used += [("sink", s.type, s.options) for s in spec.sinks]
    for node in [*spec.sources, *spec.transforms, *spec.sinks]:
        used += [(category, t.type, t.options) for category, t in embedded_targets(node.options)]
    for q in spec.quality:
        used += [("check", c.type, c) for c in q.checks]
        used += [("metrics_sink", t.type, t.options) for t in q.results_sinks]
        if q.quarantine_sink is not None:
            used.append(("sink", q.quarantine_sink.type, q.quarantine_sink.options))
    return [(registry.create(category, name), opts) for category, name, opts in used]


def required_packages(compiled: CompiledPipeline) -> list[str]:
    """Maven packages needed by the plugins this pipeline actually uses."""
    packages: list[str] = []
    for plugin, opts in _used_plugins(compiled):
        for pkg in plugin.required_packages(opts):
            pkg = format_package(pkg)
            if pkg not in packages:
                packages.append(pkg)
    return packages


def merge_extensions(*values: str | None) -> str:
    """``spark.sql.extensions`` and ``spark.jars.excludes`` are comma-separated lists:
    contributions add up, in order, without repeats."""
    seen: list[str] = []
    for value in values:
        for ext in (value or "").split(","):
            ext = ext.strip()
            if ext and ext not in seen:
                seen.append(ext)
    return ",".join(seen)


def required_conf(compiled: CompiledPipeline) -> dict[str, str]:
    """Spark settings needed by the plugins this pipeline uses (catalogs, SQL extensions).

    Two plugins asking for different values of the same key is a spec problem, except for
    the list settings (``spark.sql.extensions``, ``spark.jars.excludes``) which are merged."""
    conf: dict[str, str] = {}
    for plugin, opts in _used_plugins(compiled):
        for key, value in plugin.required_conf(opts).items():
            value = str(value)
            if key in LIST_KEYS:
                conf[key] = merge_extensions(conf.get(key), value)
            elif key in conf and conf[key] != value:
                raise TributaryError(
                    f"{plugin.category} '{plugin.type_name}' needs {key}={value!r} but another plugin "
                    f"already set it to {conf[key]!r}"
                )
            else:
                conf[key] = value
    return conf


def session_conf(compiled: CompiledPipeline, decision: ResourceDecision) -> dict[str, str]:
    """Everything the session builder gets: plugin conf first, then the resource decision
    (which already carries the user's ``spark_conf`` overrides), with packages, extensions
    and exclusions merged rather than replaced."""
    conf = required_conf(compiled)
    for key, value in decision.conf.items():
        conf[key] = merge_extensions(conf.get(key), value) if key in LIST_KEYS else value
    packages = required_packages(compiled)
    if packages:
        conf["spark.jars.packages"] = ",".join(p for p in [conf.get("spark.jars.packages"), *packages] if p)
    return conf


def create_session(compiled: CompiledPipeline, decision: ResourceDecision) -> SparkSession:
    builder = SparkSession.builder.appName(compiled.runtime.app_name or f"tributary-{compiled.pipeline_id}")
    if decision.master and decision.master.startswith("local"):
        # Local workers must use the driver's interpreter, not whatever "python3" is on PATH.
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    if decision.master:
        builder = builder.master(decision.master)
    for key, value in session_conf(compiled, decision).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
