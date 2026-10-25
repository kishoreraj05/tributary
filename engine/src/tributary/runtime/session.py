from __future__ import annotations

import os
import sys
from typing import Any

import pyspark
from pyspark.sql import SparkSession

from ..spec import CompiledPipeline
from .resources import ResourceDecision


def _versions() -> tuple[str, str]:
    spark_version = pyspark.__version__.split(".dev")[0]
    scala = "2.13" if int(spark_version.split(".")[0]) >= 4 else "2.12"
    return spark_version, scala


def format_package(pkg: str) -> str:
    """Fills "{spark_version}" / "{scala_version}" in a plugin's Maven coordinate."""
    spark_version, scala = _versions()
    return pkg.format(spark_version=spark_version, scala_version=scala)


def required_packages(compiled: CompiledPipeline) -> list[str]:
    """Maven packages needed by the plugins this pipeline actually uses."""
    registry, spec = compiled.registry, compiled.spec
    used: list[tuple[str, str, Any]] = []
    used += [("source", s.type, s.options) for s in spec.sources]
    used += [("schema", s.schema_.kind, s.schema_) for s in spec.sources if s.schema_ is not None]
    used += [("transform", t.type, t.options) for t in spec.transforms]
    used += [("sink", s.type, s.options) for s in spec.sinks]
    for q in spec.quality:
        used += [("check", c.type, c) for c in q.checks]
        used += [("metrics_sink", t.type, t.options) for t in q.results_sinks]
        if q.quarantine_sink is not None:
            used.append(("sink", q.quarantine_sink.type, q.quarantine_sink.options))

    packages: list[str] = []
    for category, name, opts in used:
        for pkg in registry.create(category, name).required_packages(opts):
            pkg = format_package(pkg)
            if pkg not in packages:
                packages.append(pkg)
    return packages


def create_session(compiled: CompiledPipeline, decision: ResourceDecision) -> SparkSession:
    builder = SparkSession.builder.appName(compiled.runtime.app_name or f"tributary-{compiled.pipeline_id}")
    if decision.master and decision.master.startswith("local"):
        # Local workers must use the driver's interpreter, not whatever "python3" is on PATH.
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    if decision.master:
        builder = builder.master(decision.master)
    conf = dict(decision.conf)
    packages = required_packages(compiled)
    if packages:
        conf["spark.jars.packages"] = ",".join(p for p in [conf.get("spark.jars.packages"), *packages] if p)
    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
