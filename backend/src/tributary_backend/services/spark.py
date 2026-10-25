"""The long-lived Spark session used for previews and deep validation."""

from __future__ import annotations

import logging
import os
import sys
import threading

from tributary.registry import default_registry
from tributary.runtime import format_package

from ..settings import Settings, get_settings

log = logging.getLogger(__name__)
_lock = threading.Lock()
_session = None


def preview_packages(settings: Settings) -> list[str]:
    """Every package any registered plugin declares: one shared session serves all pipelines."""
    packages: list[str] = []
    for category in ("source", "sink", "metrics_sink", "schema", "transform"):
        for plugin in default_registry().plugins(category).values():
            for pkg in plugin.spark_packages:
                pkg = format_package(pkg)
                if pkg not in packages:
                    packages.append(pkg)
    return packages + [p for p in settings.preview_extra_packages if p not in packages]


def get_spark():
    global _session
    with _lock:
        if _session is not None and not _is_stopped(_session):
            return _session
        _session = _build(get_settings())
        return _session


def stop_spark() -> None:
    global _session
    with _lock:
        if _session is not None:
            _session.stop()
            _session = None


def _is_stopped(session) -> bool:
    sc = getattr(session, "_sc", None)
    return sc is not None and sc._jsc is None


def _build(settings: Settings):
    from pyspark.sql import SparkSession

    if settings.spark_remote:
        log.info("connecting to Spark Connect at %s", settings.spark_remote)
        return SparkSession.builder.remote(settings.spark_remote).getOrCreate()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master(settings.spark_master)
        .appName("tributary-preview")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    packages = preview_packages(settings)
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
