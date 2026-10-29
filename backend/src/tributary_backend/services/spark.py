"""The long-lived Spark session used for previews and deep validation."""

from __future__ import annotations

import logging
import os
import sys
import threading

from tributary.registry import default_registry
from tributary.runtime import LIST_KEYS, format_package, merge_extensions

from ..settings import Settings, get_settings

log = logging.getLogger(__name__)
_lock = threading.Lock()
_session = None


_CATEGORIES = ("source", "sink", "metrics_sink", "schema", "transform")


def preview_packages(settings: Settings) -> list[str]:
    """Every package any registered plugin declares: one shared session serves all pipelines."""
    packages: list[str] = []
    for category in _CATEGORIES:
        for plugin in default_registry().plugins(category).values():
            for pkg in plugin.spark_packages:
                pkg = format_package(pkg)
                if pkg not in packages:
                    packages.append(pkg)
    return packages + [p for p in settings.preview_extra_packages if p not in packages]


def preview_conf() -> dict[str, str]:
    """Static session settings plugins declare (SQL extensions, jar exclusions). Settings that
    depend on a pipeline's options, such as Iceberg catalogs, are applied by the plugin at
    preview time instead, since the session is shared."""
    conf: dict[str, str] = {}
    for category in _CATEGORIES:
        for plugin in default_registry().plugins(category).values():
            for key, value in plugin.spark_conf.items():
                if key in LIST_KEYS:
                    conf[key] = merge_extensions(conf.get(key), value)
                elif conf.get(key, value) != value:
                    log.warning("plugins disagree on %s (%r vs %r); keeping the first", key, conf[key], value)
                else:
                    conf[key] = value
    return conf


def get_spark():
    global _session
    with _lock:
        if _session is not None:
            if not _is_gone(_session):
                return _session
            log.warning("the preview Spark JVM is gone (crashed or killed); starting a new one")
            _discard(_session)
        _session = _build(get_settings())
        return _session


def stop_spark() -> None:
    global _session
    with _lock:
        if _session is not None:
            if _is_gone(_session):
                _discard(_session)
            else:
                _session.stop()
            _session = None


def _is_gone(session) -> bool:
    """Stopped from Python, or the JVM behind it is no longer there."""
    sc = getattr(session, "_sc", None)
    if sc is None or sc._jsc is None:
        return True
    from pyspark import SparkContext

    proc = getattr(SparkContext._gateway, "proc", None)
    if proc is not None and proc.poll() is not None:
        return True
    try:
        return bool(sc._jsc.sc().isStopped())
    except Exception:  # noqa: BLE001 - Py4J cannot reach the JVM any more
        return True


def _discard(session) -> None:
    """Forgets a session whose JVM died, so that the next build launches a new JVM.

    PySpark keeps the gateway and the active context in class attributes, and
    ``SparkSession.stop()`` talks to the JVM, so a dead one has to be cleared by hand."""
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    from pyspark.sql.context import SQLContext

    sc = getattr(session, "_sc", None)
    if sc is not None:
        sc._jsc = None  # stop() must not call into the dead JVM
        try:
            sc.stop()
        except Exception:  # noqa: BLE001
            log.debug("ignoring an error while discarding the dead context", exc_info=True)
    gateway = SparkContext._gateway
    if gateway is not None:
        try:
            gateway.shutdown()
        except Exception:  # noqa: BLE001
            pass
        proc = getattr(gateway, "proc", None)
        if proc is not None and proc.poll() is None:
            proc.kill()
    SparkContext._gateway = None
    SparkContext._jvm = None
    SparkSession._instantiatedSession = None
    SparkSession._activeSession = None
    SQLContext._instantiatedContext = None


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
    for key, value in preview_conf().items():
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session
