from __future__ import annotations

import pytest

from tributary.core import CATEGORIES
from tributary.plugins.sinks.console import ConsoleSink
from tributary.plugins.sources.file import FileSource
from tributary.registry import default_registry
from tributary.testing import assert_sink_contract, assert_source_contract, static_contract_issues

ALL_PLUGINS = [
    pytest.param(cls, id=f"{category}:{name}")
    for category in CATEGORIES
    for name, cls in sorted(default_registry().plugins(category).items())
]


@pytest.mark.parametrize("cls", ALL_PLUGINS)
def test_static_contract(cls):
    assert static_contract_issues(cls) == []


def test_file_source_contract(spark, orders_csv):
    opts = FileSource.Options(path=str(orders_csv), format="csv", read_options={"header": True})
    assert_source_contract(spark, FileSource(), opts, limit=2)


def test_console_sink_contract(spark):
    assert_sink_contract(spark, ConsoleSink(), ConsoleSink.Options(rows=1), spark.range(3))
