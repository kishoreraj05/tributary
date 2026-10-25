from __future__ import annotations

import pytest

from conftest import pipeline
from tributary import SpecError, compile_pipeline
from tributary.plugins.transforms.sql import sql_reference_issues


def test_known_aliases_and_ctes_pass():
    sql = "WITH big AS (SELECT * FROM orders WHERE amount > 10) SELECT b.*, c.name FROM big b JOIN customers c USING (id)"
    assert sql_reference_issues(sql, ["orders", "customers"]) == []


def test_unknown_table_with_suggestion():
    [issue] = sql_reference_issues("SELECT * FROM order", ["orders"])
    assert "unknown table 'order'" in issue and "did you mean 'orders'" in issue


def test_catalog_tables_need_allow_list():
    sql = "SELECT * FROM orders o JOIN dims.country d ON o.c = d.c"
    assert "unknown table 'dims.country'" in sql_reference_issues(sql, ["orders"])[0]
    assert sql_reference_issues(sql, ["orders"], ["dims.country"]) == []


@pytest.mark.parametrize("sql,expected", [
    ("SELEC * FROM orders", "syntax error"),
    ("INSERT INTO orders VALUES (1)", "only SELECT"),
    ("SELECT 1; SELECT 2", "exactly one SQL statement"),
])
def test_rejects_bad_statements(sql, expected):
    assert expected in sql_reference_issues(sql, ["orders"])[0]


def test_compile_reports_sql_issue_at_node_path(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv)
    doc["transforms"][0]["options"]["sql"] = "SELECT * FROM orders_src"
    with pytest.raises(SpecError) as err:
        compile_pipeline(doc)
    [issue] = err.value.issues
    assert issue.path == "transforms[0].options"
    assert "available inputs: orders" in issue.message
