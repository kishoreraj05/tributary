from __future__ import annotations

import difflib
import threading

import sqlglot
from pydantic import Field
from sqlglot import exp

from ...core import PluginOptions, Transform

# Temp views are session-wide. Spark resolves them while analysing the query inside
# spark.sql(), so holding this lock only around register -> sql -> drop is enough to keep
# concurrent previews/micro-batches in one process from seeing each other's views.
_VIEW_LOCK = threading.RLock()


class SqlOptions(PluginOptions):
    sql: str = Field(..., description="A single SELECT; refer to inputs by their 'as' names", json_schema_extra={"format": "sql"})
    allow_catalog_tables: list[str] = Field(
        default_factory=list, description="Catalog tables (besides inputs) the query may read, e.g. db.dim_country"
    )


class SqlTransform(Transform):
    """Spark SQL over the node's inputs; each input is visible as a table named by its alias."""

    type_name = "sql"
    display_name = "SQL"
    icon = "code"
    Options = SqlOptions

    def validate(self, opts: SqlOptions, input_aliases: list[str]) -> list[str]:
        return sql_reference_issues(opts.sql, input_aliases, opts.allow_catalog_tables)

    def complexity_hint(self, opts: SqlOptions) -> int:
        try:
            tree = sqlglot.parse_one(opts.sql, read="spark")
        except sqlglot.errors.ParseError:
            return 0
        return len(list(tree.find_all(exp.Join))) + len(list(tree.find_all(exp.Group)))

    def apply(self, ctx, inputs, opts: SqlOptions):
        with _VIEW_LOCK:
            for alias, df in inputs.items():
                df.createOrReplaceTempView(alias)
            try:
                return ctx.spark.sql(opts.sql)
            finally:
                for alias in inputs:
                    ctx.spark.catalog.dropTempView(alias)


def sql_reference_issues(sql: str, input_aliases: list[str], allowed_tables: list[str] | None = None) -> list[str]:
    """Static checks: one SELECT statement that only reads its declared inputs (or CTEs)."""
    try:
        statements = [s for s in sqlglot.parse(sql, read="spark") if s is not None]
    except sqlglot.errors.ParseError as exc:
        first = exc.errors[0] if exc.errors else {}
        where = f" (line {first.get('line')}, col {first.get('col')})" if first else ""
        return [f"SQL syntax error{where}: {first.get('description', str(exc).splitlines()[0])}"]
    if len(statements) != 1:
        return [f"expected exactly one SQL statement, found {len(statements)}"]
    tree = statements[0]
    if not isinstance(tree, exp.Query):
        return [f"only SELECT queries are allowed, found {tree.key.upper()}"]

    ctes = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    allowed = {a.lower() for a in input_aliases}
    catalog = {t.lower() for t in (allowed_tables or [])}
    issues: list[str] = []
    for table in tree.find_all(exp.Table):
        if not table.name:
            continue
        qualified = ".".join(p for p in (table.catalog, table.db, table.name) if p)
        if qualified.lower() in catalog:
            continue
        if not (table.db or table.catalog) and table.name.lower() in allowed | ctes:
            continue
        message = f"unknown table '{qualified}'; available inputs: {', '.join(input_aliases)}"
        guess = difflib.get_close_matches(table.name, input_aliases, n=1)
        if guess:
            message += f" (did you mean '{guess[0]}'?)"
        if message not in issues:
            issues.append(message)
    return issues
