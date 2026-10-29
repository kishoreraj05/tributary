from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

OptionValue = str | int | float | bool


def spark_options(options: dict[str, Any]) -> dict[str, str]:
    """Spark reader/writer options must be strings; YAML gives us bools and numbers."""
    return {k: (str(v).lower() if isinstance(v, bool) else str(v)) for k, v in options.items()}


def when(field: str, applies: tuple[str | bool, ...] | None = None, required: tuple[str | bool, ...] = ()) -> dict:
    """Schema hint (``json_schema_extra``): the annotated field applies (is shown) when ``field``
    has one of the ``applies`` values (all values if None) and is mandatory when it has one
    of ``required``. Values are strings for enum fields, booleans for checkboxes. The UI form
    reads it as ``x-when``; see SchemaForm."""
    rule: dict = {"field": field, "required_in": list(required)}
    if applies is not None:
        rule["in"] = list(applies)
    return {"x-when": rule}


def has_placeholder(value: str) -> bool:
    """True when a value still holds a ``${secret:...}``/``${env.NAME}`` placeholder. Specs are
    validated without resolving secrets (versions, the editor), so a validator that checks the
    shape of a value must let placeholders through; the resolved value is checked at run time."""
    return "${" in value


def missing_columns(schema: StructType, names: list[str]) -> list[str]:
    """The names that are not columns of ``schema``; ``a.b`` looks inside struct column ``a``."""
    from pyspark.sql.types import StructType

    def present(struct: StructType, parts: list[str]) -> bool:
        for f in struct.fields:
            if f.name == parts[0]:
                return len(parts) == 1 or (isinstance(f.dataType, StructType) and present(f.dataType, parts[1:]))
        return False

    return [n for n in names if not present(schema, n.split("."))]


def column_errors(schema: StructType, fields: dict[str, list[str]]) -> list[tuple[str, str]]:
    """One ``(option, message)`` pair per option whose columns are not all in ``schema``,
    the shape ``Sink.validate_input`` returns: ``{option: [column, ...]}`` ->
    ``(option, "not in the data: x (columns: ...)")``."""
    errors = []
    for option, names in fields.items():
        missing = missing_columns(schema, names)
        if missing:
            errors.append((option, f"not in the data: {', '.join(missing)} (columns: {', '.join(f.name for f in schema.fields)})"))
    return errors


def format_errors(errors: list[tuple[str, str]]) -> str:
    """``validate_input`` pairs as one line for an error raised at write time."""
    return "; ".join(f"{field} {message}" if field else message for field, message in errors)
