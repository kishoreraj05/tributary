from __future__ import annotations

from typing import Any

OptionValue = str | int | float | bool


def spark_options(options: dict[str, Any]) -> dict[str, str]:
    """Spark reader/writer options must be strings; YAML gives us bools and numbers."""
    return {k: (str(v).lower() if isinstance(v, bool) else str(v)) for k, v in options.items()}
