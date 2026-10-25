from __future__ import annotations

from pydantic import Field

from ...core import PluginOptions, Sink


class ConsoleSinkOptions(PluginOptions):
    rows: int = Field(20, ge=1)
    truncate: bool = False


class ConsoleSink(Sink):
    """Prints rows to stdout. Handy while developing."""

    type_name = "console"
    display_name = "Console"
    icon = "terminal"
    Options = ConsoleSinkOptions

    def write(self, ctx, df, opts: ConsoleSinkOptions) -> None:
        batch = f" batch {ctx.batch_id}" if ctx.batch_id is not None else ""
        print(f"--- {ctx.pipeline_id}{batch} ---")
        df.show(n=opts.rows, truncate=opts.truncate)

    def describe(self, opts: ConsoleSinkOptions) -> str:
        return "stdout"
