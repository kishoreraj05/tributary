"""The local environment's stand-in for any sink: a node with ``dummy.enabled`` becomes
this sink in the ``local`` environment. Nothing is written; previews show the rows the
real sink would have received, and runs log how many there were."""

from __future__ import annotations

from pydantic import Field

from ...core import PluginOptions, Sink


class DummySinkOptions(PluginOptions):
    stands_for: str = Field("", description="The type of the node this sink stands in for")


class DummySink(Sink):
    """Writes nothing: the rows a real sink would get, for the local environment."""

    type_name = "dummy"
    display_name = "Dummy sink"
    icon = "box"
    hidden = True
    Options = DummySinkOptions

    def write(self, ctx, df, opts: DummySinkOptions) -> None:
        batch = f" (batch {ctx.batch_id})" if ctx.batch_id is not None else ""
        ctx.log.info("dummy %s sink%s: %d rows not written (local environment)", opts.stands_for or "", batch, df.count())

    def describe(self, opts: DummySinkOptions) -> str:
        return f"dummy {opts.stands_for} sink: nothing is written (local environment)".replace("  ", " ")
