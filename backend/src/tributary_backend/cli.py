from __future__ import annotations

import typer

app = typer.Typer(name="tributary-backend", help="Tributary API server.", no_args_is_help=True, add_completion=False)


@app.command()
def migrate() -> None:
    """Apply database migrations."""
    from .migrate import upgrade

    upgrade()
    typer.echo("database is up to date")


@app.command()
def serve(
    host: str = "127.0.0.1",
    port: int = 8000,
    reload: bool = False,
    skip_migrate: bool = typer.Option(False, "--skip-migrate", help="Do not apply migrations on start"),
) -> None:
    """Run the API (applies migrations first)."""
    import uvicorn

    if not skip_migrate:
        migrate()
    uvicorn.run("tributary_backend.main:app", host=host, port=port, reload=reload)


if __name__ == "__main__":
    app()
