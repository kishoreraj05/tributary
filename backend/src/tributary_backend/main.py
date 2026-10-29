from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from tributary.errors import TributaryError

from . import __version__
from .api import components, connections, design, pipelines, runs
from .db import session_factory
from .models import Run
from .services import runner
from .services.spark import stop_spark
from .settings import get_settings

log = logging.getLogger(__name__)


def _reconcile_runs() -> None:
    """After a restart, runs whose worker is gone are marked failed."""
    with session_factory()() as session:
        for run in session.scalars(select(Run).where(Run.status.in_(runner.ACTIVE))).all():
            runner.refresh(session, run)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    settings.logs_dir.mkdir(parents=True, exist_ok=True)
    settings.samples_dir.mkdir(parents=True, exist_ok=True)
    _reconcile_runs()
    yield
    stop_spark()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="Tributary API", version=__version__, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware, allow_origins=settings.cors_origins, allow_methods=["*"], allow_headers=["*"]
    )

    @app.exception_handler(TributaryError)
    async def tributary_error(request: Request, exc: TributaryError):
        return JSONResponse({"detail": {"message": str(exc)}}, status_code=400)

    @app.get("/health", tags=["meta"])
    def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    for module in (components, pipelines, connections, design, runs):
        app.include_router(module.router, prefix="/api/v1")
    if settings.ui_dir:
        _serve_ui(app, settings.ui_dir)
    return app


def _serve_ui(app: FastAPI, ui_dir) -> None:
    """Serves a built UI: its assets as files, every other path as index.html (the UI is a
    single-page app that routes /pipelines/<id> and /connections itself)."""
    index = ui_dir / "index.html"
    if not index.is_file():
        raise RuntimeError(f"TRIBUTARY_UI_DIR={ui_dir} has no index.html (build the UI with 'npm run build' first)")
    if (ui_dir / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=ui_dir / "assets"), name="ui-assets")

    @app.get("/{path:path}", include_in_schema=False)
    async def ui(path: str):
        if path.startswith("api/"):
            raise HTTPException(404)
        candidate = (ui_dir / path).resolve() if path else index
        if path and candidate.is_file() and ui_dir.resolve() in candidate.parents:
            return FileResponse(candidate)
        return FileResponse(index)


app = create_app()
