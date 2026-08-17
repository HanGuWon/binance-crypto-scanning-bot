from __future__ import annotations

from fastapi import FastAPI, Query

from signalbot.persistence.repository import SqlRepository


def create_api(repository: SqlRepository) -> FastAPI:
    app = FastAPI(title="Binance Signal Bot API", version="0.1.0")

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/health/ready")
    def ready() -> dict[str, str]:
        return {"status": "ready" if repository.ready else "not_ready"}

    @app.get("/signals/recent")
    def recent_signals(limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, object]]:
        return [d.model_dump(mode="json") for d in repository.recent_signals(limit)]

    return app
