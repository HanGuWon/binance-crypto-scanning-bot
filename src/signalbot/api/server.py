from __future__ import annotations

import time

from fastapi import FastAPI, HTTPException, Query

from signalbot.domain.enums import Market
from signalbot.persistence.repository import ProtectionContextCursorError, SqlRepository


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

    @app.get("/protection-contexts")
    def protection_contexts(
        market: Market,
        symbol: str,
        primary_interval: str,
        limit: int = Query(default=100, ge=1, le=200),
        after_context_id: str | None = None,
        after_close_time_ms: int | None = Query(default=None, ge=0),
    ) -> list[dict[str, object]]:
        try:
            contexts = repository.list_protection_contexts(
                market=market,
                symbol=symbol,
                primary_interval=primary_interval,
                limit=limit,
                after_context_id=after_context_id,
                after_close_time_ms=after_close_time_ms,
            )
        except ProtectionContextCursorError as exc:
            raise HTTPException(
                status_code=409,
                detail="PROTECTION_CONTEXT_CURSOR_INVALID",
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return [context.model_dump(mode="json") for context in contexts]

    @app.get("/protection-contexts/latest")
    def latest_protection_context(
        market: Market,
        symbol: str,
        primary_interval: str,
        max_age_ms: int = Query(gt=0, le=86_400_000),
    ) -> dict[str, object]:
        context = repository.latest_protection_context(
            market=market,
            symbol=symbol,
            primary_interval=primary_interval,
        )
        if context is None:
            return {"status": "NOT_FOUND", "age_ms": None, "context": None}
        now_ms = time.time_ns() // 1_000_000
        age_ms = max(0, now_ms - context.candle_close_time_ms)
        return {
            "status": "STALE_CONTEXT" if age_ms > max_age_ms else "OK",
            "age_ms": age_ms,
            "context": context.model_dump(mode="json"),
        }

    return app
