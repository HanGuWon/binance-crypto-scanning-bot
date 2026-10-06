from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy.exc import SQLAlchemyError

from signalbot.domain.enums import Market
from signalbot.persistence.repository import ProtectionContextCursorError, SqlRepository


def _wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def evaluate_readiness(
    repository: SqlRepository,
    markets: Sequence[Market],
    max_staleness_ms: int,
    now_ms: int,
) -> tuple[bool, dict[str, Any]]:
    """Pipeline readiness: every configured market has a fresh WebSocket heartbeat."""

    if not repository.ready:
        return False, {"status": "not_ready", "reasons": ["repository not ready"]}
    try:
        heartbeats = repository.get_heartbeats()
    except SQLAlchemyError:
        return False, {"status": "not_ready", "reasons": ["heartbeat table unreadable"]}
    reasons: list[str] = []
    details: dict[str, Any] = {}
    for market in markets:
        record = heartbeats.get(market.value)
        if record is None or record.last_ws_message_ms is None:
            reasons.append(f"{market.value}: no WebSocket heartbeat recorded")
            details[market.value] = {"ws_message_age_ms": None}
            continue
        age_ms = max(0, now_ms - record.last_ws_message_ms)
        details[market.value] = {
            "ws_message_age_ms": age_ms,
            "last_closed_candle_ms": record.last_closed_candle_ms,
            "last_decision_ms": record.last_decision_ms,
            "last_outbox_drain_ms": record.last_outbox_drain_ms,
            "max_loop_lag_ms": record.max_loop_lag_ms,
        }
        if age_ms > max_staleness_ms:
            reasons.append(
                f"{market.value}: WebSocket heartbeat is {age_ms} ms old "
                f"(limit {max_staleness_ms} ms)"
            )
    body: dict[str, Any] = {
        "status": "not_ready" if reasons else "ready",
        "markets": details,
    }
    if reasons:
        body["reasons"] = reasons
    return not reasons, body


def create_api(
    repository: SqlRepository,
    *,
    markets: Sequence[Market] | None = None,
    ready_max_staleness_seconds: int = 120,
    now_ms: Callable[[], int] = _wall_clock_ms,
) -> FastAPI:
    """Build the API.

    With ``markets`` the readiness probe also requires fresh scanner heartbeats;
    without it only the API process's own repository state is checked.
    """

    app = FastAPI(title="Binance Signal Bot API", version="0.1.0")

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/health/ready", response_model=None)
    def ready() -> Any:
        if markets is None:
            return {"status": "ready" if repository.ready else "not_ready"}
        ok, body = evaluate_readiness(
            repository, markets, ready_max_staleness_seconds * 1000, now_ms()
        )
        return JSONResponse(body, status_code=200 if ok else 503)

    @app.get("/outbox/summary")
    def outbox_summary() -> dict[str, Any]:
        return repository.outbox_summary(now_ms())

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
        age_ms = max(0, now_ms() - context.candle_close_time_ms)
        return {
            "status": "STALE_CONTEXT" if age_ms > max_age_ms else "OK",
            "age_ms": age_ms,
            "context": context.model_dump(mode="json"),
        }

    return app
