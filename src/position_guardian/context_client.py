from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

import httpx
from pydantic import ValidationError

from signalbot.domain.enums import Market
from signalbot.signals.protection_context import ProtectionContext

SUPPORTED_PROTECTION_CONTEXT_MAJOR = 1
_VERSION_PATTERN = re.compile(r"^protection-context-v(?P<major>\d+)$")

ContextRejectionReason = Literal[
    "CONTEXT_NOT_FOUND",
    "UNSUPPORTED_MAJOR_VERSION",
    "MALFORMED_CONTEXT",
    "NOT_CLOSED_CANDLE",
    "MARKET_MISMATCH",
    "SYMBOL_MISMATCH",
    "INTERVAL_MISMATCH",
    "FUTURE_OR_UNCLOSED_CURSOR",
    "STALE_CONTEXT",
    "CURSOR_INVALID",
    "CURSOR_REGRESSION",
    "CURSOR_CONFLICT",
]


class ProtectionContextRejected(ValueError):
    """Fail-closed rejection raised before Guardian policy evaluation."""

    def __init__(self, reason: ContextRejectionReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class ClosedCandleCursor:
    """Durable consumer cursor for one immutable closed-candle context."""

    candle_close_time_ms: int
    context_id: str

    def __post_init__(self) -> None:
        if self.candle_close_time_ms < 0:
            raise ValueError("candle_close_time_ms must be non-negative")
        if not self.context_id.strip():
            raise ValueError("context_id must not be blank")

    def serialize(self) -> str:
        return json.dumps(
            {
                "candle_close_time_ms": self.candle_close_time_ms,
                "context_id": self.context_id,
            },
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def parse(cls, value: str) -> ClosedCandleCursor:
        try:
            payload = json.loads(value)
            if not isinstance(payload, dict):
                raise TypeError("cursor payload must be an object")
            close_time = payload["candle_close_time_ms"]
            context_id = payload["context_id"]
            if not isinstance(close_time, int) or isinstance(close_time, bool):
                raise TypeError("cursor close time must be an integer")
            if not isinstance(context_id, str):
                raise TypeError("cursor context_id must be a string")
            return cls(candle_close_time_ms=close_time, context_id=context_id)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ProtectionContextRejected(
                "CURSOR_INVALID",
                "persisted protection-context cursor is malformed",
            ) from exc


@dataclass(frozen=True, slots=True)
class ValidatedProtectionContext:
    """A v1 context proven usable for one Guardian shadow decision."""

    context: ProtectionContext
    cursor: ClosedCandleCursor
    age_ms: int


class ProtectionContextClient:
    """Read-only adapter for the scanner's bounded ProtectionContext API."""

    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def fetch_latest_payload(
        self,
        *,
        market: Market,
        symbol: str,
        primary_interval: str,
        max_age_ms: int,
    ) -> Mapping[str, object]:
        if max_age_ms <= 0:
            raise ValueError("max_age_ms must be positive")
        response = self._client.get(
            "/protection-contexts/latest",
            params={
                "market": market.value,
                "symbol": symbol.upper().strip(),
                "primary_interval": primary_interval,
                "max_age_ms": max_age_ms,
            },
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ProtectionContextRejected(
                "MALFORMED_CONTEXT",
                "latest protection-context response must be an object",
            )
        status = payload.get("status")
        if status == "NOT_FOUND":
            raise ProtectionContextRejected(
                "CONTEXT_NOT_FOUND",
                "no protection context is available for the requested stream",
            )
        if status == "STALE_CONTEXT":
            raise ProtectionContextRejected(
                "STALE_CONTEXT",
                "scanner reports the latest protection context as stale",
            )
        if status != "OK":
            raise ProtectionContextRejected(
                "MALFORMED_CONTEXT",
                "latest protection-context response has an unsupported status",
            )
        context = payload.get("context")
        if not isinstance(context, dict):
            raise ProtectionContextRejected(
                "MALFORMED_CONTEXT",
                "latest protection-context response is missing a context object",
            )
        return context


def validate_protection_context(
    payload: Mapping[str, object] | ProtectionContext,
    *,
    expected_market: Market,
    expected_symbol: str,
    expected_primary_interval: str,
    now_ms: int,
    max_age_ms: int,
    previous_cursor: str | None = None,
) -> ValidatedProtectionContext:
    """Validate one public-data context before any stop-policy evaluation."""

    if now_ms < 0:
        raise ValueError("now_ms must be non-negative")
    if max_age_ms <= 0:
        raise ValueError("max_age_ms must be positive")
    normalized_symbol = expected_symbol.upper().strip()
    if not normalized_symbol:
        raise ValueError("expected_symbol must not be blank")
    if not expected_primary_interval.strip():
        raise ValueError("expected_primary_interval must not be blank")

    raw = (
        payload.model_dump(mode="json")
        if isinstance(payload, ProtectionContext)
        else dict(payload)
    )
    version = raw.get("context_version")
    if not isinstance(version, str):
        raise ProtectionContextRejected(
            "UNSUPPORTED_MAJOR_VERSION",
            "context_version must be a supported version string",
        )
    version_match = _VERSION_PATTERN.fullmatch(version)
    if (
        version_match is None
        or int(version_match.group("major")) != SUPPORTED_PROTECTION_CONTEXT_MAJOR
    ):
        raise ProtectionContextRejected(
            "UNSUPPORTED_MAJOR_VERSION",
            f"unsupported protection context version: {version}",
        )
    if raw.get("source_candle_closed") is not True:
        raise ProtectionContextRejected(
            "NOT_CLOSED_CANDLE",
            "Guardian requires a fully closed source candle",
        )

    try:
        context = ProtectionContext.model_validate(raw)
    except ValidationError as exc:
        raise ProtectionContextRejected(
            "MALFORMED_CONTEXT",
            "protection context failed schema or deterministic-ID validation",
        ) from exc

    if context.market is not expected_market:
        raise ProtectionContextRejected(
            "MARKET_MISMATCH",
            "protection context market does not match managed position",
        )
    if context.symbol != normalized_symbol:
        raise ProtectionContextRejected(
            "SYMBOL_MISMATCH",
            "protection context symbol does not match managed position",
        )
    if context.primary_interval != expected_primary_interval:
        raise ProtectionContextRejected(
            "INTERVAL_MISMATCH",
            "protection context primary interval does not match consumer stream",
        )
    if context.candle_close_time_ms >= now_ms:
        raise ProtectionContextRejected(
            "FUTURE_OR_UNCLOSED_CURSOR",
            "closed-candle cursor must be strictly before the consumer clock",
        )

    age_ms = max(
        now_ms - context.candle_close_time_ms,
        context.context_freshness_ms,
    )
    if age_ms > max_age_ms:
        raise ProtectionContextRejected(
            "STALE_CONTEXT",
            "protection context exceeds the configured freshness boundary",
        )

    cursor = ClosedCandleCursor(
        candle_close_time_ms=context.candle_close_time_ms,
        context_id=context.context_id,
    )
    if previous_cursor is not None:
        previous = ClosedCandleCursor.parse(previous_cursor)
        if cursor.candle_close_time_ms < previous.candle_close_time_ms:
            raise ProtectionContextRejected(
                "CURSOR_REGRESSION",
                "protection-context closed-candle cursor regressed",
            )
        if (
            cursor.candle_close_time_ms == previous.candle_close_time_ms
            and cursor.context_id != previous.context_id
        ):
            raise ProtectionContextRejected(
                "CURSOR_CONFLICT",
                "one closed-candle cursor is bound to multiple context IDs",
            )

    return ValidatedProtectionContext(context=context, cursor=cursor, age_ms=age_ms)
