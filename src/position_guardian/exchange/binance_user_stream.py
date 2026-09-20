from __future__ import annotations

import hashlib
import json
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Literal
from urllib.parse import quote

UserStreamEventType = Literal[
    "ACCOUNT_UPDATE",
    "ORDER_TRADE_UPDATE",
    "ALGO_UPDATE",
    "ACCOUNT_CONFIG_UPDATE",
    "CONDITIONAL_ORDER_TRIGGER_REJECT",
    "MARGIN_CALL",
    "TRADE_LITE",
    "STRATEGY_UPDATE",
    "GRID_UPDATE",
    "listenKeyExpired",
]
UserStreamHealth = Literal["HEALTHY", "DEGRADED", "DISCONNECTED", "EXPIRED"]
IngestStatus = Literal["ACCEPTED", "DUPLICATE", "OUT_OF_ORDER", "OVERFLOW", "INVALID"]

PRODUCTION_USER_STREAM_BASE_URL = "wss://fstream.binance.com/private"


class UserStreamPayloadError(ValueError):
    """Raised when a recorded user-stream payload cannot be trusted."""


@dataclass(frozen=True)
class UserStreamEvent:
    event_id: str
    event_type: UserStreamEventType
    event_time_ms: int
    transaction_time_ms: int | None
    payload: Mapping[str, object]

    @property
    def ordering_key(self) -> tuple[int, int]:
        return (
            self.transaction_time_ms
            if self.transaction_time_ms is not None
            else self.event_time_ms,
            self.event_time_ms,
        )


@dataclass(frozen=True)
class IngestResult:
    status: IngestStatus
    event: UserStreamEvent | None
    health: UserStreamHealth


def build_user_stream_url(
    listen_key: str,
    *,
    base_url: str = PRODUCTION_USER_STREAM_BASE_URL,
) -> str:
    normalized_key = listen_key.strip()
    if not normalized_key:
        raise ValueError("listen_key must not be blank")
    normalized_base = base_url.rstrip("/")
    return f"{normalized_base}/ws/{quote(normalized_key, safe='')}"


def parse_user_stream_event(raw_payload: str | bytes) -> UserStreamEvent:
    try:
        decoded = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError) as exc:
        raise UserStreamPayloadError("user-stream payload is not valid JSON") from exc
    if not isinstance(decoded, dict):
        raise UserStreamPayloadError("user-stream payload root must be an object")

    event_type = decoded.get("e")
    event_time = decoded.get("E")
    transaction_time = decoded.get("T")
    supported_event_types = {
        "ACCOUNT_UPDATE",
        "ORDER_TRADE_UPDATE",
        "ALGO_UPDATE",
        "ACCOUNT_CONFIG_UPDATE",
        "CONDITIONAL_ORDER_TRIGGER_REJECT",
        "MARGIN_CALL",
        "TRADE_LITE",
        "STRATEGY_UPDATE",
        "GRID_UPDATE",
        "listenKeyExpired",
    }
    if event_type not in supported_event_types:
        raise UserStreamPayloadError("unsupported user-stream event type")
    if not isinstance(event_time, int) or event_time < 0:
        raise UserStreamPayloadError("user-stream event time must be a non-negative integer")
    if transaction_time is not None and (
        not isinstance(transaction_time, int) or transaction_time < 0
    ):
        raise UserStreamPayloadError("user-stream transaction time must be a non-negative integer")

    canonical = json.dumps(decoded, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    event_id = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return UserStreamEvent(
        event_id=event_id,
        event_type=event_type,
        event_time_ms=event_time,
        transaction_time_ms=transaction_time,
        payload=decoded,
    )


class BoundedUserEventBuffer:
    """Bounded queue and dedupe/order guard for one user-stream connection."""

    def __init__(self, *, max_queue_size: int = 1024, max_dedupe_size: int = 4096) -> None:
        if max_queue_size < 1 or max_dedupe_size < 1:
            raise ValueError("buffer limits must be positive")
        self._max_queue_size = max_queue_size
        self._max_dedupe_size = max_dedupe_size
        self._queue: deque[UserStreamEvent] = deque()
        self._dedupe_order: deque[str] = deque()
        self._dedupe_ids: set[str] = set()
        self._last_ordering: dict[UserStreamEventType, tuple[int, int]] = {}
        self._health: UserStreamHealth = "HEALTHY"

    @property
    def health(self) -> UserStreamHealth:
        return self._health

    @property
    def rest_resync_required(self) -> bool:
        return self._health != "HEALTHY"

    @property
    def pending_count(self) -> int:
        return len(self._queue)

    @property
    def dedupe_count(self) -> int:
        return len(self._dedupe_ids)

    def ingest(self, event: UserStreamEvent) -> IngestResult:
        if event.event_id in self._dedupe_ids:
            return IngestResult("DUPLICATE", event, self._health)
        last_ordering = self._last_ordering.get(event.event_type)
        if last_ordering is not None and event.ordering_key < last_ordering:
            self._health = "DEGRADED"
            return IngestResult("OUT_OF_ORDER", event, self._health)
        if len(self._queue) >= self._max_queue_size:
            self._health = "DEGRADED"
            return IngestResult("OVERFLOW", event, self._health)

        self._queue.append(event)
        self._last_ordering[event.event_type] = event.ordering_key
        self._dedupe_order.append(event.event_id)
        self._dedupe_ids.add(event.event_id)
        while len(self._dedupe_order) > self._max_dedupe_size:
            evicted = self._dedupe_order.popleft()
            self._dedupe_ids.remove(evicted)
        if event.event_type == "listenKeyExpired":
            self._health = "EXPIRED"
        return IngestResult("ACCEPTED", event, self._health)

    def ingest_raw(self, raw_payload: str | bytes) -> IngestResult:
        try:
            event = parse_user_stream_event(raw_payload)
        except UserStreamPayloadError:
            self._health = "DEGRADED"
            return IngestResult("INVALID", None, self._health)
        return self.ingest(event)

    def drain(self, *, limit: int | None = None) -> tuple[UserStreamEvent, ...]:
        if limit is not None and limit < 1:
            raise ValueError("drain limit must be positive")
        count = len(self._queue) if limit is None else min(limit, len(self._queue))
        return tuple(self._queue.popleft() for _ in range(count))

    def mark_disconnected(self) -> None:
        self._health = "DISCONNECTED"

    def mark_rest_resync_complete(self) -> None:
        self._health = "HEALTHY"
        self._last_ordering.clear()


class BoundedReconnectBackoff:
    """Finite reconnect schedule; callers own the actual sleep and connection."""

    def __init__(self, delays_seconds: Iterable[float] = (1.0, 2.0, 5.0, 10.0, 30.0)) -> None:
        delays = tuple(delays_seconds)
        if not delays or any(delay < 0 for delay in delays):
            raise ValueError("reconnect delays must be a non-empty non-negative sequence")
        self._delays = delays
        self._attempt = 0

    def next_delay(self) -> float | None:
        if self._attempt >= len(self._delays):
            return None
        delay = self._delays[self._attempt]
        self._attempt += 1
        return delay

    def reset(self) -> None:
        self._attempt = 0

    @property
    def attempts(self) -> int:
        return self._attempt
