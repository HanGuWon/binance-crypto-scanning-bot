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
ResyncStatus = Literal["CERTAIN", "DEGRADED"]

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


@dataclass(frozen=True)
class RestResyncSession:
    """Fence identity for one authoritative REST reconciliation attempt."""

    generation: int
    start_ingest_sequence: int
    pending_event_ids: tuple[str, ...]


@dataclass(frozen=True)
class RestResyncResult:
    """Result of applying the REST/stream fence contract."""

    status: ResyncStatus
    snapshot_id: str
    snapshot_cursor: str
    reason: str | None
    crossed_event_ids: tuple[str, ...]

    @property
    def certainty_restored(self) -> bool:
        return self.status == "CERTAIN"


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
        self._ingest_sequence = 0
        self._resync_generation = 0
        self._active_resync: RestResyncSession | None = None
        self._resync_event_ids: list[str] = []
        self._resync_interference = False

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

    @property
    def resync_in_progress(self) -> bool:
        return self._active_resync is not None

    def ingest(self, event: UserStreamEvent) -> IngestResult:
        self._ingest_sequence += 1
        if self._active_resync is not None:
            self._resync_event_ids.append(event.event_id)
            self._resync_interference = True
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
            self._ingest_sequence += 1
            if self._active_resync is not None:
                self._resync_interference = True
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

    def begin_rest_resync(self) -> RestResyncSession:
        """Start a bounded REST attempt and fence all stream activity around it."""

        if self._active_resync is not None:
            raise RuntimeError("a REST resync is already in progress")
        self._resync_generation += 1
        session = RestResyncSession(
            generation=self._resync_generation,
            start_ingest_sequence=self._ingest_sequence,
            pending_event_ids=tuple(event.event_id for event in self._queue),
        )
        self._active_resync = session
        self._resync_event_ids = []
        self._resync_interference = False
        self._health = "DEGRADED"
        return session

    def complete_rest_resync(
        self,
        session: RestResyncSession,
        *,
        snapshot_id: str,
        snapshot_cursor: str,
    ) -> RestResyncResult:
        """Restore certainty only when no stream event crossed the REST fence.

        Binance REST snapshots do not provide a stream sequence number. The
        conservative contract therefore refuses to infer ordering from wall
        clock timestamps. A crossed or still-buffered event keeps the buffer
        degraded until the caller explicitly fences it and performs another
        bounded REST attempt.
        """

        self._require_active_resync(session)
        if not snapshot_id.strip() or not snapshot_cursor.strip():
            raise ValueError("authoritative REST snapshot identity must be non-empty")
        crossed = tuple(dict.fromkeys((*session.pending_event_ids, *self._resync_event_ids)))
        if (
            self._resync_interference
            or self._ingest_sequence != session.start_ingest_sequence
            or self._queue
            or crossed
        ):
            return RestResyncResult(
                status="DEGRADED",
                snapshot_id=snapshot_id,
                snapshot_cursor=snapshot_cursor,
                reason="STREAM_ACTIVITY_CROSSED_REST_FENCE",
                crossed_event_ids=crossed,
            )
        self._active_resync = None
        self._resync_event_ids = []
        self._resync_interference = False
        self._health = "HEALTHY"
        self._last_ordering.clear()
        return RestResyncResult(
            status="CERTAIN",
            snapshot_id=snapshot_id,
            snapshot_cursor=snapshot_cursor,
            reason=None,
            crossed_event_ids=(),
        )

    def fence_buffered_events(self, session: RestResyncSession) -> tuple[str, ...]:
        """Explicitly discard an unsafe interval and keep the buffer degraded."""

        self._require_active_resync(session)
        event_ids = tuple(
            dict.fromkeys(
                (
                    *session.pending_event_ids,
                    *self._resync_event_ids,
                    *(event.event_id for event in self._queue),
                )
            )
        )
        self._queue.clear()
        self._active_resync = None
        self._resync_event_ids = []
        self._resync_interference = False
        self._health = "DEGRADED"
        return event_ids

    def mark_rest_resync_complete(self) -> None:
        """Reject the historical unsafe shortcut; use begin/complete instead."""

        raise RuntimeError(
            "REST resync requires an authoritative snapshot and an explicit stream fence"
        )

    def _require_active_resync(self, session: RestResyncSession) -> None:
        if self._active_resync != session:
            raise ValueError("REST resync session is stale or belongs to another buffer")


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
