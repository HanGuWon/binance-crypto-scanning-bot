from __future__ import annotations

import binascii
import json
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Final, TypeAlias, cast

from signalbot.capture.config import CANARY_SYMBOLS
from signalbot.capture.depth_sequence import (
    DepthSequenceError,
    classify_depth_snapshot_bridge,
)
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CaptureRecord,
    ConnectionState,
    ConnectionTransitionV1,
    RestEnvelopeV2,
    payload_bytes,
)
from signalbot.domain.enums import Market

_DEPTH_SUFFIX: Final = "@depth@100ms"
_DEPTH_ROLES: Final = frozenset({"spot_depth_snapshot", "futures_depth_snapshot"})
_DEFAULT_MAX_BUFFERED_EVENTS_PER_BOOK: Final = 50_000
_DEFAULT_MAX_BUFFERED_BYTES_PER_BOOK: Final = 67_108_864
_DEFAULT_MAX_BUFFERED_LEVEL_CHANGES_PER_BOOK: Final = 262_144
_DEFAULT_MAX_LEVELS_PER_SIDE: Final = 10_000
_DEFAULT_MAX_LEVEL_CHANGES_PER_EVENT: Final = 10_000
_DEFAULT_FEATURE_BAND_BPS: Final = 10
_DEFAULT_GUARD_BAND_BPS: Final = 20
_BPS_DENOMINATOR: Final = Decimal(10_000)

PriceLevel: TypeAlias = tuple[Decimal, Decimal]  # noqa: UP040 - host may be Python 3.11
_BookKey: TypeAlias = tuple[Market, str]  # noqa: UP040 - host may be Python 3.11


class LocalBookReplayError(RuntimeError):
    """The persisted record sequence cannot support deterministic replay."""


class LocalBookStatus(StrEnum):
    DISCONNECTED = "disconnected"
    AWAITING_SNAPSHOT = "awaiting_snapshot"
    VALID = "valid"
    REPLAY_FAILED = "replay_failed"


class LocalBookReason(StrEnum):
    NOT_CONNECTED = "not_connected"
    CONNECTING = "connecting"
    CONNECTED_AWAITING_SNAPSHOT = "connected_awaiting_snapshot"
    SNAPSHOT_PENDING_FIRST_EVENT = "snapshot_pending_first_event"
    SNAPSHOT_REQUEST_FAILED = "snapshot_request_failed"
    STALE_SNAPSHOT = "stale_snapshot"
    CONNECTION_PROTOCOL_ERROR = "connection_protocol_error"
    MALFORMED_EVENT = "malformed_event"
    MALFORMED_SNAPSHOT = "malformed_snapshot"
    BRIDGE_FAILURE = "bridge_failure"
    SEQUENCE_GAP = "sequence_gap"
    BUFFER_OVERFLOW = "buffer_overflow"
    LEVEL_OVERFLOW = "level_overflow"
    DISCONNECTED = "disconnected"
    RECYCLED = "recycled"
    VALID = "valid"
    INGEST_GAP = "ingest_gap"


@dataclass(frozen=True, slots=True)
class LocalBookView:
    """Immutable current view; replay retains no historical observations."""

    market: Market
    symbol: str
    status: LocalBookStatus
    reason: LocalBookReason
    connection_id: str | None
    generation: int
    update_id: int | None
    availability_receipt_at_ms: int | None
    availability_receipt_monotonic_ns: int | None
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]
    best_bid: PriceLevel | None
    best_ask: PriceLevel | None
    crossed: bool
    buffered_event_count: int
    buffered_bytes: int
    buffered_level_changes: int
    pending_snapshot_update_id: int | None
    feature_band_bps: int
    guard_band_bps: int
    retained_level_capacity_per_side: int
    buffered_level_change_capacity: int
    bid_feature_band_complete: bool
    ask_feature_band_complete: bool
    bid_retained_floor: Decimal | None
    ask_retained_ceiling: Decimal | None
    resnapshot_required: bool
    gap_reason: LocalBookReason | None
    gap_first_update_id: int | None
    gap_last_update_id: int | None
    gap_first_receipt_monotonic_ns: int | None
    gap_last_receipt_monotonic_ns: int | None

    @property
    def valid(self) -> bool:
        return self.status is LocalBookStatus.VALID

    @property
    def awaiting_snapshot(self) -> bool:
        return self.status is LocalBookStatus.AWAITING_SNAPSHOT

    @property
    def feature_band_complete(self) -> bool:
        return self.bid_feature_band_complete and self.ask_feature_band_complete


@dataclass(frozen=True, slots=True)
class LocalBookCoverageState:
    """No-sort book state for bounded coverage aggregation."""

    market: Market
    symbol: str
    status: LocalBookStatus
    reason: LocalBookReason
    generation: int
    availability_receipt_monotonic_ns: int | None
    bid_level_count: int
    ask_level_count: int
    has_bid: bool
    has_ask: bool
    crossed_or_locked: bool
    unresolved_sequence_gap: bool
    unresolved_reconstruction: bool
    bid_feature_band_complete: bool = True
    ask_feature_band_complete: bool = True
    resnapshot_required: bool = False
    gap_reason: LocalBookReason | None = None

    @property
    def sequence_valid(self) -> bool:
        return self.status is LocalBookStatus.VALID

    @property
    def two_sided_uncrossed(self) -> bool:
        return (
            self.sequence_valid
            and self.bid_feature_band_complete
            and self.ask_feature_band_complete
            and self.has_bid
            and self.has_ask
            and not self.crossed_or_locked
        )


@dataclass(frozen=True, slots=True)
class LocalBookProcessResult:
    """Current states for only the fixed books touched by one record."""

    affected_books: tuple[LocalBookCoverageState, ...]


@dataclass(frozen=True, slots=True)
class LocalBookMetrics:
    records_processed: int
    depth_transitions: int
    connection_resets: int
    disconnects: int
    depth_events_received: int
    depth_events_buffered: int
    depth_events_applied: int
    old_events_ignored: int
    snapshots_received: int
    snapshot_candidates_held: int
    snapshots_applied: int
    snapshot_request_failures: int
    stale_snapshots_rejected: int
    redundant_snapshots_ignored: int
    bridge_failures: int
    sequence_gaps: int
    malformed_records: int
    buffer_overflows: int
    level_overflows: int
    stale_connection_records: int
    books_became_valid: int
    ingest_gaps: int


@dataclass(frozen=True, slots=True)
class LocalBookPerBookMetrics:
    market: Market
    symbol: str
    depth_transitions: int
    connection_resets: int
    disconnects: int
    depth_events_received: int
    depth_events_buffered: int
    depth_events_applied: int
    old_events_ignored: int
    snapshots_received: int
    snapshot_candidates_held: int
    snapshots_applied: int
    snapshot_request_failures: int
    stale_snapshots_rejected: int
    redundant_snapshots_ignored: int
    bridge_failures: int
    sequence_gaps: int
    malformed_records: int
    buffer_overflows: int
    level_overflows: int
    stale_connection_records: int
    books_became_valid: int


@dataclass(slots=True)
class _MutablePerBookMetrics:
    depth_transitions: int = 0
    connection_resets: int = 0
    disconnects: int = 0
    depth_events_received: int = 0
    depth_events_buffered: int = 0
    depth_events_applied: int = 0
    old_events_ignored: int = 0
    snapshots_received: int = 0
    snapshot_candidates_held: int = 0
    snapshots_applied: int = 0
    snapshot_request_failures: int = 0
    stale_snapshots_rejected: int = 0
    redundant_snapshots_ignored: int = 0
    bridge_failures: int = 0
    sequence_gaps: int = 0
    malformed_records: int = 0
    buffer_overflows: int = 0
    level_overflows: int = 0
    stale_connection_records: int = 0
    books_became_valid: int = 0

    def frozen(self, market: Market, symbol: str) -> LocalBookPerBookMetrics:
        return LocalBookPerBookMetrics(
            market=market,
            symbol=symbol,
            depth_transitions=self.depth_transitions,
            connection_resets=self.connection_resets,
            disconnects=self.disconnects,
            depth_events_received=self.depth_events_received,
            depth_events_buffered=self.depth_events_buffered,
            depth_events_applied=self.depth_events_applied,
            old_events_ignored=self.old_events_ignored,
            snapshots_received=self.snapshots_received,
            snapshot_candidates_held=self.snapshot_candidates_held,
            snapshots_applied=self.snapshots_applied,
            snapshot_request_failures=self.snapshot_request_failures,
            stale_snapshots_rejected=self.stale_snapshots_rejected,
            redundant_snapshots_ignored=self.redundant_snapshots_ignored,
            bridge_failures=self.bridge_failures,
            sequence_gaps=self.sequence_gaps,
            malformed_records=self.malformed_records,
            buffer_overflows=self.buffer_overflows,
            level_overflows=self.level_overflows,
            stale_connection_records=self.stale_connection_records,
            books_became_valid=self.books_became_valid,
        )


@dataclass(slots=True)
class _MutableMetrics:
    records_processed: int = 0
    depth_transitions: int = 0
    connection_resets: int = 0
    disconnects: int = 0
    depth_events_received: int = 0
    depth_events_buffered: int = 0
    depth_events_applied: int = 0
    old_events_ignored: int = 0
    snapshots_received: int = 0
    snapshot_candidates_held: int = 0
    snapshots_applied: int = 0
    snapshot_request_failures: int = 0
    stale_snapshots_rejected: int = 0
    redundant_snapshots_ignored: int = 0
    bridge_failures: int = 0
    sequence_gaps: int = 0
    malformed_records: int = 0
    buffer_overflows: int = 0
    level_overflows: int = 0
    stale_connection_records: int = 0
    books_became_valid: int = 0
    ingest_gaps: int = 0

    def frozen(self) -> LocalBookMetrics:
        return LocalBookMetrics(
            records_processed=self.records_processed,
            depth_transitions=self.depth_transitions,
            connection_resets=self.connection_resets,
            disconnects=self.disconnects,
            depth_events_received=self.depth_events_received,
            depth_events_buffered=self.depth_events_buffered,
            depth_events_applied=self.depth_events_applied,
            old_events_ignored=self.old_events_ignored,
            snapshots_received=self.snapshots_received,
            snapshot_candidates_held=self.snapshot_candidates_held,
            snapshots_applied=self.snapshots_applied,
            snapshot_request_failures=self.snapshot_request_failures,
            stale_snapshots_rejected=self.stale_snapshots_rejected,
            redundant_snapshots_ignored=self.redundant_snapshots_ignored,
            bridge_failures=self.bridge_failures,
            sequence_gaps=self.sequence_gaps,
            malformed_records=self.malformed_records,
            buffer_overflows=self.buffer_overflows,
            level_overflows=self.level_overflows,
            stale_connection_records=self.stale_connection_records,
            books_became_valid=self.books_became_valid,
            ingest_gaps=self.ingest_gaps,
        )


@dataclass(frozen=True, slots=True)
class _DepthEvent:
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int | None
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]
    received_at_ms: int
    received_monotonic_ns: int
    raw_size_bytes: int

    @property
    def level_change_count(self) -> int:
        return len(self.bids) + len(self.asks)


@dataclass(frozen=True, slots=True)
class _Snapshot:
    update_id: int
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    response_completed_at_ms: int
    response_completed_monotonic_ns: int
    bid_retained_floor: Decimal | None
    ask_retained_ceiling: Decimal | None
    bid_feature_band_complete: bool
    ask_feature_band_complete: bool


@dataclass(frozen=True, slots=True)
class _BoundedSnapshotSide:
    levels: dict[Decimal, Decimal]
    retained_boundary: Decimal | None


@dataclass(frozen=True, slots=True)
class _SideUpdatePlan:
    changes: tuple[PriceLevel, ...]
    retained_boundary: Decimal | None
    best_price: Decimal | None


@dataclass(frozen=True, slots=True)
class _BandApplyResult:
    best_bid_price: Decimal | None
    best_ask_price: Decimal | None
    bid_retained_floor: Decimal | None
    ask_retained_ceiling: Decimal | None


@dataclass(slots=True)
class _BookState:
    market: Market
    symbol: str
    status: LocalBookStatus = LocalBookStatus.DISCONNECTED
    reason: LocalBookReason = LocalBookReason.NOT_CONNECTED
    connection_id: str | None = None
    connected: bool = False
    generation: int = 0
    snapshot_min_started_monotonic_ns: int = 0
    update_id: int | None = None
    availability_receipt_at_ms: int | None = None
    availability_receipt_monotonic_ns: int | None = None
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    best_bid_price: Decimal | None = None
    best_ask_price: Decimal | None = None
    buffer: list[_DepthEvent] = field(default_factory=list)
    buffered_bytes: int = 0
    buffered_level_changes: int = 0
    pending_snapshot: _Snapshot | None = None
    bid_retained_floor: Decimal | None = None
    ask_retained_ceiling: Decimal | None = None
    bid_feature_band_complete: bool = False
    ask_feature_band_complete: bool = False
    resnapshot_required: bool = False
    gap_reason: LocalBookReason | None = None
    gap_first_update_id: int | None = None
    gap_last_update_id: int | None = None
    gap_first_receipt_monotonic_ns: int | None = None
    gap_last_receipt_monotonic_ns: int | None = None
    gap_generation: int = 0
    unresolved_sequence_gap: bool = False
    unresolved_reconstruction: bool = False
    metrics: _MutablePerBookMetrics = field(default_factory=_MutablePerBookMetrics)


class _DepthDataError(ValueError):
    pass


class _LevelBoundError(_DepthDataError):
    pass


class LocalBookMaterializer:
    """Reconstruct the six canary books from raw persisted records.

    Records must be supplied in contiguous global ``ingest_seq`` order.  This is
    deliberately an offline, raw-first acceptance boundary; the live raw depth
    monitor remains an operational resnapshot trigger and is not authoritative
    for local-book admission.  Its decoded reconstruction buffer is only a
    bounded window over those already-durable raw records.  If that window is
    exhausted, an explicit gap range remains until a fresh snapshot bridges it.

    The retained book is not a full-depth claim.  It keeps the qualified feature
    band plus a wider resnapshot guard.  Far levels are discarded deterministically;
    a capacity breach inside that band or movement through the retained frontier
    invalidates the book and requires a fresh snapshot instead of raising an
    arbitrary whole-book level cap.

    Spot bootstrapping follows the frozen DESIGN reconciliation of Binance's
    contradictory bootstrap wording: after dropping buffered events with
    ``u <= lastUpdateId``, the first remaining event must contain
    ``lastUpdateId + 1`` in its inclusive ``[U, u]`` range.  Spot never receives
    or synthesizes ``pu``.  USD-M Futures continues to bridge on
    ``lastUpdateId`` and uses ``U/u/pu`` plus the official
    ``pu == previous u`` continuation rule.
    """

    def __init__(
        self,
        *,
        max_buffered_events_per_book: int = _DEFAULT_MAX_BUFFERED_EVENTS_PER_BOOK,
        max_buffered_bytes_per_book: int = _DEFAULT_MAX_BUFFERED_BYTES_PER_BOOK,
        max_buffered_level_changes_per_book: int = (
            _DEFAULT_MAX_BUFFERED_LEVEL_CHANGES_PER_BOOK
        ),
        max_levels_per_side: int = _DEFAULT_MAX_LEVELS_PER_SIDE,
        max_level_changes_per_event: int = _DEFAULT_MAX_LEVEL_CHANGES_PER_EVENT,
        feature_band_bps: int = _DEFAULT_FEATURE_BAND_BPS,
        guard_band_bps: int = _DEFAULT_GUARD_BAND_BPS,
    ) -> None:
        self._max_buffered_events = _positive_int(
            max_buffered_events_per_book, "max_buffered_events_per_book"
        )
        self._max_buffered_bytes = _positive_int(
            max_buffered_bytes_per_book, "max_buffered_bytes_per_book"
        )
        self._max_buffered_level_changes = _positive_int(
            max_buffered_level_changes_per_book,
            "max_buffered_level_changes_per_book",
        )
        self._max_levels = _positive_int(max_levels_per_side, "max_levels_per_side")
        self._max_level_changes_per_event = _positive_int(
            max_level_changes_per_event,
            "max_level_changes_per_event",
        )
        self._feature_band_bps = _positive_int(
            feature_band_bps,
            "feature_band_bps",
        )
        self._guard_band_bps = _positive_int(
            guard_band_bps,
            "guard_band_bps",
        )
        if self._feature_band_bps >= self._guard_band_bps:
            raise ValueError("guard_band_bps must be strictly wider than feature_band_bps")
        if self._guard_band_bps >= 10_000:
            raise ValueError("guard_band_bps must be less than 10000")
        self._states: dict[_BookKey, _BookState] = {
            (market, symbol): _BookState(market=market, symbol=symbol)
            for market in (Market.SPOT, Market.FUTURES)
            for symbol in CANARY_SYMBOLS
        }
        self._metrics = _MutableMetrics()
        self._last_ingest_seq: int | None = None
        self._replay_failed = False

    @property
    def metrics(self) -> LocalBookMetrics:
        return self._metrics.frozen()

    @property
    def views(self) -> tuple[LocalBookView, ...]:
        return tuple(
            self.view(market, symbol)
            for market in (Market.SPOT, Market.FUTURES)
            for symbol in CANARY_SYMBOLS
        )

    @property
    def coverage_states(self) -> tuple[LocalBookCoverageState, ...]:
        return tuple(
            self.coverage_state(market, symbol)
            for market in (Market.SPOT, Market.FUTURES)
            for symbol in CANARY_SYMBOLS
        )

    @property
    def per_book_metrics(self) -> tuple[LocalBookPerBookMetrics, ...]:
        return tuple(
            self.book_metrics(market, symbol)
            for market in (Market.SPOT, Market.FUTURES)
            for symbol in CANARY_SYMBOLS
        )

    def coverage_state(self, market: Market, symbol: str) -> LocalBookCoverageState:
        state = self._require_state(market, symbol)
        best_bid = state.best_bid_price
        best_ask = state.best_ask_price
        return LocalBookCoverageState(
            market=state.market,
            symbol=state.symbol,
            status=state.status,
            reason=state.reason,
            generation=state.generation,
            availability_receipt_monotonic_ns=(
                state.availability_receipt_monotonic_ns
            ),
            bid_level_count=len(state.bids),
            ask_level_count=len(state.asks),
            has_bid=best_bid is not None,
            has_ask=best_ask is not None,
            crossed_or_locked=(
                best_bid is not None and best_ask is not None and best_bid >= best_ask
            ),
            unresolved_sequence_gap=state.unresolved_sequence_gap,
            unresolved_reconstruction=state.unresolved_reconstruction,
            bid_feature_band_complete=state.bid_feature_band_complete,
            ask_feature_band_complete=state.ask_feature_band_complete,
            resnapshot_required=state.resnapshot_required,
            gap_reason=state.gap_reason,
        )

    def book_metrics(self, market: Market, symbol: str) -> LocalBookPerBookMetrics:
        state = self._require_state(market, symbol)
        return state.metrics.frozen(state.market, state.symbol)

    def view(self, market: Market, symbol: str) -> LocalBookView:
        state = self._require_state(market, symbol)
        bids = tuple(sorted(state.bids.items(), reverse=True))
        asks = tuple(sorted(state.asks.items()))
        best_bid = (
            None
            if state.best_bid_price is None
            else (state.best_bid_price, state.bids[state.best_bid_price])
        )
        best_ask = (
            None
            if state.best_ask_price is None
            else (state.best_ask_price, state.asks[state.best_ask_price])
        )
        crossed = (
            best_bid is not None and best_ask is not None and best_bid[0] >= best_ask[0]
        )
        pending_id = (
            state.pending_snapshot.update_id if state.pending_snapshot is not None else None
        )
        return LocalBookView(
            market=state.market,
            symbol=state.symbol,
            status=state.status,
            reason=state.reason,
            connection_id=state.connection_id,
            generation=state.generation,
            update_id=state.update_id,
            availability_receipt_at_ms=state.availability_receipt_at_ms,
            availability_receipt_monotonic_ns=state.availability_receipt_monotonic_ns,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            crossed=crossed,
            buffered_event_count=len(state.buffer),
            buffered_bytes=state.buffered_bytes,
            buffered_level_changes=state.buffered_level_changes,
            pending_snapshot_update_id=pending_id,
            feature_band_bps=self._feature_band_bps,
            guard_band_bps=self._guard_band_bps,
            retained_level_capacity_per_side=self._max_levels,
            buffered_level_change_capacity=self._max_buffered_level_changes,
            bid_feature_band_complete=state.bid_feature_band_complete,
            ask_feature_band_complete=state.ask_feature_band_complete,
            bid_retained_floor=state.bid_retained_floor,
            ask_retained_ceiling=state.ask_retained_ceiling,
            resnapshot_required=state.resnapshot_required,
            gap_reason=state.gap_reason,
            gap_first_update_id=state.gap_first_update_id,
            gap_last_update_id=state.gap_last_update_id,
            gap_first_receipt_monotonic_ns=(
                state.gap_first_receipt_monotonic_ns
            ),
            gap_last_receipt_monotonic_ns=state.gap_last_receipt_monotonic_ns,
        )

    def process(self, record: CaptureRecord) -> LocalBookProcessResult:
        """Admit exactly one persisted record in contiguous ingest order."""

        if self._replay_failed:
            raise LocalBookReplayError("local-book replay already failed")
        if self._last_ingest_seq is None and record.ingest_seq != 1:
            self._fail_ingest_sequence(record.ingest_seq)
        if self._last_ingest_seq is not None and record.ingest_seq != self._last_ingest_seq + 1:
            self._fail_ingest_sequence(record.ingest_seq)
        self._last_ingest_seq = record.ingest_seq
        self._metrics.records_processed += 1
        if isinstance(record, ConnectionTransitionV1):
            affected = self._process_transition(record)
        elif isinstance(record, CaptureEnvelopeV1):
            affected = self._process_capture(record)
        elif isinstance(record, RestEnvelopeV2):
            affected = self._process_snapshot(record)
        else:
            affected = ()
        return LocalBookProcessResult(
            affected_books=tuple(self.coverage_state(*key) for key in affected)
        )

    def _require_state(self, market: Market, symbol: str) -> _BookState:
        if not isinstance(market, Market):
            raise ValueError("market must be a supported Market")
        state = self._states.get((market, symbol))
        if state is None:
            raise ValueError("symbol must be one of the fixed canary symbols")
        return state

    def _fail_ingest_sequence(self, received: int) -> None:
        expected = 1 if self._last_ingest_seq is None else cast(int, self._last_ingest_seq) + 1
        self._metrics.ingest_gaps += 1
        self._replay_failed = True
        for state in self._states.values():
            self._clear_reconstruction(state)
            state.connected = False
            state.status = LocalBookStatus.REPLAY_FAILED
            state.reason = LocalBookReason.INGEST_GAP
        raise LocalBookReplayError(
            f"ingest_seq is not contiguous: expected {expected}, received {received}"
        )

    def _process_transition(
        self, record: ConnectionTransitionV1
    ) -> tuple[_BookKey, ...]:
        keys, malformed = _depth_keys(record.market, record.streams)
        if malformed:
            self._metrics.malformed_records += 1
            for key in keys:
                self._states[key].metrics.malformed_records += 1
        if not keys:
            return ()
        self._metrics.depth_transitions += 1
        for key in keys:
            state = self._states[key]
            state.metrics.depth_transitions += 1
            if record.state is ConnectionState.CONNECTED:
                if state.connected and state.connection_id == record.connection_id:
                    self._metrics.malformed_records += 1
                    state.metrics.malformed_records += 1
                    self._invalidate(
                        state,
                        LocalBookReason.CONNECTION_PROTOCOL_ERROR,
                        record.received_monotonic_ns,
                    )
                    continue
                self._clear_reconstruction(state)
                state.connection_id = record.connection_id
                state.connected = True
                state.generation += 1
                state.unresolved_reconstruction = True
                state.resnapshot_required = True
                state.snapshot_min_started_monotonic_ns = record.received_monotonic_ns
                state.status = LocalBookStatus.AWAITING_SNAPSHOT
                state.reason = LocalBookReason.CONNECTED_AWAITING_SNAPSHOT
                self._metrics.connection_resets += 1
                state.metrics.connection_resets += 1
                continue
            if state.connection_id is not None and state.connection_id != record.connection_id:
                self._metrics.stale_connection_records += 1
                state.metrics.stale_connection_records += 1
                continue
            self._clear_reconstruction(state)
            state.connection_id = record.connection_id
            state.connected = False
            state.resnapshot_required = False
            state.snapshot_min_started_monotonic_ns = max(
                state.snapshot_min_started_monotonic_ns,
                record.received_monotonic_ns,
            )
            state.status = LocalBookStatus.DISCONNECTED
            if record.state is ConnectionState.RECYCLED:
                state.reason = LocalBookReason.RECYCLED
                state.unresolved_reconstruction = True
            elif record.state is ConnectionState.CONNECTING:
                state.reason = LocalBookReason.CONNECTING
                state.unresolved_reconstruction = True
            else:
                state.reason = LocalBookReason.DISCONNECTED
                if record.reason != "owner_stop":
                    state.unresolved_reconstruction = True
            if record.state in (ConnectionState.DISCONNECTED, ConnectionState.RECYCLED):
                self._metrics.disconnects += 1
                state.metrics.disconnects += 1
        return keys

    def _process_capture(self, record: CaptureEnvelopeV1) -> tuple[_BookKey, ...]:
        target_keys, malformed_streams = _depth_keys(record.market, record.subscription_streams)
        reject_keys = target_keys
        if not target_keys:
            if malformed_streams:
                self._metrics.malformed_records += 1
            return ()
        if malformed_streams or not _depth_route_is_valid(record.market, record.route):
            self._reject_capture(record, target_keys)
            return target_keys
        try:
            raw, payload = _decode_json_payload(record)
            stream = payload.get("stream")
            data = payload.get("data")
            if not isinstance(stream, str) or stream not in record.subscription_streams:
                raise _DepthDataError("combined frame has an unauthorized stream")
            if not record.stream.startswith("combined:") and record.stream != stream:
                raise _DepthDataError("envelope stream differs from combined frame")
            key = _depth_key(record.market, stream)
            if key is None:
                if stream.endswith(_DEPTH_SUFFIX):
                    raise _DepthDataError("combined frame has an unsupported depth stream")
                return ()
            reject_keys = (key,)
            state = self._states[key]
            if not state.connected or state.connection_id != record.connection_id:
                self._metrics.stale_connection_records += 1
                state.metrics.stale_connection_records += 1
                return (key,)
            self._metrics.depth_events_received += 1
            state.metrics.depth_events_received += 1
            event = _parse_depth_event(
                market=record.market,
                symbol=state.symbol,
                data=data,
                received_at_ms=record.received_at_ms,
                received_monotonic_ns=record.received_monotonic_ns,
                raw_size_bytes=len(raw),
                max_levels=self._max_level_changes_per_event,
            )
        except _LevelBoundError:
            self._metrics.level_overflows += 1
            for key in reject_keys:
                self._states[key].metrics.level_overflows += 1
            self._reject_capture(record, reject_keys, LocalBookReason.LEVEL_OVERFLOW)
            return reject_keys
        except (_DepthDataError, UnicodeDecodeError, json.JSONDecodeError, binascii.Error):
            self._reject_capture(record, reject_keys)
            return reject_keys
        self._accept_depth_event(state, event)
        return (key,)

    def _reject_capture(
        self,
        record: CaptureEnvelopeV1,
        target_keys: tuple[_BookKey, ...],
        reason: LocalBookReason = LocalBookReason.MALFORMED_EVENT,
    ) -> None:
        self._metrics.malformed_records += 1
        for key in target_keys:
            state = self._states[key]
            state.metrics.malformed_records += 1
            if state.connected and state.connection_id == record.connection_id:
                self._record_gap(
                    state,
                    reason,
                    first_update_id=None,
                    last_update_id=None,
                    first_receipt_monotonic_ns=record.received_monotonic_ns,
                    last_receipt_monotonic_ns=record.received_monotonic_ns,
                )
                self._invalidate(state, reason, record.received_monotonic_ns)

    def _accept_depth_event(self, state: _BookState, event: _DepthEvent) -> None:
        if state.status is LocalBookStatus.VALID:
            self._apply_live_event(state, event)
            return
        if state.status is not LocalBookStatus.AWAITING_SNAPSHOT:
            self._metrics.stale_connection_records += 1
            state.metrics.stale_connection_records += 1
            return
        if not self._append_buffer(state, event):
            return
        if state.pending_snapshot is not None:
            self._attempt_bridge(state)

    def _append_buffer(self, state: _BookState, event: _DepthEvent) -> bool:
        if (
            len(state.buffer) >= self._max_buffered_events
            or state.buffered_bytes + event.raw_size_bytes > self._max_buffered_bytes
            or state.buffered_level_changes + event.level_change_count
            > self._max_buffered_level_changes
        ):
            self._metrics.buffer_overflows += 1
            state.metrics.buffer_overflows += 1
            first = state.buffer[0] if state.buffer else event
            self._record_gap(
                state,
                LocalBookReason.BUFFER_OVERFLOW,
                first_update_id=first.first_update_id,
                last_update_id=event.final_update_id,
                first_receipt_monotonic_ns=first.received_monotonic_ns,
                last_receipt_monotonic_ns=event.received_monotonic_ns,
            )
            self._invalidate(
                state,
                LocalBookReason.BUFFER_OVERFLOW,
                event.received_monotonic_ns,
            )
            return False
        state.buffer.append(event)
        state.buffered_bytes += event.raw_size_bytes
        state.buffered_level_changes += event.level_change_count
        self._metrics.depth_events_buffered += 1
        state.metrics.depth_events_buffered += 1
        return True

    def _process_snapshot(self, record: RestEnvelopeV2) -> tuple[_BookKey, ...]:
        if record.request_role not in _DEPTH_ROLES:
            return ()
        self._metrics.snapshots_received += 1
        try:
            symbol = _snapshot_query_symbol(record)
            key = (record.market, symbol)
            state = self._states.get(key)
            if state is None:
                raise _DepthDataError("snapshot names an unsupported symbol")
        except _DepthDataError:
            self._metrics.malformed_records += 1
            return ()
        state.metrics.snapshots_received += 1
        try:
            _validate_snapshot_identity(record)
        except _DepthDataError:
            self._metrics.malformed_records += 1
            state.metrics.malformed_records += 1
            return (key,)
        if not state.connected:
            self._metrics.stale_snapshots_rejected += 1
            state.metrics.stale_snapshots_rejected += 1
            return (key,)
        if record.request_started_monotonic_ns < state.snapshot_min_started_monotonic_ns:
            self._metrics.stale_snapshots_rejected += 1
            state.metrics.stale_snapshots_rejected += 1
            if state.status is LocalBookStatus.AWAITING_SNAPSHOT:
                state.reason = LocalBookReason.STALE_SNAPSHOT
            return (key,)
        if state.status is LocalBookStatus.VALID:
            self._metrics.redundant_snapshots_ignored += 1
            state.metrics.redundant_snapshots_ignored += 1
            return (key,)
        if not _successful_snapshot_response(record):
            self._metrics.snapshot_request_failures += 1
            state.metrics.snapshot_request_failures += 1
            state.reason = LocalBookReason.SNAPSHOT_REQUEST_FAILED
            return (key,)
        try:
            _raw, payload = _decode_json_payload(record)
            snapshot = _parse_snapshot(
                payload,
                response_completed_at_ms=record.response_completed_at_ms,
                response_completed_monotonic_ns=record.response_completed_monotonic_ns,
                source_limit=_venue_snapshot_limit(record.market),
                maximum_retained_levels=self._max_levels,
                feature_band_bps=self._feature_band_bps,
                guard_band_bps=self._guard_band_bps,
            )
        except _LevelBoundError:
            self._metrics.level_overflows += 1
            self._metrics.malformed_records += 1
            state.metrics.level_overflows += 1
            state.metrics.malformed_records += 1
            state.pending_snapshot = None
            state.reason = LocalBookReason.LEVEL_OVERFLOW
            state.resnapshot_required = True
            state.snapshot_min_started_monotonic_ns = max(
                state.snapshot_min_started_monotonic_ns,
                record.response_completed_monotonic_ns,
            )
            first = state.buffer[0] if state.buffer else None
            last = state.buffer[-1] if state.buffer else None
            self._record_gap(
                state,
                LocalBookReason.LEVEL_OVERFLOW,
                first_update_id=(first.first_update_id if first is not None else None),
                last_update_id=(last.final_update_id if last is not None else None),
                first_receipt_monotonic_ns=(
                    first.received_monotonic_ns
                    if first is not None
                    else record.response_completed_monotonic_ns
                ),
                last_receipt_monotonic_ns=(
                    last.received_monotonic_ns
                    if last is not None
                    else record.response_completed_monotonic_ns
                ),
            )
            return (key,)
        except (_DepthDataError, UnicodeDecodeError, json.JSONDecodeError, binascii.Error):
            self._metrics.malformed_records += 1
            state.metrics.malformed_records += 1
            state.pending_snapshot = None
            state.reason = LocalBookReason.MALFORMED_SNAPSHOT
            state.resnapshot_required = True
            return (key,)
        state.pending_snapshot = snapshot
        state.reason = LocalBookReason.SNAPSHOT_PENDING_FIRST_EVENT
        self._metrics.snapshot_candidates_held += 1
        state.metrics.snapshot_candidates_held += 1
        self._attempt_bridge(state)
        return (key,)

    def _attempt_bridge(self, state: _BookState) -> None:
        snapshot = state.pending_snapshot
        if snapshot is None:
            return
        try:
            decision = classify_depth_snapshot_bridge(
                state.market,
                snapshot.update_id,
                tuple(
                    (event.first_update_id, event.final_update_id)
                    for event in state.buffer
                ),
            )
        except DepthSequenceError:
            self._metrics.bridge_failures += 1
            state.metrics.bridge_failures += 1
            self._install_snapshot(state, snapshot)
            first_receipt_ns = (
                state.buffer[0].received_monotonic_ns
                if state.buffer
                else snapshot.response_completed_monotonic_ns
            )
            first = state.buffer[0] if state.buffer else None
            last = state.buffer[-1] if state.buffer else None
            self._record_gap(
                state,
                LocalBookReason.BRIDGE_FAILURE,
                first_update_id=(first.first_update_id if first is not None else None),
                last_update_id=(last.final_update_id if last is not None else None),
                first_receipt_monotonic_ns=first_receipt_ns,
                last_receipt_monotonic_ns=max(
                    last.received_monotonic_ns
                    if last is not None
                    else first_receipt_ns,
                    snapshot.response_completed_monotonic_ns,
                ),
            )
            self._invalidate(
                state,
                LocalBookReason.BRIDGE_FAILURE,
                max(first_receipt_ns, snapshot.response_completed_monotonic_ns),
            )
            return
        if decision.status == "waiting":
            self._metrics.old_events_ignored += decision.discarded_range_count
            state.metrics.old_events_ignored += decision.discarded_range_count
            state.buffer.clear()
            state.buffered_bytes = 0
            state.buffered_level_changes = 0
            state.reason = LocalBookReason.SNAPSHOT_PENDING_FIRST_EVENT
            return
        if decision.status == "stale":
            # Reject only this stale candidate.  The original buffer (including
            # events this candidate would have called old) may bridge a later
            # scheduler retry, including one already in flight.
            self._metrics.stale_snapshots_rejected += 1
            state.metrics.stale_snapshots_rejected += 1
            state.pending_snapshot = None
            state.reason = LocalBookReason.STALE_SNAPSHOT
            return
        self._metrics.old_events_ignored += decision.discarded_range_count
        state.metrics.old_events_ignored += decision.discarded_range_count
        buffered = tuple(state.buffer)[decision.discarded_range_count :]
        first = buffered[0]
        state.buffer.clear()
        state.buffered_bytes = 0
        state.buffered_level_changes = 0
        state.pending_snapshot = None
        self._install_snapshot(state, snapshot)
        if not self._apply_bootstrap_event(state, first):
            self._restore_buffer(state, buffered)
            return
        for index, event in enumerate(buffered[1:], start=1):
            if state.status is not LocalBookStatus.VALID:
                break
            self._apply_live_event(state, event, rebuffer_gap=False)
            if state.status is not LocalBookStatus.VALID:
                suffix = buffered[index:]
                self._restore_buffer(state, suffix)
                break
        if state.status is LocalBookStatus.VALID:
            self._metrics.snapshots_applied += 1
            self._metrics.books_became_valid += 1
            state.metrics.snapshots_applied += 1
            state.metrics.books_became_valid += 1
            state.unresolved_sequence_gap = False
            state.unresolved_reconstruction = False
            state.resnapshot_required = False
            self._clear_gap(state)

    def _install_snapshot(self, state: _BookState, snapshot: _Snapshot) -> None:
        state.bids = snapshot.bids.copy()
        state.asks = snapshot.asks.copy()
        state.best_bid_price = max(state.bids, default=None)
        state.best_ask_price = min(state.asks, default=None)
        state.bid_retained_floor = snapshot.bid_retained_floor
        state.ask_retained_ceiling = snapshot.ask_retained_ceiling
        state.bid_feature_band_complete = snapshot.bid_feature_band_complete
        state.ask_feature_band_complete = snapshot.ask_feature_band_complete
        state.update_id = snapshot.update_id
        state.availability_receipt_at_ms = snapshot.response_completed_at_ms
        state.availability_receipt_monotonic_ns = snapshot.response_completed_monotonic_ns
        state.status = LocalBookStatus.VALID
        state.reason = LocalBookReason.VALID

    def _apply_bootstrap_event(self, state: _BookState, event: _DepthEvent) -> bool:
        if not self._apply_level_changes(state, event):
            return False
        state.update_id = event.final_update_id
        self._advance_availability(state, event)
        self._metrics.depth_events_applied += 1
        state.metrics.depth_events_applied += 1
        return True

    def _apply_live_event(
        self,
        state: _BookState,
        event: _DepthEvent,
        *,
        rebuffer_gap: bool = True,
    ) -> None:
        current_id = state.update_id
        if current_id is None:
            state.unresolved_sequence_gap = True
            self._record_gap_from_event(state, LocalBookReason.SEQUENCE_GAP, event)
            self._invalidate(state, LocalBookReason.SEQUENCE_GAP, event.received_monotonic_ns)
            self._metrics.sequence_gaps += 1
            state.metrics.sequence_gaps += 1
            if rebuffer_gap:
                self._append_buffer(state, event)
            return
        if state.market is Market.SPOT:
            if event.final_update_id < current_id:
                self._metrics.old_events_ignored += 1
                state.metrics.old_events_ignored += 1
                return
            if event.first_update_id > current_id + 1:
                self._metrics.sequence_gaps += 1
                state.metrics.sequence_gaps += 1
                state.unresolved_sequence_gap = True
                self._record_gap_from_event(state, LocalBookReason.SEQUENCE_GAP, event)
                self._invalidate(
                    state,
                    LocalBookReason.SEQUENCE_GAP,
                    event.received_monotonic_ns,
                )
                if rebuffer_gap:
                    self._append_buffer(state, event)
                return
        else:
            if (
                event.previous_final_update_id != current_id
                or event.final_update_id <= current_id
            ):
                self._metrics.sequence_gaps += 1
                state.metrics.sequence_gaps += 1
                state.unresolved_sequence_gap = True
                self._record_gap_from_event(state, LocalBookReason.SEQUENCE_GAP, event)
                self._invalidate(
                    state,
                    LocalBookReason.SEQUENCE_GAP,
                    event.received_monotonic_ns,
                )
                if rebuffer_gap:
                    self._append_buffer(state, event)
                return
        if not self._apply_level_changes(state, event):
            if rebuffer_gap:
                self._append_buffer(state, event)
            return
        state.update_id = event.final_update_id
        self._advance_availability(state, event)
        self._metrics.depth_events_applied += 1
        state.metrics.depth_events_applied += 1

    def _apply_level_changes(self, state: _BookState, event: _DepthEvent) -> bool:
        result = _apply_level_changes_within_band(
            state.bids,
            state.asks,
            event.bids,
            event.asks,
            maximum_levels=self._max_levels,
            feature_band_bps=self._feature_band_bps,
            guard_band_bps=self._guard_band_bps,
            bid_retained_floor=state.bid_retained_floor,
            ask_retained_ceiling=state.ask_retained_ceiling,
        )
        if result is None:
            self._metrics.level_overflows += 1
            state.metrics.level_overflows += 1
            self._record_gap_from_event(state, LocalBookReason.LEVEL_OVERFLOW, event)
            self._invalidate(
                state,
                LocalBookReason.LEVEL_OVERFLOW,
                event.received_monotonic_ns,
            )
            return False
        state.best_bid_price = result.best_bid_price
        state.best_ask_price = result.best_ask_price
        state.bid_retained_floor = result.bid_retained_floor
        state.ask_retained_ceiling = result.ask_retained_ceiling
        state.bid_feature_band_complete = True
        state.ask_feature_band_complete = True
        return True

    @staticmethod
    def _advance_availability(state: _BookState, event: _DepthEvent) -> None:
        current_monotonic = state.availability_receipt_monotonic_ns
        if current_monotonic is None or event.received_monotonic_ns > current_monotonic:
            # Wall and monotonic receipts are an observed pair.  Independent
            # maxima would synthesize a timestamp across two records if UTC
            # steps.  On an exact monotonic tie, preserve the earlier pair.
            state.availability_receipt_at_ms = event.received_at_ms
            state.availability_receipt_monotonic_ns = event.received_monotonic_ns
        state.status = LocalBookStatus.VALID
        state.reason = LocalBookReason.VALID

    def _invalidate(
        self,
        state: _BookState,
        reason: LocalBookReason,
        boundary_monotonic_ns: int,
    ) -> None:
        availability = state.availability_receipt_monotonic_ns or 0
        boundary = max(
            state.snapshot_min_started_monotonic_ns,
            boundary_monotonic_ns,
            availability,
        )
        self._clear_reconstruction(state)
        state.unresolved_reconstruction = True
        state.resnapshot_required = state.connected
        state.snapshot_min_started_monotonic_ns = boundary
        state.status = LocalBookStatus.AWAITING_SNAPSHOT
        state.reason = reason

    @staticmethod
    def _clear_reconstruction(state: _BookState) -> None:
        state.update_id = None
        state.availability_receipt_at_ms = None
        state.availability_receipt_monotonic_ns = None
        state.bids.clear()
        state.asks.clear()
        state.best_bid_price = None
        state.best_ask_price = None
        state.bid_retained_floor = None
        state.ask_retained_ceiling = None
        state.bid_feature_band_complete = False
        state.ask_feature_band_complete = False
        state.buffer.clear()
        state.buffered_bytes = 0
        state.buffered_level_changes = 0
        state.pending_snapshot = None

    def _restore_buffer(
        self,
        state: _BookState,
        events: tuple[_DepthEvent, ...],
    ) -> None:
        buffered_bytes = sum(event.raw_size_bytes for event in events)
        buffered_level_changes = sum(event.level_change_count for event in events)
        if (
            len(events) > self._max_buffered_events
            or buffered_bytes > self._max_buffered_bytes
            or buffered_level_changes > self._max_buffered_level_changes
        ):
            raise LocalBookReplayError(
                "previously admitted reconstruction suffix exceeds its sealed bounds"
            )
        state.buffer = list(events)
        state.buffered_bytes = buffered_bytes
        state.buffered_level_changes = buffered_level_changes

    @staticmethod
    def _record_gap_from_event(
        state: _BookState,
        reason: LocalBookReason,
        event: _DepthEvent,
    ) -> None:
        LocalBookMaterializer._record_gap(
            state,
            reason,
            first_update_id=event.first_update_id,
            last_update_id=event.final_update_id,
            first_receipt_monotonic_ns=event.received_monotonic_ns,
            last_receipt_monotonic_ns=event.received_monotonic_ns,
        )

    @staticmethod
    def _record_gap(
        state: _BookState,
        reason: LocalBookReason,
        *,
        first_update_id: int | None,
        last_update_id: int | None,
        first_receipt_monotonic_ns: int,
        last_receipt_monotonic_ns: int,
    ) -> None:
        if state.gap_reason is None or state.gap_generation != state.generation:
            state.gap_reason = reason
            state.gap_first_update_id = first_update_id
            state.gap_last_update_id = last_update_id
            state.gap_first_receipt_monotonic_ns = first_receipt_monotonic_ns
            state.gap_last_receipt_monotonic_ns = last_receipt_monotonic_ns
            state.gap_generation = state.generation
            return
        if last_update_id is not None and (
            state.gap_last_update_id is None
            or last_update_id > state.gap_last_update_id
        ):
            state.gap_last_update_id = last_update_id
        if state.gap_first_receipt_monotonic_ns is None:
            state.gap_first_receipt_monotonic_ns = first_receipt_monotonic_ns
        state.gap_last_receipt_monotonic_ns = max(
            state.gap_last_receipt_monotonic_ns or 0,
            last_receipt_monotonic_ns,
        )

    @staticmethod
    def _clear_gap(state: _BookState) -> None:
        state.gap_reason = None
        state.gap_first_update_id = None
        state.gap_last_update_id = None
        state.gap_first_receipt_monotonic_ns = None
        state.gap_last_receipt_monotonic_ns = None
        state.gap_generation = state.generation


def _positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _depth_route_is_valid(market: Market, route: str) -> bool:
    return route == ("spot" if market is Market.SPOT else "public")


def _depth_key(market: Market, stream: str) -> _BookKey | None:
    if not stream.endswith(_DEPTH_SUFFIX):
        return None
    symbol_part = stream[: -len(_DEPTH_SUFFIX)]
    if not symbol_part or symbol_part != symbol_part.casefold():
        return None
    symbol = symbol_part.upper()
    if symbol not in CANARY_SYMBOLS:
        return None
    return market, symbol


def _depth_keys(
    market: Market,
    streams: tuple[str, ...],
) -> tuple[tuple[_BookKey, ...], bool]:
    keys: list[_BookKey] = []
    malformed = False
    for stream in streams:
        if not stream.endswith(_DEPTH_SUFFIX):
            continue
        key = _depth_key(market, stream)
        if key is None or key in keys:
            malformed = True
            continue
        keys.append(key)
    return tuple(keys), malformed


def _decode_json_payload(
    record: CaptureEnvelopeV1 | RestEnvelopeV2,
) -> tuple[bytes, dict[str, object]]:
    raw = payload_bytes(record.raw_payload, record.raw_payload_encoding)
    text = raw.decode("utf-8")

    def reject_constant(value: str) -> object:
        raise _DepthDataError(f"non-finite JSON constant is forbidden: {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        parsed_object: dict[str, object] = {}
        for key, value in pairs:
            if key in parsed_object:
                raise _DepthDataError(f"duplicate JSON object key is forbidden: {key}")
            parsed_object[key] = value
        return parsed_object

    parsed = json.loads(
        text,
        parse_constant=reject_constant,
        object_pairs_hook=reject_duplicate_keys,
    )
    if not isinstance(parsed, dict):
        raise _DepthDataError("payload root must be an object")
    return raw, cast(dict[str, object], parsed)


def _sequence_id(data: dict[str, object], field_name: str) -> int:
    value = data.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _DepthDataError(f"{field_name} must be a nonnegative integer")
    return value


def _parse_depth_event(
    *,
    market: Market,
    symbol: str,
    data: object,
    received_at_ms: int,
    received_monotonic_ns: int,
    raw_size_bytes: int,
    max_levels: int,
) -> _DepthEvent:
    if not isinstance(data, dict):
        raise _DepthDataError("depth event data must be an object")
    payload = cast(dict[str, object], data)
    if payload.get("e") != "depthUpdate" or payload.get("s") != symbol:
        raise _DepthDataError("depth event identity differs from its stream")
    first_u = _sequence_id(payload, "U")
    final_u = _sequence_id(payload, "u")
    if first_u > final_u:
        raise _DepthDataError("depth event update range is reversed")
    if market is Market.SPOT:
        if "pu" in payload:
            raise _DepthDataError("Spot depth must not contain or synthesize pu")
        previous_u = None
    else:
        previous_u = _sequence_id(payload, "pu")
        stream_type = payload.get("st")
        if isinstance(stream_type, bool) or not isinstance(stream_type, int) or stream_type != 1:
            raise _DepthDataError("USD-M depth st must be integer 1")
        if payload.get("ps") != symbol:
            raise _DepthDataError("USD-M depth ps must match the routed symbol")
    bids = _parse_levels(payload.get("b"), "b", max_levels)
    asks = _parse_levels(payload.get("a"), "a", max_levels)
    return _DepthEvent(
        first_update_id=first_u,
        final_update_id=final_u,
        previous_final_update_id=previous_u,
        bids=bids,
        asks=asks,
        received_at_ms=received_at_ms,
        received_monotonic_ns=received_monotonic_ns,
        raw_size_bytes=raw_size_bytes,
    )


def _snapshot_query_symbol(record: RestEnvelopeV2) -> str:
    if len(record.canonical_query) != 2:
        raise _DepthDataError("depth snapshot query must contain symbol and limit only")
    query = dict(record.canonical_query)
    if len(query) != 2 or set(query) != {"limit", "symbol"}:
        raise _DepthDataError("depth snapshot query must contain unique symbol and limit")
    symbol = query["symbol"]
    if symbol not in CANARY_SYMBOLS:
        raise _DepthDataError("depth snapshot symbol is outside the fixed canary")
    try:
        limit = int(query["limit"])
    except ValueError as exc:
        raise _DepthDataError("depth snapshot limit must be an integer") from exc
    if str(limit) != query["limit"] or limit < 1:
        raise _DepthDataError("depth snapshot limit must be a canonical positive integer")
    required = _venue_snapshot_limit(record.market)
    if limit != required:
        raise _DepthDataError("depth snapshot limit differs from the frozen venue value")
    return symbol


def _venue_snapshot_limit(market: Market) -> int:
    return 5_000 if market is Market.SPOT else 1_000


def _validate_snapshot_identity(record: RestEnvelopeV2) -> None:
    expected = (
        ("spot_depth_snapshot", "/api/v3/depth")
        if record.market is Market.SPOT
        else ("futures_depth_snapshot", "/fapi/v1/depth")
    )
    if (record.request_role, record.endpoint_path) != expected:
        raise _DepthDataError("depth snapshot role, market, and endpoint disagree")


def _successful_snapshot_response(record: RestEnvelopeV2) -> bool:
    return (
        record.response_status == 200
        and record.payload_complete
        and record.error_category is None
    )


def _parse_snapshot(
    payload: dict[str, object],
    *,
    response_completed_at_ms: int,
    response_completed_monotonic_ns: int,
    source_limit: int,
    maximum_retained_levels: int,
    feature_band_bps: int,
    guard_band_bps: int,
) -> _Snapshot:
    update_id = _sequence_id(payload, "lastUpdateId")
    bid_levels = _parse_levels(payload.get("bids"), "bids", source_limit)
    ask_levels = _parse_levels(payload.get("asks"), "asks", source_limit)
    _require_snapshot_order(bid_levels, highest=True, field_name="bids")
    _require_snapshot_order(ask_levels, highest=False, field_name="asks")
    bids: dict[Decimal, Decimal] = {}
    asks: dict[Decimal, Decimal] = {}
    _apply_levels(bids, bid_levels)
    _apply_levels(asks, ask_levels)
    bounded_bids = _bounded_snapshot_side(
        bids,
        source_count=len(bid_levels),
        source_limit=source_limit,
        maximum_retained_levels=maximum_retained_levels,
        feature_band_bps=feature_band_bps,
        guard_band_bps=guard_band_bps,
        highest=True,
    )
    bounded_asks = _bounded_snapshot_side(
        asks,
        source_count=len(ask_levels),
        source_limit=source_limit,
        maximum_retained_levels=maximum_retained_levels,
        feature_band_bps=feature_band_bps,
        guard_band_bps=guard_band_bps,
        highest=False,
    )
    return _Snapshot(
        update_id=update_id,
        bids=bounded_bids.levels,
        asks=bounded_asks.levels,
        response_completed_at_ms=response_completed_at_ms,
        response_completed_monotonic_ns=response_completed_monotonic_ns,
        bid_retained_floor=bounded_bids.retained_boundary,
        ask_retained_ceiling=bounded_asks.retained_boundary,
        bid_feature_band_complete=True,
        ask_feature_band_complete=True,
    )


def _parse_levels(value: object, field_name: str, max_levels: int) -> tuple[PriceLevel, ...]:
    if not isinstance(value, list):
        raise _DepthDataError(f"{field_name} must be an array")
    if len(value) > max_levels:
        raise _LevelBoundError(f"{field_name} exceeds the level bound")
    result: list[PriceLevel] = []
    seen: set[Decimal] = set()
    for raw_level in value:
        if not isinstance(raw_level, list) or len(raw_level) != 2:
            raise _DepthDataError(f"{field_name} entries must be exact two-item arrays")
        price = _decimal_field(raw_level[0], f"{field_name}.price", positive=True)
        quantity = _decimal_field(raw_level[1], f"{field_name}.quantity", positive=False)
        if price in seen:
            raise _DepthDataError(f"{field_name} contains a duplicate price")
        seen.add(price)
        result.append((price, quantity))
    return tuple(result)


def _decimal_field(value: object, field_name: str, *, positive: bool) -> Decimal:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise _DepthDataError(f"{field_name} must be a normalized decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise _DepthDataError(f"{field_name} must be a decimal") from exc
    if not parsed.is_finite():
        raise _DepthDataError(f"{field_name} must be finite")
    if positive and parsed <= 0:
        raise _DepthDataError(f"{field_name} must be positive")
    if not positive and parsed < 0:
        raise _DepthDataError(f"{field_name} must be nonnegative")
    return parsed


def _apply_levels(book: dict[Decimal, Decimal], levels: tuple[PriceLevel, ...]) -> None:
    for price, quantity in levels:
        if quantity == 0:
            book.pop(price, None)
        else:
            book[price] = quantity


def _require_snapshot_order(
    levels: tuple[PriceLevel, ...],
    *,
    highest: bool,
    field_name: str,
) -> None:
    previous: Decimal | None = None
    for price, quantity in levels:
        if quantity <= 0:
            raise _DepthDataError(f"{field_name} snapshot quantity must be positive")
        if previous is not None and (
            (highest and price >= previous) or (not highest and price <= previous)
        ):
            raise _DepthDataError(
                f"{field_name} snapshot prices must be strictly book-ordered"
            )
        previous = price


def _bounded_snapshot_side(
    book: dict[Decimal, Decimal],
    *,
    source_count: int,
    source_limit: int,
    maximum_retained_levels: int,
    feature_band_bps: int,
    guard_band_bps: int,
    highest: bool,
) -> _BoundedSnapshotSide:
    if not book:
        return _BoundedSnapshotSide(levels={}, retained_boundary=None)
    best = max(book) if highest else min(book)
    worst = min(book) if highest else max(book)
    feature_boundary = _band_boundary(best, feature_band_bps, highest=highest)
    guard_boundary = _band_boundary(best, guard_band_bps, highest=highest)
    source_is_truncated = source_count == source_limit
    if source_is_truncated and not _boundary_is_covered(
        worst,
        feature_boundary,
        highest=highest,
    ):
        raise _LevelBoundError(
            "maximum venue snapshot does not prove complete feature-band depth"
        )
    retained_boundary = guard_boundary
    if source_is_truncated and not _boundary_is_covered(
        worst,
        guard_boundary,
        highest=highest,
    ):
        retained_boundary = worst
    retained = {
        price: quantity
        for price, quantity in book.items()
        if _price_is_retained(price, retained_boundary, highest=highest)
    }
    if len(retained) > maximum_retained_levels:
        feature_level_count = sum(
            1
            for price in retained
            if _price_is_retained(price, feature_boundary, highest=highest)
        )
        if feature_level_count > maximum_retained_levels:
            raise _LevelBoundError(
                "feature-band representation exceeds qualified level capacity"
            )
        ordered = sorted(retained, reverse=highest)
        retained_boundary = (
            feature_boundary
            if feature_level_count == maximum_retained_levels
            else ordered[maximum_retained_levels - 1]
        )
        retained = {
            price: book[price]
            for price in ordered
            if _price_is_retained(price, retained_boundary, highest=highest)
        }
    return _BoundedSnapshotSide(
        levels=retained,
        retained_boundary=retained_boundary,
    )


def _apply_level_changes_within_band(
    bids: dict[Decimal, Decimal],
    asks: dict[Decimal, Decimal],
    bid_changes: tuple[PriceLevel, ...],
    ask_changes: tuple[PriceLevel, ...],
    *,
    maximum_levels: int,
    feature_band_bps: int,
    guard_band_bps: int,
    bid_retained_floor: Decimal | None,
    ask_retained_ceiling: Decimal | None,
) -> _BandApplyResult | None:
    """Atomically update only a certified feature band plus resnapshot guard."""

    bid_plan = _plan_side_update(
        bids,
        bid_changes,
        retained_boundary=bid_retained_floor,
        maximum_levels=maximum_levels,
        feature_band_bps=feature_band_bps,
        guard_band_bps=guard_band_bps,
        highest=True,
    )
    ask_plan = _plan_side_update(
        asks,
        ask_changes,
        retained_boundary=ask_retained_ceiling,
        maximum_levels=maximum_levels,
        feature_band_bps=feature_band_bps,
        guard_band_bps=guard_band_bps,
        highest=False,
    )
    if bid_plan is None or ask_plan is None:
        return None
    _commit_side_plan(bids, bid_plan, highest=True)
    _commit_side_plan(asks, ask_plan, highest=False)
    return _BandApplyResult(
        best_bid_price=bid_plan.best_price,
        best_ask_price=ask_plan.best_price,
        bid_retained_floor=bid_plan.retained_boundary,
        ask_retained_ceiling=ask_plan.retained_boundary,
    )


def _plan_side_update(
    book: dict[Decimal, Decimal],
    changes: tuple[PriceLevel, ...],
    *,
    retained_boundary: Decimal | None,
    maximum_levels: int,
    feature_band_bps: int,
    guard_band_bps: int,
    highest: bool,
) -> _SideUpdatePlan | None:
    eligible_changes = tuple(
        change
        for change in changes
        if retained_boundary is None
        or _price_is_retained(change[0], retained_boundary, highest=highest)
    )
    overrides = dict(eligible_changes)
    candidate_prices = [
        price
        for price, quantity in book.items()
        if overrides.get(price, quantity) != 0
    ]
    candidate_prices.extend(
        price
        for price, quantity in eligible_changes
        if price not in book and quantity != 0
    )
    if not candidate_prices:
        if not book and retained_boundary is None:
            return _SideUpdatePlan(
                changes=(),
                retained_boundary=None,
                best_price=None,
            )
        return None
    best = max(candidate_prices) if highest else min(candidate_prices)
    desired_guard = _band_boundary(best, guard_band_bps, highest=highest)
    if retained_boundary is None:
        next_boundary = desired_guard
    elif highest:
        next_boundary = max(retained_boundary, desired_guard)
    else:
        next_boundary = min(retained_boundary, desired_guard)
    feature_boundary = _band_boundary(best, feature_band_bps, highest=highest)
    if not _boundary_is_covered(
        next_boundary,
        feature_boundary,
        highest=highest,
    ):
        return None
    final_prices = {
        price
        for price, quantity in book.items()
        if _price_is_retained(price, next_boundary, highest=highest)
        and overrides.get(price, quantity) != 0
    }
    final_prices.update(
        price
        for price, quantity in eligible_changes
        if _price_is_retained(price, next_boundary, highest=highest)
        and quantity != 0
    )
    if len(final_prices) > maximum_levels:
        feature_level_count = sum(
            1
            for price in final_prices
            if _price_is_retained(price, feature_boundary, highest=highest)
        )
        if feature_level_count > maximum_levels:
            return None
        ordered = sorted(final_prices, reverse=highest)
        next_boundary = (
            feature_boundary
            if feature_level_count == maximum_levels
            else ordered[maximum_levels - 1]
        )
    retained_changes = tuple(
        change
        for change in eligible_changes
        if _price_is_retained(change[0], next_boundary, highest=highest)
    )
    return _SideUpdatePlan(
        changes=retained_changes,
        retained_boundary=next_boundary,
        best_price=best,
    )


def _commit_side_plan(
    book: dict[Decimal, Decimal],
    plan: _SideUpdatePlan,
    *,
    highest: bool,
) -> None:
    boundary = plan.retained_boundary
    if boundary is None:
        book.clear()
        return
    for price in tuple(book):
        if not _price_is_retained(price, boundary, highest=highest):
            del book[price]
    _apply_levels(book, plan.changes)


def _band_boundary(best: Decimal, band_bps: int, *, highest: bool) -> Decimal:
    signed = Decimal(-band_bps if highest else band_bps)
    return best * (_BPS_DENOMINATOR + signed) / _BPS_DENOMINATOR


def _price_is_retained(
    price: Decimal,
    boundary: Decimal,
    *,
    highest: bool,
) -> bool:
    return price >= boundary if highest else price <= boundary


def _boundary_is_covered(
    observed_boundary: Decimal,
    required_boundary: Decimal,
    *,
    highest: bool,
) -> bool:
    return (
        observed_boundary <= required_boundary
        if highest
        else observed_boundary >= required_boundary
    )


def _apply_level_changes_within_bound(
    bids: dict[Decimal, Decimal],
    asks: dict[Decimal, Decimal],
    bid_changes: tuple[PriceLevel, ...],
    ask_changes: tuple[PriceLevel, ...],
    maximum_levels: int,
) -> bool:
    """Atomically preflight and apply one parsed delta in O(changed levels)."""

    if (
        _projected_level_count(bids, bid_changes) > maximum_levels
        or _projected_level_count(asks, ask_changes) > maximum_levels
    ):
        return False
    _apply_levels(bids, bid_changes)
    _apply_levels(asks, ask_changes)
    return True


def _projected_level_count(
    book: dict[Decimal, Decimal],
    changes: tuple[PriceLevel, ...],
) -> int:
    projected = len(book)
    for price, quantity in changes:
        exists = price in book
        if quantity == 0 and exists:
            projected -= 1
        elif quantity != 0 and not exists:
            projected += 1
    return projected
