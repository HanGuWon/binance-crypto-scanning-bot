from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal, TypeAlias

from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import WebSocketPlan

DepthResyncEvent: TypeAlias = Literal[  # noqa: UP040 - host compileall is Python 3.11
    "startup", "reconnect", "sequence_gap"
]


@dataclass(frozen=True, slots=True)
class DepthResyncRequest:
    """Immutable operational request joining raw depth to REST capture.

    ``watermarks`` contains the first buffered ``U`` observed for each symbol
    covered by this request.  It is deliberately only a snapshot-capture
    watermark; offline local-book materialization remains responsible for the
    venue-specific bridge decision: Spot targets ``lastUpdateId + 1`` under the
    frozen DESIGN reconciliation, while USD-M Futures targets
    ``lastUpdateId``.
    """

    event: DepthResyncEvent
    market: Market
    generation: int
    watermarks: tuple[tuple[str, int], ...]

    def __post_init__(self) -> None:
        if not isinstance(self.event, str) or self.event not in (
            "startup",
            "reconnect",
            "sequence_gap",
        ):
            raise ValueError("depth resync event is unsupported")
        if not isinstance(self.market, Market):
            raise ValueError("depth resync market is unsupported")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 1
        ):
            raise ValueError("depth resync generation must be a positive integer")
        if not isinstance(self.watermarks, tuple) or not self.watermarks:
            raise ValueError("depth resync watermarks must be a non-empty immutable tuple")
        symbols: list[str] = []
        for watermark in self.watermarks:
            if not isinstance(watermark, tuple) or len(watermark) != 2:
                raise ValueError("each depth resync watermark must be an immutable pair")
            symbol, first_buffered_u = watermark
            if (
                not isinstance(symbol, str)
                or not symbol
                or symbol != symbol.strip()
                or symbol != symbol.upper()
                or not symbol.isascii()
                or not symbol.isalnum()
            ):
                raise ValueError("depth resync symbols must be normalized uppercase identifiers")
            if (
                isinstance(first_buffered_u, bool)
                or not isinstance(first_buffered_u, int)
                or first_buffered_u < 0
            ):
                raise ValueError("depth resync first-buffered U must be a nonnegative integer")
            symbols.append(symbol)
        if len(set(symbols)) != len(symbols):
            raise ValueError("depth resync watermarks must contain unique symbols")
        if self.watermarks != tuple(sorted(self.watermarks)):
            raise ValueError("depth resync watermarks must be sorted")
        if self.event == "sequence_gap" and len(self.watermarks) != 1:
            raise ValueError("a sequence-gap resync must cover exactly one symbol")


@dataclass(frozen=True, slots=True)
class DepthRangeObservation:
    """One validated diff-depth range observed after its raw frame was offered."""

    market: Market
    symbol: str
    generation: int
    U: int
    u: int
    reset: bool

    def __post_init__(self) -> None:
        if not isinstance(self.market, Market):
            raise ValueError("depth range market is unsupported")
        if (
            not isinstance(self.symbol, str)
            or not self.symbol
            or self.symbol != self.symbol.strip()
            or self.symbol != self.symbol.upper()
            or not self.symbol.isascii()
            or not self.symbol.isalnum()
        ):
            raise ValueError("depth range symbol must be a normalized uppercase identifier")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 1
        ):
            raise ValueError("depth range generation must be a positive integer")
        for name, value in (("U", self.U), ("u", self.u)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"depth range {name} must be a nonnegative integer")
        if self.U > self.u:
            raise ValueError("depth range is reversed")
        if not isinstance(self.reset, bool):
            raise ValueError("depth range reset must be boolean")


DepthResyncCallback: TypeAlias = Callable[  # noqa: UP040 - host compileall is Python 3.11
    [DepthResyncRequest], None
]
DepthRangeCallback: TypeAlias = Callable[  # noqa: UP040 - host compileall is Python 3.11
    [DepthRangeObservation], None
]

DepthSnapshotBridgeStatus: TypeAlias = Literal[  # noqa: UP040 - host compileall is Python 3.11
    "accepted", "stale", "waiting"
]


@dataclass(frozen=True, slots=True)
class DepthSnapshotBridgeDecision:
    """Pure venue-specific classification of one snapshot against buffered ranges."""

    status: DepthSnapshotBridgeStatus
    discarded_range_count: int
    target_update_id: int


def classify_depth_snapshot_bridge(
    market: Market,
    last_update_id: int,
    ranges: Sequence[tuple[int, int]],
) -> DepthSnapshotBridgeDecision:
    """Reduce buffered ranges to the first venue-eligible snapshot bridge.

    Spot discards ranges through ``u == lastUpdateId`` and targets the exact
    successor ``lastUpdateId + 1``. USD-M Futures discards only ranges with
    ``u < lastUpdateId`` and targets ``lastUpdateId`` itself. The input is not
    mutated, so online capture and offline replay can apply their own retention
    policy while sharing the same boundary decision.
    """

    if not isinstance(market, Market):
        raise ValueError("snapshot bridge market is unsupported")
    if (
        isinstance(last_update_id, bool)
        or not isinstance(last_update_id, int)
        or last_update_id < 0
    ):
        raise ValueError("snapshot bridge lastUpdateId must be a nonnegative integer")
    target_update_id = (
        last_update_id + 1 if market is Market.SPOT else last_update_id
    )
    discarded_range_count = 0
    for first_update_id, final_update_id in ranges:
        for name, value in (("U", first_update_id), ("u", final_update_id)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise DepthSequenceError(
                    f"snapshot bridge range {name} must be a nonnegative integer"
                )
        if first_update_id > final_update_id:
            raise DepthSequenceError("snapshot bridge range is reversed")
        discard = (
            final_update_id <= last_update_id
            if market is Market.SPOT
            else final_update_id < last_update_id
        )
        if discard:
            discarded_range_count += 1
            continue
        if first_update_id <= target_update_id <= final_update_id:
            return DepthSnapshotBridgeDecision(
                status="accepted",
                discarded_range_count=discarded_range_count,
                target_update_id=target_update_id,
            )
        if first_update_id > target_update_id:
            return DepthSnapshotBridgeDecision(
                status="stale",
                discarded_range_count=discarded_range_count,
                target_update_id=target_update_id,
            )
        raise DepthSequenceError(
            "snapshot bridge retained an impossible range ordering"
        )
    return DepthSnapshotBridgeDecision(
        status="waiting",
        discarded_range_count=discarded_range_count,
        target_update_id=target_update_id,
    )

_DEPTH_SUFFIX = "@depth@100ms"
_MAXIMUM_DEPTH_STREAMS = 50


class DepthSequenceError(RuntimeError):
    """A raw depth frame cannot be admitted to continuity evidence."""


class DepthResyncUnavailable(DepthSequenceError):
    """A depth plan needs a resnapshot but no bounded scheduler callback is wired."""


class RawDepthContinuityMonitor:
    """Bounded generation-local continuity checks over raw combined frames.

    The monitor owns one fixed state slot per depth stream already authorized by
    the WebSocket plan. It never creates a key from inbound data. The first depth
    event of each connection generation establishes a raw-stream baseline; the
    REST snapshot bridge remains a separate local-book responsibility.
    """

    def __init__(
        self,
        plan: WebSocketPlan,
        *,
        on_resync: DepthResyncCallback,
        on_range: DepthRangeCallback,
    ) -> None:
        depth_streams = tuple(stream for stream in plan.streams if stream.endswith(_DEPTH_SUFFIX))
        if len(depth_streams) > _MAXIMUM_DEPTH_STREAMS:
            raise ValueError("depth continuity state exceeds its fixed stream bound")
        self._plan = plan
        self._authorized_streams = frozenset(plan.streams)
        self._depth_streams = frozenset(depth_streams)
        self._previous_u: dict[str, int | None] = {stream: None for stream in depth_streams}
        self._generation_first_u: dict[str, int | None] = {
            stream: None for stream in depth_streams
        }
        self._pending_generation_baselines: set[str] | None = None
        self._on_resync = on_resync
        self._on_range = on_range
        self._generation = 0

    @property
    def has_depth(self) -> bool:
        return bool(self._depth_streams)

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def state_count(self) -> int:
        return len(self._previous_u)

    def start_generation(self, generation: int) -> None:
        """Reset every fixed stream slot for a strictly newer connection."""

        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
            raise ValueError("depth connection generation must be a positive integer")
        if generation <= self._generation:
            raise ValueError("depth connection generation must increase monotonically")
        self._generation = generation
        for stream in self._previous_u:
            self._previous_u[stream] = None
            self._generation_first_u[stream] = None
        self._pending_generation_baselines = set(self._depth_streams)

    def observe_after_offer(self, raw: str | bytes) -> None:
        """Validate one already-offered raw frame and emit bounded resync events."""

        if not self.has_depth:
            return
        if self._generation < 1:
            raise DepthSequenceError("depth frame arrived before generation reset")
        payload = _decode_json_object(raw)
        stream = payload.get("stream")
        data = payload.get("data")
        if not isinstance(stream, str) or stream not in self._authorized_streams:
            raise DepthSequenceError("combined frame names an unknown subscription stream")
        if stream not in self._depth_streams:
            return
        if not isinstance(data, dict):
            raise DepthSequenceError("depth combined frame data must be an object")
        self._observe_depth(stream, data)

    def _observe_depth(self, stream: str, data: dict[str, object]) -> None:
        if data.get("e") != "depthUpdate":
            raise DepthSequenceError("depth stream frame has an unknown event type")
        expected_symbol = stream.split("@", 1)[0].upper()
        if data.get("s") != expected_symbol:
            raise DepthSequenceError("depth stream symbol differs from its subscription")
        first_u = _sequence_id(data, "U")
        final_u = _sequence_id(data, "u")
        if first_u > final_u:
            raise DepthSequenceError("depth update range is reversed")

        previous_u = self._previous_u[stream]
        if self._plan.market is Market.SPOT:
            gap = self._observe_spot(stream, first_u, final_u, previous_u)
        else:
            _validate_futures_public_identity(data, expected_symbol)
            previous_event_u = _sequence_id(data, "pu")
            gap = self._observe_futures(
                stream,
                first_u,
                final_u,
                previous_event_u,
                previous_u,
            )
        self._on_range(
            DepthRangeObservation(
                market=self._plan.market,
                symbol=expected_symbol,
                generation=self._generation,
                U=first_u,
                u=final_u,
                reset=previous_u is None or gap,
            )
        )
        if gap:
            self._emit_sequence_gap(stream, first_u)
        self._mark_generation_baseline(stream, first_u)

    def _mark_generation_baseline(self, stream: str, first_u: int) -> None:
        pending = self._pending_generation_baselines
        if pending is None:
            return
        if self._generation_first_u[stream] is None:
            self._generation_first_u[stream] = first_u
        pending.discard(stream)
        if pending:
            return
        # Clear before invoking an external callback so re-entrancy or a
        # fail-closed callback cannot emit the generation bootstrap twice.
        self._pending_generation_baselines = None
        event: DepthResyncEvent = "startup" if self._generation == 1 else "reconnect"
        watermarks: list[tuple[str, int]] = []
        for depth_stream, generation_first_u in self._generation_first_u.items():
            if generation_first_u is None:
                raise DepthSequenceError("depth generation lacks a first-buffered U watermark")
            symbol = depth_stream.split("@", 1)[0].upper()
            watermarks.append((symbol, generation_first_u))
        self._on_resync(
            DepthResyncRequest(
                event=event,
                market=self._plan.market,
                generation=self._generation,
                watermarks=tuple(sorted(watermarks)),
            )
        )

    def _observe_spot(
        self,
        stream: str,
        first_u: int,
        final_u: int,
        previous_u: int | None,
    ) -> bool:
        if previous_u is None:
            self._previous_u[stream] = final_u
            return False
        if final_u <= previous_u:
            # Official Spot local-book handling permits stale/duplicate ranges
            # to be ignored. They cannot advance or enlarge monitor state.
            return False
        gap = first_u > previous_u + 1
        self._previous_u[stream] = final_u
        return gap

    def _observe_futures(
        self,
        stream: str,
        first_u: int,
        final_u: int,
        previous_event_u: int,
        previous_u: int | None,
    ) -> bool:
        if previous_u is None:
            self._previous_u[stream] = final_u
            return False
        if final_u <= previous_u:
            raise DepthSequenceError("Futures depth update ID did not advance")
        gap = previous_event_u != previous_u
        self._previous_u[stream] = final_u
        return gap

    def _emit_sequence_gap(self, stream: str, first_u: int) -> None:
        symbol = stream.split("@", 1)[0].upper()
        self._on_resync(
            DepthResyncRequest(
                event="sequence_gap",
                market=self._plan.market,
                generation=self._generation,
                watermarks=((symbol, first_u),),
            )
        )


def _decode_json_object(raw: str | bytes) -> dict[str, object]:
    try:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
    except UnicodeDecodeError as exc:
        raise DepthSequenceError("combined frame is not valid UTF-8") from exc
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, TypeError) as exc:
        raise DepthSequenceError("combined frame is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise DepthSequenceError("combined frame root must be an object")
    return payload


def _sequence_id(data: dict[str, object], field: str) -> int:
    value = data.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise DepthSequenceError(f"depth field {field} must be a nonnegative integer")
    return value


def _validate_futures_public_identity(
    data: dict[str, object],
    expected_symbol: str,
) -> None:
    stream_type = data.get("st")
    if isinstance(stream_type, bool) or not isinstance(stream_type, int) or stream_type != 1:
        raise DepthSequenceError("Futures public depth field st must be integer 1")
    if data.get("ps") != expected_symbol:
        raise DepthSequenceError("Futures public depth pair differs from its subscription")
