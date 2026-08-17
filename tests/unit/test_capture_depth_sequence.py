from __future__ import annotations

import json
from typing import cast

import pytest

from signalbot.capture.depth_sequence import (
    DepthRangeObservation,
    DepthResyncEvent,
    DepthResyncRequest,
    DepthSequenceError,
    RawDepthContinuityMonitor,
    classify_depth_snapshot_bridge,
)
from signalbot.capture.plans import build_prospective_capture_plans
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import WebSocketPlan


def _plan(*, market: Market, route: str) -> WebSocketPlan:
    return next(
        plan
        for plan in build_prospective_capture_plans(
            ("BTCUSDT", "ETHUSDT", "SOLUSDT"),
            batch_size=25,
        )
        if plan.market is market and plan.route == route
    )


def _frame(stream: str, data: object) -> str:
    return json.dumps({"stream": stream, "data": data}, separators=(",", ":"))


def _spot_depth(*, first_u: int, final_u: int, symbol: str = "BTCUSDT") -> str:
    return _frame(
        f"{symbol.lower()}@depth@100ms",
        {
            "e": "depthUpdate",
            "s": symbol,
            "U": first_u,
            "u": final_u,
        },
    )


def _futures_depth(
    *,
    first_u: int,
    final_u: int,
    previous_u: int,
    symbol: str = "BTCUSDT",
) -> str:
    return _frame(
        f"{symbol.lower()}@depth@100ms",
        {
            "e": "depthUpdate",
            "s": symbol,
            "ps": symbol,
            "st": 1,
            "U": first_u,
            "u": final_u,
            "pu": previous_u,
        },
    )


def _monitor(
    plan: WebSocketPlan,
    events: list[DepthResyncRequest],
    observations: list[DepthRangeObservation] | None = None,
) -> RawDepthContinuityMonitor:
    def callback(request: DepthResyncRequest) -> None:
        events.append(request)

    return RawDepthContinuityMonitor(
        plan,
        on_resync=callback,
        on_range=([] if observations is None else observations).append,
    )


def _request_from_untrusted(
    event: object,
    market: object,
    watermarks: object,
) -> DepthResyncRequest:
    return DepthResyncRequest(
        event=cast(DepthResyncEvent, event),
        market=cast(Market, market),
        generation=1,
        watermarks=cast(tuple[tuple[str, int], ...], watermarks),
    )


def test_depth_resync_request_accepts_zero_boundary_and_is_immutable() -> None:
    request = DepthResyncRequest(
        event="sequence_gap",
        market=Market.SPOT,
        generation=1,
        watermarks=(("BTCUSDT", 0),),
    )

    assert request.watermarks == (("BTCUSDT", 0),)
    with pytest.raises(AttributeError):
        request.event = "startup"  # type: ignore[misc]
    with pytest.raises(ValueError, match="generation"):
        DepthResyncRequest(
            event="startup",
            market=Market.SPOT,
            generation=0,
            watermarks=(("BTCUSDT", 1),),
        )


def test_depth_range_observation_accepts_zero_boundary_and_rejects_bad_shape() -> None:
    observation = DepthRangeObservation(
        market=Market.SPOT,
        symbol="BTCUSDT",
        generation=1,
        U=0,
        u=0,
        reset=True,
    )

    assert observation.U == observation.u == 0
    with pytest.raises(ValueError, match="generation"):
        DepthRangeObservation(
            market=Market.SPOT,
            symbol="BTCUSDT",
            generation=0,
            U=0,
            u=0,
            reset=True,
        )
    with pytest.raises(ValueError, match="reversed"):
        DepthRangeObservation(
            market=Market.SPOT,
            symbol="BTCUSDT",
            generation=1,
            U=2,
            u=1,
            reset=False,
        )
    with pytest.raises(ValueError, match="reset"):
        DepthRangeObservation(
            market=Market.SPOT,
            symbol="BTCUSDT",
            generation=1,
            U=1,
            u=1,
            reset=cast(bool, 1),
        )


@pytest.mark.parametrize(
    ("market", "last_update_id", "ranges", "status", "discarded", "target"),
    [
        pytest.param(Market.SPOT, 100, (), "waiting", 0, 101, id="spot-empty"),
        pytest.param(
            Market.SPOT,
            100,
            ((90, 100),),
            "waiting",
            1,
            101,
            id="spot-u-equal-is-old",
        ),
        pytest.param(
            Market.SPOT,
            100,
            ((90, 100), (101, 105)),
            "accepted",
            1,
            101,
            id="spot-successor-contained",
        ),
        pytest.param(
            Market.SPOT,
            100,
            ((90, 100), (102, 105)),
            "stale",
            1,
            101,
            id="spot-successor-missed",
        ),
        pytest.param(
            Market.FUTURES,
            100,
            ((90, 99), (100, 105)),
            "accepted",
            1,
            100,
            id="futures-snapshot-id-contained",
        ),
        pytest.param(
            Market.FUTURES,
            100,
            ((90, 99), (101, 105)),
            "stale",
            1,
            100,
            id="futures-u-equals-l-plus-one-is-stale",
        ),
    ],
)
def test_snapshot_bridge_reducer_covers_venue_boundaries(
    market: Market,
    last_update_id: int,
    ranges: tuple[tuple[int, int], ...],
    status: str,
    discarded: int,
    target: int,
) -> None:
    decision = classify_depth_snapshot_bridge(market, last_update_id, ranges)

    assert decision.status == status
    assert decision.discarded_range_count == discarded
    assert decision.target_update_id == target


@pytest.mark.parametrize(
    ("last_update_id", "discarded_range", "retained_range"),
    [
        pytest.param(
            78_896_562_817,
            (78_896_562_791, 78_896_562_817),
            (78_896_562_818, 78_896_562_832),
            id="eth-cycle-1",
        ),
        pytest.param(
            29_601_413_474,
            (29_601_413_472, 29_601_413_474),
            (29_601_413_475, 29_601_413_477),
            id="sol-cycle-1",
        ),
        pytest.param(
            97_532_164_629,
            (97_532_164_621, 97_532_164_629),
            (97_532_164_630, 97_532_164_651),
            id="btc-cycle-1",
        ),
    ],
)
def test_snapshot_bridge_reducer_accepts_failed_smoke_spot_successors(
    last_update_id: int,
    discarded_range: tuple[int, int],
    retained_range: tuple[int, int],
) -> None:
    decision = classify_depth_snapshot_bridge(
        Market.SPOT,
        last_update_id,
        (discarded_range, retained_range),
    )

    assert decision.status == "accepted"
    assert decision.discarded_range_count == 1
    assert decision.target_update_id == last_update_id + 1


@pytest.mark.parametrize(
    ("last_update_id", "ranges", "message"),
    [
        pytest.param(True, (), "lastUpdateId", id="boolean-snapshot-id"),
        pytest.param(100, ((102, 101),), "reversed", id="reversed-range"),
        pytest.param(100, ((-1, 101),), "nonnegative", id="negative-range"),
    ],
)
def test_snapshot_bridge_reducer_fails_closed_on_invalid_ordering(
    last_update_id: int,
    ranges: tuple[tuple[int, int], ...],
    message: str,
) -> None:
    error = ValueError if isinstance(last_update_id, bool) else DepthSequenceError
    with pytest.raises(error, match=message):
        classify_depth_snapshot_bridge(Market.SPOT, last_update_id, ranges)


@pytest.mark.parametrize(
    ("event", "market", "watermarks"),
    [
        pytest.param("unknown", Market.SPOT, (("BTCUSDT", 1),), id="event"),
        pytest.param([], Market.SPOT, (("BTCUSDT", 1),), id="event-type"),
        pytest.param("startup", "spot", (("BTCUSDT", 1),), id="market"),
        pytest.param("startup", Market.SPOT, [], id="mutable-container"),
        pytest.param("startup", Market.SPOT, (), id="empty"),
        pytest.param("startup", Market.SPOT, (("btcusdt", 1),), id="symbol-case"),
        pytest.param("startup", Market.SPOT, (("BTC-USDT", 1),), id="symbol-shape"),
        pytest.param("startup", Market.SPOT, (("BTCUSDT", True),), id="boolean-u"),
        pytest.param("startup", Market.SPOT, (("BTCUSDT", -1),), id="negative-u"),
        pytest.param(
            "startup",
            Market.SPOT,
            (("ETHUSDT", 1), ("BTCUSDT", 2)),
            id="unsorted",
        ),
        pytest.param(
            "startup",
            Market.SPOT,
            (("BTCUSDT", 1), ("BTCUSDT", 2)),
            id="duplicate",
        ),
        pytest.param(
            "sequence_gap",
            Market.SPOT,
            (("BTCUSDT", 1), ("ETHUSDT", 2)),
            id="multi-symbol-gap",
        ),
    ],
)
def test_depth_resync_request_rejects_untrusted_shape(
    event: object,
    market: object,
    watermarks: object,
) -> None:
    with pytest.raises(ValueError):
        _request_from_untrusted(event, market, watermarks)


def test_spot_monitor_accepts_overlap_stale_and_exact_boundary_but_reports_gap() -> None:
    events: list[DepthResyncRequest] = []
    monitor = _monitor(_plan(market=Market.SPOT, route="spot"), events)
    monitor.start_generation(1)

    monitor.observe_after_offer(_spot_depth(first_u=10, final_u=12))
    monitor.observe_after_offer(_spot_depth(first_u=13, final_u=14))
    monitor.observe_after_offer(_spot_depth(first_u=14, final_u=16))
    monitor.observe_after_offer(_spot_depth(first_u=1, final_u=12))
    assert events == []

    monitor.observe_after_offer(_spot_depth(first_u=18, final_u=19))
    assert events == [
        DepthResyncRequest(
            event="sequence_gap",
            market=Market.SPOT,
            generation=1,
            watermarks=(("BTCUSDT", 18),),
        )
    ]
    assert monitor.state_count == 3


def test_gap_observation_resets_and_precedes_its_resync_request() -> None:
    trace: list[DepthRangeObservation | DepthResyncRequest] = []
    monitor = RawDepthContinuityMonitor(
        _plan(market=Market.SPOT, route="spot"),
        on_range=trace.append,
        on_resync=trace.append,
    )
    monitor.start_generation(1)

    monitor.observe_after_offer(_spot_depth(first_u=10, final_u=12))
    monitor.observe_after_offer(_spot_depth(first_u=14, final_u=15))

    assert trace == [
        DepthRangeObservation(
            market=Market.SPOT,
            symbol="BTCUSDT",
            generation=1,
            U=10,
            u=12,
            reset=True,
        ),
        DepthRangeObservation(
            market=Market.SPOT,
            symbol="BTCUSDT",
            generation=1,
            U=14,
            u=15,
            reset=True,
        ),
        DepthResyncRequest(
            event="sequence_gap",
            market=Market.SPOT,
            generation=1,
            watermarks=(("BTCUSDT", 14),),
        ),
    ]


def test_bootstrap_waits_for_every_depth_stream_and_is_one_shot_per_generation() -> None:
    events: list[DepthResyncRequest] = []
    monitor = _monitor(_plan(market=Market.SPOT, route="spot"), events)
    monitor.start_generation(1)

    monitor.observe_after_offer(_spot_depth(first_u=10, final_u=12))
    monitor.observe_after_offer(_spot_depth(first_u=10, final_u=12))
    monitor.observe_after_offer(
        _spot_depth(first_u=20, final_u=22, symbol="ETHUSDT")
    )
    assert events == []

    monitor.observe_after_offer(
        _spot_depth(first_u=30, final_u=32, symbol="SOLUSDT")
    )
    monitor.observe_after_offer(_spot_depth(first_u=13, final_u=14))

    assert events == [
        DepthResyncRequest(
            event="startup",
            market=Market.SPOT,
            generation=1,
            watermarks=(
                ("BTCUSDT", 10),
                ("ETHUSDT", 20),
                ("SOLUSDT", 30),
            ),
        )
    ]


def test_reconnect_bootstrap_resets_all_fixed_stream_baselines() -> None:
    events: list[DepthResyncRequest] = []
    observations: list[DepthRangeObservation] = []
    monitor = _monitor(
        _plan(market=Market.SPOT, route="spot"),
        events,
        observations,
    )
    monitor.start_generation(1)
    for index, symbol in enumerate(("BTCUSDT", "ETHUSDT", "SOLUSDT"), start=1):
        monitor.observe_after_offer(
            _spot_depth(first_u=index, final_u=index, symbol=symbol)
        )

    monitor.start_generation(2)
    monitor.observe_after_offer(_spot_depth(first_u=100, final_u=100))
    monitor.observe_after_offer(
        _spot_depth(first_u=200, final_u=200, symbol="ETHUSDT")
    )
    assert events == [
        DepthResyncRequest(
            event="startup",
            market=Market.SPOT,
            generation=1,
            watermarks=(
                ("BTCUSDT", 1),
                ("ETHUSDT", 2),
                ("SOLUSDT", 3),
            ),
        )
    ]

    monitor.observe_after_offer(
        _spot_depth(first_u=300, final_u=300, symbol="SOLUSDT")
    )
    monitor.observe_after_offer(_spot_depth(first_u=101, final_u=101))

    assert events == [
        DepthResyncRequest(
            event="startup",
            market=Market.SPOT,
            generation=1,
            watermarks=(
                ("BTCUSDT", 1),
                ("ETHUSDT", 2),
                ("SOLUSDT", 3),
            ),
        ),
        DepthResyncRequest(
            event="reconnect",
            market=Market.SPOT,
            generation=2,
            watermarks=(
                ("BTCUSDT", 100),
                ("ETHUSDT", 200),
                ("SOLUSDT", 300),
            ),
        ),
    ]
    assert [observation.generation for observation in observations] == [1, 1, 1, 2, 2, 2, 2]
    assert [observation.reset for observation in observations] == [
        True,
        True,
        True,
        True,
        True,
        True,
        False,
    ]


def test_futures_monitor_requires_previous_update_link_and_resets_by_generation() -> None:
    events: list[DepthResyncRequest] = []
    monitor = _monitor(_plan(market=Market.FUTURES, route="public"), events)
    monitor.start_generation(1)

    monitor.observe_after_offer(_futures_depth(first_u=100, final_u=105, previous_u=99))
    monitor.observe_after_offer(_futures_depth(first_u=106, final_u=110, previous_u=105))
    monitor.observe_after_offer(_futures_depth(first_u=111, final_u=115, previous_u=109))
    assert events == [
        DepthResyncRequest(
            event="sequence_gap",
            market=Market.FUTURES,
            generation=1,
            watermarks=(("BTCUSDT", 111),),
        )
    ]

    monitor.start_generation(2)
    monitor.observe_after_offer(_futures_depth(first_u=1_000, final_u=1_010, previous_u=777))
    assert events == [
        DepthResyncRequest(
            event="sequence_gap",
            market=Market.FUTURES,
            generation=1,
            watermarks=(("BTCUSDT", 111),),
        )
    ]
    assert monitor.generation == 2


def test_non_depth_authorized_stream_does_not_change_depth_state() -> None:
    events: list[DepthResyncRequest] = []
    monitor = _monitor(_plan(market=Market.SPOT, route="spot"), events)
    monitor.start_generation(1)

    monitor.observe_after_offer(_frame("btcusdt@aggTrade", {"e": "aggTrade", "s": "BTCUSDT"}))

    assert events == []
    assert monitor.state_count == 3


def test_plan_without_depth_never_emits_a_bootstrap() -> None:
    events: list[DepthResyncRequest] = []
    monitor = _monitor(_plan(market=Market.FUTURES, route="market"), events)
    monitor.start_generation(1)

    monitor.observe_after_offer(
        _frame("btcusdt@aggTrade", {"e": "aggTrade", "s": "BTCUSDT"})
    )

    assert monitor.has_depth is False
    assert monitor.state_count == 0
    assert events == []


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("not-json", id="invalid-json"),
        pytest.param("[]", id="non-object-root"),
        pytest.param(
            _frame("bnbusdt@depth@100ms", {}),
            id="unknown-stream",
        ),
        pytest.param(
            _frame("btcusdt@depth@100ms", []),
            id="non-object-data",
        ),
        pytest.param(
            _frame(
                "btcusdt@depth@100ms",
                {"e": "unknown", "s": "BTCUSDT", "U": 1, "u": 2},
            ),
            id="unknown-depth-event",
        ),
        pytest.param(
            _frame(
                "btcusdt@depth@100ms",
                {"e": "depthUpdate", "s": "ETHUSDT", "U": 1, "u": 2},
            ),
            id="symbol",
        ),
        pytest.param(_spot_depth(first_u=3, final_u=2), id="reversed-range"),
        pytest.param(
            _frame(
                "btcusdt@depth@100ms",
                {"e": "depthUpdate", "s": "BTCUSDT", "U": True, "u": 2},
            ),
            id="boolean-sequence",
        ),
    ],
)
def test_spot_monitor_fails_closed_on_malformed_or_unknown_frames(raw: str) -> None:
    monitor = _monitor(_plan(market=Market.SPOT, route="spot"), [])
    monitor.start_generation(1)

    with pytest.raises(DepthSequenceError):
        monitor.observe_after_offer(raw)


def test_futures_monitor_fails_closed_when_pu_is_missing_or_update_regresses() -> None:
    monitor = _monitor(_plan(market=Market.FUTURES, route="public"), [])
    monitor.start_generation(1)
    missing_pu = _frame(
        "btcusdt@depth@100ms",
        {
            "e": "depthUpdate",
            "s": "BTCUSDT",
            "ps": "BTCUSDT",
            "st": 1,
            "U": 1,
            "u": 2,
        },
    )
    with pytest.raises(DepthSequenceError, match="pu"):
        monitor.observe_after_offer(missing_pu)

    monitor.observe_after_offer(_futures_depth(first_u=10, final_u=12, previous_u=9))
    with pytest.raises(DepthSequenceError, match="did not advance"):
        monitor.observe_after_offer(_futures_depth(first_u=10, final_u=12, previous_u=12))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        pytest.param("st", None, "field st", id="missing-st"),
        pytest.param("st", True, "field st", id="boolean-st"),
        pytest.param("st", 2, "field st", id="wrong-st"),
        pytest.param("ps", None, "pair differs", id="missing-pair"),
        pytest.param("ps", "ETHUSDT", "pair differs", id="wrong-pair"),
    ],
)
def test_futures_monitor_requires_routed_public_stream_identity(
    field: str,
    value: object,
    message: str,
) -> None:
    monitor = _monitor(_plan(market=Market.FUTURES, route="public"), [])
    monitor.start_generation(1)
    raw = json.loads(_futures_depth(first_u=1, final_u=2, previous_u=0))
    assert isinstance(raw, dict) and isinstance(raw["data"], dict)
    if value is None:
        raw["data"].pop(field)
    else:
        raw["data"][field] = value

    with pytest.raises(DepthSequenceError, match=message):
        monitor.observe_after_offer(json.dumps(raw))


@pytest.mark.parametrize("generation", [True, 0, -1])
def test_generation_requires_a_strictly_increasing_positive_integer(
    generation: object,
) -> None:
    monitor = _monitor(_plan(market=Market.SPOT, route="spot"), [])
    with pytest.raises(ValueError):
        monitor.start_generation(generation)  # pyright: ignore[reportArgumentType]
    monitor.start_generation(1)
    with pytest.raises(ValueError, match="increase monotonically"):
        monitor.start_generation(1)


def test_binary_depth_frame_requires_utf8() -> None:
    monitor = _monitor(_plan(market=Market.SPOT, route="spot"), [])
    monitor.start_generation(1)
    with pytest.raises(DepthSequenceError, match="UTF-8"):
        monitor.observe_after_offer(b"\xff")
