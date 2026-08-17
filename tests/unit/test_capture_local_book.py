from __future__ import annotations

import json
from collections.abc import Iterable
from decimal import Decimal

import pytest

from signalbot.capture.local_book import (
    LocalBookMaterializer,
    LocalBookReason,
    LocalBookReplayError,
    LocalBookStatus,
    _apply_level_changes_within_bound,
)
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    ConnectionState,
    ConnectionTransitionV1,
    RestEnvelopeV2,
)
from signalbot.domain.enums import Market

_PLAN_SHA = "a" * 64
_BOOT_ID = "boot-1"


def _stream(symbol: str = "BTCUSDT") -> str:
    return f"{symbol.lower()}@depth@100ms"


def _route(market: Market) -> str:
    return "spot" if market is Market.SPOT else "public"


def _transition(
    ingest_seq: int,
    *,
    market: Market = Market.SPOT,
    symbol: str = "BTCUSDT",
    connection_id: str = "depth-g1",
    state: ConnectionState = ConnectionState.CONNECTED,
    monotonic_ns: int = 100,
) -> ConnectionTransitionV1:
    return ConnectionTransitionV1(
        received_at_ms=monotonic_ns,
        received_monotonic_ns=monotonic_ns,
        plan_sha256=_PLAN_SHA,
        process_boot_id=_BOOT_ID,
        connection_id=connection_id,
        ingest_seq=ingest_seq,
        last_frame_seq=max(0, ingest_seq - 1),
        market=market,
        route=_route(market),
        streams=(_stream(symbol),),
        state=state,
        reason=f"test_{state.value}",
    )


def _depth_data(
    *,
    market: Market = Market.SPOT,
    symbol: str = "BTCUSDT",
    first_u: int,
    final_u: int,
    previous_u: int = 0,
    bids: Iterable[object] = (),
    asks: Iterable[object] = (),
) -> dict[str, object]:
    payload: dict[str, object] = {
        "e": "depthUpdate",
        "s": symbol,
        "U": first_u,
        "u": final_u,
        "b": list(bids),
        "a": list(asks),
    }
    if market is Market.FUTURES:
        payload["pu"] = previous_u
        payload["st"] = 1
        payload["ps"] = symbol
    return payload


def _depth(
    ingest_seq: int,
    *,
    market: Market = Market.SPOT,
    symbol: str = "BTCUSDT",
    connection_id: str = "depth-g1",
    first_u: int,
    final_u: int,
    previous_u: int = 0,
    bids: Iterable[object] = (),
    asks: Iterable[object] = (),
    monotonic_ns: int | None = None,
    wall_ms: int | None = None,
    data_override: object | None = None,
    raw_payload_override: str | None = None,
) -> CaptureEnvelopeV1:
    receipt = monotonic_ns if monotonic_ns is not None else ingest_seq * 100
    wall_receipt = wall_ms if wall_ms is not None else receipt
    stream = _stream(symbol)
    data = (
        data_override
        if data_override is not None
        else _depth_data(
            market=market,
            symbol=symbol,
            first_u=first_u,
            final_u=final_u,
            previous_u=previous_u,
            bids=bids,
            asks=asks,
        )
    )
    return CaptureEnvelopeV1(
        received_at_ms=wall_receipt,
        received_monotonic_ns=receipt,
        plan_sha256=_PLAN_SHA,
        process_boot_id=_BOOT_ID,
        connection_id=connection_id,
        frame_seq=ingest_seq,
        ingest_seq=ingest_seq,
        market=market,
        route=_route(market),
        stream=stream,
        subscription_streams=(stream,),
        raw_payload=(
            raw_payload_override
            if raw_payload_override is not None
            else json.dumps({"stream": stream, "data": data}, separators=(",", ":"))
        ),
    )


def _snapshot(
    ingest_seq: int,
    *,
    market: Market = Market.SPOT,
    symbol: str = "BTCUSDT",
    update_id: int,
    bids: Iterable[object] = (),
    asks: Iterable[object] = (),
    request_started_ns: int = 150,
    response_completed_ns: int = 300,
    response_completed_at_ms: int | None = None,
    limit: str | None = None,
    response_status: int = 200,
    raw_payload_override: str | None = None,
) -> RestEnvelopeV2:
    role = "spot_depth_snapshot" if market is Market.SPOT else "futures_depth_snapshot"
    endpoint = "/api/v3/depth" if market is Market.SPOT else "/fapi/v1/depth"
    response_wall = (
        response_completed_at_ms
        if response_completed_at_ms is not None
        else response_completed_ns
    )
    return RestEnvelopeV2(
        request_started_at_ms=request_started_ns,
        request_started_monotonic_ns=request_started_ns,
        response_first_byte_at_ms=response_wall - 1,
        response_first_byte_monotonic_ns=response_completed_ns - 1,
        response_completed_at_ms=response_wall,
        response_completed_monotonic_ns=response_completed_ns,
        plan_sha256=_PLAN_SHA,
        process_boot_id=_BOOT_ID,
        request_role=role,
        correlation_id=f"snapshot-{ingest_seq}",
        attempt=1,
        ingest_seq=ingest_seq,
        market=market,
        endpoint_path=endpoint,
        canonical_query=(
            ("limit", limit or ("5000" if market is Market.SPOT else "1000")),
            ("symbol", symbol),
        ),
        response_status=response_status,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        raw_payload=(
            raw_payload_override
            if raw_payload_override is not None
            else json.dumps(
                {"lastUpdateId": update_id, "bids": list(bids), "asks": list(asks)},
                separators=(",", ":"),
            )
        ),
    )


def _spot_book(*, max_levels_per_side: int = 10_000) -> LocalBookMaterializer:
    # Legacy fixture prices use one-dollar jumps around 100. Keep this fixture's
    # guard intentionally wide; production defaults and dedicated boundary tests
    # exercise the sealed 10/20 bp representation.
    materializer = LocalBookMaterializer(
        max_levels_per_side=max_levels_per_side,
        guard_band_bps=2_000,
    )
    materializer.process(_transition(1))
    materializer.process(
        _depth(
            2,
            first_u=100,
            final_u=102,
            bids=(("99.00", "2.50"), ("97", "4")),
            asks=(("101", "0"), ("102", "5")),
        )
    )
    materializer.process(
        _snapshot(
            3,
            update_id=101,
            bids=(("99.00", "1.0"), ("98", "3")),
            asks=(("101", "1"),),
        )
    )
    return materializer


def test_spot_bridge_applies_exact_decimals_live_upsert_and_delete() -> None:
    materializer = _spot_book()

    bridged = materializer.view(Market.SPOT, "BTCUSDT")
    assert bridged.valid
    assert bridged.update_id == 102
    assert bridged.best_bid == (Decimal("99.00"), Decimal("2.50"))
    assert bridged.best_ask == (Decimal("102"), Decimal("5"))
    assert bridged.availability_receipt_monotonic_ns == 300
    assert not bridged.crossed

    materializer.process(
        _depth(
            4,
            first_u=103,
            final_u=103,
            bids=(("99", "0"), ("100", "7.000")),
            asks=(("102", "0"), ("103", "8")),
        )
    )
    current = materializer.view(Market.SPOT, "BTCUSDT")
    assert current.valid
    assert current.update_id == 103
    assert current.best_bid == (Decimal("100"), Decimal("7.000"))
    assert current.best_ask == (Decimal("103"), Decimal("8"))
    assert current.availability_receipt_monotonic_ns == 400
    assert materializer.metrics.depth_events_applied == 2


def test_spot_bootstrap_accepts_snapshot_successor_inside_first_remaining_range() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1))
    materializer.process(_depth(2, first_u=102, final_u=103))
    materializer.process(_snapshot(3, update_id=101))

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.valid
    assert view.reason is LocalBookReason.VALID
    assert view.update_id == 103
    assert view.buffered_event_count == 0
    assert materializer.metrics.stale_snapshots_rejected == 0


def test_offline_bridge_accepts_failed_smoke_spot_successor() -> None:
    last_update_id = 78_896_562_817
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, symbol="ETHUSDT"))
    materializer.process(
        _depth(
            2,
            symbol="ETHUSDT",
            first_u=78_896_562_791,
            final_u=last_update_id,
        )
    )
    materializer.process(
        _depth(
            3,
            symbol="ETHUSDT",
            first_u=last_update_id + 1,
            final_u=78_896_562_832,
        )
    )
    materializer.process(
        _snapshot(
            4,
            symbol="ETHUSDT",
            update_id=last_update_id,
        )
    )

    view = materializer.view(Market.SPOT, "ETHUSDT")
    assert view.valid
    assert view.update_id == 78_896_562_832
    assert view.buffered_event_count == 0
    assert materializer.metrics.old_events_ignored == 1


@pytest.mark.parametrize(
    ("market", "stale_update_id", "accepted_update_id"),
    [
        pytest.param(Market.SPOT, 100, 101, id="spot-targets-successor"),
        pytest.param(Market.FUTURES, 101, 102, id="futures-targets-snapshot-id"),
    ],
)
def test_stale_candidate_preserves_full_buffer_for_in_flight_retry(
    market: Market,
    stale_update_id: int,
    accepted_update_id: int,
) -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, market=market))
    old = _depth(
        2,
        market=market,
        first_u=90,
        final_u=100,
        previous_u=89,
        monotonic_ns=200,
    )
    bridge = _depth(
        3,
        market=market,
        first_u=102,
        final_u=103,
        previous_u=100,
        monotonic_ns=250,
    )
    materializer.process(old)
    materializer.process(bridge)
    original_bytes = len(old.raw_payload.encode()) + len(bridge.raw_payload.encode())

    materializer.process(
        _snapshot(
            4,
            market=market,
            update_id=stale_update_id,
            request_started_ns=150,
            response_completed_ns=300,
        )
    )
    stale = materializer.view(market, "BTCUSDT")
    assert stale.reason is LocalBookReason.STALE_SNAPSHOT
    assert stale.buffered_event_count == 2
    assert stale.buffered_bytes == original_bytes
    assert stale.pending_snapshot_update_id is None
    assert stale.generation == 1
    assert materializer.metrics.old_events_ignored == 0

    # This retry began before the stale response completed.  Rejecting only the
    # candidate (without advancing the barrier) lets it bridge the same buffer.
    materializer.process(
        _snapshot(
            5,
            market=market,
            update_id=accepted_update_id,
            request_started_ns=200,
            response_completed_ns=400,
        )
    )
    valid = materializer.view(market, "BTCUSDT")
    assert valid.valid
    assert valid.update_id == 103
    assert valid.buffered_event_count == 0
    assert valid.generation == 1
    assert materializer.metrics.stale_snapshots_rejected == 1
    assert materializer.metrics.old_events_ignored == 1


def test_spot_discards_old_bootstrap_events_then_bridges_first_remaining_event() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1))
    materializer.process(_depth(2, first_u=90, final_u=100))
    materializer.process(_depth(3, first_u=100, final_u=102))
    materializer.process(_snapshot(4, update_id=101))

    assert materializer.view(Market.SPOT, "BTCUSDT").valid
    assert materializer.metrics.old_events_ignored == 1


def test_spot_live_ignores_old_range_and_fails_closed_on_gap() -> None:
    materializer = _spot_book()
    materializer.process(_depth(4, first_u=1, final_u=101))
    assert materializer.view(Market.SPOT, "BTCUSDT").update_id == 102

    materializer.process(_depth(5, first_u=104, final_u=105))
    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.awaiting_snapshot
    assert view.reason is LocalBookReason.SEQUENCE_GAP
    assert view.update_id is None
    assert view.buffered_event_count == 1
    assert materializer.metrics.old_events_ignored == 1
    assert materializer.metrics.sequence_gaps == 1


def test_spot_live_equal_u_applies_absolute_quantity_update() -> None:
    materializer = _spot_book()
    materializer.process(
        _depth(
            4,
            first_u=100,
            final_u=102,
            bids=(("99", "9.25"),),
        )
    )

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.valid
    assert view.update_id == 102
    assert view.best_bid == (Decimal("99"), Decimal("9.25"))
    assert materializer.metrics.old_events_ignored == 0
    assert materializer.metrics.depth_events_applied == 2


def test_spot_gap_event_bridges_when_snapshot_id_is_inside_range_below_u() -> None:
    materializer = _spot_book()
    materializer.process(_depth(4, first_u=104, final_u=105, monotonic_ns=400))
    gap = materializer.view(Market.SPOT, "BTCUSDT")
    assert gap.reason is LocalBookReason.SEQUENCE_GAP
    assert gap.buffered_event_count == 1

    materializer.process(
        _snapshot(
            5,
            update_id=104,
            request_started_ns=401,
            response_completed_ns=500,
        )
    )
    materializer.process(_depth(6, first_u=106, final_u=106, monotonic_ns=600))

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.valid
    assert view.update_id == 106
    assert view.buffered_event_count == 0
    assert materializer.metrics.sequence_gaps == 1


def test_spot_gap_event_equal_u_is_discarded_then_successor_bridges() -> None:
    materializer = _spot_book()
    materializer.process(_depth(4, first_u=104, final_u=105, monotonic_ns=400))
    assert materializer.view(Market.SPOT, "BTCUSDT").buffered_event_count == 1

    materializer.process(
        _snapshot(
            5,
            update_id=105,
            request_started_ns=401,
            response_completed_ns=500,
        )
    )
    pending = materializer.view(Market.SPOT, "BTCUSDT")
    assert pending.awaiting_snapshot
    assert pending.reason is LocalBookReason.SNAPSHOT_PENDING_FIRST_EVENT
    assert pending.buffered_event_count == 0

    materializer.process(_depth(6, first_u=106, final_u=106, monotonic_ns=600))
    valid = materializer.view(Market.SPOT, "BTCUSDT")
    assert valid.valid
    assert valid.reason is LocalBookReason.VALID
    assert valid.update_id == 106
    assert materializer.metrics.stale_snapshots_rejected == 0


def test_availability_receipts_remain_observed_wall_monotonic_pairs() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1))
    materializer.process(
        _depth(
            2,
            first_u=100,
            final_u=102,
            monotonic_ns=200,
            wall_ms=1_000,
        )
    )
    materializer.process(
        _snapshot(
            3,
            update_id=101,
            response_completed_ns=300,
            response_completed_at_ms=900,
        )
    )
    snapshot_pair = materializer.view(Market.SPOT, "BTCUSDT")
    assert (
        snapshot_pair.availability_receipt_at_ms,
        snapshot_pair.availability_receipt_monotonic_ns,
    ) == (900, 300)

    materializer.process(
        _depth(
            4,
            first_u=103,
            final_u=103,
            monotonic_ns=400,
            wall_ms=800,
        )
    )
    later_pair = materializer.view(Market.SPOT, "BTCUSDT")
    assert (
        later_pair.availability_receipt_at_ms,
        later_pair.availability_receipt_monotonic_ns,
    ) == (800, 400)

    materializer.process(
        _depth(
            5,
            first_u=104,
            final_u=104,
            monotonic_ns=400,
            wall_ms=700,
        )
    )
    tied_pair = materializer.view(Market.SPOT, "BTCUSDT")
    assert (
        tied_pair.availability_receipt_at_ms,
        tied_pair.availability_receipt_monotonic_ns,
    ) == (800, 400)


def test_buffered_gap_never_exposes_or_counts_a_transient_valid_book() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1))
    materializer.process(_depth(2, first_u=100, final_u=102, monotonic_ns=200))
    materializer.process(_depth(3, first_u=104, final_u=105, monotonic_ns=300))
    materializer.process(
        _snapshot(
            4,
            update_id=101,
            request_started_ns=150,
            response_completed_ns=300,
        )
    )

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.reason is LocalBookReason.SEQUENCE_GAP
    assert not view.valid
    assert materializer.metrics.snapshots_applied == 0
    assert materializer.metrics.books_became_valid == 0

    materializer.process(
        _snapshot(
            5,
            update_id=104,
            request_started_ns=299,
            response_completed_ns=500,
        )
    )
    assert materializer.view(Market.SPOT, "BTCUSDT").reason is LocalBookReason.STALE_SNAPSHOT
    assert materializer.metrics.stale_snapshots_rejected == 1


def test_spot_bootstrap_gap_preserves_already_counted_buffer_suffix() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1))
    first = _depth(2, first_u=100, final_u=102, monotonic_ns=200)
    gap = _depth(3, first_u=104, final_u=105, monotonic_ns=300)
    after_gap = _depth(4, first_u=106, final_u=107, monotonic_ns=400)
    for event in (first, gap, after_gap):
        materializer.process(event)
    suffix_bytes = len(gap.raw_payload.encode()) + len(after_gap.raw_payload.encode())

    materializer.process(
        _snapshot(
            5,
            update_id=101,
            request_started_ns=150,
            response_completed_ns=500,
        )
    )
    invalid = materializer.view(Market.SPOT, "BTCUSDT")
    assert invalid.reason is LocalBookReason.SEQUENCE_GAP
    assert invalid.buffered_event_count == 2
    assert invalid.buffered_bytes == suffix_bytes
    assert materializer.metrics.depth_events_buffered == 3
    assert materializer.metrics.snapshots_applied == 0

    materializer.process(
        _snapshot(
            6,
            update_id=104,
            request_started_ns=501,
            response_completed_ns=600,
        )
    )
    valid = materializer.view(Market.SPOT, "BTCUSDT")
    assert valid.valid
    assert valid.update_id == 107
    assert valid.buffered_event_count == 0
    assert materializer.metrics.depth_events_buffered == 3
    assert materializer.metrics.snapshots_applied == 1


def test_futures_bootstrap_ignores_first_pu_then_requires_previous_u() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, market=Market.FUTURES))
    materializer.process(
        _depth(
            2,
            market=Market.FUTURES,
            first_u=100,
            final_u=105,
            previous_u=7,
            bids=(("99", "1"),),
        )
    )
    materializer.process(
        _snapshot(
            3,
            market=Market.FUTURES,
            update_id=102,
            bids=(("98", "1"),),
            asks=(("101", "1"),),
        )
    )
    materializer.process(
        _depth(
            4,
            market=Market.FUTURES,
            first_u=106,
            final_u=110,
            previous_u=105,
        )
    )
    assert materializer.view(Market.FUTURES, "BTCUSDT").update_id == 110

    materializer.process(
        _depth(
            5,
            market=Market.FUTURES,
            first_u=111,
            final_u=115,
            previous_u=109,
        )
    )
    view = materializer.view(Market.FUTURES, "BTCUSDT")
    assert view.awaiting_snapshot
    assert view.reason is LocalBookReason.SEQUENCE_GAP
    assert view.buffered_event_count == 1
    assert materializer.metrics.sequence_gaps == 1


def test_futures_snapshot_before_first_u_is_classified_stale() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, market=Market.FUTURES))
    materializer.process(
        _depth(
            2,
            market=Market.FUTURES,
            first_u=102,
            final_u=103,
            previous_u=101,
        )
    )
    materializer.process(_snapshot(3, market=Market.FUTURES, update_id=101))

    view = materializer.view(Market.FUTURES, "BTCUSDT")
    assert view.awaiting_snapshot
    assert view.reason is LocalBookReason.STALE_SNAPSHOT
    assert materializer.metrics.stale_snapshots_rejected == 1


def test_futures_gap_event_anchors_resnapshot_and_next_pu_link() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, market=Market.FUTURES))
    materializer.process(
        _depth(
            2,
            market=Market.FUTURES,
            first_u=100,
            final_u=105,
            previous_u=7,
        )
    )
    materializer.process(_snapshot(3, market=Market.FUTURES, update_id=102))
    materializer.process(
        _depth(
            4,
            market=Market.FUTURES,
            first_u=106,
            final_u=110,
            previous_u=104,
            monotonic_ns=400,
        )
    )
    gap = materializer.view(Market.FUTURES, "BTCUSDT")
    assert gap.reason is LocalBookReason.SEQUENCE_GAP
    assert gap.buffered_event_count == 1

    materializer.process(
        _snapshot(
            5,
            market=Market.FUTURES,
            update_id=110,
            request_started_ns=401,
            response_completed_ns=500,
        )
    )
    materializer.process(
        _depth(
            6,
            market=Market.FUTURES,
            first_u=111,
            final_u=115,
            previous_u=110,
            monotonic_ns=600,
        )
    )

    view = materializer.view(Market.FUTURES, "BTCUSDT")
    assert view.valid
    assert view.update_id == 115
    assert materializer.metrics.sequence_gaps == 1


def test_futures_bootstrap_gap_preserves_already_counted_buffer_suffix() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, market=Market.FUTURES))
    first = _depth(
        2,
        market=Market.FUTURES,
        first_u=100,
        final_u=105,
        previous_u=7,
        monotonic_ns=200,
    )
    gap = _depth(
        3,
        market=Market.FUTURES,
        first_u=106,
        final_u=110,
        previous_u=104,
        monotonic_ns=300,
    )
    after_gap = _depth(
        4,
        market=Market.FUTURES,
        first_u=111,
        final_u=115,
        previous_u=110,
        monotonic_ns=400,
    )
    for event in (first, gap, after_gap):
        materializer.process(event)
    suffix_bytes = len(gap.raw_payload.encode()) + len(after_gap.raw_payload.encode())

    materializer.process(
        _snapshot(
            5,
            market=Market.FUTURES,
            update_id=102,
            request_started_ns=150,
            response_completed_ns=500,
        )
    )
    invalid = materializer.view(Market.FUTURES, "BTCUSDT")
    assert invalid.reason is LocalBookReason.SEQUENCE_GAP
    assert invalid.buffered_event_count == 2
    assert invalid.buffered_bytes == suffix_bytes
    assert materializer.metrics.depth_events_buffered == 3

    materializer.process(
        _snapshot(
            6,
            market=Market.FUTURES,
            update_id=108,
            request_started_ns=501,
            response_completed_ns=600,
        )
    )
    valid = materializer.view(Market.FUTURES, "BTCUSDT")
    assert valid.valid
    assert valid.update_id == 115
    assert valid.buffered_event_count == 0
    assert materializer.metrics.depth_events_buffered == 3
    assert materializer.metrics.snapshots_applied == 1


def test_snapshot_started_before_current_generation_is_rejected() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, connection_id="depth-g1", monotonic_ns=100))
    materializer.process(
        _depth(2, connection_id="depth-g1", first_u=100, final_u=102, monotonic_ns=200)
    )
    materializer.process(_transition(3, connection_id="depth-g2", monotonic_ns=500))
    materializer.process(
        _depth(4, connection_id="depth-g2", first_u=200, final_u=202, monotonic_ns=600)
    )
    materializer.process(
        _snapshot(
            5,
            update_id=201,
            request_started_ns=150,
            response_completed_ns=650,
        )
    )
    stale = materializer.view(Market.SPOT, "BTCUSDT")
    assert stale.awaiting_snapshot
    assert stale.reason is LocalBookReason.STALE_SNAPSHOT

    # Equality is the admitted boundary: only requests strictly before it are stale.
    materializer.process(
        _snapshot(
            6,
            update_id=201,
            request_started_ns=500,
            response_completed_ns=700,
        )
    )
    current = materializer.view(Market.SPOT, "BTCUSDT")
    assert current.valid
    assert current.generation == 2
    assert current.connection_id == "depth-g2"
    assert materializer.metrics.stale_snapshots_rejected == 1


def test_duplicate_connected_transition_does_not_create_a_new_generation() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, connection_id="depth-g1", monotonic_ns=100))
    materializer.process(
        _depth(2, connection_id="depth-g1", first_u=1, final_u=2, monotonic_ns=200)
    )
    materializer.process(_transition(3, connection_id="depth-g1", monotonic_ns=300))

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.awaiting_snapshot
    assert view.reason is LocalBookReason.CONNECTION_PROTOCOL_ERROR
    assert view.generation == 1
    assert view.buffered_event_count == 0
    assert materializer.metrics.malformed_records == 1


def test_disconnect_invalidates_and_clears_the_current_book() -> None:
    materializer = _spot_book()
    materializer.process(
        _transition(
            4,
            connection_id="depth-g1",
            state=ConnectionState.DISCONNECTED,
            monotonic_ns=400,
        )
    )

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.status is LocalBookStatus.DISCONNECTED
    assert view.reason is LocalBookReason.DISCONNECTED
    assert not view.valid
    assert view.update_id is None
    assert view.bids == ()
    assert view.asks == ()


@pytest.mark.parametrize(
    "bad_bids",
    [
        pytest.param((("99", "1"), ("99.0", "2")), id="duplicate-price"),
        pytest.param((("NaN", "1"),), id="nonfinite-price"),
        pytest.param((("99", "Infinity"),), id="nonfinite-quantity"),
        pytest.param((("99", "1", "extra"),), id="non-pair-level"),
    ],
)
def test_malformed_duplicate_or_nonfinite_depth_event_fails_closed(
    bad_bids: tuple[object, ...],
) -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1))
    materializer.process(_depth(2, first_u=1, final_u=2, bids=bad_bids))

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.awaiting_snapshot
    assert view.reason is LocalBookReason.MALFORMED_EVENT
    assert view.buffered_event_count == 0
    assert materializer.metrics.malformed_records == 1


@pytest.mark.parametrize(
    "bad_bids",
    [
        pytest.param((("99", "1"), ("99.0", "2")), id="duplicate-price"),
        pytest.param((("NaN", "1"),), id="nonfinite-price"),
    ],
)
def test_malformed_snapshot_is_not_partially_installed(
    bad_bids: tuple[object, ...],
) -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1))
    materializer.process(_snapshot(2, update_id=10, bids=bad_bids))

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.awaiting_snapshot
    assert view.reason is LocalBookReason.MALFORMED_SNAPSHOT
    assert view.pending_snapshot_update_id is None
    assert view.bids == ()


def test_partial_content_depth_snapshot_is_not_admitted() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1))
    materializer.process(_depth(2, first_u=10, final_u=12))
    materializer.process(
        _snapshot(
            3,
            update_id=11,
            response_status=206,
        )
    )

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.awaiting_snapshot
    assert view.reason is LocalBookReason.SNAPSHOT_REQUEST_FAILED
    assert view.update_id is None
    assert view.buffered_event_count == 1
    assert materializer.metrics.snapshot_request_failures == 1


def test_event_count_and_byte_buffers_fail_closed_without_eviction() -> None:
    event_limited = LocalBookMaterializer(max_buffered_events_per_book=1)
    event_limited.process(_transition(1))
    event_limited.process(_depth(2, first_u=1, final_u=2))
    event_limited.process(_depth(3, first_u=3, final_u=4))
    event_view = event_limited.view(Market.SPOT, "BTCUSDT")
    assert event_view.reason is LocalBookReason.BUFFER_OVERFLOW
    assert event_view.buffered_event_count == 0
    assert event_limited.metrics.buffer_overflows == 1

    byte_limited = LocalBookMaterializer(max_buffered_bytes_per_book=1)
    byte_limited.process(_transition(1))
    byte_limited.process(_depth(2, first_u=1, final_u=2))
    byte_view = byte_limited.view(Market.SPOT, "BTCUSDT")
    assert byte_view.reason is LocalBookReason.BUFFER_OVERFLOW
    assert byte_view.buffered_bytes == 0
    assert byte_limited.metrics.buffer_overflows == 1


def test_feature_guard_band_prunes_far_levels_instead_of_hitting_side_cap() -> None:
    materializer = LocalBookMaterializer(max_levels_per_side=3)
    materializer.process(_transition(1))
    materializer.process(_depth(2, first_u=100, final_u=102))
    materializer.process(
        _snapshot(
            3,
            update_id=101,
            bids=(
                ("100", "1"),
                ("99.95", "2"),
                ("99.85", "3"),
                ("99.8", "4"),
                ("90", "5"),
            ),
            asks=(
                ("100.1", "1"),
                ("100.2", "2"),
                ("100.25", "3"),
                ("100.3", "4"),
                ("110", "5"),
            ),
        )
    )

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.valid
    assert view.feature_band_bps == 10
    assert view.guard_band_bps == 20
    assert view.retained_level_capacity_per_side == 3
    assert view.feature_band_complete
    assert tuple(price for price, _quantity in view.bids) == (
        Decimal("100"),
        Decimal("99.95"),
        Decimal("99.85"),
    )
    assert tuple(price for price, _quantity in view.asks) == (
        Decimal("100.1"),
        Decimal("100.2"),
        Decimal("100.25"),
    )
    assert view.bid_retained_floor == Decimal("99.85")
    assert view.ask_retained_ceiling == Decimal("100.25")
    assert not view.resnapshot_required
    assert materializer.metrics.level_overflows == 0


def test_dense_guard_band_fails_closed_then_fresh_snapshot_rebridges() -> None:
    materializer = LocalBookMaterializer(max_levels_per_side=2)
    materializer.process(_transition(1))
    materializer.process(_depth(2, first_u=100, final_u=102))
    materializer.process(
        _snapshot(
            3,
            update_id=101,
            bids=(("100", "1"), ("99.95", "1"), ("99.9", "1")),
            asks=(("100.1", "1"),),
        )
    )

    failed = materializer.view(Market.SPOT, "BTCUSDT")
    assert failed.awaiting_snapshot
    assert failed.reason is LocalBookReason.LEVEL_OVERFLOW
    assert failed.resnapshot_required
    assert failed.gap_reason is LocalBookReason.LEVEL_OVERFLOW
    assert (failed.gap_first_update_id, failed.gap_last_update_id) == (100, 102)
    assert failed.buffered_event_count == 1
    assert failed.bids == ()

    materializer.process(
        _snapshot(
            4,
            update_id=101,
            bids=(("100", "1"), ("99.9", "1")),
            asks=(("100.1", "1"),),
            request_started_ns=300,
            response_completed_ns=400,
        )
    )
    recovered = materializer.view(Market.SPOT, "BTCUSDT")
    assert recovered.valid
    assert recovered.feature_band_complete
    assert not recovered.resnapshot_required
    assert recovered.gap_reason is None
    assert recovered.buffered_event_count == 0
    assert materializer.metrics.level_overflows == 1


def test_guard_exhaustion_rebuffers_trigger_and_resnapshot_rebridges() -> None:
    materializer = LocalBookMaterializer(max_levels_per_side=3)
    materializer.process(_transition(1))
    materializer.process(_depth(2, first_u=100, final_u=102))
    materializer.process(
        _snapshot(
            3,
            update_id=101,
            bids=(("100", "1"), ("99.9", "1"), ("99.8", "1")),
            asks=(("100.1", "1"), ("100.2", "1")),
        )
    )
    materializer.process(
        _depth(4, first_u=103, final_u=103, bids=(("100", "0"),))
    )
    assert materializer.view(Market.SPOT, "BTCUSDT").valid

    materializer.process(
        _depth(5, first_u=104, final_u=104, bids=(("99.9", "0"),))
    )
    exhausted = materializer.view(Market.SPOT, "BTCUSDT")
    assert exhausted.awaiting_snapshot
    assert exhausted.reason is LocalBookReason.LEVEL_OVERFLOW
    assert exhausted.resnapshot_required
    assert exhausted.buffered_event_count == 1
    assert exhausted.buffered_level_changes == 1
    assert (exhausted.gap_first_update_id, exhausted.gap_last_update_id) == (104, 104)

    materializer.process(
        _snapshot(
            6,
            update_id=103,
            bids=(("99.8", "1"), ("99.7", "1")),
            asks=(("100.1", "1"),),
            request_started_ns=501,
            response_completed_ns=600,
        )
    )
    recovered = materializer.view(Market.SPOT, "BTCUSDT")
    assert recovered.valid
    assert recovered.update_id == 104
    assert recovered.best_bid == (Decimal("99.8"), Decimal("1"))
    assert recovered.gap_reason is None
    assert not recovered.resnapshot_required


def test_repeated_decoded_buffer_overflow_keeps_exact_bounded_gap_state() -> None:
    materializer = LocalBookMaterializer(
        max_buffered_events_per_book=100,
        max_buffered_level_changes_per_book=2,
    )
    materializer.process(_transition(1))
    events = (
        _depth(
            2,
            first_u=1,
            final_u=2,
            bids=(("100", "1"),),
            asks=(("101", "1"),),
        ),
        _depth(
            3,
            first_u=3,
            final_u=4,
            bids=(("99", "1"),),
            asks=(("102", "1"),),
        ),
        _depth(
            4,
            first_u=5,
            final_u=6,
            bids=(("98", "1"),),
            asks=(("103", "1"),),
        ),
        _depth(
            5,
            first_u=7,
            final_u=8,
            bids=(("97", "1"),),
            asks=(("104", "1"),),
        ),
    )
    for event in events:
        materializer.process(event)
        current = materializer.view(Market.SPOT, "BTCUSDT")
        assert current.buffered_event_count <= 1
        assert current.buffered_level_changes <= 2

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.reason is LocalBookReason.BUFFER_OVERFLOW
    assert view.gap_reason is LocalBookReason.BUFFER_OVERFLOW
    assert (view.gap_first_update_id, view.gap_last_update_id) == (1, 8)
    assert view.buffered_event_count == 0
    assert view.buffered_bytes == 0
    assert view.buffered_level_changes == 0
    assert materializer.metrics.buffer_overflows == 2


def test_far_live_updates_cannot_grow_retained_representation() -> None:
    materializer = LocalBookMaterializer(max_levels_per_side=3)
    materializer.process(_transition(1))
    materializer.process(_depth(2, first_u=100, final_u=102))
    materializer.process(
        _snapshot(
            3,
            update_id=101,
            bids=(("100", "1"), ("99.9", "1"), ("99.8", "1")),
            asks=(("100.1", "1"), ("100.2", "1"), ("100.3", "1")),
        )
    )
    for ingest_seq in range(4, 204):
        update_id = 99 + ingest_seq
        materializer.process(
            _depth(
                ingest_seq,
                first_u=update_id,
                final_u=update_id,
                bids=((str(90 - ingest_seq / 1_000), "1"),),
                asks=((str(110 + ingest_seq / 1_000), "1"),),
            )
        )
        coverage = materializer.coverage_state(Market.SPOT, "BTCUSDT")
        assert coverage.bid_level_count <= 3
        assert coverage.ask_level_count <= 3

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.valid
    assert view.feature_band_complete
    assert len(view.bids) == 3
    assert len(view.asks) == 3
    assert view.buffered_event_count == 0
    assert materializer.metrics.level_overflows == 0


def test_maximum_snapshot_must_reach_the_full_10bp_feature_band() -> None:
    materializer = LocalBookMaterializer(max_levels_per_side=2_000)
    materializer.process(_transition(1, market=Market.FUTURES))
    materializer.process(
        _depth(
            2,
            market=Market.FUTURES,
            first_u=100,
            final_u=102,
            previous_u=99,
        )
    )
    dense_bids = tuple(
        (f"{100 - index * 0.00001:.5f}", "1") for index in range(1_000)
    )
    materializer.process(
        _snapshot(
            3,
            market=Market.FUTURES,
            update_id=101,
            bids=dense_bids,
        )
    )

    view = materializer.view(Market.FUTURES, "BTCUSDT")
    assert view.awaiting_snapshot
    assert view.reason is LocalBookReason.LEVEL_OVERFLOW
    assert not view.bid_feature_band_complete
    assert view.resnapshot_required
    assert view.buffered_event_count == 1


def test_snapshot_and_live_level_bounds_fail_closed_without_partial_book() -> None:
    snapshot_limited = LocalBookMaterializer(max_levels_per_side=1)
    snapshot_limited.process(_transition(1))
    snapshot_limited.process(
        _snapshot(2, update_id=10, bids=(("99", "1"), ("98.99", "1")))
    )
    snapshot_view = snapshot_limited.view(Market.SPOT, "BTCUSDT")
    assert snapshot_view.reason is LocalBookReason.LEVEL_OVERFLOW
    assert snapshot_view.bids == ()
    assert snapshot_limited.metrics.level_overflows == 1

    live_limited = LocalBookMaterializer(max_levels_per_side=1)
    live_limited.process(_transition(1))
    live_limited.process(_depth(2, first_u=10, final_u=12, bids=(("99", "2"),)))
    live_limited.process(
        _snapshot(
            3,
            update_id=11,
            bids=(("99", "1"),),
            asks=(("101", "1"),),
        )
    )
    assert live_limited.view(Market.SPOT, "BTCUSDT").valid
    live_limited.process(
        _depth(4, first_u=13, final_u=13, bids=(("98.99", "1"),))
    )
    live_view = live_limited.view(Market.SPOT, "BTCUSDT")
    assert live_view.reason is LocalBookReason.LEVEL_OVERFLOW
    assert live_view.bids == ()
    assert live_limited.metrics.level_overflows == 1


def test_in_place_level_preflight_is_atomic_when_either_side_exceeds_bound() -> None:
    bids = {Decimal("99"): Decimal("1")}
    asks = {Decimal("101"): Decimal("2")}
    original_bids = bids.copy()
    original_asks = asks.copy()

    applied = _apply_level_changes_within_bound(
        bids,
        asks,
        ((Decimal("98"), Decimal("3")),),
        ((Decimal("101"), Decimal("0")),),
        1,
    )

    assert not applied
    assert bids == original_bids
    assert asks == original_asks


def test_event_level_bound_is_counted_once_and_invalidates_only_its_book() -> None:
    materializer = LocalBookMaterializer(max_level_changes_per_event=1)
    materializer.process(_transition(1))
    materializer.process(
        _depth(
            2,
            first_u=1,
            final_u=2,
            bids=(("99", "1"), ("98", "1")),
        )
    )

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.reason is LocalBookReason.LEVEL_OVERFLOW
    assert materializer.metrics.level_overflows == 1
    assert materializer.metrics.malformed_records == 1


@pytest.mark.parametrize(
    ("market", "limit"),
    [
        pytest.param(Market.SPOT, "1000", id="spot-not-5000"),
        pytest.param(Market.FUTURES, "500", id="futures-not-1000"),
    ],
)
def test_snapshot_limit_must_equal_the_frozen_venue_value(
    market: Market,
    limit: str,
) -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, market=market))
    materializer.process(_snapshot(2, market=market, update_id=10, limit=limit))

    assert materializer.metrics.malformed_records == 1
    assert materializer.view(market, "BTCUSDT").awaiting_snapshot


@pytest.mark.parametrize(
    "mutation",
    [
        pytest.param({"st": True}, id="boolean-stream-type"),
        pytest.param({"st": 2}, id="wrong-stream-type"),
        pytest.param({"ps": "ETHUSDT"}, id="wrong-pair-symbol"),
        pytest.param({"pu": None}, id="missing-previous-update-id"),
    ],
)
def test_futures_requires_current_routed_identity_and_sequence_fields(
    mutation: dict[str, object],
) -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, market=Market.FUTURES))
    data = _depth_data(
        market=Market.FUTURES,
        first_u=1,
        final_u=2,
        previous_u=0,
    )
    data.update(mutation)
    materializer.process(
        _depth(
            2,
            market=Market.FUTURES,
            first_u=1,
            final_u=2,
            data_override=data,
        )
    )

    view = materializer.view(Market.FUTURES, "BTCUSDT")
    assert view.reason is LocalBookReason.MALFORMED_EVENT
    assert materializer.metrics.malformed_records == 1


def test_ingest_gap_fails_the_whole_fixed_replay() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1))

    with pytest.raises(LocalBookReplayError, match="expected 2, received 3"):
        materializer.process(_depth(3, first_u=1, final_u=2))

    assert materializer.metrics.records_processed == 1
    assert materializer.metrics.ingest_gaps == 1
    assert len(materializer.views) == 6
    assert all(view.status is LocalBookStatus.REPLAY_FAILED for view in materializer.views)
    assert all(view.reason is LocalBookReason.INGEST_GAP for view in materializer.views)


def test_snapshot_before_first_delta_waits_then_bridges() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, monotonic_ns=100))
    materializer.process(
        _snapshot(
            2,
            update_id=101,
            bids=(("99", "1"),),
            asks=(("101", "1"),),
            request_started_ns=100,
            response_completed_ns=300,
        )
    )
    pending = materializer.view(Market.SPOT, "BTCUSDT")
    assert pending.awaiting_snapshot
    assert pending.pending_snapshot_update_id == 101
    assert pending.update_id is None

    materializer.process(
        _depth(
            3,
            first_u=100,
            final_u=102,
            bids=(("100", "2"),),
            monotonic_ns=400,
        )
    )
    current = materializer.view(Market.SPOT, "BTCUSDT")
    assert current.valid
    assert current.update_id == 102
    assert current.availability_receipt_monotonic_ns == 400
    assert current.pending_snapshot_update_id is None


def test_crossed_state_is_exposed_without_fabricating_execution_eligibility() -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1))
    materializer.process(
        _depth(
            2,
            first_u=10,
            final_u=12,
            bids=(("102", "1"),),
            asks=(("100", "1"),),
        )
    )
    materializer.process(
        _snapshot(
            3,
            update_id=11,
            bids=(("99", "1"),),
            asks=(("101", "1"),),
        )
    )

    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.valid
    assert view.crossed
    assert view.best_bid is not None and view.best_bid[0] == Decimal("102")
    assert view.best_ask is not None and view.best_ask[0] == Decimal("100")


@pytest.mark.parametrize("value", [True, 0, -1])
def test_materializer_bounds_require_positive_non_boolean_integers(value: object) -> None:
    with pytest.raises(ValueError):
        LocalBookMaterializer(
            max_buffered_events_per_book=value,  # pyright: ignore[reportArgumentType]
        )


def test_view_rejects_dynamic_symbols() -> None:
    materializer = LocalBookMaterializer()
    with pytest.raises(ValueError, match="fixed canary"):
        materializer.view(Market.SPOT, "BNBUSDT")


def test_no_sort_coverage_state_tracks_cached_bbo_and_affected_book() -> None:
    materializer = _spot_book()

    initial = materializer.coverage_state(Market.SPOT, "BTCUSDT")
    assert initial.bid_level_count == 3
    assert initial.ask_level_count == 1
    assert initial.has_bid is True
    assert initial.has_ask is True
    assert initial.crossed_or_locked is False

    crossed = materializer.process(
        _depth(
            4,
            first_u=103,
            final_u=103,
            bids=(("103", "1"),),
        )
    )
    assert tuple((item.market, item.symbol) for item in crossed.affected_books) == (
        (Market.SPOT, "BTCUSDT"),
    )
    assert crossed.affected_books[0].crossed_or_locked is True

    restored = materializer.process(
        _depth(
            5,
            first_u=104,
            final_u=104,
            bids=(("103", "0"),),
        )
    )
    assert restored.affected_books[0].crossed_or_locked is False
    view = materializer.view(Market.SPOT, "BTCUSDT")
    assert view.best_bid == (Decimal("99.00"), Decimal("2.50"))


def test_per_book_metrics_and_unresolved_gap_survive_terminal_disconnect() -> None:
    materializer = _spot_book()
    materializer.process(_depth(4, first_u=200, final_u=201, monotonic_ns=400))

    gap = materializer.coverage_state(Market.SPOT, "BTCUSDT")
    metrics = materializer.book_metrics(Market.SPOT, "BTCUSDT")
    assert gap.unresolved_sequence_gap is True
    assert metrics.sequence_gaps == 1
    assert metrics.books_became_valid == 1

    materializer.process(
        _transition(
            5,
            state=ConnectionState.DISCONNECTED,
            monotonic_ns=500,
        )
    )
    terminal = materializer.coverage_state(Market.SPOT, "BTCUSDT")
    assert terminal.status is LocalBookStatus.DISCONNECTED
    assert terminal.unresolved_sequence_gap is True
    assert terminal.unresolved_reconstruction is True


def test_first_record_must_start_at_ingest_sequence_one() -> None:
    materializer = LocalBookMaterializer()

    with pytest.raises(
        LocalBookReplayError,
        match="expected 1, received 2",
    ):
        materializer.process(_transition(2))

    assert materializer.metrics.ingest_gaps == 1
    assert all(
        state.status is LocalBookStatus.REPLAY_FAILED
        for state in materializer.coverage_states
    )


@pytest.mark.parametrize(
    ("market", "duplicate_key"),
    [
        (Market.SPOT, "stream"),
        (Market.SPOT, "data"),
        (Market.SPOT, "U"),
        (Market.SPOT, "u"),
        (Market.FUTURES, "U"),
        (Market.FUTURES, "u"),
        (Market.FUTURES, "pu"),
        (Market.FUTURES, "st"),
        (Market.FUTURES, "ps"),
    ],
)
def test_recursive_duplicate_depth_json_keys_fail_closed(
    market: Market,
    duplicate_key: str,
) -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, market=market))

    result = materializer.process(
        _depth(
            2,
            market=market,
            first_u=100,
            final_u=102,
            previous_u=99,
            raw_payload_override=_duplicate_depth_json(market, duplicate_key),
        )
    )

    assert result.affected_books[0].reason is LocalBookReason.MALFORMED_EVENT
    assert materializer.metrics.malformed_records == 1


@pytest.mark.parametrize("market", [Market.SPOT, Market.FUTURES])
def test_duplicate_snapshot_update_id_fails_closed(market: Market) -> None:
    materializer = LocalBookMaterializer()
    materializer.process(_transition(1, market=market))
    materializer.process(
        _depth(
            2,
            market=market,
            first_u=100,
            final_u=102,
            previous_u=99,
        )
    )

    result = materializer.process(
        _snapshot(
            3,
            market=market,
            update_id=101,
            raw_payload_override=(
                '{"lastUpdateId":101,"lastUpdateId":101,'
                '"bids":[["100","1"]],"asks":[["101","1"]]}'
            ),
        )
    )

    assert result.affected_books[0].reason is LocalBookReason.MALFORMED_SNAPSHOT
    assert materializer.metrics.malformed_records == 1


def test_successful_new_generation_bridge_clears_unresolved_reconstruction() -> None:
    materializer = _spot_book()
    assert not materializer.coverage_state(
        Market.SPOT, "BTCUSDT"
    ).unresolved_reconstruction
    materializer.process(
        _transition(4, state=ConnectionState.DISCONNECTED, monotonic_ns=400)
    )
    materializer.process(
        _transition(5, connection_id="depth-g2", monotonic_ns=500)
    )
    awaiting = materializer.coverage_state(Market.SPOT, "BTCUSDT")
    assert awaiting.unresolved_reconstruction is True
    materializer.process(
        _depth(
            6,
            connection_id="depth-g2",
            first_u=200,
            final_u=202,
            monotonic_ns=600,
        )
    )
    materializer.process(
        _snapshot(
            7,
            update_id=201,
            request_started_ns=550,
            response_completed_ns=700,
        )
    )
    synchronized = materializer.coverage_state(Market.SPOT, "BTCUSDT")
    assert synchronized.status is LocalBookStatus.VALID
    assert synchronized.unresolved_reconstruction is False


def _duplicate_depth_json(market: Market, duplicate_key: str) -> str:
    stream = _stream()
    fields = [
        '"e":"depthUpdate"',
        '"s":"BTCUSDT"',
        '"U":100',
        '"u":102',
        '"b":[]',
        '"a":[]',
    ]
    duplicate_values = {"U": "100", "u": "102"}
    if market is Market.FUTURES:
        fields.extend(('"pu":99', '"st":1', '"ps":"BTCUSDT"'))
        duplicate_values.update({"pu": "99", "st": "1", "ps": '"BTCUSDT"'})
    if duplicate_key in duplicate_values:
        fields.append(f'"{duplicate_key}":{duplicate_values[duplicate_key]}')
    data = "{" + ",".join(fields) + "}"
    if duplicate_key == "stream":
        return f'{{"stream":"{stream}","stream":"{stream}","data":{data}}}'
    if duplicate_key == "data":
        return f'{{"stream":"{stream}","data":{data},"data":{data}}}'
    return f'{{"stream":"{stream}","data":{data}}}'
