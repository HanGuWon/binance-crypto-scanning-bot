from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import replace
from decimal import Context, Decimal, Underflow, localcontext

import pytest

from signalbot.r4b_v2.alerts.actionability import CausalTargetCursorV2
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    PAPER_FOK_RULE_VERSION_V2,
    CausalMarkPriceEvidenceV2,
    ContinuousBookHealthEvidenceV2,
    DepthLevelV2,
    FuturesDepthContinuityWitnessV2,
    FuturesDepthSnapshotV2,
    FuturesExchangeInfoEvidenceV2,
    FuturesStandardDepthEventV2,
    PaperFokClosureEvidenceV2,
    PaperFokClosureMethodV2,
    PaperFokContractErrorV2,
    PaperFokDecisionRegistryV2,
    PaperFokEntryDecisionV2,
    PaperFokEntryInputV2,
    PaperFokEntryStatusV2,
    PaperFokFullFillCertificateV2,
    PaperFokInconclusiveCauseV2,
    PaperFokLineageV2,
    PaperFokRegistryDispositionV2,
    PaperFokSideV2,
    QuietRestSnapshotEvidenceV2,
    RawQuantityFilterV2,
    canonical_paper_fok_entry_decision_v2,
    evaluate_paper_fok_entry_v2,
    intersect_quantity_filters_v2,
    issue_paper_fok_full_fill_certificate_v2,
    reconstruct_futures_standard_book_v2,
    verify_paper_fok_full_fill_certificate_v2,
)
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2

PLAN = "11" * 32
SOURCE = "22" * 32
SNAPSHOT_SCHEMA = "33" * 32
DEPTH_SCHEMA = "44" * 32
MARK_SCHEMA = "55" * 32
EXCHANGE_SCHEMA = "66" * 32
HEALTH_SCHEMA = "77" * 32
SIGNAL = "88" * 32
BAR_OPEN_MS = 0
BAR_CLOSE_MS = 299_999
DECISION_CUTOFF_MS = 302_000
TARGET_VENUE_MS = 312_000
TARGET_LOCAL_CURSOR_MS = 400_000
FINALIZATION_GRACE_MS = 900_000
CLOCK_ROOT = "99" * 32
GRACE_BINDING = "aa" * 32


def _target_cursor() -> CausalTargetCursorV2:
    return CausalTargetCursorV2(
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        target_venue_ms=TARGET_VENUE_MS,
        prior_local_cursor_ms=TARGET_LOCAL_CURSOR_MS - 1,
        prior_venue_lower_bound_ms=TARGET_VENUE_MS - 1,
        target_local_cursor_ms=TARGET_LOCAL_CURSOR_MS,
        target_venue_lower_bound_ms=TARGET_VENUE_MS,
        clock_segment_root_sha256=CLOCK_ROOT,
        contiguous_cursor_evidence=True,
    )


def _level(price: str, quantity: str) -> DepthLevelV2:
    return DepthLevelV2(price=Decimal(price), quantity=Decimal(quantity))


def _lineage() -> PaperFokLineageV2:
    return PaperFokLineageV2(
        promoting_plan_sha256=PLAN,
        source_root_sha256=SOURCE,
        depth_snapshot_schema_sha256=SNAPSHOT_SCHEMA,
        standard_depth_schema_sha256=DEPTH_SCHEMA,
        mark_schema_sha256=MARK_SCHEMA,
        exchange_info_schema_sha256=EXCHANGE_SCHEMA,
        health_schema_sha256=HEALTH_SCHEMA,
    )


def _snapshot(
    *,
    bids: tuple[DepthLevelV2, ...] | None = None,
    asks: tuple[DepthLevelV2, ...] | None = None,
) -> FuturesDepthSnapshotV2:
    return FuturesDepthSnapshotV2(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=SOURCE,
        schema_sha256=SNAPSHOT_SCHEMA,
        response_completion_ms=399_000,
        last_update_id=100,
        depth_limit=3,
        bids=(
            bids
            if bids is not None
            else (
                _level("99.00", "4.00"),
                _level("98.90", "4.00"),
                _level("98.80", "4.00"),
            )
        ),
        asks=(
            asks
            if asks is not None
            else (
                _level("100.00", "2.00"),
                _level("100.10", "2.00"),
                _level("100.20", "2.00"),
            )
        ),
    )


def _depth_event(
    *,
    ingest_seq: int = 10,
    previous_same_stream_ingest_seq: int = 0,
    first_update_id: int = 100,
    final_update_id: int = 101,
    previous_final_update_id: int = 99,
    bids: tuple[DepthLevelV2, ...] = (),
    asks: tuple[DepthLevelV2, ...] = (),
) -> FuturesStandardDepthEventV2:
    return FuturesStandardDepthEventV2(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=SOURCE,
        schema_sha256=DEPTH_SCHEMA,
        pair="BTCUSDT",
        routing_status=1,
        event_time_ms=311_000,
        transaction_time_ms=311_000,
        receipt_completion_ms=399_500,
        ingest_seq=ingest_seq,
        previous_same_stream_ingest_seq=previous_same_stream_ingest_seq,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        previous_final_update_id=previous_final_update_id,
        bids=bids,
        asks=asks,
    )


def _successor(
    *,
    ingest_seq: int = 11,
    previous_same_stream_ingest_seq: int = 10,
    previous_final_update_id: int = 101,
    receipt_completion_ms: int = 400_001,
    transaction_time_ms: int = 311_000,
    first_update_id: int = 102,
    final_update_id: int = 102,
) -> FuturesDepthContinuityWitnessV2:
    return FuturesDepthContinuityWitnessV2(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=SOURCE,
        schema_sha256=DEPTH_SCHEMA,
        pair="BTCUSDT",
        routing_status=1,
        event_time_ms=transaction_time_ms,
        transaction_time_ms=transaction_time_ms,
        receipt_completion_ms=receipt_completion_ms,
        ingest_seq=ingest_seq,
        previous_same_stream_ingest_seq=previous_same_stream_ingest_seq,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        previous_final_update_id=previous_final_update_id,
    )


def _closure(
    *,
    successors: tuple[FuturesDepthContinuityWitnessV2, ...] | None = None,
) -> PaperFokClosureEvidenceV2:
    return PaperFokClosureEvidenceV2(
        closure_grace_end_local_ms=FINALIZATION_GRACE_MS,
        finalization_grace_binding_sha256=GRACE_BINDING,
        finalized_through_local_ms=TARGET_LOCAL_CURSOR_MS,
        successor_candidates=(
            (_successor(),) if successors is None else successors
        ),
    )


def _mark() -> CausalMarkPriceEvidenceV2:
    return CausalMarkPriceEvidenceV2(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=SOURCE,
        schema_sha256=MARK_SCHEMA,
        pair="BTCUSDT",
        routing_status=1,
        mark_price=Decimal("100.00"),
        event_time_ms=TARGET_VENUE_MS - 2_000,
        receipt_completion_ms=TARGET_LOCAL_CURSOR_MS,
    )


def _quantity_filter(
    *,
    min_qty: str = "0.01",
    max_qty: str = "100.00",
    step_size: str = "0.01",
) -> RawQuantityFilterV2:
    return RawQuantityFilterV2(
        min_qty=Decimal(min_qty),
        max_qty=Decimal(max_qty),
        step_size=Decimal(step_size),
    )


def _rules() -> FuturesExchangeInfoEvidenceV2:
    return FuturesExchangeInfoEvidenceV2(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=SOURCE,
        schema_sha256=EXCHANGE_SCHEMA,
        response_completion_ms=399_000,
        version_valid_from_local_ms=390_000,
        version_valid_through_local_ms=TARGET_LOCAL_CURSOR_MS,
        applicable_filter_inventory_complete=True,
        tick_size=Decimal("0.01"),
        min_price=Decimal("1.00"),
        max_price=Decimal("1000000.00"),
        percent_price_multiplier_down=Decimal("0.90"),
        percent_price_multiplier_up=Decimal("1.10"),
        market_take_bound=Decimal("0.05"),
        min_notional=Decimal("0"),
        max_notional=Decimal("0"),
        lot_size=_quantity_filter(),
        market_lot_size=_quantity_filter(),
    )


def _item(**overrides: object) -> PaperFokEntryInputV2:
    events = overrides.pop("pre_target_depth_events", (_depth_event(),))
    assert isinstance(events, tuple)
    values: dict[str, object] = {
        "attempt_id": "attempt-1",
        "signal_event_id": SIGNAL,
        "symbol": "BTCUSDT",
        "venue": VenueV2.USDM_FUTURES,
        "lineage": _lineage(),
        "bar_open_ms": BAR_OPEN_MS,
        "bar_close_ms": BAR_CLOSE_MS,
        "decision_cutoff_ms": DECISION_CUTOFF_MS,
        "target_cursor": _target_cursor(),
        "target_state_last_ingest_seq": max(
            (event.ingest_seq for event in events),
            default=0,
        ),
        "side": PaperFokSideV2.BUY,
        "requested_quantity": Decimal("2.00"),
        "snapshot": _snapshot(),
        "pre_target_depth_events": events,
        "closure": _closure(),
        "mark": _mark(),
        "exchange_info": _rules(),
    }
    values.update(overrides)
    return PaperFokEntryInputV2(**values)  # type: ignore[arg-type]


def _registry_for(
    decision: PaperFokEntryDecisionV2,
    *,
    maximum_events: int = 4,
) -> PaperFokDecisionRegistryV2:
    registry = PaperFokDecisionRegistryV2(
        maximum_events=maximum_events,
        attempt_id=decision.attempt_id,
        promoting_plan_sha256=decision.promoting_plan_sha256,
    )
    registry.register(decision)
    return registry


def _certificate_for(
    decision: PaperFokEntryDecisionV2,
) -> tuple[PaperFokFullFillCertificateV2, PaperFokDecisionRegistryV2]:
    registry = _registry_for(decision)
    checkpoint = registry.terminal_checkpoint_v2()
    return (
        issue_paper_fok_full_fill_certificate_v2(
            decision,
            registry=registry,
            externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
        ),
        registry,
    )


def _restore_registry(
    registry: PaperFokDecisionRegistryV2,
    *,
    state: bytes | None = None,
) -> PaperFokDecisionRegistryV2:
    checkpoint = registry.terminal_checkpoint_v2()
    return PaperFokDecisionRegistryV2.from_state_v2(
        registry.export_state_v2() if state is None else state,
        expected_replay_root_sha256=checkpoint.replay_root_sha256,
        expected_event_count=checkpoint.event_count,
        expected_maximum_events=checkpoint.maximum_events,
        expected_attempt_id=checkpoint.attempt_id,
        expected_promoting_plan_sha256=checkpoint.promoting_plan_sha256,
        expected_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )


def test_buy_full_fill_uses_tick_rounded_cap_and_full_depth_vwap() -> None:
    decision = evaluate_paper_fok_entry_v2(_item())

    assert decision.status is PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY
    assert decision.certified_quantity == Decimal("2.00")
    assert decision.filled_quantity == Decimal("2.00")
    assert decision.executable_vwap == Decimal("100.05")
    assert decision.executable_notional == Decimal("200.10")
    assert decision.paper_price_cap == Decimal("100.10")
    assert decision.discord_timing_present is False
    assert decision.production_order_placement is False
    assert b"bookTicker" not in canonical_paper_fok_entry_decision_v2(decision)
    certificate, registry = _certificate_for(decision)
    assert certificate.decision_event_id == decision.event_id
    assert certificate.attempt_id == decision.attempt_id
    assert certificate.signal_event_id == decision.signal_event_id
    assert certificate.venue is VenueV2.USDM_FUTURES
    assert certificate.promoting_plan_sha256 == PLAN
    assert certificate.decision_cutoff_ms == DECISION_CUTOFF_MS
    assert certificate.target_venue_ms == DECISION_CUTOFF_MS + 10_000
    verify_paper_fok_full_fill_certificate_v2(
        certificate,
        decision,
        registry=registry,
        expected_attempt_id="attempt-1",
        expected_promoting_plan_sha256=PLAN,
        expected_target_cursor_evidence_sha256=(
            decision.target_cursor.cursor_evidence_sha256
        ),
        expected_terminal_registry_checkpoint_sha256=(
            registry.terminal_checkpoint_v2().checkpoint_sha256
        ),
    )


def test_paper_fok_patch_rule_version_is_bound_into_canonical_decision() -> None:
    decision = evaluate_paper_fok_entry_v2(_item())
    document = json.loads(canonical_paper_fok_entry_decision_v2(decision))

    assert PAPER_FOK_RULE_VERSION_V2 == "R4B_CAUSAL_V2.3.1_PAPER_FOK_ENTRY"
    assert decision.rule_version == PAPER_FOK_RULE_VERSION_V2
    assert document["rule_version"] == PAPER_FOK_RULE_VERSION_V2


def test_sell_walk_is_side_correct_and_uses_ceil_tick_cap() -> None:
    decision = evaluate_paper_fok_entry_v2(
        _item(side=PaperFokSideV2.SELL)
    )

    assert decision.status is PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY
    assert decision.executable_vwap == Decimal("99.00")
    assert decision.paper_price_cap == Decimal("98.91")


def test_primary_target_is_exactly_d_plus_10000() -> None:
    with pytest.raises(ValueError, match="plus 10000"):
        CausalTargetCursorV2(
            decision_cutoff_ms=DECISION_CUTOFF_MS,
            target_venue_ms=TARGET_VENUE_MS + 1,
            prior_local_cursor_ms=TARGET_LOCAL_CURSOR_MS - 1,
            prior_venue_lower_bound_ms=TARGET_VENUE_MS - 1,
            target_local_cursor_ms=TARGET_LOCAL_CURSOR_MS,
            target_venue_lower_bound_ms=TARGET_VENUE_MS + 1,
            clock_segment_root_sha256=CLOCK_ROOT,
            contiguous_cursor_evidence=True,
        )


def test_decision_binds_clock_root_and_cursor_evidence() -> None:
    first = evaluate_paper_fok_entry_v2(_item())
    alternate_cursor = CausalTargetCursorV2(
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        target_venue_ms=TARGET_VENUE_MS,
        prior_local_cursor_ms=TARGET_LOCAL_CURSOR_MS - 1,
        prior_venue_lower_bound_ms=TARGET_VENUE_MS - 1,
        target_local_cursor_ms=TARGET_LOCAL_CURSOR_MS,
        target_venue_lower_bound_ms=TARGET_VENUE_MS,
        clock_segment_root_sha256="bb" * 32,
        contiguous_cursor_evidence=True,
    )
    alternate = evaluate_paper_fok_entry_v2(
        _item(target_cursor=alternate_cursor)
    )
    document = json.loads(canonical_paper_fok_entry_decision_v2(first))

    assert document["target_cursor"]["clock_segment_root_sha256"] == CLOCK_ROOT
    assert document["target_cursor"]["cursor_evidence_sha256"] == (
        first.target_cursor.cursor_evidence_sha256
    )
    assert first.event_id == alternate.event_id
    assert first.payload_sha256 != alternate.payload_sha256


@pytest.mark.parametrize(
    "changes",
    [
        {"bar_close_ms": BAR_CLOSE_MS + 1},
        {"decision_cutoff_ms": DECISION_CUTOFF_MS - 1},
        {"decision_cutoff_ms": DECISION_CUTOFF_MS + 1},
    ],
)
def test_closed_five_minute_bar_and_exact_d_boundaries(
    changes: dict[str, int],
) -> None:
    with pytest.raises(PaperFokContractErrorV2):
        _item(**changes)


def test_generalized_crt_nonzero_residue_and_per_level_floor_boundaries() -> None:
    grid = intersect_quantity_filters_v2(
        _quantity_filter(min_qty="0.001", max_qty="1", step_size="0.002"),
        _quantity_filter(min_qty="0.003", max_qty="1", step_size="0.004"),
    )

    assert grid.first_legal == Decimal("0.003")
    assert grid.quantum == Decimal("0.004")
    assert grid.floor_capacity_per_level(Decimal("0.002")) == 0
    assert grid.floor_capacity_per_level(Decimal("0.003")) == Decimal("0.003")
    assert grid.floor_capacity_per_level(Decimal("0.006")) == Decimal("0.003")
    assert grid.floor_capacity_per_level(Decimal("0.007")) == Decimal("0.007")


def test_generalized_crt_rejects_incompatible_origins() -> None:
    with pytest.raises(PaperFokContractErrorV2, match="no congruence"):
        intersect_quantity_filters_v2(
            _quantity_filter(min_qty="0.000", max_qty="1", step_size="0.002"),
            _quantity_filter(min_qty="0.001", max_qty="1", step_size="0.002"),
        )


def test_crt_integerization_ignores_hostile_ambient_decimal_precision() -> None:
    first = RawQuantityFilterV2(
        min_qty=Decimal("0.123456789012345678901234567890123456789"),
        max_qty=Decimal("9.9999999999999999999999999999999999999999"),
        step_size=Decimal("0.000000000000000000000000000000000000002"),
    )
    second = RawQuantityFilterV2(
        min_qty=Decimal("0.123456789012345678901234567890123456789"),
        max_qty=Decimal("8.8888888888888888888888888888888888888888"),
        step_size=Decimal("0.000000000000000000000000000000000000004"),
    )
    with localcontext(Context(prec=3)):
        grid = intersect_quantity_filters_v2(first, second)

    assert grid.first_legal == first.min_qty
    assert grid.quantum == Decimal("0.000000000000000000000000000000000000004")


def test_shared_decimal34_context_traps_underflow() -> None:
    assert protocol_decimal_context_v2().traps[Underflow] is True


def test_haircut_is_floored_per_level_not_after_aggregation() -> None:
    filters = _quantity_filter(min_qty="1", max_qty="100", step_size="1")
    rules = replace(_rules(), lot_size=filters, market_lot_size=filters)
    snapshot = _snapshot(
        asks=(
            _level("100.00", "1.9"),
            _level("100.10", "1.9"),
            _level("100.20", "1.9"),
        )
    )
    decision = evaluate_paper_fok_entry_v2(
        _item(
            requested_quantity=Decimal("1"),
            exchange_info=rules,
            snapshot=snapshot,
        )
    )

    assert decision.status is PaperFokEntryStatusV2.ADMITTED_PAPER_IOC_NO_FILL
    assert decision.certified_quantity == 0


@pytest.mark.parametrize(
    ("level_quantity", "requested", "status", "certified"),
    [
        ("0.01", "0.01", PaperFokEntryStatusV2.ADMITTED_PAPER_IOC_NO_FILL, "0"),
        ("2.00", "2.00", PaperFokEntryStatusV2.NOT_ADMITTED_PAPER_CAPACITY, "1.00"),
        ("4.00", "2.00", PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY, "2.00"),
    ],
)
def test_certified_quantity_zero_below_and_equal_boundaries(
    level_quantity: str,
    requested: str,
    status: PaperFokEntryStatusV2,
    certified: str,
) -> None:
    decision = evaluate_paper_fok_entry_v2(
        _item(
            requested_quantity=Decimal(requested),
            snapshot=_snapshot(
                asks=(
                    _level("100.00", level_quantity),
                    _level("100.20", "1"),
                )
            ),
        )
    )

    assert decision.status is status
    assert decision.certified_quantity == Decimal(certified)
    assert decision.filled_quantity in (Decimal(0), Decimal(requested))


def test_market_take_bound_and_10bp_cap_equalities_are_consumable() -> None:
    rules = replace(_rules(), market_take_bound=Decimal("0.001"))
    decision = evaluate_paper_fok_entry_v2(_item(exchange_info=rules))

    assert decision.market_take_bound_price == Decimal("100.10000")
    assert decision.certified_quantity == Decimal("2.00")
    assert decision.status is PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY


def test_one_tick_beyond_market_take_bound_is_not_consumed() -> None:
    rules = replace(_rules(), market_take_bound=Decimal("0"))
    snapshot = _snapshot(
        asks=(
            _level("100.00", "2.00"),
            _level("100.01", "2.00"),
            _level("100.20", "2.00"),
        )
    )
    decision = evaluate_paper_fok_entry_v2(
        _item(exchange_info=rules, snapshot=snapshot)
    )

    assert decision.status is PaperFokEntryStatusV2.NOT_ADMITTED_PAPER_CAPACITY
    assert decision.certified_quantity == Decimal("1.00")


def test_min_notional_equality_passes_and_one_cent_above_rejects() -> None:
    equal = replace(_rules(), min_notional=Decimal("200.00"))
    above = replace(_rules(), min_notional=Decimal("200.01"))

    assert (
        evaluate_paper_fok_entry_v2(_item(exchange_info=equal)).status
        is PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY
    )
    rejected = evaluate_paper_fok_entry_v2(_item(exchange_info=above))
    assert rejected.status is PaperFokEntryStatusV2.ADMITTED_PAPER_IOC_NO_FILL
    assert rejected.reasons == ("CERTAIN_NOTIONAL_FILTER_REJECTION",)


def test_actual_walked_notional_is_rechecked_after_mark_notional_passes() -> None:
    rules = replace(_rules(), max_notional=Decimal("200.05"))

    decision = evaluate_paper_fok_entry_v2(_item(exchange_info=rules))

    assert decision.status is PaperFokEntryStatusV2.ADMITTED_PAPER_IOC_NO_FILL
    assert decision.reasons == ("CERTAIN_EXECUTABLE_NOTIONAL_FILTER_REJECTION",)
    assert decision.certified_quantity == 0
    assert decision.filled_quantity == 0


def test_price_and_percent_filter_bound_equalities_pass_then_one_tick_fails() -> None:
    equality_rules = replace(
        _rules(),
        max_price=Decimal("100.10"),
        percent_price_multiplier_up=Decimal("1.001"),
    )
    below_rules = replace(
        equality_rules,
        max_price=Decimal("100.09"),
        percent_price_multiplier_up=Decimal("1.0009"),
    )

    equal = evaluate_paper_fok_entry_v2(_item(exchange_info=equality_rules))
    below = evaluate_paper_fok_entry_v2(_item(exchange_info=below_rules))
    assert equal.executed_full_quantity
    assert below.status is PaperFokEntryStatusV2.NOT_ADMITTED_PAPER_CAPACITY
    assert below.certified_quantity == Decimal("1.00")


def test_off_tick_consumable_depth_level_is_inconclusive_filter() -> None:
    snapshot = _snapshot(
        asks=(
            _level("100.005", "2.00"),
            _level("100.10", "2.00"),
            _level("100.20", "2.00"),
        )
    )
    decision = evaluate_paper_fok_entry_v2(_item(snapshot=snapshot))

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_FILTER
    assert decision.inconclusive_cause is PaperFokInconclusiveCauseV2.INCONCLUSIVE_FILTER


def test_repeating_vwap_uses_frozen_decimal34_under_hostile_context() -> None:
    snapshot = _snapshot(
        asks=(
            _level("100.00", "2.00"),
            _level("100.01", "4.00"),
            _level("100.20", "2.00"),
        )
    )
    with localcontext(Context(prec=3)):
        decision = evaluate_paper_fok_entry_v2(
            _item(requested_quantity=Decimal("3.00"), snapshot=snapshot)
        )

    assert decision.executed_full_quantity
    assert str(decision.executable_vwap) == "100.0066666666666666666666666666667"


def test_mark_staleness_equality_passes_and_plus_one_is_typed_inconclusive() -> None:
    assert evaluate_paper_fok_entry_v2(_item()).executed_full_quantity
    late_mark = replace(_mark(), event_time_ms=TARGET_VENUE_MS - 2_001)
    decision = evaluate_paper_fok_entry_v2(_item(mark=late_mark))

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert (
        decision.inconclusive_cause
        is PaperFokInconclusiveCauseV2.INCONCLUSIVE_EXECUTION_RULE
    )


def test_filter_validity_equality_passes_and_plus_one_is_inconclusive_filter() -> None:
    assert evaluate_paper_fok_entry_v2(_item()).executed_full_quantity
    rules = replace(
        _rules(),
        version_valid_through_local_ms=TARGET_LOCAL_CURSOR_MS - 1,
    )
    decision = evaluate_paper_fok_entry_v2(_item(exchange_info=rules))

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_FILTER
    assert decision.inconclusive_cause is PaperFokInconclusiveCauseV2.INCONCLUSIVE_FILTER


def test_missing_filter_inventory_and_future_filter_response_are_inconclusive() -> None:
    incomplete = evaluate_paper_fok_entry_v2(
        _item(
            exchange_info=replace(
                _rules(),
                applicable_filter_inventory_complete=False,
            )
        )
    )
    future = evaluate_paper_fok_entry_v2(
        _item(
            exchange_info=replace(
                _rules(),
                response_completion_ms=TARGET_LOCAL_CURSOR_MS + 1,
            )
        )
    )
    for decision in (incomplete, future):
        assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_FILTER
        assert (
            decision.inconclusive_cause
            is PaperFokInconclusiveCauseV2.INCONCLUSIVE_FILTER
        )


def test_mark_receipt_cursor_equality_passes_and_plus_one_is_inconclusive() -> None:
    assert evaluate_paper_fok_entry_v2(_item()).executed_full_quantity
    decision = evaluate_paper_fok_entry_v2(
        _item(
            mark=replace(
                _mark(),
                receipt_completion_ms=TARGET_LOCAL_CURSOR_MS + 1,
            )
        )
    )
    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert (
        decision.inconclusive_cause
        is PaperFokInconclusiveCauseV2.INCONCLUSIVE_EXECUTION_RULE
    )


def test_delayed_transaction_time_successor_is_continuity_only() -> None:
    decision = evaluate_paper_fok_entry_v2(
        _item(closure=_closure(successors=(_successor(transaction_time_ms=1),)))
    )

    assert decision.executed_full_quantity
    assert decision.closure_method is PaperFokClosureMethodV2.CONTIGUOUS_SUCCESSOR


def test_future_successor_prices_cannot_change_fill_or_payload() -> None:
    first_raw = _depth_event(
        ingest_seq=11,
        previous_same_stream_ingest_seq=10,
        first_update_id=102,
        final_update_id=102,
        previous_final_update_id=101,
        asks=(_level("1000000", "999999"),),
    )
    second_raw = replace(first_raw, asks=(_level("0.01", "0"),))
    first = FuturesDepthContinuityWitnessV2.from_event(first_raw)
    second = FuturesDepthContinuityWitnessV2.from_event(second_raw)

    assert first == second
    decision_a = evaluate_paper_fok_entry_v2(
        _item(closure=_closure(successors=(first,)))
    )
    decision_b = evaluate_paper_fok_entry_v2(
        _item(closure=_closure(successors=(second,)))
    )
    assert canonical_paper_fok_entry_decision_v2(decision_a) == (
        canonical_paper_fok_entry_decision_v2(decision_b)
    )


def test_terminal_successor_freezes_first_material_row_and_finalization_cursor() -> None:
    first = _successor()
    later_malformed = replace(
        _successor(
            ingest_seq=12,
            previous_same_stream_ingest_seq=999,
            receipt_completion_ms=FINALIZATION_GRACE_MS + 50_000,
        ),
        source_root_sha256="bb" * 32,
        routing_status=0,
        pair="ETHUSDT",
    )
    base_closure = _closure(successors=(first,))
    extended_closure = replace(
        base_closure,
        finalized_through_local_ms=FINALIZATION_GRACE_MS + 100_000,
        successor_candidates=(later_malformed, first),
    )

    base = evaluate_paper_fok_entry_v2(_item(closure=base_closure))
    extended = evaluate_paper_fok_entry_v2(_item(closure=extended_closure))

    assert canonical_paper_fok_entry_decision_v2(base) == (
        canonical_paper_fok_entry_decision_v2(extended)
    )
    base_registry = _registry_for(base)
    extended_registry = _registry_for(extended)
    assert base_registry.replay_root_sha256 == extended_registry.replay_root_sha256


def test_exact_duplicate_first_successor_is_idempotent_but_conflict_is_invalid() -> None:
    successor = _successor()
    duplicate = evaluate_paper_fok_entry_v2(
        _item(closure=_closure(successors=(successor, successor)))
    )
    base = evaluate_paper_fok_entry_v2(
        _item(closure=_closure(successors=(successor,)))
    )
    assert canonical_paper_fok_entry_decision_v2(duplicate) == (
        canonical_paper_fok_entry_decision_v2(base)
    )

    conflict = replace(successor, transaction_time_ms=successor.transaction_time_ms - 1)
    forward = evaluate_paper_fok_entry_v2(
        _item(closure=_closure(successors=(successor, conflict)))
    )
    reverse = evaluate_paper_fok_entry_v2(
        _item(closure=_closure(successors=(conflict, successor)))
    )
    assert forward.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert forward.reasons == (
        "CONFLICTING_SUCCESSOR_ROWS_AT_FIRST_INGEST_SEQUENCE",
    )
    assert canonical_paper_fok_entry_decision_v2(forward) == (
        canonical_paper_fok_entry_decision_v2(reverse)
    )


@pytest.mark.parametrize("final_update_id", [101, 100])
def test_successor_must_advance_past_prior_u(final_update_id: int) -> None:
    decision = evaluate_paper_fok_entry_v2(
        _item(
            closure=_closure(
                successors=(
                    _successor(
                        first_update_id=final_update_id,
                        final_update_id=final_update_id,
                    ),
                )
            )
        )
    )

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert decision.reasons == ("SUCCESSOR_U_DID_NOT_ADVANCE",)


def test_omitted_immediate_successor_cannot_be_hidden_by_later_candidate() -> None:
    omitted = _successor(
        ingest_seq=12,
        previous_same_stream_ingest_seq=11,
        previous_final_update_id=101,
    )
    decision = evaluate_paper_fok_entry_v2(
        _item(closure=_closure(successors=(omitted,)))
    )

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert decision.inconclusive_cause is PaperFokInconclusiveCauseV2.INCONCLUSIVE_CLOSURE


def test_first_gap_successor_wins_over_later_contiguous_candidate() -> None:
    first_bad = _successor(previous_final_update_id=999)
    later_good = _successor(
        ingest_seq=12,
        previous_same_stream_ingest_seq=11,
        previous_final_update_id=101,
        receipt_completion_ms=400_002,
    )
    decision = evaluate_paper_fok_entry_v2(
        _item(closure=_closure(successors=(later_good, first_bad)))
    )

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert decision.reasons == ("SUCCESSOR_PU_DOES_NOT_EQUAL_PRIOR_U",)


def test_externally_bound_grace_has_no_fixed_30_second_cutoff() -> None:
    delayed = _successor(receipt_completion_ms=TARGET_LOCAL_CURSOR_MS + 30_001)
    at_grace = _successor(receipt_completion_ms=FINALIZATION_GRACE_MS)
    after_grace = _successor(receipt_completion_ms=FINALIZATION_GRACE_MS + 1)

    for successor in (delayed, at_grace):
        assert evaluate_paper_fok_entry_v2(
            _item(closure=_closure(successors=(successor,)))
        ).executed_full_quantity
    invalid = evaluate_paper_fok_entry_v2(
        _item(closure=_closure(successors=(after_grace,)))
    )
    assert invalid.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert invalid.inconclusive_cause is PaperFokInconclusiveCauseV2.INCONCLUSIVE_CLOSURE


def _quiet_closure(
    *,
    last_update_id: int = 101,
    finalized: int = FINALIZATION_GRACE_MS,
    health_end: int = FINALIZATION_GRACE_MS,
) -> PaperFokClosureEvidenceV2:
    return PaperFokClosureEvidenceV2(
        closure_grace_end_local_ms=FINALIZATION_GRACE_MS,
        finalization_grace_binding_sha256=GRACE_BINDING,
        finalized_through_local_ms=finalized,
        successor_candidates=(),
        quiet_rest_snapshot=QuietRestSnapshotEvidenceV2(
            symbol="BTCUSDT",
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN,
            source_root_sha256=SOURCE,
            schema_sha256=SNAPSHOT_SCHEMA,
            response_completion_ms=FINALIZATION_GRACE_MS,
            last_update_id=last_update_id,
        ),
        continuous_health=ContinuousBookHealthEvidenceV2(
            symbol="BTCUSDT",
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN,
            source_root_sha256=SOURCE,
            schema_sha256=HEALTH_SCHEMA,
            interval_start_local_ms=399_000,
            interval_end_local_ms=health_end,
            generation=1,
            disconnect_count=0,
            parser_error_count=0,
            queue_drop_count=0,
            sequence_gap_count=0,
        ),
    )


def test_quiet_rest_equal_accepts_but_plus_or_minus_one_is_inconclusive() -> None:
    equal = evaluate_paper_fok_entry_v2(_item(closure=_quiet_closure()))
    assert equal.executed_full_quantity
    assert equal.closure_method is PaperFokClosureMethodV2.QUIET_REST_EQUAL

    for update_id in (100, 102):
        decision = evaluate_paper_fok_entry_v2(
            _item(closure=_quiet_closure(last_update_id=update_id))
        )
        assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
        assert (
            decision.inconclusive_cause
            is PaperFokInconclusiveCauseV2.INCONCLUSIVE_CLOSURE
        )


def test_quiet_rest_health_interruption_is_inconclusive() -> None:
    closure = _quiet_closure()
    assert closure.continuous_health is not None
    interrupted = replace(closure.continuous_health, queue_drop_count=1)
    decision = evaluate_paper_fok_entry_v2(
        _item(closure=replace(closure, continuous_health=interrupted))
    )

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert decision.inconclusive_cause is PaperFokInconclusiveCauseV2.INCONCLUSIVE_CLOSURE


def test_quiet_health_must_cover_through_grace_boundary() -> None:
    before = evaluate_paper_fok_entry_v2(
        _item(
            closure=_quiet_closure(
                health_end=FINALIZATION_GRACE_MS - 1,
            )
        )
    )
    at = evaluate_paper_fok_entry_v2(
        _item(closure=_quiet_closure(health_end=FINALIZATION_GRACE_MS))
    )

    assert before.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert before.reasons == ("QUIET_BOOK_HEALTH_NOT_CONTINUOUS",)
    assert at.executed_full_quantity


def test_quiet_terminal_bytes_ignore_later_finalization_cursor_advances() -> None:
    at_grace = evaluate_paper_fok_entry_v2(
        _item(closure=_quiet_closure(finalized=FINALIZATION_GRACE_MS))
    )
    later = evaluate_paper_fok_entry_v2(
        _item(closure=_quiet_closure(finalized=FINALIZATION_GRACE_MS + 500_000))
    )

    assert canonical_paper_fok_entry_decision_v2(at_grace) == (
        canonical_paper_fok_entry_decision_v2(later)
    )


def test_no_successor_before_finalization_remains_nonterminal_pending() -> None:
    closure = PaperFokClosureEvidenceV2(
        closure_grace_end_local_ms=FINALIZATION_GRACE_MS,
        finalization_grace_binding_sha256=GRACE_BINDING,
        finalized_through_local_ms=FINALIZATION_GRACE_MS - 1,
    )
    decision = evaluate_paper_fok_entry_v2(_item(closure=closure))

    assert decision.status is PaperFokEntryStatusV2.CLOSURE_PENDING
    assert decision.inconclusive_cause is None
    with pytest.raises(PaperFokContractErrorV2, match="monitoring view"):
        PaperFokDecisionRegistryV2(
            maximum_events=1,
            attempt_id="attempt-1",
            promoting_plan_sha256=PLAN,
        ).register(decision)


@pytest.mark.parametrize(
    ("field", "replacement", "cause"),
    [
        ("symbol", "ETHUSDT", PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_SCHEMA),
        ("venue", VenueV2.SPOT, PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_SCHEMA),
        ("promoting_plan_sha256", "aa" * 32, PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_SCHEMA),
        ("source_root_sha256", "99" * 32, PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_SCHEMA),
        ("schema_sha256", "bb" * 32, PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_SCHEMA),
    ],
)
def test_cross_symbol_spot_and_root_row_injection_fail_closed(
    field: str,
    replacement: object,
    cause: PaperFokInconclusiveCauseV2,
) -> None:
    snapshot = replace(_snapshot(), **{field: replacement})
    decision = evaluate_paper_fok_entry_v2(_item(snapshot=snapshot))

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert decision.inconclusive_cause is cause


def test_sequence_gap_has_typed_sequence_cause() -> None:
    second = _depth_event(
        ingest_seq=11,
        previous_same_stream_ingest_seq=10,
        first_update_id=102,
        final_update_id=102,
        previous_final_update_id=999,
    )
    decision = evaluate_paper_fok_entry_v2(
        _item(
            pre_target_depth_events=(_depth_event(), second),
            closure=_closure(
                successors=(
                    _successor(
                        ingest_seq=12,
                        previous_same_stream_ingest_seq=11,
                        previous_final_update_id=102,
                    ),
                )
            ),
        )
    )

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert decision.inconclusive_cause is PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_SEQUENCE


@pytest.mark.parametrize(
    "events",
    (
        (),
        (
            _depth_event(
                first_update_id=98,
                final_update_id=99,
                previous_final_update_id=97,
            ),
        ),
    ),
)
def test_snapshot_without_usable_depth_bridge_is_sequence_inconclusive(
    events: tuple[FuturesStandardDepthEventV2, ...],
) -> None:
    decision = evaluate_paper_fok_entry_v2(
        _item(pre_target_depth_events=events)
    )

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert decision.inconclusive_cause is (
        PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_SEQUENCE
    )
    assert decision.reasons == ("DEPTH_SNAPSHOT_BRIDGE_MISSING",)


def test_snapshot_bridge_accepts_exact_update_id_boundary() -> None:
    bridge = _depth_event(
        first_update_id=100,
        final_update_id=100,
        previous_final_update_id=99,
    )

    book, reason = reconstruct_futures_standard_book_v2(
        snapshot=_snapshot(),
        pre_target_depth_events=(bridge,),
        target_venue_ms=TARGET_VENUE_MS,
        target_local_cursor_ms=TARGET_LOCAL_CURSOR_MS,
        target_state_last_ingest_seq=bridge.ingest_seq,
    )

    assert book is not None
    assert book.prior_u == 100
    assert reason == "TARGET_BOOK_RECONSTRUCTED"


def test_stale_depth_row_before_exact_snapshot_bridge_is_accepted() -> None:
    stale = _depth_event(
        first_update_id=98,
        final_update_id=99,
        previous_final_update_id=97,
    )
    bridge = _depth_event(
        ingest_seq=11,
        previous_same_stream_ingest_seq=10,
        first_update_id=100,
        final_update_id=100,
        previous_final_update_id=99,
    )

    book, reason = reconstruct_futures_standard_book_v2(
        snapshot=_snapshot(),
        pre_target_depth_events=(stale, bridge),
        target_venue_ms=TARGET_VENUE_MS,
        target_local_cursor_ms=TARGET_LOCAL_CURSOR_MS,
        target_state_last_ingest_seq=bridge.ingest_seq,
    )

    assert book is not None
    assert book.prior_u == 100
    assert reason == "TARGET_BOOK_RECONSTRUCTED"


def test_post_bridge_depth_event_must_advance_u() -> None:
    second = _depth_event(
        ingest_seq=11,
        previous_same_stream_ingest_seq=10,
        first_update_id=101,
        final_update_id=101,
        previous_final_update_id=101,
    )
    decision = evaluate_paper_fok_entry_v2(
        _item(
            pre_target_depth_events=(_depth_event(), second),
            closure=_closure(
                successors=(
                    _successor(
                        ingest_seq=12,
                        previous_same_stream_ingest_seq=11,
                        previous_final_update_id=101,
                    ),
                )
            ),
        )
    )

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert decision.reasons == ("POST_BRIDGE_DEPTH_U_DID_NOT_ADVANCE",)


def test_exact_duplicate_depth_row_collapses_but_same_ingest_conflict_rejects() -> None:
    first = _depth_event()
    base = evaluate_paper_fok_entry_v2(_item(pre_target_depth_events=(first,)))
    duplicate = evaluate_paper_fok_entry_v2(
        _item(pre_target_depth_events=(first, first))
    )
    assert canonical_paper_fok_entry_decision_v2(base) == (
        canonical_paper_fok_entry_decision_v2(duplicate)
    )

    conflict = replace(first, asks=(_level("100.00", "9.00"),))
    forward = evaluate_paper_fok_entry_v2(
        _item(pre_target_depth_events=(first, conflict))
    )
    reverse = evaluate_paper_fok_entry_v2(
        _item(pre_target_depth_events=(conflict, first))
    )
    assert forward.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert forward.inconclusive_cause is (
        PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_SEQUENCE
    )
    assert canonical_paper_fok_entry_decision_v2(forward) == (
        canonical_paper_fok_entry_decision_v2(reverse)
    )


def test_incomplete_ten_bp_snapshot_coverage_is_typed_book_inconclusive() -> None:
    snapshot = replace(
        _snapshot(
            bids=(
                _level("99.00", "4.00"),
                _level("98.90", "4.00"),
            ),
            asks=(
                _level("100.00", "2.00"),
                _level("100.05", "2.00"),
            )
        ),
        depth_limit=2,
    )
    decision = evaluate_paper_fok_entry_v2(_item(snapshot=snapshot))

    assert decision.status is PaperFokEntryStatusV2.INCONCLUSIVE_DATA
    assert decision.inconclusive_cause is PaperFokInconclusiveCauseV2.INCONCLUSIVE_DATA_BOOK


def test_depth_event_input_permutation_is_replay_invariant() -> None:
    first = _depth_event()
    second = _depth_event(
        ingest_seq=11,
        previous_same_stream_ingest_seq=10,
        first_update_id=102,
        final_update_id=102,
        previous_final_update_id=101,
    )
    closure = _closure(
        successors=(
            _successor(
                ingest_seq=12,
                previous_same_stream_ingest_seq=11,
                previous_final_update_id=102,
            ),
        )
    )
    forward = evaluate_paper_fok_entry_v2(
        _item(pre_target_depth_events=(first, second), closure=closure)
    )
    reverse = evaluate_paper_fok_entry_v2(
        _item(pre_target_depth_events=(second, first), closure=closure)
    )

    assert canonical_paper_fok_entry_decision_v2(forward) == (
        canonical_paper_fok_entry_decision_v2(reverse)
    )


def test_direct_or_replaced_forged_full_decision_is_rejected() -> None:
    no_fill = evaluate_paper_fok_entry_v2(
        _item(
            requested_quantity=Decimal("0.01"),
            snapshot=_snapshot(
                asks=(_level("100.00", "0.01"), _level("100.20", "1"))
            ),
        )
    )

    with pytest.raises(ValueError, match="InitVar"):
        replace(
            no_fill,
            status=PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY,
            certified_quantity=Decimal("0.01"),
            filled_quantity=Decimal("0.01"),
            executable_vwap=Decimal("100"),
        )
    with pytest.raises(PaperFokContractErrorV2, match="causal evaluator"):
        replace(
            no_fill,
            status=PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY,
            certified_quantity=Decimal("0.01"),
            filled_quantity=Decimal("0.01"),
            executable_vwap=Decimal("100"),
            _factory_token=object(),
        )
    registry = _registry_for(no_fill)
    with pytest.raises(PaperFokContractErrorV2, match="only full"):
        issue_paper_fok_full_fill_certificate_v2(
            no_fill,
            registry=registry,
            externally_pinned_checkpoint_sha256=(
                registry.terminal_checkpoint_v2().checkpoint_sha256
            ),
        )


def test_certificate_requires_exact_registry_membership() -> None:
    decision = evaluate_paper_fok_entry_v2(_item())
    empty = PaperFokDecisionRegistryV2(
        maximum_events=1,
        attempt_id="attempt-1",
        promoting_plan_sha256=PLAN,
    )

    with pytest.raises(PaperFokContractErrorV2, match="absent"):
        issue_paper_fok_full_fill_certificate_v2(
            decision,
            registry=empty,
            externally_pinned_checkpoint_sha256=(
                empty.terminal_checkpoint_v2().checkpoint_sha256
            ),
        )


def test_public_certificate_verifier_pins_cursor_and_terminal_checkpoint() -> None:
    decision = evaluate_paper_fok_entry_v2(_item())
    certificate, registry = _certificate_for(decision)
    checkpoint = registry.terminal_checkpoint_v2()

    assert certificate.requested_quantity == certificate.filled_quantity
    assert certificate.executable_notional == decision.executable_notional
    assert certificate.terminal_registry_checkpoint_sha256 == (
        checkpoint.checkpoint_sha256
    )
    with pytest.raises(PaperFokContractErrorV2, match="differs"):
        verify_paper_fok_full_fill_certificate_v2(
            certificate,
            decision,
            registry=registry,
            expected_attempt_id="attempt-1",
            expected_promoting_plan_sha256=PLAN,
            expected_target_cursor_evidence_sha256="00" * 32,
            expected_terminal_registry_checkpoint_sha256=(
                checkpoint.checkpoint_sha256
            ),
        )
    with pytest.raises(PaperFokContractErrorV2, match="differs"):
        verify_paper_fok_full_fill_certificate_v2(
            certificate,
            decision,
            registry=registry,
            expected_attempt_id="attempt-1",
            expected_promoting_plan_sha256=PLAN,
            expected_target_cursor_evidence_sha256=(
                decision.target_cursor.cursor_evidence_sha256
            ),
            expected_terminal_registry_checkpoint_sha256="00" * 32,
        )


def test_registry_duplicate_conflict_bound_and_replay_restore() -> None:
    decision = evaluate_paper_fok_entry_v2(_item())
    changed = evaluate_paper_fok_entry_v2(
        _item(
            snapshot=_snapshot(
                asks=(
                    _level("100.00", "4.00"),
                    _level("100.10", "4.00"),
                    _level("100.20", "4.00"),
                )
            )
        )
    )
    assert decision.event_id == changed.event_id
    assert decision.payload_sha256 != changed.payload_sha256

    registry = PaperFokDecisionRegistryV2(
        maximum_events=1,
        attempt_id="attempt-1",
        promoting_plan_sha256=PLAN,
    )
    assert registry.register(decision) is PaperFokRegistryDispositionV2.NEW
    assert (
        registry.register(decision)
        is PaperFokRegistryDispositionV2.IDEMPOTENT_DUPLICATE
    )
    with pytest.raises(PaperFokContractErrorV2, match="collides"):
        registry.register(changed)

    state = registry.export_state_v2()
    restored = _restore_registry(registry, state=state)
    assert restored.replay_root_sha256 == registry.replay_root_sha256
    assert restored.export_state_v2() == state


def test_registry_type_checks_before_reading_decision_attributes() -> None:
    registry = PaperFokDecisionRegistryV2(
        maximum_events=1,
        attempt_id="attempt-1",
        promoting_plan_sha256=PLAN,
    )

    with pytest.raises(PaperFokContractErrorV2, match="wrong type"):
        registry.register(object())  # type: ignore[arg-type]


def test_registry_rejects_attempt_or_plan_mismatch() -> None:
    decision = evaluate_paper_fok_entry_v2(_item())
    wrong_attempt = PaperFokDecisionRegistryV2(
        maximum_events=1,
        attempt_id="attempt-2",
        promoting_plan_sha256=PLAN,
    )
    wrong_plan = PaperFokDecisionRegistryV2(
        maximum_events=1,
        attempt_id="attempt-1",
        promoting_plan_sha256="bb" * 32,
    )

    for registry in (wrong_attempt, wrong_plan):
        with pytest.raises(PaperFokContractErrorV2, match="differs"):
            registry.register(decision)


def test_externally_pinned_restore_rejects_consistent_prefix_truncation() -> None:
    first = evaluate_paper_fok_entry_v2(_item())
    second = evaluate_paper_fok_entry_v2(_item(signal_event_id="99" * 32))
    full = PaperFokDecisionRegistryV2(
        maximum_events=2,
        attempt_id="attempt-1",
        promoting_plan_sha256=PLAN,
    )
    prefix = PaperFokDecisionRegistryV2(
        maximum_events=2,
        attempt_id="attempt-1",
        promoting_plan_sha256=PLAN,
    )
    full.register(first)
    full.register(second)
    prefix.register(first)

    with pytest.raises(PaperFokContractErrorV2, match="count differs"):
        _restore_registry(full, state=prefix.export_state_v2())


def test_registry_restore_rejects_nonexact_state_schema() -> None:
    registry = _registry_for(evaluate_paper_fok_entry_v2(_item()))
    state = json.loads(registry.export_state_v2())
    state["unexpected"] = True

    with pytest.raises(PaperFokContractErrorV2, match="schema"):
        _restore_registry(
            registry,
            state=json.dumps(state, separators=(",", ":")).encode() + b"\n",
        )


def test_registry_restore_rejects_pre_patch_rule_version() -> None:
    registry = _registry_for(evaluate_paper_fok_entry_v2(_item()))
    state = json.loads(registry.export_state_v2())
    encoded = state["events"][0]["payload_base64"]
    decision_document = json.loads(base64.b64decode(encoded))
    decision_document["rule_version"] = "R4B_CAUSAL_V2.3.0_PAPER_FOK_ENTRY"
    old_payload = (
        json.dumps(decision_document, separators=(",", ":")).encode() + b"\n"
    )
    state["events"][0]["payload_base64"] = base64.b64encode(
        old_payload
    ).decode()
    state["events"][0]["payload_sha256"] = hashlib.sha256(
        old_payload
    ).hexdigest()

    with pytest.raises(PaperFokContractErrorV2, match="frozen contract fields"):
        _restore_registry(
            registry,
            state=json.dumps(state, separators=(",", ":")).encode() + b"\n",
        )


def test_registry_root_is_permutation_invariant_and_restore_rejects_tamper() -> None:
    first = evaluate_paper_fok_entry_v2(_item())
    second = evaluate_paper_fok_entry_v2(
        _item(signal_event_id="99" * 32)
    )
    forward = PaperFokDecisionRegistryV2(
        maximum_events=2,
        attempt_id="attempt-1",
        promoting_plan_sha256=PLAN,
    )
    reverse = PaperFokDecisionRegistryV2(
        maximum_events=2,
        attempt_id="attempt-1",
        promoting_plan_sha256=PLAN,
    )
    for value in (first, second):
        forward.register(value)
    for value in (second, first):
        reverse.register(value)
    assert forward.replay_root_sha256 == reverse.replay_root_sha256
    assert forward.export_state_v2() == reverse.export_state_v2()

    state = json.loads(forward.export_state_v2())
    encoded = state["events"][0]["payload_base64"]
    payload = json.loads(base64.b64decode(encoded))
    payload["filled_quantity"] = "999"
    tampered = json.dumps(payload, separators=(",", ":")).encode() + b"\n"
    state["events"][0]["payload_base64"] = base64.b64encode(tampered).decode()
    state["events"][0]["payload_sha256"] = "00" * 32
    with pytest.raises(PaperFokContractErrorV2):
        _restore_registry(
            forward,
            state=json.dumps(state, separators=(",", ":")).encode() + b"\n",
        )


def test_registry_capacity_is_fail_closed_for_distinct_logical_event() -> None:
    registry = PaperFokDecisionRegistryV2(
        maximum_events=1,
        attempt_id="attempt-1",
        promoting_plan_sha256=PLAN,
    )
    registry.register(evaluate_paper_fok_entry_v2(_item()))
    with pytest.raises(PaperFokContractErrorV2, match="capacity exhausted"):
        registry.register(
            evaluate_paper_fok_entry_v2(
                _item(signal_event_id="99" * 32)
            )
        )


def test_top_level_spot_is_rejected_at_contract_boundary() -> None:
    with pytest.raises(PaperFokContractErrorV2, match="USD-M"):
        _item(venue=VenueV2.SPOT)


def test_decision_type_cannot_be_built_without_private_evaluator_seal() -> None:
    valid = evaluate_paper_fok_entry_v2(_item())
    values = {
        field: getattr(valid, field)
        for field in valid.__dataclass_fields__
        if field
        not in {
            "event_id",
            "payload_sha256",
            "role",
            "rule_version",
            "primary_depth_haircut",
            "price_cap_rate",
            "partial_primary_entry",
            "production_order_placement",
            "discord_timing_present",
            "discord_timing_can_change_result",
            "_factory_token",
        }
    }
    values["_factory_token"] = object()
    with pytest.raises(PaperFokContractErrorV2, match="causal evaluator"):
        PaperFokEntryDecisionV2(**values)  # type: ignore[arg-type]
