from __future__ import annotations

import json
from dataclasses import replace
from decimal import Context, Decimal, Underflow, localcontext

import pytest

from signalbot.r4b_v2.alerts.actionability import (
    AlertTransportTimesV2,
    PromotingFamilyV2,
)
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.fees import (
    FEE_POLL_CADENCE_MS_V2,
    FeeMultiplierV2,
    FeeTimelineCheckpointV2,
    FilledPositionFeeStatusV2,
    PublicFeeManifestV2,
    calculate_filled_position_fee_v2,
    canonical_filled_position_fee_v2,
    resolve_fee_version_v2,
)
from signalbot.r4b_v2.execution.mandatory_exit import (
    EXIT_RETRY_WINDOW_MS_V2,
    MANDATORY_EXIT_RULE_VERSION_V2,
    MandatoryExitAttemptStatusV2,
    MandatoryExitBookGenerationV2,
    MandatoryExitContractErrorV2,
    MandatoryExitLedgerV2,
    MandatoryExitRegistryDispositionV2,
    MandatoryExitTargetModeV2,
    MandatoryExitTerminalStatusV2,
    build_mandatory_exit_intent_v2,
    build_mandatory_exit_target_cursor_v2,
    canonical_mandatory_exit_fee_certificate_v2,
    mandatory_exit_position_from_certificate_v2,
)
from signalbot.r4b_v2.execution.paper_fok import (
    CausalMarkPriceEvidenceV2,
    DepthLevelV2,
    FuturesDepthContinuityWitnessV2,
    FuturesDepthSnapshotV2,
    FuturesExchangeInfoEvidenceV2,
    FuturesStandardDepthEventV2,
    PaperFokClosureEvidenceV2,
    PaperFokLineageV2,
    PaperFokSideV2,
    RawQuantityFilterV2,
)

from .paper_fok_testkit import build_usdm_paper_full_fill_v2
from .test_fees import _HORIZON_END, _T0, _manifest, _post, _registry, _timeline

ATTEMPT = "prospective-attempt-v2"
PLAN = "11" * 32
SIGNAL = "22" * 32
SOURCE = "33" * 32
SNAPSHOT_SCHEMA = "44" * 32
DEPTH_SCHEMA = "55" * 32
MARK_SCHEMA = "66" * 32
EXCHANGE_SCHEMA = "77" * 32
HEALTH_SCHEMA = "88" * 32
CLOCK_ROOT = "99" * 32
TRANSPORT_CHECKPOINT = "aa" * 32
FAMILY_CHECKPOINT = "bb" * 32
EXIT_DECISION_EVENT = "cc" * 32
EXIT_DECISION_PAYLOAD = "dd" * 32
GRACE_BINDING = "ee" * 32
EXIT_CUTOFF = 600_000
ACK_MS = 600_100
TARGET_VENUE = 610_100
TARGET_LOCAL = 700_000


def _level(price: str, quantity: str) -> DepthLevelV2:
    return DepthLevelV2(price=Decimal(price), quantity=Decimal(quantity))


def _certificate(*, side: PaperFokSideV2 = PaperFokSideV2.BUY):
    _, certificate, _ = build_usdm_paper_full_fill_v2(
        attempt_id=ATTEMPT,
        signal_event_id=SIGNAL,
        symbol="BTCUSDT",
        promoting_plan_sha256=PLAN,
        bar_open_ms=0,
        bar_close_ms=299_999,
        decision_cutoff_ms=302_000,
        side=side,
        requested_quantity=Decimal("2.00"),
    )
    return certificate


def _transport(*, ack_ms: int | None = ACK_MS) -> AlertTransportTimesV2:
    return AlertTransportTimesV2(
        durable_outbox_enqueue_ms=EXIT_CUTOFF,
        send_start_ms=EXIT_CUTOFF + 10,
        response_first_byte_ms=(None if ack_ms is None else ack_ms - 10),
        provider_acceptance_completion_ms=ack_ms,
        request_completion_ms=(None if ack_ms is None else ack_ms + 10),
    )


def _target(
    *,
    ack_ms: int | None = ACK_MS,
    exit_cutoff: int = EXIT_CUTOFF,
    target_venue: int = TARGET_VENUE,
    target_local: int = TARGET_LOCAL,
):
    return build_mandatory_exit_target_cursor_v2(
        exit_decision_cutoff_ms=exit_cutoff,
        transport_times=_transport(ack_ms=ack_ms),
        transport_ledger_checkpoint_sha256=TRANSPORT_CHECKPOINT,
        target_venue_ms=target_venue,
        prior_local_cursor_ms=target_local - 1,
        prior_venue_lower_bound_ms=target_venue - 1,
        target_local_cursor_ms=target_local,
        target_venue_lower_bound_ms=target_venue,
        clock_segment_root_sha256=CLOCK_ROOT,
    )


def _setup(
    *,
    entry_side: PaperFokSideV2 = PaperFokSideV2.BUY,
    target=None,
):
    position = mandatory_exit_position_from_certificate_v2(
        _certificate(side=entry_side),
        family=PromotingFamilyV2.B,
    )
    cursor = _target() if target is None else target
    intent = build_mandatory_exit_intent_v2(
        position,
        exit_decision_event_id=EXIT_DECISION_EVENT,
        exit_decision_payload_sha256=EXIT_DECISION_PAYLOAD,
        canonical_exit_decision=b'{"action":"EXIT_LONG"}',
        family_exit_registry_checkpoint_sha256=FAMILY_CHECKPOINT,
        family_rule_version="FAMILY_B_TEST_RULE",
        exit_reason="HARD_HORIZON",
        exit_decision_cutoff_ms=cursor.exit_decision_cutoff_ms,
        target_cursor=cursor,
    )
    ledger = MandatoryExitLedgerV2(
        maximum_events=32,
        maximum_positions=4,
        attempt_id=ATTEMPT,
        promoting_plan_sha256=PLAN,
    )
    assert ledger.register_position_v2(position) is MandatoryExitRegistryDispositionV2.NEW
    assert ledger.schedule_intent_v2(intent) is MandatoryExitRegistryDispositionV2.NEW
    return position, intent, ledger


def _fee_execution_setup():
    _, certificate, _ = build_usdm_paper_full_fill_v2(
        attempt_id=ATTEMPT,
        signal_event_id=SIGNAL,
        symbol="BTCUSDT",
        promoting_plan_sha256=PLAN,
        bar_open_ms=10_500_000,
        bar_close_ms=10_799_999,
        decision_cutoff_ms=10_802_000,
        side=PaperFokSideV2.BUY,
        requested_quantity=Decimal("2.00"),
    )
    position = mandatory_exit_position_from_certificate_v2(
        certificate,
        family=PromotingFamilyV2.B,
    )
    exit_target = _T0 + 2 * FEE_POLL_CADENCE_MS_V2
    target = _target(
        ack_ms=exit_target - 10_000,
        exit_cutoff=exit_target - 10_100,
        target_venue=exit_target,
        target_local=exit_target + 100_000,
    )
    intent = build_mandatory_exit_intent_v2(
        position,
        exit_decision_event_id=EXIT_DECISION_EVENT,
        exit_decision_payload_sha256=EXIT_DECISION_PAYLOAD,
        canonical_exit_decision=b'{"action":"EXIT_LONG"}',
        family_exit_registry_checkpoint_sha256=FAMILY_CHECKPOINT,
        family_rule_version="FAMILY_B_TEST_RULE",
        exit_reason="HARD_HORIZON",
        exit_decision_cutoff_ms=target.exit_decision_cutoff_ms,
        target_cursor=target,
    )
    ledger = MandatoryExitLedgerV2(
        maximum_events=32,
        maximum_positions=4,
        attempt_id=ATTEMPT,
        promoting_plan_sha256=PLAN,
    )
    ledger.register_position_v2(position)
    ledger.schedule_intent_v2(intent)
    return position, intent, ledger


def _extended_fee_evidence(
    *,
    first_rate: str | None = None,
    second_rate: str | None = None,
    third_rate: str | None = None,
):
    base = _manifest()
    horizon_end = _T0 + 3 * FEE_POLL_CADENCE_MS_V2
    manifest = PublicFeeManifestV2(
        scope=base.scope,
        t0_ms=_T0,
        horizon_end_ms=horizon_end,
        spot_capture=base.spot_capture,
        usdm_capture=base.usdm_capture,
    )
    registry = _registry(
        manifest,
        _post(VenueV2.USDM_FUTURES, 1, taker_percent=first_rate),
        _post(VenueV2.USDM_FUTURES, 2, taker_percent=second_rate),
        _post(VenueV2.USDM_FUTURES, 3, taker_percent=third_rate),
    )
    timeline = _timeline(manifest, registry, observed_through_ms=horizon_end)
    assert isinstance(timeline, FeeTimelineCheckpointV2)
    return manifest, registry, timeline


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
    local_ms: int = TARGET_LOCAL,
    bids: tuple[DepthLevelV2, ...] = (_level("99.00", "2.00"),),
    asks: tuple[DepthLevelV2, ...] = (_level("100.00", "2.00"),),
) -> FuturesDepthSnapshotV2:
    return FuturesDepthSnapshotV2(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=SOURCE,
        schema_sha256=SNAPSHOT_SCHEMA,
        response_completion_ms=local_ms - 1_000,
        last_update_id=200,
        depth_limit=3,
        bids=bids,
        asks=asks,
    )


def _event(
    *,
    venue_ms: int,
    local_ms: int,
    ingest_seq: int,
    previous_ingest_seq: int,
    first_update_id: int,
    final_update_id: int,
    previous_final_update_id: int,
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
        event_time_ms=venue_ms,
        transaction_time_ms=venue_ms,
        receipt_completion_ms=local_ms,
        ingest_seq=ingest_seq,
        previous_same_stream_ingest_seq=previous_ingest_seq,
        first_update_id=first_update_id,
        final_update_id=final_update_id,
        previous_final_update_id=previous_final_update_id,
        bids=bids,
        asks=asks,
    )


def _witness(event: FuturesStandardDepthEventV2) -> FuturesDepthContinuityWitnessV2:
    return FuturesDepthContinuityWitnessV2.from_event(event)


def _rules(
    *,
    local_ms: int = TARGET_LOCAL,
    min_notional: str = "0",
    tick_size: str = "0.01",
):
    quantity_filter = RawQuantityFilterV2(
        min_qty=Decimal("0.01"),
        max_qty=Decimal("100.00"),
        step_size=Decimal("0.01"),
    )
    return FuturesExchangeInfoEvidenceV2(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=SOURCE,
        schema_sha256=EXCHANGE_SCHEMA,
        response_completion_ms=local_ms - 1_000,
        version_valid_from_local_ms=local_ms - 20_000,
        version_valid_through_local_ms=local_ms + 100_000,
        applicable_filter_inventory_complete=True,
        tick_size=Decimal(tick_size),
        min_price=Decimal("1"),
        max_price=Decimal("1000000"),
        percent_price_multiplier_down=Decimal("0.50"),
        percent_price_multiplier_up=Decimal("1.50"),
        market_take_bound=Decimal("0.50"),
        min_notional=Decimal(min_notional),
        max_notional=Decimal(0),
        lot_size=quantity_filter,
        market_lot_size=quantity_filter,
    )


def _generation(
    position,
    intent,
    *,
    generation_venue_ms: int = TARGET_VENUE,
    generation_local_ms: int = TARGET_LOCAL,
    events: tuple[FuturesStandardDepthEventV2, ...] | None = None,
    successor: FuturesStandardDepthEventV2 | None = None,
    bids: tuple[DepthLevelV2, ...] = (_level("99.00", "2.00"),),
    asks: tuple[DepthLevelV2, ...] = (_level("100.00", "2.00"),),
    fee_resolution=None,
    min_notional: str = "0",
    tick_size: str = "0.01",
) -> MandatoryExitBookGenerationV2:
    if events is None:
        events = (
            _event(
                venue_ms=generation_venue_ms - 100,
                local_ms=generation_local_ms - 100,
                ingest_seq=20,
                previous_ingest_seq=0,
                first_update_id=200,
                final_update_id=201,
                previous_final_update_id=199,
            ),
        )
    if successor is None:
        successor = _event(
            venue_ms=generation_venue_ms + 1_000,
            local_ms=generation_local_ms + 1_000,
            ingest_seq=events[-1].ingest_seq + 1,
            previous_ingest_seq=events[-1].ingest_seq,
            first_update_id=events[-1].final_update_id + 1,
            final_update_id=events[-1].final_update_id + 1,
            previous_final_update_id=events[-1].final_update_id,
        )
    closure = PaperFokClosureEvidenceV2(
        closure_grace_end_local_ms=generation_local_ms + 10_000,
        finalization_grace_binding_sha256=GRACE_BINDING,
        finalized_through_local_ms=generation_local_ms,
        successor_candidates=(_witness(successor),),
    )
    mark = CausalMarkPriceEvidenceV2(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=SOURCE,
        schema_sha256=MARK_SCHEMA,
        pair="BTCUSDT",
        routing_status=1,
        mark_price=Decimal("100"),
        event_time_ms=generation_venue_ms - 2_000,
        receipt_completion_ms=generation_local_ms,
    )
    return MandatoryExitBookGenerationV2(
        position_event_id=position.event_id,
        intent_event_id=intent.event_id,
        lineage=_lineage(),
        generation_venue_ms=generation_venue_ms,
        generation_local_cursor_ms=generation_local_ms,
        target_state_last_ingest_seq=events[-1].ingest_seq,
        snapshot=_snapshot(local_ms=generation_local_ms, bids=bids, asks=asks),
        pre_generation_depth_events=events,
        closure=closure,
        mark=mark,
        exchange_info=_rules(
            local_ms=generation_local_ms,
            min_notional=min_notional,
            tick_size=tick_size,
        ),
        fee_resolution=fee_resolution,
    )


def _apply_two_exit_slices(position, intent, ledger):
    target_venue = intent.target_cursor.target_venue_ms
    target_local = intent.target_cursor.target_local_cursor_ms
    next_event = _event(
        venue_ms=target_venue + 1_000,
        local_ms=target_local + 1_000,
        ingest_seq=21,
        previous_ingest_seq=20,
        first_update_id=202,
        final_update_id=202,
        previous_final_update_id=201,
        bids=(_level("98.00", "2.00"),),
    )
    first_generation = _generation(
        position,
        intent,
        generation_venue_ms=target_venue,
        generation_local_ms=target_local,
        bids=(_level("99.00", "2.00"),),
        successor=next_event,
    )
    first = ledger.apply_generation_v2(first_generation)
    final_witness = _event(
        venue_ms=target_venue + 2_000,
        local_ms=target_local + 2_000,
        ingest_seq=22,
        previous_ingest_seq=21,
        first_update_id=203,
        final_update_id=203,
        previous_final_update_id=202,
    )
    second_generation = _generation(
        position,
        intent,
        generation_venue_ms=next_event.transaction_time_ms,
        generation_local_ms=next_event.receipt_completion_ms,
        events=(first_generation.pre_generation_depth_events[0], next_event),
        successor=final_witness,
        bids=(_level("99.00", "2.00"),),
    )
    second = ledger.apply_generation_v2(second_generation)
    return first_generation, first, second


def test_long_exit_ignores_wide_spread_and_feature_band_and_books_partial() -> None:
    position, intent, ledger = _setup()
    generation = _generation(
        position,
        intent,
        bids=(_level("80.00", "2.00"),),
        asks=(_level("120.00", "2.00"),),
    )

    attempt = ledger.apply_generation_v2(generation)
    state = ledger.state_for_position_v2(position.event_id)

    assert attempt.status is MandatoryExitAttemptStatusV2.PARTIAL_FILL
    assert attempt.exit_side is PaperFokSideV2.SELL
    assert attempt.filled_quantity == Decimal("1.00")
    assert attempt.gross_notional == Decimal("80.0000")
    assert state.residual_quantity == Decimal("1.00")
    assert any("10BP" in reason for reason in attempt.reasons)
    assert state.final_fee_cost_complete is False


def test_short_exit_walks_asks_and_signed_cashflow_is_negative() -> None:
    position, intent, ledger = _setup(entry_side=PaperFokSideV2.SELL)

    attempt = ledger.apply_generation_v2(
        _generation(position, intent, asks=(_level("101.00", "4.00"),))
    )

    assert attempt.status is MandatoryExitAttemptStatusV2.FULL_FILL
    assert attempt.exit_side is PaperFokSideV2.BUY
    assert attempt.filled_quantity == position.initial_quantity
    assert attempt.signed_gross_cashflow == Decimal("-202.0000")
    assert ledger.state_for_position_v2(position.event_id).terminal is not None


def test_partial_retry_requires_each_next_generation_and_resets_only_updated_level() -> None:
    position, intent, ledger = _setup()
    next_event = _event(
        venue_ms=TARGET_VENUE + 1_000,
        local_ms=TARGET_LOCAL + 1_000,
        ingest_seq=21,
        previous_ingest_seq=20,
        first_update_id=202,
        final_update_id=202,
        previous_final_update_id=201,
        asks=(_level("100.00", "2.00"),),
    )
    first = _generation(position, intent, successor=next_event)
    assert ledger.apply_generation_v2(first).filled_quantity == Decimal("1.00")

    event20 = first.pre_generation_depth_events[0]
    event22 = _event(
        venue_ms=TARGET_VENUE + 2_000,
        local_ms=TARGET_LOCAL + 2_000,
        ingest_seq=22,
        previous_ingest_seq=21,
        first_update_id=203,
        final_update_id=203,
        previous_final_update_id=202,
        bids=(_level("99.00", "2.00"),),
    )
    unchanged_retry = _generation(
        position,
        intent,
        generation_venue_ms=next_event.transaction_time_ms,
        generation_local_ms=next_event.receipt_completion_ms,
        events=(event20, next_event),
        successor=event22,
    )
    unchanged = ledger.apply_generation_v2(unchanged_retry)
    assert unchanged.status is MandatoryExitAttemptStatusV2.NO_FILL
    assert unchanged.filled_quantity == 0

    event23 = _event(
        venue_ms=TARGET_VENUE + 3_000,
        local_ms=TARGET_LOCAL + 3_000,
        ingest_seq=23,
        previous_ingest_seq=22,
        first_update_id=204,
        final_update_id=204,
        previous_final_update_id=203,
    )
    reset_retry = _generation(
        position,
        intent,
        generation_venue_ms=event22.transaction_time_ms,
        generation_local_ms=event22.receipt_completion_ms,
        events=(event20, next_event, event22),
        successor=event23,
    )
    reset = ledger.apply_generation_v2(reset_retry)
    assert reset.status is MandatoryExitAttemptStatusV2.FULL_FILL
    state = ledger.state_for_position_v2(position.event_id)
    assert state.total_filled_quantity == position.initial_quantity
    assert state.residual_quantity == 0


def test_skipping_an_intermediate_retry_generation_is_inconclusive() -> None:
    position, intent, ledger = _setup()
    first = _generation(position, intent)
    ledger.apply_generation_v2(first)
    event20 = first.pre_generation_depth_events[0]
    skipped = _event(
        venue_ms=TARGET_VENUE + 2_000,
        local_ms=TARGET_LOCAL + 2_000,
        ingest_seq=22,
        previous_ingest_seq=21,
        first_update_id=202,
        final_update_id=202,
        previous_final_update_id=201,
        bids=(_level("99.00", "2.00"),),
    )
    successor = _event(
        venue_ms=TARGET_VENUE + 3_000,
        local_ms=TARGET_LOCAL + 3_000,
        ingest_seq=23,
        previous_ingest_seq=22,
        first_update_id=203,
        final_update_id=203,
        previous_final_update_id=202,
    )
    attempt = ledger.apply_generation_v2(
        _generation(
            position,
            intent,
            generation_venue_ms=skipped.transaction_time_ms,
            generation_local_ms=skipped.receipt_completion_ms,
            events=(event20, skipped),
            successor=successor,
        )
    )
    assert attempt.status is MandatoryExitAttemptStatusV2.INCONCLUSIVE_DATA
    assert attempt.filled_quantity == 0
    assert ledger.state_for_position_v2(position.event_id).family_inconclusive


def test_target_plus_30000_is_inclusive_but_plus_30001_cannot_change_inventory() -> None:
    position, intent, ledger = _setup()
    first = _generation(
        position,
        intent,
        bids=(_level("99.00", "0.01"),),
    )
    ledger.apply_generation_v2(first)
    event20 = first.pre_generation_depth_events[0]
    boundary = _event(
        venue_ms=TARGET_VENUE + EXIT_RETRY_WINDOW_MS_V2,
        local_ms=TARGET_LOCAL + EXIT_RETRY_WINDOW_MS_V2,
        ingest_seq=21,
        previous_ingest_seq=20,
        first_update_id=202,
        final_update_id=202,
        previous_final_update_id=201,
        bids=(_level("99.00", "4.00"),),
    )
    successor = _event(
        venue_ms=boundary.transaction_time_ms + 1,
        local_ms=boundary.receipt_completion_ms + 1,
        ingest_seq=22,
        previous_ingest_seq=21,
        first_update_id=203,
        final_update_id=203,
        previous_final_update_id=202,
    )
    accepted = ledger.apply_generation_v2(
        _generation(
            position,
            intent,
            generation_venue_ms=boundary.transaction_time_ms,
            generation_local_ms=boundary.receipt_completion_ms,
            events=(event20, boundary),
            successor=successor,
            bids=(_level("99.00", "0.01"),),
        )
    )
    assert accepted.status is MandatoryExitAttemptStatusV2.FULL_FILL

    position2, intent2, ledger2 = _setup()
    with pytest.raises(MandatoryExitContractErrorV2, match="after target plus"):
        ledger2.apply_generation_v2(
            _generation(
                position2,
                intent2,
                generation_venue_ms=TARGET_VENUE + EXIT_RETRY_WINDOW_MS_V2 + 1,
                generation_local_ms=TARGET_LOCAL + EXIT_RETRY_WINDOW_MS_V2 + 1,
            )
        )


def test_plus_30001_finalizes_non_dust_residual_without_deletion() -> None:
    position, intent, ledger = _setup()
    ledger.apply_generation_v2(
        _generation(position, intent, bids=(_level("99.00", "0.01"),))
    )
    with pytest.raises(MandatoryExitContractErrorV2, match="remains open"):
        ledger.finalize_retry_window_v2(
            position.event_id,
            finalized_at_venue_ms=TARGET_VENUE + EXIT_RETRY_WINDOW_MS_V2,
        )
    terminal = ledger.finalize_retry_window_v2(
        position.event_id,
        finalized_at_venue_ms=TARGET_VENUE + EXIT_RETRY_WINDOW_MS_V2 + 1,
    )
    state = ledger.state_for_position_v2(position.event_id)
    assert terminal.terminal_status is MandatoryExitTerminalStatusV2.POST_ENTRY_UNRESOLVED_EXIT
    assert terminal.family_inconclusive
    assert state.residual_quantity == position.initial_quantity
    assert state.position.event_id == position.event_id


def test_filter_dust_is_retained_as_distinct_terminal_inventory() -> None:
    position, intent, ledger = _setup()
    attempt = ledger.apply_generation_v2(
        _generation(position, intent, min_notional="1000")
    )
    assert attempt.residual_is_filter_dust
    terminal = ledger.finalize_retry_window_v2(
        position.event_id,
        finalized_at_venue_ms=TARGET_VENUE + EXIT_RETRY_WINDOW_MS_V2 + 1,
    )
    assert terminal.terminal_status is MandatoryExitTerminalStatusV2.DUST_RESIDUAL_RETAINED
    assert terminal.residual_quantity == position.initial_quantity


def test_missing_ack_uses_plus_15000_and_makes_even_full_exit_inconclusive() -> None:
    emergency_target = EXIT_CUTOFF + 15_000
    cursor = _target(
        ack_ms=None,
        target_venue=emergency_target,
        target_local=TARGET_LOCAL,
    )
    position, intent, ledger = _setup(target=cursor)
    attempt = ledger.apply_generation_v2(
        _generation(
            position,
            intent,
            generation_venue_ms=emergency_target,
            generation_local_ms=TARGET_LOCAL,
            bids=(_level("99.00", "4.00"),),
        )
    )
    state = ledger.state_for_position_v2(position.event_id)
    assert cursor.mode is MandatoryExitTargetModeV2.MISSING_ACK_EMERGENCY_PLUS_15000
    assert attempt.status is MandatoryExitAttemptStatusV2.FULL_FILL
    assert state.terminal is not None and state.terminal.family_inconclusive


def test_ack_target_equality_is_required_and_plus_one_is_rejected() -> None:
    with pytest.raises(MandatoryExitContractErrorV2, match="frozen ack"):
        _target(target_venue=TARGET_VENUE + 1)


def test_invalid_target_closure_retains_inventory_and_marks_family_inconclusive() -> None:
    position, intent, ledger = _setup()
    generation = _generation(position, intent)
    invalid = replace(
        generation,
        closure=replace(
            generation.closure,
            successor_candidates=(),
            finalized_through_local_ms=(
                generation.closure.closure_grace_end_local_ms
            ),
        ),
    )
    attempt = ledger.apply_generation_v2(invalid)
    state = ledger.state_for_position_v2(position.event_id)
    assert attempt.status is MandatoryExitAttemptStatusV2.INCONCLUSIVE_DATA
    assert attempt.filled_quantity == 0
    assert state.residual_quantity == position.initial_quantity
    assert state.family_inconclusive


def test_snapshot_only_exit_generation_retains_position_as_inconclusive() -> None:
    position, intent, ledger = _setup()
    snapshot_only = replace(
        _generation(position, intent),
        target_state_last_ingest_seq=0,
        pre_generation_depth_events=(),
    )

    attempt = ledger.apply_generation_v2(snapshot_only)
    state = ledger.state_for_position_v2(position.event_id)

    assert attempt.status is MandatoryExitAttemptStatusV2.INCONCLUSIVE_DATA
    assert attempt.reasons == ("DEPTH_SNAPSHOT_BRIDGE_MISSING",)
    assert attempt.filled_quantity == 0
    assert state.residual_quantity == position.initial_quantity
    assert state.position.event_id == position.event_id
    assert state.family_inconclusive


def test_mandatory_exit_patch_rule_version_is_bound_into_ledger_state() -> None:
    position, intent, ledger = _setup()
    ledger.apply_generation_v2(_generation(position, intent))
    state_document = json.loads(ledger.export_state_v2())

    assert MANDATORY_EXIT_RULE_VERSION_V2 == (
        "R4B_CAUSAL_V2.3.1_MANDATORY_USDM_POST_ENTRY_EXIT"
    )
    assert state_document["states"][0]["attempts"][0]["rule_version"] == (
        MANDATORY_EXIT_RULE_VERSION_V2
    )


def test_terminal_generation_duplicate_is_exactly_once_and_capacity_is_bounded() -> None:
    position, intent, ledger = _setup()
    generation = _generation(
        position,
        intent,
        bids=(_level("99", "4"),),
    )
    first = ledger.apply_generation_v2(generation)
    event_count = ledger.event_count
    duplicate = ledger.apply_generation_v2(generation)
    assert duplicate == first
    assert ledger.event_count == event_count

    certificate = _certificate()
    first_position = mandatory_exit_position_from_certificate_v2(
        certificate,
        family=PromotingFamilyV2.B,
    )
    second_position = mandatory_exit_position_from_certificate_v2(
        certificate,
        family=PromotingFamilyV2.C,
    )
    bounded = MandatoryExitLedgerV2(
        maximum_events=8,
        maximum_positions=1,
        attempt_id=ATTEMPT,
        promoting_plan_sha256=PLAN,
    )
    bounded.register_position_v2(first_position)
    with pytest.raises(MandatoryExitContractErrorV2, match="position capacity"):
        bounded.register_position_v2(second_position)


def test_later_irrelevant_successor_does_not_rewrite_generation_evidence() -> None:
    position, intent, _ = _setup()
    first = _generation(position, intent)
    later = _event(
        venue_ms=TARGET_VENUE + 2_000,
        local_ms=TARGET_LOCAL + 2_000,
        ingest_seq=22,
        previous_ingest_seq=21,
        first_update_id=203,
        final_update_id=203,
        previous_final_update_id=202,
    )
    appended = replace(
        first,
        closure=replace(
            first.closure,
            successor_candidates=(*first.closure.successor_candidates, _witness(later)),
            finalized_through_local_ms=TARGET_LOCAL + 50_000,
        ),
    )
    assert appended.event_id == first.event_id
    assert appended.evidence_sha256 == first.evidence_sha256


def test_worsening_exit_depth_cannot_improve_long_proceeds_or_short_cost() -> None:
    long_position, long_intent, long_good = _setup()
    _, _, long_bad = _setup()
    good_long = long_good.apply_generation_v2(
        _generation(long_position, long_intent, bids=(_level("99", "4"),))
    )
    bad_long = long_bad.apply_generation_v2(
        _generation(long_position, long_intent, bids=(_level("90", "4"),))
    )
    assert bad_long.signed_gross_cashflow <= good_long.signed_gross_cashflow

    short_position, short_intent, short_good = _setup(entry_side=PaperFokSideV2.SELL)
    _, _, short_bad = _setup(entry_side=PaperFokSideV2.SELL)
    good_short = short_good.apply_generation_v2(
        _generation(short_position, short_intent, asks=(_level("101", "4"),))
    )
    bad_short = short_bad.apply_generation_v2(
        _generation(short_position, short_intent, asks=(_level("110", "4"),))
    )
    assert bad_short.signed_gross_cashflow <= good_short.signed_gross_cashflow


def test_restore_requires_external_root_count_capacity_scope_and_checkpoint() -> None:
    position, intent, ledger = _setup()
    ledger.apply_generation_v2(
        _generation(position, intent, bids=(_level("99", "4"),))
    )
    checkpoint = ledger.terminal_checkpoint_v2()
    restored = MandatoryExitLedgerV2.from_state_v2(
        ledger.export_state_v2(),
        expected_attempt_id=checkpoint.attempt_id,
        expected_promoting_plan_sha256=checkpoint.promoting_plan_sha256,
        expected_replay_root_sha256=checkpoint.replay_root_sha256,
        expected_state_root_sha256=checkpoint.state_root_sha256,
        expected_event_count=checkpoint.event_count,
        expected_position_count=checkpoint.position_count,
        expected_maximum_events=checkpoint.maximum_events,
        expected_maximum_positions=checkpoint.maximum_positions,
        expected_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    assert restored.export_state_v2() == ledger.export_state_v2()
    assert restored.replay_root_sha256 == ledger.replay_root_sha256

    truncated = json.loads(ledger.export_state_v2())
    truncated["states"] = []
    with pytest.raises(MandatoryExitContractErrorV2):
        MandatoryExitLedgerV2.from_state_v2(
            json.dumps(truncated).encode(),
            expected_attempt_id=checkpoint.attempt_id,
            expected_promoting_plan_sha256=checkpoint.promoting_plan_sha256,
            expected_replay_root_sha256=checkpoint.replay_root_sha256,
            expected_state_root_sha256=checkpoint.state_root_sha256,
            expected_event_count=checkpoint.event_count,
            expected_position_count=checkpoint.position_count,
            expected_maximum_events=checkpoint.maximum_events,
            expected_maximum_positions=checkpoint.maximum_positions,
            expected_checkpoint_sha256=checkpoint.checkpoint_sha256,
        )


def test_cross_position_replay_root_is_independent_of_arrival_order() -> None:
    certificate = _certificate()
    positions = tuple(
        mandatory_exit_position_from_certificate_v2(certificate, family=family)
        for family in (PromotingFamilyV2.A, PromotingFamilyV2.B)
    )
    cursor = _target()
    intents = tuple(
        build_mandatory_exit_intent_v2(
            position,
            exit_decision_event_id=("ca" if index == 0 else "cb") * 32,
            exit_decision_payload_sha256=("da" if index == 0 else "db") * 32,
            canonical_exit_decision=f'{{"family":"{position.family.value}"}}'.encode(),
            family_exit_registry_checkpoint_sha256=FAMILY_CHECKPOINT,
            family_rule_version=f"FAMILY_{position.family.value}_TEST_RULE",
            exit_reason="HARD_HORIZON",
            exit_decision_cutoff_ms=cursor.exit_decision_cutoff_ms,
            target_cursor=cursor,
        )
        for index, position in enumerate(positions)
    )

    def build(order: tuple[int, int]) -> MandatoryExitLedgerV2:
        ledger = MandatoryExitLedgerV2(
            maximum_events=16,
            maximum_positions=4,
            attempt_id=ATTEMPT,
            promoting_plan_sha256=PLAN,
        )
        for index in order:
            ledger.register_position_v2(positions[index])
            ledger.schedule_intent_v2(intents[index])
        return ledger

    forward = build((0, 1))
    reverse = build((1, 0))
    assert forward.replay_root_sha256 == reverse.replay_root_sha256
    assert forward.state_root_sha256 == reverse.state_root_sha256


def test_fee_version_binds_exact_fill_event_timestamp_equality_not_plus_one() -> None:
    target = _T0
    exit_cutoff = target - 10_100
    cursor = _target(
        ack_ms=target - 10_000,
        exit_cutoff=exit_cutoff,
        target_venue=target,
        target_local=target + 100_000,
    )
    position, intent, ledger = _setup(target=cursor)
    manifest = _manifest()
    registry = _registry(
        manifest,
        _post(VenueV2.USDM_FUTURES, 1),
        _post(VenueV2.USDM_FUTURES, 2),
    )
    timeline = _timeline(manifest, registry, observed_through_ms=_HORIZON_END)
    assert isinstance(timeline, FeeTimelineCheckpointV2)
    exact = resolve_fee_version_v2(
        manifest,
        registry,
        timeline,
        target_ms=target,
        symbol="BTCUSDT",
        position_event_id=position.event_id,
        expected_timeline_checkpoint_sha256=timeline.checkpoint_sha256,
    )
    accepted = ledger.apply_generation_v2(
        _generation(
            position,
            intent,
            generation_venue_ms=target,
            generation_local_ms=target + 100_000,
            bids=(_level("99", "4"),),
            fee_resolution=exact,
        )
    )
    assert accepted.fee_resolution_event_id == exact.event_id
    assert accepted.fee_resolution_status == "RESOLVED"
    assert ledger.state_for_position_v2(position.event_id).final_fee_cost_complete is False

    position2, intent2, ledger2 = _setup(target=cursor)
    plus_one = resolve_fee_version_v2(
        manifest,
        registry,
        timeline,
        target_ms=target + 1,
        symbol="BTCUSDT",
        position_event_id=position2.event_id,
        expected_timeline_checkpoint_sha256=timeline.checkpoint_sha256,
    )
    with pytest.raises(MandatoryExitContractErrorV2, match="target differs"):
        ledger2.apply_generation_v2(
            _generation(
                position2,
                intent2,
                generation_venue_ms=target,
                generation_local_ms=target + 100_000,
                bids=(_level("99", "4"),),
                fee_resolution=plus_one,
            )
        )


def test_decimal34_is_independent_of_hostile_ambient_context() -> None:
    position, intent, ledger = _setup()
    hostile = Context(prec=7)
    hostile.traps[Underflow] = False
    with localcontext(hostile):
        attempt = ledger.apply_generation_v2(
            _generation(
                position,
                intent,
                bids=(_level("99.12345678901234567890123456789012", "4"),),
                tick_size="0.00000000000000000000000000000001",
            )
        )
    assert attempt.gross_notional == Decimal("198.2469135780246913578024691357802")
    assert attempt.executable_vwap == Decimal("99.1234567890123456789012345678901")


def test_fee_certificate_reuses_slice_idempotency_and_restart_checkpoint() -> None:
    position, intent, ledger = _fee_execution_setup()
    generation = _generation(
        position,
        intent,
        generation_venue_ms=intent.target_cursor.target_venue_ms,
        generation_local_ms=intent.target_cursor.target_local_cursor_ms,
        bids=(_level("99.00", "2.00"),),
    )
    first = ledger.apply_generation_v2(generation)
    event_count = ledger.event_count
    assert ledger.apply_generation_v2(generation) == first
    assert ledger.event_count == event_count
    conflicting = replace(
        generation,
        snapshot=_snapshot(
            local_ms=intent.target_cursor.target_local_cursor_ms,
            bids=(_level("97.00", "2.00"),),
        ),
    )
    with pytest.raises(MandatoryExitContractErrorV2, match="conflicting generation"):
        ledger.apply_generation_v2(conflicting)
    certificate = ledger.issue_fee_certificate_v2(position.event_id)
    exported = canonical_mandatory_exit_fee_certificate_v2(certificate)
    assert len(certificate.filled_exit_attempts) == 1
    checkpoint = ledger.terminal_checkpoint_v2()
    restored = MandatoryExitLedgerV2.from_state_v2(
        ledger.export_state_v2(),
        expected_attempt_id=ATTEMPT,
        expected_promoting_plan_sha256=PLAN,
        expected_replay_root_sha256=checkpoint.replay_root_sha256,
        expected_state_root_sha256=checkpoint.state_root_sha256,
        expected_event_count=checkpoint.event_count,
        expected_position_count=checkpoint.position_count,
        expected_maximum_events=checkpoint.maximum_events,
        expected_maximum_positions=checkpoint.maximum_positions,
        expected_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    assert canonical_mandatory_exit_fee_certificate_v2(
        restored.issue_fee_certificate_v2(position.event_id)
    ) == exported
    manifest, registry, timeline = _extended_fee_evidence(
        second_rate="0.0600",
        third_rate="0.0600",
    )
    partial_fee = calculate_filled_position_fee_v2(
        certificate,
        manifest=manifest,
        registry=registry,
        final_timeline_checkpoint=timeline,
        expected_final_timeline_checkpoint_sha256=timeline.checkpoint_sha256,
        multiplier=FeeMultiplierV2.PRIMARY_1_0X,
    )
    assert partial_fee.status is FilledPositionFeeStatusV2.PARTIAL_EXIT
    assert partial_fee.known_realized_exit_fee == Decimal("0.059400000")
    assert partial_fee.final_total_fee is None


@pytest.mark.parametrize(
    ("multiplier", "expected_total"),
    (
        (FeeMultiplierV2.PRIMARY_1_0X, Decimal("0.218200000")),
        (FeeMultiplierV2.PRIMARY_1_5X, Decimal("0.3273000000")),
        (FeeMultiplierV2.MANDATORY_ADVERSE_2_0X, Decimal("0.436400000")),
    ),
)
def test_multi_slice_fee_re_resolves_each_version_and_multiplier(
    multiplier: FeeMultiplierV2,
    expected_total: Decimal,
) -> None:
    position, intent, ledger = _fee_execution_setup()
    _, first, second = _apply_two_exit_slices(position, intent, ledger)
    assert (first.gross_notional, second.gross_notional) == (
        Decimal("99.0000"),
        Decimal("98.0000"),
    )
    certificate = ledger.issue_fee_certificate_v2(position.event_id)
    manifest, registry, timeline = _extended_fee_evidence(
        second_rate="0.0600",
        third_rate="0.0600",
    )
    fee = calculate_filled_position_fee_v2(
        certificate,
        manifest=manifest,
        registry=registry,
        final_timeline_checkpoint=timeline,
        expected_final_timeline_checkpoint_sha256=timeline.checkpoint_sha256,
        multiplier=multiplier,
    )
    assert fee.status is FilledPositionFeeStatusV2.BOTH_LEGS_COMPLETE
    assert fee.known_realized_total_fee == expected_total
    assert fee.entry_slice.taker_rate == Decimal("0.000500")
    assert tuple(value.taker_rate for value in fee.exit_slices) == (
        Decimal("0.000600"),
        Decimal("0.000600"),
    )
    assert tuple(value.execution_event_id for value in fee.exit_slices) == tuple(
        value.event_id for value in certificate.filled_exit_attempts
    )
    assert tuple(value.fill_event_ms for value in fee.exit_slices) == tuple(
        value.generation_venue_ms for value in certificate.filled_exit_attempts
    )
    assert fee.legacy_single_exit_fee is None
    assert json.loads(canonical_filled_position_fee_v2(fee))["payload_sha256"] == (
        fee.payload_sha256
    )


def test_single_exit_reuses_legacy_both_leg_owner() -> None:
    position, intent, ledger = _fee_execution_setup()
    ledger.apply_generation_v2(
        _generation(
            position,
            intent,
            generation_venue_ms=intent.target_cursor.target_venue_ms,
            generation_local_ms=intent.target_cursor.target_local_cursor_ms,
            bids=(_level("99.00", "4.00"),),
        )
    )
    manifest, registry, timeline = _extended_fee_evidence(
        second_rate="0.0600",
        third_rate="0.0600",
    )
    fee = calculate_filled_position_fee_v2(
        ledger.issue_fee_certificate_v2(position.event_id),
        manifest=manifest,
        registry=registry,
        final_timeline_checkpoint=timeline,
        expected_final_timeline_checkpoint_sha256=timeline.checkpoint_sha256,
        multiplier=FeeMultiplierV2.PRIMARY_1_0X,
    )
    assert fee.status is FilledPositionFeeStatusV2.BOTH_LEGS_COMPLETE
    assert fee.legacy_single_exit_fee is not None
    assert fee.legacy_single_exit_fee.total_fee == fee.known_realized_total_fee


def test_unresolved_slice_preserves_known_subtotal_and_never_claims_complete() -> None:
    position, intent, ledger = _fee_execution_setup()
    _apply_two_exit_slices(position, intent, ledger)
    manifest, registry, timeline = _extended_fee_evidence(
        second_rate="0.0600",
        third_rate="0.0700",
    )
    fee = calculate_filled_position_fee_v2(
        ledger.issue_fee_certificate_v2(position.event_id),
        manifest=manifest,
        registry=registry,
        final_timeline_checkpoint=timeline,
        expected_final_timeline_checkpoint_sha256=timeline.checkpoint_sha256,
        multiplier=FeeMultiplierV2.PRIMARY_1_0X,
    )
    assert fee.terminal_status == "EXITED_FULL"
    assert fee.status is FilledPositionFeeStatusV2.INCOMPLETE_FEE_VERSION
    assert fee.unresolved_slice_count == 1
    assert fee.exit_slices[0].realized_fee == Decimal("0.059400000")
    assert fee.exit_slices[1].taker_rate is None
    assert fee.exit_slices[1].realized_fee is None
    assert fee.known_realized_total_fee == Decimal("0.159400000")
    assert fee.final_total_fee is None


def test_dust_keeps_entry_fee_but_cannot_be_both_legs_complete() -> None:
    position, intent, ledger = _fee_execution_setup()
    ledger.apply_generation_v2(
        _generation(
            position,
            intent,
            generation_venue_ms=intent.target_cursor.target_venue_ms,
            generation_local_ms=intent.target_cursor.target_local_cursor_ms,
            min_notional="1000",
        )
    )
    ledger.finalize_retry_window_v2(
        position.event_id,
        finalized_at_venue_ms=intent.retry_deadline_venue_ms + 1,
    )
    manifest, registry, timeline = _extended_fee_evidence()
    fee = calculate_filled_position_fee_v2(
        ledger.issue_fee_certificate_v2(position.event_id),
        manifest=manifest,
        registry=registry,
        final_timeline_checkpoint=timeline,
        expected_final_timeline_checkpoint_sha256=timeline.checkpoint_sha256,
        multiplier=FeeMultiplierV2.PRIMARY_1_0X,
    )
    assert fee.status is FilledPositionFeeStatusV2.ENTRY_ONLY
    assert fee.terminal_status == "DUST_RESIDUAL_RETAINED"
    assert fee.known_realized_entry_fee == Decimal("0.100000000")
    assert fee.final_total_fee is None


def test_higher_fee_inputs_cannot_reduce_multi_slice_decimal34_sum() -> None:
    position, intent, ledger = _fee_execution_setup()
    _apply_two_exit_slices(position, intent, ledger)
    certificate = ledger.issue_fee_certificate_v2(position.event_id)
    low_manifest, low_registry, low_timeline = _extended_fee_evidence()
    high_manifest, high_registry, high_timeline = _extended_fee_evidence(
        second_rate="0.0600",
        third_rate="0.0600",
    )
    hostile = Context(prec=7)
    hostile.traps[Underflow] = False
    with localcontext(hostile):
        low = calculate_filled_position_fee_v2(
            certificate,
            manifest=low_manifest,
            registry=low_registry,
            final_timeline_checkpoint=low_timeline,
            expected_final_timeline_checkpoint_sha256=low_timeline.checkpoint_sha256,
            multiplier=FeeMultiplierV2.PRIMARY_1_0X,
        )
        high = calculate_filled_position_fee_v2(
            certificate,
            manifest=high_manifest,
            registry=high_registry,
            final_timeline_checkpoint=high_timeline,
            expected_final_timeline_checkpoint_sha256=high_timeline.checkpoint_sha256,
            multiplier=FeeMultiplierV2.PRIMARY_1_0X,
        )
    assert high.known_realized_total_fee >= low.known_realized_total_fee
