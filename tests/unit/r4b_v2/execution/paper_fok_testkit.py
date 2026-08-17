from __future__ import annotations

import hashlib
from decimal import Decimal

from signalbot.r4b_v2.alerts.actionability import CausalTargetCursorV2
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    CausalMarkPriceEvidenceV2,
    DepthLevelV2,
    FuturesDepthContinuityWitnessV2,
    FuturesDepthSnapshotV2,
    FuturesExchangeInfoEvidenceV2,
    FuturesStandardDepthEventV2,
    PaperFokClosureEvidenceV2,
    PaperFokDecisionRegistryV2,
    PaperFokEntryDecisionV2,
    PaperFokEntryInputV2,
    PaperFokFullFillCertificateV2,
    PaperFokLineageV2,
    PaperFokSideV2,
    RawQuantityFilterV2,
    evaluate_paper_fok_entry_v2,
    issue_paper_fok_full_fill_certificate_v2,
)


def build_usdm_paper_full_fill_v2(
    *,
    attempt_id: str,
    signal_event_id: str,
    symbol: str,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    side: PaperFokSideV2,
    requested_quantity: Decimal = Decimal("2.00"),
) -> tuple[
    PaperFokEntryDecisionV2,
    PaperFokFullFillCertificateV2,
    PaperFokDecisionRegistryV2,
]:
    """Build concrete factory-sealed PAPER evidence for strategy integration tests."""

    def digest(label: str) -> str:
        return hashlib.sha256(f"{signal_event_id}:{label}".encode()).hexdigest()

    source = digest("source")
    snapshot_schema = digest("snapshot-schema")
    depth_schema = digest("depth-schema")
    mark_schema = digest("mark-schema")
    exchange_schema = digest("exchange-schema")
    health_schema = digest("health-schema")
    target_venue = decision_cutoff_ms + 10_000
    target_local = decision_cutoff_ms + 100_000
    target_cursor = CausalTargetCursorV2(
        decision_cutoff_ms=decision_cutoff_ms,
        target_venue_ms=target_venue,
        prior_local_cursor_ms=target_local - 1,
        prior_venue_lower_bound_ms=target_venue - 1,
        target_local_cursor_ms=target_local,
        target_venue_lower_bound_ms=target_venue,
        clock_segment_root_sha256=digest("target-clock"),
        contiguous_cursor_evidence=True,
    )
    lineage = PaperFokLineageV2(
        promoting_plan_sha256=promoting_plan_sha256,
        source_root_sha256=source,
        depth_snapshot_schema_sha256=snapshot_schema,
        standard_depth_schema_sha256=depth_schema,
        mark_schema_sha256=mark_schema,
        exchange_info_schema_sha256=exchange_schema,
        health_schema_sha256=health_schema,
    )
    snapshot = FuturesDepthSnapshotV2(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=promoting_plan_sha256,
        source_root_sha256=source,
        schema_sha256=snapshot_schema,
        response_completion_ms=target_local - 1_000,
        last_update_id=100,
        depth_limit=2,
        bids=(
            DepthLevelV2(price=Decimal("99.00"), quantity=Decimal("4.00")),
            DepthLevelV2(price=Decimal("98.90"), quantity=Decimal("4.00")),
        ),
        asks=(
            DepthLevelV2(price=Decimal("100.00"), quantity=Decimal("4.00")),
            DepthLevelV2(price=Decimal("100.10"), quantity=Decimal("4.00")),
        ),
    )
    depth_event = FuturesStandardDepthEventV2(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=promoting_plan_sha256,
        source_root_sha256=source,
        schema_sha256=depth_schema,
        pair=symbol,
        routing_status=1,
        event_time_ms=target_venue - 1_000,
        transaction_time_ms=target_venue - 1_000,
        receipt_completion_ms=target_local - 500,
        ingest_seq=10,
        previous_same_stream_ingest_seq=0,
        first_update_id=100,
        final_update_id=101,
        previous_final_update_id=99,
        bids=(),
        asks=(),
    )
    successor = FuturesDepthContinuityWitnessV2(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=promoting_plan_sha256,
        source_root_sha256=source,
        schema_sha256=depth_schema,
        pair=symbol,
        routing_status=1,
        event_time_ms=target_venue - 500,
        transaction_time_ms=target_venue - 500,
        receipt_completion_ms=target_local + 1,
        ingest_seq=11,
        previous_same_stream_ingest_seq=10,
        first_update_id=102,
        final_update_id=102,
        previous_final_update_id=101,
    )
    closure = PaperFokClosureEvidenceV2(
        closure_grace_end_local_ms=target_local + 30_000,
        finalization_grace_binding_sha256=digest("finalization-grace"),
        finalized_through_local_ms=target_local,
        successor_candidates=(successor,),
    )
    mark = CausalMarkPriceEvidenceV2(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=promoting_plan_sha256,
        source_root_sha256=source,
        schema_sha256=mark_schema,
        pair=symbol,
        routing_status=1,
        mark_price=Decimal("100.00"),
        event_time_ms=target_venue - 2_000,
        receipt_completion_ms=target_local,
    )
    quantity_filter = RawQuantityFilterV2(
        min_qty=Decimal("0.01"),
        max_qty=Decimal("100.00"),
        step_size=Decimal("0.01"),
    )
    exchange_info = FuturesExchangeInfoEvidenceV2(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=promoting_plan_sha256,
        source_root_sha256=source,
        schema_sha256=exchange_schema,
        response_completion_ms=target_local - 1_000,
        version_valid_from_local_ms=target_local - 10_000,
        version_valid_through_local_ms=target_local,
        applicable_filter_inventory_complete=True,
        tick_size=Decimal("0.01"),
        min_price=Decimal("1.00"),
        max_price=Decimal("1000000.00"),
        percent_price_multiplier_down=Decimal("0.90"),
        percent_price_multiplier_up=Decimal("1.10"),
        market_take_bound=Decimal("0.05"),
        min_notional=Decimal(0),
        max_notional=Decimal(0),
        lot_size=quantity_filter,
        market_lot_size=quantity_filter,
    )
    paper_input = PaperFokEntryInputV2(
        attempt_id=attempt_id,
        signal_event_id=signal_event_id,
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        lineage=lineage,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        target_cursor=target_cursor,
        target_state_last_ingest_seq=10,
        side=side,
        requested_quantity=requested_quantity,
        snapshot=snapshot,
        pre_target_depth_events=(depth_event,),
        closure=closure,
        mark=mark,
        exchange_info=exchange_info,
    )
    decision = evaluate_paper_fok_entry_v2(paper_input)
    registry = PaperFokDecisionRegistryV2(
        maximum_events=4,
        attempt_id=attempt_id,
        promoting_plan_sha256=promoting_plan_sha256,
    )
    registry.register(decision)
    checkpoint = registry.terminal_checkpoint_v2()
    certificate = issue_paper_fok_full_fill_certificate_v2(
        decision,
        registry=registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    return decision, certificate, registry
