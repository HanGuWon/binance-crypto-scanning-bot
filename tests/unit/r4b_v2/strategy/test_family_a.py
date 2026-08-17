from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import ROUND_DOWN, ROUND_UP, Context, Decimal, localcontext
from typing import cast

import pytest
from hypothesis import given
from hypothesis import strategies as st

from signalbot.r4b_v2.alerts.actionability import CausalTargetCursorV2
from signalbot.r4b_v2.canonical import canonical_json_line
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
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.strategy.family_a import (
    FAMILY_A_HARD_HORIZON_BARS_V2,
    FamilyAAdmissionDispositionV2,
    FamilyAAdmissionReceiptV2,
    FamilyAContractError,
    FamilyADecisionRegistryV2,
    FamilyAEntryCommitDispositionV2,
    FamilyAEntryCommitReceiptV2,
    FamilyAEntryDecisionV2,
    FamilyAEntryInputV2,
    FamilyAEntryPreviewV2,
    FamilyAEntryStatusV2,
    FamilyAEpisodeLedgerV2,
    FamilyAExitActionV2,
    FamilyAExitDecisionV2,
    FamilyAExitDispositionV2,
    FamilyAExitInputV2,
    FamilyAExitMutationReceiptV2,
    FamilyAExitReasonV2,
    FamilyAIntervalStatusV2,
    FamilyAPositionV2,
    FamilyARegistryDispositionV2,
    FamilyASideV2,
    canonical_family_a_entry_decision_v2,
    evaluate_family_a_entry_v2,
    evaluate_family_a_exit_v2,
    parse_canonical_family_a_entry_decision_v2,
)
from signalbot.r4b_v2.strategy.family_a_features import (
    FamilyAEntryFeatureEvidenceV2,
    FamilyAExitFeatureEvidenceV2,
    FamilyAFeatureReadinessV2,
)

ATTEMPT = "attempt-1"
SYMBOL = "BTCUSDT"
PLAN = "a" * 64
BAR_OPEN = 2_000_160_000_000
BAR_CLOSE = BAR_OPEN + FIVE_MINUTE_MS_V2 - 1
D_MS = BAR_CLOSE + DECISION_DELAY_MS_V2
EPSILON = Decimal("0.0001")


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _trusted_entry_evidence(**overrides: object) -> FamilyAEntryFeatureEvidenceV2:
    """Unit-only truth-table fixture; raw factories are tested separately."""

    values: dict[str, object] = {
        "attempt_id": ATTEMPT,
        "symbol": SYMBOL,
        "venue": VenueV2.USDM_FUTURES,
        "promoting_plan_sha256": PLAN,
        "bar_open_ms": BAR_OPEN,
        "bar_close_ms": BAR_CLOSE,
        "decision_cutoff_ms": D_MS,
        "source_root_sha256": _sha("entry-source-root"),
        "latest_source_event_ms": BAR_CLOSE,
        "latest_source_receipt_ms": D_MS,
        "readiness": FamilyAFeatureReadinessV2.READY,
        "reasons": ("TRUSTED_TEST_FIXTURE",),
        "r12_previous": Decimal("0.02"),
        "rz_r12_previous": Decimal("1.5"),
        "rz_doi12_previous": Decimal("1.5"),
        "rz_basis_previous": Decimal("1.5"),
        "rz_funding_previous": Decimal("1.0"),
        "rz_r1_current": Decimal("-0.5"),
        "rz_doi1_current": Decimal("-1.0"),
        "flow_current": Decimal("-0.35"),
        "crowded_long_high": Decimal("110"),
        "crowded_short_low": Decimal("90"),
    }
    values.update(overrides)
    evidence = cast(
        FamilyAEntryFeatureEvidenceV2,
        object.__new__(FamilyAEntryFeatureEvidenceV2),
    )
    for name, value in values.items():
        object.__setattr__(evidence, name, value)
    object.__setattr__(
        evidence,
        "evidence_sha256",
        _sha(repr(sorted((name, repr(value)) for name, value in values.items()))),
    )
    return evidence


def _short_crowd_evidence(**overrides: object) -> FamilyAEntryFeatureEvidenceV2:
    values: dict[str, object] = {
        "r12_previous": Decimal("-0.02"),
        "rz_r12_previous": Decimal("-1.5"),
        "rz_basis_previous": Decimal("-1.5"),
        "rz_funding_previous": Decimal("-1.0"),
        "rz_r1_current": Decimal("0.5"),
        "flow_current": Decimal("0.35"),
    }
    values.update(overrides)
    return _trusted_entry_evidence(**values)


def _entry_input(evidence: FamilyAEntryFeatureEvidenceV2) -> FamilyAEntryInputV2:
    return FamilyAEntryInputV2(
        attempt_id=evidence.attempt_id,
        symbol=evidence.symbol,
        venue=evidence.venue,
        promoting_plan_sha256=evidence.promoting_plan_sha256,
        bar_open_ms=evidence.bar_open_ms,
        bar_close_ms=evidence.bar_close_ms,
        decision_cutoff_ms=evidence.decision_cutoff_ms,
        feature_evidence=evidence,
    )


def _paper_full_fill(
    signal: FamilyAEntryDecisionV2,
) -> tuple[
    PaperFokEntryDecisionV2,
    PaperFokFullFillCertificateV2,
    PaperFokDecisionRegistryV2,
]:
    source = _sha("paper-source")
    snapshot_schema = _sha("paper-snapshot-schema")
    depth_schema = _sha("paper-depth-schema")
    mark_schema = _sha("paper-mark-schema")
    exchange_schema = _sha("paper-exchange-schema")
    health_schema = _sha("paper-health-schema")
    target_venue = D_MS + 10_000
    target_local = D_MS + 100_000
    target_cursor = CausalTargetCursorV2(
        decision_cutoff_ms=D_MS,
        target_venue_ms=target_venue,
        prior_local_cursor_ms=target_local - 1,
        prior_venue_lower_bound_ms=target_venue - 1,
        target_local_cursor_ms=target_local,
        target_venue_lower_bound_ms=target_venue,
        clock_segment_root_sha256=_sha("paper-target-clock"),
        contiguous_cursor_evidence=True,
    )
    lineage = PaperFokLineageV2(
        promoting_plan_sha256=PLAN,
        source_root_sha256=source,
        depth_snapshot_schema_sha256=snapshot_schema,
        standard_depth_schema_sha256=depth_schema,
        mark_schema_sha256=mark_schema,
        exchange_info_schema_sha256=exchange_schema,
        health_schema_sha256=health_schema,
    )
    bids = (
        DepthLevelV2(price=Decimal("99.00"), quantity=Decimal("4.00")),
        DepthLevelV2(price=Decimal("98.90"), quantity=Decimal("4.00")),
    )
    asks = (
        DepthLevelV2(price=Decimal("100.00"), quantity=Decimal("4.00")),
        DepthLevelV2(price=Decimal("100.10"), quantity=Decimal("4.00")),
    )
    snapshot = FuturesDepthSnapshotV2(
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=source,
        schema_sha256=snapshot_schema,
        response_completion_ms=target_local - 1_000,
        last_update_id=100,
        depth_limit=2,
        bids=bids,
        asks=asks,
    )
    depth_event = FuturesStandardDepthEventV2(
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=source,
        schema_sha256=depth_schema,
        pair=SYMBOL,
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
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=source,
        schema_sha256=depth_schema,
        pair=SYMBOL,
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
        finalization_grace_binding_sha256=_sha("paper-finalization-grace"),
        finalized_through_local_ms=target_local,
        successor_candidates=(successor,),
    )
    mark = CausalMarkPriceEvidenceV2(
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_root_sha256=source,
        schema_sha256=mark_schema,
        pair=SYMBOL,
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
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
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
        attempt_id=ATTEMPT,
        signal_event_id=signal.event_id,
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        lineage=lineage,
        bar_open_ms=BAR_OPEN,
        bar_close_ms=BAR_CLOSE,
        decision_cutoff_ms=D_MS,
        target_cursor=target_cursor,
        target_state_last_ingest_seq=10,
        side=(PaperFokSideV2.SELL if signal.side is FamilyASideV2.SHORT else PaperFokSideV2.BUY),
        requested_quantity=Decimal("2.00"),
        snapshot=snapshot,
        pre_target_depth_events=(depth_event,),
        closure=closure,
        mark=mark,
        exchange_info=exchange_info,
    )
    paper_decision = evaluate_paper_fok_entry_v2(paper_input)
    registry = PaperFokDecisionRegistryV2(
        maximum_events=4,
        attempt_id=ATTEMPT,
        promoting_plan_sha256=PLAN,
    )
    registry.register(paper_decision)
    checkpoint = registry.terminal_checkpoint_v2()
    certificate = issue_paper_fok_full_fill_certificate_v2(
        paper_decision,
        registry=registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    return paper_decision, certificate, registry


def _trusted_position(side: FamilyASideV2 = FamilyASideV2.SHORT) -> FamilyAPositionV2:
    """Unit-only exit truth-table state; production construction is sealed."""

    position = cast(FamilyAPositionV2, object.__new__(FamilyAPositionV2))
    crowd = 1 if side is FamilyASideV2.SHORT else -1
    values: dict[str, object] = {
        "entry_event_id": _sha(f"entry-{side.value}"),
        "attempt_id": ATTEMPT,
        "symbol": SYMBOL,
        "venue": VenueV2.USDM_FUTURES,
        "promoting_plan_sha256": PLAN,
        "feature_evidence_sha256": _sha("entry-evidence"),
        "feature_source_root_sha256": _sha("entry-source"),
        "admission_evidence_sha256": _sha("admission"),
        "side": side,
        "crowd_sign": crowd,
        "signal_bar_open_ms": BAR_OPEN,
        "crowded_long_high": Decimal("110"),
        "crowded_short_low": Decimal("90"),
    }
    for name, value in values.items():
        object.__setattr__(position, name, value)
    return position


def _trusted_exit_evidence(
    *,
    horizon: int = 1,
    side: FamilyASideV2 = FamilyASideV2.SHORT,
    **overrides: object,
) -> FamilyAExitFeatureEvidenceV2:
    bar_open = BAR_OPEN + horizon * FIVE_MINUTE_MS_V2
    bar_close = bar_open + FIVE_MINUTE_MS_V2 - 1
    values: dict[str, object] = {
        "attempt_id": ATTEMPT,
        "symbol": SYMBOL,
        "venue": VenueV2.USDM_FUTURES,
        "promoting_plan_sha256": PLAN,
        "bar_open_ms": bar_open,
        "bar_close_ms": bar_close,
        "decision_cutoff_ms": bar_close + DECISION_DELAY_MS_V2,
        "source_root_sha256": _sha(f"exit-source-{horizon}"),
        "latest_source_event_ms": bar_close,
        "latest_source_receipt_ms": bar_close + DECISION_DELAY_MS_V2,
        "readiness": FamilyAFeatureReadinessV2.READY,
        "reasons": ("TRUSTED_TEST_FIXTURE",),
        "close_price": Decimal("100"),
        "rz_basis_current": (Decimal("0.1") if side is FamilyASideV2.SHORT else Decimal("-0.1")),
        "flow_previous": Decimal("0"),
        "flow_current": Decimal("0"),
    }
    values.update(overrides)
    evidence = cast(
        FamilyAExitFeatureEvidenceV2,
        object.__new__(FamilyAExitFeatureEvidenceV2),
    )
    for name, value in values.items():
        object.__setattr__(evidence, name, value)
    object.__setattr__(
        evidence,
        "evidence_sha256",
        _sha(repr(sorted((name, repr(value)) for name, value in values.items()))),
    )
    return evidence


def _exit_input(
    side: FamilyASideV2 = FamilyASideV2.SHORT,
    *,
    horizon: int = 1,
    **overrides: object,
) -> FamilyAExitInputV2:
    return FamilyAExitInputV2(
        position=_trusted_position(side),
        feature_evidence=_trusted_exit_evidence(
            horizon=horizon,
            side=side,
            **overrides,
        ),
    )


def _admitted_ledger_with_receipt(
    *,
    maximum_events: int = 20,
) -> tuple[
    FamilyAEpisodeLedgerV2,
    FamilyAEntryInputV2,
    FamilyAAdmissionReceiptV2,
    PaperFokDecisionRegistryV2,
]:
    ledger = FamilyAEpisodeLedgerV2(maximum_events=maximum_events)
    item = _entry_input(_trusted_entry_evidence())
    signal = ledger.evaluate_entry(item)
    paper_decision, certificate, paper_registry = _paper_full_fill(signal)
    receipt = ledger.admit_external_full_fill_with_receipt(
        item,
        signal,
        paper_decision,
        certificate,
        paper_registry,
    )
    return ledger, item, receipt, paper_registry


def _exit_input_for_admission(
    receipt: FamilyAAdmissionReceiptV2,
    *,
    horizon: int = 1,
    **overrides: object,
) -> FamilyAExitInputV2:
    return FamilyAExitInputV2(
        position=receipt.position,
        feature_evidence=_trusted_exit_evidence(
            horizon=horizon,
            side=receipt.position.side,
            **overrides,
        ),
    )


def test_all_entry_equalities_emit_exact_short_and_long_intents() -> None:
    short = evaluate_family_a_entry_v2(_entry_input(_trusted_entry_evidence()))
    long = evaluate_family_a_entry_v2(_entry_input(_short_crowd_evidence()))

    assert short.status is FamilyAEntryStatusV2.SIGNAL
    assert short.side is FamilyASideV2.SHORT
    assert short.crowd_sign == 1
    assert "INTENT_ONLY_PAPER_ADMISSION_REQUIRED" in short.reasons
    assert long.status is FamilyAEntryStatusV2.SIGNAL
    assert long.side is FamilyASideV2.LONG
    assert long.crowd_sign == -1
    assert short.event_id == long.event_id
    assert short.payload_sha256 != long.payload_sha256


@pytest.mark.parametrize(
    ("field_name", "failing_value", "reason"),
    [
        ("rz_r12_previous", Decimal("1.5") - EPSILON, "PRE_ABS_RZ_R12_LT_1_5"),
        ("rz_doi12_previous", Decimal("1.5") - EPSILON, "PRE_RZ_DOI12_LT_1_5"),
        ("rz_basis_previous", Decimal("1.5") - EPSILON, "PRE_ALIGNED_BASIS_LT_1_5"),
        ("rz_funding_previous", Decimal("1.0") - EPSILON, "PRE_ALIGNED_FUNDING_LT_1_0"),
        ("rz_r1_current", Decimal("-0.5") + EPSILON, "TRIGGER_REVERSAL_GT_NEG_0_5"),
        ("rz_doi1_current", Decimal("-1.0") + EPSILON, "TRIGGER_DOI1_GT_NEG_1_0"),
        ("flow_current", Decimal("-0.35") + EPSILON, "TRIGGER_FLOW_GT_NEG_0_35"),
    ],
)
def test_each_entry_boundary_one_quantum_beyond_fails(
    field_name: str,
    failing_value: Decimal,
    reason: str,
) -> None:
    decision = evaluate_family_a_entry_v2(
        _entry_input(_trusted_entry_evidence(**{field_name: failing_value}))
    )
    assert decision.status is FamilyAEntryStatusV2.NO_SIGNAL
    assert reason in decision.reasons


@given(extra_margin=st.integers(min_value=0, max_value=10_000))
def test_property_more_adverse_trigger_margin_cannot_remove_short_intent(
    extra_margin: int,
) -> None:
    evidence = _trusted_entry_evidence(
        rz_r1_current=Decimal("-0.5") - Decimal(extra_margin) / Decimal(10_000)
    )
    decision = evaluate_family_a_entry_v2(_entry_input(evidence))
    assert decision.status is FamilyAEntryStatusV2.SIGNAL
    assert decision.side is FamilyASideV2.SHORT


def test_strategy_arithmetic_ignores_hostile_ambient_decimal_context() -> None:
    item = _entry_input(
        _trusted_entry_evidence(
            rz_basis_previous=Decimal("1.500000000000000000000000000000001"),
            rz_funding_previous=Decimal("1.000000000000000000000000000000001"),
            flow_current=Decimal("-0.350000000000000000000000000000001"),
        )
    )
    with localcontext(Context(prec=6, rounding=ROUND_DOWN)):
        low_precision = evaluate_family_a_entry_v2(item)
    with localcontext(Context(prec=80, rounding=ROUND_UP)):
        high_precision = evaluate_family_a_entry_v2(item)
    assert low_precision == high_precision
    assert low_precision.payload_sha256 == high_precision.payload_sha256


def test_zero_c_and_missing_source_statuses_fail_closed() -> None:
    zero = evaluate_family_a_entry_v2(
        _entry_input(_trusted_entry_evidence(r12_previous=Decimal(0)))
    )
    unavailable = evaluate_family_a_entry_v2(
        _entry_input(
            _trusted_entry_evidence(
                readiness=FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA,
                reasons=("CURRENT_OI_UNAVAILABLE_10000MS",),
                r12_previous=None,
                rz_r12_previous=None,
                rz_doi12_previous=None,
                rz_basis_previous=None,
                rz_funding_previous=None,
                rz_r1_current=None,
                rz_doi1_current=None,
                flow_current=None,
                crowded_long_high=None,
                crowded_short_low=None,
            )
        )
    )
    assert zero.status is FamilyAEntryStatusV2.NO_SIGNAL
    assert unavailable.status is FamilyAEntryStatusV2.INCONCLUSIVE_DATA


def test_decisions_are_factory_sealed_and_registry_rejects_same_slot_conflict() -> None:
    item = _entry_input(_trusted_entry_evidence())
    first = evaluate_family_a_entry_v2(item)
    opposite = evaluate_family_a_entry_v2(_entry_input(_short_crowd_evidence()))
    registry = FamilyADecisionRegistryV2(maximum_events=2)

    assert registry.register(first) is FamilyARegistryDispositionV2.NEW
    assert registry.register(first) is FamilyARegistryDispositionV2.IDEMPOTENT_DUPLICATE
    with pytest.raises(FamilyAContractError, match="conflicting payload"):
        registry.register(opposite)

    with pytest.raises(FamilyAContractError, match="created by the evaluator"):
        FamilyAEntryDecisionV2(
            attempt_id=ATTEMPT,
            symbol=SYMBOL,
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN,
            bar_open_ms=BAR_OPEN,
            bar_close_ms=BAR_CLOSE,
            decision_cutoff_ms=D_MS,
            feature_evidence_sha256=_sha("fake"),
            feature_source_root_sha256=_sha("fake-source"),
            status=FamilyAEntryStatusV2.SIGNAL,
            side=FamilyASideV2.SHORT,
            reasons=("FORGED",),
            invalidation="FORGED",
            crowd_sign=1,
            crowded_long_high=Decimal("110"),
            crowded_short_low=Decimal("90"),
        )


def test_registry_replay_restore_has_identical_root_and_rejects_corruption() -> None:
    decision = evaluate_family_a_entry_v2(_entry_input(_trusted_entry_evidence()))
    registry = FamilyADecisionRegistryV2(maximum_events=2)
    registry.register(decision)
    replay = registry.export_replay_v2()
    restored = FamilyADecisionRegistryV2.restore_replay_v2(
        replay,
        maximum_events=2,
        expected_event_count=registry.event_count,
        expected_root_sha256=registry.root_sha256,
    )
    assert restored.root_sha256 == registry.root_sha256
    assert restored.export_replay_v2() == replay

    outer = json.loads(replay)
    inner = json.loads(outer["records"][0]["canonical_payload"])
    inner["status"] = FamilyAEntryStatusV2.NO_SIGNAL.value
    outer["records"][0]["canonical_payload"] = canonical_json_line(inner).decode()
    corrupt = canonical_json_line(outer)
    with pytest.raises(
        FamilyAContractError,
        match=r"payload hash|invalid|non-signal decision",
    ):
        FamilyADecisionRegistryV2.restore_replay_v2(
            corrupt,
            maximum_events=2,
            expected_event_count=registry.event_count,
            expected_root_sha256=registry.root_sha256,
        )


def test_episode_ledger_is_idempotent_and_conflicting_input_fails() -> None:
    ledger = FamilyAEpisodeLedgerV2(maximum_events=2)
    item = _entry_input(_trusted_entry_evidence())
    first = ledger.evaluate_entry(item)
    assert ledger.evaluate_entry(item) == first
    root = ledger.root_sha256
    assert len(root) == 64

    with pytest.raises(FamilyAContractError, match="conflicting causal input"):
        ledger.evaluate_entry(_entry_input(_short_crowd_evidence()))


def test_entry_preview_commit_is_non_mutating_exact_and_idempotent() -> None:
    ledger = FamilyAEpisodeLedgerV2(maximum_events=2)
    item = _entry_input(_trusted_entry_evidence())
    root = ledger.root_sha256
    preview = ledger.preview_entry(item)

    assert ledger.maximum_events == 2
    assert preview.pre_root_sha256 == root
    assert preview.pre_event_count == 0
    assert not preview.already_committed
    assert ledger.root_sha256 == root
    assert ledger.event_count == 0
    receipt = ledger.commit_entry_preview_with_receipt(item, preview)
    assert receipt.decision == preview.decision
    assert receipt.input_sha256 == preview.input_sha256
    assert receipt.event_id == preview.decision.event_id
    assert receipt.pre_root_sha256 == preview.pre_root_sha256
    assert receipt.pre_event_count == preview.pre_event_count
    assert receipt.disposition is FamilyAEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION
    committed_root = ledger.root_sha256
    assert receipt.post_root_sha256 == committed_root
    assert receipt.post_event_count == 1
    assert committed_root != root
    assert ledger.event_count == 1
    assert ledger.commit_entry_preview(item, preview) == preview.decision
    assert ledger.root_sha256 == committed_root

    replay = ledger.preview_entry(item)
    assert replay.already_committed
    assert replay.pre_root_sha256 == committed_root
    assert replay.pre_event_count == 1
    replay_receipt = ledger.commit_entry_preview_with_receipt(item, replay)
    assert replay_receipt.decision == preview.decision
    assert replay_receipt.disposition is FamilyAEntryCommitDispositionV2.PREEXISTING
    assert replay_receipt.pre_root_sha256 == replay_receipt.post_root_sha256
    assert replay_receipt.pre_event_count == replay_receipt.post_event_count == 1
    with pytest.raises(FamilyAContractError, match="pre-existing"):
        ledger.rollback_entry_preview(item, replay, replay_receipt)

    with pytest.raises(FamilyAContractError, match="created by the ledger"):
        FamilyAEntryPreviewV2(
            input_sha256=preview.input_sha256,
            pre_root_sha256=preview.pre_root_sha256,
            pre_event_count=preview.pre_event_count,
            decision=preview.decision,
            already_committed=preview.already_committed,
        )
    with pytest.raises(FamilyAContractError, match="created by the ledger"):
        FamilyAEntryCommitReceiptV2(
            input_sha256=receipt.input_sha256,
            event_id=receipt.event_id,
            decision=receipt.decision,
            preview_already_committed=receipt.preview_already_committed,
            pre_root_sha256=receipt.pre_root_sha256,
            pre_event_count=receipt.pre_event_count,
            post_root_sha256=receipt.post_root_sha256,
            post_event_count=receipt.post_event_count,
            disposition=receipt.disposition,
            _owner_token=object(),
            _rollback_capability=object(),
        )


def test_entry_decision_public_parser_requires_exact_canonical_jsonl() -> None:
    decision = evaluate_family_a_entry_v2(_entry_input(_trusted_entry_evidence()))
    payload = canonical_family_a_entry_decision_v2(decision)
    assert parse_canonical_family_a_entry_decision_v2(payload) == decision
    with pytest.raises(FamilyAContractError, match="canonical JSONL"):
        parse_canonical_family_a_entry_decision_v2(payload + b"\n")


def test_entry_preview_rejects_input_conflict_capacity_and_state_drift() -> None:
    item = _entry_input(_trusted_entry_evidence())
    conflict = _entry_input(_short_crowd_evidence())
    conflict_ledger = FamilyAEpisodeLedgerV2(maximum_events=2)
    preview = conflict_ledger.preview_entry(item)
    conflict_ledger.evaluate_entry(conflict)
    with pytest.raises(FamilyAContractError, match="conflicts with committed input"):
        conflict_ledger.commit_entry_preview(item, preview)

    drift_ledger = FamilyAEpisodeLedgerV2(maximum_events=2)
    drift_preview = drift_ledger.preview_entry(item)
    other = _entry_input(
        _trusted_entry_evidence(
            attempt_id="attempt-state-drift",
            source_root_sha256=_sha("state-drift-source"),
        )
    )
    drift_ledger.evaluate_entry(other)
    with pytest.raises(FamilyAContractError, match="state drifted"):
        drift_ledger.commit_entry_preview(item, drift_preview)
    with pytest.raises(FamilyAContractError, match="differs from exact input"):
        FamilyAEpisodeLedgerV2(maximum_events=2).commit_entry_preview(conflict, preview)

    capacity_ledger = FamilyAEpisodeLedgerV2(maximum_events=1)
    capacity_ledger.evaluate_entry(item)
    assert capacity_ledger.preview_entry(item).already_committed
    with pytest.raises(FamilyAContractError, match="ledger exhausted"):
        capacity_ledger.preview_entry(other)


def test_entry_preview_rollback_restores_only_untouched_pre_state() -> None:
    ledger = FamilyAEpisodeLedgerV2(maximum_events=4)
    item = _entry_input(_trusted_entry_evidence())
    preview = ledger.preview_entry(item)
    receipt = ledger.commit_entry_preview_with_receipt(item, preview)
    assert ledger.rollback_entry_preview(item, preview, receipt)
    assert ledger.root_sha256 == preview.pre_root_sha256
    assert ledger.event_count == preview.pre_event_count
    with pytest.raises(FamilyAContractError, match="does not own"):
        ledger.rollback_entry_preview(item, preview, receipt)

    recommit = ledger.commit_entry_preview_with_receipt(item, preview)
    with pytest.raises(FamilyAContractError, match="does not own"):
        ledger.rollback_entry_preview(item, preview, receipt)
    assert ledger.event_count == 1
    assert ledger.rollback_entry_preview(item, preview, recommit)

    admitted = FamilyAEpisodeLedgerV2(maximum_events=4)
    admitted_preview = admitted.preview_entry(item)
    admitted_receipt = admitted.commit_entry_preview_with_receipt(item, admitted_preview)
    signal = admitted_receipt.decision
    paper_decision, certificate, paper_registry = _paper_full_fill(signal)
    admitted.admit_external_full_fill(
        item,
        signal,
        paper_decision,
        certificate,
        paper_registry,
    )
    admitted_root = admitted.root_sha256
    with pytest.raises(FamilyAContractError, match="state drifted"):
        admitted.rollback_entry_preview(item, admitted_preview, admitted_receipt)
    assert admitted.root_sha256 == admitted_root
    assert admitted.position_for_entry(signal.event_id).entry_event_id == signal.event_id


def test_identical_concurrent_entry_commits_issue_only_one_rollback_capability() -> None:
    ledger = FamilyAEpisodeLedgerV2(maximum_events=2)
    item = _entry_input(_trusted_entry_evidence())
    preview = ledger.preview_entry(item)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = tuple(
            pool.submit(ledger.commit_entry_preview_with_receipt, item, preview) for _ in range(2)
        )
        receipts = tuple(future.result() for future in futures)

    by_disposition = {receipt.disposition: receipt for receipt in receipts}
    assert set(by_disposition) == {
        FamilyAEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION,
        FamilyAEntryCommitDispositionV2.PREEXISTING,
    }
    preexisting = by_disposition[FamilyAEntryCommitDispositionV2.PREEXISTING]
    with pytest.raises(FamilyAContractError, match="pre-existing"):
        ledger.rollback_entry_preview(item, preview, preexisting)
    assert ledger.event_count == 1

    foreign = FamilyAEpisodeLedgerV2(maximum_events=2)
    foreign_preview = foreign.preview_entry(item)
    foreign_receipt = foreign.commit_entry_preview_with_receipt(item, foreign_preview)
    with pytest.raises(FamilyAContractError, match="another ledger"):
        ledger.rollback_entry_preview(item, preview, foreign_receipt)
    assert ledger.event_count == 1

    created = by_disposition[FamilyAEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION]
    assert ledger.rollback_entry_preview(item, preview, created)


def test_prospective_authority_gates_every_family_a_mutation_surface() -> None:
    ledger = FamilyAEpisodeLedgerV2(maximum_events=4)
    item = _entry_input(_trusted_entry_evidence())
    preview = ledger.preview_entry(item)
    authority = ledger._claim_prospective_decision_authority_v2()  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(FamilyAContractError, match="held prospective decision authority"):
        ledger.evaluate_entry(item)
    with pytest.raises(FamilyAContractError, match="held prospective decision authority"):
        ledger.commit_entry_preview(item, preview)
    with pytest.raises(FamilyAContractError, match="held prospective decision authority"):
        ledger.admit_external_full_fill(
            item,
            preview.decision,
            cast(PaperFokEntryDecisionV2, object()),
            cast(PaperFokFullFillCertificateV2, object()),
            cast(PaperFokDecisionRegistryV2, object()),
        )
    with pytest.raises(FamilyAContractError, match="held prospective decision authority"):
        ledger.evaluate_exit(cast(FamilyAExitInputV2, object()))

    receipt = ledger.commit_entry_preview_with_receipt(
        item,
        preview,
        _prospective_authority=authority,
    )
    with pytest.raises(FamilyAContractError, match="cannot release a non-genesis"):
        ledger._release_unconsumed_prospective_decision_authority_v2(  # pyright: ignore[reportPrivateUsage]
            authority
        )
    assert ledger.rollback_entry_preview(
        item,
        preview,
        receipt,
        _prospective_authority=authority,
    )
    ledger._release_unconsumed_prospective_decision_authority_v2(  # pyright: ignore[reportPrivateUsage]
        authority
    )
    assert ledger.evaluate_entry(item) == preview.decision


def test_family_a_prospective_authority_rejects_non_genesis_hidden_state() -> None:
    ledger = FamilyAEpisodeLedgerV2(maximum_events=4)
    ledger._active_by_key[(PLAN, VenueV2.USDM_FUTURES, SYMBOL)] = _sha(  # pyright: ignore[reportPrivateUsage]
        "unrooted-active-entry"
    )

    assert ledger.event_count == 0
    with pytest.raises(FamilyAContractError, match="requires exact genesis state"):
        ledger._claim_prospective_decision_authority_v2()  # pyright: ignore[reportPrivateUsage]


def test_entry_decisions_are_independent_of_unrelated_symbol_order() -> None:
    btc_item = _entry_input(_trusted_entry_evidence())
    eth_item = _entry_input(
        _trusted_entry_evidence(
            symbol="ETHUSDT",
            source_root_sha256=_sha("eth-entry-source"),
        )
    )
    forward = FamilyAEpisodeLedgerV2(maximum_events=4)
    reverse = FamilyAEpisodeLedgerV2(maximum_events=4)

    forward_decisions = {
        btc_item.symbol: forward.evaluate_entry(btc_item),
        eth_item.symbol: forward.evaluate_entry(eth_item),
    }
    reverse_decisions = {
        eth_item.symbol: reverse.evaluate_entry(eth_item),
        btc_item.symbol: reverse.evaluate_entry(btc_item),
    }

    assert reverse_decisions == forward_decisions
    assert reverse.root_sha256 == forward.root_sha256
    assert forward_decisions[SYMBOL] == evaluate_family_a_entry_v2(btc_item)


def test_nominal_fake_admission_cannot_create_a_position() -> None:
    ledger = FamilyAEpisodeLedgerV2(maximum_events=2)
    item = _entry_input(_trusted_entry_evidence())
    decision = ledger.evaluate_entry(item)
    with pytest.raises(FamilyAContractError, match="concrete PAPER FOK"):
        ledger.admit_external_full_fill(
            item,
            decision,
            cast(PaperFokEntryDecisionV2, object()),
            cast(PaperFokFullFillCertificateV2, object()),
            cast(PaperFokDecisionRegistryV2, object()),
        )
    assert not ledger.is_active(
        promoting_plan_sha256=PLAN,
        venue=VenueV2.USDM_FUTURES,
        symbol=SYMBOL,
    )
    with pytest.raises(FamilyAContractError, match="ledgered external PAPER"):
        FamilyAPositionV2(
            entry_event_id=decision.event_id,
            attempt_id=ATTEMPT,
            symbol=SYMBOL,
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN,
            feature_evidence_sha256=item.feature_evidence.evidence_sha256,
            feature_source_root_sha256=item.feature_evidence.source_root_sha256,
            admission_evidence_sha256=_sha("fake"),
            paper_decision_event_id=_sha("paper-event"),
            paper_decision_payload_sha256=_sha("paper-payload"),
            paper_registry_root_sha256=_sha("paper-registry"),
            paper_registry_event_count=1,
            paper_registry_checkpoint_sha256=_sha("paper-registry-checkpoint"),
            paper_requested_quantity=Decimal("1"),
            paper_filled_quantity=Decimal("1"),
            paper_executable_vwap=Decimal("100"),
            side=FamilyASideV2.SHORT,
            crowd_sign=1,
            signal_bar_open_ms=BAR_OPEN,
            crowded_long_high=Decimal("110"),
            crowded_short_low=Decimal("90"),
        )


def test_only_registry_checkpointed_concrete_full_fill_creates_position() -> None:
    ledger = FamilyAEpisodeLedgerV2(maximum_events=20)
    item = _entry_input(_trusted_entry_evidence())
    signal = ledger.evaluate_entry(item)
    paper_decision, certificate, paper_registry = _paper_full_fill(signal)

    position = ledger.admit_external_full_fill(
        item,
        signal,
        paper_decision,
        certificate,
        paper_registry,
    )

    assert position.paper_requested_quantity == Decimal("2.00")
    assert position.paper_filled_quantity == Decimal("2.00")
    assert position.admission_evidence_sha256 == certificate.certificate_sha256
    assert position.paper_registry_root_sha256 == paper_registry.replay_root_sha256
    assert (
        position.paper_registry_checkpoint_sha256
        == paper_registry.terminal_checkpoint_v2().checkpoint_sha256
    )
    assert (
        ledger.admit_external_full_fill(
            item,
            signal,
            paper_decision,
            certificate,
            paper_registry,
        )
        == position
    )
    assert ledger.is_active(
        promoting_plan_sha256=PLAN,
        venue=VenueV2.USDM_FUTURES,
        symbol=SYMBOL,
    )


def test_episode_restart_preserves_h1_h12_sticky_state_and_terminal_root() -> None:
    ledger = FamilyAEpisodeLedgerV2(maximum_events=20)
    item = _entry_input(_trusted_entry_evidence())
    signal = ledger.evaluate_entry(item)
    paper_decision, certificate, paper_registry = _paper_full_fill(signal)
    position = ledger.admit_external_full_fill(
        item,
        signal,
        paper_decision,
        certificate,
        paper_registry,
    )
    h1 = FamilyAExitInputV2(
        position=position,
        feature_evidence=_trusted_exit_evidence(
            horizon=1,
            readiness=FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW,
            reasons=("NORMAL_FLOW_CAPTURE_INCOMPLETE",),
            flow_previous=None,
            flow_current=None,
        ),
    )
    first_exit = ledger.evaluate_exit(h1)
    assert first_exit.action is FamilyAExitActionV2.HOLD
    assert first_exit.interval_status is FamilyAIntervalStatusV2.INCONCLUSIVE_DATA

    checkpoint = ledger.export_state_v2()
    restored = FamilyAEpisodeLedgerV2.restore_state_v2(
        checkpoint,
        maximum_events=20,
        expected_event_count=ledger.event_count,
        expected_root_sha256=ledger.root_sha256,
    )
    assert restored.export_state_v2() == checkpoint

    original_decision: FamilyAExitDecisionV2 | None = None
    for horizon in range(2, FAMILY_A_HARD_HORIZON_BARS_V2 + 1):
        original_input = FamilyAExitInputV2(
            position=ledger.position_for_entry(signal.event_id),
            feature_evidence=_trusted_exit_evidence(horizon=horizon),
        )
        restored_input = FamilyAExitInputV2(
            position=restored.position_for_entry(signal.event_id),
            feature_evidence=_trusted_exit_evidence(horizon=horizon),
        )
        original_decision = ledger.evaluate_exit(original_input)
        restored_decision = restored.evaluate_exit(restored_input)
        assert restored_decision == original_decision
        assert restored_decision.interval_status is (FamilyAIntervalStatusV2.INCONCLUSIVE_DATA)
        assert restored.root_sha256 == ledger.root_sha256

    assert original_decision is not None
    assert original_decision.reason is FamilyAExitReasonV2.HARD_HORIZON
    assert not ledger.is_active(
        promoting_plan_sha256=PLAN,
        venue=VenueV2.USDM_FUTURES,
        symbol=SYMBOL,
    )
    terminal_checkpoint = ledger.export_state_v2()
    terminal_restored = FamilyAEpisodeLedgerV2.restore_state_v2(
        terminal_checkpoint,
        maximum_events=20,
        expected_event_count=ledger.event_count,
        expected_root_sha256=ledger.root_sha256,
    )
    assert terminal_restored.root_sha256 == ledger.root_sha256
    with pytest.raises(FamilyAContractError, match="event count"):
        FamilyAEpisodeLedgerV2.restore_state_v2(
            terminal_checkpoint,
            maximum_events=20,
            expected_event_count=ledger.event_count - 1,
            expected_root_sha256=ledger.root_sha256,
        )


@pytest.mark.parametrize("side", [FamilyASideV2.SHORT, FamilyASideV2.LONG])
def test_adverse_exit_is_strict_and_equality_does_not_trigger(
    side: FamilyASideV2,
) -> None:
    boundary = Decimal("110") if side is FamilyASideV2.SHORT else Decimal("90")
    equality = evaluate_family_a_exit_v2(_exit_input(side, close_price=boundary))
    breached = evaluate_family_a_exit_v2(
        _exit_input(
            side,
            close_price=(boundary + EPSILON if side is FamilyASideV2.SHORT else boundary - EPSILON),
        )
    )
    assert equality.reason is FamilyAExitReasonV2.HOLD
    assert breached.reason is FamilyAExitReasonV2.ADVERSE_INVALIDATION


@pytest.mark.parametrize("side", [FamilyASideV2.SHORT, FamilyASideV2.LONG])
def test_normalization_zero_and_two_bar_flow_equalities_trigger(
    side: FamilyASideV2,
) -> None:
    normalized = evaluate_family_a_exit_v2(_exit_input(side, rz_basis_current=Decimal(0)))
    crowd = Decimal(1) if side is FamilyASideV2.SHORT else Decimal(-1)
    flow = evaluate_family_a_exit_v2(
        _exit_input(
            side,
            flow_previous=crowd * Decimal("0.20"),
            flow_current=crowd * Decimal("0.20"),
        )
    )
    assert normalized.reason is FamilyAExitReasonV2.BASIS_NORMALIZATION
    assert flow.reason is FamilyAExitReasonV2.TWO_BAR_FLOW_REVERSAL


def test_incomplete_flow_is_sticky_eligible_hold_not_a_complete_interval() -> None:
    decision = evaluate_family_a_exit_v2(
        _exit_input(
            readiness=FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW,
            reasons=("NORMAL_FLOW_CAPTURE_INCOMPLETE",),
            flow_previous=None,
            flow_current=None,
        )
    )
    assert decision.action is FamilyAExitActionV2.HOLD
    assert decision.interval_status is FamilyAIntervalStatusV2.INCONCLUSIVE_DATA
    assert decision.reasons == ("INCOMPLETE_FLOW_CONDITION_NOT_EVALUATED",)


def test_hard_horizon_is_exact_t_plus_twelve_and_adverse_has_priority() -> None:
    hard = evaluate_family_a_exit_v2(_exit_input(horizon=FAMILY_A_HARD_HORIZON_BARS_V2))
    adverse = evaluate_family_a_exit_v2(
        _exit_input(
            horizon=FAMILY_A_HARD_HORIZON_BARS_V2,
            close_price=Decimal("111"),
            rz_basis_current=Decimal(0),
        )
    )
    assert hard.reason is FamilyAExitReasonV2.HARD_HORIZON
    assert adverse.reason is FamilyAExitReasonV2.ADVERSE_INVALIDATION


def test_exit_decision_direct_constructor_is_rejected() -> None:
    with pytest.raises(FamilyAContractError, match="created by the evaluator"):
        FamilyAExitDecisionV2(
            entry_event_id=_sha("entry"),
            attempt_id=ATTEMPT,
            symbol=SYMBOL,
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN,
            bar_open_ms=BAR_OPEN + FIVE_MINUTE_MS_V2,
            bar_close_ms=BAR_CLOSE + FIVE_MINUTE_MS_V2,
            decision_cutoff_ms=D_MS + FIVE_MINUTE_MS_V2,
            feature_evidence_sha256=_sha("feature"),
            feature_source_root_sha256=_sha("source"),
            side=FamilyASideV2.SHORT,
            action=FamilyAExitActionV2.EXIT_SHORT,
            reason=FamilyAExitReasonV2.HARD_HORIZON,
            reasons=("FORGED",),
            invalidation="FORGED",
            interval_status=FamilyAIntervalStatusV2.COMPLETE,
        )


def test_canonical_decision_contains_source_and_payload_roots() -> None:
    decision = evaluate_family_a_entry_v2(_entry_input(_trusted_entry_evidence()))
    document = json.loads(canonical_family_a_entry_decision_v2(decision))
    assert document["event_id"] == decision.event_id
    assert document["payload_sha256"] == decision.payload_sha256
    assert document["feature_source_root_sha256"] == decision.feature_source_root_sha256


def test_admission_receipt_binds_exact_paper_checkpoint_and_rolls_back_once() -> None:
    ledger, item, receipt, paper_registry = _admitted_ledger_with_receipt()

    assert receipt.disposition is FamilyAAdmissionDispositionV2.NEW_BY_THIS_TRANSACTION
    assert receipt.entry_event_id == receipt.entry_decision.event_id
    assert receipt.position.entry_event_id == receipt.entry_event_id
    assert receipt.paper_decision_event_id == receipt.paper_decision.event_id
    assert receipt.certificate_sha256 == receipt.certificate.certificate_sha256
    assert receipt.paper_registry_root_sha256 == paper_registry.replay_root_sha256
    assert receipt.paper_registry_event_count == paper_registry.event_count
    assert receipt.paper_registry_maximum_events == paper_registry.maximum_events
    assert (
        receipt.paper_registry_checkpoint_sha256
        == paper_registry.terminal_checkpoint_v2().checkpoint_sha256
    )
    assert receipt.post_event_count == receipt.pre_event_count + 1
    assert receipt.post_root_sha256 == ledger.root_sha256

    replay = ledger.admit_external_full_fill_with_receipt(
        item,
        receipt.entry_decision,
        receipt.paper_decision,
        receipt.certificate,
        paper_registry,
    )
    assert replay.position == receipt.position
    assert replay.disposition is FamilyAAdmissionDispositionV2.PREEXISTING
    assert replay.pre_root_sha256 == replay.post_root_sha256 == ledger.root_sha256
    assert replay.pre_event_count == replay.post_event_count == ledger.event_count
    with pytest.raises(FamilyAContractError, match="pre-existing"):
        ledger.rollback_external_full_fill_admission(
            item,
            receipt.entry_decision,
            replay,
        )

    assert ledger.rollback_external_full_fill_admission(
        item,
        receipt.entry_decision,
        receipt,
    )
    assert ledger.root_sha256 == receipt.pre_root_sha256
    assert ledger.event_count == receipt.pre_event_count
    assert not ledger.is_active(
        promoting_plan_sha256=PLAN,
        venue=VenueV2.USDM_FUTURES,
        symbol=SYMBOL,
    )
    with pytest.raises(FamilyAContractError, match="does not own"):
        ledger.rollback_external_full_fill_admission(
            item,
            receipt.entry_decision,
            receipt,
        )
    with pytest.raises(FamilyAContractError, match="created by the episode ledger"):
        replace(receipt)


def test_admission_rollback_rejects_foreign_tampered_and_drifted_receipts() -> None:
    left, left_item, left_receipt, _ = _admitted_ledger_with_receipt()
    _, _, foreign_receipt, _ = _admitted_ledger_with_receipt()
    with pytest.raises(FamilyAContractError, match="another ledger"):
        left.rollback_external_full_fill_admission(
            left_item,
            left_receipt.entry_decision,
            foreign_receipt,
        )

    object.__setattr__(left_receipt, "input_sha256", "0" * 64)
    with pytest.raises(FamilyAContractError, match="input hash is noncanonical"):
        left.rollback_external_full_fill_admission(
            left_item,
            left_receipt.entry_decision,
            left_receipt,
        )
    assert left.is_active(
        promoting_plan_sha256=PLAN,
        venue=VenueV2.USDM_FUTURES,
        symbol=SYMBOL,
    )

    drifted, drifted_item, drifted_receipt, _ = _admitted_ledger_with_receipt()
    other = _entry_input(
        _trusted_entry_evidence(
            attempt_id="admission-foreign-state-drift",
            source_root_sha256=_sha("admission-foreign-state-drift"),
        )
    )
    drifted.evaluate_entry(other)
    with pytest.raises(FamilyAContractError, match="state drifted"):
        drifted.rollback_external_full_fill_admission(
            drifted_item,
            drifted_receipt.entry_decision,
            drifted_receipt,
        )


def test_exit_receipt_hold_is_idempotent_and_exactly_rollbackable() -> None:
    ledger, item, admission, _ = _admitted_ledger_with_receipt()
    exit_input = _exit_input_for_admission(admission)
    receipt = ledger.evaluate_exit_with_receipt(exit_input)

    assert type(receipt) is FamilyAExitMutationReceiptV2
    assert receipt.disposition is FamilyAExitDispositionV2.NEW_BY_THIS_TRANSACTION
    assert receipt.decision.action is FamilyAExitActionV2.HOLD
    assert receipt.entry_event_id == admission.entry_event_id
    assert receipt.exit_event_id == receipt.decision.event_id
    assert receipt.pre_next_horizon == 1
    assert receipt.post_next_horizon == 2
    assert receipt.pre_active and receipt.post_active
    assert not receipt.pre_terminal and not receipt.post_terminal
    assert receipt.post_event_count == receipt.pre_event_count + 1
    replay = ledger.evaluate_exit_with_receipt(exit_input)
    assert replay.decision == receipt.decision
    assert replay.disposition is FamilyAExitDispositionV2.PREEXISTING
    assert replay.pre_root_sha256 == replay.post_root_sha256 == ledger.root_sha256
    with pytest.raises(FamilyAContractError, match="pre-existing"):
        ledger.rollback_exit(exit_input, replay)

    assert ledger.rollback_exit(exit_input, receipt)
    assert ledger.root_sha256 == receipt.pre_root_sha256
    assert ledger.event_count == receipt.pre_event_count
    assert ledger.is_active(
        promoting_plan_sha256=PLAN,
        venue=VenueV2.USDM_FUTURES,
        symbol=SYMBOL,
    )
    with pytest.raises(FamilyAContractError, match="does not own"):
        ledger.rollback_exit(exit_input, receipt)
    assert ledger.rollback_external_full_fill_admission(
        item,
        admission.entry_decision,
        admission,
    )
    with pytest.raises(FamilyAContractError, match="created by the episode ledger"):
        replace(receipt)


def test_terminal_exit_rollback_and_lifo_state_drift_are_exact() -> None:
    ledger, _, admission, _ = _admitted_ledger_with_receipt()
    terminal_input = _exit_input_for_admission(
        admission,
        close_price=Decimal("111"),
    )
    terminal = ledger.evaluate_exit_with_receipt(terminal_input)
    assert terminal.decision.exits_position
    assert terminal.post_terminal
    assert not terminal.post_active
    assert ledger.rollback_exit(terminal_input, terminal)
    assert ledger.is_active(
        promoting_plan_sha256=PLAN,
        venue=VenueV2.USDM_FUTURES,
        symbol=SYMBOL,
    )

    h1_input = _exit_input_for_admission(
        admission,
        readiness=FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW,
        reasons=("NORMAL_FLOW_CAPTURE_INCOMPLETE",),
        flow_previous=None,
        flow_current=None,
    )
    h1 = ledger.evaluate_exit_with_receipt(h1_input)
    assert not h1.pre_sticky_inconclusive
    assert h1.post_sticky_inconclusive
    h2_input = _exit_input_for_admission(admission, horizon=2)
    h2 = ledger.evaluate_exit_with_receipt(h2_input)
    with pytest.raises(FamilyAContractError, match="state drifted"):
        ledger.rollback_exit(h1_input, h1)
    assert ledger.rollback_exit(h2_input, h2)
    assert ledger.rollback_exit(h1_input, h1)
    assert ledger.root_sha256 == h1.pre_root_sha256


def test_exit_rollback_rejects_foreign_and_tampered_receipts() -> None:
    left, _, left_admission, _ = _admitted_ledger_with_receipt()
    right, _, right_admission, _ = _admitted_ledger_with_receipt()
    left_input = _exit_input_for_admission(left_admission)
    right_input = _exit_input_for_admission(right_admission)
    left_receipt = left.evaluate_exit_with_receipt(left_input)
    right_receipt = right.evaluate_exit_with_receipt(right_input)

    with pytest.raises(FamilyAContractError, match="another ledger"):
        left.rollback_exit(left_input, right_receipt)
    object.__setattr__(left_receipt, "post_event_count", 999)
    with pytest.raises(FamilyAContractError, match="invalid pre/post state"):
        left.rollback_exit(left_input, left_receipt)
    assert left.event_count == 3


def test_lifecycle_receipts_respect_capacity_and_prospective_authority() -> None:
    full, _, full_admission, _ = _admitted_ledger_with_receipt(maximum_events=2)
    full_root = full.root_sha256
    with pytest.raises(FamilyAContractError, match="ledger exhausted"):
        full.evaluate_exit_with_receipt(_exit_input_for_admission(full_admission))
    assert full.event_count == 2
    assert full.root_sha256 == full_root

    ledger = FamilyAEpisodeLedgerV2(maximum_events=4)
    item = _entry_input(_trusted_entry_evidence())
    preview = ledger.preview_entry(item)
    authority = ledger._claim_prospective_decision_authority_v2()  # pyright: ignore[reportPrivateUsage]
    entry_receipt = ledger.commit_entry_preview_with_receipt(
        item,
        preview,
        _prospective_authority=authority,
    )
    paper_decision, certificate, paper_registry = _paper_full_fill(entry_receipt.decision)
    with pytest.raises(FamilyAContractError, match="held prospective decision authority"):
        ledger.admit_external_full_fill_with_receipt(
            item,
            entry_receipt.decision,
            paper_decision,
            certificate,
            paper_registry,
        )
    admission = ledger.admit_external_full_fill_with_receipt(
        item,
        entry_receipt.decision,
        paper_decision,
        certificate,
        paper_registry,
        _prospective_authority=authority,
    )
    exit_input = _exit_input_for_admission(admission)
    with pytest.raises(FamilyAContractError, match="held prospective decision authority"):
        ledger.evaluate_exit_with_receipt(exit_input)
    exit_receipt = ledger.evaluate_exit_with_receipt(
        exit_input,
        _prospective_authority=authority,
    )
    with pytest.raises(FamilyAContractError, match="held prospective decision authority"):
        ledger.rollback_exit(exit_input, exit_receipt)
    assert ledger.rollback_exit(
        exit_input,
        exit_receipt,
        _prospective_authority=authority,
    )
    with pytest.raises(FamilyAContractError, match="held prospective decision authority"):
        ledger.rollback_external_full_fill_admission(
            item,
            entry_receipt.decision,
            admission,
        )
    assert ledger.rollback_external_full_fill_admission(
        item,
        entry_receipt.decision,
        admission,
        _prospective_authority=authority,
    )
    assert ledger.rollback_entry_preview(
        item,
        preview,
        entry_receipt,
        _prospective_authority=authority,
    )
    ledger._release_unconsumed_prospective_decision_authority_v2(  # pyright: ignore[reportPrivateUsage]
        authority
    )
