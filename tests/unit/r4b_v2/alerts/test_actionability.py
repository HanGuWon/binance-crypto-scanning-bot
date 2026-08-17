from __future__ import annotations

from dataclasses import replace

import pytest

from signalbot.r4b_v2.alerts.actionability import (
    AlertActionabilityContractErrorV2,
    AlertActionabilityGateV2,
    AlertActionabilityInputV2,
    AlertActionabilityRecordV2,
    AlertActionabilityRegistryDispositionV2,
    AlertActionabilityRegistryV2,
    AlertActionabilityStatusV2,
    AlertTransportTimesV2,
    CausalTargetCursorV2,
    ExpectedPromotingAlertV2,
    PromotingFamilyV2,
    PromotingSignalCensusV2,
    canonical_alert_actionability_record_v2,
    evaluate_alert_actionability_v2,
    summarize_alert_actionability_v2,
)
from signalbot.r4b_v2.capture.models import VenueV2

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64
_SHA_D = "d" * 64
_D = 40_000
_TARGET = _D + 10_000


def _cursor(
    *,
    target_local_cursor_ms: int = _TARGET,
    target_venue_ms: int = _TARGET,
    prior_venue_lower_bound_ms: int = _TARGET - 1,
    target_venue_lower_bound_ms: int = _TARGET,
) -> CausalTargetCursorV2:
    return CausalTargetCursorV2(
        decision_cutoff_ms=_D,
        target_venue_ms=target_venue_ms,
        prior_local_cursor_ms=target_local_cursor_ms - 1,
        prior_venue_lower_bound_ms=prior_venue_lower_bound_ms,
        target_local_cursor_ms=target_local_cursor_ms,
        target_venue_lower_bound_ms=target_venue_lower_bound_ms,
        clock_segment_root_sha256=_SHA_C,
        contiguous_cursor_evidence=True,
    )


def _transport(
    *,
    accepted_ms: int | None,
    outbox_ms: int = _D,
) -> AlertTransportTimesV2:
    if accepted_ms is None:
        return AlertTransportTimesV2(
            durable_outbox_enqueue_ms=outbox_ms,
            send_start_ms=outbox_ms + 1,
            response_first_byte_ms=None,
            provider_acceptance_completion_ms=None,
            request_completion_ms=outbox_ms + 10,
        )
    return AlertTransportTimesV2(
        durable_outbox_enqueue_ms=outbox_ms,
        send_start_ms=outbox_ms + 1,
        response_first_byte_ms=accepted_ms - 2,
        provider_acceptance_completion_ms=accepted_ms,
        request_completion_ms=accepted_ms + 1,
        observable_delivery_or_ack_ms=accepted_ms + 2,
    )


def _input(
    *,
    accepted_ms: int | None,
    finalized_through_ms: int,
    signal_event_id: str = _SHA_A,
    target_cursor: CausalTargetCursorV2 | None = None,
    attempt_id: str = "ATTEMPT-1",
) -> AlertActionabilityInputV2:
    return AlertActionabilityInputV2(
        attempt_id=attempt_id,
        signal_event_id=signal_event_id,
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        family=PromotingFamilyV2.A,
        promoting_plan_sha256=_SHA_B,
        target_cursor=target_cursor or _cursor(),
        finalized_through_ms=finalized_through_ms,
        transport=_transport(accepted_ms=accepted_ms),
    )


def _expected(
    signal_event_id: str,
    *,
    target_cursor: CausalTargetCursorV2 | None = None,
) -> ExpectedPromotingAlertV2:
    return ExpectedPromotingAlertV2(
        signal_event_id=signal_event_id,
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        family=PromotingFamilyV2.A,
        target_cursor=target_cursor or _cursor(),
    )


def _census(
    signal_event_ids: tuple[str, ...],
    *,
    attempt_id: str = "ATTEMPT-1",
) -> PromotingSignalCensusV2:
    return PromotingSignalCensusV2(
        attempt_id=attempt_id,
        promoting_plan_sha256=_SHA_B,
        promoting_signal_ledger_root_sha256=_SHA_D,
        expected_alerts=tuple(_expected(value) for value in signal_event_ids),
    )


@pytest.mark.parametrize(
    ("accepted_ms", "expected"),
    [
        (_TARGET - 1, AlertActionabilityStatusV2.ALERT_ON_TIME),
        (_TARGET, AlertActionabilityStatusV2.ALERT_ON_TIME),
        (_TARGET + 1, AlertActionabilityStatusV2.ALERT_LATE),
        (_TARGET + 30_000, AlertActionabilityStatusV2.ALERT_LATE),
        (_TARGET + 30_001, AlertActionabilityStatusV2.ALERT_MISSING),
    ],
)
def test_provider_acceptance_boundaries(
    accepted_ms: int,
    expected: AlertActionabilityStatusV2,
) -> None:
    record = evaluate_alert_actionability_v2(
        _input(accepted_ms=accepted_ms, finalized_through_ms=accepted_ms + 2)
    )

    assert record.status is expected
    assert record.changes_paper_execution is False
    assert record.changes_position_or_pnl_root is False


def test_primary_target_and_infimum_cursor_boundaries_are_structurally_bound() -> None:
    with pytest.raises(AlertActionabilityContractErrorV2, match="plus 10000"):
        _cursor(target_venue_ms=_TARGET - 1)
    with pytest.raises(AlertActionabilityContractErrorV2, match="plus 10000"):
        _cursor(target_venue_ms=_TARGET + 1)
    with pytest.raises(AlertActionabilityContractErrorV2, match="straddle"):
        _cursor(prior_venue_lower_bound_ms=_TARGET)
    with pytest.raises(AlertActionabilityContractErrorV2, match="straddle"):
        _cursor(target_venue_lower_bound_ms=_TARGET - 1)

    cursor = _cursor()
    changed = _cursor(target_local_cursor_ms=_TARGET + 1)
    assert cursor.cursor_evidence_sha256 != changed.cursor_evidence_sha256


def test_missing_remains_pending_until_inclusive_grace_boundary() -> None:
    pending = evaluate_alert_actionability_v2(
        _input(accepted_ms=None, finalized_through_ms=_TARGET + 29_999)
    )
    missing = evaluate_alert_actionability_v2(
        _input(accepted_ms=None, finalized_through_ms=_TARGET + 30_000)
    )

    assert pending.status is AlertActionabilityStatusV2.PENDING
    assert missing.status is AlertActionabilityStatusV2.ALERT_MISSING


def test_transport_lineage_is_ordered_and_finalization_is_causal() -> None:
    with pytest.raises(
        AlertActionabilityContractErrorV2,
        match="within the observed response",
    ):
        AlertTransportTimesV2(
            durable_outbox_enqueue_ms=1,
            send_start_ms=2,
            response_first_byte_ms=4,
            provider_acceptance_completion_ms=3,
            request_completion_ms=5,
        )

    with pytest.raises(
        AlertActionabilityContractErrorV2,
        match="after finalized_through_ms",
    ):
        replace(
            _input(accepted_ms=_TARGET, finalized_through_ms=_TARGET + 2),
            finalized_through_ms=_TARGET,
        )


def test_direct_record_cannot_claim_a_contradictory_status() -> None:
    item = _input(accepted_ms=_TARGET, finalized_through_ms=_TARGET + 2)
    with pytest.raises(AlertActionabilityContractErrorV2, match="contradicts"):
        AlertActionabilityRecordV2(
            attempt_id=item.attempt_id,
            signal_event_id=item.signal_event_id,
            symbol=item.symbol,
            venue=item.venue,
            family=item.family,
            promoting_plan_sha256=item.promoting_plan_sha256,
            target_cursor=item.target_cursor,
            finalized_through_ms=item.finalized_through_ms,
            transport=item.transport,
            status=AlertActionabilityStatusV2.ALERT_LATE,
            reasons=("FORGED",),
        )


def test_spot_signal_cannot_enter_promoting_actionability() -> None:
    with pytest.raises(AlertActionabilityContractErrorV2, match="USD-M Futures"):
        replace(
            _input(accepted_ms=_TARGET, finalized_through_ms=_TARGET + 2),
            venue=VenueV2.SPOT,
        )


def test_exact_99_percent_gate_accepts_equality_and_rejects_below() -> None:
    signal_ids = tuple(f"{index:064x}" for index in range(100))
    census = _census(signal_ids)
    records = tuple(
        evaluate_alert_actionability_v2(
            _input(
                accepted_ms=_TARGET if index < 99 else _TARGET + 1,
                finalized_through_ms=_TARGET + 30_000,
                signal_event_id=signal_event_id,
            )
        )
        for index, signal_event_id in enumerate(signal_ids)
    )
    equality = summarize_alert_actionability_v2(
        census,
        records,
        finalized_through_ms=_TARGET + 30_000,
    )
    below_records = tuple(
        replace(
            record,
            transport=_transport(accepted_ms=_TARGET + 1),
            status=AlertActionabilityStatusV2.ALERT_LATE,
            reasons=(
                AlertActionabilityStatusV2.ALERT_LATE.value,
                "DISCORD_TIMING_CANNOT_CHANGE_PAPER_EXECUTION_OR_PNL",
            ),
        )
        if index == 98
        else record
        for index, record in enumerate(records)
    )
    below = summarize_alert_actionability_v2(
        census,
        below_records,
        finalized_through_ms=_TARGET + 30_000,
    )

    assert equality.gate is AlertActionabilityGateV2.PASS
    assert equality.on_time_count == 99
    assert equality.attempted_count == len(census.expected_alerts)
    assert below.gate is AlertActionabilityGateV2.FAIL
    assert below.on_time_count == 98

    with pytest.raises(ValueError, match="init=False"):
        replace(equality, threshold_numerator=1)


def test_missing_record_cannot_be_dropped_from_census_denominator() -> None:
    signal_ids = (_SHA_A, _SHA_C)
    census = _census(signal_ids)
    only_first = evaluate_alert_actionability_v2(
        _input(
            accepted_ms=_TARGET,
            finalized_through_ms=_TARGET + 30_000,
            signal_event_id=_SHA_A,
        )
    )

    summary = summarize_alert_actionability_v2(
        census,
        (only_first,),
        finalized_through_ms=_TARGET + 30_000,
    )

    assert summary.attempted_count == 2
    assert summary.on_time_count == 1
    assert summary.missing_count == 1
    assert summary.gate is AlertActionabilityGateV2.FAIL


def test_pre_outbox_failure_is_pending_then_missing_from_census_alone() -> None:
    census = _census((_SHA_A,))

    pending = summarize_alert_actionability_v2(
        census,
        (),
        finalized_through_ms=_TARGET + 29_999,
    )
    missing = summarize_alert_actionability_v2(
        census,
        (),
        finalized_through_ms=_TARGET + 30_000,
    )
    empty = summarize_alert_actionability_v2(
        _census(()),
        (),
        finalized_through_ms=_TARGET + 30_000,
    )

    assert pending.gate is AlertActionabilityGateV2.NOT_FINALIZED
    assert missing.missing_count == 1
    assert missing.gate is AlertActionabilityGateV2.FAIL
    assert empty.gate is AlertActionabilityGateV2.INCONCLUSIVE_NO_ATTEMPTS


def test_identical_duplicates_do_not_inflate_summary_or_registry() -> None:
    record = evaluate_alert_actionability_v2(
        _input(accepted_ms=_TARGET, finalized_through_ms=_TARGET + 2)
    )
    registry = AlertActionabilityRegistryV2(maximum_events=1)

    assert registry.register(record) is AlertActionabilityRegistryDispositionV2.NEW
    assert (
        registry.register(record)
        is AlertActionabilityRegistryDispositionV2.IDEMPOTENT_DUPLICATE
    )
    summary = summarize_alert_actionability_v2(
        _census((_SHA_A,)),
        (record, record),
        finalized_through_ms=_TARGET + 2,
    )
    assert summary.attempted_count == 1


def test_pending_monitoring_view_cannot_enter_terminal_registry_or_summary() -> None:
    pending = evaluate_alert_actionability_v2(
        _input(accepted_ms=None, finalized_through_ms=_TARGET + 1)
    )
    registry = AlertActionabilityRegistryV2(maximum_events=1)

    with pytest.raises(AlertActionabilityContractErrorV2, match="monitoring view"):
        registry.register(pending)
    with pytest.raises(AlertActionabilityContractErrorV2, match="terminal records"):
        summarize_alert_actionability_v2(
            _census((_SHA_A,)),
            (pending,),
            finalized_through_ms=_TARGET + 1,
        )


def test_same_logical_attempt_with_different_payload_fails_closed() -> None:
    on_time = evaluate_alert_actionability_v2(
        _input(accepted_ms=_TARGET, finalized_through_ms=_TARGET + 2)
    )
    late = evaluate_alert_actionability_v2(
        _input(accepted_ms=_TARGET + 1, finalized_through_ms=_TARGET + 3)
    )
    assert on_time.event_id == late.event_id
    assert on_time.payload_sha256 != late.payload_sha256

    registry = AlertActionabilityRegistryV2(maximum_events=2)
    registry.register(on_time)
    with pytest.raises(AlertActionabilityContractErrorV2, match="collides"):
        registry.register(late)
    with pytest.raises(AlertActionabilityContractErrorV2, match="conflicting"):
        summarize_alert_actionability_v2(
            _census((_SHA_A,)),
            (on_time, late),
            finalized_through_ms=_TARGET + 3,
        )


def test_canonical_payload_is_deterministic_and_self_hashed() -> None:
    first = evaluate_alert_actionability_v2(
        _input(accepted_ms=_TARGET, finalized_through_ms=_TARGET + 2)
    )
    second = evaluate_alert_actionability_v2(
        _input(accepted_ms=_TARGET, finalized_through_ms=_TARGET + 2)
    )

    assert first.event_id == second.event_id
    assert first.payload_sha256 == second.payload_sha256
    assert canonical_alert_actionability_record_v2(first) == (
        canonical_alert_actionability_record_v2(second)
    )


def test_record_must_match_census_attempt_identity_target_and_membership() -> None:
    record = evaluate_alert_actionability_v2(
        _input(accepted_ms=_TARGET, finalized_through_ms=_TARGET + 2)
    )
    wrong_attempt = _census((_SHA_A,), attempt_id="ATTEMPT-2")
    with pytest.raises(AlertActionabilityContractErrorV2, match="attempt and plan"):
        summarize_alert_actionability_v2(
            wrong_attempt,
            (record,),
            finalized_through_ms=_TARGET + 2,
        )

    absent = _census((_SHA_C,))
    with pytest.raises(AlertActionabilityContractErrorV2, match="absent"):
        summarize_alert_actionability_v2(
            absent,
            (record,),
            finalized_through_ms=_TARGET + 2,
        )

    changed_target = PromotingSignalCensusV2(
        attempt_id="ATTEMPT-1",
        promoting_plan_sha256=_SHA_B,
        promoting_signal_ledger_root_sha256=_SHA_D,
        expected_alerts=(
            _expected(_SHA_A, target_cursor=_cursor(target_local_cursor_ms=_TARGET + 1)),
        ),
    )
    with pytest.raises(AlertActionabilityContractErrorV2, match="differs"):
        summarize_alert_actionability_v2(
            changed_target,
            (record,),
            finalized_through_ms=_TARGET + 2,
        )


def test_census_requires_canonical_unique_expected_signal_set() -> None:
    with pytest.raises(AlertActionabilityContractErrorV2, match="canonical"):
        _census((_SHA_C, _SHA_A))
    with pytest.raises(AlertActionabilityContractErrorV2, match="unique"):
        _census((_SHA_A, _SHA_A))
