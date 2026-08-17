from __future__ import annotations

import json
from dataclasses import replace
from decimal import ROUND_DOWN, Context, Decimal, localcontext

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.funding import (
    FUNDING_ENDPOINT_PATH_V2,
    FUNDING_HORIZON_GRACE_MS_V2,
    FUNDING_ROUTE_ID_V2,
    FundingConfirmationDecisionV2,
    FundingConfirmationInputV2,
    FundingConfirmationRegistryV2,
    FundingConfirmationStatusV2,
    FundingContractErrorV2,
    FundingHttpAttemptV2,
    FundingHttpErrorV2,
    FundingLotTimingV2,
    FundingPositionLedgerCheckpointV2,
    FundingPositionSnapshotV2,
    FundingRegistryDispositionV2,
    FundingScopeV2,
    build_funding_position_snapshot_v2,
    calculate_realized_funding_cashflow_v2,
    canonical_funding_confirmation_v2,
    canonical_realized_funding_cashflow_v2,
    evaluate_funding_confirmation_v2,
    funding_position_ledger_leaf_sha256_v2,
    funding_position_ledger_root_v2,
)
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2

_F = 1_800_000_000_000
_H_END = _F + 600_000
_H_MAX = _H_END
_DEADLINE = _H_END + FUNDING_HORIZON_GRACE_MS_V2
_SYMBOL = "BTCUSDT"


def _sha(label: str) -> str:
    import hashlib

    return hashlib.sha256(label.encode()).hexdigest()


_SCOPE = FundingScopeV2(
    attempt_id="funding-attempt-1",
    plan_id="frozen-plan-v2",
    protocol_hash=_sha("protocol"),
    universe_sha256=_sha("universe"),
)


def _query(
    *,
    symbol: str = _SYMBOL,
    funding_time_ms: int = _F,
) -> tuple[tuple[str, str], ...]:
    return (
        ("endTime", str(funding_time_ms)),
        ("limit", "1"),
        ("startTime", str(funding_time_ms)),
        ("symbol", symbol),
    )


def _body(
    *,
    symbol: str = _SYMBOL,
    funding_time_ms: int = _F,
    rate: str = "0.00010000",
    mark: str = "50000.00",
) -> bytes:
    return json.dumps(
        [
            {
                "fundingRate": rate,
                "fundingTime": funding_time_ms,
                "markPrice": mark,
                "symbol": symbol,
            }
        ],
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _attempt(
    *,
    funding_time_ms: int = _F,
    request_number: int = 1,
    previous: FundingHttpAttemptV2 | None = None,
    previous_sha256: str | None = None,
    completion_ms: int | None = None,
    query: tuple[tuple[str, str], ...] | None = None,
    body: bytes | None = None,
    method: str = "GET",
    route_id: str = FUNDING_ROUTE_ID_V2,
    endpoint_path: str = FUNDING_ENDPOINT_PATH_V2,
    response_status: int | None = 200,
    payload_complete: bool = True,
    error: FundingHttpErrorV2 | None = None,
    content_type: str | None = "application/json",
    correlation_id: str = "funding-BTCUSDT-1800000000000",
    scope: FundingScopeV2 = _SCOPE,
) -> FundingHttpAttemptV2:
    prior_completion = previous.response_completion_ms if previous is not None else funding_time_ms
    if completion_ms is None:
        completion_ms = prior_completion + 1_000
    request_started_ms = max(funding_time_ms, prior_completion + (1 if previous else 0))
    if previous_sha256 is None and previous is not None:
        previous_sha256 = previous.payload_sha256
    return FundingHttpAttemptV2(
        scope=scope,
        correlation_id=correlation_id,
        request_number=request_number,
        previous_attempt_payload_sha256=previous_sha256,
        request_started_ms=request_started_ms,
        response_completion_ms=completion_ms,
        receipt_monotonic_ns=1_000_000 + request_number,
        ingest_seq=request_number,
        method=method,
        route_id=route_id,
        endpoint_path=endpoint_path,
        canonical_query=query if query is not None else _query(funding_time_ms=funding_time_ms),
        response_status=response_status,
        content_type=content_type,
        payload_complete=payload_complete,
        raw_response_bytes=_body(funding_time_ms=funding_time_ms) if body is None else body,
        error=error,
    )


def _timeout(
    *,
    request_number: int = 1,
    previous: FundingHttpAttemptV2 | None = None,
    previous_sha256: str | None = None,
) -> FundingHttpAttemptV2:
    prior_completion = previous.response_completion_ms if previous is not None else _F
    return _attempt(
        request_number=request_number,
        previous=previous,
        previous_sha256=previous_sha256,
        completion_ms=prior_completion + 1_000,
        body=b"",
        response_status=None,
        payload_complete=False,
        error=FundingHttpErrorV2.TIMEOUT,
        content_type=None,
    )


def _input(
    *attempts: FundingHttpAttemptV2,
    funding_time_ms: int = _F,
    horizon_end_ms: int = _H_END,
    horizon_max_ms: int = _H_MAX,
    observed_through_ms: int | None = None,
    candidate_set_complete: bool = True,
    maximum_attempts: int = 4,
    scope: FundingScopeV2 = _SCOPE,
) -> FundingConfirmationInputV2:
    deadline = min(funding_time_ms + 900_000, horizon_end_ms + 60_000)
    latest_completion = max(
        (attempt.response_completion_ms for attempt in attempts),
        default=0,
    )
    return FundingConfirmationInputV2(
        scope=scope,
        symbol=_SYMBOL,
        funding_time_ms=funding_time_ms,
        horizon_end_ms=horizon_end_ms,
        horizon_max_ms=horizon_max_ms,
        observed_through_ms=(
            max(deadline, latest_completion) if observed_through_ms is None else observed_through_ms
        ),
        candidate_set_complete=candidate_set_complete,
        maximum_attempts=maximum_attempts,
        attempts=tuple(attempts),
    )


def _confirmed(
    *,
    rate: str = "0.00010000",
    mark: str = "50000.00",
) -> tuple[FundingConfirmationDecisionV2, FundingConfirmationRegistryV2]:
    decision = evaluate_funding_confirmation_v2(_input(_attempt(body=_body(rate=rate, mark=mark))))
    registry = FundingConfirmationRegistryV2(maximum_events=4, scope=_SCOPE)
    registry.register(decision)
    return decision, registry


def _position(
    quantity: Decimal,
    *,
    lot_timing: FundingLotTimingV2 = FundingLotTimingV2.STRICTLY_BEFORE_FUNDING,
    funding_time_ms: int = _F,
    horizon_max_ms: int = _H_MAX,
    contract_multiplier: Decimal = Decimal("1"),
) -> FundingPositionSnapshotV2:
    position_event_id = _sha(f"position-{quantity}-{lot_timing.value}")
    position_payload_sha256 = _sha(f"position-payload-{quantity}-{lot_timing.value}")
    position_source_root_sha256 = _sha("position-source")
    multiplier_version = _sha("multiplier-v1")
    quantity_timestamp_ms = (
        funding_time_ms
        if lot_timing is FundingLotTimingV2.EQUAL_MS_AMBIGUOUS
        else funding_time_ms - 1
    )
    leaf = funding_position_ledger_leaf_sha256_v2(
        scope=_SCOPE,
        symbol=_SYMBOL,
        funding_time_ms=funding_time_ms,
        horizon_max_ms=horizon_max_ms,
        position_event_id=position_event_id,
        position_payload_sha256=position_payload_sha256,
        position_source_root_sha256=position_source_root_sha256,
        signed_quantity_before_funding=quantity,
        contract_multiplier=contract_multiplier,
        contract_multiplier_version_sha256=multiplier_version,
        quantity_timestamp_ms=quantity_timestamp_ms,
        lot_timing=lot_timing,
    )
    checkpoint = FundingPositionLedgerCheckpointV2(
        scope=_SCOPE,
        ledger_id="position-ledger-1",
        ledger_root_sha256=funding_position_ledger_root_v2((leaf,)),
        event_count=1,
        observed_through_ms=funding_time_ms,
        sealed_at_ms=funding_time_ms + 1,
    )
    return build_funding_position_snapshot_v2(
        scope=_SCOPE,
        symbol=_SYMBOL,
        funding_time_ms=funding_time_ms,
        horizon_max_ms=horizon_max_ms,
        position_event_id=position_event_id,
        position_payload_sha256=position_payload_sha256,
        position_source_root_sha256=position_source_root_sha256,
        signed_quantity_before_funding=quantity,
        contract_multiplier=contract_multiplier,
        contract_multiplier_version_sha256=multiplier_version,
        quantity_timestamp_ms=quantity_timestamp_ms,
        lot_timing=lot_timing,
        ledger_checkpoint=checkpoint,
        expected_ledger_checkpoint_sha256=checkpoint.checkpoint_sha256,
        ledger_leaf_index=0,
        ledger_merkle_siblings=(),
    )


def test_exact_public_route_row_and_deadline_confirm_deterministically() -> None:
    attempt = _attempt()
    item = _input(attempt)
    first = evaluate_funding_confirmation_v2(item)
    second = evaluate_funding_confirmation_v2(item)

    assert attempt.method == "GET"
    assert attempt.endpoint_path == "/fapi/v1/fundingRate"
    assert attempt.canonical_query == _query()
    assert item.confirmation_deadline_ms == _DEADLINE
    assert first.status is FundingConfirmationStatusV2.CONFIRMED
    assert first.funding_rate == Decimal("0.00010000")
    assert first.mark_price == Decimal("50000.00")
    assert first == second
    assert canonical_funding_confirmation_v2(first) == canonical_funding_confirmation_v2(second)


def test_request_mismatch_is_typed_inconclusive() -> None:
    wrong_query = (
        ("endTime", str(_F)),
        ("limit", "1"),
        ("startTime", str(_F - 1)),
        ("symbol", _SYMBOL),
    )
    decision = evaluate_funding_confirmation_v2(_input(_attempt(query=wrong_query)))
    assert decision.status is FundingConfirmationStatusV2.INCONCLUSIVE_REQUEST_MISMATCH
    assert decision.funding_rate is None


@pytest.mark.parametrize(
    "payload",
    [
        b"[]",
        b"not-json",
        json.dumps([{}, {}]).encode(),
        _body(symbol="ETHUSDT"),
        _body(funding_time_ms=_F + 1),
        _body(rate="NaN"),
        _body(mark="0"),
        json.dumps(
            [
                {
                    "fundingRate": 0.0001,
                    "fundingTime": _F,
                    "markPrice": "50000",
                    "symbol": _SYMBOL,
                }
            ]
        ).encode(),
        json.dumps(
            [
                {
                    "extra": "not-allowed",
                    "fundingRate": "0.0001",
                    "fundingTime": _F,
                    "markPrice": "50000",
                    "symbol": _SYMBOL,
                }
            ]
        ).encode(),
    ],
)
def test_missing_malformed_or_mismatched_row_never_uses_numeric_fallback(
    payload: bytes,
) -> None:
    decision = evaluate_funding_confirmation_v2(_input(_attempt(body=payload)))
    assert decision.status is FundingConfirmationStatusV2.INCONCLUSIVE_RESPONSE_MISMATCH
    assert decision.funding_rate is None
    assert decision.mark_price is None


def test_missing_after_recorded_retries_and_bad_lineage_are_typed() -> None:
    first = _timeout()
    second = _timeout(request_number=2, previous=first)
    missing = evaluate_funding_confirmation_v2(_input(first, second))
    assert missing.status is FundingConfirmationStatusV2.INCONCLUSIVE_MISSING_CONFIRMATION
    assert missing.candidate_attempt_count == 2
    assert missing.retry_count == 1

    wrong_chain = _timeout(
        request_number=2,
        previous=first,
        previous_sha256=_sha("wrong-prior"),
    )
    lineage = evaluate_funding_confirmation_v2(_input(first, wrong_chain))
    assert lineage.status is FundingConfirmationStatusV2.INCONCLUSIVE_RETRY_LINEAGE


def test_identical_duplicates_are_canonical_noops_and_conflicts_are_order_stable() -> None:
    attempt = _attempt()
    single = evaluate_funding_confirmation_v2(_input(attempt))
    duplicate = evaluate_funding_confirmation_v2(_input(attempt, attempt))
    assert duplicate == single

    changed = _attempt(body=_body(rate="0.00020000"))
    left = evaluate_funding_confirmation_v2(_input(attempt, changed))
    right = evaluate_funding_confirmation_v2(_input(changed, attempt))
    assert left.status is FundingConfirmationStatusV2.INCONCLUSIVE_CONFLICTING_DUPLICATE
    assert canonical_funding_confirmation_v2(left) == canonical_funding_confirmation_v2(right)


def test_distinct_retry_confirmations_must_agree() -> None:
    first = _attempt(body=_body(rate="0.00010000"))
    second = _attempt(
        request_number=2,
        previous=first,
        body=_body(rate="0.00020000"),
    )
    decision = evaluate_funding_confirmation_v2(_input(first, second))
    assert decision.status is FundingConfirmationStatusV2.INCONCLUSIVE_CONFLICTING_CONFIRMATIONS


def test_candidate_completeness_and_response_deadline_boundaries() -> None:
    before_cutoff = evaluate_funding_confirmation_v2(
        _input(
            _attempt(),
            observed_through_ms=_DEADLINE - 1,
            candidate_set_complete=False,
        )
    )
    assert before_cutoff.status is FundingConfirmationStatusV2.PENDING_CONFIRMATION

    on_deadline = evaluate_funding_confirmation_v2(_input(_attempt(completion_ms=_DEADLINE)))
    assert on_deadline.status is FundingConfirmationStatusV2.CONFIRMED

    late = evaluate_funding_confirmation_v2(_input(_attempt(completion_ms=_DEADLINE + 1)))
    assert late.status is FundingConfirmationStatusV2.INCONCLUSIVE_LATE_RESPONSE


def test_funding_at_horizon_max_can_confirm_in_grace_without_post_horizon_mark() -> None:
    funding_time_ms = _F + 1_000_000
    deadline = funding_time_ms + FUNDING_HORIZON_GRACE_MS_V2
    attempt = _attempt(
        funding_time_ms=funding_time_ms,
        completion_ms=deadline,
        body=_body(funding_time_ms=funding_time_ms),
    )
    decision = evaluate_funding_confirmation_v2(
        _input(
            attempt,
            funding_time_ms=funding_time_ms,
            horizon_end_ms=funding_time_ms,
            horizon_max_ms=funding_time_ms,
        )
    )
    registry = FundingConfirmationRegistryV2(maximum_events=2, scope=_SCOPE)
    registry.register(decision)
    cashflow = calculate_realized_funding_cashflow_v2(
        decision,
        _position(
            Decimal("2"),
            funding_time_ms=funding_time_ms,
            horizon_max_ms=funding_time_ms,
        ),
        registry=registry,
        externally_pinned_checkpoint_sha256=(registry.terminal_checkpoint_v2().checkpoint_sha256),
    )
    assert decision.selected_response_completion_ms == deadline
    assert cashflow.market_value_time_ms == funding_time_ms
    assert cashflow.market_value_time_ms <= cashflow.horizon_max_ms


@pytest.mark.parametrize(
    ("quantity", "rate", "expected"),
    [
        (Decimal("2"), "0.00010000", Decimal("-10.0000000000")),
        (Decimal("-2"), "0.00010000", Decimal("10.0000000000")),
        (Decimal("2"), "-0.00010000", Decimal("10.0000000000")),
        (Decimal("-2"), "-0.00010000", Decimal("-10.0000000000")),
    ],
)
def test_long_short_and_rate_signs_follow_negative_signed_exposure_formula(
    quantity: Decimal,
    rate: str,
    expected: Decimal,
) -> None:
    decision, registry = _confirmed(rate=rate)
    checkpoint = registry.terminal_checkpoint_v2()
    cashflow = calculate_realized_funding_cashflow_v2(
        decision,
        _position(quantity),
        registry=registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    assert cashflow.realized_cashflow == expected


def test_equal_ms_ambiguous_lot_keeps_loss_and_discards_gain() -> None:
    decision, registry = _confirmed()
    checkpoint = registry.terminal_checkpoint_v2()
    long_loss = calculate_realized_funding_cashflow_v2(
        decision,
        _position(Decimal("2"), lot_timing=FundingLotTimingV2.EQUAL_MS_AMBIGUOUS),
        registry=registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    short_gain = calculate_realized_funding_cashflow_v2(
        decision,
        _position(Decimal("-2"), lot_timing=FundingLotTimingV2.EQUAL_MS_AMBIGUOUS),
        registry=registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    assert long_loss.realized_cashflow == long_loss.normal_cashflow < 0
    assert short_gain.normal_cashflow > 0
    assert short_gain.realized_cashflow == 0


@given(
    quantity_units=st.integers(min_value=1, max_value=1_000_000),
    rate_units=st.integers(min_value=1, max_value=10_000),
)
@settings(max_examples=40)
def test_cashflow_sign_symmetry_property(
    quantity_units: int,
    rate_units: int,
) -> None:
    quantity = Decimal(quantity_units).scaleb(-3)
    rate = Decimal(rate_units).scaleb(-8)
    decision, registry = _confirmed(rate=format(rate, "f"), mark="12345.67")
    checkpoint = registry.terminal_checkpoint_v2()
    long = calculate_realized_funding_cashflow_v2(
        decision,
        _position(quantity),
        registry=registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    short = calculate_realized_funding_cashflow_v2(
        decision,
        _position(-quantity),
        registry=registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    assert long.realized_cashflow == -short.realized_cashflow
    assert long.realized_cashflow < 0 < short.realized_cashflow


def test_cashflow_uses_shared_decimal34_not_hostile_ambient_context() -> None:
    decision, registry = _confirmed(rate="0.00012345678901234567890123456789")
    checkpoint = registry.terminal_checkpoint_v2()
    position = _position(
        Decimal("123456789.123456789123456789"),
        contract_multiplier=Decimal("0.00123456789123456789"),
    )
    baseline = calculate_realized_funding_cashflow_v2(
        decision,
        position,
        registry=registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    hostile = Context(prec=3, rounding=ROUND_DOWN)
    with localcontext(hostile):
        repeated = calculate_realized_funding_cashflow_v2(
            decision,
            position,
            registry=registry,
            externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
        )
    assert repeated.normal_cashflow == baseline.normal_cashflow
    with localcontext(protocol_decimal_context_v2()) as context:
        expected = context.minus(
            context.multiply(
                context.multiply(
                    context.multiply(
                        position.signed_quantity_before_funding,
                        position.contract_multiplier,
                    ),
                    Decimal("50000.00"),
                ),
                Decimal("0.00012345678901234567890123456789"),
            )
        )
    assert baseline.normal_cashflow == expected


def test_registry_is_bounded_idempotent_and_restores_only_against_external_pin() -> None:
    decision = evaluate_funding_confirmation_v2(_input(_attempt()))
    registry = FundingConfirmationRegistryV2(maximum_events=1, scope=_SCOPE)
    assert registry.register(decision) is FundingRegistryDispositionV2.NEW
    assert registry.register(decision) is FundingRegistryDispositionV2.IDEMPOTENT_DUPLICATE
    checkpoint = registry.terminal_checkpoint_v2()
    state = registry.export_state_v2()
    restored = FundingConfirmationRegistryV2.from_state_v2(
        state,
        expected_replay_root_sha256=checkpoint.replay_root_sha256,
        expected_event_count=checkpoint.event_count,
        expected_maximum_events=checkpoint.maximum_events,
        expected_scope=_SCOPE,
        expected_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    assert restored.contains_exact_v2(decision)
    assert restored.replay_root_sha256 == registry.replay_root_sha256
    with pytest.raises(FundingContractErrorV2, match="external funding checkpoint"):
        FundingConfirmationRegistryV2.from_state_v2(
            state,
            expected_replay_root_sha256=checkpoint.replay_root_sha256,
            expected_event_count=checkpoint.event_count,
            expected_maximum_events=checkpoint.maximum_events,
            expected_scope=_SCOPE,
            expected_checkpoint_sha256="f" * 64,
        )

    pending = evaluate_funding_confirmation_v2(
        _input(
            observed_through_ms=_DEADLINE - 1,
            candidate_set_complete=False,
        )
    )
    with pytest.raises(FundingContractErrorV2, match="pending"):
        registry.register(pending)


def test_registry_restore_rejects_canonical_state_corruption() -> None:
    decision = evaluate_funding_confirmation_v2(_input(_attempt()))
    registry = FundingConfirmationRegistryV2(maximum_events=2, scope=_SCOPE)
    registry.register(decision)
    checkpoint = registry.terminal_checkpoint_v2()
    document = json.loads(registry.export_state_v2())
    document["events"] = []
    corrupted = canonical_json_line(document)
    with pytest.raises(FundingContractErrorV2, match="event census"):
        FundingConfirmationRegistryV2.from_state_v2(
            corrupted,
            expected_replay_root_sha256=checkpoint.replay_root_sha256,
            expected_event_count=checkpoint.event_count,
            expected_maximum_events=checkpoint.maximum_events,
            expected_scope=_SCOPE,
            expected_checkpoint_sha256=checkpoint.checkpoint_sha256,
        )


def test_position_quantity_requires_factory_external_pin_and_merkle_membership() -> None:
    valid = _position(Decimal("1"))
    with pytest.raises(FundingContractErrorV2, match="externally pinned membership"):
        replace(valid, signed_quantity_before_funding=Decimal("2"))

    checkpoint = FundingPositionLedgerCheckpointV2(
        scope=_SCOPE,
        ledger_id="position-ledger-1",
        ledger_root_sha256=valid.position_ledger_leaf_sha256,
        event_count=1,
        observed_through_ms=_F,
        sealed_at_ms=_F + 1,
    )
    arguments = {
        "scope": valid.scope,
        "symbol": valid.symbol,
        "funding_time_ms": valid.funding_time_ms,
        "horizon_max_ms": valid.horizon_max_ms,
        "position_event_id": valid.position_event_id,
        "position_payload_sha256": valid.position_payload_sha256,
        "position_source_root_sha256": valid.position_source_root_sha256,
        "signed_quantity_before_funding": valid.signed_quantity_before_funding,
        "contract_multiplier": valid.contract_multiplier,
        "contract_multiplier_version_sha256": (valid.contract_multiplier_version_sha256),
        "quantity_timestamp_ms": valid.quantity_timestamp_ms,
        "lot_timing": valid.lot_timing,
        "ledger_leaf_index": 0,
        "ledger_merkle_siblings": (),
    }
    with pytest.raises(FundingContractErrorV2, match="external pin"):
        build_funding_position_snapshot_v2(
            **arguments,
            ledger_checkpoint=checkpoint,
            expected_ledger_checkpoint_sha256="f" * 64,
        )

    wrong_root_checkpoint = replace(
        checkpoint,
        ledger_root_sha256="f" * 64,
    )
    with pytest.raises(FundingContractErrorV2, match="not a member"):
        build_funding_position_snapshot_v2(
            **arguments,
            ledger_checkpoint=wrong_root_checkpoint,
            expected_ledger_checkpoint_sha256=(wrong_root_checkpoint.checkpoint_sha256),
        )


def test_cashflow_requires_exact_registry_membership_and_external_pin() -> None:
    decision, registry = _confirmed()
    checkpoint = registry.terminal_checkpoint_v2()
    cashflow = calculate_realized_funding_cashflow_v2(
        decision,
        _position(Decimal("1")),
        registry=registry,
        externally_pinned_checkpoint_sha256=checkpoint.checkpoint_sha256,
    )
    assert (
        json.loads(canonical_realized_funding_cashflow_v2(cashflow))["realized_cashflow"]
        == "-5.0000000000"
    )
    assert (
        cashflow.position_ledger_checkpoint_sha256
        == _position(Decimal("1")).position_ledger_checkpoint_sha256
    )
    with pytest.raises(FundingContractErrorV2, match="external pin"):
        calculate_realized_funding_cashflow_v2(
            decision,
            _position(Decimal("1")),
            registry=registry,
            externally_pinned_checkpoint_sha256="f" * 64,
        )


def test_transport_contract_rejects_private_or_non_json_success_evidence() -> None:
    with pytest.raises(FundingContractErrorV2, match="public and anonymous"):
        replace(_attempt(), account_authenticated=True)
    with pytest.raises(FundingContractErrorV2, match="credential"):
        _attempt(query=(("signature", "secret"),))
    with pytest.raises(FundingContractErrorV2, match="requires JSON"):
        replace(_attempt(), content_type="text/html")
