from __future__ import annotations

import hashlib
from dataclasses import replace
from decimal import ROUND_DOWN, ROUND_UP, Context, Decimal, localcontext

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.strategy.family_a import (
    FamilyAEntryInputV2,
    FamilyAEpisodeLedgerV2,
    canonical_family_a_entry_decision_v2,
)
from signalbot.r4b_v2.strategy.family_a_features import (
    FAMILY_A_ENTRY_PRIOR_BARS_V2,
    FAMILY_A_EXIT_PRIOR_BARS_V2,
    FamilyAClosedKlineV2,
    FamilyAContractMultiplierV2,
    FamilyAEntryFeatureEvidenceV2,
    FamilyAFeatureContractErrorV2,
    FamilyAFeatureReadinessV2,
    FamilyAFlowWindowV2,
    FamilyAMarkIndexFundingV2,
    FamilyANormalFlowTradeV2,
    FamilyAOIResponseV2,
    FamilyAPriorBarEvidenceV2,
    FamilyASourceBindingV2,
    FamilyASourceKindV2,
    build_family_a_contract_multiplier_v2,
    build_family_a_entry_feature_evidence_v2,
    build_family_a_exit_feature_evidence_v2,
    build_family_a_post_cutoff_completeness_conflict_v2,
    build_family_a_prior_bar_evidence_v2,
)

ATTEMPT = "attempt-1"
SYMBOL = "BTCUSDT"
PLAN = "a" * 64
BAR_OPEN = 2_000_160_000_000
BAR_CLOSE = BAR_OPEN + FIVE_MINUTE_MS_V2 - 1
D_MS = BAR_CLOSE + DECISION_DELAY_MS_V2


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _binding(
    kind: FamilyASourceKindV2,
    *,
    cursor_receipt_ms: int = D_MS,
) -> FamilyASourceBindingV2:
    return FamilyASourceBindingV2.create(
        attempt_id=ATTEMPT,
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        source_kind=kind,
        capture_root_sha256=_sha(f"capture-{kind.value}"),
        schema_sha256=_sha(f"schema-{kind.value}"),
        clock_segment_root_sha256=_sha(f"clock-{kind.value}"),
        capture_cursor_ingest_seq=1_000_000,
        capture_cursor_receipt_ms=cursor_receipt_ms,
        candidate_set_complete=True,
    )


KLINE_BINDING = _binding(FamilyASourceKindV2.KLINE)
OI_BINDING = _binding(FamilyASourceKindV2.OPEN_INTEREST)
MARK_BINDING = _binding(FamilyASourceKindV2.MARK_INDEX_PREDICTED_FUNDING)
FLOW_BINDING = _binding(FamilyASourceKindV2.NORMAL_FUTURES_FLOW)
CONTRACT_VERSION: FamilyAContractMultiplierV2 = build_family_a_contract_multiplier_v2(
    attempt_id=ATTEMPT,
    symbol=SYMBOL,
    venue=VenueV2.USDM_FUTURES,
    promoting_plan_sha256=PLAN,
    contract_multiplier=Decimal(1),
    effective_from_ms=BAR_OPEN - 10_000 * FIVE_MINUTE_MS_V2,
    effective_until_ms=BAR_OPEN + 20 * FIVE_MINUTE_MS_V2,
    source_root_sha256=_sha("contract-version-source"),
    schema_sha256=_sha("contract-version-schema"),
)


def _kline(
    bar_open_ms: int,
    *,
    close: Decimal = Decimal("100"),
    receipt_ms: int | None = None,
    label: str = "kline",
    binding: FamilyASourceBindingV2 = KLINE_BINDING,
) -> FamilyAClosedKlineV2:
    bar_close = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    return FamilyAClosedKlineV2(
        binding=binding,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close,
        event_time_ms=bar_close,
        receipt_ms=bar_close + 1 if receipt_ms is None else receipt_ms,
        close=close,
        high=close + Decimal("2"),
        low=close - Decimal("2"),
        source_evidence_sha256=_sha(label),
    )


def _oi(
    event_ms: int,
    receipt_ms: int,
    *,
    value: Decimal = Decimal("1000"),
    ingest_seq: int = 1,
    label: str = "oi",
    binding: FamilyASourceBindingV2 = OI_BINDING,
) -> FamilyAOIResponseV2:
    return FamilyAOIResponseV2(
        binding=binding,
        payload_time_ms=event_ms,
        response_completion_ms=receipt_ms,
        ingest_seq=ingest_seq,
        open_interest=value,
        source_evidence_sha256=_sha(label),
    )


def _mark(
    event_ms: int,
    receipt_ms: int,
    *,
    mark: Decimal = Decimal("100.01"),
    funding: Decimal = Decimal("0.0001"),
    ingest_seq: int = 1,
    label: str = "mark",
    binding: FamilyASourceBindingV2 = MARK_BINDING,
) -> FamilyAMarkIndexFundingV2:
    return FamilyAMarkIndexFundingV2(
        binding=binding,
        transaction_time_ms=event_ms,
        receipt_ms=receipt_ms,
        ingest_seq=ingest_seq,
        mark_price=mark,
        index_price=Decimal("100"),
        predicted_funding_rate=funding,
        source_evidence_sha256=_sha(label),
    )


def _flow(
    bar_open_ms: int,
    *,
    complete: bool = True,
    trades: tuple[FamilyANormalFlowTradeV2, ...] = (),
    completion_ms: int | None = None,
    binding: FamilyASourceBindingV2 = FLOW_BINDING,
) -> FamilyAFlowWindowV2:
    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    return FamilyAFlowWindowV2(
        binding=binding,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        trades=trades,
        contract_version=CONTRACT_VERSION,
        capture_complete=complete,
        completeness_completion_ms=(bar_close_ms + 1 if completion_ms is None else completion_ms),
        completeness_evidence_sha256=_sha(f"flow-complete-{bar_open_ms}-{complete}"),
    )


def test_source_bindings_freeze_exact_usdm_routes_symbols_and_cursor() -> None:
    assert KLINE_BINDING.route_id == "usdm_market"
    assert KLINE_BINDING.source_locator == "btcusdt@kline_5m"
    assert OI_BINDING.route_id == "usdm_public_rest"
    assert OI_BINDING.source_locator == "/fapi/v1/openInterest"
    assert MARK_BINDING.source_locator == "btcusdt@markPrice@1s"
    assert FLOW_BINDING.source_locator == "btcusdt@aggTrade"
    assert all(
        value.payload_symbol == SYMBOL and value.plan_symbol == SYMBOL
        for value in (KLINE_BINDING, OI_BINDING, MARK_BINDING, FLOW_BINDING)
    )
    with pytest.raises(FamilyAFeatureContractErrorV2, match="complete candidate cursor"):
        FamilyASourceBindingV2.create(
            attempt_id=ATTEMPT,
            symbol=SYMBOL,
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN,
            source_kind=FamilyASourceKindV2.OPEN_INTEREST,
            capture_root_sha256=_sha("incomplete-capture"),
            schema_sha256=_sha("incomplete-schema"),
            clock_segment_root_sha256=_sha("incomplete-clock"),
            capture_cursor_ingest_seq=1,
            capture_cursor_receipt_ms=D_MS,
            candidate_set_complete=False,
        )


def test_prior_selector_accepts_staleness_equalities_and_later_ingest_seq() -> None:
    kline = _kline(BAR_OPEN)
    older = _oi(
        BAR_CLOSE - 10_000,
        D_MS,
        value=Decimal("1001"),
        ingest_seq=1,
        label="oi-old-ingest",
    )
    later_ingest = replace(
        older,
        ingest_seq=2,
        open_interest=Decimal("1002"),
        source_evidence_sha256=_sha("oi-later-ingest"),
    )
    mark = _mark(BAR_CLOSE - 2_000, D_MS)

    result = build_family_a_prior_bar_evidence_v2(
        kline=kline,
        oi_source_binding=OI_BINDING,
        mark_source_binding=MARK_BINDING,
        oi_responses=(later_ingest, older),
        mark_observations=(mark,),
        as_of_cutoff_ms=D_MS,
    )

    assert result.selected_oi.ingest_seq == 2
    assert result.open_interest == Decimal("1002")
    assert result.selected_mark.transaction_time_ms == BAR_CLOSE - 2_000


@pytest.mark.parametrize(
    ("oi_age", "mark_age", "match"),
    [
        (10_001, 2_000, "OI is unavailable"),
        (10_000, 2_001, "mark/funding is unavailable"),
    ],
)
def test_prior_selector_rejects_one_ms_beyond_staleness(
    oi_age: int,
    mark_age: int,
    match: str,
) -> None:
    with pytest.raises(FamilyAFeatureContractErrorV2, match=match):
        build_family_a_prior_bar_evidence_v2(
            kline=_kline(BAR_OPEN),
            oi_source_binding=OI_BINDING,
            mark_source_binding=MARK_BINDING,
            oi_responses=(_oi(BAR_CLOSE - oi_age, D_MS),),
            mark_observations=(_mark(BAR_CLOSE - mark_age, D_MS),),
            as_of_cutoff_ms=D_MS,
        )


def test_receipt_at_d_accepts_and_d_plus_one_is_not_backdated() -> None:
    accepted = build_family_a_prior_bar_evidence_v2(
        kline=_kline(BAR_OPEN),
        oi_source_binding=OI_BINDING,
        mark_source_binding=MARK_BINDING,
        oi_responses=(_oi(BAR_CLOSE, D_MS),),
        mark_observations=(_mark(BAR_CLOSE, D_MS),),
        as_of_cutoff_ms=D_MS,
    )
    assert accepted.selected_oi.response_completion_ms == D_MS

    with pytest.raises(FamilyAFeatureContractErrorV2, match="OI is unavailable"):
        build_family_a_prior_bar_evidence_v2(
            kline=_kline(BAR_OPEN),
            oi_source_binding=OI_BINDING,
            mark_source_binding=MARK_BINDING,
            oi_responses=(
                _oi(
                    BAR_CLOSE,
                    D_MS + 1,
                    binding=_binding(
                        FamilyASourceKindV2.OPEN_INTEREST,
                        cursor_receipt_ms=D_MS + 1,
                    ),
                ),
            ),
            mark_observations=(_mark(BAR_CLOSE, D_MS),),
            as_of_cutoff_ms=D_MS,
        )


def test_conflicting_same_raw_identity_fails_closed() -> None:
    first = _oi(BAR_CLOSE, D_MS, ingest_seq=7, label="first")
    conflict = replace(
        first,
        open_interest=Decimal("2000"),
        source_evidence_sha256=_sha("conflict"),
    )
    with pytest.raises(FamilyAFeatureContractErrorV2, match="conflicting OI"):
        build_family_a_prior_bar_evidence_v2(
            kline=_kline(BAR_OPEN),
            oi_source_binding=OI_BINDING,
            mark_source_binding=MARK_BINDING,
            oi_responses=(first, conflict),
            mark_observations=(_mark(BAR_CLOSE, D_MS),),
            as_of_cutoff_ms=D_MS,
        )


def test_nq_is_required_bounded_and_never_falls_back_to_q() -> None:
    with pytest.raises(FamilyAFeatureContractErrorV2, match="nq cannot exceed q"):
        FamilyANormalFlowTradeV2(
            binding=FLOW_BINDING,
            trade_id=1,
            transaction_time_ms=BAR_OPEN,
            receipt_ms=D_MS,
            price=Decimal("100"),
            quantity=Decimal("1"),
            normal_quantity=Decimal("1.1"),
            contract_multiplier=Decimal("1"),
            buyer_maker=False,
            source_evidence_sha256=_sha("bad-nq"),
        )
    with pytest.raises(FamilyAFeatureContractErrorV2, match="q and nq"):
        FamilyANormalFlowTradeV2(
            binding=FLOW_BINDING,
            trade_id=2,
            transaction_time_ms=BAR_OPEN,
            receipt_ms=D_MS,
            price=Decimal("100"),
            quantity=Decimal("1"),
            normal_quantity=None,  # type: ignore[arg-type]
            contract_multiplier=Decimal("1"),
            buyer_maker=False,
            source_evidence_sha256=_sha("missing-nq"),
        )


def test_flow_duplicate_is_noop_but_conflict_and_mixed_multiplier_fail() -> None:
    trade = FamilyANormalFlowTradeV2(
        binding=FLOW_BINDING,
        trade_id=7,
        transaction_time_ms=BAR_OPEN + 1,
        receipt_ms=D_MS,
        price=Decimal("100"),
        quantity=Decimal("1"),
        normal_quantity=Decimal("1"),
        contract_multiplier=Decimal("1"),
        buyer_maker=False,
        source_evidence_sha256=_sha("flow-duplicate"),
    )
    canonical = _flow(BAR_OPEN, trades=(trade, trade))
    assert canonical.trades == (trade,)

    with pytest.raises(FamilyAFeatureContractErrorV2, match="conflicting"):
        _flow(
            BAR_OPEN,
            trades=(
                trade,
                replace(trade, source_evidence_sha256=_sha("flow-conflict")),
            ),
        )
    with pytest.raises(FamilyAFeatureContractErrorV2, match="mixed"):
        _flow(
            BAR_OPEN,
            trades=(replace(trade, contract_multiplier=Decimal("2")),),
        )


@pytest.fixture(scope="module")
def exact_prior_history() -> tuple[FamilyAPriorBarEvidenceV2, ...]:
    first_open = BAR_OPEN - FAMILY_A_ENTRY_PRIOR_BARS_V2 * FIVE_MINUTE_MS_V2
    rows: list[FamilyAPriorBarEvidenceV2] = []
    for index in range(FAMILY_A_ENTRY_PRIOR_BARS_V2):
        bar_open = first_open + index * FIVE_MINUTE_MS_V2
        bar_close = bar_open + FIVE_MINUTE_MS_V2 - 1
        close = Decimal(10_000 + index) + Decimal(index % 5) / Decimal(10)
        kline = _kline(
            bar_open,
            close=close,
            label=f"history-kline-{index}",
        )
        oi_value = Decimal(50_000 + index) + Decimal(index % 7) / Decimal(10)
        mark_value = Decimal("100") + Decimal(index % 5) / Decimal(100)
        funding = Decimal((index % 7) - 3) / Decimal(1_000_000)
        rows.append(
            build_family_a_prior_bar_evidence_v2(
                kline=kline,
                oi_source_binding=OI_BINDING,
                mark_source_binding=MARK_BINDING,
                oi_responses=(
                    _oi(
                        bar_close,
                        bar_close + 1,
                        value=oi_value,
                        ingest_seq=index,
                        label=f"history-oi-{index}",
                    ),
                ),
                mark_observations=(
                    _mark(
                        bar_close,
                        bar_close + 1,
                        mark=mark_value,
                        funding=funding,
                        ingest_seq=index,
                        label=f"history-mark-{index}",
                    ),
                ),
                as_of_cutoff_ms=D_MS,
            )
        )
    return tuple(rows)


def _build_entry(
    prior: tuple[FamilyAPriorBarEvidenceV2, ...],
    *,
    kline: FamilyAClosedKlineV2 | None = None,
    oi: tuple[FamilyAOIResponseV2, ...] | None = None,
    flow: FamilyAFlowWindowV2 | None = None,
) -> FamilyAEntryFeatureEvidenceV2:
    current_kline = kline or _kline(
        BAR_OPEN,
        close=Decimal("18660"),
        receipt_ms=D_MS,
        label="current-kline",
    )
    return build_family_a_entry_feature_evidence_v2(
        attempt_id=ATTEMPT,
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        bar_open_ms=BAR_OPEN,
        bar_close_ms=BAR_CLOSE,
        decision_cutoff_ms=D_MS,
        current_kline=current_kline,
        current_oi_source_binding=OI_BINDING,
        current_oi_responses=oi
        or (
            _oi(
                BAR_CLOSE,
                D_MS,
                value=Decimal("58650"),
                label="current-oi",
            ),
        ),
        current_flow=flow or _flow(BAR_OPEN),
        prior_bars=prior,
    )


def test_entry_factory_uses_exact_history_and_prior_twelve_references(
    exact_prior_history: tuple[FamilyAPriorBarEvidenceV2, ...],
) -> None:
    evidence = _build_entry(exact_prior_history)
    assert evidence.readiness is FamilyAFeatureReadinessV2.READY
    assert evidence.crowded_long_high == max(value.high for value in exact_prior_history[-12:])
    assert evidence.crowded_short_low == min(value.low for value in exact_prior_history[-12:])
    assert evidence.latest_source_receipt_ms == D_MS


def test_feature_factory_is_independent_of_hostile_ambient_decimal_context(
    exact_prior_history: tuple[FamilyAPriorBarEvidenceV2, ...],
) -> None:
    with localcontext(Context(prec=6, rounding=ROUND_DOWN)):
        low_precision_ambient = _build_entry(exact_prior_history)
    with localcontext(Context(prec=80, rounding=ROUND_UP)):
        high_precision_ambient = _build_entry(exact_prior_history)
    assert low_precision_ambient == high_precision_ambient
    assert low_precision_ambient.evidence_sha256 == high_precision_ambient.evidence_sha256


def test_late_current_record_does_not_rewrite_prior_decision(
    exact_prior_history: tuple[FamilyAPriorBarEvidenceV2, ...],
) -> None:
    on_time = _oi(BAR_CLOSE, D_MS, value=Decimal("58650"), label="on-time")
    late = _oi(
        BAR_CLOSE,
        D_MS + 1,
        value=Decimal("99999"),
        ingest_seq=2,
        label="late",
        binding=_binding(
            FamilyASourceKindV2.OPEN_INTEREST,
            cursor_receipt_ms=D_MS + 1,
        ),
    )
    first = _build_entry(exact_prior_history, oi=(on_time,))
    replay = _build_entry(exact_prior_history, oi=(late, on_time))
    assert replay == first
    assert replay.evidence_sha256 == first.evidence_sha256


def test_stale_current_selector_is_inconclusive_not_complete_no_signal(
    exact_prior_history: tuple[FamilyAPriorBarEvidenceV2, ...],
) -> None:
    evidence = _build_entry(
        exact_prior_history,
        oi=(_oi(BAR_CLOSE - 10_001, D_MS),),
    )
    assert evidence.readiness is FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA
    assert evidence.reasons == ("CURRENT_OI_UNAVAILABLE_10000MS",)


def test_after_d_kline_keeps_actual_receipt_and_is_inconclusive(
    exact_prior_history: tuple[FamilyAPriorBarEvidenceV2, ...],
) -> None:
    evidence = _build_entry(
        exact_prior_history,
        kline=_kline(
            BAR_OPEN,
            close=Decimal("18660"),
            receipt_ms=D_MS + 1,
            label="late-kline",
            binding=_binding(
                FamilyASourceKindV2.KLINE,
                cursor_receipt_ms=D_MS + 1,
            ),
        ),
    )
    assert evidence.readiness is FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA
    assert evidence.latest_source_receipt_ms == D_MS + 1


def test_flow_closure_d_equality_and_d_plus_one_are_explicit(
    exact_prior_history: tuple[FamilyAPriorBarEvidenceV2, ...],
) -> None:
    with pytest.raises(FamilyAFeatureContractErrorV2, match=r"T\+1"):
        _flow(BAR_OPEN, completion_ms=BAR_CLOSE - 1)
    at_d = _build_entry(
        exact_prior_history,
        flow=_flow(BAR_OPEN, completion_ms=D_MS),
    )
    after_d = _build_entry(
        exact_prior_history,
        flow=_flow(
            BAR_OPEN,
            completion_ms=D_MS + 1,
            binding=_binding(
                FamilyASourceKindV2.NORMAL_FUTURES_FLOW,
                cursor_receipt_ms=D_MS + 1,
            ),
        ),
    )
    late_trade = FamilyANormalFlowTradeV2(
        binding=_binding(
            FamilyASourceKindV2.NORMAL_FUTURES_FLOW,
            cursor_receipt_ms=D_MS + 1,
        ),
        trade_id=99,
        transaction_time_ms=BAR_OPEN + 1,
        receipt_ms=D_MS + 1,
        price=Decimal("100"),
        quantity=Decimal("1"),
        normal_quantity=Decimal("1"),
        contract_multiplier=Decimal("1"),
        buyer_maker=False,
        source_evidence_sha256=_sha("late-flow-trade"),
    )
    late_window = _flow(
        BAR_OPEN,
        completion_ms=D_MS,
        trades=(late_trade,),
    )
    late_record = _build_entry(exact_prior_history, flow=late_window)
    assert at_d.readiness is FamilyAFeatureReadinessV2.READY
    assert after_d.readiness is FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW
    assert after_d.latest_source_receipt_ms == D_MS + 1
    assert late_record == at_d
    assert late_record.source_root_sha256 == at_d.source_root_sha256
    assert late_record.evidence_sha256 == at_d.evidence_sha256
    assert late_record.latest_source_receipt_ms == D_MS
    at_d_input = FamilyAEntryInputV2(
        attempt_id=ATTEMPT,
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN,
        bar_open_ms=BAR_OPEN,
        bar_close_ms=BAR_CLOSE,
        decision_cutoff_ms=D_MS,
        feature_evidence=at_d,
    )
    late_input = replace(at_d_input, feature_evidence=late_record)
    first_ledger = FamilyAEpisodeLedgerV2(maximum_events=2)
    late_ledger = FamilyAEpisodeLedgerV2(maximum_events=2)
    first_decision = first_ledger.evaluate_entry(at_d_input)
    late_decision = late_ledger.evaluate_entry(late_input)
    assert canonical_family_a_entry_decision_v2(late_decision) == (
        canonical_family_a_entry_decision_v2(first_decision)
    )
    assert late_ledger.root_sha256 == first_ledger.root_sha256
    conflict = build_family_a_post_cutoff_completeness_conflict_v2(
        late_window,
        decision_cutoff_ms=D_MS,
    )
    assert conflict is not None
    assert conflict.late_trade_count == 1
    assert conflict.first_late_receipt_ms == D_MS + 1


def _exit_prior_history(
    history: tuple[FamilyAPriorBarEvidenceV2, ...],
) -> tuple[FamilyAPriorBarEvidenceV2, ...]:
    exit_bar_open = BAR_OPEN + FIVE_MINUTE_MS_V2
    exit_close = exit_bar_open + FIVE_MINUTE_MS_V2 - 1
    exit_d = exit_close + DECISION_DELAY_MS_V2
    kline_binding = _binding(
        FamilyASourceKindV2.KLINE,
        cursor_receipt_ms=exit_d,
    )
    oi_binding = _binding(
        FamilyASourceKindV2.OPEN_INTEREST,
        cursor_receipt_ms=exit_d,
    )
    mark_binding = _binding(
        FamilyASourceKindV2.MARK_INDEX_PREDICTED_FUNDING,
        cursor_receipt_ms=exit_d,
    )
    current_prior = build_family_a_prior_bar_evidence_v2(
        kline=_kline(
            BAR_OPEN,
            close=Decimal("18660"),
            receipt_ms=D_MS,
            label="exit-prior-kline",
            binding=kline_binding,
        ),
        oi_source_binding=oi_binding,
        mark_source_binding=mark_binding,
        oi_responses=(
            _oi(
                BAR_CLOSE,
                D_MS,
                value=Decimal("58650"),
                label="exit-prior-oi",
                binding=oi_binding,
            ),
        ),
        mark_observations=(
            _mark(
                BAR_CLOSE,
                D_MS,
                mark=Decimal("100.02"),
                label="exit-prior-mark",
                binding=mark_binding,
            ),
        ),
        as_of_cutoff_ms=exit_d,
    )
    historical = tuple(
        build_family_a_prior_bar_evidence_v2(
            kline=replace(value.kline, binding=kline_binding),
            oi_source_binding=oi_binding,
            mark_source_binding=mark_binding,
            oi_responses=(replace(value.selected_oi, binding=oi_binding),),
            mark_observations=(replace(value.selected_mark, binding=mark_binding),),
            as_of_cutoff_ms=exit_d,
        )
        for value in history[-(FAMILY_A_EXIT_PRIOR_BARS_V2 - 1) :]
    )
    return (*historical, current_prior)


def test_exit_factory_is_exact_and_incomplete_flow_is_explicit(
    exact_prior_history: tuple[FamilyAPriorBarEvidenceV2, ...],
) -> None:
    exit_open = BAR_OPEN + FIVE_MINUTE_MS_V2
    exit_close = exit_open + FIVE_MINUTE_MS_V2 - 1
    exit_d = exit_close + DECISION_DELAY_MS_V2
    kline_binding = _binding(
        FamilyASourceKindV2.KLINE,
        cursor_receipt_ms=exit_d,
    )
    mark_binding = _binding(
        FamilyASourceKindV2.MARK_INDEX_PREDICTED_FUNDING,
        cursor_receipt_ms=exit_d,
    )
    flow_binding = _binding(
        FamilyASourceKindV2.NORMAL_FUTURES_FLOW,
        cursor_receipt_ms=exit_d,
    )
    prior = _exit_prior_history(exact_prior_history)
    common = {
        "attempt_id": ATTEMPT,
        "symbol": SYMBOL,
        "venue": VenueV2.USDM_FUTURES,
        "promoting_plan_sha256": PLAN,
        "bar_open_ms": exit_open,
        "bar_close_ms": exit_close,
        "decision_cutoff_ms": exit_d,
        "current_kline": _kline(
            exit_open,
            close=Decimal("18661"),
            receipt_ms=exit_d,
            label="exit-kline",
            binding=kline_binding,
        ),
        "current_mark_source_binding": mark_binding,
        "current_mark_observations": (
            _mark(
                exit_close,
                exit_d,
                mark=Decimal("100.03"),
                label="exit-mark",
                binding=mark_binding,
            ),
        ),
        "prior_basis_bars": prior,
        "previous_flow": _flow(BAR_OPEN, binding=flow_binding),
    }
    ready = build_family_a_exit_feature_evidence_v2(
        **common,
        current_flow=_flow(exit_open, binding=flow_binding),
    )
    incomplete = build_family_a_exit_feature_evidence_v2(
        **common,
        current_flow=_flow(exit_open, complete=False, binding=flow_binding),
    )
    assert ready.readiness is FamilyAFeatureReadinessV2.READY
    assert ready.close_price == Decimal("18661")
    assert incomplete.readiness is FamilyAFeatureReadinessV2.INCONCLUSIVE_FLOW
    assert incomplete.close_price == ready.close_price
    assert incomplete.flow_current is None


def test_entry_evidence_direct_constructor_is_rejected() -> None:
    with pytest.raises(FamilyAFeatureContractErrorV2, match="causal factory"):
        FamilyAEntryFeatureEvidenceV2(
            attempt_id=ATTEMPT,
            symbol=SYMBOL,
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN,
            bar_open_ms=BAR_OPEN,
            bar_close_ms=BAR_CLOSE,
            decision_cutoff_ms=D_MS,
            source_root_sha256=_sha("root"),
            latest_source_event_ms=BAR_CLOSE,
            latest_source_receipt_ms=D_MS,
            readiness=FamilyAFeatureReadinessV2.INCONCLUSIVE_DATA,
            reasons=("NO_DIRECT_CONSTRUCTION",),
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
