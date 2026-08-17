from __future__ import annotations

import hashlib
from dataclasses import replace
from decimal import ROUND_DOWN, Decimal, getcontext, localcontext, setcontext

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.family_b_features import (
    FamilyBBookLevelV2,
    FamilyBBookSideV2,
    FamilyBBookSourceV2,
    FamilyBBookStateV2,
    FamilyBFeatureContractErrorV2,
    FamilyBFeatureReadinessV2,
    FamilyBFeatureSourceLineageV2,
    FamilyBFlowWindowClosureV2,
    FamilyBKlineBarV2,
    FamilyBNormalFlowTradeV2,
    FamilyBPriorBarFeaturesV2,
    FamilyBWeightedObservationV2,
    build_family_b_feature_evidence_v2,
    canonical_family_b_feature_evidence_v2,
    duration_weighted_quantile_v2,
)

BAR_OPEN_MS = 2_000_160_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + 300_000 - 1
D_MS = BAR_CLOSE_MS + 2_001
PLAN_SHA = "a" * 64
SYMBOL = "BTCUSDT"


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _lineage(*, normal_flow_schema: str | None = None) -> FamilyBFeatureSourceLineageV2:
    return FamilyBFeatureSourceLineageV2(
        book_capture_root_sha256=_sha("book-root"),
        normal_flow_capture_root_sha256=_sha("flow-root"),
        kline_capture_root_sha256=_sha("kline-root"),
        prior_feature_capture_root_sha256=_sha("prior-root"),
        book_schema_sha256=_sha("book-schema"),
        normal_flow_nq_schema_sha256=(
            normal_flow_schema or _sha("normal-flow-nq-schema")
        ),
        kline_schema_sha256=_sha("kline-schema"),
        prior_feature_schema_sha256=_sha("prior-schema"),
    )


def _levels(
    *,
    mid: Decimal,
    bid_depth: Decimal,
    ask_depth: Decimal,
    spread_bps: Decimal = Decimal(10),
    complete: bool = True,
) -> tuple[FamilyBBookLevelV2, ...]:
    with localcontext(protocol_decimal_context_v2()):
        half_spread = spread_bps / Decimal(20_000)
        bid = mid * (Decimal(1) - half_spread)
        ask = mid * (Decimal(1) + half_spread)
        bid_quantity = bid_depth / bid
        ask_quantity = ask_depth / ask
        bid_proof = bid * (Decimal("0.9989") if complete else Decimal("0.9995"))
        ask_proof = ask * (Decimal("1.0011") if complete else Decimal("1.0005"))
    return (
        FamilyBBookLevelV2(FamilyBBookSideV2.BID, bid, bid_quantity),
        FamilyBBookLevelV2(
            FamilyBBookSideV2.BID,
            bid_proof,
            Decimal("0.000000000001"),
        ),
        FamilyBBookLevelV2(FamilyBBookSideV2.ASK, ask, ask_quantity),
        FamilyBBookLevelV2(
            FamilyBBookSideV2.ASK,
            ask_proof,
            Decimal("0.000000000001"),
        ),
    )


def _state(
    *,
    offset_ms: int,
    update_id: int,
    previous_update_id: int | None,
    bid_depth: Decimal,
    ask_depth: Decimal,
    mid: Decimal,
    symbol: str = SYMBOL,
    source: FamilyBBookSourceV2 = FamilyBBookSourceV2.STANDARD_DIFF_DEPTH,
    complete: bool = True,
    receipt_ms: int | None = None,
    source_evidence_sha256: str | None = None,
) -> FamilyBBookStateV2:
    lineage = _lineage()
    transaction_time_ms = BAR_OPEN_MS + offset_ms
    return FamilyBBookStateV2.create(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        capture_root_sha256=lineage.book_capture_root_sha256,
        schema_sha256=lineage.book_schema_sha256,
        source=source,
        transaction_time_ms=transaction_time_ms,
        receipt_ms=(
            min(BAR_OPEN_MS + max(offset_ms, 0) + 1, D_MS)
            if receipt_ms is None
            else receipt_ms
        ),
        first_update_id=update_id,
        final_update_id=update_id,
        previous_final_update_id=previous_update_id,
        levels=_levels(
            mid=mid,
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            complete=complete,
        ),
        source_evidence_sha256=(
            source_evidence_sha256 or _sha(f"book-{update_id}")
        ),
    )


def _states() -> tuple[FamilyBBookStateV2, ...]:
    return (
        _state(
            offset_ms=0,
            update_id=100,
            previous_update_id=None,
            bid_depth=Decimal(10),
            ask_depth=Decimal(100),
            mid=Decimal(100),
        ),
        _state(
            offset_ms=30_000,
            update_id=101,
            previous_update_id=100,
            bid_depth=Decimal(20),
            ask_depth=Decimal(50),
            mid=Decimal(100),
        ),
        _state(
            offset_ms=270_000,
            update_id=102,
            previous_update_id=101,
            bid_depth=Decimal(30),
            ask_depth=Decimal(60),
            mid=Decimal(101),
        ),
    )


def _trade(
    *,
    trade_id: int,
    quantity: Decimal,
    normal_quantity: Decimal,
    buyer_maker: bool,
    receipt_ms: int | None = None,
    symbol: str = SYMBOL,
) -> FamilyBNormalFlowTradeV2:
    lineage = _lineage()
    transaction_time_ms = BAR_OPEN_MS + trade_id * 1_000
    return FamilyBNormalFlowTradeV2.create(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        capture_root_sha256=lineage.normal_flow_capture_root_sha256,
        schema_sha256=lineage.normal_flow_nq_schema_sha256,
        trade_id=trade_id,
        transaction_time_ms=transaction_time_ms,
        receipt_ms=(transaction_time_ms + 1 if receipt_ms is None else receipt_ms),
        price=Decimal(1),
        quantity=quantity,
        normal_quantity=normal_quantity,
        contract_multiplier=Decimal(1),
        buyer_maker=buyer_maker,
        source_evidence_sha256=_sha(f"trade-{trade_id}"),
    )


def _trades(sign: int = 1) -> tuple[FamilyBNormalFlowTradeV2, ...]:
    if sign == 0:
        return ()
    buy, sell = (Decimal(8), Decimal(2)) if sign > 0 else (Decimal(2), Decimal(8))
    return (
        _trade(
            trade_id=1,
            quantity=buy,
            normal_quantity=buy,
            buyer_maker=False,
        ),
        _trade(
            trade_id=2,
            quantity=sell,
            normal_quantity=sell,
            buyer_maker=True,
        ),
    )


def _flow_closure(
    *,
    receipt_ms: int = D_MS,
    complete_through_event_ms: int = BAR_CLOSE_MS,
    closure_event_ms: int = BAR_CLOSE_MS + 1,
) -> FamilyBFlowWindowClosureV2:
    lineage = _lineage()
    return FamilyBFlowWindowClosureV2.create(
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        capture_root_sha256=lineage.normal_flow_capture_root_sha256,
        schema_sha256=lineage.normal_flow_nq_schema_sha256,
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        complete_through_event_ms=complete_through_event_ms,
        closure_event_ms=closure_event_ms,
        closure_receipt_ms=receipt_ms,
        source_evidence_sha256=_sha("flow-closure"),
    )


def _kline(
    *,
    event_ms: int = BAR_CLOSE_MS,
    receipt_ms: int = D_MS,
    interval_ms: int = 300_000,
    closed: bool = True,
    symbol: str = SYMBOL,
) -> FamilyBKlineBarV2:
    lineage = _lineage()
    return FamilyBKlineBarV2.create(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        capture_root_sha256=lineage.kline_capture_root_sha256,
        schema_sha256=lineage.kline_schema_sha256,
        interval_ms=interval_ms,
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        open_event_id="kline-open-1",
        close_event_id="kline-close-1",
        closed=closed,
        event_ms=event_ms,
        receipt_ms=receipt_ms,
        high=Decimal(105),
        low=Decimal(95),
        close=Decimal(101),
        previous_close=Decimal(100),
        source_evidence_sha256=_sha("kline-event"),
    )


def _prior(
    *,
    receipt_ms: int,
    event_ms: int | None = None,
) -> FamilyBPriorBarFeaturesV2:
    lineage = _lineage()
    prior_open = BAR_OPEN_MS - 300_000
    return FamilyBPriorBarFeaturesV2.create(
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        capture_root_sha256=lineage.prior_feature_capture_root_sha256,
        schema_sha256=lineage.prior_feature_schema_sha256,
        bar_open_ms=prior_open,
        flow_imbalance=Decimal("0.1"),
        bar_return=Decimal("0.01"),
        latest_source_event_ms=(
            prior_open + 300_000 - 1 if event_ms is None else event_ms
        ),
        latest_source_receipt_ms=receipt_ms,
        source_evidence_sha256=_sha("prior-1"),
    )


def _build(
    *,
    venue: VenueV2 = VenueV2.USDM_FUTURES,
    states: tuple[FamilyBBookStateV2, ...] | None = None,
    trades: tuple[FamilyBNormalFlowTradeV2, ...] | None = None,
    prior: tuple[FamilyBPriorBarFeaturesV2, ...] = (),
    lineage: FamilyBFeatureSourceLineageV2 | None = None,
    closure: FamilyBFlowWindowClosureV2 | None = None,
    kline: FamilyBKlineBarV2 | None = None,
):
    return build_family_b_feature_evidence_v2(
        attempt_id="attempt-1",
        symbol=SYMBOL,
        venue=venue,
        promoting_plan_sha256=PLAN_SHA,
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=D_MS,
        source_lineage=lineage or _lineage(),
        book_states=_states() if states is None else states,
        normal_flow_trades=_trades() if trades is None else trades,
        prior_features=prior,
        flow_window_closure=closure or _flow_closure(),
        kline_bar=kline or _kline(),
    )


def test_weighted_quantile_exact_cdf_equality_and_permutation() -> None:
    observations = (
        FamilyBWeightedObservationV2(Decimal(1), 1),
        FamilyBWeightedObservationV2(Decimal(2), 1),
    )
    assert duration_weighted_quantile_v2(observations, Decimal("0.50")) == 1
    assert duration_weighted_quantile_v2(observations, Decimal("0.5001")) == 2
    assert duration_weighted_quantile_v2(tuple(reversed(observations)), Decimal("0.50")) == 1


def test_builder_derives_depth_spread_and_windows_only_from_raw_levels() -> None:
    evidence = _build()
    assert evidence.readiness is FamilyBFeatureReadinessV2.FEATURE_NOT_READY_WARMUP
    assert evidence.flow_imbalance_current == Decimal("0.6")
    assert evidence.d_start == Decimal(100)
    assert evidence.d_low == Decimal(50)
    assert evidence.d_end == Decimal(60)
    assert evidence.spread95_bps == Decimal(10)
    with localcontext(protocol_decimal_context_v2()):
        expected_return = Decimal("1.01").ln()
    assert evidence.bar_return_current == expected_return
    assert evidence.latest_source_event_ms == BAR_CLOSE_MS + 1
    assert evidence.latest_source_receipt_ms == D_MS
    payload = canonical_family_b_feature_evidence_v2(evidence)
    assert evidence.feature_evidence_sha256.encode() in payload
    assert b'"normal_flow_nq_schema_sha256"' in payload


def test_negative_and_zero_normal_flow_select_bid_or_no_opposing_depth() -> None:
    negative = _build(trades=_trades(-1))
    zero = _build(trades=())
    assert negative.flow_imbalance_current == Decimal("-0.6")
    assert negative.d_start == Decimal("9.999999999999999999999999999999999")
    assert negative.d_low == Decimal("9.999999999999999999999999999999999")
    assert negative.d_end == Decimal(30)
    assert zero.flow_imbalance_current == 0
    assert zero.d_start == 0
    assert zero.readiness is FamilyBFeatureReadinessV2.FEATURE_NOT_READY_DEPTH


def test_feature_evidence_is_independent_of_hostile_ambient_decimal_context() -> None:
    baseline = _build()
    original = getcontext().copy()
    try:
        getcontext().prec = 6
        getcontext().rounding = ROUND_DOWN
        hostile = _build()
    finally:
        setcontext(original)

    assert hostile == baseline


def test_normal_quantity_zero_never_falls_back_to_total_quantity() -> None:
    q_only = (
        _trade(
            trade_id=1,
            quantity=Decimal(8),
            normal_quantity=Decimal(0),
            buyer_maker=False,
        ),
        _trade(
            trade_id=2,
            quantity=Decimal(2),
            normal_quantity=Decimal(0),
            buyer_maker=True,
        ),
    )
    assert _build(trades=q_only).flow_imbalance_current == 0


def test_half_open_five_second_boundaries_are_exact() -> None:
    states = (
        _state(
            offset_ms=0,
            update_id=200,
            previous_update_id=None,
            bid_depth=Decimal(50),
            ask_depth=Decimal(50),
            mid=Decimal(100),
        ),
        _state(
            offset_ms=5_000,
            update_id=201,
            previous_update_id=200,
            bid_depth=Decimal(50),
            ask_depth=Decimal(50),
            mid=Decimal(200),
        ),
        _state(
            offset_ms=295_000,
            update_id=202,
            previous_update_id=201,
            bid_depth=Decimal(50),
            ask_depth=Decimal(50),
            mid=Decimal(300),
        ),
    )
    with localcontext(protocol_decimal_context_v2()):
        expected = Decimal(3).ln()
    assert _build(states=states).bar_return_current == expected


def test_input_permutation_has_identical_feature_payload_and_root() -> None:
    first = _build()
    permuted = _build(
        states=tuple(reversed(_states())),
        trades=tuple(reversed(_trades())),
    )
    assert first == permuted
    assert canonical_family_b_feature_evidence_v2(
        first
    ) == canonical_family_b_feature_evidence_v2(permuted)


def test_publication_and_receipt_boundaries_preserve_post_close_exchange_time() -> None:
    after_close = _build(
        kline=_kline(event_ms=BAR_CLOSE_MS + 1, receipt_ms=D_MS)
    )
    at_cutoff = _build(kline=_kline(event_ms=D_MS, receipt_ms=D_MS))
    closure_at_cutoff = _build(
        closure=_flow_closure(closure_event_ms=D_MS, receipt_ms=D_MS)
    )

    assert after_close.kline_event_ms == BAR_CLOSE_MS + 1
    assert at_cutoff.kline_event_ms == D_MS
    assert closure_at_cutoff.latest_source_event_ms == D_MS
    with pytest.raises(FamilyBFeatureContractErrorV2, match="after D"):
        _build(kline=_kline(receipt_ms=D_MS + 1))
    with pytest.raises(FamilyBFeatureContractErrorV2, match=r"cannot precede k\.T"):
        _build(kline=_kline(event_ms=BAR_CLOSE_MS - 1))
    with pytest.raises(FamilyBFeatureContractErrorV2, match="follow its local receipt"):
        _build(kline=_kline(event_ms=D_MS, receipt_ms=D_MS - 1))
    with pytest.raises(FamilyBFeatureContractErrorV2, match="after D"):
        _build(closure=_flow_closure(receipt_ms=D_MS + 1))
    with pytest.raises(FamilyBFeatureContractErrorV2, match="after D"):
        _build(
            closure=_flow_closure(
                closure_event_ms=D_MS + 1,
                receipt_ms=D_MS + 1,
            )
        )
    late_state = FamilyBBookStateV2.create(
        symbol=SYMBOL,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        capture_root_sha256=_lineage().book_capture_root_sha256,
        schema_sha256=_lineage().book_schema_sha256,
        source=FamilyBBookSourceV2.STANDARD_DIFF_DEPTH,
        transaction_time_ms=_states()[-1].transaction_time_ms,
        receipt_ms=D_MS + 1,
        first_update_id=102,
        final_update_id=102,
        previous_final_update_id=101,
        levels=_states()[-1].levels,
        source_evidence_sha256=_sha("late-state"),
    )
    with pytest.raises(FamilyBFeatureContractErrorV2, match="received after D"):
        _build(states=(*_states()[:-1], late_state))
    with pytest.raises(FamilyBFeatureContractErrorV2, match="received after D"):
        _build(
            trades=(
                _trade(
                    trade_id=1,
                    quantity=Decimal(8),
                    normal_quantity=Decimal(8),
                    buyer_maker=False,
                    receipt_ms=D_MS + 1,
                ),
                _trades()[1],
            )
        )


def test_prior_close_and_local_receipt_one_ms_boundaries() -> None:
    book_at_receipt = _state(
        offset_ms=0,
        update_id=100,
        previous_update_id=None,
        bid_depth=Decimal(10),
        ask_depth=Decimal(100),
        mid=Decimal(100),
        receipt_ms=BAR_OPEN_MS,
    )
    assert _build(states=(book_at_receipt, *_states()[1:]))
    with pytest.raises(
        FamilyBFeatureContractErrorV2,
        match="book transaction time cannot follow",
    ):
        _state(
            offset_ms=0,
            update_id=100,
            previous_update_id=None,
            bid_depth=Decimal(10),
            ask_depth=Decimal(100),
            mid=Decimal(100),
            receipt_ms=BAR_OPEN_MS - 1,
        )

    trade_time_ms = BAR_OPEN_MS + 1_000
    trade_at_receipt = _trade(
        trade_id=1,
        quantity=Decimal(8),
        normal_quantity=Decimal(8),
        buyer_maker=False,
        receipt_ms=trade_time_ms,
    )
    assert _build(trades=(trade_at_receipt, _trades()[1]))
    with pytest.raises(
        FamilyBFeatureContractErrorV2,
        match="normal-flow transaction time cannot follow",
    ):
        _trade(
            trade_id=1,
            quantity=Decimal(8),
            normal_quantity=Decimal(8),
            buyer_maker=False,
            receipt_ms=trade_time_ms - 1,
        )

    prior_close_ms = BAR_OPEN_MS - 1
    prior_at_close = _prior(receipt_ms=D_MS, event_ms=prior_close_ms)
    assert _build(prior=(prior_at_close,))
    with pytest.raises(
        FamilyBFeatureContractErrorV2,
        match="prior feature source event cannot precede",
    ):
        _prior(receipt_ms=D_MS, event_ms=prior_close_ms - 1)


def test_sequence_initial_coverage_raw_band_and_rpi_fail_closed() -> None:
    gap = _state(
        offset_ms=30_000,
        update_id=101,
        previous_update_id=99,
        bid_depth=Decimal(20),
        ask_depth=Decimal(50),
        mid=Decimal(100),
    )
    with pytest.raises(FamilyBFeatureContractErrorV2, match=r"next\.pu"):
        _build(states=(_states()[0], gap, _states()[2]))
    no_initial = _state(
        offset_ms=1,
        update_id=100,
        previous_update_id=None,
        bid_depth=Decimal(10),
        ask_depth=Decimal(100),
        mid=Decimal(100),
    )
    with pytest.raises(FamilyBFeatureContractErrorV2, match="exact-open"):
        _build(states=(no_initial, *_states()[1:]))
    incomplete = _state(
        offset_ms=30_000,
        update_id=101,
        previous_update_id=100,
        bid_depth=Decimal(20),
        ask_depth=Decimal(50),
        mid=Decimal(100),
        complete=False,
    )
    with pytest.raises(FamilyBFeatureContractErrorV2, match="complete 10bp"):
        _build(states=(_states()[0], incomplete, _states()[2]))
    rpi = _state(
        offset_ms=30_000,
        update_id=101,
        previous_update_id=100,
        bid_depth=Decimal(20),
        ask_depth=Decimal(50),
        mid=Decimal(100),
        source=FamilyBBookSourceV2.RPI,
    )
    with pytest.raises(FamilyBFeatureContractErrorV2, match="RPI"):
        _build(states=(_states()[0], rpi, _states()[2]))


def test_same_source_hash_scalar_tamper_is_sealed_and_new_raw_content_rehashes() -> None:
    original = _states()[1]
    with pytest.raises(FamilyBFeatureContractErrorV2, match="raw-row factory"):
        replace(
            original,
            levels=_levels(
                mid=Decimal(100),
                bid_depth=Decimal(20),
                ask_depth=Decimal(70),
            ),
        )
    changed = _state(
        offset_ms=30_000,
        update_id=101,
        previous_update_id=100,
        bid_depth=Decimal(20),
        ask_depth=Decimal(70),
        mid=Decimal(100),
        source_evidence_sha256=original.source_evidence_sha256,
    )
    assert changed.source_evidence_sha256 == original.source_evidence_sha256
    assert changed.row_payload_sha256 != original.row_payload_sha256
    changed_evidence = _build(states=(_states()[0], changed, _states()[2]))
    assert changed_evidence.feature_source_root_sha256 != _build().feature_source_root_sha256


def test_cross_symbol_rows_and_kline_membership_are_rejected() -> None:
    cross_state = _state(
        offset_ms=30_000,
        update_id=101,
        previous_update_id=100,
        bid_depth=Decimal(20),
        ask_depth=Decimal(50),
        mid=Decimal(100),
        symbol="ETHUSDT",
    )
    with pytest.raises(FamilyBFeatureContractErrorV2, match="verified member"):
        _build(states=(_states()[0], cross_state, _states()[2]))
    with pytest.raises(FamilyBFeatureContractErrorV2, match="verified member"):
        _build(kline=_kline(symbol="ETHUSDT"))


def test_intrabar_and_wrong_interval_kline_are_rejected() -> None:
    with pytest.raises(FamilyBFeatureContractErrorV2, match="intrabar"):
        _build(kline=_kline(closed=False))
    with pytest.raises(FamilyBFeatureContractErrorV2, match="exact 5m"):
        _build(kline=_kline(interval_ms=60_000))


def test_late_prior_receipt_uses_current_decision_cutoff() -> None:
    prior_own_cutoff = BAR_OPEN_MS - 1 + 2_001
    late_but_known = _prior(receipt_ms=D_MS)
    assert late_but_known.latest_source_receipt_ms > prior_own_cutoff
    assert _build(prior=(late_but_known,)).latest_source_receipt_ms == D_MS
    with pytest.raises(FamilyBFeatureContractErrorV2, match="current D"):
        _build(prior=(_prior(receipt_ms=D_MS + 1),))


def test_exact_bar_open_update_replaces_carried_pre_open_state() -> None:
    carried = _state(
        offset_ms=-1,
        update_id=99,
        previous_update_id=None,
        bid_depth=Decimal(1),
        ask_depth=Decimal(999),
        mid=Decimal(90),
    )
    at_open = _state(
        offset_ms=0,
        update_id=100,
        previous_update_id=99,
        bid_depth=Decimal(10),
        ask_depth=Decimal(100),
        mid=Decimal(100),
    )
    evidence = _build(states=(carried, at_open, *_states()[1:]))
    assert evidence.d_start == Decimal(100)
    with localcontext(protocol_decimal_context_v2()):
        assert evidence.bar_return_current == Decimal("1.01").ln()


def test_nq_schema_flow_closure_and_venue_fail_closed() -> None:
    with pytest.raises(FamilyBFeatureContractErrorV2, match="nq cannot exceed q"):
        _trade(
            trade_id=1,
            quantity=Decimal(1),
            normal_quantity=Decimal(2),
            buyer_maker=False,
        )
    with pytest.raises(FamilyBFeatureContractErrorV2, match=r"exact k\.T"):
        _build(
            closure=_flow_closure(complete_through_event_ms=BAR_CLOSE_MS - 1)
        )
    with pytest.raises(FamilyBFeatureContractErrorV2, match=r"exact k\.T"):
        _flow_closure(complete_through_event_ms=BAR_CLOSE_MS + 1)
    with pytest.raises(FamilyBFeatureContractErrorV2, match=r"k\.T\+1"):
        _flow_closure(closure_event_ms=BAR_CLOSE_MS)
    with pytest.raises(FamilyBFeatureContractErrorV2, match="follow its local receipt"):
        _flow_closure(
            closure_event_ms=BAR_CLOSE_MS + 2,
            receipt_ms=BAR_CLOSE_MS + 1,
        )
    with pytest.raises(FamilyBFeatureContractErrorV2, match="USD-M Futures"):
        _build(venue=VenueV2.SPOT)


def test_schema_and_factory_seal_are_bound_into_evidence() -> None:
    original = _build()
    with pytest.raises(FamilyBFeatureContractErrorV2, match="causal factory"):
        replace(original, d_start=Decimal(999))
    changed_lineage = _lineage(normal_flow_schema=_sha("changed-nq-schema"))
    with pytest.raises(FamilyBFeatureContractErrorV2, match="verified member"):
        _build(lineage=changed_lineage)
