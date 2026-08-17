from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, localcontext
from functools import lru_cache
from typing import cast

import pytest

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    PaperFokDecisionRegistryV2,
    PaperFokEntryDecisionV2,
    PaperFokFullFillCertificateV2,
    PaperFokSideV2,
)
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.family_b import (
    DECISION_DELAY_MS_V2,
    FAMILY_B_HARD_HORIZON_BARS_V2,
    FAMILY_B_RULE_VERSION_V2,
    FIVE_MINUTE_MS_V2,
    FamilyBChildV2,
    FamilyBContractError,
    FamilyBDecisionRegistryV2,
    FamilyBEntryCommitDispositionV2,
    FamilyBEntryCommitReceiptV2,
    FamilyBEntryDecisionV2,
    FamilyBEntryInputV2,
    FamilyBEntryPreviewV2,
    FamilyBEntryStatusV2,
    FamilyBExitActionV2,
    FamilyBExitDecisionV2,
    FamilyBExitInputV2,
    FamilyBExitReasonV2,
    FamilyBMandatoryExitV2,
    FamilyBPositionV2,
    FamilyBSideV2,
    canonical_family_b_entry_decision_v2,
    canonical_family_b_exit_decision_v2,
    evaluate_family_b_entry_v2,
    evaluate_family_b_exit_v2,
    event_true_range_v2,
    parse_canonical_family_b_entry_decision_v2,
    position_from_family_b_signal_v2,
    resolve_family_b_child_matches_v2,
)
from signalbot.r4b_v2.strategy.family_b_features import (
    FamilyBBookLevelV2,
    FamilyBBookSideV2,
    FamilyBBookSourceV2,
    FamilyBBookStateV2,
    FamilyBExitSourceLineageV2,
    FamilyBFeatureContractErrorV2,
    FamilyBFeatureSourceLineageV2,
    FamilyBFlowWindowClosureV2,
    FamilyBKlineBarV2,
    FamilyBNormalFlowTradeV2,
    FamilyBPriorBarFeaturesV2,
    build_family_b_exit_feature_evidence_v2,
    build_family_b_feature_evidence_v2,
)

from ..execution.paper_fok_testkit import (
    build_usdm_paper_full_fill_v2,
)

BAR_OPEN_MS = 2_000_160_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
D_MS = BAR_CLOSE_MS + DECISION_DELAY_MS_V2
PLAN_SHA = "a" * 64
EPSILON = Decimal("0.0001")


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _lineage() -> FamilyBFeatureSourceLineageV2:
    return FamilyBFeatureSourceLineageV2(
        book_capture_root_sha256=_sha("book-root"),
        normal_flow_capture_root_sha256=_sha("flow-root"),
        kline_capture_root_sha256=_sha("kline-root"),
        prior_feature_capture_root_sha256=_sha("prior-root"),
        book_schema_sha256=_sha("book-schema"),
        normal_flow_nq_schema_sha256=_sha("normal-flow-nq-schema"),
        kline_schema_sha256=_sha("kline-schema"),
        prior_feature_schema_sha256=_sha("prior-schema"),
    )


def _exit_lineage() -> FamilyBExitSourceLineageV2:
    lineage = _lineage()
    return FamilyBExitSourceLineageV2(
        normal_flow_capture_root_sha256=lineage.normal_flow_capture_root_sha256,
        kline_capture_root_sha256=lineage.kline_capture_root_sha256,
        normal_flow_nq_schema_sha256=lineage.normal_flow_nq_schema_sha256,
        kline_schema_sha256=lineage.kline_schema_sha256,
    )


def _levels(
    *,
    mid: Decimal,
    depth: Decimal,
    spread_bps: Decimal,
) -> tuple[FamilyBBookLevelV2, ...]:
    with localcontext(protocol_decimal_context_v2()):
        half_spread = spread_bps / Decimal(20_000)
        bid = mid * (Decimal(1) - half_spread)
        ask = mid * (Decimal(1) + half_spread)
        bid_quantity = depth / bid
        ask_quantity = depth / ask
    return (
        FamilyBBookLevelV2(FamilyBBookSideV2.BID, bid, bid_quantity),
        FamilyBBookLevelV2(
            FamilyBBookSideV2.BID,
            bid * Decimal("0.9989"),
            Decimal("0.000000000001"),
        ),
        FamilyBBookLevelV2(FamilyBBookSideV2.ASK, ask, ask_quantity),
        FamilyBBookLevelV2(
            FamilyBBookSideV2.ASK,
            ask * Decimal("1.0011"),
            Decimal("0.000000000001"),
        ),
    )


def _state(
    *,
    offset_ms: int,
    update_id: int,
    previous_update_id: int | None,
    mid: Decimal,
    depth: Decimal,
    spread_bps: Decimal,
) -> FamilyBBookStateV2:
    lineage = _lineage()
    return FamilyBBookStateV2.create(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        capture_root_sha256=lineage.book_capture_root_sha256,
        schema_sha256=lineage.book_schema_sha256,
        source=FamilyBBookSourceV2.STANDARD_DIFF_DEPTH,
        transaction_time_ms=BAR_OPEN_MS + offset_ms,
        receipt_ms=BAR_OPEN_MS + max(offset_ms, 0) + 1,
        first_update_id=update_id,
        final_update_id=update_id,
        previous_final_update_id=previous_update_id,
        levels=_levels(mid=mid, depth=depth, spread_bps=spread_bps),
        source_evidence_sha256=_sha(f"book-{update_id}"),
    )


def _book_states(
    *,
    d_start: Decimal,
    d_low: Decimal,
    d_end: Decimal,
    spread_bps: Decimal,
) -> tuple[FamilyBBookStateV2, ...]:
    return (
        _state(
            offset_ms=0,
            update_id=100,
            previous_update_id=None,
            mid=Decimal("100"),
            depth=d_start,
            spread_bps=spread_bps,
        ),
        _state(
            offset_ms=30_000,
            update_id=101,
            previous_update_id=100,
            mid=Decimal("100"),
            depth=d_low,
            spread_bps=spread_bps,
        ),
        _state(
            offset_ms=270_000,
            update_id=102,
            previous_update_id=101,
            mid=Decimal("100"),
            depth=d_end,
            spread_bps=spread_bps,
        ),
    )


def _flow_trades(flow_sign: int) -> tuple[FamilyBNormalFlowTradeV2, ...]:
    if flow_sign == 0:
        return ()
    buy_nq, sell_nq = (Decimal(8), Decimal(2)) if flow_sign > 0 else (Decimal(2), Decimal(8))
    lineage = _lineage()
    return (
        FamilyBNormalFlowTradeV2.create(
            symbol="BTCUSDT",
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN_SHA,
            capture_root_sha256=lineage.normal_flow_capture_root_sha256,
            schema_sha256=lineage.normal_flow_nq_schema_sha256,
            trade_id=1,
            transaction_time_ms=BAR_OPEN_MS + 1_000,
            receipt_ms=BAR_OPEN_MS + 1_001,
            price=Decimal(1),
            quantity=buy_nq,
            normal_quantity=buy_nq,
            contract_multiplier=Decimal(1),
            buyer_maker=False,
            source_evidence_sha256=_sha("buy-trade"),
        ),
        FamilyBNormalFlowTradeV2.create(
            symbol="BTCUSDT",
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN_SHA,
            capture_root_sha256=lineage.normal_flow_capture_root_sha256,
            schema_sha256=lineage.normal_flow_nq_schema_sha256,
            trade_id=2,
            transaction_time_ms=BAR_OPEN_MS + 2_000,
            receipt_ms=BAR_OPEN_MS + 2_001,
            price=Decimal(1),
            quantity=sell_nq,
            normal_quantity=sell_nq,
            contract_multiplier=Decimal(1),
            buyer_maker=True,
            source_evidence_sha256=_sha("sell-trade"),
        ),
    )


def _prior_features(
    *,
    current_flow: Decimal,
    current_return: Decimal,
    flow_z: Decimal,
    return_z: Decimal,
    count: int,
) -> tuple[FamilyBPriorBarFeaturesV2, ...]:
    with localcontext(protocol_decimal_context_v2()):
        flow_mad = Decimal("0.1")
        return_mad = Decimal("0.01")
        flow_location = current_flow - flow_z * Decimal("1.4826") * flow_mad
        return_location = current_return - return_z * Decimal("1.4826") * return_mad
        flow_low = flow_location - flow_mad
        flow_high = flow_location + flow_mad
        return_low = return_location - return_mad
        return_high = return_location + return_mad
    start = BAR_OPEN_MS - count * FIVE_MINUTE_MS_V2
    lineage = _lineage()
    return tuple(
        FamilyBPriorBarFeaturesV2.create(
            symbol="BTCUSDT",
            venue=VenueV2.USDM_FUTURES,
            promoting_plan_sha256=PLAN_SHA,
            capture_root_sha256=lineage.prior_feature_capture_root_sha256,
            schema_sha256=lineage.prior_feature_schema_sha256,
            bar_open_ms=start + index * FIVE_MINUTE_MS_V2,
            flow_imbalance=flow_high if index % 2 else flow_low,
            bar_return=return_high if index % 2 else return_low,
            latest_source_event_ms=(start + index * FIVE_MINUTE_MS_V2 + FIVE_MINUTE_MS_V2 - 1),
            latest_source_receipt_ms=(
                start + index * FIVE_MINUTE_MS_V2 + FIVE_MINUTE_MS_V2 - 1 + DECISION_DELAY_MS_V2
            ),
            source_evidence_sha256=_sha(f"prior-{index}"),
        )
        for index in range(count)
    )


def _flow_closure(
    *,
    bar_open_ms: int,
    decision_cutoff_ms: int,
) -> FamilyBFlowWindowClosureV2:
    lineage = _lineage()
    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    return FamilyBFlowWindowClosureV2.create(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        capture_root_sha256=lineage.normal_flow_capture_root_sha256,
        schema_sha256=lineage.normal_flow_nq_schema_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        complete_through_event_ms=bar_close_ms,
        closure_event_ms=bar_close_ms + 1,
        closure_receipt_ms=decision_cutoff_ms,
        source_evidence_sha256=_sha(f"flow-closure-{bar_open_ms}"),
    )


def _kline(
    *,
    bar_open_ms: int,
    decision_cutoff_ms: int,
    close: Decimal,
    receipt_ms: int | None = None,
) -> FamilyBKlineBarV2:
    lineage = _lineage()
    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    return FamilyBKlineBarV2.create(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        capture_root_sha256=lineage.kline_capture_root_sha256,
        schema_sha256=lineage.kline_schema_sha256,
        interval_ms=FIVE_MINUTE_MS_V2,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        open_event_id=f"kline-open-{bar_open_ms}",
        close_event_id=f"kline-close-{bar_open_ms}",
        closed=True,
        event_ms=bar_close_ms,
        receipt_ms=decision_cutoff_ms if receipt_ms is None else receipt_ms,
        high=max(Decimal(105), close),
        low=min(Decimal(95), close),
        close=close,
        previous_close=Decimal(100),
        source_evidence_sha256=_sha(f"kline-{bar_open_ms}"),
    )


@lru_cache(maxsize=64)
def _entry_input(
    *,
    attempt_id: str = "attempt-1",
    flow_sign: int = 1,
    flow_z: Decimal = Decimal("2.0"),
    return_z: Decimal = Decimal("1.0"),
    d_start: Decimal = Decimal("100"),
    d_low: Decimal = Decimal("50"),
    d_end: Decimal = Decimal("59.99"),
    spread_bps: Decimal = Decimal("20"),
    prior_count: int = 8_640,
) -> FamilyBEntryInputV2:
    states = _book_states(
        d_start=d_start,
        d_low=d_low,
        d_end=d_end,
        spread_bps=spread_bps,
    )
    current_return = Decimal(0)
    current_flow = Decimal(flow_sign) * Decimal("0.6")
    evidence = build_family_b_feature_evidence_v2(
        attempt_id=attempt_id,
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=D_MS,
        source_lineage=_lineage(),
        book_states=states,
        normal_flow_trades=_flow_trades(flow_sign),
        prior_features=_prior_features(
            current_flow=current_flow,
            current_return=current_return,
            flow_z=flow_z,
            return_z=return_z,
            count=prior_count,
        ),
        flow_window_closure=_flow_closure(
            bar_open_ms=BAR_OPEN_MS,
            decision_cutoff_ms=D_MS,
        ),
        kline_bar=_kline(
            bar_open_ms=BAR_OPEN_MS,
            decision_cutoff_ms=D_MS,
            close=Decimal(100),
        ),
    )
    return FamilyBEntryInputV2(
        attempt_id=attempt_id,
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA,
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=D_MS,
        feature_evidence=evidence,
    )


def _b2_input(*, flow_sign: int = 1, **overrides: object) -> FamilyBEntryInputV2:
    values: dict[str, object] = {
        "flow_sign": flow_sign,
        "return_z": Decimal(flow_sign) * Decimal("0.25"),
        "d_end": Decimal("75"),
    }
    values.update(overrides)
    return _entry_input(**values)  # type: ignore[arg-type]


def _registry(*, maximum_events: int = 16) -> FamilyBDecisionRegistryV2:
    return FamilyBDecisionRegistryV2(maximum_events=maximum_events)


def _paper_fill(
    item: FamilyBEntryInputV2,
    decision: FamilyBEntryDecisionV2,
    *,
    requested_quantity: Decimal = Decimal("2.00"),
) -> tuple[
    PaperFokEntryDecisionV2,
    PaperFokFullFillCertificateV2,
    PaperFokDecisionRegistryV2,
]:
    assert decision.side is not None
    return build_usdm_paper_full_fill_v2(
        attempt_id=item.attempt_id,
        signal_event_id=decision.event_id,
        symbol=item.symbol,
        promoting_plan_sha256=item.promoting_plan_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        side=(PaperFokSideV2.BUY if decision.side is FamilyBSideV2.LONG else PaperFokSideV2.SELL),
        requested_quantity=requested_quantity,
    )


def _position_state(
    side: FamilyBSideV2,
    *,
    attempt_id: str = "attempt-1",
    maximum_events: int = 16,
) -> tuple[FamilyBPositionV2, FamilyBDecisionRegistryV2]:
    flow_sign = 1 if side is FamilyBSideV2.LONG else -1
    item = _entry_input(
        attempt_id=attempt_id,
        flow_sign=flow_sign,
        return_z=Decimal(flow_sign),
    )
    registry = _registry(maximum_events=maximum_events)
    decision = evaluate_family_b_entry_v2(item, registry)
    paper_decision, certificate, paper_registry = _paper_fill(item, decision)
    position = position_from_family_b_signal_v2(
        item,
        decision,
        registry,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    return position, registry


def _position(side: FamilyBSideV2) -> FamilyBPositionV2:
    return _position_state(side)[0]


def _exit_flow_trades(
    *,
    bar_open_ms: int,
    flow_imbalance: Decimal,
) -> tuple[FamilyBNormalFlowTradeV2, ...]:
    lineage = _lineage()
    buy = Decimal(1) + flow_imbalance
    sell = Decimal(1) - flow_imbalance
    rows: list[FamilyBNormalFlowTradeV2] = []
    for trade_id, quantity, buyer_maker in (
        (1, buy, False),
        (2, sell, True),
    ):
        rows.append(
            FamilyBNormalFlowTradeV2.create(
                symbol="BTCUSDT",
                venue=VenueV2.USDM_FUTURES,
                promoting_plan_sha256=PLAN_SHA,
                capture_root_sha256=lineage.normal_flow_capture_root_sha256,
                schema_sha256=lineage.normal_flow_nq_schema_sha256,
                trade_id=trade_id,
                transaction_time_ms=bar_open_ms + trade_id * 1_000,
                receipt_ms=bar_open_ms + trade_id * 1_000 + 1,
                price=Decimal(1),
                quantity=quantity,
                normal_quantity=quantity,
                contract_multiplier=Decimal(1),
                buyer_maker=buyer_maker,
                source_evidence_sha256=_sha(f"exit-trade-{bar_open_ms}-{trade_id}"),
            )
        )
    return tuple(rows)


def _exit_state(
    side: FamilyBSideV2,
    *,
    position_state: tuple[FamilyBPositionV2, FamilyBDecisionRegistryV2] | None = None,
    **overrides: object,
) -> tuple[FamilyBExitInputV2, FamilyBDecisionRegistryV2]:
    position, registry = _position_state(side) if position_state is None else position_state
    bar_open_ms = BAR_OPEN_MS + FIVE_MINUTE_MS_V2
    values: dict[str, object] = {
        "position": position,
        "bar_open_ms": bar_open_ms,
        "bar_close_ms": bar_open_ms + FIVE_MINUTE_MS_V2 - 1,
        "decision_cutoff_ms": (bar_open_ms + FIVE_MINUTE_MS_V2 - 1 + DECISION_DELAY_MS_V2),
        "mandatory_exit": None,
        "close_price": Decimal("100"),
        "flow_imbalance_current": Decimal(0),
    }
    values.update(overrides)
    bound_position = values.pop("position")
    bound_bar_open_ms = values.pop("bar_open_ms")
    bound_bar_close_ms = values.pop("bar_close_ms")
    bound_decision_cutoff_ms = values.pop("decision_cutoff_ms")
    mandatory_exit = values.pop("mandatory_exit")
    close_price = values.pop("close_price")
    flow_imbalance = values.pop("flow_imbalance_current")
    kline_receipt_ms = values.pop("kline_receipt_ms", None)
    if values:
        raise AssertionError(f"unsupported exit test overrides: {tuple(values)}")
    assert isinstance(bound_position, type(position))
    assert type(bound_bar_open_ms) is int
    assert type(bound_bar_close_ms) is int
    assert type(bound_decision_cutoff_ms) is int
    assert bound_bar_close_ms == bound_bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    assert isinstance(close_price, Decimal)
    assert isinstance(flow_imbalance, Decimal)
    assert kline_receipt_ms is None or type(kline_receipt_ms) is int
    evidence = build_family_b_exit_feature_evidence_v2(
        attempt_id=position.attempt_id,
        symbol=position.symbol,
        venue=position.venue,
        promoting_plan_sha256=position.promoting_plan_sha256,
        bar_open_ms=bound_bar_open_ms,
        bar_close_ms=bound_bar_close_ms,
        decision_cutoff_ms=bound_decision_cutoff_ms,
        source_lineage=_exit_lineage(),
        normal_flow_trades=_exit_flow_trades(
            bar_open_ms=bound_bar_open_ms,
            flow_imbalance=flow_imbalance,
        ),
        flow_window_closure=_flow_closure(
            bar_open_ms=bound_bar_open_ms,
            decision_cutoff_ms=bound_decision_cutoff_ms,
        ),
        kline_bar=_kline(
            bar_open_ms=bound_bar_open_ms,
            decision_cutoff_ms=bound_decision_cutoff_ms,
            close=close_price,
            receipt_ms=kline_receipt_ms,
        ),
    )
    return (
        FamilyBExitInputV2(
            position=position,
            mandatory_exit=mandatory_exit,  # type: ignore[arg-type]
            exit_feature_evidence=evidence,
        ),
        registry,
    )


def _evaluate_exit(
    side: FamilyBSideV2,
    *,
    position_state: tuple[FamilyBPositionV2, FamilyBDecisionRegistryV2] | None = None,
    **overrides: object,
) -> FamilyBExitDecisionV2:
    item, registry = _exit_state(
        side,
        position_state=position_state,
        **overrides,
    )
    return evaluate_family_b_exit_v2(item, registry)


@pytest.mark.parametrize(
    ("child", "flow_sign", "expected_side"),
    [
        (FamilyBChildV2.B1, 1, FamilyBSideV2.LONG),
        (FamilyBChildV2.B1, -1, FamilyBSideV2.SHORT),
        (FamilyBChildV2.B2, 1, FamilyBSideV2.SHORT),
        (FamilyBChildV2.B2, -1, FamilyBSideV2.LONG),
    ],
)
def test_positive_b1_b2_directions_are_literal(
    child: FamilyBChildV2,
    flow_sign: int,
    expected_side: FamilyBSideV2,
) -> None:
    item = (
        _entry_input(flow_sign=flow_sign, return_z=Decimal(flow_sign))
        if child is FamilyBChildV2.B1
        else _b2_input(flow_sign=flow_sign)
    )
    decision = evaluate_family_b_entry_v2(item, _registry())
    assert decision.status is FamilyBEntryStatusV2.SIGNAL
    assert decision.child is child
    assert decision.side is expected_side
    assert decision.rule_version == FAMILY_B_RULE_VERSION_V2
    assert decision.feature_evidence_sha256 == item.feature_evidence.feature_evidence_sha256
    assert decision.feature_source_root_sha256 == item.feature_evidence.feature_source_root_sha256


@pytest.mark.parametrize(
    ("accepted", "rejected", "reason"),
    [
        ({"flow_z": Decimal("2")}, {"flow_z": Decimal("1.9999")}, "B1_ABS_RZ_I_LT_2_0"),
        (
            {"return_z": Decimal("1")},
            {"return_z": Decimal("0.9999")},
            "B1_ALIGNED_RETURN_RZ_LT_1_0",
        ),
        ({"d_low": Decimal("50")}, {"d_low": Decimal("50.0001")}, "B1_DEPLETION_RATIO_GT_0_50"),
        ({"d_end": Decimal("59.9999")}, {"d_end": Decimal("60")}, "B1_RECOVERY_RATIO_GTE_1_20"),
        ({"spread_bps": Decimal("20")}, {"spread_bps": Decimal("20.0001")}, "B1_SPREAD95_GT_20"),
    ],
)
def test_every_b1_literal_equality_and_rejected_side(
    accepted: dict[str, Decimal],
    rejected: dict[str, Decimal],
    reason: str,
) -> None:
    passed = evaluate_family_b_entry_v2(_entry_input(**accepted), _registry())
    failed = evaluate_family_b_entry_v2(_entry_input(**rejected), _registry())
    assert passed.status is FamilyBEntryStatusV2.SIGNAL
    assert failed.status is FamilyBEntryStatusV2.NO_SIGNAL
    assert reason in failed.reasons


@pytest.mark.parametrize(
    ("accepted", "rejected", "reason"),
    [
        ({"flow_z": Decimal("2")}, {"flow_z": Decimal("1.9999")}, "B2_ABS_RZ_I_LT_2_0"),
        (
            {"return_z": Decimal("0.25")},
            {"return_z": Decimal("0.2501")},
            "B2_ALIGNED_RETURN_RZ_GT_0_25",
        ),
        (
            {"return_z": Decimal("-0.75")},
            {"return_z": Decimal("-0.7501")},
            "B2_ABS_RETURN_RZ_GT_0_75",
        ),
        ({"d_end": Decimal("75")}, {"d_end": Decimal("74.9999")}, "B2_REPLENISHMENT_RATIO_LT_1_50"),
        ({"spread_bps": Decimal("20")}, {"spread_bps": Decimal("20.0001")}, "B2_SPREAD95_GT_20"),
    ],
)
def test_every_b2_literal_equality_and_rejected_side(
    accepted: dict[str, Decimal],
    rejected: dict[str, Decimal],
    reason: str,
) -> None:
    passed = evaluate_family_b_entry_v2(
        _b2_input(**accepted),  # type: ignore[arg-type]
        _registry(),
    )
    failed = evaluate_family_b_entry_v2(
        _b2_input(**rejected),  # type: ignore[arg-type]
        _registry(),
    )
    assert passed.status is FamilyBEntryStatusV2.SIGNAL
    assert failed.status is FamilyBEntryStatusV2.NO_SIGNAL
    assert reason in failed.reasons


def test_i_zero_warmup_and_registry_derived_active_position_fail_closed() -> None:
    zero_flow = evaluate_family_b_entry_v2(
        _entry_input(flow_sign=0),
        _registry(),
    )
    warmup = evaluate_family_b_entry_v2(
        _entry_input(prior_count=8_639),
        _registry(),
    )
    _, active_registry = _position_state(FamilyBSideV2.LONG)
    active = evaluate_family_b_entry_v2(
        _entry_input(attempt_id="attempt-active-conflict"),
        active_registry,
    )
    assert zero_flow.status is FamilyBEntryStatusV2.NO_SIGNAL
    assert warmup.status is FamilyBEntryStatusV2.FEATURE_NOT_READY
    assert active.status is FamilyBEntryStatusV2.NOT_ADMITTED_ACTIVE_POSITION


def test_simultaneous_b1_b2_injection_is_data_invalid_rule_invariant() -> None:
    resolution = resolve_family_b_child_matches_v2(
        b1_matches=True,
        b2_matches=True,
    )
    assert resolution.status is FamilyBEntryStatusV2.DATA_INVALID_RULE_INVARIANT
    assert resolution.child is None


def test_entry_payload_replay_idempotency_and_same_id_conflict_are_exact() -> None:
    registry = _registry(maximum_events=1)
    signal = evaluate_family_b_entry_v2(_entry_input(), registry)
    replay = evaluate_family_b_entry_v2(_entry_input(), registry)
    no_signal = evaluate_family_b_entry_v2(
        _entry_input(flow_z=Decimal("1.9999")),
        _registry(),
    )
    assert signal == replay
    assert signal.event_id == no_signal.event_id
    assert signal.payload_sha256 != no_signal.payload_sha256
    assert b'"venue":"usdm_futures"' in canonical_family_b_entry_decision_v2(signal)
    assert PLAN_SHA.encode() in canonical_family_b_entry_decision_v2(signal)
    with pytest.raises(FamilyBContractError, match="conflicting causal input"):
        evaluate_family_b_entry_v2(
            _entry_input(flow_z=Decimal("1.9999")),
            registry,
        )
    with pytest.raises(FamilyBContractError, match="capacity exhausted"):
        evaluate_family_b_entry_v2(
            _entry_input(attempt_id="attempt-capacity"),
            registry,
        )


def test_entry_preview_commit_is_non_mutating_exact_and_idempotent() -> None:
    registry = _registry(maximum_events=2)
    item = _entry_input()
    root = registry.replay_root_sha256
    preview = registry.preview_entry(item)

    assert preview.pre_replay_root_sha256 == root
    assert preview.pre_event_count == 0
    assert not preview.already_committed
    assert registry.replay_root_sha256 == root
    assert registry.event_count == 0
    receipt = registry.commit_entry_preview_with_receipt(item, preview)
    assert receipt.decision == preview.decision
    assert receipt.input_sha256 == preview.input_sha256
    assert receipt.event_id == preview.decision.event_id
    assert receipt.pre_root_sha256 == preview.pre_replay_root_sha256
    assert receipt.pre_event_count == preview.pre_event_count
    assert receipt.disposition is FamilyBEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION
    committed_root = registry.replay_root_sha256
    assert receipt.post_root_sha256 == committed_root
    assert receipt.post_event_count == 1
    assert committed_root != root
    assert registry.event_count == 1
    assert registry.commit_entry_preview(item, preview) == preview.decision
    assert registry.replay_root_sha256 == committed_root

    replay = registry.preview_entry(item)
    assert replay.already_committed
    assert replay.pre_replay_root_sha256 == committed_root
    assert replay.pre_event_count == 1
    replay_receipt = registry.commit_entry_preview_with_receipt(item, replay)
    assert replay_receipt.decision == preview.decision
    assert replay_receipt.disposition is FamilyBEntryCommitDispositionV2.PREEXISTING
    assert replay_receipt.pre_root_sha256 == replay_receipt.post_root_sha256
    assert replay_receipt.pre_event_count == replay_receipt.post_event_count == 1
    with pytest.raises(FamilyBContractError, match="pre-existing"):
        registry.rollback_entry_preview(item, replay, replay_receipt)

    with pytest.raises(FamilyBContractError, match="created by the registry"):
        FamilyBEntryPreviewV2(
            input_sha256=preview.input_sha256,
            pre_replay_root_sha256=preview.pre_replay_root_sha256,
            pre_event_count=preview.pre_event_count,
            decision=preview.decision,
            already_committed=preview.already_committed,
        )
    with pytest.raises(FamilyBContractError, match="created by the registry"):
        FamilyBEntryCommitReceiptV2(
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
    decision = evaluate_family_b_entry_v2(_entry_input(), _registry())
    payload = canonical_family_b_entry_decision_v2(decision)
    assert parse_canonical_family_b_entry_decision_v2(payload) == decision
    with pytest.raises(FamilyBContractError, match="canonical JSONL"):
        parse_canonical_family_b_entry_decision_v2(payload + b"\n")


def test_entry_preview_rejects_input_conflict_capacity_and_state_drift() -> None:
    item = _entry_input()
    conflict = _entry_input(flow_z=Decimal("1.9999"))
    conflict_registry = _registry(maximum_events=2)
    preview = conflict_registry.preview_entry(item)
    conflict_registry.evaluate_entry(conflict)
    with pytest.raises(FamilyBContractError, match="conflicts with committed input"):
        conflict_registry.commit_entry_preview(item, preview)

    drift_registry = _registry(maximum_events=2)
    drift_preview = drift_registry.preview_entry(item)
    other = _entry_input(attempt_id="attempt-state-drift")
    drift_registry.evaluate_entry(other)
    with pytest.raises(FamilyBContractError, match="state drifted"):
        drift_registry.commit_entry_preview(item, drift_preview)
    with pytest.raises(FamilyBContractError, match="differs from exact input"):
        _registry().commit_entry_preview(conflict, preview)

    capacity_registry = _registry(maximum_events=1)
    capacity_registry.evaluate_entry(item)
    assert capacity_registry.preview_entry(item).already_committed
    with pytest.raises(FamilyBContractError, match="capacity exhausted"):
        capacity_registry.preview_entry(other)


def test_entry_preview_rollback_restores_only_untouched_pre_state() -> None:
    registry = _registry(maximum_events=4)
    item = _entry_input()
    preview = registry.preview_entry(item)
    receipt = registry.commit_entry_preview_with_receipt(item, preview)
    assert registry.rollback_entry_preview(item, preview, receipt)
    assert registry.replay_root_sha256 == preview.pre_replay_root_sha256
    assert registry.event_count == preview.pre_event_count
    with pytest.raises(FamilyBContractError, match="does not own"):
        registry.rollback_entry_preview(item, preview, receipt)

    recommit = registry.commit_entry_preview_with_receipt(item, preview)
    with pytest.raises(FamilyBContractError, match="does not own"):
        registry.rollback_entry_preview(item, preview, receipt)
    assert registry.event_count == 1
    assert registry.rollback_entry_preview(item, preview, recommit)

    admitted = _registry(maximum_events=4)
    admitted_preview = admitted.preview_entry(item)
    admitted_receipt = admitted.commit_entry_preview_with_receipt(item, admitted_preview)
    signal = admitted_receipt.decision
    paper_decision, certificate, paper_registry = _paper_fill(item, signal)
    admitted.admit_position(
        item,
        signal,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    admitted_root = admitted.replay_root_sha256
    with pytest.raises(FamilyBContractError, match="state drifted"):
        admitted.rollback_entry_preview(item, admitted_preview, admitted_receipt)
    assert admitted.replay_root_sha256 == admitted_root
    assert admitted.is_active(
        promoting_plan_sha256=PLAN_SHA,
        venue=VenueV2.USDM_FUTURES,
        symbol="BTCUSDT",
    )


def test_identical_concurrent_entry_commits_issue_only_one_rollback_capability() -> None:
    registry = _registry(maximum_events=2)
    item = _entry_input()
    preview = registry.preview_entry(item)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = tuple(
            pool.submit(registry.commit_entry_preview_with_receipt, item, preview) for _ in range(2)
        )
        receipts = tuple(future.result() for future in futures)

    by_disposition = {receipt.disposition: receipt for receipt in receipts}
    assert set(by_disposition) == {
        FamilyBEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION,
        FamilyBEntryCommitDispositionV2.PREEXISTING,
    }
    preexisting = by_disposition[FamilyBEntryCommitDispositionV2.PREEXISTING]
    with pytest.raises(FamilyBContractError, match="pre-existing"):
        registry.rollback_entry_preview(item, preview, preexisting)
    assert registry.event_count == 1

    foreign = _registry(maximum_events=2)
    foreign_preview = foreign.preview_entry(item)
    foreign_receipt = foreign.commit_entry_preview_with_receipt(item, foreign_preview)
    with pytest.raises(FamilyBContractError, match="another registry"):
        registry.rollback_entry_preview(item, preview, foreign_receipt)
    assert registry.event_count == 1

    created = by_disposition[FamilyBEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION]
    assert registry.rollback_entry_preview(item, preview, created)


def test_prospective_authority_gates_every_family_b_mutation_surface() -> None:
    registry = FamilyBDecisionRegistryV2(maximum_events=4)
    item = _entry_input()
    preview = registry.preview_entry(item)
    authority = registry._claim_prospective_decision_authority_v2()  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(FamilyBContractError, match="held prospective decision authority"):
        registry.evaluate_entry(item)
    with pytest.raises(FamilyBContractError, match="held prospective decision authority"):
        registry.commit_entry_preview(item, preview)
    with pytest.raises(FamilyBContractError, match="held prospective decision authority"):
        registry.admit_position(
            item,
            preview.decision,
            paper_decision=cast(PaperFokEntryDecisionV2, object()),
            certificate=cast(PaperFokFullFillCertificateV2, object()),
            paper_registry=cast(PaperFokDecisionRegistryV2, object()),
        )
    with pytest.raises(FamilyBContractError, match="held prospective decision authority"):
        registry.evaluate_exit(cast(FamilyBExitInputV2, object()))

    receipt = registry.commit_entry_preview_with_receipt(
        item,
        preview,
        _prospective_authority=authority,
    )
    with pytest.raises(FamilyBContractError, match="cannot release a non-genesis"):
        registry._release_unconsumed_prospective_decision_authority_v2(  # pyright: ignore[reportPrivateUsage]
            authority
        )
    assert registry.rollback_entry_preview(
        item,
        preview,
        receipt,
        _prospective_authority=authority,
    )
    registry._release_unconsumed_prospective_decision_authority_v2(  # pyright: ignore[reportPrivateUsage]
        authority
    )
    assert registry.evaluate_entry(item) == preview.decision


def test_family_b_prospective_authority_rejects_prepopulated_registry() -> None:
    registry = FamilyBDecisionRegistryV2(maximum_events=4)
    registry.evaluate_entry(_entry_input())

    with pytest.raises(FamilyBContractError, match="requires exact genesis state"):
        registry._claim_prospective_decision_authority_v2()  # pyright: ignore[reportPrivateUsage]


def test_direct_entry_decision_cannot_forge_b1_direction() -> None:
    valid = evaluate_family_b_entry_v2(_entry_input(), _registry())
    assert valid.side is FamilyBSideV2.LONG
    with pytest.raises(FamilyBContractError, match="direction rule"):
        FamilyBEntryDecisionV2(
            attempt_id=valid.attempt_id,
            symbol=valid.symbol,
            venue=valid.venue,
            promoting_plan_sha256=valid.promoting_plan_sha256,
            bar_open_ms=valid.bar_open_ms,
            bar_close_ms=valid.bar_close_ms,
            decision_cutoff_ms=valid.decision_cutoff_ms,
            feature_evidence_sha256=valid.feature_evidence_sha256,
            feature_source_root_sha256=valid.feature_source_root_sha256,
            status=valid.status,
            child=valid.child,
            side=FamilyBSideV2.SHORT,
            reasons=valid.reasons,
            invalidation="close_j >= entry_VWAP + TR_t",
            flow_sign=valid.flow_sign,
            event_true_range=valid.event_true_range,
        )


def test_entry_input_rejects_mismatched_plan_and_non_futures_venue() -> None:
    evidence = _entry_input().feature_evidence
    with pytest.raises(FamilyBContractError, match="identity differs"):
        FamilyBEntryInputV2(
            attempt_id=evidence.attempt_id,
            symbol=evidence.symbol,
            venue=evidence.venue,
            promoting_plan_sha256="b" * 64,
            bar_open_ms=evidence.bar_open_ms,
            bar_close_ms=evidence.bar_close_ms,
            decision_cutoff_ms=evidence.decision_cutoff_ms,
            feature_evidence=evidence,
        )
    with pytest.raises(FamilyBContractError, match="USD-M Futures"):
        FamilyBEntryInputV2(
            attempt_id=evidence.attempt_id,
            symbol=evidence.symbol,
            venue=VenueV2.SPOT,
            promoting_plan_sha256=evidence.promoting_plan_sha256,
            bar_open_ms=evidence.bar_open_ms,
            bar_close_ms=evidence.bar_close_ms,
            decision_cutoff_ms=evidence.decision_cutoff_ms,
            feature_evidence=evidence,
        )


def test_entry_input_has_no_caller_supplied_active_position_flag() -> None:
    evidence = _entry_input().feature_evidence
    with pytest.raises(TypeError, match="active_position"):
        FamilyBEntryInputV2(
            attempt_id=evidence.attempt_id,
            symbol=evidence.symbol,
            venue=evidence.venue,
            promoting_plan_sha256=evidence.promoting_plan_sha256,
            bar_open_ms=evidence.bar_open_ms,
            bar_close_ms=evidence.bar_close_ms,
            decision_cutoff_ms=evidence.decision_cutoff_ms,
            feature_evidence=evidence,
            active_position=True,  # type: ignore[call-arg]
        )


def test_atomic_admission_is_exact_idempotent_and_rejects_conflicts() -> None:
    registry = _registry()
    first_item = _entry_input()
    second_item = _entry_input(attempt_id="attempt-second-pending-signal")
    first_decision = evaluate_family_b_entry_v2(first_item, registry)
    second_decision = evaluate_family_b_entry_v2(second_item, registry)
    assert first_decision.emitted_signal
    assert second_decision.emitted_signal
    assert not registry.is_active(
        promoting_plan_sha256=PLAN_SHA,
        venue=VenueV2.USDM_FUTURES,
        symbol="BTCUSDT",
    )

    paper_decision, certificate, paper_registry = _paper_fill(
        first_item,
        first_decision,
    )
    position = position_from_family_b_signal_v2(
        first_item,
        first_decision,
        registry,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    replay = position_from_family_b_signal_v2(
        first_item,
        first_decision,
        registry,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    assert replay is position
    assert registry.is_active(
        promoting_plan_sha256=PLAN_SHA,
        venue=VenueV2.USDM_FUTURES,
        symbol="BTCUSDT",
    )

    conflict_decision, conflict_certificate, conflict_registry = _paper_fill(
        first_item,
        first_decision,
        requested_quantity=Decimal("1.00"),
    )
    with pytest.raises(FamilyBContractError, match=r"conflicting.*admission replay"):
        position_from_family_b_signal_v2(
            first_item,
            first_decision,
            registry,
            paper_decision=conflict_decision,
            certificate=conflict_certificate,
            paper_registry=conflict_registry,
        )

    second_paper, second_certificate, second_paper_registry = _paper_fill(
        second_item,
        second_decision,
    )
    with pytest.raises(FamilyBContractError, match="already active"):
        position_from_family_b_signal_v2(
            second_item,
            second_decision,
            registry,
            paper_decision=second_paper,
            certificate=second_certificate,
            paper_registry=second_paper_registry,
        )


def test_admission_requires_the_exact_ledgered_signal_input() -> None:
    item = _entry_input()
    decision = evaluate_family_b_entry_v2(item, _registry())
    paper_decision, certificate, paper_registry = _paper_fill(item, decision)
    with pytest.raises(FamilyBContractError, match="not the ledgered result"):
        position_from_family_b_signal_v2(
            item,
            decision,
            _registry(),
            paper_decision=paper_decision,
            certificate=certificate,
            paper_registry=paper_registry,
        )


def test_true_range_and_frozen_position_retain_provenance() -> None:
    assert event_true_range_v2(
        high=Decimal("102"),
        low=Decimal("99"),
        previous_close=Decimal("95"),
    ) == Decimal(7)
    item = _entry_input()
    registry = _registry()
    decision = evaluate_family_b_entry_v2(item, registry)
    paper_decision, certificate, paper_registry = build_usdm_paper_full_fill_v2(
        attempt_id=item.attempt_id,
        signal_event_id=decision.event_id,
        symbol=item.symbol,
        promoting_plan_sha256=item.promoting_plan_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        side=PaperFokSideV2.BUY,
    )
    position = position_from_family_b_signal_v2(
        item,
        decision,
        registry,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    assert position.entry_vwap == paper_decision.executable_vwap
    assert position.paper_filled_quantity == paper_decision.requested_quantity
    assert position.paper_executable_notional == paper_decision.executable_notional
    assert position.admission_evidence_sha256 == certificate.certificate_sha256
    assert (
        position.paper_registry_checkpoint_sha256
        == paper_registry.terminal_checkpoint_v2().checkpoint_sha256
    )
    assert position.event_true_range == Decimal(10)
    assert position.venue is VenueV2.USDM_FUTURES
    assert position.promoting_plan_sha256 == PLAN_SHA


def test_position_rejects_wrong_signal_or_unpinned_paper_registry() -> None:
    item = _entry_input()
    registry = _registry()
    decision = evaluate_family_b_entry_v2(item, registry)
    wrong_decision, wrong_certificate, wrong_registry = build_usdm_paper_full_fill_v2(
        attempt_id=item.attempt_id,
        signal_event_id=_sha("different-family-b-signal"),
        symbol=item.symbol,
        promoting_plan_sha256=item.promoting_plan_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        side=PaperFokSideV2.BUY,
    )
    with pytest.raises(FamilyBContractError, match="identity differs"):
        position_from_family_b_signal_v2(
            item,
            decision,
            registry,
            paper_decision=wrong_decision,
            certificate=wrong_certificate,
            paper_registry=wrong_registry,
        )

    paper_decision, certificate, _ = build_usdm_paper_full_fill_v2(
        attempt_id=item.attempt_id,
        signal_event_id=decision.event_id,
        symbol=item.symbol,
        promoting_plan_sha256=item.promoting_plan_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        side=PaperFokSideV2.BUY,
    )
    with pytest.raises(FamilyBContractError, match="absent from its registry"):
        position_from_family_b_signal_v2(
            item,
            decision,
            registry,
            paper_decision=paper_decision,
            certificate=certificate,
            paper_registry=PaperFokDecisionRegistryV2(
                maximum_events=4,
                attempt_id=item.attempt_id,
                promoting_plan_sha256=item.promoting_plan_sha256,
            ),
        )


@pytest.mark.parametrize("side", [FamilyBSideV2.LONG, FamilyBSideV2.SHORT])
def test_adverse_one_true_range_equality_triggers(side: FamilyBSideV2) -> None:
    position = _position(side)
    boundary = (
        position.entry_vwap - position.event_true_range
        if side is FamilyBSideV2.LONG
        else position.entry_vwap + position.event_true_range
    )
    safe = boundary + EPSILON if side is FamilyBSideV2.LONG else boundary - EPSILON
    equality = _evaluate_exit(side, close_price=boundary)
    inside = _evaluate_exit(side, close_price=safe)
    assert equality.reason is FamilyBExitReasonV2.ADVERSE_INVALIDATION
    assert inside.reason is FamilyBExitReasonV2.HOLD


@pytest.mark.parametrize("side", [FamilyBSideV2.LONG, FamilyBSideV2.SHORT])
def test_flow_reversal_negative_point_30_equality_triggers(side: FamilyBSideV2) -> None:
    boundary = Decimal("-0.30") if side is FamilyBSideV2.LONG else Decimal("0.30")
    safe = Decimal("-0.2999") if side is FamilyBSideV2.LONG else Decimal("0.2999")
    equality = _evaluate_exit(side, flow_imbalance_current=boundary)
    inside = _evaluate_exit(side, flow_imbalance_current=safe)
    assert equality.reason is FamilyBExitReasonV2.FLOW_REVERSAL
    assert inside.reason is FamilyBExitReasonV2.HOLD


def test_hard_horizon_and_full_exit_priority_are_exact() -> None:
    hard_open = BAR_OPEN_MS + FAMILY_B_HARD_HORIZON_BARS_V2 * FIVE_MINUTE_MS_V2
    hard_close = hard_open + FIVE_MINUTE_MS_V2 - 1
    hard_cutoff = hard_close + DECISION_DELAY_MS_V2
    hard = _evaluate_exit(
        FamilyBSideV2.LONG,
        bar_open_ms=hard_open,
        bar_close_ms=hard_close,
        decision_cutoff_ms=hard_cutoff,
    )
    flow = _evaluate_exit(
        FamilyBSideV2.LONG,
        flow_imbalance_current=Decimal("-0.30"),
        bar_open_ms=hard_open,
        bar_close_ms=hard_close,
        decision_cutoff_ms=hard_cutoff,
    )
    adverse = _evaluate_exit(
        FamilyBSideV2.LONG,
        close_price=Decimal("90"),
        flow_imbalance_current=Decimal("-0.30"),
        bar_open_ms=hard_open,
        bar_close_ms=hard_close,
        decision_cutoff_ms=hard_cutoff,
    )
    mandatory = _evaluate_exit(
        FamilyBSideV2.LONG,
        close_price=Decimal("90"),
        mandatory_exit=FamilyBMandatoryExitV2.DATA,
        bar_open_ms=hard_open,
        bar_close_ms=hard_close,
        decision_cutoff_ms=hard_cutoff,
    )
    assert hard.reason is FamilyBExitReasonV2.HARD_HORIZON
    assert flow.reason is FamilyBExitReasonV2.FLOW_REVERSAL
    assert adverse.reason is FamilyBExitReasonV2.ADVERSE_INVALIDATION
    assert mandatory.reason is FamilyBExitReasonV2.MANDATORY_DATA_EMERGENCY


def test_exit_payload_hash_registry_and_capacity_are_deterministic() -> None:
    position_state = _position_state(
        FamilyBSideV2.SHORT,
        maximum_events=2,
    )
    item, registry = _exit_state(
        FamilyBSideV2.SHORT,
        position_state=position_state,
    )
    first = evaluate_family_b_exit_v2(item, registry)
    second = evaluate_family_b_exit_v2(item, registry)
    assert first == second
    assert len(first.payload_sha256) == 64
    assert b'"action":"HOLD"' in canonical_family_b_exit_decision_v2(first)
    with pytest.raises(FamilyBContractError, match="capacity exhausted"):
        next_open = BAR_OPEN_MS + 2 * FIVE_MINUTE_MS_V2
        later, _ = _exit_state(
            FamilyBSideV2.SHORT,
            position_state=position_state,
            bar_open_ms=next_open,
            bar_close_ms=next_open + FIVE_MINUTE_MS_V2 - 1,
            decision_cutoff_ms=(next_open + FIVE_MINUTE_MS_V2 - 1 + DECISION_DELAY_MS_V2),
        )
        evaluate_family_b_exit_v2(later, registry)


def test_exit_requires_admitted_position_and_conflicting_replay_fails_closed() -> None:
    position_state = _position_state(FamilyBSideV2.LONG)
    item, registry = _exit_state(
        FamilyBSideV2.LONG,
        position_state=position_state,
    )
    with pytest.raises(FamilyBContractError, match="absent from its episode"):
        evaluate_family_b_exit_v2(item, _registry())

    decision = evaluate_family_b_exit_v2(item, registry)
    assert evaluate_family_b_exit_v2(item, registry) == decision
    conflicting, _ = _exit_state(
        FamilyBSideV2.LONG,
        position_state=position_state,
        flow_imbalance_current=Decimal("0.10"),
    )
    with pytest.raises(FamilyBContractError, match="conflicting causal input"):
        evaluate_family_b_exit_v2(conflicting, registry)


def test_terminal_exit_releases_active_and_exact_replay_does_not_reopen() -> None:
    position_state = _position_state(FamilyBSideV2.LONG)
    position, registry = position_state
    terminal_item, _ = _exit_state(
        FamilyBSideV2.LONG,
        position_state=position_state,
        close_price=Decimal("90"),
    )
    terminal = evaluate_family_b_exit_v2(terminal_item, registry)
    assert terminal.exits_position
    assert not registry.is_active(
        promoting_plan_sha256=PLAN_SHA,
        venue=VenueV2.USDM_FUTURES,
        symbol="BTCUSDT",
    )
    assert evaluate_family_b_exit_v2(terminal_item, registry) == terminal

    old_item = _entry_input(flow_sign=1, return_z=Decimal(1))
    old_decision = evaluate_family_b_entry_v2(old_item, registry)
    old_paper, old_certificate, old_paper_registry = _paper_fill(
        old_item,
        old_decision,
    )
    assert (
        position_from_family_b_signal_v2(
            old_item,
            old_decision,
            registry,
            paper_decision=old_paper,
            certificate=old_certificate,
            paper_registry=old_paper_registry,
        )
        is position
    )
    assert not registry.is_active(
        promoting_plan_sha256=PLAN_SHA,
        venue=VenueV2.USDM_FUTURES,
        symbol="BTCUSDT",
    )

    later_open = BAR_OPEN_MS + 2 * FIVE_MINUTE_MS_V2
    later_exit, _ = _exit_state(
        FamilyBSideV2.LONG,
        position_state=position_state,
        bar_open_ms=later_open,
        bar_close_ms=later_open + FIVE_MINUTE_MS_V2 - 1,
        decision_cutoff_ms=(later_open + FIVE_MINUTE_MS_V2 - 1 + DECISION_DELAY_MS_V2),
    )
    with pytest.raises(FamilyBContractError, match="already terminal"):
        evaluate_family_b_exit_v2(later_exit, registry)

    next_item = _entry_input(attempt_id="attempt-after-terminal")
    next_decision = evaluate_family_b_entry_v2(next_item, registry)
    assert next_decision.status is FamilyBEntryStatusV2.SIGNAL
    next_paper, next_certificate, next_paper_registry = _paper_fill(
        next_item,
        next_decision,
    )
    position_from_family_b_signal_v2(
        next_item,
        next_decision,
        registry,
        paper_decision=next_paper,
        certificate=next_certificate,
        paper_registry=next_paper_registry,
    )
    assert registry.is_active(
        promoting_plan_sha256=PLAN_SHA,
        venue=VenueV2.USDM_FUTURES,
        symbol="BTCUSDT",
    )


def test_exit_decision_retains_sealed_evidence_and_cannot_forge_side_action() -> None:
    item, registry = _exit_state(FamilyBSideV2.LONG)
    valid = evaluate_family_b_exit_v2(item, registry)
    assert valid.exit_evidence_sha256 == item.exit_feature_evidence.exit_evidence_sha256
    assert valid.exit_source_root_sha256 == item.exit_feature_evidence.exit_source_root_sha256
    with pytest.raises(FamilyBContractError, match="position side"):
        FamilyBExitDecisionV2(
            entry_event_id=valid.entry_event_id,
            attempt_id=valid.attempt_id,
            symbol=valid.symbol,
            venue=valid.venue,
            promoting_plan_sha256=valid.promoting_plan_sha256,
            bar_open_ms=valid.bar_open_ms,
            bar_close_ms=valid.bar_close_ms,
            decision_cutoff_ms=valid.decision_cutoff_ms,
            position_side=FamilyBSideV2.LONG,
            exit_evidence_sha256=valid.exit_evidence_sha256,
            exit_source_root_sha256=valid.exit_source_root_sha256,
            action=FamilyBExitActionV2.EXIT_SHORT,
            reason=FamilyBExitReasonV2.FLOW_REVERSAL,
            reasons=("FORGED",),
            invalidation="POSITION_EXIT_REQUIRED",
        )


def test_registry_restart_requires_external_root_count_and_capacity_pins() -> None:
    entry_item = _entry_input(flow_sign=1, return_z=Decimal(1))
    position_state = _position_state(FamilyBSideV2.LONG)
    _, registry = position_state
    exit_item, _ = _exit_state(
        FamilyBSideV2.LONG,
        position_state=position_state,
    )
    exit_decision = evaluate_family_b_exit_v2(exit_item, registry)
    payload = registry.export_state_v2()
    root = registry.replay_root_sha256
    count = registry.event_count
    maximum_events = registry.maximum_events

    restored = FamilyBDecisionRegistryV2.from_state_v2(
        payload,
        expected_replay_root_sha256=root,
        expected_event_count=count,
        expected_maximum_events=maximum_events,
    )
    assert restored.export_state_v2() == payload
    assert evaluate_family_b_entry_v2(entry_item, restored).event_id == (
        position_state[0].entry_event_id
    )
    assert evaluate_family_b_exit_v2(exit_item, restored) == exit_decision

    prefix_position_state = _position_state(FamilyBSideV2.LONG)
    prefix_registry = prefix_position_state[1]
    with pytest.raises(FamilyBContractError, match="external checkpoint"):
        FamilyBDecisionRegistryV2.from_state_v2(
            prefix_registry.export_state_v2(),
            expected_replay_root_sha256=root,
            expected_event_count=count,
            expected_maximum_events=maximum_events,
        )


def test_registry_restart_rejects_corrupted_active_and_position_state() -> None:
    _, registry = _position_state(FamilyBSideV2.LONG)
    document = json.loads(registry.export_state_v2())
    root = registry.replay_root_sha256
    count = registry.event_count
    maximum_events = registry.maximum_events

    missing_active = dict(document)
    missing_active["active"] = []
    with pytest.raises(FamilyBContractError, match="active index"):
        FamilyBDecisionRegistryV2.from_state_v2(
            canonical_json_line(missing_active),
            expected_replay_root_sha256=root,
            expected_event_count=count,
            expected_maximum_events=maximum_events,
        )

    corrupted_position = json.loads(registry.export_state_v2())
    corrupted_position["episodes"][0]["position_sha256"] = "b" * 64
    with pytest.raises(FamilyBContractError, match="position hash"):
        FamilyBDecisionRegistryV2.from_state_v2(
            canonical_json_line(corrupted_position),
            expected_replay_root_sha256=root,
            expected_event_count=count,
            expected_maximum_events=maximum_events,
        )


def test_overdue_exit_and_d_plus_one_exit_evidence_fail_closed() -> None:
    overdue_open = BAR_OPEN_MS + (FAMILY_B_HARD_HORIZON_BARS_V2 + 1) * FIVE_MINUTE_MS_V2
    overdue = _evaluate_exit(
        FamilyBSideV2.SHORT,
        bar_open_ms=overdue_open,
        bar_close_ms=overdue_open + FIVE_MINUTE_MS_V2 - 1,
        decision_cutoff_ms=(overdue_open + FIVE_MINUTE_MS_V2 - 1 + DECISION_DELAY_MS_V2),
    )
    assert overdue.reason is FamilyBExitReasonV2.MANDATORY_TERMINAL_EMERGENCY
    exit_open = BAR_OPEN_MS + FIVE_MINUTE_MS_V2
    exit_d = exit_open + FIVE_MINUTE_MS_V2 - 1 + DECISION_DELAY_MS_V2
    with pytest.raises(FamilyBFeatureContractErrorV2, match="after D"):
        _exit_state(
            FamilyBSideV2.SHORT,
            kline_receipt_ms=exit_d + 1,
        )
