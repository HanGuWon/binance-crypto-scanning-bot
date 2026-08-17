from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import fields, replace
from decimal import ROUND_DOWN, Decimal, getcontext, setcontext
from functools import cache
from typing import cast

import pytest

import signalbot.r4b_v2.strategy.family_c as family_c_module
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    PaperFokDecisionRegistryV2,
    PaperFokEntryDecisionV2,
    PaperFokFullFillCertificateV2,
    PaperFokSideV2,
)
from signalbot.r4b_v2.strategy.family_c import (
    DECISION_DELAY_MS_V2,
    FAMILY_C_HARD_HORIZON_BARS_V2,
    FAMILY_C_PANEL_BAR_COUNT_V2,
    FAMILY_C_PRIOR_WINDOW_V2,
    FAMILY_C_RULE_VERSION_V2,
    FIVE_MINUTE_MS_V2,
    FamilyCCandlePanelV2,
    FamilyCClosedCandleV2,
    FamilyCContractError,
    FamilyCDecisionRegistryV2,
    FamilyCEntryCommitDispositionV2,
    FamilyCEntryCommitReceiptV2,
    FamilyCEntryInputV2,
    FamilyCEntryPreviewV2,
    FamilyCEntryStatusV2,
    FamilyCEpisodeLedgerV2,
    FamilyCExitActionV2,
    FamilyCExitInputV2,
    FamilyCExitReasonV2,
    FamilyCFeatureSnapshotV2,
    FamilyCFeatureStatusV2,
    FamilyCIntervalStatusV2,
    FamilyCMandatoryExitV2,
    FamilyCMemberFeatureV2,
    FamilyCPositionV2,
    FamilyCPriorUniverseV2,
    FamilyCRawMemberHistoryV2,
    FamilyCRegistryDispositionV2,
    FamilyCSideV2,
    FamilyCSymbolCloseV2,
    FamilyCSymbolMoveV2,
    build_family_c_exit_input_v2,
    canonical_family_c_candle_panel_v2,
    canonical_family_c_entry_decision_v2,
    canonical_family_c_exit_decision_v2,
    canonical_family_c_feature_evidence_v2,
    construct_family_c_features_v2,
    construct_family_c_log_moves_v2,
    evaluate_family_c_entry_v2,
    evaluate_family_c_exit_v2,
    family_c_top_decile_count_v2,
    parse_canonical_family_c_entry_decision_v2,
    population_beta_v2,
    position_from_family_c_signal_v2,
    rank_family_c_members_v2,
)

from ..execution.paper_fok_testkit import build_usdm_paper_full_fill_v2

BAR_OPEN_MS = 2_000_160_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
DECISION_CUTOFF_MS = BAR_CLOSE_MS + DECISION_DELAY_MS_V2
EPSILON = Decimal("0.0001")
PROMOTING_PLAN_SHA256 = "b" * 64
SOURCE_ROOT_SHA256 = "a" * 64
PANEL_ROOT_SHA256 = "c" * 64
EXIT_SOURCE_ROOT_SHA256 = "d" * 64


def _symbols(count: int = 20) -> tuple[str, ...]:
    return tuple(f"S{index:02d}USDT" for index in range(count))


@cache
def _universe(count: int = 20) -> FamilyCPriorUniverseV2:
    day_start_ms = BAR_OPEN_MS // 86_400_000 * 86_400_000
    return FamilyCPriorUniverseV2(
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PROMOTING_PLAN_SHA256,
        source_root_sha256=SOURCE_ROOT_SHA256,
        effective_day_start_ms=day_start_ms,
        eligibility_cutoff_ms=day_start_ms - 1,
        members=_symbols(count),
    )


@cache
def _causal_panel() -> FamilyCCandlePanelV2:
    first_open_ms = BAR_OPEN_MS - ((FAMILY_C_PANEL_BAR_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2)
    factors = tuple(range(-10, 0)) + tuple(range(1, 11))
    candles: list[FamilyCClosedCandleV2] = []
    for symbol, factor in zip(_symbols(), factors, strict=True):
        evidence_sha256 = hashlib.sha256(symbol.encode("utf-8")).hexdigest()
        for index in range(FAMILY_C_PANEL_BAR_COUNT_V2):
            bar_open_ms = first_open_ms + index * FIVE_MINUTE_MS_V2
            bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
            common_close = Decimal(100 + index % 11)
            member_noise = (
                Decimal(factor) * Decimal("0.001") * (Decimal(1) if index % 4 < 2 else Decimal(-1))
            )
            candles.append(
                FamilyCClosedCandleV2(
                    symbol=symbol,
                    bar_open_ms=bar_open_ms,
                    bar_close_ms=bar_close_ms,
                    event_time_ms=bar_close_ms,
                    receipt_time_ms=(
                        DECISION_CUTOFF_MS if bar_open_ms == BAR_OPEN_MS else bar_close_ms
                    ),
                    close=common_close + member_noise,
                    source_evidence_sha256=evidence_sha256,
                )
            )
    return FamilyCCandlePanelV2(
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PROMOTING_PLAN_SHA256,
        source_root_sha256=SOURCE_ROOT_SHA256,
        universe=_universe(),
        current_bar_open_ms=BAR_OPEN_MS,
        current_bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        candles=tuple(reversed(candles)),
    )


def _feature_context(count: int = 20):
    return family_c_module._FamilyCFeatureBuildContextV2(
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PROMOTING_PLAN_SHA256,
        source_root_sha256=SOURCE_ROOT_SHA256,
        universe_root_sha256=_universe(count).universe_root_sha256,
        panel_root_sha256=PANEL_ROOT_SHA256,
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        latest_source_event_ms=BAR_CLOSE_MS,
        latest_source_receipt_ms=DECISION_CUTOFF_MS,
        current_closes=tuple(
            FamilyCSymbolCloseV2(symbol, Decimal(100)) for symbol in _symbols(count)
        ),
    )


def _ready_snapshot(
    *,
    count: int = 20,
    m3: Decimal = Decimal("3"),
    shock_scale: Decimal = Decimal("1.2"),
    lag_scores: tuple[Decimal, ...] | None = None,
    reverse: bool = False,
) -> FamilyCFeatureSnapshotV2:
    symbols = _symbols(count)
    scores = lag_scores or tuple(Decimal("2") for _ in symbols)
    if len(scores) != count:
        raise AssertionError("test fixture score count differs from member count")
    shock_sign = 1 if m3 > 0 else -1 if m3 < 0 else 0
    members: list[FamilyCMemberFeatureV2] = []
    for symbol, score in zip(symbols, scores, strict=True):
        if shock_sign == 0:
            g0 = Decimal(0)
            lag_score = Decimal(0)
            current_return = Decimal(0)
        else:
            g0 = score
            lag_score = score
            current_return = m3 - g0 / Decimal(shock_sign)
        members.append(
            FamilyCMemberFeatureV2(
                symbol=symbol,
                beta_raw=Decimal(1),
                beta=Decimal(1),
                residual_scale=Decimal(1),
                current_three_bar_return=current_return,
                g0=g0,
                lag_score=lag_score,
            )
        )
    ordered_members = tuple(reversed(members)) if reverse else tuple(members)
    member_set = tuple(reversed(symbols)) if reverse else symbols
    breadth_count = sum(
        (shock_sign > 0 and item.current_three_bar_return > 0)
        or (shock_sign < 0 and item.current_three_bar_return < 0)
        for item in ordered_members
    )
    return FamilyCFeatureSnapshotV2(
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PROMOTING_PLAN_SHA256,
        source_root_sha256=SOURCE_ROOT_SHA256,
        universe_root_sha256=_universe(count).universe_root_sha256,
        panel_root_sha256=PANEL_ROOT_SHA256,
        bar_open_ms=BAR_OPEN_MS,
        bar_close_ms=BAR_CLOSE_MS,
        decision_cutoff_ms=DECISION_CUTOFF_MS,
        latest_source_event_ms=BAR_CLOSE_MS,
        latest_source_receipt_ms=DECISION_CUTOFF_MS,
        current_closes=tuple(FamilyCSymbolCloseV2(symbol, Decimal(100)) for symbol in symbols),
        status=FamilyCFeatureStatusV2.READY,
        reasons=("FAMILY_C_FEATURES_READY",),
        member_set=member_set,
        prior_observation_count=FAMILY_C_PRIOR_WINDOW_V2,
        m3_current=m3,
        shock_scale=shock_scale,
        shock_score=abs(m3) / shock_scale,
        breadth_count=breadth_count,
        members=ordered_members,
        _factory_token=family_c_module._FEATURE_FACTORY_TOKEN,
    )


def _entry_input(
    *,
    features: FamilyCFeatureSnapshotV2 | None = None,
    **overrides: object,
) -> FamilyCEntryInputV2:
    values: dict[str, object] = {
        "attempt_id": "attempt-c-1",
        "target_symbol": "S00USDT",
        "venue": VenueV2.USDM_FUTURES,
        "promoting_plan_sha256": PROMOTING_PLAN_SHA256,
        "source_root_sha256": SOURCE_ROOT_SHA256,
        "universe_root_sha256": (features or _ready_snapshot()).universe_root_sha256,
        "bar_open_ms": BAR_OPEN_MS,
        "bar_close_ms": BAR_CLOSE_MS,
        "decision_cutoff_ms": DECISION_CUTOFF_MS,
        "features": features or _ready_snapshot(),
    }
    values.update(overrides)
    return FamilyCEntryInputV2(**values)  # type: ignore[arg-type]


@cache
def _market_history() -> tuple[Decimal, ...]:
    divisor = Decimal(1000)
    return tuple(Decimal(index % 5 - 2) / divisor for index in range(FAMILY_C_PRIOR_WINDOW_V2))


@cache
def _noise_history() -> tuple[Decimal, ...]:
    positive = Decimal("0.0001")
    return tuple(
        positive if index % 10 < 5 else -positive for index in range(FAMILY_C_PRIOR_WINDOW_V2)
    )


@cache
def _raw_histories(kind: str = "ready") -> tuple[FamilyCRawMemberHistoryV2, ...]:
    market = _market_history()
    noise = _noise_history()
    factors = tuple(range(-10, 0)) + tuple(range(1, 11))
    histories: list[FamilyCRawMemberHistoryV2] = []
    for symbol, factor in zip(_symbols(), factors, strict=True):
        factor_decimal = Decimal(factor)
        if kind == "zero_market_variance":
            one_bar = tuple(factor_decimal * value for value in noise)
        elif kind == "zero_residual_mad":
            one_bar = market
        else:
            one_bar = tuple(
                market_value + factor_decimal * noise_value
                for market_value, noise_value in zip(market, noise, strict=True)
            )
        prior_three = tuple(Decimal(0) for _ in market) if kind == "zero_shock_mad" else market
        histories.append(
            FamilyCRawMemberHistoryV2(
                symbol=symbol,
                prior_one_bar_returns=one_bar,
                prior_three_bar_returns=prior_three,
                current_three_bar_return=Decimal("0.004"),
            )
        )
    return tuple(histories)


def _math_features(
    histories: tuple[FamilyCRawMemberHistoryV2, ...],
    *,
    members: tuple[str, ...] | None = None,
) -> FamilyCFeatureSnapshotV2:
    expected = members or _symbols()
    return family_c_module._construct_family_c_math_v2(
        _feature_context(len(expected)),
        expected,
        histories,
    )


def _ledger(*, maximum_events: int = 64) -> FamilyCEpisodeLedgerV2:
    return FamilyCEpisodeLedgerV2(maximum_events=maximum_events)


def _evaluate_entry(
    item: FamilyCEntryInputV2,
    ledger: FamilyCEpisodeLedgerV2 | None = None,
):
    return evaluate_family_c_entry_v2(item, ledger or _ledger())


def _position(
    *,
    m3: Decimal = Decimal("3"),
) -> tuple[FamilyCPositionV2, FamilyCEpisodeLedgerV2]:
    ledger = _ledger()
    item = _entry_input(features=_ready_snapshot(m3=m3))
    decision = evaluate_family_c_entry_v2(item, ledger)
    paper_decision, certificate, paper_registry = _paper_admission(item, decision)
    return (
        position_from_family_c_signal_v2(
            item,
            decision,
            ledger,
            paper_decision=paper_decision,
            certificate=certificate,
            paper_registry=paper_registry,
        ),
        ledger,
    )


def _paper_admission(
    item: FamilyCEntryInputV2,
    decision,  # type: ignore[no-untyped-def]
    **overrides: object,
):  # type: ignore[no-untyped-def]
    values: dict[str, object] = {
        "attempt_id": item.attempt_id,
        "signal_event_id": decision.event_id,
        "symbol": item.target_symbol,
        "promoting_plan_sha256": item.promoting_plan_sha256,
        "bar_open_ms": item.bar_open_ms,
        "bar_close_ms": item.bar_close_ms,
        "decision_cutoff_ms": item.decision_cutoff_ms,
        "side": (
            PaperFokSideV2.BUY if decision.side is FamilyCSideV2.LONG else PaperFokSideV2.SELL
        ),
        "requested_quantity": Decimal("2.00"),
    }
    values.update(overrides)
    return build_usdm_paper_full_fill_v2(**values)  # type: ignore[arg-type]


def _complete_moves(target_move: Decimal = Decimal(0)) -> tuple[FamilyCSymbolMoveV2, ...]:
    moves: list[FamilyCSymbolMoveV2] = []
    for index, symbol in enumerate(_symbols()):
        if index == 0:
            move = target_move
        elif index <= 8:
            move = Decimal(-2)
        elif index <= 11:
            move = Decimal(0)
        else:
            move = Decimal(2)
        moves.append(FamilyCSymbolMoveV2(symbol=symbol, log_move=move))
    return tuple(moves)


def _exit_input(
    *,
    horizon: int = 1,
    position_state: tuple[FamilyCPositionV2, FamilyCEpisodeLedgerV2] | None = None,
    target_move: Decimal = Decimal(0),
    **overrides: object,
) -> tuple[FamilyCExitInputV2, FamilyCEpisodeLedgerV2]:
    frozen_position, ledger = position_state or _position()
    bar_open_ms = BAR_OPEN_MS + horizon * FIVE_MINUTE_MS_V2
    values: dict[str, object] = {
        "position": frozen_position,
        "bar_open_ms": bar_open_ms,
        "bar_close_ms": bar_open_ms + FIVE_MINUTE_MS_V2 - 1,
        "decision_cutoff_ms": (bar_open_ms + FIVE_MINUTE_MS_V2 - 1 + DECISION_DELAY_MS_V2),
        "mandatory_exit": None,
        "member_moves": _complete_moves(target_move),
        "exit_source_root_sha256": EXIT_SOURCE_ROOT_SHA256,
        "latest_source_event_ms": bar_open_ms + FIVE_MINUTE_MS_V2 - 1,
        "latest_source_receipt_ms": (bar_open_ms + FIVE_MINUTE_MS_V2 - 1 + DECISION_DELAY_MS_V2),
        "_factory_token": family_c_module._EXIT_INPUT_FACTORY_TOKEN,
    }
    values.update(overrides)
    return FamilyCExitInputV2(**values), ledger  # type: ignore[arg-type]


def _evaluate_exit(**overrides: object):
    item, ledger = _exit_input(**overrides)  # type: ignore[arg-type]
    return evaluate_family_c_exit_v2(item, ledger)


def _evaluate_at_horizon(
    horizon: int,
    *,
    target_move: Decimal,
    position_state: tuple[FamilyCPositionV2, FamilyCEpisodeLedgerV2] | None = None,
):
    state = position_state or _position()
    decision = None
    for current_horizon in range(1, horizon + 1):
        item, ledger = _exit_input(
            horizon=current_horizon,
            position_state=state,
            target_move=(target_move if current_horizon == horizon else Decimal(0)),
        )
        decision = evaluate_family_c_exit_v2(item, ledger)
    assert decision is not None
    return decision


@pytest.mark.parametrize(
    ("factor", "expected_raw", "expected_clipped"),
    [
        (Decimal(-1), Decimal(-1), Decimal("0.25")),
        (Decimal(0), Decimal(0), Decimal("0.25")),
        (Decimal("0.25"), Decimal("0.25"), Decimal("0.25")),
        (Decimal("2.5"), Decimal("2.5"), Decimal("2.5")),
        (Decimal(3), Decimal(3), Decimal("2.5")),
    ],
)
def test_population_beta_uses_ddof_zero_and_literal_clipping(
    factor: Decimal,
    expected_raw: Decimal,
    expected_clipped: Decimal,
) -> None:
    market = tuple(Decimal(index % 2) for index in range(FAMILY_C_PRIOR_WINDOW_V2))
    asset = tuple(factor * value for value in market)
    result = population_beta_v2(asset, market)
    assert result.variance_pop == Decimal("0.25")
    assert result.covariance_pop == factor * Decimal("0.25")
    assert result.beta_raw == expected_raw
    assert result.beta == expected_clipped


def test_population_beta_requires_exact_8640_and_nonzero_market_variance() -> None:
    market = _market_history()
    with pytest.raises(FamilyCContractError, match="exactly 8,640"):
        population_beta_v2(market[:-1], market[:-1])
    zeros = tuple(Decimal(0) for _ in market)
    with pytest.raises(FamilyCContractError, match="variance is zero"):
        population_beta_v2(zeros, zeros)


def test_raw_feature_construction_is_member_complete_and_permutation_stable() -> None:
    histories = _raw_histories()
    forward = _math_features(histories)
    reverse = _math_features(
        tuple(reversed(histories)),
        members=tuple(reversed(_symbols())),
    )
    assert forward == reverse
    assert forward.status is FamilyCFeatureStatusV2.READY
    assert forward.prior_observation_count == 8_640
    assert forward.member_set == _symbols()
    assert forward.m3_current == Decimal("0.004")
    assert forward.shock_scale == Decimal("0.0014826")
    assert forward.breadth_count == 20
    assert all(member.beta == Decimal(1) for member in forward.members)
    assert all(member.residual_scale > 0 for member in forward.members)
    assert forward.members[0].residual_scale == Decimal("0.0014826")
    assert forward.members[9].residual_scale == Decimal("0.00014826")


def test_raw_feature_history_must_be_exactly_8640_prior_rows() -> None:
    histories = _raw_histories()
    short = (
        replace(histories[0], prior_one_bar_returns=_market_history()[:-1]),
        *histories[1:],
    )
    result = _math_features(short)
    assert result.status is FamilyCFeatureStatusV2.FEATURE_NOT_READY_HISTORY
    assert result.prior_observation_count == FAMILY_C_PRIOR_WINDOW_V2 - 1

    long_history = replace(
        histories[0],
        prior_one_bar_returns=(*histories[0].prior_one_bar_returns, Decimal(0)),
    )
    with pytest.raises(FamilyCContractError, match="exceeds exactly 8,640"):
        _math_features((long_history, *histories[1:]))


def test_missing_entry_member_is_inconclusive_without_survivor_dropping() -> None:
    result = _math_features(_raw_histories()[:-1])
    assert result.status is FamilyCFeatureStatusV2.INCONCLUSIVE_CROSS_SECTION
    assert result.reasons == ("ANY_MISSING_OR_EXTRA_DAILY_ELIGIBLE_MEMBER",)
    assert not result.members


def test_missing_or_nonfinite_member_return_fails_closed() -> None:
    histories = _raw_histories()
    missing_history = replace(
        histories[0],
        prior_one_bar_returns=(None, *histories[0].prior_one_bar_returns[1:]),
    )
    nonfinite_history = replace(
        histories[0],
        current_three_bar_return=Decimal("NaN"),
    )
    missing = _math_features((missing_history, *histories[1:]))
    nonfinite = _math_features((nonfinite_history, *histories[1:]))
    assert missing.status is FamilyCFeatureStatusV2.INCONCLUSIVE_CROSS_SECTION
    assert missing.reasons == ("MISSING_ENTRY_MEMBER_RETURN",)
    assert nonfinite.status is FamilyCFeatureStatusV2.DATA_INVALID


def test_zero_market_variance_has_its_exact_readiness_status() -> None:
    result = _math_features(_raw_histories("zero_market_variance"))
    assert result.status is FamilyCFeatureStatusV2.FEATURE_NOT_READY_ZERO_MARKET_VARIANCE
    assert result.reasons == ("MARKET_VARIANCE_EQ_ZERO",)


def test_zero_residual_mad_has_zero_scale_status() -> None:
    result = _math_features(_raw_histories("zero_residual_mad"))
    assert result.status is FamilyCFeatureStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    assert result.reasons == ("RESIDUAL_MAD_LE_ZERO:S00USDT",)


def test_zero_shock_mad_has_zero_scale_status() -> None:
    result = _math_features(_raw_histories("zero_shock_mad"))
    assert result.status is FamilyCFeatureStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    assert result.reasons == ("SHOCK_MAD_LE_ZERO",)


def test_causal_panel_builds_sealed_exact_prior_feature_evidence() -> None:
    panel = _causal_panel()
    evidence = construct_family_c_features_v2(panel)
    assert evidence.status is FamilyCFeatureStatusV2.READY
    assert evidence.prior_observation_count == FAMILY_C_PRIOR_WINDOW_V2
    assert len(panel.candles) == 20 * FAMILY_C_PANEL_BAR_COUNT_V2
    assert panel.candles[-1].bar_open_ms == BAR_OPEN_MS
    assert evidence.panel_root_sha256 == panel.panel_root_sha256
    assert evidence.universe_root_sha256 == _universe().universe_root_sha256
    assert evidence.latest_source_event_ms == BAR_CLOSE_MS
    assert evidence.latest_source_receipt_ms == DECISION_CUTOFF_MS
    assert len(evidence.feature_evidence_sha256) == 64
    assert canonical_family_c_candle_panel_v2(panel)
    assert canonical_family_c_feature_evidence_v2(evidence)


def test_feature_evidence_is_independent_of_hostile_ambient_decimal_context() -> None:
    panel = _causal_panel()
    baseline = construct_family_c_features_v2(panel)
    original = getcontext().copy()
    try:
        getcontext().prec = 6
        getcontext().rounding = ROUND_DOWN
        hostile = construct_family_c_features_v2(panel)
    finally:
        setcontext(original)

    assert hostile == baseline


def test_prior_only_universe_and_candle_timestamp_equalities_are_literal() -> None:
    universe = _universe()
    assert universe.eligibility_cutoff_ms == universe.effective_day_start_ms - 1
    with pytest.raises(FamilyCContractError, match="strictly prior"):
        replace(
            universe,
            eligibility_cutoff_ms=universe.effective_day_start_ms,
        )
    current = _causal_panel().candles[-1]
    assert current.event_time_ms == current.bar_close_ms
    assert current.receipt_time_ms == DECISION_CUTOFF_MS
    with pytest.raises(FamilyCContractError, match=r"after k\.T"):
        replace(current, event_time_ms=current.bar_close_ms + 1)
    with pytest.raises(FamilyCContractError, match="closed candles only"):
        replace(current, closed=1)  # type: ignore[arg-type]


def test_usdm_plan_and_source_roots_are_bound_across_panel_lineage() -> None:
    universe = _universe()
    with pytest.raises(FamilyCContractError, match="USD-M Futures"):
        FamilyCPriorUniverseV2(
            venue=VenueV2.SPOT,
            promoting_plan_sha256=PROMOTING_PLAN_SHA256,
            source_root_sha256=SOURCE_ROOT_SHA256,
            effective_day_start_ms=universe.effective_day_start_ms,
            eligibility_cutoff_ms=universe.eligibility_cutoff_ms,
            members=universe.members,
        )
    with pytest.raises(FamilyCContractError, match="lineage differs"):
        replace(_causal_panel(), source_root_sha256="e" * 64)


def test_panel_rejects_receipt_after_d_and_missing_universe_member() -> None:
    panel = _causal_panel()
    late = replace(
        panel.candles[-1],
        receipt_time_ms=DECISION_CUTOFF_MS + 1,
    )
    with pytest.raises(FamilyCContractError, match="receipt after D"):
        replace(panel, candles=(*panel.candles[:-1], late))
    with pytest.raises(FamilyCContractError, match="8,644 candles"):
        replace(panel, candles=panel.candles[:-1])


def test_feature_and_decision_factory_seals_reject_direct_reconstruction() -> None:
    evidence = _ready_snapshot()
    with pytest.raises(FamilyCContractError, match="causal factory"):
        replace(evidence, reasons=evidence.reasons)
    decision = _evaluate_entry(_entry_input(features=evidence))
    with pytest.raises(FamilyCContractError, match="created by the evaluator"):
        replace(decision, reasons=decision.reasons)


def test_tampered_feature_hash_fails_before_entry_evaluation() -> None:
    evidence = _ready_snapshot()
    object.__setattr__(evidence, "reasons", ("TAMPERED",))
    with pytest.raises(FamilyCContractError, match="feature evidence hash"):
        _entry_input(features=evidence)


@pytest.mark.parametrize(
    ("member_count", "expected"),
    [
        (1, 1),
        (2, 1),
        (9, 1),
        (10, 1),
        (11, 2),
        (19, 2),
        (20, 2),
        (21, 3),
        (29, 3),
        (30, 3),
        (31, 4),
    ],
)
def test_every_top_decile_count_boundary_has_fixed_k_without_expansion(
    member_count: int,
    expected: int,
) -> None:
    assert family_c_top_decile_count_v2(member_count) == expected


def test_common_shock_gate_accepts_shock_breadth_and_n_equalities() -> None:
    scores = tuple(Decimal(4) for _ in range(6)) + tuple(Decimal(2) for _ in range(14))
    decision = _evaluate_entry(_entry_input(features=_ready_snapshot(lag_scores=scores)))
    assert decision.status is FamilyCEntryStatusV2.SIGNAL
    assert decision.side is FamilyCSideV2.LONG
    assert decision.rule_version == FAMILY_C_RULE_VERSION_V2
    assert decision.selected_rank == 1


def test_each_common_shock_gate_fails_one_quantum_below_boundary() -> None:
    low_shock = _evaluate_entry(_entry_input(features=_ready_snapshot(m3=Decimal("2.99988"))))
    low_breadth_scores = tuple(Decimal(4) for _ in range(7)) + tuple(Decimal(2) for _ in range(13))
    low_breadth = _evaluate_entry(
        _entry_input(features=_ready_snapshot(lag_scores=low_breadth_scores))
    )
    low_n = _evaluate_entry(_entry_input(features=_ready_snapshot(count=19)))
    assert low_shock.reasons == ("SHOCK_SCORE_LT_2_5",)
    assert low_breadth.reasons == ("BREADTH_LT_0_70",)
    assert low_n.reasons == ("MEMBER_COUNT_LT_20",)


def test_m3_zero_is_complete_no_signal_before_gate_failures() -> None:
    decision = _evaluate_entry(_entry_input(features=_ready_snapshot(m3=Decimal(0))))
    assert decision.status is FamilyCEntryStatusV2.NO_SIGNAL
    assert decision.reasons == ("M3_ZERO_COMPLETE_NO_C_SIGNAL",)


def test_lag_score_1_5_equality_accepts_and_one_quantum_below_fails() -> None:
    equality = tuple(Decimal("1.5") for _ in _symbols())
    below = tuple(Decimal("1.4999") for _ in _symbols())
    accepted = _evaluate_entry(_entry_input(features=_ready_snapshot(lag_scores=equality)))
    rejected = _evaluate_entry(_entry_input(features=_ready_snapshot(lag_scores=below)))
    assert accepted.status is FamilyCEntryStatusV2.SIGNAL
    assert rejected.status is FamilyCEntryStatusV2.NO_SIGNAL
    assert rejected.reasons == ("LAG_SCORE_LT_1_5",)


def test_score_ties_use_utf8_order_and_never_expand_fixed_k() -> None:
    snapshot = _ready_snapshot()
    ranked = rank_family_c_members_v2(snapshot)
    assert tuple(member.symbol for member in ranked) == _symbols()
    first = _evaluate_entry(_entry_input(features=snapshot, target_symbol="S00USDT"))
    second = _evaluate_entry(_entry_input(features=snapshot, target_symbol="S01USDT"))
    third = _evaluate_entry(_entry_input(features=snapshot, target_symbol="S02USDT"))
    assert first.status is FamilyCEntryStatusV2.SIGNAL
    assert second.status is FamilyCEntryStatusV2.SIGNAL
    assert third.status is FamilyCEntryStatusV2.NO_SIGNAL
    assert third.selected_rank == 3
    assert third.reasons == ("OUTSIDE_FIXED_TOP_DECILE_K",)


def test_negative_common_shock_emits_short_and_freezes_rule_state_only() -> None:
    item = _entry_input(features=_ready_snapshot(m3=Decimal(-3)))
    ledger = _ledger()
    decision = evaluate_family_c_entry_v2(item, ledger)
    paper_decision, certificate, paper_registry = _paper_admission(item, decision)
    position = position_from_family_c_signal_v2(
        item,
        decision,
        ledger,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    assert decision.status is FamilyCEntryStatusV2.SIGNAL
    assert decision.side is FamilyCSideV2.SHORT
    assert decision.reasons[-1] == "ACTION_SHORT"
    assert position.beta == Decimal(1)
    assert position.m3 == Decimal(-3)
    assert position.r_i3 == Decimal(-1)
    assert position.g0 == Decimal(2)
    assert position.entry_member_set == _symbols()
    assert position.entry_vwap == paper_decision.executable_vwap
    assert position.paper_requested_quantity == position.paper_filled_quantity
    assert position.paper_executable_notional == paper_decision.executable_notional
    assert position.admission_evidence_sha256 == certificate.certificate_sha256
    assert not {"portfolio", "pnl", "production_order"} & {field.name for field in fields(position)}


def test_signal_only_ledgers_decision_until_registry_pinned_full_fill() -> None:
    ledger = _ledger()
    item = _entry_input()
    decision = evaluate_family_c_entry_v2(item, ledger)
    decision_only_root = ledger.root_sha256
    assert decision.status is FamilyCEntryStatusV2.SIGNAL
    assert ledger.event_count == 1
    assert not ledger.is_active(
        promoting_plan_sha256=item.promoting_plan_sha256,
        venue=item.venue,
        symbol=item.target_symbol,
    )

    paper_decision, certificate, paper_registry = _paper_admission(
        item,
        decision,
        requested_quantity=Decimal("0.01"),
    )
    position = position_from_family_c_signal_v2(
        item,
        decision,
        ledger,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    checkpoint = paper_registry.terminal_checkpoint_v2()
    assert ledger.root_sha256 != decision_only_root
    assert ledger.is_active(
        promoting_plan_sha256=item.promoting_plan_sha256,
        venue=item.venue,
        symbol=item.target_symbol,
    )
    assert position.entry_vwap == paper_decision.executable_vwap
    assert position.paper_requested_quantity == Decimal("0.01")
    assert position.paper_filled_quantity == Decimal("0.01")
    assert position.paper_executable_notional == paper_decision.executable_notional
    assert position.paper_decision_event_id == paper_decision.event_id
    assert position.paper_decision_payload_sha256 == paper_decision.payload_sha256
    assert position.admission_evidence_sha256 == certificate.certificate_sha256
    assert position.paper_registry_root_sha256 == checkpoint.replay_root_sha256
    assert position.paper_registry_event_count == checkpoint.event_count
    assert position.paper_registry_checkpoint_sha256 == checkpoint.checkpoint_sha256
    with pytest.raises(FamilyCContractError, match="registry-pinned full PAPER fill"):
        replace(position)


def test_paper_admission_replay_is_idempotent_and_conflicting_fill_fails_closed() -> None:
    ledger = _ledger()
    item = _entry_input()
    decision = evaluate_family_c_entry_v2(item, ledger)
    paper_decision, certificate, paper_registry = _paper_admission(item, decision)
    first = position_from_family_c_signal_v2(
        item,
        decision,
        ledger,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    admitted_root = ledger.root_sha256
    replay = position_from_family_c_signal_v2(
        item,
        decision,
        ledger,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    assert replay is first
    assert ledger.root_sha256 == admitted_root

    other_decision, other_certificate, other_registry = _paper_admission(
        item,
        decision,
        requested_quantity=Decimal("3.00"),
    )
    with pytest.raises(FamilyCContractError, match=r"conflicting.*admission replay"):
        position_from_family_c_signal_v2(
            item,
            decision,
            ledger,
            paper_decision=other_decision,
            certificate=other_certificate,
            paper_registry=other_registry,
        )
    assert ledger.root_sha256 == admitted_root


def test_terminal_admission_replay_is_idempotent_without_reopening_episode() -> None:
    ledger = _ledger()
    item = _entry_input()
    decision = evaluate_family_c_entry_v2(item, ledger)
    paper_decision, certificate, paper_registry = _paper_admission(item, decision)
    position = position_from_family_c_signal_v2(
        item,
        decision,
        ledger,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    exit_input, _ = _exit_input(
        position_state=(position, ledger),
        mandatory_exit=FamilyCMandatoryExitV2.DATA,
    )
    terminal = evaluate_family_c_exit_v2(exit_input, ledger)
    assert terminal.exits_position
    assert not ledger.is_active(
        promoting_plan_sha256=item.promoting_plan_sha256,
        venue=item.venue,
        symbol=item.target_symbol,
    )
    root_after_exit = ledger.root_sha256
    replay = position_from_family_c_signal_v2(
        item,
        decision,
        ledger,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    assert replay is position
    assert ledger.root_sha256 == root_after_exit
    assert not ledger.is_active(
        promoting_plan_sha256=item.promoting_plan_sha256,
        venue=item.venue,
        symbol=item.target_symbol,
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"attempt_id": "wrong-attempt"},
        {"signal_event_id": hashlib.sha256(b"wrong-family-c-signal").hexdigest()},
        {"symbol": "S01USDT"},
        {"promoting_plan_sha256": "f" * 64},
        {"side": PaperFokSideV2.SELL},
        {
            "bar_open_ms": BAR_OPEN_MS + FIVE_MINUTE_MS_V2,
            "bar_close_ms": BAR_CLOSE_MS + FIVE_MINUTE_MS_V2,
            "decision_cutoff_ms": DECISION_CUTOFF_MS + FIVE_MINUTE_MS_V2,
        },
    ],
)
def test_paper_admission_requires_exact_signal_identity(
    overrides: dict[str, object],
) -> None:
    ledger = _ledger()
    item = _entry_input()
    decision = evaluate_family_c_entry_v2(item, ledger)
    paper_decision, certificate, paper_registry = _paper_admission(
        item,
        decision,
        **overrides,
    )
    with pytest.raises(FamilyCContractError, match="identity differs"):
        position_from_family_c_signal_v2(
            item,
            decision,
            ledger,
            paper_decision=paper_decision,
            certificate=certificate,
            paper_registry=paper_registry,
        )
    assert not ledger.is_active(
        promoting_plan_sha256=item.promoting_plan_sha256,
        venue=item.venue,
        symbol=item.target_symbol,
    )


def test_paper_admission_requires_concrete_registry_and_rejects_other_active() -> None:
    ledger = _ledger()
    first_item = _entry_input(attempt_id="first-family-c-admission")
    second_item = _entry_input(attempt_id="second-family-c-admission")
    first_decision = evaluate_family_c_entry_v2(first_item, ledger)
    second_decision = evaluate_family_c_entry_v2(second_item, ledger)
    first_paper, first_certificate, first_registry = _paper_admission(
        first_item,
        first_decision,
    )
    empty_registry = PaperFokDecisionRegistryV2(
        maximum_events=4,
        attempt_id=first_item.attempt_id,
        promoting_plan_sha256=first_item.promoting_plan_sha256,
    )
    with pytest.raises(FamilyCContractError, match="absent from its registry"):
        position_from_family_c_signal_v2(
            first_item,
            first_decision,
            ledger,
            paper_decision=first_paper,
            certificate=first_certificate,
            paper_registry=empty_registry,
        )
    position_from_family_c_signal_v2(
        first_item,
        first_decision,
        ledger,
        paper_decision=first_paper,
        certificate=first_certificate,
        paper_registry=first_registry,
    )
    second_paper, second_certificate, second_registry = _paper_admission(
        second_item,
        second_decision,
    )
    with pytest.raises(FamilyCContractError, match="another Family C position"):
        position_from_family_c_signal_v2(
            second_item,
            second_decision,
            ledger,
            paper_decision=second_paper,
            certificate=second_certificate,
            paper_registry=second_registry,
        )


def test_admission_provenance_changes_position_hash_and_episode_root() -> None:
    position, ledger = _position()
    original_root = ledger.root_sha256
    object.__setattr__(position, "admission_evidence_sha256", "f" * 64)
    assert ledger.root_sha256 != original_root
    item, ledger = _exit_input(position_state=(position, ledger))
    with pytest.raises(FamilyCContractError, match="position differs"):
        evaluate_family_c_exit_v2(item, ledger)


def test_entry_is_permutation_deterministic_with_stable_event_identity() -> None:
    forward = _evaluate_entry(_entry_input(features=_ready_snapshot()))
    reverse = _evaluate_entry(_entry_input(features=_ready_snapshot(reverse=True)))
    altered = _evaluate_entry(
        _entry_input(features=_ready_snapshot(lag_scores=tuple(Decimal("1.5") for _ in _symbols())))
    )
    other_attempt = _evaluate_entry(_entry_input(attempt_id="attempt-c-2"))
    assert forward == reverse
    assert forward.event_id == altered.event_id
    assert forward.event_id != other_attempt.event_id
    assert len(forward.event_id) == 64
    assert forward.reasons
    assert forward.invalidation


def test_entry_ledger_is_idempotent_and_rejects_same_id_conflicting_input() -> None:
    ledger = _ledger()
    item = _entry_input()
    first = evaluate_family_c_entry_v2(item, ledger)
    duplicate = evaluate_family_c_entry_v2(item, ledger)
    conflicting_item = _entry_input(
        features=_ready_snapshot(lag_scores=tuple(Decimal("1.5") for _ in _symbols()))
    )
    assert duplicate is first
    assert ledger.event_count == 1
    with pytest.raises(FamilyCContractError, match="conflicting causal input"):
        evaluate_family_c_entry_v2(conflicting_item, ledger)


def test_entry_preview_commit_is_non_mutating_pre_root_bound_and_idempotent() -> None:
    ledger = _ledger(maximum_events=2)
    item = _entry_input()
    root = ledger.root_sha256
    preview = ledger.preview_entry(item)

    assert preview.pre_root_sha256 == root
    assert preview.pre_event_count == 0
    assert preview.decision.episode_ledger_root_sha256 == root
    assert not preview.already_committed
    assert ledger.root_sha256 == root
    assert ledger.event_count == 0
    receipt = ledger.commit_entry_preview_with_receipt(item, preview)
    assert receipt.decision == preview.decision
    assert receipt.input_sha256 == preview.input_sha256
    assert receipt.event_id == preview.decision.event_id
    assert receipt.pre_root_sha256 == preview.pre_root_sha256
    assert receipt.pre_event_count == preview.pre_event_count
    assert receipt.disposition is FamilyCEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION
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
    assert replay_receipt.disposition is FamilyCEntryCommitDispositionV2.PREEXISTING
    assert replay_receipt.pre_root_sha256 == replay_receipt.post_root_sha256
    assert replay_receipt.pre_event_count == replay_receipt.post_event_count == 1
    with pytest.raises(FamilyCContractError, match="pre-existing"):
        ledger.rollback_entry_preview(item, replay, replay_receipt)
    with pytest.raises(FamilyCContractError, match="created by the ledger"):
        FamilyCEntryPreviewV2(
            input_sha256=preview.input_sha256,
            pre_root_sha256=preview.pre_root_sha256,
            pre_event_count=preview.pre_event_count,
            decision=preview.decision,
            already_committed=preview.already_committed,
        )
    with pytest.raises(FamilyCContractError, match="created by the ledger"):
        FamilyCEntryCommitReceiptV2(
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


def test_entry_preview_rejects_conflict_capacity_and_owner_state_drift() -> None:
    item = _entry_input()
    conflicting = _entry_input(
        features=_ready_snapshot(lag_scores=tuple(Decimal("1.5") for _ in _symbols()))
    )
    conflict_ledger = _ledger(maximum_events=2)
    preview = conflict_ledger.preview_entry(item)
    evaluate_family_c_entry_v2(conflicting, conflict_ledger)
    with pytest.raises(FamilyCContractError, match="conflicts with committed input"):
        conflict_ledger.commit_entry_preview(item, preview)

    drift_ledger = _ledger(maximum_events=2)
    drift_preview = drift_ledger.preview_entry(item)
    evaluate_family_c_entry_v2(
        _entry_input(attempt_id="family-c-preview-state-drift"),
        drift_ledger,
    )
    with pytest.raises(FamilyCContractError, match="state drifted"):
        drift_ledger.commit_entry_preview(item, drift_preview)
    with pytest.raises(FamilyCContractError, match="differs from exact input"):
        _ledger(maximum_events=2).commit_entry_preview(conflicting, preview)

    capacity_ledger = _ledger(maximum_events=1)
    evaluate_family_c_entry_v2(item, capacity_ledger)
    assert capacity_ledger.preview_entry(item).already_committed
    with pytest.raises(FamilyCContractError, match="capacity exhausted"):
        capacity_ledger.preview_entry(_entry_input(attempt_id="family-c-preview-capacity"))


def test_entry_preview_rollback_restores_only_untouched_entry_state() -> None:
    ledger = _ledger(maximum_events=4)
    item = _entry_input()
    preview = ledger.preview_entry(item)
    receipt = ledger.commit_entry_preview_with_receipt(item, preview)
    assert ledger.rollback_entry_preview(item, preview, receipt)
    assert ledger.root_sha256 == preview.pre_root_sha256
    assert ledger.event_count == preview.pre_event_count
    with pytest.raises(FamilyCContractError, match="does not own"):
        ledger.rollback_entry_preview(item, preview, receipt)

    recommit = ledger.commit_entry_preview_with_receipt(item, preview)
    with pytest.raises(FamilyCContractError, match="does not own"):
        ledger.rollback_entry_preview(item, preview, receipt)
    assert ledger.event_count == 1
    assert ledger.rollback_entry_preview(item, preview, recommit)

    admitted = _ledger(maximum_events=4)
    admitted_preview = admitted.preview_entry(item)
    admitted_receipt = admitted.commit_entry_preview_with_receipt(item, admitted_preview)
    signal = admitted_receipt.decision
    paper_decision, certificate, paper_registry = _paper_admission(item, signal)
    position_from_family_c_signal_v2(
        item,
        signal,
        admitted,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    admitted_root = admitted.root_sha256
    with pytest.raises(FamilyCContractError, match="state drifted"):
        admitted.rollback_entry_preview(item, admitted_preview, admitted_receipt)
    assert admitted.root_sha256 == admitted_root


def test_identical_concurrent_entry_commits_issue_only_one_rollback_capability() -> None:
    ledger = _ledger(maximum_events=2)
    item = _entry_input()
    preview = ledger.preview_entry(item)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = tuple(
            pool.submit(ledger.commit_entry_preview_with_receipt, item, preview) for _ in range(2)
        )
        receipts = tuple(future.result() for future in futures)

    by_disposition = {receipt.disposition: receipt for receipt in receipts}
    assert set(by_disposition) == {
        FamilyCEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION,
        FamilyCEntryCommitDispositionV2.PREEXISTING,
    }
    preexisting = by_disposition[FamilyCEntryCommitDispositionV2.PREEXISTING]
    with pytest.raises(FamilyCContractError, match="pre-existing"):
        ledger.rollback_entry_preview(item, preview, preexisting)
    assert ledger.event_count == 1

    foreign = _ledger(maximum_events=2)
    foreign_preview = foreign.preview_entry(item)
    foreign_receipt = foreign.commit_entry_preview_with_receipt(item, foreign_preview)
    with pytest.raises(FamilyCContractError, match="another ledger"):
        ledger.rollback_entry_preview(item, preview, foreign_receipt)
    assert ledger.event_count == 1

    created = by_disposition[FamilyCEntryCommitDispositionV2.NEW_BY_THIS_TRANSACTION]
    assert ledger.rollback_entry_preview(item, preview, created)


def test_prospective_authority_gates_every_family_c_mutation_surface() -> None:
    ledger = _ledger(maximum_events=4)
    item = _entry_input()
    preview = ledger.preview_entry(item)
    authority = ledger._claim_prospective_decision_authority_v2()  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(FamilyCContractError, match="held prospective decision authority"):
        ledger.evaluate_entry(item)
    with pytest.raises(FamilyCContractError, match="held prospective decision authority"):
        ledger.commit_entry_preview(item, preview)
    with pytest.raises(FamilyCContractError, match="held prospective decision authority"):
        ledger.admit_position(
            item,
            preview.decision,
            paper_decision=cast(PaperFokEntryDecisionV2, object()),
            certificate=cast(PaperFokFullFillCertificateV2, object()),
            paper_registry=cast(PaperFokDecisionRegistryV2, object()),
        )
    with pytest.raises(FamilyCContractError, match="held prospective decision authority"):
        ledger.evaluate_exit(cast(FamilyCExitInputV2, object()))

    receipt = ledger.commit_entry_preview_with_receipt(
        item,
        preview,
        _prospective_authority=authority,
    )
    with pytest.raises(FamilyCContractError, match="cannot release a non-genesis"):
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


def test_family_c_prospective_authority_rejects_prepopulated_ledger() -> None:
    ledger = _ledger(maximum_events=4)
    ledger.evaluate_entry(_entry_input())

    with pytest.raises(FamilyCContractError, match="requires exact genesis state"):
        ledger._claim_prospective_decision_authority_v2()  # pyright: ignore[reportPrivateUsage]


def test_entry_decision_canonical_parser_rederives_and_rejects_shape_drift() -> None:
    decision = _evaluate_entry(_entry_input())
    payload = canonical_family_c_entry_decision_v2(decision)
    assert parse_canonical_family_c_entry_decision_v2(payload) == decision

    document = json.loads(payload)
    document["unexpected"] = True
    with pytest.raises(FamilyCContractError, match="fields are not exact"):
        parse_canonical_family_c_entry_decision_v2(canonical_json_line(document))
    with pytest.raises(FamilyCContractError, match="canonical JSONL"):
        parse_canonical_family_c_entry_decision_v2(payload.rstrip())


def test_episode_checkpoint_empty_and_active_sticky_state_replay_exactly() -> None:
    empty = _ledger(maximum_events=1)
    empty_payload = empty.export_state_v2()
    restored_empty = FamilyCEpisodeLedgerV2.restore_state_v2(
        empty_payload,
        maximum_events=empty.maximum_events,
        expected_event_count=empty.event_count,
        expected_root_sha256=empty.root_sha256,
    )
    assert restored_empty.export_state_v2() == empty_payload

    position, ledger = _position()
    missing_h1, ledger = _exit_input(
        position_state=(position, ledger),
        member_moves=_complete_moves()[:-1],
    )
    first_exit = evaluate_family_c_exit_v2(missing_h1, ledger)
    assert first_exit.reason is FamilyCExitReasonV2.MISSING_MEMBER_INCONCLUSIVE
    payload = ledger.export_state_v2()
    restored = FamilyCEpisodeLedgerV2.restore_state_v2(
        payload,
        maximum_events=ledger.maximum_events,
        expected_event_count=ledger.event_count,
        expected_root_sha256=ledger.root_sha256,
    )
    assert restored.export_state_v2() == payload
    assert restored.is_active(
        promoting_plan_sha256=position.promoting_plan_sha256,
        venue=position.venue,
        symbol=position.symbol,
    )

    h2, _ = _exit_input(
        horizon=2,
        position_state=(position, restored),
        target_move=Decimal(0),
    )
    continued = evaluate_family_c_exit_v2(h2, restored)
    assert continued.action is FamilyCExitActionV2.HOLD
    assert continued.interval_status is FamilyCIntervalStatusV2.INCONCLUSIVE_DATA


def test_episode_checkpoint_terminal_replay_stays_closed_and_allows_reentry() -> None:
    position, ledger = _position()
    exit_input, ledger = _exit_input(
        position_state=(position, ledger),
        mandatory_exit=FamilyCMandatoryExitV2.TERMINAL,
    )
    terminal = evaluate_family_c_exit_v2(exit_input, ledger)
    assert terminal.exits_position
    payload = ledger.export_state_v2()
    restored = FamilyCEpisodeLedgerV2.restore_state_v2(
        payload,
        maximum_events=ledger.maximum_events,
        expected_event_count=ledger.event_count,
        expected_root_sha256=ledger.root_sha256,
    )
    assert restored.export_state_v2() == payload
    assert not restored.is_active(
        promoting_plan_sha256=position.promoting_plan_sha256,
        venue=position.venue,
        symbol=position.symbol,
    )
    reentry = evaluate_family_c_entry_v2(
        _entry_input(attempt_id="checkpoint-terminal-reentry"),
        restored,
    )
    assert reentry.status is FamilyCEntryStatusV2.SIGNAL


def test_episode_checkpoint_preserves_unobservable_sticky_on_mandatory_exit() -> None:
    position, ledger = _position()
    exit_input, ledger = _exit_input(
        position_state=(position, ledger),
        mandatory_exit=FamilyCMandatoryExitV2.DATA,
        member_moves=_complete_moves()[:-1],
    )
    terminal = evaluate_family_c_exit_v2(exit_input, ledger)
    assert terminal.reason is FamilyCExitReasonV2.MANDATORY_DATA_EMERGENCY
    payload = ledger.export_state_v2()
    restored = FamilyCEpisodeLedgerV2.restore_state_v2(
        payload,
        maximum_events=ledger.maximum_events,
        expected_event_count=ledger.event_count,
        expected_root_sha256=ledger.root_sha256,
    )
    assert restored.export_state_v2() == payload


def test_episode_checkpoint_requires_exact_external_pins_and_capacity_boundary() -> None:
    ledger = _ledger(maximum_events=1)
    evaluate_family_c_entry_v2(_entry_input(), ledger)
    payload = ledger.export_state_v2()
    with pytest.raises(FamilyCContractError, match="capacity differs"):
        FamilyCEpisodeLedgerV2.restore_state_v2(
            payload,
            maximum_events=2,
            expected_event_count=ledger.event_count,
            expected_root_sha256=ledger.root_sha256,
        )
    with pytest.raises(FamilyCContractError, match="event count differs"):
        FamilyCEpisodeLedgerV2.restore_state_v2(
            payload,
            maximum_events=1,
            expected_event_count=0,
            expected_root_sha256=ledger.root_sha256,
        )
    with pytest.raises(FamilyCContractError, match="root differs"):
        FamilyCEpisodeLedgerV2.restore_state_v2(
            payload,
            maximum_events=1,
            expected_event_count=ledger.event_count,
            expected_root_sha256="f" * 64,
        )
    restored = FamilyCEpisodeLedgerV2.restore_state_v2(
        payload,
        maximum_events=1,
        expected_event_count=ledger.event_count,
        expected_root_sha256=ledger.root_sha256,
    )
    with pytest.raises(FamilyCContractError, match="capacity exhausted"):
        evaluate_family_c_entry_v2(
            _entry_input(attempt_id="checkpoint-capacity-boundary"),
            restored,
        )


def test_episode_checkpoint_rejects_unknown_and_corrupt_nested_decision() -> None:
    ledger = _ledger()
    evaluate_family_c_entry_v2(_entry_input(), ledger)
    payload = ledger.export_state_v2()
    document = json.loads(payload)
    document["unknown"] = "forbidden"
    with pytest.raises(FamilyCContractError, match="schema is unsupported"):
        FamilyCEpisodeLedgerV2.restore_state_v2(
            canonical_json_line(document),
            maximum_events=ledger.maximum_events,
            expected_event_count=ledger.event_count,
            expected_root_sha256=ledger.root_sha256,
        )

    document = json.loads(payload)
    inner = json.loads(document["entries"][0]["canonical_decision"])
    inner["unknown"] = "forbidden"
    document["entries"][0]["canonical_decision"] = canonical_json_line(inner).decode("utf-8")
    with pytest.raises(FamilyCContractError, match="fields are not exact"):
        FamilyCEpisodeLedgerV2.restore_state_v2(
            canonical_json_line(document),
            maximum_events=ledger.maximum_events,
            expected_event_count=ledger.event_count,
            expected_root_sha256=ledger.root_sha256,
        )


@pytest.mark.parametrize(
    "mutation",
    ["active", "position", "horizon", "terminal", "sticky"],
)
def test_episode_checkpoint_rejects_corrupt_semantic_state(mutation: str) -> None:
    position, ledger = _position()
    exit_input, ledger = _exit_input(
        position_state=(position, ledger),
        member_moves=_complete_moves()[:-1],
    )
    evaluate_family_c_exit_v2(exit_input, ledger)
    document = json.loads(ledger.export_state_v2())
    if mutation == "active":
        document["active"] = []
    elif mutation == "position":
        document["episodes"][0]["position"]["attempt_id"] = "other-attempt"
    elif mutation == "horizon":
        document["episodes"][0]["next_horizon"] = 3
    elif mutation == "terminal":
        document["episodes"][0]["terminal"] = True
    else:
        document["episodes"][0]["sticky_inconclusive"] = False
    with pytest.raises(FamilyCContractError):
        FamilyCEpisodeLedgerV2.restore_state_v2(
            canonical_json_line(document),
            maximum_events=ledger.maximum_events,
            expected_event_count=ledger.event_count,
            expected_root_sha256=ledger.root_sha256,
        )


def test_decision_registry_handles_duplicate_conflict_and_capacity() -> None:
    item = _entry_input()
    signal = _evaluate_entry(item)
    active_ledger = _ledger()
    preexisting_item = _entry_input(attempt_id="preexisting-position")
    preexisting = evaluate_family_c_entry_v2(preexisting_item, active_ledger)
    paper_decision, certificate, paper_registry = _paper_admission(
        preexisting_item,
        preexisting,
    )
    position_from_family_c_signal_v2(
        preexisting_item,
        preexisting,
        active_ledger,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    conflicting = evaluate_family_c_entry_v2(item, active_ledger)
    assert signal.event_id == conflicting.event_id
    assert signal.payload_sha256 != conflicting.payload_sha256

    registry = FamilyCDecisionRegistryV2(maximum_events=2)
    assert registry.register(signal) is FamilyCRegistryDispositionV2.NEW
    assert registry.register(signal) is FamilyCRegistryDispositionV2.IDEMPOTENT_DUPLICATE
    with pytest.raises(FamilyCContractError, match="collides"):
        registry.register(conflicting)

    bounded = FamilyCDecisionRegistryV2(maximum_events=1)
    bounded.register(signal)
    other = _evaluate_entry(_entry_input(attempt_id="another-event"))
    with pytest.raises(FamilyCContractError, match="capacity exhausted"):
        bounded.register(other)


def test_entry_decision_payload_and_direct_state_invariants_are_sealed() -> None:
    decision = _evaluate_entry(_entry_input())
    assert canonical_family_c_entry_decision_v2(decision)
    assert len(decision.payload_sha256) == 64
    with pytest.raises(FamilyCContractError, match="SIGNAL requires"):
        replace(
            decision,
            side=None,
            _factory_token=family_c_module._DECISION_FACTORY_TOKEN,
        )


def test_missing_target_and_ledger_derived_active_signal_fail_closed() -> None:
    target_missing = _evaluate_entry(_entry_input(target_symbol="ZZZUSDT"))
    ledger = _ledger()
    admitted_item = _entry_input()
    admitted = evaluate_family_c_entry_v2(admitted_item, ledger)
    assert not ledger.is_active(
        promoting_plan_sha256=admitted_item.promoting_plan_sha256,
        venue=admitted_item.venue,
        symbol=admitted_item.target_symbol,
    )
    paper_decision, certificate, paper_registry = _paper_admission(
        admitted_item,
        admitted,
    )
    position_from_family_c_signal_v2(
        admitted_item,
        admitted,
        ledger,
        paper_decision=paper_decision,
        certificate=certificate,
        paper_registry=paper_registry,
    )
    active = evaluate_family_c_entry_v2(
        _entry_input(attempt_id="attempt-c-active-2"),
        ledger,
    )
    assert target_missing.status is FamilyCEntryStatusV2.INCONCLUSIVE_CROSS_SECTION
    assert admitted.status is FamilyCEntryStatusV2.SIGNAL
    assert active.status is FamilyCEntryStatusV2.NOT_ADMITTED_ACTIVE_POSITION
    assert active.invalidation == "ACTIVE_POSITION_UNCHANGED"


def test_decision_clock_contract_is_reused_for_entry() -> None:
    with pytest.raises(FamilyCContractError, match="cutoff"):
        _entry_input(decision_cutoff_ms=DECISION_CUTOFF_MS - 1)


def test_raw_log_move_construction_is_separate_and_member_order_deterministic() -> None:
    moves = construct_family_c_log_moves_v2(
        (
            FamilyCSymbolCloseV2("S01USDT", Decimal(100)),
            FamilyCSymbolCloseV2("S00USDT", Decimal(100)),
        ),
        (FamilyCSymbolCloseV2("S00USDT", Decimal(200)),),
    )
    assert tuple(move.symbol for move in moves) == ("S00USDT",)
    assert abs(moves[0].log_move.exp() - Decimal(2)) < Decimal("1e-25")


def test_causal_exit_candle_factory_seals_closed_member_moves() -> None:
    position, ledger = _position()
    bar_open_ms = BAR_OPEN_MS + FIVE_MINUTE_MS_V2
    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    decision_cutoff_ms = bar_close_ms + DECISION_DELAY_MS_V2
    candles = tuple(
        FamilyCClosedCandleV2(
            symbol=item.symbol,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            event_time_ms=bar_close_ms,
            receipt_time_ms=decision_cutoff_ms,
            close=item.close,
            source_evidence_sha256=SOURCE_ROOT_SHA256,
        )
        for item in reversed(position.entry_member_closes)
    )
    exit_input = build_family_c_exit_input_v2(
        position=position,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        candles=candles,
    )
    decision = evaluate_family_c_exit_v2(exit_input, ledger)
    assert decision.action is FamilyCExitActionV2.HOLD
    assert decision.asset_move == Decimal(0)
    assert len(exit_input.causal_input_sha256) == 64
    assert canonical_family_c_exit_decision_v2(decision)
    with pytest.raises(FamilyCContractError, match="causal candle factory"):
        replace(exit_input, member_moves=exit_input.member_moves)


def test_exit_ledger_duplicate_is_idempotent_and_conflict_fails_closed() -> None:
    state = _position()
    item, ledger = _exit_input(position_state=state)
    first = evaluate_family_c_exit_v2(item, ledger)
    duplicate = evaluate_family_c_exit_v2(item, ledger)
    conflicting, ledger = _exit_input(
        position_state=state,
        target_move=EPSILON,
    )
    assert duplicate is first
    with pytest.raises(FamilyCContractError, match="conflicting causal input"):
        evaluate_family_c_exit_v2(conflicting, ledger)


def test_exit_ledger_detects_mutated_frozen_position_payload() -> None:
    state = _position()
    item, ledger = _exit_input(position_state=state)
    object.__setattr__(state[0], "beta", Decimal(2))
    with pytest.raises(FamilyCContractError, match="causal input hash"):
        evaluate_family_c_exit_v2(item, ledger)


def test_exit_decision_registry_payload_and_direct_invariants() -> None:
    decision = _evaluate_exit(target_move=Decimal(-1))
    registry = FamilyCDecisionRegistryV2(maximum_events=1)
    assert registry.register(decision) is FamilyCRegistryDispositionV2.NEW
    assert registry.register(decision) is FamilyCRegistryDispositionV2.IDEMPOTENT_DUPLICATE
    assert canonical_family_c_exit_decision_v2(decision)
    with pytest.raises(FamilyCContractError, match="HOLD action and reason"):
        replace(
            decision,
            action=FamilyCExitActionV2.HOLD,
            _factory_token=family_c_module._DECISION_FACTORY_TOKEN,
        )


def test_adverse_and_catchup_exit_equalities_trigger_but_nearby_values_hold() -> None:
    adverse = _evaluate_exit(target_move=Decimal(-1))
    adverse_hold = _evaluate_exit(target_move=Decimal(-1) + EPSILON)
    catchup = _evaluate_exit(target_move=Decimal("1.5"))
    catchup_hold = _evaluate_exit(target_move=Decimal("1.5") - EPSILON)
    assert adverse.reason is FamilyCExitReasonV2.ADVERSE_WIDENING
    assert adverse.catch_h == Decimal(-1)
    assert catchup.reason is FamilyCExitReasonV2.CATCHUP_COMPLETE
    assert catchup.catch_h == Decimal("1.5")
    assert adverse_hold.action is FamilyCExitActionV2.HOLD
    assert catchup_hold.action is FamilyCExitActionV2.HOLD


@pytest.mark.parametrize(
    ("target_move", "expected_reason"),
    [
        (Decimal(-1), FamilyCExitReasonV2.ADVERSE_WIDENING),
        (Decimal("1.5"), FamilyCExitReasonV2.CATCHUP_COMPLETE),
        (Decimal(0), FamilyCExitReasonV2.HARD_HORIZON),
    ],
)
def test_bar_t_plus_6_obeys_adverse_then_catchup_then_hard_priority(
    target_move: Decimal,
    expected_reason: FamilyCExitReasonV2,
) -> None:
    decision = _evaluate_at_horizon(
        FAMILY_C_HARD_HORIZON_BARS_V2,
        target_move=target_move,
    )
    assert decision.reason is expected_reason
    assert decision.exits_position


def test_missing_entry_mask_member_skips_early_exit_but_retains_hard_exit() -> None:
    missing_moves = _complete_moves(Decimal(-2))[:-1]
    state = _position()
    early_item, ledger = _exit_input(
        horizon=1,
        position_state=state,
        member_moves=missing_moves,
    )
    early = evaluate_family_c_exit_v2(early_item, ledger)
    for horizon in range(2, 6):
        hold_item, ledger = _exit_input(
            horizon=horizon,
            position_state=state,
            target_move=Decimal(0),
        )
        hold = evaluate_family_c_exit_v2(hold_item, ledger)
        assert hold.action is FamilyCExitActionV2.HOLD
        assert hold.interval_status is FamilyCIntervalStatusV2.INCONCLUSIVE_DATA
    hard_item, ledger = _exit_input(
        horizon=6,
        position_state=state,
        member_moves=missing_moves,
    )
    hard = evaluate_family_c_exit_v2(hard_item, ledger)
    assert early.action is FamilyCExitActionV2.HOLD
    assert early.reason is FamilyCExitReasonV2.MISSING_MEMBER_INCONCLUSIVE
    assert early.catch_h is None
    assert early.interval_status is FamilyCIntervalStatusV2.INCONCLUSIVE_DATA
    assert hard.reason is FamilyCExitReasonV2.HARD_HORIZON
    assert hard.exits_position
    assert hard.interval_status is FamilyCIntervalStatusV2.INCONCLUSIVE_DATA


def test_exit_member_permutation_is_deterministic_and_uses_frozen_entry_set() -> None:
    moves = _complete_moves(Decimal("1.5"))
    forward = _evaluate_exit(member_moves=moves)
    reverse = _evaluate_exit(member_moves=tuple(reversed(moves)))
    different_moves = _evaluate_exit(target_move=Decimal(0))
    assert forward == reverse
    assert forward.event_id == different_moves.event_id
    assert forward.market_move == Decimal(0)
    assert forward.rule_version == FAMILY_C_RULE_VERSION_V2


def test_short_exit_uses_negative_m3_sign_and_exit_short_action() -> None:
    decision = _evaluate_exit(
        position_state=_position(m3=Decimal(-3)),
        target_move=Decimal("-1.5"),
    )
    assert decision.catch_h == Decimal("1.5")
    assert decision.reason is FamilyCExitReasonV2.CATCHUP_COMPLETE
    assert decision.action is FamilyCExitActionV2.EXIT_SHORT


def test_terminal_exit_releases_ledger_active_suppression() -> None:
    state = _position()
    position, ledger = state
    assert ledger.is_active(
        promoting_plan_sha256=PROMOTING_PLAN_SHA256,
        venue=VenueV2.USDM_FUTURES,
        symbol=position.symbol,
    )
    exit_item, ledger = _exit_input(
        position_state=state,
        target_move=Decimal(-1),
    )
    evaluate_family_c_exit_v2(exit_item, ledger)
    assert not ledger.is_active(
        promoting_plan_sha256=PROMOTING_PLAN_SHA256,
        venue=VenueV2.USDM_FUTURES,
        symbol=position.symbol,
    )
    reentry = evaluate_family_c_entry_v2(
        _entry_input(attempt_id="reentry-after-terminal"),
        ledger,
    )
    assert reentry.status is FamilyCEntryStatusV2.SIGNAL


@pytest.mark.parametrize(
    ("mandatory", "expected_reason"),
    [
        (
            FamilyCMandatoryExitV2.DATA,
            FamilyCExitReasonV2.MANDATORY_DATA_EMERGENCY,
        ),
        (
            FamilyCMandatoryExitV2.TERMINAL,
            FamilyCExitReasonV2.MANDATORY_TERMINAL_EMERGENCY,
        ),
    ],
)
def test_mandatory_exit_has_priority_over_rule_exit(
    mandatory: FamilyCMandatoryExitV2,
    expected_reason: FamilyCExitReasonV2,
) -> None:
    decision = _evaluate_exit(
        target_move=Decimal(-2),
        mandatory_exit=mandatory,
    )
    assert decision.reason is expected_reason
    assert decision.exits_position


def test_exit_ledger_rejects_skipped_and_out_of_range_horizons() -> None:
    skipped_item, skipped_ledger = _exit_input(horizon=2)
    with pytest.raises(FamilyCContractError, match=r"expected.*h=1"):
        evaluate_family_c_exit_v2(skipped_item, skipped_ledger)
    overdue_item, overdue_ledger = _exit_input(horizon=7)
    with pytest.raises(FamilyCContractError, match=r"h=1\.\.6"):
        evaluate_family_c_exit_v2(overdue_item, overdue_ledger)
