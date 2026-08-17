from decimal import Decimal

import pytest

from conftest import make_candle, make_decision, make_feature
from signalbot.backtest.config import ExitPolicySettings
from signalbot.backtest.engine import FundingRate
from signalbot.backtest.outcomes import (
    CounterfactualTechnicalExitEvaluator,
    CounterfactualTechnicalExitExclusion,
    CounterfactualTechnicalExitOutcome,
    DirectionalHitStatus,
    OneRPathStatus,
    OutcomeEvaluator,
    RecommendationOutcome,
    RecommendationOutcomeEvaluator,
    RecommendationOutcomeExclusion,
    RecommendationOutcomeExclusionReason,
    TechnicalExitOutcomeExclusionReason,
    classify_directional_hit,
    evaluate_one_r_path,
)
from signalbot.domain.enums import Direction, Market, SignalStage
from signalbot.domain.models import Candle, FeatureSnapshot
from signalbot.signals.positions import ExitReason


def test_outcome_uses_only_candles_after_signal_and_within_horizon() -> None:
    decision = make_decision(event_time_ms=600_000, price="100")
    before = make_candle(1, close=200)
    first = make_candle(3, close=105).model_copy(update={"high": 110, "low": 95})
    second = make_candle(4, close=98).model_copy(update={"high": 106, "low": 90})
    outside = make_candle(8, close=150)
    outcome = OutcomeEvaluator().evaluate(decision, [before, first, second, outside], 900)
    assert outcome is not None
    assert outcome.mfe == pytest.approx(0.10)
    assert outcome.mae == pytest.approx(-0.10)
    assert outcome.close_return == pytest.approx(-0.02)
    assert outcome.observed_until_ms == second.close_time_ms


def _priced_candle(
    index: int,
    *,
    open_price: str,
    high: str,
    low: str,
    close: str,
    market: Market = Market.FUTURES,
) -> Candle:
    return make_candle(index, market=market).model_copy(
        update={
            "open": Decimal(open_price),
            "high": Decimal(high),
            "low": Decimal(low),
            "close": Decimal(close),
        }
    )


def _exit_feature(candle: Candle, **updates: object) -> FeatureSnapshot:
    return make_feature(
        market=candle.market,
        symbol=candle.symbol,
        interval=candle.interval,
        event_time_ms=candle.close_time_ms,
        price=float(candle.close),
        **updates,
    )


def test_recommendation_outcome_uses_next_open_full_horizon_and_all_costs() -> None:
    decision = make_decision(event_time_ms=599_999, invalidation=Decimal("98"))
    first = _priced_candle(2, open_price="100", high="103", low="99", close="102")
    second = _priced_candle(3, open_price="102", high="104", low="100", close="103")
    funding = [FundingRate(750_000, 0.001, 100.0)]

    result = RecommendationOutcomeEvaluator().evaluate(
        decision,
        [first, second],
        2,
        fee_bps=10,
        slippage_bps=5,
        funding=funding,
        hit_margin_bps=10,
    )

    assert isinstance(result, RecommendationOutcome)
    assert result.entry_time_ms == 600_000
    assert result.exit_time_ms == second.close_time_ms
    assert result.entry_price == 100
    assert result.exit_price == 103
    assert result.gross_return == pytest.approx(0.03)
    assert result.mfe == pytest.approx(0.04)
    assert result.mae == pytest.approx(-0.01)
    assert result.funding_return == pytest.approx(-0.001)
    assert result.net_return == pytest.approx(
        result.gross_return
        - result.slippage_return
        - result.fee_return
        + result.funding_return
    )
    assert result.hit_status is DirectionalHitStatus.HIT
    assert result.one_r_path.status is OneRPathStatus.TARGET_FIRST
    assert result.one_r_path.target_price == 102


def test_recommendation_outcome_is_direction_aware_for_short() -> None:
    decision = make_decision(
        event_time_ms=599_999,
        direction=Direction.SHORT,
        family="breakdown_short",
        invalidation=Decimal("103"),
    )
    first = _priced_candle(2, open_price="100", high="101", low="98", close="99")
    second = _priced_candle(3, open_price="99", high="100", low="94", close="95")

    result = RecommendationOutcomeEvaluator().evaluate(
        decision,
        [first, second],
        2,
        funding=[FundingRate(750_000, 0.001)],
    )

    assert isinstance(result, RecommendationOutcome)
    assert result.gross_return == pytest.approx(0.05)
    assert result.mfe == pytest.approx(0.06)
    assert result.mae == pytest.approx(-0.01)
    assert result.funding_return == pytest.approx(0.001)
    assert result.hit_status is DirectionalHitStatus.HIT
    assert result.one_r_path.status is OneRPathStatus.TARGET_FIRST


def test_spot_short_remains_a_direction_metric_and_does_not_apply_funding() -> None:
    decision = make_decision(
        market=Market.SPOT,
        event_time_ms=599_999,
        direction=Direction.SHORT,
        family="breakdown_short",
        invalidation=Decimal("103"),
    )
    candle = _priced_candle(
        2,
        market=Market.SPOT,
        open_price="100",
        high="101",
        low="94",
        close="95",
    )

    result = RecommendationOutcomeEvaluator().evaluate(
        decision,
        [candle],
        1,
        funding=[FundingRate(750_000, 0.25)],
    )

    assert isinstance(result, RecommendationOutcome)
    assert result.gross_return == pytest.approx(0.05)
    assert result.funding_return == 0


@pytest.mark.parametrize(
    ("net_return", "expected"),
    [
        (0.0011, DirectionalHitStatus.HIT),
        (0.001, DirectionalHitStatus.AMBIGUOUS),
        (0.0, DirectionalHitStatus.AMBIGUOUS),
        (-0.001, DirectionalHitStatus.AMBIGUOUS),
        (-0.0011, DirectionalHitStatus.MISS),
    ],
)
def test_directional_hit_uses_strict_symmetric_margin(
    net_return: float, expected: DirectionalHitStatus
) -> None:
    assert classify_directional_hit(net_return, 10) is expected


@pytest.mark.parametrize(
    ("high", "low", "invalidation", "expected"),
    [
        ("102", "99", 98.0, OneRPathStatus.TARGET_FIRST),
        ("101", "98", 98.0, OneRPathStatus.INVALIDATION_FIRST),
        ("102", "98", 98.0, OneRPathStatus.COLLISION),
        ("101", "99", 98.0, OneRPathStatus.TIMEOUT),
        ("102", "99", 101.0, OneRPathStatus.INVALID_INVALIDATION),
        ("102", "99", None, OneRPathStatus.INVALID_INVALIDATION),
    ],
)
def test_one_r_path_statuses_are_explicit_and_same_bar_both_is_collision(
    high: str,
    low: str,
    invalidation: float | None,
    expected: OneRPathStatus,
) -> None:
    candle = _priced_candle(2, open_price="100", high=high, low=low, close="100")
    result = evaluate_one_r_path(Direction.LONG, 100, invalidation, [candle])
    assert result.status is expected


@pytest.mark.parametrize(
    ("direction", "open_price", "invalidation", "expected"),
    [
        (Direction.LONG, "103", 98.0, OneRPathStatus.TARGET_FIRST),
        (Direction.LONG, "97", 98.0, OneRPathStatus.INVALIDATION_FIRST),
        (Direction.SHORT, "97", 102.0, OneRPathStatus.TARGET_FIRST),
        (Direction.SHORT, "103", 102.0, OneRPathStatus.INVALIDATION_FIRST),
    ],
)
def test_one_r_gap_open_resolves_order_before_intrabar_collision(
    direction: Direction,
    open_price: str,
    invalidation: float,
    expected: OneRPathStatus,
) -> None:
    candle = _priced_candle(
        2,
        open_price=open_price,
        high="104",
        low="96",
        close="100",
    )

    result = evaluate_one_r_path(direction, 100, invalidation, [candle])

    assert result.status is expected
    assert result.observed_until_ms == candle.open_time_ms


@pytest.mark.parametrize(
    ("candles", "expected_reason", "observed_bars"),
    [
        ([], RecommendationOutcomeExclusionReason.NEXT_BAR_UNAVAILABLE, 0),
        (
            [_priced_candle(3, open_price="100", high="101", low="99", close="100")],
            RecommendationOutcomeExclusionReason.NEXT_BAR_NOT_CONTIGUOUS,
            0,
        ),
        (
            [
                _priced_candle(2, open_price="100", high="101", low="99", close="100"),
                _priced_candle(4, open_price="100", high="101", low="99", close="100"),
            ],
            RecommendationOutcomeExclusionReason.DATA_GAP_IN_HORIZON,
            1,
        ),
        (
            [_priced_candle(2, open_price="100", high="101", low="99", close="100")],
            RecommendationOutcomeExclusionReason.INSUFFICIENT_HORIZON,
            1,
        ),
    ],
)
def test_recommendation_outcome_returns_auditable_horizon_exclusions(
    candles: list,
    expected_reason: RecommendationOutcomeExclusionReason,
    observed_bars: int,
) -> None:
    result = RecommendationOutcomeEvaluator().evaluate(
        make_decision(event_time_ms=599_999), candles, 2
    )
    assert isinstance(result, RecommendationOutcomeExclusion)
    assert result.reason is expected_reason
    assert result.observed_bars == observed_bars


def test_recommendation_outcome_rejects_unsorted_or_mismatched_series() -> None:
    first = _priced_candle(2, open_price="100", high="101", low="99", close="100")
    second = _priced_candle(3, open_price="100", high="101", low="99", close="100")
    evaluator = RecommendationOutcomeEvaluator()

    with pytest.raises(ValueError, match="strictly ordered"):
        evaluator.evaluate(make_decision(event_time_ms=599_999), [second, first], 2)
    with pytest.raises(ValueError, match="match the recommendation series"):
        evaluator.evaluate(
            make_decision(event_time_ms=599_999),
            [first.model_copy(update={"market": Market.SPOT})],
            1,
        )


def test_recommendation_outcome_rejects_invalid_cost_policy_even_when_excluded() -> None:
    with pytest.raises(ValueError, match="fees"):
        RecommendationOutcomeEvaluator().evaluate(
            make_decision(event_time_ms=599_999), [], 1, fee_bps=-1
        )


def test_counterfactual_technical_exit_applies_trailing_stop_and_all_costs() -> None:
    policy = ExitPolicySettings(
        trend_failure_bars=2,
        trailing_activation_r=1,
        trailing_atr_multiple=2,
        max_holding_bars=72,
    )
    decision = make_decision(
        event_time_ms=599_999,
        stage=SignalStage.SETUP,
        invalidation=Decimal("98"),
        metadata={"informational_only": True},
    )
    first = _priced_candle(
        2,
        open_price="100",
        high="103",
        low="99",
        close="102",
    )
    second = _priced_candle(
        3,
        open_price="102",
        high="103",
        low="100.5",
        close="101.5",
    )

    result = CounterfactualTechnicalExitEvaluator(policy).evaluate(
        decision,
        [first, second],
        [_exit_feature(first, atr=1.0, ema20=100.0, macd_histogram=0.1), None],
        split_start_ms=0,
        split_end_ms=10_000_000,
        fee_bps=10,
        slippage_bps=5,
        funding=[FundingRate(750_000, 0.001, 100.0)],
    )

    assert isinstance(result, CounterfactualTechnicalExitOutcome)
    assert result.source_information_only is True
    assert result.entry_action_label == "FUTURES_LONG"
    assert result.exit_action_label == "FUTURES_LONG_EXIT"
    assert result.entry_time_ms == first.open_time_ms
    assert result.exit_time_ms == second.close_time_ms
    assert result.exit_signal_observed_at_ms == second.close_time_ms
    assert result.entry_price == 100
    assert result.exit_price == 101
    assert result.initial_stop == 98
    assert result.active_stop == 101
    assert result.exit_reason is ExitReason.TRAILING_STOP
    assert result.execution_model == "counterfactual_stop_touch_in_closed_bar"
    assert result.bars_held == 2
    assert result.gross_return == pytest.approx(0.01)
    assert result.mfe == pytest.approx(0.03)
    assert result.mae == pytest.approx(-0.01)
    assert result.funding_return == pytest.approx(-0.001)
    assert result.net_return == pytest.approx(
        result.gross_return
        - result.slippage_return
        - result.fee_return
        + result.funding_return
    )
    assert result.opposite_signal_evaluated is False
    assert result.order_placed is False


def test_counterfactual_trend_failure_is_filled_at_next_contiguous_open() -> None:
    policy = ExitPolicySettings(
        trend_failure_bars=2,
        trailing_activation_r=10,
        trailing_atr_multiple=2,
        max_holding_bars=72,
    )
    decision = make_decision(
        event_time_ms=599_999,
        invalidation=Decimal("90"),
    )
    first = _priced_candle(
        2,
        open_price="100",
        high="101",
        low="98",
        close="99",
    )
    second = _priced_candle(
        3,
        open_price="99",
        high="100",
        low="98.5",
        close="98.75",
    )
    exit_bar = _priced_candle(
        4,
        open_price="98.75",
        high="99",
        low="98",
        close="98.5",
    )

    result = CounterfactualTechnicalExitEvaluator(policy).evaluate(
        decision,
        [first, second, exit_bar],
        [
            _exit_feature(first, ema20=100.0, macd_histogram=-0.1),
            _exit_feature(second, ema20=100.0, macd_histogram=-0.1),
            None,
        ],
        split_start_ms=0,
        split_end_ms=10_000_000,
    )

    assert isinstance(result, CounterfactualTechnicalExitOutcome)
    assert result.exit_reason is ExitReason.TREND_FAILURE
    assert result.execution_model == "counterfactual_next_bar_open"
    assert result.exit_signal_observed_at_ms == second.close_time_ms
    assert result.exit_time_ms == exit_bar.open_time_ms
    assert result.exit_price == pytest.approx(98.75)
    assert result.bars_held == 2


def test_counterfactual_time_exit_is_filled_at_next_contiguous_open() -> None:
    policy = ExitPolicySettings(
        trend_failure_bars=2,
        trailing_activation_r=10,
        trailing_atr_multiple=2,
        max_holding_bars=1,
    )
    decision = make_decision(
        event_time_ms=599_999,
        invalidation=Decimal("90"),
    )
    first = _priced_candle(
        2,
        open_price="100",
        high="101",
        low="99",
        close="100.5",
    )
    exit_bar = _priced_candle(
        3,
        open_price="101.25",
        high="102",
        low="100",
        close="101",
    )

    result = CounterfactualTechnicalExitEvaluator(policy).evaluate(
        decision,
        [first, exit_bar],
        [_exit_feature(first, ema20=100.0, macd_histogram=0.1), None],
        split_start_ms=0,
        split_end_ms=10_000_000,
    )

    assert isinstance(result, CounterfactualTechnicalExitOutcome)
    assert result.exit_reason is ExitReason.TIME_EXIT
    assert result.execution_model == "counterfactual_next_bar_open"
    assert result.exit_signal_observed_at_ms == first.close_time_ms
    assert result.exit_time_ms == exit_bar.open_time_ms
    assert result.exit_price == pytest.approx(101.25)
    assert result.bars_held == 1


@pytest.mark.parametrize(
    (
        "market",
        "direction",
        "family",
        "invalidation",
        "high",
        "low",
        "entry_action",
        "exit_action",
    ),
    [
        (
            Market.SPOT,
            Direction.LONG,
            "breakout_long",
            "99",
            "101",
            "98.5",
            "SPOT_BUY",
            "SPOT_EXIT",
        ),
        (
            Market.FUTURES,
            Direction.SHORT,
            "breakdown_short",
            "101",
            "101.5",
            "99",
            "FUTURES_SHORT",
            "FUTURES_SHORT_EXIT",
        ),
    ],
)
def test_counterfactual_initial_stops_preserve_market_action_semantics(
    market: Market,
    direction: Direction,
    family: str,
    invalidation: str,
    high: str,
    low: str,
    entry_action: str,
    exit_action: str,
) -> None:
    decision = make_decision(
        market=market,
        event_time_ms=599_999,
        direction=direction,
        family=family,
        invalidation=Decimal(invalidation),
    )
    candle = _priced_candle(
        2,
        market=market,
        open_price="100",
        high=high,
        low=low,
        close="100",
    )

    result = CounterfactualTechnicalExitEvaluator(ExitPolicySettings()).evaluate(
        decision,
        [candle],
        [None],
        split_start_ms=0,
        split_end_ms=10_000_000,
    )

    assert isinstance(result, CounterfactualTechnicalExitOutcome)
    assert result.exit_reason is ExitReason.INITIAL_STOP
    assert result.exit_price == float(invalidation)
    assert result.entry_action_label == entry_action
    assert result.exit_action_label == exit_action
    assert result.bars_held == 1


def test_counterfactual_technical_exit_fails_closed_on_spot_short() -> None:
    decision = make_decision(
        market=Market.SPOT,
        event_time_ms=599_999,
        direction=Direction.SHORT,
        family="breakdown_short",
        invalidation=Decimal("101"),
    )

    result = CounterfactualTechnicalExitEvaluator(ExitPolicySettings()).evaluate(
        decision,
        [],
        [],
        split_start_ms=0,
        split_end_ms=10_000_000,
    )

    assert isinstance(result, CounterfactualTechnicalExitExclusion)
    assert result.reason is TechnicalExitOutcomeExclusionReason.SPOT_SHORT_NOT_EXECUTABLE


def test_counterfactual_technical_exit_fails_closed_on_invalid_invalidation() -> None:
    candle = _priced_candle(
        2,
        open_price="100",
        high="101",
        low="99",
        close="100",
    )

    result = CounterfactualTechnicalExitEvaluator(ExitPolicySettings()).evaluate(
        make_decision(event_time_ms=599_999, invalidation=None),
        [candle],
        [_exit_feature(candle)],
        split_start_ms=0,
        split_end_ms=10_000_000,
    )

    assert isinstance(result, CounterfactualTechnicalExitExclusion)
    assert result.reason is TechnicalExitOutcomeExclusionReason.INVALID_INVALIDATION


def test_counterfactual_technical_exit_reports_data_gap_after_observed_bar() -> None:
    first = _priced_candle(
        2,
        open_price="100",
        high="101",
        low="99",
        close="100",
    )
    after_gap = _priced_candle(
        4,
        open_price="100",
        high="101",
        low="99",
        close="100",
    )

    result = CounterfactualTechnicalExitEvaluator(ExitPolicySettings()).evaluate(
        make_decision(event_time_ms=599_999, invalidation=Decimal("90")),
        [first, after_gap],
        [_exit_feature(first), _exit_feature(after_gap)],
        split_start_ms=0,
        split_end_ms=10_000_000,
    )

    assert isinstance(result, CounterfactualTechnicalExitExclusion)
    assert result.reason is TechnicalExitOutcomeExclusionReason.DATA_GAP
    assert result.expected_open_time_ms == 900_000
    assert result.actual_open_time_ms == after_gap.open_time_ms
    assert result.observed_bars == 1
    assert result.observed_until_ms == first.close_time_ms


@pytest.mark.parametrize(
    ("feature", "expected"),
    [
        (None, TechnicalExitOutcomeExclusionReason.FEATURE_UNAVAILABLE),
        ("mismatch", TechnicalExitOutcomeExclusionReason.FEATURE_MISMATCH),
    ],
)
def test_counterfactual_technical_exit_requires_causal_matching_features(
    feature: str | None,
    expected: TechnicalExitOutcomeExclusionReason,
) -> None:
    candle = _priced_candle(
        2,
        open_price="100",
        high="101",
        low="99",
        close="100",
    )
    snapshot = (
        None
        if feature is None
        else _exit_feature(candle).model_copy(update={"symbol": "ETHUSDT"})
    )

    result = CounterfactualTechnicalExitEvaluator(ExitPolicySettings()).evaluate(
        make_decision(event_time_ms=599_999, invalidation=Decimal("90")),
        [candle],
        [snapshot],
        split_start_ms=0,
        split_end_ms=10_000_000,
    )

    assert isinstance(result, CounterfactualTechnicalExitExclusion)
    assert result.reason is expected


def test_counterfactual_pending_exit_cannot_cross_split_boundary() -> None:
    first = _priced_candle(
        2,
        open_price="100",
        high="101",
        low="99",
        close="100.5",
    )
    next_split = _priced_candle(
        3,
        open_price="101",
        high="102",
        low="100",
        close="101",
    )
    policy = ExitPolicySettings(max_holding_bars=1)

    result = CounterfactualTechnicalExitEvaluator(policy).evaluate(
        make_decision(event_time_ms=599_999, invalidation=Decimal("90")),
        [first, next_split],
        [_exit_feature(first), None],
        split_start_ms=0,
        split_end_ms=900_000,
    )

    assert isinstance(result, CounterfactualTechnicalExitExclusion)
    assert result.reason is TechnicalExitOutcomeExclusionReason.SPLIT_LEAKAGE
    assert result.observed_bars == 1
    assert result.observed_until_ms == first.close_time_ms


def test_counterfactual_pending_exit_fails_closed_at_end_of_data() -> None:
    candle = _priced_candle(
        2,
        open_price="100",
        high="101",
        low="99",
        close="100.5",
    )

    result = CounterfactualTechnicalExitEvaluator(
        ExitPolicySettings(max_holding_bars=1)
    ).evaluate(
        make_decision(event_time_ms=599_999, invalidation=Decimal("90")),
        [candle],
        [_exit_feature(candle)],
        split_start_ms=0,
        split_end_ms=10_000_000,
    )

    assert isinstance(result, CounterfactualTechnicalExitExclusion)
    assert result.reason is TechnicalExitOutcomeExclusionReason.END_OF_DATA_BEFORE_EXIT
    assert result.observed_bars == 1
    assert result.observed_until_ms == candle.close_time_ms
