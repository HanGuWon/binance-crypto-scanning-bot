from __future__ import annotations

import hashlib
import math
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from itertools import pairwise

from signalbot.backtest.config import BacktestAsset, BacktestSpec
from signalbot.backtest.context import (
    HIGHER_TIMEFRAME_INTERVALS,
    StrictContextIndex,
    aggregate_closed_candles,
)
from signalbot.backtest.labels import classify_kline_proxy_outcome
from signalbot.config import Settings, SignalSettings
from signalbot.data.candles import interval_to_milliseconds
from signalbot.data.funding import FundingRatePoint, FundingRateTracker
from signalbot.data.microstructure import OrderFlowSnapshot, closed_kline_flow
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import (
    Candle,
    FeatureSnapshot,
    MarketRegime,
    RuleEvaluation,
    SignalDecision,
)
from signalbot.indicators.core import FeatureEngine, ema_series
from signalbot.signals.gates import (
    StrictPriorHtfEvaluation,
    evaluate_strict_prior_htf,
)
from signalbot.signals.positions import ExitReason, PaperPosition, TechnicalExitEngine
from signalbot.signals.rules import SignalRuleEngine
from signalbot.signals.state_machine import SignalStateMachine

_LIVE_FEATURE_MINIMUM_HISTORY = 210


@dataclass(frozen=True, slots=True)
class FundingRate:
    funding_time_ms: int
    rate: float
    mark_price: float | None = None


@dataclass(frozen=True, slots=True)
class ExecutionReturns:
    entry_execution_price: float
    exit_execution_price: float
    gross_return: float
    slippage_return: float
    fee_return: float
    net_before_funding: float


def calculate_execution_returns(
    direction: Direction,
    entry_price: float,
    exit_price: float,
    fee_bps: float,
    slippage_bps: float,
) -> ExecutionReturns:
    if direction not in {Direction.LONG, Direction.SHORT}:
        raise ValueError("execution direction must be long or short")
    if (
        not math.isfinite(entry_price)
        or not math.isfinite(exit_price)
        or entry_price <= 0
        or exit_price <= 0
    ):
        raise ValueError("execution prices must be finite and positive")
    if (
        not math.isfinite(fee_bps)
        or not math.isfinite(slippage_bps)
        or fee_bps < 0
        or slippage_bps < 0
    ):
        raise ValueError("execution costs must be finite and non-negative")
    direction_sign = 1.0 if direction is Direction.LONG else -1.0
    slippage_rate = slippage_bps / 10_000
    fee_rate = fee_bps / 10_000
    gross_return = direction_sign * (exit_price - entry_price) / entry_price
    if direction is Direction.LONG:
        entry_execution = entry_price * (1 + slippage_rate)
        exit_execution = exit_price * (1 - slippage_rate)
    else:
        entry_execution = entry_price * (1 - slippage_rate)
        exit_execution = exit_price * (1 + slippage_rate)
    execution_return = direction_sign * (exit_execution - entry_execution) / entry_price
    slippage_return = max(0.0, gross_return - execution_return)
    fee_return = fee_rate * (entry_execution + exit_execution) / entry_price
    return ExecutionReturns(
        entry_execution_price=entry_execution,
        exit_execution_price=exit_execution,
        gross_return=gross_return,
        slippage_return=slippage_return,
        fee_return=fee_return,
        net_before_funding=execution_return - fee_return,
    )


def directional_excursion(
    direction: Direction, entry_price: float, low_price: float, high_price: float
) -> tuple[float, float]:
    if direction is Direction.LONG:
        return (high_price - entry_price) / entry_price, (low_price - entry_price) / entry_price
    if direction is Direction.SHORT:
        return (entry_price - low_price) / entry_price, (entry_price - high_price) / entry_price
    raise ValueError("excursion direction must be long or short")


def count_held_bars(entry_index: int, exit_index: int, *, exit_on_open: bool) -> int:
    return max(0, exit_index - entry_index + (0 if exit_on_open else 1))


def calculate_funding_return(
    direction: Direction,
    entry_time_ms: int,
    exit_time_ms: int,
    entry_price: float,
    funding: list[FundingRate],
) -> float:
    if direction not in {Direction.LONG, Direction.SHORT}:
        return 0.0
    direction_sign = 1.0 if direction is Direction.LONG else -1.0
    total = 0.0
    for item in funding:
        if not entry_time_ms < item.funding_time_ms < exit_time_ms:
            continue
        mark = item.mark_price or entry_price
        total += -direction_sign * item.rate * mark / entry_price
    return total


def _opportunity_id(
    market: Market,
    symbol: str,
    family: SignalFamily,
    decision_time_ms: int,
) -> str:
    """Return the shared deterministic identity for an entry opportunity."""

    identity = "|".join(
        (market.value, symbol, family.value, str(decision_time_ms))
    )
    return hashlib.sha256(identity.encode()).hexdigest()[:24]


@dataclass(frozen=True, slots=True)
class Trade:
    trade_id: str
    opportunity_id: str
    protocol_version: str
    rule_version: str
    asset: str
    cohort: str
    market: str
    symbol: str
    direction: str
    family: str
    score: int
    split: str
    split_contained: bool
    regime: str
    entry_signal_id: str
    entry_signal_time_ms: int
    entry_time_ms: int
    exit_time_ms: int
    entry_price: float
    exit_price: float
    entry_execution_price: float
    exit_execution_price: float
    initial_stop: float
    exit_reason: str
    bars_held: int
    gross_return: float
    slippage_return: float
    fee_return: float
    funding_return: float
    net_return: float
    gross_pnl_usdt: float
    slippage_usdt: float
    fees_usdt: float
    funding_pnl_usdt: float
    net_pnl_usdt: float
    mfe: float
    mae: float
    net_r_multiple: float


@dataclass(frozen=True, slots=True)
class Opportunity:
    opportunity_id: str
    protocol_version: str
    rule_version: str
    volume_feature_set: str
    asset: str
    cohort: str
    market: str
    symbol: str
    direction: str
    family: str
    decision_time_ms: int
    next_open_time_ms: int | None
    setup_strength: int
    reasons: str
    invalidation: float | None
    eligible: bool
    gate_passed: bool
    gate_failures: str
    htf_filter_accepted: bool
    htf_filter_failures: str
    execution_observed: bool
    full_r2_eligible: bool | None
    split: str
    regime: str
    btc_trend: str
    breadth_ratio: float
    analysis_eligible: bool
    analysis_exclusion: str
    analysis_eligible_3: bool
    analysis_exclusion_3: str
    analysis_eligible_6: bool
    analysis_exclusion_6: str
    analysis_eligible_12: bool
    analysis_exclusion_12: str
    analysis_eligible_72: bool
    analysis_exclusion_72: str
    volume_feature_available: bool
    volume_feature_unavailable_reason: str
    taker_delta_3: float | None
    taker_delta_12: float | None
    normalized_vpci: float | None
    normalized_vpci_signal: float | None
    normalized_vpci_slope_3: float | None
    forward_return_3: float | None
    forward_return_6: float | None
    forward_return_12: float | None
    forward_return_72: float | None
    long_net_return_3: float | None
    long_net_return_6: float | None
    long_net_return_12: float | None
    short_net_return_3: float | None
    short_net_return_6: float | None
    short_net_return_12: float | None
    outcome_label_3: str | None
    outcome_label_6: str | None
    outcome_label_12: str | None
    signal_gross_return_3: float | None
    signal_fee_return_3: float | None
    signal_slippage_return_3: float | None
    signal_funding_return_3: float | None
    signal_net_return_3: float | None
    signal_gross_return_6: float | None
    signal_fee_return_6: float | None
    signal_slippage_return_6: float | None
    signal_funding_return_6: float | None
    signal_net_return_6: float | None
    signal_gross_return_12: float | None
    signal_fee_return_12: float | None
    signal_slippage_return_12: float | None
    signal_funding_return_12: float | None
    signal_net_return_12: float | None
    f60_execution_model: str
    f60_gross_return: float | None
    f60_fee_return: float | None
    f60_slippage_return: float | None
    f60_funding_return: float | None
    f60_net_return: float | None
    mfe_72: float | None
    mae_72: float | None


@dataclass(frozen=True, slots=True)
class SymbolBacktest:
    asset: str
    market: Market
    symbol: str
    eligible_from_ms: int
    candles: int
    evaluated_bars: int
    candidate_setups: int
    confirmed_signals: int
    scheduled_entries: int
    cancelled_gap_entries: int
    trades: tuple[Trade, ...]
    opportunities: tuple[Opportunity, ...] = ()


@dataclass(slots=True)
class _LiveTrade:
    position: PaperPosition
    mfe: float = 0.0
    mae: float = 0.0


@dataclass(frozen=True, slots=True)
class _CandidateEvaluation:
    evaluation: RuleEvaluation
    htf: StrictPriorHtfEvaluation


def historical_order_flow(candle: Candle) -> OrderFlowSnapshot:
    """Backward-compatible alias for the canonical closed-kline flow owner."""

    return closed_kline_flow(candle)


def _btc_trends_by_close(
    candles: list[Candle],
    *,
    minimum_history: int = 0,
) -> dict[int, str]:
    """Build BTC trend points with the live engine's no-gap-reset history policy."""

    result: dict[int, str] = {}
    if not candles:
        return result
    closes = [float(candle.close) for candle in candles]
    e20 = ema_series(closes, 20)
    e50 = ema_series(closes, 50)
    for index, candle in enumerate(candles):
        trend = "neutral"
        e20_value = e20[index]
        e50_value = e50[index]
        if (
            index + 1 >= minimum_history
            and e20_value is not None
            and e50_value is not None
        ):
            if closes[index] > e20_value > e50_value:
                trend = "bullish"
            elif closes[index] < e20_value < e50_value:
                trend = "bearish"
        result[candle.close_time_ms] = trend
    return result


def _strict_prior_breadth_by_close(
    candles_by_asset: Mapping[str, list[Candle]],
) -> dict[int, float]:
    """Return live-parity breadth from the latest two 5m closes before each close."""

    decision_closes = sorted(
        {
            candle.close_time_ms
            for candles in candles_by_asset.values()
            for candle in candles
        }
    )
    series_by_asset = sorted(candles_by_asset.items())
    positions = {asset: 0 for asset, _candles in series_by_asset}
    recent_prices: dict[str, list[float]] = {}
    breadth_by_close: dict[int, float] = {}
    for close_time_ms in decision_closes:
        for asset, candles in series_by_asset:
            position = positions[asset]
            while (
                position < len(candles)
                and candles[position].close_time_ms < close_time_ms
            ):
                candle = candles[position]
                if candle.interval == "5m" and candle.is_closed:
                    prices = recent_prices.setdefault(asset, [])
                    prices.append(float(candle.close))
                    if len(prices) > 2:
                        del prices[:-2]
                position += 1
            positions[asset] = position
        directions = [
            prices[-1] > prices[-2]
            for prices in recent_prices.values()
            if len(prices) == 2
        ]
        breadth_by_close[close_time_ms] = (
            sum(directions) / len(directions) if directions else 0.5
        )
    return breadth_by_close


def _strict_prior_trends_by_close(
    btc_trends: Mapping[int, str], decision_closes: set[int]
) -> dict[int, str]:
    """Carry the latest BTC trend strictly before each decision close."""

    trend_points = sorted(btc_trends.items())
    point_index = 0
    current_trend = "neutral"
    result: dict[int, str] = {}
    for close_time_ms in sorted(decision_closes):
        while (
            point_index < len(trend_points)
            and trend_points[point_index][0] < close_time_ms
        ):
            current_trend = trend_points[point_index][1]
            point_index += 1
        result[close_time_ms] = current_trend
    return result


def build_market_regimes(
    candles_by_asset: dict[str, list[Candle]],
) -> dict[str, list[MarketRegime]]:
    """Build point-in-time BTC trend and selected-panel breadth regimes."""
    has_candles = any(candles for candles in candles_by_asset.values())
    uses_five_minute_panel = has_candles and all(
        not candles or candles[0].interval == "5m"
        for candles in candles_by_asset.values()
    )
    breadth: dict[int, list[int]] = {}
    strict_prior_breadth: dict[int, float] = {}
    if uses_five_minute_panel:
        strict_prior_breadth = _strict_prior_breadth_by_close(candles_by_asset)
    else:
        for candles in candles_by_asset.values():
            for previous, current in pairwise(candles):
                step_ms = interval_to_milliseconds(current.interval)
                if current.open_time_ms - previous.open_time_ms != step_ms:
                    continue
                values = breadth.setdefault(current.close_time_ms, [0, 0])
                values[0] += int(current.close > previous.close)
                values[1] += 1

    btc = candles_by_asset.get("BTC", [])
    uses_hourly_btc_context = bool(btc) and all(candle.interval == "5m" for candle in btc)
    if uses_hourly_btc_context:
        hourly_btc = aggregate_closed_candles(btc, "1h")
        btc_trends = _btc_trends_by_close(
            hourly_btc,
            minimum_history=_LIVE_FEATURE_MINIMUM_HISTORY,
        )
    else:
        btc_trends = _btc_trends_by_close(btc)
    decision_closes = {
        candle.close_time_ms
        for candles in candles_by_asset.values()
        for candle in candles
    }
    strict_prior_btc_trends = (
        _strict_prior_trends_by_close(btc_trends, decision_closes)
        if uses_five_minute_panel
        else {}
    )

    result: dict[str, list[MarketRegime]] = {}
    for asset, candles in candles_by_asset.items():
        regimes = []
        for candle in candles:
            if uses_five_minute_panel:
                ratio = strict_prior_breadth.get(candle.close_time_ms, 0.5)
                btc_trend = strict_prior_btc_trends.get(candle.close_time_ms, "neutral")
            else:
                up, total = breadth.get(candle.close_time_ms, [0, 0])
                ratio = up / total if total else 0.5
                btc_trend = btc_trends.get(candle.close_time_ms, "neutral")
            label = (
                "risk_on"
                if btc_trend == "bullish" and ratio >= 0.55
                else "risk_off"
                if btc_trend == "bearish" and ratio <= 0.45
                else "neutral"
            )
            regimes.append(MarketRegime(label=label, btc_trend=btc_trend, breadth_ratio=ratio))
        result[asset] = regimes
    return result


class ResearchBacktester:
    def __init__(
        self,
        settings: Settings,
        spec: BacktestSpec,
        *,
        signal_settings: SignalSettings | None = None,
    ) -> None:
        self.settings = settings
        self.spec = spec
        if signal_settings is None:
            reversal_intervals = list(settings.signals.reversal_intervals)
            if not spec.include_rsi_reversals:
                reversal_intervals = [
                    interval for interval in reversal_intervals if interval != spec.interval
                ]
            confirmation_mode = spec.confirmation_mode or (
                "explicit_trigger"
                if spec.strategy_mode == "pit_breakout_volume"
                else "score"
            )
            signal_settings = settings.signals.model_copy(
                update={
                    "confirmed_score": spec.entry_score,
                    "gate_enabled": spec.strategy_mode != "legacy",
                    "confirmation_mode": confirmation_mode,
                    "gate_use_participation": spec.gate_use_participation,
                    "gate_use_crowding": spec.gate_use_crowding,
                    "gate_use_higher_timeframes": spec.gate_use_higher_timeframes,
                    "entry_policy": "legacy_gates",
                    "trend_gate": spec.trend_gate,
                    "participation_gate": spec.participation_gate,
                    "crowding_risk_cap": spec.crowding_risk_cap,
                    "execution_gate": spec.execution_gate,
                    "completeness_gate": spec.completeness_gate,
                    "volume_feature_set": spec.volume_feature_set,
                    "volume": spec.volume,
                    "reversal_intervals": reversal_intervals,
                }
            )
        self.feature_engine = FeatureEngine(signal_settings)
        self.rule_settings = signal_settings
        self.exit_engine = TechnicalExitEngine(spec.exits)

    def run_symbol(
        self,
        asset: BacktestAsset,
        market: Market,
        candles: list[Candle],
        regimes: list[MarketRegime],
        funding: list[FundingRate] | None = None,
    ) -> SymbolBacktest:
        if not candles:
            raise ValueError(f"no candles for {market.value}:{asset.asset}")
        if len(candles) != len(regimes):
            raise ValueError("regimes must align with candles")
        if any(not candle.is_closed for candle in candles):
            raise ValueError("backtests require closed candles only")
        if any(candle.market is not market for candle in candles):
            raise ValueError("candle market does not match requested market")

        # Historical kline flow is assembled inside FeatureEngine from the same
        # closed candle as live evaluation. Do not masquerade it as 60-second
        # intrabar aggTrade flow in research output.
        flows = [OrderFlowSnapshot() for _ in candles]
        features = self._continuous_features(candles, flows, regimes)
        needs_htf_context = (
            self.rule_settings.gate_use_higher_timeframes
            or self.spec.candidate_policy is not None
        )
        context_index = (
            self._higher_timeframe_context_index(candles, regimes)
            if needs_htf_context
            else StrictContextIndex({})
        )
        rule_engine = SignalRuleEngine(self.rule_settings)
        state_machine = SignalStateMachine(self.rule_settings, self.spec.rule_version)
        interval_ms = interval_to_milliseconds(self.spec.interval)
        evaluation_start_ms = int(self.spec.evaluation_start.timestamp() * 1000)
        evaluation_end_ms = int(self.spec.evaluation_end.timestamp() * 1000)
        age_gate_ms = candles[0].open_time_ms + self.spec.minimum_age_days * 86_400_000
        history_gate_ms = candles[0].open_time_ms + self.spec.minimum_history_bars * interval_ms
        eligible_from_ms = max(evaluation_start_ms, age_gate_ms, history_gate_ms)

        live: _LiveTrade | None = None
        pending_entry: SignalDecision | None = None
        pending_exit: ExitReason | None = None
        trades: list[Trade] = []
        opportunities: list[Opportunity] = []
        evaluated_bars = 0
        candidate_setups = 0
        confirmed_signals = 0
        scheduled_entries = 0
        cancelled_gap_entries = 0
        funding = sorted(funding or [], key=lambda item: item.funding_time_ms)
        allowed_direction = Direction.LONG if market is Market.SPOT else Direction.SHORT
        if market is Market.FUTURES and funding:
            features = self._with_funding_features(
                features,
                funding,
                asset.futures_symbol,
            )
        last_processed_index = -1

        for index, candle in enumerate(candles):
            if candle.open_time_ms >= evaluation_end_ms:
                break
            last_processed_index = index

            is_data_gap = (
                index > 0
                and candle.open_time_ms - candles[index - 1].open_time_ms
                != interval_ms
            )
            is_split_boundary = (
                self.spec.candidate_policy is not None
                and index > 0
                and self.spec.split_name(candle.open_time_ms)
                != self.spec.split_name(candles[index - 1].open_time_ms)
            )
            if is_data_gap or is_split_boundary:
                if live is not None:
                    self._update_excursion(live, float(candle.open), float(candle.open))
                    trades.append(
                        self._close_trade(
                            asset,
                            market,
                            live,
                            float(candle.open),
                            candle.open_time_ms,
                            index,
                            (
                                ExitReason.SPLIT_BOUNDARY
                                if is_split_boundary
                                else ExitReason.DATA_GAP
                            ),
                            funding,
                            exit_on_open=True,
                        )
                    )
                    live = None
                pending_entry = None
                pending_exit = None
                state_machine = SignalStateMachine(self.rule_settings, self.spec.rule_version)

            if live is not None:
                stop_fill = self.exit_engine.stop_at_open(live.position, float(candle.open))
                if stop_fill is not None:
                    self._update_excursion(live, float(candle.open), float(candle.open))
                    trades.append(
                        self._close_trade(
                            asset,
                            market,
                            live,
                            stop_fill.price,
                            candle.open_time_ms,
                            index,
                            stop_fill.reason,
                            funding,
                            exit_on_open=True,
                        )
                    )
                    live = None
                    pending_exit = None
                elif pending_exit is not None:
                    self._update_excursion(live, float(candle.open), float(candle.open))
                    trades.append(
                        self._close_trade(
                            asset,
                            market,
                            live,
                            float(candle.open),
                            candle.open_time_ms,
                            index,
                            pending_exit,
                            funding,
                            exit_on_open=True,
                        )
                    )
                    live = None
                    pending_exit = None
                else:
                    intrabar = self.exit_engine.stop_in_bar(live.position, candle)
                    if intrabar is not None:
                        self._update_execution_excursion(live, float(candle.open), intrabar.price)
                        trades.append(
                            self._close_trade(
                                asset,
                                market,
                                live,
                                intrabar.price,
                                candle.close_time_ms,
                                index,
                                intrabar.reason,
                                funding,
                            )
                        )
                        live = None

            if live is None and pending_entry is not None:
                position = self.exit_engine.open_position(pending_entry, candle, index)
                pending_entry = None
                if position is None:
                    cancelled_gap_entries += 1
                else:
                    live = _LiveTrade(position)
                    intrabar = self.exit_engine.stop_in_bar(position, candle)
                    if intrabar is not None:
                        self._update_execution_excursion(live, float(candle.open), intrabar.price)
                        trades.append(
                            self._close_trade(
                                asset,
                                market,
                                live,
                                intrabar.price,
                                candle.close_time_ms,
                                index,
                                intrabar.reason,
                                funding,
                            )
                        )
                        live = None

            feature = features[index]
            entry_decisions: list[SignalDecision] = []
            evaluations: list[RuleEvaluation] = []
            candidate_evaluations: list[_CandidateEvaluation] = []
            if feature is not None:
                contexts = context_index.at(feature.event_time_ms)
                evaluations = rule_engine.evaluate(feature, contexts)
                candidate_evaluations = [
                    self._apply_candidate_policy(feature, evaluation, contexts)
                    for evaluation in evaluations
                ]
                if candle.open_time_ms >= eligible_from_ms:
                    opportunities.extend(
                        self._build_opportunity(
                            asset,
                            market,
                            candles,
                            index,
                            feature,
                            candidate,
                            funding,
                        )
                        for candidate in candidate_evaluations
                        if candidate.evaluation.triggered
                        and candidate.evaluation.direction is allowed_direction
                        and candidate.evaluation.family
                        in {SignalFamily.BREAKOUT_LONG, SignalFamily.BREAKDOWN_SHORT}
                    )
                for candidate in candidate_evaluations:
                    evaluation = candidate.evaluation
                    state_machine.process(evaluation)
                    if (
                        evaluation.triggered
                        and evaluation.direction is allowed_direction
                        and evaluation.family
                        in {SignalFamily.BREAKOUT_LONG, SignalFamily.BREAKDOWN_SHORT}
                    ):
                        decision = state_machine.decision_for_research_entry(
                            evaluation
                        )
                        if decision is not None:
                            entry_decisions.append(decision)
                if candle.open_time_ms >= eligible_from_ms:
                    evaluated_bars += 1
                    candidate_setups += sum(
                        int(
                            evaluation.metadata.get(
                                "raw_signal_score", evaluation.score
                            )
                            > 0
                        )
                        for evaluation in evaluations
                        if evaluation.direction is allowed_direction
                    )
                    confirmed_signals += len(entry_decisions)

            if live is not None and feature is not None:
                self._update_excursion(live, float(candle.low), float(candle.high))
                opposite = any(
                    candidate.evaluation.triggered
                    and candidate.evaluation.eligible
                    and candidate.evaluation.direction is not live.position.direction
                    and candidate.evaluation.family
                    in {SignalFamily.BREAKOUT_LONG, SignalFamily.BREAKDOWN_SHORT}
                    for candidate in candidate_evaluations
                )
                pending_exit = self.exit_engine.after_close(
                    live.position,
                    candle,
                    feature,
                    index,
                    opposite,
                )

            can_schedule = (
                live is None
                and pending_entry is None
                and candle.open_time_ms >= eligible_from_ms
                and candle.close_time_ms < evaluation_end_ms
                and (
                    self.spec.candidate_policy is None
                    or self._common_72_path_eligible(candles, index)
                )
            )
            if can_schedule:
                candidates = [
                    item
                    for item in entry_decisions
                    if item.direction is allowed_direction
                ]
                if candidates:
                    pending_entry = sorted(
                        candidates,
                        key=lambda item: (-item.score, item.family.value, item.event_id),
                    )[0]
                    scheduled_entries += 1

        if live is not None and last_processed_index >= 0:
            final_index = last_processed_index
            final = candles[final_index]
            self._update_excursion(live, float(final.close), float(final.close))
            trades.append(
                self._close_trade(
                    asset,
                    market,
                    live,
                    float(final.close),
                    final.close_time_ms,
                    final_index,
                    ExitReason.END_OF_DATA,
                    funding,
                )
            )

        return SymbolBacktest(
            asset=asset.asset,
            market=market,
            symbol=asset.spot_symbol if market is Market.SPOT else asset.futures_symbol,
            eligible_from_ms=eligible_from_ms,
            candles=len(candles),
            evaluated_bars=evaluated_bars,
            candidate_setups=candidate_setups,
            confirmed_signals=confirmed_signals,
            scheduled_entries=scheduled_entries,
            cancelled_gap_entries=cancelled_gap_entries,
            trades=tuple(trades),
            opportunities=tuple(opportunities),
        )

    def _apply_candidate_policy(
        self,
        feature: FeatureSnapshot,
        evaluation: RuleEvaluation,
        contexts: Mapping[str, FeatureSnapshot],
    ) -> _CandidateEvaluation:
        policy = self.spec.candidate_policy
        if policy is not None:
            htf = evaluate_strict_prior_htf(
                feature,
                evaluation.direction,
                contexts,
            )
        else:
            htf = StrictPriorHtfEvaluation(True, ())

        if policy is None:
            return _CandidateEvaluation(evaluation, htf)
        accepted = True if policy == "c0_frozen" else htf.accepted
        return _CandidateEvaluation(
            evaluation.model_copy(update={"eligible": accepted}),
            htf,
        )

    def _analysis_horizon_status(
        self,
        candles: list[Candle],
        index: int,
        horizon: int,
        *,
        split_start_embargo_bars: int | None = None,
        require_next_open_after_horizon: bool = False,
    ) -> tuple[bool, str]:
        step_ms = interval_to_milliseconds(candles[index].interval)
        embargo_bars = (
            self.spec.opportunity_panel_horizon_bars
            if split_start_embargo_bars is None
            else split_start_embargo_bars
        )
        terminal_index = index + horizon + int(require_next_open_after_horizon)
        if index + 1 >= len(candles) or terminal_index >= len(candles):
            return False, f"insufficient_{horizon}_bar_horizon"
        path = candles[index : terminal_index + 1]
        if any(
            current.open_time_ms - previous.open_time_ms != step_ms
            for previous, current in pairwise(path)
        ):
            return False, "data_gap_in_horizon"

        next_open_time_ms = candles[index + 1].open_time_ms
        split = next(
            (
                item
                for item in self.spec.splits
                if int(item.start.timestamp() * 1000)
                <= next_open_time_ms
                < int(item.end.timestamp() * 1000)
            ),
            None,
        )
        if split is None:
            return False, "outside_declared_split"
        split_start_ms = int(split.start.timestamp() * 1000)
        split_end_ms = int(split.end.timestamp() * 1000)
        if next_open_time_ms < split_start_ms + embargo_bars * step_ms:
            return False, "split_start_embargo"
        if candles[index + horizon].close_time_ms >= split_end_ms:
            return False, "horizon_crosses_split"
        if (
            require_next_open_after_horizon
            and candles[index + horizon + 1].open_time_ms >= split_end_ms
        ):
            return False, "technical_exit_open_crosses_split"
        return True, ""

    def _common_72_path_eligible(
        self,
        candles: list[Candle],
        index: int,
    ) -> bool:
        eligible, _ = self._analysis_horizon_status(
            candles,
            index,
            72,
            split_start_embargo_bars=72,
            require_next_open_after_horizon=True,
        )
        return eligible

    def _build_opportunity(
        self,
        asset: BacktestAsset,
        market: Market,
        candles: list[Candle],
        index: int,
        feature: FeatureSnapshot,
        candidate: _CandidateEvaluation,
        funding: list[FundingRate],
    ) -> Opportunity:
        evaluation = candidate.evaluation
        direction_sign = 1.0 if evaluation.direction is Direction.LONG else -1.0
        next_open_time_ms = (
            candles[index + 1].open_time_ms if index + 1 < len(candles) else None
        )
        split_name = self.spec.split_name(next_open_time_ms or feature.event_time_ms) or "outside"
        eligibility: dict[int, bool] = {}
        exclusions: dict[int, str] = {}
        returns: dict[int, float | None] = {3: None, 6: None, 12: None, 72: None}
        for horizon in (3, 6, 12):
            eligible, exclusion = self._analysis_horizon_status(
                candles,
                index,
                horizon,
                split_start_embargo_bars=self.spec.opportunity_panel_horizon_bars,
            )
            eligibility[horizon] = eligible
            exclusions[horizon] = exclusion
        technical_72_eligible, technical_72_exclusion = self._analysis_horizon_status(
            candles,
            index,
            72,
            split_start_embargo_bars=72,
            require_next_open_after_horizon=True,
        )
        eligibility[72] = technical_72_eligible
        exclusions[72] = technical_72_exclusion

        if self.spec.candidate_policy is not None:
            common_horizon = self.spec.opportunity_panel_horizon_bars
            if common_horizon == 72:
                common_eligible = technical_72_eligible
                common_exclusion = technical_72_exclusion
            else:
                common_eligible = eligibility[12]
                common_exclusion = exclusions[12]
            for horizon in (3, 6, 12):
                eligibility[horizon] = common_eligible
                exclusions[horizon] = common_exclusion

        if next_open_time_ms is not None:
            entry = float(candles[index + 1].open)
            for horizon in returns:
                if eligibility[horizon]:
                    returns[horizon] = direction_sign * (
                        float(candles[index + horizon].close) / entry - 1
                    )

        mfe: float | None = None
        mae: float | None = None
        if eligibility[72] and next_open_time_ms is not None:
            entry = float(candles[index + 1].open)
            lows = [float(item.low) for item in candles[index + 1 : index + 73]]
            highs = [float(item.high) for item in candles[index + 1 : index + 73]]
            mfe, mae = directional_excursion(
                evaluation.direction, entry, min(lows), max(highs)
            )

        long_net_returns: dict[int, float | None] = {
            horizon: None for horizon in (3, 6, 12)
        }
        short_net_returns: dict[int, float | None] = {
            horizon: None for horizon in (3, 6, 12)
        }
        outcome_labels: dict[int, str | None] = {
            horizon: None for horizon in (3, 6, 12)
        }
        signal_gross_returns: dict[int, float | None] = {
            horizon: None for horizon in (3, 6, 12)
        }
        signal_fee_returns: dict[int, float | None] = {
            horizon: None for horizon in (3, 6, 12)
        }
        signal_slippage_returns: dict[int, float | None] = {
            horizon: None for horizon in (3, 6, 12)
        }
        signal_funding_returns: dict[int, float | None] = {
            horizon: None for horizon in (3, 6, 12)
        }
        signal_net_returns: dict[int, float | None] = {
            horizon: None for horizon in (3, 6, 12)
        }
        if market is Market.SPOT:
            fee_bps = self.spec.costs.spot_fee_bps
            slippage_bps = self.spec.costs.spot_slippage_bps[asset.cohort]
        else:
            fee_bps = self.spec.costs.futures_fee_bps
            slippage_bps = self.spec.costs.futures_slippage_bps[asset.cohort]
        for horizon in (3, 6, 12):
            if not eligibility[horizon] or next_open_time_ms is None:
                continue
            entry_price = float(candles[index + 1].open)
            exit_candle = candles[index + horizon]
            exit_price = float(exit_candle.close)
            executions = {
                direction: calculate_execution_returns(
                    direction,
                    entry_price,
                    exit_price,
                    fee_bps,
                    slippage_bps,
                )
                for direction in (Direction.LONG, Direction.SHORT)
            }
            funding_returns = {
                direction: (
                    calculate_funding_return(
                        direction,
                        next_open_time_ms,
                        exit_candle.close_time_ms,
                        entry_price,
                        funding,
                    )
                    if market is Market.FUTURES
                    and self.spec.costs.include_funding
                    else 0.0
                )
                for direction in (Direction.LONG, Direction.SHORT)
            }
            long_net = (
                executions[Direction.LONG].net_before_funding
                + funding_returns[Direction.LONG]
            )
            short_net = (
                executions[Direction.SHORT].net_before_funding
                + funding_returns[Direction.SHORT]
            )
            long_net_returns[horizon] = long_net
            short_net_returns[horizon] = short_net
            outcome_labels[horizon] = classify_kline_proxy_outcome(
                long_net,
                short_net,
                self.spec.outcome_edge_margin_bps / 10_000,
            ).value
            signal_execution = executions[evaluation.direction]
            signal_funding = funding_returns[evaluation.direction]
            signal_gross_returns[horizon] = signal_execution.gross_return
            signal_fee_returns[horizon] = signal_execution.fee_return
            signal_slippage_returns[horizon] = signal_execution.slippage_return
            signal_funding_returns[horizon] = signal_funding
            signal_net_returns[horizon] = (
                signal_execution.net_before_funding + signal_funding
            )
            # Keep the legacy signaled-direction forward-return contract exact.
            returns[horizon] = signal_execution.gross_return

        is_f60_horizon = self.spec.interval == "5m"
        f60_gross_return = signal_gross_returns[12] if is_f60_horizon else None
        f60_fee_return = signal_fee_returns[12] if is_f60_horizon else None
        f60_slippage_return = (
            signal_slippage_returns[12] if is_f60_horizon else None
        )
        f60_funding_return = (
            signal_funding_returns[12] if is_f60_horizon else None
        )
        f60_net_return = signal_net_returns[12] if is_f60_horizon else None

        volume_feature_available = True
        volume_feature_unavailable_reason = ""
        if self.spec.volume_feature_set == "kline_taker_delta":
            volume_feature_available = (
                feature.taker_delta_3 is not None and feature.taker_delta_12 is not None
            )
            volume_feature_unavailable_reason = (
                ""
                if volume_feature_available
                else feature.taker_delta_unavailable_reason or "unknown"
            )
        elif self.spec.volume_feature_set == "normalized_vpci":
            volume_feature_available = all(
                value is not None
                for value in (
                    feature.normalized_vpci,
                    feature.normalized_vpci_signal,
                    feature.normalized_vpci_slope_3,
                )
            )
            volume_feature_unavailable_reason = (
                ""
                if volume_feature_available
                else feature.normalized_vpci_unavailable_reason or "unknown"
            )

        gate_passed = evaluation.gate.passed if evaluation.gate is not None else True
        gate_failures = (
            ""
            if evaluation.gate is None
            else "; ".join(evaluation.gate.failures)
        )
        return Opportunity(
            opportunity_id=_opportunity_id(
                market,
                feature.symbol,
                evaluation.family,
                feature.event_time_ms,
            ),
            protocol_version=self.spec.protocol_version,
            rule_version=self.spec.rule_version,
            volume_feature_set=self.spec.volume_feature_set,
            asset=asset.asset,
            cohort=asset.cohort,
            market=market.value,
            symbol=feature.symbol,
            direction=evaluation.direction.value,
            family=evaluation.family.value,
            decision_time_ms=feature.event_time_ms,
            next_open_time_ms=next_open_time_ms,
            setup_strength=evaluation.score,
            reasons="; ".join(evaluation.reasons),
            invalidation=(
                None
                if evaluation.invalidation is None
                else float(evaluation.invalidation)
            ),
            eligible=evaluation.eligible,
            gate_passed=gate_passed,
            gate_failures=gate_failures,
            htf_filter_accepted=candidate.htf.accepted,
            htf_filter_failures="; ".join(candidate.htf.failures),
            execution_observed=False,
            full_r2_eligible=None,
            split=split_name,
            regime=feature.regime.label,
            btc_trend=feature.regime.btc_trend,
            breadth_ratio=feature.regime.breadth_ratio,
            analysis_eligible=eligibility[12],
            analysis_exclusion=exclusions[12],
            analysis_eligible_3=eligibility[3],
            analysis_exclusion_3=exclusions[3],
            analysis_eligible_6=eligibility[6],
            analysis_exclusion_6=exclusions[6],
            analysis_eligible_12=eligibility[12],
            analysis_exclusion_12=exclusions[12],
            analysis_eligible_72=eligibility[72],
            analysis_exclusion_72=exclusions[72],
            volume_feature_available=volume_feature_available,
            volume_feature_unavailable_reason=volume_feature_unavailable_reason,
            taker_delta_3=feature.taker_delta_3,
            taker_delta_12=feature.taker_delta_12,
            normalized_vpci=feature.normalized_vpci,
            normalized_vpci_signal=feature.normalized_vpci_signal,
            normalized_vpci_slope_3=feature.normalized_vpci_slope_3,
            forward_return_3=returns[3],
            forward_return_6=returns[6],
            forward_return_12=returns[12],
            forward_return_72=returns[72],
            long_net_return_3=long_net_returns[3],
            long_net_return_6=long_net_returns[6],
            long_net_return_12=long_net_returns[12],
            short_net_return_3=short_net_returns[3],
            short_net_return_6=short_net_returns[6],
            short_net_return_12=short_net_returns[12],
            outcome_label_3=outcome_labels[3],
            outcome_label_6=outcome_labels[6],
            outcome_label_12=outcome_labels[12],
            signal_gross_return_3=signal_gross_returns[3],
            signal_fee_return_3=signal_fee_returns[3],
            signal_slippage_return_3=signal_slippage_returns[3],
            signal_funding_return_3=signal_funding_returns[3],
            signal_net_return_3=signal_net_returns[3],
            signal_gross_return_6=signal_gross_returns[6],
            signal_fee_return_6=signal_fee_returns[6],
            signal_slippage_return_6=signal_slippage_returns[6],
            signal_funding_return_6=signal_funding_returns[6],
            signal_net_return_6=signal_net_returns[6],
            signal_gross_return_12=signal_gross_returns[12],
            signal_fee_return_12=signal_fee_returns[12],
            signal_slippage_return_12=signal_slippage_returns[12],
            signal_funding_return_12=signal_funding_returns[12],
            signal_net_return_12=signal_net_returns[12],
            f60_execution_model=(
                "next_5m_open_to_12th_close_kline_proxy"
                if is_f60_horizon
                else "unavailable_non_5m_interval"
            ),
            f60_gross_return=f60_gross_return,
            f60_fee_return=f60_fee_return,
            f60_slippage_return=f60_slippage_return,
            f60_funding_return=f60_funding_return,
            f60_net_return=f60_net_return,
            mfe_72=mfe,
            mae_72=mae,
        )

    def _continuous_features(
        self,
        candles: list[Candle],
        flows: list[OrderFlowSnapshot],
        regimes: list[MarketRegime],
    ) -> list[FeatureSnapshot | None]:
        step_ms = interval_to_milliseconds(candles[0].interval)
        output: list[FeatureSnapshot | None] = [None] * len(candles)
        segment_start = 0
        boundaries = [
            index
            for index in range(1, len(candles))
            if candles[index].open_time_ms - candles[index - 1].open_time_ms != step_ms
        ]
        for segment_end in [*boundaries, len(candles)]:
            segment = self.feature_engine.compute_series(
                candles[segment_start:segment_end],
                flows[segment_start:segment_end],
                self.spec.historical_spread_proxy_bps,
                regimes[segment_start:segment_end],
                spread_is_proxy=True,
            )
            output[segment_start:segment_end] = segment
            segment_start = segment_end
        return output

    def _with_funding_features(
        self,
        features: list[FeatureSnapshot | None],
        funding: list[FundingRate],
        symbol: str,
    ) -> list[FeatureSnapshot | None]:
        tracker = FundingRateTracker(
            maximum_points=self.settings.binance.funding_history_points,
            minimum_history=self.settings.signals.funding_zscore_minimum_history,
            maximum_symbols=1,
            lookback_ms=self.settings.signals.funding_zscore_lookback_ms,
        )
        output: list[FeatureSnapshot | None] = []
        pointer = 0
        for feature in features:
            if feature is None:
                output.append(None)
                continue
            while (
                pointer < len(funding)
                and funding[pointer].funding_time_ms < feature.event_time_ms
            ):
                item = funding[pointer]
                tracker.update(
                    FundingRatePoint(
                        symbol=symbol,
                        funding_time_ms=item.funding_time_ms,
                        rate=Decimal(str(item.rate)),
                    )
                )
                pointer += 1
            snapshot = tracker.snapshot(
                symbol,
                feature.event_time_ms,
                self.settings.signals.funding_maximum_age_ms,
            )
            output.append(
                feature.model_copy(
                    update={
                        "funding_rate": None if snapshot is None else snapshot.rate,
                        "funding_zscore": None if snapshot is None else snapshot.zscore,
                    }
                )
            )
        return output

    def _higher_timeframe_context_index(
        self,
        candles: list[Candle],
        regimes: list[MarketRegime],
    ) -> StrictContextIndex:
        if self.spec.interval != "5m":
            return StrictContextIndex({})
        regime_by_close = {
            candle.close_time_ms: regime for candle, regime in zip(candles, regimes, strict=True)
        }
        features_by_interval: dict[str, list[FeatureSnapshot | None]] = {}
        for interval in HIGHER_TIMEFRAME_INTERVALS:
            aggregated = aggregate_closed_candles(candles, interval)
            if not aggregated:
                features_by_interval[interval] = []
                continue
            flows = [OrderFlowSnapshot() for _ in aggregated]
            context_regimes = [regime_by_close[candle.close_time_ms] for candle in aggregated]
            features_by_interval[interval] = self._continuous_features(
                aggregated,
                flows,
                context_regimes,
            )
        return StrictContextIndex(features_by_interval)

    @staticmethod
    def _update_excursion(live: _LiveTrade, low: float, high: float) -> None:
        entry = live.position.entry_price
        favorable, adverse = directional_excursion(live.position.direction, entry, low, high)
        live.mfe = max(live.mfe, favorable)
        live.mae = min(live.mae, adverse)

    @classmethod
    def _update_execution_excursion(
        cls, live: _LiveTrade, first_price: float, second_price: float
    ) -> None:
        cls._update_excursion(
            live,
            min(first_price, second_price),
            max(first_price, second_price),
        )

    def _close_trade(
        self,
        asset: BacktestAsset,
        market: Market,
        live: _LiveTrade,
        exit_price: float,
        exit_time_ms: int,
        exit_index: int,
        reason: ExitReason,
        funding: list[FundingRate],
        *,
        exit_on_open: bool = False,
    ) -> Trade:
        position = live.position
        cohort = asset.cohort
        if market is Market.SPOT:
            fee_bps = self.spec.costs.spot_fee_bps
            slippage_bps = self.spec.costs.spot_slippage_bps[cohort]
        else:
            fee_bps = self.spec.costs.futures_fee_bps
            slippage_bps = self.spec.costs.futures_slippage_bps[cohort]
        execution = calculate_execution_returns(
            position.direction,
            position.entry_price,
            exit_price,
            fee_bps,
            slippage_bps,
        )
        funding_return = (
            self._funding_return(position, exit_time_ms, funding)
            if market is Market.FUTURES
            else 0.0
        )
        net_return = execution.net_before_funding + funding_return
        notional = self.spec.costs.notional_usdt
        initial_risk_return = position.initial_risk / position.entry_price
        trade_identity = "|".join(
            (
                self.spec.protocol_version,
                position.decision.event_id,
                str(position.entry_time_ms),
                str(exit_time_ms),
                reason.value,
            )
        )
        trade_id = hashlib.sha256(trade_identity.encode()).hexdigest()[:24]
        entry_split = next(
            (
                split
                for split in self.spec.splits
                if int(split.start.timestamp() * 1000)
                <= position.entry_time_ms
                < int(split.end.timestamp() * 1000)
            ),
            None,
        )
        split_contained = bool(
            entry_split is not None
            and int(entry_split.start.timestamp() * 1000)
            <= exit_time_ms
            < int(entry_split.end.timestamp() * 1000)
        )
        return Trade(
            trade_id=trade_id,
            opportunity_id=_opportunity_id(
                position.decision.market,
                position.decision.symbol,
                position.decision.family,
                position.decision.event_time_ms,
            ),
            protocol_version=self.spec.protocol_version,
            rule_version=self.spec.rule_version,
            asset=asset.asset,
            cohort=cohort,
            market=market.value,
            symbol=position.decision.symbol,
            direction=position.direction.value,
            family=position.decision.family.value,
            score=position.decision.score,
            split=(
                entry_split.name
                if entry_split is not None
                else "outside_evaluation"
            ),
            split_contained=split_contained,
            regime=position.decision.regime.label,
            entry_signal_id=position.decision.event_id,
            entry_signal_time_ms=position.decision.event_time_ms,
            entry_time_ms=position.entry_time_ms,
            exit_time_ms=exit_time_ms,
            entry_price=position.entry_price,
            exit_price=exit_price,
            entry_execution_price=execution.entry_execution_price,
            exit_execution_price=execution.exit_execution_price,
            initial_stop=position.initial_stop,
            exit_reason=reason.value,
            bars_held=count_held_bars(position.entry_index, exit_index, exit_on_open=exit_on_open),
            gross_return=execution.gross_return,
            slippage_return=execution.slippage_return,
            fee_return=execution.fee_return,
            funding_return=funding_return,
            net_return=net_return,
            gross_pnl_usdt=execution.gross_return * notional,
            slippage_usdt=execution.slippage_return * notional,
            fees_usdt=execution.fee_return * notional,
            funding_pnl_usdt=funding_return * notional,
            net_pnl_usdt=net_return * notional,
            mfe=live.mfe,
            mae=live.mae,
            net_r_multiple=net_return / initial_risk_return if initial_risk_return > 0 else 0.0,
        )

    def _funding_return(
        self,
        position: PaperPosition,
        exit_time_ms: int,
        funding: list[FundingRate],
    ) -> float:
        if (
            position.direction not in {Direction.LONG, Direction.SHORT}
            or not self.spec.costs.include_funding
            or not funding
        ):
            return 0.0
        return calculate_funding_return(
            position.direction,
            position.entry_time_ms,
            exit_time_ms,
            position.entry_price,
            funding,
        )


def candle_from_values(
    *,
    market: Market,
    symbol: str,
    interval: str,
    open_time_ms: int,
    open_price: float,
    high: float,
    low: float,
    close: float,
    volume: float = 1.0,
) -> Candle:
    """Small public constructor useful for deterministic external experiments."""
    step = interval_to_milliseconds(interval)
    return Candle(
        market=market,
        symbol=symbol,
        interval=interval,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + step - 1,
        open=Decimal(str(open_price)),
        high=Decimal(str(high)),
        low=Decimal(str(low)),
        close=Decimal(str(close)),
        volume=Decimal(str(volume)),
        quote_volume=Decimal(str(volume * close)),
        trade_count=1,
        taker_buy_base_volume=Decimal(str(volume / 2)),
        taker_buy_quote_volume=Decimal(str(volume * close / 2)),
        is_closed=True,
    )
