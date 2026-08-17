from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from signalbot.backtest.engine import (
    FundingRate,
    calculate_execution_returns,
    calculate_funding_return,
    count_held_bars,
    directional_excursion,
)
from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import Candle, FeatureSnapshot, SignalDecision
from signalbot.signals.positions import (
    ExitPolicy,
    ExitReason,
    PaperPosition,
    TechnicalExitEngine,
)


@dataclass(frozen=True, slots=True)
class Outcome:
    event_id: str
    horizon_seconds: int
    mfe: float
    mae: float
    close_return: float
    observed_until_ms: int


class OutcomeEvaluator:
    def evaluate(
        self, decision: SignalDecision, candles: list[Candle], horizon_seconds: int
    ) -> Outcome | None:
        end = decision.event_time_ms + horizon_seconds * 1000
        future = [
            c for c in candles if c.open_time_ms > decision.event_time_ms and c.close_time_ms <= end
        ]
        if not future or decision.price <= 0:
            return None
        entry = Decimal(decision.price)
        highs = [float(c.high / entry - 1) for c in future]
        lows = [float(c.low / entry - 1) for c in future]
        return Outcome(
            decision.event_id,
            horizon_seconds,
            max(highs),
            min(lows),
            float(future[-1].close / entry - 1),
            future[-1].close_time_ms,
        )


class DirectionalHitStatus(StrEnum):
    HIT = "hit"
    MISS = "miss"
    AMBIGUOUS = "ambiguous"


class OneRPathStatus(StrEnum):
    TARGET_FIRST = "target_first"
    INVALIDATION_FIRST = "invalidation_first"
    COLLISION = "collision"
    TIMEOUT = "timeout"
    INVALID_INVALIDATION = "invalid_invalidation"


class RecommendationOutcomeExclusionReason(StrEnum):
    NEXT_BAR_UNAVAILABLE = "next_bar_unavailable"
    NEXT_BAR_NOT_CONTIGUOUS = "next_bar_not_contiguous"
    INSUFFICIENT_HORIZON = "insufficient_horizon"
    DATA_GAP_IN_HORIZON = "data_gap_in_horizon"


@dataclass(frozen=True, slots=True)
class OneRPathOutcome:
    status: OneRPathStatus
    invalidation_price: float | None
    target_price: float | None
    risk_fraction: float | None
    observed_until_ms: int | None


@dataclass(frozen=True, slots=True)
class RecommendationOutcome:
    event_id: str
    direction: Direction
    horizon_bars: int
    entry_time_ms: int
    exit_time_ms: int
    entry_price: float
    exit_price: float
    gross_return: float
    slippage_return: float
    fee_return: float
    funding_return: float
    net_return: float
    mfe: float
    mae: float
    hit_status: DirectionalHitStatus
    one_r_path: OneRPathOutcome


@dataclass(frozen=True, slots=True)
class RecommendationOutcomeExclusion:
    event_id: str
    horizon_bars: int
    reason: RecommendationOutcomeExclusionReason
    expected_open_time_ms: int
    actual_open_time_ms: int | None
    observed_bars: int


type RecommendationOutcomeResult = RecommendationOutcome | RecommendationOutcomeExclusion


class TechnicalExitOutcomeExclusionReason(StrEnum):
    UNSUPPORTED_TIMEFRAME = "unsupported_timeframe"
    UNSUPPORTED_DIRECTION = "unsupported_direction"
    SPOT_SHORT_NOT_EXECUTABLE = "spot_short_not_executable"
    SPLIT_LEAKAGE = "split_leakage"
    NEXT_BAR_UNAVAILABLE = "next_bar_unavailable"
    NEXT_BAR_NOT_CONTIGUOUS = "next_bar_not_contiguous"
    INVALID_CANDLE_SERIES = "invalid_candle_series"
    INVALID_INVALIDATION = "invalid_invalidation"
    DATA_GAP = "data_gap"
    FEATURE_UNAVAILABLE = "feature_unavailable"
    FEATURE_MISMATCH = "feature_mismatch"
    END_OF_DATA_BEFORE_EXIT = "end_of_data_before_exit"


@dataclass(frozen=True, slots=True)
class CounterfactualTechnicalExitOutcome:
    event_id: str
    policy_version: str
    market: Market
    direction: Direction
    source_information_only: bool
    entry_action_label: str
    exit_action_label: str
    entry_time_ms: int
    exit_time_ms: int
    exit_signal_observed_at_ms: int
    entry_price: float
    exit_price: float
    initial_stop: float
    active_stop: float
    exit_reason: ExitReason
    execution_model: str
    bars_held: int
    gross_return: float
    slippage_return: float
    fee_return: float
    funding_return: float
    net_return: float
    mfe: float
    mae: float
    opposite_signal_evaluated: bool
    order_placed: bool


@dataclass(frozen=True, slots=True)
class CounterfactualTechnicalExitExclusion:
    event_id: str
    reason: TechnicalExitOutcomeExclusionReason
    expected_open_time_ms: int
    actual_open_time_ms: int | None
    observed_bars: int
    observed_until_ms: int


type CounterfactualTechnicalExitResult = (
    CounterfactualTechnicalExitOutcome | CounterfactualTechnicalExitExclusion
)


def classify_directional_hit(
    net_return: float, margin_bps: float
) -> DirectionalHitStatus:
    """Classify a signed, after-cost return with a symmetric no-call zone."""

    if not math.isfinite(net_return):
        raise ValueError("net return must be finite")
    if not math.isfinite(margin_bps) or margin_bps < 0:
        raise ValueError("hit margin must be finite and non-negative")
    margin = margin_bps / 10_000
    if net_return > margin:
        return DirectionalHitStatus.HIT
    if net_return < -margin:
        return DirectionalHitStatus.MISS
    return DirectionalHitStatus.AMBIGUOUS


def evaluate_one_r_path(
    direction: Direction,
    entry_price: float,
    invalidation_price: float | None,
    candles: Sequence[Candle],
) -> OneRPathOutcome:
    """Resolve a 1R target against invalidation without inventing intrabar order."""

    if direction not in {Direction.LONG, Direction.SHORT}:
        raise ValueError("1R path direction must be long or short")
    if not math.isfinite(entry_price) or entry_price <= 0:
        raise ValueError("1R path entry price must be finite and positive")
    observed_until_ms = candles[-1].close_time_ms if candles else None
    if invalidation_price is None or not math.isfinite(invalidation_price):
        return OneRPathOutcome(
            OneRPathStatus.INVALID_INVALIDATION,
            invalidation_price,
            None,
            None,
            observed_until_ms,
        )

    if direction is Direction.LONG:
        valid_invalidation = 0 < invalidation_price < entry_price
        risk = entry_price - invalidation_price
        target = entry_price + risk
    else:
        valid_invalidation = invalidation_price > entry_price
        risk = invalidation_price - entry_price
        target = entry_price - risk
        valid_invalidation = valid_invalidation and target > 0
    if not valid_invalidation:
        return OneRPathOutcome(
            OneRPathStatus.INVALID_INVALIDATION,
            invalidation_price,
            None,
            None,
            observed_until_ms,
        )

    risk_fraction = risk / entry_price
    for candle in candles:
        open_price = float(candle.open)
        if direction is Direction.LONG:
            if open_price >= target:
                open_status = OneRPathStatus.TARGET_FIRST
            elif open_price <= invalidation_price:
                open_status = OneRPathStatus.INVALIDATION_FIRST
            else:
                open_status = None
        else:
            if open_price <= target:
                open_status = OneRPathStatus.TARGET_FIRST
            elif open_price >= invalidation_price:
                open_status = OneRPathStatus.INVALIDATION_FIRST
            else:
                open_status = None
        if open_status is not None:
            return OneRPathOutcome(
                open_status,
                invalidation_price,
                target,
                risk_fraction,
                candle.open_time_ms,
            )
        high = float(candle.high)
        low = float(candle.low)
        if direction is Direction.LONG:
            target_touched = high >= target
            invalidation_touched = low <= invalidation_price
        else:
            target_touched = low <= target
            invalidation_touched = high >= invalidation_price
        if target_touched and invalidation_touched:
            status = OneRPathStatus.COLLISION
        elif target_touched:
            status = OneRPathStatus.TARGET_FIRST
        elif invalidation_touched:
            status = OneRPathStatus.INVALIDATION_FIRST
        else:
            continue
        return OneRPathOutcome(
            status,
            invalidation_price,
            target,
            risk_fraction,
            candle.close_time_ms,
        )
    return OneRPathOutcome(
        OneRPathStatus.TIMEOUT,
        invalidation_price,
        target,
        risk_fraction,
        observed_until_ms,
    )


class RecommendationOutcomeEvaluator:
    """Evaluate one alert from the next contiguous bar open over a full bar horizon."""

    def evaluate(
        self,
        decision: SignalDecision,
        candles: Sequence[Candle],
        horizon_bars: int,
        *,
        fee_bps: float = 0.0,
        slippage_bps: float = 0.0,
        funding: Sequence[FundingRate] = (),
        hit_margin_bps: float = 0.0,
    ) -> RecommendationOutcomeResult:
        if horizon_bars <= 0:
            raise ValueError("outcome horizon must be a positive number of bars")
        if decision.direction not in {Direction.LONG, Direction.SHORT}:
            raise ValueError("recommendation direction must be long or short")
        self._validate_policy(fee_bps, slippage_bps, hit_margin_bps)

        step_ms = interval_to_milliseconds(decision.timeframe)
        expected_entry_ms = decision.event_time_ms + 1
        self._validate_candles(decision, candles, step_ms)
        self._validate_funding(funding)

        future = [candle for candle in candles if candle.open_time_ms >= expected_entry_ms]
        if not future:
            return RecommendationOutcomeExclusion(
                decision.event_id,
                horizon_bars,
                RecommendationOutcomeExclusionReason.NEXT_BAR_UNAVAILABLE,
                expected_entry_ms,
                None,
                0,
            )
        if future[0].open_time_ms != expected_entry_ms:
            return RecommendationOutcomeExclusion(
                decision.event_id,
                horizon_bars,
                RecommendationOutcomeExclusionReason.NEXT_BAR_NOT_CONTIGUOUS,
                expected_entry_ms,
                future[0].open_time_ms,
                0,
            )

        path: list[Candle] = []
        expected_open_ms = expected_entry_ms
        for candle in future:
            if len(path) == horizon_bars:
                break
            if candle.open_time_ms != expected_open_ms:
                return RecommendationOutcomeExclusion(
                    decision.event_id,
                    horizon_bars,
                    RecommendationOutcomeExclusionReason.DATA_GAP_IN_HORIZON,
                    expected_open_ms,
                    candle.open_time_ms,
                    len(path),
                )
            path.append(candle)
            expected_open_ms += step_ms
        if len(path) != horizon_bars:
            return RecommendationOutcomeExclusion(
                decision.event_id,
                horizon_bars,
                RecommendationOutcomeExclusionReason.INSUFFICIENT_HORIZON,
                expected_open_ms,
                None,
                len(path),
            )

        entry_price = float(path[0].open)
        exit_price = float(path[-1].close)
        execution = calculate_execution_returns(
            decision.direction,
            entry_price,
            exit_price,
            fee_bps,
            slippage_bps,
        )
        funding_return = (
            calculate_funding_return(
                decision.direction,
                path[0].open_time_ms,
                path[-1].close_time_ms,
                entry_price,
                list(funding),
            )
            if decision.market is Market.FUTURES
            else 0.0
        )
        net_return = execution.net_before_funding + funding_return
        mfe, mae = directional_excursion(
            decision.direction,
            entry_price,
            min(float(candle.low) for candle in path),
            max(float(candle.high) for candle in path),
        )
        invalidation = (
            float(decision.invalidation) if decision.invalidation is not None else None
        )
        return RecommendationOutcome(
            event_id=decision.event_id,
            direction=decision.direction,
            horizon_bars=horizon_bars,
            entry_time_ms=path[0].open_time_ms,
            exit_time_ms=path[-1].close_time_ms,
            entry_price=entry_price,
            exit_price=exit_price,
            gross_return=execution.gross_return,
            slippage_return=execution.slippage_return,
            fee_return=execution.fee_return,
            funding_return=funding_return,
            net_return=net_return,
            mfe=mfe,
            mae=mae,
            hit_status=classify_directional_hit(net_return, hit_margin_bps),
            one_r_path=evaluate_one_r_path(
                decision.direction,
                entry_price,
                invalidation,
                path,
            ),
        )

    @staticmethod
    def _validate_candles(
        decision: SignalDecision, candles: Sequence[Candle], step_ms: int
    ) -> None:
        previous_open_ms: int | None = None
        for candle in candles:
            if (
                candle.market is not decision.market
                or candle.symbol != decision.symbol
                or candle.interval != decision.timeframe
            ):
                raise ValueError("outcome candles must match the recommendation series")
            if not candle.is_closed:
                raise ValueError("outcome candles must be closed")
            if candle.close_time_ms != candle.open_time_ms + step_ms - 1:
                raise ValueError("outcome candle close time does not match its interval")
            if previous_open_ms is not None and candle.open_time_ms <= previous_open_ms:
                raise ValueError("outcome candles must be strictly ordered and unique")
            previous_open_ms = candle.open_time_ms

    @staticmethod
    def _validate_policy(
        fee_bps: float, slippage_bps: float, hit_margin_bps: float
    ) -> None:
        if not math.isfinite(fee_bps) or fee_bps < 0:
            raise ValueError("outcome fees must be finite and non-negative")
        if not math.isfinite(slippage_bps) or slippage_bps < 0:
            raise ValueError("outcome slippage must be finite and non-negative")
        if not math.isfinite(hit_margin_bps) or hit_margin_bps < 0:
            raise ValueError("hit margin must be finite and non-negative")

    @staticmethod
    def _validate_funding(funding: Sequence[FundingRate]) -> None:
        for item in funding:
            if not math.isfinite(item.rate):
                raise ValueError("funding rates must be finite")
            if item.mark_price is not None and (
                not math.isfinite(item.mark_price) or item.mark_price <= 0
            ):
                raise ValueError("funding mark prices must be finite and positive")


class CounterfactualTechnicalExitEvaluator:
    """Evaluate one 5m alert with the shared closed-candle technical-exit policy.

    Opposite-signal exits are deliberately outside this single-alert contract.
    Stops known before a bar act at its open or within that bar. Trend, time,
    and trailing decisions made from a close cannot act before the next bar.
    """

    POLICY_VERSION = "counterfactual-technical-exit-v1"
    _STEP_MS = 300_000

    def __init__(self, policy: ExitPolicy) -> None:
        self.engine = TechnicalExitEngine(policy)

    def evaluate(
        self,
        decision: SignalDecision,
        candles: Sequence[Candle],
        features: Sequence[FeatureSnapshot | None],
        *,
        split_start_ms: int,
        split_end_ms: int,
        fee_bps: float = 0.0,
        slippage_bps: float = 0.0,
        funding: Sequence[FundingRate] = (),
    ) -> CounterfactualTechnicalExitResult:
        decision = SignalDecision.model_validate(
            decision.model_dump(mode="python", warnings="none")
        )
        if split_start_ms < 0 or split_end_ms <= split_start_ms:
            raise ValueError("technical-exit split bounds must be ordered")
        RecommendationOutcomeEvaluator._validate_policy(fee_bps, slippage_bps, 0.0)
        RecommendationOutcomeEvaluator._validate_funding(funding)

        expected_entry_ms = decision.event_time_ms + 1
        if decision.timeframe != "5m":
            return self._exclude(
                decision,
                TechnicalExitOutcomeExclusionReason.UNSUPPORTED_TIMEFRAME,
                expected_entry_ms,
            )
        if decision.direction not in {Direction.LONG, Direction.SHORT}:
            return self._exclude(
                decision,
                TechnicalExitOutcomeExclusionReason.UNSUPPORTED_DIRECTION,
                expected_entry_ms,
            )
        if decision.market is Market.SPOT and decision.direction is Direction.SHORT:
            return self._exclude(
                decision,
                TechnicalExitOutcomeExclusionReason.SPOT_SHORT_NOT_EXECUTABLE,
                expected_entry_ms,
            )
        if not split_start_ms <= decision.event_time_ms < split_end_ms:
            return self._exclude(
                decision,
                TechnicalExitOutcomeExclusionReason.SPLIT_LEAKAGE,
                expected_entry_ms,
            )
        if len(features) != len(candles):
            return self._exclude(
                decision,
                TechnicalExitOutcomeExclusionReason.FEATURE_UNAVAILABLE,
                expected_entry_ms,
            )

        invalid = self._invalid_candle_series(decision, candles)
        if invalid is not None:
            return self._exclude(
                decision,
                TechnicalExitOutcomeExclusionReason.INVALID_CANDLE_SERIES,
                expected_entry_ms,
                actual_open_time_ms=invalid,
            )

        future = [
            (candle, feature)
            for candle, feature in zip(candles, features, strict=True)
            if candle.open_time_ms >= expected_entry_ms
        ]
        if not future:
            return self._exclude(
                decision,
                TechnicalExitOutcomeExclusionReason.NEXT_BAR_UNAVAILABLE,
                expected_entry_ms,
            )
        first = future[0][0]
        if first.open_time_ms != expected_entry_ms:
            return self._exclude(
                decision,
                TechnicalExitOutcomeExclusionReason.NEXT_BAR_NOT_CONTIGUOUS,
                expected_entry_ms,
                actual_open_time_ms=first.open_time_ms,
            )
        if self._crosses_split(first, split_start_ms, split_end_ms):
            return self._exclude(
                decision,
                TechnicalExitOutcomeExclusionReason.SPLIT_LEAKAGE,
                expected_entry_ms,
                actual_open_time_ms=first.open_time_ms,
            )

        position = self.engine.open_position(decision, first, 0)
        if position is None:
            return self._exclude(
                decision,
                TechnicalExitOutcomeExclusionReason.INVALID_INVALIDATION,
                expected_entry_ms,
                actual_open_time_ms=first.open_time_ms,
            )

        pending_exit: ExitReason | None = None
        pending_observed_at_ms: int | None = None
        mfe = 0.0
        mae = 0.0
        for index, (candle, feature) in enumerate(future):
            expected_open_ms = expected_entry_ms + index * self._STEP_MS
            if candle.open_time_ms != expected_open_ms:
                reason = (
                    TechnicalExitOutcomeExclusionReason.SPLIT_LEAKAGE
                    if expected_open_ms >= split_end_ms
                    or candle.open_time_ms >= split_end_ms
                    else TechnicalExitOutcomeExclusionReason.DATA_GAP
                )
                return self._exclude(
                    decision,
                    reason,
                    expected_open_ms,
                    actual_open_time_ms=candle.open_time_ms,
                    observed_bars=index,
                    observed_until_ms=future[index - 1][0].close_time_ms,
                )
            if self._crosses_split(candle, split_start_ms, split_end_ms):
                return self._exclude(
                    decision,
                    TechnicalExitOutcomeExclusionReason.SPLIT_LEAKAGE,
                    expected_open_ms,
                    actual_open_time_ms=candle.open_time_ms,
                    observed_bars=index,
                    observed_until_ms=(
                        decision.event_time_ms
                        if index == 0
                        else future[index - 1][0].close_time_ms
                    ),
                )

            if index > 0:
                open_fill = self.engine.stop_at_open(position, float(candle.open))
                if open_fill is not None:
                    mfe, mae = self._update_excursion(
                        decision.direction,
                        position.entry_price,
                        float(candle.open),
                        float(candle.open),
                        mfe,
                        mae,
                    )
                    return self._finish(
                        decision,
                        position,
                        open_fill.reason,
                        open_fill.price,
                        candle.open_time_ms,
                        candle.open_time_ms,
                        "counterfactual_stop_gap_at_open",
                        count_held_bars(0, index, exit_on_open=True),
                        fee_bps,
                        slippage_bps,
                        funding,
                        mfe,
                        mae,
                    )
                if pending_exit is not None:
                    mfe, mae = self._update_excursion(
                        decision.direction,
                        position.entry_price,
                        float(candle.open),
                        float(candle.open),
                        mfe,
                        mae,
                    )
                    if pending_observed_at_ms is None:  # pragma: no cover - invariant
                        raise RuntimeError("pending technical exit lacks an observation time")
                    return self._finish(
                        decision,
                        position,
                        pending_exit,
                        float(candle.open),
                        candle.open_time_ms,
                        pending_observed_at_ms,
                        "counterfactual_next_bar_open",
                        count_held_bars(0, index, exit_on_open=True),
                        fee_bps,
                        slippage_bps,
                        funding,
                        mfe,
                        mae,
                    )

            intrabar_fill = self.engine.stop_in_bar(position, candle)
            if intrabar_fill is not None:
                mfe, mae = self._update_excursion(
                    decision.direction,
                    position.entry_price,
                    min(float(candle.open), intrabar_fill.price),
                    max(float(candle.open), intrabar_fill.price),
                    mfe,
                    mae,
                )
                return self._finish(
                    decision,
                    position,
                    intrabar_fill.reason,
                    intrabar_fill.price,
                    candle.close_time_ms,
                    candle.close_time_ms,
                    "counterfactual_stop_touch_in_closed_bar",
                    count_held_bars(0, index, exit_on_open=False),
                    fee_bps,
                    slippage_bps,
                    funding,
                    mfe,
                    mae,
                )

            if feature is None:
                return self._exclude(
                    decision,
                    TechnicalExitOutcomeExclusionReason.FEATURE_UNAVAILABLE,
                    expected_open_ms,
                    actual_open_time_ms=candle.open_time_ms,
                    observed_bars=index,
                    observed_until_ms=candle.close_time_ms,
                )
            if not self._feature_matches(candle, feature):
                return self._exclude(
                    decision,
                    TechnicalExitOutcomeExclusionReason.FEATURE_MISMATCH,
                    expected_open_ms,
                    actual_open_time_ms=candle.open_time_ms,
                    observed_bars=index,
                    observed_until_ms=candle.close_time_ms,
                )
            mfe, mae = self._update_excursion(
                decision.direction,
                position.entry_price,
                float(candle.low),
                float(candle.high),
                mfe,
                mae,
            )
            pending_exit = self.engine.after_close(
                position,
                candle,
                feature,
                index,
                False,
            )
            pending_observed_at_ms = (
                candle.close_time_ms if pending_exit is not None else None
            )

        last = future[-1][0]
        return self._exclude(
            decision,
            TechnicalExitOutcomeExclusionReason.END_OF_DATA_BEFORE_EXIT,
            last.open_time_ms + self._STEP_MS,
            observed_bars=len(future),
            observed_until_ms=last.close_time_ms,
        )

    @staticmethod
    def _invalid_candle_series(
        decision: SignalDecision,
        candles: Sequence[Candle],
    ) -> int | None:
        previous_open_ms: int | None = None
        for candle in candles:
            invalid = (
                candle.market is not decision.market
                or candle.symbol != decision.symbol
                or candle.interval != decision.timeframe
                or not candle.is_closed
                or candle.close_time_ms
                != candle.open_time_ms + CounterfactualTechnicalExitEvaluator._STEP_MS - 1
                or (
                    previous_open_ms is not None
                    and candle.open_time_ms <= previous_open_ms
                )
            )
            if invalid:
                return candle.open_time_ms
            previous_open_ms = candle.open_time_ms
        return None

    @staticmethod
    def _crosses_split(candle: Candle, split_start_ms: int, split_end_ms: int) -> bool:
        return not (
            split_start_ms <= candle.open_time_ms
            and candle.close_time_ms < split_end_ms
        )

    @staticmethod
    def _feature_matches(candle: Candle, feature: FeatureSnapshot) -> bool:
        return (
            feature.market is candle.market
            and feature.symbol == candle.symbol
            and feature.interval == candle.interval
            and feature.event_time_ms == candle.close_time_ms
        )

    @staticmethod
    def _update_excursion(
        direction: Direction,
        entry_price: float,
        low_price: float,
        high_price: float,
        mfe: float,
        mae: float,
    ) -> tuple[float, float]:
        favorable, adverse = directional_excursion(
            direction,
            entry_price,
            low_price,
            high_price,
        )
        return max(mfe, favorable), min(mae, adverse)

    def _finish(
        self,
        decision: SignalDecision,
        position: PaperPosition,
        reason: ExitReason,
        exit_price: float,
        exit_time_ms: int,
        exit_signal_observed_at_ms: int,
        execution_model: str,
        bars_held: int,
        fee_bps: float,
        slippage_bps: float,
        funding: Sequence[FundingRate],
        mfe: float,
        mae: float,
    ) -> CounterfactualTechnicalExitOutcome:
        execution = calculate_execution_returns(
            decision.direction,
            position.entry_price,
            exit_price,
            fee_bps,
            slippage_bps,
        )
        funding_return = (
            calculate_funding_return(
                decision.direction,
                position.entry_time_ms,
                exit_time_ms,
                position.entry_price,
                list(funding),
            )
            if decision.market is Market.FUTURES
            else 0.0
        )
        action_contract = decision.model_copy(update={"metadata": {}})
        exit_contract = action_contract.model_copy(
            update={"family": SignalFamily.TECHNICAL_EXIT}
        )
        return CounterfactualTechnicalExitOutcome(
            event_id=decision.event_id,
            policy_version=self.POLICY_VERSION,
            market=decision.market,
            direction=decision.direction,
            source_information_only=(
                decision.metadata.get("informational_only") is True
            ),
            entry_action_label=action_contract.action_label,
            exit_action_label=exit_contract.action_label,
            entry_time_ms=position.entry_time_ms,
            exit_time_ms=exit_time_ms,
            exit_signal_observed_at_ms=exit_signal_observed_at_ms,
            entry_price=position.entry_price,
            exit_price=exit_price,
            initial_stop=position.initial_stop,
            active_stop=position.active_stop,
            exit_reason=reason,
            execution_model=execution_model,
            bars_held=bars_held,
            gross_return=execution.gross_return,
            slippage_return=execution.slippage_return,
            fee_return=execution.fee_return,
            funding_return=funding_return,
            net_return=execution.net_before_funding + funding_return,
            mfe=mfe,
            mae=mae,
            opposite_signal_evaluated=False,
            order_placed=False,
        )

    @staticmethod
    def _exclude(
        decision: SignalDecision,
        reason: TechnicalExitOutcomeExclusionReason,
        expected_open_time_ms: int,
        *,
        actual_open_time_ms: int | None = None,
        observed_bars: int = 0,
        observed_until_ms: int | None = None,
    ) -> CounterfactualTechnicalExitExclusion:
        return CounterfactualTechnicalExitExclusion(
            event_id=decision.event_id,
            reason=reason,
            expected_open_time_ms=expected_open_time_ms,
            actual_open_time_ms=actual_open_time_ms,
            observed_bars=observed_bars,
            observed_until_ms=(
                decision.event_time_ms
                if observed_until_ms is None
                else observed_until_ms
            ),
        )
