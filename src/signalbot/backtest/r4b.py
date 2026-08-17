"""Quarantined deterministic R4b draft formula library.

The functions in this module remain available for unit-level provenance tests,
but this protocol must not be replayed or promoted.  Independent review found
that its H5 predicate cannot precede the H1 base rank exit and that H2--H4 can
use the next bar's open in entry eligibility.  The later ``R4B_CAUSAL_V1``
design also requires a different, at-least-20-symbol point-in-time universe and
microstructure data that are not represented here.
"""

from __future__ import annotations

import hashlib
import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from itertools import pairwise
from typing import Literal

from pydantic import ConfigDict, field_validator, model_validator

from signalbot.config import StrictModel
from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.indicators.core import atr_series, ema_series

R4B_DRAFT_STATUS = "QUARANTINED_UNRUNNABLE"
R4B_DRAFT_BLOCKERS = (
    "H5 rank<0.50 cannot precede the H1 base rank<0.70 exit",
    "H2-H4 entry eligibility can depend on the next 5m open",
    "R4B_CAUSAL_V1 requires a different >=20-symbol point-in-time universe",
    "Families A/B require unavailable historical receipt-time microstructure data",
)


def assert_r4b_draft_runnable() -> None:
    """Fail closed if a caller attempts to build a runner around this draft."""

    raise RuntimeError(
        f"R4b draft status is {R4B_DRAFT_STATUS}: " + "; ".join(R4B_DRAFT_BLOCKERS)
    )


class R4bDiagnosticSpec(StrictModel):
    """Frozen deterministic R4b diagnostic contract.

    The local eight-asset panel can only provide an exposed-sample diagnostic.
    Literal integer fields and exact-value validation make performance-directed
    threshold changes require a new protocol version rather than silently
    changing this contract.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    protocol_version: str = "r4b_deterministic_hypotheses_v1"
    expected_assets: Literal[8] = 8
    h1_formation_end_lag_bars: Literal[72] = 72
    h1_formation_start_lag_bars: Literal[2088] = 2088
    h1_atr_period: Literal[288] = 288
    h1_volatility_lookback_bars: Literal[576] = 576
    h1_entry_rank: float = 0.9
    h1_regime_ecdf: float = 0.5
    h1_rank_exit: float = 0.7
    h1_rank_invalidation: float = 0.5
    h1_stop_atr_multiple: float = 1.5
    h1_cost_survival_multiple: float = 4.0
    h1_timeout_bars: Literal[576] = 576
    hourly_fast_ema: Literal[24] = 24
    hourly_slow_ema: Literal[96] = 96
    hourly_atr_period: Literal[96] = 96
    hourly_regime_lookback: Literal[1440] = 1440
    h2_trend_percentile: float = 0.8
    h3_trend_percentile: float = 0.2
    intraday_atr_period: Literal[48] = 48
    intraday_vwma_period: Literal[48] = 48
    trend_stop_lookback: Literal[6] = 6
    trend_stop_buffer_atr: float = 0.25
    trend_target_lookback: Literal[96] = 96
    trend_timeout_bars: Literal[72] = 72
    reward_risk_multiple: float = 1.5
    cost_survival_multiple: float = 3.0
    h4_support_lookback: Literal[288] = 288
    h4_support_exclusion: Literal[12] = 12
    h4_penetration_bars: Literal[3] = 3
    h4_min_penetration_atr: float = 0.1
    h4_max_penetration_atr: float = 0.75
    h4_trend_percentile_veto: float = 0.2
    h4_target_vwma_period: Literal[96] = 96
    h4_stop_buffer_atr: float = 0.25
    h4_timeout_bars: Literal[36] = 36
    h5_min_holding_bars: Literal[12] = 12
    h5_rank_exit: float = 0.5
    h5_regime_exit: float = 0.5

    @field_validator("protocol_version")
    @classmethod
    def nonempty_protocol_version(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("protocol_version must be non-empty")
        return value

    @model_validator(mode="after")
    def frozen_float_thresholds(self) -> R4bDiagnosticSpec:
        expected = {
            "h1_entry_rank": 0.9,
            "h1_regime_ecdf": 0.5,
            "h1_rank_exit": 0.7,
            "h1_rank_invalidation": 0.5,
            "h1_stop_atr_multiple": 1.5,
            "h1_cost_survival_multiple": 4.0,
            "h2_trend_percentile": 0.8,
            "h3_trend_percentile": 0.2,
            "trend_stop_buffer_atr": 0.25,
            "reward_risk_multiple": 1.5,
            "cost_survival_multiple": 3.0,
            "h4_min_penetration_atr": 0.1,
            "h4_max_penetration_atr": 0.75,
            "h4_trend_percentile_veto": 0.2,
            "h4_stop_buffer_atr": 0.25,
            "h5_rank_exit": 0.5,
            "h5_regime_exit": 0.5,
        }
        changed = [name for name, value in expected.items() if getattr(self, name) != value]
        if changed:
            raise ValueError(f"frozen R4b thresholds changed: {', '.join(changed)}")
        return self


DEFAULT_R4B_SPEC = R4bDiagnosticSpec()


class R4bHypothesis(StrEnum):
    H1_RELATIVE_MOMENTUM = "h1_relative_momentum"
    H2_TREND_PULLBACK_RECLAIM = "h2_trend_pullback_reclaim"
    H3_RALLY_FAILURE_SHORT = "h3_rally_failure_short"
    H4_FALSE_DOWNSIDE_BREAK = "h4_false_downside_break"
    H5_PAIRED_EXIT = "h5_paired_exit"


class R4bRole(StrEnum):
    H1_SPOT_LONG_ENTRY = "h1_spot_long_entry"
    H1_SPOT_LONG_EXIT = "h1_spot_long_exit"
    H2_FUTURES_LONG_ENTRY = "h2_futures_long_entry"
    H2_FUTURES_LONG_EXIT = "h2_futures_long_exit"
    H3_FUTURES_SHORT_ENTRY = "h3_futures_short_entry"
    H3_FUTURES_SHORT_EXIT = "h3_futures_short_exit"
    H4_SPOT_LONG_ENTRY = "h4_spot_long_entry"
    H4_SPOT_LONG_EXIT = "h4_spot_long_exit"
    H5_PAIRED_EXIT = "h5_paired_exit"


class R4bSide(StrEnum):
    LONG = "long"
    SHORT = "short"


class R4bExitReason(StrEnum):
    H1_RANK_INVALIDATION = "h1_rank_invalidation"
    H1_RANK_EXIT = "h1_rank_exit"
    TWO_CLOSE_VWMA_EXIT = "two_close_vwma_exit"
    H5_PAIRED_EXIT = "h5_paired_exit"
    TIMEOUT = "timeout"


@dataclass(frozen=True, slots=True)
class R4bEntrySignal:
    event_id: str
    hypothesis: R4bHypothesis
    role: R4bRole
    market: Market
    symbol: str
    decision_time_ms: int
    side: R4bSide
    decision_price: float
    entry_price_estimate: float
    stop_price: float
    target_price: float | None
    timeout_bars: int
    structural_start_index: int | None
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class R4bRelativeMomentum:
    raw_momentum: float
    demeaned_momentum: float
    cross_section_percentile: float


@dataclass(frozen=True, slots=True)
class R4bHourlyTrendPoint:
    close_time_ms: int
    ema24: float
    ema96: float
    atr96: float
    normalized_trend: float


@dataclass(frozen=True, slots=True)
class R4bHourlyContext:
    """Strictly completed hourly inputs supplied to an H2/H3 decision."""

    as_of_ms: int
    trend_percentile: float
    ema24_now: float
    ema24_six_hours_prior: float
    cross_section_median_trend: float
    prior_60d_median_trend: float


def r4b_event_id(
    protocol_version: str,
    role: R4bRole,
    market: Market,
    symbol: str,
    decision_time_ms: int,
) -> str:
    """Return the deterministic identity for one R4b decision role."""
    normalized_symbol = _normalized_symbol(symbol)
    if not protocol_version.strip():
        raise ValueError("protocol_version must be non-empty")
    if decision_time_ms < 0:
        raise ValueError("decision_time_ms must be non-negative")
    identity = "|".join(
        (
            protocol_version,
            role.value,
            market.value,
            normalized_symbol,
            str(decision_time_ms),
        )
    )
    return hashlib.sha256(identity.encode()).hexdigest()[:24]


def wilder_atr_series(
    candles: Sequence[Candle], period: int
) -> tuple[float | None, ...]:
    """Compute causal Wilder ATR after validating one closed, contiguous series."""
    if period <= 0:
        raise ValueError("ATR period must be positive")
    _validate_complete_series(candles)
    return tuple(atr_series(candles, period))


def vwma_series(candles: Sequence[Candle], period: int) -> tuple[float | None, ...]:
    """Return close/quote-volume VWMA using only each point's trailing window."""
    if period <= 0:
        raise ValueError("VWMA period must be positive")
    _validate_complete_series(candles)
    result: list[float | None] = [None] * len(candles)
    if len(candles) < period:
        return tuple(result)

    weighted = [float(candle.close) * float(candle.quote_volume) for candle in candles]
    volumes = [float(candle.quote_volume) for candle in candles]
    weighted_sum = math.fsum(weighted[:period])
    volume_sum = math.fsum(volumes[:period])
    result[period - 1] = weighted_sum / volume_sum if volume_sum > 0 else None
    for index in range(period, len(candles)):
        weighted_sum += weighted[index] - weighted[index - period]
        volume_sum += volumes[index] - volumes[index - period]
        result[index] = weighted_sum / volume_sum if volume_sum > 0 else None
    return tuple(result)


def empirical_cdf(value: float, strictly_prior_values: Sequence[float]) -> float:
    """Weak empirical CDF, with the current observation excluded by contract."""
    _require_finite("value", value)
    if not strictly_prior_values:
        raise ValueError("strictly_prior_values must be non-empty")
    _require_finite_values("strictly_prior_values", strictly_prior_values)
    return sum(item <= value for item in strictly_prior_values) / len(strictly_prior_values)


def cross_sectional_percentiles(
    values_by_symbol: Mapping[str, float],
    *,
    expected_size: int | None = None,
) -> dict[str, float]:
    """Return PRANK=(average_rank-0.5)/N with deterministic average ties."""
    if not values_by_symbol:
        raise ValueError("cross section must be non-empty")
    if expected_size is not None and len(values_by_symbol) != expected_size:
        raise ValueError(f"cross section must contain exactly {expected_size} assets")
    normalized: dict[str, float] = {}
    for symbol, value in values_by_symbol.items():
        key = _normalized_symbol(symbol)
        if key in normalized:
            raise ValueError("cross-section symbols must be unique after normalization")
        _require_finite(f"cross-section value for {key}", value)
        normalized[key] = value
    ordered = sorted(normalized.items(), key=lambda item: (item[1], item[0]))
    result: dict[str, float] = {}
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and ordered[end][1] == ordered[start][1]:
            end += 1
        first_rank = start + 1
        last_rank = end
        midrank = (first_rank + last_rank) / 2
        percentile = (midrank - 0.5) / len(ordered)
        for symbol, _ in ordered[start:end]:
            result[symbol] = percentile
        start = end
    return {symbol: result[symbol] for symbol in sorted(result)}


def prior_rolling_median(
    values: Sequence[float], index: int, lookback: int
) -> float | None:
    """Return a strictly-prior rolling median; the value at index is never read."""
    if lookback <= 0:
        raise ValueError("median lookback must be positive")
    if index < 0 or index > len(values):
        raise IndexError("median index is outside the series")
    if index < lookback:
        return None
    window = values[index - lookback : index]
    _require_finite_values("rolling median window", window)
    return float(statistics.median(window))


def h1_relative_momentum(
    closes: Sequence[float],
    index: int,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> float | None:
    """M=log(C[t-72]/C[t-2088]), ending the seven-day formation six hours early."""
    if index < 0 or index >= len(closes):
        raise IndexError("momentum index is outside the close series")
    start_index = index - spec.h1_formation_start_lag_bars
    end_index = index - spec.h1_formation_end_lag_bars
    if start_index < 0:
        return None
    start = closes[start_index]
    end = closes[end_index]
    _require_positive_finite("formation start close", start)
    _require_positive_finite("formation end close", end)
    return math.log(end / start)


def h1_relative_momentum_from_candles(
    candles: Sequence[Candle],
    index: int,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> float | None:
    """Candle-validated H1 momentum for the Spot 5m diagnostic."""
    if index < 0 or index >= len(candles):
        raise IndexError("momentum index is outside the candle series")
    if index < spec.h1_formation_start_lag_bars:
        return None
    relevant = candles[index - spec.h1_formation_start_lag_bars : index + 1]
    _validate_complete_series(relevant, required_interval="5m")
    if candles[index].market is not Market.SPOT:
        raise ValueError("H1 is a Spot-only hypothesis")
    closes = tuple(float(candle.close) for candle in candles[: index + 1])
    return h1_relative_momentum(closes, index, spec)


def h1_cross_sectional_state(
    momentum_by_symbol: Mapping[str, float],
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> dict[str, R4bRelativeMomentum]:
    """Demean H1 momentum and rank it across the frozen eight-asset panel."""
    if len(momentum_by_symbol) != spec.expected_assets:
        raise ValueError(f"H1 requires exactly {spec.expected_assets} assets")
    normalized = {_normalized_symbol(key): value for key, value in momentum_by_symbol.items()}
    if len(normalized) != len(momentum_by_symbol):
        raise ValueError("H1 symbols must be unique after normalization")
    _require_finite_values("H1 momentum", tuple(normalized.values()))
    median = float(statistics.median(normalized.values()))
    demeaned = {symbol: value - median for symbol, value in normalized.items()}
    ranks = cross_sectional_percentiles(demeaned, expected_size=spec.expected_assets)
    return {
        symbol: R4bRelativeMomentum(normalized[symbol], demeaned[symbol], ranks[symbol])
        for symbol in sorted(normalized)
    }


def h1_realized_volatility_bps(
    closes: Sequence[float],
    index: int,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> float | None:
    """Return 10,000*sqrt(sum(r^2)) over 576 trailing closed 5m log returns.

    This is a path-volatility magnitude in basis points, without annualization
    or division by the number of returns. The radical is stated explicitly
    because it was lost in one text extraction of the source recommendation.
    """
    if index < 0 or index >= len(closes):
        raise IndexError("volatility index is outside the close series")
    lookback = spec.h1_volatility_lookback_bars
    if index < lookback:
        return None
    window = closes[index - lookback : index + 1]
    for offset, close in enumerate(window):
        _require_positive_finite(f"volatility close[{offset}]", close)
    squared_returns = (
        math.log(current / previous) ** 2
        for previous, current in pairwise(window)
    )
    return 10_000 * math.sqrt(math.fsum(squared_returns))


def h1_realized_volatility_bps_from_candles(
    candles: Sequence[Candle],
    index: int,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> float | None:
    """Candle-validated H1 path volatility over 576 closed, contiguous 5m returns."""
    if index < 0 or index >= len(candles):
        raise IndexError("volatility index is outside the candle series")
    lookback = spec.h1_volatility_lookback_bars
    if index < lookback:
        return None
    relevant = candles[index - lookback : index + 1]
    _validate_complete_series(relevant, required_interval="5m")
    if candles[index].market is not Market.SPOT:
        raise ValueError("H1 is a Spot-only hypothesis")
    closes = tuple(float(candle.close) for candle in relevant)
    return h1_realized_volatility_bps(closes, lookback, spec)


def h1_passes_cost_survival(
    realized_volatility_bps: float,
    estimated_roundtrip_cost_bps: float,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> bool:
    """Require trailing path volatility to cover at least four roundtrip costs."""
    _require_finite("realized_volatility_bps", realized_volatility_bps)
    _require_finite("estimated_roundtrip_cost_bps", estimated_roundtrip_cost_bps)
    if realized_volatility_bps < 0 or estimated_roundtrip_cost_bps < 0:
        raise ValueError("volatility and cost must be non-negative")
    return (
        realized_volatility_bps
        >= spec.h1_cost_survival_multiple * estimated_roundtrip_cost_bps
    )


def h1_spot_entry_signal(
    *,
    symbol: str,
    decision_time_ms: int,
    decision_close: float,
    entry_price_estimate: float,
    atr288: float,
    current_rank: float,
    prior_rank: float,
    market_regime_ecdf: float,
    realized_volatility_bps: float,
    estimated_roundtrip_cost_bps: float,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> R4bEntrySignal | None:
    """Create H1 only on a top-decile transition in a non-adverse regime."""
    _validate_probability("current_rank", current_rank)
    _validate_probability("prior_rank", prior_rank)
    _validate_probability("market_regime_ecdf", market_regime_ecdf)
    _require_positive_finite("decision_close", decision_close)
    _require_positive_finite("entry_price_estimate", entry_price_estimate)
    _require_positive_finite("atr288", atr288)
    if current_rank < spec.h1_entry_rank:
        return None
    if prior_rank >= spec.h1_entry_rank:
        return None
    if market_regime_ecdf < spec.h1_regime_ecdf:
        return None
    if not h1_passes_cost_survival(
        realized_volatility_bps, estimated_roundtrip_cost_bps, spec
    ):
        return None
    stop = entry_price_estimate - spec.h1_stop_atr_multiple * atr288
    if stop <= 0:
        return None
    symbol = _normalized_symbol(symbol)
    role = R4bRole.H1_SPOT_LONG_ENTRY
    return R4bEntrySignal(
        event_id=r4b_event_id(
            spec.protocol_version, role, Market.SPOT, symbol, decision_time_ms
        ),
        hypothesis=R4bHypothesis.H1_RELATIVE_MOMENTUM,
        role=role,
        market=Market.SPOT,
        symbol=symbol,
        decision_time_ms=decision_time_ms,
        side=R4bSide.LONG,
        decision_price=decision_close,
        entry_price_estimate=entry_price_estimate,
        stop_price=stop,
        target_price=None,
        timeout_bars=spec.h1_timeout_bars,
        structural_start_index=None,
        reasons=(
            "cross-sectional rank entered top decile",
            "market regime ECDF at or above median",
            "trailing path volatility covered four estimated roundtrip costs",
            "frozen 1.5 ATR288 structural stop",
        ),
    )


def h1_exit_reason(
    current_rank: float,
    held_bars: int,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> R4bExitReason | None:
    _validate_probability("current_rank", current_rank)
    if held_bars < 0:
        raise ValueError("held_bars must be non-negative")
    if current_rank < spec.h1_rank_invalidation:
        return R4bExitReason.H1_RANK_INVALIDATION
    if current_rank < spec.h1_rank_exit:
        return R4bExitReason.H1_RANK_EXIT
    if held_bars >= spec.h1_timeout_bars:
        return R4bExitReason.TIMEOUT
    return None


def hourly_trend_series(
    completed_hourly_candles: Sequence[Candle],
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> tuple[R4bHourlyTrendPoint | None, ...]:
    """Compute completed-1h T=(EMA24-EMA96)/ATR96 causally."""
    _validate_complete_series(completed_hourly_candles, required_interval="1h")
    closes = [float(candle.close) for candle in completed_hourly_candles]
    fast = ema_series(closes, spec.hourly_fast_ema)
    slow = ema_series(closes, spec.hourly_slow_ema)
    atr = atr_series(completed_hourly_candles, spec.hourly_atr_period)
    result: list[R4bHourlyTrendPoint | None] = [None] * len(completed_hourly_candles)
    for index, candle in enumerate(completed_hourly_candles):
        ema24 = fast[index]
        ema96 = slow[index]
        atr96 = atr[index]
        if ema24 is None or ema96 is None or atr96 is None or atr96 <= 0:
            continue
        result[index] = R4bHourlyTrendPoint(
            close_time_ms=candle.close_time_ms,
            ema24=ema24,
            ema96=ema96,
            atr96=atr96,
            normalized_trend=(ema24 - ema96) / atr96,
        )
    return tuple(result)


def latest_strictly_completed_index(
    completed_candles: Sequence[Candle], decision_time_ms: int
) -> int | None:
    """Return the latest candle whose close is strictly before the decision."""
    if decision_time_ms < 0:
        raise ValueError("decision_time_ms must be non-negative")
    latest: int | None = None
    for index, candle in enumerate(completed_candles):
        if not candle.is_closed:
            raise ValueError("completed-candle series contains an unclosed candle")
        if index and candle.open_time_ms <= completed_candles[index - 1].open_time_ms:
            raise ValueError("completed candles must be strictly ordered")
        if candle.close_time_ms < decision_time_ms:
            latest = index
        else:
            break
    return latest


def normalized_vwma_distance(close: float, vwma: float, atr: float) -> float:
    _require_positive_finite("close", close)
    _require_positive_finite("vwma", vwma)
    _require_positive_finite("atr", atr)
    return (close - vwma) / atr


def passes_reward_cost_gate(
    side: R4bSide,
    entry_price: float,
    stop_price: float,
    target_price: float,
    estimated_cost_bps: float,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> bool:
    """Apply the frozen 1.5R-plus-cost and three-times-cost survival gate."""
    for name, value in (
        ("entry_price", entry_price),
        ("stop_price", stop_price),
        ("target_price", target_price),
    ):
        _require_positive_finite(name, value)
    _require_finite("estimated_cost_bps", estimated_cost_bps)
    if estimated_cost_bps < 0:
        raise ValueError("estimated_cost_bps must be non-negative")

    if side is R4bSide.LONG:
        risk = entry_price - stop_price
        target_distance = target_price - entry_price
    else:
        risk = stop_price - entry_price
        target_distance = entry_price - target_price
    if risk <= 0 or target_distance <= 0:
        return False
    cost_distance = entry_price * estimated_cost_bps / 10_000
    target_distance_bps = target_distance / entry_price * 10_000
    return (
        target_distance
        >= spec.reward_risk_multiple * risk + cost_distance
        and target_distance_bps >= spec.cost_survival_multiple * estimated_cost_bps
    )


def h2_futures_long_entry_signal(
    candles: Sequence[Candle],
    index: int,
    *,
    vwma48: Sequence[float | None],
    atr48: Sequence[float | None],
    hourly: R4bHourlyContext,
    entry_price_estimate: float,
    estimated_cost_bps: float,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> R4bEntrySignal | None:
    """Evaluate the deterministic completed-hour trend pullback/reclaim long."""
    current, previous, current_atr, previous_atr, current_vwma, previous_vwma = (
        _trend_inputs(candles, index, vwma48, atr48, hourly)
    )
    if current.market is not Market.FUTURES:
        raise ValueError("H2 is a Futures-only hypothesis")
    previous_z = normalized_vwma_distance(
        float(previous.close), previous_vwma, previous_atr
    )
    current_z = normalized_vwma_distance(float(current.close), current_vwma, current_atr)
    if hourly.trend_percentile < spec.h2_trend_percentile:
        return None
    if hourly.ema24_now <= hourly.ema24_six_hours_prior:
        return None
    if hourly.cross_section_median_trend < hourly.prior_60d_median_trend:
        return None
    if not -1 <= previous_z <= 0:
        return None
    if not 0 < current_z <= 1:
        return None
    if float(current.close) <= float(previous.high):
        return None

    recent = candles[index - spec.trend_stop_lookback + 1 : index + 1]
    prior_target = candles[index - spec.trend_target_lookback : index]
    stop = min(float(candle.low) for candle in recent) - spec.trend_stop_buffer_atr * current_atr
    target = max(float(candle.high) for candle in prior_target)
    _require_positive_finite("entry_price_estimate", entry_price_estimate)
    entry = entry_price_estimate
    if stop <= 0:
        return None
    if not passes_reward_cost_gate(
        R4bSide.LONG, entry, stop, target, estimated_cost_bps, spec
    ):
        return None
    return _entry_signal(
        spec=spec,
        hypothesis=R4bHypothesis.H2_TREND_PULLBACK_RECLAIM,
        role=R4bRole.H2_FUTURES_LONG_ENTRY,
        candle=current,
        side=R4bSide.LONG,
        entry=entry,
        stop=stop,
        target=target,
        timeout_bars=spec.trend_timeout_bars,
        structural_start_index=index - spec.trend_stop_lookback + 1,
        reasons=(
            "completed-hour trend percentile and slope confirmed",
            "five-minute pullback reclaimed VWMA48 and prior high",
            "target survived reward-risk and cost gates",
        ),
    )


def h3_futures_short_entry_signal(
    candles: Sequence[Candle],
    index: int,
    *,
    vwma48: Sequence[float | None],
    atr48: Sequence[float | None],
    hourly: R4bHourlyContext,
    entry_price_estimate: float,
    estimated_cost_bps: float,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> R4bEntrySignal | None:
    """Evaluate the deterministic completed-hour rally-failure short."""
    current, previous, current_atr, previous_atr, current_vwma, previous_vwma = (
        _trend_inputs(candles, index, vwma48, atr48, hourly)
    )
    if current.market is not Market.FUTURES:
        raise ValueError("H3 is a Futures-only hypothesis")
    previous_z = normalized_vwma_distance(
        float(previous.close), previous_vwma, previous_atr
    )
    current_z = normalized_vwma_distance(float(current.close), current_vwma, current_atr)
    if hourly.trend_percentile > spec.h3_trend_percentile:
        return None
    if hourly.ema24_now >= hourly.ema24_six_hours_prior:
        return None
    if hourly.cross_section_median_trend > hourly.prior_60d_median_trend:
        return None
    if not 0 <= previous_z <= 1:
        return None
    if not -1 <= current_z < 0:
        return None
    if float(current.close) >= float(previous.low):
        return None

    recent = candles[index - spec.trend_stop_lookback + 1 : index + 1]
    prior_target = candles[index - spec.trend_target_lookback : index]
    stop = max(float(candle.high) for candle in recent) + spec.trend_stop_buffer_atr * current_atr
    target = min(float(candle.low) for candle in prior_target)
    _require_positive_finite("entry_price_estimate", entry_price_estimate)
    entry = entry_price_estimate
    if not passes_reward_cost_gate(
        R4bSide.SHORT, entry, stop, target, estimated_cost_bps, spec
    ):
        return None
    return _entry_signal(
        spec=spec,
        hypothesis=R4bHypothesis.H3_RALLY_FAILURE_SHORT,
        role=R4bRole.H3_FUTURES_SHORT_ENTRY,
        candle=current,
        side=R4bSide.SHORT,
        entry=entry,
        stop=stop,
        target=target,
        timeout_bars=spec.trend_timeout_bars,
        structural_start_index=index - spec.trend_stop_lookback + 1,
        reasons=(
            "completed-hour downtrend percentile and slope confirmed",
            "five-minute rally failed below VWMA48 and prior low",
            "target survived reward-risk and cost gates",
        ),
    )


def two_close_vwma_exit(
    side: R4bSide,
    previous_close: float,
    current_close: float,
    previous_vwma: float,
    current_vwma: float,
) -> bool:
    for name, value in (
        ("previous_close", previous_close),
        ("current_close", current_close),
        ("previous_vwma", previous_vwma),
        ("current_vwma", current_vwma),
    ):
        _require_positive_finite(name, value)
    if side is R4bSide.LONG:
        return previous_close < previous_vwma and current_close < current_vwma
    return previous_close > previous_vwma and current_close > current_vwma


def h4_causal_support(
    candles: Sequence[Candle],
    index: int,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> float | None:
    """Return min(low[t-288:t-12]); the most recent 12 bars are excluded."""
    if index < 0 or index >= len(candles):
        raise IndexError("support index is outside the candle series")
    if index < spec.h4_support_lookback:
        return None
    _validate_signal_prefix(candles, index, spec.h4_support_lookback)
    support_window = candles[
        index - spec.h4_support_lookback : index - spec.h4_support_exclusion
    ]
    return min(float(candle.low) for candle in support_window)


def h4_spot_false_break_entry_signal(
    candles: Sequence[Candle],
    index: int,
    *,
    atr48: Sequence[float | None],
    vwma96: Sequence[float | None],
    hourly_trend_percentile: float,
    entry_price_estimate: float,
    estimated_cost_bps: float,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> R4bEntrySignal | None:
    """Evaluate the causal Spot false-downside-break reclaim setup."""
    minimum_index = spec.h4_support_lookback
    _validate_signal_prefix(candles, index, minimum_index)
    _validate_aligned_series("atr48", atr48, candles, index)
    _validate_aligned_series("vwma96", vwma96, candles, index)
    current = candles[index]
    if current.market is not Market.SPOT:
        raise ValueError("H4 is a Spot-only hypothesis")
    _validate_probability("hourly_trend_percentile", hourly_trend_percentile)
    if hourly_trend_percentile <= spec.h4_trend_percentile_veto:
        return None
    current_atr = _required_series_value("atr48", atr48, index)
    target = _required_series_value("vwma96", vwma96, index)
    support = h4_causal_support(candles, index, spec)
    if support is None:
        return None

    first_penetration: int | None = None
    start = index - spec.h4_penetration_bars + 1
    for candidate_index in range(start, index + 1):
        candidate_atr = _required_series_value("atr48", atr48, candidate_index)
        penetration = (support - float(candles[candidate_index].low)) / candidate_atr
        if spec.h4_min_penetration_atr <= penetration <= spec.h4_max_penetration_atr:
            first_penetration = candidate_index
            break
    if first_penetration is None:
        return None
    if float(current.close) <= support or float(current.close) <= float(candles[index - 1].high):
        return None

    stop = (
        min(float(candle.low) for candle in candles[first_penetration : index + 1])
        - spec.h4_stop_buffer_atr * current_atr
    )
    _require_positive_finite("entry_price_estimate", entry_price_estimate)
    entry = entry_price_estimate
    if stop <= 0:
        return None
    if not passes_reward_cost_gate(
        R4bSide.LONG, entry, stop, target, estimated_cost_bps, spec
    ):
        return None
    return _entry_signal(
        spec=spec,
        hypothesis=R4bHypothesis.H4_FALSE_DOWNSIDE_BREAK,
        role=R4bRole.H4_SPOT_LONG_ENTRY,
        candle=current,
        side=R4bSide.LONG,
        entry=entry,
        stop=stop,
        target=target,
        timeout_bars=spec.h4_timeout_bars,
        structural_start_index=first_penetration,
        reasons=(
            "causal support penetration was between 0.10 and 0.75 ATR48",
            "close reclaimed support and the prior-bar high",
            "VWMA96 target survived reward-risk and cost gates",
        ),
    )


def h5_paired_exit(
    *,
    held_bars: int,
    h1_rank: float,
    previous_close: float,
    current_close: float,
    previous_vwma48: float,
    current_vwma48: float,
    market_regime_ecdf: float,
    spec: R4bDiagnosticSpec = DEFAULT_R4B_SPEC,
) -> bool:
    """Return the H5 paired H1-exit overlay predicate."""
    if held_bars < 0:
        raise ValueError("held_bars must be non-negative")
    _validate_probability("h1_rank", h1_rank)
    _validate_probability("market_regime_ecdf", market_regime_ecdf)
    if held_bars < spec.h5_min_holding_bars:
        return False
    if h1_rank >= spec.h5_rank_exit or market_regime_ecdf >= spec.h5_regime_exit:
        return False
    return two_close_vwma_exit(
        R4bSide.LONG,
        previous_close,
        current_close,
        previous_vwma48,
        current_vwma48,
    )


def _trend_inputs(
    candles: Sequence[Candle],
    index: int,
    vwma48: Sequence[float | None],
    atr48: Sequence[float | None],
    hourly: R4bHourlyContext,
) -> tuple[Candle, Candle, float, float, float, float]:
    _validate_signal_prefix(candles, index, 96)
    _validate_aligned_series("vwma48", vwma48, candles, index)
    _validate_aligned_series("atr48", atr48, candles, index)
    current = candles[index]
    previous = candles[index - 1]
    if hourly.as_of_ms >= current.close_time_ms:
        raise ValueError("hourly context must close strictly before the decision")
    for name, value in (
        ("trend_percentile", hourly.trend_percentile),
        ("ema24_now", hourly.ema24_now),
        ("ema24_six_hours_prior", hourly.ema24_six_hours_prior),
        ("cross_section_median_trend", hourly.cross_section_median_trend),
        ("prior_60d_median_trend", hourly.prior_60d_median_trend),
    ):
        _require_finite(name, value)
    _validate_probability("trend_percentile", hourly.trend_percentile)
    return (
        current,
        previous,
        _required_series_value("atr48", atr48, index),
        _required_series_value("atr48", atr48, index - 1),
        _required_series_value("vwma48", vwma48, index),
        _required_series_value("vwma48", vwma48, index - 1),
    )


def _entry_signal(
    *,
    spec: R4bDiagnosticSpec,
    hypothesis: R4bHypothesis,
    role: R4bRole,
    candle: Candle,
    side: R4bSide,
    entry: float,
    stop: float,
    target: float | None,
    timeout_bars: int,
    structural_start_index: int | None,
    reasons: tuple[str, ...],
) -> R4bEntrySignal:
    symbol = _normalized_symbol(candle.symbol)
    return R4bEntrySignal(
        event_id=r4b_event_id(
            spec.protocol_version,
            role,
            candle.market,
            symbol,
            candle.close_time_ms,
        ),
        hypothesis=hypothesis,
        role=role,
        market=candle.market,
        symbol=symbol,
        decision_time_ms=candle.close_time_ms,
        side=side,
        decision_price=float(candle.close),
        entry_price_estimate=entry,
        stop_price=stop,
        target_price=target,
        timeout_bars=timeout_bars,
        structural_start_index=structural_start_index,
        reasons=reasons,
    )


def _validate_complete_series(
    candles: Sequence[Candle], required_interval: str | None = None
) -> None:
    if not candles:
        return
    first = candles[0]
    interval = required_interval or first.interval
    if first.interval != interval:
        raise ValueError(f"candle interval must be {interval}")
    step_ms = interval_to_milliseconds(interval)
    for index, candle in enumerate(candles):
        if not candle.is_closed:
            raise ValueError("indicator inputs must contain only closed candles")
        if (
            candle.market is not first.market
            or candle.symbol != first.symbol
            or candle.interval != interval
        ):
            raise ValueError("indicator inputs must be one market/symbol/interval series")
        if index and candle.open_time_ms != candles[index - 1].open_time_ms + step_ms:
            raise ValueError("indicator inputs must be contiguous")


def _validate_signal_prefix(
    candles: Sequence[Candle], index: int, minimum_index: int
) -> None:
    if index < 0 or index >= len(candles):
        raise IndexError("signal index is outside the candle series")
    if index < minimum_index:
        raise ValueError(f"signal requires index >= {minimum_index}")
    relevant = candles[index - minimum_index : index + 1]
    _validate_complete_series(relevant, required_interval="5m")


def _validate_aligned_series(
    name: str,
    values: Sequence[float | None],
    candles: Sequence[Candle],
    index: int,
) -> None:
    if len(values) != len(candles):
        raise ValueError(f"{name} must align one-to-one with candles")
    if index >= len(values):
        raise IndexError(f"{name} does not contain the decision index")


def _required_series_value(
    name: str, values: Sequence[float | None], index: int
) -> float:
    value = values[index]
    if value is None:
        raise ValueError(f"{name} is unavailable at index {index}")
    _require_positive_finite(f"{name}[{index}]", value)
    return value


def _normalized_symbol(symbol: str) -> str:
    value = symbol.strip().upper()
    if not value:
        raise ValueError("symbol must be non-empty")
    return value


def _validate_probability(name: str, value: float) -> None:
    _require_finite(name, value)
    if not 0 <= value <= 1:
        raise ValueError(f"{name} must be in [0, 1]")


def _require_finite(name: str, value: float) -> None:
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")


def _require_positive_finite(name: str, value: float) -> None:
    _require_finite(name, value)
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def _require_finite_values(name: str, values: Sequence[float]) -> None:
    if any(not math.isfinite(value) for value in values):
        raise ValueError(f"{name} must contain only finite values")
