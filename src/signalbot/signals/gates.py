from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from signalbot.config import SignalSettings
from signalbot.domain.enums import Direction, Market
from signalbot.domain.models import FeatureSnapshot, GateEvaluation


@dataclass(frozen=True, slots=True)
class StrictPriorHtfEvaluation:
    """Non-compensating 15m and 1h trend-alignment diagnostic."""

    accepted: bool
    failures: tuple[str, ...]


def evaluate_strict_prior_htf(
    feature: FeatureSnapshot,
    direction: Direction,
    contexts: Mapping[str, FeatureSnapshot],
) -> StrictPriorHtfEvaluation:
    """Require both strictly-prior 15m and 1h contexts to align.

    Context feature construction already requires 210 completed bars. This
    predicate additionally verifies interval identity and strict event-time
    ordering so a malformed caller cannot turn a same-close value into a pass.
    """

    if direction not in {Direction.LONG, Direction.SHORT}:
        return StrictPriorHtfEvaluation(False, ("direction is not long or short",))

    failures: list[str] = []
    for interval in ("15m", "1h"):
        context = contexts.get(interval)
        if context is None:
            failures.append(f"missing mature strictly-prior {interval} context")
            continue
        if context.interval != interval:
            failures.append(f"{interval} context interval mismatch")
            continue
        if context.event_time_ms >= feature.event_time_ms:
            failures.append(f"{interval} context is not strictly prior")
            continue
        aligned = (
            context.price > context.ema20 > context.ema50
            if direction is Direction.LONG
            else context.price < context.ema20 < context.ema50
        )
        if not aligned:
            failures.append(f"{interval} close/EMA20/EMA50 not directionally aligned")
    return StrictPriorHtfEvaluation(not failures, tuple(failures))


def _directional(value: float, direction: Direction) -> float:
    if direction is Direction.LONG:
        return value
    if direction is Direction.SHORT:
        return -value
    return 0.0


def _trend_score(
    feature: FeatureSnapshot,
    direction: Direction,
    contexts: Mapping[str, FeatureSnapshot],
    settings: SignalSettings,
) -> tuple[int, list[str]]:
    if settings.entry_policy == "r2_pit_htf_exec":
        strict = evaluate_strict_prior_htf(feature, direction, contexts)
        return (100 if strict.accepted else 0), list(strict.failures)

    score = 0
    failures: list[str] = []
    if _directional(feature.ema20_slope_atr, direction) > 0:
        score += 30
    aligned = feature.ema20 > feature.ema50
    if (aligned and direction is Direction.LONG) or (
        not aligned and direction is Direction.SHORT
    ):
        score += 25
    if feature.adx >= 20:
        score += 15
    elif feature.adx >= 15:
        score += 8

    if not settings.gate_use_higher_timeframes:
        return min(100, round(score / 0.7)), failures

    for interval, points in (("15m", 15), ("1h", 15)):
        context = contexts.get(interval)
        if context is None:
            failures.append(f"missing strictly available {interval} context")
            continue
        context_aligned = context.ema20 > context.ema50
        price_aligned = context.price > context.ema20
        if direction is Direction.SHORT:
            context_aligned = context.ema20 < context.ema50
            price_aligned = context.price < context.ema20
        if context_aligned and price_aligned:
            score += points
    return min(100, score), failures


def _participation_score(feature: FeatureSnapshot, direction: Direction) -> int:
    activity = max(feature.volume_zscore, feature.trade_count_zscore)
    activity_score = (
        35 if activity >= 1.0 else 25 if activity >= 0.5 else 10 if activity >= 0 else 0
    )

    taker = _directional(feature.taker_imbalance, direction)
    taker_score = 35 if taker >= 0.10 else 25 if taker >= 0.03 else 10 if taker >= 0 else 0

    cvd = _directional(feature.cvd_pressure, direction)
    cvd_score = 30 if cvd >= 0.05 else 20 if cvd >= 0.015 else 10 if cvd >= 0 else 0
    return activity_score + taker_score + cvd_score


def _crowding_risk_score(feature: FeatureSnapshot, direction: Direction) -> int:
    if feature.funding_zscore is None:
        return 0
    crowded = _directional(feature.funding_zscore, direction)
    if crowded < 0.5:
        return 10
    if crowded < 1.0:
        return 25
    if crowded < 2.0:
        return 50
    if crowded < 3.0:
        return 75
    return 95


def _execution_score(
    feature: FeatureSnapshot,
    direction: Direction,
    settings: SignalSettings,
) -> tuple[int, list[str]]:
    spread = feature.spread_bps
    if spread is None or spread > settings.maximum_spread_bps:
        return 0, ["fresh decision-time BBO spread unavailable or too wide"]
    if settings.entry_policy == "r2_pit_htf_exec":
        capacity = (
            feature.ask_quote_capacity
            if direction is Direction.LONG
            else feature.bid_quote_capacity
        )
        if feature.spread_is_proxy or feature.book_age_ms is None or capacity is None:
            return 0, ["fresh observed BBO price and quantity are required"]
        if not 0 <= feature.book_age_ms <= settings.book_maximum_age_ms:
            return 0, [
                "observed BBO age "
                f"{feature.book_age_ms}ms is outside [0, "
                f"{settings.book_maximum_age_ms}]ms"
            ]
        if capacity < settings.execution_notional_usdt:
            return 0, [
                "top-of-book quote capacity "
                f"{capacity:.2f} < {settings.execution_notional_usdt:.2f} USDT"
            ]
    if feature.spread_is_proxy:
        return 65, []
    return (100 if spread <= settings.maximum_spread_bps / 2 else 75), []


def _volume_policy_score(
    feature: FeatureSnapshot,
    direction: Direction,
    settings: SignalSettings,
) -> tuple[int, list[str]]:
    feature_set = settings.volume_feature_set
    if feature_set == "none":
        return 100, []

    if feature_set == "kline_taker_delta":
        short = feature.taker_delta_3
        long = feature.taker_delta_12
        if short is None or long is None:
            reason = feature.taker_delta_unavailable_reason or "unknown"
            return 0, [f"volume policy: closed-kline D3/D12 unavailable ({reason})"]
        aligned_short = _directional(short, direction)
        aligned_long = _directional(long, direction)
        failures = []
        if aligned_long < 0:
            failures.append(f"volume policy: directional D12 {aligned_long:.4f} < 0")
        threshold = settings.volume.taker_short_threshold
        if aligned_short < threshold:
            failures.append(
                f"volume policy: directional D3 {aligned_short:.4f} < {threshold:.4f}"
            )
        return (100 if not failures else 0), failures

    if feature_set == "normalized_vpci":
        value = feature.normalized_vpci
        signal = feature.normalized_vpci_signal
        slope = feature.normalized_vpci_slope_3
        if value is None or signal is None or slope is None:
            reason = feature.normalized_vpci_unavailable_reason or "unknown"
            return 0, [f"volume policy: normalized VPCI state unavailable ({reason})"]
        components = (
            ("level", _directional(value, direction)),
            ("signal spread", _directional(value - signal, direction)),
            ("3-bar slope", _directional(slope, direction)),
        )
        failures = [
            f"volume policy: directional VPCI {name} {component:.6f} <= 0"
            for name, component in components
            if component <= 0
        ]
        return (100 if not failures else 0), failures

    raise ValueError(f"unsupported volume feature set: {feature_set}")


def evaluate_entry_gates(
    feature: FeatureSnapshot,
    direction: Direction,
    contexts: Mapping[str, FeatureSnapshot],
    settings: SignalSettings,
) -> GateEvaluation:
    """Evaluate four non-compensating gates using point-in-time features only."""
    trend, failures = _trend_score(feature, direction, contexts, settings)
    r2_policy = settings.entry_policy == "r2_pit_htf_exec"
    participation = (
        _participation_score(feature, direction)
        if settings.gate_use_participation and not r2_policy
        else 100
    )
    crowding = (
        _crowding_risk_score(feature, direction)
        if settings.gate_use_crowding and not r2_policy
        else 0
    )
    execution, execution_failures = _execution_score(feature, direction, settings)
    completeness = max(0, min(100, round(feature.data_completeness * 100)))
    volume_policy, volume_failures = (
        (100, [])
        if r2_policy
        else _volume_policy_score(feature, direction, settings)
    )

    if trend < settings.trend_gate:
        failures.append(f"trend {trend} < {settings.trend_gate}")
    if participation < settings.participation_gate:
        failures.append(
            f"participation {participation} < {settings.participation_gate}"
        )
    if (
        settings.gate_use_participation
        and not r2_policy
        and not feature.closed_kline_flow_available
    ):
        participation = 0
        failures.append("canonical closed-kline flow unavailable")
    if (
        settings.gate_use_crowding
        and not r2_policy
        and feature.market is Market.FUTURES
        and feature.funding_zscore is None
    ):
        crowding = 100
        failures.append("missing futures funding crowding input")
    if crowding >= settings.crowding_risk_cap:
        failures.append(f"crowding risk {crowding} >= {settings.crowding_risk_cap}")
    if execution < settings.execution_gate:
        failures.append(f"execution {execution} < {settings.execution_gate}")
    failures.extend(execution_failures)
    if completeness < settings.completeness_gate:
        failures.append(f"completeness {completeness} < {settings.completeness_gate}")
    failures.extend(volume_failures)

    proxy_fields = ("spread_bps",) if feature.spread_is_proxy else ()
    return GateEvaluation(
        trend_score=trend,
        participation_score=participation,
        crowding_risk_score=crowding,
        execution_score=execution,
        completeness_score=completeness,
        volume_policy_score=volume_policy,
        volume_feature_set=settings.volume_feature_set,
        passed=not failures,
        failures=tuple(failures),
        proxy_fields=proxy_fields,
    )
