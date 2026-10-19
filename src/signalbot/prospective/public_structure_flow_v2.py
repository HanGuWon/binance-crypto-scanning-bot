from __future__ import annotations

import hashlib
import json
import statistics
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import Decimal
from functools import partial
from itertools import pairwise
from math import isclose
from pathlib import Path
from typing import Literal, cast

from signalbot.config import Settings
from signalbot.data.candles import interval_to_milliseconds
from signalbot.data.microstructure import closed_kline_flow
from signalbot.domain.enums import Direction, Market
from signalbot.domain.models import Candle, FeatureSnapshot
from signalbot.indicators.core import (
    atr_series,
    bollinger_width_series,
    ema_series,
    macd_histogram_series,
)
from signalbot.persistence.repository import (
    PublicStructureFlowConflictError,
    PublicStructureFlowTerminalWrite,
    SqlRepository,
)
from signalbot.prospective.public_structure_flow_contract_v2 import (
    PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256,
    PUBLIC_STRUCTURE_FLOW_SCHEMA_VERSION,
    PUBLIC_STRUCTURE_FLOW_SOURCE_IDENTITY,
    PUBLIC_STRUCTURE_FLOW_VERSION,
    public_structure_flow_contract,
)
from signalbot.signals.gates import evaluate_strict_prior_htf
from signalbot.signals.market_watch import PriorityGainer

CandidateStatus = Literal["UNAVAILABLE", "NO_SIGNAL", "OBSERVE", "DIRECTIONAL"]
CandidateDirection = Literal["long", "short"]
PendingResolutionKind = Literal["incomplete", "ready", "disqualified"]

_CONTRACT = public_structure_flow_contract()
_NUMERIC = cast(dict[str, object], _CONTRACT["numeric_comparison"])
_MINUTE_PRIMARY = cast(dict[str, object], _CONTRACT["minute_primary_consistency"])
_PENDING = cast(dict[str, object], _CONTRACT["pending"])
_C1 = cast(dict[str, object], _CONTRACT["c1"])
_C2 = cast(dict[str, object], _CONTRACT["c2"])

FLOAT_ABS_TOL = float(cast(float | int | str, _NUMERIC["abs_tolerance"]))
FLOAT_REL_TOL = float(cast(float | int | str, _NUMERIC["rel_tolerance"]))
PRICE_ABS_TOL = Decimal(str(_MINUTE_PRIMARY["price_abs_tolerance"]))
PRICE_REL_TOL = Decimal(str(_MINUTE_PRIMARY["price_rel_tolerance"]))
QUOTE_ABS_TOL = Decimal(str(_MINUTE_PRIMARY["quote_abs_tolerance"]))
QUOTE_REL_TOL = Decimal(str(_MINUTE_PRIMARY["quote_rel_tolerance"]))
PENDING_WAIT_MS = int(cast(int | str, _PENDING["wait_ms"]))
MAX_PENDING = int(cast(int | str, _PENDING["max_pending_per_runtime"]))
MAX_FINALIZE_RETRIES = 3
C1_FLOW_THRESHOLD = float(cast(float | int | str, _C1["signed_quote_flow_threshold"]))
C1_RESPONSE_THRESHOLD = float(cast(float | int | str, _C1["price_response_atr_threshold"]))
C2_IMPULSE_THRESHOLD = float(cast(float | int | str, _C2["impulse_atr_threshold"]))
C2_FLOW_THRESHOLD = float(cast(float | int | str, _C2["signed_quote_flow_threshold"]))


@dataclass(frozen=True, slots=True)
class CandidateEvaluation:
    candidate_id: str
    status: CandidateStatus
    direction: CandidateDirection | None
    reason: str
    metrics: dict[str, float | bool | str | None]


@dataclass(frozen=True, slots=True)
class PendingResolution:
    kind: PendingResolutionKind
    ready_at_ms: int | None
    forced_reason: str | None


@dataclass(frozen=True, slots=True)
class PublicStructureFlowObservation:
    schema_version: str
    candidate_version: str
    market: str
    symbol: str
    timeframe: str
    event_time_ms: int
    both_prior_band_touches: bool
    watch_observed_ms: int | None
    gainer_rank: int | None
    retained_gainer: bool
    population_inclusion: str | None
    price_change_percent_24h: float | None
    quote_volume_24h_usdt: float | None
    watch_expires_at_ms: int | None
    peak_quote_volume_usdt: float | None
    input_quality: str
    candidates: tuple[CandidateEvaluation, ...]
    production_entry: bool = False
    discord_delivery: bool = False
    order_placement: bool = False

    def payload(self) -> dict[str, object]:
        return asdict(self)


def observation_id(
    observation: PublicStructureFlowObservation, *, campaign_id: str = "unregistered"
) -> str:
    raw = "\0".join(
        (
            campaign_id,
            observation.candidate_version,
            observation.market,
            observation.symbol,
            observation.timeframe,
            str(observation.event_time_ms),
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def opportunity_id(*, campaign_id: str, market: Market, symbol: str, event_time_ms: int) -> str:
    raw = "\0".join((campaign_id, market.value, symbol.upper(), str(event_time_ms)))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _unavailable(
    candidate_id: str,
    reason: str,
    metrics: dict[str, float | bool | str | None] | None = None,
) -> CandidateEvaluation:
    return CandidateEvaluation(candidate_id, "UNAVAILABLE", None, reason, metrics or {})


def _inclusive_ge(value: float, threshold: float) -> bool:
    return value > threshold or isclose(
        value, threshold, rel_tol=FLOAT_REL_TOL, abs_tol=FLOAT_ABS_TOL
    )


def _inclusive_le(value: float, threshold: float) -> bool:
    return value < threshold or isclose(
        value, threshold, rel_tol=FLOAT_REL_TOL, abs_tol=FLOAT_ABS_TOL
    )


def _strict_gt(value: float, threshold: float) -> bool:
    return value > threshold and not isclose(
        value, threshold, rel_tol=FLOAT_REL_TOL, abs_tol=FLOAT_ABS_TOL
    )


def _strict_lt(value: float, threshold: float) -> bool:
    return value < threshold and not isclose(
        value, threshold, rel_tol=FLOAT_REL_TOL, abs_tol=FLOAT_ABS_TOL
    )


def _validate_tail(
    candles: Sequence[Candle],
    *,
    market: Market,
    symbol: str,
    interval: str,
    as_of_ms: int,
    minimum_count: int,
) -> list[Candle] | None:
    normalized = symbol.upper()
    rows = sorted(
        (
            candle
            for candle in candles
            if candle.market is market
            and candle.symbol == normalized
            and candle.interval == interval
            and candle.is_closed
            and candle.close_time_ms <= as_of_ms
        ),
        key=lambda candle: candle.open_time_ms,
    )
    if len(rows) < minimum_count or rows[-1].close_time_ms != as_of_ms:
        return None
    step_ms = interval_to_milliseconds(interval)
    if any(candle.open_time_ms % step_ms != 0 for candle in rows):
        return None
    if any(candle.close_time_ms != candle.open_time_ms + step_ms - 1 for candle in rows):
        return None
    if any(
        current.open_time_ms != previous.open_time_ms + step_ms
        for previous, current in pairwise(rows)
    ):
        return None
    return rows


def _bands(values: Sequence[float]) -> tuple[float, float, float] | None:
    if len(values) != 20:
        return None
    middle = statistics.fmean(values)
    deviation = statistics.pstdev(values)
    upper = middle + 2 * deviation
    lower = middle - 2 * deviation
    if middle == 0 or not _strict_gt(upper, lower):
        return None
    return lower, middle, upper


def _signed_quote_flow(rows: Sequence[Candle]) -> float | None:
    total_decimal = sum((row.quote_volume for row in rows), Decimal("0"))
    if total_decimal <= 0:
        return None
    try:
        flows = [closed_kline_flow(row) for row in rows]
    except ValueError:
        return None
    if not all(flow.available for flow in flows):
        return None
    buys = sum((row.taker_buy_quote_volume for row in rows), Decimal("0"))
    return float((2 * buys - total_decimal) / total_decimal)


def _decimal_matches(
    actual: Decimal,
    expected: Decimal,
    *,
    abs_tol: Decimal,
    rel_tol: Decimal,
) -> tuple[bool, Decimal, Decimal]:
    difference = abs(actual - expected)
    tolerance = max(abs_tol, abs(expected) * rel_tol)
    return difference <= tolerance, difference, tolerance


def _minute_primary_consistency(
    minute_rows: Sequence[Candle], current_5m: Candle
) -> tuple[bool, str, dict[str, float | bool | str | None]]:
    step_ms = 60_000
    expected_opens = [current_5m.open_time_ms + step_ms * index for index in range(5)]
    exact = [
        row
        for row in minute_rows
        if current_5m.open_time_ms <= row.open_time_ms <= current_5m.close_time_ms
    ]
    exact.sort(key=lambda row: row.open_time_ms)
    metrics: dict[str, float | bool | str | None] = {
        "minute_count": float(len(exact)),
        "minute_grid_ok": False,
        "aggregate_match": False,
    }
    if len(exact) != 5:
        return False, "missing_exact_1m_aggregation_window", metrics
    if [row.open_time_ms for row in exact] != expected_opens:
        return False, "misaligned_1m_aggregation_window", metrics
    if any(
        row.interval != "1m"
        or row.market is not current_5m.market
        or row.symbol != current_5m.symbol
        or not row.is_closed
        or row.open_time_ms % step_ms != 0
        or row.close_time_ms != row.open_time_ms + step_ms - 1
        for row in exact
    ):
        return False, "invalid_1m_identity_or_grid", metrics
    metrics["minute_grid_ok"] = True
    if any(
        row.quote_volume < 0
        or row.taker_buy_quote_volume < 0
        or row.taker_buy_quote_volume > row.quote_volume
        for row in (*exact, current_5m)
    ):
        return False, "invalid_1m_or_5m_quote_volume", metrics
    aggregate = {
        "open": exact[0].open,
        "high": max(row.high for row in exact),
        "low": min(row.low for row in exact),
        "close": exact[-1].close,
        "quote_volume": sum((row.quote_volume for row in exact), Decimal("0")),
        "taker_buy_quote_volume": sum(
            (row.taker_buy_quote_volume for row in exact), Decimal("0")
        ),
    }
    official = {
        "open": current_5m.open,
        "high": current_5m.high,
        "low": current_5m.low,
        "close": current_5m.close,
        "quote_volume": current_5m.quote_volume,
        "taker_buy_quote_volume": current_5m.taker_buy_quote_volume,
    }
    mismatches: list[str] = []
    for field in ("open", "high", "low", "close"):
        matched, difference, tolerance = _decimal_matches(
            aggregate[field],
            official[field],
            abs_tol=PRICE_ABS_TOL,
            rel_tol=PRICE_REL_TOL,
        )
        metrics[f"aggregate_{field}"] = str(aggregate[field])
        metrics[f"official_{field}"] = str(official[field])
        metrics[f"{field}_difference"] = str(difference)
        metrics[f"{field}_tolerance"] = str(tolerance)
        if not matched:
            mismatches.append(field)
    for field in ("quote_volume", "taker_buy_quote_volume"):
        matched, difference, tolerance = _decimal_matches(
            aggregate[field],
            official[field],
            abs_tol=QUOTE_ABS_TOL,
            rel_tol=QUOTE_REL_TOL,
        )
        metrics[f"aggregate_{field}"] = str(aggregate[field])
        metrics[f"official_{field}"] = str(official[field])
        metrics[f"{field}_difference"] = str(difference)
        metrics[f"{field}_tolerance"] = str(tolerance)
        if not matched:
            mismatches.append(field)
    if mismatches:
        metrics["mismatch_fields"] = ";".join(mismatches)
        return False, "1m_5m_aggregate_mismatch", metrics
    metrics["aggregate_match"] = True
    return True, "consistent", metrics


def _c1_flow_price_response(
    minute_rows: Sequence[Candle],
    current_5m: Candle,
    *,
    prior_upper: float,
    prior_lower: float,
    both_touched: bool,
    forced_unavailable_reason: str | None,
) -> CandidateEvaluation:
    candidate_id = "PUB09_FLOW_PRICE_RESPONSE"
    if forced_unavailable_reason is not None:
        return _unavailable(candidate_id, forced_unavailable_reason)
    consistent, consistency_reason, consistency_metrics = _minute_primary_consistency(
        minute_rows, current_5m
    )
    if not consistent:
        return _unavailable(candidate_id, consistency_reason, consistency_metrics)
    if both_touched:
        return CandidateEvaluation(
            candidate_id,
            "NO_SIGNAL",
            None,
            "both_prior_bands_touched_direction_withheld",
            {**consistency_metrics, "upper_touch": True, "lower_touch": True},
        )
    if len(minute_rows) < 17:
        return _unavailable(candidate_id, "insufficient_1m_history", consistency_metrics)
    recent = list(minute_rows[-3:])
    previous = list(minute_rows[-6:-3])
    fr = _signed_quote_flow(recent)
    fp = _signed_quote_flow(previous)
    if fr is None or fp is None:
        return _unavailable(candidate_id, "invalid_or_zero_1m_quote_flow", consistency_metrics)
    atr_values = atr_series(minute_rows)
    atr_before_recent = atr_values[len(minute_rows) - 4]
    if atr_before_recent is None or not _strict_gt(float(atr_before_recent), 0.0):
        return _unavailable(candidate_id, "immature_or_zero_1m_atr14", consistency_metrics)
    r3 = (float(recent[-1].close) - float(recent[0].open)) / float(atr_before_recent)
    upper_touch = _inclusive_ge(float(current_5m.high), prior_upper)
    lower_touch = _inclusive_le(float(current_5m.low), prior_lower)
    upper_slowdown = (
        _inclusive_ge(fr, C1_FLOW_THRESHOLD)
        and _inclusive_le(r3, C1_RESPONSE_THRESHOLD)
        and upper_touch
    )
    lower_slowdown = (
        _inclusive_le(fr, -C1_FLOW_THRESHOLD)
        and _inclusive_ge(r3, -C1_RESPONSE_THRESHOLD)
        and lower_touch
    )
    upper_confirmed = upper_slowdown and recent[-1].close < recent[-2].low
    lower_confirmed = lower_slowdown and recent[-1].close > recent[-2].high
    metrics: dict[str, float | bool | str | None] = {
        **consistency_metrics,
        "flow_recent": fr,
        "flow_previous": fp,
        "flow_delta": fr - fp,
        "r3_atr": r3,
        "atr1m14_prior": float(atr_before_recent),
        "upper_touch": upper_touch,
        "lower_touch": lower_touch,
        "upper_confirmed": upper_confirmed,
        "lower_confirmed": lower_confirmed,
    }
    if upper_confirmed:
        return CandidateEvaluation(
            candidate_id,
            "DIRECTIONAL",
            "short",
            "upper_flow_pressure_failed_to_lift_price_then_1m_confirmed",
            metrics,
        )
    if lower_confirmed:
        return CandidateEvaluation(
            candidate_id,
            "DIRECTIONAL",
            "long",
            "lower_sell_pressure_failed_to_push_price_then_1m_confirmed",
            metrics,
        )
    if upper_slowdown:
        return CandidateEvaluation(
            candidate_id,
            "OBSERVE",
            None,
            "upper_flow_price_response_slowdown_without_confirmation",
            metrics,
        )
    if lower_slowdown:
        return CandidateEvaluation(
            candidate_id,
            "OBSERVE",
            None,
            "lower_flow_price_response_slowdown_without_confirmation",
            metrics,
        )
    return CandidateEvaluation(candidate_id, "NO_SIGNAL", None, "conditions_not_met", metrics)


def _valid_strict_prior_contexts(
    feature: FeatureSnapshot, contexts: Mapping[str, FeatureSnapshot]
) -> tuple[bool, str | None]:
    for interval in ("15m", "1h"):
        context = contexts.get(interval)
        if context is None:
            return False, "missing_strict_prior_htf_context"
        if (
            context.market is not feature.market
            or context.symbol != feature.symbol
            or context.interval != interval
        ):
            return False, "invalid_strict_prior_htf_identity"
        if context.event_time_ms >= feature.event_time_ms:
            return False, "non_strict_prior_context"
    return True, None


def _c2_atr_impulse(
    five_minute_rows: Sequence[Candle],
    feature: FeatureSnapshot,
    contexts: Mapping[str, FeatureSnapshot],
    *,
    prior_upper: float,
    prior_lower: float,
    both_touched: bool,
) -> CandidateEvaluation:
    candidate_id = "PUB02_ATR_IMPULSE_CONTINUATION"
    if both_touched:
        return CandidateEvaluation(
            candidate_id,
            "NO_SIGNAL",
            None,
            "both_prior_bands_touched_direction_withheld",
            {"upper_touch": True, "lower_touch": True},
        )
    valid_contexts, context_reason = _valid_strict_prior_contexts(feature, contexts)
    if not valid_contexts:
        return _unavailable(candidate_id, context_reason or "invalid_strict_prior_htf_context")
    atr_values = atr_series(five_minute_rows)
    prior_atr = atr_values[-2]
    if prior_atr is None or not _strict_gt(float(prior_atr), 0.0):
        return _unavailable(candidate_id, "immature_or_zero_prior_atr14")
    closes = [float(row.close) for row in five_minute_rows]
    widths = bollinger_width_series(closes)
    current_width = widths[-1]
    prior_width = widths[-2]
    if (
        current_width is None
        or prior_width is None
        or not _strict_gt(float(prior_width), 0.0)
    ):
        return _unavailable(candidate_id, "immature_or_zero_bollinger_width")
    current_flow = _signed_quote_flow((five_minute_rows[-1],))
    if current_flow is None:
        return _unavailable(candidate_id, "invalid_or_zero_5m_quote_flow")
    impulse = (closes[-1] - closes[-2]) / float(prior_atr)
    expansion = float(current_width) / float(prior_width)
    strict_long = evaluate_strict_prior_htf(feature, Direction.LONG, contexts)
    strict_short = evaluate_strict_prior_htf(feature, Direction.SHORT, contexts)
    long_signal = (
        _inclusive_ge(impulse, C2_IMPULSE_THRESHOLD)
        and _strict_gt(expansion, 1.0)
        and _inclusive_ge(current_flow, C2_FLOW_THRESHOLD)
        and _strict_gt(closes[-1], prior_upper)
        and strict_long.accepted
    )
    short_signal = (
        _inclusive_le(impulse, -C2_IMPULSE_THRESHOLD)
        and _strict_gt(expansion, 1.0)
        and _inclusive_le(current_flow, -C2_FLOW_THRESHOLD)
        and _strict_lt(closes[-1], prior_lower)
        and strict_short.accepted
    )
    metrics: dict[str, float | bool | str | None] = {
        "impulse_atr": impulse,
        "bandwidth_expansion": expansion,
        "flow_5m": current_flow,
        "prior_atr14": float(prior_atr),
        "prior_upper": prior_upper,
        "prior_lower": prior_lower,
        "strict_htf_long": strict_long.accepted,
        "strict_htf_short": strict_short.accepted,
        "strict_htf_long_failures": ";".join(strict_long.failures),
        "strict_htf_short_failures": ";".join(strict_short.failures),
    }
    if long_signal:
        return CandidateEvaluation(
            candidate_id,
            "DIRECTIONAL",
            "long",
            "atr_impulse_band_expansion_flow_and_htf_aligned",
            metrics,
        )
    if short_signal:
        return CandidateEvaluation(
            candidate_id,
            "DIRECTIONAL",
            "short",
            "atr_impulse_band_expansion_flow_and_htf_aligned",
            metrics,
        )
    return CandidateEvaluation(candidate_id, "NO_SIGNAL", None, "conditions_not_met", metrics)


def _c3_bb_macd_reaction(
    five_minute_rows: Sequence[Candle],
    *,
    previous_bands: tuple[float, float, float],
    current_bands: tuple[float, float, float],
    both_touched: bool,
) -> CandidateEvaluation:
    candidate_id = "PUB03_BB_MACD_REACTION"
    if both_touched:
        return CandidateEvaluation(
            candidate_id,
            "NO_SIGNAL",
            None,
            "both_prior_bands_touched_direction_withheld",
            {"upper_touch": True, "lower_touch": True},
        )
    prior_lower, _prior_middle, prior_upper = previous_bands
    current_lower, _current_middle, current_upper = current_bands
    prior_width = prior_upper - prior_lower
    current_width = current_upper - current_lower
    if not _strict_gt(prior_width, 0.0) or not _strict_gt(current_width, 0.0):
        return _unavailable(candidate_id, "zero_bollinger_band_width")
    closes = [float(row.close) for row in five_minute_rows]
    fast = ema_series(closes, 12)
    slow = ema_series(closes, 26)
    hist = macd_histogram_series(closes)
    if fast[-1] is None or slow[-1] is None or hist[-1] is None:
        return _unavailable(candidate_id, "immature_macd")
    macd_line = float(fast[-1] - slow[-1])
    histogram = float(hist[-1])
    previous_percent_b = (closes[-2] - prior_lower) / prior_width
    current_percent_b = (closes[-1] - current_lower) / current_width
    current_inside = _strict_gt(current_percent_b, 0.0) and _strict_lt(current_percent_b, 1.0)
    long_signal = (
        _inclusive_le(previous_percent_b, 0.0)
        and current_inside
        and _strict_lt(macd_line, 0.0)
        and _strict_gt(histogram, 0.0)
    )
    short_signal = (
        _inclusive_ge(previous_percent_b, 1.0)
        and current_inside
        and _strict_gt(macd_line, 0.0)
        and _strict_lt(histogram, 0.0)
    )
    metrics: dict[str, float | bool | str | None] = {
        "percent_b_previous": previous_percent_b,
        "percent_b_current": current_percent_b,
        "macd_line": macd_line,
        "macd_histogram": histogram,
    }
    if long_signal:
        return CandidateEvaluation(
            candidate_id,
            "DIRECTIONAL",
            "long",
            "lower_band_reentry_with_macd_reaction",
            metrics,
        )
    if short_signal:
        return CandidateEvaluation(
            candidate_id,
            "DIRECTIONAL",
            "short",
            "upper_band_reentry_with_macd_reaction",
            metrics,
        )
    return CandidateEvaluation(candidate_id, "NO_SIGNAL", None, "conditions_not_met", metrics)


def _minute_tail_ready(
    minute_candles: Sequence[Candle], *, market: Market, symbol: str, event_time_ms: int
) -> bool:
    return (
        _validate_tail(
            minute_candles,
            market=market,
            symbol=symbol,
            interval="1m",
            as_of_ms=event_time_ms,
            minimum_count=17,
        )
        is not None
    )


def evaluate_public_structure_flow(
    *,
    candle: Candle,
    feature: FeatureSnapshot,
    five_minute_candles: Sequence[Candle],
    minute_candles: Sequence[Candle],
    contexts: Mapping[str, FeatureSnapshot],
    watch_context: PriorityGainer | None,
    watch_observed_ms: int | None = None,
    c1_forced_unavailable_reason: str | None = None,
) -> PublicStructureFlowObservation:
    """Evaluate v2 C1/C2/C3 from frozen event-time evidence only."""

    if candle.interval != "5m" or not candle.is_closed:
        raise ValueError("public structure-flow shadow requires one closed 5m candle")
    if candle.open_time_ms % 300_000 != 0 or candle.close_time_ms != candle.open_time_ms + 299_999:
        raise ValueError("public structure-flow 5m candle is off the UTC grid")
    if (
        feature.market is not candle.market
        or feature.symbol != candle.symbol
        or feature.interval != candle.interval
        or feature.event_time_ms != candle.close_time_ms
    ):
        raise ValueError("feature identity does not match public structure-flow candle")
    if watch_context is not None and watch_context.symbol.upper() != candle.symbol:
        raise ValueError("watch identity does not match public structure-flow candle")
    if watch_observed_ms is not None and watch_observed_ms > candle.close_time_ms:
        raise ValueError("watch snapshot must not be newer than the decision close")

    five_rows = _validate_tail(
        five_minute_candles,
        market=candle.market,
        symbol=candle.symbol,
        interval="5m",
        as_of_ms=candle.close_time_ms,
        minimum_count=35,
    )
    minute_rows = _validate_tail(
        minute_candles,
        market=candle.market,
        symbol=candle.symbol,
        interval="1m",
        as_of_ms=candle.close_time_ms,
        minimum_count=17,
    )
    both_touched = False
    if five_rows is None:
        candidates = (
            _unavailable("PUB09_FLOW_PRICE_RESPONSE", "noncontiguous_or_misaligned_5m_history"),
            _unavailable(
                "PUB02_ATR_IMPULSE_CONTINUATION",
                "noncontiguous_or_misaligned_5m_history",
            ),
            _unavailable("PUB03_BB_MACD_REACTION", "noncontiguous_or_misaligned_5m_history"),
        )
        input_quality = "invalid_5m_history"
    elif five_rows[-1] != candle:
        candidates = (
            _unavailable("PUB09_FLOW_PRICE_RESPONSE", "current_5m_history_mismatch"),
            _unavailable("PUB02_ATR_IMPULSE_CONTINUATION", "current_5m_history_mismatch"),
            _unavailable("PUB03_BB_MACD_REACTION", "current_5m_history_mismatch"),
        )
        input_quality = "current_5m_history_mismatch"
    else:
        closes = [float(row.close) for row in five_rows]
        previous_bands = _bands(closes[-21:-1])
        current_bands = _bands(closes[-20:])
        if previous_bands is None or current_bands is None:
            candidates = (
                _unavailable("PUB09_FLOW_PRICE_RESPONSE", "bollinger_bands_unavailable"),
                _unavailable("PUB02_ATR_IMPULSE_CONTINUATION", "bollinger_bands_unavailable"),
                _unavailable("PUB03_BB_MACD_REACTION", "bollinger_bands_unavailable"),
            )
            input_quality = "bollinger_unavailable"
        else:
            prior_lower, _prior_middle, prior_upper = previous_bands
            both_touched = _inclusive_ge(float(candle.high), prior_upper) and _inclusive_le(
                float(candle.low), prior_lower
            )
            if minute_rows is None:
                c1 = _unavailable(
                    "PUB09_FLOW_PRICE_RESPONSE",
                    c1_forced_unavailable_reason or "noncontiguous_or_misaligned_1m_history",
                )
                c1_quality = "c1_minute_unavailable"
            else:
                c1 = _c1_flow_price_response(
                    minute_rows,
                    candle,
                    prior_upper=prior_upper,
                    prior_lower=prior_lower,
                    both_touched=both_touched,
                    forced_unavailable_reason=c1_forced_unavailable_reason,
                )
                c1_quality = "complete" if c1.status != "UNAVAILABLE" else c1.reason
            candidates = (
                c1,
                _c2_atr_impulse(
                    five_rows,
                    feature,
                    contexts,
                    prior_upper=prior_upper,
                    prior_lower=prior_lower,
                    both_touched=both_touched,
                ),
                _c3_bb_macd_reaction(
                    five_rows,
                    previous_bands=previous_bands,
                    current_bands=current_bands,
                    both_touched=both_touched,
                ),
            )
            input_quality = c1_quality

    return PublicStructureFlowObservation(
        schema_version=PUBLIC_STRUCTURE_FLOW_SCHEMA_VERSION,
        candidate_version=PUBLIC_STRUCTURE_FLOW_VERSION,
        market=candle.market.value,
        symbol=candle.symbol,
        timeframe=candle.interval,
        event_time_ms=candle.close_time_ms,
        both_prior_band_touches=both_touched,
        watch_observed_ms=watch_observed_ms,
        gainer_rank=None if watch_context is None else watch_context.rank,
        retained_gainer=watch_context is not None and watch_context.rank is None,
        population_inclusion=(
            None
            if watch_context is None
            else "top5"
            if watch_context.rank is not None
            else "retained"
        ),
        price_change_percent_24h=(
            None if watch_context is None else watch_context.price_change_percent_24h
        ),
        quote_volume_24h_usdt=(
            None if watch_context is None else watch_context.quote_volume_24h_usdt
        ),
        watch_expires_at_ms=None if watch_context is None else watch_context.expires_at_ms,
        peak_quote_volume_usdt=(
            None if watch_context is None else watch_context.peak_quote_volume_usdt
        ),
        input_quality=input_quality,
        candidates=candidates,
    )


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _serialize_candles(candles: Sequence[Candle], event_time_ms: int) -> list[dict[str, object]]:
    return [
        cast(dict[str, object], candle.model_dump(mode="json"))
        for candle in candles
        if candle.is_closed and candle.close_time_ms <= event_time_ms
    ]


def _deserialize_candles(value: object) -> list[Candle]:
    if not isinstance(value, list):
        raise ValueError("frozen candle evidence must be a list")
    return [Candle.model_validate(item) for item in value]


def _minute_dependency_open_bounds(
    event_time_ms: int,
    minute_candles: Sequence[Candle] = (),
) -> tuple[int, int]:
    """Return the frozen 1m span that can affect C1, including Wilder ATR seed history.

    C1 needs at least 17 trailing one-minute candles, but when a longer event-time
    history is already frozen the Wilder ATR value depends on that earlier seed
    history too.  Preserve the calculation semantics by treating every grid slot
    from the earliest frozen causal 1m row (or the minimum 17-bar start) through
    the decision close as a dependency.  Rows older than that frozen start are
    deliberately irrelevant to this opportunity.
    """

    latest_open_ms = event_time_ms + 1 - 60_000
    minimum_open_ms = event_time_ms + 1 - (17 * 60_000)
    causal_opens = [
        candle.open_time_ms
        for candle in minute_candles
        if candle.interval == "1m"
        and candle.is_closed
        and candle.close_time_ms <= event_time_ms
        and candle.open_time_ms <= latest_open_ms
    ]
    earliest_open_ms = min([minimum_open_ms, *causal_opens])
    return earliest_open_ms, latest_open_ms


class PublicStructureFlowShadowObserver:
    """Durable research-only v2 observer with a bounded pending denominator."""

    def __init__(
        self,
        settings: Settings,
        repository: SqlRepository,
        *,
        registered_at_ms: int,
    ) -> None:
        self.settings = settings
        self.repository = repository
        watch = settings.alerts.market_watch
        if not watch.public_structure_flow_shadow_enabled:
            raise ValueError("public structure-flow observer requires shadow_enabled")
        if watch.public_structure_flow_shadow_version != PUBLIC_STRUCTURE_FLOW_VERSION:
            raise ValueError("public structure-flow observer requires the v2 version")
        if watch.public_structure_flow_campaign_id is None:
            raise ValueError("public structure-flow campaign id is missing")
        if watch.public_structure_flow_activation_ms is None:
            raise ValueError("public structure-flow activation time is missing")
        if watch.public_structure_flow_contract_sha256 != PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256:
            raise ValueError("public structure-flow contract sha256 does not match frozen contract")
        self.campaign_id = watch.public_structure_flow_campaign_id
        self.activation_ms = watch.public_structure_flow_activation_ms
        package_root = Path(__file__).resolve().parents[1]
        source_paths = {
            "prospective/public_structure_flow_v2.py": Path(__file__).resolve(),
            "prospective/pilot_guard.py": package_root / "prospective" / "pilot_guard.py",
            "prospective/public_structure_flow_contract_v2.py": (
                Path(__file__).resolve().with_name("public_structure_flow_contract_v2.py")
            ),
            "config.py": package_root / "config.py",
            "clock.py": package_root / "clock.py",
            "runtime.py": package_root / "runtime.py",
            "scanner.py": package_root / "scanner.py",
            "data/candles.py": package_root / "data" / "candles.py",
            "data/microstructure.py": package_root / "data" / "microstructure.py",
            "domain/enums.py": package_root / "domain" / "enums.py",
            "domain/models.py": package_root / "domain" / "models.py",
            "exchange/binance/schemas.py": package_root / "exchange" / "binance" / "schemas.py",
            "indicators/core.py": package_root / "indicators" / "core.py",
            "signals/gates.py": package_root / "signals" / "gates.py",
            "signals/market_watch.py": package_root / "signals" / "market_watch.py",
            "persistence/models.py": package_root / "persistence" / "models.py",
            "persistence/repository.py": package_root / "persistence" / "repository.py",
        }
        source_sha256 = {name: _sha256_file(path) for name, path in source_paths.items()}
        config_authority = {
            "binance": settings.binance.model_dump(mode="json"),
            "signals": settings.signals.model_dump(mode="json"),
            "runtime": settings.runtime.model_dump(mode="json"),
            "market_watch": watch.model_dump(mode="json"),
        }
        self.config_sha256 = _canonical_sha256(config_authority)
        self.manifest: dict[str, object] = {
            "schema_version": PUBLIC_STRUCTURE_FLOW_SCHEMA_VERSION,
            "candidate_version": PUBLIC_STRUCTURE_FLOW_VERSION,
            "campaign_id": self.campaign_id,
            "activation_ms": self.activation_ms,
            "contract": public_structure_flow_contract(),
            "contract_sha256": PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256,
            "source_identity": PUBLIC_STRUCTURE_FLOW_SOURCE_IDENTITY,
            "source_sha256": source_sha256,
            "source_bundle_sha256": _canonical_sha256(source_sha256),
            "config": config_authority,
            "config_sha256": self.config_sha256,
            "rule_version": settings.rule_version,
        }
        self.manifest_sha256 = _canonical_sha256(self.manifest)
        self.repository.register_public_structure_flow_campaign(
            campaign_id=self.campaign_id,
            candidate_version=PUBLIC_STRUCTURE_FLOW_VERSION,
            activation_ms=self.activation_ms,
            contract_sha256=PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256,
            manifest=self.manifest,
            registered_at_ms=registered_at_ms,
        )
        pending_rows = self.repository.load_public_structure_flow_pending(
            self.campaign_id, limit=MAX_PENDING + 1
        )
        if len(pending_rows) > MAX_PENDING:
            raise ValueError("durable public structure-flow pending set exceeds frozen bound")
        self._pending: dict[str, dict[str, object]] = {
            cast(str, row["opportunity_id"]): row for row in pending_rows
        }
        self._uncertain_pending: set[str] = set()
        self._recover_pending_on_startup(registered_at_ms)

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @staticmethod
    def _retry_storage(operation: Callable[[], bool]) -> bool:
        last_error: Exception | None = None
        for _attempt in range(MAX_FINALIZE_RETRIES):
            try:
                return operation()
            except PublicStructureFlowConflictError:
                raise
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise RuntimeError("public structure-flow storage retry loop produced no result")

    def _reload_opportunity(self, oid: str) -> dict[str, object] | None:
        """Replace speculative memory with the repository's durable opportunity state."""

        durable = self.repository.load_public_structure_flow_opportunity(oid)
        if durable is None:
            raise PublicStructureFlowConflictError(
                f"public structure-flow opportunity disappeared during reconciliation: {oid}"
            )
        if durable.get("campaign_id") != self.campaign_id:
            raise PublicStructureFlowConflictError(
                "reconciled opportunity belongs to a different campaign"
            )
        self._uncertain_pending.discard(oid)
        if durable.get("status") != "PENDING":
            self._pending.pop(oid, None)
            return None
        self._pending[oid] = durable
        return durable

    def _persist_pending_evidence(
        self,
        *,
        oid: str,
        evidence: dict[str, object],
        updated_at_ms: int,
        late_event: bool = False,
        evidence_ready_at_ms: int | None = None,
    ) -> bool:
        """Persist copy-on-write evidence and reconcile any exhausted ambiguous acknowledgement."""

        try:
            updated = self._retry_storage(
                partial(
                    self.repository.update_public_structure_flow_pending_evidence,
                    opportunity_id=oid,
                    evidence=evidence,
                    updated_at_ms=updated_at_ms,
                    late_event=late_event,
                    evidence_ready_at_ms=evidence_ready_at_ms,
                )
            )
        except PublicStructureFlowConflictError:
            raise
        except Exception as exc:
            self._uncertain_pending.add(oid)
            try:
                self._reload_opportunity(oid)
            except Exception as reconcile_error:
                self._uncertain_pending.add(oid)
                raise exc from reconcile_error
            # Preserve the storage anomaly for the caller even when readback established which
            # state committed. Later callbacks now see durable memory instead of speculative input.
            raise exc
        if not updated:
            self._reload_opportunity(oid)
            return False
        pending = self._pending.get(oid)
        if pending is None:
            return False
        pending["evidence"] = evidence
        pending["updated_at_ms"] = max(
            int(cast(int | str, pending.get("updated_at_ms", updated_at_ms))), updated_at_ms
        )
        if evidence_ready_at_ms is not None:
            existing_ready = pending.get("ready_at_ms")
            pending["ready_at_ms"] = (
                evidence_ready_at_ms
                if existing_ready is None
                else min(int(cast(int | str, existing_ready)), evidence_ready_at_ms)
            )
        self._uncertain_pending.discard(oid)
        return True

    def _pending_resolution(self, oid: str) -> PendingResolution | None:
        """Classify one pending row using durable precedence, never callback-local policy."""

        if oid in self._uncertain_pending:
            if self._reload_opportunity(oid) is None:
                return None
        pending = self._pending.get(oid)
        if pending is None:
            return None
        evidence = pending.get("evidence")
        if not isinstance(evidence, dict):
            return PendingResolution("incomplete", None, None)
        forced_reason = evidence.get("c1_forced_unavailable_reason")
        if isinstance(forced_reason, str):
            forced_at = evidence.get("c1_forced_unavailable_at_ms")
            if not isinstance(forced_at, int):
                forced_at = int(cast(int | str, pending["updated_at_ms"]))
            return PendingResolution("disqualified", forced_at, forced_reason)
        try:
            candle, _feature, _five, minutes, _contexts, _watch, _observed = self._decode(pending)
        except (KeyError, TypeError, ValueError):
            return PendingResolution("incomplete", None, None)
        if not _minute_tail_ready(
            minutes,
            market=candle.market,
            symbol=candle.symbol,
            event_time_ms=candle.close_time_ms,
        ):
            return PendingResolution("incomplete", None, None)
        ready_value = pending.get("ready_at_ms")
        if ready_value is None:
            # Local legacy rows can predate the explicit ready timestamp. Their durable evidence
            # update/registration time is the conservative completion authority after tail checks.
            ready_value = pending.get("updated_at_ms")
        ready_at_ms = int(cast(int | str, ready_value))
        if ready_at_ms <= int(cast(int | str, pending["deadline_ms"])):
            return PendingResolution("ready", ready_at_ms, None)
        return PendingResolution("incomplete", None, None)

    def _terminal_choice(
        self,
        oid: str,
        *,
        fallback_reason: str,
        fallback_ready_at_ms: int,
    ) -> tuple[int, str | None] | None:
        resolution = self._pending_resolution(oid)
        if resolution is None:
            return None
        if resolution.kind == "disqualified":
            assert resolution.ready_at_ms is not None and resolution.forced_reason is not None
            return resolution.ready_at_ms, resolution.forced_reason
        if resolution.kind == "ready":
            assert resolution.ready_at_ms is not None
            return resolution.ready_at_ms, None
        return fallback_ready_at_ms, fallback_reason

    def _prepare_terminal_write_for_state(
        self,
        oid: str,
        *,
        fallback_reason: str,
        fallback_ready_at_ms: int,
        recorded_at_ms: int,
    ) -> PublicStructureFlowTerminalWrite | None:
        choice = self._terminal_choice(
            oid,
            fallback_reason=fallback_reason,
            fallback_ready_at_ms=fallback_ready_at_ms,
        )
        if choice is None:
            return None
        ready_at_ms, forced_reason = choice
        return self._prepare_terminal_write(
            oid,
            ready_at_ms=ready_at_ms,
            recorded_at_ms=recorded_at_ms,
            forced_reason=forced_reason,
        )

    def _finalize_for_state(
        self,
        oid: str,
        *,
        fallback_reason: str,
        fallback_ready_at_ms: int,
        recorded_at_ms: int,
    ) -> bool:
        choice = self._terminal_choice(
            oid,
            fallback_reason=fallback_reason,
            fallback_ready_at_ms=fallback_ready_at_ms,
        )
        if choice is None:
            return False
        ready_at_ms, forced_reason = choice
        return self._finalize(
            oid,
            ready_at_ms=ready_at_ms,
            recorded_at_ms=recorded_at_ms,
            forced_reason=forced_reason,
        )

    def _recover_pending_on_startup(self, recorded_at_ms: int) -> None:
        """Converge durable pending rows before applying deadline degradation."""

        for oid in tuple(self._pending):
            pending = self._pending.get(oid)
            if pending is None:
                continue
            evidence = pending.get("evidence")
            if not isinstance(evidence, dict):
                continue
            missing_reason = evidence.get("missing_reason")
            if isinstance(missing_reason, str) and "feature" not in evidence:
                event_time_ms = int(cast(int | str, pending["event_time_ms"]))
                ready_at_ms = int(cast(int | str, pending.get("updated_at_ms", event_time_ms)))
                self._retry_storage(
                    partial(
                        self.repository.register_public_structure_flow_missing_opportunity,
                        opportunity_id=oid,
                        campaign_id=self.campaign_id,
                        market=Market(str(pending["market"])),
                        symbol=str(pending["symbol"]),
                        event_time_ms=event_time_ms,
                        deadline_ms=int(cast(int | str, pending["deadline_ms"])),
                        evidence=evidence,
                        ready_at_ms=ready_at_ms,
                        recorded_at_ms=max(recorded_at_ms, ready_at_ms),
                        reason=missing_reason,
                    )
                )
                self._pending.pop(oid, None)
                continue
            resolution = self._pending_resolution(oid)
            if resolution is not None and resolution.kind != "incomplete":
                assert resolution.ready_at_ms is not None
                self._finalize(
                    oid,
                    ready_at_ms=resolution.ready_at_ms,
                    recorded_at_ms=max(recorded_at_ms, resolution.ready_at_ms),
                    forced_reason=resolution.forced_reason,
                )
        self.expire_due(recorded_at_ms)

    def _evidence(
        self,
        *,
        candle: Candle,
        feature: FeatureSnapshot,
        five_minute_candles: Sequence[Candle],
        minute_candles: Sequence[Candle],
        contexts: Mapping[str, FeatureSnapshot],
        watch_context: PriorityGainer,
        watch_observed_ms: int,
        deadline_ms: int,
    ) -> dict[str, object]:
        minute_dependency_start_open_ms, minute_dependency_end_open_ms = (
            _minute_dependency_open_bounds(candle.close_time_ms, minute_candles)
        )
        return {
            "campaign_id": self.campaign_id,
            "candidate_version": PUBLIC_STRUCTURE_FLOW_VERSION,
            "contract_sha256": PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256,
            "manifest_sha256": self.manifest_sha256,
            "market": candle.market.value,
            "symbol": candle.symbol,
            "event_time_ms": candle.close_time_ms,
            "deadline_ms": deadline_ms,
            "candle": candle.model_dump(mode="json"),
            "feature": feature.model_dump(mode="json"),
            "five_minute_candles": _serialize_candles(
                five_minute_candles, candle.close_time_ms
            ),
            "minute_candles": _serialize_candles(minute_candles, candle.close_time_ms),
            "minute_dependency_start_open_ms": minute_dependency_start_open_ms,
            "minute_dependency_end_open_ms": minute_dependency_end_open_ms,
            "contexts": {
                interval: context.model_dump(mode="json")
                for interval, context in contexts.items()
                if interval in {"15m", "1h"} and context.event_time_ms < candle.close_time_ms
            },
            "watch_context": asdict(watch_context),
            "watch_observed_ms": watch_observed_ms,
        }

    def register_opportunity(
        self,
        *,
        candle: Candle,
        feature: FeatureSnapshot,
        five_minute_candles: Sequence[Candle],
        minute_candles: Sequence[Candle],
        contexts: Mapping[str, FeatureSnapshot],
        watch_context: PriorityGainer | None,
        watch_observed_ms: int | None,
        received_at_ms: int,
    ) -> bool:
        if candle.close_time_ms < self.activation_ms:
            return False
        if received_at_ms < candle.close_time_ms:
            raise ValueError("opportunity receipt time cannot precede event close")
        if watch_context is None or watch_observed_ms is None:
            return False
        if watch_observed_ms > candle.close_time_ms:
            raise ValueError("watch snapshot must not be newer than the decision close")
        if watch_context.symbol.upper() != candle.symbol:
            raise ValueError("watch membership symbol does not match opportunity")
        if watch_context.rank is None and (
            watch_context.expires_at_ms is None
            or candle.close_time_ms >= watch_context.expires_at_ms
        ):
            return False
        self.expire_due(received_at_ms)
        oid = opportunity_id(
            campaign_id=self.campaign_id,
            market=candle.market,
            symbol=candle.symbol,
            event_time_ms=candle.close_time_ms,
        )
        deadline_ms = candle.close_time_ms + PENDING_WAIT_MS
        evidence = self._evidence(
            candle=candle,
            feature=feature,
            five_minute_candles=five_minute_candles,
            minute_candles=minute_candles,
            contexts=contexts,
            watch_context=watch_context,
            watch_observed_ms=watch_observed_ms,
            deadline_ms=deadline_ms,
        )
        eviction_oid: str | None = None
        eviction_write: PublicStructureFlowTerminalWrite | None = None
        incoming_exists = oid in self._pending
        if len(self._pending) >= MAX_PENDING and not incoming_exists:
            incoming_exists = (
                self.repository.load_public_structure_flow_opportunity(oid) is not None
            )
        if len(self._pending) >= MAX_PENDING and not incoming_exists:
            candidate_oid = min(
                self._pending,
                key=lambda item: int(cast(int | str, self._pending[item]["event_time_ms"])),
            )
            eviction_write = self._prepare_terminal_write_for_state(
                candidate_oid,
                fallback_reason="pending_capacity_limit",
                fallback_ready_at_ms=received_at_ms,
                recorded_at_ms=received_at_ms,
            )
            if eviction_write is not None:
                eviction_oid = candidate_oid
        created = self._retry_storage(
            lambda: self.repository.register_public_structure_flow_opportunity(
                opportunity_id=oid,
                campaign_id=self.campaign_id,
                market=candle.market,
                symbol=candle.symbol,
                event_time_ms=candle.close_time_ms,
                deadline_ms=deadline_ms,
                evidence=evidence,
                created_at_ms=received_at_ms,
                eviction_write=eviction_write,
            )
        )
        if created:
            if eviction_oid is not None:
                self._pending.pop(eviction_oid, None)
            self._pending[oid] = {
                "opportunity_id": oid,
                "campaign_id": self.campaign_id,
                "market": candle.market.value,
                "symbol": candle.symbol,
                "event_time_ms": candle.close_time_ms,
                "deadline_ms": deadline_ms,
                "status": "PENDING",
                "updated_at_ms": received_at_ms,
                "ready_at_ms": None,
                "evidence": evidence,
            }
        elif oid not in self._pending:
            # A retry after an uncertain post-commit failure may discover that the first
            # transaction actually admitted the row (and perhaps evicted another row). Reconcile
            # memory from durable state; a true terminal replay simply remains absent.
            durable_pending = self.repository.load_public_structure_flow_pending(
                self.campaign_id, limit=MAX_PENDING + 1
            )
            if len(durable_pending) > MAX_PENDING:
                raise PublicStructureFlowConflictError(
                    "durable public structure-flow pending set exceeds frozen bound"
                )
            self._pending = {
                cast(str, row["opportunity_id"]): row for row in durable_pending
            }
            if oid not in self._pending:
                return False
        self._try_finalize(oid, ready_at_ms=received_at_ms)
        return created

    def register_missing_opportunity(
        self,
        *,
        candle: Candle,
        watch_context: PriorityGainer | None,
        watch_observed_ms: int | None,
        received_at_ms: int,
        reason: str,
    ) -> bool:
        """Record a population denominator when frozen decision evidence is unrecoverable."""

        if candle.close_time_ms < self.activation_ms:
            return False
        if watch_context is None or watch_observed_ms is None:
            return False
        if watch_observed_ms > candle.close_time_ms:
            raise ValueError("watch snapshot must not be newer than the decision close")
        if watch_context.symbol.upper() != candle.symbol:
            raise ValueError("watch membership symbol does not match missing opportunity")
        if watch_context.rank is None and (
            watch_context.expires_at_ms is None
            or candle.close_time_ms >= watch_context.expires_at_ms
        ):
            return False
        oid = opportunity_id(
            campaign_id=self.campaign_id,
            market=candle.market,
            symbol=candle.symbol,
            event_time_ms=candle.close_time_ms,
        )
        deadline_ms = candle.close_time_ms + PENDING_WAIT_MS
        evidence: dict[str, object] = {
            "campaign_id": self.campaign_id,
            "candidate_version": PUBLIC_STRUCTURE_FLOW_VERSION,
            "contract_sha256": PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256,
            "manifest_sha256": self.manifest_sha256,
            "market": candle.market.value,
            "symbol": candle.symbol,
            "event_time_ms": candle.close_time_ms,
            "deadline_ms": deadline_ms,
            "candle": candle.model_dump(mode="json"),
            "watch_context": asdict(watch_context),
            "watch_observed_ms": watch_observed_ms,
            "missing_reason": reason,
        }
        return self._retry_storage(
            lambda: self.repository.register_public_structure_flow_missing_opportunity(
                opportunity_id=oid,
                campaign_id=self.campaign_id,
                market=candle.market,
                symbol=candle.symbol,
                event_time_ms=candle.close_time_ms,
                deadline_ms=deadline_ms,
                evidence=evidence,
                ready_at_ms=received_at_ms,
                recorded_at_ms=received_at_ms,
                reason=reason,
            )
        )

    def _decode(self, pending: Mapping[str, object]) -> tuple[
        Candle,
        FeatureSnapshot,
        list[Candle],
        list[Candle],
        dict[str, FeatureSnapshot],
        PriorityGainer,
        int,
    ]:
        evidence = pending.get("evidence")
        if not isinstance(evidence, dict):
            raise ValueError("pending evidence is not an object")
        candle = Candle.model_validate(evidence["candle"])
        feature = FeatureSnapshot.model_validate(evidence["feature"])
        five = _deserialize_candles(evidence["five_minute_candles"])
        minutes = _deserialize_candles(evidence["minute_candles"])
        raw_contexts = evidence.get("contexts")
        if not isinstance(raw_contexts, dict):
            raise ValueError("pending HTF contexts are not an object")
        contexts = {
            str(interval): FeatureSnapshot.model_validate(context)
            for interval, context in raw_contexts.items()
        }
        raw_watch = evidence.get("watch_context")
        if not isinstance(raw_watch, dict):
            raise ValueError("pending watch context is not an object")
        watch_context = PriorityGainer(**raw_watch)
        watch_observed_ms = int(evidence["watch_observed_ms"])
        return candle, feature, five, minutes, contexts, watch_context, watch_observed_ms

    def _try_finalize(self, oid: str, *, ready_at_ms: int) -> bool:
        pending = self._pending.get(oid)
        if pending is None:
            return False
        resolution = self._pending_resolution(oid)
        if resolution is None:
            return False
        if resolution.kind == "disqualified":
            assert resolution.ready_at_ms is not None
            return self._finalize(
                oid,
                ready_at_ms=resolution.ready_at_ms,
                recorded_at_ms=ready_at_ms,
                forced_reason=resolution.forced_reason,
            )
        pending = self._pending.get(oid)
        if pending is None:
            return False
        candle, _feature, _five, minutes, _contexts, _watch, _observed = self._decode(pending)
        if not _minute_tail_ready(
            minutes,
            market=candle.market,
            symbol=candle.symbol,
            event_time_ms=candle.close_time_ms,
        ):
            return False
        persisted_ready = pending.get("ready_at_ms")
        if persisted_ready is None:
            evidence = pending.get("evidence")
            if not isinstance(evidence, dict):
                raise ValueError("pending evidence is not an object")
            self._persist_pending_evidence(
                oid=oid,
                evidence=dict(evidence),
                updated_at_ms=ready_at_ms,
                evidence_ready_at_ms=ready_at_ms,
            )
        resolution = self._pending_resolution(oid)
        if resolution is None or resolution.kind == "incomplete":
            return False
        assert resolution.ready_at_ms is not None
        return self._finalize(
            oid,
            ready_at_ms=resolution.ready_at_ms,
            recorded_at_ms=ready_at_ms,
            forced_reason=resolution.forced_reason,
        )

    def _prepare_terminal_write(
        self,
        oid: str,
        *,
        ready_at_ms: int,
        recorded_at_ms: int,
        forced_reason: str | None,
    ) -> PublicStructureFlowTerminalWrite:
        pending = self._pending.get(oid)
        if pending is None:
            raise PublicStructureFlowConflictError("pending opportunity is not loaded")
        (
            candle,
            feature,
            five,
            minutes,
            contexts,
            watch_context,
            watch_observed_ms,
        ) = self._decode(pending)
        observation = evaluate_public_structure_flow(
            candle=candle,
            feature=feature,
            five_minute_candles=five,
            minute_candles=minutes,
            contexts=contexts,
            watch_context=watch_context,
            watch_observed_ms=watch_observed_ms,
            c1_forced_unavailable_reason=forced_reason,
        )
        payload = observation.payload()
        payload.update(
            {
                "campaign_id": self.campaign_id,
                "activation_ms": self.activation_ms,
                "contract_sha256": PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256,
                "manifest_sha256": self.manifest_sha256,
                "config_sha256": self.config_sha256,
                "rule_version": self.settings.rule_version,
                "source_identity": PUBLIC_STRUCTURE_FLOW_SOURCE_IDENTITY,
            }
        )
        status = "RECORDED_DEGRADED" if forced_reason is not None else "RECORDED"
        return PublicStructureFlowTerminalWrite(
            opportunity_id=oid,
            observation_id=observation_id(observation, campaign_id=self.campaign_id),
            campaign_id=self.campaign_id,
            market=candle.market,
            symbol=candle.symbol,
            timeframe=candle.interval,
            event_time_ms=candle.close_time_ms,
            candidate_version=observation.candidate_version,
            payload=payload,
            status=status,
            reason=forced_reason,
            ready_at_ms=ready_at_ms,
            recorded_at_ms=max(recorded_at_ms, ready_at_ms),
        )

    def _finalize(
        self,
        oid: str,
        *,
        ready_at_ms: int,
        recorded_at_ms: int | None = None,
        forced_reason: str | None,
    ) -> bool:
        pending = self._pending.get(oid)
        if pending is None:
            return False
        write = self._prepare_terminal_write(
            oid,
            ready_at_ms=ready_at_ms,
            recorded_at_ms=ready_at_ms if recorded_at_ms is None else recorded_at_ms,
            forced_reason=forced_reason,
        )
        last_error: Exception | None = None
        for _attempt in range(MAX_FINALIZE_RETRIES):
            try:
                self.repository.commit_public_structure_flow_result(write)
                self._pending.pop(oid, None)
                return True
            except PublicStructureFlowConflictError:
                raise
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            self._uncertain_pending.add(oid)
            try:
                durable = self.repository.load_public_structure_flow_opportunity(oid)
            except Exception as reconcile_error:
                raise last_error from reconcile_error
            if durable is None:
                raise PublicStructureFlowConflictError(
                    f"terminal opportunity disappeared during reconciliation: {oid}"
                ) from last_error
            self._uncertain_pending.discard(oid)
            if durable.get("status") != "PENDING":
                if (
                    durable.get("status") == write.status
                    and durable.get("reason") == write.reason
                    and durable.get("observation_id") == write.observation_id
                ):
                    self._pending.pop(oid, None)
                    return True
                raise PublicStructureFlowConflictError(
                    f"opportunity {oid} became terminal with different content"
                ) from last_error
            self._pending[oid] = durable
            raise last_error
        return False

    def accept_minute(self, candle: Candle, *, received_at_ms: int) -> bool:
        if candle.interval != "1m" or not candle.is_closed or candle.open_time_ms % 60_000 != 0:
            return False
        affected = False
        for oid, pending in tuple(self._pending.items()):
            if oid in self._uncertain_pending:
                reconciled = self._reload_opportunity(oid)
                if reconciled is None:
                    continue
                pending = reconciled
            if str(pending.get("market")) != candle.market.value:
                continue
            if str(pending.get("symbol", "")).upper() != candle.symbol:
                continue
            event_time_ms = int(cast(int | str, pending["event_time_ms"]))
            evidence = pending.get("evidence")
            if not isinstance(evidence, dict):
                raise ValueError("pending evidence is not an object")
            stored_start = evidence.get("minute_dependency_start_open_ms")
            stored_end = evidence.get("minute_dependency_end_open_ms")
            if stored_start is None or stored_end is None:
                # Compatibility for local pre-fix pending rows: derive the span from
                # their already-frozen minute evidence without rewriting frozen data.
                frozen_minutes = _deserialize_candles(evidence.get("minute_candles"))
                earliest_open_ms, latest_open_ms = _minute_dependency_open_bounds(
                    event_time_ms, frozen_minutes
                )
            else:
                earliest_open_ms = int(cast(int | str, stored_start))
                latest_open_ms = int(cast(int | str, stored_end))
            if not earliest_open_ms <= candle.open_time_ms <= latest_open_ms:
                continue
            if candle.close_time_ms > event_time_ms:
                continue
            affected = True
            deadline_ms = int(cast(int | str, pending["deadline_ms"]))
            if isinstance(evidence.get("c1_forced_unavailable_reason"), str):
                self._finalize_for_state(
                    oid,
                    fallback_reason="minute_deadline_expired",
                    fallback_ready_at_ms=deadline_ms + 1,
                    recorded_at_ms=received_at_ms,
                )
                continue
            if received_at_ms > deadline_ms:
                self._persist_pending_evidence(
                    oid=oid,
                    evidence=dict(evidence),
                    updated_at_ms=received_at_ms,
                    late_event=True,
                )
                self._finalize_for_state(
                    oid,
                    fallback_reason="minute_deadline_expired",
                    fallback_ready_at_ms=deadline_ms + 1,
                    recorded_at_ms=received_at_ms,
                )
                continue
            minutes = _deserialize_candles(evidence.get("minute_candles"))
            by_open = {row.open_time_ms: row for row in minutes}
            existing = by_open.get(candle.open_time_ms)
            if existing is not None and existing != candle:
                next_evidence = dict(evidence)
                next_evidence["c1_forced_unavailable_reason"] = "conflicting_1m_evidence"
                next_evidence.setdefault("c1_forced_unavailable_at_ms", received_at_ms)
                self._persist_pending_evidence(
                    oid=oid,
                    evidence=next_evidence,
                    updated_at_ms=received_at_ms,
                    late_event=received_at_ms > event_time_ms,
                )
                self._finalize_for_state(
                    oid,
                    fallback_reason="conflicting_1m_evidence",
                    fallback_ready_at_ms=received_at_ms,
                    recorded_at_ms=received_at_ms,
                )
                continue
            by_open[candle.open_time_ms] = candle
            updated_minutes = [
                row
                for row in sorted(by_open.values(), key=lambda item: item.open_time_ms)
                if row.close_time_ms <= event_time_ms
            ]
            next_evidence = dict(evidence)
            next_evidence["minute_candles"] = [
                row.model_dump(mode="json") for row in updated_minutes
            ]
            complete = _minute_tail_ready(
                updated_minutes,
                market=candle.market,
                symbol=candle.symbol,
                event_time_ms=event_time_ms,
            )
            self._persist_pending_evidence(
                oid=oid,
                evidence=next_evidence,
                updated_at_ms=received_at_ms,
                late_event=received_at_ms > event_time_ms,
                evidence_ready_at_ms=received_at_ms if complete else None,
            )
            if complete:
                self._try_finalize(oid, ready_at_ms=received_at_ms)
        return affected

    def expire_due(self, now_ms: int) -> int:
        expired = [
            oid
            for oid, pending in self._pending.items()
            if int(cast(int | str, pending["deadline_ms"])) < now_ms
        ]
        for oid in expired:
            pending = self._pending.get(oid)
            if pending is None:
                continue
            deadline_ms = int(cast(int | str, pending["deadline_ms"]))
            self._finalize_for_state(
                oid,
                fallback_reason="minute_deadline_expired",
                fallback_ready_at_ms=deadline_ms + 1,
                recorded_at_ms=now_ms,
            )
        return len(expired)

    def finalize_removed_symbols(self, symbols: frozenset[str], *, now_ms: int) -> int:
        normalized = frozenset(symbol.upper() for symbol in symbols)
        removed = [
            oid
            for oid, pending in self._pending.items()
            if str(pending["symbol"]).upper() in normalized
        ]
        for oid in removed:
            self._finalize_for_state(
                oid,
                fallback_reason="symbol_left_data_universe",
                fallback_ready_at_ms=now_ms,
                recorded_at_ms=now_ms,
            )
        return len(removed)

    def summary(self) -> dict[str, object]:
        return self.repository.summarize_public_structure_flow_campaign(self.campaign_id)
