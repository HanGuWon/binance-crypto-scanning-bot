from __future__ import annotations

import hashlib
import json
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from itertools import pairwise
from pathlib import Path
from typing import Literal

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
from signalbot.persistence.repository import SqlRepository
from signalbot.prospective.public_structure_flow_contract import (
    PUBLIC_STRUCTURE_FLOW_CONTRACT,
    PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256,
    PUBLIC_STRUCTURE_FLOW_SCHEMA_VERSION,
    PUBLIC_STRUCTURE_FLOW_SOURCE_IDENTITY,
    PUBLIC_STRUCTURE_FLOW_VERSION,
)
from signalbot.signals.gates import evaluate_strict_prior_htf
from signalbot.signals.market_watch import PriorityGainer

CandidateStatus = Literal["UNAVAILABLE", "NO_SIGNAL", "OBSERVE", "DIRECTIONAL"]
CandidateDirection = Literal["long", "short"]


@dataclass(frozen=True, slots=True)
class CandidateEvaluation:
    candidate_id: str
    status: CandidateStatus
    direction: CandidateDirection | None
    reason: str
    metrics: dict[str, float | bool | str | None]


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


def _unavailable(candidate_id: str, reason: str) -> CandidateEvaluation:
    return CandidateEvaluation(candidate_id, "UNAVAILABLE", None, reason, {})


def _validate_tail(
    candles: Sequence[Candle],
    *,
    market: Market,
    symbol: str,
    interval: str,
    as_of_ms: int,
    minimum_count: int,
) -> list[Candle] | None:
    rows = sorted(
        (
            candle
            for candle in candles
            if candle.market is market
            and candle.symbol == symbol
            and candle.interval == interval
            and candle.is_closed
            and candle.close_time_ms <= as_of_ms
        ),
        key=lambda candle: candle.open_time_ms,
    )
    if len(rows) < minimum_count or rows[-1].close_time_ms != as_of_ms:
        return None
    step_ms = interval_to_milliseconds(interval)
    if any(
        candle.close_time_ms != candle.open_time_ms + step_ms - 1 for candle in rows
    ):
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
    if middle == 0 or upper <= lower:
        return None
    return lower, middle, upper


def _signed_quote_flow(rows: Sequence[Candle]) -> float | None:
    total = sum(float(row.quote_volume) for row in rows)
    if total <= 0:
        return None
    try:
        flows = [closed_kline_flow(row) for row in rows]
    except ValueError:
        return None
    if not all(flow.available for flow in flows):
        return None
    buys = sum(float(row.taker_buy_quote_volume) for row in rows)
    return (2 * buys - total) / total


def _c1_flow_price_response(
    minute_rows: Sequence[Candle],
    current_5m: Candle,
    *,
    prior_upper: float,
    prior_lower: float,
    both_touched: bool,
) -> CandidateEvaluation:
    candidate_id = "PUB09_FLOW_PRICE_RESPONSE"
    if both_touched:
        return CandidateEvaluation(
            candidate_id,
            "NO_SIGNAL",
            None,
            "both_prior_bands_touched_direction_withheld",
            {"upper_touch": True, "lower_touch": True},
        )
    if len(minute_rows) < 17:
        return _unavailable(candidate_id, "insufficient_1m_history")
    recent = list(minute_rows[-3:])
    previous = list(minute_rows[-6:-3])
    if len(previous) != 3:
        return _unavailable(candidate_id, "insufficient_1m_comparison_window")
    fr = _signed_quote_flow(recent)
    fp = _signed_quote_flow(previous)
    if fr is None or fp is None:
        return _unavailable(candidate_id, "invalid_or_zero_1m_quote_flow")
    atr_values = atr_series(minute_rows)
    atr_before_recent = atr_values[len(minute_rows) - 4]
    if atr_before_recent is None or atr_before_recent <= 0:
        return _unavailable(candidate_id, "immature_or_zero_1m_atr14")
    r3 = (float(recent[-1].close) - float(recent[0].open)) / atr_before_recent
    upper_touch = float(current_5m.high) >= prior_upper
    lower_touch = float(current_5m.low) <= prior_lower
    upper_slowdown = fr >= 0.20 and r3 <= 0.25 and upper_touch
    lower_slowdown = fr <= -0.20 and r3 >= -0.25 and lower_touch
    upper_confirmed = upper_slowdown and recent[-1].close < recent[-2].low
    lower_confirmed = lower_slowdown and recent[-1].close > recent[-2].high
    metrics: dict[str, float | bool | str | None] = {
        "flow_recent": fr,
        "flow_previous": fp,
        "flow_delta": fr - fp,
        "r3_atr": r3,
        "atr1m14_prior": atr_before_recent,
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
    if "15m" not in contexts or "1h" not in contexts:
        return _unavailable(candidate_id, "missing_strict_prior_htf_context")
    if any(context.event_time_ms >= feature.event_time_ms for context in contexts.values()):
        return _unavailable(candidate_id, "non_strict_prior_context")
    atr_values = atr_series(five_minute_rows)
    prior_atr = atr_values[-2]
    if prior_atr is None or prior_atr <= 0:
        return _unavailable(candidate_id, "immature_or_zero_prior_atr14")
    closes = [float(row.close) for row in five_minute_rows]
    widths = bollinger_width_series(closes)
    current_width = widths[-1]
    prior_width = widths[-2]
    if current_width is None or prior_width is None or prior_width <= 0:
        return _unavailable(candidate_id, "immature_or_zero_bollinger_width")
    current_flow = _signed_quote_flow((five_minute_rows[-1],))
    if current_flow is None:
        return _unavailable(candidate_id, "invalid_or_zero_5m_quote_flow")
    impulse = (closes[-1] - closes[-2]) / prior_atr
    expansion = current_width / prior_width
    strict_long = evaluate_strict_prior_htf(feature, Direction.LONG, contexts)
    strict_short = evaluate_strict_prior_htf(feature, Direction.SHORT, contexts)
    long_signal = (
        impulse >= 1.5
        and expansion > 1
        and current_flow >= 0.20
        and closes[-1] > prior_upper
        and strict_long.accepted
    )
    short_signal = (
        impulse <= -1.5
        and expansion > 1
        and current_flow <= -0.20
        and closes[-1] < prior_lower
        and strict_short.accepted
    )
    metrics: dict[str, float | bool | str | None] = {
        "impulse_atr": impulse,
        "bandwidth_expansion": expansion,
        "flow_5m": current_flow,
        "prior_atr14": prior_atr,
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
    if prior_width <= 0 or current_width <= 0:
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
    long_signal = (
        previous_percent_b <= 0
        and 0 < current_percent_b < 1
        and macd_line < 0
        and histogram > 0
    )
    short_signal = (
        previous_percent_b >= 1
        and 0 < current_percent_b < 1
        and macd_line > 0
        and histogram < 0
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


def evaluate_public_structure_flow(
    *,
    candle: Candle,
    feature: FeatureSnapshot,
    five_minute_candles: Sequence[Candle],
    minute_candles: Sequence[Candle],
    contexts: Mapping[str, FeatureSnapshot],
    watch_context: PriorityGainer | None,
    watch_observed_ms: int | None = None,
) -> PublicStructureFlowObservation:
    """Evaluate frozen C1/C2/C3 seeds without changing production decisions.

    The evaluator is intentionally outcome-blind. It only reads closed rows at or
    before ``candle.close_time_ms`` and strictly-prior 15m/1h context supplied by
    the incumbent runtime.
    """

    if candle.interval != "5m" or not candle.is_closed:
        raise ValueError("public structure-flow shadow requires one closed 5m candle")
    if (
        feature.market is not candle.market
        or feature.symbol != candle.symbol
        or feature.interval != candle.interval
        or feature.event_time_ms != candle.close_time_ms
    ):
        raise ValueError("feature identity does not match public structure-flow candle")

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
    unavailable = (
        _unavailable("PUB09_FLOW_PRICE_RESPONSE", "noncontiguous_or_misaligned_1m_history"),
        _unavailable("PUB02_ATR_IMPULSE_CONTINUATION", "noncontiguous_or_misaligned_5m_history"),
        _unavailable("PUB03_BB_MACD_REACTION", "noncontiguous_or_misaligned_5m_history"),
    )
    if five_rows is None:
        candidates = unavailable
        both_touched = False
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
            both_touched = False
        else:
            prior_lower, _prior_middle, prior_upper = previous_bands
            both_touched = (
                float(candle.high) >= prior_upper and float(candle.low) <= prior_lower
            )
            c1 = (
                _unavailable(
                    "PUB09_FLOW_PRICE_RESPONSE", "noncontiguous_or_misaligned_1m_history"
                )
                if minute_rows is None
                else _c1_flow_price_response(
                    minute_rows,
                    candle,
                    prior_upper=prior_upper,
                    prior_lower=prior_lower,
                    both_touched=both_touched,
                )
            )
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
        watch_expires_at_ms=(None if watch_context is None else watch_context.expires_at_ms),
        peak_quote_volume_usdt=(
            None if watch_context is None else watch_context.peak_quote_volume_usdt
        ),
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


class PublicStructureFlowShadowObserver:
    """Persist research-only C1/C2/C3 observations without an alert/outbox path."""

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
            "public_structure_flow.py": Path(__file__).resolve(),
            "indicators/core.py": package_root / "indicators" / "core.py",
            "data/microstructure.py": package_root / "data" / "microstructure.py",
            "signals/gates.py": package_root / "signals" / "gates.py",
            "signals/market_watch.py": package_root / "signals" / "market_watch.py",
        }
        source_sha256 = {name: _sha256_file(path) for name, path in source_paths.items()}
        config_authority = {
            "binance": settings.binance.model_dump(mode="json"),
            "signals": settings.signals.model_dump(mode="json"),
            "market_watch": watch.model_dump(mode="json"),
        }
        self.config_sha256 = _canonical_sha256(config_authority)
        self.manifest: dict[str, object] = {
            "schema_version": PUBLIC_STRUCTURE_FLOW_SCHEMA_VERSION,
            "candidate_version": PUBLIC_STRUCTURE_FLOW_VERSION,
            "campaign_id": self.campaign_id,
            "activation_ms": self.activation_ms,
            "contract": PUBLIC_STRUCTURE_FLOW_CONTRACT,
            "contract_sha256": PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256,
            "source_identity": PUBLIC_STRUCTURE_FLOW_SOURCE_IDENTITY,
            "source_sha256": source_sha256,
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

    def observe(
        self,
        *,
        candle: Candle,
        feature: FeatureSnapshot,
        five_minute_candles: Sequence[Candle],
        minute_candles: Sequence[Candle],
        contexts: Mapping[str, FeatureSnapshot],
        watch_context: PriorityGainer | None,
        watch_observed_ms: int | None,
        recorded_at_ms: int,
    ) -> bool:
        if candle.close_time_ms < self.activation_ms:
            return False
        if recorded_at_ms < candle.close_time_ms:
            raise ValueError("shadow observation cannot be recorded before its decision close")
        if watch_context is None or watch_observed_ms is None:
            raise ValueError("shadow observation requires event-time watch membership evidence")
        if watch_observed_ms > candle.close_time_ms:
            raise ValueError("watch snapshot must not be newer than the decision close")
        if watch_context.rank is None and (
            watch_context.expires_at_ms is None
            or candle.close_time_ms >= watch_context.expires_at_ms
        ):
            return False
        observation = evaluate_public_structure_flow(
            candle=candle,
            feature=feature,
            five_minute_candles=five_minute_candles,
            minute_candles=minute_candles,
            contexts=contexts,
            watch_context=watch_context,
            watch_observed_ms=watch_observed_ms,
        )
        payload = observation.payload()
        payload["campaign_id"] = self.campaign_id
        payload["activation_ms"] = self.activation_ms
        payload["contract_sha256"] = PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256
        payload["manifest_sha256"] = self.manifest_sha256
        payload["config_sha256"] = self.config_sha256
        payload["rule_version"] = self.settings.rule_version
        payload["source_identity"] = PUBLIC_STRUCTURE_FLOW_SOURCE_IDENTITY
        return self.repository.save_public_structure_flow_shadow(
            observation_id=observation_id(observation, campaign_id=self.campaign_id),
            campaign_id=self.campaign_id,
            market=candle.market,
            symbol=candle.symbol,
            timeframe=candle.interval,
            event_time_ms=candle.close_time_ms,
            candidate_version=observation.candidate_version,
            payload=payload,
            recorded_at_ms=recorded_at_ms,
        )
