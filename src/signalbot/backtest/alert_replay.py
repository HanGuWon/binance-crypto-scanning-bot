from __future__ import annotations

import csv
import hashlib
import json
import math
import random
import statistics
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any

from signalbot.backtest.alert_filter import (
    AlertFilterSnapshot,
    alert_filter_snapshot_at,
    compute_alert_filter_series,
)
from signalbot.backtest.config import BacktestAsset, BacktestSpec
from signalbot.backtest.dataset import (
    build_dataset_manifest,
    read_kline_csv,
    verify_dataset_manifest,
)
from signalbot.backtest.engine import FundingRate, ResearchBacktester, build_market_regimes
from signalbot.backtest.funding import funding_sha256, verify_funding_dataset
from signalbot.backtest.outcomes import (
    RecommendationOutcome,
    RecommendationOutcomeEvaluator,
    RecommendationOutcomeExclusion,
)
from signalbot.backtest.runner import (
    _research_kline_request,
    dataset_path,
    funding_path,
    source_code_digest,
)
from signalbot.config import Settings
from signalbot.data.candles import interval_to_milliseconds
from signalbot.data.microstructure import OrderFlowSnapshot
from signalbot.domain.enums import Market, SignalFamily, SignalStage
from signalbot.domain.models import Candle, FeatureSnapshot, MarketRegime, SignalDecision
from signalbot.signals.rules import SignalRuleEngine
from signalbot.signals.state_machine import SignalStateMachine

_PROTOCOL_VERSION = "alert_replay_v3_2026-07-20_indicator_discriminator"
_HORIZONS = (1, 3, 6, 12, 72)
_PRIMARY_HORIZON = 12
_PRIMARY_MARGIN_BPS = 5.0
_SENSITIVITY_MARGINS_BPS = (0.0, 10.0, 25.0)
_PATH_HORIZON = 72
_DAY_MS = 86_400_000
_WARMUP_DAYS = 40


@dataclass(frozen=True, slots=True)
class RecommendationEvent:
    event_id: str
    protocol_version: str
    rule_version: str
    asset: str
    cohort: str
    market: str
    symbol: str
    family: str
    direction: str
    stage: str
    information_only: bool
    action_label: str
    decision_time_ms: int
    split: str
    score: int
    price: float
    invalidation: float | None
    reasons: str
    regime: str
    btc_trend: str
    breadth_ratio: float
    rsi: float
    ema9: float
    ema20: float
    ema50: float
    ema200: float | None
    ema20_slope_atr: float
    macd_histogram: float
    adx: float
    atr: float
    atr_percent: float
    relative_volume: float
    taker_buy_ratio: float
    volume_zscore: float
    efficiency_ratio_20: float | None
    plus_di_14: float | None
    minus_di_14: float | None
    adx_14: float | None
    adx_delta: float | None
    directional_di_balance: float | None
    directional_di_spread: float | None
    directional_macd_delta_atr: float | None
    directional_taker_delta: float | None
    directional_taker_delta_source: str
    pullback_range_contraction: float | None
    pullback_volume_contraction: float | None
    structure_state: str
    impulse_size_atr: float | None
    pullback_depth: float | None
    pullback_duration_bars: int | None
    pullback_status: str
    confluence_distance_atr: float | None
    recovery_confirmed: bool
    structure_intact: bool


@dataclass(frozen=True, slots=True)
class RecommendationOutcomeRow:
    event_id: str
    horizon_bars: int
    horizon_minutes: int
    evaluable: bool
    exclusion_reason: str
    entry_time_ms: int | None
    exit_time_ms: int | None
    entry_price: float | None
    exit_price: float | None
    raw_close_return: float | None
    maximum_rise: float | None
    maximum_drop: float | None
    gross_return: float | None
    slippage_return: float | None
    fee_return: float | None
    funding_return: float | None
    net_return: float | None
    mfe: float | None
    mae: float | None
    hit_status_5bps: str
    hit_status_0bps: str
    hit_status_10bps: str
    hit_status_25bps: str
    one_r_path_status: str
    one_r_target_price: float | None
    one_r_risk_fraction: float | None
    observed_until_ms: int | None


@dataclass(frozen=True, slots=True)
class _ReplaySelection:
    start_ms: int
    end_ms: int
    split_names: tuple[str, ...]


def _canonical_json(payload: Any) -> str:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _score_bucket(score: int) -> str:
    if score >= 90:
        return "90-100"
    if score >= 80:
        return "80-89"
    if score >= 70:
        return "70-79"
    return "below-70"


def _percentile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _status_for_margin(net_return: float, margin_bps: float) -> str:
    margin = margin_bps / 10_000
    if net_return > margin:
        return "hit"
    if net_return < -margin:
        return "miss"
    return "ambiguous"


def _selection(spec: BacktestSpec, split_names: Sequence[str] | None) -> _ReplaySelection:
    if not split_names:
        return _ReplaySelection(
            int(spec.evaluation_start.timestamp() * 1000),
            int(spec.evaluation_end.timestamp() * 1000),
            tuple(item.name for item in spec.splits),
        )
    requested = tuple(dict.fromkeys(split_names))
    by_name = {item.name: item for item in spec.splits}
    missing = [name for name in requested if name not in by_name]
    if missing:
        raise ValueError(f"unknown backtest split names: {missing}")
    chosen = sorted((by_name[name] for name in requested), key=lambda item: item.start)
    for previous, current in pairwise(chosen):
        if previous.end != current.start:
            raise ValueError("selected splits must form one contiguous interval")
    return _ReplaySelection(
        int(chosen[0].start.timestamp() * 1000),
        int(chosen[-1].end.timestamp() * 1000),
        requested,
    )


def _is_index_recommendation(decision: SignalDecision) -> bool:
    informational = decision.metadata.get("informational_only") is True
    if informational:
        return decision.stage is SignalStage.SETUP and decision.family in {
            SignalFamily.PULLBACK_LONG,
            SignalFamily.PULLBACK_SHORT,
        }
    return decision.stage is SignalStage.CONFIRMED


def _event_row(
    asset: BacktestAsset,
    decision: SignalDecision,
    feature: FeatureSnapshot,
    split: str,
    alert_filter: AlertFilterSnapshot | None = None,
) -> RecommendationEvent:
    structure = feature.chart_structure
    return RecommendationEvent(
        event_id=decision.event_id,
        protocol_version=_PROTOCOL_VERSION,
        rule_version=decision.rule_version,
        asset=asset.asset,
        cohort=asset.cohort,
        market=decision.market.value,
        symbol=decision.symbol,
        family=decision.family.value,
        direction=decision.direction.value,
        stage=decision.stage.value,
        information_only=decision.metadata.get("informational_only") is True,
        action_label=decision.action_label,
        decision_time_ms=decision.event_time_ms,
        split=split,
        score=decision.score,
        price=float(decision.price),
        invalidation=(None if decision.invalidation is None else float(decision.invalidation)),
        reasons="; ".join(decision.reasons),
        regime=feature.regime.label,
        btc_trend=feature.regime.btc_trend,
        breadth_ratio=feature.regime.breadth_ratio,
        rsi=feature.rsi,
        ema9=feature.ema9,
        ema20=feature.ema20,
        ema50=feature.ema50,
        ema200=feature.ema200,
        ema20_slope_atr=feature.ema20_slope_atr,
        macd_histogram=feature.macd_histogram,
        adx=feature.adx,
        atr=feature.atr,
        atr_percent=feature.atr_percent,
        relative_volume=feature.relative_volume,
        taker_buy_ratio=feature.taker_buy_ratio,
        volume_zscore=feature.volume_zscore,
        efficiency_ratio_20=(
            None if alert_filter is None else alert_filter.efficiency_ratio_20
        ),
        plus_di_14=None if alert_filter is None else alert_filter.plus_di_14,
        minus_di_14=None if alert_filter is None else alert_filter.minus_di_14,
        adx_14=None if alert_filter is None else alert_filter.adx_14,
        adx_delta=None if alert_filter is None else alert_filter.adx_delta,
        directional_di_balance=(
            None if alert_filter is None else alert_filter.directional_di_balance
        ),
        directional_di_spread=(
            None if alert_filter is None else alert_filter.directional_di_spread
        ),
        directional_macd_delta_atr=(
            None if alert_filter is None else alert_filter.directional_macd_delta_atr
        ),
        directional_taker_delta=(
            None if alert_filter is None else alert_filter.directional_taker_delta
        ),
        directional_taker_delta_source=(
            "unavailable"
            if alert_filter is None
            else alert_filter.directional_taker_delta_source
        ),
        pullback_range_contraction=(
            None if alert_filter is None else alert_filter.pullback_range_contraction
        ),
        pullback_volume_contraction=(
            None if alert_filter is None else alert_filter.pullback_volume_contraction
        ),
        structure_state=structure.state,
        impulse_size_atr=structure.impulse_size_atr,
        pullback_depth=structure.pullback_depth,
        pullback_duration_bars=structure.pullback_duration_bars,
        pullback_status=structure.pullback_status,
        confluence_distance_atr=structure.confluence_distance_atr,
        recovery_confirmed=structure.recovery_confirmed,
        structure_intact=structure.structure_intact,
    )


def _excluded_outcome_row(
    event_id: str,
    horizon: int,
    interval_ms: int,
    reason: str,
) -> RecommendationOutcomeRow:
    return RecommendationOutcomeRow(
        event_id=event_id,
        horizon_bars=horizon,
        horizon_minutes=horizon * interval_ms // 60_000,
        evaluable=False,
        exclusion_reason=reason,
        entry_time_ms=None,
        exit_time_ms=None,
        entry_price=None,
        exit_price=None,
        raw_close_return=None,
        maximum_rise=None,
        maximum_drop=None,
        gross_return=None,
        slippage_return=None,
        fee_return=None,
        funding_return=None,
        net_return=None,
        mfe=None,
        mae=None,
        hit_status_5bps="unevaluable",
        hit_status_0bps="unevaluable",
        hit_status_10bps="unevaluable",
        hit_status_25bps="unevaluable",
        one_r_path_status="unevaluable",
        one_r_target_price=None,
        one_r_risk_fraction=None,
        observed_until_ms=None,
    )


def _outcome_row(
    outcome: RecommendationOutcome,
    candles: Sequence[Candle],
    decision_index: int,
    interval_ms: int,
) -> RecommendationOutcomeRow:
    horizon = outcome.horizon_bars
    path = candles[decision_index + 1 : decision_index + horizon + 1]
    entry = outcome.entry_price
    maximum_rise = max(float(item.high) / entry - 1 for item in path)
    maximum_drop = min(float(item.low) / entry - 1 for item in path)
    return RecommendationOutcomeRow(
        event_id=outcome.event_id,
        horizon_bars=horizon,
        horizon_minutes=horizon * interval_ms // 60_000,
        evaluable=True,
        exclusion_reason="",
        entry_time_ms=outcome.entry_time_ms,
        exit_time_ms=outcome.exit_time_ms,
        entry_price=entry,
        exit_price=outcome.exit_price,
        raw_close_return=outcome.exit_price / entry - 1,
        maximum_rise=maximum_rise,
        maximum_drop=maximum_drop,
        gross_return=outcome.gross_return,
        slippage_return=outcome.slippage_return,
        fee_return=outcome.fee_return,
        funding_return=outcome.funding_return,
        net_return=outcome.net_return,
        mfe=outcome.mfe,
        mae=outcome.mae,
        hit_status_5bps=outcome.hit_status.value,
        hit_status_0bps=_status_for_margin(outcome.net_return, 0.0),
        hit_status_10bps=_status_for_margin(outcome.net_return, 10.0),
        hit_status_25bps=_status_for_margin(outcome.net_return, 25.0),
        one_r_path_status=outcome.one_r_path.status.value,
        one_r_target_price=outcome.one_r_path.target_price,
        one_r_risk_fraction=outcome.one_r_path.risk_fraction,
        observed_until_ms=outcome.one_r_path.observed_until_ms,
    )


def _run_symbol(
    settings: Settings,
    spec: BacktestSpec,
    asset: BacktestAsset,
    market: Market,
    candles: list[Candle],
    regimes: Sequence[MarketRegime],
    funding: list[FundingRate],
    selection: _ReplaySelection,
    original_first_open_ms: int,
) -> tuple[list[RecommendationEvent], list[RecommendationOutcomeRow]]:
    if len(candles) != len(regimes):
        raise ValueError("regimes must align with replay candles")
    backtester = ResearchBacktester(settings, spec)
    rule_settings = backtester.rule_settings
    flows = [OrderFlowSnapshot() for _ in candles]
    features = backtester._continuous_features(candles, flows, list(regimes))
    alert_filter_series = compute_alert_filter_series(candles)
    context_index = backtester._higher_timeframe_context_index(candles, list(regimes))
    if market is Market.FUTURES and funding:
        features = backtester._with_funding_features(
            features,
            funding,
            asset.futures_symbol,
        )

    rule_engine = SignalRuleEngine(rule_settings)
    state_machine = SignalStateMachine(rule_settings, spec.rule_version)
    evaluator = RecommendationOutcomeEvaluator()
    interval_ms = interval_to_milliseconds(spec.interval)
    age_gate_ms = original_first_open_ms + spec.minimum_age_days * _DAY_MS
    history_gate_ms = original_first_open_ms + spec.minimum_history_bars * interval_ms
    eligible_from_ms = max(selection.start_ms, age_gate_ms, history_gate_ms)
    selected_splits = frozenset(selection.split_names)
    events: list[RecommendationEvent] = []
    outcomes: list[RecommendationOutcomeRow] = []
    active_information_episodes: set[tuple[str, str]] = set()

    fee_bps = spec.costs.spot_fee_bps if market is Market.SPOT else spec.costs.futures_fee_bps
    slippage_bps = (
        spec.costs.spot_slippage_bps[asset.cohort]
        if market is Market.SPOT
        else spec.costs.futures_slippage_bps[asset.cohort]
    )

    for index, candle in enumerate(candles):
        if candle.open_time_ms >= selection.end_ms:
            break
        gap = index > 0 and candle.open_time_ms - candles[index - 1].open_time_ms != interval_ms
        split_boundary = index > 0 and spec.split_name(candle.open_time_ms) != spec.split_name(
            candles[index - 1].open_time_ms
        )
        if gap or split_boundary:
            state_machine = SignalStateMachine(rule_settings, spec.rule_version)
            active_information_episodes.clear()
        feature = features[index]
        if feature is None:
            continue
        contexts = context_index.at(feature.event_time_ms)
        for evaluation in rule_engine.evaluate(feature, contexts):
            decision = state_machine.process(evaluation)
            if decision is None:
                continue
            informational = decision.metadata.get("informational_only") is True
            episode_key = (decision.family.value, decision.timeframe)
            if informational and decision.stage is SignalStage.INVALIDATED:
                active_information_episodes.discard(episode_key)
                continue
            if not _is_index_recommendation(decision):
                continue
            if informational:
                if episode_key in active_information_episodes:
                    continue
                active_information_episodes.add(episode_key)
            split = spec.split_name(candle.open_time_ms)
            if (
                candle.open_time_ms < eligible_from_ms
                or candle.open_time_ms >= selection.end_ms
                or split is None
                or split not in selected_splits
            ):
                continue
            alert_filter = alert_filter_snapshot_at(
                alert_filter_series,
                feature,
                decision.direction,
                index,
            )
            event = _event_row(asset, decision, feature, split, alert_filter)
            events.append(event)
            for horizon in _HORIZONS:
                eligible, exclusion = backtester._analysis_horizon_status(
                    candles,
                    index,
                    horizon,
                    split_start_embargo_bars=_PATH_HORIZON,
                )
                if not eligible:
                    outcomes.append(
                        _excluded_outcome_row(
                            decision.event_id,
                            horizon,
                            interval_ms,
                            exclusion,
                        )
                    )
                    continue
                outcome_candles = candles[index + 1 : index + horizon + 1]
                entry_time_ms = outcome_candles[0].open_time_ms
                exit_time_ms = outcome_candles[-1].close_time_ms
                relevant_funding = [
                    item for item in funding if entry_time_ms < item.funding_time_ms < exit_time_ms
                ]
                result = evaluator.evaluate(
                    decision,
                    outcome_candles,
                    horizon,
                    fee_bps=fee_bps,
                    slippage_bps=slippage_bps,
                    funding=(
                        relevant_funding
                        if market is Market.FUTURES and spec.costs.include_funding
                        else ()
                    ),
                    hit_margin_bps=_PRIMARY_MARGIN_BPS,
                )
                if isinstance(result, RecommendationOutcomeExclusion):
                    outcomes.append(
                        _excluded_outcome_row(
                            decision.event_id,
                            horizon,
                            interval_ms,
                            result.reason.value,
                        )
                    )
                else:
                    outcomes.append(_outcome_row(result, candles, index, interval_ms))
    return events, outcomes


def _write_dataclass_csv(values: Iterable[Any], model: type[Any], path: Path) -> None:
    names = [field.name for field in fields(model)]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, lineterminator="\n")
        writer.writeheader()
        for value in values:
            writer.writerow(asdict(value))


def _panel_name(event: RecommendationEvent) -> str:
    return "information_only_shadow" if event.information_only else "confirmed_paper"


def _summary_row(
    pairs: Sequence[tuple[RecommendationEvent, RecommendationOutcomeRow]],
    *,
    evaluation_days: float,
) -> dict[str, Any]:
    event_count = len(pairs)
    evaluable = [outcome for _, outcome in pairs if outcome.evaluable]
    net = [value.net_return for value in evaluable if value.net_return is not None]
    mfe = [value.mfe for value in evaluable if value.mfe is not None]
    mae = [value.mae for value in evaluable if value.mae is not None]
    rise = [value.maximum_rise for value in evaluable if value.maximum_rise is not None]
    drop = [value.maximum_drop for value in evaluable if value.maximum_drop is not None]
    statuses = Counter(value.hit_status_5bps for value in evaluable)
    resolved = statuses["hit"] + statuses["miss"]
    positive = sum(value for value in net if value > 0)
    negative = -sum(value for value in net if value < 0)
    return {
        "events": event_count,
        "evaluable": len(evaluable),
        "coverage": len(evaluable) / event_count if event_count else 0.0,
        "hits": statuses["hit"],
        "misses": statuses["miss"],
        "ambiguous": statuses["ambiguous"],
        "strict_hit_rate": statuses["hit"] / event_count if event_count else 0.0,
        "resolved_accuracy": statuses["hit"] / resolved if resolved else None,
        "resolved_coverage": resolved / event_count if event_count else 0.0,
        "mean_net_return": statistics.fmean(net) if net else None,
        "median_net_return": statistics.median(net) if net else None,
        "net_return_q25": _percentile(net, 0.25),
        "net_return_q75": _percentile(net, 0.75),
        "profit_factor": positive / negative if negative > 0 else None,
        "mean_mfe": statistics.fmean(mfe) if mfe else None,
        "median_mfe": statistics.median(mfe) if mfe else None,
        "mean_mae": statistics.fmean(mae) if mae else None,
        "median_mae": statistics.median(mae) if mae else None,
        "mean_maximum_rise": statistics.fmean(rise) if rise else None,
        "mean_maximum_drop": statistics.fmean(drop) if drop else None,
        "notional_100_mean_pnl_usdt": statistics.fmean(net) * 100 if net else None,
        "events_per_day": event_count / evaluation_days if evaluation_days else None,
        "exclusions": dict(
            sorted(
                Counter(
                    outcome.exclusion_reason for _, outcome in pairs if not outcome.evaluable
                ).items()
            )
        ),
    }


def _grouped_summaries(
    pairs: Sequence[tuple[RecommendationEvent, RecommendationOutcomeRow]],
    *,
    evaluation_days: float,
) -> dict[str, list[dict[str, Any]]]:
    dimensions: dict[str, Any] = {
        "panel": lambda event: _panel_name(event),
        "market": lambda event: event.market,
        "direction": lambda event: event.direction,
        "family": lambda event: event.family,
        "asset": lambda event: event.asset,
        "split": lambda event: event.split,
        "regime": lambda event: event.regime,
        "score_bucket": lambda event: _score_bucket(event.score),
    }
    output: dict[str, list[dict[str, Any]]] = {}
    for name, key_function in dimensions.items():
        grouped: dict[str, list[tuple[RecommendationEvent, RecommendationOutcomeRow]]] = (
            defaultdict(list)
        )
        for pair in pairs:
            grouped[str(key_function(pair[0]))].append(pair)
        output[name] = [
            {
                "value": value,
                **_summary_row(rows, evaluation_days=evaluation_days),
            }
            for value, rows in sorted(grouped.items())
        ]
        if name == "panel":
            present = {row["value"] for row in output[name]}
            for panel in ("confirmed_paper", "information_only_shadow"):
                if panel not in present:
                    output[name].append(
                        {
                            "value": panel,
                            **_summary_row((), evaluation_days=evaluation_days),
                        }
                    )
            output[name].sort(key=lambda row: row["value"])
    return output


def _quantile(values: Sequence[float], probability: float) -> float | None:
    return _percentile(values, probability)


def _shared_calendar_bootstrap(
    pairs: Sequence[tuple[RecommendationEvent, RecommendationOutcomeRow]],
    selection: _ReplaySelection,
    *,
    samples: int,
    seed: int,
    block_days: int,
) -> dict[str, Any]:
    if samples < 100 or block_days <= 0:
        raise ValueError("bootstrap requires at least 100 samples and a positive block")
    start_day = selection.start_ms // _DAY_MS
    end_day = (selection.end_ms - 1) // _DAY_MS
    day_count = end_day - start_day + 1
    panels = ("confirmed_paper", "information_only_shadow")
    arrays: dict[str, dict[str, list[float]]] = {
        panel: {
            "events": [0.0] * day_count,
            "hits": [0.0] * day_count,
            "net_count": [0.0] * day_count,
            "net_sum": [0.0] * day_count,
        }
        for panel in panels
    }
    for event, outcome in pairs:
        offset = event.decision_time_ms // _DAY_MS - start_day
        if not 0 <= offset < day_count:
            raise ValueError("recommendation lies outside the bootstrap calendar")
        panel = _panel_name(event)
        arrays[panel]["events"][offset] += 1
        if outcome.evaluable:
            arrays[panel]["hits"][offset] += int(outcome.hit_status_5bps == "hit")
            if outcome.net_return is not None:
                arrays[panel]["net_count"][offset] += 1
                arrays[panel]["net_sum"][offset] += outcome.net_return

    full_blocks, remainder = divmod(day_count, block_days)
    lengths = [block_days] * full_blocks + ([remainder] if remainder else [])
    rng = random.Random(seed)
    estimates: dict[str, dict[str, list[float]]] = {
        panel: {"strict_hit_rate": [], "mean_net_return": []} for panel in panels
    }
    invalid = Counter[str]()
    schedule_digest = hashlib.sha256()
    for _ in range(samples):
        starts = [rng.randrange(day_count) for _ in lengths]
        schedule_digest.update(",".join(str(value) for value in starts).encode("ascii") + b"\n")
        day_indices = [
            (start + offset) % day_count
            for start, length in zip(starts, lengths, strict=True)
            for offset in range(length)
        ]
        for panel in panels:
            values = arrays[panel]
            event_total = sum(values["events"][index] for index in day_indices)
            net_count = sum(values["net_count"][index] for index in day_indices)
            if event_total <= 0 or net_count <= 0:
                invalid[panel] += 1
                continue
            estimates[panel]["strict_hit_rate"].append(
                sum(values["hits"][index] for index in day_indices) / event_total
            )
            estimates[panel]["mean_net_return"].append(
                sum(values["net_sum"][index] for index in day_indices) / net_count
            )

    result: dict[str, Any] = {
        "method": "shared circular UTC-calendar moving-block percentile bootstrap",
        "samples": samples,
        "seed": seed,
        "block_days": block_days,
        "calendar_days": day_count,
        "shared_draw_schedule_sha256": schedule_digest.hexdigest(),
        "panels": {},
    }
    for panel in panels:
        panel_estimates = estimates[panel]
        result["panels"][panel] = {
            "valid_replicates": len(panel_estimates["mean_net_return"]),
            "invalid_replicates": invalid[panel],
            "strict_hit_rate_95_interval": [
                _quantile(panel_estimates["strict_hit_rate"], 0.025),
                _quantile(panel_estimates["strict_hit_rate"], 0.975),
            ],
            "mean_net_return_95_interval": [
                _quantile(panel_estimates["mean_net_return"], 0.025),
                _quantile(panel_estimates["mean_net_return"], 0.975),
            ],
        }
    return result


def _path_summaries(
    pairs: Sequence[tuple[RecommendationEvent, RecommendationOutcomeRow]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[RecommendationOutcomeRow]] = defaultdict(list)
    for event, outcome in pairs:
        grouped[_panel_name(event)].append(outcome)
    rows: list[dict[str, Any]] = []
    for panel in ("confirmed_paper", "information_only_shadow"):
        values = grouped[panel]
        statuses = Counter(value.one_r_path_status for value in values)
        total = len(values)
        rows.append(
            {
                "panel": panel,
                "events": total,
                "target_first": statuses["target_first"],
                "invalidation_first": statuses["invalidation_first"],
                "collision": statuses["collision"],
                "timeout": statuses["timeout"],
                "invalid_invalidation": statuses["invalid_invalidation"],
                "unevaluable": statuses["unevaluable"],
                "strict_path_hit_rate": (statuses["target_first"] / total if total else 0.0),
            }
        )
    return rows


def _historical_screen_status(
    summary: dict[str, Any],
    bootstrap: dict[str, Any],
) -> str:
    interval = bootstrap["mean_net_return_95_interval"]
    if (
        summary["events"] < 100
        or summary["coverage"] < 0.95
        or interval[0] is None
        or interval[1] is None
    ):
        return "INCONCLUSIVE_LOW_INFORMATION"
    return "RETROSPECTIVE_DESCRIPTIVE_ONLY_NO_MATCHED_BASELINE"


def _fmt_percent(value: float | None) -> str:
    return "N/A" if value is None else f"{value * 100:.2f}%"


def _fmt_bps(value: float | None) -> str:
    return "N/A" if value is None else f"{value * 10_000:.2f}"


def _render_report(results: dict[str, Any]) -> str:
    assets = results["selection"]["assets"]
    asset_summary = ", ".join(assets)
    lines = [
        "# 현재 Discord 추천 규칙 회고 백테스트",
        "",
        (
            "> 이 결과는 과거 봉에 현재 규칙을 재생한 counterfactual audit입니다. "
            "실제로 전달된 Discord outbox 로그의 사후 감사가 아니며, 규칙 설계 뒤의 "
            "미사용 prospective 검증을 대체하지 않습니다."
        ),
        "",
        "## 평가 계약",
        "",
        "- 폐봉에서 판단하고 다음 연속 5분봉 시가를 가상 진입가로 사용",
        "- 12봉(60분) 후 비용 반영 방향수익이 +5bp 초과면 HIT, -5bp 미만이면 MISS",
        "- -5bp~+5bp는 AMBIGUOUS이며 엄격 적중률 분모에 그대로 포함",
        "- 정보성 눌림목 SETUP과 실행 가능 CONFIRMED PAPER 경보를 분리",
        "- 72봉 안의 1R 목표/무효화 동시 터치는 OHLC 순서 불명으로 collision 처리",
        "- 1R 장벽은 비용 미반영 가격 경로이며 순수익 판정과 별개",
        "- 기술적 청산은 이 고정 구간 표에 섞지 않고 별도 counterfactual 평가 대상으로 관리",
        "",
        "## 구간별 비용 반영 결과",
        "",
        "| 구간 | 패널 | N | 평가율 | 엄격 적중률 | 평균 net(bp) | PF |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for horizon_text, rows in results["horizons_by_panel"].items():
        horizon_bars = int(horizon_text)
        horizon_minutes = horizon_bars * 5
        for row in rows:
            profit_factor = row["profit_factor"]
            profit_factor_text = "N/A" if profit_factor is None else f"{profit_factor:.3f}"
            lines.append(
                f"| {horizon_minutes}분({horizon_bars}봉) | {row['value']} | "
                f"{row['events']} | {_fmt_percent(row['coverage'])} | "
                f"{_fmt_percent(row['strict_hit_rate'])} | "
                f"{_fmt_bps(row['mean_net_return'])} | {profit_factor_text} |"
            )
    lines.extend(
        [
            "",
            "## 60분 핵심 결과",
            "",
            (
                "| 패널 | N | 평가율 | 엄격 적중률 | 판정된 정확도 | "
                "평균 net(bp) | 중앙 net(bp) | PF | 상태 |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    bootstrap = results["bootstrap"]["7"]
    for row in results["primary_by_panel"]:
        panel = row["value"]
        status = results["screen_status"][panel]
        profit_factor = row["profit_factor"]
        profit_factor_text = "N/A" if profit_factor is None else f"{profit_factor:.3f}"
        lines.append(
            f"| {panel} | {row['events']} | {_fmt_percent(row['coverage'])} | "
            f"{_fmt_percent(row['strict_hit_rate'])} | "
            f"{_fmt_percent(row['resolved_accuracy'])} | "
            f"{_fmt_bps(row['mean_net_return'])} | "
            f"{_fmt_bps(row['median_net_return'])} | "
            f"{profit_factor_text} | "
            f"{status} |"
        )
        ci = bootstrap["panels"][panel]
        lines.extend(
            [
                "",
                (
                    f"- {panel} 7일 공유 블록 95% 구간: net "
                    f"{_fmt_bps(ci['mean_net_return_95_interval'][0])}~"
                    f"{_fmt_bps(ci['mean_net_return_95_interval'][1])}bp, 엄격 적중률 "
                    f"{_fmt_percent(ci['strict_hit_rate_95_interval'][0])}~"
                    f"{_fmt_percent(ci['strict_hit_rate_95_interval'][1])}"
                ),
            ]
        )
    lines.extend(
        [
            "",
            "## 72봉 1R 경로 결과",
            "",
            (
                "| 패널 | N | 목표 선도 | 무효화 선도 | 동시충돌 | "
                "시간초과 | 무효 무효화선 | 엄격 path hit |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in results["path_72"]:
        lines.append(
            f"| {row['panel']} | {row['events']} | {row['target_first']} | "
            f"{row['invalidation_first']} | {row['collision']} | {row['timeout']} | "
            f"{row['invalid_invalidation']} | {_fmt_percent(row['strict_path_hit_rate'])} |"
        )
    lines.extend(
        [
            "",
            "## 해석 제한",
            "",
            "- Spot short 방향은 실제 공매도 손익이 아니라 하락/보유회피 경고의 방향 정확도입니다.",
            (
                "- 정보성 눌림목의 net은 가상 방향수익(counterfactual expectancy)이며 "
                "거래 P&L이 아닙니다."
            ),
            (
                "- 고정 horizon 이벤트는 서로 겹칠 수 있어 실현 가능한 포트폴리오 "
                "equity curve가 아닙니다."
            ),
            (
                "- 기술적 청산 평가기는 다음 연속 5분봉 시가 진입, 손절·트레일링·"
                "추세 실패·시간 종료와 비용을 인과적으로 계산하지만, 이 보고서에는 "
                "아직 결합하지 않았습니다."
            ),
            (
                "- Binance kline과 정산 funding만 사용했습니다. 과거 BBO/OI가 없는 "
                "지표는 live parity가 아닙니다."
            ),
            (
                f"- 고정 {len(assets)}종 연구 universe({asset_summary})이며 live 동적 "
                "top-N 전체를 재현하지 않습니다. "
                "confirmed_paper가 0건이면 실행 가능 규칙의 성과는 미검증입니다."
            ),
            (
                "- funding mark price가 없는 과거 행은 진입가를 대체값으로 사용하므로 "
                "5bp 경계 부근에는 작은 근사가 있습니다."
            ),
            (
                "- 모든 과거 구간이 규칙 설계 전에 완전히 봉인된 holdout은 아니므로 "
                "독립 수익 검증 상태는 NO입니다."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def run_alert_replay(
    settings: Settings,
    spec: BacktestSpec,
    data_dir: str | Path,
    output_dir: str | Path,
    *,
    workspace_root: str | Path,
    spec_path: str | Path,
    config_path: str | Path,
    split_names: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Replay current alert transitions and persist a separate outcome audit panel."""

    started = datetime.now(UTC)
    started_counter = time.perf_counter()
    data_root = Path(data_dir)
    output_root = Path(output_dir)
    workspace = Path(workspace_root)
    selection = _selection(spec, split_names)
    warmup_start_ms = selection.start_ms - int(timedelta(days=_WARMUP_DAYS).total_seconds() * 1000)
    start_ms = int(spec.data_start.timestamp() * 1000)
    end_ms = int(spec.evaluation_end.timestamp() * 1000) - 1
    input_hashes: dict[str, str] = {}
    events: list[RecommendationEvent] = []
    outcomes: list[RecommendationOutcomeRow] = []
    per_symbol: list[dict[str, Any]] = []

    for market in (Market.SPOT, Market.FUTURES):
        candles_by_asset: dict[str, list[Candle]] = {}
        original_first_open: dict[str, int] = {}
        for asset in spec.assets:
            symbol = asset.spot_symbol if market is Market.SPOT else asset.futures_symbol
            path = dataset_path(data_root, market, asset.asset, symbol, spec.interval)
            manifest_path = path.with_suffix(path.suffix + ".manifest.json")
            request = _research_kline_request(spec, market, asset.asset, symbol)
            verify_dataset_manifest(path, manifest_path, expected_request=request)
            dataset = read_kline_csv(path)
            original_first_open[asset.asset] = dataset.candles[0].open_time_ms
            sliced = [
                candle
                for candle in dataset.candles
                if warmup_start_ms <= candle.open_time_ms < selection.end_ms
            ]
            if not sliced or sliced[0].open_time_ms > selection.start_ms - 10 * _DAY_MS:
                raise ValueError(
                    f"insufficient alert-replay warmup for {market.value}:{asset.asset}"
                )
            candles_by_asset[asset.asset] = sliced
            input_hashes[path.relative_to(data_root).as_posix()] = build_dataset_manifest(
                path
            ).sha256

        regimes = build_market_regimes(candles_by_asset)
        for asset in spec.assets:
            funding: list[FundingRate] = []
            if market is Market.FUTURES and spec.costs.include_funding:
                path = funding_path(
                    data_root,
                    asset.asset,
                    asset.futures_symbol,
                    spec.interval,
                )
                dataset = verify_funding_dataset(
                    path,
                    expected_symbol=asset.futures_symbol,
                    expected_start_time_ms=start_ms,
                    expected_end_time_ms=end_ms,
                )
                funding = [
                    item
                    for item in dataset.rates
                    if warmup_start_ms <= item.funding_time_ms < selection.end_ms
                ]
                input_hashes[path.relative_to(data_root).as_posix()] = funding_sha256(path)
            symbol_started = time.perf_counter()
            symbol_events, symbol_outcomes = _run_symbol(
                settings,
                spec,
                asset,
                market,
                candles_by_asset[asset.asset],
                regimes[asset.asset],
                funding,
                selection,
                original_first_open[asset.asset],
            )
            events.extend(symbol_events)
            outcomes.extend(symbol_outcomes)
            per_symbol.append(
                {
                    "market": market.value,
                    "asset": asset.asset,
                    "candles": len(candles_by_asset[asset.asset]),
                    "events": len(symbol_events),
                    "outcome_rows": len(symbol_outcomes),
                    "duration_seconds": time.perf_counter() - symbol_started,
                }
            )

    events.sort(key=lambda item: (item.decision_time_ms, item.event_id))
    outcomes.sort(key=lambda item: (item.event_id, item.horizon_bars))
    event_by_id = {event.event_id: event for event in events}
    if len(event_by_id) != len(events):
        raise ValueError("alert replay produced duplicate event ids")
    pairs_by_horizon: dict[int, list[tuple[RecommendationEvent, RecommendationOutcomeRow]]] = {
        horizon: [] for horizon in _HORIZONS
    }
    for outcome in outcomes:
        event = event_by_id.get(outcome.event_id)
        if event is None:
            raise ValueError(f"outcome references unknown event {outcome.event_id}")
        pairs_by_horizon[outcome.horizon_bars].append((event, outcome))

    evaluation_days = (selection.end_ms - selection.start_ms) / _DAY_MS
    primary_pairs = pairs_by_horizon[_PRIMARY_HORIZON]
    grouped = _grouped_summaries(primary_pairs, evaluation_days=evaluation_days)
    bootstrap = {
        str(block_days): _shared_calendar_bootstrap(
            primary_pairs,
            selection,
            samples=spec.bootstrap.samples,
            seed=spec.bootstrap.seed + block_days,
            block_days=block_days,
        )
        for block_days in (7, 14, 28)
    }
    primary_by_panel = grouped["panel"]
    seven_day = bootstrap["7"]["panels"]
    screen_status = {
        row["value"]: _historical_screen_status(row, seven_day[row["value"]])
        for row in primary_by_panel
    }
    results: dict[str, Any] = {
        "protocol_version": _PROTOCOL_VERSION,
        "rule_version": spec.rule_version,
        "status": {
            "independently_validated": False,
            "deployment_approved": False,
            "reason": (
                "historical counterfactual replay on an already exposed period; "
                "prospective post-freeze shadow validation is still required"
            ),
        },
        "selection": {
            "start_utc": datetime.fromtimestamp(selection.start_ms / 1000, UTC).isoformat(),
            "end_utc": datetime.fromtimestamp(selection.end_ms / 1000, UTC).isoformat(),
            "splits": list(selection.split_names),
            "interval": spec.interval,
            "warmup_days": _WARMUP_DAYS,
            "assets": [asset.asset for asset in spec.assets],
            "markets": [Market.SPOT.value, Market.FUTURES.value],
            "universe_mode": "fixed_backtest_spec_assets_not_live_dynamic_top_n",
            "live_top_n_setting": settings.binance.top_n,
        },
        "evaluation_contract": {
            "index_events": {
                "information_only": (
                    "first SETUP transition until an INVALIDATED episode boundary"
                ),
                "actionable": "CONFIRMED transition",
            },
            "entry": f"next contiguous {spec.interval} open after a closed decision bar",
            "horizons_bars": list(_HORIZONS),
            "primary_horizon_bars": _PRIMARY_HORIZON,
            "primary_margin_bps": _PRIMARY_MARGIN_BPS,
            "sensitivity_margins_bps": list(_SENSITIVITY_MARGINS_BPS),
            "costs": spec.costs.model_dump(mode="json"),
            "same_bar_target_stop": "collision; strict non-hit",
            "split_start_embargo_bars": _PATH_HORIZON,
        },
        "events": len(events),
        "outcome_rows": len(outcomes),
        "per_symbol": per_symbol,
        "primary_by_panel": primary_by_panel,
        "primary_slices": grouped,
        "horizons_by_panel": {
            str(horizon): _grouped_summaries(
                pairs,
                evaluation_days=evaluation_days,
            )["panel"]
            for horizon, pairs in pairs_by_horizon.items()
        },
        "path_72": _path_summaries(pairs_by_horizon[_PATH_HORIZON]),
        "bootstrap": bootstrap,
        "screen_status": screen_status,
    }

    output_root.mkdir(parents=True, exist_ok=True)
    recommendations_path = output_root / "recommendations.csv"
    outcomes_path = output_root / "outcomes.csv"
    results_path = output_root / "results.json"
    report_path = output_root / "report_ko.md"
    _write_dataclass_csv(events, RecommendationEvent, recommendations_path)
    _write_dataclass_csv(outcomes, RecommendationOutcomeRow, outcomes_path)
    results_path.write_text(_canonical_json(results), encoding="utf-8", newline="\n")
    report_path.write_text(_render_report(results), encoding="utf-8", newline="\n")

    completed = datetime.now(UTC)
    manifest = {
        "protocol_version": _PROTOCOL_VERSION,
        "rule_version": spec.rule_version,
        "code_sha256": source_code_digest(workspace),
        "spec_path": str(Path(spec_path).resolve()),
        "spec_sha256": _sha256(Path(spec_path)),
        "config_path": str(Path(config_path).resolve()),
        "config_sha256": _sha256(Path(config_path)),
        "started_at_utc": started.isoformat(),
        "completed_at_utc": completed.isoformat(),
        "duration_seconds": time.perf_counter() - started_counter,
        "inputs": dict(sorted(input_hashes.items())),
        "outputs": {
            path.name: _sha256(path)
            for path in (recommendations_path, outcomes_path, results_path, report_path)
        },
    }
    manifest_path = output_root / "run_manifest.json"
    manifest_path.write_text(_canonical_json(manifest), encoding="utf-8", newline="\n")
    return {
        "output_dir": str(output_root.resolve()),
        "events": len(events),
        "outcome_rows": len(outcomes),
        "screen_status": screen_status,
        "duration_seconds": manifest["duration_seconds"],
    }
