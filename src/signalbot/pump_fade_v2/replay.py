"""Reproducible public-kline feasibility replay, with strict evidence labels.

Historical klines cannot recreate receipt-time OI, cap metadata, BBO, forced
liquidations, historical PIT universe, or mark/queue fills. The output therefore
reports *proxy diagnostics* and UNAVAILABLE official efficacy gates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from statistics import median
from typing import Any

from signalbot.backtest.dataset import (
    read_dataset_manifest,
    read_kline_csv,
    sha256_file,
    verify_dataset_manifest,
)
from signalbot.domain.models import Candle
from signalbot.pump_fade_v2 import (
    POLICY_VERSION,
    PRIMARY_SQUEEZE_ADVERSE_MARK_EXCURSION_PCT,
)
from signalbot.pump_fade_v2.execution_replay import (
    ParentPath,
    compare_parent_paths,
    summarize_arm_results,
)

FIVE_MIN = 300_000
HORIZON = 288
RELEASES = ("R1_NO_HIGH_60M", "R2_15M_BREAK_FAILED_RETEST",
            "R3_OI_FALL_5PCT_LAG", "R4_15M_CLOSE_BELOW_AVWAP",
            "R5_PREDICTED_FUNDING_NORMALIZED")
SKIP_CELLS = tuple((dd, reb) for dd in (20, 30, 40) for reb in (5, 10, 15))
SKIP_ARM_KEYS = (*tuple(f"joint_{d}_{r}" for d, r in SKIP_CELLS),
                 "joint_25_8", "unconditional_40")


@dataclass(frozen=True, slots=True)
class ProxyEvent:
    """Retrospective kline prefilter, not an eligible PIT strategy opportunity."""

    event_id: str
    symbol: str
    event_ms: int
    event_day_utc: str
    quote_volume_24h: float
    return_24h_pct: float
    future_bars: int
    baseline_24h_short_return_bps: float | None
    kline_proxy_24h_squeeze: bool | None
    baseline_4h_short_return_bps: float | None
    kline_proxy_4h_squeeze: bool | None
    kline_proxy_24h_mfe_pct: float | None
    post_4h_landmark_20h_kline_squeeze: bool | None
    first_release_minutes: dict[str, int | None]
    release_4h_proxy_short_return_bps: dict[str, float | None]
    release_4h_proxy_squeeze: dict[str, bool | None]
    release_24h_proxy_short_return_bps: dict[str, float | None]
    release_24h_proxy_squeeze: dict[str, bool | None]
    skip_cells: dict[str, bool | None]
    continuation_long_new_high_wait_minutes: int | None
    continuation_long_new_high_wait_return_bps: float | None
    kline_proxy_24h_squeeze_20pct_sensitivity: bool | None


def _wait_from_5m(bars: tuple[Candle, ...], index: int) -> bool | None:
    if index < 156:
        return None
    prefix = bars[index - 156:index + 1]
    if any(b.open_time_ms - a.open_time_ms != FIVE_MIN
           for a, b in pairwise(prefix)):
        return None
    ret1 = bars[index].close / bars[index - 12].close - Decimal(1)
    ret4 = bars[index].close / bars[index - 48].close - Decimal(1)
    last_hour = sum(float(x.quote_volume) for x in bars[index - 11:index + 1])
    baseline = [sum(float(x.quote_volume) for x in bars[index - 11 - 12 * (j + 1):
                                                              index - 11 - 12 * j])
                for j in range(12)]
    reference = median(baseline)
    return (ret1 > Decimal("0.05") or ret4 > Decimal("0.15")
            or reference <= 0 or last_hour >= 2 * reference)


def _squeeze_crossed(high: Decimal, origin: Decimal, threshold_pct: float) -> bool:
    """Compare exact decimal candle prices at the inclusive registered boundary."""

    threshold = Decimal(str(threshold_pct)) / Decimal(100)
    return high / origin - Decimal(1) >= threshold


def _release_proxy(
    bars: tuple[Candle, ...], event_index: int, horizon_end: int,
) -> dict[str, int | None]:
    first: dict[str, int | None] = dict.fromkeys(RELEASES)
    peak = float(bars[event_index].high)
    peak_at = event_index
    cum_price_volume = 0.0
    cum_volume = 0.0
    fifteen: list[tuple[int, float, float, float]] = []
    for i in range(event_index + 1, horizon_end + 1):
        bar = bars[i]
        if float(bar.high) > peak:
            peak = float(bar.high)
            peak_at = i
        vol = float(bar.volume)
        cum_price_volume += (float(bar.high + bar.low + bar.close) / 3) * vol
        cum_volume += vol
        if (bar.close_time_ms + 1) % 900_000 == 0 and i >= event_index + 3:
            chunk = bars[i - 2:i + 1]
            if all(b.open_time_ms - a.open_time_ms == FIVE_MIN
                   for a, b in pairwise(chunk)):
                fifteen.append((i, min(float(x.low) for x in chunk),
                                max(float(x.high) for x in chunk), float(bar.close)))
        waiting = _wait_from_5m(bars, i)
        if waiting is not False:
            continue
        if first[RELEASES[0]] is None and i - peak_at >= 12:
            first[RELEASES[0]] = (i - event_index) * 5
        if first[RELEASES[1]] is None:
            for j in range(2, len(fifteen)):
                break_level = min(fifteen[j - 2][1], fifteen[j - 1][1])
                if fifteen[j][3] >= break_level:
                    continue
                if any(b[2] >= break_level * 0.9975 and b[2] < peak
                       and b[3] < break_level
                       for b in fifteen[j + 1:j + 5]):
                    first[RELEASES[1]] = (i - event_index) * 5
                    break
        if first[RELEASES[3]] is None and fifteen and cum_volume > 0:
            if fifteen[-1][0] == i and fifteen[-1][3] < cum_price_volume / cum_volume:
                first[RELEASES[3]] = (i - event_index) * 5
    return first


def _skip_proxy(bars: tuple[Candle, ...], event_idx: int,
                horizon_end: int) -> dict[str, bool | None]:
    prices = bars[event_idx:horizon_end + 1]
    if len(prices) < 49:  # only the fixed *first four hours* are examined
        return dict.fromkeys(SKIP_ARM_KEYS)
    peak = float(prices[0].high)
    trough = float(prices[0].close)
    hits: dict[str, bool | None] = dict.fromkeys(SKIP_ARM_KEYS, False)
    for bar in prices[1:]:
        if float(bar.high) > peak:
            peak, trough = float(bar.high), float(bar.close)
        else:
            trough = min(trough, float(bar.low))
        drawdown = (peak - trough) / peak * 100
        rebound = (float(bar.close) - trough) / trough * 100
        for d, r in SKIP_CELLS:
            if drawdown >= d and rebound >= r:
                hits[f"joint_{d}_{r}"] = True
        if drawdown >= 25 and rebound >= 8:
            hits["joint_25_8"] = True
        if drawdown >= 40:
            hits["unconditional_40"] = True
    return hits


def _continuation_long_new_high_proxy(
    bars: tuple[Candle, ...], event_idx: int, horizon_end: int,
) -> tuple[int | None, float | None]:
    """Kline-only R4 refutation arm: new high while A/B/D WAIT remains active."""

    peak = float(bars[event_idx].high)
    for i in range(event_idx + 1, horizon_end):
        high = float(bars[i].high)
        broke_high = high > peak
        peak = max(peak, high)
        if not broke_high or _wait_from_5m(bars, i) is not True:
            continue
        entry_index = i + 1
        entry = float(bars[entry_index].open)
        terminal = float(bars[horizon_end].close)
        return (i - event_idx) * 5, (terminal / entry - 1) * 10_000
    return None, None


def find_proxy_events(
    candles: tuple[Candle, ...], *, squeeze_adverse_pct: float =
    PRIMARY_SQUEEZE_ADVERSE_MARK_EXCURSION_PCT,
) -> list[ProxyEvent]:
    """Identify observable kline *prefilters* with full 24h after-event horizon."""

    found: list[ProxyEvent] = []
    quote_prefix = [0.0]
    gap_prefix = [0]
    for index, bar in enumerate(candles):
        quote_prefix.append(quote_prefix[-1] + float(bar.quote_volume))
        gap_prefix.append(gap_prefix[-1] + int(index > 0 and (
            bar.open_time_ms - candles[index - 1].open_time_ms != FIVE_MIN)))
    last_event = -10**30
    for i in range(288, len(candles)):
        if (candles[i].close_time_ms - last_event < 60 * 60_000
                or gap_prefix[i + 1] - gap_prefix[i - 288 + 1] > 0):
            continue
        move = float(candles[i].close / candles[i - 288].close - 1)
        volume = quote_prefix[i + 1] - quote_prefix[i - 288 + 1]
        if move < 0.30 or volume < 2_000_000:
            continue
        last_event = candles[i].close_time_ms
        event_id = hashlib.sha256(
            f"{POLICY_VERSION}|USD-M|{candles[i].symbol}|{last_event}".encode()
        ).hexdigest()
        end = min(i + HORIZON, len(candles) - 1)
        future = candles[i + 1:end + 1]
        horizon_continuous = (len(future) == HORIZON and
                              all(bar.open_time_ms == candles[i].open_time_ms
                                  + j * FIVE_MIN for j, bar in enumerate(future, 1)))
        squeeze20: bool | None = None
        if horizon_continuous:
            short_bps = (1 - float(future[-1].close / candles[i].close)) * 10_000
            squeeze = any(_squeeze_crossed(bar.high, candles[i].close, squeeze_adverse_pct)
                          for bar in future)
            squeeze20 = any(_squeeze_crossed(bar.high, candles[i].close, 20.0)
                            for bar in future)
            four_hour_bps = (1 - float(future[47].close / candles[i].close)) * 10_000
            squeeze4 = any(_squeeze_crossed(bar.high, candles[i].close, squeeze_adverse_pct)
                           for bar in future[:48])
            mfe24 = (1 - min(float(bar.low / candles[i].close) for bar in future)) * 100
            landmark_squeeze = any(_squeeze_crossed(
                bar.high, future[47].close, squeeze_adverse_pct) for bar in future[48:])
        else:
            short_bps = None
            squeeze = None
            four_hour_bps = None
            squeeze4 = None
            mfe24 = None
            landmark_squeeze = None
        # Strictly kline-only and retrospective; OI, cap, BBO, listing not inferred.
        releases: dict[str, int | None] = (_release_proxy(candles, i, end)
                                           if horizon_continuous else {
                                               str(r): None for r in RELEASES})
        proxy_released: dict[str, float | None] = {}
        release4: dict[str, float | None] = {}
        release4_squeeze: dict[str, bool | None] = {}
        release_squeeze: dict[str, bool | None] = {}
        for rule in RELEASES:
            minute = releases[rule]
            if not horizon_continuous:
                release4[rule] = None
                release4_squeeze[rule] = None
                proxy_released[rule] = None
                release_squeeze[rule] = None
            elif minute is None:
                release4[rule] = None
                release4_squeeze[rule] = None
                proxy_released[rule] = 0.0  # abstention on original opportunity
                release_squeeze[rule] = False
            else:
                next_entry = i + minute // 5 + 1
                if next_entry + 47 <= end:
                    close4 = candles[next_entry + 47].close
                    entry_open = candles[next_entry].open
                    release4[rule] = float((1 - close4 / entry_open) * 10_000)
                    release4_squeeze[rule] = any(_squeeze_crossed(
                        bar.high, entry_open, squeeze_adverse_pct
                    ) for bar in candles[next_entry:next_entry + 48])
                else:
                    release4[rule] = None
                    release4_squeeze[rule] = None
                proxy_released[rule] = (
                    (1 - float(candles[end].close / candles[next_entry].open)) * 10_000
                    if next_entry <= end else 0.0
                )
                release_squeeze[rule] = (
                    any(_squeeze_crossed(bar.high, candles[next_entry].open,
                                         squeeze_adverse_pct)
                        for bar in candles[next_entry:end + 1])
                    if next_entry <= end else False
                )
        skips = (_skip_proxy(candles, i, i + 48) if horizon_continuous
                 else dict.fromkeys(SKIP_ARM_KEYS))
        continuation_minute, continuation_return = (
            _continuation_long_new_high_proxy(candles, i, end)
            if horizon_continuous else (None, None)
        )
        found.append(ProxyEvent(
            event_id, candles[i].symbol, last_event,
            datetime.fromtimestamp(last_event / 1_000, tz=UTC).date().isoformat(),
            volume, move * 100, len(future), short_bps, squeeze,
            four_hour_bps, squeeze4, mfe24, landmark_squeeze,
            releases, release4, release4_squeeze, proxy_released,
            release_squeeze, skips,
            continuation_minute, continuation_return,
            squeeze20,
        ))
    return found


def day_block_interval(
    values: tuple[tuple[str, float], ...], *, seed: int = 20261007,
    replicates: int = 10_000,
) -> dict[str, float | int | None]:
    """Cluster-bootstrap parent events by UTC start day, preserving symbol dependence."""

    daily: dict[str, list[float]] = defaultdict(list)
    for day, value in values:
        daily[day].append(value)
    if len(daily) < 2:
        return {"clusters": len(daily), "replicates": 0, "lower_95": None,
                "upper_95": None}
    day_items = tuple(daily.values())
    rng = random.Random(seed)
    samples = []
    for _ in range(replicates):
        selected = [day_items[rng.randrange(len(day_items))]
                    for _ in range(len(day_items))]
        flattened = [x for day in selected for x in day]
        samples.append(sum(flattened) / len(flattened))
    samples.sort()
    return {"clusters": len(day_items), "replicates": replicates,
            "lower_95": samples[int(replicates * 0.025)],
            "upper_95": samples[min(replicates - 1, int(replicates * 0.975))]}


def _completed_return(row: ProxyEvent) -> float:
    if row.baseline_24h_short_return_bps is None:
        raise ValueError("incomplete opportunities cannot enter completed-return statistics")
    return row.baseline_24h_short_return_bps


def parent_release_accounting(
    *, release_return_bps: float | None, favorable_excursion_pct: float | None,
    missed_fade_threshold_pct: float = 10.0,
) -> dict[str, bool | float | None | str]:
    """Keep abstention, opportunity value, and realized trade outcome distinct."""

    if missed_fade_threshold_pct <= 0:
        raise ValueError("missed-fade threshold must be positive")
    traded = release_return_bps is not None
    return {
        "parent_in_denominator": True,
        "trade_created": traded,
        "trade_return_bps": release_return_bps,
        "missed_fade_indicator": (
            not traded and favorable_excursion_pct is not None
            and favorable_excursion_pct >= missed_fade_threshold_pct
        ),
        "net_trade_pnl": release_return_bps if traded else "NOT_APPLICABLE",
    }


def holm_adjust(raw_p_values: dict[str, float]) -> dict[str, float]:
    """Family-wise Holm step-down p-adjustment; no p-values invented from proxies."""

    ordered = sorted(raw_p_values.items(), key=lambda pair: pair[1])
    adjusted: dict[str, float] = {}
    previous = 0.0
    for index, (name, p) in enumerate(ordered):
        if not 0 <= p <= 1:
            raise ValueError("raw p-values must lie in [0,1]")
        previous = max(previous, min(1.0, p * (len(ordered) - index)))
        adjusted[name] = previous
    return adjusted


def run_public_kline_feasibility(
    data_dir: Path, *, policy_file: Path, output_dir: Path,
    execution_paths: tuple[ParentPath, ...] | None = None,
) -> dict[str, Any]:
    """Validate hashed 5m USD-M source files and write honest offline proxy results."""

    if not policy_file.is_file():
        raise FileNotFoundError("frozen policy is required")
    config = json.loads(policy_file.read_text(encoding="utf-8"))
    if config.get("policy_version") != POLICY_VERSION:
        raise ValueError("policy version differs from frozen code")
    inputs = sorted(data_dir.glob("*__5m.csv.gz"))
    results: list[ProxyEvent] = []
    source_files: list[dict[str, object]] = []
    failures: list[str] = []
    for file in inputs:
        manifest_path = file.with_name(file.name + ".manifest.json")
        if not manifest_path.is_file():
            failures.append(f"{file.name}: no source hash manifest")
            continue
        manifest = read_dataset_manifest(manifest_path)
        verify_dataset_manifest(file, manifest)
        dataset = read_kline_csv(file)
        if dataset.request.interval != "5m" or dataset.request.market.value != "futures":
            continue
        threshold = float(config["evaluation"]["squeeze_adverse_mark_excursion_pct"])
        if threshold != PRIMARY_SQUEEZE_ADVERSE_MARK_EXCURSION_PCT:
            raise ValueError("frozen policy squeeze threshold differs from active v2c code")
        subset = find_proxy_events(dataset.candles, squeeze_adverse_pct=threshold)
        results.extend(subset)
        source_files.append({"name": file.name, "sha256": sha256_file(file),
                             "rows": len(dataset.candles), "proxy_events": len(subset),
                             "start_ms": dataset.candles[0].close_time_ms,
                             "end_ms": dataset.candles[-1].close_time_ms})
    results.sort(key=lambda row: (row.event_ms, row.symbol))
    complete = [row for row in results if row.baseline_24h_short_return_bps is not None]
    release_counts = {key: sum(row.first_release_minutes[key] is not None for row in complete)
                      for key in RELEASES}
    rates = {key: sum(row.skip_cells[key] is True for row in complete)
             for key in SKIP_ARM_KEYS}
    immediate_squeeze_n = sum(row.kline_proxy_24h_squeeze is True for row in complete)
    immediate_squeeze_rate = immediate_squeeze_n / len(complete) if complete else None
    beneficial_fade_n = sum(
        row.kline_proxy_24h_mfe_pct is not None and row.kline_proxy_24h_mfe_pct >= 10
        for row in complete
    )
    paired_releases = {}
    for release in RELEASES:
        release_minutes = [
            minute for row in complete
            if (minute := row.first_release_minutes[release]) is not None
        ]
        diffs = tuple((row.event_day_utc,
                       float(row.release_24h_proxy_short_return_bps[release] or 0.0)
                       - _completed_return(row)) for row in complete)
        paired_releases[release] = {
            "original_parent_denominator_n": len(complete),
            "per_parent_mean_difference_to_immediate_bps": (
                sum(value for _, value in diffs) / len(diffs) if diffs else None),
            "4h_release_observed_n": sum(
                row.release_4h_proxy_short_return_bps[release] is not None
                for row in complete
            ),
            "4h_release_squeeze_rate_among_observed": (
                sum(row.release_4h_proxy_squeeze[release] is True for row in complete)
                / sum(row.release_4h_proxy_squeeze[release] is not None for row in complete)
                if any(row.release_4h_proxy_squeeze[release] is not None for row in complete)
                else None
            ),
            "day_block_95": day_block_interval(diffs),
            "no_release_or_expired_n": sum(row.first_release_minutes[release] is None
                                           for row in complete),
            "abstention_is_not_trade_pnl": True,
            "abstention_policy_payoff_assumption": (
                "zero capital allocation; missed-fade opportunity reported separately"
            ),
            "missed_fade_10pct_n": sum(
                row.first_release_minutes[release] is None
                and row.kline_proxy_24h_mfe_pct is not None
                and row.kline_proxy_24h_mfe_pct >= 10 for row in complete),
            "missed_fade_10pct_denominator_n": beneficial_fade_n,
            "missed_fade_10pct_fraction": (
                sum(row.first_release_minutes[release] is None
                    and row.kline_proxy_24h_mfe_pct is not None
                    and row.kline_proxy_24h_mfe_pct >= 10 for row in complete)
                / beneficial_fade_n if beneficial_fade_n else None
            ),
            "release_n": len(release_minutes),
            "median_release_minutes": (median(release_minutes)
                                       if release_minutes else None),
            "immediate_proxy_squeeze_rate": immediate_squeeze_rate,
            "release_proxy_squeeze_rate_per_parent": (
                sum(row.release_24h_proxy_squeeze[release] is True for row in complete)
                / len(complete) if complete else None
            ),
            "release_proxy_squeeze_rate_ratio": (
                (sum(row.release_24h_proxy_squeeze[release] is True for row in complete)
                 / len(complete)) / immediate_squeeze_rate
                if complete and immediate_squeeze_rate else None
            ),
        }
    r3_landmark_assoc: dict[str, dict[str, int | float | None]] = {}
    for key in SKIP_ARM_KEYS:
        skipped = [row for row in complete if row.skip_cells[key] is True]
        kept = [row for row in complete if row.skip_cells[key] is False]
        skipped_squeeze = sum(row.post_4h_landmark_20h_kline_squeeze is True
                             for row in skipped)
        kept_squeeze = sum(row.post_4h_landmark_20h_kline_squeeze is True
                          for row in kept)
        skip_rate = skipped_squeeze / len(skipped) if skipped else None
        keep_rate = kept_squeeze / len(kept) if kept else None
        r3_landmark_assoc[key] = {
            "skipped_n": len(skipped), "kept_n": len(kept),
            "skipped_squeeze_n": skipped_squeeze, "kept_squeeze_n": kept_squeeze,
            "post_landmark_proxy_risk_ratio": (skip_rate / keep_rate
                if skip_rate is not None and keep_rate is not None and keep_rate > 0
                else None),
        }
    continuation_rows = [
        row for row in complete
        if row.continuation_long_new_high_wait_return_bps is not None
    ]
    continuation_returns = [
        value for row in continuation_rows
        if (value := row.continuation_long_new_high_wait_return_bps) is not None
    ]
    continuation_parent_values = tuple(
        (row.event_day_utc, float(row.continuation_long_new_high_wait_return_bps or 0.0))
        for row in complete
    )
    bootstrap = day_block_interval(tuple((row.event_day_utc,
                                        _completed_return(row))
                                         for row in complete))
    execution_summary: dict[str, Any]
    if execution_paths is None:
        execution_summary = {
            "status": "UNAVAILABLE_NO_EXECUTABLE_PATH_DATA",
            "arms": {},
        }
    else:
        execution_rows = compare_parent_paths(execution_paths)
        execution_summary = {
            "status": "EXECUTABLE_SCENARIO_COMPARISON_NOT_HISTORICAL_EFFICACY",
            "parent_ids": [parent.parent_id for parent in execution_paths],
            "arms": summarize_arm_results(execution_rows),
        }
    output: dict[str, Any] = {
        "schema_version": "pump_fade_v2_offline_kline_feasibility_v1",
        "policy_version": POLICY_VERSION,
        "primary_squeeze_adverse_mark_excursion_pct": config["evaluation"][
            "squeeze_adverse_mark_excursion_pct"],
        "squeeze_20pct_sensitivity": "reported per parent in proxy_events; not primary",
        "evidence_class": "RETROSPECTIVE_EXPOSED_KLINE_PROXY_NOT_PIT_ELIGIBLE",
        "status": "DATA_INCOMPLETE_FOR_OFFICIAL_R1_R2_R3_R4_GATES",
        "policy_sha256": sha256_file(policy_file),
        "source_files": source_files,
        "unverified_source_manifests": failures,
        "prefilter_events_n": len(results),
        "complete_24h_kline_proxy_events_n": len(complete),
        "utc_day_clusters_n": len({row.event_day_utc for row in complete}),
        "symbol_counts": dict(Counter(row.symbol for row in results)),
        "releases_with_24h_proxy_coverage_n": release_counts,
        "r1_r2_r4_24h_kline_proxy_paired_parent_comparisons": paired_releases,
        "r3_skip_joint_cell_proxy_hits": rates,
        "r3_four_hour_landmark_post_twenty_hour_proxy_squeeze": r3_landmark_assoc,
        "r3_estimand": (
            "four-hour landmark membership and disjoint twenty-hour follow-up association; "
            "does not identify causal skip benefit"
        ),
        "historical_funding_cap_change_chronology": (
            "UNAVAILABLE; absent fundingInfo rows and current snapshots are not backfilled"
        ),
        "r4_continuation_long_new_high_wait_kline_proxy": {
            "signal_n": len(continuation_rows),
            "no_signal_n": len(complete) - len(continuation_rows),
            "conditional_mean_return_bps": (
                sum(continuation_returns) / len(continuation_returns)
                if continuation_returns else None
            ),
            "per_parent_mean_return_bps_with_abstention_zero": (
                sum(value for _, value in continuation_parent_values) / len(complete)
                if complete else None
            ),
            "day_block_95_per_parent": day_block_interval(continuation_parent_values),
            "execution_cost_and_full_wait_inputs": "UNAVAILABLE",
        },
        "baseline_immediate_24h_kline_return_bps": {
            "mean": (sum(_completed_return(x) for x in complete)
                     / len(complete)) if complete else None,
            "day_block_bootstrap_95": bootstrap,
            "round_trip_cost_scenario_bps": list(
                config["evaluation"]["cost_scenarios_round_trip_bps"]),
            "execution_and_actual_funding": "UNAVAILABLE",
        },
        "r2_executable_scenario_comparison": execution_summary,
        "registered_arm_status": {
            "P1_prior_pump_validation": (
                "EXPOSED_PRIOR_RESULTS; independent clean replication input is not in this package"
            ),
            "R1": {release: "KLINE_PROXY_ONLY_INELIGIBLE_FOR_CONFIRMATION"
                   if release_counts[release] else "NO_OBSERVED_PROXY_RELEASE"
                   for release in RELEASES},
            "R2_primary_ladder": execution_summary["status"],
            "R2_first_fill_only": execution_summary["status"],
            "R2_eight_equal_additions_5pct_from_last_fill": (
                "EXECUTABLE_COMPARATOR_ONLY_NOT_POLICY" if execution_paths
                else execution_summary["status"]
            ),
            "R2_three_addition_sensitivity": (
                "EXECUTABLE_SENSITIVITY_ONLY_NOT_POLICY" if execution_paths
                else execution_summary["status"]
            ),
            "R2_q99_loss_reduction": (
                "UNAVAILABLE_INSUFFICIENT_OBSERVED_TAIL; bootstrap draws do not create tail "
                "observations; see per-arm q99_status"
            ),
            "R3_nine_joint_skip_cells": "KLINE_PROXY_ONLY_HISTORICAL_POINT_IN_TIME_COHORT_MISSING",
            "R3_25_8_sensitivity": "KLINE_PROXY_ONLY_EXPOSED_DEVELOPMENT",
            "R3_unconditional_40_sensitivity": "KLINE_PROXY_ONLY_EXPOSED_DEVELOPMENT",
            "R4_long_refutation": "KLINE_NEW_HIGH_WAIT_PROXY_ONLY_EXECUTION_UNAVAILABLE",
            "P2_post_crash": "KLINE_PROXY_ONLY_EXPOSED_DEVELOPMENT",
            "P3_private_ladder": "NOT_EVALUATED_BY_PUBLIC_KLINE_RUNNER",
        },
        "official_holm_p_values": None,
        "confirmed_positive_expectancy": False,
        "notes": [
            "Quote-volume and closed kline close references support retrospective prescreen only.",
            "Unknown historical listing/metadata/OI/forced-liquidation/BBO/mark receipt times "
            "block full policy assessment.",
            "All R3 association comparisons use a four-hour landmark and disjoint future "
            "twenty-hour follow-up; neither part supplies causal treatment effects.",
            "R1/R2/R4 proxy entry uses the next 5m bar open; abstentions remain zero "
            "on the same parent population; values have no executable fill proof.",
            "Proxy 24h price differences omit funding, spread and slippage and are not net "
            "tradable returns.",
            "No synthetic observations are counted as strategy evidence.",
            "P1 historical results previously opened; all events here are development evidence.",
        ],
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "results.json").write_text(json.dumps(output, indent=2, sort_keys=True)
                                              + "\n", encoding="utf-8")
    with (output_dir / "proxy_events.jsonl").open("w", encoding="utf-8") as handle:
        for row in results:
            handle.write(json.dumps(asdict(row), sort_keys=True) + "\n")
    (output_dir / "data_manifest.json").write_text(json.dumps({
        "policy_sha256": output["policy_sha256"],
        "data_files": source_files,
        "result_sha256": sha256_file(output_dir / "results.json"),
        "proxy_event_sha256": sha256_file(output_dir / "proxy_events.jsonl"),
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output


def main(argv: Sequence[str] | None = None) -> None:
    """CLI with explicit local input and output paths, no sockets or private reads."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    output = run_public_kline_feasibility(args.data_dir, policy_file=args.policy,
                                          output_dir=args.output_dir)
    print(json.dumps({k: output[k] for k in ("evidence_class", "prefilter_events_n",
                                               "complete_24h_kline_proxy_events_n",
                                               "utc_day_clusters_n", "status")}, indent=2))


if __name__ == "__main__":
    main()
