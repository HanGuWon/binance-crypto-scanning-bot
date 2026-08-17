from __future__ import annotations

import hashlib
import math
import random
import statistics
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any

from signalbot.backtest.config import BacktestSpec
from signalbot.backtest.engine import Trade, calculate_execution_returns
from signalbot.domain.enums import Direction
from signalbot.domain.models import Candle

_R3_RAW_REPLAY_PROTOCOL = "r3_exposed_kline_proxy_diagnostic_v1"
_R3_RAW_ARTIFACT_ROLE = "RAW_REPLAY_INPUT_NOT_FINAL_R3_ANALYSIS"
RUN_MANIFEST_OUTPUTS = (
    "trades.csv",
    "results.json",
    "report.md",
    "opportunities.csv",
)


@dataclass(frozen=True, slots=True)
class MetricRow:
    group: dict[str, str]
    trades: int
    at_least_30_trades: bool
    gross_pnl_usdt: float
    net_pnl_usdt: float
    fees_usdt: float
    slippage_usdt: float
    funding_pnl_usdt: float
    no_funding_net_pnl_usdt: float
    mean_net_return: float
    median_net_return: float
    mean_ci_low: float | None
    mean_ci_high: float | None
    gross_mean_return: float
    gross_mean_ci_low: float | None
    gross_mean_ci_high: float | None
    win_rate: float
    profit_factor: float | None
    average_bars_held: float
    average_mfe: float
    average_mae: float
    two_x_slippage_net_pnl_usdt: float
    zero_slippage_net_pnl_usdt: float
    latency_5bps_net_pnl_usdt: float
    latency_10bps_net_pnl_usdt: float


@dataclass(frozen=True, slots=True)
class SleevePortfolio:
    market: str
    split: str
    assets: int
    trades: int
    equal_weight_return: float
    scaled_profit_on_10k_usdt: float
    realized_max_drawdown: float


@dataclass(frozen=True, slots=True)
class BuyHoldBaseline:
    asset: str
    cohort: str
    split: str
    entry_time_ms: int
    exit_time_ms: int
    gross_return: float
    net_return: float


def _block_bootstrap_mean(
    trades: list[Trade],
    *,
    samples: int,
    block_days: int,
    seed: int,
    return_field: str = "net_return",
) -> tuple[float | None, float | None]:
    if len(trades) < 2:
        return None, None
    block_ms = block_days * 86_400_000
    grouped: dict[int, list[float]] = defaultdict(list)
    origin = min(item.exit_time_ms for item in trades)
    for trade in trades:
        grouped[(trade.exit_time_ms - origin) // block_ms].append(
            float(getattr(trade, return_field))
        )
    blocks = [grouped[key] for key in sorted(grouped)]
    rng = random.Random(seed)
    means = []
    for _ in range(samples):
        draw = [rng.choice(blocks) for _ in blocks]
        values = [value for block in draw for value in block]
        if values:
            means.append(statistics.fmean(values))
    if not means:
        return None, None
    means.sort()
    low_index = max(0, math.floor(0.025 * (len(means) - 1)))
    high_index = min(len(means) - 1, math.ceil(0.975 * (len(means) - 1)))
    return means[low_index], means[high_index]


def metric_row(
    trades: list[Trade], group: dict[str, str], spec: BacktestSpec
) -> MetricRow:
    returns = [item.net_return for item in trades]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    profit_factor = sum(wins) / abs(sum(losses)) if losses else None
    seed_material = "|".join(f"{key}={value}" for key, value in sorted(group.items()))
    seed_offset = int(hashlib.sha256(seed_material.encode()).hexdigest()[:8], 16)
    ci_low, ci_high = _block_bootstrap_mean(
        trades,
        samples=spec.bootstrap.samples,
        block_days=spec.bootstrap.block_days,
        seed=spec.bootstrap.seed + seed_offset,
    )
    gross_ci_low, gross_ci_high = _block_bootstrap_mean(
        trades,
        samples=spec.bootstrap.samples,
        block_days=spec.bootstrap.block_days,
        seed=spec.bootstrap.seed + seed_offset + 1,
        return_field="gross_return",
    )
    return MetricRow(
        group=group,
        trades=len(trades),
        at_least_30_trades=len(trades) >= 30,
        gross_pnl_usdt=sum(item.gross_pnl_usdt for item in trades),
        net_pnl_usdt=sum(item.net_pnl_usdt for item in trades),
        fees_usdt=sum(item.fees_usdt for item in trades),
        slippage_usdt=sum(item.slippage_usdt for item in trades),
        funding_pnl_usdt=sum(item.funding_pnl_usdt for item in trades),
        no_funding_net_pnl_usdt=sum(
            item.net_pnl_usdt - item.funding_pnl_usdt for item in trades
        ),
        mean_net_return=statistics.fmean(returns) if returns else 0.0,
        median_net_return=statistics.median(returns) if returns else 0.0,
        mean_ci_low=ci_low,
        mean_ci_high=ci_high,
        gross_mean_return=(
            statistics.fmean(item.gross_return for item in trades) if trades else 0.0
        ),
        gross_mean_ci_low=gross_ci_low,
        gross_mean_ci_high=gross_ci_high,
        win_rate=len(wins) / len(trades) if trades else 0.0,
        profit_factor=profit_factor,
        average_bars_held=(
            statistics.fmean(item.bars_held for item in trades) if trades else 0.0
        ),
        average_mfe=statistics.fmean(item.mfe for item in trades) if trades else 0.0,
        average_mae=statistics.fmean(item.mae for item in trades) if trades else 0.0,
        two_x_slippage_net_pnl_usdt=sum(
            item.net_pnl_usdt - item.slippage_usdt for item in trades
        ),
        zero_slippage_net_pnl_usdt=sum(
            item.net_pnl_usdt + item.slippage_usdt for item in trades
        ),
        latency_5bps_net_pnl_usdt=sum(
            item.net_pnl_usdt - spec.costs.notional_usdt * 5 / 10_000
            for item in trades
        ),
        latency_10bps_net_pnl_usdt=sum(
            item.net_pnl_usdt - spec.costs.notional_usdt * 10 / 10_000
            for item in trades
        ),
    )


def group_metrics(
    trades: list[Trade], fields: tuple[str, ...], spec: BacktestSpec
) -> list[MetricRow]:
    grouped: dict[tuple[str, ...], list[Trade]] = defaultdict(list)
    for trade in trades:
        grouped[tuple(str(getattr(trade, field)) for field in fields)].append(trade)
    return [
        metric_row(values, dict(zip(fields, key, strict=True)), spec)
        for key, values in sorted(grouped.items())
    ]


def sleeve_portfolios(trades: list[Trade]) -> list[SleevePortfolio]:
    grouped: dict[tuple[str, str], list[Trade]] = defaultdict(list)
    for trade in trades:
        grouped[(trade.market, trade.split)].append(trade)
    rows = []
    for (market, split), values in sorted(grouped.items()):
        assets = sorted({item.asset for item in values})
        equities = {asset: 1.0 for asset in assets}
        peak = 1.0
        max_drawdown = 0.0
        for trade in sorted(values, key=lambda item: (item.exit_time_ms, item.trade_id)):
            equities[trade.asset] *= max(0.0, 1 + trade.net_return)
            portfolio = statistics.fmean(equities.values())
            peak = max(peak, portfolio)
            max_drawdown = min(max_drawdown, portfolio / peak - 1)
        portfolio_return = statistics.fmean(equities.values()) - 1
        rows.append(
            SleevePortfolio(
                market=market,
                split=split,
                assets=len(assets),
                trades=len(values),
                equal_weight_return=portfolio_return,
                scaled_profit_on_10k_usdt=portfolio_return * 10_000,
                realized_max_drawdown=max_drawdown,
            )
        )
    return rows


def buy_hold_baselines(
    candles_by_asset: dict[str, list[Candle]],
    eligible_from_ms: dict[str, int],
    spec: BacktestSpec,
) -> list[BuyHoldBaseline]:
    assets = {item.asset: item for item in spec.assets}
    rows = []
    for split in spec.splits:
        split_start = int(split.start.timestamp() * 1000)
        split_end = int(split.end.timestamp() * 1000)
        for asset_name, candles in sorted(candles_by_asset.items()):
            asset = assets[asset_name]
            start = max(split_start, eligible_from_ms[asset_name])
            eligible = [
                candle
                for candle in candles
                if candle.open_time_ms >= start and candle.close_time_ms < split_end
            ]
            if not eligible:
                continue
            entry = float(eligible[0].open)
            exit_price = float(eligible[-1].close)
            execution = calculate_execution_returns(
                Direction.LONG,
                entry,
                exit_price,
                spec.costs.spot_fee_bps,
                spec.costs.spot_slippage_bps[asset.cohort],
            )
            rows.append(
                BuyHoldBaseline(
                    asset=asset_name,
                    cohort=asset.cohort,
                    split=split.name,
                    entry_time_ms=eligible[0].open_time_ms,
                    exit_time_ms=eligible[-1].close_time_ms,
                    gross_return=execution.gross_return,
                    net_return=execution.net_before_funding,
                )
            )
    return rows


def aggregate_buy_hold(rows: list[BuyHoldBaseline]) -> list[dict[str, Any]]:
    grouped: dict[str, list[BuyHoldBaseline]] = defaultdict(list)
    for row in rows:
        grouped[row.split].append(row)
    return [
        {
            "split": split,
            "assets": len(values),
            "equal_weight_gross_return": statistics.fmean(
                item.gross_return for item in values
            ),
            "equal_weight_net_return": statistics.fmean(item.net_return for item in values),
        }
        for split, values in sorted(grouped.items())
    ]


def build_results(trades: list[Trade], spec: BacktestSpec) -> dict[str, Any]:
    groupings = {
        "direction": ("market", "direction"),
        "split": ("market", "direction", "split"),
        "symbol": ("market", "direction", "asset"),
        "family": ("market", "direction", "family"),
        "cohort": ("market", "direction", "cohort"),
        "exit_reason": ("market", "direction", "exit_reason"),
    }
    results: dict[str, Any] = {
        "protocol_version": spec.protocol_version,
        "interval": spec.interval,
        "data_start": spec.data_start.isoformat(),
        "evaluation_start": spec.evaluation_start.isoformat(),
        "evaluation_end": spec.evaluation_end.isoformat(),
        "trade_count": len(trades),
        "groups": {
            name: [asdict(row) for row in group_metrics(trades, fields, spec)]
            for name, fields in groupings.items()
        },
        "sleeve_portfolios": [asdict(row) for row in sleeve_portfolios(trades)],
        "block_bootstrap_sensitivity": _block_bootstrap_sensitivity(trades, spec),
    }
    if spec.protocol_version == _R3_RAW_REPLAY_PROTOCOL:
        results.update(
            {
                "artifact_role": _R3_RAW_ARTIFACT_ROLE,
                "r3_raw_replay_contract": {
                    "sequential_t72_ledger": {
                        "independent_episodes": False,
                        "analysis_role": "SECONDARY_NON_PRIMARY",
                    },
                    "legacy_bootstrap": {
                        "is_frozen_r3_shared_utc_day_mbb": False,
                        "analysis_role": "DESCRIPTIVE_NOT_FINAL_R3_INFERENCE",
                    },
                    "r2_c0_t72_status": "PROTOCOL_MISMATCH",
                    "final_r3_analysis": {
                        "source": "SEPARATE_OPPORTUNITY_BASED_R3_ANALYZER",
                        "provides_primary_efficacy": True,
                        "provides_status_axes": True,
                    },
                },
            }
        )
    return results


def _block_bootstrap_sensitivity(
    trades: list[Trade], spec: BacktestSpec
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Trade]] = defaultdict(list)
    for trade in trades:
        grouped[(trade.market, trade.direction)].append(trade)
    rows: list[dict[str, Any]] = []
    for (market, direction), values in sorted(grouped.items()):
        for block_days in (7, 14, 28):
            block_ms = block_days * 86_400_000
            origin = min(item.exit_time_ms for item in values)
            block_count = len(
                {(item.exit_time_ms - origin) // block_ms for item in values}
            )
            low, high = _block_bootstrap_mean(
                values,
                samples=spec.bootstrap.samples,
                block_days=block_days,
                seed=spec.bootstrap.seed + block_days,
            )
            rows.append(
                {
                    "market": market,
                    "direction": direction,
                    "block_days": block_days,
                    "blocks": block_count,
                    "mean_net_return": statistics.fmean(
                        item.net_return for item in values
                    ),
                    "ci_low": low,
                    "ci_high": high,
                }
            )
    return rows


def pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.2f}%"


def render_report(
    results: dict[str, Any],
    spec: BacktestSpec,
    *,
    spec_label: str = "in-memory BacktestSpec",
) -> str:
    is_r3_raw_replay = spec.protocol_version == _R3_RAW_REPLAY_PROTOCOL
    manifest_output_labels = ", ".join(
        f"`{name}`" for name in RUN_MANIFEST_OUTPUTS
    )
    flow_boundary = (
        f"- Results use Binance {spec.interval} klines; participation features are "
        "disabled in this ablation."
        if not spec.gate_use_participation
        else f"- Results use Binance {spec.interval} klines and a kline-wide taker-flow proxy."
    )
    context_boundary = (
        "- 15m/1h context is gap-safe and strictly lagged; the live scanner's "
        "last-minute trade-flow path is still not byte-identical."
        if spec.interval == "5m"
        and spec.strategy_mode == "gate_v2"
        and spec.gate_use_higher_timeframes
        else "- Higher-timeframe confirmation is disabled in this ablation."
        if spec.interval == "5m" and spec.strategy_mode == "gate_v2"
        else "- This research path is not byte-identical to live microstructure."
    )
    headline = (
        "## Secondary sequential T72 ledger (non-independent; non-primary)"
        if is_r3_raw_replay
        else "## Headline: upward buys versus downward sells"
    )
    lines = [
        "# Binance technical-signal backtest",
        "",
        f"Protocol: `{spec.protocol_version}`  ",
        f"Rule version: `{spec.rule_version}`  ",
        f"Interval: `{spec.interval}`  ",
        (
            f"Evaluation: `{spec.evaluation_start.isoformat()}` to "
            f"`{spec.evaluation_end.isoformat()}` (end exclusive)  "
        ),
        f"Research spec: `{spec_label}`  ",
        "Status: **COMPUTED; see verification and limitations below**",
        "",
    ]
    if is_r3_raw_replay:
        lines.extend(
            [
                "> **R3 RAW REPLAY INPUT — NOT FINAL R3 ANALYSIS**",
                ">",
                f"> Artifact role: `{_R3_RAW_ARTIFACT_ROLE}`. The sequential T72 ledger",
                "> below is non-independent, secondary, and non-primary. Its generic legacy",
                "> bootstrap is not the frozen R3 shared UTC-day moving-block bootstrap.",
                "> Primary efficacy and final status axes come only from the separate",
                "> opportunity-based R3 analyzer.",
                "",
            ]
        )
    lines.extend(
        [
        headline,
        "",
        "All dollar figures in trade tables use the frozen "
        f"{spec.costs.notional_usdt:g} USDT notional per trade.",
        "The portfolio view independently compounds equal-weight symbol sleeves and scales",
        "the percentage result to a 10,000 USDT illustration; it is not a live account claim.",
        "",
        (
            "| Side | Trades | Gross | Fees | Slippage | Funding | Net | "
            "No-funding net | 0x-slip | 2x-slip | +5bp latency | +10bp latency |"
        ),
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for raw in results["groups"]["direction"]:
        group = raw["group"]
        label = "Spot upward buy" if group["market"] == "spot" else "Futures downward sell"
        lines.append(
            f"| {label} | {raw['trades']} | {raw['gross_pnl_usdt']:.2f} | "
            f"-{raw['fees_usdt']:.2f} | -{raw['slippage_usdt']:.2f} | "
            f"{raw['funding_pnl_usdt']:.2f} | {raw['net_pnl_usdt']:.2f} USDT | "
            f"{raw['no_funding_net_pnl_usdt']:.2f} | "
            f"{raw['zero_slippage_net_pnl_usdt']:.2f} | "
            f"{raw['two_x_slippage_net_pnl_usdt']:.2f} | "
            f"{raw['latency_5bps_net_pnl_usdt']:.2f} | "
            f"{raw['latency_10bps_net_pnl_usdt']:.2f} USDT |"
        )

    lines.extend(
        [
            "",
            *(
                [
                    "**R3 warning:** the intervals in this raw replay are legacy generic",
                    "per-ledger fixed-block summaries. They are descriptive only and are not",
                    "the frozen shared UTC-calendar-day MBB used by the final R3 analyzer.",
                    "",
                ]
                if is_r3_raw_replay
                else []
            ),
            "The intervals below estimate historical mean return per "
            f"{spec.costs.notional_usdt:g}-USDT trade using",
            f"{spec.bootstrap.block_days}-day fixed calendar-block clusters. "
            "They are not account-return "
            "intervals or future guarantees.",
            "",
            "| Side | Gross expectancy (95% CI) | Net expectancy (95% CI) | Win rate | PF |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for raw in results["groups"]["direction"]:
        group = raw["group"]
        label = "Spot upward buy" if group["market"] == "spot" else "Futures downward sell"
        pf = "n/a" if raw["profit_factor"] is None else f"{raw['profit_factor']:.2f}"
        gross_ci = f"{pct(raw['gross_mean_ci_low'])} to {pct(raw['gross_mean_ci_high'])}"
        net_ci = f"{pct(raw['mean_ci_low'])} to {pct(raw['mean_ci_high'])}"
        lines.append(
            f"| {label} | {pct(raw['gross_mean_return'])} ({gross_ci}) | "
            f"{pct(raw['mean_net_return'])} ({net_ci}) | {pct(raw['win_rate'])} | {pf} |"
        )

    lines.extend(
        [
            "",
            "### Dependence sensitivity",
            "",
            "The same mean is re-bootstrapped with longer non-overlapping fixed blocks;",
            "the block count, not the trade count, is the effective resampling unit.",
            "Widening or sign",
            "changes indicate sensitivity to serial and cross-market dependence.",
            "",
            "| Side | Block | Clusters | Mean net | 95% CI |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in results["block_bootstrap_sensitivity"]:
        label = (
            "Spot upward buy"
            if row["market"] == "spot"
            else "Futures downward sell"
        )
        lines.append(
            f"| {label} | {row['block_days']}d | {row['blocks']} | "
            f"{pct(row['mean_net_return'])} | "
            f"{pct(row['ci_low'])} to {pct(row['ci_high'])} |"
        )

    lines.extend(
        [
            "",
            "## Signal funnel and capital footprint",
            "",
            "Candidate counts are raw family setups before independent gates. Time in market",
            "is exposure bars divided by evaluated symbol-bars, not account utilization.",
            "",
            (
                "| Market | Symbol-bars | Candidates | Candidate rate | Confirmed | Scheduled | "
                "Trades | Trades/day | Time in market | Max concurrent | Peak notional |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in results.get("signal_funnel", []):
        lines.append(
            f"| {row['market']} | {row['evaluated_symbol_bars']} | "
            f"{row['candidate_setups']} | {pct(row['candidate_rate'])} | "
            f"{row['confirmed_state_transitions']} | {row['scheduled_entries']} | "
            f"{row['trades']} | {row['trades_per_day']:.2f} | "
            f"{pct(row['symbol_time_in_market'])} | "
            f"{row['maximum_concurrent_positions']} | "
            f"{row['required_notional_at_peak_usdt']:.2f} USDT |"
        )

    lines.extend(
        [
            "",
            "## Chronological splits",
            "",
            "| Side | Split | Trades | Net P&L | Expectancy | CI | Win rate | PF | n>=30 |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for raw in results["groups"]["split"]:
        group = raw["group"]
        pf = "n/a" if raw["profit_factor"] is None else f"{raw['profit_factor']:.2f}"
        lines.append(
            f"| {group['direction']} | {group['split']} | {raw['trades']} | "
            f"{raw['net_pnl_usdt']:.2f} | {pct(raw['mean_net_return'])} | "
            f"{pct(raw['mean_ci_low'])}..{pct(raw['mean_ci_high'])} | "
            f"{pct(raw['win_rate'])} | {pf} | {raw['at_least_30_trades']} |"
        )

    lines.extend(
        [
            "",
            "## Equal-weight symbol-sleeve portfolio illustration",
            "",
            "This secondary view assumes each symbol sleeve is fully reinvested; it is not the",
            "frozen fixed-100-USDT trade ledger and is not an account-return forecast.",
            "",
            "| Market | Split | Assets | Trades | Return | Profit on 10k | Realized max DD |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in results["sleeve_portfolios"]:
        lines.append(
            f"| {row['market']} | {row['split']} | {row['assets']} | {row['trades']} | "
            f"{pct(row['equal_weight_return'])} | {row['scaled_profit_on_10k_usdt']:.2f} USDT | "
            f"{pct(row['realized_max_drawdown'])} |"
        )

    lines.extend(
        [
            "",
            "## Spot buy-and-hold baseline",
            "",
            "This is an equal-weight, one-entry/one-exit baseline over each asset's eligible",
            "portion of the same split, with the same Spot fee and slippage assumptions.",
            "Later-listed assets therefore have shorter windows, and current-listing membership",
            "makes this a conditional reference rather than a common-calendar alpha benchmark.",
            "",
            "| Split | Assets | Gross return | Net return | Profit on 10k |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in results.get("buy_hold_aggregate", []):
        lines.append(
            f"| {row['split']} | {row['assets']} | "
            f"{pct(row['equal_weight_gross_return'])} | "
            f"{pct(row['equal_weight_net_return'])} | "
            f"{row['equal_weight_net_return'] * 10_000:.2f} USDT |"
        )

    lines.extend(
        [
            "",
            "## By asset",
            "",
            "| Side | Asset | Trades | Net P&L | Expectancy | Win rate | PF | n>=30 |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for raw in results["groups"]["symbol"]:
        group = raw["group"]
        pf = "n/a" if raw["profit_factor"] is None else f"{raw['profit_factor']:.2f}"
        lines.append(
            f"| {group['direction']} | {group['asset']} | {raw['trades']} | "
            f"{raw['net_pnl_usdt']:.2f} | {pct(raw['mean_net_return'])} | "
            f"{pct(raw['win_rate'])} | {pf} | {raw['at_least_30_trades']} |"
        )

    lines.extend(
        [
            "",
            "## By signal family",
            "",
            "| Side | Family | Trades | Gross P&L | Net P&L | Expectancy | Win rate | PF | n>=30 |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for raw in results["groups"]["family"]:
        group = raw["group"]
        pf = "n/a" if raw["profit_factor"] is None else f"{raw['profit_factor']:.2f}"
        lines.append(
            f"| {group['direction']} | {group['family']} | {raw['trades']} | "
            f"{raw['gross_pnl_usdt']:.2f} | {raw['net_pnl_usdt']:.2f} | "
            f"{pct(raw['mean_net_return'])} | {pct(raw['win_rate'])} | {pf} | "
            f"{raw['at_least_30_trades']} |"
        )

    lines.extend(
        [
            "",
            "## By technical exit",
            "",
            "Exit reason is a post-path attribution, not a randomized exit-rule comparison;",
            "high trailing/time-exit PF must not be read as causal superiority.",
            "",
            "| Side | Exit | Trades | Net P&L | Expectancy | Win rate | PF |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for raw in results["groups"]["exit_reason"]:
        group = raw["group"]
        pf = "n/a" if raw["profit_factor"] is None else f"{raw['profit_factor']:.2f}"
        lines.append(
            f"| {group['direction']} | {group['exit_reason']} | {raw['trades']} | "
            f"{raw['net_pnl_usdt']:.2f} | {pct(raw['mean_net_return'])} | "
            f"{pct(raw['win_rate'])} | {pf} |"
        )

    lines.extend(
        [
            "",
            "## By cohort",
            "",
            "| Side | Cohort | Trades | Net P&L | Expectancy | Win rate | PF |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for raw in results["groups"]["cohort"]:
        group = raw["group"]
        pf = "n/a" if raw["profit_factor"] is None else f"{raw['profit_factor']:.2f}"
        lines.append(
            f"| {group['direction']} | {group['cohort']} | {raw['trades']} | "
            f"{raw['net_pnl_usdt']:.2f} | {pct(raw['mean_net_return'])} | "
            f"{pct(raw['win_rate'])} | {pf} |"
        )

    lines.extend(
        [
            "",
            "## Interpretation boundaries",
            "",
            "- Signals are rule-strength events, not calibrated probabilities.",
            flow_boundary,
            context_boundary,
            "- Klines do not contain historical top-of-book spread. The frozen proxy grants no",
            "  tight-spread score, while fees and adverse slippage are explicitly deducted.",
            "- The fixed current-listings panel has survivorship bias. The n>=30 column is only a",
            "  minimum-count flag, not a stability claim; trades share market and time dependence.",
            "- Realized drawdown updates on trade exits; it is not intratrade "
            "mark-to-market drawdown.",
            "- Intrabar stop order relative to a funding event is unknowable from OHLC. Stops are",
            "  timestamped at bar close; the effect is direction-dependent, so no-funding net is",
            "  reported as a sensitivity bound rather than labelled conservative.",
            "- This is an alert/paper-trade research tool and sends no exchange orders.",
            "- Feature-family and threshold comparisons belong to the companion experiment",
            "  summary; this per-run report does not treat them as hidden confirmations.",
            *(
                [
                    "",
                    "## R3 raw replay interpretation contract",
                    "",
                    f"- Artifact role: `{_R3_RAW_ARTIFACT_ROLE}`.",
                    "- The sequential T72 trade ledger is non-independent, secondary, and",
                    "  non-primary; it is not the opportunity-based 15/30/60-minute endpoint.",
                    "- The legacy CI/bootstrap shown above is not the frozen R3 shared",
                    "  UTC-day moving-block bootstrap and cannot supply final R3 inference.",
                    "- The R2 C0 T72 result remains `PROTOCOL_MISMATCH`.",
                    "- Primary efficacy and final status axes are supplied only by the",
                    "  separate opportunity-based R3 analyzer.",
                ]
                if is_r3_raw_replay
                else []
            ),
            "",
            "## Material Passport",
            "",
            "- Generator: `academic-research-suite / experiment-agent`",
            "- Stage: run and validate",
            f"- Protocol/config: `{spec.protocol_version}` / `{spec_label}`",
            "- Inputs: official Binance public Spot and USD-M kline/funding endpoints",
            "- Reproducibility: deterministic dataset gzip, SHA-256 manifests, "
            "fixed bootstrap seed",
            "- Runner provenance: `run_manifest.json` records SHA-256 hashes for "
            f"{manifest_output_labels}.",
            "",
        ]
    )
    return "\n".join(lines)
