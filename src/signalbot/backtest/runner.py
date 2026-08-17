from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import platform
import sys
import time
from dataclasses import asdict, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from signalbot.backtest.analysis import (
    RUN_MANIFEST_OUTPUTS,
    aggregate_buy_hold,
    build_results,
    buy_hold_baselines,
    render_report,
)
from signalbot.backtest.config import BacktestSpec
from signalbot.backtest.dataset import (
    KlineDatasetRequest,
    build_dataset_manifest,
    download_kline_dataset,
    read_kline_csv,
    verify_dataset_manifest,
    write_dataset_manifest,
    write_kline_csv,
)
from signalbot.backtest.engine import (
    Opportunity,
    ResearchBacktester,
    SymbolBacktest,
    Trade,
    build_market_regimes,
)
from signalbot.backtest.funding import (
    download_funding_dataset,
    funding_sha256,
    verify_funding_dataset,
    write_funding_csv,
)
from signalbot.config import Settings
from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.exchange.binance.rest import BinanceRestClient


def dataset_path(data_dir: Path, market: Market, asset: str, symbol: str, interval: str) -> Path:
    return data_dir / market.value / f"{asset}__{symbol}__{interval}.csv.gz"


def funding_path(
    data_dir: Path, asset: str, symbol: str, interval: str | None = None
) -> Path:
    interval_tag = "" if interval in {None, "1h"} else f"__{interval}"
    return data_dir / "funding" / f"{asset}__{symbol}{interval_tag}.csv.gz"


def _research_kline_request(
    spec: BacktestSpec, market: Market, asset: str, symbol: str
) -> KlineDatasetRequest:
    return KlineDatasetRequest(
        market=market,
        symbol=symbol,
        alias=asset,
        interval=spec.interval,
        start_time_ms=int(spec.data_start.timestamp() * 1000),
        end_time_ms=int(spec.evaluation_end.timestamp() * 1000) - 1,
    )


async def download_research_data(
    spec: BacktestSpec,
    data_dir: str | Path,
    *,
    concurrency: int = 3,
) -> dict[str, Any]:
    root = Path(data_dir)
    start_ms = int(spec.data_start.timestamp() * 1000)
    end_ms = int(spec.evaluation_end.timestamp() * 1000) - 1
    semaphore = asyncio.Semaphore(concurrency)
    downloaded: list[str] = []
    reused: list[str] = []

    async with BinanceRestClient(Market.SPOT) as spot_client, BinanceRestClient(
        Market.FUTURES
    ) as futures_client:

        async def one_kline(market: Market, asset: str, symbol: str) -> None:
            target = dataset_path(root, market, asset, symbol, spec.interval)
            manifest_path = target.with_suffix(target.suffix + ".manifest.json")
            request = _research_kline_request(spec, market, asset, symbol)
            if target.exists() and manifest_path.exists():
                verify_dataset_manifest(
                    target, manifest_path, expected_request=request
                )
                reused.append(str(target))
                return
            client = spot_client if market is Market.SPOT else futures_client
            page_limit = 1000 if market is Market.SPOT else 499
            async with semaphore:
                dataset = await download_kline_dataset(
                    client, request, page_limit=page_limit
                )
            write_kline_csv(dataset, target)
            write_dataset_manifest(build_dataset_manifest(target), manifest_path)
            downloaded.append(str(target))

        await asyncio.gather(
            *(
                one_kline(market, asset.asset, symbol)
                for asset in spec.assets
                for market, symbol in (
                    (Market.SPOT, asset.spot_symbol),
                    (Market.FUTURES, asset.futures_symbol),
                )
            )
        )

        async def one_funding(asset: str, symbol: str) -> None:
            target = funding_path(root, asset, symbol, spec.interval)
            if target.exists():
                verify_funding_dataset(
                    target,
                    expected_symbol=symbol,
                    expected_start_time_ms=start_ms,
                    expected_end_time_ms=end_ms,
                )
                reused.append(str(target))
                return
            async with semaphore:
                dataset = await download_funding_dataset(
                    futures_client, symbol, start_ms, end_ms
                )
            write_funding_csv(dataset, target)
            downloaded.append(str(target))

        if spec.costs.include_funding:
            await asyncio.gather(
                *(one_funding(asset.asset, asset.futures_symbol) for asset in spec.assets)
            )

    return {
        "protocol_version": spec.protocol_version,
        "downloaded": sorted(downloaded),
        "reused": sorted(reused),
        "files": len(downloaded) + len(reused),
    }


def _write_trades(trades: list[Trade], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = [field.name for field in fields(Trade)]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, lineterminator="\n")
        writer.writeheader()
        for trade in sorted(trades, key=lambda item: (item.entry_time_ms, item.trade_id)):
            writer.writerow(asdict(trade))


def _write_opportunities(opportunities: list[Opportunity], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = [field.name for field in fields(Opportunity)]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, lineterminator="\n")
        writer.writeheader()
        for opportunity in sorted(
            opportunities,
            key=lambda item: (item.decision_time_ms, item.opportunity_id),
        ):
            writer.writerow(asdict(opportunity))


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n"


def _maximum_concurrent_positions(trades: list[Trade]) -> int:
    events = [
        event
        for trade in trades
        for event in ((trade.entry_time_ms, 1), (trade.exit_time_ms, -1))
    ]
    active = 0
    maximum = 0
    for _, change in sorted(events, key=lambda item: (item[0], item[1])):
        active += change
        maximum = max(maximum, active)
    return maximum


def _build_signal_funnel(
    symbol_runs: list[SymbolBacktest], trades: list[Trade], spec: BacktestSpec
) -> list[dict[str, Any]]:
    evaluation_days = (
        spec.evaluation_end - spec.evaluation_start
    ).total_seconds() / 86_400
    rows: list[dict[str, Any]] = []
    for market in (Market.SPOT, Market.FUTURES):
        runs = [run for run in symbol_runs if run.market is market]
        market_trades = [trade for trade in trades if trade.market == market.value]
        evaluated_bars = sum(run.evaluated_bars for run in runs)
        candidate_setups = sum(run.candidate_setups for run in runs)
        exposure_bars = sum(trade.bars_held for trade in market_trades)
        maximum = _maximum_concurrent_positions(market_trades)
        rows.append(
            {
                "market": market.value,
                "evaluated_symbol_bars": evaluated_bars,
                "candidate_setups": candidate_setups,
                "candidate_rate": (
                    candidate_setups / evaluated_bars if evaluated_bars else 0.0
                ),
                "confirmed_state_transitions": sum(
                    run.confirmed_signals for run in runs
                ),
                "scheduled_entries": sum(run.scheduled_entries for run in runs),
                "cancelled_gap_entries": sum(
                    run.cancelled_gap_entries for run in runs
                ),
                "trades": len(market_trades),
                "trades_per_day": (
                    len(market_trades) / evaluation_days if evaluation_days else 0.0
                ),
                "symbol_time_in_market": (
                    exposure_bars / evaluated_bars if evaluated_bars else 0.0
                ),
                "maximum_concurrent_positions": maximum,
                "required_notional_at_peak_usdt": (
                    maximum * spec.costs.notional_usdt
                ),
            }
        )
    return rows


def _build_opportunity_summary(
    opportunities: list[Opportunity], spec: BacktestSpec
) -> list[dict[str, Any]]:
    interval_minutes = interval_to_milliseconds(spec.interval) // 60_000
    rows: list[dict[str, Any]] = []
    for market in (Market.SPOT, Market.FUTURES):
        values = [item for item in opportunities if item.market == market.value]
        available = sum(item.volume_feature_available for item in values)
        analysis = [item for item in values if item.analysis_eligible]
        eligible = [item for item in analysis if item.eligible]
        horizons: dict[str, dict[str, Any]] = {}
        for horizon in (3, 6, 12):
            horizon_analysis = [
                item
                for item in values
                if getattr(item, f"analysis_eligible_{horizon}")
            ]
            horizon_eligible = [item for item in horizon_analysis if item.eligible]
            labels = [
                getattr(item, f"outcome_label_{horizon}")
                for item in horizon_eligible
            ]
            label_counts = {
                label: labels.count(label)
                for label in (
                    "KLINE_PROXY_LONG",
                    "KLINE_PROXY_FLAT",
                    "KLINE_PROXY_SHORT",
                )
            }
            count = len(horizon_eligible)
            horizons[str(horizon)] = {
                "horizon_bars": horizon,
                "horizon_minutes": horizon * interval_minutes,
                "analysis_eligible_opportunities": len(horizon_analysis),
                "gate_eligible_opportunities": count,
                "label_counts": label_counts,
                "label_prevalence": {
                    label: value / count if count else 0.0
                    for label, value in label_counts.items()
                },
                "mean_long_net_return": (
                    sum(
                        getattr(item, f"long_net_return_{horizon}")
                        for item in horizon_eligible
                    )
                    / count
                    if count
                    else 0.0
                ),
                "mean_short_net_return": (
                    sum(
                        getattr(item, f"short_net_return_{horizon}")
                        for item in horizon_eligible
                    )
                    / count
                    if count
                    else 0.0
                ),
                "mean_signal_net_return": (
                    sum(
                        getattr(item, f"signal_net_return_{horizon}")
                        for item in horizon_eligible
                    )
                    / count
                    if count
                    else 0.0
                ),
            }
        rows.append(
            {
                "market": market.value,
                "volume_feature_set": spec.volume_feature_set,
                "price_trigger_opportunities": len(values),
                "volume_feature_available": available,
                "volume_feature_availability_rate": (
                    available / len(values) if values else 0.0
                ),
                "analysis_eligible_opportunities": len(analysis),
                "gate_eligible_opportunities": len(eligible),
                "gate_acceptance_rate": (
                    len(eligible) / len(analysis) if analysis else 0.0
                ),
                "mean_forward_return_12": (
                    sum(item.forward_return_12 or 0.0 for item in eligible) / len(eligible)
                    if eligible
                    else 0.0
                ),
                "horizons": horizons,
            }
        )
    return rows


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def source_code_digest(root: Path) -> str:
    """Hash the complete Python implementation used by a research run."""

    digest = hashlib.sha256()
    files = sorted((root / "src" / "signalbot").rglob("*.py"))
    for path in files:
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _path_label(path: str | Path, workspace: Path) -> str:
    source = Path(path).resolve()
    try:
        return source.relative_to(workspace.resolve()).as_posix()
    except ValueError:
        return source.name


def _build_run_manifest(
    *,
    settings: Settings,
    spec: BacktestSpec,
    workspace: Path,
    spec_path: str | Path,
    config_path: str | Path | None,
    input_hashes: dict[str, str],
    output_root: Path,
    started_at_utc: datetime | None = None,
    duration_seconds: float = 0.0,
) -> dict[str, Any]:
    config_digest = (
        None
        if config_path is None
        else hashlib.sha256(Path(config_path).read_bytes()).hexdigest()
    )
    started_at = started_at_utc or datetime.now(UTC)
    plan_path = (
        None
        if spec.experiment_plan_path is None
        else (workspace / spec.experiment_plan_path).resolve()
    )
    if plan_path is not None and not plan_path.is_file():
        raise FileNotFoundError(f"experiment plan not found: {plan_path}")
    uv_lock = workspace / "uv.lock"
    return {
        "protocol_version": spec.protocol_version,
        "rule_version": spec.rule_version,
        "code_sha256": source_code_digest(workspace),
        "spec_sha256": hashlib.sha256(Path(spec_path).read_bytes()).hexdigest(),
        "spec_input_path": _path_label(spec_path, workspace),
        "backtest_contract": {
            "candidate_policy": spec.candidate_policy,
            "confirmation_mode": spec.confirmation_mode,
            "interval": spec.interval,
            "max_holding_bars": spec.exits.max_holding_bars,
            "opportunity_panel_horizon_bars": (
                spec.opportunity_panel_horizon_bars
            ),
            "outcome_edge_margin_bps": spec.outcome_edge_margin_bps,
            "prediction_horizons_bars": [3, 6, 12],
            "prediction_entry": f"next_contiguous_{spec.interval}_open",
            "prediction_exit": "decision_index_plus_h_close",
            "outcome_labels": [
                "KLINE_PROXY_LONG",
                "KLINE_PROXY_FLAT",
                "KLINE_PROXY_SHORT",
            ],
        },
        "effective_settings_sha256": _canonical_sha256(
            settings.model_dump(mode="json")
        ),
        "config_input_sha256": config_digest,
        "config_input_path": (
            None if config_path is None else _path_label(config_path, workspace)
        ),
        "experiment_plan_path": (
            None if plan_path is None else _path_label(plan_path, workspace)
        ),
        "experiment_plan_sha256": (
            None if plan_path is None else hashlib.sha256(plan_path.read_bytes()).hexdigest()
        ),
        "environment": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "uv_lock_sha256": (
                hashlib.sha256(uv_lock.read_bytes()).hexdigest()
                if uv_lock.is_file()
                else None
            ),
        },
        "execution": {
            "argv": sys.argv,
            "working_directory": _path_label(Path.cwd(), workspace),
            "started_at_utc": started_at.isoformat(),
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "duration_seconds": duration_seconds,
            "exit_code": 0,
        },
        "inputs": dict(sorted(input_hashes.items())),
        "outputs": {
            name: hashlib.sha256((output_root / name).read_bytes()).hexdigest()
            for name in RUN_MANIFEST_OUTPUTS
            if (output_root / name).is_file()
        },
    }


def run_research_backtest(
    settings: Settings,
    spec: BacktestSpec,
    data_dir: str | Path,
    output_dir: str | Path,
    *,
    workspace_root: str | Path,
    spec_path: str | Path,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    started_at_utc = datetime.now(UTC)
    started_at_counter = time.perf_counter()
    data_root = Path(data_dir)
    output_root = Path(output_dir)
    workspace = Path(workspace_root)
    start_ms = int(spec.data_start.timestamp() * 1000)
    end_ms = int(spec.evaluation_end.timestamp() * 1000) - 1
    candles: dict[Market, dict[str, list[Candle]]] = {
        Market.SPOT: {},
        Market.FUTURES: {},
    }
    input_hashes: dict[str, str] = {}
    for asset in spec.assets:
        for market, symbol in (
            (Market.SPOT, asset.spot_symbol),
            (Market.FUTURES, asset.futures_symbol),
        ):
            path = dataset_path(data_root, market, asset.asset, symbol, spec.interval)
            manifest_path = path.with_suffix(path.suffix + ".manifest.json")
            request = _research_kline_request(spec, market, asset.asset, symbol)
            verify_dataset_manifest(
                path, manifest_path, expected_request=request
            )
            dataset = read_kline_csv(path)
            candles[market][asset.asset] = list(dataset.candles)
            input_hashes[path.relative_to(data_root).as_posix()] = build_dataset_manifest(
                path
            ).sha256

    regimes = {
        market: build_market_regimes(by_asset)
        for market, by_asset in candles.items()
    }
    backtester = ResearchBacktester(settings, spec)
    symbol_runs: list[SymbolBacktest] = []
    trades: list[Trade] = []
    opportunities: list[Opportunity] = []
    for asset in spec.assets:
        for market in (Market.SPOT, Market.FUTURES):
            funding = []
            if market is Market.FUTURES and spec.costs.include_funding:
                path = funding_path(
                    data_root, asset.asset, asset.futures_symbol, spec.interval
                )
                funding_dataset = verify_funding_dataset(
                    path,
                    expected_symbol=asset.futures_symbol,
                    expected_start_time_ms=start_ms,
                    expected_end_time_ms=end_ms,
                )
                funding = list(funding_dataset.rates)
                input_hashes[path.relative_to(data_root).as_posix()] = funding_sha256(path)
            run = backtester.run_symbol(
                asset,
                market,
                candles[market][asset.asset],
                regimes[market][asset.asset],
                funding,
            )
            symbol_runs.append(run)
            trades.extend(run.trades)
            opportunities.extend(run.opportunities)

    results = build_results(trades, spec)
    eligible = {
        run.asset: run.eligible_from_ms
        for run in symbol_runs
        if run.market is Market.SPOT
    }
    baselines = buy_hold_baselines(candles[Market.SPOT], eligible, spec)
    results["buy_hold"] = [asdict(row) for row in baselines]
    results["buy_hold_aggregate"] = aggregate_buy_hold(baselines)
    results["symbol_runs"] = [
        {
            **asdict(run),
            "market": run.market.value,
            "trades": len(run.trades),
            "opportunities": len(run.opportunities),
        }
        for run in symbol_runs
    ]
    results["signal_funnel"] = _build_signal_funnel(symbol_runs, trades, spec)
    results["opportunity_summary"] = _build_opportunity_summary(
        opportunities, spec
    )
    output_root.mkdir(parents=True, exist_ok=True)
    trades_path = output_root / "trades.csv"
    results_path = output_root / "results.json"
    report_path = output_root / "report.md"
    opportunities_path = output_root / "opportunities.csv"
    _write_trades(trades, trades_path)
    _write_opportunities(opportunities, opportunities_path)
    results_path.write_text(_canonical_json(results), encoding="utf-8", newline="\n")
    report_path.write_text(
        render_report(
            results,
            spec,
            spec_label=_path_label(spec_path, workspace),
        ),
        encoding="utf-8",
        newline="\n",
    )

    manifest = _build_run_manifest(
        settings=settings,
        spec=spec,
        workspace=workspace,
        spec_path=spec_path,
        config_path=config_path,
        input_hashes=input_hashes,
        output_root=output_root,
        started_at_utc=started_at_utc,
        duration_seconds=time.perf_counter() - started_at_counter,
    )
    (output_root / "run_manifest.json").write_text(
        _canonical_json(manifest), encoding="utf-8", newline="\n"
    )
    return {
        "protocol_version": spec.protocol_version,
        "trades": len(trades),
        "output_dir": str(output_root),
        "outputs": manifest["outputs"],
    }
