from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn

import uvicorn

from signalbot.api.server import create_api
from signalbot.app import SignalApplication
from signalbot.backtest.alert_replay import run_alert_replay
from signalbot.backtest.carry_runner import run_carry_experiment
from signalbot.backtest.comparison import (
    compare_common_opportunity_panels,
    compare_strategy_runs,
    read_opportunity_observations,
    read_trade_observations,
)
from signalbot.backtest.config import load_backtest_spec
from signalbot.backtest.outcomes import OutcomeEvaluator
from signalbot.backtest.r2 import (
    analyze_r2_retrospective,
    validate_r2_analysis_parameters,
    validate_r2_run_provenance,
)
from signalbot.backtest.r3 import analyze_r3_run
from signalbot.backtest.r4 import analyze_r4_run
from signalbot.backtest.replay import ReplayEngine
from signalbot.backtest.runner import (
    download_research_data,
    run_research_backtest,
    source_code_digest,
)
from signalbot.backtest.verdict import (
    evaluate_r1_verdict,
    read_verdict_opportunities,
    read_verdict_trades,
)
from signalbot.clock import ReplayClock
from signalbot.config import Settings, load_settings
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle, SignalDecision
from signalbot.exchange.binance.endpoints import build_websocket_plans
from signalbot.observability.logging import configure_logging
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="signalbot")
    subs = parser.add_subparsers(dest="command", required=True)
    validate = subs.add_parser("validate-config")
    validate.add_argument("--config", required=True)
    run = subs.add_parser("run")
    run.add_argument("--config", required=True)
    run.add_argument("--dry-run", action="store_true")
    replay = subs.add_parser("replay")
    replay.add_argument("--config", required=True)
    replay.add_argument("--market", choices=[m.value for m in Market], required=True)
    replay.add_argument("--input", required=True)
    outcomes = subs.add_parser("evaluate-outcomes")
    outcomes.add_argument("--config", required=True)
    outcomes.add_argument("--input", required=True)
    outcomes.add_argument(
        "--horizons",
        type=int,
        nargs="+",
        default=[900, 3600, 14400, 43200, 86400],
    )
    api = subs.add_parser("serve-api")
    api.add_argument("--config", required=True)
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8080)
    download = subs.add_parser("backtest-download")
    download.add_argument("--spec", required=True)
    download.add_argument("--data-dir", required=True)
    download.add_argument("--concurrency", type=int, default=3)
    backtest = subs.add_parser("backtest-run")
    backtest.add_argument("--config", required=True)
    backtest.add_argument("--spec", required=True)
    backtest.add_argument("--data-dir", required=True)
    backtest.add_argument("--output-dir", required=True)
    alert_replay = subs.add_parser("backtest-alert-replay")
    alert_replay.add_argument("--config", required=True)
    alert_replay.add_argument("--spec", required=True)
    alert_replay.add_argument("--data-dir", required=True)
    alert_replay.add_argument("--output-dir", required=True)
    alert_replay.add_argument(
        "--split",
        dest="split_names",
        action="append",
        help="Declared chronological split to replay; repeat to join adjacent splits.",
    )
    compare = subs.add_parser("backtest-compare")
    compare.add_argument("--spec", required=True)
    compare.add_argument("--b0-dir", required=True)
    compare.add_argument("--b3-dir", required=True)
    compare.add_argument("--b2-dir", required=True)
    compare.add_argument("--headline-dir", required=True)
    compare.add_argument("--output", required=True)
    volume_compare = subs.add_parser("backtest-volume-compare")
    volume_compare.add_argument("--spec", required=True)
    volume_compare.add_argument("--c0-dir", required=True)
    volume_compare.add_argument("--g2-dir", required=True)
    volume_compare.add_argument("--g4-dir", required=True)
    volume_compare.add_argument("--samples", type=int, default=50_000)
    volume_compare.add_argument("--block-days", type=int, default=7)
    volume_compare.add_argument("--seed", type=int, default=20_260_715)
    volume_compare.add_argument("--output", required=True)
    volume_verdict = subs.add_parser("backtest-volume-verdict")
    volume_verdict.add_argument("--c0-dir", required=True)
    volume_verdict.add_argument("--g2-dir", required=True)
    volume_verdict.add_argument("--g4-dir", required=True)
    volume_verdict.add_argument("--comparison", required=True)
    volume_verdict.add_argument("--determinism-parity-passed", action="store_true")
    volume_verdict.add_argument("--output", required=True)
    r2_analyze = subs.add_parser("backtest-r2-analyze")
    r2_analyze.add_argument("--c0-a-dir", required=True)
    r2_analyze.add_argument("--c0-b-dir", required=True)
    r2_analyze.add_argument("--h1-a-dir", required=True)
    r2_analyze.add_argument("--h1-b-dir", required=True)
    r2_analyze.add_argument("--samples", type=int, default=50_000)
    r2_analyze.add_argument("--seed", type=int, default=20_260_716)
    r2_analyze.add_argument("--output", required=True)
    r3_analyze = subs.add_parser("backtest-r3-analyze")
    r3_analyze.add_argument("--run-dir", required=True)
    r3_analyze.add_argument("--spec", required=True)
    r3_analyze.add_argument("--output-dir", required=True)
    r4_analyze = subs.add_parser("backtest-r4-analyze")
    r4_analyze.add_argument("--opportunities", required=True)
    r4_analyze.add_argument("--spec", required=True)
    r4_analyze.add_argument("--output-dir", required=True)
    c1_run = subs.add_parser("backtest-c1-run")
    c1_run.add_argument("--spec", required=True)
    c1_run.add_argument("--data-dir", required=True)
    c1_run.add_argument("--output-dir", required=True)
    return parser


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _summary(settings: Settings) -> dict[str, object]:
    return {
        "app_name": settings.app_name,
        "markets": [m.value for m in settings.binance.markets],
        "top_n": settings.binance.top_n,
        "intervals": settings.binance.intervals,
        "primary_interval": settings.binance.primary_interval,
        "discord_enabled": settings.alerts.discord_enabled,
        "storage_driver": settings.storage.url.split(":", 1)[0],
        "rule_version": settings.rule_version,
    }


def _dry_run(settings: Settings) -> None:
    symbols = ["BTCUSDT", "ETHUSDT"]
    plans = {
        market.value: [
            {"name": p.name, "route": p.route, "stream_count": len(p.streams)}
            for p in build_websocket_plans(
                market, symbols, settings.binance.intervals, settings.binance.websocket_batch_size
            )
        ]
        for market in settings.binance.markets
    }
    print(json.dumps({"configuration": _summary(settings), "example_plans": plans}, indent=2))


async def _replay(settings: Settings, market: Market, input_path: Path) -> None:
    clock = ReplayClock()
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()

    async def collect(_: SignalDecision) -> object:
        return None

    runtime = MarketRuntime(market, settings, repository, clock, collect)
    runtime.set_surveillance_symbols(frozenset({"BTCUSDT", "ETHUSDT", "TESTUSDT"}))
    result = await ReplayEngine(runtime, clock).run_file(input_path)
    print(
        json.dumps(
            {
                "events_read": result.events_read,
                "parse_errors": result.parse_errors,
                "decisions": [d.model_dump(mode="json") for d in result.decisions],
            },
            indent=2,
        )
    )
    repository.close()


def _evaluate_outcomes(input_path: Path, horizons: list[int]) -> None:
    raw = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("outcome input root must be an object")
    decision = SignalDecision.model_validate(raw.get("decision"))
    candle_rows = raw.get("candles")
    if not isinstance(candle_rows, list):
        raise ValueError("outcome input candles must be an array")
    candles = [Candle.model_validate(row) for row in candle_rows]
    evaluator = OutcomeEvaluator()
    values = [
        asdict(outcome)
        for horizon in sorted(set(horizons))
        if horizon > 0 and (outcome := evaluator.evaluate(decision, candles, horizon)) is not None
    ]
    print(json.dumps({"event_id": decision.event_id, "outcomes": values}, indent=2))


def main() -> None:
    args = _parser().parse_args()
    if args.command == "backtest-download":
        spec = load_backtest_spec(args.spec)
        result = asyncio.run(
            download_research_data(spec, args.data_dir, concurrency=args.concurrency)
        )
        print(json.dumps(result, indent=2))
        return
    if args.command == "backtest-compare":
        spec = load_backtest_spec(args.spec)
        runs = {
            name: read_trade_observations(Path(directory) / "trades.csv")
            for name, directory in (
                ("b0", args.b0_dir),
                ("b3", args.b3_dir),
                ("b2", args.b2_dir),
                ("headline", args.headline_dir),
            )
        }
        result = compare_strategy_runs(
            runs,
            evaluation_start_ms=int(spec.evaluation_start.timestamp() * 1000),
            evaluation_end_ms=int(spec.evaluation_end.timestamp() * 1000),
            samples=spec.bootstrap.samples,
            block_days=spec.bootstrap.block_days,
            seed=spec.bootstrap.seed,
        )
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        print(json.dumps(result, indent=2))
        return
    if args.command == "backtest-volume-compare":
        spec = load_backtest_spec(args.spec)
        panels = {
            variant: read_opportunity_observations(
                Path(directory) / "opportunities.csv"
            )
            for variant, directory in (
                ("C0", args.c0_dir),
                ("G2", args.g2_dir),
                ("G4", args.g4_dir),
            )
        }
        result = compare_common_opportunity_panels(
            panels,
            evaluation_start_ms=int(spec.evaluation_start.timestamp() * 1000),
            evaluation_end_ms=int(spec.evaluation_end.timestamp() * 1000),
            samples=args.samples,
            block_days=args.block_days,
            seed=args.seed,
        )
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        print(json.dumps(result, indent=2))
        return
    if args.command == "backtest-volume-verdict":
        directories = {"C0": args.c0_dir, "G2": args.g2_dir, "G4": args.g4_dir}
        opportunities = {
            variant: read_verdict_opportunities(
                Path(directory) / "opportunities.csv"
            )
            for variant, directory in directories.items()
        }
        trades = {
            variant: read_verdict_trades(Path(directory) / "trades.csv")
            for variant, directory in directories.items()
        }
        comparison_raw = json.loads(Path(args.comparison).read_text(encoding="utf-8"))
        if not isinstance(comparison_raw, dict):
            raise ValueError("comparison root must be an object")
        result = evaluate_r1_verdict(
            opportunities,
            trades,
            comparison_raw,
            determinism_parity_passed=args.determinism_parity_passed,
        ).to_dict()
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        print(json.dumps(result, indent=2))
        return
    if args.command == "backtest-r2-analyze":
        validate_r2_analysis_parameters(args.samples, args.seed)
        directories = {
            "c0_a": Path(args.c0_a_dir),
            "c0_b": Path(args.c0_b_dir),
            "h1_a": Path(args.h1_a_dir),
            "h1_b": Path(args.h1_b_dir),
        }
        provenance = validate_r2_run_provenance(
            directories["c0_a"],
            directories["c0_b"],
            directories["h1_a"],
            directories["h1_b"],
        )
        analysis_root = Path(__file__).resolve().parents[2]
        analysis_code_sha256 = source_code_digest(analysis_root)
        if analysis_code_sha256 != provenance["code_sha256"]:
            raise ValueError(
                "R2 analyzer source differs from the source used for the frozen runs"
            )
        result = analyze_r2_retrospective(
            directories["c0_a"] / "opportunities.csv",
            directories["h1_a"] / "opportunities.csv",
            c0_trades=directories["c0_a"] / "trades.csv",
            h1_trades=directories["h1_a"] / "trades.csv",
            bootstrap_samples=args.samples,
            seed=args.seed,
        )
        result["provenance"] = provenance
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        source_files = {
            f"{name}/{filename}": directory / filename
            for name, directory in directories.items()
            for filename in ("run_manifest.json", "opportunities.csv", "trades.csv")
        }
        analysis_manifest = {
            "protocol_version": "r2_retrospective_screen_v1",
            "created_at_utc": datetime.now(UTC).isoformat(),
            "argv": sys.argv,
            "run_code_sha256": provenance["code_sha256"],
            "analysis_code_sha256": analysis_code_sha256,
            "experiment_plan_sha256": provenance["experiment_plan_sha256"],
            "inputs": {
                name: _file_sha256(path) for name, path in sorted(source_files.items())
            },
            "output": {output.name: _file_sha256(output)},
            "bootstrap_samples": args.samples,
            "seed": args.seed,
        }
        manifest_path = output.with_name("r2_analysis_manifest.json")
        manifest_path.write_text(
            json.dumps(
                analysis_manifest,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
            newline="\n",
        )
        print(
            json.dumps(
                {
                    "status": result["status"],
                    "full_r2_status": result["full_r2_status"],
                    "output": str(output),
                    "manifest": str(manifest_path),
                },
                indent=2,
            )
        )
        return
    if args.command == "backtest-r3-analyze":
        root = Path(__file__).resolve().parents[2]
        result = analyze_r3_run(
            args.run_dir,
            args.spec,
            workspace_root=root,
            output_dir=args.output_dir,
        )
        status_axes = result["status_axes"]
        print(
            json.dumps(
                {
                    "data_integrity": status_axes["data_integrity"],
                    "status": status_axes["kline_proxy_efficacy"],
                    "execution_validity": status_axes["execution_validity"],
                    "output_dir": args.output_dir,
                },
                indent=2,
            )
        )
        if status_axes["data_integrity"] != "PASS":
            raise SystemExit(2)
        return
    if args.command == "backtest-r4-analyze":
        root = Path(__file__).resolve().parents[2]
        result, paths = analyze_r4_run(
            args.opportunities,
            args.spec,
            args.output_dir,
            workspace_root=root,
        )
        print(
            json.dumps(
                {
                    "status": result["status"],
                    "deployment": result["deployment"],
                    "outputs": paths,
                },
                indent=2,
            )
        )
        return
    if args.command == "backtest-c1-run":
        root = Path(__file__).resolve().parents[2]
        result, paths = run_carry_experiment(
            args.spec,
            args.data_dir,
            args.output_dir,
            workspace_root=root,
        )
        status_axes = result["status_axes"]
        print(
            json.dumps(
                {
                    "data_integrity": status_axes["data_integrity"],
                    "status": status_axes["efficacy"],
                    "deployment": status_axes["deployment"],
                    "outputs": paths,
                },
                indent=2,
            )
        )
        if status_axes["data_integrity"] != "PASS":
            raise SystemExit(2)
        return
    settings = load_settings(args.config)
    configure_logging(settings.log_level)
    if args.command == "validate-config":
        print(json.dumps(_summary(settings), indent=2))
        return
    if args.command == "run":
        if args.dry_run:
            _dry_run(settings)
            return
        asyncio.run(SignalApplication(settings).run())
        return
    if args.command == "replay":
        asyncio.run(_replay(settings, Market(args.market), Path(args.input)))
        return
    if args.command == "evaluate-outcomes":
        _evaluate_outcomes(Path(args.input), args.horizons)
        return
    if args.command == "serve-api":
        repository = SqlRepository(settings.storage.url, settings.storage.echo_sql)
        repository.initialize()
        uvicorn.run(create_api(repository), host=args.host, port=args.port)
        return
    if args.command == "backtest-run":
        spec = load_backtest_spec(args.spec)
        root = Path(__file__).resolve().parents[2]
        result = run_research_backtest(
            settings,
            spec,
            args.data_dir,
            args.output_dir,
            workspace_root=root,
            spec_path=args.spec,
            config_path=args.config,
        )
        print(json.dumps(result, indent=2))
        return
    if args.command == "backtest-alert-replay":
        spec = load_backtest_spec(args.spec)
        root = Path(__file__).resolve().parents[2]
        result = run_alert_replay(
            settings,
            spec,
            args.data_dir,
            args.output_dir,
            workspace_root=root,
            spec_path=args.spec,
            config_path=args.config,
            split_names=args.split_names,
        )
        print(json.dumps(result, indent=2))
        return
    _unreachable()


def _unreachable() -> NoReturn:
    raise RuntimeError("unreachable command")
