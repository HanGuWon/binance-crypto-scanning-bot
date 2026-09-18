"""Executable validation pipeline for the preregistered Futures successor.

The runner consumes the existing local, manifest-verified archive-backed data.
It never downloads raw Drive shards, writes raw candles to the repository, or
touches the production alert/order path.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from signalbot.backtest.config import BacktestSpec, load_backtest_spec
from signalbot.backtest.dataset import (
    build_dataset_manifest,
    read_dataset_manifest,
    read_kline_csv,
    verify_dataset_manifest,
)
from signalbot.backtest.engine import Opportunity, ResearchBacktester, build_market_regimes
from signalbot.backtest.funding import verify_funding_dataset
from signalbot.backtest.runner import dataset_path, funding_path
from signalbot.config import Settings
from signalbot.domain.enums import Direction, Market
from signalbot.prospective.directional_candidates import (
    DirectionalPreregistration,
    DirectionalReceipt,
    HistoricalScreenStatus,
    PromotionEvidence,
    ProspectiveStatus,
    build_research_manifest,
    canonical_sha256,
    load_preregistration,
    replay_fixture,
)
from signalbot.prospective.source_freeze import freeze_source

_DIRECTIONAL_PROTOCOL = "futures_bidirectional_validation_v1"
_FIXTURE_LIMIT_PER_DIRECTION = 256


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _base_spec_for_directional_research(
    spec: BacktestSpec, preregistration: DirectionalPreregistration
) -> BacktestSpec:
    if spec.interval != preregistration.interval:
        raise ValueError("base backtest interval does not match preregistration")
    if tuple(asset.futures_symbol for asset in spec.assets) != tuple(
        item.symbol for item in preregistration.universe
    ):
        raise ValueError("base backtest Futures universe does not match preregistration")
    return spec.model_copy(
        update={
            "protocol_version": preregistration.protocol_version,
            "rule_version": preregistration.rule_version,
            "candidate_policy": "strict_pit_htf_diagnostic",
            "strategy_mode": "pit_breakout_volume",
            "confirmation_mode": "explicit_trigger",
            "gate_use_participation": False,
            "gate_use_crowding": False,
            "gate_use_higher_timeframes": True,
            "include_rsi_reversals": False,
            "direction_scope": "futures_bidirectional",
        }
    )


def _data_authority(
    *,
    preregistration: DirectionalPreregistration,
    spec: BacktestSpec,
    paths: dict[str, Path],
) -> tuple[str, dict[str, str]]:
    hashes: dict[str, str] = {}
    for label, path in sorted(paths.items()):
        hashes[label] = _sha256_file(path)
    payload = {
        "protocol_version": _DIRECTIONAL_PROTOCOL,
        "candidate_version": preregistration.candidate_version,
        "config_sha256": preregistration.config_sha256,
        "base_spec_sha256": canonical_sha256(spec.model_dump(mode="json")),
        "files": hashes,
    }
    return canonical_sha256(payload), payload


def _fixture_rows(
    opportunities: list[Opportunity],
    preregistration: DirectionalPreregistration,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_direction: dict[str, list[Opportunity]] = defaultdict(list)
    for item in opportunities:
        if item.analysis_eligible_3 and item.forward_return_3 is not None:
            by_direction[item.direction].append(item)
    for direction in (Direction.LONG.value, Direction.SHORT.value):
        selected = sorted(
            by_direction[direction], key=lambda item: (item.decision_time_ms, item.opportunity_id)
        )[:_FIXTURE_LIMIT_PER_DIRECTION]
        for item in selected:
            entry_time = item.next_open_time_ms
            forward_return = item.forward_return_3
            if entry_time is None or forward_return is None:
                continue
            direction_sign = 1.0 if direction == Direction.LONG.value else -1.0
            entry_price = 1.0
            exit_price = entry_price * (1.0 + direction_sign * forward_return)
            if exit_price <= 0:
                continue
            rows.append(
                {
                    "opportunity_id": item.opportunity_id,
                    "candidate_version": preregistration.candidate_version,
                    "symbol": item.symbol,
                    "direction": direction,
                    "decision_time_ms": item.decision_time_ms,
                    "candle_close_time_ms": item.decision_time_ms,
                    "higher_timeframe_time_ms": item.decision_time_ms - 3_600_000,
                    "entry_open_time_ms": entry_time,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "funding_return": item.signal_funding_return_3 or 0.0,
                    "candle_closed": True,
                }
            )
    return rows


def _direction_receipts(
    opportunities: list[Opportunity], preregistration: DirectionalPreregistration
) -> tuple[DirectionalReceipt, ...]:
    grouped: dict[str, list[Opportunity]] = defaultdict(list)
    for item in opportunities:
        if item.analysis_eligible_3:
            grouped[item.direction].append(item)
    receipts: list[DirectionalReceipt] = []
    for direction in (Direction.LONG, Direction.SHORT):
        values = grouped[direction.value]
        total = sum(1 for item in opportunities if item.direction == direction.value)
        censor_fraction = 1.0 if total == 0 else 1.0 - len(values) / total
        symbols = {item.symbol for item in values}
        regimes = {item.regime for item in values}
        receipts.append(
            DirectionalReceipt(
                direction=direction,
                candidate_version=preregistration.candidate_version,
                config_sha256=preregistration.config_sha256,
                sample_count=len(values),
                symbol_count=len(symbols),
                regime_count=len(regimes),
                censor_fraction=censor_fraction,
                data_quality_passed=all(
                    not item.analysis_exclusion_3.startswith("data_gap")
                    for item in values
                ),
                operational_health_passed=True,
                independent_review_passed=False,
            )
        )
    return tuple(receipts)


def run_directional_validation(
    settings: Settings,
    preregistration_path: str | Path,
    base_spec_path: str | Path,
    data_dir: str | Path,
    output_dir: str | Path,
    *,
    workspace_root: str | Path,
) -> dict[str, Any]:
    """Run fixture replay and historical screening for both Futures directions."""

    preregistration = load_preregistration(preregistration_path)
    loaded_spec = load_backtest_spec(base_spec_path)
    spec = _base_spec_for_directional_research(loaded_spec, preregistration)
    source = freeze_source(workspace_root)
    data_root = Path(data_dir)
    output_root = Path(output_dir)
    start_ms = int(spec.data_start.timestamp() * 1000)
    end_ms = int(spec.evaluation_end.timestamp() * 1000) - 1
    candles_by_asset: dict[str, list[Any]] = {}
    input_paths: dict[str, Path] = {}
    funding_by_asset: dict[str, list[Any]] = {}
    for asset in spec.assets:
        kline = dataset_path(
            data_root, Market.FUTURES, asset.asset, asset.futures_symbol, spec.interval
        )
        manifest_path = kline.with_suffix(kline.suffix + ".manifest.json")
        verify_dataset_manifest(kline, manifest_path)
        manifest = read_dataset_manifest(manifest_path)
        computed = build_dataset_manifest(kline)
        if computed != manifest:
            raise ValueError(f"dataset manifest changed or is non-canonical: {kline}")
        input_paths[f"futures/{kline.name}"] = kline
        input_paths[f"futures/{manifest_path.name}"] = manifest_path
        candles_by_asset[asset.asset] = list(read_kline_csv(kline).candles)
        funding = funding_path(data_root, asset.asset, asset.futures_symbol, spec.interval)
        verify_funding_dataset(
            funding,
            expected_symbol=asset.futures_symbol,
            expected_start_time_ms=start_ms,
            expected_end_time_ms=end_ms,
        )
        input_paths[f"funding/{funding.name}"] = funding
        funding_by_asset[asset.asset] = list(
            verify_funding_dataset(
                funding,
                expected_symbol=asset.futures_symbol,
                expected_start_time_ms=start_ms,
                expected_end_time_ms=end_ms,
            ).rates
        )
    data_authority_sha256, data_authority_payload = _data_authority(
        preregistration=preregistration,
        spec=spec,
        paths=input_paths,
    )
    universe_manifest_sha256 = canonical_sha256(
        [item.model_dump(mode="json") for item in preregistration.universe]
    )
    trial_registry_sha256 = canonical_sha256(
        {
            "candidate_version": preregistration.candidate_version,
            "splits": [item.model_dump(mode="json") for item in preregistration.splits],
            "horizons_bars": preregistration.outcome_horizons_bars,
            "cost_bps": preregistration.research_round_trip_cost_bps,
        }
    )
    research_manifest = build_research_manifest(
        preregistration,
        source_identity=source.source_identity,
        data_authority_sha256=data_authority_sha256,
        universe_manifest_sha256=universe_manifest_sha256,
        trial_registry_sha256=trial_registry_sha256,
    )
    regimes = build_market_regimes(candles_by_asset)
    backtester = ResearchBacktester(settings, spec)
    opportunities: list[Opportunity] = []
    symbol_summaries: list[dict[str, Any]] = []
    for asset in spec.assets:
        run = backtester.run_symbol(
            asset,
            Market.FUTURES,
            candles_by_asset[asset.asset],
            regimes[asset.asset],
            funding_by_asset[asset.asset],
        )
        opportunities.extend(run.opportunities)
        symbol_summaries.append(
            {
                "asset": run.asset,
                "symbol": run.symbol,
                "candles": run.candles,
                "evaluated_bars": run.evaluated_bars,
                "candidate_setups": run.candidate_setups,
                "confirmed_signals": run.confirmed_signals,
                "scheduled_entries": run.scheduled_entries,
                "opportunities": len(run.opportunities),
            }
        )
    fixture_payload = _fixture_rows(opportunities, preregistration)
    from signalbot.prospective.directional_candidates import FixtureReplayRow

    fixture_receipt = replay_fixture(
        [FixtureReplayRow.model_validate(item) for item in fixture_payload],
        preregistration=preregistration,
    )
    direction_receipts = _direction_receipts(opportunities, preregistration)
    historical_pass = all(
        item.sample_count >= preregistration.minimum_evidence.min_samples_per_direction
        and item.symbol_count >= preregistration.minimum_evidence.min_symbols_per_direction
        and item.regime_count >= preregistration.minimum_evidence.min_regimes_per_direction
        and item.censor_fraction <= preregistration.minimum_evidence.max_censor_fraction
        and item.data_quality_passed
        for item in direction_receipts
    )
    promotion_evidence = PromotionEvidence(
        candidate_version=preregistration.candidate_version,
        config_sha256=preregistration.config_sha256,
        data_authority_sha256=data_authority_sha256,
        trial_registry_sha256=trial_registry_sha256,
        authority_released=False,
        historical_screen=(
            HistoricalScreenStatus.PASS
            if historical_pass
            else HistoricalScreenStatus.NO_QUALIFIED_CANDIDATE
        ),
        prospective_status=ProspectiveStatus.NOT_STARTED,
        independent_review_passed=False,
        directions=direction_receipts,
    )
    output_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "source-freeze.json", source.as_dict())
    _write_json(output_root / "data-authority.json", data_authority_payload)
    _write_json(output_root / "fixture-replay.json", fixture_receipt.model_dump(mode="json"))
    _write_json(
        output_root / "historical-summary.json",
        {
            "protocol_version": _DIRECTIONAL_PROTOCOL,
            "candidate_version": preregistration.candidate_version,
            "config_sha256": preregistration.config_sha256,
            "data_authority_sha256": data_authority_sha256,
            "source_identity": source.source_identity,
            "historical_screen": promotion_evidence.historical_screen.value,
            "direction_receipts": [item.model_dump(mode="json") for item in direction_receipts],
            "symbols": symbol_summaries,
            "opportunity_count": len(opportunities),
            "outcome_counts_by_direction": dict(
                Counter(item.direction for item in opportunities if item.analysis_eligible_3)
            ),
            "bbo_status": preregistration.historical_bbo_status,
        },
    )
    _write_json(output_root / "promotion-evidence.json", promotion_evidence.model_dump(mode="json"))
    manifest_payload = {
        "protocol_version": _DIRECTIONAL_PROTOCOL,
        "candidate_version": preregistration.candidate_version,
        "config_sha256": preregistration.config_sha256,
        "source_identity": source.source_identity,
        "data_authority_sha256": data_authority_sha256,
        "universe_manifest_sha256": universe_manifest_sha256,
        "trial_registry_sha256": trial_registry_sha256,
        "research_manifest_sha256": research_manifest.manifest_sha256(),
        "fixture_replay_sha256": fixture_receipt.replay_sha256,
        "outputs": {
            path.name: _sha256_file(path)
            for path in sorted(output_root.glob("*.json"))
            if path.name not in {"validation-manifest.json", "independent-review.json"}
        },
        "created_at_utc": datetime.now(UTC).isoformat(),
        "freqtrade": {
            "status": "NOT_INSTALLED",
            "sidecar_config": "integrations/freqtrade/config-futures-validation-static.json",
        },
        "forward_shadow": {
            "status": "NOT_STARTED",
            "reason": "requires a future live activation boundary",
        },
        "independent_review": {"status": "NOT_STARTED"},
    }
    _write_json(output_root / "validation-manifest.json", manifest_payload)
    return {
        "status": "PASS" if historical_pass else "NO_QUALIFIED_CANDIDATE",
        "output_dir": str(output_root),
        "source_identity": source.source_identity,
        "data_authority_sha256": data_authority_sha256,
        "fixture_replay_sha256": fixture_receipt.replay_sha256,
        "historical_screen": promotion_evidence.historical_screen.value,
        "direction_receipts": [item.model_dump(mode="json") for item in direction_receipts],
    }
