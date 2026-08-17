from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from signalbot.backtest.dataset import (
    KlineDataset,
    KlineDatasetRequest,
    build_dataset_manifest,
    sha256_file,
    write_dataset_manifest,
    write_kline_csv,
)
from signalbot.backtest.downstream_code_freeze import create_downstream_code_freeze_v1
from signalbot.backtest.engine import FundingRate
from signalbot.backtest.funding import FundingDataset, funding_sha256, write_funding_csv
from signalbot.backtest.historical_three_family_census import (
    HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
    HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
    HistoricalConsensusCensusRowV2,
    _consensus_csv_bytes_v2,
)
from signalbot.backtest.historical_three_family_outcomes import (
    HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,
    HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2,
    HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_PROTOCOL_V2,
    HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2,
    HistoricalFixedHorizonExclusionV2,
    HistoricalFundingFileBindingV2,
    HistoricalThreeFamilyFixedHorizonErrorV2,
    _fixed_horizon_csv_bytes,
    canonical_historical_funding_authority_manifest_v2,
    evaluate_historical_fixed_horizons_v2,
    historical_return_to_micros_v2,
    historical_three_family_split_bounds_v2,
    load_authenticated_historical_consensus_v2,
    load_authenticated_historical_fixed_horizon_artifacts_v2,
    load_authenticated_historical_funding_authority_v2,
    run_historical_three_family_fixed_horizons_v2,
    summarize_historical_fixed_horizon_cost_components_v2,
)
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.research.historical_three_family_bootstrap import (
    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
)
from signalbot.r4b_v2.research.historical_three_family_outcome_audit import (
    HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
)
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    build_historical_execution_contract_v2,
)

_EXPERIMENT_SHA = "1" * 64
_TOPOLOGY_AMENDMENT_SHA = "3" * 64
_CENSUS_RESULT_SHA = "2" * 64
_SPLIT_START_MS = 1_719_792_000_000
_DECISION_OPEN_MS = _SPLIT_START_MS + 100 * FIVE_MINUTE_MS_V2
_DECISION_TIME_MS = _DECISION_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
_EXECUTION_SHA = build_historical_execution_contract_v2().execution_contract_sha256
_FILES = {
    "BONK": "BONK__1000BONKUSDT__5m.csv.gz",
    "ENA": "ENA__ENAUSDT__5m.csv.gz",
    "WIF": "WIF__WIFUSDT__5m.csv.gz",
    "FLOKI": "FLOKI__1000FLOKIUSDT__5m.csv.gz",
    "ARB": "ARB__ARBUSDT__5m.csv.gz",
    "OP": "OP__OPUSDT__5m.csv.gz",
    "SEI": "SEI__SEIUSDT__5m.csv.gz",
}
_SYMBOLS = {
    "BONK": "1000BONKUSDT",
    "ENA": "ENAUSDT",
    "WIF": "WIFUSDT",
    "FLOKI": "1000FLOKIUSDT",
    "ARB": "ARBUSDT",
    "OP": "OPUSDT",
    "SEI": "SEIUSDT",
}


def _candle(symbol: str, open_time_ms: int, price: str) -> Candle:
    value = Decimal(price)
    return Candle(
        market=Market.FUTURES,
        symbol=symbol,
        interval="5m",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + FIVE_MINUTE_MS_V2 - 1,
        open=value,
        high=value,
        low=value,
        close=value,
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=10,
        taker_buy_base_volume=Decimal("5"),
        taker_buy_quote_volume=Decimal("500"),
        is_closed=True,
    )


def _dataset(
    candles: tuple[Candle, ...],
    *,
    asset: str = "BONK",
    symbol: str = "1000BONKUSDT",
) -> KlineDataset:
    return KlineDataset(
        request=KlineDatasetRequest(
            market=Market.FUTURES,
            symbol=symbol,
            alias=asset,
            interval="5m",
            start_time_ms=candles[0].open_time_ms,
            end_time_ms=candles[-1].close_time_ms,
        ),
        candles=candles,
    )


def _full_dataset(*, rising: bool = True) -> KlineDataset:
    candles = tuple(
        _candle(
            "1000BONKUSDT",
            _DECISION_OPEN_MS + index * FIVE_MINUTE_MS_V2,
            str(Decimal("100") + (Decimal(index) if rising else -Decimal(index))),
        )
        for index in range(73)
    )
    return _dataset(candles)


def _event(
    *,
    direction: Direction = Direction.LONG,
    state_class: str | None = None,
    agreement: int | None = None,
    decision_time_ms: int = _DECISION_TIME_MS,
    decision_price: str = "100",
):
    from signalbot.backtest.historical_three_family_outcomes import (
        HistoricalConsensusOutcomeEventV2,
    )
    from signalbot.r4b_v2.strategy.directional_evidence import DirectionalStateClassV2

    if state_class is None:
        state_class = (
            DirectionalStateClassV2.BROAD_BULLISH_STATE.value
            if direction is Direction.LONG
            else DirectionalStateClassV2.BROAD_BEARISH_STATE.value
        )
    if agreement is None:
        agreement = 800_000 if direction is Direction.LONG else -800_000
    return HistoricalConsensusOutcomeEventV2(
        split="development",
        asset="BONK",
        symbol="1000BONKUSDT",
        event_id="a" * 64,
        anchor_sha256="b" * 64,
        primary_family=(
            SignalFamily.PULLBACK_LONG
            if direction is Direction.LONG
            else SignalFamily.PULLBACK_SHORT
        ),
        primary_direction=direction,
        decision_time_ms=decision_time_ms,
        decision_price=Decimal(decision_price),
        invalidation=(Decimal("90") if direction is Direction.LONG else Decimal("110")),
        atr=Decimal("5"),
        state_class=DirectionalStateClassV2(state_class),
        directional_agreement_micros=agreement,
        execution_contract_sha256=_EXECUTION_SHA,
    )


def _consensus_row(
    *,
    signs: tuple[int, int, int] = (1, 1, 1),
    event_seed: str = "a",
) -> HistoricalConsensusCensusRowV2:
    bullish = signs.count(1)
    bearish = signs.count(-1)
    neutral = signs.count(0)
    topology_by_counts = {
        (3, 0, 0): "UNANIMOUS_BULLISH_3_0_0",
        (2, 0, 1): "CLEAN_BULLISH_2_0_1",
        (2, 1, 0): "CONFLICTED_BULLISH_2_1_0",
    }
    topology = topology_by_counts[(bullish, bearish, neutral)]
    broad = topology.startswith("UNANIMOUS")
    clean = topology.startswith("CLEAN")
    conflicted = topology.startswith("CONFLICTED")
    admitted = broad or clean
    state_class = (
        "BROAD_BULLISH_STATE"
        if broad
        else "BULLISH_STATE_TILT"
        if clean
        else "MIXED_OR_NEUTRAL_STATE"
    )
    bucket = "BROAD_3_OF_3" if broad else "CLEAN_2_PLUS_NEUTRAL" if clean else "CONFLICTED_2_VS_1"
    grade = (
        "UNANIMOUS_BREADTH_UNCALIBRATED"
        if broad
        else "CLEAN_TWO_FAMILY_BREADTH_UNCALIBRATED"
        if clean
        else "CONFLICTED_MAJORITY_UNCALIBRATED"
    )
    return HistoricalConsensusCensusRowV2(
        split="development",
        asset="BONK",
        symbol="1000BONKUSDT",
        source_event_id="1" * 24,
        source_row_sha256="2" * 64,
        source_replay_manifest_sha256="3" * 64,
        anchor_sha256="b" * 64,
        primary_family="pullback_long",
        primary_direction="long",
        decision_time_ms=_DECISION_TIME_MS,
        price=Decimal("100"),
        invalidation=Decimal("90"),
        atr=Decimal("5"),
        event_id=event_seed * 64,
        payload_sha256="4" * 64,
        canonical_consensus_sha256="5" * 64,
        topology_sha256="6" * 64,
        canonical_topology_sha256="7" * 64,
        topology_contract_sha256=_TOPOLOGY_AMENDMENT_SHA,
        topology_rule_version=HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        topology_class=topology,
        topology_comparison_bucket=bucket,
        topology_display_grade=grade,
        topology_majority_direction="BULLISH",
        topology_majority_family_count=bullish,
        topology_opposing_family_count=bearish,
        topology_has_opposition=bearish > 0,
        topology_primary_support_count=bullish,
        topology_primary_oppose_count=bearish,
        topology_primary_neutral_count=neutral,
        clean_primary_audit_eligible=admitted,
        conflicted_comparator_eligible=conflicted,
        conflicted_comparator_outcome_authorized=False,
        rule_version=HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        status="READY",
        state_class=state_class,
        directional_numerator_micros=sum(signs) * 500_000,
        directional_denominator=3,
        directional_agreement_micros=(500_000 if broad else 333_333 if clean else 166_667),
        bullish_family_count=bullish,
        bearish_family_count=bearish,
        neutral_family_count=neutral,
        primary_relationship=("SUPPORTS_PRIMARY" if admitted else "MIXED_OR_NEUTRAL"),
        admitted=admitted,
        price_status="READY",
        price_direction=signs[0],
        price_strength_micros=500_000,
        price_calculation_sha256="8" * 64,
        price_source_slice_sha256="9" * 64,
        participation_status="READY",
        participation_direction=signs[1],
        participation_strength_micros=500_000,
        participation_calculation_sha256="c" * 64,
        participation_source_slice_sha256="d" * 64,
        cross_section_status="READY",
        cross_section_direction=signs[2],
        cross_section_strength_micros=500_000,
        cross_section_calculation_sha256="e" * 64,
        cross_section_source_slice_sha256="f" * 64,
        execution_contract_sha256=_EXECUTION_SHA,
        zero_move_round_trip_cost_micros=2_600,
        atr_fraction_micros=50_000,
        one_atr_cost_headroom_micros=47_400,
        cross_peer_set_root_sha256="0" * 64,
        cross_peer_input_sha256="1" * 64,
        reasons=("HISTORICAL_ONLY", "NOT_A_PROBABILITY"),
    )


def _dummy_panel_hashes() -> tuple[dict[str, str], dict[str, str]]:
    data = {
        f"futures/{filename}": format(index + 1, "x") * 64
        for index, filename in enumerate(_FILES.values())
    }
    manifests = {
        f"{path}.manifest.json": format(index + 8, "x") * 64 for index, path in enumerate(data)
    }
    return data, manifests


def _census_manifest_bytes(
    consensus_raw: bytes,
    *,
    data_hashes: dict[str, str] | None = None,
    manifest_hashes: dict[str, str] | None = None,
    complete: bool = True,
) -> bytes:
    if data_hashes is None or manifest_hashes is None:
        data_hashes, manifest_hashes = _dummy_panel_hashes()
    return canonical_json_line(
        {
            "anchor_set_sha256": "f" * 64,
            "census_complete": complete,
            "conflicted_comparator_outcome_authorized": False,
            "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
            "diagnostic_mode": not complete,
            "execution_contract_sha256": _EXECUTION_SHA,
            "experiment_contract_sha256": _EXPERIMENT_SHA,
            "historical_only": True,
            "historical_receipt_policy": "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME",
            "inputs": {
                "futures_data_sha256": data_hashes,
                "futures_manifest_sha256": manifest_hashes,
                "recommendations_sha256": {},
                "run_manifest_sha256": {},
            },
            "maximum_anchors": None if complete else 1,
            "outcome_data_read": False,
            "outputs": {
                "consensus.csv": _sha(consensus_raw),
                "results.json": _CENSUS_RESULT_SHA,
            },
            "probability": False,
            "promoting": False,
            "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
            "schema_version": HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
            "source_representation": "CANONICAL_NUMERIC_CALCULATION",
            "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
            "topology_contract_sha256": _TOPOLOGY_AMENDMENT_SHA,
            "v1a_fitted_selection_used": False,
        }
    )


def _write_consensus_authority(
    root: Path,
    rows: tuple[HistoricalConsensusCensusRowV2, ...],
    *,
    data_hashes: dict[str, str] | None = None,
    manifest_hashes: dict[str, str] | None = None,
    complete: bool = True,
) -> tuple[Path, Path, str]:
    root.mkdir(parents=True, exist_ok=True)
    consensus_raw = _consensus_csv_bytes_v2(rows)
    manifest_raw = _census_manifest_bytes(
        consensus_raw,
        data_hashes=data_hashes,
        manifest_hashes=manifest_hashes,
        complete=complete,
    )
    consensus_path = root / "consensus.csv"
    manifest_path = root / "manifest.json"
    consensus_path.write_bytes(consensus_raw)
    manifest_path.write_bytes(manifest_raw)
    return consensus_path, manifest_path, _sha(manifest_raw)


def _funding(*rates: FundingRate, start: int | None = None, end: int | None = None):
    return FundingDataset(
        "1000BONKUSDT",
        _DECISION_TIME_MS + 1 if start is None else start,
        _DECISION_TIME_MS + 72 * FIVE_MINUTE_MS_V2 if end is None else end,
        rates,
    )


def _sha(value: bytes) -> str:
    import hashlib

    return hashlib.sha256(value).hexdigest()


def test_consensus_loader_requires_external_manifest_hash_and_selects_only_clean_v1(
    tmp_path: Path,
) -> None:
    broad = _consensus_row()
    conflicted = replace(
        _consensus_row(signs=(1, 1, -1), event_seed="d"),
        decision_time_ms=_DECISION_TIME_MS + FIVE_MINUTE_MS_V2,
        anchor_sha256="e" * 64,
    )
    consensus, manifest, manifest_sha = _write_consensus_authority(tmp_path, (broad, conflicted))

    loaded = load_authenticated_historical_consensus_v2(
        consensus,
        manifest,
        expected_census_manifest_sha256=manifest_sha,
        expected_experiment_contract_sha256=_EXPERIMENT_SHA,
        expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
    )

    assert loaded.census_rows == 2
    assert len(loaded.events) == 1
    assert loaded.events[0].event_id == broad.event_id
    assert loaded.events[0].topology_version == HISTORICAL_THREE_FAMILY_PRIMARY_TOPOLOGY_V2
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="externally frozen"):
        load_authenticated_historical_consensus_v2(
            consensus,
            manifest,
            expected_census_manifest_sha256="0" * 64,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
        )
    with pytest.raises(
        HistoricalThreeFamilyFixedHorizonErrorV2,
        match="topology_contract_sha256",
    ):
        load_authenticated_historical_consensus_v2(
            consensus,
            manifest,
            expected_census_manifest_sha256=manifest_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256="0" * 64,
        )


def test_consensus_loader_rejects_diagnostic_or_tampered_consensus(
    tmp_path: Path,
) -> None:
    consensus, manifest, manifest_sha = _write_consensus_authority(
        tmp_path / "diagnostic", (_consensus_row(),), complete=False
    )
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="census_complete"):
        load_authenticated_historical_consensus_v2(
            consensus,
            manifest,
            expected_census_manifest_sha256=manifest_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
        )

    consensus, manifest, manifest_sha = _write_consensus_authority(
        tmp_path / "tampered", (_consensus_row(),)
    )
    consensus.write_bytes(consensus.read_bytes() + b"\n")
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match=r"consensus\.csv SHA"):
        load_authenticated_historical_consensus_v2(
            consensus,
            manifest,
            expected_census_manifest_sha256=manifest_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
        )


def test_conflicted_majority_cannot_be_silently_authorized_or_pooled(tmp_path: Path) -> None:
    conflicted = _consensus_row(signs=(1, 1, -1))
    illegally_admitted = replace(
        conflicted,
        admitted=True,
        clean_primary_audit_eligible=True,
        primary_relationship="SUPPORTS_PRIMARY",
    )
    consensus, manifest, manifest_sha = _write_consensus_authority(
        tmp_path / "pool", (illegally_admitted,)
    )
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="mixed/withheld"):
        load_authenticated_historical_consensus_v2(
            consensus,
            manifest,
            expected_census_manifest_sha256=manifest_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
        )

    authorized = replace(conflicted, conflicted_comparator_outcome_authorized=True)
    consensus, manifest, manifest_sha = _write_consensus_authority(
        tmp_path / "authorized", (authorized,)
    )
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="separate frozen adapter"):
        load_authenticated_historical_consensus_v2(
            consensus,
            manifest,
            expected_census_manifest_sha256=manifest_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
        )


def test_fixed_horizons_use_next_open_costs_and_strictly_interior_funding() -> None:
    event = _event()
    dataset = _full_dataset()
    entry_ms = _DECISION_TIME_MS + 1
    first_exit_ms = entry_ms + FIVE_MINUTE_MS_V2 - 1
    funding = _funding(
        FundingRate(entry_ms, 0.001, 100.0),
        FundingRate(entry_ms + 1, 0.001, 100.0),
        FundingRate(first_exit_ms, 0.001, 100.0),
    )

    rows = evaluate_historical_fixed_horizons_v2(event, dataset, funding)

    assert tuple(row.horizon_bars for row in rows) == (1, 3, 6, 12, 72)
    first = rows[0]
    assert first.evaluable
    assert first.entry_price == Decimal("101")
    assert first.exit_price == Decimal("101")
    assert first.funding_event_count == 1
    assert first.funding_return_micros == -990
    assert first.total_cost_micros == 2_600
    assert first.net_return_micros == -3_590
    assert first.gross_directional_return_micros is not None
    assert first.total_cost_micros is not None
    assert first.funding_return_micros is not None
    assert (
        first.gross_directional_return_micros
        - first.total_cost_micros
        + first.funding_return_micros
        == first.net_return_micros
    )
    assert first.to_audit_outcome().net_return_micros == first.net_return_micros


def test_short_direction_sign_and_funding_are_applied_in_signal_direction() -> None:
    event = _event(direction=Direction.SHORT, agreement=-800_000)
    dataset = _full_dataset(rising=False)
    entry_ms = _DECISION_TIME_MS + 1
    funding = _funding(FundingRate(entry_ms + 1, 0.001, 99.0))

    first = evaluate_historical_fixed_horizons_v2(event, dataset, funding)[0]

    assert first.evaluable
    assert first.gross_directional_return_micros == 0
    assert first.funding_return_micros == 1_000
    assert first.net_return_micros == -1_600


def test_split_boundary_missing_bars_gap_and_funding_are_explicit_exclusions() -> None:
    full = _full_dataset()
    funding = _funding(FundingRate(_DECISION_TIME_MS + 2, 0.0, 100.0))

    missing_decision = _dataset(full.candles[1:])
    assert {
        row.exclusion_reason
        for row in evaluate_historical_fixed_horizons_v2(_event(), missing_decision, funding)
    } == {HistoricalFixedHorizonExclusionV2.MISSING_DECISION_BAR.value}

    missing_next = _dataset((full.candles[0], *full.candles[2:]))
    assert {
        row.exclusion_reason
        for row in evaluate_historical_fixed_horizons_v2(_event(), missing_next, funding)
    } == {HistoricalFixedHorizonExclusionV2.MISSING_NEXT_OPEN.value}

    internal_gap = _dataset((full.candles[0], full.candles[1], *full.candles[3:]))
    gap_rows = evaluate_historical_fixed_horizons_v2(_event(), internal_gap, funding)
    assert gap_rows[0].evaluable
    assert all(
        row.exclusion_reason == HistoricalFixedHorizonExclusionV2.DATA_GAP_IN_HORIZON
        for row in gap_rows[1:]
    )

    short_tail = _dataset(full.candles[:2])
    tail_rows = evaluate_historical_fixed_horizons_v2(_event(), short_tail, funding)
    assert tail_rows[0].evaluable
    assert all(
        row.exclusion_reason == HistoricalFixedHorizonExclusionV2.MISSING_HORIZON_CLOSE
        for row in tail_rows[1:]
    )

    no_funding = evaluate_historical_fixed_horizons_v2(_event(), full, None)
    assert all(
        row.exclusion_reason == HistoricalFixedHorizonExclusionV2.FUNDING_DATASET_UNAVAILABLE
        for row in no_funding
    )

    uncovered = _funding(
        FundingRate(_DECISION_TIME_MS + 2, 0.0, 100.0),
        start=_DECISION_TIME_MS + 2,
    )
    coverage = evaluate_historical_fixed_horizons_v2(_event(), full, uncovered)
    assert all(
        row.exclusion_reason == HistoricalFixedHorizonExclusionV2.FUNDING_COVERAGE_UNAVAILABLE
        for row in coverage
    )


def test_entry_at_split_end_is_excluded_before_missing_data() -> None:
    split_end_ms = 1_740_787_200_000
    decision_time = split_end_ms - 1
    decision_open = decision_time - FIVE_MINUTE_MS_V2 + 1
    dataset = _dataset((_candle("1000BONKUSDT", decision_open, "100"),))
    event = _event(decision_time_ms=decision_time)
    rows = evaluate_historical_fixed_horizons_v2(event, dataset, None)
    assert all(
        row.exclusion_reason == HistoricalFixedHorizonExclusionV2.SPLIT_BOUNDARY_ENTRY
        for row in rows
    )
    assert historical_three_family_split_bounds_v2("development") == (
        _SPLIT_START_MS,
        split_end_ms,
    )
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="unsupported"):
        historical_three_family_split_bounds_v2("future_split")


def test_outcome_csv_is_deterministic_and_preserves_cost_reconciliation() -> None:
    funding = _funding(FundingRate(_DECISION_TIME_MS + 2, 0.0, 100.0))
    rows = evaluate_historical_fixed_horizons_v2(_event(), _full_dataset(), funding)

    forward = _fixed_horizon_csv_bytes(rows)
    reverse = _fixed_horizon_csv_bytes(tuple(reversed(rows)))

    assert forward == reverse
    assert b"gross_directional_return_micros" in forward
    assert b"total_cost_micros" in forward
    assert b"\r" not in forward
    assert rows[0].total_cost_micros is not None
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="reconcile"):
        replace(rows[0], total_cost_micros=rows[0].total_cost_micros + 1)
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="no economic"):
        replace(
            rows[0],
            evaluable=False,
            exclusion_reason=HistoricalFixedHorizonExclusionV2.DATA_GAP_IN_HORIZON,
            net_return_micros=None,
        )


def test_cost_attribution_separates_directional_hit_from_after_cost_hit() -> None:
    funding = _funding(FundingRate(_DECISION_TIME_MS + 2, 0.0, 100.0))
    rows = evaluate_historical_fixed_horizons_v2(_event(), _full_dataset(), funding)
    cost_lost_hit = replace(
        rows[0],
        gross_directional_return_micros=1_000,
        slippage_return_micros=800,
        fee_return_micros=500,
        funding_return_micros=0,
        rounding_residual_micros=0,
        total_cost_micros=1_300,
        net_return_micros=-300,
    )

    summaries = summarize_historical_fixed_horizon_cost_components_v2((cost_lost_hit, *rows[1:]))
    cell = next(
        value
        for value in summaries
        if value.horizon_bars == 1
        and value.side == "BULLISH"
        and value.agreement_bucket == "BROAD_3_OF_3"
    )

    assert len(summaries) == 20
    assert cell.events == 1
    assert cell.evaluable == 1
    assert cell.gross_directional_strict_hits == 1
    assert cell.net_strict_hits == 0
    assert cell.gross_to_net_hit_loss_count == 1
    assert cell.gross_to_net_hit_loss_rate_micros == 1_000_000
    assert cell.mean_gross_directional_return_micros == 1_000
    assert cell.mean_slippage_return_micros == 800
    assert cell.mean_fee_return_micros == 500
    assert cell.mean_funding_return_micros == 0
    assert cell.mean_total_cost_micros == 1_300
    assert cell.mean_net_return_micros == -300
    empty = summaries[-1]
    assert empty.events == 0
    assert empty.evaluable == 0
    assert empty.mean_net_return_micros is None


def test_funding_authority_is_canonical_hash_bound_and_symbol_checked(
    tmp_path: Path,
) -> None:
    relative = "funding/BONK__1000BONKUSDT__5m.csv.gz"
    source = tmp_path / relative
    source.parent.mkdir(parents=True)
    write_funding_csv(_funding(FundingRate(_DECISION_TIME_MS + 2, 0.0, 100.0)), source)
    binding = HistoricalFundingFileBindingV2(
        symbol="1000BONKUSDT",
        relative_path=relative,
        sha256=funding_sha256(source),
    )
    raw = canonical_historical_funding_authority_manifest_v2((binding,))
    path = tmp_path / "funding-authority.json"
    path.write_bytes(raw)

    loaded = load_authenticated_historical_funding_authority_v2(
        path,
        expected_manifest_sha256=_sha(raw),
        data_root=tmp_path,
    )

    assert loaded.by_symbol()["1000BONKUSDT"].rates
    assert json.loads(raw)["protocol"] == HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_PROTOCOL_V2
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="frozen value"):
        load_authenticated_historical_funding_authority_v2(
            path,
            expected_manifest_sha256="0" * 64,
            data_root=tmp_path,
        )
    source.unlink()
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="cannot read"):
        load_authenticated_historical_funding_authority_v2(
            path,
            expected_manifest_sha256=_sha(raw),
            data_root=tmp_path,
        )
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="outside"):
        HistoricalFundingFileBindingV2(
            symbol="BTCUSDT",
            relative_path="funding/BTC.csv.gz",
            sha256="1" * 64,
        )


def _write_panel(data_root: Path) -> tuple[dict[str, str], dict[str, str]]:
    data_hashes: dict[str, str] = {}
    manifest_hashes: dict[str, str] = {}
    for asset, filename in _FILES.items():
        symbol = _SYMBOLS[asset]
        candles = (
            _full_dataset().candles
            if asset == "BONK"
            else (_candle(symbol, _DECISION_OPEN_MS, "100"),)
        )
        dataset = _dataset(candles, asset=asset, symbol=symbol)
        data_path = data_root / "futures" / filename
        write_kline_csv(dataset, data_path)
        dataset_manifest_path = data_path.with_name(f"{data_path.name}.manifest.json")
        write_dataset_manifest(build_dataset_manifest(data_path), dataset_manifest_path)
        relative_data = data_path.relative_to(data_root).as_posix()
        relative_manifest = dataset_manifest_path.relative_to(data_root).as_posix()
        data_hashes[relative_data] = sha256_file(data_path)
        manifest_hashes[relative_manifest] = sha256_file(dataset_manifest_path)
    return data_hashes, manifest_hashes


def test_end_to_end_runner_publishes_three_deterministic_artifacts_without_network(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    data_hashes, manifest_hashes = _write_panel(data_root)
    consensus, census_manifest, census_manifest_sha = _write_consensus_authority(
        tmp_path / "census",
        (_consensus_row(),),
        data_hashes=data_hashes,
        manifest_hashes=manifest_hashes,
    )
    funding_relative = "funding/BONK__1000BONKUSDT__5m.csv.gz"
    funding_path = data_root / funding_relative
    write_funding_csv(_funding(FundingRate(_DECISION_TIME_MS + 2, 0.0, 100.0)), funding_path)
    funding_binding = HistoricalFundingFileBindingV2(
        symbol="1000BONKUSDT",
        relative_path=funding_relative,
        sha256=funding_sha256(funding_path),
    )
    funding_manifest_raw = canonical_historical_funding_authority_manifest_v2((funding_binding,))
    funding_manifest = tmp_path / "funding-authority.json"
    funding_manifest.write_bytes(funding_manifest_raw)
    funding_manifest_sha = _sha(funding_manifest_raw)
    (tmp_path / "frozen.py").write_text("VALUE = 1\n", encoding="utf-8")
    downstream_freeze = create_downstream_code_freeze_v1(
        workspace_root=tmp_path,
        manifest_path="freeze/manifest.json",
        purpose="unit test downstream outcome authority",
        include_trees=(),
        include_files=("frozen.py",),
        upstream_sha256={
            "bootstrap_schedule": HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
            "census_artifact_manifest": census_manifest_sha,
            "census_code_freeze": HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,
            "experiment_contract": _EXPERIMENT_SHA,
            "funding_authority": funding_manifest_sha,
            "topology_amendment": _TOPOLOGY_AMENDMENT_SHA,
        },
    )

    first = run_historical_three_family_fixed_horizons_v2(
        consensus_path=consensus,
        census_manifest_path=census_manifest,
        expected_census_manifest_sha256=census_manifest_sha,
        expected_experiment_contract_sha256=_EXPERIMENT_SHA,
        expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
        data_root=data_root,
        output_dir=tmp_path / "out-one",
        workspace_root=tmp_path,
        downstream_code_freeze_manifest_path=downstream_freeze.manifest_path,
        expected_downstream_code_freeze_manifest_sha256=(downstream_freeze.manifest_sha256),
        funding_authority_manifest_path=funding_manifest,
        expected_funding_authority_manifest_sha256=funding_manifest_sha,
    )
    second = run_historical_three_family_fixed_horizons_v2(
        consensus_path=consensus,
        census_manifest_path=census_manifest,
        expected_census_manifest_sha256=census_manifest_sha,
        expected_experiment_contract_sha256=_EXPERIMENT_SHA,
        expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
        data_root=data_root,
        output_dir=tmp_path / "out-two",
        workspace_root=tmp_path,
        downstream_code_freeze_manifest_path=downstream_freeze.manifest_path,
        expected_downstream_code_freeze_manifest_sha256=(downstream_freeze.manifest_sha256),
        funding_authority_manifest_path=funding_manifest,
        expected_funding_authority_manifest_sha256=funding_manifest_sha,
    )

    assert first.admitted_events == 1
    assert first.outcome_rows == 5
    assert second.outcomes_sha256 == first.outcomes_sha256
    assert second.results_sha256 == first.results_sha256
    assert second.manifest_sha256 == first.manifest_sha256
    assert {path.name for path in first.output_dir.iterdir()} == {
        "fixed_horizon_outcomes.csv",
        "results.json",
        "manifest.json",
    }
    results = json.loads((first.output_dir / "results.json").read_bytes())
    assert results["protocol"] == HISTORICAL_THREE_FAMILY_FIXED_HORIZON_RUNNER_PROTOCOL_V2
    assert results["outcome_rows"] == 5
    assert results["evaluable_outcomes"] == 5
    assert len(results["cost_attribution_summaries"]) == 20
    assert results["execution_contract"] == {
        "fee_bps_per_side": 5,
        "slippage_bps_per_side": 8,
        "zero_move_round_trip_cost_micros": 2_600,
    }
    assert results["funding_missing_is_zero"] is False
    assert results["downstream_code_freeze_manifest_sha256"] == downstream_freeze.manifest_sha256
    assert results["probability"] is False
    assert results["promoting"] is False

    loaded = load_authenticated_historical_fixed_horizon_artifacts_v2(
        first.output_dir,
        expected_manifest_sha256=first.manifest_sha256,
        expected_census_manifest_sha256=census_manifest_sha,
        expected_experiment_contract_sha256=_EXPERIMENT_SHA,
        expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
        expected_funding_authority_manifest_sha256=_sha(funding_manifest_raw),
        expected_downstream_code_freeze_manifest_sha256=(downstream_freeze.manifest_sha256),
    )
    assert loaded.rows == tuple(
        sorted(
            loaded.rows,
            key=lambda row: (
                row.split,
                row.asset,
                row.decision_time_ms,
                row.event_id,
                row.horizon_bars,
            ),
        )
    )
    assert loaded.admitted_events == 1
    assert loaded.downstream_code_freeze_manifest_sha256 == downstream_freeze.manifest_sha256
    assert len(loaded.rows) == 5

    (second.output_dir / "unexpected.txt").write_text("surplus", encoding="utf-8")
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="exact published"):
        load_authenticated_historical_fixed_horizon_artifacts_v2(
            second.output_dir,
            expected_manifest_sha256=second.manifest_sha256,
            expected_census_manifest_sha256=census_manifest_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
            expected_funding_authority_manifest_sha256=_sha(funding_manifest_raw),
            expected_downstream_code_freeze_manifest_sha256=(downstream_freeze.manifest_sha256),
        )

    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="fresh target"):
        run_historical_three_family_fixed_horizons_v2(
            consensus_path=consensus,
            census_manifest_path=census_manifest,
            expected_census_manifest_sha256=census_manifest_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_AMENDMENT_SHA,
            data_root=data_root,
            output_dir=first.output_dir,
            workspace_root=tmp_path,
            downstream_code_freeze_manifest_path=downstream_freeze.manifest_path,
            expected_downstream_code_freeze_manifest_sha256=(downstream_freeze.manifest_sha256),
            funding_authority_manifest_path=funding_manifest,
            expected_funding_authority_manifest_sha256=_sha(funding_manifest_raw),
        )


def test_five_rows_per_event_and_no_probability_or_order_claim() -> None:
    funding = _funding(FundingRate(_DECISION_TIME_MS + 2, 0.0, 100.0))
    rows = evaluate_historical_fixed_horizons_v2(_event(), _full_dataset(), funding)
    assert len(rows) == len(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2)
    assert all(row.historical_only for row in rows)
    assert all(not row.probability for row in rows)
    assert all(not row.probability_calibrated for row in rows)
    assert all(not row.promoting for row in rows)
    assert all(not row.order_placement for row in rows)


def test_shared_return_micros_rounding_is_half_away_and_rejects_nonfinite() -> None:
    assert historical_return_to_micros_v2(0.000_000_5) == 1
    assert historical_return_to_micros_v2(-0.000_000_5) == -1
    with pytest.raises(HistoricalThreeFamilyFixedHorizonErrorV2, match="finite"):
        historical_return_to_micros_v2(float("nan"))
