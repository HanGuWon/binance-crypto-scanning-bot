from __future__ import annotations

import hashlib
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

import pytest

from signalbot.backtest.dataset import KlineDataset, KlineDatasetRequest
from signalbot.backtest.downstream_code_freeze import create_downstream_code_freeze_v1
from signalbot.backtest.funding import FundingDataset
from signalbot.backtest.historical_three_family_conflicted_adapter import (
    HISTORICAL_THREE_FAMILY_CONFLICTED_TOPOLOGY_VERSION_V1,
    HistoricalConflictedComparatorEventV1,
)
from signalbot.backtest.historical_three_family_conflicted_outcomes import (
    HISTORICAL_CONFLICTED_FIXED_HORIZON_COST_SUMMARY_V1,
    HISTORICAL_CONFLICTED_FIXED_HORIZON_RUNNER_PROTOCOL_V1,
    HISTORICAL_CONFLICTED_FIXED_HORIZON_SCHEMA_VERSION_V1,
    HistoricalConflictedFixedHorizonErrorV1,
    _load_downstream_code_freeze,
    _outcomes_csv_bytes,
    _summaries,
    evaluate_historical_conflicted_fixed_horizons_v1,
    load_authenticated_historical_conflicted_fixed_horizon_artifacts_v1,
)
from signalbot.backtest.historical_three_family_outcomes import (
    HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,
    HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_PROTOCOL_V2,
    HistoricalConsensusOutcomeEventV2,
    evaluate_historical_fixed_horizons_v2,
)
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.research.historical_three_family_bootstrap import (
    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
    HistoricalThreeFamilyConflictedOutcomeV2,
    HistoricalThreeFamilyCostSourceV2,
    cost_attribution_from_fixed_horizon_row_v2,
)
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.directional_evidence import DirectionalStateClassV2
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    build_historical_execution_contract_v2,
)

_ROOT = Path(__file__).resolve().parents[2]
_SPLIT_START_MS = 1_719_792_000_000
_DECISION_OPEN_MS = _SPLIT_START_MS + 100 * FIVE_MINUTE_MS_V2
_DECISION_TIME_MS = _DECISION_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
_EXECUTION_SHA = build_historical_execution_contract_v2().execution_contract_sha256
_ADAPTER_MANIFEST_SHA = "a" * 64
_DOWNSTREAM_FREEZE_SHA = "b" * 64
_FUNDING_MANIFEST_SHA = "c" * 64
_CENSUS_MANIFEST_SHA = "d" * 64


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _candle(open_time_ms: int, price: Decimal) -> Candle:
    return Candle(
        market=Market.FUTURES,
        symbol="1000BONKUSDT",
        interval="5m",
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + FIVE_MINUTE_MS_V2 - 1,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=10,
        taker_buy_base_volume=Decimal("5"),
        taker_buy_quote_volume=Decimal("500"),
        is_closed=True,
    )


def _dataset(*, rising: bool = True, omit_index: int | None = None) -> KlineDataset:
    candles = tuple(
        _candle(
            _DECISION_OPEN_MS + index * FIVE_MINUTE_MS_V2,
            Decimal("100") + (Decimal(index) if rising else -Decimal(index)),
        )
        for index in range(73)
        if index != omit_index
    )
    return KlineDataset(
        request=KlineDatasetRequest(
            market=Market.FUTURES,
            symbol="1000BONKUSDT",
            alias="BONK",
            interval="5m",
            start_time_ms=candles[0].open_time_ms,
            end_time_ms=candles[-1].close_time_ms,
        ),
        candles=candles,
    )


def _conflicted_event(
    direction: Direction = Direction.LONG,
    *,
    agreement: int | None = None,
) -> HistoricalConflictedComparatorEventV1:
    long = direction is Direction.LONG
    return HistoricalConflictedComparatorEventV1(
        split="development",
        asset="BONK",
        symbol="1000BONKUSDT",
        event_id="1" * 64,
        anchor_sha256="2" * 64,
        source_event_id="source-event",
        source_row_sha256="3" * 64,
        source_replay_manifest_sha256="4" * 64,
        source_census_row_sha256="5" * 64,
        payload_sha256="6" * 64,
        canonical_consensus_sha256="7" * 64,
        topology_sha256="8" * 64,
        canonical_topology_sha256="9" * 64,
        topology_contract_sha256="a" * 64,
        topology_class=(
            "CONFLICTED_BULLISH_2_1_0" if long else "CONFLICTED_BEARISH_1_2_0"
        ),
        topology_majority_direction="BULLISH" if long else "BEARISH",
        primary_family="pullback_long" if long else "pullback_short",
        primary_direction=direction.value,
        decision_time_ms=_DECISION_TIME_MS,
        decision_price="100",
        invalidation="90" if long else "110",
        atr="5",
        directional_agreement_micros=(
            agreement if agreement is not None else (166_667 if long else -166_667)
        ),
        price_calculation_sha256="b" * 64,
        price_source_slice_sha256="c" * 64,
        participation_calculation_sha256="d" * 64,
        participation_source_slice_sha256="e" * 64,
        cross_section_calculation_sha256="f" * 64,
        cross_section_source_slice_sha256="0" * 64,
        cross_peer_set_root_sha256="1" * 64,
        cross_peer_input_sha256="2" * 64,
        execution_contract_sha256=_EXECUTION_SHA,
        census_manifest_sha256="3" * 64,
        experiment_contract_sha256="4" * 64,
        adapter_contract_sha256="5" * 64,
        code_freeze_manifest_sha256="6" * 64,
    )


def _clean_event(direction: Direction) -> HistoricalConsensusOutcomeEventV2:
    long = direction is Direction.LONG
    return HistoricalConsensusOutcomeEventV2(
        split="development",
        asset="BONK",
        symbol="1000BONKUSDT",
        event_id="7" * 64,
        anchor_sha256="8" * 64,
        primary_family=SignalFamily.PULLBACK_LONG if long else SignalFamily.PULLBACK_SHORT,
        primary_direction=direction,
        decision_time_ms=_DECISION_TIME_MS,
        decision_price=Decimal("100"),
        invalidation=Decimal("90") if long else Decimal("110"),
        atr=Decimal("5"),
        state_class=(
            DirectionalStateClassV2.BROAD_BULLISH_STATE
            if long
            else DirectionalStateClassV2.BROAD_BEARISH_STATE
        ),
        directional_agreement_micros=800_000 if long else -800_000,
        execution_contract_sha256=_EXECUTION_SHA,
    )


def _funding() -> FundingDataset:
    return FundingDataset(
        symbol="1000BONKUSDT",
        start_time_ms=_DECISION_TIME_MS + 1,
        end_time_ms=_DECISION_TIME_MS + 72 * FIVE_MINUTE_MS_V2,
        rates=(),
    )


@pytest.mark.parametrize(
    ("direction", "rising"),
    ((Direction.LONG, True), (Direction.SHORT, False)),
)
def test_conflicted_economics_match_existing_clean_owner_semantics(
    direction: Direction,
    rising: bool,
) -> None:
    dataset = _dataset(rising=rising)
    funding = _funding()
    conflicted = evaluate_historical_conflicted_fixed_horizons_v1(
        _conflicted_event(direction),
        dataset,
        funding,
        adapter_manifest_sha256=_ADAPTER_MANIFEST_SHA,
        downstream_freeze_manifest_sha256=_DOWNSTREAM_FREEZE_SHA,
    )
    clean = evaluate_historical_fixed_horizons_v2(
        _clean_event(direction), dataset, funding
    )
    assert len(conflicted) == len(clean) == 5
    for actual, owner in zip(conflicted, clean, strict=True):
        assert (
            actual.horizon_bars,
            actual.expected_entry_time_ms,
            actual.expected_exit_close_time_ms,
            actual.entry_price,
            actual.exit_price,
            actual.gross_directional_return_micros,
            actual.slippage_return_micros,
            actual.fee_return_micros,
            actual.funding_return_micros,
            actual.rounding_residual_micros,
            actual.total_cost_micros,
            actual.funding_event_count,
            actual.evaluable,
            actual.exclusion_reason,
            actual.net_return_micros,
        ) == (
            owner.horizon_bars,
            owner.expected_entry_time_ms,
            owner.expected_exit_close_time_ms,
            owner.entry_price,
            owner.exit_price,
            owner.gross_directional_return_micros,
            owner.slippage_return_micros,
            owner.fee_return_micros,
            owner.funding_return_micros,
            owner.rounding_residual_micros,
            owner.total_cost_micros,
            owner.funding_event_count,
            owner.evaluable,
            owner.exclusion_reason,
            owner.net_return_micros,
        )


def test_gap_and_missing_funding_exclusions_match_existing_owner() -> None:
    for dataset, funding in ((_dataset(omit_index=2), _funding()), (_dataset(), None)):
        conflicted = evaluate_historical_conflicted_fixed_horizons_v1(
            _conflicted_event(),
            dataset,
            funding,
            adapter_manifest_sha256=_ADAPTER_MANIFEST_SHA,
            downstream_freeze_manifest_sha256=_DOWNSTREAM_FREEZE_SHA,
        )
        clean = evaluate_historical_fixed_horizons_v2(
            _clean_event(Direction.LONG), dataset, funding
        )
        assert [(row.evaluable, row.exclusion_reason) for row in conflicted] == [
            (row.evaluable, row.exclusion_reason) for row in clean
        ]


@pytest.mark.parametrize(
    ("direction", "agreement"),
    (
        (Direction.LONG, 500_000),
        (Direction.LONG, -500_000),
        (Direction.LONG, 0),
        (Direction.SHORT, -500_000),
        (Direction.SHORT, 500_000),
        (Direction.SHORT, 0),
    ),
)
def test_consumer_and_loader_preserve_descriptive_agreement_without_changing_side(
    tmp_path: Path,
    direction: Direction,
    agreement: int,
) -> None:
    dataset = _dataset(rising=direction is Direction.LONG)
    rows = evaluate_historical_conflicted_fixed_horizons_v1(
        _conflicted_event(direction, agreement=agreement),
        dataset,
        _funding(),
        adapter_manifest_sha256=_ADAPTER_MANIFEST_SHA,
        downstream_freeze_manifest_sha256=_DOWNSTREAM_FREEZE_SHA,
    )
    clean = evaluate_historical_fixed_horizons_v2(
        _clean_event(direction), dataset, _funding()
    )
    assert all(row.primary_direction == direction.value for row in rows)
    assert all(row.directional_agreement_micros == agreement for row in rows)
    assert [row.net_return_micros for row in rows] == [
        row.net_return_micros for row in clean
    ]
    output, manifest_sha = _artifact_dir(
        tmp_path,
        direction=direction,
        agreement=agreement,
    )
    loaded = load_authenticated_historical_conflicted_fixed_horizon_artifacts_v1(
        output,
        expected_manifest_sha256=manifest_sha,
        expected_adapter_manifest_sha256=_ADAPTER_MANIFEST_SHA,
        expected_execution_contract_sha256=_EXECUTION_SHA,
        expected_downstream_code_freeze_manifest_sha256=_DOWNSTREAM_FREEZE_SHA,
        expected_funding_authority_manifest_sha256=_FUNDING_MANIFEST_SHA,
        expected_census_manifest_sha256=_CENSUS_MANIFEST_SHA,
    )
    assert all(row.primary_direction == direction.value for row in loaded.rows)
    assert all(row.directional_agreement_micros == agreement for row in loaded.rows)
    assert all(row.to_bootstrap_outcome().side.value == (
        "BULLISH" if direction is Direction.LONG else "BEARISH"
    ) for row in loaded.rows)


def test_rows_feed_existing_conflicted_bootstrap_and_cost_contracts() -> None:
    row = evaluate_historical_conflicted_fixed_horizons_v1(
        _conflicted_event(),
        _dataset(),
        _funding(),
        adapter_manifest_sha256=_ADAPTER_MANIFEST_SHA,
        downstream_freeze_manifest_sha256=_DOWNSTREAM_FREEZE_SHA,
    )[0]
    outcome = row.to_bootstrap_outcome()
    cost = cost_attribution_from_fixed_horizon_row_v2(
        row,
        source=HistoricalThreeFamilyCostSourceV2.CONFLICTED_COMPARATOR,
    )
    assert type(outcome) is HistoricalThreeFamilyConflictedOutcomeV2
    assert outcome.event_id == row.event_id
    assert cost.bucket.value == "CONFLICTED_2_VS_1"
    assert cost.net_return_micros == row.net_return_micros


def _artifact_dir(
    tmp_path: Path,
    *,
    direction: Direction = Direction.LONG,
    agreement: int | None = None,
) -> tuple[Path, str]:
    rows = evaluate_historical_conflicted_fixed_horizons_v1(
        _conflicted_event(direction, agreement=agreement),
        _dataset(rising=direction is Direction.LONG),
        _funding(),
        adapter_manifest_sha256=_ADAPTER_MANIFEST_SHA,
        downstream_freeze_manifest_sha256=_DOWNSTREAM_FREEZE_SHA,
    )
    outcomes = _outcomes_csv_bytes(rows)
    results = canonical_json_line(
        {
            "adapter_manifest_sha256": _ADAPTER_MANIFEST_SHA,
            "clean_population_pooled": False,
            "conflicted_comparator": True,
            "census_manifest_sha256": _CENSUS_MANIFEST_SHA,
            "cost_attribution": [asdict(value) for value in _summaries(rows)],
            "cost_summary_version": HISTORICAL_CONFLICTED_FIXED_HORIZON_COST_SUMMARY_V1,
            "downstream_freeze_manifest_sha256": _DOWNSTREAM_FREEZE_SHA,
            "event_count": 1,
            "execution_contract_sha256": _EXECUTION_SHA,
            "funding_authority_manifest_sha256": _FUNDING_MANIFEST_SHA,
            "historical_only": True,
            "horizons_bars": [1, 3, 6, 12, 72],
            "order_placement": False,
            "outcome_rows": 5,
            "outcomes_sha256": _sha(outcomes),
            "probability": False,
            "probability_calibrated": False,
            "promoting": False,
            "protocol": HISTORICAL_CONFLICTED_FIXED_HORIZON_RUNNER_PROTOCOL_V1,
            "schema_version": HISTORICAL_CONFLICTED_FIXED_HORIZON_SCHEMA_VERSION_V1,
        }
    )
    manifest = canonical_json_line(
        {
            "adapter_manifest_sha256": _ADAPTER_MANIFEST_SHA,
            "clean_population_pooled": False,
            "conflicted_comparator": True,
            "census_manifest_sha256": _CENSUS_MANIFEST_SHA,
            "downstream_freeze_manifest_sha256": _DOWNSTREAM_FREEZE_SHA,
            "execution_contract_sha256": _EXECUTION_SHA,
            "funding_authority_manifest_sha256": _FUNDING_MANIFEST_SHA,
            "funding_authority_protocol": (
                HISTORICAL_THREE_FAMILY_FUNDING_AUTHORITY_PROTOCOL_V2
            ),
            "historical_only": True,
            "kline_authority": [
                {
                    "asset": asset,
                    "data_sha256": format(index + 1, "x") * 64,
                    "manifest_sha256": format(index + 8, "x") * 64,
                    "relative_data_path": f"futures/{asset}.csv.gz",
                    "row_count": 100,
                    "symbol": f"{asset}USDT",
                }
                for index, asset in enumerate(("ARB", "BONK", "ENA", "FLOKI", "OP", "SEI", "WIF"))
            ],
            "order_placement": False,
            "outputs": {
                "fixed_horizon_outcomes.csv": _sha(outcomes),
                "results.json": _sha(results),
            },
            "probability": False,
            "probability_calibrated": False,
            "promoting": False,
            "protocol": HISTORICAL_CONFLICTED_FIXED_HORIZON_RUNNER_PROTOCOL_V1,
            "schema_version": HISTORICAL_CONFLICTED_FIXED_HORIZON_SCHEMA_VERSION_V1,
            "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
            "topology_version": HISTORICAL_THREE_FAMILY_CONFLICTED_TOPOLOGY_VERSION_V1,
        }
    )
    output = tmp_path / "outcomes"
    output.mkdir()
    (output / "fixed_horizon_outcomes.csv").write_bytes(outcomes)
    (output / "results.json").write_bytes(results)
    (output / "manifest.json").write_bytes(manifest)
    return output, _sha(manifest)


def test_public_loader_authenticates_exact_rows_for_downstream_analysis(tmp_path: Path) -> None:
    output, manifest_sha = _artifact_dir(tmp_path)
    loaded = load_authenticated_historical_conflicted_fixed_horizon_artifacts_v1(
        output,
        expected_manifest_sha256=manifest_sha,
        expected_adapter_manifest_sha256=_ADAPTER_MANIFEST_SHA,
        expected_execution_contract_sha256=_EXECUTION_SHA,
        expected_downstream_code_freeze_manifest_sha256=_DOWNSTREAM_FREEZE_SHA,
        expected_funding_authority_manifest_sha256=_FUNDING_MANIFEST_SHA,
        expected_census_manifest_sha256=_CENSUS_MANIFEST_SHA,
    )
    assert len(loaded.rows) == 5
    assert all(row.agreement_bucket == "CONFLICTED_2_VS_1" for row in loaded.rows)
    assert all(row.clean_population_pooled is False for row in loaded.rows)


def test_public_loader_rejects_tampered_output(tmp_path: Path) -> None:
    output, manifest_sha = _artifact_dir(tmp_path)
    with (output / "fixed_horizon_outcomes.csv").open("ab") as handle:
        handle.write(b"tamper\n")
    with pytest.raises(HistoricalConflictedFixedHorizonErrorV1, match="manifest hashes"):
        load_authenticated_historical_conflicted_fixed_horizon_artifacts_v1(
            output,
            expected_manifest_sha256=manifest_sha,
            expected_adapter_manifest_sha256=_ADAPTER_MANIFEST_SHA,
            expected_execution_contract_sha256=_EXECUTION_SHA,
            expected_downstream_code_freeze_manifest_sha256=_DOWNSTREAM_FREEZE_SHA,
            expected_funding_authority_manifest_sha256=_FUNDING_MANIFEST_SHA,
            expected_census_manifest_sha256=_CENSUS_MANIFEST_SHA,
        )


def test_consumer_accepts_the_same_generic_freeze_b_authority(tmp_path: Path) -> None:
    gate = tmp_path / "gate.py"
    gate.write_text("FROZEN = True\n", encoding="utf-8")
    bindings = {
        "adapter_code_freeze": "2" * 64,
        "bootstrap_schedule": HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
        "census_artifact_manifest": "1" * 64,
        "census_code_freeze": HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,
        "conflicted_adapter_contract": "5" * 64,
        "conflicted_adapter_manifest": "3" * 64,
        "experiment_contract": "6" * 64,
        "funding_authority": "4" * 64,
        "topology_amendment": "7" * 64,
    }
    frozen = create_downstream_code_freeze_v1(
        workspace_root=tmp_path,
        manifest_path="freeze-b.json",
        purpose="shared primary te0 conflicted freeze b",
        include_trees=(),
        include_files=("gate.py",),
        upstream_sha256=bindings,
    )
    loaded = _load_downstream_code_freeze(
        workspace_root=tmp_path,
        manifest_path=frozen.manifest_path,
        expected_manifest_sha256=frozen.manifest_sha256,
        census_manifest_sha256="1" * 64,
        experiment_contract_sha256="6" * 64,
        topology_amendment_sha256="7" * 64,
        adapter_code_freeze_manifest_sha256="2" * 64,
        adapter_manifest_sha256="3" * 64,
        adapter_contract_sha256="5" * 64,
        funding_authority_manifest_sha256="4" * 64,
    )
    assert loaded.manifest_sha256 == frozen.manifest_sha256
    assert loaded.upstream_sha256 == bindings
