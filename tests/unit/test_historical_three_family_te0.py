from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from conftest import make_feature
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
    HistoricalConsensusOutcomeEventV2,
    HistoricalFundingFileBindingV2,
    canonical_historical_funding_authority_manifest_v2,
)
from signalbot.backtest.historical_three_family_te0 import (
    HISTORICAL_THREE_FAMILY_TE0_RULE_V2,
    HistoricalThreeFamilyTe0ErrorV2,
    HistoricalThreeFamilyTe0ExclusionV2,
    _technical_exit_te0_csv_bytes_v2,
    build_historical_three_family_te0_features_v2,
    build_historical_three_family_te0_policy_v2,
    evaluate_historical_three_family_te0_v2,
    load_authenticated_historical_three_family_te0_artifacts_v2,
    run_historical_three_family_te0_v2,
)
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import Candle, FeatureSnapshot
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.research.historical_three_family_bootstrap import (
    HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
)
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.directional_evidence import DirectionalStateClassV2
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    build_historical_execution_contract_v2,
)

_SPLIT_START_MS = 1_719_792_000_000
_DECISION_OPEN_MS = _SPLIT_START_MS + 300 * FIVE_MINUTE_MS_V2
_DECISION_TIME_MS = _DECISION_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
_EXECUTION_SHA = build_historical_execution_contract_v2().execution_contract_sha256
_EXPERIMENT_SHA = "1" * 64
_TOPOLOGY_SHA = "3" * 64
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


def _candle(
    offset: int,
    *,
    open_price: str = "100",
    high: str = "101",
    low: str = "99",
    close: str = "100",
    open_time_ms: int | None = None,
) -> Candle:
    opening = _DECISION_OPEN_MS + offset * FIVE_MINUTE_MS_V2
    if open_time_ms is not None:
        opening = open_time_ms
    return Candle(
        market=Market.FUTURES,
        symbol="1000BONKUSDT",
        interval="5m",
        open_time_ms=opening,
        close_time_ms=opening + FIVE_MINUTE_MS_V2 - 1,
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("10"),
        quote_volume=Decimal("1000"),
        trade_count=10,
        taker_buy_base_volume=Decimal("5"),
        taker_buy_quote_volume=Decimal("500"),
        is_closed=True,
    )


def _dataset(candles: tuple[Candle, ...]) -> KlineDataset:
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


def _event(
    *,
    direction: Direction = Direction.LONG,
    invalidation: Decimal | None = Decimal("98"),
) -> HistoricalConsensusOutcomeEventV2:
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
        decision_time_ms=_DECISION_TIME_MS,
        decision_price=Decimal("100"),
        invalidation=invalidation,
        atr=Decimal("1"),
        state_class=(
            DirectionalStateClassV2.BROAD_BULLISH_STATE
            if direction is Direction.LONG
            else DirectionalStateClassV2.BROAD_BEARISH_STATE
        ),
        directional_agreement_micros=(800_000 if direction is Direction.LONG else -800_000),
        execution_contract_sha256=_EXECUTION_SHA,
    )


def _feature(
    candle: Candle,
    *,
    ema20: float = 100.0,
    macd_histogram: float = 0.1,
    atr: float = 1.0,
) -> FeatureSnapshot:
    return make_feature(
        market=Market.FUTURES,
        symbol=candle.symbol,
        interval=candle.interval,
        event_time_ms=candle.close_time_ms,
        price=float(candle.close),
        ema20=ema20,
        macd_histogram=macd_histogram,
        atr=atr,
    )


def _funding(
    *rates: FundingRate,
    start_ms: int | None = None,
    end_ms: int | None = None,
) -> FundingDataset:
    return FundingDataset(
        symbol="1000BONKUSDT",
        start_time_ms=_DECISION_TIME_MS + 1 if start_ms is None else start_ms,
        end_time_ms=(_DECISION_TIME_MS + 100 * FIVE_MINUTE_MS_V2 if end_ms is None else end_ms),
        rates=tuple(rates),
    )


def _consensus_row() -> HistoricalConsensusCensusRowV2:
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
        invalidation=Decimal("98"),
        atr=Decimal("1"),
        event_id="a" * 64,
        payload_sha256="4" * 64,
        canonical_consensus_sha256="5" * 64,
        topology_sha256="6" * 64,
        canonical_topology_sha256="7" * 64,
        topology_contract_sha256=_TOPOLOGY_SHA,
        topology_rule_version=HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        topology_class="UNANIMOUS_BULLISH_3_0_0",
        topology_comparison_bucket="BROAD_3_OF_3",
        topology_display_grade="UNANIMOUS_BREADTH_UNCALIBRATED",
        topology_majority_direction="BULLISH",
        topology_majority_family_count=3,
        topology_opposing_family_count=0,
        topology_has_opposition=False,
        topology_primary_support_count=3,
        topology_primary_oppose_count=0,
        topology_primary_neutral_count=0,
        clean_primary_audit_eligible=True,
        conflicted_comparator_eligible=False,
        conflicted_comparator_outcome_authorized=False,
        rule_version=HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        status="READY",
        state_class="BROAD_BULLISH_STATE",
        directional_numerator_micros=1_500_000,
        directional_denominator=3,
        directional_agreement_micros=500_000,
        bullish_family_count=3,
        bearish_family_count=0,
        neutral_family_count=0,
        primary_relationship="SUPPORTS_PRIMARY",
        admitted=True,
        price_status="READY",
        price_direction=1,
        price_strength_micros=500_000,
        price_calculation_sha256="8" * 64,
        price_source_slice_sha256="9" * 64,
        participation_status="READY",
        participation_direction=1,
        participation_strength_micros=500_000,
        participation_calculation_sha256="c" * 64,
        participation_source_slice_sha256="d" * 64,
        cross_section_status="READY",
        cross_section_direction=1,
        cross_section_strength_micros=500_000,
        cross_section_calculation_sha256="e" * 64,
        cross_section_source_slice_sha256="f" * 64,
        execution_contract_sha256=_EXECUTION_SHA,
        zero_move_round_trip_cost_micros=2_600,
        atr_fraction_micros=10_000,
        one_atr_cost_headroom_micros=7_400,
        cross_peer_set_root_sha256="0" * 64,
        cross_peer_input_sha256="1" * 64,
        reasons=("HISTORICAL_ONLY", "NOT_A_PROBABILITY"),
    )


def _write_panel(data_root: Path) -> tuple[dict[str, str], dict[str, str]]:
    data_hashes: dict[str, str] = {}
    manifest_hashes: dict[str, str] = {}
    for asset, filename in _FILES.items():
        symbol = _SYMBOLS[asset]
        if asset == "BONK":
            history = tuple(
                _candle(
                    offset,
                    open_price="100",
                    high="101",
                    low="99",
                    close="100",
                )
                for offset in range(-210, 1)
            )
            entry = _candle(1, open_price="100", high="101", low="97", close="99")
            candles = (*history, entry)
        else:
            candles = (_candle(0).model_copy(update={"symbol": symbol}),)
        dataset = KlineDataset(
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
        path = data_root / "futures" / filename
        write_kline_csv(dataset, path)
        manifest_path = path.with_name(f"{path.name}.manifest.json")
        write_dataset_manifest(build_dataset_manifest(path), manifest_path)
        relative_data = path.relative_to(data_root).as_posix()
        relative_manifest = manifest_path.relative_to(data_root).as_posix()
        data_hashes[relative_data] = sha256_file(path)
        manifest_hashes[relative_manifest] = sha256_file(manifest_path)
    return data_hashes, manifest_hashes


def _write_census_authority(
    root: Path,
    *,
    data_hashes: dict[str, str],
    manifest_hashes: dict[str, str],
) -> tuple[Path, Path, str]:
    root.mkdir(parents=True)
    consensus_raw = _consensus_csv_bytes_v2((_consensus_row(),))
    manifest_raw = canonical_json_line(
        {
            "anchor_set_sha256": "f" * 64,
            "census_complete": True,
            "conflicted_comparator_outcome_authorized": False,
            "consensus_rule_version": HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
            "diagnostic_mode": False,
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
            "maximum_anchors": None,
            "outcome_data_read": False,
            "outputs": {
                "consensus.csv": hashlib.sha256(consensus_raw).hexdigest(),
                "results.json": "2" * 64,
            },
            "probability": False,
            "promoting": False,
            "protocol": HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
            "schema_version": HISTORICAL_THREE_FAMILY_CENSUS_SCHEMA_VERSION_V2,
            "source_representation": "CANONICAL_NUMERIC_CALCULATION",
            "topology_contract_sha256": _TOPOLOGY_SHA,
            "topology_rule_version": HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
            "v1a_fitted_selection_used": False,
        }
    )
    consensus = root / "consensus.csv"
    manifest = root / "manifest.json"
    consensus.write_bytes(consensus_raw)
    manifest.write_bytes(manifest_raw)
    return consensus, manifest, hashlib.sha256(manifest_raw).hexdigest()


def test_te0_policy_is_exact_and_disables_opposite_signals_and_orders() -> None:
    policy = build_historical_three_family_te0_policy_v2()

    assert policy.rule == HISTORICAL_THREE_FAMILY_TE0_RULE_V2
    assert policy.trend_failure_bars == 3
    assert policy.trailing_activation_r == 1
    assert policy.trailing_atr_multiple == 2
    assert policy.max_holding_bars == 72
    assert not policy.opposite_signal_evaluated
    assert not policy.order_placement
    assert len(policy.policy_sha256) == 64


def test_te0_reuses_trailing_exit_and_reconciles_fee_slippage_and_funding() -> None:
    decision = _candle(0)
    first = _candle(1, open_price="100", high="103", low="99", close="102")
    second = _candle(2, open_price="102", high="103", low="100.5", close="101.5")
    dataset = _dataset((decision, first, second))
    funding_time = first.open_time_ms + 1

    row = evaluate_historical_three_family_te0_v2(
        _event(),
        dataset,
        (None, _feature(first), None),
        _funding(FundingRate(funding_time, 0.001, 100.0)),
    )

    assert row.evaluable
    assert row.exit_reason == "trailing_stop"
    assert row.entry_time_ms == first.open_time_ms
    assert row.exit_time_ms == second.close_time_ms
    assert row.exit_signal_observed_at_ms == second.close_time_ms
    assert row.initial_stop == Decimal("98.0")
    assert row.active_stop == Decimal("101.0")
    assert row.bars_held == 2
    assert row.funding_event_count == 1
    assert row.funding_return_micros == -1_000
    assert row.opposite_signal_evaluated is False
    assert row.order_placement is False
    assert row.gross_directional_return_micros is not None
    assert row.total_cost_micros is not None
    assert row.net_return_micros is not None
    assert row.funding_return_micros is not None
    assert (
        row.gross_directional_return_micros - row.total_cost_micros + row.funding_return_micros
        == row.net_return_micros
    )


def test_te0_requires_three_closed_trend_failures_then_fills_next_open() -> None:
    decision = _candle(0)
    first = _candle(1, open_price="100", high="101", low="98", close="99")
    second = _candle(2, open_price="99", high="100", low="98", close="98.5")
    third = _candle(3, open_price="98.5", high="99", low="97", close="98")
    exit_bar = _candle(4, open_price="97.75", high="98", low="97", close="97.5")
    dataset = _dataset((decision, first, second, third, exit_bar))
    failing = tuple(
        _feature(candle, ema20=100.0, macd_histogram=-0.1) for candle in (first, second, third)
    )

    row = evaluate_historical_three_family_te0_v2(
        replace(_event(), invalidation=Decimal("90")),
        dataset,
        (None, *failing, None),
        _funding(),
    )

    assert row.evaluable
    assert row.exit_reason == "trend_failure"
    assert row.bars_held == 3
    assert row.exit_signal_observed_at_ms == third.close_time_ms
    assert row.exit_time_ms == exit_bar.open_time_ms
    assert row.execution_model == "counterfactual_next_bar_open"


def test_te0_short_preserves_futures_exit_semantics_and_funding_sign() -> None:
    decision = _candle(0)
    entry = _candle(1, open_price="100", high="103", low="99", close="101")
    funding_time = entry.open_time_ms + 1

    row = evaluate_historical_three_family_te0_v2(
        _event(direction=Direction.SHORT, invalidation=Decimal("102")),
        _dataset((decision, entry)),
        (None, None),
        _funding(FundingRate(funding_time, 0.001, 100.0)),
    )

    assert row.evaluable
    assert row.exit_reason == "initial_stop"
    assert row.entry_action_label == "FUTURES_SHORT"
    assert row.exit_action_label == "FUTURES_SHORT_EXIT"
    assert row.initial_stop == Decimal("102")
    assert row.funding_return_micros == 1_000


def test_te0_maximum_72_completed_bars_exits_at_next_contiguous_open() -> None:
    decision = _candle(0)
    held = tuple(
        _candle(index, open_price="100", high="100.5", low="99.5", close="100")
        for index in range(1, 73)
    )
    exit_bar = _candle(73, open_price="100.25", high="101", low="100", close="100.5")
    dataset = _dataset((decision, *held, exit_bar))
    features = (
        None,
        *tuple(_feature(candle, ema20=99.0, macd_histogram=0.1) for candle in held),
        None,
    )

    row = evaluate_historical_three_family_te0_v2(
        replace(_event(), invalidation=Decimal("90")),
        dataset,
        features,
        _funding(),
    )

    assert row.evaluable
    assert row.exit_reason == "time_exit"
    assert row.bars_held == 72
    assert row.exit_signal_observed_at_ms == held[-1].close_time_ms
    assert row.exit_time_ms == exit_bar.open_time_ms


@pytest.mark.parametrize(
    ("direction", "invalidation", "expected"),
    [
        (
            Direction.LONG,
            None,
            HistoricalThreeFamilyTe0ExclusionV2.SOURCE_INVALIDATION_MISSING,
        ),
        (
            Direction.LONG,
            Decimal("100"),
            HistoricalThreeFamilyTe0ExclusionV2.SOURCE_INVALIDATION_WRONG_SIDE,
        ),
        (
            Direction.SHORT,
            Decimal("100"),
            HistoricalThreeFamilyTe0ExclusionV2.SOURCE_INVALIDATION_WRONG_SIDE,
        ),
    ],
)
def test_te0_explicitly_excludes_missing_or_wrong_side_source_stop(
    direction: Direction,
    invalidation: Decimal | None,
    expected: HistoricalThreeFamilyTe0ExclusionV2,
) -> None:
    decision = _candle(0)
    entry = _candle(1)

    row = evaluate_historical_three_family_te0_v2(
        _event(direction=direction, invalidation=invalidation),
        _dataset((decision, entry)),
        (None, _feature(entry)),
        _funding(),
    )

    assert not row.evaluable
    assert row.exclusion_reason == expected.value
    assert row.initial_stop is None
    assert row.net_return_micros is None


def test_te0_feature_mismatch_and_data_gap_are_explicit_exclusions() -> None:
    decision = _candle(0)
    first = _candle(1)
    bad_feature = _feature(first).model_copy(update={"symbol": "ENAUSDT"})
    mismatch = evaluate_historical_three_family_te0_v2(
        replace(_event(), invalidation=Decimal("90")),
        _dataset((decision, first)),
        (None, bad_feature),
        _funding(),
    )
    assert mismatch.exclusion_reason == HistoricalThreeFamilyTe0ExclusionV2.FEATURE_MISMATCH

    after_gap = _candle(3)
    gap = evaluate_historical_three_family_te0_v2(
        replace(_event(), invalidation=Decimal("90")),
        _dataset((decision, first, after_gap)),
        (None, _feature(first), _feature(after_gap)),
        _funding(),
    )
    assert gap.exclusion_reason == HistoricalThreeFamilyTe0ExclusionV2.DATA_GAP
    assert gap.observed_bars == 1
    assert gap.exclusion_expected_open_time_ms == first.open_time_ms + FIVE_MINUTE_MS_V2


@pytest.mark.parametrize(
    ("funding", "expected"),
    [
        (None, HistoricalThreeFamilyTe0ExclusionV2.FUNDING_DATASET_UNAVAILABLE),
        (
            _funding(end_ms=_DECISION_TIME_MS + 1),
            HistoricalThreeFamilyTe0ExclusionV2.FUNDING_COVERAGE_UNAVAILABLE,
        ),
    ],
)
def test_te0_never_treats_missing_funding_as_zero(
    funding: FundingDataset | None,
    expected: HistoricalThreeFamilyTe0ExclusionV2,
) -> None:
    decision = _candle(0)
    first = _candle(1, open_price="100", high="103", low="99", close="102")
    second = _candle(2, open_price="102", high="103", low="100.5", close="101.5")
    row = evaluate_historical_three_family_te0_v2(
        _event(),
        _dataset((decision, first, second)),
        (None, _feature(first), None),
        funding,
    )

    assert not row.evaluable
    assert row.exclusion_reason == expected.value
    assert row.funding_return_micros is None
    assert row.net_return_micros is None


def test_te0_feature_builder_resets_after_gap_and_uses_closed_history_only() -> None:
    first_segment = tuple(
        _candle(
            index - 300,
            open_price=str(100 + (index % 4) / 10),
            high="101",
            low="99",
            close=str(100 + (index % 4) / 10),
        )
        for index in range(210)
    )
    second_start = first_segment[-1].open_time_ms + 2 * FIVE_MINUTE_MS_V2
    second_segment = tuple(
        _candle(
            index,
            open_time_ms=second_start + index * FIVE_MINUTE_MS_V2,
            open_price=str(100 + (index % 4) / 10),
            high="101",
            low="99",
            close=str(100 + (index % 4) / 10),
        )
        for index in range(210)
    )
    features = build_historical_three_family_te0_features_v2(
        _dataset((*first_segment, *second_segment))
    )

    assert features[208] is None
    assert features[209] is not None
    assert features[210] is None
    assert features[-2] is None
    assert features[-1] is not None


def test_te0_csv_is_order_independent_and_forbids_mixed_result_exclusion() -> None:
    decision = _candle(0)
    first = _candle(1, open_price="100", high="103", low="99", close="102")
    second = _candle(2, open_price="102", high="103", low="100.5", close="101.5")
    row = evaluate_historical_three_family_te0_v2(
        _event(),
        _dataset((decision, first, second)),
        (None, _feature(first), None),
        _funding(),
    )

    assert _technical_exit_te0_csv_bytes_v2((row,)) == _technical_exit_te0_csv_bytes_v2((row,))
    assert b"\r" not in _technical_exit_te0_csv_bytes_v2((row,))
    with pytest.raises(HistoricalThreeFamilyTe0ErrorV2, match="no exclusion"):
        replace(row, exclusion_reason="DATA_GAP")
    with pytest.raises(HistoricalThreeFamilyTe0ErrorV2, match="prohibited claim"):
        replace(row, probability=True)  # type: ignore[arg-type]
    with pytest.raises(HistoricalThreeFamilyTe0ErrorV2, match="conflicted-majority"):
        replace(
            row,
            state_class="MIXED_OR_NEUTRAL_STATE",
            agreement_bucket="CONFLICTED_2_VS_1",
        )


def test_te0_runner_authenticates_both_contracts_and_publishes_deterministically(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    data_hashes, manifest_hashes = _write_panel(data_root)
    consensus, census_manifest, census_sha = _write_census_authority(
        tmp_path / "census",
        data_hashes=data_hashes,
        manifest_hashes=manifest_hashes,
    )
    funding_relative = "funding/BONK__1000BONKUSDT__5m.csv.gz"
    funding_path = data_root / funding_relative
    funding = FundingDataset(
        symbol="1000BONKUSDT",
        start_time_ms=_DECISION_TIME_MS + 1,
        end_time_ms=_DECISION_TIME_MS + FIVE_MINUTE_MS_V2,
        rates=(FundingRate(_DECISION_TIME_MS + 2, 0.001, 100.0),),
    )
    write_funding_csv(funding, funding_path)
    binding = HistoricalFundingFileBindingV2(
        symbol="1000BONKUSDT",
        relative_path=funding_relative,
        sha256=funding_sha256(funding_path),
    )
    funding_manifest_raw = canonical_historical_funding_authority_manifest_v2((binding,))
    funding_manifest = tmp_path / "funding-authority.json"
    funding_manifest.write_bytes(funding_manifest_raw)
    funding_manifest_sha = hashlib.sha256(funding_manifest_raw).hexdigest()
    (tmp_path / "frozen.py").write_text("VALUE = 1\n", encoding="utf-8")
    downstream_freeze = create_downstream_code_freeze_v1(
        workspace_root=tmp_path,
        manifest_path="freeze/manifest.json",
        purpose="unit test downstream TE0 authority",
        include_trees=(),
        include_files=("frozen.py",),
        upstream_sha256={
            "bootstrap_schedule": HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2,
            "census_artifact_manifest": census_sha,
            "census_code_freeze": HISTORICAL_THREE_FAMILY_CENSUS_CODE_FREEZE_SHA256_V2,
            "experiment_contract": _EXPERIMENT_SHA,
            "funding_authority": funding_manifest_sha,
            "topology_amendment": _TOPOLOGY_SHA,
        },
    )

    first = run_historical_three_family_te0_v2(
        consensus_path=consensus,
        census_manifest_path=census_manifest,
        expected_census_manifest_sha256=census_sha,
        expected_experiment_contract_sha256=_EXPERIMENT_SHA,
        expected_topology_amendment_sha256=_TOPOLOGY_SHA,
        data_root=data_root,
        output_dir=tmp_path / "out-one",
        workspace_root=tmp_path,
        downstream_code_freeze_manifest_path=downstream_freeze.manifest_path,
        expected_downstream_code_freeze_manifest_sha256=(downstream_freeze.manifest_sha256),
        funding_authority_manifest_path=funding_manifest,
        expected_funding_authority_manifest_sha256=funding_manifest_sha,
    )
    second = run_historical_three_family_te0_v2(
        consensus_path=consensus,
        census_manifest_path=census_manifest,
        expected_census_manifest_sha256=census_sha,
        expected_experiment_contract_sha256=_EXPERIMENT_SHA,
        expected_topology_amendment_sha256=_TOPOLOGY_SHA,
        data_root=data_root,
        output_dir=tmp_path / "out-two",
        workspace_root=tmp_path,
        downstream_code_freeze_manifest_path=downstream_freeze.manifest_path,
        expected_downstream_code_freeze_manifest_sha256=(downstream_freeze.manifest_sha256),
        funding_authority_manifest_path=funding_manifest,
        expected_funding_authority_manifest_sha256=funding_manifest_sha,
    )

    assert first.admitted_events == first.result_rows == first.evaluable_rows == 1
    assert second.technical_exit_sha256 == first.technical_exit_sha256
    assert second.results_sha256 == first.results_sha256
    assert second.manifest_sha256 == first.manifest_sha256
    assert {path.name for path in first.output_dir.iterdir()} == {
        "technical_exit_te0.csv",
        "results.json",
        "manifest.json",
    }
    results = json.loads((first.output_dir / "results.json").read_bytes())
    assert results["one_result_or_exclusion_per_event"] is True
    assert results["conflicted_majority_included"] is False
    assert results["opposite_signal_evaluated"] is False
    assert results["portfolio_equity_claim"] is False
    assert results["drawdown_claim"] is False
    assert results["downstream_code_freeze_manifest_sha256"] == downstream_freeze.manifest_sha256
    assert results["probability"] is False
    assert results["promoting"] is False

    loaded = load_authenticated_historical_three_family_te0_artifacts_v2(
        first.output_dir,
        expected_manifest_sha256=first.manifest_sha256,
        expected_census_manifest_sha256=census_sha,
        expected_experiment_contract_sha256=_EXPERIMENT_SHA,
        expected_topology_amendment_sha256=_TOPOLOGY_SHA,
        expected_funding_authority_manifest_sha256=funding_manifest_sha,
        expected_downstream_code_freeze_manifest_sha256=(downstream_freeze.manifest_sha256),
    )
    assert loaded.admitted_events == 1
    assert loaded.downstream_code_freeze_manifest_sha256 == downstream_freeze.manifest_sha256
    assert len(loaded.rows) == 1
    assert loaded.rows[0].event_id == _event().event_id

    (second.output_dir / "unexpected.txt").write_text("surplus", encoding="utf-8")
    with pytest.raises(HistoricalThreeFamilyTe0ErrorV2, match="exact published"):
        load_authenticated_historical_three_family_te0_artifacts_v2(
            second.output_dir,
            expected_manifest_sha256=second.manifest_sha256,
            expected_census_manifest_sha256=census_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256=_TOPOLOGY_SHA,
            expected_funding_authority_manifest_sha256=funding_manifest_sha,
            expected_downstream_code_freeze_manifest_sha256=(downstream_freeze.manifest_sha256),
        )

    with pytest.raises(ValueError, match="required upstream binding"):
        run_historical_three_family_te0_v2(
            consensus_path=consensus,
            census_manifest_path=census_manifest,
            expected_census_manifest_sha256=census_sha,
            expected_experiment_contract_sha256=_EXPERIMENT_SHA,
            expected_topology_amendment_sha256="0" * 64,
            data_root=data_root,
            output_dir=tmp_path / "wrong-topology",
            workspace_root=tmp_path,
            downstream_code_freeze_manifest_path=downstream_freeze.manifest_path,
            expected_downstream_code_freeze_manifest_sha256=(downstream_freeze.manifest_sha256),
            funding_authority_manifest_path=funding_manifest,
            expected_funding_authority_manifest_sha256=funding_manifest_sha,
        )
