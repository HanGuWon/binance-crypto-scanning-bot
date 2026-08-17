from __future__ import annotations

import csv
import inspect
import io
from dataclasses import fields, replace
from decimal import Decimal
from pathlib import Path

import pytest

from signalbot.backtest.alert_replay import RecommendationEvent
from signalbot.backtest.dataset import (
    KlineDataset,
    KlineDatasetRequest,
    build_dataset_manifest,
    sha256_file,
    write_dataset_manifest,
    write_kline_csv,
)
from signalbot.backtest.historical_three_family_census import (
    HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2,
    HISTORICAL_THREE_FAMILY_EXPECTED_ANCHORS_BY_SPLIT_V2,
    HISTORICAL_THREE_FAMILY_EXPECTED_ANCHORS_V2,
    HistoricalAnchorDispositionV2,
    HistoricalConsensusCensusRowV2,
    HistoricalContractAuthorityV2,
    HistoricalFuturesKlineAuthorityV2,
    HistoricalNumericRepresentationProvenanceV2,
    HistoricalSourceReplayAuditV2,
    HistoricalThreeFamilyCensusErrorV2,
    LoadedHistoricalRecommendationAnchorsV2,
    _artifact_manifest_document_v2,
    _census_results_document_v2,
    _CensusBuildersV2,
    _consensus_csv_bytes_v2,
    _contract_authority_v2,
    _disposition_sha256,
    _evaluate_anchors_v2,
    _family_c_source_evidence_sha256,
    _HistoricalCloseIndexV2,
    _HistoricalPeerWindowV2,
    _load_one_verified_dataset_v2,
    _parse_recommendations_v2,
    _publish_artifacts_v2,
    _TargetCandleIndexV2,
    _topology_analysis_document_v2,
    _WindowContractV2,
    load_historical_recommendation_anchors_v2,
    main,
    run_historical_three_family_census_v2,
)
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import Candle
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.historical_numeric_precompute import (
    HISTORICAL_NUMERIC_PRECOMPUTE_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
    HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2,
    HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2,
    HistoricalRecommendationAnchorV2,
    build_historical_execution_contract_v2,
)

_MANIFEST_HASH = "a" * 64
_EXPERIMENT_HASH = "b" * 64
_TOPOLOGY_CONTRACT_HASH = "c" * 64
_CODE_FREEZE_HASH = "d" * 64
_DATASET_HASH = "c" * 64
_KLINE_MANIFEST_HASH = "d" * 64
_BAR_OPEN_MS = 1_719_792_000_000

_FUTURES_SYMBOLS = {
    "BONK": "1000BONKUSDT",
    "ENA": "ENAUSDT",
    "WIF": "WIFUSDT",
    "FLOKI": "1000FLOKIUSDT",
    "ARB": "ARBUSDT",
    "OP": "OPUSDT",
    "SEI": "SEIUSDT",
}


def _recommendation_row(
    *,
    event_id: str = "1" * 24,
    market: str = "futures",
    asset: str = "BONK",
    family: str = "pullback_long",
    direction: str = "long",
    stage: str = "setup",
    information_only: str = "True",
    decision_time_ms: int = _BAR_OPEN_MS + 299_999,
    price: str = "10.1250",
    invalidation: str = "9.5",
    atr: str = "0.25",
    score: str = "100",
) -> dict[str, str]:
    row = {field.name: "" for field in fields(RecommendationEvent)}
    row.update(
        {
            "event_id": event_id,
            "protocol_version": HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2,
            "rule_version": HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2,
            "asset": asset,
            "cohort": "volatile",
            "market": market,
            "symbol": (
                _FUTURES_SYMBOLS[asset]
                if market == "futures"
                else ("BONKUSDT" if asset == "BONK" else _FUTURES_SYMBOLS[asset])
            ),
            "family": family,
            "direction": direction,
            "stage": stage,
            "information_only": information_only,
            "action_label": "INFORMATION_ONLY",
            "decision_time_ms": str(decision_time_ms),
            "split": "development",
            "score": score,
            "price": price,
            "invalidation": invalidation,
            "reasons": "source reason",
            "atr": atr,
            "recovery_confirmed": "False",
            "structure_intact": "True",
        }
    )
    return row


def _recommendation_bytes(*rows: dict[str, str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=[field.name for field in fields(RecommendationEvent)],
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode()


def _anchor(
    *,
    seed: str = "1",
    bar_open_ms: int = _BAR_OPEN_MS,
    price: str = "10",
    direction: Direction = Direction.LONG,
) -> HistoricalRecommendationAnchorV2:
    family = (
        SignalFamily.PULLBACK_LONG
        if direction is Direction.LONG
        else SignalFamily.PULLBACK_SHORT
    )
    invalidation = Decimal("9") if direction is Direction.LONG else Decimal("12")
    return HistoricalRecommendationAnchorV2(
        source_event_id=seed * 24,
        source_row_sha256=seed * 64,
        source_replay_manifest_sha256=_MANIFEST_HASH,
        split="development",
        asset="BONK",
        cohort="volatile",
        symbol="1000BONKUSDT",
        primary_family=family,
        primary_direction=direction,
        decision_time_ms=bar_open_ms + 299_999,
        price=Decimal(price),
        invalidation=invalidation,
        atr=Decimal("0.5"),
        source_rule_version=HISTORICAL_THREE_FAMILY_SOURCE_RULE_VERSION_V2,
        source_protocol_version=HISTORICAL_THREE_FAMILY_SOURCE_PROTOCOL_VERSION_V2,
    )


def _candle(symbol: str, bar_open_ms: int, close: str) -> Candle:
    price = Decimal(close)
    return Candle(
        market=Market.FUTURES,
        symbol=symbol,
        interval="5m",
        open_time_ms=bar_open_ms,
        close_time_ms=bar_open_ms + 299_999,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("10"),
        quote_volume=Decimal("100"),
        trade_count=5,
        taker_buy_base_volume=Decimal("4"),
        taker_buy_quote_volume=Decimal("40"),
        is_closed=True,
    )


def _compact_row(
    anchor: HistoricalRecommendationAnchorV2,
    *,
    event_seed: str = "e",
    signs: tuple[int, int, int] = (1, 1, 1),
) -> HistoricalConsensusCensusRowV2:
    bullish = signs.count(1)
    bearish = signs.count(-1)
    neutral = signs.count(0)
    topology = {
        (3, 0, 0): "UNANIMOUS_BULLISH_3_0_0",
        (0, 3, 0): "UNANIMOUS_BEARISH_0_3_0",
        (2, 0, 1): "CLEAN_BULLISH_2_0_1",
        (0, 2, 1): "CLEAN_BEARISH_0_2_1",
        (2, 1, 0): "CONFLICTED_BULLISH_2_1_0",
        (1, 2, 0): "CONFLICTED_BEARISH_1_2_0",
        (1, 0, 2): "LONE_BULLISH_1_0_2",
        (0, 1, 2): "LONE_BEARISH_0_1_2",
        (1, 1, 1): "BALANCED_1_1_1",
        (0, 0, 3): "ALL_NEUTRAL_0_0_3",
    }[(bullish, bearish, neutral)]
    if topology.startswith("UNANIMOUS"):
        bucket = "BROAD_3_OF_3"
        grade = "UNANIMOUS_BREADTH_UNCALIBRATED"
    elif topology.startswith("CLEAN"):
        bucket = "CLEAN_2_PLUS_NEUTRAL"
        grade = "CLEAN_TWO_FAMILY_BREADTH_UNCALIBRATED"
    elif topology.startswith("CONFLICTED"):
        bucket = "CONFLICTED_2_VS_1"
        grade = "CONFLICTED_MAJORITY_UNCALIBRATED"
    elif topology.startswith("LONE"):
        bucket = "NOT_COMPARABLE"
        grade = "INSUFFICIENT_DIRECTIONAL_BREADTH"
    else:
        bucket = "NOT_COMPARABLE"
        grade = "NO_DIRECTIONAL_CONSENSUS"
    support = bullish if anchor.primary_direction is Direction.LONG else bearish
    oppose = bearish if anchor.primary_direction is Direction.LONG else bullish
    clean = bucket in {"BROAD_3_OF_3", "CLEAN_2_PLUS_NEUTRAL"} and support >= 2
    conflicted = bucket == "CONFLICTED_2_VS_1" and support == 2 and oppose == 1
    majority_direction = "BULLISH" if bullish >= 2 else "BEARISH" if bearish >= 2 else None
    majority_count = bullish if bullish >= 2 else bearish if bearish >= 2 else None
    opposing_count = bearish if bullish >= 2 else bullish if bearish >= 2 else None
    return HistoricalConsensusCensusRowV2(
        split=anchor.split,
        asset=anchor.asset,
        symbol=anchor.symbol,
        source_event_id=anchor.source_event_id,
        source_row_sha256=anchor.source_row_sha256,
        source_replay_manifest_sha256=anchor.source_replay_manifest_sha256,
        anchor_sha256=anchor.anchor_sha256,
        primary_family=anchor.primary_family.value,
        primary_direction=anchor.primary_direction.value,
        decision_time_ms=anchor.decision_time_ms,
        price=anchor.price,
        invalidation=anchor.invalidation,
        atr=anchor.atr,
        event_id=event_seed * 64,
        payload_sha256="f" * 64,
        canonical_consensus_sha256="0" * 64,
        topology_sha256="b" * 64,
        canonical_topology_sha256="c" * 64,
        topology_contract_sha256=_TOPOLOGY_CONTRACT_HASH,
        topology_rule_version=HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        topology_class=topology,
        topology_comparison_bucket=bucket,
        topology_display_grade=grade,
        topology_majority_direction=majority_direction,
        topology_majority_family_count=majority_count,
        topology_opposing_family_count=opposing_count,
        topology_has_opposition=bullish > 0 and bearish > 0,
        topology_primary_support_count=support,
        topology_primary_oppose_count=oppose,
        topology_primary_neutral_count=neutral,
        clean_primary_audit_eligible=clean,
        conflicted_comparator_eligible=conflicted,
        conflicted_comparator_outcome_authorized=False,
        rule_version=HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        status="READY",
        state_class="BROAD_BULLISH_STATE",
        directional_numerator_micros=1_500_000,
        directional_denominator=3,
        directional_agreement_micros=500_000,
        bullish_family_count=bullish,
        bearish_family_count=bearish,
        neutral_family_count=neutral,
        primary_relationship="SUPPORTS_PRIMARY",
        admitted=clean,
        price_status="READY",
        price_direction=signs[0],
        price_strength_micros=500_000,
        price_calculation_sha256="5" * 64,
        price_source_slice_sha256="6" * 64,
        participation_status="READY",
        participation_direction=signs[1],
        participation_strength_micros=500_000,
        participation_calculation_sha256="7" * 64,
        participation_source_slice_sha256="8" * 64,
        cross_section_status="READY",
        cross_section_direction=signs[2],
        cross_section_strength_micros=500_000,
        cross_section_calculation_sha256="9" * 64,
        cross_section_source_slice_sha256="a" * 64,
        execution_contract_sha256="2" * 64,
        zero_move_round_trip_cost_micros=2_600,
        atr_fraction_micros=50_000,
        one_atr_cost_headroom_micros=47_400,
        cross_peer_set_root_sha256="3" * 64,
        cross_peer_input_sha256="4" * 64,
        reasons=("HISTORICAL_ONLY", "NOT_A_PROBABILITY"),
    )


def _tiny_indexes() -> dict[str, _HistoricalCloseIndexV2]:
    first_open_ms = _BAR_OPEN_MS - 300_000
    return {
        asset: _HistoricalCloseIndexV2(
            asset=asset,
            symbol=symbol,
            dataset_sha256=_DATASET_HASH,
            manifest_sha256=_KLINE_MANIFEST_HASH,
            first_open_time_ms=first_open_ms,
            closes=(Decimal("1"), Decimal("2"), Decimal("3")),
        )
        for asset, symbol in _FUTURES_SYMBOLS.items()
    }


def _tiny_target() -> _TargetCandleIndexV2:
    first = _BAR_OPEN_MS - 300_000
    candles = (
        _candle("1000BONKUSDT", first, "9"),
        _candle("1000BONKUSDT", _BAR_OPEN_MS, "10"),
        _candle("1000BONKUSDT", _BAR_OPEN_MS + 300_000, "11"),
    )
    return _TargetCandleIndexV2(
        asset="BONK",
        symbol="1000BONKUSDT",
        dataset_sha256=_DATASET_HASH,
        first_open_time_ms=first,
        candles=candles,
    )


def _contract_authority() -> HistoricalContractAuthorityV2:
    return _contract_authority_v2(
        experiment_contract_sha256=_EXPERIMENT_HASH,
        topology_contract_sha256=_TOPOLOGY_CONTRACT_HASH,
        code_freeze_manifest_sha256=_CODE_FREEZE_HASH,
        workspace_root=None,
    )


def _numeric_provenance(
    processed_assets: tuple[str, ...] = ("BONK",),
) -> HistoricalNumericRepresentationProvenanceV2:
    r3_roots = tuple(
        (asset, character * 64)
        for asset, character in zip(_FUTURES_SYMBOLS, "1234567", strict=True)
    )
    target_characters = dict(
        zip(_FUTURES_SYMBOLS, "89abcde", strict=True)
    )
    cross_characters = dict(
        zip(_FUTURES_SYMBOLS, "abcdef1", strict=True)
    )
    return HistoricalNumericRepresentationProvenanceV2(
        rule_version=HISTORICAL_NUMERIC_PRECOMPUTE_RULE_VERSION_V2,
        r3_cache_sha256s=r3_roots,
        target_cache_sha256s=tuple(
            (asset, target_characters[asset] * 64) for asset in processed_assets
        ),
        cross_cache_sha256s=tuple(
            (asset, cross_characters[asset] * 64) for asset in processed_assets
        ),
    )


def test_parser_keeps_exact_futures_pullback_anchor_decimals_and_source_row_hash() -> None:
    anchor_row = _recommendation_row()
    spot_row = _recommendation_row(event_id="2" * 24, market="spot")
    confirmed_row = _recommendation_row(
        event_id="3" * 24,
        family="breakout_long",
        stage="confirmed",
        information_only="False",
        score="80",
    )

    parsed = _parse_recommendations_v2(
        _recommendation_bytes(anchor_row, spot_row, confirmed_row),
        expected_split="development",
        source_replay_manifest_sha256=_MANIFEST_HASH,
    )

    assert parsed.recommendation_rows == 3
    assert len(parsed.anchors) == 1
    anchor = parsed.anchors[0]
    assert anchor.price == Decimal("10.1250")
    assert anchor.invalidation == Decimal("9.5")
    assert anchor.atr == Decimal("0.25")
    assert anchor.source_replay_manifest_sha256 == _MANIFEST_HASH
    assert len(anchor.source_row_sha256) == 64
    changed = dict(anchor_row)
    changed["reasons"] = "different source reason"
    changed_anchor = _parse_recommendations_v2(
        _recommendation_bytes(changed),
        expected_split="development",
        source_replay_manifest_sha256=_MANIFEST_HASH,
    ).anchors[0]
    assert changed_anchor.source_row_sha256 != anchor.source_row_sha256


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"protocol_version": "drift"}, "protocol drift"),
        ({"rule_version": "drift"}, "rule drift"),
        ({"split": "validation"}, "split drift"),
        ({"decision_time_ms": str(_BAR_OPEN_MS + 299_998)}, "split/grid"),
        ({"information_only": "true"}, "exact True or False"),
        ({"invalidation": ""}, "positive invalidation"),
        ({"invalidation": "0"}, "positive invalidation"),
        ({"event_id": "A" * 24}, "lowercase hexadecimal"),
    ],
)
def test_parser_rejects_identity_boundary_and_decimal_contract_drift(
    changes: dict[str, str],
    match: str,
) -> None:
    row = _recommendation_row()
    row.update(changes)
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match=match):
        _parse_recommendations_v2(
            _recommendation_bytes(row),
            expected_split="development",
            source_replay_manifest_sha256=_MANIFEST_HASH,
        )


def test_parser_rejects_duplicate_event_and_duplicate_anchor_identity() -> None:
    first = _recommendation_row()
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="event_id"):
        _parse_recommendations_v2(
            _recommendation_bytes(first, first),
            expected_split="development",
            source_replay_manifest_sha256=_MANIFEST_HASH,
        )
    second = _recommendation_row(event_id="2" * 24)
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="asset, direction"):
        _parse_recommendations_v2(
            _recommendation_bytes(first, second),
            expected_split="development",
            source_replay_manifest_sha256=_MANIFEST_HASH,
        )


def test_parser_preserves_positive_wrong_side_invalidation_for_te0_only() -> None:
    long_row = _recommendation_row(invalidation="11")
    short_row = _recommendation_row(
        event_id="2" * 24,
        family="pullback_short",
        direction="short",
        invalidation="9",
    )

    parsed = _parse_recommendations_v2(
        _recommendation_bytes(long_row, short_row),
        expected_split="development",
        source_replay_manifest_sha256=_MANIFEST_HASH,
    )

    assert len(parsed.anchors) == 2
    assert parsed.anchors[0].primary_direction is Direction.LONG
    assert parsed.anchors[0].invalidation == Decimal("11")
    assert parsed.anchors[1].primary_direction is Direction.SHORT
    assert parsed.anchors[1].invalidation == Decimal("9")


def test_parser_rejects_non_lf_or_surplus_column() -> None:
    raw = _recommendation_bytes(_recommendation_row())
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="LF-only"):
        _parse_recommendations_v2(
            raw.replace(b"\n", b"\r\n"),
            expected_split="development",
            source_replay_manifest_sha256=_MANIFEST_HASH,
        )
    header, body = raw.split(b"\n", 1)
    surplus = header + b",extra\n" + body
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="header"):
        _parse_recommendations_v2(
            surplus,
            expected_split="development",
            source_replay_manifest_sha256=_MANIFEST_HASH,
        )


def test_latest_amendment_loader_has_exact_counts_and_reader_never_sees_outcome_path() -> None:
    workspace = Path(__file__).resolve().parents[2]
    base = (
        workspace
        / "artifacts"
        / "backtest"
        / "2026-07-20-indicator-discriminator-v1a-7asset"
    )
    replay_dirs = {
        "development": base / "replay-development-amendment-1",
        "validation": base / "replay-validation-amendment-1",
        "retrospective_test": base / "replay-retrospective-amendment-1",
    }
    if any(
        not (directory / "recommendations.csv").is_file()
        for directory in replay_dirs.values()
    ):
        pytest.skip(
            "requires ignored local V1A replay artifacts; "
            "the parser is covered by synthetic fixtures in this suite"
        )
    seen: list[Path] = []

    def recommendation_reader(path: Path) -> bytes:
        seen.append(path)
        assert path.name == "recommendations.csv"
        return path.read_bytes()

    loaded = load_historical_recommendation_anchors_v2(
        replay_dirs=replay_dirs,
        recommendation_reader=recommendation_reader,
    )

    assert len(loaded.anchors) == HISTORICAL_THREE_FAMILY_EXPECTED_ANCHORS_V2
    assert {
        audit.split: audit.anchor_rows for audit in loaded.replay_audits
    } == HISTORICAL_THREE_FAMILY_EXPECTED_ANCHORS_BY_SPLIT_V2
    assert [path.name for path in seen] == ["recommendations.csv"] * 3
    assert all("outcomes.csv" not in str(path) for path in seen)
    assert loaded.outcome_data_read is False
    assert loaded.fitted_v1a_selection_used is False


def test_family_c_mapping_uses_close_time_for_event_and_receipt_and_exact_slice() -> None:
    index = _tiny_indexes()["ENA"]
    rows = index.family_c_slice(final_open_time_ms=_BAR_OPEN_MS, row_count=2)

    assert len(rows) == 2
    assert rows[-1].bar_open_ms == _BAR_OPEN_MS
    assert rows[-1].event_time_ms == rows[-1].bar_close_ms
    assert rows[-1].receipt_time_ms == rows[-1].bar_close_ms
    assert rows[-1].close == Decimal("2")
    assert rows[-1].source_evidence_sha256 == _family_c_source_evidence_sha256(
        dataset_sha256=_DATASET_HASH,
        manifest_sha256=_KLINE_MANIFEST_HASH,
        symbol="ENAUSDT",
        bar_open_ms=_BAR_OPEN_MS,
        bar_close_ms=_BAR_OPEN_MS + 299_999,
        close=Decimal("2"),
    )
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="lacks 4"):
        index.family_c_slice(final_open_time_ms=_BAR_OPEN_MS, row_count=4)
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="no exact indexed"):
        index.family_c_slice(final_open_time_ms=_BAR_OPEN_MS + 1, row_count=1)


def test_evaluator_uses_tiny_injected_builders_groups_target_load_and_accounts_limit() -> None:
    first = _anchor(seed="1", price="10")
    second = _anchor(
        seed="2",
        bar_open_ms=_BAR_OPEN_MS + 300_000,
        price="11",
        direction=Direction.SHORT,
    )
    calls: list[tuple[str, int]] = []
    target_loads: list[str] = []

    def target_loader(asset: str) -> _TargetCandleIndexV2:
        target_loads.append(asset)
        return _tiny_target()

    def price_builder(
        attempt_id: str,
        dataset_sha256: str,
        bar_open_ms: int,
        rows: tuple[Candle, ...],
    ) -> tuple[str, int]:
        assert attempt_id.startswith("historical-census:")
        assert dataset_sha256 == _DATASET_HASH
        assert len(rows) == 2
        calls.append(("price", bar_open_ms))
        return "price", bar_open_ms

    def participation_builder(
        attempt_id: str,
        dataset_sha256: str,
        bar_open_ms: int,
        rows: tuple[Candle, ...],
    ) -> tuple[str, int]:
        assert attempt_id.startswith("historical-census:")
        assert dataset_sha256 == _DATASET_HASH
        assert len(rows) == 2
        calls.append(("participation", bar_open_ms))
        return "participation", bar_open_ms

    def cross_builder(
        target_symbol: str,
        bar_open_ms: int,
        peers: tuple[_HistoricalPeerWindowV2, ...],
    ) -> tuple[str, int]:
        assert target_symbol == "1000BONKUSDT"
        assert len(peers) == 6
        assert all(len(window.closes) == 2 for window in peers)
        assert all(target_symbol != window.symbol for window in peers)
        for window in peers:
            assert window.event_time_ms == window.final_close_ms
            assert window.receipt_time_ms == window.final_close_ms
        calls.append(("cross", bar_open_ms))
        return "cross", bar_open_ms

    def compact_builder(
        anchor: HistoricalRecommendationAnchorV2,
        price: tuple[str, int],
        participation: tuple[str, int],
        cross: tuple[str, int],
        execution_contract: object,
        experiment_hash: str,
        topology_contract_hash: str,
    ) -> HistoricalConsensusCensusRowV2:
        assert price[1] == participation[1] == cross[1] == anchor.bar_open_ms
        assert execution_contract is not None
        assert experiment_hash == _EXPERIMENT_HASH
        assert topology_contract_hash == _TOPOLOGY_CONTRACT_HASH
        return _compact_row(anchor, event_seed=anchor.source_event_id[0])

    builders = _CensusBuildersV2(
        price=price_builder,
        participation=participation_builder,
        cross_section=cross_builder,
        compact_consensus=compact_builder,
    )
    rows, dispositions = _evaluate_anchors_v2(
        anchors=(second, first),
        close_indexes=_tiny_indexes(),
        target_loader=target_loader,
        execution_contract=build_historical_execution_contract_v2(),
        experiment_contract_sha256=_EXPERIMENT_HASH,
        topology_contract_sha256=_TOPOLOGY_CONTRACT_HASH,
        maximum_anchors=1,
        windows=_WindowContractV2(price_rows=2, participation_rows=2, cross_rows=2),
        builders=builders,
    )

    assert [row.anchor_sha256 for row in rows] == [first.anchor_sha256]
    assert target_loads == ["BONK"]
    assert calls == [
        ("price", _BAR_OPEN_MS),
        ("participation", _BAR_OPEN_MS),
        ("cross", _BAR_OPEN_MS),
    ]
    assert [item.disposition for item in dispositions] == [
        HistoricalAnchorDispositionV2.CONSENSUS_EMITTED,
        HistoricalAnchorDispositionV2.DIAGNOSTIC_LIMIT_NOT_EVALUATED,
    ]
    assert dispositions[0].consensus_event_id == "1" * 64
    assert dispositions[1].consensus_event_id is None


def test_evaluator_processes_all_and_loads_one_target_once_per_asset() -> None:
    first = _anchor(seed="1", price="10")
    second = _anchor(
        seed="2",
        bar_open_ms=_BAR_OPEN_MS + 300_000,
        price="11",
        direction=Direction.SHORT,
    )
    loads = 0

    def target_loader(_: str) -> _TargetCandleIndexV2:
        nonlocal loads
        loads += 1
        return _tiny_target()

    def passthrough(*args: object) -> object:
        return args

    def compact(
        anchor: HistoricalRecommendationAnchorV2,
        _price: object,
        _participation: object,
        _cross: object,
        _execution: object,
        _experiment: str,
        _topology_contract: str,
    ) -> HistoricalConsensusCensusRowV2:
        return _compact_row(anchor, event_seed=anchor.source_event_id[0])

    rows, dispositions = _evaluate_anchors_v2(
        anchors=(second, first),
        close_indexes=_tiny_indexes(),
        target_loader=target_loader,
        execution_contract=build_historical_execution_contract_v2(),
        experiment_contract_sha256=_EXPERIMENT_HASH,
        topology_contract_sha256=_TOPOLOGY_CONTRACT_HASH,
        maximum_anchors=None,
        windows=_WindowContractV2(price_rows=2, participation_rows=2, cross_rows=2),
        builders=_CensusBuildersV2(
            price=passthrough,
            participation=passthrough,
            cross_section=passthrough,
            compact_consensus=compact,
        ),
    )

    assert len(rows) == 2
    assert loads == 1
    assert all(
        item.disposition is HistoricalAnchorDispositionV2.CONSENSUS_EMITTED
        for item in dispositions
    )


def test_evaluator_rejects_anchor_price_drift_before_any_proxy_builder() -> None:
    bad = _anchor(seed="1", price="10.1")
    calls = 0

    def forbidden(*_args: object) -> object:
        nonlocal calls
        calls += 1
        raise AssertionError("proxy builder must not run")

    def forbidden_compact(
        _anchor_value: HistoricalRecommendationAnchorV2,
        _price: object,
        _participation: object,
        _cross: object,
        _execution: object,
        _experiment: str,
        _topology_contract: str,
    ) -> HistoricalConsensusCensusRowV2:
        nonlocal calls
        calls += 1
        raise AssertionError("compact builder must not run")

    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="price differs"):
        _evaluate_anchors_v2(
            anchors=(bad,),
            close_indexes=_tiny_indexes(),
            target_loader=lambda _asset: _tiny_target(),
            execution_contract=build_historical_execution_contract_v2(),
            experiment_contract_sha256=_EXPERIMENT_HASH,
            topology_contract_sha256=_TOPOLOGY_CONTRACT_HASH,
            maximum_anchors=None,
            windows=_WindowContractV2(price_rows=2, participation_rows=2, cross_rows=2),
            builders=_CensusBuildersV2(
                price=forbidden,
                participation=forbidden,
                cross_section=forbidden,
                compact_consensus=forbidden_compact,
            ),
        )
    assert calls == 0


def test_consensus_csv_is_canonical_stably_sorted_and_preserves_decimal_text() -> None:
    first = _compact_row(_anchor(seed="1", price="10.1250"), event_seed="1")
    second = _compact_row(
        _anchor(
            seed="2",
            bar_open_ms=_BAR_OPEN_MS + 300_000,
            price="11",
            direction=Direction.SHORT,
        ),
        event_seed="2",
    )

    forward = _consensus_csv_bytes_v2((first, second))
    reverse = _consensus_csv_bytes_v2((second, first))

    assert forward == reverse
    assert b"\r" not in forward
    assert b"10.1250" in forward
    assert b"HISTORICAL_ONLY|NOT_A_PROBABILITY" in forward
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="duplicate"):
        _consensus_csv_bytes_v2((first, first))


def test_diagnostic_results_and_manifest_cannot_masquerade_as_complete() -> None:
    first = _anchor(seed="1")
    second = _anchor(
        seed="2",
        bar_open_ms=_BAR_OPEN_MS + 300_000,
        price="11",
        direction=Direction.SHORT,
    )
    row = _compact_row(first)
    dispositions = (
        _disposition(first, row=row),
        _disposition(second, row=None),
    )
    loaded = LoadedHistoricalRecommendationAnchorsV2(
        anchors=(first, second),
        replay_audits=(),
        anchor_set_sha256="5" * 64,
    )
    execution = build_historical_execution_contract_v2()

    results = _census_results_document_v2(
        loaded=loaded,
        authorities=(),
        rows=(row,),
        dispositions=dispositions,
        execution_contract=execution,
        contract_authority=_contract_authority(),
        numeric_provenance=_numeric_provenance(),
        maximum_anchors=1,
        consensus_csv_sha256="6" * 64,
    )
    manifest = _artifact_manifest_document_v2(
        loaded=loaded,
        authorities=(),
        contract_authority=_contract_authority(),
        numeric_provenance=_numeric_provenance(),
        execution_contract=execution,
        maximum_anchors=1,
        payload_sha256={"consensus.csv": "6" * 64, "results.json": "7" * 64},
    )

    assert results["diagnostic_mode"] is True
    assert results["census_complete"] is False
    assert results["maximum_anchors"] == 1
    assert results["diagnostic_limit_reached"] is True
    assert results["outcome_data_read"] is False
    assert results["v1a_fitted_selection_used"] is False
    assert results["topology_contract_sha256"] == _TOPOLOGY_CONTRACT_HASH
    assert results["code_freeze_manifest_sha256"] == _CODE_FREEZE_HASH
    provenance = results["numeric_representation_provenance"]
    assert isinstance(provenance, dict)
    assert provenance["calculation_authority"] is False
    assert len(provenance["r3_cache_sha256s"]) == 7
    assert results["disposition_counts"] == {
        "CONSENSUS_EMITTED": 1,
        "DIAGNOSTIC_LIMIT_NOT_EVALUATED": 1,
    }
    assert results["all_anchor_dispositions_sha256"] == _disposition_sha256(
        dispositions
    )
    performance = results["performance_contract"]
    assert isinstance(performance, dict)
    assert performance["warning"] == "FULL_SOURCE_PROXY_FALLBACK_EXPECTED_TO_EXCEED_FIVE_HOURS"
    assert manifest["diagnostic_mode"] is True
    assert manifest["census_complete"] is False
    assert manifest["maximum_anchors"] == 1
    assert manifest["topology_contract_sha256"] == _TOPOLOGY_CONTRACT_HASH
    assert manifest["code_freeze_manifest_sha256"] == _CODE_FREEZE_HASH


def test_topology_analysis_is_exhaustive_ordered_hash_bound_and_outcome_blind() -> None:
    first = _compact_row(_anchor(seed="1"), event_seed="1", signs=(1, 1, 1))
    second = _compact_row(
        _anchor(seed="2", bar_open_ms=_BAR_OPEN_MS + 300_000, price="11"),
        event_seed="2",
        signs=(1, -1, 0),
    )
    third = _compact_row(
        _anchor(
            seed="3",
            bar_open_ms=_BAR_OPEN_MS + 600_000,
            price="12",
            direction=Direction.SHORT,
        ),
        event_seed="3",
        signs=(-1, -1, 1),
    )

    analysis = _topology_analysis_document_v2((third, first, second))
    reversed_analysis = _topology_analysis_document_v2((second, first, third))

    assert analysis == reversed_analysis
    assert analysis["outcome_data_used"] is False
    assert analysis["conflicted_comparator_outcome_authorized"] is False
    sign_table = analysis["ordered_27_sign_table"]
    assert isinstance(sign_table, list)
    assert len(sign_table) == 27
    assert [entry["ordinal"] for entry in sign_table] == list(range(27))
    assert [
        (
            entry["price_structure_momentum_direction"],
            entry["participation_flow_direction"],
            entry["cross_sectional_context_ex_target_direction"],
        )
        for entry in sign_table
    ] == [
        (price, participation, cross)
        for price in (-1, 0, 1)
        for participation in (-1, 0, 1)
        for cross in (-1, 0, 1)
    ]
    sign_counts = {
        (
            entry["price_structure_momentum_direction"],
            entry["participation_flow_direction"],
            entry["cross_sectional_context_ex_target_direction"],
        ): entry["consensus_rows"]
        for entry in sign_table
    }
    assert sign_counts[(1, 1, 1)] == 1
    assert sign_counts[(1, -1, 0)] == 1
    assert sign_counts[(-1, -1, 1)] == 1
    assert sum(sign_counts.values()) == 3

    grouped = analysis["split_asset_primary_direction_topology_counts"]
    assert isinstance(grouped, list)
    assert len(grouped) == 3 * 7 * 2 * 11
    assert sum(entry["consensus_rows"] for entry in grouped) == 3

    leaf_rates = analysis["leaf_sign_rates"]
    assert isinstance(leaf_rates, list)
    price_rates = leaf_rates[0]
    assert price_rates["family"] == "PRICE_STRUCTURE_MOMENTUM"
    assert price_rates["sign_counts"] == [
        {"consensus_rows": 1, "direction": -1, "rate_micros": 333_333},
        {"consensus_rows": 0, "direction": 0, "rate_micros": 0},
        {"consensus_rows": 2, "direction": 1, "rate_micros": 666_667},
        {"consensus_rows": 0, "direction": None, "rate_micros": 0},
    ]

    pairwise = analysis["pairwise_family_relationship_rates"]
    assert isinstance(pairwise, list)
    assert pairwise[0]["agreement_rows"] == 2
    assert pairwise[0]["disagreement_rows"] == 1
    assert pairwise[0]["neutral_involved_rows"] == 0
    assert pairwise[0]["both_ready_denominator_rows"] == 3
    assert pairwise[1]["agreement_rows"] == 1
    assert pairwise[1]["disagreement_rows"] == 1
    assert pairwise[1]["neutral_involved_rows"] == 1

    reconciliation = analysis["admission_reconciliation"]
    assert isinstance(reconciliation, dict)
    assert reconciliation["admission_parity"] is True
    assert reconciliation["source_admitted_rows"] == 1
    assert reconciliation["clean_primary_audit_eligible_rows"] == 1
    assert reconciliation["conflicted_comparator_eligible_rows"] == 1
    assert reconciliation["conflicted_comparator_outcome_authorized"] is False

    changed = _topology_analysis_document_v2(
        (replace(first, topology_sha256="d" * 64), second, third)
    )
    assert changed["topology_analysis_sha256"] != analysis["topology_analysis_sha256"]

    empty = _topology_analysis_document_v2(())
    empty_pairwise = empty["pairwise_family_relationship_rates"]
    assert isinstance(empty_pairwise, list)
    assert all(item["agreement_rate_micros"] is None for item in empty_pairwise)
    assert all(item["disagreement_rate_micros"] is None for item in empty_pairwise)
    assert all(item["neutral_involved_rate_micros"] is None for item in empty_pairwise)


def _disposition(
    anchor: HistoricalRecommendationAnchorV2,
    *,
    row: HistoricalConsensusCensusRowV2 | None,
):
    from signalbot.backtest.historical_three_family_census import (
        HistoricalAnchorDispositionRowV2,
    )

    return HistoricalAnchorDispositionRowV2(
        split=anchor.split,
        asset=anchor.asset,
        primary_direction=anchor.primary_direction.value,
        decision_time_ms=anchor.decision_time_ms,
        anchor_sha256=anchor.anchor_sha256,
        disposition=(
            HistoricalAnchorDispositionV2.CONSENSUS_EMITTED
            if row is not None
            else HistoricalAnchorDispositionV2.DIAGNOSTIC_LIMIT_NOT_EVALUATED
        ),
        consensus_event_id=None if row is None else row.event_id,
        consensus_payload_sha256=None if row is None else row.payload_sha256,
    )


def test_one_dataset_loader_verifies_canonical_manifest_hash_identity_and_gaps(
    tmp_path: Path,
) -> None:
    first_open = _BAR_OPEN_MS - 300_000
    candles = (
        _candle("1000BONKUSDT", first_open, "9"),
        _candle("1000BONKUSDT", _BAR_OPEN_MS, "10"),
    )
    request = KlineDatasetRequest(
        market=Market.FUTURES,
        symbol="1000BONKUSDT",
        alias="BONK",
        interval="5m",
        start_time_ms=first_open,
        end_time_ms=_BAR_OPEN_MS + 299_999,
    )
    dataset = KlineDataset(request=request, candles=candles)
    data_path = tmp_path / "futures" / "BONK__1000BONKUSDT__5m.csv.gz"
    write_kline_csv(dataset, data_path)
    manifest_path = data_path.with_name(f"{data_path.name}.manifest.json")
    write_dataset_manifest(build_dataset_manifest(data_path), manifest_path)
    digest = sha256_file(data_path)

    loaded, authority = _load_one_verified_dataset_v2(
        data_root=tmp_path,
        asset="BONK",
        replay_expected_sha256=digest,
    )

    assert loaded == dataset
    assert authority.data_sha256 == digest
    assert authority.manifest_sha256 == sha256_file(manifest_path)
    assert authority.row_count == 2
    manifest_path.write_bytes(b" " + manifest_path.read_bytes())
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="not canonical"):
        _load_one_verified_dataset_v2(
            data_root=tmp_path,
            asset="BONK",
            replay_expected_sha256=digest,
        )


def test_atomic_publication_writes_exact_three_files_and_rejects_existing_target(
    tmp_path: Path,
) -> None:
    target = (tmp_path / "census").resolve()
    payloads = {
        "consensus.csv": b"header\n",
        "results.json": b"{}\n",
        "manifest.json": b"{}\n",
    }

    _publish_artifacts_v2(target=target, payloads=payloads)

    assert {path.name for path in target.iterdir()} == set(payloads)
    assert all((target / name).read_bytes() == payload for name, payload in payloads.items())
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="fresh target"):
        _publish_artifacts_v2(target=target, payloads=payloads)


def test_results_protocol_and_authority_document_are_historical_only() -> None:
    anchor = _anchor()
    row = _compact_row(anchor)
    authority = HistoricalFuturesKlineAuthorityV2(
        asset="BONK",
        symbol="1000BONKUSDT",
        relative_data_path="futures/BONK__1000BONKUSDT__5m.csv.gz",
        data_sha256="8" * 64,
        manifest_sha256="9" * 64,
        row_count=10,
        first_open_time_ms=1,
        last_close_time_ms=2,
    )
    replay = HistoricalSourceReplayAuditV2(
        split="development",
        replay_dir=Path("ignored"),
        run_manifest_sha256="a" * 64,
        recommendations_sha256="b" * 64,
        recommendation_rows=1,
        anchor_rows=1,
        anchor_set_sha256="c" * 64,
        futures_input_sha256s=((authority.relative_data_path, authority.data_sha256),),
    )
    loaded = LoadedHistoricalRecommendationAnchorsV2(
        anchors=(anchor,),
        replay_audits=(replay,),
        anchor_set_sha256="d" * 64,
    )
    results = _census_results_document_v2(
        loaded=loaded,
        authorities=(authority,),
        rows=(row,),
        dispositions=(_disposition(anchor, row=row),),
        execution_contract=build_historical_execution_contract_v2(),
        contract_authority=_contract_authority(),
        numeric_provenance=_numeric_provenance(),
        maximum_anchors=None,
        consensus_csv_sha256="e" * 64,
    )

    assert results["protocol"] == HISTORICAL_THREE_FAMILY_CENSUS_PROTOCOL_V2
    assert results["historical_only"] is True
    assert results["probability"] is False
    assert results["probability_calibrated"] is False
    assert results["promoting"] is False
    assert results["target_return_used"] is False
    assert results["census_complete"] is True
    assert results["diagnostic_mode"] is False
    assert results["maximum_anchors"] is None


def test_maximum_anchor_validation_rejects_zero_negative_and_boolean() -> None:
    from signalbot.backtest.historical_three_family_census import (
        _validate_maximum_anchors,
    )

    for value in (0, -1, True):
        with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match="positive integer"):
            _validate_maximum_anchors(value)
    _validate_maximum_anchors(None)
    _validate_maximum_anchors(1)


def test_contract_hashes_are_required_and_bad_cli_or_api_values_fail_early(
    tmp_path: Path,
) -> None:
    signature = inspect.signature(run_historical_three_family_census_v2)
    assert signature.parameters["topology_contract_sha256"].default is (
        inspect.Parameter.empty
    )
    assert signature.parameters["code_freeze_manifest_sha256"].default is (
        inspect.Parameter.empty
    )

    common = [
        "--workspace-root",
        str(tmp_path),
        "--output-dir",
        str(tmp_path / "output"),
        "--experiment-contract-sha256",
        _EXPERIMENT_HASH,
    ]
    with pytest.raises(SystemExit):
        main([*common, "--topology-contract-sha256", _TOPOLOGY_CONTRACT_HASH])
    with pytest.raises(SystemExit):
        main(
            [
                *common,
                "--topology-contract-sha256",
                "bad",
                "--code-freeze-manifest-sha256",
                _CODE_FREEZE_HASH,
            ]
        )
    with pytest.raises(SystemExit):
        main(
            [
                *common,
                "--topology-contract-sha256",
                _TOPOLOGY_CONTRACT_HASH,
                "--code-freeze-manifest-sha256",
                "bad",
            ]
        )
    with pytest.raises(
        HistoricalThreeFamilyCensusErrorV2,
        match="code_freeze_manifest_sha256",
    ):
        run_historical_three_family_census_v2(
            replay_dirs={},
            data_dir=tmp_path,
            output_dir=tmp_path / "never-written",
            experiment_contract_sha256=_EXPERIMENT_HASH,
            topology_contract_sha256=_TOPOLOGY_CONTRACT_HASH,
            code_freeze_manifest_sha256="bad",
        )


def test_workspace_contract_files_match_caller_bound_raw_hashes() -> None:
    workspace = Path(__file__).resolve().parents[2]
    experiment_sha256 = (
        "e3c29f4a7e6fc87750a8ccf84e3b59df26ba6557ed9cb16da02b210f11fec125"
    )
    topology_sha256 = (
        "4828ebab7a0409372d5c702c0801483f9da6d3f32dd220f1b0b9f3f8d12ece19"
    )

    authority = _contract_authority_v2(
        experiment_contract_sha256=experiment_sha256,
        topology_contract_sha256=topology_sha256,
        code_freeze_manifest_sha256=_CODE_FREEZE_HASH,
        workspace_root=workspace,
    )

    assert authority.workspace_files_verified is True
    assert authority.experiment_contract_sha256 == experiment_sha256
    assert authority.topology_contract_sha256 == topology_sha256
    with pytest.raises(HistoricalThreeFamilyCensusErrorV2, match=r"topology.*hash"):
        _contract_authority_v2(
            experiment_contract_sha256=experiment_sha256,
            topology_contract_sha256="0" * 64,
            code_freeze_manifest_sha256=_CODE_FREEZE_HASH,
            workspace_root=workspace,
        )


def test_compact_row_replacement_demonstrates_hash_accounting_changes() -> None:
    anchor = _anchor()
    row = _compact_row(anchor)
    first = _consensus_csv_bytes_v2((row,))
    changed = replace(row, directional_agreement_micros=500_001)
    second = _consensus_csv_bytes_v2((changed,))
    assert first != second
