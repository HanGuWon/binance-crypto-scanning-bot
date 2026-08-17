from __future__ import annotations

import hashlib
from dataclasses import fields, replace
from decimal import Decimal
from functools import cache

import pytest

from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.protocol.features import RobustZStatusV2
from signalbot.r4b_v2.strategy.cross_sectional_historical_7asset_proxy import (
    HistoricalCrossSectional7AssetProxyInputV2,
    HistoricalCrossSectional7AssetProxyStatusV2,
    HistoricalPeerCandlePathV2,
    build_historical_cross_sectional_7asset_proxy_v2,
    canonical_historical_cross_sectional_7asset_calculation_v2,
)
from signalbot.r4b_v2.strategy.family_c import FamilyCClosedCandleV2
from signalbot.r4b_v2.strategy.historical_numeric_precompute import (
    HISTORICAL_NUMERIC_PRECOMPUTE_MAX_ROWS_V2,
    HistoricalNumericPrecomputeContractErrorV2,
    HistoricalR3SeriesCacheV2,
    HistoricalTargetNumericCacheV2,
    build_historical_r3_series_cache_v2,
    build_historical_target_excluded_median_r3_cache_v2,
    build_historical_target_numeric_cache_v2,
    calculate_historical_cross_anchor_v2,
    calculate_historical_target_anchor_v2,
    canonical_historical_r3_series_cache_v2,
    canonical_historical_target_excluded_median_r3_cache_v2,
    canonical_historical_target_numeric_cache_v2,
)
from signalbot.r4b_v2.strategy.participation_evidence import (
    canonical_participation_flow_calculation_v2,
)
from signalbot.r4b_v2.strategy.participation_historical_kline_proxy import (
    build_participation_historical_kline_proxy_v2,
)
from signalbot.r4b_v2.strategy.price_evidence import (
    calculate_price_close_path_v2,
    canonical_price_close_path_calculation_v2,
)

SYMBOLS = (
    "1000BONKUSDT",
    "1000FLOKIUSDT",
    "ARBUSDT",
    "ENAUSDT",
    "OPUSDT",
    "SEIUSDT",
    "WIFUSDT",
)
TARGET = SYMBOLS[0]
ROW_COUNT = 8_655
FIRST_OPEN_MS = 5_900_000 * FIVE_MINUTE_MS_V2


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@cache
def _rows(symbol: str, mode: str = "varied") -> tuple[Candle, ...]:
    symbol_index = SYMBOLS.index(symbol)
    base = Decimal(100 + symbol_index * 20)
    result: list[Candle] = []
    for index in range(ROW_COUNT):
        open_ms = FIRST_OPEN_MS + index * FIVE_MINUTE_MS_V2
        if mode == "constant":
            close = base
        else:
            close = (
                base
                + Decimal(index) * Decimal("0.001")
                + Decimal(index % (11 + symbol_index)) * Decimal("0.0001")
            )
        quote_volume = Decimal(1_000 + symbol_index * 10 + index % 17)
        taker_buy_quote = quote_volume * (
            Decimal("0.42") + Decimal(index % 9) * Decimal("0.01")
        )
        volume = Decimal(100 + symbol_index + index % 7)
        result.append(
            Candle(
                market=Market.FUTURES,
                symbol=symbol,
                interval="5m",
                open_time_ms=open_ms,
                close_time_ms=open_ms + FIVE_MINUTE_MS_V2 - 1,
                open=close,
                high=close + Decimal(1),
                low=close - Decimal(1),
                close=close,
                volume=volume,
                quote_volume=quote_volume,
                trade_count=100 + index % 13,
                taker_buy_base_volume=volume / Decimal(2),
                taker_buy_quote_volume=taker_buy_quote,
                is_closed=True,
            )
        )
    return tuple(result)


@cache
def _r3(symbol: str, mode: str = "varied") -> HistoricalR3SeriesCacheV2:
    return build_historical_r3_series_cache_v2(
        dataset_sha256=_sha(f"dataset:{symbol}:{mode}"),
        manifest_sha256=_sha(f"manifest:{symbol}:{mode}"),
        rows=_rows(symbol, mode),
    )


@cache
def _target(mode: str = "varied") -> HistoricalTargetNumericCacheV2:
    return build_historical_target_numeric_cache_v2(
        source_r3_cache=_r3(TARGET, mode),
        rows=_rows(TARGET, mode),
    )


@cache
def _cross(mode: str = "varied"):
    return build_historical_target_excluded_median_r3_cache_v2(
        target_symbol=TARGET,
        peer_caches=tuple(_r3(symbol, mode) for symbol in SYMBOLS[1:]),
    )


def _family_c_path(symbol: str, *, final_index: int) -> HistoricalPeerCandlePathV2:
    rows = _rows(symbol)[final_index - 8_643 : final_index + 1]
    candles = tuple(
        FamilyCClosedCandleV2(
            symbol=symbol,
            bar_open_ms=row.open_time_ms,
            bar_close_ms=row.close_time_ms,
            event_time_ms=row.close_time_ms,
            receipt_time_ms=row.close_time_ms,
            close=row.close,
            source_evidence_sha256=_sha(f"family-c:{symbol}:{row.open_time_ms}"),
        )
        for row in rows
    )
    return HistoricalPeerCandlePathV2(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        candles=candles,
    )


def test_dataset_and_target_caches_are_bounded_canonical_and_nonauthoritative() -> None:
    r3 = _r3(TARGET)
    target = _target()

    assert r3.row_count == target.row_count == ROW_COUNT
    assert len(r3.returns_3) == ROW_COUNT - 3
    assert len(target.returns_1) == ROW_COUNT - 1
    assert len(target.returns_12) == ROW_COUNT - 12
    assert len(target.participation_bars) == ROW_COUNT
    assert r3.final_bar_open_ms == target.final_bar_open_ms
    assert not hasattr(r3, "rows")
    assert not hasattr(target, "rows")
    assert r3.historical_only and r3.numeric_only
    assert target.historical_only and target.numeric_only and target.target_scoped
    for value in (r3, target):
        assert not value.live_authority
        assert not value.promoting
        assert not value.probability
        assert not value.outcome_used
        assert value.data_through_ms is None
    assert canonical_historical_r3_series_cache_v2(r3)
    assert canonical_historical_target_numeric_cache_v2(target)


def test_cached_target_calculations_match_per_anchor_row_owners_byte_exactly() -> None:
    target = _target()
    final_open_ms = target.final_bar_open_ms
    inputs = target.anchor_inputs_at(final_open_ms)
    repeated = target.anchor_inputs_at(final_open_ms)
    cached = calculate_historical_target_anchor_v2(inputs)
    rows = _rows(TARGET)

    expected_price = calculate_price_close_path_v2(
        tuple(row.close for row in rows[-8_653:])
    )
    participation_proxy = build_participation_historical_kline_proxy_v2(
        attempt_id="historical-precompute-parity",
        dataset_sha256=_sha("dataset:1000BONKUSDT:varied"),
        bar_open_ms=final_open_ms,
        rows=rows[-8_641:],
    )

    assert repeated == inputs
    assert len(inputs.returns_1) == len(inputs.returns_12) == 8_641
    assert len(inputs.prior_participation_bars) == 8_640
    assert inputs.current_participation_bar.bar_open_ms == final_open_ms
    assert cached.price == expected_price
    assert cached.price.calculation_sha256 == expected_price.calculation_sha256
    assert canonical_price_close_path_calculation_v2(
        cached.price
    ) == canonical_price_close_path_calculation_v2(expected_price)
    assert participation_proxy.calculation is not None
    assert cached.participation == participation_proxy.calculation
    assert (
        cached.participation.calculation_sha256
        == participation_proxy.calculation.calculation_sha256
    )
    assert canonical_participation_flow_calculation_v2(
        cached.participation
    ) == canonical_participation_flow_calculation_v2(
        participation_proxy.calculation
    )


def test_cached_cross_calculation_matches_full_peer_paths_exactly() -> None:
    cache_value = _cross()
    final_index = ROW_COUNT - 1
    final_open_ms = FIRST_OPEN_MS + final_index * FIVE_MINUTE_MS_V2
    inputs = cache_value.anchor_inputs_at(final_open_ms)
    cached = calculate_historical_cross_anchor_v2(inputs)
    paths = tuple(_family_c_path(symbol, final_index=final_index) for symbol in SYMBOLS[1:])
    source = HistoricalCrossSectional7AssetProxyInputV2(
        target_symbol=TARGET,
        peer_paths=paths,
    )
    exact = build_historical_cross_sectional_7asset_proxy_v2(source)

    assert cache_value.target_returns_used is False
    assert TARGET not in cache_value.peer_symbols
    assert len(inputs.prior_market_median_returns_3) == 8_640
    assert len(inputs.current_peer_returns_3) == 6
    assert cached.calculation.calculation_sha256 == exact.calculation_sha256
    assert cached.calculation.status is exact.status
    assert cached.calculation.m3_ex_target == exact.m3_ex_target
    assert cached.calculation.shock_scale == exact.shock_scale
    assert cached.calculation.direction == exact.direction
    assert cached.calculation.strength_micros == exact.strength_micros
    assert canonical_historical_cross_sectional_7asset_calculation_v2(
        cached.calculation
    )
    assert canonical_historical_target_excluded_median_r3_cache_v2(cache_value)


def test_peer_input_order_is_canonical_and_calculation_invariant() -> None:
    peers = tuple(_r3(symbol) for symbol in SYMBOLS[1:])
    forward = build_historical_target_excluded_median_r3_cache_v2(
        target_symbol=TARGET,
        peer_caches=peers,
    )
    reverse = build_historical_target_excluded_median_r3_cache_v2(
        target_symbol=TARGET,
        peer_caches=tuple(reversed(peers)),
    )
    final_open_ms = forward.final_return_open_ms

    assert reverse == forward
    assert reverse.peer_symbols == tuple(sorted(SYMBOLS[1:], key=str.encode))
    assert reverse.cache_sha256 == forward.cache_sha256
    assert reverse.anchor_inputs_at(final_open_ms) == forward.anchor_inputs_at(
        final_open_ms
    )
    assert calculate_historical_cross_anchor_v2(
        reverse.anchor_inputs_at(final_open_ms)
    ) == calculate_historical_cross_anchor_v2(
        forward.anchor_inputs_at(final_open_ms)
    )


def test_anchor_hashes_match_existing_compact_dataset_root_documents() -> None:
    target = _target()
    final_open_ms = target.final_bar_open_ms
    final_close_ms = final_open_ms + FIVE_MINUTE_MS_V2 - 1
    target_inputs = target.anchor_inputs_at(final_open_ms)

    def target_hash(
        domain: bytes,
        *,
        representation: str,
        first_open_ms: int,
        row_count: int,
    ) -> str:
        return hashlib.sha256(
            domain
            + canonical_json_line(
                {
                    "dataset_sha256": target.dataset_sha256,
                    "final_close_ms": final_close_ms,
                    "final_open_ms": final_open_ms,
                    "first_open_ms": first_open_ms,
                    "historical_receipt_policy": (
                        "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME"
                    ),
                    "interval": "5m",
                    "market": "futures",
                    "representation": representation,
                    "row_count": row_count,
                    "schema_version": (
                        "r4b_historical_numeric_dataset_root_slice_v2"
                    ),
                    "symbol": target.symbol,
                }
            )
        ).hexdigest()

    assert target_inputs.price_source_slice_sha256 == target_hash(
        b"R4B_HISTORICAL_CENSUS_PRICE_NUMERIC_SLICE_V2\0",
        representation="PRICE_CLOSE_PATH_DATASET_ROOT_WINDOW",
        first_open_ms=final_open_ms - 8_652 * FIVE_MINUTE_MS_V2,
        row_count=8_653,
    )
    assert target_inputs.participation_source_slice_sha256 == target_hash(
        b"R4B_HISTORICAL_CENSUS_PARTICIPATION_NUMERIC_SLICE_V2\0",
        representation=(
            "ALL_TRADES_ASSUMED_NORMAL_KLINE_PROXY_DATASET_ROOT_WINDOW"
        ),
        first_open_ms=final_open_ms - 8_640 * FIVE_MINUTE_MS_V2,
        row_count=8_641,
    )

    cross = _cross()
    cross_inputs = cross.anchor_inputs_at(final_open_ms)
    expected_paths = []
    for cache_value in cross.peer_caches:
        expected_paths.append(
            (
                cache_value.symbol,
                hashlib.sha256(
                    b"R4B_HISTORICAL_CENSUS_CROSS_NUMERIC_PATH_V2\0"
                    + canonical_json_line(
                        {
                            "dataset_sha256": cache_value.dataset_sha256,
                            "final_close_ms": final_close_ms,
                            "final_open_ms": final_open_ms,
                            "first_open_ms": final_open_ms
                            - 8_643 * FIVE_MINUTE_MS_V2,
                            "historical_receipt_policy": (
                                "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME"
                            ),
                            "interval": "5m",
                            "manifest_sha256": cache_value.manifest_sha256,
                            "market": "futures",
                            "representation": (
                                "CROSS_CLOSE_PATH_DATASET_ROOT_WINDOW"
                            ),
                            "row_count": 8_644,
                            "schema_version": (
                                "r4b_historical_cross_numeric_peer_path_v2"
                            ),
                            "symbol": cache_value.symbol,
                        }
                    )
                ).hexdigest(),
            )
        )
    expected_path_tuple = tuple(expected_paths)
    assert cross_inputs.peer_path_sha256s == expected_path_tuple
    assert cross_inputs.peer_input_sha256 == hashlib.sha256(
        b"R4B_HISTORICAL_CENSUS_CROSS_NUMERIC_INPUT_V2\0"
        + canonical_json_line(
            {
                "final_close_ms": final_close_ms,
                "final_open_ms": final_open_ms,
                "historical_receipt_policy": (
                    "RECEIPT_EQUALS_CLOSED_KLINE_CLOSE_TIME"
                ),
                "interval": "5m",
                "market": "futures",
                "peer_paths": [
                    {"path_sha256": digest, "symbol": symbol}
                    for symbol, digest in expected_path_tuple
                ],
                "representation": "TARGET_EXCLUDED_DATASET_ROOT_PEER_WINDOWS",
                "schema_version": "r4b_historical_cross_numeric_input_v2",
                "target_symbol": TARGET,
            }
        )
    ).hexdigest()


def test_future_rows_do_not_change_earlier_numeric_calculations() -> None:
    rows = _rows(TARGET)
    earlier_open_ms = rows[-2].open_time_ms
    baseline_r3 = build_historical_r3_series_cache_v2(
        dataset_sha256=_sha("future-baseline"),
        manifest_sha256=_sha("future-manifest"),
        rows=rows,
    )
    changed_rows = (
        *rows[:-1],
        rows[-1].model_copy(
            update={
                "open": rows[-1].open * Decimal("1.2"),
                "high": rows[-1].high * Decimal("1.2"),
                "low": rows[-1].low * Decimal("1.2"),
                "close": rows[-1].close * Decimal("1.2"),
            }
        ),
    )
    changed_r3 = build_historical_r3_series_cache_v2(
        dataset_sha256=_sha("future-changed"),
        manifest_sha256=_sha("future-manifest"),
        rows=changed_rows,
    )
    baseline_target = build_historical_target_numeric_cache_v2(
        source_r3_cache=baseline_r3,
        rows=rows,
    )
    changed_target = build_historical_target_numeric_cache_v2(
        source_r3_cache=changed_r3,
        rows=changed_rows,
    )

    baseline = calculate_historical_target_anchor_v2(
        baseline_target.anchor_inputs_at(earlier_open_ms)
    )
    changed = calculate_historical_target_anchor_v2(
        changed_target.anchor_inputs_at(earlier_open_ms)
    )

    assert baseline.price.calculation_sha256 == changed.price.calculation_sha256
    assert (
        baseline.participation.calculation_sha256
        == changed.participation.calculation_sha256
    )


@pytest.mark.parametrize("defect", ("gap", "order", "unclosed", "spot"))
def test_source_cache_rejects_gap_order_unclosed_and_wrong_market(defect: str) -> None:
    rows = list(_rows(TARGET))
    index = 100
    if defect == "gap":
        row = rows[index]
        rows[index] = row.model_copy(
            update={
                "open_time_ms": row.open_time_ms + FIVE_MINUTE_MS_V2,
                "close_time_ms": row.close_time_ms + FIVE_MINUTE_MS_V2,
            }
        )
    elif defect == "order":
        rows[index], rows[index + 1] = rows[index + 1], rows[index]
    elif defect == "unclosed":
        rows[index] = rows[index].model_copy(update={"is_closed": False})
    else:
        rows[index] = rows[index].model_copy(update={"market": Market.SPOT})

    with pytest.raises(HistoricalNumericPrecomputeContractErrorV2):
        build_historical_r3_series_cache_v2(
            dataset_sha256=_sha(f"defect:{defect}"),
            manifest_sha256=_sha("defect-manifest"),
            rows=tuple(rows),
        )


def test_row_count_bounds_and_mutable_input_fail_closed() -> None:
    rows = _rows(TARGET)
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="row count",
    ):
        build_historical_r3_series_cache_v2(
            dataset_sha256=_sha("short"),
            manifest_sha256=_sha("short-manifest"),
            rows=rows[:8_652],
        )
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="immutable tuple",
    ):
        build_historical_r3_series_cache_v2(
            dataset_sha256=_sha("mutable"),
            manifest_sha256=_sha("mutable-manifest"),
            rows=list(rows),  # type: ignore[arg-type]
        )

    oversized = (rows[0],) * (HISTORICAL_NUMERIC_PRECOMPUTE_MAX_ROWS_V2 + 1)
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="row count",
    ):
        build_historical_r3_series_cache_v2(
            dataset_sha256=_sha("oversized"),
            manifest_sha256=_sha("oversized-manifest"),
            rows=oversized,
        )


def test_anchor_lookup_rejects_warmup_misalignment_and_outside_cache() -> None:
    target = _target()
    too_early = FIRST_OPEN_MS + 8_651 * FIVE_MINUTE_MS_V2

    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="lacks the exact 8641-value",
    ):
        target.anchor_inputs_at(too_early)
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="no exact aligned value",
    ):
        target.anchor_inputs_at(target.final_bar_open_ms - 1)
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="outside the bounded cache",
    ):
        target.anchor_inputs_at(target.final_bar_open_ms + FIVE_MINUTE_MS_V2)

    cross = _cross()
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="lacks 8640 prior values",
    ):
        cross.anchor_inputs_at(cross.first_return_open_ms)


def test_target_exclusion_duplicate_and_peer_count_fail_closed() -> None:
    peers = tuple(_r3(symbol) for symbol in SYMBOLS[1:])
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="exactly 6",
    ):
        build_historical_target_excluded_median_r3_cache_v2(
            target_symbol=TARGET,
            peer_caches=peers[:5],
        )
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="duplicate",
    ):
        build_historical_target_excluded_median_r3_cache_v2(
            target_symbol=TARGET,
            peer_caches=(*peers[:5], peers[0]),
        )
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="target returns",
    ):
        build_historical_target_excluded_median_r3_cache_v2(
            target_symbol=TARGET,
            peer_caches=(_r3(TARGET), *peers[:5]),
        )


def test_shifted_peer_start_uses_exact_common_overlap() -> None:
    shifted_symbol = SYMBOLS[1]
    shifted_rows = _rows(shifted_symbol)[1:]
    shifted = build_historical_r3_series_cache_v2(
        dataset_sha256=_sha("shifted-dataset"),
        manifest_sha256=_sha("shifted-manifest"),
        rows=shifted_rows,
    )
    peers = (shifted, *tuple(_r3(symbol) for symbol in SYMBOLS[2:]))
    value = build_historical_target_excluded_median_r3_cache_v2(
        target_symbol=TARGET,
        peer_caches=peers,
    )

    assert value.first_return_open_ms == shifted.first_return_open_ms
    assert value.final_return_open_ms == min(
        cache.final_bar_open_ms for cache in peers
    )
    assert value.anchor_inputs_at(value.final_return_open_ms)


def test_constant_paths_preserve_zero_scale_without_partial_values() -> None:
    target = _target("constant")
    target_calculations = calculate_historical_target_anchor_v2(
        target.anchor_inputs_at(target.final_bar_open_ms)
    )
    cross = _cross("constant")
    cross_calculation = calculate_historical_cross_anchor_v2(
        cross.anchor_inputs_at(cross.final_return_open_ms)
    ).calculation

    assert target_calculations.price.status is (
        RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    )
    assert target_calculations.price.composite is None
    assert target_calculations.price.direction == 0
    assert cross_calculation.status is (
        HistoricalCrossSectional7AssetProxyStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    )
    assert cross_calculation.m3_ex_target is None
    assert cross_calculation.direction == 0


def test_target_rows_must_match_source_cache_close_identity() -> None:
    rows = _rows(TARGET)
    changed = list(rows)
    row = changed[-1]
    changed[-1] = row.model_copy(
        update={
            "open": row.open + Decimal(1),
            "high": row.high + Decimal(1),
            "low": row.low + Decimal(1),
            "close": row.close + Decimal(1),
        }
    )

    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="close series differs",
    ):
        build_historical_target_numeric_cache_v2(
            source_r3_cache=_r3(TARGET),
            rows=tuple(changed),
        )


def test_cache_factories_and_canonical_tamper_checks_fail_closed() -> None:
    value = _r3(TARGET)
    constructor_values = {
        item.name: getattr(value, item.name) for item in fields(value) if item.init
    }
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="sealed factory",
    ):
        HistoricalR3SeriesCacheV2(**constructor_values)  # type: ignore[arg-type]
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="sealed factory",
    ):
        replace(value, symbol="ETHUSDT")

    tampered = build_historical_target_numeric_cache_v2(
        source_r3_cache=_r3(TARGET),
        rows=_rows(TARGET),
    )
    object.__setattr__(tampered, "returns_1_sha256", "0" * 64)
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="component hash differs",
    ):
        canonical_historical_target_numeric_cache_v2(tampered)


def test_cache_builders_reject_tampered_source_cache_identity() -> None:
    target_source = build_historical_r3_series_cache_v2(
        dataset_sha256=_sha("tampered-target-source"),
        manifest_sha256=_sha("tampered-target-manifest"),
        rows=_rows(TARGET),
    )
    object.__setattr__(target_source, "cache_sha256", "0" * 64)
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="hash differs from canonical content",
    ):
        build_historical_target_numeric_cache_v2(
            source_r3_cache=target_source,
            rows=_rows(TARGET),
        )

    peer_symbol = SYMBOLS[1]
    tampered_peer = build_historical_r3_series_cache_v2(
        dataset_sha256=_sha("tampered-peer-source"),
        manifest_sha256=_sha("tampered-peer-manifest"),
        rows=_rows(peer_symbol),
    )
    object.__setattr__(tampered_peer, "cache_sha256", "0" * 64)
    with pytest.raises(
        HistoricalNumericPrecomputeContractErrorV2,
        match="hash differs from canonical content",
    ):
        build_historical_target_excluded_median_r3_cache_v2(
            target_symbol=TARGET,
            peer_caches=(
                tampered_peer,
                *tuple(_r3(symbol) for symbol in SYMBOLS[2:]),
            ),
        )
