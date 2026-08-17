from __future__ import annotations

import hashlib
import json
from dataclasses import fields, replace
from decimal import Decimal, localcontext
from functools import cache

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.cross_sectional_historical_7asset_proxy import (
    HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2,
    HistoricalCrossSectional7AssetCalculationV2,
    HistoricalCrossSectional7AssetProxyContractErrorV2,
    HistoricalCrossSectional7AssetProxyInputV2,
    HistoricalCrossSectional7AssetProxyStatusV2,
    HistoricalCrossSectional7AssetProxyV2,
    HistoricalPeerCandlePathV2,
    build_historical_cross_sectional_7asset_proxy_v2,
    calculate_historical_cross_sectional_7asset_returns_v2,
    canonical_historical_cross_sectional_7asset_calculation_v2,
    canonical_historical_cross_sectional_7asset_proxy_v2,
    select_exact_historical_peer_candle_path_v2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FAMILY_C_PANEL_BAR_COUNT_V2,
    FIVE_MINUTE_MS_V2,
    FamilyCClosedCandleV2,
)

TARGET = "BTCUSDT"
PEERS = (
    "ARBUSDT",
    "BONKUSDT",
    "ENAUSDT",
    "ETHUSDT",
    "OPUSDT",
    "SEIUSDT",
)
FIRST_BAR_OPEN_MS = 5_700_000 * FIVE_MINUTE_MS_V2


def _source_hash(symbol: str, index: int) -> str:
    return hashlib.sha256(f"{symbol}:{index}".encode()).hexdigest()


def _peer_index(symbol: str) -> int:
    if symbol in PEERS:
        return PEERS.index(symbol)
    return len(PEERS)


@cache
def _candles(
    symbol: str,
    mode: str = "bullish",
) -> tuple[FamilyCClosedCandleV2, ...]:
    peer_index = _peer_index(symbol)
    base = Decimal(100 + peer_index * 20)
    rows: list[FamilyCClosedCandleV2] = []
    for index in range(FAMILY_C_PANEL_BAR_COUNT_V2):
        bar_open_ms = FIRST_BAR_OPEN_MS + index * FIVE_MINUTE_MS_V2
        bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
        if mode == "zero_scale":
            close = base
        else:
            close = (
                base
                + Decimal(index) * Decimal("0.01")
                + Decimal(index % 17) * Decimal("0.001")
            )
        rows.append(
            FamilyCClosedCandleV2(
                symbol=symbol,
                bar_open_ms=bar_open_ms,
                bar_close_ms=bar_close_ms,
                event_time_ms=bar_close_ms,
                receipt_time_ms=bar_close_ms,
                close=close,
                source_evidence_sha256=_source_hash(symbol, index),
            )
        )
    if mode != "zero_scale":
        current_base = rows[-4].close
        if mode == "bullish":
            ratio = Decimal(1) + Decimal(peer_index + 1) * Decimal("0.001")
        elif mode == "bearish":
            ratio = Decimal(1) - Decimal(peer_index + 1) * Decimal("0.001")
        elif mode == "neutral":
            ratio = Decimal(1)
        else:
            raise AssertionError(f"unsupported fixture mode: {mode}")
        rows[-1] = replace(rows[-1], close=current_base * ratio)
    return tuple(rows)


@cache
def _path(symbol: str, mode: str = "bullish") -> HistoricalPeerCandlePathV2:
    return HistoricalPeerCandlePathV2(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        candles=_candles(symbol, mode),
    )


@cache
def _input(mode: str = "bullish") -> HistoricalCrossSectional7AssetProxyInputV2:
    return HistoricalCrossSectional7AssetProxyInputV2(
        target_symbol=TARGET,
        peer_paths=tuple(_path(symbol, mode) for symbol in PEERS),
    )


@cache
def _proxy(mode: str = "bullish") -> HistoricalCrossSectional7AssetProxyV2:
    return build_historical_cross_sectional_7asset_proxy_v2(_input(mode))


@cache
def _numeric_inputs(
    mode: str = "bullish",
) -> tuple[tuple[Decimal, ...], tuple[Decimal, ...]]:
    prior_rows: list[list[Decimal]] = [
        [] for _ in range(FAMILY_C_PANEL_BAR_COUNT_V2 - 4)
    ]
    current: list[Decimal] = []
    with localcontext(protocol_decimal_context_v2()):
        for path in _input(mode).peer_paths:
            closes = tuple(candle.close for candle in path.candles)
            prior = tuple(
                (closes[index] / closes[index - 3]).ln()
                for index in range(3, FAMILY_C_PANEL_BAR_COUNT_V2 - 1)
            )
            for index, value in enumerate(prior):
                prior_rows[index].append(value)
            current.append((closes[-1] / closes[-4]).ln())
        prior_market = tuple(_test_median(tuple(row)) for row in prior_rows)
    return prior_market, tuple(current)


def _test_median(values: tuple[Decimal, ...]) -> Decimal:
    ordered = tuple(sorted(values))
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _with_path(
    source: HistoricalCrossSectional7AssetProxyInputV2,
    replacement: HistoricalPeerCandlePathV2,
) -> HistoricalCrossSectional7AssetProxyInputV2:
    return HistoricalCrossSectional7AssetProxyInputV2(
        target_symbol=source.target_symbol,
        peer_paths=tuple(
            replacement if path.symbol == replacement.symbol else path
            for path in source.peer_paths
        ),
    )


def test_exact_six_peer_boundary_is_immutable_canonical_and_stable() -> None:
    paths = _input().peer_paths
    reversed_input = HistoricalCrossSectional7AssetProxyInputV2(
        target_symbol=TARGET,
        peer_paths=tuple(reversed(paths)),
    )

    assert HISTORICAL_CROSS_SECTIONAL_7ASSET_PEER_COUNT_V2 == 6
    assert reversed_input.peer_symbols == PEERS
    assert reversed_input.input_sha256 == _input().input_sha256
    assert len(reversed_input.peer_paths) == 6

    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="exactly 6 peer paths",
    ):
        HistoricalCrossSectional7AssetProxyInputV2(
            target_symbol=TARGET,
            peer_paths=paths[:5],
        )
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="exactly 6 peer paths",
    ):
        HistoricalCrossSectional7AssetProxyInputV2(
            target_symbol=TARGET,
            peer_paths=(*paths, paths[0]),
        )
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="immutable tuple",
    ):
        HistoricalCrossSectional7AssetProxyInputV2(
            target_symbol=TARGET,
            peer_paths=list(paths),  # type: ignore[arg-type]
        )


def test_target_path_is_rejected_and_external_target_mutation_is_invariant() -> None:
    target_before = _path(TARGET)
    target_after = HistoricalPeerCandlePathV2(
        symbol=TARGET,
        venue=VenueV2.USDM_FUTURES,
        candles=(
            *target_before.candles[:-1],
            replace(
                target_before.candles[-1],
                close=target_before.candles[-1].close * Decimal("1.5"),
            ),
        ),
    )
    paths = _input().peer_paths
    source_before = HistoricalCrossSectional7AssetProxyInputV2(
        target_symbol=target_before.symbol,
        peer_paths=paths,
    )
    source_after = HistoricalCrossSectional7AssetProxyInputV2(
        target_symbol=target_after.symbol,
        peer_paths=paths,
    )

    assert target_before.path_sha256 != target_after.path_sha256
    assert "target_candles" not in {item.name for item in fields(_input())}
    assert source_before == source_after
    assert build_historical_cross_sectional_7asset_proxy_v2(
        source_before
    ) == build_historical_cross_sectional_7asset_proxy_v2(source_after)
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="target candles must not be accepted",
    ):
        HistoricalCrossSectional7AssetProxyInputV2(
            target_symbol=TARGET,
            peer_paths=(target_after, *paths[:5]),
        )


def test_one_peer_current_candle_mutation_changes_bound_proxy_evidence() -> None:
    source = _input()
    original = next(path for path in source.peer_paths if path.symbol == "ENAUSDT")
    changed_path = HistoricalPeerCandlePathV2(
        symbol=original.symbol,
        venue=original.venue,
        candles=(
            *original.candles[:-1],
            replace(
                original.candles[-1],
                close=original.candles[-4].close * Decimal("1.05"),
            ),
        ),
    )
    changed_source = _with_path(source, changed_path)
    baseline = _proxy()
    changed = build_historical_cross_sectional_7asset_proxy_v2(changed_source)

    assert original.path_sha256 != changed_path.path_sha256
    assert source.input_sha256 != changed_source.input_sha256
    assert baseline.m3_ex_target != changed.m3_ex_target
    assert baseline.shock_score != changed.shock_score
    assert baseline.proxy_sha256 != changed.proxy_sha256


def test_future_only_mutation_cannot_enter_the_exact_selected_window() -> None:
    path = _path(PEERS[0])
    final = path.candles[-1]
    future_open_ms = final.bar_open_ms + FIVE_MINUTE_MS_V2
    future_close_ms = future_open_ms + FIVE_MINUTE_MS_V2 - 1
    future = FamilyCClosedCandleV2(
        symbol=path.symbol,
        bar_open_ms=future_open_ms,
        bar_close_ms=future_close_ms,
        event_time_ms=future_close_ms,
        receipt_time_ms=future_close_ms,
        close=final.close * Decimal("2"),
        source_evidence_sha256=_source_hash(path.symbol, 99_999),
    )
    changed_future = replace(future, close=future.close * Decimal("3"))

    first = select_exact_historical_peer_candle_path_v2(
        symbol=path.symbol,
        venue=path.venue,
        candles=(*path.candles, future),
        final_decision_bar_open_ms=path.final_bar_open_ms,
    )
    second = select_exact_historical_peer_candle_path_v2(
        symbol=path.symbol,
        venue=path.venue,
        candles=(*path.candles, changed_future),
        final_decision_bar_open_ms=path.final_bar_open_ms,
    )

    assert first == second == path
    assert first.path_sha256 == second.path_sha256
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="exactly 8,644 candles",
    ):
        HistoricalPeerCandlePathV2(
            symbol=path.symbol,
            venue=path.venue,
            candles=(*path.candles, future),
        )


@pytest.mark.parametrize("defect", ("gap", "order", "unclosed", "identity"))
def test_peer_path_rejects_gap_order_unclosed_and_wrong_identity(defect: str) -> None:
    path = _path(PEERS[0])
    rows = list(path.candles)
    index = 100
    if defect == "gap":
        candle = rows[index]
        rows[index] = replace(
            candle,
            bar_open_ms=candle.bar_open_ms + FIVE_MINUTE_MS_V2,
            bar_close_ms=candle.bar_close_ms + FIVE_MINUTE_MS_V2,
            event_time_ms=candle.event_time_ms + FIVE_MINUTE_MS_V2,
            receipt_time_ms=candle.receipt_time_ms + FIVE_MINUTE_MS_V2,
        )
    elif defect == "order":
        rows[index], rows[index + 1] = rows[index + 1], rows[index]
    elif defect == "unclosed":
        hostile = replace(rows[index])
        object.__setattr__(hostile, "closed", False)
        rows[index] = hostile
    else:
        rows[index] = replace(rows[index], symbol="XRPUSDT")

    with pytest.raises(HistoricalCrossSectional7AssetProxyContractErrorV2):
        HistoricalPeerCandlePathV2(
            symbol=path.symbol,
            venue=path.venue,
            candles=tuple(rows),
        )


def test_paths_reject_wrong_venue_and_input_rejects_different_final_bar() -> None:
    path = _path(PEERS[0])
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="USD-M Futures",
    ):
        HistoricalPeerCandlePathV2(
            symbol=path.symbol,
            venue=VenueV2.SPOT,
            candles=path.candles,
        )

    shifted_rows = tuple(
        replace(
            candle,
            bar_open_ms=candle.bar_open_ms + FIVE_MINUTE_MS_V2,
            bar_close_ms=candle.bar_close_ms + FIVE_MINUTE_MS_V2,
            event_time_ms=candle.event_time_ms + FIVE_MINUTE_MS_V2,
            receipt_time_ms=candle.receipt_time_ms + FIVE_MINUTE_MS_V2,
        )
        for candle in path.candles
    )
    shifted = HistoricalPeerCandlePathV2(
        symbol=path.symbol,
        venue=path.venue,
        candles=shifted_rows,
    )
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="same final decision bar",
    ):
        _with_path(_input(), shifted)


def test_bullish_bearish_neutral_and_zero_scale_mapping_is_exact() -> None:
    bullish = _proxy("bullish")
    bearish = _proxy("bearish")
    neutral = _proxy("neutral")
    zero_scale = _proxy("zero_scale")

    assert bullish.ready
    assert bullish.m3_ex_target is not None and bullish.m3_ex_target > 0
    assert bullish.direction == 1
    assert bullish.breadth_count == bullish.breadth_denominator == 6
    assert bullish.breadth_support == Decimal(1)
    assert bullish.strength_micros > 0
    assert bullish.signed_strength_micros == bullish.strength_micros

    assert bearish.ready
    assert bearish.m3_ex_target is not None and bearish.m3_ex_target < 0
    assert bearish.direction == -1
    assert bearish.breadth_count == bearish.breadth_denominator == 6
    assert bearish.breadth_support == Decimal(1)
    assert bearish.strength_micros > 0
    assert bearish.signed_strength_micros == -bearish.strength_micros

    assert neutral.ready
    assert neutral.m3_ex_target == neutral.shock_score == Decimal(0)
    assert neutral.shock_scale is not None and neutral.shock_scale > 0
    assert neutral.breadth_count == 0
    assert neutral.breadth_support == Decimal(0)
    assert neutral.direction == neutral.strength_micros == 0

    assert zero_scale.status is (
        HistoricalCrossSectional7AssetProxyStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    )
    assert not zero_scale.ready
    assert zero_scale.m3_ex_target is None
    assert zero_scale.shock_scale is None
    assert zero_scale.breadth_count is None
    assert zero_scale.direction == zero_scale.strength_micros == 0
    assert "DIRECTION_WITHHELD_NOT_NEUTRAL_FALLBACK" in zero_scale.reasons


def test_precomputed_returns_match_full_path_numeric_result_exactly() -> None:
    prior_market, current_peers = _numeric_inputs("bullish")
    calculation = calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=prior_market,
        current_peer_returns_3=current_peers,
    )
    proxy = _proxy("bullish")

    assert calculation.ready
    assert (
        calculation.status,
        calculation.reasons,
        calculation.m3_ex_target,
        calculation.shock_scale,
        calculation.shock_score,
        calculation.breadth_count,
        calculation.breadth_denominator,
        calculation.shock_magnitude,
        calculation.breadth_support,
        calculation.direction,
        calculation.strength_micros,
        calculation.signed_strength_micros,
    ) == (
        proxy.status,
        proxy.reasons,
        proxy.m3_ex_target,
        proxy.shock_scale,
        proxy.shock_score,
        proxy.breadth_count,
        proxy.breadth_denominator,
        proxy.shock_magnitude,
        proxy.breadth_support,
        proxy.direction,
        proxy.strength_micros,
        proxy.signed_strength_micros,
    )
    assert proxy.calculation_sha256 == calculation.calculation_sha256
    assert calculation.historical_only and calculation.numeric_only
    assert not calculation.live_authority
    assert not calculation.promoting
    assert not calculation.probability
    assert not calculation.outcome_used
    assert canonical_historical_cross_sectional_7asset_calculation_v2(calculation)


def test_current_peer_return_multiset_is_permutation_invariant() -> None:
    prior_market, current_peers = _numeric_inputs("bullish")

    forward = calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=prior_market,
        current_peer_returns_3=current_peers,
    )
    reversed_result = calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=prior_market,
        current_peer_returns_3=tuple(reversed(current_peers)),
    )

    assert reversed_result == forward
    assert reversed_result.calculation_sha256 == forward.calculation_sha256
    assert canonical_historical_cross_sectional_7asset_calculation_v2(
        reversed_result
    ) == canonical_historical_cross_sectional_7asset_calculation_v2(forward)


def test_precomputed_zero_scale_matches_full_path_with_no_partial_values() -> None:
    prior_market, current_peers = _numeric_inputs("zero_scale")
    calculation = calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=prior_market,
        current_peer_returns_3=current_peers,
    )
    proxy = _proxy("zero_scale")

    assert calculation.status is proxy.status is (
        HistoricalCrossSectional7AssetProxyStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    )
    assert calculation.m3_ex_target is proxy.m3_ex_target is None
    assert calculation.shock_scale is proxy.shock_scale is None
    assert calculation.breadth_count is proxy.breadth_count is None
    assert calculation.direction == calculation.strength_micros == 0
    assert proxy.calculation_sha256 == calculation.calculation_sha256


def test_precomputed_finite_overflow_is_invalid_without_partial_values() -> None:
    overflowing_prior = (
        Decimal("1e999999"),
        Decimal("-1e999999"),
    ) * 4_320

    calculation = calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=overflowing_prior,
        current_peer_returns_3=(Decimal(1),) * 6,
    )

    assert calculation.status is (
        HistoricalCrossSectional7AssetProxyStatusV2.DATA_INVALID_ARITHMETIC
    )
    assert calculation.m3_ex_target is None
    assert calculation.shock_scale is None
    assert calculation.breadth_count is None
    assert calculation.direction == calculation.strength_micros == 0
    assert canonical_historical_cross_sectional_7asset_calculation_v2(calculation)


def test_precomputed_even_median_and_breadth_boundaries_are_exact() -> None:
    prior_market = tuple(
        (Decimal(-1), Decimal(0), Decimal(1), Decimal(2))[index % 4]
        for index in range(8_640)
    )
    current_peers = (
        Decimal(-3),
        Decimal(-2),
        Decimal(1),
        Decimal(3),
        Decimal(4),
        Decimal(5),
    )

    calculation = calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=prior_market,
        current_peer_returns_3=current_peers,
    )

    assert calculation.ready
    assert calculation.m3_ex_target == Decimal(2)
    assert calculation.shock_scale == Decimal("1.4826")
    assert calculation.breadth_count == 4
    assert calculation.breadth_denominator == 6
    with localcontext(protocol_decimal_context_v2()):
        expected_breadth_support = Decimal(4) / Decimal(6)
    assert calculation.breadth_support == expected_breadth_support
    assert calculation.direction == 1
    assert calculation.strength_micros > 0


@pytest.mark.parametrize(
    ("prior_market", "current_peers", "message"),
    (
        ((Decimal(0),) * 8_639, (Decimal(0),) * 6, "8,640"),
        ((Decimal(0),) * 8_640, (Decimal(0),) * 5, "exactly 6"),
        (
            (Decimal(0),) * 8_639 + (Decimal("Infinity"),),
            (Decimal(0),) * 6,
            "finite Decimal",
        ),
        (
            (Decimal(0),) * 8_640,
            (Decimal(0),) * 5 + (Decimal("NaN"),),
            "finite Decimal",
        ),
    ),
)
def test_precomputed_return_contract_rejects_wrong_count_and_nonfinite(
    prior_market: tuple[Decimal, ...],
    current_peers: tuple[Decimal, ...],
    message: str,
) -> None:
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match=message,
    ):
        calculate_historical_cross_sectional_7asset_returns_v2(
            prior_market_median_returns_3=prior_market,
            current_peer_returns_3=current_peers,
        )


def test_precomputed_return_contract_rejects_mutable_inputs() -> None:
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="immutable",
    ):
        calculate_historical_cross_sectional_7asset_returns_v2(
            prior_market_median_returns_3=list((Decimal(0),) * 8_640),  # type: ignore[arg-type]
            current_peer_returns_3=(Decimal(0),) * 6,
        )
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="immutable",
    ):
        calculate_historical_cross_sectional_7asset_returns_v2(
            prior_market_median_returns_3=(Decimal(0),) * 8_640,
            current_peer_returns_3=list((Decimal(0),) * 6),  # type: ignore[arg-type]
        )


def test_precomputed_calculation_is_factory_sealed_and_tamper_checked() -> None:
    prior_market, current_peers = _numeric_inputs("bullish")
    calculation = calculate_historical_cross_sectional_7asset_returns_v2(
        prior_market_median_returns_3=prior_market,
        current_peer_returns_3=current_peers,
    )
    constructor_values = {
        item.name: getattr(calculation, item.name)
        for item in fields(calculation)
        if item.init
    }

    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="sealed factory",
    ):
        HistoricalCrossSectional7AssetCalculationV2(  # type: ignore[arg-type]
            **constructor_values
        )
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="sealed factory",
    ):
        replace(calculation, direction=-1)

    object.__setattr__(calculation, "calculation_sha256", "0" * 64)
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="hash differs",
    ):
        canonical_historical_cross_sectional_7asset_calculation_v2(calculation)


def test_canonical_payload_is_stable_and_denies_every_live_or_outcome_claim() -> None:
    first = build_historical_cross_sectional_7asset_proxy_v2(_input())
    second = build_historical_cross_sectional_7asset_proxy_v2(_input())
    first_payload = canonical_historical_cross_sectional_7asset_proxy_v2(first)
    second_payload = canonical_historical_cross_sectional_7asset_proxy_v2(second)
    document = json.loads(first_payload)

    assert first == second
    assert first.event_id == second.event_id
    assert first.proxy_sha256 == second.proxy_sha256
    assert first_payload == second_payload
    assert document["historical_only"] is True
    assert document["shadow_only"] is True
    for key in (
        "live_authority",
        "verified_raw_membership_m0_bound",
        "strict_source_parser_m1_bound",
        "causal_cursor_finality_m2_bound",
        "causal_inputs_complete",
        "producer_ready",
        "paper_executable",
        "promoting",
        "deployment_approved",
        "probability",
        "probability_calibrated",
        "target_candles_used",
        "target_return_used",
        "primary_direction_used",
        "outcome_used",
    ):
        assert document[key] is False
    assert document["data_through_ms"] is None
    assert document["peer_symbols"] == list(PEERS)
    assert TARGET not in document["peer_symbols"]
    assert "target_return" not in document
    assert "outcome" not in document
    assert "calculation_sha256" not in document


@pytest.mark.parametrize(
    ("mode", "event_id", "proxy_sha256", "input_sha256"),
    (
        (
            "bullish",
            "9ef8d96e9b2c825c8ba1b7236f811880defbfd161647c1f4656b6a44e8168e20",
            "3ef69e616918d94a07572b669cc3653ef48911f953682485b4f9b582b9d31478",
            "96b0ee40fe79c3cdc79f05a1c67fe2c66c41d328806547bf5dff005d14dcbff7",
        ),
        (
            "bearish",
            "9fd415a4563b62ca0f5b8e972471ff958961cb33664b32feb496f9a1dcafebda",
            "50a36284bb866a433413b58d59c426ec144d16ef2a4203119fba5b92abaa10a3",
            "c0b3dcb42dcfa47776bf0e18b97385e5ee03e6040e395809a198ffdeebd95757",
        ),
        (
            "neutral",
            "0d351fa04ce869c8b91a9aa4acddf8b8f0323a8f674ec83aad5fe3765b6ff430",
            "ba023d2a216be741e15f5d87631f5ff8238da45655fac1da6648a30649369a86",
            "f6361583d116dd5d105c08339ff81ac02b5de6737733721b435f13050fef3270",
        ),
        (
            "zero_scale",
            "52763ac088638a2fa72dbbbcf8764e8bce392865e268cd2db656de5c05faa8e4",
            "aa03083fed20bbee151f5075f38798227d5c280585c52352b8e2de17bdeea695",
            "42381bcb0941547f6a332e83d92b4fe95a9c11106b427a0eeb183242d226a70e",
        ),
    ),
)
def test_numeric_refactor_preserves_frozen_proxy_identity_bytes(
    mode: str,
    event_id: str,
    proxy_sha256: str,
    input_sha256: str,
) -> None:
    proxy = _proxy(mode)

    assert proxy.event_id == event_id
    assert proxy.proxy_sha256 == proxy_sha256
    assert proxy.source_input.input_sha256 == input_sha256
    assert canonical_historical_cross_sectional_7asset_proxy_v2(proxy)


def test_factory_seal_and_canonical_tamper_checks_fail_closed() -> None:
    proxy = _proxy()
    constructor_values = {
        item.name: getattr(proxy, item.name) for item in fields(proxy) if item.init
    }

    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="sealed factory",
    ):
        HistoricalCrossSectional7AssetProxyV2(**constructor_values)  # type: ignore[arg-type]
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="sealed factory",
    ):
        replace(proxy, strength_micros=1)

    tampered = build_historical_cross_sectional_7asset_proxy_v2(_input())
    object.__setattr__(tampered, "direction", -1)
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match=r"signed-magnitude contract|contradict the bound source input",
    ):
        canonical_historical_cross_sectional_7asset_proxy_v2(tampered)


def test_input_and_peer_hash_tampering_is_rejected_before_derivation() -> None:
    source = HistoricalCrossSectional7AssetProxyInputV2(
        target_symbol=TARGET,
        peer_paths=_input().peer_paths,
    )
    object.__setattr__(source, "input_sha256", "0" * 64)
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="source input differs",
    ):
        build_historical_cross_sectional_7asset_proxy_v2(source)

    path = HistoricalPeerCandlePathV2(
        symbol=PEERS[0],
        venue=VenueV2.USDM_FUTURES,
        candles=_path(PEERS[0]).candles,
    )
    object.__setattr__(path, "path_sha256", "0" * 64)
    with pytest.raises(
        HistoricalCrossSectional7AssetProxyContractErrorV2,
        match="peer path differs",
    ):
        HistoricalCrossSectional7AssetProxyInputV2(
            target_symbol=TARGET,
            peer_paths=(path, *_input().peer_paths[1:]),
        )
