from __future__ import annotations

from dataclasses import replace
from decimal import ROUND_DOWN, Decimal, getcontext, localcontext, setcontext

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.d1_scefb import (
    D1_AUTHORITY_BINDING_V0,
    D1_CALCULATION_BAR_COUNT_V0,
    D1_HARD_HORIZON_BARS_V0,
    D1_HOURLY_BAR_COUNT_V0,
    D1_PRIOR_FIVE_MINUTE_BAR_COUNT_V0,
    D1EntryReferenceKindV0,
    D1EntryStatusV0,
    D1ExitActionV0,
    D1ExitReasonV0,
    D1ExitStatusV0,
    D1FiveMinuteBarV0,
    D1ScefbContractErrorV0,
    D1SideV0,
    build_d1_entry_input_v0,
    build_d1_exit_input_v0,
    build_d1_five_minute_bar_v0,
    build_d1_hourly_bar_v0,
    build_d1_paper_position_anchor_v0,
    canonical_d1_entry_decision_v0,
    canonical_d1_entry_input_v0,
    canonical_d1_exit_decision_v0,
    evaluate_d1_entry_v0,
    evaluate_d1_exit_v0,
)

_FIVE_MINUTE_MS = 300_000
_HOUR_MS = 3_600_000
_CURRENT_OPEN_MS = 1_800_000_000_000
_SOURCE_ROOT = "1" * 64
_VWAP_SOURCE = "2" * 64
_EXIT_SOURCE = "3" * 64


def _d(value: str | int) -> Decimal:
    return Decimal(value)


def _prior_bars(
    *,
    center: Decimal = Decimal("100"),
    scale: Decimal = Decimal("1"),
    quote_volume: Decimal = Decimal("200000"),
    imbalance_abs: Decimal = Decimal("0.05"),
    baseline_width: Decimal = Decimal("2"),
    final_compressed_width: Decimal | None = None,
    all_widths_zero: bool = False,
) -> tuple[D1FiveMinuteBarV0, ...]:
    first_open = _CURRENT_OPEN_MS - D1_PRIOR_FIVE_MINUTE_BAR_COUNT_V0 * _FIVE_MINUTE_MS
    bars: list[D1FiveMinuteBarV0] = []
    with localcontext(protocol_decimal_context_v2()):
        bars.append(
            _five_minute_bar(
                open_ms=first_open,
                center=center,
                width=Decimal(0) if all_widths_zero else scale,
                quote_volume=quote_volume,
                imbalance=Decimal(0),
            )
        )
        for index in range(D1_CALCULATION_BAR_COUNT_V0):
            if all_widths_zero:
                width = Decimal(0)
            elif index < 216:
                width = scale
            elif index < 276:
                width = baseline_width * scale
            else:
                width = scale if final_compressed_width is None else final_compressed_width * scale
            imbalance = -imbalance_abs if index % 2 == 0 else imbalance_abs
            bars.append(
                _five_minute_bar(
                    open_ms=first_open + (index + 1) * _FIVE_MINUTE_MS,
                    center=center,
                    width=width,
                    quote_volume=quote_volume,
                    imbalance=imbalance,
                )
            )
    return tuple(bars)


def _prior_bars_with_exact_frozen_atr(
    target_atr: Decimal,
) -> tuple[D1FiveMinuteBarV0, ...]:
    """Build a compression path whose final Wilder ATR equals target_atr exactly."""

    first_open = _CURRENT_OPEN_MS - D1_PRIOR_FIVE_MINUTE_BAR_COUNT_V0 * _FIVE_MINUTE_MS
    center = Decimal(1)
    quote_volume = Decimal("200000")
    with localcontext(protocol_decimal_context_v2()):
        compressed_range = target_atr / Decimal(2)
        atr_before_last = target_atr
        for _ in range(11):
            atr_before_last = (Decimal(13) * atr_before_last + compressed_range) / Decimal(14)
        final_range = Decimal(14) * target_atr - Decimal(13) * atr_before_last
        ranges = (target_atr,) * 276 + (compressed_range,) * 11 + (final_range,)
        bars = [
            _five_minute_bar(
                open_ms=first_open,
                center=center,
                width=target_atr,
                quote_volume=quote_volume,
                imbalance=Decimal(0),
            )
        ]
        for index, true_range in enumerate(ranges):
            imbalance = Decimal("-0.05") if index % 2 == 0 else Decimal("0.05")
            bars.append(
                _five_minute_bar(
                    open_ms=first_open + (index + 1) * _FIVE_MINUTE_MS,
                    center=center,
                    width=Decimal(0),
                    quote_volume=quote_volume,
                    imbalance=imbalance,
                    close_price=center,
                    high_price=center,
                    low_price=center - true_range,
                )
            )
    return tuple(bars)


def _five_minute_bar(
    *,
    open_ms: int,
    center: Decimal,
    width: Decimal,
    quote_volume: Decimal,
    imbalance: Decimal,
    close_price: Decimal | None = None,
    high_price: Decimal | None = None,
    low_price: Decimal | None = None,
    receipt_ms: int | None = None,
    is_closed: bool = True,
) -> D1FiveMinuteBarV0:
    with localcontext(protocol_decimal_context_v2()):
        close = center if close_price is None else close_price
        high = center + width / Decimal(2) if high_price is None else high_price
        low = center - width / Decimal(2) if low_price is None else low_price
        taker_buy = quote_volume * (Decimal(1) + imbalance) / Decimal(2)
    return build_d1_five_minute_bar_v0(
        open_ms=open_ms,
        open_price=center,
        high_price=high,
        low_price=low,
        close_price=close,
        quote_volume=quote_volume,
        taker_buy_quote_volume=taker_buy,
        receipt_ms=receipt_ms,
        is_closed=is_closed,
    )


def _current_bar(
    side: D1SideV0,
    *,
    center: Decimal = Decimal("100"),
    scale: Decimal = Decimal("1"),
    quote_volume: Decimal = Decimal("500000"),
    imbalance: Decimal | None = None,
    close_price: Decimal | None = None,
    high_price: Decimal | None = None,
    low_price: Decimal | None = None,
    receipt_ms: int | None = None,
    is_closed: bool = True,
) -> D1FiveMinuteBarV0:
    direction = Decimal(1) if side is D1SideV0.LONG else Decimal(-1)
    with localcontext(protocol_decimal_context_v2()):
        close = center + direction * Decimal("1.5") * scale if close_price is None else close_price
        high = center + Decimal("1.7") * scale if high_price is None else high_price
        low = center - Decimal("1.7") * scale if low_price is None else low_price
    flow = direction * Decimal("0.40") if imbalance is None else imbalance
    return _five_minute_bar(
        open_ms=_CURRENT_OPEN_MS,
        center=center,
        width=Decimal(0),
        quote_volume=quote_volume,
        imbalance=flow,
        close_price=close,
        high_price=high,
        low_price=low,
        receipt_ms=receipt_ms,
        is_closed=is_closed,
    )


def _hourly_bars(
    side: D1SideV0,
    *,
    flat: bool = False,
) -> tuple:
    first_open = _CURRENT_OPEN_MS - D1_HOURLY_BAR_COUNT_V0 * _HOUR_MS
    bars = []
    for index in range(D1_HOURLY_BAR_COUNT_V0):
        if flat:
            close = Decimal("150")
        elif side is D1SideV0.LONG:
            close = Decimal("100") + Decimal(index) / Decimal(10)
        else:
            close = Decimal("200") - Decimal(index) / Decimal(10)
        bars.append(
            build_d1_hourly_bar_v0(
                open_ms=first_open + index * _HOUR_MS,
                close_price=close,
            )
        )
    return tuple(bars)


def _entry_input(
    side: D1SideV0,
    *,
    prior_bars: tuple[D1FiveMinuteBarV0, ...] | None = None,
    current_bar: D1FiveMinuteBarV0 | None = None,
    hourly_bars: tuple | None = None,
    required_fields_complete: bool = True,
) -> object:
    return build_d1_entry_input_v0(
        attempt_id="d1-attempt",
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        source_root_sha256=_SOURCE_ROOT,
        prior_bars=_prior_bars() if prior_bars is None else prior_bars,
        current_bar=_current_bar(side) if current_bar is None else current_bar,
        hourly_bars=_hourly_bars(side) if hourly_bars is None else hourly_bars,
        required_fields_complete=required_fields_complete,
    )


@pytest.mark.parametrize("side", [D1SideV0.LONG, D1SideV0.SHORT])
def test_exact_289_prior_arithmetic_and_positive_mirrored_signals(side: D1SideV0) -> None:
    item = _entry_input(side)
    decision = evaluate_d1_entry_v0(item)  # type: ignore[arg-type]

    assert len(item.prior_bars) == 289  # type: ignore[union-attr]
    assert len(item.prior_bars[1:]) == 288  # type: ignore[union-attr]
    assert decision.status is D1EntryStatusV0.SIGNAL
    assert decision.side is side
    assert decision.reasons[0] == "D1_FULL_SETUP"
    assert decision.authority_binding == D1_AUTHORITY_BINDING_V0
    assert not decision.causal_authority_bound
    assert not decision.paper_input_authorized
    assert not decision.probability_claim
    assert not decision.efficacy_claim
    assert not decision.production_order_placement
    assert not decision.paper_entry_evaluated
    assert canonical_d1_entry_decision_v0(decision).endswith(b"\n")


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    [
        ("hourly_count", "HOURLY_COUNT_NOT_250"),
        ("hourly_unclosed", "HOURLY_BAR_NOT_FULLY_CLOSED"),
        (
            "hourly_sequence",
            "HOURLY_SEQUENCE_OR_LATEST_CUTOFF_MISMATCH",
        ),
        ("future_source", "SOURCE_DATA_AFTER_SIGNAL_CLOSE"),
    ],
)
def test_hourly_readiness_failures_are_explicit_and_fail_closed(
    case: str,
    expected_reason: str,
) -> None:
    hourly = list(_hourly_bars(D1SideV0.LONG))
    if case == "hourly_count":
        hourly.pop()
    elif case == "hourly_unclosed":
        last = hourly[-1]
        hourly[-1] = build_d1_hourly_bar_v0(
            open_ms=last.open_ms,
            close_price=last.close_price,
            is_closed=False,
        )
    elif case == "hourly_sequence":
        last = hourly[-1]
        hourly[-1] = build_d1_hourly_bar_v0(
            open_ms=last.open_ms - _HOUR_MS,
            close_price=last.close_price,
        )
    elif case == "future_source":
        last = hourly[-1]
        hourly[-1] = build_d1_hourly_bar_v0(
            open_ms=last.open_ms,
            close_price=last.close_price,
            data_through_ms=_CURRENT_OPEN_MS + _FIVE_MINUTE_MS,
        )
    else:  # pragma: no cover - parametrization is a closed set
        raise AssertionError(f"unsupported case: {case}")

    decision = evaluate_d1_entry_v0(
        _entry_input(
            D1SideV0.LONG,
            hourly_bars=tuple(hourly),
        )  # type: ignore[arg-type]
    )

    assert decision.status is D1EntryStatusV0.INCONCLUSIVE
    assert decision.side is None
    assert expected_reason in decision.reasons
    assert decision.invalidation == "NO_POSITION_CREATED_INCONCLUSIVE_INPUT"
    assert not decision.probability_claim
    assert not decision.efficacy_claim
    assert not decision.production_order_placement


@pytest.mark.parametrize(
    ("case", "expected_reason"),
    [
        ("inside_channel", "NO_PRIOR_CHANNEL_BREAKOUT"),
        ("compression", "COMPRESSION_GT_0_70"),
        ("expansion_low", "CURRENT_EXPANSION_LT_1_50"),
        ("atr_floor", "ATR_FRACTION_LT_0_0035"),
        ("liquidity", "PRIOR_MEDIAN_QUOTE_VOLUME_LT_100000"),
        ("imbalance", "LONG_TAKER_IMBALANCE_LT_0_20"),
        ("robust_z", "LONG_TAKER_ROBUST_Z_LT_2_00"),
        ("activity", "ACTIVITY_LT_2_00"),
        ("hourly_trend", "LONG_STRICT_PRIOR_HOURLY_TREND_FAILED"),
    ],
)
def test_each_major_entry_gate_withholds_signal(case: str, expected_reason: str) -> None:
    prior = _prior_bars()
    current = _current_bar(D1SideV0.LONG)
    hourly = _hourly_bars(D1SideV0.LONG)
    if case == "inside_channel":
        current = _current_bar(
            D1SideV0.LONG,
            close_price=Decimal("100.5"),
        )
    elif case == "compression":
        prior = _prior_bars(final_compressed_width=Decimal("2"))
    elif case == "expansion_low":
        current = _current_bar(
            D1SideV0.LONG,
            close_price=Decimal("101.5"),
            high_price=Decimal("101.6"),
            low_price=Decimal("100.0"),
        )
    elif case == "atr_floor":
        prior = _prior_bars(scale=Decimal("0.1"))
        current = _current_bar(D1SideV0.LONG, scale=Decimal("0.1"))
    elif case == "liquidity":
        prior = _prior_bars(quote_volume=Decimal("99999"))
    elif case == "imbalance":
        current = _current_bar(D1SideV0.LONG, imbalance=Decimal("0.19"))
    elif case == "robust_z":
        prior = _prior_bars(imbalance_abs=Decimal("0.20"))
        current = _current_bar(D1SideV0.LONG, imbalance=Decimal("0.30"))
    elif case == "activity":
        current = _current_bar(D1SideV0.LONG, quote_volume=Decimal("399999"))
    elif case == "hourly_trend":
        hourly = _hourly_bars(D1SideV0.LONG, flat=True)

    decision = evaluate_d1_entry_v0(
        _entry_input(
            D1SideV0.LONG,
            prior_bars=prior,
            current_bar=current,
            hourly_bars=hourly,
        )  # type: ignore[arg-type]
    )

    assert decision.status is D1EntryStatusV0.NO_SIGNAL
    assert decision.side is None
    assert expected_reason in decision.reasons


def test_short_is_an_exact_sign_mirror_for_directional_flow() -> None:
    decision = evaluate_d1_entry_v0(
        _entry_input(
            D1SideV0.SHORT,
            current_bar=_current_bar(
                D1SideV0.SHORT,
                imbalance=Decimal("0.40"),
            ),
        )  # type: ignore[arg-type]
    )

    assert decision.status is D1EntryStatusV0.NO_SIGNAL
    assert "SHORT_TAKER_IMBALANCE_GT_NEG_0_20" in decision.reasons
    assert "SHORT_TAKER_ROBUST_Z_GT_NEG_2_00" in decision.reasons


def test_breakout_penetration_and_close_location_exact_boundaries_are_inclusive() -> None:
    exact_prior = _prior_bars_with_exact_frozen_atr(Decimal("0.1"))
    base_item = _entry_input(
        D1SideV0.LONG,
        prior_bars=exact_prior,
        current_bar=_current_bar(D1SideV0.LONG, center=Decimal(1), scale=Decimal("0.1")),
    )
    base = evaluate_d1_entry_v0(base_item)  # type: ignore[arg-type]
    assert base.frozen_atr is not None
    assert base.frozen_channel_upper is not None
    atr = base.frozen_atr
    upper = base.frozen_channel_upper
    prior = base_item.prior_bars  # type: ignore[union-attr]
    hourly = base_item.hourly_bars  # type: ignore[union-attr]

    def decision_for(penetration: Decimal, high_extra: Decimal, low_distance: Decimal):
        with localcontext(protocol_decimal_context_v2()):
            close = upper + penetration * atr
            current = _current_bar(
                D1SideV0.LONG,
                center=Decimal(1),
                close_price=close,
                high_price=close + high_extra * atr,
                low_price=close - low_distance * atr,
            )
        return evaluate_d1_entry_v0(
            _entry_input(
                D1SideV0.LONG,
                prior_bars=prior,
                current_bar=current,
                hourly_bars=hourly,
            )  # type: ignore[arg-type]
        )

    assert decision_for(Decimal("0.10"), Decimal("0.10"), Decimal("2.0")).status is (
        D1EntryStatusV0.SIGNAL
    )
    assert decision_for(Decimal("0.50"), Decimal("0.10"), Decimal("2.0")).status is (
        D1EntryStatusV0.SIGNAL
    )
    below = decision_for(Decimal("0.099999"), Decimal("0.10"), Decimal("2.0"))
    above = decision_for(Decimal("0.500001"), Decimal("0.10"), Decimal("2.0"))
    assert "LONG_BREAKOUT_ATR_LT_0_10" in below.reasons
    assert "LONG_BREAKOUT_ATR_GT_0_50" in above.reasons

    exact_location = decision_for(Decimal("0.20"), Decimal("0.50"), Decimal("1.50"))
    below_location = decision_for(
        Decimal("0.20"),
        Decimal("0.500001"),
        Decimal("1.50"),
    )
    assert exact_location.status is D1EntryStatusV0.SIGNAL
    assert "LONG_CLOSE_LOCATION_LT_0_75" in below_location.reasons


def test_shared_and_flow_exact_threshold_boundaries_are_inclusive() -> None:
    exact_compression_prior = _prior_bars(final_compressed_width=Decimal("1.4"))
    assert (
        evaluate_d1_entry_v0(
            _entry_input(D1SideV0.LONG, prior_bars=exact_compression_prior)  # type: ignore[arg-type]
        ).status
        is D1EntryStatusV0.SIGNAL
    )

    exact_liquidity_prior = _prior_bars(quote_volume=Decimal("100000"))
    assert (
        evaluate_d1_entry_v0(
            _entry_input(
                D1SideV0.LONG,
                prior_bars=exact_liquidity_prior,
                current_bar=_current_bar(
                    D1SideV0.LONG,
                    quote_volume=Decimal("200000"),
                    imbalance=Decimal("0.20"),
                ),
            )  # type: ignore[arg-type]
        ).status
        is D1EntryStatusV0.SIGNAL
    )

    z_prior = _prior_bars(imbalance_abs=Decimal("0.10"))
    exact_z = evaluate_d1_entry_v0(
        _entry_input(
            D1SideV0.LONG,
            prior_bars=z_prior,
            current_bar=_current_bar(
                D1SideV0.LONG,
                imbalance=Decimal("0.29652"),
            ),
        )  # type: ignore[arg-type]
    )
    assert exact_z.status is D1EntryStatusV0.SIGNAL
    assert exact_z.taker_robust_z == Decimal("2")

    exact_atr_floor_prior = _prior_bars_with_exact_frozen_atr(Decimal("0.0035"))
    probe = evaluate_d1_entry_v0(
        _entry_input(
            D1SideV0.LONG,
            prior_bars=exact_atr_floor_prior,
            current_bar=_current_bar(
                D1SideV0.LONG,
                center=Decimal(1),
                scale=Decimal("0.0035"),
            ),
        )  # type: ignore[arg-type]
    )
    assert probe.frozen_atr is not None
    with localcontext(protocol_decimal_context_v2()):
        floor_atr = probe.frozen_atr
        exact_previous_close = floor_atr / Decimal("0.0035")
        floor_close = Decimal(1) + Decimal("0.30") * floor_atr
        floor_current = _current_bar(
            D1SideV0.LONG,
            center=Decimal(1),
            close_price=floor_close,
            high_price=floor_close + Decimal("0.20") * floor_atr,
            low_price=floor_close - Decimal(2) * floor_atr,
        )
    last = exact_atr_floor_prior[-1]
    last_with_exact_fraction_denominator = build_d1_five_minute_bar_v0(
        open_ms=last.open_ms,
        open_price=last.open_price,
        high_price=last.high_price,
        low_price=last.low_price,
        close_price=exact_previous_close,
        quote_volume=last.quote_volume,
        taker_buy_quote_volume=last.taker_buy_quote_volume,
        data_through_ms=last.data_through_ms,
        receipt_ms=last.receipt_ms,
        is_closed=last.is_closed,
    )
    exact_atr_floor_prior = (
        *exact_atr_floor_prior[:-1],
        last_with_exact_fraction_denominator,
    )
    exact_floor = evaluate_d1_entry_v0(
        _entry_input(
            D1SideV0.LONG,
            prior_bars=exact_atr_floor_prior,
            current_bar=floor_current,
        )  # type: ignore[arg-type]
    )
    assert exact_floor.status is D1EntryStatusV0.SIGNAL
    assert exact_floor.atr_fraction == Decimal("0.0035")


def test_current_expansion_exact_1_50_and_3_00_boundaries_are_inclusive() -> None:
    prior = _prior_bars_with_exact_frozen_atr(Decimal("0.1"))
    atr = Decimal("0.1")
    upper = Decimal(1)

    def expansion_decision(total_range_atr: Decimal):
        with localcontext(protocol_decimal_context_v2()):
            close = upper + Decimal("0.20") * atr
            high = close + Decimal("0.20") * atr
            low = high - total_range_atr * atr
        return evaluate_d1_entry_v0(
            _entry_input(
                D1SideV0.LONG,
                prior_bars=prior,
                current_bar=_current_bar(
                    D1SideV0.LONG,
                    center=Decimal(1),
                    close_price=close,
                    high_price=high,
                    low_price=low,
                ),
            )  # type: ignore[arg-type]
        )

    lower = expansion_decision(Decimal("1.50"))
    upper_boundary = expansion_decision(Decimal("3.00"))
    too_small = expansion_decision(Decimal("1.4999"))
    too_large = expansion_decision(Decimal("3.0001"))
    assert lower.status is D1EntryStatusV0.SIGNAL
    assert lower.current_expansion == Decimal("1.50")
    assert upper_boundary.status is D1EntryStatusV0.SIGNAL
    assert upper_boundary.current_expansion == Decimal("3.00")
    assert "CURRENT_EXPANSION_LT_1_50" in too_small.reasons
    assert "CURRENT_EXPANSION_GT_3_00" in too_large.reasons


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("wrong_count", "PRIOR_5M_COUNT_NOT_289"),
        ("gap", "FIVE_MINUTE_SEQUENCE_NOT_CONTIGUOUS"),
        ("unclosed", "FIVE_MINUTE_BAR_NOT_FULLY_CLOSED"),
        ("late", "SOURCE_RECEIPT_AFTER_DECISION_CUTOFF"),
        ("zero_mad", "ZERO_TAKER_IMBALANCE_MAD"),
        ("zero_prior_quote", "ZERO_PRIOR_QUOTE_VOLUME_DENOMINATOR"),
        ("zero_current_quote", "ZERO_CURRENT_QUOTE_VOLUME"),
        ("zero_current_range", "ZERO_CURRENT_HIGH_LOW_RANGE"),
        ("zero_atr", "ZERO_ATR14"),
        ("zero_compression_baseline", "ZERO_COMPRESSION_BASELINE_MEDIAN"),
    ],
)
def test_invalid_temporal_inputs_and_zero_denominators_are_inconclusive(
    case: str,
    reason: str,
) -> None:
    prior = _prior_bars()
    current = _current_bar(D1SideV0.LONG)
    if case == "wrong_count":
        prior = prior[1:]
    elif case == "gap":
        broken = _five_minute_bar(
            open_ms=prior[100].open_ms + _FIVE_MINUTE_MS,
            center=Decimal("100"),
            width=Decimal("1"),
            quote_volume=Decimal("200000"),
            imbalance=Decimal("0.05"),
        )
        prior = (*prior[:100], broken, *prior[101:])
    elif case == "unclosed":
        current = _current_bar(D1SideV0.LONG, is_closed=False)
    elif case == "late":
        cutoff = _CURRENT_OPEN_MS + _FIVE_MINUTE_MS - 1 + 2_001
        current = _current_bar(D1SideV0.LONG, receipt_ms=cutoff + 1)
    elif case == "zero_mad":
        prior = _prior_bars(imbalance_abs=Decimal(0))
    elif case == "zero_prior_quote":
        zero = _five_minute_bar(
            open_ms=prior[1].open_ms,
            center=Decimal("100"),
            width=Decimal("1"),
            quote_volume=Decimal(0),
            imbalance=Decimal(0),
        )
        prior = (prior[0], zero, *prior[2:])
    elif case == "zero_current_quote":
        current = _current_bar(
            D1SideV0.LONG,
            quote_volume=Decimal(0),
            imbalance=Decimal(0),
        )
    elif case == "zero_current_range":
        current = _current_bar(
            D1SideV0.LONG,
            center=Decimal("101.5"),
            close_price=Decimal("101.5"),
            high_price=Decimal("101.5"),
            low_price=Decimal("101.5"),
        )
    elif case == "zero_atr":
        prior = _prior_bars(all_widths_zero=True)
    elif case == "zero_compression_baseline":
        prior = _prior_bars(baseline_width=Decimal(0))

    decision = evaluate_d1_entry_v0(
        _entry_input(
            D1SideV0.LONG,
            prior_bars=prior,
            current_bar=current,
        )  # type: ignore[arg-type]
    )

    assert decision.status is D1EntryStatusV0.INCONCLUSIVE
    assert reason in decision.reasons
    assert decision.side is None


def test_factory_seals_and_protocol_decimal_context_make_output_deterministic() -> None:
    item = _entry_input(D1SideV0.LONG)
    canonical_input = canonical_d1_entry_input_v0(item)  # type: ignore[arg-type]
    first = evaluate_d1_entry_v0(item)  # type: ignore[arg-type]
    ambient = getcontext().copy()
    try:
        getcontext().prec = 6
        getcontext().rounding = ROUND_DOWN
        second = evaluate_d1_entry_v0(item)  # type: ignore[arg-type]
    finally:
        setcontext(ambient)

    assert first == second
    assert canonical_input == canonical_d1_entry_input_v0(item)  # type: ignore[arg-type]
    assert canonical_d1_entry_decision_v0(first) == canonical_d1_entry_decision_v0(second)
    with pytest.raises(D1ScefbContractErrorV0, match="factory-created"):
        replace(item, attempt_id="forged")  # type: ignore[call-overload]
    with pytest.raises(D1ScefbContractErrorV0, match="evaluator-created"):
        replace(first, reasons=("FORGED",))


def _signal_and_position(side: D1SideV0):
    signal = evaluate_d1_entry_v0(_entry_input(side))  # type: ignore[arg-type]
    assert signal.status is D1EntryStatusV0.SIGNAL
    assert signal.signal_close is not None
    position = build_d1_paper_position_anchor_v0(
        entry_decision=signal,
        entry_vwap=signal.signal_close,
        entry_fill_ms=signal.decision_cutoff_ms + 10_000,
        entry_reference_kind=D1EntryReferenceKindV0.UNBOUND_PAPER_VWAP_REFERENCE,
        entry_vwap_source_sha256=_VWAP_SOURCE,
    )
    return signal, position


def test_position_anchor_enforces_the_frozen_half_atr_vwap_distance() -> None:
    signal = evaluate_d1_entry_v0(_entry_input(D1SideV0.LONG))  # type: ignore[arg-type]
    assert signal.status is D1EntryStatusV0.SIGNAL
    assert signal.signal_close is not None
    assert signal.frozen_atr is not None
    with localcontext(protocol_decimal_context_v2()):
        boundary = signal.signal_close + Decimal("0.50") * signal.frozen_atr
        outside = signal.signal_close + Decimal("0.5001") * signal.frozen_atr

    position = build_d1_paper_position_anchor_v0(
        entry_decision=signal,
        entry_vwap=boundary,
        entry_fill_ms=signal.decision_cutoff_ms + 10_000,
        entry_reference_kind=D1EntryReferenceKindV0.UNBOUND_PAPER_VWAP_REFERENCE,
        entry_vwap_source_sha256=_VWAP_SOURCE,
    )
    assert not position.paper_fill_claim
    with pytest.raises(D1ScefbContractErrorV0, match=r"0\.50 ATR"):
        build_d1_paper_position_anchor_v0(
            entry_decision=signal,
            entry_vwap=outside,
            entry_fill_ms=signal.decision_cutoff_ms + 10_000,
            entry_reference_kind=D1EntryReferenceKindV0.UNBOUND_PAPER_VWAP_REFERENCE,
            entry_vwap_source_sha256=_VWAP_SOURCE,
        )


@pytest.mark.parametrize("offset_ms", [0, 12_345])
def test_position_anchor_starts_exit_path_in_the_fill_containing_bar(
    offset_ms: int,
) -> None:
    signal = evaluate_d1_entry_v0(_entry_input(D1SideV0.LONG))  # type: ignore[arg-type]
    assert signal.signal_close is not None
    t_plus_2_open = signal.bar_open_ms + 2 * _FIVE_MINUTE_MS
    position = build_d1_paper_position_anchor_v0(
        entry_decision=signal,
        entry_vwap=signal.signal_close,
        entry_fill_ms=t_plus_2_open + offset_ms,
        entry_reference_kind=D1EntryReferenceKindV0.HISTORICAL_OPEN_PROXY,
        entry_vwap_source_sha256=_VWAP_SOURCE,
    )

    assert position.entry_bar_open_ms == signal.bar_open_ms
    assert position.first_exit_bar_open_ms == t_plus_2_open
    decision = _exit(position, 1, position.entry_vwap)
    assert decision.status is D1ExitStatusV0.KEEP
    assert decision.bar_open_ms == t_plus_2_open


@pytest.mark.parametrize("relative_ms", [-1, 0])
def test_position_anchor_rejects_fill_at_or_before_signal_cutoff(relative_ms: int) -> None:
    signal = evaluate_d1_entry_v0(_entry_input(D1SideV0.LONG))  # type: ignore[arg-type]
    assert signal.signal_close is not None
    with pytest.raises(D1ScefbContractErrorV0, match="strictly after"):
        build_d1_paper_position_anchor_v0(
            entry_decision=signal,
            entry_vwap=signal.signal_close,
            entry_fill_ms=signal.decision_cutoff_ms + relative_ms,
            entry_reference_kind=D1EntryReferenceKindV0.UNBOUND_PAPER_VWAP_REFERENCE,
            entry_vwap_source_sha256=_VWAP_SOURCE,
        )


def test_exit_rejects_a_wrong_first_bar_relative_to_actual_fill() -> None:
    signal = evaluate_d1_entry_v0(_entry_input(D1SideV0.LONG))  # type: ignore[arg-type]
    assert signal.signal_close is not None
    t_plus_2_open = signal.bar_open_ms + 2 * _FIVE_MINUTE_MS
    position = build_d1_paper_position_anchor_v0(
        entry_decision=signal,
        entry_vwap=signal.signal_close,
        entry_fill_ms=t_plus_2_open,
        entry_reference_kind=D1EntryReferenceKindV0.HISTORICAL_OPEN_PROXY,
        entry_vwap_source_sha256=_VWAP_SOURCE,
    )
    wrong_first = _five_minute_bar(
        open_ms=t_plus_2_open - _FIVE_MINUTE_MS,
        center=position.entry_vwap,
        width=Decimal("0.2"),
        quote_volume=Decimal("200000"),
        imbalance=Decimal(0),
    )

    decision = evaluate_d1_exit_v0(
        build_d1_exit_input_v0(
            position=position,
            source_root_sha256=_EXIT_SOURCE,
            bars_since_entry=(wrong_first,),
        )
    )

    assert decision.status is D1ExitStatusV0.INCONCLUSIVE_EXIT
    assert "EXIT_FIVE_MINUTE_SEQUENCE_NOT_CONTIGUOUS" in decision.reasons
    assert "EXIT_BAR_CLOSES_BEFORE_ENTRY_FILL" in decision.reasons


def _exit_bars(position, count: int, final_close: Decimal) -> tuple[D1FiveMinuteBarV0, ...]:
    bars = []
    for index in range(1, count + 1):
        close = final_close if index == count else position.entry_vwap
        bars.append(
            _five_minute_bar(
                open_ms=position.first_exit_bar_open_ms + (index - 1) * _FIVE_MINUTE_MS,
                center=close,
                width=Decimal("0.2"),
                quote_volume=Decimal("200000"),
                imbalance=Decimal(0),
            )
        )
    return tuple(bars)


def _exit(position, count: int, final_close: Decimal, **kwargs):
    return evaluate_d1_exit_v0(
        build_d1_exit_input_v0(
            position=position,
            source_root_sha256=_EXIT_SOURCE,
            bars_since_entry=_exit_bars(position, count, final_close),
            **kwargs,
        )
    )


@pytest.mark.parametrize("side", [D1SideV0.LONG, D1SideV0.SHORT])
def test_exit_boundaries_are_inclusive_and_mirrored(side: D1SideV0) -> None:
    _, position = _signal_and_position(side)
    with localcontext(protocol_decimal_context_v2()):
        adverse = (
            position.entry_vwap - Decimal("0.80") * position.frozen_atr
            if side is D1SideV0.LONG
            else position.entry_vwap + Decimal("0.80") * position.frozen_atr
        )
        profit = (
            position.entry_vwap + Decimal("3.00") * position.frozen_atr
            if side is D1SideV0.LONG
            else position.entry_vwap - Decimal("3.00") * position.frozen_atr
        )
    adverse_decision = _exit(position, 1, adverse)
    structure_decision = _exit(
        position,
        1,
        (position.frozen_channel_upper if side is D1SideV0.LONG else position.frozen_channel_lower),
    )
    profit_decision = _exit(position, 1, profit)

    assert adverse_decision.exit_reason is D1ExitReasonV0.ADVERSE_CLOSE
    assert structure_decision.exit_reason is D1ExitReasonV0.STRUCTURE_FAILURE
    assert profit_decision.exit_reason is D1ExitReasonV0.PROFIT_CLOSE
    expected_action = (
        D1ExitActionV0.EXIT_LONG if side is D1SideV0.LONG else D1ExitActionV0.EXIT_SHORT
    )
    assert adverse_decision.action is expected_action
    assert profit_decision.action is expected_action
    assert not profit_decision.paper_exit_fill_claim
    assert not profit_decision.production_order_placement


def test_exit_priority_is_authority_then_adverse_then_structure_then_profit() -> None:
    _, position = _signal_and_position(D1SideV0.LONG)
    with localcontext(protocol_decimal_context_v2()):
        profit = position.entry_vwap + Decimal(3) * position.frozen_atr
        adverse = position.entry_vwap - Decimal("0.80") * position.frozen_atr

    authority = _exit(
        position,
        1,
        profit,
        authority_continuity_declared=False,
    )
    adverse_priority = _exit(position, 1, adverse)
    profit_over_horizon = _exit(position, D1_HARD_HORIZON_BARS_V0, profit)

    assert authority.status is D1ExitStatusV0.INCONCLUSIVE_EXIT
    assert authority.exit_reason is D1ExitReasonV0.AUTHORITY_LOSS
    assert not authority.interval_conclusive
    assert adverse_priority.exit_reason is D1ExitReasonV0.ADVERSE_CLOSE
    assert profit_over_horizon.exit_reason is D1ExitReasonV0.PROFIT_CLOSE


def test_exit_horizon_is_exactly_24_subsequent_closed_bars() -> None:
    _, position = _signal_and_position(D1SideV0.LONG)
    keep = _exit(position, D1_HARD_HORIZON_BARS_V0 - 1, position.entry_vwap)
    horizon = _exit(position, D1_HARD_HORIZON_BARS_V0, position.entry_vwap)

    assert keep.status is D1ExitStatusV0.KEEP
    assert keep.action is D1ExitActionV0.KEEP
    assert not keep.alert_intent
    assert horizon.status is D1ExitStatusV0.EXIT
    assert horizon.exit_reason is D1ExitReasonV0.HARD_HORIZON
    assert horizon.paper_exit_intent
    assert canonical_d1_exit_decision_v0(horizon).endswith(b"\n")


def test_exit_gap_unclosed_and_late_paths_force_inconclusive_authority_exit() -> None:
    _, position = _signal_and_position(D1SideV0.LONG)
    valid = list(_exit_bars(position, 2, position.entry_vwap))
    gap = _five_minute_bar(
        open_ms=valid[1].open_ms + _FIVE_MINUTE_MS,
        center=position.entry_vwap,
        width=Decimal("0.2"),
        quote_volume=Decimal("200000"),
        imbalance=Decimal(0),
    )
    unclosed = _five_minute_bar(
        open_ms=valid[1].open_ms,
        center=position.entry_vwap,
        width=Decimal("0.2"),
        quote_volume=Decimal("200000"),
        imbalance=Decimal(0),
        is_closed=False,
    )
    cutoff = valid[1].close_ms + 2_001
    late = _five_minute_bar(
        open_ms=valid[1].open_ms,
        center=position.entry_vwap,
        width=Decimal("0.2"),
        quote_volume=Decimal("200000"),
        imbalance=Decimal(0),
        receipt_ms=cutoff + 1,
    )

    for path, reason in (
        ((valid[0], gap), "EXIT_FIVE_MINUTE_SEQUENCE_NOT_CONTIGUOUS"),
        ((valid[0], unclosed), "EXIT_BAR_NOT_FULLY_CLOSED"),
        ((valid[0], late), "EXIT_SOURCE_RECEIPT_AFTER_DECISION_CUTOFF"),
    ):
        decision = evaluate_d1_exit_v0(
            build_d1_exit_input_v0(
                position=position,
                source_root_sha256=_EXIT_SOURCE,
                bars_since_entry=path,
            )
        )
        assert decision.status is D1ExitStatusV0.INCONCLUSIVE_EXIT
        assert reason in decision.reasons


def test_raw_dataclass_construction_cannot_forge_a_sealed_bar() -> None:
    with pytest.raises(D1ScefbContractErrorV0, match="factory-created"):
        D1FiveMinuteBarV0(
            open_ms=_CURRENT_OPEN_MS,
            close_ms=_CURRENT_OPEN_MS + _FIVE_MINUTE_MS - 1,
            open_price=_d(100),
            high_price=_d(101),
            low_price=_d(99),
            close_price=_d(100),
            quote_volume=_d(1),
            taker_buy_quote_volume=_d(1),
            data_through_ms=_CURRENT_OPEN_MS,
            receipt_ms=_CURRENT_OPEN_MS,
            is_closed=True,
        )
