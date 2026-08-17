from __future__ import annotations

import hashlib
from dataclasses import fields, replace
from decimal import ROUND_FLOOR, Decimal, localcontext
from functools import cache

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.protocol.features import RobustZStatusV2
from signalbot.r4b_v2.strategy.evidence_producer import EVIDENCE_STRENGTH_SCALE_V2
from signalbot.r4b_v2.strategy.family_b_features import (
    FamilyBFeatureContractErrorV2,
    FamilyBKlineBarV2,
)
from signalbot.r4b_v2.strategy.price_evidence import (
    PRICE_STRUCTURE_MOMENTUM_ROLE_V2,
    PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2,
    PriceClosePathCalculationV2,
    PriceEvidenceContractErrorV2,
    PriceStructureMomentumEvidenceV2,
    build_price_structure_momentum_evidence_v2,
    calculate_price_close_path_v2,
    calculate_price_return_series_v2,
    canonical_price_close_path_calculation_v2,
    canonical_price_structure_momentum_evidence_v2,
)

BAR_OPEN_MS = 2_000_160_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1
D_MS = BAR_CLOSE_MS + DECISION_DELAY_MS_V2
PLAN_SHA256 = "a" * 64
CAPTURE_ROOT_SHA256 = "b" * 64
SCHEMA_SHA256 = "c" * 64


def _row(
    index: int,
    *,
    close: Decimal,
    previous_close: Decimal,
    high: Decimal | None = None,
    low: Decimal | None = None,
    closed: bool = True,
    event_ms: int | None = None,
    receipt_ms: int | None = None,
    symbol: str = "BTCUSDT",
) -> FamilyBKlineBarV2:
    first_open_ms = BAR_OPEN_MS - ((PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2 - 1) * FIVE_MINUTE_MS_V2)
    bar_open_ms = first_open_ms + index * FIVE_MINUTE_MS_V2
    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    return FamilyBKlineBarV2.create(
        symbol=symbol,
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA256,
        capture_root_sha256=CAPTURE_ROOT_SHA256,
        schema_sha256=SCHEMA_SHA256,
        interval_ms=FIVE_MINUTE_MS_V2,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        open_event_id=f"price-kline-open-{index}",
        close_event_id=f"price-kline-close-{index}",
        closed=closed,
        event_ms=bar_close_ms if event_ms is None else event_ms,
        receipt_ms=(
            D_MS
            if receipt_ms is None and index == PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2 - 1
            else bar_close_ms
            if receipt_ms is None
            else receipt_ms
        ),
        high=close + Decimal(1) if high is None else high,
        low=close - Decimal(1) if low is None else low,
        close=close,
        previous_close=previous_close,
        source_evidence_sha256=hashlib.sha256(f"price-kline-{index}".encode()).hexdigest(),
    )


def _copy_row(
    value: FamilyBKlineBarV2,
    *,
    venue: VenueV2 | None = None,
    promoting_plan_sha256: str | None = None,
    capture_root_sha256: str | None = None,
    schema_sha256: str | None = None,
    close: Decimal | None = None,
    previous_close: Decimal | None = None,
    high: Decimal | None = None,
    low: Decimal | None = None,
    closed: bool | None = None,
    event_ms: int | None = None,
    receipt_ms: int | None = None,
    symbol: str | None = None,
) -> FamilyBKlineBarV2:
    return FamilyBKlineBarV2.create(
        symbol=value.symbol if symbol is None else symbol,
        venue=value.venue if venue is None else venue,
        promoting_plan_sha256=(
            value.promoting_plan_sha256 if promoting_plan_sha256 is None else promoting_plan_sha256
        ),
        capture_root_sha256=(
            value.capture_root_sha256 if capture_root_sha256 is None else capture_root_sha256
        ),
        schema_sha256=(value.schema_sha256 if schema_sha256 is None else schema_sha256),
        interval_ms=value.interval_ms,
        bar_open_ms=value.bar_open_ms,
        bar_close_ms=value.bar_close_ms,
        open_event_id=value.open_event_id,
        close_event_id=value.close_event_id,
        closed=value.closed if closed is None else closed,
        event_ms=value.event_ms if event_ms is None else event_ms,
        receipt_ms=value.receipt_ms if receipt_ms is None else receipt_ms,
        high=value.high if high is None else high,
        low=value.low if low is None else low,
        close=value.close if close is None else close,
        previous_close=(value.previous_close if previous_close is None else previous_close),
        source_evidence_sha256=value.source_evidence_sha256,
    )


@cache
def _base_rows() -> tuple[FamilyBKlineBarV2, ...]:
    rows: list[FamilyBKlineBarV2] = []
    previous_close = Decimal(100)
    for index in range(PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2):
        close = (
            Decimal(100)
            + Decimal(index) * Decimal("0.001")
            + Decimal(index % 7) * Decimal("0.0001")
        )
        rows.append(
            _row(
                index,
                close=close,
                previous_close=previous_close,
            )
        )
        previous_close = close
    return tuple(rows)


@cache
def _constant_rows() -> tuple[FamilyBKlineBarV2, ...]:
    return tuple(
        _row(index, close=Decimal(100), previous_close=Decimal(100))
        for index in range(PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2)
    )


@cache
def _positive_evidence() -> PriceStructureMomentumEvidenceV2:
    return build_price_structure_momentum_evidence_v2(_base_rows())


def _with_current_close(close: Decimal) -> tuple[FamilyBKlineBarV2, ...]:
    rows = _base_rows()
    current = rows[-1]
    return (
        *rows[:-1],
        _copy_row(
            current,
            close=close,
            high=close + Decimal(1),
            low=close - Decimal(1),
        ),
    )


def _neutral_current_rows() -> tuple[FamilyBKlineBarV2, ...]:
    rows = _base_rows()
    anchor_index = len(rows) - 13
    anchor = rows[anchor_index].close
    offsets = (
        Decimal("0.001"),
        Decimal("0.002"),
        Decimal("0.003"),
        Decimal("0.004"),
        Decimal("0.005"),
        Decimal("0.004"),
        Decimal("0.003"),
        Decimal("0.002"),
        Decimal("0.001"),
        Decimal("0.0005"),
        Decimal(0),
        Decimal(0),
    )
    rebuilt = list(rows[: anchor_index + 1])
    previous_close = anchor
    for value, offset in zip(rows[anchor_index + 1 :], offsets, strict=True):
        close = anchor + offset
        rebuilt.append(
            _copy_row(
                value,
                close=close,
                previous_close=previous_close,
                high=close + Decimal(1),
                low=close - Decimal(1),
            )
        )
        previous_close = close
    return tuple(rebuilt)


def test_exact_8653_rows_build_positive_nonpromoting_shadow_evidence() -> None:
    evidence = _positive_evidence()
    rows = _base_rows()

    assert evidence.status is RobustZStatusV2.READY
    assert evidence.prior_observation_count == 8_640
    assert evidence.direction == 1
    assert 0 < evidence.strength_micros < EVIDENCE_STRENGTH_SCALE_V2
    assert evidence.role == PRICE_STRUCTURE_MOMENTUM_ROLE_V2
    assert not evidence.raw_membership_verified
    assert not evidence.cursor_finality_verified
    assert not evidence.promoting_eligible
    assert evidence.latest_source_event_ms == BAR_CLOSE_MS
    assert evidence.latest_source_receipt_ms == D_MS
    assert canonical_price_structure_momentum_evidence_v2(evidence)

    assert evidence.composite is not None
    with localcontext(protocol_decimal_context_v2()):
        expected_return_1 = (rows[-1].close / rows[-2].close).ln()
        expected_return_12 = (rows[-1].close / rows[-13].close).ln()
        magnitude = abs(evidence.composite)
        expected = int(
            (
                Decimal(EVIDENCE_STRENGTH_SCALE_V2) * magnitude / (Decimal(1) + magnitude)
            ).to_integral_value(rounding=ROUND_FLOOR)
        )
    assert evidence.current_return_1 == expected_return_1
    assert evidence.current_return_12 == expected_return_12
    assert evidence.strength_micros == expected


def test_negative_current_close_is_bearish_and_prior_scales_exclude_current() -> None:
    positive = _positive_evidence()
    rows = _base_rows()
    negative_close = rows[-2].close - Decimal(1)
    negative = build_price_structure_momentum_evidence_v2(_with_current_close(negative_close))

    assert negative.status is RobustZStatusV2.READY
    assert negative.direction == -1
    assert negative.strength_micros > 0
    assert negative.prior_location_1 == positive.prior_location_1
    assert negative.prior_location_12 == positive.prior_location_12
    assert negative.prior_mad_1 == positive.prior_mad_1
    assert negative.prior_mad_12 == positive.prior_mad_12
    assert negative.prior_scale_1 == positive.prior_scale_1
    assert negative.prior_scale_12 == positive.prior_scale_12
    assert negative.close_path_slice_sha256 != positive.close_path_slice_sha256


def test_same_prior_larger_current_magnitude_cannot_reduce_strength() -> None:
    rows = _base_rows()
    anchor = rows[-2].close
    smaller = build_price_structure_momentum_evidence_v2(
        _with_current_close(anchor * Decimal("1.001"))
    )
    larger = build_price_structure_momentum_evidence_v2(
        _with_current_close(anchor * Decimal("1.01"))
    )

    assert smaller.status is RobustZStatusV2.READY
    assert larger.status is RobustZStatusV2.READY
    assert smaller.prior_scale_1 == larger.prior_scale_1
    assert smaller.prior_scale_12 == larger.prior_scale_12
    assert smaller.composite is not None
    assert larger.composite is not None
    assert Decimal(0) < smaller.composite < larger.composite
    assert 0 < smaller.strength_micros <= larger.strength_micros


def test_exact_zero_composite_is_ready_neutral_with_zero_strength() -> None:
    evidence = build_price_structure_momentum_evidence_v2(_neutral_current_rows())

    assert evidence.status is RobustZStatusV2.READY
    assert evidence.current_return_1 == 0
    assert evidence.current_return_12 == 0
    assert evidence.normalized_return_1 == 0
    assert evidence.normalized_return_12 == 0
    assert evidence.composite == 0
    assert evidence.direction == 0
    assert evidence.strength_micros == 0


@pytest.mark.parametrize(
    ("multiplier", "expected_sign"),
    (
        (Decimal("1.000000000000000001"), 1),
        (Decimal("0.999999999999999999"), -1),
    ),
)
def test_tiny_nonzero_composite_quantizes_to_ready_neutral(
    multiplier: Decimal,
    expected_sign: int,
) -> None:
    rows = _neutral_current_rows()
    current = rows[-1]
    tiny_close = current.close * multiplier
    evidence = build_price_structure_momentum_evidence_v2(
        (
            *rows[:-1],
            _copy_row(
                current,
                close=tiny_close,
                high=tiny_close + Decimal(1),
                low=tiny_close - Decimal(1),
            ),
        )
    )

    assert evidence.status is RobustZStatusV2.READY
    assert evidence.composite is not None
    assert (evidence.composite > 0) is (expected_sign > 0)
    assert evidence.direction == 0
    assert evidence.strength_micros == 0


def test_permutation_is_canonical_and_exact_row_count_boundaries_fail_closed() -> None:
    forward = _positive_evidence()
    reverse = build_price_structure_momentum_evidence_v2(tuple(reversed(_base_rows())))
    warmup = build_price_structure_momentum_evidence_v2(_base_rows()[1:])
    rows = _base_rows()
    future = _row(
        PRICE_STRUCTURE_MOMENTUM_ROW_COUNT_V2,
        close=rows[-1].close + Decimal("0.001"),
        previous_close=rows[-1].close,
    )

    assert reverse == forward
    assert warmup.status is RobustZStatusV2.FEATURE_NOT_READY_WARMUP
    assert warmup.prior_observation_count == 8_639
    assert warmup.composite is None
    with pytest.raises(PriceEvidenceContractErrorV2, match="8,653-row"):
        build_price_structure_momentum_evidence_v2((*rows, future))


def test_gap_duplicate_previous_close_mismatch_and_identity_drift_are_invalid() -> None:
    rows = _base_rows()
    gap = build_price_structure_momentum_evidence_v2((*rows[:100], *rows[101:]))
    duplicate = build_price_structure_momentum_evidence_v2(
        (*rows[:100], rows[100], rows[100], *rows[102:])
    )
    mismatch_row = _copy_row(rows[100], previous_close=Decimal(999))
    mismatch = build_price_structure_momentum_evidence_v2((*rows[:100], mismatch_row, *rows[101:]))
    drift_rows = (
        _copy_row(rows[100], symbol="ETHUSDT"),
        _copy_row(rows[100], promoting_plan_sha256="d" * 64),
        _copy_row(rows[100], capture_root_sha256="e" * 64),
        _copy_row(rows[100], schema_sha256="f" * 64),
    )
    drifts = tuple(
        build_price_structure_momentum_evidence_v2((*rows[:100], drift_row, *rows[101:]))
        for drift_row in drift_rows
    )

    for evidence in (gap, duplicate, mismatch, *drifts):
        assert evidence.status is RobustZStatusV2.DATA_INVALID_FEATURE
        assert evidence.composite is None
        assert evidence.direction == 0
        assert evidence.strength_micros == 0
    assert "PRICE_KLINE_HISTORY_GAP_OR_DUPLICATE" in duplicate.reasons
    for evidence in drifts:
        assert "PRICE_KLINE_IDENTITY_DRIFT" in evidence.reasons


def test_spot_venue_drift_is_rejected_by_upstream_family_b_factory() -> None:
    with pytest.raises(
        FamilyBFeatureContractErrorV2,
        match="USD-M Futures provenance",
    ):
        _copy_row(_base_rows()[100], venue=VenueV2.SPOT)


def test_closed_publication_time_accepts_post_close_through_d_and_rejects_bad_order() -> None:
    rows = _base_rows()
    current = rows[-1]
    post_close = build_price_structure_momentum_evidence_v2(
        (*rows[:-1], _copy_row(current, event_ms=BAR_CLOSE_MS + 1))
    )
    at_cutoff = build_price_structure_momentum_evidence_v2(
        (*rows[:-1], _copy_row(current, event_ms=D_MS, receipt_ms=D_MS))
    )
    late = build_price_structure_momentum_evidence_v2(
        (*rows[:-1], _copy_row(current, receipt_ms=D_MS + 1))
    )
    preclose_event = build_price_structure_momentum_evidence_v2(
        (*rows[:-1], _copy_row(current, event_ms=BAR_CLOSE_MS - 1))
    )
    unclosed = build_price_structure_momentum_evidence_v2(
        (*rows[:-1], _copy_row(current, closed=False))
    )
    receipt_before_event = build_price_structure_momentum_evidence_v2(
        (*rows[:-1], _copy_row(current, event_ms=D_MS, receipt_ms=D_MS - 1))
    )

    assert post_close.status is RobustZStatusV2.READY
    assert post_close.latest_source_event_ms == BAR_CLOSE_MS + 1
    assert at_cutoff.status is RobustZStatusV2.READY
    assert at_cutoff.latest_source_event_ms == D_MS
    assert late.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert preclose_event.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert unclosed.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert receipt_before_event.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert "PRICE_KLINE_RECEIPT_AFTER_DECISION_CUTOFF" in late.reasons
    assert "PRICE_KLINE_EVENT_PRECEDES_OWN_CLOSE" in preclose_event.reasons
    assert "PRICE_REQUIRES_CLOSED_CONTIGUOUS_5M_KLINES" in unclosed.reasons
    assert "PRICE_KLINE_RECEIPT_PRECEDES_EVENT" in receipt_before_event.reasons


def test_zero_mad_scale_is_nonready_without_partial_numeric_fallback() -> None:
    evidence = build_price_structure_momentum_evidence_v2(_constant_rows())

    assert evidence.status is RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    assert evidence.current_return_1 is None
    assert evidence.current_return_12 is None
    assert evidence.composite is None
    assert evidence.direction == 0
    assert evidence.strength_micros == 0


def test_shared_close_calculation_matches_frozen_evidence_and_is_factory_sealed() -> None:
    rows = _base_rows()
    evidence = _positive_evidence()
    calculation = calculate_price_close_path_v2(tuple(row.close for row in rows))

    assert calculation.status is RobustZStatusV2.READY
    assert calculation.current_return_1 == evidence.current_return_1
    assert calculation.current_return_12 == evidence.current_return_12
    assert calculation.composite == evidence.composite
    assert calculation.direction == evidence.direction
    assert calculation.strength_micros == evidence.strength_micros
    assert canonical_price_close_path_calculation_v2(calculation)

    constructor_values = {
        item.name: getattr(calculation, item.name) for item in fields(calculation) if item.init
    }
    with pytest.raises(PriceEvidenceContractErrorV2, match="frozen factory"):
        PriceClosePathCalculationV2(**constructor_values)  # type: ignore[arg-type]
    with pytest.raises(PriceEvidenceContractErrorV2, match="frozen factory"):
        replace(calculation, direction=-1)


def test_shared_close_calculation_rejects_wrong_count_and_nonpositive_close() -> None:
    closes = tuple(row.close for row in _base_rows())

    with pytest.raises(PriceEvidenceContractErrorV2, match="8,653"):
        calculate_price_close_path_v2(closes[1:])
    with pytest.raises(PriceEvidenceContractErrorV2, match="positive finite"):
        calculate_price_close_path_v2((*closes[:-1], Decimal(0)))


def test_precomputed_return_series_is_exactly_equal_to_close_path_calculation() -> None:
    closes = tuple(row.close for row in _base_rows())
    with localcontext(protocol_decimal_context_v2()):
        returns_1 = tuple(
            (closes[index] / closes[index - 1]).ln()
            for index in range(12, len(closes))
        )
        returns_12 = tuple(
            (closes[index] / closes[index - 12]).ln()
            for index in range(12, len(closes))
        )

    from_closes = calculate_price_close_path_v2(closes)
    from_returns = calculate_price_return_series_v2(returns_1, returns_12)

    assert from_returns == from_closes
    assert from_returns.calculation_sha256 == from_closes.calculation_sha256
    assert (
        from_closes.calculation_sha256
        == "7efb5e5c7e7f02bd51137ab929e3263a22c01e5eabeb414fe9fb2e2907b1eb15"
    )
    assert canonical_price_close_path_calculation_v2(
        from_returns
    ) == canonical_price_close_path_calculation_v2(from_closes)


def test_precomputed_return_series_preserves_zero_scale_status() -> None:
    zero_returns = (Decimal(0),) * 8_641

    calculation = calculate_price_return_series_v2(zero_returns, zero_returns)

    assert calculation.status is RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    assert not calculation.ready
    assert calculation.current_return_1 is None
    assert calculation.current_return_12 is None
    assert calculation.direction == calculation.strength_micros == 0


def test_precomputed_finite_return_overflow_fails_closed_without_partial_values() -> None:
    overflowing_returns = (
        Decimal("1e999999"),
        Decimal("-1e999999"),
    ) * 4_320 + (Decimal(1),)

    calculation = calculate_price_return_series_v2(
        overflowing_returns,
        overflowing_returns,
    )

    assert calculation.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert calculation.reason == "PRICE_DECIMAL_ARITHMETIC_INVALID"
    assert calculation.current_return_1 is None
    assert calculation.current_return_12 is None
    assert calculation.composite is None
    assert calculation.direction == calculation.strength_micros == 0


@pytest.mark.parametrize(
    ("returns_1", "returns_12", "message"),
    (
        ((Decimal(0),) * 8_640, (Decimal(0),) * 8_641, "8,641"),
        ((Decimal(0),) * 8_641, (Decimal(0),) * 8_642, "8,641"),
        ((Decimal(0),) * 8_640 + (Decimal("NaN"),), (Decimal(0),) * 8_641, "finite"),
    ),
)
def test_precomputed_return_series_rejects_wrong_count_and_nonfinite_values(
    returns_1: tuple[Decimal, ...],
    returns_12: tuple[Decimal, ...],
    message: str,
) -> None:
    with pytest.raises(PriceEvidenceContractErrorV2, match=message):
        calculate_price_return_series_v2(returns_1, returns_12)

    with pytest.raises(PriceEvidenceContractErrorV2, match="immutable"):
        calculate_price_return_series_v2(
            list((Decimal(0),) * 8_641),  # type: ignore[arg-type]
            (Decimal(0),) * 8_641,
        )


def test_high_low_changes_only_full_lineage_not_close_economics_or_score() -> None:
    baseline = _positive_evidence()
    rows = _base_rows()
    changed_row = _copy_row(
        rows[500],
        high=rows[500].close + Decimal(10),
        low=rows[500].close - Decimal(10),
    )
    changed = build_price_structure_momentum_evidence_v2((*rows[:500], changed_row, *rows[501:]))

    assert changed.close_path_slice_sha256 == baseline.close_path_slice_sha256
    assert changed.current_return_1 == baseline.current_return_1
    assert changed.current_return_12 == baseline.current_return_12
    assert changed.composite == baseline.composite
    assert changed.direction == baseline.direction
    assert changed.strength_micros == baseline.strength_micros
    assert changed.source_lineage_root_sha256 != baseline.source_lineage_root_sha256
    assert changed.evidence_sha256 != baseline.evidence_sha256


def test_direct_construction_and_replace_cannot_bypass_shadow_factory() -> None:
    evidence = _positive_evidence()
    constructor_values = {
        item.name: getattr(evidence, item.name) for item in fields(evidence) if item.init
    }

    with pytest.raises(PriceEvidenceContractErrorV2, match="shadow factory"):
        PriceStructureMomentumEvidenceV2(**constructor_values)  # type: ignore[arg-type]
    with pytest.raises(PriceEvidenceContractErrorV2, match="shadow factory"):
        replace(evidence, symbol="ETHUSDT")


def test_canonical_serializer_rejects_object_setattr_tampering() -> None:
    evidence = build_price_structure_momentum_evidence_v2(_base_rows())
    object.__setattr__(evidence, "direction", -1)

    with pytest.raises(PriceEvidenceContractErrorV2, match="canonical content"):
        canonical_price_structure_momentum_evidence_v2(evidence)
