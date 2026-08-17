from __future__ import annotations

import hashlib
from dataclasses import fields, replace
from decimal import Decimal
from functools import cache

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.protocol.features import (
    ROBUST_Z_PRIOR_WINDOW_V2,
    RobustZStatusV2,
)
from signalbot.r4b_v2.strategy.family_b_features import FamilyBKlineBarV2
from signalbot.r4b_v2.strategy.volatility_evidence import (
    VolatilityEvidenceContractErrorV2,
    VolatilityRegimeEvidenceV2,
    build_volatility_regime_evidence_v2,
    canonical_volatility_regime_evidence_v2,
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
    width: Decimal,
    previous_close: Decimal,
    close: Decimal,
    event_ms: int | None = None,
    receipt_ms: int | None = None,
) -> FamilyBKlineBarV2:
    first_open_ms = BAR_OPEN_MS - (
        (ROBUST_Z_PRIOR_WINDOW_V2 + 1) * FIVE_MINUTE_MS_V2
    )
    bar_open_ms = first_open_ms + index * FIVE_MINUTE_MS_V2
    bar_close_ms = bar_open_ms + FIVE_MINUTE_MS_V2 - 1
    return FamilyBKlineBarV2.create(
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        promoting_plan_sha256=PLAN_SHA256,
        capture_root_sha256=CAPTURE_ROOT_SHA256,
        schema_sha256=SCHEMA_SHA256,
        interval_ms=FIVE_MINUTE_MS_V2,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        open_event_id=f"kline-open-{index}",
        close_event_id=f"kline-close-{index}",
        closed=True,
        event_ms=bar_close_ms if event_ms is None else event_ms,
        receipt_ms=(
            D_MS
            if receipt_ms is None and index == ROBUST_Z_PRIOR_WINDOW_V2 + 1
            else bar_close_ms if receipt_ms is None else receipt_ms
        ),
        high=close + width,
        low=close - width,
        close=close,
        previous_close=previous_close,
        source_evidence_sha256=hashlib.sha256(f"kline-{index}".encode()).hexdigest(),
    )


@cache
def _bars(*, constant_range: bool = False) -> tuple[FamilyBKlineBarV2, ...]:
    rows: list[FamilyBKlineBarV2] = []
    previous_close = Decimal(100)
    for index in range(ROBUST_Z_PRIOR_WINDOW_V2 + 2):
        close = previous_close + Decimal("0.0001")
        width = Decimal(1) if constant_range else Decimal(index % 5 + 1)
        rows.append(
            _row(
                index,
                width=width,
                previous_close=previous_close,
                close=close,
            )
        )
        previous_close = close
    return tuple(rows)


def test_exact_8640_prior_true_ranges_are_ready_but_remain_nondirectional() -> None:
    evidence = build_volatility_regime_evidence_v2(_bars())

    assert evidence.status is RobustZStatusV2.READY
    assert evidence.prior_observation_count == 8_640
    assert evidence.current_true_range is not None
    assert evidence.robust_z is not None
    assert not evidence.directional
    assert evidence.direction == 0
    assert evidence.directional_strength_micros == 0
    assert canonical_volatility_regime_evidence_v2(evidence)


def test_permutation_is_canonical_and_event_receipt_equalities_pass() -> None:
    forward = build_volatility_regime_evidence_v2(_bars())
    reverse = build_volatility_regime_evidence_v2(tuple(reversed(_bars())))

    assert forward == reverse
    assert forward.bar_close_ms == BAR_CLOSE_MS
    assert forward.latest_source_event_ms == BAR_CLOSE_MS
    assert forward.latest_source_receipt_ms == D_MS


def test_post_close_publication_time_is_preserved_through_cutoff() -> None:
    bars = _bars()
    current = bars[-1]
    post_close_current = _row(
        ROBUST_Z_PRIOR_WINDOW_V2 + 1,
        width=Decimal(1),
        previous_close=current.previous_close,
        close=current.close,
        event_ms=BAR_CLOSE_MS + 1,
    )
    cutoff_current = _row(
        ROBUST_Z_PRIOR_WINDOW_V2 + 1,
        width=Decimal(1),
        previous_close=current.previous_close,
        close=current.close,
        event_ms=D_MS,
        receipt_ms=D_MS,
    )

    post_close = build_volatility_regime_evidence_v2(
        (*bars[:-1], post_close_current)
    )
    at_cutoff = build_volatility_regime_evidence_v2(
        (*bars[:-1], cutoff_current)
    )

    assert post_close.status is RobustZStatusV2.READY
    assert post_close.latest_source_event_ms == BAR_CLOSE_MS + 1
    assert at_cutoff.status is RobustZStatusV2.READY
    assert at_cutoff.latest_source_event_ms == D_MS


def test_preclose_or_postreceipt_publication_time_fails_closed() -> None:
    bars = _bars()
    current = bars[-1]
    preclose = _row(
        ROBUST_Z_PRIOR_WINDOW_V2 + 1,
        width=Decimal(1),
        previous_close=current.previous_close,
        close=current.close,
        event_ms=BAR_CLOSE_MS - 1,
    )
    postreceipt = _row(
        ROBUST_Z_PRIOR_WINDOW_V2 + 1,
        width=Decimal(1),
        previous_close=current.previous_close,
        close=current.close,
        event_ms=D_MS,
        receipt_ms=D_MS - 1,
    )

    preclose_evidence = build_volatility_regime_evidence_v2(
        (*bars[:-1], preclose)
    )
    postreceipt_evidence = build_volatility_regime_evidence_v2(
        (*bars[:-1], postreceipt)
    )

    assert preclose_evidence.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert postreceipt_evidence.status is RobustZStatusV2.DATA_INVALID_FEATURE


def test_short_history_is_warmup_and_exposes_no_partial_regime_values() -> None:
    evidence = build_volatility_regime_evidence_v2(_bars()[:-1])

    assert evidence.status is RobustZStatusV2.FEATURE_NOT_READY_WARMUP
    assert evidence.prior_observation_count == 8_639
    assert evidence.current_true_range is None
    assert evidence.robust_z is None


def test_gap_and_previous_close_mismatch_fail_closed_as_invalid_data() -> None:
    bars = _bars()
    gap = build_volatility_regime_evidence_v2((*bars[:100], *bars[101:]))
    bad_row = _row(
        100,
        width=Decimal(1),
        previous_close=Decimal(999),
        close=bars[100].close,
    )
    mismatch = build_volatility_regime_evidence_v2(
        (*bars[:100], bad_row, *bars[101:])
    )

    assert gap.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert mismatch.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert gap.current_true_range is None
    assert mismatch.current_true_range is None


def test_first_computed_true_range_is_anchored_inside_the_sealed_slice() -> None:
    bars = _bars()
    first_tr = bars[1]
    bad_first_tr = _row(
        1,
        width=Decimal(1),
        previous_close=Decimal(999),
        close=first_tr.close,
    )
    evidence = build_volatility_regime_evidence_v2(
        (bars[0], bad_first_tr, *bars[2:])
    )

    assert evidence.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert evidence.current_true_range is None
    assert evidence.robust_z is None


def test_zero_scale_and_receipt_after_d_fail_without_numeric_fallback() -> None:
    zero = build_volatility_regime_evidence_v2(_bars(constant_range=True))
    bars = _bars()
    late_current = _row(
        ROBUST_Z_PRIOR_WINDOW_V2 + 1,
        width=Decimal(1),
        previous_close=bars[-1].previous_close,
        close=bars[-1].close,
        receipt_ms=D_MS + 1,
    )
    late = build_volatility_regime_evidence_v2((*bars[:-1], late_current))

    assert zero.status is RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    assert zero.current_true_range is None
    assert late.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert late.robust_z is None


def test_decimal_overflow_fails_closed_without_partial_numeric_output() -> None:
    bars = _bars()
    current = bars[-1]
    hostile_current = FamilyBKlineBarV2.create(
        symbol=current.symbol,
        venue=current.venue,
        promoting_plan_sha256=current.promoting_plan_sha256,
        capture_root_sha256=current.capture_root_sha256,
        schema_sha256=current.schema_sha256,
        interval_ms=current.interval_ms,
        bar_open_ms=current.bar_open_ms,
        bar_close_ms=current.bar_close_ms,
        open_event_id=current.open_event_id,
        close_event_id=current.close_event_id,
        closed=current.closed,
        event_ms=current.event_ms,
        receipt_ms=current.receipt_ms,
        high=Decimal("1E+1000000"),
        low=current.low,
        close=current.close,
        previous_close=current.previous_close,
        source_evidence_sha256=current.source_evidence_sha256,
    )
    evidence = build_volatility_regime_evidence_v2(
        (*bars[:-1], hostile_current)
    )

    assert evidence.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert evidence.current_true_range is None
    assert evidence.robust_z is None


def test_direct_construction_and_replace_cannot_bypass_factory() -> None:
    evidence = build_volatility_regime_evidence_v2(_bars())
    constructor_values = {
        item.name: getattr(evidence, item.name)
        for item in fields(evidence)
        if item.init
    }

    with pytest.raises(VolatilityEvidenceContractErrorV2, match="causal factory"):
        VolatilityRegimeEvidenceV2(**constructor_values)  # type: ignore[arg-type]
    with pytest.raises(VolatilityEvidenceContractErrorV2, match="causal factory"):
        replace(evidence, symbol="ETHUSDT")
