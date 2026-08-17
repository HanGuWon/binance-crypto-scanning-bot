from __future__ import annotations

import hashlib
from dataclasses import replace
from decimal import Decimal

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.research.directional_agreement_audit import (
    DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2,
    DIRECTIONAL_AGREEMENT_OUTCOME_PROTOCOL_V2,
    DirectionalAgreementAuditErrorV2,
    DirectionalAgreementBucketV2,
    DirectionalAgreementOutcomeV2,
    DirectionalAgreementSideV2,
)
from signalbot.r4b_v2.research.directional_agreement_bootstrap import (
    DIRECTIONAL_AGREEMENT_BOOTSTRAP_BLOCK_DAYS_V2,
    DIRECTIONAL_AGREEMENT_BOOTSTRAP_VERSION_V2,
    bootstrap_directional_agreement_contrasts_v2,
)
from signalbot.r4b_v2.strategy.directional_evidence import (
    DIRECTIONAL_EVIDENCE_RULE_VERSION_V2,
    DirectionalStateClassV2,
)

DAY_MS = 86_400_000
CALENDAR_START_MS = 20_000 * DAY_MS


def _event_rows(
    name: str,
    state_class: DirectionalStateClassV2,
    agreement: int,
    *,
    day: int,
    values: tuple[int | None, ...],
    execution_contract_sha256: str = "9" * 64,
) -> tuple[DirectionalAgreementOutcomeV2, ...]:
    assert len(values) == len(DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2)
    event_id = hashlib.sha256(name.encode()).hexdigest()
    return tuple(
        DirectionalAgreementOutcomeV2(
            event_id=event_id,
            outcome_protocol_version=DIRECTIONAL_AGREEMENT_OUTCOME_PROTOCOL_V2,
            rule_version=DIRECTIONAL_EVIDENCE_RULE_VERSION_V2,
            execution_contract_sha256=execution_contract_sha256,
            venue=VenueV2.USDM_FUTURES,
            symbol="BTCUSDT",
            decision_time_ms=CALENDAR_START_MS + day * DAY_MS + DAY_MS // 2,
            state_class=state_class,
            directional_agreement_micros=agreement,
            horizon_bars=horizon,
            evaluable=value is not None,
            exclusion_reason="" if value is not None else "MISSING_EXECUTION_EVIDENCE",
            net_return_micros=value,
        )
        for horizon, value in zip(
            DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2,
            values,
            strict=True,
        )
    )


def _four_cells() -> tuple[DirectionalAgreementOutcomeV2, ...]:
    return (
        *_event_rows(
            "bull-tilt",
            DirectionalStateClassV2.BULLISH_STATE_TILT,
            400_000,
            day=0,
            values=(100, 110, 120, 130, 140),
        ),
        *_event_rows(
            "bull-broad",
            DirectionalStateClassV2.BROAD_BULLISH_STATE,
            700_000,
            day=0,
            values=(300, 320, 340, 360, 380),
        ),
        *_event_rows(
            "bear-tilt",
            DirectionalStateClassV2.BEARISH_STATE_TILT,
            -400_000,
            day=7,
            values=(50, 60, 70, 80, 90),
        ),
        *_event_rows(
            "bear-broad",
            DirectionalStateClassV2.BROAD_BEARISH_STATE,
            -700_000,
            day=7,
            values=(150, 170, 190, 210, 230),
        ),
    )


def test_shared_utc_schedule_is_order_invariant_and_includes_zero_alert_days() -> None:
    rows = _four_cells()
    first = bootstrap_directional_agreement_contrasts_v2(
        rows,
        calendar_start_ms=CALENDAR_START_MS,
        calendar_end_ms=CALENDAR_START_MS + 14 * DAY_MS,
        samples=200,
        seed=17,
    )
    reversed_result = bootstrap_directional_agreement_contrasts_v2(
        tuple(reversed(rows)),
        calendar_start_ms=CALENDAR_START_MS,
        calendar_end_ms=CALENDAR_START_MS + 14 * DAY_MS,
        samples=200,
        seed=17,
    )

    assert first == reversed_result
    assert first.bootstrap_version == DIRECTIONAL_AGREEMENT_BOOTSTRAP_VERSION_V2
    assert first.block_days == DIRECTIONAL_AGREEMENT_BOOTSTRAP_BLOCK_DAYS_V2 == 7
    assert first.samples == 200
    assert first.seed == 17
    assert first.event_count == 4
    assert first.outcome_count == 20
    assert first.shared_draw_schedule_sha256
    assert {
        item.shared_draw_schedule_sha256 for item in (*first.cells, *first.contrasts)
    } == {first.shared_draw_schedule_sha256}
    bull_tilt = next(
        item
        for item in first.cells
        if item.horizon_bars == 1
        and item.side is DirectionalAgreementSideV2.BULLISH
        and item.bucket is DirectionalAgreementBucketV2.TILT_2_OF_3
    )
    assert bull_tilt.events == 1
    assert bull_tilt.evaluable == 1
    assert bull_tilt.zero_alert_days == 13
    assert bull_tilt.valid_replicates + bull_tilt.invalid_replicates == 200
    assert not bull_tilt.inference_complete
    assert not bull_tilt.frozen_formula_efficacy_validated
    assert not bull_tilt.probability
    assert not bull_tilt.probability_calibrated
    assert first.exposure_status == "EXPOSED_RETROSPECTIVE_ONLY"
    assert not first.inference_complete
    assert not first.frozen_formula_efficacy_validated
    assert not first.probability
    assert not first.probability_calibrated


def test_exact_seven_day_constant_bootstrap_statistics_are_frozen() -> None:
    rows = (
        *_event_rows(
            "constant-tilt",
            DirectionalStateClassV2.BULLISH_STATE_TILT,
            400_000,
            day=0,
            values=(100, 100, 100, 100, 100),
        ),
        *_event_rows(
            "constant-broad",
            DirectionalStateClassV2.BROAD_BULLISH_STATE,
            700_000,
            day=0,
            values=(300, 300, 300, 300, 300),
        ),
    )
    result = bootstrap_directional_agreement_contrasts_v2(
        rows,
        calendar_start_ms=CALENDAR_START_MS,
        calendar_end_ms=CALENDAR_START_MS + 7 * DAY_MS,
        samples=4,
        seed=0,
    )
    bullish = next(
        item
        for item in result.contrasts
        if item.horizon_bars == 1
        and item.side is DirectionalAgreementSideV2.BULLISH
    )
    bearish = next(
        item
        for item in result.contrasts
        if item.horizon_bars == 1
        and item.side is DirectionalAgreementSideV2.BEARISH
    )

    assert bullish.broad_minus_tilt_mean_net_return_micros == 200
    assert bullish.valid_replicates == 4
    assert bullish.invalid_replicates == 0
    assert bullish.two_sided_95_interval_micros == (Decimal(200), Decimal(200))
    assert bullish.one_sided_basic_95_lower_micros == Decimal(200)
    assert bullish.null_centered_one_sided_p_value == Decimal("0.2")
    assert not bullish.inference_complete
    assert not bullish.frozen_formula_efficacy_validated
    assert not bullish.probability
    assert not bullish.probability_calibrated
    assert bearish.broad_minus_tilt_mean_net_return_micros is None
    assert bearish.valid_replicates == 0
    assert bearish.invalid_replicates == 4
    assert bearish.two_sided_95_interval_micros is None
    assert bearish.one_sided_basic_95_lower_micros is None
    assert bearish.null_centered_one_sided_p_value is None


def test_nonpositive_point_uses_frozen_one_sided_null_p_value_of_one() -> None:
    rows = (
        *_event_rows(
            "negative-tilt",
            DirectionalStateClassV2.BULLISH_STATE_TILT,
            400_000,
            day=0,
            values=(100, 100, 100, 100, 100),
        ),
        *_event_rows(
            "negative-broad",
            DirectionalStateClassV2.BROAD_BULLISH_STATE,
            700_000,
            day=0,
            values=(50, 50, 50, 50, 50),
        ),
    )
    result = bootstrap_directional_agreement_contrasts_v2(
        rows,
        calendar_start_ms=CALENDAR_START_MS,
        calendar_end_ms=CALENDAR_START_MS + 7 * DAY_MS,
        samples=3,
        seed=1,
    )
    contrast = next(
        item
        for item in result.contrasts
        if item.horizon_bars == 1
        and item.side is DirectionalAgreementSideV2.BULLISH
    )

    assert contrast.broad_minus_tilt_mean_net_return_micros == -50
    assert contrast.two_sided_95_interval_micros == (Decimal(-50), Decimal(-50))
    assert contrast.one_sided_basic_95_lower_micros == Decimal(-50)
    assert contrast.null_centered_one_sided_p_value == Decimal(1)


def test_unevaluable_alert_is_missing_not_zero_and_is_not_a_zero_alert_day() -> None:
    rows = (
        *_event_rows(
            "missing-tilt",
            DirectionalStateClassV2.BULLISH_STATE_TILT,
            400_000,
            day=0,
            values=(None, 1, 1, 1, 1),
        ),
        *_event_rows(
            "present-broad",
            DirectionalStateClassV2.BROAD_BULLISH_STATE,
            700_000,
            day=0,
            values=(100, 100, 100, 100, 100),
        ),
    )
    result = bootstrap_directional_agreement_contrasts_v2(
        rows,
        calendar_start_ms=CALENDAR_START_MS,
        calendar_end_ms=CALENDAR_START_MS + 7 * DAY_MS,
        samples=5,
        seed=2,
    )
    tilt_cell = next(
        item
        for item in result.cells
        if item.horizon_bars == 1
        and item.side is DirectionalAgreementSideV2.BULLISH
        and item.bucket is DirectionalAgreementBucketV2.TILT_2_OF_3
    )
    contrast = next(
        item
        for item in result.contrasts
        if item.horizon_bars == 1
        and item.side is DirectionalAgreementSideV2.BULLISH
    )

    assert tilt_cell.events == 1
    assert tilt_cell.evaluable == 0
    assert tilt_cell.zero_alert_days == 6
    assert tilt_cell.valid_replicates == 0
    assert tilt_cell.invalid_replicates == 5
    assert contrast.broad_minus_tilt_mean_net_return_micros is None
    assert contrast.valid_replicates == 0
    assert contrast.invalid_replicates == 5


def test_bootstrap_preserves_duplicate_partial_identity_and_cost_fail_closed() -> None:
    rows = _event_rows(
        "boundary",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        700_000,
        day=0,
        values=(1, 2, 3, 4, 5),
    )
    kwargs = {
        "calendar_start_ms": CALENDAR_START_MS,
        "calendar_end_ms": CALENDAR_START_MS + 7 * DAY_MS,
        "samples": 2,
        "seed": 3,
    }
    with pytest.raises(DirectionalAgreementAuditErrorV2, match="every frozen horizon"):
        bootstrap_directional_agreement_contrasts_v2(rows[:-1], **kwargs)
    with pytest.raises(DirectionalAgreementAuditErrorV2, match="duplicate"):
        bootstrap_directional_agreement_contrasts_v2((*rows, rows[0]), **kwargs)
    with pytest.raises(DirectionalAgreementAuditErrorV2, match="changes identity"):
        bootstrap_directional_agreement_contrasts_v2(
            (*rows[:-1], replace(rows[-1], symbol="ETHUSDT")),
            **kwargs,
        )
    with pytest.raises(DirectionalAgreementAuditErrorV2, match="changes identity"):
        bootstrap_directional_agreement_contrasts_v2(
            (*rows[:-1], replace(rows[-1], execution_contract_sha256="8" * 64)),
            **kwargs,
        )

    other_cost = _event_rows(
        "other-cost",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        400_000,
        day=1,
        values=(1, 2, 3, 4, 5),
        execution_contract_sha256="8" * 64,
    )
    with pytest.raises(DirectionalAgreementAuditErrorV2, match="exactly one execution"):
        bootstrap_directional_agreement_contrasts_v2(
            (*rows, *other_cost),
            **kwargs,
        )


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"calendar_start_ms": CALENDAR_START_MS + 1}, "UTC midnights"),
        ({"calendar_end_ms": CALENDAR_START_MS + 6 * DAY_MS}, "between 7"),
        ({"samples": 0}, "samples"),
        ({"samples": True}, "samples"),
        ({"seed": -1}, "seed"),
        ({"seed": True}, "seed"),
        ({"seed": 9_007_199_254_740_992}, "seed"),
    ],
)
def test_bootstrap_request_boundaries_fail_closed(
    updates: dict[str, int | bool],
    message: str,
) -> None:
    rows = _event_rows(
        "request-boundary",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        700_000,
        day=0,
        values=(1, 2, 3, 4, 5),
    )
    kwargs: dict[str, int | bool] = {
        "calendar_start_ms": CALENDAR_START_MS,
        "calendar_end_ms": CALENDAR_START_MS + 7 * DAY_MS,
        "samples": 2,
        "seed": 3,
    }
    kwargs.update(updates)

    with pytest.raises(DirectionalAgreementAuditErrorV2, match=message):
        bootstrap_directional_agreement_contrasts_v2(
            rows,
            calendar_start_ms=int(kwargs["calendar_start_ms"]),
            calendar_end_ms=int(kwargs["calendar_end_ms"]),
            samples=kwargs["samples"],  # type: ignore[arg-type]
            seed=kwargs["seed"],  # type: ignore[arg-type]
        )


def test_outcome_outside_explicit_calendar_fails_closed() -> None:
    rows = _event_rows(
        "outside-calendar",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        700_000,
        day=7,
        values=(1, 2, 3, 4, 5),
    )

    with pytest.raises(DirectionalAgreementAuditErrorV2, match="outside"):
        bootstrap_directional_agreement_contrasts_v2(
            rows,
            calendar_start_ms=CALENDAR_START_MS,
            calendar_end_ms=CALENDAR_START_MS + 7 * DAY_MS,
            samples=2,
            seed=3,
        )
