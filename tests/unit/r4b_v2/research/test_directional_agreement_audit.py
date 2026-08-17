from __future__ import annotations

from dataclasses import replace

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.research.directional_agreement_audit import (
    DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2,
    DIRECTIONAL_AGREEMENT_OUTCOME_PROTOCOL_V2,
    DirectionalAgreementAuditErrorV2,
    DirectionalAgreementBucketV2,
    DirectionalAgreementOutcomeV2,
    DirectionalAgreementSideV2,
    ProfitFactorStateV2,
    audit_directional_agreement_outcomes_v2,
)
from signalbot.r4b_v2.strategy.directional_evidence import (
    DIRECTIONAL_EVIDENCE_RULE_VERSION_V2,
    DirectionalStateClassV2,
)


def _event_rows(
    seed: str,
    state_class: DirectionalStateClassV2,
    agreement: int,
    values: tuple[int | None, ...],
) -> tuple[DirectionalAgreementOutcomeV2, ...]:
    assert len(values) == len(DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2)
    event_id = seed * 64
    return tuple(
        DirectionalAgreementOutcomeV2(
            event_id=event_id,
            outcome_protocol_version=DIRECTIONAL_AGREEMENT_OUTCOME_PROTOCOL_V2,
            rule_version=DIRECTIONAL_EVIDENCE_RULE_VERSION_V2,
            execution_contract_sha256="9" * 64,
            venue=VenueV2.USDM_FUTURES,
            symbol="BTCUSDT",
            decision_time_ms=1_750_000_000_000 + int(seed, 16),
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


def test_audit_compares_frozen_broad_and_tilt_without_probability_claim() -> None:
    tilt = _event_rows(
        "a",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        400_000,
        (100, -50, 200, -100, 0),
    )
    broad = _event_rows(
        "b",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        700_000,
        (300, 100, 400, 200, 50),
    )

    audit = audit_directional_agreement_outcomes_v2((*tilt, *broad))

    assert audit.event_count == 2
    assert audit.outcome_count == 10
    assert audit.horizons_bars == (1, 3, 6, 12, 72)
    assert audit.outcome_protocol_version == DIRECTIONAL_AGREEMENT_OUTCOME_PROTOCOL_V2
    assert audit.execution_contract_sha256s == ("9" * 64,)
    assert not audit.frozen_formula_efficacy_validated
    assert not audit.probability_calibrated
    assert not audit.overlapping_event_drawdown_valid
    contrast = next(
        item
        for item in audit.contrasts
        if item.horizon_bars == 1
        and item.side is DirectionalAgreementSideV2.BULLISH
    )
    assert contrast.broad_minus_tilt_mean_net_return_micros == 200
    assert contrast.point_monotonic is True
    assert not contrast.inference_complete
    assert not contrast.probability_calibrated
    broad_summary = next(
        item
        for item in audit.summaries
        if item.horizon_bars == 1
        and item.side is DirectionalAgreementSideV2.BULLISH
        and item.bucket is DirectionalAgreementBucketV2.BROAD_3_OF_3
    )
    assert broad_summary.coverage_micros == 1_000_000
    assert broad_summary.strict_after_cost_hit_rate_micros == 1_000_000
    assert broad_summary.profit_factor_state is ProfitFactorStateV2.POSITIVE_WITH_NO_LOSSES


def test_zero_after_cost_return_is_not_a_strict_hit_and_missing_is_not_zero() -> None:
    rows = _event_rows(
        "c",
        DirectionalStateClassV2.BEARISH_STATE_TILT,
        -500_000,
        (0, None, -25, 25, 0),
    )

    audit = audit_directional_agreement_outcomes_v2(rows)

    one_bar = next(
        item
        for item in audit.summaries
        if item.horizon_bars == 1
        and item.side is DirectionalAgreementSideV2.BEARISH
        and item.bucket is DirectionalAgreementBucketV2.TILT_2_OF_3
    )
    three_bar = next(
        item
        for item in audit.summaries
        if item.horizon_bars == 3
        and item.side is DirectionalAgreementSideV2.BEARISH
        and item.bucket is DirectionalAgreementBucketV2.TILT_2_OF_3
    )
    assert one_bar.strict_after_cost_hits == 0
    assert one_bar.strict_after_cost_hit_rate_micros == 0
    assert one_bar.profit_factor_state is ProfitFactorStateV2.ZERO_GROSS_WITH_NO_LOSSES
    assert three_bar.events == 1
    assert three_bar.evaluable == 0
    assert three_bar.coverage_micros == 0
    assert three_bar.mean_net_return_micros is None


def test_audit_rejects_partial_duplicate_and_identity_drift() -> None:
    rows = _event_rows(
        "d",
        DirectionalStateClassV2.BROAD_BEARISH_STATE,
        -800_000,
        (1, 2, 3, 4, 5),
    )
    with pytest.raises(DirectionalAgreementAuditErrorV2, match="every frozen horizon"):
        audit_directional_agreement_outcomes_v2(rows[:-1])
    with pytest.raises(DirectionalAgreementAuditErrorV2, match="duplicate"):
        audit_directional_agreement_outcomes_v2((*rows, rows[0]))
    drifted = (*rows[:-1], replace(rows[-1], symbol="ETHUSDT"))
    with pytest.raises(DirectionalAgreementAuditErrorV2, match="changes identity"):
        audit_directional_agreement_outcomes_v2(drifted)


def test_audit_rejects_two_ids_for_one_deterministic_panel_identity() -> None:
    first = _event_rows(
        "7",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        600_000,
        (1, 2, 3, 4, 5),
    )
    second = tuple(replace(item, event_id="8" * 64) for item in first)

    with pytest.raises(DirectionalAgreementAuditErrorV2, match="same deterministic"):
        audit_directional_agreement_outcomes_v2((*first, *second))


@pytest.mark.parametrize(
    ("state_class", "agreement"),
    [
        (DirectionalStateClassV2.BROAD_BULLISH_STATE, 0),
        (DirectionalStateClassV2.BEARISH_STATE_TILT, 1),
        (DirectionalStateClassV2.MIXED_OR_NEUTRAL_STATE, 10),
        (DirectionalStateClassV2.WITHHELD, -10),
    ],
)
def test_outcome_rejects_non_directional_or_sign_inconsistent_states(
    state_class: DirectionalStateClassV2,
    agreement: int,
) -> None:
    with pytest.raises(DirectionalAgreementAuditErrorV2):
        _event_rows("e", state_class, agreement, (1, 2, 3, 4, 5))


def test_outcome_boundary_requires_exact_evaluation_pairing() -> None:
    row = _event_rows(
        "f",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        1_000_000,
        (1, 2, 3, 4, 5),
    )[0]
    with pytest.raises(DirectionalAgreementAuditErrorV2, match="exclusion"):
        replace(row, exclusion_reason="SHOULD_NOT_EXIST")
    with pytest.raises(DirectionalAgreementAuditErrorV2, match="reason and no return"):
        replace(row, evaluable=False, exclusion_reason="DATA_GAP")


def test_summary_uses_deterministic_half_away_integer_median() -> None:
    first = _event_rows(
        "1",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        300_000,
        (-2, 1, 1, 1, 1),
    )
    second = _event_rows(
        "2",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        300_000,
        (-1, 1, 1, 1, 1),
    )

    audit = audit_directional_agreement_outcomes_v2((*first, *second))

    summary = next(
        item
        for item in audit.summaries
        if item.horizon_bars == 1
        and item.side is DirectionalAgreementSideV2.BULLISH
        and item.bucket is DirectionalAgreementBucketV2.TILT_2_OF_3
    )
    assert summary.mean_net_return_micros == -2
    assert summary.median_net_return_micros == -2
