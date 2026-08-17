from __future__ import annotations

from dataclasses import replace

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.research.historical_three_family_outcome_audit import (
    HISTORICAL_THREE_FAMILY_OUTCOME_AUDIT_VERSION_V2,
    HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
    HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
    HistoricalThreeFamilyAgreementBucketV2,
    HistoricalThreeFamilyOutcomeAuditErrorV2,
    HistoricalThreeFamilyOutcomeSummaryV2,
    HistoricalThreeFamilyOutcomeV2,
    HistoricalThreeFamilyProfitFactorStateV2,
    HistoricalThreeFamilySideV2,
    audit_historical_three_family_outcomes_v2,
)
from signalbot.r4b_v2.strategy.directional_evidence import DirectionalStateClassV2
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
)


def _event_rows(
    seed: str,
    state_class: DirectionalStateClassV2,
    agreement: int,
    values: tuple[int | None, ...],
    *,
    contract_seed: str = "9",
    symbol: str = "BTCUSDT",
) -> tuple[HistoricalThreeFamilyOutcomeV2, ...]:
    assert len(values) == len(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2)
    return tuple(
        HistoricalThreeFamilyOutcomeV2(
            event_id=seed * 64,
            outcome_protocol_version=HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
            rule_version=HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
            execution_contract_sha256=contract_seed * 64,
            venue=VenueV2.USDM_FUTURES,
            symbol=symbol,
            decision_time_ms=1_750_000_000_000 + int(seed, 16),
            state_class=state_class,
            directional_agreement_micros=agreement,
            horizon_bars=horizon,
            evaluable=value is not None,
            exclusion_reason="" if value is not None else "MISSING_FORWARD_BAR",
            net_return_micros=value,
        )
        for horizon, value in zip(
            HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
            values,
            strict=True,
        )
    )


def _summary(
    rows: tuple[HistoricalThreeFamilyOutcomeV2, ...],
    *,
    horizon: int,
    side: HistoricalThreeFamilySideV2,
    bucket: HistoricalThreeFamilyAgreementBucketV2,
) -> HistoricalThreeFamilyOutcomeSummaryV2:
    audit = audit_historical_three_family_outcomes_v2(rows)
    return next(
        item
        for item in audit.summaries
        if (item.horizon_bars, item.side, item.bucket) == (horizon, side, bucket)
    )


def test_audit_is_historical_nonpromoting_and_compares_three_of_three_to_two_of_three() -> None:
    bullish_tilt = _event_rows(
        "1",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        400_000,
        (100, -50, 200, -100, 0),
        contract_seed="b",
    )
    bullish_broad = _event_rows(
        "2",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        700_000,
        (300, 100, 400, 200, 50),
        contract_seed="a",
    )
    bearish_tilt = _event_rows(
        "3",
        DirectionalStateClassV2.BEARISH_STATE_TILT,
        -300_000,
        (75, 50, 25, 0, -25),
    )
    bearish_broad = _event_rows(
        "4",
        DirectionalStateClassV2.BROAD_BEARISH_STATE,
        -900_000,
        (-25, 50, 25, -50, -25),
    )

    audit = audit_historical_three_family_outcomes_v2(
        (*bullish_tilt, *bullish_broad, *bearish_tilt, *bearish_broad)
    )

    assert audit.audit_version == HISTORICAL_THREE_FAMILY_OUTCOME_AUDIT_VERSION_V2
    assert audit.consensus_rule_version == HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2
    assert audit.outcome_protocol_version == HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2
    assert audit.horizons_bars == (1, 3, 6, 12, 72)
    assert audit.execution_contract_sha256s == ("9" * 64, "a" * 64, "b" * 64)
    assert audit.event_count == 4
    assert audit.outcome_count == 20
    assert audit.historical_only
    assert not audit.probability
    assert not audit.probability_calibrated
    assert not audit.promoting
    assert not audit.inference_complete
    assert not audit.frozen_formula_efficacy_validated
    assert not audit.overlapping_event_drawdown_valid

    bullish = next(
        item
        for item in audit.contrasts
        if item.horizon_bars == 1 and item.side is HistoricalThreeFamilySideV2.BULLISH
    )
    bearish = next(
        item
        for item in audit.contrasts
        if item.horizon_bars == 1 and item.side is HistoricalThreeFamilySideV2.BEARISH
    )
    assert bullish.broad_minus_tilt_mean_net_return_micros == 200
    assert bullish.point_monotonic is True
    assert bearish.broad_minus_tilt_mean_net_return_micros == -100
    assert bearish.point_monotonic is False
    for contrast in audit.contrasts:
        assert contrast.historical_only
        assert not contrast.probability
        assert not contrast.probability_calibrated
        assert not contrast.promoting
        assert not contrast.inference_complete


def test_audit_output_order_is_stable_and_independent_of_input_order() -> None:
    first = _event_rows(
        "5",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        600_000,
        (1, 2, 3, 4, 5),
        contract_seed="f",
    )
    second = _event_rows(
        "6",
        DirectionalStateClassV2.BEARISH_STATE_TILT,
        -400_000,
        (-1, -2, -3, -4, -5),
        contract_seed="0",
    )

    forward = audit_historical_three_family_outcomes_v2((*first, *second))
    reverse = audit_historical_three_family_outcomes_v2(tuple(reversed((*first, *second))))

    assert forward == reverse
    assert tuple(
        (item.horizon_bars, item.side, item.bucket) for item in forward.summaries
    ) == tuple(
        (horizon, side, bucket)
        for horizon in (1, 3, 6, 12, 72)
        for side in (
            HistoricalThreeFamilySideV2.BULLISH,
            HistoricalThreeFamilySideV2.BEARISH,
        )
        for bucket in (
            HistoricalThreeFamilyAgreementBucketV2.TILT_2_OF_3,
            HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3,
        )
    )
    assert tuple((item.horizon_bars, item.side) for item in forward.contrasts) == tuple(
        (horizon, side)
        for horizon in (1, 3, 6, 12, 72)
        for side in (
            HistoricalThreeFamilySideV2.BULLISH,
            HistoricalThreeFamilySideV2.BEARISH,
        )
    )


def test_profit_factor_states_and_zero_is_a_strict_miss() -> None:
    finite_gain = _event_rows(
        "7",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        500_000,
        (100, 1, 1, 1, 1),
    )
    finite_loss = _event_rows(
        "8",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        500_000,
        (-25, 1, 1, 1, 1),
    )
    positive_only = _event_rows(
        "9",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        800_000,
        (75, 1, 1, 1, 1),
    )
    zeros = _event_rows(
        "a",
        DirectionalStateClassV2.BEARISH_STATE_TILT,
        -500_000,
        (0, 1, 1, 1, 1),
    )
    rows = (*finite_gain, *finite_loss, *positive_only, *zeros)

    finite = _summary(
        rows,
        horizon=1,
        side=HistoricalThreeFamilySideV2.BULLISH,
        bucket=HistoricalThreeFamilyAgreementBucketV2.TILT_2_OF_3,
    )
    positive = _summary(
        rows,
        horizon=1,
        side=HistoricalThreeFamilySideV2.BULLISH,
        bucket=HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3,
    )
    zero = _summary(
        rows,
        horizon=1,
        side=HistoricalThreeFamilySideV2.BEARISH,
        bucket=HistoricalThreeFamilyAgreementBucketV2.TILT_2_OF_3,
    )
    empty = _summary(
        rows,
        horizon=1,
        side=HistoricalThreeFamilySideV2.BEARISH,
        bucket=HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3,
    )

    assert finite.profit_factor_state is HistoricalThreeFamilyProfitFactorStateV2.FINITE
    assert finite.profit_factor_micros == 4_000_000
    assert finite.strict_after_cost_hits == 1
    assert finite.strict_after_cost_hit_rate_micros == 500_000
    assert positive.profit_factor_state is (
        HistoricalThreeFamilyProfitFactorStateV2.POSITIVE_WITH_NO_LOSSES
    )
    assert positive.profit_factor_micros is None
    assert zero.strict_after_cost_hits == 0
    assert zero.strict_after_cost_hit_rate_micros == 0
    assert zero.profit_factor_state is (
        HistoricalThreeFamilyProfitFactorStateV2.ZERO_GROSS_WITH_NO_LOSSES
    )
    assert empty.events == 0
    assert empty.evaluable == 0
    assert empty.coverage_micros == 0
    assert empty.profit_factor_state is (HistoricalThreeFamilyProfitFactorStateV2.NO_EVALUABLE_ROWS)


def test_unevaluable_is_not_zero_and_missing_cell_prevents_point_contrast() -> None:
    rows = _event_rows(
        "b",
        DirectionalStateClassV2.BROAD_BEARISH_STATE,
        -700_000,
        (None, 0, -25, 25, 0),
    )

    audit = audit_historical_three_family_outcomes_v2(rows)
    one_bar = next(
        item
        for item in audit.summaries
        if item.horizon_bars == 1
        and item.side is HistoricalThreeFamilySideV2.BEARISH
        and item.bucket is HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3
    )
    three_bar = next(
        item
        for item in audit.summaries
        if item.horizon_bars == 3
        and item.side is HistoricalThreeFamilySideV2.BEARISH
        and item.bucket is HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3
    )
    contrast = next(
        item
        for item in audit.contrasts
        if item.horizon_bars == 3 and item.side is HistoricalThreeFamilySideV2.BEARISH
    )

    assert one_bar.events == 1
    assert one_bar.evaluable == 0
    assert one_bar.mean_net_return_micros is None
    assert three_bar.strict_after_cost_hits == 0
    assert three_bar.strict_after_cost_hit_rate_micros == 0
    assert contrast.broad_minus_tilt_mean_net_return_micros is None
    assert contrast.point_monotonic is None


def test_audit_rejects_empty_wrong_type_partial_duplicate_and_identity_drift() -> None:
    rows = _event_rows(
        "c",
        DirectionalStateClassV2.BROAD_BEARISH_STATE,
        -800_000,
        (1, 2, 3, 4, 5),
    )

    with pytest.raises(HistoricalThreeFamilyOutcomeAuditErrorV2, match="at least"):
        audit_historical_three_family_outcomes_v2(())
    with pytest.raises(HistoricalThreeFamilyOutcomeAuditErrorV2, match="rows only"):
        audit_historical_three_family_outcomes_v2((*rows, object()))  # type: ignore[arg-type]
    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="every frozen horizon",
    ):
        audit_historical_three_family_outcomes_v2(rows[:-1])
    with pytest.raises(HistoricalThreeFamilyOutcomeAuditErrorV2, match="duplicate"):
        audit_historical_three_family_outcomes_v2((*rows, rows[0]))
    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="changes identity",
    ):
        audit_historical_three_family_outcomes_v2((*rows[:-1], replace(rows[-1], symbol="ETHUSDT")))


def test_audit_rejects_two_ids_for_one_deterministic_consensus_identity() -> None:
    first = _event_rows(
        "d",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        600_000,
        (1, 2, 3, 4, 5),
    )
    second = tuple(replace(item, event_id="e" * 64) for item in first)

    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="same deterministic consensus identity",
    ):
        audit_historical_three_family_outcomes_v2((*first, *second))


def test_outcome_hard_rejects_live_or_other_rule_version() -> None:
    row = _event_rows(
        "e",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        700_000,
        (1, 2, 3, 4, 5),
    )[0]

    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="historical three-family consensus rule",
    ):
        replace(
            row,
            rule_version=("R4B_CAUSAL_V2.4.0_DIRECTIONAL_EVIDENCE_PANEL_V1_SHADOW_NONPROMOTING"),
        )


@pytest.mark.parametrize(
    ("state_class", "agreement"),
    [
        (DirectionalStateClassV2.BROAD_BULLISH_STATE, 0),
        (DirectionalStateClassV2.BULLISH_STATE_TILT, -1),
        (DirectionalStateClassV2.BROAD_BEARISH_STATE, 1),
        (DirectionalStateClassV2.BEARISH_STATE_TILT, 0),
        (DirectionalStateClassV2.MIXED_OR_NEUTRAL_STATE, 10),
        (DirectionalStateClassV2.WITHHELD, -10),
    ],
)
def test_outcome_rejects_mixed_withheld_or_sign_inconsistent_state(
    state_class: DirectionalStateClassV2,
    agreement: int,
) -> None:
    with pytest.raises(HistoricalThreeFamilyOutcomeAuditErrorV2):
        _event_rows("f", state_class, agreement, (1, 2, 3, 4, 5))


@pytest.mark.parametrize("agreement", [-1_000_001, 1_000_001, True])
def test_outcome_rejects_out_of_range_or_noninteger_agreement(
    agreement: int,
) -> None:
    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="directional_agreement_micros",
    ):
        _event_rows(
            "f",
            DirectionalStateClassV2.BROAD_BULLISH_STATE,
            agreement,
            (1, 2, 3, 4, 5),
        )


def test_outcome_accepts_signed_agreement_boundaries() -> None:
    bullish = _event_rows(
        "1",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        1_000_000,
        (1, 2, 3, 4, 5),
    )[0]
    bearish = _event_rows(
        "2",
        DirectionalStateClassV2.BROAD_BEARISH_STATE,
        -1_000_000,
        (1, 2, 3, 4, 5),
    )[0]

    assert bullish.side is HistoricalThreeFamilySideV2.BULLISH
    assert bullish.bucket is HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3
    assert bearish.side is HistoricalThreeFamilySideV2.BEARISH
    assert bearish.bucket is HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3
    assert bullish.historical_only and bearish.historical_only
    assert not bullish.probability and not bearish.promoting


@pytest.mark.parametrize(
    "reason",
    ["", "missing_forward_bar", "MISSING FORWARD BAR", " MISSING_FORWARD_BAR", "X" * 129],
)
def test_unevaluable_outcome_rejects_malformed_exclusion_reason(reason: str) -> None:
    row = _event_rows(
        "3",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        300_000,
        (1, 2, 3, 4, 5),
    )[0]

    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="normalized exclusion reason",
    ):
        replace(
            row,
            evaluable=False,
            exclusion_reason=reason,
            net_return_micros=None,
        )


def test_evaluation_and_exclusion_fields_are_exactly_paired() -> None:
    row = _event_rows(
        "4",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        300_000,
        (1, 2, 3, 4, 5),
    )[0]

    with pytest.raises(HistoricalThreeFamilyOutcomeAuditErrorV2, match="cannot carry"):
        replace(row, exclusion_reason="SHOULD_NOT_EXIST")
    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="normalized exclusion reason",
    ):
        replace(
            row,
            evaluable=False,
            exclusion_reason="DATA_GAP",
            net_return_micros=0,
        )
    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="integer after-cost return",
    ):
        replace(row, net_return_micros=None)


@pytest.mark.parametrize("horizon", [0, 2, 73, True])
def test_outcome_rejects_nonfrozen_or_boolean_horizon(horizon: int) -> None:
    row = _event_rows(
        "5",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        500_000,
        (1, 2, 3, 4, 5),
    )[0]

    with pytest.raises(HistoricalThreeFamilyOutcomeAuditErrorV2, match="horizon"):
        replace(row, horizon_bars=horizon)


def test_outcome_rejects_bad_identity_contract_protocol_and_venue() -> None:
    row = _event_rows(
        "6",
        DirectionalStateClassV2.BROAD_BULLISH_STATE,
        500_000,
        (1, 2, 3, 4, 5),
    )[0]

    with pytest.raises(HistoricalThreeFamilyOutcomeAuditErrorV2, match="event_id"):
        replace(row, event_id="A" * 64)
    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="execution_contract",
    ):
        replace(row, execution_contract_sha256="g" * 64)
    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="outcome_protocol",
    ):
        replace(row, outcome_protocol_version="WRONG")
    with pytest.raises(
        HistoricalThreeFamilyOutcomeAuditErrorV2,
        match="USD-M futures",
    ):
        replace(row, venue=VenueV2.SPOT)
    with pytest.raises(HistoricalThreeFamilyOutcomeAuditErrorV2, match="symbol"):
        replace(row, symbol="btc-usdt")
    with pytest.raises(HistoricalThreeFamilyOutcomeAuditErrorV2, match="decision_time"):
        replace(row, decision_time_ms=-1)


def test_summary_uses_deterministic_half_away_integer_mean_and_median() -> None:
    first = _event_rows(
        "7",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        300_000,
        (-2, 1, 1, 1, 1),
    )
    second = _event_rows(
        "8",
        DirectionalStateClassV2.BULLISH_STATE_TILT,
        300_000,
        (-1, 1, 1, 1, 1),
    )

    summary = _summary(
        (*first, *second),
        horizon=1,
        side=HistoricalThreeFamilySideV2.BULLISH,
        bucket=HistoricalThreeFamilyAgreementBucketV2.TILT_2_OF_3,
    )

    assert summary.mean_net_return_micros == -2
    assert summary.median_net_return_micros == -2
    assert summary.historical_only
    assert not summary.probability
    assert not summary.probability_calibrated
    assert not summary.promoting
    assert not summary.inference_complete
