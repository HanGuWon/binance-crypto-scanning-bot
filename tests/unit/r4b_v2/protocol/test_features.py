from __future__ import annotations

from decimal import ROUND_DOWN, Decimal, getcontext, localcontext, setcontext

import pytest
from hypothesis import given
from hypothesis import strategies as st

from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.protocol.features import (
    ROBUST_Z_MAD_SCALE_V2,
    ROBUST_Z_PRIOR_WINDOW_V2,
    ProtocolFeatureError,
    RobustZStatusV2,
    robust_z_v2,
)


def _balanced_prior() -> tuple[Decimal, ...]:
    return tuple(
        Decimal(index % 5)
        for index in range(ROBUST_Z_PRIOR_WINDOW_V2)
    )


def test_hand_calculated_robust_z_uses_prior_window_and_excludes_current() -> None:
    prior = _balanced_prior()
    result = robust_z_v2(prior, Decimal(4))

    assert result.status is RobustZStatusV2.READY
    assert result.ready
    assert result.location == Decimal(2)
    assert result.mad == Decimal(1)
    assert result.scale == ROBUST_Z_MAD_SCALE_V2
    with localcontext(protocol_decimal_context_v2()):
        expected = Decimal(2) / ROBUST_Z_MAD_SCALE_V2
    assert result.value == expected

    extreme_current = robust_z_v2(prior, Decimal("1000000"))
    assert extreme_current.location == result.location
    assert extreme_current.mad == result.mad
    assert extreme_current.scale == result.scale


def test_zero_mad_is_not_divided_by_epsilon_and_emits_no_value() -> None:
    result = robust_z_v2(
        tuple(Decimal("-3") for _ in range(ROBUST_Z_PRIOR_WINDOW_V2)),
        Decimal("-3"),
    )

    assert result.status is RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE
    assert not result.ready
    assert result.value is None
    assert result.scale is None


def test_warmup_requires_exactly_8640_prior_finite_observations() -> None:
    result = robust_z_v2(
        tuple(Decimal(index) for index in range(ROBUST_Z_PRIOR_WINDOW_V2 - 1)),
        Decimal(1),
    )
    assert result.status is RobustZStatusV2.FEATURE_NOT_READY_WARMUP
    assert result.prior_observation_count == ROBUST_Z_PRIOR_WINDOW_V2 - 1

    with pytest.raises(ProtocolFeatureError, match="exceeds"):
        robust_z_v2(
            tuple(Decimal(index) for index in range(ROBUST_Z_PRIOR_WINDOW_V2 + 1)),
            Decimal(1),
        )


@pytest.mark.parametrize(
    ("prior", "current"),
    [
        ((*_balanced_prior()[:-1], None), Decimal(1)),
        (_balanced_prior(), None),
        ((*_balanced_prior()[:-1], Decimal("NaN")), Decimal(1)),
        (_balanced_prior(), Decimal("Infinity")),
    ],
)
def test_missing_or_nonfinite_components_are_data_invalid(
    prior: tuple[Decimal | None, ...],
    current: Decimal | None,
) -> None:
    result = robust_z_v2(prior, current)
    assert result.status is RobustZStatusV2.DATA_INVALID_FEATURE
    assert not result.ready
    assert result.value is None


def test_negative_signed_feature_values_remain_valid() -> None:
    prior = tuple(
        Decimal((index % 5) - 2)
        for index in range(ROBUST_Z_PRIOR_WINDOW_V2)
    )
    result = robust_z_v2(prior, Decimal(-2))
    assert result.status is RobustZStatusV2.READY
    assert result.location == 0
    assert result.value is not None and result.value < 0


def test_mutable_or_boolean_inputs_do_not_cross_the_decimal_contract() -> None:
    with pytest.raises(ProtocolFeatureError, match="immutable tuple"):
        robust_z_v2(list(_balanced_prior()), Decimal(1))  # type: ignore[arg-type]
    result = robust_z_v2(_balanced_prior(), True)  # type: ignore[arg-type]
    assert result.status is RobustZStatusV2.DATA_INVALID_FEATURE


@given(
    translation=st.integers(min_value=-1_000, max_value=1_000),
    positive_scale=st.integers(min_value=1, max_value=100),
)
def test_property_translation_and_positive_scale_equivariance(
    translation: int,
    positive_scale: int,
) -> None:
    prior = _balanced_prior()
    current = Decimal(4)
    baseline = robust_z_v2(prior, current)
    shift = Decimal(translation)
    factor = Decimal(positive_scale)
    transformed = robust_z_v2(
        tuple((item + shift) * factor for item in prior),
        (current + shift) * factor,
    )

    assert baseline.status is RobustZStatusV2.READY
    assert transformed.status is RobustZStatusV2.READY
    assert transformed.value == baseline.value
    assert transformed.location == (baseline.location + shift) * factor  # type: ignore[operator]
    assert transformed.mad == baseline.mad * factor  # type: ignore[operator]
    assert transformed.scale == baseline.scale * factor  # type: ignore[operator]


def test_robust_z_uses_frozen_decimal_context_not_hostile_ambient_context() -> None:
    prior = _balanced_prior()
    original = getcontext().copy()
    try:
        getcontext().prec = 6
        getcontext().rounding = ROUND_DOWN
        hostile_result = robust_z_v2(prior, Decimal(4))
    finally:
        setcontext(original)

    baseline = robust_z_v2(prior, Decimal(4))
    assert hostile_result == baseline
    assert baseline.value == Decimal("1.348981518953190341292324295157156")
