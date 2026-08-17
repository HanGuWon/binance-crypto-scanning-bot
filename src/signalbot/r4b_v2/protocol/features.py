from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, localcontext
from enum import StrEnum
from typing import Final

from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2

ROBUST_Z_PRIOR_WINDOW_V2: Final = 8_640
ROBUST_Z_MAD_SCALE_V2: Final = Decimal("1.4826")


class ProtocolFeatureError(ValueError):
    """Raised when a caller violates a frozen V2 feature contract."""


class RobustZStatusV2(StrEnum):
    READY = "READY"
    FEATURE_NOT_READY_WARMUP = "FEATURE_NOT_READY_WARMUP"
    FEATURE_NOT_READY_ZERO_SCALE = "FEATURE_NOT_READY_ZERO_SCALE"
    DATA_INVALID_FEATURE = "DATA_INVALID_FEATURE"


@dataclass(frozen=True, slots=True)
class RobustZResultV2:
    """Auditable robust-z result whose location window never includes current."""

    status: RobustZStatusV2
    prior_observation_count: int
    value: Decimal | None = None
    location: Decimal | None = None
    mad: Decimal | None = None
    scale: Decimal | None = None

    def __post_init__(self) -> None:
        if type(self.prior_observation_count) is not int or self.prior_observation_count < 0:
            raise ProtocolFeatureError(
                "prior_observation_count must be a nonnegative integer"
            )
        outputs = (self.value, self.location, self.mad, self.scale)
        if self.status is RobustZStatusV2.READY:
            if any(item is None or not item.is_finite() for item in outputs):
                raise ProtocolFeatureError("READY robust-z requires four finite outputs")
            assert self.mad is not None
            assert self.scale is not None
            if self.mad <= 0 or self.scale <= 0:
                raise ProtocolFeatureError("READY robust-z requires positive MAD and scale")
            if self.prior_observation_count != ROBUST_Z_PRIOR_WINDOW_V2:
                raise ProtocolFeatureError("READY robust-z requires the exact prior window")
        elif any(item is not None for item in outputs):
            raise ProtocolFeatureError("non-ready robust-z statuses cannot expose values")

    @property
    def ready(self) -> bool:
        return self.status is RobustZStatusV2.READY


def robust_z_v2(
    prior_observations: tuple[Decimal | None, ...],
    current: Decimal | None,
) -> RobustZResultV2:
    """Compute the frozen V2 robust z-score from exactly 8,640 prior values.

    Missing or non-finite values fail closed. A shorter valid window is warmup;
    an oversized window is a caller contract error. Negative finite feature
    values are valid because returns, basis, funding, and flow may be signed.
    No epsilon or scale floor is used.
    """

    if type(prior_observations) is not tuple:
        raise ProtocolFeatureError("prior_observations must be an immutable tuple")
    count = len(prior_observations)
    if count > ROBUST_Z_PRIOR_WINDOW_V2:
        raise ProtocolFeatureError("prior_observations exceeds the exact 8,640-value window")
    if not _is_finite_decimal(current) or any(
        not _is_finite_decimal(item) for item in prior_observations
    ):
        return RobustZResultV2(
            status=RobustZStatusV2.DATA_INVALID_FEATURE,
            prior_observation_count=count,
        )
    if count < ROBUST_Z_PRIOR_WINDOW_V2:
        return RobustZResultV2(
            status=RobustZStatusV2.FEATURE_NOT_READY_WARMUP,
            prior_observation_count=count,
        )

    observations = tuple(item for item in prior_observations if item is not None)
    assert len(observations) == ROBUST_Z_PRIOR_WINDOW_V2
    assert current is not None
    with localcontext(protocol_decimal_context_v2()):
        location = _median_decimal(observations)
        mad = _median_decimal(tuple(abs(item - location) for item in observations))
        if mad == 0:
            return RobustZResultV2(
                status=RobustZStatusV2.FEATURE_NOT_READY_ZERO_SCALE,
                prior_observation_count=count,
            )
        if mad < 0 or not mad.is_finite():
            return RobustZResultV2(
                status=RobustZStatusV2.DATA_INVALID_FEATURE,
                prior_observation_count=count,
            )
        scale = ROBUST_Z_MAD_SCALE_V2 * mad
        value = (current - location) / scale
    return RobustZResultV2(
        status=RobustZStatusV2.READY,
        prior_observation_count=count,
        value=value,
        location=location,
        mad=mad,
        scale=scale,
    )


def _median_decimal(values: tuple[Decimal, ...]) -> Decimal:
    if not values:
        raise ProtocolFeatureError("median requires at least one value")
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / Decimal(2)


def _is_finite_decimal(value: Decimal | None) -> bool:
    return isinstance(value, Decimal) and value.is_finite()
