from __future__ import annotations

import hashlib
import random
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal, DecimalException, localcontext
from fractions import Fraction
from typing import Final, Literal

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.research.directional_agreement_audit import (
    DIRECTIONAL_AGREEMENT_AUDIT_VERSION_V2,
    DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2,
    DIRECTIONAL_AGREEMENT_OUTCOME_PROTOCOL_V2,
    DirectionalAgreementAuditErrorV2,
    DirectionalAgreementBucketV2,
    DirectionalAgreementOutcomeV2,
    DirectionalAgreementSideV2,
    audit_directional_agreement_outcomes_v2,
)
from signalbot.r4b_v2.strategy.directional_evidence import (
    DIRECTIONAL_EVIDENCE_RULE_VERSION_V2,
)

DIRECTIONAL_AGREEMENT_BOOTSTRAP_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.0_DIRECTIONAL_AGREEMENT_SHARED_UTC_MBB_V1_FROZEN"
)
DIRECTIONAL_AGREEMENT_BOOTSTRAP_BLOCK_DAYS_V2: Final = 7
DIRECTIONAL_AGREEMENT_BOOTSTRAP_MAX_SAMPLES_V2: Final = 100_000
DIRECTIONAL_AGREEMENT_BOOTSTRAP_MAX_CALENDAR_DAYS_V2: Final = 36_600

_DAY_MS: Final = 86_400_000
_JCS_SAFE_INTEGER_MAX: Final = 9_007_199_254_740_991
_DRAW_SCHEDULE_DOMAIN: Final = b"R4B_DIRECTIONAL_AGREEMENT_SHARED_UTC_MBB_V2\0"
_EXPOSURE_STATUS: Final = "EXPOSED_RETROSPECTIVE_ONLY"
_TWO_SIDED_LOWER_QUANTILE: Final = Fraction(1, 40)
_TWO_SIDED_UPPER_QUANTILE: Final = Fraction(39, 40)
_ONE_SIDED_UPPER_QUANTILE: Final = Fraction(19, 20)

type _CellKeyV2 = tuple[
    int,
    DirectionalAgreementSideV2,
    DirectionalAgreementBucketV2,
]
type _ContrastKeyV2 = tuple[int, DirectionalAgreementSideV2]

_CELL_KEYS: Final = tuple(
    (horizon, side, bucket)
    for horizon in DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2
    for side in DirectionalAgreementSideV2
    for bucket in DirectionalAgreementBucketV2
)
_CONTRAST_KEYS: Final = tuple(
    (horizon, side)
    for horizon in DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2
    for side in DirectionalAgreementSideV2
)


@dataclass(frozen=True, slots=True)
class DirectionalAgreementBootstrapCellV2:
    horizon_bars: int
    side: DirectionalAgreementSideV2
    bucket: DirectionalAgreementBucketV2
    events: int
    evaluable: int
    zero_alert_days: int
    valid_replicates: int
    invalid_replicates: int
    shared_draw_schedule_sha256: str
    inference_complete: Literal[False] = field(init=False, default=False)
    frozen_formula_efficacy_validated: Literal[False] = field(
        init=False,
        default=False,
    )
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class DirectionalAgreementBootstrapContrastV2:
    horizon_bars: int
    side: DirectionalAgreementSideV2
    broad_minus_tilt_mean_net_return_micros: int | None
    valid_replicates: int
    invalid_replicates: int
    two_sided_95_interval_micros: tuple[Decimal, Decimal] | None
    one_sided_basic_95_lower_micros: Decimal | None
    null_centered_one_sided_p_value: Decimal | None
    shared_draw_schedule_sha256: str
    inference_complete: Literal[False] = field(init=False, default=False)
    frozen_formula_efficacy_validated: Literal[False] = field(
        init=False,
        default=False,
    )
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class DirectionalAgreementBootstrapV2:
    bootstrap_version: str
    source_audit_version: str
    evidence_rule_version: str
    outcome_protocol_version: str
    execution_contract_sha256: str
    calendar_start_ms: int
    calendar_end_ms: int
    calendar_days: int
    block_days: int
    samples: int
    seed: int
    event_count: int
    outcome_count: int
    shared_draw_schedule_sha256: str
    cells: tuple[DirectionalAgreementBootstrapCellV2, ...]
    contrasts: tuple[DirectionalAgreementBootstrapContrastV2, ...]
    exposure_status: str = field(init=False, default=_EXPOSURE_STATUS)
    inference_complete: Literal[False] = field(init=False, default=False)
    frozen_formula_efficacy_validated: Literal[False] = field(
        init=False,
        default=False,
    )
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)


@dataclass(slots=True)
class _MutableDailyCellV2:
    events: list[int]
    evaluable: list[int]
    return_sums: list[int]


@dataclass(frozen=True, slots=True)
class _DailyCellV2:
    events: tuple[int, ...]
    evaluable: tuple[int, ...]
    return_sums: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _BlockAggregateV2:
    evaluable: tuple[int, ...]
    return_sums: tuple[int, ...]


def bootstrap_directional_agreement_contrasts_v2(
    rows: Sequence[DirectionalAgreementOutcomeV2],
    *,
    calendar_start_ms: int,
    calendar_end_ms: int,
    samples: int,
    seed: int,
) -> DirectionalAgreementBootstrapV2:
    """Apply one shared 7-day circular UTC-calendar draw to every audit cell."""

    snapshot = tuple(rows)
    day_count = _validate_bootstrap_request(
        calendar_start_ms=calendar_start_ms,
        calendar_end_ms=calendar_end_ms,
        samples=samples,
        seed=seed,
    )
    source_audit = audit_directional_agreement_outcomes_v2(snapshot)
    if len(source_audit.execution_contract_sha256s) != 1:
        raise DirectionalAgreementAuditErrorV2(
            "bootstrap comparison requires exactly one execution/cost contract hash"
        )
    daily = _build_daily_cells(
        snapshot,
        calendar_start_ms=calendar_start_ms,
        calendar_end_ms=calendar_end_ms,
        day_count=day_count,
    )
    block_lengths = _block_lengths(day_count)
    block_aggregates = {
        (key, length): _block_aggregate(daily[key], length)
        for key in _CELL_KEYS
        for length in set(block_lengths)
    }

    schedule_digest = hashlib.sha256(
        _DRAW_SCHEDULE_DOMAIN
        + canonical_json_line(
            {
                "block_days": DIRECTIONAL_AGREEMENT_BOOTSTRAP_BLOCK_DAYS_V2,
                "calendar_days": day_count,
                "calendar_end_ms": calendar_end_ms,
                "calendar_start_ms": calendar_start_ms,
                "samples": samples,
                "seed": seed,
            }
        )
    )
    rng = random.Random(seed)
    cell_valid = dict.fromkeys(_CELL_KEYS, 0)
    cell_invalid = dict.fromkeys(_CELL_KEYS, 0)
    contrast_estimates: dict[_ContrastKeyV2, list[Fraction]] = {
        key: [] for key in _CONTRAST_KEYS
    }
    contrast_invalid = dict.fromkeys(_CONTRAST_KEYS, 0)
    for _ in range(samples):
        starts = tuple(rng.randrange(day_count) for _ in block_lengths)
        for start in starts:
            schedule_digest.update(start.to_bytes(8, byteorder="little", signed=False))
        means: dict[_CellKeyV2, Fraction] = {}
        for key in _CELL_KEYS:
            total_count = 0
            total_return = 0
            for start, length in zip(starts, block_lengths, strict=True):
                aggregate = block_aggregates[(key, length)]
                total_count += aggregate.evaluable[start]
                total_return += aggregate.return_sums[start]
            if total_count == 0:
                cell_invalid[key] += 1
            else:
                cell_valid[key] += 1
                means[key] = Fraction(total_return, total_count)
        for contrast_key in _CONTRAST_KEYS:
            horizon, side = contrast_key
            tilt_key = (
                horizon,
                side,
                DirectionalAgreementBucketV2.TILT_2_OF_3,
            )
            broad_key = (
                horizon,
                side,
                DirectionalAgreementBucketV2.BROAD_3_OF_3,
            )
            tilt = means.get(tilt_key)
            broad = means.get(broad_key)
            if tilt is None or broad is None:
                contrast_invalid[contrast_key] += 1
            else:
                contrast_estimates[contrast_key].append(broad - tilt)

    schedule_sha256 = schedule_digest.hexdigest()
    cells = tuple(
        DirectionalAgreementBootstrapCellV2(
            horizon_bars=key[0],
            side=key[1],
            bucket=key[2],
            events=sum(daily[key].events),
            evaluable=sum(daily[key].evaluable),
            zero_alert_days=sum(value == 0 for value in daily[key].events),
            valid_replicates=cell_valid[key],
            invalid_replicates=cell_invalid[key],
            shared_draw_schedule_sha256=schedule_sha256,
        )
        for key in _CELL_KEYS
    )
    point_by_key = {
        (item.horizon_bars, item.side): (
            item.broad_minus_tilt_mean_net_return_micros
        )
        for item in source_audit.contrasts
    }
    contrasts = tuple(
        _contrast_result(
            key,
            point=point_by_key[key],
            estimates=contrast_estimates[key],
            invalid_replicates=contrast_invalid[key],
            schedule_sha256=schedule_sha256,
        )
        for key in _CONTRAST_KEYS
    )
    return DirectionalAgreementBootstrapV2(
        bootstrap_version=DIRECTIONAL_AGREEMENT_BOOTSTRAP_VERSION_V2,
        source_audit_version=DIRECTIONAL_AGREEMENT_AUDIT_VERSION_V2,
        evidence_rule_version=DIRECTIONAL_EVIDENCE_RULE_VERSION_V2,
        outcome_protocol_version=DIRECTIONAL_AGREEMENT_OUTCOME_PROTOCOL_V2,
        execution_contract_sha256=source_audit.execution_contract_sha256s[0],
        calendar_start_ms=calendar_start_ms,
        calendar_end_ms=calendar_end_ms,
        calendar_days=day_count,
        block_days=DIRECTIONAL_AGREEMENT_BOOTSTRAP_BLOCK_DAYS_V2,
        samples=samples,
        seed=seed,
        event_count=source_audit.event_count,
        outcome_count=source_audit.outcome_count,
        shared_draw_schedule_sha256=schedule_sha256,
        cells=cells,
        contrasts=contrasts,
    )


def _validate_bootstrap_request(
    *,
    calendar_start_ms: int,
    calendar_end_ms: int,
    samples: int,
    seed: int,
) -> int:
    if (
        type(calendar_start_ms) is not int
        or type(calendar_end_ms) is not int
        or calendar_start_ms < 0
        or calendar_end_ms <= calendar_start_ms
        or calendar_end_ms > _JCS_SAFE_INTEGER_MAX
        or calendar_start_ms % _DAY_MS != 0
        or calendar_end_ms % _DAY_MS != 0
    ):
        raise DirectionalAgreementAuditErrorV2(
            "bootstrap calendar must be a positive exclusive range of UTC midnights"
        )
    day_count = (calendar_end_ms - calendar_start_ms) // _DAY_MS
    if not (
        DIRECTIONAL_AGREEMENT_BOOTSTRAP_BLOCK_DAYS_V2
        <= day_count
        <= DIRECTIONAL_AGREEMENT_BOOTSTRAP_MAX_CALENDAR_DAYS_V2
    ):
        raise DirectionalAgreementAuditErrorV2(
            "bootstrap calendar must contain between 7 and 36,600 UTC days"
        )
    if (
        type(samples) is not int
        or not 1 <= samples <= DIRECTIONAL_AGREEMENT_BOOTSTRAP_MAX_SAMPLES_V2
    ):
        raise DirectionalAgreementAuditErrorV2(
            "bootstrap samples must be an explicit integer in [1, 100000]"
        )
    if type(seed) is not int or not 0 <= seed <= _JCS_SAFE_INTEGER_MAX:
        raise DirectionalAgreementAuditErrorV2(
            "bootstrap seed must be an explicit nonnegative JCS-safe integer"
        )
    return day_count


def _build_daily_cells(
    rows: tuple[DirectionalAgreementOutcomeV2, ...],
    *,
    calendar_start_ms: int,
    calendar_end_ms: int,
    day_count: int,
) -> dict[_CellKeyV2, _DailyCellV2]:
    mutable = {
        key: _MutableDailyCellV2(
            events=[0] * day_count,
            evaluable=[0] * day_count,
            return_sums=[0] * day_count,
        )
        for key in _CELL_KEYS
    }
    for row in rows:
        if not calendar_start_ms <= row.decision_time_ms < calendar_end_ms:
            raise DirectionalAgreementAuditErrorV2(
                "directional outcome lies outside the frozen UTC bootstrap calendar"
            )
        offset = (row.decision_time_ms - calendar_start_ms) // _DAY_MS
        key = (row.horizon_bars, row.side, row.bucket)
        cell = mutable[key]
        cell.events[offset] += 1
        if row.evaluable:
            assert row.net_return_micros is not None
            cell.evaluable[offset] += 1
            cell.return_sums[offset] += row.net_return_micros
    return {
        key: _DailyCellV2(
            events=tuple(value.events),
            evaluable=tuple(value.evaluable),
            return_sums=tuple(value.return_sums),
        )
        for key, value in mutable.items()
    }


def _block_lengths(day_count: int) -> tuple[int, ...]:
    full_blocks, remainder = divmod(
        day_count,
        DIRECTIONAL_AGREEMENT_BOOTSTRAP_BLOCK_DAYS_V2,
    )
    return (
        (DIRECTIONAL_AGREEMENT_BOOTSTRAP_BLOCK_DAYS_V2,) * full_blocks
        + ((remainder,) if remainder else ())
    )


def _block_aggregate(cell: _DailyCellV2, length: int) -> _BlockAggregateV2:
    return _BlockAggregateV2(
        evaluable=_rolling_circular_sum(cell.evaluable, length),
        return_sums=_rolling_circular_sum(cell.return_sums, length),
    )


def _rolling_circular_sum(values: tuple[int, ...], length: int) -> tuple[int, ...]:
    count = len(values)
    return tuple(
        sum(values[(start + offset) % count] for offset in range(length))
        for start in range(count)
    )


def _contrast_result(
    key: _ContrastKeyV2,
    *,
    point: int | None,
    estimates: list[Fraction],
    invalid_replicates: int,
    schedule_sha256: str,
) -> DirectionalAgreementBootstrapContrastV2:
    two_sided: tuple[Decimal, Decimal] | None = None
    lower: Decimal | None = None
    p_value: Decimal | None = None
    if point is not None and estimates:
        ordered = tuple(sorted(estimates))
        lower_quantile = _linear_quantile(ordered, _TWO_SIDED_LOWER_QUANTILE)
        upper_quantile = _linear_quantile(ordered, _TWO_SIDED_UPPER_QUANTILE)
        upper_one_sided = _linear_quantile(
            ordered,
            _ONE_SIDED_UPPER_QUANTILE,
        )
        two_sided = (
            _fraction_to_decimal(lower_quantile),
            _fraction_to_decimal(upper_quantile),
        )
        lower = _fraction_to_decimal(Fraction(2 * point) - upper_one_sided)
        if point <= 0:
            p_value = Decimal(1)
        else:
            exceedances = sum(
                value - Fraction(point) >= Fraction(point) for value in ordered
            )
            p_value = _fraction_to_decimal(
                Fraction(1 + exceedances, len(ordered) + 1)
            )
    return DirectionalAgreementBootstrapContrastV2(
        horizon_bars=key[0],
        side=key[1],
        broad_minus_tilt_mean_net_return_micros=point,
        valid_replicates=len(estimates),
        invalid_replicates=invalid_replicates,
        two_sided_95_interval_micros=two_sided,
        one_sided_basic_95_lower_micros=lower,
        null_centered_one_sided_p_value=p_value,
        shared_draw_schedule_sha256=schedule_sha256,
    )


def _linear_quantile(
    ordered: tuple[Fraction, ...],
    probability: Fraction,
) -> Fraction:
    if not ordered:
        raise DirectionalAgreementAuditErrorV2(
            "bootstrap quantile requires at least one valid replicate"
        )
    position = probability * (len(ordered) - 1)
    lower_index = position.numerator // position.denominator
    upper_index = lower_index if position.denominator == 1 else lower_index + 1
    if lower_index == upper_index:
        return ordered[lower_index]
    weight = position - lower_index
    return ordered[lower_index] * (1 - weight) + ordered[upper_index] * weight


def _fraction_to_decimal(value: Fraction) -> Decimal:
    try:
        with localcontext(protocol_decimal_context_v2()):
            return Decimal(value.numerator) / Decimal(value.denominator)
    except DecimalException as exc:
        raise DirectionalAgreementAuditErrorV2(
            "bootstrap inference cannot be represented under Decimal34"
        ) from exc
