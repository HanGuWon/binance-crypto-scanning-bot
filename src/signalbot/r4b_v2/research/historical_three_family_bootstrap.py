"""Shared-calendar bootstrap for the historical three-family experiment.

The module consumes already matched, frozen fixed-horizon outcomes.  It never
selects events, changes a consensus decision, reads a file, or places an order.
All intervals are descriptive because the source calendars are already
exposed.  The optional conflicted-majority population has a separate protocol
and is never pooled with clean ``2 + neutral`` observations.
"""

from __future__ import annotations

import hashlib
import math
import random
import re
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from fractions import Fraction
from typing import Final, Literal, Protocol, cast

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.research.historical_three_family_outcome_audit import (
    HISTORICAL_THREE_FAMILY_OUTCOME_AUDIT_VERSION_V2,
    HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
    HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
    HistoricalThreeFamilyAgreementBucketV2,
    HistoricalThreeFamilyOutcomeAuditErrorV2,
    HistoricalThreeFamilyOutcomeV2,
    HistoricalThreeFamilyProfitFactorStateV2,
    HistoricalThreeFamilySideV2,
    audit_historical_three_family_outcomes_v2,
)
from signalbot.r4b_v2.research.historical_three_family_topology import (
    HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
)

HISTORICAL_THREE_FAMILY_BOOTSTRAP_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.1_HISTORICAL_THREE_FAMILY_SHARED_UTC_MBB_V1_FROZEN"
)
HISTORICAL_THREE_FAMILY_BOOTSTRAP_BLOCK_DAYS_V2: Final = 7
HISTORICAL_THREE_FAMILY_BOOTSTRAP_SAMPLES_V2: Final = 10_000
HISTORICAL_THREE_FAMILY_BOOTSTRAP_SEED_V2: Final = 20_260_720
HISTORICAL_THREE_FAMILY_BOOTSTRAP_MIN_EVALUABLE_V2: Final = 30
HISTORICAL_THREE_FAMILY_FULL_CALENDAR_START_MS_V2: Final = 1_719_792_000_000
HISTORICAL_THREE_FAMILY_FULL_CALENDAR_END_MS_V2: Final = 1_782_864_000_000
HISTORICAL_THREE_FAMILY_FULL_CALENDAR_SCHEDULE_SHA256_V2: Final = (
    "0e767578284742d595c7f79750446752233d4c3ee563e763b150d6e7e886bac7"
)
HISTORICAL_THREE_FAMILY_CONFLICTED_OUTCOME_PROTOCOL_V2: Final = (
    "R4B_CAUSAL_V2_HISTORICAL_THREE_FAMILY_CONFLICTED_2_VS_1_"
    "AFTER_COST_FIXED_HORIZON_OUTCOME_V1_FROZEN"
)

_DAY_MS: Final = 86_400_000
_MAX_CALENDAR_DAYS: Final = 36_600
_JCS_SAFE_INTEGER_MAX: Final = 9_007_199_254_740_991
_MICROS: Final = 1_000_000
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]{2,30}$")
_EXCLUSION_RE: Final = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
_SCHEDULE_DOMAIN: Final = b"R4B_HISTORICAL_THREE_FAMILY_SHARED_UTC_MBB_V2\0"
_RESULT_DOMAIN: Final = b"R4B_HISTORICAL_THREE_FAMILY_BOOTSTRAP_RESULT_V2\0"
_PRIMARY_ROWS_DOMAIN: Final = b"R4B_HISTORICAL_THREE_FAMILY_PRIMARY_ROWS_V2\0"
_CONFLICTED_ROWS_DOMAIN: Final = b"R4B_HISTORICAL_THREE_FAMILY_CONFLICTED_ROWS_V2\0"
_COST_ROWS_DOMAIN: Final = b"R4B_HISTORICAL_THREE_FAMILY_COST_ROWS_V2\0"


class HistoricalThreeFamilyBootstrapErrorV2(ValueError):
    """Raised when the frozen bootstrap or one of its inputs is invalid."""


class HistoricalThreeFamilyBootstrapBucketV2(StrEnum):
    CLEAN_2_PLUS_NEUTRAL = "CLEAN_2_PLUS_NEUTRAL"
    BROAD_3_OF_3 = "BROAD_3_OF_3"
    CONFLICTED_2_VS_1 = "CONFLICTED_2_VS_1"


class HistoricalThreeFamilyBootstrapMetricV2(StrEnum):
    MEAN_NET_RETURN_MICROS = "MEAN_NET_RETURN_MICROS"
    STRICT_AFTER_COST_HIT_RATE_MICROS = "STRICT_AFTER_COST_HIT_RATE_MICROS"
    PROFIT_FACTOR_MICROS = "PROFIT_FACTOR_MICROS"


class HistoricalThreeFamilyBootstrapFeasibilityV2(StrEnum):
    INCONCLUSIVE_EMPTY = "INCONCLUSIVE_EMPTY"
    INCONCLUSIVE_SPARSE = "INCONCLUSIVE_SPARSE"
    DESCRIPTIVE_EXPOSED_ONLY = "DESCRIPTIVE_EXPOSED_ONLY"


class HistoricalThreeFamilyBootstrapComparisonV2(StrEnum):
    BROAD_MINUS_CLEAN_2_PLUS_NEUTRAL = "BROAD_MINUS_CLEAN_2_PLUS_NEUTRAL"
    BROAD_MINUS_CONFLICTED_2_VS_1 = "BROAD_MINUS_CONFLICTED_2_VS_1"


class HistoricalThreeFamilyCostSourceV2(StrEnum):
    PRIMARY_CLEAN = "PRIMARY_CLEAN"
    CONFLICTED_COMPARATOR = "CONFLICTED_COMPARATOR"


@dataclass(frozen=True, slots=True)
class HistoricalExactRationalV2:
    """Canonical exact rational represented with decimal strings."""

    numerator: str
    denominator: str

    def __post_init__(self) -> None:
        try:
            numerator = int(self.numerator)
            denominator = int(self.denominator)
        except (TypeError, ValueError) as exc:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "exact rational components must be canonical decimal integers"
            ) from exc
        if str(numerator) != self.numerator or str(denominator) != self.denominator:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "exact rational components must be canonical decimal integers"
            )
        if denominator <= 0 or math.gcd(numerator, denominator) != 1:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "exact rational must have a positive reduced denominator"
            )

    def as_fraction(self) -> Fraction:
        """Return the exact value for local formatting or audit checks."""

        return Fraction(int(self.numerator), int(self.denominator))


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyConflictedOutcomeV2:
    """Separately versioned outcome for a supporting conflicted 2-vs-1 event."""

    event_id: str
    comparator_protocol_version: str
    topology_rule_version: str
    execution_contract_sha256: str
    symbol: str
    decision_time_ms: int
    side: HistoricalThreeFamilySideV2
    directional_agreement_micros: int
    horizon_bars: int
    evaluable: bool
    exclusion_reason: str
    net_return_micros: int | None
    historical_only: Literal[True] = field(init=False, default=True)
    conflicted_comparator: Literal[True] = field(init=False, default=True)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    inference_complete: Literal[False] = field(init=False, default=False)

    def __post_init__(self) -> None:
        _require_sha256(self.event_id, "conflicted event_id")
        if (
            self.comparator_protocol_version
            != HISTORICAL_THREE_FAMILY_CONFLICTED_OUTCOME_PROTOCOL_V2
        ):
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "conflicted outcome must bind its separate frozen protocol"
            )
        if self.topology_rule_version != HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "conflicted outcome must bind the frozen topology rule"
            )
        _require_sha256(self.execution_contract_sha256, "conflicted execution contract")
        if not isinstance(self.symbol, str) or _SYMBOL_RE.fullmatch(self.symbol) is None:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "conflicted symbol must be uppercase and normalized"
            )
        if type(self.decision_time_ms) is not int or not (
            0 <= self.decision_time_ms <= _JCS_SAFE_INTEGER_MAX
        ):
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "conflicted decision_time_ms must be a nonnegative JCS-safe integer"
            )
        if not isinstance(self.side, HistoricalThreeFamilySideV2):
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "conflicted side must be an exact historical side"
            )
        if type(self.directional_agreement_micros) is not int or not (
            -_MICROS <= self.directional_agreement_micros <= _MICROS
        ):
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "conflicted directional agreement must be in [-1e6, 1e6]"
            )
        # Side is the frozen sign-count majority.  The weighted agreement is
        # descriptive provenance and may be opposite-signed or exactly zero
        # when the opposing leaf has greater magnitude.
        if type(self.horizon_bars) is not int or (
            self.horizon_bars not in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
        ):
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "conflicted horizon is outside the frozen horizon set"
            )
        _validate_outcome_economics(
            evaluable=self.evaluable,
            exclusion_reason=self.exclusion_reason,
            net_return_micros=self.net_return_micros,
            label="conflicted outcome",
        )


class HistoricalFixedHorizonCostRowLikeV2(Protocol):
    """Structural subset implemented by fixed-horizon output rows."""

    @property
    def event_id(self) -> str: ...

    @property
    def execution_contract_sha256(self) -> str: ...

    @property
    def agreement_bucket(self) -> str: ...

    @property
    def primary_direction(self) -> str: ...

    @property
    def horizon_bars(self) -> int: ...

    @property
    def evaluable(self) -> bool: ...

    @property
    def exclusion_reason(self) -> str: ...

    @property
    def gross_directional_return_micros(self) -> int | None: ...

    @property
    def slippage_return_micros(self) -> int | None: ...

    @property
    def fee_return_micros(self) -> int | None: ...

    @property
    def funding_return_micros(self) -> int | None: ...

    @property
    def rounding_residual_micros(self) -> int | None: ...

    @property
    def total_cost_micros(self) -> int | None: ...

    @property
    def net_return_micros(self) -> int | None: ...


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyCostAttributionV2:
    """Exact gross-to-net companion keyed to one fixed-horizon outcome."""

    source: HistoricalThreeFamilyCostSourceV2
    event_id: str
    execution_contract_sha256: str
    side: HistoricalThreeFamilySideV2
    bucket: HistoricalThreeFamilyBootstrapBucketV2
    horizon_bars: int
    evaluable: bool
    exclusion_reason: str
    gross_directional_return_micros: int | None
    slippage_return_micros: int | None
    fee_return_micros: int | None
    funding_return_micros: int | None
    rounding_residual_micros: int | None
    total_cost_micros: int | None
    net_return_micros: int | None

    def __post_init__(self) -> None:
        if not isinstance(self.source, HistoricalThreeFamilyCostSourceV2):
            raise HistoricalThreeFamilyBootstrapErrorV2("cost source is invalid")
        _require_sha256(self.event_id, "cost event_id")
        _require_sha256(self.execution_contract_sha256, "cost execution contract")
        if not isinstance(self.side, HistoricalThreeFamilySideV2):
            raise HistoricalThreeFamilyBootstrapErrorV2("cost side is invalid")
        if not isinstance(self.bucket, HistoricalThreeFamilyBootstrapBucketV2):
            raise HistoricalThreeFamilyBootstrapErrorV2("cost bucket is invalid")
        allowed = (
            {
                HistoricalThreeFamilyBootstrapBucketV2.CLEAN_2_PLUS_NEUTRAL,
                HistoricalThreeFamilyBootstrapBucketV2.BROAD_3_OF_3,
            }
            if self.source is HistoricalThreeFamilyCostSourceV2.PRIMARY_CLEAN
            else {HistoricalThreeFamilyBootstrapBucketV2.CONFLICTED_2_VS_1}
        )
        if self.bucket not in allowed:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "cost bucket cannot cross its primary/conflicted source boundary"
            )
        if type(self.horizon_bars) is not int or (
            self.horizon_bars not in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
        ):
            raise HistoricalThreeFamilyBootstrapErrorV2("cost horizon is not frozen")
        values = (
            self.gross_directional_return_micros,
            self.slippage_return_micros,
            self.fee_return_micros,
            self.funding_return_micros,
            self.rounding_residual_micros,
            self.total_cost_micros,
            self.net_return_micros,
        )
        if type(self.evaluable) is not bool or not isinstance(self.exclusion_reason, str):
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "cost evaluable and exclusion fields are malformed"
            )
        if self.evaluable:
            if self.exclusion_reason or any(type(value) is not int for value in values):
                raise HistoricalThreeFamilyBootstrapErrorV2(
                    "evaluable cost attribution requires every exact component"
                )
            integers = cast(tuple[int, ...], values)
            if any(abs(value) > _JCS_SAFE_INTEGER_MAX for value in integers):
                raise HistoricalThreeFamilyBootstrapErrorV2(
                    "cost component exceeds the JCS-safe integer range"
                )
            gross, slippage, fee, funding, residual, total_cost, net = integers
            if slippage < 0 or fee < 0 or total_cost < 0:
                raise HistoricalThreeFamilyBootstrapErrorV2(
                    "slippage, fee, and total cost must be nonnegative"
                )
            if total_cost != slippage + fee - residual:
                raise HistoricalThreeFamilyBootstrapErrorV2(
                    "cost attribution does not reconcile fee, slippage, and residual"
                )
            if gross - total_cost + funding != net:
                raise HistoricalThreeFamilyBootstrapErrorV2(
                    "cost attribution does not reconcile gross, funding, and net"
                )
            return
        if _EXCLUSION_RE.fullmatch(self.exclusion_reason) is None or any(
            value is not None for value in values
        ):
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "excluded cost attribution requires one reason and no economics"
            )


def cost_attribution_from_fixed_horizon_row_v2(
    row: HistoricalFixedHorizonCostRowLikeV2,
    *,
    source: HistoricalThreeFamilyCostSourceV2,
) -> HistoricalThreeFamilyCostAttributionV2:
    """Adapt the exact economic subset of a fixed-horizon output row."""

    if not isinstance(source, HistoricalThreeFamilyCostSourceV2):
        raise HistoricalThreeFamilyBootstrapErrorV2("cost adapter source is invalid")
    try:
        raw_bucket = row.agreement_bucket
        bucket = (
            HistoricalThreeFamilyBootstrapBucketV2.CLEAN_2_PLUS_NEUTRAL
            if raw_bucket == HistoricalThreeFamilyAgreementBucketV2.TILT_2_OF_3.value
            else HistoricalThreeFamilyBootstrapBucketV2(raw_bucket)
        )
        side = (
            HistoricalThreeFamilySideV2.BULLISH
            if row.primary_direction == "long"
            else HistoricalThreeFamilySideV2.BEARISH
            if row.primary_direction == "short"
            else None
        )
        if side is None:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "fixed-horizon row has no normalized long/short direction"
            )
        return HistoricalThreeFamilyCostAttributionV2(
            source=source,
            event_id=row.event_id,
            execution_contract_sha256=row.execution_contract_sha256,
            side=side,
            bucket=bucket,
            horizon_bars=row.horizon_bars,
            evaluable=row.evaluable,
            exclusion_reason=row.exclusion_reason,
            gross_directional_return_micros=row.gross_directional_return_micros,
            slippage_return_micros=row.slippage_return_micros,
            fee_return_micros=row.fee_return_micros,
            funding_return_micros=row.funding_return_micros,
            rounding_residual_micros=row.rounding_residual_micros,
            total_cost_micros=row.total_cost_micros,
            net_return_micros=row.net_return_micros,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "fixed-horizon row is not compatible with the cost adapter"
        ) from exc


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyBootstrapEndpointV2:
    metric: HistoricalThreeFamilyBootstrapMetricV2
    point_estimate: HistoricalExactRationalV2 | None
    valid_replicates: int
    invalid_replicates: int
    two_sided_percentile_95_interval: (
        tuple[HistoricalExactRationalV2, HistoricalExactRationalV2] | None
    )
    one_sided_basic_95_lower: HistoricalExactRationalV2 | None
    null_centered_one_sided_p_value: HistoricalExactRationalV2 | None
    shared_draw_schedule_sha256: str
    historical_only: Literal[True] = field(init=False, default=True)
    inference_complete: Literal[False] = field(init=False, default=False)
    multiplicity_adjusted: Literal[False] = field(init=False, default=False)
    efficacy_validated: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyCostSummaryV2:
    events: int
    evaluable: int
    coverage_micros: int
    gross_directional_strict_hits: int
    gross_directional_strict_hit_rate_micros: int | None
    net_strict_hits: int
    net_strict_hit_rate_micros: int | None
    gross_to_net_hit_loss_count: int
    gross_to_net_hit_loss_rate_micros: int | None
    net_positive_without_gross_positive_count: int
    mean_gross_directional_return_micros: int | None
    mean_slippage_return_micros: int | None
    mean_fee_return_micros: int | None
    mean_funding_return_micros: int | None
    mean_total_cost_micros: int | None
    mean_net_return_micros: int | None
    gross_to_net_mean_change_micros: int | None


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyBootstrapCellV2:
    horizon_bars: int
    side: HistoricalThreeFamilySideV2
    bucket: HistoricalThreeFamilyBootstrapBucketV2
    events: int
    evaluable: int
    zero_alert_days: int
    feasibility: HistoricalThreeFamilyBootstrapFeasibilityV2
    profit_factor_state: HistoricalThreeFamilyProfitFactorStateV2
    endpoints: tuple[HistoricalThreeFamilyBootstrapEndpointV2, ...]
    cost_attribution: HistoricalThreeFamilyCostSummaryV2 | None
    shared_draw_schedule_sha256: str
    historical_only: Literal[True] = field(init=False, default=True)
    inference_complete: Literal[False] = field(init=False, default=False)
    efficacy_validated: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyBootstrapContrastV2:
    comparison: HistoricalThreeFamilyBootstrapComparisonV2
    comparator_protocol_version: str
    horizon_bars: int
    side: HistoricalThreeFamilySideV2
    broad_bucket: Literal[HistoricalThreeFamilyBootstrapBucketV2.BROAD_3_OF_3]
    comparator_bucket: HistoricalThreeFamilyBootstrapBucketV2
    broad_evaluable: int
    comparator_evaluable: int
    feasibility: HistoricalThreeFamilyBootstrapFeasibilityV2
    comparison_interpretable: Literal[False]
    endpoints: tuple[HistoricalThreeFamilyBootstrapEndpointV2, ...]
    shared_draw_schedule_sha256: str
    historical_only: Literal[True] = field(init=False, default=True)
    exposed_retrospective_only: Literal[True] = field(init=False, default=True)
    inference_complete: Literal[False] = field(init=False, default=False)
    multiplicity_adjusted: Literal[False] = field(init=False, default=False)
    efficacy_validated: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyBootstrapScheduleV2:
    """Outcome-blind full-calendar draw authority for one frozen run."""

    calendar_start_ms: int
    calendar_end_ms: int
    calendar_days: int
    block_lengths: tuple[int, ...]
    starts: tuple[tuple[int, ...], ...] = field(repr=False)
    schedule_sha256: str
    block_days: int = field(
        init=False,
        default=HISTORICAL_THREE_FAMILY_BOOTSTRAP_BLOCK_DAYS_V2,
    )
    samples: int = field(
        init=False,
        default=HISTORICAL_THREE_FAMILY_BOOTSTRAP_SAMPLES_V2,
    )
    seed: int = field(
        init=False,
        default=HISTORICAL_THREE_FAMILY_BOOTSTRAP_SEED_V2,
    )
    circular: Literal[True] = field(init=False, default=True)
    zero_alert_days_retained: Literal[True] = field(init=False, default=True)
    shared_across_all_horizons_sides_buckets_metrics: Literal[True] = field(
        init=False,
        default=True,
    )
    outcome_data_used: Literal[False] = field(init=False, default=False)

    def artifact(self) -> dict[str, object]:
        """Return compact authority metadata without embedding one million starts."""

        return {
            "block_days": self.block_days,
            "block_lengths": list(self.block_lengths),
            "calendar_days": self.calendar_days,
            "calendar_end_ms_exclusive": self.calendar_end_ms,
            "calendar_start_ms_inclusive": self.calendar_start_ms,
            "circular": self.circular,
            "outcome_data_used": self.outcome_data_used,
            "samples": self.samples,
            "schedule_sha256": self.schedule_sha256,
            "seed": self.seed,
            "shared_across_all_horizons_sides_buckets_metrics": (
                self.shared_across_all_horizons_sides_buckets_metrics
            ),
            "zero_alert_days_retained": self.zero_alert_days_retained,
        }


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyBootstrapV2:
    bootstrap_version: str
    source_audit_version: str
    consensus_rule_version: str
    primary_outcome_protocol_version: str
    conflicted_outcome_protocol_version: str | None
    topology_rule_version: str
    execution_contract_sha256: str
    calendar_start_ms: int
    calendar_end_ms: int
    calendar_days: int
    block_days: int
    samples: int
    seed: int
    minimum_evaluable_per_cell: int
    primary_event_count: int
    primary_outcome_count: int
    conflicted_event_count: int
    conflicted_outcome_count: int
    primary_rows_sha256: str
    conflicted_rows_sha256: str | None
    cost_attribution_rows_sha256: str | None
    cost_attribution_complete: bool
    shared_draw_schedule_sha256: str
    cells: tuple[HistoricalThreeFamilyBootstrapCellV2, ...]
    contrasts: tuple[HistoricalThreeFamilyBootstrapContrastV2, ...]
    artifact_sha256: str = field(init=False, default="")
    historical_only: Literal[True] = field(init=False, default=True)
    exposed_retrospective_only: Literal[True] = field(init=False, default=True)
    zero_alert_days_retained: Literal[True] = field(init=False, default=True)
    shared_draws_across_all_cells: Literal[True] = field(init=False, default=True)
    conflicted_pooled_with_clean: Literal[False] = field(init=False, default=False)
    overlapping_event_drawdown_valid: Literal[False] = field(init=False, default=False)
    inference_complete: Literal[False] = field(init=False, default=False)
    multiplicity_adjusted: Literal[False] = field(init=False, default=False)
    efficacy_validated: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    order_placement: Literal[False] = field(init=False, default=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_sha256",
            hashlib.sha256(
                _RESULT_DOMAIN + canonical_json_line(_result_document(self, include_hash=False))
            ).hexdigest(),
        )


@dataclass(slots=True)
class _MutableDailyCellV2:
    events: list[int]
    evaluable: list[int]
    net_sum: list[int]
    hits: list[int]
    gains: list[int]
    losses: list[int]


@dataclass(frozen=True, slots=True)
class _DailyCellV2:
    events: tuple[int, ...]
    evaluable: tuple[int, ...]
    net_sum: tuple[int, ...]
    hits: tuple[int, ...]
    gains: tuple[int, ...]
    losses: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _BlockAggregateV2:
    evaluable: tuple[int, ...]
    net_sum: tuple[int, ...]
    hits: tuple[int, ...]
    gains: tuple[int, ...]
    losses: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _AnalysisRowV2:
    source: HistoricalThreeFamilyCostSourceV2
    event_id: str
    execution_contract_sha256: str
    symbol: str
    decision_time_ms: int
    side: HistoricalThreeFamilySideV2
    bucket: HistoricalThreeFamilyBootstrapBucketV2
    horizon_bars: int
    evaluable: bool
    exclusion_reason: str
    net_return_micros: int | None


type _CellKeyV2 = tuple[
    int,
    HistoricalThreeFamilySideV2,
    HistoricalThreeFamilyBootstrapBucketV2,
]
type _MetricValuesV2 = tuple[Fraction | None, Fraction | None, Fraction | None]


def bootstrap_historical_three_family_outcomes_v2(
    primary_rows: Sequence[HistoricalThreeFamilyOutcomeV2],
    *,
    calendar_start_ms: int,
    calendar_end_ms: int,
    cost_attributions: Sequence[HistoricalThreeFamilyCostAttributionV2] | None = None,
    conflicted_rows: Sequence[HistoricalThreeFamilyConflictedOutcomeV2] = (),
) -> HistoricalThreeFamilyBootstrapV2:
    """Run the exact frozen shared-calendar descriptive bootstrap."""

    day_count = _validate_calendar(calendar_start_ms, calendar_end_ms)
    primary_snapshot = tuple(primary_rows)
    conflicted_snapshot = tuple(conflicted_rows)
    try:
        source_audit = audit_historical_three_family_outcomes_v2(primary_snapshot)
    except HistoricalThreeFamilyOutcomeAuditErrorV2 as exc:
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "primary outcomes fail their frozen structural audit"
        ) from exc
    if len(source_audit.execution_contract_sha256s) != 1:
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "bootstrap requires exactly one primary execution/cost contract"
        )
    execution_contract_sha256 = source_audit.execution_contract_sha256s[0]
    primary_analysis = tuple(_primary_analysis_row(row) for row in primary_snapshot)
    conflicted_analysis, conflicted_events = _validate_conflicted_rows(
        conflicted_snapshot,
        execution_contract_sha256=execution_contract_sha256,
    )
    _validate_disjoint_sources(primary_analysis, conflicted_analysis)
    analysis_rows = (*primary_analysis, *conflicted_analysis)
    _validate_rows_in_calendar(
        analysis_rows,
        calendar_start_ms=calendar_start_ms,
        calendar_end_ms=calendar_end_ms,
    )
    costs = None if cost_attributions is None else tuple(cost_attributions)
    cost_by_key = _validate_cost_attributions(analysis_rows, costs)
    schedule = build_historical_three_family_bootstrap_schedule_v2(
        calendar_start_ms=calendar_start_ms,
        calendar_end_ms=calendar_end_ms,
    )
    daily = _build_daily(
        analysis_rows,
        calendar_start_ms=calendar_start_ms,
        day_count=day_count,
    )
    active_keys = tuple(key for key in _all_cell_keys() if sum(daily[key].events) > 0)
    aggregates = {
        (key, length): _block_aggregate(daily[key], length)
        for key in active_keys
        for length in set(schedule.block_lengths)
    }
    cell_estimates: dict[
        _CellKeyV2,
        tuple[list[Fraction], list[Fraction], list[Fraction]],
    ] = {key: ([], [], []) for key in _all_cell_keys()}
    comparison_specs = _comparison_specs(bool(conflicted_snapshot))
    contrast_estimates: dict[
        tuple[HistoricalThreeFamilyBootstrapComparisonV2, int, HistoricalThreeFamilySideV2],
        tuple[list[Fraction], list[Fraction], list[Fraction]],
    ] = {
        (comparison, horizon, side): ([], [], [])
        for comparison, _ in comparison_specs
        for horizon in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
        for side in HistoricalThreeFamilySideV2
    }
    for starts in schedule.starts:
        replicate: dict[_CellKeyV2, _MetricValuesV2] = {}
        for key in active_keys:
            totals = [0, 0, 0, 0, 0]
            for start, length in zip(starts, schedule.block_lengths, strict=True):
                aggregate = aggregates[(key, length)]
                totals[0] += aggregate.evaluable[start]
                totals[1] += aggregate.net_sum[start]
                totals[2] += aggregate.hits[start]
                totals[3] += aggregate.gains[start]
                totals[4] += aggregate.losses[start]
            metrics = _metrics_from_totals(*totals)
            replicate[key] = metrics
            for index, value in enumerate(metrics):
                if value is not None:
                    cell_estimates[key][index].append(value)
        for comparison, comparator_bucket in comparison_specs:
            for horizon in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2:
                for side in HistoricalThreeFamilySideV2:
                    broad = replicate.get(
                        (horizon, side, HistoricalThreeFamilyBootstrapBucketV2.BROAD_3_OF_3),
                        (None, None, None),
                    )
                    comparator = replicate.get(
                        (horizon, side, comparator_bucket),
                        (None, None, None),
                    )
                    output = contrast_estimates[(comparison, horizon, side)]
                    for index, (broad_value, comparator_value) in enumerate(
                        zip(broad, comparator, strict=True)
                    ):
                        if broad_value is not None and comparator_value is not None:
                            output[index].append(broad_value - comparator_value)

    cells = tuple(
        _build_cell(
            key,
            daily=daily[key],
            estimates=cell_estimates[key],
            cost_by_key=cost_by_key,
            schedule_sha256=schedule.schedule_sha256,
        )
        for key in _all_cell_keys()
    )
    cell_by_key = {(cell.horizon_bars, cell.side, cell.bucket): cell for cell in cells}
    contrasts = tuple(
        _build_contrast(
            comparison=comparison,
            comparator_bucket=comparator_bucket,
            horizon=horizon,
            side=side,
            broad=cell_by_key[(horizon, side, HistoricalThreeFamilyBootstrapBucketV2.BROAD_3_OF_3)],
            comparator=cell_by_key[(horizon, side, comparator_bucket)],
            estimates=contrast_estimates[(comparison, horizon, side)],
            schedule_sha256=schedule.schedule_sha256,
        )
        for comparison, comparator_bucket in comparison_specs
        for horizon in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
        for side in HistoricalThreeFamilySideV2
    )
    result = HistoricalThreeFamilyBootstrapV2(
        bootstrap_version=HISTORICAL_THREE_FAMILY_BOOTSTRAP_VERSION_V2,
        source_audit_version=HISTORICAL_THREE_FAMILY_OUTCOME_AUDIT_VERSION_V2,
        consensus_rule_version=HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        primary_outcome_protocol_version=HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
        conflicted_outcome_protocol_version=(
            HISTORICAL_THREE_FAMILY_CONFLICTED_OUTCOME_PROTOCOL_V2 if conflicted_snapshot else None
        ),
        topology_rule_version=HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
        execution_contract_sha256=execution_contract_sha256,
        calendar_start_ms=calendar_start_ms,
        calendar_end_ms=calendar_end_ms,
        calendar_days=day_count,
        block_days=HISTORICAL_THREE_FAMILY_BOOTSTRAP_BLOCK_DAYS_V2,
        samples=HISTORICAL_THREE_FAMILY_BOOTSTRAP_SAMPLES_V2,
        seed=HISTORICAL_THREE_FAMILY_BOOTSTRAP_SEED_V2,
        minimum_evaluable_per_cell=HISTORICAL_THREE_FAMILY_BOOTSTRAP_MIN_EVALUABLE_V2,
        primary_event_count=source_audit.event_count,
        primary_outcome_count=source_audit.outcome_count,
        conflicted_event_count=conflicted_events,
        conflicted_outcome_count=len(conflicted_snapshot),
        primary_rows_sha256=_rows_sha256(_PRIMARY_ROWS_DOMAIN, primary_analysis),
        conflicted_rows_sha256=(
            _rows_sha256(_CONFLICTED_ROWS_DOMAIN, conflicted_analysis)
            if conflicted_snapshot
            else None
        ),
        cost_attribution_rows_sha256=(_cost_rows_sha256(costs) if costs is not None else None),
        cost_attribution_complete=costs is not None,
        shared_draw_schedule_sha256=schedule.schedule_sha256,
        cells=cells,
        contrasts=contrasts,
    )
    canonical_historical_three_family_bootstrap_v2(result)
    return result


def canonical_historical_three_family_bootstrap_v2(
    value: HistoricalThreeFamilyBootstrapV2,
) -> bytes:
    """Return deterministic canonical bytes after checking the fixed claims."""

    if type(value) is not HistoricalThreeFamilyBootstrapV2:
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "bootstrap value must be an exact HistoricalThreeFamilyBootstrapV2"
        )
    if (
        value.bootstrap_version != HISTORICAL_THREE_FAMILY_BOOTSTRAP_VERSION_V2
        or value.block_days != HISTORICAL_THREE_FAMILY_BOOTSTRAP_BLOCK_DAYS_V2
        or value.samples != HISTORICAL_THREE_FAMILY_BOOTSTRAP_SAMPLES_V2
        or value.seed != HISTORICAL_THREE_FAMILY_BOOTSTRAP_SEED_V2
        or value.minimum_evaluable_per_cell != HISTORICAL_THREE_FAMILY_BOOTSTRAP_MIN_EVALUABLE_V2
        or value.historical_only is not True
        or value.exposed_retrospective_only is not True
        or value.zero_alert_days_retained is not True
        or value.shared_draws_across_all_cells is not True
        or value.conflicted_pooled_with_clean is not False
        or value.overlapping_event_drawdown_valid is not False
        or value.inference_complete is not False
        or value.multiplicity_adjusted is not False
        or value.efficacy_validated is not False
        or value.probability is not False
        or value.probability_calibrated is not False
        or value.promoting is not False
        or value.order_placement is not False
    ):
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "bootstrap version, method, or fixed claims differ"
        )
    expected = hashlib.sha256(
        _RESULT_DOMAIN + canonical_json_line(_result_document(value, include_hash=False))
    ).hexdigest()
    if value.artifact_sha256 != expected:
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "bootstrap artifact hash differs from canonical content"
        )
    return canonical_json_line(_result_document(value, include_hash=True))


def _primary_analysis_row(row: HistoricalThreeFamilyOutcomeV2) -> _AnalysisRowV2:
    bucket = (
        HistoricalThreeFamilyBootstrapBucketV2.BROAD_3_OF_3
        if row.bucket is HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3
        else HistoricalThreeFamilyBootstrapBucketV2.CLEAN_2_PLUS_NEUTRAL
    )
    _require_safe_return(row.net_return_micros, "primary net return")
    return _AnalysisRowV2(
        source=HistoricalThreeFamilyCostSourceV2.PRIMARY_CLEAN,
        event_id=row.event_id,
        execution_contract_sha256=row.execution_contract_sha256,
        symbol=row.symbol,
        decision_time_ms=row.decision_time_ms,
        side=row.side,
        bucket=bucket,
        horizon_bars=row.horizon_bars,
        evaluable=row.evaluable,
        exclusion_reason=row.exclusion_reason,
        net_return_micros=row.net_return_micros,
    )


def _validate_conflicted_rows(
    rows: tuple[HistoricalThreeFamilyConflictedOutcomeV2, ...],
    *,
    execution_contract_sha256: str,
) -> tuple[tuple[_AnalysisRowV2, ...], int]:
    if any(type(row) is not HistoricalThreeFamilyConflictedOutcomeV2 for row in rows):
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "conflicted input requires exact conflicted outcome rows"
        )
    by_event: dict[str, list[HistoricalThreeFamilyConflictedOutcomeV2]] = {}
    seen: set[tuple[str, int]] = set()
    for row in rows:
        key = (row.event_id, row.horizon_bars)
        if key in seen:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "duplicate conflicted event/horizon outcome is not permitted"
            )
        seen.add(key)
        by_event.setdefault(row.event_id, []).append(row)
        if row.execution_contract_sha256 != execution_contract_sha256:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "conflicted and primary outcomes require the same execution contract"
            )
        _require_safe_return(row.net_return_micros, "conflicted net return")
    identities: dict[tuple[object, ...], str] = {}
    for event_id, event_rows in by_event.items():
        if {row.horizon_bars for row in event_rows} != set(
            HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
        ):
            raise HistoricalThreeFamilyBootstrapErrorV2(
                f"conflicted event {event_id} does not contain every frozen horizon"
            )
        identity_values = {
            (
                row.comparator_protocol_version,
                row.topology_rule_version,
                row.execution_contract_sha256,
                row.symbol,
                row.decision_time_ms,
                row.side,
                row.directional_agreement_micros,
            )
            for row in event_rows
        }
        if len(identity_values) != 1:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                f"conflicted event {event_id} changes identity across horizons"
            )
        identity = next(iter(identity_values))
        previous = identities.setdefault(identity, event_id)
        if previous != event_id:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "two conflicted event IDs claim the same comparator identity"
            )
    return (
        tuple(
            _AnalysisRowV2(
                source=HistoricalThreeFamilyCostSourceV2.CONFLICTED_COMPARATOR,
                event_id=row.event_id,
                execution_contract_sha256=row.execution_contract_sha256,
                symbol=row.symbol,
                decision_time_ms=row.decision_time_ms,
                side=row.side,
                bucket=HistoricalThreeFamilyBootstrapBucketV2.CONFLICTED_2_VS_1,
                horizon_bars=row.horizon_bars,
                evaluable=row.evaluable,
                exclusion_reason=row.exclusion_reason,
                net_return_micros=row.net_return_micros,
            )
            for row in rows
        ),
        len(by_event),
    )


def _validate_disjoint_sources(
    primary: tuple[_AnalysisRowV2, ...],
    conflicted: tuple[_AnalysisRowV2, ...],
) -> None:
    primary_ids = {row.event_id for row in primary}
    conflicted_ids = {row.event_id for row in conflicted}
    if primary_ids & conflicted_ids:
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "primary and conflicted outcomes cannot share an event ID"
        )
    primary_economics = {(row.symbol, row.decision_time_ms) for row in primary}
    conflicted_economics = {(row.symbol, row.decision_time_ms) for row in conflicted}
    if primary_economics & conflicted_economics:
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "one economic event cannot be both clean/broad and conflicted"
        )


def _validate_calendar(calendar_start_ms: int, calendar_end_ms: int) -> int:
    if (
        type(calendar_start_ms) is not int
        or type(calendar_end_ms) is not int
        or calendar_start_ms < 0
        or calendar_end_ms <= calendar_start_ms
        or calendar_end_ms > _JCS_SAFE_INTEGER_MAX
        or calendar_start_ms % _DAY_MS
        or calendar_end_ms % _DAY_MS
    ):
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "bootstrap calendar must be an increasing UTC-midnight half-open range"
        )
    day_count = (calendar_end_ms - calendar_start_ms) // _DAY_MS
    if not HISTORICAL_THREE_FAMILY_BOOTSTRAP_BLOCK_DAYS_V2 <= day_count <= (_MAX_CALENDAR_DAYS):
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "bootstrap calendar must contain between 7 and 36,600 UTC days"
        )
    return day_count


def _validate_rows_in_calendar(
    rows: Sequence[_AnalysisRowV2],
    *,
    calendar_start_ms: int,
    calendar_end_ms: int,
) -> None:
    for row in rows:
        if not calendar_start_ms <= row.decision_time_ms < calendar_end_ms:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "outcome lies outside the frozen UTC bootstrap calendar"
            )


def build_historical_three_family_bootstrap_schedule_v2(
    *,
    calendar_start_ms: int,
    calendar_end_ms: int,
) -> HistoricalThreeFamilyBootstrapScheduleV2:
    """Build and hash the frozen schedule without consulting any outcomes."""

    day_count = _validate_calendar(calendar_start_ms, calendar_end_ms)
    full_blocks, remainder = divmod(
        day_count,
        HISTORICAL_THREE_FAMILY_BOOTSTRAP_BLOCK_DAYS_V2,
    )
    block_lengths = (HISTORICAL_THREE_FAMILY_BOOTSTRAP_BLOCK_DAYS_V2,) * full_blocks + (
        (remainder,) if remainder else ()
    )
    header = {
        "block_lengths": list(block_lengths),
        "calendar_end_ms_exclusive": calendar_end_ms,
        "calendar_start_ms_inclusive": calendar_start_ms,
        "method": "shared_circular_moving_utc_calendar_day_blocks",
        "samples": HISTORICAL_THREE_FAMILY_BOOTSTRAP_SAMPLES_V2,
        "seed": HISTORICAL_THREE_FAMILY_BOOTSTRAP_SEED_V2,
    }
    digest = hashlib.sha256(_SCHEDULE_DOMAIN + canonical_json_line(header))
    rng = random.Random(HISTORICAL_THREE_FAMILY_BOOTSTRAP_SEED_V2)
    schedules: list[tuple[int, ...]] = []
    for _ in range(HISTORICAL_THREE_FAMILY_BOOTSTRAP_SAMPLES_V2):
        starts = tuple(rng.randrange(day_count) for _ in block_lengths)
        schedules.append(starts)
        digest.update(",".join(str(value) for value in starts).encode("ascii"))
        digest.update(b"\n")
    return HistoricalThreeFamilyBootstrapScheduleV2(
        calendar_start_ms=calendar_start_ms,
        calendar_end_ms=calendar_end_ms,
        calendar_days=day_count,
        block_lengths=block_lengths,
        starts=tuple(schedules),
        schedule_sha256=digest.hexdigest(),
    )


def _all_cell_keys() -> tuple[_CellKeyV2, ...]:
    return tuple(
        (horizon, side, bucket)
        for horizon in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
        for side in HistoricalThreeFamilySideV2
        for bucket in HistoricalThreeFamilyBootstrapBucketV2
    )


def _build_daily(
    rows: Sequence[_AnalysisRowV2],
    *,
    calendar_start_ms: int,
    day_count: int,
) -> dict[_CellKeyV2, _DailyCellV2]:
    mutable = {
        key: _MutableDailyCellV2(
            events=[0] * day_count,
            evaluable=[0] * day_count,
            net_sum=[0] * day_count,
            hits=[0] * day_count,
            gains=[0] * day_count,
            losses=[0] * day_count,
        )
        for key in _all_cell_keys()
    }
    for row in rows:
        offset = (row.decision_time_ms - calendar_start_ms) // _DAY_MS
        cell = mutable[(row.horizon_bars, row.side, row.bucket)]
        cell.events[offset] += 1
        if not row.evaluable:
            continue
        value = cast(int, row.net_return_micros)
        cell.evaluable[offset] += 1
        cell.net_sum[offset] += value
        cell.hits[offset] += int(value > 0)
        cell.gains[offset] += max(value, 0)
        cell.losses[offset] += max(-value, 0)
    return {
        key: _DailyCellV2(
            events=tuple(cell.events),
            evaluable=tuple(cell.evaluable),
            net_sum=tuple(cell.net_sum),
            hits=tuple(cell.hits),
            gains=tuple(cell.gains),
            losses=tuple(cell.losses),
        )
        for key, cell in mutable.items()
    }


def _block_aggregate(cell: _DailyCellV2, length: int) -> _BlockAggregateV2:
    return _BlockAggregateV2(
        evaluable=_circular_sums(cell.evaluable, length),
        net_sum=_circular_sums(cell.net_sum, length),
        hits=_circular_sums(cell.hits, length),
        gains=_circular_sums(cell.gains, length),
        losses=_circular_sums(cell.losses, length),
    )


def _circular_sums(values: tuple[int, ...], length: int) -> tuple[int, ...]:
    size = len(values)
    doubled = (*values, *values)
    prefix = [0]
    for value in doubled:
        prefix.append(prefix[-1] + value)
    return tuple(prefix[start + length] - prefix[start] for start in range(size))


def _metrics_from_totals(
    evaluable: int,
    net_sum: int,
    hits: int,
    gains: int,
    losses: int,
) -> _MetricValuesV2:
    if evaluable == 0:
        return None, None, None
    return (
        Fraction(net_sum, evaluable),
        Fraction(hits * _MICROS, evaluable),
        Fraction(gains * _MICROS, losses) if losses else None,
    )


def _build_cell(
    key: _CellKeyV2,
    *,
    daily: _DailyCellV2,
    estimates: tuple[list[Fraction], list[Fraction], list[Fraction]],
    cost_by_key: dict[
        tuple[HistoricalThreeFamilyCostSourceV2, str, int],
        HistoricalThreeFamilyCostAttributionV2,
    ]
    | None,
    schedule_sha256: str,
) -> HistoricalThreeFamilyBootstrapCellV2:
    events = sum(daily.events)
    evaluable = sum(daily.evaluable)
    points = _metrics_from_totals(
        evaluable,
        sum(daily.net_sum),
        sum(daily.hits),
        sum(daily.gains),
        sum(daily.losses),
    )
    profit_factor_state = _profit_factor_state(
        evaluable=evaluable,
        gains=sum(daily.gains),
        losses=sum(daily.losses),
    )
    return HistoricalThreeFamilyBootstrapCellV2(
        horizon_bars=key[0],
        side=key[1],
        bucket=key[2],
        events=events,
        evaluable=evaluable,
        zero_alert_days=sum(value == 0 for value in daily.events),
        feasibility=_feasibility(evaluable),
        profit_factor_state=profit_factor_state,
        endpoints=tuple(
            _endpoint(
                metric,
                point=points[index],
                estimates=estimates[index],
                schedule_sha256=schedule_sha256,
                with_null_p_value=False,
            )
            for index, metric in enumerate(HistoricalThreeFamilyBootstrapMetricV2)
        ),
        cost_attribution=(
            None
            if cost_by_key is None
            else _cost_summary_for_cell(key, tuple(cost_by_key.values()))
        ),
        shared_draw_schedule_sha256=schedule_sha256,
    )


def _build_contrast(
    *,
    comparison: HistoricalThreeFamilyBootstrapComparisonV2,
    comparator_bucket: HistoricalThreeFamilyBootstrapBucketV2,
    horizon: int,
    side: HistoricalThreeFamilySideV2,
    broad: HistoricalThreeFamilyBootstrapCellV2,
    comparator: HistoricalThreeFamilyBootstrapCellV2,
    estimates: tuple[list[Fraction], list[Fraction], list[Fraction]],
    schedule_sha256: str,
) -> HistoricalThreeFamilyBootstrapContrastV2:
    broad_points = tuple(endpoint.point_estimate for endpoint in broad.endpoints)
    comparator_points = tuple(endpoint.point_estimate for endpoint in comparator.endpoints)
    point_differences = tuple(
        None if left is None or right is None else left.as_fraction() - right.as_fraction()
        for left, right in zip(broad_points, comparator_points, strict=True)
    )
    feasibility = _comparison_feasibility(broad.evaluable, comparator.evaluable)
    return HistoricalThreeFamilyBootstrapContrastV2(
        comparison=comparison,
        comparator_protocol_version=(
            HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2
            if comparison
            is HistoricalThreeFamilyBootstrapComparisonV2.BROAD_MINUS_CLEAN_2_PLUS_NEUTRAL
            else HISTORICAL_THREE_FAMILY_CONFLICTED_OUTCOME_PROTOCOL_V2
        ),
        horizon_bars=horizon,
        side=side,
        broad_bucket=HistoricalThreeFamilyBootstrapBucketV2.BROAD_3_OF_3,
        comparator_bucket=comparator_bucket,
        broad_evaluable=broad.evaluable,
        comparator_evaluable=comparator.evaluable,
        feasibility=feasibility,
        comparison_interpretable=False,
        endpoints=tuple(
            _endpoint(
                metric,
                point=point_differences[index],
                estimates=estimates[index],
                schedule_sha256=schedule_sha256,
                with_null_p_value=True,
            )
            for index, metric in enumerate(HistoricalThreeFamilyBootstrapMetricV2)
        ),
        shared_draw_schedule_sha256=schedule_sha256,
    )


def _endpoint(
    metric: HistoricalThreeFamilyBootstrapMetricV2,
    *,
    point: Fraction | None,
    estimates: Sequence[Fraction],
    schedule_sha256: str,
    with_null_p_value: bool,
) -> HistoricalThreeFamilyBootstrapEndpointV2:
    ordered = tuple(sorted(estimates))
    interval = None
    lower = None
    p_value = None
    if point is not None and ordered:
        interval = (
            _rational(_type7_quantile(ordered, Fraction(1, 40))),
            _rational(_type7_quantile(ordered, Fraction(39, 40))),
        )
        lower = _rational(2 * point - _type7_quantile(ordered, Fraction(19, 20)))
        if with_null_p_value:
            if point <= 0:
                p_value = _rational(Fraction(1))
            else:
                exceedances = sum(value - point >= point for value in ordered)
                p_value = _rational(Fraction(1 + exceedances, len(ordered) + 1))
    return HistoricalThreeFamilyBootstrapEndpointV2(
        metric=metric,
        point_estimate=None if point is None else _rational(point),
        valid_replicates=len(ordered),
        invalid_replicates=HISTORICAL_THREE_FAMILY_BOOTSTRAP_SAMPLES_V2 - len(ordered),
        two_sided_percentile_95_interval=interval,
        one_sided_basic_95_lower=lower,
        null_centered_one_sided_p_value=p_value,
        shared_draw_schedule_sha256=schedule_sha256,
    )


def _type7_quantile(values: tuple[Fraction, ...], probability: Fraction) -> Fraction:
    if not values:
        raise HistoricalThreeFamilyBootstrapErrorV2("bootstrap quantile requires a valid replicate")
    position = probability * (len(values) - 1)
    lower = position.numerator // position.denominator
    upper = lower if position.denominator == 1 else lower + 1
    if lower == upper:
        return values[lower]
    weight = position - lower
    return values[lower] * (1 - weight) + values[upper] * weight


def _rational(value: Fraction) -> HistoricalExactRationalV2:
    return HistoricalExactRationalV2(str(value.numerator), str(value.denominator))


def _feasibility(evaluable: int) -> HistoricalThreeFamilyBootstrapFeasibilityV2:
    if evaluable == 0:
        return HistoricalThreeFamilyBootstrapFeasibilityV2.INCONCLUSIVE_EMPTY
    if evaluable < HISTORICAL_THREE_FAMILY_BOOTSTRAP_MIN_EVALUABLE_V2:
        return HistoricalThreeFamilyBootstrapFeasibilityV2.INCONCLUSIVE_SPARSE
    return HistoricalThreeFamilyBootstrapFeasibilityV2.DESCRIPTIVE_EXPOSED_ONLY


def _comparison_feasibility(
    broad_evaluable: int,
    comparator_evaluable: int,
) -> HistoricalThreeFamilyBootstrapFeasibilityV2:
    if broad_evaluable == 0 or comparator_evaluable == 0:
        return HistoricalThreeFamilyBootstrapFeasibilityV2.INCONCLUSIVE_EMPTY
    if min(broad_evaluable, comparator_evaluable) < (
        HISTORICAL_THREE_FAMILY_BOOTSTRAP_MIN_EVALUABLE_V2
    ):
        return HistoricalThreeFamilyBootstrapFeasibilityV2.INCONCLUSIVE_SPARSE
    return HistoricalThreeFamilyBootstrapFeasibilityV2.DESCRIPTIVE_EXPOSED_ONLY


def _profit_factor_state(
    *,
    evaluable: int,
    gains: int,
    losses: int,
) -> HistoricalThreeFamilyProfitFactorStateV2:
    if evaluable == 0:
        return HistoricalThreeFamilyProfitFactorStateV2.NO_EVALUABLE_ROWS
    if losses:
        return HistoricalThreeFamilyProfitFactorStateV2.FINITE
    if gains:
        return HistoricalThreeFamilyProfitFactorStateV2.POSITIVE_WITH_NO_LOSSES
    return HistoricalThreeFamilyProfitFactorStateV2.ZERO_GROSS_WITH_NO_LOSSES


def _comparison_specs(
    include_conflicted: bool,
) -> tuple[
    tuple[
        HistoricalThreeFamilyBootstrapComparisonV2,
        HistoricalThreeFamilyBootstrapBucketV2,
    ],
    ...,
]:
    primary = (
        HistoricalThreeFamilyBootstrapComparisonV2.BROAD_MINUS_CLEAN_2_PLUS_NEUTRAL,
        HistoricalThreeFamilyBootstrapBucketV2.CLEAN_2_PLUS_NEUTRAL,
    )
    if not include_conflicted:
        return (primary,)
    return (
        primary,
        (
            HistoricalThreeFamilyBootstrapComparisonV2.BROAD_MINUS_CONFLICTED_2_VS_1,
            HistoricalThreeFamilyBootstrapBucketV2.CONFLICTED_2_VS_1,
        ),
    )


def _validate_cost_attributions(
    rows: Sequence[_AnalysisRowV2],
    costs: tuple[HistoricalThreeFamilyCostAttributionV2, ...] | None,
) -> (
    dict[
        tuple[HistoricalThreeFamilyCostSourceV2, str, int],
        HistoricalThreeFamilyCostAttributionV2,
    ]
    | None
):
    if costs is None:
        return None
    if any(type(cost) is not HistoricalThreeFamilyCostAttributionV2 for cost in costs):
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "cost attribution input requires exact companion rows"
        )
    expected = {(row.source, row.event_id, row.horizon_bars): row for row in rows}
    actual: dict[
        tuple[HistoricalThreeFamilyCostSourceV2, str, int],
        HistoricalThreeFamilyCostAttributionV2,
    ] = {}
    for cost in costs:
        key = (cost.source, cost.event_id, cost.horizon_bars)
        if key in actual:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "duplicate cost attribution key is not permitted"
            )
        actual[key] = cost
    if set(actual) != set(expected):
        raise HistoricalThreeFamilyBootstrapErrorV2(
            "cost attributions must cover every primary/conflicted outcome exactly once"
        )
    for key, cost in actual.items():
        row = expected[key]
        if (
            cost.execution_contract_sha256 != row.execution_contract_sha256
            or cost.side is not row.side
            or cost.bucket is not row.bucket
            or cost.evaluable is not row.evaluable
            or cost.exclusion_reason != row.exclusion_reason
            or cost.net_return_micros != row.net_return_micros
        ):
            raise HistoricalThreeFamilyBootstrapErrorV2(
                "cost attribution differs from its matched outcome"
            )
    return actual


def _cost_summary_for_cell(
    key: _CellKeyV2,
    costs: Sequence[HistoricalThreeFamilyCostAttributionV2],
) -> HistoricalThreeFamilyCostSummaryV2:
    expected_source = (
        HistoricalThreeFamilyCostSourceV2.CONFLICTED_COMPARATOR
        if key[2] is HistoricalThreeFamilyBootstrapBucketV2.CONFLICTED_2_VS_1
        else HistoricalThreeFamilyCostSourceV2.PRIMARY_CLEAN
    )
    cell = tuple(
        cost
        for cost in costs
        if cost.source is expected_source
        and cost.horizon_bars == key[0]
        and cost.side is key[1]
        and cost.bucket is key[2]
    )
    evaluable = tuple(cost for cost in cell if cost.evaluable)
    gross = [cast(int, cost.gross_directional_return_micros) for cost in evaluable]
    slippage = [cast(int, cost.slippage_return_micros) for cost in evaluable]
    fees = [cast(int, cost.fee_return_micros) for cost in evaluable]
    funding = [cast(int, cost.funding_return_micros) for cost in evaluable]
    total_cost = [cast(int, cost.total_cost_micros) for cost in evaluable]
    net = [cast(int, cost.net_return_micros) for cost in evaluable]
    mean_gross = _mean_integer(gross)
    mean_net = _mean_integer(net)
    evaluable_count = len(evaluable)
    gross_hits = sum(value > 0 for value in gross)
    net_hits = sum(value > 0 for value in net)
    hit_loss_count = sum(
        gross_value > 0 and net_value <= 0
        for gross_value, net_value in zip(gross, net, strict=True)
    )
    return HistoricalThreeFamilyCostSummaryV2(
        events=len(cell),
        evaluable=evaluable_count,
        coverage_micros=(_round_ratio(evaluable_count * _MICROS, len(cell)) if cell else 0),
        gross_directional_strict_hits=gross_hits,
        gross_directional_strict_hit_rate_micros=(
            _round_ratio(gross_hits * _MICROS, evaluable_count) if evaluable_count else None
        ),
        net_strict_hits=net_hits,
        net_strict_hit_rate_micros=(
            _round_ratio(net_hits * _MICROS, evaluable_count) if evaluable_count else None
        ),
        gross_to_net_hit_loss_count=hit_loss_count,
        gross_to_net_hit_loss_rate_micros=(
            _round_ratio(hit_loss_count * _MICROS, evaluable_count) if evaluable_count else None
        ),
        net_positive_without_gross_positive_count=sum(
            net_value > 0 and gross_value <= 0
            for gross_value, net_value in zip(gross, net, strict=True)
        ),
        mean_gross_directional_return_micros=mean_gross,
        mean_slippage_return_micros=_mean_integer(slippage),
        mean_fee_return_micros=_mean_integer(fees),
        mean_funding_return_micros=_mean_integer(funding),
        mean_total_cost_micros=_mean_integer(total_cost),
        mean_net_return_micros=mean_net,
        gross_to_net_mean_change_micros=(
            None if mean_gross is None or mean_net is None else mean_net - mean_gross
        ),
    )


def _mean_integer(values: Sequence[int]) -> int | None:
    return _round_ratio(sum(values), len(values)) if values else None


def _round_ratio(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise HistoricalThreeFamilyBootstrapErrorV2("ratio denominator must be positive")
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    if remainder * 2 >= denominator:
        quotient += 1
    return sign * quotient


def _validate_outcome_economics(
    *,
    evaluable: bool,
    exclusion_reason: str,
    net_return_micros: int | None,
    label: str,
) -> None:
    if type(evaluable) is not bool or not isinstance(exclusion_reason, str):
        raise HistoricalThreeFamilyBootstrapErrorV2(
            f"{label} evaluable and exclusion fields are malformed"
        )
    if evaluable:
        if exclusion_reason or type(net_return_micros) is not int:
            raise HistoricalThreeFamilyBootstrapErrorV2(
                f"{label} evaluable row requires one exact net return"
            )
        _require_safe_return(net_return_micros, f"{label} net return")
        return
    if _EXCLUSION_RE.fullmatch(exclusion_reason) is None or net_return_micros is not None:
        raise HistoricalThreeFamilyBootstrapErrorV2(
            f"{label} excluded row requires one normalized reason and no return"
        )


def _require_safe_return(value: int | None, label: str) -> None:
    if value is not None and (type(value) is not int or abs(value) > _JCS_SAFE_INTEGER_MAX):
        raise HistoricalThreeFamilyBootstrapErrorV2(
            f"{label} must be a JCS-safe integer when present"
        )


def _require_sha256(value: object, label: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise HistoricalThreeFamilyBootstrapErrorV2(f"{label} must be a lowercase SHA-256 digest")


def _rows_sha256(domain: bytes, rows: Sequence[_AnalysisRowV2]) -> str:
    ordered = sorted(
        rows,
        key=lambda row: (
            row.source.value,
            row.event_id,
            row.horizon_bars,
        ),
    )
    return hashlib.sha256(
        domain + canonical_json_line({"rows": [asdict(row) for row in ordered]})
    ).hexdigest()


def _cost_rows_sha256(
    rows: tuple[HistoricalThreeFamilyCostAttributionV2, ...],
) -> str:
    ordered = sorted(rows, key=lambda row: (row.source.value, row.event_id, row.horizon_bars))
    return hashlib.sha256(
        _COST_ROWS_DOMAIN + canonical_json_line({"rows": [asdict(row) for row in ordered]})
    ).hexdigest()


def _result_document(
    value: HistoricalThreeFamilyBootstrapV2,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document = asdict(value)
    document.pop("artifact_sha256", None)
    if include_hash:
        document["artifact_sha256"] = value.artifact_sha256
    return document
