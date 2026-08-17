from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final, Literal

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.strategy.directional_evidence import DirectionalStateClassV2
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
)

HISTORICAL_THREE_FAMILY_OUTCOME_AUDIT_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.1_HISTORICAL_THREE_FAMILY_OUTCOME_AUDIT_V1_FROZEN"
)
HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2: Final = (
    "R4B_CAUSAL_V2_HISTORICAL_THREE_FAMILY_AFTER_COST_FIXED_HORIZON_OUTCOME_V1_FROZEN"
)
HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2: Final = (1, 3, 6, 12, 72)

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]{2,30}$")
_EXCLUSION_REASON_RE: Final = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
_MICROS: Final = 1_000_000


class HistoricalThreeFamilyOutcomeAuditErrorV2(ValueError):
    """Raised when a historical three-family outcome audit is invalid."""


class HistoricalThreeFamilyAgreementBucketV2(StrEnum):
    TILT_2_OF_3 = "TILT_2_OF_3"
    BROAD_3_OF_3 = "BROAD_3_OF_3"


class HistoricalThreeFamilySideV2(StrEnum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"


class HistoricalThreeFamilyProfitFactorStateV2(StrEnum):
    NO_EVALUABLE_ROWS = "NO_EVALUABLE_ROWS"
    FINITE = "FINITE"
    POSITIVE_WITH_NO_LOSSES = "POSITIVE_WITH_NO_LOSSES"
    ZERO_GROSS_WITH_NO_LOSSES = "ZERO_GROSS_WITH_NO_LOSSES"


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyOutcomeV2:
    """One historical, after-cost outcome for a frozen consensus event.

    ``net_return_micros`` is signed in the consensus direction. A positive
    value therefore means that the indicated bullish or bearish side made
    money after the frozen execution/cost contract. It is never an unsigned
    market return and this record is never a live or probability claim.
    """

    event_id: str
    outcome_protocol_version: str
    rule_version: str
    execution_contract_sha256: str
    venue: VenueV2
    symbol: str
    decision_time_ms: int
    state_class: DirectionalStateClassV2
    directional_agreement_micros: int
    horizon_bars: int
    evaluable: bool
    exclusion_reason: str
    net_return_micros: int | None
    historical_only: Literal[True] = field(init=False, default=True)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    inference_complete: Literal[False] = field(init=False, default=False)

    def __post_init__(self) -> None:
        if not isinstance(self.event_id, str) or _SHA256_RE.fullmatch(self.event_id) is None:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "event_id must be a lowercase SHA-256 digest"
            )
        if self.outcome_protocol_version != HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "outcome_protocol_version must bind the frozen historical contract"
            )
        if self.rule_version != HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "outcome must bind the frozen historical three-family consensus rule"
            )
        if (
            not isinstance(self.execution_contract_sha256, str)
            or _SHA256_RE.fullmatch(self.execution_contract_sha256) is None
        ):
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "execution_contract_sha256 must be a lowercase SHA-256 digest"
            )
        if self.venue is not VenueV2.USDM_FUTURES:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "historical three-family outcomes require the USD-M futures venue"
            )
        if not isinstance(self.symbol, str) or _SYMBOL_RE.fullmatch(self.symbol) is None:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "symbol must be an uppercase normalized market symbol"
            )
        if type(self.decision_time_ms) is not int or self.decision_time_ms < 0:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "decision_time_ms must be a nonnegative integer"
            )
        if not isinstance(self.state_class, DirectionalStateClassV2):
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "state_class must be a DirectionalStateClassV2 value"
            )
        _, side = _classify_state(self.state_class)
        if type(self.directional_agreement_micros) is not int or not (
            -_MICROS <= self.directional_agreement_micros <= _MICROS
        ):
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "directional_agreement_micros must be an integer in [-1e6, 1e6]"
            )
        sign_valid = (
            self.directional_agreement_micros > 0
            if side is HistoricalThreeFamilySideV2.BULLISH
            else self.directional_agreement_micros < 0
        )
        if not sign_valid:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "agreement sign must match the frozen historical consensus state"
            )
        if type(self.horizon_bars) is not int or (
            self.horizon_bars not in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
        ):
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "horizon_bars is not in the frozen 5/15/30/60/360-minute set"
            )
        if type(self.evaluable) is not bool:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2("evaluable must be Boolean")
        if not isinstance(self.exclusion_reason, str):
            raise HistoricalThreeFamilyOutcomeAuditErrorV2("exclusion_reason must be a string")
        if self.evaluable:
            if self.exclusion_reason:
                raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                    "evaluable outcomes cannot carry an exclusion reason"
                )
            if type(self.net_return_micros) is not int:
                raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                    "evaluable outcomes require an integer after-cost return"
                )
            return
        if (
            _EXCLUSION_REASON_RE.fullmatch(self.exclusion_reason) is None
            or self.net_return_micros is not None
        ):
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "unevaluable outcomes require a normalized exclusion reason and no return"
            )

    @property
    def bucket(self) -> HistoricalThreeFamilyAgreementBucketV2:
        return _classify_state(self.state_class)[0]

    @property
    def side(self) -> HistoricalThreeFamilySideV2:
        return _classify_state(self.state_class)[1]


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyOutcomeSummaryV2:
    horizon_bars: int
    side: HistoricalThreeFamilySideV2
    bucket: HistoricalThreeFamilyAgreementBucketV2
    events: int
    evaluable: int
    coverage_micros: int
    strict_after_cost_hits: int
    strict_after_cost_hit_rate_micros: int | None
    mean_net_return_micros: int | None
    median_net_return_micros: int | None
    profit_factor_micros: int | None
    profit_factor_state: HistoricalThreeFamilyProfitFactorStateV2
    historical_only: Literal[True] = field(init=False, default=True)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    inference_complete: Literal[False] = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyOutcomeContrastV2:
    horizon_bars: int
    side: HistoricalThreeFamilySideV2
    broad_minus_tilt_mean_net_return_micros: int | None
    point_monotonic: bool | None
    historical_only: Literal[True] = field(init=False, default=True)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    inference_complete: Literal[False] = field(init=False, default=False)


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyOutcomeAuditV2:
    audit_version: str
    consensus_rule_version: str
    outcome_protocol_version: str
    execution_contract_sha256s: tuple[str, ...]
    horizons_bars: tuple[int, ...]
    event_count: int
    outcome_count: int
    summaries: tuple[HistoricalThreeFamilyOutcomeSummaryV2, ...]
    contrasts: tuple[HistoricalThreeFamilyOutcomeContrastV2, ...]
    historical_only: Literal[True] = field(init=False, default=True)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    inference_complete: Literal[False] = field(init=False, default=False)
    frozen_formula_efficacy_validated: Literal[False] = field(
        init=False,
        default=False,
    )
    overlapping_event_drawdown_valid: Literal[False] = field(
        init=False,
        default=False,
    )


def audit_historical_three_family_outcomes_v2(
    rows: Sequence[HistoricalThreeFamilyOutcomeV2],
) -> HistoricalThreeFamilyOutcomeAuditV2:
    """Audit complete matched outcomes without fitting or inference."""

    snapshot = tuple(rows)
    if not snapshot:
        raise HistoricalThreeFamilyOutcomeAuditErrorV2(
            "audit requires at least one historical outcome"
        )
    if any(not isinstance(item, HistoricalThreeFamilyOutcomeV2) for item in snapshot):
        raise HistoricalThreeFamilyOutcomeAuditErrorV2(
            "audit accepts HistoricalThreeFamilyOutcomeV2 rows only"
        )

    by_event: dict[str, list[HistoricalThreeFamilyOutcomeV2]] = defaultdict(list)
    unique_keys: set[tuple[str, int]] = set()
    for item in snapshot:
        key = (item.event_id, item.horizon_bars)
        if key in unique_keys:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "duplicate event/horizon outcome is not permitted"
            )
        unique_keys.add(key)
        by_event[item.event_id].append(item)

    expected_horizons = set(HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2)
    event_id_by_identity: dict[tuple[object, ...], str] = {}
    for event_id, event_rows in by_event.items():
        if {item.horizon_bars for item in event_rows} != expected_horizons:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                f"event {event_id} does not contain every frozen horizon"
            )
        identities = {
            (
                item.rule_version,
                item.outcome_protocol_version,
                item.execution_contract_sha256,
                item.venue,
                item.symbol,
                item.decision_time_ms,
                item.state_class,
                item.directional_agreement_micros,
            )
            for item in event_rows
        }
        if len(identities) != 1:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                f"event {event_id} changes identity across horizons"
            )
        identity = next(iter(identities))
        prior_event_id = event_id_by_identity.setdefault(identity, event_id)
        if prior_event_id != event_id:
            raise HistoricalThreeFamilyOutcomeAuditErrorV2(
                "two event IDs claim the same deterministic consensus identity"
            )

    grouped: dict[
        tuple[
            int,
            HistoricalThreeFamilySideV2,
            HistoricalThreeFamilyAgreementBucketV2,
        ],
        list[HistoricalThreeFamilyOutcomeV2],
    ] = defaultdict(list)
    for item in snapshot:
        grouped[(item.horizon_bars, item.side, item.bucket)].append(item)

    summaries = tuple(
        _summarize(horizon, side, bucket, grouped.get((horizon, side, bucket), ()))
        for horizon in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2
        for side in HistoricalThreeFamilySideV2
        for bucket in HistoricalThreeFamilyAgreementBucketV2
    )
    summary_by_key = {(item.horizon_bars, item.side, item.bucket): item for item in summaries}
    contrasts: list[HistoricalThreeFamilyOutcomeContrastV2] = []
    for horizon in HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2:
        for side in HistoricalThreeFamilySideV2:
            tilt = summary_by_key[
                (horizon, side, HistoricalThreeFamilyAgreementBucketV2.TILT_2_OF_3)
            ]
            broad = summary_by_key[
                (horizon, side, HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3)
            ]
            if tilt.mean_net_return_micros is None or broad.mean_net_return_micros is None:
                difference = None
                point_monotonic = None
            else:
                difference = broad.mean_net_return_micros - tilt.mean_net_return_micros
                point_monotonic = difference > 0
            contrasts.append(
                HistoricalThreeFamilyOutcomeContrastV2(
                    horizon_bars=horizon,
                    side=side,
                    broad_minus_tilt_mean_net_return_micros=difference,
                    point_monotonic=point_monotonic,
                )
            )

    return HistoricalThreeFamilyOutcomeAuditV2(
        audit_version=HISTORICAL_THREE_FAMILY_OUTCOME_AUDIT_VERSION_V2,
        consensus_rule_version=HISTORICAL_THREE_FAMILY_CONSENSUS_RULE_VERSION_V2,
        outcome_protocol_version=HISTORICAL_THREE_FAMILY_OUTCOME_PROTOCOL_V2,
        execution_contract_sha256s=tuple(
            sorted({item.execution_contract_sha256 for item in snapshot})
        ),
        horizons_bars=HISTORICAL_THREE_FAMILY_OUTCOME_HORIZONS_BARS_V2,
        event_count=len(by_event),
        outcome_count=len(snapshot),
        summaries=summaries,
        contrasts=tuple(contrasts),
    )


def _classify_state(
    state_class: DirectionalStateClassV2,
) -> tuple[
    HistoricalThreeFamilyAgreementBucketV2,
    HistoricalThreeFamilySideV2,
]:
    mapping = {
        DirectionalStateClassV2.BULLISH_STATE_TILT: (
            HistoricalThreeFamilyAgreementBucketV2.TILT_2_OF_3,
            HistoricalThreeFamilySideV2.BULLISH,
        ),
        DirectionalStateClassV2.BROAD_BULLISH_STATE: (
            HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3,
            HistoricalThreeFamilySideV2.BULLISH,
        ),
        DirectionalStateClassV2.BEARISH_STATE_TILT: (
            HistoricalThreeFamilyAgreementBucketV2.TILT_2_OF_3,
            HistoricalThreeFamilySideV2.BEARISH,
        ),
        DirectionalStateClassV2.BROAD_BEARISH_STATE: (
            HistoricalThreeFamilyAgreementBucketV2.BROAD_3_OF_3,
            HistoricalThreeFamilySideV2.BEARISH,
        ),
    }
    try:
        return mapping[state_class]
    except KeyError as exc:
        raise HistoricalThreeFamilyOutcomeAuditErrorV2(
            "only frozen 2-of-3 and 3-of-3 historical states are auditable"
        ) from exc


def _summarize(
    horizon: int,
    side: HistoricalThreeFamilySideV2,
    bucket: HistoricalThreeFamilyAgreementBucketV2,
    rows: Sequence[HistoricalThreeFamilyOutcomeV2],
) -> HistoricalThreeFamilyOutcomeSummaryV2:
    values = [
        item.net_return_micros
        for item in rows
        if item.evaluable and item.net_return_micros is not None
    ]
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if not values:
        profit_factor = None
        profit_factor_state = HistoricalThreeFamilyProfitFactorStateV2.NO_EVALUABLE_ROWS
    elif losses > 0:
        profit_factor = _round_ratio(gains * _MICROS, losses)
        profit_factor_state = HistoricalThreeFamilyProfitFactorStateV2.FINITE
    elif gains > 0:
        profit_factor = None
        profit_factor_state = HistoricalThreeFamilyProfitFactorStateV2.POSITIVE_WITH_NO_LOSSES
    else:
        profit_factor = None
        profit_factor_state = HistoricalThreeFamilyProfitFactorStateV2.ZERO_GROSS_WITH_NO_LOSSES
    return HistoricalThreeFamilyOutcomeSummaryV2(
        horizon_bars=horizon,
        side=side,
        bucket=bucket,
        events=len(rows),
        evaluable=len(values),
        coverage_micros=_round_ratio(len(values) * _MICROS, len(rows)) if rows else 0,
        strict_after_cost_hits=sum(value > 0 for value in values),
        strict_after_cost_hit_rate_micros=(
            _round_ratio(sum(value > 0 for value in values) * _MICROS, len(values))
            if values
            else None
        ),
        mean_net_return_micros=(_round_ratio(sum(values), len(values)) if values else None),
        median_net_return_micros=_integer_median(values) if values else None,
        profit_factor_micros=profit_factor,
        profit_factor_state=profit_factor_state,
    )


def _round_ratio(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise HistoricalThreeFamilyOutcomeAuditErrorV2("ratio denominator must be positive")
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    if 2 * remainder >= denominator:
        quotient += 1
    return sign * quotient


def _integer_median(values: Sequence[int]) -> int:
    if not values:
        raise HistoricalThreeFamilyOutcomeAuditErrorV2("median requires at least one value")
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return _round_ratio(ordered[midpoint - 1] + ordered[midpoint], 2)
