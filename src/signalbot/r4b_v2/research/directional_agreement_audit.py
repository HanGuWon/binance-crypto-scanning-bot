from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.strategy.directional_evidence import (
    DIRECTIONAL_EVIDENCE_RULE_VERSION_V2,
    DirectionalStateClassV2,
)

DIRECTIONAL_AGREEMENT_AUDIT_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.0_DIRECTIONAL_AGREEMENT_AUDIT_V1_FROZEN"
)
DIRECTIONAL_AGREEMENT_OUTCOME_PROTOCOL_V2: Final = (
    "R4B_CAUSAL_V2_DIRECTIONAL_AFTER_COST_FIXED_HORIZON_OUTCOME_V1"
)
DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2: Final = (1, 3, 6, 12, 72)

_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]{2,30}$")
_MICROS: Final = 1_000_000


class DirectionalAgreementAuditErrorV2(ValueError):
    """Raised when a matched directional-agreement audit is incomplete."""


class DirectionalAgreementBucketV2(StrEnum):
    TILT_2_OF_3 = "TILT_2_OF_3"
    BROAD_3_OF_3 = "BROAD_3_OF_3"


class DirectionalAgreementSideV2(StrEnum):
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"


class ProfitFactorStateV2(StrEnum):
    NO_EVALUABLE_ROWS = "NO_EVALUABLE_ROWS"
    FINITE = "FINITE"
    POSITIVE_WITH_NO_LOSSES = "POSITIVE_WITH_NO_LOSSES"
    ZERO_GROSS_WITH_NO_LOSSES = "ZERO_GROSS_WITH_NO_LOSSES"


@dataclass(frozen=True, slots=True)
class DirectionalAgreementOutcomeV2:
    """One after-cost directional outcome for one frozen panel and horizon.

    ``net_return_micros`` is already signed in the panel direction: a positive
    value means the indicated bullish or bearish side made money after all
    modeled costs. It must never be a raw unsigned market return.
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

    def __post_init__(self) -> None:
        if _SHA256_RE.fullmatch(self.event_id) is None:
            raise DirectionalAgreementAuditErrorV2(
                "event_id must be a lowercase SHA-256 digest"
            )
        if self.outcome_protocol_version != DIRECTIONAL_AGREEMENT_OUTCOME_PROTOCOL_V2:
            raise DirectionalAgreementAuditErrorV2(
                "outcome_protocol_version must bind the frozen fixed-horizon contract"
            )
        if self.rule_version != DIRECTIONAL_EVIDENCE_RULE_VERSION_V2:
            raise DirectionalAgreementAuditErrorV2(
                "outcome must bind the frozen directional evidence rule"
            )
        if _SHA256_RE.fullmatch(self.execution_contract_sha256) is None:
            raise DirectionalAgreementAuditErrorV2(
                "execution_contract_sha256 must be a lowercase SHA-256 digest"
            )
        if not isinstance(self.venue, VenueV2):
            raise DirectionalAgreementAuditErrorV2("venue must be a VenueV2 value")
        if _SYMBOL_RE.fullmatch(self.symbol) is None:
            raise DirectionalAgreementAuditErrorV2(
                "symbol must be an uppercase normalized market symbol"
            )
        if type(self.decision_time_ms) is not int or self.decision_time_ms < 0:
            raise DirectionalAgreementAuditErrorV2(
                "decision_time_ms must be a nonnegative integer"
            )
        if not isinstance(self.state_class, DirectionalStateClassV2):
            raise DirectionalAgreementAuditErrorV2(
                "state_class must be a DirectionalStateClassV2 value"
            )
        bucket, side = _classify_state(self.state_class)
        if type(self.directional_agreement_micros) is not int or not (
            -_MICROS <= self.directional_agreement_micros <= _MICROS
        ):
            raise DirectionalAgreementAuditErrorV2(
                "directional_agreement_micros must be an integer in [-1e6, 1e6]"
            )
        if side is DirectionalAgreementSideV2.BULLISH:
            sign_valid = self.directional_agreement_micros > 0
        else:
            sign_valid = self.directional_agreement_micros < 0
        if not sign_valid:
            raise DirectionalAgreementAuditErrorV2(
                "agreement sign must match the frozen directional state"
            )
        if bucket is DirectionalAgreementBucketV2.BROAD_3_OF_3 and (
            self.state_class
            not in {
                DirectionalStateClassV2.BROAD_BULLISH_STATE,
                DirectionalStateClassV2.BROAD_BEARISH_STATE,
            }
        ):
            raise DirectionalAgreementAuditErrorV2("invalid broad state")
        if self.horizon_bars not in DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2:
            raise DirectionalAgreementAuditErrorV2(
                "horizon_bars is not in the frozen 5/15/30/60/360-minute set"
            )
        if type(self.evaluable) is not bool:
            raise DirectionalAgreementAuditErrorV2("evaluable must be Boolean")
        if self.evaluable:
            if self.exclusion_reason:
                raise DirectionalAgreementAuditErrorV2(
                    "evaluable outcomes cannot carry an exclusion reason"
                )
            if type(self.net_return_micros) is not int:
                raise DirectionalAgreementAuditErrorV2(
                    "evaluable outcomes require an integer after-cost return"
                )
        elif not self.exclusion_reason or self.net_return_micros is not None:
            raise DirectionalAgreementAuditErrorV2(
                "unevaluable outcomes require a reason and no return"
            )

    @property
    def bucket(self) -> DirectionalAgreementBucketV2:
        return _classify_state(self.state_class)[0]

    @property
    def side(self) -> DirectionalAgreementSideV2:
        return _classify_state(self.state_class)[1]


@dataclass(frozen=True, slots=True)
class DirectionalAgreementSummaryV2:
    horizon_bars: int
    side: DirectionalAgreementSideV2
    bucket: DirectionalAgreementBucketV2
    events: int
    evaluable: int
    coverage_micros: int
    strict_after_cost_hits: int
    strict_after_cost_hit_rate_micros: int | None
    mean_net_return_micros: int | None
    median_net_return_micros: int | None
    profit_factor_micros: int | None
    profit_factor_state: ProfitFactorStateV2


@dataclass(frozen=True, slots=True)
class DirectionalAgreementContrastV2:
    horizon_bars: int
    side: DirectionalAgreementSideV2
    broad_minus_tilt_mean_net_return_micros: int | None
    point_monotonic: bool | None
    inference_complete: bool = False
    probability_calibrated: bool = False


@dataclass(frozen=True, slots=True)
class DirectionalAgreementAuditV2:
    audit_version: str
    evidence_rule_version: str
    outcome_protocol_version: str
    execution_contract_sha256s: tuple[str, ...]
    horizons_bars: tuple[int, ...]
    event_count: int
    outcome_count: int
    summaries: tuple[DirectionalAgreementSummaryV2, ...]
    contrasts: tuple[DirectionalAgreementContrastV2, ...]
    frozen_formula_efficacy_validated: bool = False
    probability_calibrated: bool = False
    overlapping_event_drawdown_valid: bool = False


def audit_directional_agreement_outcomes_v2(
    rows: Sequence[DirectionalAgreementOutcomeV2],
) -> DirectionalAgreementAuditV2:
    """Audit complete matched outcomes without fitting or selecting a threshold."""

    snapshot = tuple(rows)
    if not snapshot:
        raise DirectionalAgreementAuditErrorV2("audit requires at least one outcome")
    if any(not isinstance(item, DirectionalAgreementOutcomeV2) for item in snapshot):
        raise DirectionalAgreementAuditErrorV2(
            "audit accepts DirectionalAgreementOutcomeV2 rows only"
        )

    by_event: dict[str, list[DirectionalAgreementOutcomeV2]] = defaultdict(list)
    unique_keys: set[tuple[str, int]] = set()
    for item in snapshot:
        key = (item.event_id, item.horizon_bars)
        if key in unique_keys:
            raise DirectionalAgreementAuditErrorV2(
                "duplicate event/horizon outcome is not permitted"
            )
        unique_keys.add(key)
        by_event[item.event_id].append(item)

    expected_horizons = set(DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2)
    event_id_by_identity: dict[tuple[object, ...], str] = {}
    for event_id, event_rows in by_event.items():
        if {item.horizon_bars for item in event_rows} != expected_horizons:
            raise DirectionalAgreementAuditErrorV2(
                f"event {event_id} does not contain every frozen horizon"
            )
        identity = {
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
        if len(identity) != 1:
            raise DirectionalAgreementAuditErrorV2(
                f"event {event_id} changes identity across horizons"
            )
        event_identity = next(iter(identity))
        prior_event_id = event_id_by_identity.setdefault(event_identity, event_id)
        if prior_event_id != event_id:
            raise DirectionalAgreementAuditErrorV2(
                "two event IDs claim the same deterministic panel identity"
            )

    grouped: dict[
        tuple[int, DirectionalAgreementSideV2, DirectionalAgreementBucketV2],
        list[DirectionalAgreementOutcomeV2],
    ] = defaultdict(list)
    for item in snapshot:
        grouped[(item.horizon_bars, item.side, item.bucket)].append(item)

    summaries = tuple(
        _summarize(horizon, side, bucket, grouped.get((horizon, side, bucket), ()))
        for horizon in DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2
        for side in DirectionalAgreementSideV2
        for bucket in DirectionalAgreementBucketV2
    )
    summary_by_key = {
        (item.horizon_bars, item.side, item.bucket): item for item in summaries
    }
    contrasts: list[DirectionalAgreementContrastV2] = []
    for horizon in DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2:
        for side in DirectionalAgreementSideV2:
            tilt = summary_by_key[
                (horizon, side, DirectionalAgreementBucketV2.TILT_2_OF_3)
            ]
            broad = summary_by_key[
                (horizon, side, DirectionalAgreementBucketV2.BROAD_3_OF_3)
            ]
            if tilt.mean_net_return_micros is None or broad.mean_net_return_micros is None:
                difference = None
                point_monotonic = None
            else:
                difference = broad.mean_net_return_micros - tilt.mean_net_return_micros
                point_monotonic = difference > 0
            contrasts.append(
                DirectionalAgreementContrastV2(
                    horizon_bars=horizon,
                    side=side,
                    broad_minus_tilt_mean_net_return_micros=difference,
                    point_monotonic=point_monotonic,
                )
            )

    return DirectionalAgreementAuditV2(
        audit_version=DIRECTIONAL_AGREEMENT_AUDIT_VERSION_V2,
        evidence_rule_version=DIRECTIONAL_EVIDENCE_RULE_VERSION_V2,
        outcome_protocol_version=DIRECTIONAL_AGREEMENT_OUTCOME_PROTOCOL_V2,
        execution_contract_sha256s=tuple(
            sorted({item.execution_contract_sha256 for item in snapshot})
        ),
        horizons_bars=DIRECTIONAL_AGREEMENT_HORIZONS_BARS_V2,
        event_count=len(by_event),
        outcome_count=len(snapshot),
        summaries=summaries,
        contrasts=tuple(contrasts),
    )


def _classify_state(
    state_class: DirectionalStateClassV2,
) -> tuple[DirectionalAgreementBucketV2, DirectionalAgreementSideV2]:
    mapping = {
        DirectionalStateClassV2.BULLISH_STATE_TILT: (
            DirectionalAgreementBucketV2.TILT_2_OF_3,
            DirectionalAgreementSideV2.BULLISH,
        ),
        DirectionalStateClassV2.BROAD_BULLISH_STATE: (
            DirectionalAgreementBucketV2.BROAD_3_OF_3,
            DirectionalAgreementSideV2.BULLISH,
        ),
        DirectionalStateClassV2.BEARISH_STATE_TILT: (
            DirectionalAgreementBucketV2.TILT_2_OF_3,
            DirectionalAgreementSideV2.BEARISH,
        ),
        DirectionalStateClassV2.BROAD_BEARISH_STATE: (
            DirectionalAgreementBucketV2.BROAD_3_OF_3,
            DirectionalAgreementSideV2.BEARISH,
        ),
    }
    try:
        return mapping[state_class]
    except KeyError as exc:
        raise DirectionalAgreementAuditErrorV2(
            "only frozen 2-of-3 and 3-of-3 directional states are auditable"
        ) from exc


def _summarize(
    horizon: int,
    side: DirectionalAgreementSideV2,
    bucket: DirectionalAgreementBucketV2,
    rows: Sequence[DirectionalAgreementOutcomeV2],
) -> DirectionalAgreementSummaryV2:
    values = [
        item.net_return_micros
        for item in rows
        if item.evaluable and item.net_return_micros is not None
    ]
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if not values:
        profit_factor = None
        profit_factor_state = ProfitFactorStateV2.NO_EVALUABLE_ROWS
    elif losses > 0:
        profit_factor = _round_ratio(gains * _MICROS, losses)
        profit_factor_state = ProfitFactorStateV2.FINITE
    elif gains > 0:
        profit_factor = None
        profit_factor_state = ProfitFactorStateV2.POSITIVE_WITH_NO_LOSSES
    else:
        profit_factor = None
        profit_factor_state = ProfitFactorStateV2.ZERO_GROSS_WITH_NO_LOSSES
    return DirectionalAgreementSummaryV2(
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
        mean_net_return_micros=(
            _round_ratio(sum(values), len(values)) if values else None
        ),
        median_net_return_micros=(_integer_median(values) if values else None),
        profit_factor_micros=profit_factor,
        profit_factor_state=profit_factor_state,
    )


def _round_ratio(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise DirectionalAgreementAuditErrorV2("ratio denominator must be positive")
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    if 2 * remainder >= denominator:
        quotient += 1
    return sign * quotient


def _integer_median(values: Sequence[int]) -> int:
    if not values:
        raise DirectionalAgreementAuditErrorV2("median requires at least one value")
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return _round_ratio(ordered[midpoint - 1] + ordered[midpoint], 2)
