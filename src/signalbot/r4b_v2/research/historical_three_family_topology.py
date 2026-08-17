"""Outcome-blind topology projection for the frozen three-family consensus.

This module does not alter the source consensus, its event identity, or its
admission decision.  It gives every exact three-leaf sign pattern one explicit
name so a later audit cannot silently merge clean ``2 + neutral`` agreement
with a conflicted ``2 versus 1`` majority.
"""

from __future__ import annotations

import hashlib
from dataclasses import InitVar, dataclass, field
from enum import StrEnum
from typing import Final, Literal

from signalbot.domain.enums import Direction
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.strategy.historical_three_family_consensus import (
    HistoricalConsensusStatusV2,
    HistoricalFamilyV2,
    HistoricalLeafStatusV2,
    HistoricalThreeFamilyConsensusContractErrorV2,
    HistoricalThreeFamilyConsensusV2,
    canonical_historical_three_family_consensus_v2,
)

HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.1_HISTORICAL_THREE_FAMILY_TOPOLOGY_V1_FROZEN"
)
HISTORICAL_THREE_FAMILY_TOPOLOGY_ROLE_V2: Final = (
    "HISTORICAL_ONLY_OUTCOME_BLIND_THREE_FAMILY_TOPOLOGY"
)

_SCHEMA_VERSION: Final = "r4b_historical_three_family_topology_v2"
_SOURCE_CONSENSUS_RULE_VERSION: Final = (
    "R4B_CAUSAL_V2.4.1_HISTORICAL_THREE_FAMILY_CONSENSUS_V1_FROZEN"
)
_TOPOLOGY_DOMAIN: Final = b"R4B_HISTORICAL_THREE_FAMILY_TOPOLOGY_V2\0"
_FACTORY_TOKEN: Final = object()


class HistoricalThreeFamilyTopologyErrorV2(ValueError):
    """Raised when a topology projection or its source consensus is invalid."""


class HistoricalThreeFamilyTopologyClassV2(StrEnum):
    """Exhaustive sign topology for three READY leaves, plus WITHHELD."""

    UNANIMOUS_BULLISH_3_0_0 = "UNANIMOUS_BULLISH_3_0_0"
    UNANIMOUS_BEARISH_0_3_0 = "UNANIMOUS_BEARISH_0_3_0"
    CLEAN_BULLISH_2_0_1 = "CLEAN_BULLISH_2_0_1"
    CLEAN_BEARISH_0_2_1 = "CLEAN_BEARISH_0_2_1"
    CONFLICTED_BULLISH_2_1_0 = "CONFLICTED_BULLISH_2_1_0"
    CONFLICTED_BEARISH_1_2_0 = "CONFLICTED_BEARISH_1_2_0"
    LONE_BULLISH_1_0_2 = "LONE_BULLISH_1_0_2"
    LONE_BEARISH_0_1_2 = "LONE_BEARISH_0_1_2"
    BALANCED_1_1_1 = "BALANCED_1_1_1"
    ALL_NEUTRAL_0_0_3 = "ALL_NEUTRAL_0_0_3"
    WITHHELD = "WITHHELD"


class HistoricalThreeFamilyMajorityDirectionV2(StrEnum):
    """Strict sign-count majority direction when at least two leaves agree."""

    BULLISH = "BULLISH"
    BEARISH = "BEARISH"


class HistoricalThreeFamilyComparisonBucketV2(StrEnum):
    """Pre-outcome comparison bucket without side or efficacy semantics."""

    BROAD_3_OF_3 = "BROAD_3_OF_3"
    CLEAN_2_PLUS_NEUTRAL = "CLEAN_2_PLUS_NEUTRAL"
    CONFLICTED_2_VS_1 = "CONFLICTED_2_VS_1"
    NOT_COMPARABLE = "NOT_COMPARABLE"
    WITHHELD = "WITHHELD"


class HistoricalThreeFamilyDisplayGradeV2(StrEnum):
    """Uncalibrated display label; no member is a success probability."""

    UNANIMOUS_BREADTH_UNCALIBRATED = "UNANIMOUS_BREADTH_UNCALIBRATED"
    CLEAN_TWO_FAMILY_BREADTH_UNCALIBRATED = (
        "CLEAN_TWO_FAMILY_BREADTH_UNCALIBRATED"
    )
    CONFLICTED_MAJORITY_UNCALIBRATED = "CONFLICTED_MAJORITY_UNCALIBRATED"
    INSUFFICIENT_DIRECTIONAL_BREADTH = "INSUFFICIENT_DIRECTIONAL_BREADTH"
    NO_DIRECTIONAL_CONSENSUS = "NO_DIRECTIONAL_CONSENSUS"
    WITHHELD_DATA = "WITHHELD_DATA"


@dataclass(frozen=True, slots=True)
class HistoricalThreeFamilyTopologyV2:
    """Factory-sealed, canonical topology derived from one exact consensus."""

    source_consensus: HistoricalThreeFamilyConsensusV2 = field(repr=False)
    source_consensus_canonical_sha256: str
    topology: HistoricalThreeFamilyTopologyClassV2
    comparison_bucket: HistoricalThreeFamilyComparisonBucketV2
    display_grade: HistoricalThreeFamilyDisplayGradeV2
    ready_family_count: int
    unavailable_family_count: int
    bullish_family_count: int | None
    bearish_family_count: int | None
    neutral_family_count: int | None
    majority_direction: HistoricalThreeFamilyMajorityDirectionV2 | None
    majority_family_count: int | None
    opposing_family_count: int | None
    has_opposition: bool | None
    primary_support_count: int | None
    primary_oppose_count: int | None
    primary_neutral_count: int | None
    clean_primary_audit_eligible: bool
    conflicted_comparator_eligible: bool
    _factory_token: InitVar[object | None] = None
    topology_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_SCHEMA_VERSION)
    rule_version: str = field(
        init=False,
        default=HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2,
    )
    role: str = field(
        init=False,
        default=HISTORICAL_THREE_FAMILY_TOPOLOGY_ROLE_V2,
    )
    historical_only: Literal[True] = field(init=False, default=True)
    topology_derivation_only: Literal[True] = field(init=False, default=True)
    outcome_data_used: Literal[False] = field(init=False, default=False)
    promoting: Literal[False] = field(init=False, default=False)
    probability: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    order_instruction: Literal[False] = field(init=False, default=False)
    changes_consensus_decision: Literal[False] = field(init=False, default=False)
    conflicted_comparator_outcome_authorized: Literal[False] = field(
        init=False,
        default=False,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise HistoricalThreeFamilyTopologyErrorV2(
                "historical topology values require their frozen derivation factory"
            )
        _validate_topology(self, require_hash=False)
        object.__setattr__(
            self,
            "topology_sha256",
            hashlib.sha256(
                _TOPOLOGY_DOMAIN + canonical_json_line(_topology_document(self, False))
            ).hexdigest(),
        )

    @property
    def source_event_id(self) -> str:
        """Return the deterministic source consensus event identity."""

        return self.source_consensus.event_id

    @property
    def source_payload_sha256(self) -> str:
        """Return the source consensus payload identity."""

        return self.source_consensus.payload_sha256

    @property
    def source_anchor_sha256(self) -> str:
        """Return the authenticated source recommendation anchor identity."""

        return self.source_consensus.anchor.anchor_sha256


@dataclass(frozen=True, slots=True)
class _DerivedTopologyV2:
    topology: HistoricalThreeFamilyTopologyClassV2
    comparison_bucket: HistoricalThreeFamilyComparisonBucketV2
    display_grade: HistoricalThreeFamilyDisplayGradeV2
    ready_family_count: int
    unavailable_family_count: int
    bullish_family_count: int | None
    bearish_family_count: int | None
    neutral_family_count: int | None
    majority_direction: HistoricalThreeFamilyMajorityDirectionV2 | None
    majority_family_count: int | None
    opposing_family_count: int | None
    has_opposition: bool | None
    primary_support_count: int | None
    primary_oppose_count: int | None
    primary_neutral_count: int | None
    clean_primary_audit_eligible: bool
    conflicted_comparator_eligible: bool


def derive_historical_three_family_topology_v2(
    source_consensus: HistoricalThreeFamilyConsensusV2,
) -> HistoricalThreeFamilyTopologyV2:
    """Derive an exhaustive topology without reading or accepting outcomes."""

    canonical_source = _canonical_source(source_consensus)
    derived = _derive(source_consensus)
    return HistoricalThreeFamilyTopologyV2(
        source_consensus=source_consensus,
        source_consensus_canonical_sha256=hashlib.sha256(canonical_source).hexdigest(),
        topology=derived.topology,
        comparison_bucket=derived.comparison_bucket,
        display_grade=derived.display_grade,
        ready_family_count=derived.ready_family_count,
        unavailable_family_count=derived.unavailable_family_count,
        bullish_family_count=derived.bullish_family_count,
        bearish_family_count=derived.bearish_family_count,
        neutral_family_count=derived.neutral_family_count,
        majority_direction=derived.majority_direction,
        majority_family_count=derived.majority_family_count,
        opposing_family_count=derived.opposing_family_count,
        has_opposition=derived.has_opposition,
        primary_support_count=derived.primary_support_count,
        primary_oppose_count=derived.primary_oppose_count,
        primary_neutral_count=derived.primary_neutral_count,
        clean_primary_audit_eligible=derived.clean_primary_audit_eligible,
        conflicted_comparator_eligible=derived.conflicted_comparator_eligible,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_historical_three_family_topology_v2(
    value: HistoricalThreeFamilyTopologyV2,
) -> bytes:
    """Revalidate and canonically serialize one topology projection."""

    if type(value) is not HistoricalThreeFamilyTopologyV2:
        raise HistoricalThreeFamilyTopologyErrorV2(
            "value must be an exact HistoricalThreeFamilyTopologyV2"
        )
    _validate_topology(value, require_hash=True)
    return canonical_json_line(_topology_document(value, True))


def _canonical_source(value: object) -> bytes:
    if type(value) is not HistoricalThreeFamilyConsensusV2:
        raise HistoricalThreeFamilyTopologyErrorV2(
            "source_consensus must be an exact HistoricalThreeFamilyConsensusV2"
        )
    try:
        canonical = canonical_historical_three_family_consensus_v2(value)
    except HistoricalThreeFamilyConsensusContractErrorV2 as exc:
        raise HistoricalThreeFamilyTopologyErrorV2(
            "source consensus failed canonical validation"
        ) from exc
    if value.rule_version != _SOURCE_CONSENSUS_RULE_VERSION:
        raise HistoricalThreeFamilyTopologyErrorV2(
            "source consensus rule version differs from the frozen topology input"
        )
    return canonical


def _derive(value: HistoricalThreeFamilyConsensusV2) -> _DerivedTopologyV2:
    leaves_by_family = {leaf.family: leaf for leaf in value.leaves}
    if set(leaves_by_family) != set(HistoricalFamilyV2) or len(value.leaves) != 3:
        raise HistoricalThreeFamilyTopologyErrorV2(
            "source consensus must contain the exact three named families"
        )
    leaves = tuple(leaves_by_family[family] for family in HistoricalFamilyV2)
    ready_count = sum(leaf.status is HistoricalLeafStatusV2.READY for leaf in leaves)
    if ready_count != 3:
        if value.status is HistoricalConsensusStatusV2.READY:
            raise HistoricalThreeFamilyTopologyErrorV2(
                "READY source consensus cannot contain an unavailable leaf"
            )
        return _withheld(ready_count)
    if value.status is not HistoricalConsensusStatusV2.READY:
        raise HistoricalThreeFamilyTopologyErrorV2(
            "three READY leaves require a READY source consensus"
        )

    directions: tuple[int, ...] = tuple(_ready_direction(leaf.direction) for leaf in leaves)
    bullish = directions.count(1)
    bearish = directions.count(-1)
    neutral = directions.count(0)
    topology = _ready_topology(bullish, bearish, neutral)
    comparison_bucket, display_grade = _labels(topology)
    majority_direction, majority_count, opposing_count = _majority_counts(
        bullish,
        bearish,
    )
    has_opposition = bullish > 0 and bearish > 0
    if value.anchor.primary_direction is Direction.LONG:
        support, oppose = bullish, bearish
    elif value.anchor.primary_direction is Direction.SHORT:
        support, oppose = bearish, bullish
    else:  # pragma: no cover - canonical source validation owns this enum boundary.
        raise HistoricalThreeFamilyTopologyErrorV2(
            "source primary direction is outside LONG/SHORT"
        )
    clean_eligible = (
        comparison_bucket
        in {
            HistoricalThreeFamilyComparisonBucketV2.BROAD_3_OF_3,
            HistoricalThreeFamilyComparisonBucketV2.CLEAN_2_PLUS_NEUTRAL,
        }
        and support >= 2
        and oppose == 0
    )
    if clean_eligible is not value.admitted:
        raise HistoricalThreeFamilyTopologyErrorV2(
            "derived clean-primary eligibility differs from frozen admission"
        )
    conflicted_eligible = (
        comparison_bucket
        is HistoricalThreeFamilyComparisonBucketV2.CONFLICTED_2_VS_1
        and support == 2
        and oppose == 1
    )
    return _DerivedTopologyV2(
        topology=topology,
        comparison_bucket=comparison_bucket,
        display_grade=display_grade,
        ready_family_count=3,
        unavailable_family_count=0,
        bullish_family_count=bullish,
        bearish_family_count=bearish,
        neutral_family_count=neutral,
        majority_direction=majority_direction,
        majority_family_count=majority_count,
        opposing_family_count=opposing_count,
        has_opposition=has_opposition,
        primary_support_count=support,
        primary_oppose_count=oppose,
        primary_neutral_count=neutral,
        clean_primary_audit_eligible=clean_eligible,
        conflicted_comparator_eligible=conflicted_eligible,
    )


def _withheld(ready_count: int) -> _DerivedTopologyV2:
    return _DerivedTopologyV2(
        topology=HistoricalThreeFamilyTopologyClassV2.WITHHELD,
        comparison_bucket=HistoricalThreeFamilyComparisonBucketV2.WITHHELD,
        display_grade=HistoricalThreeFamilyDisplayGradeV2.WITHHELD_DATA,
        ready_family_count=ready_count,
        unavailable_family_count=3 - ready_count,
        bullish_family_count=None,
        bearish_family_count=None,
        neutral_family_count=None,
        majority_direction=None,
        majority_family_count=None,
        opposing_family_count=None,
        has_opposition=None,
        primary_support_count=None,
        primary_oppose_count=None,
        primary_neutral_count=None,
        clean_primary_audit_eligible=False,
        conflicted_comparator_eligible=False,
    )


def _ready_direction(value: object) -> int:
    if type(value) is not int or value not in (-1, 0, 1):
        raise HistoricalThreeFamilyTopologyErrorV2(
            "READY topology leaf direction must be exactly -1, 0, or 1"
        )
    return value


def _ready_topology(
    bullish: int,
    bearish: int,
    neutral: int,
) -> HistoricalThreeFamilyTopologyClassV2:
    mapping = {
        (3, 0, 0): HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BULLISH_3_0_0,
        (0, 3, 0): HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BEARISH_0_3_0,
        (2, 0, 1): HistoricalThreeFamilyTopologyClassV2.CLEAN_BULLISH_2_0_1,
        (0, 2, 1): HistoricalThreeFamilyTopologyClassV2.CLEAN_BEARISH_0_2_1,
        (2, 1, 0): HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BULLISH_2_1_0,
        (1, 2, 0): HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BEARISH_1_2_0,
        (1, 0, 2): HistoricalThreeFamilyTopologyClassV2.LONE_BULLISH_1_0_2,
        (0, 1, 2): HistoricalThreeFamilyTopologyClassV2.LONE_BEARISH_0_1_2,
        (1, 1, 1): HistoricalThreeFamilyTopologyClassV2.BALANCED_1_1_1,
        (0, 0, 3): HistoricalThreeFamilyTopologyClassV2.ALL_NEUTRAL_0_0_3,
    }
    try:
        return mapping[(bullish, bearish, neutral)]
    except KeyError as exc:  # pragma: no cover - guarded by three validated signs.
        raise HistoricalThreeFamilyTopologyErrorV2(
            "READY leaf counts do not form an exhaustive three-family topology"
        ) from exc


def _labels(
    topology: HistoricalThreeFamilyTopologyClassV2,
) -> tuple[
    HistoricalThreeFamilyComparisonBucketV2,
    HistoricalThreeFamilyDisplayGradeV2,
]:
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BULLISH_3_0_0,
        HistoricalThreeFamilyTopologyClassV2.UNANIMOUS_BEARISH_0_3_0,
    }:
        return (
            HistoricalThreeFamilyComparisonBucketV2.BROAD_3_OF_3,
            HistoricalThreeFamilyDisplayGradeV2.UNANIMOUS_BREADTH_UNCALIBRATED,
        )
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.CLEAN_BULLISH_2_0_1,
        HistoricalThreeFamilyTopologyClassV2.CLEAN_BEARISH_0_2_1,
    }:
        return (
            HistoricalThreeFamilyComparisonBucketV2.CLEAN_2_PLUS_NEUTRAL,
            HistoricalThreeFamilyDisplayGradeV2.CLEAN_TWO_FAMILY_BREADTH_UNCALIBRATED,
        )
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BULLISH_2_1_0,
        HistoricalThreeFamilyTopologyClassV2.CONFLICTED_BEARISH_1_2_0,
    }:
        return (
            HistoricalThreeFamilyComparisonBucketV2.CONFLICTED_2_VS_1,
            HistoricalThreeFamilyDisplayGradeV2.CONFLICTED_MAJORITY_UNCALIBRATED,
        )
    if topology in {
        HistoricalThreeFamilyTopologyClassV2.LONE_BULLISH_1_0_2,
        HistoricalThreeFamilyTopologyClassV2.LONE_BEARISH_0_1_2,
    }:
        return (
            HistoricalThreeFamilyComparisonBucketV2.NOT_COMPARABLE,
            HistoricalThreeFamilyDisplayGradeV2.INSUFFICIENT_DIRECTIONAL_BREADTH,
        )
    return (
        HistoricalThreeFamilyComparisonBucketV2.NOT_COMPARABLE,
        HistoricalThreeFamilyDisplayGradeV2.NO_DIRECTIONAL_CONSENSUS,
    )


def _majority_counts(
    bullish: int,
    bearish: int,
) -> tuple[HistoricalThreeFamilyMajorityDirectionV2 | None, int | None, int | None]:
    if bullish >= 2:
        return HistoricalThreeFamilyMajorityDirectionV2.BULLISH, bullish, bearish
    if bearish >= 2:
        return HistoricalThreeFamilyMajorityDirectionV2.BEARISH, bearish, bullish
    return None, None, None


def _validate_topology(
    value: HistoricalThreeFamilyTopologyV2,
    *,
    require_hash: bool,
) -> None:
    canonical_source = _canonical_source(value.source_consensus)
    expected_source_hash = hashlib.sha256(canonical_source).hexdigest()
    if value.source_consensus_canonical_sha256 != expected_source_hash:
        raise HistoricalThreeFamilyTopologyErrorV2(
            "source consensus canonical hash differs"
        )
    if (
        value.schema_version != _SCHEMA_VERSION
        or value.rule_version != HISTORICAL_THREE_FAMILY_TOPOLOGY_RULE_VERSION_V2
        or value.role != HISTORICAL_THREE_FAMILY_TOPOLOGY_ROLE_V2
        or value.historical_only is not True
        or value.topology_derivation_only is not True
        or value.outcome_data_used is not False
        or value.promoting is not False
        or value.probability is not False
        or value.probability_calibrated is not False
        or value.order_instruction is not False
        or value.changes_consensus_decision is not False
        or value.conflicted_comparator_outcome_authorized is not False
    ):
        raise HistoricalThreeFamilyTopologyErrorV2(
            "topology authority, role, version, or fixed claims differ"
        )
    expected = _derive(value.source_consensus)
    for field_name in _DERIVED_FIELD_NAMES:
        if getattr(value, field_name) != getattr(expected, field_name):
            raise HistoricalThreeFamilyTopologyErrorV2(
                f"topology field {field_name} differs from the exact source leaves"
            )
    if require_hash:
        expected_hash = hashlib.sha256(
            _TOPOLOGY_DOMAIN + canonical_json_line(_topology_document(value, False))
        ).hexdigest()
        if value.topology_sha256 != expected_hash:
            raise HistoricalThreeFamilyTopologyErrorV2(
                "topology hash differs from canonical content"
            )


_DERIVED_FIELD_NAMES: Final = (
    "topology",
    "comparison_bucket",
    "display_grade",
    "ready_family_count",
    "unavailable_family_count",
    "bullish_family_count",
    "bearish_family_count",
    "neutral_family_count",
    "majority_direction",
    "majority_family_count",
    "opposing_family_count",
    "has_opposition",
    "primary_support_count",
    "primary_oppose_count",
    "primary_neutral_count",
    "clean_primary_audit_eligible",
    "conflicted_comparator_eligible",
)


def _topology_document(
    value: HistoricalThreeFamilyTopologyV2,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "bearish_family_count": value.bearish_family_count,
        "bullish_family_count": value.bullish_family_count,
        "changes_consensus_decision": value.changes_consensus_decision,
        "clean_primary_audit_eligible": value.clean_primary_audit_eligible,
        "comparison_bucket": value.comparison_bucket.value,
        "conflicted_comparator_eligible": value.conflicted_comparator_eligible,
        "conflicted_comparator_outcome_authorized": (
            value.conflicted_comparator_outcome_authorized
        ),
        "display_grade": value.display_grade.value,
        "has_opposition": value.has_opposition,
        "historical_only": value.historical_only,
        "leaf_states": [
            {
                "direction": leaf.direction,
                "family": leaf.family.value,
                "leaf_sha256": leaf.leaf_sha256,
                "status": leaf.status.value,
                "strength_micros": leaf.strength_micros,
            }
            for leaf in value.source_consensus.leaves
        ],
        "majority_direction": (
            None if value.majority_direction is None else value.majority_direction.value
        ),
        "majority_family_count": value.majority_family_count,
        "neutral_family_count": value.neutral_family_count,
        "opposing_family_count": value.opposing_family_count,
        "order_instruction": value.order_instruction,
        "outcome_data_used": value.outcome_data_used,
        "primary_neutral_count": value.primary_neutral_count,
        "primary_oppose_count": value.primary_oppose_count,
        "primary_support_count": value.primary_support_count,
        "probability": value.probability,
        "probability_calibrated": value.probability_calibrated,
        "promoting": value.promoting,
        "ready_family_count": value.ready_family_count,
        "role": value.role,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "source_anchor_sha256": value.source_anchor_sha256,
        "source_consensus_admitted": value.source_consensus.admitted,
        "source_consensus_canonical_sha256": (
            value.source_consensus_canonical_sha256
        ),
        "source_consensus_event_id": value.source_event_id,
        "source_consensus_payload_sha256": value.source_payload_sha256,
        "source_consensus_rule_version": value.source_consensus.rule_version,
        "source_consensus_status": value.source_consensus.status.value,
        "source_primary_direction": value.source_consensus.anchor.primary_direction.value,
        "topology": value.topology.value,
        "topology_derivation_only": value.topology_derivation_only,
        "unavailable_family_count": value.unavailable_family_count,
    }
    if include_hash:
        document["topology_sha256"] = value.topology_sha256
    return document
