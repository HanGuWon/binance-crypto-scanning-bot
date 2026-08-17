from __future__ import annotations

import hashlib
from dataclasses import InitVar, dataclass, field, replace
from enum import StrEnum
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.strategy.evidence_producer import (
    EvidenceFamilyObservationV2,
    EvidenceInformationFamilyV2,
    EvidenceProducerContractErrorV2,
    EvidenceReadinessV2,
    evidence_family_observation_document_v2,
)
from signalbot.r4b_v2.strategy.evidence_score import EvidenceScoreInputV2

DIRECTIONAL_EVIDENCE_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.0_DIRECTIONAL_EVIDENCE_PANEL_V1_SHADOW_NONPROMOTING"
)
DIRECTIONAL_EVIDENCE_ROLE_V2: Final = "NON_PROMOTING_SHADOW_DIRECTIONAL_STATE"

_EVENT_ID_DOMAIN: Final = b"R4B_DIRECTIONAL_EVIDENCE_PANEL_ID_V2\0"
_PAYLOAD_DOMAIN: Final = b"R4B_DIRECTIONAL_EVIDENCE_PANEL_PAYLOAD_V2\0"
_DECISION_FACTORY_TOKEN: Final = object()
_FIXED_INVALIDATION: Final = (
    "SHADOW_DIRECTIONAL_STATE_CANNOT_OPEN_CLOSE_FILTER_OR_RANK_A_B_C_POSITIONS"
)
_BOOK_PRESSURE_STATUS: Final = "NOT_CONNECTED_SHADOW_CANDIDATE"
_SOURCE_AUTHORITY_STATUS: Final = "LEGACY_OBSERVATIONS_M0_M1_M2_UNBOUND"

_DIRECTIONAL_FAMILY_ORDER: Final = (
    EvidenceInformationFamilyV2.PRICE_STRUCTURE_MOMENTUM,
    EvidenceInformationFamilyV2.PARTICIPATION_FLOW,
    EvidenceInformationFamilyV2.CROSS_SECTIONAL_CONTEXT,
)
_CONTEXT_FAMILY_ORDER: Final = (
    EvidenceInformationFamilyV2.VOLATILITY_REGIME,
    EvidenceInformationFamilyV2.DERIVATIVES_POSITIONING,
    EvidenceInformationFamilyV2.LIQUIDITY_EXECUTION,
)


class DirectionalEvidenceContractErrorV2(ValueError):
    """Raised when a successor directional panel violates its frozen contract."""


class DirectionalStateClassV2(StrEnum):
    WITHHELD = "WITHHELD"
    BROAD_BULLISH_STATE = "BROAD_BULLISH_STATE"
    BULLISH_STATE_TILT = "BULLISH_STATE_TILT"
    BROAD_BEARISH_STATE = "BROAD_BEARISH_STATE"
    BEARISH_STATE_TILT = "BEARISH_STATE_TILT"
    MIXED_OR_NEUTRAL_STATE = "MIXED_OR_NEUTRAL_STATE"


class DirectionalEvidenceRegistryDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


class PrimaryBindingStatusV2(StrEnum):
    UNAVAILABLE = "PRIMARY_BINDING_UNAVAILABLE"


class EvidencePanelRoleV2(StrEnum):
    DIRECTIONAL_STATE = "DIRECTIONAL_STATE"
    CONTEXT_ONLY_DIRECTION_IGNORED = "CONTEXT_ONLY_DIRECTION_IGNORED"


class PrimaryEvidenceRelationshipV2(StrEnum):
    PRIMARY_BINDING_UNAVAILABLE = "PRIMARY_BINDING_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class _DerivedDirectionalPanelV2:
    status: EvidenceReadinessV2
    state_class: DirectionalStateClassV2
    directional_numerator_micros: int | None
    directional_denominator: int | None
    directional_agreement_micros: int | None
    bullish_family_count: int | None
    bearish_family_count: int | None
    neutral_family_count: int | None
    context_unavailable_families: tuple[EvidenceInformationFamilyV2, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DirectionalEvidencePanelDecisionV2:
    """Factory-built descriptive agreement across three non-duplicated families."""

    source_input: EvidenceScoreInputV2 = field(repr=False)
    status: EvidenceReadinessV2
    state_class: DirectionalStateClassV2
    directional_numerator_micros: int | None
    directional_denominator: int | None
    directional_agreement_micros: int | None
    bullish_family_count: int | None
    bearish_family_count: int | None
    neutral_family_count: int | None
    context_unavailable_families: tuple[EvidenceInformationFamilyV2, ...]
    reasons: tuple[str, ...]
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    role: str = field(init=False, default=DIRECTIONAL_EVIDENCE_ROLE_V2)
    rule_version: str = field(
        init=False,
        default=DIRECTIONAL_EVIDENCE_RULE_VERSION_V2,
    )
    promoting: bool = field(init=False, default=False)
    changes_primary_decision: bool = field(init=False, default=False)
    probability_calibrated: bool = field(init=False, default=False)
    invalidation: str = field(init=False, default=_FIXED_INVALIDATION)
    book_pressure_status: str = field(init=False, default=_BOOK_PRESSURE_STATUS)
    source_authority_status: str = field(
        init=False,
        default=_SOURCE_AUTHORITY_STATUS,
    )
    primary_binding_status: PrimaryBindingStatusV2 = field(
        init=False,
        default=PrimaryBindingStatusV2.UNAVAILABLE,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _DECISION_FACTORY_TOKEN:
            raise DirectionalEvidenceContractErrorV2(
                "directional panel decisions require the frozen evaluator"
            )
        if not isinstance(self.source_input, EvidenceScoreInputV2):
            raise DirectionalEvidenceContractErrorV2(
                "source_input must be an EvidenceScoreInputV2"
            )
        if self.source_input.observations != tuple(
            sorted(
                self.source_input.observations,
                key=lambda value: value.family.value,
            )
        ):
            raise DirectionalEvidenceContractErrorV2(
                "source observations must use canonical family order"
            )
        expected = _derive_directional_panel(self.source_input)
        observed = (
            self.status,
            self.state_class,
            self.directional_numerator_micros,
            self.directional_denominator,
            self.directional_agreement_micros,
            self.bullish_family_count,
            self.bearish_family_count,
            self.neutral_family_count,
            self.context_unavailable_families,
            self.reasons,
        )
        derived = (
            expected.status,
            expected.state_class,
            expected.directional_numerator_micros,
            expected.directional_denominator,
            expected.directional_agreement_micros,
            expected.bullish_family_count,
            expected.bearish_family_count,
            expected.neutral_family_count,
            expected.context_unavailable_families,
            expected.reasons,
        )
        if observed != derived:
            raise DirectionalEvidenceContractErrorV2(
                "directional panel fields contradict the bound source observations"
            )
        identity = {
            "attempt_id": self.attempt_id,
            "bar_close_ms": self.bar_close_ms,
            "bar_open_ms": self.bar_open_ms,
            "decision_cutoff_ms": self.decision_cutoff_ms,
            "primary_binding_status": self.primary_binding_status.value,
            "promoting_plan_sha256": self.promoting_plan_sha256,
            "role": self.role,
            "rule_version": self.rule_version,
            "symbol": self.symbol,
            "venue": self.venue.value,
        }
        object.__setattr__(
            self,
            "event_id",
            hashlib.sha256(_EVENT_ID_DOMAIN + canonical_json_line(identity)).hexdigest(),
        )
        object.__setattr__(
            self,
            "payload_sha256",
            hashlib.sha256(
                _PAYLOAD_DOMAIN
                + canonical_json_line(
                    _decision_document(self, include_payload_hash=False)
                )
            ).hexdigest(),
        )

    @property
    def ready(self) -> bool:
        return self.status is EvidenceReadinessV2.READY

    @property
    def attempt_id(self) -> str:
        return self.source_input.attempt_id

    @property
    def symbol(self) -> str:
        return self.source_input.symbol

    @property
    def venue(self) -> VenueV2:
        return self.source_input.venue

    @property
    def promoting_plan_sha256(self) -> str:
        return self.source_input.promoting_plan_sha256

    @property
    def bar_open_ms(self) -> int:
        return self.source_input.bar_open_ms

    @property
    def bar_close_ms(self) -> int:
        return self.source_input.bar_close_ms

    @property
    def decision_cutoff_ms(self) -> int:
        return self.source_input.decision_cutoff_ms

    @property
    def directional_observations(self) -> tuple[EvidenceFamilyObservationV2, ...]:
        return _observations_in_order(
            self.source_input.observations,
            _DIRECTIONAL_FAMILY_ORDER,
        )

    @property
    def context_observations(self) -> tuple[EvidenceFamilyObservationV2, ...]:
        return _observations_in_order(
            self.source_input.observations,
            _CONTEXT_FAMILY_ORDER,
        )

    def primary_relationship(
        self,
        family: EvidenceInformationFamilyV2,
    ) -> PrimaryEvidenceRelationshipV2:
        if not isinstance(family, EvidenceInformationFamilyV2):
            raise DirectionalEvidenceContractErrorV2(
                "family must be an EvidenceInformationFamilyV2"
            )
        return PrimaryEvidenceRelationshipV2.PRIMARY_BINDING_UNAVAILABLE

    def effective_readiness(
        self,
        family: EvidenceInformationFamilyV2,
    ) -> EvidenceReadinessV2:
        if not isinstance(family, EvidenceInformationFamilyV2):
            raise DirectionalEvidenceContractErrorV2(
                "family must be an EvidenceInformationFamilyV2"
            )
        observation = next(
            value
            for value in self.source_input.observations
            if value.family is family
        )
        return _effective_readiness(observation, self.source_input)


class DirectionalEvidencePanelRegistryV2:
    """Bounded duplicate/conflict gate for future shadow annotation delivery."""

    def __init__(self, *, maximum_events: int) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise DirectionalEvidenceContractErrorV2(
                "maximum_events must be a positive integer"
            )
        self._maximum_events = maximum_events
        self._payload_by_event_id: dict[str, bytes] = {}

    @property
    def event_count(self) -> int:
        return len(self._payload_by_event_id)

    def register(
        self,
        decision: DirectionalEvidencePanelDecisionV2,
    ) -> DirectionalEvidenceRegistryDispositionV2:
        if not isinstance(decision, DirectionalEvidencePanelDecisionV2):
            raise DirectionalEvidenceContractErrorV2(
                "registry accepts DirectionalEvidencePanelDecisionV2 values only"
            )
        payload = canonical_directional_evidence_panel_v2(decision)
        prior = self._payload_by_event_id.get(decision.event_id)
        if prior is not None:
            if prior != payload:
                raise DirectionalEvidenceContractErrorV2(
                    "deterministic directional event ID collides with different payload"
                )
            return DirectionalEvidenceRegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if len(self._payload_by_event_id) >= self._maximum_events:
            raise DirectionalEvidenceContractErrorV2(
                "bounded directional evidence registry capacity exhausted"
            )
        self._payload_by_event_id[decision.event_id] = payload
        return DirectionalEvidenceRegistryDispositionV2.NEW


def evaluate_directional_evidence_panel_v2(
    item: EvidenceScoreInputV2,
) -> DirectionalEvidencePanelDecisionV2:
    """Build an uncalibrated state panel; context families never cast votes."""

    if not isinstance(item, EvidenceScoreInputV2):
        raise DirectionalEvidenceContractErrorV2(
            "item must be an EvidenceScoreInputV2"
        )
    canonical_input = replace(
        item,
        observations=tuple(
            sorted(item.observations, key=lambda value: value.family.value)
        ),
    )
    derived = _derive_directional_panel(canonical_input)
    return DirectionalEvidencePanelDecisionV2(
        source_input=canonical_input,
        status=derived.status,
        state_class=derived.state_class,
        directional_numerator_micros=derived.directional_numerator_micros,
        directional_denominator=derived.directional_denominator,
        directional_agreement_micros=derived.directional_agreement_micros,
        bullish_family_count=derived.bullish_family_count,
        bearish_family_count=derived.bearish_family_count,
        neutral_family_count=derived.neutral_family_count,
        context_unavailable_families=derived.context_unavailable_families,
        reasons=derived.reasons,
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def canonical_directional_evidence_panel_v2(
    decision: DirectionalEvidencePanelDecisionV2,
) -> bytes:
    """Return canonical self-hash-checked JSONL for one shadow panel."""

    if not isinstance(decision, DirectionalEvidencePanelDecisionV2):
        raise DirectionalEvidenceContractErrorV2(
            "decision must be a DirectionalEvidencePanelDecisionV2"
        )
    expected = hashlib.sha256(
        _PAYLOAD_DOMAIN
        + canonical_json_line(_decision_document(decision, include_payload_hash=False))
    ).hexdigest()
    if decision.payload_sha256 != expected:
        raise DirectionalEvidenceContractErrorV2(
            "directional panel payload hash differs from canonical content"
        )
    return canonical_json_line(_decision_document(decision, include_payload_hash=True))


def _derive_directional_panel(
    item: EvidenceScoreInputV2,
) -> _DerivedDirectionalPanelV2:
    directional = _observations_in_order(
        item.observations,
        _DIRECTIONAL_FAMILY_ORDER,
    )
    context = _observations_in_order(
        item.observations,
        _CONTEXT_FAMILY_ORDER,
    )
    effective_directional = tuple(
        (value, _effective_readiness(value, item)) for value in directional
    )
    effective_context = tuple(
        (value, _effective_readiness(value, item)) for value in context
    )
    context_unavailable = tuple(
        value.family
        for value, status in effective_context
        if status is not EvidenceReadinessV2.READY
    )
    context_reasons = tuple(
        f"CONTEXT_WITHHELD:{value.family.value}:{status.value}"
        for value, status in effective_context
        if status is not EvidenceReadinessV2.READY
    )
    for status in (
        EvidenceReadinessV2.DATA_INVALID,
        EvidenceReadinessV2.INCONCLUSIVE_DATA,
        EvidenceReadinessV2.FEATURE_NOT_READY,
    ):
        affected = tuple(
            value.family.value
            for value, effective in effective_directional
            if effective is status
        )
        if affected:
            return _DerivedDirectionalPanelV2(
                status=status,
                state_class=DirectionalStateClassV2.WITHHELD,
                directional_numerator_micros=None,
                directional_denominator=None,
                directional_agreement_micros=None,
                bullish_family_count=None,
                bearish_family_count=None,
                neutral_family_count=None,
                context_unavailable_families=context_unavailable,
                reasons=(
                    "DIRECTIONAL_PANEL_WITHHELD",
                    *(f"{status.value}:{family}" for family in affected),
                    *context_reasons,
                ),
            )

    numerator = sum(
        value.direction * value.strength_micros for value in directional
    )
    denominator = len(_DIRECTIONAL_FAMILY_ORDER)
    bullish_count = sum(value.direction == 1 for value in directional)
    bearish_count = sum(value.direction == -1 for value in directional)
    neutral_count = denominator - bullish_count - bearish_count
    state_class = _state_class(
        bullish_count=bullish_count,
        bearish_count=bearish_count,
    )
    return _DerivedDirectionalPanelV2(
        status=EvidenceReadinessV2.READY,
        state_class=state_class,
        directional_numerator_micros=numerator,
        directional_denominator=denominator,
        directional_agreement_micros=_round_nearest_away_from_zero(
            numerator,
            denominator,
        ),
        bullish_family_count=bullish_count,
        bearish_family_count=bearish_count,
        neutral_family_count=neutral_count,
        context_unavailable_families=context_unavailable,
        reasons=(
            "THREE_CAPPED_DIRECTIONAL_STATE_FAMILIES_AGGREGATED",
            "CONTEXT_FAMILIES_EXCLUDED_FROM_DIRECTIONAL_NUMERATOR",
            "DIRECTIONAL_AGREEMENT_IS_NOT_A_PROBABILITY",
            f"STATE_CLASS_{state_class.value}",
            *context_reasons,
        ),
    )


def _effective_readiness(
    observation: EvidenceFamilyObservationV2,
    item: EvidenceScoreInputV2,
) -> EvidenceReadinessV2:
    envelope = observation.producer_envelope
    if not envelope.closed_bar:
        return EvidenceReadinessV2.DATA_INVALID
    if (
        observation.latest_source_event_ms > observation.latest_source_receipt_ms
        or observation.latest_source_receipt_ms > item.decision_cutoff_ms
    ):
        return EvidenceReadinessV2.DATA_INVALID
    if not envelope.causal_inputs_complete:
        return EvidenceReadinessV2.INCONCLUSIVE_DATA
    return observation.readiness


def _state_class(
    *,
    bullish_count: int,
    bearish_count: int,
) -> DirectionalStateClassV2:
    if bullish_count == 3 and bearish_count == 0:
        return DirectionalStateClassV2.BROAD_BULLISH_STATE
    if bullish_count >= 2 and bearish_count == 0:
        return DirectionalStateClassV2.BULLISH_STATE_TILT
    if bearish_count == 3 and bullish_count == 0:
        return DirectionalStateClassV2.BROAD_BEARISH_STATE
    if bearish_count >= 2 and bullish_count == 0:
        return DirectionalStateClassV2.BEARISH_STATE_TILT
    return DirectionalStateClassV2.MIXED_OR_NEUTRAL_STATE


def _round_nearest_away_from_zero(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise DirectionalEvidenceContractErrorV2(
            "directional denominator must be positive"
        )
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    if 2 * remainder >= denominator:
        quotient += 1
    return sign * quotient


def _observations_in_order(
    observations: tuple[EvidenceFamilyObservationV2, ...],
    order: tuple[EvidenceInformationFamilyV2, ...],
) -> tuple[EvidenceFamilyObservationV2, ...]:
    by_family = {value.family: value for value in observations}
    try:
        return tuple(by_family[family] for family in order)
    except KeyError as exc:
        raise DirectionalEvidenceContractErrorV2(
            "source input is missing a frozen evidence family"
        ) from exc


def _panel_role(family: EvidenceInformationFamilyV2) -> EvidencePanelRoleV2:
    if family in _DIRECTIONAL_FAMILY_ORDER:
        return EvidencePanelRoleV2.DIRECTIONAL_STATE
    if family in _CONTEXT_FAMILY_ORDER:
        return EvidencePanelRoleV2.CONTEXT_ONLY_DIRECTION_IGNORED
    raise DirectionalEvidenceContractErrorV2("unsupported evidence family")


def _observation_document(
    decision: DirectionalEvidencePanelDecisionV2,
    observation: EvidenceFamilyObservationV2,
) -> dict[str, object]:
    try:
        document = evidence_family_observation_document_v2(observation)
    except EvidenceProducerContractErrorV2 as exc:
        raise DirectionalEvidenceContractErrorV2(str(exc)) from exc
    role = _panel_role(observation.family)
    if role is EvidencePanelRoleV2.CONTEXT_ONLY_DIRECTION_IGNORED:
        document.pop("direction")
        document.pop("strength_micros")
    document.update(
        {
            "assumed_closed_bar_through_ms": (
                decision.bar_close_ms
                if observation.producer_envelope.closed_bar
                else None
            ),
            "context_intensity_status": (
                "NOT_AVAILABLE_IN_LEGACY_SCHEMA"
                if role is EvidencePanelRoleV2.CONTEXT_ONLY_DIRECTION_IGNORED
                else None
            ),
            "data_through_ms": None,
            "data_through_status": "UNBOUND_M2",
            "direction_in_numerator": role is EvidencePanelRoleV2.DIRECTIONAL_STATE,
            "effective_readiness": _effective_readiness(
                observation,
                decision.source_input,
            ).value,
            "exchange_event_ms": observation.latest_source_event_ms,
            "local_receipt_ms": observation.latest_source_receipt_ms,
            "panel_role": role.value,
            "primary_relationship": decision.primary_relationship(
                observation.family
            ).value,
            "source_direction_ignored": (
                role is EvidencePanelRoleV2.CONTEXT_ONLY_DIRECTION_IGNORED
            ),
        }
    )
    return document


def _decision_document(
    decision: DirectionalEvidencePanelDecisionV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    ordered = (*decision.directional_observations, *decision.context_observations)
    document: dict[str, object] = {
        "all_causal_inputs_complete": decision.source_input.causal_inputs_complete,
        "all_observations_closed_bar": decision.source_input.closed_bar,
        "attempt_id": decision.attempt_id,
        "bar_close_ms": decision.bar_close_ms,
        "bar_open_ms": decision.bar_open_ms,
        "bearish_family_count": decision.bearish_family_count,
        "book_pressure_status": decision.book_pressure_status,
        "bullish_family_count": decision.bullish_family_count,
        "changes_primary_decision": decision.changes_primary_decision,
        "context_unavailable_families": [
            value.value for value in decision.context_unavailable_families
        ],
        "decision_cutoff_ms": decision.decision_cutoff_ms,
        "directional_agreement_micros": decision.directional_agreement_micros,
        "directional_denominator": decision.directional_denominator,
        "directional_numerator_micros": decision.directional_numerator_micros,
        "event_id": decision.event_id,
        "invalidation": decision.invalidation,
        "neutral_family_count": decision.neutral_family_count,
        "observations": [
            _observation_document(decision, value) for value in ordered
        ],
        "primary_binding_status": decision.primary_binding_status.value,
        "probability_calibrated": decision.probability_calibrated,
        "promoting": decision.promoting,
        "promoting_plan_sha256": decision.promoting_plan_sha256,
        "reasons": list(decision.reasons),
        "role": decision.role,
        "rule_version": decision.rule_version,
        "source_authority_status": decision.source_authority_status,
        "state_class": decision.state_class.value,
        "status": decision.status.value,
        "symbol": decision.symbol,
        "venue": decision.source_input.venue.value,
    }
    if include_payload_hash:
        document["payload_sha256"] = decision.payload_sha256
    return document
