from __future__ import annotations

import hashlib
import json
import re
from dataclasses import InitVar, dataclass, field
from enum import StrEnum
from typing import Final, TypedDict

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decision_clock import (
    DecisionClockContractErrorV2,
    validate_decision_bar_v2,
)

EVIDENCE_STRENGTH_SCALE_V2: Final = 1_000_000

_ENVELOPE_ID_DOMAIN: Final = b"R4B_EVIDENCE_PRODUCER_ENVELOPE_ID_V2\0"
_ENVELOPE_PAYLOAD_DOMAIN: Final = b"R4B_EVIDENCE_PRODUCER_ENVELOPE_V2\0"
_DEPENDENCY_CLAIM_DOMAIN: Final = b"R4B_EVIDENCE_DEPENDENCY_CLAIM_V2\0"
_SCOPE_DOMAIN: Final = b"R4B_EVIDENCE_PRODUCER_SCOPE_V2\0"
_OWNERSHIP_ROOT_DOMAIN: Final = b"R4B_EVIDENCE_OWNERSHIP_ROOT_V2\0"
_STATE_SCHEMA: Final = "r4b_evidence_dependency_ownership_state_v2"
_OWNERSHIP_ROOT_SCHEMA: Final = "r4b_evidence_dependency_ownership_root_v2"
_ENVELOPE_SCHEMA: Final = "r4b_producer_evidence_envelope_v2"
_CLAIM_SCHEMA: Final = "r4b_evidence_dependency_claim_v2"
_MAX_DEPENDENCY_CLAIMS: Final = 16
_MAX_REASONS: Final = 32
_MAX_IDENTITY_LENGTH: Final = 256
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_ENVELOPE_FACTORY_TOKEN: Final = object()
_OBSERVATION_FACTORY_TOKEN: Final = object()


class _ScopeFields(TypedDict):
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int


class EvidenceProducerContractErrorV2(ValueError):
    """Raised when producer evidence or dependency ownership is invalid."""


class EvidenceInformationFamilyV2(StrEnum):
    """Capped information families; multiple indicators never create extra votes."""

    PRICE_STRUCTURE_MOMENTUM = "PRICE_STRUCTURE_MOMENTUM"
    PARTICIPATION_FLOW = "PARTICIPATION_FLOW"
    VOLATILITY_REGIME = "VOLATILITY_REGIME"
    DERIVATIVES_POSITIONING = "DERIVATIVES_POSITIONING"
    LIQUIDITY_EXECUTION = "LIQUIDITY_EXECUTION"
    CROSS_SECTIONAL_CONTEXT = "CROSS_SECTIONAL_CONTEXT"


class EvidenceReadinessV2(StrEnum):
    READY = "READY"
    FEATURE_NOT_READY = "FEATURE_NOT_READY"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"
    DATA_INVALID = "DATA_INVALID"


class EvidenceDependencyClassV2(StrEnum):
    TARGET_CLOSE_PATH = "TARGET_CLOSE_PATH"
    NORMAL_AGGTRADE_FLOW = "NORMAL_AGGTRADE_FLOW"
    TARGET_HIGH_LOW_RANGE = "TARGET_HIGH_LOW_RANGE"
    MARK_OI_POSITIONING = "MARK_OI_POSITIONING"
    STANDARD_DIFF_DEPTH_SYMMETRIC = "STANDARD_DIFF_DEPTH_SYMMETRIC"
    TARGET_EXCLUDED_CROSS_SECTION = "TARGET_EXCLUDED_CROSS_SECTION"


class DependencyOwnershipDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


_ALLOWED_DEPENDENCY_CLASSES: Final = {
    EvidenceInformationFamilyV2.PRICE_STRUCTURE_MOMENTUM: frozenset(
        {EvidenceDependencyClassV2.TARGET_CLOSE_PATH}
    ),
    EvidenceInformationFamilyV2.PARTICIPATION_FLOW: frozenset(
        {EvidenceDependencyClassV2.NORMAL_AGGTRADE_FLOW}
    ),
    EvidenceInformationFamilyV2.VOLATILITY_REGIME: frozenset(
        {EvidenceDependencyClassV2.TARGET_HIGH_LOW_RANGE}
    ),
    EvidenceInformationFamilyV2.DERIVATIVES_POSITIONING: frozenset(
        {EvidenceDependencyClassV2.MARK_OI_POSITIONING}
    ),
    EvidenceInformationFamilyV2.LIQUIDITY_EXECUTION: frozenset(
        {EvidenceDependencyClassV2.STANDARD_DIFF_DEPTH_SYMMETRIC}
    ),
    EvidenceInformationFamilyV2.CROSS_SECTIONAL_CONTEXT: frozenset(
        {EvidenceDependencyClassV2.TARGET_EXCLUDED_CROSS_SECTION}
    ),
}


@dataclass(frozen=True, slots=True)
class EvidenceDependencyClaimV2:
    """Canonical ownership claim for one atomic economic feature slice."""

    dependency_class: EvidenceDependencyClassV2
    economic_slice_sha256: str
    source_lineage_root_sha256: str
    claim_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.dependency_class, EvidenceDependencyClassV2):
            raise EvidenceProducerContractErrorV2(
                "dependency_class must be an EvidenceDependencyClassV2"
            )
        _validate_sha256(self.economic_slice_sha256, "economic_slice_sha256")
        _validate_sha256(
            self.source_lineage_root_sha256,
            "source_lineage_root_sha256",
        )
        claim_sha256 = hashlib.sha256(
            _DEPENDENCY_CLAIM_DOMAIN
            + canonical_json_line(_dependency_claim_document(self, include_hash=False))
        ).hexdigest()
        object.__setattr__(self, "claim_sha256", claim_sha256)


@dataclass(frozen=True, slots=True)
class ProducerEvidenceEnvelopeV2:
    """Factory-sealed normalized output from one authoritative family producer."""

    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    family: EvidenceInformationFamilyV2
    readiness: EvidenceReadinessV2
    direction: int
    strength_micros: int
    producer_version_id: str
    source_lineage_root_sha256: str
    feature_slice_root_sha256: str
    producer_evidence_sha256: str
    dependency_claims: tuple[EvidenceDependencyClaimV2, ...]
    latest_source_event_ms: int
    latest_source_receipt_ms: int
    closed_bar: bool
    causal_inputs_complete: bool
    reasons: tuple[str, ...]
    _factory_token: InitVar[object] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _ENVELOPE_FACTORY_TOKEN:
            raise EvidenceProducerContractErrorV2(
                "ProducerEvidenceEnvelopeV2 requires an authoritative producer factory"
            )
        _validate_scope_fields(
            attempt_id=self.attempt_id,
            symbol=self.symbol,
            venue=self.venue,
            promoting_plan_sha256=self.promoting_plan_sha256,
            bar_open_ms=self.bar_open_ms,
            bar_close_ms=self.bar_close_ms,
            decision_cutoff_ms=self.decision_cutoff_ms,
        )
        if not isinstance(self.family, EvidenceInformationFamilyV2):
            raise EvidenceProducerContractErrorV2(
                "family must be an EvidenceInformationFamilyV2"
            )
        if not isinstance(self.readiness, EvidenceReadinessV2):
            raise EvidenceProducerContractErrorV2(
                "readiness must be an EvidenceReadinessV2"
            )
        _validate_direction_and_strength(
            self.readiness,
            self.direction,
            self.strength_micros,
        )
        _validate_identity(self.producer_version_id, "producer_version_id")
        for value, field_name in (
            (self.source_lineage_root_sha256, "source_lineage_root_sha256"),
            (self.feature_slice_root_sha256, "feature_slice_root_sha256"),
            (self.producer_evidence_sha256, "producer_evidence_sha256"),
        ):
            _validate_sha256(value, field_name)
        _validate_dependency_claims(self.family, self.dependency_claims)
        _validate_nonnegative_int(
            self.latest_source_event_ms,
            "latest_source_event_ms",
        )
        _validate_nonnegative_int(
            self.latest_source_receipt_ms,
            "latest_source_receipt_ms",
        )
        for field_name in ("closed_bar", "causal_inputs_complete"):
            if type(getattr(self, field_name)) is not bool:
                raise EvidenceProducerContractErrorV2(
                    f"{field_name} must be boolean"
                )
        _validate_reasons(self.reasons)
        event_id = hashlib.sha256(
            _ENVELOPE_ID_DOMAIN + canonical_json_line(_envelope_identity_document(self))
        ).hexdigest()
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = hashlib.sha256(
            _ENVELOPE_PAYLOAD_DOMAIN
            + canonical_json_line(_envelope_document(self, include_payload_hash=False))
        ).hexdigest()
        object.__setattr__(self, "payload_sha256", payload_sha256)

    @property
    def scope_sha256(self) -> str:
        return evidence_decision_scope_sha256_v2(
            attempt_id=self.attempt_id,
            symbol=self.symbol,
            venue=self.venue,
            promoting_plan_sha256=self.promoting_plan_sha256,
            bar_open_ms=self.bar_open_ms,
            bar_close_ms=self.bar_close_ms,
            decision_cutoff_ms=self.decision_cutoff_ms,
        )

    @property
    def source_feature_ids(self) -> tuple[str, ...]:
        return tuple(sorted(claim.claim_sha256 for claim in self.dependency_claims))


@dataclass(frozen=True, slots=True)
class EvidenceFamilyObservationV2:
    """Ledger-issued capped contribution from one authoritative producer."""

    producer_envelope: ProducerEvidenceEnvelopeV2 = field(repr=False)
    ownership_scope_sha256: str
    ownership_ledger_root_sha256: str
    _factory_token: InitVar[object] = None

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _OBSERVATION_FACTORY_TOKEN:
            raise EvidenceProducerContractErrorV2(
                "EvidenceFamilyObservationV2 requires ownership-ledger finalization"
            )
        if not isinstance(self.producer_envelope, ProducerEvidenceEnvelopeV2):
            raise EvidenceProducerContractErrorV2(
                "producer_envelope must be a ProducerEvidenceEnvelopeV2"
            )
        canonical_producer_evidence_envelope_v2(self.producer_envelope)
        _validate_sha256(self.ownership_scope_sha256, "ownership_scope_sha256")
        _validate_sha256(
            self.ownership_ledger_root_sha256,
            "ownership_ledger_root_sha256",
        )
        if self.ownership_scope_sha256 != self.producer_envelope.scope_sha256:
            raise EvidenceProducerContractErrorV2(
                "observation scope differs from its producer envelope"
            )

    @property
    def family(self) -> EvidenceInformationFamilyV2:
        return self.producer_envelope.family

    @property
    def readiness(self) -> EvidenceReadinessV2:
        return self.producer_envelope.readiness

    @property
    def direction(self) -> int:
        return self.producer_envelope.direction

    @property
    def strength_micros(self) -> int:
        return self.producer_envelope.strength_micros

    @property
    def source_feature_ids(self) -> tuple[str, ...]:
        return self.producer_envelope.source_feature_ids

    @property
    def latest_source_event_ms(self) -> int:
        return self.producer_envelope.latest_source_event_ms

    @property
    def latest_source_receipt_ms(self) -> int:
        return self.producer_envelope.latest_source_receipt_ms

    @property
    def evidence_sha256(self) -> str:
        return self.producer_envelope.payload_sha256

    @property
    def reasons(self) -> tuple[str, ...]:
        return self.producer_envelope.reasons

    @property
    def producer_version_id(self) -> str:
        return self.producer_envelope.producer_version_id

    @property
    def source_lineage_root_sha256(self) -> str:
        return self.producer_envelope.source_lineage_root_sha256

    @property
    def feature_slice_root_sha256(self) -> str:
        return self.producer_envelope.feature_slice_root_sha256

    @property
    def producer_evidence_sha256(self) -> str:
        return self.producer_envelope.producer_evidence_sha256

    @property
    def dependency_claim_sha256s(self) -> tuple[str, ...]:
        return self.source_feature_ids

    @property
    def producer_envelope_sha256(self) -> str:
        return self.producer_envelope.payload_sha256


class DependencyOwnershipLedgerV2:
    """Bounded per-slot atomic owner of six producer leaves and their slices."""

    def __init__(
        self,
        *,
        attempt_id: str,
        symbol: str,
        venue: VenueV2,
        promoting_plan_sha256: str,
        bar_open_ms: int,
        bar_close_ms: int,
        decision_cutoff_ms: int,
    ) -> None:
        _validate_scope_fields(
            attempt_id=attempt_id,
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            decision_cutoff_ms=decision_cutoff_ms,
        )
        self._attempt_id = attempt_id
        self._symbol = symbol
        self._venue = venue
        self._promoting_plan_sha256 = promoting_plan_sha256
        self._bar_open_ms = bar_open_ms
        self._bar_close_ms = bar_close_ms
        self._decision_cutoff_ms = decision_cutoff_ms
        self._scope_sha256 = evidence_decision_scope_sha256_v2(
            attempt_id=attempt_id,
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            decision_cutoff_ms=decision_cutoff_ms,
        )
        self._envelopes: dict[
            EvidenceInformationFamilyV2,
            tuple[bytes, ProducerEvidenceEnvelopeV2],
        ] = {}
        self._slice_owner: dict[str, EvidenceInformationFamilyV2] = {}
        self._producer_evidence_owner: dict[str, EvidenceInformationFamilyV2] = {}
        self._finalized_observations: tuple[EvidenceFamilyObservationV2, ...] | None = (
            None
        )

    @property
    def event_count(self) -> int:
        return len(self._envelopes)

    @property
    def maximum_families(self) -> int:
        return len(EvidenceInformationFamilyV2)

    @property
    def scope_sha256(self) -> str:
        return self._scope_sha256

    @property
    def finalized(self) -> bool:
        return self._finalized_observations is not None

    @property
    def replay_root_sha256(self) -> str:
        return _ownership_root_sha256(
            scope_sha256=self._scope_sha256,
            envelopes=self._ordered_envelopes(),
            finalized=self.finalized,
        )

    @property
    def attempt_id(self) -> str:
        return self._attempt_id

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def venue(self) -> VenueV2:
        return self._venue

    @property
    def promoting_plan_sha256(self) -> str:
        return self._promoting_plan_sha256

    @property
    def bar_open_ms(self) -> int:
        return self._bar_open_ms

    @property
    def bar_close_ms(self) -> int:
        return self._bar_close_ms

    @property
    def decision_cutoff_ms(self) -> int:
        return self._decision_cutoff_ms

    def register(
        self,
        envelope: ProducerEvidenceEnvelopeV2,
    ) -> DependencyOwnershipDispositionV2:
        if not isinstance(envelope, ProducerEvidenceEnvelopeV2):
            raise EvidenceProducerContractErrorV2(
                "ownership ledger accepts ProducerEvidenceEnvelopeV2 values only"
            )
        payload = canonical_producer_evidence_envelope_v2(envelope)
        if envelope.scope_sha256 != self._scope_sha256:
            raise EvidenceProducerContractErrorV2(
                "producer envelope decision-slot scope differs from ownership ledger"
            )
        prior = self._envelopes.get(envelope.family)
        if prior is not None:
            if prior[0] != payload:
                raise EvidenceProducerContractErrorV2(
                    "same evidence family received a conflicting producer envelope"
                )
            return DependencyOwnershipDispositionV2.IDEMPOTENT_DUPLICATE
        if self.finalized:
            raise EvidenceProducerContractErrorV2(
                "finalized dependency ownership ledger cannot accept a new family"
            )
        if self.event_count >= self.maximum_families:
            raise EvidenceProducerContractErrorV2(
                "bounded six-family dependency ownership capacity exhausted"
            )
        for claim in envelope.dependency_claims:
            owner = self._slice_owner.get(claim.economic_slice_sha256)
            if owner is not None and owner is not envelope.family:
                raise EvidenceProducerContractErrorV2(
                    "economic feature slice is already owned by another family"
                )
        evidence_owner = self._producer_evidence_owner.get(
            envelope.producer_evidence_sha256
        )
        if evidence_owner is not None and evidence_owner is not envelope.family:
            raise EvidenceProducerContractErrorV2(
                "producer evidence document is already owned by another family"
            )

        self._envelopes[envelope.family] = (payload, envelope)
        for claim in envelope.dependency_claims:
            self._slice_owner[claim.economic_slice_sha256] = envelope.family
        self._producer_evidence_owner[envelope.producer_evidence_sha256] = (
            envelope.family
        )
        return DependencyOwnershipDispositionV2.NEW

    def finalize_observations_v2(
        self,
    ) -> tuple[EvidenceFamilyObservationV2, ...]:
        if self._finalized_observations is not None:
            return self._finalized_observations
        if set(self._envelopes) != set(EvidenceInformationFamilyV2):
            raise EvidenceProducerContractErrorV2(
                "all six evidence families are required before atomic finalization"
            )
        envelopes = self._ordered_envelopes()
        root = _ownership_root_sha256(
            scope_sha256=self._scope_sha256,
            envelopes=envelopes,
            finalized=True,
        )
        observations = tuple(
            EvidenceFamilyObservationV2(
                producer_envelope=envelope,
                ownership_scope_sha256=self._scope_sha256,
                ownership_ledger_root_sha256=root,
                _factory_token=_OBSERVATION_FACTORY_TOKEN,
            )
            for envelope in envelopes
        )
        verify_evidence_observation_set_v2(
            observations,
            expected_scope_sha256=self._scope_sha256,
        )
        self._finalized_observations = observations
        return observations

    def export_state_v2(self) -> bytes:
        document = self._state_document()
        return canonical_json_line(document)

    @classmethod
    def from_state_v2(
        cls,
        payload: bytes,
        *,
        expected_replay_root_sha256: str,
        expected_envelope_count: int,
        expected_maximum_families: int,
        expected_scope_sha256: str,
        expected_finalized: bool,
    ) -> DependencyOwnershipLedgerV2:
        _validate_sha256(
            expected_replay_root_sha256,
            "expected_replay_root_sha256",
        )
        _validate_sha256(expected_scope_sha256, "expected_scope_sha256")
        _validate_nonnegative_int(
            expected_envelope_count,
            "expected_envelope_count",
        )
        if (
            type(expected_maximum_families) is not int
            or expected_maximum_families != len(EvidenceInformationFamilyV2)
        ):
            raise EvidenceProducerContractErrorV2(
                "expected_maximum_families must equal the frozen six-family capacity"
            )
        if type(expected_finalized) is not bool:
            raise EvidenceProducerContractErrorV2(
                "expected_finalized must be boolean"
            )
        if type(payload) is not bytes or not payload:
            raise EvidenceProducerContractErrorV2(
                "ownership state must be non-empty bytes"
            )
        try:
            document = json.loads(payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise EvidenceProducerContractErrorV2(
                "ownership state is not valid UTF-8 JSON"
            ) from exc
        if not isinstance(document, dict) or canonical_json_line(document) != payload:
            raise EvidenceProducerContractErrorV2(
                "ownership state must be canonical JSONL"
            )
        expected_keys = {
            "envelopes",
            "finalized",
            "maximum_families",
            "replay_root_sha256",
            "schema_version",
            "scope",
            "scope_sha256",
        }
        if set(document) != expected_keys or document.get("schema_version") != (
            _STATE_SCHEMA
        ):
            raise EvidenceProducerContractErrorV2(
                "ownership state schema is unsupported"
            )
        raw_scope = document.get("scope")
        scope = _parse_scope_document(raw_scope)
        observed_scope_sha256 = document.get("scope_sha256")
        _validate_sha256_object(observed_scope_sha256, "scope_sha256")
        assert isinstance(observed_scope_sha256, str)
        derived_scope_sha256 = evidence_decision_scope_sha256_v2(**scope)
        raw_envelopes = document.get("envelopes")
        finalized = document.get("finalized")
        maximum_families = document.get("maximum_families")
        if (
            observed_scope_sha256 != derived_scope_sha256
            or observed_scope_sha256 != expected_scope_sha256
            or not isinstance(raw_envelopes, list)
            or len(raw_envelopes) != expected_envelope_count
            or len(raw_envelopes) > len(EvidenceInformationFamilyV2)
            or type(finalized) is not bool
            or finalized is not expected_finalized
            or type(maximum_families) is not int
            or maximum_families != expected_maximum_families
        ):
            raise EvidenceProducerContractErrorV2(
                "ownership state differs from its externally pinned scope or count"
            )
        ledger = cls(**scope)
        prior_family: str | None = None
        for raw_envelope in raw_envelopes:
            envelope = _producer_envelope_from_document(raw_envelope)
            if prior_family is not None and envelope.family.value <= prior_family:
                raise EvidenceProducerContractErrorV2(
                    "ownership envelopes must use strict canonical family order"
                )
            prior_family = envelope.family.value
            ledger.register(envelope)
        if finalized:
            ledger.finalize_observations_v2()
        observed_root = document.get("replay_root_sha256")
        _validate_sha256_object(observed_root, "replay_root_sha256")
        if (
            observed_root != ledger.replay_root_sha256
            or observed_root != expected_replay_root_sha256
            or ledger.export_state_v2() != payload
        ):
            raise EvidenceProducerContractErrorV2(
                "ownership replay root differs from its external checkpoint"
            )
        return ledger

    def _ordered_envelopes(self) -> tuple[ProducerEvidenceEnvelopeV2, ...]:
        return tuple(
            self._envelopes[family][1]
            for family in sorted(self._envelopes, key=lambda value: value.value)
        )

    def _state_document(self) -> dict[str, object]:
        envelopes = self._ordered_envelopes()
        return {
            "envelopes": [
                _envelope_document(value, include_payload_hash=True)
                for value in envelopes
            ],
            "finalized": self.finalized,
            "maximum_families": self.maximum_families,
            "replay_root_sha256": _ownership_root_sha256(
                scope_sha256=self._scope_sha256,
                envelopes=envelopes,
                finalized=self.finalized,
            ),
            "schema_version": _STATE_SCHEMA,
            "scope": _scope_document(
                attempt_id=self._attempt_id,
                symbol=self._symbol,
                venue=self._venue,
                promoting_plan_sha256=self._promoting_plan_sha256,
                bar_open_ms=self._bar_open_ms,
                bar_close_ms=self._bar_close_ms,
                decision_cutoff_ms=self._decision_cutoff_ms,
            ),
            "scope_sha256": self._scope_sha256,
        }


def evidence_decision_scope_sha256_v2(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> str:
    _validate_scope_fields(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
    )
    return hashlib.sha256(
        _SCOPE_DOMAIN
        + canonical_json_line(
            _scope_document(
                attempt_id=attempt_id,
                symbol=symbol,
                venue=venue,
                promoting_plan_sha256=promoting_plan_sha256,
                bar_open_ms=bar_open_ms,
                bar_close_ms=bar_close_ms,
                decision_cutoff_ms=decision_cutoff_ms,
            )
        )
    ).hexdigest()


def canonical_producer_evidence_envelope_v2(
    envelope: ProducerEvidenceEnvelopeV2,
) -> bytes:
    if not isinstance(envelope, ProducerEvidenceEnvelopeV2):
        raise EvidenceProducerContractErrorV2(
            "envelope must be a ProducerEvidenceEnvelopeV2"
        )
    expected = hashlib.sha256(
        _ENVELOPE_PAYLOAD_DOMAIN
        + canonical_json_line(_envelope_document(envelope, include_payload_hash=False))
    ).hexdigest()
    if envelope.payload_sha256 != expected:
        raise EvidenceProducerContractErrorV2(
            "producer envelope payload hash differs from canonical content"
        )
    return canonical_json_line(_envelope_document(envelope, include_payload_hash=True))


def evidence_family_observation_document_v2(
    observation: EvidenceFamilyObservationV2,
) -> dict[str, object]:
    if not isinstance(observation, EvidenceFamilyObservationV2):
        raise EvidenceProducerContractErrorV2(
            "observation must be an EvidenceFamilyObservationV2"
        )
    canonical_producer_evidence_envelope_v2(observation.producer_envelope)
    return {
        "dependency_claims": [
            _dependency_claim_document(claim, include_hash=True)
            for claim in observation.producer_envelope.dependency_claims
        ],
        "direction": observation.direction,
        "evidence_sha256": observation.evidence_sha256,
        "family": observation.family.value,
        "feature_slice_root_sha256": observation.feature_slice_root_sha256,
        "latest_source_event_ms": observation.latest_source_event_ms,
        "latest_source_receipt_ms": observation.latest_source_receipt_ms,
        "ownership_ledger_root_sha256": (
            observation.ownership_ledger_root_sha256
        ),
        "ownership_scope_sha256": observation.ownership_scope_sha256,
        "producer_envelope_sha256": observation.producer_envelope_sha256,
        "producer_evidence_sha256": observation.producer_evidence_sha256,
        "producer_version_id": observation.producer_version_id,
        "readiness": observation.readiness.value,
        "reasons": list(observation.reasons),
        "source_feature_ids": list(observation.source_feature_ids),
        "source_lineage_root_sha256": observation.source_lineage_root_sha256,
        "strength_micros": observation.strength_micros,
    }


def verify_evidence_observation_set_v2(
    observations: tuple[EvidenceFamilyObservationV2, ...],
    *,
    expected_scope_sha256: str,
    expected_closed_bar: bool | None = None,
    expected_causal_inputs_complete: bool | None = None,
) -> None:
    _validate_sha256(expected_scope_sha256, "expected_scope_sha256")
    if type(observations) is not tuple or any(
        not isinstance(value, EvidenceFamilyObservationV2) for value in observations
    ):
        raise EvidenceProducerContractErrorV2(
            "every observation must be factory-issued by an ownership ledger"
        )
    families = tuple(value.family for value in observations)
    if len(families) != len(EvidenceInformationFamilyV2) or set(families) != set(
        EvidenceInformationFamilyV2
    ):
        raise EvidenceProducerContractErrorV2(
            "observations must contain every fixed information family exactly once"
        )
    if any(value.ownership_scope_sha256 != expected_scope_sha256 for value in observations):
        raise EvidenceProducerContractErrorV2(
            "observation decision-slot scope differs from score input"
        )
    roots = {value.ownership_ledger_root_sha256 for value in observations}
    if len(roots) != 1:
        raise EvidenceProducerContractErrorV2(
            "observations must share one finalized ownership ledger root"
        )
    envelopes = tuple(
        sorted(
            (value.producer_envelope for value in observations),
            key=lambda value: value.family.value,
        )
    )
    for envelope in envelopes:
        canonical_producer_evidence_envelope_v2(envelope)
        if envelope.scope_sha256 != expected_scope_sha256:
            raise EvidenceProducerContractErrorV2(
                "producer envelope scope differs from score input"
            )
    _validate_cross_family_ownership(envelopes)
    expected_root = _ownership_root_sha256(
        scope_sha256=expected_scope_sha256,
        envelopes=envelopes,
        finalized=True,
    )
    if roots != {expected_root}:
        raise EvidenceProducerContractErrorV2(
            "observation ownership root differs from canonical producer leaves"
        )
    derived_closed_bar = all(value.closed_bar for value in envelopes)
    derived_complete = all(value.causal_inputs_complete for value in envelopes)
    if (
        expected_closed_bar is not None
        and (
            type(expected_closed_bar) is not bool
            or expected_closed_bar is not derived_closed_bar
        )
    ):
        raise EvidenceProducerContractErrorV2(
            "closed_bar differs from factory-sealed producer envelopes"
        )
    if (
        expected_causal_inputs_complete is not None
        and (
            type(expected_causal_inputs_complete) is not bool
            or expected_causal_inputs_complete is not derived_complete
        )
    ):
        raise EvidenceProducerContractErrorV2(
            "causal_inputs_complete differs from factory-sealed producer envelopes"
        )


def _seal_producer_evidence_envelope_v2(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    family: EvidenceInformationFamilyV2,
    readiness: EvidenceReadinessV2,
    direction: int,
    strength_micros: int,
    producer_version_id: str,
    source_lineage_root_sha256: str,
    feature_slice_root_sha256: str,
    producer_evidence_sha256: str,
    dependency_claims: tuple[EvidenceDependencyClaimV2, ...],
    latest_source_event_ms: int,
    latest_source_receipt_ms: int,
    closed_bar: bool,
    causal_inputs_complete: bool,
    reasons: tuple[str, ...],
) -> ProducerEvidenceEnvelopeV2:
    """Internal seal hook used only by authoritative family producers/testkit."""

    return ProducerEvidenceEnvelopeV2(
        attempt_id=attempt_id,
        symbol=symbol,
        venue=venue,
        promoting_plan_sha256=promoting_plan_sha256,
        bar_open_ms=bar_open_ms,
        bar_close_ms=bar_close_ms,
        decision_cutoff_ms=decision_cutoff_ms,
        family=family,
        readiness=readiness,
        direction=direction,
        strength_micros=strength_micros,
        producer_version_id=producer_version_id,
        source_lineage_root_sha256=source_lineage_root_sha256,
        feature_slice_root_sha256=feature_slice_root_sha256,
        producer_evidence_sha256=producer_evidence_sha256,
        dependency_claims=dependency_claims,
        latest_source_event_ms=latest_source_event_ms,
        latest_source_receipt_ms=latest_source_receipt_ms,
        closed_bar=closed_bar,
        causal_inputs_complete=causal_inputs_complete,
        reasons=reasons,
        _factory_token=_ENVELOPE_FACTORY_TOKEN,
    )


def _ownership_root_sha256(
    *,
    scope_sha256: str,
    envelopes: tuple[ProducerEvidenceEnvelopeV2, ...],
    finalized: bool,
) -> str:
    _validate_sha256(scope_sha256, "scope_sha256")
    if type(finalized) is not bool:
        raise EvidenceProducerContractErrorV2("finalized must be boolean")
    leaves = []
    for envelope in sorted(envelopes, key=lambda value: value.family.value):
        canonical_producer_evidence_envelope_v2(envelope)
        leaves.append(
            {
                "event_id": envelope.event_id,
                "family": envelope.family.value,
                "payload_sha256": envelope.payload_sha256,
            }
        )
    return hashlib.sha256(
        _OWNERSHIP_ROOT_DOMAIN
        + canonical_json_line(
            {
                "finalized": finalized,
                "leaves": leaves,
                "schema_version": _OWNERSHIP_ROOT_SCHEMA,
                "scope_sha256": scope_sha256,
            }
        )
    ).hexdigest()


def _validate_cross_family_ownership(
    envelopes: tuple[ProducerEvidenceEnvelopeV2, ...],
) -> None:
    slice_owner: dict[str, EvidenceInformationFamilyV2] = {}
    evidence_owner: dict[str, EvidenceInformationFamilyV2] = {}
    for envelope in envelopes:
        for claim in envelope.dependency_claims:
            owner = slice_owner.get(claim.economic_slice_sha256)
            if owner is not None and owner is not envelope.family:
                raise EvidenceProducerContractErrorV2(
                    "economic feature slice is owned by multiple families"
                )
            slice_owner[claim.economic_slice_sha256] = envelope.family
        owner = evidence_owner.get(envelope.producer_evidence_sha256)
        if owner is not None and owner is not envelope.family:
            raise EvidenceProducerContractErrorV2(
                "producer evidence document is owned by multiple families"
            )
        evidence_owner[envelope.producer_evidence_sha256] = envelope.family


def _dependency_claim_document(
    claim: EvidenceDependencyClaimV2,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "dependency_class": claim.dependency_class.value,
        "economic_slice_sha256": claim.economic_slice_sha256,
        "schema_version": _CLAIM_SCHEMA,
        "source_lineage_root_sha256": claim.source_lineage_root_sha256,
    }
    if include_hash:
        document["claim_sha256"] = claim.claim_sha256
    return document


def _envelope_identity_document(
    envelope: ProducerEvidenceEnvelopeV2,
) -> dict[str, object]:
    return {
        "attempt_id": envelope.attempt_id,
        "bar_open_ms": envelope.bar_open_ms,
        "family": envelope.family.value,
        "role": "EVIDENCE_PRODUCER",
        "symbol": envelope.symbol,
    }


def _envelope_document(
    envelope: ProducerEvidenceEnvelopeV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "attempt_id": envelope.attempt_id,
        "bar_close_ms": envelope.bar_close_ms,
        "bar_open_ms": envelope.bar_open_ms,
        "causal_inputs_complete": envelope.causal_inputs_complete,
        "closed_bar": envelope.closed_bar,
        "decision_cutoff_ms": envelope.decision_cutoff_ms,
        "dependency_claims": [
            _dependency_claim_document(value, include_hash=True)
            for value in envelope.dependency_claims
        ],
        "direction": envelope.direction,
        "event_id": envelope.event_id,
        "family": envelope.family.value,
        "feature_slice_root_sha256": envelope.feature_slice_root_sha256,
        "latest_source_event_ms": envelope.latest_source_event_ms,
        "latest_source_receipt_ms": envelope.latest_source_receipt_ms,
        "producer_evidence_sha256": envelope.producer_evidence_sha256,
        "producer_version_id": envelope.producer_version_id,
        "promoting_plan_sha256": envelope.promoting_plan_sha256,
        "readiness": envelope.readiness.value,
        "reasons": list(envelope.reasons),
        "schema_version": _ENVELOPE_SCHEMA,
        "source_lineage_root_sha256": envelope.source_lineage_root_sha256,
        "strength_micros": envelope.strength_micros,
        "symbol": envelope.symbol,
        "venue": envelope.venue.value,
    }
    if include_payload_hash:
        document["payload_sha256"] = envelope.payload_sha256
    return document


def _producer_envelope_from_document(raw: object) -> ProducerEvidenceEnvelopeV2:
    if not isinstance(raw, dict):
        raise EvidenceProducerContractErrorV2(
            "producer envelope state must be an object"
        )
    expected_keys = {
        "attempt_id",
        "bar_close_ms",
        "bar_open_ms",
        "causal_inputs_complete",
        "closed_bar",
        "decision_cutoff_ms",
        "dependency_claims",
        "direction",
        "event_id",
        "family",
        "feature_slice_root_sha256",
        "latest_source_event_ms",
        "latest_source_receipt_ms",
        "payload_sha256",
        "producer_evidence_sha256",
        "producer_version_id",
        "promoting_plan_sha256",
        "readiness",
        "reasons",
        "schema_version",
        "source_lineage_root_sha256",
        "strength_micros",
        "symbol",
        "venue",
    }
    if set(raw) != expected_keys or raw.get("schema_version") != _ENVELOPE_SCHEMA:
        raise EvidenceProducerContractErrorV2(
            "producer envelope state schema is unsupported"
        )
    raw_claims = raw.get("dependency_claims")
    raw_reasons = raw.get("reasons")
    if not isinstance(raw_claims, list) or not isinstance(raw_reasons, list):
        raise EvidenceProducerContractErrorV2(
            "producer envelope claims and reasons must be arrays"
        )
    try:
        claims = tuple(_dependency_claim_from_document(value) for value in raw_claims)
        envelope = _seal_producer_evidence_envelope_v2(
            attempt_id=raw["attempt_id"],
            symbol=raw["symbol"],
            venue=VenueV2(raw["venue"]),
            promoting_plan_sha256=raw["promoting_plan_sha256"],
            bar_open_ms=raw["bar_open_ms"],
            bar_close_ms=raw["bar_close_ms"],
            decision_cutoff_ms=raw["decision_cutoff_ms"],
            family=EvidenceInformationFamilyV2(raw["family"]),
            readiness=EvidenceReadinessV2(raw["readiness"]),
            direction=raw["direction"],
            strength_micros=raw["strength_micros"],
            producer_version_id=raw["producer_version_id"],
            source_lineage_root_sha256=raw["source_lineage_root_sha256"],
            feature_slice_root_sha256=raw["feature_slice_root_sha256"],
            producer_evidence_sha256=raw["producer_evidence_sha256"],
            dependency_claims=claims,
            latest_source_event_ms=raw["latest_source_event_ms"],
            latest_source_receipt_ms=raw["latest_source_receipt_ms"],
            closed_bar=raw["closed_bar"],
            causal_inputs_complete=raw["causal_inputs_complete"],
            reasons=tuple(raw_reasons),
        )  # type: ignore[arg-type]
    except (KeyError, TypeError, ValueError) as exc:
        raise EvidenceProducerContractErrorV2(
            "producer envelope state is invalid"
        ) from exc
    if (
        raw.get("event_id") != envelope.event_id
        or raw.get("payload_sha256") != envelope.payload_sha256
        or _envelope_document(envelope, include_payload_hash=True) != raw
    ):
        raise EvidenceProducerContractErrorV2(
            "producer envelope state differs from canonical content"
        )
    return envelope


def _dependency_claim_from_document(raw: object) -> EvidenceDependencyClaimV2:
    if not isinstance(raw, dict) or set(raw) != {
        "claim_sha256",
        "dependency_class",
        "economic_slice_sha256",
        "schema_version",
        "source_lineage_root_sha256",
    } or raw.get("schema_version") != _CLAIM_SCHEMA:
        raise EvidenceProducerContractErrorV2(
            "dependency claim state schema is unsupported"
        )
    try:
        claim = EvidenceDependencyClaimV2(
            dependency_class=EvidenceDependencyClassV2(raw["dependency_class"]),
            economic_slice_sha256=raw["economic_slice_sha256"],
            source_lineage_root_sha256=raw["source_lineage_root_sha256"],
        )  # type: ignore[arg-type]
    except (KeyError, TypeError, ValueError) as exc:
        raise EvidenceProducerContractErrorV2(
            "dependency claim state is invalid"
        ) from exc
    if (
        raw.get("claim_sha256") != claim.claim_sha256
        or _dependency_claim_document(claim, include_hash=True) != raw
    ):
        raise EvidenceProducerContractErrorV2(
            "dependency claim hash differs from canonical content"
        )
    return claim


def _scope_document(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> dict[str, object]:
    return {
        "attempt_id": attempt_id,
        "bar_close_ms": bar_close_ms,
        "bar_open_ms": bar_open_ms,
        "decision_cutoff_ms": decision_cutoff_ms,
        "promoting_plan_sha256": promoting_plan_sha256,
        "symbol": symbol,
        "venue": venue.value,
    }


def _parse_scope_document(raw: object) -> _ScopeFields:
    if not isinstance(raw, dict) or set(raw) != {
        "attempt_id",
        "bar_close_ms",
        "bar_open_ms",
        "decision_cutoff_ms",
        "promoting_plan_sha256",
        "symbol",
        "venue",
    }:
        raise EvidenceProducerContractErrorV2(
            "ownership scope state schema is unsupported"
        )
    try:
        scope = _ScopeFields(
            attempt_id=raw["attempt_id"],
            symbol=raw["symbol"],
            venue=VenueV2(raw["venue"]),
            promoting_plan_sha256=raw["promoting_plan_sha256"],
            bar_open_ms=raw["bar_open_ms"],
            bar_close_ms=raw["bar_close_ms"],
            decision_cutoff_ms=raw["decision_cutoff_ms"],
        )  # type: ignore[typeddict-item]
        _validate_scope_fields(**scope)
    except (KeyError, TypeError, ValueError) as exc:
        raise EvidenceProducerContractErrorV2(
            "ownership scope state is invalid"
        ) from exc
    return scope


def _validate_scope_fields(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> None:
    _validate_identity(attempt_id, "attempt_id")
    if not isinstance(symbol, str) or _SYMBOL_RE.fullmatch(symbol) is None:
        raise EvidenceProducerContractErrorV2(
            "symbol must be a normalized USDT symbol"
        )
    if venue is not VenueV2.USDM_FUTURES:
        raise EvidenceProducerContractErrorV2(
            "producer evidence requires USD-M Futures provenance"
        )
    _validate_sha256(promoting_plan_sha256, "promoting_plan_sha256")
    try:
        validate_decision_bar_v2(
            bar_open_ms,
            bar_close_ms,
            decision_cutoff_ms,
        )
    except DecisionClockContractErrorV2 as exc:
        raise EvidenceProducerContractErrorV2(str(exc)) from exc


def _validate_dependency_claims(
    family: EvidenceInformationFamilyV2,
    claims: tuple[EvidenceDependencyClaimV2, ...],
) -> None:
    if (
        type(claims) is not tuple
        or not claims
        or len(claims) > _MAX_DEPENDENCY_CLAIMS
        or any(not isinstance(value, EvidenceDependencyClaimV2) for value in claims)
    ):
        raise EvidenceProducerContractErrorV2(
            "dependency_claims must be a non-empty bounded immutable tuple"
        )
    expected = tuple(sorted(claims, key=_dependency_claim_order_key))
    if claims != expected or len({value.claim_sha256 for value in claims}) != len(claims):
        raise EvidenceProducerContractErrorV2(
            "dependency_claims must be canonical, sorted, and unique"
        )
    if len({value.economic_slice_sha256 for value in claims}) != len(claims):
        raise EvidenceProducerContractErrorV2(
            "one economic slice cannot be claimed twice in an envelope"
        )
    allowed = _ALLOWED_DEPENDENCY_CLASSES[family]
    if any(value.dependency_class not in allowed for value in claims):
        raise EvidenceProducerContractErrorV2(
            "dependency class is not allowed for this evidence family"
        )


def _dependency_claim_order_key(
    value: EvidenceDependencyClaimV2,
) -> tuple[str, str, str]:
    return (
        value.dependency_class.value,
        value.economic_slice_sha256,
        value.source_lineage_root_sha256,
    )


def _validate_direction_and_strength(
    readiness: EvidenceReadinessV2,
    direction: int,
    strength_micros: int,
) -> None:
    if type(direction) is not int or direction not in (-1, 0, 1):
        raise EvidenceProducerContractErrorV2(
            "direction must be exactly -1, 0, or 1"
        )
    if (
        type(strength_micros) is not int
        or not 0 <= strength_micros <= EVIDENCE_STRENGTH_SCALE_V2
    ):
        raise EvidenceProducerContractErrorV2(
            "strength_micros must be an integer in [0, 1000000]"
        )
    if readiness is EvidenceReadinessV2.READY:
        if (direction == 0) != (strength_micros == 0):
            raise EvidenceProducerContractErrorV2(
                "READY neutral evidence requires zero strength and directional "
                "evidence requires positive strength"
            )
    elif direction != 0 or strength_micros != 0:
        raise EvidenceProducerContractErrorV2(
            "non-ready evidence cannot expose a direction or strength"
        )


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > _MAX_REASONS:
        raise EvidenceProducerContractErrorV2(
            "reasons must be a non-empty bounded immutable tuple"
        )
    for value in values:
        _validate_identity(value, "reason")


def _validate_identity(value: object, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise EvidenceProducerContractErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )


def _validate_sha256(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise EvidenceProducerContractErrorV2(
            f"{field_name} must be a lowercase SHA-256 digest"
        )


def _validate_sha256_object(value: object, field_name: str) -> None:
    _validate_sha256(value, field_name)


def _validate_nonnegative_int(value: object, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise EvidenceProducerContractErrorV2(
            f"{field_name} must be a nonnegative integer"
        )
