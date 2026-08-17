from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.protocol.decision_clock import (
    DecisionClockContractErrorV2,
    validate_decision_bar_v2,
)
from signalbot.r4b_v2.strategy.evidence_producer import (
    EVIDENCE_STRENGTH_SCALE_V2,
    DependencyOwnershipLedgerV2,
    EvidenceFamilyObservationV2,
    EvidenceProducerContractErrorV2,
    EvidenceReadinessV2,
    evidence_decision_scope_sha256_v2,
    evidence_family_observation_document_v2,
    verify_evidence_observation_set_v2,
)
from signalbot.r4b_v2.strategy.evidence_producer import (
    EvidenceInformationFamilyV2 as EvidenceInformationFamilyV2,
)

EVIDENCE_SCORE_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.3.0_EVIDENCE_SCORE_V1_FACTORY_OWNED_NONPROMOTING"
)
EVIDENCE_SCORE_ROLE_V2: Final = "NON_PROMOTING_SHADOW_ANNOTATION"

_EVENT_ID_DOMAIN: Final = b"R4B_EVIDENCE_SCORE_ANNOTATION_V2\0"
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_LEAN_NUMERATOR_THRESHOLD: Final = 2 * EVIDENCE_STRENGTH_SCALE_V2
_STRONG_NUMERATOR_THRESHOLD: Final = 4 * EVIDENCE_STRENGTH_SCALE_V2
_FIXED_INVALIDATION: Final = (
    "NON_PROMOTING_ANNOTATION_CANNOT_OPEN_CLOSE_OR_FILTER_A_B_C_POSITIONS"
)


class EvidenceScoreContractErrorV2(ValueError):
    """Raised when an evidence annotation would violate the frozen contract."""


class EvidenceBiasV2(StrEnum):
    NOT_AVAILABLE = "NOT_AVAILABLE"
    NEUTRAL = "NEUTRAL"
    BULLISH_LEAN = "BULLISH_LEAN"
    BULLISH_STRONG = "BULLISH_STRONG"
    BEARISH_LEAN = "BEARISH_LEAN"
    BEARISH_STRONG = "BEARISH_STRONG"


class EvidenceRegistryDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


@dataclass(frozen=True, slots=True)
class EvidenceScoreInputV2:
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    closed_bar: bool
    causal_inputs_complete: bool
    observations: tuple[EvidenceFamilyObservationV2, ...]

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        if _SYMBOL_RE.fullmatch(self.symbol) is None:
            raise EvidenceScoreContractErrorV2(
                "symbol must be a normalized USDT symbol"
            )
        if self.venue is not VenueV2.USDM_FUTURES:
            raise EvidenceScoreContractErrorV2(
                "Evidence Score V2 accepts promoting USD-M Futures provenance only"
            )
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        try:
            validate_decision_bar_v2(
                self.bar_open_ms,
                self.bar_close_ms,
                self.decision_cutoff_ms,
            )
        except DecisionClockContractErrorV2 as error:
            raise EvidenceScoreContractErrorV2(str(error)) from error
        for field_name in ("closed_bar", "causal_inputs_complete"):
            if type(getattr(self, field_name)) is not bool:
                raise EvidenceScoreContractErrorV2(
                    f"{field_name} must be boolean"
                )
        if type(self.observations) is not tuple:
            raise EvidenceScoreContractErrorV2(
                "observations must be an immutable tuple"
            )
        scope_sha256 = _score_scope_sha256(
            attempt_id=self.attempt_id,
            symbol=self.symbol,
            venue=self.venue,
            promoting_plan_sha256=self.promoting_plan_sha256,
            bar_open_ms=self.bar_open_ms,
            bar_close_ms=self.bar_close_ms,
            decision_cutoff_ms=self.decision_cutoff_ms,
        )
        _validate_observation_set(
            self.observations,
            expected_scope_sha256=scope_sha256,
            expected_closed_bar=self.closed_bar,
            expected_causal_inputs_complete=self.causal_inputs_complete,
        )


@dataclass(frozen=True, slots=True)
class EvidenceScoreDecisionV2:
    attempt_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    status: EvidenceReadinessV2
    bias: EvidenceBiasV2
    score_numerator_micros: int | None
    score_denominator: int | None
    evidence_score_micros: int | None
    bullish_family_count: int | None
    bearish_family_count: int | None
    neutral_family_count: int | None
    observations: tuple[EvidenceFamilyObservationV2, ...]
    reasons: tuple[str, ...]
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    role: str = field(init=False, default=EVIDENCE_SCORE_ROLE_V2)
    promoting: bool = field(init=False, default=False)
    changes_primary_decision: bool = field(init=False, default=False)
    probability_calibrated: bool = field(init=False, default=False)
    invalidation: str = field(init=False, default=_FIXED_INVALIDATION)
    rule_version: str = field(
        init=False,
        default=EVIDENCE_SCORE_RULE_VERSION_V2,
    )

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        if _SYMBOL_RE.fullmatch(self.symbol) is None:
            raise EvidenceScoreContractErrorV2(
                "symbol must be a normalized USDT symbol"
            )
        if self.venue is not VenueV2.USDM_FUTURES:
            raise EvidenceScoreContractErrorV2(
                "evidence decision must retain USD-M Futures provenance"
            )
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        try:
            validate_decision_bar_v2(
                self.bar_open_ms,
                self.bar_close_ms,
                self.decision_cutoff_ms,
            )
        except DecisionClockContractErrorV2 as error:
            raise EvidenceScoreContractErrorV2(str(error)) from error
        if not isinstance(self.status, EvidenceReadinessV2):
            raise EvidenceScoreContractErrorV2(
                "status must be an EvidenceReadinessV2"
            )
        if not isinstance(self.bias, EvidenceBiasV2):
            raise EvidenceScoreContractErrorV2("bias must be an EvidenceBiasV2")
        _validate_observation_set(
            self.observations,
            expected_scope_sha256=_score_scope_sha256(
                attempt_id=self.attempt_id,
                symbol=self.symbol,
                venue=self.venue,
                promoting_plan_sha256=self.promoting_plan_sha256,
                bar_open_ms=self.bar_open_ms,
                bar_close_ms=self.bar_close_ms,
                decision_cutoff_ms=self.decision_cutoff_ms,
            ),
            expected_closed_bar=(
                True if self.status is EvidenceReadinessV2.READY else None
            ),
            expected_causal_inputs_complete=(
                True if self.status is EvidenceReadinessV2.READY else None
            ),
        )
        if self.observations != tuple(
            sorted(self.observations, key=lambda value: value.family.value)
        ):
            raise EvidenceScoreContractErrorV2(
                "decision observations must use canonical family order"
            )
        preclose_event_families = tuple(
            value.family.value
            for value in self.observations
            if value.latest_source_event_ms < self.bar_close_ms
        )
        postreceipt_event_families = tuple(
            value.family.value
            for value in self.observations
            if value.latest_source_event_ms > value.latest_source_receipt_ms
        )
        late_receipt_families = tuple(
            value.family.value
            for value in self.observations
            if value.latest_source_receipt_ms > self.decision_cutoff_ms
        )
        if self.status is not EvidenceReadinessV2.DATA_INVALID and (
            preclose_event_families
            or postreceipt_event_families
            or late_receipt_families
        ):
            raise EvidenceScoreContractErrorV2(
                "non-DATA_INVALID evidence decisions require k.T <= source "
                "event <= source receipt <= D"
            )
        _validate_reasons(self.reasons)
        self._validate_score_contract()
        identity = {
            "attempt_id": self.attempt_id,
            "bar_open_ms": self.bar_open_ms,
            "role": self.role,
            "rule_version": self.rule_version,
            "symbol": self.symbol,
        }
        event_id = hashlib.sha256(
            _EVENT_ID_DOMAIN + canonical_json_line(identity)
        ).hexdigest()
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = hashlib.sha256(
            canonical_json_line(_decision_document(self, include_payload_hash=False))
        ).hexdigest()
        object.__setattr__(self, "payload_sha256", payload_sha256)

    def _validate_score_contract(self) -> None:
        score_fields = (
            self.score_numerator_micros,
            self.score_denominator,
            self.evidence_score_micros,
            self.bullish_family_count,
            self.bearish_family_count,
            self.neutral_family_count,
        )
        if self.status is not EvidenceReadinessV2.READY:
            if self.bias is not EvidenceBiasV2.NOT_AVAILABLE:
                raise EvidenceScoreContractErrorV2(
                    "non-ready decisions require NOT_AVAILABLE bias"
                )
            if any(value is not None for value in score_fields):
                raise EvidenceScoreContractErrorV2(
                    "non-ready decisions cannot expose partial score fields"
                )
            return
        if any(type(value) is not int for value in score_fields):
            raise EvidenceScoreContractErrorV2(
                "READY decisions require every exact integer score field"
            )
        assert self.score_numerator_micros is not None
        assert self.score_denominator is not None
        assert self.evidence_score_micros is not None
        assert self.bullish_family_count is not None
        assert self.bearish_family_count is not None
        assert self.neutral_family_count is not None
        expected_numerator = sum(
            value.direction * value.strength_micros for value in self.observations
        )
        expected_denominator = len(self.observations)
        expected_bullish = sum(value.direction == 1 for value in self.observations)
        expected_bearish = sum(value.direction == -1 for value in self.observations)
        expected_neutral = expected_denominator - expected_bullish - expected_bearish
        expected_bias = _bias_from_exact_score(
            expected_numerator,
            bullish_count=expected_bullish,
            bearish_count=expected_bearish,
        )
        if (
            self.score_numerator_micros != expected_numerator
            or self.score_denominator != expected_denominator
            or self.evidence_score_micros
            != _round_nearest_away_from_zero(
                expected_numerator,
                expected_denominator,
            )
            or self.bullish_family_count != expected_bullish
            or self.bearish_family_count != expected_bearish
            or self.neutral_family_count != expected_neutral
            or self.bias is not expected_bias
        ):
            raise EvidenceScoreContractErrorV2(
                "READY score, counts, or bias contradict the bound observations"
            )

    @property
    def ready(self) -> bool:
        return self.status is EvidenceReadinessV2.READY


class EvidenceAnnotationRegistryV2:
    """Bounded in-memory duplicate/conflict gate for annotation delivery."""

    def __init__(self, *, maximum_events: int) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise EvidenceScoreContractErrorV2(
                "maximum_events must be a positive integer"
            )
        self._maximum_events = maximum_events
        self._payload_by_event_id: dict[str, bytes] = {}

    @property
    def event_count(self) -> int:
        return len(self._payload_by_event_id)

    def register(
        self,
        decision: EvidenceScoreDecisionV2,
    ) -> EvidenceRegistryDispositionV2:
        if not isinstance(decision, EvidenceScoreDecisionV2):
            raise EvidenceScoreContractErrorV2(
                "registry accepts EvidenceScoreDecisionV2 values only"
            )
        payload = canonical_evidence_score_decision_v2(decision)
        prior = self._payload_by_event_id.get(decision.event_id)
        if prior is not None:
            if prior != payload:
                raise EvidenceScoreContractErrorV2(
                    "deterministic evidence event ID collides with different payload"
                )
            return EvidenceRegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if len(self._payload_by_event_id) >= self._maximum_events:
            raise EvidenceScoreContractErrorV2(
                "bounded evidence annotation registry capacity exhausted"
            )
        self._payload_by_event_id[decision.event_id] = payload
        return EvidenceRegistryDispositionV2.NEW


def assemble_evidence_score_input_v2(
    ledger: DependencyOwnershipLedgerV2,
) -> EvidenceScoreInputV2:
    """Derive one score input atomically from six factory-sealed producers."""

    if not isinstance(ledger, DependencyOwnershipLedgerV2):
        raise EvidenceScoreContractErrorV2(
            "ledger must be a DependencyOwnershipLedgerV2"
        )
    try:
        observations = ledger.finalize_observations_v2()
    except EvidenceProducerContractErrorV2 as exc:
        raise EvidenceScoreContractErrorV2(str(exc)) from exc
    closed_bar = all(
        value.producer_envelope.closed_bar for value in observations
    )
    causal_inputs_complete = all(
        value.producer_envelope.causal_inputs_complete for value in observations
    )
    return EvidenceScoreInputV2(
        attempt_id=ledger.attempt_id,
        symbol=ledger.symbol,
        venue=ledger.venue,
        promoting_plan_sha256=ledger.promoting_plan_sha256,
        bar_open_ms=ledger.bar_open_ms,
        bar_close_ms=ledger.bar_close_ms,
        decision_cutoff_ms=ledger.decision_cutoff_ms,
        closed_bar=closed_bar,
        causal_inputs_complete=causal_inputs_complete,
        observations=observations,
    )


def evaluate_evidence_score_v2(item: EvidenceScoreInputV2) -> EvidenceScoreDecisionV2:
    """Aggregate six capped families without creating a probability or signal."""

    observations = tuple(sorted(item.observations, key=lambda value: value.family.value))
    if not item.closed_bar:
        return _unavailable_decision(
            item,
            observations,
            EvidenceReadinessV2.DATA_INVALID,
            ("UNCLOSED_CANDLE_FORBIDDEN",),
        )
    preclose_event_families = tuple(
        value.family.value
        for value in observations
        if value.latest_source_event_ms < item.bar_close_ms
    )
    postreceipt_event_families = tuple(
        value.family.value
        for value in observations
        if value.latest_source_event_ms > value.latest_source_receipt_ms
    )
    late_receipt_families = tuple(
        value.family.value
        for value in observations
        if value.latest_source_receipt_ms > item.decision_cutoff_ms
    )
    if (
        preclose_event_families
        or postreceipt_event_families
        or late_receipt_families
    ):
        reasons = tuple(
            [
                *(
                    f"PRECLOSE_EVENT_FORBIDDEN:{name}"
                    for name in preclose_event_families
                ),
                *(
                    f"POSTRECEIPT_EVENT_FORBIDDEN:{name}"
                    for name in postreceipt_event_families
                ),
                *(f"LATE_RECEIPT_FORBIDDEN:{name}" for name in late_receipt_families),
            ]
        )
        return _unavailable_decision(
            item,
            observations,
            EvidenceReadinessV2.DATA_INVALID,
            reasons,
        )
    if not item.causal_inputs_complete:
        return _unavailable_decision(
            item,
            observations,
            EvidenceReadinessV2.INCONCLUSIVE_DATA,
            ("CAUSAL_INPUTS_INCOMPLETE",),
        )

    for status in (
        EvidenceReadinessV2.DATA_INVALID,
        EvidenceReadinessV2.INCONCLUSIVE_DATA,
        EvidenceReadinessV2.FEATURE_NOT_READY,
    ):
        affected = tuple(
            value.family.value for value in observations if value.readiness is status
        )
        if affected:
            return _unavailable_decision(
                item,
                observations,
                status,
                tuple(f"{status.value}:{name}" for name in affected),
            )

    numerator = sum(
        value.direction * value.strength_micros for value in observations
    )
    denominator = len(observations)
    evidence_score_micros = _round_nearest_away_from_zero(numerator, denominator)
    bullish_count = sum(value.direction == 1 for value in observations)
    bearish_count = sum(value.direction == -1 for value in observations)
    neutral_count = denominator - bullish_count - bearish_count
    bias = _bias_from_exact_score(
        numerator,
        bullish_count=bullish_count,
        bearish_count=bearish_count,
    )
    return EvidenceScoreDecisionV2(
        attempt_id=item.attempt_id,
        symbol=item.symbol,
        venue=item.venue,
        promoting_plan_sha256=item.promoting_plan_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        status=EvidenceReadinessV2.READY,
        bias=bias,
        score_numerator_micros=numerator,
        score_denominator=denominator,
        evidence_score_micros=evidence_score_micros,
        bullish_family_count=bullish_count,
        bearish_family_count=bearish_count,
        neutral_family_count=neutral_count,
        observations=observations,
        reasons=(
            "SIX_CAPPED_INFORMATION_FAMILIES_AGGREGATED",
            "EVIDENCE_SCORE_IS_NOT_A_PROBABILITY",
            f"BIAS_{bias.value}",
        ),
    )


def canonical_evidence_score_decision_v2(
    decision: EvidenceScoreDecisionV2,
) -> bytes:
    """Return the one canonical, self-hash-checked annotation JSONL payload."""

    if not isinstance(decision, EvidenceScoreDecisionV2):
        raise EvidenceScoreContractErrorV2(
            "decision must be an EvidenceScoreDecisionV2"
        )
    expected = hashlib.sha256(
        canonical_json_line(_decision_document(decision, include_payload_hash=False))
    ).hexdigest()
    if decision.payload_sha256 != expected:
        raise EvidenceScoreContractErrorV2(
            "evidence annotation payload hash differs from canonical content"
        )
    return canonical_json_line(_decision_document(decision, include_payload_hash=True))


def _unavailable_decision(
    item: EvidenceScoreInputV2,
    observations: tuple[EvidenceFamilyObservationV2, ...],
    status: EvidenceReadinessV2,
    reasons: tuple[str, ...],
) -> EvidenceScoreDecisionV2:
    return EvidenceScoreDecisionV2(
        attempt_id=item.attempt_id,
        symbol=item.symbol,
        venue=item.venue,
        promoting_plan_sha256=item.promoting_plan_sha256,
        bar_open_ms=item.bar_open_ms,
        bar_close_ms=item.bar_close_ms,
        decision_cutoff_ms=item.decision_cutoff_ms,
        status=status,
        bias=EvidenceBiasV2.NOT_AVAILABLE,
        score_numerator_micros=None,
        score_denominator=None,
        evidence_score_micros=None,
        bullish_family_count=None,
        bearish_family_count=None,
        neutral_family_count=None,
        observations=observations,
        reasons=reasons,
    )


def _bias_from_exact_score(
    numerator: int,
    *,
    bullish_count: int,
    bearish_count: int,
) -> EvidenceBiasV2:
    if (
        numerator >= _STRONG_NUMERATOR_THRESHOLD
        and bullish_count >= 4
        and bearish_count <= 1
    ):
        return EvidenceBiasV2.BULLISH_STRONG
    if numerator >= _LEAN_NUMERATOR_THRESHOLD and bullish_count >= 3:
        return EvidenceBiasV2.BULLISH_LEAN
    if (
        numerator <= -_STRONG_NUMERATOR_THRESHOLD
        and bearish_count >= 4
        and bullish_count <= 1
    ):
        return EvidenceBiasV2.BEARISH_STRONG
    if numerator <= -_LEAN_NUMERATOR_THRESHOLD and bearish_count >= 3:
        return EvidenceBiasV2.BEARISH_LEAN
    return EvidenceBiasV2.NEUTRAL


def _round_nearest_away_from_zero(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise EvidenceScoreContractErrorV2("score denominator must be positive")
    sign = -1 if numerator < 0 else 1
    quotient, remainder = divmod(abs(numerator), denominator)
    if 2 * remainder >= denominator:
        quotient += 1
    return sign * quotient


def _decision_document(
    decision: EvidenceScoreDecisionV2,
    *,
    include_payload_hash: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "attempt_id": decision.attempt_id,
        "bar_close_ms": decision.bar_close_ms,
        "bar_open_ms": decision.bar_open_ms,
        "bearish_family_count": decision.bearish_family_count,
        "bias": decision.bias.value,
        "bullish_family_count": decision.bullish_family_count,
        "changes_primary_decision": decision.changes_primary_decision,
        "decision_cutoff_ms": decision.decision_cutoff_ms,
        "event_id": decision.event_id,
        "evidence_score_micros": decision.evidence_score_micros,
        "invalidation": decision.invalidation,
        "neutral_family_count": decision.neutral_family_count,
        "observations": [_observation_document(value) for value in decision.observations],
        "probability_calibrated": decision.probability_calibrated,
        "promoting": decision.promoting,
        "promoting_plan_sha256": decision.promoting_plan_sha256,
        "reasons": list(decision.reasons),
        "role": decision.role,
        "rule_version": decision.rule_version,
        "score_denominator": decision.score_denominator,
        "score_numerator_micros": decision.score_numerator_micros,
        "status": decision.status.value,
        "symbol": decision.symbol,
        "venue": decision.venue.value,
    }
    if include_payload_hash:
        document["payload_sha256"] = decision.payload_sha256
    return document


def _observation_document(value: EvidenceFamilyObservationV2) -> dict[str, object]:
    try:
        return evidence_family_observation_document_v2(value)
    except EvidenceProducerContractErrorV2 as exc:
        raise EvidenceScoreContractErrorV2(str(exc)) from exc


def _validate_observation_set(
    observations: tuple[EvidenceFamilyObservationV2, ...],
    *,
    expected_scope_sha256: str,
    expected_closed_bar: bool | None = None,
    expected_causal_inputs_complete: bool | None = None,
) -> None:
    try:
        verify_evidence_observation_set_v2(
            observations,
            expected_scope_sha256=expected_scope_sha256,
            expected_closed_bar=expected_closed_bar,
            expected_causal_inputs_complete=expected_causal_inputs_complete,
        )
    except EvidenceProducerContractErrorV2 as exc:
        raise EvidenceScoreContractErrorV2(str(exc)) from exc


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 32:
        raise EvidenceScoreContractErrorV2(
            "reasons must be a non-empty bounded immutable tuple"
        )
    for value in values:
        _validate_identity(value, "reason")


def _score_scope_sha256(
    *,
    attempt_id: str,
    symbol: str,
    venue: VenueV2,
    promoting_plan_sha256: str,
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> str:
    try:
        return evidence_decision_scope_sha256_v2(
            attempt_id=attempt_id,
            symbol=symbol,
            venue=venue,
            promoting_plan_sha256=promoting_plan_sha256,
            bar_open_ms=bar_open_ms,
            bar_close_ms=bar_close_ms,
            decision_cutoff_ms=decision_cutoff_ms,
        )
    except EvidenceProducerContractErrorV2 as exc:
        raise EvidenceScoreContractErrorV2(str(exc)) from exc


def _validate_identity(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
    ):
        raise EvidenceScoreContractErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )


def _validate_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise EvidenceScoreContractErrorV2(
            f"{field_name} must be a lowercase SHA-256 digest"
        )


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise EvidenceScoreContractErrorV2(
            f"{field_name} must be a nonnegative integer"
        )
