"""Strict public USD-M realized-funding confirmation and cashflow contracts.

This module consumes retained HTTP bytes and never performs network I/O.  Its HTTP
attempt hashes record transport lineage but do not prove membership in the authoritative
capture ledger.  Position quantities are factory-sealed only after Merkle verification
against an externally pinned position-ledger checkpoint; the checkpoint's authority and
the production NAV/position-ledger integration remain upstream responsibilities.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2

FUNDING_RULE_VERSION_V2: Final = "R4B_CAUSAL_V2.2.0_REALIZED_FUNDING_V2"
FUNDING_ROUTE_ID_V2: Final = "usdm_public_rest"
FUNDING_ENDPOINT_PATH_V2: Final = "/fapi/v1/fundingRate"
FUNDING_CONFIRMATION_MAX_DELAY_MS_V2: Final = 900_000
FUNDING_HORIZON_GRACE_MS_V2: Final = 60_000

_MAX_IDENTITY_LENGTH: Final = 256
_MAX_RESPONSE_BYTES: Final = 1_048_576
_MAX_REQUEST_ATTEMPTS: Final = 16
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_DECIMAL_TEXT_RE: Final = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_SENSITIVE_QUERY_NAMES: Final = frozenset(
    {"apikey", "api_key", "listenkey", "signature", "token", "secret"}
)

_ATTEMPT_ID_DOMAIN: Final = b"R4B_REALIZED_FUNDING_HTTP_ATTEMPT_ID_V2\0"
_ATTEMPT_PAYLOAD_DOMAIN: Final = b"R4B_REALIZED_FUNDING_HTTP_ATTEMPT_V2\0"
_LINEAGE_DOMAIN: Final = b"R4B_REALIZED_FUNDING_REQUEST_LINEAGE_V2\0"
_EVIDENCE_ROOT_DOMAIN: Final = b"R4B_REALIZED_FUNDING_EVIDENCE_ROOT_V2\0"
_DECISION_ID_DOMAIN: Final = b"R4B_REALIZED_FUNDING_DECISION_ID_V2\0"
_DECISION_PAYLOAD_DOMAIN: Final = b"R4B_REALIZED_FUNDING_DECISION_PAYLOAD_V2\0"
_REGISTRY_REPLAY_DOMAIN: Final = b"R4B_REALIZED_FUNDING_REGISTRY_REPLAY_V2\0"
_REGISTRY_CHECKPOINT_DOMAIN: Final = b"R4B_REALIZED_FUNDING_REGISTRY_CHECKPOINT_V2\0"
_POSITION_SNAPSHOT_DOMAIN: Final = b"R4B_REALIZED_FUNDING_POSITION_SNAPSHOT_V2\0"
_POSITION_LEDGER_LEAF_DOMAIN: Final = b"R4B_REALIZED_FUNDING_POSITION_LEDGER_LEAF_V2\0"
_POSITION_LEDGER_NODE_DOMAIN: Final = b"R4B_REALIZED_FUNDING_POSITION_LEDGER_NODE_V2\0"
_POSITION_LEDGER_CHECKPOINT_DOMAIN: Final = b"R4B_REALIZED_FUNDING_POSITION_LEDGER_CHECKPOINT_V2\0"
_CASHFLOW_ID_DOMAIN: Final = b"R4B_REALIZED_FUNDING_CASHFLOW_ID_V2\0"
_CASHFLOW_PAYLOAD_DOMAIN: Final = b"R4B_REALIZED_FUNDING_CASHFLOW_PAYLOAD_V2\0"

_REGISTRY_STATE_SCHEMA: Final = "r4b_realized_funding_registry_state_v2"
_DECISION_SCHEMA: Final = "r4b_realized_funding_confirmation_v2"
_CASHFLOW_SCHEMA: Final = "r4b_realized_funding_cashflow_v2"
_DECISION_FACTORY_TOKEN: Final = object()
_POSITION_FACTORY_TOKEN: Final = object()
_CASHFLOW_FACTORY_TOKEN: Final = object()


class FundingContractErrorV2(ValueError):
    """Raised when realized-funding evidence violates the frozen V2 contract."""


class FundingHttpErrorV2(StrEnum):
    NETWORK = "NETWORK"
    TIMEOUT = "TIMEOUT"
    HTTP_STATUS = "HTTP_STATUS"
    INCOMPLETE_BODY = "INCOMPLETE_BODY"
    PROTOCOL = "PROTOCOL"


class FundingConfirmationStatusV2(StrEnum):
    CONFIRMED = "CONFIRMED"
    PENDING_CONFIRMATION = "PENDING_CONFIRMATION"
    INCONCLUSIVE_MISSING_CONFIRMATION = "INCONCLUSIVE_MISSING_CONFIRMATION"
    INCONCLUSIVE_REQUEST_MISMATCH = "INCONCLUSIVE_REQUEST_MISMATCH"
    INCONCLUSIVE_RESPONSE_MISMATCH = "INCONCLUSIVE_RESPONSE_MISMATCH"
    INCONCLUSIVE_CONFLICTING_CONFIRMATIONS = "INCONCLUSIVE_CONFLICTING_CONFIRMATIONS"
    INCONCLUSIVE_CONFLICTING_DUPLICATE = "INCONCLUSIVE_CONFLICTING_DUPLICATE"
    INCONCLUSIVE_RETRY_LINEAGE = "INCONCLUSIVE_RETRY_LINEAGE"
    INCONCLUSIVE_LATE_RESPONSE = "INCONCLUSIVE_LATE_RESPONSE"


class FundingRegistryDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


class FundingLotTimingV2(StrEnum):
    STRICTLY_BEFORE_FUNDING = "STRICTLY_BEFORE_FUNDING"
    EQUAL_MS_AMBIGUOUS = "EQUAL_MS_AMBIGUOUS"


@dataclass(frozen=True, slots=True)
class FundingScopeV2:
    attempt_id: str
    plan_id: str
    protocol_hash: str
    universe_sha256: str

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_identity(self.plan_id, "plan_id")
        _validate_sha256(self.protocol_hash, "protocol_hash")
        _validate_sha256(self.universe_sha256, "universe_sha256")


@dataclass(frozen=True, slots=True)
class FundingHttpAttemptV2:
    """One retained anonymous GET attempt; this is not a capture-membership proof."""

    scope: FundingScopeV2
    correlation_id: str
    request_number: int
    previous_attempt_payload_sha256: str | None
    request_started_ms: int
    response_completion_ms: int
    receipt_monotonic_ns: int
    ingest_seq: int
    method: str
    route_id: str
    endpoint_path: str
    canonical_query: tuple[tuple[str, str], ...]
    response_status: int | None
    content_type: str | None
    payload_complete: bool
    raw_response_bytes: bytes = field(repr=False)
    error: FundingHttpErrorV2 | None
    tls_verified: bool = True
    account_authenticated: bool = False
    authorization_header_present: bool = False
    raw_response_sha256: str = field(init=False)
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FUNDING_RULE_VERSION_V2)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, FundingScopeV2):
            raise FundingContractErrorV2("scope must be FundingScopeV2")
        _validate_identity(self.correlation_id, "correlation_id")
        _validate_positive_int(self.request_number, "request_number")
        if self.previous_attempt_payload_sha256 is not None:
            _validate_sha256(
                self.previous_attempt_payload_sha256,
                "previous_attempt_payload_sha256",
            )
        if self.request_number == 1 and self.previous_attempt_payload_sha256 is not None:
            raise FundingContractErrorV2("first request cannot name a previous attempt")
        _validate_nonnegative_int(self.request_started_ms, "request_started_ms")
        _validate_nonnegative_int(self.response_completion_ms, "response_completion_ms")
        if self.response_completion_ms < self.request_started_ms:
            raise FundingContractErrorV2("response completed before request start")
        _validate_nonnegative_int(self.receipt_monotonic_ns, "receipt_monotonic_ns")
        _validate_positive_int(self.ingest_seq, "ingest_seq")
        _validate_identity(self.method, "method")
        _validate_identity(self.route_id, "route_id")
        _validate_identity(self.endpoint_path, "endpoint_path")
        _validate_query_shape(self.canonical_query)
        if self.tls_verified is not True:
            raise FundingContractErrorV2("funding evidence requires verified TLS")
        if self.account_authenticated is not False:
            raise FundingContractErrorV2("funding evidence must be public and anonymous")
        if self.authorization_header_present is not False:
            raise FundingContractErrorV2("funding requests cannot carry authorization")
        if type(self.payload_complete) is not bool:
            raise FundingContractErrorV2("payload_complete must be boolean")
        if not isinstance(self.raw_response_bytes, bytes):
            raise FundingContractErrorV2("raw_response_bytes must be immutable bytes")
        if len(self.raw_response_bytes) > _MAX_RESPONSE_BYTES:
            raise FundingContractErrorV2("funding response exceeds the bounded body size")
        _validate_http_outcome(self)
        raw_hash = hashlib.sha256(self.raw_response_bytes).hexdigest()
        object.__setattr__(self, "raw_response_sha256", raw_hash)
        event_id = _hash_document(_ATTEMPT_ID_DOMAIN, _attempt_identity_document(self))
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = _hash_document(
            _ATTEMPT_PAYLOAD_DOMAIN,
            _attempt_document(self, include_payload_sha256=False),
        )
        object.__setattr__(self, "payload_sha256", payload_sha256)

    @property
    def successful_response(self) -> bool:
        return self.response_status == 200 and self.payload_complete and self.error is None


@dataclass(frozen=True, slots=True)
class FundingConfirmationInputV2:
    scope: FundingScopeV2
    symbol: str
    funding_time_ms: int
    horizon_end_ms: int
    horizon_max_ms: int
    observed_through_ms: int
    candidate_set_complete: bool
    maximum_attempts: int
    attempts: tuple[FundingHttpAttemptV2, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.scope, FundingScopeV2):
            raise FundingContractErrorV2("scope must be FundingScopeV2")
        _validate_symbol(self.symbol)
        for value, name in (
            (self.funding_time_ms, "funding_time_ms"),
            (self.horizon_end_ms, "horizon_end_ms"),
            (self.horizon_max_ms, "horizon_max_ms"),
            (self.observed_through_ms, "observed_through_ms"),
        ):
            _validate_nonnegative_int(value, name)
        if not (self.funding_time_ms <= self.horizon_end_ms <= self.horizon_max_ms):
            raise FundingContractErrorV2(
                "funding time, horizon end, and maximum horizon are not ordered"
            )
        if type(self.candidate_set_complete) is not bool:
            raise FundingContractErrorV2("candidate_set_complete must be boolean")
        _validate_positive_int(self.maximum_attempts, "maximum_attempts")
        if self.maximum_attempts > _MAX_REQUEST_ATTEMPTS:
            raise FundingContractErrorV2("maximum_attempts exceeds the frozen bound")
        if not isinstance(self.attempts, tuple) or len(self.attempts) > 2 * self.maximum_attempts:
            raise FundingContractErrorV2("funding attempts exceed the bounded candidate set")
        for attempt in self.attempts:
            if not isinstance(attempt, FundingHttpAttemptV2):
                raise FundingContractErrorV2("attempts must contain FundingHttpAttemptV2")
            if attempt.response_completion_ms > self.observed_through_ms:
                raise FundingContractErrorV2(
                    "candidate response lies beyond the observed-through cursor"
                )

    @property
    def confirmation_deadline_ms(self) -> int:
        return min(
            self.funding_time_ms + FUNDING_CONFIRMATION_MAX_DELAY_MS_V2,
            self.horizon_end_ms + FUNDING_HORIZON_GRACE_MS_V2,
        )


@dataclass(frozen=True, slots=True)
class FundingConfirmationDecisionV2:
    scope: FundingScopeV2
    symbol: str
    funding_time_ms: int
    horizon_end_ms: int
    horizon_max_ms: int
    confirmation_deadline_ms: int
    observed_through_ms: int
    status: FundingConfirmationStatusV2
    reasons: tuple[str, ...]
    invalidation: str
    funding_rate: Decimal | None
    mark_price: Decimal | None
    selected_response_completion_ms: int | None
    selected_attempt_event_id: str | None
    selected_attempt_payload_sha256: str | None
    candidate_attempt_count: int
    retry_count: int
    request_lineage_sha256: str
    evidence_root_sha256: str
    attempt_event_ids: tuple[str, ...]
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FUNDING_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _DECISION_FACTORY_TOKEN:
            raise FundingContractErrorV2("funding confirmations are evaluator-sealed")
        if not isinstance(self.scope, FundingScopeV2):
            raise FundingContractErrorV2("scope must be FundingScopeV2")
        _validate_symbol(self.symbol)
        for value, name in (
            (self.funding_time_ms, "funding_time_ms"),
            (self.horizon_end_ms, "horizon_end_ms"),
            (self.horizon_max_ms, "horizon_max_ms"),
            (self.confirmation_deadline_ms, "confirmation_deadline_ms"),
            (self.observed_through_ms, "observed_through_ms"),
        ):
            _validate_nonnegative_int(value, name)
        if not (self.funding_time_ms <= self.horizon_end_ms <= self.horizon_max_ms):
            raise FundingContractErrorV2("decision horizon ordering is invalid")
        expected_deadline = min(
            self.funding_time_ms + FUNDING_CONFIRMATION_MAX_DELAY_MS_V2,
            self.horizon_end_ms + FUNDING_HORIZON_GRACE_MS_V2,
        )
        if self.confirmation_deadline_ms != expected_deadline:
            raise FundingContractErrorV2("confirmation deadline differs from the rule")
        if not isinstance(self.status, FundingConfirmationStatusV2):
            raise FundingContractErrorV2("funding confirmation status is invalid")
        _validate_reasons(self.reasons)
        _validate_identity(self.invalidation, "invalidation")
        _validate_nonnegative_int(self.candidate_attempt_count, "candidate_attempt_count")
        _validate_nonnegative_int(self.retry_count, "retry_count")
        if self.retry_count != max(0, self.candidate_attempt_count - 1):
            raise FundingContractErrorV2("retry count differs from candidate census")
        _validate_sha256(self.request_lineage_sha256, "request_lineage_sha256")
        _validate_sha256(self.evidence_root_sha256, "evidence_root_sha256")
        if (
            not isinstance(self.attempt_event_ids, tuple)
            or len(self.attempt_event_ids) != self.candidate_attempt_count
        ):
            raise FundingContractErrorV2("attempt event census is invalid")
        for event_id in self.attempt_event_ids:
            _validate_sha256(event_id, "attempt_event_id")
        _validate_confirmation_state(self)
        event_id = _hash_document(_DECISION_ID_DOMAIN, _decision_identity_document(self))
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = _hash_document(
            _DECISION_PAYLOAD_DOMAIN,
            _decision_document(self, include_payload_sha256=False),
        )
        object.__setattr__(self, "payload_sha256", payload_sha256)

    @property
    def confirmed(self) -> bool:
        return self.status is FundingConfirmationStatusV2.CONFIRMED

    @property
    def terminal(self) -> bool:
        return self.status is not FundingConfirmationStatusV2.PENDING_CONFIRMATION


def evaluate_funding_confirmation_v2(
    item: FundingConfirmationInputV2,
) -> FundingConfirmationDecisionV2:
    """Resolve one fixed public funding row without making a network call."""

    if not isinstance(item, FundingConfirmationInputV2):
        raise FundingContractErrorV2("item must be FundingConfirmationInputV2")
    attempts, duplicate_conflict = _normalize_attempts(item.attempts)
    lineage_sha256 = _request_lineage_sha256(item, attempts)
    evidence_root_sha256 = _evidence_root_sha256(item, attempts)
    common = {
        "item": item,
        "attempts": attempts,
        "request_lineage_sha256": lineage_sha256,
        "evidence_root_sha256": evidence_root_sha256,
    }
    if not item.candidate_set_complete or item.observed_through_ms < item.confirmation_deadline_ms:
        return _nonconfirmed_decision(
            **common,
            status=FundingConfirmationStatusV2.PENDING_CONFIRMATION,
        )
    if duplicate_conflict:
        return _nonconfirmed_decision(
            **common,
            status=FundingConfirmationStatusV2.INCONCLUSIVE_CONFLICTING_DUPLICATE,
        )
    if not _retry_lineage_is_valid(item, attempts):
        return _nonconfirmed_decision(
            **common,
            status=FundingConfirmationStatusV2.INCONCLUSIVE_RETRY_LINEAGE,
        )
    if any(not _request_matches_target(item, attempt) for attempt in attempts):
        return _nonconfirmed_decision(
            **common,
            status=FundingConfirmationStatusV2.INCONCLUSIVE_REQUEST_MISMATCH,
        )

    parsed: list[tuple[Decimal, Decimal, FundingHttpAttemptV2]] = []
    response_mismatch = False
    late_success = False
    for attempt in attempts:
        if not attempt.successful_response:
            continue
        if attempt.response_completion_ms > item.confirmation_deadline_ms:
            late_success = True
            continue
        try:
            rate, mark = _parse_exact_funding_row(item, attempt.raw_response_bytes)
        except FundingContractErrorV2:
            response_mismatch = True
            continue
        parsed.append((rate, mark, attempt))
    if response_mismatch:
        return _nonconfirmed_decision(
            **common,
            status=FundingConfirmationStatusV2.INCONCLUSIVE_RESPONSE_MISMATCH,
        )
    unique_values = {(rate, mark) for rate, mark, _ in parsed}
    if len(unique_values) > 1:
        return _nonconfirmed_decision(
            **common,
            status=FundingConfirmationStatusV2.INCONCLUSIVE_CONFLICTING_CONFIRMATIONS,
        )
    if not parsed:
        status = (
            FundingConfirmationStatusV2.INCONCLUSIVE_LATE_RESPONSE
            if late_success
            else FundingConfirmationStatusV2.INCONCLUSIVE_MISSING_CONFIRMATION
        )
        return _nonconfirmed_decision(**common, status=status)
    rate, mark, selected = min(
        parsed,
        key=lambda row: (
            row[2].response_completion_ms,
            row[2].request_number,
            row[2].event_id,
        ),
    )
    return FundingConfirmationDecisionV2(
        scope=item.scope,
        symbol=item.symbol,
        funding_time_ms=item.funding_time_ms,
        horizon_end_ms=item.horizon_end_ms,
        horizon_max_ms=item.horizon_max_ms,
        confirmation_deadline_ms=item.confirmation_deadline_ms,
        observed_through_ms=item.observed_through_ms,
        status=FundingConfirmationStatusV2.CONFIRMED,
        reasons=("EXACT_PUBLIC_USDM_FUNDING_ROW_CONFIRMED",),
        invalidation="INVALID_IF_CAPTURE_OR_POSITION_LINEAGE_DIFFERS",
        funding_rate=rate,
        mark_price=mark,
        selected_response_completion_ms=selected.response_completion_ms,
        selected_attempt_event_id=selected.event_id,
        selected_attempt_payload_sha256=selected.payload_sha256,
        candidate_attempt_count=len(attempts),
        retry_count=max(0, len(attempts) - 1),
        request_lineage_sha256=lineage_sha256,
        evidence_root_sha256=evidence_root_sha256,
        attempt_event_ids=tuple(attempt.event_id for attempt in attempts),
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def canonical_funding_http_attempt_v2(attempt: FundingHttpAttemptV2) -> bytes:
    if not isinstance(attempt, FundingHttpAttemptV2):
        raise FundingContractErrorV2("attempt must be FundingHttpAttemptV2")
    expected = _hash_document(
        _ATTEMPT_PAYLOAD_DOMAIN,
        _attempt_document(attempt, include_payload_sha256=False),
    )
    if expected != attempt.payload_sha256:
        raise FundingContractErrorV2("funding attempt payload hash mismatch")
    return canonical_json_line(_attempt_document(attempt, include_payload_sha256=True))


def canonical_funding_confirmation_v2(
    decision: FundingConfirmationDecisionV2,
) -> bytes:
    if not isinstance(decision, FundingConfirmationDecisionV2):
        raise FundingContractErrorV2("decision must be FundingConfirmationDecisionV2")
    expected = _hash_document(
        _DECISION_PAYLOAD_DOMAIN,
        _decision_document(decision, include_payload_sha256=False),
    )
    if expected != decision.payload_sha256:
        raise FundingContractErrorV2("funding decision payload hash mismatch")
    return canonical_json_line(_decision_document(decision, include_payload_sha256=True))


@dataclass(frozen=True, slots=True)
class FundingRegistryCheckpointV2:
    scope: FundingScopeV2
    replay_root_sha256: str
    event_count: int
    maximum_events: int
    checkpoint_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FUNDING_RULE_VERSION_V2)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, FundingScopeV2):
            raise FundingContractErrorV2("checkpoint scope must be FundingScopeV2")
        _validate_sha256(self.replay_root_sha256, "replay_root_sha256")
        _validate_nonnegative_int(self.event_count, "event_count")
        _validate_positive_int(self.maximum_events, "maximum_events")
        if self.event_count > self.maximum_events:
            raise FundingContractErrorV2("checkpoint event count exceeds capacity")
        checkpoint_sha256 = _hash_document(
            _REGISTRY_CHECKPOINT_DOMAIN,
            _registry_checkpoint_document(self),
        )
        object.__setattr__(self, "checkpoint_sha256", checkpoint_sha256)


class FundingConfirmationRegistryV2:
    """Bounded terminal-decision registry with externally pinned restart state."""

    def __init__(self, *, maximum_events: int, scope: FundingScopeV2) -> None:
        _validate_positive_int(maximum_events, "maximum_events")
        if not isinstance(scope, FundingScopeV2):
            raise FundingContractErrorV2("registry scope must be FundingScopeV2")
        self._maximum_events = maximum_events
        self._scope = scope
        self._decisions: dict[str, FundingConfirmationDecisionV2] = {}

    @property
    def scope(self) -> FundingScopeV2:
        return self._scope

    @property
    def maximum_events(self) -> int:
        return self._maximum_events

    @property
    def event_count(self) -> int:
        return len(self._decisions)

    @property
    def replay_root_sha256(self) -> str:
        return _registry_replay_root(
            self._ordered_state_rows(),
            scope=self._scope,
            maximum_events=self._maximum_events,
        )

    def terminal_checkpoint_v2(self) -> FundingRegistryCheckpointV2:
        return FundingRegistryCheckpointV2(
            scope=self._scope,
            replay_root_sha256=self.replay_root_sha256,
            event_count=self.event_count,
            maximum_events=self._maximum_events,
        )

    def contains_exact_v2(self, decision: FundingConfirmationDecisionV2) -> bool:
        if not isinstance(decision, FundingConfirmationDecisionV2):
            raise FundingContractErrorV2("decision has the wrong type")
        prior = self._decisions.get(decision.event_id)
        return prior is not None and canonical_funding_confirmation_v2(
            prior
        ) == canonical_funding_confirmation_v2(decision)

    def register(
        self,
        decision: FundingConfirmationDecisionV2,
    ) -> FundingRegistryDispositionV2:
        if not isinstance(decision, FundingConfirmationDecisionV2):
            raise FundingContractErrorV2("registry accepts FundingConfirmationDecisionV2 only")
        canonical = canonical_funding_confirmation_v2(decision)
        if decision.scope != self._scope:
            raise FundingContractErrorV2("registry rejects a different funding scope")
        if not decision.terminal:
            raise FundingContractErrorV2("pending funding views are not registry rows")
        prior = self._decisions.get(decision.event_id)
        if prior is not None:
            if canonical_funding_confirmation_v2(prior) != canonical:
                raise FundingContractErrorV2(
                    "deterministic funding event ID collides with different evidence"
                )
            return FundingRegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if len(self._decisions) >= self._maximum_events:
            raise FundingContractErrorV2("bounded funding registry capacity exhausted")
        self._decisions[decision.event_id] = decision
        return FundingRegistryDispositionV2.NEW

    def export_state_v2(self) -> bytes:
        rows = self._ordered_state_rows()
        checkpoint = self.terminal_checkpoint_v2()
        return canonical_json_line(
            {
                "checkpoint_sha256": checkpoint.checkpoint_sha256,
                "event_count": checkpoint.event_count,
                "events": rows,
                "maximum_events": self._maximum_events,
                "replay_root_sha256": checkpoint.replay_root_sha256,
                "schema_version": _REGISTRY_STATE_SCHEMA,
                "scope": _scope_document(self._scope),
            }
        )

    @classmethod
    def from_state_v2(
        cls,
        payload: bytes,
        *,
        expected_replay_root_sha256: str,
        expected_event_count: int,
        expected_maximum_events: int,
        expected_scope: FundingScopeV2,
        expected_checkpoint_sha256: str,
    ) -> FundingConfirmationRegistryV2:
        _validate_sha256(expected_replay_root_sha256, "expected_replay_root_sha256")
        _validate_nonnegative_int(expected_event_count, "expected_event_count")
        _validate_positive_int(expected_maximum_events, "expected_maximum_events")
        if expected_event_count > expected_maximum_events:
            raise FundingContractErrorV2("expected event count exceeds capacity")
        if not isinstance(expected_scope, FundingScopeV2):
            raise FundingContractErrorV2("expected_scope must be FundingScopeV2")
        _validate_sha256(expected_checkpoint_sha256, "expected_checkpoint_sha256")
        expected_checkpoint = FundingRegistryCheckpointV2(
            scope=expected_scope,
            replay_root_sha256=expected_replay_root_sha256,
            event_count=expected_event_count,
            maximum_events=expected_maximum_events,
        )
        if expected_checkpoint.checkpoint_sha256 != expected_checkpoint_sha256:
            raise FundingContractErrorV2("external funding checkpoint hash mismatch")
        document = _parse_canonical_json(payload, "funding registry state")
        if (
            set(document)
            != {
                "checkpoint_sha256",
                "event_count",
                "events",
                "maximum_events",
                "replay_root_sha256",
                "schema_version",
                "scope",
            }
            or document.get("schema_version") != _REGISTRY_STATE_SCHEMA
        ):
            raise FundingContractErrorV2("funding registry state schema is unsupported")
        if document.get("scope") != _scope_document(expected_scope):
            raise FundingContractErrorV2("funding registry scope differs from pin")
        if document.get("event_count") != expected_event_count:
            raise FundingContractErrorV2("funding registry count differs from pin")
        if document.get("maximum_events") != expected_maximum_events:
            raise FundingContractErrorV2("funding registry capacity differs from pin")
        if document.get("checkpoint_sha256") != expected_checkpoint_sha256:
            raise FundingContractErrorV2("funding state checkpoint differs from pin")
        raw_rows = document.get("events")
        if not isinstance(raw_rows, list) or len(raw_rows) != expected_event_count:
            raise FundingContractErrorV2("funding registry event census is invalid")
        registry = cls(maximum_events=expected_maximum_events, scope=expected_scope)
        prior_key: tuple[int, str, str] | None = None
        canonical_rows: list[dict[str, object]] = []
        for raw_row in raw_rows:
            row, decision, order_key = _parse_registry_row(
                raw_row,
                expected_scope=expected_scope,
            )
            if prior_key is not None and order_key <= prior_key:
                raise FundingContractErrorV2("funding registry rows are not in strict replay order")
            prior_key = order_key
            registry.register(decision)
            canonical_rows.append(row)
        replay_root = _registry_replay_root(
            canonical_rows,
            scope=expected_scope,
            maximum_events=expected_maximum_events,
        )
        if document.get("replay_root_sha256") != replay_root:
            raise FundingContractErrorV2("funding registry replay root is invalid")
        if replay_root != expected_replay_root_sha256:
            raise FundingContractErrorV2("funding registry replay root differs from pin")
        return registry

    def _ordered_state_rows(self) -> list[dict[str, object]]:
        return [
            _registry_state_row(decision)
            for decision in sorted(
                self._decisions.values(),
                key=lambda item: (item.funding_time_ms, item.symbol, item.event_id),
            )
        ]


@dataclass(frozen=True, slots=True)
class FundingPositionLedgerCheckpointV2:
    """Externally pinned execution/position-ledger root; authority remains upstream."""

    scope: FundingScopeV2
    ledger_id: str
    ledger_root_sha256: str
    event_count: int
    observed_through_ms: int
    sealed_at_ms: int
    checkpoint_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FUNDING_RULE_VERSION_V2)

    def __post_init__(self) -> None:
        if not isinstance(self.scope, FundingScopeV2):
            raise FundingContractErrorV2("position checkpoint scope must be FundingScopeV2")
        _validate_identity(self.ledger_id, "ledger_id")
        _validate_sha256(self.ledger_root_sha256, "ledger_root_sha256")
        _validate_positive_int(self.event_count, "event_count")
        _validate_nonnegative_int(self.observed_through_ms, "observed_through_ms")
        _validate_nonnegative_int(self.sealed_at_ms, "sealed_at_ms")
        if self.sealed_at_ms < self.observed_through_ms:
            raise FundingContractErrorV2("position checkpoint seal predates its cursor")
        checkpoint_sha256 = _hash_document(
            _POSITION_LEDGER_CHECKPOINT_DOMAIN,
            _position_ledger_checkpoint_document(self),
        )
        object.__setattr__(self, "checkpoint_sha256", checkpoint_sha256)


@dataclass(frozen=True, slots=True)
class FundingPositionSnapshotV2:
    scope: FundingScopeV2
    symbol: str
    funding_time_ms: int
    horizon_max_ms: int
    position_event_id: str
    position_payload_sha256: str
    position_source_root_sha256: str
    signed_quantity_before_funding: Decimal
    contract_multiplier: Decimal
    contract_multiplier_version_sha256: str
    quantity_timestamp_ms: int
    lot_timing: FundingLotTimingV2
    position_ledger_checkpoint_sha256: str
    position_ledger_leaf_sha256: str
    position_ledger_leaf_index: int
    position_ledger_merkle_siblings: tuple[str, ...]
    _factory_token: InitVar[object | None] = None
    snapshot_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FUNDING_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _POSITION_FACTORY_TOKEN:
            raise FundingContractErrorV2(
                "funding position snapshots require externally pinned membership"
            )
        if not isinstance(self.scope, FundingScopeV2):
            raise FundingContractErrorV2("position scope must be FundingScopeV2")
        _validate_symbol(self.symbol)
        _validate_nonnegative_int(self.funding_time_ms, "funding_time_ms")
        _validate_nonnegative_int(self.horizon_max_ms, "horizon_max_ms")
        if self.funding_time_ms > self.horizon_max_ms:
            raise FundingContractErrorV2("position funding time exceeds maximum horizon")
        _validate_sha256(self.position_event_id, "position_event_id")
        _validate_sha256(self.position_payload_sha256, "position_payload_sha256")
        _validate_sha256(self.position_source_root_sha256, "position_source_root_sha256")
        _validate_finite_decimal(
            self.signed_quantity_before_funding,
            "signed_quantity_before_funding",
        )
        _validate_positive_finite_decimal(
            self.contract_multiplier,
            "contract_multiplier",
        )
        _validate_sha256(
            self.contract_multiplier_version_sha256,
            "contract_multiplier_version_sha256",
        )
        _validate_nonnegative_int(self.quantity_timestamp_ms, "quantity_timestamp_ms")
        if not isinstance(self.lot_timing, FundingLotTimingV2):
            raise FundingContractErrorV2("lot_timing must be FundingLotTimingV2")
        if self.lot_timing is FundingLotTimingV2.STRICTLY_BEFORE_FUNDING:
            if self.quantity_timestamp_ms >= self.funding_time_ms:
                raise FundingContractErrorV2(
                    "unambiguous quantity must be timestamped strictly before funding"
                )
        elif self.quantity_timestamp_ms != self.funding_time_ms:
            raise FundingContractErrorV2(
                "equal-ms ambiguity must be timestamped exactly at funding"
            )
        _validate_sha256(
            self.position_ledger_checkpoint_sha256,
            "position_ledger_checkpoint_sha256",
        )
        _validate_sha256(
            self.position_ledger_leaf_sha256,
            "position_ledger_leaf_sha256",
        )
        _validate_nonnegative_int(
            self.position_ledger_leaf_index,
            "position_ledger_leaf_index",
        )
        if not isinstance(self.position_ledger_merkle_siblings, tuple):
            raise FundingContractErrorV2("position ledger siblings must be a tuple")
        for sibling in self.position_ledger_merkle_siblings:
            _validate_sha256(sibling, "position ledger sibling")
        snapshot_sha256 = _hash_document(
            _POSITION_SNAPSHOT_DOMAIN,
            _position_snapshot_document(self),
        )
        object.__setattr__(self, "snapshot_sha256", snapshot_sha256)


def funding_position_ledger_leaf_sha256_v2(
    *,
    scope: FundingScopeV2,
    symbol: str,
    funding_time_ms: int,
    horizon_max_ms: int,
    position_event_id: str,
    position_payload_sha256: str,
    position_source_root_sha256: str,
    signed_quantity_before_funding: Decimal,
    contract_multiplier: Decimal,
    contract_multiplier_version_sha256: str,
    quantity_timestamp_ms: int,
    lot_timing: FundingLotTimingV2,
) -> str:
    """Derive the exact position-ledger leaf committed by the external checkpoint."""

    document = _position_quantity_document(
        scope=scope,
        symbol=symbol,
        funding_time_ms=funding_time_ms,
        horizon_max_ms=horizon_max_ms,
        position_event_id=position_event_id,
        position_payload_sha256=position_payload_sha256,
        position_source_root_sha256=position_source_root_sha256,
        signed_quantity_before_funding=signed_quantity_before_funding,
        contract_multiplier=contract_multiplier,
        contract_multiplier_version_sha256=contract_multiplier_version_sha256,
        quantity_timestamp_ms=quantity_timestamp_ms,
        lot_timing=lot_timing,
    )
    return _hash_document(_POSITION_LEDGER_LEAF_DOMAIN, document)


def funding_position_ledger_root_v2(leaves: Sequence[str]) -> str:
    """Build the deterministic duplicate-last Merkle root used by position proofs."""

    if isinstance(leaves, (str, bytes)) or not isinstance(leaves, Sequence):
        raise FundingContractErrorV2("position leaves must be a finite digest sequence")
    level = list(leaves)
    if not level:
        raise FundingContractErrorV2("position ledger requires at least one leaf")
    for leaf in level:
        _validate_sha256(leaf, "position ledger leaf")
    while len(level) > 1:
        if len(level) % 2:
            level.append(level[-1])
        level = [
            _position_merkle_parent(level[index], level[index + 1])
            for index in range(0, len(level), 2)
        ]
    return level[0]


def build_funding_position_snapshot_v2(
    *,
    scope: FundingScopeV2,
    symbol: str,
    funding_time_ms: int,
    horizon_max_ms: int,
    position_event_id: str,
    position_payload_sha256: str,
    position_source_root_sha256: str,
    signed_quantity_before_funding: Decimal,
    contract_multiplier: Decimal,
    contract_multiplier_version_sha256: str,
    quantity_timestamp_ms: int,
    lot_timing: FundingLotTimingV2,
    ledger_checkpoint: FundingPositionLedgerCheckpointV2,
    expected_ledger_checkpoint_sha256: str,
    ledger_leaf_index: int,
    ledger_merkle_siblings: tuple[str, ...],
) -> FundingPositionSnapshotV2:
    """Seal Q-before-F only after external checkpoint and Merkle membership checks."""

    if not isinstance(ledger_checkpoint, FundingPositionLedgerCheckpointV2):
        raise FundingContractErrorV2("ledger_checkpoint must be FundingPositionLedgerCheckpointV2")
    _validate_sha256(
        expected_ledger_checkpoint_sha256,
        "expected_ledger_checkpoint_sha256",
    )
    if ledger_checkpoint.checkpoint_sha256 != expected_ledger_checkpoint_sha256:
        raise FundingContractErrorV2("position ledger checkpoint differs from external pin")
    if ledger_checkpoint.scope != scope:
        raise FundingContractErrorV2("position ledger and requested scopes differ")
    if ledger_checkpoint.observed_through_ms < funding_time_ms:
        raise FundingContractErrorV2(
            "position ledger does not observe the quantity state through fundingTime"
        )
    leaf = funding_position_ledger_leaf_sha256_v2(
        scope=scope,
        symbol=symbol,
        funding_time_ms=funding_time_ms,
        horizon_max_ms=horizon_max_ms,
        position_event_id=position_event_id,
        position_payload_sha256=position_payload_sha256,
        position_source_root_sha256=position_source_root_sha256,
        signed_quantity_before_funding=signed_quantity_before_funding,
        contract_multiplier=contract_multiplier,
        contract_multiplier_version_sha256=contract_multiplier_version_sha256,
        quantity_timestamp_ms=quantity_timestamp_ms,
        lot_timing=lot_timing,
    )
    _verify_position_membership(
        leaf_sha256=leaf,
        leaf_index=ledger_leaf_index,
        event_count=ledger_checkpoint.event_count,
        siblings=ledger_merkle_siblings,
        expected_root_sha256=ledger_checkpoint.ledger_root_sha256,
    )
    return FundingPositionSnapshotV2(
        scope=scope,
        symbol=symbol,
        funding_time_ms=funding_time_ms,
        horizon_max_ms=horizon_max_ms,
        position_event_id=position_event_id,
        position_payload_sha256=position_payload_sha256,
        position_source_root_sha256=position_source_root_sha256,
        signed_quantity_before_funding=signed_quantity_before_funding,
        contract_multiplier=contract_multiplier,
        contract_multiplier_version_sha256=contract_multiplier_version_sha256,
        quantity_timestamp_ms=quantity_timestamp_ms,
        lot_timing=lot_timing,
        position_ledger_checkpoint_sha256=ledger_checkpoint.checkpoint_sha256,
        position_ledger_leaf_sha256=leaf,
        position_ledger_leaf_index=ledger_leaf_index,
        position_ledger_merkle_siblings=ledger_merkle_siblings,
        _factory_token=_POSITION_FACTORY_TOKEN,
    )


@dataclass(frozen=True, slots=True)
class RealizedFundingCashflowV2:
    scope: FundingScopeV2
    symbol: str
    funding_time_ms: int
    horizon_max_ms: int
    confirmation_event_id: str
    confirmation_payload_sha256: str
    confirmation_evidence_root_sha256: str
    registry_checkpoint_sha256: str
    position_snapshot_sha256: str
    position_ledger_checkpoint_sha256: str
    funding_rate: Decimal
    mark_price: Decimal
    market_value_time_ms: int
    signed_quantity_before_funding: Decimal
    contract_multiplier: Decimal
    lot_timing: FundingLotTimingV2
    normal_cashflow: Decimal
    realized_cashflow: Decimal
    reasons: tuple[str, ...]
    invalidation: str
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FUNDING_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CASHFLOW_FACTORY_TOKEN:
            raise FundingContractErrorV2("realized funding cashflows are factory-sealed")
        if not isinstance(self.scope, FundingScopeV2):
            raise FundingContractErrorV2("cashflow scope must be FundingScopeV2")
        _validate_symbol(self.symbol)
        _validate_nonnegative_int(self.funding_time_ms, "funding_time_ms")
        _validate_nonnegative_int(self.horizon_max_ms, "horizon_max_ms")
        if self.funding_time_ms > self.horizon_max_ms:
            raise FundingContractErrorV2("cashflow funding time exceeds maximum horizon")
        for value, name in (
            (self.confirmation_event_id, "confirmation_event_id"),
            (self.confirmation_payload_sha256, "confirmation_payload_sha256"),
            (
                self.confirmation_evidence_root_sha256,
                "confirmation_evidence_root_sha256",
            ),
            (self.registry_checkpoint_sha256, "registry_checkpoint_sha256"),
            (self.position_snapshot_sha256, "position_snapshot_sha256"),
            (
                self.position_ledger_checkpoint_sha256,
                "position_ledger_checkpoint_sha256",
            ),
        ):
            _validate_sha256(value, name)
        _validate_finite_decimal(self.funding_rate, "funding_rate")
        _validate_positive_finite_decimal(self.mark_price, "mark_price")
        _validate_nonnegative_int(self.market_value_time_ms, "market_value_time_ms")
        if self.market_value_time_ms != self.funding_time_ms:
            raise FundingContractErrorV2(
                "funding cashflow may use only the mark attached to fundingTime"
            )
        _validate_finite_decimal(
            self.signed_quantity_before_funding,
            "signed_quantity_before_funding",
        )
        _validate_positive_finite_decimal(
            self.contract_multiplier,
            "contract_multiplier",
        )
        if not isinstance(self.lot_timing, FundingLotTimingV2):
            raise FundingContractErrorV2("lot_timing is invalid")
        _validate_finite_decimal(self.normal_cashflow, "normal_cashflow")
        _validate_finite_decimal(self.realized_cashflow, "realized_cashflow")
        expected_normal, expected_realized = _funding_cashflows(
            signed_quantity=self.signed_quantity_before_funding,
            contract_multiplier=self.contract_multiplier,
            mark_price=self.mark_price,
            funding_rate=self.funding_rate,
            lot_timing=self.lot_timing,
        )
        if self.normal_cashflow != expected_normal or self.realized_cashflow != expected_realized:
            raise FundingContractErrorV2("funding cashflow differs from Decimal34 rule")
        _validate_reasons(self.reasons)
        _validate_identity(self.invalidation, "invalidation")
        event_id = _hash_document(_CASHFLOW_ID_DOMAIN, _cashflow_identity_document(self))
        object.__setattr__(self, "event_id", event_id)
        payload_sha256 = _hash_document(
            _CASHFLOW_PAYLOAD_DOMAIN,
            _cashflow_document(self, include_payload_sha256=False),
        )
        object.__setattr__(self, "payload_sha256", payload_sha256)


def calculate_realized_funding_cashflow_v2(
    confirmation: FundingConfirmationDecisionV2,
    position: FundingPositionSnapshotV2,
    *,
    registry: FundingConfirmationRegistryV2,
    externally_pinned_checkpoint_sha256: str,
) -> RealizedFundingCashflowV2:
    """Calculate only from a terminal, registry-pinned exact public confirmation."""

    if not isinstance(confirmation, FundingConfirmationDecisionV2):
        raise FundingContractErrorV2("confirmation has the wrong type")
    if not confirmation.confirmed:
        raise FundingContractErrorV2("cashflow requires a confirmed funding row")
    if not isinstance(position, FundingPositionSnapshotV2):
        raise FundingContractErrorV2("position has the wrong type")
    if not isinstance(registry, FundingConfirmationRegistryV2):
        raise FundingContractErrorV2("registry has the wrong type")
    _validate_sha256(
        externally_pinned_checkpoint_sha256,
        "externally_pinned_checkpoint_sha256",
    )
    checkpoint = registry.terminal_checkpoint_v2()
    if checkpoint.checkpoint_sha256 != externally_pinned_checkpoint_sha256:
        raise FundingContractErrorV2("funding registry differs from external pin")
    if not registry.contains_exact_v2(confirmation):
        raise FundingContractErrorV2("confirmation is absent from pinned registry")
    if (
        position.scope != confirmation.scope
        or position.symbol != confirmation.symbol
        or position.funding_time_ms != confirmation.funding_time_ms
        or position.horizon_max_ms != confirmation.horizon_max_ms
    ):
        raise FundingContractErrorV2("position and confirmation identity differ")
    assert confirmation.funding_rate is not None
    assert confirmation.mark_price is not None
    normal, realized = _funding_cashflows(
        signed_quantity=position.signed_quantity_before_funding,
        contract_multiplier=position.contract_multiplier,
        mark_price=confirmation.mark_price,
        funding_rate=confirmation.funding_rate,
        lot_timing=position.lot_timing,
    )
    reason = (
        "EQUAL_MS_AMBIGUITY_ADVERSE_ONLY"
        if position.lot_timing is FundingLotTimingV2.EQUAL_MS_AMBIGUOUS
        else "SIGNED_PRE_FUNDING_POSITION_CASHFLOW"
    )
    return RealizedFundingCashflowV2(
        scope=confirmation.scope,
        symbol=confirmation.symbol,
        funding_time_ms=confirmation.funding_time_ms,
        horizon_max_ms=confirmation.horizon_max_ms,
        confirmation_event_id=confirmation.event_id,
        confirmation_payload_sha256=confirmation.payload_sha256,
        confirmation_evidence_root_sha256=confirmation.evidence_root_sha256,
        registry_checkpoint_sha256=checkpoint.checkpoint_sha256,
        position_snapshot_sha256=position.snapshot_sha256,
        position_ledger_checkpoint_sha256=(position.position_ledger_checkpoint_sha256),
        funding_rate=confirmation.funding_rate,
        mark_price=confirmation.mark_price,
        market_value_time_ms=confirmation.funding_time_ms,
        signed_quantity_before_funding=position.signed_quantity_before_funding,
        contract_multiplier=position.contract_multiplier,
        lot_timing=position.lot_timing,
        normal_cashflow=normal,
        realized_cashflow=realized,
        reasons=(reason,),
        invalidation="INVALID_IF_CONFIRMATION_POSITION_OR_EXTERNAL_PIN_DIFFERS",
        _factory_token=_CASHFLOW_FACTORY_TOKEN,
    )


def canonical_realized_funding_cashflow_v2(
    cashflow: RealizedFundingCashflowV2,
) -> bytes:
    if not isinstance(cashflow, RealizedFundingCashflowV2):
        raise FundingContractErrorV2("cashflow must be RealizedFundingCashflowV2")
    expected = _hash_document(
        _CASHFLOW_PAYLOAD_DOMAIN,
        _cashflow_document(cashflow, include_payload_sha256=False),
    )
    if expected != cashflow.payload_sha256:
        raise FundingContractErrorV2("funding cashflow payload hash mismatch")
    return canonical_json_line(_cashflow_document(cashflow, include_payload_sha256=True))


def _nonconfirmed_decision(
    *,
    item: FundingConfirmationInputV2,
    attempts: tuple[FundingHttpAttemptV2, ...],
    request_lineage_sha256: str,
    evidence_root_sha256: str,
    status: FundingConfirmationStatusV2,
) -> FundingConfirmationDecisionV2:
    reasons = {
        FundingConfirmationStatusV2.PENDING_CONFIRMATION: (
            "AWAITING_COMPLETE_CANDIDATE_SET_THROUGH_DEADLINE",
        ),
        FundingConfirmationStatusV2.INCONCLUSIVE_MISSING_CONFIRMATION: (
            "NO_EXACT_PUBLIC_FUNDING_ROW_BY_DEADLINE",
        ),
        FundingConfirmationStatusV2.INCONCLUSIVE_REQUEST_MISMATCH: (
            "RECORDED_REQUEST_DIFFERS_FROM_EXACT_FUNDING_ROUTE",
        ),
        FundingConfirmationStatusV2.INCONCLUSIVE_RESPONSE_MISMATCH: (
            "RESPONSE_IS_NOT_EXACTLY_ONE_MATCHING_FUNDING_ROW",
        ),
        FundingConfirmationStatusV2.INCONCLUSIVE_CONFLICTING_CONFIRMATIONS: (
            "MATCHING_RESPONSES_DISAGREE_ON_RATE_OR_MARK",
        ),
        FundingConfirmationStatusV2.INCONCLUSIVE_CONFLICTING_DUPLICATE: (
            "SAME_REQUEST_ID_HAS_DIFFERENT_RETAINED_PAYLOAD",
        ),
        FundingConfirmationStatusV2.INCONCLUSIVE_RETRY_LINEAGE: (
            "RETRY_CHAIN_IS_NOT_CONTIGUOUS_AND_CAUSAL",
        ),
        FundingConfirmationStatusV2.INCONCLUSIVE_LATE_RESPONSE: (
            "FUNDING_RESPONSE_COMPLETED_AFTER_DEADLINE",
        ),
    }
    if status not in reasons:
        raise FundingContractErrorV2("nonconfirmed decision status is invalid")
    return FundingConfirmationDecisionV2(
        scope=item.scope,
        symbol=item.symbol,
        funding_time_ms=item.funding_time_ms,
        horizon_end_ms=item.horizon_end_ms,
        horizon_max_ms=item.horizon_max_ms,
        confirmation_deadline_ms=item.confirmation_deadline_ms,
        observed_through_ms=item.observed_through_ms,
        status=status,
        reasons=reasons[status],
        invalidation="NO_REALIZED_CASHFLOW_WITHOUT_EXACT_CONFIRMATION",
        funding_rate=None,
        mark_price=None,
        selected_response_completion_ms=None,
        selected_attempt_event_id=None,
        selected_attempt_payload_sha256=None,
        candidate_attempt_count=len(attempts),
        retry_count=max(0, len(attempts) - 1),
        request_lineage_sha256=request_lineage_sha256,
        evidence_root_sha256=evidence_root_sha256,
        attempt_event_ids=tuple(attempt.event_id for attempt in attempts),
        _factory_token=_DECISION_FACTORY_TOKEN,
    )


def _normalize_attempts(
    attempts: tuple[FundingHttpAttemptV2, ...],
) -> tuple[tuple[FundingHttpAttemptV2, ...], bool]:
    by_event_id: dict[str, FundingHttpAttemptV2] = {}
    conflict = False
    for attempt in attempts:
        prior = by_event_id.get(attempt.event_id)
        if prior is None:
            by_event_id[attempt.event_id] = attempt
        else:
            prior_payload = canonical_funding_http_attempt_v2(prior)
            attempt_payload = canonical_funding_http_attempt_v2(attempt)
            if prior_payload != attempt_payload:
                conflict = True
                if attempt_payload < prior_payload:
                    by_event_id[attempt.event_id] = attempt
    normalized = tuple(
        sorted(
            by_event_id.values(),
            key=lambda item: (item.request_number, item.event_id),
        )
    )
    return normalized, conflict


def _retry_lineage_is_valid(
    item: FundingConfirmationInputV2,
    attempts: tuple[FundingHttpAttemptV2, ...],
) -> bool:
    if len(attempts) > item.maximum_attempts:
        return False
    if not attempts:
        return True
    correlation_id = attempts[0].correlation_id
    prior: FundingHttpAttemptV2 | None = None
    for expected_number, attempt in enumerate(attempts, start=1):
        if (
            attempt.scope != item.scope
            or attempt.correlation_id != correlation_id
            or attempt.request_number != expected_number
            or attempt.request_number > item.maximum_attempts
        ):
            return False
        if prior is None:
            if attempt.previous_attempt_payload_sha256 is not None:
                return False
        elif (
            attempt.previous_attempt_payload_sha256 != prior.payload_sha256
            or attempt.request_started_ms < prior.response_completion_ms
            or attempt.ingest_seq <= prior.ingest_seq
        ):
            return False
        prior = attempt
    return True


def _request_matches_target(
    item: FundingConfirmationInputV2,
    attempt: FundingHttpAttemptV2,
) -> bool:
    return (
        attempt.method == "GET"
        and attempt.route_id == FUNDING_ROUTE_ID_V2
        and attempt.endpoint_path == FUNDING_ENDPOINT_PATH_V2
        and attempt.canonical_query == _expected_query(item.symbol, item.funding_time_ms)
        and attempt.request_started_ms >= item.funding_time_ms
    )


def _expected_query(symbol: str, funding_time_ms: int) -> tuple[tuple[str, str], ...]:
    return (
        ("endTime", str(funding_time_ms)),
        ("limit", "1"),
        ("startTime", str(funding_time_ms)),
        ("symbol", symbol),
    )


def _parse_exact_funding_row(
    item: FundingConfirmationInputV2,
    payload: bytes,
) -> tuple[Decimal, Decimal]:
    try:
        document = json.loads(
            payload,
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, FundingContractErrorV2) as exc:
        raise FundingContractErrorV2("funding response is not strict JSON") from exc
    if not isinstance(document, list) or len(document) != 1:
        raise FundingContractErrorV2("funding response must contain exactly one row")
    row = document[0]
    if not isinstance(row, dict) or set(row) != {
        "fundingRate",
        "fundingTime",
        "markPrice",
        "symbol",
    }:
        raise FundingContractErrorV2("funding response row schema is not exact")
    if row.get("symbol") != item.symbol or row.get("fundingTime") != item.funding_time_ms:
        raise FundingContractErrorV2("funding response identity differs from target")
    rate = _parse_decimal_text(row.get("fundingRate"), "fundingRate")
    mark = _parse_decimal_text(row.get("markPrice"), "markPrice")
    if mark <= 0:
        raise FundingContractErrorV2("markPrice must be positive")
    return rate, mark


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise FundingContractErrorV2("funding JSON repeats an object key")
        document[key] = value
    return document


def _reject_json_constant(value: str) -> object:
    raise FundingContractErrorV2(f"funding JSON contains forbidden constant {value}")


def _parse_decimal_text(value: object, field_name: str) -> Decimal:
    if not isinstance(value, str) or len(value) > 128 or _DECIMAL_TEXT_RE.fullmatch(value) is None:
        raise FundingContractErrorV2(f"{field_name} must be a strict decimal string")
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise FundingContractErrorV2(f"{field_name} must be finite")
    return parsed


def _funding_cashflows(
    *,
    signed_quantity: Decimal,
    contract_multiplier: Decimal,
    mark_price: Decimal,
    funding_rate: Decimal,
    lot_timing: FundingLotTimingV2,
) -> tuple[Decimal, Decimal]:
    with localcontext(protocol_decimal_context_v2()) as context:
        exposure = context.multiply(signed_quantity, contract_multiplier)
        marked_exposure = context.multiply(exposure, mark_price)
        normal = context.minus(context.multiply(marked_exposure, funding_rate))
    normal = _canonical_zero(normal)
    realized = normal
    if lot_timing is FundingLotTimingV2.EQUAL_MS_AMBIGUOUS and normal > 0:
        realized = Decimal(0)
    return normal, _canonical_zero(realized)


def _request_lineage_sha256(
    item: FundingConfirmationInputV2,
    attempts: tuple[FundingHttpAttemptV2, ...],
) -> str:
    return _hash_document(
        _LINEAGE_DOMAIN,
        {
            "attempts": [
                {
                    "event_id": attempt.event_id,
                    "payload_sha256": attempt.payload_sha256,
                    "previous_attempt_payload_sha256": (attempt.previous_attempt_payload_sha256),
                    "request_number": attempt.request_number,
                }
                for attempt in attempts
            ],
            "maximum_attempts": item.maximum_attempts,
            "schema_version": "r4b_realized_funding_request_lineage_v2",
            "scope": _scope_document(item.scope),
            "symbol": item.symbol,
            "funding_time_ms": item.funding_time_ms,
        },
    )


def _evidence_root_sha256(
    item: FundingConfirmationInputV2,
    attempts: tuple[FundingHttpAttemptV2, ...],
) -> str:
    return _hash_document(
        _EVIDENCE_ROOT_DOMAIN,
        {
            "attempts": [
                {
                    "event_id": attempt.event_id,
                    "payload_sha256": attempt.payload_sha256,
                }
                for attempt in attempts
            ],
            "candidate_set_complete": item.candidate_set_complete,
            "confirmation_deadline_ms": item.confirmation_deadline_ms,
            "observed_through_ms": item.observed_through_ms,
            "schema_version": "r4b_realized_funding_evidence_root_v2",
        },
    )


def _validate_confirmation_state(decision: FundingConfirmationDecisionV2) -> None:
    optional_values = (
        decision.funding_rate,
        decision.mark_price,
        decision.selected_response_completion_ms,
        decision.selected_attempt_event_id,
        decision.selected_attempt_payload_sha256,
    )
    if decision.confirmed:
        if any(value is None for value in optional_values):
            raise FundingContractErrorV2("confirmed decision lacks row evidence")
        assert decision.funding_rate is not None
        assert decision.mark_price is not None
        assert decision.selected_response_completion_ms is not None
        assert decision.selected_attempt_event_id is not None
        assert decision.selected_attempt_payload_sha256 is not None
        _validate_finite_decimal(decision.funding_rate, "funding_rate")
        _validate_positive_finite_decimal(decision.mark_price, "mark_price")
        if decision.selected_response_completion_ms > decision.confirmation_deadline_ms:
            raise FundingContractErrorV2("confirmed response completed after deadline")
        _validate_sha256(decision.selected_attempt_event_id, "selected_attempt_event_id")
        _validate_sha256(
            decision.selected_attempt_payload_sha256,
            "selected_attempt_payload_sha256",
        )
        if decision.selected_attempt_event_id not in decision.attempt_event_ids:
            raise FundingContractErrorV2("selected attempt is absent from evidence census")
    elif any(value is not None for value in optional_values):
        raise FundingContractErrorV2("nonconfirmed decision cannot carry row values")


def _validate_http_outcome(attempt: FundingHttpAttemptV2) -> None:
    status = attempt.response_status
    if isinstance(status, bool) or (status is not None and not 100 <= status <= 599):
        raise FundingContractErrorV2("response_status must be an HTTP status")
    if attempt.error is not None and not isinstance(attempt.error, FundingHttpErrorV2):
        raise FundingContractErrorV2("error must be FundingHttpErrorV2")
    if status == 200:
        if not attempt.payload_complete or attempt.error is not None:
            raise FundingContractErrorV2("HTTP 200 funding response must be complete")
        if not isinstance(attempt.content_type, str) or not attempt.content_type.lower().startswith(
            "application/json"
        ):
            raise FundingContractErrorV2("HTTP 200 funding response requires JSON")
        return
    if attempt.error is None:
        raise FundingContractErrorV2("unsuccessful funding attempts require an error")
    if status is None:
        if attempt.payload_complete or attempt.content_type is not None:
            raise FundingContractErrorV2("pre-response failure cannot be complete")
    elif attempt.error is FundingHttpErrorV2.HTTP_STATUS:
        if 200 <= status < 300 or not attempt.payload_complete:
            raise FundingContractErrorV2("HTTP_STATUS requires a complete non-2xx body")
    elif attempt.payload_complete:
        raise FundingContractErrorV2("non-status failure cannot claim a complete body")


def _attempt_identity_document(attempt: FundingHttpAttemptV2) -> dict[str, object]:
    return {
        "correlation_id": attempt.correlation_id,
        "request_number": attempt.request_number,
        "schema_version": "r4b_realized_funding_http_attempt_identity_v2",
        "scope": _scope_document(attempt.scope),
    }


def _attempt_document(
    attempt: FundingHttpAttemptV2,
    *,
    include_payload_sha256: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "account_authenticated": attempt.account_authenticated,
        "authorization_header_present": attempt.authorization_header_present,
        "canonical_query": [list(pair) for pair in attempt.canonical_query],
        "content_type": attempt.content_type,
        "correlation_id": attempt.correlation_id,
        "endpoint_path": attempt.endpoint_path,
        "error": attempt.error.value if attempt.error is not None else None,
        "event_id": attempt.event_id,
        "ingest_seq": attempt.ingest_seq,
        "method": attempt.method,
        "payload_complete": attempt.payload_complete,
        "previous_attempt_payload_sha256": attempt.previous_attempt_payload_sha256,
        "raw_response_base64": base64.b64encode(attempt.raw_response_bytes).decode("ascii"),
        "raw_response_sha256": attempt.raw_response_sha256,
        "receipt_monotonic_ns": attempt.receipt_monotonic_ns,
        "request_number": attempt.request_number,
        "request_started_ms": attempt.request_started_ms,
        "response_completion_ms": attempt.response_completion_ms,
        "response_status": attempt.response_status,
        "route_id": attempt.route_id,
        "rule_version": attempt.rule_version,
        "schema_version": "r4b_realized_funding_http_attempt_v2",
        "scope": _scope_document(attempt.scope),
        "tls_verified": attempt.tls_verified,
    }
    if include_payload_sha256:
        document["payload_sha256"] = attempt.payload_sha256
    return document


def _decision_identity_document(
    decision: FundingConfirmationDecisionV2,
) -> dict[str, object]:
    return {
        "funding_time_ms": decision.funding_time_ms,
        "horizon_end_ms": decision.horizon_end_ms,
        "horizon_max_ms": decision.horizon_max_ms,
        "schema_version": "r4b_realized_funding_decision_identity_v2",
        "scope": _scope_document(decision.scope),
        "symbol": decision.symbol,
    }


def _decision_document(
    decision: FundingConfirmationDecisionV2,
    *,
    include_payload_sha256: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "attempt_event_ids": list(decision.attempt_event_ids),
        "candidate_attempt_count": decision.candidate_attempt_count,
        "confirmation_deadline_ms": decision.confirmation_deadline_ms,
        "event_id": decision.event_id,
        "evidence_root_sha256": decision.evidence_root_sha256,
        "funding_rate": _optional_decimal_text(decision.funding_rate),
        "funding_time_ms": decision.funding_time_ms,
        "horizon_end_ms": decision.horizon_end_ms,
        "horizon_max_ms": decision.horizon_max_ms,
        "invalidation": decision.invalidation,
        "mark_price": _optional_decimal_text(decision.mark_price),
        "observed_through_ms": decision.observed_through_ms,
        "reasons": list(decision.reasons),
        "request_lineage_sha256": decision.request_lineage_sha256,
        "retry_count": decision.retry_count,
        "rule_version": decision.rule_version,
        "schema_version": _DECISION_SCHEMA,
        "scope": _scope_document(decision.scope),
        "selected_attempt_event_id": decision.selected_attempt_event_id,
        "selected_attempt_payload_sha256": decision.selected_attempt_payload_sha256,
        "selected_response_completion_ms": decision.selected_response_completion_ms,
        "status": decision.status.value,
        "symbol": decision.symbol,
    }
    if include_payload_sha256:
        document["payload_sha256"] = decision.payload_sha256
    return document


def _registry_state_row(
    decision: FundingConfirmationDecisionV2,
) -> dict[str, object]:
    payload = canonical_funding_confirmation_v2(decision)
    return {
        "event_id": decision.event_id,
        "order_key": [decision.funding_time_ms, decision.symbol, decision.event_id],
        "payload_base64": base64.b64encode(payload).decode("ascii"),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
    }


def _registry_replay_root(
    rows: list[dict[str, object]],
    *,
    scope: FundingScopeV2,
    maximum_events: int,
) -> str:
    return _hash_document(
        _REGISTRY_REPLAY_DOMAIN,
        {
            "events": rows,
            "maximum_events": maximum_events,
            "schema_version": "r4b_realized_funding_registry_replay_v2",
            "scope": _scope_document(scope),
        },
    )


def _registry_checkpoint_document(
    checkpoint: FundingRegistryCheckpointV2,
) -> dict[str, object]:
    return {
        "event_count": checkpoint.event_count,
        "maximum_events": checkpoint.maximum_events,
        "replay_root_sha256": checkpoint.replay_root_sha256,
        "schema_version": "r4b_realized_funding_registry_checkpoint_v2",
        "scope": _scope_document(checkpoint.scope),
    }


def _parse_registry_row(
    raw: object,
    *,
    expected_scope: FundingScopeV2,
) -> tuple[dict[str, object], FundingConfirmationDecisionV2, tuple[int, str, str]]:
    if not isinstance(raw, dict) or set(raw) != {
        "event_id",
        "order_key",
        "payload_base64",
        "payload_sha256",
    }:
        raise FundingContractErrorV2("funding registry row schema is unsupported")
    event_id = _require_str(raw.get("event_id"), "event_id")
    _validate_sha256(event_id, "event_id")
    order_key_raw = raw.get("order_key")
    if not isinstance(order_key_raw, list) or len(order_key_raw) != 3:
        raise FundingContractErrorV2("funding registry order key is invalid")
    funding_time_ms = _require_int(order_key_raw[0], "order_key funding time")
    symbol = _require_str(order_key_raw[1], "order_key symbol")
    key_event_id = _require_str(order_key_raw[2], "order_key event ID")
    if key_event_id != event_id:
        raise FundingContractErrorV2("funding registry event ID differs from key")
    encoded = _require_str(raw.get("payload_base64"), "payload_base64")
    try:
        payload = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise FundingContractErrorV2("funding registry payload is invalid base64") from exc
    if base64.b64encode(payload).decode("ascii") != encoded:
        raise FundingContractErrorV2("funding registry payload base64 is noncanonical")
    payload_sha256 = _require_str(raw.get("payload_sha256"), "payload_sha256")
    _validate_sha256(payload_sha256, "payload_sha256")
    if hashlib.sha256(payload).hexdigest() != payload_sha256:
        raise FundingContractErrorV2("funding registry row payload hash mismatch")
    decision = _decision_from_canonical(payload, expected_scope=expected_scope)
    if (
        decision.event_id != event_id
        or decision.funding_time_ms != funding_time_ms
        or decision.symbol != symbol
    ):
        raise FundingContractErrorV2("funding registry order key differs from payload")
    row = _registry_state_row(decision)
    if row != raw:
        raise FundingContractErrorV2("funding registry row is not canonical")
    return row, decision, (funding_time_ms, symbol, event_id)


def _decision_from_canonical(
    payload: bytes,
    *,
    expected_scope: FundingScopeV2,
) -> FundingConfirmationDecisionV2:
    document = _parse_canonical_json(payload, "funding decision")
    required = {
        "attempt_event_ids",
        "candidate_attempt_count",
        "confirmation_deadline_ms",
        "event_id",
        "evidence_root_sha256",
        "funding_rate",
        "funding_time_ms",
        "horizon_end_ms",
        "horizon_max_ms",
        "invalidation",
        "mark_price",
        "observed_through_ms",
        "payload_sha256",
        "reasons",
        "request_lineage_sha256",
        "retry_count",
        "rule_version",
        "schema_version",
        "scope",
        "selected_attempt_event_id",
        "selected_attempt_payload_sha256",
        "selected_response_completion_ms",
        "status",
        "symbol",
    }
    if set(document) != required or document.get("schema_version") != _DECISION_SCHEMA:
        raise FundingContractErrorV2("funding decision schema is unsupported")
    if document.get("rule_version") != FUNDING_RULE_VERSION_V2:
        raise FundingContractErrorV2("funding decision rule version differs")
    scope = _scope_from_document(document.get("scope"))
    if scope != expected_scope:
        raise FundingContractErrorV2("funding decision scope differs from registry")
    raw_attempt_ids = document.get("attempt_event_ids")
    raw_reasons = document.get("reasons")
    if not isinstance(raw_attempt_ids, list) or not all(
        isinstance(value, str) for value in raw_attempt_ids
    ):
        raise FundingContractErrorV2("funding decision attempt IDs are invalid")
    if not isinstance(raw_reasons, list) or not all(
        isinstance(value, str) for value in raw_reasons
    ):
        raise FundingContractErrorV2("funding decision reasons are invalid")
    try:
        decision = FundingConfirmationDecisionV2(
            scope=scope,
            symbol=_require_str(document.get("symbol"), "symbol"),
            funding_time_ms=_require_int(
                document.get("funding_time_ms"),
                "funding_time_ms",
            ),
            horizon_end_ms=_require_int(document.get("horizon_end_ms"), "horizon_end_ms"),
            horizon_max_ms=_require_int(document.get("horizon_max_ms"), "horizon_max_ms"),
            confirmation_deadline_ms=_require_int(
                document.get("confirmation_deadline_ms"),
                "confirmation_deadline_ms",
            ),
            observed_through_ms=_require_int(
                document.get("observed_through_ms"),
                "observed_through_ms",
            ),
            status=FundingConfirmationStatusV2(_require_str(document.get("status"), "status")),
            reasons=tuple(raw_reasons),
            invalidation=_require_str(document.get("invalidation"), "invalidation"),
            funding_rate=_optional_decimal_from_json(
                document.get("funding_rate"),
                "funding_rate",
            ),
            mark_price=_optional_decimal_from_json(
                document.get("mark_price"),
                "mark_price",
            ),
            selected_response_completion_ms=_optional_int(
                document.get("selected_response_completion_ms"),
                "selected_response_completion_ms",
            ),
            selected_attempt_event_id=_optional_str(
                document.get("selected_attempt_event_id"),
                "selected_attempt_event_id",
            ),
            selected_attempt_payload_sha256=_optional_str(
                document.get("selected_attempt_payload_sha256"),
                "selected_attempt_payload_sha256",
            ),
            candidate_attempt_count=_require_int(
                document.get("candidate_attempt_count"),
                "candidate_attempt_count",
            ),
            retry_count=_require_int(document.get("retry_count"), "retry_count"),
            request_lineage_sha256=_require_str(
                document.get("request_lineage_sha256"),
                "request_lineage_sha256",
            ),
            evidence_root_sha256=_require_str(
                document.get("evidence_root_sha256"),
                "evidence_root_sha256",
            ),
            attempt_event_ids=tuple(raw_attempt_ids),
            _factory_token=_DECISION_FACTORY_TOKEN,
        )
    except ValueError as exc:
        if isinstance(exc, FundingContractErrorV2):
            raise
        raise FundingContractErrorV2("funding decision contains an invalid enum") from exc
    if (
        document.get("event_id") != decision.event_id
        or document.get("payload_sha256") != decision.payload_sha256
        or canonical_funding_confirmation_v2(decision) != payload
    ):
        raise FundingContractErrorV2("funding decision hashes or bytes differ")
    return decision


def _position_quantity_document(
    *,
    scope: FundingScopeV2,
    symbol: str,
    funding_time_ms: int,
    horizon_max_ms: int,
    position_event_id: str,
    position_payload_sha256: str,
    position_source_root_sha256: str,
    signed_quantity_before_funding: Decimal,
    contract_multiplier: Decimal,
    contract_multiplier_version_sha256: str,
    quantity_timestamp_ms: int,
    lot_timing: FundingLotTimingV2,
) -> dict[str, object]:
    if not isinstance(scope, FundingScopeV2):
        raise FundingContractErrorV2("position leaf scope must be FundingScopeV2")
    _validate_symbol(symbol)
    _validate_nonnegative_int(funding_time_ms, "funding_time_ms")
    _validate_nonnegative_int(horizon_max_ms, "horizon_max_ms")
    if funding_time_ms > horizon_max_ms:
        raise FundingContractErrorV2("position leaf funding time exceeds horizon")
    _validate_sha256(position_event_id, "position_event_id")
    _validate_sha256(position_payload_sha256, "position_payload_sha256")
    _validate_sha256(position_source_root_sha256, "position_source_root_sha256")
    _validate_finite_decimal(
        signed_quantity_before_funding,
        "signed_quantity_before_funding",
    )
    _validate_positive_finite_decimal(contract_multiplier, "contract_multiplier")
    _validate_sha256(
        contract_multiplier_version_sha256,
        "contract_multiplier_version_sha256",
    )
    _validate_nonnegative_int(quantity_timestamp_ms, "quantity_timestamp_ms")
    if not isinstance(lot_timing, FundingLotTimingV2):
        raise FundingContractErrorV2("position leaf lot_timing is invalid")
    if (
        lot_timing is FundingLotTimingV2.STRICTLY_BEFORE_FUNDING
        and quantity_timestamp_ms >= funding_time_ms
    ):
        raise FundingContractErrorV2("position leaf quantity is not strictly before funding")
    if (
        lot_timing is FundingLotTimingV2.EQUAL_MS_AMBIGUOUS
        and quantity_timestamp_ms != funding_time_ms
    ):
        raise FundingContractErrorV2("ambiguous position leaf is not at fundingTime")
    return {
        "contract_multiplier": str(contract_multiplier),
        "contract_multiplier_version_sha256": contract_multiplier_version_sha256,
        "funding_time_ms": funding_time_ms,
        "horizon_max_ms": horizon_max_ms,
        "lot_timing": lot_timing.value,
        "position_event_id": position_event_id,
        "position_payload_sha256": position_payload_sha256,
        "position_source_root_sha256": position_source_root_sha256,
        "quantity_timestamp_ms": quantity_timestamp_ms,
        "schema_version": "r4b_realized_funding_position_ledger_leaf_v2",
        "scope": _scope_document(scope),
        "signed_quantity_before_funding": str(signed_quantity_before_funding),
        "symbol": symbol,
    }


def _position_ledger_checkpoint_document(
    checkpoint: FundingPositionLedgerCheckpointV2,
) -> dict[str, object]:
    return {
        "event_count": checkpoint.event_count,
        "ledger_id": checkpoint.ledger_id,
        "ledger_root_sha256": checkpoint.ledger_root_sha256,
        "observed_through_ms": checkpoint.observed_through_ms,
        "schema_version": "r4b_realized_funding_position_ledger_checkpoint_v2",
        "scope": _scope_document(checkpoint.scope),
        "sealed_at_ms": checkpoint.sealed_at_ms,
    }


def _verify_position_membership(
    *,
    leaf_sha256: str,
    leaf_index: int,
    event_count: int,
    siblings: tuple[str, ...],
    expected_root_sha256: str,
) -> None:
    _validate_sha256(leaf_sha256, "position ledger leaf")
    _validate_nonnegative_int(leaf_index, "position ledger leaf index")
    _validate_positive_int(event_count, "position ledger event count")
    _validate_sha256(expected_root_sha256, "position ledger root")
    if leaf_index >= event_count:
        raise FundingContractErrorV2("position ledger leaf index exceeds event census")
    if not isinstance(siblings, tuple):
        raise FundingContractErrorV2("position ledger siblings must be a tuple")
    expected_depth = 0
    width = event_count
    while width > 1:
        expected_depth += 1
        width = (width + 1) // 2
    if len(siblings) != expected_depth:
        raise FundingContractErrorV2("position Merkle proof depth differs from census")
    current = leaf_sha256
    index = leaf_index
    width = event_count
    for sibling in siblings:
        _validate_sha256(sibling, "position ledger sibling")
        if width % 2 and index == width - 1 and sibling != current:
            raise FundingContractErrorV2("odd position Merkle leaf must duplicate itself")
        current = (
            _position_merkle_parent(current, sibling)
            if index % 2 == 0
            else _position_merkle_parent(sibling, current)
        )
        index //= 2
        width = (width + 1) // 2
    if current != expected_root_sha256:
        raise FundingContractErrorV2(
            "position quantity is not a member of the external ledger root"
        )


def _position_merkle_parent(left: str, right: str) -> str:
    return hashlib.sha256(
        _POSITION_LEDGER_NODE_DOMAIN + bytes.fromhex(left) + bytes.fromhex(right)
    ).hexdigest()


def _position_snapshot_document(
    position: FundingPositionSnapshotV2,
) -> dict[str, object]:
    return {
        "contract_multiplier": str(position.contract_multiplier),
        "contract_multiplier_version_sha256": (position.contract_multiplier_version_sha256),
        "funding_time_ms": position.funding_time_ms,
        "horizon_max_ms": position.horizon_max_ms,
        "lot_timing": position.lot_timing.value,
        "position_event_id": position.position_event_id,
        "position_ledger_checkpoint_sha256": (position.position_ledger_checkpoint_sha256),
        "position_ledger_leaf_index": position.position_ledger_leaf_index,
        "position_ledger_leaf_sha256": position.position_ledger_leaf_sha256,
        "position_ledger_merkle_siblings": list(position.position_ledger_merkle_siblings),
        "position_payload_sha256": position.position_payload_sha256,
        "position_source_root_sha256": position.position_source_root_sha256,
        "quantity_timestamp_ms": position.quantity_timestamp_ms,
        "rule_version": position.rule_version,
        "schema_version": "r4b_realized_funding_position_snapshot_v2",
        "scope": _scope_document(position.scope),
        "signed_quantity_before_funding": str(position.signed_quantity_before_funding),
        "symbol": position.symbol,
    }


def _cashflow_identity_document(
    cashflow: RealizedFundingCashflowV2,
) -> dict[str, object]:
    return {
        "confirmation_event_id": cashflow.confirmation_event_id,
        "funding_time_ms": cashflow.funding_time_ms,
        "position_snapshot_sha256": cashflow.position_snapshot_sha256,
        "position_ledger_checkpoint_sha256": (cashflow.position_ledger_checkpoint_sha256),
        "schema_version": "r4b_realized_funding_cashflow_identity_v2",
        "scope": _scope_document(cashflow.scope),
        "symbol": cashflow.symbol,
    }


def _cashflow_document(
    cashflow: RealizedFundingCashflowV2,
    *,
    include_payload_sha256: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "confirmation_event_id": cashflow.confirmation_event_id,
        "confirmation_evidence_root_sha256": (cashflow.confirmation_evidence_root_sha256),
        "confirmation_payload_sha256": cashflow.confirmation_payload_sha256,
        "contract_multiplier": str(cashflow.contract_multiplier),
        "event_id": cashflow.event_id,
        "funding_rate": str(cashflow.funding_rate),
        "funding_time_ms": cashflow.funding_time_ms,
        "horizon_max_ms": cashflow.horizon_max_ms,
        "invalidation": cashflow.invalidation,
        "lot_timing": cashflow.lot_timing.value,
        "mark_price": str(cashflow.mark_price),
        "market_value_time_ms": cashflow.market_value_time_ms,
        "normal_cashflow": str(cashflow.normal_cashflow),
        "position_snapshot_sha256": cashflow.position_snapshot_sha256,
        "realized_cashflow": str(cashflow.realized_cashflow),
        "reasons": list(cashflow.reasons),
        "registry_checkpoint_sha256": cashflow.registry_checkpoint_sha256,
        "rule_version": cashflow.rule_version,
        "schema_version": _CASHFLOW_SCHEMA,
        "scope": _scope_document(cashflow.scope),
        "signed_quantity_before_funding": str(cashflow.signed_quantity_before_funding),
        "symbol": cashflow.symbol,
    }
    if include_payload_sha256:
        document["payload_sha256"] = cashflow.payload_sha256
    return document


def _scope_document(scope: FundingScopeV2) -> dict[str, object]:
    return {
        "attempt_id": scope.attempt_id,
        "plan_id": scope.plan_id,
        "protocol_hash": scope.protocol_hash,
        "universe_sha256": scope.universe_sha256,
    }


def _scope_from_document(raw: object) -> FundingScopeV2:
    if not isinstance(raw, dict) or set(raw) != {
        "attempt_id",
        "plan_id",
        "protocol_hash",
        "universe_sha256",
    }:
        raise FundingContractErrorV2("funding scope document is invalid")
    return FundingScopeV2(
        attempt_id=_require_str(raw.get("attempt_id"), "attempt_id"),
        plan_id=_require_str(raw.get("plan_id"), "plan_id"),
        protocol_hash=_require_str(raw.get("protocol_hash"), "protocol_hash"),
        universe_sha256=_require_str(
            raw.get("universe_sha256"),
            "universe_sha256",
        ),
    )


def _parse_canonical_json(payload: bytes, label: str) -> dict[str, object]:
    if not isinstance(payload, bytes) or not payload:
        raise FundingContractErrorV2(f"{label} must be non-empty bytes")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FundingContractErrorV2(f"{label} is invalid UTF-8 JSON") from exc
    if not isinstance(document, dict) or canonical_json_line(document) != payload:
        raise FundingContractErrorV2(f"{label} must be canonical JSONL")
    return document


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _validate_query_shape(query: tuple[tuple[str, str], ...]) -> None:
    if not isinstance(query, tuple) or tuple(sorted(query)) != query:
        raise FundingContractErrorV2("canonical_query must be a sorted tuple")
    names: set[str] = set()
    for pair in query:
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise FundingContractErrorV2("canonical_query pairs are invalid")
        name, value = pair
        _validate_identity(name, "query name")
        if not isinstance(value, str) or any(character in value for character in "\r\n\x00"):
            raise FundingContractErrorV2("canonical_query contains an invalid value")
        if name in names:
            raise FundingContractErrorV2("canonical_query repeats a parameter")
        if name.casefold() in _SENSITIVE_QUERY_NAMES:
            raise FundingContractErrorV2("canonical_query contains a credential")
        names.add(name)


def _validate_reasons(reasons: tuple[str, ...]) -> None:
    if not isinstance(reasons, tuple) or not reasons:
        raise FundingContractErrorV2("reasons must be a non-empty tuple")
    for reason in reasons:
        _validate_identity(reason, "reason")


def _validate_symbol(symbol: str) -> None:
    if not isinstance(symbol, str) or _SYMBOL_RE.fullmatch(symbol) is None:
        raise FundingContractErrorV2("symbol must be an uppercase USDT market")


def _validate_identity(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise FundingContractErrorV2(f"{field_name} must be a bounded normalized identity")


def _validate_sha256(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise FundingContractErrorV2(f"{field_name} must be a lowercase SHA-256 digest")


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise FundingContractErrorV2(f"{field_name} must be a nonnegative integer")


def _validate_positive_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 1:
        raise FundingContractErrorV2(f"{field_name} must be a positive integer")


def _validate_finite_decimal(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise FundingContractErrorV2(f"{field_name} must be a finite Decimal")


def _validate_positive_finite_decimal(value: Decimal, field_name: str) -> None:
    _validate_finite_decimal(value, field_name)
    if value <= 0:
        raise FundingContractErrorV2(f"{field_name} must be positive")


def _optional_decimal_text(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _optional_decimal_from_json(value: object, field_name: str) -> Decimal | None:
    if value is None:
        return None
    return _parse_decimal_text(value, field_name)


def _canonical_zero(value: Decimal) -> Decimal:
    return Decimal(0) if value.is_zero() else value


def _require_str(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise FundingContractErrorV2(f"{field_name} must be text")
    return value


def _optional_str(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_str(value, field_name)


def _require_int(value: object, field_name: str) -> int:
    if type(value) is not int:
        raise FundingContractErrorV2(f"{field_name} must be an integer")
    return value


def _optional_int(value: object, field_name: str) -> int | None:
    if value is None:
        return None
    return _require_int(value, field_name)
