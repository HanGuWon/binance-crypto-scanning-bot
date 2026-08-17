from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2

ACTIONABILITY_RULE_VERSION_V2: Final = "R4B_CAUSAL_V2.2.0_ALERT_ACTIONABILITY_V1"
ACTIONABILITY_LATE_GRACE_MS_V2: Final = 30_000
PRIMARY_PAPER_TARGET_DELAY_MS_V2: Final = 10_000
ACTIONABILITY_THRESHOLD_NUMERATOR_V2: Final = 99
ACTIONABILITY_THRESHOLD_DENOMINATOR_V2: Final = 100
ACTIONABILITY_ROLE_V2: Final = "PROMOTING_SIGNAL_ALERT_ATTEMPT"

_EVENT_ID_DOMAIN: Final = b"R4B_ALERT_ACTIONABILITY_V2\0"
_CURSOR_EVIDENCE_DOMAIN: Final = b"R4B_ACTIONABILITY_CURSOR_V2\0"
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")


class AlertActionabilityContractErrorV2(ValueError):
    """Raised when an alert-attempt ledger would violate the frozen contract."""


class PromotingFamilyV2(StrEnum):
    A = "A"
    B = "B"
    C = "C"


@dataclass(frozen=True, slots=True)
class CausalTargetCursorV2:
    """Two-point witness for ``r_tau = inf{r: V_lower(r) >= D+10s}``."""

    decision_cutoff_ms: int
    target_venue_ms: int
    prior_local_cursor_ms: int
    prior_venue_lower_bound_ms: int
    target_local_cursor_ms: int
    target_venue_lower_bound_ms: int
    clock_segment_root_sha256: str
    contiguous_cursor_evidence: bool
    cursor_evidence_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        for field_name in (
            "decision_cutoff_ms",
            "target_venue_ms",
            "prior_local_cursor_ms",
            "prior_venue_lower_bound_ms",
            "target_local_cursor_ms",
            "target_venue_lower_bound_ms",
        ):
            _validate_nonnegative_ms(getattr(self, field_name), field_name)
        _validate_sha256(
            self.clock_segment_root_sha256,
            "clock_segment_root_sha256",
        )
        if type(self.contiguous_cursor_evidence) is not bool:
            raise AlertActionabilityContractErrorV2("contiguous_cursor_evidence must be boolean")
        if not self.contiguous_cursor_evidence:
            raise AlertActionabilityContractErrorV2(
                "target cursor requires contiguous clock evidence"
            )
        if self.target_venue_ms != (self.decision_cutoff_ms + PRIMARY_PAPER_TARGET_DELAY_MS_V2):
            raise AlertActionabilityContractErrorV2(
                "primary venue target must equal decision cutoff plus 10000 ms"
            )
        if self.prior_local_cursor_ms >= self.target_local_cursor_ms:
            raise AlertActionabilityContractErrorV2(
                "prior local cursor must strictly precede target local cursor"
            )
        if not (
            self.prior_venue_lower_bound_ms
            < self.target_venue_ms
            <= self.target_venue_lower_bound_ms
        ):
            raise AlertActionabilityContractErrorV2(
                "cursor witness must straddle the exact primary venue target"
            )
        evidence_document = {
            "clock_segment_root_sha256": self.clock_segment_root_sha256,
            "contiguous_cursor_evidence": self.contiguous_cursor_evidence,
            "decision_cutoff_ms": self.decision_cutoff_ms,
            "prior_local_cursor_ms": self.prior_local_cursor_ms,
            "prior_venue_lower_bound_ms": self.prior_venue_lower_bound_ms,
            "target_local_cursor_ms": self.target_local_cursor_ms,
            "target_venue_lower_bound_ms": self.target_venue_lower_bound_ms,
            "target_venue_ms": self.target_venue_ms,
        }
        object.__setattr__(
            self,
            "cursor_evidence_sha256",
            hashlib.sha256(
                _CURSOR_EVIDENCE_DOMAIN + canonical_json_line(evidence_document)
            ).hexdigest(),
        )


@dataclass(frozen=True, slots=True)
class ExpectedPromotingAlertV2:
    """One promoting signal that must contribute to the actionability denominator."""

    signal_event_id: str
    symbol: str
    venue: VenueV2
    family: PromotingFamilyV2
    target_cursor: CausalTargetCursorV2

    def __post_init__(self) -> None:
        _validate_sha256(self.signal_event_id, "signal_event_id")
        if _SYMBOL_RE.fullmatch(self.symbol) is None:
            raise AlertActionabilityContractErrorV2("symbol must be a normalized USDT symbol")
        if self.venue is not VenueV2.USDM_FUTURES:
            raise AlertActionabilityContractErrorV2(
                "promoting actionability accepts USD-M Futures signals only"
            )
        if not isinstance(self.family, PromotingFamilyV2):
            raise AlertActionabilityContractErrorV2(
                "family must be one of the isolated promoting families A/B/C"
            )
        if not isinstance(self.target_cursor, CausalTargetCursorV2):
            raise AlertActionabilityContractErrorV2("target_cursor must be CausalTargetCursorV2")


@dataclass(frozen=True, slots=True)
class PromotingSignalCensusV2:
    """Root-bound complete set of signals that fixes the actionability denominator."""

    attempt_id: str
    promoting_plan_sha256: str
    promoting_signal_ledger_root_sha256: str
    expected_alerts: tuple[ExpectedPromotingAlertV2, ...]
    census_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_sha256(
            self.promoting_signal_ledger_root_sha256,
            "promoting_signal_ledger_root_sha256",
        )
        if type(self.expected_alerts) is not tuple or any(
            not isinstance(value, ExpectedPromotingAlertV2) for value in self.expected_alerts
        ):
            raise AlertActionabilityContractErrorV2(
                "expected_alerts must be an immutable tuple of expected alerts"
            )
        canonical_order = tuple(
            sorted(
                self.expected_alerts,
                key=lambda value: (
                    value.family.value,
                    value.symbol,
                    value.signal_event_id,
                ),
            )
        )
        if self.expected_alerts != canonical_order:
            raise AlertActionabilityContractErrorV2(
                "expected alerts must use canonical family/symbol/event order"
            )
        signal_ids = tuple(value.signal_event_id for value in self.expected_alerts)
        if len(signal_ids) != len(set(signal_ids)):
            raise AlertActionabilityContractErrorV2(
                "expected promoting signal event IDs must be unique"
            )
        document = {
            "attempt_id": self.attempt_id,
            "expected_alerts": [_expected_alert_document(value) for value in self.expected_alerts],
            "promoting_plan_sha256": self.promoting_plan_sha256,
            "promoting_signal_ledger_root_sha256": (self.promoting_signal_ledger_root_sha256),
        }
        object.__setattr__(
            self,
            "census_sha256",
            hashlib.sha256(canonical_json_line(document)).hexdigest(),
        )


class AlertActionabilityStatusV2(StrEnum):
    PENDING = "PENDING"
    ALERT_ON_TIME = "ALERT_ON_TIME"
    ALERT_LATE = "ALERT_LATE"
    ALERT_MISSING = "ALERT_MISSING"


class AlertActionabilityGateV2(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    NOT_FINALIZED = "NOT_FINALIZED"
    INCONCLUSIVE_NO_ATTEMPTS = "INCONCLUSIVE_NO_ATTEMPTS"


class AlertActionabilityRegistryDispositionV2(StrEnum):
    NEW = "NEW"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"


@dataclass(frozen=True, slots=True)
class AlertTransportTimesV2:
    """Local Unix-millisecond transport observations for one visible alert."""

    durable_outbox_enqueue_ms: int
    send_start_ms: int
    response_first_byte_ms: int | None
    provider_acceptance_completion_ms: int | None
    request_completion_ms: int | None
    observable_delivery_or_ack_ms: int | None = None

    def __post_init__(self) -> None:
        _validate_nonnegative_ms(
            self.durable_outbox_enqueue_ms,
            "durable_outbox_enqueue_ms",
        )
        _validate_nonnegative_ms(self.send_start_ms, "send_start_ms")
        for field_name in (
            "response_first_byte_ms",
            "provider_acceptance_completion_ms",
            "request_completion_ms",
            "observable_delivery_or_ack_ms",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _validate_nonnegative_ms(value, field_name)
        if self.send_start_ms < self.durable_outbox_enqueue_ms:
            raise AlertActionabilityContractErrorV2(
                "send_start_ms cannot precede durable outbox enqueue"
            )
        if (
            self.response_first_byte_ms is not None
            and self.response_first_byte_ms < self.send_start_ms
        ):
            raise AlertActionabilityContractErrorV2(
                "response_first_byte_ms cannot precede send_start_ms"
            )
        if (
            self.request_completion_ms is not None
            and self.request_completion_ms < self.send_start_ms
        ):
            raise AlertActionabilityContractErrorV2(
                "request_completion_ms cannot precede send_start_ms"
            )
        if self.response_first_byte_ms is not None:
            if self.request_completion_ms is None:
                raise AlertActionabilityContractErrorV2(
                    "a first response byte requires request completion lineage"
                )
            if self.request_completion_ms < self.response_first_byte_ms:
                raise AlertActionabilityContractErrorV2(
                    "request completion cannot precede the first response byte"
                )
        if self.provider_acceptance_completion_ms is not None:
            if self.response_first_byte_ms is None or self.request_completion_ms is None:
                raise AlertActionabilityContractErrorV2(
                    "provider acceptance requires response and request-completion lineage"
                )
            if not (
                self.response_first_byte_ms
                <= self.provider_acceptance_completion_ms
                <= self.request_completion_ms
            ):
                raise AlertActionabilityContractErrorV2(
                    "provider acceptance must fall within the observed response"
                )
        if self.observable_delivery_or_ack_ms is not None:
            if self.provider_acceptance_completion_ms is None:
                raise AlertActionabilityContractErrorV2(
                    "observable delivery requires provider-acceptance lineage"
                )
            if self.observable_delivery_or_ack_ms < self.provider_acceptance_completion_ms:
                raise AlertActionabilityContractErrorV2(
                    "observable delivery cannot precede provider acceptance"
                )

    @property
    def latest_observation_ms(self) -> int:
        return max(
            value
            for value in (
                self.durable_outbox_enqueue_ms,
                self.send_start_ms,
                self.response_first_byte_ms,
                self.provider_acceptance_completion_ms,
                self.request_completion_ms,
                self.observable_delivery_or_ack_ms,
            )
            if value is not None
        )


@dataclass(frozen=True, slots=True)
class AlertActionabilityInputV2:
    attempt_id: str
    signal_event_id: str
    symbol: str
    venue: VenueV2
    family: PromotingFamilyV2
    promoting_plan_sha256: str
    target_cursor: CausalTargetCursorV2
    finalized_through_ms: int
    transport: AlertTransportTimesV2

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_sha256(self.signal_event_id, "signal_event_id")
        if _SYMBOL_RE.fullmatch(self.symbol) is None:
            raise AlertActionabilityContractErrorV2("symbol must be a normalized USDT symbol")
        if self.venue is not VenueV2.USDM_FUTURES:
            raise AlertActionabilityContractErrorV2(
                "promoting actionability accepts USD-M Futures signals only"
            )
        if not isinstance(self.family, PromotingFamilyV2):
            raise AlertActionabilityContractErrorV2(
                "family must be one of the isolated promoting families A/B/C"
            )
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        if not isinstance(self.target_cursor, CausalTargetCursorV2):
            raise AlertActionabilityContractErrorV2("target_cursor must be CausalTargetCursorV2")
        _validate_nonnegative_ms(self.finalized_through_ms, "finalized_through_ms")
        if not isinstance(self.transport, AlertTransportTimesV2):
            raise AlertActionabilityContractErrorV2("transport must be AlertTransportTimesV2")
        if self.transport.latest_observation_ms > self.finalized_through_ms:
            raise AlertActionabilityContractErrorV2(
                "transport observations cannot occur after finalized_through_ms"
            )

    @property
    def target_local_cursor_ms(self) -> int:
        return self.target_cursor.target_local_cursor_ms


@dataclass(frozen=True, slots=True)
class AlertActionabilityRecordV2:
    attempt_id: str
    signal_event_id: str
    symbol: str
    venue: VenueV2
    family: PromotingFamilyV2
    promoting_plan_sha256: str
    target_cursor: CausalTargetCursorV2
    finalized_through_ms: int
    transport: AlertTransportTimesV2
    status: AlertActionabilityStatusV2
    reasons: tuple[str, ...]
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    role: str = field(init=False, default=ACTIONABILITY_ROLE_V2)
    rule_version: str = field(init=False, default=ACTIONABILITY_RULE_VERSION_V2)
    changes_paper_execution: bool = field(init=False, default=False)
    changes_position_or_pnl_root: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        item = AlertActionabilityInputV2(
            attempt_id=self.attempt_id,
            signal_event_id=self.signal_event_id,
            symbol=self.symbol,
            venue=self.venue,
            family=self.family,
            promoting_plan_sha256=self.promoting_plan_sha256,
            target_cursor=self.target_cursor,
            finalized_through_ms=self.finalized_through_ms,
            transport=self.transport,
        )
        if not isinstance(self.status, AlertActionabilityStatusV2):
            raise AlertActionabilityContractErrorV2("status must be an AlertActionabilityStatusV2")
        expected_status = _classify_actionability(item)
        if self.status is not expected_status:
            raise AlertActionabilityContractErrorV2(
                "status contradicts the target cursor, finalization cursor, or acceptance"
            )
        _validate_reasons(self.reasons)
        identity = {
            "attempt_id": self.attempt_id,
            "family": self.family.value,
            "role": self.role,
            "rule_version": self.rule_version,
            "signal_event_id": self.signal_event_id,
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
                canonical_json_line(_record_document(self, include_payload_sha256=False))
            ).hexdigest(),
        )

    @property
    def target_local_cursor_ms(self) -> int:
        return self.target_cursor.target_local_cursor_ms


@dataclass(frozen=True, slots=True)
class AlertActionabilitySummaryV2:
    attempt_id: str
    promoting_plan_sha256: str
    promoting_signal_ledger_root_sha256: str
    signal_census_sha256: str
    finalized_through_ms: int
    attempted_count: int
    on_time_count: int
    late_count: int
    missing_count: int
    pending_count: int
    gate: AlertActionabilityGateV2
    threshold_numerator: int = field(
        init=False,
        default=ACTIONABILITY_THRESHOLD_NUMERATOR_V2,
    )
    threshold_denominator: int = field(
        init=False,
        default=ACTIONABILITY_THRESHOLD_DENOMINATOR_V2,
    )

    def __post_init__(self) -> None:
        _validate_identity(self.attempt_id, "attempt_id")
        _validate_sha256(self.promoting_plan_sha256, "promoting_plan_sha256")
        _validate_sha256(
            self.promoting_signal_ledger_root_sha256,
            "promoting_signal_ledger_root_sha256",
        )
        _validate_sha256(self.signal_census_sha256, "signal_census_sha256")
        _validate_nonnegative_ms(self.finalized_through_ms, "finalized_through_ms")
        for field_name in (
            "attempted_count",
            "on_time_count",
            "late_count",
            "missing_count",
            "pending_count",
        ):
            value = getattr(self, field_name)
            if type(value) is not int or value < 0:
                raise AlertActionabilityContractErrorV2(
                    f"{field_name} must be a nonnegative integer"
                )
        if self.attempted_count != (
            self.on_time_count + self.late_count + self.missing_count + self.pending_count
        ):
            raise AlertActionabilityContractErrorV2(
                "attempted_count must equal the four terminal/pending buckets"
            )
        if not isinstance(self.gate, AlertActionabilityGateV2):
            raise AlertActionabilityContractErrorV2("gate must be an AlertActionabilityGateV2")
        expected_gate = _summary_gate(
            attempted_count=self.attempted_count,
            on_time_count=self.on_time_count,
            pending_count=self.pending_count,
        )
        if self.gate is not expected_gate:
            raise AlertActionabilityContractErrorV2(
                "gate contradicts the exact actionability counts"
            )

    @property
    def threshold_passed(self) -> bool:
        return self.gate is AlertActionabilityGateV2.PASS


class AlertActionabilityRegistryV2:
    """Bounded terminal-record gate that cannot inflate the denominator.

    ``PENDING`` is a read-only monitoring view, not an appendable final record.
    Raw transport observations stay append-only in their own ledger; this gate
    accepts the derived attempt exactly once after it becomes terminal.
    """

    def __init__(self, *, maximum_events: int) -> None:
        if type(maximum_events) is not int or maximum_events < 1:
            raise AlertActionabilityContractErrorV2("maximum_events must be a positive integer")
        self._maximum_events = maximum_events
        self._payload_by_event_id: dict[str, bytes] = {}

    def register(
        self,
        record: AlertActionabilityRecordV2,
    ) -> AlertActionabilityRegistryDispositionV2:
        if not isinstance(record, AlertActionabilityRecordV2):
            raise AlertActionabilityContractErrorV2(
                "registry accepts AlertActionabilityRecordV2 values only"
            )
        if record.status is AlertActionabilityStatusV2.PENDING:
            raise AlertActionabilityContractErrorV2(
                "PENDING is a monitoring view and cannot enter the terminal registry"
            )
        payload = canonical_alert_actionability_record_v2(record)
        prior = self._payload_by_event_id.get(record.event_id)
        if prior is not None:
            if prior != payload:
                raise AlertActionabilityContractErrorV2(
                    "deterministic alert event ID collides with a different payload"
                )
            return AlertActionabilityRegistryDispositionV2.IDEMPOTENT_DUPLICATE
        if len(self._payload_by_event_id) >= self._maximum_events:
            raise AlertActionabilityContractErrorV2(
                "bounded actionability registry capacity exhausted"
            )
        self._payload_by_event_id[record.event_id] = payload
        return AlertActionabilityRegistryDispositionV2.NEW


def evaluate_authorized_alert_actionability_v2(
    item: AlertActionabilityInputV2,
    *,
    current_target_authority: object,
) -> AlertActionabilityRecordV2:
    """Classify one alert only after current cursor-authority consumption."""

    from signalbot.r4b_v2.capture.causal_target_authority import (
        CurrentCausalTargetAuthorityUseV2,
        consume_current_causal_target_authority_v2,
    )

    if type(item) is not AlertActionabilityInputV2:
        raise AlertActionabilityContractErrorV2("item must be an exact AlertActionabilityInputV2")
    if type(current_target_authority) is not CurrentCausalTargetAuthorityUseV2:
        raise AlertActionabilityContractErrorV2(
            "runtime actionability requires current causal-target authority; "
            "direct CausalTargetCursorV2 values are rejected"
        )
    expected_plan_sha256 = current_target_authority.promoting_plan_sha256
    authorized_cursor = consume_current_causal_target_authority_v2(current_target_authority)
    if item.target_cursor != authorized_cursor:
        raise AlertActionabilityContractErrorV2(
            "actionability cursor differs from current signed-prefix authority"
        )
    if item.promoting_plan_sha256 != expected_plan_sha256:
        raise AlertActionabilityContractErrorV2(
            "actionability plan differs from current signed-prefix authority"
        )
    return evaluate_alert_actionability_v2(item)


def evaluate_alert_actionability_v2(
    item: AlertActionabilityInputV2,
) -> AlertActionabilityRecordV2:
    """Pure classification without PAPER entry, PnL, or root mutation.

    Runtime authority must use ``evaluate_authorized_alert_actionability_v2``;
    this low-level function retains deterministic arithmetic-test utility only.
    """

    if not isinstance(item, AlertActionabilityInputV2):
        raise AlertActionabilityContractErrorV2("item must be an AlertActionabilityInputV2")
    status = _classify_actionability(item)
    return AlertActionabilityRecordV2(
        attempt_id=item.attempt_id,
        signal_event_id=item.signal_event_id,
        symbol=item.symbol,
        venue=item.venue,
        family=item.family,
        promoting_plan_sha256=item.promoting_plan_sha256,
        target_cursor=item.target_cursor,
        finalized_through_ms=item.finalized_through_ms,
        transport=item.transport,
        status=status,
        reasons=(
            status.value,
            "DISCORD_TIMING_CANNOT_CHANGE_PAPER_EXECUTION_OR_PNL",
        ),
    )


def summarize_alert_actionability_v2(
    census: PromotingSignalCensusV2,
    records: tuple[AlertActionabilityRecordV2, ...],
    *,
    finalized_through_ms: int,
) -> AlertActionabilitySummaryV2:
    """Reconcile every promoting signal and apply the exact 99% co-gate."""

    if not isinstance(census, PromotingSignalCensusV2):
        raise AlertActionabilityContractErrorV2("census must be PromotingSignalCensusV2")
    _validate_nonnegative_ms(finalized_through_ms, "finalized_through_ms")
    if type(records) is not tuple or any(
        not isinstance(record, AlertActionabilityRecordV2) for record in records
    ):
        raise AlertActionabilityContractErrorV2(
            "records must be an immutable tuple of actionability records"
        )
    unique: dict[str, AlertActionabilityRecordV2] = {}
    payload_by_event_id: dict[str, bytes] = {}
    for record in records:
        if record.status is AlertActionabilityStatusV2.PENDING:
            raise AlertActionabilityContractErrorV2(
                "summary accepts terminal records only; pending is census-derived"
            )
        if record.finalized_through_ms > finalized_through_ms:
            raise AlertActionabilityContractErrorV2(
                "record finalization cannot exceed summary finalization"
            )
        if (
            record.attempt_id != census.attempt_id
            or record.promoting_plan_sha256 != census.promoting_plan_sha256
        ):
            raise AlertActionabilityContractErrorV2(
                "actionability record does not match census attempt and plan"
            )
        payload = canonical_alert_actionability_record_v2(record)
        prior = payload_by_event_id.get(record.event_id)
        if prior is not None and prior != payload:
            raise AlertActionabilityContractErrorV2(
                "summary contains a conflicting deterministic event ID"
            )
        payload_by_event_id[record.event_id] = payload
        unique[record.event_id] = record
    expected_by_signal_id = {value.signal_event_id: value for value in census.expected_alerts}
    record_by_signal_id: dict[str, AlertActionabilityRecordV2] = {}
    for record in unique.values():
        expected = expected_by_signal_id.get(record.signal_event_id)
        if expected is None:
            raise AlertActionabilityContractErrorV2(
                "actionability record is absent from the promoting signal census"
            )
        if (
            record.symbol != expected.symbol
            or record.venue is not expected.venue
            or record.family is not expected.family
            or record.target_cursor != expected.target_cursor
        ):
            raise AlertActionabilityContractErrorV2(
                "actionability record identity or target differs from census"
            )
        prior = record_by_signal_id.get(record.signal_event_id)
        if prior is not None and prior.event_id != record.event_id:
            raise AlertActionabilityContractErrorV2(
                "one promoting signal cannot map to multiple alert attempts"
            )
        record_by_signal_id[record.signal_event_id] = record

    on_time_count = 0
    late_count = 0
    missing_count = 0
    pending_count = 0
    for expected in census.expected_alerts:
        record = record_by_signal_id.get(expected.signal_event_id)
        if record is None:
            deadline_ms = (
                expected.target_cursor.target_local_cursor_ms + ACTIONABILITY_LATE_GRACE_MS_V2
            )
            if finalized_through_ms >= deadline_ms:
                missing_count += 1
            else:
                pending_count += 1
        elif record.status is AlertActionabilityStatusV2.ALERT_ON_TIME:
            on_time_count += 1
        elif record.status is AlertActionabilityStatusV2.ALERT_LATE:
            late_count += 1
        elif record.status is AlertActionabilityStatusV2.ALERT_MISSING:
            missing_count += 1
        else:  # pragma: no cover - PENDING rejected before reconciliation
            raise AlertActionabilityContractErrorV2("unexpected nonterminal actionability record")
    attempted_count = len(census.expected_alerts)
    return AlertActionabilitySummaryV2(
        attempt_id=census.attempt_id,
        promoting_plan_sha256=census.promoting_plan_sha256,
        promoting_signal_ledger_root_sha256=(census.promoting_signal_ledger_root_sha256),
        signal_census_sha256=census.census_sha256,
        finalized_through_ms=finalized_through_ms,
        attempted_count=attempted_count,
        on_time_count=on_time_count,
        late_count=late_count,
        missing_count=missing_count,
        pending_count=pending_count,
        gate=_summary_gate(
            attempted_count=attempted_count,
            on_time_count=on_time_count,
            pending_count=pending_count,
        ),
    )


def canonical_alert_actionability_record_v2(
    record: AlertActionabilityRecordV2,
) -> bytes:
    """Return the canonical, self-hash-checked JSONL ledger record."""

    if not isinstance(record, AlertActionabilityRecordV2):
        raise AlertActionabilityContractErrorV2("record must be an AlertActionabilityRecordV2")
    expected = hashlib.sha256(
        canonical_json_line(_record_document(record, include_payload_sha256=False))
    ).hexdigest()
    if record.payload_sha256 != expected:
        raise AlertActionabilityContractErrorV2(
            "actionability payload hash differs from canonical content"
        )
    return canonical_json_line(_record_document(record, include_payload_sha256=True))


def _classify_actionability(
    item: AlertActionabilityInputV2,
) -> AlertActionabilityStatusV2:
    acceptance_ms = item.transport.provider_acceptance_completion_ms
    deadline_ms = item.target_local_cursor_ms + ACTIONABILITY_LATE_GRACE_MS_V2
    if acceptance_ms is not None:
        if acceptance_ms <= item.target_local_cursor_ms:
            return AlertActionabilityStatusV2.ALERT_ON_TIME
        if acceptance_ms <= deadline_ms:
            return AlertActionabilityStatusV2.ALERT_LATE
        return AlertActionabilityStatusV2.ALERT_MISSING
    if item.finalized_through_ms >= deadline_ms:
        return AlertActionabilityStatusV2.ALERT_MISSING
    return AlertActionabilityStatusV2.PENDING


def _summary_gate(
    *,
    attempted_count: int,
    on_time_count: int,
    pending_count: int,
) -> AlertActionabilityGateV2:
    if attempted_count == 0:
        return AlertActionabilityGateV2.INCONCLUSIVE_NO_ATTEMPTS
    if pending_count:
        return AlertActionabilityGateV2.NOT_FINALIZED
    if (
        ACTIONABILITY_THRESHOLD_DENOMINATOR_V2 * on_time_count
        >= ACTIONABILITY_THRESHOLD_NUMERATOR_V2 * attempted_count
    ):
        return AlertActionabilityGateV2.PASS
    return AlertActionabilityGateV2.FAIL


def _record_document(
    record: AlertActionabilityRecordV2,
    *,
    include_payload_sha256: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "attempt_id": record.attempt_id,
        "changes_paper_execution": record.changes_paper_execution,
        "changes_position_or_pnl_root": record.changes_position_or_pnl_root,
        "event_id": record.event_id,
        "family": record.family.value,
        "finalized_through_ms": record.finalized_through_ms,
        "promoting_plan_sha256": record.promoting_plan_sha256,
        "reasons": list(record.reasons),
        "role": record.role,
        "rule_version": record.rule_version,
        "signal_event_id": record.signal_event_id,
        "status": record.status.value,
        "symbol": record.symbol,
        "venue": record.venue.value,
        "target_cursor": _target_cursor_document(record.target_cursor),
        "transport": {
            "durable_outbox_enqueue_ms": (record.transport.durable_outbox_enqueue_ms),
            "observable_delivery_or_ack_ms": (record.transport.observable_delivery_or_ack_ms),
            "provider_acceptance_completion_ms": (
                record.transport.provider_acceptance_completion_ms
            ),
            "request_completion_ms": record.transport.request_completion_ms,
            "response_first_byte_ms": record.transport.response_first_byte_ms,
            "send_start_ms": record.transport.send_start_ms,
        },
    }
    if include_payload_sha256:
        document["payload_sha256"] = record.payload_sha256
    return document


def _expected_alert_document(value: ExpectedPromotingAlertV2) -> dict[str, object]:
    return {
        "family": value.family.value,
        "signal_event_id": value.signal_event_id,
        "symbol": value.symbol,
        "venue": value.venue.value,
        "target_cursor": _target_cursor_document(value.target_cursor),
    }


def _target_cursor_document(value: CausalTargetCursorV2) -> dict[str, object]:
    return {
        "clock_segment_root_sha256": value.clock_segment_root_sha256,
        "contiguous_cursor_evidence": value.contiguous_cursor_evidence,
        "cursor_evidence_sha256": value.cursor_evidence_sha256,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "prior_local_cursor_ms": value.prior_local_cursor_ms,
        "prior_venue_lower_bound_ms": value.prior_venue_lower_bound_ms,
        "target_local_cursor_ms": value.target_local_cursor_ms,
        "target_venue_lower_bound_ms": value.target_venue_lower_bound_ms,
        "target_venue_ms": value.target_venue_ms,
    }


def _validate_nonnegative_ms(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise AlertActionabilityContractErrorV2(
            f"{field_name} must be a nonnegative Unix-millisecond integer"
        )


def _validate_identity(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value or value.strip() != value or len(value) > 256:
        raise AlertActionabilityContractErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )


def _validate_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise AlertActionabilityContractErrorV2(f"{field_name} must be a lowercase SHA-256 digest")


def _validate_reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not values or len(values) > 16:
        raise AlertActionabilityContractErrorV2(
            "reasons must be a non-empty bounded immutable tuple"
        )
    for value in values:
        _validate_identity(value, "reason")
