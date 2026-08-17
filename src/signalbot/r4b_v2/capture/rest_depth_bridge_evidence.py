from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import InitVar, asdict, dataclass, field, fields
from enum import StrEnum
from typing import Any, Literal

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingPlanV8,
    provisional_promoting_plan_sha256_v8,
    validate_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.rest_depth import (
    public_depth_rest_plan_sha256_v8,
    validate_public_depth_rest_plan_v8,
)

DEPTH_BRIDGE_EVENT_TYPE_V8 = "DEPTH_BRIDGE"
DEPTH_BRIDGE_TERMINAL_RESERVE_BYTES_V8 = 64 * 1024
DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8 = 1_024

_SCHEMA = "r4b_v2_depth_bridge_evidence_v8"
_RANGE_SUMMARY_SCHEMA = "r4b_v2_depth_bridge_range_summary_v8"
_WS_LOCATOR_SCHEMA = "r4b_v2_depth_bridge_ws_source_locator_v8"
_REST_LOCATOR_SCHEMA = "r4b_v2_depth_bridge_rest_source_locator_v8"
_CYCLE_REF_SCHEMA = "r4b_v2_depth_bridge_cycle_ref_v8"
_SYMBOL_CENSUS_DOMAIN = b"R4B_V2_DEPTH_BRIDGE_SYMBOL_CENSUS_V8\0"
_CYCLE_ID_DOMAIN = b"R4B_V2_DEPTH_BRIDGE_CYCLE_ID_V8\0"
_RANGE_ROOT_DOMAIN = b"R4B_V2_DEPTH_BRIDGE_RANGE_ROOT_V8\0"
_COORDINATOR_CLEAN_CLOSE_RECEIPT_SCHEMA = (
    "r4b_v2_depth_bridge_coordinator_clean_close_receipt_v8"
)
_COORDINATOR_CLOSURE_ENTRY_SCHEMA = (
    "r4b_v2_depth_bridge_coordinator_closure_entry_v8"
)
_COORDINATOR_CLEAN_CLOSE_RECEIPT_DOMAIN = (
    b"R4B_V2_DEPTH_BRIDGE_COORDINATOR_CLEAN_CLOSE_RECEIPT_V8\0"
)
_COORDINATOR_CLOSURE_ENTRY_DOMAIN = (
    b"R4B_V2_DEPTH_BRIDGE_COORDINATOR_CLOSURE_ENTRY_V8\0"
)
_COORDINATOR_CLEAN_CLOSE_FACTORY_TOKEN = object()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_IDENTITY_LENGTH = 256
_MAX_SIGNED_INT64 = (1 << 63) - 1


class DepthBridgeEvidenceErrorV8(ValueError):
    """One depth-bridge evidence value or lifecycle transition is invalid."""


class DepthBridgePhaseV8(StrEnum):
    """The complete persisted depth-bridge lifecycle."""

    GENERATION_STARTED = "GENERATION_STARTED"
    TRIGGER_REGISTERED = "TRIGGER_REGISTERED"
    ATTEMPT_STARTED = "ATTEMPT_STARTED"
    ATTEMPT_TERMINAL = "ATTEMPT_TERMINAL"
    WAIT_TERMINAL = "WAIT_TERMINAL"
    CYCLE_TERMINAL = "CYCLE_TERMINAL"
    GENERATION_DRAINED = "GENERATION_DRAINED"


class DepthBridgeAttemptClassificationV8(StrEnum):
    ACCEPTED = "accepted"
    STALE = "stale"
    WAITING = "waiting"
    FAILED = "failed"


class DepthBridgeWaitOutcomeV8(StrEnum):
    ACCEPTED = "accepted"
    STALE = "stale"
    TIMEOUT = "timeout"
    SUPERSEDED = "superseded"
    GENERATION_DRAINING = "generation_draining"
    OWNER_STOPPED = "owner_stopped"


class DepthBridgeCycleOutcomeV8(StrEnum):
    ACCEPTED = "accepted"
    SUPERSEDED = "superseded"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class DepthBridgeWebSocketSourceLocatorV8:
    """Compact locator for one already-retained diff-depth WS observation."""

    symbol: str
    frame_seq: int
    ingest_seq: int
    raw_payload_sha256: str
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    first_update_id: int
    final_update_id: int
    reset: bool
    schema_version: str = _WS_LOCATOR_SCHEMA

    def __post_init__(self) -> None:
        _require_symbol(self.symbol)
        _require_positive_int(self.frame_seq, "frame_seq")
        _require_positive_int(self.ingest_seq, "ingest_seq")
        _require_sha256(self.raw_payload_sha256, "raw_payload_sha256")
        _require_nonnegative_int(self.receipt_wall_ms, "receipt_wall_ms")
        _require_nonnegative_int(
            self.receipt_monotonic_ns,
            "receipt_monotonic_ns",
        )
        _require_nonnegative_int(self.first_update_id, "first_update_id")
        _require_nonnegative_int(self.final_update_id, "final_update_id")
        if self.first_update_id > self.final_update_id:
            raise DepthBridgeEvidenceErrorV8("WS depth source range is reversed")
        if type(self.reset) is not bool:
            raise TypeError("reset must be an exact boolean")
        if self.schema_version != _WS_LOCATOR_SCHEMA:
            raise DepthBridgeEvidenceErrorV8("unsupported WS source locator schema")


@dataclass(frozen=True, slots=True)
class DepthBridgeRestSourceLocatorV8:
    """Compact locator for one queue-admitted public depth REST record."""

    symbol: str
    trigger_seq: int
    first_buffered_u: int
    bridge_attempt: int
    ingest_seq: int
    raw_record_sha256: str
    attempt_payload_sha256: str
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    schema_version: str = _REST_LOCATOR_SCHEMA

    def __post_init__(self) -> None:
        _require_symbol(self.symbol)
        _require_positive_int(self.trigger_seq, "trigger_seq")
        _require_nonnegative_int(self.first_buffered_u, "first_buffered_u")
        _require_positive_int(self.bridge_attempt, "bridge_attempt")
        _require_positive_int(self.ingest_seq, "ingest_seq")
        _require_sha256(self.raw_record_sha256, "raw_record_sha256")
        _require_sha256(
            self.attempt_payload_sha256,
            "attempt_payload_sha256",
        )
        _require_nonnegative_int(self.receipt_wall_ms, "receipt_wall_ms")
        _require_nonnegative_int(
            self.receipt_monotonic_ns,
            "receipt_monotonic_ns",
        )
        if self.schema_version != _REST_LOCATOR_SCHEMA:
            raise DepthBridgeEvidenceErrorV8("unsupported REST source locator schema")


@dataclass(frozen=True, slots=True)
class DepthBridgeRangeSummaryV8:
    """Bounded commitment to the exact buffered WS ranges used by a decision."""

    symbol: str
    range_count: int
    range_root_sha256: str
    first_ingest_seq: int | None
    last_ingest_seq: int | None
    schema_version: str = _RANGE_SUMMARY_SCHEMA

    def __post_init__(self) -> None:
        _require_symbol(self.symbol)
        _require_nonnegative_int(self.range_count, "range_count")
        if self.range_count > DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8:
            raise DepthBridgeEvidenceErrorV8(
                "range summary exceeds the frozen per-symbol buffer capacity"
            )
        _require_sha256(self.range_root_sha256, "range_root_sha256")
        if self.range_count == 0:
            if self.first_ingest_seq is not None or self.last_ingest_seq is not None:
                raise DepthBridgeEvidenceErrorV8(
                    "an empty range summary cannot name ingest endpoints"
                )
        else:
            _require_positive_int(self.first_ingest_seq, "first_ingest_seq")
            _require_positive_int(self.last_ingest_seq, "last_ingest_seq")
            assert self.first_ingest_seq is not None
            assert self.last_ingest_seq is not None
            if self.first_ingest_seq > self.last_ingest_seq:
                raise DepthBridgeEvidenceErrorV8(
                    "range summary ingest endpoints are reversed"
                )
        if self.schema_version != _RANGE_SUMMARY_SCHEMA:
            raise DepthBridgeEvidenceErrorV8("unsupported range summary schema")


@dataclass(frozen=True, slots=True)
class DepthBridgeCycleRefV8:
    cycle_id: str
    symbol: str
    symbol_ordinal: int
    trigger_seq: int
    first_buffered_u: int
    schema_version: str = _CYCLE_REF_SCHEMA

    def __post_init__(self) -> None:
        _require_sha256(self.cycle_id, "cycle_id")
        _require_symbol(self.symbol)
        _require_nonnegative_int(self.symbol_ordinal, "symbol_ordinal")
        _require_positive_int(self.trigger_seq, "trigger_seq")
        _require_nonnegative_int(self.first_buffered_u, "first_buffered_u")
        if self.schema_version != _CYCLE_REF_SCHEMA:
            raise DepthBridgeEvidenceErrorV8("unsupported cycle-reference schema")


@dataclass(frozen=True, slots=True)
class DepthBridgeRegisteredCycleV8:
    cycle: DepthBridgeCycleRefV8
    initial_range_source: DepthBridgeWebSocketSourceLocatorV8
    supersedes_cycle_id: str | None

    def __post_init__(self) -> None:
        if type(self.cycle) is not DepthBridgeCycleRefV8:
            raise TypeError("cycle must be an exact DepthBridgeCycleRefV8")
        if type(self.initial_range_source) is not DepthBridgeWebSocketSourceLocatorV8:
            raise TypeError(
                "initial_range_source must be an exact WS source locator"
            )
        _require_optional_sha256(self.supersedes_cycle_id, "supersedes_cycle_id")
        if self.initial_range_source.symbol != self.cycle.symbol:
            raise DepthBridgeEvidenceErrorV8(
                "registered cycle and initial WS source symbols differ"
            )
        if self.initial_range_source.first_update_id != self.cycle.first_buffered_u:
            raise DepthBridgeEvidenceErrorV8(
                "registered cycle first buffered U differs from its WS source"
            )


@dataclass(frozen=True, slots=True)
class DepthBridgeGenerationStartedV8:
    symbol_count: int
    symbol_census_sha256: str
    maximum_concurrency: int
    maximum_buffered_ranges_per_symbol: int
    bridge_maximum_attempts: int
    bridge_wait_timeout_ms: int

    def __post_init__(self) -> None:
        _require_positive_int(self.symbol_count, "symbol_count")
        _require_sha256(self.symbol_census_sha256, "symbol_census_sha256")
        _require_positive_int(self.maximum_concurrency, "maximum_concurrency")
        if (
            self.maximum_buffered_ranges_per_symbol
            != DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8
        ):
            raise DepthBridgeEvidenceErrorV8(
                "generation range capacity differs from the frozen bound"
            )
        _require_positive_int(
            self.bridge_maximum_attempts,
            "bridge_maximum_attempts",
        )
        _require_positive_int(
            self.bridge_wait_timeout_ms,
            "bridge_wait_timeout_ms",
        )


@dataclass(frozen=True, slots=True)
class DepthBridgeTriggerRegisteredV8:
    trigger: str
    trigger_seq: int
    cycles: tuple[DepthBridgeRegisteredCycleV8, ...]

    def __post_init__(self) -> None:
        if self.trigger not in ("startup", "reconnect", "sequence_gap"):
            raise DepthBridgeEvidenceErrorV8("unsupported bridge trigger")
        _require_positive_int(self.trigger_seq, "trigger_seq")
        if type(self.cycles) is not tuple or not self.cycles:
            raise TypeError("cycles must be a nonempty exact tuple")
        if any(type(value) is not DepthBridgeRegisteredCycleV8 for value in self.cycles):
            raise TypeError("cycles contain a non-exact registered-cycle value")
        if any(value.cycle.trigger_seq != self.trigger_seq for value in self.cycles):
            raise DepthBridgeEvidenceErrorV8(
                "registered cycles differ from their trigger sequence"
            )


@dataclass(frozen=True, slots=True)
class DepthBridgeAttemptStartedV8:
    cycle: DepthBridgeCycleRefV8
    bridge_attempt: int

    def __post_init__(self) -> None:
        if type(self.cycle) is not DepthBridgeCycleRefV8:
            raise TypeError("cycle must be an exact DepthBridgeCycleRefV8")
        _require_positive_int(self.bridge_attempt, "bridge_attempt")


@dataclass(frozen=True, slots=True)
class DepthBridgeAttemptTerminalV8:
    cycle: DepthBridgeCycleRefV8
    bridge_attempt: int
    classification: str
    rest_source: DepthBridgeRestSourceLocatorV8 | None
    semantic_admission_sha256: str | None
    last_update_id: int | None
    target_update_id: int | None
    discarded_range_count: int
    range_summary: DepthBridgeRangeSummaryV8
    failure_code: str | None
    wait_started_monotonic_ns: int | None
    wait_deadline_monotonic_ns: int | None

    def __post_init__(self) -> None:
        if type(self.cycle) is not DepthBridgeCycleRefV8:
            raise TypeError("cycle must be an exact DepthBridgeCycleRefV8")
        _require_positive_int(self.bridge_attempt, "bridge_attempt")
        if self.classification not in tuple(DepthBridgeAttemptClassificationV8):
            raise DepthBridgeEvidenceErrorV8("unsupported attempt classification")
        if (
            self.rest_source is not None
            and type(self.rest_source) is not DepthBridgeRestSourceLocatorV8
        ):
            raise TypeError("rest_source must be an exact REST source locator or None")
        if type(self.range_summary) is not DepthBridgeRangeSummaryV8:
            raise TypeError("range_summary must be an exact range summary")
        _require_nonnegative_int(
            self.discarded_range_count,
            "discarded_range_count",
        )
        if self.discarded_range_count > self.range_summary.range_count:
            raise DepthBridgeEvidenceErrorV8(
                "discarded range count exceeds the committed range count"
            )
        if self.range_summary.symbol != self.cycle.symbol:
            raise DepthBridgeEvidenceErrorV8(
                "attempt and range-summary symbols differ"
            )
        if self.classification == DepthBridgeAttemptClassificationV8.FAILED:
            if self.failure_code not in (
                "http_terminal",
                "semantic_invalid",
                "admission_cancelled",
                "owner_failure",
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "failed attempt requires a closed failure code"
                )
            if any(
                value is not None
                for value in (
                    self.semantic_admission_sha256,
                    self.last_update_id,
                    self.target_update_id,
                    self.wait_started_monotonic_ns,
                    self.wait_deadline_monotonic_ns,
                )
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "failed attempt cannot assert semantic or wait evidence"
                )
            if (
                self.failure_code
                in ("http_terminal", "semantic_invalid", "admission_cancelled")
                and self.rest_source is None
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "retained failed attempt requires its REST source locator"
                )
            if (
                self.rest_source is not None
                and self.rest_source.symbol != self.cycle.symbol
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "failed attempt and REST source symbols differ"
                )
            if self.rest_source is not None and (
                self.rest_source.trigger_seq != self.cycle.trigger_seq
                or self.rest_source.first_buffered_u
                != self.cycle.first_buffered_u
                or self.rest_source.bridge_attempt != self.bridge_attempt
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "failed attempt and REST source scheduler identities differ"
                )
        else:
            if self.failure_code is not None or self.rest_source is None:
                raise DepthBridgeEvidenceErrorV8(
                    "classified snapshot requires one REST source and no failure code"
                )
            _require_sha256(
                self.semantic_admission_sha256,
                "semantic_admission_sha256",
            )
            _require_nonnegative_int(self.last_update_id, "last_update_id")
            _require_nonnegative_int(self.target_update_id, "target_update_id")
            if self.rest_source.symbol != self.cycle.symbol:
                raise DepthBridgeEvidenceErrorV8(
                    "attempt and REST source symbols differ"
                )
            if (
                self.rest_source.trigger_seq != self.cycle.trigger_seq
                or self.rest_source.first_buffered_u
                != self.cycle.first_buffered_u
                or self.rest_source.bridge_attempt != self.bridge_attempt
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "attempt and REST source scheduler identities differ"
                )
            if self.classification == DepthBridgeAttemptClassificationV8.WAITING:
                _require_nonnegative_int(
                    self.wait_started_monotonic_ns,
                    "wait_started_monotonic_ns",
                )
                _require_nonnegative_int(
                    self.wait_deadline_monotonic_ns,
                    "wait_deadline_monotonic_ns",
                )
                assert self.wait_started_monotonic_ns is not None
                assert self.wait_deadline_monotonic_ns is not None
                if self.wait_deadline_monotonic_ns <= self.wait_started_monotonic_ns:
                    raise DepthBridgeEvidenceErrorV8(
                        "bridge wait deadline must follow its start"
                    )
            elif (
                self.wait_started_monotonic_ns is not None
                or self.wait_deadline_monotonic_ns is not None
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "non-waiting attempt cannot assert a wait interval"
                )


@dataclass(frozen=True, slots=True)
class DepthBridgeWaitTerminalV8:
    cycle: DepthBridgeCycleRefV8
    bridge_attempt: int
    outcome: str
    wait_started_monotonic_ns: int
    wait_deadline_monotonic_ns: int
    wait_ended_monotonic_ns: int
    target_update_id: int
    discarded_range_count: int
    range_summary: DepthBridgeRangeSummaryV8

    def __post_init__(self) -> None:
        if type(self.cycle) is not DepthBridgeCycleRefV8:
            raise TypeError("cycle must be an exact DepthBridgeCycleRefV8")
        _require_positive_int(self.bridge_attempt, "bridge_attempt")
        if self.outcome not in tuple(DepthBridgeWaitOutcomeV8):
            raise DepthBridgeEvidenceErrorV8("unsupported wait outcome")
        _require_nonnegative_int(
            self.wait_started_monotonic_ns,
            "wait_started_monotonic_ns",
        )
        _require_nonnegative_int(
            self.wait_deadline_monotonic_ns,
            "wait_deadline_monotonic_ns",
        )
        _require_nonnegative_int(
            self.wait_ended_monotonic_ns,
            "wait_ended_monotonic_ns",
        )
        if self.wait_deadline_monotonic_ns <= self.wait_started_monotonic_ns:
            raise DepthBridgeEvidenceErrorV8(
                "bridge wait deadline must follow its start"
            )
        if self.wait_ended_monotonic_ns < self.wait_started_monotonic_ns:
            raise DepthBridgeEvidenceErrorV8("bridge wait ended before it started")
        if (
            self.outcome == DepthBridgeWaitOutcomeV8.TIMEOUT
            and self.wait_ended_monotonic_ns < self.wait_deadline_monotonic_ns
        ):
            raise DepthBridgeEvidenceErrorV8("timeout evidence precedes its deadline")
        _require_nonnegative_int(self.target_update_id, "target_update_id")
        _require_nonnegative_int(
            self.discarded_range_count,
            "discarded_range_count",
        )
        if type(self.range_summary) is not DepthBridgeRangeSummaryV8:
            raise TypeError("range_summary must be an exact range summary")
        if self.discarded_range_count > self.range_summary.range_count:
            raise DepthBridgeEvidenceErrorV8(
                "discarded range count exceeds the committed range count"
            )
        if self.range_summary.symbol != self.cycle.symbol:
            raise DepthBridgeEvidenceErrorV8(
                "wait and range-summary symbols differ"
            )


@dataclass(frozen=True, slots=True)
class DepthBridgeCycleTerminalV8:
    cycle: DepthBridgeCycleRefV8
    outcome: str
    reason: str
    terminal_bridge_attempt: int | None
    semantic_admission_sha256: str | None
    target_update_id: int | None
    bridging_range_summary: DepthBridgeRangeSummaryV8 | None

    def __post_init__(self) -> None:
        if type(self.cycle) is not DepthBridgeCycleRefV8:
            raise TypeError("cycle must be an exact DepthBridgeCycleRefV8")
        if self.outcome not in tuple(DepthBridgeCycleOutcomeV8):
            raise DepthBridgeEvidenceErrorV8("unsupported cycle outcome")
        allowed_reasons = {
            DepthBridgeCycleOutcomeV8.ACCEPTED: {"snapshot_range_bridge"},
            DepthBridgeCycleOutcomeV8.SUPERSEDED: {
                "newer_trigger",
                "generation_draining",
            },
            DepthBridgeCycleOutcomeV8.FAILED: {
                "http_terminal",
                "semantic_invalid",
                "attempts_exhausted_stale",
                "attempts_exhausted_timeout",
                "range_buffer_overflow",
                "owner_stopped_unresolved",
                "coordinator_fatal",
            },
        }
        if self.reason not in allowed_reasons[DepthBridgeCycleOutcomeV8(self.outcome)]:
            raise DepthBridgeEvidenceErrorV8(
                "cycle terminal reason differs from its outcome"
            )
        if self.outcome == DepthBridgeCycleOutcomeV8.ACCEPTED:
            _require_positive_int(
                self.terminal_bridge_attempt,
                "terminal_bridge_attempt",
            )
            _require_sha256(
                self.semantic_admission_sha256,
                "semantic_admission_sha256",
            )
            _require_nonnegative_int(self.target_update_id, "target_update_id")
            if type(self.bridging_range_summary) is not DepthBridgeRangeSummaryV8:
                raise TypeError(
                    "accepted cycle requires an exact bridging range summary"
                )
            if self.bridging_range_summary.symbol != self.cycle.symbol:
                raise DepthBridgeEvidenceErrorV8(
                    "cycle and bridging range-summary symbols differ"
                )
        elif any(
            value is not None
            for value in (
                self.semantic_admission_sha256,
                self.target_update_id,
                self.bridging_range_summary,
            )
        ):
            raise DepthBridgeEvidenceErrorV8(
                "non-accepted cycle cannot assert a successful bridge"
            )
        elif self.terminal_bridge_attempt is not None:
            _require_positive_int(
                self.terminal_bridge_attempt,
                "terminal_bridge_attempt",
            )


@dataclass(frozen=True, slots=True)
class DepthBridgeGenerationDrainedV8:
    reason: str
    fatal_cause_code: str | None
    fatal_cause_sha256: str | None
    registered_cycle_count: int
    accepted_cycle_count: int
    superseded_cycle_count: int
    failed_cycle_count: int
    worker_count: int
    permit_in_use_count: int
    retained_registration_count: int
    pending_registration_count: int
    retained_token_count: int
    claimed_token_count: int
    adapter_active_attempt_count: int
    adapter_pending_owner_task_count: int
    retained_terminal_admission_count: int
    adapter_closed: bool
    adapter_cleanly_closed: bool

    def __post_init__(self) -> None:
        if self.reason not in ("reconnect", "normal_stop", "fatal"):
            raise DepthBridgeEvidenceErrorV8("unsupported generation drain reason")
        if self.reason == "fatal":
            if self.fatal_cause_code not in (
                "pretrigger_range_buffer_overflow",
                "coordinator_failure",
                "adapter_failure",
                "ledger_failure",
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "fatal generation drain requires a closed cause code"
                )
            _require_sha256(self.fatal_cause_sha256, "fatal_cause_sha256")
        elif self.fatal_cause_code is not None or self.fatal_cause_sha256 is not None:
            raise DepthBridgeEvidenceErrorV8(
                "nonfatal generation drain cannot assert fatal cause evidence"
            )
        for name in (
            "registered_cycle_count",
            "accepted_cycle_count",
            "superseded_cycle_count",
            "failed_cycle_count",
            "worker_count",
            "permit_in_use_count",
            "retained_registration_count",
            "pending_registration_count",
            "retained_token_count",
            "claimed_token_count",
            "adapter_active_attempt_count",
            "adapter_pending_owner_task_count",
            "retained_terminal_admission_count",
        ):
            _require_nonnegative_int(getattr(self, name), name)
        if (
            self.accepted_cycle_count
            + self.superseded_cycle_count
            + self.failed_cycle_count
            != self.registered_cycle_count
        ):
            raise DepthBridgeEvidenceErrorV8(
                "generation terminal cycle census does not close"
            )
        if any(
            getattr(self, name) != 0
            for name in (
                "worker_count",
                "permit_in_use_count",
                "retained_registration_count",
                "pending_registration_count",
                "retained_token_count",
                "claimed_token_count",
                "adapter_active_attempt_count",
                "adapter_pending_owner_task_count",
                "retained_terminal_admission_count",
            )
        ):
            raise DepthBridgeEvidenceErrorV8(
                "generation drain requires zero live owner counters"
            )
        if self.adapter_closed is not True:
            raise DepthBridgeEvidenceErrorV8(
                "generation drain requires the adapter to be closed"
            )
        if type(self.adapter_cleanly_closed) is not bool:
            raise TypeError("adapter_cleanly_closed must be an exact boolean")
        if self.reason != "fatal" and self.adapter_cleanly_closed is not True:
            raise DepthBridgeEvidenceErrorV8(
                "nonfatal generation drain requires a clean adapter closure"
            )


type DepthBridgePhaseMaterialV8 = (
    DepthBridgeGenerationStartedV8
    | DepthBridgeTriggerRegisteredV8
    | DepthBridgeAttemptStartedV8
    | DepthBridgeAttemptTerminalV8
    | DepthBridgeWaitTerminalV8
    | DepthBridgeCycleTerminalV8
    | DepthBridgeGenerationDrainedV8
)


_MATERIAL_TYPE_BY_PHASE: dict[DepthBridgePhaseV8, type[DepthBridgePhaseMaterialV8]] = {
    DepthBridgePhaseV8.GENERATION_STARTED: DepthBridgeGenerationStartedV8,
    DepthBridgePhaseV8.TRIGGER_REGISTERED: DepthBridgeTriggerRegisteredV8,
    DepthBridgePhaseV8.ATTEMPT_STARTED: DepthBridgeAttemptStartedV8,
    DepthBridgePhaseV8.ATTEMPT_TERMINAL: DepthBridgeAttemptTerminalV8,
    DepthBridgePhaseV8.WAIT_TERMINAL: DepthBridgeWaitTerminalV8,
    DepthBridgePhaseV8.CYCLE_TERMINAL: DepthBridgeCycleTerminalV8,
    DepthBridgePhaseV8.GENERATION_DRAINED: DepthBridgeGenerationDrainedV8,
}


@dataclass(frozen=True, slots=True)
class DepthBridgeEvidencePayloadV8:
    """Canonical, qualification-only evidence written by the integrity ledger."""

    phase: str
    session_id: str
    protocol_hash: str
    plan_bundle_sha256: str
    depth_plan_sha256: str
    connection_id: str
    connection_generation: int
    material: DepthBridgePhaseMaterialV8
    qualification_only: bool = True
    promoting: bool = False
    promotion_ready: bool = False
    wal_durability_verified: bool = False
    finality_fence_verified: bool = False
    m2_certified: bool = False
    book_bridge_certified: bool = False
    liquidity_signal_emitted: bool = False
    order_execution_enabled: bool = False
    schema_version: str = _SCHEMA

    def __post_init__(self) -> None:
        try:
            phase = DepthBridgePhaseV8(self.phase)
        except ValueError as exc:
            raise DepthBridgeEvidenceErrorV8("unsupported depth-bridge phase") from exc
        expected_material_type = _MATERIAL_TYPE_BY_PHASE[phase]
        if type(self.material) is not expected_material_type:
            raise TypeError("depth-bridge phase material has the wrong exact type")
        _require_identity(self.session_id, "session_id")
        _require_sha256(self.protocol_hash, "protocol_hash")
        _require_sha256(self.plan_bundle_sha256, "plan_bundle_sha256")
        _require_sha256(self.depth_plan_sha256, "depth_plan_sha256")
        _require_identity(self.connection_id, "connection_id")
        _require_positive_int(
            self.connection_generation,
            "connection_generation",
        )
        if (
            self.qualification_only is not True
            or self.promoting is not False
            or self.promotion_ready is not False
            or self.wal_durability_verified is not False
            or self.finality_fence_verified is not False
            or self.m2_certified is not False
            or self.book_bridge_certified is not False
            or self.liquidity_signal_emitted is not False
            or self.order_execution_enabled is not False
        ):
            raise DepthBridgeEvidenceErrorV8(
                "depth-bridge evidence must remain qualification-only and non-authoritative"
            )
        if self.schema_version != _SCHEMA:
            raise DepthBridgeEvidenceErrorV8(
                "unsupported depth-bridge evidence schema"
            )


@dataclass(frozen=True, slots=True)
class DepthBridgeEvidenceCensusV8:
    event_count: int
    generation_started_count: int
    generation_drained_count: int
    trigger_count: int
    cycle_count: int
    failed_cycle_count: int
    fatal_generation_count: int
    last_drain_reason: str | None
    open_generation_count: int
    open_cycle_count: int
    open_attempt_count: int
    open_wait_count: int

    @property
    def open_terminal_reservation_count(self) -> int:
        return (
            self.open_generation_count
            + self.open_cycle_count
            + self.open_attempt_count
            + self.open_wait_count
        )


@dataclass(frozen=True, slots=True)
class DepthBridgeCoordinatorCleanCloseReceiptV8:
    """Factory-only proof that the bounded bridge owner stopped normally.

    This receipt closes only the qualification-only depth bridge.  It does not
    assert capture finality, parser completeness, M2 certification, or a CLEAN
    capture session.
    """

    session_id: str
    protocol_hash: str
    plan_bundle_sha256: str
    depth_plan_sha256: str
    plan_count: Literal[4]
    last_connection_id: str
    last_connection_generation: int
    generation_started_count: int
    generation_drained_count: int
    fatal_generation_count: int
    close_reason: Literal["normal_stop"]
    last_generation_drained_event_sequence: int
    last_generation_drained_event_sha256: str
    last_generation_drained_recorded_wall_ms: int
    last_generation_drained_recorded_monotonic_ns: int
    worker_count: Literal[0]
    permit_in_use_count: Literal[0]
    retained_registration_count: Literal[0]
    pending_registration_count: Literal[0]
    retained_token_count: Literal[0]
    claimed_token_count: Literal[0]
    adapter_active_attempt_count: Literal[0]
    adapter_pending_owner_task_count: Literal[0]
    retained_terminal_admission_count: Literal[0]
    coordinator_closed: Literal[True]
    generation_open: Literal[False]
    callbacks_accepting: Literal[False]
    scheduler_generation_open: Literal[False]
    adapter_attached: Literal[False]
    close_wall_ms: int
    close_monotonic_ns: int
    qualification_only: Literal[True]
    capture_finality_verified: Literal[False]
    m2_certified: Literal[False]
    session_clean_claimed: Literal[False]
    order_execution_enabled: Literal[False]
    schema_version: Literal[
        "r4b_v2_depth_bridge_coordinator_clean_close_receipt_v8"
    ] = _COORDINATOR_CLEAN_CLOSE_RECEIPT_SCHEMA
    receipt_sha256: str = field(init=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _COORDINATOR_CLEAN_CLOSE_FACTORY_TOKEN:
            raise TypeError("depth bridge clean-close receipts are factory-sealed")
        object.__setattr__(
            self,
            "_factory_seal",
            _COORDINATOR_CLEAN_CLOSE_FACTORY_TOKEN,
        )
        _validate_depth_bridge_coordinator_close_material_v8(self)
        object.__setattr__(
            self,
            "receipt_sha256",
            _depth_bridge_coordinator_clean_close_receipt_sha256_v8(self),
        )


@dataclass(frozen=True, slots=True)
class DepthBridgeCoordinatorClosureEntryV8:
    """Canonical serializable projection of one verified bridge close receipt."""

    session_id: str
    protocol_hash: str
    plan_bundle_sha256: str
    depth_plan_sha256: str
    plan_count: Literal[4]
    last_connection_id: str
    last_connection_generation: int
    generation_started_count: int
    generation_drained_count: int
    fatal_generation_count: int
    close_reason: Literal["normal_stop"]
    last_generation_drained_event_sequence: int
    last_generation_drained_event_sha256: str
    last_generation_drained_recorded_wall_ms: int
    last_generation_drained_recorded_monotonic_ns: int
    worker_count: Literal[0]
    permit_in_use_count: Literal[0]
    retained_registration_count: Literal[0]
    pending_registration_count: Literal[0]
    retained_token_count: Literal[0]
    claimed_token_count: Literal[0]
    adapter_active_attempt_count: Literal[0]
    adapter_pending_owner_task_count: Literal[0]
    retained_terminal_admission_count: Literal[0]
    coordinator_closed: Literal[True]
    generation_open: Literal[False]
    callbacks_accepting: Literal[False]
    scheduler_generation_open: Literal[False]
    adapter_attached: Literal[False]
    close_wall_ms: int
    close_monotonic_ns: int
    qualification_only: Literal[True]
    capture_finality_verified: Literal[False]
    m2_certified: Literal[False]
    session_clean_claimed: Literal[False]
    order_execution_enabled: Literal[False]
    receipt_sha256: str
    schema_version: Literal[
        "r4b_v2_depth_bridge_coordinator_closure_entry_v8"
    ] = _COORDINATOR_CLOSURE_ENTRY_SCHEMA

    def __post_init__(self) -> None:
        _validate_depth_bridge_coordinator_close_material_v8(self)
        _require_sha256(self.receipt_sha256, "receipt_sha256")


_COORDINATOR_SHARED_CLOSE_FIELD_NAMES_V8 = (
    "session_id",
    "protocol_hash",
    "plan_bundle_sha256",
    "depth_plan_sha256",
    "plan_count",
    "last_connection_id",
    "last_connection_generation",
    "generation_started_count",
    "generation_drained_count",
    "fatal_generation_count",
    "close_reason",
    "last_generation_drained_event_sequence",
    "last_generation_drained_event_sha256",
    "last_generation_drained_recorded_wall_ms",
    "last_generation_drained_recorded_monotonic_ns",
    "worker_count",
    "permit_in_use_count",
    "retained_registration_count",
    "pending_registration_count",
    "retained_token_count",
    "claimed_token_count",
    "adapter_active_attempt_count",
    "adapter_pending_owner_task_count",
    "retained_terminal_admission_count",
    "coordinator_closed",
    "generation_open",
    "callbacks_accepting",
    "scheduler_generation_open",
    "adapter_attached",
    "close_wall_ms",
    "close_monotonic_ns",
    "qualification_only",
    "capture_finality_verified",
    "m2_certified",
    "session_clean_claimed",
    "order_execution_enabled",
)


def _issue_depth_bridge_coordinator_clean_close_receipt_v8(
    *,
    session_id: str,
    protocol_hash: str,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
    last_connection_id: str,
    last_connection_generation: int,
    generation_started_count: int,
    generation_drained_count: int,
    fatal_generation_count: int,
    last_generation_drained_event_sequence: int,
    last_generation_drained_event_sha256: str,
    last_generation_drained_recorded_wall_ms: int,
    last_generation_drained_recorded_monotonic_ns: int,
    close_wall_ms: int,
    close_monotonic_ns: int,
) -> DepthBridgeCoordinatorCleanCloseReceiptV8:
    """Mint one normal-close authority for the exact four-plan V8 bridge."""

    validate_provisional_promoting_capture_plans_v8(promoting_plans)
    validate_public_depth_rest_plan_v8(depth_plan)
    if len(promoting_plans) != 4:
        raise DepthBridgeEvidenceErrorV8(
            "bridge clean close requires the exact four-plan V8 authority"
        )
    if not any(plan is depth_plan for plan in promoting_plans):
        raise DepthBridgeEvidenceErrorV8(
            "bridge clean close requires the exact depth plan member"
        )
    receipt = DepthBridgeCoordinatorCleanCloseReceiptV8(
        session_id=session_id,
        protocol_hash=protocol_hash,
        plan_bundle_sha256=provisional_promoting_plan_sha256_v8(promoting_plans),
        depth_plan_sha256=public_depth_rest_plan_sha256_v8(depth_plan),
        plan_count=4,
        last_connection_id=last_connection_id,
        last_connection_generation=last_connection_generation,
        generation_started_count=generation_started_count,
        generation_drained_count=generation_drained_count,
        fatal_generation_count=fatal_generation_count,
        close_reason="normal_stop",
        last_generation_drained_event_sequence=(
            last_generation_drained_event_sequence
        ),
        last_generation_drained_event_sha256=(
            last_generation_drained_event_sha256
        ),
        last_generation_drained_recorded_wall_ms=(
            last_generation_drained_recorded_wall_ms
        ),
        last_generation_drained_recorded_monotonic_ns=(
            last_generation_drained_recorded_monotonic_ns
        ),
        worker_count=0,
        permit_in_use_count=0,
        retained_registration_count=0,
        pending_registration_count=0,
        retained_token_count=0,
        claimed_token_count=0,
        adapter_active_attempt_count=0,
        adapter_pending_owner_task_count=0,
        retained_terminal_admission_count=0,
        coordinator_closed=True,
        generation_open=False,
        callbacks_accepting=False,
        scheduler_generation_open=False,
        adapter_attached=False,
        close_wall_ms=close_wall_ms,
        close_monotonic_ns=close_monotonic_ns,
        qualification_only=True,
        capture_finality_verified=False,
        m2_certified=False,
        session_clean_claimed=False,
        order_execution_enabled=False,
        _factory_token=_COORDINATOR_CLEAN_CLOSE_FACTORY_TOKEN,
    )
    validate_depth_bridge_coordinator_clean_close_receipt_v8(
        receipt,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    return receipt


def validate_depth_bridge_coordinator_clean_close_receipt_v8(
    receipt: DepthBridgeCoordinatorCleanCloseReceiptV8,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> None:
    """Validate factory provenance, digest, and exact live plan bindings."""

    if type(receipt) is not DepthBridgeCoordinatorCleanCloseReceiptV8:
        raise TypeError(
            "receipt must be an exact DepthBridgeCoordinatorCleanCloseReceiptV8"
        )
    if (
        getattr(receipt, "_factory_seal", None)
        is not _COORDINATOR_CLEAN_CLOSE_FACTORY_TOKEN
    ):
        raise DepthBridgeEvidenceErrorV8(
            "depth bridge clean-close receipt lacks factory provenance"
        )
    _validate_depth_bridge_coordinator_close_material_v8(receipt)
    validate_provisional_promoting_capture_plans_v8(promoting_plans)
    validate_public_depth_rest_plan_v8(depth_plan)
    if (
        len(promoting_plans) != 4
        or not any(plan is depth_plan for plan in promoting_plans)
        or receipt.plan_bundle_sha256
        != provisional_promoting_plan_sha256_v8(promoting_plans)
        or receipt.depth_plan_sha256 != public_depth_rest_plan_sha256_v8(depth_plan)
    ):
        raise DepthBridgeEvidenceErrorV8(
            "depth bridge clean-close receipt has foreign plan authority"
        )
    expected = _depth_bridge_coordinator_clean_close_receipt_sha256_v8(receipt)
    if not hmac.compare_digest(receipt.receipt_sha256, expected):
        raise DepthBridgeEvidenceErrorV8(
            "depth bridge clean-close receipt digest changed"
        )


def depth_bridge_coordinator_closure_entry_v8(
    receipt: DepthBridgeCoordinatorCleanCloseReceiptV8,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> DepthBridgeCoordinatorClosureEntryV8:
    """Project one verified factory receipt into canonical persisted values."""

    validate_depth_bridge_coordinator_clean_close_receipt_v8(
        receipt,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    entry = DepthBridgeCoordinatorClosureEntryV8(
        **{
            name: getattr(receipt, name)
            for name in _COORDINATOR_SHARED_CLOSE_FIELD_NAMES_V8
        },
        receipt_sha256=receipt.receipt_sha256,
    )
    validate_depth_bridge_coordinator_closure_entry_v8(
        entry,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    return entry


def validate_depth_bridge_coordinator_closure_entry_v8(
    entry: DepthBridgeCoordinatorClosureEntryV8,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> None:
    """Validate the persisted bridge closure projection and plan bindings."""

    if type(entry) is not DepthBridgeCoordinatorClosureEntryV8:
        raise TypeError(
            "entry must be an exact DepthBridgeCoordinatorClosureEntryV8"
        )
    entry.__post_init__()
    validate_provisional_promoting_capture_plans_v8(promoting_plans)
    validate_public_depth_rest_plan_v8(depth_plan)
    if (
        len(promoting_plans) != 4
        or not any(plan is depth_plan for plan in promoting_plans)
        or entry.plan_bundle_sha256
        != provisional_promoting_plan_sha256_v8(promoting_plans)
        or entry.depth_plan_sha256 != public_depth_rest_plan_sha256_v8(depth_plan)
    ):
        raise DepthBridgeEvidenceErrorV8(
            "depth bridge closure entry has foreign plan authority"
        )
    receipt_material = {
        name: getattr(entry, name)
        for name in _COORDINATOR_SHARED_CLOSE_FIELD_NAMES_V8
    }
    receipt_material["schema_version"] = _COORDINATOR_CLEAN_CLOSE_RECEIPT_SCHEMA
    expected_receipt_sha256 = hashlib.sha256(
        _COORDINATOR_CLEAN_CLOSE_RECEIPT_DOMAIN
        + canonical_json_line(receipt_material)
    ).hexdigest()
    if not hmac.compare_digest(entry.receipt_sha256, expected_receipt_sha256):
        raise DepthBridgeEvidenceErrorV8(
            "depth bridge closure entry receipt digest changed"
        )


def depth_bridge_coordinator_closure_entry_sha256_v8(
    entry: DepthBridgeCoordinatorClosureEntryV8,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> str:
    """Hash one exact canonical persisted bridge closure entry."""

    validate_depth_bridge_coordinator_closure_entry_v8(
        entry,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    return hashlib.sha256(
        _COORDINATOR_CLOSURE_ENTRY_DOMAIN + canonical_json_line(asdict(entry))
    ).hexdigest()


def build_depth_bridge_range_summary_v8(
    locators: tuple[DepthBridgeWebSocketSourceLocatorV8, ...],
    *,
    symbol: str | None = None,
) -> DepthBridgeRangeSummaryV8:
    """Commit to a bounded, ingest-ordered tuple without copying raw payloads."""

    if type(locators) is not tuple:
        raise TypeError("locators must be an exact tuple")
    if any(type(value) is not DepthBridgeWebSocketSourceLocatorV8 for value in locators):
        raise TypeError("locators contain a non-exact WS source locator")
    if symbol is None:
        if not locators:
            raise DepthBridgeEvidenceErrorV8(
                "an empty range root requires its exact symbol"
            )
        observed_symbol = locators[0].symbol
    else:
        _require_symbol(symbol)
        observed_symbol = symbol
    if any(value.symbol != observed_symbol for value in locators):
        raise DepthBridgeEvidenceErrorV8(
            "range locators cannot cross symbols"
        )
    ingest_sequences = tuple(value.ingest_seq for value in locators)
    if tuple(sorted(ingest_sequences)) != ingest_sequences:
        raise DepthBridgeEvidenceErrorV8(
            "range locators must be ordered by ingest sequence"
        )
    if len(set(ingest_sequences)) != len(ingest_sequences):
        raise DepthBridgeEvidenceErrorV8(
            "range locators contain duplicate ingest sequences"
        )
    document = {
        "schema_version": "r4b_v2_depth_bridge_range_root_input_v8",
        "locators": [asdict(value) for value in locators],
    }
    return DepthBridgeRangeSummaryV8(
        symbol=observed_symbol,
        range_count=len(locators),
        range_root_sha256=hashlib.sha256(
            _RANGE_ROOT_DOMAIN + canonical_json_line(document)
        ).hexdigest(),
        first_ingest_seq=ingest_sequences[0] if ingest_sequences else None,
        last_ingest_seq=ingest_sequences[-1] if ingest_sequences else None,
    )


def depth_bridge_symbol_census_sha256_v8(symbols: tuple[str, ...]) -> str:
    if type(symbols) is not tuple or not symbols:
        raise TypeError("symbols must be a nonempty exact tuple")
    if tuple(sorted(symbols)) != symbols or len(set(symbols)) != len(symbols):
        raise DepthBridgeEvidenceErrorV8(
            "symbols must be unique lexicographic order"
        )
    for symbol in symbols:
        _require_symbol(symbol)
    document = {
        "schema_version": "r4b_v2_depth_bridge_symbol_census_v8",
        "symbols": symbols,
    }
    return hashlib.sha256(
        _SYMBOL_CENSUS_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def build_depth_bridge_cycle_ref_v8(
    *,
    session_id: str,
    protocol_hash: str,
    plan_bundle_sha256: str,
    depth_plan_sha256: str,
    connection_id: str,
    connection_generation: int,
    symbol: str,
    symbol_ordinal: int,
    trigger_seq: int,
    first_buffered_u: int,
) -> DepthBridgeCycleRefV8:
    """Build the deterministic persistent identity for one scheduler cycle."""

    identity = _cycle_identity_document(
        session_id=session_id,
        protocol_hash=protocol_hash,
        plan_bundle_sha256=plan_bundle_sha256,
        depth_plan_sha256=depth_plan_sha256,
        connection_id=connection_id,
        connection_generation=connection_generation,
        symbol=symbol,
        symbol_ordinal=symbol_ordinal,
        trigger_seq=trigger_seq,
        first_buffered_u=first_buffered_u,
    )
    return DepthBridgeCycleRefV8(
        cycle_id=hashlib.sha256(
            _CYCLE_ID_DOMAIN + canonical_json_line(identity)
        ).hexdigest(),
        symbol=symbol,
        symbol_ordinal=symbol_ordinal,
        trigger_seq=trigger_seq,
        first_buffered_u=first_buffered_u,
    )


def build_depth_bridge_evidence_payload_v8(
    *,
    phase: DepthBridgePhaseV8,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    connection_generation: int,
    material: DepthBridgePhaseMaterialV8,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> DepthBridgeEvidencePayloadV8:
    """Build and exact-plan validate one payload for ledger append."""

    if type(phase) is not DepthBridgePhaseV8:
        raise TypeError("phase must be an exact DepthBridgePhaseV8")
    _validate_exact_plan_pair(promoting_plans, depth_plan)
    payload = DepthBridgeEvidencePayloadV8(
        phase=phase.value,
        session_id=session_id,
        protocol_hash=protocol_hash,
        plan_bundle_sha256=provisional_promoting_plan_sha256_v8(promoting_plans),
        depth_plan_sha256=public_depth_rest_plan_sha256_v8(depth_plan),
        connection_id=connection_id,
        connection_generation=connection_generation,
        material=material,
    )
    validate_depth_bridge_evidence_payload_v8(
        payload,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    return payload


def parse_depth_bridge_evidence_payload_v8(
    document: dict[str, object],
) -> DepthBridgeEvidencePayloadV8:
    """Parse only the exact canonical seven-phase union projection."""

    _require_exact_document_keys(document, DepthBridgeEvidencePayloadV8)
    converted = dict(document)
    raw_phase = converted.get("phase")
    if type(raw_phase) is not str:
        raise TypeError("depth-bridge phase must be exact text")
    try:
        phase = DepthBridgePhaseV8(raw_phase)
    except ValueError as exc:
        raise DepthBridgeEvidenceErrorV8("unsupported depth-bridge phase") from exc
    material = converted.get("material")
    if type(material) is not dict:
        raise TypeError("depth-bridge material must be an exact object")
    converted["material"] = _parse_phase_material(phase, material)
    payload = DepthBridgeEvidencePayloadV8(**converted)  # type: ignore[arg-type]
    if canonical_json_line(asdict(payload)) != canonical_json_line(document):
        raise DepthBridgeEvidenceErrorV8(
            "depth-bridge payload differs from its canonical typed projection"
        )
    return payload


def validate_depth_bridge_evidence_payload_v8(
    payload: DepthBridgeEvidencePayloadV8,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...] | None = None,
    depth_plan: ProvisionalDepthRestQualificationPlanV8 | None = None,
) -> None:
    """Revalidate canonical material and, when supplied, the exact v8 plan pair."""

    if type(payload) is not DepthBridgeEvidencePayloadV8:
        raise TypeError("payload must be an exact DepthBridgeEvidencePayloadV8")
    payload.__post_init__()
    _validate_cycle_references(payload)
    if (promoting_plans is None) is not (depth_plan is None):
        raise TypeError("promoting plans and depth plan must be supplied together")
    if promoting_plans is None or depth_plan is None:
        return
    _validate_exact_plan_pair(promoting_plans, depth_plan)
    if payload.plan_bundle_sha256 != provisional_promoting_plan_sha256_v8(
        promoting_plans
    ):
        raise DepthBridgeEvidenceErrorV8(
            "depth-bridge payload names a foreign v8 plan tuple"
        )
    if payload.depth_plan_sha256 != public_depth_rest_plan_sha256_v8(depth_plan):
        raise DepthBridgeEvidenceErrorV8(
            "depth-bridge payload names a foreign depth plan"
        )
    _validate_material_against_plan(payload, depth_plan)


def depth_bridge_evidence_census_v8(
    payloads: tuple[DepthBridgeEvidencePayloadV8, ...],
) -> DepthBridgeEvidenceCensusV8:
    """Replay strict lifecycle order and return all open terminal obligations."""

    if type(payloads) is not tuple:
        raise TypeError("payloads must be an exact tuple")
    if any(type(value) is not DepthBridgeEvidencePayloadV8 for value in payloads):
        raise TypeError("payloads contain a non-exact depth-bridge payload")
    replay = _DepthBridgeReplayV8()
    for payload in payloads:
        validate_depth_bridge_evidence_payload_v8(payload)
        replay.apply(payload)
    return replay.census(len(payloads))


def validate_depth_bridge_evidence_order_v8(
    payload: DepthBridgeEvidencePayloadV8,
    *,
    prior: tuple[DepthBridgeEvidencePayloadV8, ...],
) -> None:
    """Fail closed unless appending payload preserves the exact lifecycle."""

    if type(payload) is not DepthBridgeEvidencePayloadV8:
        raise TypeError("payload must be an exact DepthBridgeEvidencePayloadV8")
    depth_bridge_evidence_census_v8((*prior, payload))


@dataclass(slots=True)
class _CycleStateV8:
    cycle: DepthBridgeCycleRefV8
    supersedes_cycle_id: str | None
    terminal_outcome: str | None = None
    last_attempt: int = 0
    attempt_open: bool = False
    last_attempt_classification: str | None = None
    last_failure_code: str | None = None
    semantic_admission_sha256: str | None = None
    target_update_id: int | None = None
    range_summary: DepthBridgeRangeSummaryV8 | None = None
    wait_open: bool = False
    wait_started_monotonic_ns: int | None = None
    wait_deadline_monotonic_ns: int | None = None
    last_wait_outcome: str | None = None


class _DepthBridgeReplayV8:
    def __init__(self) -> None:
        self.session_id: str | None = None
        self.protocol_hash: str | None = None
        self.plan_bundle_sha256: str | None = None
        self.depth_plan_sha256: str | None = None
        self.current_connection_id: str | None = None
        self.current_generation = 0
        self.generation_open = False
        self.last_trigger_seq = 0
        self.cycles: dict[str, _CycleStateV8] = {}
        self.generation_cycle_ids: list[str] = []
        self.generation_started_count = 0
        self.generation_drained_count = 0
        self.fatal_generation_count = 0
        self.last_drain_reason: str | None = None
        self.fatal_terminal = False
        self.trigger_count = 0
        self.failed_cycle_count = 0

    def apply(self, payload: DepthBridgeEvidencePayloadV8) -> None:
        phase = DepthBridgePhaseV8(payload.phase)
        if phase is DepthBridgePhaseV8.GENERATION_STARTED:
            self._start_generation(payload)
            return
        self._require_current_generation(payload)
        if phase is DepthBridgePhaseV8.TRIGGER_REGISTERED:
            self._register_trigger(payload)
        elif phase is DepthBridgePhaseV8.ATTEMPT_STARTED:
            self._start_attempt(payload)
        elif phase is DepthBridgePhaseV8.ATTEMPT_TERMINAL:
            self._terminal_attempt(payload)
        elif phase is DepthBridgePhaseV8.WAIT_TERMINAL:
            self._terminal_wait(payload)
        elif phase is DepthBridgePhaseV8.CYCLE_TERMINAL:
            self._terminal_cycle(payload)
        else:
            assert phase is DepthBridgePhaseV8.GENERATION_DRAINED
            self._drain_generation(payload)

    def _start_generation(self, payload: DepthBridgeEvidencePayloadV8) -> None:
        if self.generation_open:
            raise DepthBridgeEvidenceErrorV8(
                "a new generation cannot start before the prior drain"
            )
        if self.fatal_terminal:
            raise DepthBridgeEvidenceErrorV8(
                "a fatal generation drain is terminal for the integrity ledger"
            )
        if self.current_generation and self.last_drain_reason != "reconnect":
            raise DepthBridgeEvidenceErrorV8(
                "only a reconnect drain may precede a successor generation"
            )
        if payload.connection_generation != self.current_generation + 1:
            raise DepthBridgeEvidenceErrorV8(
                "connection generations must start contiguously at one"
            )
        if self.session_id is None:
            self.session_id = payload.session_id
            self.protocol_hash = payload.protocol_hash
            self.plan_bundle_sha256 = payload.plan_bundle_sha256
            self.depth_plan_sha256 = payload.depth_plan_sha256
        elif (
            payload.session_id != self.session_id
            or payload.protocol_hash != self.protocol_hash
            or payload.plan_bundle_sha256 != self.plan_bundle_sha256
            or payload.depth_plan_sha256 != self.depth_plan_sha256
        ):
            raise DepthBridgeEvidenceErrorV8(
                "depth-bridge lineage changed within one integrity ledger"
            )
        if payload.connection_id == self.current_connection_id:
            raise DepthBridgeEvidenceErrorV8(
                "a new generation requires a new connection ID"
            )
        self.current_connection_id = payload.connection_id
        self.current_generation = payload.connection_generation
        self.generation_open = True
        self.generation_cycle_ids = []
        self.generation_started_count += 1

    def _require_current_generation(self, payload: DepthBridgeEvidencePayloadV8) -> None:
        if not self.generation_open:
            raise DepthBridgeEvidenceErrorV8(
                "depth-bridge phase requires one open generation"
            )
        if (
            payload.session_id != self.session_id
            or payload.protocol_hash != self.protocol_hash
            or payload.plan_bundle_sha256 != self.plan_bundle_sha256
            or payload.depth_plan_sha256 != self.depth_plan_sha256
            or payload.connection_id != self.current_connection_id
            or payload.connection_generation != self.current_generation
        ):
            raise DepthBridgeEvidenceErrorV8(
                "depth-bridge phase differs from its open generation lineage"
            )

    def _register_trigger(self, payload: DepthBridgeEvidencePayloadV8) -> None:
        material = payload.material
        assert isinstance(material, DepthBridgeTriggerRegisteredV8)
        if material.trigger_seq != self.last_trigger_seq + 1:
            raise DepthBridgeEvidenceErrorV8(
                "trigger sequence must be globally contiguous and strictly increasing"
            )
        if material.trigger == "startup" and payload.connection_generation != 1:
            raise DepthBridgeEvidenceErrorV8(
                "startup trigger is valid only in generation one"
            )
        if material.trigger == "reconnect" and payload.connection_generation == 1:
            raise DepthBridgeEvidenceErrorV8(
                "reconnect trigger requires a later generation"
            )
        seen_symbols: set[str] = set()
        for registered in material.cycles:
            cycle = registered.cycle
            if cycle.cycle_id in self.cycles:
                raise DepthBridgeEvidenceErrorV8("cycle ID was already registered")
            if cycle.symbol in seen_symbols:
                raise DepthBridgeEvidenceErrorV8(
                    "one trigger cannot register a symbol twice"
                )
            seen_symbols.add(cycle.symbol)
            open_for_symbol = [
                state
                for state in self.cycles.values()
                if state.cycle.symbol == cycle.symbol
                and state.terminal_outcome is None
                and state.cycle.cycle_id in self.generation_cycle_ids
            ]
            if len(open_for_symbol) > 1:
                raise DepthBridgeEvidenceErrorV8(
                    "a symbol must terminally resolve one retained successor before another trigger"
                )
            expected_superseded = (
                open_for_symbol[0].cycle.cycle_id if open_for_symbol else None
            )
            if registered.supersedes_cycle_id != expected_superseded:
                raise DepthBridgeEvidenceErrorV8(
                    "cycle supersession link differs from the current open cycle"
                )
            prior_first_u = [
                state.cycle.first_buffered_u
                for state in self.cycles.values()
                if state.cycle.symbol == cycle.symbol
                and state.cycle.cycle_id in self.generation_cycle_ids
            ]
            if prior_first_u and cycle.first_buffered_u <= max(prior_first_u):
                raise DepthBridgeEvidenceErrorV8(
                    "first buffered U must increase strictly per symbol and generation"
                )
            self.cycles[cycle.cycle_id] = _CycleStateV8(
                cycle=cycle,
                supersedes_cycle_id=registered.supersedes_cycle_id,
            )
            self.generation_cycle_ids.append(cycle.cycle_id)
        self.last_trigger_seq = material.trigger_seq
        self.trigger_count += 1

    def _cycle_state(self, cycle: DepthBridgeCycleRefV8) -> _CycleStateV8:
        state = self.cycles.get(cycle.cycle_id)
        if state is None or state.cycle != cycle:
            raise DepthBridgeEvidenceErrorV8(
                "depth-bridge phase references an unknown or conflicting cycle"
            )
        if cycle.cycle_id not in self.generation_cycle_ids:
            raise DepthBridgeEvidenceErrorV8(
                "depth-bridge phase references a stale generation cycle"
            )
        if state.terminal_outcome is not None:
            raise DepthBridgeEvidenceErrorV8("cycle already has one terminal outcome")
        return state

    def _start_attempt(self, payload: DepthBridgeEvidencePayloadV8) -> None:
        material = payload.material
        assert isinstance(material, DepthBridgeAttemptStartedV8)
        state = self._cycle_state(material.cycle)
        if state.attempt_open or state.wait_open:
            raise DepthBridgeEvidenceErrorV8(
                "attempt cannot start while an attempt or paired wait is open"
            )
        if material.bridge_attempt != state.last_attempt + 1 or material.bridge_attempt > 3:
            raise DepthBridgeEvidenceErrorV8(
                "bridge attempts must be contiguous and bounded by three"
            )
        if state.last_attempt:
            retryable = (
                state.last_attempt_classification
                == DepthBridgeAttemptClassificationV8.STALE
                or (
                    state.last_attempt_classification
                    == DepthBridgeAttemptClassificationV8.WAITING
                    and state.last_wait_outcome
                    in (
                        DepthBridgeWaitOutcomeV8.STALE,
                        DepthBridgeWaitOutcomeV8.TIMEOUT,
                    )
                )
            )
            if not retryable:
                raise DepthBridgeEvidenceErrorV8(
                    "only stale or timed-out bridge evidence may be retried"
                )
        state.last_attempt = material.bridge_attempt
        state.attempt_open = True
        state.last_attempt_classification = None
        state.last_failure_code = None
        state.semantic_admission_sha256 = None
        state.target_update_id = None
        state.range_summary = None
        state.wait_started_monotonic_ns = None
        state.wait_deadline_monotonic_ns = None
        state.last_wait_outcome = None

    def _terminal_attempt(self, payload: DepthBridgeEvidencePayloadV8) -> None:
        material = payload.material
        assert isinstance(material, DepthBridgeAttemptTerminalV8)
        state = self._cycle_state(material.cycle)
        if not state.attempt_open or material.bridge_attempt != state.last_attempt:
            raise DepthBridgeEvidenceErrorV8(
                "attempt terminal lacks its exact open attempt"
            )
        state.attempt_open = False
        state.last_attempt_classification = material.classification
        state.last_failure_code = material.failure_code
        state.semantic_admission_sha256 = material.semantic_admission_sha256
        state.target_update_id = material.target_update_id
        state.range_summary = material.range_summary
        state.wait_open = (
            material.classification == DepthBridgeAttemptClassificationV8.WAITING
        )
        state.wait_started_monotonic_ns = material.wait_started_monotonic_ns
        state.wait_deadline_monotonic_ns = material.wait_deadline_monotonic_ns

    def _terminal_wait(self, payload: DepthBridgeEvidencePayloadV8) -> None:
        material = payload.material
        assert isinstance(material, DepthBridgeWaitTerminalV8)
        state = self._cycle_state(material.cycle)
        if not state.wait_open or material.bridge_attempt != state.last_attempt:
            raise DepthBridgeEvidenceErrorV8(
                "wait terminal lacks its exact waiting attempt"
            )
        if (
            material.wait_started_monotonic_ns
            != state.wait_started_monotonic_ns
            or material.wait_deadline_monotonic_ns
            != state.wait_deadline_monotonic_ns
            or material.target_update_id != state.target_update_id
        ):
            raise DepthBridgeEvidenceErrorV8(
                "wait terminal differs from its paired waiting attempt"
            )
        state.wait_open = False
        state.last_wait_outcome = material.outcome
        state.target_update_id = material.target_update_id
        state.range_summary = material.range_summary

    def _terminal_cycle(self, payload: DepthBridgeEvidencePayloadV8) -> None:
        material = payload.material
        assert isinstance(material, DepthBridgeCycleTerminalV8)
        state = self._cycle_state(material.cycle)
        if state.attempt_open or state.wait_open:
            raise DepthBridgeEvidenceErrorV8(
                "cycle terminal cannot strand an open attempt or wait"
            )
        if material.terminal_bridge_attempt is not None and (
            material.terminal_bridge_attempt != state.last_attempt
        ):
            raise DepthBridgeEvidenceErrorV8(
                "cycle terminal attempt differs from the latest attempt"
            )
        successor_exists = any(
            candidate.supersedes_cycle_id == state.cycle.cycle_id
            for candidate in self.cycles.values()
        )
        if successor_exists and not (
            material.outcome == DepthBridgeCycleOutcomeV8.SUPERSEDED
            and material.reason == "newer_trigger"
        ):
            raise DepthBridgeEvidenceErrorV8(
                "a registered successor requires a superseded cycle terminal"
            )
        if material.outcome == DepthBridgeCycleOutcomeV8.ACCEPTED:
            accepted = (
                state.last_attempt_classification
                == DepthBridgeAttemptClassificationV8.ACCEPTED
                or state.last_wait_outcome == DepthBridgeWaitOutcomeV8.ACCEPTED
            )
            if not accepted:
                raise DepthBridgeEvidenceErrorV8(
                    "accepted cycle lacks an accepted attempt or paired wait"
                )
            if (
                material.semantic_admission_sha256
                != state.semantic_admission_sha256
                or material.target_update_id != state.target_update_id
                or material.bridging_range_summary != state.range_summary
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "accepted cycle differs from its terminal bridge evidence"
                )
        elif material.outcome == DepthBridgeCycleOutcomeV8.SUPERSEDED:
            if material.reason == "newer_trigger" and not successor_exists:
                raise DepthBridgeEvidenceErrorV8(
                    "newer-trigger terminal lacks its registered successor"
                )
            if (
                material.reason == "generation_draining"
                and state.last_wait_outcome
                not in (None, DepthBridgeWaitOutcomeV8.GENERATION_DRAINING)
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "generation-drain terminal conflicts with its paired wait"
                )
        else:
            exhausted_stale = (
                material.reason == "attempts_exhausted_stale"
                and state.last_attempt == 3
                and state.last_attempt_classification
                == DepthBridgeAttemptClassificationV8.STALE
            )
            exhausted_timeout = (
                material.reason == "attempts_exhausted_timeout"
                and state.last_attempt == 3
                and state.last_wait_outcome == DepthBridgeWaitOutcomeV8.TIMEOUT
            )
            directly_failed = (
                material.reason == state.last_failure_code
                and state.last_attempt_classification
                == DepthBridgeAttemptClassificationV8.FAILED
            )
            exceptional = material.reason in {
                "range_buffer_overflow",
                "owner_stopped_unresolved",
                "coordinator_fatal",
            }
            if not (
                exhausted_stale
                or exhausted_timeout
                or directly_failed
                or exceptional
            ):
                raise DepthBridgeEvidenceErrorV8(
                    "failed cycle lacks terminal attempt or owner evidence"
                )
            self.failed_cycle_count += 1
        state.terminal_outcome = material.outcome

    def _drain_generation(self, payload: DepthBridgeEvidencePayloadV8) -> None:
        material = payload.material
        assert isinstance(material, DepthBridgeGenerationDrainedV8)
        states = [self.cycles[cycle_id] for cycle_id in self.generation_cycle_ids]
        if any(
            state.terminal_outcome is None
            or state.attempt_open
            or state.wait_open
            for state in states
        ):
            raise DepthBridgeEvidenceErrorV8(
                "generation drain cannot strand an open cycle, attempt, or wait"
            )
        expected = (
            len(states),
            sum(
                state.terminal_outcome == DepthBridgeCycleOutcomeV8.ACCEPTED
                for state in states
            ),
            sum(
                state.terminal_outcome == DepthBridgeCycleOutcomeV8.SUPERSEDED
                for state in states
            ),
            sum(
                state.terminal_outcome == DepthBridgeCycleOutcomeV8.FAILED
                for state in states
            ),
        )
        observed = (
            material.registered_cycle_count,
            material.accepted_cycle_count,
            material.superseded_cycle_count,
            material.failed_cycle_count,
        )
        if observed != expected:
            raise DepthBridgeEvidenceErrorV8(
                "generation drain census differs from lifecycle evidence"
            )
        self.generation_open = False
        self.generation_drained_count += 1
        self.last_drain_reason = material.reason
        if material.reason == "fatal":
            self.fatal_generation_count += 1
            self.fatal_terminal = True

    def census(self, event_count: int) -> DepthBridgeEvidenceCensusV8:
        generation_cycle_states = [
            self.cycles[cycle_id] for cycle_id in self.generation_cycle_ids
        ]
        return DepthBridgeEvidenceCensusV8(
            event_count=event_count,
            generation_started_count=self.generation_started_count,
            generation_drained_count=self.generation_drained_count,
            trigger_count=self.trigger_count,
            cycle_count=len(self.cycles),
            failed_cycle_count=self.failed_cycle_count,
            fatal_generation_count=self.fatal_generation_count,
            last_drain_reason=self.last_drain_reason,
            open_generation_count=int(self.generation_open),
            open_cycle_count=sum(
                state.terminal_outcome is None for state in generation_cycle_states
            ),
            open_attempt_count=sum(
                state.attempt_open for state in generation_cycle_states
            ),
            open_wait_count=sum(state.wait_open for state in generation_cycle_states),
        )


def _parse_phase_material(
    phase: DepthBridgePhaseV8,
    document: dict[str, object],
) -> DepthBridgePhaseMaterialV8:
    material_type = _MATERIAL_TYPE_BY_PHASE[phase]
    _require_exact_document_keys(document, material_type)
    converted = dict(document)
    if phase is DepthBridgePhaseV8.TRIGGER_REGISTERED:
        raw_cycles = converted.get("cycles")
        if not isinstance(raw_cycles, (list, tuple)):
            raise TypeError("cycles must be a canonical array or typed tuple")
        converted["cycles"] = tuple(_parse_registered_cycle(value) for value in raw_cycles)
    elif phase in (
        DepthBridgePhaseV8.ATTEMPT_STARTED,
        DepthBridgePhaseV8.ATTEMPT_TERMINAL,
        DepthBridgePhaseV8.WAIT_TERMINAL,
        DepthBridgePhaseV8.CYCLE_TERMINAL,
    ):
        raw_cycle = converted.get("cycle")
        if type(raw_cycle) is not dict:
            raise TypeError("cycle must be a canonical object")
        converted["cycle"] = _parse_exact_dataclass(DepthBridgeCycleRefV8, raw_cycle)
    if phase is DepthBridgePhaseV8.ATTEMPT_TERMINAL:
        raw_rest = converted.get("rest_source")
        if raw_rest is not None:
            if type(raw_rest) is not dict:
                raise TypeError("rest_source must be an object or null")
            converted["rest_source"] = _parse_exact_dataclass(
                DepthBridgeRestSourceLocatorV8,
                raw_rest,
            )
        converted["range_summary"] = _parse_nested_range_summary(converted)
    elif phase is DepthBridgePhaseV8.WAIT_TERMINAL:
        converted["range_summary"] = _parse_nested_range_summary(converted)
    elif phase is DepthBridgePhaseV8.CYCLE_TERMINAL:
        raw_summary = converted.get("bridging_range_summary")
        if raw_summary is not None:
            if type(raw_summary) is not dict:
                raise TypeError("bridging_range_summary must be an object or null")
            converted["bridging_range_summary"] = _parse_exact_dataclass(
                DepthBridgeRangeSummaryV8,
                raw_summary,
            )
    return material_type(**converted)  # type: ignore[call-arg,return-value]


def _parse_registered_cycle(value: object) -> DepthBridgeRegisteredCycleV8:
    if type(value) is not dict:
        raise TypeError("registered cycle must be a canonical object")
    _require_exact_document_keys(value, DepthBridgeRegisteredCycleV8)
    converted = dict(value)
    raw_cycle = converted.get("cycle")
    raw_source = converted.get("initial_range_source")
    if type(raw_cycle) is not dict or type(raw_source) is not dict:
        raise TypeError("registered cycle nested values must be canonical objects")
    converted["cycle"] = _parse_exact_dataclass(DepthBridgeCycleRefV8, raw_cycle)
    converted["initial_range_source"] = _parse_exact_dataclass(
        DepthBridgeWebSocketSourceLocatorV8,
        raw_source,
    )
    return DepthBridgeRegisteredCycleV8(**converted)  # type: ignore[arg-type]


def _parse_nested_range_summary(
    converted: dict[str, object],
) -> DepthBridgeRangeSummaryV8:
    raw_summary = converted.get("range_summary")
    if type(raw_summary) is not dict:
        raise TypeError("range_summary must be a canonical object")
    return _parse_exact_dataclass(DepthBridgeRangeSummaryV8, raw_summary)


def _parse_exact_dataclass[T](value_type: type[T], document: dict[str, object]) -> T:
    _require_exact_document_keys(document, value_type)
    return value_type(**document)  # type: ignore[arg-type]


def _require_exact_document_keys(
    document: dict[str, object],
    value_type: type[Any],
) -> None:
    if type(document) is not dict:
        raise TypeError("canonical evidence value must be an exact object")
    expected = {field.name for field in fields(value_type) if field.init}
    if set(document) != expected:
        raise DepthBridgeEvidenceErrorV8(
            f"{value_type.__name__} fields differ from its exact schema"
        )


def _validate_cycle_references(payload: DepthBridgeEvidencePayloadV8) -> None:
    material = payload.material
    cycles: tuple[DepthBridgeCycleRefV8, ...]
    if isinstance(material, DepthBridgeTriggerRegisteredV8):
        cycles = tuple(value.cycle for value in material.cycles)
    elif isinstance(
        material,
        (
            DepthBridgeAttemptStartedV8,
            DepthBridgeAttemptTerminalV8,
            DepthBridgeWaitTerminalV8,
            DepthBridgeCycleTerminalV8,
        ),
    ):
        cycles = (material.cycle,)
    else:
        cycles = ()
    for cycle in cycles:
        expected = build_depth_bridge_cycle_ref_v8(
            session_id=payload.session_id,
            protocol_hash=payload.protocol_hash,
            plan_bundle_sha256=payload.plan_bundle_sha256,
            depth_plan_sha256=payload.depth_plan_sha256,
            connection_id=payload.connection_id,
            connection_generation=payload.connection_generation,
            symbol=cycle.symbol,
            symbol_ordinal=cycle.symbol_ordinal,
            trigger_seq=cycle.trigger_seq,
            first_buffered_u=cycle.first_buffered_u,
        )
        if cycle != expected:
            raise DepthBridgeEvidenceErrorV8(
                "cycle ID differs from its exact lineage and scheduler identity"
            )


def _validate_material_against_plan(
    payload: DepthBridgeEvidencePayloadV8,
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> None:
    material = payload.material
    if isinstance(material, DepthBridgeGenerationStartedV8):
        expected = (
            len(depth_plan.symbols),
            depth_bridge_symbol_census_sha256_v8(depth_plan.symbols),
            depth_plan.maximum_concurrency,
            DEPTH_BRIDGE_MAXIMUM_BUFFERED_RANGES_PER_SYMBOL_V8,
            depth_plan.bridge_maximum_attempts,
            depth_plan.bridge_wait_timeout_ms,
        )
        observed = (
            material.symbol_count,
            material.symbol_census_sha256,
            material.maximum_concurrency,
            material.maximum_buffered_ranges_per_symbol,
            material.bridge_maximum_attempts,
            material.bridge_wait_timeout_ms,
        )
        if observed != expected:
            raise DepthBridgeEvidenceErrorV8(
                "generation-start material differs from the exact depth plan"
            )
        return
    cycle_refs: tuple[DepthBridgeCycleRefV8, ...]
    if isinstance(material, DepthBridgeTriggerRegisteredV8):
        if material.trigger not in depth_plan.snapshot_triggers:
            raise DepthBridgeEvidenceErrorV8("trigger is outside the exact depth plan")
        cycle_refs = tuple(value.cycle for value in material.cycles)
        if material.trigger in ("startup", "reconnect"):
            observed_symbols = tuple(value.symbol for value in cycle_refs)
            if observed_symbols != depth_plan.symbols:
                raise DepthBridgeEvidenceErrorV8(
                    "startup/reconnect trigger must cover the exact symbol census"
                )
        elif len(cycle_refs) != 1:
            raise DepthBridgeEvidenceErrorV8(
                "sequence-gap trigger must cover exactly one symbol"
            )
    elif isinstance(
        material,
        (
            DepthBridgeAttemptStartedV8,
            DepthBridgeAttemptTerminalV8,
            DepthBridgeWaitTerminalV8,
            DepthBridgeCycleTerminalV8,
        ),
    ):
        cycle_refs = (material.cycle,)
    else:
        cycle_refs = ()
    for cycle in cycle_refs:
        if (
            cycle.symbol_ordinal >= len(depth_plan.symbols)
            or depth_plan.symbols[cycle.symbol_ordinal] != cycle.symbol
        ):
            raise DepthBridgeEvidenceErrorV8(
                "cycle symbol or ordinal differs from the exact depth plan"
            )
    if isinstance(
        material,
        (DepthBridgeAttemptStartedV8, DepthBridgeAttemptTerminalV8, DepthBridgeWaitTerminalV8),
    ) and material.bridge_attempt > depth_plan.bridge_maximum_attempts:
        raise DepthBridgeEvidenceErrorV8(
            "bridge attempt exceeds the exact depth plan"
        )
    if isinstance(material, DepthBridgeAttemptTerminalV8) and (
        material.classification == DepthBridgeAttemptClassificationV8.WAITING
    ):
        assert material.wait_started_monotonic_ns is not None
        assert material.wait_deadline_monotonic_ns is not None
        if (
            material.wait_deadline_monotonic_ns
            - material.wait_started_monotonic_ns
            != depth_plan.bridge_wait_timeout_ms * 1_000_000
        ):
            raise DepthBridgeEvidenceErrorV8(
                "bridge wait interval differs from the exact depth plan"
            )


def _validate_exact_plan_pair(
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> None:
    if type(promoting_plans) is not tuple:
        raise TypeError("promoting_plans must be the exact immutable v8 tuple")
    validate_provisional_promoting_capture_plans_v8(promoting_plans)
    validate_public_depth_rest_plan_v8(depth_plan)
    matching = tuple(
        plan
        for plan in promoting_plans
        if type(plan) is ProvisionalDepthRestQualificationPlanV8
    )
    if len(matching) != 1 or matching[0] is not depth_plan:
        raise DepthBridgeEvidenceErrorV8(
            "depth_plan must be the exact member of the v8 plan tuple"
        )


def _validate_depth_bridge_coordinator_close_material_v8(
    value: (
        DepthBridgeCoordinatorCleanCloseReceiptV8
        | DepthBridgeCoordinatorClosureEntryV8
    ),
) -> None:
    if type(value) not in (
        DepthBridgeCoordinatorCleanCloseReceiptV8,
        DepthBridgeCoordinatorClosureEntryV8,
    ):
        raise TypeError("depth bridge close material has a foreign exact type")
    _require_identity(value.session_id, "session_id")
    _require_sha256(value.protocol_hash, "protocol_hash")
    _require_sha256(value.plan_bundle_sha256, "plan_bundle_sha256")
    _require_sha256(value.depth_plan_sha256, "depth_plan_sha256")
    if type(value.plan_count) is not int or value.plan_count != 4:
        raise DepthBridgeEvidenceErrorV8(
            "depth bridge close material requires exactly four plans"
        )
    _require_identity(value.last_connection_id, "last_connection_id")
    _require_positive_int(
        value.last_connection_generation,
        "last_connection_generation",
    )
    _require_positive_int(
        value.generation_started_count,
        "generation_started_count",
    )
    _require_positive_int(
        value.generation_drained_count,
        "generation_drained_count",
    )
    _require_nonnegative_int(
        value.fatal_generation_count,
        "fatal_generation_count",
    )
    if (
        value.generation_started_count != value.generation_drained_count
        or value.fatal_generation_count != 0
    ):
        raise DepthBridgeEvidenceErrorV8(
            "clean bridge close requires every generation drained and no fatal generation"
        )
    if value.close_reason != "normal_stop":
        raise DepthBridgeEvidenceErrorV8(
            "clean bridge close requires a final normal_stop drain"
        )
    _require_positive_int(
        value.last_generation_drained_event_sequence,
        "last_generation_drained_event_sequence",
    )
    _require_sha256(
        value.last_generation_drained_event_sha256,
        "last_generation_drained_event_sha256",
    )
    for name in (
        "last_generation_drained_recorded_wall_ms",
        "last_generation_drained_recorded_monotonic_ns",
        "close_wall_ms",
        "close_monotonic_ns",
    ):
        _require_nonnegative_int(getattr(value, name), name)
    if (
        value.close_monotonic_ns
        < value.last_generation_drained_recorded_monotonic_ns
    ):
        raise DepthBridgeEvidenceErrorV8(
            "bridge close monotonic clock precedes its persisted generation drain"
        )
    for name in (
        "worker_count",
        "permit_in_use_count",
        "retained_registration_count",
        "pending_registration_count",
        "retained_token_count",
        "claimed_token_count",
        "adapter_active_attempt_count",
        "adapter_pending_owner_task_count",
        "retained_terminal_admission_count",
    ):
        observed = getattr(value, name)
        if type(observed) is not int or observed != 0:
            raise DepthBridgeEvidenceErrorV8(
                f"clean bridge close requires zero {name}"
            )
    exact_flags = (
        (value.coordinator_closed, True, "coordinator_closed"),
        (value.generation_open, False, "generation_open"),
        (value.callbacks_accepting, False, "callbacks_accepting"),
        (
            value.scheduler_generation_open,
            False,
            "scheduler_generation_open",
        ),
        (value.adapter_attached, False, "adapter_attached"),
        (value.qualification_only, True, "qualification_only"),
        (
            value.capture_finality_verified,
            False,
            "capture_finality_verified",
        ),
        (value.m2_certified, False, "m2_certified"),
        (value.session_clean_claimed, False, "session_clean_claimed"),
        (value.order_execution_enabled, False, "order_execution_enabled"),
    )
    for observed, expected, name in exact_flags:
        if observed is not expected:
            raise DepthBridgeEvidenceErrorV8(
                f"depth bridge close material has invalid {name}"
            )
    expected_schema = (
        _COORDINATOR_CLEAN_CLOSE_RECEIPT_SCHEMA
        if type(value) is DepthBridgeCoordinatorCleanCloseReceiptV8
        else _COORDINATOR_CLOSURE_ENTRY_SCHEMA
    )
    if value.schema_version != expected_schema:
        raise DepthBridgeEvidenceErrorV8(
            "unsupported depth bridge coordinator close schema"
        )


def _depth_bridge_coordinator_clean_close_receipt_sha256_v8(
    receipt: DepthBridgeCoordinatorCleanCloseReceiptV8,
) -> str:
    material = {
        name: getattr(receipt, name)
        for name in _COORDINATOR_SHARED_CLOSE_FIELD_NAMES_V8
    }
    material["schema_version"] = receipt.schema_version
    return hashlib.sha256(
        _COORDINATOR_CLEAN_CLOSE_RECEIPT_DOMAIN + canonical_json_line(material)
    ).hexdigest()


def _cycle_identity_document(
    *,
    session_id: str,
    protocol_hash: str,
    plan_bundle_sha256: str,
    depth_plan_sha256: str,
    connection_id: str,
    connection_generation: int,
    symbol: str,
    symbol_ordinal: int,
    trigger_seq: int,
    first_buffered_u: int,
) -> dict[str, object]:
    _require_identity(session_id, "session_id")
    _require_sha256(protocol_hash, "protocol_hash")
    _require_sha256(plan_bundle_sha256, "plan_bundle_sha256")
    _require_sha256(depth_plan_sha256, "depth_plan_sha256")
    _require_identity(connection_id, "connection_id")
    _require_positive_int(connection_generation, "connection_generation")
    _require_symbol(symbol)
    _require_nonnegative_int(symbol_ordinal, "symbol_ordinal")
    _require_positive_int(trigger_seq, "trigger_seq")
    _require_nonnegative_int(first_buffered_u, "first_buffered_u")
    return {
        "schema_version": "r4b_v2_depth_bridge_cycle_identity_v8",
        "session_id": session_id,
        "protocol_hash": protocol_hash,
        "plan_bundle_sha256": plan_bundle_sha256,
        "depth_plan_sha256": depth_plan_sha256,
        "connection_id": connection_id,
        "connection_generation": connection_generation,
        "symbol": symbol,
        "symbol_ordinal": symbol_ordinal,
        "trigger_seq": trigger_seq,
        "first_buffered_u": first_buffered_u,
    }


def _require_identity(value: object, name: str) -> None:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > _MAX_IDENTITY_LENGTH
    ):
        raise DepthBridgeEvidenceErrorV8(f"{name} is not a bounded identity")


def _require_symbol(value: object) -> None:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or value != value.upper()
        or not value.isascii()
        or not value.isalnum()
        or len(value) > _MAX_IDENTITY_LENGTH
    ):
        raise DepthBridgeEvidenceErrorV8(
            "symbol must be a normalized uppercase identifier"
        )


def _require_sha256(value: object, name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise DepthBridgeEvidenceErrorV8(f"{name} must be lowercase SHA-256")


def _require_optional_sha256(value: object, name: str) -> None:
    if value is not None:
        _require_sha256(value, name)


def _require_nonnegative_int(value: object, name: str) -> None:
    if type(value) is not int or not 0 <= value <= _MAX_SIGNED_INT64:
        raise DepthBridgeEvidenceErrorV8(
            f"{name} must be a nonnegative signed-int64 integer"
        )


def _require_positive_int(value: object, name: str) -> None:
    if type(value) is not int or not 1 <= value <= _MAX_SIGNED_INT64:
        raise DepthBridgeEvidenceErrorV8(
            f"{name} must be a positive signed-int64 integer"
        )
