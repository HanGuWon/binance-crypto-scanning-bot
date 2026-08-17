"""NONAUTHORITATIVE V1 serialization prototype for prospective decision cells.

This journal records exactly one decision for every admitted cell and, only for
signal cells, at most one execution-observation terminal.  It deliberately does
not call PAPER FOK, compute PnL, access a network, or authorize an order.  It is
not an efficacy ledger: it lacks a frozen universe/time census, an OS writer
lease, factory-owned decision admission, and first-post-target capture proof.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import threading
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import BinaryIO, Final, Literal, cast

from signalbot.capture.path_safety import inspect_link_free_path
from signalbot.r4b_v2.alerts.actionability import (
    PRIMARY_PAPER_TARGET_DELAY_MS_V2,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.decision_clock import DECISION_DELAY_MS_V2

type ProspectiveDecisionV1 = Literal[
    "SIGNAL_LONG",
    "SIGNAL_SHORT",
    "NO_SIGNAL",
]
type ProspectiveExecutionOutcomeV1 = Literal[
    "FULL_FILL",
    "NO_FILL",
    "INCONCLUSIVE",
]
type ProspectiveEvidenceStateV1 = Literal["VALID", "MISSING", "GAP", "STALE"]
type ProspectiveLedgerRecordTypeV1 = Literal["DECISION_CELL", "EXECUTION_TERMINAL"]
type ProspectiveCellKeyV1 = tuple[str, str, str, str, int]
type ProspectiveAttemptLedgerFaultHookV1 = Callable[[str], None]

PROSPECTIVE_ATTEMPT_LEDGER_AUTHORITY_V1: Final = (
    "NONAUTHORITATIVE_SERIALIZATION_PROTOTYPE"
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,30}$")
_MAX_IDENTITY_LENGTH = 256
_MAX_REASON_LENGTH = 1_024
_MAX_LOCATOR_LENGTH = 2_048
_MAX_CELLS = 1_000_000
_FIVE_MINUTES_MS = 300_000
_JCS_MAX_SAFE_INTEGER = 9_007_199_254_740_991
_DECISION_SCHEMA = "r4b_v2_prospective_decision_cell_v1"
_TERMINAL_SCHEMA = "r4b_v2_prospective_execution_terminal_v1"
_RECORD_SCHEMA = "r4b_v2_prospective_attempt_ledger_record_v1"
_CHECKPOINT_SCHEMA = "r4b_v2_prospective_attempt_ledger_checkpoint_v1"
_CELL_ID_DOMAIN = b"R4B_V2_PROSPECTIVE_DECISION_CELL_ID_V1\0"
_DECISION_HASH_DOMAIN = b"R4B_V2_PROSPECTIVE_DECISION_HASH_V1\0"
_TERMINAL_ID_DOMAIN = b"R4B_V2_PROSPECTIVE_EXECUTION_TERMINAL_ID_V1\0"
_TERMINAL_HASH_DOMAIN = b"R4B_V2_PROSPECTIVE_EXECUTION_TERMINAL_HASH_V1\0"
_RECORD_HASH_DOMAIN = b"R4B_V2_PROSPECTIVE_ATTEMPT_LEDGER_RECORD_V1\0"
_JOURNAL_ROOT_DOMAIN = b"R4B_V2_PROSPECTIVE_ATTEMPT_LEDGER_ROOT_V1\0"
_CHECKPOINT_HASH_DOMAIN = b"R4B_V2_PROSPECTIVE_ATTEMPT_LEDGER_CHECKPOINT_V1\0"
_DECISIONS = frozenset({"SIGNAL_LONG", "SIGNAL_SHORT", "NO_SIGNAL"})
_SIGNALS = frozenset({"SIGNAL_LONG", "SIGNAL_SHORT"})
_OUTCOMES = frozenset({"FULL_FILL", "NO_FILL", "INCONCLUSIVE"})
_EVIDENCE_STATES = frozenset({"VALID", "MISSING", "GAP", "STALE"})


class ProspectiveAttemptLedgerErrorV1(RuntimeError):
    """Base failure for prospective attempt journal admission or recovery."""


class ProspectiveAttemptLedgerIntegrityErrorV1(ProspectiveAttemptLedgerErrorV1):
    """Raised when durable journal bytes fail exact replay."""


class ProspectiveAttemptLedgerConflictErrorV1(ProspectiveAttemptLedgerErrorV1):
    """Raised when an existing cell or terminal is replayed with different facts."""


class ProspectiveAttemptLedgerCapacityErrorV1(ProspectiveAttemptLedgerErrorV1):
    """Raised before a new decision would exceed the fixed in-memory bound."""


class ProspectiveAttemptLedgerWriteErrorV1(ProspectiveAttemptLedgerErrorV1):
    """Raised when append, flush, or fsync does not complete exactly."""


@dataclass(frozen=True, slots=True)
class ProspectiveDecisionCellV1:
    attempt_id: str
    rule_version: str
    universe_version: str
    symbol: str
    closed_5m_close_ms: int
    decision_cutoff_ms: int
    decision: ProspectiveDecisionV1
    source_root_sha256: str
    execution_contract_sha256: str
    decision_event_id: str | None
    payload_sha256: str | None
    cell_id: str
    decision_sha256: str
    paper_fok_invoked: Literal[False]
    pnl_computed: Literal[False]
    network_accessed: Literal[False]
    order_execution_enabled: Literal[False]
    schema_version: Literal["r4b_v2_prospective_decision_cell_v1"] = _DECISION_SCHEMA

    def __post_init__(self) -> None:
        _validate_cell_key(self.cell_key)
        _require_nonnegative_int(self.decision_cutoff_ms, "decision_cutoff_ms")
        if self.decision_cutoff_ms != (
            self.closed_5m_close_ms + DECISION_DELAY_MS_V2
        ):
            raise ValueError("decision_cutoff_ms must equal T+2001ms")
        if self.decision not in _DECISIONS:
            raise ValueError("unsupported prospective decision")
        _require_sha256(self.source_root_sha256, "source_root_sha256")
        _require_sha256(
            self.execution_contract_sha256,
            "execution_contract_sha256",
        )
        if self.decision in _SIGNALS:
            _require_identity(self.decision_event_id, "decision_event_id")
            _require_sha256(self.payload_sha256, "payload_sha256")
        elif self.decision_event_id is not None or self.payload_sha256 is not None:
            raise ValueError("NO_SIGNAL forbids decision_event_id and payload_sha256")
        _require_sha256(self.cell_id, "cell_id")
        if self.cell_id != _cell_id_v1(self.cell_key):
            raise ValueError("prospective cell_id differs from its exact cell key")
        _require_sha256(self.decision_sha256, "decision_sha256")
        if self.decision_sha256 != _decision_sha256_v1(self):
            raise ValueError("prospective decision hash differs from its exact payload")
        _require_nonclaims(
            self.paper_fok_invoked,
            self.pnl_computed,
            self.network_accessed,
            self.order_execution_enabled,
        )
        if self.schema_version != _DECISION_SCHEMA:
            raise ValueError("unsupported prospective decision schema")

    @property
    def cell_key(self) -> ProspectiveCellKeyV1:
        return (
            self.attempt_id,
            self.rule_version,
            self.universe_version,
            self.symbol,
            self.closed_5m_close_ms,
        )

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(asdict(self))


@dataclass(frozen=True, slots=True)
class ProspectiveExecutionTerminalV1:
    attempt_id: str
    rule_version: str
    universe_version: str
    symbol: str
    closed_5m_close_ms: int
    cell_id: str
    decision_sha256: str
    decision_cutoff_ms: int
    target_ms: int
    outcome: ProspectiveExecutionOutcomeV1
    evidence_state: ProspectiveEvidenceStateV1
    evidence_locator: str | None
    evidence_root_sha256: str | None
    evidence_exchange_event_ms: int | None
    evidence_receipt_wall_ms: int | None
    evidence_receipt_monotonic_ns: int | None
    evidence_evaluation_wall_ms: int | None
    evidence_age_ms: int | None
    freshness_limit_ms: int
    reason: str
    terminal_id: str
    terminal_sha256: str
    paper_fok_invoked: Literal[False]
    pnl_computed: Literal[False]
    network_accessed: Literal[False]
    order_execution_enabled: Literal[False]
    schema_version: Literal["r4b_v2_prospective_execution_terminal_v1"] = (
        _TERMINAL_SCHEMA
    )

    def __post_init__(self) -> None:
        _validate_cell_key(self.cell_key)
        _require_sha256(self.cell_id, "cell_id")
        if self.cell_id != _cell_id_v1(self.cell_key):
            raise ValueError("execution terminal cell_id differs from its exact key")
        _require_sha256(self.decision_sha256, "decision_sha256")
        _require_nonnegative_int(self.decision_cutoff_ms, "decision_cutoff_ms")
        if self.decision_cutoff_ms != (
            self.closed_5m_close_ms + DECISION_DELAY_MS_V2
        ):
            raise ValueError("terminal decision_cutoff_ms must equal T+2001ms")
        _require_nonnegative_int(self.target_ms, "target_ms")
        if (
            self.target_ms
            != self.decision_cutoff_ms + PRIMARY_PAPER_TARGET_DELAY_MS_V2
        ):
            raise ValueError("execution target_ms must equal D+10000ms")
        if self.outcome not in _OUTCOMES:
            raise ValueError("unsupported prospective execution outcome")
        if self.evidence_state not in _EVIDENCE_STATES:
            raise ValueError("unsupported prospective evidence state")
        _require_positive_int(self.freshness_limit_ms, "freshness_limit_ms")
        _require_text(self.reason, "reason", maximum=_MAX_REASON_LENGTH)
        self._validate_evidence()
        _require_sha256(self.terminal_id, "terminal_id")
        if self.terminal_id != _terminal_id_v1(self.cell_id, self.target_ms):
            raise ValueError("execution terminal_id differs from its exact identity")
        _require_sha256(self.terminal_sha256, "terminal_sha256")
        if self.terminal_sha256 != _terminal_sha256_v1(self):
            raise ValueError("execution terminal hash differs from its exact payload")
        _require_nonclaims(
            self.paper_fok_invoked,
            self.pnl_computed,
            self.network_accessed,
            self.order_execution_enabled,
        )
        if self.schema_version != _TERMINAL_SCHEMA:
            raise ValueError("unsupported prospective execution terminal schema")

    @property
    def cell_key(self) -> ProspectiveCellKeyV1:
        return (
            self.attempt_id,
            self.rule_version,
            self.universe_version,
            self.symbol,
            self.closed_5m_close_ms,
        )

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(asdict(self))

    def _validate_evidence(self) -> None:
        timed_evidence_values = (
            self.evidence_exchange_event_ms,
            self.evidence_receipt_wall_ms,
            self.evidence_receipt_monotonic_ns,
            self.evidence_evaluation_wall_ms,
            self.evidence_age_ms,
        )
        if self.evidence_state == "MISSING":
            if (
                self.evidence_locator is not None
                or self.evidence_root_sha256 is not None
                or any(value is not None for value in timed_evidence_values)
            ):
                raise ValueError(
                    "MISSING forbids an evidence projection"
                )
            if self.outcome != "INCONCLUSIVE":
                raise ValueError("MISSING evidence forces INCONCLUSIVE")
            return
        if self.evidence_state == "GAP":
            if any(value is not None for value in timed_evidence_values):
                raise ValueError("GAP forbids exchange/receipt freshness clocks")
            if (self.evidence_locator is None) != (self.evidence_root_sha256 is None):
                raise ValueError("GAP evidence locator and root must be supplied together")
            if self.evidence_locator is not None:
                _require_text(
                    self.evidence_locator,
                    "evidence_locator",
                    maximum=_MAX_LOCATOR_LENGTH,
                )
                _require_sha256(self.evidence_root_sha256, "evidence_root_sha256")
            if self.outcome != "INCONCLUSIVE":
                raise ValueError("GAP evidence forces INCONCLUSIVE")
            return
        _require_text(
            self.evidence_locator,
            "evidence_locator",
            maximum=_MAX_LOCATOR_LENGTH,
        )
        _require_sha256(
            self.evidence_root_sha256,
            "evidence_root_sha256",
        )
        _require_nonnegative_int(
            self.evidence_exchange_event_ms,
            "evidence_exchange_event_ms",
        )
        _require_nonnegative_int(
            self.evidence_receipt_wall_ms,
            "evidence_receipt_wall_ms",
        )
        _require_nonnegative_int(
            self.evidence_receipt_monotonic_ns,
            "evidence_receipt_monotonic_ns",
        )
        _require_nonnegative_int(
            self.evidence_evaluation_wall_ms,
            "evidence_evaluation_wall_ms",
        )
        _require_nonnegative_int(self.evidence_age_ms, "evidence_age_ms")
        assert self.evidence_exchange_event_ms is not None
        assert self.evidence_receipt_wall_ms is not None
        assert self.evidence_evaluation_wall_ms is not None
        assert self.evidence_age_ms is not None
        if self.evidence_exchange_event_ms < self.target_ms:
            raise ValueError("first evidence exchange event precedes the execution target")
        if not (
            self.evidence_exchange_event_ms
            <= self.evidence_receipt_wall_ms
            <= self.evidence_evaluation_wall_ms
        ):
            raise ValueError("evidence exchange/receipt/evaluation wall clocks are reordered")
        if self.evidence_age_ms != (
            self.evidence_evaluation_wall_ms - self.evidence_exchange_event_ms
        ):
            raise ValueError(
                "evidence_age_ms differs from evaluation wall minus exchange event"
            )
        if self.evidence_state == "VALID":
            if self.evidence_age_ms > self.freshness_limit_ms:
                raise ValueError("VALID evidence exceeds the frozen freshness limit")
            if self.outcome not in {"FULL_FILL", "NO_FILL"}:
                raise ValueError("VALID evidence requires FULL_FILL or NO_FILL")
            return
        if self.evidence_state != "STALE":
            raise AssertionError("validated evidence state is unreachable")
        if self.evidence_age_ms <= self.freshness_limit_ms:
            raise ValueError("STALE evidence does not exceed the freshness limit")
        if self.outcome != "INCONCLUSIVE":
            raise ValueError("STALE evidence forces INCONCLUSIVE")


type ProspectiveLedgerPayloadV1 = (
    ProspectiveDecisionCellV1 | ProspectiveExecutionTerminalV1
)


@dataclass(frozen=True, slots=True)
class ProspectiveAttemptLedgerRecordV1:
    sequence: int
    record_type: ProspectiveLedgerRecordTypeV1
    previous_record_sha256: str | None
    payload: ProspectiveLedgerPayloadV1
    record_sha256: str
    schema_version: Literal["r4b_v2_prospective_attempt_ledger_record_v1"] = (
        _RECORD_SCHEMA
    )

    def __post_init__(self) -> None:
        _require_positive_int(self.sequence, "sequence")
        if self.record_type not in {"DECISION_CELL", "EXECUTION_TERMINAL"}:
            raise ValueError("unsupported prospective ledger record type")
        if self.sequence == 1:
            if self.previous_record_sha256 is not None:
                raise ValueError("first prospective ledger record forbids a previous hash")
        else:
            _require_sha256(
                self.previous_record_sha256,
                "previous_record_sha256",
            )
        expected_payload_type = (
            ProspectiveDecisionCellV1
            if self.record_type == "DECISION_CELL"
            else ProspectiveExecutionTerminalV1
        )
        if type(self.payload) is not expected_payload_type:
            raise TypeError("prospective ledger record payload has a foreign exact type")
        self.payload.__post_init__()
        _require_sha256(self.record_sha256, "record_sha256")
        if self.record_sha256 != _record_sha256_v1(self):
            raise ValueError("prospective ledger record hash differs")
        if self.schema_version != _RECORD_SCHEMA:
            raise ValueError("unsupported prospective ledger record schema")

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(_record_document_v1(self, include_hash=True))


@dataclass(frozen=True, slots=True)
class ProspectiveAttemptLedgerCheckpointV1:
    """Immutable external pin required to trust a nonempty journal on reopen."""

    canonical_path: str
    journal_identity: str
    record_count: int
    byte_count: int
    tip_sha256: str
    journal_root_sha256: str
    checkpoint_sha256: str
    schema_version: Literal[
        "r4b_v2_prospective_attempt_ledger_checkpoint_v1"
    ] = _CHECKPOINT_SCHEMA

    def __post_init__(self) -> None:
        _require_text(
            self.canonical_path,
            "canonical_path",
            maximum=_MAX_LOCATOR_LENGTH,
        )
        if not Path(self.canonical_path).is_absolute():
            raise ValueError("checkpoint canonical_path must be absolute")
        _require_identity(self.journal_identity, "journal_identity")
        _require_positive_int(self.record_count, "record_count")
        _require_positive_int(self.byte_count, "byte_count")
        _require_sha256(self.tip_sha256, "tip_sha256")
        _require_sha256(self.journal_root_sha256, "journal_root_sha256")
        _require_sha256(self.checkpoint_sha256, "checkpoint_sha256")
        if self.checkpoint_sha256 != _checkpoint_sha256_v1(self):
            raise ValueError("prospective journal checkpoint hash differs")
        if self.schema_version != _CHECKPOINT_SCHEMA:
            raise ValueError("unsupported prospective journal checkpoint schema")


class ProspectiveAttemptLedgerV1:
    """Single-owner append journal with bounded replay state and exact idempotency."""

    def __init__(
        self,
        journal_path: str | Path,
        *,
        max_cells: int,
        expected_checkpoint: ProspectiveAttemptLedgerCheckpointV1 | None = None,
        fault_hook: ProspectiveAttemptLedgerFaultHookV1 | None = None,
    ) -> None:
        if type(max_cells) is not int or not 1 <= max_cells <= _MAX_CELLS:
            raise ValueError(f"max_cells must be within [1, {_MAX_CELLS}]")
        if fault_hook is not None and not callable(fault_hook):
            raise TypeError("fault_hook must be callable")
        if (
            expected_checkpoint is not None
            and type(expected_checkpoint) is not ProspectiveAttemptLedgerCheckpointV1
        ):
            raise TypeError(
                "expected_checkpoint must be an exact prospective checkpoint"
            )
        if expected_checkpoint is not None:
            expected_checkpoint.__post_init__()
        self._journal_path = _inspect_journal_location(journal_path)
        self._max_cells = max_cells
        self._fault_hook = fault_hook
        self._lock = threading.RLock()
        self._poisoned = False
        self._records: list[ProspectiveAttemptLedgerRecordV1] = []
        self._decisions: dict[ProspectiveCellKeyV1, ProspectiveDecisionCellV1] = {}
        self._terminals: dict[
            ProspectiveCellKeyV1,
            ProspectiveExecutionTerminalV1,
        ] = {}
        self._file_identity: tuple[int, int] | None = None
        self._load_current_journal(expected_checkpoint=expected_checkpoint)

    @property
    def journal_path(self) -> Path:
        return self._journal_path

    @property
    def max_cells(self) -> int:
        return self._max_cells

    @property
    def cell_count(self) -> int:
        return len(self._decisions)

    @property
    def record_count(self) -> int:
        return len(self._records)

    @property
    def execution_terminal_count(self) -> int:
        return len(self._terminals)

    @property
    def pending_signal_count(self) -> int:
        return sum(
            decision.decision in _SIGNALS and key not in self._terminals
            for key, decision in self._decisions.items()
        )

    @property
    def tip_sha256(self) -> str | None:
        return self._records[-1].record_sha256 if self._records else None

    @property
    def checkpoint(self) -> ProspectiveAttemptLedgerCheckpointV1 | None:
        """Return the current immutable truncation-detection authority."""

        with self._lock:
            if not self._records:
                return None
            status = _inspect_existing_journal(
                self._journal_path,
                allow_missing=False,
            )
            assert status is not None
            identity = (int(status.st_dev), int(status.st_ino))
            if self._file_identity != identity:
                raise ProspectiveAttemptLedgerIntegrityErrorV1(
                    "prospective journal identity changed before checkpoint"
                )
            expected_bytes = sum(len(record.encoded_line) for record in self._records)
            if int(status.st_size) != expected_bytes:
                raise ProspectiveAttemptLedgerIntegrityErrorV1(
                    "prospective journal size differs from admitted records"
                )
            return _checkpoint_v1(
                path=self._journal_path,
                status=status,
                records=self._records,
            )

    @property
    def records(self) -> tuple[ProspectiveAttemptLedgerRecordV1, ...]:
        return tuple(self._records)

    @property
    def decisions(self) -> tuple[ProspectiveDecisionCellV1, ...]:
        return tuple(self._decisions.values())

    @property
    def terminals(self) -> tuple[ProspectiveExecutionTerminalV1, ...]:
        return tuple(self._terminals.values())

    def decision_for(
        self,
        *,
        attempt_id: str,
        rule_version: str,
        universe_version: str,
        symbol: str,
        closed_5m_close_ms: int,
    ) -> ProspectiveDecisionCellV1 | None:
        key = _cell_key_v1(
            attempt_id,
            rule_version,
            universe_version,
            symbol,
            closed_5m_close_ms,
        )
        return self._decisions.get(key)

    def terminal_for(
        self,
        *,
        attempt_id: str,
        rule_version: str,
        universe_version: str,
        symbol: str,
        closed_5m_close_ms: int,
    ) -> ProspectiveExecutionTerminalV1 | None:
        key = _cell_key_v1(
            attempt_id,
            rule_version,
            universe_version,
            symbol,
            closed_5m_close_ms,
        )
        return self._terminals.get(key)

    def record_decision(
        self,
        *,
        attempt_id: str,
        rule_version: str,
        universe_version: str,
        symbol: str,
        closed_5m_close_ms: int,
        decision_cutoff_ms: int,
        decision: ProspectiveDecisionV1,
        source_root_sha256: str,
        execution_contract_sha256: str,
        decision_event_id: str | None = None,
        payload_sha256: str | None = None,
    ) -> ProspectiveDecisionCellV1:
        candidate = prospective_decision_cell_v1(
            attempt_id=attempt_id,
            rule_version=rule_version,
            universe_version=universe_version,
            symbol=symbol,
            closed_5m_close_ms=closed_5m_close_ms,
            decision_cutoff_ms=decision_cutoff_ms,
            decision=decision,
            source_root_sha256=source_root_sha256,
            execution_contract_sha256=execution_contract_sha256,
            decision_event_id=decision_event_id,
            payload_sha256=payload_sha256,
        )
        with self._lock:
            self._assert_writable()
            existing = self._decisions.get(candidate.cell_key)
            if existing is not None:
                if existing == candidate:
                    return existing
                raise ProspectiveAttemptLedgerConflictErrorV1(
                    "prospective decision cell already exists with different facts"
                )
            if len(self._decisions) >= self._max_cells:
                raise ProspectiveAttemptLedgerCapacityErrorV1(
                    "prospective decision cell bound is exhausted"
                )
            record = _new_record_v1(
                sequence=len(self._records) + 1,
                previous_record_sha256=self.tip_sha256,
                payload=candidate,
            )
            self._append_durable(record)
            self._records.append(record)
            self._decisions[candidate.cell_key] = candidate
            return candidate

    def record_execution_terminal(
        self,
        *,
        attempt_id: str,
        rule_version: str,
        universe_version: str,
        symbol: str,
        closed_5m_close_ms: int,
        target_ms: int,
        outcome: ProspectiveExecutionOutcomeV1,
        evidence_state: ProspectiveEvidenceStateV1,
        evidence_locator: str | None,
        evidence_root_sha256: str | None,
        evidence_exchange_event_ms: int | None,
        evidence_receipt_wall_ms: int | None,
        evidence_receipt_monotonic_ns: int | None,
        evidence_evaluation_wall_ms: int | None,
        evidence_age_ms: int | None,
        freshness_limit_ms: int,
        reason: str,
    ) -> ProspectiveExecutionTerminalV1:
        key = _cell_key_v1(
            attempt_id,
            rule_version,
            universe_version,
            symbol,
            closed_5m_close_ms,
        )
        with self._lock:
            self._assert_writable()
            decision = self._decisions.get(key)
            if decision is None:
                raise ProspectiveAttemptLedgerConflictErrorV1(
                    "execution terminal requires its prior exact decision cell"
                )
            if decision.decision == "NO_SIGNAL":
                raise ProspectiveAttemptLedgerConflictErrorV1(
                    "NO_SIGNAL decision cells forbid execution terminals"
                )
            candidate = prospective_execution_terminal_v1(
                decision,
                target_ms=target_ms,
                outcome=outcome,
                evidence_state=evidence_state,
                evidence_locator=evidence_locator,
                evidence_root_sha256=evidence_root_sha256,
                evidence_exchange_event_ms=evidence_exchange_event_ms,
                evidence_receipt_wall_ms=evidence_receipt_wall_ms,
                evidence_receipt_monotonic_ns=evidence_receipt_monotonic_ns,
                evidence_evaluation_wall_ms=evidence_evaluation_wall_ms,
                evidence_age_ms=evidence_age_ms,
                freshness_limit_ms=freshness_limit_ms,
                reason=reason,
            )
            existing = self._terminals.get(key)
            if existing is not None:
                if existing == candidate:
                    return existing
                raise ProspectiveAttemptLedgerConflictErrorV1(
                    "prospective execution terminal already exists with different facts"
                )
            record = _new_record_v1(
                sequence=len(self._records) + 1,
                previous_record_sha256=self.tip_sha256,
                payload=candidate,
            )
            self._append_durable(record)
            self._records.append(record)
            self._terminals[key] = candidate
            return candidate

    def _assert_writable(self) -> None:
        if self._poisoned:
            raise ProspectiveAttemptLedgerWriteErrorV1(
                "prospective attempt ledger is poisoned after a failed append"
            )

    def _load_current_journal(
        self,
        *,
        expected_checkpoint: ProspectiveAttemptLedgerCheckpointV1 | None,
    ) -> None:
        status = _inspect_existing_journal(self._journal_path, allow_missing=True)
        if status is None:
            if expected_checkpoint is not None:
                raise ProspectiveAttemptLedgerIntegrityErrorV1(
                    "checkpoint was supplied for a missing prospective journal"
                )
            return
        if int(status.st_size) == 0:
            raise ProspectiveAttemptLedgerIntegrityErrorV1(
                "existing prospective journal is empty or truncated"
            )
        if expected_checkpoint is None:
            raise ProspectiveAttemptLedgerIntegrityErrorV1(
                "reopening a nonempty prospective journal requires its checkpoint"
            )
        identity = (int(status.st_dev), int(status.st_ino))
        try:
            encoded = self._journal_path.read_bytes()
        except OSError as exc:
            raise ProspectiveAttemptLedgerIntegrityErrorV1(
                "prospective journal could not be read"
            ) from exc
        after = _inspect_existing_journal(self._journal_path, allow_missing=False)
        assert after is not None
        if (
            (int(after.st_dev), int(after.st_ino)) != identity
            or int(after.st_size) != len(encoded)
        ):
            raise ProspectiveAttemptLedgerIntegrityErrorV1(
                "prospective journal identity changed during replay"
            )
        if not encoded.endswith(b"\n"):
            raise ProspectiveAttemptLedgerIntegrityErrorV1(
                "prospective journal has a truncated final record"
            )
        for line in encoded.splitlines(keepends=True):
            if not line.endswith(b"\n"):
                raise ProspectiveAttemptLedgerIntegrityErrorV1(
                    "prospective journal contains a truncated record"
                )
            record = _parse_record_v1(line)
            self._admit_replayed_record(record)
        self._file_identity = identity
        current_checkpoint = _checkpoint_v1(
            path=self._journal_path,
            status=after,
            records=self._records,
        )
        if current_checkpoint != expected_checkpoint:
            raise ProspectiveAttemptLedgerIntegrityErrorV1(
                "prospective journal differs from its immutable checkpoint"
            )

    def _admit_replayed_record(self, record: ProspectiveAttemptLedgerRecordV1) -> None:
        expected_sequence = len(self._records) + 1
        if record.sequence != expected_sequence:
            raise ProspectiveAttemptLedgerIntegrityErrorV1(
                "prospective journal record sequence is reordered or discontinuous"
            )
        if record.previous_record_sha256 != self.tip_sha256:
            raise ProspectiveAttemptLedgerIntegrityErrorV1(
                "prospective journal previous-hash chain is broken"
            )
        payload = record.payload
        key = payload.cell_key
        if type(payload) is ProspectiveDecisionCellV1:
            if key in self._decisions:
                raise ProspectiveAttemptLedgerIntegrityErrorV1(
                    "prospective journal contains a duplicate/conflicting decision cell"
                )
            if len(self._decisions) >= self._max_cells:
                raise ProspectiveAttemptLedgerCapacityErrorV1(
                    "persisted prospective journal exceeds configured max_cells"
                )
            self._decisions[key] = payload
        else:
            terminal = cast(ProspectiveExecutionTerminalV1, payload)
            if key not in self._decisions:
                raise ProspectiveAttemptLedgerIntegrityErrorV1(
                    "execution terminal precedes its decision cell"
                )
            if self._decisions[key].decision == "NO_SIGNAL":
                raise ProspectiveAttemptLedgerIntegrityErrorV1(
                    "persisted NO_SIGNAL cell has an execution terminal"
                )
            if terminal.decision_sha256 != self._decisions[key].decision_sha256:
                raise ProspectiveAttemptLedgerIntegrityErrorV1(
                    "execution terminal differs from its decision hash"
                )
            if key in self._terminals:
                raise ProspectiveAttemptLedgerIntegrityErrorV1(
                    "prospective journal contains a duplicate execution terminal"
                )
            self._terminals[key] = terminal
        self._records.append(record)

    def _append_durable(self, record: ProspectiveAttemptLedgerRecordV1) -> None:
        encoded = record.encoded_line
        prior_identity = self._file_identity
        created = prior_identity is None
        mode = "xb" if created else "ab"
        try:
            self._call_fault("before_open")
            with self._journal_path.open(mode) as handle:
                opened = os.fstat(handle.fileno())
                _require_regular_single_link(opened, "opened prospective journal")
                opened_identity = (int(opened.st_dev), int(opened.st_ino))
                if prior_identity is not None and opened_identity != prior_identity:
                    raise ProspectiveAttemptLedgerIntegrityErrorV1(
                        "prospective journal identity changed before append"
                    )
                self._call_fault("before_write")
                written = _write_exact(handle, encoded)
                if written != len(encoded):
                    raise ProspectiveAttemptLedgerWriteErrorV1(
                        "prospective journal append was short"
                    )
                self._call_fault("after_write")
                handle.flush()
                self._call_fault("after_flush")
                os.fsync(handle.fileno())
                self._call_fault("after_fsync")
            if created:
                _fsync_parent(self._journal_path)
            after = _inspect_existing_journal(
                self._journal_path,
                allow_missing=False,
            )
            assert after is not None
            if (int(after.st_dev), int(after.st_ino)) != opened_identity:
                raise ProspectiveAttemptLedgerIntegrityErrorV1(
                    "prospective journal identity changed after append"
                )
            self._file_identity = opened_identity
        except ProspectiveAttemptLedgerErrorV1:
            self._poisoned = True
            raise
        except (OSError, RuntimeError) as exc:
            self._poisoned = True
            raise ProspectiveAttemptLedgerWriteErrorV1(
                "prospective journal append could not be made durable"
            ) from exc

    def _call_fault(self, point: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(point)


def prospective_decision_cell_v1(
    *,
    attempt_id: str,
    rule_version: str,
    universe_version: str,
    symbol: str,
    closed_5m_close_ms: int,
    decision_cutoff_ms: int,
    decision: ProspectiveDecisionV1,
    source_root_sha256: str,
    execution_contract_sha256: str,
    decision_event_id: str | None = None,
    payload_sha256: str | None = None,
) -> ProspectiveDecisionCellV1:
    key = _cell_key_v1(
        attempt_id,
        rule_version,
        universe_version,
        symbol,
        closed_5m_close_ms,
    )
    cell_id = _cell_id_v1(key)
    provisional = ProspectiveDecisionCellV1.__new__(ProspectiveDecisionCellV1)
    values: dict[str, object] = {
        "attempt_id": attempt_id,
        "rule_version": rule_version,
        "universe_version": universe_version,
        "symbol": symbol,
        "closed_5m_close_ms": closed_5m_close_ms,
        "decision_cutoff_ms": decision_cutoff_ms,
        "decision": decision,
        "source_root_sha256": source_root_sha256,
        "execution_contract_sha256": execution_contract_sha256,
        "decision_event_id": decision_event_id,
        "payload_sha256": payload_sha256,
        "cell_id": cell_id,
        "paper_fok_invoked": False,
        "pnl_computed": False,
        "network_accessed": False,
        "order_execution_enabled": False,
        "schema_version": _DECISION_SCHEMA,
    }
    for name, value in values.items():
        object.__setattr__(provisional, name, value)
    decision_sha256 = hashlib.sha256(
        _DECISION_HASH_DOMAIN
        + canonical_json_line(_decision_document_v1(provisional, include_hash=False))
    ).hexdigest()
    return ProspectiveDecisionCellV1(
        **cast(dict[str, object], values),  # pyright: ignore[reportArgumentType]
        decision_sha256=decision_sha256,
    )


def prospective_execution_terminal_v1(
    decision: ProspectiveDecisionCellV1,
    *,
    target_ms: int,
    outcome: ProspectiveExecutionOutcomeV1,
    evidence_state: ProspectiveEvidenceStateV1,
    evidence_locator: str | None,
    evidence_root_sha256: str | None,
    evidence_exchange_event_ms: int | None,
    evidence_receipt_wall_ms: int | None,
    evidence_receipt_monotonic_ns: int | None,
    evidence_evaluation_wall_ms: int | None,
    evidence_age_ms: int | None,
    freshness_limit_ms: int,
    reason: str,
) -> ProspectiveExecutionTerminalV1:
    if type(decision) is not ProspectiveDecisionCellV1:
        raise TypeError("decision must be an exact ProspectiveDecisionCellV1")
    decision.__post_init__()
    if decision.decision == "NO_SIGNAL":
        raise ValueError("NO_SIGNAL decision cells forbid execution terminals")
    terminal_id = _terminal_id_v1(decision.cell_id, target_ms)
    values: dict[str, object] = {
        "attempt_id": decision.attempt_id,
        "rule_version": decision.rule_version,
        "universe_version": decision.universe_version,
        "symbol": decision.symbol,
        "closed_5m_close_ms": decision.closed_5m_close_ms,
        "cell_id": decision.cell_id,
        "decision_sha256": decision.decision_sha256,
        "decision_cutoff_ms": decision.decision_cutoff_ms,
        "target_ms": target_ms,
        "outcome": outcome,
        "evidence_state": evidence_state,
        "evidence_locator": evidence_locator,
        "evidence_root_sha256": evidence_root_sha256,
        "evidence_exchange_event_ms": evidence_exchange_event_ms,
        "evidence_receipt_wall_ms": evidence_receipt_wall_ms,
        "evidence_receipt_monotonic_ns": evidence_receipt_monotonic_ns,
        "evidence_evaluation_wall_ms": evidence_evaluation_wall_ms,
        "evidence_age_ms": evidence_age_ms,
        "freshness_limit_ms": freshness_limit_ms,
        "reason": reason,
        "terminal_id": terminal_id,
        "paper_fok_invoked": False,
        "pnl_computed": False,
        "network_accessed": False,
        "order_execution_enabled": False,
        "schema_version": _TERMINAL_SCHEMA,
    }
    provisional = ProspectiveExecutionTerminalV1.__new__(
        ProspectiveExecutionTerminalV1
    )
    for name, value in values.items():
        object.__setattr__(provisional, name, value)
    terminal_sha256 = hashlib.sha256(
        _TERMINAL_HASH_DOMAIN
        + canonical_json_line(_terminal_document_v1(provisional, include_hash=False))
    ).hexdigest()
    return ProspectiveExecutionTerminalV1(
        **cast(dict[str, object], values),  # pyright: ignore[reportArgumentType]
        terminal_sha256=terminal_sha256,
    )


def _cell_key_v1(
    attempt_id: str,
    rule_version: str,
    universe_version: str,
    symbol: str,
    closed_5m_close_ms: int,
) -> ProspectiveCellKeyV1:
    key = (
        attempt_id,
        rule_version,
        universe_version,
        symbol,
        closed_5m_close_ms,
    )
    _validate_cell_key(key)
    return key


def _cell_id_v1(key: ProspectiveCellKeyV1) -> str:
    return hashlib.sha256(
        _CELL_ID_DOMAIN
        + canonical_json_line(
            {
                "attempt_id": key[0],
                "rule_version": key[1],
                "universe_version": key[2],
                "symbol": key[3],
                "closed_5m_close_ms": key[4],
            }
        )
    ).hexdigest()


def _terminal_id_v1(cell_id: str, target_ms: int) -> str:
    return hashlib.sha256(
        _TERMINAL_ID_DOMAIN
        + canonical_json_line({"cell_id": cell_id, "target_ms": target_ms})
    ).hexdigest()


def _decision_sha256_v1(value: ProspectiveDecisionCellV1) -> str:
    return hashlib.sha256(
        _DECISION_HASH_DOMAIN
        + canonical_json_line(_decision_document_v1(value, include_hash=False))
    ).hexdigest()


def _terminal_sha256_v1(value: ProspectiveExecutionTerminalV1) -> str:
    return hashlib.sha256(
        _TERMINAL_HASH_DOMAIN
        + canonical_json_line(_terminal_document_v1(value, include_hash=False))
    ).hexdigest()


def _decision_document_v1(
    value: ProspectiveDecisionCellV1,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document = {
        "attempt_id": value.attempt_id,
        "rule_version": value.rule_version,
        "universe_version": value.universe_version,
        "symbol": value.symbol,
        "closed_5m_close_ms": value.closed_5m_close_ms,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "decision": value.decision,
        "source_root_sha256": value.source_root_sha256,
        "execution_contract_sha256": value.execution_contract_sha256,
        "decision_event_id": value.decision_event_id,
        "payload_sha256": value.payload_sha256,
        "cell_id": value.cell_id,
        "paper_fok_invoked": value.paper_fok_invoked,
        "pnl_computed": value.pnl_computed,
        "network_accessed": value.network_accessed,
        "order_execution_enabled": value.order_execution_enabled,
        "schema_version": value.schema_version,
    }
    if include_hash:
        document["decision_sha256"] = value.decision_sha256
    return document


def _terminal_document_v1(
    value: ProspectiveExecutionTerminalV1,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document = {
        "attempt_id": value.attempt_id,
        "rule_version": value.rule_version,
        "universe_version": value.universe_version,
        "symbol": value.symbol,
        "closed_5m_close_ms": value.closed_5m_close_ms,
        "cell_id": value.cell_id,
        "decision_sha256": value.decision_sha256,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "target_ms": value.target_ms,
        "outcome": value.outcome,
        "evidence_state": value.evidence_state,
        "evidence_locator": value.evidence_locator,
        "evidence_root_sha256": value.evidence_root_sha256,
        "evidence_exchange_event_ms": value.evidence_exchange_event_ms,
        "evidence_receipt_wall_ms": value.evidence_receipt_wall_ms,
        "evidence_receipt_monotonic_ns": value.evidence_receipt_monotonic_ns,
        "evidence_evaluation_wall_ms": value.evidence_evaluation_wall_ms,
        "evidence_age_ms": value.evidence_age_ms,
        "freshness_limit_ms": value.freshness_limit_ms,
        "reason": value.reason,
        "terminal_id": value.terminal_id,
        "paper_fok_invoked": value.paper_fok_invoked,
        "pnl_computed": value.pnl_computed,
        "network_accessed": value.network_accessed,
        "order_execution_enabled": value.order_execution_enabled,
        "schema_version": value.schema_version,
    }
    if include_hash:
        document["terminal_sha256"] = value.terminal_sha256
    return document


def _new_record_v1(
    *,
    sequence: int,
    previous_record_sha256: str | None,
    payload: ProspectiveLedgerPayloadV1,
) -> ProspectiveAttemptLedgerRecordV1:
    record_type: ProspectiveLedgerRecordTypeV1 = (
        "DECISION_CELL"
        if type(payload) is ProspectiveDecisionCellV1
        else "EXECUTION_TERMINAL"
    )
    provisional = ProspectiveAttemptLedgerRecordV1.__new__(
        ProspectiveAttemptLedgerRecordV1
    )
    values = {
        "sequence": sequence,
        "record_type": record_type,
        "previous_record_sha256": previous_record_sha256,
        "payload": payload,
        "schema_version": _RECORD_SCHEMA,
    }
    for name, value in values.items():
        object.__setattr__(provisional, name, value)
    record_sha256 = hashlib.sha256(
        _RECORD_HASH_DOMAIN
        + canonical_json_line(_record_document_v1(provisional, include_hash=False))
    ).hexdigest()
    return ProspectiveAttemptLedgerRecordV1(
        sequence=sequence,
        record_type=record_type,
        previous_record_sha256=previous_record_sha256,
        payload=payload,
        record_sha256=record_sha256,
    )


def _record_sha256_v1(value: ProspectiveAttemptLedgerRecordV1) -> str:
    return hashlib.sha256(
        _RECORD_HASH_DOMAIN
        + canonical_json_line(_record_document_v1(value, include_hash=False))
    ).hexdigest()


def _record_document_v1(
    value: ProspectiveAttemptLedgerRecordV1,
    *,
    include_hash: bool,
) -> dict[str, object]:
    payload = value.payload
    payload_document = (
        _decision_document_v1(payload, include_hash=True)
        if type(payload) is ProspectiveDecisionCellV1
        else _terminal_document_v1(
            cast(ProspectiveExecutionTerminalV1, payload),
            include_hash=True,
        )
    )
    document = {
        "sequence": value.sequence,
        "record_type": value.record_type,
        "previous_record_sha256": value.previous_record_sha256,
        "payload": payload_document,
        "schema_version": value.schema_version,
    }
    if include_hash:
        document["record_sha256"] = value.record_sha256
    return document


def _journal_root_sha256_v1(
    records: list[ProspectiveAttemptLedgerRecordV1],
) -> str:
    if not records:
        raise ValueError("journal root requires at least one record")
    return hashlib.sha256(
        _JOURNAL_ROOT_DOMAIN
        + canonical_json_line(
            {
                "record_count": len(records),
                "record_sha256s": [record.record_sha256 for record in records],
            }
        )
    ).hexdigest()


def _checkpoint_v1(
    *,
    path: Path,
    status: os.stat_result,
    records: list[ProspectiveAttemptLedgerRecordV1],
) -> ProspectiveAttemptLedgerCheckpointV1:
    if not records:
        raise ValueError("prospective checkpoint requires at least one record")
    values: dict[str, object] = {
        "canonical_path": str(path),
        "journal_identity": f"{int(status.st_dev)}:{int(status.st_ino)}",
        "record_count": len(records),
        "byte_count": int(status.st_size),
        "tip_sha256": records[-1].record_sha256,
        "journal_root_sha256": _journal_root_sha256_v1(records),
        "schema_version": _CHECKPOINT_SCHEMA,
    }
    provisional = ProspectiveAttemptLedgerCheckpointV1.__new__(
        ProspectiveAttemptLedgerCheckpointV1
    )
    for name, value in values.items():
        object.__setattr__(provisional, name, value)
    checkpoint_sha256 = hashlib.sha256(
        _CHECKPOINT_HASH_DOMAIN
        + canonical_json_line(_checkpoint_document_v1(provisional, include_hash=False))
    ).hexdigest()
    return ProspectiveAttemptLedgerCheckpointV1(
        **cast(dict[str, object], values),  # pyright: ignore[reportArgumentType]
        checkpoint_sha256=checkpoint_sha256,
    )


def _checkpoint_sha256_v1(
    value: ProspectiveAttemptLedgerCheckpointV1,
) -> str:
    return hashlib.sha256(
        _CHECKPOINT_HASH_DOMAIN
        + canonical_json_line(_checkpoint_document_v1(value, include_hash=False))
    ).hexdigest()


def _checkpoint_document_v1(
    value: ProspectiveAttemptLedgerCheckpointV1,
    *,
    include_hash: bool,
) -> dict[str, object]:
    document = {
        "canonical_path": value.canonical_path,
        "journal_identity": value.journal_identity,
        "record_count": value.record_count,
        "byte_count": value.byte_count,
        "tip_sha256": value.tip_sha256,
        "journal_root_sha256": value.journal_root_sha256,
        "schema_version": value.schema_version,
    }
    if include_hash:
        document["checkpoint_sha256"] = value.checkpoint_sha256
    return document


def _parse_record_v1(encoded: bytes) -> ProspectiveAttemptLedgerRecordV1:
    try:
        document = json.loads(encoded)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ProspectiveAttemptLedgerIntegrityErrorV1(
            "prospective journal record is not valid UTF-8 JSON"
        ) from exc
    if type(document) is not dict or canonical_json_line(document) != encoded:
        raise ProspectiveAttemptLedgerIntegrityErrorV1(
            "prospective journal record is not exact canonical JSONL"
        )
    _require_exact_fields(
        document,
        {
            "sequence",
            "record_type",
            "previous_record_sha256",
            "payload",
            "record_sha256",
            "schema_version",
        },
        "ledger record",
    )
    if document["schema_version"] != _RECORD_SCHEMA:
        raise ProspectiveAttemptLedgerIntegrityErrorV1(
            "prospective journal record has an unsupported schema"
        )
    record_type = _required_str(document, "record_type")
    payload_document = document["payload"]
    if type(payload_document) is not dict:
        raise ProspectiveAttemptLedgerIntegrityErrorV1(
            "prospective journal payload must be an object"
        )
    try:
        if record_type == "DECISION_CELL":
            payload: ProspectiveLedgerPayloadV1 = _parse_decision_v1(payload_document)
        elif record_type == "EXECUTION_TERMINAL":
            payload = _parse_terminal_v1(payload_document)
        else:
            raise ProspectiveAttemptLedgerIntegrityErrorV1(
                "prospective journal record type is unsupported"
            )
        return ProspectiveAttemptLedgerRecordV1(
            sequence=_required_int(document, "sequence"),
            record_type=cast(ProspectiveLedgerRecordTypeV1, record_type),
            previous_record_sha256=_optional_str(
                document,
                "previous_record_sha256",
            ),
            payload=payload,
            record_sha256=_required_str(document, "record_sha256"),
            schema_version=cast(
                Literal["r4b_v2_prospective_attempt_ledger_record_v1"],
                document["schema_version"],
            ),
        )
    except (TypeError, ValueError) as exc:
        raise ProspectiveAttemptLedgerIntegrityErrorV1(
            "prospective journal record failed exact typed replay"
        ) from exc


def _parse_decision_v1(document: Mapping[str, object]) -> ProspectiveDecisionCellV1:
    _require_exact_fields(
        document,
        {
            "attempt_id",
            "rule_version",
            "universe_version",
            "symbol",
            "closed_5m_close_ms",
            "decision_cutoff_ms",
            "decision",
            "source_root_sha256",
            "execution_contract_sha256",
            "decision_event_id",
            "payload_sha256",
            "cell_id",
            "decision_sha256",
            "paper_fok_invoked",
            "pnl_computed",
            "network_accessed",
            "order_execution_enabled",
            "schema_version",
        },
        "decision payload",
    )
    return ProspectiveDecisionCellV1(
        attempt_id=_required_str(document, "attempt_id"),
        rule_version=_required_str(document, "rule_version"),
        universe_version=_required_str(document, "universe_version"),
        symbol=_required_str(document, "symbol"),
        closed_5m_close_ms=_required_int(document, "closed_5m_close_ms"),
        decision_cutoff_ms=_required_int(document, "decision_cutoff_ms"),
        decision=cast(ProspectiveDecisionV1, _required_str(document, "decision")),
        source_root_sha256=_required_str(document, "source_root_sha256"),
        execution_contract_sha256=_required_str(
            document,
            "execution_contract_sha256",
        ),
        decision_event_id=_optional_str(document, "decision_event_id"),
        payload_sha256=_optional_str(document, "payload_sha256"),
        cell_id=_required_str(document, "cell_id"),
        decision_sha256=_required_str(document, "decision_sha256"),
        paper_fok_invoked=cast(
            Literal[False],
            _required_bool(document, "paper_fok_invoked"),
        ),
        pnl_computed=cast(Literal[False], _required_bool(document, "pnl_computed")),
        network_accessed=cast(
            Literal[False],
            _required_bool(document, "network_accessed"),
        ),
        order_execution_enabled=cast(
            Literal[False],
            _required_bool(document, "order_execution_enabled"),
        ),
        schema_version=cast(
            Literal["r4b_v2_prospective_decision_cell_v1"],
            _required_str(document, "schema_version"),
        ),
    )


def _parse_terminal_v1(
    document: Mapping[str, object],
) -> ProspectiveExecutionTerminalV1:
    _require_exact_fields(
        document,
        {
            "attempt_id",
            "rule_version",
            "universe_version",
            "symbol",
            "closed_5m_close_ms",
            "cell_id",
            "decision_sha256",
            "decision_cutoff_ms",
            "target_ms",
            "outcome",
            "evidence_state",
            "evidence_locator",
            "evidence_root_sha256",
            "evidence_exchange_event_ms",
            "evidence_receipt_wall_ms",
            "evidence_receipt_monotonic_ns",
            "evidence_evaluation_wall_ms",
            "evidence_age_ms",
            "freshness_limit_ms",
            "reason",
            "terminal_id",
            "terminal_sha256",
            "paper_fok_invoked",
            "pnl_computed",
            "network_accessed",
            "order_execution_enabled",
            "schema_version",
        },
        "terminal payload",
    )
    return ProspectiveExecutionTerminalV1(
        attempt_id=_required_str(document, "attempt_id"),
        rule_version=_required_str(document, "rule_version"),
        universe_version=_required_str(document, "universe_version"),
        symbol=_required_str(document, "symbol"),
        closed_5m_close_ms=_required_int(document, "closed_5m_close_ms"),
        cell_id=_required_str(document, "cell_id"),
        decision_sha256=_required_str(document, "decision_sha256"),
        decision_cutoff_ms=_required_int(document, "decision_cutoff_ms"),
        target_ms=_required_int(document, "target_ms"),
        outcome=cast(
            ProspectiveExecutionOutcomeV1,
            _required_str(document, "outcome"),
        ),
        evidence_state=cast(
            ProspectiveEvidenceStateV1,
            _required_str(document, "evidence_state"),
        ),
        evidence_locator=_optional_str(document, "evidence_locator"),
        evidence_root_sha256=_optional_str(document, "evidence_root_sha256"),
        evidence_exchange_event_ms=_optional_int(
            document,
            "evidence_exchange_event_ms",
        ),
        evidence_receipt_wall_ms=_optional_int(
            document,
            "evidence_receipt_wall_ms",
        ),
        evidence_receipt_monotonic_ns=_optional_int(
            document,
            "evidence_receipt_monotonic_ns",
        ),
        evidence_evaluation_wall_ms=_optional_int(
            document,
            "evidence_evaluation_wall_ms",
        ),
        evidence_age_ms=_optional_int(document, "evidence_age_ms"),
        freshness_limit_ms=_required_int(document, "freshness_limit_ms"),
        reason=_required_str(document, "reason"),
        terminal_id=_required_str(document, "terminal_id"),
        terminal_sha256=_required_str(document, "terminal_sha256"),
        paper_fok_invoked=cast(
            Literal[False],
            _required_bool(document, "paper_fok_invoked"),
        ),
        pnl_computed=cast(Literal[False], _required_bool(document, "pnl_computed")),
        network_accessed=cast(
            Literal[False],
            _required_bool(document, "network_accessed"),
        ),
        order_execution_enabled=cast(
            Literal[False],
            _required_bool(document, "order_execution_enabled"),
        ),
        schema_version=cast(
            Literal["r4b_v2_prospective_execution_terminal_v1"],
            _required_str(document, "schema_version"),
        ),
    )


def _validate_cell_key(key: ProspectiveCellKeyV1) -> None:
    if type(key) is not tuple or len(key) != 5:
        raise TypeError("prospective cell key must be an exact five-item tuple")
    _require_identity(key[0], "attempt_id")
    _require_identity(key[1], "rule_version")
    _require_identity(key[2], "universe_version")
    if type(key[3]) is not str or _SYMBOL_RE.fullmatch(key[3]) is None:
        raise ValueError("symbol must be an uppercase normalized market symbol")
    _require_nonnegative_int(key[4], "closed_5m_close_ms")
    if (key[4] + 1) % _FIVE_MINUTES_MS != 0:
        raise ValueError("closed_5m_close_ms is not an exact closed five-minute boundary")


def _require_nonclaims(*values: object) -> None:
    if any(value is not False for value in values):
        raise ValueError(
            "prospective attempt evidence forbids PAPER FOK, PnL, network, and orders"
        )


def _require_identity(value: object, field: str) -> None:
    if (
        type(value) is not str
        or not value
        or len(value) > _MAX_IDENTITY_LENGTH
        or not value.isascii()
        or any(character.isspace() or ord(character) < 33 for character in value)
    ):
        raise ValueError(f"{field} must be a bounded non-whitespace ASCII identity")


def _require_text(value: object, field: str, *, maximum: int) -> None:
    if (
        type(value) is not str
        or not value
        or len(value) > maximum
        or any(character in "\r\n\x00" for character in value)
    ):
        raise ValueError(f"{field} must be bounded nonempty text without line controls")


def _require_sha256(value: object, field: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be lowercase SHA-256 hex")


def _require_nonnegative_int(value: object, field: str) -> None:
    if (
        type(value) is not int
        or value < 0
        or value > _JCS_MAX_SAFE_INTEGER
    ):
        raise ValueError(f"{field} must be a nonnegative JCS-safe integer")


def _require_positive_int(value: object, field: str) -> None:
    _require_nonnegative_int(value, field)
    if cast(int, value) < 1:
        raise ValueError(f"{field} must be positive")


def _required_str(document: Mapping[str, object], field: str) -> str:
    value = document[field]
    if type(value) is not str:
        raise TypeError(f"{field} must be an exact string")
    return value


def _optional_str(document: Mapping[str, object], field: str) -> str | None:
    value = document[field]
    if value is not None and type(value) is not str:
        raise TypeError(f"{field} must be null or an exact string")
    return cast(str | None, value)


def _required_int(document: Mapping[str, object], field: str) -> int:
    value = document[field]
    if type(value) is not int:
        raise TypeError(f"{field} must be an exact integer")
    return value


def _optional_int(document: Mapping[str, object], field: str) -> int | None:
    value = document[field]
    if value is not None and type(value) is not int:
        raise TypeError(f"{field} must be null or an exact integer")
    return cast(int | None, value)


def _required_bool(document: Mapping[str, object], field: str) -> bool:
    value = document[field]
    if type(value) is not bool:
        raise TypeError(f"{field} must be an exact boolean")
    return value


def _require_exact_fields(
    document: Mapping[str, object],
    expected: set[str],
    label: str,
) -> None:
    if set(document) != expected:
        raise ProspectiveAttemptLedgerIntegrityErrorV1(
            f"{label} fields differ from the exact schema"
        )


def _inspect_journal_location(path: str | Path) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = candidate.absolute()
    try:
        inspection = inspect_link_free_path(
            candidate,
            "prospective attempt journal",
            allow_missing_tail=True,
        )
    except ValueError as exc:
        raise ProspectiveAttemptLedgerIntegrityErrorV1(str(exc)) from exc
    absolute = inspection.absolute_path
    if inspection.final_status is None and inspection.first_missing_component != absolute:
        raise ProspectiveAttemptLedgerIntegrityErrorV1(
            "prospective journal parent directory must already exist"
        )
    if inspection.final_status is not None:
        _require_regular_single_link(
            inspection.final_status,
            "prospective attempt journal",
        )
    return absolute


def _inspect_existing_journal(
    path: Path,
    *,
    allow_missing: bool,
) -> os.stat_result | None:
    try:
        inspection = inspect_link_free_path(
            path,
            "prospective attempt journal",
            allow_missing_tail=allow_missing,
        )
    except ValueError as exc:
        raise ProspectiveAttemptLedgerIntegrityErrorV1(str(exc)) from exc
    status = inspection.final_status
    if status is None:
        if allow_missing and inspection.first_missing_component == path:
            return None
        raise ProspectiveAttemptLedgerIntegrityErrorV1(
            "prospective attempt journal does not exist"
        )
    _require_regular_single_link(status, "prospective attempt journal")
    return status


def _require_regular_single_link(status: os.stat_result, label: str) -> None:
    if not stat.S_ISREG(status.st_mode):
        raise ProspectiveAttemptLedgerIntegrityErrorV1(
            f"{label} must be a regular file"
        )
    if int(status.st_nlink) != 1:
        raise ProspectiveAttemptLedgerIntegrityErrorV1(
            f"{label} must have exactly one hard link"
        )


def _write_exact(handle: BinaryIO, encoded: bytes) -> int:
    return handle.write(encoded)


def _fsync_parent(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


PROSPECTIVE_ATTEMPT_LEDGER_NONCLAIMS_V1: Final = (
    "NO_PAPER_FOK_NO_PNL_NO_NETWORK_NO_ORDER"
)
