"""NONAUTHORITATIVE reconciliation candidate for one prospective UTC-day shard.

Until a strict, clean-closed dual-WAL replay owner supplies every input through
a factory-sealed snapshot, even ``COMPLETE`` is only a deterministic projection
of caller-supplied sets.  It never establishes durable storage authority or
authorizes an efficacy PASS, an alert promotion, or a production order.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from enum import StrEnum
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.mirrored_wal import MirroredWalPrefixProofV2
from signalbot.r4b_v2.execution.paper_sizing import (
    PAPER_SIZING_CELLS_V2,
    PaperSizingCellV2,
)
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveCensusSegmentV2,
)

PROSPECTIVE_SEGMENT_SEAL_SCHEMA_V2: Final = (
    "r4b_v2_prospective_segment_storage_seal_v2"
)
PROSPECTIVE_SEGMENT_SEAL_AUTHORITY_V2: Final = (
    "NONAUTHORITATIVE_PURE_RECONCILIATION_PROTOTYPE"
)
_SEAL_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_SEGMENT_STORAGE_SEAL_V2\0"
_SET_ROOT_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_SEGMENT_SET_ROOT_V2\0"
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_FACTORY_TOKEN: Final = object()


class ProspectiveSegmentSealContractErrorV2(ValueError):
    """Raised when a segment storage census is contradictory."""


class ProspectiveSegmentSealStatusV2(StrEnum):
    COMPLETE = "COMPLETE"
    INCOMPLETE = "INCOMPLETE"


@dataclass(frozen=True, slots=True, order=True)
class ProspectiveTerminalKeyV2:
    cell_id: str
    sizing_cell: PaperSizingCellV2

    def __post_init__(self) -> None:
        _validate_sha256(self.cell_id, "terminal cell_id")
        if not isinstance(self.sizing_cell, PaperSizingCellV2):
            raise ProspectiveSegmentSealContractErrorV2(
                "terminal sizing_cell must be PaperSizingCellV2"
            )


@dataclass(frozen=True, slots=True)
class ProspectiveSegmentSealV2:
    attempt_plan_sha256: str
    segment_id: str
    segment_index: int
    day_start_ms: int
    previous_segment_seal_sha256: str | None
    wal_authority_sha256: str
    wal_selection_receipt_sha256: str
    wal_durability_binding_sha256: str
    wal_durable_ack_seq: int
    wal_record_count: int
    wal_prefix_sha256: str
    wal_tail_record_sha256: str | None
    expected_cell_count: int
    expected_cell_root_sha256: str
    prepare_count: int
    prepare_root_sha256: str
    disposition_count: int
    disposition_root_sha256: str
    missing_disposition_count: int
    missing_disposition_root_sha256: str
    orphan_prepare_count: int
    orphan_prepare_root_sha256: str
    signal_count: int
    signal_root_sha256: str
    paper_terminal_count: int
    paper_terminal_root_sha256: str
    pending_terminal_count: int
    pending_terminal_root_sha256: str
    recovery_receipt_count: int
    recovery_receipt_root_sha256: str
    status: ProspectiveSegmentSealStatusV2
    _factory_token: InitVar[object]
    seal_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=PROSPECTIVE_SEGMENT_SEAL_SCHEMA_V2)
    authority_status: str = field(
        init=False,
        default=PROSPECTIVE_SEGMENT_SEAL_AUTHORITY_V2,
    )
    durable_storage_authority_established: bool = field(init=False, default=False)
    efficacy_pass_authorized: bool = field(init=False, default=False)
    production_order_authorized: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ProspectiveSegmentSealContractErrorV2(
                "segment seals can be issued only by the census reconciler"
            )
        object.__setattr__(
            self,
            "seal_sha256",
            hashlib.sha256(
                _SEAL_DOMAIN + canonical_json_line(_seal_document(self))
            ).hexdigest(),
        )


def build_prospective_segment_seal_v2(
    *,
    plan: ProspectiveCensusPlanV2,
    segment: ProspectiveCensusSegmentV2,
    segment_index: int,
    previous_segment_seal_sha256: str | None,
    wal_authority_sha256: str,
    wal_prefix_proof: MirroredWalPrefixProofV2,
    wal_tail_record_sha256: str | None,
    prepared_cell_ids: tuple[str, ...],
    disposition_cell_ids: tuple[str, ...],
    signal_cell_ids: tuple[str, ...],
    paper_terminal_keys: tuple[ProspectiveTerminalKeyV2, ...],
    recovery_receipt_sha256s: tuple[str, ...] = (),
) -> ProspectiveSegmentSealV2:
    """Reconcile one exact day against its frozen census and durable WAL proof."""

    if type(plan) is not ProspectiveCensusPlanV2:
        raise TypeError("plan must be exact ProspectiveCensusPlanV2")
    if type(segment) is not ProspectiveCensusSegmentV2:
        raise TypeError("segment must be exact ProspectiveCensusSegmentV2")
    if type(segment_index) is not int or not 0 <= segment_index < plan.segment_count:
        raise ProspectiveSegmentSealContractErrorV2(
            "segment_index is outside the frozen plan"
        )
    exact_segment = tuple(plan.iter_segments())[segment_index]
    if segment != exact_segment:
        raise ProspectiveSegmentSealContractErrorV2(
            "segment differs from its frozen plan index"
        )
    if segment_index == 0:
        if previous_segment_seal_sha256 is not None:
            raise ProspectiveSegmentSealContractErrorV2(
                "first segment cannot have a predecessor seal"
            )
    else:
        _validate_sha256(
            previous_segment_seal_sha256,
            "previous_segment_seal_sha256",
        )
    _validate_sha256(wal_authority_sha256, "wal_authority_sha256")
    if type(wal_prefix_proof) is not MirroredWalPrefixProofV2:
        raise TypeError("wal_prefix_proof must be exact MirroredWalPrefixProofV2")
    if wal_tail_record_sha256 is None:
        if wal_prefix_proof.durable_ack_seq != 0:
            raise ProspectiveSegmentSealContractErrorV2(
                "nonempty WAL prefix requires its tail record hash"
            )
    else:
        _validate_sha256(wal_tail_record_sha256, "wal_tail_record_sha256")
        if wal_prefix_proof.durable_ack_seq == 0:
            raise ProspectiveSegmentSealContractErrorV2(
                "empty WAL prefix forbids a tail record hash"
            )

    expected = tuple(
        cell.cell_id for cell in plan.iter_expected_cells_for_segment(segment)
    )
    prepared = _normalized_hash_set(prepared_cell_ids, "prepared_cell_ids")
    dispositions = _normalized_hash_set(
        disposition_cell_ids,
        "disposition_cell_ids",
    )
    signals = _normalized_hash_set(signal_cell_ids, "signal_cell_ids")
    expected_set = frozenset(expected)
    if not prepared.issubset(expected_set):
        raise ProspectiveSegmentSealContractErrorV2(
            "prepared cell is outside the frozen segment census"
        )
    if not dispositions.issubset(prepared):
        raise ProspectiveSegmentSealContractErrorV2(
            "every disposition requires its durable prepare"
        )
    if not signals.issubset(dispositions):
        raise ProspectiveSegmentSealContractErrorV2(
            "signal cells must be disposed census members"
        )

    terminal_keys = _normalized_terminal_keys(paper_terminal_keys)
    if any(key.cell_id not in signals for key in terminal_keys):
        raise ProspectiveSegmentSealContractErrorV2(
            "PAPER terminal exists for a non-signal cell"
        )
    expected_terminals = frozenset(
        ProspectiveTerminalKeyV2(cell_id=cell_id, sizing_cell=sizing_cell)
        for cell_id in signals
        for sizing_cell in PAPER_SIZING_CELLS_V2
    )
    if not terminal_keys.issubset(expected_terminals):
        raise ProspectiveSegmentSealContractErrorV2(
            "PAPER terminal key is outside the frozen sizing census"
        )
    accounted_wal_record_count = (
        len(prepared) + len(dispositions) + len(terminal_keys)
    )
    if wal_prefix_proof.record_count != accounted_wal_record_count:
        raise ProspectiveSegmentSealContractErrorV2(
            "durable WAL record count differs from the exact prepare, "
            "disposition, and PAPER-terminal census"
        )

    recovery = _normalized_hash_set(
        recovery_receipt_sha256s,
        "recovery_receipt_sha256s",
    )
    missing = expected_set - dispositions
    orphan_prepares = prepared - dispositions
    pending_terminals = expected_terminals - terminal_keys
    complete = not missing and not orphan_prepares and not pending_terminals
    status = (
        ProspectiveSegmentSealStatusV2.COMPLETE
        if complete
        else ProspectiveSegmentSealStatusV2.INCOMPLETE
    )
    return ProspectiveSegmentSealV2(
        attempt_plan_sha256=plan.plan_sha256,
        segment_id=segment.segment_id,
        segment_index=segment_index,
        day_start_ms=segment.day_start_ms,
        previous_segment_seal_sha256=previous_segment_seal_sha256,
        wal_authority_sha256=wal_authority_sha256,
        wal_selection_receipt_sha256=wal_prefix_proof.selection_receipt_sha256,
        wal_durability_binding_sha256=(
            wal_prefix_proof.durability_binding_sha256
        ),
        wal_durable_ack_seq=wal_prefix_proof.durable_ack_seq,
        wal_record_count=wal_prefix_proof.record_count,
        wal_prefix_sha256=wal_prefix_proof.prefix_sha256,
        wal_tail_record_sha256=wal_tail_record_sha256,
        expected_cell_count=len(expected_set),
        expected_cell_root_sha256=_hash_set("EXPECTED", expected_set),
        prepare_count=len(prepared),
        prepare_root_sha256=_hash_set("PREPARE", prepared),
        disposition_count=len(dispositions),
        disposition_root_sha256=_hash_set("DISPOSITION", dispositions),
        missing_disposition_count=len(missing),
        missing_disposition_root_sha256=_hash_set("MISSING", missing),
        orphan_prepare_count=len(orphan_prepares),
        orphan_prepare_root_sha256=_hash_set("ORPHAN_PREPARE", orphan_prepares),
        signal_count=len(signals),
        signal_root_sha256=_hash_set("SIGNAL", signals),
        paper_terminal_count=len(terminal_keys),
        paper_terminal_root_sha256=_hash_terminal_set(
            "PAPER_TERMINAL",
            terminal_keys,
        ),
        pending_terminal_count=len(pending_terminals),
        pending_terminal_root_sha256=_hash_terminal_set(
            "PENDING_TERMINAL",
            pending_terminals,
        ),
        recovery_receipt_count=len(recovery),
        recovery_receipt_root_sha256=_hash_set("RECOVERY", recovery),
        status=status,
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_prospective_segment_seal_v2(
    seal: ProspectiveSegmentSealV2,
) -> bytes:
    """Return canonical seal bytes after rechecking the content hash."""

    if type(seal) is not ProspectiveSegmentSealV2:
        raise TypeError("seal must be exact ProspectiveSegmentSealV2")
    document = _seal_document(seal)
    expected = hashlib.sha256(
        _SEAL_DOMAIN + canonical_json_line(document)
    ).hexdigest()
    if seal.seal_sha256 != expected:
        raise ProspectiveSegmentSealContractErrorV2(
            "segment seal hash differs from canonical content"
        )
    return canonical_json_line({**document, "seal_sha256": seal.seal_sha256})


def _normalized_hash_set(values: tuple[str, ...], label: str) -> frozenset[str]:
    if type(values) is not tuple:
        raise ProspectiveSegmentSealContractErrorV2(f"{label} must be a tuple")
    for value in values:
        _validate_sha256(value, label)
    if len(set(values)) != len(values):
        raise ProspectiveSegmentSealContractErrorV2(f"{label} contains duplicates")
    return frozenset(values)


def _normalized_terminal_keys(
    values: tuple[ProspectiveTerminalKeyV2, ...],
) -> frozenset[ProspectiveTerminalKeyV2]:
    if type(values) is not tuple or any(
        type(value) is not ProspectiveTerminalKeyV2 for value in values
    ):
        raise ProspectiveSegmentSealContractErrorV2(
            "paper_terminal_keys must be an exact tuple"
        )
    if len(set(values)) != len(values):
        raise ProspectiveSegmentSealContractErrorV2(
            "paper_terminal_keys contains duplicates"
        )
    return frozenset(values)


def _hash_set(label: str, values: frozenset[str]) -> str:
    return hashlib.sha256(
        _SET_ROOT_DOMAIN
        + canonical_json_line({"label": label, "values": sorted(values)})
    ).hexdigest()


def _hash_terminal_set(
    label: str,
    values: frozenset[ProspectiveTerminalKeyV2],
) -> str:
    rows = [
        {"cell_id": value.cell_id, "sizing_cell": value.sizing_cell.value}
        for value in sorted(values)
    ]
    return hashlib.sha256(
        _SET_ROOT_DOMAIN + canonical_json_line({"label": label, "values": rows})
    ).hexdigest()


def _seal_document(seal: ProspectiveSegmentSealV2) -> dict[str, object]:
    return {
        "attempt_plan_sha256": seal.attempt_plan_sha256,
        "authority_status": seal.authority_status,
        "day_start_ms": seal.day_start_ms,
        "disposition_count": seal.disposition_count,
        "disposition_root_sha256": seal.disposition_root_sha256,
        "durable_storage_authority_established": (
            seal.durable_storage_authority_established
        ),
        "efficacy_pass_authorized": seal.efficacy_pass_authorized,
        "expected_cell_count": seal.expected_cell_count,
        "expected_cell_root_sha256": seal.expected_cell_root_sha256,
        "missing_disposition_count": seal.missing_disposition_count,
        "missing_disposition_root_sha256": seal.missing_disposition_root_sha256,
        "orphan_prepare_count": seal.orphan_prepare_count,
        "orphan_prepare_root_sha256": seal.orphan_prepare_root_sha256,
        "paper_terminal_count": seal.paper_terminal_count,
        "paper_terminal_root_sha256": seal.paper_terminal_root_sha256,
        "pending_terminal_count": seal.pending_terminal_count,
        "pending_terminal_root_sha256": seal.pending_terminal_root_sha256,
        "prepare_count": seal.prepare_count,
        "prepare_root_sha256": seal.prepare_root_sha256,
        "previous_segment_seal_sha256": seal.previous_segment_seal_sha256,
        "production_order_authorized": seal.production_order_authorized,
        "recovery_receipt_count": seal.recovery_receipt_count,
        "recovery_receipt_root_sha256": seal.recovery_receipt_root_sha256,
        "schema_version": seal.schema_version,
        "segment_id": seal.segment_id,
        "segment_index": seal.segment_index,
        "signal_count": seal.signal_count,
        "signal_root_sha256": seal.signal_root_sha256,
        "status": seal.status.value,
        "wal_authority_sha256": seal.wal_authority_sha256,
        "wal_durability_binding_sha256": seal.wal_durability_binding_sha256,
        "wal_durable_ack_seq": seal.wal_durable_ack_seq,
        "wal_prefix_sha256": seal.wal_prefix_sha256,
        "wal_record_count": seal.wal_record_count,
        "wal_selection_receipt_sha256": seal.wal_selection_receipt_sha256,
        "wal_tail_record_sha256": seal.wal_tail_record_sha256,
    }


def _validate_sha256(value: object, label: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ProspectiveSegmentSealContractErrorV2(
            f"{label} must be lowercase SHA-256 hex"
        )
