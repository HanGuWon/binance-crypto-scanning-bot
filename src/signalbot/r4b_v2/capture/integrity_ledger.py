from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import stat
import threading
import time
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path

from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import (
    StorageRootBindingError,
    StorageRootBindingV2,
    StorageRootOpenedIdentityV2,
    bind_storage_root_v2,
    inspect_storage_root_opened_identity_v2,
)
from signalbot.r4b_v2.capture.batching import CaptureBatchAckErrorV2
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    SignedBlockContainerError,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockError,
    BlockManifestV2,
    BlockPolicyV2,
    GroupedBlockWriterV2,
    consume_verified_grouped_records_v2,
    grouped_block_root_contract_v2,
    parse_raw_record_line_v2,
    verify_grouped_blocks,
)
from signalbot.r4b_v2.capture.mirrored_wal import MirroredWalWriterV2
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import (
    CaptureFinalityFenceReceiptV2,
    verify_capture_finality_fence_receipt_v2,
)
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingPlanV8,
    ProvisionalPromotingRestCapturePlanV2,
    provisional_promoting_plan_sha256_v2,
    provisional_promoting_plan_sha256_v8,
    provisional_promoting_stream_census_sha256_v2,
    validate_provisional_promoting_capture_plans_v2,
    validate_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.rest_depth_bridge_evidence import (
    DEPTH_BRIDGE_EVENT_TYPE_V8,
    DEPTH_BRIDGE_TERMINAL_RESERVE_BYTES_V8,
    DepthBridgeCoordinatorCleanCloseReceiptV8,
    DepthBridgeCoordinatorClosureEntryV8,
    DepthBridgeEvidenceCensusV8,
    DepthBridgeEvidenceErrorV8,
    DepthBridgeEvidencePayloadV8,
    DepthBridgeGenerationDrainedV8,
    depth_bridge_coordinator_closure_entry_sha256_v8,
    depth_bridge_coordinator_closure_entry_v8,
    depth_bridge_evidence_census_v8,
    parse_depth_bridge_evidence_payload_v8,
    validate_depth_bridge_coordinator_clean_close_receipt_v8,
    validate_depth_bridge_coordinator_closure_entry_v8,
    validate_depth_bridge_evidence_order_v8,
    validate_depth_bridge_evidence_payload_v8,
)
from signalbot.r4b_v2.capture.wal import (
    WalAuthorityV2,
    WalDurabilityBindingV2,
    WalError,
)
from signalbot.r4b_v2.capture.websocket_finality import (
    FinalizedWebSocketRouteCursorPairV8,
    WebSocketRouteCursorClosureEntryV8,
    WebSocketRouteCursorClosurePairV8,
    validate_finalized_websocket_route_cursor_pair_v8,
    validate_websocket_route_cursor_closure_pair_v8,
    websocket_route_cursor_closure_pair_sha256_v8,
    websocket_route_cursor_closure_pair_v8,
)

_EVENT_RE = re.compile(r"integrity-event-(?P<sequence>[0-9]{8})\.json")
_PARTIAL_RE = re.compile(
    r"integrity-event-(?P<sequence>[0-9]{8})\.json\.partial"
)
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_EVENT_ID_DOMAIN = b"R4B_V2_CAPTURE_INTEGRITY_EVENT_ID\0"
_SOURCE_GAP_ID_DOMAIN = b"R4B_V2_SOURCE_GAP_ID\0"
_PATH_ID_DOMAIN = b"R4B_V2_STORAGE_ROOT_PATH\0"
_MAX_IDENTITY_LENGTH = 256
_MAX_EVENT_SEQUENCE = 99_999_999
_LEDGER_SCHEMA = "r4b_v2_capture_integrity_event_v1"
_LEDGER_CONTRACT_SCHEMA = "r4b_v2_capture_integrity_ledger_contract_v2"
_FINALIZED_REFERENCE_SCHEMA = "r4b_v2_finalized_block_reference_v1"
_FINALIZED_RECORD_LOCATOR_SCHEMA = "r4b_v2_finalized_record_locator_v1"
_FINALIZED_RECORD_LOCATOR_DOMAIN = b"R4B_V2_FINALIZED_RECORD_LOCATOR\0"
_BLOCK_SIGNATURE_DOMAIN = b"R4B_BLOCK_ED25519_SIGNATURE_V2\0"
_SOURCE_GAP_BOUNDED_RESERVE_BYTES = 64 * 1024
_CLEAN_CLOSURE_SEAL_RESERVE_BYTES = 64 * 1024
_CLEAN_CLOSURE_SEAL_FILE = "capture-clean-closure-seal.json"
_CLEAN_CLOSURE_SEAL_PARTIAL_FILE = f"{_CLEAN_CLOSURE_SEAL_FILE}.partial"
_CLEAN_CLOSURE_SEAL_SCHEMA = "r4b_v2_capture_clean_closure_seal_v2"
_PERSISTED_CLEAN_CLOSURE_RECEIPT_SCHEMA = (
    "r4b_v2_persisted_capture_clean_closure_seal_receipt_v1"
)
_PERSISTED_CLEAN_CLOSURE_RECEIPT_DOMAIN = (
    b"R4B_V2_PERSISTED_CAPTURE_CLEAN_CLOSURE_SEAL_RECEIPT\0"
)
_PERSISTED_CLEAN_CLOSURE_FACTORY_TOKEN = object()
_CLEAN_CLOSURE_SEAL_SCHEMA_V8 = "r4b_v2_capture_clean_closure_seal_v8"
_PERSISTED_CLEAN_CLOSURE_RECEIPT_SCHEMA_V8 = (
    "r4b_v2_persisted_capture_clean_closure_seal_receipt_v8"
)
_PERSISTED_CLEAN_CLOSURE_RECEIPT_DOMAIN_V8 = (
    b"R4B_V2_PERSISTED_CAPTURE_CLEAN_CLOSURE_SEAL_RECEIPT_V8\0"
)
_PERSISTED_CLEAN_CLOSURE_FACTORY_TOKEN_V8 = object()
_DEPTH_BRIDGE_CLOSE_RECEIPT_DOMAIN_V8 = (
    b"R4B_V2_DEPTH_BRIDGE_COORDINATOR_CLEAN_CLOSE_RECEIPT_V8\0"
)
_DEPTH_BRIDGE_CLOSURE_ENTRY_DOMAIN_V8 = (
    b"R4B_V2_DEPTH_BRIDGE_COORDINATOR_CLOSURE_ENTRY_V8\0"
)


class CaptureIntegrityLedgerError(RuntimeError):
    """Base error for the append-only capture-integrity ledger."""


class CaptureIntegrityLedgerIntegrityError(CaptureIntegrityLedgerError):
    """Raised when ledger, block-root, or event evidence cannot be trusted."""


class CaptureIntegrityLedgerCapacityError(CaptureIntegrityLedgerError):
    """Raised before a sealed ledger count or disk bound would be crossed."""


def capture_integrity_ledger_root_contract_v2(
    *,
    block_root_binding: StorageRootBindingV2,
    block_directory: str | Path,
    block_signing_authority: BlockSigningAuthorityV2,
    max_events: int,
) -> dict[str, object]:
    """Return the immutable contract for one capture-integrity ledger root."""

    if not isinstance(block_root_binding, StorageRootBindingV2):
        raise TypeError("block_root_binding must be a StorageRootBindingV2")
    if block_root_binding.storage_kind != "GROUPED_BLOCK":
        raise ValueError("integrity ledger requires a GROUPED_BLOCK root binding")
    if not isinstance(block_signing_authority, BlockSigningAuthorityV2):
        raise TypeError(
            "block_signing_authority must be a BlockSigningAuthorityV2"
        )
    if type(max_events) is not int or not 1 <= max_events <= _MAX_EVENT_SEQUENCE:
        raise ValueError("max_events is outside the sealed filename bound")
    normalized_block_directory = _normalized_resolved_path(block_directory)
    block_root_binding_sha256 = hashlib.sha256(
        canonical_json_line(asdict(block_root_binding))
    ).hexdigest()
    return {
        "schema_version": _LEDGER_CONTRACT_SCHEMA,
        "block_root_binding_sha256": block_root_binding_sha256,
        "block_root_path_sha256": _root_path_sha256(normalized_block_directory),
        "block_signing_authority_sha256": block_signing_authority.sha256,
        "max_events": max_events,
    }


class DataGapCauseV2(StrEnum):
    """Causes for an exact interval of already allocated local ingest IDs."""

    BOUNDED_QUEUE_OVERFLOW = "BOUNDED_QUEUE_OVERFLOW"
    UNRECOVERABLE_PARTIAL_APPEND = "UNRECOVERABLE_PARTIAL_APPEND"
    INGEST_SEQUENCE_DISCONTINUITY = "INGEST_SEQUENCE_DISCONTINUITY"


class SourceGapCauseV2(StrEnum):
    """Closed causes for an upstream interval whose message count is unknowable."""

    SESSION_START_PENDING = "SESSION_START_PENDING"
    WEBSOCKET_DISCONNECT = "WEBSOCKET_DISCONNECT"
    PROACTIVE_RECYCLE = "PROACTIVE_RECYCLE"


class SourceGapPhaseV2(StrEnum):
    OPEN = "OPEN"
    BOUNDED = "BOUNDED"


class SourceGapLeftBoundaryV2(StrEnum):
    SESSION_START = "SESSION_START"
    RETAINED_FRAME = "RETAINED_FRAME"


@dataclass(frozen=True, slots=True)
class DataGapPayloadV2:
    first_missing_ingest_seq: int
    last_missing_ingest_seq: int
    missing_count: int
    receipt_wall_lower_bound_ms: int
    receipt_wall_upper_bound_ms: int
    receipt_monotonic_lower_bound_ns: int
    receipt_monotonic_upper_bound_ns: int
    cause: str
    source_component: str
    evidence_sha256: str
    schema_version: str = "r4b_v2_data_gap_payload_v1"

    def __post_init__(self) -> None:
        if (
            type(self.first_missing_ingest_seq) is not int
            or type(self.last_missing_ingest_seq) is not int
            or self.first_missing_ingest_seq < 1
            or self.last_missing_ingest_seq < self.first_missing_ingest_seq
        ):
            raise ValueError("DATA_GAP ingest interval is invalid")
        expected_count = (
            self.last_missing_ingest_seq - self.first_missing_ingest_seq + 1
        )
        if type(self.missing_count) is not int or self.missing_count != expected_count:
            raise ValueError("DATA_GAP missing_count must equal its exact closed interval")
        _validate_closed_bounds(
            self.receipt_wall_lower_bound_ms,
            self.receipt_wall_upper_bound_ms,
            "receipt wall",
        )
        _validate_closed_bounds(
            self.receipt_monotonic_lower_bound_ns,
            self.receipt_monotonic_upper_bound_ns,
            "receipt monotonic",
        )
        if self.cause not in {cause.value for cause in DataGapCauseV2}:
            raise ValueError("DATA_GAP cause is not in the sealed cause set")
        _validate_identity(self.source_component, "source_component")
        _validate_sha256(self.evidence_sha256, "evidence_sha256")
        if self.schema_version != "r4b_v2_data_gap_payload_v1":
            raise ValueError("unsupported DATA_GAP payload schema")


@dataclass(frozen=True, slots=True)
class FinalizedRecordLocatorV2:
    """Compact exact-record pointer resolved only through the signed block chain."""

    authority_sha256: str
    block_sequence: int
    block_hash: str
    ingest_seq: int
    record_jsonl_sha256: str
    locator_sha256: str
    schema_version: str = _FINALIZED_RECORD_LOCATOR_SCHEMA

    def __post_init__(self) -> None:
        for value, label in (
            (self.authority_sha256, "authority_sha256"),
            (self.block_hash, "block_hash"),
            (self.record_jsonl_sha256, "record_jsonl_sha256"),
            (self.locator_sha256, "locator_sha256"),
        ):
            _validate_sha256(value, label)
        _validate_positive_value(self.block_sequence, "block_sequence")
        _validate_positive_value(self.ingest_seq, "ingest_seq")
        if self.schema_version != _FINALIZED_RECORD_LOCATOR_SCHEMA:
            raise ValueError("unsupported finalized-record locator schema")
        if self.locator_sha256 != _finalized_record_locator_sha256(self):
            raise ValueError("finalized-record locator hash differs from its evidence")


@dataclass(frozen=True, slots=True)
class SourceGapPayloadV2:
    """Durable upstream-coverage gap, opened before reconnect and bounded later.

    Binance does not expose a sequence covering every combined-stream message.
    A disconnect therefore cannot truthfully claim a missing message count or
    manufacture local ``ingest_seq`` values.  OPEN is fsynced at detection;
    BOUNDED may be appended only after the successor frame is independently
    durable and finalized.  BOUNDED means the unknown interval has endpoints,
    never that missing market data was recovered.
    """

    gap_id: str
    phase: str
    session_id: str
    process_boot_id: str
    plan_id: str
    venue: str
    route_id: str
    affected_streams_sha256: str
    affected_stream_count: int
    cause: str
    left_boundary_kind: str
    left_connection_id: str | None
    left_generation: int | None
    left_frame_seq: int | None
    left_ingest_seq: int | None
    left_wall_ms: int
    left_monotonic_ns: int
    detected_wall_ms: int
    detected_monotonic_ns: int
    right_connection_id: str | None
    right_generation: int | None
    right_frame_seq: int | None
    right_ingest_seq: int | None
    right_wall_ms: int | None
    right_monotonic_ns: int | None
    open_event_sha256: str | None
    left_record_locator: FinalizedRecordLocatorV2 | None
    right_record_locator: FinalizedRecordLocatorV2 | None
    source_message_count_known: bool
    missing_source_message_count: int | None
    source_component: str
    evidence_sha256: str
    schema_version: str = "r4b_v2_source_gap_payload_v2_compact_locator"

    def __post_init__(self) -> None:
        for field_name in ("left_record_locator", "right_record_locator"):
            value: object = getattr(self, field_name)
            if isinstance(value, dict):
                object.__setattr__(
                    self,
                    field_name,
                    FinalizedRecordLocatorV2(**value),  # type: ignore[arg-type]
                )
        _validate_sha256(self.gap_id, "gap_id")
        if self.phase not in {phase.value for phase in SourceGapPhaseV2}:
            raise ValueError("SOURCE_GAP phase is not in the sealed phase set")
        _validate_identity(self.session_id, "session_id")
        _validate_identity(self.process_boot_id, "process_boot_id")
        _validate_identity(self.plan_id, "plan_id")
        if self.venue != "usdm_futures":
            raise ValueError("SOURCE_GAP venue must be usdm_futures")
        _validate_identity(self.route_id, "route_id")
        _validate_sha256(self.affected_streams_sha256, "affected_streams_sha256")
        if type(self.affected_stream_count) is not int or self.affected_stream_count < 1:
            raise ValueError("SOURCE_GAP affected_stream_count must be positive")
        if self.cause not in {cause.value for cause in SourceGapCauseV2}:
            raise ValueError("SOURCE_GAP cause is not in the sealed cause set")
        if self.left_boundary_kind not in {
            boundary.value for boundary in SourceGapLeftBoundaryV2
        }:
            raise ValueError("SOURCE_GAP left boundary is not in the sealed set")
        _validate_source_gap_left_boundary(self)
        _validate_nonnegative_value(self.left_wall_ms, "left_wall_ms")
        _validate_nonnegative_value(self.left_monotonic_ns, "left_monotonic_ns")
        _validate_nonnegative_value(self.detected_wall_ms, "detected_wall_ms")
        _validate_nonnegative_value(
            self.detected_monotonic_ns,
            "detected_monotonic_ns",
        )
        if self.detected_monotonic_ns < self.left_monotonic_ns:
            raise ValueError("SOURCE_GAP detection precedes its left monotonic bound")
        _validate_source_gap_phase_fields(self)
        if self.source_message_count_known is not False:
            raise ValueError("SOURCE_GAP source message count must remain unknown")
        if self.missing_source_message_count is not None:
            raise ValueError("SOURCE_GAP cannot claim a missing source message count")
        _validate_identity(self.source_component, "source_component")
        _validate_sha256(self.evidence_sha256, "evidence_sha256")
        if self.gap_id != _source_gap_id(self):
            raise ValueError("SOURCE_GAP gap_id differs from its immutable open identity")
        if self.schema_version != "r4b_v2_source_gap_payload_v2_compact_locator":
            raise ValueError("unsupported SOURCE_GAP payload schema")


@dataclass(frozen=True, slots=True)
class FinalizedBlockReferenceV2:
    authority_sha256: str
    block_root_binding_sha256: str
    block_root_path_sha256: str
    block_sequence: int
    block_hash: str
    previous_block_hash: str | None
    first_ingest_seq: int
    last_ingest_seq: int
    data_file: str
    manifest_file: str
    expected_container_bytes: int
    expected_container_sha256: str
    expected_manifest_sha256: str
    writer_key_id: str
    writer_ed25519_signature: str
    signing_authority_sha256: str
    schema_version: str = _FINALIZED_REFERENCE_SCHEMA

    def __post_init__(self) -> None:
        for name in (
            "authority_sha256",
            "block_root_binding_sha256",
            "block_root_path_sha256",
            "block_hash",
            "expected_container_sha256",
            "expected_manifest_sha256",
            "signing_authority_sha256",
        ):
            _validate_sha256(getattr(self, name), name)
        if self.previous_block_hash is not None:
            _validate_sha256(self.previous_block_hash, "previous_block_hash")
        if type(self.block_sequence) is not int or self.block_sequence < 1:
            raise ValueError("block_sequence must be a positive integer")
        if (
            type(self.first_ingest_seq) is not int
            or type(self.last_ingest_seq) is not int
            or self.first_ingest_seq < 1
            or self.last_ingest_seq < self.first_ingest_seq
        ):
            raise ValueError("finalized block ingest interval is invalid")
        if type(self.expected_container_bytes) is not int or self.expected_container_bytes < 1:
            raise ValueError("expected_container_bytes must be positive")
        _validate_local_filename(self.data_file, "data_file")
        _validate_local_filename(self.manifest_file, "manifest_file")
        _validate_identity(self.writer_key_id, "writer_key_id")
        try:
            signature = base64.b64decode(
                self.writer_ed25519_signature,
                validate=True,
            )
        except (ValueError, TypeError) as exc:
            raise ValueError("writer_ed25519_signature is not strict base64") from exc
        if len(signature) != 64:
            raise ValueError("writer_ed25519_signature must contain 64 bytes")
        if self.schema_version != _FINALIZED_REFERENCE_SCHEMA:
            raise ValueError("unsupported finalized-block reference schema")


@dataclass(frozen=True, slots=True)
class VoidFinalizedBlockPayloadV2:
    authority_sha256: str
    block_root_binding_sha256: str
    block_root_path_sha256: str
    block_sequence: int
    block_hash: str
    previous_block_hash: str | None
    first_ingest_seq: int
    last_ingest_seq: int
    data_file: str
    manifest_file: str
    expected_container_bytes: int
    expected_container_sha256: str
    expected_manifest_sha256: str
    writer_key_id: str
    writer_ed25519_signature: str
    signing_authority_sha256: str
    observed_data_state: str
    observed_data_bytes: int | None
    observed_data_sha256: str | None
    observed_manifest_state: str
    observed_manifest_bytes: int | None
    observed_manifest_sha256: str | None
    corruption_kinds: tuple[str, ...]
    detector_component: str
    detection_evidence_sha256: str
    void_reason: str = "FINALIZED_BLOCK_CORRUPTION"
    schema_version: str = "r4b_v2_void_finalized_block_payload_v1"

    def __post_init__(self) -> None:
        FinalizedBlockReferenceV2(
            authority_sha256=self.authority_sha256,
            block_root_binding_sha256=self.block_root_binding_sha256,
            block_root_path_sha256=self.block_root_path_sha256,
            block_sequence=self.block_sequence,
            block_hash=self.block_hash,
            previous_block_hash=self.previous_block_hash,
            first_ingest_seq=self.first_ingest_seq,
            last_ingest_seq=self.last_ingest_seq,
            data_file=self.data_file,
            manifest_file=self.manifest_file,
            expected_container_bytes=self.expected_container_bytes,
            expected_container_sha256=self.expected_container_sha256,
            expected_manifest_sha256=self.expected_manifest_sha256,
            writer_key_id=self.writer_key_id,
            writer_ed25519_signature=self.writer_ed25519_signature,
            signing_authority_sha256=self.signing_authority_sha256,
        )
        _validate_observation(
            self.observed_data_state,
            self.observed_data_bytes,
            self.observed_data_sha256,
            "data",
        )
        _validate_observation(
            self.observed_manifest_state,
            self.observed_manifest_bytes,
            self.observed_manifest_sha256,
            "manifest",
        )
        allowed_kinds = (
            "DATA_MISSING",
            "DATA_NON_REGULAR",
            "DATA_LENGTH_MISMATCH",
            "DATA_SHA256_MISMATCH",
            "MANIFEST_MISSING",
            "MANIFEST_NON_REGULAR",
            "MANIFEST_SHA256_MISMATCH",
        )
        if (
            not self.corruption_kinds
            or len(set(self.corruption_kinds)) != len(self.corruption_kinds)
            or tuple(kind for kind in allowed_kinds if kind in self.corruption_kinds)
            != self.corruption_kinds
        ):
            raise ValueError("VOID corruption kinds are empty, unknown, or out of order")
        _validate_identity(self.detector_component, "detector_component")
        _validate_sha256(
            self.detection_evidence_sha256,
            "detection_evidence_sha256",
        )
        if self.void_reason != "FINALIZED_BLOCK_CORRUPTION":
            raise ValueError("VOID reason must identify finalized-block corruption")
        if self.schema_version != "r4b_v2_void_finalized_block_payload_v1":
            raise ValueError("unsupported VOID payload schema")


@dataclass(frozen=True, slots=True)
class CaptureIntegrityEventV2:
    event_sequence: int
    previous_event_sha256: str | None
    event_id: str
    event_type: str
    authority_sha256: str
    ledger_root_binding_sha256: str
    block_root_binding_sha256: str
    block_root_path_sha256: str
    recorded_wall_ms: int
    recorded_monotonic_ns: int
    payload: dict[str, object]
    schema_version: str = _LEDGER_SCHEMA

    @property
    def sha256(self) -> str:
        return hashlib.sha256(canonical_json_line(asdict(self))).hexdigest()


@dataclass(frozen=True, slots=True)
class CaptureCleanClosureSealV2:
    """Canonical CLEAN terminal state for one exact retained capture prefix."""

    session_id: str
    process_boot_id: str
    seal_wall_ms: int
    seal_monotonic_ns: int
    authority_sha256: str
    attempt_id: str
    qualification_id: str
    ledger_root_binding_sha256: str
    ledger_root_path_sha256: str
    block_root_binding_sha256: str
    block_root_path_sha256: str
    block_signing_authority_sha256: str
    block_clean_tail_terminal_sha256: str
    wal_durability_binding_sha256: str
    event_count: int
    event_tip_sha256: str | None
    source_gap_open_count: int
    source_gap_bounded_count: int
    unmatched_source_gap_open_count: int
    data_gap_count: int
    void_count: int
    finality_receipt: CaptureFinalityFenceReceiptV2
    finality_receipt_sha256: str
    finality_prefix_proof_sha256: str
    finality_tail_ingest_seq: int
    closure_status: str = "CLEAN"
    schema_version: str = _CLEAN_CLOSURE_SEAL_SCHEMA

    def __post_init__(self) -> None:
        for value, name in (
            (self.session_id, "session_id"),
            (self.process_boot_id, "process_boot_id"),
            (self.attempt_id, "attempt_id"),
            (self.qualification_id, "qualification_id"),
        ):
            _validate_identity(value, name)
        for value, name in (
            (self.seal_wall_ms, "seal_wall_ms"),
            (self.seal_monotonic_ns, "seal_monotonic_ns"),
            (self.event_count, "event_count"),
            (self.source_gap_open_count, "source_gap_open_count"),
            (self.source_gap_bounded_count, "source_gap_bounded_count"),
            (
                self.unmatched_source_gap_open_count,
                "unmatched_source_gap_open_count",
            ),
            (self.data_gap_count, "data_gap_count"),
            (self.void_count, "void_count"),
        ):
            _validate_nonnegative_integer(value, name)
        _validate_positive_value(
            self.finality_tail_ingest_seq,
            "finality_tail_ingest_seq",
        )
        for value, name in (
            (self.authority_sha256, "authority_sha256"),
            (
                self.ledger_root_binding_sha256,
                "ledger_root_binding_sha256",
            ),
            (self.ledger_root_path_sha256, "ledger_root_path_sha256"),
            (self.block_root_binding_sha256, "block_root_binding_sha256"),
            (self.block_root_path_sha256, "block_root_path_sha256"),
            (
                self.block_signing_authority_sha256,
                "block_signing_authority_sha256",
            ),
            (
                self.block_clean_tail_terminal_sha256,
                "block_clean_tail_terminal_sha256",
            ),
            (
                self.wal_durability_binding_sha256,
                "wal_durability_binding_sha256",
            ),
            (self.finality_receipt_sha256, "finality_receipt_sha256"),
            (
                self.finality_prefix_proof_sha256,
                "finality_prefix_proof_sha256",
            ),
        ):
            _validate_sha256(value, name)
        if self.event_tip_sha256 is not None:
            _validate_sha256(self.event_tip_sha256, "event_tip_sha256")
        if (self.event_count == 0) != (self.event_tip_sha256 is None):
            raise ValueError("clean closure event count and tip are inconsistent")
        if (
            self.source_gap_bounded_count > self.source_gap_open_count
            or self.unmatched_source_gap_open_count
            != self.source_gap_open_count - self.source_gap_bounded_count
        ):
            raise ValueError("clean closure SOURCE_GAP census is inconsistent")
        if self.event_count != (
            self.source_gap_open_count
            + self.source_gap_bounded_count
            + self.data_gap_count
            + self.void_count
        ):
            raise ValueError("clean closure event census does not equal event_count")
        if type(self.finality_receipt) is not CaptureFinalityFenceReceiptV2:
            raise TypeError(
                "finality_receipt must be an exact CaptureFinalityFenceReceiptV2"
            )
        self.finality_receipt.__post_init__()
        if self.finality_receipt_sha256 != self.finality_receipt.sha256:
            raise ValueError("clean closure finality-receipt hash differs")
        if self.finality_prefix_proof_sha256 != (
            self.finality_receipt.prefix_proof_sha256
        ):
            raise ValueError("clean closure finality-prefix proof differs")
        if self.finality_tail_ingest_seq != self.finality_receipt.fence_ingest_seq:
            raise ValueError("clean closure finality tail differs from its receipt")
        if (
            self.authority_sha256 != self.finality_receipt.authority_sha256
            or self.attempt_id != self.finality_receipt.attempt_id
            or self.qualification_id != self.finality_receipt.qualification_id
        ):
            raise ValueError("clean closure authority differs from its finality receipt")
        expected_wal_binding_sha256 = hashlib.sha256(
            canonical_json_line(asdict(self.finality_receipt.wal_durability_binding))
        ).hexdigest()
        if self.wal_durability_binding_sha256 != expected_wal_binding_sha256:
            raise ValueError("clean closure WAL durability binding hash differs")
        if self.block_root_binding_sha256 != (
            self.finality_receipt.grouped_block_root_binding_sha256
        ):
            raise ValueError("clean closure block-root binding hash differs")
        if self.block_signing_authority_sha256 != (
            self.finality_receipt.block_signing_authority_sha256
        ):
            raise ValueError("clean closure signing authority differs")
        if self.unmatched_source_gap_open_count != 0:
            raise ValueError("CLEAN closure cannot retain unmatched SOURCE_GAP OPEN")
        if self.void_count != 0:
            raise ValueError("CLEAN closure cannot retain VOID evidence")
        if self.closure_status != "CLEAN":
            raise ValueError("capture closure seal status must be CLEAN")
        if self.schema_version != _CLEAN_CLOSURE_SEAL_SCHEMA:
            raise ValueError("unsupported capture clean-closure seal schema")
        if self.seal_wall_ms < self.finality_receipt.target_last_receipt_wall_ms:
            raise ValueError("clean closure wall clock precedes its final data receipt")
        if self.seal_monotonic_ns < (
            self.finality_receipt.writer_observed_monotonic_ns
        ):
            raise ValueError("clean closure monotonic clock precedes finality")

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(asdict(self))

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.encoded_line).hexdigest()


@dataclass(frozen=True, slots=True)
class CaptureCleanClosureSealV8:
    """Durable infrastructure-CLEAN state for the exact four-plan V8 prefix."""

    session_id: str
    process_boot_id: str
    protocol_hash: str
    plan_bundle_sha256: str
    depth_plan_sha256: str
    seal_wall_ms: int
    seal_monotonic_ns: int
    authority_sha256: str
    attempt_id: str
    qualification_id: str
    ledger_root_binding_sha256: str
    ledger_root_path_sha256: str
    block_root_binding_sha256: str
    block_root_path_sha256: str
    block_signing_authority_sha256: str
    block_clean_tail_terminal_sha256: str
    wal_durability_binding_sha256: str
    event_count: int
    event_tip_sha256: str
    source_gap_open_count: int
    source_gap_bounded_count: int
    unmatched_source_gap_open_count: int
    data_gap_count: int
    void_count: int
    depth_bridge_event_count: int
    depth_bridge_generation_started_count: int
    depth_bridge_generation_drained_count: int
    depth_bridge_fatal_generation_count: int
    depth_bridge_trigger_count: int
    depth_bridge_cycle_count: int
    depth_bridge_failed_cycle_count: int
    depth_bridge_open_generation_count: int
    depth_bridge_open_cycle_count: int
    depth_bridge_open_attempt_count: int
    depth_bridge_open_wait_count: int
    last_depth_bridge_drain_event_sequence: int
    last_depth_bridge_drain_event_sha256: str
    last_depth_bridge_drain_recorded_wall_ms: int
    last_depth_bridge_drain_recorded_monotonic_ns: int
    finality_receipt: CaptureFinalityFenceReceiptV2
    finality_receipt_sha256: str
    finality_prefix_proof_sha256: str
    finality_tail_ingest_seq: int
    websocket_route_cursor_closure_pair: WebSocketRouteCursorClosurePairV8
    websocket_route_cursor_closure_pair_sha256: str
    depth_bridge_closure_entry: DepthBridgeCoordinatorClosureEntryV8
    depth_bridge_closure_entry_sha256: str
    qualification_complete_claimed: bool
    promoting: bool
    book_bridge_certified: bool
    m2_certified: bool
    order_execution_enabled: bool
    closure_status: str = "CLEAN"
    schema_version: str = _CLEAN_CLOSURE_SEAL_SCHEMA_V8

    def __post_init__(self) -> None:
        for value, name in (
            (self.session_id, "session_id"),
            (self.process_boot_id, "process_boot_id"),
            (self.attempt_id, "attempt_id"),
            (self.qualification_id, "qualification_id"),
        ):
            _validate_identity(value, name)
        for value, name in (
            (self.protocol_hash, "protocol_hash"),
            (self.plan_bundle_sha256, "plan_bundle_sha256"),
            (self.depth_plan_sha256, "depth_plan_sha256"),
            (self.authority_sha256, "authority_sha256"),
            (self.ledger_root_binding_sha256, "ledger_root_binding_sha256"),
            (self.ledger_root_path_sha256, "ledger_root_path_sha256"),
            (self.block_root_binding_sha256, "block_root_binding_sha256"),
            (self.block_root_path_sha256, "block_root_path_sha256"),
            (
                self.block_signing_authority_sha256,
                "block_signing_authority_sha256",
            ),
            (
                self.block_clean_tail_terminal_sha256,
                "block_clean_tail_terminal_sha256",
            ),
            (
                self.wal_durability_binding_sha256,
                "wal_durability_binding_sha256",
            ),
            (self.event_tip_sha256, "event_tip_sha256"),
            (self.last_depth_bridge_drain_event_sha256, "last drain event SHA-256"),
            (self.finality_receipt_sha256, "finality_receipt_sha256"),
            (self.finality_prefix_proof_sha256, "finality_prefix_proof_sha256"),
            (
                self.websocket_route_cursor_closure_pair_sha256,
                "websocket_route_cursor_closure_pair_sha256",
            ),
            (
                self.depth_bridge_closure_entry_sha256,
                "depth_bridge_closure_entry_sha256",
            ),
        ):
            _validate_sha256(value, name)
        for value, name in (
            (self.seal_wall_ms, "seal_wall_ms"),
            (self.seal_monotonic_ns, "seal_monotonic_ns"),
            (self.event_count, "event_count"),
            (self.source_gap_open_count, "source_gap_open_count"),
            (self.source_gap_bounded_count, "source_gap_bounded_count"),
            (
                self.unmatched_source_gap_open_count,
                "unmatched_source_gap_open_count",
            ),
            (self.data_gap_count, "data_gap_count"),
            (self.void_count, "void_count"),
            (self.depth_bridge_event_count, "depth_bridge_event_count"),
            (
                self.depth_bridge_generation_started_count,
                "depth_bridge_generation_started_count",
            ),
            (
                self.depth_bridge_generation_drained_count,
                "depth_bridge_generation_drained_count",
            ),
            (
                self.depth_bridge_fatal_generation_count,
                "depth_bridge_fatal_generation_count",
            ),
            (self.depth_bridge_trigger_count, "depth_bridge_trigger_count"),
            (self.depth_bridge_cycle_count, "depth_bridge_cycle_count"),
            (
                self.depth_bridge_failed_cycle_count,
                "depth_bridge_failed_cycle_count",
            ),
            (
                self.depth_bridge_open_generation_count,
                "depth_bridge_open_generation_count",
            ),
            (
                self.depth_bridge_open_cycle_count,
                "depth_bridge_open_cycle_count",
            ),
            (
                self.depth_bridge_open_attempt_count,
                "depth_bridge_open_attempt_count",
            ),
            (
                self.depth_bridge_open_wait_count,
                "depth_bridge_open_wait_count",
            ),
            (
                self.last_depth_bridge_drain_recorded_wall_ms,
                "last_depth_bridge_drain_recorded_wall_ms",
            ),
            (
                self.last_depth_bridge_drain_recorded_monotonic_ns,
                "last_depth_bridge_drain_recorded_monotonic_ns",
            ),
        ):
            _validate_nonnegative_integer(value, name)
        _validate_positive_value(self.event_count, "event_count")
        _validate_positive_value(
            self.depth_bridge_event_count,
            "depth_bridge_event_count",
        )
        _validate_positive_value(
            self.depth_bridge_generation_started_count,
            "depth_bridge_generation_started_count",
        )
        _validate_positive_value(
            self.last_depth_bridge_drain_event_sequence,
            "last_depth_bridge_drain_event_sequence",
        )
        _validate_positive_value(
            self.finality_tail_ingest_seq,
            "finality_tail_ingest_seq",
        )
        if (
            self.source_gap_bounded_count > self.source_gap_open_count
            or self.unmatched_source_gap_open_count
            != self.source_gap_open_count - self.source_gap_bounded_count
        ):
            raise ValueError("V8 CLEAN closure SOURCE_GAP census is inconsistent")
        if self.event_count != (
            self.source_gap_open_count
            + self.source_gap_bounded_count
            + self.data_gap_count
            + self.void_count
            + self.depth_bridge_event_count
        ):
            raise ValueError("V8 CLEAN closure event census does not equal event_count")
        if (
            self.depth_bridge_generation_started_count
            != self.depth_bridge_generation_drained_count
            or self.depth_bridge_fatal_generation_count != 0
            or any(
                (
                    self.depth_bridge_open_generation_count,
                    self.depth_bridge_open_cycle_count,
                    self.depth_bridge_open_attempt_count,
                    self.depth_bridge_open_wait_count,
                )
            )
        ):
            raise ValueError("V8 CLEAN closure depth bridge is not cleanly drained")
        if self.unmatched_source_gap_open_count != 0 or self.void_count != 0:
            raise ValueError("V8 CLEAN closure rejects open SOURCE_GAP or VOID evidence")
        if type(self.finality_receipt) is not CaptureFinalityFenceReceiptV2:
            raise TypeError(
                "finality_receipt must be an exact CaptureFinalityFenceReceiptV2"
            )
        self.finality_receipt.__post_init__()
        if (
            self.finality_receipt_sha256 != self.finality_receipt.sha256
            or self.finality_prefix_proof_sha256
            != self.finality_receipt.prefix_proof_sha256
            or self.finality_tail_ingest_seq != self.finality_receipt.fence_ingest_seq
            or self.authority_sha256 != self.finality_receipt.authority_sha256
            or self.attempt_id != self.finality_receipt.attempt_id
            or self.qualification_id != self.finality_receipt.qualification_id
        ):
            raise ValueError("V8 CLEAN closure differs from its finality receipt")
        expected_wal_hash = hashlib.sha256(
            canonical_json_line(asdict(self.finality_receipt.wal_durability_binding))
        ).hexdigest()
        if (
            self.wal_durability_binding_sha256 != expected_wal_hash
            or self.block_root_binding_sha256
            != self.finality_receipt.grouped_block_root_binding_sha256
            or self.block_signing_authority_sha256
            != self.finality_receipt.block_signing_authority_sha256
        ):
            raise ValueError("V8 CLEAN closure storage authority differs")
        validate_websocket_route_cursor_closure_pair_v8(
            self.websocket_route_cursor_closure_pair,
            finality_receipt=self.finality_receipt,
        )
        if self.websocket_route_cursor_closure_pair_sha256 != (
            websocket_route_cursor_closure_pair_sha256_v8(
                self.websocket_route_cursor_closure_pair,
                finality_receipt=self.finality_receipt,
            )
        ):
            raise ValueError("V8 CLEAN closure WebSocket cursor hash differs")
        if type(self.depth_bridge_closure_entry) is not DepthBridgeCoordinatorClosureEntryV8:
            raise TypeError(
                "depth bridge closure entry must be an exact V8 projection"
            )
        self.depth_bridge_closure_entry.__post_init__()
        if self.depth_bridge_closure_entry_sha256 != (
            _intrinsic_depth_bridge_closure_entry_sha256_v8(
                self.depth_bridge_closure_entry
            )
        ):
            raise ValueError("V8 CLEAN closure depth bridge entry hash differs")
        _validate_intrinsic_depth_bridge_receipt_digest_v8(
            self.depth_bridge_closure_entry
        )
        public_cursor = self.websocket_route_cursor_closure_pair[1]
        bridge = self.depth_bridge_closure_entry
        if (
            public_cursor.route_id != "usdm_public"
            or bridge.session_id != self.session_id
            or bridge.protocol_hash != self.protocol_hash
            or bridge.plan_bundle_sha256 != self.plan_bundle_sha256
            or bridge.depth_plan_sha256 != self.depth_plan_sha256
            or public_cursor.session_id != self.session_id
            or public_cursor.process_boot_id != self.process_boot_id
            or public_cursor.plan_bundle_sha256 != self.plan_bundle_sha256
            or bridge.last_connection_id != public_cursor.connection_id
            or bridge.last_connection_generation != public_cursor.generation
        ):
            raise ValueError("V8 CLEAN closure bridge/public cursor lineage differs")
        if (
            public_cursor.stop_observed_wall_ms > bridge.close_wall_ms
            or public_cursor.stop_observed_monotonic_ns > bridge.close_monotonic_ns
            or bridge.close_monotonic_ns > self.finality_receipt.fence_monotonic_ns
            or self.seal_wall_ms < bridge.close_wall_ms
            or self.seal_wall_ms
            < self.finality_receipt.target_last_receipt_wall_ms
            or self.seal_monotonic_ns
            < self.finality_receipt.writer_observed_monotonic_ns
        ):
            raise ValueError("V8 CLEAN closure clock ordering is invalid")
        if (
            self.last_depth_bridge_drain_event_sequence
            != bridge.last_generation_drained_event_sequence
            or self.last_depth_bridge_drain_event_sha256
            != bridge.last_generation_drained_event_sha256
            or self.last_depth_bridge_drain_recorded_wall_ms
            != bridge.last_generation_drained_recorded_wall_ms
            or self.last_depth_bridge_drain_recorded_monotonic_ns
            != bridge.last_generation_drained_recorded_monotonic_ns
        ):
            raise ValueError("V8 CLEAN closure last drain locator differs")
        if any(
            value is not False
            for value in (
                self.qualification_complete_claimed,
                self.promoting,
                self.book_bridge_certified,
                self.m2_certified,
                self.order_execution_enabled,
            )
        ):
            raise ValueError("V8 infrastructure CLEAN cannot assert strategy authority")
        if self.closure_status != "CLEAN":
            raise ValueError("V8 capture closure seal status must be CLEAN")
        if self.schema_version != _CLEAN_CLOSURE_SEAL_SCHEMA_V8:
            raise ValueError("unsupported V8 capture clean-closure seal schema")

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(asdict(self))

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.encoded_line).hexdigest()


@dataclass(frozen=True, slots=True, init=False)
class PersistedCaptureCleanClosureSealReceiptV2:
    """Factory-only binding to the exact durable fixed-path CLEAN seal."""

    seal: CaptureCleanClosureSealV2
    canonical_path: str
    file_name: str
    seal_sha256: str
    byte_count: int
    file_device: int
    file_inode: int
    file_nlink: int
    schema_version: str
    _factory_token: object = field(repr=False, compare=False)

    def __init__(
        self,
        *,
        seal: CaptureCleanClosureSealV2,
        canonical_path: str,
        file_name: str,
        seal_sha256: str,
        byte_count: int,
        file_device: int,
        file_inode: int,
        file_nlink: int,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _PERSISTED_CLEAN_CLOSURE_FACTORY_TOKEN:
            raise TypeError(
                "PersistedCaptureCleanClosureSealReceiptV2 can only be created "
                "by the durable ledger owner"
            )
        object.__setattr__(self, "seal", seal)
        object.__setattr__(self, "canonical_path", canonical_path)
        object.__setattr__(self, "file_name", file_name)
        object.__setattr__(self, "seal_sha256", seal_sha256)
        object.__setattr__(self, "byte_count", byte_count)
        object.__setattr__(self, "file_device", file_device)
        object.__setattr__(self, "file_inode", file_inode)
        object.__setattr__(self, "file_nlink", file_nlink)
        object.__setattr__(
            self,
            "schema_version",
            _PERSISTED_CLEAN_CLOSURE_RECEIPT_SCHEMA,
        )
        object.__setattr__(self, "_factory_token", _factory_token)
        self.__post_init__()

    def __post_init__(self) -> None:
        if self._factory_token is not _PERSISTED_CLEAN_CLOSURE_FACTORY_TOKEN:
            raise ValueError("persisted clean-closure receipt lacks factory provenance")
        if type(self.seal) is not CaptureCleanClosureSealV2:
            raise TypeError("seal must be an exact CaptureCleanClosureSealV2")
        self.seal.__post_init__()
        if self.schema_version != _PERSISTED_CLEAN_CLOSURE_RECEIPT_SCHEMA:
            raise ValueError("unsupported persisted clean-closure receipt schema")
        expected_path = os.path.normcase(os.path.abspath(self.canonical_path))
        if self.canonical_path != expected_path:
            raise ValueError("clean-closure receipt path must be canonical and absolute")
        if self.file_name != _CLEAN_CLOSURE_SEAL_FILE:
            raise ValueError("clean-closure receipt file name is not canonical")
        if Path(self.canonical_path).name != self.file_name:
            raise ValueError("clean-closure receipt path and file name differ")
        _validate_sha256(self.seal_sha256, "seal_sha256")
        if self.seal_sha256 != self.seal.sha256:
            raise ValueError("persisted clean-closure hash differs from its seal")
        if type(self.byte_count) is not int or self.byte_count < 1:
            raise ValueError("persisted clean-closure byte count must be positive")
        if self.byte_count != len(self.seal.encoded_line):
            raise ValueError("persisted clean-closure byte count differs")
        for value, name in (
            (self.file_device, "file_device"),
            (self.file_inode, "file_inode"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if type(self.file_nlink) is not int or self.file_nlink != 1:
            raise ValueError("persisted clean-closure seal must bind one hard link")

    @property
    def encoded_line(self) -> bytes:
        return self.seal.encoded_line

    @property
    def sha256(self) -> str:
        document = {
            "schema_version": self.schema_version,
            "seal_sha256": self.seal_sha256,
            "canonical_path": self.canonical_path,
            "file_name": self.file_name,
            "byte_count": self.byte_count,
            # Native filesystem identities are opaque unsigned values, not
            # application arithmetic.  In particular, Windows can expose an
            # NTFS file ID above RFC 8785's exact JSON integer domain.  Decimal
            # strings preserve the complete identity on every supported host.
            "file_device": str(self.file_device),
            "file_inode": str(self.file_inode),
            "file_nlink": str(self.file_nlink),
        }
        return hashlib.sha256(
            _PERSISTED_CLEAN_CLOSURE_RECEIPT_DOMAIN
            + canonical_json_line(document)
        ).hexdigest()


@dataclass(frozen=True, slots=True, init=False)
class PersistedCaptureCleanClosureSealReceiptV8:
    """Factory-only binding to the exact durable fixed-path V8 CLEAN seal."""

    seal: CaptureCleanClosureSealV8
    canonical_path: str
    file_name: str
    seal_sha256: str
    byte_count: int
    file_device: int
    file_inode: int
    file_nlink: int
    schema_version: str
    _factory_token: object = field(repr=False, compare=False)

    def __init__(
        self,
        *,
        seal: CaptureCleanClosureSealV8,
        canonical_path: str,
        file_name: str,
        seal_sha256: str,
        byte_count: int,
        file_device: int,
        file_inode: int,
        file_nlink: int,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _PERSISTED_CLEAN_CLOSURE_FACTORY_TOKEN_V8:
            raise TypeError(
                "PersistedCaptureCleanClosureSealReceiptV8 can only be created "
                "by the durable ledger owner"
            )
        object.__setattr__(self, "seal", seal)
        object.__setattr__(self, "canonical_path", canonical_path)
        object.__setattr__(self, "file_name", file_name)
        object.__setattr__(self, "seal_sha256", seal_sha256)
        object.__setattr__(self, "byte_count", byte_count)
        object.__setattr__(self, "file_device", file_device)
        object.__setattr__(self, "file_inode", file_inode)
        object.__setattr__(self, "file_nlink", file_nlink)
        object.__setattr__(
            self,
            "schema_version",
            _PERSISTED_CLEAN_CLOSURE_RECEIPT_SCHEMA_V8,
        )
        object.__setattr__(self, "_factory_token", _factory_token)
        self.__post_init__()

    def __post_init__(self) -> None:
        if self._factory_token is not _PERSISTED_CLEAN_CLOSURE_FACTORY_TOKEN_V8:
            raise ValueError("persisted V8 CLEAN receipt lacks factory provenance")
        if type(self.seal) is not CaptureCleanClosureSealV8:
            raise TypeError("seal must be an exact CaptureCleanClosureSealV8")
        self.seal.__post_init__()
        if self.schema_version != _PERSISTED_CLEAN_CLOSURE_RECEIPT_SCHEMA_V8:
            raise ValueError("unsupported persisted V8 CLEAN receipt schema")
        expected_path = os.path.normcase(os.path.abspath(self.canonical_path))
        if self.canonical_path != expected_path:
            raise ValueError("V8 CLEAN receipt path must be canonical and absolute")
        if (
            self.file_name != _CLEAN_CLOSURE_SEAL_FILE
            or Path(self.canonical_path).name != self.file_name
        ):
            raise ValueError("V8 CLEAN receipt does not bind the fixed seal path")
        _validate_sha256(self.seal_sha256, "seal_sha256")
        if self.seal_sha256 != self.seal.sha256:
            raise ValueError("persisted V8 CLEAN hash differs from its seal")
        if type(self.byte_count) is not int or self.byte_count < 1:
            raise ValueError("persisted V8 CLEAN byte count must be positive")
        if self.byte_count != len(self.seal.encoded_line):
            raise ValueError("persisted V8 CLEAN byte count differs")
        for value, name in (
            (self.file_device, "file_device"),
            (self.file_inode, "file_inode"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if type(self.file_nlink) is not int or self.file_nlink != 1:
            raise ValueError("persisted V8 CLEAN seal must bind one hard link")

    @property
    def encoded_line(self) -> bytes:
        return self.seal.encoded_line

    @property
    def sha256(self) -> str:
        document = {
            "schema_version": self.schema_version,
            "seal_sha256": self.seal_sha256,
            "canonical_path": self.canonical_path,
            "file_name": self.file_name,
            "byte_count": self.byte_count,
            "file_device": str(self.file_device),
            "file_inode": str(self.file_inode),
            "file_nlink": str(self.file_nlink),
        }
        return hashlib.sha256(
            _PERSISTED_CLEAN_CLOSURE_RECEIPT_DOMAIN_V8
            + canonical_json_line(document)
        ).hexdigest()


type _CaptureCleanClosureSeal = CaptureCleanClosureSealV2 | CaptureCleanClosureSealV8
type _PersistedCaptureCleanClosureSealReceipt = (
    PersistedCaptureCleanClosureSealReceiptV2
    | PersistedCaptureCleanClosureSealReceiptV8
)


FaultHook = Callable[[str], None]
WallClockMs = Callable[[], int]
MonotonicClockNs = Callable[[], int]


class CaptureIntegrityLedgerV2:
    """Bounded ledger for local gaps, upstream gaps, and finalized-block VOID evidence.

    The concurrency hierarchy is strict: acquire ``WriterLease.operation_guard()``
    before ``self._lock`` whenever an operation needs both.  The lease serializes
    the complete storage authority while the ledger lock protects this instance's
    in-memory tip.  Reversing that order can deadlock a late append against a
    session closure that already owns the lease.
    """

    def __init__(
        self,
        directory: str | Path,
        *,
        authority: WalAuthorityV2,
        block_directory: str | Path,
        block_root_binding: StorageRootBindingV2,
        block_signing_authority: BlockSigningAuthorityV2,
        block_policy: BlockPolicyV2,
        block_stream_group_id: str,
        block_segment_id: str,
        maximum_total_bytes: int,
        emergency_reserve_bytes: int,
        max_events: int,
        failure_domain_id: str,
        writer_lease: WriterLease | None = None,
        wall_clock_ms: WallClockMs = lambda: time.time_ns() // 1_000_000,
        monotonic_clock_ns: MonotonicClockNs = time.monotonic_ns,
        fault_hook: FaultHook | None = None,
    ) -> None:
        if writer_lease is not None and type(writer_lease) is not WriterLease:
            raise TypeError("writer_lease must be an exact WriterLease or None")
        if writer_lease is not None:
            writer_lease.assert_held()
        if emergency_reserve_bytes < 1024:
            raise ValueError("emergency_reserve_bytes must be at least 1024")
        if maximum_total_bytes <= emergency_reserve_bytes:
            raise ValueError("maximum_total_bytes must exceed emergency reserve")
        if type(max_events) is not int or not 1 <= max_events <= _MAX_EVENT_SEQUENCE:
            raise ValueError("max_events is outside the sealed filename bound")
        if block_root_binding.storage_kind != "GROUPED_BLOCK":
            raise ValueError("integrity ledger requires a GROUPED_BLOCK root binding")
        if block_root_binding.authority_sha256 != authority.sha256:
            raise ValueError("block-root and ledger authorities differ")
        block_contract_sha256 = hashlib.sha256(
            canonical_json_line(
                grouped_block_root_contract_v2(
                    block_policy,
                    block_signing_authority,
                    block_stream_group_id,
                    block_segment_id,
                )
            )
        ).hexdigest()
        if block_root_binding.contract_sha256 != block_contract_sha256:
            raise ValueError(
                "block-root contract differs from the ledger public-read authority"
            )
        _validate_identity(failure_domain_id, "failure_domain_id")

        self.directory = Path(directory)
        self.authority = authority
        self.block_directory = _normalized_resolved_path(block_directory)
        self.block_root_binding = block_root_binding
        self.block_signing_authority = block_signing_authority
        self.block_policy = block_policy
        self.block_stream_group_id = block_stream_group_id
        self.block_segment_id = block_segment_id
        self.maximum_total_bytes = maximum_total_bytes
        self.emergency_reserve_bytes = emergency_reserve_bytes
        self.max_events = max_events
        self.writer_lease = writer_lease
        self._wall_clock_ms = wall_clock_ms
        self._monotonic_clock_ns = monotonic_clock_ns
        self._fault_hook = fault_hook
        self._lock = threading.RLock()
        self._failed: BaseException | None = None
        self._block_root_binding_bytes = canonical_json_line(asdict(block_root_binding))
        self.block_root_binding_sha256 = hashlib.sha256(
            self._block_root_binding_bytes
        ).hexdigest()
        self.block_root_path_sha256 = _root_path_sha256(self.block_directory)
        _verify_exact_file(
            self.block_directory / "storage-root-binding.json",
            self._block_root_binding_bytes,
            "grouped-block root binding",
        )
        self._opened_block_root_identity = inspect_storage_root_opened_identity_v2(
            self.block_directory,
            self.block_root_binding,
        )

        self.directory.mkdir(parents=True, exist_ok=True)
        ledger_root_contract = capture_integrity_ledger_root_contract_v2(
            block_root_binding=block_root_binding,
            block_directory=self.block_directory,
            block_signing_authority=block_signing_authority,
            max_events=max_events,
        )
        try:
            self.root_binding = bind_storage_root_v2(
                self.directory,
                storage_kind="CAPTURE_INTEGRITY_LEDGER",
                root_role="APPEND_ONLY_PRIMARY",
                failure_domain_id=failure_domain_id,
                authority_sha256=authority.sha256,
                contract=ledger_root_contract,
            )
        except StorageRootBindingError as exc:
            raise CaptureIntegrityLedgerIntegrityError(str(exc)) from exc
        self._opened_root_identity = inspect_storage_root_opened_identity_v2(
            self.directory,
            self.root_binding,
        )
        self._ledger_root_binding_bytes = canonical_json_line(asdict(self.root_binding))
        self.ledger_root_binding_sha256 = hashlib.sha256(
            self._ledger_root_binding_bytes
        ).hexdigest()
        self.ledger_root_path_sha256 = _root_path_sha256(
            _normalized_resolved_path(self.directory)
        )
        self._known_disk_bytes = _directory_size(self.directory)
        if self._known_disk_bytes > maximum_total_bytes:
            raise CaptureIntegrityLedgerCapacityError(
                "integrity ledger already exceeds its configured quota"
            )
        with self._writer_lease_operation():
            self._events = self._load_events_and_recover_partial()
            self._clean_closure_receipt: (
                PersistedCaptureCleanClosureSealReceiptV2
                | PersistedCaptureCleanClosureSealReceiptV8
                | None
            ) = (
                self._load_clean_closure_seal_and_recover_partial_unlocked()
            )
        self._clean_closure_claimed = self._clean_closure_receipt is not None
        unmatched_source_gaps = _unmatched_source_gap_count(self._events)
        bridge_census = _depth_bridge_census(self._events)
        reserved_event_slots = (
            unmatched_source_gaps
            + bridge_census.open_terminal_reservation_count
        )
        if len(self._events) + reserved_event_slots > self.max_events:
            raise CaptureIntegrityLedgerCapacityError(
                "integrity ledger cannot preserve all open terminal-event slots"
            )
        reserved_bytes = (
            unmatched_source_gaps
            * _SOURCE_GAP_BOUNDED_RESERVE_BYTES
            + bridge_census.open_terminal_reservation_count
            * DEPTH_BRIDGE_TERMINAL_RESERVE_BYTES_V8
        )
        if self._clean_closure_receipt is None:
            reserved_bytes += _CLEAN_CLOSURE_SEAL_RESERVE_BYTES
        if (
            self._known_disk_bytes + self.emergency_reserve_bytes + reserved_bytes
            > self.maximum_total_bytes
        ):
            raise CaptureIntegrityLedgerCapacityError(
                "integrity ledger cannot preserve all open terminal-event capacity"
            )

    @property
    def opened_directory(self) -> Path:
        return Path(self._opened_root_identity.canonical_path)

    @property
    def opened_root_identity(self) -> StorageRootOpenedIdentityV2:
        return self._opened_root_identity

    @property
    def opened_block_root_identity(self) -> StorageRootOpenedIdentityV2:
        return self._opened_block_root_identity

    def assert_running_healthy_and_writer_open_v2(self) -> None:
        with self._writer_lease_operation():
            with self._lock:
                self._raise_if_failed()
                self._verify_bound_roots()
                self._refresh_clean_closure_seal_unlocked()
                if self._clean_closure_claimed:
                    raise CaptureIntegrityLedgerError(
                        "integrity ledger is durably closed by a CLEAN seal"
                    )

    @property
    def events(self) -> tuple[CaptureIntegrityEventV2, ...]:
        with self._lock:
            return tuple(_detached_event(event) for event in self._events)

    @property
    def next_event_sequence(self) -> int:
        with self._lock:
            return len(self._events) + 1

    @property
    def last_event_sha256(self) -> str | None:
        with self._lock:
            return self._events[-1].sha256 if self._events else None

    def seal_clean_closure_v2(
        self,
        *,
        promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
        finality_receipt: CaptureFinalityFenceReceiptV2,
        wal_writer: MirroredWalWriterV2,
        block_writer: GroupedBlockWriterV2,
        session_id: str,
        process_boot_id: str,
        seal_wall_ms: int,
        seal_monotonic_ns: int,
    ) -> PersistedCaptureCleanClosureSealReceiptV2:
        """Atomically persist one exact CLEAN terminal state for this ledger."""

        _require_v2_clean_closure_plan_authority(
            promoting_plans,
            authority_sha256=self.authority.plan_sha256,
        )
        _require_exact_clean_closure_inputs_v2(
            finality_receipt=finality_receipt,
            wal_writer=wal_writer,
            block_writer=block_writer,
        )
        _validate_identity(session_id, "session_id")
        _validate_identity(process_boot_id, "process_boot_id")
        _validate_nonnegative_value(seal_wall_ms, "seal_wall_ms")
        _validate_nonnegative_value(seal_monotonic_ns, "seal_monotonic_ns")
        if self.writer_lease is None:
            raise CaptureIntegrityLedgerError(
                "CLEAN closure requires an exact held WriterLease"
            )
        with self._writer_lease_operation():
            with self._lock:
                self._raise_if_failed()
                self._verify_bound_roots()
                self._refresh_events_unlocked()
                self._refresh_clean_closure_seal_unlocked()
                if self._clean_closure_claimed:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "CLEAN closure issuance was already consumed"
                    )
                if _depth_bridge_census(self._events).event_count:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "V2 CLEAN closure rejects DEPTH_BRIDGE evidence"
                    )
                self._clean_closure_claimed = True
                self._verify_exact_clean_closure_inputs_unlocked(
                    finality_receipt=finality_receipt,
                    wal_writer=wal_writer,
                    block_writer=block_writer,
                    session_id=session_id,
                    process_boot_id=process_boot_id,
                    require_block_terminal=False,
                    allow_verification_only_wal=False,
                )
                block_terminal = block_writer.terminalize_clean_tail_v2(
                    finality_receipt
                )
                terminal_sha256 = self._verify_exact_clean_closure_inputs_unlocked(
                    finality_receipt=finality_receipt,
                    wal_writer=wal_writer,
                    block_writer=block_writer,
                    session_id=session_id,
                    process_boot_id=process_boot_id,
                    require_block_terminal=True,
                    allow_verification_only_wal=False,
                )
                if terminal_sha256 != block_terminal.sha256:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "CLEAN closure block terminal hash changed after issuance"
                    )
                census = _integrity_event_census(self._events)
                seal = CaptureCleanClosureSealV2(
                    session_id=session_id,
                    process_boot_id=process_boot_id,
                    seal_wall_ms=seal_wall_ms,
                    seal_monotonic_ns=seal_monotonic_ns,
                    authority_sha256=self.authority.sha256,
                    attempt_id=self.authority.attempt_id,
                    qualification_id=finality_receipt.qualification_id,
                    ledger_root_binding_sha256=self.ledger_root_binding_sha256,
                    ledger_root_path_sha256=self.ledger_root_path_sha256,
                    block_root_binding_sha256=self.block_root_binding_sha256,
                    block_root_path_sha256=self.block_root_path_sha256,
                    block_signing_authority_sha256=(
                        self.block_signing_authority.sha256
                    ),
                    block_clean_tail_terminal_sha256=block_terminal.sha256,
                    wal_durability_binding_sha256=hashlib.sha256(
                        canonical_json_line(asdict(wal_writer.durability_binding))
                    ).hexdigest(),
                    event_count=len(self._events),
                    event_tip_sha256=(
                        self._events[-1].sha256 if self._events else None
                    ),
                    source_gap_open_count=census.source_gap_open_count,
                    source_gap_bounded_count=census.source_gap_bounded_count,
                    unmatched_source_gap_open_count=(
                        census.unmatched_source_gap_open_count
                    ),
                    data_gap_count=census.data_gap_count,
                    void_count=census.void_count,
                    finality_receipt=finality_receipt,
                    finality_receipt_sha256=finality_receipt.sha256,
                    finality_prefix_proof_sha256=(
                        finality_receipt.prefix_proof_sha256
                    ),
                    finality_tail_ingest_seq=finality_receipt.fence_ingest_seq,
                )
                return self._persist_clean_closure_seal_unlocked(
                    seal,
                    wal_writer=wal_writer,
                    block_writer=block_writer,
                )

    def verify_current_clean_closure_seal_v2(
        self,
        *,
        promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
        wal_writer: MirroredWalWriterV2,
        block_writer: GroupedBlockWriterV2,
        session_id: str,
        process_boot_id: str,
    ) -> PersistedCaptureCleanClosureSealReceiptV2:
        """Reprove the fixed seal, full ledger, and exact current WAL/block tail."""

        _require_v2_clean_closure_plan_authority(
            promoting_plans,
            authority_sha256=self.authority.plan_sha256,
        )
        _require_exact_clean_closure_owners_v2(
            wal_writer=wal_writer,
            block_writer=block_writer,
        )
        _validate_identity(session_id, "session_id")
        _validate_identity(process_boot_id, "process_boot_id")
        if self.writer_lease is None:
            raise CaptureIntegrityLedgerError(
                "CLEAN closure verification requires an exact held WriterLease"
            )
        with self._writer_lease_operation():
            with self._lock:
                self._raise_if_failed()
                self._verify_bound_roots()
                self._refresh_events_unlocked()
                self._refresh_clean_closure_seal_unlocked()
                persisted = self._clean_closure_receipt
                if persisted is None:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "durable CLEAN closure seal is absent"
                    )
                if type(persisted) is not PersistedCaptureCleanClosureSealReceiptV2:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "V2 verifier rejects a V8 CLEAN closure seal"
                    )
                if (persisted.seal.session_id, persisted.seal.process_boot_id) != (
                    session_id,
                    process_boot_id,
                ):
                    raise CaptureIntegrityLedgerIntegrityError(
                        "requested session or process boot differs from the CLEAN seal"
                    )
                terminal_sha256 = self._verify_exact_clean_closure_inputs_unlocked(
                    finality_receipt=persisted.seal.finality_receipt,
                    wal_writer=wal_writer,
                    block_writer=block_writer,
                    session_id=session_id,
                    process_boot_id=process_boot_id,
                    require_block_terminal=True,
                    allow_verification_only_wal=True,
                )
                if (
                    terminal_sha256
                    != persisted.seal.block_clean_tail_terminal_sha256
                ):
                    raise CaptureIntegrityLedgerIntegrityError(
                        "CLEAN closure seal differs from the current block terminal"
                    )
                self._validate_clean_closure_seal_against_ledger_unlocked(
                    persisted.seal
                )
                _assert_persisted_clean_closure_file_current(persisted)
                return persisted

    def seal_clean_closure_v8(
        self,
        *,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        depth_plan: ProvisionalDepthRestQualificationPlanV8,
        depth_bridge_close_receipt: DepthBridgeCoordinatorCleanCloseReceiptV8,
        finalized_websocket_cursor_pair: FinalizedWebSocketRouteCursorPairV8,
        finality_receipt: CaptureFinalityFenceReceiptV2,
        wal_writer: MirroredWalWriterV2,
        block_writer: GroupedBlockWriterV2,
        session_id: str,
        process_boot_id: str,
        seal_wall_ms: int,
        seal_monotonic_ns: int,
    ) -> PersistedCaptureCleanClosureSealReceiptV8:
        """Persist the exact four-plan V8 infrastructure-CLEAN terminal state."""

        _require_v8_clean_closure_plan_authority(
            promoting_plans,
            depth_plan=depth_plan,
            authority_sha256=self.authority.plan_sha256,
        )
        _require_exact_clean_closure_inputs_v8(
            promoting_plans=promoting_plans,
            depth_plan=depth_plan,
            depth_bridge_close_receipt=depth_bridge_close_receipt,
            finalized_websocket_cursor_pair=finalized_websocket_cursor_pair,
            finality_receipt=finality_receipt,
            wal_writer=wal_writer,
            block_writer=block_writer,
        )
        _validate_identity(session_id, "session_id")
        _validate_identity(process_boot_id, "process_boot_id")
        _validate_nonnegative_value(seal_wall_ms, "seal_wall_ms")
        _validate_nonnegative_value(seal_monotonic_ns, "seal_monotonic_ns")
        websocket_projection = websocket_route_cursor_closure_pair_v8(
            finalized_websocket_cursor_pair,
            finality_receipt=finality_receipt,
            promoting_plans=promoting_plans,
        )
        websocket_projection_sha256 = (
            websocket_route_cursor_closure_pair_sha256_v8(
                websocket_projection,
                finality_receipt=finality_receipt,
                promoting_plans=promoting_plans,
            )
        )
        bridge_entry = depth_bridge_coordinator_closure_entry_v8(
            depth_bridge_close_receipt,
            promoting_plans=promoting_plans,
            depth_plan=depth_plan,
        )
        bridge_entry_sha256 = depth_bridge_coordinator_closure_entry_sha256_v8(
            bridge_entry,
            promoting_plans=promoting_plans,
            depth_plan=depth_plan,
        )
        if self.writer_lease is None:
            raise CaptureIntegrityLedgerError(
                "V8 CLEAN closure requires an exact held WriterLease"
            )
        with self._writer_lease_operation():
            with self._lock:
                self._raise_if_failed()
                self._verify_bound_roots()
                self._refresh_events_unlocked()
                self._refresh_clean_closure_seal_unlocked()
                if self._clean_closure_claimed:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "CLEAN closure issuance was already consumed"
                    )
                self._verify_exact_clean_closure_inputs_unlocked(
                    finality_receipt=finality_receipt,
                    wal_writer=wal_writer,
                    block_writer=block_writer,
                    session_id=session_id,
                    process_boot_id=process_boot_id,
                    require_block_terminal=False,
                    allow_verification_only_wal=False,
                    allow_depth_bridge_v8=True,
                )
                self._validate_v8_live_closure_inputs_against_ledger_unlocked(
                    promoting_plans=promoting_plans,
                    depth_plan=depth_plan,
                    depth_bridge_close_receipt=depth_bridge_close_receipt,
                    websocket_projection=websocket_projection,
                    finality_receipt=finality_receipt,
                    session_id=session_id,
                    process_boot_id=process_boot_id,
                    seal_wall_ms=seal_wall_ms,
                    seal_monotonic_ns=seal_monotonic_ns,
                )
                self._clean_closure_claimed = True
                block_terminal = block_writer.terminalize_clean_tail_v2(
                    finality_receipt
                )
                terminal_sha256 = self._verify_exact_clean_closure_inputs_unlocked(
                    finality_receipt=finality_receipt,
                    wal_writer=wal_writer,
                    block_writer=block_writer,
                    session_id=session_id,
                    process_boot_id=process_boot_id,
                    require_block_terminal=True,
                    allow_verification_only_wal=False,
                    allow_depth_bridge_v8=True,
                )
                if terminal_sha256 != block_terminal.sha256:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "V8 CLEAN closure block terminal hash changed after issuance"
                    )
                self._validate_v8_live_closure_inputs_against_ledger_unlocked(
                    promoting_plans=promoting_plans,
                    depth_plan=depth_plan,
                    depth_bridge_close_receipt=depth_bridge_close_receipt,
                    websocket_projection=websocket_projection,
                    finality_receipt=finality_receipt,
                    session_id=session_id,
                    process_boot_id=process_boot_id,
                    seal_wall_ms=seal_wall_ms,
                    seal_monotonic_ns=seal_monotonic_ns,
                )
                census = _integrity_event_census(self._events)
                last_drain = _last_depth_bridge_drain_event(self._events)
                seal = CaptureCleanClosureSealV8(
                    session_id=session_id,
                    process_boot_id=process_boot_id,
                    protocol_hash=self.authority.protocol_sha256,
                    plan_bundle_sha256=provisional_promoting_plan_sha256_v8(
                        promoting_plans
                    ),
                    depth_plan_sha256=bridge_entry.depth_plan_sha256,
                    seal_wall_ms=seal_wall_ms,
                    seal_monotonic_ns=seal_monotonic_ns,
                    authority_sha256=self.authority.sha256,
                    attempt_id=self.authority.attempt_id,
                    qualification_id=finality_receipt.qualification_id,
                    ledger_root_binding_sha256=self.ledger_root_binding_sha256,
                    ledger_root_path_sha256=self.ledger_root_path_sha256,
                    block_root_binding_sha256=self.block_root_binding_sha256,
                    block_root_path_sha256=self.block_root_path_sha256,
                    block_signing_authority_sha256=(
                        self.block_signing_authority.sha256
                    ),
                    block_clean_tail_terminal_sha256=block_terminal.sha256,
                    wal_durability_binding_sha256=hashlib.sha256(
                        canonical_json_line(asdict(wal_writer.durability_binding))
                    ).hexdigest(),
                    event_count=len(self._events),
                    event_tip_sha256=self._events[-1].sha256,
                    source_gap_open_count=census.source_gap_open_count,
                    source_gap_bounded_count=census.source_gap_bounded_count,
                    unmatched_source_gap_open_count=(
                        census.unmatched_source_gap_open_count
                    ),
                    data_gap_count=census.data_gap_count,
                    void_count=census.void_count,
                    depth_bridge_event_count=census.depth_bridge_event_count,
                    depth_bridge_generation_started_count=(
                        census.depth_bridge_generation_started_count
                    ),
                    depth_bridge_generation_drained_count=(
                        census.depth_bridge_generation_drained_count
                    ),
                    depth_bridge_fatal_generation_count=(
                        census.depth_bridge_fatal_generation_count
                    ),
                    depth_bridge_trigger_count=census.depth_bridge_trigger_count,
                    depth_bridge_cycle_count=census.depth_bridge_cycle_count,
                    depth_bridge_failed_cycle_count=(
                        census.depth_bridge_failed_cycle_count
                    ),
                    depth_bridge_open_generation_count=(
                        census.depth_bridge_open_generation_count
                    ),
                    depth_bridge_open_cycle_count=(
                        census.depth_bridge_open_cycle_count
                    ),
                    depth_bridge_open_attempt_count=(
                        census.depth_bridge_open_attempt_count
                    ),
                    depth_bridge_open_wait_count=(
                        census.depth_bridge_open_wait_count
                    ),
                    last_depth_bridge_drain_event_sequence=(
                        last_drain.event_sequence
                    ),
                    last_depth_bridge_drain_event_sha256=last_drain.sha256,
                    last_depth_bridge_drain_recorded_wall_ms=(
                        last_drain.recorded_wall_ms
                    ),
                    last_depth_bridge_drain_recorded_monotonic_ns=(
                        last_drain.recorded_monotonic_ns
                    ),
                    finality_receipt=finality_receipt,
                    finality_receipt_sha256=finality_receipt.sha256,
                    finality_prefix_proof_sha256=(
                        finality_receipt.prefix_proof_sha256
                    ),
                    finality_tail_ingest_seq=finality_receipt.fence_ingest_seq,
                    websocket_route_cursor_closure_pair=websocket_projection,
                    websocket_route_cursor_closure_pair_sha256=(
                        websocket_projection_sha256
                    ),
                    depth_bridge_closure_entry=bridge_entry,
                    depth_bridge_closure_entry_sha256=bridge_entry_sha256,
                    qualification_complete_claimed=False,
                    promoting=False,
                    book_bridge_certified=False,
                    m2_certified=False,
                    order_execution_enabled=False,
                )
                return self._persist_clean_closure_seal_v8_unlocked(
                    seal,
                    promoting_plans=promoting_plans,
                    depth_plan=depth_plan,
                    wal_writer=wal_writer,
                    block_writer=block_writer,
                )

    def verify_current_clean_closure_seal_v8(
        self,
        *,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        depth_plan: ProvisionalDepthRestQualificationPlanV8,
        wal_writer: MirroredWalWriterV2,
        block_writer: GroupedBlockWriterV2,
        session_id: str,
        process_boot_id: str,
    ) -> PersistedCaptureCleanClosureSealReceiptV8:
        """Reprove a durable V8 seal from persisted projections after restart."""

        _require_v8_clean_closure_plan_authority(
            promoting_plans,
            depth_plan=depth_plan,
            authority_sha256=self.authority.plan_sha256,
        )
        _require_exact_clean_closure_owners_v2(
            wal_writer=wal_writer,
            block_writer=block_writer,
        )
        _validate_identity(session_id, "session_id")
        _validate_identity(process_boot_id, "process_boot_id")
        if self.writer_lease is None:
            raise CaptureIntegrityLedgerError(
                "V8 CLEAN verification requires an exact held WriterLease"
            )
        with self._writer_lease_operation():
            with self._lock:
                self._raise_if_failed()
                self._verify_bound_roots()
                self._refresh_events_unlocked()
                self._refresh_clean_closure_seal_unlocked()
                persisted = self._clean_closure_receipt
                if type(persisted) is not PersistedCaptureCleanClosureSealReceiptV8:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "durable V8 CLEAN closure seal is absent or wrong-version"
                    )
                seal = persisted.seal
                if (seal.session_id, seal.process_boot_id) != (
                    session_id,
                    process_boot_id,
                ):
                    raise CaptureIntegrityLedgerIntegrityError(
                        "requested session or process boot differs from the V8 seal"
                    )
                terminal_sha256 = self._verify_exact_clean_closure_inputs_unlocked(
                    finality_receipt=seal.finality_receipt,
                    wal_writer=wal_writer,
                    block_writer=block_writer,
                    session_id=session_id,
                    process_boot_id=process_boot_id,
                    require_block_terminal=True,
                    allow_verification_only_wal=True,
                    allow_depth_bridge_v8=True,
                )
                if terminal_sha256 != seal.block_clean_tail_terminal_sha256:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "V8 CLEAN seal differs from the current block terminal"
                    )
                self._validate_clean_closure_seal_against_ledger_unlocked(seal)
                _validate_v8_seal_exact_plan_bindings(
                    seal,
                    promoting_plans=promoting_plans,
                    depth_plan=depth_plan,
                )
                _assert_persisted_clean_closure_file_current(persisted)
                return persisted

    def _persist_clean_closure_seal_unlocked(
        self,
        seal: CaptureCleanClosureSealV2,
        *,
        wal_writer: MirroredWalWriterV2,
        block_writer: GroupedBlockWriterV2,
    ) -> PersistedCaptureCleanClosureSealReceiptV2:
        encoded = seal.encoded_line
        if len(encoded) > _CLEAN_CLOSURE_SEAL_RESERVE_BYTES:
            raise CaptureIntegrityLedgerCapacityError(
                "CLEAN closure seal exceeds its reserved byte bound"
            )
        self._ensure_capacity(len(encoded))
        final_path = self.directory / _CLEAN_CLOSURE_SEAL_FILE
        partial_path = self.directory / _CLEAN_CLOSURE_SEAL_PARTIAL_FILE
        if final_path.exists() or partial_path.exists():
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure seal path already exists"
            )
        try:
            self._call_fault("before_clean_closure_seal_write")
            with partial_path.open("xb", buffering=0) as handle:
                written = handle.write(encoded)
                if written != len(encoded):
                    raise CaptureIntegrityLedgerIntegrityError(
                        "CLEAN closure seal short write"
                    )
                self._call_fault("after_clean_closure_seal_write")
                os.fsync(handle.fileno())
            self._known_disk_bytes += len(encoded)
            self._call_fault("after_clean_closure_seal_fsync")
            os.replace(partial_path, final_path)
            self._call_fault("after_clean_closure_seal_rename")
            _fsync_parent(final_path)
            self._call_fault("after_clean_closure_seal_parent_fsync")
            _fsync_path(final_path)
            self._verify_bound_roots()
            self._refresh_events_unlocked()
            terminal_sha256 = self._verify_exact_clean_closure_inputs_unlocked(
                finality_receipt=seal.finality_receipt,
                wal_writer=wal_writer,
                block_writer=block_writer,
                session_id=seal.session_id,
                process_boot_id=seal.process_boot_id,
                require_block_terminal=True,
                allow_verification_only_wal=False,
            )
            if terminal_sha256 != seal.block_clean_tail_terminal_sha256:
                raise CaptureIntegrityLedgerIntegrityError(
                    "CLEAN closure seal differs from the current block terminal"
                )
            self._validate_clean_closure_seal_against_ledger_unlocked(seal)
            persisted = _persisted_clean_closure_receipt(final_path, seal)
            if type(persisted) is not PersistedCaptureCleanClosureSealReceiptV2:
                raise CaptureIntegrityLedgerIntegrityError(
                    "V2 CLEAN persistence returned a wrong-version receipt"
                )
            _assert_persisted_clean_closure_file_current(persisted)
            self._clean_closure_receipt = persisted
            return persisted
        except BaseException as exc:
            self._failed = exc
            raise

    def _persist_clean_closure_seal_v8_unlocked(
        self,
        seal: CaptureCleanClosureSealV8,
        *,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        depth_plan: ProvisionalDepthRestQualificationPlanV8,
        wal_writer: MirroredWalWriterV2,
        block_writer: GroupedBlockWriterV2,
    ) -> PersistedCaptureCleanClosureSealReceiptV8:
        encoded = seal.encoded_line
        if len(encoded) > _CLEAN_CLOSURE_SEAL_RESERVE_BYTES:
            raise CaptureIntegrityLedgerCapacityError(
                "V8 CLEAN closure seal exceeds its reserved byte bound"
            )
        self._ensure_capacity(len(encoded))
        final_path = self.directory / _CLEAN_CLOSURE_SEAL_FILE
        partial_path = self.directory / _CLEAN_CLOSURE_SEAL_PARTIAL_FILE
        if final_path.exists() or partial_path.exists():
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure seal path already exists"
            )
        try:
            self._call_fault("before_clean_closure_seal_write")
            with partial_path.open("xb", buffering=0) as handle:
                written = handle.write(encoded)
                if written != len(encoded):
                    raise CaptureIntegrityLedgerIntegrityError(
                        "V8 CLEAN closure seal short write"
                    )
                self._call_fault("after_clean_closure_seal_write")
                os.fsync(handle.fileno())
            self._known_disk_bytes += len(encoded)
            self._call_fault("after_clean_closure_seal_fsync")
            os.replace(partial_path, final_path)
            self._call_fault("after_clean_closure_seal_rename")
            _fsync_parent(final_path)
            self._call_fault("after_clean_closure_seal_parent_fsync")
            _fsync_path(final_path)
            self._verify_bound_roots()
            self._refresh_events_unlocked()
            terminal_sha256 = self._verify_exact_clean_closure_inputs_unlocked(
                finality_receipt=seal.finality_receipt,
                wal_writer=wal_writer,
                block_writer=block_writer,
                session_id=seal.session_id,
                process_boot_id=seal.process_boot_id,
                require_block_terminal=True,
                allow_verification_only_wal=False,
                allow_depth_bridge_v8=True,
            )
            if terminal_sha256 != seal.block_clean_tail_terminal_sha256:
                raise CaptureIntegrityLedgerIntegrityError(
                    "V8 CLEAN seal differs from the current block terminal"
                )
            self._validate_clean_closure_seal_against_ledger_unlocked(seal)
            _validate_v8_seal_exact_plan_bindings(
                seal,
                promoting_plans=promoting_plans,
                depth_plan=depth_plan,
            )
            persisted = _persisted_clean_closure_receipt(final_path, seal)
            if type(persisted) is not PersistedCaptureCleanClosureSealReceiptV8:
                raise CaptureIntegrityLedgerIntegrityError(
                    "V8 CLEAN persistence returned a wrong-version receipt"
                )
            _assert_persisted_clean_closure_file_current(persisted)
            self._clean_closure_receipt = persisted
            return persisted
        except BaseException as exc:
            self._failed = exc
            raise

    def assert_finalized_prefix_not_void_v2(
        self,
        reference: FinalizedBlockReferenceV2,
    ) -> None:
        """Fail closed if the trusted ledger currently VOIDs the block prefix.

        A VOID on an earlier block poisons every later hash-chain descendant even
        if the physical bytes are subsequently restored.  The disk ledger is
        reread on every call so a certificate cannot rely on a stale in-memory
        event tuple.
        """

        with self._writer_lease_operation():
            with self._lock:
                self._assert_finalized_prefix_not_void_unlocked_v2(reference)

    def _assert_finalized_prefix_not_void_unlocked_v2(
        self,
        reference: FinalizedBlockReferenceV2,
    ) -> None:
        """Check the prefix while the outer lease guard and ``self._lock`` are held."""

        self._raise_if_failed()
        self._verify_bound_roots()
        if reference.authority_sha256 != self.authority.sha256:
            raise CaptureIntegrityLedgerIntegrityError(
                "finalized-block reference authority differs"
            )
        if (
            reference.block_root_binding_sha256 != self.block_root_binding_sha256
            or reference.block_root_path_sha256 != self.block_root_path_sha256
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "finalized-block reference names a different grouped-block root"
            )
        self._verify_block_signature(
            block_hash=reference.block_hash,
            writer_key_id=reference.writer_key_id,
            writer_ed25519_signature=reference.writer_ed25519_signature,
            signing_authority_sha256=reference.signing_authority_sha256,
        )
        self._refresh_events_unlocked()
        for event in self._events:
            if event.event_type != "VOID":
                continue
            payload = _typed_payload(event.event_type, event.payload)
            assert isinstance(payload, VoidFinalizedBlockPayloadV2)
            if payload.block_sequence <= reference.block_sequence:
                raise CaptureIntegrityLedgerIntegrityError(
                    "finalized block prefix has current VOID evidence"
                )

    def append_data_gap(
        self,
        *,
        first_missing_ingest_seq: int,
        last_missing_ingest_seq: int,
        receipt_wall_lower_bound_ms: int,
        receipt_wall_upper_bound_ms: int,
        receipt_monotonic_lower_bound_ns: int,
        receipt_monotonic_upper_bound_ns: int,
        cause: DataGapCauseV2,
        source_component: str,
        evidence_sha256: str,
    ) -> CaptureIntegrityEventV2:
        if not isinstance(cause, DataGapCauseV2):
            raise ValueError("cause must be a DataGapCauseV2 value")
        if (
            type(first_missing_ingest_seq) is not int
            or type(last_missing_ingest_seq) is not int
        ):
            raise ValueError("DATA_GAP ingest bounds must be integers")
        payload = DataGapPayloadV2(
            first_missing_ingest_seq=first_missing_ingest_seq,
            last_missing_ingest_seq=last_missing_ingest_seq,
            missing_count=last_missing_ingest_seq - first_missing_ingest_seq + 1,
            receipt_wall_lower_bound_ms=receipt_wall_lower_bound_ms,
            receipt_wall_upper_bound_ms=receipt_wall_upper_bound_ms,
            receipt_monotonic_lower_bound_ns=receipt_monotonic_lower_bound_ns,
            receipt_monotonic_upper_bound_ns=receipt_monotonic_upper_bound_ns,
            cause=cause.value,
            source_component=source_component,
            evidence_sha256=evidence_sha256,
        )
        return self._append("DATA_GAP", asdict(payload))

    def append_depth_bridge_v8(
        self,
        payload: DepthBridgeEvidencePayloadV8,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        depth_plan: ProvisionalDepthRestQualificationPlanV8,
    ) -> CaptureIntegrityEventV2:
        """Fsync one strict qualification-only bridge lifecycle transition."""

        if type(payload) is not DepthBridgeEvidencePayloadV8:
            raise TypeError(
                "payload must be an exact DepthBridgeEvidencePayloadV8"
            )
        try:
            validate_depth_bridge_evidence_payload_v8(
                payload,
                promoting_plans=promoting_plans,
                depth_plan=depth_plan,
            )
        except (TypeError, ValueError) as exc:
            raise CaptureIntegrityLedgerIntegrityError(
                "DEPTH_BRIDGE evidence differs from the exact V8 plan authority"
            ) from exc
        expected_plan_sha256 = provisional_promoting_plan_sha256_v8(
            promoting_plans
        )
        if (
            self.authority.plan_sha256 != expected_plan_sha256
            or payload.plan_bundle_sha256 != expected_plan_sha256
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "DEPTH_BRIDGE evidence names a foreign ledger plan authority"
            )
        return self._append(DEPTH_BRIDGE_EVENT_TYPE_V8, asdict(payload))

    def append_source_gap_open(
        self,
        promoting_plans: Sequence[ProvisionalPromotingPlanV2],
        selected_plan: ProvisionalPromotingCapturePlanV2,
        *,
        session_id: str,
        process_boot_id: str,
        cause: SourceGapCauseV2,
        left_boundary_kind: SourceGapLeftBoundaryV2,
        left_connection_id: str | None,
        left_generation: int | None,
        left_frame_seq: int | None,
        left_ingest_seq: int | None,
        left_wall_ms: int,
        left_monotonic_ns: int,
        detected_wall_ms: int,
        detected_monotonic_ns: int,
        source_component: str,
        evidence_sha256: str,
    ) -> CaptureIntegrityEventV2:
        """Fsync an upstream gap before any reconnect successor is accepted."""

        if not isinstance(cause, SourceGapCauseV2) or not isinstance(
            left_boundary_kind,
            SourceGapLeftBoundaryV2,
        ):
            raise ValueError("SOURCE_GAP cause or left boundary enum is invalid")
        _validate_source_gap_plan_authority(
            self.authority,
            promoting_plans,
            selected_plan,
        )
        return self._append_source_gap_open_material(
            selected_plan=selected_plan,
            session_id=session_id,
            process_boot_id=process_boot_id,
            cause=cause,
            left_boundary_kind=left_boundary_kind,
            left_connection_id=left_connection_id,
            left_generation=left_generation,
            left_frame_seq=left_frame_seq,
            left_ingest_seq=left_ingest_seq,
            left_wall_ms=left_wall_ms,
            left_monotonic_ns=left_monotonic_ns,
            detected_wall_ms=detected_wall_ms,
            detected_monotonic_ns=detected_monotonic_ns,
            source_component=source_component,
            evidence_sha256=evidence_sha256,
        )

    def append_source_gap_open_v8(
        self,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        selected_plan: ProvisionalPromotingCapturePlanV2,
        *,
        session_id: str,
        process_boot_id: str,
        cause: SourceGapCauseV2,
        left_boundary_kind: SourceGapLeftBoundaryV2,
        left_connection_id: str | None,
        left_generation: int | None,
        left_frame_seq: int | None,
        left_ingest_seq: int | None,
        left_wall_ms: int,
        left_monotonic_ns: int,
        detected_wall_ms: int,
        detected_monotonic_ns: int,
        source_component: str,
        evidence_sha256: str,
    ) -> CaptureIntegrityEventV2:
        """Fsync SOURCE_GAP OPEN under the full exact V8 plan authority."""

        if not isinstance(cause, SourceGapCauseV2) or not isinstance(
            left_boundary_kind,
            SourceGapLeftBoundaryV2,
        ):
            raise ValueError("SOURCE_GAP cause or left boundary enum is invalid")
        _validate_source_gap_plan_authority_v8(
            self.authority,
            promoting_plans,
            selected_plan,
        )
        return self._append_source_gap_open_material(
            selected_plan=selected_plan,
            session_id=session_id,
            process_boot_id=process_boot_id,
            cause=cause,
            left_boundary_kind=left_boundary_kind,
            left_connection_id=left_connection_id,
            left_generation=left_generation,
            left_frame_seq=left_frame_seq,
            left_ingest_seq=left_ingest_seq,
            left_wall_ms=left_wall_ms,
            left_monotonic_ns=left_monotonic_ns,
            detected_wall_ms=detected_wall_ms,
            detected_monotonic_ns=detected_monotonic_ns,
            source_component=source_component,
            evidence_sha256=evidence_sha256,
        )

    def _append_source_gap_open_material(
        self,
        *,
        selected_plan: ProvisionalPromotingCapturePlanV2,
        session_id: str,
        process_boot_id: str,
        cause: SourceGapCauseV2,
        left_boundary_kind: SourceGapLeftBoundaryV2,
        left_connection_id: str | None,
        left_generation: int | None,
        left_frame_seq: int | None,
        left_ingest_seq: int | None,
        left_wall_ms: int,
        left_monotonic_ns: int,
        detected_wall_ms: int,
        detected_monotonic_ns: int,
        source_component: str,
        evidence_sha256: str,
    ) -> CaptureIntegrityEventV2:
        plan_id = selected_plan.name
        venue = selected_plan.venue.value
        route_id = selected_plan.route_id
        affected_streams_sha256 = provisional_promoting_stream_census_sha256_v2(
            selected_plan
        )
        affected_stream_count = len(selected_plan.streams)
        gap_id = _source_gap_id_from_values(
            session_id=session_id,
            process_boot_id=process_boot_id,
            plan_id=plan_id,
            venue=venue,
            route_id=route_id,
            affected_streams_sha256=affected_streams_sha256,
            affected_stream_count=affected_stream_count,
            cause=cause.value,
            left_boundary_kind=left_boundary_kind.value,
            left_connection_id=left_connection_id,
            left_generation=left_generation,
            left_frame_seq=left_frame_seq,
            left_ingest_seq=left_ingest_seq,
            left_wall_ms=left_wall_ms,
            left_monotonic_ns=left_monotonic_ns,
            detected_wall_ms=detected_wall_ms,
            detected_monotonic_ns=detected_monotonic_ns,
            source_component=source_component,
        )
        payload = SourceGapPayloadV2(
            gap_id=gap_id,
            phase=SourceGapPhaseV2.OPEN.value,
            session_id=session_id,
            process_boot_id=process_boot_id,
            plan_id=plan_id,
            venue=venue,
            route_id=route_id,
            affected_streams_sha256=affected_streams_sha256,
            affected_stream_count=affected_stream_count,
            cause=cause.value,
            left_boundary_kind=left_boundary_kind.value,
            left_connection_id=left_connection_id,
            left_generation=left_generation,
            left_frame_seq=left_frame_seq,
            left_ingest_seq=left_ingest_seq,
            left_wall_ms=left_wall_ms,
            left_monotonic_ns=left_monotonic_ns,
            detected_wall_ms=detected_wall_ms,
            detected_monotonic_ns=detected_monotonic_ns,
            right_connection_id=None,
            right_generation=None,
            right_frame_seq=None,
            right_ingest_seq=None,
            right_wall_ms=None,
            right_monotonic_ns=None,
            open_event_sha256=None,
            left_record_locator=None,
            right_record_locator=None,
            source_message_count_known=False,
            missing_source_message_count=None,
            source_component=source_component,
            evidence_sha256=evidence_sha256,
        )
        return self._append("SOURCE_GAP", asdict(payload))

    def append_source_gap_bounded(
        self,
        open_event: CaptureIntegrityEventV2,
        *,
        right_ingest_seq: int,
        evidence_sha256: str,
    ) -> CaptureIntegrityEventV2:
        """Resolve signed endpoints and atomically bound one durable source gap."""

        with self._writer_lease_operation():
            with self._lock:
                internal_open = self._resolve_current_event_unlocked(open_event)
                if internal_open.event_type != "SOURCE_GAP":
                    raise CaptureIntegrityLedgerIntegrityError(
                        "SOURCE_GAP BOUNDED requires an OPEN event from this current ledger"
                    )
                open_payload = _typed_payload(
                    internal_open.event_type,
                    internal_open.payload,
                )
                if (
                    not isinstance(open_payload, SourceGapPayloadV2)
                    or open_payload.phase != SourceGapPhaseV2.OPEN.value
                ):
                    raise CaptureIntegrityLedgerIntegrityError(
                        "SOURCE_GAP BOUNDED reference is not an OPEN event"
                    )
                requested_ingest_seqs = (
                    (right_ingest_seq,)
                    if open_payload.left_ingest_seq is None
                    else (open_payload.left_ingest_seq, right_ingest_seq)
                )
                endpoints = self._read_source_gap_endpoints_unlocked(
                    requested_ingest_seqs
                )
                right_record, right_locator = endpoints[right_ingest_seq]
                self._validate_source_gap_record_scope_unlocked(
                    right_record,
                    open_payload,
                    "right",
                )
                if right_record.frame_seq is None:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "SOURCE_GAP right WebSocket record has no frame sequence"
                    )
                left_locator: FinalizedRecordLocatorV2 | None = None
                if open_payload.left_ingest_seq is not None:
                    left_record, left_locator = endpoints[open_payload.left_ingest_seq]
                    self._validate_source_gap_record_scope_unlocked(
                        left_record,
                        open_payload,
                        "left",
                    )
                    expected_left = (
                        open_payload.left_connection_id,
                        open_payload.left_generation,
                        open_payload.left_frame_seq,
                        open_payload.left_ingest_seq,
                        open_payload.left_wall_ms,
                        open_payload.left_monotonic_ns,
                    )
                    observed_left = (
                        left_record.connection_id,
                        left_record.generation,
                        left_record.frame_seq,
                        left_record.ingest_seq,
                        left_record.receipt_wall_ms,
                        left_record.receipt_monotonic_ns,
                    )
                    if observed_left != expected_left:
                        raise CaptureIntegrityLedgerIntegrityError(
                            "SOURCE_GAP left signed record differs from its OPEN cursor"
                        )
                document = asdict(open_payload)
                document.update(
                    {
                        "phase": SourceGapPhaseV2.BOUNDED.value,
                        "right_connection_id": right_record.connection_id,
                        "right_generation": right_record.generation,
                        "right_frame_seq": right_record.frame_seq,
                        "right_ingest_seq": right_record.ingest_seq,
                        "right_wall_ms": right_record.receipt_wall_ms,
                        "right_monotonic_ns": right_record.receipt_monotonic_ns,
                        "open_event_sha256": internal_open.sha256,
                        "left_record_locator": (
                            None if left_locator is None else asdict(left_locator)
                        ),
                        "right_record_locator": asdict(right_locator),
                        "evidence_sha256": evidence_sha256,
                    }
                )
                payload = _source_gap_payload_from_document(document)
                return _detached_event(
                    self._append_unlocked_guarded("SOURCE_GAP", asdict(payload))
                )

    def _resolve_current_event_unlocked(
        self,
        candidate: CaptureIntegrityEventV2,
    ) -> CaptureIntegrityEventV2:
        """Resolve a detached public event to the exact sealed internal value."""

        if not isinstance(candidate, CaptureIntegrityEventV2):
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity event reference has the wrong type"
            )
        index = candidate.event_sequence - 1
        if index < 0 or index >= len(self._events):
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity event reference is outside this current ledger"
            )
        internal = self._events[index]
        if canonical_json_line(asdict(candidate)) != canonical_json_line(
            asdict(internal)
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity event reference differs from this current ledger"
            )
        return internal

    def assert_source_gap_bounded_current_v2(
        self,
        bounded_event: CaptureIntegrityEventV2,
    ) -> None:
        """Assert current signed endpoints without returning a reusable authority token."""

        with self._writer_lease_operation():
            with self._lock:
                self._raise_if_failed()
                self._verify_bound_roots()
                self._refresh_events_unlocked()
                internal = self._resolve_current_event_unlocked(bounded_event)
                if internal.event_type != "SOURCE_GAP":
                    raise CaptureIntegrityLedgerIntegrityError(
                        "current replay requires a SOURCE_GAP event"
                    )
                payload = _typed_payload(internal.event_type, internal.payload)
                if (
                    not isinstance(payload, SourceGapPayloadV2)
                    or payload.phase != SourceGapPhaseV2.BOUNDED.value
                ):
                    raise CaptureIntegrityLedgerIntegrityError(
                        "current replay requires a BOUNDED SOURCE_GAP event"
                    )
                self._reverify_source_gap_payload_unlocked(payload)
                self._verify_bound_roots()
                self._refresh_events_unlocked()
                self._resolve_current_event_unlocked(bounded_event)
                assert payload.right_record_locator is not None
                self._assert_block_sequence_not_void_unlocked(
                    payload.right_record_locator.block_sequence
                )
                if payload.left_record_locator is not None:
                    self._assert_block_sequence_not_void_unlocked(
                        payload.left_record_locator.block_sequence
                    )

    def _reverify_source_gap_payload_unlocked(
        self,
        payload: SourceGapPayloadV2,
    ) -> tuple[RawRecordV2 | None, RawRecordV2]:
        assert payload.phase == SourceGapPhaseV2.BOUNDED.value
        assert payload.right_ingest_seq is not None
        requested = (
            (payload.right_ingest_seq,)
            if payload.left_ingest_seq is None
            else (payload.left_ingest_seq, payload.right_ingest_seq)
        )
        endpoints = self._read_source_gap_endpoints_unlocked(requested)
        right_record, right_locator = endpoints[payload.right_ingest_seq]
        self._validate_source_gap_record_scope_unlocked(
            right_record,
            payload,
            "right",
        )
        expected_right = (
            payload.right_connection_id,
            payload.right_generation,
            payload.right_frame_seq,
            payload.right_ingest_seq,
            payload.right_wall_ms,
            payload.right_monotonic_ns,
            payload.right_record_locator,
        )
        observed_right = (
            right_record.connection_id,
            right_record.generation,
            right_record.frame_seq,
            right_record.ingest_seq,
            right_record.receipt_wall_ms,
            right_record.receipt_monotonic_ns,
            right_locator,
        )
        if observed_right != expected_right:
            raise CaptureIntegrityLedgerIntegrityError(
                "SOURCE_GAP right locator or cursor differs from current signed storage"
            )
        left_record: RawRecordV2 | None = None
        if payload.left_ingest_seq is not None:
            left_record, left_locator = endpoints[payload.left_ingest_seq]
            self._validate_source_gap_record_scope_unlocked(
                left_record,
                payload,
                "left",
            )
            expected_left = (
                payload.left_connection_id,
                payload.left_generation,
                payload.left_frame_seq,
                payload.left_ingest_seq,
                payload.left_wall_ms,
                payload.left_monotonic_ns,
                payload.left_record_locator,
            )
            observed_left = (
                left_record.connection_id,
                left_record.generation,
                left_record.frame_seq,
                left_record.ingest_seq,
                left_record.receipt_wall_ms,
                left_record.receipt_monotonic_ns,
                left_locator,
            )
            if observed_left != expected_left:
                raise CaptureIntegrityLedgerIntegrityError(
                    "SOURCE_GAP left locator or cursor differs from current signed storage"
                )
        elif payload.left_record_locator is not None:
            raise CaptureIntegrityLedgerIntegrityError(
                "session-start SOURCE_GAP unexpectedly has a left locator"
            )
        return left_record, right_record

    def _refresh_events_unlocked(self) -> None:
        observed = self._load_events_and_recover_partial()
        if (
            len(observed) < len(self._events)
            or observed[: len(self._events)] != self._events
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity ledger changed other than by append"
            )
        self._events = observed

    def _read_source_gap_endpoints_unlocked(
        self,
        ingest_seqs: tuple[int, ...],
    ) -> dict[int, tuple[RawRecordV2, FinalizedRecordLocatorV2]]:
        if not ingest_seqs or len(ingest_seqs) > 2:
            raise CaptureIntegrityLedgerIntegrityError(
                "SOURCE_GAP endpoint read requires one or two ingest IDs"
            )
        if len(set(ingest_seqs)) != len(ingest_seqs):
            raise CaptureIntegrityLedgerIntegrityError(
                "SOURCE_GAP endpoint ingest IDs must be distinct"
            )
        for ingest_seq in ingest_seqs:
            _validate_positive_value(ingest_seq, "endpoint ingest_seq")
        try:
            manifests = verify_grouped_blocks(
                self.block_directory,
                authority=self.authority,
                policy=self.block_policy,
                signing_authority=self.block_signing_authority,
                stream_group_id=self.block_stream_group_id,
                segment_id=self.block_segment_id,
            )
            encoded_by_ingest: dict[int, bytes] = {}
            requested = set(ingest_seqs)

            def retain(ingest_seq: int, encoded_line: bytes) -> None:
                if ingest_seq in requested:
                    if ingest_seq in encoded_by_ingest:
                        raise CaptureIntegrityLedgerIntegrityError(
                            "SOURCE_GAP endpoint is duplicated in signed storage"
                        )
                    encoded_by_ingest[ingest_seq] = bytes(encoded_line)

            consume_verified_grouped_records_v2(
                self.block_directory,
                authority=self.authority,
                policy=self.block_policy,
                signing_authority=self.block_signing_authority,
                stream_group_id=self.block_stream_group_id,
                segment_id=self.block_segment_id,
                consume=retain,
            )
            if set(encoded_by_ingest) != requested:
                raise CaptureIntegrityLedgerIntegrityError(
                    "SOURCE_GAP endpoint is absent from current signed storage"
                )
            result: dict[
                int,
                tuple[RawRecordV2, FinalizedRecordLocatorV2],
            ] = {}
            for ingest_seq in ingest_seqs:
                matching = tuple(
                    manifest
                    for manifest in manifests
                    if manifest.first_ingest_seq
                    <= ingest_seq
                    <= manifest.last_ingest_seq
                )
                if len(matching) != 1:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "SOURCE_GAP endpoint does not map to one finalized block"
                    )
                manifest = matching[0]
                self._assert_block_sequence_not_void_unlocked(
                    manifest.block_sequence
                )
                encoded_line = encoded_by_ingest[ingest_seq]
                record = parse_raw_record_line_v2(encoded_line)
                locator = _build_finalized_record_locator_v2(
                    authority_sha256=self.authority.sha256,
                    manifest=manifest,
                    ingest_seq=ingest_seq,
                    encoded_line=encoded_line,
                )
                result[ingest_seq] = (record, locator)
            # The signed-block scan can be long enough for another ledger reader
            # to append durable VOID evidence.  Re-establish both root bindings
            # and the ledger tip after the scan, then reject a mixed snapshot
            # that never had valid endpoints and a non-VOID prefix together.
            self._verify_bound_roots()
            self._refresh_events_unlocked()
            for _, locator in result.values():
                self._assert_block_sequence_not_void_unlocked(
                    locator.block_sequence
                )
            return result
        except CaptureIntegrityLedgerError:
            raise
        except (BlockError, OSError, TypeError, ValueError) as exc:
            raise CaptureIntegrityLedgerIntegrityError(
                "SOURCE_GAP endpoint signed-block verification failed closed"
            ) from exc

    def _assert_block_sequence_not_void_unlocked(self, block_sequence: int) -> None:
        for event in self._events:
            if event.event_type != "VOID":
                continue
            payload = _typed_payload(event.event_type, event.payload)
            assert isinstance(payload, VoidFinalizedBlockPayloadV2)
            if payload.block_sequence <= block_sequence:
                raise CaptureIntegrityLedgerIntegrityError(
                    "SOURCE_GAP endpoint block prefix has current VOID evidence"
                )

    def _validate_source_gap_record_scope_unlocked(
        self,
        record: RawRecordV2,
        payload: SourceGapPayloadV2,
        label: str,
    ) -> None:
        expected = (
            payload.session_id,
            payload.plan_id,
            self.authority.protocol_sha256,
            TransportV2.WEBSOCKET,
            VenueV2.USDM_FUTURES,
            payload.route_id,
            None,
        )
        observed = (
            record.session_id,
            record.plan_id,
            record.protocol_hash,
            record.transport,
            record.venue,
            record.route_id,
            record.symbol,
        )
        if observed != expected:
            raise CaptureIntegrityLedgerIntegrityError(
                f"SOURCE_GAP {label} record differs from its trusted combined-stream scope"
            )

    def append_void_for_finalized_block(
        self,
        reference: FinalizedBlockReferenceV2,
        *,
        detector_component: str,
        detection_evidence_sha256: str,
    ) -> CaptureIntegrityEventV2:
        self._verify_bound_roots()
        if reference.authority_sha256 != self.authority.sha256:
            raise CaptureIntegrityLedgerIntegrityError(
                "finalized-block reference authority differs"
            )
        if (
            reference.block_root_binding_sha256 != self.block_root_binding_sha256
            or reference.block_root_path_sha256 != self.block_root_path_sha256
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "finalized-block reference names a different grouped-block root"
            )
        self._verify_block_signature(
            block_hash=reference.block_hash,
            writer_key_id=reference.writer_key_id,
            writer_ed25519_signature=reference.writer_ed25519_signature,
            signing_authority_sha256=reference.signing_authority_sha256,
        )
        data_state, data_bytes, data_sha256 = _observe_file(
            self.block_directory / reference.data_file
        )
        manifest_state, manifest_bytes, manifest_sha256 = _observe_file(
            self.block_directory / reference.manifest_file
        )
        corruption_kinds = _derive_corruption_kinds(
            observed_data_state=data_state,
            observed_data_bytes=data_bytes,
            observed_data_sha256=data_sha256,
            expected_container_bytes=reference.expected_container_bytes,
            expected_container_sha256=reference.expected_container_sha256,
            observed_manifest_state=manifest_state,
            observed_manifest_sha256=manifest_sha256,
            expected_manifest_sha256=reference.expected_manifest_sha256,
        )
        reference_payload = asdict(reference)
        reference_payload.pop("schema_version")
        payload = VoidFinalizedBlockPayloadV2(
            **reference_payload,  # type: ignore[arg-type]
            observed_data_state=data_state,
            observed_data_bytes=data_bytes,
            observed_data_sha256=data_sha256,
            observed_manifest_state=manifest_state,
            observed_manifest_bytes=manifest_bytes,
            observed_manifest_sha256=manifest_sha256,
            corruption_kinds=corruption_kinds,
            detector_component=detector_component,
            detection_evidence_sha256=detection_evidence_sha256,
        )
        return self._append("VOID", asdict(payload))

    def _append(
        self,
        event_type: str,
        payload: dict[str, object],
    ) -> CaptureIntegrityEventV2:
        with self._writer_lease_operation():
            with self._lock:
                if event_type == "SOURCE_GAP":
                    source_payload = _typed_payload(event_type, payload)
                    assert isinstance(source_payload, SourceGapPayloadV2)
                    if source_payload.phase == SourceGapPhaseV2.BOUNDED.value:
                        raise CaptureIntegrityLedgerIntegrityError(
                            "BOUNDED SOURCE_GAP must use ledger-owned endpoint resolution"
                        )
                return _detached_event(
                    self._append_unlocked_guarded(event_type, payload)
                )

    def _append_unlocked_guarded(
        self,
        event_type: str,
        payload: dict[str, object],
    ) -> CaptureIntegrityEventV2:
        self._raise_if_failed()
        self._verify_bound_roots()
        self._refresh_clean_closure_seal_unlocked()
        if self._clean_closure_claimed:
            raise CaptureIntegrityLedgerError(
                "integrity ledger rejects append after CLEAN closure issuance"
            )
        typed_payload = _typed_payload(event_type, payload)
        if (
            event_type == "SOURCE_GAP"
            and isinstance(typed_payload, SourceGapPayloadV2)
            and typed_payload.phase == SourceGapPhaseV2.BOUNDED.value
        ):
            self._reverify_source_gap_payload_unlocked(typed_payload)
        normalized_payload = json.loads(canonical_json_line(asdict(typed_payload)))
        if not isinstance(normalized_payload, dict):
            raise CaptureIntegrityLedgerIntegrityError(
                "canonical integrity payload is not an object"
            )
        canonical_payload: dict[str, object] = normalized_payload
        event_id = _event_id(
            event_type=event_type,
            authority_sha256=self.authority.sha256,
            ledger_root_binding_sha256=self.ledger_root_binding_sha256,
            block_root_binding_sha256=self.block_root_binding_sha256,
            block_root_path_sha256=self.block_root_path_sha256,
            payload=canonical_payload,
        )
        for existing in self._events:
            if existing.event_id == event_id:
                if (
                    existing.event_type != event_type
                    or canonical_json_line(existing.payload)
                    != canonical_json_line(canonical_payload)
                ):
                    raise CaptureIntegrityLedgerIntegrityError(
                        "deterministic event ID collides with different evidence"
                    )
                return existing
        self._validate_semantic_order(event_type, typed_payload)
        unmatched_before = _unmatched_source_gap_count(self._events)
        unmatched_after = unmatched_before
        if event_type == "SOURCE_GAP":
            assert isinstance(typed_payload, SourceGapPayloadV2)
            unmatched_after += (
                1 if typed_payload.phase == SourceGapPhaseV2.OPEN.value else -1
            )
        if unmatched_after < 0:
            raise CaptureIntegrityLedgerIntegrityError(
                "SOURCE_GAP closure reservation underflowed"
            )
        bridge_payloads = _depth_bridge_payloads(self._events)
        if event_type == DEPTH_BRIDGE_EVENT_TYPE_V8:
            assert isinstance(typed_payload, DepthBridgeEvidencePayloadV8)
            bridge_payloads = (*bridge_payloads, typed_payload)
        try:
            bridge_census = depth_bridge_evidence_census_v8(bridge_payloads)
        except DepthBridgeEvidenceErrorV8 as exc:
            raise CaptureIntegrityLedgerIntegrityError(
                "DEPTH_BRIDGE lifecycle evidence is invalid"
            ) from exc
        reserved_event_slots = (
            unmatched_after + bridge_census.open_terminal_reservation_count
        )
        if len(self._events) + 1 + reserved_event_slots > self.max_events:
            raise CaptureIntegrityLedgerCapacityError(
                "integrity ledger event count bound cannot preserve closure slots"
            )
        recorded_wall_ms = self._wall_clock_ms()
        recorded_monotonic_ns = self._monotonic_clock_ns()
        _validate_nonnegative_integer(recorded_wall_ms, "recorded_wall_ms")
        _validate_nonnegative_integer(recorded_monotonic_ns, "recorded_monotonic_ns")
        if (
            self._events
            and recorded_monotonic_ns < self._events[-1].recorded_monotonic_ns
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity ledger monotonic recording clock moved backwards"
            )
        event = CaptureIntegrityEventV2(
            event_sequence=len(self._events) + 1,
            previous_event_sha256=self.last_event_sha256,
            event_id=event_id,
            event_type=event_type,
            authority_sha256=self.authority.sha256,
            ledger_root_binding_sha256=self.ledger_root_binding_sha256,
            block_root_binding_sha256=self.block_root_binding_sha256,
            block_root_path_sha256=self.block_root_path_sha256,
            recorded_wall_ms=recorded_wall_ms,
            recorded_monotonic_ns=recorded_monotonic_ns,
            payload=canonical_payload,
        )
        _validate_source_gap_recording_causality(event_type, typed_payload, event)
        encoded = canonical_json_line(asdict(event))
        if (
            event_type == "SOURCE_GAP"
            and isinstance(typed_payload, SourceGapPayloadV2)
            and typed_payload.phase == SourceGapPhaseV2.BOUNDED.value
            and len(encoded) > _SOURCE_GAP_BOUNDED_RESERVE_BYTES
        ):
            raise CaptureIntegrityLedgerCapacityError(
                "SOURCE_GAP BOUNDED event exceeds its reserved byte bound"
            )
        if (
            event_type == DEPTH_BRIDGE_EVENT_TYPE_V8
            and len(encoded) > DEPTH_BRIDGE_TERMINAL_RESERVE_BYTES_V8
        ):
            raise CaptureIntegrityLedgerCapacityError(
                "DEPTH_BRIDGE event exceeds its reserved byte bound"
            )
        self._ensure_capacity(
            len(encoded),
            reserved_bytes=(
                unmatched_after * _SOURCE_GAP_BOUNDED_RESERVE_BYTES
                + bridge_census.open_terminal_reservation_count
                * DEPTH_BRIDGE_TERMINAL_RESERVE_BYTES_V8
                + _CLEAN_CLOSURE_SEAL_RESERVE_BYTES
            ),
        )
        final_path = self.directory / f"integrity-event-{event.event_sequence:08d}.json"
        partial_path = final_path.with_name(final_path.name + ".partial")
        if final_path.exists() or partial_path.exists():
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity ledger append path already exists"
            )
        try:
            self._call_fault("before_event_write")
            with partial_path.open("xb", buffering=0) as handle:
                written = handle.write(encoded)
                if written != len(encoded):
                    raise CaptureIntegrityLedgerIntegrityError(
                        "integrity ledger event short write"
                    )
                self._call_fault("after_event_write")
                os.fsync(handle.fileno())
            self._known_disk_bytes += len(encoded)
            self._call_fault("after_event_fsync")
            os.replace(partial_path, final_path)
            self._call_fault("after_event_rename")
            _fsync_parent(final_path)
            self._call_fault("after_event_parent_fsync")
            _fsync_path(final_path)
            self._verify_bound_roots()
            self._events.append(event)
            return event
        except BaseException as exc:
            self._failed = exc
            raise

    def _load_events_and_recover_partial(self) -> list[CaptureIntegrityEventV2]:
        _reject_unknown_event_artifacts(self.directory)
        final_paths = sorted(self.directory.glob("integrity-event-*.json"))
        if len(final_paths) > self.max_events:
            raise CaptureIntegrityLedgerCapacityError(
                "integrity ledger event count exceeds its sealed bound"
            )
        events: list[CaptureIntegrityEventV2] = []
        for path in final_paths:
            event = _read_event(path)
            self._validate_event(event, events, path)
            events.append(event)
        partials = sorted(self.directory.glob("integrity-event-*.json.partial"))
        if len(partials) > 1:
            raise CaptureIntegrityLedgerIntegrityError(
                "multiple integrity ledger partial tails require audit"
            )
        if partials:
            if len(events) >= self.max_events:
                raise CaptureIntegrityLedgerCapacityError(
                    "partial integrity event exceeds the event count bound"
                )
            partial = partials[0]
            event = _read_event(partial)
            self._validate_event(event, events, partial)
            final_path = self.directory / f"integrity-event-{event.event_sequence:08d}.json"
            if final_path.exists():
                raise CaptureIntegrityLedgerIntegrityError(
                    "integrity ledger has both final and partial event tails"
                )
            _fsync_path(partial)
            os.replace(partial, final_path)
            _fsync_parent(final_path)
            _fsync_path(final_path)
            events.append(event)
        return events

    def _validate_event(
        self,
        event: CaptureIntegrityEventV2,
        prior: Sequence[CaptureIntegrityEventV2],
        path: Path,
    ) -> None:
        expected_sequence = len(prior) + 1
        sequence = _sequence_from_name(
            path,
            _PARTIAL_RE if path.name.endswith(".partial") else _EVENT_RE,
        )
        if (
            type(event.event_sequence) is not int
            or event.event_sequence != expected_sequence
            or sequence != expected_sequence
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity ledger event sequence is not contiguous"
            )
        expected_previous = prior[-1].sha256 if prior else None
        if event.previous_event_sha256 != expected_previous:
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity ledger event hash chain is broken"
            )
        if event.schema_version != _LEDGER_SCHEMA:
            raise CaptureIntegrityLedgerIntegrityError(
                "unsupported integrity ledger event schema"
            )
        if (
            event.authority_sha256 != self.authority.sha256
            or event.ledger_root_binding_sha256 != self.ledger_root_binding_sha256
            or event.block_root_binding_sha256 != self.block_root_binding_sha256
            or event.block_root_path_sha256 != self.block_root_path_sha256
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity event differs from its bound authority or storage roots"
            )
        _validate_nonnegative_integer(event.recorded_wall_ms, "recorded_wall_ms")
        _validate_nonnegative_integer(
            event.recorded_monotonic_ns,
            "recorded_monotonic_ns",
        )
        if prior and event.recorded_monotonic_ns < prior[-1].recorded_monotonic_ns:
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity ledger monotonic recording clock moved backwards"
            )
        typed_payload = _typed_payload(event.event_type, event.payload)
        _validate_source_gap_recording_causality(
            event.event_type,
            typed_payload,
            event,
        )
        expected_id = _event_id(
            event_type=event.event_type,
            authority_sha256=event.authority_sha256,
            ledger_root_binding_sha256=event.ledger_root_binding_sha256,
            block_root_binding_sha256=event.block_root_binding_sha256,
            block_root_path_sha256=event.block_root_path_sha256,
            payload=asdict(typed_payload),
        )
        if event.event_id != expected_id:
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity event deterministic ID differs from its evidence"
            )
        if any(existing.event_id == event.event_id for existing in prior):
            raise CaptureIntegrityLedgerIntegrityError(
                "duplicate deterministic event was appended instead of deduplicated"
            )
        self._validate_semantic_order(event.event_type, typed_payload, prior=prior)

    def _validate_semantic_order(
        self,
        event_type: str,
        payload: (
            DataGapPayloadV2
            | SourceGapPayloadV2
            | VoidFinalizedBlockPayloadV2
            | DepthBridgeEvidencePayloadV8
        ),
        *,
        prior: Sequence[CaptureIntegrityEventV2] | None = None,
    ) -> None:
        history = self._events if prior is None else prior
        if event_type == "DATA_GAP":
            assert isinstance(payload, DataGapPayloadV2)
            prior_gaps = [
                _typed_payload(existing.event_type, existing.payload)
                for existing in history
                if existing.event_type == "DATA_GAP"
            ]
            if prior_gaps:
                latest = prior_gaps[-1]
                assert isinstance(latest, DataGapPayloadV2)
                if payload.first_missing_ingest_seq <= latest.last_missing_ingest_seq:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "DATA_GAP intervals overlap or were appended out of ingest order"
                    )
            return
        if event_type == "SOURCE_GAP":
            assert isinstance(payload, SourceGapPayloadV2)
            prior_source_gaps = [
                _typed_payload(existing.event_type, existing.payload)
                for existing in history
                if existing.event_type == "SOURCE_GAP"
            ]
            typed_gaps = [
                gap
                for gap in prior_source_gaps
                if isinstance(gap, SourceGapPayloadV2)
            ]
            if any(
                (gap.session_id, gap.process_boot_id)
                != (payload.session_id, payload.process_boot_id)
                for gap in typed_gaps
            ):
                raise CaptureIntegrityLedgerIntegrityError(
                    "SOURCE_GAP ledger cannot cross capture sessions or process boots"
                )
            same_source = [
                gap for gap in typed_gaps if _same_source_gap_plan_route(gap, payload)
            ]
            if any(
                gap.process_boot_id != payload.process_boot_id
                or gap.affected_streams_sha256 != payload.affected_streams_sha256
                or gap.affected_stream_count != payload.affected_stream_count
                or gap.source_component != payload.source_component
                for gap in same_source
            ):
                raise CaptureIntegrityLedgerIntegrityError(
                    "SOURCE_GAP stream census or source owner drifted within one plan route"
                )
            same_gap = [gap for gap in typed_gaps if gap.gap_id == payload.gap_id]
            if payload.phase == SourceGapPhaseV2.OPEN.value:
                if same_gap:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "SOURCE_GAP gap_id already has conflicting OPEN/BOUNDED evidence"
                    )
                same_scope = [
                    gap for gap in typed_gaps if _same_source_gap_scope(gap, payload)
                ]
                open_ids = {
                    gap.gap_id
                    for gap in same_scope
                    if gap.phase == SourceGapPhaseV2.OPEN.value
                }
                bounded_ids = {
                    gap.gap_id
                    for gap in same_scope
                    if gap.phase == SourceGapPhaseV2.BOUNDED.value
                }
                if open_ids - bounded_ids:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "SOURCE_GAP scope already has an unbounded OPEN event"
                    )
                bounded = [
                    gap
                    for gap in same_scope
                    if gap.phase == SourceGapPhaseV2.BOUNDED.value
                ]
                if bounded:
                    latest = bounded[-1]
                    assert latest.right_monotonic_ns is not None
                    if payload.left_monotonic_ns < latest.right_monotonic_ns:
                        raise CaptureIntegrityLedgerIntegrityError(
                            "SOURCE_GAP intervals overlap or were appended out of source order"
                        )
                    if (
                        payload.left_boundary_kind
                        != SourceGapLeftBoundaryV2.RETAINED_FRAME.value
                        or payload.left_connection_id != latest.right_connection_id
                        or payload.left_generation != latest.right_generation
                        or payload.left_frame_seq is None
                        or payload.left_ingest_seq is None
                        or payload.left_ingest_seq < latest.right_ingest_seq  # type: ignore[operator]
                    ):
                        raise CaptureIntegrityLedgerIntegrityError(
                            "SOURCE_GAP OPEN does not continue the prior bounded source cursor"
                        )
                elif payload.left_boundary_kind == (
                    SourceGapLeftBoundaryV2.RETAINED_FRAME.value
                ):
                    raise CaptureIntegrityLedgerIntegrityError(
                        "SOURCE_GAP first OPEN for a source must be SESSION_START"
                    )
                return
            assert payload.phase == SourceGapPhaseV2.BOUNDED.value
            opens = [
                gap
                for gap in same_gap
                if gap.phase == SourceGapPhaseV2.OPEN.value
            ]
            bounded = [
                gap
                for gap in same_gap
                if gap.phase == SourceGapPhaseV2.BOUNDED.value
            ]
            if len(opens) != 1 or bounded:
                raise CaptureIntegrityLedgerIntegrityError(
                    "SOURCE_GAP BOUNDED lacks one unmatched OPEN event"
                )
            open_payload = opens[0]
            open_event = next(
                existing
                for existing in history
                if existing.event_type == "SOURCE_GAP"
                and existing.payload.get("gap_id") == payload.gap_id
                and existing.payload.get("phase") == SourceGapPhaseV2.OPEN.value
            )
            if payload.open_event_sha256 != open_event.sha256:
                raise CaptureIntegrityLedgerIntegrityError(
                    "SOURCE_GAP BOUNDED open-event hash differs"
                )
            assert payload.right_monotonic_ns is not None
            if payload.right_monotonic_ns <= open_event.recorded_monotonic_ns:
                raise CaptureIntegrityLedgerIntegrityError(
                    "SOURCE_GAP successor cursor does not follow durable OPEN recording"
                )
            if not _same_source_gap_open_identity(open_payload, payload):
                raise CaptureIntegrityLedgerIntegrityError(
                    "SOURCE_GAP BOUNDED scope differs from its OPEN event"
                )
            return
        if event_type == DEPTH_BRIDGE_EVENT_TYPE_V8:
            assert isinstance(payload, DepthBridgeEvidencePayloadV8)
            prior_payloads = _depth_bridge_payloads(history)
            try:
                validate_depth_bridge_evidence_order_v8(
                    payload,
                    prior=prior_payloads,
                )
            except (TypeError, ValueError) as exc:
                raise CaptureIntegrityLedgerIntegrityError(
                    "DEPTH_BRIDGE lifecycle phase order is invalid"
                ) from exc
            return
        assert event_type == "VOID"
        assert isinstance(payload, VoidFinalizedBlockPayloadV2)
        self._validate_void_evidence(payload)
        for existing in history:
            if existing.event_type != "VOID":
                continue
            prior_void = _typed_payload(existing.event_type, existing.payload)
            assert isinstance(prior_void, VoidFinalizedBlockPayloadV2)
            if prior_void.block_sequence == payload.block_sequence:
                raise CaptureIntegrityLedgerIntegrityError(
                    "finalized block already has different VOID evidence"
                )

    def _validate_void_evidence(self, payload: VoidFinalizedBlockPayloadV2) -> None:
        if (
            payload.authority_sha256 != self.authority.sha256
            or payload.block_root_binding_sha256 != self.block_root_binding_sha256
            or payload.block_root_path_sha256 != self.block_root_path_sha256
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "VOID evidence differs from its bound authority or block root"
            )
        self._verify_block_signature(
            block_hash=payload.block_hash,
            writer_key_id=payload.writer_key_id,
            writer_ed25519_signature=payload.writer_ed25519_signature,
            signing_authority_sha256=payload.signing_authority_sha256,
        )
        expected_kinds = _derive_corruption_kinds(
            observed_data_state=payload.observed_data_state,
            observed_data_bytes=payload.observed_data_bytes,
            observed_data_sha256=payload.observed_data_sha256,
            expected_container_bytes=payload.expected_container_bytes,
            expected_container_sha256=payload.expected_container_sha256,
            observed_manifest_state=payload.observed_manifest_state,
            observed_manifest_sha256=payload.observed_manifest_sha256,
            expected_manifest_sha256=payload.expected_manifest_sha256,
        )
        if payload.corruption_kinds != expected_kinds:
            raise CaptureIntegrityLedgerIntegrityError(
                "VOID corruption kinds differ from the exact stored observation"
            )

    def _verify_block_signature(
        self,
        *,
        block_hash: str,
        writer_key_id: str,
        writer_ed25519_signature: str,
        signing_authority_sha256: str,
    ) -> None:
        if (
            signing_authority_sha256 != self.block_signing_authority.sha256
            or writer_key_id != self.block_signing_authority.key_id
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "finalized-block reference names a different signing authority"
            )
        try:
            signature = base64.b64decode(
                writer_ed25519_signature,
                validate=True,
            )
            self.block_signing_authority.verify(
                signature,
                _BLOCK_SIGNATURE_DOMAIN + bytes.fromhex(block_hash),
            )
        except (ValueError, SignedBlockContainerError) as exc:
            raise CaptureIntegrityLedgerIntegrityError(
                "finalized-block reference signature is invalid"
            ) from exc

    def _load_clean_closure_seal_and_recover_partial_unlocked(
        self,
    ) -> _PersistedCaptureCleanClosureSealReceipt | None:
        _reject_unknown_clean_closure_artifacts(self.directory)
        final_path = self.directory / _CLEAN_CLOSURE_SEAL_FILE
        partial_path = self.directory / _CLEAN_CLOSURE_SEAL_PARTIAL_FILE
        final_exists = final_path.exists() or final_path.is_symlink()
        partial_exists = partial_path.exists() or partial_path.is_symlink()
        if final_exists and partial_exists:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure has both final and partial seal artifacts"
            )
        if not final_exists and not partial_exists:
            return None
        source = partial_path if partial_exists else final_path
        seal = _read_clean_closure_seal(source)
        self._validate_clean_closure_seal_against_ledger_unlocked(seal)
        if partial_exists:
            _fsync_path(partial_path)
            os.replace(partial_path, final_path)
            _fsync_parent(final_path)
            _fsync_path(final_path)
        return _persisted_clean_closure_receipt(final_path, seal)

    def _refresh_clean_closure_seal_unlocked(self) -> None:
        observed = self._load_clean_closure_seal_and_recover_partial_unlocked()
        if self._clean_closure_receipt is None:
            if observed is not None:
                self._clean_closure_receipt = observed
                self._clean_closure_claimed = True
            return
        if observed is None:
            raise CaptureIntegrityLedgerIntegrityError(
                "durable CLEAN closure seal was removed"
            )
        if observed != self._clean_closure_receipt:
            raise CaptureIntegrityLedgerIntegrityError(
                "durable CLEAN closure seal file identity changed"
            )

    def _validate_clean_closure_seal_against_ledger_unlocked(
        self,
        seal: _CaptureCleanClosureSeal,
    ) -> None:
        if type(seal) is CaptureCleanClosureSealV8:
            self._validate_clean_closure_seal_v8_against_ledger_unlocked(seal)
            return
        if type(seal) is not CaptureCleanClosureSealV2:
            raise TypeError("seal must be an exact CaptureCleanClosureSealV2")
        try:
            seal.__post_init__()
        except (TypeError, ValueError) as exc:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure seal value is invalid"
            ) from exc
        if (
            seal.authority_sha256 != self.authority.sha256
            or seal.attempt_id != self.authority.attempt_id
            or seal.ledger_root_binding_sha256
            != self.ledger_root_binding_sha256
            or seal.ledger_root_path_sha256 != self.ledger_root_path_sha256
            or seal.block_root_binding_sha256 != self.block_root_binding_sha256
            or seal.block_root_path_sha256 != self.block_root_path_sha256
            or seal.block_signing_authority_sha256
            != self.block_signing_authority.sha256
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure seal differs from ledger authority or roots"
            )
        receipt = seal.finality_receipt
        if (
            receipt.grouped_block_root_binding != self.block_root_binding
            or receipt.block_signing_authority_sha256
            != self.block_signing_authority.sha256
            or receipt.stream_group_id != self.block_stream_group_id
            or receipt.segment_id != self.block_segment_id
            or receipt.qualification_id != self.block_policy.qualification_id
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure finality receipt differs from ledger block contract"
            )
        census = _integrity_event_census(self._events)
        if census.depth_bridge_event_count:
            raise CaptureIntegrityLedgerIntegrityError(
                "V2 CLEAN closure cannot coexist with DEPTH_BRIDGE evidence"
            )
        observed = (
            len(self._events),
            self._events[-1].sha256 if self._events else None,
            census.source_gap_open_count,
            census.source_gap_bounded_count,
            census.unmatched_source_gap_open_count,
            census.data_gap_count,
            census.void_count,
        )
        expected = (
            seal.event_count,
            seal.event_tip_sha256,
            seal.source_gap_open_count,
            seal.source_gap_bounded_count,
            seal.unmatched_source_gap_open_count,
            seal.data_gap_count,
            seal.void_count,
        )
        if observed != expected:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure event census or ledger tip differs"
            )
        source_identities = _source_gap_session_boot_identities(self._events)
        if source_identities and source_identities != {
            (seal.session_id, seal.process_boot_id)
        }:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure session or process boot differs from SOURCE_GAP evidence"
            )
        if self._events and (
            seal.seal_wall_ms
            < max(event.recorded_wall_ms for event in self._events)
            or seal.seal_monotonic_ns
            < max(event.recorded_monotonic_ns for event in self._events)
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure clocks precede the integrity-ledger tip"
            )

    def _validate_clean_closure_seal_v8_against_ledger_unlocked(
        self,
        seal: CaptureCleanClosureSealV8,
    ) -> None:
        if type(seal) is not CaptureCleanClosureSealV8:
            raise TypeError("seal must be an exact CaptureCleanClosureSealV8")
        try:
            seal.__post_init__()
        except (TypeError, ValueError) as exc:
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN closure seal value is invalid"
            ) from exc
        if (
            seal.authority_sha256 != self.authority.sha256
            or seal.attempt_id != self.authority.attempt_id
            or seal.protocol_hash != self.authority.protocol_sha256
            or seal.plan_bundle_sha256 != self.authority.plan_sha256
            or seal.ledger_root_binding_sha256
            != self.ledger_root_binding_sha256
            or seal.ledger_root_path_sha256 != self.ledger_root_path_sha256
            or seal.block_root_binding_sha256 != self.block_root_binding_sha256
            or seal.block_root_path_sha256 != self.block_root_path_sha256
            or seal.block_signing_authority_sha256
            != self.block_signing_authority.sha256
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN seal differs from ledger authority or roots"
            )
        receipt = seal.finality_receipt
        if (
            receipt.grouped_block_root_binding != self.block_root_binding
            or receipt.block_signing_authority_sha256
            != self.block_signing_authority.sha256
            or receipt.stream_group_id != self.block_stream_group_id
            or receipt.segment_id != self.block_segment_id
            or receipt.qualification_id != self.block_policy.qualification_id
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN finality receipt differs from ledger block contract"
            )
        census = _integrity_event_census(self._events)
        observed = (
            len(self._events),
            self._events[-1].sha256 if self._events else None,
            census.source_gap_open_count,
            census.source_gap_bounded_count,
            census.unmatched_source_gap_open_count,
            census.data_gap_count,
            census.void_count,
            census.depth_bridge_event_count,
            census.depth_bridge_generation_started_count,
            census.depth_bridge_generation_drained_count,
            census.depth_bridge_fatal_generation_count,
            census.depth_bridge_trigger_count,
            census.depth_bridge_cycle_count,
            census.depth_bridge_failed_cycle_count,
            census.depth_bridge_open_generation_count,
            census.depth_bridge_open_cycle_count,
            census.depth_bridge_open_attempt_count,
            census.depth_bridge_open_wait_count,
        )
        expected = (
            seal.event_count,
            seal.event_tip_sha256,
            seal.source_gap_open_count,
            seal.source_gap_bounded_count,
            seal.unmatched_source_gap_open_count,
            seal.data_gap_count,
            seal.void_count,
            seal.depth_bridge_event_count,
            seal.depth_bridge_generation_started_count,
            seal.depth_bridge_generation_drained_count,
            seal.depth_bridge_fatal_generation_count,
            seal.depth_bridge_trigger_count,
            seal.depth_bridge_cycle_count,
            seal.depth_bridge_failed_cycle_count,
            seal.depth_bridge_open_generation_count,
            seal.depth_bridge_open_cycle_count,
            seal.depth_bridge_open_attempt_count,
            seal.depth_bridge_open_wait_count,
        )
        if observed != expected:
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN event census or ledger tip differs"
            )
        last_drain = _last_depth_bridge_drain_event(self._events)
        if (
            last_drain.event_sequence
            != seal.last_depth_bridge_drain_event_sequence
            or last_drain.sha256 != seal.last_depth_bridge_drain_event_sha256
            or last_drain.recorded_wall_ms
            != seal.last_depth_bridge_drain_recorded_wall_ms
            or last_drain.recorded_monotonic_ns
            != seal.last_depth_bridge_drain_recorded_monotonic_ns
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN last depth-bridge drain locator differs"
            )
        _validate_last_depth_bridge_drain_lineage(
            last_drain,
            seal.depth_bridge_closure_entry,
        )
        source_identities = _source_gap_session_boot_identities(self._events)
        if source_identities and source_identities != {
            (seal.session_id, seal.process_boot_id)
        }:
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN session or boot differs from SOURCE_GAP evidence"
            )
        if self._events and (
            seal.seal_wall_ms
            < max(event.recorded_wall_ms for event in self._events)
            or seal.seal_monotonic_ns
            < max(event.recorded_monotonic_ns for event in self._events)
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN clocks precede the integrity-ledger tip"
            )

    def _validate_v8_live_closure_inputs_against_ledger_unlocked(
        self,
        *,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        depth_plan: ProvisionalDepthRestQualificationPlanV8,
        depth_bridge_close_receipt: DepthBridgeCoordinatorCleanCloseReceiptV8,
        websocket_projection: WebSocketRouteCursorClosurePairV8,
        finality_receipt: CaptureFinalityFenceReceiptV2,
        session_id: str,
        process_boot_id: str,
        seal_wall_ms: int,
        seal_monotonic_ns: int,
    ) -> None:
        validate_depth_bridge_coordinator_clean_close_receipt_v8(
            depth_bridge_close_receipt,
            promoting_plans=promoting_plans,
            depth_plan=depth_plan,
        )
        validate_websocket_route_cursor_closure_pair_v8(
            websocket_projection,
            finality_receipt=finality_receipt,
            promoting_plans=promoting_plans,
        )
        census = _depth_bridge_census(self._events)
        if (
            census.event_count < 1
            or census.generation_started_count < 1
            or census.generation_started_count != census.generation_drained_count
            or census.fatal_generation_count != 0
            or census.last_drain_reason != "normal_stop"
            or census.open_terminal_reservation_count != 0
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN requires a fully drained nonfatal depth bridge"
            )
        if (
            depth_bridge_close_receipt.generation_started_count
            != census.generation_started_count
            or depth_bridge_close_receipt.generation_drained_count
            != census.generation_drained_count
            or depth_bridge_close_receipt.fatal_generation_count
            != census.fatal_generation_count
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN bridge receipt census differs from ledger evidence"
            )
        last_drain = _last_depth_bridge_drain_event(self._events)
        if (
            depth_bridge_close_receipt.last_generation_drained_event_sequence
            != last_drain.event_sequence
            or depth_bridge_close_receipt.last_generation_drained_event_sha256
            != last_drain.sha256
            or depth_bridge_close_receipt.last_generation_drained_recorded_wall_ms
            != last_drain.recorded_wall_ms
            or depth_bridge_close_receipt.last_generation_drained_recorded_monotonic_ns
            != last_drain.recorded_monotonic_ns
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN bridge receipt differs from the last durable drain"
            )
        bridge_entry = depth_bridge_coordinator_closure_entry_v8(
            depth_bridge_close_receipt,
            promoting_plans=promoting_plans,
            depth_plan=depth_plan,
        )
        _validate_last_depth_bridge_drain_lineage(last_drain, bridge_entry)
        public_cursor = websocket_projection[1]
        if (
            depth_bridge_close_receipt.session_id != session_id
            or depth_bridge_close_receipt.protocol_hash
            != self.authority.protocol_sha256
            or public_cursor.session_id != session_id
            or public_cursor.process_boot_id != process_boot_id
            or depth_bridge_close_receipt.last_connection_id
            != public_cursor.connection_id
            or depth_bridge_close_receipt.last_connection_generation
            != public_cursor.generation
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN bridge/public cursor lineage differs"
            )
        if (
            public_cursor.stop_observed_wall_ms
            > depth_bridge_close_receipt.close_wall_ms
            or public_cursor.stop_observed_monotonic_ns
            > depth_bridge_close_receipt.close_monotonic_ns
            or depth_bridge_close_receipt.close_monotonic_ns
            > finality_receipt.fence_monotonic_ns
            or seal_wall_ms < depth_bridge_close_receipt.close_wall_ms
            or seal_wall_ms < finality_receipt.target_last_receipt_wall_ms
            or seal_monotonic_ns < finality_receipt.writer_observed_monotonic_ns
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "V8 CLEAN receipt, bridge, finality, or seal clocks are misordered"
            )

    def _verify_exact_clean_closure_inputs_unlocked(
        self,
        *,
        finality_receipt: CaptureFinalityFenceReceiptV2,
        wal_writer: MirroredWalWriterV2,
        block_writer: GroupedBlockWriterV2,
        session_id: str,
        process_boot_id: str,
        require_block_terminal: bool,
        allow_verification_only_wal: bool,
        allow_depth_bridge_v8: bool = False,
    ) -> str | None:
        if type(require_block_terminal) is not bool:
            raise TypeError("require_block_terminal must be boolean")
        if type(allow_verification_only_wal) is not bool:
            raise TypeError("allow_verification_only_wal must be boolean")
        if type(allow_depth_bridge_v8) is not bool:
            raise TypeError("allow_depth_bridge_v8 must be boolean")
        if allow_verification_only_wal and not require_block_terminal:
            raise ValueError(
                "verification-only WAL requires an already-terminal block prefix"
            )
        _require_exact_clean_closure_inputs_v2(
            finality_receipt=finality_receipt,
            wal_writer=wal_writer,
            block_writer=block_writer,
        )

        def assert_wal_prefix_current() -> None:
            if wal_writer.verification_only:
                if not allow_verification_only_wal:
                    raise CaptureIntegrityLedgerIntegrityError(
                        "CLEAN closure issuance rejects a verification-only WAL owner"
                    )
                wal_writer.assert_verification_only_prefix_current_v2(
                    expected_durable_ack_seq=finality_receipt.fence_ingest_seq,
                    expected_durability_binding=(
                        finality_receipt.wal_durability_binding
                    ),
                )
                return
            wal_writer.assert_cleanly_closed_and_current_v2()

        census = _integrity_event_census(self._events)
        if census.depth_bridge_event_count and not allow_depth_bridge_v8:
            raise CaptureIntegrityLedgerIntegrityError(
                "V2 CLEAN closure rejects DEPTH_BRIDGE evidence"
            )
        if census.unmatched_source_gap_open_count:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure rejects unmatched SOURCE_GAP OPEN evidence"
            )
        if census.void_count:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure rejects VOID-poisoned finalized evidence"
            )
        source_identities = _source_gap_session_boot_identities(self._events)
        if source_identities and source_identities != {
            (session_id, process_boot_id)
        }:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure session or process boot differs from ledger evidence"
            )
        if (
            wal_writer.authority != self.authority
            or block_writer.authority != self.authority
            or block_writer.root_binding != self.block_root_binding
            or _normalized_resolved_path(block_writer.directory)
            != self.block_directory
            or block_writer.signing_authority != self.block_signing_authority
            or block_writer.policy != self.block_policy
            or block_writer.stream_group_id != self.block_stream_group_id
            or block_writer.segment_id != self.block_segment_id
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure owners differ from ledger authority or block contract"
            )
        if (
            finality_receipt.grouped_block_root_binding
            != self.block_root_binding
            or finality_receipt.block_signing_authority_sha256
            != self.block_signing_authority.sha256
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure receipt differs from ledger block authority"
            )
        fence = finality_receipt.fence_ingest_seq
        if (
            wal_writer.durable_ack_seq != fence
            or wal_writer.next_ingest_seq != fence + 1
            or block_writer.next_ingest_seq != fence + 1
            or block_writer.last_block_hash != finality_receipt.final_block_hash
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure finality receipt is not the exact current storage tail"
            )
        try:
            assert_wal_prefix_current()
            if require_block_terminal:
                terminal_sha256 = (
                    block_writer.assert_clean_tail_terminal_and_current_v2(
                        finality_receipt
                    )
                )
            else:
                block_writer.assert_running_healthy_and_writer_open_v2()
                terminal_sha256 = None
            verified_prefix = verify_capture_finality_fence_receipt_v2(
                finality_receipt,
                wal_writer=wal_writer,
                block_writer=block_writer,
            )
        except (CaptureBatchAckErrorV2, BlockError, WalError) as exc:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure finality evidence failed current verification"
            ) from exc
        if verified_prefix != finality_receipt.prefix_proof_sha256:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure finality prefix proof differs"
            )
        observed_record_count = 0

        def verify_record_session(
            ingest_seq: int,
            encoded_line: bytes,
        ) -> None:
            nonlocal observed_record_count
            record = parse_raw_record_line_v2(encoded_line)
            if ingest_seq != observed_record_count + 1:
                raise CaptureIntegrityLedgerIntegrityError(
                    "CLEAN closure WAL record sequence is not contiguous"
                )
            if record.session_id != session_id:
                raise CaptureIntegrityLedgerIntegrityError(
                    "CLEAN closure session differs from retained WAL records"
                )
            observed_record_count += 1

        try:
            wal_writer.consume_durable_records(verify_record_session)
        except (BlockError, WalError) as exc:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure could not re-read the exact WAL tail"
            ) from exc
        if observed_record_count != fence:
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure WAL census differs from its finality tail"
            )
        assert_wal_prefix_current()
        if require_block_terminal:
            observed_terminal_sha256 = (
                block_writer.assert_clean_tail_terminal_and_current_v2(
                    finality_receipt
                )
            )
            if observed_terminal_sha256 != terminal_sha256:
                raise CaptureIntegrityLedgerIntegrityError(
                    "CLEAN closure block terminal changed during verification"
                )
        else:
            block_writer.assert_running_healthy_and_writer_open_v2()
        if (
            wal_writer.durable_ack_seq != fence
            or wal_writer.next_ingest_seq != fence + 1
            or block_writer.next_ingest_seq != fence + 1
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                "CLEAN closure storage tail advanced during verification"
            )
        self._verify_bound_roots()
        return terminal_sha256

    def _verify_bound_roots(self) -> None:
        if _normalized_resolved_path(self.directory) != self.opened_directory:
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity-ledger directory differs from its construction-time root"
            )
        try:
            ledger_identity = inspect_storage_root_opened_identity_v2(
                self.opened_directory,
                self.root_binding,
            )
        except StorageRootBindingError as exc:
            raise CaptureIntegrityLedgerIntegrityError(
                f"integrity-ledger root binding is invalid: {exc}"
            ) from exc
        try:
            block_identity = inspect_storage_root_opened_identity_v2(
                self.block_directory,
                self.block_root_binding,
            )
        except StorageRootBindingError as exc:
            raise CaptureIntegrityLedgerIntegrityError(
                f"integrity-ledger block root binding is invalid: {exc}"
            ) from exc
        if ledger_identity != self._opened_root_identity:
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity-ledger root identity changed after construction"
            )
        if block_identity != self._opened_block_root_identity:
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity-ledger block-root identity changed after construction"
            )

    def _writer_lease_operation(self) -> AbstractContextManager[None]:
        """Return the outer lock-hierarchy guard; enter it before ``self._lock``."""

        if self.writer_lease is None:
            return nullcontext()
        return self.writer_lease.operation_guard()

    def _ensure_capacity(
        self,
        additional_bytes: int,
        *,
        reserved_bytes: int = 0,
    ) -> None:
        if (
            self._known_disk_bytes
            + additional_bytes
            + self.emergency_reserve_bytes
            + reserved_bytes
            > self.maximum_total_bytes
        ):
            raise CaptureIntegrityLedgerCapacityError(
                "integrity event would consume the reserved disk budget"
            )

    def _call_fault(self, point: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(point)

    def _raise_if_failed(self) -> None:
        if self._failed is not None:
            raise CaptureIntegrityLedgerError(
                "integrity ledger is fault-latched after an incomplete append"
            ) from self._failed


def verify_persisted_capture_clean_closure_seal_receipt_v2(
    receipt: PersistedCaptureCleanClosureSealReceiptV2,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    ledger: CaptureIntegrityLedgerV2,
) -> str:
    """Reprove one factory receipt against the current fixed seal pathname."""

    if type(receipt) is not PersistedCaptureCleanClosureSealReceiptV2:
        raise TypeError(
            "receipt must be an exact PersistedCaptureCleanClosureSealReceiptV2"
        )
    if type(ledger) is not CaptureIntegrityLedgerV2:
        raise TypeError("ledger must be an exact CaptureIntegrityLedgerV2")
    _require_v2_clean_closure_plan_authority(
        promoting_plans,
        authority_sha256=ledger.authority.plan_sha256,
    )
    if ledger.writer_lease is None:
        raise CaptureIntegrityLedgerError(
            "persisted CLEAN closure verification requires an exact held WriterLease"
        )
    with ledger._writer_lease_operation():
        with ledger._lock:
            receipt.__post_init__()
            ledger._raise_if_failed()
            ledger._verify_bound_roots()
            ledger._refresh_events_unlocked()
            ledger._refresh_clean_closure_seal_unlocked()
            if ledger._clean_closure_receipt != receipt:
                raise CaptureIntegrityLedgerIntegrityError(
                    "persisted CLEAN closure receipt differs from the current seal"
                )
            ledger._validate_clean_closure_seal_against_ledger_unlocked(
                receipt.seal
            )
            _assert_persisted_clean_closure_file_current(receipt)
            return receipt.seal_sha256


def verify_persisted_capture_clean_closure_seal_receipt_v8(
    receipt: PersistedCaptureCleanClosureSealReceiptV8,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
    ledger: CaptureIntegrityLedgerV2,
) -> str:
    """Reprove one V8 factory receipt against its exact plans and fixed file."""

    if type(receipt) is not PersistedCaptureCleanClosureSealReceiptV8:
        raise TypeError(
            "receipt must be an exact PersistedCaptureCleanClosureSealReceiptV8"
        )
    if type(ledger) is not CaptureIntegrityLedgerV2:
        raise TypeError("ledger must be an exact CaptureIntegrityLedgerV2")
    _require_v8_clean_closure_plan_authority(
        promoting_plans,
        depth_plan=depth_plan,
        authority_sha256=ledger.authority.plan_sha256,
    )
    if ledger.writer_lease is None:
        raise CaptureIntegrityLedgerError(
            "persisted V8 CLEAN verification requires an exact held WriterLease"
        )
    with ledger._writer_lease_operation():
        with ledger._lock:
            receipt.__post_init__()
            ledger._raise_if_failed()
            ledger._verify_bound_roots()
            ledger._refresh_events_unlocked()
            ledger._refresh_clean_closure_seal_unlocked()
            if ledger._clean_closure_receipt != receipt:
                raise CaptureIntegrityLedgerIntegrityError(
                    "persisted V8 CLEAN receipt differs from the current seal"
                )
            ledger._validate_clean_closure_seal_against_ledger_unlocked(
                receipt.seal
            )
            _validate_v8_seal_exact_plan_bindings(
                receipt.seal,
                promoting_plans=promoting_plans,
                depth_plan=depth_plan,
            )
            _assert_persisted_clean_closure_file_current(receipt)
            return receipt.seal_sha256


def _require_v2_clean_closure_plan_authority(
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    *,
    authority_sha256: str,
) -> None:
    """Reject V8/foreign authority before V2 irreversibly terminalizes storage."""

    if type(promoting_plans) is not tuple:
        raise TypeError("V2 CLEAN closure requires an exact plan tuple")
    exact_types = (
        ProvisionalPromotingCapturePlanV2,
        ProvisionalPromotingRestCapturePlanV2,
    )
    if any(type(plan) not in exact_types for plan in promoting_plans):
        raise TypeError("V2 CLEAN closure rejects non-exact plan members")
    try:
        validate_provisional_promoting_capture_plans_v2(promoting_plans)
        plan_sha256 = provisional_promoting_plan_sha256_v2(promoting_plans)
    except (TypeError, ValueError) as exc:
        raise CaptureIntegrityLedgerIntegrityError(
            "V2 CLEAN closure requires the exact three-plan authority"
        ) from exc
    if plan_sha256 != authority_sha256:
        raise CaptureIntegrityLedgerIntegrityError(
            "V2 CLEAN closure plan authority differs from the integrity ledger"
        )


def _require_v8_clean_closure_plan_authority(
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    *,
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
    authority_sha256: str,
) -> None:
    """Reject V2, reordered, or foreign V8 authority before block terminalization."""

    if type(promoting_plans) is not tuple:
        raise TypeError("V8 CLEAN closure requires an exact plan tuple")
    if type(depth_plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise TypeError("V8 CLEAN closure requires an exact depth plan")
    try:
        validate_provisional_promoting_capture_plans_v8(promoting_plans)
        plan_sha256 = provisional_promoting_plan_sha256_v8(promoting_plans)
    except (TypeError, ValueError) as exc:
        raise CaptureIntegrityLedgerIntegrityError(
            "V8 CLEAN closure requires the exact four-plan authority"
        ) from exc
    if (
        len(promoting_plans) != 4
        or tuple(plan.route_id for plan in promoting_plans)
        != (
            "usdm_market",
            "usdm_public",
            "usdm_public_rest",
            "usdm_public_depth_rest",
        )
        or sum(plan is depth_plan for plan in promoting_plans) != 1
        or plan_sha256 != authority_sha256
    ):
        raise CaptureIntegrityLedgerIntegrityError(
            "V8 CLEAN plan/depth authority differs from the integrity ledger"
        )


def _require_exact_clean_closure_inputs_v8(
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
    depth_bridge_close_receipt: DepthBridgeCoordinatorCleanCloseReceiptV8,
    finalized_websocket_cursor_pair: FinalizedWebSocketRouteCursorPairV8,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    wal_writer: MirroredWalWriterV2,
    block_writer: GroupedBlockWriterV2,
) -> None:
    _require_exact_clean_closure_inputs_v2(
        finality_receipt=finality_receipt,
        wal_writer=wal_writer,
        block_writer=block_writer,
    )
    validate_depth_bridge_coordinator_clean_close_receipt_v8(
        depth_bridge_close_receipt,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    validate_finalized_websocket_route_cursor_pair_v8(
        finalized_websocket_cursor_pair,
        finality_receipt=finality_receipt,
        promoting_plans=promoting_plans,
    )


def _validate_v8_seal_exact_plan_bindings(
    seal: CaptureCleanClosureSealV8,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> None:
    if type(seal) is not CaptureCleanClosureSealV8:
        raise TypeError("seal must be an exact CaptureCleanClosureSealV8")
    _require_v8_clean_closure_plan_authority(
        promoting_plans,
        depth_plan=depth_plan,
        authority_sha256=seal.plan_bundle_sha256,
    )
    validate_websocket_route_cursor_closure_pair_v8(
        seal.websocket_route_cursor_closure_pair,
        finality_receipt=seal.finality_receipt,
        promoting_plans=promoting_plans,
    )
    expected_websocket_hash = websocket_route_cursor_closure_pair_sha256_v8(
        seal.websocket_route_cursor_closure_pair,
        finality_receipt=seal.finality_receipt,
        promoting_plans=promoting_plans,
    )
    validate_depth_bridge_coordinator_closure_entry_v8(
        seal.depth_bridge_closure_entry,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    expected_bridge_hash = depth_bridge_coordinator_closure_entry_sha256_v8(
        seal.depth_bridge_closure_entry,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    if (
        seal.websocket_route_cursor_closure_pair_sha256
        != expected_websocket_hash
        or seal.depth_bridge_closure_entry_sha256 != expected_bridge_hash
        or seal.depth_plan_sha256
        != seal.depth_bridge_closure_entry.depth_plan_sha256
    ):
        raise CaptureIntegrityLedgerIntegrityError(
            "V8 CLEAN persisted projection hashes differ from exact plans"
        )


def attest_finalized_block_v2(
    block_writer: GroupedBlockWriterV2,
    manifest: BlockManifestV2,
) -> FinalizedBlockReferenceV2:
    """Create a reference only while the exact signed block and root still verify."""

    attested = attest_finalized_block_chain_v2(block_writer)
    if manifest.block_sequence > len(attested) or manifest.block_sequence < 1:
        raise CaptureIntegrityLedgerIntegrityError(
            "finalized-block reference is outside the verified committed chain"
        )
    observed, reference = attested[manifest.block_sequence - 1]
    if canonical_json_line(asdict(observed)) != canonical_json_line(asdict(manifest)):
        raise CaptureIntegrityLedgerIntegrityError(
            "requested finalized-block manifest differs from the verified chain"
        )
    return reference


def attest_finalized_block_chain_v2(
    block_writer: GroupedBlockWriterV2,
) -> tuple[tuple[BlockManifestV2, FinalizedBlockReferenceV2], ...]:
    """Attest every block from one exact signed-chain verification.

    Returned references remain non-current durable evidence.  Consumers that
    need current authority must still perform their complete operation while
    rechecking this chain; this helper only removes repeated whole-prefix scans
    when several records share the same finalized block.
    """

    if type(block_writer) is not GroupedBlockWriterV2:
        raise TypeError("block_writer must be an exact GroupedBlockWriterV2")
    verified = verify_grouped_blocks(
        block_writer.directory,
        authority=block_writer.authority,
        policy=block_writer.policy,
        signing_authority=block_writer.signing_authority,
        stream_group_id=block_writer.stream_group_id,
        segment_id=block_writer.segment_id,
    )
    return tuple(
        (
            observed,
            _reference_from_verified_manifest_v2(
                observed,
                block_directory=block_writer.directory,
                block_root_binding=block_writer.root_binding,
                authority=block_writer.authority,
            ),
        )
        for observed in verified
    )


def verify_finalized_block_reference_v2(
    reference: FinalizedBlockReferenceV2,
    *,
    block_directory: str | Path,
    block_root_binding: StorageRootBindingV2,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
) -> BlockManifestV2:
    """Verify a finalized reference against an explicitly trusted current root."""

    if (
        block_root_binding.storage_kind != "GROUPED_BLOCK"
        or block_root_binding.authority_sha256 != authority.sha256
    ):
        raise CaptureIntegrityLedgerIntegrityError(
            "trusted grouped-block root binding differs from authority"
        )
    verified = verify_grouped_blocks(
        block_directory,
        authority=authority,
        policy=policy,
        signing_authority=signing_authority,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
    )
    if reference.block_sequence < 1 or reference.block_sequence > len(verified):
        raise CaptureIntegrityLedgerIntegrityError(
            "finalized-block reference is outside the verified committed chain"
        )
    manifest = verified[reference.block_sequence - 1]
    expected = _reference_from_verified_manifest_v2(
        manifest,
        block_directory=block_directory,
        block_root_binding=block_root_binding,
        authority=authority,
    )
    if canonical_json_line(asdict(reference)) != canonical_json_line(asdict(expected)):
        raise CaptureIntegrityLedgerIntegrityError(
            "finalized-block reference differs from the trusted current chain"
        )
    return manifest


def _reference_from_verified_manifest_v2(
    manifest: BlockManifestV2,
    *,
    block_directory: str | Path,
    block_root_binding: StorageRootBindingV2,
    authority: WalAuthorityV2,
) -> FinalizedBlockReferenceV2:
    binding_bytes = canonical_json_line(asdict(block_root_binding))
    normalized_directory = _normalized_resolved_path(block_directory)
    _verify_exact_file(
        normalized_directory / "storage-root-binding.json",
        binding_bytes,
        "grouped-block root binding",
    )
    manifest_file = f"block-{manifest.block_sequence:08d}.manifest.json"
    manifest_path = normalized_directory / manifest_file
    try:
        manifest_bytes = manifest_path.read_bytes()
    except OSError as exc:
        raise CaptureIntegrityLedgerIntegrityError(
            "finalized-block manifest is unreadable"
        ) from exc
    expected_manifest_bytes = canonical_json_line(asdict(manifest))
    if manifest_bytes != expected_manifest_bytes:
        raise CaptureIntegrityLedgerIntegrityError(
            "finalized-block manifest bytes are not the verified JCS projection"
        )
    return FinalizedBlockReferenceV2(
        authority_sha256=authority.sha256,
        block_root_binding_sha256=hashlib.sha256(binding_bytes).hexdigest(),
        block_root_path_sha256=_root_path_sha256(normalized_directory),
        block_sequence=manifest.block_sequence,
        block_hash=manifest.block_hash,
        previous_block_hash=manifest.previous_block_hash,
        first_ingest_seq=manifest.first_ingest_seq,
        last_ingest_seq=manifest.last_ingest_seq,
        data_file=manifest.data_file,
        manifest_file=manifest_file,
        expected_container_bytes=manifest.container_bytes,
        expected_container_sha256=manifest.container_sha256,
        expected_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        writer_key_id=manifest.writer_key_id,
        writer_ed25519_signature=manifest.writer_ed25519_signature,
        signing_authority_sha256=manifest.signing_authority_sha256,
    )


def _typed_payload(
    event_type: str,
    payload: dict[str, object],
) -> (
    DataGapPayloadV2
    | SourceGapPayloadV2
    | VoidFinalizedBlockPayloadV2
    | DepthBridgeEvidencePayloadV8
):
    if type(payload) is not dict:
        raise CaptureIntegrityLedgerIntegrityError(
            "integrity event payload must be an object"
        )
    try:
        if event_type == "DATA_GAP":
            return DataGapPayloadV2(**payload)  # type: ignore[arg-type]
        if event_type == "SOURCE_GAP":
            return _source_gap_payload_from_document(payload)
        if event_type == DEPTH_BRIDGE_EVENT_TYPE_V8:
            return parse_depth_bridge_evidence_payload_v8(payload)
        if event_type == "VOID":
            converted = dict(payload)
            kinds = converted.get("corruption_kinds")
            if not isinstance(kinds, (list, tuple)):
                raise TypeError("corruption_kinds must be an array")
            converted["corruption_kinds"] = tuple(kinds)
            return VoidFinalizedBlockPayloadV2(**converted)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise CaptureIntegrityLedgerIntegrityError(
            f"invalid {event_type} integrity payload"
        ) from exc
    raise CaptureIntegrityLedgerIntegrityError(
        "integrity event type is not DATA_GAP, SOURCE_GAP, DEPTH_BRIDGE, or VOID"
    )


def _source_gap_payload_from_document(
    payload: dict[str, object],
) -> SourceGapPayloadV2:
    converted = dict(payload)
    for field_name in ("left_record_locator", "right_record_locator"):
        value = converted.get(field_name)
        if value is None or isinstance(value, FinalizedRecordLocatorV2):
            continue
        if not isinstance(value, dict):
            raise TypeError(f"{field_name} must be an object or null")
        converted[field_name] = FinalizedRecordLocatorV2(**value)  # type: ignore[arg-type]
    return SourceGapPayloadV2(**converted)  # type: ignore[arg-type]


def _unmatched_source_gap_count(
    events: Sequence[CaptureIntegrityEventV2],
) -> int:
    open_ids: set[str] = set()
    bounded_ids: set[str] = set()
    for event in events:
        if event.event_type != "SOURCE_GAP":
            continue
        payload = _typed_payload(event.event_type, event.payload)
        assert isinstance(payload, SourceGapPayloadV2)
        if payload.phase == SourceGapPhaseV2.OPEN.value:
            open_ids.add(payload.gap_id)
        else:
            bounded_ids.add(payload.gap_id)
    if not bounded_ids <= open_ids:
        raise CaptureIntegrityLedgerIntegrityError(
            "SOURCE_GAP BOUNDED set is not a subset of OPEN events"
        )
    return len(open_ids - bounded_ids)


def _depth_bridge_payloads(
    events: Sequence[CaptureIntegrityEventV2],
) -> tuple[DepthBridgeEvidencePayloadV8, ...]:
    payloads: list[DepthBridgeEvidencePayloadV8] = []
    for event in events:
        if event.event_type != DEPTH_BRIDGE_EVENT_TYPE_V8:
            continue
        payload = _typed_payload(event.event_type, event.payload)
        assert isinstance(payload, DepthBridgeEvidencePayloadV8)
        payloads.append(payload)
    return tuple(payloads)


def _depth_bridge_census(
    events: Sequence[CaptureIntegrityEventV2],
) -> DepthBridgeEvidenceCensusV8:
    try:
        return depth_bridge_evidence_census_v8(_depth_bridge_payloads(events))
    except (TypeError, ValueError) as exc:
        raise CaptureIntegrityLedgerIntegrityError(
            "DEPTH_BRIDGE lifecycle evidence is invalid"
        ) from exc


def _last_depth_bridge_drain_event(
    events: Sequence[CaptureIntegrityEventV2],
) -> CaptureIntegrityEventV2:
    drains: list[CaptureIntegrityEventV2] = []
    for event in events:
        if event.event_type != DEPTH_BRIDGE_EVENT_TYPE_V8:
            continue
        payload = _typed_payload(event.event_type, event.payload)
        if (
            type(payload) is DepthBridgeEvidencePayloadV8
            and type(payload.material) is DepthBridgeGenerationDrainedV8
        ):
            drains.append(event)
    if not drains:
        raise CaptureIntegrityLedgerIntegrityError(
            "V8 CLEAN closure lacks a durable generation drain"
        )
    return drains[-1]


def _validate_last_depth_bridge_drain_lineage(
    event: CaptureIntegrityEventV2,
    entry: DepthBridgeCoordinatorClosureEntryV8,
) -> None:
    if type(event) is not CaptureIntegrityEventV2:
        raise TypeError("last drain event must be an exact CaptureIntegrityEventV2")
    if type(entry) is not DepthBridgeCoordinatorClosureEntryV8:
        raise TypeError("bridge closure entry must be an exact V8 projection")
    if event.event_type != DEPTH_BRIDGE_EVENT_TYPE_V8:
        raise CaptureIntegrityLedgerIntegrityError(
            "last bridge drain locator names a foreign event type"
        )
    payload = _typed_payload(event.event_type, event.payload)
    if (
        type(payload) is not DepthBridgeEvidencePayloadV8
        or type(payload.material) is not DepthBridgeGenerationDrainedV8
        or payload.phase != "GENERATION_DRAINED"
        or payload.material.reason != "normal_stop"
        or payload.session_id != entry.session_id
        or payload.protocol_hash != entry.protocol_hash
        or payload.plan_bundle_sha256 != entry.plan_bundle_sha256
        or payload.depth_plan_sha256 != entry.depth_plan_sha256
        or payload.connection_id != entry.last_connection_id
        or payload.connection_generation != entry.last_connection_generation
        or event.event_sequence != entry.last_generation_drained_event_sequence
        or event.sha256 != entry.last_generation_drained_event_sha256
        or event.recorded_wall_ms
        != entry.last_generation_drained_recorded_wall_ms
        or event.recorded_monotonic_ns
        != entry.last_generation_drained_recorded_monotonic_ns
    ):
        raise CaptureIntegrityLedgerIntegrityError(
            "last durable depth-bridge drain differs from its closure entry"
        )


def _intrinsic_depth_bridge_closure_entry_sha256_v8(
    entry: DepthBridgeCoordinatorClosureEntryV8,
) -> str:
    return hashlib.sha256(
        _DEPTH_BRIDGE_CLOSURE_ENTRY_DOMAIN_V8 + canonical_json_line(asdict(entry))
    ).hexdigest()


def _validate_intrinsic_depth_bridge_receipt_digest_v8(
    entry: DepthBridgeCoordinatorClosureEntryV8,
) -> None:
    document = asdict(entry)
    observed = document.pop("receipt_sha256", None)
    document["schema_version"] = (
        "r4b_v2_depth_bridge_coordinator_clean_close_receipt_v8"
    )
    expected = hashlib.sha256(
        _DEPTH_BRIDGE_CLOSE_RECEIPT_DOMAIN_V8 + canonical_json_line(document)
    ).hexdigest()
    if observed != expected:
        raise ValueError("depth bridge closure entry receipt digest differs")


@dataclass(frozen=True, slots=True)
class _IntegrityEventCensusV2:
    source_gap_open_count: int
    source_gap_bounded_count: int
    unmatched_source_gap_open_count: int
    data_gap_count: int
    void_count: int
    depth_bridge_event_count: int
    depth_bridge_generation_started_count: int
    depth_bridge_generation_drained_count: int
    depth_bridge_fatal_generation_count: int
    depth_bridge_last_drain_reason: str | None
    depth_bridge_trigger_count: int
    depth_bridge_cycle_count: int
    depth_bridge_failed_cycle_count: int
    depth_bridge_open_generation_count: int
    depth_bridge_open_cycle_count: int
    depth_bridge_open_attempt_count: int
    depth_bridge_open_wait_count: int


def _integrity_event_census(
    events: Sequence[CaptureIntegrityEventV2],
) -> _IntegrityEventCensusV2:
    source_gap_open_count = 0
    source_gap_bounded_count = 0
    data_gap_count = 0
    void_count = 0
    for event in events:
        if event.event_type == "DATA_GAP":
            data_gap_count += 1
            continue
        if event.event_type == "VOID":
            void_count += 1
            continue
        if event.event_type == DEPTH_BRIDGE_EVENT_TYPE_V8:
            continue
        if event.event_type != "SOURCE_GAP":
            raise CaptureIntegrityLedgerIntegrityError(
                "integrity ledger census found an unsupported event type"
            )
        payload = _typed_payload(event.event_type, event.payload)
        assert isinstance(payload, SourceGapPayloadV2)
        if payload.phase == SourceGapPhaseV2.OPEN.value:
            source_gap_open_count += 1
        else:
            source_gap_bounded_count += 1
    bridge_census = _depth_bridge_census(events)
    return _IntegrityEventCensusV2(
        source_gap_open_count=source_gap_open_count,
        source_gap_bounded_count=source_gap_bounded_count,
        unmatched_source_gap_open_count=_unmatched_source_gap_count(events),
        data_gap_count=data_gap_count,
        void_count=void_count,
        depth_bridge_event_count=bridge_census.event_count,
        depth_bridge_generation_started_count=(
            bridge_census.generation_started_count
        ),
        depth_bridge_generation_drained_count=(
            bridge_census.generation_drained_count
        ),
        depth_bridge_fatal_generation_count=bridge_census.fatal_generation_count,
        depth_bridge_last_drain_reason=bridge_census.last_drain_reason,
        depth_bridge_trigger_count=bridge_census.trigger_count,
        depth_bridge_cycle_count=bridge_census.cycle_count,
        depth_bridge_failed_cycle_count=bridge_census.failed_cycle_count,
        depth_bridge_open_generation_count=bridge_census.open_generation_count,
        depth_bridge_open_cycle_count=bridge_census.open_cycle_count,
        depth_bridge_open_attempt_count=bridge_census.open_attempt_count,
        depth_bridge_open_wait_count=bridge_census.open_wait_count,
    )


def _source_gap_session_boot_identities(
    events: Sequence[CaptureIntegrityEventV2],
) -> set[tuple[str, str]]:
    identities: set[tuple[str, str]] = set()
    for event in events:
        if event.event_type != "SOURCE_GAP":
            continue
        payload = _typed_payload(event.event_type, event.payload)
        assert isinstance(payload, SourceGapPayloadV2)
        identities.add((payload.session_id, payload.process_boot_id))
    return identities


def _detached_event(event: CaptureIntegrityEventV2) -> CaptureIntegrityEventV2:
    """Return a deep canonical copy so callers cannot mutate ledger-owned state."""

    document = json.loads(canonical_json_line(asdict(event)))
    if not isinstance(document, dict):
        raise CaptureIntegrityLedgerIntegrityError(
            "canonical integrity event is not an object"
        )
    return CaptureIntegrityEventV2(**document)  # type: ignore[arg-type]


def _event_id(
    *,
    event_type: str,
    authority_sha256: str,
    ledger_root_binding_sha256: str,
    block_root_binding_sha256: str,
    block_root_path_sha256: str,
    payload: dict[str, object],
) -> str:
    identity = {
        "schema_version": "r4b_v2_capture_integrity_event_identity_v1",
        "event_type": event_type,
        "authority_sha256": authority_sha256,
        "ledger_root_binding_sha256": ledger_root_binding_sha256,
        "block_root_binding_sha256": block_root_binding_sha256,
        "block_root_path_sha256": block_root_path_sha256,
        "payload": payload,
    }
    return hashlib.sha256(_EVENT_ID_DOMAIN + canonical_json_line(identity)).hexdigest()


def _read_event(path: Path) -> CaptureIntegrityEventV2:
    try:
        encoded = path.read_bytes()
        raw = json.loads(encoded)
        if canonical_json_line(raw) != encoded:
            raise ValueError("event is not canonical JCS JSONL")
        return CaptureIntegrityEventV2(**raw)
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as exc:
        raise CaptureIntegrityLedgerIntegrityError(
            f"invalid integrity event: {path.name}"
        ) from exc


def _read_clean_closure_seal(path: Path) -> _CaptureCleanClosureSeal:
    try:
        encoded, _ = _read_exact_regular_file(path, "CLEAN closure seal")
        raw = json.loads(encoded)
        if not isinstance(raw, dict) or canonical_json_line(raw) != encoded:
            raise ValueError("CLEAN closure seal is not canonical JCS JSONL")
        converted = dict(raw)
        receipt_document = converted.get("finality_receipt")
        if not isinstance(receipt_document, dict):
            raise TypeError("CLEAN closure finality receipt must be an object")
        converted["finality_receipt"] = _finality_receipt_from_document(
            receipt_document
        )
        schema_version = converted.get("schema_version")
        if schema_version == _CLEAN_CLOSURE_SEAL_SCHEMA:
            seal: _CaptureCleanClosureSeal = CaptureCleanClosureSealV2(
                **converted  # type: ignore[arg-type]
            )
        elif schema_version == _CLEAN_CLOSURE_SEAL_SCHEMA_V8:
            cursor_documents = converted.get(
                "websocket_route_cursor_closure_pair"
            )
            if not isinstance(cursor_documents, list) or len(cursor_documents) != 2:
                raise TypeError("V8 CLEAN WebSocket cursor pair must be two entries")
            cursor_entries = tuple(
                WebSocketRouteCursorClosureEntryV8(
                    **document  # type: ignore[arg-type]
                )
                for document in cursor_documents
                if isinstance(document, dict)
            )
            if len(cursor_entries) != 2:
                raise TypeError("V8 CLEAN WebSocket cursor entry must be an object")
            converted["websocket_route_cursor_closure_pair"] = cursor_entries
            bridge_document = converted.get("depth_bridge_closure_entry")
            if not isinstance(bridge_document, dict):
                raise TypeError("V8 CLEAN depth bridge entry must be an object")
            converted["depth_bridge_closure_entry"] = (
                DepthBridgeCoordinatorClosureEntryV8(
                    **bridge_document  # type: ignore[arg-type]
                )
            )
            seal = CaptureCleanClosureSealV8(
                **converted  # type: ignore[arg-type]
            )
        else:
            raise ValueError("unknown CLEAN closure seal schema")
        seal.__post_init__()
        return seal
    except CaptureIntegrityLedgerIntegrityError:
        raise
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as exc:
        raise CaptureIntegrityLedgerIntegrityError(
            f"invalid CLEAN closure seal: {path.name}"
        ) from exc


def _require_exact_clean_closure_inputs_v2(
    *,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    wal_writer: MirroredWalWriterV2,
    block_writer: GroupedBlockWriterV2,
) -> None:
    if type(finality_receipt) is not CaptureFinalityFenceReceiptV2:
        raise TypeError(
            "finality_receipt must be an exact CaptureFinalityFenceReceiptV2"
        )
    finality_receipt.__post_init__()
    _require_exact_clean_closure_owners_v2(
        wal_writer=wal_writer,
        block_writer=block_writer,
    )
    if finality_receipt.wal_durability_binding != wal_writer.durability_binding:
        raise CaptureIntegrityLedgerIntegrityError(
            "finality receipt differs from the exact dual-WAL owner"
        )
    if finality_receipt.grouped_block_root_binding != block_writer.root_binding:
        raise CaptureIntegrityLedgerIntegrityError(
            "finality receipt differs from the exact grouped-block owner"
        )


def _require_exact_clean_closure_owners_v2(
    *,
    wal_writer: MirroredWalWriterV2,
    block_writer: GroupedBlockWriterV2,
) -> None:
    if type(wal_writer) is not MirroredWalWriterV2:
        raise TypeError("wal_writer must be an exact MirroredWalWriterV2")
    if type(block_writer) is not GroupedBlockWriterV2:
        raise TypeError("block_writer must be an exact GroupedBlockWriterV2")
    binding = wal_writer.durability_binding
    if binding.mode != "QUALIFIED_DUAL_OWNER" or len(binding.root_bindings) != 2:
        raise CaptureIntegrityLedgerIntegrityError(
            "CLEAN closure requires QUALIFIED_DUAL_OWNER WAL durability"
        )


def _finality_receipt_from_document(
    document: dict[str, object],
) -> CaptureFinalityFenceReceiptV2:
    converted = dict(document)
    wal_document = converted.get("wal_durability_binding")
    if not isinstance(wal_document, dict):
        raise TypeError("finality WAL durability binding must be an object")
    wal_converted = dict(wal_document)
    root_documents = wal_converted.get("root_bindings")
    if not isinstance(root_documents, list):
        raise TypeError("finality WAL root bindings must be an array")
    wal_converted["root_bindings"] = tuple(
        StorageRootBindingV2(**root)  # type: ignore[arg-type]
        for root in root_documents
        if isinstance(root, dict)
    )
    if len(wal_converted["root_bindings"]) != len(root_documents):  # type: ignore[arg-type]
        raise TypeError("finality WAL root binding is not an object")
    converted["wal_durability_binding"] = WalDurabilityBindingV2(
        **wal_converted  # type: ignore[arg-type]
    )
    block_document = converted.get("grouped_block_root_binding")
    if not isinstance(block_document, dict):
        raise TypeError("finality grouped-block root binding must be an object")
    converted["grouped_block_root_binding"] = StorageRootBindingV2(
        **block_document  # type: ignore[arg-type]
    )
    return CaptureFinalityFenceReceiptV2(**converted)  # type: ignore[arg-type]


def _persisted_clean_closure_receipt(
    path: Path,
    seal: _CaptureCleanClosureSeal,
) -> _PersistedCaptureCleanClosureSealReceipt:
    encoded, status = _read_exact_regular_file(path, "CLEAN closure seal")
    if encoded != seal.encoded_line:
        raise CaptureIntegrityLedgerIntegrityError(
            "persisted CLEAN closure bytes differ from the seal value"
        )
    canonical_path = os.path.normcase(os.path.abspath(path))
    common = {
        "canonical_path": canonical_path,
        "file_name": path.name,
        "seal_sha256": hashlib.sha256(encoded).hexdigest(),
        "byte_count": len(encoded),
        "file_device": int(status.st_dev),
        "file_inode": int(status.st_ino),
        "file_nlink": int(status.st_nlink),
    }
    if type(seal) is CaptureCleanClosureSealV2:
        return PersistedCaptureCleanClosureSealReceiptV2(
            seal=seal,
            **common,  # type: ignore[arg-type]
            _factory_token=_PERSISTED_CLEAN_CLOSURE_FACTORY_TOKEN,
        )
    if type(seal) is CaptureCleanClosureSealV8:
        return PersistedCaptureCleanClosureSealReceiptV8(
            seal=seal,
            **common,  # type: ignore[arg-type]
            _factory_token=_PERSISTED_CLEAN_CLOSURE_FACTORY_TOKEN_V8,
        )
    raise TypeError("CLEAN closure persistence rejects a foreign seal type")


def _assert_persisted_clean_closure_file_current(
    receipt: _PersistedCaptureCleanClosureSealReceipt,
) -> None:
    try:
        receipt.__post_init__()
    except (TypeError, ValueError) as exc:
        raise CaptureIntegrityLedgerIntegrityError(
            "persisted CLEAN closure receipt is invalid"
        ) from exc
    path = Path(receipt.canonical_path)
    encoded, status = _read_exact_regular_file(path, "CLEAN closure seal")
    if (
        encoded != receipt.encoded_line
        or hashlib.sha256(encoded).hexdigest() != receipt.seal_sha256
        or int(status.st_size) != receipt.byte_count
        or int(status.st_dev) != receipt.file_device
        or int(status.st_ino) != receipt.file_inode
        or int(status.st_nlink) != receipt.file_nlink
    ):
        raise CaptureIntegrityLedgerIntegrityError(
            "persisted CLEAN closure file identity or bytes differ"
        )


def _read_exact_regular_file(
    path: Path,
    label: str,
) -> tuple[bytes, os.stat_result]:
    try:
        before = path.lstat()
    except OSError as exc:
        raise CaptureIntegrityLedgerIntegrityError(f"{label} is unreadable") from exc
    if not stat.S_ISREG(before.st_mode) or int(before.st_nlink) != 1:
        raise CaptureIntegrityLedgerIntegrityError(
            f"{label} must be one exact regular-file hard link"
        )
    identity = (int(before.st_dev), int(before.st_ino))
    try:
        encoded = path.read_bytes()
        after = path.lstat()
    except OSError as exc:
        raise CaptureIntegrityLedgerIntegrityError(f"{label} is unreadable") from exc
    if (
        not stat.S_ISREG(after.st_mode)
        or int(after.st_nlink) != 1
        or (int(after.st_dev), int(after.st_ino)) != identity
        or int(after.st_size) != len(encoded)
    ):
        raise CaptureIntegrityLedgerIntegrityError(
            f"{label} identity changed during validation"
        )
    return encoded, after


def _reject_unknown_event_artifacts(directory: Path) -> None:
    for path in directory.glob("integrity-event-*"):
        if (
            not path.is_file()
            or (
                _EVENT_RE.fullmatch(path.name) is None
                and _PARTIAL_RE.fullmatch(path.name) is None
            )
        ):
            raise CaptureIntegrityLedgerIntegrityError(
                f"unknown integrity ledger artifact requires audit: {path.name}"
            )


def _reject_unknown_clean_closure_artifacts(directory: Path) -> None:
    allowed = {
        _CLEAN_CLOSURE_SEAL_FILE,
        _CLEAN_CLOSURE_SEAL_PARTIAL_FILE,
    }
    for path in directory.glob(f"{_CLEAN_CLOSURE_SEAL_FILE}*"):
        if path.name not in allowed:
            raise CaptureIntegrityLedgerIntegrityError(
                f"unknown CLEAN closure artifact requires audit: {path.name}"
            )


def _observe_file(path: Path) -> tuple[str, int | None, str | None]:
    if not path.exists() and not path.is_symlink():
        return "MISSING", None, None
    if path.is_symlink() or not path.is_file():
        return "NON_REGULAR", None, None
    try:
        size = path.stat().st_size
        return "PRESENT", size, _sha256_file(path)
    except OSError as exc:
        raise CaptureIntegrityLedgerIntegrityError(
            f"cannot read finalized-block artifact: {path.name}"
        ) from exc


def _derive_corruption_kinds(
    *,
    observed_data_state: str,
    observed_data_bytes: int | None,
    observed_data_sha256: str | None,
    expected_container_bytes: int,
    expected_container_sha256: str,
    observed_manifest_state: str,
    observed_manifest_sha256: str | None,
    expected_manifest_sha256: str,
) -> tuple[str, ...]:
    kinds: list[str] = []
    if observed_data_state == "MISSING":
        kinds.append("DATA_MISSING")
    elif observed_data_state == "NON_REGULAR":
        kinds.append("DATA_NON_REGULAR")
    else:
        if observed_data_bytes != expected_container_bytes:
            kinds.append("DATA_LENGTH_MISMATCH")
        if observed_data_sha256 != expected_container_sha256:
            kinds.append("DATA_SHA256_MISMATCH")
    if observed_manifest_state == "MISSING":
        kinds.append("MANIFEST_MISSING")
    elif observed_manifest_state == "NON_REGULAR":
        kinds.append("MANIFEST_NON_REGULAR")
    elif observed_manifest_sha256 != expected_manifest_sha256:
        kinds.append("MANIFEST_SHA256_MISMATCH")
    if not kinds:
        raise CaptureIntegrityLedgerIntegrityError(
            "VOID observation does not prove finalized-block corruption; block is intact"
        )
    return tuple(kinds)


def _validate_source_gap_left_boundary(payload: SourceGapPayloadV2) -> None:
    cursor_values = (
        payload.left_connection_id,
        payload.left_generation,
        payload.left_frame_seq,
        payload.left_ingest_seq,
    )
    if payload.left_boundary_kind == SourceGapLeftBoundaryV2.SESSION_START.value:
        if any(value is not None for value in cursor_values):
            raise ValueError("SESSION_START SOURCE_GAP cannot claim a retained cursor")
        if payload.cause != SourceGapCauseV2.SESSION_START_PENDING.value:
            raise ValueError("SESSION_START SOURCE_GAP requires SESSION_START_PENDING")
        return
    if any(value is None for value in cursor_values):
        raise ValueError("RETAINED_FRAME SOURCE_GAP requires a complete left cursor")
    assert payload.left_connection_id is not None
    assert payload.left_generation is not None
    assert payload.left_frame_seq is not None
    assert payload.left_ingest_seq is not None
    _validate_identity(payload.left_connection_id, "left_connection_id")
    _validate_positive_value(payload.left_generation, "left_generation")
    _validate_positive_value(payload.left_frame_seq, "left_frame_seq")
    _validate_positive_value(payload.left_ingest_seq, "left_ingest_seq")
    if payload.cause == SourceGapCauseV2.SESSION_START_PENDING.value:
        raise ValueError("RETAINED_FRAME SOURCE_GAP cannot use SESSION_START_PENDING")


def _validate_source_gap_plan_authority(
    authority: WalAuthorityV2,
    promoting_plans: Sequence[ProvisionalPromotingPlanV2],
    selected_plan: ProvisionalPromotingCapturePlanV2,
) -> None:
    if not isinstance(selected_plan, ProvisionalPromotingCapturePlanV2):
        raise TypeError("SOURCE_GAP requires a promoting WebSocket plan")
    observed_plan_sha256 = provisional_promoting_plan_sha256_v2(promoting_plans)
    if observed_plan_sha256 != authority.plan_sha256:
        raise CaptureIntegrityLedgerIntegrityError(
            "SOURCE_GAP plan bundle differs from the WAL authority"
        )
    matches = [
        plan
        for plan in promoting_plans
        if isinstance(plan, ProvisionalPromotingCapturePlanV2)
        and plan == selected_plan
    ]
    if len(matches) != 1:
        raise CaptureIntegrityLedgerIntegrityError(
            "SOURCE_GAP selected WebSocket plan is not unique in its authority bundle"
        )


def _validate_source_gap_plan_authority_v8(
    authority: WalAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    selected_plan: ProvisionalPromotingCapturePlanV2,
) -> None:
    if type(promoting_plans) is not tuple:
        raise TypeError("V8 SOURCE_GAP requires an exact plan tuple")
    validate_provisional_promoting_capture_plans_v8(promoting_plans)
    if type(selected_plan) is not ProvisionalPromotingCapturePlanV2:
        raise TypeError("V8 SOURCE_GAP requires an exact promoting WebSocket plan")
    observed_plan_sha256 = provisional_promoting_plan_sha256_v8(promoting_plans)
    if observed_plan_sha256 != authority.plan_sha256:
        raise CaptureIntegrityLedgerIntegrityError(
            "V8 SOURCE_GAP plan bundle differs from the WAL authority"
        )
    if sum(plan is selected_plan for plan in promoting_plans) != 1:
        raise CaptureIntegrityLedgerIntegrityError(
            "V8 SOURCE_GAP selected WebSocket plan is not the authority object"
        )


def _validate_source_gap_recording_causality(
    event_type: str,
    payload: (
        DataGapPayloadV2
        | SourceGapPayloadV2
        | VoidFinalizedBlockPayloadV2
        | DepthBridgeEvidencePayloadV8
    ),
    event: CaptureIntegrityEventV2,
) -> None:
    if event_type != "SOURCE_GAP":
        return
    if not isinstance(payload, SourceGapPayloadV2):
        raise CaptureIntegrityLedgerIntegrityError(
            "SOURCE_GAP recording causality requires a typed payload"
        )
    if payload.phase == SourceGapPhaseV2.OPEN.value:
        evidence_monotonic_ns = payload.detected_monotonic_ns
        label = "detection"
    else:
        assert payload.right_monotonic_ns is not None
        evidence_monotonic_ns = payload.right_monotonic_ns
        label = "successor cursor"
    if evidence_monotonic_ns > event.recorded_monotonic_ns:
        raise CaptureIntegrityLedgerIntegrityError(
            f"SOURCE_GAP {label} occurs after its durable event recording"
        )


def _validate_source_gap_phase_fields(payload: SourceGapPayloadV2) -> None:
    right_values = (
        payload.right_connection_id,
        payload.right_generation,
        payload.right_frame_seq,
        payload.right_ingest_seq,
        payload.right_wall_ms,
        payload.right_monotonic_ns,
    )
    if payload.phase == SourceGapPhaseV2.OPEN.value:
        if any(value is not None for value in right_values):
            raise ValueError("OPEN SOURCE_GAP cannot claim a right cursor")
        if payload.open_event_sha256 is not None:
            raise ValueError("OPEN SOURCE_GAP cannot reference another OPEN event")
        if (
            payload.left_record_locator is not None
            or payload.right_record_locator is not None
        ):
            raise ValueError("OPEN SOURCE_GAP cannot claim finalized membership")
        return
    if any(value is None for value in right_values):
        raise ValueError("BOUNDED SOURCE_GAP requires a complete right cursor")
    assert payload.right_connection_id is not None
    assert payload.right_generation is not None
    assert payload.right_frame_seq is not None
    assert payload.right_ingest_seq is not None
    assert payload.right_wall_ms is not None
    assert payload.right_monotonic_ns is not None
    _validate_identity(payload.right_connection_id, "right_connection_id")
    _validate_positive_value(payload.right_generation, "right_generation")
    _validate_positive_value(payload.right_frame_seq, "right_frame_seq")
    _validate_positive_value(payload.right_ingest_seq, "right_ingest_seq")
    _validate_nonnegative_value(payload.right_wall_ms, "right_wall_ms")
    _validate_nonnegative_value(payload.right_monotonic_ns, "right_monotonic_ns")
    if payload.open_event_sha256 is None:
        raise ValueError("BOUNDED SOURCE_GAP requires its OPEN event hash")
    _validate_sha256(payload.open_event_sha256, "open_event_sha256")
    if payload.right_record_locator is None:
        raise ValueError("BOUNDED SOURCE_GAP requires a right record locator")
    if payload.right_record_locator.ingest_seq != payload.right_ingest_seq:
        raise ValueError("SOURCE_GAP right locator differs from its cursor")
    if payload.left_boundary_kind == SourceGapLeftBoundaryV2.RETAINED_FRAME.value:
        if payload.left_record_locator is None:
            raise ValueError("retained-frame BOUNDED gap requires a left record locator")
        if payload.left_record_locator.ingest_seq != payload.left_ingest_seq:
            raise ValueError("SOURCE_GAP left locator differs from its cursor")
    elif payload.left_record_locator is not None:
        raise ValueError("session-start BOUNDED gap cannot claim a left locator")
    if payload.right_monotonic_ns <= payload.detected_monotonic_ns:
        raise ValueError("SOURCE_GAP right cursor must follow detection monotonically")
    if (
        payload.left_generation is not None
        and payload.right_generation <= payload.left_generation
    ):
        raise ValueError("SOURCE_GAP right generation must advance")
    if payload.right_frame_seq != 1:
        raise ValueError("SOURCE_GAP right cursor must be successor frame 1")
    if (
        payload.left_connection_id is not None
        and payload.right_connection_id == payload.left_connection_id
    ):
        raise ValueError("SOURCE_GAP requires distinct connection IDs")
    if (
        payload.left_ingest_seq is not None
        and payload.right_ingest_seq <= payload.left_ingest_seq
    ):
        raise ValueError("SOURCE_GAP retained ingest cursors must advance")


def _source_gap_id(payload: SourceGapPayloadV2) -> str:
    return _source_gap_id_from_values(
        session_id=payload.session_id,
        process_boot_id=payload.process_boot_id,
        plan_id=payload.plan_id,
        venue=payload.venue,
        route_id=payload.route_id,
        affected_streams_sha256=payload.affected_streams_sha256,
        affected_stream_count=payload.affected_stream_count,
        cause=payload.cause,
        left_boundary_kind=payload.left_boundary_kind,
        left_connection_id=payload.left_connection_id,
        left_generation=payload.left_generation,
        left_frame_seq=payload.left_frame_seq,
        left_ingest_seq=payload.left_ingest_seq,
        left_wall_ms=payload.left_wall_ms,
        left_monotonic_ns=payload.left_monotonic_ns,
        detected_wall_ms=payload.detected_wall_ms,
        detected_monotonic_ns=payload.detected_monotonic_ns,
        source_component=payload.source_component,
    )


def _finalized_record_locator_sha256(locator: FinalizedRecordLocatorV2) -> str:
    document = asdict(locator)
    document.pop("locator_sha256")
    return hashlib.sha256(
        _FINALIZED_RECORD_LOCATOR_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def _build_finalized_record_locator_v2(
    *,
    authority_sha256: str,
    manifest: BlockManifestV2,
    ingest_seq: int,
    encoded_line: bytes,
) -> FinalizedRecordLocatorV2:
    fields: dict[str, object] = {
        "authority_sha256": authority_sha256,
        "block_sequence": manifest.block_sequence,
        "block_hash": manifest.block_hash,
        "ingest_seq": ingest_seq,
        "record_jsonl_sha256": hashlib.sha256(encoded_line).hexdigest(),
        "schema_version": _FINALIZED_RECORD_LOCATOR_SCHEMA,
    }
    locator_sha256 = hashlib.sha256(
        _FINALIZED_RECORD_LOCATOR_DOMAIN + canonical_json_line(fields)
    ).hexdigest()
    return FinalizedRecordLocatorV2(
        **fields,  # type: ignore[arg-type]
        locator_sha256=locator_sha256,
    )


def _source_gap_id_from_values(
    *,
    session_id: str,
    process_boot_id: str,
    plan_id: str,
    venue: str,
    route_id: str,
    affected_streams_sha256: str,
    affected_stream_count: int,
    cause: str,
    left_boundary_kind: str,
    left_connection_id: str | None,
    left_generation: int | None,
    left_frame_seq: int | None,
    left_ingest_seq: int | None,
    left_wall_ms: int,
    left_monotonic_ns: int,
    detected_wall_ms: int,
    detected_monotonic_ns: int,
    source_component: str,
) -> str:
    identity = {
        "schema_version": "r4b_v2_source_gap_identity_v1",
        "session_id": session_id,
        "process_boot_id": process_boot_id,
        "plan_id": plan_id,
        "venue": venue,
        "route_id": route_id,
        "affected_streams_sha256": affected_streams_sha256,
        "affected_stream_count": affected_stream_count,
        "cause": cause,
        "left_boundary_kind": left_boundary_kind,
        "left_connection_id": left_connection_id,
        "left_generation": left_generation,
        "left_frame_seq": left_frame_seq,
        "left_ingest_seq": left_ingest_seq,
        "left_wall_ms": left_wall_ms,
        "left_monotonic_ns": left_monotonic_ns,
        "detected_wall_ms": detected_wall_ms,
        "detected_monotonic_ns": detected_monotonic_ns,
        "source_component": source_component,
    }
    return hashlib.sha256(
        _SOURCE_GAP_ID_DOMAIN + canonical_json_line(identity)
    ).hexdigest()


def _same_source_gap_scope(
    left: SourceGapPayloadV2,
    right: SourceGapPayloadV2,
) -> bool:
    return (
        left.session_id,
        left.process_boot_id,
        left.plan_id,
        left.venue,
        left.route_id,
        left.affected_streams_sha256,
        left.affected_stream_count,
    ) == (
        right.session_id,
        right.process_boot_id,
        right.plan_id,
        right.venue,
        right.route_id,
        right.affected_streams_sha256,
        right.affected_stream_count,
    )


def _same_source_gap_plan_route(
    left: SourceGapPayloadV2,
    right: SourceGapPayloadV2,
) -> bool:
    return (
        left.session_id,
        left.plan_id,
        left.venue,
        left.route_id,
    ) == (
        right.session_id,
        right.plan_id,
        right.venue,
        right.route_id,
    )


def _same_source_gap_open_identity(
    left: SourceGapPayloadV2,
    right: SourceGapPayloadV2,
) -> bool:
    return left.gap_id == right.gap_id and _source_gap_id(left) == _source_gap_id(right)


def _validate_observation(
    state: str,
    size: int | None,
    sha256: str | None,
    label: str,
) -> None:
    if state not in {"PRESENT", "MISSING", "NON_REGULAR"}:
        raise ValueError(f"observed {label} state is invalid")
    if state == "PRESENT":
        if type(size) is not int or size < 0 or sha256 is None:
            raise ValueError(f"observed {label} metadata is incomplete")
        _validate_sha256(sha256, f"observed_{label}_sha256")
    elif size is not None or sha256 is not None:
        raise ValueError(f"absent/non-regular {label} cannot claim byte metadata")


def _validate_closed_bounds(lower: int, upper: int, label: str) -> None:
    if type(lower) is not int or type(upper) is not int or lower < 0 or upper < lower:
        raise ValueError(f"{label} bounds are invalid")


def _validate_positive_value(value: int, field: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field} must be a positive integer")


def _validate_nonnegative_value(value: int, field: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")


def _validate_nonnegative_integer(value: int, field: str) -> None:
    if type(value) is not int or value < 0:
        raise CaptureIntegrityLedgerIntegrityError(
            f"{field} must be a nonnegative integer"
        )


def _validate_identity(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a bounded normalized identity")


def _validate_local_filename(value: str, field: str) -> None:
    if (
        not value
        or Path(value).name != value
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a local filename")


def _validate_sha256(value: str, field: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")


def _verify_exact_file(path: Path, expected: bytes, label: str) -> None:
    try:
        observed = path.read_bytes()
    except OSError as exc:
        raise CaptureIntegrityLedgerIntegrityError(f"{label} is unreadable") from exc
    if observed != expected:
        raise CaptureIntegrityLedgerIntegrityError(
            f"{label} differs from its immutable authority contract"
        )


def _sequence_from_name(path: Path, pattern: re.Pattern[str]) -> int:
    match = pattern.fullmatch(path.name)
    if match is None:
        raise CaptureIntegrityLedgerIntegrityError(
            f"invalid integrity event filename: {path.name}"
        )
    return int(match.group("sequence"))


def _normalized_resolved_path(value: str | Path) -> Path:
    return Path(os.path.normcase(os.path.realpath(Path(value)))).resolve(strict=False)


def _root_path_sha256(path: Path) -> str:
    normalized = os.path.normcase(os.path.realpath(path))
    return hashlib.sha256(
        _PATH_ID_DOMAIN + canonical_json_line({"normalized_path": normalized})
    ).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _directory_size(directory: Path) -> int:
    return sum(path.stat().st_size for path in directory.rglob("*") if path.is_file())


def _fsync_path(path: Path) -> None:
    with path.open("rb+") as handle:
        os.fsync(handle.fileno())


def _fsync_parent(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
