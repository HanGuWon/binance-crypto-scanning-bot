"""Write-once authority for prospective V2/V8 public-market capture sessions.

The CLEAN closure implemented here is deliberately a local capture prerequisite.
It proves one non-empty, clean-stopped WAL/block tail and one sealed integrity
ledger.  The V8 form additionally binds the exact four-source plan, WebSocket
cursor pair, OI coverage close, and qualification-only depth-bridge close.  It
still does not certify parser/source/book completeness, M2, PAPER execution,
strategy efficacy, PnL, promotion, or production-order authority.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Final, Literal

from signalbot.capture.path_safety import inspect_link_free_path
from signalbot.capture.writer_lease import (
    WriterLease,
    WriterLeaseSessionClosureClaimError,
    WriterLeaseSessionStartClaimError,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import (
    StorageRootBindingV2,
    assert_storage_root_binding_v2,
)
from signalbot.r4b_v2.capture.block_container import BlockSigningAuthorityV2
from signalbot.r4b_v2.capture.blocks import (
    BlockPolicyV2,
    grouped_block_root_contract_v2,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureCleanClosureSealV2,
    CaptureCleanClosureSealV8,
    CaptureIntegrityLedgerV2,
    PersistedCaptureCleanClosureSealReceiptV2,
    PersistedCaptureCleanClosureSealReceiptV8,
    capture_integrity_ledger_root_contract_v2,
    verify_persisted_capture_clean_closure_seal_receipt_v2,
    verify_persisted_capture_clean_closure_seal_receipt_v8,
)
from signalbot.r4b_v2.capture.models import RawRecordV2
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    CaptureFinalityFenceReceiptV2,
    verify_clean_stopped_current_tail_v2,
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
from signalbot.r4b_v2.capture.rest_census import (
    PublicOiRestCoverageCloseV2,
    public_oi_rest_plan_sha256_v2,
)
from signalbot.r4b_v2.capture.rest_depth import public_depth_rest_plan_sha256_v8
from signalbot.r4b_v2.capture.rest_depth_bridge_evidence import (
    DepthBridgeCoordinatorCleanCloseReceiptV8,
    DepthBridgeCoordinatorClosureEntryV8,
    depth_bridge_coordinator_closure_entry_sha256_v8,
    depth_bridge_coordinator_closure_entry_v8,
    validate_depth_bridge_coordinator_clean_close_receipt_v8,
    validate_depth_bridge_coordinator_closure_entry_v8,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2, WalDurabilityBindingV2
from signalbot.r4b_v2.capture.websocket import (
    PublicOiCensusAdmissionReceiptV2,
    validate_public_oi_census_admission_receipt_v2,
)
from signalbot.r4b_v2.capture.websocket_finality import (
    FinalizedWebSocketRouteCursorPairV2,
    FinalizedWebSocketRouteCursorPairV8,
    WebSocketRouteCursorClosureEntryV2,
    WebSocketRouteCursorClosureEntryV8,
    validate_websocket_route_cursor_closure_pair_v2,
    validate_websocket_route_cursor_closure_pair_v8,
    websocket_route_cursor_closure_pair_sha256_v2,
    websocket_route_cursor_closure_pair_sha256_v8,
    websocket_route_cursor_closure_pair_v2,
    websocket_route_cursor_closure_pair_v8,
)

SESSION_CLOSURE_SUPPORTED_V2: Final = False

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PROCESS_BOOT_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_MAX_IDENTITY_LENGTH = 256
_ROOT_BINDING_FILE = "storage-root-binding.json"
_PATH_HASH_DOMAIN = b"R4B2-SESSION-CANONICAL-PATH-V1\0"
_WRITER_LEASE_BACKENDS = frozenset({"WINDOWS_LOCKFILEEX", "POSIX_FLOCK"})
_SCHEMA_VERSION = "r4b_v2_capture_session_start_manifest_v2"
_PURPOSE = "prospective_public_market_capture_authority"
_SESSION_START_FILE_PREFIX = ".signalbot-session-authority-"
_SESSION_START_FILE_SUFFIX = ".start.jsonl"
_SESSION_CLOSURE_FILE_SUFFIX = ".closure.jsonl"
_PERSISTED_AUTHORITY_FACTORY_TOKEN = object()
_PERSISTED_CLOSURE_AUTHORITY_FACTORY_TOKEN = object()
_PERSISTED_CLOSURE_AUTHORITY_FACTORY_TOKEN_V8 = object()
_CLOSURE_SCHEMA_VERSION = "r4b_v2_capture_session_closure_manifest_v2"
_CLOSURE_SCHEMA_VERSION_V8 = "r4b_v2_capture_session_closure_manifest_v8"
_CLOSURE_PURPOSE = "local_clean_capture_closure_prerequisite"
_NORMAL_CLOSURE_STOP_REASONS = frozenset({"COMPLETED_DURATION", "OPERATOR_REQUESTED"})
_PLANNED_SOURCE_CENSUS_DOMAIN = b"R4B_V2_PLANNED_SOURCE_CENSUS\0"
_PLANNED_OI_REST_CENSUS_DOMAIN = b"R4B_V2_PLANNED_OI_REST_CENSUS\0"
_PLANNED_SOURCE_CENSUS_DOMAIN_V8 = b"R4B_V2_PLANNED_SOURCE_CENSUS_V8\0"
_OI_COVERAGE_CLOSE_RECORD_DOMAIN_V8 = b"R4B_V2_OI_COVERAGE_CLOSE_RECORD_V8\0"
_V8_SOURCE_ROUTES = (
    "usdm_market",
    "usdm_public",
    "usdm_public_rest",
    "usdm_public_depth_rest",
)


class SessionAuthorityError(RuntimeError):
    """Base error for V2 session authority construction or persistence."""


class SessionAuthorityIntegrityError(SessionAuthorityError):
    """Raised when current paths or authority bytes cannot prove the manifest."""


class SessionAuthorityExistsError(SessionAuthorityError):
    """Raised when the requested write-once start path already exists."""


class SessionAuthorityWriteError(SessionAuthorityError):
    """Raised when exact durable persistence of a start manifest fails."""


@dataclass(frozen=True, slots=True)
class SessionWriterLeaseBindingV2:
    scope_canonical_path: str
    scope_path_sha256: str
    owner_pid: int
    owner_id: str
    backend: str
    acquired_wall_ms: int
    acquired_monotonic_ns: int
    schema_version: str = "r4b_v2_session_writer_lease_binding_v1"

    def __post_init__(self) -> None:
        if self.schema_version != "r4b_v2_session_writer_lease_binding_v1":
            raise ValueError("unsupported session writer-lease binding schema")
        _require_canonical_absolute_path(
            self.scope_canonical_path,
            "scope_canonical_path",
        )
        _require_sha256(self.scope_path_sha256, "scope_path_sha256")
        if self.scope_path_sha256 != _path_sha256(self.scope_canonical_path):
            raise ValueError("writer-lease scope path hash differs from its path")
        if type(self.owner_pid) is not int or self.owner_pid < 1:
            raise ValueError("writer-lease owner_pid must be a positive integer")
        _require_identity(self.owner_id, "writer-lease owner_id")
        if self.backend not in _WRITER_LEASE_BACKENDS:
            raise ValueError("writer-lease backend is not in the sealed set")
        _require_nonnegative_int(self.acquired_wall_ms, "acquired_wall_ms")
        _require_nonnegative_int(
            self.acquired_monotonic_ns,
            "acquired_monotonic_ns",
        )


@dataclass(frozen=True, slots=True)
class SessionStorageRootReferenceV2:
    canonical_path: str
    path_sha256: str
    root_binding: StorageRootBindingV2
    root_binding_sha256: str
    root_device: str
    root_inode: str
    binding_device: str
    binding_inode: str
    schema_version: str = "r4b_v2_session_storage_root_reference_v1"

    def __post_init__(self) -> None:
        if self.schema_version != "r4b_v2_session_storage_root_reference_v1":
            raise ValueError("unsupported session storage-root reference schema")
        _require_canonical_absolute_path(self.canonical_path, "canonical_path")
        _require_sha256(self.path_sha256, "path_sha256")
        if self.path_sha256 != _path_sha256(self.canonical_path):
            raise ValueError("storage-root path hash differs from its path")
        if not isinstance(self.root_binding, StorageRootBindingV2):
            raise TypeError("root_binding must be a StorageRootBindingV2")
        _validate_storage_root_binding(self.root_binding)
        _require_sha256(self.root_binding_sha256, "root_binding_sha256")
        if self.root_binding_sha256 != _binding_sha256(self.root_binding):
            raise ValueError("storage-root binding hash differs from its binding")
        for field_name in (
            "root_device",
            "root_inode",
            "binding_device",
            "binding_inode",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, str)
                or not value.isascii()
                or not value.isdecimal()
                or value != str(int(value))
            ):
                raise ValueError(f"{field_name} must be a canonical decimal identity")


@dataclass(frozen=True, slots=True)
class SessionStartManifestV2:
    purpose: str
    production_order_execution_enabled: bool
    private_credentials_permitted: bool
    attempt_id: str
    session_id: str
    process_boot_id: str
    writer_lease: SessionWriterLeaseBindingV2
    started_wall_ms: int
    started_monotonic_ns: int
    wal_authority: WalAuthorityV2
    wal_authority_sha256: str
    wal_durability_binding: WalDurabilityBindingV2
    wal_durability_binding_sha256: str
    qualification_selection_receipt_sha256: str
    block_policy: BlockPolicyV2
    block_signing_authority: BlockSigningAuthorityV2
    block_signing_authority_sha256: str
    stream_group_id: str
    segment_id: str
    integrity_ledger_max_events: int
    storage_roots: tuple[
        SessionStorageRootReferenceV2,
        SessionStorageRootReferenceV2,
        SessionStorageRootReferenceV2,
        SessionStorageRootReferenceV2,
    ]
    previous_closure_sha256: str | None
    schema_version: str = _SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("unsupported V2 session-start manifest schema")
        if self.purpose != _PURPOSE:
            raise ValueError("session-start purpose is not prospective public capture")
        if type(self.production_order_execution_enabled) is not bool:
            raise TypeError("production_order_execution_enabled must be a boolean")
        if self.production_order_execution_enabled:
            raise ValueError("production order execution is forbidden")
        if type(self.private_credentials_permitted) is not bool:
            raise TypeError("private_credentials_permitted must be a boolean")
        if self.private_credentials_permitted:
            raise ValueError("private credentials are forbidden")
        for value, label in (
            (self.attempt_id, "attempt_id"),
            (self.session_id, "session_id"),
            (self.stream_group_id, "stream_group_id"),
            (self.segment_id, "segment_id"),
        ):
            _require_identity(value, label)
        if _PROCESS_BOOT_ID_RE.fullmatch(self.process_boot_id) is None:
            raise ValueError("process_boot_id must be a lowercase UUID hex value")
        if not isinstance(self.writer_lease, SessionWriterLeaseBindingV2):
            raise TypeError("writer_lease must be a SessionWriterLeaseBindingV2")
        _require_nonnegative_int(self.started_wall_ms, "started_wall_ms")
        _require_nonnegative_int(
            self.started_monotonic_ns,
            "started_monotonic_ns",
        )
        if self.started_wall_ms < self.writer_lease.acquired_wall_ms:
            raise ValueError("session wall start precedes writer-lease acquisition")
        if self.started_monotonic_ns < self.writer_lease.acquired_monotonic_ns:
            raise ValueError("session monotonic start precedes writer-lease acquisition")
        if self.session_id != f"{self.started_wall_ms}-{self.process_boot_id}":
            raise ValueError("session_id must bind the UTC wall start and process boot ID")
        if not isinstance(self.wal_authority, WalAuthorityV2):
            raise TypeError("wal_authority must be a WalAuthorityV2")
        if self.attempt_id != self.wal_authority.attempt_id:
            raise ValueError("session attempt_id differs from WAL authority")
        _require_sha256(self.wal_authority_sha256, "wal_authority_sha256")
        if self.wal_authority_sha256 != self.wal_authority.sha256:
            raise ValueError("WAL authority hash differs from its authority")
        if not isinstance(self.wal_durability_binding, WalDurabilityBindingV2):
            raise TypeError("wal_durability_binding must be a WalDurabilityBindingV2")
        if self.wal_durability_binding.mode != "QUALIFIED_DUAL_OWNER":
            raise ValueError(
                "prospective live capture requires QUALIFIED_DUAL_OWNER WAL durability"
            )
        _require_sha256(
            self.wal_durability_binding_sha256,
            "wal_durability_binding_sha256",
        )
        if self.wal_durability_binding_sha256 != self.wal_durability_binding.sha256:
            raise ValueError("WAL durability hash differs from its binding")
        _require_sha256(
            self.qualification_selection_receipt_sha256,
            "qualification_selection_receipt_sha256",
        )
        if (
            self.qualification_selection_receipt_sha256
            != self.wal_durability_binding.qualification_selection_receipt_sha256
        ):
            raise ValueError("qualification selection receipt differs from WAL durability binding")
        if not isinstance(self.block_policy, BlockPolicyV2):
            raise TypeError("block_policy must be a BlockPolicyV2")
        if not isinstance(self.block_signing_authority, BlockSigningAuthorityV2):
            raise TypeError("block_signing_authority must be a BlockSigningAuthorityV2")
        _require_sha256(
            self.block_signing_authority_sha256,
            "block_signing_authority_sha256",
        )
        if self.block_signing_authority_sha256 != self.block_signing_authority.sha256:
            raise ValueError("block signing authority hash differs from its exact authority")
        if (
            type(self.integrity_ledger_max_events) is not int
            or not 1 <= self.integrity_ledger_max_events <= 99_999_999
        ):
            raise ValueError("integrity_ledger_max_events is outside the sealed bound")
        if type(self.storage_roots) is not tuple or len(self.storage_roots) != 4:
            raise ValueError(
                "session storage_roots must be the exact ordered dual-WAL/block/ledger tuple"
            )
        if any(
            not isinstance(reference, SessionStorageRootReferenceV2)
            for reference in self.storage_roots
        ):
            raise TypeError("session storage roots must be SessionStorageRootReferenceV2 values")
        self._validate_storage_roots()
        if self.previous_closure_sha256 is not None:
            _require_sha256(
                self.previous_closure_sha256,
                "previous_closure_sha256",
            )

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.encoded_line).hexdigest()

    def _validate_storage_roots(self) -> None:
        wal_roots = self.wal_durability_binding.root_bindings
        observed_wal_roots = tuple(reference.root_binding for reference in self.storage_roots[:2])
        if observed_wal_roots != wal_roots:
            raise ValueError("session WAL storage roots differ from their exact durability order")
        block_binding = self.storage_roots[2].root_binding
        if block_binding.storage_kind != "GROUPED_BLOCK":
            raise ValueError("the third session storage root must be GROUPED_BLOCK")
        ledger_binding = self.storage_roots[3].root_binding
        if ledger_binding.storage_kind != "CAPTURE_INTEGRITY_LEDGER":
            raise ValueError("the fourth session storage root must be CAPTURE_INTEGRITY_LEDGER")
        expected_authority_sha256 = self.wal_authority.sha256
        if any(
            reference.root_binding.authority_sha256 != expected_authority_sha256
            for reference in self.storage_roots
        ):
            raise ValueError("session storage-root authority differs from WAL authority")
        paths = tuple(reference.canonical_path for reference in self.storage_roots)
        path_hashes = tuple(reference.path_sha256 for reference in self.storage_roots)
        if len(set(paths)) != 4 or len(set(path_hashes)) != 4:
            raise ValueError("session storage-root paths must be pairwise distinct")
        scope_path = self.writer_lease.scope_canonical_path
        if any(not _is_strict_descendant(path, scope_path) for path in paths):
            raise ValueError(
                "session storage-root paths must be strict descendants of the writer-lease scope"
            )
        for index, left in enumerate(paths):
            for right in paths[index + 1 :]:
                if _paths_equal_or_nested(left, right):
                    raise ValueError("session storage-root paths must be pairwise non-nested")
        expected_block_contract_sha256 = _contract_sha256(
            grouped_block_root_contract_v2(
                self.block_policy,
                self.block_signing_authority,
                self.stream_group_id,
                self.segment_id,
            )
        )
        if block_binding.contract_sha256 != expected_block_contract_sha256:
            raise ValueError("grouped-block root contract differs from the session block authority")
        expected_ledger_contract_sha256 = _contract_sha256(
            capture_integrity_ledger_root_contract_v2(
                block_root_binding=block_binding,
                block_directory=self.storage_roots[2].canonical_path,
                block_signing_authority=self.block_signing_authority,
                max_events=self.integrity_ledger_max_events,
            )
        )
        if ledger_binding.contract_sha256 != expected_ledger_contract_sha256:
            raise ValueError(
                "integrity-ledger root contract differs from the session block authority"
            )


@dataclass(frozen=True, slots=True, init=False)
class PersistedSessionStartAuthorityV2:
    """Factory-only receipt for the exact persisted session-start pathname.

    A manifest value alone is not network authority: admission additionally
    binds the one file created by :func:`write_session_start_manifest_v2`, its
    pathname identity, exact bytes, and the exact writer-lease acquisition.
    """

    manifest: SessionStartManifestV2
    canonical_path: str
    manifest_sha256: str
    byte_count: int
    file_device: int
    file_inode: int
    file_nlink: int
    writer_lease: SessionWriterLeaseBindingV2
    schema_version: str
    _factory_token: object = field(repr=False, compare=False)

    def __init__(
        self,
        *,
        manifest: SessionStartManifestV2,
        canonical_path: str,
        manifest_sha256: str,
        byte_count: int,
        file_device: int,
        file_inode: int,
        file_nlink: int,
        writer_lease: SessionWriterLeaseBindingV2,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _PERSISTED_AUTHORITY_FACTORY_TOKEN:
            raise TypeError(
                "PersistedSessionStartAuthorityV2 can only be created by the durable writer"
            )
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "canonical_path", canonical_path)
        object.__setattr__(self, "manifest_sha256", manifest_sha256)
        object.__setattr__(self, "byte_count", byte_count)
        object.__setattr__(self, "file_device", file_device)
        object.__setattr__(self, "file_inode", file_inode)
        object.__setattr__(self, "file_nlink", file_nlink)
        object.__setattr__(self, "writer_lease", writer_lease)
        object.__setattr__(
            self,
            "schema_version",
            "r4b_v2_persisted_session_start_authority_v1",
        )
        object.__setattr__(self, "_factory_token", _factory_token)
        self.__post_init__()

    def __post_init__(self) -> None:
        if self._factory_token is not _PERSISTED_AUTHORITY_FACTORY_TOKEN:
            raise ValueError("persisted session-start authority lacks its factory provenance")
        if self.schema_version != "r4b_v2_persisted_session_start_authority_v1":
            raise ValueError("unsupported persisted session-start authority schema")
        if type(self.manifest) is not SessionStartManifestV2:
            raise TypeError("manifest must be an exact SessionStartManifestV2")
        self.manifest.__post_init__()
        _require_canonical_absolute_path(self.canonical_path, "canonical_path")
        _require_sha256(self.manifest_sha256, "manifest_sha256")
        if self.manifest_sha256 != self.manifest.sha256:
            raise ValueError("persisted authority hash differs from its manifest")
        if type(self.byte_count) is not int or self.byte_count < 1:
            raise ValueError("persisted authority byte_count must be positive")
        if self.byte_count != len(self.manifest.encoded_line):
            raise ValueError("persisted authority byte count differs from its manifest")
        for name in ("file_device", "file_inode"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if type(self.file_nlink) is not int or self.file_nlink != 1:
            raise ValueError("persisted session-start authority must bind one hard link")
        if type(self.writer_lease) is not SessionWriterLeaseBindingV2:
            raise TypeError("writer_lease must be an exact SessionWriterLeaseBindingV2")
        if self.writer_lease != self.manifest.writer_lease:
            raise ValueError("persisted authority lease differs from its manifest")
        if not _is_strict_descendant(
            self.canonical_path,
            self.writer_lease.scope_canonical_path,
        ):
            raise ValueError("persisted authority path must be inside its writer-lease scope")
        if any(
            _paths_equal_or_nested(self.canonical_path, root.canonical_path)
            for root in self.manifest.storage_roots
        ):
            raise ValueError("persisted authority path must be non-nested with storage roots")


@dataclass(frozen=True, slots=True)
class PlannedSourceCensusEntryV2:
    """One planned public source; this is not an observed-completeness claim."""

    plan_name: str
    route_id: str
    transport: str
    member_kind: str
    member_count: int
    member_census_sha256: str
    public_unauthenticated: bool
    schema_version: str = "r4b_v2_planned_source_census_entry_v1"

    def __post_init__(self) -> None:
        if self.schema_version != "r4b_v2_planned_source_census_entry_v1":
            raise ValueError("unsupported planned-source census entry schema")
        _require_identity(self.plan_name, "planned source plan_name")
        _require_identity(self.route_id, "planned source route_id")
        if self.transport not in {"WEBSOCKET", "PUBLIC_REST"}:
            raise ValueError("planned source transport is unsupported")
        if self.member_kind not in {"LOGICAL_STREAM", "SYMBOL_REQUEST"}:
            raise ValueError("planned source member kind is unsupported")
        if type(self.member_count) is not int or self.member_count < 1:
            raise ValueError("planned source member_count must be positive")
        _require_sha256(
            self.member_census_sha256,
            "planned source member_census_sha256",
        )
        if type(self.public_unauthenticated) is not bool:
            raise TypeError("public_unauthenticated must be a boolean")
        if not self.public_unauthenticated:
            raise ValueError("planned closure sources must be public and unauthenticated")


@dataclass(frozen=True, slots=True)
class PlannedSourceCensusV2:
    """The exact two-WS plus public-OI plan census, without M2 status."""

    plan_bundle_sha256: str
    entries: tuple[
        PlannedSourceCensusEntryV2,
        PlannedSourceCensusEntryV2,
        PlannedSourceCensusEntryV2,
    ]
    observed_source_completeness_claimed: bool
    m2_certified: bool
    schema_version: str = "r4b_v2_planned_source_census_v1"

    def __post_init__(self) -> None:
        if self.schema_version != "r4b_v2_planned_source_census_v1":
            raise ValueError("unsupported planned-source census schema")
        _require_sha256(self.plan_bundle_sha256, "plan_bundle_sha256")
        if type(self.entries) is not tuple or len(self.entries) != 3:
            raise ValueError(
                "planned source census requires exactly two WebSocket and one OI REST source"
            )
        if any(type(entry) is not PlannedSourceCensusEntryV2 for entry in self.entries):
            raise TypeError(
                "planned source entries must be exact PlannedSourceCensusEntryV2 values"
            )
        for entry in self.entries:
            entry.__post_init__()
        expected_routes = ("usdm_market", "usdm_public", "usdm_public_rest")
        if tuple(entry.route_id for entry in self.entries) != expected_routes:
            raise ValueError("planned source routes differ from the canonical three-source order")
        if tuple(entry.transport for entry in self.entries) != (
            "WEBSOCKET",
            "WEBSOCKET",
            "PUBLIC_REST",
        ):
            raise ValueError("planned source transports differ from two WebSocket plus REST")
        if tuple(entry.member_kind for entry in self.entries) != (
            "LOGICAL_STREAM",
            "LOGICAL_STREAM",
            "SYMBOL_REQUEST",
        ):
            raise ValueError("planned source member kinds differ from the public plan")
        for value, field_name in (
            (
                self.observed_source_completeness_claimed,
                "observed_source_completeness_claimed",
            ),
            (self.m2_certified, "m2_certified"),
        ):
            if type(value) is not bool:
                raise TypeError(f"{field_name} must be a boolean")
            if value:
                raise ValueError("a planned source census cannot claim observed completeness or M2")

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(_PLANNED_SOURCE_CENSUS_DOMAIN + self.encoded_line).hexdigest()


@dataclass(frozen=True, slots=True)
class PlannedSourceCensusEntryV8:
    """One exact V8 source role; still not an observed-completeness claim."""

    plan_name: str
    route_id: str
    transport: Literal["WEBSOCKET", "PUBLIC_REST"]
    member_kind: Literal["LOGICAL_STREAM", "SYMBOL_REQUEST"]
    member_count: int
    member_census_sha256: str
    authority_role: Literal["PROMOTING", "QUALIFICATION_ONLY"]
    public_unauthenticated: Literal[True]
    schema_version: Literal["r4b_v2_planned_source_census_entry_v8"] = (
        "r4b_v2_planned_source_census_entry_v8"
    )

    def __post_init__(self) -> None:
        if self.schema_version != "r4b_v2_planned_source_census_entry_v8":
            raise ValueError("unsupported V8 planned-source census entry schema")
        _require_identity(self.plan_name, "V8 planned source plan_name")
        _require_identity(self.route_id, "V8 planned source route_id")
        if self.transport not in {"WEBSOCKET", "PUBLIC_REST"}:
            raise ValueError("V8 planned source transport is unsupported")
        if self.member_kind not in {"LOGICAL_STREAM", "SYMBOL_REQUEST"}:
            raise ValueError("V8 planned source member kind is unsupported")
        _require_positive_int(self.member_count, "V8 planned source member_count")
        _require_sha256(
            self.member_census_sha256,
            "V8 planned source member_census_sha256",
        )
        if self.authority_role not in {"PROMOTING", "QUALIFICATION_ONLY"}:
            raise ValueError("V8 planned source authority role is unsupported")
        if self.public_unauthenticated is not True:
            raise ValueError("V8 planned closure sources must be public and unauthenticated")


@dataclass(frozen=True, slots=True)
class PlannedSourceCensusV8:
    """Canonical V8 two-WS/OI/depth source roles without outcome claims."""

    plan_bundle_sha256: str
    depth_plan_sha256: str
    entries: tuple[
        PlannedSourceCensusEntryV8,
        PlannedSourceCensusEntryV8,
        PlannedSourceCensusEntryV8,
        PlannedSourceCensusEntryV8,
    ]
    observed_source_completeness_claimed: Literal[False]
    book_completeness_claimed: Literal[False]
    m2_certified: Literal[False]
    promotion_ready: Literal[False]
    schema_version: Literal["r4b_v2_planned_source_census_v8"] = (
        "r4b_v2_planned_source_census_v8"
    )

    def __post_init__(self) -> None:
        if self.schema_version != "r4b_v2_planned_source_census_v8":
            raise ValueError("unsupported V8 planned-source census schema")
        _require_sha256(self.plan_bundle_sha256, "V8 plan_bundle_sha256")
        _require_sha256(self.depth_plan_sha256, "V8 depth_plan_sha256")
        if type(self.entries) is not tuple or len(self.entries) != 4:
            raise ValueError("V8 planned source census requires exactly four source roles")
        if any(type(entry) is not PlannedSourceCensusEntryV8 for entry in self.entries):
            raise TypeError(
                "V8 planned source entries must be exact PlannedSourceCensusEntryV8 values"
            )
        for entry in self.entries:
            entry.__post_init__()
        if tuple(entry.route_id for entry in self.entries) != _V8_SOURCE_ROUTES:
            raise ValueError("V8 planned source routes differ from canonical order")
        if tuple(entry.transport for entry in self.entries) != (
            "WEBSOCKET",
            "WEBSOCKET",
            "PUBLIC_REST",
            "PUBLIC_REST",
        ):
            raise ValueError("V8 source transports differ from the exact four-role plan")
        if tuple(entry.member_kind for entry in self.entries) != (
            "LOGICAL_STREAM",
            "LOGICAL_STREAM",
            "SYMBOL_REQUEST",
            "SYMBOL_REQUEST",
        ):
            raise ValueError("V8 source member kinds differ from the exact four-role plan")
        if tuple(entry.authority_role for entry in self.entries) != (
            "PROMOTING",
            "PROMOTING",
            "PROMOTING",
            "QUALIFICATION_ONLY",
        ):
            raise ValueError(
                "V8 source roles differ from promoting/promoting/promoting/qualification"
            )
        for value, field_name in (
            (
                self.observed_source_completeness_claimed,
                "observed_source_completeness_claimed",
            ),
            (self.book_completeness_claimed, "book_completeness_claimed"),
            (self.m2_certified, "m2_certified"),
            (self.promotion_ready, "promotion_ready"),
        ):
            if value is not False:
                raise ValueError(f"V8 planned source census forbids {field_name}=true")

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(
            _PLANNED_SOURCE_CENSUS_DOMAIN_V8 + self.encoded_line
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class SessionClosureManifestV2:
    """CLEAN-only proof for one exact, non-empty local capture tail."""

    purpose: str
    closure_status: str
    fatal: bool
    production_order_execution_enabled: bool
    private_credentials_permitted: bool
    stop_reason: str
    session_id: str
    process_boot_id: str
    attempt_id: str
    writer_lease: SessionWriterLeaseBindingV2
    session_start_manifest: SessionStartManifestV2
    session_start_manifest_sha256: str
    session_start_canonical_path: str
    session_start_byte_count: int
    session_start_file_device: str
    session_start_file_inode: str
    session_start_file_nlink: str
    plan_bundle_sha256: str
    planned_source_census: PlannedSourceCensusV2
    planned_source_census_sha256: str
    finality_receipt: CaptureFinalityFenceReceiptV2
    finality_receipt_sha256: str
    finality_prefix_proof_sha256: str
    finality_tail_ingest_seq: int
    wal_durable_ack_seq: int
    finalized_block_tail_ingest_seq: int
    exact_prefix_sha256: str
    final_block_sequence: int
    final_block_hash: str
    final_block_manifest_sha256: str
    final_block_container_sha256: str
    ledger_clean_closure_seal: CaptureCleanClosureSealV2
    ledger_clean_closure_seal_sha256: str
    ledger_clean_closure_receipt_sha256: str
    ledger_clean_closure_canonical_path: str
    ledger_clean_closure_file_name: str
    ledger_clean_closure_byte_count: int
    ledger_clean_closure_file_device: str
    ledger_clean_closure_file_inode: str
    ledger_clean_closure_file_nlink: str
    websocket_route_cursors: tuple[WebSocketRouteCursorClosureEntryV2, ...]
    websocket_route_cursors_sha256: str | None
    websocket_route_cursor_finality_persisted: bool
    closed_wall_ms: int
    closed_monotonic_ns: int
    schema_version: str = _CLOSURE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != _CLOSURE_SCHEMA_VERSION:
            raise ValueError("unsupported V2 session-closure manifest schema")
        if self.purpose != _CLOSURE_PURPOSE:
            raise ValueError("session closure purpose is not the local CLEAN prerequisite")
        if self.closure_status != "CLEAN":
            raise ValueError("V2 session closure status must be CLEAN")
        for value, field_name in (
            (self.fatal, "fatal"),
            (
                self.production_order_execution_enabled,
                "production_order_execution_enabled",
            ),
            (self.private_credentials_permitted, "private_credentials_permitted"),
        ):
            if type(value) is not bool:
                raise TypeError(f"{field_name} must be a boolean")
            if value:
                raise ValueError(f"CLEAN session closure forbids {field_name}=true")
        if self.stop_reason not in _NORMAL_CLOSURE_STOP_REASONS:
            raise ValueError("CLEAN session closure requires a normal stop reason")
        for value, field_name in (
            (self.session_id, "session_id"),
            (self.attempt_id, "attempt_id"),
        ):
            _require_identity(value, field_name)
        if _PROCESS_BOOT_ID_RE.fullmatch(self.process_boot_id) is None:
            raise ValueError("process_boot_id must be a lowercase UUID hex value")
        if type(self.writer_lease) is not SessionWriterLeaseBindingV2:
            raise TypeError("writer_lease must be an exact SessionWriterLeaseBindingV2")
        self.writer_lease.__post_init__()
        if type(self.session_start_manifest) is not SessionStartManifestV2:
            raise TypeError("session_start_manifest must be an exact SessionStartManifestV2")
        self.session_start_manifest.__post_init__()
        start = self.session_start_manifest
        if (
            self.session_id != start.session_id
            or self.process_boot_id != start.process_boot_id
            or self.attempt_id != start.attempt_id
            or self.writer_lease != start.writer_lease
        ):
            raise ValueError("session closure identity differs from its exact start manifest")
        _require_sha256(
            self.session_start_manifest_sha256,
            "session_start_manifest_sha256",
        )
        if self.session_start_manifest_sha256 != start.sha256:
            raise ValueError("session closure start hash differs from its manifest")
        _require_canonical_absolute_path(
            self.session_start_canonical_path,
            "session_start_canonical_path",
        )
        _require_positive_int(self.session_start_byte_count, "session_start_byte_count")
        if self.session_start_byte_count != len(start.encoded_line):
            raise ValueError("session closure start byte count differs")
        _require_decimal_file_identity(
            self.session_start_file_device,
            self.session_start_file_inode,
            self.session_start_file_nlink,
            "session start",
        )
        if not _is_strict_descendant(
            self.session_start_canonical_path,
            self.writer_lease.scope_canonical_path,
        ):
            raise ValueError("session closure start path is outside its lease scope")
        _require_sha256(self.plan_bundle_sha256, "plan_bundle_sha256")
        if self.plan_bundle_sha256 != start.wal_authority.plan_sha256:
            raise ValueError("session closure plan bundle differs from its start authority")
        if type(self.planned_source_census) is not PlannedSourceCensusV2:
            raise TypeError("planned_source_census must be an exact PlannedSourceCensusV2")
        self.planned_source_census.__post_init__()
        if self.planned_source_census.plan_bundle_sha256 != self.plan_bundle_sha256:
            raise ValueError("planned source census differs from the plan bundle")
        _require_sha256(
            self.planned_source_census_sha256,
            "planned_source_census_sha256",
        )
        if self.planned_source_census_sha256 != self.planned_source_census.sha256:
            raise ValueError("planned source census hash differs from its census")
        self._validate_finality()
        self._validate_ledger_seal()
        self._validate_websocket_route_cursor_persistence()
        _require_nonnegative_int(self.closed_wall_ms, "closed_wall_ms")
        _require_nonnegative_int(self.closed_monotonic_ns, "closed_monotonic_ns")
        if self.closed_wall_ms < max(
            start.started_wall_ms,
            self.finality_receipt.target_last_receipt_wall_ms,
            self.ledger_clean_closure_seal.seal_wall_ms,
        ):
            raise ValueError("session closure wall clock precedes start, finality, or seal")
        if self.closed_monotonic_ns < max(
            start.started_monotonic_ns,
            self.finality_receipt.writer_observed_monotonic_ns,
            self.ledger_clean_closure_seal.seal_monotonic_ns,
        ):
            raise ValueError("session closure monotonic clock precedes start, finality, or seal")

    def _validate_finality(self) -> None:
        if type(self.finality_receipt) is not CaptureFinalityFenceReceiptV2:
            raise TypeError("finality_receipt must be an exact CaptureFinalityFenceReceiptV2")
        self.finality_receipt.__post_init__()
        receipt = self.finality_receipt
        _require_sha256(self.finality_receipt_sha256, "finality_receipt_sha256")
        if self.finality_receipt_sha256 != receipt.sha256:
            raise ValueError("session closure finality receipt hash differs")
        _require_sha256(
            self.finality_prefix_proof_sha256,
            "finality_prefix_proof_sha256",
        )
        if self.finality_prefix_proof_sha256 != receipt.prefix_proof_sha256:
            raise ValueError("session closure stable prefix proof differs")
        expected = {
            "finality_tail_ingest_seq": receipt.fence_ingest_seq,
            "wal_durable_ack_seq": receipt.wal_durable_ack_seq,
            "finalized_block_tail_ingest_seq": receipt.finalized_block_tail_ingest_seq,
            "exact_prefix_sha256": receipt.exact_prefix_sha256,
            "final_block_sequence": receipt.final_block_sequence,
            "final_block_hash": receipt.final_block_hash,
            "final_block_manifest_sha256": receipt.final_block_manifest_sha256,
            "final_block_container_sha256": receipt.final_block_container_sha256,
        }
        if any(getattr(self, name) != value for name, value in expected.items()):
            raise ValueError("session closure WAL/block heads differ from finality")
        if (
            receipt.fence_ingest_seq < 1
            or receipt.attempt_id != self.attempt_id
            or receipt.authority_sha256 != self.session_start_manifest.wal_authority.sha256
            or receipt.wal_durability_binding != self.session_start_manifest.wal_durability_binding
            or receipt.grouped_block_root_binding
            != self.session_start_manifest.storage_roots[2].root_binding
            or receipt.block_signing_authority_sha256
            != self.session_start_manifest.block_signing_authority_sha256
            or receipt.stream_group_id != self.session_start_manifest.stream_group_id
            or receipt.segment_id != self.session_start_manifest.segment_id
        ):
            raise ValueError("session closure finality differs from its start authority")

    def _validate_ledger_seal(self) -> None:
        if type(self.ledger_clean_closure_seal) is not CaptureCleanClosureSealV2:
            raise TypeError("ledger_clean_closure_seal must be an exact CaptureCleanClosureSealV2")
        self.ledger_clean_closure_seal.__post_init__()
        seal = self.ledger_clean_closure_seal
        if (
            seal.session_id != self.session_id
            or seal.process_boot_id != self.process_boot_id
            or seal.attempt_id != self.attempt_id
            or seal.authority_sha256 != self.session_start_manifest.wal_authority.sha256
            or seal.finality_receipt != self.finality_receipt
            or seal.ledger_root_binding_sha256
            != self.session_start_manifest.storage_roots[3].root_binding_sha256
            or seal.block_root_binding_sha256
            != self.session_start_manifest.storage_roots[2].root_binding_sha256
            or seal.event_count < 0
            or seal.unmatched_source_gap_open_count != 0
            or seal.void_count != 0
        ):
            raise ValueError("session closure ledger seal differs from the CLEAN authority")
        _require_sha256(
            self.ledger_clean_closure_seal_sha256,
            "ledger_clean_closure_seal_sha256",
        )
        if self.ledger_clean_closure_seal_sha256 != seal.sha256:
            raise ValueError("session closure ledger seal hash differs")
        _require_sha256(
            self.ledger_clean_closure_receipt_sha256,
            "ledger_clean_closure_receipt_sha256",
        )
        _require_canonical_absolute_path(
            self.ledger_clean_closure_canonical_path,
            "ledger_clean_closure_canonical_path",
        )
        _require_identity(
            self.ledger_clean_closure_file_name,
            "ledger_clean_closure_file_name",
        )
        if Path(self.ledger_clean_closure_canonical_path).name != (
            self.ledger_clean_closure_file_name
        ):
            raise ValueError("ledger CLEAN seal path and file name differ")
        _require_positive_int(
            self.ledger_clean_closure_byte_count,
            "ledger_clean_closure_byte_count",
        )
        if self.ledger_clean_closure_byte_count != len(seal.encoded_line):
            raise ValueError("ledger CLEAN seal byte count differs")
        _require_decimal_file_identity(
            self.ledger_clean_closure_file_device,
            self.ledger_clean_closure_file_inode,
            self.ledger_clean_closure_file_nlink,
            "ledger CLEAN seal",
        )
        ledger_root_path = self.session_start_manifest.storage_roots[3].canonical_path
        if not _is_strict_descendant(
            self.ledger_clean_closure_canonical_path,
            ledger_root_path,
        ):
            raise ValueError("ledger CLEAN seal path is outside the bound ledger root")

    def _validate_websocket_route_cursor_persistence(self) -> None:
        if type(self.websocket_route_cursors) is not tuple:
            raise TypeError("websocket_route_cursors must be an exact tuple")
        if type(self.websocket_route_cursor_finality_persisted) is not bool:
            raise TypeError(
                "websocket_route_cursor_finality_persisted must be a boolean"
            )
        if not self.websocket_route_cursors:
            if self.websocket_route_cursors_sha256 is not None:
                raise ValueError("absent WebSocket route cursors forbid a pair hash")
            if self.websocket_route_cursor_finality_persisted:
                raise ValueError(
                    "absent WebSocket route cursors cannot claim persisted finality"
                )
            return
        if len(self.websocket_route_cursors) != 2:
            raise ValueError(
                "persisted WebSocket route cursors require the exact market/public pair"
            )
        pair = (self.websocket_route_cursors[0], self.websocket_route_cursors[1])
        validate_websocket_route_cursor_closure_pair_v2(pair)
        if self.websocket_route_cursor_finality_persisted is not True:
            raise ValueError(
                "present WebSocket route cursors require persisted local finality"
            )
        if self.websocket_route_cursors_sha256 is None:
            raise ValueError("persisted WebSocket route cursors require a pair hash")
        _require_sha256(
            self.websocket_route_cursors_sha256,
            "websocket_route_cursors_sha256",
        )
        if self.websocket_route_cursors_sha256 != (
            websocket_route_cursor_closure_pair_sha256_v2(pair)
        ):
            raise ValueError("persisted WebSocket route cursor pair hash differs")
        planned_websocket = self.planned_source_census.entries[:2]
        for entry, planned in zip(pair, planned_websocket, strict=True):
            expected_scope = (
                self.session_id,
                self.process_boot_id,
                self.plan_bundle_sha256,
                planned.plan_name,
                planned.route_id,
                planned.member_census_sha256,
                planned.member_count,
            )
            observed_scope = (
                entry.session_id,
                entry.process_boot_id,
                entry.plan_bundle_sha256,
                entry.plan_id,
                entry.route_id,
                entry.stream_census_sha256,
                entry.stream_count,
            )
            if observed_scope != expected_scope:
                raise ValueError(
                    "persisted WebSocket route cursor differs from session scope"
                )
            expected_finality = (
                self.finality_receipt_sha256,
                self.finality_receipt.authority_sha256,
                self.exact_prefix_sha256,
                self.finality_prefix_proof_sha256,
                self.finality_tail_ingest_seq,
            )
            observed_finality = (
                entry.finality_receipt_sha256,
                entry.finality_authority_sha256,
                entry.finality_exact_prefix_sha256,
                entry.finality_prefix_proof_sha256,
                entry.finality_tail_ingest_seq,
            )
            if observed_finality != expected_finality:
                raise ValueError(
                    "persisted WebSocket route cursor differs from session finality"
                )
            if (
                entry.stop_observed_monotonic_ns
                > self.finality_receipt.fence_monotonic_ns
            ):
                raise ValueError(
                    "persisted WebSocket owner stop occurs after the finality fence"
                )

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.encoded_line).hexdigest()


@dataclass(frozen=True, slots=True)
class SessionClosureManifestV8:
    """Write-once infrastructure-CLEAN proof for the exact V8 four-source tail."""

    purpose: str
    closure_status: Literal["CLEAN"]
    fatal: Literal[False]
    production_order_execution_enabled: Literal[False]
    private_credentials_permitted: Literal[False]
    stop_reason: str
    session_id: str
    process_boot_id: str
    attempt_id: str
    writer_lease: SessionWriterLeaseBindingV2
    session_start_manifest: SessionStartManifestV2
    session_start_manifest_sha256: str
    session_start_canonical_path: str
    session_start_byte_count: int
    session_start_file_device: str
    session_start_file_inode: str
    session_start_file_nlink: str
    plan_bundle_sha256: str
    depth_plan_sha256: str
    planned_source_census: PlannedSourceCensusV8
    planned_source_census_sha256: str
    finality_receipt: CaptureFinalityFenceReceiptV2
    finality_receipt_sha256: str
    finality_prefix_proof_sha256: str
    finality_tail_ingest_seq: int
    wal_durable_ack_seq: int
    finalized_block_tail_ingest_seq: int
    exact_prefix_sha256: str
    final_block_sequence: int
    final_block_hash: str
    final_block_manifest_sha256: str
    final_block_container_sha256: str
    ledger_clean_closure_seal: CaptureCleanClosureSealV8
    ledger_clean_closure_seal_sha256: str
    ledger_clean_closure_receipt_sha256: str
    ledger_clean_closure_canonical_path: str
    ledger_clean_closure_file_name: str
    ledger_clean_closure_byte_count: int
    ledger_clean_closure_file_device: str
    ledger_clean_closure_file_inode: str
    ledger_clean_closure_file_nlink: str
    websocket_route_cursors: tuple[
        WebSocketRouteCursorClosureEntryV8,
        WebSocketRouteCursorClosureEntryV8,
    ]
    websocket_route_cursors_sha256: str
    websocket_route_cursor_finality_persisted: Literal[True]
    depth_bridge_closure_entry: DepthBridgeCoordinatorClosureEntryV8
    depth_bridge_closure_entry_sha256: str
    oi_coverage_close: PublicOiRestCoverageCloseV2
    oi_coverage_close_sha256: str
    oi_coverage_close_record_sha256: str
    oi_coverage_close_accepted_ingest_seq: int
    oi_coverage_close_receipt_wall_ms: int
    oi_coverage_close_receipt_monotonic_ns: int
    depth_bridge_lifecycle_cleanly_closed: Literal[True]
    retained_frame_parser_health_claimed: Literal[False]
    observed_source_completeness_claimed: Literal[False]
    book_completeness_claimed: Literal[False]
    m2_certified: Literal[False]
    paper_execution_enabled: Literal[False]
    promotion_ready: Literal[False]
    closed_wall_ms: int
    closed_monotonic_ns: int
    schema_version: Literal["r4b_v2_capture_session_closure_manifest_v8"] = (
        _CLOSURE_SCHEMA_VERSION_V8
    )

    def __post_init__(self) -> None:
        if self.schema_version != _CLOSURE_SCHEMA_VERSION_V8:
            raise ValueError("unsupported V8 session-closure manifest schema")
        if self.purpose != _CLOSURE_PURPOSE:
            raise ValueError("V8 session closure purpose is not the local CLEAN prerequisite")
        if self.closure_status != "CLEAN":
            raise ValueError("V8 session closure status must be CLEAN")
        for value, field_name in (
            (self.fatal, "fatal"),
            (self.production_order_execution_enabled, "production_order_execution_enabled"),
            (self.private_credentials_permitted, "private_credentials_permitted"),
            (self.retained_frame_parser_health_claimed, "retained_frame_parser_health_claimed"),
            (self.observed_source_completeness_claimed, "observed_source_completeness_claimed"),
            (self.book_completeness_claimed, "book_completeness_claimed"),
            (self.m2_certified, "m2_certified"),
            (self.paper_execution_enabled, "paper_execution_enabled"),
            (self.promotion_ready, "promotion_ready"),
        ):
            if value is not False:
                raise ValueError(f"V8 infrastructure CLEAN forbids {field_name}=true")
        if self.depth_bridge_lifecycle_cleanly_closed is not True:
            raise ValueError("V8 session closure requires a cleanly closed depth bridge")
        if self.websocket_route_cursor_finality_persisted is not True:
            raise ValueError("V8 session closure requires persisted WebSocket cursor finality")
        if self.stop_reason not in _NORMAL_CLOSURE_STOP_REASONS:
            raise ValueError("V8 CLEAN session closure requires a normal stop reason")
        self._validate_start_binding()
        self._validate_plan_census()
        self._validate_finality()
        self._validate_ledger_receipt_projection()
        self._validate_websocket_and_bridge()
        self._validate_oi_coverage_close()
        _require_nonnegative_int(self.closed_wall_ms, "V8 closed_wall_ms")
        _require_nonnegative_int(self.closed_monotonic_ns, "V8 closed_monotonic_ns")
        if self.closed_wall_ms < max(
            self.session_start_manifest.started_wall_ms,
            self.finality_receipt.target_last_receipt_wall_ms,
            self.ledger_clean_closure_seal.seal_wall_ms,
            self.depth_bridge_closure_entry.close_wall_ms,
            self.oi_coverage_close_receipt_wall_ms,
        ):
            raise ValueError("V8 session closure wall clock precedes a bound terminal fact")
        if self.closed_monotonic_ns < max(
            self.session_start_manifest.started_monotonic_ns,
            self.finality_receipt.writer_observed_monotonic_ns,
            self.ledger_clean_closure_seal.seal_monotonic_ns,
            self.depth_bridge_closure_entry.close_monotonic_ns,
            self.oi_coverage_close_receipt_monotonic_ns,
        ):
            raise ValueError("V8 session closure monotonic clock precedes a bound terminal fact")

    def _validate_start_binding(self) -> None:
        for value, field_name in (
            (self.session_id, "session_id"),
            (self.attempt_id, "attempt_id"),
        ):
            _require_identity(value, field_name)
        if _PROCESS_BOOT_ID_RE.fullmatch(self.process_boot_id) is None:
            raise ValueError("process_boot_id must be a lowercase UUID hex value")
        if type(self.writer_lease) is not SessionWriterLeaseBindingV2:
            raise TypeError("writer_lease must be an exact SessionWriterLeaseBindingV2")
        self.writer_lease.__post_init__()
        if type(self.session_start_manifest) is not SessionStartManifestV2:
            raise TypeError("session_start_manifest must be an exact SessionStartManifestV2")
        self.session_start_manifest.__post_init__()
        start = self.session_start_manifest
        if (
            self.session_id != start.session_id
            or self.process_boot_id != start.process_boot_id
            or self.attempt_id != start.attempt_id
            or self.writer_lease != start.writer_lease
        ):
            raise ValueError("V8 session closure identity differs from its exact start manifest")
        _require_sha256(self.session_start_manifest_sha256, "session_start_manifest_sha256")
        if self.session_start_manifest_sha256 != start.sha256:
            raise ValueError("V8 session closure start hash differs from its manifest")
        _require_canonical_absolute_path(
            self.session_start_canonical_path,
            "session_start_canonical_path",
        )
        _require_positive_int(self.session_start_byte_count, "session_start_byte_count")
        if self.session_start_byte_count != len(start.encoded_line):
            raise ValueError("V8 session closure start byte count differs")
        _require_decimal_file_identity(
            self.session_start_file_device,
            self.session_start_file_inode,
            self.session_start_file_nlink,
            "V8 session start",
        )
        if not _is_strict_descendant(
            self.session_start_canonical_path,
            self.writer_lease.scope_canonical_path,
        ):
            raise ValueError("V8 session closure start path is outside its lease scope")

    def _validate_plan_census(self) -> None:
        _require_sha256(self.plan_bundle_sha256, "V8 plan_bundle_sha256")
        _require_sha256(self.depth_plan_sha256, "V8 depth_plan_sha256")
        start = self.session_start_manifest
        if self.plan_bundle_sha256 != start.wal_authority.plan_sha256:
            raise ValueError("V8 session closure plan bundle differs from its start authority")
        if type(self.planned_source_census) is not PlannedSourceCensusV8:
            raise TypeError("planned_source_census must be an exact PlannedSourceCensusV8")
        self.planned_source_census.__post_init__()
        if (
            self.planned_source_census.plan_bundle_sha256 != self.plan_bundle_sha256
            or self.planned_source_census.depth_plan_sha256 != self.depth_plan_sha256
        ):
            raise ValueError("V8 planned source census differs from its plan authority")
        _require_sha256(self.planned_source_census_sha256, "planned_source_census_sha256")
        if self.planned_source_census_sha256 != self.planned_source_census.sha256:
            raise ValueError("V8 planned source census hash differs")

    def _validate_finality(self) -> None:
        if type(self.finality_receipt) is not CaptureFinalityFenceReceiptV2:
            raise TypeError("finality_receipt must be an exact CaptureFinalityFenceReceiptV2")
        self.finality_receipt.__post_init__()
        receipt = self.finality_receipt
        _require_sha256(self.finality_receipt_sha256, "finality_receipt_sha256")
        _require_sha256(self.finality_prefix_proof_sha256, "finality_prefix_proof_sha256")
        expected = {
            "finality_receipt_sha256": receipt.sha256,
            "finality_prefix_proof_sha256": receipt.prefix_proof_sha256,
            "finality_tail_ingest_seq": receipt.fence_ingest_seq,
            "wal_durable_ack_seq": receipt.wal_durable_ack_seq,
            "finalized_block_tail_ingest_seq": receipt.finalized_block_tail_ingest_seq,
            "exact_prefix_sha256": receipt.exact_prefix_sha256,
            "final_block_sequence": receipt.final_block_sequence,
            "final_block_hash": receipt.final_block_hash,
            "final_block_manifest_sha256": receipt.final_block_manifest_sha256,
            "final_block_container_sha256": receipt.final_block_container_sha256,
        }
        if any(getattr(self, name) != value for name, value in expected.items()):
            raise ValueError("V8 session closure WAL/block heads differ from finality")
        start = self.session_start_manifest
        if (
            receipt.fence_ingest_seq < 1
            or receipt.attempt_id != self.attempt_id
            or receipt.authority_sha256 != start.wal_authority.sha256
            or receipt.wal_durability_binding != start.wal_durability_binding
            or receipt.grouped_block_root_binding != start.storage_roots[2].root_binding
            or receipt.block_signing_authority_sha256 != start.block_signing_authority_sha256
            or receipt.stream_group_id != start.stream_group_id
            or receipt.segment_id != start.segment_id
        ):
            raise ValueError("V8 session closure finality differs from its start authority")

    def _validate_ledger_receipt_projection(self) -> None:
        if type(self.ledger_clean_closure_seal) is not CaptureCleanClosureSealV8:
            raise TypeError("ledger_clean_closure_seal must be an exact CaptureCleanClosureSealV8")
        self.ledger_clean_closure_seal.__post_init__()
        seal = self.ledger_clean_closure_seal
        start = self.session_start_manifest
        if (
            seal.session_id != self.session_id
            or seal.process_boot_id != self.process_boot_id
            or seal.protocol_hash != start.wal_authority.protocol_sha256
            or seal.plan_bundle_sha256 != self.plan_bundle_sha256
            or seal.depth_plan_sha256 != self.depth_plan_sha256
            or seal.attempt_id != self.attempt_id
            or seal.authority_sha256 != start.wal_authority.sha256
            or seal.finality_receipt != self.finality_receipt
            or seal.ledger_root_binding_sha256 != start.storage_roots[3].root_binding_sha256
            or seal.block_root_binding_sha256 != start.storage_roots[2].root_binding_sha256
            or seal.unmatched_source_gap_open_count != 0
            or seal.void_count != 0
        ):
            raise ValueError("V8 session closure ledger seal differs from CLEAN authority")
        _require_sha256(self.ledger_clean_closure_seal_sha256, "ledger_clean_closure_seal_sha256")
        if self.ledger_clean_closure_seal_sha256 != seal.sha256:
            raise ValueError("V8 session closure ledger seal hash differs")
        _require_sha256(
            self.ledger_clean_closure_receipt_sha256,
            "ledger_clean_closure_receipt_sha256",
        )
        _require_canonical_absolute_path(
            self.ledger_clean_closure_canonical_path,
            "ledger_clean_closure_canonical_path",
        )
        _require_identity(
            self.ledger_clean_closure_file_name,
            "ledger_clean_closure_file_name",
        )
        if (
            Path(self.ledger_clean_closure_canonical_path).name
            != self.ledger_clean_closure_file_name
        ):
            raise ValueError("V8 ledger CLEAN seal path and file name differ")
        _require_positive_int(
            self.ledger_clean_closure_byte_count,
            "ledger_clean_closure_byte_count",
        )
        if self.ledger_clean_closure_byte_count != len(seal.encoded_line):
            raise ValueError("V8 ledger CLEAN seal byte count differs")
        _require_decimal_file_identity(
            self.ledger_clean_closure_file_device,
            self.ledger_clean_closure_file_inode,
            self.ledger_clean_closure_file_nlink,
            "V8 ledger CLEAN seal",
        )
        if not _is_strict_descendant(
            self.ledger_clean_closure_canonical_path,
            start.storage_roots[3].canonical_path,
        ):
            raise ValueError("V8 ledger CLEAN seal path is outside the bound ledger root")

    def _validate_websocket_and_bridge(self) -> None:
        validate_websocket_route_cursor_closure_pair_v8(
            self.websocket_route_cursors,
            finality_receipt=self.finality_receipt,
        )
        _require_sha256(self.websocket_route_cursors_sha256, "websocket_route_cursors_sha256")
        if self.websocket_route_cursors_sha256 != websocket_route_cursor_closure_pair_sha256_v8(
            self.websocket_route_cursors,
            finality_receipt=self.finality_receipt,
        ):
            raise ValueError("V8 persisted WebSocket route cursor pair hash differs")
        if type(self.depth_bridge_closure_entry) is not DepthBridgeCoordinatorClosureEntryV8:
            raise TypeError("depth_bridge_closure_entry must be an exact V8 projection")
        self.depth_bridge_closure_entry.__post_init__()
        _require_sha256(self.depth_bridge_closure_entry_sha256, "depth_bridge_closure_entry_sha256")
        seal = self.ledger_clean_closure_seal
        if (
            self.websocket_route_cursors != seal.websocket_route_cursor_closure_pair
            or self.websocket_route_cursors_sha256
            != seal.websocket_route_cursor_closure_pair_sha256
            or self.depth_bridge_closure_entry != seal.depth_bridge_closure_entry
            or self.depth_bridge_closure_entry_sha256
            != seal.depth_bridge_closure_entry_sha256
        ):
            raise ValueError(
                "V8 session closure cursor/bridge projections differ from ledger CLEAN"
            )
        market_cursor, public_cursor = self.websocket_route_cursors
        bridge = self.depth_bridge_closure_entry
        if (
            market_cursor.route_id != "usdm_market"
            or public_cursor.route_id != "usdm_public"
            or market_cursor.session_id != self.session_id
            or public_cursor.session_id != self.session_id
            or market_cursor.process_boot_id != self.process_boot_id
            or public_cursor.process_boot_id != self.process_boot_id
            or market_cursor.plan_bundle_sha256 != self.plan_bundle_sha256
            or public_cursor.plan_bundle_sha256 != self.plan_bundle_sha256
            or bridge.session_id != self.session_id
            or bridge.plan_bundle_sha256 != self.plan_bundle_sha256
            or bridge.depth_plan_sha256 != self.depth_plan_sha256
            or bridge.last_connection_id != public_cursor.connection_id
            or bridge.last_connection_generation != public_cursor.generation
        ):
            raise ValueError("V8 session bridge/public WebSocket generation lineage differs")

    def _validate_oi_coverage_close(self) -> None:
        if type(self.oi_coverage_close) is not PublicOiRestCoverageCloseV2:
            raise TypeError("oi_coverage_close must be an exact PublicOiRestCoverageCloseV2")
        self.oi_coverage_close.__post_init__()
        coverage = self.oi_coverage_close
        _require_sha256(self.oi_coverage_close_sha256, "oi_coverage_close_sha256")
        _require_sha256(
            self.oi_coverage_close_record_sha256,
            "oi_coverage_close_record_sha256",
        )
        _require_positive_int(
            self.oi_coverage_close_accepted_ingest_seq,
            "oi_coverage_close_accepted_ingest_seq",
        )
        _require_nonnegative_int(
            self.oi_coverage_close_receipt_wall_ms,
            "oi_coverage_close_receipt_wall_ms",
        )
        _require_nonnegative_int(
            self.oi_coverage_close_receipt_monotonic_ns,
            "oi_coverage_close_receipt_monotonic_ns",
        )
        oi_entry = self.planned_source_census.entries[2]
        if (
            self.oi_coverage_close_sha256 != coverage.sha256
            or coverage.session_id != self.session_id
            or coverage.session_start_manifest_sha256 != self.session_start_manifest_sha256
            or coverage.plan_bundle_sha256 != self.plan_bundle_sha256
            or coverage.plan_id != oi_entry.plan_name
            or coverage.route_id != oi_entry.route_id
            or coverage.rest_plan_sha256 != oi_entry.member_census_sha256
            or self.oi_coverage_close_accepted_ingest_seq > self.finality_tail_ingest_seq
            or self.oi_coverage_close_receipt_wall_ms
            < coverage.stop_requested_wall_ms
            or self.oi_coverage_close_receipt_monotonic_ns
            < coverage.stop_requested_monotonic_ns
            or self.oi_coverage_close_receipt_monotonic_ns
            > self.finality_receipt.fence_monotonic_ns
        ):
            raise ValueError("V8 OI coverage-close projection differs from session authority")

    @property
    def encoded_line(self) -> bytes:
        return canonical_json_line(self)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.encoded_line).hexdigest()


@dataclass(frozen=True, slots=True, init=False)
class PersistedSessionClosureAuthorityV2:
    """Factory-only receipt for one exact persisted CLEAN session closure."""

    manifest: SessionClosureManifestV2
    canonical_path: str
    manifest_sha256: str
    byte_count: int
    file_device: int
    file_inode: int
    file_nlink: int
    writer_lease: SessionWriterLeaseBindingV2
    schema_version: str
    _factory_token: object = field(repr=False, compare=False)

    def __init__(
        self,
        *,
        manifest: SessionClosureManifestV2,
        canonical_path: str,
        manifest_sha256: str,
        byte_count: int,
        file_device: int,
        file_inode: int,
        file_nlink: int,
        writer_lease: SessionWriterLeaseBindingV2,
        session_start_authority: PersistedSessionStartAuthorityV2,
        ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _PERSISTED_CLOSURE_AUTHORITY_FACTORY_TOKEN:
            raise TypeError(
                "PersistedSessionClosureAuthorityV2 can only be created by the durable writer"
            )
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "canonical_path", canonical_path)
        object.__setattr__(self, "manifest_sha256", manifest_sha256)
        object.__setattr__(self, "byte_count", byte_count)
        object.__setattr__(self, "file_device", file_device)
        object.__setattr__(self, "file_inode", file_inode)
        object.__setattr__(self, "file_nlink", file_nlink)
        object.__setattr__(self, "writer_lease", writer_lease)
        object.__setattr__(
            self,
            "schema_version",
            "r4b_v2_persisted_session_closure_authority_v1",
        )
        object.__setattr__(self, "_factory_token", _factory_token)
        self.__post_init__()
        _assert_manifest_binds_start_receipt(manifest, session_start_authority)
        _assert_manifest_binds_ledger_receipt(manifest, ledger_seal_receipt)

    def __post_init__(self) -> None:
        if self._factory_token is not _PERSISTED_CLOSURE_AUTHORITY_FACTORY_TOKEN:
            raise ValueError("persisted session closure lacks factory provenance")
        if self.schema_version != "r4b_v2_persisted_session_closure_authority_v1":
            raise ValueError("unsupported persisted session-closure authority schema")
        if type(self.manifest) is not SessionClosureManifestV2:
            raise TypeError("manifest must be an exact SessionClosureManifestV2")
        self.manifest.__post_init__()
        _require_canonical_absolute_path(self.canonical_path, "canonical_path")
        _require_sha256(self.manifest_sha256, "manifest_sha256")
        if self.manifest_sha256 != self.manifest.sha256:
            raise ValueError("persisted session-closure hash differs from its manifest")
        _require_positive_int(self.byte_count, "byte_count")
        if self.byte_count != len(self.manifest.encoded_line):
            raise ValueError("persisted session-closure byte count differs")
        _require_file_identity(
            self.file_device,
            self.file_inode,
            self.file_nlink,
            "persisted session closure",
        )
        if type(self.writer_lease) is not SessionWriterLeaseBindingV2:
            raise TypeError("writer_lease must be an exact SessionWriterLeaseBindingV2")
        if self.writer_lease != self.manifest.writer_lease:
            raise ValueError("persisted session-closure lease differs from its manifest")
        if not _is_strict_descendant(
            self.canonical_path,
            self.writer_lease.scope_canonical_path,
        ):
            raise ValueError("persisted session closure is outside its lease scope")
        if self.canonical_path == self.manifest.session_start_canonical_path:
            raise ValueError("session start and closure must use different fixed paths")
        if any(
            _paths_equal_or_nested(self.canonical_path, root.canonical_path)
            for root in self.manifest.session_start_manifest.storage_roots
        ):
            raise ValueError("persisted session closure must be non-nested with storage roots")


@dataclass(frozen=True, slots=True, init=False)
class PersistedSessionClosureAuthorityV8:
    """Factory-only receipt for the shared-path V8 CLEAN session closure."""

    manifest: SessionClosureManifestV8
    canonical_path: str
    manifest_sha256: str
    byte_count: int
    file_device: int
    file_inode: int
    file_nlink: int
    writer_lease: SessionWriterLeaseBindingV2
    schema_version: str
    _factory_token: object = field(repr=False, compare=False)

    def __init__(
        self,
        *,
        manifest: SessionClosureManifestV8,
        canonical_path: str,
        manifest_sha256: str,
        byte_count: int,
        file_device: int,
        file_inode: int,
        file_nlink: int,
        writer_lease: SessionWriterLeaseBindingV2,
        session_start_authority: PersistedSessionStartAuthorityV2,
        ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV8,
        depth_bridge_closure_entry: DepthBridgeCoordinatorClosureEntryV8,
        oi_coverage_close_receipt: PublicOiCensusAdmissionReceiptV2,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _PERSISTED_CLOSURE_AUTHORITY_FACTORY_TOKEN_V8:
            raise TypeError(
                "PersistedSessionClosureAuthorityV8 can only be created by the durable writer"
            )
        object.__setattr__(self, "manifest", manifest)
        object.__setattr__(self, "canonical_path", canonical_path)
        object.__setattr__(self, "manifest_sha256", manifest_sha256)
        object.__setattr__(self, "byte_count", byte_count)
        object.__setattr__(self, "file_device", file_device)
        object.__setattr__(self, "file_inode", file_inode)
        object.__setattr__(self, "file_nlink", file_nlink)
        object.__setattr__(self, "writer_lease", writer_lease)
        object.__setattr__(
            self,
            "schema_version",
            "r4b_v2_persisted_session_closure_authority_v8",
        )
        object.__setattr__(self, "_factory_token", _factory_token)
        self.__post_init__()
        _assert_manifest_v8_binds_start_receipt(manifest, session_start_authority)
        _assert_manifest_v8_binds_ledger_receipt(manifest, ledger_seal_receipt)
        _assert_manifest_v8_binds_bridge_entry(manifest, depth_bridge_closure_entry)
        _assert_manifest_v8_binds_oi_receipt(manifest, oi_coverage_close_receipt)

    def __post_init__(self) -> None:
        if self._factory_token is not _PERSISTED_CLOSURE_AUTHORITY_FACTORY_TOKEN_V8:
            raise ValueError("persisted V8 session closure lacks factory provenance")
        if self.schema_version != "r4b_v2_persisted_session_closure_authority_v8":
            raise ValueError("unsupported persisted V8 session-closure authority schema")
        if type(self.manifest) is not SessionClosureManifestV8:
            raise TypeError("manifest must be an exact SessionClosureManifestV8")
        self.manifest.__post_init__()
        _require_canonical_absolute_path(self.canonical_path, "canonical_path")
        _require_sha256(self.manifest_sha256, "manifest_sha256")
        if self.manifest_sha256 != self.manifest.sha256:
            raise ValueError("persisted V8 session-closure hash differs from its manifest")
        _require_positive_int(self.byte_count, "byte_count")
        if self.byte_count != len(self.manifest.encoded_line):
            raise ValueError("persisted V8 session-closure byte count differs")
        _require_file_identity(
            self.file_device,
            self.file_inode,
            self.file_nlink,
            "persisted V8 session closure",
        )
        if type(self.writer_lease) is not SessionWriterLeaseBindingV2:
            raise TypeError("writer_lease must be an exact SessionWriterLeaseBindingV2")
        if self.writer_lease != self.manifest.writer_lease:
            raise ValueError("persisted V8 session-closure lease differs from its manifest")
        if not _is_strict_descendant(
            self.canonical_path,
            self.writer_lease.scope_canonical_path,
        ):
            raise ValueError("persisted V8 session closure is outside its lease scope")
        if self.canonical_path == self.manifest.session_start_canonical_path:
            raise ValueError("V8 session start and closure must use different fixed paths")
        if any(
            _paths_equal_or_nested(self.canonical_path, root.canonical_path)
            for root in self.manifest.session_start_manifest.storage_roots
        ):
            raise ValueError("persisted V8 session closure must be non-nested with storage roots")


def canonical_session_start_manifest_path_v2(lease: WriterLease) -> Path:
    """Return the sole start-authority pathname for one lease acquisition."""

    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    lease.assert_held()
    scope_path = _inspect_existing_directory(lease.scope_root, "writer lease scope")
    return scope_path / (
        f"{_SESSION_START_FILE_PREFIX}{lease.owner_id}{_SESSION_START_FILE_SUFFIX}"
    )


def canonical_session_closure_manifest_path_v2(lease: WriterLease) -> Path:
    """Return the sole local CLEAN-closure pathname for one lease acquisition."""

    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    lease.assert_held()
    scope_path = _inspect_existing_directory(lease.scope_root, "writer lease scope")
    return scope_path / (
        f"{_SESSION_START_FILE_PREFIX}{lease.owner_id}{_SESSION_CLOSURE_FILE_SUFFIX}"
    )


def canonical_session_closure_manifest_path_v8(lease: WriterLease) -> Path:
    """Return V8's intentionally shared, sole session-closure pathname."""

    return canonical_session_closure_manifest_path_v2(lease)


def write_session_closure_manifest_v8(
    manifest_path: str | Path,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV8,
    ledger: CaptureIntegrityLedgerV2,
    depth_bridge_close_receipt: DepthBridgeCoordinatorCleanCloseReceiptV8,
    depth_bridge_closure_entry: DepthBridgeCoordinatorClosureEntryV8,
    finalized_websocket_route_cursors: FinalizedWebSocketRouteCursorPairV8,
    oi_coverage_close_receipt: PublicOiCensusAdmissionReceiptV2,
    stop_reason: str,
    closed_wall_ms: int,
    closed_monotonic_ns: int,
) -> PersistedSessionClosureAuthorityV8:
    """Persist one exact V8 infrastructure closure under the shared V2/V8 claim."""

    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    with lease.operation_guard():
        claim_path = _canonical_path_text(canonical_session_closure_manifest_path_v8(lease))
        try:
            lease.claim_session_closure_authority(canonical_path=claim_path)
        except WriterLeaseSessionClosureClaimError as exc:
            raise SessionAuthorityExistsError(
                "session-closure authority already exists or this writer lease acquisition "
                "already consumed its issuance"
            ) from exc
        return _write_session_closure_manifest_guarded_v8(
            manifest_path,
            lease=lease,
            session_start_authority=session_start_authority,
            promoting_plans=promoting_plans,
            depth_plan=depth_plan,
            finality_receipt=finality_receipt,
            pipeline=pipeline,
            ledger_seal_receipt=ledger_seal_receipt,
            ledger=ledger,
            depth_bridge_close_receipt=depth_bridge_close_receipt,
            depth_bridge_closure_entry=depth_bridge_closure_entry,
            finalized_websocket_route_cursors=finalized_websocket_route_cursors,
            oi_coverage_close_receipt=oi_coverage_close_receipt,
            stop_reason=stop_reason,
            closed_wall_ms=closed_wall_ms,
            closed_monotonic_ns=closed_monotonic_ns,
        )


def _write_session_closure_manifest_guarded_v8(
    manifest_path: str | Path,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV8,
    ledger: CaptureIntegrityLedgerV2,
    depth_bridge_close_receipt: DepthBridgeCoordinatorCleanCloseReceiptV8,
    depth_bridge_closure_entry: DepthBridgeCoordinatorClosureEntryV8,
    finalized_websocket_route_cursors: FinalizedWebSocketRouteCursorPairV8,
    oi_coverage_close_receipt: PublicOiCensusAdmissionReceiptV2,
    stop_reason: str,
    closed_wall_ms: int,
    closed_monotonic_ns: int,
) -> PersistedSessionClosureAuthorityV8:
    lease.assert_held()
    _require_v8_session_closure_input_types(
        session_start_authority=session_start_authority,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
        finality_receipt=finality_receipt,
        pipeline=pipeline,
        ledger_seal_receipt=ledger_seal_receipt,
        ledger=ledger,
        depth_bridge_close_receipt=depth_bridge_close_receipt,
        depth_bridge_closure_entry=depth_bridge_closure_entry,
        finalized_websocket_route_cursors=finalized_websocket_route_cursors,
        oi_coverage_close_receipt=oi_coverage_close_receipt,
    )
    _validate_exact_v8_session_closure_plans(promoting_plans, depth_plan=depth_plan)

    expected_path = canonical_session_closure_manifest_path_v8(lease)
    if _canonical_path_text(manifest_path) != _canonical_path_text(expected_path):
        raise SessionAuthorityIntegrityError(
            "V8 session-closure manifest path differs from the lease-acquisition canonical path"
        )
    scope_path = _inspect_existing_directory(lease.scope_root, "writer lease scope")
    output_path = _inspect_new_closure_manifest_path(manifest_path, scope_path=scope_path)
    assert_persisted_session_start_authority_current_v2(
        session_start_authority,
        lease=lease,
    )
    _assert_start_receipt_uses_this_lease(session_start_authority, lease)
    _assert_closure_path_separation_v8(
        output_path,
        session_start_authority=session_start_authority,
        ledger_seal_receipt=ledger_seal_receipt,
    )

    start = session_start_authority.manifest
    planned_source_census = _planned_source_census_v8(
        promoting_plans,
        depth_plan=depth_plan,
    )
    if planned_source_census.plan_bundle_sha256 != start.wal_authority.plan_sha256:
        raise SessionAuthorityIntegrityError(
            "V8 closure plan bundle differs from the persisted session start"
        )
    prefix_proof_sha256 = verify_clean_stopped_current_tail_v2(
        finality_receipt,
        pipeline=pipeline,
    )
    ledger_seal_sha256 = verify_persisted_capture_clean_closure_seal_receipt_v8(
        ledger_seal_receipt,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
        ledger=ledger,
    )
    if ledger_seal_receipt.seal.finality_receipt != finality_receipt:
        raise SessionAuthorityIntegrityError(
            "V8 ledger CLEAN seal differs from the clean-stopped finality receipt"
        )
    if prefix_proof_sha256 != finality_receipt.prefix_proof_sha256:
        raise SessionAuthorityIntegrityError(
            "V8 clean-stopped finality verifier returned a different stable prefix"
        )
    if ledger_seal_sha256 != ledger_seal_receipt.seal_sha256:
        raise SessionAuthorityIntegrityError(
            "V8 ledger CLEAN verifier returned a different seal hash"
        )

    validate_depth_bridge_coordinator_clean_close_receipt_v8(
        depth_bridge_close_receipt,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    projected_bridge = depth_bridge_coordinator_closure_entry_v8(
        depth_bridge_close_receipt,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    validate_depth_bridge_coordinator_closure_entry_v8(
        depth_bridge_closure_entry,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )
    if (
        projected_bridge != depth_bridge_closure_entry
        or depth_bridge_closure_entry
        is not ledger_seal_receipt.seal.depth_bridge_closure_entry
    ):
        raise SessionAuthorityIntegrityError(
            "V8 bridge close receipt/entry differs from the exact ledger projection"
        )
    bridge_entry_sha256 = depth_bridge_coordinator_closure_entry_sha256_v8(
        depth_bridge_closure_entry,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
    )

    websocket_route_cursors = websocket_route_cursor_closure_pair_v8(
        finalized_websocket_route_cursors,
        finality_receipt=finality_receipt,
        promoting_plans=promoting_plans,
    )
    websocket_route_cursors_sha256 = websocket_route_cursor_closure_pair_sha256_v8(
        websocket_route_cursors,
        finality_receipt=finality_receipt,
        promoting_plans=promoting_plans,
    )
    if (
        websocket_route_cursors
        != ledger_seal_receipt.seal.websocket_route_cursor_closure_pair
        or websocket_route_cursors_sha256
        != ledger_seal_receipt.seal.websocket_route_cursor_closure_pair_sha256
    ):
        raise SessionAuthorityIntegrityError(
            "V8 finalized WebSocket pair differs from the ledger CLEAN projection"
        )

    coverage, coverage_record_sha256, coverage_record = _oi_coverage_close_projection_v8(
        oi_coverage_close_receipt,
        promoting_plans=promoting_plans,
        session_start_authority=session_start_authority,
        finality_receipt=finality_receipt,
    )
    manifest = SessionClosureManifestV8(
        purpose=_CLOSURE_PURPOSE,
        closure_status="CLEAN",
        fatal=False,
        production_order_execution_enabled=False,
        private_credentials_permitted=False,
        stop_reason=stop_reason,
        session_id=start.session_id,
        process_boot_id=start.process_boot_id,
        attempt_id=start.attempt_id,
        writer_lease=start.writer_lease,
        session_start_manifest=start,
        session_start_manifest_sha256=session_start_authority.manifest_sha256,
        session_start_canonical_path=session_start_authority.canonical_path,
        session_start_byte_count=session_start_authority.byte_count,
        session_start_file_device=str(session_start_authority.file_device),
        session_start_file_inode=str(session_start_authority.file_inode),
        session_start_file_nlink=str(session_start_authority.file_nlink),
        plan_bundle_sha256=planned_source_census.plan_bundle_sha256,
        depth_plan_sha256=planned_source_census.depth_plan_sha256,
        planned_source_census=planned_source_census,
        planned_source_census_sha256=planned_source_census.sha256,
        finality_receipt=finality_receipt,
        finality_receipt_sha256=finality_receipt.sha256,
        finality_prefix_proof_sha256=prefix_proof_sha256,
        finality_tail_ingest_seq=finality_receipt.fence_ingest_seq,
        wal_durable_ack_seq=finality_receipt.wal_durable_ack_seq,
        finalized_block_tail_ingest_seq=finality_receipt.finalized_block_tail_ingest_seq,
        exact_prefix_sha256=finality_receipt.exact_prefix_sha256,
        final_block_sequence=finality_receipt.final_block_sequence,
        final_block_hash=finality_receipt.final_block_hash,
        final_block_manifest_sha256=finality_receipt.final_block_manifest_sha256,
        final_block_container_sha256=finality_receipt.final_block_container_sha256,
        ledger_clean_closure_seal=ledger_seal_receipt.seal,
        ledger_clean_closure_seal_sha256=ledger_seal_receipt.seal_sha256,
        ledger_clean_closure_receipt_sha256=ledger_seal_receipt.sha256,
        ledger_clean_closure_canonical_path=ledger_seal_receipt.canonical_path,
        ledger_clean_closure_file_name=ledger_seal_receipt.file_name,
        ledger_clean_closure_byte_count=ledger_seal_receipt.byte_count,
        ledger_clean_closure_file_device=str(ledger_seal_receipt.file_device),
        ledger_clean_closure_file_inode=str(ledger_seal_receipt.file_inode),
        ledger_clean_closure_file_nlink=str(ledger_seal_receipt.file_nlink),
        websocket_route_cursors=websocket_route_cursors,
        websocket_route_cursors_sha256=websocket_route_cursors_sha256,
        websocket_route_cursor_finality_persisted=True,
        depth_bridge_closure_entry=depth_bridge_closure_entry,
        depth_bridge_closure_entry_sha256=bridge_entry_sha256,
        oi_coverage_close=coverage,
        oi_coverage_close_sha256=coverage.sha256,
        oi_coverage_close_record_sha256=coverage_record_sha256,
        oi_coverage_close_accepted_ingest_seq=(
            oi_coverage_close_receipt.accepted_ingest_seq
        ),
        oi_coverage_close_receipt_wall_ms=coverage_record.receipt_wall_ms,
        oi_coverage_close_receipt_monotonic_ns=(coverage_record.receipt_monotonic_ns),
        depth_bridge_lifecycle_cleanly_closed=True,
        retained_frame_parser_health_claimed=False,
        observed_source_completeness_claimed=False,
        book_completeness_claimed=False,
        m2_certified=False,
        paper_execution_enabled=False,
        promotion_ready=False,
        closed_wall_ms=closed_wall_ms,
        closed_monotonic_ns=closed_monotonic_ns,
    )
    encoded = manifest.encoded_line

    assert_persisted_session_start_authority_current_v2(
        session_start_authority,
        lease=lease,
    )
    verify_clean_stopped_current_tail_v2(finality_receipt, pipeline=pipeline)
    verify_persisted_capture_clean_closure_seal_receipt_v8(
        ledger_seal_receipt,
        promoting_plans=promoting_plans,
        depth_plan=depth_plan,
        ledger=ledger,
    )
    _oi_coverage_close_projection_v8(
        oi_coverage_close_receipt,
        promoting_plans=promoting_plans,
        session_start_authority=session_start_authority,
        finality_receipt=finality_receipt,
    )
    _write_closure_once(output_path, encoded)
    path, status = _inspect_persisted_closure_manifest_file(
        output_path,
        manifest,
        lease=lease,
    )
    canonical_path = _canonical_path_text(path)
    try:
        lease.seal_session_closure_authority(
            canonical_path=canonical_path,
            manifest_sha256=manifest.sha256,
            byte_count=len(encoded),
            file_device=int(status.st_dev),
            file_inode=int(status.st_ino),
            file_nlink=int(status.st_nlink),
        )
    except WriterLeaseSessionClosureClaimError as exc:
        raise SessionAuthorityIntegrityError(
            "persisted V8 session closure could not seal its writer-lease claim"
        ) from exc
    return PersistedSessionClosureAuthorityV8(
        manifest=manifest,
        canonical_path=canonical_path,
        manifest_sha256=manifest.sha256,
        byte_count=len(encoded),
        file_device=int(status.st_dev),
        file_inode=int(status.st_ino),
        file_nlink=int(status.st_nlink),
        writer_lease=manifest.writer_lease,
        session_start_authority=session_start_authority,
        ledger_seal_receipt=ledger_seal_receipt,
        depth_bridge_closure_entry=depth_bridge_closure_entry,
        oi_coverage_close_receipt=oi_coverage_close_receipt,
        _factory_token=_PERSISTED_CLOSURE_AUTHORITY_FACTORY_TOKEN_V8,
    )


def write_session_closure_manifest_v2(
    manifest_path: str | Path,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    finality_receipt: CaptureFinalityFenceReceiptV2,
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    ledger: CaptureIntegrityLedgerV2,
    stop_reason: str,
    closed_wall_ms: int,
    closed_monotonic_ns: int,
    finalized_websocket_route_cursors: (
        FinalizedWebSocketRouteCursorPairV2 | None
    ) = None,
) -> PersistedSessionClosureAuthorityV2:
    """Create the sole CLEAN closure after reproving all current local tails."""

    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    with lease.operation_guard():
        claim_path = _canonical_path_text(canonical_session_closure_manifest_path_v2(lease))
        try:
            lease.claim_session_closure_authority(canonical_path=claim_path)
        except WriterLeaseSessionClosureClaimError as exc:
            raise SessionAuthorityExistsError(
                "session-closure authority already exists or this writer lease acquisition "
                "already consumed its issuance"
            ) from exc
        return _write_session_closure_manifest_guarded_v2(
            manifest_path,
            lease=lease,
            session_start_authority=session_start_authority,
            promoting_plans=promoting_plans,
            finality_receipt=finality_receipt,
            pipeline=pipeline,
            ledger_seal_receipt=ledger_seal_receipt,
            ledger=ledger,
            stop_reason=stop_reason,
            closed_wall_ms=closed_wall_ms,
            closed_monotonic_ns=closed_monotonic_ns,
            finalized_websocket_route_cursors=finalized_websocket_route_cursors,
        )


def _write_session_closure_manifest_guarded_v2(
    manifest_path: str | Path,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    finality_receipt: CaptureFinalityFenceReceiptV2,
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    ledger: CaptureIntegrityLedgerV2,
    stop_reason: str,
    closed_wall_ms: int,
    closed_monotonic_ns: int,
    finalized_websocket_route_cursors: (
        FinalizedWebSocketRouteCursorPairV2 | None
    ),
) -> PersistedSessionClosureAuthorityV2:
    lease.assert_held()
    if type(session_start_authority) is not PersistedSessionStartAuthorityV2:
        raise TypeError("session_start_authority must be an exact PersistedSessionStartAuthorityV2")
    if type(promoting_plans) is not tuple:
        raise TypeError("promoting_plans must be an exact tuple")
    if type(finality_receipt) is not CaptureFinalityFenceReceiptV2:
        raise TypeError("finality_receipt must be an exact CaptureFinalityFenceReceiptV2")
    if type(pipeline) is not CaptureBatchPipelineV2:
        raise TypeError("pipeline must be an exact CaptureBatchPipelineV2")
    if type(ledger_seal_receipt) is not PersistedCaptureCleanClosureSealReceiptV2:
        raise TypeError(
            "ledger_seal_receipt must be an exact PersistedCaptureCleanClosureSealReceiptV2"
        )
    if type(ledger) is not CaptureIntegrityLedgerV2:
        raise TypeError("ledger must be an exact CaptureIntegrityLedgerV2")

    expected_path = canonical_session_closure_manifest_path_v2(lease)
    if _canonical_path_text(manifest_path) != _canonical_path_text(expected_path):
        raise SessionAuthorityIntegrityError(
            "session-closure manifest path differs from the lease-acquisition canonical path"
        )
    scope_path = _inspect_existing_directory(lease.scope_root, "writer lease scope")
    output_path = _inspect_new_closure_manifest_path(
        manifest_path,
        scope_path=scope_path,
    )
    assert_persisted_session_start_authority_current_v2(
        session_start_authority,
        lease=lease,
    )
    _assert_start_receipt_uses_this_lease(session_start_authority, lease)
    _assert_closure_path_separation(
        output_path,
        session_start_authority=session_start_authority,
        ledger_seal_receipt=ledger_seal_receipt,
    )
    planned_source_census = _planned_source_census_v2(promoting_plans)
    start = session_start_authority.manifest
    if planned_source_census.plan_bundle_sha256 != start.wal_authority.plan_sha256:
        raise SessionAuthorityIntegrityError(
            "closure promoting plan bundle differs from the persisted session start"
        )
    prefix_proof_sha256 = verify_clean_stopped_current_tail_v2(
        finality_receipt,
        pipeline=pipeline,
    )
    ledger_seal_sha256 = verify_persisted_capture_clean_closure_seal_receipt_v2(
        ledger_seal_receipt,
        promoting_plans=promoting_plans,
        ledger=ledger,
    )
    if ledger_seal_receipt.seal.finality_receipt != finality_receipt:
        raise SessionAuthorityIntegrityError(
            "ledger CLEAN seal differs from the clean-stopped finality receipt"
        )
    if prefix_proof_sha256 != finality_receipt.prefix_proof_sha256:
        raise SessionAuthorityIntegrityError(
            "clean-stopped finality verifier returned a different stable prefix"
        )
    if ledger_seal_sha256 != ledger_seal_receipt.seal_sha256:
        raise SessionAuthorityIntegrityError("ledger CLEAN verifier returned a different seal hash")

    if finalized_websocket_route_cursors is None:
        websocket_route_cursors: tuple[WebSocketRouteCursorClosureEntryV2, ...] = ()
        websocket_route_cursors_sha256: str | None = None
        websocket_route_cursor_finality_persisted = False
    else:
        projected_pair = websocket_route_cursor_closure_pair_v2(
            finalized_websocket_route_cursors,
            finality_receipt=finality_receipt,
            promoting_plans=promoting_plans,
        )
        websocket_route_cursors = projected_pair
        websocket_route_cursors_sha256 = (
            websocket_route_cursor_closure_pair_sha256_v2(projected_pair)
        )
        websocket_route_cursor_finality_persisted = True

    manifest = SessionClosureManifestV2(
        purpose=_CLOSURE_PURPOSE,
        closure_status="CLEAN",
        fatal=False,
        production_order_execution_enabled=False,
        private_credentials_permitted=False,
        stop_reason=stop_reason,
        session_id=start.session_id,
        process_boot_id=start.process_boot_id,
        attempt_id=start.attempt_id,
        writer_lease=start.writer_lease,
        session_start_manifest=start,
        session_start_manifest_sha256=session_start_authority.manifest_sha256,
        session_start_canonical_path=session_start_authority.canonical_path,
        session_start_byte_count=session_start_authority.byte_count,
        session_start_file_device=str(session_start_authority.file_device),
        session_start_file_inode=str(session_start_authority.file_inode),
        session_start_file_nlink=str(session_start_authority.file_nlink),
        plan_bundle_sha256=start.wal_authority.plan_sha256,
        planned_source_census=planned_source_census,
        planned_source_census_sha256=planned_source_census.sha256,
        finality_receipt=finality_receipt,
        finality_receipt_sha256=finality_receipt.sha256,
        finality_prefix_proof_sha256=prefix_proof_sha256,
        finality_tail_ingest_seq=finality_receipt.fence_ingest_seq,
        wal_durable_ack_seq=finality_receipt.wal_durable_ack_seq,
        finalized_block_tail_ingest_seq=(finality_receipt.finalized_block_tail_ingest_seq),
        exact_prefix_sha256=finality_receipt.exact_prefix_sha256,
        final_block_sequence=finality_receipt.final_block_sequence,
        final_block_hash=finality_receipt.final_block_hash,
        final_block_manifest_sha256=finality_receipt.final_block_manifest_sha256,
        final_block_container_sha256=finality_receipt.final_block_container_sha256,
        ledger_clean_closure_seal=ledger_seal_receipt.seal,
        ledger_clean_closure_seal_sha256=ledger_seal_receipt.seal_sha256,
        ledger_clean_closure_receipt_sha256=ledger_seal_receipt.sha256,
        ledger_clean_closure_canonical_path=ledger_seal_receipt.canonical_path,
        ledger_clean_closure_file_name=ledger_seal_receipt.file_name,
        ledger_clean_closure_byte_count=ledger_seal_receipt.byte_count,
        ledger_clean_closure_file_device=str(ledger_seal_receipt.file_device),
        ledger_clean_closure_file_inode=str(ledger_seal_receipt.file_inode),
        ledger_clean_closure_file_nlink=str(ledger_seal_receipt.file_nlink),
        websocket_route_cursors=websocket_route_cursors,
        websocket_route_cursors_sha256=websocket_route_cursors_sha256,
        websocket_route_cursor_finality_persisted=(
            websocket_route_cursor_finality_persisted
        ),
        closed_wall_ms=closed_wall_ms,
        closed_monotonic_ns=closed_monotonic_ns,
    )
    encoded = manifest.encoded_line
    assert_persisted_session_start_authority_current_v2(
        session_start_authority,
        lease=lease,
    )
    verify_clean_stopped_current_tail_v2(finality_receipt, pipeline=pipeline)
    verify_persisted_capture_clean_closure_seal_receipt_v2(
        ledger_seal_receipt,
        promoting_plans=promoting_plans,
        ledger=ledger,
    )
    _write_closure_once(output_path, encoded)
    path, status = _inspect_persisted_closure_manifest_file(
        output_path,
        manifest,
        lease=lease,
    )
    canonical_path = _canonical_path_text(path)
    try:
        lease.seal_session_closure_authority(
            canonical_path=canonical_path,
            manifest_sha256=manifest.sha256,
            byte_count=len(encoded),
            file_device=int(status.st_dev),
            file_inode=int(status.st_ino),
            file_nlink=int(status.st_nlink),
        )
    except WriterLeaseSessionClosureClaimError as exc:
        raise SessionAuthorityIntegrityError(
            "persisted session closure could not seal its writer-lease claim"
        ) from exc
    return PersistedSessionClosureAuthorityV2(
        manifest=manifest,
        canonical_path=canonical_path,
        manifest_sha256=manifest.sha256,
        byte_count=len(encoded),
        file_device=int(status.st_dev),
        file_inode=int(status.st_ino),
        file_nlink=int(status.st_nlink),
        writer_lease=manifest.writer_lease,
        session_start_authority=session_start_authority,
        ledger_seal_receipt=ledger_seal_receipt,
        _factory_token=_PERSISTED_CLOSURE_AUTHORITY_FACTORY_TOKEN,
    )


def assert_persisted_session_closure_authority_current_v2(
    authority: PersistedSessionClosureAuthorityV2,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    finality_receipt: CaptureFinalityFenceReceiptV2,
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    ledger: CaptureIntegrityLedgerV2,
) -> None:
    """Reprove one persisted CLEAN closure against every current local owner."""

    if type(authority) is not PersistedSessionClosureAuthorityV2:
        raise TypeError("authority must be an exact PersistedSessionClosureAuthorityV2")
    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    with lease.operation_guard():
        authority.__post_init__()
        manifest = authority.manifest
        path, status = _inspect_persisted_closure_manifest_file(
            authority.canonical_path,
            manifest,
            lease=lease,
        )
        if _canonical_path_text(path) != authority.canonical_path:
            raise SessionAuthorityIntegrityError(
                "persisted session-closure pathname differs from its receipt"
            )
        if (
            int(status.st_dev) != authority.file_device
            or int(status.st_ino) != authority.file_inode
            or int(status.st_nlink) != authority.file_nlink
            or int(status.st_size) != authority.byte_count
        ):
            raise SessionAuthorityIntegrityError(
                "persisted session-closure file identity differs from its receipt"
            )
        try:
            lease.assert_session_closure_authority_claim(
                canonical_path=authority.canonical_path,
                manifest_sha256=authority.manifest_sha256,
                byte_count=authority.byte_count,
                file_device=authority.file_device,
                file_inode=authority.file_inode,
                file_nlink=authority.file_nlink,
            )
        except WriterLeaseSessionClosureClaimError as exc:
            raise SessionAuthorityIntegrityError(
                "persisted session closure differs from its writer-lease claim"
            ) from exc
        _assert_exact_live_lease_binding(authority.writer_lease, lease)
        assert_persisted_session_start_authority_current_v2(
            session_start_authority,
            lease=lease,
        )
        _assert_manifest_binds_start_receipt(manifest, session_start_authority)
        expected_census = _planned_source_census_v2(promoting_plans)
        if (
            manifest.planned_source_census != expected_census
            or manifest.planned_source_census_sha256 != expected_census.sha256
        ):
            raise SessionAuthorityIntegrityError(
                "persisted session closure differs from the exact planned source census"
            )
        if finality_receipt != manifest.finality_receipt:
            raise SessionAuthorityIntegrityError(
                "current finality receipt differs from the persisted session closure"
            )
        if ledger_seal_receipt.seal != manifest.ledger_clean_closure_seal:
            raise SessionAuthorityIntegrityError(
                "current ledger CLEAN seal differs from the persisted session closure"
            )
        if ledger_seal_receipt.sha256 != manifest.ledger_clean_closure_receipt_sha256:
            raise SessionAuthorityIntegrityError(
                "current ledger CLEAN receipt differs from the persisted session closure"
            )
        prefix_proof = verify_clean_stopped_current_tail_v2(
            finality_receipt,
            pipeline=pipeline,
        )
        seal_sha256 = verify_persisted_capture_clean_closure_seal_receipt_v2(
            ledger_seal_receipt,
            promoting_plans=promoting_plans,
            ledger=ledger,
        )
        if prefix_proof != manifest.finality_prefix_proof_sha256:
            raise SessionAuthorityIntegrityError(
                "current clean-stopped prefix differs from the session closure"
            )
        if seal_sha256 != manifest.ledger_clean_closure_seal_sha256:
            raise SessionAuthorityIntegrityError(
                "current ledger seal differs from the session closure"
            )
        _, after_status = _inspect_persisted_closure_manifest_file(
            authority.canonical_path,
            manifest,
            lease=lease,
        )
        if (
            int(after_status.st_dev) != authority.file_device
            or int(after_status.st_ino) != authority.file_inode
            or int(after_status.st_nlink) != authority.file_nlink
        ):
            raise SessionAuthorityIntegrityError(
                "persisted session-closure identity changed during validation"
            )


def assert_persisted_session_closure_authority_current_v8(
    authority: PersistedSessionClosureAuthorityV8,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV8,
    ledger: CaptureIntegrityLedgerV2,
    depth_bridge_closure_entry: DepthBridgeCoordinatorClosureEntryV8,
    finalized_websocket_route_cursors: FinalizedWebSocketRouteCursorPairV8,
    oi_coverage_close_receipt: PublicOiCensusAdmissionReceiptV2,
) -> None:
    """Reprove one persisted V8 closure against exact current local owners."""

    if type(authority) is not PersistedSessionClosureAuthorityV8:
        raise TypeError("authority must be an exact PersistedSessionClosureAuthorityV8")
    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    with lease.operation_guard():
        authority.__post_init__()
        _require_v8_session_closure_input_types(
            session_start_authority=session_start_authority,
            promoting_plans=promoting_plans,
            depth_plan=depth_plan,
            finality_receipt=finality_receipt,
            pipeline=pipeline,
            ledger_seal_receipt=ledger_seal_receipt,
            ledger=ledger,
            depth_bridge_close_receipt=None,
            depth_bridge_closure_entry=depth_bridge_closure_entry,
            finalized_websocket_route_cursors=finalized_websocket_route_cursors,
            oi_coverage_close_receipt=oi_coverage_close_receipt,
        )
        _validate_exact_v8_session_closure_plans(promoting_plans, depth_plan=depth_plan)
        manifest = authority.manifest
        path, status = _inspect_persisted_closure_manifest_file(
            authority.canonical_path,
            manifest,
            lease=lease,
        )
        if _canonical_path_text(path) != authority.canonical_path:
            raise SessionAuthorityIntegrityError(
                "persisted V8 session-closure pathname differs from its receipt"
            )
        if (
            int(status.st_dev) != authority.file_device
            or int(status.st_ino) != authority.file_inode
            or int(status.st_nlink) != authority.file_nlink
            or int(status.st_size) != authority.byte_count
        ):
            raise SessionAuthorityIntegrityError(
                "persisted V8 session-closure file identity differs from its receipt"
            )
        try:
            lease.assert_session_closure_authority_claim(
                canonical_path=authority.canonical_path,
                manifest_sha256=authority.manifest_sha256,
                byte_count=authority.byte_count,
                file_device=authority.file_device,
                file_inode=authority.file_inode,
                file_nlink=authority.file_nlink,
            )
        except WriterLeaseSessionClosureClaimError as exc:
            raise SessionAuthorityIntegrityError(
                "persisted V8 session closure differs from its writer-lease claim"
            ) from exc
        _assert_exact_live_lease_binding(authority.writer_lease, lease)
        assert_persisted_session_start_authority_current_v2(
            session_start_authority,
            lease=lease,
        )
        _assert_manifest_v8_binds_start_receipt(manifest, session_start_authority)
        expected_census = _planned_source_census_v8(
            promoting_plans,
            depth_plan=depth_plan,
        )
        if (
            manifest.planned_source_census != expected_census
            or manifest.planned_source_census_sha256 != expected_census.sha256
        ):
            raise SessionAuthorityIntegrityError(
                "persisted V8 session closure differs from exact planned source census"
            )
        if finality_receipt != manifest.finality_receipt:
            raise SessionAuthorityIntegrityError(
                "current finality receipt differs from persisted V8 session closure"
            )
        _assert_manifest_v8_binds_ledger_receipt(manifest, ledger_seal_receipt)
        _assert_manifest_v8_binds_bridge_entry(manifest, depth_bridge_closure_entry)
        prefix_proof = verify_clean_stopped_current_tail_v2(
            finality_receipt,
            pipeline=pipeline,
        )
        seal_sha256 = verify_persisted_capture_clean_closure_seal_receipt_v8(
            ledger_seal_receipt,
            promoting_plans=promoting_plans,
            depth_plan=depth_plan,
            ledger=ledger,
        )
        if prefix_proof != manifest.finality_prefix_proof_sha256:
            raise SessionAuthorityIntegrityError(
                "current clean-stopped prefix differs from the V8 session closure"
            )
        if seal_sha256 != manifest.ledger_clean_closure_seal_sha256:
            raise SessionAuthorityIntegrityError(
                "current ledger seal differs from the V8 session closure"
            )
        validate_depth_bridge_coordinator_closure_entry_v8(
            depth_bridge_closure_entry,
            promoting_plans=promoting_plans,
            depth_plan=depth_plan,
        )
        websocket_projection = websocket_route_cursor_closure_pair_v8(
            finalized_websocket_route_cursors,
            finality_receipt=finality_receipt,
            promoting_plans=promoting_plans,
        )
        if websocket_projection != manifest.websocket_route_cursors:
            raise SessionAuthorityIntegrityError(
                "current finalized V8 WebSocket pair differs from session closure"
            )
        _oi_coverage_close_projection_v8(
            oi_coverage_close_receipt,
            promoting_plans=promoting_plans,
            session_start_authority=session_start_authority,
            finality_receipt=finality_receipt,
        )
        _assert_manifest_v8_binds_oi_receipt(manifest, oi_coverage_close_receipt)
        _, after_status = _inspect_persisted_closure_manifest_file(
            authority.canonical_path,
            manifest,
            lease=lease,
        )
        if (
            int(after_status.st_dev) != authority.file_device
            or int(after_status.st_ino) != authority.file_inode
            or int(after_status.st_nlink) != authority.file_nlink
        ):
            raise SessionAuthorityIntegrityError(
                "persisted V8 session-closure identity changed during validation"
            )


def _planned_source_census_v2(
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
) -> PlannedSourceCensusV2:
    if type(promoting_plans) is not tuple:
        raise TypeError("promoting_plans must be an exact tuple")
    if len(promoting_plans) != 3:
        raise ValueError("closure plan requires exactly two WebSocket and one OI REST plan")
    if any(
        type(plan)
        not in {
            ProvisionalPromotingCapturePlanV2,
            ProvisionalPromotingRestCapturePlanV2,
        }
        for plan in promoting_plans
    ):
        raise TypeError("closure plan requires exact promoting plan values")
    validate_provisional_promoting_capture_plans_v2(promoting_plans)
    plan_bundle_sha256 = provisional_promoting_plan_sha256_v2(promoting_plans)
    entries: list[PlannedSourceCensusEntryV2] = []
    for plan in sorted(promoting_plans, key=lambda value: value.route_id):
        if type(plan) is ProvisionalPromotingCapturePlanV2:
            entries.append(
                PlannedSourceCensusEntryV2(
                    plan_name=plan.name,
                    route_id=plan.route_id,
                    transport="WEBSOCKET",
                    member_kind="LOGICAL_STREAM",
                    member_count=len(plan.streams),
                    member_census_sha256=(provisional_promoting_stream_census_sha256_v2(plan)),
                    public_unauthenticated=True,
                )
            )
            continue
        assert type(plan) is ProvisionalPromotingRestCapturePlanV2
        rest_census = {
            "schema_version": "r4b_v2_planned_oi_rest_census_v1",
            "plan_name": plan.name,
            "venue": plan.venue.value,
            "route_id": plan.route_id,
            "method": plan.method,
            "endpoint": plan.endpoint,
            "symbols": tuple(sorted(plan.symbols)),
            "request_fields": plan.request_fields,
            "auth_mode": plan.auth_mode,
            "requires_api_key": plan.requires_api_key,
            "is_private": plan.is_private,
        }
        entries.append(
            PlannedSourceCensusEntryV2(
                plan_name=plan.name,
                route_id=plan.route_id,
                transport="PUBLIC_REST",
                member_kind="SYMBOL_REQUEST",
                member_count=len(plan.symbols),
                member_census_sha256=hashlib.sha256(
                    _PLANNED_OI_REST_CENSUS_DOMAIN + canonical_json_line(rest_census)
                ).hexdigest(),
                public_unauthenticated=True,
            )
        )
    if len(entries) != 3:
        raise AssertionError("validated closure plan did not yield exactly three sources")
    return PlannedSourceCensusV2(
        plan_bundle_sha256=plan_bundle_sha256,
        entries=(entries[0], entries[1], entries[2]),
        observed_source_completeness_claimed=False,
        m2_certified=False,
    )


def _planned_source_census_v8(
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    *,
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> PlannedSourceCensusV8:
    _validate_exact_v8_session_closure_plans(promoting_plans, depth_plan=depth_plan)
    market_plan = promoting_plans[0]
    public_plan = promoting_plans[1]
    oi_plan = promoting_plans[2]
    if type(market_plan) is not ProvisionalPromotingCapturePlanV2:
        raise AssertionError("validated V8 market plan has a foreign type")
    if type(public_plan) is not ProvisionalPromotingCapturePlanV2:
        raise AssertionError("validated V8 public plan has a foreign type")
    if type(oi_plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise AssertionError("validated V8 OI plan has a foreign type")
    entries = (
        PlannedSourceCensusEntryV8(
            plan_name=market_plan.name,
            route_id=market_plan.route_id,
            transport="WEBSOCKET",
            member_kind="LOGICAL_STREAM",
            member_count=len(market_plan.streams),
            member_census_sha256=provisional_promoting_stream_census_sha256_v2(
                market_plan
            ),
            authority_role="PROMOTING",
            public_unauthenticated=True,
        ),
        PlannedSourceCensusEntryV8(
            plan_name=public_plan.name,
            route_id=public_plan.route_id,
            transport="WEBSOCKET",
            member_kind="LOGICAL_STREAM",
            member_count=len(public_plan.streams),
            member_census_sha256=provisional_promoting_stream_census_sha256_v2(
                public_plan
            ),
            authority_role="PROMOTING",
            public_unauthenticated=True,
        ),
        PlannedSourceCensusEntryV8(
            plan_name=oi_plan.name,
            route_id=oi_plan.route_id,
            transport="PUBLIC_REST",
            member_kind="SYMBOL_REQUEST",
            member_count=len(oi_plan.symbols),
            member_census_sha256=public_oi_rest_plan_sha256_v2(oi_plan),
            authority_role="PROMOTING",
            public_unauthenticated=True,
        ),
        PlannedSourceCensusEntryV8(
            plan_name=depth_plan.name,
            route_id=depth_plan.route_id,
            transport="PUBLIC_REST",
            member_kind="SYMBOL_REQUEST",
            member_count=len(depth_plan.symbols),
            member_census_sha256=public_depth_rest_plan_sha256_v8(depth_plan),
            authority_role="QUALIFICATION_ONLY",
            public_unauthenticated=True,
        ),
    )
    return PlannedSourceCensusV8(
        plan_bundle_sha256=provisional_promoting_plan_sha256_v8(promoting_plans),
        depth_plan_sha256=public_depth_rest_plan_sha256_v8(depth_plan),
        entries=entries,
        observed_source_completeness_claimed=False,
        book_completeness_claimed=False,
        m2_certified=False,
        promotion_ready=False,
    )


def _validate_exact_v8_session_closure_plans(
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    *,
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
) -> None:
    if type(promoting_plans) is not tuple:
        raise TypeError("V8 session closure requires an exact plan tuple")
    if type(depth_plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise TypeError("V8 session closure requires an exact depth plan")
    validate_provisional_promoting_capture_plans_v8(promoting_plans)
    if tuple(plan.route_id for plan in promoting_plans) != _V8_SOURCE_ROUTES:
        raise ValueError("V8 session closure rejects a permuted plan tuple")
    if sum(plan is depth_plan for plan in promoting_plans) != 1:
        raise ValueError("V8 session closure requires the exact depth plan member identity")
    expected_types = (
        ProvisionalPromotingCapturePlanV2,
        ProvisionalPromotingCapturePlanV2,
        ProvisionalPromotingRestCapturePlanV2,
        ProvisionalDepthRestQualificationPlanV8,
    )
    if any(type(plan) is not expected for plan, expected in zip(
        promoting_plans,
        expected_types,
        strict=True,
    )):
        raise TypeError("V8 session closure plan tuple contains a foreign exact type")


def _require_v8_session_closure_input_types(
    *,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    depth_plan: ProvisionalDepthRestQualificationPlanV8,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV8,
    ledger: CaptureIntegrityLedgerV2,
    depth_bridge_close_receipt: DepthBridgeCoordinatorCleanCloseReceiptV8 | None,
    depth_bridge_closure_entry: DepthBridgeCoordinatorClosureEntryV8,
    finalized_websocket_route_cursors: FinalizedWebSocketRouteCursorPairV8,
    oi_coverage_close_receipt: PublicOiCensusAdmissionReceiptV2,
) -> None:
    if type(session_start_authority) is not PersistedSessionStartAuthorityV2:
        raise TypeError(
            "session_start_authority must be an exact PersistedSessionStartAuthorityV2"
        )
    if type(promoting_plans) is not tuple:
        raise TypeError("promoting_plans must be an exact V8 tuple")
    if type(depth_plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise TypeError("depth_plan must be an exact ProvisionalDepthRestQualificationPlanV8")
    if type(finality_receipt) is not CaptureFinalityFenceReceiptV2:
        raise TypeError("finality_receipt must be an exact CaptureFinalityFenceReceiptV2")
    if type(pipeline) is not CaptureBatchPipelineV2:
        raise TypeError("pipeline must be an exact CaptureBatchPipelineV2")
    if type(ledger_seal_receipt) is not PersistedCaptureCleanClosureSealReceiptV8:
        raise TypeError(
            "ledger_seal_receipt must be an exact PersistedCaptureCleanClosureSealReceiptV8"
        )
    if type(ledger) is not CaptureIntegrityLedgerV2:
        raise TypeError("ledger must be an exact CaptureIntegrityLedgerV2")
    if (
        depth_bridge_close_receipt is not None
        and type(depth_bridge_close_receipt) is not DepthBridgeCoordinatorCleanCloseReceiptV8
    ):
        raise TypeError(
            "depth_bridge_close_receipt must be an exact V8 receipt when supplied"
        )
    if type(depth_bridge_closure_entry) is not DepthBridgeCoordinatorClosureEntryV8:
        raise TypeError("depth_bridge_closure_entry must be an exact V8 entry")
    if type(finalized_websocket_route_cursors) is not tuple or len(
        finalized_websocket_route_cursors
    ) != 2:
        raise TypeError("finalized V8 WebSocket route cursors must be an exact pair")
    if type(oi_coverage_close_receipt) is not PublicOiCensusAdmissionReceiptV2:
        raise TypeError(
            "oi_coverage_close_receipt must be an exact PublicOiCensusAdmissionReceiptV2"
        )


def _oi_coverage_close_projection_v8(
    receipt: PublicOiCensusAdmissionReceiptV2,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    session_start_authority: PersistedSessionStartAuthorityV2,
    finality_receipt: CaptureFinalityFenceReceiptV2,
) -> tuple[PublicOiRestCoverageCloseV2, str, RawRecordV2]:
    if type(receipt) is not PublicOiCensusAdmissionReceiptV2:
        raise TypeError("V8 OI coverage close requires an exact ingress receipt")
    record = validate_public_oi_census_admission_receipt_v2(receipt)
    oi_plan = promoting_plans[2]
    if type(oi_plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise TypeError("V8 OI coverage close requires the exact third OI plan")
    coverage = PublicOiRestCoverageCloseV2.from_canonical_bytes(
        record.payload_bytes(),
        plan=oi_plan,
    )
    start = session_start_authority.manifest
    expected_plan_hash = provisional_promoting_plan_sha256_v8(promoting_plans)
    if (
        coverage.session_id != start.session_id
        or coverage.session_start_manifest_sha256
        != session_start_authority.manifest_sha256
        or coverage.plan_bundle_sha256 != expected_plan_hash
        or record.session_id != start.session_id
        or record.plan_id != oi_plan.name
        or record.protocol_hash != start.wal_authority.protocol_sha256
        or record.ingest_seq != receipt.accepted_ingest_seq
        or receipt.accepted_ingest_seq > finality_receipt.fence_ingest_seq
        or record.receipt_monotonic_ns > finality_receipt.fence_monotonic_ns
        or coverage.stop_requested_monotonic_ns > record.receipt_monotonic_ns
        or coverage.stop_requested_wall_ms > record.receipt_wall_ms
    ):
        raise SessionAuthorityIntegrityError(
            "V8 OI coverage close differs from start, plan, admission, or finality"
        )
    record_sha256 = hashlib.sha256(
        _OI_COVERAGE_CLOSE_RECORD_DOMAIN_V8 + canonical_json_line(record)
    ).hexdigest()
    return coverage, record_sha256, record


def _assert_start_receipt_uses_this_lease(
    authority: PersistedSessionStartAuthorityV2,
    lease: WriterLease,
) -> None:
    expected_path = _canonical_path_text(canonical_session_start_manifest_path_v2(lease))
    if authority.canonical_path != expected_path:
        raise SessionAuthorityIntegrityError(
            "persisted session start is not the canonical path for this lease acquisition"
        )
    _assert_exact_live_lease_binding(authority.writer_lease, lease)


def _assert_manifest_binds_start_receipt(
    manifest: SessionClosureManifestV2,
    receipt: PersistedSessionStartAuthorityV2,
) -> None:
    if type(receipt) is not PersistedSessionStartAuthorityV2:
        raise TypeError("receipt must be an exact PersistedSessionStartAuthorityV2")
    receipt.__post_init__()
    expected = {
        "session_start_manifest": receipt.manifest,
        "session_start_manifest_sha256": receipt.manifest_sha256,
        "session_start_canonical_path": receipt.canonical_path,
        "session_start_byte_count": receipt.byte_count,
        "session_start_file_device": str(receipt.file_device),
        "session_start_file_inode": str(receipt.file_inode),
        "session_start_file_nlink": str(receipt.file_nlink),
        "writer_lease": receipt.writer_lease,
    }
    if any(getattr(manifest, name) != value for name, value in expected.items()):
        raise SessionAuthorityIntegrityError(
            "session closure does not bind the exact persisted session-start receipt"
        )


def _assert_manifest_binds_ledger_receipt(
    manifest: SessionClosureManifestV2,
    receipt: PersistedCaptureCleanClosureSealReceiptV2,
) -> None:
    if type(receipt) is not PersistedCaptureCleanClosureSealReceiptV2:
        raise TypeError("receipt must be an exact PersistedCaptureCleanClosureSealReceiptV2")
    receipt.__post_init__()
    expected = {
        "ledger_clean_closure_seal": receipt.seal,
        "ledger_clean_closure_seal_sha256": receipt.seal_sha256,
        "ledger_clean_closure_receipt_sha256": receipt.sha256,
        "ledger_clean_closure_canonical_path": receipt.canonical_path,
        "ledger_clean_closure_file_name": receipt.file_name,
        "ledger_clean_closure_byte_count": receipt.byte_count,
        "ledger_clean_closure_file_device": str(receipt.file_device),
        "ledger_clean_closure_file_inode": str(receipt.file_inode),
        "ledger_clean_closure_file_nlink": str(receipt.file_nlink),
    }
    if any(getattr(manifest, name) != value for name, value in expected.items()):
        raise SessionAuthorityIntegrityError(
            "session closure does not bind the exact persisted ledger CLEAN receipt"
        )


def _assert_manifest_v8_binds_start_receipt(
    manifest: SessionClosureManifestV8,
    receipt: PersistedSessionStartAuthorityV2,
) -> None:
    if type(manifest) is not SessionClosureManifestV8:
        raise TypeError("manifest must be an exact SessionClosureManifestV8")
    if type(receipt) is not PersistedSessionStartAuthorityV2:
        raise TypeError("receipt must be an exact PersistedSessionStartAuthorityV2")
    receipt.__post_init__()
    expected = {
        "session_start_manifest": receipt.manifest,
        "session_start_manifest_sha256": receipt.manifest_sha256,
        "session_start_canonical_path": receipt.canonical_path,
        "session_start_byte_count": receipt.byte_count,
        "session_start_file_device": str(receipt.file_device),
        "session_start_file_inode": str(receipt.file_inode),
        "session_start_file_nlink": str(receipt.file_nlink),
        "writer_lease": receipt.writer_lease,
    }
    if any(getattr(manifest, name) != value for name, value in expected.items()):
        raise SessionAuthorityIntegrityError(
            "V8 session closure does not bind the exact persisted start receipt"
        )


def _assert_manifest_v8_binds_ledger_receipt(
    manifest: SessionClosureManifestV8,
    receipt: PersistedCaptureCleanClosureSealReceiptV8,
) -> None:
    if type(manifest) is not SessionClosureManifestV8:
        raise TypeError("manifest must be an exact SessionClosureManifestV8")
    if type(receipt) is not PersistedCaptureCleanClosureSealReceiptV8:
        raise TypeError("receipt must be an exact PersistedCaptureCleanClosureSealReceiptV8")
    receipt.__post_init__()
    expected = {
        "ledger_clean_closure_seal": receipt.seal,
        "ledger_clean_closure_seal_sha256": receipt.seal_sha256,
        "ledger_clean_closure_receipt_sha256": receipt.sha256,
        "ledger_clean_closure_canonical_path": receipt.canonical_path,
        "ledger_clean_closure_file_name": receipt.file_name,
        "ledger_clean_closure_byte_count": receipt.byte_count,
        "ledger_clean_closure_file_device": str(receipt.file_device),
        "ledger_clean_closure_file_inode": str(receipt.file_inode),
        "ledger_clean_closure_file_nlink": str(receipt.file_nlink),
    }
    if any(getattr(manifest, name) != value for name, value in expected.items()):
        raise SessionAuthorityIntegrityError(
            "V8 session closure does not bind the exact persisted ledger CLEAN receipt"
        )


def _assert_manifest_v8_binds_bridge_entry(
    manifest: SessionClosureManifestV8,
    entry: DepthBridgeCoordinatorClosureEntryV8,
) -> None:
    if type(entry) is not DepthBridgeCoordinatorClosureEntryV8:
        raise TypeError("entry must be an exact DepthBridgeCoordinatorClosureEntryV8")
    entry.__post_init__()
    if (
        manifest.depth_bridge_closure_entry != entry
        or manifest.depth_bridge_closure_entry_sha256
        != manifest.ledger_clean_closure_seal.depth_bridge_closure_entry_sha256
    ):
        raise SessionAuthorityIntegrityError(
            "V8 session closure does not bind the exact ledger bridge entry"
        )


def _assert_manifest_v8_binds_oi_receipt(
    manifest: SessionClosureManifestV8,
    receipt: PublicOiCensusAdmissionReceiptV2,
) -> None:
    if type(receipt) is not PublicOiCensusAdmissionReceiptV2:
        raise TypeError("receipt must be an exact PublicOiCensusAdmissionReceiptV2")
    record = validate_public_oi_census_admission_receipt_v2(receipt)
    coverage = PublicOiRestCoverageCloseV2.from_canonical_bytes(record.payload_bytes())
    record_sha256 = hashlib.sha256(
        _OI_COVERAGE_CLOSE_RECORD_DOMAIN_V8 + canonical_json_line(record)
    ).hexdigest()
    if (
        manifest.oi_coverage_close != coverage
        or manifest.oi_coverage_close_sha256 != coverage.sha256
        or manifest.oi_coverage_close_record_sha256 != record_sha256
        or manifest.oi_coverage_close_accepted_ingest_seq != receipt.accepted_ingest_seq
        or manifest.oi_coverage_close_receipt_wall_ms != record.receipt_wall_ms
        or manifest.oi_coverage_close_receipt_monotonic_ns
        != record.receipt_monotonic_ns
    ):
        raise SessionAuthorityIntegrityError(
            "V8 session closure does not bind the exact OI coverage-close receipt"
        )


def _assert_closure_path_separation(
    output_path: Path,
    *,
    session_start_authority: PersistedSessionStartAuthorityV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
) -> None:
    output_text = _canonical_path_text(output_path)
    if _paths_equal_or_nested(output_text, session_start_authority.canonical_path):
        raise SessionAuthorityIntegrityError(
            "session closure path must differ from the persisted start path"
        )
    if _paths_equal_or_nested(output_text, ledger_seal_receipt.canonical_path):
        raise SessionAuthorityIntegrityError(
            "session closure path must be non-nested with the ledger CLEAN seal"
        )
    if any(
        _paths_equal_or_nested(output_text, root.canonical_path)
        for root in session_start_authority.manifest.storage_roots
    ):
        raise SessionAuthorityIntegrityError(
            "session closure path must be non-nested with every storage root"
        )


def _assert_closure_path_separation_v8(
    output_path: Path,
    *,
    session_start_authority: PersistedSessionStartAuthorityV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV8,
) -> None:
    output_text = _canonical_path_text(output_path)
    if _paths_equal_or_nested(output_text, session_start_authority.canonical_path):
        raise SessionAuthorityIntegrityError(
            "V8 session closure path must differ from the persisted start path"
        )
    if _paths_equal_or_nested(output_text, ledger_seal_receipt.canonical_path):
        raise SessionAuthorityIntegrityError(
            "V8 session closure path must be non-nested with the ledger CLEAN seal"
        )
    if any(
        _paths_equal_or_nested(output_text, root.canonical_path)
        for root in session_start_authority.manifest.storage_roots
    ):
        raise SessionAuthorityIntegrityError(
            "V8 session closure path must be non-nested with every storage root"
        )


def _inspect_new_closure_manifest_path(
    path: str | Path,
    *,
    scope_path: Path,
) -> Path:
    try:
        inspection = inspect_link_free_path(
            path,
            "session-closure manifest",
            allow_missing_tail=True,
        )
    except ValueError as exc:
        raise SessionAuthorityIntegrityError(str(exc)) from exc
    output_path = inspection.absolute_path
    _assert_no_session_closure_partial(output_path)
    if inspection.final_status is not None:
        raise SessionAuthorityExistsError("session-closure manifest path already exists")
    if inspection.first_missing_component != output_path:
        raise SessionAuthorityIntegrityError(
            "session-closure manifest parent directory must already exist"
        )
    if not _is_strict_descendant(output_path, scope_path):
        raise SessionAuthorityIntegrityError(
            "session-closure manifest must be a strict lease-scope descendant"
        )
    return output_path


def _inspect_persisted_closure_manifest_file(
    manifest_path: str | Path,
    manifest: SessionClosureManifestV2 | SessionClosureManifestV8,
    *,
    lease: WriterLease,
) -> tuple[Path, os.stat_result]:
    if type(manifest) not in {SessionClosureManifestV2, SessionClosureManifestV8}:
        raise TypeError("manifest must be an exact supported session-closure manifest")
    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    lease.assert_held()
    manifest.__post_init__()
    scope_path = _inspect_existing_directory(lease.scope_root, "writer lease scope")
    if _canonical_path_text(scope_path) != manifest.writer_lease.scope_canonical_path:
        raise SessionAuthorityIntegrityError(
            "session-closure file lease scope differs from the manifest"
        )
    try:
        inspection = inspect_link_free_path(
            manifest_path,
            "session-closure manifest file",
        )
    except ValueError as exc:
        raise SessionAuthorityIntegrityError(str(exc)) from exc
    path = inspection.absolute_path
    _assert_no_session_closure_partial(path)
    status = inspection.final_status
    if status is None or not stat.S_ISREG(status.st_mode):
        raise SessionAuthorityIntegrityError(
            "session-closure manifest file must be an existing regular file"
        )
    if not _is_strict_descendant(path, scope_path):
        raise SessionAuthorityIntegrityError(
            "session-closure manifest file must be a strict lease-scope descendant"
        )
    if int(status.st_nlink) != 1:
        raise SessionAuthorityIntegrityError(
            "session-closure manifest file must have exactly one hard link"
        )
    expected = manifest.encoded_line
    if int(status.st_size) != len(expected):
        raise SessionAuthorityIntegrityError(
            "persisted session-closure byte length differs from the manifest"
        )
    identity = (int(status.st_dev), int(status.st_ino))
    try:
        observed = path.read_bytes()
    except OSError as exc:
        raise SessionAuthorityIntegrityError(
            "persisted session-closure manifest could not be read"
        ) from exc
    try:
        after = inspect_link_free_path(path, "session-closure manifest file")
    except ValueError as exc:
        raise SessionAuthorityIntegrityError(str(exc)) from exc
    after_status = after.final_status
    if (
        after_status is None
        or not stat.S_ISREG(after_status.st_mode)
        or int(after_status.st_nlink) != 1
        or (int(after_status.st_dev), int(after_status.st_ino)) != identity
    ):
        raise SessionAuthorityIntegrityError(
            "session-closure manifest identity changed during validation"
        )
    _validate_session_closure_schema_dispatch(
        observed,
        expected_schema=manifest.schema_version,
    )
    if observed != expected:
        raise SessionAuthorityIntegrityError(
            "persisted session-closure bytes differ from the admitted manifest"
        )
    lease.assert_held()
    return path, after_status


def _assert_no_session_closure_partial(path: Path) -> None:
    partial_path = path.with_name(path.name + ".partial")
    try:
        inspection = inspect_link_free_path(
            partial_path,
            "session-closure partial artifact",
            allow_missing_tail=True,
        )
    except ValueError as exc:
        raise SessionAuthorityIntegrityError(str(exc)) from exc
    if inspection.final_status is not None or inspection.first_missing_component is None:
        raise SessionAuthorityIntegrityError(
            "session closure rejects an unfinished or coexisting partial artifact"
        )


def _validate_session_closure_schema_dispatch(
    encoded: bytes,
    *,
    expected_schema: str,
) -> None:
    try:
        document = json.loads(encoded)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SessionAuthorityIntegrityError(
            "persisted session closure is not one canonical JSON document"
        ) from exc
    if type(document) is not dict:
        raise SessionAuthorityIntegrityError(
            "persisted session closure must decode to one object"
        )
    schema_version = document.get("schema_version")
    if schema_version not in {_CLOSURE_SCHEMA_VERSION, _CLOSURE_SCHEMA_VERSION_V8}:
        raise SessionAuthorityIntegrityError(
            "persisted session closure has an unknown schema version"
        )
    if schema_version != expected_schema:
        raise SessionAuthorityIntegrityError(
            "persisted session closure schema differs from the exact authority type"
        )
    if canonical_json_line(document) != encoded:
        raise SessionAuthorityIntegrityError(
            "persisted session closure bytes are not exact canonical JSONL"
        )


def _write_closure_once(path: Path, encoded: bytes) -> None:
    try:
        with path.open("xb", buffering=0) as handle:
            written = handle.write(encoded)
            if written != len(encoded):
                raise SessionAuthorityWriteError("session-closure manifest write was short")
            os.fsync(handle.fileno())
        if os.name != "nt":
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    except FileExistsError:
        raise SessionAuthorityExistsError(
            "session-closure manifest raced with another write"
        ) from None
    except SessionAuthorityError:
        raise
    except OSError as exc:
        raise SessionAuthorityWriteError(
            "session-closure manifest could not be made durable"
        ) from exc


def write_session_start_manifest_v2(
    manifest_path: str | Path,
    *,
    lease: WriterLease,
    session_id: str,
    process_boot_id: str,
    started_wall_ms: int,
    started_monotonic_ns: int,
    wal_authority: WalAuthorityV2,
    wal_durability_binding: WalDurabilityBindingV2,
    block_policy: BlockPolicyV2,
    block_signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    integrity_ledger_max_events: int,
    storage_root_directories: tuple[
        str | Path,
        str | Path,
        str | Path,
        str | Path,
    ],
    grouped_block_root_binding: StorageRootBindingV2,
    integrity_ledger_root_binding: StorageRootBindingV2,
    previous_closure_sha256: str | None = None,
) -> PersistedSessionStartAuthorityV2:
    """Build and persist one exact start while excluding concurrent release."""

    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    with lease.operation_guard():
        claim_path = _canonical_path_text(canonical_session_start_manifest_path_v2(lease))
        try:
            lease.claim_session_start_authority(canonical_path=claim_path)
        except WriterLeaseSessionStartClaimError as exc:
            raise SessionAuthorityExistsError(
                "session-start authority already exists or this writer lease acquisition "
                "already consumed its issuance"
            ) from exc
        manifest = _write_session_start_manifest_guarded_v2(
            manifest_path,
            lease=lease,
            session_id=session_id,
            process_boot_id=process_boot_id,
            started_wall_ms=started_wall_ms,
            started_monotonic_ns=started_monotonic_ns,
            wal_authority=wal_authority,
            wal_durability_binding=wal_durability_binding,
            block_policy=block_policy,
            block_signing_authority=block_signing_authority,
            stream_group_id=stream_group_id,
            segment_id=segment_id,
            integrity_ledger_max_events=integrity_ledger_max_events,
            storage_root_directories=storage_root_directories,
            grouped_block_root_binding=grouped_block_root_binding,
            integrity_ledger_root_binding=integrity_ledger_root_binding,
            previous_closure_sha256=previous_closure_sha256,
        )
        canonical_path, status = _inspect_persisted_manifest_file(
            manifest_path,
            manifest,
            lease=lease,
        )
        canonical_path_text = _canonical_path_text(canonical_path)
        try:
            lease.seal_session_start_authority(
                canonical_path=canonical_path_text,
                manifest_sha256=manifest.sha256,
                byte_count=len(manifest.encoded_line),
                file_device=int(status.st_dev),
                file_inode=int(status.st_ino),
                file_nlink=int(status.st_nlink),
            )
        except WriterLeaseSessionStartClaimError as exc:
            raise SessionAuthorityIntegrityError(
                "persisted session-start could not seal its writer-lease claim"
            ) from exc
        return PersistedSessionStartAuthorityV2(
            manifest=manifest,
            canonical_path=canonical_path_text,
            manifest_sha256=manifest.sha256,
            byte_count=len(manifest.encoded_line),
            file_device=int(status.st_dev),
            file_inode=int(status.st_ino),
            file_nlink=int(status.st_nlink),
            writer_lease=manifest.writer_lease,
            _factory_token=_PERSISTED_AUTHORITY_FACTORY_TOKEN,
        )


def _write_session_start_manifest_guarded_v2(
    manifest_path: str | Path,
    *,
    lease: WriterLease,
    session_id: str,
    process_boot_id: str,
    started_wall_ms: int,
    started_monotonic_ns: int,
    wal_authority: WalAuthorityV2,
    wal_durability_binding: WalDurabilityBindingV2,
    block_policy: BlockPolicyV2,
    block_signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    integrity_ledger_max_events: int,
    storage_root_directories: tuple[
        str | Path,
        str | Path,
        str | Path,
        str | Path,
    ],
    grouped_block_root_binding: StorageRootBindingV2,
    integrity_ledger_root_binding: StorageRootBindingV2,
    previous_closure_sha256: str | None = None,
) -> SessionStartManifestV2:
    """Build, durably create once, and byte-verify one V2 start authority."""

    if type(lease) is not WriterLease:
        raise TypeError("lease must be a WriterLease")
    lease.assert_held()
    if not isinstance(wal_authority, WalAuthorityV2):
        raise TypeError("wal_authority must be a WalAuthorityV2")
    if not isinstance(wal_durability_binding, WalDurabilityBindingV2):
        raise TypeError("wal_durability_binding must be a WalDurabilityBindingV2")
    if wal_durability_binding.mode != "QUALIFIED_DUAL_OWNER":
        raise ValueError("prospective live capture requires QUALIFIED_DUAL_OWNER WAL durability")
    if not isinstance(block_policy, BlockPolicyV2):
        raise TypeError("block_policy must be a BlockPolicyV2")
    if not isinstance(block_signing_authority, BlockSigningAuthorityV2):
        raise TypeError("block_signing_authority must be a BlockSigningAuthorityV2")
    if type(storage_root_directories) is not tuple or len(storage_root_directories) != 4:
        raise ValueError(
            "storage_root_directories must be an exact ordered dual-WAL/block/ledger tuple"
        )
    if not isinstance(grouped_block_root_binding, StorageRootBindingV2):
        raise TypeError("grouped_block_root_binding must be a StorageRootBindingV2")
    if not isinstance(integrity_ledger_root_binding, StorageRootBindingV2):
        raise TypeError("integrity_ledger_root_binding must be a StorageRootBindingV2")

    scope_path = _inspect_existing_directory(lease.scope_root, "writer lease scope")
    expected_output_path = canonical_session_start_manifest_path_v2(lease)
    if _canonical_path_text(manifest_path) != _canonical_path_text(expected_output_path):
        raise SessionAuthorityIntegrityError(
            "session-start manifest path differs from the lease-acquisition canonical path"
        )
    output_path = _inspect_new_manifest_path(manifest_path, scope_path=scope_path)
    expected_bindings = (
        wal_durability_binding.root_bindings[0],
        wal_durability_binding.root_bindings[1],
        grouped_block_root_binding,
        integrity_ledger_root_binding,
    )
    storage_roots = tuple(
        _current_storage_root_reference(
            directory,
            expected,
            scope_path=scope_path,
        )
        for directory, expected in zip(
            storage_root_directories,
            expected_bindings,
            strict=True,
        )
    )
    assert len(storage_roots) == 4
    output_text = _canonical_path_text(output_path)
    if any(
        _paths_equal_or_nested(output_text, reference.canonical_path) for reference in storage_roots
    ):
        raise SessionAuthorityIntegrityError(
            "session-start manifest path must be non-nested with every storage root"
        )
    scope_text = _canonical_path_text(scope_path)
    lease_binding = SessionWriterLeaseBindingV2(
        scope_canonical_path=scope_text,
        scope_path_sha256=_path_sha256(scope_text),
        owner_pid=lease.owner_pid,
        owner_id=lease.owner_id,
        backend=lease.backend,
        acquired_wall_ms=lease.acquired_wall_ms,
        acquired_monotonic_ns=lease.acquired_monotonic_ns,
    )
    selection_sha256 = wal_durability_binding.qualification_selection_receipt_sha256
    if selection_sha256 is None:
        raise ValueError("qualified WAL durability is missing its selection receipt SHA-256")
    manifest = SessionStartManifestV2(
        purpose=_PURPOSE,
        production_order_execution_enabled=False,
        private_credentials_permitted=False,
        attempt_id=wal_authority.attempt_id,
        session_id=session_id,
        process_boot_id=process_boot_id,
        writer_lease=lease_binding,
        started_wall_ms=started_wall_ms,
        started_monotonic_ns=started_monotonic_ns,
        wal_authority=wal_authority,
        wal_authority_sha256=wal_authority.sha256,
        wal_durability_binding=wal_durability_binding,
        wal_durability_binding_sha256=wal_durability_binding.sha256,
        qualification_selection_receipt_sha256=selection_sha256,
        block_policy=block_policy,
        block_signing_authority=block_signing_authority,
        block_signing_authority_sha256=block_signing_authority.sha256,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        integrity_ledger_max_events=integrity_ledger_max_events,
        storage_roots=storage_roots,
        previous_closure_sha256=previous_closure_sha256,
    )
    encoded = manifest.encoded_line

    lease.assert_held()
    _assert_current_storage_roots(storage_roots)
    _write_once(output_path, encoded)
    lease.assert_held()
    _assert_current_storage_roots(storage_roots)
    try:
        observed = output_path.read_bytes()
    except OSError as exc:
        raise SessionAuthorityWriteError("session-start manifest could not be reread") from exc
    if observed != encoded:
        raise SessionAuthorityWriteError(
            "session-start manifest reread differs from its exact canonical bytes"
        )
    return manifest


def assert_session_start_manifest_current_v2(
    manifest: SessionStartManifestV2,
) -> None:
    """Revalidate one start authority and every referenced root in place."""

    if not isinstance(manifest, SessionStartManifestV2):
        raise TypeError("manifest must be a SessionStartManifestV2")
    # Frozen dataclasses can still be corrupted through low-level object APIs.
    # Re-running the complete invariant set keeps this admission boundary
    # fail-closed instead of trusting construction history alone.
    manifest.__post_init__()
    scope_path = _inspect_existing_directory(
        manifest.writer_lease.scope_canonical_path,
        "session writer-lease scope",
    )
    if _canonical_path_text(scope_path) != manifest.writer_lease.scope_canonical_path:
        raise SessionAuthorityIntegrityError(
            "session writer-lease scope differs from its canonical path"
        )
    _assert_current_storage_roots(manifest.storage_roots)


def assert_session_start_manifest_file_current_v2(
    manifest_path: str | Path,
    manifest: SessionStartManifestV2,
    *,
    lease: WriterLease,
) -> None:
    """Prove one held lease and its exact persisted write-once start bytes."""

    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    with lease.operation_guard():
        _inspect_persisted_manifest_file(manifest_path, manifest, lease=lease)


def assert_persisted_session_start_authority_current_v2(
    authority: PersistedSessionStartAuthorityV2,
    *,
    lease: WriterLease,
) -> None:
    """Reprove the factory receipt, original pathname, bytes, and lease."""

    if type(authority) is not PersistedSessionStartAuthorityV2:
        raise TypeError("authority must be an exact PersistedSessionStartAuthorityV2")
    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    with lease.operation_guard():
        authority.__post_init__()
        path, status = _inspect_persisted_manifest_file(
            authority.canonical_path,
            authority.manifest,
            lease=lease,
        )
        if _canonical_path_text(path) != authority.canonical_path:
            raise SessionAuthorityIntegrityError(
                "persisted session-start pathname differs from its receipt"
            )
        if (
            int(status.st_dev) != authority.file_device
            or int(status.st_ino) != authority.file_inode
            or int(status.st_nlink) != authority.file_nlink
            or int(status.st_size) != authority.byte_count
        ):
            raise SessionAuthorityIntegrityError(
                "persisted session-start file identity differs from its receipt"
            )
        if authority.manifest_sha256 != authority.manifest.sha256:
            raise SessionAuthorityIntegrityError(
                "persisted session-start manifest hash differs from its receipt"
            )
        if authority.writer_lease != authority.manifest.writer_lease:
            raise SessionAuthorityIntegrityError(
                "persisted session-start lease differs from its receipt"
            )
        try:
            lease.assert_session_start_authority_claim(
                canonical_path=authority.canonical_path,
                manifest_sha256=authority.manifest_sha256,
                byte_count=authority.byte_count,
                file_device=authority.file_device,
                file_inode=authority.file_inode,
                file_nlink=authority.file_nlink,
            )
        except WriterLeaseSessionStartClaimError as exc:
            raise SessionAuthorityIntegrityError(
                "persisted session-start differs from its writer-lease claim"
            ) from exc
        _assert_exact_live_lease_binding(authority.writer_lease, lease)


def _inspect_persisted_manifest_file(
    manifest_path: str | Path,
    manifest: SessionStartManifestV2,
    *,
    lease: WriterLease,
) -> tuple[Path, os.stat_result]:
    if type(manifest) is not SessionStartManifestV2:
        raise TypeError("manifest must be an exact SessionStartManifestV2")
    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    lease.assert_held()
    assert_session_start_manifest_current_v2(manifest)
    scope_path = _inspect_existing_directory(lease.scope_root, "writer lease scope")
    if _canonical_path_text(scope_path) != manifest.writer_lease.scope_canonical_path:
        raise SessionAuthorityIntegrityError(
            "session-start file lease scope differs from the manifest"
        )
    try:
        inspection = inspect_link_free_path(
            manifest_path,
            "session-start manifest file",
        )
    except ValueError as exc:
        raise SessionAuthorityIntegrityError(str(exc)) from exc
    path = inspection.absolute_path
    status = inspection.final_status
    if status is None or not stat.S_ISREG(status.st_mode):
        raise SessionAuthorityIntegrityError(
            "session-start manifest file must be an existing regular file"
        )
    if not _is_strict_descendant(path, scope_path):
        raise SessionAuthorityIntegrityError(
            "session-start manifest file must be a strict lease-scope descendant"
        )
    if int(status.st_nlink) != 1:
        raise SessionAuthorityIntegrityError(
            "session-start manifest file must have exactly one hard link"
        )
    expected = manifest.encoded_line
    if status.st_size != len(expected):
        raise SessionAuthorityIntegrityError(
            "persisted session-start byte length differs from the manifest"
        )
    identity = (int(status.st_dev), int(status.st_ino))
    try:
        observed = path.read_bytes()
    except OSError as exc:
        raise SessionAuthorityIntegrityError(
            "persisted session-start manifest could not be read"
        ) from exc
    try:
        after = inspect_link_free_path(path, "session-start manifest file")
    except ValueError as exc:
        raise SessionAuthorityIntegrityError(str(exc)) from exc
    after_status = after.final_status
    if (
        after_status is None
        or not stat.S_ISREG(after_status.st_mode)
        or int(after_status.st_nlink) != 1
        or (int(after_status.st_dev), int(after_status.st_ino)) != identity
    ):
        raise SessionAuthorityIntegrityError(
            "session-start manifest file identity changed during validation"
        )
    if observed != expected:
        raise SessionAuthorityIntegrityError(
            "persisted session-start bytes differ from the admitted manifest"
        )
    lease.assert_held()
    return path, after_status


def _assert_exact_live_lease_binding(
    binding: SessionWriterLeaseBindingV2,
    lease: WriterLease,
) -> None:
    observed = SessionWriterLeaseBindingV2(
        scope_canonical_path=_canonical_path_text(lease.scope_root),
        scope_path_sha256=_path_sha256(_canonical_path_text(lease.scope_root)),
        owner_pid=lease.owner_pid,
        owner_id=lease.owner_id,
        backend=lease.backend,
        acquired_wall_ms=lease.acquired_wall_ms,
        acquired_monotonic_ns=lease.acquired_monotonic_ns,
    )
    if observed != binding:
        raise SessionAuthorityIntegrityError(
            "live writer lease differs from the persisted session-start receipt"
        )


def _current_storage_root_reference(
    directory: str | Path,
    expected: StorageRootBindingV2,
    *,
    scope_path: Path | None = None,
) -> SessionStorageRootReferenceV2:
    canonical_path, root_identity, binding_identity = _inspect_storage_root(
        directory,
        "session storage root",
    )
    if scope_path is not None and not _is_strict_descendant(
        canonical_path,
        scope_path,
    ):
        raise SessionAuthorityIntegrityError(
            "session storage root must be a strict descendant of the writer-lease scope"
        )
    try:
        assert_storage_root_binding_v2(canonical_path, expected)
    except RuntimeError as exc:
        raise SessionAuthorityIntegrityError(str(exc)) from exc
    observed_path, observed_root_identity, observed_binding_identity = _inspect_storage_root(
        canonical_path, "session storage root"
    )
    if (
        observed_path != canonical_path
        or observed_root_identity != root_identity
        or observed_binding_identity != binding_identity
    ):
        raise SessionAuthorityIntegrityError(
            "session storage-root pathname identity changed during validation"
        )
    canonical_text = _canonical_path_text(canonical_path)
    return SessionStorageRootReferenceV2(
        canonical_path=canonical_text,
        path_sha256=_path_sha256(canonical_text),
        root_binding=expected,
        root_binding_sha256=_binding_sha256(expected),
        root_device=str(root_identity[0]),
        root_inode=str(root_identity[1]),
        binding_device=str(binding_identity[0]),
        binding_inode=str(binding_identity[1]),
    )


def _assert_current_storage_roots(
    references: tuple[SessionStorageRootReferenceV2, ...],
) -> None:
    for reference in references:
        observed = _current_storage_root_reference(
            reference.canonical_path,
            reference.root_binding,
        )
        if observed != reference:
            raise SessionAuthorityIntegrityError(
                "session storage-root reference changed during start persistence"
            )


def _inspect_storage_root(
    directory: str | Path,
    field: str,
) -> tuple[Path, tuple[int, int], tuple[int, int]]:
    root = _inspect_existing_directory(directory, field)
    binding_path = root / _ROOT_BINDING_FILE
    try:
        binding_inspection = inspect_link_free_path(binding_path, f"{field} binding")
    except ValueError as exc:
        raise SessionAuthorityIntegrityError(str(exc)) from exc
    binding_status = binding_inspection.final_status
    if binding_status is None or not stat.S_ISREG(binding_status.st_mode):
        raise SessionAuthorityIntegrityError(f"{field} binding must be an existing regular file")
    root_status = os.lstat(root)
    return (
        root,
        (int(root_status.st_dev), int(root_status.st_ino)),
        (int(binding_status.st_dev), int(binding_status.st_ino)),
    )


def _inspect_existing_directory(path: str | Path, field: str) -> Path:
    try:
        inspection = inspect_link_free_path(path, field)
    except ValueError as exc:
        raise SessionAuthorityIntegrityError(str(exc)) from exc
    status = inspection.final_status
    if status is None or not stat.S_ISDIR(status.st_mode):
        raise SessionAuthorityIntegrityError(f"{field} must be an existing directory")
    return inspection.absolute_path


def _inspect_new_manifest_path(path: str | Path, *, scope_path: Path) -> Path:
    try:
        inspection = inspect_link_free_path(
            path,
            "session-start manifest",
            allow_missing_tail=True,
        )
    except ValueError as exc:
        raise SessionAuthorityIntegrityError(str(exc)) from exc
    output_path = inspection.absolute_path
    if inspection.final_status is not None:
        raise SessionAuthorityExistsError("session-start manifest path already exists")
    if inspection.first_missing_component != output_path:
        raise SessionAuthorityIntegrityError(
            "session-start manifest parent directory must already exist"
        )
    if not _is_strict_descendant(output_path, scope_path):
        raise SessionAuthorityIntegrityError(
            "session-start manifest must be a strict descendant of the held writer-lease scope"
        )
    if output_path.parent == scope_path and output_path.name == ".signalbot-writer.lock":
        raise SessionAuthorityIntegrityError(
            "session-start manifest cannot replace the writer-lease lock path"
        )
    return output_path


def _write_once(path: Path, encoded: bytes) -> None:
    try:
        with path.open("xb", buffering=0) as handle:
            written = handle.write(encoded)
            if written != len(encoded):
                raise SessionAuthorityWriteError("session-start manifest write was short")
            os.fsync(handle.fileno())
        if os.name != "nt":
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    except FileExistsError:
        raise SessionAuthorityExistsError(
            "session-start manifest raced with another write"
        ) from None
    except SessionAuthorityError:
        raise
    except OSError as exc:
        raise SessionAuthorityWriteError(
            "session-start manifest could not be made durable"
        ) from exc


def _validate_storage_root_binding(binding: StorageRootBindingV2) -> None:
    if binding.schema_version != "r4b_v2_storage_root_binding_v1":
        raise ValueError("unsupported storage-root binding schema")
    if binding.storage_kind not in {
        "WAL",
        "GROUPED_BLOCK",
        "CAPTURE_INTEGRITY_LEDGER",
    }:
        raise ValueError("session storage root has an unsupported storage kind")
    for value, label in (
        (binding.storage_kind, "storage_kind"),
        (binding.root_role, "root_role"),
        (binding.failure_domain_id, "failure_domain_id"),
    ):
        _require_identity(value, label)
    _require_sha256(binding.authority_sha256, "root authority_sha256")
    _require_sha256(binding.contract_sha256, "root contract_sha256")


def _binding_sha256(binding: StorageRootBindingV2) -> str:
    return hashlib.sha256(canonical_json_line(asdict(binding))).hexdigest()


def _contract_sha256(contract: dict[str, object]) -> str:
    return hashlib.sha256(canonical_json_line(contract)).hexdigest()


def _path_sha256(canonical_path: str) -> str:
    return hashlib.sha256(
        _PATH_HASH_DOMAIN + canonical_json_line({"canonical_path": canonical_path})
    ).hexdigest()


def _canonical_path_text(path: str | Path) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


def _paths_equal_or_nested(left: str, right: str) -> bool:
    left_path = Path(left)
    right_path = Path(right)
    return left_path.is_relative_to(right_path) or right_path.is_relative_to(left_path)


def _is_strict_descendant(child: str | Path, parent: str | Path) -> bool:
    child_path = Path(child)
    parent_path = Path(parent)
    return child_path != parent_path and child_path.is_relative_to(parent_path)


def _require_canonical_absolute_path(value: str, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    try:
        canonical = _canonical_path_text(value)
    except (OSError, ValueError) as exc:
        raise ValueError(f"{field} is not a valid path") from exc
    if value != canonical or not Path(value).is_absolute():
        raise ValueError(f"{field} must be an absolute canonical path")


def _require_identity(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a bounded normalized identity")


def _require_sha256(value: str, field: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")


def _require_nonnegative_int(value: int, field: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")


def _require_positive_int(value: int, field: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field} must be a positive integer")


def _require_file_identity(
    device: int,
    inode: int,
    nlink: int,
    field: str,
) -> None:
    if type(device) is not int or device < 0:
        raise ValueError(f"{field} file device must be nonnegative")
    if type(inode) is not int or inode < 0:
        raise ValueError(f"{field} file inode must be nonnegative")
    if type(nlink) is not int or nlink != 1:
        raise ValueError(f"{field} file nlink must equal one")


def _require_decimal_file_identity(
    device: str,
    inode: str,
    nlink: str,
    field: str,
) -> None:
    for value, component in ((device, "device"), (inode, "inode")):
        if (
            not isinstance(value, str)
            or not value.isascii()
            or not value.isdecimal()
            or value != str(int(value))
        ):
            raise ValueError(f"{field} file {component} must be a canonical decimal identity")
    if nlink != "1":
        raise ValueError(f"{field} file nlink must equal canonical decimal one")
