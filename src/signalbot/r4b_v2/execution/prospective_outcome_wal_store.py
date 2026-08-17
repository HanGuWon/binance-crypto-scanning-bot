"""Attempt-wide mirrored storage for structurally admitted position outcomes.

The daily prospective WAL owns decision-cell transactions.  Position evidence
can outlive the originating UTC day, so this module owns one separate bounded
hash chain for the whole attempt.  It deliberately validates only record
identity and lifecycle order.  A higher-level typed position owner must
validate payload meaning before it can obtain the append capability.

This module never places an order and its structural receipts are not efficacy
evidence by themselves.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import stat
import time
from dataclasses import InitVar, asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Final, cast

from signalbot.capture.path_safety import inspect_link_free_path
from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.mirrored_wal import (
    MirroredWalPrefixProofV2,
    MirroredWalWriterV2,
)
from signalbot.r4b_v2.capture.wal import (
    ClockNs,
    FaultHook,
    WalAuthorityV2,
    WalSyncPolicyV2,
)
from signalbot.r4b_v2.capture.wal_qualification import WalSelectionReceiptV2
from signalbot.r4b_v2.execution.paper_sizing import PaperSizingCellV2
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveExpectedCellV2,
    canonical_prospective_census_plan_v2,
)
from signalbot.r4b_v2.execution.prospective_outcome_wal_record import (
    FAMILY_EXIT_DISPOSITION_PAYLOAD_SCHEMA_V2,
    FAMILY_EXIT_PREPARE_PAYLOAD_SCHEMA_V2,
    MAX_PROSPECTIVE_OUTCOME_WAL_RECORD_BYTES_V2,
    POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2,
    POSITION_OPEN_DISPOSITION_PAYLOAD_SCHEMA_V2,
    POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2,
    POSITION_TERMINAL_PAYLOAD_SCHEMA_V2,
    ProspectiveOutcomeWalRecordKindV2,
    ProspectiveOutcomeWalRecordV2,
    build_prospective_outcome_wal_record_v2,
    parse_prospective_outcome_wal_record_v2,
    verify_prospective_outcome_wal_successor_v2,
)

PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_SCHEMA_V2: Final = (
    "r4b_v2_prospective_outcome_wal_store_manifest_v2"
)
PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_FILE_V2: Final = (
    "prospective-outcome-wal-store-manifest-v2.json"
)
PROSPECTIVE_OUTCOME_WAL_BATCH_RECEIPT_SCHEMA_V2: Final = (
    "r4b_v2_prospective_outcome_wal_batch_receipt_v2"
)
PROSPECTIVE_OUTCOME_WAL_REPLAY_SNAPSHOT_SCHEMA_V2: Final = (
    "r4b_v2_prospective_outcome_wal_replay_snapshot_v2"
)
MAX_PROSPECTIVE_OUTCOME_WAL_BATCH_RECORDS_V2: Final = 4_096
MAX_PROSPECTIVE_OUTCOME_WAL_RECORDS_V2: Final = 10_000_000
MAX_PROSPECTIVE_OUTCOME_WAL_ACTIVE_OUTCOMES_V2: Final = 1_000_000
MAX_PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_BYTES_V2: Final = 64 * 1_024

_MANIFEST_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_V2\0"
_BATCH_RECEIPT_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_OUTCOME_WAL_BATCH_RECEIPT_V2\0"
_REPLAY_SNAPSHOT_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_OUTCOME_WAL_REPLAY_SNAPSHOT_V2\0"
_MANIFEST_FACTORY_TOKEN: Final = object()
_RECEIPT_FACTORY_TOKEN: Final = object()
_SNAPSHOT_FACTORY_TOKEN: Final = object()
_STORE_FACTORY_TOKEN: Final = object()
_SHA256_CHARACTERS: Final = frozenset("0123456789abcdef")
_JCS_MAX_SAFE_INTEGER: Final = 9_007_199_254_740_991
_PAYLOAD_SCHEMA_BY_KIND: Final = {
    ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE: (
        POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION: (
        POSITION_OPEN_DISPOSITION_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_PREPARE: (FAMILY_EXIT_PREPARE_PAYLOAD_SCHEMA_V2),
    ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_DISPOSITION: (
        FAMILY_EXIT_DISPOSITION_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW: (POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2),
    ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL: (POSITION_TERMINAL_PAYLOAD_SCHEMA_V2),
}


class ProspectiveOutcomeWalStoreErrorV2(RuntimeError):
    """Base error for attempt-wide outcome WAL ownership."""


class ProspectiveOutcomeWalStoreContractErrorV2(ProspectiveOutcomeWalStoreErrorV2):
    """Raised before mutation when a requested operation is not admissible."""


class ProspectiveOutcomeWalStoreIntegrityErrorV2(ProspectiveOutcomeWalStoreErrorV2):
    """Raised when persisted bytes, paths, or replay order are not exact."""


class ProspectiveOutcomeWalStoreFailedErrorV2(ProspectiveOutcomeWalStoreErrorV2):
    """Raised after a storage mutation makes the owner unusable."""


class ProspectiveOutcomeLifecyclePhaseV2(StrEnum):
    """Structural phase only; it makes no fill or PnL claim."""

    OPEN_PREPARED = "OPEN_PREPARED"
    OPEN_DISPOSITIONED = "OPEN_DISPOSITIONED"
    EXIT_PREPARED = "EXIT_PREPARED"
    TERMINAL = "TERMINAL"


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeWalStoreConfigV2:
    """Frozen attempt-wide WAL bounds and qualified durability inputs."""

    attempt_plan_sha256: str
    primary_directory: Path
    mirror_directory: Path
    policy: WalSyncPolicyV2
    selection_receipt: WalSelectionReceiptV2
    protocol_sha256: str
    source_manifest_sha256: str
    schema_sha256: str
    runtime_manifest_sha256: str
    primary_maximum_total_bytes: int
    mirror_maximum_total_bytes: int
    primary_emergency_reserve_bytes: int
    mirror_emergency_reserve_bytes: int
    primary_failure_domain_id: str
    mirror_failure_domain_id: str
    maximum_batch_records: int
    maximum_records: int
    maximum_active_outcomes: int

    def __post_init__(self) -> None:
        _require_sha256(self.attempt_plan_sha256, "attempt_plan_sha256")
        for value, label in (
            (self.protocol_sha256, "protocol_sha256"),
            (self.source_manifest_sha256, "source_manifest_sha256"),
            (self.schema_sha256, "schema_sha256"),
            (self.runtime_manifest_sha256, "runtime_manifest_sha256"),
        ):
            _require_sha256(value, label)
        if not isinstance(self.primary_directory, Path):
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "primary_directory must be a pathlib.Path"
            )
        if not isinstance(self.mirror_directory, Path):
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "mirror_directory must be a pathlib.Path"
            )
        if type(self.policy) is not WalSyncPolicyV2:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "policy must be an exact WalSyncPolicyV2"
            )
        if type(self.selection_receipt) is not WalSelectionReceiptV2:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "selection_receipt must be an exact WalSelectionReceiptV2"
            )
        self.selection_receipt.require_selected_policy(self.policy)
        if self.policy.max_record_bytes < MAX_PROSPECTIVE_OUTCOME_WAL_RECORD_BYTES_V2:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "selected WAL max_record_bytes cannot hold the outcome record bound"
            )
        for value, label in (
            (self.primary_emergency_reserve_bytes, "primary_emergency_reserve_bytes"),
            (self.mirror_emergency_reserve_bytes, "mirror_emergency_reserve_bytes"),
        ):
            if type(value) is not int or value < 1_024:
                raise ProspectiveOutcomeWalStoreContractErrorV2(
                    f"{label} must be an integer of at least 1024"
                )
        for maximum, reserve, label in (
            (
                self.primary_maximum_total_bytes,
                self.primary_emergency_reserve_bytes,
                "primary_maximum_total_bytes",
            ),
            (
                self.mirror_maximum_total_bytes,
                self.mirror_emergency_reserve_bytes,
                "mirror_maximum_total_bytes",
            ),
        ):
            if type(maximum) is not int or maximum <= reserve:
                raise ProspectiveOutcomeWalStoreContractErrorV2(
                    f"{label} must exceed its emergency reserve"
                )
        _require_identity(self.primary_failure_domain_id, "primary_failure_domain_id")
        _require_identity(self.mirror_failure_domain_id, "mirror_failure_domain_id")
        if self.primary_failure_domain_id.casefold() == self.mirror_failure_domain_id.casefold():
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "failure-domain IDs must be distinct after normalization"
            )
        if type(self.maximum_batch_records) is not int or not (
            1
            <= self.maximum_batch_records
            <= min(
                MAX_PROSPECTIVE_OUTCOME_WAL_BATCH_RECORDS_V2,
                self.policy.max_unsynced_records,
            )
        ):
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "maximum_batch_records exceeds the fixed or selected-policy bound"
            )
        if type(self.maximum_records) is not int or not (
            1 <= self.maximum_records <= MAX_PROSPECTIVE_OUTCOME_WAL_RECORDS_V2
        ):
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "maximum_records exceeds the fixed attempt-wide bound"
            )
        if type(self.maximum_active_outcomes) is not int or not (
            1
            <= self.maximum_active_outcomes
            <= min(
                self.maximum_records,
                MAX_PROSPECTIVE_OUTCOME_WAL_ACTIVE_OUTCOMES_V2,
            )
        ):
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "maximum_active_outcomes exceeds the fixed or record bound"
            )

    @property
    def typed_payload_semantics_authoritative(self) -> bool:
        return False

    @property
    def efficacy_eligible(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeWalStoreManifestV2:
    """Factory-sealed immutable authority persisted outside both WAL roots."""

    attempt_id: str
    attempt_plan_sha256: str
    scope_canonical_path_sha256: str
    scope_device_decimal: str
    scope_inode_decimal: str
    primary_relative_to_scope: str
    primary_device_decimal: str
    primary_inode_decimal: str
    mirror_relative_to_scope: str
    mirror_device_decimal: str
    mirror_inode_decimal: str
    selection_receipt_sha256: str
    policy_sha256: str
    protocol_sha256: str
    source_manifest_sha256: str
    schema_sha256: str
    runtime_manifest_sha256: str
    primary_maximum_total_bytes: int
    mirror_maximum_total_bytes: int
    primary_emergency_reserve_bytes: int
    mirror_emergency_reserve_bytes: int
    primary_failure_domain_id: str
    mirror_failure_domain_id: str
    maximum_batch_records: int
    maximum_records: int
    maximum_active_outcomes: int
    recover_torn_tail: bool
    typed_payload_semantics_authoritative: bool
    efficacy_eligible: bool
    production_order_placement: bool
    _factory_token: InitVar[object | None] = None
    manifest_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default=PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_SCHEMA_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _MANIFEST_FACTORY_TOKEN:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "outcome WAL store manifests are factory-sealed"
            )
        _validate_manifest_fields(self)
        object.__setattr__(
            self,
            "manifest_sha256",
            _hash_document(_MANIFEST_DOMAIN, _manifest_document(self)),
        )

    @property
    def authority(self) -> WalAuthorityV2:
        return WalAuthorityV2(
            attempt_id=self.attempt_id,
            protocol_sha256=self.protocol_sha256,
            plan_sha256=self.manifest_sha256,
            source_manifest_sha256=self.source_manifest_sha256,
            schema_sha256=self.schema_sha256,
            runtime_manifest_sha256=self.runtime_manifest_sha256,
        )


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeWalAppendItemV2:
    """One exact frozen origin and canonical schema-tagged payload."""

    origin_cell: ProspectiveExpectedCellV2
    sizing_cell: PaperSizingCellV2
    kind: ProspectiveOutcomeWalRecordKindV2
    canonical_payload_jsonl: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.origin_cell) is not ProspectiveExpectedCellV2:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "origin_cell must be an exact ProspectiveExpectedCellV2"
            )
        if not isinstance(self.sizing_cell, PaperSizingCellV2):
            raise ProspectiveOutcomeWalStoreContractErrorV2("sizing_cell must be PaperSizingCellV2")
        if not isinstance(self.kind, ProspectiveOutcomeWalRecordKindV2):
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "kind must be ProspectiveOutcomeWalRecordKindV2"
            )
        if type(self.canonical_payload_jsonl) is not bytes:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "canonical_payload_jsonl must be immutable bytes"
            )


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeWalDurableRecordV2:
    """Exact record identity covered by one dual-WAL durable ACK."""

    ingest_seq: int
    outcome_id: str
    origin_segment_id: str
    origin_cell_id: str
    sizing_cell: PaperSizingCellV2
    kind: ProspectiveOutcomeWalRecordKindV2
    payload_sha256: str
    record_sha256: str


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeWalDurableBatchReceiptV2:
    """Factory-sealed proof for one bounded attempt-wide append batch."""

    attempt_plan_sha256: str
    store_manifest_sha256: str
    first_ingest_seq: int
    last_ingest_seq: int
    records: tuple[ProspectiveOutcomeWalDurableRecordV2, ...]
    durable_prefix_proof: MirroredWalPrefixProofV2
    _factory_token: InitVar[object | None] = None
    receipt_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default=PROSPECTIVE_OUTCOME_WAL_BATCH_RECEIPT_SCHEMA_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _RECEIPT_FACTORY_TOKEN:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "outcome WAL durable batch receipts are factory-sealed"
            )
        _require_sha256(self.attempt_plan_sha256, "receipt attempt_plan_sha256")
        _require_sha256(self.store_manifest_sha256, "receipt store_manifest_sha256")
        if type(self.records) is not tuple or not self.records:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "receipt records must be a non-empty tuple"
            )
        if self.first_ingest_seq != self.records[0].ingest_seq:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "receipt first sequence differs from its records"
            )
        if self.last_ingest_seq != self.records[-1].ingest_seq:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "receipt last sequence differs from its records"
            )
        if tuple(item.ingest_seq for item in self.records) != tuple(
            range(self.first_ingest_seq, self.last_ingest_seq + 1)
        ):
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "receipt record sequences are not contiguous"
            )
        for item in self.records:
            _validate_durable_record(item)
        if type(self.durable_prefix_proof) is not MirroredWalPrefixProofV2:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "receipt requires an exact mirrored prefix proof"
            )
        if self.durable_prefix_proof.durable_ack_seq < self.last_ingest_seq:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "durable prefix does not cover the complete batch"
            )
        object.__setattr__(
            self,
            "receipt_sha256",
            _hash_document(_BATCH_RECEIPT_DOMAIN, _receipt_document(self)),
        )

    @property
    def typed_payload_semantics_authoritative(self) -> bool:
        return False

    @property
    def efficacy_eligible(self) -> bool:
        return False

    @property
    def production_order_placement(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeLifecycleSnapshotV2:
    """Bounded structural state for one deterministic outcome identity."""

    outcome_id: str
    origin_segment_id: str
    origin_cell_id: str
    sizing_cell: PaperSizingCellV2
    phase: ProspectiveOutcomeLifecyclePhaseV2
    record_count: int
    cashflow_count: int
    completed_exit_pair_count: int
    last_record_sha256: str


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeWalReplaySnapshotV2:
    """Factory-sealed, structurally replayed prefix for a typed restart owner."""

    attempt_plan_sha256: str
    store_manifest_sha256: str
    records: tuple[ProspectiveOutcomeWalRecordV2, ...] = field(repr=False)
    outcomes: tuple[ProspectiveOutcomeLifecycleSnapshotV2, ...]
    active_outcome_count: int
    terminal_outcome_count: int
    durable_prefix_proof: MirroredWalPrefixProofV2
    _factory_token: InitVar[object | None] = None
    snapshot_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default=PROSPECTIVE_OUTCOME_WAL_REPLAY_SNAPSHOT_SCHEMA_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _SNAPSHOT_FACTORY_TOKEN:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "outcome WAL replay snapshots are factory-sealed"
            )
        _require_sha256(self.attempt_plan_sha256, "snapshot attempt_plan_sha256")
        _require_sha256(self.store_manifest_sha256, "snapshot store_manifest_sha256")
        if type(self.records) is not tuple or any(
            type(record) is not ProspectiveOutcomeWalRecordV2 for record in self.records
        ):
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "snapshot records must be an exact immutable record tuple"
            )
        if type(self.outcomes) is not tuple or any(
            type(outcome) is not ProspectiveOutcomeLifecycleSnapshotV2 for outcome in self.outcomes
        ):
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "snapshot outcomes must be an exact immutable tuple"
            )
        if tuple(outcome.outcome_id for outcome in self.outcomes) != tuple(
            sorted(outcome.outcome_id for outcome in self.outcomes)
        ):
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "snapshot outcomes must be sorted by outcome_id"
            )
        observed_active = sum(
            outcome.phase is not ProspectiveOutcomeLifecyclePhaseV2.TERMINAL
            for outcome in self.outcomes
        )
        observed_terminal = len(self.outcomes) - observed_active
        if (
            self.active_outcome_count != observed_active
            or self.terminal_outcome_count != observed_terminal
        ):
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "snapshot lifecycle counts differ from its outcomes"
            )
        if type(self.durable_prefix_proof) is not MirroredWalPrefixProofV2:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "snapshot requires an exact mirrored prefix proof"
            )
        if self.durable_prefix_proof.durable_ack_seq != len(self.records):
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "snapshot record count differs from its durable prefix"
            )
        object.__setattr__(
            self,
            "snapshot_sha256",
            _hash_document(_REPLAY_SNAPSHOT_DOMAIN, _snapshot_document(self)),
        )

    @property
    def typed_payload_semantics_authoritative(self) -> bool:
        return False

    @property
    def efficacy_eligible(self) -> bool:
        return False

    @property
    def production_order_placement(self) -> bool:
        return False


@dataclass(slots=True)
class _MutableOutcomeState:
    outcome_id: str
    origin_segment_id: str
    origin_cell_id: str
    sizing_cell: PaperSizingCellV2
    phase: ProspectiveOutcomeLifecyclePhaseV2
    record_count: int
    cashflow_count: int
    completed_exit_pair_count: int
    last_record_sha256: str
    operation_fingerprints: set[tuple[ProspectiveOutcomeWalRecordKindV2, str]]


@dataclass(frozen=True, slots=True)
class _PathIdentity:
    canonical_path: Path
    device: int
    inode: int


@dataclass(frozen=True, slots=True)
class _PathBinding:
    scope: _PathIdentity
    primary: _PathIdentity
    mirror: _PathIdentity
    primary_relative: str
    mirror_relative: str


@dataclass(frozen=True, slots=True)
class ProspectiveOutcomeWalStoreFactoryV2:
    """Open or structurally verify the single attempt-wide dual WAL."""

    config: ProspectiveOutcomeWalStoreConfigV2
    clock_ns: ClockNs = time.monotonic_ns
    primary_fault_hook: FaultHook | None = field(default=None, repr=False)
    mirror_fault_hook: FaultHook | None = field(default=None, repr=False)
    recover_torn_tail: bool = False

    def __post_init__(self) -> None:
        if type(self.config) is not ProspectiveOutcomeWalStoreConfigV2:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "factory config must be exact ProspectiveOutcomeWalStoreConfigV2"
            )
        if not callable(self.clock_ns):
            raise ProspectiveOutcomeWalStoreContractErrorV2("clock_ns must be callable")
        for hook, label in (
            (self.primary_fault_hook, "primary_fault_hook"),
            (self.mirror_fault_hook, "mirror_fault_hook"),
        ):
            if hook is not None and not callable(hook):
                raise ProspectiveOutcomeWalStoreContractErrorV2(f"{label} must be callable or None")
        if self.recover_torn_tail is not False:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "authoritative outcome WAL requires recover_torn_tail=False"
            )

    def open(
        self,
        *,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
        replay_snapshot: ProspectiveOutcomeWalReplaySnapshotV2 | None = None,
        recovered_state_owner: object | None = None,
    ) -> ProspectiveOutcomeWalStoreV2:
        """Open genesis or resume only with an exact verified prefix and owner.

        ``recovered_state_owner`` is an identity capability supplied by the
        higher-level typed owner.  This structural layer cannot certify that
        owner's payload semantics; it merely requires the same identity again
        when the append capability is claimed.
        """

        self._validate_attempt_binding(census_plan, writer_lease)
        binding = _validate_paths(
            writer_lease=writer_lease,
            primary_directory=self.config.primary_directory,
            mirror_directory=self.config.mirror_directory,
        )
        writer: MirroredWalWriterV2 | None = None
        with writer_lease.operation_guard():
            _assert_lease_claim(writer_lease, census_plan.plan_sha256)
            manifest, resumed = _load_or_create_manifest(
                census_plan=census_plan,
                config=self.config,
                binding=binding,
            )
            if resumed:
                if (
                    type(replay_snapshot) is not ProspectiveOutcomeWalReplaySnapshotV2
                    or recovered_state_owner is None
                ):
                    raise ProspectiveOutcomeWalStoreContractErrorV2(
                        "reopening requires an exact replay snapshot and higher-level "
                        "typed state owner"
                    )
                _assert_snapshot_manifest(replay_snapshot, manifest)
            elif replay_snapshot is not None or recovered_state_owner is not None:
                raise ProspectiveOutcomeWalStoreContractErrorV2(
                    "fresh outcome WAL forbids restart-only owner or snapshot inputs"
                )
            try:
                writer = self._open_writer(manifest)
                states, records, snapshot = _replay_writer(
                    writer=writer,
                    census_plan=census_plan,
                    config=self.config,
                    manifest=manifest,
                )
                if resumed:
                    assert replay_snapshot is not None
                    if not hmac.compare_digest(
                        replay_snapshot.snapshot_sha256,
                        snapshot.snapshot_sha256,
                    ):
                        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                            "live replay differs from the supplied verified snapshot"
                        )
                elif records:
                    raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                        "new outcome WAL unexpectedly replayed durable records"
                    )
            except Exception as open_error:
                if writer is None:
                    raise
                try:
                    writer.abort()
                except Exception as abort_error:
                    raise ExceptionGroup(
                        "outcome WAL open/replay and cleanup failed",
                        [open_error, abort_error],
                    ) from None
                raise
        return ProspectiveOutcomeWalStoreV2(
            census_plan=census_plan,
            writer_lease=writer_lease,
            factory=self,
            manifest=manifest,
            writer=writer,
            states=states,
            records=records,
            resumed=resumed,
            expected_recovery_owner=recovered_state_owner,
            replay_snapshot=snapshot,
            _factory_token=_STORE_FACTORY_TOKEN,
        )

    def verify_replay_snapshot_v2(
        self,
        *,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
    ) -> ProspectiveOutcomeWalReplaySnapshotV2:
        """Read and structurally replay one cleanly finalized existing prefix."""

        self._validate_attempt_binding(census_plan, writer_lease)
        binding = _validate_paths(
            writer_lease=writer_lease,
            primary_directory=self.config.primary_directory,
            mirror_directory=self.config.mirror_directory,
        )
        with writer_lease.operation_guard():
            _assert_lease_claim(writer_lease, census_plan.plan_sha256)
            manifest = _load_existing_manifest(
                census_plan=census_plan,
                config=self.config,
                binding=binding,
            )
            writer = MirroredWalWriterV2.open_verification_only_v2(
                binding.primary.canonical_path,
                binding.mirror.canonical_path,
                authority=manifest.authority,
                policy=self.config.policy,
                selection_receipt=self.config.selection_receipt,
                primary_maximum_total_bytes=self.config.primary_maximum_total_bytes,
                mirror_maximum_total_bytes=self.config.mirror_maximum_total_bytes,
                primary_emergency_reserve_bytes=(self.config.primary_emergency_reserve_bytes),
                mirror_emergency_reserve_bytes=(self.config.mirror_emergency_reserve_bytes),
                primary_failure_domain_id=self.config.primary_failure_domain_id,
                mirror_failure_domain_id=self.config.mirror_failure_domain_id,
                clock_ns=self.clock_ns,
            )
            _states, _records, snapshot = _replay_writer(
                writer=writer,
                census_plan=census_plan,
                config=self.config,
                manifest=manifest,
            )
            return snapshot

    def _validate_attempt_binding(
        self,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
    ) -> None:
        if type(census_plan) is not ProspectiveCensusPlanV2:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "census_plan must be exact ProspectiveCensusPlanV2"
            )
        if type(writer_lease) is not WriterLease:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "writer_lease must be an exact WriterLease"
            )
        try:
            canonical_prospective_census_plan_v2(census_plan)
        except ValueError as exc:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "census plan failed its canonical self-hash check"
            ) from exc
        config = self.config
        if census_plan.plan_sha256 != config.attempt_plan_sha256:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "census plan differs from config attempt_plan_sha256"
            )
        if config.selection_receipt.h_start_wall_ms != census_plan.attempt.horizon.h_start_ms:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "selection receipt H_start differs from the census plan"
            )
        if config.protocol_sha256 != census_plan.execution_contract_sha256:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "config protocol_sha256 differs from the execution contract"
            )
        if config.source_manifest_sha256 != census_plan.strategy_code_freeze_manifest_sha256:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "config source manifest differs from the strategy code freeze"
            )
        if config.runtime_manifest_sha256 != (
            config.selection_receipt.qualification.runtime_manifest_sha256
        ):
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "config runtime manifest differs from WAL qualification"
            )

    def _open_writer(
        self,
        manifest: ProspectiveOutcomeWalStoreManifestV2,
    ) -> MirroredWalWriterV2:
        config = self.config
        return MirroredWalWriterV2(
            config.primary_directory,
            config.mirror_directory,
            authority=manifest.authority,
            policy=config.policy,
            selection_receipt=config.selection_receipt,
            primary_maximum_total_bytes=config.primary_maximum_total_bytes,
            mirror_maximum_total_bytes=config.mirror_maximum_total_bytes,
            primary_emergency_reserve_bytes=config.primary_emergency_reserve_bytes,
            mirror_emergency_reserve_bytes=config.mirror_emergency_reserve_bytes,
            primary_failure_domain_id=config.primary_failure_domain_id,
            mirror_failure_domain_id=config.mirror_failure_domain_id,
            clock_ns=self.clock_ns,
            primary_fault_hook=self.primary_fault_hook,
            mirror_fault_hook=self.mirror_fault_hook,
            recover_torn_tail=False,
        )


class ProspectiveOutcomeWalStoreV2:
    """Single-lease structural owner of one attempt-wide position chain."""

    __slots__ = (
        "_active_outcome_count",
        "_census_plan",
        "_closed",
        "_expected_recovery_owner",
        "_factory",
        "_failed",
        "_lifecycle_claim",
        "_lifecycle_owner",
        "_manifest",
        "_records",
        "_replay_snapshot",
        "_resumed",
        "_states",
        "_writer",
        "_writer_lease",
    )

    def __init__(
        self,
        *,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
        factory: ProspectiveOutcomeWalStoreFactoryV2,
        manifest: ProspectiveOutcomeWalStoreManifestV2,
        writer: MirroredWalWriterV2,
        states: dict[str, _MutableOutcomeState],
        records: list[ProspectiveOutcomeWalRecordV2],
        resumed: bool,
        expected_recovery_owner: object | None,
        replay_snapshot: ProspectiveOutcomeWalReplaySnapshotV2,
        _factory_token: object | None = None,
    ) -> None:
        if _factory_token is not _STORE_FACTORY_TOKEN:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "outcome WAL stores must be opened by the exact factory"
            )
        self._census_plan = census_plan
        self._writer_lease = writer_lease
        self._factory = factory
        self._manifest = manifest
        self._writer = writer
        self._states = states
        self._records = records
        self._active_outcome_count = sum(
            state.phase is not ProspectiveOutcomeLifecyclePhaseV2.TERMINAL
            for state in states.values()
        )
        self._resumed = resumed
        self._expected_recovery_owner = expected_recovery_owner
        self._replay_snapshot = replay_snapshot
        self._lifecycle_owner: object | None = None
        self._lifecycle_claim: object | None = None
        self._closed = False
        self._failed: Exception | None = None

    @property
    def manifest(self) -> ProspectiveOutcomeWalStoreManifestV2:
        return self._manifest

    @property
    def record_count(self) -> int:
        return len(self._records)

    @property
    def active_outcome_count(self) -> int:
        return self._active_outcome_count

    @property
    def typed_payload_semantics_authoritative(self) -> bool:
        return False

    @property
    def efficacy_eligible(self) -> bool:
        return False

    @property
    def production_order_placement(self) -> bool:
        return False

    def assert_position_lifecycle_binding_v2(
        self,
        *,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
        lifecycle_owner: object,
        lifecycle_claim: object | None = None,
    ) -> None:
        """Require the exact attempt, lease, owner identity, and optional claim."""

        self._raise_if_unavailable()
        if type(census_plan) is not ProspectiveCensusPlanV2:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "census_plan must be exact ProspectiveCensusPlanV2"
            )
        if census_plan is not self._census_plan:
            canonical_prospective_census_plan_v2(census_plan)
            if census_plan != self._census_plan:
                raise ProspectiveOutcomeWalStoreContractErrorV2(
                    "position owner does not share the outcome WAL census plan"
                )
        if type(writer_lease) is not WriterLease or writer_lease is not self._writer_lease:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "position owner does not share the exact outcome WAL writer lease"
            )
        if lifecycle_owner is None:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "lifecycle_owner identity must be supplied"
            )
        if self._lifecycle_owner is not None and lifecycle_owner is not self._lifecycle_owner:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "outcome WAL belongs to a different position lifecycle owner"
            )
        if lifecycle_claim is not None and lifecycle_claim is not self._lifecycle_claim:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "position lifecycle claim differs from the exact store capability"
            )
        self._assert_lease_claim()

    def _claim_position_lifecycle_owner_v2(
        self,
        *,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
        lifecycle_owner: object,
        replay_snapshot: ProspectiveOutcomeWalReplaySnapshotV2 | None = None,
    ) -> object:
        """Irreversibly mint the only append capability for the typed owner."""

        self.assert_position_lifecycle_binding_v2(
            census_plan=census_plan,
            writer_lease=writer_lease,
            lifecycle_owner=lifecycle_owner,
        )
        if self._lifecycle_claim is not None:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "outcome WAL already has a position lifecycle owner"
            )
        if self._resumed:
            if lifecycle_owner is not self._expected_recovery_owner:
                raise ProspectiveOutcomeWalStoreContractErrorV2(
                    "resumed outcome WAL owner differs from the supplied recovery owner"
                )
            if type(replay_snapshot) is not ProspectiveOutcomeWalReplaySnapshotV2:
                raise ProspectiveOutcomeWalStoreContractErrorV2(
                    "resumed outcome WAL claim requires the exact replay snapshot"
                )
            if not hmac.compare_digest(
                replay_snapshot.snapshot_sha256,
                self._replay_snapshot.snapshot_sha256,
            ):
                raise ProspectiveOutcomeWalStoreContractErrorV2(
                    "resumed owner snapshot differs from the opened durable prefix"
                )
        elif replay_snapshot is not None:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "fresh outcome WAL owner cannot claim a replay snapshot"
            )
        claim = object()
        self._lifecycle_owner = lifecycle_owner
        self._lifecycle_claim = claim
        return claim

    def append_and_sync(
        self,
        *,
        item: ProspectiveOutcomeWalAppendItemV2,
        lifecycle_claim: object,
    ) -> ProspectiveOutcomeWalDurableBatchReceiptV2:
        return self.append_batch_and_sync((item,), lifecycle_claim=lifecycle_claim)

    def append_batch_and_sync(
        self,
        items: tuple[ProspectiveOutcomeWalAppendItemV2, ...],
        *,
        lifecycle_claim: object,
    ) -> ProspectiveOutcomeWalDurableBatchReceiptV2:
        """Append one bounded batch and force the exact prefix to both roots."""

        self._raise_if_unavailable()
        with self._writer_lease.operation_guard():
            self._assert_lease_claim()
            self._assert_append_claim(lifecycle_claim)
            return self._append_batch_guarded(items)

    def replay_snapshot_v2(
        self,
        *,
        lifecycle_claim: object,
    ) -> ProspectiveOutcomeWalReplaySnapshotV2:
        """Return a fresh forced-durable structural snapshot of this live owner."""

        self._raise_if_unavailable()
        with self._writer_lease.operation_guard():
            self._assert_lease_claim()
            self._assert_append_claim(lifecycle_claim)
            try:
                prefix = self._writer.prove_durable_prefix_v2()
            except Exception as exc:
                self._failed = exc
                raise
            snapshot = _build_snapshot(
                manifest=self._manifest,
                records=self._records,
                states=self._states,
                prefix=prefix,
            )
            self._replay_snapshot = snapshot
            return snapshot

    def close(self) -> None:
        if self._closed:
            return
        self._raise_if_failed()
        with self._writer_lease.operation_guard():
            self._assert_lease_claim()
            try:
                self._writer.close()
            except Exception as exc:
                self._failed = exc
                self._closed = True
                try:
                    self._writer.abort()
                except Exception as abort_error:
                    raise ExceptionGroup(
                        "outcome WAL close and abort failed",
                        [exc, abort_error],
                    ) from None
                raise
            self._closed = True

    def abort(self) -> None:
        if self._closed:
            return
        with self._writer_lease.operation_guard():
            self._assert_lease_claim()
            try:
                self._writer.abort()
            except Exception as exc:
                self._failed = exc
                self._closed = True
                raise
            self._closed = True

    def _append_batch_guarded(
        self,
        items: tuple[ProspectiveOutcomeWalAppendItemV2, ...],
    ) -> ProspectiveOutcomeWalDurableBatchReceiptV2:
        config = self._factory.config
        if type(items) is not tuple or not items:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "append batch must be a non-empty immutable tuple"
            )
        if len(items) > config.maximum_batch_records:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "append batch exceeds maximum_batch_records"
            )
        if any(type(item) is not ProspectiveOutcomeWalAppendItemV2 for item in items):
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "append batch contains a non-exact item"
            )
        if len(self._records) + len(items) > config.maximum_records:
            raise ProspectiveOutcomeWalStoreContractErrorV2("append would exceed maximum_records")
        exact_cells = tuple(self._exact_plan_cell(item.origin_cell) for item in items)
        planned_states: dict[str, _MutableOutcomeState] = {}
        planned_active_outcome_count = self._active_outcome_count
        records: list[ProspectiveOutcomeWalRecordV2] = []
        predecessor = self._records[-1].record_sha256 if self._records else None
        next_ingest_seq = self._writer.next_ingest_seq
        expected_next = len(self._records) + 1
        if next_ingest_seq != expected_next:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "writer cursor differs from the replayed outcome chain"
            )
        for offset, (item, cell) in enumerate(zip(items, exact_cells, strict=True)):
            record = build_prospective_outcome_wal_record_v2(
                ingest_seq=next_ingest_seq + offset,
                kind=item.kind,
                attempt_plan_sha256=self._census_plan.plan_sha256,
                origin_segment_id=cell.segment_id,
                origin_cell_id=cell.cell_id,
                sizing_cell=item.sizing_cell,
                payload_schema=_PAYLOAD_SCHEMA_BY_KIND[item.kind],
                canonical_payload_jsonl=item.canonical_payload_jsonl,
                previous_record_sha256=predecessor,
            )
            if record.outcome_id not in planned_states:
                existing = self._states.get(record.outcome_id)
                if existing is not None:
                    planned_states[record.outcome_id] = _clone_state(existing)
            planned_active_outcome_count = _admit_transition(
                states=planned_states,
                record=record,
                maximum_active_outcomes=config.maximum_active_outcomes,
                active_outcome_count=planned_active_outcome_count,
                error_type=ProspectiveOutcomeWalStoreContractErrorV2,
            )
            records.append(record)
            predecessor = record.record_sha256
        if sum(record.encoded_len for record in records) > config.policy.max_unsynced_bytes:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "encoded batch exceeds the selected WAL byte bound"
            )
        try:
            result = self._writer.append_batch(records)
            durable_ack_seq = self._writer.sync()
            if (
                result.first_ingest_seq != records[0].ingest_seq
                or result.last_ingest_seq != records[-1].ingest_seq
                or result.record_count != len(records)
                or durable_ack_seq != records[-1].ingest_seq
            ):
                raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                    "mirrored WAL acknowledgement differs from the exact outcome batch"
                )
            prefix = self._writer.prove_durable_prefix_v2()
            if prefix.durable_ack_seq != durable_ack_seq:
                raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                    "mirrored prefix proof differs from the forced durable ACK"
                )
            receipt = ProspectiveOutcomeWalDurableBatchReceiptV2(
                attempt_plan_sha256=self._census_plan.plan_sha256,
                store_manifest_sha256=self._manifest.manifest_sha256,
                first_ingest_seq=records[0].ingest_seq,
                last_ingest_seq=records[-1].ingest_seq,
                records=tuple(_durable_record(record) for record in records),
                durable_prefix_proof=prefix,
                _factory_token=_RECEIPT_FACTORY_TOKEN,
            )
        except Exception as exc:
            self._failed = exc
            raise
        self._states.update(planned_states)
        self._active_outcome_count = planned_active_outcome_count
        self._records.extend(records)
        return receipt

    def _exact_plan_cell(
        self,
        cell: ProspectiveExpectedCellV2,
    ) -> ProspectiveExpectedCellV2:
        if type(cell) is not ProspectiveExpectedCellV2:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "origin cell must be exact ProspectiveExpectedCellV2"
            )
        try:
            expected = self._census_plan.expected_cell(
                family=cell.family,
                symbol=cell.symbol,
                bar_open_ms=cell.bar_open_ms,
            )
        except ValueError as exc:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "origin cell is outside the frozen prospective census"
            ) from exc
        if cell != expected:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "origin cell differs from its exact frozen census identity"
            )
        return expected

    def _assert_append_claim(self, lifecycle_claim: object) -> None:
        if self._lifecycle_claim is None or lifecycle_claim is not self._lifecycle_claim:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                "append lacks the exact position lifecycle owner claim"
            )

    def _assert_lease_claim(self) -> None:
        _assert_lease_claim(self._writer_lease, self._census_plan.plan_sha256)

    def _raise_if_failed(self) -> None:
        if self._failed is not None:
            raise ProspectiveOutcomeWalStoreFailedErrorV2(
                "outcome WAL store is failed"
            ) from self._failed

    def _raise_if_unavailable(self) -> None:
        self._raise_if_failed()
        if self._closed:
            raise ProspectiveOutcomeWalStoreFailedErrorV2("outcome WAL store is closed")


def canonical_prospective_outcome_wal_store_manifest_v2(
    manifest: ProspectiveOutcomeWalStoreManifestV2,
) -> bytes:
    if type(manifest) is not ProspectiveOutcomeWalStoreManifestV2:
        raise TypeError("manifest must be exact ProspectiveOutcomeWalStoreManifestV2")
    return canonical_json_line(
        {**_manifest_document(manifest), "manifest_sha256": manifest.manifest_sha256}
    )


def parse_prospective_outcome_wal_store_manifest_v2(
    encoded: bytes,
) -> ProspectiveOutcomeWalStoreManifestV2:
    if type(encoded) is not bytes or not encoded.endswith(b"\n") or b"\n" in encoded[:-1]:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest must be one immutable JSONL line"
        )
    if len(encoded) > MAX_PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_BYTES_V2:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest exceeds its fixed byte bound"
        )
    try:
        value = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(value, dict):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest must be an object"
        )
    document = cast(dict[str, object], value)
    expected_keys = frozenset(_manifest_document_keys()) | {"manifest_sha256"}
    if frozenset(document) != expected_keys:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest has missing or unknown fields"
        )
    try:
        if canonical_json_line(document) != encoded:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "outcome WAL store manifest is not canonical JSONL"
            )
    except (TypeError, ValueError) as exc:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest has unsupported canonical JSON"
        ) from exc
    manifest = ProspectiveOutcomeWalStoreManifestV2(
        attempt_id=_manifest_text(document, "attempt_id"),
        attempt_plan_sha256=_manifest_text(document, "attempt_plan_sha256"),
        scope_canonical_path_sha256=_manifest_text(document, "scope_canonical_path_sha256"),
        scope_device_decimal=_manifest_text(document, "scope_device_decimal"),
        scope_inode_decimal=_manifest_text(document, "scope_inode_decimal"),
        primary_relative_to_scope=_manifest_text(document, "primary_relative_to_scope"),
        primary_device_decimal=_manifest_text(document, "primary_device_decimal"),
        primary_inode_decimal=_manifest_text(document, "primary_inode_decimal"),
        mirror_relative_to_scope=_manifest_text(document, "mirror_relative_to_scope"),
        mirror_device_decimal=_manifest_text(document, "mirror_device_decimal"),
        mirror_inode_decimal=_manifest_text(document, "mirror_inode_decimal"),
        selection_receipt_sha256=_manifest_text(document, "selection_receipt_sha256"),
        policy_sha256=_manifest_text(document, "policy_sha256"),
        protocol_sha256=_manifest_text(document, "protocol_sha256"),
        source_manifest_sha256=_manifest_text(document, "source_manifest_sha256"),
        schema_sha256=_manifest_text(document, "schema_sha256"),
        runtime_manifest_sha256=_manifest_text(document, "runtime_manifest_sha256"),
        primary_maximum_total_bytes=_manifest_int(document, "primary_maximum_total_bytes"),
        mirror_maximum_total_bytes=_manifest_int(document, "mirror_maximum_total_bytes"),
        primary_emergency_reserve_bytes=_manifest_int(document, "primary_emergency_reserve_bytes"),
        mirror_emergency_reserve_bytes=_manifest_int(document, "mirror_emergency_reserve_bytes"),
        primary_failure_domain_id=_manifest_text(document, "primary_failure_domain_id"),
        mirror_failure_domain_id=_manifest_text(document, "mirror_failure_domain_id"),
        maximum_batch_records=_manifest_int(document, "maximum_batch_records"),
        maximum_records=_manifest_int(document, "maximum_records"),
        maximum_active_outcomes=_manifest_int(document, "maximum_active_outcomes"),
        recover_torn_tail=_manifest_bool(document, "recover_torn_tail"),
        typed_payload_semantics_authoritative=_manifest_bool(
            document, "typed_payload_semantics_authoritative"
        ),
        efficacy_eligible=_manifest_bool(document, "efficacy_eligible"),
        production_order_placement=_manifest_bool(document, "production_order_placement"),
        _factory_token=_MANIFEST_FACTORY_TOKEN,
    )
    if document.get("schema_version") != manifest.schema_version:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "unsupported outcome WAL store manifest schema"
        )
    persisted_hash = _manifest_text(document, "manifest_sha256")
    if not hmac.compare_digest(persisted_hash, manifest.manifest_sha256):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest self-hash differs"
        )
    return manifest


def _replay_writer(
    *,
    writer: MirroredWalWriterV2,
    census_plan: ProspectiveCensusPlanV2,
    config: ProspectiveOutcomeWalStoreConfigV2,
    manifest: ProspectiveOutcomeWalStoreManifestV2,
) -> tuple[
    dict[str, _MutableOutcomeState],
    list[ProspectiveOutcomeWalRecordV2],
    ProspectiveOutcomeWalReplaySnapshotV2,
]:
    states: dict[str, _MutableOutcomeState] = {}
    records: list[ProspectiveOutcomeWalRecordV2] = []
    last_record: ProspectiveOutcomeWalRecordV2 | None = None
    active_outcome_count = 0

    def consume(ingest_seq: int, encoded_line: bytes) -> None:
        nonlocal active_outcome_count, last_record
        if len(records) >= config.maximum_records:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "replayed outcome prefix exceeds maximum_records"
            )
        record = parse_prospective_outcome_wal_record_v2(encoded_line)
        if record.ingest_seq != ingest_seq:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "replayed callback sequence differs from the strict outcome record"
            )
        if record.attempt_plan_sha256 != census_plan.plan_sha256:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                "replayed outcome record targets a foreign attempt plan"
            )
        if last_record is None:
            if record.ingest_seq != 1 or record.previous_record_sha256 is not None:
                raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                    "replayed outcome chain has no exact genesis"
                )
        else:
            try:
                verify_prospective_outcome_wal_successor_v2(last_record, record)
            except ValueError as exc:
                raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                    "replayed outcome chain has a non-adjacent successor"
                ) from exc
        active_outcome_count = _admit_transition(
            states=states,
            record=record,
            maximum_active_outcomes=config.maximum_active_outcomes,
            active_outcome_count=active_outcome_count,
            error_type=ProspectiveOutcomeWalStoreIntegrityErrorV2,
        )
        records.append(record)
        last_record = record

    delivered = writer.consume_durable_records(consume)
    if delivered != writer.durable_ack_seq or delivered != len(records):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "replayed outcome record count differs from the durable ACK"
        )
    if writer.next_ingest_seq != delivered + 1:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome writer cursor differs from its replayed durable prefix"
        )
    prefix = writer.prove_durable_prefix_v2()
    snapshot = _build_snapshot(
        manifest=manifest,
        records=records,
        states=states,
        prefix=prefix,
    )
    return states, records, snapshot


def _admit_transition(
    *,
    states: dict[str, _MutableOutcomeState],
    record: ProspectiveOutcomeWalRecordV2,
    maximum_active_outcomes: int,
    active_outcome_count: int,
    error_type: type[ProspectiveOutcomeWalStoreErrorV2],
) -> int:
    state = states.get(record.outcome_id)
    operation = (record.kind, record.payload_sha256)
    if state is None:
        if record.kind is not ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE:
            raise error_type("outcome lifecycle must begin with POSITION_OPEN_PREPARE")
        if active_outcome_count >= maximum_active_outcomes:
            raise error_type("outcome append would exceed maximum_active_outcomes")
        states[record.outcome_id] = _MutableOutcomeState(
            outcome_id=record.outcome_id,
            origin_segment_id=record.origin_segment_id,
            origin_cell_id=record.origin_cell_id,
            sizing_cell=record.sizing_cell,
            phase=ProspectiveOutcomeLifecyclePhaseV2.OPEN_PREPARED,
            record_count=1,
            cashflow_count=0,
            completed_exit_pair_count=0,
            last_record_sha256=record.record_sha256,
            operation_fingerprints={operation},
        )
        return active_outcome_count + 1
    if (
        state.origin_segment_id != record.origin_segment_id
        or state.origin_cell_id != record.origin_cell_id
        or state.sizing_cell is not record.sizing_cell
    ):
        raise error_type("outcome identity conflicts with its admitted origin")
    if operation in state.operation_fingerprints:
        raise error_type("duplicate outcome operation is forbidden")
    phase = state.phase
    kind = record.kind
    if (
        phase is ProspectiveOutcomeLifecyclePhaseV2.OPEN_PREPARED
        and kind is ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION
    ):
        state.phase = ProspectiveOutcomeLifecyclePhaseV2.OPEN_DISPOSITIONED
    elif phase is ProspectiveOutcomeLifecyclePhaseV2.OPEN_DISPOSITIONED:
        if kind is ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW:
            state.cashflow_count += 1
        elif kind is ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_PREPARE:
            state.phase = ProspectiveOutcomeLifecyclePhaseV2.EXIT_PREPARED
        elif kind is ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL:
            state.phase = ProspectiveOutcomeLifecyclePhaseV2.TERMINAL
            active_outcome_count -= 1
        else:
            raise error_type("outcome operation conflicts with OPEN_DISPOSITIONED phase")
    elif phase is ProspectiveOutcomeLifecyclePhaseV2.EXIT_PREPARED:
        if kind is ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_DISPOSITION:
            state.phase = ProspectiveOutcomeLifecyclePhaseV2.OPEN_DISPOSITIONED
            state.completed_exit_pair_count += 1
        elif kind is ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL:
            state.phase = ProspectiveOutcomeLifecyclePhaseV2.TERMINAL
            active_outcome_count -= 1
        else:
            raise error_type("outcome operation conflicts with EXIT_PREPARED phase")
    elif phase is ProspectiveOutcomeLifecyclePhaseV2.TERMINAL:
        raise error_type("post-terminal outcome operation is forbidden")
    else:
        raise error_type("outcome operation violates the structural transition grammar")
    state.record_count += 1
    state.last_record_sha256 = record.record_sha256
    state.operation_fingerprints.add(operation)
    return active_outcome_count


def _build_snapshot(
    *,
    manifest: ProspectiveOutcomeWalStoreManifestV2,
    records: list[ProspectiveOutcomeWalRecordV2],
    states: dict[str, _MutableOutcomeState],
    prefix: MirroredWalPrefixProofV2,
) -> ProspectiveOutcomeWalReplaySnapshotV2:
    outcomes = tuple(
        ProspectiveOutcomeLifecycleSnapshotV2(
            outcome_id=state.outcome_id,
            origin_segment_id=state.origin_segment_id,
            origin_cell_id=state.origin_cell_id,
            sizing_cell=state.sizing_cell,
            phase=state.phase,
            record_count=state.record_count,
            cashflow_count=state.cashflow_count,
            completed_exit_pair_count=state.completed_exit_pair_count,
            last_record_sha256=state.last_record_sha256,
        )
        for state in sorted(states.values(), key=lambda item: item.outcome_id)
    )
    active = sum(
        outcome.phase is not ProspectiveOutcomeLifecyclePhaseV2.TERMINAL for outcome in outcomes
    )
    return ProspectiveOutcomeWalReplaySnapshotV2(
        attempt_plan_sha256=manifest.attempt_plan_sha256,
        store_manifest_sha256=manifest.manifest_sha256,
        records=tuple(records),
        outcomes=outcomes,
        active_outcome_count=active,
        terminal_outcome_count=len(outcomes) - active,
        durable_prefix_proof=prefix,
        _factory_token=_SNAPSHOT_FACTORY_TOKEN,
    )


def _clone_state(value: _MutableOutcomeState) -> _MutableOutcomeState:
    return _MutableOutcomeState(
        outcome_id=value.outcome_id,
        origin_segment_id=value.origin_segment_id,
        origin_cell_id=value.origin_cell_id,
        sizing_cell=value.sizing_cell,
        phase=value.phase,
        record_count=value.record_count,
        cashflow_count=value.cashflow_count,
        completed_exit_pair_count=value.completed_exit_pair_count,
        last_record_sha256=value.last_record_sha256,
        operation_fingerprints=set(value.operation_fingerprints),
    )


def _durable_record(
    record: ProspectiveOutcomeWalRecordV2,
) -> ProspectiveOutcomeWalDurableRecordV2:
    return ProspectiveOutcomeWalDurableRecordV2(
        ingest_seq=record.ingest_seq,
        outcome_id=record.outcome_id,
        origin_segment_id=record.origin_segment_id,
        origin_cell_id=record.origin_cell_id,
        sizing_cell=record.sizing_cell,
        kind=record.kind,
        payload_sha256=record.payload_sha256,
        record_sha256=record.record_sha256,
    )


def _validate_durable_record(item: ProspectiveOutcomeWalDurableRecordV2) -> None:
    if type(item) is not ProspectiveOutcomeWalDurableRecordV2:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "receipt contains a non-exact durable record identity"
        )
    if type(item.ingest_seq) is not int or not 1 <= item.ingest_seq <= _JCS_MAX_SAFE_INTEGER:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "receipt ingest_seq must be a positive JCS-safe integer"
        )
    if not isinstance(item.kind, ProspectiveOutcomeWalRecordKindV2):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "receipt kind must be ProspectiveOutcomeWalRecordKindV2"
        )
    if not isinstance(item.sizing_cell, PaperSizingCellV2):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "receipt sizing_cell must be PaperSizingCellV2"
        )
    for value, label in (
        (item.outcome_id, "receipt outcome_id"),
        (item.origin_segment_id, "receipt origin_segment_id"),
        (item.origin_cell_id, "receipt origin_cell_id"),
        (item.payload_sha256, "receipt payload_sha256"),
        (item.record_sha256, "receipt record_sha256"),
    ):
        _require_sha256(value, label)


def _load_or_create_manifest(
    *,
    census_plan: ProspectiveCensusPlanV2,
    config: ProspectiveOutcomeWalStoreConfigV2,
    binding: _PathBinding,
) -> tuple[ProspectiveOutcomeWalStoreManifestV2, bool]:
    final_path = binding.scope.canonical_path / PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_FILE_V2
    partial_path = final_path.with_name(final_path.name + ".partial")
    if _regular_artifact_exists(partial_path, "outcome WAL store manifest partial"):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest has an unfinished partial"
        )
    if _regular_artifact_exists(final_path, "outcome WAL store manifest"):
        manifest = _read_manifest(final_path)
        _assert_manifest_matches(manifest, census_plan, config, binding)
        return manifest, True
    if any(binding.primary.canonical_path.iterdir()) or any(
        binding.mirror.canonical_path.iterdir()
    ):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "existing outcome WAL residues require the exact store manifest"
        )
    manifest = _build_manifest(census_plan, config, binding)
    encoded = canonical_prospective_outcome_wal_store_manifest_v2(manifest)
    try:
        with partial_path.open("xb", buffering=0) as handle:
            total = 0
            while total < len(encoded):
                written = handle.write(encoded[total:])
                if written is None or written <= 0:
                    raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
                        "outcome WAL manifest partial write made no progress"
                    )
                total += written
            os.fsync(handle.fileno())
        os.replace(partial_path, final_path)
        _fsync_parent(final_path)
        _fsync_file(final_path)
    except FileExistsError as exc:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest creation raced with another artifact"
        ) from exc
    persisted = _read_manifest(final_path)
    if persisted != manifest:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "persisted outcome WAL store manifest differs from its candidate"
        )
    return persisted, False


def _load_existing_manifest(
    *,
    census_plan: ProspectiveCensusPlanV2,
    config: ProspectiveOutcomeWalStoreConfigV2,
    binding: _PathBinding,
) -> ProspectiveOutcomeWalStoreManifestV2:
    final_path = binding.scope.canonical_path / PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_FILE_V2
    partial_path = final_path.with_name(final_path.name + ".partial")
    if _regular_artifact_exists(partial_path, "outcome WAL store manifest partial"):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest has an unfinished partial"
        )
    if not _regular_artifact_exists(final_path, "outcome WAL store manifest"):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "verification requires the exact outcome WAL store manifest"
        )
    manifest = _read_manifest(final_path)
    _assert_manifest_matches(manifest, census_plan, config, binding)
    return manifest


def _build_manifest(
    census_plan: ProspectiveCensusPlanV2,
    config: ProspectiveOutcomeWalStoreConfigV2,
    binding: _PathBinding,
) -> ProspectiveOutcomeWalStoreManifestV2:
    return ProspectiveOutcomeWalStoreManifestV2(
        attempt_id=census_plan.attempt_id,
        attempt_plan_sha256=census_plan.plan_sha256,
        scope_canonical_path_sha256=_path_sha256(binding.scope.canonical_path),
        scope_device_decimal=str(binding.scope.device),
        scope_inode_decimal=str(binding.scope.inode),
        primary_relative_to_scope=binding.primary_relative,
        primary_device_decimal=str(binding.primary.device),
        primary_inode_decimal=str(binding.primary.inode),
        mirror_relative_to_scope=binding.mirror_relative,
        mirror_device_decimal=str(binding.mirror.device),
        mirror_inode_decimal=str(binding.mirror.inode),
        selection_receipt_sha256=config.selection_receipt.sha256,
        policy_sha256=_policy_sha256(config.policy),
        protocol_sha256=config.protocol_sha256,
        source_manifest_sha256=config.source_manifest_sha256,
        schema_sha256=config.schema_sha256,
        runtime_manifest_sha256=config.runtime_manifest_sha256,
        primary_maximum_total_bytes=config.primary_maximum_total_bytes,
        mirror_maximum_total_bytes=config.mirror_maximum_total_bytes,
        primary_emergency_reserve_bytes=config.primary_emergency_reserve_bytes,
        mirror_emergency_reserve_bytes=config.mirror_emergency_reserve_bytes,
        primary_failure_domain_id=config.primary_failure_domain_id,
        mirror_failure_domain_id=config.mirror_failure_domain_id,
        maximum_batch_records=config.maximum_batch_records,
        maximum_records=config.maximum_records,
        maximum_active_outcomes=config.maximum_active_outcomes,
        recover_torn_tail=False,
        typed_payload_semantics_authoritative=False,
        efficacy_eligible=False,
        production_order_placement=False,
        _factory_token=_MANIFEST_FACTORY_TOKEN,
    )


def _assert_manifest_matches(
    manifest: ProspectiveOutcomeWalStoreManifestV2,
    census_plan: ProspectiveCensusPlanV2,
    config: ProspectiveOutcomeWalStoreConfigV2,
    binding: _PathBinding,
) -> None:
    expected = _build_manifest(census_plan, config, binding)
    if manifest != expected:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "existing outcome WAL store manifest differs from the exact plan, paths, or config"
        )


def _assert_snapshot_manifest(
    snapshot: ProspectiveOutcomeWalReplaySnapshotV2,
    manifest: ProspectiveOutcomeWalStoreManifestV2,
) -> None:
    if (
        snapshot.attempt_plan_sha256 != manifest.attempt_plan_sha256
        or snapshot.store_manifest_sha256 != manifest.manifest_sha256
    ):
        raise ProspectiveOutcomeWalStoreContractErrorV2(
            "replay snapshot differs from the exact outcome WAL manifest"
        )


def _validate_paths(
    *,
    writer_lease: WriterLease,
    primary_directory: Path,
    mirror_directory: Path,
) -> _PathBinding:
    writer_lease.assert_held()
    scope = _inspect_directory(writer_lease.scope_root, "writer lease scope")
    primary = _inspect_directory(primary_directory, "PRIMARY outcome WAL directory")
    mirror = _inspect_directory(mirror_directory, "INDEPENDENT_MIRROR outcome WAL directory")
    for identity, label in ((primary, "PRIMARY"), (mirror, "INDEPENDENT_MIRROR")):
        if identity.canonical_path == scope.canonical_path or not (
            identity.canonical_path.is_relative_to(scope.canonical_path)
        ):
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                f"{label} outcome WAL directory must be a strict child of the lease scope"
            )
    if (
        primary.canonical_path == mirror.canonical_path
        or primary.canonical_path.is_relative_to(mirror.canonical_path)
        or mirror.canonical_path.is_relative_to(primary.canonical_path)
    ):
        raise ProspectiveOutcomeWalStoreContractErrorV2(
            "PRIMARY and INDEPENDENT_MIRROR outcome WAL paths must not overlap"
        )
    return _PathBinding(
        scope=scope,
        primary=primary,
        mirror=mirror,
        primary_relative=primary.canonical_path.relative_to(scope.canonical_path).as_posix(),
        mirror_relative=mirror.canonical_path.relative_to(scope.canonical_path).as_posix(),
    )


def _inspect_directory(path: Path, label: str) -> _PathIdentity:
    try:
        inspection = inspect_link_free_path(path, label)
    except ValueError as exc:
        raise ProspectiveOutcomeWalStoreContractErrorV2(str(exc)) from exc
    status = inspection.final_status
    if status is None or not stat.S_ISDIR(status.st_mode):
        raise ProspectiveOutcomeWalStoreContractErrorV2(
            f"{label} must be an existing link-free directory"
        )
    return _PathIdentity(
        canonical_path=inspection.absolute_path.resolve(strict=True),
        device=int(status.st_dev),
        inode=int(status.st_ino),
    )


def _regular_artifact_exists(path: Path, label: str) -> bool:
    try:
        inspection = inspect_link_free_path(path, label, allow_missing_tail=True)
    except ValueError as exc:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(str(exc)) from exc
    status = inspection.final_status
    if status is None:
        if inspection.first_missing_component != path:
            raise ProspectiveOutcomeWalStoreIntegrityErrorV2(f"{label} parent directory is missing")
        return False
    if not stat.S_ISREG(status.st_mode) or int(status.st_nlink) != 1:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            f"{label} must be a regular single-link file"
        )
    return True


def _read_manifest(path: Path) -> ProspectiveOutcomeWalStoreManifestV2:
    before = inspect_link_free_path(path, "outcome WAL store manifest").final_status
    if (
        before is None
        or not stat.S_ISREG(before.st_mode)
        or int(before.st_nlink) != 1
        or not 1 <= int(before.st_size) <= MAX_PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_BYTES_V2
    ):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest has an invalid file identity or size"
        )
    identity = (int(before.st_dev), int(before.st_ino), int(before.st_size))
    try:
        encoded = path.read_bytes()
    except OSError as exc:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest could not be read"
        ) from exc
    after = inspect_link_free_path(path, "outcome WAL store manifest").final_status
    if (
        after is None
        or not stat.S_ISREG(after.st_mode)
        or int(after.st_nlink) != 1
        or (int(after.st_dev), int(after.st_ino), int(after.st_size)) != identity
    ):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            "outcome WAL store manifest changed identity while being read"
        )
    return parse_prospective_outcome_wal_store_manifest_v2(encoded)


def _validate_manifest_fields(manifest: ProspectiveOutcomeWalStoreManifestV2) -> None:
    _require_identity(manifest.attempt_id, "manifest attempt_id")
    for value, label in (
        (manifest.attempt_plan_sha256, "manifest attempt_plan_sha256"),
        (manifest.scope_canonical_path_sha256, "manifest scope path hash"),
        (manifest.selection_receipt_sha256, "manifest selection receipt hash"),
        (manifest.policy_sha256, "manifest policy hash"),
        (manifest.protocol_sha256, "manifest protocol hash"),
        (manifest.source_manifest_sha256, "manifest source hash"),
        (manifest.schema_sha256, "manifest schema hash"),
        (manifest.runtime_manifest_sha256, "manifest runtime hash"),
    ):
        _require_sha256(value, label)
    for value, label in (
        (manifest.scope_device_decimal, "scope_device_decimal"),
        (manifest.scope_inode_decimal, "scope_inode_decimal"),
        (manifest.primary_device_decimal, "primary_device_decimal"),
        (manifest.primary_inode_decimal, "primary_inode_decimal"),
        (manifest.mirror_device_decimal, "mirror_device_decimal"),
        (manifest.mirror_inode_decimal, "mirror_inode_decimal"),
    ):
        if not isinstance(value, str) or not value.isdecimal():
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                f"manifest {label} must be unsigned decimal text"
            )
    for value, label in (
        (manifest.primary_relative_to_scope, "primary_relative_to_scope"),
        (manifest.mirror_relative_to_scope, "mirror_relative_to_scope"),
        (manifest.primary_failure_domain_id, "primary_failure_domain_id"),
        (manifest.mirror_failure_domain_id, "mirror_failure_domain_id"),
    ):
        _require_identity(value, f"manifest {label}")
    for value, label in (
        (manifest.primary_emergency_reserve_bytes, "primary_emergency_reserve_bytes"),
        (manifest.mirror_emergency_reserve_bytes, "mirror_emergency_reserve_bytes"),
    ):
        if type(value) is not int or value < 1_024:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                f"manifest {label} must be an integer of at least 1024"
            )
    for maximum, reserve, label in (
        (
            manifest.primary_maximum_total_bytes,
            manifest.primary_emergency_reserve_bytes,
            "primary_maximum_total_bytes",
        ),
        (
            manifest.mirror_maximum_total_bytes,
            manifest.mirror_emergency_reserve_bytes,
            "mirror_maximum_total_bytes",
        ),
    ):
        if type(maximum) is not int or maximum <= reserve:
            raise ProspectiveOutcomeWalStoreContractErrorV2(
                f"manifest {label} must exceed its emergency reserve"
            )
    if type(manifest.maximum_batch_records) is not int or not (
        1 <= manifest.maximum_batch_records <= MAX_PROSPECTIVE_OUTCOME_WAL_BATCH_RECORDS_V2
    ):
        raise ProspectiveOutcomeWalStoreContractErrorV2(
            "manifest maximum_batch_records exceeds its fixed bound"
        )
    if type(manifest.maximum_records) is not int or not (
        1 <= manifest.maximum_records <= MAX_PROSPECTIVE_OUTCOME_WAL_RECORDS_V2
    ):
        raise ProspectiveOutcomeWalStoreContractErrorV2(
            "manifest maximum_records exceeds its fixed bound"
        )
    if type(manifest.maximum_active_outcomes) is not int or not (
        1
        <= manifest.maximum_active_outcomes
        <= min(
            manifest.maximum_records,
            MAX_PROSPECTIVE_OUTCOME_WAL_ACTIVE_OUTCOMES_V2,
        )
    ):
        raise ProspectiveOutcomeWalStoreContractErrorV2(
            "manifest maximum_active_outcomes exceeds its fixed or record bound"
        )
    if manifest.schema_version != PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_SCHEMA_V2:
        raise ProspectiveOutcomeWalStoreContractErrorV2(
            "unsupported outcome WAL store manifest schema"
        )
    if (
        manifest.recover_torn_tail is not False
        or manifest.typed_payload_semantics_authoritative is not False
        or manifest.efficacy_eligible is not False
        or manifest.production_order_placement is not False
    ):
        raise ProspectiveOutcomeWalStoreContractErrorV2(
            "outcome WAL manifest cannot claim recovery, typed semantics, efficacy, or orders"
        )


def _manifest_document(
    manifest: ProspectiveOutcomeWalStoreManifestV2,
) -> dict[str, object]:
    return {
        "attempt_id": manifest.attempt_id,
        "attempt_plan_sha256": manifest.attempt_plan_sha256,
        "efficacy_eligible": manifest.efficacy_eligible,
        "maximum_active_outcomes": manifest.maximum_active_outcomes,
        "maximum_batch_records": manifest.maximum_batch_records,
        "maximum_records": manifest.maximum_records,
        "mirror_device_decimal": manifest.mirror_device_decimal,
        "mirror_emergency_reserve_bytes": manifest.mirror_emergency_reserve_bytes,
        "mirror_failure_domain_id": manifest.mirror_failure_domain_id,
        "mirror_inode_decimal": manifest.mirror_inode_decimal,
        "mirror_maximum_total_bytes": manifest.mirror_maximum_total_bytes,
        "mirror_relative_to_scope": manifest.mirror_relative_to_scope,
        "policy_sha256": manifest.policy_sha256,
        "primary_device_decimal": manifest.primary_device_decimal,
        "primary_emergency_reserve_bytes": manifest.primary_emergency_reserve_bytes,
        "primary_failure_domain_id": manifest.primary_failure_domain_id,
        "primary_inode_decimal": manifest.primary_inode_decimal,
        "primary_maximum_total_bytes": manifest.primary_maximum_total_bytes,
        "primary_relative_to_scope": manifest.primary_relative_to_scope,
        "production_order_placement": manifest.production_order_placement,
        "protocol_sha256": manifest.protocol_sha256,
        "recover_torn_tail": manifest.recover_torn_tail,
        "runtime_manifest_sha256": manifest.runtime_manifest_sha256,
        "schema_sha256": manifest.schema_sha256,
        "schema_version": manifest.schema_version,
        "scope_canonical_path_sha256": manifest.scope_canonical_path_sha256,
        "scope_device_decimal": manifest.scope_device_decimal,
        "scope_inode_decimal": manifest.scope_inode_decimal,
        "selection_receipt_sha256": manifest.selection_receipt_sha256,
        "source_manifest_sha256": manifest.source_manifest_sha256,
        "typed_payload_semantics_authoritative": (manifest.typed_payload_semantics_authoritative),
    }


def _manifest_document_keys() -> tuple[str, ...]:
    return (
        "attempt_id",
        "attempt_plan_sha256",
        "efficacy_eligible",
        "maximum_active_outcomes",
        "maximum_batch_records",
        "maximum_records",
        "mirror_device_decimal",
        "mirror_emergency_reserve_bytes",
        "mirror_failure_domain_id",
        "mirror_inode_decimal",
        "mirror_maximum_total_bytes",
        "mirror_relative_to_scope",
        "policy_sha256",
        "primary_device_decimal",
        "primary_emergency_reserve_bytes",
        "primary_failure_domain_id",
        "primary_inode_decimal",
        "primary_maximum_total_bytes",
        "primary_relative_to_scope",
        "production_order_placement",
        "protocol_sha256",
        "recover_torn_tail",
        "runtime_manifest_sha256",
        "schema_sha256",
        "schema_version",
        "scope_canonical_path_sha256",
        "scope_device_decimal",
        "scope_inode_decimal",
        "selection_receipt_sha256",
        "source_manifest_sha256",
        "typed_payload_semantics_authoritative",
    )


def _receipt_document(
    receipt: ProspectiveOutcomeWalDurableBatchReceiptV2,
) -> dict[str, object]:
    return {
        "attempt_plan_sha256": receipt.attempt_plan_sha256,
        "durable_prefix_proof": asdict(receipt.durable_prefix_proof),
        "first_ingest_seq": receipt.first_ingest_seq,
        "last_ingest_seq": receipt.last_ingest_seq,
        "records": [
            {
                "ingest_seq": item.ingest_seq,
                "kind": item.kind.value,
                "origin_cell_id": item.origin_cell_id,
                "origin_segment_id": item.origin_segment_id,
                "outcome_id": item.outcome_id,
                "payload_sha256": item.payload_sha256,
                "record_sha256": item.record_sha256,
                "sizing_cell": item.sizing_cell.value,
            }
            for item in receipt.records
        ],
        "schema_version": receipt.schema_version,
        "store_manifest_sha256": receipt.store_manifest_sha256,
    }


def _snapshot_document(
    snapshot: ProspectiveOutcomeWalReplaySnapshotV2,
) -> dict[str, object]:
    return {
        "active_outcome_count": snapshot.active_outcome_count,
        "attempt_plan_sha256": snapshot.attempt_plan_sha256,
        "durable_prefix_proof": asdict(snapshot.durable_prefix_proof),
        "outcomes": [
            {
                "cashflow_count": outcome.cashflow_count,
                "completed_exit_pair_count": outcome.completed_exit_pair_count,
                "last_record_sha256": outcome.last_record_sha256,
                "origin_cell_id": outcome.origin_cell_id,
                "origin_segment_id": outcome.origin_segment_id,
                "outcome_id": outcome.outcome_id,
                "phase": outcome.phase.value,
                "record_count": outcome.record_count,
                "sizing_cell": outcome.sizing_cell.value,
            }
            for outcome in snapshot.outcomes
        ],
        "record_sha256s": [record.record_sha256 for record in snapshot.records],
        "schema_version": snapshot.schema_version,
        "store_manifest_sha256": snapshot.store_manifest_sha256,
        "terminal_outcome_count": snapshot.terminal_outcome_count,
    }


def _policy_sha256(policy: WalSyncPolicyV2) -> str:
    return hashlib.sha256(canonical_json_line(asdict(policy))).hexdigest()


def _path_sha256(path: Path) -> str:
    return hashlib.sha256(
        b"R4B_V2_PROSPECTIVE_OUTCOME_WAL_SCOPE_PATH\0" + os.fspath(path).encode("utf-8")
    ).hexdigest()


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _assert_lease_claim(writer_lease: WriterLease, attempt_plan_sha256: str) -> None:
    writer_lease.assert_prospective_attempt_authority_claim(attempt_plan_sha256=attempt_plan_sha256)


def _require_sha256(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _SHA256_CHARACTERS for character in value)
    ):
        raise ProspectiveOutcomeWalStoreContractErrorV2(f"{label} must be lowercase SHA-256 hex")


def _require_identity(value: object, label: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
        or any(character in value for character in "\r\n\x00")
    ):
        raise ProspectiveOutcomeWalStoreContractErrorV2(
            f"{label} must be a bounded normalized identity"
        )


def _manifest_text(document: dict[str, object], field_name: str) -> str:
    value = document.get(field_name)
    if not isinstance(value, str):
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(f"manifest {field_name} must be text")
    return value


def _manifest_int(document: dict[str, object], field_name: str) -> int:
    value = document.get(field_name)
    if type(value) is not int:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(
            f"manifest {field_name} must be an integer"
        )
    return value


def _manifest_bool(document: dict[str, object], field_name: str) -> bool:
    value = document.get(field_name)
    if type(value) is not bool:
        raise ProspectiveOutcomeWalStoreIntegrityErrorV2(f"manifest {field_name} must be boolean")
    return value


def _fsync_file(path: Path) -> None:
    mode = "rb+" if os.name == "nt" else "rb"
    with path.open(mode, buffering=0) as handle:
        os.fsync(handle.fileno())


def _fsync_parent(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
