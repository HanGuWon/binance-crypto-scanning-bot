"""Bounded daily mirrored-WAL storage for one frozen prospective census.

This owner assigns segment-local sequence numbers and predecessor hashes.  It
does not evaluate a strategy, mint decision receipts, evaluate PAPER fills, or
seal a segment.  Every recovered byte is parsed through the strict prospective
record parser before a shard accepts another append.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import time
from dataclasses import InitVar, asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Final

from signalbot.capture.path_safety import inspect_link_free_path
from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp, SystemReceiptClock
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
    ProspectiveCensusSegmentV2,
    ProspectiveExpectedCellV2,
    canonical_prospective_census_plan_v2,
)
from signalbot.r4b_v2.execution.prospective_decision_payload import (
    ProspectiveCellDispositionPayloadV2,
    ProspectiveDecisionPreparePayloadV2,
    ProspectiveDispositionClassV2,
    canonical_prospective_cell_disposition_payload_v2,
    canonical_prospective_decision_prepare_payload_v2,
    parse_prospective_cell_disposition_payload_v2,
    parse_prospective_decision_prepare_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_paper_terminal_payload import (
    ProspectivePaperTerminalPayloadV2,
    ProspectivePaperTerminalStatusV2,
    canonical_prospective_paper_terminal_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_wal_record import (
    CELL_DISPOSITION_PAYLOAD_SCHEMA_V2,
    DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
    PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
    ProspectiveWalRecordKindV2,
    ProspectiveWalRecordV2,
    build_prospective_wal_record_v2,
    parse_prospective_wal_record_v2,
    verify_prospective_wal_successor_v2,
)
from signalbot.r4b_v2.protocol.lifecycle import MILLISECONDS_PER_DAY_V2

PROSPECTIVE_DAILY_WAL_SHARD_PLAN_SCHEMA_V2: Final = "r4b_v2_prospective_daily_wal_shard_plan_v2"
PROSPECTIVE_DAILY_WAL_BATCH_RECEIPT_SCHEMA_V2: Final = (
    "r4b_v2_prospective_daily_wal_batch_receipt_v2"
)
PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_SCHEMA_V2: Final = (
    "r4b_v2_prospective_daily_wal_store_manifest_v2"
)
PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_FILE_V2: Final = "prospective-daily-wal-store-manifest-v2.json"
MAX_PROSPECTIVE_DAILY_WAL_BATCH_RECORDS_V2: Final = 4_096
MAX_ACTIVE_PROSPECTIVE_DAILY_WAL_SHARDS_V2: Final = 2
MAX_PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_BYTES_V2: Final = 64 * 1_024

_SHARD_PLAN_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_DAILY_WAL_SHARD_PLAN_V2\0"
_BATCH_RECEIPT_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_DAILY_WAL_BATCH_RECEIPT_V2\0"
_STORE_MANIFEST_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_V2\0"
_SHARD_PLAN_FACTORY_TOKEN: Final = object()
_BATCH_RECEIPT_FACTORY_TOKEN: Final = object()
_STORE_MANIFEST_FACTORY_TOKEN: Final = object()
_STORE_FACTORY_TOKEN: Final = object()
_SHA256_CHARACTERS: Final = frozenset("0123456789abcdef")
_PAYLOAD_SCHEMA_BY_KIND: Final = {
    ProspectiveWalRecordKindV2.DECISION_PREPARE: (DECISION_PREPARE_PAYLOAD_SCHEMA_V2),
    ProspectiveWalRecordKindV2.CELL_DISPOSITION: (CELL_DISPOSITION_PAYLOAD_SCHEMA_V2),
    ProspectiveWalRecordKindV2.PAPER_TERMINAL: PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
}
_PAPER_TERMINAL_STATUSES_BY_DISPOSITION_V2: Final = {
    ProspectiveDispositionClassV2.NO_SIGNAL: frozenset(
        {ProspectivePaperTerminalStatusV2.NO_SIGNAL}
    ),
    ProspectiveDispositionClassV2.SUPPRESSED: frozenset(
        {ProspectivePaperTerminalStatusV2.SUPPRESSED_DECISION}
    ),
    ProspectiveDispositionClassV2.INCONCLUSIVE: frozenset(
        {ProspectivePaperTerminalStatusV2.INCOMPLETE_DECISION}
    ),
    ProspectiveDispositionClassV2.SIGNAL: frozenset(
        {
            ProspectivePaperTerminalStatusV2.SUPPRESSED_SIZING,
            ProspectivePaperTerminalStatusV2.INCOMPLETE_PAPER,
            ProspectivePaperTerminalStatusV2.PAPER_IOC_NO_FILL,
            ProspectivePaperTerminalStatusV2.PAPER_CAPACITY_REJECTED,
            ProspectivePaperTerminalStatusV2.PAPER_EXECUTED_FULL_QUANTITY,
        }
    ),
}


class ProspectiveDailyWalStoreErrorV2(RuntimeError):
    """Base error for prospective daily WAL ownership and recovery."""


class ProspectiveDailyWalStoreContractErrorV2(ProspectiveDailyWalStoreErrorV2):
    """Raised before an append when caller input violates the frozen contract."""


class ProspectiveDailyWalStoreIntegrityErrorV2(ProspectiveDailyWalStoreErrorV2):
    """Raised when durable shard bytes or authority do not replay exactly."""


class ProspectiveDailyWalStoreFailedErrorV2(ProspectiveDailyWalStoreErrorV2):
    """Raised after a storage mutation failed and the owner became unusable."""


@dataclass(frozen=True, slots=True)
class ProspectiveDailyWalStoreManifestV2:
    """Factory-sealed attempt-wide configuration pinned before ``H_start``."""

    attempt_id: str
    census_plan_sha256: str
    scope_canonical_path_sha256: str
    scope_device_decimal: str
    scope_inode_decimal: str
    primary_base_device_decimal: str
    primary_base_inode_decimal: str
    mirror_base_device_decimal: str
    mirror_base_inode_decimal: str
    primary_base_relative_to_scope: str
    mirror_base_relative_to_scope: str
    selection_receipt_sha256: str
    policy_sha256: str
    protocol_sha256: str
    source_manifest_sha256: str
    schema_sha256: str
    runtime_manifest_sha256: str
    primary_maximum_total_bytes_per_shard: int
    mirror_maximum_total_bytes_per_shard: int
    primary_emergency_reserve_bytes_per_shard: int
    mirror_emergency_reserve_bytes_per_shard: int
    primary_failure_domain_id: str
    mirror_failure_domain_id: str
    maximum_batch_records: int
    typed_decision_payloads_required: bool
    h_start_ms: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    _factory_token: InitVar[object | None] = None
    manifest_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default=PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_SCHEMA_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _STORE_MANIFEST_FACTORY_TOKEN:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "daily WAL store manifests are factory-sealed"
            )
        _validate_store_manifest_fields(self)
        object.__setattr__(
            self,
            "manifest_sha256",
            _hash_document(
                _STORE_MANIFEST_DOMAIN,
                _store_manifest_document(self),
            ),
        )


@dataclass(frozen=True, slots=True)
class ProspectiveDailyWalStoreConfigV2:
    """Frozen storage inputs shared by every daily shard of one attempt."""

    attempt_plan_sha256: str
    primary_base_directory: Path
    mirror_base_directory: Path
    policy: WalSyncPolicyV2
    selection_receipt: WalSelectionReceiptV2
    protocol_sha256: str
    source_manifest_sha256: str
    schema_sha256: str
    runtime_manifest_sha256: str
    primary_maximum_total_bytes_per_shard: int
    mirror_maximum_total_bytes_per_shard: int
    primary_emergency_reserve_bytes_per_shard: int
    mirror_emergency_reserve_bytes_per_shard: int
    primary_failure_domain_id: str
    mirror_failure_domain_id: str
    maximum_batch_records: int
    typed_decision_payloads_required: bool = False

    def __post_init__(self) -> None:
        _require_sha256(self.attempt_plan_sha256, "attempt_plan_sha256")
        for value, name in (
            (self.protocol_sha256, "protocol_sha256"),
            (self.source_manifest_sha256, "source_manifest_sha256"),
            (self.schema_sha256, "schema_sha256"),
            (self.runtime_manifest_sha256, "runtime_manifest_sha256"),
        ):
            _require_sha256(value, name)
        if not isinstance(self.primary_base_directory, Path):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "primary_base_directory must be a pathlib.Path"
            )
        if not isinstance(self.mirror_base_directory, Path):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "mirror_base_directory must be a pathlib.Path"
            )
        if type(self.policy) is not WalSyncPolicyV2:
            raise ProspectiveDailyWalStoreContractErrorV2("policy must be an exact WalSyncPolicyV2")
        if type(self.selection_receipt) is not WalSelectionReceiptV2:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "selection_receipt must be an exact WalSelectionReceiptV2"
            )
        self.selection_receipt.require_selected_policy(self.policy)
        if type(self.typed_decision_payloads_required) is not bool:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "typed_decision_payloads_required must be boolean"
            )
        for value, name in (
            (
                self.primary_emergency_reserve_bytes_per_shard,
                "primary_emergency_reserve_bytes_per_shard",
            ),
            (
                self.mirror_emergency_reserve_bytes_per_shard,
                "mirror_emergency_reserve_bytes_per_shard",
            ),
        ):
            if type(value) is not int or value < 1_024:
                raise ProspectiveDailyWalStoreContractErrorV2(
                    f"{name} must be an integer of at least 1024"
                )
        for maximum, reserve, name in (
            (
                self.primary_maximum_total_bytes_per_shard,
                self.primary_emergency_reserve_bytes_per_shard,
                "primary_maximum_total_bytes_per_shard",
            ),
            (
                self.mirror_maximum_total_bytes_per_shard,
                self.mirror_emergency_reserve_bytes_per_shard,
                "mirror_maximum_total_bytes_per_shard",
            ),
        ):
            if type(maximum) is not int or maximum <= reserve:
                raise ProspectiveDailyWalStoreContractErrorV2(
                    f"{name} must exceed its emergency reserve"
                )
        _require_identity(
            self.primary_failure_domain_id,
            "primary_failure_domain_id",
        )
        _require_identity(
            self.mirror_failure_domain_id,
            "mirror_failure_domain_id",
        )
        if self.primary_failure_domain_id.casefold() == (self.mirror_failure_domain_id.casefold()):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "failure-domain IDs must be distinct after normalization"
            )
        if type(
            self.maximum_batch_records
        ) is not int or not 1 <= self.maximum_batch_records <= min(
            MAX_PROSPECTIVE_DAILY_WAL_BATCH_RECORDS_V2,
            self.policy.max_unsynced_records,
        ):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "maximum_batch_records exceeds the frozen or selected-policy bound"
            )


@dataclass(frozen=True, slots=True)
class ProspectiveDailyWalShardPlanV2:
    """Factory-sealed exact authority document for one UTC-day shard."""

    attempt_id: str
    attempt_plan_sha256: str
    store_manifest_sha256: str
    segment_index: int
    segment_id: str
    day_start_ms: int
    first_bar_open_ms: int
    bar_open_stop_exclusive_ms: int
    expected_cell_count: int
    primary_directory_relative_to_scope: str
    mirror_directory_relative_to_scope: str
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
    typed_decision_payloads_required: bool
    _factory_token: InitVar[object | None] = None
    shard_plan_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default=PROSPECTIVE_DAILY_WAL_SHARD_PLAN_SCHEMA_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _SHARD_PLAN_FACTORY_TOKEN:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "daily WAL shard plans are factory-sealed"
            )
        object.__setattr__(
            self,
            "shard_plan_sha256",
            _hash_document(_SHARD_PLAN_DOMAIN, _shard_plan_document(self)),
        )

    @property
    def authority(self) -> WalAuthorityV2:
        """Bind WAL authority to the shard digest, not the attempt digest."""

        return WalAuthorityV2(
            attempt_id=self.attempt_id,
            protocol_sha256=self.protocol_sha256,
            plan_sha256=self.shard_plan_sha256,
            source_manifest_sha256=self.source_manifest_sha256,
            schema_sha256=self.schema_sha256,
            runtime_manifest_sha256=self.runtime_manifest_sha256,
        )


@dataclass(frozen=True, slots=True)
class ProspectiveDailyWalAppendItemV2:
    """One exact cell-targeted canonical payload requested for durable append."""

    cell: ProspectiveExpectedCellV2
    kind: ProspectiveWalRecordKindV2
    canonical_payload_jsonl: bytes = field(repr=False)
    sizing_cell: PaperSizingCellV2 | None = None
    typed_paper_terminal: ProspectivePaperTerminalPayloadV2 | None = field(
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        if type(self.cell) is not ProspectiveExpectedCellV2:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "append cell must be an exact ProspectiveExpectedCellV2"
            )
        if not isinstance(self.kind, ProspectiveWalRecordKindV2):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "append kind must be ProspectiveWalRecordKindV2"
            )
        if type(self.canonical_payload_jsonl) is not bytes:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "canonical_payload_jsonl must be immutable bytes"
            )
        _validate_sizing_cell_argument(self.kind, self.sizing_cell)
        if self.typed_paper_terminal is not None:
            if self.kind is not ProspectiveWalRecordKindV2.PAPER_TERMINAL:
                raise ProspectiveDailyWalStoreContractErrorV2(
                    "typed_paper_terminal is valid only for PAPER_TERMINAL"
                )
            if type(self.typed_paper_terminal) is not ProspectivePaperTerminalPayloadV2:
                raise ProspectiveDailyWalStoreContractErrorV2(
                    "typed_paper_terminal must be an exact factory-sealed payload"
                )


@dataclass(frozen=True, slots=True)
class ProspectiveDailyWalDurableRecordV2:
    """Identity of one exact record covered by a durable batch receipt."""

    ingest_seq: int
    cell_id: str
    kind: ProspectiveWalRecordKindV2
    sizing_cell: PaperSizingCellV2 | None
    payload_sha256: str
    record_sha256: str


@dataclass(frozen=True, slots=True)
class ProspectiveDailyWalDurableBatchReceiptV2:
    """Public proof that one bounded batch reached both selected WAL roots."""

    attempt_plan_sha256: str
    shard_plan_sha256: str
    segment_index: int
    segment_id: str
    first_ingest_seq: int
    last_ingest_seq: int
    records: tuple[ProspectiveDailyWalDurableRecordV2, ...]
    durable_prefix_proof: MirroredWalPrefixProofV2
    _factory_token: InitVar[object | None] = None
    receipt_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default=PROSPECTIVE_DAILY_WAL_BATCH_RECEIPT_SCHEMA_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _BATCH_RECEIPT_FACTORY_TOKEN:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "daily WAL durable receipts are factory-sealed"
            )
        _require_sha256(self.attempt_plan_sha256, "attempt_plan_sha256")
        _require_sha256(self.shard_plan_sha256, "shard_plan_sha256")
        _require_sha256(self.segment_id, "segment_id")
        if type(self.segment_index) is not int or self.segment_index < 0:
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "receipt segment_index must be nonnegative"
            )
        if type(self.records) is not tuple or not self.records:
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "receipt records must be a non-empty tuple"
            )
        if self.first_ingest_seq != self.records[0].ingest_seq:
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "receipt first sequence differs from its records"
            )
        if self.last_ingest_seq != self.records[-1].ingest_seq:
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "receipt last sequence differs from its records"
            )
        if tuple(item.ingest_seq for item in self.records) != tuple(
            range(self.first_ingest_seq, self.last_ingest_seq + 1)
        ):
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "receipt record sequences are not contiguous"
            )
        for item in self.records:
            if type(item) is not ProspectiveDailyWalDurableRecordV2:
                raise ProspectiveDailyWalStoreIntegrityErrorV2(
                    "receipt contains a non-exact durable record identity"
                )
            if not isinstance(item.kind, ProspectiveWalRecordKindV2):
                raise ProspectiveDailyWalStoreIntegrityErrorV2(
                    "receipt record kind must be ProspectiveWalRecordKindV2"
                )
            _validate_sizing_cell_argument(item.kind, item.sizing_cell)
            _require_sha256(item.cell_id, "receipt cell_id")
            _require_sha256(item.payload_sha256, "receipt payload_sha256")
            _require_sha256(item.record_sha256, "receipt record_sha256")
        if type(self.durable_prefix_proof) is not MirroredWalPrefixProofV2:
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "receipt requires an exact mirrored WAL prefix proof"
            )
        if self.durable_prefix_proof.durable_ack_seq < self.last_ingest_seq:
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "durable prefix does not cover the complete batch"
            )
        object.__setattr__(
            self,
            "receipt_sha256",
            _hash_document(_BATCH_RECEIPT_DOMAIN, _batch_receipt_document(self)),
        )


@dataclass(frozen=True, slots=True)
class ProspectiveDailyWalStoreFactoryV2:
    """Runtime-only factory; fault hooks and clocks never enter protocol hashes."""

    config: ProspectiveDailyWalStoreConfigV2
    receipt_clock: ReceiptClock = field(
        default_factory=SystemReceiptClock,
        repr=False,
        compare=False,
    )
    clock_ns: ClockNs = time.monotonic_ns
    primary_fault_hook: FaultHook | None = field(default=None, repr=False)
    mirror_fault_hook: FaultHook | None = field(default=None, repr=False)
    recover_torn_tail: bool = True

    def __post_init__(self) -> None:
        if type(self.config) is not ProspectiveDailyWalStoreConfigV2:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "factory config must be an exact ProspectiveDailyWalStoreConfigV2"
            )
        if not callable(getattr(self.receipt_clock, "capture", None)):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "receipt_clock must implement synchronous ReceiptClock.capture"
            )
        if not callable(self.clock_ns):
            raise ProspectiveDailyWalStoreContractErrorV2("clock_ns must be callable")
        for hook, name in (
            (self.primary_fault_hook, "primary_fault_hook"),
            (self.mirror_fault_hook, "mirror_fault_hook"),
        ):
            if hook is not None and not callable(hook):
                raise ProspectiveDailyWalStoreContractErrorV2(f"{name} must be callable or None")
        if type(self.recover_torn_tail) is not bool:
            raise ProspectiveDailyWalStoreContractErrorV2("recover_torn_tail must be boolean")

    def open(
        self,
        *,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
    ) -> ProspectiveDailyWalStoreV2:
        """Validate all immutable inputs, then consume the lease's sole claim."""

        if type(census_plan) is not ProspectiveCensusPlanV2:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "census_plan must be an exact ProspectiveCensusPlanV2"
            )
        if type(writer_lease) is not WriterLease:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "writer_lease must be an exact WriterLease"
            )
        try:
            canonical_prospective_census_plan_v2(census_plan)
        except ValueError as exc:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "census plan failed its canonical self-hash check"
            ) from exc
        if census_plan.plan_sha256 != self.config.attempt_plan_sha256:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "census plan differs from config attempt_plan_sha256"
            )
        if self.config.selection_receipt.h_start_wall_ms != (
            census_plan.attempt.horizon.h_start_ms
        ):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "selection receipt H_start differs from the census plan"
            )
        if self.config.protocol_sha256 != census_plan.execution_contract_sha256:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "config protocol_sha256 differs from the execution contract"
            )
        if self.config.source_manifest_sha256 != (census_plan.strategy_code_freeze_manifest_sha256):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "config source manifest differs from the prospective code freeze"
            )
        if self.config.runtime_manifest_sha256 != (
            self.config.selection_receipt.qualification.runtime_manifest_sha256
        ):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "config runtime manifest differs from WAL qualification"
            )
        if self.config.typed_decision_payloads_required and self.recover_torn_tail:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "typed decision store forbids implicit torn-tail recovery at open"
            )
        path_binding = _validate_store_paths(
            writer_lease=writer_lease,
            primary_base=self.config.primary_base_directory,
            mirror_base=self.config.mirror_base_directory,
        )
        with writer_lease.operation_guard():
            writer_lease.claim_prospective_attempt_authority(
                attempt_plan_sha256=census_plan.plan_sha256
            )
            (
                store_manifest,
                resumed_existing_manifest,
            ) = _load_or_create_store_manifest_guarded(
                census_plan=census_plan,
                config=self.config,
                writer_lease=writer_lease,
                path_binding=path_binding,
                receipt_clock=self.receipt_clock,
            )
        return ProspectiveDailyWalStoreV2(
            census_plan=census_plan,
            writer_lease=writer_lease,
            factory=self,
            path_binding=path_binding,
            store_manifest=store_manifest,
            resumed_existing_manifest=resumed_existing_manifest,
            _factory_token=_STORE_FACTORY_TOKEN,
        )


class _CellState(StrEnum):
    PREPARED = "PREPARED"
    DISPOSITIONED = "DISPOSITIONED"


@dataclass(frozen=True, slots=True)
class _BasePathIdentity:
    canonical_path: Path
    device: int
    inode: int


@dataclass(frozen=True, slots=True)
class _StorePathBinding:
    scope: _BasePathIdentity
    primary: _BasePathIdentity
    mirror: _BasePathIdentity
    primary_relative: str
    mirror_relative: str


@dataclass(slots=True)
class _OpenShard:
    segment: ProspectiveCensusSegmentV2
    shard_plan: ProspectiveDailyWalShardPlanV2
    writer: MirroredWalWriterV2
    expected_cell_ids: frozenset[str]
    cell_states: dict[str, _CellState]
    terminal_sizing_cells: dict[str, set[PaperSizingCellV2]]
    decision_prepares: dict[
        str,
        tuple[ProspectiveDecisionPreparePayloadV2, str],
    ]
    decision_dispositions: dict[
        str,
        tuple[ProspectiveCellDispositionPayloadV2, str],
    ]
    recovered_orphan_prepares: set[str]
    last_record: ProspectiveWalRecordV2 | None


@dataclass(frozen=True, slots=True)
class _TypedDecisionAppendV2:
    prepare: ProspectiveDecisionPreparePayloadV2 | None = None
    disposition: ProspectiveCellDispositionPayloadV2 | None = None


class ProspectiveDailyWalStoreV2:
    """Single-lease owner of at most current and previous UTC-day shards."""

    __slots__ = (
        "_active_shards",
        "_census_plan",
        "_closed",
        "_decision_transaction_claim",
        "_factory",
        "_failed",
        "_latest_segment_index",
        "_path_binding",
        "_resumed_existing_manifest",
        "_store_manifest",
        "_writer_lease",
    )

    def __init__(
        self,
        *,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
        factory: ProspectiveDailyWalStoreFactoryV2,
        path_binding: _StorePathBinding,
        store_manifest: ProspectiveDailyWalStoreManifestV2,
        resumed_existing_manifest: bool,
        _factory_token: object | None = None,
    ) -> None:
        if _factory_token is not _STORE_FACTORY_TOKEN:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "daily WAL stores must be opened by the exact factory"
            )
        if type(resumed_existing_manifest) is not bool:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "resumed_existing_manifest must be boolean"
            )
        self._census_plan = census_plan
        self._writer_lease = writer_lease
        self._factory = factory
        self._path_binding = path_binding
        self._store_manifest = store_manifest
        self._resumed_existing_manifest = resumed_existing_manifest
        self._active_shards: dict[int, _OpenShard] = {}
        self._latest_segment_index: int | None = None
        self._decision_transaction_claim: object | None = None
        self._closed = False
        self._failed: Exception | None = None

    @property
    def active_shard_count(self) -> int:
        return len(self._active_shards)

    @property
    def active_segment_indices(self) -> tuple[int, ...]:
        return tuple(sorted(self._active_shards))

    @property
    def store_manifest(self) -> ProspectiveDailyWalStoreManifestV2:
        """Return the immutable attempt-wide store manifest admitted at open."""

        return self._store_manifest

    def assert_decision_transaction_binding_v2(
        self,
        *,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
        transaction_claim: object | None = None,
    ) -> None:
        """Verify that a decision coordinator shares this exact plan and lease.

        The caller must already hold ``writer_lease.operation_guard()`` across
        the complete PREPARE/commit/DISPOSITION transaction.  Individual WAL
        appends reenter that same guard, so lease release cannot split it.
        """

        self._raise_if_unavailable()
        if type(census_plan) is not ProspectiveCensusPlanV2:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "census_plan must be exact ProspectiveCensusPlanV2"
            )
        if type(writer_lease) is not WriterLease:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "writer_lease must be an exact WriterLease"
            )
        if writer_lease is not self._writer_lease:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "decision coordinator does not share the daily store lease"
            )
        if census_plan is not self._census_plan:
            canonical_prospective_census_plan_v2(census_plan)
            if census_plan != self._census_plan:
                raise ProspectiveDailyWalStoreContractErrorV2(
                    "decision coordinator does not share the daily store plan"
                )
        existing_claim = self._decision_transaction_claim
        if existing_claim is not None and transaction_claim is not existing_claim:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "daily store belongs to a different decision transaction owner"
            )
        self._assert_lease_claim()

    def _claim_decision_transaction_owner_v2(
        self,
        *,
        census_plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
    ) -> object:
        """Internal irreversible claim used only by the owner factory."""

        self.assert_decision_transaction_binding_v2(
            census_plan=census_plan,
            writer_lease=writer_lease,
        )
        if not self._factory.config.typed_decision_payloads_required:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "decision transaction owner requires typed decision payload replay"
            )
        if self._factory.recover_torn_tail:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "decision transaction owner forbids implicit torn-tail recovery"
            )
        if self._resumed_existing_manifest:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "resumed typed store requires an authoritative state-recovery owner"
            )
        if self._decision_transaction_claim is not None:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "daily store already has a decision transaction owner"
            )
        claim = object()
        self._decision_transaction_claim = claim
        return claim

    def append_and_sync(
        self,
        *,
        cell: ProspectiveExpectedCellV2,
        kind: ProspectiveWalRecordKindV2,
        canonical_payload_jsonl: bytes,
        sizing_cell: PaperSizingCellV2 | None = None,
        typed_paper_terminal: ProspectivePaperTerminalPayloadV2 | None = None,
        transaction_claim: object | None = None,
    ) -> ProspectiveDailyWalDurableBatchReceiptV2:
        """Durably append one record; the store owns its sequence and predecessor."""

        return self.append_batch_and_sync(
            (
                ProspectiveDailyWalAppendItemV2(
                    cell=cell,
                    kind=kind,
                    canonical_payload_jsonl=canonical_payload_jsonl,
                    sizing_cell=sizing_cell,
                    typed_paper_terminal=typed_paper_terminal,
                ),
            ),
            transaction_claim=transaction_claim,
        )

    def append_batch_and_sync(
        self,
        items: tuple[ProspectiveDailyWalAppendItemV2, ...],
        *,
        transaction_claim: object | None = None,
    ) -> ProspectiveDailyWalDurableBatchReceiptV2:
        """Append one bounded same-day batch and force it durable on both roots."""

        self._raise_if_unavailable()
        with self._writer_lease.operation_guard():
            self._assert_lease_claim()
            self._assert_decision_append_claim(items, transaction_claim)
            return self._append_batch_guarded(items)

    def _assert_decision_append_claim(
        self,
        items: tuple[ProspectiveDailyWalAppendItemV2, ...],
        transaction_claim: object | None,
    ) -> None:
        if type(items) is not tuple or any(
            type(item) is not ProspectiveDailyWalAppendItemV2 for item in items
        ):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "decision append claim requires exact immutable append items"
            )
        if not self._factory.config.typed_decision_payloads_required:
            if transaction_claim is not None:
                raise ProspectiveDailyWalStoreContractErrorV2(
                    "non-typed store forbids a decision transaction claim"
                )
            return
        existing_claim = self._decision_transaction_claim
        decision_kinds = {
            ProspectiveWalRecordKindV2.DECISION_PREPARE,
            ProspectiveWalRecordKindV2.CELL_DISPOSITION,
        }
        if transaction_claim is None:
            if any(item.kind in decision_kinds for item in items):
                raise ProspectiveDailyWalStoreContractErrorV2(
                    "typed decision append lacks its exact transaction-owner claim"
                )
            return
        if (
            existing_claim is None
            or transaction_claim is not existing_claim
            or len(items) != 1
            or items[0].kind not in decision_kinds
        ):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "transaction-owner claim authorizes one exact decision record only"
            )

    def close(self) -> None:
        """Cleanly finalize every currently open shard without issuing a seal."""

        if self._closed:
            return
        self._raise_if_failed()
        with self._writer_lease.operation_guard():
            self._assert_lease_claim()
            errors: list[Exception] = []
            for index in sorted(self._active_shards):
                try:
                    self._active_shards[index].writer.close()
                except Exception as exc:
                    errors.append(exc)
            if errors:
                self._failed = errors[0]
                self._closed = True
                self._abort_all_best_effort(errors)
                raise ExceptionGroup("daily WAL close failed", errors)
            self._active_shards.clear()
            self._closed = True

    def abort(self) -> None:
        """Abort all open shard writers without converting failure into an ACK."""

        if self._closed and not self._active_shards:
            return
        with self._writer_lease.operation_guard():
            self._assert_lease_claim()
            errors: list[Exception] = []
            self._abort_all_best_effort(errors)
            self._closed = True
            if errors:
                self._failed = errors[0]
                raise ExceptionGroup("daily WAL abort failed", errors)

    def _append_batch_guarded(
        self,
        items: tuple[ProspectiveDailyWalAppendItemV2, ...],
    ) -> ProspectiveDailyWalDurableBatchReceiptV2:
        config = self._factory.config
        if type(items) is not tuple or not items:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "append batch must be a non-empty immutable tuple"
            )
        if len(items) > config.maximum_batch_records:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "append batch exceeds maximum_batch_records"
            )
        if any(type(item) is not ProspectiveDailyWalAppendItemV2 for item in items):
            raise ProspectiveDailyWalStoreContractErrorV2("append batch contains a non-exact item")
        exact_cells = tuple(self._exact_plan_cell(item.cell) for item in items)
        segment_ids = {cell.segment_id for cell in exact_cells}
        if len(segment_ids) != 1:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "one append batch cannot cross daily shards"
            )
        operation_ids = tuple(
            (cell.cell_id, item.kind, item.sizing_cell)
            for item, cell in zip(items, exact_cells, strict=True)
        )
        if len(set(operation_ids)) != len(operation_ids):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "one append batch cannot repeat the same cell/kind/sizing identity"
            )
        segment_index = self._segment_index_for_cell(exact_cells[0])
        self._assert_same_segment(exact_cells, segment_index)
        shard = self._route_shard(segment_index)

        next_ingest_seq = shard.writer.next_ingest_seq
        if next_ingest_seq != (
            1 if shard.last_record is None else shard.last_record.ingest_seq + 1
        ):
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "writer cursor differs from the replayed prospective chain"
            )
        base_states = dict(shard.cell_states)
        planned_states = dict(base_states)
        planned_terminals = {
            cell_id: set(sizing_cells)
            for cell_id, sizing_cells in shard.terminal_sizing_cells.items()
        }
        planned_decision_prepares = dict(shard.decision_prepares)
        planned_decision_dispositions = dict(shard.decision_dispositions)
        records: list[ProspectiveWalRecordV2] = []
        predecessor = shard.last_record.record_sha256 if shard.last_record else None
        for offset, (item, cell) in enumerate(zip(items, exact_cells, strict=True)):
            _validate_requested_transition(
                cell_id=cell.cell_id,
                kind=item.kind,
                states=base_states,
                recovered_orphans=shard.recovered_orphan_prepares,
                terminal_sizing_cells=planned_terminals,
                sizing_cell=item.sizing_cell,
            )
            _validate_payload_sizing_cell(
                kind=item.kind,
                canonical_payload_jsonl=item.canonical_payload_jsonl,
                sizing_cell=item.sizing_cell,
            )
            typed_decision = self._validate_typed_decision_append(
                item=item,
                cell=cell,
                decision_prepares=planned_decision_prepares,
            )
            self._validate_typed_paper_terminal_append(
                item=item,
                cell=cell,
                decision_prepares=planned_decision_prepares,
                decision_dispositions=planned_decision_dispositions,
            )
            record = build_prospective_wal_record_v2(
                ingest_seq=next_ingest_seq + offset,
                kind=item.kind,
                attempt_plan_sha256=self._census_plan.plan_sha256,
                segment_id=shard.segment.segment_id,
                cell_id=cell.cell_id,
                payload_schema=_PAYLOAD_SCHEMA_BY_KIND[item.kind],
                canonical_payload_jsonl=item.canonical_payload_jsonl,
                previous_record_sha256=predecessor,
            )
            records.append(record)
            if typed_decision is not None and typed_decision.prepare is not None:
                planned_decision_prepares[cell.cell_id] = (
                    typed_decision.prepare,
                    record.record_sha256,
                )
            if typed_decision is not None and typed_decision.disposition is not None:
                planned_decision_dispositions[cell.cell_id] = (
                    typed_decision.disposition,
                    record.record_sha256,
                )
            predecessor = record.record_sha256
            _apply_transition(
                states=planned_states,
                terminal_sizing_cells=planned_terminals,
                cell_id=cell.cell_id,
                kind=item.kind,
                sizing_cell=item.sizing_cell,
            )
        if sum(record.encoded_len for record in records) > config.policy.max_unsynced_bytes:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "encoded batch exceeds the selected WAL byte bound"
            )

        try:
            result = shard.writer.append_batch(records)
            durable_ack_seq = shard.writer.sync()
            if (
                result.first_ingest_seq != records[0].ingest_seq
                or result.last_ingest_seq != records[-1].ingest_seq
                or result.record_count != len(records)
                or durable_ack_seq != records[-1].ingest_seq
            ):
                raise ProspectiveDailyWalStoreIntegrityErrorV2(
                    "mirrored WAL acknowledgement differs from the exact batch"
                )
            prefix_proof = shard.writer.prove_durable_prefix_v2()
            if prefix_proof.durable_ack_seq != durable_ack_seq:
                raise ProspectiveDailyWalStoreIntegrityErrorV2(
                    "mirrored prefix proof differs from the forced durable ACK"
                )
            receipt = ProspectiveDailyWalDurableBatchReceiptV2(
                attempt_plan_sha256=self._census_plan.plan_sha256,
                shard_plan_sha256=shard.shard_plan.shard_plan_sha256,
                segment_index=segment_index,
                segment_id=shard.segment.segment_id,
                first_ingest_seq=records[0].ingest_seq,
                last_ingest_seq=records[-1].ingest_seq,
                records=tuple(
                    ProspectiveDailyWalDurableRecordV2(
                        ingest_seq=record.ingest_seq,
                        cell_id=record.cell_id,
                        kind=record.kind,
                        sizing_cell=item.sizing_cell,
                        payload_sha256=record.payload_sha256,
                        record_sha256=record.record_sha256,
                    )
                    for record, item in zip(records, items, strict=True)
                ),
                durable_prefix_proof=prefix_proof,
                _factory_token=_BATCH_RECEIPT_FACTORY_TOKEN,
            )
        except Exception as exc:
            self._failed = exc
            raise

        shard.cell_states = planned_states
        shard.terminal_sizing_cells = planned_terminals
        shard.decision_prepares = planned_decision_prepares
        shard.decision_dispositions = planned_decision_dispositions
        shard.last_record = records[-1]
        return receipt

    def _validate_typed_decision_append(
        self,
        *,
        item: ProspectiveDailyWalAppendItemV2,
        cell: ProspectiveExpectedCellV2,
        decision_prepares: dict[
            str,
            tuple[ProspectiveDecisionPreparePayloadV2, str],
        ],
    ) -> _TypedDecisionAppendV2 | None:
        if not self._factory.config.typed_decision_payloads_required:
            return None
        try:
            if item.kind is ProspectiveWalRecordKindV2.DECISION_PREPARE:
                return _TypedDecisionAppendV2(
                    prepare=parse_prospective_decision_prepare_payload_v2(
                        item.canonical_payload_jsonl,
                        plan=self._census_plan,
                        cell=cell,
                    )
                )
            if item.kind is ProspectiveWalRecordKindV2.CELL_DISPOSITION:
                binding = decision_prepares.get(cell.cell_id)
                if binding is None:
                    raise ProspectiveDailyWalStoreContractErrorV2(
                        "typed DISPOSITION has no exact durable PREPARE binding"
                    )
                prepare, prepare_record_sha256 = binding
                return _TypedDecisionAppendV2(
                    disposition=parse_prospective_cell_disposition_payload_v2(
                        item.canonical_payload_jsonl,
                        prepare=prepare,
                        prepare_record_sha256=prepare_record_sha256,
                    )
                )
        except ValueError as exc:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "decision payload fails exact typed admission"
            ) from exc
        return None

    def _validate_typed_paper_terminal_append(
        self,
        *,
        item: ProspectiveDailyWalAppendItemV2,
        cell: ProspectiveExpectedCellV2,
        decision_prepares: dict[
            str,
            tuple[ProspectiveDecisionPreparePayloadV2, str],
        ],
        decision_dispositions: dict[
            str,
            tuple[ProspectiveCellDispositionPayloadV2, str],
        ],
    ) -> None:
        payload = item.typed_paper_terminal
        if item.kind is not ProspectiveWalRecordKindV2.PAPER_TERMINAL:
            if payload is not None:
                raise ProspectiveDailyWalStoreContractErrorV2(
                    "non-terminal append cannot carry a typed PAPER terminal"
                )
            return
        if not self._factory.config.typed_decision_payloads_required:
            if payload is not None:
                raise ProspectiveDailyWalStoreContractErrorV2(
                    "non-typed store cannot claim typed PAPER terminal admission"
                )
            return
        if type(payload) is not ProspectivePaperTerminalPayloadV2:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "typed PAPER_TERMINAL requires its exact factory-sealed payload"
            )
        if item.sizing_cell is None:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "typed PAPER_TERMINAL requires an exact sizing cell"
            )
        prepare_binding = decision_prepares.get(cell.cell_id)
        disposition_binding = decision_dispositions.get(cell.cell_id)
        if prepare_binding is None or disposition_binding is None:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "typed PAPER_TERMINAL lacks its exact durable decision transition"
            )
        prepare, prepare_record_sha256 = prepare_binding
        disposition, disposition_record_sha256 = disposition_binding
        try:
            terminal_jsonl = canonical_prospective_paper_terminal_payload_v2(payload)
            prepare_jsonl = canonical_prospective_decision_prepare_payload_v2(prepare)
            disposition_jsonl = canonical_prospective_cell_disposition_payload_v2(disposition)
        except (TypeError, ValueError) as exc:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "typed PAPER_TERMINAL fails canonical source validation"
            ) from exc
        if terminal_jsonl != item.canonical_payload_jsonl:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "typed PAPER_TERMINAL bytes differ from the sealed payload"
            )
        if (
            payload.attempt_id,
            payload.attempt_plan_sha256,
            payload.promoting_plan_sha256,
            payload.execution_contract_sha256,
            payload.segment_id,
            payload.cell_id,
            payload.family,
            payload.family_rule_version,
            payload.symbol,
            payload.venue,
            payload.bar_open_ms,
            payload.bar_close_ms,
            payload.decision_cutoff_ms,
            payload.sizing_cell,
        ) != (
            cell.attempt_id,
            self._census_plan.plan_sha256,
            self._census_plan.promoting_plan_sha256,
            self._census_plan.execution_contract_sha256,
            cell.segment_id,
            cell.cell_id,
            cell.family,
            cell.rule_version,
            cell.symbol,
            prepare.venue,
            cell.bar_open_ms,
            cell.bar_close_ms,
            cell.decision_cutoff_ms,
            item.sizing_cell,
        ):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "typed PAPER_TERMINAL targets a foreign plan, cell, or sizing cell"
            )
        if (
            payload.prepare_payload_sha256,
            payload.prepare_wal_payload_sha256,
            payload.prepare_record_sha256,
            payload.disposition_payload_sha256,
            payload.disposition_wal_payload_sha256,
            payload.disposition_record_sha256,
            payload.decision_event_id,
            payload.decision_payload_sha256,
            payload.disposition_class,
            payload.signal_side,
        ) != (
            prepare.payload_sha256,
            hashlib.sha256(prepare_jsonl).hexdigest(),
            prepare_record_sha256,
            disposition.payload_sha256,
            hashlib.sha256(disposition_jsonl).hexdigest(),
            disposition_record_sha256,
            prepare.decision_event_id,
            prepare.decision_payload_sha256,
            disposition.disposition_class,
            disposition.signal_side,
        ):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "typed PAPER_TERMINAL differs from its durable PREPARE/DISPOSITION identities"
            )
        allowed_statuses = _PAPER_TERMINAL_STATUSES_BY_DISPOSITION_V2.get(
            disposition.disposition_class
        )
        if allowed_statuses is None or payload.terminal_status not in allowed_statuses:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "typed PAPER_TERMINAL status is incompatible with its disposition"
            )

    def _route_shard(self, segment_index: int) -> _OpenShard:
        latest = self._latest_segment_index
        if latest is not None and segment_index < latest - 1:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "record targets a shard older than the previous UTC day"
            )
        proposed_latest = segment_index if latest is None else max(latest, segment_index)
        if latest is not None and proposed_latest > latest:
            self._evict_before(proposed_latest - 1)
        existing = self._active_shards.get(segment_index)
        if existing is not None:
            self._latest_segment_index = proposed_latest
            return existing
        if len(self._active_shards) >= MAX_ACTIVE_PROSPECTIVE_DAILY_WAL_SHARDS_V2:
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "daily shard owner exceeded its fixed two-shard bound"
            )
        try:
            shard = self._open_shard(segment_index)
        except Exception as exc:
            self._failed = exc
            raise
        self._active_shards[segment_index] = shard
        self._latest_segment_index = proposed_latest
        return shard

    def _evict_before(self, minimum_index: int) -> None:
        for index in sorted(tuple(self._active_shards)):
            if index >= minimum_index:
                continue
            shard = self._active_shards[index]
            try:
                shard.writer.close()
            except Exception as exc:
                self._failed = exc
                raise
            del self._active_shards[index]

    def _open_shard(self, segment_index: int) -> _OpenShard:
        _assert_path_binding_current(self._path_binding)
        segment = _segment_at_index(self._census_plan, segment_index)
        shard_plan = build_prospective_daily_wal_shard_plan_v2(
            census_plan=self._census_plan,
            segment=segment,
            segment_index=segment_index,
            config=self._factory.config,
            scope_directory=self._path_binding.scope.canonical_path,
            store_manifest_sha256=self._store_manifest.manifest_sha256,
        )
        scope = self._path_binding.scope.canonical_path
        primary_directory = scope / Path(shard_plan.primary_directory_relative_to_scope)
        mirror_directory = scope / Path(shard_plan.mirror_directory_relative_to_scope)
        config = self._factory.config
        writer = MirroredWalWriterV2(
            primary_directory,
            mirror_directory,
            authority=shard_plan.authority,
            policy=config.policy,
            selection_receipt=config.selection_receipt,
            primary_maximum_total_bytes=shard_plan.primary_maximum_total_bytes,
            mirror_maximum_total_bytes=shard_plan.mirror_maximum_total_bytes,
            primary_emergency_reserve_bytes=(shard_plan.primary_emergency_reserve_bytes),
            mirror_emergency_reserve_bytes=(shard_plan.mirror_emergency_reserve_bytes),
            primary_failure_domain_id=shard_plan.primary_failure_domain_id,
            mirror_failure_domain_id=shard_plan.mirror_failure_domain_id,
            clock_ns=self._factory.clock_ns,
            primary_fault_hook=self._factory.primary_fault_hook,
            mirror_fault_hook=self._factory.mirror_fault_hook,
            recover_torn_tail=self._factory.recover_torn_tail,
        )
        expected_cell_ids = frozenset(
            cell.cell_id for cell in self._census_plan.iter_expected_cells_for_segment(segment)
        )
        expected_cells_by_id = {
            cell.cell_id: cell
            for cell in self._census_plan.iter_expected_cells_for_segment(segment)
        }
        if len(expected_cell_ids) != segment.expected_cell_count:
            writer.abort()
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "daily expected-cell identities are not complete and unique"
            )
        states: dict[str, _CellState] = {}
        terminal_sizing_cells: dict[str, set[PaperSizingCellV2]] = {}
        decision_prepares: dict[
            str,
            tuple[ProspectiveDecisionPreparePayloadV2, str],
        ] = {}
        decision_dispositions: dict[
            str,
            tuple[ProspectiveCellDispositionPayloadV2, str],
        ] = {}
        last_record: ProspectiveWalRecordV2 | None = None

        def consume(ingest_seq: int, encoded_line: bytes) -> None:
            nonlocal last_record
            record = parse_prospective_wal_record_v2(encoded_line)
            if record.ingest_seq != ingest_seq:
                raise ProspectiveDailyWalStoreIntegrityErrorV2(
                    "replayed callback sequence differs from the strict record"
                )
            if record.attempt_plan_sha256 != self._census_plan.plan_sha256:
                raise ProspectiveDailyWalStoreIntegrityErrorV2(
                    "replayed record targets a foreign attempt plan"
                )
            if record.segment_id != segment.segment_id:
                raise ProspectiveDailyWalStoreIntegrityErrorV2(
                    "replayed record targets a foreign daily shard"
                )
            if record.cell_id not in expected_cell_ids:
                raise ProspectiveDailyWalStoreIntegrityErrorV2(
                    "replayed record target is outside the daily census shard"
                )
            sizing_cell = _sizing_cell_from_payload(
                kind=record.kind,
                payload=record.payload,
            )
            if last_record is None:
                if record.ingest_seq != 1 or record.previous_record_sha256 is not None:
                    raise ProspectiveDailyWalStoreIntegrityErrorV2(
                        "replayed daily chain has no exact genesis"
                    )
            else:
                verify_prospective_wal_successor_v2(last_record, record)
            if config.typed_decision_payloads_required:
                try:
                    if record.kind is ProspectiveWalRecordKindV2.DECISION_PREPARE:
                        prepare = parse_prospective_decision_prepare_payload_v2(
                            record.payload_jsonl,
                            plan=self._census_plan,
                            cell=expected_cells_by_id[record.cell_id],
                        )
                        decision_prepares[record.cell_id] = (
                            prepare,
                            record.record_sha256,
                        )
                    elif record.kind is ProspectiveWalRecordKindV2.CELL_DISPOSITION:
                        binding = decision_prepares.get(record.cell_id)
                        if binding is None:
                            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                                "typed replay DISPOSITION has no exact PREPARE"
                            )
                        prepare, prepare_record_sha256 = binding
                        disposition = parse_prospective_cell_disposition_payload_v2(
                            record.payload_jsonl,
                            prepare=prepare,
                            prepare_record_sha256=prepare_record_sha256,
                        )
                        decision_dispositions[record.cell_id] = (
                            disposition,
                            record.record_sha256,
                        )
                    elif record.kind is ProspectiveWalRecordKindV2.PAPER_TERMINAL:
                        raise ProspectiveDailyWalStoreIntegrityErrorV2(
                            "typed PAPER_TERMINAL replay cannot reconstruct its live "
                            "factory-sealed transaction and fails closed"
                        )
                except ValueError as exc:
                    raise ProspectiveDailyWalStoreIntegrityErrorV2(
                        "replayed decision payload fails exact typed validation"
                    ) from exc
            _validate_replay_transition(
                states=states,
                terminal_sizing_cells=terminal_sizing_cells,
                cell_id=record.cell_id,
                kind=record.kind,
                sizing_cell=sizing_cell,
            )
            _apply_transition(
                states=states,
                terminal_sizing_cells=terminal_sizing_cells,
                cell_id=record.cell_id,
                kind=record.kind,
                sizing_cell=sizing_cell,
            )
            last_record = record

        try:
            delivered = writer.consume_durable_records(consume)
            if delivered != writer.durable_ack_seq:
                raise ProspectiveDailyWalStoreIntegrityErrorV2(
                    "replayed record count differs from durable ACK"
                )
            if delivered != (0 if last_record is None else last_record.ingest_seq):
                raise ProspectiveDailyWalStoreIntegrityErrorV2(
                    "replayed daily tail differs from its contiguous chain"
                )
        except Exception as replay_error:
            try:
                writer.abort()
            except Exception as abort_error:
                raise ExceptionGroup(
                    "daily WAL replay and cleanup failed",
                    [replay_error, abort_error],
                ) from None
            raise
        recovered_orphans = {
            cell_id for cell_id, state in states.items() if state is _CellState.PREPARED
        }
        return _OpenShard(
            segment=segment,
            shard_plan=shard_plan,
            writer=writer,
            expected_cell_ids=expected_cell_ids,
            cell_states=states,
            terminal_sizing_cells=terminal_sizing_cells,
            decision_prepares=decision_prepares,
            decision_dispositions=decision_dispositions,
            recovered_orphan_prepares=recovered_orphans,
            last_record=last_record,
        )

    def _exact_plan_cell(
        self,
        cell: ProspectiveExpectedCellV2,
    ) -> ProspectiveExpectedCellV2:
        if type(cell) is not ProspectiveExpectedCellV2:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "append cell must be an exact ProspectiveExpectedCellV2"
            )
        if cell.attempt_plan_sha256 != self._census_plan.plan_sha256:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "record cell targets a foreign attempt plan"
            )
        try:
            expected = self._census_plan.expected_cell(
                family=cell.family,
                symbol=cell.symbol,
                bar_open_ms=cell.bar_open_ms,
            )
        except ValueError as exc:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "record cell is outside the frozen prospective census"
            ) from exc
        if cell != expected:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "record cell differs from its exact frozen census identity"
            )
        return expected

    def _segment_index_for_cell(self, cell: ProspectiveExpectedCellV2) -> int:
        first_day = next(self._census_plan.iter_segments()).day_start_ms
        index = (cell.bar_open_ms - first_day) // MILLISECONDS_PER_DAY_V2
        segment = _segment_at_index(self._census_plan, index)
        if cell.segment_id != segment.segment_id:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "record cell segment ID differs from its daily shard index"
            )
        return index

    def _assert_same_segment(
        self,
        cells: tuple[ProspectiveExpectedCellV2, ...],
        segment_index: int,
    ) -> None:
        segment = _segment_at_index(self._census_plan, segment_index)
        if any(cell.segment_id != segment.segment_id for cell in cells):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "record batch contains a target outside its daily shard"
            )

    def _assert_lease_claim(self) -> None:
        self._writer_lease.assert_prospective_attempt_authority_claim(
            attempt_plan_sha256=self._census_plan.plan_sha256
        )

    def _raise_if_failed(self) -> None:
        if self._failed is not None:
            raise ProspectiveDailyWalStoreFailedErrorV2(
                "daily WAL store failed and must be aborted"
            ) from self._failed

    def _raise_if_unavailable(self) -> None:
        self._raise_if_failed()
        if self._closed:
            raise ProspectiveDailyWalStoreFailedErrorV2("daily WAL store is closed")

    def _abort_all_best_effort(self, errors: list[Exception]) -> None:
        for shard in tuple(self._active_shards.values()):
            try:
                shard.writer.abort()
            except Exception as exc:
                errors.append(exc)
        self._active_shards.clear()


def build_prospective_daily_wal_shard_plan_v2(
    *,
    census_plan: ProspectiveCensusPlanV2,
    segment: ProspectiveCensusSegmentV2,
    segment_index: int,
    config: ProspectiveDailyWalStoreConfigV2,
    scope_directory: Path,
    store_manifest_sha256: str,
) -> ProspectiveDailyWalShardPlanV2:
    """Build the sole shard authority after exact plan, index, and path checks."""

    if type(census_plan) is not ProspectiveCensusPlanV2:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "census_plan must be an exact ProspectiveCensusPlanV2"
        )
    if type(segment) is not ProspectiveCensusSegmentV2:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "segment must be an exact ProspectiveCensusSegmentV2"
        )
    if type(config) is not ProspectiveDailyWalStoreConfigV2:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "config must be an exact ProspectiveDailyWalStoreConfigV2"
        )
    _require_sha256(store_manifest_sha256, "store_manifest_sha256")
    try:
        canonical_prospective_census_plan_v2(census_plan)
    except ValueError as exc:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "census plan failed its canonical self-hash check"
        ) from exc
    if census_plan.plan_sha256 != config.attempt_plan_sha256:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "config attempt plan differs from the census plan"
        )
    if config.protocol_sha256 != census_plan.execution_contract_sha256:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "config protocol differs from the execution contract"
        )
    if config.source_manifest_sha256 != (census_plan.strategy_code_freeze_manifest_sha256):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "config source manifest differs from the prospective code freeze"
        )
    if config.runtime_manifest_sha256 != (
        config.selection_receipt.qualification.runtime_manifest_sha256
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "config runtime manifest differs from WAL qualification"
        )
    expected_segment = _segment_at_index(census_plan, segment_index)
    if segment != expected_segment:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "segment ID or index differs from the frozen census"
        )
    if not isinstance(scope_directory, Path) or not scope_directory.is_absolute():
        raise ProspectiveDailyWalStoreContractErrorV2(
            "scope_directory must be an absolute pathlib.Path"
        )
    primary_base = _canonical_path(config.primary_base_directory)
    mirror_base = _canonical_path(config.mirror_base_directory)
    scope = _canonical_path(scope_directory)
    try:
        primary_relative_base = primary_base.relative_to(scope)
        mirror_relative_base = mirror_base.relative_to(scope)
    except ValueError as exc:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "daily WAL bases must be under the writer-lease scope"
        ) from exc
    shard_name = f"segment-{segment_index:03d}-{segment.segment_id}"
    primary_relative = (primary_relative_base / shard_name).as_posix()
    mirror_relative = (mirror_relative_base / shard_name).as_posix()
    return ProspectiveDailyWalShardPlanV2(
        attempt_id=census_plan.attempt_id,
        attempt_plan_sha256=census_plan.plan_sha256,
        store_manifest_sha256=store_manifest_sha256,
        segment_index=segment_index,
        segment_id=segment.segment_id,
        day_start_ms=segment.day_start_ms,
        first_bar_open_ms=segment.first_bar_open_ms,
        bar_open_stop_exclusive_ms=segment.bar_open_stop_exclusive_ms,
        expected_cell_count=segment.expected_cell_count,
        primary_directory_relative_to_scope=primary_relative,
        mirror_directory_relative_to_scope=mirror_relative,
        selection_receipt_sha256=config.selection_receipt.sha256,
        policy_sha256=_policy_sha256(config.policy),
        protocol_sha256=config.protocol_sha256,
        source_manifest_sha256=config.source_manifest_sha256,
        schema_sha256=config.schema_sha256,
        runtime_manifest_sha256=config.runtime_manifest_sha256,
        primary_maximum_total_bytes=(config.primary_maximum_total_bytes_per_shard),
        mirror_maximum_total_bytes=config.mirror_maximum_total_bytes_per_shard,
        primary_emergency_reserve_bytes=(config.primary_emergency_reserve_bytes_per_shard),
        mirror_emergency_reserve_bytes=(config.mirror_emergency_reserve_bytes_per_shard),
        primary_failure_domain_id=config.primary_failure_domain_id,
        mirror_failure_domain_id=config.mirror_failure_domain_id,
        maximum_batch_records=config.maximum_batch_records,
        typed_decision_payloads_required=(config.typed_decision_payloads_required),
        _factory_token=_SHARD_PLAN_FACTORY_TOKEN,
    )


def canonical_prospective_daily_wal_shard_plan_v2(
    plan: ProspectiveDailyWalShardPlanV2,
) -> bytes:
    """Return the exact self-hash-checked shard authority document."""

    if type(plan) is not ProspectiveDailyWalShardPlanV2:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "plan must be an exact ProspectiveDailyWalShardPlanV2"
        )
    document = _shard_plan_document(plan)
    if plan.shard_plan_sha256 != _hash_document(_SHARD_PLAN_DOMAIN, document):
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "daily WAL shard plan hash differs from canonical content"
        )
    return canonical_json_line({**document, "shard_plan_sha256": plan.shard_plan_sha256})


def canonical_prospective_daily_wal_store_manifest_v2(
    manifest: ProspectiveDailyWalStoreManifestV2,
) -> bytes:
    """Return exact canonical bytes after rechecking the manifest self-hash."""

    if type(manifest) is not ProspectiveDailyWalStoreManifestV2:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "manifest must be an exact ProspectiveDailyWalStoreManifestV2"
        )
    document = _store_manifest_document(manifest)
    expected = _hash_document(_STORE_MANIFEST_DOMAIN, document)
    if manifest.manifest_sha256 != expected:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "daily WAL store manifest hash differs from canonical content"
        )
    return canonical_json_line({**document, "manifest_sha256": manifest.manifest_sha256})


def parse_prospective_daily_wal_store_manifest_v2(
    encoded_line: bytes,
) -> ProspectiveDailyWalStoreManifestV2:
    """Strictly parse only one canonical self-hash-checked manifest record."""

    if type(encoded_line) is not bytes:
        raise ProspectiveDailyWalStoreIntegrityErrorV2("store manifest bytes must be immutable")
    if not 1 <= len(encoded_line) <= MAX_PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_BYTES_V2:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "store manifest byte length is outside its fixed bound"
        )
    if not encoded_line.endswith(b"\n") or encoded_line.count(b"\n") != 1:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "store manifest must be exactly one JSONL record"
        )
    try:
        decoded: object = json.loads(encoded_line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "store manifest is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(decoded, dict):
        raise ProspectiveDailyWalStoreIntegrityErrorV2("store manifest must decode to one object")
    expected_keys = frozenset(
        {
            "attempt_id",
            "census_plan_sha256",
            "h_start_ms",
            "manifest_sha256",
            "maximum_batch_records",
            "mirror_base_relative_to_scope",
            "mirror_emergency_reserve_bytes_per_shard",
            "mirror_failure_domain_id",
            "mirror_maximum_total_bytes_per_shard",
            "policy_sha256",
            "primary_base_device_decimal",
            "primary_base_inode_decimal",
            "primary_base_relative_to_scope",
            "primary_emergency_reserve_bytes_per_shard",
            "primary_failure_domain_id",
            "primary_maximum_total_bytes_per_shard",
            "protocol_sha256",
            "receipt_monotonic_ns",
            "receipt_wall_ms",
            "runtime_manifest_sha256",
            "schema_sha256",
            "schema_version",
            "scope_canonical_path_sha256",
            "scope_device_decimal",
            "scope_inode_decimal",
            "selection_receipt_sha256",
            "source_manifest_sha256",
            "typed_decision_payloads_required",
            "mirror_base_device_decimal",
            "mirror_base_inode_decimal",
        }
    )
    if frozenset(decoded) != expected_keys:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "store manifest has missing or unknown fields"
        )
    document = decoded
    if document.get("schema_version") != PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_SCHEMA_V2:
        raise ProspectiveDailyWalStoreIntegrityErrorV2("store manifest schema is unsupported")
    try:
        manifest = ProspectiveDailyWalStoreManifestV2(
            attempt_id=_manifest_text(document, "attempt_id"),
            census_plan_sha256=_manifest_text(document, "census_plan_sha256"),
            scope_canonical_path_sha256=_manifest_text(
                document,
                "scope_canonical_path_sha256",
            ),
            scope_device_decimal=_manifest_text(
                document,
                "scope_device_decimal",
            ),
            scope_inode_decimal=_manifest_text(
                document,
                "scope_inode_decimal",
            ),
            primary_base_device_decimal=_manifest_text(
                document,
                "primary_base_device_decimal",
            ),
            primary_base_inode_decimal=_manifest_text(
                document,
                "primary_base_inode_decimal",
            ),
            mirror_base_device_decimal=_manifest_text(
                document,
                "mirror_base_device_decimal",
            ),
            mirror_base_inode_decimal=_manifest_text(
                document,
                "mirror_base_inode_decimal",
            ),
            primary_base_relative_to_scope=_manifest_text(
                document,
                "primary_base_relative_to_scope",
            ),
            mirror_base_relative_to_scope=_manifest_text(
                document,
                "mirror_base_relative_to_scope",
            ),
            selection_receipt_sha256=_manifest_text(
                document,
                "selection_receipt_sha256",
            ),
            policy_sha256=_manifest_text(document, "policy_sha256"),
            protocol_sha256=_manifest_text(document, "protocol_sha256"),
            source_manifest_sha256=_manifest_text(
                document,
                "source_manifest_sha256",
            ),
            schema_sha256=_manifest_text(document, "schema_sha256"),
            runtime_manifest_sha256=_manifest_text(
                document,
                "runtime_manifest_sha256",
            ),
            primary_maximum_total_bytes_per_shard=_manifest_int(
                document,
                "primary_maximum_total_bytes_per_shard",
            ),
            mirror_maximum_total_bytes_per_shard=_manifest_int(
                document,
                "mirror_maximum_total_bytes_per_shard",
            ),
            primary_emergency_reserve_bytes_per_shard=_manifest_int(
                document,
                "primary_emergency_reserve_bytes_per_shard",
            ),
            mirror_emergency_reserve_bytes_per_shard=_manifest_int(
                document,
                "mirror_emergency_reserve_bytes_per_shard",
            ),
            primary_failure_domain_id=_manifest_text(
                document,
                "primary_failure_domain_id",
            ),
            mirror_failure_domain_id=_manifest_text(
                document,
                "mirror_failure_domain_id",
            ),
            maximum_batch_records=_manifest_int(
                document,
                "maximum_batch_records",
            ),
            typed_decision_payloads_required=_manifest_bool(
                document,
                "typed_decision_payloads_required",
            ),
            h_start_ms=_manifest_int(document, "h_start_ms"),
            receipt_wall_ms=_manifest_int(document, "receipt_wall_ms"),
            receipt_monotonic_ns=_manifest_int(
                document,
                "receipt_monotonic_ns",
            ),
            _factory_token=_STORE_MANIFEST_FACTORY_TOKEN,
        )
    except ProspectiveDailyWalStoreContractErrorV2 as exc:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "store manifest fields violate the frozen contract"
        ) from exc
    stored_sha256 = _manifest_text(document, "manifest_sha256")
    if manifest.manifest_sha256 != stored_sha256:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "stored manifest SHA-256 differs from rebuilt content"
        )
    if canonical_prospective_daily_wal_store_manifest_v2(manifest) != encoded_line:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "store manifest bytes are not exact canonical JSONL"
        )
    return manifest


def _validate_sizing_cell_argument(
    kind: ProspectiveWalRecordKindV2,
    sizing_cell: PaperSizingCellV2 | None,
) -> None:
    if kind is ProspectiveWalRecordKindV2.PAPER_TERMINAL:
        if not isinstance(sizing_cell, PaperSizingCellV2):
            raise ProspectiveDailyWalStoreContractErrorV2(
                "PAPER_TERMINAL requires an exact PaperSizingCellV2 identity"
            )
        return
    if sizing_cell is not None:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "non-terminal WAL records forbid a sizing-cell identity"
        )


def _validate_payload_sizing_cell(
    *,
    kind: ProspectiveWalRecordKindV2,
    canonical_payload_jsonl: bytes,
    sizing_cell: PaperSizingCellV2 | None,
) -> None:
    try:
        decoded: object = json.loads(canonical_payload_jsonl)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "prospective WAL payload is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(decoded, dict):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "prospective WAL payload must be a JSON object"
        )
    observed = _sizing_cell_from_payload(kind=kind, payload=decoded)
    if observed is not sizing_cell:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "typed sizing cell differs from the canonical PAPER_TERMINAL payload"
        )


def _sizing_cell_from_payload(
    *,
    kind: ProspectiveWalRecordKindV2,
    payload: dict[str, object],
) -> PaperSizingCellV2 | None:
    value = payload.get("sizing_cell")
    if kind is not ProspectiveWalRecordKindV2.PAPER_TERMINAL:
        if "sizing_cell" in payload:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "non-terminal prospective payload forbids sizing_cell"
            )
        return None
    if not isinstance(value, str):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "PAPER_TERMINAL payload requires a textual sizing_cell"
        )
    try:
        return PaperSizingCellV2(value)
    except ValueError as exc:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "PAPER_TERMINAL payload has an unsupported sizing_cell"
        ) from exc


def _validate_requested_transition(
    *,
    cell_id: str,
    kind: ProspectiveWalRecordKindV2,
    states: dict[str, _CellState],
    recovered_orphans: set[str],
    terminal_sizing_cells: dict[str, set[PaperSizingCellV2]],
    sizing_cell: PaperSizingCellV2 | None,
) -> None:
    state = states.get(cell_id)
    if kind is ProspectiveWalRecordKindV2.DECISION_PREPARE:
        if state is not None:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "DECISION_PREPARE requires a fresh census cell"
            )
        return
    if kind is ProspectiveWalRecordKindV2.CELL_DISPOSITION:
        if cell_id in recovered_orphans:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "recovered orphan PREPARE permanently blocks later disposition"
            )
        if state is not _CellState.PREPARED:
            raise ProspectiveDailyWalStoreContractErrorV2(
                "CELL_DISPOSITION requires a previously durable PREPARE"
            )
        return
    if state is not _CellState.DISPOSITIONED:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "PAPER_TERMINAL requires a previously durable disposition"
        )
    assert sizing_cell is not None
    if sizing_cell in terminal_sizing_cells.get(cell_id, set()):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "duplicate PAPER_TERMINAL sizing cell is forbidden"
        )


def _validate_replay_transition(
    *,
    states: dict[str, _CellState],
    terminal_sizing_cells: dict[str, set[PaperSizingCellV2]],
    cell_id: str,
    kind: ProspectiveWalRecordKindV2,
    sizing_cell: PaperSizingCellV2 | None,
) -> None:
    state = states.get(cell_id)
    if kind is ProspectiveWalRecordKindV2.DECISION_PREPARE and state is None:
        return
    if kind is ProspectiveWalRecordKindV2.CELL_DISPOSITION and state is _CellState.PREPARED:
        return
    if (
        kind is ProspectiveWalRecordKindV2.PAPER_TERMINAL
        and state is _CellState.DISPOSITIONED
        and sizing_cell is not None
        and sizing_cell not in terminal_sizing_cells.get(cell_id, set())
    ):
        return
    raise ProspectiveDailyWalStoreIntegrityErrorV2(
        "replayed prospective records violate PREPARE/disposition/terminal order"
    )


def _apply_transition(
    *,
    states: dict[str, _CellState],
    terminal_sizing_cells: dict[str, set[PaperSizingCellV2]],
    cell_id: str,
    kind: ProspectiveWalRecordKindV2,
    sizing_cell: PaperSizingCellV2 | None,
) -> None:
    if kind is ProspectiveWalRecordKindV2.DECISION_PREPARE:
        states[cell_id] = _CellState.PREPARED
    elif kind is ProspectiveWalRecordKindV2.CELL_DISPOSITION:
        states[cell_id] = _CellState.DISPOSITIONED
    else:
        assert sizing_cell is not None
        terminal_sizing_cells.setdefault(cell_id, set()).add(sizing_cell)


def _segment_at_index(
    census_plan: ProspectiveCensusPlanV2,
    segment_index: int,
) -> ProspectiveCensusSegmentV2:
    if type(segment_index) is not int or not 0 <= segment_index < census_plan.segment_count:
        raise ProspectiveDailyWalStoreContractErrorV2("segment_index is outside the frozen census")
    first_day = next(census_plan.iter_segments()).day_start_ms
    return census_plan.segment_for_day(first_day + segment_index * MILLISECONDS_PER_DAY_V2)


def _load_or_create_store_manifest_guarded(
    *,
    census_plan: ProspectiveCensusPlanV2,
    config: ProspectiveDailyWalStoreConfigV2,
    writer_lease: WriterLease,
    path_binding: _StorePathBinding,
    receipt_clock: ReceiptClock,
) -> tuple[ProspectiveDailyWalStoreManifestV2, bool]:
    writer_lease.assert_prospective_attempt_authority_claim(
        attempt_plan_sha256=census_plan.plan_sha256
    )
    _assert_path_binding_current(path_binding)
    final_path = path_binding.scope.canonical_path / PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_FILE_V2
    partial_path = final_path.with_name(final_path.name + ".partial")
    final_exists = _manifest_artifact_exists(final_path, "store manifest")
    partial_exists = _manifest_artifact_exists(
        partial_path,
        "store manifest partial",
    )
    if final_exists and partial_exists:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "store manifest has both final and partial artifacts"
        )
    if final_exists:
        manifest = _read_store_manifest_file(final_path, "store manifest")
        _assert_store_manifest_matches(
            manifest,
            census_plan=census_plan,
            config=config,
            path_binding=path_binding,
        )
        return manifest, True
    if partial_exists:
        manifest = _read_store_manifest_file(
            partial_path,
            "store manifest partial",
        )
        _assert_store_manifest_matches(
            manifest,
            census_plan=census_plan,
            config=config,
            path_binding=path_binding,
        )
        _fsync_file_path(partial_path)
        os.replace(partial_path, final_path)
        _fsync_parent_path(final_path)
        _fsync_file_path(final_path)
        recovered = _read_store_manifest_file(final_path, "store manifest")
        if recovered != manifest:
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                "recovered store manifest differs from its exact partial"
            )
        return recovered, True
    if any(path_binding.primary.canonical_path.iterdir()) or any(
        path_binding.mirror.canonical_path.iterdir()
    ):
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "existing daily WAL residues require the exact store manifest"
        )

    captured = receipt_clock.capture()
    if type(captured) is not ReceiptTimestamp:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "receipt_clock.capture must return exact ReceiptTimestamp"
        )
    manifest = _build_store_manifest(
        census_plan=census_plan,
        config=config,
        path_binding=path_binding,
        captured=captured,
    )
    encoded = canonical_prospective_daily_wal_store_manifest_v2(manifest)
    if len(encoded) > MAX_PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_BYTES_V2:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "store manifest exceeds its fixed byte bound"
        )
    try:
        with partial_path.open("xb", buffering=0) as handle:
            total = 0
            while total < len(encoded):
                written = handle.write(encoded[total:])
                if written is None or written <= 0:
                    raise ProspectiveDailyWalStoreIntegrityErrorV2(
                        "store manifest partial write made no progress"
                    )
                total += written
            os.fsync(handle.fileno())
        os.replace(partial_path, final_path)
        _fsync_parent_path(final_path)
        _fsync_file_path(final_path)
    except FileExistsError as exc:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "store manifest creation raced with another artifact"
        ) from exc
    persisted = _read_store_manifest_file(final_path, "store manifest")
    if persisted != manifest:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "persisted store manifest differs from its admitted candidate"
        )
    writer_lease.assert_prospective_attempt_authority_claim(
        attempt_plan_sha256=census_plan.plan_sha256
    )
    return persisted, False


def _build_store_manifest(
    *,
    census_plan: ProspectiveCensusPlanV2,
    config: ProspectiveDailyWalStoreConfigV2,
    path_binding: _StorePathBinding,
    captured: ReceiptTimestamp,
) -> ProspectiveDailyWalStoreManifestV2:
    if (
        type(captured.received_at_ms) is not int
        or not census_plan.attempt.qualification_start_ms
        <= captured.received_at_ms
        < census_plan.attempt.horizon.h_start_ms
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "first store manifest receipt must be during qualification and before H_start"
        )
    if type(captured.received_monotonic_ns) is not int or captured.received_monotonic_ns <= 0:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "first store manifest monotonic receipt must be positive"
        )
    return ProspectiveDailyWalStoreManifestV2(
        attempt_id=census_plan.attempt_id,
        census_plan_sha256=census_plan.plan_sha256,
        scope_canonical_path_sha256=_scope_path_sha256(path_binding.scope.canonical_path),
        scope_device_decimal=str(path_binding.scope.device),
        scope_inode_decimal=str(path_binding.scope.inode),
        primary_base_device_decimal=str(path_binding.primary.device),
        primary_base_inode_decimal=str(path_binding.primary.inode),
        mirror_base_device_decimal=str(path_binding.mirror.device),
        mirror_base_inode_decimal=str(path_binding.mirror.inode),
        primary_base_relative_to_scope=path_binding.primary_relative,
        mirror_base_relative_to_scope=path_binding.mirror_relative,
        selection_receipt_sha256=config.selection_receipt.sha256,
        policy_sha256=_policy_sha256(config.policy),
        protocol_sha256=config.protocol_sha256,
        source_manifest_sha256=config.source_manifest_sha256,
        schema_sha256=config.schema_sha256,
        runtime_manifest_sha256=config.runtime_manifest_sha256,
        primary_maximum_total_bytes_per_shard=(config.primary_maximum_total_bytes_per_shard),
        mirror_maximum_total_bytes_per_shard=(config.mirror_maximum_total_bytes_per_shard),
        primary_emergency_reserve_bytes_per_shard=(
            config.primary_emergency_reserve_bytes_per_shard
        ),
        mirror_emergency_reserve_bytes_per_shard=(config.mirror_emergency_reserve_bytes_per_shard),
        primary_failure_domain_id=config.primary_failure_domain_id,
        mirror_failure_domain_id=config.mirror_failure_domain_id,
        maximum_batch_records=config.maximum_batch_records,
        typed_decision_payloads_required=(config.typed_decision_payloads_required),
        h_start_ms=census_plan.attempt.horizon.h_start_ms,
        receipt_wall_ms=captured.received_at_ms,
        receipt_monotonic_ns=captured.received_monotonic_ns,
        _factory_token=_STORE_MANIFEST_FACTORY_TOKEN,
    )


def _assert_store_manifest_matches(
    manifest: ProspectiveDailyWalStoreManifestV2,
    *,
    census_plan: ProspectiveCensusPlanV2,
    config: ProspectiveDailyWalStoreConfigV2,
    path_binding: _StorePathBinding,
) -> None:
    expected = _store_manifest_static_document_from_inputs(
        census_plan=census_plan,
        config=config,
        path_binding=path_binding,
    )
    if _store_manifest_static_document(manifest) != expected:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "existing store manifest differs from the exact attempt or config"
        )
    if not (
        census_plan.attempt.qualification_start_ms
        <= manifest.receipt_wall_ms
        < census_plan.attempt.horizon.h_start_ms
    ):
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            "existing store manifest receipt is outside qualification"
        )


def _manifest_artifact_exists(path: Path, field_name: str) -> bool:
    try:
        inspection = inspect_link_free_path(
            path,
            field_name,
            allow_missing_tail=True,
        )
    except ValueError as exc:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(str(exc)) from exc
    status = inspection.final_status
    if status is None:
        if inspection.first_missing_component != path:
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                f"{field_name} parent directory must already exist"
            )
        return False
    if not stat.S_ISREG(status.st_mode) or int(status.st_nlink) != 1:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            f"{field_name} must be one regular single-link file"
        )
    return True


def _read_store_manifest_file(
    path: Path,
    field_name: str,
) -> ProspectiveDailyWalStoreManifestV2:
    try:
        before = inspect_link_free_path(path, field_name)
    except ValueError as exc:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(str(exc)) from exc
    status = before.final_status
    if (
        status is None
        or not stat.S_ISREG(status.st_mode)
        or int(status.st_nlink) != 1
        or not 1 <= int(status.st_size) <= MAX_PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_BYTES_V2
    ):
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            f"{field_name} has an invalid file identity or byte length"
        )
    identity = (int(status.st_dev), int(status.st_ino), int(status.st_size))
    try:
        encoded = path.read_bytes()
    except OSError as exc:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(f"{field_name} could not be read") from exc
    try:
        after = inspect_link_free_path(path, field_name).final_status
    except ValueError as exc:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(str(exc)) from exc
    if (
        after is None
        or not stat.S_ISREG(after.st_mode)
        or int(after.st_nlink) != 1
        or (int(after.st_dev), int(after.st_ino), int(after.st_size)) != identity
    ):
        raise ProspectiveDailyWalStoreIntegrityErrorV2(
            f"{field_name} changed identity while being read"
        )
    return parse_prospective_daily_wal_store_manifest_v2(encoded)


def _validate_store_paths(
    *,
    writer_lease: WriterLease,
    primary_base: Path,
    mirror_base: Path,
) -> _StorePathBinding:
    writer_lease.assert_held()
    scope = _inspect_existing_directory(writer_lease.scope_root, "writer lease scope")
    primary = _inspect_existing_directory(primary_base, "PRIMARY WAL base")
    mirror = _inspect_existing_directory(mirror_base, "INDEPENDENT_MIRROR WAL base")
    if primary.canonical_path == scope.canonical_path or not (
        primary.canonical_path.is_relative_to(scope.canonical_path)
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "PRIMARY WAL base must be a strict child of the writer-lease scope"
        )
    if mirror.canonical_path == scope.canonical_path or not (
        mirror.canonical_path.is_relative_to(scope.canonical_path)
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "INDEPENDENT_MIRROR WAL base must be a strict child of the writer-lease scope"
        )
    if (
        primary.canonical_path == mirror.canonical_path
        or primary.canonical_path.is_relative_to(mirror.canonical_path)
        or mirror.canonical_path.is_relative_to(primary.canonical_path)
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "PRIMARY and INDEPENDENT_MIRROR WAL bases must not overlap"
        )
    return _StorePathBinding(
        scope=scope,
        primary=primary,
        mirror=mirror,
        primary_relative=primary.canonical_path.relative_to(scope.canonical_path).as_posix(),
        mirror_relative=mirror.canonical_path.relative_to(scope.canonical_path).as_posix(),
    )


def _inspect_existing_directory(path: Path, field_name: str) -> _BasePathIdentity:
    try:
        inspection = inspect_link_free_path(path, field_name)
    except ValueError as exc:
        raise ProspectiveDailyWalStoreContractErrorV2(str(exc)) from exc
    status = inspection.final_status
    if status is None or not stat.S_ISDIR(status.st_mode):
        raise ProspectiveDailyWalStoreContractErrorV2(
            f"{field_name} must be an existing link-free directory"
        )
    return _BasePathIdentity(
        canonical_path=_canonical_path(inspection.absolute_path),
        device=int(status.st_dev),
        inode=int(status.st_ino),
    )


def _assert_path_binding_current(binding: _StorePathBinding) -> None:
    for expected, field_name in (
        (binding.scope, "writer lease scope"),
        (binding.primary, "PRIMARY WAL base"),
        (binding.mirror, "INDEPENDENT_MIRROR WAL base"),
    ):
        observed = _inspect_existing_directory(expected.canonical_path, field_name)
        if observed != expected:
            raise ProspectiveDailyWalStoreIntegrityErrorV2(
                f"{field_name} pathname identity changed after store open"
            )


def _canonical_path(path: Path) -> Path:
    return Path(os.path.normcase(os.path.abspath(os.fspath(path))))


def _policy_sha256(policy: WalSyncPolicyV2) -> str:
    return hashlib.sha256(canonical_json_line(asdict(policy))).hexdigest()


def _validate_store_manifest_fields(
    manifest: ProspectiveDailyWalStoreManifestV2,
) -> None:
    if manifest.schema_version != PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_SCHEMA_V2:
        raise ProspectiveDailyWalStoreContractErrorV2("unsupported daily WAL store manifest schema")
    _require_identity(manifest.attempt_id, "manifest attempt_id")
    for value, name in (
        (manifest.census_plan_sha256, "manifest census_plan_sha256"),
        (
            manifest.scope_canonical_path_sha256,
            "manifest scope_canonical_path_sha256",
        ),
        (manifest.selection_receipt_sha256, "manifest selection_receipt_sha256"),
        (manifest.policy_sha256, "manifest policy_sha256"),
        (manifest.protocol_sha256, "manifest protocol_sha256"),
        (manifest.source_manifest_sha256, "manifest source_manifest_sha256"),
        (manifest.schema_sha256, "manifest schema_sha256"),
        (manifest.runtime_manifest_sha256, "manifest runtime_manifest_sha256"),
    ):
        _require_sha256(value, name)
    for value, name in (
        (manifest.scope_device_decimal, "manifest scope device"),
        (manifest.scope_inode_decimal, "manifest scope inode"),
        (manifest.primary_base_device_decimal, "manifest primary base device"),
        (manifest.primary_base_inode_decimal, "manifest primary base inode"),
        (manifest.mirror_base_device_decimal, "manifest mirror base device"),
        (manifest.mirror_base_inode_decimal, "manifest mirror base inode"),
    ):
        _validate_nonnegative_decimal_integer_text(value, name)
    _validate_relative_manifest_path(
        manifest.primary_base_relative_to_scope,
        "manifest primary base",
    )
    _validate_relative_manifest_path(
        manifest.mirror_base_relative_to_scope,
        "manifest mirror base",
    )
    primary_parts = tuple(manifest.primary_base_relative_to_scope.split("/"))
    mirror_parts = tuple(manifest.mirror_base_relative_to_scope.split("/"))
    if (
        primary_parts == mirror_parts
        or primary_parts == mirror_parts[: len(primary_parts)]
        or mirror_parts == primary_parts[: len(mirror_parts)]
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "manifest primary and mirror bases must not overlap"
        )
    for value, name in (
        (
            manifest.primary_emergency_reserve_bytes_per_shard,
            "manifest primary emergency reserve",
        ),
        (
            manifest.mirror_emergency_reserve_bytes_per_shard,
            "manifest mirror emergency reserve",
        ),
    ):
        if type(value) is not int or value < 1_024:
            raise ProspectiveDailyWalStoreContractErrorV2(f"{name} must be at least 1024 bytes")
    if (
        type(manifest.primary_maximum_total_bytes_per_shard) is not int
        or manifest.primary_maximum_total_bytes_per_shard
        <= manifest.primary_emergency_reserve_bytes_per_shard
        or type(manifest.mirror_maximum_total_bytes_per_shard) is not int
        or manifest.mirror_maximum_total_bytes_per_shard
        <= manifest.mirror_emergency_reserve_bytes_per_shard
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "manifest shard quotas must exceed their reserves"
        )
    _require_identity(
        manifest.primary_failure_domain_id,
        "manifest primary_failure_domain_id",
    )
    _require_identity(
        manifest.mirror_failure_domain_id,
        "manifest mirror_failure_domain_id",
    )
    if manifest.primary_failure_domain_id.casefold() == (
        manifest.mirror_failure_domain_id.casefold()
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "manifest failure-domain IDs must be distinct"
        )
    if (
        type(manifest.maximum_batch_records) is not int
        or not 1 <= manifest.maximum_batch_records <= MAX_PROSPECTIVE_DAILY_WAL_BATCH_RECORDS_V2
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "manifest maximum_batch_records is outside its fixed bound"
        )
    if type(manifest.typed_decision_payloads_required) is not bool:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "manifest typed_decision_payloads_required must be boolean"
        )
    if type(manifest.h_start_ms) is not int or manifest.h_start_ms < 1:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "manifest H_start must be a positive Unix millisecond"
        )
    if (
        type(manifest.receipt_wall_ms) is not int
        or not 0 <= manifest.receipt_wall_ms < manifest.h_start_ms
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            "manifest receipt wall time must precede H_start"
        )
    if type(manifest.receipt_monotonic_ns) is not int or manifest.receipt_monotonic_ns <= 0:
        raise ProspectiveDailyWalStoreContractErrorV2(
            "manifest receipt monotonic time must be positive"
        )


def _validate_relative_manifest_path(value: object, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or value.startswith("/")
        or "\\" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            f"{field_name} must be a normalized strict relative POSIX path"
        )


def _validate_nonnegative_decimal_integer_text(
    value: object,
    field_name: str,
) -> None:
    if (
        not isinstance(value, str)
        or not value
        or not value.isascii()
        or not value.isdecimal()
        or (len(value) > 1 and value.startswith("0"))
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            f"{field_name} must be canonical nonnegative decimal integer text"
        )


def _store_manifest_document(
    manifest: ProspectiveDailyWalStoreManifestV2,
) -> dict[str, object]:
    return {
        **_store_manifest_static_document(manifest),
        "receipt_monotonic_ns": manifest.receipt_monotonic_ns,
        "receipt_wall_ms": manifest.receipt_wall_ms,
    }


def _store_manifest_static_document(
    manifest: ProspectiveDailyWalStoreManifestV2,
) -> dict[str, object]:
    return {
        "attempt_id": manifest.attempt_id,
        "census_plan_sha256": manifest.census_plan_sha256,
        "h_start_ms": manifest.h_start_ms,
        "maximum_batch_records": manifest.maximum_batch_records,
        "mirror_base_device_decimal": manifest.mirror_base_device_decimal,
        "mirror_base_inode_decimal": manifest.mirror_base_inode_decimal,
        "mirror_base_relative_to_scope": manifest.mirror_base_relative_to_scope,
        "mirror_emergency_reserve_bytes_per_shard": (
            manifest.mirror_emergency_reserve_bytes_per_shard
        ),
        "mirror_failure_domain_id": manifest.mirror_failure_domain_id,
        "mirror_maximum_total_bytes_per_shard": (manifest.mirror_maximum_total_bytes_per_shard),
        "policy_sha256": manifest.policy_sha256,
        "primary_base_device_decimal": manifest.primary_base_device_decimal,
        "primary_base_inode_decimal": manifest.primary_base_inode_decimal,
        "primary_base_relative_to_scope": (manifest.primary_base_relative_to_scope),
        "primary_emergency_reserve_bytes_per_shard": (
            manifest.primary_emergency_reserve_bytes_per_shard
        ),
        "primary_failure_domain_id": manifest.primary_failure_domain_id,
        "primary_maximum_total_bytes_per_shard": (manifest.primary_maximum_total_bytes_per_shard),
        "protocol_sha256": manifest.protocol_sha256,
        "runtime_manifest_sha256": manifest.runtime_manifest_sha256,
        "schema_sha256": manifest.schema_sha256,
        "schema_version": manifest.schema_version,
        "scope_canonical_path_sha256": manifest.scope_canonical_path_sha256,
        "scope_device_decimal": manifest.scope_device_decimal,
        "scope_inode_decimal": manifest.scope_inode_decimal,
        "selection_receipt_sha256": manifest.selection_receipt_sha256,
        "source_manifest_sha256": manifest.source_manifest_sha256,
        "typed_decision_payloads_required": (manifest.typed_decision_payloads_required),
    }


def _store_manifest_static_document_from_inputs(
    *,
    census_plan: ProspectiveCensusPlanV2,
    config: ProspectiveDailyWalStoreConfigV2,
    path_binding: _StorePathBinding,
) -> dict[str, object]:
    return {
        "attempt_id": census_plan.attempt_id,
        "census_plan_sha256": census_plan.plan_sha256,
        "h_start_ms": census_plan.attempt.horizon.h_start_ms,
        "maximum_batch_records": config.maximum_batch_records,
        "mirror_base_device_decimal": str(path_binding.mirror.device),
        "mirror_base_inode_decimal": str(path_binding.mirror.inode),
        "mirror_base_relative_to_scope": path_binding.mirror_relative,
        "mirror_emergency_reserve_bytes_per_shard": (
            config.mirror_emergency_reserve_bytes_per_shard
        ),
        "mirror_failure_domain_id": config.mirror_failure_domain_id,
        "mirror_maximum_total_bytes_per_shard": (config.mirror_maximum_total_bytes_per_shard),
        "policy_sha256": _policy_sha256(config.policy),
        "primary_base_device_decimal": str(path_binding.primary.device),
        "primary_base_inode_decimal": str(path_binding.primary.inode),
        "primary_base_relative_to_scope": path_binding.primary_relative,
        "primary_emergency_reserve_bytes_per_shard": (
            config.primary_emergency_reserve_bytes_per_shard
        ),
        "primary_failure_domain_id": config.primary_failure_domain_id,
        "primary_maximum_total_bytes_per_shard": (config.primary_maximum_total_bytes_per_shard),
        "protocol_sha256": config.protocol_sha256,
        "runtime_manifest_sha256": config.runtime_manifest_sha256,
        "schema_sha256": config.schema_sha256,
        "schema_version": PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_SCHEMA_V2,
        "scope_canonical_path_sha256": _scope_path_sha256(path_binding.scope.canonical_path),
        "scope_device_decimal": str(path_binding.scope.device),
        "scope_inode_decimal": str(path_binding.scope.inode),
        "selection_receipt_sha256": config.selection_receipt.sha256,
        "source_manifest_sha256": config.source_manifest_sha256,
        "typed_decision_payloads_required": (config.typed_decision_payloads_required),
    }


def _manifest_text(document: dict[str, object], field_name: str) -> str:
    value = document.get(field_name)
    if not isinstance(value, str):
        raise ProspectiveDailyWalStoreIntegrityErrorV2(f"manifest {field_name} must be text")
    return value


def _manifest_int(document: dict[str, object], field_name: str) -> int:
    value = document.get(field_name)
    if type(value) is not int:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(f"manifest {field_name} must be an integer")
    return value


def _manifest_bool(document: dict[str, object], field_name: str) -> bool:
    value = document.get(field_name)
    if type(value) is not bool:
        raise ProspectiveDailyWalStoreIntegrityErrorV2(f"manifest {field_name} must be a boolean")
    return value


def _scope_path_sha256(path: Path) -> str:
    return hashlib.sha256(
        b"R4B_V2_PROSPECTIVE_DAILY_WAL_SCOPE_PATH\0" + os.fspath(path).encode("utf-8")
    ).hexdigest()


def _fsync_file_path(path: Path) -> None:
    mode = "rb+" if os.name == "nt" else "rb"
    with path.open(mode, buffering=0) as handle:
        os.fsync(handle.fileno())


def _fsync_parent_path(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _shard_plan_document(plan: ProspectiveDailyWalShardPlanV2) -> dict[str, object]:
    return {
        "attempt_id": plan.attempt_id,
        "attempt_plan_sha256": plan.attempt_plan_sha256,
        "store_manifest_sha256": plan.store_manifest_sha256,
        "bar_open_stop_exclusive_ms": plan.bar_open_stop_exclusive_ms,
        "day_start_ms": plan.day_start_ms,
        "expected_cell_count": plan.expected_cell_count,
        "first_bar_open_ms": plan.first_bar_open_ms,
        "maximum_batch_records": plan.maximum_batch_records,
        "mirror_directory_relative_to_scope": (plan.mirror_directory_relative_to_scope),
        "mirror_emergency_reserve_bytes": plan.mirror_emergency_reserve_bytes,
        "mirror_failure_domain_id": plan.mirror_failure_domain_id,
        "mirror_maximum_total_bytes": plan.mirror_maximum_total_bytes,
        "policy_sha256": plan.policy_sha256,
        "primary_directory_relative_to_scope": (plan.primary_directory_relative_to_scope),
        "primary_emergency_reserve_bytes": plan.primary_emergency_reserve_bytes,
        "primary_failure_domain_id": plan.primary_failure_domain_id,
        "primary_maximum_total_bytes": plan.primary_maximum_total_bytes,
        "protocol_sha256": plan.protocol_sha256,
        "runtime_manifest_sha256": plan.runtime_manifest_sha256,
        "schema_sha256": plan.schema_sha256,
        "schema_version": plan.schema_version,
        "segment_id": plan.segment_id,
        "segment_index": plan.segment_index,
        "selection_receipt_sha256": plan.selection_receipt_sha256,
        "source_manifest_sha256": plan.source_manifest_sha256,
        "typed_decision_payloads_required": (plan.typed_decision_payloads_required),
    }


def _batch_receipt_document(
    receipt: ProspectiveDailyWalDurableBatchReceiptV2,
) -> dict[str, object]:
    return {
        "attempt_plan_sha256": receipt.attempt_plan_sha256,
        "durable_prefix_proof": asdict(receipt.durable_prefix_proof),
        "first_ingest_seq": receipt.first_ingest_seq,
        "last_ingest_seq": receipt.last_ingest_seq,
        "records": [
            {
                "cell_id": item.cell_id,
                "ingest_seq": item.ingest_seq,
                "kind": item.kind.value,
                "sizing_cell": (None if item.sizing_cell is None else item.sizing_cell.value),
                "payload_sha256": item.payload_sha256,
                "record_sha256": item.record_sha256,
            }
            for item in receipt.records
        ],
        "schema_version": receipt.schema_version,
        "segment_id": receipt.segment_id,
        "segment_index": receipt.segment_index,
        "shard_plan_sha256": receipt.shard_plan_sha256,
    }


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _require_sha256(value: object, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in _SHA256_CHARACTERS for character in value)
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(f"{field_name} must be lowercase SHA-256 hex")


def _require_identity(value: object, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 256
        or any(character in value for character in "\r\n\x00")
    ):
        raise ProspectiveDailyWalStoreContractErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )
