from __future__ import annotations

import hashlib
import hmac
import os
import struct
import time
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import (
    StorageRootBindingV2,
    StorageRootOpenedIdentityV2,
)
from signalbot.r4b_v2.capture.wal import (
    ClockNs,
    FaultHook,
    WalAppendResultV2,
    WalAuthorityV2,
    WalDurabilityBindingV2,
    WalError,
    WalIntegrityError,
    WalQueuedRecordV2,
    WalRecordConsumer,
    WalSyncPolicyV2,
    WalWriterV2,
    verify_wal_segments,
)
from signalbot.r4b_v2.capture.wal_qualification import WalSelectionReceiptV2

_FINGERPRINT_DOMAIN = b"R4B_V2_DUAL_WAL_DURABLE_PREFIX\0"
MIRRORED_WAL_PREFIX_PROOF_SCHEMA_V2 = (
    "r4b_v2_mirrored_wal_prefix_proof_v2"
)


class DurableWalWriterProtocolV2(Protocol):
    """Storage boundary consumed by the disconnected durable batch adapter."""

    authority: WalAuthorityV2
    policy: WalSyncPolicyV2

    @property
    def durable_ack_seq(self) -> int: ...

    @property
    def durability_binding(self) -> WalDurabilityBindingV2: ...

    def assert_root_binding_current(self) -> None: ...

    def assert_running_healthy_and_writer_open_v2(self) -> None: ...

    @property
    def next_ingest_seq(self) -> int: ...

    def append_batch(
        self,
        records: Sequence[WalQueuedRecordV2],
        *,
        now_ns: int | None = None,
    ) -> WalAppendResultV2: ...

    def sync(self, *, now_ns: int | None = None) -> int: ...

    def consume_durable_records(self, consume: WalRecordConsumer) -> int: ...

    def close(self) -> None: ...

    def abort(self) -> None: ...


class MirroredWalIntegrityError(WalIntegrityError):
    """Raised when the two declared WAL copies cannot prove one exact prefix."""


class MirroredWalFailedError(WalError):
    """Raised after an asymmetric or otherwise unsafe dual-WAL operation."""


@dataclass(frozen=True, slots=True)
class MirroredWalPrefixProofV2:
    """Exact identical durable-prefix fingerprint, including the empty prefix."""

    durable_ack_seq: int
    record_count: int
    prefix_sha256: str
    durability_binding_sha256: str
    selection_receipt_sha256: str
    schema_version: str = MIRRORED_WAL_PREFIX_PROOF_SCHEMA_V2

    def __post_init__(self) -> None:
        if self.schema_version != MIRRORED_WAL_PREFIX_PROOF_SCHEMA_V2:
            raise ValueError("unsupported mirrored WAL prefix-proof schema")
        if type(self.durable_ack_seq) is not int or self.durable_ack_seq < 0:
            raise ValueError("durable_ack_seq must be nonnegative")
        if self.record_count != self.durable_ack_seq:
            raise ValueError("record_count must equal the contiguous durable ACK")
        for value, name in (
            (self.prefix_sha256, "prefix_sha256"),
            (self.durability_binding_sha256, "durability_binding_sha256"),
            (self.selection_receipt_sha256, "selection_receipt_sha256"),
        ):
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(character not in "0123456789abcdef" for character in value)
            ):
                raise ValueError(f"{name} must be lowercase SHA-256 hex")

    @property
    def proof_sha256(self) -> str:
        return hashlib.sha256(canonical_json_line(self)).hexdigest()


class MirroredWalWriterV2:
    """Own two independently written WAL roots behind one durable ACK cursor.

    Distinct paths and normalized failure-domain identifiers are necessary
    configuration checks, not evidence that the paths are physically independent.
    Physical failure-domain independence remains a qualification-time obligation.
    """

    def __init__(
        self,
        primary_directory: str | Path,
        mirror_directory: str | Path,
        *,
        authority: WalAuthorityV2,
        policy: WalSyncPolicyV2,
        selection_receipt: WalSelectionReceiptV2,
        primary_maximum_total_bytes: int,
        mirror_maximum_total_bytes: int,
        primary_emergency_reserve_bytes: int,
        mirror_emergency_reserve_bytes: int,
        primary_failure_domain_id: str,
        mirror_failure_domain_id: str,
        clock_ns: ClockNs = time.monotonic_ns,
        primary_fault_hook: FaultHook | None = None,
        mirror_fault_hook: FaultHook | None = None,
        recover_torn_tail: bool = True,
    ) -> None:
        selection_receipt.require_selected_policy(policy)
        primary_path = _normalized_resolved_path(primary_directory)
        mirror_path = _normalized_resolved_path(mirror_directory)
        if _paths_overlap(primary_path, mirror_path):
            raise ValueError("PRIMARY and INDEPENDENT_MIRROR WAL roots must not overlap")
        if _normalized_failure_domain_id(primary_failure_domain_id) == (
            _normalized_failure_domain_id(mirror_failure_domain_id)
        ):
            raise ValueError(
                "PRIMARY and INDEPENDENT_MIRROR failure-domain IDs must be distinct "
                "after normalization"
            )

        self.authority = authority
        self.policy = policy
        self.selection_receipt = selection_receipt
        self._selection_receipt_sha256 = selection_receipt.sha256
        self._clock_ns = clock_ns
        self._closed = False
        self._cleanly_closed = False
        self._verification_only = False
        self._failed: Exception | None = None
        self._primary = WalWriterV2(
            primary_path,
            authority=authority,
            policy=policy,
            maximum_total_bytes=primary_maximum_total_bytes,
            emergency_reserve_bytes=primary_emergency_reserve_bytes,
            clock_ns=clock_ns,
            fault_hook=primary_fault_hook,
            recover_torn_tail=recover_torn_tail,
            root_role="PRIMARY",
            failure_domain_id=primary_failure_domain_id,
            qualification_selection_receipt_sha256=self._selection_receipt_sha256,
        )
        try:
            self._mirror = WalWriterV2(
                mirror_path,
                authority=authority,
                policy=policy,
                maximum_total_bytes=mirror_maximum_total_bytes,
                emergency_reserve_bytes=mirror_emergency_reserve_bytes,
                clock_ns=clock_ns,
                fault_hook=mirror_fault_hook,
                recover_torn_tail=recover_torn_tail,
                root_role="INDEPENDENT_MIRROR",
                failure_domain_id=mirror_failure_domain_id,
                qualification_selection_receipt_sha256=self._selection_receipt_sha256,
            )
        except Exception as exc:
            cleanup_errors = _abort_writers(self._primary)
            if cleanup_errors:
                raise ExceptionGroup(
                    "INDEPENDENT_MIRROR construction and PRIMARY cleanup failed",
                    [exc, *cleanup_errors],
                ) from None
            raise

        try:
            self._verify_root_bindings()
            self._durability_binding = WalDurabilityBindingV2(
                mode="QUALIFIED_DUAL_OWNER",
                root_bindings=(
                    self._primary.root_binding,
                    self._mirror.root_binding,
                ),
                qualification_selection_receipt_sha256=(self._selection_receipt_sha256),
                physical_failure_domain_independence_verified=(
                    self.physical_failure_domain_independence_verified
                ),
            )
            durable_ack_seq = self._verify_identical_durable_prefixes()
        except Exception as exc:
            cleanup_errors = _abort_writers(self._primary, self._mirror)
            if cleanup_errors:
                raise ExceptionGroup(
                    "dual-WAL construction and cleanup both failed",
                    [exc, *cleanup_errors],
                ) from None
            raise
        self._durable_ack_seq = durable_ack_seq
        self._next_ingest_seq = durable_ack_seq + 1

    @classmethod
    def open_verification_only_v2(
        cls,
        primary_directory: str | Path,
        mirror_directory: str | Path,
        *,
        authority: WalAuthorityV2,
        policy: WalSyncPolicyV2,
        selection_receipt: WalSelectionReceiptV2,
        primary_maximum_total_bytes: int,
        mirror_maximum_total_bytes: int,
        primary_emergency_reserve_bytes: int,
        mirror_emergency_reserve_bytes: int,
        primary_failure_domain_id: str,
        mirror_failure_domain_id: str,
        clock_ns: ClockNs = time.monotonic_ns,
    ) -> MirroredWalWriterV2:
        """Reopen an existing finalized dual prefix without granting write authority.

        This mode proves only a current, identical, finalized prefix.  It does not
        by itself prove that a prior process stopped cleanly; the persisted CLEAN
        ledger/session authority supplies that separate claim.
        """

        primary_path = _normalized_resolved_path(primary_directory)
        mirror_path = _normalized_resolved_path(mirror_directory)
        for path, label in (
            (primary_path, "PRIMARY"),
            (mirror_path, "INDEPENDENT_MIRROR"),
        ):
            if not path.is_dir():
                raise MirroredWalIntegrityError(
                    f"{label} verification-only WAL root must already exist"
                )
            if not (path / "storage-root-binding.json").is_file():
                raise MirroredWalIntegrityError(
                    f"{label} verification-only WAL root binding is missing"
                )
            if tuple(path.glob("wal-*.partial")):
                raise MirroredWalIntegrityError(
                    f"{label} verification-only WAL root contains an unfinished partial"
                )
            verify_wal_segments(
                path,
                authority=authority,
                policy=policy,
                allow_finalized_orphan=False,
            )

        reopened = cls(
            primary_path,
            mirror_path,
            authority=authority,
            policy=policy,
            selection_receipt=selection_receipt,
            primary_maximum_total_bytes=primary_maximum_total_bytes,
            mirror_maximum_total_bytes=mirror_maximum_total_bytes,
            primary_emergency_reserve_bytes=primary_emergency_reserve_bytes,
            mirror_emergency_reserve_bytes=mirror_emergency_reserve_bytes,
            primary_failure_domain_id=primary_failure_domain_id,
            mirror_failure_domain_id=mirror_failure_domain_id,
            clock_ns=clock_ns,
            recover_torn_tail=False,
        )
        try:
            reopened._primary.close()
            reopened._mirror.close()
            if reopened._verify_identical_durable_prefixes() != reopened._durable_ack_seq:
                raise MirroredWalIntegrityError(
                    "verification-only dual-WAL prefix differs from its discovered ACK"
                )
        except Exception:
            _abort_writers(reopened._primary, reopened._mirror)
            raise
        reopened._closed = True
        reopened._cleanly_closed = False
        reopened._verification_only = True
        return reopened

    @property
    def durable_ack_seq(self) -> int:
        """Return only the last sequence proven durable on both copies."""

        return self._durable_ack_seq

    @property
    def durability_binding(self) -> WalDurabilityBindingV2:
        return self._durability_binding

    @property
    def root_directories(self) -> tuple[Path, Path]:
        """Return the exact ordered PRIMARY and INDEPENDENT_MIRROR roots."""

        return (self._primary.opened_directory, self._mirror.opened_directory)

    @property
    def opened_root_identities(
        self,
    ) -> tuple[StorageRootOpenedIdentityV2, StorageRootOpenedIdentityV2]:
        return (
            self._primary.opened_root_identity,
            self._mirror.opened_root_identity,
        )

    def assert_running_healthy_and_writer_open_v2(self) -> None:
        self._raise_if_unavailable()
        if self._closed:
            raise MirroredWalFailedError("mirrored WAL writer is closed")
        self._primary.assert_running_healthy_and_writer_open_v2()
        self._mirror.assert_running_healthy_and_writer_open_v2()

    def assert_root_binding_current(self) -> None:
        """Revalidate both immutable root bindings while open or cleanly closed."""

        self._raise_if_failed()
        if self._closed and not (self._cleanly_closed or self._verification_only):
            raise MirroredWalFailedError(
                "aborted mirrored WAL cannot provide current root authority"
            )
        try:
            self._primary.assert_root_binding_current()
            self._mirror.assert_root_binding_current()
        except Exception as exc:
            self._failed = exc
            raise

    def assert_cleanly_closed_and_current_v2(self) -> None:
        """Prove a normal close and one unchanged, identical dual-WAL prefix."""

        self._raise_if_failed()
        if not self._closed or not self._cleanly_closed:
            raise MirroredWalFailedError(
                "mirrored WAL is not cleanly closed"
            )
        self.assert_root_binding_current()
        try:
            if self._verify_identical_durable_prefixes() != self._durable_ack_seq:
                raise MirroredWalIntegrityError(
                    "cleanly closed dual-WAL prefix differs from the joint ACK"
                )
        except Exception as exc:
            self._failed = exc
            raise

    @property
    def verification_only(self) -> bool:
        """Whether this owner was reopened without any write or clean-stop claim."""

        return self._verification_only

    def assert_verification_only_prefix_current_v2(
        self,
        *,
        expected_durable_ack_seq: int,
        expected_durability_binding: WalDurabilityBindingV2,
    ) -> None:
        """Prove the exact persisted prefix exposed by a read-only reconstruction."""

        self._raise_if_failed()
        if not self._verification_only or not self._closed or self._cleanly_closed:
            raise MirroredWalFailedError(
                "mirrored WAL is not a verification-only reconstruction"
            )
        if type(expected_durable_ack_seq) is not int or expected_durable_ack_seq < 0:
            raise ValueError("expected_durable_ack_seq must be a nonnegative integer")
        if type(expected_durability_binding) is not WalDurabilityBindingV2:
            raise TypeError(
                "expected_durability_binding must be an exact WalDurabilityBindingV2"
            )
        if expected_durability_binding != self._durability_binding:
            raise MirroredWalIntegrityError(
                "verification-only dual-WAL binding differs from the persisted closure"
            )
        self.assert_root_binding_current()
        try:
            if any(tuple(path.glob("wal-*.partial")) for path in self.root_directories):
                raise MirroredWalIntegrityError(
                    "verification-only dual-WAL prefix gained an unfinished partial"
                )
            observed = self._verify_identical_durable_prefixes()
            if observed != expected_durable_ack_seq or observed != self._durable_ack_seq:
                raise MirroredWalIntegrityError(
                    "verification-only dual-WAL prefix differs from the persisted tail"
                )
        except Exception as exc:
            self._failed = exc
            raise

    @property
    def next_ingest_seq(self) -> int:
        return self._next_ingest_seq

    @property
    def primary_root_binding(self) -> StorageRootBindingV2:
        return self._primary.root_binding

    @property
    def mirror_root_binding(self) -> StorageRootBindingV2:
        return self._mirror.root_binding

    @property
    def physical_failure_domain_independence_verified(self) -> bool:
        """Code-level path checks cannot establish physical independence."""

        return False

    def append_batch(
        self,
        records: Sequence[WalQueuedRecordV2],
        *,
        now_ns: int | None = None,
    ) -> WalAppendResultV2:
        self._raise_if_unavailable()
        observed_now_ns = self._clock_ns() if now_ns is None else now_ns
        try:
            self._verify_runtime_cursors()
            primary = self._primary.append_batch(records, now_ns=observed_now_ns)
            mirror = self._mirror.append_batch(records, now_ns=observed_now_ns)
            _verify_append_metadata(primary, mirror)
            self._next_ingest_seq = primary.last_ingest_seq + 1
            fsynced = primary.fsynced or mirror.fsynced
            if fsynced:
                self._sync_both(now_ns=observed_now_ns)
            else:
                self._verify_pending_counters()
            return WalAppendResultV2(
                first_ingest_seq=primary.first_ingest_seq,
                last_ingest_seq=primary.last_ingest_seq,
                record_count=primary.record_count,
                encoded_bytes=primary.encoded_bytes,
                durable_ack_seq=self._durable_ack_seq,
                pending_records=self._primary.pending_records,
                pending_bytes=self._primary.pending_bytes,
                fsynced=fsynced,
            )
        except Exception as exc:
            self._failed = exc
            raise

    def sync(self, *, now_ns: int | None = None) -> int:
        self._raise_if_unavailable()
        observed_now_ns = self._clock_ns() if now_ns is None else now_ns
        try:
            return self._sync_both(now_ns=observed_now_ns)
        except Exception as exc:
            self._failed = exc
            raise

    def consume_durable_records(self, consume: WalRecordConsumer) -> int:
        """Verify both durable prefixes before exposing an open or cleanly closed copy."""

        self._raise_if_failed()
        if self._closed and not (self._cleanly_closed or self._verification_only):
            raise MirroredWalFailedError(
                "aborted mirrored WAL cannot expose durable records"
            )
        try:
            if self._verification_only:
                self.assert_verification_only_prefix_current_v2(
                    expected_durable_ack_seq=self._durable_ack_seq,
                    expected_durability_binding=self._durability_binding,
                )
            elif self._closed:
                self.assert_cleanly_closed_and_current_v2()
            else:
                self.sync()
            verified_ack = self._verify_identical_durable_prefixes()
            if verified_ack != self._durable_ack_seq:
                raise MirroredWalIntegrityError(
                    "verified dual-WAL prefix differs from the joint ACK"
                )
            delivered = self._primary.consume_durable_records(consume)
            if delivered != verified_ack:
                raise MirroredWalIntegrityError(
                    "PRIMARY delivery count differs from the verified dual-WAL prefix"
                )
            return delivered
        except Exception as exc:
            self._failed = exc
            raise

    def prove_durable_prefix_v2(self) -> MirroredWalPrefixProofV2:
        """Prove the exact identical dual prefix without exposing record bytes."""

        self._raise_if_failed()
        try:
            if self._verification_only:
                self.assert_verification_only_prefix_current_v2(
                    expected_durable_ack_seq=self._durable_ack_seq,
                    expected_durability_binding=self._durability_binding,
                )
            elif self._closed:
                self.assert_cleanly_closed_and_current_v2()
            else:
                self.sync()
            durable_ack_seq, prefix_digest = (
                self._identical_durable_prefix_fingerprint()
            )
            if durable_ack_seq != self._durable_ack_seq:
                raise MirroredWalIntegrityError(
                    "fingerprinted dual-WAL prefix differs from the joint ACK"
                )
            return MirroredWalPrefixProofV2(
                durable_ack_seq=durable_ack_seq,
                record_count=durable_ack_seq,
                prefix_sha256=prefix_digest.hex(),
                durability_binding_sha256=self._durability_binding.sha256,
                selection_receipt_sha256=self._selection_receipt_sha256,
            )
        except Exception as exc:
            self._failed = exc
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._raise_if_unavailable()
        try:
            self.sync()
            self._primary.close()
            self._mirror.close()
            if self._verify_identical_durable_prefixes() != self._durable_ack_seq:
                raise MirroredWalIntegrityError("closed dual-WAL prefix differs from the joint ACK")
            self._closed = True
            self._cleanly_closed = True
        except Exception as exc:
            self._failed = exc
            raise

    def abort(self) -> None:
        """Close both roots without converting a failed operation into an ACK."""

        if self._closed:
            return
        errors = _abort_writers(self._primary, self._mirror)
        self._closed = True
        if errors:
            self._failed = errors[0]
            raise ExceptionGroup("one or more dual-WAL aborts failed", errors)

    def _sync_both(self, *, now_ns: int) -> int:
        prior_joint_ack = self._durable_ack_seq
        primary_ack = self._primary.sync(now_ns=now_ns)
        mirror_ack = self._mirror.sync(now_ns=now_ns)
        if primary_ack != mirror_ack:
            raise MirroredWalIntegrityError(
                "PRIMARY and INDEPENDENT_MIRROR durable ACK sequences differ"
            )
        expected_tail = self._next_ingest_seq - 1
        if primary_ack != expected_tail:
            raise MirroredWalIntegrityError(
                "dual-WAL sync did not cover the exact jointly appended tail"
            )
        if primary_ack < prior_joint_ack:
            raise MirroredWalIntegrityError("dual-WAL durable ACK moved backwards")
        self._verify_pending_counters()
        if self._primary.pending_records or self._primary.pending_bytes:
            raise MirroredWalIntegrityError("dual-WAL sync retained pending records")
        self._durable_ack_seq = primary_ack
        return self._durable_ack_seq

    def _verify_root_bindings(self) -> None:
        primary = self._primary.root_binding
        mirror = self._mirror.root_binding
        if primary.root_role != "PRIMARY" or mirror.root_role != "INDEPENDENT_MIRROR":
            raise MirroredWalIntegrityError("dual-WAL root roles differ from the contract")
        if primary.authority_sha256 != mirror.authority_sha256:
            raise MirroredWalIntegrityError("dual-WAL root authority bindings differ")
        if primary.contract_sha256 != mirror.contract_sha256:
            raise MirroredWalIntegrityError("dual-WAL root policy bindings differ")
        if (
            self._primary.qualification_selection_receipt_sha256 != self._selection_receipt_sha256
            or self._mirror.qualification_selection_receipt_sha256 != self._selection_receipt_sha256
        ):
            raise MirroredWalIntegrityError(
                "dual-WAL roots are not bound to the exact selection receipt"
            )
        if _normalized_failure_domain_id(primary.failure_domain_id) == (
            _normalized_failure_domain_id(mirror.failure_domain_id)
        ):
            raise MirroredWalIntegrityError("dual-WAL bound failure-domain IDs are not distinct")

    def _verify_runtime_cursors(self) -> None:
        if self._primary.next_ingest_seq != self._mirror.next_ingest_seq:
            raise MirroredWalIntegrityError("dual-WAL next sequence cursors differ")
        if self._primary.next_ingest_seq != self._next_ingest_seq:
            raise MirroredWalIntegrityError("dual-WAL owner next sequence cursor differs")
        if self._primary.durable_ack_seq != self._mirror.durable_ack_seq:
            raise MirroredWalIntegrityError("dual-WAL durable cursors differ")
        if self._primary.durable_ack_seq != self._durable_ack_seq:
            raise MirroredWalIntegrityError("dual-WAL owner durable cursor differs")
        self._verify_pending_counters()

    def _verify_pending_counters(self) -> None:
        if (
            self._primary.pending_records != self._mirror.pending_records
            or self._primary.pending_bytes != self._mirror.pending_bytes
        ):
            raise MirroredWalIntegrityError("dual-WAL pending counters differ")

    def _verify_identical_durable_prefixes(self) -> int:
        return self._identical_durable_prefix_fingerprint()[0]

    def _identical_durable_prefix_fingerprint(self) -> tuple[int, bytes]:
        primary = _fingerprint_durable_prefix(self._primary)
        mirror = _fingerprint_durable_prefix(self._mirror)
        if primary[0] != mirror[0] or not hmac.compare_digest(primary[1], mirror[1]):
            raise MirroredWalIntegrityError(
                "PRIMARY and INDEPENDENT_MIRROR durable prefix bytes or sequences differ"
            )
        return primary

    def _raise_if_unavailable(self) -> None:
        self._raise_if_failed()
        if self._closed:
            raise MirroredWalFailedError("dual-WAL owner is closed")

    def _raise_if_failed(self) -> None:
        if self._failed is not None:
            raise MirroredWalFailedError(
                "dual-WAL owner is fault-latched after an unsafe operation"
            ) from self._failed


def _normalized_failure_domain_id(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("failure-domain IDs must be non-empty strings")
    return unicodedata.normalize("NFKC", value).strip().casefold()


def _normalized_resolved_path(value: str | Path) -> Path:
    return Path(os.path.normcase(os.path.realpath(Path(value)))).resolve(strict=False)


def _paths_overlap(first: Path, second: Path) -> bool:
    try:
        common = Path(os.path.commonpath((first, second)))
    except ValueError:
        return False
    return common == first or common == second


def _verify_append_metadata(
    primary: WalAppendResultV2,
    mirror: WalAppendResultV2,
) -> None:
    if (
        primary.first_ingest_seq,
        primary.last_ingest_seq,
        primary.record_count,
        primary.encoded_bytes,
    ) != (
        mirror.first_ingest_seq,
        mirror.last_ingest_seq,
        mirror.record_count,
        mirror.encoded_bytes,
    ):
        raise MirroredWalIntegrityError("dual-WAL append metadata differs")


def _abort_writers(*writers: WalWriterV2) -> list[Exception]:
    errors: list[Exception] = []
    for writer in writers:
        try:
            writer.abort()
        except Exception as exc:
            errors.append(exc)
    return errors


def _fingerprint_durable_prefix(writer: WalWriterV2) -> tuple[int, bytes]:
    digest = hashlib.sha256(_FINGERPRINT_DOMAIN)
    observed = 0

    def consume(ingest_seq: int, encoded_line: bytes) -> None:
        nonlocal observed
        digest.update(struct.pack(">Q", ingest_seq))
        digest.update(struct.pack(">Q", len(encoded_line)))
        digest.update(encoded_line)
        observed += 1

    delivered = writer.consume_durable_records(consume)
    if delivered != observed or delivered != writer.durable_ack_seq:
        raise MirroredWalIntegrityError(
            "WAL durable cursor differs from its fingerprinted record prefix"
        )
    if writer.next_ingest_seq != delivered + 1:
        raise MirroredWalIntegrityError("WAL materialized tail extends beyond its durable prefix")
    return delivered, digest.digest()
