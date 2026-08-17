from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable
from dataclasses import InitVar, dataclass, field
from typing import Final, TypeVar

from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.alerts.actionability import CausalTargetCursorV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.blocks import GroupedBlockWriterV2
from signalbot.r4b_v2.capture.causal_target_cursor import (
    CausalTargetCursorSnapshotV2,
    canonical_causal_target_cursor_snapshot_v2,
    derive_causal_target_cursor_snapshot_v2,
    require_factory_causal_target_cursor_snapshot_v2,
)
from signalbot.r4b_v2.capture.integrity_ledger import CaptureIntegrityLedgerV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingPlanV9,
    provisional_promoting_plan_sha256_v9,
    validate_provisional_promoting_capture_plans_v9,
)

CAUSAL_TARGET_AUTHORITY_RULE_VERSION_V2: Final = "R4B_CAUSAL_V2.4.0_CURRENT_CAUSAL_TARGET_AUTHORITY"

_CAPABILITY_ID_DOMAIN = b"R4B_V2_CURRENT_CAUSAL_TARGET_CAPABILITY\0"
_CAPABILITY_FACTORY_TOKEN = object()
_MAXIMUM_AUTHORIZATIONS = 1_000_000
_T = TypeVar("_T")


class CausalTargetAuthorityErrorV2(RuntimeError):
    """Current cursor authority could not be proved or was misused."""


class CurrentCausalTargetAuthorityUseV2:
    """Callback-scoped, one-use authority for one exactly reverified cursor."""

    __slots__ = (
        "_active",
        "_capability_id",
        "_consumed",
        "_cursor",
        "_factory_seal",
        "_snapshot",
    )

    def __init__(
        self,
        *,
        snapshot: CausalTargetCursorSnapshotV2,
        cursor: CausalTargetCursorV2,
        capability_id: str,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _CAPABILITY_FACTORY_TOKEN:
            raise CausalTargetAuthorityErrorV2(
                "current causal-target authority uses are factory-sealed"
            )
        require_factory_causal_target_cursor_snapshot_v2(snapshot)
        if type(cursor) is not CausalTargetCursorV2:
            raise TypeError("cursor must be an exact CausalTargetCursorV2")
        expected_cursor = _cursor_from_snapshot(snapshot)
        if cursor != expected_cursor:
            raise CausalTargetAuthorityErrorV2(
                "current authority cursor differs from its reverified snapshot"
            )
        self._snapshot = snapshot
        self._cursor = cursor
        self._capability_id = capability_id
        self._active = True
        self._consumed = False
        self._factory_seal = _CAPABILITY_FACTORY_TOKEN

    @property
    def active(self) -> bool:
        return self._active

    @property
    def consumed(self) -> bool:
        return self._consumed

    @property
    def capability_id(self) -> str:
        return self._capability_id

    @property
    def snapshot_sha256(self) -> str:
        return self._snapshot.snapshot_sha256

    @property
    def promoting_plan_sha256(self) -> str:
        return self._snapshot.promoting_plan_sha256

    @property
    def paper_input_authorized(self) -> bool:
        return self._active and not self._consumed

    @property
    def production_order_placement(self) -> bool:
        return False

    @property
    def durable_current_authority_claimed(self) -> bool:
        return False

    def __repr__(self) -> str:
        return (
            f"CurrentCausalTargetAuthorityUseV2(active={self._active}, consumed={self._consumed})"
        )

    def __reduce__(self) -> str | tuple[object, ...]:
        raise TypeError("current causal-target authority use is not serializable")


@dataclass(frozen=True, slots=True)
class CausalTargetAuthorityOwnerBindingV2:
    capture_authority_sha256: str
    promoting_plan_sha256: str
    integrity_ledger_root_binding_sha256: str
    block_root_binding_sha256: str
    maximum_authorizations: int
    _factory_token: InitVar[object | None] = None
    binding_sha256: str = field(init=False)
    rule_version: str = field(
        init=False,
        default=CAUSAL_TARGET_AUTHORITY_RULE_VERSION_V2,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CAPABILITY_FACTORY_TOKEN:
            raise CausalTargetAuthorityErrorV2("causal-target authority bindings are owner-sealed")
        document = {
            "block_root_binding_sha256": self.block_root_binding_sha256,
            "capture_authority_sha256": self.capture_authority_sha256,
            "integrity_ledger_root_binding_sha256": (self.integrity_ledger_root_binding_sha256),
            "maximum_authorizations": self.maximum_authorizations,
            "promoting_plan_sha256": self.promoting_plan_sha256,
            "rule_version": self.rule_version,
        }
        object.__setattr__(
            self,
            "binding_sha256",
            hashlib.sha256(canonical_json_line(document)).hexdigest(),
        )


class CausalTargetAuthorityOwnerV2:
    """Serialize live re-verification and consume authority inside one callback."""

    def __init__(
        self,
        *,
        block_writer: GroupedBlockWriterV2,
        integrity_ledger: CaptureIntegrityLedgerV2,
        promoting_plans: tuple[ProvisionalPromotingPlanV9, ...],
        writer_lease: WriterLease,
        maximum_authorizations: int,
    ) -> None:
        if type(block_writer) is not GroupedBlockWriterV2:
            raise TypeError("block_writer must be an exact GroupedBlockWriterV2")
        if type(integrity_ledger) is not CaptureIntegrityLedgerV2:
            raise TypeError("integrity_ledger must be an exact CaptureIntegrityLedgerV2")
        if type(promoting_plans) is not tuple:
            raise TypeError("promoting_plans must be the exact frozen tuple")
        if type(writer_lease) is not WriterLease:
            raise TypeError("writer_lease must be an exact WriterLease")
        if (
            type(maximum_authorizations) is not int
            or not 1 <= maximum_authorizations <= _MAXIMUM_AUTHORIZATIONS
        ):
            raise ValueError("maximum_authorizations is outside the sealed bound")
        writer_lease.assert_held()
        if integrity_ledger.writer_lease is not writer_lease:
            raise CausalTargetAuthorityErrorV2(
                "integrity ledger does not share the exact authority writer lease"
            )
        frozen_plans = promoting_plans
        validate_provisional_promoting_capture_plans_v9(frozen_plans)
        plan_sha256 = provisional_promoting_plan_sha256_v9(frozen_plans)
        if block_writer.authority.plan_sha256 != plan_sha256:
            raise CausalTargetAuthorityErrorV2(
                "block writer authority differs from the frozen capture plan"
            )
        if integrity_ledger.authority is not block_writer.authority:
            raise CausalTargetAuthorityErrorV2(
                "integrity ledger and block writer must share exact authority ownership"
            )
        self._block_writer = block_writer
        self._integrity_ledger = integrity_ledger
        self._promoting_plans = frozen_plans
        self._writer_lease = writer_lease
        self._maximum_authorizations = maximum_authorizations
        self._authorization_sequence = 0
        self._active = False
        self._lock = threading.RLock()
        self._binding = CausalTargetAuthorityOwnerBindingV2(
            capture_authority_sha256=block_writer.authority.sha256,
            promoting_plan_sha256=plan_sha256,
            integrity_ledger_root_binding_sha256=(integrity_ledger.ledger_root_binding_sha256),
            block_root_binding_sha256=(integrity_ledger.block_root_binding_sha256),
            maximum_authorizations=maximum_authorizations,
            _factory_token=_CAPABILITY_FACTORY_TOKEN,
        )

    @property
    def binding(self) -> CausalTargetAuthorityOwnerBindingV2:
        return self._binding

    @property
    def authorization_count(self) -> int:
        with self._lock:
            return self._authorization_sequence

    def with_current_authority(
        self,
        snapshot: CausalTargetCursorSnapshotV2,
        *,
        consume: Callable[[CurrentCausalTargetAuthorityUseV2], _T],
    ) -> _T:
        """Reverify, issue, and revoke one capability inside the lease guard."""

        require_factory_causal_target_cursor_snapshot_v2(snapshot)
        if not callable(consume):
            raise TypeError("consume must be callable")
        with self._lock:
            if self._active:
                raise CausalTargetAuthorityErrorV2(
                    "causal-target authority owner rejects reentrant use"
                )
            if self._authorization_sequence >= self._maximum_authorizations:
                raise CausalTargetAuthorityErrorV2(
                    "causal-target authority owner exhausted its bounded issuance cap"
                )
            self._active = True
            try:
                with self._writer_lease.operation_guard():
                    self._assert_binding_current()
                    expected_snapshot = canonical_causal_target_cursor_snapshot_v2(snapshot)
                    self._assert_snapshot_current(
                        snapshot,
                        expected_snapshot=expected_snapshot,
                    )
                    issuance_sequence = self._authorization_sequence + 1
                    capability_id = _capability_id(
                        binding=self._binding,
                        snapshot=snapshot,
                        issuance_sequence=issuance_sequence,
                    )
                    current_use = CurrentCausalTargetAuthorityUseV2(
                        snapshot=snapshot,
                        cursor=_cursor_from_snapshot(snapshot),
                        capability_id=capability_id,
                        _factory_token=_CAPABILITY_FACTORY_TOKEN,
                    )
                    self._authorization_sequence = issuance_sequence
                    try:
                        result = consume(current_use)
                        if not current_use._consumed:
                            raise CausalTargetAuthorityErrorV2(
                                "current causal-target capability was not consumed"
                            )
                        self._assert_binding_current()
                        self._assert_snapshot_current(
                            snapshot,
                            expected_snapshot=expected_snapshot,
                        )
                        return result
                    finally:
                        current_use._active = False
            finally:
                self._active = False

    def _assert_binding_current(self) -> None:
        self._writer_lease.assert_held()
        if self._integrity_ledger.writer_lease is not self._writer_lease:
            raise CausalTargetAuthorityErrorV2(
                "integrity ledger writer lease changed after owner construction"
            )
        if (
            self._block_writer.authority.sha256 != self._binding.capture_authority_sha256
            or self._integrity_ledger.ledger_root_binding_sha256
            != self._binding.integrity_ledger_root_binding_sha256
            or self._integrity_ledger.block_root_binding_sha256
            != self._binding.block_root_binding_sha256
        ):
            raise CausalTargetAuthorityErrorV2("causal-target authority owner binding drifted")

    def _assert_snapshot_current(
        self,
        snapshot: CausalTargetCursorSnapshotV2,
        *,
        expected_snapshot: bytes,
    ) -> None:
        refreshed = derive_causal_target_cursor_snapshot_v2(
            self._block_writer,
            integrity_ledger=self._integrity_ledger,
            promoting_plans=self._promoting_plans,
            decision_cutoff_ms=snapshot.decision_cutoff_ms,
        )
        if canonical_causal_target_cursor_snapshot_v2(refreshed) != expected_snapshot:
            raise CausalTargetAuthorityErrorV2(
                "current signed prefix differs from the factory snapshot"
            )


def consume_current_causal_target_authority_v2(
    current_use: CurrentCausalTargetAuthorityUseV2,
) -> CausalTargetCursorV2:
    """Consume one active capability and return its internally derived cursor."""

    if type(current_use) is not CurrentCausalTargetAuthorityUseV2:
        raise TypeError(
            "current_use must be an exact CurrentCausalTargetAuthorityUseV2; "
            "direct CausalTargetCursorV2 values are rejected"
        )
    if current_use._factory_seal is not _CAPABILITY_FACTORY_TOKEN:
        raise CausalTargetAuthorityErrorV2("current causal-target capability factory seal differs")
    if not current_use._active:
        raise CausalTargetAuthorityErrorV2("current causal-target capability has been revoked")
    if current_use._consumed:
        raise CausalTargetAuthorityErrorV2(
            "current causal-target capability has already been consumed"
        )
    require_factory_causal_target_cursor_snapshot_v2(current_use._snapshot)
    if current_use._cursor != _cursor_from_snapshot(current_use._snapshot):
        raise CausalTargetAuthorityErrorV2("current causal-target capability cursor was tampered")
    current_use._consumed = True
    return current_use._cursor


def _cursor_from_snapshot(
    snapshot: CausalTargetCursorSnapshotV2,
) -> CausalTargetCursorV2:
    return CausalTargetCursorV2(
        decision_cutoff_ms=snapshot.decision_cutoff_ms,
        target_venue_ms=snapshot.target_venue_ms,
        prior_local_cursor_ms=snapshot.prior_local_cursor_ms,
        prior_venue_lower_bound_ms=snapshot.prior_venue_lower_bound_ms,
        target_local_cursor_ms=snapshot.target_local_cursor_ms,
        target_venue_lower_bound_ms=snapshot.target_venue_lower_bound_ms,
        clock_segment_root_sha256=snapshot.clock_segment_root_sha256,
        contiguous_cursor_evidence=True,
    )


def _capability_id(
    *,
    binding: CausalTargetAuthorityOwnerBindingV2,
    snapshot: CausalTargetCursorSnapshotV2,
    issuance_sequence: int,
) -> str:
    return hashlib.sha256(
        _CAPABILITY_ID_DOMAIN
        + canonical_json_line(
            {
                "binding_sha256": binding.binding_sha256,
                "issuance_sequence": issuance_sequence,
                "snapshot_sha256": snapshot.snapshot_sha256,
            }
        )
    ).hexdigest()
