from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Callable
from dataclasses import InitVar, asdict, dataclass, field
from pathlib import Path
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.authority import StorageRootBindingV2
from signalbot.r4b_v2.capture.block_container import BlockSigningAuthorityV2
from signalbot.r4b_v2.capture.blocks import (
    BlockError,
    BlockManifestV2,
    BlockPolicyV2,
    GroupedBlockWriterV2,
    consume_verified_grouped_records_v2,
    parse_raw_record_line_v2,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityEventV2,
    CaptureIntegrityLedgerError,
    CaptureIntegrityLedgerV2,
    FinalizedBlockReferenceV2,
    SourceGapLeftBoundaryV2,
    SourceGapPayloadV2,
    SourceGapPhaseV2,
    attest_finalized_block_chain_v2,
    attest_finalized_block_v2,
    verify_finalized_block_reference_v2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.wal import WalAuthorityV2

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_IDENTITY_LENGTH = 256
_CERTIFICATE_SCHEMA = "r4b_v2_raw_record_membership_certificate_v1"
_CERTIFICATE_ID_DOMAIN = b"R4B_V2_RAW_RECORD_MEMBERSHIP_CERTIFICATE_ID\0"
_VERIFIED_LEAF_SCHEMA = "r4b_v2_verified_raw_membership_leaf_v2"
_VERIFIED_LEAF_DOMAIN = b"R4B_V2_VERIFIED_RAW_MEMBERSHIP_LEAF\0"
_VERIFIED_LEAF_FACTORY_TOKEN = object()
_CURRENT_LEAF_USE_FACTORY_TOKEN = object()
RAW_MEMBERSHIP_ONLY_REASON_V2: Final = (
    "SIGNED_RAW_MEMBERSHIP_SNAPSHOT_REQUIRES_LIVE_REVERIFICATION_"
    "PARSER_AND_CURSOR_COMPLETENESS_UNPROVEN"
)


class RawRecordMembershipErrorV2(RuntimeError):
    """Raised when exact signed-block membership cannot be established."""


@dataclass(frozen=True, slots=True)
class RawRecordMembershipCertificateV2:
    """Immutable exact-line certificate, verified against current trusted storage.

    The certificate proves membership of one retained canonical RawRecordV2 line.
    It does not prove that Binance emitted no uncaptured source events, nor does it
    claim cursor completeness outside the signed finalized grouped-block prefix.
    """

    finalized_block: FinalizedBlockReferenceV2
    integrity_ledger_root_binding_sha256: str
    integrity_ledger_root_path_sha256: str
    stream_group_id: str
    segment_id: str
    leaf_index: int
    leaf_count: int
    ingest_seq: int
    record_jsonl_base64: str
    record_jsonl_sha256: str
    raw_payload_hash_v2: str
    receipt_wall_ms: int
    receipt_monotonic_ns: int
    transport: str
    venue: str
    route_id: str
    symbol: str | None
    certificate_id: str
    schema_version: str = _CERTIFICATE_SCHEMA

    def __post_init__(self) -> None:
        if not isinstance(self.finalized_block, FinalizedBlockReferenceV2):
            raise ValueError("finalized_block must be a FinalizedBlockReferenceV2")
        for value, field_name in (
            (
                self.integrity_ledger_root_binding_sha256,
                "integrity_ledger_root_binding_sha256",
            ),
            (
                self.integrity_ledger_root_path_sha256,
                "integrity_ledger_root_path_sha256",
            ),
            (self.record_jsonl_sha256, "record_jsonl_sha256"),
            (self.raw_payload_hash_v2, "raw_payload_hash_v2"),
            (self.certificate_id, "certificate_id"),
        ):
            _validate_sha256(value, field_name)
        _validate_identity(self.stream_group_id, "stream_group_id")
        _validate_identity(self.segment_id, "segment_id")
        _validate_identity(self.route_id, "route_id")
        if type(self.leaf_index) is not int or self.leaf_index < 0:
            raise ValueError("leaf_index must be a nonnegative integer")
        if type(self.leaf_count) is not int or self.leaf_count < 1:
            raise ValueError("leaf_count must be a positive integer")
        if self.leaf_index >= self.leaf_count:
            raise ValueError("leaf_index is outside leaf_count")
        reference_count = (
            self.finalized_block.last_ingest_seq
            - self.finalized_block.first_ingest_seq
            + 1
        )
        if self.leaf_count != reference_count:
            raise ValueError("leaf_count differs from finalized block ingest bounds")
        if (
            self.ingest_seq
            != self.finalized_block.first_ingest_seq + self.leaf_index
        ):
            raise ValueError("leaf_index does not identify ingest_seq")

        encoded_line = _strict_base64(self.record_jsonl_base64, "record JSONL")
        record = _parse_canonical_record_line(encoded_line)
        expected_material = (
            hashlib.sha256(encoded_line).hexdigest(),
            record.derive_raw_payload_hash(self.stream_group_id),
            record.ingest_seq,
            record.receipt_wall_ms,
            record.receipt_monotonic_ns,
            record.transport.value,
            record.venue.value,
            record.route_id,
            record.symbol,
        )
        observed_material = (
            self.record_jsonl_sha256,
            self.raw_payload_hash_v2,
            self.ingest_seq,
            self.receipt_wall_ms,
            self.receipt_monotonic_ns,
            self.transport,
            self.venue,
            self.route_id,
            self.symbol,
        )
        if observed_material != expected_material:
            raise ValueError("certificate fields differ from the exact RawRecordV2 line")
        if self.schema_version != _CERTIFICATE_SCHEMA:
            raise ValueError("unsupported raw-record membership certificate schema")
        expected_id = _certificate_id(self)
        if self.certificate_id != expected_id:
            raise ValueError("certificate deterministic ID differs from its evidence")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_line(asdict(self))

    @property
    def record_jsonl(self) -> bytes:
        return _strict_base64(self.record_jsonl_base64, "record JSONL")


@dataclass(frozen=True, slots=True)
class VerifiedRawMembershipLeafV2:
    """Factory-only snapshot of a signed raw member verified when minted.

    This leaf deliberately stops at storage membership.  It does not parse the
    Binance payload, prove a source candidate census, or establish a causal
    decision cursor.  Integrity-ledger authority can also be revoked after the
    leaf is minted.  Callers therefore must live-reverify it at every authority
    use and cannot use the durable snapshot alone to claim current authority or
    complete producer inputs.
    """

    certificate: RawRecordMembershipCertificateV2 = field(repr=False)
    authority: WalAuthorityV2 = field(repr=False)
    stream_group_id: str
    segment_id: str
    record: RawRecordV2 = field(repr=False)
    _factory_token: InitVar[object] = None
    authority_sha256: str = field(init=False)
    certificate_canonical_sha256: str = field(init=False)
    finalized_block_reference_sha256: str = field(init=False)
    raw_payload_hash_v2: str = field(init=False)
    leaf_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_VERIFIED_LEAF_SCHEMA)
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _VERIFIED_LEAF_FACTORY_TOKEN:
            raise RawRecordMembershipErrorV2(
                "verified raw membership leaf requires live membership verification"
            )
        _validate_verified_leaf_material(self)
        object.__setattr__(self, "authority_sha256", self.authority.sha256)
        object.__setattr__(
            self,
            "certificate_canonical_sha256",
            hashlib.sha256(self.certificate.canonical_bytes).hexdigest(),
        )
        object.__setattr__(
            self,
            "finalized_block_reference_sha256",
            hashlib.sha256(
                canonical_json_line(asdict(self.certificate.finalized_block))
            ).hexdigest(),
        )
        object.__setattr__(
            self,
            "raw_payload_hash_v2",
            self.record.derive_raw_payload_hash(self.stream_group_id),
        )
        object.__setattr__(self, "_factory_seal", _VERIFIED_LEAF_FACTORY_TOKEN)
        object.__setattr__(
            self,
            "leaf_sha256",
            hashlib.sha256(
                _VERIFIED_LEAF_DOMAIN
                + canonical_json_line(
                    _verified_raw_membership_leaf_document(self, include_hash=False)
                )
            ).hexdigest(),
        )

    @property
    def parser_bound(self) -> bool:
        return False

    @property
    def verified_raw_membership_m0_at_issuance(self) -> bool:
        return True

    @property
    def live_reverification_required(self) -> bool:
        return True

    @property
    def current_authority_claimed(self) -> bool:
        return False

    @property
    def cursor_complete(self) -> bool:
        return False

    @property
    def causal_inputs_complete(self) -> bool:
        return False

    @property
    def authority_reason(self) -> str:
        return RAW_MEMBERSHIP_ONLY_REASON_V2


class CurrentVerifiedRawMembershipLeafUseV2:
    """Callback-scoped, one-shot access to one just-verified M0 leaf.

    The object is minted only while the exact signed prefix is being streamed.
    It is revoked when that callback returns, cannot be serialized, and can be
    consumed only once.  It is therefore not a reusable current-authority token.
    """

    __slots__ = ("_active", "_consumed", "_factory_seal", "_leaf")

    def __init__(
        self,
        leaf: VerifiedRawMembershipLeafV2,
        *,
        _factory_token: object,
    ) -> None:
        if _factory_token is not _CURRENT_LEAF_USE_FACTORY_TOKEN:
            raise TypeError(
                "current raw-membership use requires the streaming verifier factory"
            )
        if type(leaf) is not VerifiedRawMembershipLeafV2:
            raise TypeError("current raw-membership use requires an exact verified leaf")
        self._leaf = leaf
        self._active = True
        self._consumed = False
        self._factory_seal = _CURRENT_LEAF_USE_FACTORY_TOKEN

    @property
    def active(self) -> bool:
        return self._active

    @property
    def consumed(self) -> bool:
        return self._consumed

    def __repr__(self) -> str:
        return (
            "CurrentVerifiedRawMembershipLeafUseV2("
            f"active={self._active}, consumed={self._consumed})"
        )

    def __reduce__(self) -> str | tuple[object, ...]:
        raise TypeError("callback-scoped current raw-membership use is not serializable")


type CurrentRawMembershipConsumerV2 = Callable[
    [int, bytes, CurrentVerifiedRawMembershipLeafUseV2 | None],
    None,
]


def verify_raw_record_membership_leaf_v2(
    certificate: RawRecordMembershipCertificateV2,
    *,
    block_directory: str | Path,
    block_root_binding: StorageRootBindingV2,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    integrity_ledger: CaptureIntegrityLedgerV2,
    expected_transport: TransportV2,
    expected_venue: VenueV2,
    expected_route_id: str,
    expected_symbol: str | None,
) -> VerifiedRawMembershipLeafV2:
    """Verify current signed membership, then mint one non-promoting M0 leaf."""

    if not isinstance(authority, WalAuthorityV2):
        raise TypeError("authority must be a WalAuthorityV2")
    if not isinstance(expected_transport, TransportV2):
        raise TypeError("expected_transport must be a TransportV2")
    if not isinstance(expected_venue, VenueV2):
        raise TypeError("expected_venue must be a VenueV2")
    _validate_identity(expected_route_id, "expected_route_id")

    record = verify_raw_record_membership_v2(
        certificate,
        block_directory=block_directory,
        block_root_binding=block_root_binding,
        authority=authority,
        policy=policy,
        signing_authority=signing_authority,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        integrity_ledger=integrity_ledger,
    )
    if record.protocol_hash != authority.protocol_sha256:
        raise RawRecordMembershipErrorV2(
            "verified raw record protocol differs from trusted WAL authority"
        )
    observed_scope = (
        record.transport,
        record.venue,
        record.route_id,
        record.symbol,
    )
    expected_scope = (
        expected_transport,
        expected_venue,
        expected_route_id,
        expected_symbol,
    )
    if observed_scope != expected_scope:
        raise RawRecordMembershipErrorV2(
            "verified raw record transport, venue, route, or symbol differs from trusted scope"
        )
    return VerifiedRawMembershipLeafV2(
        certificate=certificate,
        authority=authority,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        record=record,
        _factory_token=_VERIFIED_LEAF_FACTORY_TOKEN,
    )


def reverify_verified_raw_membership_leaf_v2(
    leaf: VerifiedRawMembershipLeafV2,
    *,
    block_directory: str | Path,
    block_root_binding: StorageRootBindingV2,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    integrity_ledger: CaptureIntegrityLedgerV2,
    expected_transport: TransportV2,
    expected_venue: VenueV2,
    expected_route_id: str,
    expected_symbol: str | None,
) -> None:
    """Recheck a durable M0 snapshot against live authority before one use.

    No current-authority token is returned.  A consumer that needs current
    authority must call this assertion internally in the same operation that
    parses or consumes the leaf; caching the durable leaf across later
    integrity changes never carries the successful check forward.
    """

    canonical_verified_raw_membership_leaf_v2(leaf)
    refreshed = verify_raw_record_membership_leaf_v2(
        leaf.certificate,
        block_directory=block_directory,
        block_root_binding=block_root_binding,
        authority=authority,
        policy=policy,
        signing_authority=signing_authority,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        integrity_ledger=integrity_ledger,
        expected_transport=expected_transport,
        expected_venue=expected_venue,
        expected_route_id=expected_route_id,
        expected_symbol=expected_symbol,
    )
    if refreshed != leaf:
        raise RawRecordMembershipErrorV2(
            "live-reverified raw membership differs from its durable snapshot"
        )


def consume_current_verified_raw_membership_leaf_v2(
    current_use: CurrentVerifiedRawMembershipLeafUseV2,
) -> VerifiedRawMembershipLeafV2:
    """Consume one callback-scoped leaf exactly once without exporting authority."""

    if type(current_use) is not CurrentVerifiedRawMembershipLeafUseV2:
        raise TypeError("current_use must be an exact current raw-membership use")
    if current_use._factory_seal is not _CURRENT_LEAF_USE_FACTORY_TOKEN:
        raise RawRecordMembershipErrorV2("current raw-membership factory seal differs")
    if not current_use._active:
        raise RawRecordMembershipErrorV2(
            "current raw-membership use is outside its streaming callback"
        )
    if current_use._consumed:
        raise RawRecordMembershipErrorV2(
            "current raw-membership use has already been consumed"
        )
    current_use._consumed = True
    return current_use._leaf


def inspect_current_verified_raw_membership_leaf_v2(
    current_use: CurrentVerifiedRawMembershipLeafUseV2,
) -> VerifiedRawMembershipLeafV2:
    """Inspect callback-local immutable M0 material without consuming its use."""

    if type(current_use) is not CurrentVerifiedRawMembershipLeafUseV2:
        raise TypeError("current_use must be an exact current raw-membership use")
    if current_use._factory_seal is not _CURRENT_LEAF_USE_FACTORY_TOKEN:
        raise RawRecordMembershipErrorV2("current raw-membership factory seal differs")
    if not current_use._active:
        raise RawRecordMembershipErrorV2(
            "current raw-membership use is outside its streaming callback"
        )
    return current_use._leaf


def consume_current_verified_raw_membership_prefix_v2(
    block_writer: GroupedBlockWriterV2,
    *,
    integrity_ledger: CaptureIntegrityLedgerV2,
    expected_transport: TransportV2,
    expected_venue: VenueV2,
    expected_route_id: str,
    expected_symbol: str | None,
    consume: CurrentRawMembershipConsumerV2,
) -> tuple[int, tuple[BlockManifestV2, ...]]:
    """Stream one exact signed prefix and mint callback-scoped M0 leaf uses.

    The signed manifest chain is attested before and after one streaming read.
    Finalized-block references are reused per block, never looked up per row.
    Target-route leaves are valid only inside their callback and must be consumed
    exactly once there.  Non-target rows are still delivered for prefix hashing
    with ``current_use=None``.
    """

    if type(block_writer) is not GroupedBlockWriterV2:
        raise TypeError("block_writer must be an exact GroupedBlockWriterV2")
    if type(integrity_ledger) is not CaptureIntegrityLedgerV2:
        raise TypeError("integrity_ledger must be an exact CaptureIntegrityLedgerV2")
    if not isinstance(expected_transport, TransportV2):
        raise TypeError("expected_transport must be a TransportV2")
    if not isinstance(expected_venue, VenueV2):
        raise TypeError("expected_venue must be a VenueV2")
    _validate_identity(expected_route_id, "expected_route_id")
    if not callable(consume):
        raise TypeError("consume must be callable")

    before = attest_finalized_block_chain_v2(block_writer)
    if not before:
        raise RawRecordMembershipErrorV2(
            "current raw-membership prefix requires a finalized signed block"
        )
    integrity_ledger.assert_finalized_prefix_not_void_v2(before[-1][1])
    manifest_index = 0

    def emit(ingest_seq: int, encoded_line: bytes) -> None:
        nonlocal manifest_index
        while (
            manifest_index + 1 < len(before)
            and ingest_seq > before[manifest_index][0].last_ingest_seq
        ):
            manifest_index += 1
        manifest, reference = before[manifest_index]
        if not manifest.first_ingest_seq <= ingest_seq <= manifest.last_ingest_seq:
            raise RawRecordMembershipErrorV2(
                "streamed ingest sequence is outside its attested block"
            )
        record = _parse_canonical_record_line(encoded_line)
        if record.ingest_seq != ingest_seq:
            raise RawRecordMembershipErrorV2(
                "streamed record differs from its signed ingest sequence"
            )
        if record.protocol_hash != block_writer.authority.protocol_sha256:
            raise RawRecordMembershipErrorV2(
                "streamed raw record protocol differs from trusted WAL authority"
            )
        if record.route_id != expected_route_id:
            consume(ingest_seq, encoded_line, None)
            return
        observed_scope = (
            record.transport,
            record.venue,
            record.route_id,
            record.symbol,
        )
        expected_scope = (
            expected_transport,
            expected_venue,
            expected_route_id,
            expected_symbol,
        )
        if observed_scope != expected_scope:
            raise RawRecordMembershipErrorV2(
                "streamed target record transport, venue, route, or symbol differs"
            )
        certificate = _build_certificate(
            reference=reference,
            integrity_ledger=integrity_ledger,
            stream_group_id=block_writer.stream_group_id,
            segment_id=block_writer.segment_id,
            record=record,
            encoded_line=encoded_line,
        )
        leaf = VerifiedRawMembershipLeafV2(
            certificate=certificate,
            authority=block_writer.authority,
            stream_group_id=block_writer.stream_group_id,
            segment_id=block_writer.segment_id,
            record=record,
            _factory_token=_VERIFIED_LEAF_FACTORY_TOKEN,
        )
        current_use = CurrentVerifiedRawMembershipLeafUseV2(
            leaf,
            _factory_token=_CURRENT_LEAF_USE_FACTORY_TOKEN,
        )
        try:
            consume(ingest_seq, encoded_line, current_use)
            if not current_use._consumed:
                raise RawRecordMembershipErrorV2(
                    "target current raw-membership use was not consumed in its callback"
                )
        finally:
            current_use._active = False

    delivered = block_writer.consume_committed_records(emit)
    after = attest_finalized_block_chain_v2(block_writer)
    if after != before:
        raise RawRecordMembershipErrorV2(
            "signed grouped-block prefix changed during current membership consumption"
        )
    integrity_ledger.assert_finalized_prefix_not_void_v2(after[-1][1])
    return delivered, tuple(manifest for manifest, _reference in after)


def append_source_gap_bounded_from_membership_v2(
    integrity_ledger: CaptureIntegrityLedgerV2,
    open_event: CaptureIntegrityEventV2,
    *,
    left_certificate: RawRecordMembershipCertificateV2 | None,
    right_certificate: RawRecordMembershipCertificateV2,
    block_directory: str | Path,
    block_root_binding: StorageRootBindingV2,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    evidence_sha256: str,
) -> CaptureIntegrityEventV2:
    """Compatibility facade; the ledger independently resolves signed endpoints."""

    if not isinstance(open_event, CaptureIntegrityEventV2):
        raise TypeError("open_event must be a CaptureIntegrityEventV2")
    try:
        open_payload = SourceGapPayloadV2(**open_event.payload)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise RawRecordMembershipErrorV2(
            "SOURCE_GAP OPEN payload is invalid"
        ) from exc
    if (
        open_event.event_type != "SOURCE_GAP"
        or open_payload.phase != SourceGapPhaseV2.OPEN.value
    ):
        raise RawRecordMembershipErrorV2(
            "source-gap membership binding requires an OPEN event"
        )

    right_record = verify_raw_record_membership_v2(
        right_certificate,
        block_directory=block_directory,
        block_root_binding=block_root_binding,
        authority=authority,
        policy=policy,
        signing_authority=signing_authority,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        integrity_ledger=integrity_ledger,
    )
    _validate_source_gap_member_scope(
        right_record,
        open_payload=open_payload,
        authority=authority,
        label="right",
    )
    if right_record.frame_seq is None:
        raise RawRecordMembershipErrorV2(
            "SOURCE_GAP right WebSocket member has no frame sequence"
        )

    if (
        open_payload.left_boundary_kind
        == SourceGapLeftBoundaryV2.RETAINED_FRAME.value
    ):
        if left_certificate is None:
            raise RawRecordMembershipErrorV2(
                "retained-frame SOURCE_GAP requires left membership"
            )
        left_record = verify_raw_record_membership_v2(
            left_certificate,
            block_directory=block_directory,
            block_root_binding=block_root_binding,
            authority=authority,
            policy=policy,
            signing_authority=signing_authority,
            stream_group_id=stream_group_id,
            segment_id=segment_id,
            integrity_ledger=integrity_ledger,
        )
        _validate_source_gap_member_scope(
            left_record,
            open_payload=open_payload,
            authority=authority,
            label="left",
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
            raise RawRecordMembershipErrorV2(
                "SOURCE_GAP left member differs from its OPEN cursor"
            )
    elif left_certificate is not None:
        raise RawRecordMembershipErrorV2(
            "session-start SOURCE_GAP cannot provide left membership"
        )

    try:
        return integrity_ledger.append_source_gap_bounded(
            open_event,
            right_ingest_seq=right_record.ingest_seq,
            evidence_sha256=evidence_sha256,
        )
    except (CaptureIntegrityLedgerError, ValueError) as exc:
        raise RawRecordMembershipErrorV2(
            "SOURCE_GAP membership-bound commit failed closed"
        ) from exc


def reverify_source_gap_bounded_membership_v2(
    integrity_ledger: CaptureIntegrityLedgerV2,
    bounded_event: CaptureIntegrityEventV2,
    *,
    block_directory: str | Path,
    block_root_binding: StorageRootBindingV2,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
) -> None:
    """Compatibility facade for the non-reusable current-authority assertion."""

    expected_scope = (
        Path(block_directory).resolve(),
        block_root_binding,
        authority,
        policy,
        signing_authority,
        stream_group_id,
        segment_id,
    )
    observed_scope = (
        integrity_ledger.block_directory,
        integrity_ledger.block_root_binding,
        integrity_ledger.authority,
        integrity_ledger.block_policy,
        integrity_ledger.block_signing_authority,
        integrity_ledger.block_stream_group_id,
        integrity_ledger.block_segment_id,
    )
    if observed_scope != expected_scope:
        raise RawRecordMembershipErrorV2(
            "SOURCE_GAP replay scope differs from ledger authority"
        )
    try:
        integrity_ledger.assert_source_gap_bounded_current_v2(bounded_event)
    except CaptureIntegrityLedgerError as exc:
        raise RawRecordMembershipErrorV2(
            "SOURCE_GAP current signed-record replay failed closed"
        ) from exc


def canonical_verified_raw_membership_leaf_v2(
    leaf: VerifiedRawMembershipLeafV2,
) -> bytes:
    """Serialize a self-consistent issuance snapshot, not current authority."""

    if not isinstance(leaf, VerifiedRawMembershipLeafV2):
        raise TypeError("leaf must be a VerifiedRawMembershipLeafV2")
    try:
        if leaf._factory_seal is not _VERIFIED_LEAF_FACTORY_TOKEN:
            raise ValueError("verified raw membership leaf factory seal differs")
        _validate_verified_leaf_material(leaf)
        expected_authority = leaf.authority.sha256
        expected_certificate = hashlib.sha256(leaf.certificate.canonical_bytes).hexdigest()
        expected_reference = hashlib.sha256(
            canonical_json_line(asdict(leaf.certificate.finalized_block))
        ).hexdigest()
        expected_raw_payload = leaf.record.derive_raw_payload_hash(
            leaf.stream_group_id
        )
        expected_leaf = hashlib.sha256(
            _VERIFIED_LEAF_DOMAIN
            + canonical_json_line(
                _verified_raw_membership_leaf_document(leaf, include_hash=False)
            )
        ).hexdigest()
        if (
            leaf.schema_version != _VERIFIED_LEAF_SCHEMA
            or leaf.authority_sha256 != expected_authority
            or leaf.certificate_canonical_sha256 != expected_certificate
            or leaf.finalized_block_reference_sha256 != expected_reference
            or leaf.raw_payload_hash_v2 != expected_raw_payload
            or leaf.leaf_sha256 != expected_leaf
        ):
            raise ValueError("verified raw membership leaf differs from canonical evidence")
        return canonical_json_line(
            _verified_raw_membership_leaf_document(leaf, include_hash=True)
        )
    except ValueError as exc:
        raise RawRecordMembershipErrorV2(
            "verified raw membership leaf is invalid"
        ) from exc


def _validate_verified_leaf_material(leaf: VerifiedRawMembershipLeafV2) -> None:
    if not isinstance(leaf.certificate, RawRecordMembershipCertificateV2):
        raise ValueError(
            "certificate must be a RawRecordMembershipCertificateV2"
        )
    if not isinstance(leaf.authority, WalAuthorityV2):
        raise ValueError("authority must be a WalAuthorityV2")
    if not isinstance(leaf.record, RawRecordV2):
        raise ValueError("record must be a RawRecordV2")
    _validate_identity(leaf.stream_group_id, "stream_group_id")
    _validate_identity(leaf.segment_id, "segment_id")

    certificate = leaf.certificate
    reference = certificate.finalized_block
    if (
        certificate.stream_group_id != leaf.stream_group_id
        or certificate.segment_id != leaf.segment_id
    ):
        raise ValueError("certificate stream or segment differs from leaf scope")
    if reference.authority_sha256 != leaf.authority.sha256:
        raise ValueError("finalized block differs from leaf WAL authority")

    exact_record = _parse_canonical_record_line(certificate.record_jsonl)
    if exact_record != leaf.record:
        raise ValueError("leaf record differs from the exact certificate line")
    if exact_record.protocol_hash != leaf.authority.protocol_sha256:
        raise ValueError("leaf record protocol differs from WAL authority")
    expected_envelope = (
        exact_record.ingest_seq,
        exact_record.receipt_wall_ms,
        exact_record.receipt_monotonic_ns,
        exact_record.transport.value,
        exact_record.venue.value,
        exact_record.route_id,
        exact_record.symbol,
    )
    certificate_envelope = (
        certificate.ingest_seq,
        certificate.receipt_wall_ms,
        certificate.receipt_monotonic_ns,
        certificate.transport,
        certificate.venue,
        certificate.route_id,
        certificate.symbol,
    )
    if certificate_envelope != expected_envelope:
        raise ValueError("certificate envelope differs from exact leaf record")
    if (
        certificate.record_jsonl_sha256
        != hashlib.sha256(certificate.record_jsonl).hexdigest()
    ):
        raise ValueError("certificate exact-line digest differs")
    expected_payload_hash = exact_record.derive_raw_payload_hash(
        leaf.stream_group_id
    )
    if certificate.raw_payload_hash_v2 != expected_payload_hash:
        raise ValueError("certificate raw-payload digest differs")


def _validate_source_gap_member_scope(
    record: RawRecordV2,
    *,
    open_payload: SourceGapPayloadV2,
    authority: WalAuthorityV2,
    label: str,
) -> None:
    expected = (
        open_payload.session_id,
        open_payload.plan_id,
        authority.protocol_sha256,
        TransportV2.WEBSOCKET,
        VenueV2.USDM_FUTURES,
        open_payload.route_id,
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
        raise RawRecordMembershipErrorV2(
            f"SOURCE_GAP {label} member differs from its trusted combined-stream scope"
        )


def _verified_raw_membership_leaf_document(
    leaf: VerifiedRawMembershipLeafV2,
    *,
    include_hash: bool,
) -> dict[str, object]:
    reference = leaf.certificate.finalized_block
    document: dict[str, object] = {
        "authority": asdict(leaf.authority),
        "authority_reason": leaf.authority_reason,
        "authority_sha256": leaf.authority_sha256,
        "causal_inputs_complete": leaf.causal_inputs_complete,
        "certificate": asdict(leaf.certificate),
        "certificate_canonical_sha256": leaf.certificate_canonical_sha256,
        "certificate_id": leaf.certificate.certificate_id,
        "cursor_complete": leaf.cursor_complete,
        "finalized_block_scope": {
            "block_hash": reference.block_hash,
            "block_root_binding_sha256": reference.block_root_binding_sha256,
            "block_root_path_sha256": reference.block_root_path_sha256,
            "block_sequence": reference.block_sequence,
            "finalized_block_reference_sha256": (
                leaf.finalized_block_reference_sha256
            ),
            "first_ingest_seq": reference.first_ingest_seq,
            "last_ingest_seq": reference.last_ingest_seq,
            "previous_block_hash": reference.previous_block_hash,
        },
        "integrity_scope": {
            "ledger_root_binding_sha256": (
                leaf.certificate.integrity_ledger_root_binding_sha256
            ),
            "ledger_root_path_sha256": (
                leaf.certificate.integrity_ledger_root_path_sha256
            ),
        },
        "current_authority_claimed": leaf.current_authority_claimed,
        "live_reverification_required": leaf.live_reverification_required,
        "parser_bound": leaf.parser_bound,
        "raw_payload_hash_v2": leaf.raw_payload_hash_v2,
        "record": asdict(leaf.record),
        "record_jsonl_sha256": leaf.certificate.record_jsonl_sha256,
        "schema_version": leaf.schema_version,
        "segment_id": leaf.segment_id,
        "stream_group_id": leaf.stream_group_id,
        "verified_raw_membership_m0_at_issuance": (
            leaf.verified_raw_membership_m0_at_issuance
        ),
    }
    if include_hash:
        document["leaf_sha256"] = leaf.leaf_sha256
    return document


def attest_raw_record_membership_v2(
    block_writer: GroupedBlockWriterV2,
    manifest: BlockManifestV2,
    *,
    expected_record_jsonl: bytes,
    integrity_ledger: CaptureIntegrityLedgerV2,
) -> RawRecordMembershipCertificateV2:
    """Attest an exact canonical line in a currently healthy finalized block."""

    try:
        record = _parse_canonical_record_line(expected_record_jsonl)
        reference = attest_finalized_block_v2(block_writer, manifest)
        if not reference.first_ingest_seq <= record.ingest_seq <= reference.last_ingest_seq:
            raise RawRecordMembershipErrorV2(
                "expected record does not belong to the requested finalized block"
            )
        observed = _read_exact_ingest_line(
            directory=block_writer.directory,
            authority=block_writer.authority,
            policy=block_writer.policy,
            signing_authority=block_writer.signing_authority,
            stream_group_id=block_writer.stream_group_id,
            segment_id=block_writer.segment_id,
            ingest_seq=record.ingest_seq,
        )
        if observed != expected_record_jsonl:
            raise RawRecordMembershipErrorV2(
                "expected RawRecordV2 line differs from signed block bytes"
            )
        integrity_ledger.assert_finalized_prefix_not_void_v2(reference)
        return _build_certificate(
            reference=reference,
            integrity_ledger=integrity_ledger,
            stream_group_id=block_writer.stream_group_id,
            segment_id=block_writer.segment_id,
            record=record,
            encoded_line=observed,
        )
    except RawRecordMembershipErrorV2:
        raise
    except (BlockError, CaptureIntegrityLedgerError, OSError, ValueError) as exc:
        raise RawRecordMembershipErrorV2(
            f"raw-record membership attestation failed closed: {exc}"
        ) from exc


def verify_raw_record_membership_v2(
    certificate: RawRecordMembershipCertificateV2,
    *,
    block_directory: str | Path,
    block_root_binding: StorageRootBindingV2,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    integrity_ledger: CaptureIntegrityLedgerV2,
) -> RawRecordV2:
    """Publicly verify exact membership using only trusted public authority data."""

    if not isinstance(certificate, RawRecordMembershipCertificateV2):
        raise TypeError("certificate must be a RawRecordMembershipCertificateV2")
    try:
        if (
            certificate.stream_group_id != stream_group_id
            or certificate.segment_id != segment_id
        ):
            raise RawRecordMembershipErrorV2(
                "certificate stream or segment differs from trusted scope"
            )
        if (
            certificate.integrity_ledger_root_binding_sha256
            != integrity_ledger.ledger_root_binding_sha256
            or certificate.integrity_ledger_root_path_sha256
            != integrity_ledger.ledger_root_path_sha256
        ):
            raise RawRecordMembershipErrorV2(
                "certificate names a different integrity-ledger root"
            )
        manifest = verify_finalized_block_reference_v2(
            certificate.finalized_block,
            block_directory=block_directory,
            block_root_binding=block_root_binding,
            authority=authority,
            policy=policy,
            signing_authority=signing_authority,
            stream_group_id=stream_group_id,
            segment_id=segment_id,
        )
        if manifest.record_count != certificate.leaf_count:
            raise RawRecordMembershipErrorV2(
                "certificate leaf_count differs from signed manifest"
            )
        observed = _read_exact_ingest_line(
            directory=block_directory,
            authority=authority,
            policy=policy,
            signing_authority=signing_authority,
            stream_group_id=stream_group_id,
            segment_id=segment_id,
            ingest_seq=certificate.ingest_seq,
        )
        if observed != certificate.record_jsonl:
            raise RawRecordMembershipErrorV2(
                "certificate line differs from current signed block bytes"
            )
        integrity_ledger.assert_finalized_prefix_not_void_v2(
            certificate.finalized_block
        )
        return _parse_canonical_record_line(observed)
    except RawRecordMembershipErrorV2:
        raise
    except (BlockError, CaptureIntegrityLedgerError, OSError, ValueError) as exc:
        raise RawRecordMembershipErrorV2(
            f"raw-record membership verification failed closed: {exc}"
        ) from exc


def parse_raw_record_membership_certificate_v2(
    encoded: bytes,
) -> RawRecordMembershipCertificateV2:
    """Parse only the exact canonical serialized certificate representation."""

    if not isinstance(encoded, bytes):
        raise TypeError("encoded certificate must be immutable bytes")
    try:
        document = json.loads(encoded)
        if not isinstance(document, dict):
            raise TypeError("certificate must be a JSON object")
        if canonical_json_line(document) != encoded:
            raise ValueError("certificate is not canonical JCS JSONL")
        converted = dict(document)
        reference = converted.get("finalized_block")
        if not isinstance(reference, dict):
            raise TypeError("finalized_block must be an object")
        converted["finalized_block"] = FinalizedBlockReferenceV2(**reference)
        certificate = RawRecordMembershipCertificateV2(**converted)  # type: ignore[arg-type]
        if certificate.canonical_bytes != encoded:
            raise ValueError("certificate canonical replay differs")
        return certificate
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as exc:
        raise RawRecordMembershipErrorV2(
            "raw-record membership certificate is invalid"
        ) from exc


def _build_certificate(
    *,
    reference: FinalizedBlockReferenceV2,
    integrity_ledger: CaptureIntegrityLedgerV2,
    stream_group_id: str,
    segment_id: str,
    record: RawRecordV2,
    encoded_line: bytes,
) -> RawRecordMembershipCertificateV2:
    fields: dict[str, object] = {
        "finalized_block": reference,
        "integrity_ledger_root_binding_sha256": (
            integrity_ledger.ledger_root_binding_sha256
        ),
        "integrity_ledger_root_path_sha256": integrity_ledger.ledger_root_path_sha256,
        "stream_group_id": stream_group_id,
        "segment_id": segment_id,
        "leaf_index": record.ingest_seq - reference.first_ingest_seq,
        "leaf_count": reference.last_ingest_seq - reference.first_ingest_seq + 1,
        "ingest_seq": record.ingest_seq,
        "record_jsonl_base64": base64.b64encode(encoded_line).decode("ascii"),
        "record_jsonl_sha256": hashlib.sha256(encoded_line).hexdigest(),
        "raw_payload_hash_v2": record.derive_raw_payload_hash(stream_group_id),
        "receipt_wall_ms": record.receipt_wall_ms,
        "receipt_monotonic_ns": record.receipt_monotonic_ns,
        "transport": record.transport.value,
        "venue": record.venue.value,
        "route_id": record.route_id,
        "symbol": record.symbol,
        "schema_version": _CERTIFICATE_SCHEMA,
    }
    identity = dict(fields)
    identity["finalized_block"] = asdict(reference)
    certificate_id = hashlib.sha256(
        _CERTIFICATE_ID_DOMAIN + canonical_json_line(identity)
    ).hexdigest()
    return RawRecordMembershipCertificateV2(
        **fields,  # type: ignore[arg-type]
        certificate_id=certificate_id,
    )


def _read_exact_ingest_line(
    *,
    directory: str | Path,
    authority: WalAuthorityV2,
    policy: BlockPolicyV2,
    signing_authority: BlockSigningAuthorityV2,
    stream_group_id: str,
    segment_id: str,
    ingest_seq: int,
) -> bytes:
    matches: list[bytes] = []

    def select(observed_ingest_seq: int, encoded_line: bytes) -> None:
        if observed_ingest_seq == ingest_seq:
            matches.append(encoded_line)

    consume_verified_grouped_records_v2(
        directory,
        authority=authority,
        policy=policy,
        signing_authority=signing_authority,
        stream_group_id=stream_group_id,
        segment_id=segment_id,
        consume=select,
    )
    if len(matches) != 1:
        raise RawRecordMembershipErrorV2(
            "ingest_seq is absent or duplicated in the verified finalized chain"
        )
    return matches[0]


def _parse_canonical_record_line(encoded_line: bytes) -> RawRecordV2:
    if not isinstance(encoded_line, bytes):
        raise TypeError("record JSONL must be immutable bytes")
    try:
        document = json.loads(encoded_line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record JSONL is invalid") from exc
    if not isinstance(document, dict) or canonical_json_line(document) != encoded_line:
        raise ValueError("record is not exact canonical JCS JSONL")
    return parse_raw_record_line_v2(encoded_line)


def _certificate_id(certificate: RawRecordMembershipCertificateV2) -> str:
    document = asdict(certificate)
    document.pop("certificate_id")
    return hashlib.sha256(
        _CERTIFICATE_ID_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def _strict_base64(value: str, label: str) -> bytes:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be strict base64 text")
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError(f"{label} is not strict base64") from exc
    if base64.b64encode(decoded).decode("ascii") != value:
        raise ValueError(f"{label} is not canonical base64")
    return decoded


def _validate_identity(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a bounded normalized identity")


def _validate_sha256(value: str, field: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
