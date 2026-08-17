from __future__ import annotations

import hashlib
import hmac
import json
import re
import struct
from collections.abc import Iterable
from dataclasses import InitVar, dataclass, field, fields
from typing import Final, Literal, cast

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.batching import QueuedRawRecordV2
from signalbot.r4b_v2.capture.blocks import BlockIntegrityError, parse_raw_record_line_v2
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import CaptureFinalityFenceReceiptV2
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2
from signalbot.r4b_v2.capture.rest import (
    PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2,
    PUBLIC_OI_REST_POLL_INTERVAL_MS_V2,
    PublicOiRestAttemptPayloadV2,
)
from signalbot.r4b_v2.capture.rest_census import (
    PUBLIC_OI_REST_CENSUS_MAX_CANONICAL_BYTES_V2,
    PublicOiRestCellOutcomeV2,
    PublicOiRestCensusPayloadV2,
    PublicOiRestCoverageCloseV2,
    PublicOiRestForwardGapRangeV2,
    PublicOiRestSlotCensusV2,
    public_oi_rest_attempt_record_sha256_v2,
    public_oi_rest_plan_sha256_v2,
    public_oi_rest_symbol_census_sha256_v2,
)

BODY_SEMANTICS_UNVERIFIED_V2: Final = "BODY_SEMANTICS_UNVERIFIED"

_REST_ROUTE_ID = "usdm_public_rest"
_CENSUS_CONNECTION_ID = "oi-rest-census"
_CENSUS_SOURCE_LOGICAL_KEY = "openInterest:census"
_SLOT_SCHEMA = "r4b_v2_public_oi_rest_slot_census_v1"
_GAP_SCHEMA = "r4b_v2_public_oi_rest_forward_gap_range_v1"
_CLOSE_SCHEMA = "r4b_v2_public_oi_rest_coverage_close_v1"
_CERTIFICATE_SCHEMA = "r4b_v2_public_oi_rest_census_verification_certificate_v1"
_WAL_BLOCK_PREFIX_DOMAIN = b"R4B_V2_WAL_BLOCK_PREFIX\0"
_CERTIFICATE_DOMAIN = b"R4B_V2_PUBLIC_OI_REST_CENSUS_VERIFICATION\0"
_CERTIFICATE_FACTORY_TOKEN = object()
_VERIFIER_FACTORY_TOKEN = object()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class PublicOiRestCensusVerificationErrorV2(ValueError):
    """The retained OI schedule evidence does not prove one exact coverage prefix."""


@dataclass(frozen=True, slots=True)
class PublicOiRestCensusVerificationCertificateV2:
    """Factory-sealed local schedule-coverage proof, never an M2 certificate.

    The certificate binds a caller-supplied, finality-fenced prefix.  Reproving
    current WAL files, grouped-block roots, and the current tail remains the
    caller's boundary; this pure verifier performs no filesystem access.
    """

    session_id: str
    protocol_hash: str
    session_start_manifest_sha256: str
    plan_bundle_sha256: str
    plan_id: str
    rest_plan_sha256: str
    symbol_census_sha256: str
    symbol_count: int
    finality_receipt_sha256: str
    finality_authority_sha256: str
    finality_exact_prefix_sha256: str
    finality_prefix_proof_sha256: str
    verified_prefix_tail_ingest_seq: int
    verified_record_count: int
    coverage_start_slot_wall_ms: int
    coverage_end_slot_exclusive_wall_ms: int
    coverage_close_ingest_seq: int
    last_census_ingest_seq: int | None
    rest_attempt_record_count: int
    slot_census_record_count: int
    forward_gap_record_count: int
    ignored_websocket_record_count: int
    covered_slot_count: int
    scheduled_cell_count: int
    attempt_retained_cell_count: int
    unstarted_cell_count: int
    coverage_closed: Literal[True]
    data_complete: Literal[False]
    data_completeness_reason: Literal["BODY_SEMANTICS_UNVERIFIED"]
    m2_certified: Literal[False]
    session_close_authorized: Literal[False]
    current_storage_reproved: Literal[False]
    schema_version: Literal["r4b_v2_public_oi_rest_census_verification_certificate_v1"] = (
        _CERTIFICATE_SCHEMA
    )
    certificate_sha256: str = field(init=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CERTIFICATE_FACTORY_TOKEN:
            raise TypeError("public OI census verification certificates are factory-sealed")
        object.__setattr__(self, "_factory_seal", _CERTIFICATE_FACTORY_TOKEN)
        _validate_certificate_material(self, verify_digest=False)
        object.__setattr__(self, "certificate_sha256", _certificate_sha256(self))


@dataclass(frozen=True, slots=True)
class _PendingAttemptV2:
    ingest_seq: int
    encoded_sha256: str
    symbol_ordinal: int
    symbol: str
    scheduled_slot_wall_ms: int
    poll_cycle_seq: int
    receipt_wall_ms: int
    receipt_monotonic_ns: int


@dataclass(slots=True)
class _VerificationStateV2:
    pending_by_ordinal: dict[int, _PendingAttemptV2] = field(default_factory=dict)
    pending_slot_wall_ms: int | None = None
    pending_poll_cycle_seq: int | None = None
    coverage_start_slot_wall_ms: int | None = None
    coverage_cursor_wall_ms: int | None = None
    close_payload: PublicOiRestCoverageCloseV2 | None = None
    close_ingest_seq: int | None = None
    last_census_ingest_seq: int | None = None
    rest_attempt_record_count: int = 0
    slot_census_record_count: int = 0
    forward_gap_record_count: int = 0
    ignored_websocket_record_count: int = 0
    covered_slot_count: int = 0
    scheduled_cell_count: int = 0
    attempt_retained_cell_count: int = 0
    unstarted_cell_count: int = 0


class PublicOiRestCensusPrefixVerifierV2:
    """One-shot synchronous verifier for one exact finalized record prefix.

    Instances are created by
    :func:`create_public_oi_rest_census_prefix_verifier_v2`.  ``consume`` has
    the exact callback shape used by ``GroupedBlockWriterV2`` and retains only
    the current slot's at-most-32 pending attempts plus counters and digests.
    Any failed consume or any finalize attempt makes the instance terminal.
    """

    __slots__ = (
        "_expected_ingest_seq",
        "_finality_receipt",
        "_last_receipt_monotonic_ns",
        "_last_receipt_wall_ms",
        "_lifecycle",
        "_plan",
        "_plan_bundle_sha256",
        "_prefix_digest",
        "_previous_monotonic_ns",
        "_previous_wall_ms",
        "_protocol_hash",
        "_session_id",
        "_session_start_manifest_sha256",
        "_state",
    )

    def __init__(
        self,
        *,
        plan: ProvisionalPromotingRestCapturePlanV2,
        session_id: str,
        protocol_hash: str,
        session_start_manifest_sha256: str,
        plan_bundle_sha256: str,
        finality_receipt: CaptureFinalityFenceReceiptV2,
        _factory_token: object | None = None,
    ) -> None:
        if _factory_token is not _VERIFIER_FACTORY_TOKEN:
            raise TypeError("public OI census prefix verifiers must be created by their factory")
        _validate_verifier_authority_inputs(
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            session_start_manifest_sha256=session_start_manifest_sha256,
            plan_bundle_sha256=plan_bundle_sha256,
            finality_receipt=finality_receipt,
        )
        self._plan = plan
        self._session_id = session_id
        self._protocol_hash = protocol_hash
        self._session_start_manifest_sha256 = session_start_manifest_sha256
        self._plan_bundle_sha256 = plan_bundle_sha256
        self._finality_receipt = finality_receipt
        self._state = _VerificationStateV2()
        self._prefix_digest = hashlib.sha256(_WAL_BLOCK_PREFIX_DOMAIN)
        self._expected_ingest_seq = 1
        self._previous_wall_ms: int | None = None
        self._previous_monotonic_ns: int | None = None
        self._last_receipt_wall_ms: int | None = None
        self._last_receipt_monotonic_ns: int | None = None
        self._lifecycle: Literal["OPEN", "FAILED", "FINALIZED"] = "OPEN"

    def consume(self, ingest_seq: int, encoded_line: bytes) -> None:
        """Consume one exact canonical JSONL record from the finalized prefix."""

        self._require_open("consume")
        try:
            self._consume_open(ingest_seq, encoded_line)
        except Exception:
            self._lifecycle = "FAILED"
            raise

    def finalize(self) -> PublicOiRestCensusVerificationCertificateV2:
        """Close this one-shot verifier and return its factory-sealed certificate."""

        self._require_open("finalize")
        try:
            certificate = self._finalize_open()
        except Exception:
            self._lifecycle = "FAILED"
            raise
        self._lifecycle = "FINALIZED"
        return certificate

    def _consume_open(self, ingest_seq: int, encoded_line: bytes) -> None:
        if type(ingest_seq) is not int or ingest_seq < 1:
            raise TypeError("ingest_seq must be a positive exact integer")
        if type(encoded_line) is not bytes:
            raise TypeError("encoded_line must be immutable bytes")
        try:
            record = parse_raw_record_line_v2(encoded_line)
        except BlockIntegrityError as exc:
            raise PublicOiRestCensusVerificationErrorV2(
                "the callback line is not a valid raw-record JSONL envelope"
            ) from exc
        if canonical_json_line(record) != encoded_line:
            raise PublicOiRestCensusVerificationErrorV2(
                "the callback line is not the exact canonical raw record"
            )
        if record.ingest_seq != ingest_seq:
            raise PublicOiRestCensusVerificationErrorV2(
                "the callback ingest sequence differs from its encoded raw record"
            )
        if self._expected_ingest_seq > self._finality_receipt.fence_ingest_seq:
            raise PublicOiRestCensusVerificationErrorV2(
                "the supplied prefix extends beyond the finality-fence tail"
            )
        if ingest_seq != self._expected_ingest_seq:
            raise PublicOiRestCensusVerificationErrorV2(
                "the supplied prefix must contain exact ingest sequences 1..tail"
            )
        if record.session_id != self._session_id or record.protocol_hash != self._protocol_hash:
            raise PublicOiRestCensusVerificationErrorV2(
                "a prefix record differs from the exact session or protocol lineage"
            )
        _validate_outer_receipt_order(
            record,
            previous_wall_ms=self._previous_wall_ms,
            previous_monotonic_ns=self._previous_monotonic_ns,
        )
        _update_prefix_digest(self._prefix_digest, ingest_seq, encoded_line)
        if self._state.close_payload is None:
            _consume_record(
                self._state,
                record=record,
                encoded_line=encoded_line,
                plan=self._plan,
                session_id=self._session_id,
                protocol_hash=self._protocol_hash,
                session_start_manifest_sha256=(self._session_start_manifest_sha256),
                plan_bundle_sha256=self._plan_bundle_sha256,
            )
        else:
            _consume_post_close_record(
                self._state,
                record=record,
                plan=self._plan,
            )
        self._previous_wall_ms = record.receipt_wall_ms
        self._previous_monotonic_ns = record.receipt_monotonic_ns
        self._last_receipt_wall_ms = record.receipt_wall_ms
        self._last_receipt_monotonic_ns = record.receipt_monotonic_ns
        self._expected_ingest_seq += 1

    def _finalize_open(self) -> PublicOiRestCensusVerificationCertificateV2:
        verified_count = self._expected_ingest_seq - 1
        if verified_count != self._finality_receipt.fence_ingest_seq:
            raise PublicOiRestCensusVerificationErrorV2(
                "the supplied prefix does not reach the exact finality-fence tail"
            )
        if self._last_receipt_wall_ms is None or self._last_receipt_monotonic_ns is None:
            raise PublicOiRestCensusVerificationErrorV2("the finalized prefix is empty")
        if not hmac.compare_digest(
            self._prefix_digest.hexdigest(),
            self._finality_receipt.exact_prefix_sha256,
        ):
            raise PublicOiRestCensusVerificationErrorV2(
                "the recomputed WAL/block prefix digest differs from finality"
            )
        if (
            self._last_receipt_wall_ms != self._finality_receipt.target_last_receipt_wall_ms
            or self._last_receipt_monotonic_ns
            != self._finality_receipt.target_last_receipt_monotonic_ns
        ):
            raise PublicOiRestCensusVerificationErrorV2(
                "the finality target receipt clocks differ from the exact prefix tail"
            )
        close = self._state.close_payload
        close_ingest_seq = self._state.close_ingest_seq
        if close is None or close_ingest_seq is None:
            raise PublicOiRestCensusVerificationErrorV2(
                "the finalized prefix lacks its one exact OI coverage close"
            )
        if close_ingest_seq > self._finality_receipt.fence_ingest_seq:
            raise PublicOiRestCensusVerificationErrorV2(
                "the OI coverage close cannot follow the finalized prefix tail"
            )
        if self._state.pending_by_ordinal:
            raise PublicOiRestCensusVerificationErrorV2(
                "the finalized prefix contains unreferenced public OI attempts"
            )
        certificate = PublicOiRestCensusVerificationCertificateV2(
            session_id=self._session_id,
            protocol_hash=self._protocol_hash,
            session_start_manifest_sha256=self._session_start_manifest_sha256,
            plan_bundle_sha256=self._plan_bundle_sha256,
            plan_id=self._plan.name,
            rest_plan_sha256=public_oi_rest_plan_sha256_v2(self._plan),
            symbol_census_sha256=public_oi_rest_symbol_census_sha256_v2(self._plan),
            symbol_count=len(self._plan.symbols),
            finality_receipt_sha256=self._finality_receipt.sha256,
            finality_authority_sha256=self._finality_receipt.authority_sha256,
            finality_exact_prefix_sha256=(self._finality_receipt.exact_prefix_sha256),
            finality_prefix_proof_sha256=(self._finality_receipt.prefix_proof_sha256),
            verified_prefix_tail_ingest_seq=(self._finality_receipt.fence_ingest_seq),
            verified_record_count=verified_count,
            coverage_start_slot_wall_ms=close.coverage_start_slot_wall_ms,
            coverage_end_slot_exclusive_wall_ms=(close.coverage_end_slot_exclusive_wall_ms),
            coverage_close_ingest_seq=close_ingest_seq,
            last_census_ingest_seq=self._state.last_census_ingest_seq,
            rest_attempt_record_count=self._state.rest_attempt_record_count,
            slot_census_record_count=self._state.slot_census_record_count,
            forward_gap_record_count=self._state.forward_gap_record_count,
            ignored_websocket_record_count=(self._state.ignored_websocket_record_count),
            covered_slot_count=self._state.covered_slot_count,
            scheduled_cell_count=self._state.scheduled_cell_count,
            attempt_retained_cell_count=(self._state.attempt_retained_cell_count),
            unstarted_cell_count=self._state.unstarted_cell_count,
            coverage_closed=True,
            data_complete=False,
            data_completeness_reason=BODY_SEMANTICS_UNVERIFIED_V2,
            m2_certified=False,
            session_close_authorized=False,
            current_storage_reproved=False,
            _factory_token=_CERTIFICATE_FACTORY_TOKEN,
        )
        validate_public_oi_rest_census_verification_certificate_v2(certificate)
        return certificate

    def _require_open(self, operation: str) -> None:
        if self._lifecycle != "OPEN":
            raise PublicOiRestCensusVerificationErrorV2(
                f"cannot {operation}: the one-shot verifier is {self._lifecycle.lower()}"
            )


def create_public_oi_rest_census_prefix_verifier_v2(
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
    finality_receipt: CaptureFinalityFenceReceiptV2,
) -> PublicOiRestCensusPrefixVerifierV2:
    """Validate authority inputs and create one empty push verifier."""

    return PublicOiRestCensusPrefixVerifierV2(
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        session_start_manifest_sha256=session_start_manifest_sha256,
        plan_bundle_sha256=plan_bundle_sha256,
        finality_receipt=finality_receipt,
        _factory_token=_VERIFIER_FACTORY_TOKEN,
    )


def verify_public_oi_rest_census_prefix_v2(
    records: Iterable[RawRecordV2 | QueuedRawRecordV2],
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
    finality_receipt: CaptureFinalityFenceReceiptV2,
) -> PublicOiRestCensusVerificationCertificateV2:
    """Stream-verify the exact finalized prefix and its public-OI schedule census.

    Only pending attempts for the current 5-second slot are retained in memory,
    bounded by the frozen 32-symbol plan census.  Interleaved WebSocket records
    contribute to the exact-prefix digest and sequence checks but not OI state.
    """

    if not isinstance(records, Iterable):
        raise TypeError("records must be an ordered iterable prefix")
    verifier = create_public_oi_rest_census_prefix_verifier_v2(
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        session_start_manifest_sha256=session_start_manifest_sha256,
        plan_bundle_sha256=plan_bundle_sha256,
        finality_receipt=finality_receipt,
    )
    for value in records:
        record, encoded_line = _normalize_prefix_record(value)
        verifier.consume(record.ingest_seq, encoded_line)
    return verifier.finalize()


def validate_public_oi_rest_census_verification_certificate_v2(
    certificate: PublicOiRestCensusVerificationCertificateV2,
) -> str:
    """Revalidate factory provenance and return the deterministic certificate hash."""

    if type(certificate) is not PublicOiRestCensusVerificationCertificateV2:
        raise TypeError("certificate must be an exact PublicOiRestCensusVerificationCertificateV2")
    if getattr(certificate, "_factory_seal", None) is not _CERTIFICATE_FACTORY_TOKEN:
        raise PublicOiRestCensusVerificationErrorV2(
            "public OI census certificate lacks verifier provenance"
        )
    _validate_certificate_material(certificate, verify_digest=True)
    return certificate.certificate_sha256


def _consume_record(
    state: _VerificationStateV2,
    *,
    record: RawRecordV2,
    encoded_line: bytes,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
) -> None:
    rest_related = (
        record.transport is TransportV2.HTTPS
        or record.route_id == plan.route_id
        or record.plan_id == plan.name
    )
    if not rest_related:
        if record.transport is not TransportV2.WEBSOCKET:
            raise PublicOiRestCensusVerificationErrorV2(
                "the prefix contains a foreign non-WebSocket transport"
            )
        state.ignored_websocket_record_count += 1
        return
    if (
        record.transport is not TransportV2.HTTPS
        or record.venue is not VenueV2.USDM_FUTURES
        or record.plan_id != plan.name
        or record.route_id != _REST_ROUTE_ID
        or record.frame_seq is not None
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "the prefix contains a foreign or malformed public OI REST record"
        )
    if record.symbol is None:
        payload = _parse_and_validate_census_record(
            record,
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            session_start_manifest_sha256=session_start_manifest_sha256,
            plan_bundle_sha256=plan_bundle_sha256,
        )
        _consume_census_payload(state, payload=payload, carrier_ingest_seq=record.ingest_seq)
        return
    _consume_attempt(
        state,
        record=record,
        encoded_line=encoded_line,
        plan=plan,
    )


def _consume_post_close_record(
    state: _VerificationStateV2,
    *,
    record: RawRecordV2,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> None:
    """Accept only the already-supported non-REST WebSocket envelope class.

    WebSocket owners can finish after the OI scheduler commits its write-once
    normal-stop close. Those later records remain part of the exact finalized
    prefix but never extend OI schedule coverage. Any HTTPS carrier, or a
    WebSocket envelope impersonating the REST plan/route, is fail-closed.
    """

    if (
        record.transport is not TransportV2.WEBSOCKET
        or record.route_id == plan.route_id
        or record.plan_id == plan.name
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "only non-REST WebSocket records may follow the OI coverage close"
        )
    state.ignored_websocket_record_count += 1


def _consume_attempt(
    state: _VerificationStateV2,
    *,
    record: RawRecordV2,
    encoded_line: bytes,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> None:
    symbol = record.symbol
    assert symbol is not None
    if symbol not in plan.symbols:
        raise PublicOiRestCensusVerificationErrorV2(
            "a public OI attempt symbol is outside the exact plan census"
        )
    ordinal = plan.symbols.index(symbol)
    if record.source_logical_key != f"openInterest:{symbol}":
        raise PublicOiRestCensusVerificationErrorV2(
            "a public OI attempt has the wrong stable logical key"
        )
    payload = PublicOiRestAttemptPayloadV2.from_canonical_bytes(record.payload_bytes(), plan=plan)
    if (
        payload.symbol != symbol
        or payload.symbol_ordinal != ordinal
        or payload.attempt != 1
        or payload.completion_admission_wall_ms != record.receipt_wall_ms
        or payload.completion_admission_monotonic_ns != record.receipt_monotonic_ns
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "a public OI attempt differs from its plan, ordinal, attempt, or receipt clocks"
        )
    expected_slot = state.coverage_cursor_wall_ms
    if expected_slot is not None and payload.scheduled_slot_wall_ms != expected_slot:
        raise PublicOiRestCensusVerificationErrorV2(
            "a public OI attempt belongs to an earlier or later coverage slot"
        )
    expected_poll_cycle_seq = state.slot_census_record_count + 1
    if payload.poll_cycle_seq != expected_poll_cycle_seq:
        raise PublicOiRestCensusVerificationErrorV2(
            "a public OI attempt differs from the exact launched-slot poll cycle"
        )
    if (
        state.pending_slot_wall_ms is not None
        and payload.scheduled_slot_wall_ms != state.pending_slot_wall_ms
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "attempts from multiple slots cannot precede one census carrier"
        )
    if (
        state.pending_poll_cycle_seq is not None
        and payload.poll_cycle_seq != state.pending_poll_cycle_seq
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "attempts in one slot disagree on their poll cycle"
        )
    if ordinal in state.pending_by_ordinal:
        raise PublicOiRestCensusVerificationErrorV2(
            "a schedule cell contains duplicate public OI attempts"
        )
    if len(state.pending_by_ordinal) >= min(
        len(plan.symbols), PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "pending public OI attempt state exceeds the frozen symbol bound"
        )
    canonical_hash = public_oi_rest_attempt_record_sha256_v2(record)
    encoded_hash = hashlib.sha256(encoded_line).hexdigest()
    if not hmac.compare_digest(canonical_hash, encoded_hash):
        raise PublicOiRestCensusVerificationErrorV2(
            "a retained OI attempt line differs from its canonical record hash"
        )
    state.pending_slot_wall_ms = payload.scheduled_slot_wall_ms
    state.pending_poll_cycle_seq = payload.poll_cycle_seq
    state.pending_by_ordinal[ordinal] = _PendingAttemptV2(
        ingest_seq=record.ingest_seq,
        encoded_sha256=encoded_hash,
        symbol_ordinal=ordinal,
        symbol=symbol,
        scheduled_slot_wall_ms=payload.scheduled_slot_wall_ms,
        poll_cycle_seq=payload.poll_cycle_seq,
        receipt_wall_ms=record.receipt_wall_ms,
        receipt_monotonic_ns=record.receipt_monotonic_ns,
    )
    state.rest_attempt_record_count += 1


def _consume_census_payload(
    state: _VerificationStateV2,
    *,
    payload: PublicOiRestCensusPayloadV2,
    carrier_ingest_seq: int,
) -> None:
    if type(payload) is PublicOiRestSlotCensusV2:
        _consume_slot_census(state, payload=payload, carrier_ingest_seq=carrier_ingest_seq)
        return
    if type(payload) is PublicOiRestForwardGapRangeV2:
        _consume_forward_gap(state, payload=payload, carrier_ingest_seq=carrier_ingest_seq)
        return
    if type(payload) is PublicOiRestCoverageCloseV2:
        _consume_coverage_close(state, payload=payload, carrier_ingest_seq=carrier_ingest_seq)
        return
    raise TypeError("public OI census payload has a non-exact union member")


def _consume_slot_census(
    state: _VerificationStateV2,
    *,
    payload: PublicOiRestSlotCensusV2,
    carrier_ingest_seq: int,
) -> None:
    _start_segment(state, payload.scheduled_slot_wall_ms)
    if (
        state.pending_slot_wall_ms is not None
        and state.pending_slot_wall_ms != payload.scheduled_slot_wall_ms
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "the slot census differs from its pending attempt slot"
        )
    referenced_sequences: set[int] = set()
    retained_count = 0
    for entry in payload.entries:
        pending = state.pending_by_ordinal.get(entry.symbol_ordinal)
        if entry.outcome is PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED:
            if pending is None:
                raise PublicOiRestCensusVerificationErrorV2(
                    "a slot census references a missing or foreign OI attempt"
                )
            if (
                entry.attempt_ingest_seq != pending.ingest_seq
                or entry.attempt_record_sha256 != pending.encoded_sha256
                or pending.ingest_seq >= carrier_ingest_seq
                or pending.scheduled_slot_wall_ms != payload.scheduled_slot_wall_ms
                or pending.symbol_ordinal != entry.symbol_ordinal
                or pending.symbol != payload.symbols[entry.symbol_ordinal]
                or pending.receipt_wall_ms > payload.closed_wall_ms
                or pending.receipt_monotonic_ns > payload.closed_monotonic_ns
            ):
                raise PublicOiRestCensusVerificationErrorV2(
                    "a slot census attempt reference or terminal clocks differ from its "
                    "preceding canonical line"
                )
            if pending.ingest_seq in referenced_sequences:
                raise PublicOiRestCensusVerificationErrorV2(
                    "a slot census references one OI attempt more than once"
                )
            referenced_sequences.add(pending.ingest_seq)
            retained_count += 1
        elif pending is not None:
            raise PublicOiRestCensusVerificationErrorV2(
                "a retained OI attempt is orphaned by an unstarted census outcome"
            )
    if len(referenced_sequences) != len(state.pending_by_ordinal):
        raise PublicOiRestCensusVerificationErrorV2(
            "the slot census leaves one or more OI attempts unreferenced"
        )
    state.pending_by_ordinal.clear()
    state.pending_slot_wall_ms = None
    state.pending_poll_cycle_seq = None
    state.coverage_cursor_wall_ms = (
        payload.scheduled_slot_wall_ms + PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
    )
    state.last_census_ingest_seq = carrier_ingest_seq
    state.slot_census_record_count += 1
    state.covered_slot_count += 1
    state.scheduled_cell_count += len(payload.entries)
    state.attempt_retained_cell_count += retained_count
    state.unstarted_cell_count += len(payload.entries) - retained_count


def _consume_forward_gap(
    state: _VerificationStateV2,
    *,
    payload: PublicOiRestForwardGapRangeV2,
    carrier_ingest_seq: int,
) -> None:
    if state.pending_by_ordinal:
        raise PublicOiRestCensusVerificationErrorV2(
            "a forward-gap carrier would orphan pending public OI attempts"
        )
    _start_segment(state, payload.first_slot_wall_ms)
    state.coverage_cursor_wall_ms = payload.end_slot_exclusive_wall_ms
    state.last_census_ingest_seq = carrier_ingest_seq
    state.forward_gap_record_count += 1
    state.covered_slot_count += payload.covered_slot_count
    cells = payload.covered_slot_count * len(payload.symbols)
    state.scheduled_cell_count += cells
    state.unstarted_cell_count += cells


def _consume_coverage_close(
    state: _VerificationStateV2,
    *,
    payload: PublicOiRestCoverageCloseV2,
    carrier_ingest_seq: int,
) -> None:
    if state.close_payload is not None:
        raise PublicOiRestCensusVerificationErrorV2(
            "the prefix contains more than one OI coverage close"
        )
    if state.pending_by_ordinal:
        raise PublicOiRestCensusVerificationErrorV2(
            "the OI coverage close would orphan pending attempts"
        )
    if state.coverage_start_slot_wall_ms is None:
        state.coverage_start_slot_wall_ms = payload.coverage_start_slot_wall_ms
        state.coverage_cursor_wall_ms = payload.coverage_start_slot_wall_ms
    if (
        payload.coverage_start_slot_wall_ms != state.coverage_start_slot_wall_ms
        or payload.coverage_end_slot_exclusive_wall_ms != state.coverage_cursor_wall_ms
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "slot and gap carriers do not form the close's exact contiguous coverage"
        )
    if payload.last_census_ingest_seq != state.last_census_ingest_seq:
        raise PublicOiRestCensusVerificationErrorV2(
            "coverage close last-census reference differs from the exact preceding carrier"
        )
    state.close_payload = payload
    state.close_ingest_seq = carrier_ingest_seq


def _start_segment(state: _VerificationStateV2, segment_start_wall_ms: int) -> None:
    if state.coverage_start_slot_wall_ms is None:
        if (
            state.pending_slot_wall_ms is not None
            and state.pending_slot_wall_ms != segment_start_wall_ms
        ):
            raise PublicOiRestCensusVerificationErrorV2(
                "the first census segment differs from its pending attempt slot"
            )
        state.coverage_start_slot_wall_ms = segment_start_wall_ms
        state.coverage_cursor_wall_ms = segment_start_wall_ms
    if state.coverage_cursor_wall_ms != segment_start_wall_ms:
        raise PublicOiRestCensusVerificationErrorV2(
            "public OI census segments overlap or leave a coverage gap"
        )


def _parse_and_validate_census_record(
    record: RawRecordV2,
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
) -> PublicOiRestCensusPayloadV2:
    if (
        record.session_id != session_id
        or record.protocol_hash != protocol_hash
        or record.connection_id != _CENSUS_CONNECTION_ID
        or record.generation != 1
        or record.source_logical_key != _CENSUS_SOURCE_LOGICAL_KEY
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "a census carrier has the wrong fixed outer identity"
        )
    payload = _parse_census_payload(record.payload_bytes(), plan=plan)
    if (
        payload.session_id != session_id
        or payload.session_start_manifest_sha256 != session_start_manifest_sha256
        or payload.plan_bundle_sha256 != plan_bundle_sha256
        or payload.plan_id != plan.name
        or payload.route_id != plan.route_id
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "a census carrier differs from the exact session, manifest, or plan bundle"
        )
    if type(payload) is PublicOiRestSlotCensusV2:
        terminal_wall_ms = payload.closed_wall_ms
        terminal_monotonic_ns = payload.closed_monotonic_ns
    elif type(payload) is PublicOiRestForwardGapRangeV2:
        terminal_wall_ms = payload.observed_wall_ms
        terminal_monotonic_ns = payload.observed_monotonic_ns
    elif type(payload) is PublicOiRestCoverageCloseV2:
        terminal_wall_ms = payload.stop_requested_wall_ms
        terminal_monotonic_ns = payload.stop_requested_monotonic_ns
    else:
        raise TypeError("census parser returned a non-exact union member")
    if (
        record.receipt_wall_ms < terminal_wall_ms
        or record.receipt_monotonic_ns < terminal_monotonic_ns
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "a census outer receipt precedes its terminal schedule evidence"
        )
    return payload


def _parse_census_payload(
    encoded: bytes,
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> PublicOiRestCensusPayloadV2:
    if (
        type(encoded) is not bytes
        or not encoded
        or len(encoded) > PUBLIC_OI_REST_CENSUS_MAX_CANONICAL_BYTES_V2
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "a census carrier exceeds its exact bounded canonical payload size"
        )
    try:
        document = json.loads(encoded)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise PublicOiRestCensusVerificationErrorV2("a census carrier is not valid JSON") from exc
    if type(document) is not dict:
        raise PublicOiRestCensusVerificationErrorV2("a census carrier payload must be an object")
    schema = cast(dict[str, object], document).get("schema_version")
    if schema == _SLOT_SCHEMA:
        return PublicOiRestSlotCensusV2.from_canonical_bytes(encoded, plan=plan)
    if schema == _GAP_SCHEMA:
        return PublicOiRestForwardGapRangeV2.from_canonical_bytes(encoded, plan=plan)
    if schema == _CLOSE_SCHEMA:
        return PublicOiRestCoverageCloseV2.from_canonical_bytes(encoded, plan=plan)
    raise PublicOiRestCensusVerificationErrorV2("a census carrier has an unsupported exact schema")


def _normalize_prefix_record(
    value: RawRecordV2 | QueuedRawRecordV2,
) -> tuple[RawRecordV2, bytes]:
    if type(value) is RawRecordV2:
        value.__post_init__()
        encoded_line = canonical_json_line(value)
        return value, encoded_line
    if type(value) is not QueuedRawRecordV2:
        raise TypeError("prefix entries must be exact RawRecordV2 or QueuedRawRecordV2 values")
    value.verify_integrity()
    parsed = parse_raw_record_line_v2(value.encoded_line)
    if (
        parsed != value.record
        or canonical_json_line(parsed) != value.encoded_line
        or value.encoded_sha256 != hashlib.sha256(value.encoded_line).hexdigest()
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "a queued prefix line is not the exact canonical raw record"
        )
    return parsed, value.encoded_line


def _validate_outer_receipt_order(
    record: RawRecordV2,
    *,
    previous_wall_ms: int | None,
    previous_monotonic_ns: int | None,
) -> None:
    if previous_wall_ms is not None and record.receipt_wall_ms < previous_wall_ms:
        raise PublicOiRestCensusVerificationErrorV2(
            "prefix outer receipt wall time moved backwards"
        )
    if previous_monotonic_ns is not None and record.receipt_monotonic_ns < previous_monotonic_ns:
        raise PublicOiRestCensusVerificationErrorV2(
            "prefix outer receipt monotonic time moved backwards"
        )


def _update_prefix_digest(digest: object, ingest_seq: int, encoded_line: bytes) -> None:
    update = getattr(digest, "update", None)
    if not callable(update):
        raise TypeError("prefix digest lacks an update operation")
    update(struct.pack(">Q", ingest_seq))
    update(struct.pack(">Q", len(encoded_line)))
    update(encoded_line)


def _validate_verifier_authority_inputs(
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
    finality_receipt: CaptureFinalityFenceReceiptV2,
) -> None:
    if type(plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise TypeError("census verification requires the exact promoting REST plan")
    plan.__post_init__()
    _require_identity(session_id, "session_id")
    _require_sha256(protocol_hash, "protocol_hash")
    _require_sha256(session_start_manifest_sha256, "session_start_manifest_sha256")
    _require_sha256(plan_bundle_sha256, "plan_bundle_sha256")
    if type(finality_receipt) is not CaptureFinalityFenceReceiptV2:
        raise TypeError("finality_receipt must be an exact CaptureFinalityFenceReceiptV2")
    finality_receipt.__post_init__()


def _validate_certificate_material(
    certificate: PublicOiRestCensusVerificationCertificateV2,
    *,
    verify_digest: bool,
) -> None:
    _require_identity(certificate.session_id, "session_id")
    for name in (
        "protocol_hash",
        "session_start_manifest_sha256",
        "plan_bundle_sha256",
        "rest_plan_sha256",
        "symbol_census_sha256",
        "finality_receipt_sha256",
        "finality_authority_sha256",
        "finality_exact_prefix_sha256",
        "finality_prefix_proof_sha256",
    ):
        _require_sha256(getattr(certificate, name), name)
    _require_identity(certificate.plan_id, "plan_id")
    _require_positive_int(certificate.symbol_count, "symbol_count")
    if certificate.symbol_count > PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2:
        raise PublicOiRestCensusVerificationErrorV2("certificate symbol count exceeds 32")
    for name in (
        "verified_prefix_tail_ingest_seq",
        "verified_record_count",
        "coverage_close_ingest_seq",
    ):
        _require_positive_int(getattr(certificate, name), name)
    for name in (
        "coverage_start_slot_wall_ms",
        "coverage_end_slot_exclusive_wall_ms",
        "rest_attempt_record_count",
        "slot_census_record_count",
        "forward_gap_record_count",
        "ignored_websocket_record_count",
        "covered_slot_count",
        "scheduled_cell_count",
        "attempt_retained_cell_count",
        "unstarted_cell_count",
    ):
        _require_nonnegative_int(getattr(certificate, name), name)
    if certificate.last_census_ingest_seq is not None:
        _require_positive_int(certificate.last_census_ingest_seq, "last_census_ingest_seq")
        if certificate.last_census_ingest_seq >= certificate.coverage_close_ingest_seq:
            raise PublicOiRestCensusVerificationErrorV2(
                "certificate last census must precede its coverage close"
            )
    if (
        certificate.verified_prefix_tail_ingest_seq != certificate.verified_record_count
        or certificate.coverage_close_ingest_seq > certificate.verified_prefix_tail_ingest_seq
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "certificate prefix count or close/tail ordering differs"
        )
    for name, value in (
        ("coverage_start_slot_wall_ms", certificate.coverage_start_slot_wall_ms),
        (
            "coverage_end_slot_exclusive_wall_ms",
            certificate.coverage_end_slot_exclusive_wall_ms,
        ),
    ):
        if value % PUBLIC_OI_REST_POLL_INTERVAL_MS_V2 != 0:
            raise PublicOiRestCensusVerificationErrorV2(
                f"certificate {name} is not a 5-second UTC boundary"
            )
    if certificate.coverage_end_slot_exclusive_wall_ms < certificate.coverage_start_slot_wall_ms:
        raise PublicOiRestCensusVerificationErrorV2("certificate coverage end precedes its start")
    exact_slots = (
        certificate.coverage_end_slot_exclusive_wall_ms - certificate.coverage_start_slot_wall_ms
    ) // PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
    if exact_slots != certificate.covered_slot_count:
        raise PublicOiRestCensusVerificationErrorV2(
            "certificate covered-slot count differs from its half-open interval"
        )
    if (
        certificate.scheduled_cell_count
        != certificate.covered_slot_count * certificate.symbol_count
        or certificate.rest_attempt_record_count != certificate.attempt_retained_cell_count
        or certificate.unstarted_cell_count
        != certificate.scheduled_cell_count - certificate.attempt_retained_cell_count
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "certificate attempt and scheduled-cell counters differ"
        )
    if certificate.covered_slot_count == 0:
        if certificate.last_census_ingest_seq is not None:
            raise PublicOiRestCensusVerificationErrorV2(
                "empty coverage cannot reference a census carrier"
            )
    elif certificate.last_census_ingest_seq is None:
        raise PublicOiRestCensusVerificationErrorV2(
            "non-empty coverage requires its last census carrier"
        )
    if (
        certificate.coverage_closed is not True
        or certificate.data_complete is not False
        or certificate.data_completeness_reason != BODY_SEMANTICS_UNVERIFIED_V2
        or certificate.m2_certified is not False
        or certificate.session_close_authorized is not False
        or certificate.current_storage_reproved is not False
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            "census verification may not claim body completeness, M2, closure, or storage reproof"
        )
    if certificate.schema_version != _CERTIFICATE_SCHEMA:
        raise PublicOiRestCensusVerificationErrorV2(
            "unsupported public OI census verification certificate schema"
        )
    if verify_digest:
        _require_sha256(certificate.certificate_sha256, "certificate_sha256")
        if not hmac.compare_digest(
            certificate.certificate_sha256, _certificate_sha256(certificate)
        ):
            raise PublicOiRestCensusVerificationErrorV2(
                "public OI census verification certificate hash differs"
            )


def _certificate_sha256(
    certificate: PublicOiRestCensusVerificationCertificateV2,
) -> str:
    document = {
        model_field.name: getattr(certificate, model_field.name)
        for model_field in fields(certificate)
        if model_field.name not in {"certificate_sha256", "_factory_seal"}
    }
    return hashlib.sha256(_CERTIFICATE_DOMAIN + canonical_json_line(document)).hexdigest()


def _require_sha256(value: object, field_name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise PublicOiRestCensusVerificationErrorV2(
            f"{field_name} must be a lowercase SHA-256 digest"
        )


def _require_identity(value: object, field_name: str) -> None:
    if (
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > 256
        or any(character in value for character in "\r\n\x00")
    ):
        raise PublicOiRestCensusVerificationErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )


def _require_positive_int(value: object, field_name: str) -> None:
    if type(value) is not int or value < 1:
        raise PublicOiRestCensusVerificationErrorV2(f"{field_name} must be a positive integer")


def _require_nonnegative_int(value: object, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise PublicOiRestCensusVerificationErrorV2(f"{field_name} must be a nonnegative integer")
