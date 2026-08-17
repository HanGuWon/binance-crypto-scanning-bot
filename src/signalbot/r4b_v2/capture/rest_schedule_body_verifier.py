from __future__ import annotations

import hashlib
import hmac
import re
import struct
from collections.abc import Iterable
from dataclasses import InitVar, dataclass, field, fields
from typing import Final, Literal

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.batching import QueuedRawRecordV2
from signalbot.r4b_v2.capture.blocks import BlockIntegrityError, parse_raw_record_line_v2
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import CaptureFinalityFenceReceiptV2
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2
from signalbot.r4b_v2.capture.rest import (
    PUBLIC_OI_REST_POLL_INTERVAL_MS_V2,
    PublicOiRestAttemptPayloadV2,
)
from signalbot.r4b_v2.capture.rest_census import (
    public_oi_rest_attempt_record_sha256_v2,
    public_oi_rest_plan_sha256_v2,
    public_oi_rest_symbol_census_sha256_v2,
)
from signalbot.r4b_v2.capture.rest_census_verifier import (
    PublicOiRestCensusVerificationCertificateV2,
    validate_public_oi_rest_census_verification_certificate_v2,
)
from signalbot.r4b_v2.capture.rest_open_interest_semantics import (
    PUBLIC_OI_REST_BODY_SEMANTIC_SCOPE_V2,
    VerifiedPublicOiRestBodyV2,
    verify_public_oi_rest_attempt_body_v2,
)

PUBLIC_OI_SCHEDULE_BODY_VERIFICATION_SCOPE_V2: Final = (
    "EXACT_FINALIZED_PUBLIC_OI_LOCAL_SCHEDULE_ONLY"
)
BINANCE_TRANSACTION_TIME_CAUSAL_BOUND_REASON_V2: Final = (
    "BINANCE_SERVER_CLOCK_OFFSET_UNVERIFIED"
)

_CERTIFICATE_SCHEMA = "r4b_v2_public_oi_schedule_body_verification_certificate_v1"
_REST_ROUTE_ID = "usdm_public_rest"
_WAL_BLOCK_PREFIX_DOMAIN = b"R4B_V2_WAL_BLOCK_PREFIX\0"
_BODY_BINDING_SET_DOMAIN = b"R4B_V2_PUBLIC_OI_BODY_BINDING_SET\0"
_CERTIFICATE_DOMAIN = b"R4B_V2_PUBLIC_OI_SCHEDULE_BODY_VERIFICATION\0"
_CERTIFICATE_FACTORY_TOKEN = object()
_VERIFIER_FACTORY_TOKEN = object()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class PublicOiScheduleBodyVerificationErrorV2(ValueError):
    """The finalized schedule/body evidence does not prove the narrow claim."""


@dataclass(frozen=True, slots=True)
class PublicOiScheduleBodyVerificationCertificateV2:
    """Factory-sealed proof for retained bodies in one exact local OI schedule.

    This deliberately is not a general data-completeness certificate.  In
    particular, Binance's response ``time`` and local receipt clocks have no
    authenticated clock-offset bound in V2, so this value can never establish
    freshness or transaction-time causality.  It also says nothing about
    WebSocket coverage, M2, session closure, storage reproof, or profitability.
    """

    observed_schedule_certificate_sha256: str
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
    covered_slot_count: int
    slot_census_record_count: int
    rest_attempt_record_count: int
    scheduled_cell_count: int
    attempt_retained_cell_count: int
    verified_body_count: int
    ignored_websocket_record_count: int
    verified_body_bindings_sha256: str
    verification_scope: Literal[
        "EXACT_FINALIZED_PUBLIC_OI_LOCAL_SCHEDULE_ONLY"
    ]
    body_verification_scope: Literal["SINGLE_COMPLETE_HTTP_200_BODY_ONLY"]
    schedule_body_complete: Literal[True]
    body_semantics_verified: Literal[True]
    freshness_verified: Literal[False]
    transaction_time_causally_bounded: Literal[False]
    transaction_time_causal_bound_reason: Literal[
        "BINANCE_SERVER_CLOCK_OFFSET_UNVERIFIED"
    ]
    websocket_completeness_verified: Literal[False]
    m2_certified: Literal[False]
    session_close_authorized: Literal[False]
    profitability_verified: Literal[False]
    current_storage_reproved: Literal[False]
    schema_version: Literal[
        "r4b_v2_public_oi_schedule_body_verification_certificate_v1"
    ] = _CERTIFICATE_SCHEMA
    certificate_sha256: str = field(init=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _CERTIFICATE_FACTORY_TOKEN:
            raise TypeError("public OI schedule/body certificates are factory-sealed")
        object.__setattr__(self, "_factory_seal", _CERTIFICATE_FACTORY_TOKEN)
        _validate_certificate_material(self, verify_digest=False)
        object.__setattr__(self, "certificate_sha256", _certificate_sha256(self))


class PublicOiScheduleBodyPrefixVerifierV2:
    """One-shot push verifier for bodies in one exact finalized schedule prefix.

    Instances are factory-created. ``consume`` matches the callback accepted by
    ``GroupedBlockWriterV2`` and retains only fixed authority, counters, receipt
    clocks, and two incremental digests. Any failed operation is terminal.
    """

    __slots__ = (
        "_body_binding_digest",
        "_expected_ingest_seq",
        "_finality_receipt",
        "_ignored_websocket_record_count",
        "_last_receipt_monotonic_ns",
        "_last_receipt_wall_ms",
        "_lifecycle",
        "_observed_schedule_certificate",
        "_plan",
        "_plan_bundle_sha256",
        "_prefix_digest",
        "_protocol_hash",
        "_session_id",
        "_session_start_manifest_sha256",
        "_verified_body_count",
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
        observed_schedule_certificate: PublicOiRestCensusVerificationCertificateV2,
        _factory_token: object | None = None,
    ) -> None:
        if _factory_token is not _VERIFIER_FACTORY_TOKEN:
            raise TypeError(
                "public OI schedule/body prefix verifiers must be created by "
                "their factory"
            )
        _validate_verifier_authority_inputs(
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            session_start_manifest_sha256=session_start_manifest_sha256,
            plan_bundle_sha256=plan_bundle_sha256,
            finality_receipt=finality_receipt,
            observed_schedule_certificate=observed_schedule_certificate,
        )
        self._plan = plan
        self._session_id = session_id
        self._protocol_hash = protocol_hash
        self._session_start_manifest_sha256 = session_start_manifest_sha256
        self._plan_bundle_sha256 = plan_bundle_sha256
        self._finality_receipt = finality_receipt
        self._observed_schedule_certificate = observed_schedule_certificate
        self._prefix_digest = hashlib.sha256(_WAL_BLOCK_PREFIX_DOMAIN)
        self._body_binding_digest = hashlib.sha256(_BODY_BINDING_SET_DOMAIN)
        self._expected_ingest_seq = 1
        self._verified_body_count = 0
        self._ignored_websocket_record_count = 0
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

    def finalize(self) -> PublicOiScheduleBodyVerificationCertificateV2:
        """Close this one-shot verifier and return its factory-sealed proof."""

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
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the callback line is not a valid raw-record JSONL envelope"
            ) from exc
        if canonical_json_line(record) != encoded_line:
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the callback line is not the exact canonical raw record"
            )
        if record.ingest_seq != ingest_seq:
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the callback ingest sequence differs from its encoded raw record"
            )
        if self._expected_ingest_seq > self._finality_receipt.fence_ingest_seq:
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the supplied prefix extends beyond the finality-fence tail"
            )
        if ingest_seq != self._expected_ingest_seq:
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the supplied prefix must contain exact ingest sequences 1..tail"
            )
        if record.session_id != self._session_id or record.protocol_hash != self._protocol_hash:
            raise PublicOiScheduleBodyVerificationErrorV2(
                "a prefix record differs from the exact session or protocol lineage"
            )
        _update_length_delimited_digest(
            self._prefix_digest,
            sequence=ingest_seq,
            encoded=encoded_line,
        )
        if record.transport is TransportV2.HTTPS and record.symbol is not None:
            payload, verified_body = _verify_attempt_body(
                record,
                encoded_line=encoded_line,
                plan=self._plan,
            )
            _update_verified_body_binding_digest(
                self._body_binding_digest,
                record=record,
                encoded_line=encoded_line,
                payload=payload,
                verified_body=verified_body,
            )
            self._verified_body_count += 1
        elif record.transport is TransportV2.WEBSOCKET:
            self._ignored_websocket_record_count += 1
        self._last_receipt_wall_ms = record.receipt_wall_ms
        self._last_receipt_monotonic_ns = record.receipt_monotonic_ns
        self._expected_ingest_seq += 1

    def _finalize_open(self) -> PublicOiScheduleBodyVerificationCertificateV2:
        verified_record_count = self._expected_ingest_seq - 1
        if verified_record_count != self._finality_receipt.fence_ingest_seq:
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the supplied prefix does not reach the exact finality-fence tail"
            )
        if self._last_receipt_wall_ms is None or self._last_receipt_monotonic_ns is None:
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the finalized prefix is empty"
            )
        if not hmac.compare_digest(
            self._prefix_digest.hexdigest(),
            self._finality_receipt.exact_prefix_sha256,
        ):
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the supplied prefix differs from the schedule certificate and finality"
            )
        if (
            self._last_receipt_wall_ms
            != self._finality_receipt.target_last_receipt_wall_ms
            or self._last_receipt_monotonic_ns
            != self._finality_receipt.target_last_receipt_monotonic_ns
        ):
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the finality target receipt clocks differ from the exact prefix tail"
            )
        schedule = self._observed_schedule_certificate
        if self._verified_body_count != schedule.rest_attempt_record_count:
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the semantically verified body count differs from the observed schedule"
            )
        if self._ignored_websocket_record_count != schedule.ignored_websocket_record_count:
            raise PublicOiScheduleBodyVerificationErrorV2(
                "the prefix WebSocket count differs from the observed schedule"
            )
        certificate = PublicOiScheduleBodyVerificationCertificateV2(
            observed_schedule_certificate_sha256=schedule.certificate_sha256,
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
            finality_exact_prefix_sha256=self._finality_receipt.exact_prefix_sha256,
            finality_prefix_proof_sha256=self._finality_receipt.prefix_proof_sha256,
            verified_prefix_tail_ingest_seq=self._finality_receipt.fence_ingest_seq,
            verified_record_count=verified_record_count,
            coverage_start_slot_wall_ms=schedule.coverage_start_slot_wall_ms,
            coverage_end_slot_exclusive_wall_ms=(
                schedule.coverage_end_slot_exclusive_wall_ms
            ),
            coverage_close_ingest_seq=schedule.coverage_close_ingest_seq,
            covered_slot_count=schedule.covered_slot_count,
            slot_census_record_count=schedule.slot_census_record_count,
            rest_attempt_record_count=schedule.rest_attempt_record_count,
            scheduled_cell_count=schedule.scheduled_cell_count,
            attempt_retained_cell_count=schedule.attempt_retained_cell_count,
            verified_body_count=self._verified_body_count,
            ignored_websocket_record_count=self._ignored_websocket_record_count,
            verified_body_bindings_sha256=self._body_binding_digest.hexdigest(),
            verification_scope=PUBLIC_OI_SCHEDULE_BODY_VERIFICATION_SCOPE_V2,
            body_verification_scope=PUBLIC_OI_REST_BODY_SEMANTIC_SCOPE_V2,
            schedule_body_complete=True,
            body_semantics_verified=True,
            freshness_verified=False,
            transaction_time_causally_bounded=False,
            transaction_time_causal_bound_reason=(
                BINANCE_TRANSACTION_TIME_CAUSAL_BOUND_REASON_V2
            ),
            websocket_completeness_verified=False,
            m2_certified=False,
            session_close_authorized=False,
            profitability_verified=False,
            current_storage_reproved=False,
            _factory_token=_CERTIFICATE_FACTORY_TOKEN,
        )
        validate_public_oi_schedule_body_verification_certificate_v2(certificate)
        return certificate

    def _require_open(self, operation: str) -> None:
        if self._lifecycle != "OPEN":
            raise PublicOiScheduleBodyVerificationErrorV2(
                f"cannot {operation}: the one-shot verifier is {self._lifecycle.lower()}"
            )


def create_public_oi_schedule_body_prefix_verifier_v2(
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    observed_schedule_certificate: PublicOiRestCensusVerificationCertificateV2,
) -> PublicOiScheduleBodyPrefixVerifierV2:
    """Validate exact authority inputs and create one empty push verifier."""

    return PublicOiScheduleBodyPrefixVerifierV2(
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        session_start_manifest_sha256=session_start_manifest_sha256,
        plan_bundle_sha256=plan_bundle_sha256,
        finality_receipt=finality_receipt,
        observed_schedule_certificate=observed_schedule_certificate,
        _factory_token=_VERIFIER_FACTORY_TOKEN,
    )


def verify_public_oi_schedule_bodies_v2(
    records: Iterable[RawRecordV2 | QueuedRawRecordV2],
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    observed_schedule_certificate: PublicOiRestCensusVerificationCertificateV2,
) -> PublicOiScheduleBodyVerificationCertificateV2:
    """Stream exact prefix bytes through the same one-shot push owner."""

    if not isinstance(records, Iterable):
        raise TypeError("records must be an ordered iterable prefix")
    verifier = create_public_oi_schedule_body_prefix_verifier_v2(
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        session_start_manifest_sha256=session_start_manifest_sha256,
        plan_bundle_sha256=plan_bundle_sha256,
        finality_receipt=finality_receipt,
        observed_schedule_certificate=observed_schedule_certificate,
    )
    for value in records:
        record, encoded_line = _normalize_prefix_record(value)
        verifier.consume(record.ingest_seq, encoded_line)
    return verifier.finalize()


def validate_public_oi_schedule_body_verification_certificate_v2(
    certificate: PublicOiScheduleBodyVerificationCertificateV2,
) -> str:
    """Revalidate factory provenance and return the deterministic certificate hash."""

    if type(certificate) is not PublicOiScheduleBodyVerificationCertificateV2:
        raise TypeError(
            "certificate must be an exact "
            "PublicOiScheduleBodyVerificationCertificateV2"
        )
    if getattr(certificate, "_factory_seal", None) is not _CERTIFICATE_FACTORY_TOKEN:
        raise PublicOiScheduleBodyVerificationErrorV2(
            "public OI schedule/body certificate lacks verifier provenance"
        )
    _validate_certificate_material(certificate, verify_digest=True)
    return certificate.certificate_sha256


def _validate_verifier_authority_inputs(
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    observed_schedule_certificate: PublicOiRestCensusVerificationCertificateV2,
) -> None:
    if type(plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise TypeError("body verification requires the exact promoting REST plan")
    plan.__post_init__()
    _require_identity(session_id, "session_id")
    for name, value in (
        ("protocol_hash", protocol_hash),
        ("session_start_manifest_sha256", session_start_manifest_sha256),
        ("plan_bundle_sha256", plan_bundle_sha256),
    ):
        _require_sha256(value, name)
    if type(finality_receipt) is not CaptureFinalityFenceReceiptV2:
        raise TypeError("finality_receipt must be an exact CaptureFinalityFenceReceiptV2")
    finality_receipt.__post_init__()
    validate_public_oi_rest_census_verification_certificate_v2(
        observed_schedule_certificate
    )

    expected_bindings = (
        (observed_schedule_certificate.session_id, session_id, "session_id"),
        (
            observed_schedule_certificate.protocol_hash,
            protocol_hash,
            "protocol_hash",
        ),
        (
            observed_schedule_certificate.session_start_manifest_sha256,
            session_start_manifest_sha256,
            "session_start_manifest_sha256",
        ),
        (
            observed_schedule_certificate.plan_bundle_sha256,
            plan_bundle_sha256,
            "plan_bundle_sha256",
        ),
        (observed_schedule_certificate.plan_id, plan.name, "plan_id"),
        (
            observed_schedule_certificate.rest_plan_sha256,
            public_oi_rest_plan_sha256_v2(plan),
            "rest_plan_sha256",
        ),
        (
            observed_schedule_certificate.symbol_census_sha256,
            public_oi_rest_symbol_census_sha256_v2(plan),
            "symbol_census_sha256",
        ),
        (
            observed_schedule_certificate.symbol_count,
            len(plan.symbols),
            "symbol_count",
        ),
        (
            observed_schedule_certificate.finality_receipt_sha256,
            finality_receipt.sha256,
            "finality_receipt_sha256",
        ),
        (
            observed_schedule_certificate.finality_authority_sha256,
            finality_receipt.authority_sha256,
            "finality_authority_sha256",
        ),
        (
            observed_schedule_certificate.finality_exact_prefix_sha256,
            finality_receipt.exact_prefix_sha256,
            "finality_exact_prefix_sha256",
        ),
        (
            observed_schedule_certificate.finality_prefix_proof_sha256,
            finality_receipt.prefix_proof_sha256,
            "finality_prefix_proof_sha256",
        ),
        (
            observed_schedule_certificate.verified_prefix_tail_ingest_seq,
            finality_receipt.fence_ingest_seq,
            "verified_prefix_tail_ingest_seq",
        ),
        (
            observed_schedule_certificate.verified_record_count,
            finality_receipt.fence_ingest_seq,
            "verified_record_count",
        ),
    )
    for observed, expected, field_name in expected_bindings:
        if observed != expected:
            raise PublicOiScheduleBodyVerificationErrorV2(
                f"observed schedule {field_name} differs from its exact authority"
            )

    if (
        observed_schedule_certificate.covered_slot_count < 1
        or observed_schedule_certificate.scheduled_cell_count < 1
    ):
        raise PublicOiScheduleBodyVerificationErrorV2(
            "vacuous empty schedule coverage cannot prove body completeness"
        )
    if (
        observed_schedule_certificate.forward_gap_record_count != 0
        or observed_schedule_certificate.unstarted_cell_count != 0
        or observed_schedule_certificate.slot_census_record_count
        != observed_schedule_certificate.covered_slot_count
        or observed_schedule_certificate.attempt_retained_cell_count
        != observed_schedule_certificate.scheduled_cell_count
        or observed_schedule_certificate.rest_attempt_record_count
        != observed_schedule_certificate.scheduled_cell_count
    ):
        raise PublicOiScheduleBodyVerificationErrorV2(
            "observed schedule contains gaps, omissions, or non-retained cells"
        )


def _verify_attempt_body(
    record: RawRecordV2,
    *,
    encoded_line: bytes,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> tuple[PublicOiRestAttemptPayloadV2, VerifiedPublicOiRestBodyV2]:
    symbol = record.symbol
    assert symbol is not None
    if (
        record.venue is not VenueV2.USDM_FUTURES
        or record.plan_id != plan.name
        or record.route_id != _REST_ROUTE_ID
        or record.frame_seq is not None
        or symbol not in plan.symbols
        or record.source_logical_key != f"openInterest:{symbol}"
    ):
        raise PublicOiScheduleBodyVerificationErrorV2(
            "a public OI body carrier has a foreign or malformed outer identity"
        )
    try:
        payload = PublicOiRestAttemptPayloadV2.from_canonical_bytes(
            record.payload_bytes(), plan=plan
        )
    except (TypeError, ValueError) as exc:
        raise PublicOiScheduleBodyVerificationErrorV2(
            "a public OI attempt payload is not exact canonical plan evidence"
        ) from exc
    if (
        payload.symbol != symbol
        or payload.symbol_ordinal != plan.symbols.index(symbol)
        or payload.attempt != 1
        or payload.completion_admission_wall_ms != record.receipt_wall_ms
        or payload.completion_admission_monotonic_ns != record.receipt_monotonic_ns
    ):
        raise PublicOiScheduleBodyVerificationErrorV2(
            "a public OI attempt differs from its ordinal, attempt, or receipt clocks"
        )
    if not hmac.compare_digest(
        public_oi_rest_attempt_record_sha256_v2(record),
        hashlib.sha256(encoded_line).hexdigest(),
    ):
        raise PublicOiScheduleBodyVerificationErrorV2(
            "a public OI attempt line differs from its canonical record hash"
        )
    try:
        verified_body = verify_public_oi_rest_attempt_body_v2(payload)
    except (TypeError, ValueError) as exc:
        raise PublicOiScheduleBodyVerificationErrorV2(
            "a retained scheduled public OI body fails strict semantic verification"
        ) from exc
    return payload, verified_body


def _update_verified_body_binding_digest(
    digest: object,
    *,
    record: RawRecordV2,
    encoded_line: bytes,
    payload: PublicOiRestAttemptPayloadV2,
    verified_body: VerifiedPublicOiRestBodyV2,
) -> None:
    row = canonical_json_line(
        {
            "ingest_seq": record.ingest_seq,
            "attempt_record_sha256": hashlib.sha256(encoded_line).hexdigest(),
            "symbol": verified_body.symbol,
            "symbol_ordinal": payload.symbol_ordinal,
            "scheduled_slot_wall_ms": payload.scheduled_slot_wall_ms,
            "poll_cycle_seq": payload.poll_cycle_seq,
            "body_sha256": verified_body.body_sha256,
            # Binance documents a signed-int64 domain, which is wider than the
            # RFC 8785 interoperable integer domain used by canonical_json_line.
            # Decimal text preserves the exact value without narrowing it.
            "transaction_time_ms_text": str(verified_body.transaction_time_ms),
            "open_interest_text": verified_body.open_interest_text,
        }
    )
    _update_length_delimited_digest(digest, sequence=record.ingest_seq, encoded=row)


def _normalize_prefix_record(
    value: RawRecordV2 | QueuedRawRecordV2,
) -> tuple[RawRecordV2, bytes]:
    if type(value) is RawRecordV2:
        value.__post_init__()
        return value, canonical_json_line(value)
    if type(value) is not QueuedRawRecordV2:
        raise TypeError("prefix entries must be exact RawRecordV2 or QueuedRawRecordV2 values")
    value.verify_integrity()
    parsed = parse_raw_record_line_v2(value.encoded_line)
    if (
        parsed != value.record
        or canonical_json_line(parsed) != value.encoded_line
        or value.encoded_sha256 != hashlib.sha256(value.encoded_line).hexdigest()
    ):
        raise PublicOiScheduleBodyVerificationErrorV2(
            "a queued prefix line is not the exact canonical raw record"
        )
    return parsed, value.encoded_line


def _update_length_delimited_digest(
    digest: object,
    *,
    sequence: int,
    encoded: bytes,
) -> None:
    update = getattr(digest, "update", None)
    if not callable(update):
        raise TypeError("digest lacks an update operation")
    update(struct.pack(">Q", sequence))
    update(struct.pack(">Q", len(encoded)))
    update(encoded)


def _validate_certificate_material(
    certificate: PublicOiScheduleBodyVerificationCertificateV2,
    *,
    verify_digest: bool,
) -> None:
    _require_identity(certificate.session_id, "session_id")
    _require_identity(certificate.plan_id, "plan_id")
    for name in (
        "observed_schedule_certificate_sha256",
        "protocol_hash",
        "session_start_manifest_sha256",
        "plan_bundle_sha256",
        "rest_plan_sha256",
        "symbol_census_sha256",
        "finality_receipt_sha256",
        "finality_authority_sha256",
        "finality_exact_prefix_sha256",
        "finality_prefix_proof_sha256",
        "verified_body_bindings_sha256",
    ):
        _require_sha256(getattr(certificate, name), name)
    for name in (
        "symbol_count",
        "verified_prefix_tail_ingest_seq",
        "verified_record_count",
        "coverage_close_ingest_seq",
        "covered_slot_count",
        "slot_census_record_count",
        "rest_attempt_record_count",
        "scheduled_cell_count",
        "attempt_retained_cell_count",
        "verified_body_count",
    ):
        _require_positive_int(getattr(certificate, name), name)
    _require_nonnegative_int(
        certificate.ignored_websocket_record_count,
        "ignored_websocket_record_count",
    )
    for name in (
        "coverage_start_slot_wall_ms",
        "coverage_end_slot_exclusive_wall_ms",
    ):
        value = getattr(certificate, name)
        _require_nonnegative_int(value, name)
        if value % PUBLIC_OI_REST_POLL_INTERVAL_MS_V2 != 0:
            raise PublicOiScheduleBodyVerificationErrorV2(
                f"certificate {name} is not a 5-second UTC boundary"
            )
    if (
        certificate.verified_prefix_tail_ingest_seq != certificate.verified_record_count
        or certificate.coverage_close_ingest_seq
        > certificate.verified_prefix_tail_ingest_seq
    ):
        raise PublicOiScheduleBodyVerificationErrorV2(
            "certificate prefix count or close/tail ordering differs"
        )
    exact_slots = (
        certificate.coverage_end_slot_exclusive_wall_ms
        - certificate.coverage_start_slot_wall_ms
    ) // PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
    if exact_slots != certificate.covered_slot_count:
        raise PublicOiScheduleBodyVerificationErrorV2(
            "certificate coverage interval differs from its covered-slot count"
        )
    if (
        certificate.scheduled_cell_count
        != certificate.covered_slot_count * certificate.symbol_count
        or certificate.slot_census_record_count != certificate.covered_slot_count
        or certificate.rest_attempt_record_count != certificate.scheduled_cell_count
        or certificate.attempt_retained_cell_count
        != certificate.scheduled_cell_count
        or certificate.verified_body_count != certificate.scheduled_cell_count
    ):
        raise PublicOiScheduleBodyVerificationErrorV2(
            "certificate body counts differ from its exact scheduled cells"
        )
    if (
        certificate.verification_scope
        != PUBLIC_OI_SCHEDULE_BODY_VERIFICATION_SCOPE_V2
        or certificate.body_verification_scope
        != PUBLIC_OI_REST_BODY_SEMANTIC_SCOPE_V2
        or certificate.schedule_body_complete is not True
        or certificate.body_semantics_verified is not True
        or certificate.freshness_verified is not False
        or certificate.transaction_time_causally_bounded is not False
        or certificate.transaction_time_causal_bound_reason
        != BINANCE_TRANSACTION_TIME_CAUSAL_BOUND_REASON_V2
        or certificate.websocket_completeness_verified is not False
        or certificate.m2_certified is not False
        or certificate.session_close_authorized is not False
        or certificate.profitability_verified is not False
        or certificate.current_storage_reproved is not False
    ):
        raise PublicOiScheduleBodyVerificationErrorV2(
            "certificate exceeds the narrow schedule/body verification scope"
        )
    if certificate.schema_version != _CERTIFICATE_SCHEMA:
        raise PublicOiScheduleBodyVerificationErrorV2(
            "unsupported public OI schedule/body certificate schema"
        )
    if verify_digest:
        _require_sha256(certificate.certificate_sha256, "certificate_sha256")
        if not hmac.compare_digest(
            certificate.certificate_sha256, _certificate_sha256(certificate)
        ):
            raise PublicOiScheduleBodyVerificationErrorV2(
                "public OI schedule/body certificate hash differs"
            )


def _certificate_sha256(
    certificate: PublicOiScheduleBodyVerificationCertificateV2,
) -> str:
    document = {
        model_field.name: getattr(certificate, model_field.name)
        for model_field in fields(certificate)
        if model_field.name not in {"certificate_sha256", "_factory_seal"}
    }
    return hashlib.sha256(
        _CERTIFICATE_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def _require_sha256(value: object, field_name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise PublicOiScheduleBodyVerificationErrorV2(
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
        raise PublicOiScheduleBodyVerificationErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )


def _require_positive_int(value: object, field_name: str) -> None:
    if type(value) is not int or value < 1:
        raise PublicOiScheduleBodyVerificationErrorV2(
            f"{field_name} must be a positive integer"
        )


def _require_nonnegative_int(value: object, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise PublicOiScheduleBodyVerificationErrorV2(
            f"{field_name} must be a nonnegative integer"
        )
