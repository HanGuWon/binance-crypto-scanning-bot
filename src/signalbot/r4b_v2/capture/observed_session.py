"""Conservative observed-session binding above the local CLEAN closure.

This layer joins the persisted local session closure with the finalized prefix,
the public-OI schedule verifier, and the exact schedule/body verifier.  It
deliberately cannot promote those inputs into observed source completeness, M2,
or session-close authority: OI freshness and the two planned WebSocket sources
still lack the required causal completeness certificate.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import InitVar, asdict, dataclass, field, fields
from typing import Final, Literal, cast

from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityLedgerV2,
    PersistedCaptureCleanClosureSealReceiptV2,
)
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    CaptureFinalityFenceReceiptV2,
)
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingRestCapturePlanV2,
)
from signalbot.r4b_v2.capture.rest_census import (
    public_oi_rest_plan_sha256_v2,
    public_oi_rest_symbol_census_sha256_v2,
)
from signalbot.r4b_v2.capture.rest_census_verifier import (
    PublicOiRestCensusVerificationCertificateV2,
    validate_public_oi_rest_census_verification_certificate_v2,
)
from signalbot.r4b_v2.capture.rest_schedule_body_verifier import (
    PublicOiScheduleBodyVerificationCertificateV2,
    validate_public_oi_schedule_body_verification_certificate_v2,
)
from signalbot.r4b_v2.capture.session import (
    PersistedSessionClosureAuthorityV2,
    PersistedSessionStartAuthorityV2,
    SessionClosureManifestV2,
    SessionStorageRootReferenceV2,
    SessionWriterLeaseBindingV2,
    assert_persisted_session_closure_authority_current_v2,
)

_SCHEMA_VERSION: Final = "r4b_v2_observed_session_certificate_v2"
_PURPOSE: Final = "local_observed_session_binding_without_source_completeness"
_CERTIFICATE_DOMAIN = b"R4B_V2_OBSERVED_SESSION_CERTIFICATE\0"
_CLOSURE_AUTHORITY_DOMAIN = b"R4B_V2_OBSERVED_SESSION_CLOSURE_AUTHORITY\0"
_WRITER_LEASE_DOMAIN = b"R4B_V2_OBSERVED_SESSION_WRITER_LEASE\0"
_STORAGE_ROOTS_DOMAIN = b"R4B_V2_OBSERVED_SESSION_STORAGE_ROOTS\0"
_FACTORY_TOKEN = object()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# Persistence needs its own irreversible WriterLease claim/seal/assert owner.
# Until that owner exists, this module issues only an in-memory prerequisite.
OBSERVED_SESSION_CERTIFICATE_PERSISTENCE_SUPPORTED_V2: Final = False


class ObservedSessionCertificateErrorV2(ValueError):
    """Observed-session inputs do not form one exact conservative binding."""


@dataclass(frozen=True, slots=True)
class ObservedSessionCertificateV2:
    """Factory-sealed join of local closure and OI schedule/body evidence.

    ``current_authority_reproved_at_issue`` is an issuance-time statement, not
    a transferable claim that mutable filesystem paths remain current.  A
    consumer must call :func:`verify_observed_session_certificate_current_v2`
    immediately before relying on the local persistence evidence.
    """

    purpose: Literal["local_observed_session_binding_without_source_completeness"]
    session_closure_authority: PersistedSessionClosureAuthorityV2
    session_closure_manifest: SessionClosureManifestV2
    session_closure_authority_sha256: str
    session_closure_manifest_sha256: str
    session_closure_canonical_path: str
    finality_receipt: CaptureFinalityFenceReceiptV2
    finality_receipt_sha256: str
    finality_exact_prefix_sha256: str
    finality_prefix_proof_sha256: str
    oi_schedule_certificate: PublicOiRestCensusVerificationCertificateV2
    oi_schedule_certificate_sha256: str
    oi_schedule_body_certificate: PublicOiScheduleBodyVerificationCertificateV2
    oi_schedule_body_certificate_sha256: str
    oi_schedule_body_verified: Literal[True]
    oi_freshness_verified: Literal[False]
    session_id: str
    attempt_id: str
    protocol_hash: str
    plan_bundle_sha256: str
    session_start_manifest_sha256: str
    writer_lease: SessionWriterLeaseBindingV2
    writer_lease_sha256: str
    storage_roots: tuple[
        SessionStorageRootReferenceV2,
        SessionStorageRootReferenceV2,
        SessionStorageRootReferenceV2,
        SessionStorageRootReferenceV2,
    ]
    storage_roots_sha256: str
    current_authority_reproved_at_issue: Literal[True]
    certificate_persisted: Literal[False]
    observed_source_completeness_claimed: Literal[False]
    data_complete: Literal[False]
    m2_certified: Literal[False]
    session_close_authorized: Literal[False]
    production_order_execution_enabled: Literal[False]
    schema_version: Literal["r4b_v2_observed_session_certificate_v2"] = _SCHEMA_VERSION
    certificate_sha256: str = field(init=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise TypeError("observed-session certificates are factory-sealed")
        object.__setattr__(self, "_factory_seal", _FACTORY_TOKEN)
        _validate_certificate_material(self, verify_digest=False)
        object.__setattr__(self, "certificate_sha256", _certificate_sha256(self))

    @property
    def bound_material_line(self) -> bytes:
        """Return deterministic canonical material covered by the certificate hash."""

        return canonical_json_line(_certificate_document(self))


def create_observed_session_certificate_v2(
    *,
    session_closure_authority: PersistedSessionClosureAuthorityV2,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    oi_schedule_certificate: PublicOiRestCensusVerificationCertificateV2,
    oi_schedule_body_certificate: PublicOiScheduleBodyVerificationCertificateV2,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    ledger: CaptureIntegrityLedgerV2,
) -> ObservedSessionCertificateV2:
    """Issue a conservative certificate after reproving every current owner."""

    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    with lease.operation_guard():
        _validate_factory_inputs(
            session_closure_authority=session_closure_authority,
            finality_receipt=finality_receipt,
            oi_schedule_certificate=oi_schedule_certificate,
            oi_schedule_body_certificate=oi_schedule_body_certificate,
            lease=lease,
            session_start_authority=session_start_authority,
            promoting_plans=promoting_plans,
            pipeline=pipeline,
            ledger_seal_receipt=ledger_seal_receipt,
            ledger=ledger,
        )
        _validate_join(
            session_closure_authority=session_closure_authority,
            finality_receipt=finality_receipt,
            oi_schedule_certificate=oi_schedule_certificate,
            oi_schedule_body_certificate=oi_schedule_body_certificate,
            promoting_plans=promoting_plans,
        )
        assert_persisted_session_closure_authority_current_v2(
            session_closure_authority,
            lease=lease,
            session_start_authority=session_start_authority,
            promoting_plans=promoting_plans,
            finality_receipt=finality_receipt,
            pipeline=pipeline,
            ledger_seal_receipt=ledger_seal_receipt,
            ledger=ledger,
        )
        manifest = session_closure_authority.manifest
        start = manifest.session_start_manifest
        certificate = ObservedSessionCertificateV2(
            purpose=_PURPOSE,
            session_closure_authority=session_closure_authority,
            session_closure_manifest=manifest,
            session_closure_authority_sha256=(
                _session_closure_authority_sha256(session_closure_authority)
            ),
            session_closure_manifest_sha256=(session_closure_authority.manifest_sha256),
            session_closure_canonical_path=session_closure_authority.canonical_path,
            finality_receipt=finality_receipt,
            finality_receipt_sha256=finality_receipt.sha256,
            finality_exact_prefix_sha256=finality_receipt.exact_prefix_sha256,
            finality_prefix_proof_sha256=finality_receipt.prefix_proof_sha256,
            oi_schedule_certificate=oi_schedule_certificate,
            oi_schedule_certificate_sha256=(oi_schedule_certificate.certificate_sha256),
            oi_schedule_body_certificate=oi_schedule_body_certificate,
            oi_schedule_body_certificate_sha256=(oi_schedule_body_certificate.certificate_sha256),
            oi_schedule_body_verified=True,
            oi_freshness_verified=False,
            session_id=manifest.session_id,
            attempt_id=manifest.attempt_id,
            protocol_hash=start.wal_authority.protocol_sha256,
            plan_bundle_sha256=manifest.plan_bundle_sha256,
            session_start_manifest_sha256=manifest.session_start_manifest_sha256,
            writer_lease=manifest.writer_lease,
            writer_lease_sha256=_writer_lease_sha256(manifest.writer_lease),
            storage_roots=start.storage_roots,
            storage_roots_sha256=_storage_roots_sha256(start.storage_roots),
            current_authority_reproved_at_issue=True,
            certificate_persisted=False,
            observed_source_completeness_claimed=False,
            data_complete=False,
            m2_certified=False,
            session_close_authorized=False,
            production_order_execution_enabled=False,
            _factory_token=_FACTORY_TOKEN,
        )
        validate_observed_session_certificate_v2(certificate)
        return certificate


def validate_observed_session_certificate_v2(
    certificate: ObservedSessionCertificateV2,
) -> str:
    """Validate factory provenance and deterministic bound material."""

    if type(certificate) is not ObservedSessionCertificateV2:
        raise TypeError("certificate must be an exact ObservedSessionCertificateV2")
    if getattr(certificate, "_factory_seal", None) is not _FACTORY_TOKEN:
        raise ObservedSessionCertificateErrorV2(
            "observed-session certificate lacks factory provenance"
        )
    _validate_certificate_material(certificate, verify_digest=True)
    return certificate.certificate_sha256


def verify_observed_session_certificate_current_v2(
    certificate: ObservedSessionCertificateV2,
    *,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    ledger: CaptureIntegrityLedgerV2,
) -> str:
    """Reprove the certificate and its persisted local closure as current."""

    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    with lease.operation_guard():
        certificate_sha256 = validate_observed_session_certificate_v2(certificate)
        _validate_factory_inputs(
            session_closure_authority=certificate.session_closure_authority,
            finality_receipt=certificate.finality_receipt,
            oi_schedule_certificate=certificate.oi_schedule_certificate,
            oi_schedule_body_certificate=certificate.oi_schedule_body_certificate,
            lease=lease,
            session_start_authority=session_start_authority,
            promoting_plans=promoting_plans,
            pipeline=pipeline,
            ledger_seal_receipt=ledger_seal_receipt,
            ledger=ledger,
        )
        _validate_join(
            session_closure_authority=certificate.session_closure_authority,
            finality_receipt=certificate.finality_receipt,
            oi_schedule_certificate=certificate.oi_schedule_certificate,
            oi_schedule_body_certificate=certificate.oi_schedule_body_certificate,
            promoting_plans=promoting_plans,
        )
        assert_persisted_session_closure_authority_current_v2(
            certificate.session_closure_authority,
            lease=lease,
            session_start_authority=session_start_authority,
            promoting_plans=promoting_plans,
            finality_receipt=certificate.finality_receipt,
            pipeline=pipeline,
            ledger_seal_receipt=ledger_seal_receipt,
            ledger=ledger,
        )
        if validate_observed_session_certificate_v2(certificate) != certificate_sha256:
            raise ObservedSessionCertificateErrorV2(
                "observed-session certificate changed during current-authority verification"
            )
        return certificate_sha256


def _validate_factory_inputs(
    *,
    session_closure_authority: PersistedSessionClosureAuthorityV2,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    oi_schedule_certificate: PublicOiRestCensusVerificationCertificateV2,
    oi_schedule_body_certificate: PublicOiScheduleBodyVerificationCertificateV2,
    lease: WriterLease,
    session_start_authority: PersistedSessionStartAuthorityV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
    pipeline: CaptureBatchPipelineV2,
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2,
    ledger: CaptureIntegrityLedgerV2,
) -> None:
    if type(session_closure_authority) is not PersistedSessionClosureAuthorityV2:
        raise TypeError(
            "session_closure_authority must be an exact PersistedSessionClosureAuthorityV2"
        )
    if type(finality_receipt) is not CaptureFinalityFenceReceiptV2:
        raise TypeError("finality_receipt must be an exact CaptureFinalityFenceReceiptV2")
    if type(oi_schedule_certificate) is not PublicOiRestCensusVerificationCertificateV2:
        raise TypeError(
            "oi_schedule_certificate must be an exact PublicOiRestCensusVerificationCertificateV2"
        )
    if type(oi_schedule_body_certificate) is not (PublicOiScheduleBodyVerificationCertificateV2):
        raise TypeError(
            "oi_schedule_body_certificate must be an exact "
            "PublicOiScheduleBodyVerificationCertificateV2"
        )
    if type(lease) is not WriterLease:
        raise TypeError("lease must be an exact WriterLease")
    if type(session_start_authority) is not PersistedSessionStartAuthorityV2:
        raise TypeError("session_start_authority must be an exact PersistedSessionStartAuthorityV2")
    if type(promoting_plans) is not tuple:
        raise TypeError("promoting_plans must be an exact tuple")
    if type(pipeline) is not CaptureBatchPipelineV2:
        raise TypeError("pipeline must be an exact CaptureBatchPipelineV2")
    if type(ledger_seal_receipt) is not PersistedCaptureCleanClosureSealReceiptV2:
        raise TypeError(
            "ledger_seal_receipt must be an exact PersistedCaptureCleanClosureSealReceiptV2"
        )
    if type(ledger) is not CaptureIntegrityLedgerV2:
        raise TypeError("ledger must be an exact CaptureIntegrityLedgerV2")


def _validate_join(
    *,
    session_closure_authority: PersistedSessionClosureAuthorityV2,
    finality_receipt: CaptureFinalityFenceReceiptV2,
    oi_schedule_certificate: PublicOiRestCensusVerificationCertificateV2,
    oi_schedule_body_certificate: PublicOiScheduleBodyVerificationCertificateV2,
    promoting_plans: tuple[ProvisionalPromotingPlanV2, ...],
) -> None:
    session_closure_authority.__post_init__()
    finality_receipt.__post_init__()
    validate_public_oi_rest_census_verification_certificate_v2(oi_schedule_certificate)
    validate_public_oi_schedule_body_verification_certificate_v2(oi_schedule_body_certificate)
    manifest = session_closure_authority.manifest
    start = manifest.session_start_manifest
    if finality_receipt is not manifest.finality_receipt:
        raise ObservedSessionCertificateErrorV2(
            "observed session requires the exact closure finality receipt object"
        )
    if (
        oi_schedule_certificate.session_id != manifest.session_id
        or oi_schedule_certificate.protocol_hash != start.wal_authority.protocol_sha256
        or oi_schedule_certificate.session_start_manifest_sha256
        != manifest.session_start_manifest_sha256
        or oi_schedule_certificate.plan_bundle_sha256 != manifest.plan_bundle_sha256
    ):
        raise ObservedSessionCertificateErrorV2(
            "OI schedule evidence differs from the closure session, protocol, or plan"
        )
    if (
        oi_schedule_certificate.finality_receipt_sha256 != finality_receipt.sha256
        or oi_schedule_certificate.finality_authority_sha256 != finality_receipt.authority_sha256
        or oi_schedule_certificate.finality_exact_prefix_sha256
        != finality_receipt.exact_prefix_sha256
        or oi_schedule_certificate.finality_prefix_proof_sha256
        != finality_receipt.prefix_proof_sha256
        or oi_schedule_certificate.verified_prefix_tail_ingest_seq
        != finality_receipt.fence_ingest_seq
    ):
        raise ObservedSessionCertificateErrorV2(
            "OI schedule evidence differs from the exact finalized prefix"
        )
    rest_plans = tuple(
        cast(ProvisionalPromotingRestCapturePlanV2, plan)
        for plan in promoting_plans
        if type(plan) is ProvisionalPromotingRestCapturePlanV2
    )
    if len(rest_plans) != 1:
        raise ObservedSessionCertificateErrorV2(
            "observed session requires exactly one promoting public-OI REST plan"
        )
    rest_plan = rest_plans[0]
    if (
        oi_schedule_certificate.plan_id != rest_plan.name
        or oi_schedule_certificate.rest_plan_sha256 != public_oi_rest_plan_sha256_v2(rest_plan)
        or oi_schedule_certificate.symbol_census_sha256
        != public_oi_rest_symbol_census_sha256_v2(rest_plan)
        or oi_schedule_certificate.symbol_count != len(rest_plan.symbols)
    ):
        raise ObservedSessionCertificateErrorV2(
            "OI schedule evidence differs from the exact promoting REST plan"
        )
    if (
        oi_schedule_certificate.coverage_closed is not True
        or oi_schedule_certificate.data_complete is not False
        or oi_schedule_certificate.m2_certified is not False
        or oi_schedule_certificate.session_close_authorized is not False
        or oi_schedule_certificate.current_storage_reproved is not False
    ):
        raise ObservedSessionCertificateErrorV2(
            "OI schedule evidence may not claim data completeness, M2, closure, or storage"
        )
    if (
        oi_schedule_body_certificate.observed_schedule_certificate_sha256
        != oi_schedule_certificate.certificate_sha256
        or oi_schedule_body_certificate.session_id != manifest.session_id
        or oi_schedule_body_certificate.protocol_hash != start.wal_authority.protocol_sha256
        or oi_schedule_body_certificate.session_start_manifest_sha256
        != manifest.session_start_manifest_sha256
        or oi_schedule_body_certificate.plan_bundle_sha256 != manifest.plan_bundle_sha256
        or oi_schedule_body_certificate.finality_receipt_sha256 != finality_receipt.sha256
        or oi_schedule_body_certificate.finality_exact_prefix_sha256
        != finality_receipt.exact_prefix_sha256
        or oi_schedule_body_certificate.finality_prefix_proof_sha256
        != finality_receipt.prefix_proof_sha256
        or oi_schedule_body_certificate.verified_prefix_tail_ingest_seq
        != finality_receipt.fence_ingest_seq
        or oi_schedule_body_certificate.schedule_body_complete is not True
        or oi_schedule_body_certificate.body_semantics_verified is not True
        or oi_schedule_body_certificate.freshness_verified is not False
        or oi_schedule_body_certificate.transaction_time_causally_bounded is not False
        or oi_schedule_body_certificate.websocket_completeness_verified is not False
        or oi_schedule_body_certificate.m2_certified is not False
        or oi_schedule_body_certificate.session_close_authorized is not False
        or oi_schedule_body_certificate.current_storage_reproved is not False
    ):
        raise ObservedSessionCertificateErrorV2(
            "OI schedule/body evidence differs from the exact incomplete session prefix"
        )


def _validate_certificate_material(
    certificate: ObservedSessionCertificateV2,
    *,
    verify_digest: bool,
) -> None:
    if certificate.purpose != _PURPOSE or certificate.schema_version != _SCHEMA_VERSION:
        raise ObservedSessionCertificateErrorV2("unsupported observed-session purpose or schema")
    if type(certificate.session_closure_authority) is not PersistedSessionClosureAuthorityV2:
        raise TypeError(
            "session_closure_authority must be an exact PersistedSessionClosureAuthorityV2"
        )
    authority = certificate.session_closure_authority
    authority.__post_init__()
    manifest = authority.manifest
    if (
        type(certificate.session_closure_manifest) is not SessionClosureManifestV2
        or certificate.session_closure_manifest is not manifest
    ):
        raise ObservedSessionCertificateErrorV2(
            "certificate does not bind the exact persisted closure manifest object"
        )
    manifest.__post_init__()
    if type(certificate.finality_receipt) is not CaptureFinalityFenceReceiptV2:
        raise TypeError("finality_receipt must be an exact CaptureFinalityFenceReceiptV2")
    if certificate.finality_receipt is not manifest.finality_receipt:
        raise ObservedSessionCertificateErrorV2(
            "certificate finality is not the exact persisted closure receipt"
        )
    certificate.finality_receipt.__post_init__()
    if type(certificate.oi_schedule_certificate) is not (
        PublicOiRestCensusVerificationCertificateV2
    ):
        raise TypeError(
            "oi_schedule_certificate must be an exact PublicOiRestCensusVerificationCertificateV2"
        )
    oi_sha256 = validate_public_oi_rest_census_verification_certificate_v2(
        certificate.oi_schedule_certificate
    )
    if type(certificate.oi_schedule_body_certificate) is not (
        PublicOiScheduleBodyVerificationCertificateV2
    ):
        raise TypeError(
            "oi_schedule_body_certificate must be an exact "
            "PublicOiScheduleBodyVerificationCertificateV2"
        )
    oi_body_sha256 = validate_public_oi_schedule_body_verification_certificate_v2(
        certificate.oi_schedule_body_certificate
    )
    if type(certificate.writer_lease) is not SessionWriterLeaseBindingV2:
        raise TypeError("writer_lease must be an exact SessionWriterLeaseBindingV2")
    if certificate.writer_lease is not manifest.writer_lease:
        raise ObservedSessionCertificateErrorV2(
            "certificate lease is not the exact persisted closure lease binding"
        )
    certificate.writer_lease.__post_init__()
    if (
        type(certificate.storage_roots) is not tuple
        or len(certificate.storage_roots) != 4
        or certificate.storage_roots is not manifest.session_start_manifest.storage_roots
        or any(
            type(root) is not SessionStorageRootReferenceV2 for root in certificate.storage_roots
        )
    ):
        raise ObservedSessionCertificateErrorV2(
            "certificate storage roots are not the exact ordered session roots"
        )
    for root in certificate.storage_roots:
        root.__post_init__()
    start = manifest.session_start_manifest
    expected_values = {
        "session_closure_authority_sha256": _session_closure_authority_sha256(authority),
        "session_closure_manifest_sha256": authority.manifest_sha256,
        "session_closure_canonical_path": authority.canonical_path,
        "finality_receipt_sha256": certificate.finality_receipt.sha256,
        "finality_exact_prefix_sha256": certificate.finality_receipt.exact_prefix_sha256,
        "finality_prefix_proof_sha256": certificate.finality_receipt.prefix_proof_sha256,
        "oi_schedule_certificate_sha256": oi_sha256,
        "oi_schedule_body_certificate_sha256": oi_body_sha256,
        "session_id": manifest.session_id,
        "attempt_id": manifest.attempt_id,
        "protocol_hash": start.wal_authority.protocol_sha256,
        "plan_bundle_sha256": manifest.plan_bundle_sha256,
        "session_start_manifest_sha256": manifest.session_start_manifest_sha256,
        "writer_lease_sha256": _writer_lease_sha256(certificate.writer_lease),
        "storage_roots_sha256": _storage_roots_sha256(certificate.storage_roots),
    }
    if any(getattr(certificate, name) != value for name, value in expected_values.items()):
        raise ObservedSessionCertificateErrorV2(
            "observed-session canonical binding differs from its exact inputs"
        )
    oi = certificate.oi_schedule_certificate
    if (
        oi.session_id != certificate.session_id
        or oi.protocol_hash != certificate.protocol_hash
        or oi.session_start_manifest_sha256 != certificate.session_start_manifest_sha256
        or oi.plan_bundle_sha256 != certificate.plan_bundle_sha256
        or oi.finality_receipt_sha256 != certificate.finality_receipt_sha256
        or oi.finality_exact_prefix_sha256 != certificate.finality_exact_prefix_sha256
        or oi.finality_prefix_proof_sha256 != certificate.finality_prefix_proof_sha256
    ):
        raise ObservedSessionCertificateErrorV2(
            "observed-session OI certificate differs from the bound session prefix"
        )
    oi_body = certificate.oi_schedule_body_certificate
    if (
        oi_body.observed_schedule_certificate_sha256 != certificate.oi_schedule_certificate_sha256
        or oi_body.session_id != certificate.session_id
        or oi_body.protocol_hash != certificate.protocol_hash
        or oi_body.session_start_manifest_sha256 != certificate.session_start_manifest_sha256
        or oi_body.plan_bundle_sha256 != certificate.plan_bundle_sha256
        or oi_body.finality_receipt_sha256 != certificate.finality_receipt_sha256
        or oi_body.finality_exact_prefix_sha256 != certificate.finality_exact_prefix_sha256
        or oi_body.finality_prefix_proof_sha256 != certificate.finality_prefix_proof_sha256
        or oi_body.schedule_body_complete is not True
        or oi_body.body_semantics_verified is not True
        or oi_body.freshness_verified is not False
        or oi_body.transaction_time_causally_bounded is not False
        or oi_body.websocket_completeness_verified is not False
        or oi_body.m2_certified is not False
        or oi_body.session_close_authorized is not False
        or oi_body.current_storage_reproved is not False
        or certificate.oi_schedule_body_verified is not True
        or certificate.oi_freshness_verified is not False
    ):
        raise ObservedSessionCertificateErrorV2(
            "observed-session OI schedule/body evidence exceeds or differs from its bound scope"
        )
    if (
        certificate.current_authority_reproved_at_issue is not True
        or certificate.certificate_persisted is not False
        or certificate.observed_source_completeness_claimed is not False
        or certificate.data_complete is not False
        or certificate.m2_certified is not False
        or certificate.session_close_authorized is not False
        or certificate.production_order_execution_enabled is not False
    ):
        raise ObservedSessionCertificateErrorV2(
            "observed-session certificate cannot claim completeness, M2, closure, or orders"
        )
    for name in (
        "session_closure_authority_sha256",
        "session_closure_manifest_sha256",
        "finality_receipt_sha256",
        "finality_exact_prefix_sha256",
        "finality_prefix_proof_sha256",
        "oi_schedule_certificate_sha256",
        "oi_schedule_body_certificate_sha256",
        "protocol_hash",
        "plan_bundle_sha256",
        "session_start_manifest_sha256",
        "writer_lease_sha256",
        "storage_roots_sha256",
    ):
        _require_sha256(getattr(certificate, name), name)
    if verify_digest:
        _require_sha256(certificate.certificate_sha256, "certificate_sha256")
        if not hmac.compare_digest(
            certificate.certificate_sha256,
            _certificate_sha256(certificate),
        ):
            raise ObservedSessionCertificateErrorV2(
                "observed-session certificate hash differs from its canonical material"
            )


def _session_closure_authority_document(
    authority: PersistedSessionClosureAuthorityV2,
) -> dict[str, object]:
    return {
        "schema_version": authority.schema_version,
        "manifest": asdict(authority.manifest),
        "canonical_path": authority.canonical_path,
        "manifest_sha256": authority.manifest_sha256,
        "byte_count": authority.byte_count,
        "file_device": str(authority.file_device),
        "file_inode": str(authority.file_inode),
        "file_nlink": str(authority.file_nlink),
        "writer_lease": asdict(authority.writer_lease),
    }


def _oi_schedule_certificate_document(
    certificate: PublicOiRestCensusVerificationCertificateV2,
) -> dict[str, object]:
    return {
        model_field.name: getattr(certificate, model_field.name)
        for model_field in fields(certificate)
        if model_field.name != "_factory_seal"
    }


def _oi_schedule_body_certificate_document(
    certificate: PublicOiScheduleBodyVerificationCertificateV2,
) -> dict[str, object]:
    return {
        model_field.name: getattr(certificate, model_field.name)
        for model_field in fields(certificate)
        if model_field.name != "_factory_seal"
    }


def _certificate_document(
    certificate: ObservedSessionCertificateV2,
) -> dict[str, object]:
    return {
        "schema_version": certificate.schema_version,
        "purpose": certificate.purpose,
        "session_closure_authority": _session_closure_authority_document(
            certificate.session_closure_authority
        ),
        "session_closure_authority_sha256": (certificate.session_closure_authority_sha256),
        "session_closure_manifest_sha256": (certificate.session_closure_manifest_sha256),
        "session_closure_canonical_path": certificate.session_closure_canonical_path,
        "finality_receipt": asdict(certificate.finality_receipt),
        "finality_receipt_sha256": certificate.finality_receipt_sha256,
        "finality_exact_prefix_sha256": certificate.finality_exact_prefix_sha256,
        "finality_prefix_proof_sha256": certificate.finality_prefix_proof_sha256,
        "oi_schedule_certificate": _oi_schedule_certificate_document(
            certificate.oi_schedule_certificate
        ),
        "oi_schedule_certificate_sha256": certificate.oi_schedule_certificate_sha256,
        "oi_schedule_body_certificate": _oi_schedule_body_certificate_document(
            certificate.oi_schedule_body_certificate
        ),
        "oi_schedule_body_certificate_sha256": (certificate.oi_schedule_body_certificate_sha256),
        "oi_schedule_body_verified": certificate.oi_schedule_body_verified,
        "oi_freshness_verified": certificate.oi_freshness_verified,
        "session_id": certificate.session_id,
        "attempt_id": certificate.attempt_id,
        "protocol_hash": certificate.protocol_hash,
        "plan_bundle_sha256": certificate.plan_bundle_sha256,
        "session_start_manifest_sha256": certificate.session_start_manifest_sha256,
        "writer_lease": asdict(certificate.writer_lease),
        "writer_lease_sha256": certificate.writer_lease_sha256,
        "storage_roots": [asdict(root) for root in certificate.storage_roots],
        "storage_roots_sha256": certificate.storage_roots_sha256,
        "current_authority_reproved_at_issue": (certificate.current_authority_reproved_at_issue),
        "certificate_persisted": certificate.certificate_persisted,
        "observed_source_completeness_claimed": (certificate.observed_source_completeness_claimed),
        "data_complete": certificate.data_complete,
        "m2_certified": certificate.m2_certified,
        "session_close_authorized": certificate.session_close_authorized,
        "production_order_execution_enabled": (certificate.production_order_execution_enabled),
    }


def _certificate_sha256(certificate: ObservedSessionCertificateV2) -> str:
    return hashlib.sha256(
        _CERTIFICATE_DOMAIN + canonical_json_line(_certificate_document(certificate))
    ).hexdigest()


def _session_closure_authority_sha256(
    authority: PersistedSessionClosureAuthorityV2,
) -> str:
    return hashlib.sha256(
        _CLOSURE_AUTHORITY_DOMAIN
        + canonical_json_line(_session_closure_authority_document(authority))
    ).hexdigest()


def _writer_lease_sha256(binding: SessionWriterLeaseBindingV2) -> str:
    return hashlib.sha256(_WRITER_LEASE_DOMAIN + canonical_json_line(binding)).hexdigest()


def _storage_roots_sha256(
    roots: tuple[
        SessionStorageRootReferenceV2,
        SessionStorageRootReferenceV2,
        SessionStorageRootReferenceV2,
        SessionStorageRootReferenceV2,
    ],
) -> str:
    return hashlib.sha256(
        _STORAGE_ROOTS_DOMAIN
        + canonical_json_line({"storage_roots": [asdict(root) for root in roots]})
    ).hexdigest()


def _require_sha256(value: object, field_name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ObservedSessionCertificateErrorV2(f"{field_name} must be a lowercase SHA-256 digest")
