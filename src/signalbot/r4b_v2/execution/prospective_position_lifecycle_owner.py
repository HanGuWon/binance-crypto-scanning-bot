"""Typed owner for the attempt-wide prospective position-outcome WAL.

The lower-level outcome store deliberately knows only the six-record grammar.
This module owns the semantic grammar: a position can be admitted only from an
exact PAPER full-fill, a family exit must become terminal before cashflows are
recorded, and the final typed terminal must reconcile every signed cashflow and
evidence reference already made durable.

This remains a PAPER/research seam.  It never places production orders and all
efficacy/authority flags remain false while the upstream execution contract and
replay-owned evidence certificates are incomplete.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal
from enum import StrEnum
from fractions import Fraction
from typing import Final, cast

from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.mandatory_exit import MandatoryExitPositionSideV2
from signalbot.r4b_v2.execution.paper_fok import (
    PaperFokFullFillCertificateV2,
    PaperFokSideV2,
    canonical_paper_fok_full_fill_certificate_v2,
)
from signalbot.r4b_v2.execution.paper_sizing import PaperSizingCellV2
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveExpectedCellV2,
    canonical_prospective_census_plan_v2,
)
from signalbot.r4b_v2.execution.prospective_outcome_wal_record import (
    FAMILY_EXIT_DISPOSITION_PAYLOAD_SCHEMA_V2,
    FAMILY_EXIT_PREPARE_PAYLOAD_SCHEMA_V2,
    MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2,
    POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2,
    POSITION_OPEN_DISPOSITION_PAYLOAD_SCHEMA_V2,
    POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2,
    POSITION_TERMINAL_PAYLOAD_SCHEMA_V2,
    ProspectiveOutcomeWalRecordKindV2,
    ProspectiveOutcomeWalRecordV2,
    prospective_outcome_id_v2,
)
from signalbot.r4b_v2.execution.prospective_outcome_wal_store import (
    ProspectiveOutcomeWalAppendItemV2,
    ProspectiveOutcomeWalDurableBatchReceiptV2,
    ProspectiveOutcomeWalReplaySnapshotV2,
    ProspectiveOutcomeWalStoreV2,
)
from signalbot.r4b_v2.execution.prospective_paper_terminal_payload import (
    ProspectivePaperTerminalCompletenessV2,
    ProspectivePaperTerminalPayloadV2,
    ProspectivePaperTerminalStatusV2,
    canonical_prospective_paper_terminal_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_position_terminal_payload import (
    FinalFeeEvidenceReferenceV2,
    FundingCensusEvidenceReferenceV2,
    MandatoryExitEvidenceReferenceV2,
    ProspectivePositionTerminalPayloadV2,
    ProspectivePositionTerminalStatusV2,
    canonical_prospective_position_terminal_payload_v2,
    prospective_position_id_v2,
)
from signalbot.r4b_v2.strategy.family_a import (
    FamilyAAdmissionReceiptV2,
    FamilyAExitDecisionV2,
    FamilyAExitMutationReceiptV2,
    canonical_family_a_entry_decision_v2,
    canonical_family_a_exit_decision_v2,
)
from signalbot.r4b_v2.strategy.family_b import (
    FamilyBAdmissionReceiptV2,
    FamilyBExitDecisionV2,
    FamilyBExitMutationReceiptV2,
    canonical_family_b_entry_decision_v2,
    canonical_family_b_exit_decision_v2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FamilyCAdmissionReceiptV2,
    FamilyCExitDecisionV2,
    FamilyCExitMutationReceiptV2,
    canonical_family_c_entry_decision_v2,
    canonical_family_c_exit_decision_v2,
)
from signalbot.r4b_v2.strategy.prospective_plan import (
    current_prospective_execution_contract_sha256_v2,
)

PROSPECTIVE_POSITION_LIFECYCLE_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.0_PROSPECTIVE_POSITION_LIFECYCLE_OWNER_DRAFT"
)
PROSPECTIVE_POSITION_LIFECYCLE_AUTHORITY_V2: Final = (
    "NONAUTHORITATIVE_UPSTREAM_REPLAY_CERTIFICATES_INCOMPLETE"
)
MAX_PROSPECTIVE_POSITION_LIFECYCLE_REASONS_V2: Final = 32
MAX_PROSPECTIVE_POSITION_LIFECYCLE_CASHFLOWS_V2: Final = 4
MAX_PROSPECTIVE_POSITION_LIFECYCLE_OUTCOMES_V2: Final = 1_000_000

_IDENTITY_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_POSITION_LIFECYCLE_IDENTITY_V2\0"
_ADMISSION_EVIDENCE_DOMAIN: Final = b"R4B_V2_POSITION_ADMISSION_EVIDENCE_V2\0"
_EXIT_EVIDENCE_DOMAIN: Final = b"R4B_V2_POSITION_EXIT_EVIDENCE_V2\0"
_OPEN_PREPARE_EVENT_DOMAIN: Final = b"R4B_V2_POSITION_OPEN_PREPARE_EVENT_V2\0"
_OPEN_PREPARE_PAYLOAD_DOMAIN: Final = b"R4B_V2_POSITION_OPEN_PREPARE_PAYLOAD_V2\0"
_OPEN_DISPOSITION_EVENT_DOMAIN: Final = b"R4B_V2_POSITION_OPEN_DISPOSITION_EVENT_V2\0"
_OPEN_DISPOSITION_PAYLOAD_DOMAIN: Final = b"R4B_V2_POSITION_OPEN_DISPOSITION_PAYLOAD_V2\0"
_EXIT_PREPARE_EVENT_DOMAIN: Final = b"R4B_V2_FAMILY_EXIT_PREPARE_EVENT_V2\0"
_EXIT_PREPARE_PAYLOAD_DOMAIN: Final = b"R4B_V2_FAMILY_EXIT_PREPARE_PAYLOAD_V2\0"
_EXIT_DISPOSITION_EVENT_DOMAIN: Final = b"R4B_V2_FAMILY_EXIT_DISPOSITION_EVENT_V2\0"
_EXIT_DISPOSITION_PAYLOAD_DOMAIN: Final = b"R4B_V2_FAMILY_EXIT_DISPOSITION_PAYLOAD_V2\0"
_CASHFLOW_EVENT_DOMAIN: Final = b"R4B_V2_POSITION_CASHFLOW_EVENT_V2\0"
_CASHFLOW_PAYLOAD_DOMAIN: Final = b"R4B_V2_POSITION_CASHFLOW_PAYLOAD_V2\0"
_POSITION_TERMINAL_PAYLOAD_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_POSITION_TERMINAL_PAYLOAD_V2\0"
_MANDATORY_EXIT_REFERENCE_DOMAIN: Final = b"R4B_V2_MANDATORY_EXIT_REFERENCE_V2\0"
_FEE_REFERENCE_DOMAIN: Final = b"R4B_V2_FINAL_FEE_REFERENCE_V2\0"
_FUNDING_REFERENCE_DOMAIN: Final = b"R4B_V2_FUNDING_CENSUS_REFERENCE_V2\0"
_FAMILY_REFERENCE_DOMAIN: Final = b"R4B_V2_FAMILY_LIFECYCLE_REFERENCE_V2\0"
_FACTORY_TOKEN: Final = object()
_OWNER_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_JCS_MAX_SAFE_INTEGER: Final = 9_007_199_254_740_991


class ProspectivePositionLifecycleOwnerErrorV2(RuntimeError):
    """Base error for the typed position lifecycle owner."""


class ProspectivePositionLifecycleContractErrorV2(ProspectivePositionLifecycleOwnerErrorV2):
    """Raised before mutation when typed evidence or a transition is invalid."""


class ProspectivePositionLifecycleIntegrityErrorV2(ProspectivePositionLifecycleOwnerErrorV2):
    """Raised when a durable typed replay is inconsistent."""


class ProspectivePositionLifecycleFailedErrorV2(ProspectivePositionLifecycleOwnerErrorV2):
    """Raised after a durable mutation makes the owner unusable."""


class ProspectivePositionOpenIntentV2(StrEnum):
    NO_POSITION = "NO_POSITION"
    FULL_FILL_POSITION = "FULL_FILL_POSITION"


class ProspectivePositionOpenDispositionV2(StrEnum):
    SUPPRESSED_NO_POSITION = "SUPPRESSED_NO_POSITION"
    ADMITTED_FULL_FILL = "ADMITTED_FULL_FILL"


class ProspectivePositionCashflowClassV2(StrEnum):
    ENTRY_EXECUTION = "ENTRY_EXECUTION"
    EXIT_EXECUTION = "EXIT_EXECUTION"
    PUBLIC_FEE = "PUBLIC_FEE"
    PUBLIC_FUNDING = "PUBLIC_FUNDING"


class ProspectiveLifecycleEvidenceAuthorityV2(StrEnum):
    LIVE_FACTORY_SEALED_FAMILY_A = "LIVE_FACTORY_SEALED_FAMILY_A_PROCESS_LOCAL"
    LIVE_FACTORY_SEALED_FAMILY_B = "LIVE_FACTORY_SEALED_FAMILY_B_PROCESS_LOCAL"
    LIVE_FACTORY_SEALED_FAMILY_C = "LIVE_FACTORY_SEALED_FAMILY_C_PROCESS_LOCAL"
    LIVE_FACTORY_SEALED_PAPER_FULL_FILL = "LIVE_FACTORY_SEALED_PAPER_FULL_FILL_PROCESS_LOCAL"
    EXPLICIT_NONAUTHORITATIVE_HASH_REFERENCE = (
        "EXPLICIT_NONAUTHORITATIVE_HASH_REFERENCE_NOT_MEMBERSHIP_PROOF"
    )


class ProspectiveTypedLifecyclePhaseV2(StrEnum):
    OPEN_PREPARED = "OPEN_PREPARED"
    NO_POSITION = "NO_POSITION"
    POSITION_OPEN = "POSITION_OPEN"
    EXIT_PREPARED = "EXIT_PREPARED"
    EXIT_TERMINAL = "EXIT_TERMINAL"
    TERMINAL = "TERMINAL"


@dataclass(frozen=True, slots=True)
class ProspectivePositionLifecycleOwnerConfigV2:
    maximum_outcomes: int = MAX_PROSPECTIVE_POSITION_LIFECYCLE_OUTCOMES_V2
    maximum_cashflows_per_position: int = MAX_PROSPECTIVE_POSITION_LIFECYCLE_CASHFLOWS_V2

    def __post_init__(self) -> None:
        if type(self.maximum_outcomes) is not int or not (
            1 <= self.maximum_outcomes <= MAX_PROSPECTIVE_POSITION_LIFECYCLE_OUTCOMES_V2
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "maximum_outcomes exceeds the fixed lifecycle bound"
            )
        if (
            type(self.maximum_cashflows_per_position) is not int
            or self.maximum_cashflows_per_position
            != MAX_PROSPECTIVE_POSITION_LIFECYCLE_CASHFLOWS_V2
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "exactly four bounded cashflow classes are required"
            )


@dataclass(frozen=True, slots=True)
class ProspectivePositionLifecycleIdentityV2:
    attempt_id: str
    attempt_plan_sha256: str
    promoting_plan_sha256: str
    execution_contract_sha256: str
    origin_segment_id: str
    origin_cell_id: str
    sizing_cell: PaperSizingCellV2
    family: PromotingFamilyV2
    family_rule_version: str
    symbol: str
    venue: VenueV2
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    outcome_id: str
    position_id: str | None
    position_side: MandatoryExitPositionSideV2 | None
    _factory_token: InitVar[object | None] = None
    identity_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ProspectivePositionLifecycleContractErrorV2(
                "position lifecycle identities are factory-sealed"
            )
        _validate_identity(self)
        object.__setattr__(
            self,
            "identity_sha256",
            _hash_document(_IDENTITY_DOMAIN, _identity_document(self)),
        )


@dataclass(frozen=True, slots=True)
class ProspectiveFamilyAdmissionEvidenceV2:
    family: PromotingFamilyV2
    entry_event_id: str
    full_fill_certificate_sha256: str
    admission_input_sha256: str
    admission_pre_root_sha256: str
    admission_pre_event_count: int
    admission_post_root_sha256: str
    admission_post_event_count: int
    admission_disposition: str
    source_authority: ProspectiveLifecycleEvidenceAuthorityV2
    _factory_token: InitVar[object | None] = None
    evidence_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ProspectivePositionLifecycleContractErrorV2(
                "family admission evidence is factory-sealed"
            )
        _validate_admission_evidence(self)
        object.__setattr__(
            self,
            "evidence_sha256",
            _hash_document(_ADMISSION_EVIDENCE_DOMAIN, _admission_document(self)),
        )


@dataclass(frozen=True, slots=True)
class ProspectiveFamilyExitEvidenceV2:
    family: PromotingFamilyV2
    entry_event_id: str
    exit_event_id: str
    exit_decision_payload_sha256: str
    exit_input_sha256: str
    exit_pre_root_sha256: str
    exit_pre_event_count: int
    exit_post_root_sha256: str
    exit_post_event_count: int
    exit_disposition: str
    exit_decision_cutoff_ms: int
    terminal_exit: bool
    source_authority: ProspectiveLifecycleEvidenceAuthorityV2
    _factory_token: InitVar[object | None] = None
    evidence_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ProspectivePositionLifecycleContractErrorV2(
                "family exit evidence is factory-sealed"
            )
        _validate_exit_evidence(self)
        object.__setattr__(
            self,
            "evidence_sha256",
            _hash_document(_EXIT_EVIDENCE_DOMAIN, _exit_evidence_document(self)),
        )


@dataclass(frozen=True, slots=True)
class ProspectivePositionOpenPreparePayloadV2:
    identity: ProspectivePositionLifecycleIdentityV2
    open_intent: ProspectivePositionOpenIntentV2
    paper_terminal_payload_sha256: str
    paper_terminal_jsonl_sha256: str
    paper_terminal_record_sha256: str | None
    full_fill_certificate_sha256: str | None
    full_fill_certificate_jsonl_sha256: str | None
    prepared_at_ms: int
    reasons: tuple[str, ...]
    invalidation: str
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2)
    rule_version: str = field(init=False, default=PROSPECTIVE_POSITION_LIFECYCLE_RULE_VERSION_V2)
    authority_status: str = field(init=False, default=PROSPECTIVE_POSITION_LIFECYCLE_AUTHORITY_V2)
    typed_payload_semantics_authoritative: bool = field(init=False, default=False)
    efficacy_eligible: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        _seal_payload(self, _factory_token)


@dataclass(frozen=True, slots=True)
class ProspectivePositionOpenDispositionPayloadV2:
    identity: ProspectivePositionLifecycleIdentityV2
    prepare_event_id: str
    prepare_payload_sha256: str
    disposition: ProspectivePositionOpenDispositionV2
    admission_evidence: ProspectiveFamilyAdmissionEvidenceV2 | None
    dispositioned_at_ms: int
    reasons: tuple[str, ...]
    invalidation: str
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=POSITION_OPEN_DISPOSITION_PAYLOAD_SCHEMA_V2)
    rule_version: str = field(init=False, default=PROSPECTIVE_POSITION_LIFECYCLE_RULE_VERSION_V2)
    authority_status: str = field(init=False, default=PROSPECTIVE_POSITION_LIFECYCLE_AUTHORITY_V2)
    typed_payload_semantics_authoritative: bool = field(init=False, default=False)
    efficacy_eligible: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        _seal_payload(self, _factory_token)


@dataclass(frozen=True, slots=True)
class ProspectiveFamilyExitPreparePayloadV2:
    identity: ProspectivePositionLifecycleIdentityV2
    open_disposition_event_id: str
    admission_evidence_sha256: str
    entry_event_id: str
    exit_event_id: str
    exit_decision_payload_sha256: str
    exit_input_sha256: str
    exit_decision_cutoff_ms: int
    exits_position: bool
    exit_decision_authority: ProspectiveLifecycleEvidenceAuthorityV2
    prepared_at_ms: int
    reasons: tuple[str, ...]
    invalidation: str
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=FAMILY_EXIT_PREPARE_PAYLOAD_SCHEMA_V2)
    rule_version: str = field(init=False, default=PROSPECTIVE_POSITION_LIFECYCLE_RULE_VERSION_V2)
    authority_status: str = field(init=False, default=PROSPECTIVE_POSITION_LIFECYCLE_AUTHORITY_V2)
    typed_payload_semantics_authoritative: bool = field(init=False, default=False)
    efficacy_eligible: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        _seal_payload(self, _factory_token)


@dataclass(frozen=True, slots=True)
class ProspectiveFamilyExitDispositionPayloadV2:
    identity: ProspectivePositionLifecycleIdentityV2
    exit_prepare_event_id: str
    exit_prepare_payload_sha256: str
    exit_evidence: ProspectiveFamilyExitEvidenceV2
    dispositioned_at_ms: int
    reasons: tuple[str, ...]
    invalidation: str
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=FAMILY_EXIT_DISPOSITION_PAYLOAD_SCHEMA_V2)
    rule_version: str = field(init=False, default=PROSPECTIVE_POSITION_LIFECYCLE_RULE_VERSION_V2)
    authority_status: str = field(init=False, default=PROSPECTIVE_POSITION_LIFECYCLE_AUTHORITY_V2)
    typed_payload_semantics_authoritative: bool = field(init=False, default=False)
    efficacy_eligible: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        _seal_payload(self, _factory_token)


@dataclass(frozen=True, slots=True)
class ProspectivePositionCashflowPayloadV2:
    identity: ProspectivePositionLifecycleIdentityV2
    terminal_exit_event_id: str
    terminal_exit_evidence_sha256: str
    cashflow_class: ProspectivePositionCashflowClassV2
    signed_amount_usdt: Decimal
    evidence_reference_sha256: str
    evidence_authority: ProspectiveLifecycleEvidenceAuthorityV2
    observed_at_ms: int
    reasons: tuple[str, ...]
    invalidation: str
    _factory_token: InitVar[object | None] = None
    event_id: str = field(init=False)
    payload_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2)
    rule_version: str = field(init=False, default=PROSPECTIVE_POSITION_LIFECYCLE_RULE_VERSION_V2)
    authority_status: str = field(init=False, default=PROSPECTIVE_POSITION_LIFECYCLE_AUTHORITY_V2)
    typed_payload_semantics_authoritative: bool = field(init=False, default=False)
    efficacy_eligible: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        _seal_payload(self, _factory_token)


_PayloadV2 = (
    ProspectivePositionOpenPreparePayloadV2
    | ProspectivePositionOpenDispositionPayloadV2
    | ProspectiveFamilyExitPreparePayloadV2
    | ProspectiveFamilyExitDispositionPayloadV2
    | ProspectivePositionCashflowPayloadV2
)


@dataclass(frozen=True, slots=True)
class ProspectivePositionLifecycleDurableReceiptV2:
    payload: _PayloadV2 | ProspectivePositionTerminalPayloadV2
    wal_receipt: ProspectiveOutcomeWalDurableBatchReceiptV2

    def __post_init__(self) -> None:
        if type(self.wal_receipt) is not ProspectiveOutcomeWalDurableBatchReceiptV2:
            raise ProspectivePositionLifecycleIntegrityErrorV2(
                "durable lifecycle receipt requires an exact outcome WAL receipt"
            )
        if len(self.wal_receipt.records) != 1:
            raise ProspectivePositionLifecycleIntegrityErrorV2(
                "one lifecycle operation must map to one durable WAL record"
            )
        record = self.wal_receipt.records[0]
        if record.payload_sha256 != _canonical_payload_sha256(self.payload):
            raise ProspectivePositionLifecycleIntegrityErrorV2(
                "durable WAL payload differs from its typed lifecycle payload"
            )

    @property
    def production_order_placement(self) -> bool:
        return False

    @property
    def efficacy_eligible(self) -> bool:
        return False


def build_prospective_position_open_prepare_payload_v2(
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
    sizing_cell: PaperSizingCellV2,
    paper_terminal: ProspectivePaperTerminalPayloadV2,
    paper_terminal_record_sha256: str,
    prepared_at_ms: int,
    full_fill_certificate: PaperFokFullFillCertificateV2 | None = None,
) -> ProspectivePositionOpenPreparePayloadV2:
    """Bind an exact PAPER terminal before any family admission."""

    exact_cell = _exact_plan_cell(plan, cell)
    if not isinstance(sizing_cell, PaperSizingCellV2):
        raise ProspectivePositionLifecycleContractErrorV2("sizing_cell must be PaperSizingCellV2")
    paper_jsonl = canonical_prospective_paper_terminal_payload_v2(paper_terminal)
    _validate_paper_terminal_binding(
        plan=plan,
        cell=exact_cell,
        sizing_cell=sizing_cell,
        paper_terminal=paper_terminal,
    )
    _require_sha256(paper_terminal_record_sha256, "paper_terminal_record_sha256")
    _safe_nonnegative_integer(prepared_at_ms, "prepared_at_ms")
    if prepared_at_ms < exact_cell.decision_cutoff_ms:
        raise ProspectivePositionLifecycleContractErrorV2(
            "open prepare predates the closed-candle decision cutoff"
        )
    if paper_terminal.completeness is ProspectivePaperTerminalCompletenessV2.INCOMPLETE:
        raise ProspectivePositionLifecycleContractErrorV2(
            "an incomplete PAPER terminal cannot decide whether a position exists"
        )
    full_fill = paper_terminal.terminal_status is (
        ProspectivePaperTerminalStatusV2.PAPER_EXECUTED_FULL_QUANTITY
    )
    if full_fill:
        if type(full_fill_certificate) is not PaperFokFullFillCertificateV2:
            raise ProspectivePositionLifecycleContractErrorV2(
                "a full PAPER position requires an exact full-fill certificate"
            )
        certificate_jsonl = canonical_paper_fok_full_fill_certificate_v2(full_fill_certificate)
        _validate_full_fill_binding(paper_terminal, full_fill_certificate)
        certificate_sha256: str | None = full_fill_certificate.certificate_sha256
        certificate_jsonl_sha256: str | None = hashlib.sha256(certificate_jsonl).hexdigest()
        outcome_id = prospective_outcome_id_v2(
            attempt_plan_sha256=plan.plan_sha256,
            origin_segment_id=exact_cell.segment_id,
            origin_cell_id=exact_cell.cell_id,
            sizing_cell=sizing_cell,
        )
        position_id = prospective_position_id_v2(
            outcome_id=outcome_id,
            certificate_sha256=certificate_sha256,
        )
        position_side = _position_side_from_paper(full_fill_certificate.side)
        intent = ProspectivePositionOpenIntentV2.FULL_FILL_POSITION
        reasons = (
            "EXACT_PAPER_FULL_FILL_CERTIFICATE_BOUND",
            "FAMILY_ADMISSION_NOT_YET_DURABLE",
        )
        invalidation = "INVALID_IF_FAMILY_ADMISSION_OR_FULL_FILL_BINDING_DIFFERS"
    else:
        if full_fill_certificate is not None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "a no-position PAPER terminal forbids a full-fill certificate"
            )
        certificate_sha256 = None
        certificate_jsonl_sha256 = None
        position_id = None
        position_side = None
        intent = ProspectivePositionOpenIntentV2.NO_POSITION
        reasons = ("UPSTREAM_PAPER_TERMINAL_PROVES_NO_POSITION",)
        invalidation = "INVALID_IF_UPSTREAM_PAPER_TERMINAL_DIFFERS"
    identity = _build_identity(
        plan=plan,
        cell=exact_cell,
        sizing_cell=sizing_cell,
        position_id=position_id,
        position_side=position_side,
    )
    return ProspectivePositionOpenPreparePayloadV2(
        identity=identity,
        open_intent=intent,
        paper_terminal_payload_sha256=paper_terminal.payload_sha256,
        paper_terminal_jsonl_sha256=hashlib.sha256(paper_jsonl).hexdigest(),
        paper_terminal_record_sha256=paper_terminal_record_sha256,
        full_fill_certificate_sha256=certificate_sha256,
        full_fill_certificate_jsonl_sha256=certificate_jsonl_sha256,
        prepared_at_ms=prepared_at_ms,
        reasons=reasons,
        invalidation=invalidation,
        _factory_token=_FACTORY_TOKEN,
    )


def build_prospective_position_open_disposition_payload_v2(
    *,
    prepare: ProspectivePositionOpenPreparePayloadV2,
    dispositioned_at_ms: int,
    admission_receipt: (
        FamilyAAdmissionReceiptV2 | FamilyBAdmissionReceiptV2 | FamilyCAdmissionReceiptV2 | None
    ) = None,
    _allow_preexisting_recovery: bool = False,
) -> ProspectivePositionOpenDispositionPayloadV2:
    """Project an exact family admission, or suppress an exact no-position."""

    canonical_prospective_position_open_prepare_payload_v2(prepare)
    _safe_nonnegative_integer(dispositioned_at_ms, "dispositioned_at_ms")
    if dispositioned_at_ms < prepare.prepared_at_ms:
        raise ProspectivePositionLifecycleContractErrorV2(
            "open disposition predates its durable prepare"
        )
    if prepare.open_intent is ProspectivePositionOpenIntentV2.NO_POSITION:
        if admission_receipt is not None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "a no-position disposition forbids family admission evidence"
            )
        disposition = ProspectivePositionOpenDispositionV2.SUPPRESSED_NO_POSITION
        admission_evidence = None
        reasons = ("NO_POSITION_SUPPRESSED_WITHOUT_FAMILY_MUTATION",)
        invalidation = "INVALID_IF_ANY_POSITION_OR_ADMISSION_EXISTS"
    else:
        if admission_receipt is None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "a full-fill position requires a factory-sealed family admission receipt"
            )
        admission_evidence = build_prospective_family_admission_evidence_v2(
            prepare=prepare,
            receipt=admission_receipt,
            _allow_preexisting_recovery=_allow_preexisting_recovery,
        )
        disposition = ProspectivePositionOpenDispositionV2.ADMITTED_FULL_FILL
        reasons = (
            "FULL_FILL_PREPARE_PRECEDED_FAMILY_ADMISSION_DISPOSITION",
            (
                "RECOVERED_PREEXISTING_FAMILY_ADMISSION_RECONCILED"
                if admission_evidence.admission_disposition == "PREEXISTING"
                else "LIVE_PROCESS_LOCAL_FAMILY_RECEIPT_PROJECTED"
            ),
        )
        invalidation = "INVALID_IF_ADMISSION_RECEIPT_OR_DURABLE_PREPARE_DIFFERS"
    return ProspectivePositionOpenDispositionPayloadV2(
        identity=prepare.identity,
        prepare_event_id=prepare.event_id,
        prepare_payload_sha256=prepare.payload_sha256,
        disposition=disposition,
        admission_evidence=admission_evidence,
        dispositioned_at_ms=dispositioned_at_ms,
        reasons=reasons,
        invalidation=invalidation,
        _factory_token=_FACTORY_TOKEN,
    )


def build_prospective_family_admission_evidence_v2(
    *,
    prepare: ProspectivePositionOpenPreparePayloadV2,
    receipt: FamilyAAdmissionReceiptV2 | FamilyBAdmissionReceiptV2 | FamilyCAdmissionReceiptV2,
    _allow_preexisting_recovery: bool = False,
) -> ProspectiveFamilyAdmissionEvidenceV2:
    """Project one exact live A/B/C receipt without owner or rollback tokens."""

    canonical_prospective_position_open_prepare_payload_v2(prepare)
    if prepare.open_intent is not ProspectivePositionOpenIntentV2.FULL_FILL_POSITION:
        raise ProspectivePositionLifecycleContractErrorV2(
            "family admission evidence requires a full-fill open prepare"
        )
    if type(receipt) is FamilyAAdmissionReceiptV2:
        family = PromotingFamilyV2.A
        canonical_family_a_entry_decision_v2(receipt.entry_decision)
        certificate = receipt.certificate
        entry_event_id = receipt.entry_decision.event_id
        input_sha256 = receipt.input_sha256
        authority = ProspectiveLifecycleEvidenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_A
    elif type(receipt) is FamilyBAdmissionReceiptV2:
        family = PromotingFamilyV2.B
        canonical_family_b_entry_decision_v2(receipt.decision)
        certificate = receipt.paper_certificate
        entry_event_id = receipt.decision.event_id
        input_sha256 = receipt.input_sha256
        authority = ProspectiveLifecycleEvidenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_B
    elif type(receipt) is FamilyCAdmissionReceiptV2:
        family = PromotingFamilyV2.C
        canonical_family_c_entry_decision_v2(receipt.decision)
        certificate = receipt.paper_certificate
        entry_event_id = receipt.decision.event_id
        input_sha256 = receipt.input_sha256
        authority = ProspectiveLifecycleEvidenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_C
    else:
        raise ProspectivePositionLifecycleContractErrorV2(
            "admission receipt must be an exact Family A, B, or C receipt"
        )
    canonical_paper_fok_full_fill_certificate_v2(certificate)
    identity = prepare.identity
    if family is not identity.family:
        raise ProspectivePositionLifecycleContractErrorV2(
            "admission receipt family differs from the origin cell"
        )
    if (
        certificate.certificate_sha256 != prepare.full_fill_certificate_sha256
        or certificate.attempt_id != identity.attempt_id
        or certificate.promoting_plan_sha256 != identity.promoting_plan_sha256
        or certificate.symbol != identity.symbol
        or certificate.venue is not identity.venue
        or _position_side_from_paper(certificate.side) is not identity.position_side
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "admission receipt full-fill evidence differs from the open prepare"
        )
    disposition = receipt.disposition.value
    _validate_receipt_disposition_for_append(
        disposition=disposition,
        pre_root_sha256=receipt.pre_root_sha256,
        pre_event_count=receipt.pre_event_count,
        post_root_sha256=receipt.post_root_sha256,
        post_event_count=receipt.post_event_count,
        new_event_increment=(1 if family is PromotingFamilyV2.A else 0),
        allow_preexisting_recovery=_allow_preexisting_recovery,
        label="admission",
    )
    return ProspectiveFamilyAdmissionEvidenceV2(
        family=family,
        entry_event_id=entry_event_id,
        full_fill_certificate_sha256=certificate.certificate_sha256,
        admission_input_sha256=input_sha256,
        admission_pre_root_sha256=receipt.pre_root_sha256,
        admission_pre_event_count=receipt.pre_event_count,
        admission_post_root_sha256=receipt.post_root_sha256,
        admission_post_event_count=receipt.post_event_count,
        admission_disposition=disposition,
        source_authority=authority,
        _factory_token=_FACTORY_TOKEN,
    )


def build_prospective_family_exit_prepare_payload_v2(
    *,
    open_disposition: ProspectivePositionOpenDispositionPayloadV2,
    exit_decision: FamilyAExitDecisionV2 | FamilyBExitDecisionV2 | FamilyCExitDecisionV2,
    exit_input_sha256: str,
    prepared_at_ms: int,
) -> ProspectiveFamilyExitPreparePayloadV2:
    """Record an evaluator-sealed decision as an explicitly nonauthoritative preview.

    Current family ledgers lack a common non-mutating, ledger-owned preview API.
    The decision is still exact and typed, but this payload never claims that the
    pre-mutation decision was obtained atomically from the family ledger.
    """

    canonical_prospective_position_open_disposition_payload_v2(open_disposition)
    if open_disposition.disposition is not (
        ProspectivePositionOpenDispositionV2.ADMITTED_FULL_FILL
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "family exit prepare requires an admitted full-fill position"
        )
    admission = open_disposition.admission_evidence
    assert admission is not None
    family, entry_event_id = _exit_decision_projection(exit_decision)
    identity = open_disposition.identity
    if family is not identity.family or entry_event_id != admission.entry_event_id:
        raise ProspectivePositionLifecycleContractErrorV2(
            "exit decision family or entry event differs from admission"
        )
    if (
        exit_decision.attempt_id != identity.attempt_id
        or exit_decision.promoting_plan_sha256 != identity.promoting_plan_sha256
        or exit_decision.symbol != identity.symbol
        or exit_decision.venue is not identity.venue
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "exit decision identity differs from the admitted position"
        )
    _require_sha256(exit_input_sha256, "exit_input_sha256")
    _safe_nonnegative_integer(prepared_at_ms, "prepared_at_ms")
    if prepared_at_ms < exit_decision.decision_cutoff_ms:
        raise ProspectivePositionLifecycleContractErrorV2(
            "family exit prepare predates the decision cutoff"
        )
    return ProspectiveFamilyExitPreparePayloadV2(
        identity=identity,
        open_disposition_event_id=open_disposition.event_id,
        admission_evidence_sha256=admission.evidence_sha256,
        entry_event_id=admission.entry_event_id,
        exit_event_id=exit_decision.event_id,
        exit_decision_payload_sha256=exit_decision.payload_sha256,
        exit_input_sha256=exit_input_sha256,
        exit_decision_cutoff_ms=exit_decision.decision_cutoff_ms,
        exits_position=exit_decision.exits_position,
        exit_decision_authority=(
            ProspectiveLifecycleEvidenceAuthorityV2.EXPLICIT_NONAUTHORITATIVE_HASH_REFERENCE
        ),
        prepared_at_ms=prepared_at_ms,
        reasons=(
            *exit_decision.reasons,
            "NONAUTHORITATIVE_UNTIL_LEDGER_OWNED_EXIT_PREVIEW_EXISTS",
        ),
        invalidation=exit_decision.invalidation,
        _factory_token=_FACTORY_TOKEN,
    )


def build_prospective_family_exit_disposition_payload_v2(
    *,
    prepare: ProspectiveFamilyExitPreparePayloadV2,
    exit_receipt: (
        FamilyAExitMutationReceiptV2 | FamilyBExitMutationReceiptV2 | FamilyCExitMutationReceiptV2
    ),
    dispositioned_at_ms: int,
    _allow_preexisting_recovery: bool = False,
) -> ProspectiveFamilyExitDispositionPayloadV2:
    """Bind the exact post-mutation A/B/C receipt to its durable prepare."""

    canonical_prospective_family_exit_prepare_payload_v2(prepare)
    evidence = build_prospective_family_exit_evidence_v2(
        prepare=prepare,
        receipt=exit_receipt,
        _allow_preexisting_recovery=_allow_preexisting_recovery,
    )
    _safe_nonnegative_integer(dispositioned_at_ms, "dispositioned_at_ms")
    if dispositioned_at_ms < prepare.prepared_at_ms:
        raise ProspectivePositionLifecycleContractErrorV2(
            "family exit disposition predates its durable prepare"
        )
    return ProspectiveFamilyExitDispositionPayloadV2(
        identity=prepare.identity,
        exit_prepare_event_id=prepare.event_id,
        exit_prepare_payload_sha256=prepare.payload_sha256,
        exit_evidence=evidence,
        dispositioned_at_ms=dispositioned_at_ms,
        reasons=(
            "FACTORY_SEALED_FAMILY_EXIT_RECEIPT_PROJECTED",
            (
                "RECOVERED_PREEXISTING_FAMILY_EXIT_RECONCILED"
                if evidence.exit_disposition == "PREEXISTING"
                else "LIVE_NEW_FAMILY_EXIT_MUTATION_RECONCILED"
            ),
            (
                "POSITION_EXIT_BECAME_TERMINAL"
                if evidence.terminal_exit
                else "POSITION_REMAINS_OPEN_AFTER_HOLD"
            ),
        ),
        invalidation="INVALID_IF_EXIT_RECEIPT_OR_PREPARE_DIFFERS",
        _factory_token=_FACTORY_TOKEN,
    )


def build_prospective_family_exit_evidence_v2(
    *,
    prepare: ProspectiveFamilyExitPreparePayloadV2,
    receipt: (
        FamilyAExitMutationReceiptV2 | FamilyBExitMutationReceiptV2 | FamilyCExitMutationReceiptV2
    ),
    _allow_preexisting_recovery: bool = False,
) -> ProspectiveFamilyExitEvidenceV2:
    canonical_prospective_family_exit_prepare_payload_v2(prepare)
    if type(receipt) is FamilyAExitMutationReceiptV2:
        family = PromotingFamilyV2.A
        canonical_family_a_exit_decision_v2(receipt.decision)
        authority = ProspectiveLifecycleEvidenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_A
        terminal_exit = receipt.post_terminal and not receipt.post_active
    elif type(receipt) is FamilyBExitMutationReceiptV2:
        family = PromotingFamilyV2.B
        canonical_family_b_exit_decision_v2(receipt.decision)
        authority = ProspectiveLifecycleEvidenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_B
        terminal_exit = receipt.post_terminal and receipt.post_active_entry_event_id is None
    elif type(receipt) is FamilyCExitMutationReceiptV2:
        family = PromotingFamilyV2.C
        canonical_family_c_exit_decision_v2(receipt.decision)
        authority = ProspectiveLifecycleEvidenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_C
        terminal_exit = receipt.post_terminal and receipt.post_active_entry_event_id is None
    else:
        raise ProspectivePositionLifecycleContractErrorV2(
            "exit receipt must be an exact Family A, B, or C receipt"
        )
    if family is not prepare.identity.family:
        raise ProspectivePositionLifecycleContractErrorV2(
            "exit receipt family differs from the origin position"
        )
    decision = receipt.decision
    if (
        receipt.entry_event_id != decision.entry_event_id
        or receipt.entry_event_id != prepare.entry_event_id
        or receipt.input_sha256 != prepare.exit_input_sha256
        or decision.event_id != prepare.exit_event_id
        or decision.payload_sha256 != prepare.exit_decision_payload_sha256
        or decision.decision_cutoff_ms != prepare.exit_decision_cutoff_ms
        or decision.exits_position != prepare.exits_position
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "exit receipt differs from the exact prepared decision"
        )
    _validate_receipt_disposition_for_append(
        disposition=receipt.disposition.value,
        pre_root_sha256=receipt.pre_root_sha256,
        pre_event_count=receipt.pre_event_count,
        post_root_sha256=receipt.post_root_sha256,
        post_event_count=receipt.post_event_count,
        new_event_increment=1,
        allow_preexisting_recovery=_allow_preexisting_recovery,
        label="exit",
    )
    if terminal_exit != prepare.exits_position:
        raise ProspectivePositionLifecycleContractErrorV2(
            "exit receipt terminal state contradicts the prepared decision"
        )
    return ProspectiveFamilyExitEvidenceV2(
        family=family,
        entry_event_id=receipt.entry_event_id,
        exit_event_id=decision.event_id,
        exit_decision_payload_sha256=decision.payload_sha256,
        exit_input_sha256=receipt.input_sha256,
        exit_pre_root_sha256=receipt.pre_root_sha256,
        exit_pre_event_count=receipt.pre_event_count,
        exit_post_root_sha256=receipt.post_root_sha256,
        exit_post_event_count=receipt.post_event_count,
        exit_disposition=receipt.disposition.value,
        exit_decision_cutoff_ms=decision.decision_cutoff_ms,
        terminal_exit=terminal_exit,
        source_authority=authority,
        _factory_token=_FACTORY_TOKEN,
    )


def build_prospective_position_cashflow_payload_v2(
    *,
    terminal_exit: ProspectiveFamilyExitDispositionPayloadV2,
    cashflow_class: ProspectivePositionCashflowClassV2,
    evidence: (
        PaperFokFullFillCertificateV2
        | MandatoryExitEvidenceReferenceV2
        | FinalFeeEvidenceReferenceV2
        | FundingCensusEvidenceReferenceV2
    ),
    observed_at_ms: int,
) -> ProspectivePositionCashflowPayloadV2:
    """Derive one signed cashflow from an exact typed evidence object."""

    canonical_prospective_family_exit_disposition_payload_v2(terminal_exit)
    if not terminal_exit.exit_evidence.terminal_exit:
        raise ProspectivePositionLifecycleContractErrorV2(
            "cashflows are forbidden before a terminal family exit"
        )
    if not isinstance(cashflow_class, ProspectivePositionCashflowClassV2):
        raise ProspectivePositionLifecycleContractErrorV2(
            "cashflow_class must be ProspectivePositionCashflowClassV2"
        )
    identity = terminal_exit.identity
    amount, reference_sha256, authority = _cashflow_from_evidence(
        identity=identity,
        cashflow_class=cashflow_class,
        evidence=evidence,
    )
    _safe_nonnegative_integer(observed_at_ms, "observed_at_ms")
    if observed_at_ms < terminal_exit.dispositioned_at_ms:
        raise ProspectivePositionLifecycleContractErrorV2(
            "cashflow observation predates the terminal family exit"
        )
    evidence_observed_through_ms: int | None = None
    if type(evidence) is PaperFokFullFillCertificateV2:
        evidence_observed_through_ms = evidence.target_venue_ms
    elif type(evidence) is MandatoryExitEvidenceReferenceV2:
        evidence_observed_through_ms = evidence.terminal_at_ms
    elif type(evidence) is FundingCensusEvidenceReferenceV2:
        evidence_observed_through_ms = evidence.observed_through_ms
    if evidence_observed_through_ms is not None and observed_at_ms < evidence_observed_through_ms:
        raise ProspectivePositionLifecycleContractErrorV2(
            "cashflow observation predates its typed evidence horizon"
        )
    return ProspectivePositionCashflowPayloadV2(
        identity=identity,
        terminal_exit_event_id=terminal_exit.exit_evidence.exit_event_id,
        terminal_exit_evidence_sha256=terminal_exit.exit_evidence.evidence_sha256,
        cashflow_class=cashflow_class,
        signed_amount_usdt=amount,
        evidence_reference_sha256=reference_sha256,
        evidence_authority=authority,
        observed_at_ms=observed_at_ms,
        reasons=(f"SIGNED_{cashflow_class.value}_DERIVED_FROM_TYPED_EVIDENCE",),
        invalidation="INVALID_IF_TYPED_EVIDENCE_OR_POSITION_BINDING_DIFFERS",
        _factory_token=_FACTORY_TOKEN,
    )


def canonical_prospective_position_open_prepare_payload_v2(
    payload: ProspectivePositionOpenPreparePayloadV2,
) -> bytes:
    return _canonical_payload(payload, ProspectivePositionOpenPreparePayloadV2)


def canonical_prospective_position_open_disposition_payload_v2(
    payload: ProspectivePositionOpenDispositionPayloadV2,
) -> bytes:
    return _canonical_payload(payload, ProspectivePositionOpenDispositionPayloadV2)


def canonical_prospective_family_exit_prepare_payload_v2(
    payload: ProspectiveFamilyExitPreparePayloadV2,
) -> bytes:
    return _canonical_payload(payload, ProspectiveFamilyExitPreparePayloadV2)


def canonical_prospective_family_exit_disposition_payload_v2(
    payload: ProspectiveFamilyExitDispositionPayloadV2,
) -> bytes:
    return _canonical_payload(payload, ProspectiveFamilyExitDispositionPayloadV2)


def canonical_prospective_position_cashflow_payload_v2(
    payload: ProspectivePositionCashflowPayloadV2,
) -> bytes:
    return _canonical_payload(payload, ProspectivePositionCashflowPayloadV2)


def parse_prospective_position_open_prepare_payload_v2(
    encoded: bytes,
) -> ProspectivePositionOpenPreparePayloadV2:
    document = _decode_exact_payload(encoded, POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2)
    payload = ProspectivePositionOpenPreparePayloadV2(
        identity=_parse_identity(_object(document, "identity")),
        open_intent=ProspectivePositionOpenIntentV2(_text(document, "open_intent")),
        paper_terminal_payload_sha256=_text(document, "paper_terminal_payload_sha256"),
        paper_terminal_jsonl_sha256=_text(document, "paper_terminal_jsonl_sha256"),
        paper_terminal_record_sha256=_optional_text(document, "paper_terminal_record_sha256"),
        full_fill_certificate_sha256=_optional_text(document, "full_fill_certificate_sha256"),
        full_fill_certificate_jsonl_sha256=_optional_text(
            document, "full_fill_certificate_jsonl_sha256"
        ),
        prepared_at_ms=_integer(document, "prepared_at_ms"),
        reasons=_string_tuple(document, "reasons"),
        invalidation=_text(document, "invalidation"),
        _factory_token=_FACTORY_TOKEN,
    )
    _require_stored_hashes(payload, document)
    if canonical_prospective_position_open_prepare_payload_v2(payload) != encoded:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored open prepare differs from its strict typed reconstruction"
        )
    return payload


def parse_prospective_position_open_disposition_payload_v2(
    encoded: bytes,
) -> ProspectivePositionOpenDispositionPayloadV2:
    document = _decode_exact_payload(encoded, POSITION_OPEN_DISPOSITION_PAYLOAD_SCHEMA_V2)
    admission_document = document.get("admission_evidence")
    admission = (
        None
        if admission_document is None
        else _parse_admission_evidence(_cast_object(admission_document, "admission_evidence"))
    )
    payload = ProspectivePositionOpenDispositionPayloadV2(
        identity=_parse_identity(_object(document, "identity")),
        prepare_event_id=_text(document, "prepare_event_id"),
        prepare_payload_sha256=_text(document, "prepare_payload_sha256"),
        disposition=ProspectivePositionOpenDispositionV2(_text(document, "disposition")),
        admission_evidence=admission,
        dispositioned_at_ms=_integer(document, "dispositioned_at_ms"),
        reasons=_string_tuple(document, "reasons"),
        invalidation=_text(document, "invalidation"),
        _factory_token=_FACTORY_TOKEN,
    )
    _require_stored_hashes(payload, document)
    if canonical_prospective_position_open_disposition_payload_v2(payload) != encoded:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored open disposition differs from its strict typed reconstruction"
        )
    return payload


def parse_prospective_family_exit_prepare_payload_v2(
    encoded: bytes,
) -> ProspectiveFamilyExitPreparePayloadV2:
    document = _decode_exact_payload(encoded, FAMILY_EXIT_PREPARE_PAYLOAD_SCHEMA_V2)
    payload = ProspectiveFamilyExitPreparePayloadV2(
        identity=_parse_identity(_object(document, "identity")),
        open_disposition_event_id=_text(document, "open_disposition_event_id"),
        admission_evidence_sha256=_text(document, "admission_evidence_sha256"),
        entry_event_id=_text(document, "entry_event_id"),
        exit_event_id=_text(document, "exit_event_id"),
        exit_decision_payload_sha256=_text(document, "exit_decision_payload_sha256"),
        exit_input_sha256=_text(document, "exit_input_sha256"),
        exit_decision_cutoff_ms=_integer(document, "exit_decision_cutoff_ms"),
        exits_position=_boolean(document, "exits_position"),
        exit_decision_authority=ProspectiveLifecycleEvidenceAuthorityV2(
            _text(document, "exit_decision_authority")
        ),
        prepared_at_ms=_integer(document, "prepared_at_ms"),
        reasons=_string_tuple(document, "reasons"),
        invalidation=_text(document, "invalidation"),
        _factory_token=_FACTORY_TOKEN,
    )
    _require_stored_hashes(payload, document)
    if canonical_prospective_family_exit_prepare_payload_v2(payload) != encoded:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored exit prepare differs from its strict typed reconstruction"
        )
    return payload


def parse_prospective_family_exit_disposition_payload_v2(
    encoded: bytes,
) -> ProspectiveFamilyExitDispositionPayloadV2:
    document = _decode_exact_payload(encoded, FAMILY_EXIT_DISPOSITION_PAYLOAD_SCHEMA_V2)
    payload = ProspectiveFamilyExitDispositionPayloadV2(
        identity=_parse_identity(_object(document, "identity")),
        exit_prepare_event_id=_text(document, "exit_prepare_event_id"),
        exit_prepare_payload_sha256=_text(document, "exit_prepare_payload_sha256"),
        exit_evidence=_parse_exit_evidence(_object(document, "exit_evidence")),
        dispositioned_at_ms=_integer(document, "dispositioned_at_ms"),
        reasons=_string_tuple(document, "reasons"),
        invalidation=_text(document, "invalidation"),
        _factory_token=_FACTORY_TOKEN,
    )
    _require_stored_hashes(payload, document)
    if canonical_prospective_family_exit_disposition_payload_v2(payload) != encoded:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored exit disposition differs from its strict typed reconstruction"
        )
    return payload


def parse_prospective_position_cashflow_payload_v2(
    encoded: bytes,
) -> ProspectivePositionCashflowPayloadV2:
    document = _decode_exact_payload(encoded, POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2)
    payload = ProspectivePositionCashflowPayloadV2(
        identity=_parse_identity(_object(document, "identity")),
        terminal_exit_event_id=_text(document, "terminal_exit_event_id"),
        terminal_exit_evidence_sha256=_text(document, "terminal_exit_evidence_sha256"),
        cashflow_class=ProspectivePositionCashflowClassV2(_text(document, "cashflow_class")),
        signed_amount_usdt=_decimal(document, "signed_amount_usdt"),
        evidence_reference_sha256=_text(document, "evidence_reference_sha256"),
        evidence_authority=ProspectiveLifecycleEvidenceAuthorityV2(
            _text(document, "evidence_authority")
        ),
        observed_at_ms=_integer(document, "observed_at_ms"),
        reasons=_string_tuple(document, "reasons"),
        invalidation=_text(document, "invalidation"),
        _factory_token=_FACTORY_TOKEN,
    )
    _require_stored_hashes(payload, document)
    if canonical_prospective_position_cashflow_payload_v2(payload) != encoded:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored cashflow differs from its strict typed reconstruction"
        )
    return payload


@dataclass(slots=True)
class _MutableTypedOutcomeStateV2:
    identity: ProspectivePositionLifecycleIdentityV2
    phase: ProspectiveTypedLifecyclePhaseV2
    open_prepare: ProspectivePositionOpenPreparePayloadV2
    open_disposition: ProspectivePositionOpenDispositionPayloadV2 | None
    admission_evidence: ProspectiveFamilyAdmissionEvidenceV2 | None
    pending_exit_prepare: ProspectiveFamilyExitPreparePayloadV2 | None
    terminal_exit: ProspectiveFamilyExitDispositionPayloadV2 | None
    cashflows: dict[
        ProspectivePositionCashflowClassV2,
        ProspectivePositionCashflowPayloadV2,
    ]
    terminal_payload_sha256: str | None
    record_count: int
    completed_exit_pair_count: int
    last_record_sha256: str
    last_transition_at_ms: int


@dataclass(frozen=True, slots=True)
class ProspectivePositionLifecycleOutcomeSnapshotV2:
    outcome_id: str
    position_id: str | None
    phase: ProspectiveTypedLifecyclePhaseV2
    cashflow_classes: tuple[ProspectivePositionCashflowClassV2, ...]
    terminal_exit_event_id: str | None
    terminal_payload_sha256: str | None
    record_count: int
    completed_exit_pair_count: int
    last_record_sha256: str


@dataclass(frozen=True, slots=True)
class ProspectivePositionLifecycleSnapshotV2:
    attempt_plan_sha256: str
    store_snapshot_sha256: str
    outcomes: tuple[ProspectivePositionLifecycleOutcomeSnapshotV2, ...]
    _factory_token: InitVar[object | None] = None

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ProspectivePositionLifecycleContractErrorV2(
                "typed lifecycle snapshots are factory-sealed"
            )
        _require_sha256(self.attempt_plan_sha256, "attempt_plan_sha256")
        _require_sha256(self.store_snapshot_sha256, "store_snapshot_sha256")
        if tuple(item.outcome_id for item in self.outcomes) != tuple(
            sorted(item.outcome_id for item in self.outcomes)
        ):
            raise ProspectivePositionLifecycleIntegrityErrorV2(
                "typed lifecycle snapshot outcomes must be sorted"
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
class ProspectivePositionLifecycleOwnerFactoryV2:
    """Only factory allowed to mint and attach the typed store capability."""

    config: ProspectivePositionLifecycleOwnerConfigV2 = field(
        default_factory=ProspectivePositionLifecycleOwnerConfigV2
    )

    def __post_init__(self) -> None:
        if type(self.config) is not ProspectivePositionLifecycleOwnerConfigV2:
            raise ProspectivePositionLifecycleContractErrorV2(
                "config must be exact ProspectivePositionLifecycleOwnerConfigV2"
            )

    def open_fresh_v2(
        self,
        *,
        plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
        outcome_store: ProspectiveOutcomeWalStoreV2,
    ) -> ProspectivePositionLifecycleOwnerV2:
        """Claim a fresh empty outcome store as its sole typed owner."""

        _validate_owner_attempt(plan, writer_lease, outcome_store)
        if outcome_store.record_count != 0:
            raise ProspectivePositionLifecycleContractErrorV2(
                "fresh typed owner requires an empty outcome WAL"
            )
        owner = ProspectivePositionLifecycleOwnerV2(
            plan=plan,
            writer_lease=writer_lease,
            factory=self,
            outcome_store=outcome_store,
            states={},
            replay_snapshot=None,
            recovery_pending_outcomes=set(),
            _factory_token=_OWNER_FACTORY_TOKEN,
        )
        owner._claim_fresh_v2()  # pyright: ignore[reportPrivateUsage]
        return owner

    def prepare_recovery_v2(
        self,
        *,
        plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
        replay_snapshot: ProspectiveOutcomeWalReplaySnapshotV2,
    ) -> ProspectivePositionLifecycleOwnerV2:
        """Strictly rebuild typed state before the lower store is reopened.

        The returned exact object must be supplied as ``recovered_state_owner``
        to ``ProspectiveOutcomeWalStoreFactoryV2.open`` and then attached with
        ``attach_recovered_store_v2``.  No caller JSON is accepted.
        """

        _validate_plan_and_lease(plan, writer_lease)
        if type(replay_snapshot) is not ProspectiveOutcomeWalReplaySnapshotV2:
            raise ProspectivePositionLifecycleContractErrorV2(
                "recovery requires an exact outcome WAL replay snapshot"
            )
        if replay_snapshot.attempt_plan_sha256 != plan.plan_sha256:
            raise ProspectivePositionLifecycleIntegrityErrorV2(
                "replay snapshot differs from the lifecycle attempt plan"
            )
        states = _rebuild_typed_states(
            plan=plan,
            replay_snapshot=replay_snapshot,
            config=self.config,
        )
        return ProspectivePositionLifecycleOwnerV2(
            plan=plan,
            writer_lease=writer_lease,
            factory=self,
            outcome_store=None,
            states=states,
            replay_snapshot=replay_snapshot,
            recovery_pending_outcomes={
                outcome_id
                for outcome_id, state in states.items()
                if state.phase
                in (
                    ProspectiveTypedLifecyclePhaseV2.OPEN_PREPARED,
                    ProspectiveTypedLifecyclePhaseV2.EXIT_PREPARED,
                )
            },
            _factory_token=_OWNER_FACTORY_TOKEN,
        )


class ProspectivePositionLifecycleOwnerV2:
    """Single typed transition owner above ``ProspectiveOutcomeWalStoreV2``."""

    __slots__ = (
        "_claim",
        "_factory",
        "_failed",
        "_outcome_store",
        "_plan",
        "_recovery_pending_outcomes",
        "_replay_snapshot",
        "_states",
        "_writer_lease",
    )

    def __init__(
        self,
        *,
        plan: ProspectiveCensusPlanV2,
        writer_lease: WriterLease,
        factory: ProspectivePositionLifecycleOwnerFactoryV2,
        outcome_store: ProspectiveOutcomeWalStoreV2 | None,
        states: dict[str, _MutableTypedOutcomeStateV2],
        replay_snapshot: ProspectiveOutcomeWalReplaySnapshotV2 | None,
        recovery_pending_outcomes: set[str],
        _factory_token: object | None = None,
    ) -> None:
        if _factory_token is not _OWNER_FACTORY_TOKEN:
            raise ProspectivePositionLifecycleContractErrorV2(
                "position lifecycle owners are factory-sealed"
            )
        self._plan = plan
        self._writer_lease = writer_lease
        self._factory = factory
        self._outcome_store = outcome_store
        self._states = states
        self._replay_snapshot = replay_snapshot
        self._recovery_pending_outcomes = recovery_pending_outcomes
        self._claim: object | None = None
        self._failed: Exception | None = None

    @property
    def outcome_count(self) -> int:
        return len(self._states)

    @property
    def active_outcome_count(self) -> int:
        return sum(
            state.phase is not ProspectiveTypedLifecyclePhaseV2.TERMINAL
            for state in self._states.values()
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

    def attach_recovered_store_v2(
        self,
        outcome_store: ProspectiveOutcomeWalStoreV2,
    ) -> None:
        """Complete the exact recovery-owner handoff exactly once."""

        if self._replay_snapshot is None or self._outcome_store is not None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "owner is not an unattached recovery handoff"
            )
        _validate_owner_attempt(self._plan, self._writer_lease, outcome_store)
        with self._writer_lease.operation_guard():
            claim = outcome_store._claim_position_lifecycle_owner_v2(  # pyright: ignore[reportPrivateUsage]
                census_plan=self._plan,
                writer_lease=self._writer_lease,
                lifecycle_owner=self,
                replay_snapshot=self._replay_snapshot,
            )
            self._outcome_store = outcome_store
            self._claim = claim

    def prepare_position_open_v2(
        self,
        *,
        cell: ProspectiveExpectedCellV2,
        sizing_cell: PaperSizingCellV2,
        paper_terminal: ProspectivePaperTerminalPayloadV2,
        paper_terminal_record_sha256: str,
        prepared_at_ms: int,
        full_fill_certificate: PaperFokFullFillCertificateV2 | None = None,
    ) -> ProspectivePositionLifecycleDurableReceiptV2:
        with self._writer_lease.operation_guard():
            payload = build_prospective_position_open_prepare_payload_v2(
                plan=self._plan,
                cell=cell,
                sizing_cell=sizing_cell,
                paper_terminal=paper_terminal,
                paper_terminal_record_sha256=paper_terminal_record_sha256,
                prepared_at_ms=prepared_at_ms,
                full_fill_certificate=full_fill_certificate,
            )
            return self._append_payload_guarded(payload)

    def disposition_position_open_v2(
        self,
        *,
        prepare: ProspectivePositionOpenPreparePayloadV2,
        dispositioned_at_ms: int,
        admission_receipt: (
            FamilyAAdmissionReceiptV2 | FamilyBAdmissionReceiptV2 | FamilyCAdmissionReceiptV2 | None
        ) = None,
    ) -> ProspectivePositionLifecycleDurableReceiptV2:
        with self._writer_lease.operation_guard():
            recovery_reconciliation = self._is_recovered_pending_phase_v2(
                outcome_id=prepare.identity.outcome_id,
                phase=ProspectiveTypedLifecyclePhaseV2.OPEN_PREPARED,
            )
            payload = build_prospective_position_open_disposition_payload_v2(
                prepare=prepare,
                dispositioned_at_ms=dispositioned_at_ms,
                admission_receipt=admission_receipt,
                _allow_preexisting_recovery=recovery_reconciliation,
            )
            durable = self._append_payload_guarded(payload)
            self._recovery_pending_outcomes.discard(prepare.identity.outcome_id)
            return durable

    def prepare_family_exit_v2(
        self,
        *,
        open_disposition: ProspectivePositionOpenDispositionPayloadV2,
        exit_decision: (FamilyAExitDecisionV2 | FamilyBExitDecisionV2 | FamilyCExitDecisionV2),
        exit_input_sha256: str,
        prepared_at_ms: int,
    ) -> ProspectivePositionLifecycleDurableReceiptV2:
        with self._writer_lease.operation_guard():
            payload = build_prospective_family_exit_prepare_payload_v2(
                open_disposition=open_disposition,
                exit_decision=exit_decision,
                exit_input_sha256=exit_input_sha256,
                prepared_at_ms=prepared_at_ms,
            )
            return self._append_payload_guarded(payload)

    def disposition_family_exit_v2(
        self,
        *,
        prepare: ProspectiveFamilyExitPreparePayloadV2,
        exit_receipt: (
            FamilyAExitMutationReceiptV2
            | FamilyBExitMutationReceiptV2
            | FamilyCExitMutationReceiptV2
        ),
        dispositioned_at_ms: int,
    ) -> ProspectivePositionLifecycleDurableReceiptV2:
        with self._writer_lease.operation_guard():
            recovery_reconciliation = self._is_recovered_pending_phase_v2(
                outcome_id=prepare.identity.outcome_id,
                phase=ProspectiveTypedLifecyclePhaseV2.EXIT_PREPARED,
            )
            payload = build_prospective_family_exit_disposition_payload_v2(
                prepare=prepare,
                exit_receipt=exit_receipt,
                dispositioned_at_ms=dispositioned_at_ms,
                _allow_preexisting_recovery=recovery_reconciliation,
            )
            durable = self._append_payload_guarded(payload)
            self._recovery_pending_outcomes.discard(prepare.identity.outcome_id)
            return durable

    def append_position_cashflow_v2(
        self,
        *,
        terminal_exit: ProspectiveFamilyExitDispositionPayloadV2,
        cashflow_class: ProspectivePositionCashflowClassV2,
        evidence: (
            PaperFokFullFillCertificateV2
            | MandatoryExitEvidenceReferenceV2
            | FinalFeeEvidenceReferenceV2
            | FundingCensusEvidenceReferenceV2
        ),
        observed_at_ms: int,
    ) -> ProspectivePositionLifecycleDurableReceiptV2:
        with self._writer_lease.operation_guard():
            payload = build_prospective_position_cashflow_payload_v2(
                terminal_exit=terminal_exit,
                cashflow_class=cashflow_class,
                evidence=evidence,
                observed_at_ms=observed_at_ms,
            )
            return self._append_payload_guarded(payload)

    def finalize_position_v2(
        self,
        terminal: ProspectivePositionTerminalPayloadV2,
    ) -> ProspectivePositionLifecycleDurableReceiptV2:
        """Append the one final terminal only after exact WAL reconciliation."""

        with self._writer_lease.operation_guard():
            encoded = canonical_prospective_position_terminal_payload_v2(terminal)
            projection = _parse_terminal_projection(encoded)
            return self._append_payload_guarded(
                terminal,
                canonical_payload=encoded,
                terminal_projection=projection,
            )

    def snapshot_v2(self) -> ProspectivePositionLifecycleSnapshotV2:
        self._raise_if_unavailable()
        store = self._required_store()
        claim = self._required_claim()
        with self._writer_lease.operation_guard():
            store_snapshot = store.replay_snapshot_v2(lifecycle_claim=claim)
            outcomes = tuple(
                ProspectivePositionLifecycleOutcomeSnapshotV2(
                    outcome_id=state.identity.outcome_id,
                    position_id=state.identity.position_id,
                    phase=state.phase,
                    cashflow_classes=tuple(sorted(state.cashflows, key=str)),
                    terminal_exit_event_id=(
                        None
                        if state.terminal_exit is None
                        else state.terminal_exit.exit_evidence.exit_event_id
                    ),
                    terminal_payload_sha256=state.terminal_payload_sha256,
                    record_count=state.record_count,
                    completed_exit_pair_count=state.completed_exit_pair_count,
                    last_record_sha256=state.last_record_sha256,
                )
                for state in sorted(
                    self._states.values(), key=lambda item: item.identity.outcome_id
                )
            )
            return ProspectivePositionLifecycleSnapshotV2(
                attempt_plan_sha256=self._plan.plan_sha256,
                store_snapshot_sha256=store_snapshot.snapshot_sha256,
                outcomes=outcomes,
                _factory_token=_FACTORY_TOKEN,
            )

    def _claim_fresh_v2(self) -> None:
        store = self._required_store()
        with self._writer_lease.operation_guard():
            self._claim = store._claim_position_lifecycle_owner_v2(  # pyright: ignore[reportPrivateUsage]
                census_plan=self._plan,
                writer_lease=self._writer_lease,
                lifecycle_owner=self,
            )

    def _append_payload_guarded(
        self,
        payload: _PayloadV2 | ProspectivePositionTerminalPayloadV2,
        *,
        canonical_payload: bytes | None = None,
        terminal_projection: _TerminalProjectionV2 | None = None,
    ) -> ProspectivePositionLifecycleDurableReceiptV2:
        self._raise_if_unavailable()
        store = self._required_store()
        claim = self._required_claim()
        if canonical_payload is None:
            if isinstance(payload, ProspectivePositionTerminalPayloadV2):
                raise ProspectivePositionLifecycleIntegrityErrorV2(
                    "terminal append requires its prevalidated canonical bytes"
                )
            canonical_payload = _canonical_payload_exact(payload)
        identity = (
            terminal_projection.identity
            if terminal_projection is not None
            else cast(_PayloadV2, payload).identity
        )
        if identity.attempt_plan_sha256 != self._plan.plan_sha256:
            raise ProspectivePositionLifecycleContractErrorV2(
                "payload identity differs from the owner attempt"
            )
        current = self._states.get(identity.outcome_id)
        planned = _plan_typed_transition(
            current=current,
            payload=payload,
            terminal_projection=terminal_projection,
            maximum_outcomes=self._factory.config.maximum_outcomes,
            current_outcome_count=len(self._states),
            durable_record_sha256="0" * 64,
        )
        cell = _cell_for_identity(self._plan, identity)
        item = ProspectiveOutcomeWalAppendItemV2(
            origin_cell=cell,
            sizing_cell=identity.sizing_cell,
            kind=_record_kind(payload),
            canonical_payload_jsonl=canonical_payload,
        )
        try:
            wal_receipt = store.append_and_sync(item=item, lifecycle_claim=claim)
            durable = wal_receipt.records[0]
            if (
                durable.outcome_id != identity.outcome_id
                or durable.kind is not item.kind
                or durable.payload_sha256 != hashlib.sha256(canonical_payload).hexdigest()
            ):
                raise ProspectivePositionLifecycleIntegrityErrorV2(
                    "durable outcome receipt differs from the planned typed operation"
                )
            planned.last_record_sha256 = durable.record_sha256
            self._states[identity.outcome_id] = planned
            return ProspectivePositionLifecycleDurableReceiptV2(
                payload=payload,
                wal_receipt=wal_receipt,
            )
        except Exception as exc:
            self._failed = exc
            raise

    def _required_store(self) -> ProspectiveOutcomeWalStoreV2:
        if self._outcome_store is None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "recovery owner must attach its exact reopened store first"
            )
        return self._outcome_store

    def _is_recovered_pending_phase_v2(
        self,
        *,
        outcome_id: str,
        phase: ProspectiveTypedLifecyclePhaseV2,
    ) -> bool:
        if outcome_id not in self._recovery_pending_outcomes:
            return False
        state = self._states.get(outcome_id)
        return state is not None and state.phase is phase

    def _required_claim(self) -> object:
        if self._claim is None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "typed lifecycle owner has not claimed the outcome WAL"
            )
        return self._claim

    def _raise_if_unavailable(self) -> None:
        if self._failed is not None:
            raise ProspectivePositionLifecycleFailedErrorV2(
                "typed lifecycle owner is failed"
            ) from self._failed


def _seal_payload(payload: _PayloadV2, factory_token: object | None) -> None:
    if factory_token is not _FACTORY_TOKEN:
        raise ProspectivePositionLifecycleContractErrorV2(
            "position lifecycle payloads are factory-sealed"
        )
    _validate_payload_shape(payload)
    event_document = _payload_document(
        payload,
        include_event_id=False,
        include_payload_sha256=False,
    )
    object.__setattr__(
        payload,
        "event_id",
        _hash_document(_event_domain(payload), event_document),
    )
    object.__setattr__(
        payload,
        "payload_sha256",
        _hash_document(
            _payload_domain(payload),
            _payload_document(
                payload,
                include_event_id=True,
                include_payload_sha256=False,
            ),
        ),
    )


def _canonical_payload(payload: _PayloadV2, expected_type: type[_PayloadV2]) -> bytes:
    if type(payload) is not expected_type:
        raise TypeError(f"payload must be exact {expected_type.__name__}")
    _verify_payload(payload)
    encoded = canonical_json_line(
        _payload_document(
            payload,
            include_event_id=True,
            include_payload_sha256=True,
        )
    )
    if len(encoded) > MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2:
        raise ProspectivePositionLifecycleContractErrorV2(
            "position lifecycle payload exceeds the fixed 64 KiB bound"
        )
    return encoded


def _canonical_payload_exact(payload: _PayloadV2) -> bytes:
    if type(payload) is ProspectivePositionOpenPreparePayloadV2:
        return canonical_prospective_position_open_prepare_payload_v2(payload)
    if type(payload) is ProspectivePositionOpenDispositionPayloadV2:
        return canonical_prospective_position_open_disposition_payload_v2(payload)
    if type(payload) is ProspectiveFamilyExitPreparePayloadV2:
        return canonical_prospective_family_exit_prepare_payload_v2(payload)
    if type(payload) is ProspectiveFamilyExitDispositionPayloadV2:
        return canonical_prospective_family_exit_disposition_payload_v2(payload)
    if type(payload) is ProspectivePositionCashflowPayloadV2:
        return canonical_prospective_position_cashflow_payload_v2(payload)
    raise TypeError("unsupported typed lifecycle payload")


def _canonical_payload_sha256(
    payload: _PayloadV2 | ProspectivePositionTerminalPayloadV2,
) -> str:
    encoded = (
        canonical_prospective_position_terminal_payload_v2(payload)
        if type(payload) is ProspectivePositionTerminalPayloadV2
        else _canonical_payload_exact(cast(_PayloadV2, payload))
    )
    return hashlib.sha256(encoded).hexdigest()


def _verify_payload(payload: _PayloadV2) -> None:
    _validate_payload_shape(payload)
    expected_event = _hash_document(
        _event_domain(payload),
        _payload_document(
            payload,
            include_event_id=False,
            include_payload_sha256=False,
        ),
    )
    if not hmac.compare_digest(payload.event_id, expected_event):
        raise ProspectivePositionLifecycleContractErrorV2(
            "lifecycle payload event ID differs from canonical content"
        )
    expected_payload = _hash_document(
        _payload_domain(payload),
        _payload_document(
            payload,
            include_event_id=True,
            include_payload_sha256=False,
        ),
    )
    if not hmac.compare_digest(payload.payload_sha256, expected_payload):
        raise ProspectivePositionLifecycleContractErrorV2(
            "lifecycle payload hash differs from canonical content"
        )


def _validate_payload_shape(payload: _PayloadV2) -> None:
    _verify_identity(payload.identity)
    _reasons(payload.reasons)
    _identity_text(payload.invalidation, "invalidation")
    if payload.rule_version != PROSPECTIVE_POSITION_LIFECYCLE_RULE_VERSION_V2:
        raise ProspectivePositionLifecycleContractErrorV2("lifecycle rule version differs")
    if payload.authority_status != PROSPECTIVE_POSITION_LIFECYCLE_AUTHORITY_V2:
        raise ProspectivePositionLifecycleContractErrorV2("lifecycle authority status differs")
    if (
        payload.typed_payload_semantics_authoritative
        or payload.efficacy_eligible
        or payload.production_order_placement
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "draft lifecycle payload cannot claim authority, efficacy, or execution"
        )
    identity = payload.identity
    if type(payload) is ProspectivePositionOpenPreparePayloadV2:
        for digest, label in (
            (payload.paper_terminal_payload_sha256, "paper_terminal_payload_sha256"),
            (payload.paper_terminal_jsonl_sha256, "paper_terminal_jsonl_sha256"),
            (payload.paper_terminal_record_sha256, "paper_terminal_record_sha256"),
        ):
            _require_sha256(digest, label)
        _safe_nonnegative_integer(payload.prepared_at_ms, "prepared_at_ms")
        if payload.prepared_at_ms < identity.decision_cutoff_ms:
            raise ProspectivePositionLifecycleContractErrorV2(
                "open prepare predates the decision cutoff"
            )
        full = payload.open_intent is ProspectivePositionOpenIntentV2.FULL_FILL_POSITION
        if full != (identity.position_id is not None):
            raise ProspectivePositionLifecycleContractErrorV2(
                "open intent contradicts position identity"
            )
        if full:
            _require_sha256(
                payload.full_fill_certificate_sha256,
                "full_fill_certificate_sha256",
            )
            _require_sha256(
                payload.full_fill_certificate_jsonl_sha256,
                "full_fill_certificate_jsonl_sha256",
            )
            assert identity.position_id is not None
            assert payload.full_fill_certificate_sha256 is not None
            if identity.position_id != prospective_position_id_v2(
                outcome_id=identity.outcome_id,
                certificate_sha256=payload.full_fill_certificate_sha256,
            ):
                raise ProspectivePositionLifecycleContractErrorV2(
                    "position ID differs from outcome and full-fill certificate"
                )
        elif (
            payload.full_fill_certificate_sha256 is not None
            or payload.full_fill_certificate_jsonl_sha256 is not None
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "no-position prepare forbids full-fill certificate hashes"
            )
        return
    if type(payload) is ProspectivePositionOpenDispositionPayloadV2:
        _require_sha256(payload.prepare_event_id, "prepare_event_id")
        _require_sha256(payload.prepare_payload_sha256, "prepare_payload_sha256")
        _safe_nonnegative_integer(payload.dispositioned_at_ms, "dispositioned_at_ms")
        admitted = payload.disposition is ProspectivePositionOpenDispositionV2.ADMITTED_FULL_FILL
        if admitted:
            if payload.admission_evidence is None or identity.position_id is None:
                raise ProspectivePositionLifecycleContractErrorV2(
                    "admitted disposition requires position and admission evidence"
                )
            _verify_admission_evidence(payload.admission_evidence)
            if payload.admission_evidence.family is not identity.family:
                raise ProspectivePositionLifecycleContractErrorV2(
                    "admission evidence family differs from position identity"
                )
        elif payload.admission_evidence is not None or identity.position_id is not None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "suppressed disposition forbids a position or admission evidence"
            )
        return
    if type(payload) is ProspectiveFamilyExitPreparePayloadV2:
        if identity.position_id is None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "family exit prepare requires a position identity"
            )
        for digest, label in (
            (payload.open_disposition_event_id, "open_disposition_event_id"),
            (payload.admission_evidence_sha256, "admission_evidence_sha256"),
            (payload.entry_event_id, "entry_event_id"),
            (payload.exit_event_id, "exit_event_id"),
            (payload.exit_decision_payload_sha256, "exit_decision_payload_sha256"),
            (payload.exit_input_sha256, "exit_input_sha256"),
        ):
            _require_sha256(digest, label)
        _safe_nonnegative_integer(payload.exit_decision_cutoff_ms, "exit_decision_cutoff_ms")
        _safe_nonnegative_integer(payload.prepared_at_ms, "prepared_at_ms")
        if payload.prepared_at_ms < payload.exit_decision_cutoff_ms:
            raise ProspectivePositionLifecycleContractErrorV2(
                "exit prepare predates the exit decision cutoff"
            )
        if type(payload.exits_position) is not bool:
            raise ProspectivePositionLifecycleContractErrorV2("exits_position must be boolean")
        if payload.exit_decision_authority is not (
            ProspectiveLifecycleEvidenceAuthorityV2.EXPLICIT_NONAUTHORITATIVE_HASH_REFERENCE
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "exit decision preview must remain explicitly nonauthoritative"
            )
        return
    if type(payload) is ProspectiveFamilyExitDispositionPayloadV2:
        if identity.position_id is None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "family exit disposition requires a position identity"
            )
        _require_sha256(payload.exit_prepare_event_id, "exit_prepare_event_id")
        _require_sha256(payload.exit_prepare_payload_sha256, "exit_prepare_payload_sha256")
        _verify_exit_evidence(payload.exit_evidence)
        if payload.exit_evidence.family is not identity.family:
            raise ProspectivePositionLifecycleContractErrorV2(
                "exit evidence family differs from position identity"
            )
        _safe_nonnegative_integer(payload.dispositioned_at_ms, "dispositioned_at_ms")
        return
    if type(payload) is ProspectivePositionCashflowPayloadV2:
        if identity.position_id is None or identity.position_side is None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "position cashflow requires a position identity"
            )
        for digest, label in (
            (payload.terminal_exit_event_id, "terminal_exit_event_id"),
            (payload.terminal_exit_evidence_sha256, "terminal_exit_evidence_sha256"),
            (payload.evidence_reference_sha256, "evidence_reference_sha256"),
        ):
            _require_sha256(digest, label)
        _finite_decimal(payload.signed_amount_usdt, "signed_amount_usdt")
        if not isinstance(payload.evidence_authority, ProspectiveLifecycleEvidenceAuthorityV2):
            raise ProspectivePositionLifecycleContractErrorV2(
                "cashflow evidence authority is invalid"
            )
        _safe_nonnegative_integer(payload.observed_at_ms, "observed_at_ms")
        return
    raise ProspectivePositionLifecycleContractErrorV2("unknown lifecycle payload type")


def _payload_document(
    payload: _PayloadV2,
    *,
    include_event_id: bool,
    include_payload_sha256: bool,
) -> dict[str, object]:
    document: dict[str, object] = {
        "authority_status": payload.authority_status,
        "efficacy_eligible": payload.efficacy_eligible,
        "identity": _serialized_identity(payload.identity),
        "invalidation": payload.invalidation,
        "production_order_placement": payload.production_order_placement,
        "reasons": list(payload.reasons),
        "rule_version": payload.rule_version,
        "schema_version": payload.schema_version,
        "typed_payload_semantics_authoritative": (payload.typed_payload_semantics_authoritative),
    }
    if type(payload) is ProspectivePositionOpenPreparePayloadV2:
        document.update(
            {
                "full_fill_certificate_jsonl_sha256": (payload.full_fill_certificate_jsonl_sha256),
                "full_fill_certificate_sha256": payload.full_fill_certificate_sha256,
                "open_intent": payload.open_intent.value,
                "paper_terminal_jsonl_sha256": payload.paper_terminal_jsonl_sha256,
                "paper_terminal_payload_sha256": payload.paper_terminal_payload_sha256,
                "paper_terminal_record_sha256": payload.paper_terminal_record_sha256,
                "prepared_at_ms": payload.prepared_at_ms,
            }
        )
    elif type(payload) is ProspectivePositionOpenDispositionPayloadV2:
        document.update(
            {
                "admission_evidence": (
                    None
                    if payload.admission_evidence is None
                    else _serialized_admission_evidence(payload.admission_evidence)
                ),
                "disposition": payload.disposition.value,
                "dispositioned_at_ms": payload.dispositioned_at_ms,
                "prepare_event_id": payload.prepare_event_id,
                "prepare_payload_sha256": payload.prepare_payload_sha256,
            }
        )
    elif type(payload) is ProspectiveFamilyExitPreparePayloadV2:
        document.update(
            {
                "admission_evidence_sha256": payload.admission_evidence_sha256,
                "entry_event_id": payload.entry_event_id,
                "exit_decision_cutoff_ms": payload.exit_decision_cutoff_ms,
                "exit_decision_authority": payload.exit_decision_authority.value,
                "exit_decision_payload_sha256": payload.exit_decision_payload_sha256,
                "exit_event_id": payload.exit_event_id,
                "exit_input_sha256": payload.exit_input_sha256,
                "exits_position": payload.exits_position,
                "open_disposition_event_id": payload.open_disposition_event_id,
                "prepared_at_ms": payload.prepared_at_ms,
            }
        )
    elif type(payload) is ProspectiveFamilyExitDispositionPayloadV2:
        document.update(
            {
                "dispositioned_at_ms": payload.dispositioned_at_ms,
                "exit_evidence": _serialized_exit_evidence(payload.exit_evidence),
                "exit_prepare_event_id": payload.exit_prepare_event_id,
                "exit_prepare_payload_sha256": payload.exit_prepare_payload_sha256,
            }
        )
    elif type(payload) is ProspectivePositionCashflowPayloadV2:
        document.update(
            {
                "cashflow_class": payload.cashflow_class.value,
                "evidence_authority": payload.evidence_authority.value,
                "evidence_reference_sha256": payload.evidence_reference_sha256,
                "observed_at_ms": payload.observed_at_ms,
                "signed_amount_usdt": _decimal_text(payload.signed_amount_usdt),
                "terminal_exit_event_id": payload.terminal_exit_event_id,
                "terminal_exit_evidence_sha256": (payload.terminal_exit_evidence_sha256),
            }
        )
    else:
        raise ProspectivePositionLifecycleContractErrorV2("unknown lifecycle payload type")
    if include_event_id:
        document["event_id"] = payload.event_id
    if include_payload_sha256:
        document["payload_sha256"] = payload.payload_sha256
    return document


def _event_domain(payload: _PayloadV2) -> bytes:
    if type(payload) is ProspectivePositionOpenPreparePayloadV2:
        return _OPEN_PREPARE_EVENT_DOMAIN
    if type(payload) is ProspectivePositionOpenDispositionPayloadV2:
        return _OPEN_DISPOSITION_EVENT_DOMAIN
    if type(payload) is ProspectiveFamilyExitPreparePayloadV2:
        return _EXIT_PREPARE_EVENT_DOMAIN
    if type(payload) is ProspectiveFamilyExitDispositionPayloadV2:
        return _EXIT_DISPOSITION_EVENT_DOMAIN
    if type(payload) is ProspectivePositionCashflowPayloadV2:
        return _CASHFLOW_EVENT_DOMAIN
    raise TypeError("unknown lifecycle payload type")


def _payload_domain(payload: _PayloadV2) -> bytes:
    if type(payload) is ProspectivePositionOpenPreparePayloadV2:
        return _OPEN_PREPARE_PAYLOAD_DOMAIN
    if type(payload) is ProspectivePositionOpenDispositionPayloadV2:
        return _OPEN_DISPOSITION_PAYLOAD_DOMAIN
    if type(payload) is ProspectiveFamilyExitPreparePayloadV2:
        return _EXIT_PREPARE_PAYLOAD_DOMAIN
    if type(payload) is ProspectiveFamilyExitDispositionPayloadV2:
        return _EXIT_DISPOSITION_PAYLOAD_DOMAIN
    if type(payload) is ProspectivePositionCashflowPayloadV2:
        return _CASHFLOW_PAYLOAD_DOMAIN
    raise TypeError("unknown lifecycle payload type")


def _record_kind(
    payload: _PayloadV2 | ProspectivePositionTerminalPayloadV2,
) -> ProspectiveOutcomeWalRecordKindV2:
    if type(payload) is ProspectivePositionOpenPreparePayloadV2:
        return ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE
    if type(payload) is ProspectivePositionOpenDispositionPayloadV2:
        return ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION
    if type(payload) is ProspectiveFamilyExitPreparePayloadV2:
        return ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_PREPARE
    if type(payload) is ProspectiveFamilyExitDispositionPayloadV2:
        return ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_DISPOSITION
    if type(payload) is ProspectivePositionCashflowPayloadV2:
        return ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW
    if type(payload) is ProspectivePositionTerminalPayloadV2:
        return ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL
    raise TypeError("unknown lifecycle payload type")


def _identity_document(
    value: ProspectivePositionLifecycleIdentityV2,
) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "attempt_plan_sha256": value.attempt_plan_sha256,
        "bar_close_ms": value.bar_close_ms,
        "bar_open_ms": value.bar_open_ms,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "execution_contract_sha256": value.execution_contract_sha256,
        "family": value.family.value,
        "family_rule_version": value.family_rule_version,
        "origin_cell_id": value.origin_cell_id,
        "origin_segment_id": value.origin_segment_id,
        "outcome_id": value.outcome_id,
        "position_id": value.position_id,
        "position_side": (None if value.position_side is None else value.position_side.value),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "sizing_cell": value.sizing_cell.value,
        "symbol": value.symbol,
        "venue": value.venue.value,
    }


def _serialized_identity(
    value: ProspectivePositionLifecycleIdentityV2,
) -> dict[str, object]:
    _verify_identity(value)
    return {**_identity_document(value), "identity_sha256": value.identity_sha256}


def _build_identity(
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
    sizing_cell: PaperSizingCellV2,
    position_id: str | None,
    position_side: MandatoryExitPositionSideV2 | None,
) -> ProspectivePositionLifecycleIdentityV2:
    outcome_id = prospective_outcome_id_v2(
        attempt_plan_sha256=plan.plan_sha256,
        origin_segment_id=cell.segment_id,
        origin_cell_id=cell.cell_id,
        sizing_cell=sizing_cell,
    )
    return ProspectivePositionLifecycleIdentityV2(
        attempt_id=cell.attempt_id,
        attempt_plan_sha256=plan.plan_sha256,
        promoting_plan_sha256=plan.promoting_plan_sha256,
        execution_contract_sha256=plan.execution_contract_sha256,
        origin_segment_id=cell.segment_id,
        origin_cell_id=cell.cell_id,
        sizing_cell=sizing_cell,
        family=cell.family,
        family_rule_version=cell.rule_version,
        symbol=cell.symbol,
        venue=VenueV2.USDM_FUTURES,
        bar_open_ms=cell.bar_open_ms,
        bar_close_ms=cell.bar_close_ms,
        decision_cutoff_ms=cell.decision_cutoff_ms,
        outcome_id=outcome_id,
        position_id=position_id,
        position_side=position_side,
        _factory_token=_FACTORY_TOKEN,
    )


def _validate_identity(value: ProspectivePositionLifecycleIdentityV2) -> None:
    _identity_text(value.attempt_id, "attempt_id")
    _symbol(value.symbol)
    for digest, label in (
        (value.attempt_plan_sha256, "attempt_plan_sha256"),
        (value.promoting_plan_sha256, "promoting_plan_sha256"),
        (value.execution_contract_sha256, "execution_contract_sha256"),
        (value.origin_segment_id, "origin_segment_id"),
        (value.origin_cell_id, "origin_cell_id"),
        (value.outcome_id, "outcome_id"),
    ):
        _require_sha256(digest, label)
    if value.position_id is not None:
        _require_sha256(value.position_id, "position_id")
    if not isinstance(value.sizing_cell, PaperSizingCellV2):
        raise ProspectivePositionLifecycleContractErrorV2("identity sizing_cell is invalid")
    if not isinstance(value.family, PromotingFamilyV2):
        raise ProspectivePositionLifecycleContractErrorV2("identity family is invalid")
    _identity_text(value.family_rule_version, "family_rule_version")
    if value.venue is not VenueV2.USDM_FUTURES:
        raise ProspectivePositionLifecycleContractErrorV2(
            "position lifecycle venue must be USD-M Futures"
        )
    for integer, label in (
        (value.bar_open_ms, "bar_open_ms"),
        (value.bar_close_ms, "bar_close_ms"),
        (value.decision_cutoff_ms, "decision_cutoff_ms"),
    ):
        _safe_nonnegative_integer(integer, label)
    if not value.bar_open_ms < value.bar_close_ms <= value.decision_cutoff_ms:
        raise ProspectivePositionLifecycleContractErrorV2(
            "identity candle and decision times are not causal"
        )
    if (value.position_id is None) != (value.position_side is None):
        raise ProspectivePositionLifecycleContractErrorV2(
            "position ID and side must both be present or absent"
        )
    if value.position_side is not None and not isinstance(
        value.position_side, MandatoryExitPositionSideV2
    ):
        raise ProspectivePositionLifecycleContractErrorV2("position side is invalid")
    expected_outcome = prospective_outcome_id_v2(
        attempt_plan_sha256=value.attempt_plan_sha256,
        origin_segment_id=value.origin_segment_id,
        origin_cell_id=value.origin_cell_id,
        sizing_cell=value.sizing_cell,
    )
    if value.outcome_id != expected_outcome:
        raise ProspectivePositionLifecycleContractErrorV2(
            "outcome ID differs from its exact plan/cell/sizing identity"
        )


def _verify_identity(value: ProspectivePositionLifecycleIdentityV2) -> None:
    if type(value) is not ProspectivePositionLifecycleIdentityV2:
        raise ProspectivePositionLifecycleContractErrorV2(
            "payload identity must be exact ProspectivePositionLifecycleIdentityV2"
        )
    _validate_identity(value)
    expected = _hash_document(_IDENTITY_DOMAIN, _identity_document(value))
    if value.identity_sha256 != expected:
        raise ProspectivePositionLifecycleContractErrorV2(
            "lifecycle identity hash differs from canonical content"
        )


def _admission_document(
    value: ProspectiveFamilyAdmissionEvidenceV2,
) -> dict[str, object]:
    return {
        "admission_disposition": value.admission_disposition,
        "admission_input_sha256": value.admission_input_sha256,
        "admission_post_event_count": value.admission_post_event_count,
        "admission_post_root_sha256": value.admission_post_root_sha256,
        "admission_pre_event_count": value.admission_pre_event_count,
        "admission_pre_root_sha256": value.admission_pre_root_sha256,
        "entry_event_id": value.entry_event_id,
        "family": value.family.value,
        "full_fill_certificate_sha256": value.full_fill_certificate_sha256,
        "source_authority": value.source_authority.value,
    }


def _serialized_admission_evidence(
    value: ProspectiveFamilyAdmissionEvidenceV2,
) -> dict[str, object]:
    _verify_admission_evidence(value)
    return {**_admission_document(value), "evidence_sha256": value.evidence_sha256}


def _validate_receipt_disposition_for_append(
    *,
    disposition: str,
    pre_root_sha256: str,
    pre_event_count: int,
    post_root_sha256: str,
    post_event_count: int,
    new_event_increment: int,
    allow_preexisting_recovery: bool,
    label: str,
) -> None:
    """Permit PREEXISTING only while reconciling a recovered pending WAL intent."""

    _validate_projected_receipt_transition(
        disposition=disposition,
        pre_root_sha256=pre_root_sha256,
        pre_event_count=pre_event_count,
        post_root_sha256=post_root_sha256,
        post_event_count=post_event_count,
        new_event_increment=new_event_increment,
        label=label,
    )
    if disposition == "PREEXISTING" and not allow_preexisting_recovery:
        raise ProspectivePositionLifecycleContractErrorV2(
            f"{label} PREEXISTING receipt is allowed only for recovered pending reconciliation"
        )


def _validate_projected_receipt_transition(
    *,
    disposition: str,
    pre_root_sha256: str,
    pre_event_count: int,
    post_root_sha256: str,
    post_event_count: int,
    new_event_increment: int,
    label: str,
) -> None:
    _identity_text(disposition, f"{label} disposition")
    _require_sha256(pre_root_sha256, f"{label} pre_root_sha256")
    _require_sha256(post_root_sha256, f"{label} post_root_sha256")
    _safe_nonnegative_integer(pre_event_count, f"{label} pre_event_count")
    _safe_nonnegative_integer(post_event_count, f"{label} post_event_count")
    if disposition == "NEW_BY_THIS_TRANSACTION":
        if (
            post_event_count != pre_event_count + new_event_increment
            or post_root_sha256 == pre_root_sha256
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                f"{label} NEW receipt has an invalid rooted transition"
            )
        return
    if disposition == "PREEXISTING":
        if post_event_count != pre_event_count or post_root_sha256 != pre_root_sha256:
            raise ProspectivePositionLifecycleContractErrorV2(
                f"{label} PREEXISTING receipt must preserve the exact current state"
            )
        return
    raise ProspectivePositionLifecycleContractErrorV2(f"{label} receipt disposition is unsupported")


def _family_evidence_authority(
    family: PromotingFamilyV2,
) -> ProspectiveLifecycleEvidenceAuthorityV2:
    if family is PromotingFamilyV2.A:
        return ProspectiveLifecycleEvidenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_A
    if family is PromotingFamilyV2.B:
        return ProspectiveLifecycleEvidenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_B
    if family is PromotingFamilyV2.C:
        return ProspectiveLifecycleEvidenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_C
    raise ProspectivePositionLifecycleContractErrorV2(
        "family evidence requires a supported promoting family"
    )


def _validate_admission_evidence(value: ProspectiveFamilyAdmissionEvidenceV2) -> None:
    if not isinstance(value.family, PromotingFamilyV2):
        raise ProspectivePositionLifecycleContractErrorV2("admission evidence family is invalid")
    for digest, label in (
        (value.entry_event_id, "entry_event_id"),
        (value.full_fill_certificate_sha256, "full_fill_certificate_sha256"),
        (value.admission_input_sha256, "admission_input_sha256"),
        (value.admission_pre_root_sha256, "admission_pre_root_sha256"),
        (value.admission_post_root_sha256, "admission_post_root_sha256"),
    ):
        _require_sha256(digest, label)
    _safe_nonnegative_integer(value.admission_pre_event_count, "admission_pre_event_count")
    _safe_nonnegative_integer(value.admission_post_event_count, "admission_post_event_count")
    _validate_projected_receipt_transition(
        disposition=value.admission_disposition,
        pre_root_sha256=value.admission_pre_root_sha256,
        pre_event_count=value.admission_pre_event_count,
        post_root_sha256=value.admission_post_root_sha256,
        post_event_count=value.admission_post_event_count,
        new_event_increment=(1 if value.family is PromotingFamilyV2.A else 0),
        label="admission evidence",
    )
    if not isinstance(value.source_authority, ProspectiveLifecycleEvidenceAuthorityV2):
        raise ProspectivePositionLifecycleContractErrorV2("admission evidence authority is invalid")
    if value.source_authority is not _family_evidence_authority(value.family):
        raise ProspectivePositionLifecycleContractErrorV2(
            "admission evidence authority differs from its family"
        )


def _verify_admission_evidence(value: ProspectiveFamilyAdmissionEvidenceV2) -> None:
    if type(value) is not ProspectiveFamilyAdmissionEvidenceV2:
        raise ProspectivePositionLifecycleContractErrorV2(
            "admission evidence must be exact ProspectiveFamilyAdmissionEvidenceV2"
        )
    _validate_admission_evidence(value)
    expected = _hash_document(_ADMISSION_EVIDENCE_DOMAIN, _admission_document(value))
    if value.evidence_sha256 != expected:
        raise ProspectivePositionLifecycleContractErrorV2(
            "admission evidence hash differs from canonical content"
        )


def _exit_evidence_document(
    value: ProspectiveFamilyExitEvidenceV2,
) -> dict[str, object]:
    return {
        "entry_event_id": value.entry_event_id,
        "exit_decision_cutoff_ms": value.exit_decision_cutoff_ms,
        "exit_decision_payload_sha256": value.exit_decision_payload_sha256,
        "exit_disposition": value.exit_disposition,
        "exit_event_id": value.exit_event_id,
        "exit_input_sha256": value.exit_input_sha256,
        "exit_post_event_count": value.exit_post_event_count,
        "exit_post_root_sha256": value.exit_post_root_sha256,
        "exit_pre_event_count": value.exit_pre_event_count,
        "exit_pre_root_sha256": value.exit_pre_root_sha256,
        "family": value.family.value,
        "source_authority": value.source_authority.value,
        "terminal_exit": value.terminal_exit,
    }


def _serialized_exit_evidence(
    value: ProspectiveFamilyExitEvidenceV2,
) -> dict[str, object]:
    _verify_exit_evidence(value)
    return {**_exit_evidence_document(value), "evidence_sha256": value.evidence_sha256}


def _validate_exit_evidence(value: ProspectiveFamilyExitEvidenceV2) -> None:
    if not isinstance(value.family, PromotingFamilyV2):
        raise ProspectivePositionLifecycleContractErrorV2("exit evidence family is invalid")
    for digest, label in (
        (value.entry_event_id, "entry_event_id"),
        (value.exit_event_id, "exit_event_id"),
        (value.exit_decision_payload_sha256, "exit_decision_payload_sha256"),
        (value.exit_input_sha256, "exit_input_sha256"),
        (value.exit_pre_root_sha256, "exit_pre_root_sha256"),
        (value.exit_post_root_sha256, "exit_post_root_sha256"),
    ):
        _require_sha256(digest, label)
    for integer, label in (
        (value.exit_pre_event_count, "exit_pre_event_count"),
        (value.exit_post_event_count, "exit_post_event_count"),
        (value.exit_decision_cutoff_ms, "exit_decision_cutoff_ms"),
    ):
        _safe_nonnegative_integer(integer, label)
    _validate_projected_receipt_transition(
        disposition=value.exit_disposition,
        pre_root_sha256=value.exit_pre_root_sha256,
        pre_event_count=value.exit_pre_event_count,
        post_root_sha256=value.exit_post_root_sha256,
        post_event_count=value.exit_post_event_count,
        new_event_increment=1,
        label="exit evidence",
    )
    if type(value.terminal_exit) is not bool:
        raise ProspectivePositionLifecycleContractErrorV2("terminal_exit must be boolean")
    if not isinstance(value.source_authority, ProspectiveLifecycleEvidenceAuthorityV2):
        raise ProspectivePositionLifecycleContractErrorV2("exit evidence authority is invalid")
    if value.source_authority is not _family_evidence_authority(value.family):
        raise ProspectivePositionLifecycleContractErrorV2(
            "exit evidence authority differs from its family"
        )


def _verify_exit_evidence(value: ProspectiveFamilyExitEvidenceV2) -> None:
    if type(value) is not ProspectiveFamilyExitEvidenceV2:
        raise ProspectivePositionLifecycleContractErrorV2(
            "exit evidence must be exact ProspectiveFamilyExitEvidenceV2"
        )
    _validate_exit_evidence(value)
    expected = _hash_document(_EXIT_EVIDENCE_DOMAIN, _exit_evidence_document(value))
    if value.evidence_sha256 != expected:
        raise ProspectivePositionLifecycleContractErrorV2(
            "exit evidence hash differs from canonical content"
        )


def _decode_exact_payload(encoded: bytes, schema_version: str) -> dict[str, object]:
    if type(encoded) is not bytes or not encoded:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored lifecycle payload must be non-empty immutable bytes"
        )
    if (
        len(encoded) > MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2
        or not encoded.endswith(b"\n")
        or b"\n" in encoded[:-1]
    ):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored lifecycle payload violates the canonical JSONL bound"
        )
    try:
        decoded: object = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored lifecycle payload is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(decoded, dict):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored lifecycle payload must be a JSON object"
        )
    document = cast(dict[str, object], decoded)
    if document.get("schema_version") != schema_version:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored lifecycle payload schema differs from its record kind"
        )
    try:
        canonical = canonical_json_line(document)
    except (TypeError, ValueError) as exc:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored lifecycle payload contains unsupported canonical JSON"
        ) from exc
    if canonical != encoded:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored lifecycle payload is not exact canonical JSONL"
        )
    return document


def _parse_identity(document: dict[str, object]) -> ProspectivePositionLifecycleIdentityV2:
    side_text = _optional_text(document, "position_side")
    identity = ProspectivePositionLifecycleIdentityV2(
        attempt_id=_text(document, "attempt_id"),
        attempt_plan_sha256=_text(document, "attempt_plan_sha256"),
        promoting_plan_sha256=_text(document, "promoting_plan_sha256"),
        execution_contract_sha256=_text(document, "execution_contract_sha256"),
        origin_segment_id=_text(document, "origin_segment_id"),
        origin_cell_id=_text(document, "origin_cell_id"),
        sizing_cell=PaperSizingCellV2(_text(document, "sizing_cell")),
        family=PromotingFamilyV2(_text(document, "family")),
        family_rule_version=_text(document, "family_rule_version"),
        symbol=_text(document, "symbol"),
        venue=VenueV2(_text(document, "venue")),
        bar_open_ms=_integer(document, "bar_open_ms"),
        bar_close_ms=_integer(document, "bar_close_ms"),
        decision_cutoff_ms=_integer(document, "decision_cutoff_ms"),
        outcome_id=_text(document, "outcome_id"),
        position_id=_optional_text(document, "position_id"),
        position_side=(None if side_text is None else MandatoryExitPositionSideV2(side_text)),
        _factory_token=_FACTORY_TOKEN,
    )
    if _serialized_identity(identity) != document:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored lifecycle identity has missing, unknown, or inconsistent fields"
        )
    return identity


def _parse_admission_evidence(
    document: dict[str, object],
) -> ProspectiveFamilyAdmissionEvidenceV2:
    value = ProspectiveFamilyAdmissionEvidenceV2(
        family=PromotingFamilyV2(_text(document, "family")),
        entry_event_id=_text(document, "entry_event_id"),
        full_fill_certificate_sha256=_text(document, "full_fill_certificate_sha256"),
        admission_input_sha256=_text(document, "admission_input_sha256"),
        admission_pre_root_sha256=_text(document, "admission_pre_root_sha256"),
        admission_pre_event_count=_integer(document, "admission_pre_event_count"),
        admission_post_root_sha256=_text(document, "admission_post_root_sha256"),
        admission_post_event_count=_integer(document, "admission_post_event_count"),
        admission_disposition=_text(document, "admission_disposition"),
        source_authority=ProspectiveLifecycleEvidenceAuthorityV2(
            _text(document, "source_authority")
        ),
        _factory_token=_FACTORY_TOKEN,
    )
    if _serialized_admission_evidence(value) != document:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored admission evidence has missing, unknown, or inconsistent fields"
        )
    return value


def _parse_exit_evidence(document: dict[str, object]) -> ProspectiveFamilyExitEvidenceV2:
    value = ProspectiveFamilyExitEvidenceV2(
        family=PromotingFamilyV2(_text(document, "family")),
        entry_event_id=_text(document, "entry_event_id"),
        exit_event_id=_text(document, "exit_event_id"),
        exit_decision_payload_sha256=_text(document, "exit_decision_payload_sha256"),
        exit_input_sha256=_text(document, "exit_input_sha256"),
        exit_pre_root_sha256=_text(document, "exit_pre_root_sha256"),
        exit_pre_event_count=_integer(document, "exit_pre_event_count"),
        exit_post_root_sha256=_text(document, "exit_post_root_sha256"),
        exit_post_event_count=_integer(document, "exit_post_event_count"),
        exit_disposition=_text(document, "exit_disposition"),
        exit_decision_cutoff_ms=_integer(document, "exit_decision_cutoff_ms"),
        terminal_exit=_boolean(document, "terminal_exit"),
        source_authority=ProspectiveLifecycleEvidenceAuthorityV2(
            _text(document, "source_authority")
        ),
        _factory_token=_FACTORY_TOKEN,
    )
    if _serialized_exit_evidence(value) != document:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored exit evidence has missing, unknown, or inconsistent fields"
        )
    return value


def _require_stored_hashes(payload: _PayloadV2, document: dict[str, object]) -> None:
    if (
        _text(document, "event_id") != payload.event_id
        or _text(document, "payload_sha256") != payload.payload_sha256
    ):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "stored lifecycle event or payload self-hash differs"
        )


def _exact_plan_cell(
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
) -> ProspectiveExpectedCellV2:
    canonical_prospective_census_plan_v2(plan)
    if plan.execution_contract_sha256 != current_prospective_execution_contract_sha256_v2():
        raise ProspectivePositionLifecycleContractErrorV2(
            "plan does not bind the current prospective execution contract"
        )
    if type(cell) is not ProspectiveExpectedCellV2:
        raise ProspectivePositionLifecycleContractErrorV2(
            "cell must be exact ProspectiveExpectedCellV2"
        )
    try:
        expected = plan.expected_cell(
            family=cell.family,
            symbol=cell.symbol,
            bar_open_ms=cell.bar_open_ms,
        )
    except ValueError as exc:
        raise ProspectivePositionLifecycleContractErrorV2(
            "cell is outside the frozen prospective census"
        ) from exc
    if cell != expected:
        raise ProspectivePositionLifecycleContractErrorV2(
            "cell differs from its exact frozen census identity"
        )
    return expected


def _cell_for_identity(
    plan: ProspectiveCensusPlanV2,
    identity: ProspectivePositionLifecycleIdentityV2,
) -> ProspectiveExpectedCellV2:
    cell = plan.expected_cell(
        family=identity.family,
        symbol=identity.symbol,
        bar_open_ms=identity.bar_open_ms,
    )
    if (
        cell.attempt_id != identity.attempt_id
        or cell.attempt_plan_sha256 != identity.attempt_plan_sha256
        or cell.segment_id != identity.origin_segment_id
        or cell.cell_id != identity.origin_cell_id
        or cell.rule_version != identity.family_rule_version
        or cell.bar_close_ms != identity.bar_close_ms
        or cell.decision_cutoff_ms != identity.decision_cutoff_ms
    ):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "typed identity differs from its exact current plan cell"
        )
    return cell


def _validate_paper_terminal_binding(
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
    sizing_cell: PaperSizingCellV2,
    paper_terminal: ProspectivePaperTerminalPayloadV2,
) -> None:
    if type(paper_terminal) is not ProspectivePaperTerminalPayloadV2:
        raise ProspectivePositionLifecycleContractErrorV2(
            "paper_terminal must be exact ProspectivePaperTerminalPayloadV2"
        )
    expected = (
        cell.attempt_id,
        plan.plan_sha256,
        plan.promoting_plan_sha256,
        plan.execution_contract_sha256,
        cell.segment_id,
        cell.cell_id,
        cell.family,
        cell.rule_version,
        cell.symbol,
        VenueV2.USDM_FUTURES,
        cell.bar_open_ms,
        cell.bar_close_ms,
        cell.decision_cutoff_ms,
        sizing_cell,
    )
    observed = (
        paper_terminal.attempt_id,
        paper_terminal.attempt_plan_sha256,
        paper_terminal.promoting_plan_sha256,
        paper_terminal.execution_contract_sha256,
        paper_terminal.segment_id,
        paper_terminal.cell_id,
        paper_terminal.family,
        paper_terminal.family_rule_version,
        paper_terminal.symbol,
        paper_terminal.venue,
        paper_terminal.bar_open_ms,
        paper_terminal.bar_close_ms,
        paper_terminal.decision_cutoff_ms,
        paper_terminal.sizing_cell,
    )
    if observed != expected:
        raise ProspectivePositionLifecycleContractErrorV2(
            "PAPER terminal differs from the exact plan/cell/sizing identity"
        )


def _validate_full_fill_binding(
    paper_terminal: ProspectivePaperTerminalPayloadV2,
    certificate: PaperFokFullFillCertificateV2,
) -> None:
    embedded = paper_terminal.canonical_full_fill_certificate_jsonl
    canonical = canonical_paper_fok_full_fill_certificate_v2(certificate)
    if (
        paper_terminal.completeness is not ProspectivePaperTerminalCompletenessV2.COMPLETE
        or paper_terminal.full_fill_certificate_sha256 != certificate.certificate_sha256
        or embedded != canonical
        or paper_terminal.attempt_id != certificate.attempt_id
        or paper_terminal.promoting_plan_sha256 != certificate.promoting_plan_sha256
        or paper_terminal.symbol != certificate.symbol
        or paper_terminal.venue is not certificate.venue
        or paper_terminal.decision_cutoff_ms != certificate.decision_cutoff_ms
        or paper_terminal.requested_quantity != certificate.requested_quantity
        or paper_terminal.certified_quantity != certificate.requested_quantity
        or paper_terminal.filled_quantity != certificate.filled_quantity
        or paper_terminal.executable_vwap != certificate.executable_vwap
        or paper_terminal.executable_notional != certificate.executable_notional
        or paper_terminal.paper_decision_event_id != certificate.decision_event_id
        or paper_terminal.paper_decision_payload_sha256 != certificate.decision_payload_sha256
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "PAPER terminal and full-fill certificate differ"
        )


def _position_side_from_paper(side: PaperFokSideV2) -> MandatoryExitPositionSideV2:
    if side is PaperFokSideV2.BUY:
        return MandatoryExitPositionSideV2.LONG
    if side is PaperFokSideV2.SELL:
        return MandatoryExitPositionSideV2.SHORT
    raise ProspectivePositionLifecycleContractErrorV2("PAPER side is unsupported")


def _exit_decision_projection(
    decision: FamilyAExitDecisionV2 | FamilyBExitDecisionV2 | FamilyCExitDecisionV2,
) -> tuple[
    PromotingFamilyV2,
    str,
]:
    if type(decision) is FamilyAExitDecisionV2:
        canonical_family_a_exit_decision_v2(decision)
        return PromotingFamilyV2.A, decision.entry_event_id
    if type(decision) is FamilyBExitDecisionV2:
        canonical_family_b_exit_decision_v2(decision)
        return PromotingFamilyV2.B, decision.entry_event_id
    if type(decision) is FamilyCExitDecisionV2:
        canonical_family_c_exit_decision_v2(decision)
        return PromotingFamilyV2.C, decision.entry_event_id
    raise ProspectivePositionLifecycleContractErrorV2(
        "exit decision must be an exact Family A, B, or C decision"
    )


def _cashflow_from_evidence(
    *,
    identity: ProspectivePositionLifecycleIdentityV2,
    cashflow_class: ProspectivePositionCashflowClassV2,
    evidence: (
        PaperFokFullFillCertificateV2
        | MandatoryExitEvidenceReferenceV2
        | FinalFeeEvidenceReferenceV2
        | FundingCensusEvidenceReferenceV2
    ),
) -> tuple[Decimal, str, ProspectiveLifecycleEvidenceAuthorityV2]:
    assert identity.position_id is not None
    assert identity.position_side is not None
    if cashflow_class is ProspectivePositionCashflowClassV2.ENTRY_EXECUTION:
        if type(evidence) is not PaperFokFullFillCertificateV2:
            raise ProspectivePositionLifecycleContractErrorV2(
                "entry cashflow requires an exact full-fill certificate"
            )
        canonical_paper_fok_full_fill_certificate_v2(evidence)
        if (
            evidence.attempt_id != identity.attempt_id
            or evidence.promoting_plan_sha256 != identity.promoting_plan_sha256
            or evidence.symbol != identity.symbol
            or evidence.venue is not identity.venue
            or _position_side_from_paper(evidence.side) is not identity.position_side
            or prospective_position_id_v2(
                outcome_id=identity.outcome_id,
                certificate_sha256=evidence.certificate_sha256,
            )
            != identity.position_id
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "entry cashflow certificate differs from position identity"
            )
        amount = (
            -evidence.executable_notional
            if identity.position_side is MandatoryExitPositionSideV2.LONG
            else evidence.executable_notional
        )
        return (
            amount,
            evidence.certificate_sha256,
            ProspectiveLifecycleEvidenceAuthorityV2.LIVE_FACTORY_SEALED_PAPER_FULL_FILL,
        )
    if cashflow_class is ProspectivePositionCashflowClassV2.EXIT_EXECUTION:
        if type(evidence) is not MandatoryExitEvidenceReferenceV2:
            raise ProspectivePositionLifecycleContractErrorV2(
                "exit cashflow requires exact mandatory-exit evidence"
            )
        _verify_mandatory_exit_reference(evidence)
        _validate_reference_identity(identity, evidence)
        if evidence.side is not identity.position_side:
            raise ProspectivePositionLifecycleContractErrorV2(
                "mandatory-exit side differs from position identity"
            )
        return (
            evidence.signed_exit_cashflow_usdt,
            evidence.reference_sha256,
            ProspectiveLifecycleEvidenceAuthorityV2.EXPLICIT_NONAUTHORITATIVE_HASH_REFERENCE,
        )
    if cashflow_class is ProspectivePositionCashflowClassV2.PUBLIC_FEE:
        if type(evidence) is not FinalFeeEvidenceReferenceV2:
            raise ProspectivePositionLifecycleContractErrorV2(
                "fee cashflow requires exact final public-fee evidence"
            )
        _verify_fee_reference(evidence)
        _validate_reference_identity(identity, evidence)
        return (
            -evidence.total_fee_usdt,
            evidence.reference_sha256,
            ProspectiveLifecycleEvidenceAuthorityV2.EXPLICIT_NONAUTHORITATIVE_HASH_REFERENCE,
        )
    if cashflow_class is ProspectivePositionCashflowClassV2.PUBLIC_FUNDING:
        if type(evidence) is not FundingCensusEvidenceReferenceV2:
            raise ProspectivePositionLifecycleContractErrorV2(
                "funding cashflow requires exact funding-census evidence"
            )
        _verify_funding_reference(evidence)
        _validate_reference_identity(identity, evidence)
        return (
            evidence.realized_funding_cashflow_usdt,
            evidence.reference_sha256,
            ProspectiveLifecycleEvidenceAuthorityV2.EXPLICIT_NONAUTHORITATIVE_HASH_REFERENCE,
        )
    raise ProspectivePositionLifecycleContractErrorV2("unsupported position cashflow class")


def _validate_reference_identity(
    identity: ProspectivePositionLifecycleIdentityV2,
    evidence: (
        MandatoryExitEvidenceReferenceV2
        | FinalFeeEvidenceReferenceV2
        | FundingCensusEvidenceReferenceV2
    ),
) -> None:
    if (
        evidence.attempt_id != identity.attempt_id
        or evidence.promoting_plan_sha256 != identity.promoting_plan_sha256
        or evidence.symbol != identity.symbol
        or evidence.position_id != identity.position_id
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "cashflow evidence differs from the exact position identity"
        )


def _verify_mandatory_exit_reference(value: MandatoryExitEvidenceReferenceV2) -> None:
    document = {
        "attempt_id": value.attempt_id,
        "exit_intent_sha256": value.exit_intent_sha256,
        "exit_slice_count": value.exit_slice_count,
        "exit_slices_root_sha256": value.exit_slices_root_sha256,
        "family_exit_event_id": value.family_exit_event_id,
        "fee_certificate_sha256": value.fee_certificate_sha256,
        "filled_quantity": _decimal_text(value.filled_quantity),
        "gross_exit_notional_usdt": _decimal_text(value.gross_exit_notional_usdt),
        "ledger_checkpoint_sha256": value.ledger_checkpoint_sha256,
        "mandatory_position_sha256": value.mandatory_position_sha256,
        "position_id": value.position_id,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "residual_quantity": _decimal_text(value.residual_quantity),
        "rule_version": value.rule_version,
        "side": value.side.value,
        "signed_exit_cashflow_usdt": _decimal_text(value.signed_exit_cashflow_usdt),
        "source_authority": value.source_authority.value,
        "symbol": value.symbol,
        "target_cursor_sha256": value.target_cursor_sha256,
        "terminal_at_ms": value.terminal_at_ms,
        "terminal_payload_sha256": value.terminal_payload_sha256,
        "terminal_sha256": value.terminal_sha256,
        "terminal_status": value.terminal_status.value,
    }
    if value.reference_sha256 != _hash_document(_MANDATORY_EXIT_REFERENCE_DOMAIN, document):
        raise ProspectivePositionLifecycleContractErrorV2(
            "mandatory-exit evidence reference hash differs"
        )
    if (
        value.terminal_status.value != "EXITED_FULL"
        or value.source_authority.value != "CALLER_HASH_REFERENCE_NOT_MEMBERSHIP_PROOF"
        or type(value.exit_slice_count) is not int
        or value.exit_slice_count < 1
        or type(value.terminal_at_ms) is not int
        or value.terminal_at_ms < 0
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "mandatory-exit evidence is not a complete referenced exit"
        )
    for amount, label in (
        (value.filled_quantity, "filled_quantity"),
        (value.residual_quantity, "residual_quantity"),
        (value.gross_exit_notional_usdt, "gross_exit_notional_usdt"),
    ):
        _finite_decimal(amount, label)
    if value.filled_quantity <= 0 or value.gross_exit_notional_usdt <= 0:
        raise ProspectivePositionLifecycleContractErrorV2(
            "mandatory-exit quantity and notional must be positive"
        )
    for digest in (
        value.exit_intent_sha256,
        value.exit_slices_root_sha256,
        value.family_exit_event_id,
        value.fee_certificate_sha256,
        value.ledger_checkpoint_sha256,
        value.mandatory_position_sha256,
        value.position_id,
        value.promoting_plan_sha256,
        value.target_cursor_sha256,
        value.terminal_payload_sha256,
        value.terminal_sha256,
    ):
        _require_sha256(digest, "mandatory-exit evidence hash")
    _finite_decimal(value.signed_exit_cashflow_usdt, "signed_exit_cashflow_usdt")
    if value.residual_quantity != 0:
        raise ProspectivePositionLifecycleContractErrorV2(
            "mandatory exit must have zero residual quantity"
        )
    expected = (
        value.gross_exit_notional_usdt
        if value.side is MandatoryExitPositionSideV2.LONG
        else -value.gross_exit_notional_usdt
    )
    if value.signed_exit_cashflow_usdt != expected:
        raise ProspectivePositionLifecycleContractErrorV2(
            "mandatory-exit signed cashflow contradicts side"
        )


def _verify_fee_reference(value: FinalFeeEvidenceReferenceV2) -> None:
    document = {
        "actual_private_account_fee_claim": value.actual_private_account_fee_claim,
        "attempt_id": value.attempt_id,
        "entry_fee_usdt": _decimal_text(value.entry_fee_usdt),
        "exit_fee_usdt": _decimal_text(value.exit_fee_usdt),
        "exit_slice_count": value.exit_slice_count,
        "exit_slices_root_sha256": value.exit_slices_root_sha256,
        "fee_position_payload_sha256": value.fee_position_payload_sha256,
        "final_timeline_checkpoint_sha256": value.final_timeline_checkpoint_sha256,
        "final_timeline_root_sha256": value.final_timeline_root_sha256,
        "mandatory_exit_fee_certificate_sha256": (value.mandatory_exit_fee_certificate_sha256),
        "multiplier": value.multiplier.value,
        "position_id": value.position_id,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "rule_version": value.rule_version,
        "scenario": value.scenario,
        "source_authority": value.source_authority.value,
        "status": value.status.value,
        "symbol": value.symbol,
        "total_fee_usdt": _decimal_text(value.total_fee_usdt),
    }
    if value.reference_sha256 != _hash_document(_FEE_REFERENCE_DOMAIN, document):
        raise ProspectivePositionLifecycleContractErrorV2("fee evidence reference hash differs")
    if value.actual_private_account_fee_claim:
        raise ProspectivePositionLifecycleContractErrorV2(
            "public fee evidence cannot claim private account fees"
        )
    if (
        value.status.value != "BOTH_LEGS_COMPLETE"
        or value.source_authority.value != "CALLER_HASH_REFERENCE_NOT_MEMBERSHIP_PROOF"
        or type(value.exit_slice_count) is not int
        or value.exit_slice_count < 1
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "fee evidence is not a complete public-fee reference"
        )
    for amount, label in (
        (value.entry_fee_usdt, "entry_fee_usdt"),
        (value.exit_fee_usdt, "exit_fee_usdt"),
        (value.total_fee_usdt, "total_fee_usdt"),
    ):
        _finite_decimal(amount, label)
        if amount < 0:
            raise ProspectivePositionLifecycleContractErrorV2(f"{label} cannot be negative")
    if value.total_fee_usdt != _exact_sum(value.entry_fee_usdt, value.exit_fee_usdt):
        raise ProspectivePositionLifecycleContractErrorV2(
            "total fee differs from entry plus exit fee"
        )


def _verify_funding_reference(value: FundingCensusEvidenceReferenceV2) -> None:
    document = {
        "attempt_id": value.attempt_id,
        "boundary_convention": value.boundary_convention.value,
        "cashflow_event_count": value.cashflow_event_count,
        "cashflow_root_sha256": value.cashflow_root_sha256,
        "census_certificate_sha256": value.census_certificate_sha256,
        "confirmed_funding_count": value.confirmed_funding_count,
        "expected_funding_count": value.expected_funding_count,
        "interval_end_ms": value.interval_end_ms,
        "interval_start_ms": value.interval_start_ms,
        "observed_through_ms": value.observed_through_ms,
        "position_id": value.position_id,
        "position_ledger_checkpoint_sha256": (value.position_ledger_checkpoint_sha256),
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "public_data_only": value.public_data_only,
        "realized_funding_cashflow_usdt": _decimal_text(value.realized_funding_cashflow_usdt),
        "registry_checkpoint_sha256": value.registry_checkpoint_sha256,
        "rule_version": value.rule_version,
        "source_authority": value.source_authority.value,
        "symbol": value.symbol,
    }
    if value.reference_sha256 != _hash_document(_FUNDING_REFERENCE_DOMAIN, document):
        raise ProspectivePositionLifecycleContractErrorV2("funding evidence reference hash differs")
    if not value.public_data_only:
        raise ProspectivePositionLifecycleContractErrorV2(
            "funding evidence must use public data only"
        )
    if (
        value.source_authority.value != "CALLER_HASH_REFERENCE_NOT_MEMBERSHIP_PROOF"
        or value.boundary_convention.value
        != "Q_BEFORE_FUNDING_EQUAL_MS_AMBIGUOUS_USES_ADVERSE_ONLY_CASHFLOW"
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "funding evidence authority or boundary convention differs"
        )
    _finite_decimal(
        value.realized_funding_cashflow_usdt,
        "realized_funding_cashflow_usdt",
    )
    if not (
        value.expected_funding_count == value.confirmed_funding_count == value.cashflow_event_count
    ):
        raise ProspectivePositionLifecycleContractErrorV2("funding census counts are incomplete")
    if (
        value.interval_end_ms < value.interval_start_ms
        or value.observed_through_ms < value.interval_end_ms
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "funding census interval is incomplete or reversed"
        )


@dataclass(frozen=True, slots=True)
class _TerminalProjectionV2:
    identity: ProspectivePositionLifecycleIdentityV2
    terminal_status: ProspectivePositionTerminalStatusV2
    position_opened: bool
    finalized_at_ms: int
    paper_terminal_payload_sha256: str
    paper_terminal_jsonl_sha256: str
    paper_terminal_record_sha256: str | None
    paper_full_fill_certificate_sha256: str | None
    family_evidence: dict[str, object] | None
    mandatory_exit_evidence: dict[str, object] | None
    fee_evidence: dict[str, object] | None
    funding_evidence: dict[str, object] | None
    signed_entry_cashflow_usdt: Decimal | None
    signed_exit_cashflow_usdt: Decimal | None
    total_fee_usdt: Decimal | None
    realized_funding_cashflow_usdt: Decimal | None
    after_cost_pnl_usdt: Decimal | None
    costs_complete: bool
    arithmetic_complete: bool
    payload_sha256: str


_TERMINAL_KEYS: Final = frozenset(
    {
        "actual_private_account_fee_claim",
        "after_cost_pnl_usdt",
        "after_cost_return",
        "arithmetic_complete",
        "attempt_id",
        "attempt_plan_sha256",
        "authority_status",
        "bar_close_ms",
        "bar_open_ms",
        "costs_complete",
        "decision_cutoff_ms",
        "diagnostic_entry_slippage_usdt",
        "efficacy_eligible",
        "entry_executable_notional_usdt",
        "entry_executable_vwap",
        "entry_fee_usdt",
        "entry_quantity",
        "evidence_references_replay_authoritative",
        "execution_contract_sha256",
        "exit_fee_usdt",
        "exit_filled_quantity",
        "family",
        "family_evidence",
        "family_rule_version",
        "fee_evidence",
        "finalized_at_ms",
        "funding_evidence",
        "gross_exit_notional_usdt",
        "gross_pnl_usdt",
        "invalidation",
        "mandatory_exit_evidence",
        "origin_cell_id",
        "origin_segment_id",
        "outcome_id",
        "paper_full_fill_certificate_jsonl_sha256",
        "paper_full_fill_certificate_sha256",
        "paper_terminal_jsonl_sha256",
        "paper_terminal_payload_sha256",
        "paper_terminal_record_sha256",
        "payload_sha256",
        "pnl_denominator_usdt",
        "position_id",
        "position_opened",
        "position_side",
        "position_terminal",
        "position_terminal_authoritative",
        "production_order_placement",
        "promoting_plan_sha256",
        "protocol_rule_version",
        "realized_funding_cashflow_usdt",
        "reasons",
        "schema_version",
        "signed_entry_cashflow_usdt",
        "signed_exit_cashflow_usdt",
        "sizing_cell",
        "slippage_double_counted",
        "symbol",
        "terminal_rule_plan_bound",
        "terminal_status",
        "total_fee_usdt",
        "typed_wal_replay_authoritative",
        "upstream_paper_terminal_authoritative",
        "venue",
    }
)


def _parse_terminal_projection(encoded: bytes) -> _TerminalProjectionV2:
    if (
        type(encoded) is not bytes
        or not encoded
        or len(encoded) > (MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2)
    ):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "position terminal violates the fixed canonical payload bound"
        )
    try:
        decoded: object = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "position terminal is not valid UTF-8 JSON"
        ) from exc
    if not isinstance(decoded, dict):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "position terminal must be a JSON object"
        )
    document = cast(dict[str, object], decoded)
    if frozenset(document) != _TERMINAL_KEYS or canonical_json_line(document) != encoded:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "position terminal schema or canonical JSONL differs"
        )
    if _text(document, "schema_version") != POSITION_TERMINAL_PAYLOAD_SCHEMA_V2:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "position terminal schema version differs"
        )
    payload_sha256 = _text(document, "payload_sha256")
    _require_sha256(payload_sha256, "terminal payload_sha256")
    unhashed = dict(document)
    del unhashed["payload_sha256"]
    expected_hash = _hash_document(_POSITION_TERMINAL_PAYLOAD_DOMAIN, unhashed)
    if not hmac.compare_digest(payload_sha256, expected_hash):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "position terminal self-hash differs from canonical content"
        )
    position_id = _optional_text(document, "position_id")
    position_side_text = _optional_text(document, "position_side")
    identity = ProspectivePositionLifecycleIdentityV2(
        attempt_id=_text(document, "attempt_id"),
        attempt_plan_sha256=_text(document, "attempt_plan_sha256"),
        promoting_plan_sha256=_text(document, "promoting_plan_sha256"),
        execution_contract_sha256=_text(document, "execution_contract_sha256"),
        origin_segment_id=_text(document, "origin_segment_id"),
        origin_cell_id=_text(document, "origin_cell_id"),
        sizing_cell=PaperSizingCellV2(_text(document, "sizing_cell")),
        family=PromotingFamilyV2(_text(document, "family")),
        family_rule_version=_text(document, "family_rule_version"),
        symbol=_text(document, "symbol"),
        venue=VenueV2(_text(document, "venue")),
        bar_open_ms=_integer(document, "bar_open_ms"),
        bar_close_ms=_integer(document, "bar_close_ms"),
        decision_cutoff_ms=_integer(document, "decision_cutoff_ms"),
        outcome_id=_text(document, "outcome_id"),
        position_id=position_id,
        position_side=(
            None if position_side_text is None else MandatoryExitPositionSideV2(position_side_text)
        ),
        _factory_token=_FACTORY_TOKEN,
    )
    if (
        not _boolean(document, "position_terminal")
        or _boolean(document, "position_terminal_authoritative")
        or _boolean(document, "upstream_paper_terminal_authoritative")
        or _boolean(document, "evidence_references_replay_authoritative")
        or _boolean(document, "terminal_rule_plan_bound")
        or _boolean(document, "typed_wal_replay_authoritative")
        or _boolean(document, "production_order_placement")
        or _boolean(document, "actual_private_account_fee_claim")
        or _boolean(document, "slippage_double_counted")
        or _boolean(document, "efficacy_eligible")
    ):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "position terminal safety/authority flags differ"
        )
    _reasons(_string_tuple(document, "reasons"))
    _identity_text(_text(document, "invalidation"), "terminal invalidation")
    family_evidence = _optional_object(document, "family_evidence")
    mandatory_evidence = _optional_object(document, "mandatory_exit_evidence")
    fee_evidence = _optional_object(document, "fee_evidence")
    funding_evidence = _optional_object(document, "funding_evidence")
    for reference, domain, label in (
        (family_evidence, _FAMILY_REFERENCE_DOMAIN, "family_evidence"),
        (
            mandatory_evidence,
            _MANDATORY_EXIT_REFERENCE_DOMAIN,
            "mandatory_exit_evidence",
        ),
        (fee_evidence, _FEE_REFERENCE_DOMAIN, "fee_evidence"),
        (funding_evidence, _FUNDING_REFERENCE_DOMAIN, "funding_evidence"),
    ):
        if reference is not None:
            _verify_reference_document(reference, domain, label)
    return _TerminalProjectionV2(
        identity=identity,
        terminal_status=ProspectivePositionTerminalStatusV2(_text(document, "terminal_status")),
        position_opened=_boolean(document, "position_opened"),
        finalized_at_ms=_integer(document, "finalized_at_ms"),
        paper_terminal_payload_sha256=_text(document, "paper_terminal_payload_sha256"),
        paper_terminal_jsonl_sha256=_text(document, "paper_terminal_jsonl_sha256"),
        paper_terminal_record_sha256=_optional_text(document, "paper_terminal_record_sha256"),
        paper_full_fill_certificate_sha256=_optional_text(
            document, "paper_full_fill_certificate_sha256"
        ),
        family_evidence=family_evidence,
        mandatory_exit_evidence=mandatory_evidence,
        fee_evidence=fee_evidence,
        funding_evidence=funding_evidence,
        signed_entry_cashflow_usdt=_optional_decimal(document, "signed_entry_cashflow_usdt"),
        signed_exit_cashflow_usdt=_optional_decimal(document, "signed_exit_cashflow_usdt"),
        total_fee_usdt=_optional_decimal(document, "total_fee_usdt"),
        realized_funding_cashflow_usdt=_optional_decimal(
            document, "realized_funding_cashflow_usdt"
        ),
        after_cost_pnl_usdt=_optional_decimal(document, "after_cost_pnl_usdt"),
        costs_complete=_boolean(document, "costs_complete"),
        arithmetic_complete=_boolean(document, "arithmetic_complete"),
        payload_sha256=payload_sha256,
    )


def _verify_reference_document(
    reference: dict[str, object],
    domain: bytes,
    label: str,
) -> None:
    digest = _text(reference, "reference_sha256")
    _require_sha256(digest, f"{label} reference_sha256")
    document = dict(reference)
    del document["reference_sha256"]
    if digest != _hash_document(domain, document):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            f"{label} self-hash differs from canonical content"
        )


def _require_monotone_transition_time(
    state: _MutableTypedOutcomeStateV2,
    observed_at_ms: int,
    label: str,
) -> None:
    _safe_nonnegative_integer(observed_at_ms, label)
    if observed_at_ms < state.last_transition_at_ms:
        raise ProspectivePositionLifecycleContractErrorV2(
            f"{label} predates the preceding durable lifecycle transition"
        )


def _plan_typed_transition(
    *,
    current: _MutableTypedOutcomeStateV2 | None,
    payload: object,
    terminal_projection: _TerminalProjectionV2 | None,
    maximum_outcomes: int,
    current_outcome_count: int,
    durable_record_sha256: str,
) -> _MutableTypedOutcomeStateV2:
    if type(payload) is ProspectivePositionOpenPreparePayloadV2:
        if current is not None:
            raise ProspectivePositionLifecycleContractErrorV2("outcome already has an open prepare")
        if current_outcome_count >= maximum_outcomes:
            raise ProspectivePositionLifecycleContractErrorV2(
                "typed lifecycle state exceeds maximum_outcomes"
            )
        return _MutableTypedOutcomeStateV2(
            identity=payload.identity,
            phase=ProspectiveTypedLifecyclePhaseV2.OPEN_PREPARED,
            open_prepare=payload,
            open_disposition=None,
            admission_evidence=None,
            pending_exit_prepare=None,
            terminal_exit=None,
            cashflows={},
            terminal_payload_sha256=None,
            record_count=1,
            completed_exit_pair_count=0,
            last_record_sha256=durable_record_sha256,
            last_transition_at_ms=payload.prepared_at_ms,
        )
    if current is None:
        raise ProspectivePositionLifecycleContractErrorV2(
            "typed lifecycle must begin with POSITION_OPEN_PREPARE"
        )
    state = _clone_typed_state(current)
    if terminal_projection is not None:
        _require_monotone_transition_time(
            state,
            terminal_projection.finalized_at_ms,
            "position terminal finalization",
        )
        _validate_terminal_against_state(state, terminal_projection)
        state.phase = ProspectiveTypedLifecyclePhaseV2.TERMINAL
        state.terminal_payload_sha256 = terminal_projection.payload_sha256
        state.last_transition_at_ms = terminal_projection.finalized_at_ms
        state.record_count += 1
        state.last_record_sha256 = durable_record_sha256
        return state
    if not isinstance(
        payload,
        (
            ProspectivePositionOpenDispositionPayloadV2,
            ProspectiveFamilyExitPreparePayloadV2,
            ProspectiveFamilyExitDispositionPayloadV2,
            ProspectivePositionCashflowPayloadV2,
        ),
    ):
        raise ProspectivePositionLifecycleContractErrorV2("unsupported typed lifecycle transition")
    if payload.identity != state.identity:
        raise ProspectivePositionLifecycleContractErrorV2(
            "lifecycle payload identity differs from prior durable state"
        )
    if type(payload) is ProspectivePositionOpenDispositionPayloadV2:
        if state.phase is not ProspectiveTypedLifecyclePhaseV2.OPEN_PREPARED:
            raise ProspectivePositionLifecycleContractErrorV2(
                "open disposition requires exactly one pending open prepare"
            )
        if (
            payload.prepare_event_id != state.open_prepare.event_id
            or payload.prepare_payload_sha256 != state.open_prepare.payload_sha256
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "open disposition differs from its durable prepare"
            )
        _require_monotone_transition_time(
            state,
            payload.dispositioned_at_ms,
            "position open disposition",
        )
        state.open_disposition = payload
        state.admission_evidence = payload.admission_evidence
        state.phase = (
            ProspectiveTypedLifecyclePhaseV2.NO_POSITION
            if payload.disposition is ProspectivePositionOpenDispositionV2.SUPPRESSED_NO_POSITION
            else ProspectiveTypedLifecyclePhaseV2.POSITION_OPEN
        )
        state.last_transition_at_ms = payload.dispositioned_at_ms
    elif type(payload) is ProspectiveFamilyExitPreparePayloadV2:
        if state.phase is not ProspectiveTypedLifecyclePhaseV2.POSITION_OPEN:
            raise ProspectivePositionLifecycleContractErrorV2(
                "family exit prepare requires an open position"
            )
        admission = state.admission_evidence
        open_disposition = state.open_disposition
        if admission is None or open_disposition is None:
            raise ProspectivePositionLifecycleIntegrityErrorV2(
                "open position lacks its durable admission evidence"
            )
        if (
            payload.open_disposition_event_id != open_disposition.event_id
            or payload.admission_evidence_sha256 != admission.evidence_sha256
            or payload.entry_event_id != admission.entry_event_id
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "family exit prepare differs from durable admission"
            )
        _require_monotone_transition_time(
            state,
            payload.exit_decision_cutoff_ms,
            "family exit decision cutoff",
        )
        _require_monotone_transition_time(
            state,
            payload.prepared_at_ms,
            "family exit prepare",
        )
        state.pending_exit_prepare = payload
        state.phase = ProspectiveTypedLifecyclePhaseV2.EXIT_PREPARED
        state.last_transition_at_ms = payload.prepared_at_ms
    elif type(payload) is ProspectiveFamilyExitDispositionPayloadV2:
        pending = state.pending_exit_prepare
        if state.phase is not ProspectiveTypedLifecyclePhaseV2.EXIT_PREPARED or pending is None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "family exit disposition requires exactly one pending exit prepare"
            )
        evidence = payload.exit_evidence
        if (
            payload.exit_prepare_event_id != pending.event_id
            or payload.exit_prepare_payload_sha256 != pending.payload_sha256
            or evidence.entry_event_id != pending.entry_event_id
            or evidence.exit_event_id != pending.exit_event_id
            or evidence.exit_decision_payload_sha256 != pending.exit_decision_payload_sha256
            or evidence.exit_input_sha256 != pending.exit_input_sha256
            or evidence.exit_decision_cutoff_ms != pending.exit_decision_cutoff_ms
            or evidence.terminal_exit != pending.exits_position
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "family exit disposition differs from its durable prepare"
            )
        _require_monotone_transition_time(
            state,
            payload.dispositioned_at_ms,
            "family exit disposition",
        )
        state.pending_exit_prepare = None
        state.completed_exit_pair_count += 1
        if evidence.terminal_exit:
            if state.terminal_exit is not None:
                raise ProspectivePositionLifecycleContractErrorV2(
                    "position already has a terminal family exit"
                )
            state.terminal_exit = payload
            state.phase = ProspectiveTypedLifecyclePhaseV2.EXIT_TERMINAL
        else:
            state.phase = ProspectiveTypedLifecyclePhaseV2.POSITION_OPEN
        state.last_transition_at_ms = payload.dispositioned_at_ms
    elif type(payload) is ProspectivePositionCashflowPayloadV2:
        terminal_exit = state.terminal_exit
        if (
            state.phase is not ProspectiveTypedLifecyclePhaseV2.EXIT_TERMINAL
            or terminal_exit is None
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "cashflows require a terminal family exit"
            )
        if (
            payload.terminal_exit_event_id != terminal_exit.exit_evidence.exit_event_id
            or payload.terminal_exit_evidence_sha256 != terminal_exit.exit_evidence.evidence_sha256
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "cashflow differs from the durable terminal family exit"
            )
        if payload.cashflow_class in state.cashflows:
            raise ProspectivePositionLifecycleContractErrorV2(
                "each signed cashflow class can be appended exactly once"
            )
        if len(state.cashflows) >= MAX_PROSPECTIVE_POSITION_LIFECYCLE_CASHFLOWS_V2:
            raise ProspectivePositionLifecycleContractErrorV2(
                "position cashflows exceed the fixed bound"
            )
        _require_monotone_transition_time(
            state,
            payload.observed_at_ms,
            "position cashflow observation",
        )
        state.cashflows[payload.cashflow_class] = payload
        state.last_transition_at_ms = payload.observed_at_ms
    state.record_count += 1
    state.last_record_sha256 = durable_record_sha256
    return state


def _validate_terminal_against_state(
    state: _MutableTypedOutcomeStateV2,
    terminal: _TerminalProjectionV2,
) -> None:
    if terminal.identity != state.identity:
        raise ProspectivePositionLifecycleContractErrorV2(
            "position terminal identity differs from its durable lifecycle"
        )
    prepare = state.open_prepare
    if (
        terminal.paper_terminal_payload_sha256 != prepare.paper_terminal_payload_sha256
        or terminal.paper_terminal_jsonl_sha256 != prepare.paper_terminal_jsonl_sha256
        or terminal.paper_terminal_record_sha256 != prepare.paper_terminal_record_sha256
        or terminal.paper_full_fill_certificate_sha256 != prepare.full_fill_certificate_sha256
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "terminal PAPER evidence differs from the durable open prepare"
        )
    if terminal.finalized_at_ms < prepare.prepared_at_ms:
        raise ProspectivePositionLifecycleContractErrorV2(
            "position terminal predates its open prepare"
        )
    if state.phase is ProspectiveTypedLifecyclePhaseV2.NO_POSITION:
        if (
            terminal.position_opened
            or terminal.terminal_status
            is not ProspectivePositionTerminalStatusV2.SUPPRESSED_NO_POSITION
            or state.cashflows
            or any(
                value is not None
                for value in (
                    terminal.family_evidence,
                    terminal.mandatory_exit_evidence,
                    terminal.fee_evidence,
                    terminal.funding_evidence,
                )
            )
            or any(
                value != 0
                for value in (
                    terminal.signed_entry_cashflow_usdt,
                    terminal.signed_exit_cashflow_usdt,
                    terminal.total_fee_usdt,
                    terminal.realized_funding_cashflow_usdt,
                    terminal.after_cost_pnl_usdt,
                )
            )
            or not terminal.costs_complete
            or not terminal.arithmetic_complete
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                "no-position terminal is not an exact zero-cashflow suppression"
            )
        return
    if terminal.terminal_status is ProspectivePositionTerminalStatusV2.INCOMPLETE:
        _validate_opened_incomplete_terminal_against_state(state, terminal)
        return
    if (
        state.phase is not ProspectiveTypedLifecyclePhaseV2.EXIT_TERMINAL
        or state.terminal_exit is None
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "opened position cannot terminate before a terminal family exit"
        )
    if (
        not terminal.position_opened
        or terminal.terminal_status is not ProspectivePositionTerminalStatusV2.COMPLETE_CALCULATION
        or not terminal.costs_complete
        or not terminal.arithmetic_complete
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "opened position requires a complete calculated terminal"
        )
    required_classes = frozenset(ProspectivePositionCashflowClassV2)
    if frozenset(state.cashflows) != required_classes:
        raise ProspectivePositionLifecycleContractErrorV2(
            "terminal requires exactly four preceding signed cashflow classes"
        )
    family = terminal.family_evidence
    mandatory = terminal.mandatory_exit_evidence
    fee = terminal.fee_evidence
    funding = terminal.funding_evidence
    if any(value is None for value in (family, mandatory, fee, funding)):
        raise ProspectivePositionLifecycleContractErrorV2(
            "complete terminal lacks required evidence references"
        )
    assert family is not None and mandatory is not None
    assert fee is not None and funding is not None
    _validate_terminal_family_evidence(state, family)
    exit_disposition = state.terminal_exit
    if _text(mandatory, "family_exit_event_id") != (exit_disposition.exit_evidence.exit_event_id):
        raise ProspectivePositionLifecycleContractErrorV2(
            "mandatory-exit reference differs from terminal family exit"
        )
    expected = {
        ProspectivePositionCashflowClassV2.ENTRY_EXECUTION: (
            terminal.signed_entry_cashflow_usdt,
            terminal.paper_full_fill_certificate_sha256,
        ),
        ProspectivePositionCashflowClassV2.EXIT_EXECUTION: (
            terminal.signed_exit_cashflow_usdt,
            _text(mandatory, "reference_sha256"),
        ),
        ProspectivePositionCashflowClassV2.PUBLIC_FEE: (
            None if terminal.total_fee_usdt is None else -terminal.total_fee_usdt,
            _text(fee, "reference_sha256"),
        ),
        ProspectivePositionCashflowClassV2.PUBLIC_FUNDING: (
            terminal.realized_funding_cashflow_usdt,
            _text(funding, "reference_sha256"),
        ),
    }
    for cashflow_class, (amount, reference_sha256) in expected.items():
        cashflow = state.cashflows[cashflow_class]
        if (
            amount is None
            or reference_sha256 is None
            or cashflow.signed_amount_usdt != amount
            or cashflow.evidence_reference_sha256 != reference_sha256
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                f"terminal differs from preceding {cashflow_class.value} cashflow"
            )
        if cashflow.observed_at_ms > terminal.finalized_at_ms:
            raise ProspectivePositionLifecycleContractErrorV2(
                "position terminal predates a preceding cashflow observation"
            )
    observed_sum = _exact_sum(*(item.signed_amount_usdt for item in state.cashflows.values()))
    if terminal.after_cost_pnl_usdt is None or observed_sum != (terminal.after_cost_pnl_usdt):
        raise ProspectivePositionLifecycleContractErrorV2(
            "terminal after-cost PnL differs from exact preceding WAL cashflow sum"
        )


def _validate_opened_incomplete_terminal_against_state(
    state: _MutableTypedOutcomeStateV2,
    terminal: _TerminalProjectionV2,
) -> None:
    if state.phase not in (
        ProspectiveTypedLifecyclePhaseV2.POSITION_OPEN,
        ProspectiveTypedLifecyclePhaseV2.EXIT_PREPARED,
        ProspectiveTypedLifecyclePhaseV2.EXIT_TERMINAL,
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "opened incomplete terminal requires an admitted position lifecycle"
        )
    if (
        not terminal.position_opened
        or terminal.costs_complete
        or terminal.arithmetic_complete
        or terminal.signed_entry_cashflow_usdt is None
        or any(
            value is not None
            for value in (
                terminal.signed_exit_cashflow_usdt,
                terminal.total_fee_usdt,
                terminal.realized_funding_cashflow_usdt,
                terminal.after_cost_pnl_usdt,
            )
        )
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "opened incomplete terminal must retain entry only and null final arithmetic"
        )
    admission = state.admission_evidence
    if admission is None or state.open_disposition is None:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "opened incomplete lifecycle lacks durable admission evidence"
        )
    family = terminal.family_evidence
    mandatory = terminal.mandatory_exit_evidence
    fee = terminal.fee_evidence
    funding = terminal.funding_evidence
    terminal_exit = state.terminal_exit
    if family is not None:
        if terminal_exit is None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "incomplete terminal cannot claim family terminal evidence before disposition"
            )
        _validate_terminal_family_evidence(state, family)
    if mandatory is not None:
        if terminal_exit is None or family is None:
            raise ProspectivePositionLifecycleContractErrorV2(
                "incomplete mandatory-exit evidence requires durable family terminal evidence"
            )
        if _text(mandatory, "family_exit_event_id") != (terminal_exit.exit_evidence.exit_event_id):
            raise ProspectivePositionLifecycleContractErrorV2(
                "incomplete mandatory-exit reference differs from terminal family exit"
            )
    expected: dict[
        ProspectivePositionCashflowClassV2,
        tuple[Decimal | None, str | None],
    ] = {
        ProspectivePositionCashflowClassV2.ENTRY_EXECUTION: (
            terminal.signed_entry_cashflow_usdt,
            terminal.paper_full_fill_certificate_sha256,
        ),
        ProspectivePositionCashflowClassV2.EXIT_EXECUTION: (
            None if mandatory is None else _decimal(mandatory, "signed_exit_cashflow_usdt"),
            None if mandatory is None else _text(mandatory, "reference_sha256"),
        ),
        ProspectivePositionCashflowClassV2.PUBLIC_FEE: (
            None if fee is None else -_decimal(fee, "total_fee_usdt"),
            None if fee is None else _text(fee, "reference_sha256"),
        ),
        ProspectivePositionCashflowClassV2.PUBLIC_FUNDING: (
            None if funding is None else _decimal(funding, "realized_funding_cashflow_usdt"),
            None if funding is None else _text(funding, "reference_sha256"),
        ),
    }
    for cashflow_class, cashflow in state.cashflows.items():
        amount, reference_sha256 = expected[cashflow_class]
        if (
            amount is None
            or reference_sha256 is None
            or cashflow.signed_amount_usdt != amount
            or cashflow.evidence_reference_sha256 != reference_sha256
        ):
            raise ProspectivePositionLifecycleContractErrorV2(
                f"incomplete terminal differs from preceding {cashflow_class.value} cashflow"
            )


def _validate_terminal_family_evidence(
    state: _MutableTypedOutcomeStateV2,
    family: dict[str, object],
) -> None:
    admission = state.admission_evidence
    terminal_exit = state.terminal_exit
    if admission is None or terminal_exit is None:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "terminal lifecycle evidence lacks admission or exit state"
        )
    exit_evidence = terminal_exit.exit_evidence
    identity = state.identity
    expected_pairs: tuple[tuple[str, object], ...] = (
        ("family", identity.family.value),
        ("attempt_id", identity.attempt_id),
        ("promoting_plan_sha256", identity.promoting_plan_sha256),
        ("symbol", identity.symbol),
        ("position_id", identity.position_id),
        ("position_side", None),
        ("side", identity.position_side.value if identity.position_side else None),
        ("entry_event_id", admission.entry_event_id),
        (
            "full_fill_certificate_sha256",
            admission.full_fill_certificate_sha256,
        ),
        ("admission_input_sha256", admission.admission_input_sha256),
        ("admission_pre_root_sha256", admission.admission_pre_root_sha256),
        ("admission_pre_event_count", admission.admission_pre_event_count),
        ("admission_post_root_sha256", admission.admission_post_root_sha256),
        ("admission_post_event_count", admission.admission_post_event_count),
        ("admission_disposition", admission.admission_disposition),
        ("exit_event_id", exit_evidence.exit_event_id),
        ("exit_input_sha256", exit_evidence.exit_input_sha256),
        ("exit_pre_root_sha256", exit_evidence.exit_pre_root_sha256),
        ("exit_pre_event_count", exit_evidence.exit_pre_event_count),
        ("exit_post_root_sha256", exit_evidence.exit_post_root_sha256),
        ("exit_post_event_count", exit_evidence.exit_post_event_count),
        ("exit_disposition", exit_evidence.exit_disposition),
        ("exit_decision_cutoff_ms", exit_evidence.exit_decision_cutoff_ms),
        ("terminal_exit", True),
    )
    for key, expected in expected_pairs:
        if key == "position_side":
            continue
        if family.get(key) != expected:
            raise ProspectivePositionLifecycleContractErrorV2(
                f"terminal family evidence differs at {key}"
            )


def _clone_typed_state(
    value: _MutableTypedOutcomeStateV2,
) -> _MutableTypedOutcomeStateV2:
    return _MutableTypedOutcomeStateV2(
        identity=value.identity,
        phase=value.phase,
        open_prepare=value.open_prepare,
        open_disposition=value.open_disposition,
        admission_evidence=value.admission_evidence,
        pending_exit_prepare=value.pending_exit_prepare,
        terminal_exit=value.terminal_exit,
        cashflows=dict(value.cashflows),
        terminal_payload_sha256=value.terminal_payload_sha256,
        record_count=value.record_count,
        completed_exit_pair_count=value.completed_exit_pair_count,
        last_record_sha256=value.last_record_sha256,
        last_transition_at_ms=value.last_transition_at_ms,
    )


def _rebuild_typed_states(
    *,
    plan: ProspectiveCensusPlanV2,
    replay_snapshot: ProspectiveOutcomeWalReplaySnapshotV2,
    config: ProspectivePositionLifecycleOwnerConfigV2,
) -> dict[str, _MutableTypedOutcomeStateV2]:
    states: dict[str, _MutableTypedOutcomeStateV2] = {}
    for record in replay_snapshot.records:
        try:
            payload, terminal_projection = _parse_record_payload(record)
            identity = (
                terminal_projection.identity
                if terminal_projection is not None
                else cast(_PayloadV2, payload).identity
            )
            _cell_for_identity(plan, identity)
            if (
                record.attempt_plan_sha256 != identity.attempt_plan_sha256
                or record.origin_segment_id != identity.origin_segment_id
                or record.origin_cell_id != identity.origin_cell_id
                or record.sizing_cell is not identity.sizing_cell
                or record.outcome_id != identity.outcome_id
            ):
                raise ProspectivePositionLifecycleIntegrityErrorV2(
                    "typed replay payload differs from its WAL record envelope"
                )
            states[record.outcome_id] = _plan_typed_transition(
                current=states.get(record.outcome_id),
                payload=payload,
                terminal_projection=terminal_projection,
                maximum_outcomes=config.maximum_outcomes,
                current_outcome_count=len(states),
                durable_record_sha256=record.record_sha256,
            )
        except ProspectivePositionLifecycleIntegrityErrorV2:
            raise
        except (
            ProspectivePositionLifecycleOwnerErrorV2,
            TypeError,
            ValueError,
        ) as exc:
            raise ProspectivePositionLifecycleIntegrityErrorV2(
                "typed lifecycle replay rejected a durable payload"
            ) from exc
    structural_by_outcome = {outcome.outcome_id: outcome for outcome in replay_snapshot.outcomes}
    if frozenset(structural_by_outcome) != frozenset(states):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            "typed replay outcomes differ from structural store snapshot"
        )
    for outcome_id, state in states.items():
        structural = structural_by_outcome[outcome_id]
        expected_phase = _structural_phase_value(state.phase)
        if (
            structural.origin_segment_id != state.identity.origin_segment_id
            or structural.origin_cell_id != state.identity.origin_cell_id
            or structural.sizing_cell is not state.identity.sizing_cell
            or structural.phase.value != expected_phase
            or structural.record_count != state.record_count
            or structural.cashflow_count != len(state.cashflows)
            or structural.completed_exit_pair_count != state.completed_exit_pair_count
            or structural.last_record_sha256 != state.last_record_sha256
        ):
            raise ProspectivePositionLifecycleIntegrityErrorV2(
                "typed replay state differs from structural outcome snapshot"
            )
    return states


def _parse_record_payload(
    record: ProspectiveOutcomeWalRecordV2,
) -> tuple[object, _TerminalProjectionV2 | None]:
    record.verify_integrity()
    if record.kind is ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE:
        return parse_prospective_position_open_prepare_payload_v2(record.payload_jsonl), None
    if record.kind is ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION:
        return (
            parse_prospective_position_open_disposition_payload_v2(record.payload_jsonl),
            None,
        )
    if record.kind is ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_PREPARE:
        return parse_prospective_family_exit_prepare_payload_v2(record.payload_jsonl), None
    if record.kind is ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_DISPOSITION:
        return (
            parse_prospective_family_exit_disposition_payload_v2(record.payload_jsonl),
            None,
        )
    if record.kind is ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW:
        return parse_prospective_position_cashflow_payload_v2(record.payload_jsonl), None
    if record.kind is ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL:
        projection = _parse_terminal_projection(record.payload_jsonl)
        return record.payload_jsonl, projection
    raise ProspectivePositionLifecycleIntegrityErrorV2("outcome WAL record kind is unsupported")


def _structural_phase_value(phase: ProspectiveTypedLifecyclePhaseV2) -> str:
    if phase is ProspectiveTypedLifecyclePhaseV2.OPEN_PREPARED:
        return "OPEN_PREPARED"
    if phase is ProspectiveTypedLifecyclePhaseV2.EXIT_PREPARED:
        return "EXIT_PREPARED"
    if phase is ProspectiveTypedLifecyclePhaseV2.TERMINAL:
        return "TERMINAL"
    return "OPEN_DISPOSITIONED"


def _validate_plan_and_lease(
    plan: ProspectiveCensusPlanV2,
    writer_lease: WriterLease,
) -> None:
    if type(plan) is not ProspectiveCensusPlanV2:
        raise ProspectivePositionLifecycleContractErrorV2(
            "plan must be exact ProspectiveCensusPlanV2"
        )
    canonical_prospective_census_plan_v2(plan)
    if plan.execution_contract_sha256 != current_prospective_execution_contract_sha256_v2():
        raise ProspectivePositionLifecycleContractErrorV2(
            "plan does not bind the current prospective execution contract"
        )
    if type(writer_lease) is not WriterLease:
        raise ProspectivePositionLifecycleContractErrorV2("writer_lease must be exact WriterLease")
    with writer_lease.operation_guard():
        writer_lease.assert_prospective_attempt_authority_claim(
            attempt_plan_sha256=plan.plan_sha256
        )


def _validate_owner_attempt(
    plan: ProspectiveCensusPlanV2,
    writer_lease: WriterLease,
    outcome_store: ProspectiveOutcomeWalStoreV2,
) -> None:
    _validate_plan_and_lease(plan, writer_lease)
    if type(outcome_store) is not ProspectiveOutcomeWalStoreV2:
        raise ProspectivePositionLifecycleContractErrorV2(
            "outcome_store must be exact ProspectiveOutcomeWalStoreV2"
        )
    with writer_lease.operation_guard():
        outcome_store.assert_position_lifecycle_binding_v2(
            census_plan=plan,
            writer_lease=writer_lease,
            lifecycle_owner=object(),
        )


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _exact_sum(*values: Decimal) -> Decimal:
    total = sum((_decimal_fraction(value) for value in values), Fraction(0))
    return _finite_fraction_decimal(total)


def _decimal_fraction(value: Decimal) -> Fraction:
    _finite_decimal(value, "exact Decimal")
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise ProspectivePositionLifecycleContractErrorV2("exact Decimal exponent must be finite")
    coefficient = 0
    for digit in digits:
        coefficient = coefficient * 10 + digit
    if sign:
        coefficient = -coefficient
    if exponent >= 0:
        return Fraction(coefficient * (10**exponent), 1)
    return Fraction(coefficient, 10 ** (-exponent))


def _finite_fraction_decimal(value: Fraction) -> Decimal:
    denominator = value.denominator
    twos = 0
    fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator != 1:
        raise ProspectivePositionLifecycleContractErrorV2(
            "exact cashflow sum is not finite base-ten"
        )
    scale = max(twos, fives)
    scaled = value.numerator * (2 ** (scale - twos)) * (5 ** (scale - fives))
    negative = scaled < 0
    digits = str(abs(scaled))
    if scale:
        digits = digits.zfill(scale + 1)
        text = f"{digits[:-scale]}.{digits[-scale:]}"
    else:
        text = digits
    if negative and scaled != 0:
        text = f"-{text}"
    return Decimal(text)


def _decimal_text(value: Decimal) -> str:
    _finite_decimal(value, "canonical Decimal")
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


def _finite_decimal(value: object, label: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ProspectivePositionLifecycleContractErrorV2(f"{label} must be a finite Decimal")


def _require_sha256(value: object, label: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ProspectivePositionLifecycleContractErrorV2(f"{label} must be lowercase SHA-256")


def _identity_text(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 512
        or any(character in value for character in "\r\n\x00")
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            f"{label} must be a bounded canonical identity"
        )
    return value


def _symbol(value: object) -> str:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise ProspectivePositionLifecycleContractErrorV2("symbol must be an uppercase USDT symbol")
    return value


def _safe_nonnegative_integer(value: object, label: str) -> int:
    if type(value) is not int or not 0 <= value <= _JCS_MAX_SAFE_INTEGER:
        raise ProspectivePositionLifecycleContractErrorV2(
            f"{label} must be a nonnegative JCS-safe integer"
        )
    return value


def _reasons(values: tuple[str, ...]) -> None:
    if (
        type(values) is not tuple
        or not values
        or len(values) > MAX_PROSPECTIVE_POSITION_LIFECYCLE_REASONS_V2
    ):
        raise ProspectivePositionLifecycleContractErrorV2(
            "reasons must be a non-empty bounded tuple"
        )
    for value in values:
        _identity_text(value, "reason")


def _text(document: dict[str, object], field_name: str) -> str:
    value = document.get(field_name)
    if not isinstance(value, str):
        raise ProspectivePositionLifecycleIntegrityErrorV2(f"{field_name} must be text")
    return value


def _optional_text(document: dict[str, object], field_name: str) -> str | None:
    value = document.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ProspectivePositionLifecycleIntegrityErrorV2(f"{field_name} must be text or null")
    return value


def _integer(document: dict[str, object], field_name: str) -> int:
    value = document.get(field_name)
    if type(value) is not int:
        raise ProspectivePositionLifecycleIntegrityErrorV2(f"{field_name} must be an integer")
    return value


def _boolean(document: dict[str, object], field_name: str) -> bool:
    value = document.get(field_name)
    if type(value) is not bool:
        raise ProspectivePositionLifecycleIntegrityErrorV2(f"{field_name} must be boolean")
    return value


def _decimal(document: dict[str, object], field_name: str) -> Decimal:
    value = document.get(field_name)
    if not isinstance(value, str):
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            f"{field_name} must be canonical Decimal text"
        )
    try:
        decimal = Decimal(value)
    except Exception as exc:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            f"{field_name} is not Decimal text"
        ) from exc
    _finite_decimal(decimal, field_name)
    if _decimal_text(decimal) != value:
        raise ProspectivePositionLifecycleIntegrityErrorV2(
            f"{field_name} is not canonical Decimal text"
        )
    return decimal


def _optional_decimal(document: dict[str, object], field_name: str) -> Decimal | None:
    if document.get(field_name) is None:
        return None
    return _decimal(document, field_name)


def _object(document: dict[str, object], field_name: str) -> dict[str, object]:
    return _cast_object(document.get(field_name), field_name)


def _optional_object(document: dict[str, object], field_name: str) -> dict[str, object] | None:
    value = document.get(field_name)
    if value is None:
        return None
    return _cast_object(value, field_name)


def _cast_object(value: object, field_name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ProspectivePositionLifecycleIntegrityErrorV2(f"{field_name} must be an object")
    return cast(dict[str, object], value)


def _string_tuple(document: dict[str, object], field_name: str) -> tuple[str, ...]:
    value = document.get(field_name)
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ProspectivePositionLifecycleIntegrityErrorV2(f"{field_name} must be a string list")
    return tuple(cast(list[str], value))
