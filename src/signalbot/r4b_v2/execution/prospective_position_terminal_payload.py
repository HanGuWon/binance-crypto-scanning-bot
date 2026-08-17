"""Typed prospective position-terminal calculation payloads.

This module joins one frozen prospective cell and sizing identity to a typed
PAPER full-fill entry, family lifecycle references, a mandatory-exit
reference, final public-fee evidence, and a complete funding census.  Large
source objects are not embedded: the terminal retains bounded canonical
hash/checkpoint projections and exact Decimal cashflow totals.

The resulting calculation is intentionally *not* an authoritative efficacy
result yet.  The frozen execution contract still declares
``position_terminal_typed=false``; the upstream PAPER terminal explicitly
withholds restart-replay and causal reference-membership authority; family
receipts remain process-local and the mandatory-exit/fee/funding joins are
hash references rather than attempt-wide replay-owned certificates.  Those
facts are immutable flags on every payload instead of being hidden behind a
successful arithmetic calculation.

Nothing in this module performs an order, calls a private API, or subtracts
diagnostic slippage a second time.  Entry and exit executable notionals already
contain book impact.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from fractions import Fraction
from typing import Final, TypedDict, cast

from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.fees import (
    FEE_RULE_VERSION_V2,
    PUBLIC_FEE_SCENARIO_V2,
    FeeMultiplierV2,
    FilledPositionFeeStatusV2,
)
from signalbot.r4b_v2.execution.funding import FUNDING_RULE_VERSION_V2
from signalbot.r4b_v2.execution.mandatory_exit import (
    MANDATORY_EXIT_RULE_VERSION_V2,
    MandatoryExitPositionSideV2,
    MandatoryExitTerminalStatusV2,
)
from signalbot.r4b_v2.execution.paper_fok import (
    PaperFokFullFillCertificateV2,
    PaperFokSideV2,
    canonical_paper_fok_entry_decision_v2,
    canonical_paper_fok_full_fill_certificate_v2,
)
from signalbot.r4b_v2.execution.paper_sizing import PaperSizingCellV2
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveExpectedCellV2,
    canonical_prospective_census_plan_v2,
)
from signalbot.r4b_v2.execution.prospective_outcome_wal_record import (
    MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2,
    POSITION_TERMINAL_PAYLOAD_SCHEMA_V2,
    prospective_outcome_id_v2,
)
from signalbot.r4b_v2.execution.prospective_paper_terminal_payload import (
    ProspectivePaperTerminalCompletenessV2,
    ProspectivePaperTerminalPayloadV2,
    ProspectivePaperTerminalStatusV2,
    canonical_prospective_paper_terminal_payload_v2,
)
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.family_a import (
    FamilyAAdmissionReceiptV2,
    FamilyAExitMutationReceiptV2,
    canonical_family_a_exit_decision_v2,
)
from signalbot.r4b_v2.strategy.family_b import (
    FamilyBAdmissionReceiptV2,
    FamilyBExitMutationReceiptV2,
    canonical_family_b_entry_decision_v2,
    canonical_family_b_exit_decision_v2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FamilyCAdmissionReceiptV2,
    FamilyCExitMutationReceiptV2,
    canonical_family_c_entry_decision_v2,
    canonical_family_c_exit_decision_v2,
)
from signalbot.r4b_v2.strategy.prospective_plan import (
    current_prospective_execution_contract_sha256_v2,
)

PROSPECTIVE_POSITION_TERMINAL_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.0_PROSPECTIVE_POSITION_TERMINAL_DRAFT"
)
PROSPECTIVE_POSITION_TERMINAL_AUTHORITY_V2: Final = (
    "NONAUTHORITATIVE_NOT_PLAN_BOUND_OR_ATTEMPT_REPLAY_OWNED"
)

_PAYLOAD_HASH_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_POSITION_TERMINAL_PAYLOAD_V2\0"
_POSITION_ID_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_POSITION_ID_V2\0"
_FAMILY_REFERENCE_DOMAIN: Final = b"R4B_V2_FAMILY_LIFECYCLE_REFERENCE_V2\0"
_FAMILY_A_ADMISSION_REFERENCE_DOMAIN: Final = b"R4B_V2_FAMILY_A_ADMISSION_RECEIPT_REFERENCE_V2\0"
_FAMILY_A_EXIT_REFERENCE_DOMAIN: Final = b"R4B_V2_FAMILY_A_EXIT_RECEIPT_REFERENCE_V2\0"
_FAMILY_B_ADMISSION_REFERENCE_DOMAIN: Final = b"R4B_V2_FAMILY_B_ADMISSION_RECEIPT_REFERENCE_V2\0"
_FAMILY_B_EXIT_REFERENCE_DOMAIN: Final = b"R4B_V2_FAMILY_B_EXIT_RECEIPT_REFERENCE_V2\0"
_FAMILY_C_ADMISSION_REFERENCE_DOMAIN: Final = b"R4B_V2_FAMILY_C_ADMISSION_RECEIPT_REFERENCE_V2\0"
_FAMILY_C_EXIT_REFERENCE_DOMAIN: Final = b"R4B_V2_FAMILY_C_EXIT_RECEIPT_REFERENCE_V2\0"
_MANDATORY_EXIT_REFERENCE_DOMAIN: Final = b"R4B_V2_MANDATORY_EXIT_REFERENCE_V2\0"
_FEE_REFERENCE_DOMAIN: Final = b"R4B_V2_FINAL_FEE_REFERENCE_V2\0"
_FUNDING_REFERENCE_DOMAIN: Final = b"R4B_V2_FUNDING_CENSUS_REFERENCE_V2\0"
_PAYLOAD_FACTORY_TOKEN: Final = object()
_REFERENCE_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_JCS_MAX_SAFE_INTEGER: Final = 9_007_199_254_740_991
_MAX_REASONS: Final = 32


class ProspectivePositionTerminalContractErrorV2(ValueError):
    """Raised when a position-terminal source or projection is contradictory."""


class ProspectivePositionTerminalStatusV2(StrEnum):
    """Outcome status, separate from authority and efficacy eligibility."""

    SUPPRESSED_NO_POSITION = "SUPPRESSED_NO_POSITION"
    INCOMPLETE = "INCOMPLETE"
    COMPLETE_CALCULATION = "COMPLETE_CALCULATION"


class PositionEvidenceReferenceAuthorityV2(StrEnum):
    """Truthful provenance class for compact lifecycle references."""

    LIVE_FACTORY_SEALED_FAMILY_A_RECEIPTS = "LIVE_FACTORY_SEALED_FAMILY_A_RECEIPTS_PROCESS_LOCAL"
    LIVE_FACTORY_SEALED_FAMILY_B_RECEIPTS = "LIVE_FACTORY_SEALED_FAMILY_B_RECEIPTS_PROCESS_LOCAL"
    LIVE_FACTORY_SEALED_FAMILY_C_RECEIPTS = "LIVE_FACTORY_SEALED_FAMILY_C_RECEIPTS_PROCESS_LOCAL"
    CALLER_HASH_REFERENCE = "CALLER_HASH_REFERENCE_NOT_MEMBERSHIP_PROOF"


class FundingCensusBoundaryConventionV2(StrEnum):
    """Frozen equality convention used by the existing funding calculator."""

    Q_BEFORE_FUNDING_EQUAL_MS_ADVERSE_ONLY = (
        "Q_BEFORE_FUNDING_EQUAL_MS_AMBIGUOUS_USES_ADVERSE_ONLY_CASHFLOW"
    )


class _CommonTerminalFieldsV2(TypedDict):
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
    finalized_at_ms: int
    paper_terminal_payload_sha256: str
    paper_terminal_jsonl_sha256: str
    paper_terminal_record_sha256: str | None
    diagnostic_entry_slippage_usdt: Decimal | None


@dataclass(frozen=True, slots=True)
class FamilyLifecycleEvidenceReferenceV2:
    """Compact reference to one admission and one terminal family exit."""

    family: PromotingFamilyV2
    attempt_id: str
    promoting_plan_sha256: str
    symbol: str
    position_id: str
    side: MandatoryExitPositionSideV2
    entry_event_id: str
    full_fill_certificate_sha256: str
    admission_receipt_sha256: str
    admission_input_sha256: str
    admission_pre_root_sha256: str
    admission_pre_event_count: int
    admission_post_root_sha256: str
    admission_post_event_count: int
    admission_disposition: str
    exit_event_id: str
    exit_receipt_sha256: str
    exit_input_sha256: str
    exit_pre_root_sha256: str
    exit_pre_event_count: int
    exit_post_root_sha256: str
    exit_post_event_count: int
    exit_disposition: str
    exit_decision_cutoff_ms: int
    terminal_exit: bool
    source_authority: PositionEvidenceReferenceAuthorityV2
    _factory_token: InitVar[object | None] = None
    reference_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _REFERENCE_FACTORY_TOKEN:
            raise ProspectivePositionTerminalContractErrorV2(
                "family lifecycle references are factory-sealed"
            )
        _validate_family_reference(self)
        object.__setattr__(
            self,
            "reference_sha256",
            _hash_document(_FAMILY_REFERENCE_DOMAIN, _family_reference_document(self)),
        )


@dataclass(frozen=True, slots=True)
class MandatoryExitEvidenceReferenceV2:
    """Compact complete-exit projection; raw books and attempts stay external."""

    attempt_id: str
    promoting_plan_sha256: str
    symbol: str
    position_id: str
    family_exit_event_id: str
    side: MandatoryExitPositionSideV2
    mandatory_position_sha256: str
    exit_intent_sha256: str
    target_cursor_sha256: str
    terminal_sha256: str
    terminal_payload_sha256: str
    fee_certificate_sha256: str
    ledger_checkpoint_sha256: str
    exit_slices_root_sha256: str
    exit_slice_count: int
    filled_quantity: Decimal
    residual_quantity: Decimal
    gross_exit_notional_usdt: Decimal
    signed_exit_cashflow_usdt: Decimal
    terminal_at_ms: int
    terminal_status: MandatoryExitTerminalStatusV2
    source_authority: PositionEvidenceReferenceAuthorityV2
    _factory_token: InitVar[object | None] = None
    reference_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=MANDATORY_EXIT_RULE_VERSION_V2)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _REFERENCE_FACTORY_TOKEN:
            raise ProspectivePositionTerminalContractErrorV2(
                "mandatory-exit references are factory-sealed"
            )
        _validate_mandatory_exit_reference(self)
        object.__setattr__(
            self,
            "reference_sha256",
            _hash_document(
                _MANDATORY_EXIT_REFERENCE_DOMAIN,
                _mandatory_exit_reference_document(self),
            ),
        )


@dataclass(frozen=True, slots=True)
class FinalFeeEvidenceReferenceV2:
    """Compact final public-fee timeline and exact realized totals."""

    attempt_id: str
    promoting_plan_sha256: str
    symbol: str
    position_id: str
    mandatory_exit_fee_certificate_sha256: str
    final_timeline_checkpoint_sha256: str
    final_timeline_root_sha256: str
    fee_position_payload_sha256: str
    exit_slices_root_sha256: str
    exit_slice_count: int
    multiplier: FeeMultiplierV2
    entry_fee_usdt: Decimal
    exit_fee_usdt: Decimal
    total_fee_usdt: Decimal
    status: FilledPositionFeeStatusV2
    source_authority: PositionEvidenceReferenceAuthorityV2
    _factory_token: InitVar[object | None] = None
    reference_sha256: str = field(init=False)
    scenario: str = field(init=False, default=PUBLIC_FEE_SCENARIO_V2)
    rule_version: str = field(init=False, default=FEE_RULE_VERSION_V2)
    actual_private_account_fee_claim: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _REFERENCE_FACTORY_TOKEN:
            raise ProspectivePositionTerminalContractErrorV2(
                "final-fee references are factory-sealed"
            )
        _validate_fee_reference(self)
        object.__setattr__(
            self,
            "reference_sha256",
            _hash_document(_FEE_REFERENCE_DOMAIN, _fee_reference_document(self)),
        )


@dataclass(frozen=True, slots=True)
class FundingCensusEvidenceReferenceV2:
    """Compact proof claim for a complete funding-time census and sum."""

    attempt_id: str
    promoting_plan_sha256: str
    symbol: str
    position_id: str
    census_certificate_sha256: str
    registry_checkpoint_sha256: str
    position_ledger_checkpoint_sha256: str
    cashflow_root_sha256: str
    expected_funding_count: int
    confirmed_funding_count: int
    cashflow_event_count: int
    interval_start_ms: int
    interval_end_ms: int
    observed_through_ms: int
    realized_funding_cashflow_usdt: Decimal
    boundary_convention: FundingCensusBoundaryConventionV2
    source_authority: PositionEvidenceReferenceAuthorityV2
    _factory_token: InitVar[object | None] = None
    reference_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=FUNDING_RULE_VERSION_V2)
    public_data_only: bool = field(init=False, default=True)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _REFERENCE_FACTORY_TOKEN:
            raise ProspectivePositionTerminalContractErrorV2(
                "funding-census references are factory-sealed"
            )
        _validate_funding_reference(self)
        object.__setattr__(
            self,
            "reference_sha256",
            _hash_document(_FUNDING_REFERENCE_DOMAIN, _funding_reference_document(self)),
        )


@dataclass(frozen=True, slots=True)
class ProspectivePositionTerminalPayloadV2:
    """One bounded, self-hashed position outcome calculation."""

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
    position_opened: bool
    terminal_status: ProspectivePositionTerminalStatusV2
    reasons: tuple[str, ...]
    invalidation: str
    finalized_at_ms: int
    paper_terminal_payload_sha256: str
    paper_terminal_jsonl_sha256: str
    paper_terminal_record_sha256: str | None
    paper_full_fill_certificate_sha256: str | None
    paper_full_fill_certificate_jsonl_sha256: str | None
    family_evidence: FamilyLifecycleEvidenceReferenceV2 | None
    mandatory_exit_evidence: MandatoryExitEvidenceReferenceV2 | None
    fee_evidence: FinalFeeEvidenceReferenceV2 | None
    funding_evidence: FundingCensusEvidenceReferenceV2 | None
    entry_quantity: Decimal | None
    entry_executable_vwap: Decimal | None
    entry_executable_notional_usdt: Decimal | None
    signed_entry_cashflow_usdt: Decimal | None
    exit_filled_quantity: Decimal | None
    gross_exit_notional_usdt: Decimal | None
    signed_exit_cashflow_usdt: Decimal | None
    gross_pnl_usdt: Decimal | None
    entry_fee_usdt: Decimal | None
    exit_fee_usdt: Decimal | None
    total_fee_usdt: Decimal | None
    realized_funding_cashflow_usdt: Decimal | None
    after_cost_pnl_usdt: Decimal | None
    pnl_denominator_usdt: Decimal | None
    after_cost_return: Decimal | None
    diagnostic_entry_slippage_usdt: Decimal | None
    costs_complete: bool
    arithmetic_complete: bool
    _factory_token: InitVar[object | None] = None
    payload_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=POSITION_TERMINAL_PAYLOAD_SCHEMA_V2)
    protocol_rule_version: str = field(
        init=False,
        default=PROSPECTIVE_POSITION_TERMINAL_RULE_VERSION_V2,
    )
    authority_status: str = field(
        init=False,
        default=PROSPECTIVE_POSITION_TERMINAL_AUTHORITY_V2,
    )
    position_terminal: bool = field(init=False, default=True)
    position_terminal_authoritative: bool = field(init=False, default=False)
    upstream_paper_terminal_authoritative: bool = field(init=False, default=False)
    evidence_references_replay_authoritative: bool = field(init=False, default=False)
    terminal_rule_plan_bound: bool = field(init=False, default=False)
    typed_wal_replay_authoritative: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)
    actual_private_account_fee_claim: bool = field(init=False, default=False)
    slippage_double_counted: bool = field(init=False, default=False)
    efficacy_eligible: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PAYLOAD_FACTORY_TOKEN:
            raise ProspectivePositionTerminalContractErrorV2(
                "position terminal payloads are factory-sealed"
            )
        _validate_payload_shape(self)
        object.__setattr__(
            self,
            "payload_sha256",
            _hash_document(_PAYLOAD_HASH_DOMAIN, _payload_document(self)),
        )


def prospective_position_id_v2(*, outcome_id: str, certificate_sha256: str) -> str:
    """Derive a deterministic identity only for an actually opened position."""

    _require_sha256(outcome_id, "outcome_id")
    _require_sha256(certificate_sha256, "certificate_sha256")
    return _hash_document(
        _POSITION_ID_DOMAIN,
        {
            "certificate_sha256": certificate_sha256,
            "outcome_id": outcome_id,
        },
    )


def build_family_a_lifecycle_evidence_reference_v2(
    *,
    position_id: str,
    admission: FamilyAAdmissionReceiptV2,
    terminal_exit: FamilyAExitMutationReceiptV2,
) -> FamilyLifecycleEvidenceReferenceV2:
    """Project exact live Family A receipts without serializing owner tokens."""

    _require_sha256(position_id, "position_id")
    if type(admission) is not FamilyAAdmissionReceiptV2:
        raise ProspectivePositionTerminalContractErrorV2(
            "admission must be an exact FamilyAAdmissionReceiptV2"
        )
    if type(terminal_exit) is not FamilyAExitMutationReceiptV2:
        raise ProspectivePositionTerminalContractErrorV2(
            "terminal_exit must be an exact FamilyAExitMutationReceiptV2"
        )
    canonical_paper_fok_full_fill_certificate_v2(admission.certificate)
    canonical_family_a_exit_decision_v2(terminal_exit.decision)
    if (
        terminal_exit.entry_event_id != admission.entry_event_id
        or terminal_exit.item.position != admission.position
        or not terminal_exit.decision.exits_position
        or not terminal_exit.post_terminal
        or terminal_exit.post_active
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "Family A exit receipt is not the terminal exit for the admitted position"
        )
    side = (
        MandatoryExitPositionSideV2.LONG
        if admission.position.side.value == "LONG"
        else MandatoryExitPositionSideV2.SHORT
    )
    admission_sha256 = _hash_document(
        _FAMILY_A_ADMISSION_REFERENCE_DOMAIN,
        _family_a_admission_receipt_document(admission),
    )
    exit_sha256 = _hash_document(
        _FAMILY_A_EXIT_REFERENCE_DOMAIN,
        _family_a_exit_receipt_document(terminal_exit),
    )
    return FamilyLifecycleEvidenceReferenceV2(
        family=PromotingFamilyV2.A,
        attempt_id=admission.position.attempt_id,
        promoting_plan_sha256=admission.position.promoting_plan_sha256,
        symbol=admission.position.symbol,
        position_id=position_id,
        side=side,
        entry_event_id=admission.entry_event_id,
        full_fill_certificate_sha256=admission.certificate_sha256,
        admission_receipt_sha256=admission_sha256,
        admission_input_sha256=admission.input_sha256,
        admission_pre_root_sha256=admission.pre_root_sha256,
        admission_pre_event_count=admission.pre_event_count,
        admission_post_root_sha256=admission.post_root_sha256,
        admission_post_event_count=admission.post_event_count,
        admission_disposition=admission.disposition.value,
        exit_event_id=terminal_exit.exit_event_id,
        exit_receipt_sha256=exit_sha256,
        exit_input_sha256=terminal_exit.input_sha256,
        exit_pre_root_sha256=terminal_exit.pre_root_sha256,
        exit_pre_event_count=terminal_exit.pre_event_count,
        exit_post_root_sha256=terminal_exit.post_root_sha256,
        exit_post_event_count=terminal_exit.post_event_count,
        exit_disposition=terminal_exit.disposition.value,
        exit_decision_cutoff_ms=terminal_exit.decision.decision_cutoff_ms,
        terminal_exit=True,
        source_authority=(
            PositionEvidenceReferenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_A_RECEIPTS
        ),
        _factory_token=_REFERENCE_FACTORY_TOKEN,
    )


def build_family_b_lifecycle_evidence_reference_v2(
    *,
    position_id: str,
    admission: FamilyBAdmissionReceiptV2,
    terminal_exit: FamilyBExitMutationReceiptV2,
) -> FamilyLifecycleEvidenceReferenceV2:
    """Project exact live Family B admission and terminal-exit receipts."""

    _require_sha256(position_id, "position_id")
    if type(admission) is not FamilyBAdmissionReceiptV2:
        raise ProspectivePositionTerminalContractErrorV2(
            "admission must be an exact FamilyBAdmissionReceiptV2"
        )
    if type(terminal_exit) is not FamilyBExitMutationReceiptV2:
        raise ProspectivePositionTerminalContractErrorV2(
            "terminal_exit must be an exact FamilyBExitMutationReceiptV2"
        )
    canonical_family_b_entry_decision_v2(admission.decision)
    canonical_paper_fok_entry_decision_v2(admission.paper_decision)
    canonical_paper_fok_full_fill_certificate_v2(admission.paper_certificate)
    canonical_family_b_exit_decision_v2(terminal_exit.decision)
    position = admission.position
    if (
        terminal_exit.position != position
        or terminal_exit.position_sha256 != admission.position_sha256
        or terminal_exit.entry_event_id != admission.decision.event_id
        or terminal_exit.decision.entry_event_id != admission.decision.event_id
        or not terminal_exit.decision.exits_position
        or not terminal_exit.post_terminal
        or terminal_exit.post_active_entry_event_id is not None
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "Family B exit receipt is not the terminal exit for the admitted position"
        )
    if (
        (
            admission.decision.attempt_id,
            admission.decision.promoting_plan_sha256,
            admission.decision.symbol,
            admission.decision.venue,
            admission.decision.event_id,
        )
        != (
            position.attempt_id,
            position.promoting_plan_sha256,
            position.symbol,
            position.venue,
            position.entry_event_id,
        )
        or (
            admission.paper_decision.attempt_id,
            admission.paper_decision.promoting_plan_sha256,
            admission.paper_decision.symbol,
            admission.paper_decision.venue,
            admission.paper_decision.signal_event_id,
            admission.paper_decision.event_id,
            admission.paper_decision.payload_sha256,
        )
        != (
            position.attempt_id,
            position.promoting_plan_sha256,
            position.symbol,
            position.venue,
            position.entry_event_id,
            position.paper_decision_event_id,
            position.paper_decision_payload_sha256,
        )
        or (
            admission.paper_certificate.attempt_id,
            admission.paper_certificate.promoting_plan_sha256,
            admission.paper_certificate.symbol,
            admission.paper_certificate.venue,
            admission.paper_certificate.signal_event_id,
            admission.paper_certificate.decision_event_id,
            admission.paper_certificate.decision_payload_sha256,
            admission.paper_certificate.certificate_sha256,
        )
        != (
            position.attempt_id,
            position.promoting_plan_sha256,
            position.symbol,
            position.venue,
            position.entry_event_id,
            position.paper_decision_event_id,
            position.paper_decision_payload_sha256,
            position.admission_evidence_sha256,
        )
        or (
            terminal_exit.decision.attempt_id,
            terminal_exit.decision.promoting_plan_sha256,
            terminal_exit.decision.symbol,
            terminal_exit.decision.venue,
        )
        != (
            position.attempt_id,
            position.promoting_plan_sha256,
            position.symbol,
            position.venue,
        )
        or not (
            admission.paper_registry_root_sha256
            == position.paper_registry_root_sha256
            == admission.paper_certificate.terminal_registry_replay_root_sha256
        )
        or not (
            admission.paper_registry_event_count
            == position.paper_registry_event_count
            == admission.paper_certificate.terminal_registry_event_count
        )
        or (
            admission.paper_registry_maximum_events
            != admission.paper_certificate.terminal_registry_maximum_events
        )
        or not (
            admission.paper_registry_checkpoint_sha256
            == position.paper_registry_checkpoint_sha256
            == admission.paper_certificate.terminal_registry_checkpoint_sha256
        )
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "Family B admission or exit receipt identity differs"
        )
    side = (
        MandatoryExitPositionSideV2.LONG
        if position.side.value == "LONG"
        else MandatoryExitPositionSideV2.SHORT
    )
    return _build_factory_family_reference(
        family=PromotingFamilyV2.B,
        attempt_id=position.attempt_id,
        promoting_plan_sha256=position.promoting_plan_sha256,
        symbol=position.symbol,
        position_id=position_id,
        side=side,
        entry_event_id=position.entry_event_id,
        full_fill_certificate_sha256=admission.paper_certificate.certificate_sha256,
        admission_receipt_sha256=_hash_document(
            _FAMILY_B_ADMISSION_REFERENCE_DOMAIN,
            _family_b_admission_receipt_document(admission),
        ),
        admission_input_sha256=admission.input_sha256,
        admission_pre_root_sha256=admission.pre_root_sha256,
        admission_pre_event_count=admission.pre_event_count,
        admission_post_root_sha256=admission.post_root_sha256,
        admission_post_event_count=admission.post_event_count,
        admission_disposition=admission.disposition.value,
        exit_event_id=terminal_exit.decision.event_id,
        exit_receipt_sha256=_hash_document(
            _FAMILY_B_EXIT_REFERENCE_DOMAIN,
            _family_b_exit_receipt_document(terminal_exit),
        ),
        exit_input_sha256=terminal_exit.input_sha256,
        exit_pre_root_sha256=terminal_exit.pre_root_sha256,
        exit_pre_event_count=terminal_exit.pre_event_count,
        exit_post_root_sha256=terminal_exit.post_root_sha256,
        exit_post_event_count=terminal_exit.post_event_count,
        exit_disposition=terminal_exit.disposition.value,
        exit_decision_cutoff_ms=terminal_exit.decision.decision_cutoff_ms,
        source_authority=(
            PositionEvidenceReferenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_B_RECEIPTS
        ),
    )


def build_family_c_lifecycle_evidence_reference_v2(
    *,
    position_id: str,
    admission: FamilyCAdmissionReceiptV2,
    terminal_exit: FamilyCExitMutationReceiptV2,
) -> FamilyLifecycleEvidenceReferenceV2:
    """Project exact live Family C admission and terminal-exit receipts."""

    _require_sha256(position_id, "position_id")
    if type(admission) is not FamilyCAdmissionReceiptV2:
        raise ProspectivePositionTerminalContractErrorV2(
            "admission must be an exact FamilyCAdmissionReceiptV2"
        )
    if type(terminal_exit) is not FamilyCExitMutationReceiptV2:
        raise ProspectivePositionTerminalContractErrorV2(
            "terminal_exit must be an exact FamilyCExitMutationReceiptV2"
        )
    canonical_family_c_entry_decision_v2(admission.decision)
    canonical_paper_fok_entry_decision_v2(admission.paper_decision)
    canonical_paper_fok_full_fill_certificate_v2(admission.paper_certificate)
    canonical_family_c_exit_decision_v2(terminal_exit.decision)
    position = admission.position
    if (
        terminal_exit.position != position
        or terminal_exit.position_sha256 != admission.position_sha256
        or terminal_exit.entry_event_id != admission.decision.event_id
        or terminal_exit.decision.entry_event_id != admission.decision.event_id
        or not terminal_exit.decision.exits_position
        or not terminal_exit.post_terminal
        or terminal_exit.post_active_entry_event_id is not None
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "Family C exit receipt is not the terminal exit for the admitted position"
        )
    if (
        (
            admission.decision.attempt_id,
            admission.decision.promoting_plan_sha256,
            admission.decision.symbol,
            admission.decision.venue,
            admission.decision.event_id,
        )
        != (
            position.attempt_id,
            position.promoting_plan_sha256,
            position.symbol,
            position.venue,
            position.entry_event_id,
        )
        or (
            admission.paper_decision.attempt_id,
            admission.paper_decision.promoting_plan_sha256,
            admission.paper_decision.symbol,
            admission.paper_decision.venue,
            admission.paper_decision.signal_event_id,
            admission.paper_decision.event_id,
            admission.paper_decision.payload_sha256,
        )
        != (
            position.attempt_id,
            position.promoting_plan_sha256,
            position.symbol,
            position.venue,
            position.entry_event_id,
            position.paper_decision_event_id,
            position.paper_decision_payload_sha256,
        )
        or (
            admission.paper_certificate.attempt_id,
            admission.paper_certificate.promoting_plan_sha256,
            admission.paper_certificate.symbol,
            admission.paper_certificate.venue,
            admission.paper_certificate.signal_event_id,
            admission.paper_certificate.decision_event_id,
            admission.paper_certificate.decision_payload_sha256,
            admission.paper_certificate.certificate_sha256,
        )
        != (
            position.attempt_id,
            position.promoting_plan_sha256,
            position.symbol,
            position.venue,
            position.entry_event_id,
            position.paper_decision_event_id,
            position.paper_decision_payload_sha256,
            position.admission_evidence_sha256,
        )
        or (
            terminal_exit.decision.attempt_id,
            terminal_exit.decision.promoting_plan_sha256,
            terminal_exit.decision.symbol,
            terminal_exit.decision.venue,
        )
        != (
            position.attempt_id,
            position.promoting_plan_sha256,
            position.symbol,
            position.venue,
        )
        or not (
            admission.paper_registry_root_sha256
            == position.paper_registry_root_sha256
            == admission.paper_certificate.terminal_registry_replay_root_sha256
        )
        or not (
            admission.paper_registry_event_count
            == position.paper_registry_event_count
            == admission.paper_certificate.terminal_registry_event_count
        )
        or (
            admission.paper_registry_maximum_events
            != admission.paper_certificate.terminal_registry_maximum_events
        )
        or not (
            admission.paper_registry_checkpoint_sha256
            == position.paper_registry_checkpoint_sha256
            == admission.paper_certificate.terminal_registry_checkpoint_sha256
        )
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "Family C admission or exit receipt identity differs"
        )
    side = (
        MandatoryExitPositionSideV2.LONG
        if position.side.value == "LONG"
        else MandatoryExitPositionSideV2.SHORT
    )
    return _build_factory_family_reference(
        family=PromotingFamilyV2.C,
        attempt_id=position.attempt_id,
        promoting_plan_sha256=position.promoting_plan_sha256,
        symbol=position.symbol,
        position_id=position_id,
        side=side,
        entry_event_id=position.entry_event_id,
        full_fill_certificate_sha256=admission.paper_certificate.certificate_sha256,
        admission_receipt_sha256=_hash_document(
            _FAMILY_C_ADMISSION_REFERENCE_DOMAIN,
            _family_c_admission_receipt_document(admission),
        ),
        admission_input_sha256=admission.input_sha256,
        admission_pre_root_sha256=admission.pre_root_sha256,
        admission_pre_event_count=admission.pre_event_count,
        admission_post_root_sha256=admission.post_root_sha256,
        admission_post_event_count=admission.post_event_count,
        admission_disposition=admission.disposition.value,
        exit_event_id=terminal_exit.decision.event_id,
        exit_receipt_sha256=_hash_document(
            _FAMILY_C_EXIT_REFERENCE_DOMAIN,
            _family_c_exit_receipt_document(terminal_exit),
        ),
        exit_input_sha256=terminal_exit.input_sha256,
        exit_pre_root_sha256=terminal_exit.pre_root_sha256,
        exit_pre_event_count=terminal_exit.pre_event_count,
        exit_post_root_sha256=terminal_exit.post_root_sha256,
        exit_post_event_count=terminal_exit.post_event_count,
        exit_disposition=terminal_exit.disposition.value,
        exit_decision_cutoff_ms=terminal_exit.decision.decision_cutoff_ms,
        source_authority=(
            PositionEvidenceReferenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_C_RECEIPTS
        ),
    )


def _build_factory_family_reference(
    *,
    family: PromotingFamilyV2,
    attempt_id: str,
    promoting_plan_sha256: str,
    symbol: str,
    position_id: str,
    side: MandatoryExitPositionSideV2,
    entry_event_id: str,
    full_fill_certificate_sha256: str,
    admission_receipt_sha256: str,
    admission_input_sha256: str,
    admission_pre_root_sha256: str,
    admission_pre_event_count: int,
    admission_post_root_sha256: str,
    admission_post_event_count: int,
    admission_disposition: str,
    exit_event_id: str,
    exit_receipt_sha256: str,
    exit_input_sha256: str,
    exit_pre_root_sha256: str,
    exit_pre_event_count: int,
    exit_post_root_sha256: str,
    exit_post_event_count: int,
    exit_disposition: str,
    exit_decision_cutoff_ms: int,
    source_authority: PositionEvidenceReferenceAuthorityV2,
) -> FamilyLifecycleEvidenceReferenceV2:
    return FamilyLifecycleEvidenceReferenceV2(
        family=family,
        attempt_id=attempt_id,
        promoting_plan_sha256=promoting_plan_sha256,
        symbol=symbol,
        position_id=position_id,
        side=side,
        entry_event_id=entry_event_id,
        full_fill_certificate_sha256=full_fill_certificate_sha256,
        admission_receipt_sha256=admission_receipt_sha256,
        admission_input_sha256=admission_input_sha256,
        admission_pre_root_sha256=admission_pre_root_sha256,
        admission_pre_event_count=admission_pre_event_count,
        admission_post_root_sha256=admission_post_root_sha256,
        admission_post_event_count=admission_post_event_count,
        admission_disposition=admission_disposition,
        exit_event_id=exit_event_id,
        exit_receipt_sha256=exit_receipt_sha256,
        exit_input_sha256=exit_input_sha256,
        exit_pre_root_sha256=exit_pre_root_sha256,
        exit_pre_event_count=exit_pre_event_count,
        exit_post_root_sha256=exit_post_root_sha256,
        exit_post_event_count=exit_post_event_count,
        exit_disposition=exit_disposition,
        exit_decision_cutoff_ms=exit_decision_cutoff_ms,
        terminal_exit=True,
        source_authority=source_authority,
        _factory_token=_REFERENCE_FACTORY_TOKEN,
    )


def build_mandatory_exit_evidence_reference_v2(
    *,
    attempt_id: str,
    promoting_plan_sha256: str,
    symbol: str,
    position_id: str,
    family_exit_event_id: str,
    side: MandatoryExitPositionSideV2,
    mandatory_position_sha256: str,
    exit_intent_sha256: str,
    target_cursor_sha256: str,
    terminal_sha256: str,
    terminal_payload_sha256: str,
    fee_certificate_sha256: str,
    ledger_checkpoint_sha256: str,
    exit_slices_root_sha256: str,
    exit_slice_count: int,
    filled_quantity: Decimal,
    residual_quantity: Decimal,
    gross_exit_notional_usdt: Decimal,
    signed_exit_cashflow_usdt: Decimal,
    terminal_at_ms: int,
) -> MandatoryExitEvidenceReferenceV2:
    """Build a bounded full-exit reference; no book rows are embedded."""

    return MandatoryExitEvidenceReferenceV2(
        attempt_id=attempt_id,
        promoting_plan_sha256=promoting_plan_sha256,
        symbol=symbol,
        position_id=position_id,
        family_exit_event_id=family_exit_event_id,
        side=side,
        mandatory_position_sha256=mandatory_position_sha256,
        exit_intent_sha256=exit_intent_sha256,
        target_cursor_sha256=target_cursor_sha256,
        terminal_sha256=terminal_sha256,
        terminal_payload_sha256=terminal_payload_sha256,
        fee_certificate_sha256=fee_certificate_sha256,
        ledger_checkpoint_sha256=ledger_checkpoint_sha256,
        exit_slices_root_sha256=exit_slices_root_sha256,
        exit_slice_count=exit_slice_count,
        filled_quantity=filled_quantity,
        residual_quantity=residual_quantity,
        gross_exit_notional_usdt=gross_exit_notional_usdt,
        signed_exit_cashflow_usdt=signed_exit_cashflow_usdt,
        terminal_at_ms=terminal_at_ms,
        terminal_status=MandatoryExitTerminalStatusV2.EXITED_FULL,
        source_authority=PositionEvidenceReferenceAuthorityV2.CALLER_HASH_REFERENCE,
        _factory_token=_REFERENCE_FACTORY_TOKEN,
    )


def build_final_fee_evidence_reference_v2(
    *,
    attempt_id: str,
    promoting_plan_sha256: str,
    symbol: str,
    position_id: str,
    mandatory_exit_fee_certificate_sha256: str,
    final_timeline_checkpoint_sha256: str,
    final_timeline_root_sha256: str,
    fee_position_payload_sha256: str,
    exit_slices_root_sha256: str,
    exit_slice_count: int,
    multiplier: FeeMultiplierV2,
    entry_fee_usdt: Decimal,
    exit_fee_usdt: Decimal,
    total_fee_usdt: Decimal,
) -> FinalFeeEvidenceReferenceV2:
    """Build an exact complete public-fee reference."""

    return FinalFeeEvidenceReferenceV2(
        attempt_id=attempt_id,
        promoting_plan_sha256=promoting_plan_sha256,
        symbol=symbol,
        position_id=position_id,
        mandatory_exit_fee_certificate_sha256=(mandatory_exit_fee_certificate_sha256),
        final_timeline_checkpoint_sha256=final_timeline_checkpoint_sha256,
        final_timeline_root_sha256=final_timeline_root_sha256,
        fee_position_payload_sha256=fee_position_payload_sha256,
        exit_slices_root_sha256=exit_slices_root_sha256,
        exit_slice_count=exit_slice_count,
        multiplier=multiplier,
        entry_fee_usdt=entry_fee_usdt,
        exit_fee_usdt=exit_fee_usdt,
        total_fee_usdt=total_fee_usdt,
        status=FilledPositionFeeStatusV2.BOTH_LEGS_COMPLETE,
        source_authority=PositionEvidenceReferenceAuthorityV2.CALLER_HASH_REFERENCE,
        _factory_token=_REFERENCE_FACTORY_TOKEN,
    )


def build_funding_census_evidence_reference_v2(
    *,
    attempt_id: str,
    promoting_plan_sha256: str,
    symbol: str,
    position_id: str,
    census_certificate_sha256: str,
    registry_checkpoint_sha256: str,
    position_ledger_checkpoint_sha256: str,
    cashflow_root_sha256: str,
    expected_funding_count: int,
    confirmed_funding_count: int,
    cashflow_event_count: int,
    interval_start_ms: int,
    interval_end_ms: int,
    observed_through_ms: int,
    realized_funding_cashflow_usdt: Decimal,
) -> FundingCensusEvidenceReferenceV2:
    """Build a complete funding-census reference, including zero-event intervals."""

    return FundingCensusEvidenceReferenceV2(
        attempt_id=attempt_id,
        promoting_plan_sha256=promoting_plan_sha256,
        symbol=symbol,
        position_id=position_id,
        census_certificate_sha256=census_certificate_sha256,
        registry_checkpoint_sha256=registry_checkpoint_sha256,
        position_ledger_checkpoint_sha256=position_ledger_checkpoint_sha256,
        cashflow_root_sha256=cashflow_root_sha256,
        expected_funding_count=expected_funding_count,
        confirmed_funding_count=confirmed_funding_count,
        cashflow_event_count=cashflow_event_count,
        interval_start_ms=interval_start_ms,
        interval_end_ms=interval_end_ms,
        observed_through_ms=observed_through_ms,
        realized_funding_cashflow_usdt=realized_funding_cashflow_usdt,
        boundary_convention=(
            FundingCensusBoundaryConventionV2.Q_BEFORE_FUNDING_EQUAL_MS_ADVERSE_ONLY
        ),
        source_authority=PositionEvidenceReferenceAuthorityV2.CALLER_HASH_REFERENCE,
        _factory_token=_REFERENCE_FACTORY_TOKEN,
    )


def build_prospective_position_terminal_payload_v2(
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
    sizing_cell: PaperSizingCellV2,
    paper_terminal: ProspectivePaperTerminalPayloadV2,
    finalized_at_ms: int,
    paper_terminal_record_sha256: str | None = None,
    full_fill_certificate: PaperFokFullFillCertificateV2 | None = None,
    family_evidence: FamilyLifecycleEvidenceReferenceV2 | None = None,
    mandatory_exit_evidence: MandatoryExitEvidenceReferenceV2 | None = None,
    fee_evidence: FinalFeeEvidenceReferenceV2 | None = None,
    funding_evidence: FundingCensusEvidenceReferenceV2 | None = None,
) -> ProspectivePositionTerminalPayloadV2:
    """Build one fail-closed position outcome from exact bounded sources."""

    exact_cell = _exact_current_plan_cell(plan, cell)
    if not isinstance(sizing_cell, PaperSizingCellV2):
        raise ProspectivePositionTerminalContractErrorV2("sizing_cell must be PaperSizingCellV2")
    paper_jsonl = _validate_paper_terminal_binding(
        plan=plan,
        cell=exact_cell,
        sizing_cell=sizing_cell,
        paper_terminal=paper_terminal,
    )
    _safe_nonnegative_integer(finalized_at_ms, "finalized_at_ms")
    if finalized_at_ms < paper_terminal.decision_cutoff_ms:
        raise ProspectivePositionTerminalContractErrorV2(
            "position terminal finalization predates the decision cutoff"
        )
    if paper_terminal_record_sha256 is not None:
        _require_sha256(paper_terminal_record_sha256, "paper_terminal_record_sha256")
    outcome_id = prospective_outcome_id_v2(
        attempt_plan_sha256=plan.plan_sha256,
        origin_segment_id=exact_cell.segment_id,
        origin_cell_id=exact_cell.cell_id,
        sizing_cell=sizing_cell,
    )
    common: _CommonTerminalFieldsV2 = {
        "attempt_id": exact_cell.attempt_id,
        "attempt_plan_sha256": plan.plan_sha256,
        "promoting_plan_sha256": plan.promoting_plan_sha256,
        "execution_contract_sha256": plan.execution_contract_sha256,
        "origin_segment_id": exact_cell.segment_id,
        "origin_cell_id": exact_cell.cell_id,
        "sizing_cell": sizing_cell,
        "family": exact_cell.family,
        "family_rule_version": exact_cell.rule_version,
        "symbol": exact_cell.symbol,
        "venue": VenueV2.USDM_FUTURES,
        "bar_open_ms": exact_cell.bar_open_ms,
        "bar_close_ms": exact_cell.bar_close_ms,
        "decision_cutoff_ms": exact_cell.decision_cutoff_ms,
        "outcome_id": outcome_id,
        "finalized_at_ms": finalized_at_ms,
        "paper_terminal_payload_sha256": paper_terminal.payload_sha256,
        "paper_terminal_jsonl_sha256": hashlib.sha256(paper_jsonl).hexdigest(),
        "paper_terminal_record_sha256": paper_terminal_record_sha256,
        "diagnostic_entry_slippage_usdt": (paper_terminal.signed_slippage_vs_reference_usdt),
    }
    if paper_terminal.terminal_status is not (
        ProspectivePaperTerminalStatusV2.PAPER_EXECUTED_FULL_QUANTITY
    ):
        return _build_no_position_terminal(
            common=common,
            paper_terminal=paper_terminal,
            full_fill_certificate=full_fill_certificate,
            family_evidence=family_evidence,
            mandatory_exit_evidence=mandatory_exit_evidence,
            fee_evidence=fee_evidence,
            funding_evidence=funding_evidence,
        )
    return _build_opened_position_terminal(
        common=common,
        paper_terminal=paper_terminal,
        full_fill_certificate=full_fill_certificate,
        family_evidence=family_evidence,
        mandatory_exit_evidence=mandatory_exit_evidence,
        fee_evidence=fee_evidence,
        funding_evidence=funding_evidence,
    )


def canonical_prospective_position_terminal_payload_v2(
    payload: ProspectivePositionTerminalPayloadV2,
) -> bytes:
    """Serialize one factory-sealed terminal and enforce the 64 KiB bound."""

    if type(payload) is not ProspectivePositionTerminalPayloadV2:
        raise TypeError("payload must be exact ProspectivePositionTerminalPayloadV2")
    _validate_payload_shape(payload)
    expected = _hash_document(_PAYLOAD_HASH_DOMAIN, _payload_document(payload))
    if payload.payload_sha256 != expected:
        raise ProspectivePositionTerminalContractErrorV2(
            "position terminal payload hash differs from canonical content"
        )
    encoded = canonical_json_line(
        {**_payload_document(payload), "payload_sha256": payload.payload_sha256}
    )
    if len(encoded) > MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2:
        raise ProspectivePositionTerminalContractErrorV2(
            "position terminal exceeds the fixed 64 KiB payload bound"
        )
    return encoded


def parse_prospective_position_terminal_payload_v2(
    encoded: bytes,
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
    sizing_cell: PaperSizingCellV2,
    paper_terminal: ProspectivePaperTerminalPayloadV2,
    finalized_at_ms: int,
    paper_terminal_record_sha256: str | None = None,
    full_fill_certificate: PaperFokFullFillCertificateV2 | None = None,
    family_evidence: FamilyLifecycleEvidenceReferenceV2 | None = None,
    mandatory_exit_evidence: MandatoryExitEvidenceReferenceV2 | None = None,
    fee_evidence: FinalFeeEvidenceReferenceV2 | None = None,
    funding_evidence: FundingCensusEvidenceReferenceV2 | None = None,
) -> ProspectivePositionTerminalPayloadV2:
    """Rebuild from exact sources and require byte-for-byte equality."""

    _decode_exact_payload(encoded)
    expected = build_prospective_position_terminal_payload_v2(
        plan=plan,
        cell=cell,
        sizing_cell=sizing_cell,
        paper_terminal=paper_terminal,
        finalized_at_ms=finalized_at_ms,
        paper_terminal_record_sha256=paper_terminal_record_sha256,
        full_fill_certificate=full_fill_certificate,
        family_evidence=family_evidence,
        mandatory_exit_evidence=mandatory_exit_evidence,
        fee_evidence=fee_evidence,
        funding_evidence=funding_evidence,
    )
    if canonical_prospective_position_terminal_payload_v2(expected) != encoded:
        raise ProspectivePositionTerminalContractErrorV2(
            "stored position terminal differs from its exact typed sources"
        )
    return expected


def _build_no_position_terminal(
    *,
    common: _CommonTerminalFieldsV2,
    paper_terminal: ProspectivePaperTerminalPayloadV2,
    full_fill_certificate: PaperFokFullFillCertificateV2 | None,
    family_evidence: FamilyLifecycleEvidenceReferenceV2 | None,
    mandatory_exit_evidence: MandatoryExitEvidenceReferenceV2 | None,
    fee_evidence: FinalFeeEvidenceReferenceV2 | None,
    funding_evidence: FundingCensusEvidenceReferenceV2 | None,
) -> ProspectivePositionTerminalPayloadV2:
    if any(
        value is not None
        for value in (
            full_fill_certificate,
            family_evidence,
            mandatory_exit_evidence,
            fee_evidence,
            funding_evidence,
        )
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "a no-position PAPER terminal forbids position lifecycle evidence"
        )
    incomplete = paper_terminal.completeness is ProspectivePaperTerminalCompletenessV2.INCOMPLETE
    status = (
        ProspectivePositionTerminalStatusV2.INCOMPLETE
        if incomplete
        else ProspectivePositionTerminalStatusV2.SUPPRESSED_NO_POSITION
    )
    zero = None if incomplete else Decimal(0)
    return ProspectivePositionTerminalPayloadV2(
        **common,
        position_id=None,
        position_side=None,
        position_opened=False,
        terminal_status=status,
        reasons=(
            ("UPSTREAM_PAPER_TERMINAL_INCOMPLETE",) if incomplete else ("NO_PAPER_POSITION_OPENED",)
        ),
        invalidation=(
            "INCOMPLETE_UPSTREAM_ENTRY_EVIDENCE"
            if incomplete
            else "NO_POSITION_EXISTS_FOR_EXIT_OR_PNL"
        ),
        paper_full_fill_certificate_sha256=None,
        paper_full_fill_certificate_jsonl_sha256=None,
        family_evidence=None,
        mandatory_exit_evidence=None,
        fee_evidence=None,
        funding_evidence=None,
        entry_quantity=None,
        entry_executable_vwap=None,
        entry_executable_notional_usdt=None,
        signed_entry_cashflow_usdt=zero,
        exit_filled_quantity=None,
        gross_exit_notional_usdt=None,
        signed_exit_cashflow_usdt=zero,
        gross_pnl_usdt=zero,
        entry_fee_usdt=zero,
        exit_fee_usdt=zero,
        total_fee_usdt=zero,
        realized_funding_cashflow_usdt=zero,
        after_cost_pnl_usdt=zero,
        pnl_denominator_usdt=None,
        after_cost_return=None,
        costs_complete=not incomplete,
        arithmetic_complete=not incomplete,
        _factory_token=_PAYLOAD_FACTORY_TOKEN,
    )


def _build_opened_position_terminal(
    *,
    common: _CommonTerminalFieldsV2,
    paper_terminal: ProspectivePaperTerminalPayloadV2,
    full_fill_certificate: PaperFokFullFillCertificateV2 | None,
    family_evidence: FamilyLifecycleEvidenceReferenceV2 | None,
    mandatory_exit_evidence: MandatoryExitEvidenceReferenceV2 | None,
    fee_evidence: FinalFeeEvidenceReferenceV2 | None,
    funding_evidence: FundingCensusEvidenceReferenceV2 | None,
) -> ProspectivePositionTerminalPayloadV2:
    certificate_sha256 = paper_terminal.full_fill_certificate_sha256
    if certificate_sha256 is None:
        raise ProspectivePositionTerminalContractErrorV2(
            "full PAPER terminal lacks a full-fill certificate hash"
        )
    outcome_id = common["outcome_id"]
    position_id = prospective_position_id_v2(
        outcome_id=outcome_id,
        certificate_sha256=certificate_sha256,
    )
    side = _paper_terminal_position_side(paper_terminal)
    certificate_jsonl = _validate_full_fill_certificate(
        paper_terminal,
        full_fill_certificate,
    )
    _validate_reference_join(
        common=common,
        paper_terminal=paper_terminal,
        position_id=position_id,
        side=side,
        certificate_sha256=certificate_sha256,
        family_evidence=family_evidence,
        mandatory_exit_evidence=mandatory_exit_evidence,
        fee_evidence=fee_evidence,
        funding_evidence=funding_evidence,
    )
    required = (
        ("MISSING_DURABLE_PAPER_TERMINAL_RECORD", common["paper_terminal_record_sha256"]),
        ("MISSING_TYPED_FULL_FILL_CERTIFICATE", full_fill_certificate),
        ("MISSING_FAMILY_LIFECYCLE_EVIDENCE", family_evidence),
        ("MISSING_MANDATORY_EXIT_EVIDENCE", mandatory_exit_evidence),
        ("MISSING_FINAL_FEE_EVIDENCE", fee_evidence),
        ("MISSING_FUNDING_CENSUS_EVIDENCE", funding_evidence),
    )
    missing = tuple(reason for reason, value in required if value is None)
    entry_quantity = paper_terminal.filled_quantity
    entry_vwap = paper_terminal.executable_vwap
    entry_notional = paper_terminal.executable_notional
    if entry_quantity is None or entry_vwap is None or entry_notional is None:
        raise ProspectivePositionTerminalContractErrorV2(
            "full PAPER terminal lacks exact entry execution values"
        )
    signed_entry = entry_notional if side is MandatoryExitPositionSideV2.SHORT else -entry_notional
    if missing:
        return ProspectivePositionTerminalPayloadV2(
            **common,
            position_id=position_id,
            position_side=side,
            position_opened=True,
            terminal_status=ProspectivePositionTerminalStatusV2.INCOMPLETE,
            reasons=missing,
            invalidation="INCOMPLETE_POSITION_EVIDENCE_NOT_EFFICACY_ELIGIBLE",
            paper_full_fill_certificate_sha256=certificate_sha256,
            paper_full_fill_certificate_jsonl_sha256=(
                None if certificate_jsonl is None else hashlib.sha256(certificate_jsonl).hexdigest()
            ),
            family_evidence=family_evidence,
            mandatory_exit_evidence=mandatory_exit_evidence,
            fee_evidence=fee_evidence,
            funding_evidence=funding_evidence,
            entry_quantity=entry_quantity,
            entry_executable_vwap=entry_vwap,
            entry_executable_notional_usdt=entry_notional,
            signed_entry_cashflow_usdt=signed_entry,
            exit_filled_quantity=None,
            gross_exit_notional_usdt=None,
            signed_exit_cashflow_usdt=None,
            gross_pnl_usdt=None,
            entry_fee_usdt=None,
            exit_fee_usdt=None,
            total_fee_usdt=None,
            realized_funding_cashflow_usdt=None,
            after_cost_pnl_usdt=None,
            pnl_denominator_usdt=None,
            after_cost_return=None,
            costs_complete=False,
            arithmetic_complete=False,
            _factory_token=_PAYLOAD_FACTORY_TOKEN,
        )
    assert certificate_jsonl is not None
    assert mandatory_exit_evidence is not None
    assert fee_evidence is not None
    assert funding_evidence is not None
    gross_pnl = _exact_sum(
        signed_entry,
        mandatory_exit_evidence.signed_exit_cashflow_usdt,
    )
    after_cost = _exact_sum(
        gross_pnl,
        funding_evidence.realized_funding_cashflow_usdt,
        -fee_evidence.total_fee_usdt,
    )
    with localcontext(protocol_decimal_context_v2()):
        after_cost_return = after_cost / entry_notional
    return ProspectivePositionTerminalPayloadV2(
        **common,
        position_id=position_id,
        position_side=side,
        position_opened=True,
        terminal_status=ProspectivePositionTerminalStatusV2.COMPLETE_CALCULATION,
        reasons=(
            "COMPLETE_REFERENCED_POSITION_CASHFLOW_CALCULATION",
            "NONAUTHORITATIVE_UNTIL_ATTEMPT_WAL_REPLAY_AND_PLAN_BINDING_EXIST",
        ),
        invalidation="INVALID_IF_ANY_REFERENCED_HASH_CHECKPOINT_OR_CENSUS_DIFFERS",
        paper_full_fill_certificate_sha256=certificate_sha256,
        paper_full_fill_certificate_jsonl_sha256=hashlib.sha256(certificate_jsonl).hexdigest(),
        family_evidence=family_evidence,
        mandatory_exit_evidence=mandatory_exit_evidence,
        fee_evidence=fee_evidence,
        funding_evidence=funding_evidence,
        entry_quantity=entry_quantity,
        entry_executable_vwap=entry_vwap,
        entry_executable_notional_usdt=entry_notional,
        signed_entry_cashflow_usdt=signed_entry,
        exit_filled_quantity=mandatory_exit_evidence.filled_quantity,
        gross_exit_notional_usdt=mandatory_exit_evidence.gross_exit_notional_usdt,
        signed_exit_cashflow_usdt=(mandatory_exit_evidence.signed_exit_cashflow_usdt),
        gross_pnl_usdt=gross_pnl,
        entry_fee_usdt=fee_evidence.entry_fee_usdt,
        exit_fee_usdt=fee_evidence.exit_fee_usdt,
        total_fee_usdt=fee_evidence.total_fee_usdt,
        realized_funding_cashflow_usdt=(funding_evidence.realized_funding_cashflow_usdt),
        after_cost_pnl_usdt=after_cost,
        pnl_denominator_usdt=entry_notional,
        after_cost_return=after_cost_return,
        costs_complete=True,
        arithmetic_complete=True,
        _factory_token=_PAYLOAD_FACTORY_TOKEN,
    )


def _exact_current_plan_cell(
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
) -> ProspectiveExpectedCellV2:
    canonical_prospective_census_plan_v2(plan)
    if plan.execution_contract_sha256 != current_prospective_execution_contract_sha256_v2():
        raise ProspectivePositionTerminalContractErrorV2(
            "plan does not bind the current prospective execution contract"
        )
    if type(cell) is not ProspectiveExpectedCellV2:
        raise ProspectivePositionTerminalContractErrorV2(
            "cell must be exact ProspectiveExpectedCellV2"
        )
    if cell.attempt_plan_sha256 != plan.plan_sha256:
        raise ProspectivePositionTerminalContractErrorV2("cell targets a foreign prospective plan")
    try:
        exact = plan.expected_cell(
            family=cell.family,
            symbol=cell.symbol,
            bar_open_ms=cell.bar_open_ms,
        )
    except ValueError as error:
        raise ProspectivePositionTerminalContractErrorV2(
            "cell is outside the frozen prospective census"
        ) from error
    if exact != cell:
        raise ProspectivePositionTerminalContractErrorV2(
            "cell differs from its exact frozen census identity"
        )
    return exact


def _validate_paper_terminal_binding(
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
    sizing_cell: PaperSizingCellV2,
    paper_terminal: ProspectivePaperTerminalPayloadV2,
) -> bytes:
    if type(paper_terminal) is not ProspectivePaperTerminalPayloadV2:
        raise ProspectivePositionTerminalContractErrorV2(
            "paper_terminal must be exact ProspectivePaperTerminalPayloadV2"
        )
    encoded = canonical_prospective_paper_terminal_payload_v2(paper_terminal)
    observed = (
        paper_terminal.attempt_id,
        paper_terminal.attempt_plan_sha256,
        paper_terminal.promoting_plan_sha256,
        paper_terminal.execution_contract_sha256,
        paper_terminal.segment_id,
        paper_terminal.cell_id,
        paper_terminal.sizing_cell,
        paper_terminal.family,
        paper_terminal.family_rule_version,
        paper_terminal.symbol,
        paper_terminal.venue,
        paper_terminal.bar_open_ms,
        paper_terminal.bar_close_ms,
        paper_terminal.decision_cutoff_ms,
    )
    expected = (
        cell.attempt_id,
        plan.plan_sha256,
        plan.promoting_plan_sha256,
        plan.execution_contract_sha256,
        cell.segment_id,
        cell.cell_id,
        sizing_cell,
        cell.family,
        cell.rule_version,
        cell.symbol,
        VenueV2.USDM_FUTURES,
        cell.bar_open_ms,
        cell.bar_close_ms,
        cell.decision_cutoff_ms,
    )
    if observed != expected:
        raise ProspectivePositionTerminalContractErrorV2(
            "PAPER terminal differs from frozen plan/cell/sizing identity"
        )
    if (
        paper_terminal.production_order_placement
        or paper_terminal.actual_private_account_fee_claim
        or paper_terminal.position_terminal
        or paper_terminal.position_pnl_computed
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "upstream PAPER terminal makes a forbidden order, fee, or PnL claim"
        )
    return encoded


def _paper_terminal_position_side(
    paper_terminal: ProspectivePaperTerminalPayloadV2,
) -> MandatoryExitPositionSideV2:
    if paper_terminal.signal_side == "LONG":
        return MandatoryExitPositionSideV2.LONG
    if paper_terminal.signal_side == "SHORT":
        return MandatoryExitPositionSideV2.SHORT
    raise ProspectivePositionTerminalContractErrorV2(
        "full PAPER terminal has no concrete LONG/SHORT side"
    )


def _validate_full_fill_certificate(
    paper_terminal: ProspectivePaperTerminalPayloadV2,
    certificate: PaperFokFullFillCertificateV2 | None,
) -> bytes | None:
    if certificate is None:
        return None
    if type(certificate) is not PaperFokFullFillCertificateV2:
        raise ProspectivePositionTerminalContractErrorV2(
            "full_fill_certificate must be exact PaperFokFullFillCertificateV2"
        )
    encoded = canonical_paper_fok_full_fill_certificate_v2(certificate)
    expected_paper_side = (
        PaperFokSideV2.BUY if paper_terminal.signal_side == "LONG" else PaperFokSideV2.SELL
    )
    observed = (
        certificate.attempt_id,
        certificate.signal_event_id,
        certificate.decision_event_id,
        certificate.decision_payload_sha256,
        certificate.evidence_sha256,
        certificate.symbol,
        certificate.venue,
        certificate.promoting_plan_sha256,
        certificate.side,
        certificate.requested_quantity,
        certificate.filled_quantity,
        certificate.executable_vwap,
        certificate.executable_notional,
        certificate.certificate_sha256,
    )
    expected = (
        paper_terminal.attempt_id,
        paper_terminal.decision_event_id,
        paper_terminal.paper_decision_event_id,
        paper_terminal.paper_decision_payload_sha256,
        paper_terminal.paper_evidence_sha256,
        paper_terminal.symbol,
        paper_terminal.venue,
        paper_terminal.promoting_plan_sha256,
        expected_paper_side,
        paper_terminal.requested_quantity,
        paper_terminal.filled_quantity,
        paper_terminal.executable_vwap,
        paper_terminal.executable_notional,
        paper_terminal.full_fill_certificate_sha256,
    )
    if observed != expected:
        raise ProspectivePositionTerminalContractErrorV2(
            "full-fill certificate differs from typed PAPER terminal"
        )
    return encoded


def _validate_reference_join(
    *,
    common: _CommonTerminalFieldsV2,
    paper_terminal: ProspectivePaperTerminalPayloadV2,
    position_id: str,
    side: MandatoryExitPositionSideV2,
    certificate_sha256: str,
    family_evidence: FamilyLifecycleEvidenceReferenceV2 | None,
    mandatory_exit_evidence: MandatoryExitEvidenceReferenceV2 | None,
    fee_evidence: FinalFeeEvidenceReferenceV2 | None,
    funding_evidence: FundingCensusEvidenceReferenceV2 | None,
) -> None:
    identity = (
        common["attempt_id"],
        common["promoting_plan_sha256"],
        common["symbol"],
        position_id,
    )
    if family_evidence is not None:
        if type(family_evidence) is not FamilyLifecycleEvidenceReferenceV2:
            raise ProspectivePositionTerminalContractErrorV2("family_evidence has the wrong type")
        _verify_family_reference(family_evidence)
        if (
            family_evidence.attempt_id,
            family_evidence.promoting_plan_sha256,
            family_evidence.symbol,
            family_evidence.position_id,
        ) != identity or (
            family_evidence.family is not common["family"]
            or family_evidence.side is not side
            or family_evidence.entry_event_id != paper_terminal.decision_event_id
            or family_evidence.full_fill_certificate_sha256 != certificate_sha256
        ):
            raise ProspectivePositionTerminalContractErrorV2(
                "family lifecycle reference differs from the opened position"
            )
    if mandatory_exit_evidence is not None:
        if type(mandatory_exit_evidence) is not MandatoryExitEvidenceReferenceV2:
            raise ProspectivePositionTerminalContractErrorV2(
                "mandatory_exit_evidence has the wrong type"
            )
        _verify_mandatory_exit_reference(mandatory_exit_evidence)
        if (
            mandatory_exit_evidence.attempt_id,
            mandatory_exit_evidence.promoting_plan_sha256,
            mandatory_exit_evidence.symbol,
            mandatory_exit_evidence.position_id,
        ) != identity or (
            mandatory_exit_evidence.side is not side
            or family_evidence is None
            or mandatory_exit_evidence.family_exit_event_id != family_evidence.exit_event_id
            or mandatory_exit_evidence.filled_quantity != paper_terminal.filled_quantity
        ):
            raise ProspectivePositionTerminalContractErrorV2(
                "mandatory-exit reference differs from position or family exit"
            )
        if common["finalized_at_ms"] < mandatory_exit_evidence.terminal_at_ms:
            raise ProspectivePositionTerminalContractErrorV2(
                "position terminal finalization predates mandatory exit"
            )
    if fee_evidence is not None:
        if type(fee_evidence) is not FinalFeeEvidenceReferenceV2:
            raise ProspectivePositionTerminalContractErrorV2("fee_evidence has the wrong type")
        _verify_fee_reference(fee_evidence)
        if (
            fee_evidence.attempt_id,
            fee_evidence.promoting_plan_sha256,
            fee_evidence.symbol,
            fee_evidence.position_id,
        ) != identity:
            raise ProspectivePositionTerminalContractErrorV2(
                "fee reference differs from the opened position"
            )
        if mandatory_exit_evidence is not None and (
            fee_evidence.mandatory_exit_fee_certificate_sha256
            != mandatory_exit_evidence.fee_certificate_sha256
            or fee_evidence.exit_slices_root_sha256
            != mandatory_exit_evidence.exit_slices_root_sha256
            or fee_evidence.exit_slice_count != mandatory_exit_evidence.exit_slice_count
        ):
            raise ProspectivePositionTerminalContractErrorV2(
                "fee reference differs from mandatory-exit inventory"
            )
    if funding_evidence is not None:
        if type(funding_evidence) is not FundingCensusEvidenceReferenceV2:
            raise ProspectivePositionTerminalContractErrorV2("funding_evidence has the wrong type")
        _verify_funding_reference(funding_evidence)
        if (
            funding_evidence.attempt_id,
            funding_evidence.promoting_plan_sha256,
            funding_evidence.symbol,
            funding_evidence.position_id,
        ) != identity:
            raise ProspectivePositionTerminalContractErrorV2(
                "funding reference differs from the opened position"
            )
        if paper_terminal.target_venue_ms is None:
            raise ProspectivePositionTerminalContractErrorV2(
                "full PAPER terminal lacks its target venue time"
            )
        if funding_evidence.interval_start_ms != paper_terminal.target_venue_ms:
            raise ProspectivePositionTerminalContractErrorV2(
                "funding census does not start at the PAPER execution target"
            )
        if mandatory_exit_evidence is not None and (
            funding_evidence.interval_end_ms != mandatory_exit_evidence.terminal_at_ms
        ):
            raise ProspectivePositionTerminalContractErrorV2(
                "funding census does not end at mandatory-exit terminal time"
            )


def _validate_family_reference(value: FamilyLifecycleEvidenceReferenceV2) -> None:
    if not isinstance(value.family, PromotingFamilyV2):
        raise ProspectivePositionTerminalContractErrorV2("family reference family is invalid")
    _identity(value.attempt_id, "attempt_id")
    _symbol(value.symbol)
    for digest, label in (
        (value.promoting_plan_sha256, "promoting_plan_sha256"),
        (value.position_id, "position_id"),
        (value.entry_event_id, "entry_event_id"),
        (value.full_fill_certificate_sha256, "full_fill_certificate_sha256"),
        (value.admission_receipt_sha256, "admission_receipt_sha256"),
        (value.admission_input_sha256, "admission_input_sha256"),
        (value.admission_pre_root_sha256, "admission_pre_root_sha256"),
        (value.admission_post_root_sha256, "admission_post_root_sha256"),
        (value.exit_event_id, "exit_event_id"),
        (value.exit_receipt_sha256, "exit_receipt_sha256"),
        (value.exit_input_sha256, "exit_input_sha256"),
        (value.exit_pre_root_sha256, "exit_pre_root_sha256"),
        (value.exit_post_root_sha256, "exit_post_root_sha256"),
    ):
        _require_sha256(digest, label)
    for count, label in (
        (value.admission_pre_event_count, "admission_pre_event_count"),
        (value.admission_post_event_count, "admission_post_event_count"),
        (value.exit_pre_event_count, "exit_pre_event_count"),
        (value.exit_post_event_count, "exit_post_event_count"),
        (value.exit_decision_cutoff_ms, "exit_decision_cutoff_ms"),
    ):
        _safe_nonnegative_integer(count, label)
    if not isinstance(value.side, MandatoryExitPositionSideV2):
        raise ProspectivePositionTerminalContractErrorV2("family reference side is invalid")
    if not isinstance(value.source_authority, PositionEvidenceReferenceAuthorityV2):
        raise ProspectivePositionTerminalContractErrorV2("family reference authority is invalid")
    if value.family is PromotingFamilyV2.A:
        expected_authority = (
            PositionEvidenceReferenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_A_RECEIPTS
        )
        admission_new_increment = 1
    elif value.family is PromotingFamilyV2.B:
        expected_authority = (
            PositionEvidenceReferenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_B_RECEIPTS
        )
        admission_new_increment = 0
    else:
        expected_authority = (
            PositionEvidenceReferenceAuthorityV2.LIVE_FACTORY_SEALED_FAMILY_C_RECEIPTS
        )
        admission_new_increment = 0
    if value.source_authority is not expected_authority:
        raise ProspectivePositionTerminalContractErrorV2(
            f"Family {value.family.value} requires its live factory-sealed receipt projection"
        )
    if type(value.terminal_exit) is not bool or not value.terminal_exit:
        raise ProspectivePositionTerminalContractErrorV2(
            "family reference requires a terminal exit"
        )
    _validate_receipt_transition(
        value.admission_disposition,
        value.admission_pre_root_sha256,
        value.admission_pre_event_count,
        value.admission_post_root_sha256,
        value.admission_post_event_count,
        "admission",
        new_event_increment=admission_new_increment,
    )
    _validate_receipt_transition(
        value.exit_disposition,
        value.exit_pre_root_sha256,
        value.exit_pre_event_count,
        value.exit_post_root_sha256,
        value.exit_post_event_count,
        "exit",
        new_event_increment=1,
    )


def _validate_receipt_transition(
    disposition: str,
    pre_root: str,
    pre_count: int,
    post_root: str,
    post_count: int,
    label: str,
    *,
    new_event_increment: int,
) -> None:
    if disposition == "NEW_BY_THIS_TRANSACTION":
        if post_count != pre_count + new_event_increment or post_root == pre_root:
            raise ProspectivePositionTerminalContractErrorV2(
                f"{label} NEW receipt has invalid state transition"
            )
        return
    if disposition == "PREEXISTING":
        if post_count != pre_count or post_root != pre_root:
            raise ProspectivePositionTerminalContractErrorV2(
                f"{label} PREEXISTING receipt must preserve state"
            )
        return
    raise ProspectivePositionTerminalContractErrorV2(f"{label} disposition is unsupported")


def _verify_family_reference(value: FamilyLifecycleEvidenceReferenceV2) -> None:
    _validate_family_reference(value)
    expected = _hash_document(_FAMILY_REFERENCE_DOMAIN, _family_reference_document(value))
    if value.reference_sha256 != expected:
        raise ProspectivePositionTerminalContractErrorV2(
            "family lifecycle reference hash differs from canonical content"
        )


def _validate_mandatory_exit_reference(value: MandatoryExitEvidenceReferenceV2) -> None:
    _identity(value.attempt_id, "attempt_id")
    _symbol(value.symbol)
    for digest, label in (
        (value.promoting_plan_sha256, "promoting_plan_sha256"),
        (value.position_id, "position_id"),
        (value.family_exit_event_id, "family_exit_event_id"),
        (value.mandatory_position_sha256, "mandatory_position_sha256"),
        (value.exit_intent_sha256, "exit_intent_sha256"),
        (value.target_cursor_sha256, "target_cursor_sha256"),
        (value.terminal_sha256, "terminal_sha256"),
        (value.terminal_payload_sha256, "terminal_payload_sha256"),
        (value.fee_certificate_sha256, "fee_certificate_sha256"),
        (value.ledger_checkpoint_sha256, "ledger_checkpoint_sha256"),
        (value.exit_slices_root_sha256, "exit_slices_root_sha256"),
    ):
        _require_sha256(digest, label)
    if not isinstance(value.side, MandatoryExitPositionSideV2):
        raise ProspectivePositionTerminalContractErrorV2("mandatory-exit side is invalid")
    if value.terminal_status is not MandatoryExitTerminalStatusV2.EXITED_FULL:
        raise ProspectivePositionTerminalContractErrorV2(
            "mandatory-exit reference must be EXITED_FULL"
        )
    if value.rule_version != MANDATORY_EXIT_RULE_VERSION_V2:
        raise ProspectivePositionTerminalContractErrorV2(
            "mandatory-exit reference rule version differs"
        )
    if value.source_authority is not PositionEvidenceReferenceAuthorityV2.CALLER_HASH_REFERENCE:
        raise ProspectivePositionTerminalContractErrorV2(
            "mandatory-exit reference authority differs"
        )
    _safe_positive_integer(value.exit_slice_count, "exit_slice_count")
    _safe_nonnegative_integer(value.terminal_at_ms, "terminal_at_ms")
    _finite_decimal(value.filled_quantity, "filled_quantity", positive=True)
    _finite_decimal(value.residual_quantity, "residual_quantity", nonnegative=True)
    _finite_decimal(
        value.gross_exit_notional_usdt,
        "gross_exit_notional_usdt",
        positive=True,
    )
    _finite_decimal(value.signed_exit_cashflow_usdt, "signed_exit_cashflow_usdt")
    if value.residual_quantity != 0:
        raise ProspectivePositionTerminalContractErrorV2(
            "EXITED_FULL reference requires zero residual quantity"
        )
    expected_cashflow = (
        value.gross_exit_notional_usdt
        if value.side is MandatoryExitPositionSideV2.LONG
        else -value.gross_exit_notional_usdt
    )
    if value.signed_exit_cashflow_usdt != expected_cashflow:
        raise ProspectivePositionTerminalContractErrorV2(
            "mandatory-exit signed cashflow contradicts position side and notional"
        )


def _verify_mandatory_exit_reference(value: MandatoryExitEvidenceReferenceV2) -> None:
    _validate_mandatory_exit_reference(value)
    expected = _hash_document(
        _MANDATORY_EXIT_REFERENCE_DOMAIN,
        _mandatory_exit_reference_document(value),
    )
    if value.reference_sha256 != expected:
        raise ProspectivePositionTerminalContractErrorV2(
            "mandatory-exit reference hash differs from canonical content"
        )


def _validate_fee_reference(value: FinalFeeEvidenceReferenceV2) -> None:
    _identity(value.attempt_id, "attempt_id")
    _symbol(value.symbol)
    for digest, label in (
        (value.promoting_plan_sha256, "promoting_plan_sha256"),
        (value.position_id, "position_id"),
        (
            value.mandatory_exit_fee_certificate_sha256,
            "mandatory_exit_fee_certificate_sha256",
        ),
        (value.final_timeline_checkpoint_sha256, "final_timeline_checkpoint_sha256"),
        (value.final_timeline_root_sha256, "final_timeline_root_sha256"),
        (value.fee_position_payload_sha256, "fee_position_payload_sha256"),
        (value.exit_slices_root_sha256, "exit_slices_root_sha256"),
    ):
        _require_sha256(digest, label)
    _safe_positive_integer(value.exit_slice_count, "exit_slice_count")
    if not isinstance(value.multiplier, FeeMultiplierV2):
        raise ProspectivePositionTerminalContractErrorV2("fee multiplier is invalid")
    if value.status is not FilledPositionFeeStatusV2.BOTH_LEGS_COMPLETE:
        raise ProspectivePositionTerminalContractErrorV2(
            "final fee reference must resolve both legs"
        )
    if value.scenario != PUBLIC_FEE_SCENARIO_V2 or value.rule_version != FEE_RULE_VERSION_V2:
        raise ProspectivePositionTerminalContractErrorV2("fee scenario or rule version differs")
    if value.actual_private_account_fee_claim:
        raise ProspectivePositionTerminalContractErrorV2(
            "fee reference cannot claim private account fees"
        )
    if value.source_authority is not PositionEvidenceReferenceAuthorityV2.CALLER_HASH_REFERENCE:
        raise ProspectivePositionTerminalContractErrorV2("fee reference authority differs")
    for amount, label in (
        (value.entry_fee_usdt, "entry_fee_usdt"),
        (value.exit_fee_usdt, "exit_fee_usdt"),
        (value.total_fee_usdt, "total_fee_usdt"),
    ):
        _finite_decimal(amount, label, nonnegative=True)
    if value.total_fee_usdt != _exact_sum(value.entry_fee_usdt, value.exit_fee_usdt):
        raise ProspectivePositionTerminalContractErrorV2(
            "total fee differs from exact entry plus exit fee"
        )


def _verify_fee_reference(value: FinalFeeEvidenceReferenceV2) -> None:
    _validate_fee_reference(value)
    expected = _hash_document(_FEE_REFERENCE_DOMAIN, _fee_reference_document(value))
    if value.reference_sha256 != expected:
        raise ProspectivePositionTerminalContractErrorV2(
            "fee reference hash differs from canonical content"
        )


def _validate_funding_reference(value: FundingCensusEvidenceReferenceV2) -> None:
    _identity(value.attempt_id, "attempt_id")
    _symbol(value.symbol)
    for digest, label in (
        (value.promoting_plan_sha256, "promoting_plan_sha256"),
        (value.position_id, "position_id"),
        (value.census_certificate_sha256, "census_certificate_sha256"),
        (value.registry_checkpoint_sha256, "registry_checkpoint_sha256"),
        (
            value.position_ledger_checkpoint_sha256,
            "position_ledger_checkpoint_sha256",
        ),
        (value.cashflow_root_sha256, "cashflow_root_sha256"),
    ):
        _require_sha256(digest, label)
    for count, label in (
        (value.expected_funding_count, "expected_funding_count"),
        (value.confirmed_funding_count, "confirmed_funding_count"),
        (value.cashflow_event_count, "cashflow_event_count"),
        (value.interval_start_ms, "interval_start_ms"),
        (value.interval_end_ms, "interval_end_ms"),
        (value.observed_through_ms, "observed_through_ms"),
    ):
        _safe_nonnegative_integer(count, label)
    if value.interval_end_ms < value.interval_start_ms:
        raise ProspectivePositionTerminalContractErrorV2("funding census interval is reversed")
    if value.observed_through_ms < value.interval_end_ms:
        raise ProspectivePositionTerminalContractErrorV2(
            "funding census is not observed through the exit terminal"
        )
    if not (
        value.expected_funding_count == value.confirmed_funding_count == value.cashflow_event_count
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "funding census expected, confirmed, and cashflow counts differ"
        )
    _finite_decimal(
        value.realized_funding_cashflow_usdt,
        "realized_funding_cashflow_usdt",
    )
    if value.boundary_convention is not (
        FundingCensusBoundaryConventionV2.Q_BEFORE_FUNDING_EQUAL_MS_ADVERSE_ONLY
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "funding census boundary convention differs"
        )
    if value.rule_version != FUNDING_RULE_VERSION_V2 or not value.public_data_only:
        raise ProspectivePositionTerminalContractErrorV2(
            "funding reference rule or public-data flag differs"
        )
    if value.source_authority is not PositionEvidenceReferenceAuthorityV2.CALLER_HASH_REFERENCE:
        raise ProspectivePositionTerminalContractErrorV2("funding reference authority differs")


def _verify_funding_reference(value: FundingCensusEvidenceReferenceV2) -> None:
    _validate_funding_reference(value)
    expected = _hash_document(
        _FUNDING_REFERENCE_DOMAIN,
        _funding_reference_document(value),
    )
    if value.reference_sha256 != expected:
        raise ProspectivePositionTerminalContractErrorV2(
            "funding reference hash differs from canonical content"
        )


def _validate_payload_shape(payload: ProspectivePositionTerminalPayloadV2) -> None:
    _identity(payload.attempt_id, "attempt_id")
    _symbol(payload.symbol)
    for digest, label in (
        (payload.attempt_plan_sha256, "attempt_plan_sha256"),
        (payload.promoting_plan_sha256, "promoting_plan_sha256"),
        (payload.execution_contract_sha256, "execution_contract_sha256"),
        (payload.origin_segment_id, "origin_segment_id"),
        (payload.origin_cell_id, "origin_cell_id"),
        (payload.outcome_id, "outcome_id"),
        (payload.paper_terminal_payload_sha256, "paper_terminal_payload_sha256"),
        (payload.paper_terminal_jsonl_sha256, "paper_terminal_jsonl_sha256"),
    ):
        _require_sha256(digest, label)
    for digest, label in (
        (payload.position_id, "position_id"),
        (payload.paper_terminal_record_sha256, "paper_terminal_record_sha256"),
        (
            payload.paper_full_fill_certificate_sha256,
            "paper_full_fill_certificate_sha256",
        ),
        (
            payload.paper_full_fill_certificate_jsonl_sha256,
            "paper_full_fill_certificate_jsonl_sha256",
        ),
    ):
        if digest is not None:
            _require_sha256(digest, label)
    if not isinstance(payload.sizing_cell, PaperSizingCellV2):
        raise ProspectivePositionTerminalContractErrorV2("sizing_cell is invalid")
    if not isinstance(payload.family, PromotingFamilyV2):
        raise ProspectivePositionTerminalContractErrorV2("family is invalid")
    _identity(payload.family_rule_version, "family_rule_version")
    if payload.venue is not VenueV2.USDM_FUTURES:
        raise ProspectivePositionTerminalContractErrorV2(
            "position terminal venue must be USD-M Futures"
        )
    for value, label in (
        (payload.bar_open_ms, "bar_open_ms"),
        (payload.bar_close_ms, "bar_close_ms"),
        (payload.decision_cutoff_ms, "decision_cutoff_ms"),
        (payload.finalized_at_ms, "finalized_at_ms"),
    ):
        _safe_nonnegative_integer(value, label)
    if not isinstance(payload.terminal_status, ProspectivePositionTerminalStatusV2):
        raise ProspectivePositionTerminalContractErrorV2("terminal_status is invalid")
    _reasons(payload.reasons)
    _identity(payload.invalidation, "invalidation")
    for reference, exact_type, verifier, label in (
        (
            payload.family_evidence,
            FamilyLifecycleEvidenceReferenceV2,
            _verify_family_reference,
            "family_evidence",
        ),
        (
            payload.mandatory_exit_evidence,
            MandatoryExitEvidenceReferenceV2,
            _verify_mandatory_exit_reference,
            "mandatory_exit_evidence",
        ),
        (
            payload.fee_evidence,
            FinalFeeEvidenceReferenceV2,
            _verify_fee_reference,
            "fee_evidence",
        ),
        (
            payload.funding_evidence,
            FundingCensusEvidenceReferenceV2,
            _verify_funding_reference,
            "funding_evidence",
        ),
    ):
        if reference is not None:
            if type(reference) is not exact_type:
                raise ProspectivePositionTerminalContractErrorV2(f"{label} has wrong type")
            verifier(reference)  # type: ignore[arg-type]
    for value, label in (
        (payload.entry_quantity, "entry_quantity"),
        (payload.entry_executable_vwap, "entry_executable_vwap"),
        (payload.entry_executable_notional_usdt, "entry_executable_notional_usdt"),
        (payload.signed_entry_cashflow_usdt, "signed_entry_cashflow_usdt"),
        (payload.exit_filled_quantity, "exit_filled_quantity"),
        (payload.gross_exit_notional_usdt, "gross_exit_notional_usdt"),
        (payload.signed_exit_cashflow_usdt, "signed_exit_cashflow_usdt"),
        (payload.gross_pnl_usdt, "gross_pnl_usdt"),
        (payload.entry_fee_usdt, "entry_fee_usdt"),
        (payload.exit_fee_usdt, "exit_fee_usdt"),
        (payload.total_fee_usdt, "total_fee_usdt"),
        (
            payload.realized_funding_cashflow_usdt,
            "realized_funding_cashflow_usdt",
        ),
        (payload.after_cost_pnl_usdt, "after_cost_pnl_usdt"),
        (payload.pnl_denominator_usdt, "pnl_denominator_usdt"),
        (payload.after_cost_return, "after_cost_return"),
        (payload.diagnostic_entry_slippage_usdt, "diagnostic_entry_slippage_usdt"),
    ):
        if value is not None:
            _finite_decimal(value, label)
    for value, label in (
        (payload.position_opened, "position_opened"),
        (payload.costs_complete, "costs_complete"),
        (payload.arithmetic_complete, "arithmetic_complete"),
    ):
        if type(value) is not bool:
            raise ProspectivePositionTerminalContractErrorV2(f"{label} must be boolean")
    expected_outcome_id = prospective_outcome_id_v2(
        attempt_plan_sha256=payload.attempt_plan_sha256,
        origin_segment_id=payload.origin_segment_id,
        origin_cell_id=payload.origin_cell_id,
        sizing_cell=payload.sizing_cell,
    )
    if payload.outcome_id != expected_outcome_id:
        raise ProspectivePositionTerminalContractErrorV2(
            "outcome_id differs from origin cell and sizing identity"
        )
    _validate_payload_reference_bindings(payload)
    _validate_status_shape(payload)
    if payload.schema_version != POSITION_TERMINAL_PAYLOAD_SCHEMA_V2:
        raise ProspectivePositionTerminalContractErrorV2("unsupported position terminal schema")
    if payload.protocol_rule_version != PROSPECTIVE_POSITION_TERMINAL_RULE_VERSION_V2:
        raise ProspectivePositionTerminalContractErrorV2("position terminal rule version differs")
    if payload.authority_status != PROSPECTIVE_POSITION_TERMINAL_AUTHORITY_V2:
        raise ProspectivePositionTerminalContractErrorV2(
            "position terminal authority label differs"
        )
    flags = (
        payload.position_terminal,
        payload.position_terminal_authoritative,
        payload.upstream_paper_terminal_authoritative,
        payload.evidence_references_replay_authoritative,
        payload.terminal_rule_plan_bound,
        payload.typed_wal_replay_authoritative,
        payload.production_order_placement,
        payload.actual_private_account_fee_claim,
        payload.slippage_double_counted,
        payload.efficacy_eligible,
    )
    if flags != (True, False, False, False, False, False, False, False, False, False):
        raise ProspectivePositionTerminalContractErrorV2(
            "position terminal authority or safety flags differ"
        )


def _validate_payload_reference_bindings(
    payload: ProspectivePositionTerminalPayloadV2,
) -> None:
    """Recheck compact-reference joins without trusting factory construction."""

    identity = (
        payload.attempt_id,
        payload.promoting_plan_sha256,
        payload.symbol,
        payload.position_id,
    )
    family = payload.family_evidence
    if family is not None:
        if (
            family.attempt_id,
            family.promoting_plan_sha256,
            family.symbol,
            family.position_id,
        ) != identity or family.family is not payload.family:
            raise ProspectivePositionTerminalContractErrorV2(
                "family reference differs from terminal identity"
            )
        if (
            payload.position_side is None
            or family.side is not payload.position_side
            or payload.paper_full_fill_certificate_sha256 is None
            or family.full_fill_certificate_sha256 != payload.paper_full_fill_certificate_sha256
        ):
            raise ProspectivePositionTerminalContractErrorV2(
                "family reference differs from terminal side or entry certificate"
            )
    mandatory = payload.mandatory_exit_evidence
    if mandatory is not None:
        if (
            mandatory.attempt_id,
            mandatory.promoting_plan_sha256,
            mandatory.symbol,
            mandatory.position_id,
        ) != identity or (
            payload.position_side is None
            or mandatory.side is not payload.position_side
            or family is None
            or mandatory.family_exit_event_id != family.exit_event_id
            or payload.entry_quantity is None
            or mandatory.filled_quantity != payload.entry_quantity
        ):
            raise ProspectivePositionTerminalContractErrorV2(
                "mandatory-exit reference differs from terminal inventory"
            )
        if payload.finalized_at_ms < mandatory.terminal_at_ms:
            raise ProspectivePositionTerminalContractErrorV2(
                "terminal finalization predates mandatory exit"
            )
    fee = payload.fee_evidence
    if fee is not None:
        if (
            fee.attempt_id,
            fee.promoting_plan_sha256,
            fee.symbol,
            fee.position_id,
        ) != identity:
            raise ProspectivePositionTerminalContractErrorV2(
                "fee reference differs from terminal identity"
            )
        if mandatory is not None and (
            fee.mandatory_exit_fee_certificate_sha256 != mandatory.fee_certificate_sha256
            or fee.exit_slices_root_sha256 != mandatory.exit_slices_root_sha256
            or fee.exit_slice_count != mandatory.exit_slice_count
        ):
            raise ProspectivePositionTerminalContractErrorV2(
                "fee reference differs from terminal exit inventory"
            )
    funding = payload.funding_evidence
    if funding is not None:
        if (
            funding.attempt_id,
            funding.promoting_plan_sha256,
            funding.symbol,
            funding.position_id,
        ) != identity:
            raise ProspectivePositionTerminalContractErrorV2(
                "funding reference differs from terminal identity"
            )
        if mandatory is not None and funding.interval_end_ms != mandatory.terminal_at_ms:
            raise ProspectivePositionTerminalContractErrorV2(
                "funding reference does not end at mandatory exit"
            )


def _validate_status_shape(payload: ProspectivePositionTerminalPayloadV2) -> None:
    references = (
        payload.family_evidence,
        payload.mandatory_exit_evidence,
        payload.fee_evidence,
        payload.funding_evidence,
    )
    if payload.terminal_status is ProspectivePositionTerminalStatusV2.SUPPRESSED_NO_POSITION:
        if payload.position_opened or payload.position_id is not None or any(references):
            raise ProspectivePositionTerminalContractErrorV2(
                "suppressed terminal cannot contain a position or lifecycle evidence"
            )
        if payload.position_side is not None:
            raise ProspectivePositionTerminalContractErrorV2(
                "suppressed terminal cannot contain a position side"
            )
        zero_fields = (
            payload.signed_entry_cashflow_usdt,
            payload.signed_exit_cashflow_usdt,
            payload.gross_pnl_usdt,
            payload.entry_fee_usdt,
            payload.exit_fee_usdt,
            payload.total_fee_usdt,
            payload.realized_funding_cashflow_usdt,
            payload.after_cost_pnl_usdt,
        )
        if any(value != 0 for value in zero_fields) or not (
            payload.costs_complete and payload.arithmetic_complete
        ):
            raise ProspectivePositionTerminalContractErrorV2(
                "suppressed no-position terminal requires exact zero cashflows"
            )
        if any(
            value is not None
            for value in (
                payload.paper_full_fill_certificate_sha256,
                payload.paper_full_fill_certificate_jsonl_sha256,
                payload.entry_quantity,
                payload.entry_executable_vwap,
                payload.entry_executable_notional_usdt,
                payload.exit_filled_quantity,
                payload.gross_exit_notional_usdt,
                payload.pnl_denominator_usdt,
                payload.after_cost_return,
            )
        ):
            raise ProspectivePositionTerminalContractErrorV2(
                "suppressed terminal contains impossible position values"
            )
        return
    if payload.terminal_status is ProspectivePositionTerminalStatusV2.INCOMPLETE:
        if payload.costs_complete or payload.arithmetic_complete:
            raise ProspectivePositionTerminalContractErrorV2(
                "incomplete terminal cannot claim complete costs or arithmetic"
            )
        if any(
            value is not None
            for value in (
                payload.exit_filled_quantity,
                payload.gross_exit_notional_usdt,
                payload.signed_exit_cashflow_usdt,
                payload.gross_pnl_usdt,
                payload.entry_fee_usdt,
                payload.exit_fee_usdt,
                payload.total_fee_usdt,
                payload.realized_funding_cashflow_usdt,
                payload.after_cost_pnl_usdt,
                payload.pnl_denominator_usdt,
                payload.after_cost_return,
            )
        ):
            raise ProspectivePositionTerminalContractErrorV2(
                "incomplete terminal cannot expose final numeric outcomes"
            )
        if payload.position_opened:
            _validate_open_identity_and_entry(payload)
        elif any(
            value is not None
            for value in (
                payload.position_id,
                payload.position_side,
                payload.paper_full_fill_certificate_sha256,
                payload.paper_full_fill_certificate_jsonl_sha256,
                payload.entry_quantity,
                payload.entry_executable_vwap,
                payload.entry_executable_notional_usdt,
                payload.signed_entry_cashflow_usdt,
            )
        ) or any(references):
            raise ProspectivePositionTerminalContractErrorV2(
                "incomplete no-position terminal contains position evidence"
            )
        return
    if payload.terminal_status is not ProspectivePositionTerminalStatusV2.COMPLETE_CALCULATION:
        raise ProspectivePositionTerminalContractErrorV2("unsupported terminal status")
    _validate_open_identity_and_entry(payload)
    if any(reference is None for reference in references):
        raise ProspectivePositionTerminalContractErrorV2(
            "complete calculation requires every lifecycle reference"
        )
    if payload.paper_terminal_record_sha256 is None or (
        payload.paper_full_fill_certificate_jsonl_sha256 is None
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "complete calculation requires durable PAPER and typed certificate references"
        )
    if not payload.costs_complete or not payload.arithmetic_complete:
        raise ProspectivePositionTerminalContractErrorV2(
            "complete calculation must mark costs and arithmetic complete"
        )
    mandatory = payload.mandatory_exit_evidence
    fee = payload.fee_evidence
    funding = payload.funding_evidence
    assert mandatory is not None and fee is not None and funding is not None
    expected_gross = _exact_sum(
        cast(Decimal, payload.signed_entry_cashflow_usdt),
        mandatory.signed_exit_cashflow_usdt,
    )
    expected_after = _exact_sum(
        expected_gross,
        funding.realized_funding_cashflow_usdt,
        -fee.total_fee_usdt,
    )
    if (
        payload.exit_filled_quantity != mandatory.filled_quantity
        or payload.gross_exit_notional_usdt != mandatory.gross_exit_notional_usdt
        or payload.signed_exit_cashflow_usdt != mandatory.signed_exit_cashflow_usdt
        or payload.gross_pnl_usdt != expected_gross
        or payload.entry_fee_usdt != fee.entry_fee_usdt
        or payload.exit_fee_usdt != fee.exit_fee_usdt
        or payload.total_fee_usdt != fee.total_fee_usdt
        or payload.realized_funding_cashflow_usdt != funding.realized_funding_cashflow_usdt
        or payload.after_cost_pnl_usdt != expected_after
        or payload.pnl_denominator_usdt != payload.entry_executable_notional_usdt
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "complete position cashflow projection is contradictory"
        )
    denominator = payload.pnl_denominator_usdt
    if denominator is None or denominator <= 0:
        raise ProspectivePositionTerminalContractErrorV2(
            "complete position requires a positive PnL denominator"
        )
    with localcontext(protocol_decimal_context_v2()):
        expected_return = expected_after / denominator
    if payload.after_cost_return != expected_return:
        raise ProspectivePositionTerminalContractErrorV2(
            "after-cost return differs from frozen Decimal34 division"
        )


def _validate_open_identity_and_entry(payload: ProspectivePositionTerminalPayloadV2) -> None:
    if (
        payload.position_id is None
        or not isinstance(payload.position_side, MandatoryExitPositionSideV2)
        or payload.paper_full_fill_certificate_sha256 is None
        or payload.entry_quantity is None
        or payload.entry_executable_vwap is None
        or payload.entry_executable_notional_usdt is None
        or payload.signed_entry_cashflow_usdt is None
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "opened position lacks identity or entry execution values"
        )
    expected_position_id = prospective_position_id_v2(
        outcome_id=payload.outcome_id,
        certificate_sha256=payload.paper_full_fill_certificate_sha256,
    )
    if payload.position_id != expected_position_id:
        raise ProspectivePositionTerminalContractErrorV2(
            "position_id differs from outcome and entry certificate"
        )
    for value, label in (
        (payload.entry_quantity, "entry_quantity"),
        (payload.entry_executable_vwap, "entry_executable_vwap"),
        (payload.entry_executable_notional_usdt, "entry_executable_notional_usdt"),
    ):
        _finite_decimal(value, label, positive=True)
    expected_entry = (
        payload.entry_executable_notional_usdt
        if payload.position_side is MandatoryExitPositionSideV2.SHORT
        else -payload.entry_executable_notional_usdt
    )
    if payload.signed_entry_cashflow_usdt != expected_entry:
        raise ProspectivePositionTerminalContractErrorV2(
            "signed entry cashflow contradicts side and executable notional"
        )


def _family_reference_document(value: FamilyLifecycleEvidenceReferenceV2) -> dict[str, object]:
    return {
        "admission_disposition": value.admission_disposition,
        "admission_input_sha256": value.admission_input_sha256,
        "admission_post_event_count": value.admission_post_event_count,
        "admission_post_root_sha256": value.admission_post_root_sha256,
        "admission_pre_event_count": value.admission_pre_event_count,
        "admission_pre_root_sha256": value.admission_pre_root_sha256,
        "admission_receipt_sha256": value.admission_receipt_sha256,
        "attempt_id": value.attempt_id,
        "entry_event_id": value.entry_event_id,
        "exit_decision_cutoff_ms": value.exit_decision_cutoff_ms,
        "exit_disposition": value.exit_disposition,
        "exit_event_id": value.exit_event_id,
        "exit_input_sha256": value.exit_input_sha256,
        "exit_post_event_count": value.exit_post_event_count,
        "exit_post_root_sha256": value.exit_post_root_sha256,
        "exit_pre_event_count": value.exit_pre_event_count,
        "exit_pre_root_sha256": value.exit_pre_root_sha256,
        "exit_receipt_sha256": value.exit_receipt_sha256,
        "family": value.family.value,
        "full_fill_certificate_sha256": value.full_fill_certificate_sha256,
        "position_id": value.position_id,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "side": value.side.value,
        "source_authority": value.source_authority.value,
        "symbol": value.symbol,
        "terminal_exit": value.terminal_exit,
    }


def _mandatory_exit_reference_document(
    value: MandatoryExitEvidenceReferenceV2,
) -> dict[str, object]:
    return {
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


def _fee_reference_document(value: FinalFeeEvidenceReferenceV2) -> dict[str, object]:
    return {
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


def _funding_reference_document(
    value: FundingCensusEvidenceReferenceV2,
) -> dict[str, object]:
    return {
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


def _reference_document(
    value: (
        FamilyLifecycleEvidenceReferenceV2
        | MandatoryExitEvidenceReferenceV2
        | FinalFeeEvidenceReferenceV2
        | FundingCensusEvidenceReferenceV2
        | None
    ),
) -> dict[str, object] | None:
    if value is None:
        return None
    if type(value) is FamilyLifecycleEvidenceReferenceV2:
        document = _family_reference_document(value)
    elif type(value) is MandatoryExitEvidenceReferenceV2:
        document = _mandatory_exit_reference_document(value)
    elif type(value) is FinalFeeEvidenceReferenceV2:
        document = _fee_reference_document(value)
    elif type(value) is FundingCensusEvidenceReferenceV2:
        document = _funding_reference_document(value)
    else:  # pragma: no cover - all callers are guarded by exact type checks
        raise ProspectivePositionTerminalContractErrorV2("unknown evidence reference type")
    return {**document, "reference_sha256": value.reference_sha256}


def _payload_document(payload: ProspectivePositionTerminalPayloadV2) -> dict[str, object]:
    return {
        "actual_private_account_fee_claim": payload.actual_private_account_fee_claim,
        "after_cost_pnl_usdt": _decimal_text(payload.after_cost_pnl_usdt),
        "after_cost_return": _decimal_text(payload.after_cost_return),
        "arithmetic_complete": payload.arithmetic_complete,
        "attempt_id": payload.attempt_id,
        "attempt_plan_sha256": payload.attempt_plan_sha256,
        "authority_status": payload.authority_status,
        "bar_close_ms": payload.bar_close_ms,
        "bar_open_ms": payload.bar_open_ms,
        "costs_complete": payload.costs_complete,
        "decision_cutoff_ms": payload.decision_cutoff_ms,
        "diagnostic_entry_slippage_usdt": _decimal_text(payload.diagnostic_entry_slippage_usdt),
        "efficacy_eligible": payload.efficacy_eligible,
        "entry_executable_notional_usdt": _decimal_text(payload.entry_executable_notional_usdt),
        "entry_executable_vwap": _decimal_text(payload.entry_executable_vwap),
        "entry_fee_usdt": _decimal_text(payload.entry_fee_usdt),
        "entry_quantity": _decimal_text(payload.entry_quantity),
        "evidence_references_replay_authoritative": (
            payload.evidence_references_replay_authoritative
        ),
        "execution_contract_sha256": payload.execution_contract_sha256,
        "exit_fee_usdt": _decimal_text(payload.exit_fee_usdt),
        "exit_filled_quantity": _decimal_text(payload.exit_filled_quantity),
        "family": payload.family.value,
        "family_evidence": _reference_document(payload.family_evidence),
        "family_rule_version": payload.family_rule_version,
        "fee_evidence": _reference_document(payload.fee_evidence),
        "finalized_at_ms": payload.finalized_at_ms,
        "funding_evidence": _reference_document(payload.funding_evidence),
        "gross_exit_notional_usdt": _decimal_text(payload.gross_exit_notional_usdt),
        "gross_pnl_usdt": _decimal_text(payload.gross_pnl_usdt),
        "invalidation": payload.invalidation,
        "mandatory_exit_evidence": _reference_document(payload.mandatory_exit_evidence),
        "origin_cell_id": payload.origin_cell_id,
        "origin_segment_id": payload.origin_segment_id,
        "outcome_id": payload.outcome_id,
        "paper_full_fill_certificate_jsonl_sha256": (
            payload.paper_full_fill_certificate_jsonl_sha256
        ),
        "paper_full_fill_certificate_sha256": (payload.paper_full_fill_certificate_sha256),
        "paper_terminal_jsonl_sha256": payload.paper_terminal_jsonl_sha256,
        "paper_terminal_payload_sha256": payload.paper_terminal_payload_sha256,
        "paper_terminal_record_sha256": payload.paper_terminal_record_sha256,
        "pnl_denominator_usdt": _decimal_text(payload.pnl_denominator_usdt),
        "position_id": payload.position_id,
        "position_opened": payload.position_opened,
        "position_side": (None if payload.position_side is None else payload.position_side.value),
        "position_terminal": payload.position_terminal,
        "position_terminal_authoritative": payload.position_terminal_authoritative,
        "production_order_placement": payload.production_order_placement,
        "promoting_plan_sha256": payload.promoting_plan_sha256,
        "protocol_rule_version": payload.protocol_rule_version,
        "realized_funding_cashflow_usdt": _decimal_text(payload.realized_funding_cashflow_usdt),
        "reasons": list(payload.reasons),
        "schema_version": payload.schema_version,
        "signed_entry_cashflow_usdt": _decimal_text(payload.signed_entry_cashflow_usdt),
        "signed_exit_cashflow_usdt": _decimal_text(payload.signed_exit_cashflow_usdt),
        "sizing_cell": payload.sizing_cell.value,
        "slippage_double_counted": payload.slippage_double_counted,
        "symbol": payload.symbol,
        "terminal_rule_plan_bound": payload.terminal_rule_plan_bound,
        "terminal_status": payload.terminal_status.value,
        "total_fee_usdt": _decimal_text(payload.total_fee_usdt),
        "typed_wal_replay_authoritative": payload.typed_wal_replay_authoritative,
        "upstream_paper_terminal_authoritative": (payload.upstream_paper_terminal_authoritative),
        "venue": payload.venue.value,
    }


_PAYLOAD_KEYS: Final = frozenset(
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


def _decode_exact_payload(encoded: bytes) -> dict[str, object]:
    if type(encoded) is not bytes or not encoded:
        raise ProspectivePositionTerminalContractErrorV2(
            "position terminal must be non-empty immutable bytes"
        )
    if len(encoded) > MAX_PROSPECTIVE_OUTCOME_WAL_PAYLOAD_BYTES_V2:
        raise ProspectivePositionTerminalContractErrorV2(
            "position terminal exceeds the fixed 64 KiB payload bound"
        )
    try:
        decoded: object = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProspectivePositionTerminalContractErrorV2(
            "position terminal is invalid UTF-8 JSON"
        ) from error
    if (
        not isinstance(decoded, dict)
        or frozenset(decoded) != _PAYLOAD_KEYS
        or canonical_json_line(decoded) != encoded
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            "position terminal schema or canonical JSONL differs"
        )
    return cast(dict[str, object], decoded)


def _family_a_admission_receipt_document(
    value: FamilyAAdmissionReceiptV2,
) -> dict[str, object]:
    return {
        "certificate_sha256": value.certificate_sha256,
        "disposition": value.disposition.value,
        "entry_event_id": value.entry_event_id,
        "input_sha256": value.input_sha256,
        "paper_decision_event_id": value.paper_decision_event_id,
        "paper_registry_checkpoint_sha256": value.paper_registry_checkpoint_sha256,
        "paper_registry_event_count": value.paper_registry_event_count,
        "paper_registry_maximum_events": value.paper_registry_maximum_events,
        "paper_registry_root_sha256": value.paper_registry_root_sha256,
        "position_admission_evidence_sha256": value.position.admission_evidence_sha256,
        "post_event_count": value.post_event_count,
        "post_root_sha256": value.post_root_sha256,
        "pre_event_count": value.pre_event_count,
        "pre_root_sha256": value.pre_root_sha256,
    }


def _family_a_exit_receipt_document(
    value: FamilyAExitMutationReceiptV2,
) -> dict[str, object]:
    return {
        "decision_payload_sha256": value.decision.payload_sha256,
        "disposition": value.disposition.value,
        "entry_event_id": value.entry_event_id,
        "exit_event_id": value.exit_event_id,
        "input_sha256": value.input_sha256,
        "post_active": value.post_active,
        "post_event_count": value.post_event_count,
        "post_next_horizon": value.post_next_horizon,
        "post_root_sha256": value.post_root_sha256,
        "post_sticky_inconclusive": value.post_sticky_inconclusive,
        "post_terminal": value.post_terminal,
        "pre_active": value.pre_active,
        "pre_event_count": value.pre_event_count,
        "pre_next_horizon": value.pre_next_horizon,
        "pre_root_sha256": value.pre_root_sha256,
        "pre_sticky_inconclusive": value.pre_sticky_inconclusive,
        "pre_terminal": value.pre_terminal,
    }


def _family_b_admission_receipt_document(
    value: FamilyBAdmissionReceiptV2,
) -> dict[str, object]:
    return {
        "decision_event_id": value.decision.event_id,
        "decision_payload_sha256": value.decision.payload_sha256,
        "disposition": value.disposition.value,
        "input_sha256": value.input_sha256,
        "paper_certificate_sha256": value.paper_certificate.certificate_sha256,
        "paper_decision_event_id": value.paper_decision.event_id,
        "paper_decision_payload_sha256": value.paper_decision.payload_sha256,
        "paper_registry_checkpoint_sha256": value.paper_registry_checkpoint_sha256,
        "paper_registry_event_count": value.paper_registry_event_count,
        "paper_registry_maximum_events": value.paper_registry_maximum_events,
        "paper_registry_root_sha256": value.paper_registry_root_sha256,
        "position_sha256": value.position_sha256,
        "post_event_count": value.post_event_count,
        "post_root_sha256": value.post_root_sha256,
        "pre_event_count": value.pre_event_count,
        "pre_root_sha256": value.pre_root_sha256,
    }


def _family_b_exit_receipt_document(
    value: FamilyBExitMutationReceiptV2,
) -> dict[str, object]:
    return {
        "decision_event_id": value.decision.event_id,
        "decision_payload_sha256": value.decision.payload_sha256,
        "disposition": value.disposition.value,
        "entry_event_id": value.entry_event_id,
        "input_sha256": value.input_sha256,
        "position_sha256": value.position_sha256,
        "post_active_entry_event_id": value.post_active_entry_event_id,
        "post_event_count": value.post_event_count,
        "post_root_sha256": value.post_root_sha256,
        "post_terminal": value.post_terminal,
        "pre_active_entry_event_id": value.pre_active_entry_event_id,
        "pre_event_count": value.pre_event_count,
        "pre_root_sha256": value.pre_root_sha256,
        "pre_terminal": value.pre_terminal,
    }


def _family_c_admission_receipt_document(
    value: FamilyCAdmissionReceiptV2,
) -> dict[str, object]:
    return {
        "decision_event_id": value.decision.event_id,
        "decision_payload_sha256": value.decision.payload_sha256,
        "disposition": value.disposition.value,
        "input_sha256": value.input_sha256,
        "paper_certificate_sha256": value.paper_certificate.certificate_sha256,
        "paper_decision_event_id": value.paper_decision.event_id,
        "paper_decision_payload_sha256": value.paper_decision.payload_sha256,
        "paper_registry_checkpoint_sha256": value.paper_registry_checkpoint_sha256,
        "paper_registry_event_count": value.paper_registry_event_count,
        "paper_registry_maximum_events": value.paper_registry_maximum_events,
        "paper_registry_root_sha256": value.paper_registry_root_sha256,
        "position_sha256": value.position_sha256,
        "post_event_count": value.post_event_count,
        "post_root_sha256": value.post_root_sha256,
        "pre_event_count": value.pre_event_count,
        "pre_root_sha256": value.pre_root_sha256,
    }


def _family_c_exit_receipt_document(
    value: FamilyCExitMutationReceiptV2,
) -> dict[str, object]:
    return {
        "decision_event_id": value.decision.event_id,
        "decision_payload_sha256": value.decision.payload_sha256,
        "disposition": value.disposition.value,
        "entry_event_id": value.entry_event_id,
        "input_sha256": value.input_sha256,
        "position_sha256": value.position_sha256,
        "post_active_entry_event_id": value.post_active_entry_event_id,
        "post_event_count": value.post_event_count,
        "post_next_horizon": value.post_next_horizon,
        "post_root_sha256": value.post_root_sha256,
        "post_sticky_inconclusive": value.post_sticky_inconclusive,
        "post_terminal": value.post_terminal,
        "pre_active_entry_event_id": value.pre_active_entry_event_id,
        "pre_event_count": value.pre_event_count,
        "pre_next_horizon": value.pre_next_horizon,
        "pre_root_sha256": value.pre_root_sha256,
        "pre_sticky_inconclusive": value.pre_sticky_inconclusive,
        "pre_terminal": value.pre_terminal,
    }


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _exact_sum(*values: Decimal) -> Decimal:
    total = sum((_decimal_fraction(value) for value in values), Fraction(0, 1))
    return _finite_fraction_decimal(total)


def _decimal_fraction(value: Decimal) -> Fraction:
    _finite_decimal(value, "cashflow Decimal")
    sign, digits, raw_exponent = value.as_tuple()
    exponent = cast(int, raw_exponent)
    coefficient = 0
    for digit in digits:
        coefficient = coefficient * 10 + digit
    if sign:
        coefficient = -coefficient
    if exponent >= 0:
        return Fraction(coefficient * (10**exponent), 1)
    return Fraction(coefficient, 10 ** (-exponent))


def _finite_fraction_decimal(value: Fraction) -> Decimal:
    numerator = value.numerator
    denominator = value.denominator
    twos = 0
    fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator != 1:  # pragma: no cover - sums of finite Decimals stay finite
        raise ProspectivePositionTerminalContractErrorV2(
            "cashflow arithmetic is not a finite base-ten Decimal"
        )
    scale = max(twos, fives)
    scaled = numerator * (2 ** (scale - twos)) * (5 ** (scale - fives))
    negative = scaled < 0
    digits = str(abs(scaled))
    if scale:
        digits = digits.rjust(scale + 1, "0")
        text = f"{digits[:-scale]}.{digits[-scale:]}"
    else:
        text = digits
    if negative and scaled != 0:
        text = f"-{text}"
    return Decimal(text)


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    _finite_decimal(value, "canonical Decimal")
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in ("-0", "") else text


def _identity(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 512
        or any(character in value for character in "\r\n\x00")
    ):
        raise ProspectivePositionTerminalContractErrorV2(
            f"{label} must be a bounded normalized identity"
        )
    return value


def _symbol(value: object) -> str:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise ProspectivePositionTerminalContractErrorV2("symbol must be a normalized USDT symbol")
    return value


def _require_sha256(value: object, label: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ProspectivePositionTerminalContractErrorV2(f"{label} must be lowercase SHA-256 hex")


def _safe_nonnegative_integer(value: object, label: str) -> None:
    if type(value) is not int or not 0 <= value <= _JCS_MAX_SAFE_INTEGER:
        raise ProspectivePositionTerminalContractErrorV2(
            f"{label} must be a nonnegative RFC8785-safe integer"
        )


def _safe_positive_integer(value: object, label: str) -> None:
    if type(value) is not int or not 1 <= value <= _JCS_MAX_SAFE_INTEGER:
        raise ProspectivePositionTerminalContractErrorV2(
            f"{label} must be a positive RFC8785-safe integer"
        )


def _finite_decimal(
    value: object,
    label: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise ProspectivePositionTerminalContractErrorV2(f"{label} must be a finite Decimal")
    if positive and value <= 0:
        raise ProspectivePositionTerminalContractErrorV2(f"{label} must be positive")
    if nonnegative and value < 0:
        raise ProspectivePositionTerminalContractErrorV2(f"{label} must be nonnegative")


def _reasons(values: tuple[str, ...]) -> None:
    if type(values) is not tuple or not 1 <= len(values) <= _MAX_REASONS:
        raise ProspectivePositionTerminalContractErrorV2(
            "reasons must be a non-empty bounded immutable tuple"
        )
    for value in values:
        _identity(value, "reason")
