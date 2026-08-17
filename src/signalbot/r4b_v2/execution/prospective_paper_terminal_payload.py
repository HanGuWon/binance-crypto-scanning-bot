"""Typed prospective PAPER-entry terminal payloads.

The live factory accepts only a factory-sealed decision transaction, the exact
frozen census plan/cell, and (when applicable) calculator/evaluator-sealed
sizing and PAPER FOK evidence.  It emits one terminal classification for one
fixed sizing cell without placing an order.

This is deliberately an *entry* terminal, not a position/PnL terminal.  The
current repository has no single authority that seals the later family exit,
mandatory-exit fills, final public-fee timeline, realized funding, and net PnL
into the prospective daily WAL.  Full PAPER entry fills therefore retain an
explicit deferred-cost state and are ineligible for efficacy claims here.

The sizing calculator also exposes only a caller-supplied reference-evidence
digest; it does not expose a factory proof that the digest is the exact causal
target mark row.  Consequently this module is honest non-authoritative
serialization until a future owner wires typed replay and that membership
proof into the WAL.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from typing import TYPE_CHECKING, Final, cast

from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    PAPER_FOK_RULE_VERSION_V2,
    PaperFokClosureMethodV2,
    PaperFokEntryDecisionV2,
    PaperFokEntryStatusV2,
    PaperFokFullFillCertificateV2,
    PaperFokSideV2,
    canonical_paper_fok_entry_decision_v2,
    canonical_paper_fok_full_fill_certificate_v2,
)
from signalbot.r4b_v2.execution.paper_sizing import (
    PaperSizingCellV2,
    PaperSizingDecisionV2,
    PaperSizingStatusV2,
    canonical_paper_sizing_decision_v2,
)
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveExpectedCellV2,
    canonical_prospective_census_plan_v2,
)
from signalbot.r4b_v2.execution.prospective_decision_payload import (
    FamilyEntryDecisionV2,
    ProspectiveCellDispositionPayloadV2,
    ProspectiveDecisionPreparePayloadV2,
    ProspectiveDispositionClassV2,
    canonical_prospective_cell_disposition_payload_v2,
    canonical_prospective_decision_prepare_payload_v2,
    parse_prospective_cell_disposition_payload_v2,
)
from signalbot.r4b_v2.execution.prospective_terminal_contract import (
    PROSPECTIVE_PAPER_TERMINAL_RULE_VERSION_V2,
)
from signalbot.r4b_v2.execution.prospective_wal_record import (
    PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
    ProspectiveWalRecordKindV2,
)
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2
from signalbot.r4b_v2.strategy.prospective_plan import (
    current_prospective_execution_contract_sha256_v2,
)

if TYPE_CHECKING:
    from signalbot.r4b_v2.execution.prospective_decision_owner import (
        ProspectiveDecisionTransactionResultV2,
    )


PROSPECTIVE_PAPER_TERMINAL_AUTHORITY_V2: Final = (
    "NONAUTHORITATIVE_ENTRY_ONLY_REFERENCE_MEMBERSHIP_UNPROVEN"
)

_PAYLOAD_HASH_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_PAPER_ENTRY_TERMINAL_PAYLOAD_V2\0"
_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_JCS_MAX_SAFE_INTEGER: Final = 9_007_199_254_740_991
_MAX_REASONS: Final = 64


class ProspectivePaperTerminalContractErrorV2(ValueError):
    """Raised when a prospective PAPER terminal is not exact."""


class ProspectivePaperTerminalStatusV2(StrEnum):
    """Complete, suppressed, and incomplete entry-terminal outcomes."""

    NO_SIGNAL = "NO_SIGNAL"
    SUPPRESSED_DECISION = "SUPPRESSED_DECISION"
    INCOMPLETE_DECISION = "INCOMPLETE_DECISION"
    SUPPRESSED_SIZING = "SUPPRESSED_SIZING"
    INCOMPLETE_PAPER = "INCOMPLETE_PAPER"
    PAPER_IOC_NO_FILL = "PAPER_IOC_NO_FILL"
    PAPER_CAPACITY_REJECTED = "PAPER_CAPACITY_REJECTED"
    PAPER_EXECUTED_FULL_QUANTITY = "PAPER_EXECUTED_FULL_QUANTITY"


class ProspectivePaperTerminalCompletenessV2(StrEnum):
    """Whether the sizing cell is complete, suppressed, or inconclusive."""

    COMPLETE = "COMPLETE"
    SUPPRESSED = "SUPPRESSED"
    INCOMPLETE = "INCOMPLETE"


class ProspectivePaperTerminalCostStateV2(StrEnum):
    """How much cost information this entry terminal can honestly claim."""

    ZERO_NO_POSITION = "ZERO_NO_POSITION"
    INCOMPLETE_EVIDENCE = "INCOMPLETE_EVIDENCE"
    ENTRY_SLIPPAGE_ONLY_POSITION_COSTS_DEFERRED = "ENTRY_SLIPPAGE_ONLY_POSITION_COSTS_DEFERRED"


@dataclass(frozen=True, slots=True)
class _TerminalProjectionV2:
    status: ProspectivePaperTerminalStatusV2
    completeness: ProspectivePaperTerminalCompletenessV2
    reasons: tuple[str, ...]
    invalidation: str
    sizing_status: PaperSizingStatusV2 | None
    sizing_rule_version: str | None
    sizing_sha256: str | None
    reference_evidence_sha256: str | None
    target_quote_notional_usdt: Decimal | None
    reference_price: Decimal | None
    unrounded_quantity: Decimal | None
    requested_quantity: Decimal | None
    quote_notional_at_reference: Decimal | None
    paper_status: PaperFokEntryStatusV2 | None
    paper_rule_version: str | None
    paper_decision_event_id: str | None
    paper_decision_payload_sha256: str | None
    paper_evidence_sha256: str | None
    paper_inconclusive_cause: str | None
    paper_closure_method: PaperFokClosureMethodV2 | None
    target_venue_ms: int | None
    target_state_last_ingest_seq: int | None
    certified_quantity: Decimal | None
    filled_quantity: Decimal | None
    opposite_bbo: Decimal | None
    paper_price_cap: Decimal | None
    market_take_bound_price: Decimal | None
    executable_vwap: Decimal | None
    executable_notional: Decimal | None
    full_fill_certificate_sha256: str | None
    signed_slippage_vs_reference_usdt: Decimal | None
    known_fee_cost_usdt: Decimal | None
    known_funding_cost_usdt: Decimal | None
    position_after_cost_pnl_usdt: Decimal | None
    cost_state: ProspectivePaperTerminalCostStateV2
    costs_complete: bool
    canonical_sizing_jsonl: bytes | None
    canonical_paper_decision_jsonl: bytes | None
    canonical_full_fill_certificate_jsonl: bytes | None


@dataclass(frozen=True, slots=True)
class ProspectivePaperTerminalPayloadV2:
    """One hash-bound PAPER-entry outcome for one decision/sizing cell."""

    attempt_id: str
    attempt_plan_sha256: str
    promoting_plan_sha256: str
    execution_contract_sha256: str
    segment_id: str
    cell_id: str
    family: PromotingFamilyV2
    family_rule_version: str
    symbol: str
    venue: VenueV2
    bar_open_ms: int
    bar_close_ms: int
    decision_cutoff_ms: int
    sizing_cell: PaperSizingCellV2
    prepare_payload_sha256: str
    prepare_wal_payload_sha256: str
    prepare_record_sha256: str
    disposition_payload_sha256: str
    disposition_wal_payload_sha256: str
    disposition_record_sha256: str
    decision_event_id: str
    decision_payload_sha256: str
    disposition_class: ProspectiveDispositionClassV2
    signal_side: str | None
    terminal_status: ProspectivePaperTerminalStatusV2
    completeness: ProspectivePaperTerminalCompletenessV2
    reasons: tuple[str, ...]
    invalidation: str
    sizing_status: PaperSizingStatusV2 | None
    sizing_rule_version: str | None
    sizing_sha256: str | None
    reference_evidence_sha256: str | None
    target_quote_notional_usdt: Decimal | None
    reference_price: Decimal | None
    unrounded_quantity: Decimal | None
    requested_quantity: Decimal | None
    quote_notional_at_reference: Decimal | None
    paper_status: PaperFokEntryStatusV2 | None
    paper_rule_version: str | None
    paper_decision_event_id: str | None
    paper_decision_payload_sha256: str | None
    paper_evidence_sha256: str | None
    paper_inconclusive_cause: str | None
    paper_closure_method: PaperFokClosureMethodV2 | None
    target_venue_ms: int | None
    target_state_last_ingest_seq: int | None
    certified_quantity: Decimal | None
    filled_quantity: Decimal | None
    opposite_bbo: Decimal | None
    paper_price_cap: Decimal | None
    market_take_bound_price: Decimal | None
    executable_vwap: Decimal | None
    executable_notional: Decimal | None
    full_fill_certificate_sha256: str | None
    signed_slippage_vs_reference_usdt: Decimal | None
    known_fee_cost_usdt: Decimal | None
    known_funding_cost_usdt: Decimal | None
    position_after_cost_pnl_usdt: Decimal | None
    cost_state: ProspectivePaperTerminalCostStateV2
    costs_complete: bool
    canonical_sizing_jsonl: bytes | None = field(repr=False)
    canonical_paper_decision_jsonl: bytes | None = field(repr=False)
    canonical_full_fill_certificate_jsonl: bytes | None = field(repr=False)
    _factory_token: InitVar[object | None] = None
    payload_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=PAPER_TERMINAL_PAYLOAD_SCHEMA_V2)
    protocol_rule_version: str = field(
        init=False,
        default=PROSPECTIVE_PAPER_TERMINAL_RULE_VERSION_V2,
    )
    authority_status: str = field(
        init=False,
        default=PROSPECTIVE_PAPER_TERMINAL_AUTHORITY_V2,
    )
    entry_terminal_only: bool = field(init=False, default=True)
    position_terminal: bool = field(init=False, default=False)
    position_pnl_computed: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)
    actual_private_account_fee_claim: bool = field(init=False, default=False)
    closed_candle_contract_checked: bool = field(init=False, default=True)
    causal_target_membership_authoritative: bool = field(init=False, default=False)
    sizing_reference_membership_authoritative: bool = field(init=False, default=False)
    terminal_rule_plan_bound: bool = field(init=False, default=True)
    typed_wal_replay_authoritative: bool = field(init=False, default=False)
    efficacy_eligible: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise ProspectivePaperTerminalContractErrorV2(
                "PAPER terminal payloads are factory-sealed"
            )
        _validate_payload_shape(self)
        object.__setattr__(
            self,
            "payload_sha256",
            _hash_document(_payload_document(self)),
        )


def build_prospective_paper_terminal_payload_v2(
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
    transaction: ProspectiveDecisionTransactionResultV2,
    sizing_cell: PaperSizingCellV2,
    sizing: PaperSizingDecisionV2 | None = None,
    paper_decision: PaperFokEntryDecisionV2 | None = None,
    full_fill_certificate: PaperFokFullFillCertificateV2 | None = None,
) -> ProspectivePaperTerminalPayloadV2:
    """Build a typed entry terminal from one exact durable decision result."""

    (
        prepare,
        prepare_record_sha256,
        disposition,
        disposition_record_sha256,
    ) = _transaction_sources(transaction)
    return _build_from_sources(
        plan=plan,
        cell=cell,
        prepare=prepare,
        prepare_record_sha256=prepare_record_sha256,
        disposition=disposition,
        disposition_record_sha256=disposition_record_sha256,
        sizing_cell=sizing_cell,
        sizing=sizing,
        paper_decision=paper_decision,
        full_fill_certificate=full_fill_certificate,
    )


def canonical_prospective_paper_terminal_payload_v2(
    payload: ProspectivePaperTerminalPayloadV2,
) -> bytes:
    """Serialize one self-hash-checked typed PAPER terminal."""

    if type(payload) is not ProspectivePaperTerminalPayloadV2:
        raise TypeError("payload must be exact ProspectivePaperTerminalPayloadV2")
    _validate_payload_shape(payload)
    expected = _hash_document(_payload_document(payload))
    if payload.payload_sha256 != expected:
        raise ProspectivePaperTerminalContractErrorV2(
            "PAPER terminal payload hash differs from canonical content"
        )
    return canonical_json_line(
        {**_payload_document(payload), "payload_sha256": payload.payload_sha256}
    )


def parse_prospective_paper_terminal_payload_v2(
    encoded: bytes,
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
    transaction: ProspectiveDecisionTransactionResultV2,
    sizing_cell: PaperSizingCellV2,
    sizing: PaperSizingDecisionV2 | None = None,
    paper_decision: PaperFokEntryDecisionV2 | None = None,
    full_fill_certificate: PaperFokFullFillCertificateV2 | None = None,
) -> ProspectivePaperTerminalPayloadV2:
    """Rebuild from exact sealed sources and require byte-for-byte equality.

    This parser intentionally requires the live factory-sealed transaction.
    The daily WAL currently cannot reconstruct that result after restart; typed
    store replay is therefore a separately documented authority blocker.
    """

    _decode_exact_terminal(encoded)
    expected = build_prospective_paper_terminal_payload_v2(
        plan=plan,
        cell=cell,
        transaction=transaction,
        sizing_cell=sizing_cell,
        sizing=sizing,
        paper_decision=paper_decision,
        full_fill_certificate=full_fill_certificate,
    )
    if canonical_prospective_paper_terminal_payload_v2(expected) != encoded:
        raise ProspectivePaperTerminalContractErrorV2(
            "stored PAPER terminal differs from its exact typed sources"
        )
    return expected


def _transaction_sources(
    transaction: ProspectiveDecisionTransactionResultV2,
) -> tuple[
    ProspectiveDecisionPreparePayloadV2,
    str,
    ProspectiveCellDispositionPayloadV2,
    str,
]:
    # Local import keeps the future store -> terminal replay dependency acyclic.
    from signalbot.r4b_v2.execution.prospective_decision_owner import (
        ProspectiveDecisionTransactionResultV2,
    )

    if type(transaction) is not ProspectiveDecisionTransactionResultV2:
        raise ProspectivePaperTerminalContractErrorV2(
            "transaction must be an exact factory-sealed decision result"
        )
    prepare = transaction.prepare_payload
    disposition = transaction.disposition_payload
    prepare_jsonl = canonical_prospective_decision_prepare_payload_v2(prepare)
    disposition_jsonl = canonical_prospective_cell_disposition_payload_v2(disposition)
    if transaction.decision != prepare.decision:
        raise ProspectivePaperTerminalContractErrorV2(
            "transaction decision differs from its durable PREPARE"
        )
    if transaction.paper_fok_evaluated or transaction.production_order_placement:
        raise ProspectivePaperTerminalContractErrorV2(
            "decision transaction cannot pre-claim PAPER execution or an order"
        )
    prepare_receipt = transaction.prepare_durable_receipt
    disposition_receipt = transaction.disposition_durable_receipt
    if len(prepare_receipt.records) != 1 or len(disposition_receipt.records) != 1:
        raise ProspectivePaperTerminalContractErrorV2(
            "decision transaction durable receipts must each contain one record"
        )
    prepare_record = prepare_receipt.records[0]
    disposition_record = disposition_receipt.records[0]
    if (
        prepare_record.kind is not ProspectiveWalRecordKindV2.DECISION_PREPARE
        or disposition_record.kind is not ProspectiveWalRecordKindV2.CELL_DISPOSITION
        or prepare_record.ingest_seq + 1 != disposition_record.ingest_seq
        or disposition_receipt.attempt_plan_sha256 != prepare_receipt.attempt_plan_sha256
        or disposition_receipt.segment_id != prepare_receipt.segment_id
    ):
        raise ProspectivePaperTerminalContractErrorV2(
            "decision transaction WAL records are not one adjacent typed transition"
        )
    if (
        prepare_record.payload_sha256 != hashlib.sha256(prepare_jsonl).hexdigest()
        or disposition_record.payload_sha256 != hashlib.sha256(disposition_jsonl).hexdigest()
    ):
        raise ProspectivePaperTerminalContractErrorV2(
            "durable decision record payload hashes differ from typed bytes"
        )
    parse_prospective_cell_disposition_payload_v2(
        disposition_jsonl,
        prepare=prepare,
        prepare_record_sha256=prepare_record.record_sha256,
    )
    return (
        prepare,
        prepare_record.record_sha256,
        disposition,
        disposition_record.record_sha256,
    )


def _build_from_sources(
    *,
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
    prepare: ProspectiveDecisionPreparePayloadV2,
    prepare_record_sha256: str,
    disposition: ProspectiveCellDispositionPayloadV2,
    disposition_record_sha256: str,
    sizing_cell: PaperSizingCellV2,
    sizing: PaperSizingDecisionV2 | None,
    paper_decision: PaperFokEntryDecisionV2 | None,
    full_fill_certificate: PaperFokFullFillCertificateV2 | None,
) -> ProspectivePaperTerminalPayloadV2:
    exact_cell = _exact_current_plan_cell(plan, cell)
    if not isinstance(sizing_cell, PaperSizingCellV2):
        raise ProspectivePaperTerminalContractErrorV2("sizing_cell must be PaperSizingCellV2")
    prepare_jsonl = canonical_prospective_decision_prepare_payload_v2(prepare)
    disposition_jsonl = canonical_prospective_cell_disposition_payload_v2(disposition)
    _require_sha256(prepare_record_sha256, "prepare_record_sha256")
    _require_sha256(disposition_record_sha256, "disposition_record_sha256")
    parse_prospective_cell_disposition_payload_v2(
        disposition_jsonl,
        prepare=prepare,
        prepare_record_sha256=prepare_record_sha256,
    )
    if (
        prepare.attempt_id,
        prepare.attempt_plan_sha256,
        prepare.promoting_plan_sha256,
        prepare.segment_id,
        prepare.cell_id,
        prepare.family,
        prepare.rule_version,
        prepare.symbol,
        prepare.venue,
        prepare.bar_open_ms,
        prepare.bar_close_ms,
        prepare.decision_cutoff_ms,
    ) != (
        exact_cell.attempt_id,
        plan.plan_sha256,
        plan.promoting_plan_sha256,
        exact_cell.segment_id,
        exact_cell.cell_id,
        exact_cell.family,
        exact_cell.rule_version,
        exact_cell.symbol,
        VenueV2.USDM_FUTURES,
        exact_cell.bar_open_ms,
        exact_cell.bar_close_ms,
        exact_cell.decision_cutoff_ms,
    ):
        raise ProspectivePaperTerminalContractErrorV2(
            "typed decision transition differs from the exact frozen cell"
        )

    decision = prepare.decision
    projection = _project_terminal(
        disposition=disposition,
        decision=decision,
        sizing_cell=sizing_cell,
        sizing=sizing,
        paper_decision=paper_decision,
        full_fill_certificate=full_fill_certificate,
    )
    payload = ProspectivePaperTerminalPayloadV2(
        attempt_id=prepare.attempt_id,
        attempt_plan_sha256=prepare.attempt_plan_sha256,
        promoting_plan_sha256=prepare.promoting_plan_sha256,
        execution_contract_sha256=plan.execution_contract_sha256,
        segment_id=prepare.segment_id,
        cell_id=prepare.cell_id,
        family=prepare.family,
        family_rule_version=prepare.rule_version,
        symbol=prepare.symbol,
        venue=prepare.venue,
        bar_open_ms=prepare.bar_open_ms,
        bar_close_ms=prepare.bar_close_ms,
        decision_cutoff_ms=prepare.decision_cutoff_ms,
        sizing_cell=sizing_cell,
        prepare_payload_sha256=prepare.payload_sha256,
        prepare_wal_payload_sha256=hashlib.sha256(prepare_jsonl).hexdigest(),
        prepare_record_sha256=prepare_record_sha256,
        disposition_payload_sha256=disposition.payload_sha256,
        disposition_wal_payload_sha256=hashlib.sha256(disposition_jsonl).hexdigest(),
        disposition_record_sha256=disposition_record_sha256,
        decision_event_id=prepare.decision_event_id,
        decision_payload_sha256=prepare.decision_payload_sha256,
        disposition_class=prepare.disposition_class,
        signal_side=prepare.signal_side,
        terminal_status=projection.status,
        completeness=projection.completeness,
        reasons=projection.reasons,
        invalidation=projection.invalidation,
        sizing_status=projection.sizing_status,
        sizing_rule_version=projection.sizing_rule_version,
        sizing_sha256=projection.sizing_sha256,
        reference_evidence_sha256=projection.reference_evidence_sha256,
        target_quote_notional_usdt=projection.target_quote_notional_usdt,
        reference_price=projection.reference_price,
        unrounded_quantity=projection.unrounded_quantity,
        requested_quantity=projection.requested_quantity,
        quote_notional_at_reference=projection.quote_notional_at_reference,
        paper_status=projection.paper_status,
        paper_rule_version=projection.paper_rule_version,
        paper_decision_event_id=projection.paper_decision_event_id,
        paper_decision_payload_sha256=projection.paper_decision_payload_sha256,
        paper_evidence_sha256=projection.paper_evidence_sha256,
        paper_inconclusive_cause=projection.paper_inconclusive_cause,
        paper_closure_method=projection.paper_closure_method,
        target_venue_ms=projection.target_venue_ms,
        target_state_last_ingest_seq=projection.target_state_last_ingest_seq,
        certified_quantity=projection.certified_quantity,
        filled_quantity=projection.filled_quantity,
        opposite_bbo=projection.opposite_bbo,
        paper_price_cap=projection.paper_price_cap,
        market_take_bound_price=projection.market_take_bound_price,
        executable_vwap=projection.executable_vwap,
        executable_notional=projection.executable_notional,
        full_fill_certificate_sha256=projection.full_fill_certificate_sha256,
        signed_slippage_vs_reference_usdt=(projection.signed_slippage_vs_reference_usdt),
        known_fee_cost_usdt=projection.known_fee_cost_usdt,
        known_funding_cost_usdt=projection.known_funding_cost_usdt,
        position_after_cost_pnl_usdt=projection.position_after_cost_pnl_usdt,
        cost_state=projection.cost_state,
        costs_complete=projection.costs_complete,
        canonical_sizing_jsonl=projection.canonical_sizing_jsonl,
        canonical_paper_decision_jsonl=projection.canonical_paper_decision_jsonl,
        canonical_full_fill_certificate_jsonl=(projection.canonical_full_fill_certificate_jsonl),
        _factory_token=_FACTORY_TOKEN,
    )
    return payload


def _exact_current_plan_cell(
    plan: ProspectiveCensusPlanV2,
    cell: ProspectiveExpectedCellV2,
) -> ProspectiveExpectedCellV2:
    canonical_prospective_census_plan_v2(plan)
    if plan.execution_contract_sha256 != current_prospective_execution_contract_sha256_v2():
        raise ProspectivePaperTerminalContractErrorV2(
            "plan does not bind the current prospective execution contract"
        )
    if plan.paper_fok_rule_version != PAPER_FOK_RULE_VERSION_V2:
        raise ProspectivePaperTerminalContractErrorV2(
            "plan does not bind the current PAPER FOK rule"
        )
    if type(cell) is not ProspectiveExpectedCellV2:
        raise ProspectivePaperTerminalContractErrorV2(
            "cell must be exact ProspectiveExpectedCellV2"
        )
    if cell.attempt_plan_sha256 != plan.plan_sha256:
        raise ProspectivePaperTerminalContractErrorV2("cell targets a foreign prospective plan")
    try:
        exact = plan.expected_cell(
            family=cell.family,
            symbol=cell.symbol,
            bar_open_ms=cell.bar_open_ms,
        )
    except ValueError as error:
        raise ProspectivePaperTerminalContractErrorV2(
            "cell is outside the frozen prospective census"
        ) from error
    if exact != cell:
        raise ProspectivePaperTerminalContractErrorV2(
            "cell differs from its exact frozen census identity"
        )
    return exact


def _project_terminal(
    *,
    disposition: ProspectiveCellDispositionPayloadV2,
    decision: FamilyEntryDecisionV2,
    sizing_cell: PaperSizingCellV2,
    sizing: PaperSizingDecisionV2 | None,
    paper_decision: PaperFokEntryDecisionV2 | None,
    full_fill_certificate: PaperFokFullFillCertificateV2 | None,
) -> _TerminalProjectionV2:
    decision_reasons = _source_reasons(decision.reasons, "family decision reasons")
    decision_invalidation = _identity(decision.invalidation, "family decision invalidation")
    if disposition.disposition_class is ProspectiveDispositionClassV2.NO_SIGNAL:
        _forbid_execution_sources(sizing, paper_decision, full_fill_certificate)
        return _no_position_projection(
            status=ProspectivePaperTerminalStatusV2.NO_SIGNAL,
            completeness=ProspectivePaperTerminalCompletenessV2.COMPLETE,
            reasons=decision_reasons,
            invalidation=decision_invalidation,
        )
    if disposition.disposition_class is ProspectiveDispositionClassV2.SUPPRESSED:
        _forbid_execution_sources(sizing, paper_decision, full_fill_certificate)
        return _no_position_projection(
            status=ProspectivePaperTerminalStatusV2.SUPPRESSED_DECISION,
            completeness=ProspectivePaperTerminalCompletenessV2.SUPPRESSED,
            reasons=decision_reasons,
            invalidation=decision_invalidation,
        )
    if disposition.disposition_class is ProspectiveDispositionClassV2.INCONCLUSIVE:
        _forbid_execution_sources(sizing, paper_decision, full_fill_certificate)
        return _incomplete_projection(
            status=ProspectivePaperTerminalStatusV2.INCOMPLETE_DECISION,
            reasons=decision_reasons,
            invalidation=decision_invalidation,
        )
    if disposition.disposition_class is not ProspectiveDispositionClassV2.SIGNAL:
        raise ProspectivePaperTerminalContractErrorV2("disposition has no terminal mapping")
    if type(sizing) is not PaperSizingDecisionV2:
        raise ProspectivePaperTerminalContractErrorV2(
            "SIGNAL disposition requires exact PAPER sizing evidence"
        )
    sizing_jsonl = canonical_paper_sizing_decision_v2(sizing)
    if sizing.sizing_cell is not sizing_cell:
        raise ProspectivePaperTerminalContractErrorV2(
            "PAPER sizing evidence targets a different sizing cell"
        )
    if sizing.status is not PaperSizingStatusV2.READY:
        if paper_decision is not None or full_fill_certificate is not None:
            raise ProspectivePaperTerminalContractErrorV2(
                "non-READY sizing forbids PAPER evaluation evidence"
            )
        return _sizing_suppressed_projection(
            decision_reasons=decision_reasons,
            sizing=sizing,
            sizing_jsonl=sizing_jsonl,
        )
    if type(paper_decision) is not PaperFokEntryDecisionV2:
        raise ProspectivePaperTerminalContractErrorV2(
            "READY sizing requires an exact PAPER FOK decision"
        )
    paper_jsonl = canonical_paper_fok_entry_decision_v2(paper_decision)
    _validate_paper_binding(disposition, decision, sizing, paper_decision)
    if paper_decision.status is PaperFokEntryStatusV2.CLOSURE_PENDING:
        raise ProspectivePaperTerminalContractErrorV2("CLOSURE_PENDING is not a PAPER terminal")
    certificate_jsonl, certificate_sha256 = _validate_certificate(
        paper_decision,
        full_fill_certificate,
    )
    paper_reasons = _source_reasons(paper_decision.reasons, "PAPER reasons")
    paper_invalidation = _identity(paper_decision.invalidation, "PAPER invalidation")
    status, completeness, cost_state, costs_complete = _classify_paper_status(paper_decision.status)
    if cost_state is ProspectivePaperTerminalCostStateV2.ZERO_NO_POSITION:
        fee_cost = Decimal(0)
        funding_cost = Decimal(0)
    else:
        fee_cost = None
        funding_cost = None
    slippage = _signed_slippage(sizing, paper_decision)
    return _TerminalProjectionV2(
        status=status,
        completeness=completeness,
        reasons=_merge_reasons(decision_reasons, (sizing.reason,), paper_reasons),
        invalidation=paper_invalidation,
        sizing_status=sizing.status,
        sizing_rule_version=sizing.rule_version,
        sizing_sha256=sizing.sizing_sha256,
        reference_evidence_sha256=sizing.reference_evidence_sha256,
        target_quote_notional_usdt=sizing.target_quote_notional_usdt,
        reference_price=sizing.reference_price,
        unrounded_quantity=sizing.unrounded_quantity,
        requested_quantity=sizing.requested_quantity,
        quote_notional_at_reference=sizing.quote_notional_at_reference,
        paper_status=paper_decision.status,
        paper_rule_version=paper_decision.rule_version,
        paper_decision_event_id=paper_decision.event_id,
        paper_decision_payload_sha256=paper_decision.payload_sha256,
        paper_evidence_sha256=paper_decision.evidence_sha256,
        paper_inconclusive_cause=(
            None
            if paper_decision.inconclusive_cause is None
            else paper_decision.inconclusive_cause.value
        ),
        paper_closure_method=paper_decision.closure_method,
        target_venue_ms=paper_decision.target_venue_ms,
        target_state_last_ingest_seq=paper_decision.target_state_last_ingest_seq,
        certified_quantity=paper_decision.certified_quantity,
        filled_quantity=paper_decision.filled_quantity,
        opposite_bbo=paper_decision.opposite_bbo,
        paper_price_cap=paper_decision.paper_price_cap,
        market_take_bound_price=paper_decision.market_take_bound_price,
        executable_vwap=paper_decision.executable_vwap,
        executable_notional=paper_decision.executable_notional,
        full_fill_certificate_sha256=certificate_sha256,
        signed_slippage_vs_reference_usdt=slippage,
        known_fee_cost_usdt=fee_cost,
        known_funding_cost_usdt=funding_cost,
        position_after_cost_pnl_usdt=None,
        cost_state=cost_state,
        costs_complete=costs_complete,
        canonical_sizing_jsonl=sizing_jsonl,
        canonical_paper_decision_jsonl=paper_jsonl,
        canonical_full_fill_certificate_jsonl=certificate_jsonl,
    )


def _validate_paper_binding(
    disposition: ProspectiveCellDispositionPayloadV2,
    decision: FamilyEntryDecisionV2,
    sizing: PaperSizingDecisionV2,
    paper: PaperFokEntryDecisionV2,
) -> None:
    if disposition.signal_side == "LONG":
        expected_side = PaperFokSideV2.BUY
    elif disposition.signal_side == "SHORT":
        expected_side = PaperFokSideV2.SELL
    else:
        raise ProspectivePaperTerminalContractErrorV2("SIGNAL disposition has no concrete side")
    if (
        paper.attempt_id,
        paper.signal_event_id,
        paper.symbol,
        paper.venue,
        paper.promoting_plan_sha256,
        paper.bar_open_ms,
        paper.bar_close_ms,
        paper.decision_cutoff_ms,
        paper.side,
        paper.requested_quantity,
    ) != (
        decision.attempt_id,
        decision.event_id,
        decision.symbol,
        decision.venue,
        decision.promoting_plan_sha256,
        decision.bar_open_ms,
        decision.bar_close_ms,
        decision.decision_cutoff_ms,
        expected_side,
        sizing.requested_quantity,
    ):
        raise ProspectivePaperTerminalContractErrorV2(
            "PAPER decision differs from the exact signal/sizing identity"
        )
    if paper.rule_version != PAPER_FOK_RULE_VERSION_V2:
        raise ProspectivePaperTerminalContractErrorV2("PAPER decision uses a foreign rule version")


def _validate_certificate(
    paper: PaperFokEntryDecisionV2,
    certificate: PaperFokFullFillCertificateV2 | None,
) -> tuple[bytes | None, str | None]:
    if paper.status is not PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY:
        if certificate is not None:
            raise ProspectivePaperTerminalContractErrorV2(
                "non-fill PAPER status forbids a full-fill certificate"
            )
        return None, None
    if type(certificate) is not PaperFokFullFillCertificateV2:
        raise ProspectivePaperTerminalContractErrorV2(
            "full PAPER fill requires a registry-issued certificate"
        )
    encoded = canonical_paper_fok_full_fill_certificate_v2(certificate)
    if (
        certificate.attempt_id,
        certificate.signal_event_id,
        certificate.decision_event_id,
        certificate.decision_payload_sha256,
        certificate.evidence_sha256,
        certificate.symbol,
        certificate.venue,
        certificate.promoting_plan_sha256,
        certificate.target_cursor,
        certificate.side,
        certificate.requested_quantity,
        certificate.filled_quantity,
        certificate.executable_vwap,
        certificate.executable_notional,
    ) != (
        paper.attempt_id,
        paper.signal_event_id,
        paper.event_id,
        paper.payload_sha256,
        paper.evidence_sha256,
        paper.symbol,
        paper.venue,
        paper.promoting_plan_sha256,
        paper.target_cursor,
        paper.side,
        paper.requested_quantity,
        paper.filled_quantity,
        paper.executable_vwap,
        paper.executable_notional,
    ):
        raise ProspectivePaperTerminalContractErrorV2(
            "full-fill certificate differs from the PAPER decision"
        )
    return encoded, certificate.certificate_sha256


def _classify_paper_status(
    status: PaperFokEntryStatusV2,
) -> tuple[
    ProspectivePaperTerminalStatusV2,
    ProspectivePaperTerminalCompletenessV2,
    ProspectivePaperTerminalCostStateV2,
    bool,
]:
    if status in (
        PaperFokEntryStatusV2.INCONCLUSIVE_DATA,
        PaperFokEntryStatusV2.INCONCLUSIVE_FILTER,
    ):
        return (
            ProspectivePaperTerminalStatusV2.INCOMPLETE_PAPER,
            ProspectivePaperTerminalCompletenessV2.INCOMPLETE,
            ProspectivePaperTerminalCostStateV2.INCOMPLETE_EVIDENCE,
            False,
        )
    if status is PaperFokEntryStatusV2.ADMITTED_PAPER_IOC_NO_FILL:
        return (
            ProspectivePaperTerminalStatusV2.PAPER_IOC_NO_FILL,
            ProspectivePaperTerminalCompletenessV2.COMPLETE,
            ProspectivePaperTerminalCostStateV2.ZERO_NO_POSITION,
            True,
        )
    if status is PaperFokEntryStatusV2.NOT_ADMITTED_PAPER_CAPACITY:
        return (
            ProspectivePaperTerminalStatusV2.PAPER_CAPACITY_REJECTED,
            ProspectivePaperTerminalCompletenessV2.SUPPRESSED,
            ProspectivePaperTerminalCostStateV2.ZERO_NO_POSITION,
            True,
        )
    if status is PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY:
        return (
            ProspectivePaperTerminalStatusV2.PAPER_EXECUTED_FULL_QUANTITY,
            ProspectivePaperTerminalCompletenessV2.COMPLETE,
            (ProspectivePaperTerminalCostStateV2.ENTRY_SLIPPAGE_ONLY_POSITION_COSTS_DEFERRED),
            False,
        )
    raise ProspectivePaperTerminalContractErrorV2("PAPER status has no frozen terminal mapping")


def _signed_slippage(
    sizing: PaperSizingDecisionV2,
    paper: PaperFokEntryDecisionV2,
) -> Decimal | None:
    if paper.status is not PaperFokEntryStatusV2.ADMITTED_EXECUTED_FULL_QUANTITY:
        return None
    if sizing.quote_notional_at_reference is None or paper.executable_notional is None:
        raise ProspectivePaperTerminalContractErrorV2(
            "full PAPER fill lacks a reference or executable notional"
        )
    with localcontext(protocol_decimal_context_v2()):
        if paper.side is PaperFokSideV2.BUY:
            return paper.executable_notional - sizing.quote_notional_at_reference
        return sizing.quote_notional_at_reference - paper.executable_notional


def _no_position_projection(
    *,
    status: ProspectivePaperTerminalStatusV2,
    completeness: ProspectivePaperTerminalCompletenessV2,
    reasons: tuple[str, ...],
    invalidation: str,
) -> _TerminalProjectionV2:
    return _empty_projection(
        status=status,
        completeness=completeness,
        reasons=reasons,
        invalidation=invalidation,
        cost_state=ProspectivePaperTerminalCostStateV2.ZERO_NO_POSITION,
        costs_complete=True,
        fee_cost=Decimal(0),
        funding_cost=Decimal(0),
    )


def _incomplete_projection(
    *,
    status: ProspectivePaperTerminalStatusV2,
    reasons: tuple[str, ...],
    invalidation: str,
) -> _TerminalProjectionV2:
    return _empty_projection(
        status=status,
        completeness=ProspectivePaperTerminalCompletenessV2.INCOMPLETE,
        reasons=reasons,
        invalidation=invalidation,
        cost_state=ProspectivePaperTerminalCostStateV2.INCOMPLETE_EVIDENCE,
        costs_complete=False,
        fee_cost=None,
        funding_cost=None,
    )


def _empty_projection(
    *,
    status: ProspectivePaperTerminalStatusV2,
    completeness: ProspectivePaperTerminalCompletenessV2,
    reasons: tuple[str, ...],
    invalidation: str,
    cost_state: ProspectivePaperTerminalCostStateV2,
    costs_complete: bool,
    fee_cost: Decimal | None,
    funding_cost: Decimal | None,
) -> _TerminalProjectionV2:
    return _TerminalProjectionV2(
        status=status,
        completeness=completeness,
        reasons=reasons,
        invalidation=invalidation,
        sizing_status=None,
        sizing_rule_version=None,
        sizing_sha256=None,
        reference_evidence_sha256=None,
        target_quote_notional_usdt=None,
        reference_price=None,
        unrounded_quantity=None,
        requested_quantity=None,
        quote_notional_at_reference=None,
        paper_status=None,
        paper_rule_version=None,
        paper_decision_event_id=None,
        paper_decision_payload_sha256=None,
        paper_evidence_sha256=None,
        paper_inconclusive_cause=None,
        paper_closure_method=None,
        target_venue_ms=None,
        target_state_last_ingest_seq=None,
        certified_quantity=None,
        filled_quantity=None,
        opposite_bbo=None,
        paper_price_cap=None,
        market_take_bound_price=None,
        executable_vwap=None,
        executable_notional=None,
        full_fill_certificate_sha256=None,
        signed_slippage_vs_reference_usdt=(
            Decimal(0)
            if cost_state is ProspectivePaperTerminalCostStateV2.ZERO_NO_POSITION
            else None
        ),
        known_fee_cost_usdt=fee_cost,
        known_funding_cost_usdt=funding_cost,
        position_after_cost_pnl_usdt=None,
        cost_state=cost_state,
        costs_complete=costs_complete,
        canonical_sizing_jsonl=None,
        canonical_paper_decision_jsonl=None,
        canonical_full_fill_certificate_jsonl=None,
    )


def _sizing_suppressed_projection(
    *,
    decision_reasons: tuple[str, ...],
    sizing: PaperSizingDecisionV2,
    sizing_jsonl: bytes,
) -> _TerminalProjectionV2:
    return _TerminalProjectionV2(
        status=ProspectivePaperTerminalStatusV2.SUPPRESSED_SIZING,
        completeness=ProspectivePaperTerminalCompletenessV2.SUPPRESSED,
        reasons=_merge_reasons(decision_reasons, (sizing.reason,)),
        invalidation=sizing.reason,
        sizing_status=sizing.status,
        sizing_rule_version=sizing.rule_version,
        sizing_sha256=sizing.sizing_sha256,
        reference_evidence_sha256=sizing.reference_evidence_sha256,
        target_quote_notional_usdt=sizing.target_quote_notional_usdt,
        reference_price=sizing.reference_price,
        unrounded_quantity=sizing.unrounded_quantity,
        requested_quantity=None,
        quote_notional_at_reference=None,
        paper_status=None,
        paper_rule_version=None,
        paper_decision_event_id=None,
        paper_decision_payload_sha256=None,
        paper_evidence_sha256=None,
        paper_inconclusive_cause=None,
        paper_closure_method=None,
        target_venue_ms=None,
        target_state_last_ingest_seq=None,
        certified_quantity=None,
        filled_quantity=None,
        opposite_bbo=None,
        paper_price_cap=None,
        market_take_bound_price=None,
        executable_vwap=None,
        executable_notional=None,
        full_fill_certificate_sha256=None,
        signed_slippage_vs_reference_usdt=Decimal(0),
        known_fee_cost_usdt=Decimal(0),
        known_funding_cost_usdt=Decimal(0),
        position_after_cost_pnl_usdt=None,
        cost_state=ProspectivePaperTerminalCostStateV2.ZERO_NO_POSITION,
        costs_complete=True,
        canonical_sizing_jsonl=sizing_jsonl,
        canonical_paper_decision_jsonl=None,
        canonical_full_fill_certificate_jsonl=None,
    )


def _forbid_execution_sources(
    sizing: PaperSizingDecisionV2 | None,
    paper_decision: PaperFokEntryDecisionV2 | None,
    certificate: PaperFokFullFillCertificateV2 | None,
) -> None:
    if any(value is not None for value in (sizing, paper_decision, certificate)):
        raise ProspectivePaperTerminalContractErrorV2(
            "non-SIGNAL dispositions forbid sizing or PAPER evidence"
        )


def _merge_reasons(*groups: tuple[str, ...]) -> tuple[str, ...]:
    merged = tuple(value for group in groups for value in group)
    return _source_reasons(merged, "terminal reasons")


def _source_reasons(values: tuple[str, ...], field_name: str) -> tuple[str, ...]:
    if type(values) is not tuple or not values or len(values) > _MAX_REASONS:
        raise ProspectivePaperTerminalContractErrorV2(
            f"{field_name} must be a non-empty bounded tuple"
        )
    for value in values:
        _identity(value, field_name)
    return values


def _validate_payload_shape(payload: ProspectivePaperTerminalPayloadV2) -> None:
    for value, field_name in (
        (payload.attempt_plan_sha256, "attempt_plan_sha256"),
        (payload.promoting_plan_sha256, "promoting_plan_sha256"),
        (payload.execution_contract_sha256, "execution_contract_sha256"),
        (payload.segment_id, "segment_id"),
        (payload.cell_id, "cell_id"),
        (payload.prepare_payload_sha256, "prepare_payload_sha256"),
        (payload.prepare_wal_payload_sha256, "prepare_wal_payload_sha256"),
        (payload.prepare_record_sha256, "prepare_record_sha256"),
        (payload.disposition_payload_sha256, "disposition_payload_sha256"),
        (payload.disposition_wal_payload_sha256, "disposition_wal_payload_sha256"),
        (payload.disposition_record_sha256, "disposition_record_sha256"),
        (payload.decision_event_id, "decision_event_id"),
        (payload.decision_payload_sha256, "decision_payload_sha256"),
    ):
        _require_sha256(value, field_name)
    _identity(payload.attempt_id, "attempt_id")
    _identity(payload.family_rule_version, "family_rule_version")
    _identity(payload.invalidation, "invalidation")
    if not isinstance(payload.family, PromotingFamilyV2):
        raise ProspectivePaperTerminalContractErrorV2("family must be PromotingFamilyV2")
    if payload.venue is not VenueV2.USDM_FUTURES:
        raise ProspectivePaperTerminalContractErrorV2("PAPER terminal venue must be USD-M Futures")
    if not isinstance(payload.sizing_cell, PaperSizingCellV2):
        raise ProspectivePaperTerminalContractErrorV2("sizing_cell must be PaperSizingCellV2")
    if not isinstance(payload.disposition_class, ProspectiveDispositionClassV2):
        raise ProspectivePaperTerminalContractErrorV2(
            "disposition_class must be ProspectiveDispositionClassV2"
        )
    if not isinstance(payload.terminal_status, ProspectivePaperTerminalStatusV2):
        raise ProspectivePaperTerminalContractErrorV2(
            "terminal_status must be ProspectivePaperTerminalStatusV2"
        )
    if not isinstance(payload.completeness, ProspectivePaperTerminalCompletenessV2):
        raise ProspectivePaperTerminalContractErrorV2(
            "completeness must be ProspectivePaperTerminalCompletenessV2"
        )
    if not isinstance(payload.cost_state, ProspectivePaperTerminalCostStateV2):
        raise ProspectivePaperTerminalContractErrorV2(
            "cost_state must be ProspectivePaperTerminalCostStateV2"
        )
    _source_reasons(payload.reasons, "terminal reasons")
    if not isinstance(payload.symbol, str) or _SYMBOL_RE.fullmatch(payload.symbol) is None:
        raise ProspectivePaperTerminalContractErrorV2("symbol must be a normalized USDT symbol")
    for value, field_name in (
        (payload.bar_open_ms, "bar_open_ms"),
        (payload.bar_close_ms, "bar_close_ms"),
        (payload.decision_cutoff_ms, "decision_cutoff_ms"),
    ):
        _safe_nonnegative_integer(value, field_name)
    for value, field_name in (
        (payload.target_venue_ms, "target_venue_ms"),
        (payload.target_state_last_ingest_seq, "target_state_last_ingest_seq"),
    ):
        if value is not None:
            _safe_nonnegative_integer(value, field_name)
    for value, field_name in (
        (payload.sizing_sha256, "sizing_sha256"),
        (payload.reference_evidence_sha256, "reference_evidence_sha256"),
        (payload.paper_decision_event_id, "paper_decision_event_id"),
        (payload.paper_decision_payload_sha256, "paper_decision_payload_sha256"),
        (payload.paper_evidence_sha256, "paper_evidence_sha256"),
        (payload.full_fill_certificate_sha256, "full_fill_certificate_sha256"),
    ):
        if value is not None:
            _require_sha256(value, field_name)
    for value, field_name in (
        (payload.target_quote_notional_usdt, "target_quote_notional_usdt"),
        (payload.reference_price, "reference_price"),
        (payload.unrounded_quantity, "unrounded_quantity"),
        (payload.requested_quantity, "requested_quantity"),
        (payload.quote_notional_at_reference, "quote_notional_at_reference"),
        (payload.certified_quantity, "certified_quantity"),
        (payload.filled_quantity, "filled_quantity"),
        (payload.opposite_bbo, "opposite_bbo"),
        (payload.paper_price_cap, "paper_price_cap"),
        (payload.market_take_bound_price, "market_take_bound_price"),
        (payload.executable_vwap, "executable_vwap"),
        (payload.executable_notional, "executable_notional"),
        (payload.known_fee_cost_usdt, "known_fee_cost_usdt"),
        (payload.known_funding_cost_usdt, "known_funding_cost_usdt"),
        (payload.position_after_cost_pnl_usdt, "position_after_cost_pnl_usdt"),
    ):
        if value is not None:
            _finite_decimal(value, field_name, allow_negative=False)
    if payload.signed_slippage_vs_reference_usdt is not None:
        _finite_decimal(
            payload.signed_slippage_vs_reference_usdt,
            "signed_slippage_vs_reference_usdt",
            allow_negative=True,
        )
    _validate_source_presence(payload)
    _validate_cost_shape(payload)
    if payload.schema_version != PAPER_TERMINAL_PAYLOAD_SCHEMA_V2:
        raise ProspectivePaperTerminalContractErrorV2("unsupported PAPER terminal schema")
    if payload.protocol_rule_version != PROSPECTIVE_PAPER_TERMINAL_RULE_VERSION_V2:
        raise ProspectivePaperTerminalContractErrorV2("PAPER terminal rule version differs")
    if payload.authority_status != PROSPECTIVE_PAPER_TERMINAL_AUTHORITY_V2:
        raise ProspectivePaperTerminalContractErrorV2("PAPER terminal authority label differs")
    expected_flags = (
        payload.entry_terminal_only,
        payload.position_terminal,
        payload.position_pnl_computed,
        payload.production_order_placement,
        payload.actual_private_account_fee_claim,
        payload.closed_candle_contract_checked,
        payload.causal_target_membership_authoritative,
        payload.sizing_reference_membership_authoritative,
        payload.terminal_rule_plan_bound,
        payload.typed_wal_replay_authoritative,
        payload.efficacy_eligible,
    )
    if expected_flags != (
        True,
        False,
        False,
        False,
        False,
        True,
        False,
        False,
        True,
        False,
        False,
    ):
        raise ProspectivePaperTerminalContractErrorV2(
            "PAPER terminal safety or authority flags differ"
        )


def _validate_source_presence(payload: ProspectivePaperTerminalPayloadV2) -> None:
    if payload.canonical_sizing_jsonl is None:
        if any(
            value is not None
            for value in (
                payload.sizing_status,
                payload.sizing_rule_version,
                payload.sizing_sha256,
                payload.reference_evidence_sha256,
                payload.target_quote_notional_usdt,
                payload.reference_price,
                payload.unrounded_quantity,
                payload.requested_quantity,
                payload.quote_notional_at_reference,
            )
        ):
            raise ProspectivePaperTerminalContractErrorV2(
                "absent sizing bytes forbid sizing projections"
            )
    else:
        _source_object(payload.canonical_sizing_jsonl, "PAPER sizing")
        if payload.sizing_status is None or payload.sizing_sha256 is None:
            raise ProspectivePaperTerminalContractErrorV2(
                "sizing bytes require typed sizing projections"
            )
    if payload.canonical_paper_decision_jsonl is None:
        if any(
            value is not None
            for value in (
                payload.paper_status,
                payload.paper_rule_version,
                payload.paper_decision_event_id,
                payload.paper_decision_payload_sha256,
                payload.paper_evidence_sha256,
                payload.paper_inconclusive_cause,
                payload.paper_closure_method,
                payload.target_venue_ms,
                payload.target_state_last_ingest_seq,
                payload.certified_quantity,
                payload.filled_quantity,
                payload.opposite_bbo,
                payload.paper_price_cap,
                payload.market_take_bound_price,
                payload.executable_vwap,
                payload.executable_notional,
            )
        ):
            raise ProspectivePaperTerminalContractErrorV2(
                "absent PAPER bytes forbid PAPER projections"
            )
    else:
        _source_object(payload.canonical_paper_decision_jsonl, "PAPER decision")
        if payload.paper_status is None or payload.paper_decision_event_id is None:
            raise ProspectivePaperTerminalContractErrorV2(
                "PAPER bytes require typed decision projections"
            )
    if payload.canonical_full_fill_certificate_jsonl is None:
        if payload.full_fill_certificate_sha256 is not None:
            raise ProspectivePaperTerminalContractErrorV2(
                "absent certificate bytes forbid a certificate hash"
            )
    else:
        _source_object(
            payload.canonical_full_fill_certificate_jsonl,
            "full-fill certificate",
        )
        if payload.full_fill_certificate_sha256 is None:
            raise ProspectivePaperTerminalContractErrorV2(
                "certificate bytes require a certificate hash"
            )


def _validate_cost_shape(payload: ProspectivePaperTerminalPayloadV2) -> None:
    if type(payload.costs_complete) is not bool:
        raise ProspectivePaperTerminalContractErrorV2("costs_complete must be boolean")
    if payload.position_after_cost_pnl_usdt is not None:
        raise ProspectivePaperTerminalContractErrorV2(
            "entry terminal cannot claim position after-cost PnL"
        )
    if payload.cost_state is ProspectivePaperTerminalCostStateV2.ZERO_NO_POSITION:
        if (
            not payload.costs_complete
            or payload.signed_slippage_vs_reference_usdt != 0
            or payload.known_fee_cost_usdt != 0
            or payload.known_funding_cost_usdt != 0
        ):
            raise ProspectivePaperTerminalContractErrorV2(
                "ZERO_NO_POSITION requires exact zero known costs"
            )
        return
    if payload.cost_state is ProspectivePaperTerminalCostStateV2.INCOMPLETE_EVIDENCE:
        if payload.costs_complete or any(
            value is not None
            for value in (
                payload.signed_slippage_vs_reference_usdt,
                payload.known_fee_cost_usdt,
                payload.known_funding_cost_usdt,
            )
        ):
            raise ProspectivePaperTerminalContractErrorV2(
                "incomplete evidence cannot invent numeric costs"
            )
        return
    if (
        payload.costs_complete
        or payload.signed_slippage_vs_reference_usdt is None
        or payload.known_fee_cost_usdt is not None
        or payload.known_funding_cost_usdt is not None
    ):
        raise ProspectivePaperTerminalContractErrorV2(
            "full entry fill may expose only signed slippage before position accounting"
        )


def _payload_document(payload: ProspectivePaperTerminalPayloadV2) -> dict[str, object]:
    return {
        "actual_private_account_fee_claim": payload.actual_private_account_fee_claim,
        "attempt_id": payload.attempt_id,
        "attempt_plan_sha256": payload.attempt_plan_sha256,
        "authority_status": payload.authority_status,
        "bar_close_ms": payload.bar_close_ms,
        "bar_open_ms": payload.bar_open_ms,
        "canonical_full_fill_certificate": _optional_source_object(
            payload.canonical_full_fill_certificate_jsonl,
            "full-fill certificate",
        ),
        "canonical_paper_decision": _optional_source_object(
            payload.canonical_paper_decision_jsonl,
            "PAPER decision",
        ),
        "canonical_sizing": _optional_source_object(
            payload.canonical_sizing_jsonl,
            "PAPER sizing",
        ),
        "causal_target_membership_authoritative": (payload.causal_target_membership_authoritative),
        "cell_id": payload.cell_id,
        "certified_quantity": _decimal_text(payload.certified_quantity),
        "closed_candle_contract_checked": payload.closed_candle_contract_checked,
        "completeness": payload.completeness.value,
        "cost_state": payload.cost_state.value,
        "costs_complete": payload.costs_complete,
        "decision_cutoff_ms": payload.decision_cutoff_ms,
        "decision_event_id": payload.decision_event_id,
        "decision_payload_sha256": payload.decision_payload_sha256,
        "disposition_class": payload.disposition_class.value,
        "disposition_payload_sha256": payload.disposition_payload_sha256,
        "disposition_record_sha256": payload.disposition_record_sha256,
        "disposition_wal_payload_sha256": payload.disposition_wal_payload_sha256,
        "efficacy_eligible": payload.efficacy_eligible,
        "entry_terminal_only": payload.entry_terminal_only,
        "executable_notional": _decimal_text(payload.executable_notional),
        "executable_vwap": _decimal_text(payload.executable_vwap),
        "execution_contract_sha256": payload.execution_contract_sha256,
        "family": payload.family.value,
        "family_rule_version": payload.family_rule_version,
        "filled_quantity": _decimal_text(payload.filled_quantity),
        "full_fill_certificate_sha256": payload.full_fill_certificate_sha256,
        "invalidation": payload.invalidation,
        "known_fee_cost_usdt": _decimal_text(payload.known_fee_cost_usdt),
        "known_funding_cost_usdt": _decimal_text(payload.known_funding_cost_usdt),
        "market_take_bound_price": _decimal_text(payload.market_take_bound_price),
        "opposite_bbo": _decimal_text(payload.opposite_bbo),
        "paper_closure_method": (
            None if payload.paper_closure_method is None else payload.paper_closure_method.value
        ),
        "paper_decision_event_id": payload.paper_decision_event_id,
        "paper_decision_payload_sha256": payload.paper_decision_payload_sha256,
        "paper_evidence_sha256": payload.paper_evidence_sha256,
        "paper_inconclusive_cause": payload.paper_inconclusive_cause,
        "paper_price_cap": _decimal_text(payload.paper_price_cap),
        "paper_rule_version": payload.paper_rule_version,
        "paper_status": None if payload.paper_status is None else payload.paper_status.value,
        "position_after_cost_pnl_usdt": _decimal_text(payload.position_after_cost_pnl_usdt),
        "position_pnl_computed": payload.position_pnl_computed,
        "position_terminal": payload.position_terminal,
        "prepare_payload_sha256": payload.prepare_payload_sha256,
        "prepare_record_sha256": payload.prepare_record_sha256,
        "prepare_wal_payload_sha256": payload.prepare_wal_payload_sha256,
        "production_order_placement": payload.production_order_placement,
        "promoting_plan_sha256": payload.promoting_plan_sha256,
        "protocol_rule_version": payload.protocol_rule_version,
        "quote_notional_at_reference": _decimal_text(payload.quote_notional_at_reference),
        "reasons": list(payload.reasons),
        "reference_evidence_sha256": payload.reference_evidence_sha256,
        "reference_price": _decimal_text(payload.reference_price),
        "requested_quantity": _decimal_text(payload.requested_quantity),
        "schema_version": payload.schema_version,
        "segment_id": payload.segment_id,
        "signal_side": payload.signal_side,
        "signed_slippage_vs_reference_usdt": _decimal_text(
            payload.signed_slippage_vs_reference_usdt
        ),
        "sizing_cell": payload.sizing_cell.value,
        "sizing_reference_membership_authoritative": (
            payload.sizing_reference_membership_authoritative
        ),
        "sizing_rule_version": payload.sizing_rule_version,
        "sizing_sha256": payload.sizing_sha256,
        "sizing_status": None if payload.sizing_status is None else payload.sizing_status.value,
        "symbol": payload.symbol,
        "target_quote_notional_usdt": _decimal_text(payload.target_quote_notional_usdt),
        "target_state_last_ingest_seq": payload.target_state_last_ingest_seq,
        "target_venue_ms": payload.target_venue_ms,
        "terminal_rule_plan_bound": payload.terminal_rule_plan_bound,
        "terminal_status": payload.terminal_status.value,
        "typed_wal_replay_authoritative": payload.typed_wal_replay_authoritative,
        "unrounded_quantity": _decimal_text(payload.unrounded_quantity),
        "venue": payload.venue.value,
    }


_TERMINAL_KEYS: Final = frozenset(
    {
        "actual_private_account_fee_claim",
        "attempt_id",
        "attempt_plan_sha256",
        "authority_status",
        "bar_close_ms",
        "bar_open_ms",
        "canonical_full_fill_certificate",
        "canonical_paper_decision",
        "canonical_sizing",
        "causal_target_membership_authoritative",
        "cell_id",
        "certified_quantity",
        "closed_candle_contract_checked",
        "completeness",
        "cost_state",
        "costs_complete",
        "decision_cutoff_ms",
        "decision_event_id",
        "decision_payload_sha256",
        "disposition_class",
        "disposition_payload_sha256",
        "disposition_record_sha256",
        "disposition_wal_payload_sha256",
        "efficacy_eligible",
        "entry_terminal_only",
        "executable_notional",
        "executable_vwap",
        "execution_contract_sha256",
        "family",
        "family_rule_version",
        "filled_quantity",
        "full_fill_certificate_sha256",
        "invalidation",
        "known_fee_cost_usdt",
        "known_funding_cost_usdt",
        "market_take_bound_price",
        "opposite_bbo",
        "paper_closure_method",
        "paper_decision_event_id",
        "paper_decision_payload_sha256",
        "paper_evidence_sha256",
        "paper_inconclusive_cause",
        "paper_price_cap",
        "paper_rule_version",
        "paper_status",
        "payload_sha256",
        "position_after_cost_pnl_usdt",
        "position_pnl_computed",
        "position_terminal",
        "prepare_payload_sha256",
        "prepare_record_sha256",
        "prepare_wal_payload_sha256",
        "production_order_placement",
        "promoting_plan_sha256",
        "protocol_rule_version",
        "quote_notional_at_reference",
        "reasons",
        "reference_evidence_sha256",
        "reference_price",
        "requested_quantity",
        "schema_version",
        "segment_id",
        "signal_side",
        "signed_slippage_vs_reference_usdt",
        "sizing_cell",
        "sizing_reference_membership_authoritative",
        "sizing_rule_version",
        "sizing_sha256",
        "sizing_status",
        "symbol",
        "target_quote_notional_usdt",
        "target_state_last_ingest_seq",
        "target_venue_ms",
        "terminal_rule_plan_bound",
        "terminal_status",
        "typed_wal_replay_authoritative",
        "unrounded_quantity",
        "venue",
    }
)


def _decode_exact_terminal(encoded: bytes) -> dict[str, object]:
    if type(encoded) is not bytes or not encoded:
        raise ProspectivePaperTerminalContractErrorV2(
            "PAPER terminal must be non-empty immutable bytes"
        )
    try:
        decoded: object = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProspectivePaperTerminalContractErrorV2(
            "PAPER terminal is invalid UTF-8 JSON"
        ) from error
    if (
        not isinstance(decoded, dict)
        or frozenset(decoded) != _TERMINAL_KEYS
        or canonical_json_line(decoded) != encoded
    ):
        raise ProspectivePaperTerminalContractErrorV2(
            "PAPER terminal schema or canonical JSONL differs"
        )
    return cast(dict[str, object], decoded)


def _optional_source_object(encoded: bytes | None, label: str) -> dict[str, object] | None:
    return None if encoded is None else _source_object(encoded, label)


def _source_object(encoded: bytes, label: str) -> dict[str, object]:
    if type(encoded) is not bytes or not encoded:
        raise ProspectivePaperTerminalContractErrorV2(f"{label} must be non-empty immutable bytes")
    try:
        decoded: object = json.loads(encoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProspectivePaperTerminalContractErrorV2(f"{label} is invalid UTF-8 JSON") from error
    if not isinstance(decoded, dict) or canonical_json_line(decoded) != encoded:
        raise ProspectivePaperTerminalContractErrorV2(
            f"{label} is not an exact canonical JSON object"
        )
    return cast(dict[str, object], decoded)


def _hash_document(document: dict[str, object]) -> str:
    return hashlib.sha256(_PAYLOAD_HASH_DOMAIN + canonical_json_line(document)).hexdigest()


def _decimal_text(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _identity(value: str, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 512
        or any(character in value for character in "\r\n\x00")
    ):
        raise ProspectivePaperTerminalContractErrorV2(
            f"{field_name} must be a bounded normalized identity"
        )
    return value


def _require_sha256(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ProspectivePaperTerminalContractErrorV2(f"{field_name} must be lowercase SHA-256 hex")


def _safe_nonnegative_integer(value: object, field_name: str) -> None:
    if type(value) is not int or not 0 <= value <= _JCS_MAX_SAFE_INTEGER:
        raise ProspectivePaperTerminalContractErrorV2(
            f"{field_name} must be a nonnegative RFC8785-safe integer"
        )


def _finite_decimal(value: object, field_name: str, *, allow_negative: bool) -> None:
    if type(value) is not Decimal or not value.is_finite():
        raise ProspectivePaperTerminalContractErrorV2(f"{field_name} must be a finite Decimal")
    if not allow_negative and value < 0:
        raise ProspectivePaperTerminalContractErrorV2(f"{field_name} must be nonnegative")
