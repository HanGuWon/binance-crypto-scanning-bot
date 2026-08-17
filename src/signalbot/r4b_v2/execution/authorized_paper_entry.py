"""One-use current-prefix authority for fixed-quote sizing and PAPER FOK.

The low-level sizing calculator intentionally accepts scalar inputs so it can
remain a deterministic arithmetic primitive.  This module is the runtime seam:
it consumes one current causal-target capability, derives sizing only from the
exact causal USD-M mark row carried by the PAPER input, checks the current
exchange-info quantity grid, and only then evaluates the PAPER FOK decision.

The returned receipt proves current in-process authority.  Its capability is
not yet persisted in the prospective outcome WAL, so it cannot by itself make
an entry or position terminal efficacy-eligible after restart.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    MARK_PRICE_MAX_STALENESS_MS_V2,
    CausalMarkPriceEvidenceV2,
    PaperFokEntryDecisionV2,
    PaperFokEntryInputV2,
    evaluate_paper_fok_entry_v2,
    intersect_quantity_filters_v2,
)
from signalbot.r4b_v2.execution.paper_sizing import (
    PaperSizingCellV2,
    PaperSizingDecisionV2,
    PaperSizingStatusV2,
    canonical_paper_sizing_decision_v2,
    size_fixed_quote_paper_entry_v2,
)

AUTHORIZED_PAPER_ENTRY_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.5.0_CURRENT_MARK_SIZING_AND_PAPER_FOK"
)
AUTHORIZED_PAPER_ENTRY_SCHEMA_V2: Final = (
    "r4b_v2_authorized_sized_paper_entry_v2"
)
CAUSAL_PAPER_SIZING_REFERENCE_SCHEMA_V2: Final = (
    "r4b_v2_causal_paper_sizing_reference_v2"
)

_REFERENCE_DOMAIN: Final = b"R4B_V2_CAUSAL_PAPER_SIZING_REFERENCE\0"
_RESULT_DOMAIN: Final = b"R4B_V2_AUTHORIZED_SIZED_PAPER_ENTRY\0"
_REFERENCE_FACTORY_TOKEN: Final = object()
_RESULT_FACTORY_TOKEN: Final = object()
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE: Final = re.compile(r"^[A-Z0-9]+USDT$")
_MAX_IDENTITY_LENGTH: Final = 256


class AuthorizedPaperEntryContractErrorV2(ValueError):
    """Raised when current-authority sizing/PAPER evidence is not exact."""


@dataclass(frozen=True, slots=True)
class CausalPaperSizingReferenceV2:
    """Factory-sealed causal mark row used by the fixed-quote calculator."""

    attempt_id: str
    signal_event_id: str
    symbol: str
    venue: VenueV2
    promoting_plan_sha256: str
    source_root_sha256: str
    mark_schema_sha256: str
    decision_cutoff_ms: int
    target_venue_ms: int
    target_local_cursor_ms: int
    target_state_last_ingest_seq: int
    capability_id: str
    cursor_snapshot_sha256: str
    pair: str
    routing_status: int
    mark_price: Decimal
    mark_event_time_ms: int
    mark_receipt_completion_ms: int
    _factory_token: InitVar[object | None] = None
    evidence_sha256: str = field(init=False)
    schema_version: str = field(
        init=False,
        default=CAUSAL_PAPER_SIZING_REFERENCE_SCHEMA_V2,
    )
    rule_version: str = field(
        init=False,
        default=AUTHORIZED_PAPER_ENTRY_RULE_VERSION_V2,
    )
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _REFERENCE_FACTORY_TOKEN:
            raise AuthorizedPaperEntryContractErrorV2(
                "causal sizing references are factory-sealed"
            )
        _validate_reference(self)
        object.__setattr__(
            self,
            "evidence_sha256",
            _hash_document(_REFERENCE_DOMAIN, _reference_document(self)),
        )


@dataclass(frozen=True, slots=True)
class AuthorizedSizedPaperEntryV2:
    """Exact current-prefix result joining mark sizing and PAPER evaluation."""

    reference: CausalPaperSizingReferenceV2
    sizing: PaperSizingDecisionV2
    paper_decision: PaperFokEntryDecisionV2
    _factory_token: InitVar[object | None] = None
    result_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=AUTHORIZED_PAPER_ENTRY_SCHEMA_V2)
    rule_version: str = field(
        init=False,
        default=AUTHORIZED_PAPER_ENTRY_RULE_VERSION_V2,
    )
    current_signed_prefix_authoritative: bool = field(init=False, default=True)
    sizing_reference_membership_authoritative: bool = field(init=False, default=True)
    causal_target_membership_authoritative: bool = field(init=False, default=True)
    durable_capability_persisted: bool = field(init=False, default=False)
    typed_wal_replay_authoritative: bool = field(init=False, default=False)
    efficacy_eligible: bool = field(init=False, default=False)
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _RESULT_FACTORY_TOKEN:
            raise AuthorizedPaperEntryContractErrorV2(
                "authorized sized PAPER results are factory-sealed"
            )
        _validate_result(self)
        object.__setattr__(
            self,
            "result_sha256",
            _hash_document(_RESULT_DOMAIN, _result_document(self)),
        )


def evaluate_authorized_sized_paper_entry_v2(
    item: PaperFokEntryInputV2,
    *,
    sizing_cell: PaperSizingCellV2,
    current_target_authority: object,
) -> AuthorizedSizedPaperEntryV2:
    """Consume one current capability and evaluate exact mark-sized PAPER FOK.

    ``item.requested_quantity`` is treated as an asserted projection, never as
    authority.  It must equal the quantity independently recomputed here from
    the frozen sizing cell, causal mark price, and current common quantity grid.
    """

    from signalbot.r4b_v2.capture.causal_target_authority import (
        CurrentCausalTargetAuthorityUseV2,
        consume_current_causal_target_authority_v2,
    )

    if type(item) is not PaperFokEntryInputV2:
        raise AuthorizedPaperEntryContractErrorV2(
            "item must be an exact PaperFokEntryInputV2"
        )
    if not isinstance(sizing_cell, PaperSizingCellV2):
        raise AuthorizedPaperEntryContractErrorV2(
            "sizing_cell must be PaperSizingCellV2"
        )
    if type(current_target_authority) is not CurrentCausalTargetAuthorityUseV2:
        raise AuthorizedPaperEntryContractErrorV2(
            "runtime sizing/PAPER evaluation requires current causal-target authority"
        )

    capability_id = current_target_authority.capability_id
    cursor_snapshot_sha256 = current_target_authority.snapshot_sha256
    promoting_plan_sha256 = current_target_authority.promoting_plan_sha256
    authorized_cursor = consume_current_causal_target_authority_v2(
        current_target_authority
    )
    if item.target_cursor != authorized_cursor:
        raise AuthorizedPaperEntryContractErrorV2(
            "PAPER input cursor differs from current signed-prefix authority"
        )
    if item.lineage.promoting_plan_sha256 != promoting_plan_sha256:
        raise AuthorizedPaperEntryContractErrorV2(
            "PAPER lineage plan differs from current signed-prefix authority"
        )

    _validate_mark_membership(item)
    _validate_exchange_info_membership(item)
    reference = _build_reference(
        item=item,
        capability_id=capability_id,
        cursor_snapshot_sha256=cursor_snapshot_sha256,
    )
    grid = intersect_quantity_filters_v2(
        item.exchange_info.lot_size,
        item.exchange_info.market_lot_size,
    )
    sizing = size_fixed_quote_paper_entry_v2(
        sizing_cell=sizing_cell,
        reference_price=item.mark.mark_price,
        reference_evidence_sha256=reference.evidence_sha256,
        quantity_grid=grid,
    )
    if sizing.status is not PaperSizingStatusV2.READY:
        raise AuthorizedPaperEntryContractErrorV2(
            "current causal mark cannot produce an executable frozen sizing cell"
        )
    if sizing.requested_quantity != item.requested_quantity:
        raise AuthorizedPaperEntryContractErrorV2(
            "PAPER requested quantity differs from current causal mark sizing"
        )

    paper_decision = evaluate_paper_fok_entry_v2(item)
    return AuthorizedSizedPaperEntryV2(
        reference=reference,
        sizing=sizing,
        paper_decision=paper_decision,
        _factory_token=_RESULT_FACTORY_TOKEN,
    )


def canonical_causal_paper_sizing_reference_v2(
    value: CausalPaperSizingReferenceV2,
) -> bytes:
    """Return canonical reference bytes after checking the factory hash."""

    if type(value) is not CausalPaperSizingReferenceV2:
        raise TypeError("value must be exact CausalPaperSizingReferenceV2")
    _validate_reference(value)
    expected = _hash_document(_REFERENCE_DOMAIN, _reference_document(value))
    if not hmac.compare_digest(value.evidence_sha256, expected):
        raise AuthorizedPaperEntryContractErrorV2(
            "causal sizing reference hash differs from canonical content"
        )
    return canonical_json_line(
        {**_reference_document(value), "evidence_sha256": value.evidence_sha256}
    )


def canonical_authorized_sized_paper_entry_v2(
    value: AuthorizedSizedPaperEntryV2,
) -> bytes:
    """Return the bounded canonical runtime receipt projection."""

    if type(value) is not AuthorizedSizedPaperEntryV2:
        raise TypeError("value must be exact AuthorizedSizedPaperEntryV2")
    _validate_result(value)
    canonical_causal_paper_sizing_reference_v2(value.reference)
    expected = _hash_document(_RESULT_DOMAIN, _result_document(value))
    if not hmac.compare_digest(value.result_sha256, expected):
        raise AuthorizedPaperEntryContractErrorV2(
            "authorized sized PAPER result hash differs from canonical content"
        )
    return canonical_json_line(
        {**_result_document(value), "result_sha256": value.result_sha256}
    )


def _build_reference(
    *,
    item: PaperFokEntryInputV2,
    capability_id: str,
    cursor_snapshot_sha256: str,
) -> CausalPaperSizingReferenceV2:
    mark = item.mark
    return CausalPaperSizingReferenceV2(
        attempt_id=item.attempt_id,
        signal_event_id=item.signal_event_id,
        symbol=item.symbol,
        venue=item.venue,
        promoting_plan_sha256=item.lineage.promoting_plan_sha256,
        source_root_sha256=item.lineage.source_root_sha256,
        mark_schema_sha256=item.lineage.mark_schema_sha256,
        decision_cutoff_ms=item.decision_cutoff_ms,
        target_venue_ms=item.target_venue_ms,
        target_local_cursor_ms=item.target_local_cursor_ms,
        target_state_last_ingest_seq=item.target_state_last_ingest_seq,
        capability_id=capability_id,
        cursor_snapshot_sha256=cursor_snapshot_sha256,
        pair=mark.pair,
        routing_status=mark.routing_status,
        mark_price=mark.mark_price,
        mark_event_time_ms=mark.event_time_ms,
        mark_receipt_completion_ms=mark.receipt_completion_ms,
        _factory_token=_REFERENCE_FACTORY_TOKEN,
    )


def _validate_mark_membership(item: PaperFokEntryInputV2) -> None:
    mark = item.mark
    if type(mark) is not CausalMarkPriceEvidenceV2:
        raise AuthorizedPaperEntryContractErrorV2(
            "PAPER sizing requires exact causal mark evidence"
        )
    if (
        mark.symbol,
        mark.venue,
        mark.promoting_plan_sha256,
        mark.source_root_sha256,
        mark.schema_sha256,
        mark.pair,
        mark.routing_status,
    ) != (
        item.symbol,
        item.venue,
        item.lineage.promoting_plan_sha256,
        item.lineage.source_root_sha256,
        item.lineage.mark_schema_sha256,
        item.symbol,
        1,
    ):
        raise AuthorizedPaperEntryContractErrorV2(
            "causal mark is outside the exact PAPER lineage"
        )
    if mark.event_time_ms > item.target_venue_ms:
        raise AuthorizedPaperEntryContractErrorV2(
            "causal mark event time is after the target"
        )
    if mark.receipt_completion_ms > item.target_local_cursor_ms:
        raise AuthorizedPaperEntryContractErrorV2(
            "causal mark receipt is after the target cursor"
        )
    if item.target_venue_ms - mark.event_time_ms > MARK_PRICE_MAX_STALENESS_MS_V2:
        raise AuthorizedPaperEntryContractErrorV2(
            "causal mark exceeds the frozen staleness boundary"
        )


def _validate_exchange_info_membership(item: PaperFokEntryInputV2) -> None:
    rules = item.exchange_info
    if (
        rules.symbol,
        rules.venue,
        rules.promoting_plan_sha256,
        rules.source_root_sha256,
        rules.schema_sha256,
    ) != (
        item.symbol,
        item.venue,
        item.lineage.promoting_plan_sha256,
        item.lineage.source_root_sha256,
        item.lineage.exchange_info_schema_sha256,
    ):
        raise AuthorizedPaperEntryContractErrorV2(
            "quantity filters are outside the exact PAPER lineage"
        )
    if not rules.applicable_filter_inventory_complete:
        raise AuthorizedPaperEntryContractErrorV2(
            "quantity-filter inventory is incomplete at the target"
        )
    if rules.response_completion_ms > item.target_local_cursor_ms:
        raise AuthorizedPaperEntryContractErrorV2(
            "quantity filters arrived after the target cursor"
        )
    if not (
        rules.version_valid_from_local_ms
        <= item.target_local_cursor_ms
        <= rules.version_valid_through_local_ms
    ):
        raise AuthorizedPaperEntryContractErrorV2(
            "quantity-filter version is not certain at the target"
        )


def _validate_reference(value: CausalPaperSizingReferenceV2) -> None:
    _require_identity(value.attempt_id, "attempt_id")
    _require_symbol(value.symbol)
    if value.venue is not VenueV2.USDM_FUTURES:
        raise AuthorizedPaperEntryContractErrorV2(
            "causal sizing reference must remain USD-M Futures"
        )
    for candidate, name in (
        (value.signal_event_id, "signal_event_id"),
        (value.promoting_plan_sha256, "promoting_plan_sha256"),
        (value.source_root_sha256, "source_root_sha256"),
        (value.mark_schema_sha256, "mark_schema_sha256"),
        (value.capability_id, "capability_id"),
        (value.cursor_snapshot_sha256, "cursor_snapshot_sha256"),
    ):
        _require_sha256(candidate, name)
    for candidate, name in (
        (value.decision_cutoff_ms, "decision_cutoff_ms"),
        (value.target_venue_ms, "target_venue_ms"),
        (value.target_local_cursor_ms, "target_local_cursor_ms"),
        (value.target_state_last_ingest_seq, "target_state_last_ingest_seq"),
        (value.mark_event_time_ms, "mark_event_time_ms"),
        (value.mark_receipt_completion_ms, "mark_receipt_completion_ms"),
    ):
        _require_nonnegative_integer(candidate, name)
    if value.pair != value.symbol or value.routing_status != 1:
        raise AuthorizedPaperEntryContractErrorV2(
            "causal sizing reference has invalid USD-M routing"
        )
    if (
        type(value.mark_price) is not Decimal
        or not value.mark_price.is_finite()
        or value.mark_price <= 0
    ):
        raise AuthorizedPaperEntryContractErrorV2(
            "causal sizing reference mark_price must be positive finite Decimal"
        )
    if value.mark_event_time_ms > value.target_venue_ms:
        raise AuthorizedPaperEntryContractErrorV2(
            "reference mark event lies after the target"
        )
    if value.mark_receipt_completion_ms > value.target_local_cursor_ms:
        raise AuthorizedPaperEntryContractErrorV2(
            "reference mark receipt lies after the target cursor"
        )
    if (
        value.target_venue_ms - value.mark_event_time_ms
        > MARK_PRICE_MAX_STALENESS_MS_V2
    ):
        raise AuthorizedPaperEntryContractErrorV2(
            "reference mark is stale beyond the frozen boundary"
        )
    if value.production_order_placement:
        raise AuthorizedPaperEntryContractErrorV2(
            "causal sizing reference cannot place a production order"
        )


def _validate_result(value: AuthorizedSizedPaperEntryV2) -> None:
    if type(value.reference) is not CausalPaperSizingReferenceV2:
        raise AuthorizedPaperEntryContractErrorV2(
            "result reference must be exact CausalPaperSizingReferenceV2"
        )
    canonical_causal_paper_sizing_reference_v2(value.reference)
    if type(value.sizing) is not PaperSizingDecisionV2:
        raise AuthorizedPaperEntryContractErrorV2(
            "result sizing must be exact PaperSizingDecisionV2"
        )
    canonical_paper_sizing_decision_v2(value.sizing)
    if type(value.paper_decision) is not PaperFokEntryDecisionV2:
        raise AuthorizedPaperEntryContractErrorV2(
            "result PAPER decision must be exact PaperFokEntryDecisionV2"
        )
    reference = value.reference
    sizing = value.sizing
    paper = value.paper_decision
    if sizing.status is not PaperSizingStatusV2.READY:
        raise AuthorizedPaperEntryContractErrorV2(
            "authorized PAPER result requires READY sizing"
        )
    if (
        sizing.reference_price != reference.mark_price
        or sizing.reference_evidence_sha256 != reference.evidence_sha256
        or sizing.requested_quantity is None
        or paper.requested_quantity != sizing.requested_quantity
    ):
        raise AuthorizedPaperEntryContractErrorV2(
            "authorized PAPER result differs from its exact causal sizing"
        )
    if (
        paper.attempt_id,
        paper.signal_event_id,
        paper.symbol,
        paper.venue,
        paper.promoting_plan_sha256,
        paper.source_root_sha256,
        paper.decision_cutoff_ms,
        paper.target_venue_ms,
        paper.target_state_last_ingest_seq,
    ) != (
        reference.attempt_id,
        reference.signal_event_id,
        reference.symbol,
        reference.venue,
        reference.promoting_plan_sha256,
        reference.source_root_sha256,
        reference.decision_cutoff_ms,
        reference.target_venue_ms,
        reference.target_state_last_ingest_seq,
    ):
        raise AuthorizedPaperEntryContractErrorV2(
            "PAPER decision differs from the current causal reference identity"
        )
    for flag, name, expected in (
        (
            value.current_signed_prefix_authoritative,
            "current_signed_prefix_authoritative",
            True,
        ),
        (
            value.sizing_reference_membership_authoritative,
            "sizing_reference_membership_authoritative",
            True,
        ),
        (
            value.causal_target_membership_authoritative,
            "causal_target_membership_authoritative",
            True,
        ),
        (value.durable_capability_persisted, "durable_capability_persisted", False),
        (
            value.typed_wal_replay_authoritative,
            "typed_wal_replay_authoritative",
            False,
        ),
        (value.efficacy_eligible, "efficacy_eligible", False),
        (value.production_order_placement, "production_order_placement", False),
    ):
        if flag is not expected:
            raise AuthorizedPaperEntryContractErrorV2(
                f"{name} violates the runtime-only authority contract"
            )


def _reference_document(value: CausalPaperSizingReferenceV2) -> dict[str, object]:
    return {
        "attempt_id": value.attempt_id,
        "capability_id": value.capability_id,
        "cursor_snapshot_sha256": value.cursor_snapshot_sha256,
        "decision_cutoff_ms": value.decision_cutoff_ms,
        "mark_event_time_ms": value.mark_event_time_ms,
        "mark_price": str(value.mark_price),
        "mark_receipt_completion_ms": value.mark_receipt_completion_ms,
        "mark_schema_sha256": value.mark_schema_sha256,
        "pair": value.pair,
        "production_order_placement": value.production_order_placement,
        "promoting_plan_sha256": value.promoting_plan_sha256,
        "routing_status": value.routing_status,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "signal_event_id": value.signal_event_id,
        "source_root_sha256": value.source_root_sha256,
        "symbol": value.symbol,
        "target_local_cursor_ms": value.target_local_cursor_ms,
        "target_state_last_ingest_seq": value.target_state_last_ingest_seq,
        "target_venue_ms": value.target_venue_ms,
        "venue": value.venue.value,
    }


def _result_document(value: AuthorizedSizedPaperEntryV2) -> dict[str, object]:
    return {
        "causal_target_membership_authoritative": (
            value.causal_target_membership_authoritative
        ),
        "current_signed_prefix_authoritative": (
            value.current_signed_prefix_authoritative
        ),
        "durable_capability_persisted": value.durable_capability_persisted,
        "efficacy_eligible": value.efficacy_eligible,
        "paper_decision_event_id": value.paper_decision.event_id,
        "paper_decision_evidence_sha256": value.paper_decision.evidence_sha256,
        "paper_decision_payload_sha256": value.paper_decision.payload_sha256,
        "paper_decision_status": value.paper_decision.status.value,
        "production_order_placement": value.production_order_placement,
        "reference_evidence_sha256": value.reference.evidence_sha256,
        "rule_version": value.rule_version,
        "schema_version": value.schema_version,
        "sizing_cell": value.sizing.sizing_cell.value,
        "sizing_reference_membership_authoritative": (
            value.sizing_reference_membership_authoritative
        ),
        "sizing_sha256": value.sizing.sizing_sha256,
        "typed_wal_replay_authoritative": value.typed_wal_replay_authoritative,
    }


def _hash_document(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _require_identity(value: object, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in "\r\n\x00" for character in value)
    ):
        raise AuthorizedPaperEntryContractErrorV2(
            f"{field_name} must be a bounded non-empty identity"
        )


def _require_symbol(value: object) -> None:
    if not isinstance(value, str) or _SYMBOL_RE.fullmatch(value) is None:
        raise AuthorizedPaperEntryContractErrorV2(
            "symbol must be an uppercase USD-M USDT pair"
        )


def _require_sha256(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise AuthorizedPaperEntryContractErrorV2(
            f"{field_name} must be lowercase SHA-256 hex"
        )


def _require_nonnegative_integer(value: object, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise AuthorizedPaperEntryContractErrorV2(
            f"{field_name} must be a nonnegative integer"
        )
