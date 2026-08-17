"""Deterministic fixed-quote sizing for prospective PAPER entries.

The rule converts an exact frozen 100- or 1,000-USDT cell at one causal reference price to the
greatest legal common quantity-grid point not exceeding that exposure.  It
never places an order and cannot silently resize above a venue minimum or down
from a venue maximum.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import InitVar, dataclass, field
from decimal import Decimal, localcontext
from enum import StrEnum
from typing import Final

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.paper_fok import CommonQuantityGridV2
from signalbot.r4b_v2.protocol.decimal_context import protocol_decimal_context_v2

PAPER_SIZING_RULE_VERSION_V2: Final = (
    "R4B_CAUSAL_V2.4.0_FIXED_QUOTE_CELL_REFERENCE_PRICE_FLOOR"
)
PAPER_FIXED_QUOTE_NOTIONAL_USDT_V2: Final = Decimal("100")
PAPER_CAPACITY_QUOTE_NOTIONAL_USDT_V2: Final = Decimal("1000")

_SIZING_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_PAPER_SIZING_V2\0"
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_FACTORY_TOKEN: Final = object()


class PaperSizingContractErrorV2(ValueError):
    """Raised when PAPER sizing inputs or derived evidence are invalid."""


class PaperSizingStatusV2(StrEnum):
    READY = "READY"
    BELOW_MINIMUM = "BELOW_MINIMUM"
    ABOVE_MAXIMUM = "ABOVE_MAXIMUM"


class PaperSizingCellV2(StrEnum):
    NOTIONAL_100_USDT = "NOTIONAL_100_USDT"
    NOTIONAL_1000_USDT = "NOTIONAL_1000_USDT"


PAPER_SIZING_CELLS_V2: Final = tuple(PaperSizingCellV2)


@dataclass(frozen=True, slots=True)
class PaperSizingDecisionV2:
    """Hash-bound result of the frozen fixed-quote sizing rule."""

    sizing_cell: PaperSizingCellV2
    reference_price: Decimal
    reference_evidence_sha256: str
    unrounded_quantity: Decimal
    requested_quantity: Decimal | None
    quote_notional_at_reference: Decimal | None
    status: PaperSizingStatusV2
    reason: str
    _factory_token: InitVar[object]
    sizing_sha256: str = field(init=False)
    rule_version: str = field(init=False, default=PAPER_SIZING_RULE_VERSION_V2)
    target_quote_notional_usdt: Decimal
    production_order_placement: bool = field(init=False, default=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _FACTORY_TOKEN:
            raise PaperSizingContractErrorV2(
                "PAPER sizing evidence can be constructed only by the frozen calculator"
            )
        if not isinstance(self.sizing_cell, PaperSizingCellV2):
            raise PaperSizingContractErrorV2(
                "sizing_cell must be PaperSizingCellV2"
            )
        expected_notional = _quote_notional_for_cell(self.sizing_cell)
        if self.target_quote_notional_usdt != expected_notional:
            raise PaperSizingContractErrorV2(
                "target quote notional differs from its frozen sizing cell"
            )
        _validate_positive_decimal(self.reference_price, "reference_price")
        _validate_sha256(
            self.reference_evidence_sha256,
            "reference_evidence_sha256",
        )
        _validate_positive_decimal(
            self.unrounded_quantity,
            "unrounded_quantity",
        )
        if not isinstance(self.status, PaperSizingStatusV2):
            raise PaperSizingContractErrorV2("status must be PaperSizingStatusV2")
        if not isinstance(self.reason, str) or not self.reason:
            raise PaperSizingContractErrorV2("reason must be non-empty")
        if self.status is PaperSizingStatusV2.READY:
            if self.requested_quantity is None:
                raise PaperSizingContractErrorV2(
                    "READY sizing requires requested_quantity"
                )
            if self.quote_notional_at_reference is None:
                raise PaperSizingContractErrorV2(
                    "READY sizing requires quote_notional_at_reference"
                )
            _validate_positive_decimal(
                self.requested_quantity,
                "requested_quantity",
            )
            _validate_positive_decimal(
                self.quote_notional_at_reference,
                "quote_notional_at_reference",
            )
            if self.quote_notional_at_reference > self.target_quote_notional_usdt:
                raise PaperSizingContractErrorV2(
                    "requested quantity exceeds the frozen quote exposure"
                )
        elif (
            self.requested_quantity is not None
            or self.quote_notional_at_reference is not None
        ):
            raise PaperSizingContractErrorV2(
                "non-READY sizing forbids a requested quantity or notional"
            )
        object.__setattr__(
            self,
            "sizing_sha256",
            hashlib.sha256(
                _SIZING_DOMAIN + canonical_json_line(_sizing_document(self))
            ).hexdigest(),
        )


def size_fixed_quote_paper_entry_v2(
    *,
    sizing_cell: PaperSizingCellV2,
    reference_price: Decimal,
    reference_evidence_sha256: str,
    quantity_grid: CommonQuantityGridV2,
) -> PaperSizingDecisionV2:
    """Derive one requested quantity without exceeding or resizing exposure."""

    if not isinstance(sizing_cell, PaperSizingCellV2):
        raise PaperSizingContractErrorV2(
            "sizing_cell must be PaperSizingCellV2"
        )
    target_quote_notional = _quote_notional_for_cell(sizing_cell)
    _validate_positive_decimal(reference_price, "reference_price")
    _validate_sha256(reference_evidence_sha256, "reference_evidence_sha256")
    if type(quantity_grid) is not CommonQuantityGridV2:
        raise TypeError("quantity_grid must be exact CommonQuantityGridV2")
    with localcontext(protocol_decimal_context_v2()):
        unrounded = target_quote_notional / reference_price
        if unrounded < quantity_grid.first_legal:
            return PaperSizingDecisionV2(
                sizing_cell=sizing_cell,
                reference_price=reference_price,
                reference_evidence_sha256=reference_evidence_sha256,
                unrounded_quantity=unrounded,
                requested_quantity=None,
                quote_notional_at_reference=None,
                status=PaperSizingStatusV2.BELOW_MINIMUM,
                reason="FIXED_QUOTE_QUANTITY_BELOW_COMMON_FILTER_MINIMUM",
                target_quote_notional_usdt=target_quote_notional,
                _factory_token=_FACTORY_TOKEN,
            )
        if unrounded > quantity_grid.maximum:
            return PaperSizingDecisionV2(
                sizing_cell=sizing_cell,
                reference_price=reference_price,
                reference_evidence_sha256=reference_evidence_sha256,
                unrounded_quantity=unrounded,
                requested_quantity=None,
                quote_notional_at_reference=None,
                status=PaperSizingStatusV2.ABOVE_MAXIMUM,
                reason="FIXED_QUOTE_QUANTITY_ABOVE_COMMON_FILTER_MAXIMUM",
                target_quote_notional_usdt=target_quote_notional,
                _factory_token=_FACTORY_TOKEN,
            )
        requested = quantity_grid.floor_legal_total(unrounded)
        if requested <= 0 or not quantity_grid.is_legal(requested):
            raise PaperSizingContractErrorV2(
                "common quantity grid did not produce a positive legal floor"
            )
        return PaperSizingDecisionV2(
            sizing_cell=sizing_cell,
            reference_price=reference_price,
            reference_evidence_sha256=reference_evidence_sha256,
            unrounded_quantity=unrounded,
            requested_quantity=requested,
            quote_notional_at_reference=requested * reference_price,
            status=PaperSizingStatusV2.READY,
            reason="FIXED_QUOTE_REFERENCE_PRICE_FLOORED_TO_COMMON_GRID",
            target_quote_notional_usdt=target_quote_notional,
            _factory_token=_FACTORY_TOKEN,
        )


def canonical_paper_sizing_decision_v2(value: PaperSizingDecisionV2) -> bytes:
    """Return canonical bytes after rechecking the decision's content hash."""

    if type(value) is not PaperSizingDecisionV2:
        raise TypeError("value must be exact PaperSizingDecisionV2")
    document = _sizing_document(value)
    expected = hashlib.sha256(
        _SIZING_DOMAIN + canonical_json_line(document)
    ).hexdigest()
    if value.sizing_sha256 != expected:
        raise PaperSizingContractErrorV2(
            "PAPER sizing hash differs from canonical content"
        )
    return canonical_json_line({**document, "sizing_sha256": value.sizing_sha256})


def _sizing_document(value: PaperSizingDecisionV2) -> dict[str, object]:
    return {
        "production_order_placement": value.production_order_placement,
        "quote_notional_at_reference": (
            None
            if value.quote_notional_at_reference is None
            else str(value.quote_notional_at_reference)
        ),
        "reason": value.reason,
        "reference_evidence_sha256": value.reference_evidence_sha256,
        "reference_price": str(value.reference_price),
        "requested_quantity": (
            None
            if value.requested_quantity is None
            else str(value.requested_quantity)
        ),
        "rule_version": value.rule_version,
        "sizing_cell": value.sizing_cell.value,
        "status": value.status.value,
        "target_quote_notional_usdt": str(value.target_quote_notional_usdt),
        "unrounded_quantity": str(value.unrounded_quantity),
    }


def _quote_notional_for_cell(value: PaperSizingCellV2) -> Decimal:
    if value is PaperSizingCellV2.NOTIONAL_100_USDT:
        return PAPER_FIXED_QUOTE_NOTIONAL_USDT_V2
    if value is PaperSizingCellV2.NOTIONAL_1000_USDT:
        return PAPER_CAPACITY_QUOTE_NOTIONAL_USDT_V2
    raise PaperSizingContractErrorV2("unsupported PAPER sizing cell")


def _validate_positive_decimal(value: object, field_name: str) -> None:
    if type(value) is not Decimal or not value.is_finite() or value <= 0:
        raise PaperSizingContractErrorV2(
            f"{field_name} must be a positive finite Decimal"
        )


def _validate_sha256(value: object, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise PaperSizingContractErrorV2(
            f"{field_name} must be lowercase SHA-256 hex"
        )
