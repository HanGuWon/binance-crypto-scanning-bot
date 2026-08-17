from __future__ import annotations

from decimal import Decimal

import pytest

from signalbot.r4b_v2.execution.paper_fok import CommonQuantityGridV2
from signalbot.r4b_v2.execution.paper_sizing import (
    PaperSizingCellV2,
    PaperSizingContractErrorV2,
    PaperSizingDecisionV2,
    PaperSizingStatusV2,
    canonical_paper_sizing_decision_v2,
    size_fixed_quote_paper_entry_v2,
)

EVIDENCE_SHA256 = "a" * 64


def _grid(*, minimum: int = 1, maximum: int = 10_000) -> CommonQuantityGridV2:
    return CommonQuantityGridV2(
        scale=100,
        residue_units=0,
        modulus_units=1,
        minimum_units=minimum,
        maximum_units=maximum,
        first_legal_units=minimum,
    )


def test_ready_quantity_floors_to_grid_without_exceeding_100_usdt() -> None:
    decision = size_fixed_quote_paper_entry_v2(
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        reference_price=Decimal("300"),
        reference_evidence_sha256=EVIDENCE_SHA256,
        quantity_grid=_grid(),
    )

    assert decision.status is PaperSizingStatusV2.READY
    assert decision.requested_quantity == Decimal("0.33")
    assert decision.quote_notional_at_reference == Decimal("99.00")


def test_ready_boundary_uses_exact_legal_floor_and_canonical_hash() -> None:
    decision = size_fixed_quote_paper_entry_v2(
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        reference_price=Decimal("30"),
        reference_evidence_sha256=EVIDENCE_SHA256,
        quantity_grid=_grid(),
    )

    assert decision.status is PaperSizingStatusV2.READY
    assert decision.unrounded_quantity == Decimal("3.333333333333333333333333333333333")
    assert decision.requested_quantity == Decimal("3.33")
    assert decision.quote_notional_at_reference == Decimal("99.90")
    assert canonical_paper_sizing_decision_v2(decision).endswith(b"\n")


def test_below_minimum_and_above_maximum_never_resize() -> None:
    below = size_fixed_quote_paper_entry_v2(
        sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
        reference_price=Decimal("20000"),
        reference_evidence_sha256=EVIDENCE_SHA256,
        quantity_grid=_grid(minimum=1),
    )
    above = size_fixed_quote_paper_entry_v2(
        sizing_cell=PaperSizingCellV2.NOTIONAL_1000_USDT,
        reference_price=Decimal("1"),
        reference_evidence_sha256=EVIDENCE_SHA256,
        quantity_grid=_grid(maximum=1_000),
    )

    assert below.status is PaperSizingStatusV2.BELOW_MINIMUM
    assert above.status is PaperSizingStatusV2.ABOVE_MAXIMUM
    assert below.requested_quantity is above.requested_quantity is None
    assert below.quote_notional_at_reference is None
    assert above.quote_notional_at_reference is None


def test_direct_construction_and_invalid_reference_fail_closed() -> None:
    with pytest.raises(PaperSizingContractErrorV2, match="calculator"):
        PaperSizingDecisionV2(
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            reference_price=Decimal("100"),
            reference_evidence_sha256=EVIDENCE_SHA256,
            unrounded_quantity=Decimal("1"),
            requested_quantity=Decimal("1"),
            quote_notional_at_reference=Decimal("100"),
            status=PaperSizingStatusV2.READY,
            reason="FORGED",
            target_quote_notional_usdt=Decimal("100"),
            _factory_token=object(),
        )
    with pytest.raises(PaperSizingContractErrorV2, match="reference_price"):
        size_fixed_quote_paper_entry_v2(
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            reference_price=Decimal("0"),
            reference_evidence_sha256=EVIDENCE_SHA256,
            quantity_grid=_grid(),
        )
