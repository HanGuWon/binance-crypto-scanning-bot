from __future__ import annotations

from decimal import (
    ROUND_HALF_EVEN,
    Context,
    DivisionByZero,
    InvalidOperation,
    Overflow,
    Underflow,
)
from typing import Final

PROTOCOL_DECIMAL_PRECISION_V2: Final = 34


def protocol_decimal_context_v2() -> Context:
    """Return a fresh copy of the frozen R4B V2 Decimal arithmetic context."""

    context = Context(
        prec=PROTOCOL_DECIMAL_PRECISION_V2,
        rounding=ROUND_HALF_EVEN,
    )
    for signal in (InvalidOperation, DivisionByZero, Overflow, Underflow):
        context.traps[signal] = True
    return context
