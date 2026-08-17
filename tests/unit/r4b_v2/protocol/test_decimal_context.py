from __future__ import annotations

from decimal import (
    ROUND_DOWN,
    ROUND_HALF_EVEN,
    Decimal,
    DivisionByZero,
    InvalidOperation,
    Overflow,
    Underflow,
    getcontext,
    localcontext,
    setcontext,
)

from signalbot.r4b_v2.protocol.decimal_context import (
    PROTOCOL_DECIMAL_PRECISION_V2,
    protocol_decimal_context_v2,
)


def test_protocol_decimal_context_matches_the_frozen_contract() -> None:
    context = protocol_decimal_context_v2()

    assert context.prec == PROTOCOL_DECIMAL_PRECISION_V2 == 34
    assert context.rounding == ROUND_HALF_EVEN
    assert context.traps[InvalidOperation]
    assert context.traps[DivisionByZero]
    assert context.traps[Overflow]
    assert context.traps[Underflow]


def test_protocol_decimal_context_is_fresh_and_ambient_independent() -> None:
    first = protocol_decimal_context_v2()
    first.prec = 7
    first.rounding = ROUND_DOWN

    ambient = getcontext().copy()
    try:
        getcontext().prec = 6
        getcontext().rounding = ROUND_DOWN
        with localcontext(protocol_decimal_context_v2()):
            observed = Decimal(1) / Decimal(7)
    finally:
        setcontext(ambient)

    second = protocol_decimal_context_v2()
    assert second.prec == 34
    assert second.rounding == ROUND_HALF_EVEN
    assert observed == Decimal("0.1428571428571428571428571428571429")
