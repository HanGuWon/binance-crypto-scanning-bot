from __future__ import annotations

FIVE_MINUTE_MS_V2 = 300_000
DECISION_DELAY_MS_V2 = 2_001


class DecisionClockContractErrorV2(ValueError):
    """Raised when a V2 decision is not bound to one exact closed 5m bar."""


def validate_decision_bar_v2(
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
) -> None:
    """Validate the shared causal clock for every closed-bar V2 decision.

    ``bar_close_ms`` is the inclusive exchange candle end ``k.T`` and the
    decision cutoff is exactly ``k.T + 2,001 ms``.  This function validates
    clock shape only; each strategy still owns its closed-candle and feature
    readiness decisions.
    """

    for value, field_name in (
        (bar_open_ms, "bar_open_ms"),
        (bar_close_ms, "bar_close_ms"),
        (decision_cutoff_ms, "decision_cutoff_ms"),
    ):
        _validate_nonnegative_int(value, field_name)
    if bar_open_ms % FIVE_MINUTE_MS_V2 != 0:
        raise DecisionClockContractErrorV2(
            "bar_open_ms must be aligned to a 5m UTC boundary"
        )
    if bar_close_ms != bar_open_ms + FIVE_MINUTE_MS_V2 - 1:
        raise DecisionClockContractErrorV2(
            "bar_close_ms must be the inclusive 5m candle end"
        )
    if decision_cutoff_ms != bar_close_ms + DECISION_DELAY_MS_V2:
        raise DecisionClockContractErrorV2(
            "decision cutoff must equal k.T + 2001 ms"
        )


def _validate_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise DecisionClockContractErrorV2(
            f"{field_name} must be a nonnegative integer"
        )
