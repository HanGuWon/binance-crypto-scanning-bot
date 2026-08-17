from __future__ import annotations

from typing import cast

import pytest

from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
    DecisionClockContractErrorV2,
    validate_decision_bar_v2,
)

BAR_OPEN_MS = 2_000_160_000_000
BAR_CLOSE_MS = BAR_OPEN_MS + FIVE_MINUTE_MS_V2 - 1


def test_exact_closed_five_minute_decision_clock_is_accepted() -> None:
    validate_decision_bar_v2(
        BAR_OPEN_MS,
        BAR_CLOSE_MS,
        BAR_CLOSE_MS + DECISION_DELAY_MS_V2,
    )


@pytest.mark.parametrize(
    ("bar_open_ms", "bar_close_ms", "decision_cutoff_ms", "message"),
    [
        (BAR_OPEN_MS + 1, BAR_CLOSE_MS, BAR_CLOSE_MS + 2_001, "5m UTC"),
        (BAR_OPEN_MS, BAR_CLOSE_MS - 1, BAR_CLOSE_MS + 2_001, "inclusive 5m"),
        (BAR_OPEN_MS, BAR_CLOSE_MS, BAR_CLOSE_MS + 2_000, r"k\.T \+ 2001"),
        (BAR_OPEN_MS, BAR_CLOSE_MS, BAR_CLOSE_MS + 2_002, r"k\.T \+ 2001"),
    ],
)
def test_each_clock_boundary_one_millisecond_away_fails(
    bar_open_ms: int,
    bar_close_ms: int,
    decision_cutoff_ms: int,
    message: str,
) -> None:
    with pytest.raises(DecisionClockContractErrorV2, match=message):
        validate_decision_bar_v2(bar_open_ms, bar_close_ms, decision_cutoff_ms)


@pytest.mark.parametrize("value", [-1, True, 1.5, "0"])
def test_clock_rejects_negative_boolean_and_noninteger_values(value: object) -> None:
    with pytest.raises(DecisionClockContractErrorV2, match="nonnegative integer"):
        validate_decision_bar_v2(
            cast(int, value),
            BAR_CLOSE_MS,
            BAR_CLOSE_MS + DECISION_DELAY_MS_V2,
        )
