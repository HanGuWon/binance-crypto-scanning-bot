"""Independent live-vs-replay parity contract for ``causal_retest_v1``.

This module owns only a research verification boundary.  It deliberately does
not turn a replay of the same function into evidence: the caller must provide
two distinct adapter objects.  Historical decision-time BBO is an explicit
input; when it is absent the result is inconclusive and no synthetic BBO is
created.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, cast


class RetestParityStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    INCONCLUSIVE_NO_HISTORICAL_BBO = "INCONCLUSIVE_NO_HISTORICAL_BBO"


@dataclass(frozen=True, slots=True)
class RetestParityCase:
    """One immutable opportunity and its observed historical BBO evidence."""

    opportunity_id: str
    input_payload: Mapping[str, object]
    historical_decision_time_bbo: Mapping[str, object] | None


@dataclass(frozen=True, slots=True)
class RetestParityOutput:
    """Adapter output reduced to causal lifecycle fields for comparison."""

    stage: str
    terminal_reason: str | None
    ready_snapshot_sha256: str | None


class RetestAdapter(Protocol):
    def evaluate(
        self,
        case: RetestParityCase,
    ) -> RetestParityOutput: ...


@dataclass(frozen=True, slots=True)
class RetestParityMismatch:
    opportunity_id: str
    live: RetestParityOutput
    replay: RetestParityOutput


@dataclass(frozen=True, slots=True)
class RetestParityResult:
    status: RetestParityStatus
    total_cases: int
    comparable_cases: int
    missing_historical_bbo_cases: int
    mismatches: tuple[RetestParityMismatch, ...]


def run_retest_adapter_parity(
    cases: Iterable[RetestParityCase],
    *,
    live_adapter: RetestAdapter | Callable[[RetestParityCase], RetestParityOutput],
    replay_adapter: RetestAdapter | Callable[[RetestParityCase], RetestParityOutput],
) -> RetestParityResult:
    """Compare two independently supplied adapters on common opportunities.

    The adapters must be distinct objects.  Cases without a recorded
    decision-time BBO are counted but excluded from comparison; any such case
    keeps the aggregate from becoming PASS.  No BBO proxy or fabricated value
    is accepted by this contract.
    """

    if live_adapter is replay_adapter:
        raise ValueError("live and replay adapters must be independent objects")

    materialized = tuple(cases)
    mismatches: list[RetestParityMismatch] = []
    comparable = 0
    missing_bbo = 0

    def evaluate(
        adapter: RetestAdapter | Callable[[RetestParityCase], RetestParityOutput],
        case: RetestParityCase,
    ) -> RetestParityOutput:
        method = getattr(adapter, "evaluate", None)
        output = (
            method(case)
            if method is not None
            else cast(Callable[[RetestParityCase], RetestParityOutput], adapter)(case)
        )
        if not isinstance(output, RetestParityOutput):
            raise TypeError("retest adapter returned an invalid parity output")
        return output

    for case in materialized:
        if not case.historical_decision_time_bbo:
            missing_bbo += 1
            continue
        comparable += 1
        live = evaluate(live_adapter, case)
        replay = evaluate(replay_adapter, case)
        if live != replay:
            mismatches.append(
                RetestParityMismatch(
                    opportunity_id=case.opportunity_id,
                    live=live,
                    replay=replay,
                )
            )

    if mismatches:
        status = RetestParityStatus.FAIL
    elif missing_bbo:
        status = RetestParityStatus.INCONCLUSIVE_NO_HISTORICAL_BBO
    elif not comparable:
        status = RetestParityStatus.INCONCLUSIVE_NO_HISTORICAL_BBO
    else:
        status = RetestParityStatus.PASS
    return RetestParityResult(
        status=status,
        total_cases=len(materialized),
        comparable_cases=comparable,
        missing_historical_bbo_cases=missing_bbo,
        mismatches=tuple(mismatches),
    )
