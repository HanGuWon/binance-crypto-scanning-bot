from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from position_guardian.context_client import ValidatedProtectionContext
from signalbot.domain.enums import Direction, Market
from signalbot.signals.position_management import (
    ManagedPositionSnapshot,
    ProtectiveStopPlanner,
    StopUpdateIntent,
)

ShadowPlanDisposition = Literal["STOP_UPDATE_INTENT", "NO_STOP_UPDATE"]


class ShadowPlanningError(ValueError):
    """Raised when a managed snapshot cannot safely pair with its context."""


@dataclass(frozen=True, slots=True)
class ShadowPlan:
    """Pure L60-04 planning result; it contains no execution capability."""

    disposition: ShadowPlanDisposition
    context_id: str
    closed_candle_cursor: str
    intent: StopUpdateIntent | None


def plan_shadow_stop(
    snapshot: ManagedPositionSnapshot,
    validated_context: ValidatedProtectionContext,
    *,
    planner: ProtectiveStopPlanner | None = None,
) -> ShadowPlan:
    """Compose the existing pure stop planner with one validated closed context."""

    context = validated_context.context
    if snapshot.market is not Market.FUTURES or context.market is not Market.FUTURES:
        raise ShadowPlanningError("Guardian shadow planning requires USD-M futures context")
    if snapshot.symbol != context.symbol:
        raise ShadowPlanningError("managed snapshot symbol does not match protection context")
    if snapshot.observed_at_ms != context.candle_close_time_ms:
        raise ShadowPlanningError(
            "managed snapshot must be anchored to the same closed-candle cursor"
        )

    structure_stop = (
        context.confirmed_swing_support
        if snapshot.direction is Direction.LONG
        else context.confirmed_swing_resistance
    )
    momentum_weakened = context.consecutive_trend_failure_count > 0
    stop_intent = (planner or ProtectiveStopPlanner()).plan(
        snapshot,
        atr=context.atr,
        confirmed_structure_stop=structure_stop,
        momentum_weakened=momentum_weakened,
    )
    return ShadowPlan(
        disposition="STOP_UPDATE_INTENT" if stop_intent is not None else "NO_STOP_UPDATE",
        context_id=context.context_id,
        closed_candle_cursor=validated_context.cursor.serialize(),
        intent=stop_intent,
    )
