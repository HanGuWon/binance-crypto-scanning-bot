from __future__ import annotations

from collections.abc import Iterable
from decimal import Decimal

from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import SignalDecision
from signalbot.recommendations.models import (
    EvidenceTier,
    RecommendationAction,
    RecommendationEnvelope,
    RecommendationKind,
    ScoreKind,
    direction_action,
)

PROJECTION_VERSION = "recommendation-projection-v1"
DEFAULT_TTL_MS = 5 * 60 * 1_000


def project_decision(
    decision: SignalDecision,
    *,
    ttl_ms: int = DEFAULT_TTL_MS,
    projection_version: str = PROJECTION_VERSION,
) -> RecommendationEnvelope:
    """Project one existing decision without changing the source payload."""

    if ttl_ms < 0:
        raise ValueError("ttl_ms must be non-negative")
    blockers = list(_context_blockers(decision))
    gate_failures = decision.gate.failures if decision.gate is not None else ()
    blockers.extend(gate_failures)

    if decision.family in {SignalFamily.PUMP_RISK, SignalFamily.CRASH_RISK}:
        return _envelope(
            decision,
            action=RecommendationAction.NO_ENTRY,
            kind=RecommendationKind.RISK_WARNING,
            blockers=(*blockers, "risk_warning_is_not_an_entry"),
            ttl_ms=ttl_ms,
            projection_version=projection_version,
        )

    if decision.family is SignalFamily.TECHNICAL_EXIT:
        return _envelope(
            decision,
            action=RecommendationAction.NO_ENTRY,
            kind=RecommendationKind.EXIT_WARNING,
            blockers=(*blockers, "technical_exit_is_not_a_new_entry"),
            ttl_ms=ttl_ms,
            projection_version=projection_version,
        )

    if decision.metadata.get("informational_only") is True:
        blockers.append("informational_only")
    if decision.stage is not SignalStage.CONFIRMED:
        blockers.append(f"stage_not_confirmed:{decision.stage.value}")
    if decision.gate is not None and not decision.gate.passed:
        blockers.append("entry_gate_failed")

    if blockers:
        return _envelope(
            decision,
            action=RecommendationAction.NO_ENTRY,
            kind=RecommendationKind.HOLD,
            blockers=tuple(blockers),
            ttl_ms=ttl_ms,
            projection_version=projection_version,
        )

    if decision.direction not in {Direction.LONG, Direction.SHORT}:
        return _envelope(
            decision,
            action=RecommendationAction.NO_ENTRY,
            kind=RecommendationKind.HOLD,
            blockers=("direction_is_not_entry_capable",),
            ttl_ms=ttl_ms,
            projection_version=projection_version,
        )

    action = direction_action(decision.direction)
    if decision.market is Market.SPOT and decision.direction is Direction.SHORT:
        return _envelope(
            decision,
            action=action,
            kind=RecommendationKind.EXIT_WARNING,
            blockers=("spot_short_direction_maps_to_spot_exit",),
            ttl_ms=ttl_ms,
            projection_version=projection_version,
        )

    invalidation_blocker = _invalidation_blocker(decision)
    if invalidation_blocker is not None:
        return _envelope(
            decision,
            action=RecommendationAction.NO_ENTRY,
            kind=RecommendationKind.HOLD,
            blockers=(invalidation_blocker,),
            ttl_ms=ttl_ms,
            projection_version=projection_version,
        )

    return _envelope(
        decision,
        action=action,
        kind=RecommendationKind.ENTRY_CANDIDATE,
        blockers=(),
        ttl_ms=ttl_ms,
        projection_version=projection_version,
    )


def project_decisions(
    decisions: Iterable[SignalDecision],
    *,
    ttl_ms: int = DEFAULT_TTL_MS,
    projection_version: str = PROJECTION_VERSION,
) -> tuple[RecommendationEnvelope, ...]:
    """Project a batch in source-independent deterministic order."""

    projected = [
        project_decision(
            decision,
            ttl_ms=ttl_ms,
            projection_version=projection_version,
        )
        for decision in decisions
    ]
    return tuple(
        sorted(projected, key=lambda item: (item.market.value, item.symbol, item.event_id))
    )


def _envelope(
    decision: SignalDecision,
    *,
    action: RecommendationAction,
    kind: RecommendationKind,
    blockers: tuple[str, ...],
    ttl_ms: int,
    projection_version: str,
) -> RecommendationEnvelope:
    return RecommendationEnvelope(
        event_id=RecommendationEnvelope.deterministic_event_id(
            decision.event_id, projection_version
        ),
        source_event_id=decision.event_id,
        action=action,
        kind=kind,
        market=decision.market,
        symbol=decision.symbol,
        source_family=decision.family,
        source_stage=decision.stage,
        timeframe=decision.timeframe,
        decision_time_ms=decision.event_time_ms,
        expires_at_ms=decision.event_time_ms + ttl_ms,
        reasons=_unique(decision.reasons),
        blockers=_unique(blockers),
        invalidation=decision.invalidation,
        evidence_strength=decision.score,
        score_kind=ScoreKind.RULE_STRENGTH,
        evidence_tier=EvidenceTier.UNVALIDATED,
        rule_version=decision.rule_version,
    )


def _invalidation_blocker(decision: SignalDecision) -> str | None:
    if decision.invalidation is None:
        return None
    if decision.invalidation <= Decimal("0"):
        return "invalidation_not_positive"
    if decision.direction is Direction.LONG and decision.invalidation >= decision.price:
        return "long_invalidation_not_below_price"
    if decision.direction is Direction.SHORT and decision.invalidation <= decision.price:
        return "short_invalidation_not_above_price"
    return None


def _context_blockers(decision: SignalDecision) -> tuple[str, ...]:
    metadata = decision.metadata
    blockers: list[str] = []
    if (
        metadata.get("candle_is_closed") is False
        or metadata.get("source_candle_closed") is False
    ):
        blockers.append("open_candle")
    if (
        metadata.get("context_is_closed") is False
        or metadata.get("higher_timeframe_closed") is False
    ):
        blockers.append("open_higher_timeframe_context")
    if metadata.get("context_stale") is True or metadata.get("stale_context") is True:
        blockers.append("stale_higher_timeframe_context")
    context_time = metadata.get("higher_timeframe_event_time_ms")
    if context_time is None:
        context_time = metadata.get("context_event_time_ms")
    if isinstance(context_time, int) and context_time >= decision.event_time_ms:
        blockers.append("higher_timeframe_context_is_not_strictly_prior")
    return _unique(blockers)


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return tuple(result)
