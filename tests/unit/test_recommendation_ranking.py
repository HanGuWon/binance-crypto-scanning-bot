from decimal import Decimal

from signalbot.domain.enums import Market, SignalFamily, SignalStage
from signalbot.recommendations import (
    EvidenceTier,
    RecommendationAction,
    RecommendationEnvelope,
    RecommendationKind,
    rank_recommendations,
)


def item(
    *,
    symbol: str,
    action: RecommendationAction = RecommendationAction.LONG,
    kind: RecommendationKind = RecommendationKind.ENTRY_CANDIDATE,
    score: int = 80,
    tier: EvidenceTier = EvidenceTier.UNVALIDATED,
    event_id: str | None = None,
    decision_time_ms: int = 1_000,
    expires_at_ms: int = 2_000,
) -> RecommendationEnvelope:
    return RecommendationEnvelope(
        event_id=event_id or f"event-{symbol}",
        source_event_id=f"source-{symbol}",
        action=action,
        kind=kind,
        market=Market.FUTURES,
        symbol=symbol,
        source_family=(
            SignalFamily.BREAKOUT_LONG
            if action is RecommendationAction.LONG
            else SignalFamily.BREAKDOWN_SHORT
        ),
        source_stage=SignalStage.CONFIRMED,
        timeframe="5m",
        decision_time_ms=decision_time_ms,
        expires_at_ms=expires_at_ms,
        reasons=("rule",),
        blockers=("blocked",) if action is RecommendationAction.NO_ENTRY else (),
        invalidation=Decimal("98") if action is RecommendationAction.LONG else None,
        evidence_strength=score,
        evidence_tier=tier,
        rule_version="r2",
    )


def test_top_long_uses_tier_then_strength_then_symbol_stably() -> None:
    result = rank_recommendations(
        [
            item(symbol="ETHUSDT", score=99),
            item(symbol="BTCUSDT", score=10, tier=EvidenceTier.PROSPECTIVE_SHADOW),
            item(symbol="ADAUSDT", score=10, tier=EvidenceTier.PROSPECTIVE_SHADOW),
        ],
        as_of_ms=1_500,
    )

    ranking = result.for_market(Market.FUTURES)
    assert ranking is not None
    assert tuple(item.symbol for item in ranking.top_long) == ("ADAUSDT",)


def test_expiry_boundary_is_excluded_and_exposed_only_as_expired() -> None:
    result = rank_recommendations(
        [item(symbol="BTCUSDT", expires_at_ms=2_000)],
        as_of_ms=2_000,
    )

    ranking = result.for_market(Market.FUTURES)
    assert ranking is not None
    assert ranking.top_long == ()
    assert tuple(item.symbol for item in ranking.expired) == ("BTCUSDT",)


def test_no_entry_is_kept_for_query_view_and_not_ranked_as_direction() -> None:
    result = rank_recommendations(
        [
            item(
                symbol="BTCUSDT",
                action=RecommendationAction.NO_ENTRY,
                kind=RecommendationKind.HOLD,
            )
        ],
        as_of_ms=1_500,
    )

    ranking = result.for_market(Market.FUTURES)
    assert ranking is not None
    assert ranking.top_long == ()
    assert ranking.top_short == ()
    assert tuple(item.symbol for item in ranking.no_entry) == ("BTCUSDT",)
