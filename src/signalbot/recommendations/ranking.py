from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from signalbot.domain.enums import Market
from signalbot.recommendations.models import (
    EvidenceTier,
    RecommendationAction,
    RecommendationEnvelope,
    RecommendationKind,
)

_EVIDENCE_TIER_RANK: dict[EvidenceTier, int] = {
    EvidenceTier.UNVALIDATED: 0,
    EvidenceTier.HISTORICAL_ONLY: 1,
    EvidenceTier.PROSPECTIVE_SHADOW: 2,
    EvidenceTier.PROMOTION_ELIGIBLE: 3,
}


@dataclass(frozen=True, slots=True)
class MarketRecommendationRanking:
    market: Market
    top_long: tuple[RecommendationEnvelope, ...]
    top_short: tuple[RecommendationEnvelope, ...]
    no_entry: tuple[RecommendationEnvelope, ...]
    expired: tuple[RecommendationEnvelope, ...]


@dataclass(frozen=True, slots=True)
class RecommendationRanking:
    as_of_ms: int
    markets: tuple[MarketRecommendationRanking, ...]

    def for_market(self, market: Market) -> MarketRecommendationRanking | None:
        return next((item for item in self.markets if item.market is market), None)


def rank_recommendations(
    recommendations: Iterable[RecommendationEnvelope],
    *,
    as_of_ms: int,
    limit_per_action: int = 1,
) -> RecommendationRanking:
    """Return stable per-market candidate and query views at one decision clock."""

    if as_of_ms < 0:
        raise ValueError("as_of_ms must be non-negative")
    if limit_per_action < 1:
        raise ValueError("limit_per_action must be positive")

    grouped: dict[Market, list[RecommendationEnvelope]] = {}
    for recommendation in recommendations:
        grouped.setdefault(recommendation.market, []).append(recommendation)

    markets: list[MarketRecommendationRanking] = []
    for market in sorted(grouped, key=lambda item: item.value):
        items = grouped[market]
        active = [item for item in items if not item.is_expired(as_of_ms)]
        expired = tuple(
            sorted(
                (item for item in items if item.is_expired(as_of_ms)),
                key=_stable_age_key,
            )
        )
        candidates = [item for item in active if item.is_actionable]
        no_entry = tuple(
            sorted(
                (
                    item
                    for item in active
                    if item.action is RecommendationAction.NO_ENTRY
                ),
                key=_latest_key,
            )
        )
        markets.append(
            MarketRecommendationRanking(
                market=market,
                top_long=tuple(
                    item
                    for item in _rank_candidates(candidates)
                    if item.action is RecommendationAction.LONG
                )[:limit_per_action],
                top_short=tuple(
                    item
                    for item in _rank_candidates(candidates)
                    if item.action is RecommendationAction.SHORT
                )[:limit_per_action],
                no_entry=no_entry,
                expired=expired,
            )
        )
    return RecommendationRanking(as_of_ms=as_of_ms, markets=tuple(markets))


def _rank_candidates(
    candidates: Iterable[RecommendationEnvelope],
) -> tuple[RecommendationEnvelope, ...]:
    return tuple(sorted(candidates, key=_candidate_key))


def _candidate_key(item: RecommendationEnvelope) -> tuple[int, int, int, str, str]:
    return (
        -int(item.kind is RecommendationKind.ENTRY_CANDIDATE),
        -_EVIDENCE_TIER_RANK[item.evidence_tier],
        -(item.evidence_strength if item.evidence_strength is not None else -1),
        item.symbol,
        item.event_id,
    )


def _latest_key(item: RecommendationEnvelope) -> tuple[int, str, str]:
    return (-item.decision_time_ms, item.symbol, item.event_id)


def _stable_age_key(item: RecommendationEnvelope) -> tuple[str, str, int, str]:
    return (item.market.value, item.symbol, item.decision_time_ms, item.event_id)
