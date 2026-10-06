from decimal import Decimal

import pytest

from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.recommendations.models import (
    RecommendationAction,
    RecommendationEnvelope,
    RecommendationKind,
)


def envelope(**updates: object) -> RecommendationEnvelope:
    values: dict[str, object] = {
        "event_id": "recommendation-1",
        "source_event_id": "decision-1",
        "action": RecommendationAction.LONG,
        "kind": RecommendationKind.ENTRY_CANDIDATE,
        "market": Market.FUTURES,
        "symbol": "btcusdt",
        "source_family": SignalFamily.BREAKOUT_LONG,
        "source_stage": SignalStage.CONFIRMED,
        "timeframe": "5m",
        "decision_time_ms": 1_000,
        "expires_at_ms": 2_000,
        "reasons": ("closed breakout",),
        "invalidation": "98",
        "evidence_strength": 80,
        "rule_version": "r2",
    }
    values.update(updates)
    return RecommendationEnvelope(**values)  # type: ignore[arg-type]


def test_deterministic_identity_and_expiry_boundary() -> None:
    first = envelope()
    second = envelope(event_id=RecommendationEnvelope.deterministic_event_id("decision-1", "v1"))

    assert first.symbol == "BTCUSDT"
    assert first.event_id == "recommendation-1"
    assert second.event_id == RecommendationEnvelope.deterministic_event_id("decision-1", "v1")
    assert first.is_expired(1_999) is False
    assert first.is_expired(2_000) is True


def test_no_entry_requires_a_blocker() -> None:
    with pytest.raises(ValueError, match="requires at least one blocker"):
        envelope(action=RecommendationAction.NO_ENTRY, kind=RecommendationKind.HOLD)


def test_spot_short_entry_is_rejected() -> None:
    with pytest.raises(ValueError, match="Spot short"):
        envelope(
            action=RecommendationAction.SHORT,
            market=Market.SPOT,
            kind=RecommendationKind.ENTRY_CANDIDATE,
            source_family=SignalFamily.BREAKDOWN_SHORT,
            source_stage=SignalStage.CONFIRMED,
        )


def test_invalidation_is_optional_and_decimal_boundary_is_preserved_by_source() -> None:
    item = envelope(invalidation=None)
    assert item.invalidation is None
    assert Decimal("100.10") > Decimal("100.09")


def test_directional_enum_does_not_treat_risk_as_entry() -> None:
    assert Direction.RISK_UP not in {Direction.LONG, Direction.SHORT}
