from decimal import Decimal

from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import GateEvaluation, SignalDecision
from signalbot.recommendations import (
    RecommendationAction,
    RecommendationKind,
    project_decision,
    project_decisions,
)


def decision(
    *,
    market: Market = Market.FUTURES,
    direction: Direction = Direction.LONG,
    family: SignalFamily = SignalFamily.BREAKOUT_LONG,
    stage: SignalStage = SignalStage.CONFIRMED,
    event_id: str = "decision-1",
    metadata: dict[str, object] | None = None,
    invalidation: Decimal | None = Decimal("98"),
    gate: GateEvaluation | None = None,
) -> SignalDecision:
    return SignalDecision(
        event_id=event_id,
        market=market,
        symbol="BTCUSDT",
        family=family,
        stage=stage,
        direction=direction,
        timeframe="5m",
        event_time_ms=1_000,
        score=80,
        price=Decimal("100"),
        reasons=("closed candle",),
        invalidation=invalidation,
        gate=gate,
        rule_version="r2",
        metadata=metadata or {},
    )


def test_confirmed_futures_long_and_short_become_entry_candidates() -> None:
    long = project_decision(decision())
    short = project_decision(
        decision(
            direction=Direction.SHORT,
            family=SignalFamily.BREAKDOWN_SHORT,
            event_id="decision-2",
            invalidation=Decimal("102"),
        )
    )

    assert long.action is RecommendationAction.LONG
    assert long.kind is RecommendationKind.ENTRY_CANDIDATE
    assert short.action is RecommendationAction.SHORT
    assert short.kind is RecommendationKind.ENTRY_CANDIDATE


def test_spot_short_direction_becomes_exit_warning() -> None:
    result = project_decision(
        decision(
            market=Market.SPOT,
            direction=Direction.SHORT,
            family=SignalFamily.BREAKDOWN_SHORT,
            invalidation=Decimal("102"),
        )
    )

    assert result.action is RecommendationAction.SHORT
    assert result.kind is RecommendationKind.EXIT_WARNING
    assert "spot_short_direction_maps_to_spot_exit" in result.blockers


def test_preconfirmation_and_failed_gate_become_no_entry() -> None:
    result = project_decision(
        decision(
            stage=SignalStage.SETUP,
            gate=GateEvaluation(
                trend_score=50,
                participation_score=50,
                crowding_risk_score=50,
                execution_score=50,
                completeness_score=50,
                passed=False,
                failures=("fresh_bbo",),
            ),
        )
    )

    assert result.action is RecommendationAction.NO_ENTRY
    assert result.kind is RecommendationKind.HOLD
    assert "stage_not_confirmed:setup" in result.blockers
    assert "fresh_bbo" in result.blockers


def test_risk_warning_and_bad_invalidation_never_become_entries() -> None:
    risk = project_decision(
        decision(
            family=SignalFamily.PUMP_RISK,
            direction=Direction.RISK_UP,
            invalidation=None,
        )
    )
    bad_stop = project_decision(decision(invalidation=Decimal("101")))

    assert risk.kind is RecommendationKind.RISK_WARNING
    assert risk.action is RecommendationAction.NO_ENTRY
    assert bad_stop.action is RecommendationAction.NO_ENTRY
    assert "long_invalidation_not_below_price" in bad_stop.blockers


def test_open_or_future_context_is_fail_closed_and_batch_order_is_stable() -> None:
    open_context = project_decision(
        decision(metadata={"candle_is_closed": False, "context_event_time_ms": 1_000})
    )
    decisions = [decision(event_id="z"), decision(event_id="a")]
    batch = project_decisions(decisions)

    assert open_context.action is RecommendationAction.NO_ENTRY
    assert "open_candle" in open_context.blockers
    assert "higher_timeframe_context_is_not_strictly_prior" in open_context.blockers
    assert [item.event_id for item in batch] == sorted(item.event_id for item in batch)
    assert project_decision(decision()).event_id == project_decision(decision()).event_id
