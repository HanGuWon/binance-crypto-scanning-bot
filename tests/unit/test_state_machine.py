from decimal import Decimal
from typing import Literal

import pytest

from signalbot.config import SignalSettings
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import RuleEvaluation
from signalbot.signals.state_machine import SignalStateMachine


def rule(
    score: int,
    timestamp: int,
    *,
    symbol: str = "BTCUSDT",
    triggered: bool = False,
    eligible: bool = True,
    invalidation: Decimal | None = Decimal("98"),
    metadata: dict[str, object] | None = None,
) -> RuleEvaluation:
    return RuleEvaluation(
        market=Market.FUTURES,
        symbol=symbol,
        family=SignalFamily.BREAKOUT_LONG,
        direction=Direction.LONG,
        timeframe="5m",
        event_time_ms=timestamp,
        score=score,
        triggered=triggered,
        eligible=eligible,
        price=Decimal("100"),
        reasons=("test",),
        invalidation=invalidation,
        metadata=metadata or {},
    )


def test_state_transitions_emit_once_and_use_deterministic_ids() -> None:
    settings = SignalSettings(confirmation_mode="score", cooldown_seconds=10)
    machine = SignalStateMachine(settings, "v1")
    watch = machine.process(rule(60, 1_000))
    assert watch is not None and watch.stage is SignalStage.WATCH
    assert machine.process(rule(60, 1_000)) is None
    setup = machine.process(rule(70, 2_000))
    assert setup is not None and setup.stage is SignalStage.SETUP
    confirmed = machine.process(rule(80, 3_000))
    assert confirmed is not None and confirmed.stage is SignalStage.CONFIRMED

    second = SignalStateMachine(settings, "v1")
    assert second.process(rule(60, 1_000)).event_id == watch.event_id  # type: ignore[union-attr]


def test_watch_invalidates_when_score_drops_below_threshold() -> None:
    machine = SignalStateMachine(SignalSettings(), "v1")
    assert machine.process(rule(60, 1_000)) is not None
    invalidated = machine.process(rule(0, 2_000, invalidation=None))
    assert invalidated is not None
    assert invalidated.stage is SignalStage.INVALIDATED
    assert invalidated.invalidation == Decimal("98")


def test_cooldown_suppresses_alerts_but_updates_internal_stage() -> None:
    machine = SignalStateMachine(
        SignalSettings(confirmation_mode="score", cooldown_seconds=10), "v1"
    )
    first = machine.process(rule(80, 1_000))
    assert first is not None
    assert machine.process(rule(70, 5_000)) is None
    assert machine.process(rule(70, 12_000)) is None

    watch = machine.process(rule(60, 13_000))
    assert watch is not None
    assert watch.stage is SignalStage.WATCH


def test_explicit_trigger_confirms_raw_65_when_eligible() -> None:
    machine = SignalStateMachine(
        SignalSettings(confirmation_mode="explicit_trigger"), "v2"
    )

    confirmed = machine.process(rule(65, 1_000, triggered=True, eligible=True))

    assert confirmed is not None
    assert confirmed.stage is SignalStage.CONFIRMED
    assert confirmed.score == 65


def test_explicit_trigger_requires_eligibility() -> None:
    machine = SignalStateMachine(
        SignalSettings(confirmation_mode="explicit_trigger"), "v2"
    )

    watch = machine.process(rule(65, 1_000, triggered=True, eligible=False))

    assert watch is not None
    assert watch.stage is SignalStage.WATCH


def test_score_confirmation_requires_eligibility() -> None:
    machine = SignalStateMachine(
        SignalSettings(confirmation_mode="score", confirmed_score=80), "v2"
    )

    setup = machine.process(rule(100, 1_000, triggered=True, eligible=False))

    assert setup is not None
    assert setup.stage is SignalStage.SETUP


def test_state_machine_revalidates_model_copy_updates_at_its_trust_boundary() -> None:
    machine = SignalStateMachine(SignalSettings(), "v2")
    malformed = rule(80, 1_000).model_copy(update={"direction": Direction.SHORT})

    with pytest.raises(ValueError, match="incompatible with direction"):
        machine.process(malformed)


@pytest.mark.parametrize("confirmation_mode", ["explicit_trigger", "score"])
def test_informational_metadata_is_a_defense_in_depth_confirmation_lock(
    confirmation_mode: Literal["explicit_trigger", "score"],
) -> None:
    machine = SignalStateMachine(
        SignalSettings(confirmation_mode=confirmation_mode), "v2"
    )
    informational = rule(
        100,
        1_000,
        triggered=True,
        eligible=True,
        metadata={"informational_only": True},
    )

    setup = machine.process(informational)

    assert setup is not None
    assert setup.stage is SignalStage.SETUP
    assert machine.decision_for_research_entry(informational) is None
    with pytest.raises(ValueError, match="confirmation-eligible"):
        machine.decision_for_confirmed_trigger(informational)


def test_explicit_untriggered_score_above_confirmation_caps_at_setup() -> None:
    machine = SignalStateMachine(
        SignalSettings(confirmation_mode="explicit_trigger"), "v2"
    )

    setup = machine.process(rule(85, 1_000, triggered=False, eligible=True))

    assert setup is not None
    assert setup.stage is SignalStage.SETUP


def test_research_trigger_decision_is_independent_of_alert_cooldown() -> None:
    machine = SignalStateMachine(
        SignalSettings(confirmation_mode="explicit_trigger", cooldown_seconds=1_800),
        "v2",
    )
    first_rule = rule(65, 1_000, triggered=True, eligible=True)
    second_rule = rule(65, 2_000, triggered=True, eligible=True)
    assert machine.process(first_rule) is not None
    assert machine.process(second_rule) is None

    first = machine.decision_for_confirmed_trigger(first_rule)
    second = machine.decision_for_confirmed_trigger(second_rule)

    assert first.stage is second.stage is SignalStage.CONFIRMED
    assert first.event_id != second.event_id


def test_research_entry_respects_confirmation_mode_and_score_boundary() -> None:
    explicit = SignalStateMachine(
        SignalSettings(confirmation_mode="explicit_trigger"),
        "explicit",
    )
    score = SignalStateMachine(
        SignalSettings(confirmation_mode="score", confirmed_score=80),
        "score",
    )

    assert explicit.decision_for_research_entry(
        rule(65, 1_000, triggered=True)
    ) is not None
    assert score.decision_for_research_entry(
        rule(79, 1_000, triggered=True)
    ) is None
    assert score.decision_for_research_entry(
        rule(80, 2_000, triggered=False)
    ) is not None
    assert score.decision_for_research_entry(
        rule(80, 3_000, triggered=True, eligible=False)
    ) is None


def test_prune_symbols_removes_only_inactive_symbol_state() -> None:
    machine = SignalStateMachine(SignalSettings(), "v1")
    btc = rule(60, 1_000, symbol="BTCUSDT")
    eth = rule(60, 1_000, symbol="ETHUSDT")
    assert machine.process(btc) is not None
    assert machine.process(eth) is not None

    assert machine.prune_symbols({"btcusdt"}) == 1
    assert machine.process(btc) is None
    assert machine.process(eth) is not None
    assert machine.prune_symbols(set()) == 2
