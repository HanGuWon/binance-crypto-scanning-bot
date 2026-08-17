from decimal import Decimal

from signalbot.config import SignalSettings
from signalbot.data.anomaly import AnomalyDetector
from signalbot.domain.enums import Market, SignalFamily, SignalStage
from signalbot.domain.models import MarketRegime, MiniTicker
from signalbot.signals.state_machine import SignalStateMachine


def ticker(index: int, price: float) -> MiniTicker:
    return MiniTicker(
        market=Market.SPOT,
        symbol="TESTUSDT",
        event_time_ms=index * 1_000,
        close=Decimal(str(price)),
    )


def test_anomaly_detector_requires_allowlist_and_emits_intrabar_risk() -> None:
    settings = SignalSettings(
        anomaly_horizon_seconds=10,
        anomaly_min_absolute_return=0.01,
        anomaly_robust_zscore=2,
        anomaly_min_points=5,
        anomaly_history_points=50,
    )
    detector = AnomalyDetector(settings)
    regime = MarketRegime()
    assert detector.update(ticker(0, 100), frozenset(), regime) == ()

    result = None
    prices = [100 + (0.001 if i % 2 else 0) for i in range(10)] + [103]
    for index, price in enumerate(prices):
        evaluations = detector.update(
            ticker(index, price), frozenset({"TESTUSDT"}), regime
        )
        result = next((item for item in evaluations if item.triggered), None)
    assert result is not None
    assert result.family is SignalFamily.PUMP_RISK
    assert result.score >= 80
    assert result.metadata["intrabar"] is True


def test_out_of_order_point_is_ignored() -> None:
    settings = SignalSettings(anomaly_min_points=5, anomaly_history_points=50)
    detector = AnomalyDetector(settings)
    allowed = frozenset({"TESTUSDT"})
    detector.update(ticker(10, 100), allowed, MarketRegime())
    assert detector.update(ticker(9, 200), allowed, MarketRegime()) == ()


def test_valid_idle_evaluations_clear_and_rearm_but_rejected_points_do_not() -> None:
    settings = SignalSettings(
        cooldown_seconds=0,
        anomaly_horizon_seconds=10,
        anomaly_min_absolute_return=0.01,
        anomaly_robust_zscore=2,
        anomaly_min_points=5,
        anomaly_history_points=50,
    )
    detector = AnomalyDetector(settings)
    state = SignalStateMachine(settings, "test")
    allowed = frozenset({"TESTUSDT"})
    decisions = []

    prices = [100 + (0.001 if index % 2 else 0) for index in range(10)] + [103]
    for index, price in enumerate(prices):
        for evaluation in detector.update(ticker(index, price), allowed, MarketRegime()):
            if decision := state.process(evaluation):
                decisions.append(decision)
    assert [item.stage for item in decisions] == [SignalStage.CONFIRMED]

    # A rejected out-of-order point produces no idle evaluation and cannot reset state.
    assert detector.update(ticker(9, 100), allowed, MarketRegime()) == ()
    repeated = detector.update(ticker(11, 106), allowed, MarketRegime())
    assert all(state.process(item) is None for item in repeated)

    # Once the horizon contains only stable valid prices, PUMP_RISK becomes idle.
    for index in range(12, 23):
        for evaluation in detector.update(ticker(index, 106), allowed, MarketRegime()):
            state.process(evaluation)
    rearmed = detector.update(ticker(23, 110), allowed, MarketRegime())
    new_decisions = [state.process(item) for item in rearmed]
    confirmed = [item for item in new_decisions if item is not None]
    assert len(confirmed) == 1
    assert confirmed[0].family is SignalFamily.PUMP_RISK
    assert confirmed[0].stage is SignalStage.CONFIRMED


def test_anomaly_history_is_pruned_on_surveillance_rotation() -> None:
    settings = SignalSettings(anomaly_min_points=5, anomaly_history_points=50)
    detector = AnomalyDetector(settings)
    allowed = frozenset({"TESTUSDT"})
    detector.update(ticker(1, 100), allowed, MarketRegime())
    assert detector.retain_symbols(frozenset({"OTHERUSDT"})) == 1
    # A newly re-added symbol starts warm and emits only valid idle evaluations.
    evaluations = detector.update(ticker(2, 100), allowed, MarketRegime())
    assert len(evaluations) == 2
    assert all(item.score == 0 for item in evaluations)
