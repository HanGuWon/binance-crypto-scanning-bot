from __future__ import annotations

import pytest

from conftest import make_feature
from signalbot.config import Settings, ShadowPolicySettings, SignalSettings
from signalbot.domain.enums import Market, SignalFamily, SignalStage
from signalbot.domain.models import FeatureSnapshot, MarketRegime
from signalbot.signals.rules import SignalRuleEngine
from signalbot.signals.shadow_policy import SHADOW_POLICY_METADATA_KEY
from signalbot.signals.state_machine import SignalStateMachine


def _settings() -> SignalSettings:
    return SignalSettings(gate_enabled=True, confirmation_mode="explicit_trigger")


def _feature(*, market: Market = Market.SPOT, **updates: object) -> FeatureSnapshot:
    values: dict[str, object] = {
        "market": market,
        "price": 102.0,
        "previous_close": 101.0,
        "ema20": 101.0,
        "ema50": 99.0,
        "adx": 25.0,
        "atr": 2.0,
        "atr_percent": 2.0,
        "relative_volume": 2.0,
        "recent_high": 101.0,
        "recent_low": 95.0,
        "efficiency_ratio_20": 0.5,
        "spread_bps": 2.0,
        "spread_is_proxy": False,
        "book_age_ms": 100,
        "bid_quote_capacity": 500.0,
        "ask_quote_capacity": 500.0,
        "regime": MarketRegime(label="neutral", btc_trend="neutral"),
    }
    values.update(updates)
    return make_feature(**values)


def _contexts() -> dict[str, FeatureSnapshot]:
    prior = 1_710_000_000_000 - 1
    return {
        "15m": make_feature(
            market=Market.SPOT,
            interval="15m",
            event_time_ms=prior,
            price=103.0,
            ema20=101.0,
            ema50=99.0,
        ),
        "1h": make_feature(
            market=Market.SPOT,
            interval="1h",
            event_time_ms=prior,
            price=104.0,
            ema20=102.0,
            ema50=99.0,
        ),
    }


def test_shadow_policy_is_informational_only_and_never_confirms() -> None:
    settings = _settings()
    engine = SignalRuleEngine(settings, ShadowPolicySettings())
    evaluations = engine.evaluate_research_shadow(_feature(), _contexts())

    assert len(evaluations) == 8
    breakout = next(item for item in evaluations if item.family is SignalFamily.BREAKOUT_LONG)
    assert breakout.metadata["informational_only"] is True
    assert breakout.eligible is False
    assert breakout.metadata[SHADOW_POLICY_METADATA_KEY] == "er_context_v1"
    assert breakout.triggered is True

    machine = SignalStateMachine(settings, "shadow-test")
    decision = machine.process(breakout)
    assert decision is not None
    assert decision.stage is SignalStage.WATCH
    assert decision.stage is not SignalStage.CONFIRMED
    assert machine.decision_for_research_entry(breakout) is None


def test_shadow_policy_restricts_family_to_spot_long_and_futures_short() -> None:
    engine = SignalRuleEngine(_settings(), ShadowPolicySettings())
    spot = engine.evaluate_research_shadow(_feature(market=Market.SPOT), _contexts())
    breakdown = next(item for item in spot if item.family is SignalFamily.BREAKDOWN_SHORT)
    assert "inactive under shadow_er_context_v1" in breakdown.reasons

    futures = engine.evaluate_research_shadow(_feature(market=Market.FUTURES), _contexts())
    breakout = next(item for item in futures if item.family is SignalFamily.BREAKOUT_LONG)
    assert "inactive under shadow_er_context_v1" in breakout.reasons


def test_shadow_policy_fails_closed_on_missing_efficiency_ratio() -> None:
    settings = _settings()
    engine = SignalRuleEngine(settings, ShadowPolicySettings())
    feature = _feature(efficiency_ratio_20=None)
    evaluations = engine.evaluate_research_shadow(feature, _contexts())
    breakout = next(item for item in evaluations if item.family is SignalFamily.BREAKOUT_LONG)
    assert breakout.triggered is False
    assert breakout.eligible is False
    gate = breakout.metadata["shadow_gate"]
    assert gate["passed"] is False
    assert any("efficiency ratio" in item for item in gate["failures"])


def test_shadow_er_context_v1_is_not_a_selectable_production_policy() -> None:
    # The standalone shadow successor must never be selectable as a production
    # entry policy. Pydantic's Literal rejects the legacy value at load time.
    with pytest.raises(ValueError, match="entry_policy"):
        Settings.model_validate(
            {
                "binance": {
                    "markets": ["spot"],
                    "top_n": 1,
                    "surveillance_n": 2,
                    "intervals": ["5m", "15m", "1h"],
                    "primary_interval": "5m",
                },
                "signals": {"entry_policy": "shadow_er_context_v1"},
                "storage": {"url": "sqlite:///:memory:"},
            }
        )
