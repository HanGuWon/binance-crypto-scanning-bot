from __future__ import annotations

from types import SimpleNamespace

from conftest import make_feature
from signalbot.config import Settings
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import ChartStructureSnapshot, ComparatorCandidate
from signalbot.prospective.observer import build_observation_payload
from signalbot.prospective.research_context import RESEARCH_CONTEXT_VERSION


def test_observation_payload_embeds_versioned_research_context() -> None:
    settings = Settings.model_validate(
        {
            "binance": {
                "markets": ["spot", "futures"],
                "top_n": 5,
                "surveillance_n": 10,
                "intervals": ["5m", "15m", "1h"],
                "primary_interval": "5m",
                "min_quote_volume": 0,
            },
            "signals": {
                "entry_policy": "r2_pit_htf_exec",
                "gate_enabled": True,
                "confirmation_mode": "explicit_trigger",
            },
            "shadow": {
                "observation_enabled": True,
                "campaign_id": "smoke-research-context",
                "source_identity": "test-source",
                "campaign_created_at_ms": 1_709_999_999_000,
            },
        }
    )
    feature = make_feature(
        market=Market.FUTURES,
        chart_structure=ChartStructureSnapshot(
            state="bearish",
            pullback_direction="short",
            pullback_status="ready",
            impulse_size_atr=2.4,
            pullback_depth=0.35,
            pullback_duration_bars=3,
            confluence_distance_atr=0.1,
            recovery_confirmed=True,
            structure_intact=True,
        ),
        closed_kline_flow_available=True,
        taker_imbalance=-0.25,
        cvd_pressure=-0.3,
        funding_rate=0.0002,
        funding_zscore=1.1,
        book_age_ms=100,
        bid_quote_capacity=10_000.0,
        ask_quote_capacity=10_000.0,
    )
    contexts = {
        "15m": make_feature(
            market=Market.FUTURES,
            interval="15m",
            event_time_ms=feature.event_time_ms - 1,
        ),
        "1h": make_feature(
            market=Market.FUTURES,
            interval="1h",
            event_time_ms=feature.event_time_ms - 1,
        ),
    }
    candidate = ComparatorCandidate(
        market=Market.FUTURES,
        symbol=feature.symbol,
        family=SignalFamily.BREAKDOWN_SHORT,
        direction=Direction.SHORT,
        decision_time_ms=feature.event_time_ms,
        primary_interval="5m",
        raw_c0_triggered=True,
        raw_score=55,
        r2_passed=False,
        r2_failures=("test",),
        shadow_passed=False,
        shadow_failures=("test",),
        shadow_gate={},
    )
    observer = SimpleNamespace(
        settings=settings,
        campaign_id="smoke-research-context",
        campaign_manifest_sha256="a" * 64,
        activation_ms=None,
        policy_sha256="b" * 64,
        config_sha256="c" * 64,
        schema_version="shadow_observation_v1",
    )

    payload = build_observation_payload(
        observer,
        candidate,
        feature,
        contexts,
        "opportunity-1",
    )

    research = payload["research_context"]
    assert research["version"] == RESEARCH_CONTEXT_VERSION
    assert research["readiness"]["fresh_bbo_available"] is True
    assert research["futures"]["funding_rate"] == 0.0002
    assert research["causal_structure"]["state"] == "bearish"
