from __future__ import annotations

from conftest import make_feature
from signalbot.config import Settings
from signalbot.domain.enums import Market
from signalbot.domain.models import ChartStructureSnapshot, MarketRegime
from signalbot.prospective.observer import shadow_config_sha256
from signalbot.prospective.research_context import (
    RESEARCH_CONTEXT_VERSION,
    build_research_context,
)


def _settings() -> Settings:
    return Settings.model_validate(
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


def test_shadow_config_hash_binds_dynamic_universe_semantics() -> None:
    settings = _settings()
    baseline = shadow_config_sha256(settings)

    changed_refresh = settings.model_copy(deep=True)
    changed_refresh.binance = changed_refresh.binance.model_copy(
        update={
            "universe_refresh_seconds": settings.binance.universe_refresh_seconds + 60
        }
    )
    assert shadow_config_sha256(changed_refresh) != baseline

    changed_confirmations = settings.model_copy(deep=True)
    changed_confirmations.binance = changed_confirmations.binance.model_copy(
        update={
            "universe_change_confirmations": (
                settings.binance.universe_change_confirmations + 1
            )
        }
    )
    assert shadow_config_sha256(changed_confirmations) != baseline


def test_research_context_preserves_causal_structure_flow_and_funding() -> None:
    structure = ChartStructureSnapshot(
        state="bullish",
        pullback_direction="long",
        pullback_status="ready",
        impulse_size_atr=2.5,
        pullback_depth=0.4,
        pullback_duration_bars=4,
        confluence_distance_atr=0.1,
        recovery_confirmed=True,
        structure_intact=True,
    )
    feature = make_feature(
        market=Market.FUTURES,
        interval="5m",
        event_time_ms=1_710_000_000_000,
        chart_structure=structure,
        regime=MarketRegime(
            label="risk_on",
            btc_trend="bullish",
            breadth_ratio=0.7,
        ),
        closed_kline_flow_available=True,
        taker_buy_ratio=0.62,
        taker_imbalance=0.24,
        cvd_pressure=0.31,
        intrabar_taker_imbalance_60s=0.4,
        taker_delta_3=0.1,
        taker_delta_12=0.2,
        normalized_vpci=0.5,
        normalized_vpci_signal=0.4,
        normalized_vpci_slope_3=0.1,
        funding_rate=0.0001,
        funding_zscore=1.2,
    )
    prior = feature.event_time_ms - 1
    contexts = {
        "15m": make_feature(interval="15m", event_time_ms=prior),
        "1h": make_feature(interval="1h", event_time_ms=prior),
    }

    payload = build_research_context(
        feature,
        contexts,
        execution_available=True,
    )

    assert payload["version"] == RESEARCH_CONTEXT_VERSION
    assert payload["readiness"]["causal_pullback_available"] is True
    assert payload["readiness"]["fresh_bbo_available"] is True
    assert payload["causal_structure"]["pullback_status"] == "ready"
    assert payload["causal_structure"]["recovery_confirmed"] is True
    assert payload["flow"]["taker_imbalance"] == 0.24
    assert payload["flow"]["cvd_pressure"] == 0.31
    assert payload["futures"]["funding_rate"] == 0.0001
