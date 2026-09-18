from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from signalbot.domain.models import FeatureSnapshot

RESEARCH_CONTEXT_VERSION = "shadow_research_context_v2"


def build_research_context(
    feature: FeatureSnapshot,
    contexts: Mapping[str, FeatureSnapshot],
    *,
    execution_available: bool,
) -> dict[str, Any]:
    """Serialize causal research-only context for one raw-C0 opportunity.

    This payload does not participate in production eligibility. It preserves
    already-computed structure, flow, funding, and readiness coordinates so
    future ablation/retest studies can use the exact decision-time evidence
    without reconstructing it from later data.
    """

    c15 = contexts.get("15m")
    c1 = contexts.get("1h")
    structure = feature.chart_structure
    return {
        "version": RESEARCH_CONTEXT_VERSION,
        "readiness": {
            "htf_15m_available": c15 is not None,
            "htf_1h_available": c1 is not None,
            "fresh_bbo_available": execution_available,
            "closed_kline_flow_available": feature.closed_kline_flow_available,
            "intrabar_flow_available": feature.intrabar_taker_imbalance_60s is not None,
            "funding_available": feature.funding_rate is not None,
            "structure_available": structure.state != "unavailable",
            "causal_pullback_available": structure.pullback_status
            not in {"unavailable", "none"},
        },
        "regime": {
            "label": feature.regime.label,
            "btc_trend": feature.regime.btc_trend,
            "breadth_ratio": feature.regime.breadth_ratio,
        },
        "trend_quality": {
            "ema20_slope_atr": feature.ema20_slope_atr,
            "ema20_distance_atr": feature.ema20_distance_atr,
            "efficiency_ratio_20": feature.efficiency_ratio_20,
            "relative_volume": feature.relative_volume,
            "upper_wick_ratio": feature.upper_wick_ratio,
            "lower_wick_ratio": feature.lower_wick_ratio,
            "bearish_divergence": feature.bearish_divergence,
            "bullish_divergence": feature.bullish_divergence,
        },
        "flow": {
            "taker_buy_ratio": feature.taker_buy_ratio,
            "taker_imbalance": feature.taker_imbalance,
            "cvd_pressure": feature.cvd_pressure,
            "intrabar_taker_imbalance_60s": feature.intrabar_taker_imbalance_60s,
            "taker_delta_3": feature.taker_delta_3,
            "taker_delta_12": feature.taker_delta_12,
            "taker_delta_unavailable_reason": feature.taker_delta_unavailable_reason,
            "normalized_vpci": feature.normalized_vpci,
            "normalized_vpci_signal": feature.normalized_vpci_signal,
            "normalized_vpci_slope_3": feature.normalized_vpci_slope_3,
            "normalized_vpci_unavailable_reason": feature.normalized_vpci_unavailable_reason,
        },
        "futures": {
            "funding_rate": feature.funding_rate,
            "funding_zscore": feature.funding_zscore,
        },
        "causal_structure": structure.model_dump(mode="json"),
    }
