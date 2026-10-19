from __future__ import annotations

import hashlib
import json
from typing import Final

PUBLIC_STRUCTURE_FLOW_VERSION: Final = "public-structure-flow-v1"
PUBLIC_STRUCTURE_FLOW_SCHEMA_VERSION: Final = "public_structure_flow_shadow_v1"
PUBLIC_STRUCTURE_FLOW_SOURCE_IDENTITY: Final = "binance-public-closed-kline-shadow-v1"

# This object is the machine-readable authority for the first C1/C2/C3 shadow batch.
# Changing any semantic value requires a new candidate version and a new hash.
PUBLIC_STRUCTURE_FLOW_CONTRACT: Final[dict[str, object]] = {
    "candidate_version": PUBLIC_STRUCTURE_FLOW_VERSION,
    "decision_interval": "5m",
    "closed_candles_only": True,
    "same_close_1m_required": True,
    "population": "event_time_top5_plus_retained_market_watch",
    "post_event_current_liquidity_gate_used": False,
    "both_prior_band_touches": "withhold_direction",
    "bollinger": {
        "period": 20,
        "standard_deviation": "population",
        "deviations": 2.0,
        "touch_reference": "current_5m_extreme_vs_band_through_t_minus_1",
    },
    "macd": {"fast": 12, "slow": 26, "signal": 9},
    "c1": {
        "id": "PUB09_FLOW_PRICE_RESPONSE",
        "recent_minutes": 3,
        "previous_minutes": 3,
        "signed_quote_flow_threshold": 0.20,
        "price_response_atr_threshold": 0.25,
        "atr": {"period": 14, "interval": "1m", "reference": "before_recent_window"},
        "upper_confirmation": "last_1m_close_lt_previous_1m_low",
        "lower_confirmation": "last_1m_close_gt_previous_1m_high",
    },
    "c2": {
        "id": "PUB02_ATR_IMPULSE_CONTINUATION",
        "impulse_atr_threshold": 1.5,
        "atr": {"period": 14, "interval": "5m", "reference": "t_minus_1"},
        "bandwidth_expansion": "strict_gt_1",
        "signed_quote_flow_threshold": 0.20,
        "close_must_be_outside_prior_band": True,
        "strict_prior_htf": ["15m", "1h"],
    },
    "c3": {
        "id": "PUB03_BB_MACD_REACTION",
        "previous_percent_b_long": "lte_0",
        "previous_percent_b_short": "gte_1",
        "current_percent_b": "strictly_between_0_and_1",
        "long_macd": "line_lt_0_and_histogram_gt_0",
        "short_macd": "line_gt_0_and_histogram_lt_0",
    },
    "production_entry": False,
    "discord_delivery": False,
    "order_placement": False,
}


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256: Final = hashlib.sha256(
    _canonical_json(PUBLIC_STRUCTURE_FLOW_CONTRACT).encode("utf-8")
).hexdigest()
