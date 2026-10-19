from __future__ import annotations

import hashlib
import json
from typing import Final

PUBLIC_STRUCTURE_FLOW_VERSION: Final = "public-structure-flow-v2"
PUBLIC_STRUCTURE_FLOW_SCHEMA_VERSION: Final = "public_structure_flow_shadow_v2"
PUBLIC_STRUCTURE_FLOW_SOURCE_IDENTITY: Final = "binance-public-closed-kline-shadow-v2"

# The canonical JSON string is the immutable machine authority. Callers receive a
# newly decoded object, so mutating a returned dictionary cannot alter the hash.
_CONTRACT_OBJECT: Final[dict[str, object]] = {
    "candidate_version": PUBLIC_STRUCTURE_FLOW_VERSION,
    "decision_interval": "5m",
    "closed_candles_only": True,
    "population": "event_time_top5_plus_retained_market_watch",
    "post_event_current_liquidity_gate_used": False,
    "both_prior_band_touches": "withhold_direction",
    "numeric_comparison": {
        "abs_tolerance": 1e-12,
        "rel_tolerance": 1e-12,
        "inclusive": "accept_exact_or_isclose_boundary",
        "strict": "reject_exact_or_isclose_boundary",
        "zero_sign": "abs_value_within_tolerance_is_zero",
    },
    "minute_primary_consistency": {
        "required_for": "C1",
        "five_exact_1m_bars": True,
        "fields": [
            "open",
            "high",
            "low",
            "close",
            "quote_volume",
            "taker_buy_quote_volume",
        ],
        "price_abs_tolerance": "0.000000000001",
        "price_rel_tolerance": "0.000000000001",
        "quote_abs_tolerance": "0.00000001",
        "quote_rel_tolerance": "0.000000000001",
        "comparison": "abs(actual-expected)<=max(abs_tol,abs(expected)*rel_tol)",
        "utc_grid_required": True,
        "nonnegative_finite_quote_required": True,
        "taker_buy_quote_lte_quote_required": True,
    },
    "pending": {
        "bundle_policy": "one_final_row_for_C1_C2_C3",
        "wait_ms": 600_000,
        "max_pending_per_runtime": 128,
        "event_time_and_ready_time_separate": True,
        "restart": "reload_durable_pending_and_reuse_frozen_event_time_evidence",
        "deadline": "record_row_with_C1_unavailable_and_preserve_C2_C3",
        "symbol_exit": "record_row_with_C1_unavailable_and_preserve_C2_C3",
        "future_watch_or_htf_evidence": "forbidden",
    },
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

PUBLIC_STRUCTURE_FLOW_CONTRACT_JSON: Final = json.dumps(
    _CONTRACT_OBJECT,
    allow_nan=False,
    ensure_ascii=False,
    separators=(",", ":"),
    sort_keys=True,
)
PUBLIC_STRUCTURE_FLOW_CONTRACT_SHA256: Final = hashlib.sha256(
    PUBLIC_STRUCTURE_FLOW_CONTRACT_JSON.encode("utf-8")
).hexdigest()


def public_structure_flow_contract() -> dict[str, object]:
    """Return an isolated copy of the frozen v2 contract."""

    value = json.loads(PUBLIC_STRUCTURE_FLOW_CONTRACT_JSON)
    if not isinstance(value, dict):  # pragma: no cover - constant is an object
        raise AssertionError("public structure-flow contract must decode to an object")
    return value
