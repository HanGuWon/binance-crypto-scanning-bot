from __future__ import annotations

from collections.abc import Mapping

from signalbot.config import ShadowPolicySettings, SignalSettings
from signalbot.domain.enums import Direction
from signalbot.domain.models import FeatureSnapshot, RuleEvaluation
from signalbot.signals.gates import (
    evaluate_fresh_bbo_execution,
    evaluate_strict_prior_htf,
)

SHADOW_POLICY_METADATA_KEY = "shadow_policy"
SHADOW_GATE_METADATA_KEY = "shadow_gate"


def evaluate_shadow_gate(
    evaluation: RuleEvaluation,
    feature: FeatureSnapshot,
    contexts: Mapping[str, FeatureSnapshot],
    signals: SignalSettings,
    shadow: ShadowPolicySettings,
) -> RuleEvaluation:
    """Apply the conjunctive shadow successor gate to one raw rule evaluation.

    The shadow policy is informational-only by construction: the returned
    evaluation is never ``eligible`` and can never reach CONFIRMED. ``triggered``
    means only that every shadow gate passed for the frozen raw trigger; it is
    a prospective research observation, not an entry recommendation.

    Gates (all conjunctive, none compensating):
      1. raw C0 breakout/breakdown trigger complete;
      2. strictly-prior 15m and 1h close/EMA20/EMA50 alignment;
      3. BTC common-factor context does not oppose the direction (optional);
      4. EMA20/EMA50 aligned and 20-bar efficiency ratio above the floor;
      5. one participation family: relative volume above the signal threshold;
      6. anti-chase: distance from the broken boundary is bounded in ATR;
      7. cost headroom: one-bar ATR% covers a multiple of the round-trip cost;
      8. fresh observed BBO execution evidence (shared R2 contract).
    """

    direction = evaluation.direction
    failures: list[str] = []
    reasons = list(evaluation.reasons)

    if evaluation.score == 0:
        reasons.append("shadow: raw trigger absent")
        failures.append("raw trigger absent")
    elif not evaluation.triggered:
        failures.append("raw C0 trigger incomplete")

    strict = evaluate_strict_prior_htf(feature, direction, contexts)
    if not strict.accepted:
        failures.extend(f"htf: {item}" for item in strict.failures)

    if shadow.require_btc_context_aligned:
        btc_trend = feature.regime.btc_trend
        opposed = (
            (direction is Direction.LONG and btc_trend == "bearish")
            or (direction is Direction.SHORT and btc_trend == "bullish")
        )
        if opposed:
            failures.append(f"btc context opposes direction ({btc_trend})")

    aligned = (
        feature.ema20 > feature.ema50
        if direction is Direction.LONG
        else feature.ema20 < feature.ema50
    )
    if not aligned:
        failures.append("EMA20/EMA50 not directionally aligned")

    efficiency = feature.efficiency_ratio_20
    if efficiency is None or efficiency < shadow.efficiency_ratio_min:
        value = "unavailable" if efficiency is None else f"{efficiency:.3f}"
        failures.append(
            f"efficiency ratio {value} below {shadow.efficiency_ratio_min:.2f}"
        )

    if feature.relative_volume < signals.relative_volume_threshold:
        failures.append(
            f"relative volume {feature.relative_volume:.2f} below "
            f"{signals.relative_volume_threshold:.2f}"
        )

    if feature.atr > 0:
        distance = (
            feature.price - feature.recent_high
            if direction is Direction.LONG
            else feature.recent_low - feature.price
        )
        distance_atr = distance / feature.atr
        if distance_atr > shadow.breakout_max_distance_atr:
            failures.append(
                f"chase distance {distance_atr:.2f} ATR exceeds "
                f"{shadow.breakout_max_distance_atr:.2f} ATR"
            )

    required_headroom_bps = (
        shadow.cost_headroom_multiple * shadow.round_trip_cost_bps
    )
    if 100 * feature.atr_percent < required_headroom_bps:
        failures.append(
            f"ATR headroom {feature.atr_percent * 100:.2f} bps below "
            f"{required_headroom_bps:.2f} bps cost floor"
        )

    _execution_score, execution_failures = evaluate_fresh_bbo_execution(
        feature, direction, signals
    )
    failures.extend(f"execution: {item}" for item in execution_failures)

    passed = not failures
    metadata = {
        **evaluation.metadata,
        "informational_only": True,
        SHADOW_POLICY_METADATA_KEY: shadow.policy_version,
        "threshold_status": "unvalidated_shadow_seed",
        SHADOW_GATE_METADATA_KEY: {
            "passed": passed,
            "failures": failures,
            "efficiency_ratio_20": efficiency,
        },
    }
    if not passed:
        reasons.extend(f"shadow: {item}" for item in failures)
        return evaluation.model_copy(
            update={
                "eligible": False,
                "triggered": False,
                "reasons": tuple(reasons),
                "metadata": metadata,
            }
        )
    reasons.append("all shadow_er_context_v1 gates passed (shadow observation)")
    return evaluation.model_copy(
        update={
            "eligible": False,
            "triggered": True,
            "reasons": tuple(reasons),
            "metadata": metadata,
        }
    )
