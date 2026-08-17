from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal

from signalbot.config import SignalSettings
from signalbot.domain.enums import Direction, SignalFamily
from signalbot.domain.models import (
    DIRECTIONAL_DIAGNOSTICS_METADATA_KEY,
    DirectionalDiagnostics,
    DirectionalSetupScore,
    FeatureSnapshot,
    RuleEvaluation,
)
from signalbot.signals.gates import evaluate_entry_gates


def _decimal(value: float) -> Decimal:
    return Decimal(str(round(value, 12)))


def _bounded(score: float) -> int:
    return max(0, min(100, round(score)))


class SignalRuleEngine:
    def __init__(self, settings: SignalSettings) -> None:
        self.settings = settings

    def evaluate(
        self, feature: FeatureSnapshot, contexts: Mapping[str, FeatureSnapshot] | None = None
    ) -> list[RuleEvaluation]:
        context_values = contexts or {}
        if not self.settings.gate_enabled:
            if feature.spread_bps is None:
                return self._with_directional_diagnostics(
                    feature, self._idle(feature, "spread unavailable")
                )
            if feature.spread_bps > self.settings.maximum_spread_bps:
                return self._with_directional_diagnostics(
                    feature,
                    self._idle(
                        feature, f"spread {feature.spread_bps:.2f} bps exceeds gate"
                    ),
                )
        values = [
            self._squeeze(feature, Direction.LONG),
            self._squeeze(feature, Direction.SHORT),
            self._breakout(feature),
            self._breakdown(feature),
            self._pullback(feature, Direction.LONG),
            self._pullback(feature, Direction.SHORT),
            self._exhaustion(feature),
            self._capitulation(feature),
        ]
        if not self.settings.gate_enabled:
            evaluated = [self._apply_context(item, context_values) for item in values]
        else:
            evaluated = [self._apply_gate(item, feature, context_values) for item in values]
        evaluated = [self._lock_informational_only(item) for item in evaluated]
        return self._with_directional_diagnostics(feature, evaluated)

    @staticmethod
    def _lock_informational_only(evaluation: RuleEvaluation) -> RuleEvaluation:
        """Keep research diagnostics visible without making them entry-eligible."""

        if evaluation.metadata.get("informational_only") is not True:
            return evaluation
        reason = "informational-only rule; prospective validation required"
        reasons = evaluation.reasons
        if reason not in reasons:
            reasons = (*reasons, reason)
        return evaluation.model_copy(
            update={
                "eligible": False,
                "reasons": reasons,
            }
        )

    @staticmethod
    def _raw_rule_score(evaluation: RuleEvaluation) -> int:
        value = evaluation.metadata.get("raw_signal_score")
        if isinstance(value, int) and not isinstance(value, bool):
            return max(0, min(100, value))
        return evaluation.score

    def _with_directional_diagnostics(
        self,
        feature: FeatureSnapshot,
        evaluations: list[RuleEvaluation],
    ) -> list[RuleEvaluation]:
        """Attach the best long and short rule without adding correlated scores."""

        def strongest(direction: Direction) -> RuleEvaluation:
            candidates = [item for item in evaluations if item.direction is direction]
            if not candidates:
                raise ValueError(f"missing {direction.value} rule evaluations")

            def priority(item: RuleEvaluation) -> tuple[bool, bool, bool, int, bool, int]:
                raw_score = self._raw_rule_score(item)
                promoting = item.metadata.get("informational_only") is not True
                has_evidence = raw_score > 0
                return (
                    promoting and has_evidence and item.eligible and item.triggered,
                    promoting and has_evidence and item.eligible,
                    promoting and has_evidence,
                    raw_score,
                    item.triggered,
                    item.score,
                )

            return max(
                candidates,
                key=priority,
            )

        def setup(item: RuleEvaluation) -> DirectionalSetupScore:
            return DirectionalSetupScore(
                family=item.family,
                raw_score=self._raw_rule_score(item),
                decision_score=item.score,
                triggered=item.triggered,
                eligible=item.eligible,
            )

        diagnostics = DirectionalDiagnostics(
            long=setup(strongest(Direction.LONG)),
            short=setup(strongest(Direction.SHORT)),
            feature=feature,
        ).model_dump(mode="json")
        return [
            item.model_copy(
                update={
                    "metadata": {
                        **item.metadata,
                        DIRECTIONAL_DIAGNOSTICS_METADATA_KEY: diagnostics,
                    }
                }
            )
            for item in evaluations
        ]

    def _apply_gate(
        self,
        evaluation: RuleEvaluation,
        feature: FeatureSnapshot,
        contexts: Mapping[str, FeatureSnapshot],
    ) -> RuleEvaluation:
        gate = evaluate_entry_gates(
            feature, evaluation.direction, contexts, self.settings
        )
        r2_policy = self.settings.entry_policy == "r2_pit_htf_exec"
        r2_family_allowed = (
            evaluation.family is SignalFamily.BREAKOUT_LONG
            and evaluation.direction is Direction.LONG
            and evaluation.market.value == "spot"
        ) or (
            evaluation.family is SignalFamily.BREAKDOWN_SHORT
            and evaluation.direction is Direction.SHORT
            and evaluation.market.value == "futures"
        )
        informational_only = evaluation.metadata.get("informational_only") is True
        if informational_only:
            gate = gate.model_copy(
                update={
                    "passed": False,
                    "failures": (
                        *gate.failures,
                        "informational-only rule; prospective validation required",
                    ),
                }
            )
        if r2_policy and not r2_family_allowed:
            gate = gate.model_copy(
                update={
                    "passed": False,
                    "failures": (*gate.failures, "outside frozen R2 market/direction family"),
                }
            )
        metadata = {**evaluation.metadata, "raw_signal_score": evaluation.score}
        if evaluation.score == 0:
            return evaluation.model_copy(
                update={"eligible": gate.passed, "gate": gate, "metadata": metadata}
            )
        if not gate.passed:
            reasons = (*evaluation.reasons, *(f"gate: {item}" for item in gate.failures))
            score = evaluation.score if informational_only else (
                evaluation.score
                if self.settings.confirmation_mode == "explicit_trigger" and not r2_policy
                else 0
            )
            return evaluation.model_copy(
                update={
                    "score": score,
                    "eligible": False,
                    "reasons": reasons,
                    "gate": gate,
                    "metadata": metadata,
                }
            )
        reasons = (*evaluation.reasons, "all independent entry gates passed")
        score = evaluation.score
        if self.settings.confirmation_mode == "score":
            score = max(score, self.settings.confirmed_score)
        return evaluation.model_copy(
            update={
                "score": score,
                "eligible": True,
                "reasons": reasons,
                "gate": gate,
                "metadata": metadata,
            }
        )

    def _base(
        self,
        f: FeatureSnapshot,
        family: SignalFamily,
        direction: Direction,
        score: float,
        reasons: list[str],
        invalidation: float | None,
        metadata: dict[str, object] | None = None,
        *,
        triggered: bool = False,
    ) -> RuleEvaluation:
        return RuleEvaluation(
            market=f.market,
            symbol=f.symbol,
            family=family,
            direction=direction,
            timeframe=f.interval,
            event_time_ms=f.event_time_ms,
            score=_bounded(score),
            triggered=triggered,
            price=_decimal(f.price),
            reasons=tuple(reasons),
            invalidation=_decimal(invalidation) if invalidation is not None else None,
            regime=f.regime,
            metadata=metadata or {},
        )

    def _idle(self, f: FeatureSnapshot, reason: str) -> list[RuleEvaluation]:
        pairs = [
            (SignalFamily.SQUEEZE_LONG, Direction.LONG),
            (SignalFamily.SQUEEZE_SHORT, Direction.SHORT),
            (SignalFamily.BREAKOUT_LONG, Direction.LONG),
            (SignalFamily.BREAKDOWN_SHORT, Direction.SHORT),
            (SignalFamily.PULLBACK_LONG, Direction.LONG),
            (SignalFamily.PULLBACK_SHORT, Direction.SHORT),
            (SignalFamily.EXHAUSTION_SHORT, Direction.SHORT),
            (SignalFamily.CAPITULATION_LONG, Direction.LONG),
        ]
        return [self._base(f, a, b, 0, [reason], None) for a, b in pairs]

    def _apply_context(
        self, e: RuleEvaluation, contexts: Mapping[str, FeatureSnapshot]
    ) -> RuleEvaluation:
        if e.score == 0:
            return e
        score = e.score
        reasons = list(e.reasons)
        is_long = e.direction is Direction.LONG
        c15 = contexts.get("15m")
        c1 = contexts.get("1h")
        if c15 is not None and ((c15.ema20 > c15.ema50) if is_long else (c15.ema20 < c15.ema50)):
            score += 5
            reasons.append("15m trend confirms direction")
        if c1 is not None:
            if (c1.price > c1.ema20) if is_long else (c1.price < c1.ema20):
                score += 5
                reasons.append("1h price confirms direction")
            opposed = (
                (c1.price < c1.ema20 < c1.ema50) if is_long else (c1.price > c1.ema20 > c1.ema50)
            )
            if opposed:
                score -= 10
                reasons.append("penalty: 1h structure opposes direction")
        return e.model_copy(update={"score": _bounded(score), "reasons": tuple(reasons)})

    def _squeeze(self, f: FeatureSnapshot, direction: Direction) -> RuleEvaluation:
        family = (
            SignalFamily.SQUEEZE_LONG if direction is Direction.LONG else SignalFamily.SQUEEZE_SHORT
        )
        score = 0.0
        reasons = []
        compressed = f.bollinger_width_percentile <= self.settings.squeeze_percentile
        if compressed:
            score += 35
            reasons.append(f"Bollinger width percentile {f.bollinger_width_percentile:.1f}%")
        distance = (
            f.recent_high - f.price if direction is Direction.LONG else f.price - f.recent_low
        )
        near = 0 <= distance <= max(f.atr, f.price * 0.001)
        if near:
            score += 25
            reasons.append("price within one ATR of range boundary")
        aligned = f.ema20 > f.ema50 if direction is Direction.LONG else f.ema20 < f.ema50
        if aligned:
            score += 15
            reasons.append("EMA20/EMA50 trend aligned")
        if f.atr_percent <= 2:
            score += 10
            reasons.append(f"contained ATR {f.atr_percent:.2f}%")
        if not self.settings.gate_enabled:
            flow = (
                f.taker_buy_ratio >= 0.52
                if direction is Direction.LONG
                else f.taker_buy_ratio <= 0.48
            )
            if flow:
                score += 10
                reasons.append(f"taker-buy ratio {f.taker_buy_ratio:.2f}")
            if (
                f.spread_bps is not None
                and f.spread_bps <= self.settings.maximum_spread_bps / 2
            ):
                score += 5
                reasons.append(f"tight spread {f.spread_bps:.2f} bps")
        if not compressed or not near:
            score = 0
        invalid = (
            f.recent_low - f.atr * 0.25
            if direction is Direction.LONG
            else f.recent_high + f.atr * 0.25
        )
        return self._base(f, family, direction, score, reasons, invalid)

    def _breakout(self, f: FeatureSnapshot) -> RuleEvaluation:
        score = 0.0
        reasons = []
        broke = f.price > f.recent_high and f.previous_close <= f.recent_high
        macd_confirmed = (
            f.macd_histogram > 0
            and f.macd_histogram > f.macd_histogram_previous
        )
        adx_confirmed = f.adx >= 20
        ema_confirmed = f.ema20 > f.ema50
        if broke:
            score += 30
            reasons.append(f"closed above {self.settings.breakout_lookback}-bar high")
        if (
            not self.settings.gate_enabled
            and f.relative_volume >= self.settings.relative_volume_threshold
        ):
            score += 20
            reasons.append(f"relative volume {f.relative_volume:.2f}x")
        if macd_confirmed:
            score += 15
            reasons.append("positive and improving MACD histogram")
        if adx_confirmed:
            score += 10
            reasons.append(f"ADX {f.adx:.1f}")
        if not self.settings.gate_enabled and f.taker_buy_ratio >= 0.55:
            score += 10
            reasons.append(f"taker-buy ratio {f.taker_buy_ratio:.2f}")
        if ema_confirmed:
            score += 10
            reasons.append("EMA20 above EMA50")
        if not self.settings.gate_enabled and f.regime.label != "risk_off":
            score += 5
            reasons.append(f"market regime {f.regime.label}")
        if not broke:
            score = 0
        return self._base(
            f,
            SignalFamily.BREAKOUT_LONG,
            Direction.LONG,
            score,
            reasons,
            min(f.recent_high, f.price - f.atr),
            triggered=broke and macd_confirmed and adx_confirmed and ema_confirmed,
        )

    def _breakdown(self, f: FeatureSnapshot) -> RuleEvaluation:
        score = 0.0
        reasons = []
        broke = f.price < f.recent_low and f.previous_close >= f.recent_low
        macd_confirmed = (
            f.macd_histogram < 0
            and f.macd_histogram < f.macd_histogram_previous
        )
        adx_confirmed = f.adx >= 20
        ema_confirmed = f.ema20 < f.ema50
        if broke:
            score += 30
            reasons.append(f"closed below {self.settings.breakout_lookback}-bar low")
        if (
            not self.settings.gate_enabled
            and f.relative_volume >= self.settings.relative_volume_threshold
        ):
            score += 20
            reasons.append(f"relative volume {f.relative_volume:.2f}x")
        if macd_confirmed:
            score += 15
            reasons.append("negative and weakening MACD histogram")
        if adx_confirmed:
            score += 10
            reasons.append(f"ADX {f.adx:.1f}")
        if not self.settings.gate_enabled and f.taker_buy_ratio <= 0.45:
            score += 10
            reasons.append(f"taker-buy ratio {f.taker_buy_ratio:.2f}")
        if ema_confirmed:
            score += 10
            reasons.append("EMA20 below EMA50")
        if not self.settings.gate_enabled and f.regime.label != "risk_on":
            score += 5
            reasons.append(f"market regime {f.regime.label}")
        if not broke:
            score = 0
        return self._base(
            f,
            SignalFamily.BREAKDOWN_SHORT,
            Direction.SHORT,
            score,
            reasons,
            max(f.recent_low, f.price + f.atr),
            triggered=broke and macd_confirmed and adx_confirmed and ema_confirmed,
        )

    def _pullback(self, f: FeatureSnapshot, direction: Direction) -> RuleEvaluation:
        family = (
            SignalFamily.PULLBACK_LONG
            if direction is Direction.LONG
            else SignalFamily.PULLBACK_SHORT
        )
        if self.settings.pullback_alert_mode == "off":
            return self._base(
                f,
                family,
                direction,
                0,
                ["causal pullback family disabled"],
                None,
            )
        if f.interval not in self.settings.pullback_intervals:
            return self._base(
                f,
                family,
                direction,
                0,
                [f"causal pullback family disabled on {f.interval}"],
                None,
            )

        structure = f.chart_structure
        expected_direction = "long" if direction is Direction.LONG else "short"
        reasons: list[str] = []
        score = 0.0
        trend = (
            f.price > f.ema50 and f.ema20 > f.ema50 and f.ema20_slope_atr > 0
            if direction is Direction.LONG
            else f.price < f.ema50 and f.ema20 < f.ema50 and f.ema20_slope_atr < 0
        )
        if trend:
            score += 20
            reasons.append("EMA trend remained aligned through the pullback")
        structural = (
            structure.state == ("bullish" if direction is Direction.LONG else "bearish")
            and structure.pullback_direction == expected_direction
            and structure.impulse_size_atr is not None
            and structure.impulse_size_atr >= 2.0
        )
        if structural:
            score += 20
            reasons.append(
                f"confirmed {structure.state} swings and "
                f"{structure.impulse_size_atr:.2f}-ATR impulse"
            )
        depth = structure.pullback_depth
        depth_ok = depth is not None and 0.20 <= depth <= 0.60
        if depth_ok:
            score += 20
            reasons.append(f"pullback depth {depth:.1%} inside research band")
        duration_ok = (
            structure.structure_intact
            and structure.pullback_duration_bars is not None
            and structure.pullback_duration_bars <= 12
        )
        if duration_ok:
            score += 15
            reasons.append(
                f"structure intact over {structure.pullback_duration_bars} pullback bars"
            )
        confluence = structure.confluence_distance_atr
        confluence_ok = confluence is not None and confluence <= 0.25
        if confluence_ok:
            score += 10
            reasons.append(f"pullback extreme within {confluence:.2f} ATR of frozen EMA20")
        if structure.recovery_confirmed:
            score += 15
            reasons.append("closed through the prior candle in the trend direction")

        ready = (
            structure.pullback_status == "ready"
            and trend
            and structural
            and depth_ok
            and duration_ok
            and confluence_ok
            and structure.recovery_confirmed
        )
        if structure.pullback_direction != expected_direction or not structural:
            score = 0
        elif not ready:
            score = min(score, self.settings.setup_score - 1)
        invalidation = (
            None
            if structure.impulse_start is None
            else structure.impulse_start
            + (-0.25 * f.atr if direction is Direction.LONG else 0.25 * f.atr)
        )
        return self._base(
            f,
            family,
            direction,
            score,
            reasons,
            invalidation,
            metadata={
                "informational_only": True,
                "threshold_status": "unvalidated_research_seed",
                "pullback_status": structure.pullback_status,
            },
            triggered=ready,
        )

    def _exhaustion(self, f: FeatureSnapshot) -> RuleEvaluation:
        if f.interval not in self.settings.reversal_intervals:
            return self._base(
                f,
                SignalFamily.EXHAUSTION_SHORT,
                Direction.SHORT,
                0,
                [f"RSI reversal family disabled on {f.interval}"],
                None,
            )
        score = 0.0
        reasons = []
        active = max(f.rsi, f.rsi_previous) >= self.settings.overbought_rsi
        if active:
            score += 25
            reasons.append(f"RSI recently overbought ({f.rsi_previous:.1f} → {f.rsi:.1f})")
        if f.bearish_divergence:
            score += 25
            reasons.append("bearish price/RSI divergence")
        if f.upper_wick_ratio >= 0.45:
            score += 15
            reasons.append(f"upper-wick rejection {f.upper_wick_ratio:.0%}")
        if f.macd_histogram < f.macd_histogram_previous < f.macd_histogram_previous2:
            score += 15
            reasons.append("MACD histogram weakened for three observations")
        if f.price < f.ema9:
            score += 10
            reasons.append("closed below EMA9")
        if not self.settings.gate_enabled and f.taker_buy_ratio <= 0.45:
            score += 10
            reasons.append(f"sell-side taker flow {1 - f.taker_buy_ratio:.0%}")
        if (
            not self.settings.gate_enabled
            and f.relative_volume >= self.settings.relative_volume_threshold
        ):
            score += 10
            reasons.append(f"volume climax candidate {f.relative_volume:.2f}x")
        if f.regime.btc_trend == "bullish" and f.adx >= 35:
            score -= 25
            reasons.append("penalty: strong bullish higher-timeframe trend")
        if not active:
            score = 0
        return self._base(
            f,
            SignalFamily.EXHAUSTION_SHORT,
            Direction.SHORT,
            score,
            reasons,
            f.recent_high + f.atr * 0.5,
        )

    def _capitulation(self, f: FeatureSnapshot) -> RuleEvaluation:
        if f.interval not in self.settings.reversal_intervals:
            return self._base(
                f,
                SignalFamily.CAPITULATION_LONG,
                Direction.LONG,
                0,
                [f"RSI reversal family disabled on {f.interval}"],
                None,
            )
        score = 0.0
        reasons = []
        active = min(f.rsi, f.rsi_previous) <= self.settings.oversold_rsi
        if active:
            score += 25
            reasons.append(f"RSI recently oversold ({f.rsi_previous:.1f} → {f.rsi:.1f})")
        if f.bullish_divergence:
            score += 25
            reasons.append("bullish price/RSI divergence")
        if f.lower_wick_ratio >= 0.45:
            score += 15
            reasons.append(f"lower-wick rejection {f.lower_wick_ratio:.0%}")
        if f.macd_histogram > f.macd_histogram_previous > f.macd_histogram_previous2:
            score += 15
            reasons.append("MACD histogram improved for three observations")
        if f.price > f.ema9:
            score += 10
            reasons.append("closed above EMA9")
        if not self.settings.gate_enabled and f.taker_buy_ratio >= 0.55:
            score += 10
            reasons.append(f"buy-side taker flow {f.taker_buy_ratio:.0%}")
        if (
            not self.settings.gate_enabled
            and f.relative_volume >= self.settings.relative_volume_threshold
        ):
            score += 10
            reasons.append(f"volume climax candidate {f.relative_volume:.2f}x")
        if f.regime.btc_trend == "bearish" and f.adx >= 35:
            score -= 25
            reasons.append("penalty: strong bearish higher-timeframe trend")
        if not active:
            score = 0
        return self._base(
            f,
            SignalFamily.CAPITULATION_LONG,
            Direction.LONG,
            score,
            reasons,
            f.recent_low - f.atr * 0.5,
        )
