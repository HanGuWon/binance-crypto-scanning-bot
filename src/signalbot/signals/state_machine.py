from __future__ import annotations

import hashlib
from collections.abc import Collection
from dataclasses import dataclass
from decimal import Decimal

from signalbot.config import SignalSettings
from signalbot.domain.enums import SignalStage
from signalbot.domain.models import RuleEvaluation, SignalDecision


@dataclass(slots=True)
class _State:
    stage: SignalStage = SignalStage.IDLE
    cooldown_until_ms: int = 0
    invalidation: Decimal | None = None


class SignalStateMachine:
    def __init__(self, settings: SignalSettings, rule_version: str) -> None:
        self.settings = settings
        self.rule_version = rule_version
        self._states: dict[tuple[str, str, str, str], _State] = {}

    def prune_symbols(self, active_symbols: Collection[str]) -> int:
        """Remove state for symbols outside the current bounded universe."""

        active = {symbol.upper() for symbol in active_symbols}
        stale = [key for key in self._states if key[1].upper() not in active]
        for key in stale:
            del self._states[key]
        return len(stale)

    def prune_directional_states(
        self, tradable_symbols: Collection[str]
    ) -> int:
        """Drop non-risk family state for symbols outside the tradable universe.

        Risk families (PUMP_RISK/CRASH_RISK) are driven by the all-market
        mini-ticker and must survive for surveillance-only symbols. Directional
        entry families are only evaluated for tradable symbols, so their
        WATCH/SETUP/cooldown state outside tradable is stale and must not be
        resurrected when a symbol re-enters the tradable top-N.
        """

        tradable = {symbol.upper() for symbol in tradable_symbols}
        risk_families = {"pump_risk", "crash_risk"}
        stale = [
            key
            for key in self._states
            if key[1].upper() not in tradable and key[2] not in risk_families
        ]
        for key in stale:
            del self._states[key]
        return len(stale)

    def process(self, e: RuleEvaluation) -> SignalDecision | None:
        e = self._validated_evaluation(e)
        key = (e.market.value, e.symbol, e.family.value, e.timeframe)
        state = self._states.setdefault(key, _State())
        desired = self._desired(e)
        previous = state.stage
        previous_invalidation = state.invalidation
        in_cooldown = e.event_time_ms < state.cooldown_until_ms
        if desired is SignalStage.IDLE:
            state.stage = SignalStage.IDLE
            state.invalidation = None
            if (
                previous in {SignalStage.WATCH, SignalStage.SETUP}
                and not in_cooldown
            ):
                invalidated = e
                if previous_invalidation is not None:
                    invalidated = e.model_copy(
                        update={"invalidation": previous_invalidation}
                    )
                return self._decision(invalidated, SignalStage.INVALIDATED)
            return None
        if e.invalidation is not None:
            state.invalidation = e.invalidation
        if desired is previous:
            return None
        state.stage = desired
        if in_cooldown:
            return None
        if desired is SignalStage.CONFIRMED:
            state.cooldown_until_ms = e.event_time_ms + self.settings.cooldown_seconds * 1000
        return self._decision(e, desired)

    def _desired(self, evaluation: RuleEvaluation) -> SignalStage:
        if self.settings.confirmation_mode == "explicit_trigger":
            if evaluation.triggered and self._can_confirm(evaluation):
                return SignalStage.CONFIRMED
            if evaluation.score >= self.settings.setup_score:
                return SignalStage.SETUP
            if evaluation.score >= self.settings.watch_score:
                return SignalStage.WATCH
            return SignalStage.IDLE
        if evaluation.score >= self.settings.confirmed_score and self._can_confirm(
            evaluation
        ):
            return SignalStage.CONFIRMED
        if evaluation.score >= self.settings.setup_score:
            return SignalStage.SETUP
        if evaluation.score >= self.settings.watch_score:
            return SignalStage.WATCH
        return SignalStage.IDLE

    def decision_for_confirmed_trigger(
        self, evaluation: RuleEvaluation
    ) -> SignalDecision:
        """Materialize a research entry without applying alert cooldown state."""

        evaluation = self._validated_evaluation(evaluation)
        if not evaluation.triggered or not self._can_confirm(evaluation):
            raise ValueError(
                "confirmed trigger decisions require triggered, confirmation-eligible input"
            )
        return self._decision(evaluation, SignalStage.CONFIRMED)

    def decision_for_research_entry(
        self, evaluation: RuleEvaluation
    ) -> SignalDecision | None:
        """Materialize a cooldown-free entry under the configured confirmation mode."""

        evaluation = self._validated_evaluation(evaluation)
        if not self._can_confirm(evaluation):
            return None
        if self.settings.confirmation_mode == "explicit_trigger":
            if not evaluation.triggered:
                return None
        elif evaluation.score < self.settings.confirmed_score:
            return None
        return self._decision(evaluation, SignalStage.CONFIRMED)

    @staticmethod
    def _can_confirm(evaluation: RuleEvaluation) -> bool:
        return (
            evaluation.eligible
            and evaluation.metadata.get("informational_only") is not True
        )

    @staticmethod
    def _validated_evaluation(evaluation: RuleEvaluation) -> RuleEvaluation:
        return RuleEvaluation.model_validate(
            evaluation.model_dump(mode="python", warnings="none")
        )

    def _decision(self, e: RuleEvaluation, stage: SignalStage) -> SignalDecision:
        identity = "|".join(
            (
                e.market.value,
                e.symbol,
                e.family.value,
                e.timeframe,
                stage.value,
                str(e.event_time_ms),
                self.rule_version,
            )
        )
        event_id = hashlib.sha256(identity.encode()).hexdigest()[:24]
        return SignalDecision(
            event_id=event_id,
            market=e.market,
            symbol=e.symbol,
            family=e.family,
            stage=stage,
            direction=e.direction,
            timeframe=e.timeframe,
            event_time_ms=e.event_time_ms,
            score=e.score,
            price=e.price,
            reasons=e.reasons,
            invalidation=e.invalidation,
            regime=e.regime,
            gate=e.gate,
            rule_version=self.rule_version,
            metadata=e.metadata,
        )
