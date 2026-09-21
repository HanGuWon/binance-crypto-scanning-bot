from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from signalbot.domain.enums import Direction, Market
from signalbot.signals.positions import calculate_trailing_stop_candidate


class GuardianPolicyState(StrEnum):
    """Shadow-only policy states for a managed protective stop."""

    INITIAL_RISK = "INITIAL_RISK"
    TREND_PROGRESS = "TREND_PROGRESS"
    PROFIT_PROTECTION = "PROFIT_PROTECTION"
    TREND_WEAKENING = "TREND_WEAKENING"
    STALE_OR_UNCERTAIN = "STALE_OR_UNCERTAIN"


@dataclass(frozen=True, slots=True)
class ProtectiveStopPolicy:
    """Policy for proposing protective-stop changes for an existing position."""

    policy_version: str = "guardian-stop-policy-v1"
    trailing_activation_r: float = 1.0
    trailing_atr_multiple: float = 2.0
    manual_activation_atr: float = 1.0
    minimum_price_gap_bps: float = 5.0
    minimum_improvement_bps: float = 1.0

    def __post_init__(self) -> None:
        if not self.policy_version.strip():
            raise ValueError("policy_version must not be blank")
        if self.trailing_activation_r < 0:
            raise ValueError("trailing_activation_r must be non-negative")
        if self.trailing_atr_multiple <= 0:
            raise ValueError("trailing_atr_multiple must be positive")
        if self.manual_activation_atr < 0:
            raise ValueError("manual_activation_atr must be non-negative")
        if self.minimum_price_gap_bps < 0:
            raise ValueError("minimum_price_gap_bps must be non-negative")
        if self.minimum_improvement_bps < 0:
            raise ValueError("minimum_improvement_bps must be non-negative")


@dataclass(frozen=True, slots=True)
class ManagedPositionSnapshot:
    """Read-only exchange-position state observed at one closed-candle decision time."""

    position_ref: str
    market: Market
    symbol: str
    direction: Direction
    entry_price: float
    active_stop: float
    reference_price: float
    highest_price: float
    lowest_price: float
    observed_at_ms: int
    initial_stop: float | None = None
    original_risk_stop: float | None = None
    protection_floor: float | None = None
    context_state: Literal["READY", "STALE_OR_UNCERTAIN"] = "READY"

    def __post_init__(self) -> None:
        normalized_symbol = self.symbol.upper().strip()
        if not self.position_ref.strip():
            raise ValueError("position_ref must not be blank")
        if not normalized_symbol:
            raise ValueError("symbol must not be blank")
        object.__setattr__(self, "symbol", normalized_symbol)
        if self.market is not Market.FUTURES:
            raise ValueError("managed protective stops currently require futures market")
        if self.direction not in {Direction.LONG, Direction.SHORT}:
            raise ValueError("managed position direction must be long or short")
        if self.context_state not in {"READY", "STALE_OR_UNCERTAIN"}:
            raise ValueError("context_state must be READY or STALE_OR_UNCERTAIN")
        prices = (
            self.entry_price,
            self.active_stop,
            self.reference_price,
            self.highest_price,
            self.lowest_price,
        )
        optional_prices = (self.initial_stop, self.original_risk_stop, self.protection_floor)
        if any(not math.isfinite(price) or price <= 0 for price in prices):
            raise ValueError("managed position prices must be positive")
        if any(
            price is not None and (not math.isfinite(price) or price <= 0)
            for price in optional_prices
        ):
            raise ValueError("optional managed position prices must be positive")
        if (
            self.initial_stop is not None
            and self.original_risk_stop is not None
            and self.initial_stop != self.original_risk_stop
        ):
            raise ValueError("initial_stop and original_risk_stop must agree")
        resolved_risk_stop = (
            self.original_risk_stop
            if self.original_risk_stop is not None
            else self.initial_stop
        )
        object.__setattr__(self, "original_risk_stop", resolved_risk_stop)
        if self.initial_stop is None:
            object.__setattr__(self, "initial_stop", resolved_risk_stop)
        if self.protection_floor is None:
            object.__setattr__(self, "protection_floor", resolved_risk_stop)
        if resolved_risk_stop is not None:
            if self.direction is Direction.LONG and resolved_risk_stop >= self.entry_price:
                raise ValueError("long original_risk_stop must be below entry_price")
            if self.direction is Direction.SHORT and resolved_risk_stop <= self.entry_price:
                raise ValueError("short original_risk_stop must be above entry_price")
        if self.highest_price < max(self.entry_price, self.reference_price):
            raise ValueError("highest_price cannot be below entry/reference price")
        if self.lowest_price > min(self.entry_price, self.reference_price):
            raise ValueError("lowest_price cannot be above entry/reference price")
        if self.direction is Direction.LONG:
            if resolved_risk_stop is not None and self.active_stop < resolved_risk_stop:
                raise ValueError("long active_stop cannot loosen below original risk stop")
        else:
            if resolved_risk_stop is not None and self.active_stop > resolved_risk_stop:
                raise ValueError("short active_stop cannot loosen above original risk stop")
        if self.observed_at_ms < 0:
            raise ValueError("observed_at_ms must be non-negative")


@dataclass(frozen=True, slots=True)
class StopUpdateIntent:
    """Idempotent intent for a future execution gateway to amend a stop order."""

    intent_id: str
    policy_version: str
    position_ref: str
    market: Market
    symbol: str
    direction: Direction
    previous_stop: float
    proposed_stop: float
    reference_price: float
    observed_at_ms: int
    reason: str = "atr_trailing_stop"
    reduce_only: bool = True
    close_position: bool = False
    order_placed: bool = False
    policy_state: GuardianPolicyState = GuardianPolicyState.TREND_PROGRESS
    effective_from_next_candle: bool = True


class ProtectiveStopPlanner:
    """Create stop-amend intents while keeping exchange execution out of the scanner."""

    def __init__(self, policy: ProtectiveStopPolicy | None = None) -> None:
        self.policy = policy or ProtectiveStopPolicy()

    def classify_policy_state(
        self,
        snapshot: ManagedPositionSnapshot,
        *,
        atr: float,
        confirmed_structure_stop: float | None = None,
        momentum_weakened: bool = False,
    ) -> GuardianPolicyState:
        if snapshot.context_state == "STALE_OR_UNCERTAIN" or atr <= 0:
            return GuardianPolicyState.STALE_OR_UNCERTAIN
        if (
            confirmed_structure_stop is not None
            and math.isfinite(confirmed_structure_stop)
            and confirmed_structure_stop > 0
            and momentum_weakened
        ):
            return GuardianPolicyState.TREND_WEAKENING
        risk_stop = snapshot.original_risk_stop
        if risk_stop is not None:
            risk = abs(snapshot.entry_price - risk_stop)
            favourable = (
                snapshot.highest_price - snapshot.entry_price
                if snapshot.direction is Direction.LONG
                else snapshot.entry_price - snapshot.lowest_price
            )
            if favourable < risk * self.policy.trailing_activation_r:
                return GuardianPolicyState.INITIAL_RISK
        return GuardianPolicyState.TREND_PROGRESS

    def plan(
        self,
        snapshot: ManagedPositionSnapshot,
        *,
        atr: float,
        confirmed_structure_stop: float | None = None,
        momentum_weakened: bool = False,
    ) -> StopUpdateIntent | None:
        state = self.classify_policy_state(
            snapshot,
            atr=atr,
            confirmed_structure_stop=confirmed_structure_stop,
            momentum_weakened=momentum_weakened,
        )
        if state is GuardianPolicyState.STALE_OR_UNCERTAIN:
            return None
        raw_candidate = self._candidate(
            snapshot,
            atr=atr,
            state=state,
            confirmed_structure_stop=confirmed_structure_stop,
        )
        if raw_candidate is None:
            return None

        if snapshot.direction is Direction.LONG and raw_candidate >= snapshot.reference_price:
            return None
        if snapshot.direction is Direction.SHORT and raw_candidate <= snapshot.reference_price:
            return None

        price_gap = snapshot.reference_price * self.policy.minimum_price_gap_bps / 10_000
        if snapshot.direction is Direction.LONG:
            proposed = max(raw_candidate, snapshot.protection_floor or raw_candidate)
            proposed = min(proposed, snapshot.reference_price - price_gap)
            improvement = proposed - snapshot.active_stop
        else:
            proposed = min(raw_candidate, snapshot.protection_floor or raw_candidate)
            proposed = max(proposed, snapshot.reference_price + price_gap)
            improvement = snapshot.active_stop - proposed
        if snapshot.protection_floor is not None:
            if snapshot.direction is Direction.LONG and proposed < snapshot.protection_floor:
                return None
            if snapshot.direction is Direction.SHORT and proposed > snapshot.protection_floor:
                return None
        minimum_improvement = (
            snapshot.reference_price
            * self.policy.minimum_improvement_bps
            / 10_000
        )
        if improvement <= 0 or improvement < minimum_improvement:
            return None

        if snapshot.direction is Direction.LONG:
            if proposed >= snapshot.reference_price:
                return None
            final_state = (
                GuardianPolicyState.PROFIT_PROTECTION
                if state is not GuardianPolicyState.TREND_WEAKENING
                and proposed > snapshot.entry_price
                and (
                    snapshot.original_risk_stop is not None
                    or (snapshot.protection_floor or 0.0) > snapshot.entry_price
                )
                else state
            )
        else:
            if proposed <= snapshot.reference_price:
                return None
            final_state = (
                GuardianPolicyState.PROFIT_PROTECTION
                if state is not GuardianPolicyState.TREND_WEAKENING
                and proposed < snapshot.entry_price
                and (
                    snapshot.original_risk_stop is not None
                    or (snapshot.protection_floor or float("inf")) < snapshot.entry_price
                )
                else state
            )
        reason = {
            GuardianPolicyState.TREND_PROGRESS: "atr_structure_progress",
            GuardianPolicyState.PROFIT_PROTECTION: "profit_protection",
            GuardianPolicyState.TREND_WEAKENING: "confirmed_structure_weakening",
            GuardianPolicyState.INITIAL_RISK: "initial_risk",
            GuardianPolicyState.STALE_OR_UNCERTAIN: "stale_or_uncertain",
        }[final_state]
        intent_id = self._intent_id(snapshot, proposed, final_state)
        return StopUpdateIntent(
            intent_id=intent_id,
            policy_version=self.policy.policy_version,
            position_ref=snapshot.position_ref,
            market=snapshot.market,
            symbol=snapshot.symbol,
            direction=snapshot.direction,
            previous_stop=snapshot.active_stop,
            proposed_stop=proposed,
            reference_price=snapshot.reference_price,
            observed_at_ms=snapshot.observed_at_ms,
            reason=reason,
            policy_state=final_state,
        )

    def _candidate(
        self,
        snapshot: ManagedPositionSnapshot,
        *,
        atr: float,
        state: GuardianPolicyState,
        confirmed_structure_stop: float | None,
    ) -> float | None:
        if state is GuardianPolicyState.TREND_WEAKENING:
            if confirmed_structure_stop is None or not math.isfinite(confirmed_structure_stop):
                return None
            return confirmed_structure_stop if confirmed_structure_stop > 0 else None
        if snapshot.original_risk_stop is not None:
            return calculate_trailing_stop_candidate(
                direction=snapshot.direction,
                entry_price=snapshot.entry_price,
                initial_stop=snapshot.original_risk_stop,
                active_stop=snapshot.active_stop,
                highest_price=snapshot.highest_price,
                lowest_price=snapshot.lowest_price,
                atr=atr,
                activation_r=self.policy.trailing_activation_r,
                atr_multiple=self.policy.trailing_atr_multiple,
            )
        favourable = (
            snapshot.highest_price - snapshot.entry_price
            if snapshot.direction is Direction.LONG
            else snapshot.entry_price - snapshot.lowest_price
        )
        if favourable < atr * self.policy.manual_activation_atr:
            return None
        if snapshot.direction is Direction.LONG:
            return snapshot.highest_price - self.policy.trailing_atr_multiple * atr
        return snapshot.lowest_price + self.policy.trailing_atr_multiple * atr

    def _intent_id(
        self,
        snapshot: ManagedPositionSnapshot,
        proposed_stop: float,
        policy_state: GuardianPolicyState,
    ) -> str:
        payload = "|".join(
            (
                self.policy.policy_version,
                snapshot.position_ref,
                snapshot.market.value,
                snapshot.symbol,
                snapshot.direction.value,
                str(snapshot.observed_at_ms),
                policy_state.value,
                format(snapshot.original_risk_stop or 0.0, ".12g"),
                format(snapshot.protection_floor or 0.0, ".12g"),
                format(snapshot.active_stop, ".12g"),
                format(proposed_stop, ".12g"),
            )
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
