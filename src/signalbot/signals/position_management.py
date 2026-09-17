from __future__ import annotations

import hashlib
from dataclasses import dataclass

from signalbot.domain.enums import Direction, Market
from signalbot.signals.positions import calculate_trailing_stop_candidate


@dataclass(frozen=True, slots=True)
class ProtectiveStopPolicy:
    """Policy for proposing protective-stop changes for an existing position."""

    policy_version: str = "protective-stop-intent-v1"
    trailing_activation_r: float = 1.0
    trailing_atr_multiple: float = 2.0
    minimum_price_gap_bps: float = 5.0
    minimum_improvement_bps: float = 1.0

    def __post_init__(self) -> None:
        if not self.policy_version.strip():
            raise ValueError("policy_version must not be blank")
        if self.trailing_activation_r < 0:
            raise ValueError("trailing_activation_r must be non-negative")
        if self.trailing_atr_multiple <= 0:
            raise ValueError("trailing_atr_multiple must be positive")
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
    initial_stop: float
    active_stop: float
    reference_price: float
    highest_price: float
    lowest_price: float
    observed_at_ms: int

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
        prices = (
            self.entry_price,
            self.initial_stop,
            self.active_stop,
            self.reference_price,
            self.highest_price,
            self.lowest_price,
        )
        if any(price <= 0 for price in prices):
            raise ValueError("managed position prices must be positive")
        if self.highest_price < max(self.entry_price, self.reference_price):
            raise ValueError("highest_price cannot be below entry/reference price")
        if self.lowest_price > min(self.entry_price, self.reference_price):
            raise ValueError("lowest_price cannot be above entry/reference price")
        if self.direction is Direction.LONG:
            if self.initial_stop >= self.entry_price:
                raise ValueError("long initial_stop must be below entry_price")
            if self.active_stop < self.initial_stop:
                raise ValueError("long active_stop cannot loosen below initial_stop")
        else:
            if self.initial_stop <= self.entry_price:
                raise ValueError("short initial_stop must be above entry_price")
            if self.active_stop > self.initial_stop:
                raise ValueError("short active_stop cannot loosen above initial_stop")
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


class ProtectiveStopPlanner:
    """Create stop-amend intents while keeping exchange execution out of the scanner."""

    def __init__(self, policy: ProtectiveStopPolicy | None = None) -> None:
        self.policy = policy or ProtectiveStopPolicy()

    def plan(self, snapshot: ManagedPositionSnapshot, *, atr: float) -> StopUpdateIntent | None:
        if atr <= 0:
            return None
        raw_candidate = calculate_trailing_stop_candidate(
            direction=snapshot.direction,
            entry_price=snapshot.entry_price,
            initial_stop=snapshot.initial_stop,
            active_stop=snapshot.active_stop,
            highest_price=snapshot.highest_price,
            lowest_price=snapshot.lowest_price,
            atr=atr,
            activation_r=self.policy.trailing_activation_r,
            atr_multiple=self.policy.trailing_atr_multiple,
        )
        if raw_candidate is None:
            return None

        price_gap = snapshot.reference_price * self.policy.minimum_price_gap_bps / 10_000
        if snapshot.direction is Direction.LONG:
            proposed = min(raw_candidate, snapshot.reference_price - price_gap)
            improvement = proposed - snapshot.active_stop
        else:
            proposed = max(raw_candidate, snapshot.reference_price + price_gap)
            improvement = snapshot.active_stop - proposed
        minimum_improvement = (
            snapshot.reference_price
            * self.policy.minimum_improvement_bps
            / 10_000
        )
        if improvement <= 0 or improvement < minimum_improvement:
            return None

        intent_id = self._intent_id(snapshot, proposed)
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
        )

    def _intent_id(self, snapshot: ManagedPositionSnapshot, proposed_stop: float) -> str:
        payload = "|".join(
            (
                self.policy.policy_version,
                snapshot.position_ref,
                snapshot.market.value,
                snapshot.symbol,
                snapshot.direction.value,
                str(snapshot.observed_at_ms),
                format(snapshot.active_stop, ".12g"),
                format(proposed_stop, ".12g"),
            )
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
