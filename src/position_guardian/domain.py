from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

ManagedPositionSide = Literal["LONG", "SHORT"]


def _positive_decimal(value: Decimal, field_name: str) -> Decimal:
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{field_name} must be a finite positive Decimal")
    return value


@dataclass(frozen=True)
class ManagedPositionIdentity:
    """Stable identity for one explicitly adopted position generation."""

    account_alias: str
    symbol: str
    position_side: ManagedPositionSide
    adoption_generation: int

    def __post_init__(self) -> None:
        account_alias = self.account_alias.strip()
        symbol = self.symbol.strip().upper()
        if not account_alias:
            raise ValueError("account_alias must not be blank")
        if not symbol:
            raise ValueError("symbol must not be blank")
        if self.position_side not in {"LONG", "SHORT"}:
            raise ValueError("position_side must be LONG or SHORT")
        if self.adoption_generation < 1:
            raise ValueError("adoption_generation must be positive")
        object.__setattr__(self, "account_alias", account_alias)
        object.__setattr__(self, "symbol", symbol)


@dataclass(frozen=True)
class ManagedPositionAllowlist:
    """Operator opt-in record; it does not create or modify an exchange position."""

    account_alias: str
    symbol: str
    position_side: ManagedPositionSide
    adoption_generation: int = 1
    protection_floor: Decimal | None = None

    def __post_init__(self) -> None:
        identity = ManagedPositionIdentity(
            account_alias=self.account_alias,
            symbol=self.symbol,
            position_side=self.position_side,
            adoption_generation=self.adoption_generation,
        )
        object.__setattr__(self, "account_alias", identity.account_alias)
        object.__setattr__(self, "symbol", identity.symbol)
        if self.protection_floor is not None:
            object.__setattr__(
                self,
                "protection_floor",
                _positive_decimal(self.protection_floor, "protection_floor"),
            )

    @property
    def identity(self) -> ManagedPositionIdentity:
        return ManagedPositionIdentity(
            account_alias=self.account_alias,
            symbol=self.symbol,
            position_side=self.position_side,
            adoption_generation=self.adoption_generation,
        )


ProtectionSource = Literal["exchange_stop", "user_floor"]


@dataclass(frozen=True)
class ProtectiveOrderReference:
    source: Literal["open_order", "algo_order"]
    order_id: int
    trigger_price: Decimal


@dataclass(frozen=True)
class AdoptionCandidate:
    identity: ManagedPositionIdentity
    quantity: Decimal
    entry_price: Decimal
    mark_price: Decimal
    original_risk_stop: Decimal | None
    protection_floor: Decimal
    protection_source: ProtectionSource
    protective_order: ProtectiveOrderReference | None


AdoptionRejectionReason = Literal[
    "ALLOWLIST_MISMATCH",
    "UNSUPPORTED_POSITION_MODE",
    "POSITION_SIDE_MISMATCH",
    "ZERO_QUANTITY",
    "INVALID_POSITION",
    "SIDE_FLIP",
    "MISSING_PROTECTION",
    "DUPLICATE_PROTECTION",
    "CONFLICTING_PROTECTION",
    "PROTECTION_FLOOR_CONFLICT",
]


@dataclass(frozen=True)
class AdoptionDecision:
    candidate: AdoptionCandidate | None
    rejection_reason: AdoptionRejectionReason | None = None

    @property
    def adoptable(self) -> bool:
        return self.candidate is not None

    def __post_init__(self) -> None:
        if (self.candidate is None) == (self.rejection_reason is None):
            raise ValueError("an adoption decision must have exactly one outcome")
