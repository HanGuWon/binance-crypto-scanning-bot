from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage

_FAMILY_DIRECTIONS: dict[SignalFamily, frozenset[Direction]] = {
    SignalFamily.SQUEEZE_LONG: frozenset({Direction.LONG}),
    SignalFamily.SQUEEZE_SHORT: frozenset({Direction.SHORT}),
    SignalFamily.BREAKOUT_LONG: frozenset({Direction.LONG}),
    SignalFamily.BREAKDOWN_SHORT: frozenset({Direction.SHORT}),
    SignalFamily.PULLBACK_LONG: frozenset({Direction.LONG}),
    SignalFamily.PULLBACK_SHORT: frozenset({Direction.SHORT}),
    SignalFamily.EXHAUSTION_SHORT: frozenset({Direction.SHORT}),
    SignalFamily.CAPITULATION_LONG: frozenset({Direction.LONG}),
    SignalFamily.PUMP_RISK: frozenset({Direction.RISK_UP}),
    SignalFamily.CRASH_RISK: frozenset({Direction.RISK_DOWN}),
    SignalFamily.TECHNICAL_EXIT: frozenset({Direction.LONG, Direction.SHORT}),
}


def _require_family_direction(family: SignalFamily, direction: Direction) -> None:
    if direction not in _FAMILY_DIRECTIONS[family]:
        raise ValueError(
            f"signal family {family.value} is incompatible with direction {direction.value}"
        )


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="ignore", allow_inf_nan=False)


class Instrument(FrozenModel):
    market: Market
    symbol: str
    base_asset: str
    quote_asset: str
    status: str
    quote_volume: Decimal = Decimal("0")
    onboard_time_ms: int | None = None
    contract_type: str | None = None

    @field_validator("symbol", "base_asset", "quote_asset", "status")
    @classmethod
    def uppercase_text(cls, value: str) -> str:
        return value.upper()


class Candle(FrozenModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    market: Market
    symbol: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    quote_volume: Decimal
    trade_count: int
    taker_buy_base_volume: Decimal
    taker_buy_quote_volume: Decimal
    is_closed: bool = True

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def validate_exchange_fields(self) -> Candle:
        prices = (self.open, self.high, self.low, self.close)
        volumes = (
            self.volume,
            self.quote_volume,
            self.taker_buy_base_volume,
            self.taker_buy_quote_volume,
        )
        if any(not value.is_finite() for value in (*prices, *volumes)):
            raise ValueError("candle prices and volumes must be finite")
        if any(value <= 0 for value in prices):
            raise ValueError("candle prices must be positive")
        if any(value < 0 for value in volumes) or self.trade_count < 0:
            raise ValueError("candle volumes and trade_count must be non-negative")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("candle OHLC values are inconsistent")
        if self.low > self.high:
            raise ValueError("candle low must not exceed high")
        if self.taker_buy_base_volume > self.volume:
            raise ValueError("taker-buy base volume must not exceed base volume")
        if self.taker_buy_quote_volume > self.quote_volume:
            raise ValueError("taker-buy quote volume must not exceed quote volume")
        if self.close_time_ms < self.open_time_ms:
            raise ValueError("candle close time must not precede open time")
        return self


class BookTicker(FrozenModel):
    market: Market
    symbol: str
    # Keep exchange time and local receipt time separate because the Spot
    # bookTicker schema can omit E/T entirely.
    event_time_ms: int
    exchange_event_time_ms: int | None = None
    receipt_time_ms: int | None = None
    bid_price: Decimal
    bid_quantity: Decimal
    ask_price: Decimal
    ask_quantity: Decimal
    update_id: int | None = None

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        return value.upper()


class AggTrade(FrozenModel):
    market: Market
    symbol: str
    event_time_ms: int
    trade_time_ms: int
    price: Decimal
    quantity: Decimal
    is_buyer_maker: bool
    aggregate_trade_id: int | None = None

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        return value.upper()


class MiniTicker(FrozenModel):
    market: Market
    symbol: str
    event_time_ms: int
    close: Decimal
    open: Decimal | None = None
    high: Decimal | None = None
    low: Decimal | None = None
    volume: Decimal | None = None
    quote_volume: Decimal | None = None

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        return value.upper()


class MarketRegime(FrozenModel):
    label: str = "neutral"
    btc_trend: str = "neutral"
    breadth_ratio: float = 0.5


class GateEvaluation(FrozenModel):
    """Independent, auditable entry gates for the v2 signal protocol."""

    trend_score: int = Field(ge=0, le=100)
    participation_score: int = Field(ge=0, le=100)
    crowding_risk_score: int = Field(ge=0, le=100)
    execution_score: int = Field(ge=0, le=100)
    completeness_score: int = Field(ge=0, le=100)
    volume_policy_score: int = Field(default=100, ge=0, le=100)
    volume_feature_set: str = "none"
    passed: bool
    failures: tuple[str, ...] = ()
    proxy_fields: tuple[str, ...] = ()


class ChartStructureSnapshot(FrozenModel):
    """Point-in-time chart structure built only from previously confirmed pivots."""

    method: Literal["confirmed_fractal_2x2_tminus1_v1"] = (
        "confirmed_fractal_2x2_tminus1_v1"
    )
    state: Literal["bullish", "bearish", "mixed", "unavailable"] = "unavailable"
    qualified_high_count: int = Field(default=0, ge=0)
    qualified_low_count: int = Field(default=0, ge=0)
    previous_swing_high: float | None = None
    latest_swing_high: float | None = None
    previous_swing_low: float | None = None
    latest_swing_low: float | None = None
    latest_high_bars_ago: int | None = Field(default=None, ge=0)
    latest_low_bars_ago: int | None = Field(default=None, ge=0)
    swing_high_change_atr: float | None = None
    swing_low_change_atr: float | None = None
    projected_support: float | None = None
    projected_resistance: float | None = None
    support_slope_atr_per_bar: float | None = None
    resistance_slope_atr_per_bar: float | None = None
    price_minus_support_atr: float | None = None
    resistance_minus_price_atr: float | None = None
    support_broken: bool = False
    resistance_broken: bool = False
    pullback_direction: Literal["long", "short", "none"] = "none"
    pullback_status: Literal[
        "unavailable", "none", "developing", "ready", "invalid"
    ] = "unavailable"
    impulse_start: float | None = None
    impulse_end: float | None = None
    impulse_size_atr: float | None = None
    pullback_depth: float | None = None
    pullback_duration_bars: int | None = Field(default=None, ge=0)
    confluence_distance_atr: float | None = None
    pullback_range_ratio: float | None = None
    pullback_quote_volume_ratio: float | None = None
    recovery_confirmed: bool = False
    structure_intact: bool = False


class FeatureSnapshot(FrozenModel):
    market: Market
    symbol: str
    interval: str
    event_time_ms: int
    price: float
    previous_close: float
    ema9: float
    ema20: float
    ema50: float
    ema200: float | None = None
    rsi: float
    rsi_previous: float
    macd_histogram: float
    macd_histogram_previous: float
    macd_histogram_previous2: float
    atr: float
    atr_percent: float
    adx: float
    bollinger_width: float
    bollinger_width_percentile: float
    relative_volume: float
    recent_high: float
    recent_low: float
    upper_wick_ratio: float
    lower_wick_ratio: float
    bearish_divergence: bool
    bullish_divergence: bool
    taker_buy_ratio: float
    ema20_slope_atr: float = 0.0
    volume_zscore: float = 0.0
    trade_count_zscore: float = 0.0
    taker_imbalance: float = 0.0
    cvd_pressure: float = 0.0
    closed_kline_flow_available: bool = True
    intrabar_taker_imbalance_60s: float | None = None
    taker_delta_3: float | None = None
    taker_delta_12: float | None = None
    taker_delta_unavailable_reason: str | None = None
    normalized_vpci: float | None = None
    normalized_vpci_signal: float | None = None
    normalized_vpci_slope_3: float | None = None
    normalized_vpci_unavailable_reason: str | None = None
    funding_rate: float | None = None
    funding_zscore: float | None = None
    spread_bps: float | None
    spread_is_proxy: bool = False
    book_age_ms: int | None = None
    bid_quote_capacity: float | None = None
    ask_quote_capacity: float | None = None
    previous_high: float | None = None
    previous_low: float | None = None
    previous_ema20: float | None = None
    ema20_distance_atr: float | None = None
    chart_structure: ChartStructureSnapshot = ChartStructureSnapshot()
    data_completeness: float = Field(default=1.0, ge=0.0, le=1.0)
    regime: MarketRegime


class DirectionalSetupScore(FrozenModel):
    """One direction's strongest existing setup rule at a closed candle."""

    family: SignalFamily
    raw_score: int = Field(ge=0, le=100)
    decision_score: int = Field(ge=0, le=100)
    triggered: bool
    eligible: bool


class DirectionalDiagnostics(FrozenModel):
    """Auditable Discord diagnostics; these scores are not probabilities."""

    method: Literal["promotion_safe_rule_strength_per_direction"] = (
        "promotion_safe_rule_strength_per_direction"
    )
    score_is_probability: Literal[False] = False
    long: DirectionalSetupScore
    short: DirectionalSetupScore
    feature: FeatureSnapshot

    @model_validator(mode="after")
    def validate_directional_families(self) -> DirectionalDiagnostics:
        _require_family_direction(self.long.family, Direction.LONG)
        _require_family_direction(self.short.family, Direction.SHORT)
        return self


DIRECTIONAL_DIAGNOSTICS_METADATA_KEY = "directional_diagnostics_v1"


class RuleEvaluation(FrozenModel):
    market: Market
    symbol: str
    family: SignalFamily
    direction: Direction
    timeframe: str
    event_time_ms: int
    score: int = Field(ge=0, le=100)
    triggered: bool = False
    eligible: bool = True
    price: Decimal
    reasons: tuple[str, ...] = ()
    invalidation: Decimal | None = None
    regime: MarketRegime = MarketRegime()
    gate: GateEvaluation | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_family_direction(self) -> RuleEvaluation:
        _require_family_direction(self.family, self.direction)
        return self


class SignalDecision(FrozenModel):
    event_id: str
    market: Market
    symbol: str
    family: SignalFamily
    stage: SignalStage
    direction: Direction
    timeframe: str
    event_time_ms: int
    score: int = Field(ge=0, le=100)
    price: Decimal
    reasons: tuple[str, ...] = ()
    invalidation: Decimal | None = None
    regime: MarketRegime = MarketRegime()
    gate: GateEvaluation | None = None
    rule_version: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_decision_semantics(self) -> SignalDecision:
        _require_family_direction(self.family, self.direction)
        if (
            self.metadata.get("informational_only") is True
            and self.stage is SignalStage.CONFIRMED
        ):
            raise ValueError("informational-only decisions cannot be CONFIRMED")
        return self

    @property
    def action_label(self) -> str:
        if self.metadata.get("informational_only") is True:
            return "INFORMATION_ONLY"
        if self.family is SignalFamily.PUMP_RISK:
            return "PUMP_RISK"
        if self.family is SignalFamily.CRASH_RISK:
            return "CRASH_RISK"
        if self.family is SignalFamily.TECHNICAL_EXIT:
            if self.market is Market.SPOT:
                return "SPOT_EXIT"
            return (
                "FUTURES_LONG_EXIT"
                if self.direction is Direction.LONG
                else "FUTURES_SHORT_EXIT"
            )
        if self.market is Market.SPOT:
            return "SPOT_BUY" if self.direction is Direction.LONG else "SPOT_EXIT"
        return "FUTURES_LONG" if self.direction is Direction.LONG else "FUTURES_SHORT"
