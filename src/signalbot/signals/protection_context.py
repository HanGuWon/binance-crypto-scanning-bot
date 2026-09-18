from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from enum import StrEnum
from typing import Literal

from pydantic import Field, field_validator, model_validator

from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle, FeatureSnapshot, FrozenModel

PROTECTION_CONTEXT_VERSION = "protection-context-v1"


class ProtectionTrendState(StrEnum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    MIXED = "mixed"


class HigherTimeframeProtectionSnapshot(FrozenModel):
    """One mature, closed and strictly-prior public-data HTF observation."""

    market: Market
    symbol: str
    interval: str
    event_time_ms: int = Field(ge=0)
    close: float = Field(gt=0)
    atr: float = Field(gt=0)
    trend_state: ProtectionTrendState
    data_completeness: float = Field(ge=0.0, le=1.0)
    source_closed: Literal[True] = True
    mature: Literal[True] = True

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        normalized = value.upper().strip()
        if not normalized:
            raise ValueError("higher-timeframe symbol must not be blank")
        return normalized

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("higher-timeframe interval must not be blank")
        interval_to_milliseconds(normalized)
        return normalized

    @classmethod
    def from_feature(cls, feature: FeatureSnapshot) -> HigherTimeframeProtectionSnapshot:
        return cls(
            market=feature.market,
            symbol=feature.symbol,
            interval=feature.interval,
            event_time_ms=feature.event_time_ms,
            close=feature.price,
            atr=feature.atr,
            trend_state=_trend_state(feature),
            data_completeness=feature.data_completeness,
        )


class ProtectionContext(FrozenModel):
    """Versioned public-data context consumed later by the Position Guardian.

    The contract is position-agnostic. It contains only closed-candle market
    evidence and never contains account, quantity, entry-price or order state.
    """

    context_id: str
    context_version: str
    source_decision_clock_id: str
    market: Market
    symbol: str
    primary_interval: str
    candle_open_time_ms: int = Field(ge=0)
    candle_close_time_ms: int = Field(ge=0)
    source_candle_closed: Literal[True] = True
    close: float = Field(gt=0)
    atr: float = Field(gt=0)
    confirmed_swing_support: float | None = Field(default=None, gt=0)
    confirmed_swing_resistance: float | None = Field(default=None, gt=0)
    trend_state: ProtectionTrendState
    consecutive_trend_failure_count: int = Field(ge=0)
    data_completeness: float = Field(ge=0.0, le=1.0)
    context_freshness_ms: int = Field(ge=0)
    higher_timeframes: tuple[HigherTimeframeProtectionSnapshot, ...] = ()

    @field_validator("symbol")
    @classmethod
    def uppercase_symbol(cls, value: str) -> str:
        normalized = value.upper().strip()
        if not normalized:
            raise ValueError("protection-context symbol must not be blank")
        return normalized

    @field_validator("primary_interval")
    @classmethod
    def validate_primary_interval(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("primary_interval must not be blank")
        interval_to_milliseconds(normalized)
        return normalized

    @field_validator("context_id", "context_version", "source_decision_clock_id")
    @classmethod
    def non_blank_identity(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("protection-context identity fields must not be blank")
        return normalized

    @model_validator(mode="after")
    def validate_context(self) -> ProtectionContext:
        if self.candle_close_time_ms < self.candle_open_time_ms:
            raise ValueError("closed candle time must not precede open time")

        primary_ms = interval_to_milliseconds(self.primary_interval)
        expected_close = self.candle_open_time_ms + primary_ms - 1
        if self.candle_close_time_ms != expected_close:
            raise ValueError("closed candle timestamps do not match primary_interval")

        seen_intervals: set[str] = set()
        for snapshot in self.higher_timeframes:
            if snapshot.interval in seen_intervals:
                raise ValueError("duplicate higher-timeframe interval")
            seen_intervals.add(snapshot.interval)
            if snapshot.market is not self.market or snapshot.symbol != self.symbol:
                raise ValueError("higher-timeframe identity does not match protection context")
            snapshot_ms = interval_to_milliseconds(snapshot.interval)
            if snapshot_ms <= primary_ms:
                raise ValueError("higher-timeframe interval must exceed primary interval")
            if snapshot.event_time_ms >= self.candle_close_time_ms:
                raise ValueError("higher-timeframe context must be strictly prior")
            age_ms = self.candle_close_time_ms - snapshot.event_time_ms
            if age_ms > snapshot_ms * 2:
                raise ValueError("higher-timeframe context is stale")

        expected_id = self.deterministic_context_id(
            self.model_dump(mode="json", exclude={"context_id"})
        )
        if self.context_id != expected_id:
            raise ValueError("context_id does not match deterministic payload identity")
        return self

    @classmethod
    def from_closed_candle(
        cls,
        *,
        candle: Candle,
        feature: FeatureSnapshot,
        higher_timeframe_contexts: Mapping[str, FeatureSnapshot],
        source_decision_clock_id: str,
        context_version: str = PROTECTION_CONTEXT_VERSION,
        consecutive_trend_failure_count: int = 0,
        context_freshness_ms: int = 0,
    ) -> ProtectionContext:
        """Build the contract from existing causal feature owners only."""

        if not candle.is_closed:
            raise ValueError("protection context requires a fully closed candle")
        if (
            feature.market is not candle.market
            or feature.symbol != candle.symbol
            or feature.interval != candle.interval
            or feature.event_time_ms != candle.close_time_ms
        ):
            raise ValueError("primary feature does not match its closed candle")

        higher_timeframes: list[HigherTimeframeProtectionSnapshot] = []
        for interval, context in higher_timeframe_contexts.items():
            if context.interval != interval:
                raise ValueError("higher-timeframe feature interval does not match its key")
            if context.market is not candle.market or context.symbol != candle.symbol:
                raise ValueError("higher-timeframe feature identity does not match candle")
            higher_timeframes.append(HigherTimeframeProtectionSnapshot.from_feature(context))
        higher_timeframes.sort(key=lambda item: interval_to_milliseconds(item.interval))

        structure = feature.chart_structure
        values: dict[str, object] = {
            "context_version": context_version,
            "source_decision_clock_id": source_decision_clock_id,
            "market": candle.market,
            "symbol": candle.symbol,
            "primary_interval": candle.interval,
            "candle_open_time_ms": candle.open_time_ms,
            "candle_close_time_ms": candle.close_time_ms,
            "source_candle_closed": True,
            "close": float(candle.close),
            "atr": feature.atr,
            "confirmed_swing_support": structure.latest_swing_low,
            "confirmed_swing_resistance": structure.latest_swing_high,
            "trend_state": _trend_state(feature),
            "consecutive_trend_failure_count": consecutive_trend_failure_count,
            "data_completeness": feature.data_completeness,
            "context_freshness_ms": context_freshness_ms,
            "higher_timeframes": tuple(higher_timeframes),
        }
        identity_payload = _json_ready(values)
        return cls(
            context_id=cls.deterministic_context_id(identity_payload),
            context_version=context_version,
            source_decision_clock_id=source_decision_clock_id,
            market=candle.market,
            symbol=candle.symbol,
            primary_interval=candle.interval,
            candle_open_time_ms=candle.open_time_ms,
            candle_close_time_ms=candle.close_time_ms,
            source_candle_closed=True,
            close=float(candle.close),
            atr=feature.atr,
            confirmed_swing_support=structure.latest_swing_low,
            confirmed_swing_resistance=structure.latest_swing_high,
            trend_state=_trend_state(feature),
            consecutive_trend_failure_count=consecutive_trend_failure_count,
            data_completeness=feature.data_completeness,
            context_freshness_ms=context_freshness_ms,
            higher_timeframes=tuple(higher_timeframes),
        )

    @staticmethod
    def deterministic_context_id(payload: object) -> str:
        canonical = json.dumps(
            _json_ready(payload),
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _trend_state(feature: FeatureSnapshot) -> ProtectionTrendState:
    if feature.price > feature.ema20 > feature.ema50:
        return ProtectionTrendState.BULLISH
    if feature.price < feature.ema20 < feature.ema50:
        return ProtectionTrendState.BEARISH
    return ProtectionTrendState.MIXED


def _json_ready(value: object) -> object:
    if isinstance(value, FrozenModel):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_json_ready(item) for item in value]
    if isinstance(value, StrEnum):
        return value.value
    return value
