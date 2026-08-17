from __future__ import annotations

import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, field_validator, model_validator

from signalbot.config import StrictModel, VolumeFeatureSettings


class BacktestAsset(StrictModel):
    asset: str
    cohort: str
    spot_symbol: str
    futures_symbol: str

    @field_validator("asset", "spot_symbol", "futures_symbol")
    @classmethod
    def uppercase_symbols(cls, value: str) -> str:
        return value.upper()

    @field_validator("cohort")
    @classmethod
    def validate_cohort(cls, value: str) -> str:
        normalized = value.lower()
        if normalized not in {"anchor", "major", "volatile"}:
            raise ValueError("cohort must be anchor, major, or volatile")
        return normalized


class BacktestSplit(StrictModel):
    name: str
    start: datetime
    end: datetime

    @field_validator("start", "end")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("split timestamps must include a UTC offset")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def ordered(self) -> BacktestSplit:
        if self.end <= self.start:
            raise ValueError("split end must be later than start")
        return self


class ExitPolicySettings(StrictModel):
    trend_failure_bars: int = Field(default=2, ge=1, le=20)
    trailing_activation_r: float = Field(default=1.0, ge=0, le=10)
    trailing_atr_multiple: float = Field(default=2.0, gt=0, le=20)
    max_holding_bars: int = Field(default=24, ge=1, le=10_000)


class CostSettings(StrictModel):
    notional_usdt: float = Field(default=100.0, gt=0)
    spot_fee_bps: float = Field(default=10.0, ge=0, le=1000)
    futures_fee_bps: float = Field(default=5.0, ge=0, le=1000)
    spot_slippage_bps: dict[str, float] = Field(
        default_factory=lambda: {"anchor": 5.0, "major": 5.0, "volatile": 10.0}
    )
    futures_slippage_bps: dict[str, float] = Field(
        default_factory=lambda: {"anchor": 3.0, "major": 3.0, "volatile": 8.0}
    )
    include_funding: bool = True

    @model_validator(mode="after")
    def complete_cohorts(self) -> CostSettings:
        required = {"anchor", "major", "volatile"}
        for name, values in (
            ("spot_slippage_bps", self.spot_slippage_bps),
            ("futures_slippage_bps", self.futures_slippage_bps),
        ):
            if set(values) != required:
                raise ValueError(f"{name} must contain exactly {sorted(required)}")
            if any(
                not math.isfinite(value) or value < 0 or value > 1000
                for value in values.values()
            ):
                raise ValueError(
                    f"{name} values must be finite and between 0 and 1000 bps"
                )
        return self


class BootstrapSettings(StrictModel):
    samples: int = Field(default=2000, ge=100, le=100_000)
    block_days: int = Field(default=7, ge=1, le=365)
    seed: int = 20260714


class BacktestSpec(StrictModel):
    protocol_version: str
    rule_version: str
    experiment_plan_path: str | None = None
    interval: str = "1h"
    data_start: datetime
    evaluation_start: datetime
    evaluation_end: datetime
    minimum_age_days: int = Field(default=90, ge=0, le=3650)
    minimum_history_bars: int = Field(default=210, ge=50, le=10_000)
    historical_spread_proxy_bps: float = Field(default=11.25, ge=0, le=1000)
    entry_score: int = Field(default=80, ge=1, le=100)
    strategy_mode: Literal["legacy", "gate_v2", "pit_breakout_volume"] = "legacy"
    candidate_policy: Literal[
        "c0_frozen", "strict_pit_htf_diagnostic"
    ] | None = None
    opportunity_panel_horizon_bars: Literal[12, 72] = 72
    outcome_edge_margin_bps: float = Field(
        default=0.0,
        ge=0,
        le=1000,
        allow_inf_nan=False,
    )
    confirmation_mode: Literal["score", "explicit_trigger"] | None = None
    gate_use_participation: bool = True
    gate_use_crowding: bool = True
    gate_use_higher_timeframes: bool = True
    include_rsi_reversals: bool = True
    trend_gate: int = Field(default=60, ge=0, le=100)
    participation_gate: int = Field(default=60, ge=0, le=100)
    crowding_risk_cap: int = Field(default=75, ge=1, le=100)
    execution_gate: int = Field(default=65, ge=0, le=100)
    completeness_gate: int = Field(default=95, ge=0, le=100)
    volume_feature_set: Literal[
        "none", "kline_taker_delta", "normalized_vpci"
    ] = "none"
    volume: VolumeFeatureSettings = VolumeFeatureSettings()
    assets: list[BacktestAsset]
    splits: list[BacktestSplit]
    exits: ExitPolicySettings = ExitPolicySettings()
    costs: CostSettings = CostSettings()
    bootstrap: BootstrapSettings = BootstrapSettings()

    @field_validator("data_start", "evaluation_start", "evaluation_end")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("study timestamps must include a UTC offset")
        return value.astimezone(UTC)

    @field_validator("interval")
    @classmethod
    def supported_interval(cls, value: str) -> str:
        if value not in {"5m", "15m", "1h", "4h"}:
            raise ValueError("backtest interval must be 5m, 15m, 1h, or 4h")
        return value

    @model_validator(mode="after")
    def validate_contract(self) -> BacktestSpec:
        if not self.data_start < self.evaluation_start < self.evaluation_end:
            raise ValueError("expected data_start < evaluation_start < evaluation_end")
        if not self.assets:
            raise ValueError("at least one asset is required")
        if self.strategy_mode != "pit_breakout_volume" and self.volume_feature_set != "none":
            raise ValueError(
                "volume_feature_set requires strategy_mode=pit_breakout_volume"
            )
        assets = [item.asset for item in self.assets]
        spots = [item.spot_symbol for item in self.assets]
        futures = [item.futures_symbol for item in self.assets]
        if len(set(assets)) != len(assets):
            raise ValueError("asset names must be unique")
        if len(set(spots)) != len(spots) or len(set(futures)) != len(futures):
            raise ValueError("market symbols must be unique")
        ordered = sorted(self.splits, key=lambda item: item.start)
        if ordered != self.splits:
            raise ValueError("splits must be ordered by start time")
        previous_end: datetime | None = None
        for split in self.splits:
            if split.start < self.evaluation_start or split.end > self.evaluation_end:
                raise ValueError("every split must be inside the evaluation window")
            if previous_end is not None and split.start < previous_end:
                raise ValueError("splits must not overlap")
            previous_end = split.end
        return self

    def split_name(self, timestamp_ms: int) -> str | None:
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000, UTC)
        return next(
            (
                split.name
                for split in self.splits
                if split.start <= timestamp < split.end
            ),
            None,
        )


def load_backtest_spec(path: str | Path) -> BacktestSpec:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError("backtest configuration root must be a mapping")
    return BacktestSpec.model_validate(raw)
