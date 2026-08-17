from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from signalbot.domain.enums import Market


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BinanceSettings(StrictModel):
    markets: list[Market] = Field(default_factory=lambda: [Market.SPOT, Market.FUTURES])
    quote_asset: str = "USDT"
    top_n: int = Field(default=20, ge=1, le=300)
    surveillance_n: int = Field(default=200, ge=1, le=1000)
    min_quote_volume: float = Field(default=50_000_000, ge=0)
    minimum_age_days: int = Field(default=30, ge=0)
    intervals: list[str] = Field(default_factory=lambda: ["1m", "5m", "15m", "1h"])
    primary_interval: str = "5m"
    bootstrap_candles: int = Field(default=260, ge=60, le=1500)
    history_limit: int = Field(default=600, ge=250, le=5000)
    websocket_batch_size: int = Field(default=180, ge=1, le=1024)
    max_connection_age_seconds: int = Field(default=85_800, ge=60, le=86_399)
    rest_concurrency: int = Field(default=5, ge=1, le=20)
    request_timeout_seconds: float = Field(default=15, ge=1, le=60)
    funding_history_points: int = Field(default=256, ge=3, le=1000)
    funding_refresh_seconds: int = Field(default=300, ge=30, le=3600)
    blacklist: list[str] = Field(default_factory=list)
    excluded_base_assets: list[str] = Field(default_factory=list)

    @field_validator("quote_asset")
    @classmethod
    def normalize_quote(cls, value: str) -> str:
        return value.upper()

    @field_validator("blacklist", "excluded_base_assets")
    @classmethod
    def normalize_lists(cls, values: list[str]) -> list[str]:
        return [value.upper() for value in values]

    @model_validator(mode="after")
    def validate_intervals(self) -> BinanceSettings:
        allowed = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"}
        invalid = set(self.intervals) - allowed
        if invalid:
            raise ValueError(f"unsupported intervals: {sorted(invalid)}")
        if self.primary_interval not in self.intervals:
            raise ValueError("primary_interval must be present in intervals")
        if self.surveillance_n < self.top_n:
            raise ValueError("surveillance_n must be greater than or equal to top_n")
        return self


class VolumeFeatureSettings(StrictModel):
    taker_short_bars: Literal[3] = 3
    taker_long_bars: Literal[12] = 12
    taker_short_threshold: float = Field(default=0.10, ge=0, le=1)
    vpci_short_window: Literal[5] = 5
    vpci_long_window: Literal[20] = 20
    vpci_atr_window: Literal[20] = 20
    vpci_signal_window: Literal[5] = 5
    vpci_slope_lag: Literal[3] = 3


class TechnicalExitSettings(StrictModel):
    """Alert-only, in-memory PAPER position lifecycle controls."""

    enabled: bool = False
    trend_failure_bars: int = Field(default=3, ge=1, le=20)
    trailing_activation_r: float = Field(default=1.0, ge=0, le=10)
    trailing_atr_multiple: float = Field(default=2.0, gt=0, le=20)
    max_holding_bars: int = Field(default=72, ge=1, le=10_000)


class SignalSettings(StrictModel):
    watch_score: int = Field(default=60, ge=1, le=100)
    setup_score: int = Field(default=70, ge=1, le=100)
    confirmed_score: int = Field(default=80, ge=1, le=100)
    cooldown_seconds: int = Field(default=1800, ge=0)
    maximum_spread_bps: float = Field(default=15, ge=0)
    overbought_rsi: float = Field(default=75, ge=50, le=100)
    oversold_rsi: float = Field(default=25, ge=0, le=50)
    relative_volume_threshold: float = Field(default=1.8, ge=0)
    breakout_lookback: int = Field(default=20, ge=5, le=200)
    squeeze_percentile: float = Field(default=15, ge=0, le=100)
    anomaly_horizon_seconds: int = Field(default=30, ge=5, le=600)
    anomaly_min_absolute_return: float = Field(default=0.015, ge=0.001, le=1)
    anomaly_robust_zscore: float = Field(default=4.0, ge=1, le=20)
    anomaly_min_points: int = Field(default=20, ge=5, le=500)
    anomaly_history_points: int = Field(default=600, ge=50, le=10000)
    gate_enabled: bool = False
    trend_gate: int = Field(default=60, ge=0, le=100)
    participation_gate: int = Field(default=60, ge=0, le=100)
    crowding_risk_cap: int = Field(default=75, ge=1, le=100)
    execution_gate: int = Field(default=65, ge=0, le=100)
    completeness_gate: int = Field(default=95, ge=0, le=100)
    book_maximum_age_ms: int = Field(default=2000, ge=0, le=60_000)
    funding_maximum_age_ms: int = Field(default=32_400_000, ge=0, le=604_800_000)
    funding_zscore_lookback_ms: int = Field(
        default=2_592_000_000, ge=86_400_000, le=31_536_000_000
    )
    funding_zscore_minimum_history: int = Field(default=20, ge=2, le=999)
    gate_use_participation: bool = True
    gate_use_crowding: bool = True
    gate_use_higher_timeframes: bool = True
    entry_policy: Literal["legacy_gates", "r2_pit_htf_exec"] = "legacy_gates"
    execution_notional_usdt: float = Field(default=100.0, gt=0, le=1_000_000)
    confirmation_mode: Literal["score", "explicit_trigger"] = "explicit_trigger"
    volume_feature_set: Literal[
        "none", "kline_taker_delta", "normalized_vpci"
    ] = "none"
    volume: VolumeFeatureSettings = VolumeFeatureSettings()
    technical_exit: TechnicalExitSettings = TechnicalExitSettings()
    reversal_intervals: list[str] = Field(
        default_factory=lambda: ["5m", "15m", "1h", "4h"]
    )
    pullback_alert_mode: Literal["off", "informational"] = "off"
    pullback_intervals: list[str] = Field(
        default_factory=lambda: ["5m", "15m", "1h", "4h"]
    )

    @field_validator("reversal_intervals", "pullback_intervals")
    @classmethod
    def validate_rule_intervals(cls, values: list[str]) -> list[str]:
        allowed = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h"}
        invalid = set(values) - allowed
        if invalid:
            raise ValueError(f"unsupported signal intervals: {sorted(invalid)}")
        if len(values) != len(set(values)):
            raise ValueError("signal intervals must not contain duplicates")
        return values

    @model_validator(mode="after")
    def score_order(self) -> SignalSettings:
        if not self.watch_score < self.setup_score < self.confirmed_score:
            raise ValueError("score thresholds must satisfy watch < setup < confirmed")
        if self.entry_policy == "r2_pit_htf_exec" and not self.gate_enabled:
            raise ValueError("r2_pit_htf_exec requires gate_enabled")
        return self


class StorageSettings(StrictModel):
    url: str = "sqlite:///./var/signalbot.db"
    echo_sql: bool = False


class AlertSettings(StrictModel):
    discord_enabled: bool = False
    discord_webhook_url: SecretStr | None = None
    discord_username: str = Field(default="Binance Signal Bot", min_length=1, max_length=80)
    max_attempts: int = Field(default=3, ge=1, le=10)
    timeout_seconds: float = Field(default=10, ge=1, le=60)
    outbox_max_active_items: int = Field(default=10_000, ge=100, le=1_000_000)

    @field_validator("discord_username", mode="before")
    @classmethod
    def normalize_discord_username(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value

    @field_validator("discord_webhook_url")
    @classmethod
    def validate_discord_webhook_url(
        cls, value: SecretStr | None
    ) -> SecretStr | None:
        if value is None:
            return None
        raw = value.get_secret_value().strip()
        try:
            parsed = urlsplit(raw)
            hostname = parsed.hostname
            _port = parsed.port
        except ValueError as exc:
            raise ValueError(
                "discord_webhook_url must be an absolute HTTPS URL"
            ) from exc
        if (
            parsed.scheme.lower() != "https"
            or not hostname
            or any(character.isspace() for character in raw)
        ):
            raise ValueError("discord_webhook_url must be an absolute HTTPS URL")
        return SecretStr(raw)

    @model_validator(mode="after")
    def enabled_requires_url(self) -> AlertSettings:
        if self.discord_enabled and not self.discord_webhook_url:
            raise ValueError("discord_enabled requires discord_webhook_url")
        return self


class RuntimeSettings(StrictModel):
    persist_candles: bool = True
    record_raw_events: bool = False
    raw_event_directory: str = "./var/raw-events"
    raw_event_max_bytes: int = Field(
        default=10_737_418_240,
        ge=1_048_576,
        le=10_995_116_277_760,
    )


class Settings(StrictModel):
    app_name: str = "binance-signal-bot"
    log_level: str = "INFO"
    rule_version: str = "v1.0.0"
    binance: BinanceSettings = BinanceSettings()
    signals: SignalSettings = SignalSettings()
    storage: StorageSettings = StorageSettings()
    alerts: AlertSettings = AlertSettings()
    runtime: RuntimeSettings = RuntimeSettings()

    @model_validator(mode="after")
    def validate_funding_history_capacity(self) -> Settings:
        required = self.signals.funding_zscore_minimum_history + 1
        if self.binance.funding_history_points < required:
            raise ValueError(
                "funding_history_points must exceed funding_zscore_minimum_history"
            )
        if self.signals.entry_policy == "r2_pit_htf_exec":
            if self.signals.confirmation_mode != "explicit_trigger":
                raise ValueError(
                    "r2_pit_htf_exec requires confirmation_mode=explicit_trigger"
                )
            if self.binance.primary_interval != "5m":
                raise ValueError("r2_pit_htf_exec requires primary_interval=5m")
            required_intervals = {"5m", "15m", "1h"}
            missing = sorted(required_intervals.difference(self.binance.intervals))
            if missing:
                raise ValueError(
                    "r2_pit_htf_exec requires subscribed intervals: "
                    + ", ".join(missing)
                )
        return self

    @field_validator("log_level")
    @classmethod
    def normalize_level(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"unsupported log level: {value}")
        return normalized


def _apply_environment(data: dict[str, Any]) -> dict[str, Any]:
    storage = dict(data.get("storage", {}))
    alerts = dict(data.get("alerts", {}))
    if database_url := os.getenv("SIGNALBOT_DATABASE_URL"):
        storage["url"] = database_url
    if log_level := os.getenv("SIGNALBOT_LOG_LEVEL"):
        data["log_level"] = log_level
    if webhook_url := os.getenv("SIGNALBOT_DISCORD_WEBHOOK_URL"):
        alerts["discord_webhook_url"] = webhook_url
        alerts["discord_enabled"] = True
    data["storage"] = storage
    data["alerts"] = alerts
    return data


def load_settings(path: str | Path) -> Settings:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a mapping")
    return Settings.model_validate(_apply_environment(dict(raw)))
