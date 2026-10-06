from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from position_guardian.alert_contract import (
    GUARDIAN_ALERT_DELIVERY_DISABLED,
    GUARDIAN_ALERT_DELIVERY_DISCORD_V1,
    GuardianAlertDeliveryMode,
)

GuardianMode = Literal["observe", "shadow", "testnet", "live_protection"]
ExchangeEnvironment = Literal["production", "testnet"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)


class GuardianCredentials(StrictModel):
    api_key: SecretStr | None = Field(default=None, repr=False)
    api_secret: SecretStr | None = Field(default=None, repr=False)

    @field_validator("api_key", "api_secret")
    @classmethod
    def normalize_secret(cls, value: SecretStr | None) -> SecretStr | None:
        if value is None:
            return None
        raw = value.get_secret_value().strip()
        if not raw:
            return None
        return SecretStr(raw)


class GuardianAlertSettings(StrictModel):
    discord_enabled: bool = False
    discord_webhook_url: SecretStr | None = Field(default=None, repr=False)
    max_attempts: int = Field(default=3, ge=1, le=10)
    timeout_seconds: float = Field(default=10, ge=1, le=60)
    retry_after_max_seconds: float = Field(default=30, ge=0.05, le=60)
    outbox_max_active_items: int = Field(default=10_000, ge=100, le=1_000_000)
    batch_limit: int = Field(default=100, ge=1, le=1_000)

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
    def enabled_requires_url(self) -> GuardianAlertSettings:
        if self.discord_enabled and self.discord_webhook_url is None:
            raise ValueError("discord_enabled requires discord_webhook_url")
        return self


class GuardianSettings(StrictModel):
    mode: GuardianMode = "observe"
    account_alias: str
    exchange_environment: ExchangeEnvironment = "production"
    database_url: SecretStr = Field(
        default_factory=lambda: SecretStr("sqlite:///./var/position-guardian.db"),
        repr=False,
    )
    credentials: GuardianCredentials = Field(default_factory=GuardianCredentials, repr=False)
    alerts: GuardianAlertSettings = Field(default_factory=GuardianAlertSettings, repr=False)
    exchange_writes_enabled: bool = False

    @field_validator("account_alias")
    @classmethod
    def normalize_account_alias(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("account_alias must not be blank")
        if len(normalized) > 80:
            raise ValueError("account_alias must be at most 80 characters")
        return normalized

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value().strip()
        if not raw or any(character.isspace() for character in raw):
            raise ValueError("database_url must be a non-empty URL without whitespace")
        try:
            parsed = urlsplit(raw)
            hostname = parsed.hostname
            _port = parsed.port
        except ValueError as exc:
            raise ValueError("database_url must be a valid SQLAlchemy-style URL") from exc
        if not parsed.scheme:
            raise ValueError("database_url must include a URL scheme")
        if parsed.scheme == "sqlite":
            if not parsed.path:
                raise ValueError("sqlite database_url must include a database path")
        elif not hostname:
            raise ValueError("non-sqlite database_url must include a hostname")
        return SecretStr(raw)

    @model_validator(mode="after")
    def validate_capability_boundary(self) -> GuardianSettings:
        if self.credentials.api_key is None or self.credentials.api_secret is None:
            raise ValueError(
                "private account observation requires POSITION_GUARDIAN_API_KEY "
                "and POSITION_GUARDIAN_API_SECRET"
            )

        write_capable = self.mode in {"testnet", "live_protection"}
        if write_capable and not self.exchange_writes_enabled:
            raise ValueError(
                f"mode={self.mode} requires exchange_writes_enabled=true"
            )
        if not write_capable and self.exchange_writes_enabled:
            raise ValueError(
                "exchange_writes_enabled must remain false in observe/shadow mode"
            )
        if self.mode == "testnet" and self.exchange_environment != "testnet":
            raise ValueError("testnet mode requires exchange_environment=testnet")
        if self.mode == "live_protection" and self.exchange_environment != "production":
            raise ValueError(
                "live_protection mode requires exchange_environment=production"
            )
        return self


def redact_database_url(url: SecretStr | str) -> str:
    raw = url.get_secret_value() if isinstance(url, SecretStr) else url
    parsed = urlsplit(raw)
    if parsed.scheme == "sqlite":
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))

    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    if parsed.username is not None or parsed.password is not None:
        host = f"***:***@{host}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def settings_summary(settings: GuardianSettings) -> dict[str, object]:
    return {
        "schema_version": "position_guardian_config_v1",
        "mode": settings.mode,
        "account_alias": settings.account_alias,
        "exchange_environment": settings.exchange_environment,
        "database_url": redact_database_url(settings.database_url),
        "private_read_credentials_configured": (
            settings.credentials.api_key is not None
            and settings.credentials.api_secret is not None
        ),
        "exchange_writes_enabled": settings.exchange_writes_enabled,
        "guardian_alert_transport": {
            "discord_enabled": settings.alerts.discord_enabled,
            "discord_webhook_configured": settings.alerts.discord_webhook_url is not None,
            "max_attempts": settings.alerts.max_attempts,
            "timeout_seconds": settings.alerts.timeout_seconds,
            "retry_after_max_seconds": settings.alerts.retry_after_max_seconds,
            "outbox_max_active_items": settings.alerts.outbox_max_active_items,
            "batch_limit": settings.alerts.batch_limit,
        },
    }


def configured_guardian_alert_delivery_mode(
    settings: GuardianSettings,
) -> GuardianAlertDeliveryMode:
    if settings.alerts.discord_enabled and settings.alerts.discord_webhook_url is not None:
        return GUARDIAN_ALERT_DELIVERY_DISCORD_V1
    return GUARDIAN_ALERT_DELIVERY_DISABLED


def _apply_environment(data: dict[str, Any]) -> dict[str, Any]:
    credentials = dict(data.get("credentials", {}))
    if api_key := os.getenv("POSITION_GUARDIAN_API_KEY"):
        credentials["api_key"] = api_key
    if api_secret := os.getenv("POSITION_GUARDIAN_API_SECRET"):
        credentials["api_secret"] = api_secret
    if database_url := os.getenv("POSITION_GUARDIAN_DATABASE_URL"):
        data["database_url"] = database_url
    if account_alias := os.getenv("POSITION_GUARDIAN_ACCOUNT_ALIAS"):
        data["account_alias"] = account_alias
    alerts = dict(data.get("alerts", {}))
    if webhook := os.getenv("POSITION_GUARDIAN_DISCORD_WEBHOOK_URL"):
        alerts["discord_webhook_url"] = webhook
    data["alerts"] = alerts
    data["credentials"] = credentials
    return data


def load_guardian_settings(path: str | Path) -> GuardianSettings:
    with Path(path).open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a mapping")

    credentials = raw.get("credentials")
    if isinstance(credentials, dict) and ({"api_key", "api_secret"} & credentials.keys()):
        raise ValueError(
            "Guardian API credentials must be supplied through "
            "POSITION_GUARDIAN_API_KEY and POSITION_GUARDIAN_API_SECRET"
        )
    alerts = raw.get("alerts")
    if isinstance(alerts, dict) and "discord_webhook_url" in alerts:
        raise ValueError(
            "Guardian Discord webhook must be supplied through "
            "POSITION_GUARDIAN_DISCORD_WEBHOOK_URL"
        )
    return GuardianSettings.model_validate(_apply_environment(dict(raw)))
