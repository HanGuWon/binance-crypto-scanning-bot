from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from signalbot.backtest.config import load_backtest_spec
from signalbot.config import AlertSettings, Settings, load_settings

ROOT = Path(__file__).resolve().parents[2]


def test_example_configuration_loads() -> None:
    settings = load_settings(ROOT / "config/settings.example.yaml")
    assert settings.binance.primary_interval == "5m"
    assert settings.alerts.discord_enabled is False
    assert settings.binance.history_limit >= settings.binance.bootstrap_candles
    assert settings.binance.surveillance_n >= settings.binance.top_n
    assert settings.signals.reversal_intervals == []
    assert settings.signals.pullback_alert_mode == "informational"
    assert settings.signals.pullback_intervals == ["5m", "15m", "1h", "4h"]
    assert settings.signals.entry_policy == "r2_pit_htf_exec"
    assert settings.signals.execution_notional_usdt == 100.0
    assert settings.signals.technical_exit.enabled is True
    assert settings.signals.technical_exit.trend_failure_bars == 3
    assert settings.signals.technical_exit.trailing_activation_r == 1.0
    assert settings.signals.technical_exit.trailing_atr_multiple == 2.0
    assert settings.signals.technical_exit.max_holding_bars == 72


def test_paper_technical_exit_lifecycle_is_disabled_by_default() -> None:
    settings = Settings()
    assert settings.signals.technical_exit.enabled is False
    assert settings.signals.pullback_alert_mode == "off"


def test_environment_overrides_are_applied(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIGNALBOT_DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("SIGNALBOT_LOG_LEVEL", "warning")
    monkeypatch.setenv("SIGNALBOT_DISCORD_WEBHOOK_URL", "https://discord.test/webhook")
    settings = load_settings(ROOT / "config/settings.example.yaml")
    assert settings.storage.url == "sqlite:///:memory:"
    assert settings.log_level == "WARNING"
    assert settings.alerts.discord_enabled is True
    assert settings.alerts.discord_webhook_url is not None
    assert settings.alerts.discord_webhook_url.get_secret_value().endswith("/webhook")


@pytest.mark.parametrize(
    "url",
    [
        "   ",
        "not-a-url",
        "/relative/webhook",
        "http://discord.test/webhook",
        "https://discord.test:bad/webhook",
        "https://discord .test/webhook",
    ],
)
def test_discord_webhook_must_be_an_absolute_https_url(url: str) -> None:
    with pytest.raises(ValidationError, match="absolute HTTPS URL"):
        AlertSettings(
            discord_enabled=True,
            discord_webhook_url=SecretStr(url),
        )


def test_discord_username_is_trimmed_and_bounded() -> None:
    assert AlertSettings(discord_username="  Signal Bot  ").discord_username == "Signal Bot"
    with pytest.raises(ValidationError):
        AlertSettings(discord_username="   ")
    with pytest.raises(ValidationError):
        AlertSettings(discord_username="x" * 81)


def test_score_thresholds_must_be_strictly_ordered() -> None:
    with pytest.raises(ValidationError, match="watch < setup < confirmed"):
        Settings.model_validate(
            {"signals": {"watch_score": 70, "setup_score": 70, "confirmed_score": 80}}
        )


def test_primary_interval_must_be_subscribed() -> None:
    with pytest.raises(ValidationError, match="primary_interval"):
        Settings.model_validate({"binance": {"intervals": ["1m", "15m"], "primary_interval": "5m"}})


def test_surveillance_limit_must_cover_tradable_limit() -> None:
    with pytest.raises(ValidationError, match="surveillance_n"):
        Settings.model_validate({"binance": {"top_n": 2, "surveillance_n": 1}})


def test_frozen_backtest_contract_loads() -> None:
    spec = load_backtest_spec(ROOT / "config/backtest.research.yaml")
    assert spec.protocol_version == "bt_1h_v1_frozen_2026-07-14"
    assert len(spec.assets) == 16
    assert [split.name for split in spec.splits] == ["development", "validation", "holdout"]
    assert spec.assets[-1].futures_symbol == "1000PEPEUSDT"
