from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from position_guardian.cli import main
from position_guardian.config import (
    GuardianSettings,
    configured_guardian_alert_delivery_mode,
    load_guardian_settings,
    settings_summary,
)


def _write_config(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "guardian.yaml"
    path.write_text(body, encoding="utf-8")
    return path


def _set_private_read_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSITION_GUARDIAN_API_KEY", "guardian-key-value")
    monkeypatch.setenv("POSITION_GUARDIAN_API_SECRET", "guardian-secret-value")


@pytest.mark.parametrize(
    ("missing_name", "present_name", "present_value"),
    [
        ("POSITION_GUARDIAN_API_KEY", "POSITION_GUARDIAN_API_SECRET", "secret"),
        ("POSITION_GUARDIAN_API_SECRET", "POSITION_GUARDIAN_API_KEY", "key"),
    ],
)
def test_observe_requires_both_private_read_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_name: str,
    present_name: str,
    present_value: str,
) -> None:
    monkeypatch.delenv(missing_name, raising=False)
    monkeypatch.setenv(present_name, present_value)
    path = _write_config(tmp_path, "account_alias: manual-futures\n")

    with pytest.raises(ValidationError, match="private account observation requires") as exc_info:
        load_guardian_settings(path)

    message = str(exc_info.value)
    assert "guardian-key-value" not in message
    assert "guardian-secret-value" not in message


def test_observe_is_default_read_only_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_private_read_credentials(monkeypatch)
    path = _write_config(tmp_path, "account_alias: manual-futures\n")

    settings = load_guardian_settings(path)

    assert settings.mode == "observe"
    assert settings.exchange_environment == "production"
    assert settings.exchange_writes_enabled is False


@pytest.mark.parametrize(
    ("mode", "environment"),
    [("testnet", "testnet"), ("live_protection", "production")],
)
def test_write_capable_modes_require_explicit_write_enablement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    environment: str,
) -> None:
    _set_private_read_credentials(monkeypatch)
    path = _write_config(
        tmp_path,
        f"account_alias: manual-futures\nmode: {mode}\nexchange_environment: {environment}\n",
    )

    with pytest.raises(ValidationError, match="requires exchange_writes_enabled=true"):
        load_guardian_settings(path)


def test_write_capable_mode_requires_matching_exchange_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_private_read_credentials(monkeypatch)
    path = _write_config(
        tmp_path,
        "\n".join(
            [
                "account_alias: manual-futures",
                "mode: testnet",
                "exchange_environment: production",
                "exchange_writes_enabled: true",
                "",
            ]
        ),
    )

    with pytest.raises(ValidationError, match="testnet mode requires exchange_environment=testnet"):
        load_guardian_settings(path)


def test_read_only_modes_reject_write_enablement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_private_read_credentials(monkeypatch)
    path = _write_config(
        tmp_path,
        "account_alias: manual-futures\nmode: shadow\nexchange_writes_enabled: true\n",
    )

    with pytest.raises(ValidationError, match="must remain false in observe/shadow mode"):
        load_guardian_settings(path)


def test_invalid_mode_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_private_read_credentials(monkeypatch)
    path = _write_config(tmp_path, "account_alias: manual-futures\nmode: auto_entry\n")

    with pytest.raises(ValidationError, match="Input should be"):
        load_guardian_settings(path)


def test_inline_api_credentials_are_rejected(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path,
        "\n".join(
            [
                "account_alias: manual-futures",
                "credentials:",
                "  api_key: do-not-store-here",
                "  api_secret: do-not-store-here-either",
                "",
            ]
        ),
    )

    with pytest.raises(ValueError, match="must be supplied through"):
        load_guardian_settings(path)


def test_database_url_and_api_secrets_are_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_private_read_credentials(monkeypatch)
    monkeypatch.setenv(
        "POSITION_GUARDIAN_DATABASE_URL",
        "postgresql+psycopg://guardian:db-password@db.example:5432/guardian?token=hidden",
    )
    path = _write_config(tmp_path, "account_alias: manual-futures\n")

    settings = load_guardian_settings(path)
    summary = settings_summary(settings)
    rendered = repr(settings)
    serialized_summary = json.dumps(summary, sort_keys=True)

    assert summary["database_url"] == (
        "postgresql+psycopg://***:***@db.example:5432/guardian"
    )
    for secret in ("guardian-key-value", "guardian-secret-value", "db-password", "hidden"):
        assert secret not in rendered
        assert secret not in serialized_summary


def test_environment_account_alias_overrides_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _set_private_read_credentials(monkeypatch)
    monkeypatch.setenv("POSITION_GUARDIAN_ACCOUNT_ALIAS", "env-account")
    path = _write_config(tmp_path, "account_alias: file-account\n")

    settings = load_guardian_settings(path)

    assert settings.account_alias == "env-account"


def test_cli_validate_config_and_run_dry_run_are_order_free(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_private_read_credentials(monkeypatch)
    path = _write_config(tmp_path, "account_alias: manual-futures\n")

    main(["validate-config", "--config", str(path)])
    validation = json.loads(capsys.readouterr().out)
    assert validation["mode"] == "observe"
    assert validation["private_read_credentials_configured"] is True

    main(["run", "--config", str(path), "--dry-run"])
    dry_run = json.loads(capsys.readouterr().out)
    assert dry_run["network_calls"] == 0
    assert dry_run["private_account_reads"] == 0
    assert dry_run["exchange_write_calls"] == 0
    assert dry_run["runtime_started"] is False


def test_secret_fields_are_not_exposed_by_model_repr() -> None:
    settings = GuardianSettings.model_validate(
        {
            "account_alias": "manual-futures",
            "credentials": {"api_key": "key-value", "api_secret": "secret-value"},
        }
    )

    rendered = repr(settings)
    assert "key-value" not in rendered
    assert "secret-value" not in rendered


def test_inline_guardian_discord_webhook_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_private_read_credentials(monkeypatch)
    path = _write_config(
        tmp_path,
        "\n".join(
            [
                "account_alias: manual-futures",
                "alerts:",
                "  discord_enabled: true",
                "  discord_webhook_url: https://discord.example/secret",
                "",
            ]
        ),
    )

    with pytest.raises(ValueError, match="POSITION_GUARDIAN_DISCORD_WEBHOOK_URL"):
        load_guardian_settings(path)


def test_guardian_discord_delivery_requires_explicit_source_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_private_read_credentials(monkeypatch)
    monkeypatch.setenv(
        "POSITION_GUARDIAN_DISCORD_WEBHOOK_URL",
        "https://discord.test/private-webhook-secret",
    )
    disabled_path = _write_config(tmp_path, "account_alias: manual-futures\n")
    disabled = load_guardian_settings(disabled_path)
    assert configured_guardian_alert_delivery_mode(disabled) == "disabled"

    enabled_path = _write_config(
        tmp_path,
        "\n".join(
            [
                "account_alias: manual-futures",
                "alerts:",
                "  discord_enabled: true",
                "",
            ]
        ),
    )
    enabled = load_guardian_settings(enabled_path)
    assert configured_guardian_alert_delivery_mode(enabled) == "discord_v1"
    summary = settings_summary(enabled)
    serialized = json.dumps(summary, sort_keys=True)
    assert summary["guardian_alert_transport"]["discord_enabled"] is True  # type: ignore[index]
    assert summary["guardian_alert_transport"]["discord_webhook_configured"] is True  # type: ignore[index]
    assert "private-webhook-secret" not in serialized
    assert "private-webhook-secret" not in repr(enabled)


def test_dispatch_alerts_cli_runs_recovery_only_when_transport_is_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _set_private_read_credentials(monkeypatch)
    database_path = (tmp_path / "guardian.db").as_posix()
    monkeypatch.setenv(
        "POSITION_GUARDIAN_DATABASE_URL",
        f"sqlite:///{database_path}",
    )
    path = _write_config(tmp_path, "account_alias: manual-futures\n")

    main(["dispatch-alerts", "--config", str(path)])
    payload = json.loads(capsys.readouterr().out)

    assert payload["schema_version"] == "position_guardian_alert_dispatch_v1"
    assert payload["delivery_results"] == []
    assert payload["exchange_write_calls"] == 0
    assert payload["recovery"]["inflight_quarantined"] == 0
