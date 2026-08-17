import pytest
from pydantic import ValidationError

from signalbot.config import Settings


def _r2_settings(**overrides: object) -> dict[str, object]:
    signals: dict[str, object] = {
        "entry_policy": "r2_pit_htf_exec",
        "gate_enabled": True,
        "confirmation_mode": "explicit_trigger",
    }
    signals.update(overrides)
    return {"signals": signals}


def test_r2_rejects_score_based_confirmation() -> None:
    with pytest.raises(ValidationError, match="confirmation_mode=explicit_trigger"):
        Settings.model_validate(_r2_settings(confirmation_mode="score"))


@pytest.mark.parametrize(
    ("binance", "message"),
    [
        (
            {"primary_interval": "15m", "intervals": ["5m", "15m", "1h"]},
            "primary_interval=5m",
        ),
        (
            {"primary_interval": "5m", "intervals": ["5m", "1h"]},
            "15m",
        ),
        (
            {"primary_interval": "5m", "intervals": ["5m", "15m"]},
            "1h",
        ),
    ],
)
def test_r2_rejects_missing_frozen_decision_or_context_interval(
    binance: dict[str, object], message: str
) -> None:
    payload = _r2_settings()
    payload["binance"] = binance
    with pytest.raises(ValidationError, match=message):
        Settings.model_validate(payload)


def test_default_legacy_policy_remains_configurable() -> None:
    settings = Settings.model_validate(
        {"binance": {"primary_interval": "15m", "intervals": ["15m"]}}
    )
    assert settings.signals.entry_policy == "legacy_gates"
    assert settings.binance.primary_interval == "15m"
