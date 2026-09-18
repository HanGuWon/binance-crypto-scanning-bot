import pytest

from signalbot.config import Settings
from signalbot.prospective.directional_shadow import SCHEMA_VERSION


def _source_identity() -> str:
    return "worktree-source-v1:" + "a" * 64


def test_directional_shadow_defaults_off() -> None:
    settings = Settings()
    assert settings.shadow.directional_observation_enabled is False


def test_directional_shadow_requires_future_campaign_provenance() -> None:
    with pytest.raises(ValueError, match="directional observation requires"):
        Settings.model_validate(
            {
                "shadow": {"directional_observation_enabled": True},
            }
        )


def test_directional_shadow_config_accepts_frozen_provenance() -> None:
    settings = Settings.model_validate(
        {
            "shadow": {
                "directional_observation_enabled": True,
                "directional_campaign_id": "futures-bidirectional-test-1",
                "directional_source_identity": _source_identity(),
                "directional_campaign_created_at_ms": 100,
                "directional_activation_ms": 200,
            }
        }
    )
    assert settings.shadow.directional_candidate_version == "futures-bidirectional-v1"
    assert SCHEMA_VERSION == "directional_shadow_observation_v1"


def test_directional_shadow_rejects_activation_before_creation() -> None:
    with pytest.raises(ValueError, match="activation_ms"):
        Settings.model_validate(
            {
                "shadow": {
                    "directional_observation_enabled": True,
                    "directional_campaign_id": "futures-bidirectional-test-1",
                    "directional_source_identity": _source_identity(),
                    "directional_campaign_created_at_ms": 200,
                    "directional_activation_ms": 100,
                }
            }
        )
