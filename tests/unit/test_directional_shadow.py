from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast

import pytest

from signalbot.clock import ReplayClock
from signalbot.config import (
    DIRECTIONAL_FROZEN_SYMBOLS,
    DIRECTIONAL_PREREGISTRATION_SHA256,
    Settings,
)
from signalbot.domain.enums import Market
from signalbot.domain.models import FeatureSnapshot
from signalbot.persistence.repository import SqlRepository
from signalbot.prospective.directional_shadow import (
    SCHEMA_VERSION,
    DirectionalShadowObserver,
    _json_ready,
)


def _source_identity() -> str:
    return "worktree-source-v1:" + "a" * 64


def _directional_shadow(**updates: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "directional_observation_enabled": True,
        "directional_campaign_id": "futures-bidirectional-test-1",
        "directional_source_identity": _source_identity(),
        "directional_campaign_created_at_ms": 100,
        "directional_activation_ms": 200,
        "directional_symbols": list(DIRECTIONAL_FROZEN_SYMBOLS),
        "directional_preregistration_sha256": DIRECTIONAL_PREREGISTRATION_SHA256,
    }
    values.update(updates)
    return values


class _Repository:
    def __init__(self) -> None:
        self.registered: list[dict[str, Any]] = []

    def register_shadow_campaign(self, **kwargs: Any) -> None:
        self.registered.append(kwargs)

    def get_shadow_campaign(self, _campaign_id: str) -> dict[str, str]:
        return {"manifest_sha256": "b" * 64}


def _observer(
    monkeypatch: pytest.MonkeyPatch,
    *,
    shadow_updates: dict[str, Any] | None = None,
    signal_updates: dict[str, Any] | None = None,
) -> DirectionalShadowObserver:
    shadow = _directional_shadow(**(shadow_updates or {}))
    settings = Settings.model_validate(
        {"shadow": shadow, "signals": signal_updates or {}}
    )
    monkeypatch.setattr(
        "signalbot.prospective.directional_shadow.freeze_source",
        lambda _root: SimpleNamespace(source_identity=_source_identity()),
    )
    return DirectionalShadowObserver(
        settings,
        cast(SqlRepository, _Repository()),
        clock=ReplayClock(1_000),
    )


def test_directional_shadow_defaults_off() -> None:
    settings = Settings()
    assert settings.shadow.directional_observation_enabled is False


def test_directional_shadow_evidence_normalizes_decimal_values() -> None:
    assert _json_ready({"price": Decimal("100.125")}) == {"price": "100.125"}


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
            "shadow": _directional_shadow()
        }
    )
    assert settings.shadow.directional_candidate_version == "futures-bidirectional-v1"
    assert SCHEMA_VERSION == "directional_shadow_observation_v1"


def test_directional_shadow_rejects_activation_before_creation() -> None:
    with pytest.raises(ValueError, match="activation_ms"):
        Settings.model_validate(
            {
                "shadow": _directional_shadow(
                    directional_campaign_created_at_ms=200,
                    directional_activation_ms=100,
                )
            }
        )


@pytest.mark.parametrize(
    "symbols,match",
    [
        (list(DIRECTIONAL_FROZEN_SYMBOLS[:-1]), "frozen eight-symbol"),
        (
            [*DIRECTIONAL_FROZEN_SYMBOLS[:-1], DIRECTIONAL_FROZEN_SYMBOLS[0]],
            "duplicates",
        ),
        (
            [*DIRECTIONAL_FROZEN_SYMBOLS[:-1], "ADAUSDT"],
            "frozen eight-symbol",
        ),
    ],
)
def test_directional_shadow_rejects_non_frozen_universe(
    symbols: list[str], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        Settings.model_validate(
            {"shadow": _directional_shadow(directional_symbols=symbols)}
        )


def test_directional_shadow_normalizes_symbol_order_but_hash_is_order_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    forward = _observer(monkeypatch)
    reverse = _observer(
        monkeypatch,
        shadow_updates={"directional_symbols": list(reversed(DIRECTIONAL_FROZEN_SYMBOLS))},
    )

    assert forward.directional_symbols == frozenset(DIRECTIONAL_FROZEN_SYMBOLS)
    assert forward.universe_sha256 == reverse.universe_sha256
    assert forward.config_sha256 == reverse.config_sha256
    assert forward.policy_sha256 == reverse.policy_sha256


def test_directional_shadow_hash_changes_when_effective_signal_settings_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    baseline = _observer(monkeypatch)
    changed = _observer(monkeypatch, signal_updates={"watch_score": 61})

    assert baseline.signal_config_sha256 != changed.signal_config_sha256
    assert baseline.config_sha256 != changed.config_sha256
    assert baseline.policy_sha256 != changed.policy_sha256


def test_directional_shadow_rejects_wrong_preregistration_hash() -> None:
    with pytest.raises(ValueError, match="preregistration sha256"):
        Settings.model_validate(
            {
                "shadow": _directional_shadow(
                    directional_preregistration_sha256="0" * 64
                )
            }
        )


def test_directional_shadow_rejects_unsubscribable_panel_size() -> None:
    with pytest.raises(ValueError, match="top_n"):
        Settings.model_validate(
            {
                "binance": {"top_n": 7},
                "shadow": _directional_shadow(),
            }
        )


def test_excluded_directional_symbol_short_circuits_before_rule_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = _observer(monkeypatch)

    def fail_evaluate(*_args: object, **_kwargs: object) -> list[object]:
        raise AssertionError("rule engine must not see excluded symbols")

    monkeypatch.setattr(observer.engine, "evaluate", fail_evaluate)
    feature = cast(
        FeatureSnapshot,
        SimpleNamespace(
            market=Market.FUTURES,
            symbol="ADAUSDT",
            event_time_ms=observer.activation_ms,
        ),
    )

    assert observer.observe(feature, {}) == 0
