from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from signalbot.signals.protection_context import (
    PROTECTION_CONTEXT_VERSION,
    ProtectionContext,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "protection_context"
_VERSION_PATTERN = re.compile(r"^protection-context-v(?P<major>\d+)$")
_SUPPORTED_MAJOR = 1


class UnsupportedProtectionContextVersion(ValueError):
    """Raised when a Guardian consumer cannot safely interpret the payload."""


def _load(name: str) -> dict[str, Any]:
    value = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _guardian_parse(payload: dict[str, Any]) -> ProtectionContext:
    version = payload.get("context_version")
    if not isinstance(version, str):
        raise UnsupportedProtectionContextVersion("context_version must be a string")
    match = _VERSION_PATTERN.fullmatch(version)
    if match is None or int(match.group("major")) != _SUPPORTED_MAJOR:
        raise UnsupportedProtectionContextVersion(version)
    return ProtectionContext.model_validate(payload)


def test_current_v1_fixture_is_frozen_and_valid() -> None:
    payload = _load("protection_context_v1.json")

    context = _guardian_parse(payload)

    assert PROTECTION_CONTEXT_VERSION == "protection-context-v1"
    assert context.context_version == PROTECTION_CONTEXT_VERSION
    assert context.context_id == (
        "753359fc22b823bd4bb9db6322c35d54a0c5e8ddb06b91fd16f6d1c6635ff571"
    )
    assert context.model_dump(mode="json") == payload


def test_v1_unknown_transport_field_is_ignored_without_changing_identity() -> None:
    current = _guardian_parse(_load("protection_context_v1.json"))
    extended = _guardian_parse(_load("protection_context_v1_unknown_field.json"))

    assert extended == current
    assert "producer_transport_note" not in extended.model_dump(mode="json")


@pytest.mark.parametrize(
    "fixture_name",
    ["protection_context_v0.json", "protection_context_v2.json"],
)
def test_guardian_contract_rejects_unsupported_major_versions(fixture_name: str) -> None:
    payload = _load(fixture_name)

    assert ProtectionContext.model_validate(payload).context_version == payload["context_version"]

    with pytest.raises(UnsupportedProtectionContextVersion):
        _guardian_parse(payload)


@pytest.mark.parametrize("version", [None, 1, "v1", "protection-context-v1.1"])
def test_guardian_contract_rejects_malformed_version_values(version: object) -> None:
    payload = _load("protection_context_v1.json")
    payload["context_version"] = version

    with pytest.raises(UnsupportedProtectionContextVersion):
        _guardian_parse(payload)
