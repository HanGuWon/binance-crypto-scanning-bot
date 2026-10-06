from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import String
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from conftest import make_decision
from signalbot.persistence.models import ShadowCampaignRow, SignalRow
from signalbot.persistence.repository import (
    SchemaMigrationRequiredError,
    SqlRepository,
    check_rule_version_column_width,
)

LONG_FROZEN_VERSION = "v4.3.0-causal-structure-diagnostics"


class _FakeInspector:
    def __init__(self, columns: dict[str, list[dict[str, Any]]]) -> None:
        self._columns = columns

    def has_table(self, name: str) -> bool:
        return name in self._columns

    def get_columns(self, name: str) -> list[dict[str, Any]]:
        return self._columns[name]


def _rv(length: int | None) -> list[dict[str, Any]]:
    return [
        {"name": "event_id", "type": String(32)},
        {"name": "rule_version", "type": String(length)},
    ]


@pytest.mark.parametrize("model", [SignalRow, ShadowCampaignRow])
def test_postgresql_ddl_uses_varchar_64(model: Any) -> None:
    ddl = str(CreateTable(model.__table__).compile(dialect=postgresql.dialect()))
    assert "rule_version VARCHAR(64)" in ddl


def test_frozen_rule_version_fits_the_column() -> None:
    assert len(LONG_FROZEN_VERSION) == 35
    assert SignalRow.__table__.c.rule_version.type.length >= len(LONG_FROZEN_VERSION)


def test_rule_version_length_boundary() -> None:
    assert make_decision(rule_version="v" * 64).rule_version == "v" * 64
    with pytest.raises(ValidationError):
        make_decision(rule_version="v" * 65)
    with pytest.raises(ValidationError):
        make_decision(rule_version="")


def test_schema_check_rejects_narrow_columns_and_names_the_alter() -> None:
    inspector = _FakeInspector({"signals": _rv(32), "shadow_campaigns": _rv(32)})
    with pytest.raises(SchemaMigrationRequiredError) as caught:
        check_rule_version_column_width(inspector)
    message = str(caught.value)
    assert "ALTER TABLE signals ALTER COLUMN rule_version TYPE VARCHAR(64);" in message
    assert "ALTER TABLE shadow_campaigns ALTER COLUMN rule_version TYPE VARCHAR(64);" in message


def test_schema_check_accepts_wide_missing_and_unbounded_columns() -> None:
    check_rule_version_column_width(_FakeInspector({"signals": _rv(64)}))
    check_rule_version_column_width(_FakeInspector({"signals": _rv(128)}))
    check_rule_version_column_width(_FakeInspector({"signals": _rv(None)}))
    check_rule_version_column_width(_FakeInspector({}))


def test_schema_check_boundary_63_rejected() -> None:
    with pytest.raises(SchemaMigrationRequiredError):
        check_rule_version_column_width(_FakeInspector({"signals": _rv(63)}))


def test_sqlite_initialize_does_not_run_the_postgres_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    monkeypatch.setattr(
        "signalbot.persistence.repository.check_rule_version_column_width",
        lambda inspector: calls.append(inspector),
    )
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    repository.close()
    assert calls == []
