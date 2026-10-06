"""PostgreSQL smoke test (CI ``postgres`` job).

Skipped unless ``SIGNALBOT_TEST_POSTGRES_URL`` is set, so local runs and the
regular ``test`` job are unaffected. SQLite ignores VARCHAR lengths, which is how
the 35-character frozen ``rule_version`` went unnoticed; this proves the schema,
insert path and startup width check against a real PostgreSQL.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from conftest import make_decision
from signalbot.persistence.repository import (
    RULE_VERSION_MAX_LENGTH,
    SchemaMigrationRequiredError,
    SqlRepository,
    check_rule_version_column_width,
)

POSTGRES_URL = os.environ.get("SIGNALBOT_TEST_POSTGRES_URL")

pytestmark = pytest.mark.skipif(
    not POSTGRES_URL, reason="SIGNALBOT_TEST_POSTGRES_URL is not set"
)


def _fresh_repository() -> SqlRepository:
    assert POSTGRES_URL is not None
    repository = SqlRepository(POSTGRES_URL)
    repository.initialize()
    with repository.engine.begin() as connection:
        for table in ("alert_outbox", "alerts", "signals"):
            connection.execute(text(f"DELETE FROM {table}"))
    return repository


def test_schema_initializes_and_accepts_a_64_character_rule_version() -> None:
    repository = _fresh_repository()
    try:
        rule_version = "v" * RULE_VERSION_MAX_LENGTH
        assert len(rule_version) == 64
        decision = make_decision(event_id="pg-smoke-1", rule_version=rule_version)
        assert repository.save_signal(decision) is True
        stored = repository.recent_signals(limit=1)[0]
        assert stored.rule_version == rule_version
    finally:
        repository.close()


def test_frozen_campaign_rule_version_is_stored() -> None:
    repository = _fresh_repository()
    try:
        decision = make_decision(
            event_id="pg-smoke-2", rule_version="v4.3.0-causal-structure-diagnostics"
        )
        assert repository.save_signal(decision) is True
    finally:
        repository.close()


def test_startup_width_check_passes_on_a_current_schema_and_rejects_a_narrow_one() -> None:
    from sqlalchemy import inspect

    repository = _fresh_repository()
    try:
        check_rule_version_column_width(inspect(repository.engine))  # current schema passes
        with repository.engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE signals ALTER COLUMN rule_version TYPE VARCHAR(32)")
            )
        with pytest.raises(SchemaMigrationRequiredError, match=r"VARCHAR\(64\)"):
            check_rule_version_column_width(inspect(repository.engine))
    finally:
        with repository.engine.begin() as connection:
            connection.execute(
                text("ALTER TABLE signals ALTER COLUMN rule_version TYPE VARCHAR(64)")
            )
        repository.close()
