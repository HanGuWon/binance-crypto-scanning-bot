"""Regression tests for the Phase-R SQLite connection boundary."""

from __future__ import annotations

import time

import pytest
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import StaticPool

from conftest import make_candle
from signalbot.persistence.repository import SqlRepository

EXPECTED_BUSY_TIMEOUT_MS = 2_000


def test_file_sqlite_engine_has_explicit_finite_busy_timeout(tmp_path) -> None:
    repository = SqlRepository(f"sqlite:///{tmp_path / 'phase-r.db'}")
    repository.initialize()
    try:
        with repository.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar() == (
                EXPECTED_BUSY_TIMEOUT_MS
            )
            assert connection.exec_driver_sql("PRAGMA journal_mode").scalar() == "delete"
    finally:
        repository.close()


def test_memory_sqlite_keeps_shared_static_pool(tmp_path) -> None:
    del tmp_path
    repository = SqlRepository("sqlite:///:memory:")
    repository.initialize()
    try:
        assert isinstance(repository.engine.pool, StaticPool)
        with repository.engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar() == (
                EXPECTED_BUSY_TIMEOUT_MS
            )
    finally:
        repository.close()


def test_file_sqlite_lock_failure_is_bounded_and_recovers(tmp_path) -> None:
    database = tmp_path / "bounded-lock.db"
    holder = SqlRepository(f"sqlite:///{database}")
    contender = SqlRepository(f"sqlite:///{database}")
    holder.initialize()
    contender.initialize()
    lock_connection = holder.engine.connect()
    try:
        lock_connection.exec_driver_sql("BEGIN IMMEDIATE")
        started = time.monotonic()
        with pytest.raises(OperationalError, match="locked"):
            contender.save_candle(make_candle(1234))
        elapsed = time.monotonic() - started
        assert elapsed < 3.5
    finally:
        lock_connection.rollback()
        lock_connection.close()
        holder.close()
        contender.close()

    recovered = SqlRepository(f"sqlite:///{database}")
    recovered.initialize()
    try:
        assert recovered.save_candle(make_candle(1234)) is True
    finally:
        recovered.close()
