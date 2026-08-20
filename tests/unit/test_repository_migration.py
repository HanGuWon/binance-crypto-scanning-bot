"""Adversarial migration + campaign-provenance tests for the shadow schema."""

from __future__ import annotations

import sqlite3

import pytest

from signalbot.persistence.repository import EventIdConflictError, SqlRepository

LEGACY_OBSERVATIONS_DDL = """
CREATE TABLE shadow_observations(
    observation_id VARCHAR(64) PRIMARY KEY,
    campaign_id VARCHAR(64), opportunity_id VARCHAR(64),
    market VARCHAR(16), symbol VARCHAR(32), family VARCHAR(32),
    direction VARCHAR(16), decision_time_ms BIGINT, primary_interval VARCHAR(8),
    payload_json TEXT, payload_sha256 VARCHAR(64), policy_sha256 VARCHAR(64),
    created_at_ms BIGINT
);
"""

LEGACY_COVERAGE_DDL = """
CREATE TABLE shadow_coverage(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id VARCHAR(64), market VARCHAR(16), decision_close_ms BIGINT,
    primary_interval VARCHAR(8), expected_tradable_count INTEGER,
    tradable_universe_hash VARCHAR(64), mature_count INTEGER,
    htf_ready_count INTEGER, fresh_bbo_count INTEGER, raw_c0_count INTEGER,
    comparator_rows INTEGER, complete BOOLEAN, failures_json TEXT
);
"""

LEGACY_OBS_ROW = (
    "obs-legacy-1", "legacy-c", "opp-1", "SPOT", "BTCUSDT",
    "BREAKOUT_LONG", "LONG", 1_700_000_000_000, "5m",
    '{"k":"v"}', "payload-sha", "policy-sha", 1_700_000_000_000,
)


def _table_name(ddl):
    return ddl.strip().split("(")[0].replace("CREATE TABLE", "").strip()


def _build_db(tmp_path, blocks):
    db = tmp_path / "legacy.db"
    con = sqlite3.connect(str(db))
    try:
        for ddl, rows in blocks:
            con.execute(ddl)
            if rows:
                ph = ",".join("?" for _ in rows[0])
                con.executemany(f'INSERT INTO "{_table_name(ddl)}" VALUES ({ph})', rows)
        con.commit()
    finally:
        con.close()
    return db


def _columns(db, table):
    con = sqlite3.connect(str(db))
    try:
        return {r[1] for r in con.execute(f'PRAGMA table_info("{table}")')}
    finally:
        con.close()


def _rows(db, table):
    con = sqlite3.connect(str(db))
    try:
        return con.execute(f'SELECT * FROM "{table}"').fetchall()
    finally:
        con.close()


def test_case1_fresh_db_initializes_with_current_schema(tmp_path):
    db = tmp_path / "fresh.db"
    repo = SqlRepository(f"sqlite:///{db}")
    repo.initialize()
    try:
        assert "campaign_manifest_sha256" in _columns(db, "shadow_observations")
        cov = _columns(db, "shadow_coverage")
        for c in ("status", "content_sha256", "campaign_manifest_sha256",
                  "seen_symbols_json", "first_seen_ms", "sealed_at_ms",
                  "evidence_failures"):
            assert c in cov
        camp = _columns(db, "shadow_campaigns")
        for c in ("manifest_sha256", "manifest_json", "campaign_schema_version",
                  "markets_json", "families_json", "primary_interval",
                  "policy_name"):
            assert c in camp
    finally:
        repo.close()


def test_case2_legacy_coverage_migrates_without_losing_rows(tmp_path):
    cov = (1, "camp-1", "SPOT", 1_700_000_000_000, "5m", 2, "us", 2, 2, 1, 1, 1, 1, "[]")
    db = _build_db(tmp_path, [(LEGACY_COVERAGE_DDL, [cov])])
    repo = SqlRepository(f"sqlite:///{db}")
    repo.initialize()
    try:
        cols = _columns(db, "shadow_coverage")
        for c in ("status", "content_sha256", "campaign_manifest_sha256",
                  "seen_symbols_json", "first_seen_ms", "sealed_at_ms",
                  "evidence_failures"):
            assert c in cols
        rows = _rows(db, "shadow_coverage")
        assert len(rows) == 1
        assert rows[0][1] == "camp-1" and rows[0][5] == 2 and rows[0][10] == 1
    finally:
        repo.close()


def test_case3_legacy_observations_add_provenance_column_without_deleting(tmp_path):
    db = _build_db(tmp_path, [(LEGACY_OBSERVATIONS_DDL, [LEGACY_OBS_ROW])])
    repo = SqlRepository(f"sqlite:///{db}")
    repo.initialize()
    try:
        assert "campaign_manifest_sha256" in _columns(db, "shadow_observations")
        rows = _rows(db, "shadow_observations")
        assert len(rows) == 1
        assert rows[0][0] == "obs-legacy-1" and rows[0][4] == "BTCUSDT"
    finally:
        repo.close()


LEGACY_CAMPAIGNS_DDL = """
CREATE TABLE shadow_campaigns(
    campaign_id VARCHAR(64) PRIMARY KEY, campaign_mode VARCHAR(16),
    source_identity VARCHAR(128), rule_version VARCHAR(32),
    policy_sha256 VARCHAR(64), config_sha256 VARCHAR(64),
    observation_schema_version VARCHAR(32), policy_version VARCHAR(32),
    activation_ms BIGINT, created_at_ms BIGINT, status VARCHAR(16)
);
"""


def test_case4_legacy_campaign_provenance_is_never_fabricated(tmp_path):
    legacy = (
        "legacy-c", "PROSPECTIVE", "binance-public", "r2",
        "policy-sha", "config-sha", "observ-v1", "pv1",
        1_700_000_000_000, 1_700_000_000_000, "registered",
    )
    db = _build_db(tmp_path, [(LEGACY_CAMPAIGNS_DDL, [legacy])])
    repo = SqlRepository(f"sqlite:///{db}")
    repo.initialize()
    try:
        cols = _columns(db, "shadow_campaigns")
        for c in ("manifest_sha256", "manifest_json", "campaign_schema_version",
                  "markets_json", "families_json", "primary_interval",
                  "policy_name"):
            assert c in cols
        # Legacy row must remain blank-provenance; migration must not invent
        # a manifest for a row that predates the campaign-manifest authority.
        rows = _rows(db, "shadow_campaigns")
        assert len(rows) == 1
        con = sqlite3.connect(str(db))
        try:
            manifest = con.execute(
                "SELECT manifest_sha256 FROM shadow_campaigns "
                "WHERE campaign_id='legacy-c'"
            ).fetchone()[0]
        finally:
            con.close()
        assert manifest == ""
        # Reusing the id with real content must conflict loudly rather than
        # silently adopting the blank legacy provenance.
        with pytest.raises(EventIdConflictError):
            repo.register_shadow_campaign(
                campaign_schema_version="v1",
                campaign_id="legacy-c",
                campaign_mode="prospective",
                source_identity="binance-public",
                rule_version="r2",
                policy_name="p",
                policy_version="pv1",
                policy_sha256="policy-sha",
                config_sha256="config-sha",
                observation_schema_version="observ-v1",
                    primary_interval="5m",
                    markets=["SPOT"],
                    families={"SPOT": ["BREAKOUT_LONG"]},
                    activation_ms=1_700_000_000_000,
                    created_at_ms=1_700_000_000_000,
                    status="registered",
                )
    finally:
        repo.close()


def test_case5_migration_idempotent_across_reinitialize(tmp_path):
    cov = (1, "camp-1", "SPOT", 1_700_000_000_000, "5m", 2, "us", 2, 2, 1, 1, 1, 1, "[]")
    db = _build_db(tmp_path, [
        (LEGACY_OBSERVATIONS_DDL, [LEGACY_OBS_ROW]),
        (LEGACY_COVERAGE_DDL, [cov]),
    ])
    repo = SqlRepository(f"sqlite:///{db}")
    repo.initialize()
    repo.initialize()
    try:
        assert len(_rows(db, "shadow_observations")) == 1
        assert len(_rows(db, "shadow_coverage")) == 1
    finally:
        repo.close()


def test_case6_evidence_write_under_blank_provenance_fails_closed(tmp_path):
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        with pytest.raises(EventIdConflictError):
            repo.save_shadow_observation(
                observation_id="o1", campaign_id="never-registered",
                opportunity_id="opp", market="SPOT", symbol="BTCUSDT",
                family="BREAKOUT_LONG", direction="LONG",
                decision_time_ms=1_700_000_000_000, primary_interval="5m",
                payload={"k": "v"}, policy_sha256="p",
                campaign_manifest_sha256="", created_at_ms=1_700_000_000_000,
            )
    finally:
        repo.close()
