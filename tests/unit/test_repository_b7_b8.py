"""Phase B terminal contracts: campaign isolation (B7) + OPEN->SEALED
monotonicity (B8) for the shadow coverage repository."""

from __future__ import annotations

import sqlite3

import pytest

from signalbot.persistence.repository import EventIdConflictError, SqlRepository

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


def _register(repo, campaign_id):
    repo.register_shadow_campaign(
        campaign_schema_version="v1",
        campaign_id=campaign_id,
        campaign_mode="prospective",
        source_identity="src",
        rule_version="r2",
        policy_name="p",
        policy_version="pv1",
        policy_sha256="psh",
        config_sha256="csh",
        observation_schema_version="osv",
        primary_interval="5m",
        markets=["SPOT"],
        families={"SPOT": ["BREAKOUT_LONG"]},
        activation_ms=1_700_000_000_000,
        created_at_ms=1_700_000_000_000,
        status="registered",
    )
    return repo.get_shadow_campaign(campaign_id)["manifest_sha256"]


def _build_legacy(tmp_path, rows):
    db = tmp_path / "legacy.db"
    con = sqlite3.connect(str(db))
    try:
        con.execute(LEGACY_COVERAGE_DDL)
        con.executemany(
            "INSERT INTO shadow_coverage VALUES ({})".format(
                ",".join("?" for _ in rows[0])
            ),
            rows,
        )
        con.commit()
    finally:
        con.close()
    return db


def test_b7_legacy_coverage_campaign_isolation(tmp_path):
    # A legacy blank-provenance coverage row must never become evidence owned
    # by a newly registered prospective campaign.
    cov = (1, "legacy-x", "SPOT", 1_700_000_000_000, "5m",
           2, "u", 2, 2, 1, 1, 1, 1, "[]")
    db = _build_legacy(tmp_path, [cov])
    repo = SqlRepository(f"sqlite:///{db}")
    repo.initialize()
    try:
        con = sqlite3.connect(str(db))
        try:
            status = con.execute(
                "SELECT status FROM shadow_coverage WHERE campaign_id='legacy-x'"
            ).fetchone()[0]
            manifest = con.execute(
                "SELECT campaign_manifest_sha256 FROM shadow_coverage "
                "WHERE campaign_id='legacy-x'"
            ).fetchone()[0]
        finally:
            con.close()
        # Path A: migrated to a distinct non-OPEN state, never adopted.
        assert status == "LEGACY_UNVERIFIED"
        assert manifest == ""
        # Path B: registering that campaign id is rejected loudly, so the
        # legacy row can never be folded into a new prospective denominator.
        with pytest.raises(EventIdConflictError):
            _register(repo, "legacy-x")
    finally:
        repo.close()


def test_b8_open_to_sealed_is_monotonic_and_shape_valid():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo, "c8")
        close = 1_700_000_000_000
        repo.begin_shadow_coverage(
            campaign_id="c8", market="SPOT", decision_close_ms=close,
            primary_interval="5m", expected_tradable_count=5,
            tradable_universe_hash="u", campaign_manifest_sha256=manifest,
            first_seen_ms=close,
        )
        repo.update_shadow_coverage_progress(
            campaign_id="c8", market="SPOT", decision_close_ms=close,
            primary_interval="5m", campaign_manifest_sha256=manifest,
            mature_count=5, htf_ready_count=4, fresh_bbo_count=4,
            raw_c0_count=3, comparator_rows=3, evidence_failures=2,
            seen_symbols=["A", "B", "C", "D", "E"],
        )

        def _seal(**over):
            args = dict(
                campaign_id="c8", market="SPOT", decision_close_ms=close,
                primary_interval="5m", expected_tradable_count=5,
                tradable_universe_hash="u", campaign_manifest_sha256=manifest,
                mature_count=5, htf_ready_count=4, fresh_bbo_count=4,
                raw_c0_count=3, comparator_rows=3, evidence_failures=2,
                seen_symbols=["A", "B", "C", "D", "E"],
                sealed_at_ms=close + 1, complete=True, failures=[],
                created_at_ms=close,
            )
            args.update(over)
            repo.save_shadow_coverage(**args)

        regressions = [
            {"mature_count": 4, "seen_symbols": ["A", "B", "C", "D"]},
            {"htf_ready_count": 3},
            {"fresh_bbo_count": 3},
            {"raw_c0_count": 2},
            {"comparator_rows": 2},
            {"evidence_failures": 1},
            {"seen_symbols": ["A", "B", "C", "D"]},
        ]
        for over in regressions:
            with pytest.raises(EventIdConflictError):
                _seal(**over)
        # A non-regressing, shape-valid seal must succeed.
        _seal()
        coverage = repo.get_shadow_coverage(
            campaign_id="c8", market="SPOT", decision_close_ms=close,
            primary_interval="5m",
        )
        assert coverage is not None
        assert coverage["status"] == "SEALED"
        assert coverage["complete"] is True
        assert coverage["mature_count"] == 5
        assert coverage["raw_c0_count"] == 3
        assert coverage["comparator_rows"] == 3
    finally:
        repo.close()


def test_b8_seal_rejects_seen_symbol_regression():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo, "c8b")
        close = 1_700_000_000_001
        repo.begin_shadow_coverage(
            campaign_id="c8b", market="SPOT", decision_close_ms=close,
            primary_interval="5m", expected_tradable_count=3,
            tradable_universe_hash="u", campaign_manifest_sha256=manifest,
            first_seen_ms=close,
        )
        repo.update_shadow_coverage_progress(
            campaign_id="c8b", market="SPOT", decision_close_ms=close,
            primary_interval="5m", campaign_manifest_sha256=manifest,
            mature_count=3, htf_ready_count=3, fresh_bbo_count=3,
            raw_c0_count=2, comparator_rows=2, evidence_failures=0,
            seen_symbols=["A", "B", "C"],
        )
        with pytest.raises(EventIdConflictError):
            repo.save_shadow_coverage(
                campaign_id="c8b", market="SPOT", decision_close_ms=close,
                primary_interval="5m", expected_tradable_count=3,
                tradable_universe_hash="u", campaign_manifest_sha256=manifest,
                mature_count=2, htf_ready_count=2, fresh_bbo_count=2,
                raw_c0_count=2, comparator_rows=2, evidence_failures=0,
                seen_symbols=["A", "B"], sealed_at_ms=close + 1,
                complete=True, failures=[], created_at_ms=close,
            )
    finally:
        repo.close()
