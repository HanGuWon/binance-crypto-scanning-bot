"""Tests for tools/prospective_t24h_receipt.py (read-only composer)."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = str(Path(__file__).resolve().parents[2])
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


@pytest.fixture()
def campaign_db(tmp_path):
    db = tmp_path / "campaign.db"
    conn = sqlite3.connect(db)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE shadow_coverage ("
        " market TEXT NOT NULL, decision_close_ms INTEGER NOT NULL,"
        " status TEXT NOT NULL, complete INTEGER DEFAULT 0,"
        " evidence_failures INTEGER DEFAULT 0)"
    )
    cur.executemany(
        "INSERT INTO shadow_coverage VALUES (?,?,?,?,?)",
        [
            ("futures", 1000, "SEALED", 1, 0),
            ("futures", 301000, "OPEN", 0, 0),
            ("spot", 1000, "SEALED", 1, 0),
            ("spot", 301000, "INCOMPLETE", 0, 0),
        ],
    )
    conn.commit()
    conn.close()
    return db


@pytest.fixture()
def tape_root(tmp_path):
    root = tmp_path / "tape"
    for market in ("futures", "spot"):
        mdir = root / market
        mdir.mkdir(parents=True)
        # Two sealed segments with a manifest per market, past the recovery boundary.
        for seq, start in ((1, 1787738846103), (2, 1787740646103)):
            zst = mdir / f"1787738846103-{seq:08d}.jsonl.zst"
            zst.write_bytes(b"\x00" * 1000)
            man = {
                "segment_sequence": seq,
                "segment_start_ms": start,
                "compressed_file_name": zst.name,
                "last_received_at_ms": start + 300000,
            }
            (mdir / (zst.name + ".manifest.json")).write_text(json.dumps(man), encoding="utf-8")
    return root


@pytest.fixture()
def prereg(tmp_path):
    p = tmp_path / "preregistration.json"
    p.write_text(
        json.dumps(
            {
                "campaign_id": "t24h-test",
                "campaign_created_at_ms": 1787732650558,
                "activation_ms": 1787734450558,
            }
        ),
        encoding="utf-8",
    )
    return p


def test_t24h_receipt_complete_schema(campaign_db, tape_root, prereg, tmp_path):
    from tools.prospective_t24h_receipt import compose_t24h_receipt

    receipt = compose_t24h_receipt(
        db_path=campaign_db,
        campaign_id="t24h-test",
        tape_root=tape_root,
        prereg_path=prereg,
        activation_ms=1787734450558,
        quota_bytes=10737418240,
        gate_hours=48.0,
        transient_reserve_gib=0.02,
        safety_margin_gib=0.5,
        decision="AUTO",
    )
    assert receipt["receipt_schema_version"] == "phase_n_t24h_operational_receipt_v1"
    assert receipt["campaign_id"] == "t24h-test"
    assert receipt["checkpoint_schema_version"] == "prospective_operational_checkpoint_v1"
    assert "source_identity" in receipt
    assert "source_drift" in receipt["source_identity"]
    assert receipt["preregistration"]["activation_ms"] == 1787734450558
    assert "physical_storage" in receipt
    assert "p95_conservative_rate_gib_per_h" in receipt["physical_storage"]
    assert "rollover_gate" in receipt
    assert "retest_denominator" in receipt
    assert "segment_chain" in receipt
    assert "log_health" in receipt
    assert "segment_chain" in receipt
    assert receipt["rollover_gate"]["formula"] is not None
    assert receipt["rollover_gate"]["decision"] in ("CONTINUE_PHASE_M", "PLANNED_PHASE_N_ROLLOVER")


def test_t24h_receipt_readonly(campaign_db, tape_root, prereg, tmp_path):
    from tools.prospective_t24h_receipt import compose_t24h_receipt

    before = {p.name: p.stat().st_size for p in tape_root.rglob("*") if p.is_file()}
    compose_t24h_receipt(
        db_path=campaign_db,
        campaign_id="t24h-test",
        tape_root=tape_root,
        prereg_path=prereg,
        activation_ms=1787734450558,
        quota_bytes=10737418240,
        gate_hours=48.0,
        transient_reserve_gib=0.02,
        safety_margin_gib=0.5,
        decision="AUTO",
    )
    after = {p.name: p.stat().st_size for p in tape_root.rglob("*") if p.is_file()}
    assert before == after  # composer never mutates tape


def test_t24h_receipt_gate_decision(campaign_db, tape_root, prereg, tmp_path):
    from tools.prospective_t24h_receipt import compose_t24h_receipt

    receipt = compose_t24h_receipt(
        db_path=campaign_db,
        campaign_id="t24h-test",
        tape_root=tape_root,
        prereg_path=prereg,
        activation_ms=1787734450558,
        quota_bytes=1024,  # tiny quota -> gate must fail -> rollover
        gate_hours=48.0,
        transient_reserve_gib=0.02,
        safety_margin_gib=0.5,
        decision="AUTO",
    )
    assert receipt["rollover_gate"]["gate_met"] is False
    assert receipt["rollover_gate"]["decision"] == "PLANNED_PHASE_N_ROLLOVER"


def test_t24h_receipt_source_drift_flag(campaign_db, tape_root, prereg, tmp_path):
    from tools.prospective_t24h_receipt import compose_t24h_receipt

    receipt = compose_t24h_receipt(
        db_path=campaign_db,
        campaign_id="t24h-test",
        tape_root=tape_root,
        prereg_path=prereg,
        activation_ms=1787734450558,
        quota_bytes=10737418240,
        gate_hours=48.0,
        transient_reserve_gib=0.02,
        safety_margin_gib=0.5,
        decision="AUTO",
    )
    assert (
        receipt["source_identity"]["configured"]
        == "worktree-source-v1:650fe603f79812e79cd9390c0d2e94fbab8071a4eab1cc95f40fc229c7f403dd"
    )


def test_t24h_receipt_log_health(campaign_db, tape_root, prereg, tmp_path):
    from tools.prospective_t24h_receipt import compose_t24h_receipt

    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_file = log_dir / "run.log"
    lines = [
        '{"timestamp":"2026-08-26T08:54:11.000+00:00","level":"INFO","logger":"httpx","message":"ok"}',
        '{"timestamp":"2026-08-26T08:54:12.000+00:00","level":"ERROR","logger":"app","message":"boom"}',
        '{"timestamp":"2026-08-26T08:54:13.000+00:00","level":"CRITICAL","logger":"app","message":"fatal"}',
        '{"timestamp":"2026-08-26T08:54:14.000+00:00","level":"WARNING","logger":"app","message":"warn"}',
        "not-json-line",
    ]
    log_file.write_text(chr(10).join(lines) + chr(10), encoding="utf-8")

    receipt = compose_t24h_receipt(
        db_path=campaign_db,
        campaign_id="t24h-test",
        tape_root=tape_root,
        prereg_path=prereg,
        activation_ms=1787734450558,
        quota_bytes=10737418240,
        gate_hours=48.0,
        transient_reserve_gib=0.02,
        safety_margin_gib=0.5,
        decision="AUTO",
        log_path=log_file,
    )
    logh = receipt["log_health"]
    assert logh["available"] is True
    assert logh["total_lines"] == 5
    assert logh["error_count"] == 1
    assert logh["critical_count"] == 1
    assert logh["fatal_count"] == 0


def test_t24h_receipt_log_health_missing(campaign_db, tape_root, prereg, tmp_path):
    from tools.prospective_t24h_receipt import compose_t24h_receipt

    missing = tmp_path / "logs" / "run.log"
    receipt = compose_t24h_receipt(
        db_path=campaign_db,
        campaign_id="t24h-test",
        tape_root=tape_root,
        prereg_path=prereg,
        activation_ms=1787734450558,
        quota_bytes=10737418240,
        gate_hours=48.0,
        transient_reserve_gib=0.02,
        safety_margin_gib=0.5,
        decision="AUTO",
        log_path=missing,
    )
    logh = receipt["log_health"]
    assert logh["available"] is False
    assert logh["total_lines"] is None
    assert logh["error_count"] is None
