"""Tests for tools/prospective_operational_checkpoint.py."""

from __future__ import annotations

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


def test_checkpoint_deterministic_schema(campaign_db, tmp_path):
    from tools.prospective_operational_checkpoint import build_checkpoint

    cp = build_checkpoint(
        db_path=campaign_db,
        campaign_id="cp-test",
        tape_root=tmp_path,
        activation_ms=1787734450558,
    )
    assert cp["checkpoint_schema_version"] == "prospective_operational_checkpoint_v1"
    assert cp["activation_ms"] == 1787734450558
    assert cp["activation_utc"] == "2026-08-26T08:54:10.558000+00:00"
    assert "+09:00" in cp["activation_kst"]
    assert isinstance(cp["elapsed_since_activation_hours"], float)
    assert cp["coverage"]["futures"]["open"] == 1
    assert cp["coverage"]["spot"]["incomplete"] == 1


def test_kst_conversion_exact(campaign_db, tmp_path):
    from tools.prospective_operational_checkpoint import _iso_kst, _iso_utc

    ms = 1787734450558
    utc = _iso_utc(ms)
    kst = _iso_kst(ms)
    assert utc == "2026-08-26T08:54:10.558000+00:00"
    assert kst == "2026-08-26T17:54:10.558000+09:00"


def test_empty_tape_root(campaign_db, tmp_path):
    from tools.prospective_operational_checkpoint import build_checkpoint

    empty = tmp_path / "empty-tape"
    empty.mkdir()
    cp = build_checkpoint(
        db_path=campaign_db,
        campaign_id="cp-test",
        tape_root=empty,
        activation_ms=1787734450558,
    )
    assert cp["raw_tape"]["futures"]["sealed_segment_count"] == 0
    assert cp["raw_tape"]["futures"]["latest_last_received_at_ms"] is None
