"""Tests for tools/prospective_health_report_v2.py (read-only, no network)."""

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
        " market TEXT NOT NULL,"
        " decision_close_ms INTEGER NOT NULL,"
        " status TEXT NOT NULL,"
        " complete INTEGER NOT NULL DEFAULT 0,"
        " evidence_failures INTEGER NOT NULL DEFAULT 0)"
    )
    # 3 consecutive 5m cells for futures + a gap then 1 more; 1 spot cell
    cells = [
        ("futures", 1000, "SEALED", 1, 0),
        ("futures", 301000, "SEALED", 1, 0),
        ("futures", 601000, "SEALED", 1, 0),
        ("futures", 1201000, "SEALED", 1, 0),  # gap (10 min skip)
        ("spot", 1000, "SEALED", 1, 0),
    ]
    cur.executemany("INSERT INTO shadow_coverage VALUES (?,?,?,?,?)", cells)
    conn.commit()
    conn.close()
    return db


@pytest.fixture()
def tape_root(tmp_path):
    root = tmp_path / "raw-events"
    for mkt in ("futures", "spot"):
        d = root / mkt
        d.mkdir(parents=True)
    return root


def _write_manifest(mdir, seq, first_ms, last_ms, compressed_name="s.jsonl.zst"):
    mf = {
        "schema_version": "prospective_segment_manifest_v1",
        "segment_sequence": seq,
        "first_received_at_ms": first_ms,
        "last_received_at_ms": last_ms,
        "record_count": 10,
        "compressed_file_sha256": "a" * 64,
    }
    (mdir / f"{seq}.manifest.json").write_text(json.dumps(mf), encoding="utf-8")
    (mdir / compressed_name).touch()


def test_report_schema_and_coverage_gaps(campaign_db, tape_root):
    from tools.prospective_health_report_v2 import build_report

    report = build_report(
        db_path=campaign_db,
        campaign_id="test-campaign",
        tape_root=tape_root,
        quota_bytes=1024 * 1024,
        reserve_bytes=1024,
    )
    assert report["report_schema_version"] == "prospective_health_report_v2"
    assert report["database_integrity"] == "ok"
    fut = report["coverage"]["futures"]
    assert fut["total_cells"] == 4
    assert fut["sealed"] == 4
    assert fut["coverage_gap_count"] == 1  # one non-300k jump

def test_tape_gap_detection(campaign_db, tape_root):
    from tools.prospective_health_report_v2 import _read_manifests, build_report

    fdir = tape_root / "futures"
    _write_manifest(fdir, 1, 1000, 5000)
    _write_manifest(fdir, 2, 20000, 25000)  # 15s sub-second boundary (not material)
    _write_manifest(fdir, 3, 105000, 110000)  # 80s gap after seg2 (material)
    manifests = _read_manifests(fdir)
    assert len(manifests) == 3
    report = build_report(
        db_path=campaign_db,
        campaign_id="t",
        tape_root=tape_root,
        quota_bytes=1024 * 1024,
        reserve_bytes=1024,
    )
    fut = report["raw_writer"]["futures"]
    assert fut["sealed_segments"] == 3
    assert fut["gap_count"] == 1
    assert fut["gaps"][0]["gap_ms"] == 80000


def test_sub_second_boundary_not_material_gap(campaign_db, tape_root):
    """Sub-second inter-segment transitions must NOT count as gaps."""
    from tools.prospective_health_report_v2 import build_report

    fdir = tape_root / "futures"
    _write_manifest(fdir, 1, 1000, 5000)
    _write_manifest(fdir, 2, 5020, 8000)  # 15ms transition: normal boundary
    report = build_report(
        db_path=campaign_db,
        campaign_id="t",
        tape_root=tape_root,
        quota_bytes=1024 * 1024,
        reserve_bytes=1024,
    )
    fut = report["raw_writer"]["futures"]
    assert fut["sealed_segments"] == 2
    assert fut["gap_count"] == 0  # 20ms transition is not material

def test_boundary_null_recovered_segment(campaign_db, tape_root):
    """Recovered segments have null boundaries; report must flag them."""
    from tools.prospective_health_report_v2 import build_report

    fdir = tape_root / "futures"
    _write_manifest(fdir, 1, 1000, 5000)
    _write_manifest(fdir, 2, None, None)  # recovered: nulls
    report = build_report(
        db_path=campaign_db,
        campaign_id="t",
        tape_root=tape_root,
        quota_bytes=1024 * 1024,
        reserve_bytes=1024,
    )
    fut = report["raw_writer"]["futures"]
    assert fut["boundary_null_segments"] == 1
    assert fut["gap_count"] == 1  # flagged as boundary_unknown
    assert fut["gaps"][0]["note"] == "boundary_unknown_recovered_segment"

def test_active_partial_accounting(campaign_db, tape_root):
    from tools.prospective_health_report_v2 import build_report

    fdir = tape_root / "futures"
    _write_manifest(fdir, 1, 1000, 5000)
    partial = fdir / "9999.jsonl.zst.partial"
    partial.write_bytes(b"x" * 1234)
    report = build_report(
        db_path=campaign_db,
        campaign_id="t",
        tape_root=tape_root,
        quota_bytes=1024 * 1024,
        reserve_bytes=1024,
    )
    fut = report["raw_writer"]["futures"]
    assert fut["active_partials"] == 1
    assert fut["active_partial_bytes"] == 1234

def test_read_only_no_mutation(campaign_db, tape_root):
    """Tool must not create/modify any file in the campaign tree."""
    from tools.prospective_health_report_v2 import build_report

    before = sorted(p.name for p in tape_root.rglob("*"))
    build_report(
        db_path=campaign_db,
        campaign_id="t",
        tape_root=tape_root,
        quota_bytes=1024 * 1024,
        reserve_bytes=1024,
    )
    after = sorted(p.name for p in tape_root.rglob("*"))
    assert before == after
