"""Phase-L adversarial tests for segmented zstd raw-tape storage + replay."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from signalbot.domain.enums import Market
from signalbot.prospective.segmented_replay import (
    RawTapeIntegrityError,
    iter_legacy_jsonl,
    iter_segmented_zstd,
)
from signalbot.prospective.segmented_storage import (
    _PARTIAL_SUFFIX as PS,
)
from signalbot.prospective.segmented_storage import (
    ProspectiveSegmentedWriter,
    ProspectiveTapeIntegrityError,
)


def make_writer(tmp_path: Path, **kwargs) -> ProspectiveSegmentedWriter:
    defaults: dict = dict(
        campaign_id="test",
        market=Market.FUTURES,
        source_identity="worktree-source-v1:test",
        rotation_interval_ms=60_000,
    )
    defaults.update(kwargs)
    return ProspectiveSegmentedWriter(tmp_path / "futures", **defaults)


def write_records(writer, base, count, step_ms=1000):
    for i in range(count):
        ts = base + i * step_ms
        writer.append(
            json.dumps(
                {
                    "market": "futures",
                    "received_at_ms": ts,
                    "payload": {"i": i},
                }
            ),
            ts,
        )


# --- A/B/D/E: roundtrip, order, same-ts enqueue order -----------------------


@pytest.mark.asyncio
async def test_abde_v2_roundtrip_order_exact(tmp_path):
    writer = make_writer(tmp_path)
    records = []
    base = 1_710_000_000_000
    # same timestamp repeated preserves enqueue order:
    for i in range(5):
        ts = base
        rec = {
            "market": "futures",
            "received_at_ms": ts,
            "payload": {"round": 0, "i": i},
        }
        writer.append(json.dumps(rec), ts)
        records.append(rec)
    # then advancing timestamps across a bucket boundary:
    for i in range(120):
        ts = base + 61_000 + i * 1_000
        rec = {
            "market": "futures",
            "received_at_ms": ts,
            "payload": {"round": 1, "i": i},
        }
        writer.append(json.dumps(rec), ts)
        records.append(rec)
    writer.close()
    got = list(iter_segmented_zstd(tmp_path / "futures"))
    assert [g["payload"] for g in got] == [r["payload"] for r in records]


# --- F: cross-segment receipt rollback fails ---------------------------------


@pytest.mark.asyncio
async def test_f_receipt_rollback_across_rotation_fails(tmp_path):
    writer = make_writer(tmp_path)
    base = 1_710_000_000_000
    writer.append(json.dumps({"ts": base}), base)
    # force rotation via size by writing into next bucket:
    ts2 = base + 61_000
    writer.append(json.dumps({"ts": ts2}), ts2)
    writer.finalize_active()
    with pytest.raises((ProspectiveTapeIntegrityError, RawTapeIntegrityError)):
        writer.append(json.dumps({"ts": base - 1}), base - 1)


# --- G/H/I/J/K/L/M: corruption family ----------------------------------------


def _make_two_segments(tmp_path):
    writer = make_writer(tmp_path)
    base = 1_710_000_000_000
    writer.append(json.dumps({"i": 0}), base)
    writer.append(json.dumps({"i": 1}), base + 61_000)
    writer.close()


@pytest.mark.asyncio
async def test_g_missing_segment_fails(tmp_path):
    _make_two_segments(tmp_path)
    d = tmp_path / "futures"
    segs = sorted(d.glob("*.jsonl.zst"))
    segs[0].unlink()
    with pytest.raises(RawTapeIntegrityError):
        list(iter_segmented_zstd(d))


@pytest.mark.asyncio
async def test_j_compressed_corruption_fails(tmp_path):
    _make_two_segments(tmp_path)
    d = tmp_path / "futures"
    segs = sorted(d.glob("*.jsonl.zst"))
    data = bytearray(segs[0].read_bytes())
    data[len(data) // 2] ^= 0xFF
    segs[0].write_bytes(bytes(data))
    with pytest.raises(RawTapeIntegrityError):
        list(iter_segmented_zstd(d))


@pytest.mark.asyncio
async def test_k_manifest_corruption_fails(tmp_path):
    _make_two_segments(tmp_path)
    d = tmp_path / "futures"
    m = sorted(d.glob("*.manifest.json"))[0]
    m.write_text("{broken", encoding="utf-8")
    with pytest.raises((RawTapeIntegrityError, json.JSONDecodeError)):
        list(iter_segmented_zstd(d))


@pytest.mark.asyncio
async def test_l_partial_crash_recovery(tmp_path):
    writer = make_writer(tmp_path)
    base = 1_710_000_000_000
    write_records(writer, base, 50)
    # Simulate crash: do NOT finalize; abandon the writer object.
    del writer
    # New writer instance on same dir should recover or fail closed cleanly.
    writer2 = make_writer(tmp_path)
    write_records(writer2, base + 60_000, 10)
    writer2.close()
    # All segments must verify:
    records = list(iter_segmented_zstd(tmp_path / "futures"))
    assert len(records) >= 50


@pytest.mark.asyncio
async def test_m_ambiguous_partials_fail_closed(tmp_path):
    d = tmp_path / "futures"
    d.mkdir(parents=True)
    (d / "1000-00000001.jsonl.zst.partial").write_bytes(b"x")
    (d / "2000-00000002.jsonl.zst.partial").write_bytes(b"y")
    with pytest.raises(ProspectiveTapeIntegrityError):
        make_writer(tmp_path)


@pytest.mark.asyncio
async def test_m2_unreadable_partial_untouched_byte_for_byte(tmp_path):
    d = tmp_path / "futures"
    d.mkdir(parents=True)
    p = d / "1000-00000001.jsonl.zst.partial"
    p.write_bytes(b"\xff\xfe not utf8")
    before_mtime = p.stat().st_mtime_ns
    before_size = p.stat().st_size
    with pytest.raises(ProspectiveTapeIntegrityError):
        make_writer(tmp_path)
    assert p.stat().st_mtime_ns == before_mtime
    assert p.stat().st_size == before_size
    assert list(d.glob("*.evidence-failure")) == []


@pytest.mark.asyncio
async def test_n_stale_partial_after_manifest_rename_cleaned(tmp_path):
    d = tmp_path / "futures"
    d.mkdir(parents=True)
    writer = make_writer(tmp_path)
    base = 1_710_000_000_000
    for i in range(5):
        ts = base + i * 1000
        writer.append(json.dumps(
            {"market": "futures", "received_at_ms": ts, "payload": {"i": i}}
        ), ts)
    writer.close()
    # Simulate crash window (c): manifest+data durable, partial still present.
    # Same sequence as the finalized segment (bucket base, seq 1): the
    # durable pair wins and the stale partial is removed.
    stale = d / (f"{base}-00000001" + PS)
    stale.write_text('{"market":"futures","received_at_ms":1}\n', encoding="utf-8")
    make_writer(tmp_path)  # must recognize stale, delete it
    assert not stale.exists()
    records = list(iter_segmented_zstd(d))
    assert len(records) == 5


@pytest.mark.asyncio
async def test_o_sealed_data_without_manifest_fails_closed(tmp_path):
    d = tmp_path / "futures"
    d.mkdir(parents=True)
    writer = make_writer(tmp_path)
    base = 1_710_000_000_000
    write_records(writer, base, 3)
    writer.close()
    segs = sorted(d.glob("*.manifest.json"))
    segs[0].unlink()  # crash window (b): data present, manifest missing
    with pytest.raises(ProspectiveTapeIntegrityError):
        make_writer(tmp_path)


@pytest.mark.asyncio
async def test_torn_suffix_writes_evidence_failure_marker(tmp_path):
    d = tmp_path / "futures"
    d.mkdir(parents=True)
    p = d / (f"{1_710_000_000_000}-00000001" + PS)
    good = (
        json.dumps(
            {"market": "futures", "received_at_ms": 1, "payload": {}}
        )
        + "\n"
    )
    p.write_text(good + '{"torn"', encoding="utf-8")
    writer = make_writer(tmp_path)  # recovers prefix via canonical path
    writer.close()
    markers = list(d.glob("*.evidence-failure"))
    assert len(markers) == 1
    report = json.loads(markers[0].read_text(encoding="utf-8"))
    assert report["failure"] == "torn_partial_suffix"
    assert report["torn_suffix_bytes"] > 0
    records = list(iter_segmented_zstd(d))
    assert len(records) == 1


# --- Y: legacy tapes readable ------------------------------------------------


@pytest.mark.asyncio
async def test_y_legacy_jsonl_readable(tmp_path):
    d = tmp_path / "spot"
    d.mkdir(parents=True)
    (d / "2026-08-25.jsonl").write_text(
        json.dumps({"market": "spot", "received_at_ms": 1, "payload": {}})
        + "\n",
        encoding="utf-8",
    )
    records = list(iter_legacy_jsonl(d))
    assert len(records) == 1
    assert records[0]["received_at_ms"] == 1


# --- S: graceful close leaves no partial and chain verifies ------------------


@pytest.mark.asyncio
async def test_s_graceful_close_no_partial_chain_verifies(tmp_path):
    writer = make_writer(tmp_path)
    base = 1_710_000_000_000
    write_records(writer, base, 30)
    writer.close()
    d = tmp_path / "futures"
    assert not list(d.glob("*.partial"))
    # chain verifies by successful iteration:
    records = list(iter_segmented_zstd(d))
    assert len(records) == 30
