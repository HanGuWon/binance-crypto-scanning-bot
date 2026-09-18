"""WP4 adversarial tests: segment-set roots, seal-v2, chain verification.

Maps plan doc 040 s1-s3 + round-1 amendments (cross-campaign mount,
mixed v1+v2 chains, non-strict monotonicity boundary, filename<->manifest
binding, canonical encoding).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from signalbot.domain.enums import Market
from signalbot.prospective.segment_set import (
    build_market_segment_root,
    raw_tape_root_sha256,
    smoke_evidence_seal_v2,
)
from signalbot.prospective.segmented_replay import (
    RawTapeIntegrityError,
    iter_segmented_zstd,
)
from signalbot.prospective.segmented_storage import (
    NEW_STORAGE_SCHEMA_VERSION,
    ProspectiveSegmentedWriter,
    ProspectiveTapeIntegrityError,
    ProspectiveTapeRecorder,
)

_BASE = 1_710_000_000_000


def _make_writer(tmp_path: Path, **kwargs) -> ProspectiveSegmentedWriter:
    defaults: dict = dict(
        campaign_id="test",
        market=Market.FUTURES,
        source_identity="worktree-source-v1:test",
        rotation_interval_ms=60_000,
    )
    defaults.update(kwargs)
    return ProspectiveSegmentedWriter(tmp_path / "futures", **defaults)


def _write_records(writer, base, count, step_ms=1000):
    for i in range(count):
        ts = base + i * step_ms
        writer.append(
            json.dumps(
                {"market": "futures", "received_at_ms": ts, "payload": {"i": i}}
            ),
            ts,
        )


def _make_closed_tape(tmp_path: Path, count=10) -> Path:
    writer = _make_writer(tmp_path)
    _write_records(writer, _BASE, count)
    writer.close()
    return tmp_path / "futures"


# --- s1: root determinism ----------------------------------------------------


def test_s1_root_deterministic_and_byte_flip_sensitive(tmp_path: Path) -> None:
    d = _make_closed_tape(tmp_path)
    root_a = build_market_segment_root(d)
    root_b = build_market_segment_root(d)
    assert root_a == root_b
    assert root_a["entries_sha256"] != hashlib.sha256(b"").hexdigest()
    # One flipped byte => different root. Mutate the manifest file (data-file
    # tamper is covered by the s3 cross-check test which expects ValueError).
    manifests = sorted(d.glob("*.manifest.json"))
    manifest_text = manifests[0].read_text(encoding="utf-8")
    manifests[0].write_text(manifest_text + "\n", encoding="utf-8")
    root_c = build_market_segment_root(d)
    assert root_c["entries_sha256"] != root_a["entries_sha256"]


def test_s1_empty_market_dir_contributes_sha256_of_empty(tmp_path: Path) -> None:
    d = tmp_path / "empty-market"
    d.mkdir()
    root = build_market_segment_root(d)
    assert root["entry_count"] == 0
    assert root["entries_sha256"] == hashlib.sha256(b"").hexdigest()


# --- s2: refuse sealing with partials ---------------------------------------


def test_s2_seal_refuses_when_partial_exists(tmp_path: Path) -> None:
    d = _make_closed_tape(tmp_path)
    (d / f"{_BASE}-00000099.jsonl.zst.partial").write_bytes(b"junk")
    with pytest.raises(ValueError, match="refuses sealing"):
        smoke_evidence_seal_v2(d.parent)


def test_s2_seal_succeeds_on_clean_tree_and_binds_root(tmp_path: Path) -> None:
    _make_closed_tape(tmp_path)
    seal = smoke_evidence_seal_v2(tmp_path)
    assert seal["seal_schema_version"] == "smoke_evidence_seal_v2"
    assert (
        seal["raw_tape_root_sha256"] == raw_tape_root_sha256(tmp_path)
    )


# --- s3: binding completeness -----------------------------------------------


def test_s3_every_durable_file_bound_exactly_once_with_matching_sha(
    tmp_path: Path,
) -> None:
    d = _make_closed_tape(tmp_path)
    root = build_market_segment_root(d)
    # Rebuild entries from the canonical function output is not exposed, so
    # recompute expected names and compare counts + shas via a second pass.
    durable_names = sorted(
        [p.name for p in d.glob("*.jsonl.zst")]
        + [p.name for p in d.glob("*.manifest.json")]
    )
    assert root["entry_count"] == len(durable_names) == 2 * len(
        list(d.glob("*.jsonl.zst"))
    )
    root_after = build_market_segment_root(d)
    assert root_after == root  # stable
    # The metadata cross-check inside build_market_segment_root would raise
    # ValueError if any manifest's compressed_file_sha256 disagreed with its
    # data sibling - absence of exception proves the cross-check passed.


def test_s3_metadata_crosscheck_catches_data_tamper(tmp_path: Path) -> None:
    d = _make_closed_tape(tmp_path)
    segs = sorted(d.glob("*.jsonl.zst"))
    data = bytearray(segs[0].read_bytes())
    data[-1] ^= 0xFF
    segs[0].write_bytes(bytes(data))
    with pytest.raises(ValueError, match="compressed hash mismatch"):
        build_market_segment_root(d)


# --- Round-1 amendment: cross-campaign mount rejection ----------------------


def test_amendment_cross_campaign_mount_rejected(tmp_path: Path) -> None:
    _make_closed_tape(tmp_path)
    with pytest.raises(ProspectiveTapeIntegrityError, match="cross-campaign"):
        ProspectiveTapeRecorder(
            tmp_path,
            campaign_id="different-campaign",
            source_identity="worktree-source-v1:test",
        )


def test_startup_refuses_corrupt_existing_chain(tmp_path: Path) -> None:
    d = _make_closed_tape(tmp_path)
    segs = sorted(d.glob("*.jsonl.zst"))
    corrupted = bytearray(segs[0].read_bytes())
    corrupted[len(corrupted) // 2] ^= 0xFF
    segs[0].write_bytes(bytes(corrupted))
    with pytest.raises(ProspectiveTapeIntegrityError):
        ProspectiveTapeRecorder(
            tmp_path,
            campaign_id="test",
            source_identity="worktree-source-v1:test",
        )


def test_stale_tmp_file_is_manual_triage_integrity_error(
    tmp_path: Path,
) -> None:
    _make_closed_tape(tmp_path)
    d = tmp_path / "futures"
    (d / f"{_BASE}-00000001.jsonl.zst.tmp").write_bytes(b"residue")
    with pytest.raises(ProspectiveTapeIntegrityError, match="manual triage"):
        ProspectiveTapeRecorder(
            tmp_path,
            campaign_id="test",
            source_identity="worktree-source-v1:test",
        )


# --- Round-1 amendment: mixed v1+v2 chains are LEGAL ------------------------


def test_mixed_v1_v2_chain_is_legal_and_reader_accepts_both(
    tmp_path: Path,
) -> None:
    d = _make_closed_tape(tmp_path, count=5)
    manifests = sorted(d.glob("*.manifest.json"))
    assert len(manifests) >= 1
    # Downgrade segment 1 label to v1: mixed chain must still verify.
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert manifest["storage_schema_version"] == NEW_STORAGE_SCHEMA_VERSION
    manifest["storage_schema_version"] = "raw_tape_segmented_zstd_v1"
    manifests[0].write_text(json.dumps(manifest), encoding="utf-8")
    records = list(iter_segmented_zstd(d))
    assert len(records) == 5
    # Unknown label rejected.
    manifest["storage_schema_version"] = "raw_tape_segmented_zstd_v9"
    manifests[0].write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RawTapeIntegrityError, match="unknown storage_schema"):
        list(iter_segmented_zstd(d))


# --- Round-1 amendment: non-strict receipt monotonicity ---------------------


def test_equal_receipt_timestamps_legal_within_and_across_segments(
    tmp_path: Path,
) -> None:
    # Tiny uncompressed cap forces size-based rotation between records.
    writer = _make_writer(tmp_path, maximum_uncompressed_bytes=1)
    ts = _BASE
    payload_line = json.dumps(
        {"market": "futures", "received_at_ms": ts, "payload": {"i": 0}}
    )
    writer.append(payload_line, ts)
    # Same timestamp again -> lands in a NEW segment (size rotation); the
    # non-strict contract (ts >= last) must accept it across segments.
    writer.append(payload_line, ts)
    writer.close()
    d = tmp_path / "futures"
    assert len(list(d.glob("*.jsonl.zst"))) >= 2
    records = list(iter_segmented_zstd(d))
    assert [r["payload"] for r in records] == [{"i": 0}, {"i": 0}]


# --- Round-1 amendment: filename <-> manifest binding -----------------------


def test_filename_manifest_binding_mismatch_rejected(tmp_path: Path) -> None:
    d = _make_closed_tape(tmp_path)
    manifests = sorted(d.glob("*.manifest.json"))
    victim = manifests[0]
    renamed = victim.with_name(f"999-{victim.name.split('-')[-1]}")
    victim.rename(renamed)
    with pytest.raises(RawTapeIntegrityError, match="binding mismatch"):
        list(iter_segmented_zstd(d))
