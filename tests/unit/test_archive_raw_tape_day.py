"""Tests for the lossless daily raw-tape archive owner."""

from __future__ import annotations

import gzip
import hashlib
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = str(Path(__file__).resolve().parents[2])
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools.archive_raw_tape_day import (  # noqa: E402
    _count_lines,
    _extract_receipt_range,
    _sha256_gzip_content,
    _write_manifest_atomic,
    archive_day,
    sha256_path,
)


def _make_jsonl(path: Path, lines: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(json.dumps(obj) for obj in lines) + "\n"
    path.write_text(content, encoding="utf-8")


def _sample_lines(n: int = 5) -> list[dict]:
    return [
        {"market": "spot", "received_at_ms": 1787573636866 + i * 1000, "payload": {"seq": i}}
        for i in range(n)
    ]


class TestArchiveDay:
    def test_roundtrip_exact(self, tmp_path: Path) -> None:
        tape_root = tmp_path / "tape"
        archives_dir = tmp_path / "archives"
        day = "2026-08-23"
        source = tape_root / "futures" / f"{day}.jsonl"
        lines = _sample_lines(20)
        _make_jsonl(source, lines)

        original_sha = sha256_path(source)
        original_bytes = source.stat().st_size
        manifest = archive_day(tape_root, "futures", day, archives_dir)

        assert manifest["roundtrip_verified"] is True
        assert manifest["original_sha256"] == original_sha
        assert manifest["original_bytes"] == original_bytes
        assert manifest["restored_sha256"] == original_sha
        assert manifest["restored_bytes"] == original_bytes
        assert manifest["retired_original"] is True
        assert not source.exists()

        # Verify archive file exists and decompresses to same content
        target = archives_dir / "futures" / f"{day}.jsonl.gz"
        assert target.exists()
        restored_content = gzip.decompress(target.read_bytes())
        assert hashlib.sha256(restored_content).hexdigest() == original_sha

    def test_no_retire_preserves_original(self, tmp_path: Path) -> None:
        tape_root = tmp_path / "tape"
        archives_dir = tmp_path / "archives"
        day = "2026-08-22"
        source = tape_root / "spot" / f"{day}.jsonl"
        _make_jsonl(source, _sample_lines(3))

        manifest = archive_day(tape_root, "spot", day, archives_dir, retire_original=False)
        assert manifest["retired_original"] is False
        assert source.exists()

    def test_refuses_today(self, tmp_path: Path) -> None:
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        with pytest.raises(ValueError, match="non-closed day"):
            archive_day(tmp_path / "t", "futures", today, tmp_path / "a")

    def test_refuses_future(self, tmp_path: Path) -> None:
        future = (datetime.now(UTC) + timedelta(days=1)).strftime("%Y-%m-%d")
        with pytest.raises(ValueError):
            archive_day(tmp_path / "t", "futures", future, tmp_path / "a")

    def test_missing_source(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            archive_day(tmp_path / "t", "futures", "2020-01-01", tmp_path / "a")

    def test_idempotent_refusal_on_existing_archive(self, tmp_path: Path) -> None:
        day = "2025-06-15"
        source = tmp_path / "tape" / "futures" / f"{day}.jsonl"
        _make_jsonl(source, _sample_lines(2))
        archives_dir = tmp_path / "archives"
        archive_day(tmp_path / "tape", "futures", day, archives_dir)

        # Recreate the source and try again - should refuse because archive exists
        _make_jsonl(source, _sample_lines(2))
        with pytest.raises(FileExistsError, match="already exists"):
            archive_day(tmp_path / "tape", "futures", day, archives_dir)

    def test_crash_during_compression_cleans_up(self, tmp_path: Path) -> None:
        """Simulate a failure during compression; original preserved."""
        day = "2024-01-01"
        source = tmp_path / "tape" / "futures" / f"{day}.jsonl"
        lines = _sample_lines(10)
        _make_jsonl(source, lines)
        archives_dir = tmp_path / "archives"

        # Monkey-patch gzip.open to fail on write
        import tools.archive_raw_tape_day as mod
        original_gzip_open = mod.gzip.open

        def fake_gzip_open(*args, **kwargs):
            if len(args) > 1 and "wb" in str(args[1]):
                raise OSError("simulated disk full")
            return original_gzip_open(*args, **kwargs)

        try:
            mod.gzip.open = fake_gzip_open
            with pytest.raises(OSError):
                archive_day(tmp_path / "tape", "futures", day, archives_dir)
        finally:
            mod.gzip.open = original_gzip_open

        # Original must be preserved
        assert source.exists()
        original_content = source.read_bytes()
        assert hashlib.sha256(original_content).hexdigest() == sha256_path(source)

    def test_corrupt_gzip_detected_and_cleaned(self, tmp_path: Path) -> None:
        """If decompression fails after compression, clean up and preserve original."""
        day = "2023-07-10"
        source = tmp_path / "tape" / "spot" / f"{day}.jsonl"
        _make_jsonl(source, _sample_lines(2))
        archives_dir = tmp_path / "archives"

        import tools.archive_raw_tape_day as mod
        orig_func = mod._sha256_gzip_content
        mod._sha256_gzip_content = lambda p: (_ for _ in ()).throw(RuntimeError("corrupt"))
        try:
            with pytest.raises(RuntimeError):
                archive_day(tmp_path / "tape", "spot", day, archives_dir)
        finally:
            mod._sha256_gzip_content = orig_func

        assert source.exists()
        assert not (archives_dir / "spot" / f"{day}.jsonl.gz").exists()


class TestHelpers:
    def test_count_lines(self, tmp_path: Path) -> None:
        p = tmp_path / "test.jsonl"
        p.write_text("line1\nline2\nline3\n")
        assert _count_lines(p) == 3

    def test_extract_receipt_range(self, tmp_path: Path) -> None:
        lines = [
            {"market": "futures", "received_at_ms": 1000},
            {"market": "futures", "received_at_ms": 2000},
            {"market": "futures", "received_at_ms": 3000},
        ]
        p = tmp_path / "test.jsonl"
        p.write_text("\n".join(json.dumps(o) for o in lines))
        first, last = _extract_receipt_range(p)
        assert first == 1000
        assert last == 3000

    def test_write_manifest_atomic(self, tmp_path: Path) -> None:
        mp = tmp_path / "manifest.json"
        data = {"key": "value"}
        _write_manifest_atomic(mp, data)
        assert json.loads(mp.read_text()) == data

    def test_sha256_gzip_content_roundtrip(self, tmp_path: Path) -> None:
        content = b"hello world " * 1000
        gz = tmp_path / "test.gz"
        gz.write_bytes(gzip.compress(content))
        sha, n_bytes = _sha256_gzip_content(gz)
        assert sha == hashlib.sha256(content).hexdigest()
        assert n_bytes == len(content)
