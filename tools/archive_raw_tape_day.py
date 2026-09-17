"""Lossless daily raw-tape archiver with byte-exact roundtrip verification.

Operates ONLY on CLOSED UTC-day raw JSONL files (never today's active file).

Protocol:
  A. ORIGINAL SEAL: record path, bytes, SHA256.
  B. LOSSLESS COMPRESSION: gzip level 9 to archives directory.
  C. ROUNDTRIP VERIFICATION: decompress archive, require restored
     SHA256 == original SHA256 and restored bytes == original bytes.
  D. ARCHIVE MANIFEST: atomically persist manifest with all evidence.
  E. RETIRE ORIGINAL: delete original only after all prior phases succeed.
     The verified compressed archive becomes the sole evidence authority.

Fails closed on any error; preserves the original if any phase fails.
Idempotent: refuses re-archive of an already archived day.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path


def sha256_path(path: Path) -> str:
    """Compute SHA256 of a file's full content."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sha256_gzip_content(path: Path) -> tuple[str, int]:
    """SHA256 and byte count of DECOMPRESSED content of a gzip file."""
    digest = hashlib.sha256()
    total = 0
    with gzip.open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
            total += len(block)
    return digest.hexdigest(), total


def _write_manifest_atomic(manifest_path: Path, manifest: dict) -> None:
    """Write manifest to a temp file then atomically rename."""
    fd, tmp_name = tempfile.mkstemp(dir=manifest_path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp:
            json.dump(manifest, tmp, indent=2, sort_keys=True)
            tmp.write("\n")
        os.replace(tmp_name, str(manifest_path))
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def archive_day(
    tape_root: Path,
    market: str,
    day: str,
    archives_dir: Path,
    *,
    retire_original: bool = True,
) -> dict:
    """Archive one closed-day raw tape file. Returns the manifest dict."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    if day >= today:
        msg = f"refusing non-closed day {day} (today={today})"
        raise ValueError(msg)

    source = tape_root / market / f"{day}.jsonl"
    if not source.is_file():
        msg = f"missing source file: {source}"
        raise FileNotFoundError(msg)

    target_dir = archives_dir / market
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{day}.jsonl.gz"

    if target.exists():
        msg = f"archive already exists: {target}"
        raise FileExistsError(msg)

    # PHASE A - ORIGINAL SEAL
    original_sha = sha256_path(source)
    original_bytes = source.stat().st_size
    row_count = _count_lines(source)

    # PHASE B - LOSSLESS COMPRESSION
    with source.open("rb") as src, gzip.open(target, "wb", compresslevel=9) as dst:
        for block in iter(lambda: src.read(1024 * 1024), b""):
            dst.write(block)
    compressed_bytes = target.stat().st_size

    # PHASE C - ROUNDTRIP VERIFICATION (decompress + hash + byte count)
    try:
        restored_sha, restored_bytes = _sha256_gzip_content(target)
    except Exception:
        target.unlink(missing_ok=True)
        raise
    verified = restored_sha == original_sha and restored_bytes == original_bytes
    if not verified:
        target.unlink(missing_ok=True)
        msg = (
            f"roundtrip verification FAILED for {source}: "
            f"restored_sha={restored_sha} original_sha={original_sha} "
            f"restored_bytes={restored_bytes} original_bytes={original_bytes}"
        )
        raise RuntimeError(msg)

    first_receipt_ms, last_receipt_ms = _extract_receipt_range(source)

    manifest: dict = {
        "archive_schema_version": "raw_tape_archive_v1",
        "market": market,
        "utc_day": day,
        "source_relative_path": f"{market}/{day}.jsonl",
        "archive_relative_path": f"archives/{market}/{day}.jsonl.gz",
        "original_sha256": original_sha,
        "original_bytes": original_bytes,
        "compressed_bytes": compressed_bytes,
        "gzip_ratio": round(compressed_bytes / max(original_bytes, 1), 6),
        "row_count": row_count,
        "first_receipt_at_ms": first_receipt_ms,
        "last_receipt_at_ms": last_receipt_ms,
        "restored_sha256": restored_sha,
        "restored_bytes": restored_bytes,
        "roundtrip_verified": True,
        "retired_original": False,
        "archived_at_ms": int(datetime.now(UTC).timestamp() * 1000),
    }

    # PHASE D - ARCHIVE MANIFEST
    manifest_path = target_dir / f"{day}.manifest.json"
    try:
        _write_manifest_atomic(manifest_path, manifest)
    except BaseException:
        target.unlink(missing_ok=True)
        raise

    reread = json.loads(manifest_path.read_text(encoding="utf-8"))
    if reread["roundtrip_verified"] is not True or reread["original_sha256"] != original_sha:
        target.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)
        msg = "manifest re-read verification failed"
        raise RuntimeError(msg)

    # PHASE E - RETIRE ORIGINAL (only after all prior phases succeed)
    if retire_original:
        source.unlink()
        manifest["retired_original"] = True
        _write_manifest_atomic(manifest_path, manifest)

    return manifest


def _count_lines(path: Path) -> int:
    count = 0
    with path.open("rb") as f:
        for _ in f:
            count += 1
    return count


def _extract_receipt_range(path: Path) -> tuple[int | None, int | None]:
    """Read first and last received_at_ms from a JSONL."""
    first: int | None = None
    last: int | None = None
    with path.open("r", encoding="utf-8") as f:
        line = f.readline()
        if line.strip():
            obj = json.loads(line)
            first = obj.get("received_at_ms")
    with path.open("rb") as f:
        f.seek(0, 2)
        size = f.tell()
        pos = size - 1
        while pos >= 0 and pos < size:
            f.seek(pos)
            ch = f.read(1)
            if ch == b"\n" and pos < size - 1:
                f.seek(pos + 1)
                line = f.readline().decode("utf-8")
                if line.strip():
                    obj = json.loads(line)
                    last = obj.get("received_at_ms")
                break
            pos -= 1
    return first, last


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tape-root", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--day", required=True, help="UTC day as YYYY-MM-DD")
    parser.add_argument("--archives-dir", required=True)
    parser.add_argument("--no-retire", action="store_true", help="Do not delete the original")
    args = parser.parse_args(argv)

    try:
        manifest = archive_day(
            tape_root=Path(args.tape_root),
            market=args.market,
            day=args.day,
            archives_dir=Path(args.archives_dir),
            retire_original=not args.no_retire,
        )
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


if __name__ == "__main__":
    raise SystemExit(main())
