"""Canonical market segment-set roots + smoke evidence seal v2 (WP4).

build_market_segment_root(market_dir) produces a deterministic canonical
encoding over sorted data+manifest files; smoke_evidence_seal_v2 binds every
file of every market under a raw_tape_root_sha256 and refuses to seal when
any partial segment exists.

Canonical JSON lines are pinned: sort_keys=True, compact separators,
UTF-8, trailing newline. An empty market dir contributes sha256(b"") to the
root (fixed choice, tested).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

_DATA_SUFFIX = ".jsonl.zst"
_MANIFEST_SUFFIX = ".manifest.json"
_PARTIAL_SUFFIX = ".jsonl.zst.partial"
_LEGACY_JSONL_SUFFIX = ".jsonl"
_STREAM_READ_SIZE = 64 * 1024

_EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def _canonical_line(value: dict[str, Any]) -> str:
    return (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        + "\n"
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(_STREAM_READ_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def build_market_segment_root(market_directory: Path) -> dict[str, Any]:
    """Canonical root binding for one market directory.

    Deterministic: identical tree => identical root; any flipped byte =>
    different root. Every data + manifest file appears exactly once with its
    on-disk SHA; legacy *.jsonl files are also supported for old campaigns.
    Empty directory contributes sha256 of empty bytes as the root.
    """
    directory = Path(market_directory)
    entries: list[dict[str, Any]] = []
    paths: list[Path] = []
    if directory.is_dir():
        paths = sorted(
            list(directory.glob(f"*{_DATA_SUFFIX}"))
            + list(directory.glob(f"*{_MANIFEST_SUFFIX}"))
            + list(directory.glob(f"*{_LEGACY_JSONL_SUFFIX}")),
            key=lambda p: p.name,
        )
    if not paths:
        return {
            "market": directory.name if directory.name else "",
            "entry_count": 0,
            "entries_sha256": _EMPTY_SHA256,
        }
    canonical_lines: list[str] = []
    for path in paths:
        entry: dict[str, Any] = {
            "filename": path.name,
            "sha256": _sha256_file(path),
        }
        manifest_candidate = None
        if path.name.endswith(_MANIFEST_SUFFIX):
            try:
                manifest_candidate = json.loads(
                    path.read_text(encoding="utf-8")
                )
            except (json.JSONDecodeError, UnicodeDecodeError):
                manifest_candidate = None
        if isinstance(manifest_candidate, dict):
            entry["sequence"] = manifest_candidate.get("segment_sequence")
            entry["first_received_at_ms"] = manifest_candidate.get(
                "first_received_at_ms"
            )
            entry["last_received_at_ms"] = manifest_candidate.get(
                "last_received_at_ms"
            )
            entry["record_count"] = manifest_candidate.get("record_count")
            entry["bytes"] = manifest_candidate.get("compressed_bytes")
            # Cross-check embedded metadata vs the on-disk data sibling.
            data_name = path.name.removesuffix(_MANIFEST_SUFFIX) + _DATA_SUFFIX
            sibling = path.parent / data_name
            if sibling.exists():
                actual_data_sha = _sha256_file(sibling)
                declared = manifest_candidate.get("compressed_file_sha256")
                if declared is not None and declared != actual_data_sha:
                    raise ValueError(
                        f"segment_set: compressed hash mismatch for {data_name}"
                    )
        else:
            stat = path.stat()
            entry["bytes"] = stat.st_size
        entries.append(entry)
        canonical_lines.append(_canonical_line(entry))
    joined = "".join(canonical_lines).encode("utf-8")
    return {
        "market": directory.name,
        "entry_count": len(entries),
        "entries_sha256": hashlib.sha256(joined).hexdigest(),
    }


def raw_tape_root_sha256(raw_event_directory: Path) -> str:
    """SHA-256 over sorted per-market roots (deterministic)."""
    directory = Path(raw_event_directory)
    market_roots: list[dict[str, Any]] = []
    if directory.is_dir():
        for market_dir in sorted(p for p in directory.iterdir() if p.is_dir()):
            market_roots.append(build_market_segment_root(market_dir))
    canonical_lines = [
        _canonical_line(root)
        for root in sorted(market_roots, key=lambda r: r["market"])
    ]
    joined = "".join(canonical_lines).encode("utf-8")
    return hashlib.sha256(joined).hexdigest()


def smoke_evidence_seal_v2(raw_event_directory: Path) -> dict[str, Any]:
    """Seal v2: refuse when partials exist; bind every durable file."""
    directory = Path(raw_event_directory)
    partials: list[str] = []
    if directory.is_dir():
        partials = sorted(
            p.name for p in directory.rglob(f"*{_PARTIAL_SUFFIX}")
        )
    if partials:
        raise ValueError(
            f"smoke_evidence_seal_v2 refuses sealing: active partials "
            f"present: {partials}"
        )
    markets: list[dict[str, Any]] = []
    if directory.is_dir():
        for market_dir in sorted(p for p in directory.iterdir() if p.is_dir()):
            markets.append(build_market_segment_root(market_dir))
    return {
        "seal_schema_version": "smoke_evidence_seal_v2",
        "raw_tape_root_sha256": raw_tape_root_sha256(directory),
        "markets": sorted(markets, key=lambda m: m["market"]),
    }
