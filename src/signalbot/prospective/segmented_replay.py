"""Streaming raw-tape reader + replay support (Phase-L / WP4).

Unified read-only iterator over:
- legacy_jsonl_v1: daily *.jsonl files (line-by-line, bounded memory)
- segmented_zstd_v1/v2: *.jsonl.zst segments with manifest + streaming hash
  validation (64 KiB chunks; no whole-file reads of data paths)

Replay advances ReplayClock using recorded received_at_ms. Never sorts;
never loads whole files into memory.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import zstandard as zstd

LOGGER = logging.getLogger(__name__)

_STREAM_READ_SIZE = 64 * 1024
_ACCEPTED_STORAGE_SCHEMA_VERSIONS = {
    "raw_tape_segmented_zstd_v1",
    "raw_tape_segmented_zstd_v2",
}
_MANIFEST_SUFFIX = ".manifest.json"
_PARTIAL_SUFFIX = ".jsonl.zst.partial"


class RawTapeIntegrityError(RuntimeError):
    """Raised when segment chain/hash/order validation fails."""


def iter_legacy_jsonl(directory: Path) -> Iterator[dict[str, Any]]:
    """Stream legacy daily JSONL files line-by-line."""
    for file in sorted(directory.glob("*.jsonl")):
        with file.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def _segment_data_path(directory: Path, manifest: dict[str, Any]) -> Path:
    bucket = manifest["bucket_start_ms"]
    sequence = manifest["segment_sequence"]
    path = directory / f"{bucket}-{sequence:08d}.jsonl.zst"
    if not path.exists():
        raise RawTapeIntegrityError(
            f"missing data file for segment {sequence}"
        )
    return path


def _load_and_validate_manifests(directory: Path) -> list[dict[str, Any]]:
    manifests: list[dict[str, Any]] = []
    for manifest_path in sorted(
        directory.glob(f"*{_MANIFEST_SUFFIX}"),
        key=lambda p: int(p.name.split("-")[-1].removesuffix(_MANIFEST_SUFFIX)),
    ):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise RawTapeIntegrityError(
                f"unreadable manifest: {manifest_path.name}: {exc}"
            ) from exc
        if not isinstance(manifest, dict):
            raise RawTapeIntegrityError(
                f"manifest must be a JSON object: {manifest_path.name}"
            )
        # Filename <-> embedded-field binding ({bucket}-{seq}).
        filename_seq = int(
          manifest_path.name.split("-")[-1].removesuffix(_MANIFEST_SUFFIX)
        )
        filename_bucket = int(manifest_path.name.split("-")[0])
        if (
            manifest.get("segment_sequence") != filename_seq
            or manifest.get("bucket_start_ms") != filename_bucket
        ):
            raise RawTapeIntegrityError(
                f"filename/manifest field binding mismatch: "
                f"{manifest_path.name}"
            )
        schema = manifest.get("storage_schema_version")
        if schema not in _ACCEPTED_STORAGE_SCHEMA_VERSIONS:
            raise RawTapeIntegrityError(
                f"unknown storage_schema_version {schema!r}: "
                f"{manifest_path.name}"
            )
        for field in (
            "campaign_id", "market", "source_identity",
            "record_count", "uncompressed_bytes",
            "uncompressed_content_sha256", "compressed_file_sha256",
            "previous_segment_sha256",
        ):
            if field not in manifest:
                raise RawTapeIntegrityError(
                    f"manifest missing required field {field}: "
                    f"{manifest_path.name}"
                )
        manifests.append(manifest)
    manifests.sort(key=lambda m: m["segment_sequence"])
    return manifests


def _verify_no_active_partials_on_closed_campaign(directory: Path) -> None:
    partials = list(directory.glob(f"*{_PARTIAL_SUFFIX}"))
    if partials:
        raise RawTapeIntegrityError(
            f"active partial present on closed-campaign verify: "
            f"{partials[0].name}"
        )


def iter_segmented_zstd(
    directory: Path, *, allow_partials: bool = False
) -> Iterator[dict[str, Any]]:
    """Stream segmented zstd tape with full chain validation.

    Bounded-memory: compressed SHA is computed chunk-wise from the raw byte
    stream; decompressed content SHA + record envelope validation happen on
    64 KiB chunks. No whole-file read_bytes on any data path.
    """
    manifests = _load_and_validate_manifests(directory)
    if not allow_partials:
        _verify_no_active_partials_on_closed_campaign(directory)
    dctx = zstd.ZstdDecompressor()
    last_received: int | None = None
    previous_actual_sha: str | None = None
    for manifest in manifests:
        if manifest["previous_segment_sha256"] != previous_actual_sha:
            raise RawTapeIntegrityError(
                f"broken hash chain before segment "
                f"{manifest['segment_sequence']}"
            )
        data_path = _segment_data_path(directory, manifest)
        # Pass 1: chunk-wise compressed-file SHA.
        compressed_digest = hashlib.sha256()
        with data_path.open("rb") as raw_handle:
            while True:
                chunk = raw_handle.read(_STREAM_READ_SIZE)
                if not chunk:
                    break
                compressed_digest.update(chunk)
        actual_compressed_sha = compressed_digest.hexdigest()
        if actual_compressed_sha != manifest["compressed_file_sha256"]:
            raise RawTapeIntegrityError(
                f"compressed hash mismatch on segment "
                f"{manifest['segment_sequence']}"
            )
        previous_actual_sha = actual_compressed_sha
        # Pass 2: streaming decompression with envelope validation.
        content_digest = hashlib.sha256()
        uncompressed_bytes_seen = 0
        count = 0
        with data_path.open("rb") as handle:
            reader = dctx.stream_reader(handle, read_across_frames=True)
            buffer = b""
            while True:
                chunk = reader.read(_STREAM_READ_SIZE)
                if not chunk:
                    break
                content_digest.update(chunk)
                uncompressed_bytes_seen += len(chunk)
                buffer += chunk
                while b"\n" in buffer:
                    line_b, buffer = buffer.split(b"\n", 1)
                    if not line_b:
                        continue
                    record = json.loads(line_b.decode("utf-8"))
                    if not isinstance(record, dict):
                        raise RawTapeIntegrityError("record not an object")
                    market = record.get("market")
                    if not isinstance(market, str) or market != manifest["market"]:
                        raise RawTapeIntegrityError("market mismatch in record")
                    ts = record.get("received_at_ms")
                    if isinstance(ts, bool) or not isinstance(ts, int):
                        raise RawTapeIntegrityError("non-integer receipt time")
                    if "payload" not in record:
                        raise RawTapeIntegrityError("payload missing in record")
                    if last_received is not None and ts < last_received:
                        raise RawTapeIntegrityError(
                            "receipt moved backwards across records"
                        )
                    last_received = ts
                    count += 1
                    yield record
        if count != manifest["record_count"]:
            raise RawTapeIntegrityError(
                f"record count mismatch in segment "
                f"{manifest['segment_sequence']}: "
                f"{count} != {manifest['record_count']}"
            )
        if uncompressed_bytes_seen != manifest["uncompressed_bytes"]:
            raise RawTapeIntegrityError(
                f"uncompressed byte count mismatch in segment "
                f"{manifest['segment_sequence']}: "
                f"{uncompressed_bytes_seen} != {manifest['uncompressed_bytes']}"
            )
        if content_digest.hexdigest() != manifest["uncompressed_content_sha256"]:
            raise RawTapeIntegrityError(
                f"decompressed content hash mismatch in segment "
                f"{manifest['segment_sequence']}"
            )


def iter_raw_tape(directory: Path) -> Iterator[dict[str, Any]]:
    """Auto-detect format and stream logical records."""
    if any(directory.glob(f"*{_MANIFEST_SUFFIX}")):
        yield from iter_segmented_zstd(directory)
    elif any(directory.glob("*.jsonl")):
        yield from iter_legacy_jsonl(directory)
    else:
        return
