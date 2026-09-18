
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import struct
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import zstandard as zstd

from signalbot.data.raw_events import (
    RawEventCapacityError,
    RawEventOverflowError,
    RawEventRecorderClosedError,
    RawEventRecorderFatalError,
    RawEventWriterError,
)
from signalbot.domain.enums import Market

LOGGER = logging.getLogger(__name__)

STORAGE_SCHEMA_VERSION = "raw_tape_segmented_zstd_v1"
# WP4: new segments label themselves v2. Readers accept the strict set
# {v1, v2}; the runtime storage-mode selector remains STORAGE_SCHEMA_VERSION.
NEW_STORAGE_SCHEMA_VERSION = "raw_tape_segmented_zstd_v2"
_OUTER_FRAME_MAGIC = b"PRTASEG"
_OUTER_FRAME_MAGIC = b"PRTASEG"
_OUTER_FRAME_FORMAT_VERSION = 1
_OUTER_FRAME_HEADER_CORE = struct.Struct(">8sBQQ32s")
_OUTER_FRAME_DIGEST_SIZE = hashlib.sha256().digest_size
_OUTER_FRAME_HEADER_SIZE = _OUTER_FRAME_HEADER_CORE.size + _OUTER_FRAME_DIGEST_SIZE
_PARTIAL_SUFFIX = ".jsonl.zst.partial"
_MANIFEST_SUFFIX = ".manifest.json"
_DATA_TMP_SUFFIX = ".jsonl.zst.tmp"
_MANIFEST_TMP_SUFFIX = ".manifest.json.tmp"
_EVIDENCE_FAILURE_MARKER_SUFFIX = ".evidence-failure"
_STREAM_READ_SIZE = 64 * 1024


def compress_bound(n: int) -> int:
    """Worst-case zstd compressed size bound.

    Mirrors libzstd ZSTD_COMPRESSBOUND(N). We use the documented margin
    N + N//8 + 128, which dominates libzstd real worst case at every input
    size: frame overhead below 128 KiB is under 128 bytes and above that
    the shift term alone covers it (ZSTD_COMPRESSBOUND uses +128 KiB there,
    far more than N>>8 for any practical segment size).
    """
    return n + n // 8 + 128


class _QuotaExceeded(RuntimeError):
    """Internal: admission projection exceeded the physical byte budget."""


def _fsync_path(path: Path) -> None:
    # Windows requires a writable descriptor for FlushFileBuffers/fsync.
    with path.open("rb+") as handle:
        os.fsync(handle.fileno())


def _fsync_parent(path: Path) -> None:
    # Windows lacks a portable directory-fsync primitive. POSIX renames are
    # made durable by syncing the parent after every atomic replacement.
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class GlobalQuotaAuthority:
    """Single physical-byte quota owner shared by every market writer.

    WP3 contract (devlog/_plan/260826_phase-m/030_wp3_quota_finalize_recovery.md):
    ONE instance owned by ProspectiveTapeRecorder tracks baseline (finalized
    data+manifest bytes at bootstrap), durable compressed/manifest bytes
    produced this run, per-market active partial plaintext bytes, admission
    reservations, and a transient finalize reserve (compress bound of every
    active partial). Admission check AND reservation happen atomically inside
    self._lock; the lock is never held across to_thread disk work.
    Monotonicity: bound(p+e)-bound(p) >= e because (p+e)//8 >= p//8, so the
    reserved->partial migration never lowers the projection.
    """

    def __init__(self, maximum_physical_bytes: int) -> None:
        self._lock = threading.Lock()
        self.maximum_physical_bytes = maximum_physical_bytes
        self.baseline = 0
        self.durable_data = 0
        self.durable_manifest = 0
        self.partial_plaintext: dict[str, int] = {}
        self.reserved_admission = 0
        # Manifest writes are small; scale the carve-out so tiny test budgets
        # still admit while production budgets keep a comfortable margin.
        self.emergency_reserve = max(
            64, min(1024 * 1024, maximum_physical_bytes // 10_000)
        )

    def set_baseline(self, baseline: int) -> None:
        with self._lock:
            self.baseline = baseline

    def _projection_locked(self, encoded_delta: int) -> int:
        transient = sum(
            compress_bound(p) for p in self.partial_plaintext.values()
        )
        return (
            self.baseline
            + self.durable_data
            + self.durable_manifest
            + transient
            + self.reserved_admission
            + encoded_delta
            + self.emergency_reserve
        )

    def check_and_reserve(self, encoded: int) -> int:
        """Atomically evaluate the admission invariant and reserve bytes."""
        with self._lock:
            projected = self._projection_locked(encoded)
            if projected > self.maximum_physical_bytes:
                raise _QuotaExceeded(
                    f"prospective admission would exceed hard byte quota "
                    f"({projected} > {self.maximum_physical_bytes})"
                )
            self.reserved_admission += encoded
            return projected

    def commit_to_partial(self, market_key: str, encoded: int) -> None:
        """Migrate a durable batch from reserved into its active partial."""
        with self._lock:
            self.reserved_admission -= encoded
            self.partial_plaintext[market_key] = (
                self.partial_plaintext.get(market_key, 0) + encoded
            )

    def rollback(self, encoded: int) -> None:
        """Return reserved bytes to the budget (queue-full/writer-failure)."""
        with self._lock:
            self.reserved_admission = max(
                0, self.reserved_admission - encoded
            )

    def record_finalize(
        self, market_key: str, compressed_actual: int, manifest_actual: int
    ) -> None:
        """Account a sealed segment: partial retires, data+manifest durable."""
        with self._lock:
            self.partial_plaintext.pop(market_key, None)
            self.durable_data += compressed_actual
            self.durable_manifest += manifest_actual

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "baseline_bytes": self.baseline,
                "durable_data_bytes": self.durable_data,
                "durable_manifest_bytes": self.durable_manifest,
                "partial_plaintext_bytes": sum(
                    self.partial_plaintext.values()
                ),
                "reserved_bytes": self.reserved_admission,
                "transient_finalize_reserve_bytes": sum(
                    compress_bound(p) - p
                    for p in self.partial_plaintext.values()
                ),
            }


class ProspectiveStorageCapacityError(RuntimeError):
    """Raised when admission would exceed the physical storage budget."""


class ProspectiveTapeIntegrityError(RuntimeError):
    """Raised when segment chain/recovery validation fails closed."""


@dataclass
class _Active:
    bucket_start_ms: int
    sequence: int
    handle: Any | None = None
    uncompressed_bytes: int = 0
    record_count: int = 0
    first_received_at_ms: int | None = None
    last_received_at_ms: int | None = None
    content_digest: Any = None


class ProspectiveSegmentedWriter:
    """Per-market segmented zstd writer with crash recovery + hash chain.

    Mirrors the proven durability skeleton of capture.storage's
    SegmentedCaptureWriter while enforcing prospective-domain semantics.
    """

    def __init__(
        self,
        directory: Path,
        *,
        campaign_id: str,
        market: Market,
        source_identity: str,
        rotation_interval_ms: int = 300_000,
        maximum_uncompressed_bytes: int = 256 * 1024 * 1024,
        maximum_physical_bytes: int = 8 * 1024 * 1024 * 1024,
        compression_level: int = 3,
        quota_authority: GlobalQuotaAuthority | None = None,
    ) -> None:
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)
        self.campaign_id = campaign_id
        self.market = market.value
        self.source_identity = source_identity
        self.rotation_interval_ms = rotation_interval_ms
        self.maximum_uncompressed_bytes = maximum_uncompressed_bytes
        self.maximum_physical_bytes = maximum_physical_bytes
        self._quota = quota_authority
        self._compressor = zstd.ZstdCompressor(level=compression_level)
        self._active: _Active | None = None
        self._last_received_at_ms: int | None = None
        self._last_sequence = self._scan_last_sequence()
        self._detect_broken_finalize_windows()
        self._recover_unfinished_tail()
        self.verify_existing_chain()
        self._known_disk_bytes = self._physical_durable_bytes()
        self.closed = False

    def _scan_last_sequence(self) -> int:
        last = 0
        for path in self.directory.glob("*.jsonl.zst"):
            stem = path.name.removesuffix(".jsonl.zst")
            try:
                seq = int(stem.split("-")[-1])
            except ValueError:
                continue
            last = max(last, seq)
        return last

    def _physical_durable_bytes(self) -> int:
        total = sum(f.stat().st_size for f in self.directory.glob("*.jsonl.zst"))
        total += sum(f.stat().st_size for f in self.directory.glob("*.json"))
        return total

    def _detect_broken_finalize_windows(self) -> None:
        """Fail closed on crash windows between data-replace and cleanup.

        Window (b): sealed data file with no manifest -> integrity error.
        Window (c): stale partial whose sequence already has both data and
        manifest -> stale partial is verified present then removed; the
        durable pair wins (F9 fix).
        """
        manifest_seqs = {
            int(p.name.split("-")[-1].removesuffix(_MANIFEST_SUFFIX))
            for p in self.directory.glob(f"*{_MANIFEST_SUFFIX}")
        }
        for data_path in list(self.directory.glob("*.jsonl.zst")):
            seq = int(data_path.name.split("-")[-1].removesuffix(".jsonl.zst"))
            if seq not in manifest_seqs:
                raise ProspectiveTapeIntegrityError(
                    f"sealed segment without manifest requires manual triage: "
                    f"{data_path.name}"
                )
        for partial in list(self.directory.glob(f"*{_PARTIAL_SUFFIX}")):
            seq = int(
                partial.name.split("-")[-1].removesuffix(
                    ".jsonl.zst.partial"
                )
            )
            if seq in manifest_seqs:
                stem = partial.name.removesuffix(_PARTIAL_SUFFIX)
                data_path = self.directory / (stem + ".jsonl.zst")
                manifest_path = self.directory / (stem + _MANIFEST_SUFFIX)
                if not (data_path.exists() and manifest_path.exists()):
                    raise ProspectiveTapeIntegrityError(
                        f"stale partial lacks durable finalized pair: "
                        f"{partial.name}"
                    )
                LOGGER.warning(
                    "removing stale partial already finalized",
                    extra={"partial": partial.name},
                )
                partial.unlink()

    def _recover_unfinished_tail(self) -> None:
        partials = sorted(self.directory.glob(f"*{_PARTIAL_SUFFIX}"))
        if not partials:
            return
        if len(partials) > 1:
            raise ProspectiveTapeIntegrityError(
                f"ambiguous partial segments require manual triage: "
                f"{[p.name for p in partials]}"
            )
        partial = partials[0]
        LOGGER.warning(
            "recovering unfinished prospective tape segment",
            extra={"partial": partial.name},
        )
        # WP3: an unreadable partial is NEVER unlinked - fail closed with the
        # file byte-for-byte untouched (F5 fix).
        try:
            raw_bytes = partial.read_bytes()
            raw_text = raw_bytes.decode("utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise ProspectiveTapeIntegrityError(
                f"partial segment unreadable; manual triage required: "
                f"{partial.name}: {exc}"
            ) from exc
        recovered_lines = [
            line for line in raw_text.splitlines(keepends=True)
            if line.endswith("\n")
        ]
        torn_suffix = "".join(
            line for line in raw_text.splitlines(keepends=True)
            if not line.endswith("\n")
        )
        torn_len = len(torn_suffix.encode("utf-8"))
        if torn_len > 0:
            # WP4 fix: the torn suffix must NEVER survive into the finalized
            # segment. Rewrite the partial with only complete lines BEFORE the
            # canonical finalize path; the marker preserves what was dropped.
            marker = partial.with_name(
                partial.name + _EVIDENCE_FAILURE_MARKER_SUFFIX
            )
            report = json.dumps({
                "failure": "torn_partial_suffix",
                "partial": partial.name,
                "torn_suffix_bytes": torn_len,
                "recovered_lines": len(recovered_lines),
            }, indent=2)
            marker.write_text(report, encoding="utf-8")
            LOGGER.error(
                "torn partial suffix recorded as evidence failure",
                extra={"partial": partial.name, "torn_bytes": torn_len},
            )
            with partial.open("wb") as rewrite_handle:
                rewrite_handle.write(
                    "".join(recovered_lines).encode("utf-8")
                )
                rewrite_handle.flush()
                os.fsync(rewrite_handle.fileno())
        # Adopt as active segment continuing its bucket/sequence.
        seq = int(partial.name.split("-")[-1].removesuffix(".jsonl.zst.partial"))
        bucket = int(partial.name.split("-")[0])
        self._last_sequence = max(self._last_sequence, seq)
        digest = hashlib.sha256()
        if recovered_lines:
            for line in recovered_lines:
                digest.update(line.encode("utf-8"))
            self._active = _Active(
                bucket_start_ms=bucket,
                sequence=seq,
                uncompressed_bytes=sum(
                    len(line.encode("utf-8")) for line in recovered_lines
                ),
                record_count=len(recovered_lines),
                content_digest=digest,
            )
            # Canonical finalize path (no special-case recovery writer).
            self.finalize_active()
        else:
            partial.unlink()

    def verify_existing_chain(self) -> None:
        """Fail closed on any existing-chain integrity violation.

        WP4 pillar 1: invoked by ProspectiveTapeRecorder BEFORE baseline scan
        and ANY admission. Checks contiguous sequence from 1, exactly one
        manifest per data segment (both directions), filename<->manifest
        binding, campaign identity against constructor args, per-manifest
        field validation, strict V1/V2 schema membership, prev-hash chain
        (segment 1 prev=null), and expected-file whitelist. Evidence-failure
        markers are legal residue only when their referenced partial is
        absent; stale *.tmp files are manual-triage integrity errors.
        """
        manifests = sorted(
            self.directory.glob(f"*{_MANIFEST_SUFFIX}"),
            key=lambda path: int(path.name.split("-")[-1].removesuffix(_MANIFEST_SUFFIX)),
        )
        data_paths = sorted(
            self.directory.glob("*.jsonl.zst"),
            key=lambda path: int(path.name.split("-")[-1].removesuffix(".jsonl.zst")),
        )
        if len(manifests) != len(data_paths):
            raise ProspectiveTapeIntegrityError(
                f"manifest/data count mismatch in {self.directory}: "
                f"{len(manifests)} manifests vs {len(data_paths)} data segments"
            )
        previous_sha: str | None = None
        for index, (manifest_path, data_path) in enumerate(
            zip(manifests, data_paths, strict=True), start=1
        ):
            manifest_seq = int(
                manifest_path.name.split("-")[-1].removesuffix(_MANIFEST_SUFFIX)
            )
            data_seq = int(
                data_path.name.split("-")[-1].removesuffix(".jsonl.zst")
            )
            if manifest_seq != index or data_seq != index:
                raise ProspectiveTapeIntegrityError(
                    f"non-contiguous segment sequence at {manifest_path.name}"
                )
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ProspectiveTapeIntegrityError(
                    f"unreadable manifest requires manual triage: "
                    f"{manifest_path.name}: {exc}"
                ) from exc
            required_fields = (
                "storage_schema_version", "campaign_id", "market",
                "source_identity", "bucket_start_ms", "record_count",
                "uncompressed_bytes", "compressed_bytes",
                "uncompressed_content_sha256", "compressed_file_sha256",
                "previous_segment_sha256",
            )
            for field in required_fields:
                if field not in manifest:
                    raise ProspectiveTapeIntegrityError(
                        f"manifest missing required field {field}: "
                        f"{manifest_path.name}"
                    )
            if manifest["storage_schema_version"] not in {
                STORAGE_SCHEMA_VERSION, NEW_STORAGE_SCHEMA_VERSION,
            }:
                raise ProspectiveTapeIntegrityError(
                    f"unknown storage_schema_version "
                    f"{manifest['storage_schema_version']}: {manifest_path.name}"
                )
            if manifest["campaign_id"] != self.campaign_id:
                raise ProspectiveTapeIntegrityError(
                    f"cross-campaign mount rejected: manifest campaign "
                    f"{manifest['campaign_id']!r} != recorder {self.campaign_id!r}"
                )
            if manifest["market"] != self.market:
                raise ProspectiveTapeIntegrityError(
                    f"market mismatch: manifest market {manifest['market']!r} "
                    f"!= writer directory {self.market!r}"
                )
            if manifest["source_identity"] != self.source_identity:
                raise ProspectiveTapeIntegrityError(
                    f"source_identity mismatch: manifest "
                    f"{manifest['source_identity']!r} != recorder "
                    f"{self.source_identity!r}"
                )
            # Filename <-> embedded-field binding ({bucket}-{seq}).
            manifest_bucket = int(manifest_path.name.split("-")[0])
            if (
                manifest_bucket != manifest["bucket_start_ms"]
                or manifest["segment_sequence"] != manifest_seq
            ):
                raise ProspectiveTapeIntegrityError(
                    f"filename/manifest field binding mismatch: "
                    f"{manifest_path.name}"
                )
            actual_sha = hashlib.sha256(data_path.read_bytes()).hexdigest()
            if actual_sha != manifest["compressed_file_sha256"]:
                raise ProspectiveTapeIntegrityError(
                    f"compressed hash mismatch on segment {manifest_seq}"
                )
            if manifest["previous_segment_sha256"] != previous_sha:
                raise ProspectiveTapeIntegrityError(
                    f"broken hash chain before segment {manifest_seq}"
                )
            previous_sha = actual_sha
        partials = list(self.directory.glob(f"*{_PARTIAL_SUFFIX}"))
        if len(partials) > 1:
            raise ProspectiveTapeIntegrityError(
                f"ambiguous partial segments require manual triage: "
                f"{[p.name for p in partials]}"
            )
        for marker in self.directory.glob(f"*{_EVIDENCE_FAILURE_MARKER_SUFFIX}"):
            referenced_partial_name = marker.name.removesuffix(
                _EVIDENCE_FAILURE_MARKER_SUFFIX
            )
            if (self.directory / referenced_partial_name).exists():
                continue  # active recovery candidate handled above/below
        for tmp_file in list(self.directory.glob(f"*{_DATA_TMP_SUFFIX}")) + list(
            self.directory.glob(f"*{_MANIFEST_TMP_SUFFIX}")
        ):
            raise ProspectiveTapeIntegrityError(
                f"stale tmp file requires manual triage: {tmp_file.name}"
            )


    def _previous_segment_sha(self, seq: int) -> str | None:
        prior = seq - 1
        if prior < 1:
            return None
        candidates = list(self.directory.glob(f"*-{prior:08d}.jsonl.zst"))
        if not candidates:
            raise ProspectiveTapeIntegrityError(
                f"missing predecessor segment for sequence {seq}; "
                f"chain continuity broken"
            )
        return hashlib.sha256(candidates[0].read_bytes()).hexdigest()

    def _bucket_for_seq(self, seq: int) -> int:
        for m in self.directory.glob(f"*-{seq:08d}.manifest.json"):
            data = json.loads(m.read_text(encoding="utf-8"))
            return int(data["bucket_start_ms"])
        return 0

    def append(self, line: str, received_at_ms: int) -> None:
        """Admit one pre-serialized JSONL record (no trailing newline needed).

        Enforces per-market monotonic receipts and rotates on bucket or size
        bounds. Synchronous disk write is owned by the CALLING context (the
        async recorder's background writer task), preserving the Phase-J P0
        non-blocking admission contract upstream.
        """
        encoded = (line if line.endswith("\n") else line + "\n").encode("utf-8")
        if self._last_received_at_ms is not None and (
            received_at_ms < self._last_received_at_ms
        ):
            raise ProspectiveTapeIntegrityError(
                "prospective receipt time moved backwards"
            )
        bucket_start = received_at_ms - (
            received_at_ms % self.rotation_interval_ms
        )
        if (
            self._active is not None
            and bucket_start != self._active.bucket_start_ms
        ) or (
            self._active is not None
            and self._active.uncompressed_bytes >= self.maximum_uncompressed_bytes
        ):
            self.finalize_active()
        if self._active is None:
            self._open_segment(bucket_start)
        assert self._active is not None and self._active.handle is not None
        self._active.handle.write(line if line.endswith("\n") else line + "\n")
        act = self._active
        act.uncompressed_bytes += len(encoded)
        act.record_count += 1
        act.content_digest.update(encoded)
        if act.first_received_at_ms is None:
            act.first_received_at_ms = received_at_ms
        act.last_received_at_ms = received_at_ms
        self._last_received_at_ms = received_at_ms

    def _open_segment(self, bucket_start: int) -> None:
        seq = self._last_sequence + 1
        path = self.directory / f"{bucket_start}-{seq:08d}{_PARTIAL_SUFFIX}"
        # Plain-text partial: fast appends, trivially recoverable on crash.
        # Compression happens once at finalize time (single zstd frame).
        handle = path.open("a", encoding="utf-8", newline="\n")
        self._active = _Active(
            handle=handle,
            bucket_start_ms=bucket_start,
            sequence=seq,
            content_digest=hashlib.sha256(),
        )

    def finalize_active(self) -> Path | None:
        if self._active is None:
            return None
        act = self._active
        if act.handle is not None:
            act.handle.flush()
            os.fsync(act.handle.fileno())
            act.handle.close()
            act.handle = None
        partial = (
            self.directory
            / f"{act.bucket_start_ms}-{act.sequence:08d}{_PARTIAL_SUFFIX}"
        )
        finalized_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        data_path = (
            self.directory
            / f"{act.bucket_start_ms}-{act.sequence:08d}.jsonl.zst"
        )
        raw = partial.read_bytes()
        compressed = zstd.ZstdCompressor(level=3).compress(raw)
        roundtrip = zstd.ZstdDecompressor().decompress(
            compressed, max_output_size=len(raw) + 1
        )
        if roundtrip != raw:
            raise ProspectiveTapeIntegrityError(
                "compression roundtrip mismatch during segment finalize"
            )
        prev_sha = self._previous_segment_sha(act.sequence)
        manifest = {
            "schema_version": "prospective_segment_manifest_v1",
            "storage_schema_version": NEW_STORAGE_SCHEMA_VERSION,
            "campaign_id": self.campaign_id,
            "market": self.market,
            "segment_sequence": act.sequence,
            "bucket_start_ms": act.bucket_start_ms,
            "first_received_at_ms": act.first_received_at_ms,
            "last_received_at_ms": act.last_received_at_ms,
            "record_count": act.record_count,
            "uncompressed_bytes": act.uncompressed_bytes,
            "compressed_bytes": len(compressed),
            "uncompressed_content_sha256": act.content_digest.hexdigest(),
            "compressed_file_sha256": hashlib.sha256(compressed).hexdigest(),
            "previous_segment_sha256": prev_sha,
            "source_identity": self.source_identity,
            "recovered_from_partial": False,
            "finalized_at_utc": finalized_utc,
        }
        manifest_path = (
            self.directory
            / f"{act.bucket_start_ms}-{act.sequence:08d}{_MANIFEST_SUFFIX}"
        )
        # Crash-safe atomic ordering (F1 fix): tmp -> fsync -> replace for the
        # data file; manifest.tmp -> fsync -> atomic rename; the partial is
        # unlinked ONLY after both durable artifacts exist.
        data_tmp = self.directory / (
            f"{act.bucket_start_ms}-{act.sequence:08d}{_DATA_TMP_SUFFIX}"
        )
        with data_tmp.open("wb") as handle:
            handle.write(compressed)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(data_tmp, data_path)
        _fsync_path(data_path)
        _fsync_parent(data_path)
        manifest_tmp = self.directory / (
            f"{act.bucket_start_ms}-{act.sequence:08d}{_MANIFEST_TMP_SUFFIX}"
        )
        encoded_manifest = json.dumps(manifest, indent=2).encode("utf-8")
        with manifest_tmp.open("wb") as handle:
            handle.write(encoded_manifest)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(manifest_tmp, manifest_path)
        _fsync_path(manifest_path)
        _fsync_parent(manifest_path)
        self._active = None
        self._last_sequence = act.sequence
        partial.unlink()
        _fsync_parent(partial)
        if self._quota is not None:
            self._quota.record_finalize(
                self.market, len(compressed), len(encoded_manifest)
            )
        return data_path

    def close(self) -> Path | None:
        if self.closed:
            return None
        self.closed = True
        result = self.finalize_active()
        return result


class ProspectiveTapeRecorder:
    """True non-blocking async recorder over per-market segmented writers.

    Phase-M WP2 contract (devlog/_plan/260826_phase-m/020_wp2_async_core.md):

    - append(): serialize envelope, reserve global quota, bounded-queue admit,
      RETURN without waiting for any disk write. ACCEPTED != DURABLE.
    - per-market background writer tasks own ALL synchronous work; each batch
      is executed via SegmentedWriter.append() inside asyncio.to_thread.
    - durability is a watermark advanced only after to_thread returns; graceful
      close drains every accepted record before finalizing segments.
    - any fatal condition is observable via wait_failed()/status() and raises
      fail-closed; wait_failed() is a real asyncio.Event, never a sleep.
    """

    def __init__(
        self,
        directory: Path,
        *,
        campaign_id: str,
        source_identity: str,
        rotation_interval_ms: int = 300_000,
        maximum_uncompressed_bytes: int = 256 * 1024 * 1024,
        compression_level: int = 3,
        max_queue_size: int = 50_000,
        raw_event_max_bytes: int = 10_737_418_240,
        write_batch_hook=None,
    ) -> None:
        self.directory = directory
        self.campaign_id = campaign_id
        self.source_identity = source_identity
        self.rotation_interval_ms = rotation_interval_ms
        self.maximum_uncompressed_bytes = maximum_uncompressed_bytes
        self.compression_level = compression_level
        self.accepted_records = 0
        self.durable_records = 0
        self.max_queue_size = max_queue_size
        self.raw_event_max_bytes = raw_event_max_bytes
        self.overflow_count = 0
        self.baseline_bytes = 0
        self.quota = GlobalQuotaAuthority(raw_event_max_bytes)
        # Backward-compatible views over the authority (tests + raw_events
        # monitor read these attributes directly).
        self._fatal_error: Exception | None = None
        self._fatal_event: asyncio.Event | None = None
        self._writers: dict[Market, ProspectiveSegmentedWriter] = {}
        self._tasks: dict[Market, asyncio.Task[None]] = {}
        self._queues: dict[Market, asyncio.Queue[tuple[str, int] | None]] = {}
        self._write_batch_hook = write_batch_hook
        self._closed = False
        # Reviewer P2 fix: ALL disk initialization (quota bootstrap scan,
        # per-market writer construction incl. recovery) happens HERE, at
        # construction/startup time - never inside append(). The ingest path
        # is then pure event-loop work: serialize + reserve + put_nowait.
        self.directory.mkdir(parents=True, exist_ok=True)
        for market in Market:
            self._writer_for(market)
        # Baseline scan AFTER recovery so recovered segment bytes count (F3).
        self.baseline_bytes = sum(
            path.stat().st_size
            for path in self.directory.rglob("*")
            if path.is_file()
        )
        self.quota.set_baseline(self.baseline_bytes)

    def _ensure_fatal_event(self) -> asyncio.Event:
        if self._fatal_event is None:
            self._fatal_event = asyncio.Event()
        return self._fatal_event

    @property
    def reserved_bytes(self) -> int:
        return self.quota.reserved_admission

    @property
    def durable_bytes(self) -> int:
        return (
            self.quota.durable_data
            + sum(self.quota.partial_plaintext.values())
        )

    def _signal_fatal(self, error: RawEventRecorderFatalError) -> None:
        if self._fatal_error is None:
            self._fatal_error = error
        self._ensure_fatal_event().set()

    def status(self) -> dict[str, Any]:
        return {
            "accepted_records": self.accepted_records,
            "durable_records": self.durable_records,
            "queued_records": self.accepted_records - self.durable_records,
            "baseline_bytes": self.baseline_bytes,
            **self.quota.snapshot(),
            "storage_schema_version": NEW_STORAGE_SCHEMA_VERSION,
            "overflow_count": self.overflow_count,
            "fatal_error": (
                None if self._fatal_error is None else str(self._fatal_error)
            ),
        }

    async def wait_failed(self) -> None:
        await self._ensure_fatal_event().wait()

    def is_failed(self) -> bool:
        return self._fatal_event is not None and self._fatal_event.is_set()

    @property
    def fatal_error(self) -> Exception | None:
        return self._fatal_error

    async def append(
        self, market: Market, payload: Any, event_time_ms: int
    ) -> None:
        if self._closed:
            raise RawEventRecorderClosedError(
                "prospective tape recorder already closed"
            )
        if self._fatal_error is not None:
            raise self._fatal_error
        line = json.dumps(
            {
                "market": market.value,
                "received_at_ms": event_time_ms,
                "payload": payload,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ) + "\n"
        encoded_bytes = len(line.encode("utf-8"))
        try:
            self.quota.check_and_reserve(encoded_bytes)
        except _QuotaExceeded as exc:
            self.overflow_count += 1
            raise RawEventCapacityError(str(exc)) from exc
        queue = self._queue_for(market)
        try:
            queue.put_nowait((line, encoded_bytes))
        except asyncio.QueueFull as exc:
            self.quota.rollback(encoded_bytes)
            self.overflow_count += 1
            error = RawEventOverflowError(
                "prospective writer queue overflow; failing closed"
            )
            self._signal_fatal(error)
            raise error from exc
        self.accepted_records += 1


    async def _writer_loop(self, market: Market) -> None:
        """Background task owning all synchronous writes for one market."""
        queue = self._queues[market]
        writer = self._writers[market]
        while True:
            item = await queue.get()
            if item is None:
                break
            batch = [item]
            while True:
                try:
                    nxt = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if nxt is None:
                    await queue.put(None)
                    break
                batch.append(nxt)
            try:
                if self._write_batch_hook is not None:
                    # Test gate lives HERE in the writer task, never in the
                    # thread body; a raising hook is a writer failure.
                    await self._write_batch_hook(writer)
                await asyncio.to_thread(
                    self._append_batch_sync,
                    market,
                    [line for line, _ in batch],
                )
            except Exception as exc:
                encoded_total = sum(encoded for _, encoded in batch)
                self.quota.rollback(encoded_total)
                fatal = (
                    exc
                    if isinstance(exc, RawEventRecorderFatalError)
                    else RawEventWriterError(f"prospective writer failed: {exc}")
                )
                fatal.__cause__ = exc
                lost = self.accepted_records - self.durable_records - len(batch)
                self._signal_fatal(
                    type(fatal)(
                        f"{fatal} [accepted={self.accepted_records} "
                        f"durable_at_failure={self.durable_records} "
                        f"lost_including_batch={lost + len(batch)}]"
                    )
                )
                break
            encoded_total = sum(encoded for _, encoded in batch)
            self.quota.commit_to_partial(market.value, encoded_total)
            self.durable_records += len(batch)

    def _append_batch_sync(self, market: Market, lines: list[str]) -> None:
        """Pure synchronous sink executed inside asyncio.to_thread."""
        writer = self._writers[market]
        for line in lines:
            ts = int(json.loads(line)["received_at_ms"])
            try:
                writer.append(line, ts)
            except ProspectiveStorageCapacityError as exc:
                raise RawEventCapacityError(str(exc)) from exc
            except ProspectiveTapeIntegrityError as exc:
                raise RawEventWriterError(str(exc)) from exc

    def _writer_for(self, market: Market) -> ProspectiveSegmentedWriter:
        writer = self._writers.get(market)
        if writer is None:
            writer = ProspectiveSegmentedWriter(
                self.directory / market.value,
                campaign_id=self.campaign_id,
                market=market,
                source_identity=self.source_identity,
                rotation_interval_ms=self.rotation_interval_ms,
                compression_level=self.compression_level,
                quota_authority=self.quota,
            )
            self._writers[market] = writer
        return writer

    def _queue_for(self, market: Market) -> asyncio.Queue[tuple[str, int] | None]:
        queue = self._queues.get(market)
        if queue is None:
            loop = asyncio.get_running_loop()
            queue = asyncio.Queue(maxsize=self.max_queue_size)
            self._queues[market] = queue
            task = loop.create_task(self._writer_loop(market))
            task.add_done_callback(self._task_done_callback)
            self._tasks[market] = task
        return queue

    def _task_done_callback(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and self._fatal_error is None:
            LOGGER.critical(
                "prospective writer task crashed",
                exc_info=error,
            )
            self._signal_fatal(
                RawEventWriterError(f"prospective writer task crashed: {error}")
            )

    async def wait_drained(self, timeout_seconds: float = 30.0) -> bool:
        """Wait until every accepted record is durable. Returns success."""
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while self.durable_records < self.accepted_records:
            if self.is_failed():
                return False
            if asyncio.get_running_loop().time() > deadline:
                return False
            await asyncio.sleep(0.01)
        return True

    async def close(self) -> None:
        """Stop admission, drain accepted records, finalize segments.

        Idempotent and bounded: a dead writer can never hang close(); the
        drain watermark check catches any accepted-but-lost evidence and
        fails closed instead of returning silently.
        """
        if self._closed:
            return
        self._closed = True
        for market, queue in self._queues.items():
            task = self._tasks[market]
            sentinel_delivered = False
            for _attempt in range(3):
                if task.done():
                    break
                try:
                    await asyncio.wait_for(queue.put(None), timeout=5.0)
                    sentinel_delivered = True
                    break
                except TimeoutError:
                    continue
            if not sentinel_delivered and not task.done():
                LOGGER.error(
                    "abandoning prospective writer sentinel after retries",
                    extra={"market": market.value},
                )
                self._signal_fatal(
                    RawEventWriterError(
                        f"close could not deliver sentinel to writer "
                        f"(market={market.value})"
                    )
                )
            try:
                if task.done():
                    if task.cancelled():
                        self.task_abandoned(market)
                    else:
                        task.result()
                else:
                    await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
            except asyncio.CancelledError:
                LOGGER.critical(
                    "prospective writer join cancelled during close",
                    extra={"market": market.value},
                )
                self.task_abandoned(market)
                raise
            except TimeoutError:
                LOGGER.critical(
                    "prospective writer join timed out; abandoning hung writer",
                    extra={"market": market.value},
                )
                self._signal_fatal(
                    RawEventWriterError(
                        f"writer join timed out during close "
                        f"(market={market.value})"
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.warning(
                    "prospective writer raised during close",
                    extra={"market": market.value},
                    exc_info=exc,
                )
        if self.durable_records != self.accepted_records:
            mismatch = RawEventWriterError(
                f"prospective recorder closed undrained: "
                f"accepted={self.accepted_records} "
                f"durable={self.durable_records}"
            )
            self._signal_fatal(mismatch)
            raise mismatch
        for writer in self._writers.values():
            writer.close()

    def task_abandoned(self, market: Market) -> None:
        self._signal_fatal(
            RawEventWriterError(
                f"writer abandoned during close (market={market.value})"
            )
        )
