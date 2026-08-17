from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import struct
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, BinaryIO

import zstandard as zstd

from signalbot.capture.errors import (
    CaptureIntegrityError,
    CaptureShortWriteError,
    CaptureStorageCapacityError,
)
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CaptureRecord,
    CoverageTransitionV1,
    RestEnvelopeV1,
    RestEnvelopeV2,
    record_to_json_line,
)

_SEGMENT_RE = re.compile(r"^(?P<bucket>\d{13})-(?P<sequence>\d{8})\.jsonl\.zst$")
_PARTIAL_SUFFIX = ".jsonl.zst.partial"
_MANIFEST_SUFFIX = ".manifest.json"
_OUTER_FRAME_MAGIC = b"SBCAPFRM"
_OUTER_FRAME_FORMAT_VERSION = 1
_OUTER_FRAME_HEADER_CORE = struct.Struct(">8sBQQ32s")
_OUTER_FRAME_DIGEST_SIZE = hashlib.sha256().digest_size
_OUTER_FRAME_HEADER_SIZE = (
    _OUTER_FRAME_HEADER_CORE.size + _OUTER_FRAME_DIGEST_SIZE
)
_STREAM_READ_SIZE = 64 * 1024
_ZSTD_FRAME_HEADER_MAX_SIZE = 18
_KNOWN_SCHEMAS = frozenset(
    {
        "capture_envelope_v1",
        "rest_envelope_v1",
        "rest_envelope_v2",
        "connection_transition_v1",
        "coverage_transition_v1",
    }
)


@dataclass(frozen=True, slots=True)
class SegmentManifestV1:
    data_file: str
    sequence: int
    bucket_start_ms: int
    rotation_interval_ms: int
    plan_sha256: str
    process_boot_id: str
    first_received_at_ms: int
    last_received_at_ms: int
    first_ingest_seq: int
    last_ingest_seq: int
    record_count: int
    frame_count: int
    uncompressed_bytes: int
    compressed_bytes: int
    sha256: str
    previous_segment_sha256: str | None
    recovered_from_partial: bool
    frame_format_version: int
    schema_version: str = "capture_segment_manifest_v1"


@dataclass(frozen=True, slots=True)
class _RecoveredMetadata:
    process_boot_id: str
    first_received_at_ms: int
    last_received_at_ms: int
    first_ingest_seq: int
    last_ingest_seq: int
    record_count: int
    frame_count: int
    uncompressed_bytes: int


@dataclass(slots=True)
class _MetadataAccumulator:
    expected_plan_sha256: str
    process_boot_id: str | None = None
    first_received_at_ms: int | None = None
    last_received_at_ms: int | None = None
    first_ingest_seq: int | None = None
    last_ingest_seq: int | None = None
    record_count: int = 0
    frame_count: int = 0
    uncompressed_bytes: int = 0

    def consume(self, line: bytes) -> None:
        try:
            raw: Any = json.loads(line)
        except json.JSONDecodeError as exc:
            raise CaptureIntegrityError(
                "complete zstd frame contains invalid JSON"
            ) from exc
        if not isinstance(raw, dict) or raw.get("schema_version") not in _KNOWN_SCHEMAS:
            raise CaptureIntegrityError("complete zstd frame has an unknown record schema")
        if raw.get("plan_sha256") != self.expected_plan_sha256:
            raise CaptureIntegrityError("recovered record plan hash mismatch")
        raw_process_boot_id = raw.get("process_boot_id")
        if not isinstance(raw_process_boot_id, str) or not raw_process_boot_id:
            raise CaptureIntegrityError("recovered record process boot is missing")
        if self.process_boot_id is None:
            self.process_boot_id = raw_process_boot_id
        elif raw_process_boot_id != self.process_boot_id:
            raise CaptureIntegrityError("recovered segment crosses process boots")
        if raw["schema_version"] == "rest_envelope_v1":
            receipt_key = "response_received_at_ms"
        elif raw["schema_version"] == "rest_envelope_v2":
            receipt_key = "response_completed_at_ms"
        else:
            receipt_key = "received_at_ms"
        receipt = raw.get(receipt_key)
        ingest = raw.get("ingest_seq")
        if not isinstance(receipt, int) or not isinstance(ingest, int):
            raise CaptureIntegrityError(
                "recovered record lacks integer receipt/ingest fields"
            )
        if self.last_received_at_ms is not None and receipt < self.last_received_at_ms:
            raise CaptureIntegrityError("recovered receipt times are not ordered")
        if self.last_ingest_seq is not None and ingest != self.last_ingest_seq + 1:
            raise CaptureIntegrityError(
                "recovered ingest sequences are not contiguous"
            )
        if self.first_received_at_ms is None:
            self.first_received_at_ms = receipt
            self.first_ingest_seq = ingest
        self.last_received_at_ms = receipt
        self.last_ingest_seq = ingest
        self.record_count += 1
        self.frame_count += int(raw["schema_version"] == "capture_envelope_v1")
        self.uncompressed_bytes += len(line)

    def finish(self) -> _RecoveredMetadata:
        if (
            self.process_boot_id is None
            or self.first_received_at_ms is None
            or self.last_received_at_ms is None
            or self.first_ingest_seq is None
            or self.last_ingest_seq is None
        ):
            raise CaptureIntegrityError("capture segment has no complete record")
        return _RecoveredMetadata(
            process_boot_id=self.process_boot_id,
            first_received_at_ms=self.first_received_at_ms,
            last_received_at_ms=self.last_received_at_ms,
            first_ingest_seq=self.first_ingest_seq,
            last_ingest_seq=self.last_ingest_seq,
            record_count=self.record_count,
            frame_count=self.frame_count,
            uncompressed_bytes=self.uncompressed_bytes,
        )


class SegmentedCaptureWriter:
    """Append-only, hash-chained, outer-framed zstd JSONL segments.

    Every JSON line is an independent checksummed zstd payload inside a
    versioned outer frame.  The integrity-protected outer header makes payload
    length, decoded length, and compressed SHA-256 reviewable before decoding.
    """

    def __init__(
        self,
        directory: str | Path,
        *,
        plan_sha256: str,
        process_boot_id: str,
        rotation_interval_ms: int = 300_000,
        maximum_uncompressed_bytes: int = 256 * 1024 * 1024,
        maximum_frames: int = 1_000_000,
        maximum_total_bytes: int = 100 * 1024 * 1024 * 1024,
        emergency_reserve_bytes: int = 512 * 1024 * 1024,
        compression_level: int = 3,
        recover_partials: bool = True,
    ) -> None:
        if rotation_interval_ms < 1:
            raise ValueError("rotation_interval_ms must be positive")
        if maximum_uncompressed_bytes < 1:
            raise ValueError("maximum_uncompressed_bytes must be positive")
        if maximum_frames < 1:
            raise ValueError("maximum_frames must be positive")
        if emergency_reserve_bytes < 1024:
            raise ValueError("emergency_reserve_bytes must be at least 1024")
        if maximum_total_bytes <= emergency_reserve_bytes:
            raise ValueError("maximum_total_bytes must exceed emergency reserve")
        if not process_boot_id:
            raise ValueError("process_boot_id must be non-empty")
        if re.fullmatch(r"[0-9a-f]{64}", plan_sha256) is None:
            raise ValueError("plan_sha256 must be a lowercase SHA-256 digest")
        self.directory = Path(directory)
        self.plan_sha256 = plan_sha256
        self.process_boot_id = process_boot_id
        self.rotation_interval_ms = rotation_interval_ms
        self.maximum_uncompressed_bytes = maximum_uncompressed_bytes
        self.maximum_frames = maximum_frames
        self.maximum_total_bytes = maximum_total_bytes
        self.emergency_reserve_bytes = emergency_reserve_bytes
        self._compressor = zstd.ZstdCompressor(
            level=compression_level,
            write_checksum=True,
        )
        self.directory.mkdir(parents=True, exist_ok=True)
        self._reject_auxiliary_crash_residue()
        self._known_disk_bytes = self._directory_size()
        if self._known_disk_bytes > maximum_total_bytes:
            raise CaptureStorageCapacityError(
                "capture directory already exceeds its configured quota"
            )
        manifests = verify_capture_segments(
            self.directory,
            expected_plan_sha256=plan_sha256,
            allow_unfinished_tail=True,
        )
        self._previous_segment_sha256 = manifests[-1].sha256 if manifests else None
        self._next_sequence = manifests[-1].sequence + 1 if manifests else 1
        self._handle: BinaryIO | None = None
        self._partial_path: Path | None = None
        self._bucket_start_ms: int | None = None
        self._record_count = 0
        self._frame_count = 0
        self._uncompressed_bytes = 0
        self._first_received_at_ms: int | None = None
        self._last_received_at_ms: int | None = (
            manifests[-1].last_received_at_ms if manifests else None
        )
        self._first_ingest_seq: int | None = None
        self._last_ingest_seq: int | None = (
            manifests[-1].last_ingest_seq
            if manifests and manifests[-1].process_boot_id == process_boot_id
            else None
        )
        self._segment_process_boot_id: str | None = None
        if recover_partials:
            self._recover_unfinished_tail()
            verify_capture_segments(
                self.directory,
                expected_plan_sha256=plan_sha256,
            )
        elif _unfinished_segment_paths(self.directory):
            raise CaptureIntegrityError("unfinished capture tail requires recovery")

    def append(self, record: CaptureRecord, encoded_line: bytes) -> None:
        if record.plan_sha256 != self.plan_sha256:
            raise CaptureIntegrityError("record plan hash differs from writer plan")
        if record.process_boot_id != self.process_boot_id:
            raise CaptureIntegrityError("record process boot differs from writer")
        if encoded_line != record_to_json_line(record):
            raise CaptureIntegrityError("queued bytes do not match capture record")
        if len(encoded_line) > self.maximum_uncompressed_bytes:
            raise CaptureStorageCapacityError(
                "single capture record exceeds the segment byte limit"
            )
        received_at_ms = _record_received_at_ms(record)
        ingest_seq = record.ingest_seq
        if self._last_received_at_ms is not None and received_at_ms < self._last_received_at_ms:
            raise CaptureIntegrityError("capture receipt time moved backwards")
        if self._last_ingest_seq is not None and ingest_seq != self._last_ingest_seq + 1:
            raise CaptureIntegrityError("capture ingest sequence is not contiguous")
        bucket_start_ms = received_at_ms - (received_at_ms % self.rotation_interval_ms)
        is_frame = isinstance(record, CaptureEnvelopeV1)
        if self._must_rotate(bucket_start_ms, len(encoded_line), is_frame):
            self._finalize_current(recovered_from_partial=False)
        if self._handle is None:
            self._open_segment(bucket_start_ms)
        storage_frame = _encode_outer_frame(self._compressor, encoded_line)
        self._ensure_data_capacity(len(storage_frame))
        assert self._handle is not None
        _write_all(self._handle, storage_frame)
        self._known_disk_bytes += len(storage_frame)
        self._record_count += 1
        self._frame_count += int(is_frame)
        self._uncompressed_bytes += len(encoded_line)
        if self._first_received_at_ms is None:
            self._first_received_at_ms = received_at_ms
            self._first_ingest_seq = ingest_seq
        self._last_received_at_ms = received_at_ms
        self._last_ingest_seq = ingest_seq

    def close(self) -> None:
        if self._handle is not None:
            self._finalize_current(recovered_from_partial=False)

    def abort(self) -> None:
        """Durably close the current partial without claiming it is finalized."""

        if self._handle is None:
            return
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.close()
        self._handle = None

    def write_emergency_transition(self, transition: CoverageTransitionV1) -> None:
        """Best-effort reserved-space journal for failures of the primary writer."""

        encoded = record_to_json_line(transition)
        if self._known_disk_bytes + len(encoded) > self.maximum_total_bytes:
            raise CaptureStorageCapacityError("emergency coverage reserve is exhausted")
        path = self.directory / "coverage-fatal.jsonl"
        with path.open("ab", buffering=0) as handle:
            _write_all(handle, encoded)
            os.fsync(handle.fileno())
        self._known_disk_bytes += len(encoded)

    def _must_rotate(
        self,
        bucket_start_ms: int,
        next_uncompressed_bytes: int,
        next_is_frame: bool,
    ) -> bool:
        if self._handle is None:
            return False
        return (
            bucket_start_ms != self._bucket_start_ms
            or self._uncompressed_bytes + next_uncompressed_bytes
            > self.maximum_uncompressed_bytes
            or (next_is_frame and self._frame_count >= self.maximum_frames)
        )

    def _open_segment(self, bucket_start_ms: int) -> None:
        name = f"{bucket_start_ms:013d}-{self._next_sequence:08d}.jsonl.zst.partial"
        path = self.directory / name
        self._handle = path.open("xb", buffering=0)
        self._partial_path = path
        self._bucket_start_ms = bucket_start_ms
        self._record_count = 0
        self._frame_count = 0
        self._uncompressed_bytes = 0
        self._first_received_at_ms = None
        self._first_ingest_seq = None
        self._segment_process_boot_id = self.process_boot_id

    def _finalize_current(self, *, recovered_from_partial: bool) -> None:
        if self._handle is None or self._partial_path is None:
            return
        if self._record_count < 1:
            raise CaptureIntegrityError("cannot finalize an empty capture segment")
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.close()
        self._handle = None
        final_path = self._partial_path.with_suffix("")
        os.replace(self._partial_path, final_path)
        _fsync_parent(final_path)
        _fsync_path(final_path)
        assert self._bucket_start_ms is not None
        assert self._first_received_at_ms is not None
        assert self._last_received_at_ms is not None
        assert self._first_ingest_seq is not None
        assert self._last_ingest_seq is not None
        assert self._segment_process_boot_id is not None
        finalized_process_boot_id = self._segment_process_boot_id
        digest = _sha256_file(final_path)
        manifest = SegmentManifestV1(
            data_file=final_path.name,
            sequence=self._next_sequence,
            bucket_start_ms=self._bucket_start_ms,
            rotation_interval_ms=self.rotation_interval_ms,
            plan_sha256=self.plan_sha256,
            process_boot_id=finalized_process_boot_id,
            first_received_at_ms=self._first_received_at_ms,
            last_received_at_ms=self._last_received_at_ms,
            first_ingest_seq=self._first_ingest_seq,
            last_ingest_seq=self._last_ingest_seq,
            record_count=self._record_count,
            frame_count=self._frame_count,
            uncompressed_bytes=self._uncompressed_bytes,
            compressed_bytes=final_path.stat().st_size,
            sha256=digest,
            previous_segment_sha256=self._previous_segment_sha256,
            recovered_from_partial=recovered_from_partial,
            frame_format_version=_OUTER_FRAME_FORMAT_VERSION,
        )
        self._write_manifest(manifest, _manifest_path(final_path))
        self._previous_segment_sha256 = digest
        self._next_sequence += 1
        self._partial_path = None
        self._bucket_start_ms = None
        self._record_count = 0
        self._frame_count = 0
        self._uncompressed_bytes = 0
        self._first_received_at_ms = None
        self._first_ingest_seq = None
        self._segment_process_boot_id = None
        if finalized_process_boot_id != self.process_boot_id:
            self._last_ingest_seq = None

    def _write_manifest(self, manifest: SegmentManifestV1, path: Path) -> None:
        encoded = (
            json.dumps(
                asdict(manifest),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            + b"\n"
        )
        self._ensure_data_capacity(len(encoded))
        temporary = path.with_suffix(path.suffix + ".partial")
        with temporary.open("xb", buffering=0) as handle:
            _write_all(handle, encoded)
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_parent(path)
        _fsync_path(path)
        self._known_disk_bytes += len(encoded)

    def _ensure_data_capacity(self, next_bytes: int) -> None:
        if (
            self._known_disk_bytes + next_bytes + self.emergency_reserve_bytes
            > self.maximum_total_bytes
        ):
            raise CaptureStorageCapacityError(
                "capture write would consume the reserved fail-closed disk budget"
            )

    def _directory_size(self) -> int:
        return sum(path.stat().st_size for path in self.directory.rglob("*") if path.is_file())

    def _recover_unfinished_tail(self) -> None:
        unfinished = _unfinished_segment_paths(self.directory)
        for path in unfinished:
            sequence = _segment_sequence(path)
            if sequence != self._next_sequence:
                raise CaptureIntegrityError(
                    "only a contiguous unfinished tail can be recovered"
                )
            if path.name.endswith(_PARTIAL_SUFFIX):
                lines, torn_final = _read_complete_zstd_frames(path)
                if not lines:
                    raise CaptureIntegrityError("capture partial has no complete record")
                metadata = _metadata_from_lines(
                    lines,
                    expected_plan_sha256=self.plan_sha256,
                )
                self._rewrite_partial(path, lines)
                self._adopt_recovered_partial(path, metadata)
                self._finalize_current(recovered_from_partial=True)
                if torn_final:
                    continue
            else:
                metadata, torn_final = _metadata_from_segment(
                    path,
                    expected_plan_sha256=self.plan_sha256,
                )
                if torn_final:
                    raise CaptureIntegrityError("finalized orphan contains a torn zstd frame")
                assert metadata is not None
                self._seal_orphan_final(path, metadata)

    def _rewrite_partial(self, path: Path, lines: list[bytes]) -> None:
        temporary = path.with_suffix(path.suffix + ".recovery")
        temporary_size = 0
        try:
            with temporary.open("xb", buffering=0) as handle:
                for line in lines:
                    storage_frame = _encode_outer_frame(self._compressor, line)
                    if (
                        self._known_disk_bytes
                        + temporary_size
                        + len(storage_frame)
                        + self.emergency_reserve_bytes
                        > self.maximum_total_bytes
                    ):
                        raise CaptureStorageCapacityError(
                            "partial recovery would exceed the transient disk quota"
                        )
                    _write_all(handle, storage_frame)
                    temporary_size += len(storage_frame)
                os.fsync(handle.fileno())
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        old_size = path.stat().st_size
        new_size = temporary.stat().st_size
        os.replace(temporary, path)
        _fsync_parent(path)
        _fsync_path(path)
        self._known_disk_bytes += new_size - old_size

    def _adopt_recovered_partial(
        self,
        path: Path,
        metadata: _RecoveredMetadata,
    ) -> None:
        bucket_start_ms, sequence = _segment_identity(path)
        if sequence != self._next_sequence:
            raise CaptureIntegrityError("recovered partial sequence mismatch")
        self._partial_path = path
        self._bucket_start_ms = bucket_start_ms
        self._handle = path.open("ab", buffering=0)
        self._record_count = metadata.record_count
        self._frame_count = metadata.frame_count
        self._uncompressed_bytes = metadata.uncompressed_bytes
        self._first_received_at_ms = metadata.first_received_at_ms
        self._last_received_at_ms = metadata.last_received_at_ms
        self._first_ingest_seq = metadata.first_ingest_seq
        self._last_ingest_seq = metadata.last_ingest_seq
        self._segment_process_boot_id = metadata.process_boot_id

    def _seal_orphan_final(self, path: Path, metadata: _RecoveredMetadata) -> None:
        bucket_start_ms, sequence = _segment_identity(path)
        digest = _sha256_file(path)
        manifest = SegmentManifestV1(
            data_file=path.name,
            sequence=sequence,
            bucket_start_ms=bucket_start_ms,
            rotation_interval_ms=self.rotation_interval_ms,
            plan_sha256=self.plan_sha256,
            process_boot_id=metadata.process_boot_id,
            first_received_at_ms=metadata.first_received_at_ms,
            last_received_at_ms=metadata.last_received_at_ms,
            first_ingest_seq=metadata.first_ingest_seq,
            last_ingest_seq=metadata.last_ingest_seq,
            record_count=metadata.record_count,
            frame_count=metadata.frame_count,
            uncompressed_bytes=metadata.uncompressed_bytes,
            compressed_bytes=path.stat().st_size,
            sha256=digest,
            previous_segment_sha256=self._previous_segment_sha256,
            recovered_from_partial=True,
            frame_format_version=_OUTER_FRAME_FORMAT_VERSION,
        )
        self._write_manifest(manifest, _manifest_path(path))
        self._previous_segment_sha256 = digest
        self._next_sequence += 1
        self._last_received_at_ms = metadata.last_received_at_ms
        self._last_ingest_seq = (
            metadata.last_ingest_seq
            if metadata.process_boot_id == self.process_boot_id
            else None
        )

    def _reject_auxiliary_crash_residue(self) -> None:
        residues = [
            *self.directory.glob("*.manifest.json.partial"),
            *self.directory.glob("*.partial.recovery"),
        ]
        if residues:
            names = ", ".join(sorted(path.name for path in residues))
            raise CaptureIntegrityError(
                f"ambiguous capture auxiliary crash residue requires audit: {names}"
            )


def verify_capture_segments(
    directory: str | Path,
    *,
    expected_plan_sha256: str | None = None,
    expected_process_boot_id: str | None = None,
    allow_unfinished_tail: bool = False,
) -> list[SegmentManifestV1]:
    """Verify hashes, chain, schemas, metadata, and cross-segment ordering."""

    root = Path(directory)
    fatal_journal = root / "coverage-fatal.jsonl"
    if fatal_journal.exists():
        _validate_fatal_journal(fatal_journal)
        raise CaptureIntegrityError("fatal coverage journal marks capture data invalid")
    manifests: list[SegmentManifestV1] = []
    for path in sorted(root.glob(f"*.jsonl.zst{_MANIFEST_SUFFIX}")):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            manifest = SegmentManifestV1(**raw)
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            raise CaptureIntegrityError(f"invalid segment manifest: {path.name}") from exc
        if manifest.schema_version != "capture_segment_manifest_v1":
            raise CaptureIntegrityError("unsupported capture segment manifest schema")
        if (
            type(manifest.frame_format_version) is not int
            or manifest.frame_format_version != _OUTER_FRAME_FORMAT_VERSION
        ):
            raise CaptureIntegrityError("unsupported capture outer-frame format version")
        manifests.append(manifest)
    manifests.sort(key=lambda item: item.sequence)
    previous: str | None = None
    previous_manifest: SegmentManifestV1 | None = None
    seen_data_files: set[str] = set()
    for expected_sequence, manifest in enumerate(manifests, start=1):
        if manifest.sequence != expected_sequence:
            raise CaptureIntegrityError("capture segment sequence is not contiguous")
        if expected_sequence == 1 and manifest.first_ingest_seq != 1:
            raise CaptureIntegrityError(
                "capture evidence is missing its initial ingest-sequence prefix"
            )
        if manifest.previous_segment_sha256 != previous:
            raise CaptureIntegrityError("capture segment previous-hash chain is broken")
        if expected_plan_sha256 is not None and manifest.plan_sha256 != expected_plan_sha256:
            raise CaptureIntegrityError("capture segment plan hash differs from expected")
        if (
            expected_process_boot_id is not None
            and manifest.process_boot_id != expected_process_boot_id
        ):
            raise CaptureIntegrityError("capture segment process boot differs from expected")
        if Path(manifest.data_file).name != manifest.data_file:
            raise CaptureIntegrityError("capture manifest data filename is not local")
        if manifest.data_file in seen_data_files:
            raise CaptureIntegrityError("capture manifests reuse a segment data file")
        seen_data_files.add(manifest.data_file)
        data_path = root / manifest.data_file
        bucket_start_ms, filename_sequence = _segment_identity(data_path)
        if filename_sequence != manifest.sequence or bucket_start_ms != manifest.bucket_start_ms:
            raise CaptureIntegrityError("capture filename identity differs from manifest")
        if manifest.rotation_interval_ms < 1:
            raise CaptureIntegrityError("capture manifest rotation interval is invalid")
        if not data_path.is_file():
            raise CaptureIntegrityError("capture segment data file is missing")
        if data_path.stat().st_size != manifest.compressed_bytes:
            raise CaptureIntegrityError("capture segment compressed size differs from manifest")
        if _sha256_file(data_path) != manifest.sha256:
            raise CaptureIntegrityError("capture segment SHA-256 differs from manifest")
        metadata, torn_final = _metadata_from_segment(
            data_path,
            expected_plan_sha256=manifest.plan_sha256,
        )
        if torn_final:
            raise CaptureIntegrityError("finalized capture segment has a torn frame")
        assert metadata is not None
        if metadata.process_boot_id != manifest.process_boot_id:
            raise CaptureIntegrityError("capture record process boot differs from manifest")
        actual_metadata = (
            metadata.first_received_at_ms,
            metadata.last_received_at_ms,
            metadata.first_ingest_seq,
            metadata.last_ingest_seq,
            metadata.record_count,
            metadata.frame_count,
            metadata.uncompressed_bytes,
        )
        expected_metadata = (
            manifest.first_received_at_ms,
            manifest.last_received_at_ms,
            manifest.first_ingest_seq,
            manifest.last_ingest_seq,
            manifest.record_count,
            manifest.frame_count,
            manifest.uncompressed_bytes,
        )
        if actual_metadata != expected_metadata:
            raise CaptureIntegrityError("capture segment record metadata differs from manifest")
        if (
            manifest.first_received_at_ms
            - (manifest.first_received_at_ms % manifest.rotation_interval_ms)
            != manifest.bucket_start_ms
        ):
            raise CaptureIntegrityError("capture segment bucket differs from first receipt")
        if previous_manifest is not None:
            if manifest.first_received_at_ms < previous_manifest.last_received_at_ms:
                raise CaptureIntegrityError("capture receipt order crosses segment boundary")
            if (
                manifest.process_boot_id == previous_manifest.process_boot_id
                and manifest.first_ingest_seq != previous_manifest.last_ingest_seq + 1
            ):
                raise CaptureIntegrityError("capture ingest sequence has a segment-boundary gap")
        previous = manifest.sha256
        previous_manifest = manifest
    unfinished = _unfinished_segment_paths(root)
    if unfinished and not allow_unfinished_tail:
        raise CaptureIntegrityError("capture directory contains an unfinished segment tail")
    return manifests


def _validate_fatal_journal(path: Path) -> None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise CaptureIntegrityError("cannot read fatal coverage journal") from exc
    if not lines:
        raise CaptureIntegrityError("fatal coverage journal is empty or torn")
    for line in lines:
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise CaptureIntegrityError("fatal coverage journal contains invalid JSON") from exc
        if (
            not isinstance(record, dict)
            or record.get("schema_version") != "coverage_transition_v1"
            or record.get("state") != "invalid"
        ):
            raise CaptureIntegrityError("fatal coverage journal has an invalid record")


def read_segment_lines(path: str | Path) -> list[bytes]:
    """Decode complete independent frames for tests and streaming materializers."""

    lines, torn_final = _read_complete_zstd_frames(Path(path))
    if torn_final:
        raise CaptureIntegrityError("segment ends in a torn zstd frame")
    return lines


def consume_segment_lines(
    path: str | Path,
    consume: Callable[[bytes], None],
) -> None:
    """Verify and deliver one decoded JSONL frame at a time without segment buffering."""

    torn_final = _consume_complete_zstd_frames(Path(path), consume)
    if torn_final:
        raise CaptureIntegrityError("segment ends in a torn zstd frame")


def _record_received_at_ms(record: CaptureRecord) -> int:
    if isinstance(record, RestEnvelopeV1):
        return record.response_received_at_ms
    if isinstance(record, RestEnvelopeV2):
        return record.response_completed_at_ms
    return record.received_at_ms


def _metadata_from_lines(
    lines: Iterable[bytes],
    *,
    expected_plan_sha256: str,
) -> _RecoveredMetadata:
    accumulator = _MetadataAccumulator(expected_plan_sha256)
    for line in lines:
        accumulator.consume(line)
    return accumulator.finish()


def _metadata_from_segment(
    path: Path,
    *,
    expected_plan_sha256: str,
) -> tuple[_RecoveredMetadata | None, bool]:
    # Preserve the original fail-closed ordering: validate every storage frame
    # (and detect a torn tail) before interpreting any record as JSON. A second
    # streaming pass aggregates metadata without retaining decoded lines.
    torn_final = _consume_complete_zstd_frames(path, _discard_line)
    if torn_final:
        return None, True
    accumulator = _MetadataAccumulator(expected_plan_sha256)
    torn_final = _consume_complete_zstd_frames(path, accumulator.consume)
    if torn_final:
        raise CaptureIntegrityError("capture segment changed during verification")
    return accumulator.finish(), False


def _discard_line(_line: bytes) -> None:
    return


def _read_complete_zstd_frames(path: Path) -> tuple[list[bytes], bool]:
    lines: list[bytes] = []
    torn_final = _consume_complete_zstd_frames(path, lines.append)
    return lines, torn_final


def _consume_complete_zstd_frames(
    path: Path,
    consume: Callable[[bytes], None],
) -> bool:
    with path.open("rb") as handle:
        file_size = os.fstat(handle.fileno()).st_size
        while handle.tell() < file_size:
            remaining_size = file_size - handle.tell()
            if remaining_size < _OUTER_FRAME_HEADER_SIZE:
                raise CaptureIntegrityError(
                    "capture outer-frame header is incomplete or corrupt"
                )
            header = handle.read(_OUTER_FRAME_HEADER_SIZE)
            if len(header) != _OUTER_FRAME_HEADER_SIZE:
                raise CaptureIntegrityError(
                    "capture outer-frame header is incomplete or corrupt"
                )
            core = header[: _OUTER_FRAME_HEADER_CORE.size]
            stored_header_digest = header[_OUTER_FRAME_HEADER_CORE.size :]
            if not hmac.compare_digest(
                hashlib.sha256(core).digest(),
                stored_header_digest,
            ):
                raise CaptureIntegrityError(
                    "capture outer-frame header digest mismatch"
                )
            (
                magic,
                version,
                compressed_length,
                uncompressed_length,
                compressed_sha256,
            ) = _OUTER_FRAME_HEADER_CORE.unpack(core)
            if magic != _OUTER_FRAME_MAGIC:
                raise CaptureIntegrityError("capture outer-frame magic is invalid")
            if version != _OUTER_FRAME_FORMAT_VERSION:
                raise CaptureIntegrityError(
                    "capture outer-frame format version is unsupported"
                )
            if compressed_length < 1 or uncompressed_length < 1:
                raise CaptureIntegrityError("capture outer-frame lengths are invalid")
            payload_start = handle.tell()
            available_payload_bytes = file_size - payload_start
            if compressed_length > available_payload_bytes:
                # Only a fully integrity-checked header followed by a short payload is a
                # recoverable torn tail. Partial headers are deliberately fatal. Compare
                # against the actual file extent before any length-directed read.
                return True
            payload_digest, frame_header = _hash_payload(
                handle,
                compressed_length,
            )
            if not hmac.compare_digest(payload_digest, compressed_sha256):
                raise CaptureIntegrityError(
                    "capture outer-frame compressed SHA-256 mismatch"
                )
            try:
                frame_parameters = zstd.get_frame_parameters(frame_header)
            except zstd.ZstdError as exc:
                raise CaptureIntegrityError(
                    "inner zstd frame header is invalid"
                ) from exc
            if not frame_parameters.has_checksum:
                raise CaptureIntegrityError("inner zstd frame has no checksum")
            if frame_parameters.content_size != uncompressed_length:
                raise CaptureIntegrityError(
                    "inner zstd content size differs from outer frame"
                )
            handle.seek(payload_start)
            decoded = _decompress_payload(
                handle,
                compressed_length=compressed_length,
                uncompressed_length=uncompressed_length,
            )
            consume(decoded)
        return False


def _hash_payload(handle: BinaryIO, length: int) -> tuple[bytes, bytes]:
    digest = hashlib.sha256()
    frame_header = bytearray()
    remaining = length
    while remaining:
        chunk = handle.read(min(_STREAM_READ_SIZE, remaining))
        if not chunk:
            raise CaptureIntegrityError(
                "capture outer-frame payload changed while being verified"
            )
        digest.update(chunk)
        if len(frame_header) < _ZSTD_FRAME_HEADER_MAX_SIZE:
            needed = _ZSTD_FRAME_HEADER_MAX_SIZE - len(frame_header)
            frame_header.extend(chunk[:needed])
        remaining -= len(chunk)
    return digest.digest(), bytes(frame_header)


def _decompress_payload(
    handle: BinaryIO,
    *,
    compressed_length: int,
    uncompressed_length: int,
) -> bytes:
    decompressor = zstd.ZstdDecompressor().decompressobj()
    decoded = bytearray()
    remaining = compressed_length
    while remaining:
        chunk = handle.read(min(_STREAM_READ_SIZE, remaining))
        if not chunk:
            raise CaptureIntegrityError(
                "complete outer frame contains a torn inner zstd frame"
            )
        try:
            output = decompressor.decompress(chunk)
        except zstd.ZstdError as exc:
            raise CaptureIntegrityError(
                "inner zstd checksum or payload corruption is not repairable"
            ) from exc
        remaining -= len(chunk)
        if len(decoded) + len(output) > uncompressed_length:
            raise CaptureIntegrityError(
                "capture outer-frame uncompressed length mismatch"
            )
        decoded.extend(output)
        if decompressor.unused_data or (decompressor.eof and remaining):
            raise CaptureIntegrityError(
                "outer-frame payload contains trailing compressed data"
            )
    if not decompressor.eof:
        raise CaptureIntegrityError(
            "complete outer frame contains a torn inner zstd frame"
        )
    if len(decoded) != uncompressed_length:
        raise CaptureIntegrityError(
            "capture outer-frame uncompressed length mismatch"
        )
    if not decoded.endswith(b"\n") or decoded.count(b"\n") != 1:
        raise CaptureIntegrityError("zstd frame is not exactly one JSONL record")
    return bytes(decoded)


def _encode_outer_frame(
    compressor: zstd.ZstdCompressor,
    decoded: bytes,
) -> bytes:
    compressed = compressor.compress(decoded)
    core = _OUTER_FRAME_HEADER_CORE.pack(
        _OUTER_FRAME_MAGIC,
        _OUTER_FRAME_FORMAT_VERSION,
        len(compressed),
        len(decoded),
        hashlib.sha256(compressed).digest(),
    )
    return core + hashlib.sha256(core).digest() + compressed


def _segment_sequence(path: Path) -> int:
    return _segment_identity(path)[1]


def _unfinished_segment_paths(directory: Path) -> list[Path]:
    candidates = [
        *directory.glob("*.jsonl.zst"),
        *directory.glob(f"*{_PARTIAL_SUFFIX}"),
    ]
    return sorted(
        (path for path in candidates if not _manifest_path(path).exists()),
        key=_segment_sequence,
    )


def _segment_identity(path: Path) -> tuple[int, int]:
    name = path.name.removesuffix(".partial")
    match = _SEGMENT_RE.fullmatch(name)
    if match is None:
        raise CaptureIntegrityError(f"invalid capture segment filename: {path.name}")
    return int(match.group("bucket")), int(match.group("sequence"))


def _manifest_path(data_path: Path) -> Path:
    name = data_path.name.removesuffix(".partial")
    return data_path.with_name(name + _MANIFEST_SUFFIX)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_all(handle: BinaryIO, payload: bytes) -> None:
    written = handle.write(payload)
    if written != len(payload):
        raise CaptureShortWriteError(
            f"capture short write: expected {len(payload)} bytes, wrote {written}"
        )


def _fsync_path(path: Path) -> None:
    # Windows requires a writable descriptor for FlushFileBuffers/fsync.
    with path.open("rb+") as handle:
        os.fsync(handle.fileno())


def _fsync_parent(path: Path) -> None:
    # Windows lacks a portable directory-fsync primitive.  POSIX renames are
    # made durable by syncing the parent after every atomic replacement.
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
