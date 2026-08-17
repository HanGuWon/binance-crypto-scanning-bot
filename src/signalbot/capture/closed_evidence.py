"""Verified, streaming access to a closed capture evidence set.

The named authority and segment paths are verified immediately before and
after streaming.  Descriptors are not pinned across that whole interval;
defending against a privileged hostile swap-and-restore is a separate residual
from this fail-closed named-path boundary.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel, TypeAdapter, ValidationError

from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CaptureRecord,
    ConnectionTransitionV1,
    CoverageTransitionV1,
    RestEnvelopeV1,
    RestEnvelopeV2,
    record_to_json_line,
)
from signalbot.capture.path_safety import inspect_link_free_path
from signalbot.capture.provenance import ExternalAuditRecordV1, canonical_json_bytes
from signalbot.capture.session import SessionClosureV1, SessionStartV1
from signalbot.capture.storage import (
    SegmentManifestV1,
    consume_segment_lines,
    verify_capture_segments,
)

_AUTHORITY_DOCUMENT_MAXIMUM_BYTES = 2 * 1024 * 1024
_SOURCE_MANIFEST_MAXIMUM_BYTES = 16 * 1024 * 1024
_MANIFEST_ADAPTER = TypeAdapter(SegmentManifestV1)
_RECORD_ADAPTERS: dict[str, TypeAdapter[Any]] = {
    "capture_envelope_v1": TypeAdapter(CaptureEnvelopeV1),
    "rest_envelope_v1": TypeAdapter(RestEnvelopeV1),
    "rest_envelope_v2": TypeAdapter(RestEnvelopeV2),
    "connection_transition_v1": TypeAdapter(ConnectionTransitionV1),
    "coverage_transition_v1": TypeAdapter(CoverageTransitionV1),
}


@dataclass(frozen=True, slots=True)
class ClosedCaptureAuthority:
    """Exact closed-session, external-audit, and segment authority."""

    start: SessionStartV1
    closure: SessionClosureV1
    start_path: Path
    closure_path: Path
    output_root: Path
    source_manifest_path: Path
    source_manifest_sha256: str
    external_audit_root: Path
    external_start: ExternalAuditRecordV1
    external_closure: ExternalAuditRecordV1
    external_start_path: Path
    external_closure_path: Path
    capture_directory: Path
    manifests: tuple[SegmentManifestV1, ...]
    segment_data_paths: tuple[Path, ...]
    segment_manifest_paths: tuple[Path, ...]
    segment_manifest_sha256s: tuple[str, ...]
    start_sha256: str
    closure_sha256: str
    external_start_sha256: str
    external_closure_sha256: str


def verify_closed_capture_authority(
    *,
    start_path: str | Path,
    closure_path: str | Path,
    capture_directory: str | Path,
) -> ClosedCaptureAuthority:
    """Verify and return the complete named-path authority for a closed capture."""

    start, start_payload, start_resolved = _read_canonical_model(
        start_path,
        SessionStartV1,
        "session start",
    )
    closure, closure_payload, closure_resolved = _read_canonical_model(
        closure_path,
        SessionClosureV1,
        "session closure",
    )
    output = _require_real_directory(start.output_root, "session output root")
    if str(output) != start.output_root:
        raise CaptureIntegrityError("session output root is not its exact declared path")
    source_manifest_path, source_manifest_sha256 = _verify_source_manifest(output, start)
    expected_start = output / f"{start.session_id}.start.session.json"
    expected_closure = output / f"{start.session_id}.closure.session.json"
    if start_resolved != expected_start or closure_resolved != expected_closure:
        raise CaptureIntegrityError("session document path differs from its declared authority")

    start_sha256 = hashlib.sha256(start_payload).hexdigest()
    closure_sha256 = hashlib.sha256(closure_payload).hexdigest()
    bindings = (
        closure.session_id == start.session_id,
        closure.process_boot_id == start.process_boot_id,
        closure.protocol_sha256 == start.protocol_sha256,
        closure.source_manifest_sha256 == start.source_manifest_sha256,
        closure.config_sha256 == start.config_sha256,
        closure.capture_plan_sha256 == start.capture_plan_sha256,
        closure.start_document_sha256 == start_sha256,
        closure.output_root == start.output_root,
        closure.external_audit_root == start.external_audit_root,
        closure.external_audit_trust_classification
        == start.external_audit_trust_classification,
        closure.closed_at_ms >= start.started_at_ms,
        closure.closed_monotonic_ns >= start.started_monotonic_ns,
    )
    if not all(bindings):
        raise CaptureIntegrityError("session closure does not bind the canonical start")

    external = _require_real_directory(start.external_audit_root, "external audit root")
    if str(external) != start.external_audit_root:
        raise CaptureIntegrityError("external audit root is not its exact declared path")
    external_start, external_start_payload, external_start_path = _read_canonical_model(
        external / f"{start.session_id}.start.audit-head.json",
        ExternalAuditRecordV1,
        "external start audit head",
    )
    external_closure, external_closure_payload, external_closure_path = (
        _read_canonical_model(
            external / f"{start.session_id}.closure.audit-head.json",
            ExternalAuditRecordV1,
            "external closure audit head",
        )
    )
    if external_start_path.parent != external or external_closure_path.parent != external:
        raise CaptureIntegrityError("external audit head escapes its declared root")
    external_start_sha256 = hashlib.sha256(external_start_payload).hexdigest()
    external_closure_sha256 = hashlib.sha256(external_closure_payload).hexdigest()
    audit_bindings = (
        external_start.phase == "start",
        external_closure.phase == "closure",
        external_start.session_id == start.session_id,
        external_closure.session_id == start.session_id,
        external_start.protocol_sha256 == start.protocol_sha256,
        external_closure.protocol_sha256 == start.protocol_sha256,
        external_start.source_manifest_sha256 == start.source_manifest_sha256,
        external_closure.source_manifest_sha256 == start.source_manifest_sha256,
        external_start.subject_sha256 == start_sha256,
        external_closure.subject_sha256 == closure_sha256,
        external_start.previous_record_sha256 is None,
        external_closure.previous_record_sha256 == external_start_sha256,
        external_start.recorded_at_ms <= external_closure.recorded_at_ms,
    )
    if not all(audit_bindings):
        raise CaptureIntegrityError("external audit head subject or SHA chain is invalid")

    (
        capture,
        manifests,
        data_paths,
        manifest_paths,
        manifest_sha256s,
    ) = _verify_capture_authority(start, closure, capture_directory)
    return ClosedCaptureAuthority(
        start=start,
        closure=closure,
        start_path=start_resolved,
        closure_path=closure_resolved,
        output_root=output,
        source_manifest_path=source_manifest_path,
        source_manifest_sha256=source_manifest_sha256,
        external_audit_root=external,
        external_start=external_start,
        external_closure=external_closure,
        external_start_path=external_start_path,
        external_closure_path=external_closure_path,
        capture_directory=capture,
        manifests=manifests,
        segment_data_paths=data_paths,
        segment_manifest_paths=manifest_paths,
        segment_manifest_sha256s=manifest_sha256s,
        start_sha256=start_sha256,
        closure_sha256=closure_sha256,
        external_start_sha256=external_start_sha256,
        external_closure_sha256=external_closure_sha256,
    )


def consume_closed_capture_records(
    authority: ClosedCaptureAuthority,
    consume: Callable[[CaptureRecord], None],
) -> None:
    """Reverify, stream strict canonical records, then reverify exact authority.

    Records are decoded and delivered one at a time.  They are never collected
    into an in-memory segment or capture-sized container by this boundary.
    """

    before = verify_closed_capture_authority(
        start_path=authority.start_path,
        closure_path=authority.closure_path,
        capture_directory=authority.capture_directory,
    )
    if before != authority:
        raise CaptureIntegrityError("closed capture authority changed before consumption")

    def decode_and_consume(line: bytes) -> None:
        record = _decode_record(line)
        if (
            record.plan_sha256 != before.start.capture_plan_sha256
            or record.process_boot_id != before.start.process_boot_id
        ):
            raise CaptureIntegrityError("capture record differs from the closed authority")
        consume(record)

    try:
        for data_path in before.segment_data_paths:
            consume_segment_lines(data_path, decode_and_consume)
    finally:
        after = verify_closed_capture_authority(
            start_path=before.start_path,
            closure_path=before.closure_path,
            capture_directory=before.capture_directory,
        )
        if after != before:
            raise CaptureIntegrityError("closed capture authority changed during consumption")


def _verify_capture_authority(
    start: SessionStartV1,
    closure: SessionClosureV1,
    capture_directory: str | Path,
) -> tuple[
    Path,
    tuple[SegmentManifestV1, ...],
    tuple[Path, ...],
    tuple[Path, ...],
    tuple[str, ...],
]:
    output = _require_real_directory(start.output_root, "session output root")
    capture = _require_real_directory(capture_directory, "capture directory")
    if not capture.is_relative_to(output):
        raise CaptureIntegrityError("capture directory escapes the session output root")
    if closure.capture_chain.capture_directory != str(capture):
        raise CaptureIntegrityError("capture directory differs from the closure authority")
    verified_manifests = verify_capture_segments(
        capture,
        expected_plan_sha256=start.capture_plan_sha256,
        expected_process_boot_id=start.process_boot_id,
    )

    data_paths: list[Path] = []
    manifest_paths: list[Path] = []
    manifest_sha256s: list[str] = []
    for manifest in verified_manifests:
        data_path = _require_real_file(capture / manifest.data_file, "capture segment data")
        manifest_path = _require_real_file(
            capture / f"{manifest.data_file}.manifest.json",
            "capture segment manifest",
        )
        if data_path.parent != capture or manifest_path.parent != capture:
            raise CaptureIntegrityError("capture segment path escapes its declared directory")
        canonical_manifest, manifest_payload, canonical_manifest_path = (
            _read_canonical_manifest(manifest_path)
        )
        if canonical_manifest != manifest or canonical_manifest_path != manifest_path:
            raise CaptureIntegrityError("capture segment manifest changed during verification")
        data_paths.append(data_path)
        manifest_paths.append(manifest_path)
        manifest_sha256s.append(hashlib.sha256(manifest_payload).hexdigest())

    manifests = tuple(verified_manifests)
    if manifests:
        final = manifests[-1]
        final_manifest_sha256 = manifest_sha256s[-1]
        first_receipt = manifests[0].first_received_at_ms
        last_receipt = final.last_received_at_ms
        final_data_sha256 = final.sha256
    else:
        final_manifest_sha256 = None
        first_receipt = None
        last_receipt = None
        final_data_sha256 = None
    expected = {
        "schema_version": "capture_chain_summary_v1",
        "capture_directory": str(capture),
        "segment_count": len(manifests),
        "record_count": sum(item.record_count for item in manifests),
        "first_receipt_at_ms": first_receipt,
        "last_receipt_at_ms": last_receipt,
        "final_manifest_sha256": final_manifest_sha256,
        "final_data_sha256": final_data_sha256,
    }
    if closure.capture_chain.model_dump(mode="json") != expected:
        raise CaptureIntegrityError("session closure capture chain is stale or invalid")
    return (
        capture,
        manifests,
        tuple(data_paths),
        tuple(manifest_paths),
        tuple(manifest_sha256s),
    )


ModelT = TypeVar("ModelT", bound=BaseModel)


def _read_canonical_model(  # noqa: UP047 - host compileall may use Python <3.12
    path: str | Path,
    model_type: type[ModelT],
    field: str,
) -> tuple[ModelT, bytes, Path]:
    payload, resolved = _read_stable_file(path, field)
    if not payload.endswith(b"\n") or payload.count(b"\n") != 1:
        raise CaptureIntegrityError(f"{field} must be exactly one JSON line")
    try:
        raw = json.loads(payload)
        model = model_type.model_validate(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise CaptureIntegrityError(f"{field} is invalid") from exc
    if payload != canonical_json_bytes(model.model_dump(mode="json")) + b"\n":
        raise CaptureIntegrityError(f"{field} is not canonical")
    return model, payload, resolved


def _read_canonical_manifest(
    path: str | Path,
) -> tuple[SegmentManifestV1, bytes, Path]:
    payload, resolved = _read_stable_file(path, "capture segment manifest")
    if not payload.endswith(b"\n") or payload.count(b"\n") != 1:
        raise CaptureIntegrityError("capture segment manifest must be exactly one JSON line")
    try:
        manifest = _MANIFEST_ADAPTER.validate_json(payload, strict=True)
    except ValidationError as exc:
        raise CaptureIntegrityError("capture segment manifest is invalid") from exc
    if payload != canonical_json_bytes(asdict(manifest)) + b"\n":
        raise CaptureIntegrityError("capture segment manifest is not canonical")
    return manifest, payload, resolved


def _verify_source_manifest(
    output: Path,
    start: SessionStartV1,
) -> tuple[Path, str]:
    expected_path = output / "capture-source-manifest.json"
    payload, resolved = _read_stable_file(
        expected_path,
        "capture source manifest",
        maximum_bytes=_SOURCE_MANIFEST_MAXIMUM_BYTES,
    )
    if resolved != expected_path or resolved.parent != output:
        raise CaptureIntegrityError("capture source manifest path differs from its authority")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CaptureIntegrityError("capture source manifest is invalid JSON") from exc
    if not isinstance(document, dict):
        raise CaptureIntegrityError("capture source manifest must be a JSON object")
    try:
        canonical_payload = canonical_json_bytes(document)
    except (TypeError, ValueError) as exc:
        raise CaptureIntegrityError("capture source manifest is invalid") from exc
    if payload != canonical_payload:
        raise CaptureIntegrityError("capture source manifest is not canonical")

    digest = hashlib.sha256(payload).hexdigest()
    protocol = document.get("protocol")
    configuration = document.get("configuration")
    bindings = (
        document.get("schema_version") == "capture_source_manifest_v1",
        document.get("purpose") == "infrastructure_only",
        isinstance(protocol, dict),
        isinstance(configuration, dict),
        digest == start.source_manifest_sha256,
        isinstance(protocol, dict) and protocol.get("sha256") == start.protocol_sha256,
        isinstance(configuration, dict)
        and configuration.get("sha256") == start.config_sha256,
    )
    if not all(bindings):
        raise CaptureIntegrityError("capture source manifest bindings are invalid")
    return resolved, digest


def _read_stable_file(
    path: str | Path,
    field: str,
    *,
    maximum_bytes: int = _AUTHORITY_DOCUMENT_MAXIMUM_BYTES,
) -> tuple[bytes, Path]:
    inspection = inspect_link_free_path(path, field)
    status = inspection.final_status
    if status is None or not stat.S_ISREG(status.st_mode):
        raise CaptureIntegrityError(f"{field} must be an existing regular file")
    resolved = inspection.absolute_path.resolve(strict=True)
    if status.st_size > maximum_bytes:
        raise CaptureIntegrityError(f"{field} exceeds its size limit")
    before_signature = _stat_signature(status)
    with resolved.open("rb") as handle:
        payload = handle.read(maximum_bytes + 1)
        descriptor_signature = _stat_signature(os.fstat(handle.fileno()))
    after_inspection = inspect_link_free_path(path, field)
    after_status = after_inspection.final_status
    if (
        after_status is None
        or after_inspection.absolute_path.resolve(strict=True) != resolved
        or _stat_signature(after_status) != before_signature
        or descriptor_signature != before_signature
        or len(payload) != status.st_size
    ):
        raise CaptureIntegrityError(f"{field} changed while it was read")
    return payload, resolved


def _decode_record(line: bytes) -> CaptureRecord:
    try:
        raw = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CaptureIntegrityError("capture record is not valid JSON") from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("schema_version"), str):
        raise CaptureIntegrityError("capture record lacks a schema version")
    adapter = _RECORD_ADAPTERS.get(raw["schema_version"])
    if adapter is None:
        raise CaptureIntegrityError("capture record schema is not reportable")
    try:
        record: Any = adapter.validate_json(line, strict=True)
    except ValidationError as exc:
        raise CaptureIntegrityError("capture record violates its persisted schema") from exc
    if not isinstance(
        record,
        (
            CaptureEnvelopeV1,
            RestEnvelopeV1,
            RestEnvelopeV2,
            ConnectionTransitionV1,
            CoverageTransitionV1,
        ),
    ):
        raise CaptureIntegrityError("capture record adapter returned an unknown type")
    if record_to_json_line(record) != line:
        raise CaptureIntegrityError("capture record is not canonical or has extra fields")
    return record


def _require_real_directory(path: str | Path, field: str) -> Path:
    inspection = inspect_link_free_path(path, field)
    status = inspection.final_status
    if status is None or not stat.S_ISDIR(status.st_mode):
        raise CaptureIntegrityError(f"{field} must be an existing real directory")
    return inspection.absolute_path.resolve(strict=True)


def _require_real_file(path: str | Path, field: str) -> Path:
    inspection = inspect_link_free_path(path, field)
    status = inspection.final_status
    if status is None or not stat.S_ISREG(status.st_mode):
        raise CaptureIntegrityError(f"{field} must be an existing real file")
    return inspection.absolute_path.resolve(strict=True)


def _stat_signature(status: os.stat_result) -> tuple[int, int, int, int]:
    return (status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns)
