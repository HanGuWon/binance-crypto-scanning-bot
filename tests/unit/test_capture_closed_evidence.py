from __future__ import annotations

import hashlib
import json
import struct
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
import zstandard as zstd

from signalbot.capture.closed_evidence import (
    ClosedCaptureAuthority,
    consume_closed_capture_records,
    verify_closed_capture_authority,
)
from signalbot.capture.config import FROZEN_PROTOCOL_SHA256
from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.models import CaptureEnvelopeV1, CaptureRecord, record_to_json_line
from signalbot.capture.provenance import (
    ExternalAuditRecordV1,
    canonical_json_bytes,
    write_external_audit_record,
)
from signalbot.capture.session import (
    build_session_closure,
    build_session_start,
    write_session_closure,
    write_session_start,
)
from signalbot.capture.storage import (
    SegmentedCaptureWriter,
    read_segment_lines,
)
from signalbot.domain.enums import Market

_STARTED_AT_MS = 1_721_000_000_000
_STARTED_MONOTONIC_NS = 8_000_000_000
_BOOT_UUID = uuid.UUID("01234567-89ab-cdef-0123-456789abcdef")
_OUTER_FRAME_HEADER_CORE = struct.Struct(">8sBQQ32s")


@dataclass(frozen=True, slots=True)
class _ClosedEvidence:
    start_path: Path
    closure_path: Path
    capture_directory: Path


@dataclass(frozen=True, slots=True)
class _ManifestOverrides:
    schema_version: str = "capture_source_manifest_v1"
    purpose: str = "infrastructure_only"
    protocol_sha256: str = FROZEN_PROTOCOL_SHA256
    config_sha256: str = "b" * 64


def test_public_authority_streams_strict_typed_records(tmp_path: Path) -> None:
    evidence = _minimal_closed_evidence(tmp_path)

    authority = verify_closed_capture_authority(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    )
    records: list[CaptureRecord] = []
    consume_closed_capture_records(authority, records.append)

    assert authority.start_path == evidence.start_path
    assert authority.closure_path == evidence.closure_path
    assert authority.capture_directory == evidence.capture_directory
    assert authority.source_manifest_path == authority.output_root / (
        "capture-source-manifest.json"
    )
    assert authority.source_manifest_sha256 == authority.start.source_manifest_sha256
    assert len(authority.manifests) == 1
    assert len(authority.segment_manifest_sha256s) == 1
    assert authority.segment_manifest_sha256s[0] == hashlib.sha256(
        authority.segment_manifest_paths[0].read_bytes()
    ).hexdigest()
    assert len(records) == 1
    assert isinstance(records[0], CaptureEnvelopeV1)


def test_missing_source_manifest_is_rejected(tmp_path: Path) -> None:
    evidence = _minimal_closed_evidence(tmp_path)
    source_manifest = evidence.start_path.parent / "capture-source-manifest.json"
    source_manifest.unlink()

    with pytest.raises(ValueError, match="path component does not exist"):
        verify_closed_capture_authority(
            start_path=evidence.start_path,
            closure_path=evidence.closure_path,
            capture_directory=evidence.capture_directory,
        )


def test_mutated_source_manifest_sha_is_rejected(tmp_path: Path) -> None:
    evidence = _minimal_closed_evidence(tmp_path)
    source_manifest = evidence.start_path.parent / "capture-source-manifest.json"
    document = json.loads(source_manifest.read_bytes())
    document["mutation"] = True
    source_manifest.write_bytes(canonical_json_bytes(document))

    with pytest.raises(CaptureIntegrityError, match="bindings are invalid"):
        verify_closed_capture_authority(
            start_path=evidence.start_path,
            closure_path=evidence.closure_path,
            capture_directory=evidence.capture_directory,
        )


def test_noncanonical_source_manifest_is_rejected(tmp_path: Path) -> None:
    evidence = _minimal_closed_evidence(tmp_path)
    source_manifest = evidence.start_path.parent / "capture-source-manifest.json"
    document = json.loads(source_manifest.read_bytes())
    source_manifest.write_text(
        json.dumps(document, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    with pytest.raises(CaptureIntegrityError, match="source manifest is not canonical"):
        verify_closed_capture_authority(
            start_path=evidence.start_path,
            closure_path=evidence.closure_path,
            capture_directory=evidence.capture_directory,
        )


def test_symlinked_source_manifest_is_rejected_when_supported(tmp_path: Path) -> None:
    evidence = _minimal_closed_evidence(tmp_path)
    source_manifest = evidence.start_path.parent / "capture-source-manifest.json"
    target = tmp_path / "moved-capture-source-manifest.json"
    source_manifest.replace(target)
    try:
        source_manifest.symlink_to(target)
    except (NotImplementedError, OSError):
        pytest.skip("creating file symlinks is not permitted on this host")

    with pytest.raises(ValueError, match=r"symbolic-link|reparse-point"):
        verify_closed_capture_authority(
            start_path=evidence.start_path,
            closure_path=evidence.closure_path,
            capture_directory=evidence.capture_directory,
        )


@pytest.mark.parametrize(
    "overrides",
    [
        _ManifestOverrides(schema_version="wrong_source_manifest"),
        _ManifestOverrides(purpose="wrong_purpose"),
        _ManifestOverrides(protocol_sha256="e" * 64),
        _ManifestOverrides(config_sha256="f" * 64),
    ],
)
def test_source_manifest_binding_mismatch_is_rejected(
    tmp_path: Path,
    overrides: _ManifestOverrides,
) -> None:
    evidence = _minimal_closed_evidence(
        tmp_path,
        manifest_schema_version=overrides.schema_version,
        manifest_purpose=overrides.purpose,
        manifest_protocol_sha256=overrides.protocol_sha256,
        manifest_config_sha256=overrides.config_sha256,
    )

    with pytest.raises(CaptureIntegrityError, match="bindings are invalid"):
        verify_closed_capture_authority(
            start_path=evidence.start_path,
            closure_path=evidence.closure_path,
            capture_directory=evidence.capture_directory,
        )


def test_source_manifest_change_from_consumer_fails_post_reverification(
    tmp_path: Path,
) -> None:
    evidence = _minimal_closed_evidence(tmp_path)
    authority = verify_closed_capture_authority(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    )
    consumed = 0

    def mutate_source_manifest(_record: CaptureRecord) -> None:
        nonlocal consumed
        consumed += 1
        document = json.loads(authority.source_manifest_path.read_bytes())
        document["mutation"] = True
        authority.source_manifest_path.write_bytes(canonical_json_bytes(document))

    with pytest.raises(CaptureIntegrityError, match="bindings are invalid"):
        consume_closed_capture_records(authority, mutate_source_manifest)

    assert consumed == 1


def test_authority_mutation_before_consume_fails_pre_reverification(tmp_path: Path) -> None:
    evidence = _minimal_closed_evidence(tmp_path)
    authority = verify_closed_capture_authority(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    )
    _replace_with_valid_changed_closure(authority)

    with pytest.raises(CaptureIntegrityError, match="changed before consumption"):
        consume_closed_capture_records(authority, lambda _record: None)


def test_symlinked_declared_segment_is_rejected_when_supported(tmp_path: Path) -> None:
    evidence = _minimal_closed_evidence(tmp_path)
    authority = verify_closed_capture_authority(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    )
    data_path = authority.segment_data_paths[0]
    target = tmp_path / "moved-segment.jsonl.zst"
    data_path.replace(target)
    try:
        data_path.symlink_to(target)
    except (NotImplementedError, OSError):
        pytest.skip("creating file symlinks is not permitted on this host")

    with pytest.raises(ValueError, match=r"symbolic-link|reparse-point"):
        verify_closed_capture_authority(
            start_path=evidence.start_path,
            closure_path=evidence.closure_path,
            capture_directory=evidence.capture_directory,
        )


def test_noncanonical_persisted_record_is_not_delivered(tmp_path: Path) -> None:
    evidence = _minimal_closed_evidence(tmp_path, noncanonical_record=True)
    authority = verify_closed_capture_authority(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    )
    records: list[CaptureRecord] = []

    with pytest.raises(CaptureIntegrityError, match="not canonical"):
        consume_closed_capture_records(authority, records.append)

    assert records == []


def test_noncanonical_segment_manifest_is_rejected(tmp_path: Path) -> None:
    evidence = _minimal_closed_evidence(tmp_path)
    authority = verify_closed_capture_authority(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    )
    manifest_path = authority.segment_manifest_paths[0]
    manifest = json.loads(manifest_path.read_bytes())
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(CaptureIntegrityError, match="manifest is not canonical"):
        verify_closed_capture_authority(
            start_path=evidence.start_path,
            closure_path=evidence.closure_path,
            capture_directory=evidence.capture_directory,
        )


def test_authority_change_from_consumer_fails_post_reverification(tmp_path: Path) -> None:
    evidence = _minimal_closed_evidence(tmp_path)
    authority = verify_closed_capture_authority(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    )
    consumed = 0

    def mutate_after_delivery(_record: CaptureRecord) -> None:
        nonlocal consumed
        consumed += 1
        _replace_with_valid_changed_closure(authority)

    with pytest.raises(CaptureIntegrityError, match="changed during consumption"):
        consume_closed_capture_records(authority, mutate_after_delivery)

    assert consumed == 1


def _minimal_closed_evidence(
    tmp_path: Path,
    *,
    noncanonical_record: bool = False,
    manifest_schema_version: str = "capture_source_manifest_v1",
    manifest_purpose: str = "infrastructure_only",
    manifest_protocol_sha256: str = FROZEN_PROTOCOL_SHA256,
    manifest_config_sha256: str = "b" * 64,
) -> _ClosedEvidence:
    output = (tmp_path / "output").resolve()
    external = (tmp_path / "external").resolve()
    capture = output / "segments"
    output.mkdir()
    external.mkdir()
    capture.mkdir()
    source_manifest_payload = canonical_json_bytes(
        {
            "schema_version": manifest_schema_version,
            "purpose": manifest_purpose,
            "protocol": {"sha256": manifest_protocol_sha256},
            "configuration": {"sha256": manifest_config_sha256},
        }
    )
    (output / "capture-source-manifest.json").write_bytes(source_manifest_payload)
    start = build_session_start(
        protocol_sha256=FROZEN_PROTOCOL_SHA256,
        source_manifest_sha256=hashlib.sha256(source_manifest_payload).hexdigest(),
        config_sha256="b" * 64,
        output_root=output,
        external_audit_root=external,
        started_at_ms=_STARTED_AT_MS,
        started_monotonic_ns=_STARTED_MONOTONIC_NS,
        boot_uuid=_BOOT_UUID,
    )
    start_write = write_session_start(start, output_root=output)
    external_start_write = write_external_audit_record(
        ExternalAuditRecordV1(
            schema_version="capture_external_audit_record_v1",
            purpose="infrastructure_only",
            trust_classification="SEPARATE_PATH_AUDIT_ONLY",
            phase="start",
            session_id=start.session_id,
            recorded_at_ms=_STARTED_AT_MS,
            protocol_sha256=start.protocol_sha256,
            source_manifest_sha256=start.source_manifest_sha256,
            subject_sha256=start_write.sha256,
            previous_record_sha256=None,
        ),
        external_root=external,
        output_root=output,
    )
    writer = SegmentedCaptureWriter(
        capture,
        plan_sha256=start.capture_plan_sha256,
        process_boot_id=start.process_boot_id,
        maximum_total_bytes=4 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    plan = start.route_plan_summary.websocket_plans[0]
    record = CaptureEnvelopeV1(
        received_at_ms=_STARTED_AT_MS + 1,
        received_monotonic_ns=_STARTED_MONOTONIC_NS + 1,
        plan_sha256=start.capture_plan_sha256,
        process_boot_id=start.process_boot_id,
        connection_id=f"{plan.name}-g000001",
        frame_seq=1,
        ingest_seq=1,
        market=Market(plan.market),
        route=plan.route,
        stream=f"combined:{plan.name}",
        subscription_streams=tuple(plan.streams),
        raw_payload="{}",
    )
    writer.append(record, record_to_json_line(record))
    writer.close()
    if noncanonical_record:
        _rewrite_record_noncanonically(capture)

    closed_at_ms = _STARTED_AT_MS + 1_000
    closure = build_session_closure(
        start_path=start_write.path,
        capture_directory=capture,
        stop_reason="operator_requested",
        fatal=False,
        closed_at_ms=closed_at_ms,
        closed_monotonic_ns=_STARTED_MONOTONIC_NS + 1_000_000_000,
    )
    closure_write = write_session_closure(
        closure,
        start_path=start_write.path,
        capture_directory=capture,
    )
    write_external_audit_record(
        ExternalAuditRecordV1(
            schema_version="capture_external_audit_record_v1",
            purpose="infrastructure_only",
            trust_classification="SEPARATE_PATH_AUDIT_ONLY",
            phase="closure",
            session_id=start.session_id,
            recorded_at_ms=closed_at_ms,
            protocol_sha256=start.protocol_sha256,
            source_manifest_sha256=start.source_manifest_sha256,
            subject_sha256=closure_write.sha256,
            previous_record_sha256=external_start_write.sha256,
        ),
        external_root=external,
        output_root=output,
    )
    return _ClosedEvidence(
        start_path=start_write.path,
        closure_path=closure_write.path,
        capture_directory=capture,
    )


def _rewrite_record_noncanonically(capture: Path) -> None:
    [data_path] = capture.glob("*.jsonl.zst")
    [line] = read_segment_lines(data_path)
    raw = json.loads(line)
    noncanonical = json.dumps(
        raw,
        ensure_ascii=False,
        separators=(", ", ": "),
        sort_keys=False,
    ).encode("utf-8") + b"\n"
    encoded_frame = _encode_outer_frame(noncanonical)
    data_path.write_bytes(encoded_frame)

    manifest_path = data_path.with_name(data_path.name + ".manifest.json")
    manifest = json.loads(manifest_path.read_bytes())
    manifest["uncompressed_bytes"] = len(noncanonical)
    manifest["compressed_bytes"] = len(encoded_frame)
    manifest["sha256"] = hashlib.sha256(encoded_frame).hexdigest()
    manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")


def _replace_with_valid_changed_closure(authority: ClosedCaptureAuthority) -> None:
    changed_closure = authority.closure.model_copy(
        update={
            "closed_at_ms": authority.closure.closed_at_ms + 1,
            "closed_monotonic_ns": authority.closure.closed_monotonic_ns + 1,
        }
    )
    closure_payload = canonical_json_bytes(changed_closure.model_dump(mode="json")) + b"\n"
    authority.closure_path.write_bytes(closure_payload)
    changed_external = authority.external_closure.model_copy(
        update={
            "recorded_at_ms": authority.external_closure.recorded_at_ms + 1,
            "subject_sha256": hashlib.sha256(closure_payload).hexdigest(),
        }
    )
    authority.external_closure_path.write_bytes(
        canonical_json_bytes(changed_external.model_dump(mode="json")) + b"\n"
    )


def _encode_outer_frame(decoded: bytes) -> bytes:
    compressed = zstd.ZstdCompressor(level=3, write_checksum=True).compress(decoded)
    core = _OUTER_FRAME_HEADER_CORE.pack(
        b"SBCAPFRM",
        1,
        len(compressed),
        len(decoded),
        hashlib.sha256(compressed).digest(),
    )
    return core + hashlib.sha256(core).digest() + compressed
