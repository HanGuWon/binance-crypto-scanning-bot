from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from signalbot.capture.config import (
    FROZEN_PROTOCOL_SHA256,
    CaptureCanaryConfig,
    capture_route_registry,
    validate_capture_route_registry,
)
from signalbot.capture.path_safety import inspect_link_free_path
from signalbot.capture.schema_registry import (
    capture_schema_registry,
    validate_runtime_schema_contracts,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_DIRECT_DEPENDENCIES = (
    "fastapi",
    "httpx",
    "psycopg",
    "pydantic",
    "PyYAML",
    "SQLAlchemy",
    "uvicorn",
    "websockets",
    "zstandard",
)
_REQUIRED_CAPTURE_ENTRYPOINTS = (
    "src/signalbot/capture/__init__.py",
    "src/signalbot/capture/canary_report.py",
    "src/signalbot/capture/cli.py",
    "src/signalbot/capture/clock_health_report.py",
    "src/signalbot/capture/closed_evidence.py",
    "src/signalbot/capture/config.py",
    "src/signalbot/capture/depth_coverage_report.py",
    "src/signalbot/capture/depth_sequence.py",
    "src/signalbot/capture/errors.py",
    "src/signalbot/capture/handoff.py",
    "src/signalbot/capture/live.py",
    "src/signalbot/capture/local_book.py",
    "src/signalbot/capture/models.py",
    "src/signalbot/capture/pipeline.py",
    "src/signalbot/capture/plans.py",
    "src/signalbot/capture/path_safety.py",
    "src/signalbot/capture/provenance.py",
    "src/signalbot/capture/receipts.py",
    "src/signalbot/capture/rest.py",
    "src/signalbot/capture/rest_scheduler.py",
    "src/signalbot/capture/schema_registry.py",
    "src/signalbot/capture/session.py",
    "src/signalbot/capture/storage.py",
    "src/signalbot/capture/websocket.py",
    "src/signalbot/capture/ws_owner.py",
)
_READ_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class CanonicalArtifact:
    document: dict[str, object]
    canonical_bytes: bytes
    sha256: str


@dataclass(frozen=True, slots=True)
class ExternalAuditWrite:
    path: Path
    sha256: str
    byte_count: int


@dataclass(frozen=True, slots=True)
class _StableFileSnapshot:
    path: Path
    data: bytes
    sha256: str
    size_bytes: int


class ExternalAuditRecordV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["capture_external_audit_record_v1"]
    purpose: Literal["infrastructure_only"]
    trust_classification: Literal["SEPARATE_PATH_AUDIT_ONLY"]
    phase: Literal["start", "closure"]
    session_id: str
    recorded_at_ms: int = Field(ge=0)
    protocol_sha256: str
    source_manifest_sha256: str
    subject_sha256: str
    previous_record_sha256: str | None

    @field_validator(
        "protocol_sha256",
        "source_manifest_sha256",
        "subject_sha256",
        "previous_record_sha256",
    )
    @classmethod
    def validate_sha256(cls, value: str | None) -> str | None:
        if value is not None and _SHA256_RE.fullmatch(value) is None:
            raise ValueError("audit record hashes must be lowercase SHA-256 digests")
        return value

    @field_validator("session_id")
    @classmethod
    def validate_session_id(cls, value: str) -> str:
        if _SESSION_ID_RE.fullmatch(value) is None:
            raise ValueError("session_id is not safe for a write-once filename")
        return value

    @model_validator(mode="after")
    def validate_phase_chain(self) -> ExternalAuditRecordV1:
        if self.protocol_sha256 != FROZEN_PROTOCOL_SHA256:
            raise ValueError("audit record protocol hash differs from the frozen protocol")
        if self.phase == "start" and self.previous_record_sha256 is not None:
            raise ValueError("a start audit record cannot have a previous record")
        if self.phase == "closure" and self.previous_record_sha256 is None:
            raise ValueError("a closure audit record must bind the start record")
        return self


def canonical_json_bytes(value: object) -> bytes:
    """Encode a JSON-compatible value without platform- or locale-dependent spacing."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def build_capture_source_manifest(
    workspace_root: str | Path,
    *,
    protocol_file: str | Path,
    config_file: str | Path,
) -> CanonicalArtifact:
    """Bind exact source, config, routes, schemas, lockfile, and runtime versions."""

    root = _require_workspace_root(workspace_root)
    config_path = _require_source_file(root, config_file, "config_file")
    protocol_path = _require_source_file(root, protocol_file, "protocol_file")
    required_files = [
        (protocol_path, "frozen_protocol"),
        (config_path, "capture_configuration"),
        (_require_source_file(root, root / "pyproject.toml", "pyproject.toml"), "build"),
        (_require_source_file(root, root / "uv.lock", "uv.lock"), "dependency_lock"),
    ]
    python_sources = _discover_python_sources(root)
    if not python_sources:
        raise ValueError("workspace source must contain at least one Python file")
    python_source_paths = {path.relative_to(root).as_posix() for path in python_sources}
    missing_entrypoints = sorted(set(_REQUIRED_CAPTURE_ENTRYPOINTS).difference(python_source_paths))
    if missing_entrypoints:
        raise ValueError(
            "workspace source is missing required capture entrypoints: "
            + ", ".join(missing_entrypoints)
        )
    required_files.extend((path, "python_source") for path in python_sources)

    entries, snapshots = _file_entries(root, required_files)
    protocol_snapshot = snapshots[protocol_path.relative_to(root).as_posix()]
    config_snapshot = snapshots[config_path.relative_to(root).as_posix()]
    raw_config = _parse_yaml_mapping(config_snapshot, "capture configuration")
    parsed_protocol = _parse_yaml_mapping(protocol_snapshot, "frozen protocol")
    settings = CaptureCanaryConfig.model_validate(raw_config)
    if protocol_snapshot.sha256 != settings.protocol_sha256:
        raise ValueError("protocol_file SHA-256 differs from the frozen capture canary contract")
    validate_capture_route_registry()
    routes = capture_route_registry()
    schemas = _capture_schema_registry()
    runtime = _runtime_versions()
    dependencies = _dependency_versions()
    repository = detect_repository_metadata(root)
    parsed_config = settings.model_dump(mode="json")
    document: dict[str, object] = {
        "schema_version": "capture_source_manifest_v1",
        "purpose": "infrastructure_only",
        "protocol": {
            "path": protocol_path.relative_to(root).as_posix(),
            "sha256": protocol_snapshot.sha256,
            "parsed_sha256": canonical_sha256(parsed_protocol),
        },
        "configuration": {
            "path": config_path.relative_to(root).as_posix(),
            "sha256": config_snapshot.sha256,
            "parsed_sha256": canonical_sha256(parsed_config),
        },
        "files": entries,
        "files_sha256": canonical_sha256(entries),
        "registries": {
            "routes": routes,
            "routes_sha256": canonical_sha256(routes),
            "schemas": schemas,
            "schemas_sha256": canonical_sha256(schemas),
        },
        "runtime": runtime,
        "runtime_sha256": canonical_sha256(runtime),
        "dependencies": dependencies,
        "dependencies_sha256": canonical_sha256(dependencies),
        "repository": repository,
    }
    encoded = canonical_json_bytes(document)
    return CanonicalArtifact(
        document=document,
        canonical_bytes=encoded,
        sha256=hashlib.sha256(encoded).hexdigest(),
    )


def detect_repository_metadata(workspace_root: str | Path) -> dict[str, object]:
    """Return hashes and state only; never place raw status or environment data in a manifest."""

    root = Path(workspace_root)
    probe = _git(root, "rev-parse", "--is-inside-work-tree")
    if probe is None or probe.returncode != 0 or probe.stdout.strip() != "true":
        return {
            "git_head": None,
            "state": "NOT_A_GIT_REPOSITORY",
            "status_entry_count": 0,
            "status_sha256": hashlib.sha256(b"").hexdigest(),
        }
    head_result = _git(root, "rev-parse", "--verify", "HEAD")
    git_head: str | None = None
    if head_result is not None and head_result.returncode == 0:
        candidate = head_result.stdout.strip().lower()
        if re.fullmatch(r"[0-9a-f]{40,64}", candidate) is not None:
            git_head = candidate
    status_result = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status_result is None or status_result.returncode != 0:
        status = ""
        state = "GIT_STATUS_UNAVAILABLE"
    else:
        status = status_result.stdout
        if git_head is None:
            state = "UNBORN_UNTRACKED" if status else "UNBORN_CLEAN"
        else:
            state = "DIRTY" if status else "CLEAN"
    status_bytes = status.encode("utf-8")
    return {
        "git_head": git_head,
        "state": state,
        "status_entry_count": len(status.splitlines()),
        "status_sha256": hashlib.sha256(status_bytes).hexdigest(),
    }


def validate_external_audit_roots(
    external_root: str | Path,
    output_root: str | Path,
) -> tuple[Path, Path]:
    external_inspection = inspect_link_free_path(external_root, "external_root")
    output_inspection = inspect_link_free_path(
        output_root,
        "output_root",
        allow_missing_tail=True,
    )
    external_status = external_inspection.final_status
    output_status = output_inspection.final_status
    if external_status is None or not stat.S_ISDIR(external_status.st_mode):
        raise ValueError("external_root must be an existing directory")
    if output_status is not None and not stat.S_ISDIR(output_status.st_mode):
        raise ValueError("output_root must be a directory when it already exists")
    external = external_inspection.absolute_path.resolve(strict=True)
    output = output_inspection.absolute_path.resolve(strict=False)
    if external == output or external.is_relative_to(output) or output.is_relative_to(external):
        raise ValueError("external_root and output_root must be distinct, non-nested paths")
    return external, output


def write_external_audit_record(
    record: ExternalAuditRecordV1,
    *,
    external_root: str | Path,
    output_root: str | Path,
) -> ExternalAuditWrite:
    """Write one canary-grade audit head; this does not claim a WORM trust boundary."""

    external, _output = validate_external_audit_roots(external_root, output_root)
    if record.phase == "closure":
        _validate_closure_audit_chain(record, external)
    filename = f"{record.session_id}.{record.phase}.audit-head.json"
    path = external / filename
    payload = canonical_json_bytes(record.model_dump(mode="json")) + b"\n"
    with path.open("xb", buffering=0) as handle:
        written = handle.write(payload)
        if written != len(payload):
            raise OSError(
                f"external audit record short write: expected {len(payload)}, wrote {written}"
            )
        os.fsync(handle.fileno())
    _fsync_parent(path)
    return ExternalAuditWrite(
        path=path,
        sha256=hashlib.sha256(payload).hexdigest(),
        byte_count=len(payload),
    )


def _validate_closure_audit_chain(
    closure: ExternalAuditRecordV1,
    external_root: Path,
) -> None:
    start_path = external_root / (f"{closure.session_id}.start.audit-head.json")
    snapshot = _read_stable_file_snapshot(
        start_path,
        "external start audit record",
    )
    try:
        raw_start = json.loads(snapshot.data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("external start audit record must be canonical JSON") from exc
    if not isinstance(raw_start, dict):
        raise ValueError("external start audit record root must be a mapping")
    start = ExternalAuditRecordV1.model_validate(raw_start)
    canonical_start = canonical_json_bytes(start.model_dump(mode="json")) + b"\n"
    if snapshot.data != canonical_start:
        raise ValueError("external start audit record is not canonical")
    if start.phase != "start" or start.session_id != closure.session_id:
        raise ValueError("closure audit record does not bind the same-session start")
    if snapshot.sha256 != closure.previous_record_sha256:
        raise ValueError("closure previous_record_sha256 differs from the actual start")
    if (
        start.protocol_sha256 != closure.protocol_sha256
        or start.source_manifest_sha256 != closure.source_manifest_sha256
        or start.trust_classification != closure.trust_classification
        or start.purpose != closure.purpose
    ):
        raise ValueError("closure audit authority differs from the canonical start")
    if start.recorded_at_ms > closure.recorded_at_ms:
        raise ValueError("closure audit time precedes the canonical start")


def _capture_schema_registry() -> dict[str, object]:
    validate_runtime_schema_contracts()
    registry = capture_schema_registry()
    registry["lifecycle_documents"] = {
        "configuration_version": "capture_canary_config_v1",
        "source_manifest_version": "capture_source_manifest_v1",
        "external_audit_version": "capture_external_audit_record_v1",
        "session_start_version": "capture_session_start_v1",
        "session_closure_version": "capture_session_closure_v1",
    }
    return registry


def _runtime_versions() -> dict[str, str]:
    return {
        "python_implementation": platform.python_implementation(),
        "python_version": platform.python_version(),
        "platform_system": platform.system(),
        "platform_release": platform.release(),
        "machine": platform.machine(),
    }


def _dependency_versions() -> list[dict[str, str]]:
    versions: list[dict[str, str]] = []
    for distribution in _DIRECT_DEPENDENCIES:
        try:
            version = importlib.metadata.version(distribution)
        except importlib.metadata.PackageNotFoundError:
            version = "NOT_INSTALLED"
        versions.append({"distribution": distribution, "version": version})
    return versions


def _file_entries(
    root: Path,
    files: list[tuple[Path, str]],
) -> tuple[list[dict[str, object]], dict[str, _StableFileSnapshot]]:
    entries: list[dict[str, object]] = []
    snapshots: dict[str, _StableFileSnapshot] = {}
    for path, role in files:
        relative = path.relative_to(root).as_posix()
        if relative in snapshots:
            continue
        snapshot = _read_stable_file_snapshot(path, relative)
        snapshots[relative] = snapshot
        entries.append(
            {
                "path": relative,
                "role": role,
                "size_bytes": snapshot.size_bytes,
                "sha256": snapshot.sha256,
            }
        )
    return sorted(entries, key=lambda entry: str(entry["path"])), snapshots


def _read_stable_file_snapshot(path: Path, field: str) -> _StableFileSnapshot:
    before_path = inspect_link_free_path(path, field)
    before_path_status = before_path.final_status
    if before_path_status is None or not stat.S_ISREG(before_path_status.st_mode):
        raise ValueError(f"{field} must be an existing regular file")

    flags = os.O_RDONLY | int(getattr(os, "O_BINARY", 0))
    flags |= int(getattr(os, "O_CLOEXEC", 0))
    flags |= int(getattr(os, "O_NOFOLLOW", 0))
    try:
        descriptor = os.open(before_path.absolute_path, flags)
    except OSError as exc:
        raise ValueError(f"{field} could not be opened safely: {exc}") from exc
    try:
        before_descriptor = os.fstat(descriptor)
        if not stat.S_ISREG(before_descriptor.st_mode):
            raise ValueError(f"{field} must be an existing regular file")
        if _stable_stat_signature(before_path_status) != _stable_stat_signature(before_descriptor):
            raise ValueError(f"{field} changed identity before it was read")

        digest = hashlib.sha256()
        chunks: list[bytes] = []
        size_bytes = 0
        while True:
            chunk = os.read(descriptor, _READ_CHUNK_BYTES)
            if not chunk:
                break
            chunks.append(chunk)
            digest.update(chunk)
            size_bytes += len(chunk)
        after_descriptor = os.fstat(descriptor)
        after_path = inspect_link_free_path(path, field)
        after_path_status = after_path.final_status
        if after_path_status is None:
            raise ValueError(f"{field} disappeared while it was read")
        expected_signature = _stable_stat_signature(before_descriptor)
        if (
            _stable_stat_signature(after_descriptor) != expected_signature
            or _stable_stat_signature(after_path_status) != expected_signature
        ):
            raise ValueError(f"{field} changed identity, size, or mtime while read")
        if size_bytes != before_descriptor.st_size:
            raise ValueError(f"{field} byte count changed while read")
    finally:
        os.close(descriptor)

    return _StableFileSnapshot(
        path=before_path.absolute_path,
        data=b"".join(chunks),
        sha256=digest.hexdigest(),
        size_bytes=size_bytes,
    )


def _stable_stat_signature(status: os.stat_result) -> tuple[int, int, int, int]:
    return (
        status.st_dev,
        status.st_ino,
        status.st_size,
        status.st_mtime_ns,
    )


def _parse_yaml_mapping(
    snapshot: _StableFileSnapshot,
    field: str,
) -> dict[str, object]:
    try:
        parsed = yaml.safe_load(snapshot.data.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError(f"{field} must be valid UTF-8 YAML") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{field} root must be a mapping")
    return parsed


def _discover_python_sources(root: Path) -> list[Path]:
    source_root = root / "src"
    source_inspection = inspect_link_free_path(source_root, "workspace src")
    source_status = source_inspection.final_status
    if source_status is None or not stat.S_ISDIR(source_status.st_mode):
        raise ValueError("workspace src must be an existing real directory")
    try:
        candidates = list(source_root.rglob("*"))
    except OSError as exc:
        raise ValueError(f"workspace source traversal failed: {exc}") from exc

    python_sources: list[Path] = []
    for candidate in candidates:
        inspection = inspect_link_free_path(candidate, "python source tree")
        status = inspection.final_status
        if candidate.suffix != ".py":
            continue
        if status is None or not stat.S_ISREG(status.st_mode):
            raise ValueError("Python source entries must be regular files")
        python_sources.append(inspection.absolute_path)
    return sorted(python_sources, key=lambda path: path.relative_to(root).as_posix())


def _require_workspace_root(path: str | Path) -> Path:
    inspection = inspect_link_free_path(path, "workspace_root")
    status = inspection.final_status
    if status is None or not stat.S_ISDIR(status.st_mode):
        raise ValueError("workspace_root must be an existing directory")
    return inspection.absolute_path.resolve(strict=True)


def _require_source_file(root: Path, path: str | Path, field: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = root / candidate
    inspection = inspect_link_free_path(candidate, field)
    status = inspection.final_status
    if status is None or not stat.S_ISREG(status.st_mode):
        raise ValueError(f"{field} must be an existing regular file")
    resolved = inspection.absolute_path.resolve(strict=True)
    if not resolved.is_relative_to(root):
        raise ValueError(f"{field} must remain within workspace_root")
    return resolved


def _git(root: Path, *arguments: str) -> subprocess.CompletedProcess[str] | None:
    try:
        return subprocess.run(
            ["git", *arguments],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None


def _fsync_parent(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
