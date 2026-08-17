from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from signalbot.capture.config import (
    CANARY_SYMBOLS,
    FROZEN_PROTOCOL_SHA256,
    SEPARATE_PATH_AUDIT_ONLY,
    capture_route_registry,
    validate_capture_route_registry,
)
from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.path_safety import inspect_link_free_path
from signalbot.capture.plans import build_prospective_capture_plans
from signalbot.capture.provenance import (
    canonical_json_bytes,
    canonical_sha256,
    validate_external_audit_roots,
)
from signalbot.capture.storage import verify_capture_segments

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_BOOT_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_SESSION_ID_RE = re.compile(r"^(?P<started>\d+)-(?P<boot>[0-9a-f]{32})$")
_MAXIMUM_SESSION_DOCUMENT_BYTES = 2 * 1024 * 1024
_NORMAL_STOP_REASONS = frozenset({"completed_duration", "operator_requested"})
_FATAL_STOP_REASONS = frozenset(
    {
        "capture_failure",
        "capacity_exhausted",
        "clock_discontinuity",
        "network_retry_exhausted",
    }
)

StopReason = Literal[
    "completed_duration",
    "operator_requested",
    "capture_failure",
    "capacity_exhausted",
    "clock_discontinuity",
    "network_retry_exhausted",
]


class _StrictFrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SpotPublicRoutesV1(_StrictFrozenModel):
    rest_base: str
    rest_paths: tuple[str, str, str, str, str]
    websocket_base: str
    stream_suffixes: tuple[str, str, str, str]


class FuturesPublicRoutesV1(_StrictFrozenModel):
    rest_base: str
    rest_paths: tuple[str, str, str, str, str, str, str, str, str, str]
    websocket_market_base: str
    websocket_market_suffixes: tuple[str, str, str]
    websocket_public_base: str
    websocket_public_suffixes: tuple[str, str]


class PublicTransportAllowlistV1(_StrictFrozenModel):
    spot: SpotPublicRoutesV1
    futures: FuturesPublicRoutesV1


class PublicRestAllowedQueryValuesV1(_StrictFrozenModel):
    key: str
    values: tuple[str, ...]


class PublicRestRequestPlanEntryV1(_StrictFrozenModel):
    role: str
    method: Literal["GET"]
    market: Literal["spot", "futures"]
    rest_base: str
    path: str
    fixed_request_headers: tuple[tuple[str, str], ...]
    fixed_query: tuple[tuple[str, str], ...]
    allowed_query_keys: tuple[str, ...]
    allowed_query_values: tuple[PublicRestAllowedQueryValuesV1, ...]
    maximum_query_limit: int | None
    trigger: Literal[
        "interval",
        "depth_resync_event_only",
        "utc_bar_close",
        "next_funding_time",
        "interval_or_exchange_info_hash_change",
    ]
    interval_seconds: int | None
    delay_seconds: int | None
    hash_on_change: bool
    trigger_events: tuple[str, ...]
    maximum_attempts: Literal[1, 2]
    data_role: Literal["primary_capture", "cross_check_non_primary"]


class PublicRouteRegistryV1(_StrictFrozenModel):
    transport_public_allowlist: PublicTransportAllowlistV1
    frozen_canary_rest_request_plan: tuple[PublicRestRequestPlanEntryV1, ...]


class PublicWebSocketPlanV1(_StrictFrozenModel):
    name: str
    market: Literal["spot", "futures"]
    route: Literal["spot", "market", "public"]
    streams: tuple[str, ...]
    url: str


class PublicRoutePlanSummaryV1(_StrictFrozenModel):
    schema_version: Literal["capture_public_route_plan_summary_v1"]
    symbols: tuple[str, str, str]
    route_registry: PublicRouteRegistryV1
    websocket_plans: tuple[
        PublicWebSocketPlanV1,
        PublicWebSocketPlanV1,
        PublicWebSocketPlanV1,
    ]
    websocket_plan_count: Literal[3]
    websocket_stream_count: Literal[27]

    @model_validator(mode="after")
    def require_exact_frozen_public_plan(self) -> PublicRoutePlanSummaryV1:
        if self.model_dump(mode="json") != _expected_route_plan_document():
            raise ValueError("public route/plan summary differs from the frozen canary")
        return self


class CaptureChainSummaryV1(_StrictFrozenModel):
    schema_version: Literal["capture_chain_summary_v1"]
    capture_directory: str
    segment_count: int = Field(ge=0)
    record_count: int = Field(ge=0)
    first_receipt_at_ms: int | None = Field(default=None, ge=0)
    last_receipt_at_ms: int | None = Field(default=None, ge=0)
    final_manifest_sha256: str | None
    final_data_sha256: str | None

    @field_validator("capture_directory")
    @classmethod
    def validate_capture_directory(cls, value: str) -> str:
        return _require_absolute_document_path(value, "capture_directory")

    @field_validator("final_manifest_sha256", "final_data_sha256")
    @classmethod
    def validate_optional_hash(cls, value: str | None) -> str | None:
        if value is not None:
            _require_sha256(value, "capture chain hash")
        return value

    @model_validator(mode="after")
    def require_coherent_chain_summary(self) -> CaptureChainSummaryV1:
        nullable = (
            self.first_receipt_at_ms,
            self.last_receipt_at_ms,
            self.final_manifest_sha256,
            self.final_data_sha256,
        )
        if self.segment_count == 0:
            if self.record_count != 0 or any(item is not None for item in nullable):
                raise ValueError("an empty capture chain must have zero counts and null heads")
            return self
        if self.record_count < self.segment_count:
            raise ValueError("a non-empty capture chain has fewer records than segments")
        if any(item is None for item in nullable):
            raise ValueError("a non-empty capture chain must bind receipts and chain heads")
        assert self.first_receipt_at_ms is not None
        assert self.last_receipt_at_ms is not None
        if self.last_receipt_at_ms < self.first_receipt_at_ms:
            raise ValueError("capture chain receipt range is reversed")
        return self


class SessionStartV1(_StrictFrozenModel):
    schema_version: Literal["capture_session_start_v1"]
    purpose: Literal["infrastructure_only"]
    session_id: str
    process_boot_id: str
    started_at_ms: int = Field(ge=0)
    started_monotonic_ns: int = Field(ge=0)
    protocol_sha256: str
    source_manifest_sha256: str
    config_sha256: str
    capture_plan_sha256: str
    route_plan_summary: PublicRoutePlanSummaryV1
    output_root: str
    external_audit_root: str
    external_audit_trust_classification: Literal["SEPARATE_PATH_AUDIT_ONLY"]

    @field_validator(
        "protocol_sha256",
        "source_manifest_sha256",
        "config_sha256",
        "capture_plan_sha256",
    )
    @classmethod
    def validate_hash(cls, value: str) -> str:
        return _require_sha256(value, "session start hash")

    @field_validator("output_root", "external_audit_root")
    @classmethod
    def validate_root_path(cls, value: str) -> str:
        return _require_absolute_document_path(value, "session root")

    @field_validator("process_boot_id")
    @classmethod
    def validate_process_boot_id(cls, value: str) -> str:
        if _BOOT_ID_RE.fullmatch(value) is None:
            raise ValueError("process_boot_id must be a lowercase UUID hex value")
        return value

    @model_validator(mode="after")
    def require_frozen_authority(self) -> SessionStartV1:
        if self.protocol_sha256 != FROZEN_PROTOCOL_SHA256:
            raise ValueError("session protocol hash differs from the frozen protocol")
        expected_id = f"{self.started_at_ms}-{self.process_boot_id}"
        if self.session_id != expected_id or _SESSION_ID_RE.fullmatch(self.session_id) is None:
            raise ValueError("session_id must bind the UTC start and process boot UUID")
        actual_plan_sha256 = capture_plan_authority_sha256(
            protocol_sha256=self.protocol_sha256,
            source_manifest_sha256=self.source_manifest_sha256,
            config_sha256=self.config_sha256,
            route_plan_summary=self.route_plan_summary,
        )
        if self.capture_plan_sha256 != actual_plan_sha256:
            raise ValueError("capture_plan_sha256 differs from the complete capture authority")
        if self.external_audit_trust_classification != SEPARATE_PATH_AUDIT_ONLY:
            raise ValueError("external audit trust classification is invalid")
        if _paths_are_equal_or_nested(self.output_root, self.external_audit_root):
            raise ValueError("session output and external audit roots must be non-nested")
        return self


class SessionClosureV1(_StrictFrozenModel):
    schema_version: Literal["capture_session_closure_v1"]
    purpose: Literal["infrastructure_only"]
    session_id: str
    process_boot_id: str
    closed_at_ms: int = Field(ge=0)
    closed_monotonic_ns: int = Field(ge=0)
    protocol_sha256: str
    source_manifest_sha256: str
    config_sha256: str
    capture_plan_sha256: str
    start_document_sha256: str
    output_root: str
    external_audit_root: str
    external_audit_trust_classification: Literal["SEPARATE_PATH_AUDIT_ONLY"]
    stop_reason: StopReason
    fatal: bool
    capture_chain: CaptureChainSummaryV1

    @field_validator(
        "protocol_sha256",
        "source_manifest_sha256",
        "config_sha256",
        "capture_plan_sha256",
        "start_document_sha256",
    )
    @classmethod
    def validate_hash(cls, value: str) -> str:
        return _require_sha256(value, "session closure hash")

    @field_validator("output_root", "external_audit_root")
    @classmethod
    def validate_root_path(cls, value: str) -> str:
        return _require_absolute_document_path(value, "session root")

    @field_validator("process_boot_id")
    @classmethod
    def validate_process_boot_id(cls, value: str) -> str:
        if _BOOT_ID_RE.fullmatch(value) is None:
            raise ValueError("process_boot_id must be a lowercase UUID hex value")
        return value

    @model_validator(mode="after")
    def require_stop_reason_fatality(self) -> SessionClosureV1:
        if self.protocol_sha256 != FROZEN_PROTOCOL_SHA256:
            raise ValueError("closure protocol hash differs from the frozen protocol")
        session_match = _SESSION_ID_RE.fullmatch(self.session_id)
        if session_match is None or session_match.group("boot") != self.process_boot_id:
            raise ValueError("closure session_id must be safe and bind the process boot UUID")
        if self.stop_reason in _NORMAL_STOP_REASONS and self.fatal:
            raise ValueError("normal stop reasons require fatal=false")
        if self.stop_reason in _FATAL_STOP_REASONS and not self.fatal:
            raise ValueError("failure stop reasons require fatal=true")
        if self.external_audit_trust_classification != SEPARATE_PATH_AUDIT_ONLY:
            raise ValueError("external audit trust classification is invalid")
        return self


@dataclass(frozen=True, slots=True)
class SessionDocumentWrite:
    path: Path
    sha256: str
    byte_count: int


def generate_session_id(started_at_ms: int, boot_uuid: uuid.UUID | None = None) -> str:
    """Generate a filename-safe identifier from UTC milliseconds and one boot UUID."""

    if type(started_at_ms) is not int or started_at_ms < 0:
        raise ValueError("started_at_ms must be a nonnegative integer")
    identity = uuid.uuid4() if boot_uuid is None else boot_uuid
    return f"{started_at_ms}-{identity.hex}"


def build_public_route_plan_summary() -> PublicRoutePlanSummaryV1:
    """Build the exact three-socket, three-symbol public canary authority."""

    return PublicRoutePlanSummaryV1.model_validate(_expected_route_plan_document())


def capture_plan_authority_sha256(
    *,
    protocol_sha256: str,
    source_manifest_sha256: str,
    config_sha256: str,
    route_plan_summary: PublicRoutePlanSummaryV1,
) -> str:
    """Bind protocol, source, config, public routes, streams, and REST request plan."""

    return canonical_sha256(
        {
            "schema_version": "capture_plan_authority_v1",
            "protocol_sha256": protocol_sha256,
            "source_manifest_sha256": source_manifest_sha256,
            "config_sha256": config_sha256,
            "route_plan_summary": route_plan_summary.model_dump(mode="json"),
        }
    )


def build_session_start(
    *,
    protocol_sha256: str,
    source_manifest_sha256: str,
    config_sha256: str,
    output_root: str | Path,
    external_audit_root: str | Path,
    started_at_ms: int | None = None,
    started_monotonic_ns: int | None = None,
    boot_uuid: uuid.UUID | None = None,
) -> SessionStartV1:
    """Create a deterministic start document after validating both storage roots."""

    output, external = _validated_session_roots(output_root, external_audit_root)
    wall_ms = time.time_ns() // 1_000_000 if started_at_ms is None else started_at_ms
    monotonic_ns = time.monotonic_ns() if started_monotonic_ns is None else started_monotonic_ns
    if type(wall_ms) is not int or type(monotonic_ns) is not int:
        raise ValueError("session start clocks must be integers")
    identity = uuid.uuid4() if boot_uuid is None else boot_uuid
    summary = build_public_route_plan_summary()
    return SessionStartV1(
        schema_version="capture_session_start_v1",
        purpose="infrastructure_only",
        session_id=generate_session_id(wall_ms, identity),
        process_boot_id=identity.hex,
        started_at_ms=wall_ms,
        started_monotonic_ns=monotonic_ns,
        protocol_sha256=protocol_sha256,
        source_manifest_sha256=source_manifest_sha256,
        config_sha256=config_sha256,
        capture_plan_sha256=capture_plan_authority_sha256(
            protocol_sha256=protocol_sha256,
            source_manifest_sha256=source_manifest_sha256,
            config_sha256=config_sha256,
            route_plan_summary=summary,
        ),
        route_plan_summary=summary,
        output_root=str(output),
        external_audit_root=str(external),
        external_audit_trust_classification=SEPARATE_PATH_AUDIT_ONLY,
    )


def write_session_start(
    start: SessionStartV1,
    *,
    output_root: str | Path,
) -> SessionDocumentWrite:
    """Write the canonical start document exactly once and make it durable."""

    output, external = _validated_session_roots(output_root, start.external_audit_root)
    if start.output_root != str(output) or start.external_audit_root != str(external):
        raise ValueError("session start roots differ from the write roots")
    return _write_document_once(
        output / f"{start.session_id}.start.session.json",
        start.model_dump(mode="json"),
    )


def build_session_closure(
    *,
    start_path: str | Path,
    capture_directory: str | Path,
    stop_reason: StopReason,
    fatal: bool,
    closed_at_ms: int | None = None,
    closed_monotonic_ns: int | None = None,
) -> SessionClosureV1:
    """Verify the complete capture authority before constructing a closure."""

    start, start_sha256 = _read_start_authority(start_path)
    wall_ms = time.time_ns() // 1_000_000 if closed_at_ms is None else closed_at_ms
    monotonic_ns = time.monotonic_ns() if closed_monotonic_ns is None else closed_monotonic_ns
    _validate_closure_clocks(start, wall_ms, monotonic_ns)
    chain = _verified_capture_chain(start, capture_directory)
    return SessionClosureV1(
        schema_version="capture_session_closure_v1",
        purpose="infrastructure_only",
        session_id=start.session_id,
        process_boot_id=start.process_boot_id,
        closed_at_ms=wall_ms,
        closed_monotonic_ns=monotonic_ns,
        protocol_sha256=start.protocol_sha256,
        source_manifest_sha256=start.source_manifest_sha256,
        config_sha256=start.config_sha256,
        capture_plan_sha256=start.capture_plan_sha256,
        start_document_sha256=start_sha256,
        output_root=start.output_root,
        external_audit_root=start.external_audit_root,
        external_audit_trust_classification=start.external_audit_trust_classification,
        stop_reason=stop_reason,
        fatal=fatal,
        capture_chain=chain,
    )


def write_session_closure(
    closure: SessionClosureV1,
    *,
    start_path: str | Path,
    capture_directory: str | Path,
) -> SessionDocumentWrite:
    """Reverify the start and segment chain, then write the closure exactly once."""

    start, start_sha256 = _read_start_authority(start_path)
    _validate_closure_clocks(start, closure.closed_at_ms, closure.closed_monotonic_ns)
    expected_chain = _verified_capture_chain(start, capture_directory)
    expected_bindings = {
        "session_id": start.session_id,
        "process_boot_id": start.process_boot_id,
        "protocol_sha256": start.protocol_sha256,
        "source_manifest_sha256": start.source_manifest_sha256,
        "config_sha256": start.config_sha256,
        "capture_plan_sha256": start.capture_plan_sha256,
        "start_document_sha256": start_sha256,
        "output_root": start.output_root,
        "external_audit_root": start.external_audit_root,
        "external_audit_trust_classification": (start.external_audit_trust_classification),
    }
    actual = closure.model_dump(mode="json")
    if any(actual[field] != value for field, value in expected_bindings.items()):
        raise CaptureIntegrityError("session closure does not bind the stored start")
    if closure.capture_chain != expected_chain:
        raise CaptureIntegrityError("session closure capture chain is stale or invalid")
    output, external = _validated_session_roots(
        closure.output_root,
        closure.external_audit_root,
    )
    if str(external) != closure.external_audit_root:
        raise ValueError("closure external audit root differs from the resolved root")
    return _write_document_once(
        output / f"{closure.session_id}.closure.session.json",
        actual,
    )


def _expected_route_plan_document() -> dict[str, object]:
    validate_capture_route_registry()
    routes = capture_route_registry()
    plans = build_prospective_capture_plans(CANARY_SYMBOLS, batch_size=25)
    return {
        "schema_version": "capture_public_route_plan_summary_v1",
        "symbols": list(CANARY_SYMBOLS),
        "route_registry": routes,
        "websocket_plans": [
            {
                "name": plan.name,
                "market": plan.market.value,
                "route": plan.route,
                "streams": list(plan.streams),
                "url": plan.url,
            }
            for plan in plans
        ],
        "websocket_plan_count": len(plans),
        "websocket_stream_count": sum(len(plan.streams) for plan in plans),
    }


def _read_start_authority(path: str | Path) -> tuple[SessionStartV1, str]:
    candidate = Path(path)
    _reject_symlinked_path(candidate, "start_path")
    if not candidate.is_file():
        raise ValueError("start_path must be an existing regular file")
    resolved = candidate.resolve(strict=True)
    if resolved.stat().st_size > _MAXIMUM_SESSION_DOCUMENT_BYTES:
        raise CaptureIntegrityError("session start document exceeds its size limit")
    payload = resolved.read_bytes()
    if not payload.endswith(b"\n") or payload.count(b"\n") != 1:
        raise CaptureIntegrityError("session start document is not one JSON line")
    try:
        raw = json.loads(payload)
        start = SessionStartV1.model_validate(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as exc:
        raise CaptureIntegrityError("session start document is invalid") from exc
    expected_payload = canonical_json_bytes(start.model_dump(mode="json")) + b"\n"
    if payload != expected_payload:
        raise CaptureIntegrityError("session start document is not canonical")
    output, external = _validated_session_roots(
        start.output_root,
        start.external_audit_root,
    )
    expected_path = output / f"{start.session_id}.start.session.json"
    if resolved != expected_path:
        raise ValueError("start_path escapes or differs from its session output root")
    if str(external) != start.external_audit_root:
        raise ValueError("start external audit root differs from the resolved root")
    return start, hashlib.sha256(payload).hexdigest()


def _verified_capture_chain(
    start: SessionStartV1,
    capture_directory: str | Path,
) -> CaptureChainSummaryV1:
    output = _require_real_directory(start.output_root, "output_root")
    capture = _require_real_directory(capture_directory, "capture_directory")
    if not capture.is_relative_to(output):
        raise ValueError("capture_directory must remain within session output_root")
    _reject_tree_symlinks(capture)
    manifests = verify_capture_segments(
        capture,
        expected_plan_sha256=start.capture_plan_sha256,
        expected_process_boot_id=start.process_boot_id,
    )
    if not manifests:
        return CaptureChainSummaryV1(
            schema_version="capture_chain_summary_v1",
            capture_directory=str(capture),
            segment_count=0,
            record_count=0,
            first_receipt_at_ms=None,
            last_receipt_at_ms=None,
            final_manifest_sha256=None,
            final_data_sha256=None,
        )
    final = manifests[-1]
    final_manifest_path = capture / f"{final.data_file}.manifest.json"
    _reject_symlinked_path(final_manifest_path, "final segment manifest")
    return CaptureChainSummaryV1(
        schema_version="capture_chain_summary_v1",
        capture_directory=str(capture),
        segment_count=len(manifests),
        record_count=sum(item.record_count for item in manifests),
        first_receipt_at_ms=manifests[0].first_received_at_ms,
        last_receipt_at_ms=final.last_received_at_ms,
        final_manifest_sha256=_sha256_file(final_manifest_path),
        final_data_sha256=final.sha256,
    )


def _validated_session_roots(
    output_root: str | Path,
    external_audit_root: str | Path,
) -> tuple[Path, Path]:
    external, output = validate_external_audit_roots(
        external_audit_root,
        output_root,
    )
    output = _require_real_directory(output, "output_root")
    external = _require_real_directory(external, "external_audit_root")
    return output, external


def _require_real_directory(path: str | Path, field: str) -> Path:
    inspection = inspect_link_free_path(path, field)
    status = inspection.final_status
    if status is None or not stat.S_ISDIR(status.st_mode):
        raise ValueError(f"{field} must be an existing directory")
    return inspection.absolute_path.resolve(strict=True)


def _reject_symlinked_path(
    path: Path,
    field: str,
    *,
    allow_missing_tail: bool = False,
) -> None:
    inspect_link_free_path(path, field, allow_missing_tail=allow_missing_tail)


def _reject_tree_symlinks(root: Path) -> None:
    try:
        for path in root.rglob("*"):
            inspect_link_free_path(path, "capture directory entry")
    except ValueError as exc:
        raise CaptureIntegrityError(
            "capture directory contains a symbolic link or reparse point"
        ) from exc


def _require_absolute_document_path(value: str, field: str) -> str:
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError(f"{field} must be an absolute normalized path")
    return value


def _paths_are_equal_or_nested(first: str, second: str) -> bool:
    first_path = Path(first)
    second_path = Path(second)
    return (
        first_path == second_path
        or first_path.is_relative_to(second_path)
        or second_path.is_relative_to(first_path)
    )


def _require_sha256(value: str, field: str) -> str:
    if _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def _validate_closure_clocks(
    start: SessionStartV1,
    closed_at_ms: int,
    closed_monotonic_ns: int,
) -> None:
    if type(closed_at_ms) is not int or type(closed_monotonic_ns) is not int:
        raise ValueError("session closure clocks must be integers")
    if closed_at_ms < start.started_at_ms:
        raise ValueError("session closure UTC time precedes the start")
    if closed_monotonic_ns < start.started_monotonic_ns:
        raise ValueError("session closure monotonic time precedes the start")


def _write_document_once(path: Path, document: dict[str, object]) -> SessionDocumentWrite:
    _reject_symlinked_path(
        path,
        "session document path",
        allow_missing_tail=True,
    )
    payload = canonical_json_bytes(document) + b"\n"
    with path.open("xb", buffering=0) as handle:
        written = handle.write(payload)
        if written != len(payload):
            raise OSError(f"session document short write: expected {len(payload)}, wrote {written}")
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_parent(path)
    return SessionDocumentWrite(
        path=path,
        sha256=hashlib.sha256(payload).hexdigest(),
        byte_count=len(payload),
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_parent(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
