from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from signalbot.capture.closed_evidence import (
    ClosedCaptureAuthority,
    consume_closed_capture_records,
    verify_closed_capture_authority,
)
from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    CaptureRecord,
    ConnectionState,
    ConnectionTransitionV1,
    CoverageState,
    RestEnvelopeV1,
    RestEnvelopeV2,
    RestErrorCategory,
    payload_bytes,
)
from signalbot.capture.provenance import canonical_json_bytes
from signalbot.capture.session import SessionClosureV1, SessionStartV1
from signalbot.capture.storage import SegmentManifestV1

_EXPECTED_DURATION_SECONDS = 86_400
_EXPECTED_WEBSOCKET_STREAM_COUNT = 27
_EXPECTED_PLAN_NAMES = (
    "capture-futures-market-1",
    "capture-futures-public-1",
    "capture-spot-1",
)
_EXPECTED_REST_ROLES = (
    "futures_depth_snapshot",
    "futures_exchange_info",
    "futures_funding_info",
    "futures_funding_rate_confirmation",
    "futures_open_interest",
    "futures_open_interest_history",
    "futures_premium_index",
    "futures_venue_time",
    "spot_depth_snapshot",
    "spot_exchange_info",
    "spot_venue_time",
)
_EXPECTED_STREAM_KEYS = tuple(
    sorted(
        (
            *(
                f"spot|spot|{symbol}@{suffix}"
                for symbol in ("btcusdt", "ethusdt", "solusdt")
                for suffix in ("kline_5m", "aggTrade", "bookTicker", "depth@100ms")
            ),
            *(
                f"futures|market|{symbol}@{suffix}"
                for symbol in ("btcusdt", "ethusdt", "solusdt")
                for suffix in ("kline_5m", "aggTrade", "markPrice@1s")
            ),
            *(
                f"futures|public|{symbol}@{suffix}"
                for symbol in ("btcusdt", "ethusdt", "solusdt")
                for suffix in ("bookTicker", "depth@100ms")
            ),
        )
    )
)
_FORBIDDEN_REPORT_KEYS = frozenset(
    {"pnl", "outcome", "return", "label", "threshold", "signal", "order"}
)
_SCHEMA_VERSIONS = (
    "capture_envelope_v1",
    "rest_envelope_v1",
    "rest_envelope_v2",
    "connection_transition_v1",
    "coverage_transition_v1",
)
CanaryVerdict = Literal["CAPTURE_CAPACITY_SCHEMA_PASS", "INCOMPLETE", "FAIL"]
VerdictReason = Literal[
    "capacity_schema_requirements_satisfied",
    "fatal_session_closure",
    "invalid_or_non_json_payload",
    "http_418_or_429_observed",
    "rest_body_limit_observed",
    "coverage_invalid_record_observed",
    "unexpected_websocket_stream_or_plan",
    "unexpected_rest_role",
    "unexpected_connection_plan_or_generation",
    "configured_duration_not_observed",
    "closure_not_completed_duration",
    "expected_websocket_stream_missing",
    "expected_rest_role_missing",
    "connected_generation_missing",
]


class _StrictReportModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class AuthorityMetricsV1(_StrictReportModel):
    session_id: str
    protocol_sha256: str
    source_manifest_sha256: str
    capture_plan_sha256: str
    session_start_sha256: str
    session_closure_sha256: str
    external_start_sha256: str
    external_closure_sha256: str
    canonical_session_documents_verified: Literal[True]
    external_audit_subject_chain_verified: Literal[True]
    full_segment_chain_verified: Literal[True]
    clean_closure: bool

    @field_validator(
        "protocol_sha256",
        "source_manifest_sha256",
        "capture_plan_sha256",
        "session_start_sha256",
        "session_closure_sha256",
        "external_start_sha256",
        "external_closure_sha256",
    )
    @classmethod
    def require_sha256(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("report authority hashes must be lowercase SHA-256 digests")
        return value


class ReceiptRangeMetricsV1(_StrictReportModel):
    first_receipt_at_ms: int | None = Field(default=None, ge=0)
    last_receipt_at_ms: int | None = Field(default=None, ge=0)
    elapsed_receipt_ms: int = Field(ge=0)
    session_elapsed_monotonic_ms: int = Field(ge=0)
    configured_duration_seconds: Literal[86_400]
    full_configured_duration_observed: bool

    @model_validator(mode="after")
    def require_coherent_range(self) -> ReceiptRangeMetricsV1:
        if (self.first_receipt_at_ms is None) != (self.last_receipt_at_ms is None):
            raise ValueError("receipt range endpoints must be paired")
        if self.first_receipt_at_ms is None:
            if self.elapsed_receipt_ms != 0:
                raise ValueError("an empty receipt range must have zero elapsed time")
            return self
        assert self.last_receipt_at_ms is not None
        if self.last_receipt_at_ms < self.first_receipt_at_ms:
            raise ValueError("receipt range is reversed")
        if self.elapsed_receipt_ms != self.last_receipt_at_ms - self.first_receipt_at_ms:
            raise ValueError("elapsed receipt time differs from its endpoints")
        return self


class StorageMetricsV1(_StrictReportModel):
    segment_count: int = Field(ge=0)
    record_count: int = Field(ge=0)
    websocket_frame_count: int = Field(ge=0)
    compressed_bytes: int = Field(ge=0)
    uncompressed_bytes: int = Field(ge=0)
    ingest_sequence_verified: Literal[True]
    schema_counts: dict[str, int]

    @model_validator(mode="after")
    def require_schema_totals(self) -> StorageMetricsV1:
        if tuple(sorted(self.schema_counts)) != tuple(sorted(_SCHEMA_VERSIONS)):
            raise ValueError("schema_counts must cover the exact persisted record schemas")
        if any(type(value) is not int or value < 0 for value in self.schema_counts.values()):
            raise ValueError("schema counts must be nonnegative integers")
        if sum(self.schema_counts.values()) != self.record_count:
            raise ValueError("schema counts differ from the record count")
        if self.schema_counts["capture_envelope_v1"] != self.websocket_frame_count:
            raise ValueError("WebSocket schema count differs from frame count")
        return self


class WebSocketMetricsV1(_StrictReportModel):
    expected_combined_stream_count: Literal[27]
    observed_expected_combined_stream_count: int = Field(ge=0, le=27)
    combined_stream_frame_counts: dict[str, int]
    missing_expected_stream_count: int = Field(ge=0, le=27)
    unexpected_stream_or_plan_record_count: int = Field(ge=0)
    invalid_or_non_json_payload_count: int = Field(ge=0)

    @model_validator(mode="after")
    def require_stream_totals(self) -> WebSocketMetricsV1:
        if tuple(sorted(self.combined_stream_frame_counts)) != _EXPECTED_STREAM_KEYS:
            raise ValueError("combined stream counts must cover the exact 27-stream plan")
        if any(
            type(value) is not int or value < 0
            for value in self.combined_stream_frame_counts.values()
        ):
            raise ValueError("combined stream counts must be nonnegative integers")
        observed = sum(value > 0 for value in self.combined_stream_frame_counts.values())
        if observed != self.observed_expected_combined_stream_count:
            raise ValueError("observed stream count differs from per-stream counts")
        if self.missing_expected_stream_count != 27 - observed:
            raise ValueError("missing stream count differs from per-stream counts")
        return self


class ConnectionMetricsV1(_StrictReportModel):
    transition_state_counts: dict[str, int]
    generation_count: int = Field(ge=0)
    generation_counts_by_plan: dict[str, int]
    connected_generation_counts_by_plan: dict[str, int]
    unexpected_plan_or_generation_record_count: int = Field(ge=0)

    @model_validator(mode="after")
    def require_connection_keys(self) -> ConnectionMetricsV1:
        expected_states = tuple(sorted(state.value for state in ConnectionState))
        if tuple(sorted(self.transition_state_counts)) != expected_states:
            raise ValueError("transition counts must cover the exact connection states")
        if (
            tuple(sorted(self.generation_counts_by_plan)) != _EXPECTED_PLAN_NAMES
            or tuple(sorted(self.connected_generation_counts_by_plan))
            != _EXPECTED_PLAN_NAMES
        ):
            raise ValueError("connection counts must cover the exact three plans")
        mappings = (
            self.transition_state_counts,
            self.generation_counts_by_plan,
            self.connected_generation_counts_by_plan,
        )
        if any(
            type(value) is not int or value < 0
            for mapping in mappings
            for value in mapping.values()
        ):
            raise ValueError("connection counts must be nonnegative integers")
        return self


class RestMetricsV1(_StrictReportModel):
    expected_role_count: Literal[11]
    observed_expected_role_count: int = Field(ge=0, le=11)
    role_record_counts: dict[str, int]
    missing_expected_role_count: int = Field(ge=0, le=11)
    unexpected_role_record_count: int = Field(ge=0)
    attempt_counts: dict[str, int]
    status_counts: dict[str, int]
    error_counts: dict[str, int]
    invalid_or_non_json_payload_count: int = Field(ge=0)
    http_418_count: int = Field(ge=0)
    http_429_count: int = Field(ge=0)

    @model_validator(mode="after")
    def require_rest_totals(self) -> RestMetricsV1:
        if tuple(sorted(self.role_record_counts)) != _EXPECTED_REST_ROLES:
            raise ValueError("REST role counts must cover the exact request plan")
        observed = sum(value > 0 for value in self.role_record_counts.values())
        if observed != self.observed_expected_role_count:
            raise ValueError("observed REST roles differ from per-role counts")
        if self.missing_expected_role_count != self.expected_role_count - observed:
            raise ValueError("missing REST roles differ from per-role counts")
        if tuple(sorted(self.attempt_counts)) != ("1", "2", "other"):
            raise ValueError("REST attempt counts must use the bounded report buckets")
        expected_errors = {"none", "legacy_error", *(item.value for item in RestErrorCategory)}
        if set(self.error_counts) != expected_errors:
            raise ValueError("REST error counts must cover the exact report categories")
        mappings = (
            self.role_record_counts,
            self.attempt_counts,
            self.status_counts,
            self.error_counts,
        )
        if any(
            type(value) is not int or value < 0
            for mapping in mappings
            for value in mapping.values()
        ):
            raise ValueError("REST counts must be nonnegative integers")
        return self


class ScopeBoundariesV1(_StrictReportModel):
    infrastructure_only: Literal[True]
    payload_data_interpreted: Literal[False]
    future_market_information_interpreted: Literal[False]
    depth_sequence_acceptance_performed: Literal[False]
    coverage_acceptance_performed: Literal[False]
    efficacy_acceptance_performed: Literal[False]
    promotion_authorized: Literal[False]


class CanaryCapacitySchemaReportV1(_StrictReportModel):
    schema_version: Literal["capture_capacity_schema_report_v1"]
    purpose: Literal["infrastructure_only"]
    authority: AuthorityMetricsV1
    receipt_range: ReceiptRangeMetricsV1
    storage: StorageMetricsV1
    websocket: WebSocketMetricsV1
    connections: ConnectionMetricsV1
    rest: RestMetricsV1
    invalid_or_non_json_payload_count: int = Field(ge=0)
    coverage_invalid_record_count: int = Field(ge=0)
    scope_boundaries: ScopeBoundariesV1
    verdict: CanaryVerdict
    verdict_reasons: tuple[VerdictReason, ...]

    @model_validator(mode="after")
    def require_verdict_reason(self) -> CanaryCapacitySchemaReportV1:
        if not self.verdict_reasons:
            raise ValueError("the canary verdict requires at least one reason")
        if self.verdict == "CAPTURE_CAPACITY_SCHEMA_PASS" and self.verdict_reasons != (
            "capacity_schema_requirements_satisfied",
        ):
            raise ValueError("PASS requires the single satisfied-requirements reason")
        payload_total = (
            self.websocket.invalid_or_non_json_payload_count
            + self.rest.invalid_or_non_json_payload_count
        )
        if self.invalid_or_non_json_payload_count != payload_total:
            raise ValueError("payload total differs from transport payload counts")
        if self.verdict == "CAPTURE_CAPACITY_SCHEMA_PASS" and (
            not self.authority.clean_closure
            or not self.receipt_range.full_configured_duration_observed
            or self.websocket.missing_expected_stream_count
            or self.rest.missing_expected_role_count
            or self.invalid_or_non_json_payload_count
            or self.rest.http_418_count
            or self.rest.http_429_count
            or self.rest.error_counts[RestErrorCategory.BODY_LIMIT.value]
            or self.coverage_invalid_record_count
            or self.websocket.unexpected_stream_or_plan_record_count
            or self.rest.unexpected_role_record_count
            or self.connections.unexpected_plan_or_generation_record_count
            or any(
                not value
                for value in self.connections.connected_generation_counts_by_plan.values()
            )
        ):
            raise ValueError("PASS contradicts the capacity/schema acceptance fields")
        return self


@dataclass(frozen=True, slots=True)
class CanonicalCanaryReport:
    report: CanaryCapacitySchemaReportV1
    canonical_bytes: bytes
    sha256: str


class _InfrastructureAccumulator:
    def __init__(self, start: SessionStartV1) -> None:
        self.schema_counts = Counter({schema: 0 for schema in _SCHEMA_VERSIONS})
        self.websocket_frame_count = 0
        self.websocket_invalid_payload_count = 0
        self.websocket_unexpected_count = 0
        self.rest_invalid_payload_count = 0
        self.coverage_invalid_count = 0
        self.first_receipt_at_ms: int | None = None
        self.last_receipt_at_ms: int | None = None

        plans = start.route_plan_summary.websocket_plans
        self._plans_by_contract = {
            (plan.market, plan.route, tuple(plan.streams)): plan.name for plan in plans
        }
        self._expected_stream_keys = tuple(
            sorted(
                _stream_key(plan.market, plan.route, stream)
                for plan in plans
                for stream in plan.streams
            )
        )
        self.stream_counts = Counter({key: 0 for key in self._expected_stream_keys})
        self.connection_state_counts = Counter(
            {state.value: 0 for state in ConnectionState}
        )
        self.connection_generations: dict[str, set[str]] = {
            plan.name: set() for plan in plans
        }
        self.connected_generations: dict[str, set[str]] = {
            plan.name: set() for plan in plans
        }
        self.connection_unexpected_count = 0

        roles = tuple(
            item.role
            for item in start.route_plan_summary.route_registry.frozen_canary_rest_request_plan
        )
        self.rest_role_counts = Counter({role: 0 for role in roles})
        self.rest_unexpected_role_count = 0
        self.rest_attempt_counts = Counter({"1": 0, "2": 0, "other": 0})
        self.rest_status_counts: Counter[str] = Counter({"none": 0})
        self.rest_error_counts = Counter(
            {
                "none": 0,
                "legacy_error": 0,
                **{category.value: 0 for category in RestErrorCategory},
            }
        )
        self.http_418_count = 0
        self.http_429_count = 0

    def consume(self, record: CaptureRecord) -> None:
        self.schema_counts[record.schema_version] += 1
        receipt = _record_receipt_at_ms(record)
        if self.first_receipt_at_ms is None:
            self.first_receipt_at_ms = receipt
        self.last_receipt_at_ms = receipt
        if isinstance(record, CaptureEnvelopeV1):
            self._consume_websocket(record)
        elif isinstance(record, ConnectionTransitionV1):
            self._consume_connection(record)
        elif isinstance(record, RestEnvelopeV1 | RestEnvelopeV2):
            self._consume_rest(record)
        elif record.state is CoverageState.INVALID:
            self.coverage_invalid_count += 1

    def _consume_websocket(self, record: CaptureEnvelopeV1) -> None:
        self.websocket_frame_count += 1
        plan_contract = (
            record.market.value,
            record.route,
            tuple(record.subscription_streams),
        )
        plan_name = self._plans_by_contract.get(plan_contract)
        valid = plan_name is not None
        if plan_name is not None and record.stream != f"combined:{plan_name}":
            valid = False
        try:
            decoded = json.loads(
                payload_bytes(record.raw_payload, record.raw_payload_encoding)
            )
        except (UnicodeDecodeError, UnicodeEncodeError, ValueError, json.JSONDecodeError):
            decoded = None
        wrapper_stream: object = None
        if isinstance(decoded, dict) and "data" in decoded:
            wrapper_stream = decoded.get("stream")
        if not isinstance(wrapper_stream, str):
            valid = False
        elif wrapper_stream not in record.subscription_streams:
            valid = False
        else:
            key = _stream_key(record.market.value, record.route, wrapper_stream)
            if key not in self.stream_counts:
                valid = False
            elif valid:
                self.stream_counts[key] += 1
        if not valid:
            self.websocket_invalid_payload_count += 1
            self.websocket_unexpected_count += int(
                plan_name is None
                or not isinstance(wrapper_stream, str)
                or _stream_key(record.market.value, record.route, wrapper_stream)
                not in self.stream_counts
            )

    def _consume_connection(self, record: ConnectionTransitionV1) -> None:
        self.connection_state_counts[record.state.value] += 1
        contract = (record.market.value, record.route, tuple(record.streams))
        plan_name = self._plans_by_contract.get(contract)
        if plan_name is None:
            self.connection_unexpected_count += 1
            return
        match = re.fullmatch(
            rf"{re.escape(plan_name)}-g(?P<generation>\d{{6}})",
            record.connection_id,
        )
        if match is None or int(match.group("generation")) < 1:
            self.connection_unexpected_count += 1
            return
        self.connection_generations[plan_name].add(record.connection_id)
        if record.state is ConnectionState.CONNECTED:
            self.connected_generations[plan_name].add(record.connection_id)

    def _consume_rest(self, record: RestEnvelopeV1 | RestEnvelopeV2) -> None:
        attempt_key = str(record.attempt) if record.attempt in (1, 2) else "other"
        self.rest_attempt_counts[attempt_key] += 1
        status = record.response_status
        self.rest_status_counts["none" if status is None else str(status)] += 1
        self.http_418_count += int(status == 418)
        self.http_429_count += int(status == 429)

        if isinstance(record, RestEnvelopeV2):
            role = record.request_role
            error_key = "none" if record.error_category is None else record.error_category.value
            should_parse_payload = record.payload_complete
        else:
            role = "legacy_unattributed_v1"
            error_key = "legacy_error" if record.error is not None else "none"
            should_parse_payload = record.response_status is not None and bool(record.raw_payload)
        self.rest_error_counts[error_key] += 1
        if role in self.rest_role_counts:
            self.rest_role_counts[role] += 1
        else:
            self.rest_unexpected_role_count += 1
        if should_parse_payload and not _is_json_payload(
            record.raw_payload,
            record.raw_payload_encoding,
        ):
            self.rest_invalid_payload_count += 1


def build_canary_capacity_schema_report(
    *,
    start_path: str | Path,
    closure_path: str | Path,
    capture_directory: str | Path,
) -> CanonicalCanaryReport:
    """Build a deterministic infrastructure-only report from closed capture evidence."""

    authority = verify_closed_capture_authority(
        start_path=start_path,
        closure_path=closure_path,
        capture_directory=capture_directory,
    )
    accumulator = _InfrastructureAccumulator(authority.start)
    consume_closed_capture_records(authority, accumulator.consume)

    report = _assemble_report(authority, authority.manifests, accumulator)
    document = report.model_dump(mode="json")
    _require_no_forbidden_report_keys(document)
    encoded = canonical_json_bytes(document)
    return CanonicalCanaryReport(
        report=report,
        canonical_bytes=encoded,
        sha256=hashlib.sha256(encoded).hexdigest(),
    )


def _assemble_report(
    authority: ClosedCaptureAuthority,
    manifests: tuple[SegmentManifestV1, ...],
    accumulator: _InfrastructureAccumulator,
) -> CanaryCapacitySchemaReportV1:
    closure = authority.closure
    elapsed_monotonic_ns = closure.closed_monotonic_ns - authority.start.started_monotonic_ns
    if elapsed_monotonic_ns < 0:
        raise CaptureIntegrityError("session closure monotonic time precedes its start")
    elapsed_monotonic_ms = elapsed_monotonic_ns // 1_000_000
    full_duration = elapsed_monotonic_ns >= _EXPECTED_DURATION_SECONDS * 1_000_000_000
    observed_streams = sum(value > 0 for value in accumulator.stream_counts.values())
    observed_roles = sum(value > 0 for value in accumulator.rest_role_counts.values())
    connection_generation_count = len(
        {item for values in accumulator.connection_generations.values() for item in values}
    )
    reasons, verdict = _verdict(
        closure=closure,
        full_duration=full_duration,
        observed_streams=observed_streams,
        observed_roles=observed_roles,
        accumulator=accumulator,
    )
    first_receipt = accumulator.first_receipt_at_ms
    last_receipt = accumulator.last_receipt_at_ms
    elapsed_receipt = (
        0
        if first_receipt is None or last_receipt is None
        else last_receipt - first_receipt
    )
    return CanaryCapacitySchemaReportV1(
        schema_version="capture_capacity_schema_report_v1",
        purpose="infrastructure_only",
        authority=AuthorityMetricsV1(
            session_id=authority.start.session_id,
            protocol_sha256=authority.start.protocol_sha256,
            source_manifest_sha256=authority.start.source_manifest_sha256,
            capture_plan_sha256=authority.start.capture_plan_sha256,
            session_start_sha256=authority.start_sha256,
            session_closure_sha256=authority.closure_sha256,
            external_start_sha256=authority.external_start_sha256,
            external_closure_sha256=authority.external_closure_sha256,
            canonical_session_documents_verified=True,
            external_audit_subject_chain_verified=True,
            full_segment_chain_verified=True,
            clean_closure=(
                not authority.closure.fatal
                and authority.closure.stop_reason == "completed_duration"
            ),
        ),
        receipt_range=ReceiptRangeMetricsV1(
            first_receipt_at_ms=first_receipt,
            last_receipt_at_ms=last_receipt,
            elapsed_receipt_ms=elapsed_receipt,
            session_elapsed_monotonic_ms=elapsed_monotonic_ms,
            configured_duration_seconds=86_400,
            full_configured_duration_observed=full_duration,
        ),
        storage=StorageMetricsV1(
            segment_count=len(manifests),
            record_count=sum(manifest.record_count for manifest in manifests),
            websocket_frame_count=accumulator.websocket_frame_count,
            compressed_bytes=sum(manifest.compressed_bytes for manifest in manifests),
            uncompressed_bytes=sum(manifest.uncompressed_bytes for manifest in manifests),
            ingest_sequence_verified=True,
            schema_counts=dict(sorted(accumulator.schema_counts.items())),
        ),
        websocket=WebSocketMetricsV1(
            expected_combined_stream_count=27,
            observed_expected_combined_stream_count=observed_streams,
            combined_stream_frame_counts=dict(sorted(accumulator.stream_counts.items())),
            missing_expected_stream_count=27 - observed_streams,
            unexpected_stream_or_plan_record_count=accumulator.websocket_unexpected_count,
            invalid_or_non_json_payload_count=accumulator.websocket_invalid_payload_count,
        ),
        connections=ConnectionMetricsV1(
            transition_state_counts=dict(sorted(accumulator.connection_state_counts.items())),
            generation_count=connection_generation_count,
            generation_counts_by_plan={
                key: len(value)
                for key, value in sorted(accumulator.connection_generations.items())
            },
            connected_generation_counts_by_plan={
                key: len(value)
                for key, value in sorted(accumulator.connected_generations.items())
            },
            unexpected_plan_or_generation_record_count=(
                accumulator.connection_unexpected_count
            ),
        ),
        rest=RestMetricsV1(
            expected_role_count=11,
            observed_expected_role_count=observed_roles,
            role_record_counts=dict(sorted(accumulator.rest_role_counts.items())),
            missing_expected_role_count=11 - observed_roles,
            unexpected_role_record_count=accumulator.rest_unexpected_role_count,
            attempt_counts=dict(sorted(accumulator.rest_attempt_counts.items())),
            status_counts=dict(sorted(accumulator.rest_status_counts.items())),
            error_counts=dict(sorted(accumulator.rest_error_counts.items())),
            invalid_or_non_json_payload_count=accumulator.rest_invalid_payload_count,
            http_418_count=accumulator.http_418_count,
            http_429_count=accumulator.http_429_count,
        ),
        invalid_or_non_json_payload_count=(
            accumulator.websocket_invalid_payload_count
            + accumulator.rest_invalid_payload_count
        ),
        coverage_invalid_record_count=accumulator.coverage_invalid_count,
        scope_boundaries=ScopeBoundariesV1(
            infrastructure_only=True,
            payload_data_interpreted=False,
            future_market_information_interpreted=False,
            depth_sequence_acceptance_performed=False,
            coverage_acceptance_performed=False,
            efficacy_acceptance_performed=False,
            promotion_authorized=False,
        ),
        verdict=verdict,
        verdict_reasons=reasons,
    )


def _verdict(
    *,
    closure: SessionClosureV1,
    full_duration: bool,
    observed_streams: int,
    observed_roles: int,
    accumulator: _InfrastructureAccumulator,
) -> tuple[tuple[VerdictReason, ...], CanaryVerdict]:
    fail_reasons: list[VerdictReason] = []
    if closure.fatal:
        fail_reasons.append("fatal_session_closure")
    if accumulator.websocket_invalid_payload_count + accumulator.rest_invalid_payload_count:
        fail_reasons.append("invalid_or_non_json_payload")
    if accumulator.http_418_count + accumulator.http_429_count:
        fail_reasons.append("http_418_or_429_observed")
    if accumulator.rest_error_counts[RestErrorCategory.BODY_LIMIT.value]:
        fail_reasons.append("rest_body_limit_observed")
    if accumulator.coverage_invalid_count:
        fail_reasons.append("coverage_invalid_record_observed")
    if accumulator.websocket_unexpected_count:
        fail_reasons.append("unexpected_websocket_stream_or_plan")
    if accumulator.rest_unexpected_role_count:
        fail_reasons.append("unexpected_rest_role")
    if accumulator.connection_unexpected_count:
        fail_reasons.append("unexpected_connection_plan_or_generation")
    if fail_reasons:
        return tuple(fail_reasons), "FAIL"

    incomplete_reasons: list[VerdictReason] = []
    if not full_duration:
        incomplete_reasons.append("configured_duration_not_observed")
    if closure.stop_reason != "completed_duration":
        incomplete_reasons.append("closure_not_completed_duration")
    if observed_streams != _EXPECTED_WEBSOCKET_STREAM_COUNT:
        incomplete_reasons.append("expected_websocket_stream_missing")
    if observed_roles != len(accumulator.rest_role_counts):
        incomplete_reasons.append("expected_rest_role_missing")
    if any(not values for values in accumulator.connected_generations.values()):
        incomplete_reasons.append("connected_generation_missing")
    if incomplete_reasons:
        return tuple(incomplete_reasons), "INCOMPLETE"
    return ("capacity_schema_requirements_satisfied",), "CAPTURE_CAPACITY_SCHEMA_PASS"


def _record_receipt_at_ms(record: CaptureRecord) -> int:
    if isinstance(record, RestEnvelopeV1):
        return record.response_received_at_ms
    if isinstance(record, RestEnvelopeV2):
        return record.response_completed_at_ms
    return record.received_at_ms


def _is_json_payload(payload: str, encoding: Any) -> bool:
    try:
        json.loads(payload_bytes(payload, encoding))
    except (UnicodeDecodeError, UnicodeEncodeError, ValueError, json.JSONDecodeError):
        return False
    return True


def _stream_key(market: str, route: str, stream: str) -> str:
    return f"{market}|{route}|{stream}"


def _require_no_forbidden_report_keys(value: object) -> None:
    if isinstance(value, dict):
        forbidden = {
            str(key).casefold()
            for key in value
            if str(key).casefold() in _FORBIDDEN_REPORT_KEYS
        }
        if forbidden:
            raise ValueError(f"canary report contains forbidden keys: {sorted(forbidden)}")
        for item in value.values():
            _require_no_forbidden_report_keys(item)
    elif isinstance(value, list | tuple):
        for item in value:
            _require_no_forbidden_report_keys(item)
