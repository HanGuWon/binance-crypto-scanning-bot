from __future__ import annotations

import hashlib
import json
import tracemalloc
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
from pydantic import ValidationError

from signalbot.capture.canary_report import (
    CanaryCapacitySchemaReportV1,
    build_canary_capacity_schema_report,
)
from signalbot.capture.config import FROZEN_PROTOCOL_SHA256
from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.models import (
    CaptureEnvelopeV1,
    ConnectionState,
    ConnectionTransitionV1,
    RestEnvelopeV2,
    RestErrorCategory,
    record_to_json_line,
)
from signalbot.capture.provenance import (
    ExternalAuditRecordV1,
    canonical_json_bytes,
    write_external_audit_record,
)
from signalbot.capture.session import (
    SessionStartV1,
    build_session_closure,
    build_session_start,
    write_session_closure,
    write_session_start,
)
from signalbot.capture.storage import SegmentedCaptureWriter, consume_segment_lines
from signalbot.domain.enums import Market

_STARTED_AT_MS = 1_721_000_000_000
_STARTED_MONOTONIC_NS = 8_000_000_000
_FULL_DURATION_NS = 86_400 * 1_000_000_000
_BOOT_UUID = uuid.UUID("01234567-89ab-cdef-0123-456789abcdef")
_FORBIDDEN_KEYS = {"pnl", "outcome", "return", "label", "threshold", "signal", "order"}


@dataclass(frozen=True, slots=True)
class _ClosedEvidence:
    start_path: Path
    closure_path: Path
    capture_directory: Path


def test_full_closed_authority_yields_deterministic_infrastructure_pass(
    tmp_path: Path,
) -> None:
    evidence = _closed_evidence(tmp_path, elapsed_ns=_FULL_DURATION_NS)

    first = build_canary_capacity_schema_report(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    )
    second = build_canary_capacity_schema_report(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    )

    assert first.report.verdict == "CAPTURE_CAPACITY_SCHEMA_PASS"
    assert first.report.verdict_reasons == ("capacity_schema_requirements_satisfied",)
    assert first.report.websocket.observed_expected_combined_stream_count == 27
    assert first.report.websocket.missing_expected_stream_count == 0
    assert first.report.rest.observed_expected_role_count == 11
    assert first.report.rest.missing_expected_role_count == 0
    assert first.report.connections.generation_count == 3
    assert set(first.report.connections.connected_generation_counts_by_plan.values()) == {1}
    assert first.report.storage.websocket_frame_count == 27
    assert first.report.storage.record_count == 41
    assert first.report.storage.ingest_sequence_verified is True
    assert first.report.authority.external_audit_subject_chain_verified is True
    assert first.report.authority.full_segment_chain_verified is True
    assert first.report.scope_boundaries.promotion_authorized is False
    assert first.canonical_bytes == second.canonical_bytes
    assert first.sha256 == second.sha256 == hashlib.sha256(first.canonical_bytes).hexdigest()
    assert first.canonical_bytes == canonical_json_bytes(first.report.model_dump(mode="json"))


def test_smoke_is_incomplete_even_when_every_stream_and_rest_role_was_seen(
    tmp_path: Path,
) -> None:
    evidence = _closed_evidence(
        tmp_path,
        elapsed_ns=60 * 1_000_000_000,
        stop_reason="operator_requested",
    )

    report = build_canary_capacity_schema_report(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    ).report

    assert report.verdict == "INCOMPLETE"
    assert report.receipt_range.full_configured_duration_observed is False
    assert report.websocket.missing_expected_stream_count == 0
    assert report.rest.missing_expected_role_count == 0
    assert report.verdict_reasons == (
        "configured_duration_not_observed",
        "closure_not_completed_duration",
    )


def test_duration_one_nanosecond_below_boundary_is_incomplete(tmp_path: Path) -> None:
    evidence = _closed_evidence(tmp_path, elapsed_ns=_FULL_DURATION_NS - 1)

    report = build_canary_capacity_schema_report(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    ).report

    assert report.verdict == "INCOMPLETE"
    assert report.receipt_range.session_elapsed_monotonic_ms == 86_399_999
    assert report.verdict_reasons == ("configured_duration_not_observed",)


@pytest.mark.parametrize(
    ("invalid_websocket_payload", "http_429", "expected_reason"),
    [
        (True, False, "invalid_or_non_json_payload"),
        (False, True, "http_418_or_429_observed"),
    ],
)
def test_bad_payload_or_rate_limit_yields_fail(
    tmp_path: Path,
    invalid_websocket_payload: bool,
    http_429: bool,
    expected_reason: str,
) -> None:
    evidence = _closed_evidence(
        tmp_path,
        elapsed_ns=_FULL_DURATION_NS,
        invalid_websocket_payload=invalid_websocket_payload,
        http_429=http_429,
    )

    report = build_canary_capacity_schema_report(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    ).report

    assert report.verdict == "FAIL"
    assert expected_reason in report.verdict_reasons
    if invalid_websocket_payload:
        assert report.websocket.invalid_or_non_json_payload_count == 1
    if http_429:
        assert report.rest.http_429_count == 1


def test_incomplete_rest_body_limit_cannot_count_as_a_capacity_pass(
    tmp_path: Path,
) -> None:
    evidence = _closed_evidence(
        tmp_path,
        elapsed_ns=_FULL_DURATION_NS,
        rest_body_limit=True,
    )

    report = build_canary_capacity_schema_report(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    ).report

    assert report.verdict == "FAIL"
    assert report.verdict_reasons == ("rest_body_limit_observed",)
    assert report.rest.observed_expected_role_count == 11
    assert report.rest.error_counts["body_limit"] == 1

    tampered = report.model_dump(mode="python")
    tampered["verdict"] = "CAPTURE_CAPACITY_SCHEMA_PASS"
    tampered["verdict_reasons"] = ("capacity_schema_requirements_satisfied",)
    with pytest.raises(ValidationError, match="PASS contradicts"):
        CanaryCapacitySchemaReportV1.model_validate(tampered)


def test_external_closure_subject_mismatch_fails_closed(tmp_path: Path) -> None:
    evidence = _closed_evidence(
        tmp_path,
        elapsed_ns=_FULL_DURATION_NS,
        wrong_external_closure_subject=True,
    )

    with pytest.raises(CaptureIntegrityError, match="subject or SHA chain"):
        build_canary_capacity_schema_report(
            start_path=evidence.start_path,
            closure_path=evidence.closure_path,
            capture_directory=evidence.capture_directory,
        )


def test_report_has_no_efficacy_keys_or_future_payload_interpretation(
    tmp_path: Path,
) -> None:
    evidence = _closed_evidence(tmp_path, elapsed_ns=_FULL_DURATION_NS)

    artifact = build_canary_capacity_schema_report(
        start_path=evidence.start_path,
        closure_path=evidence.closure_path,
        capture_directory=evidence.capture_directory,
    )
    document = artifact.report.model_dump(mode="json")

    assert _all_keys(document).isdisjoint(_FORBIDDEN_KEYS)
    assert b"future_price" not in artifact.canonical_bytes
    assert artifact.report.scope_boundaries.payload_data_interpreted is False
    assert artifact.report.scope_boundaries.future_market_information_interpreted is False
    assert artifact.report.scope_boundaries.depth_sequence_acceptance_performed is False
    assert artifact.report.scope_boundaries.coverage_acceptance_performed is False
    assert artifact.report.scope_boundaries.efficacy_acceptance_performed is False

    raw = document | {"pnl": 1}
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        CanaryCapacitySchemaReportV1.model_validate(raw)


def test_public_segment_consumer_is_bounded_to_one_decoded_frame(tmp_path: Path) -> None:
    capture = tmp_path / "segments"
    writer = SegmentedCaptureWriter(
        capture,
        plan_sha256="a" * 64,
        process_boot_id="boot-1",
        maximum_total_bytes=32 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    frame_count = 256
    raw_payload = json.dumps({"padding": "x" * 32_768}, separators=(",", ":"))
    for ingest_seq in range(1, frame_count + 1):
        record = CaptureEnvelopeV1(
            received_at_ms=_STARTED_AT_MS + ingest_seq,
            received_monotonic_ns=_STARTED_MONOTONIC_NS + ingest_seq,
            plan_sha256="a" * 64,
            process_boot_id="boot-1",
            connection_id="capture-spot-1-g000001",
            frame_seq=ingest_seq,
            ingest_seq=ingest_seq,
            market=Market.SPOT,
            route="spot",
            stream="btcusdt@aggTrade",
            subscription_streams=("btcusdt@aggTrade",),
            raw_payload=raw_payload,
        )
        writer.append(record, record_to_json_line(record))
    writer.close()
    [data_path] = capture.glob("*.jsonl.zst")

    consumed_count = 0
    consumed_bytes = 0

    def consume(line: bytes) -> None:
        nonlocal consumed_count, consumed_bytes
        consumed_count += 1
        consumed_bytes += len(line)

    tracemalloc.start()
    consume_segment_lines(data_path, consume)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert consumed_count == frame_count
    assert consumed_bytes > 8 * 1024 * 1024
    assert peak < consumed_bytes // 4


def _closed_evidence(
    tmp_path: Path,
    *,
    elapsed_ns: int,
    stop_reason: str = "completed_duration",
    invalid_websocket_payload: bool = False,
    http_429: bool = False,
    rest_body_limit: bool = False,
    wrong_external_closure_subject: bool = False,
) -> _ClosedEvidence:
    output = (tmp_path / "output").resolve()
    external = (tmp_path / "external").resolve()
    capture = output / "segments"
    output.mkdir()
    external.mkdir()
    capture.mkdir()
    source_manifest_payload = canonical_json_bytes(
        {
            "schema_version": "capture_source_manifest_v1",
            "purpose": "infrastructure_only",
            "protocol": {"sha256": FROZEN_PROTOCOL_SHA256},
            "configuration": {"sha256": "b" * 64},
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
    _write_complete_capacity_records(
        capture,
        start,
        invalid_websocket_payload=invalid_websocket_payload,
        http_429=http_429,
        rest_body_limit=rest_body_limit,
    )
    closed_monotonic_ns = _STARTED_MONOTONIC_NS + elapsed_ns
    closed_at_ms = _STARTED_AT_MS + max(0, elapsed_ns // 1_000_000)
    closure = build_session_closure(
        start_path=start_write.path,
        capture_directory=capture,
        stop_reason=stop_reason,  # pyright: ignore[reportArgumentType]
        fatal=False,
        closed_at_ms=closed_at_ms,
        closed_monotonic_ns=closed_monotonic_ns,
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
            subject_sha256=(
                "f" * 64 if wrong_external_closure_subject else closure_write.sha256
            ),
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


def _write_complete_capacity_records(
    capture: Path,
    start: SessionStartV1,
    *,
    invalid_websocket_payload: bool,
    http_429: bool,
    rest_body_limit: bool,
) -> None:
    writer = SegmentedCaptureWriter(
        capture,
        plan_sha256=start.capture_plan_sha256,
        process_boot_id=start.process_boot_id,
        maximum_total_bytes=32 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    ingest_seq = 0
    plans = start.route_plan_summary.websocket_plans
    for plan in plans:
        ingest_seq += 1
        transition = ConnectionTransitionV1(
            received_at_ms=_STARTED_AT_MS + ingest_seq,
            received_monotonic_ns=_STARTED_MONOTONIC_NS + ingest_seq,
            plan_sha256=start.capture_plan_sha256,
            process_boot_id=start.process_boot_id,
            connection_id=f"{plan.name}-g000001",
            ingest_seq=ingest_seq,
            last_frame_seq=0,
            market=Market(plan.market),
            route=plan.route,
            streams=tuple(plan.streams),
            state=ConnectionState.CONNECTED,
            reason="public_session_open",
        )
        writer.append(transition, record_to_json_line(transition))
        for frame_seq, stream in enumerate(plan.streams, start=1):
            ingest_seq += 1
            raw_payload = json.dumps(
                {"stream": stream, "data": {"future_price": ingest_seq}},
                separators=(",", ":"),
            )
            if invalid_websocket_payload and ingest_seq == 2:
                raw_payload = "not-json"
            envelope = CaptureEnvelopeV1(
                received_at_ms=_STARTED_AT_MS + ingest_seq,
                received_monotonic_ns=_STARTED_MONOTONIC_NS + ingest_seq,
                plan_sha256=start.capture_plan_sha256,
                process_boot_id=start.process_boot_id,
                connection_id=f"{plan.name}-g000001",
                frame_seq=frame_seq,
                ingest_seq=ingest_seq,
                market=Market(plan.market),
                route=plan.route,
                stream=f"combined:{plan.name}",
                subscription_streams=tuple(plan.streams),
                raw_payload=raw_payload,
            )
            writer.append(envelope, record_to_json_line(envelope))
    for role_index, request in enumerate(
        start.route_plan_summary.route_registry.frozen_canary_rest_request_plan
    ):
        ingest_seq += 1
        status = 429 if http_429 and role_index == 0 else 200
        body_limited = rest_body_limit and request.role == "spot_exchange_info"
        rest = RestEnvelopeV2(
            request_started_at_ms=_STARTED_AT_MS + ingest_seq,
            request_started_monotonic_ns=_STARTED_MONOTONIC_NS + ingest_seq,
            response_first_byte_at_ms=_STARTED_AT_MS + ingest_seq,
            response_first_byte_monotonic_ns=_STARTED_MONOTONIC_NS + ingest_seq + 1,
            response_completed_at_ms=_STARTED_AT_MS + ingest_seq,
            response_completed_monotonic_ns=_STARTED_MONOTONIC_NS + ingest_seq + 2,
            plan_sha256=start.capture_plan_sha256,
            process_boot_id=start.process_boot_id,
            request_role=request.role,
            correlation_id=f"request-{role_index:02d}",
            attempt=1,
            ingest_seq=ingest_seq,
            market=Market(request.market),
            endpoint_path=request.path,
            canonical_query=tuple(request.fixed_query),
            response_status=status,
            response_headers=(("content-type", "application/json"),),
            payload_complete=not body_limited,
            raw_payload="{" if body_limited else "{}",
            error_category=(
                RestErrorCategory.BODY_LIMIT
                if body_limited
                else RestErrorCategory.HTTP_STATUS
                if status == 429
                else None
            ),
            error_detail=(
                "response exceeded configured body limit"
                if body_limited
                else "non-success HTTP status"
                if status == 429
                else None
            ),
        )
        writer.append(rest, record_to_json_line(rest))
    writer.close()


def _all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return {
            *(str(key).casefold() for key in value),
            *(key for item in value.values() for key in _all_keys(item)),
        }
    if isinstance(value, list | tuple):
        return {key for item in value for key in _all_keys(item)}
    return set()
