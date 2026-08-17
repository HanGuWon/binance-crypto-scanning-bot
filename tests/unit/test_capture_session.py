from __future__ import annotations

import hashlib
import shutil
import uuid
from pathlib import Path

import pytest
from pydantic import ValidationError

from signalbot.capture.config import FROZEN_PROTOCOL_SHA256
from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.models import CaptureEnvelopeV1, record_to_json_line
from signalbot.capture.provenance import canonical_json_bytes, canonical_sha256
from signalbot.capture.session import (
    PublicRoutePlanSummaryV1,
    SessionClosureV1,
    SessionStartV1,
    build_public_route_plan_summary,
    build_session_closure,
    build_session_start,
    capture_plan_authority_sha256,
    generate_session_id,
    write_session_closure,
    write_session_start,
)
from signalbot.capture.storage import SegmentedCaptureWriter
from signalbot.domain.enums import Market

STARTED_AT_MS = 1_721_000_000_000
STARTED_MONOTONIC_NS = 8_000_000_000
BOOT_UUID = uuid.UUID("01234567-89ab-cdef-0123-456789abcdef")


def test_public_route_plan_summary_is_exact_and_deterministic() -> None:
    first = build_public_route_plan_summary()
    second = build_public_route_plan_summary()

    assert first == second
    assert canonical_sha256(first.model_dump(mode="json")) == canonical_sha256(
        second.model_dump(mode="json")
    )
    assert first.symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert first.websocket_plan_count == 3
    assert first.websocket_stream_count == 27
    assert [len(plan.streams) for plan in first.websocket_plans] == [12, 9, 6]
    assert len(first.route_registry.frozen_canary_rest_request_plan) == 11
    assert first.route_registry.frozen_canary_rest_request_plan[-2].maximum_attempts == 2
    assert first.route_registry.frozen_canary_rest_request_plan[-1].path == "/fapi/v1/fundingInfo"
    encoded = canonical_json_bytes(first.model_dump(mode="json")).decode("utf-8")
    assert "data-stream.binance.vision" in encoded
    assert "/market/stream" in encoded
    assert "/public/stream" in encoded
    assert "listenkey" not in encoded.casefold()


def test_route_plan_summary_rejects_any_nested_drift() -> None:
    raw = build_public_route_plan_summary().model_dump(mode="json")
    routes = raw["route_registry"]
    assert isinstance(routes, dict)
    transport = routes["transport_public_allowlist"]
    assert isinstance(transport, dict)
    spot = transport["spot"]
    assert isinstance(spot, dict)
    spot["rest_base"] = "https://example.invalid"

    with pytest.raises(ValidationError, match="frozen canary"):
        PublicRoutePlanSummaryV1.model_validate(raw)


def test_session_id_and_start_bind_injected_clocks_boot_and_hashes(tmp_path: Path) -> None:
    output, external = _roots(tmp_path)
    start = _start(output, external)

    assert generate_session_id(STARTED_AT_MS, BOOT_UUID) == (
        "1721000000000-0123456789abcdef0123456789abcdef"
    )
    assert start.session_id == generate_session_id(STARTED_AT_MS, BOOT_UUID)
    assert start.process_boot_id == BOOT_UUID.hex
    assert start.started_at_ms == STARTED_AT_MS
    assert start.started_monotonic_ns == STARTED_MONOTONIC_NS
    assert start.protocol_sha256 == FROZEN_PROTOCOL_SHA256
    assert start.capture_plan_sha256 == capture_plan_authority_sha256(
        protocol_sha256=start.protocol_sha256,
        source_manifest_sha256=start.source_manifest_sha256,
        config_sha256=start.config_sha256,
        route_plan_summary=start.route_plan_summary,
    )
    assert start.external_audit_trust_classification == "SEPARATE_PATH_AUDIT_ONLY"

    with pytest.raises(ValueError, match="nonnegative integer"):
        generate_session_id(-1, BOOT_UUID)
    with pytest.raises(ValueError, match="clocks must be integers"):
        build_session_start(
            protocol_sha256=FROZEN_PROTOCOL_SHA256,
            source_manifest_sha256="a" * 64,
            config_sha256="b" * 64,
            output_root=output,
            external_audit_root=external,
            started_at_ms=True,
            started_monotonic_ns=STARTED_MONOTONIC_NS,
            boot_uuid=BOOT_UUID,
        )


def test_start_is_canonical_durable_and_write_once(tmp_path: Path) -> None:
    output, external = _roots(tmp_path)
    start = _start(output, external)

    written = write_session_start(start, output_root=output)
    payload = written.path.read_bytes()
    assert payload == canonical_json_bytes(start.model_dump(mode="json")) + b"\n"
    assert written.sha256 == hashlib.sha256(payload).hexdigest()
    assert written.byte_count == len(payload)
    assert payload.count(b"\n") == 1

    with pytest.raises(FileExistsError):
        write_session_start(start, output_root=output)


def test_verified_nonempty_closure_binds_start_and_final_chain_heads(
    tmp_path: Path,
) -> None:
    output, external = _roots(tmp_path)
    capture = output / "segments"
    capture.mkdir()
    start = _start(output, external)
    start_write = write_session_start(start, output_root=output)
    _write_segment(capture, start, record_count=2)

    closure = build_session_closure(
        start_path=start_write.path,
        capture_directory=capture,
        stop_reason="completed_duration",
        fatal=False,
        closed_at_ms=STARTED_AT_MS + 1_000,
        closed_monotonic_ns=STARTED_MONOTONIC_NS + 1_000,
    )

    assert closure.start_document_sha256 == start_write.sha256
    assert closure.capture_chain.segment_count == 1
    assert closure.capture_chain.record_count == 2
    assert closure.capture_chain.first_receipt_at_ms == STARTED_AT_MS + 1
    assert closure.capture_chain.last_receipt_at_ms == STARTED_AT_MS + 2
    [manifest_path] = capture.glob("*.manifest.json")
    assert closure.capture_chain.final_manifest_sha256 == _sha256_file(manifest_path)
    [data_path] = capture.glob("*.jsonl.zst")
    assert closure.capture_chain.final_data_sha256 == _sha256_file(data_path)

    closure_write = write_session_closure(
        closure,
        start_path=start_write.path,
        capture_directory=capture,
    )
    assert closure_write.path.read_bytes() == (
        canonical_json_bytes(closure.model_dump(mode="json")) + b"\n"
    )
    with pytest.raises(FileExistsError):
        write_session_closure(
            closure,
            start_path=start_write.path,
            capture_directory=capture,
        )


def test_empty_clean_capture_has_explicit_null_chain_heads(tmp_path: Path) -> None:
    output, external = _roots(tmp_path)
    capture = output / "segments"
    capture.mkdir()
    start = _start(output, external)
    start_write = write_session_start(start, output_root=output)

    closure = build_session_closure(
        start_path=start_write.path,
        capture_directory=capture,
        stop_reason="operator_requested",
        fatal=False,
        closed_at_ms=STARTED_AT_MS,
        closed_monotonic_ns=STARTED_MONOTONIC_NS,
    )

    assert closure.capture_chain.segment_count == 0
    assert closure.capture_chain.record_count == 0
    assert closure.capture_chain.first_receipt_at_ms is None
    assert closure.capture_chain.last_receipt_at_ms is None
    assert closure.capture_chain.final_manifest_sha256 is None
    assert closure.capture_chain.final_data_sha256 is None
    assert write_session_closure(
        closure,
        start_path=start_write.path,
        capture_directory=capture,
    ).path.is_file()


def test_corrupted_segment_refuses_build_and_write_of_closure(tmp_path: Path) -> None:
    output, external = _roots(tmp_path)
    capture = output / "segments"
    capture.mkdir()
    start = _start(output, external)
    start_write = write_session_start(start, output_root=output)
    _write_segment(capture, start, record_count=1)
    closure = build_session_closure(
        start_path=start_write.path,
        capture_directory=capture,
        stop_reason="completed_duration",
        fatal=False,
        closed_at_ms=STARTED_AT_MS + 1_000,
        closed_monotonic_ns=STARTED_MONOTONIC_NS + 1_000,
    )
    [data_path] = capture.glob("*.jsonl.zst")
    damaged = bytearray(data_path.read_bytes())
    damaged[-1] ^= 1
    data_path.write_bytes(damaged)

    with pytest.raises(CaptureIntegrityError, match="SHA-256"):
        build_session_closure(
            start_path=start_write.path,
            capture_directory=capture,
            stop_reason="capture_failure",
            fatal=True,
            closed_at_ms=STARTED_AT_MS + 1_001,
            closed_monotonic_ns=STARTED_MONOTONIC_NS + 1_001,
        )
    with pytest.raises(CaptureIntegrityError, match="SHA-256"):
        write_session_closure(
            closure,
            start_path=start_write.path,
            capture_directory=capture,
        )
    assert not list(output.glob("*.closure.session.json"))


def test_stale_but_structurally_valid_chain_summary_is_rejected_at_write(
    tmp_path: Path,
) -> None:
    output, external = _roots(tmp_path)
    capture = output / "segments"
    capture.mkdir()
    start = _start(output, external)
    start_write = write_session_start(start, output_root=output)
    _write_segment(capture, start, record_count=1)
    closure = build_session_closure(
        start_path=start_write.path,
        capture_directory=capture,
        stop_reason="completed_duration",
        fatal=False,
        closed_at_ms=STARTED_AT_MS + 10,
        closed_monotonic_ns=STARTED_MONOTONIC_NS + 10,
    )
    raw = closure.model_dump(mode="json")
    chain = raw["capture_chain"]
    assert isinstance(chain, dict)
    chain["record_count"] = 2
    stale = SessionClosureV1.model_validate(raw)

    with pytest.raises(CaptureIntegrityError, match="stale or invalid"):
        write_session_closure(
            stale,
            start_path=start_write.path,
            capture_directory=capture,
        )


def test_models_reject_extra_keys_bad_hashes_ids_and_stop_fatality(tmp_path: Path) -> None:
    output, external = _roots(tmp_path)
    capture = output / "segments"
    capture.mkdir()
    start = _start(output, external)
    start_write = write_session_start(start, output_root=output)
    closure = build_session_closure(
        start_path=start_write.path,
        capture_directory=capture,
        stop_reason="operator_requested",
        fatal=False,
        closed_at_ms=STARTED_AT_MS,
        closed_monotonic_ns=STARTED_MONOTONIC_NS,
    )

    start_raw = start.model_dump(mode="json")
    start_raw["unexpected"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        SessionStartV1.model_validate(start_raw)

    start_raw.pop("unexpected")
    start_raw["config_sha256"] = "c" * 64
    with pytest.raises(ValidationError, match="complete capture authority"):
        SessionStartV1.model_validate(start_raw)

    start_raw["config_sha256"] = "b" * 64
    start_raw["source_manifest_sha256"] = "A" * 64
    with pytest.raises(ValidationError, match="lowercase SHA-256"):
        SessionStartV1.model_validate(start_raw)

    start_raw["source_manifest_sha256"] = "a" * 64
    start_raw["session_id"] = "../escape"
    with pytest.raises(ValidationError, match="session_id must bind"):
        SessionStartV1.model_validate(start_raw)

    closure_raw = closure.model_dump(mode="json")
    closure_raw["unexpected"] = True
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        SessionClosureV1.model_validate(closure_raw)

    closure_raw.pop("unexpected")
    closure_raw["fatal"] = True
    with pytest.raises(ValidationError, match="fatal=false"):
        SessionClosureV1.model_validate(closure_raw)

    closure_raw["fatal"] = False
    closure_raw["session_id"] = "../escape"
    with pytest.raises(ValidationError, match="session_id must be safe"):
        SessionClosureV1.model_validate(closure_raw)

    closure_raw["session_id"] = closure.session_id
    closure_raw["stop_reason"] = "not_allowlisted"
    with pytest.raises(ValidationError, match="Input should be"):
        SessionClosureV1.model_validate(closure_raw)


def test_closure_rejects_time_reversal_and_capture_path_escape(tmp_path: Path) -> None:
    output, external = _roots(tmp_path)
    capture = output / "segments"
    capture.mkdir()
    outside = tmp_path / "outside-capture"
    outside.mkdir()
    start = _start(output, external)
    start_write = write_session_start(start, output_root=output)

    with pytest.raises(ValueError, match="UTC time precedes"):
        build_session_closure(
            start_path=start_write.path,
            capture_directory=capture,
            stop_reason="operator_requested",
            fatal=False,
            closed_at_ms=STARTED_AT_MS - 1,
            closed_monotonic_ns=STARTED_MONOTONIC_NS,
        )
    with pytest.raises(ValueError, match="monotonic time precedes"):
        build_session_closure(
            start_path=start_write.path,
            capture_directory=capture,
            stop_reason="operator_requested",
            fatal=False,
            closed_at_ms=STARTED_AT_MS,
            closed_monotonic_ns=STARTED_MONOTONIC_NS - 1,
        )
    with pytest.raises(ValueError, match="within session output_root"):
        build_session_closure(
            start_path=start_write.path,
            capture_directory=outside,
            stop_reason="operator_requested",
            fatal=False,
            closed_at_ms=STARTED_AT_MS,
            closed_monotonic_ns=STARTED_MONOTONIC_NS,
        )


def test_start_path_escape_and_symlinked_roots_are_rejected_when_supported(
    tmp_path: Path,
) -> None:
    output, external = _roots(tmp_path)
    capture = output / "segments"
    capture.mkdir()
    start = _start(output, external)
    start_write = write_session_start(start, output_root=output)
    copied = tmp_path / start_write.path.name
    shutil.copyfile(start_write.path, copied)
    with pytest.raises(ValueError, match="escapes or differs"):
        build_session_closure(
            start_path=copied,
            capture_directory=capture,
            stop_reason="operator_requested",
            fatal=False,
            closed_at_ms=STARTED_AT_MS,
            closed_monotonic_ns=STARTED_MONOTONIC_NS,
        )

    real_output = tmp_path / "real-output"
    real_output.mkdir()
    linked_output = tmp_path / "linked-output"
    try:
        linked_output.symlink_to(real_output, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symbolic links are unavailable: {exc}")
    with pytest.raises(ValueError, match="symbolic-link"):
        build_session_start(
            protocol_sha256=FROZEN_PROTOCOL_SHA256,
            source_manifest_sha256="a" * 64,
            config_sha256="b" * 64,
            output_root=linked_output,
            external_audit_root=external,
            started_at_ms=STARTED_AT_MS,
            started_monotonic_ns=STARTED_MONOTONIC_NS,
            boot_uuid=BOOT_UUID,
        )


def test_session_documents_expose_no_efficacy_fields(tmp_path: Path) -> None:
    output, external = _roots(tmp_path)
    capture = output / "segments"
    capture.mkdir()
    start = _start(output, external)
    start_write = write_session_start(start, output_root=output)
    closure = build_session_closure(
        start_path=start_write.path,
        capture_directory=capture,
        stop_reason="operator_requested",
        fatal=False,
        closed_at_ms=STARTED_AT_MS,
        closed_monotonic_ns=STARTED_MONOTONIC_NS,
    )
    keys = {
        *_all_keys(start.model_dump(mode="json")),
        *_all_keys(closure.model_dump(mode="json")),
    }
    assert keys.isdisjoint({"pnl", "outcome", "return", "label", "threshold", "signal", "order"})


def _roots(tmp_path: Path) -> tuple[Path, Path]:
    output = tmp_path / "capture-output"
    external = tmp_path / "audit-heads"
    output.mkdir()
    external.mkdir()
    return output.resolve(), external.resolve()


def _start(output: Path, external: Path) -> SessionStartV1:
    return build_session_start(
        protocol_sha256=FROZEN_PROTOCOL_SHA256,
        source_manifest_sha256="a" * 64,
        config_sha256="b" * 64,
        output_root=output,
        external_audit_root=external,
        started_at_ms=STARTED_AT_MS,
        started_monotonic_ns=STARTED_MONOTONIC_NS,
        boot_uuid=BOOT_UUID,
    )


def _write_segment(
    capture: Path,
    start: SessionStartV1,
    *,
    record_count: int,
) -> None:
    writer = SegmentedCaptureWriter(
        capture,
        plan_sha256=start.capture_plan_sha256,
        process_boot_id=start.process_boot_id,
        maximum_total_bytes=4 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    for ingest_seq in range(1, record_count + 1):
        record = CaptureEnvelopeV1(
            received_at_ms=STARTED_AT_MS + ingest_seq,
            received_monotonic_ns=STARTED_MONOTONIC_NS + ingest_seq,
            plan_sha256=start.capture_plan_sha256,
            process_boot_id=start.process_boot_id,
            connection_id="capture-spot-1",
            frame_seq=ingest_seq,
            ingest_seq=ingest_seq,
            market=Market.SPOT,
            route="spot",
            stream="btcusdt@aggTrade",
            subscription_streams=("btcusdt@aggTrade",),
            raw_payload="{}",
        )
        writer.append(record, record_to_json_line(record))
    writer.close()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return {
            *(str(key).casefold() for key in value),
            *(key for item in value.values() for key in _all_keys(item)),
        }
    if isinstance(value, list | tuple):
        return {key for item in value for key in _all_keys(item)}
    return set()
