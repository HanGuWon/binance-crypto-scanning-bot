from __future__ import annotations

from dataclasses import asdict, replace

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    build_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.rest_depth import (
    PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8,
    PublicDepthRestAttemptPayloadV8,
    PublicDepthRestErrorCategoryV8,
    PublicDepthRestTerminalObservationV8,
    public_depth_rest_attempt_payload_sha256_v8,
    public_depth_rest_plan_sha256_v8,
    public_depth_rest_source_logical_key_v8,
)

_SESSION_ID = "depth-rest-evidence-session"
_PROTOCOL_HASH = "0" * 64
_CONNECTION_ID = "usdm-public-g000003"
_BUILT_REQUEST_HEADERS = (
    ("accept", "application/json"),
    ("accept-encoding", "identity"),
    ("connection", "keep-alive"),
    ("host", "fapi.binance.com"),
    ("user-agent", "binance-signalbot-r4b-v2-capture/1"),
)


def _plan(symbols: tuple[str, ...] = ("BTCUSDT",)) -> ProvisionalDepthRestQualificationPlanV8:
    plans = build_provisional_promoting_capture_plans_v8(symbols)
    [plan] = [
        item for item in plans if type(item) is ProvisionalDepthRestQualificationPlanV8
    ]
    return plan


def _observation(
    body: bytes = b"{}",
    *,
    response_status: int | None = 200,
    payload_complete: bool = True,
    error_category: PublicDepthRestErrorCategoryV8 | None = None,
    error_detail: str | None = None,
    bridge_attempt: int = 1,
) -> PublicDepthRestTerminalObservationV8:
    headers = () if response_status is None else (("content-type", "application/json"),)
    first_wall = None if response_status is None else 1_003
    first_monotonic = None if response_status is None else 10_003
    return PublicDepthRestTerminalObservationV8.for_plan(
        _plan(),
        session_id=_SESSION_ID,
        protocol_hash=_PROTOCOL_HASH,
        connection_id=_CONNECTION_ID,
        method="GET",
        base_url="https://fapi.binance.com",
        endpoint="/fapi/v1/depth",
        symbol="BTCUSDT",
        canonical_query=(("limit", "1000"), ("symbol", "BTCUSDT")),
        request_headers=_BUILT_REQUEST_HEADERS,
        trigger="startup",
        trigger_seq=1,
        connection_generation=3,
        first_buffered_u=100,
        symbol_ordinal=0,
        bridge_attempt=bridge_attempt,
        request_started_wall_ms=1_002,
        request_started_monotonic_ns=10_002,
        response_first_header_wall_ms=first_wall,
        response_first_header_monotonic_ns=first_monotonic,
        attempt_ended_wall_ms=1_004,
        attempt_ended_monotonic_ns=10_004,
        response_status=response_status,
        response_headers=headers,
        payload_complete=payload_complete,
        body=body,
        error_category=error_category,
        error_detail=error_detail,
    )


def _payload(body: bytes = b"{}") -> PublicDepthRestAttemptPayloadV8:
    return _observation(body).build_payload(ReceiptTimestamp(1_005, 10_005))


def test_exact_plan_query_round_trip_and_hashes_are_deterministic() -> None:
    plan = _plan()
    observation = _observation()
    payload = _payload()
    encoded = payload.canonical_bytes()

    restored = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(encoded, plan=plan)

    assert restored == payload
    assert payload.method == "GET"
    assert payload.base_url == "https://fapi.binance.com"
    assert payload.endpoint == "/fapi/v1/depth"
    assert payload.canonical_query == (("limit", "1000"), ("symbol", "BTCUSDT"))
    assert payload.request_headers == _BUILT_REQUEST_HEADERS
    assert (
        payload.session_id,
        payload.protocol_hash,
        payload.connection_id,
        payload.connection_generation,
    ) == (_SESSION_ID, _PROTOCOL_HASH, _CONNECTION_ID, 3)
    assert payload.first_buffered_u == 100
    assert payload.schema_version == "r4b_v2_public_depth_rest_attempt_v9"
    assert payload.plan_sha256 == public_depth_rest_plan_sha256_v8(plan)
    assert payload.plan_sha256 == (
        "2491fd53e4a0d19747188f8927a7475d2ff5c1e5e9977f8dbec80eef1365dea1"
    )
    assert observation.http_attempt == 1
    assert public_depth_rest_attempt_payload_sha256_v8(restored) == (
        public_depth_rest_attempt_payload_sha256_v8(payload)
    )
    assert public_depth_rest_attempt_payload_sha256_v8(payload) == (
        "1a328aaa2412513760f1742814c349b257357769705054446100e5a7af83018a"
    )
    assert public_depth_rest_source_logical_key_v8("BTCUSDT") == (
        "depthSnapshot:BTCUSDT"
    )
    assert payload.qualification_only is True
    assert payload.promoting is False
    assert payload.promotion_ready is False
    assert payload.wal_durability_verified is False
    assert payload.finality_fence_verified is False
    assert payload.m2_certified is False
    assert payload.book_bridge_certified is False
    assert payload.liquidity_signal_emitted is False
    assert payload.order_execution_enabled is False


def test_raw_wall_regression_and_late_resume_duration_are_retained() -> None:
    observation = replace(
        _observation(),
        response_first_header_wall_ms=1_001,
        attempt_ended_wall_ms=1_000,
        attempt_ended_monotonic_ns=4_000_010_003,
    )

    payload = observation.build_payload(
        ReceiptTimestamp(999, observation.attempt_ended_monotonic_ns)
    )

    assert payload.request_started_wall_ms == 1_002
    assert payload.response_first_header_wall_ms == 1_001
    assert payload.attempt_ended_wall_ms == 1_000
    assert payload.completion_admission_wall_ms == 999
    assert payload.attempt_ended_monotonic_ns - (
        payload.request_started_monotonic_ns
    ) > 4_000_000_000


def test_complete_non_2xx_late_resume_retains_timeout_primary() -> None:
    observation = _observation(
        b'{"code":-1003}',
        response_status=429,
        payload_complete=True,
        error_category=PublicDepthRestErrorCategoryV8.TIMEOUT,
        error_detail="event loop resumed after the total response-body deadline",
    )

    payload = observation.build_payload(ReceiptTimestamp(1_005, 10_005))

    assert payload.response_status == 429
    assert payload.payload_complete is True
    assert payload.body_bytes() == b'{"code":-1003}'
    assert payload.error_category is PublicDepthRestErrorCategoryV8.TIMEOUT


def test_plan_and_attempt_hashes_change_with_exact_authority_or_body() -> None:
    btc_plan = _plan(("BTCUSDT",))
    eth_plan = _plan(("ETHUSDT",))

    assert public_depth_rest_plan_sha256_v8(btc_plan) != (
        public_depth_rest_plan_sha256_v8(eth_plan)
    )
    assert public_depth_rest_attempt_payload_sha256_v8(_payload(b"{}")) != (
        public_depth_rest_attempt_payload_sha256_v8(_payload(b"{ }"))
    )


def test_attempt_hash_binds_generation_lineage_and_first_buffered_sequence() -> None:
    payload = _payload()
    baseline = public_depth_rest_attempt_payload_sha256_v8(payload)

    assert public_depth_rest_attempt_payload_sha256_v8(
        replace(payload, connection_id="usdm-public-g000003-alternate")
    ) != baseline
    assert public_depth_rest_attempt_payload_sha256_v8(
        replace(payload, first_buffered_u=101)
    ) != baseline


def test_legacy_v8_canonical_shape_is_rejected_instead_of_ambiguously_decoded() -> None:
    legacy_document = asdict(_payload())
    for field_name in (
        "session_id",
        "protocol_hash",
        "connection_id",
        "request_headers",
        "first_buffered_u",
    ):
        del legacy_document[field_name]
    legacy_document["schema_version"] = "r4b_v2_public_depth_rest_attempt_v8"
    legacy_bytes = canonical_json_line(legacy_document)

    with pytest.raises(ValueError, match="fields differ"):
        PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
            legacy_bytes,
            plan=_plan(),
        )


def test_body_at_exact_byte_cap_round_trips_and_one_byte_over_fails() -> None:
    body = b"x" * PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8
    payload = _payload(body)

    restored = PublicDepthRestAttemptPayloadV8.from_canonical_bytes(
        payload.canonical_bytes(),
        plan=_plan(),
    )

    assert restored.body_len == PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8
    assert restored.body_bytes() == body
    with pytest.raises(ValueError, match="byte cap"):
        _observation(body + b"x")


@pytest.mark.parametrize("bridge_attempt", [1, 3])
def test_bridge_attempt_boundaries_are_accepted(bridge_attempt: int) -> None:
    assert _observation(bridge_attempt=bridge_attempt).bridge_attempt == bridge_attempt


@pytest.mark.parametrize("bridge_attempt", [0, 4])
def test_bridge_attempt_outside_bound_is_rejected(bridge_attempt: int) -> None:
    with pytest.raises(ValueError, match=r"bridge[_ ]attempt"):
        _observation(bridge_attempt=bridge_attempt)


def test_failure_attempt_shapes_are_retained_but_structurally_exact() -> None:
    http_error = _observation(
        b'{"code":-1}',
        response_status=500,
        error_category=PublicDepthRestErrorCategoryV8.HTTP_STATUS,
        error_detail="server status 500",
    )
    network_error = _observation(
        b"",
        response_status=None,
        payload_complete=False,
        error_category=PublicDepthRestErrorCategoryV8.NETWORK,
        error_detail="connection failed",
    )

    assert http_error.build_payload(ReceiptTimestamp(1_005, 10_005)).response_status == 500
    assert network_error.build_payload(ReceiptTimestamp(1_005, 10_005)).body_len == 0

    with pytest.raises(ValueError, match="complete non-2xx"):
        _observation(
            b"{}",
            response_status=200,
            error_category=PublicDepthRestErrorCategoryV8.HTTP_STATUS,
            error_detail="wrong category",
        )
    with pytest.raises(ValueError, match="empty incomplete"):
        _observation(
            b"x",
            response_status=None,
            payload_complete=False,
            error_category=PublicDepthRestErrorCategoryV8.NETWORK,
            error_detail="connection failed",
        )


def test_payload_plan_hash_drift_fails_exact_plan_binding() -> None:
    drifted = replace(_payload(), plan_sha256="0" * 64)
    with pytest.raises(ValueError, match="plan hash differs"):
        drifted.validate_against_plan(_plan())


def test_structural_query_and_http_attempt_drift_fail_during_construction() -> None:
    payload = _payload()
    with pytest.raises(ValueError, match="limit=1000"):
        replace(payload, canonical_query=(("symbol", "BTCUSDT"),))
    with pytest.raises(ValueError, match="keyless policy"):
        replace(
            payload,
            request_headers=(*payload.request_headers, ("x-mbx-apikey", "secret")),
        )
    with pytest.raises(ValueError, match="exactly one"):
        replace(payload, http_attempt=2)


@pytest.mark.parametrize(
    ("field", "value", "match"),
    (
        ("session_id", "", "session_id"),
        ("protocol_hash", "A" * 64, "protocol_hash"),
        ("connection_id", " bad", "connection_id"),
        ("first_buffered_u", -1, "first_buffered_u"),
    ),
)
def test_lineage_and_first_buffered_sequence_are_strictly_validated(
    field: str,
    value: str | int,
    match: str,
) -> None:
    with pytest.raises(ValueError, match=match):
        replace(_payload(), **{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("qualification_only", False),
        ("promoting", True),
        ("promotion_ready", True),
        ("wal_durability_verified", True),
        ("finality_fence_verified", True),
        ("m2_certified", True),
        ("book_bridge_certified", True),
        ("liquidity_signal_emitted", True),
        ("order_execution_enabled", True),
    ),
)
def test_forbidden_authority_claims_fail_closed(field: str, value: bool) -> None:
    with pytest.raises(ValueError, match="forbidden promotion or execution claim"):
        replace(_payload(), **{field: value})


def test_canonical_body_hash_and_base64_tampering_fail_closed() -> None:
    payload = _payload(b"snapshot")
    with pytest.raises(ValueError, match="body_sha256"):
        replace(payload, body_sha256="0" * 64)
    with pytest.raises(ValueError, match="body_len"):
        replace(payload, body_len=payload.body_len + 1)
    with pytest.raises(ValueError, match="base64"):
        replace(payload, body_base64="***")
