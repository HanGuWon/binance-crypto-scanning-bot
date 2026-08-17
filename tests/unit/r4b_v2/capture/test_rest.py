from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import replace
from typing import cast

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.batching import QueuedRawRecordV2
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2, VenueV2
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2
from signalbot.r4b_v2.capture.rest import (
    PUBLIC_OI_REST_BASE_URL_V2,
    PUBLIC_OI_REST_ENDPOINT_V2,
    PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2,
    PublicOiRestAttemptPayloadV2,
    PublicOiRestErrorCategoryV2,
    PublicOiRestMissedSlotV2,
    PublicOiRestPayloadBuilderV2,
    PublicOiRestTerminalObservationV2,
    public_oi_rest_source_logical_key_v2,
)

_SLOT = 1_700_000_000_000


def _plan(
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT"),
) -> ProvisionalPromotingRestCapturePlanV2:
    return ProvisionalPromotingRestCapturePlanV2(
        name="v2-usdm-public-rest-oi-promoting-abc",
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public_rest",
        method="GET",
        endpoint="/fapi/v1/openInterest",
        symbols=symbols,
    )


def _success_observation(
    *,
    plan: ProvisionalPromotingRestCapturePlanV2 | None = None,
    symbol: str = "BTCUSDT",
    symbol_ordinal: int = 0,
    body: bytes = b'{"openInterest":"123.45","symbol":"BTCUSDT","time":1}',
) -> PublicOiRestTerminalObservationV2:
    selected_plan = _plan() if plan is None else plan
    return PublicOiRestTerminalObservationV2.for_plan(
        selected_plan,
        symbol=symbol,
        poll_cycle_seq=1,
        symbol_ordinal=symbol_ordinal,
        scheduled_slot_wall_ms=_SLOT,
        attempt=1,
        request_started_wall_ms=_SLOT + 1,
        request_started_monotonic_ns=10_000,
        response_first_header_wall_ms=_SLOT + 2,
        response_first_header_monotonic_ns=10_001,
        attempt_ended_wall_ms=_SLOT + 2,
        attempt_ended_monotonic_ns=10_002,
        response_status=200,
        response_headers=(("content-type", "application/json"),),
        payload_complete=True,
        body=body,
    )


def _completion(
    *,
    wall_ms: int = _SLOT + 3,
    monotonic_ns: int = 10_002,
) -> ReceiptTimestamp:
    return ReceiptTimestamp(wall_ms, monotonic_ns)


def test_success_builder_attaches_ingress_completion_and_excludes_v1_lineage() -> None:
    plan = _plan()
    observation = _success_observation(plan=plan)
    builder: PublicOiRestPayloadBuilderV2 = observation

    encoded = builder(_completion())
    payload = PublicOiRestAttemptPayloadV2.from_canonical_bytes(encoded, plan=plan)
    document = json.loads(encoded)

    assert payload.method == "GET"
    assert payload.base_url == PUBLIC_OI_REST_BASE_URL_V2
    assert payload.endpoint == PUBLIC_OI_REST_ENDPOINT_V2
    assert payload.canonical_query == (("symbol", "BTCUSDT"),)
    assert payload.completion_admission_wall_ms == _SLOT + 3
    assert payload.completion_admission_monotonic_ns == 10_002
    assert payload.body_bytes() == observation.body
    assert payload.body_sha256 == hashlib.sha256(observation.body).hexdigest()
    assert document["body_base64"] == base64.b64encode(observation.body).decode("ascii")
    assert document["admission_cancellation_requested"] is False
    assert document["schema_version"] == "r4b_v2_public_oi_rest_attempt_v2"
    assert {
        "ingest_seq",
        "plan_id",
        "plan_sha256",
        "process_boot_id",
        "correlation_id",
    }.isdisjoint(document)
    assert public_oi_rest_source_logical_key_v2("BTCUSDT") == (
        "openInterest:BTCUSDT"
    )


def test_binary_body_is_lossless_deterministic_and_round_trips() -> None:
    plan = _plan()
    body = b"\x00\xff\x80\nraw\x00"
    payload = _success_observation(plan=plan, body=body).build_payload(_completion())

    first = payload.canonical_bytes()
    second = payload.canonical_bytes()
    restored = PublicOiRestAttemptPayloadV2.from_canonical_bytes(first, plan=plan)

    assert first == second
    assert restored == payload
    assert restored.body_bytes() == body
    assert restored.canonical_bytes() == first


@pytest.mark.parametrize(
    "category",
    [
        PublicOiRestErrorCategoryV2.NETWORK,
        PublicOiRestErrorCategoryV2.TIMEOUT,
        PublicOiRestErrorCategoryV2.PROTOCOL,
        PublicOiRestErrorCategoryV2.CANCELLED,
    ],
)
def test_pre_header_error_representations_are_empty_and_incomplete(
    category: PublicOiRestErrorCategoryV2,
) -> None:
    plan = _plan()
    observation = PublicOiRestTerminalObservationV2.for_plan(
        plan,
        symbol="BTCUSDT",
        poll_cycle_seq=2,
        symbol_ordinal=0,
        scheduled_slot_wall_ms=_SLOT,
        attempt=1,
        request_started_wall_ms=_SLOT,
        request_started_monotonic_ns=20_000,
        response_first_header_wall_ms=None,
        response_first_header_monotonic_ns=None,
        attempt_ended_wall_ms=_SLOT,
        attempt_ended_monotonic_ns=20_000,
        response_status=None,
        response_headers=(),
        payload_complete=False,
        body=b"",
        error_category=category,
        error_detail=f"sanitized {category.value} before response headers",
    )

    restored = PublicOiRestAttemptPayloadV2.from_canonical_bytes(
        observation(_completion(monotonic_ns=20_001)),
        plan=plan,
    )

    assert restored.response_status is None
    assert restored.body_bytes() == b""
    assert restored.payload_complete is False
    assert restored.error_category is category


def test_complete_non_2xx_preserves_body_headers_and_http_status() -> None:
    plan = _plan()
    body = b'{"code":-1003}'
    observation = PublicOiRestTerminalObservationV2.for_plan(
        plan,
        symbol="ETHUSDT",
        poll_cycle_seq=3,
        symbol_ordinal=1,
        scheduled_slot_wall_ms=_SLOT,
        attempt=1,
        request_started_wall_ms=_SLOT + 1,
        request_started_monotonic_ns=30_000,
        response_first_header_wall_ms=_SLOT + 2,
        response_first_header_monotonic_ns=30_001,
        attempt_ended_wall_ms=_SLOT + 2,
        attempt_ended_monotonic_ns=30_002,
        response_status=429,
        response_headers=(
            ("content-type", "application/json"),
            ("date", "Sun, 19 Jul 2026 00:00:00 GMT"),
            ("retry-after", "2"),
            ("x-mbx-used-weight-1m", "12"),
        ),
        payload_complete=True,
        body=body,
        error_category=PublicOiRestErrorCategoryV2.HTTP_STATUS,
        error_detail="HTTP status 429",
    )

    payload = observation.build_payload(_completion(monotonic_ns=30_002))

    assert payload.response_status == 429
    assert payload.body_bytes() == body
    assert payload.error_category is PublicOiRestErrorCategoryV2.HTTP_STATUS


def test_complete_non_2xx_observed_after_deadline_retains_timeout_primary() -> None:
    observation = replace(
        _success_observation(body=b'{"code":-1003}'),
        response_status=429,
        error_category=PublicOiRestErrorCategoryV2.TIMEOUT,
        error_detail="event loop resumed after the total response-body deadline",
    )

    payload = observation.build_payload(_completion())

    assert payload.response_status == 429
    assert payload.payload_complete is True
    assert payload.body_bytes() == b'{"code":-1003}'
    assert payload.error_category is PublicOiRestErrorCategoryV2.TIMEOUT


def test_body_limit_retains_the_exact_cap_as_an_incomplete_prefix() -> None:
    body = bytes(range(256)) * (PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2 // 256)
    observation = replace(
        _success_observation(),
        payload_complete=False,
        body=body,
        error_category=PublicOiRestErrorCategoryV2.BODY_LIMIT,
        error_detail="response body exceeded the configured byte cap",
    )

    payload = observation.build_payload(_completion())

    assert payload.body_len == PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2
    assert payload.body_bytes() == body
    assert payload.payload_complete is False


@pytest.mark.parametrize(
    ("payload_complete", "body", "status", "expected_stage"),
    [
        (False, b"partial", 200, "body"),
        (True, b"complete", 200, "close"),
        (True, b'{"code":-1003}', 429, "non-2xx close"),
    ],
)
def test_cancelled_attempts_retain_the_exact_stage_body(
    payload_complete: bool,
    body: bytes,
    status: int,
    expected_stage: str,
) -> None:
    observation = replace(
        _success_observation(body=body),
        response_status=status,
        payload_complete=payload_complete,
        error_category=PublicOiRestErrorCategoryV2.CANCELLED,
        error_detail=f"request cancelled during {expected_stage}",
    )

    payload = observation.build_payload(_completion())

    assert payload.body_bytes() == body
    assert payload.payload_complete is payload_complete
    assert payload.error_category is PublicOiRestErrorCategoryV2.CANCELLED


@pytest.mark.parametrize(
    ("category", "payload_complete"),
    [
        (PublicOiRestErrorCategoryV2.RESPONSE_READ, False),
        (PublicOiRestErrorCategoryV2.TIMEOUT, False),
        (PublicOiRestErrorCategoryV2.TIMEOUT, True),
        (PublicOiRestErrorCategoryV2.RESPONSE_CLOSE, True),
    ],
)
def test_post_header_read_timeout_and_close_error_representations(
    category: PublicOiRestErrorCategoryV2,
    payload_complete: bool,
) -> None:
    observation = replace(
        _success_observation(body=b"retained"),
        payload_complete=payload_complete,
        error_category=category,
        error_detail=f"sanitized {category.value} after response headers",
    )

    assert observation.build_payload(_completion()).body_bytes() == b"retained"


@pytest.mark.parametrize(
    ("field_name", "invalid"),
    [
        ("method", "POST"),
        ("base_url", "https://api.binance.com"),
        ("endpoint", "/fapi/v1/openInterest/private"),
        ("symbol", "btcusdt"),
        ("canonical_query", (("symbol", "ETHUSDT"),)),
        ("poll_cycle_seq", 0),
        ("poll_cycle_seq", True),
        ("poll_cycle_seq", 1.0),
        ("symbol_ordinal", -1),
        ("symbol_ordinal", 32),
        ("symbol_ordinal", False),
        ("symbol_ordinal", 0.0),
        ("scheduled_slot_wall_ms", -1),
        ("scheduled_slot_wall_ms", _SLOT + 1),
        ("scheduled_slot_wall_ms", True),
        ("scheduled_slot_wall_ms", float(_SLOT)),
        ("attempt", 0),
        ("attempt", 2),
        ("attempt", True),
        ("attempt", 1.0),
        ("request_started_wall_ms", -1),
        ("request_started_wall_ms", True),
        ("request_started_wall_ms", float(_SLOT)),
        ("request_started_monotonic_ns", -1),
        ("request_started_monotonic_ns", False),
        ("request_started_monotonic_ns", 10_000.0),
        ("attempt_ended_wall_ms", -1),
        ("attempt_ended_wall_ms", True),
        ("attempt_ended_monotonic_ns", -1),
        ("attempt_ended_monotonic_ns", 10_002.0),
        ("response_first_header_wall_ms", True),
        ("response_first_header_wall_ms", float(_SLOT + 2)),
        ("response_first_header_monotonic_ns", False),
        ("response_first_header_monotonic_ns", 10_001.0),
        ("response_status", True),
        ("response_status", 200.0),
        ("payload_complete", 1),
        ("admission_cancellation_requested", 1),
        ("body", bytearray(b"mutable")),
    ],
)
def test_strict_attempt_fields_reject_invalid_bool_float_and_policy_drift(
    field_name: str,
    invalid: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        replace(_success_observation(), **{field_name: invalid})


def test_request_start_slot_boundary_is_half_open() -> None:
    valid = replace(
        _success_observation(),
        request_started_wall_ms=_SLOT + 4_999,
        response_first_header_wall_ms=_SLOT + 4_999,
        attempt_ended_wall_ms=_SLOT + 4_999,
    )

    assert valid.request_started_wall_ms == _SLOT + 4_999
    with pytest.raises(ValueError, match="assigned UTC slot"):
        replace(valid, request_started_wall_ms=_SLOT + 5_000)
    with pytest.raises(ValueError, match="assigned UTC slot"):
        replace(valid, request_started_wall_ms=_SLOT - 1)


def test_attempt_duration_is_raw_evidence_not_a_deadline_enforcement_bound() -> None:
    observation = replace(
        _success_observation(),
        attempt_ended_wall_ms=_SLOT + 4_001,
        attempt_ended_monotonic_ns=10_000 + 4_000_000_000,
    )
    late_admission = ReceiptTimestamp(
        _SLOT + 100_000,
        observation.attempt_ended_monotonic_ns + 100_000_000_000,
    )

    payload = observation.build_payload(late_admission)

    assert payload.attempt_ended_monotonic_ns == 4_000_010_000
    assert payload.completion_admission_monotonic_ns > payload.attempt_ended_monotonic_ns
    resumed_late = replace(
        observation,
        attempt_ended_monotonic_ns=10_000 + 4_000_000_001,
    )
    assert resumed_late.attempt_ended_monotonic_ns == 4_000_010_001


def test_raw_wall_clock_regression_is_retained_under_monotonic_causality() -> None:
    observation = replace(
        _success_observation(),
        response_first_header_wall_ms=_SLOT,
        attempt_ended_wall_ms=_SLOT - 1,
    )
    payload = observation.build_payload(
        ReceiptTimestamp(_SLOT - 2, observation.attempt_ended_monotonic_ns)
    )

    assert payload.request_started_wall_ms == _SLOT + 1
    assert payload.response_first_header_wall_ms == _SLOT
    assert payload.attempt_ended_wall_ms == _SLOT - 1
    assert payload.completion_admission_wall_ms == _SLOT - 2
    first_header_monotonic_ns = payload.response_first_header_monotonic_ns
    assert first_header_monotonic_ns is not None
    assert payload.request_started_monotonic_ns < first_header_monotonic_ns
    assert first_header_monotonic_ns < payload.attempt_ended_monotonic_ns


@pytest.mark.parametrize(
    "changes",
    [
        {"attempt_ended_monotonic_ns": 9_999},
        {"attempt_ended_monotonic_ns": 10_000},
    ],
)
def test_attempt_end_cannot_precede_start_or_first_header(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="attempt end"):
        replace(_success_observation(), **changes)


def test_admission_cancellation_replaces_once_and_preserves_terminal_evidence() -> None:
    original = replace(
        _success_observation(body=b"retained"),
        response_status=429,
        error_category=PublicOiRestErrorCategoryV2.HTTP_STATUS,
        error_detail="HTTP status 429",
    )

    cancelled = original.with_admission_cancellation_v2()

    assert cancelled is not original
    assert cancelled.response_status == original.response_status
    assert cancelled.body == original.body
    assert cancelled.payload_complete is original.payload_complete
    assert cancelled.attempt_ended_wall_ms == original.attempt_ended_wall_ms
    assert cancelled.error_category is original.error_category
    assert cancelled.error_detail == original.error_detail
    assert cancelled.admission_cancellation_requested is True
    assert original.admission_cancellation_requested is False
    assert cancelled.build_payload(_completion()).admission_cancellation_requested is True
    assert cancelled.with_admission_cancellation_v2() is cancelled


def test_missed_slot_exception_is_strict_and_carries_exact_identity() -> None:
    missed = PublicOiRestMissedSlotV2(
        symbol="BTCUSDT",
        poll_cycle_seq=7,
        symbol_ordinal=0,
        scheduled_slot_wall_ms=_SLOT,
        observed_request_start_wall_ms=_SLOT + 5_000,
    )

    assert missed.symbol == "BTCUSDT"
    assert missed.poll_cycle_seq == 7
    assert missed.symbol_ordinal == 0
    assert missed.scheduled_slot_wall_ms == _SLOT
    assert missed.observed_request_start_wall_ms == _SLOT + 5_000


@pytest.mark.parametrize(
    "changes",
    [
        {"symbol": "btcusdt"},
        {"poll_cycle_seq": 0},
        {"poll_cycle_seq": True},
        {"symbol_ordinal": -1},
        {"symbol_ordinal": 32},
        {"scheduled_slot_wall_ms": _SLOT + 1},
        {"observed_request_start_wall_ms": _SLOT + 4_999},
        {"observed_request_start_wall_ms": True},
    ],
)
def test_missed_slot_exception_rejects_invalid_or_in_slot_evidence(
    changes: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "symbol": "BTCUSDT",
        "poll_cycle_seq": 7,
        "symbol_ordinal": 0,
        "scheduled_slot_wall_ms": _SLOT,
        "observed_request_start_wall_ms": _SLOT + 5_000,
    }
    values.update(changes)
    with pytest.raises((TypeError, ValueError)):
        PublicOiRestMissedSlotV2(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "changes",
    [
        {"response_first_header_wall_ms": None},
        {"response_first_header_monotonic_ns": None},
        {"response_first_header_monotonic_ns": 9_999},
        {"response_status": None},
        {"response_status": 99},
        {"response_status": 600},
        {"response_headers": (("Content-Type", "application/json"),)},
        {"response_headers": (("authorization", "redacted"),)},
        {"response_headers": (("content-type", " padded "),)},
        {"response_headers": (("retry-after", "2"), ("date", "today"))},
    ],
)
def test_header_status_and_clock_contract_rejects_inconsistent_values(
    changes: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        replace(_success_observation(), **changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"error_category": PublicOiRestErrorCategoryV2.TIMEOUT},
        {"error_detail": "detail without category"},
        {
            "payload_complete": False,
            "error_category": None,
            "error_detail": None,
        },
        {
            "response_status": 429,
            "error_category": None,
            "error_detail": None,
        },
        {
            "response_status": 200,
            "error_category": PublicOiRestErrorCategoryV2.HTTP_STATUS,
            "error_detail": "HTTP status 200",
        },
        {
            "payload_complete": False,
            "error_category": PublicOiRestErrorCategoryV2.RESPONSE_CLOSE,
            "error_detail": "close before body completion",
        },
        {
            "payload_complete": False,
            "body": b"short",
            "error_category": PublicOiRestErrorCategoryV2.BODY_LIMIT,
            "error_detail": "body limit",
        },
        {
            "error_category": PublicOiRestErrorCategoryV2.CANCELLED,
            "error_detail": "token=must-not-be-retained",
        },
        {
            "error_category": PublicOiRestErrorCategoryV2.CANCELLED,
            "error_detail": " https://fapi.binance.com/private ",
        },
    ],
)
def test_error_contract_rejects_inconsistent_or_unsafe_representations(
    changes: dict[str, object],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        replace(_success_observation(), **changes)


def test_pre_header_errors_reject_body_headers_and_post_header_categories() -> None:
    base = replace(
        _success_observation(),
        response_first_header_wall_ms=None,
        response_first_header_monotonic_ns=None,
        response_status=None,
        response_headers=(),
        payload_complete=False,
        body=b"",
        error_category=PublicOiRestErrorCategoryV2.NETWORK,
        error_detail="network failure before response headers",
    )
    invalid_changes = (
        {"body": b"unexpected"},
        {"response_headers": (("date", "today"),)},
        {
            "error_category": PublicOiRestErrorCategoryV2.RESPONSE_READ,
            "error_detail": "read failure without response headers",
        },
    )

    for changes in invalid_changes:
        with pytest.raises((TypeError, ValueError)):
            replace(base, **changes)


def test_body_header_error_and_symbol_boundaries_are_exact() -> None:
    maximum_symbol = "A" * 26 + "USDT"
    boundary = PublicOiRestTerminalObservationV2(
        method="GET",
        base_url=PUBLIC_OI_REST_BASE_URL_V2,
        endpoint=PUBLIC_OI_REST_ENDPOINT_V2,
        symbol=maximum_symbol,
        canonical_query=(("symbol", maximum_symbol),),
        poll_cycle_seq=1,
        symbol_ordinal=31,
        scheduled_slot_wall_ms=0,
        attempt=1,
        request_started_wall_ms=0,
        request_started_monotonic_ns=0,
        response_first_header_wall_ms=0,
        response_first_header_monotonic_ns=0,
        attempt_ended_wall_ms=0,
        attempt_ended_monotonic_ns=0,
        response_status=200,
        response_headers=tuple(
            (f"x-mbx-used-weight-{index:02d}", str(index)) for index in range(16)
        ),
        payload_complete=True,
        body=b"x" * PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2,
    )
    payload = boundary.build_payload(ReceiptTimestamp(0, 0))

    assert payload.symbol_ordinal == 31
    assert payload.body_len == PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2
    assert len(payload.response_headers) == 16
    assert public_oi_rest_source_logical_key_v2(maximum_symbol).endswith(maximum_symbol)

    with pytest.raises(ValueError, match="byte cap"):
        replace(boundary, body=b"x" * (PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2 + 1))
    with pytest.raises(ValueError, match="member count"):
        replace(
            boundary,
            response_headers=tuple(
                (f"x-mbx-used-weight-{index:02d}", str(index)) for index in range(17)
            ),
        )
    with pytest.raises(ValueError, match="normalized lowercase"):
        replace(
            boundary,
            response_headers=(("x-mbx-used-weight-" + "x" * 129, "1"),),
        )
    with pytest.raises(ValueError, match="bounded value"):
        replace(boundary, response_headers=(("date", "x" * 257),))
    with pytest.raises(ValueError, match="symbol"):
        replace(boundary, symbol="A" * 27 + "USDT")


def test_worst_case_valid_attempt_fits_current_20k_wal_record_bound() -> None:
    plan = _plan(("BTCUSDT",))
    observation = PublicOiRestTerminalObservationV2.for_plan(
        plan,
        symbol="BTCUSDT",
        poll_cycle_seq=1,
        symbol_ordinal=0,
        scheduled_slot_wall_ms=0,
        attempt=1,
        request_started_wall_ms=1,
        request_started_monotonic_ns=1,
        response_first_header_wall_ms=2,
        response_first_header_monotonic_ns=2,
        attempt_ended_wall_ms=2,
        attempt_ended_monotonic_ns=3,
        response_status=200,
        response_headers=tuple(("date", "x" * 256) for _ in range(16)),
        payload_complete=False,
        body=b"x" * PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2,
        error_category=PublicOiRestErrorCategoryV2.BODY_LIMIT,
        error_detail="response body exceeded the configured byte cap",
    )
    completion = ReceiptTimestamp(3, 3)
    raw = RawRecordV2.from_payload(
        session_id="s" * 256,
        plan_id=plan.name,
        protocol_hash="a" * 64,
        transport=TransportV2.HTTPS,
        venue=VenueV2.USDM_FUTURES,
        route_id=plan.route_id,
        symbol="BTCUSDT",
        connection_id="c" * 256,
        generation=1,
        frame_seq=None,
        ingest_seq=1,
        receipt_wall_ms=completion.received_at_ms,
        receipt_monotonic_ns=completion.received_monotonic_ns,
        raw_payload=observation(completion),
        source_logical_key="openInterest:BTCUSDT",
    )

    queued = QueuedRawRecordV2.encode(raw, enqueued_monotonic_ns=4)

    assert queued.encoded_len <= 20_000


@pytest.mark.parametrize("status", [100, 599])
def test_http_status_boundaries_are_retained(status: int) -> None:
    observation = replace(
        _success_observation(),
        response_status=status,
        error_category=PublicOiRestErrorCategoryV2.HTTP_STATUS,
        error_detail=f"HTTP status {status}",
    )

    assert observation.build_payload(_completion()).response_status == status


def test_error_detail_length_boundary_is_exact() -> None:
    valid = replace(
        _success_observation(),
        error_category=PublicOiRestErrorCategoryV2.RESPONSE_CLOSE,
        error_detail="x" * 256,
    )

    assert len(valid.error_detail or "") == 256
    with pytest.raises(ValueError, match="bounded"):
        replace(valid, error_detail="x" * 257)


@pytest.mark.parametrize(
    "completion",
    [
        ReceiptTimestamp(-1, 10_002),
        ReceiptTimestamp(True, 10_002),
        ReceiptTimestamp(_SLOT + 3, -1),
        ReceiptTimestamp(_SLOT + 3, False),
        ReceiptTimestamp(_SLOT + 3, cast(int, 10_002.0)),
        ReceiptTimestamp(_SLOT + 3, 9_999),
    ],
)
def test_builder_rejects_invalid_or_backdated_completion_clocks(
    completion: ReceiptTimestamp,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _success_observation().build_payload(completion)


def test_builder_rejects_completion_before_first_header() -> None:
    with pytest.raises(ValueError, match="first header"):
        _success_observation().build_payload(_completion(monotonic_ns=10_000))


@pytest.mark.parametrize(
    "changes",
    [
        {"body_encoding": "text"},
        {"body_len": True},
        {"body_len": 1.0},
        {"body_len": 999},
        {"body_sha256": "f" * 64},
        {"body_sha256": "F" * 64},
        {"body_base64": "not-base64!"},
        {"completion_admission_wall_ms": True},
        {"completion_admission_wall_ms": 1.0},
        {"completion_admission_monotonic_ns": False},
        {"completion_admission_monotonic_ns": 10_002.0},
        {"completion_admission_monotonic_ns": 9_999},
        {"admission_cancellation_requested": 1},
        {"schema_version": "r4b_v2_public_oi_rest_attempt_v1"},
    ],
)
def test_payload_integrity_and_completion_fields_reject_tampering(
    changes: dict[str, object],
) -> None:
    payload = _success_observation().build_payload(_completion())
    with pytest.raises((TypeError, ValueError)):
        replace(payload, **changes)


@pytest.mark.parametrize(
    "mutation",
    [
        {"body_base64": base64.b64encode(b"tampered").decode("ascii")},
        {"body_len": 1},
        {"body_sha256": "f" * 64},
        {"admission_cancellation_requested": 1},
        {"schema_version": "r4b_v2_public_oi_rest_attempt_v1"},
        {"unexpected": "field"},
    ],
)
def test_canonical_parser_rejects_tamper_and_schema_drift(
    mutation: dict[str, object],
) -> None:
    document = json.loads(_success_observation()(_completion()))
    document.update(mutation)
    encoded = canonical_json_line(document)

    with pytest.raises((TypeError, ValueError)):
        PublicOiRestAttemptPayloadV2.from_canonical_bytes(encoded)


def test_canonical_parser_rejects_missing_noncanonical_duplicate_float_and_bad_json() -> None:
    canonical = _success_observation()(_completion())
    document = json.loads(canonical)
    del document["admission_cancellation_requested"]
    missing = canonical_json_line(document)
    noncanonical = canonical[:-1] + b" \n"
    float_value = canonical.replace(b'"poll_cycle_seq":1', b'"poll_cycle_seq":1.0')
    duplicate = canonical.replace(
        b'"attempt":1,',
        b'"attempt":1,"attempt":1,',
        1,
    )
    invalid_inputs = (
        missing,
        noncanonical,
        float_value,
        duplicate,
        canonical[:-1],
        canonical + b"{}\n",
        b"\xff\n",
        b"[]\n",
        b"not-json\n",
    )

    for encoded in invalid_inputs:
        with pytest.raises((TypeError, ValueError)):
            PublicOiRestAttemptPayloadV2.from_canonical_bytes(encoded)


def test_exact_plan_type_and_symbol_ordinal_are_required() -> None:
    plan = _plan()

    class StructuralLookalike:
        def __getattr__(self, name: str) -> object:
            return getattr(plan, name)

    fake = cast(ProvisionalPromotingRestCapturePlanV2, StructuralLookalike())
    with pytest.raises(TypeError, match="exact promoting REST plan"):
        PublicOiRestTerminalObservationV2.for_plan(
            fake,
            symbol="BTCUSDT",
            poll_cycle_seq=1,
            symbol_ordinal=0,
            scheduled_slot_wall_ms=_SLOT,
            attempt=1,
            request_started_wall_ms=_SLOT,
            request_started_monotonic_ns=1,
            response_first_header_wall_ms=_SLOT,
            response_first_header_monotonic_ns=2,
            attempt_ended_wall_ms=_SLOT,
            attempt_ended_monotonic_ns=2,
            response_status=200,
            response_headers=(),
            payload_complete=True,
            body=b"{}",
        )
    with pytest.raises(ValueError, match="ordinal"):
        PublicOiRestTerminalObservationV2.for_plan(
            plan,
            symbol="ETHUSDT",
            poll_cycle_seq=1,
            symbol_ordinal=0,
            scheduled_slot_wall_ms=_SLOT,
            attempt=1,
            request_started_wall_ms=_SLOT,
            request_started_monotonic_ns=1,
            response_first_header_wall_ms=_SLOT,
            response_first_header_monotonic_ns=2,
            attempt_ended_wall_ms=_SLOT,
            attempt_ended_monotonic_ns=2,
            response_status=200,
            response_headers=(),
            payload_complete=True,
            body=b"{}",
        )


@pytest.mark.parametrize(
    ("field_name", "invalid"),
    [
        ("base_url", "https://example.invalid"),
        ("maximum_body_bytes", True),
        ("poll_interval_ms", 5_000.0),
        ("promoting_families", ("A", "B")),
    ],
)
def test_plan_is_revalidated_before_it_can_authorize_evidence(
    field_name: str,
    invalid: object,
) -> None:
    plan = _plan()
    object.__setattr__(plan, field_name, invalid)

    with pytest.raises((TypeError, ValueError)):
        _success_observation(plan=plan)


def test_exact_32_symbol_plan_accepts_last_zero_based_ordinal_and_33_is_rejected() -> None:
    symbols = tuple(f"A{index:02d}USDT" for index in range(32))
    plan = _plan(symbols)
    last = _success_observation(
        plan=plan,
        symbol=symbols[-1],
        symbol_ordinal=31,
        body=b"{}",
    )

    last.validate_against_plan(plan)
    assert last.symbol_ordinal == 31

    with pytest.raises(ValueError, match="maximum of 32"):
        _plan((*symbols, "Z99USDT"))


@pytest.mark.parametrize("symbol", ["", "USDT", "btcusdt", "BTCUSD", "A" * 27 + "USDT"])
def test_logical_key_rejects_noncanonical_or_overbound_symbols(symbol: str) -> None:
    with pytest.raises(ValueError, match="symbol"):
        public_oi_rest_source_logical_key_v2(symbol)
