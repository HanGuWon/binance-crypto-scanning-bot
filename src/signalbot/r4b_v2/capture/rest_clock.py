from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields, replace
from enum import StrEnum
from typing import Literal, cast

from signalbot.capture.models import is_allowed_rest_response_header
from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.plans import (
    ProvisionalUsdmVenueClockRestCapturePlanV9,
)

PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9 = "venueTime:usdm"
PUBLIC_USDM_VENUE_CLOCK_MAXIMUM_BODY_BYTES_V9 = 4_096

_PAYLOAD_SCHEMA = "r4b_v2_public_usdm_venue_clock_rest_attempt_v1"
_PLAN_HASH_DOMAIN = b"R4B_V2_PUBLIC_USDM_VENUE_CLOCK_REST_PLAN_V9\0"
_MAX_IDENTITY_LENGTH = 256
_MAX_ERROR_DETAIL_LENGTH = 256
_MAX_RESPONSE_HEADERS = 16
_MAX_HEADER_NAME_LENGTH = 128
_MAX_HEADER_VALUE_LENGTH = 256
_MAX_CANONICAL_INTEGER = (1 << 53) - 1
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

type CanonicalClockHeaderPairsV9 = tuple[tuple[str, str], ...]


class PublicUsdmVenueClockRestErrorCategoryV9(StrEnum):
    """Sanitized terminal categories for one public venue-time attempt."""

    HTTP_STATUS = "http_status"
    NETWORK = "network"
    TIMEOUT = "timeout"
    PROTOCOL = "protocol"
    RESPONSE_READ = "response_read"
    RESPONSE_CLOSE = "response_close"
    BODY_LIMIT = "body_limit"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class PublicUsdmVenueClockRestTerminalObservationV9:
    """One bounded terminal public venue-time observation awaiting admission."""

    plan_sha256: str
    session_id: str
    protocol_hash: str
    connection_id: str
    connection_generation: int
    poll_cycle_seq: int
    scheduled_slot_wall_ms: int
    http_attempt: int
    method: str
    base_url: str
    endpoint: str
    canonical_query: tuple[tuple[str, str], ...]
    request_headers: CanonicalClockHeaderPairsV9
    request_started_wall_ms: int
    request_started_monotonic_ns: int
    response_first_header_wall_ms: int | None
    response_first_header_monotonic_ns: int | None
    attempt_ended_wall_ms: int
    attempt_ended_monotonic_ns: int
    response_status: int | None
    response_headers: CanonicalClockHeaderPairsV9
    payload_complete: bool
    body: bytes
    admission_cancellation_requested: bool = False
    error_category: PublicUsdmVenueClockRestErrorCategoryV9 | None = None
    error_detail: str | None = None

    def __post_init__(self) -> None:
        _validate_attempt_material(
            plan_sha256=self.plan_sha256,
            session_id=self.session_id,
            protocol_hash=self.protocol_hash,
            connection_id=self.connection_id,
            connection_generation=self.connection_generation,
            poll_cycle_seq=self.poll_cycle_seq,
            scheduled_slot_wall_ms=self.scheduled_slot_wall_ms,
            http_attempt=self.http_attempt,
            method=self.method,
            base_url=self.base_url,
            endpoint=self.endpoint,
            canonical_query=self.canonical_query,
            request_headers=self.request_headers,
            request_started_wall_ms=self.request_started_wall_ms,
            request_started_monotonic_ns=self.request_started_monotonic_ns,
            response_first_header_wall_ms=self.response_first_header_wall_ms,
            response_first_header_monotonic_ns=self.response_first_header_monotonic_ns,
            attempt_ended_wall_ms=self.attempt_ended_wall_ms,
            attempt_ended_monotonic_ns=self.attempt_ended_monotonic_ns,
            response_status=self.response_status,
            response_headers=self.response_headers,
            payload_complete=self.payload_complete,
            body_len=_strict_body_length(self.body),
            admission_cancellation_requested=self.admission_cancellation_requested,
            error_category=self.error_category,
            error_detail=self.error_detail,
        )

    @classmethod
    def for_plan(
        cls,
        plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
        *,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        connection_generation: int,
        poll_cycle_seq: int,
        scheduled_slot_wall_ms: int,
        request_started_wall_ms: int,
        request_started_monotonic_ns: int,
        response_first_header_wall_ms: int | None,
        response_first_header_monotonic_ns: int | None,
        attempt_ended_wall_ms: int,
        attempt_ended_monotonic_ns: int,
        response_status: int | None,
        response_headers: CanonicalClockHeaderPairsV9,
        payload_complete: bool,
        body: bytes,
        error_category: PublicUsdmVenueClockRestErrorCategoryV9 | None = None,
        error_detail: str | None = None,
    ) -> PublicUsdmVenueClockRestTerminalObservationV9:
        plan.__post_init__()
        return cls(
            plan_sha256=public_usdm_venue_clock_rest_plan_sha256_v9(plan),
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            connection_generation=connection_generation,
            poll_cycle_seq=poll_cycle_seq,
            scheduled_slot_wall_ms=scheduled_slot_wall_ms,
            http_attempt=1,
            method=plan.method,
            base_url=plan.base_url,
            endpoint=plan.endpoint,
            canonical_query=(),
            request_headers=plan.request_headers,
            request_started_wall_ms=request_started_wall_ms,
            request_started_monotonic_ns=request_started_monotonic_ns,
            response_first_header_wall_ms=response_first_header_wall_ms,
            response_first_header_monotonic_ns=response_first_header_monotonic_ns,
            attempt_ended_wall_ms=attempt_ended_wall_ms,
            attempt_ended_monotonic_ns=attempt_ended_monotonic_ns,
            response_status=response_status,
            response_headers=response_headers,
            payload_complete=payload_complete,
            body=body,
            error_category=error_category,
            error_detail=error_detail,
        )

    def validate_against_plan(
        self,
        plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    ) -> None:
        plan.__post_init__()
        if self.plan_sha256 != public_usdm_venue_clock_rest_plan_sha256_v9(plan):
            raise ValueError("venue-clock REST observation plan hash differs")
        if (
            self.method != plan.method
            or self.base_url != plan.base_url
            or self.endpoint != plan.endpoint
            or self.canonical_query != plan.fixed_query
            or self.request_headers != plan.request_headers
        ):
            raise ValueError("venue-clock REST observation request differs from its plan")
        if self.scheduled_slot_wall_ms % plan.poll_interval_ms != 0:
            raise ValueError("venue-clock REST scheduled slot is not epoch aligned")

    def build_payload(
        self,
        completion_admission: ReceiptTimestamp,
    ) -> PublicUsdmVenueClockRestAttemptPayloadV9:
        _validate_receipt(completion_admission)
        return PublicUsdmVenueClockRestAttemptPayloadV9(
            plan_sha256=self.plan_sha256,
            session_id=self.session_id,
            protocol_hash=self.protocol_hash,
            connection_id=self.connection_id,
            connection_generation=self.connection_generation,
            poll_cycle_seq=self.poll_cycle_seq,
            scheduled_slot_wall_ms=self.scheduled_slot_wall_ms,
            http_attempt=self.http_attempt,
            method=self.method,
            base_url=self.base_url,
            endpoint=self.endpoint,
            canonical_query=self.canonical_query,
            request_headers=self.request_headers,
            request_started_wall_ms=self.request_started_wall_ms,
            request_started_monotonic_ns=self.request_started_monotonic_ns,
            response_first_header_wall_ms=self.response_first_header_wall_ms,
            response_first_header_monotonic_ns=self.response_first_header_monotonic_ns,
            attempt_ended_wall_ms=self.attempt_ended_wall_ms,
            attempt_ended_monotonic_ns=self.attempt_ended_monotonic_ns,
            completion_admission_wall_ms=completion_admission.received_at_ms,
            completion_admission_monotonic_ns=(
                completion_admission.received_monotonic_ns
            ),
            response_status=self.response_status,
            response_headers=self.response_headers,
            payload_complete=self.payload_complete,
            body_len=len(self.body),
            body_sha256=hashlib.sha256(self.body).hexdigest(),
            body_base64=base64.b64encode(self.body).decode("ascii"),
            admission_cancellation_requested=self.admission_cancellation_requested,
            error_category=self.error_category,
            error_detail=self.error_detail,
        )

    def with_admission_cancellation_v9(
        self,
    ) -> PublicUsdmVenueClockRestTerminalObservationV9:
        if self.admission_cancellation_requested:
            return self
        return replace(self, admission_cancellation_requested=True)

    def __call__(self, completion_admission: ReceiptTimestamp, /) -> bytes:
        return self.build_payload(completion_admission).canonical_bytes()


@dataclass(frozen=True, slots=True)
class PublicUsdmVenueClockRestAttemptPayloadV9:
    """Canonical raw evidence for one public USD-M venue-time HTTP attempt."""

    plan_sha256: str
    session_id: str
    protocol_hash: str
    connection_id: str
    connection_generation: int
    poll_cycle_seq: int
    scheduled_slot_wall_ms: int
    http_attempt: int
    method: str
    base_url: str
    endpoint: str
    canonical_query: tuple[tuple[str, str], ...]
    request_headers: CanonicalClockHeaderPairsV9
    request_started_wall_ms: int
    request_started_monotonic_ns: int
    response_first_header_wall_ms: int | None
    response_first_header_monotonic_ns: int | None
    attempt_ended_wall_ms: int
    attempt_ended_monotonic_ns: int
    completion_admission_wall_ms: int
    completion_admission_monotonic_ns: int
    response_status: int | None
    response_headers: CanonicalClockHeaderPairsV9
    payload_complete: bool
    body_len: int
    body_sha256: str
    body_base64: str
    admission_cancellation_requested: bool = False
    error_category: PublicUsdmVenueClockRestErrorCategoryV9 | None = None
    error_detail: str | None = None
    infrastructure_clock_only: Literal[True] = True
    promoting: Literal[False] = False
    causal_cursor_complete: Literal[False] = False
    order_execution_enabled: Literal[False] = False
    schema_version: Literal["r4b_v2_public_usdm_venue_clock_rest_attempt_v1"] = (
        _PAYLOAD_SCHEMA
    )

    def __post_init__(self) -> None:
        body = self.body_bytes()
        _validate_attempt_material(
            plan_sha256=self.plan_sha256,
            session_id=self.session_id,
            protocol_hash=self.protocol_hash,
            connection_id=self.connection_id,
            connection_generation=self.connection_generation,
            poll_cycle_seq=self.poll_cycle_seq,
            scheduled_slot_wall_ms=self.scheduled_slot_wall_ms,
            http_attempt=self.http_attempt,
            method=self.method,
            base_url=self.base_url,
            endpoint=self.endpoint,
            canonical_query=self.canonical_query,
            request_headers=self.request_headers,
            request_started_wall_ms=self.request_started_wall_ms,
            request_started_monotonic_ns=self.request_started_monotonic_ns,
            response_first_header_wall_ms=self.response_first_header_wall_ms,
            response_first_header_monotonic_ns=self.response_first_header_monotonic_ns,
            attempt_ended_wall_ms=self.attempt_ended_wall_ms,
            attempt_ended_monotonic_ns=self.attempt_ended_monotonic_ns,
            response_status=self.response_status,
            response_headers=self.response_headers,
            payload_complete=self.payload_complete,
            body_len=len(body),
            admission_cancellation_requested=self.admission_cancellation_requested,
            error_category=self.error_category,
            error_detail=self.error_detail,
        )
        _require_nonnegative_canonical_int(
            self.completion_admission_wall_ms,
            "completion_admission_wall_ms",
        )
        _require_nonnegative_canonical_int(
            self.completion_admission_monotonic_ns,
            "completion_admission_monotonic_ns",
        )
        if (
            self.completion_admission_monotonic_ns < self.attempt_ended_monotonic_ns
        ):
            raise ValueError("venue-clock completion admission precedes attempt end")
        if self.schema_version != _PAYLOAD_SCHEMA:
            raise ValueError("unsupported venue-clock REST attempt schema")
        if (
            self.infrastructure_clock_only is not True
            or self.promoting is not False
            or self.causal_cursor_complete is not False
            or self.order_execution_enabled is not False
        ):
            raise ValueError("venue-clock attempt overclaims authority")

    def body_bytes(self) -> bytes:
        if type(self.body_base64) is not str:
            raise TypeError("venue-clock retained body must be base64 text")
        try:
            body = base64.b64decode(self.body_base64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("venue-clock retained body is not strict base64") from exc
        if base64.b64encode(body).decode("ascii") != self.body_base64:
            raise ValueError("venue-clock retained body is not canonical base64")
        if len(body) != self.body_len:
            raise ValueError("venue-clock retained body length differs")
        _require_sha256(self.body_sha256, "body_sha256")
        if hashlib.sha256(body).hexdigest() != self.body_sha256:
            raise ValueError("venue-clock retained body hash differs")
        return body

    def canonical_bytes(self) -> bytes:
        return canonical_json_line(self)

    def validate_against_plan(
        self,
        plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    ) -> None:
        plan.__post_init__()
        if self.plan_sha256 != public_usdm_venue_clock_rest_plan_sha256_v9(plan):
            raise ValueError("venue-clock REST payload plan hash differs")
        if (
            self.method != plan.method
            or self.base_url != plan.base_url
            or self.endpoint != plan.endpoint
            or self.canonical_query != plan.fixed_query
            or self.request_headers != plan.request_headers
        ):
            raise ValueError("venue-clock REST payload request differs from its plan")
        if self.scheduled_slot_wall_ms % plan.poll_interval_ms != 0:
            raise ValueError("venue-clock REST payload slot is not epoch aligned")

    @classmethod
    def from_canonical_bytes(
        cls,
        encoded: bytes,
        *,
        plan: ProvisionalUsdmVenueClockRestCapturePlanV9 | None = None,
    ) -> PublicUsdmVenueClockRestAttemptPayloadV9:
        document = _decode_canonical_document(encoded)
        if set(document) != {item.name for item in fields(cls)}:
            raise ValueError("canonical venue-clock REST attempt fields differ")
        error_text = _optional_str(document, "error_category")
        try:
            error_category = (
                None
                if error_text is None
                else PublicUsdmVenueClockRestErrorCategoryV9(error_text)
            )
        except ValueError as exc:
            raise ValueError("unsupported venue-clock REST error category") from exc
        payload = cls(
            plan_sha256=_required_str(document, "plan_sha256"),
            session_id=_required_str(document, "session_id"),
            protocol_hash=_required_str(document, "protocol_hash"),
            connection_id=_required_str(document, "connection_id"),
            connection_generation=_required_int(document, "connection_generation"),
            poll_cycle_seq=_required_int(document, "poll_cycle_seq"),
            scheduled_slot_wall_ms=_required_int(document, "scheduled_slot_wall_ms"),
            http_attempt=_required_int(document, "http_attempt"),
            method=_required_str(document, "method"),
            base_url=_required_str(document, "base_url"),
            endpoint=_required_str(document, "endpoint"),
            canonical_query=_required_pairs(document, "canonical_query"),
            request_headers=_required_pairs(document, "request_headers"),
            request_started_wall_ms=_required_int(document, "request_started_wall_ms"),
            request_started_monotonic_ns=_required_int(
                document, "request_started_monotonic_ns"
            ),
            response_first_header_wall_ms=_optional_int(
                document, "response_first_header_wall_ms"
            ),
            response_first_header_monotonic_ns=_optional_int(
                document, "response_first_header_monotonic_ns"
            ),
            attempt_ended_wall_ms=_required_int(document, "attempt_ended_wall_ms"),
            attempt_ended_monotonic_ns=_required_int(
                document, "attempt_ended_monotonic_ns"
            ),
            completion_admission_wall_ms=_required_int(
                document, "completion_admission_wall_ms"
            ),
            completion_admission_monotonic_ns=_required_int(
                document, "completion_admission_monotonic_ns"
            ),
            response_status=_optional_int(document, "response_status"),
            response_headers=_required_pairs(document, "response_headers"),
            payload_complete=_required_bool(document, "payload_complete"),
            body_len=_required_int(document, "body_len"),
            body_sha256=_required_str(document, "body_sha256"),
            body_base64=_required_str(document, "body_base64"),
            admission_cancellation_requested=_required_bool(
                document, "admission_cancellation_requested"
            ),
            error_category=error_category,
            error_detail=_optional_str(document, "error_detail"),
            infrastructure_clock_only=cast(
                Literal[True], _required_bool(document, "infrastructure_clock_only")
            ),
            promoting=cast(Literal[False], _required_bool(document, "promoting")),
            causal_cursor_complete=cast(
                Literal[False], _required_bool(document, "causal_cursor_complete")
            ),
            order_execution_enabled=cast(
                Literal[False], _required_bool(document, "order_execution_enabled")
            ),
            schema_version=cast(
                Literal["r4b_v2_public_usdm_venue_clock_rest_attempt_v1"],
                _required_str(document, "schema_version"),
            ),
        )
        if payload.canonical_bytes() != encoded:
            raise ValueError("venue-clock REST attempt canonical replay differs")
        if plan is not None:
            payload.validate_against_plan(plan)
        return payload


def public_usdm_venue_clock_rest_plan_sha256_v9(
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
) -> str:
    """Hash the exact public clock request role independently of its bundle."""

    if type(plan) is not ProvisionalUsdmVenueClockRestCapturePlanV9:
        raise TypeError("venue-clock REST plan hash requires the exact v9 plan")
    plan.__post_init__()
    return hashlib.sha256(
        _PLAN_HASH_DOMAIN + canonical_json_line(asdict(plan))
    ).hexdigest()


def _validate_attempt_material(
    *,
    plan_sha256: str,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    connection_generation: int,
    poll_cycle_seq: int,
    scheduled_slot_wall_ms: int,
    http_attempt: int,
    method: str,
    base_url: str,
    endpoint: str,
    canonical_query: tuple[tuple[str, str], ...],
    request_headers: CanonicalClockHeaderPairsV9,
    request_started_wall_ms: int,
    request_started_monotonic_ns: int,
    response_first_header_wall_ms: int | None,
    response_first_header_monotonic_ns: int | None,
    attempt_ended_wall_ms: int,
    attempt_ended_monotonic_ns: int,
    response_status: int | None,
    response_headers: CanonicalClockHeaderPairsV9,
    payload_complete: bool,
    body_len: int,
    admission_cancellation_requested: bool,
    error_category: PublicUsdmVenueClockRestErrorCategoryV9 | None,
    error_detail: str | None,
) -> None:
    _require_sha256(plan_sha256, "plan_sha256")
    _require_sha256(protocol_hash, "protocol_hash")
    _require_identity(session_id, "session_id")
    _require_identity(connection_id, "connection_id")
    for value, field_name in (
        (connection_generation, "connection_generation"),
        (poll_cycle_seq, "poll_cycle_seq"),
        (http_attempt, "http_attempt"),
    ):
        _require_positive_canonical_int(value, field_name)
    if http_attempt != 1:
        raise ValueError("venue-clock REST requires the frozen single attempt")
    for value, field_name in (
        (scheduled_slot_wall_ms, "scheduled_slot_wall_ms"),
        (request_started_wall_ms, "request_started_wall_ms"),
        (request_started_monotonic_ns, "request_started_monotonic_ns"),
        (attempt_ended_wall_ms, "attempt_ended_wall_ms"),
        (attempt_ended_monotonic_ns, "attempt_ended_monotonic_ns"),
        (body_len, "body_len"),
    ):
        _require_nonnegative_canonical_int(value, field_name)
    if method != "GET" or base_url != "https://fapi.binance.com":
        raise ValueError("venue-clock REST host or method differs")
    if endpoint != "/fapi/v1/time" or canonical_query != ():
        raise ValueError("venue-clock REST endpoint must be exact and queryless")
    _validate_request_headers(request_headers)
    _validate_response_headers(response_headers)
    first_pair = (response_first_header_wall_ms, response_first_header_monotonic_ns)
    if (first_pair[0] is None) != (first_pair[1] is None):
        raise ValueError("venue-clock first-header clocks must be both present or absent")
    if first_pair[0] is not None and first_pair[1] is not None:
        _require_nonnegative_canonical_int(first_pair[0], "response_first_header_wall_ms")
        _require_nonnegative_canonical_int(
            first_pair[1], "response_first_header_monotonic_ns"
        )
        if not (
            request_started_monotonic_ns
            <= first_pair[1]
            <= attempt_ended_monotonic_ns
        ):
            raise ValueError("venue-clock monotonic attempt clocks are reversed")
    elif attempt_ended_monotonic_ns < request_started_monotonic_ns:
        raise ValueError("venue-clock monotonic attempt clocks are reversed")
    if type(payload_complete) is not bool or type(admission_cancellation_requested) is not bool:
        raise TypeError("venue-clock terminal flags must be exact booleans")
    if response_status is not None:
        if type(response_status) is not int or not 100 <= response_status <= 599:
            raise ValueError("venue-clock HTTP status is invalid")
    if body_len > PUBLIC_USDM_VENUE_CLOCK_MAXIMUM_BODY_BYTES_V9:
        raise ValueError("venue-clock response body exceeds its frozen cap")
    success = (
        response_status == 200
        and payload_complete
        and error_category is None
        and not admission_cancellation_requested
    )
    if success:
        if first_pair[0] is None or body_len == 0 or error_detail is not None:
            raise ValueError("successful venue-clock attempt has incomplete terminal material")
        return
    if error_category is None or error_detail is None:
        raise ValueError("failed venue-clock attempt requires one sanitized error")
    if (
        type(error_detail) is not str
        or not error_detail
        or len(error_detail) > _MAX_ERROR_DETAIL_LENGTH
    ):
        raise ValueError("venue-clock error detail must be bounded nonempty text")
    if any(character in error_detail for character in "\r\n\x00"):
        raise ValueError("venue-clock error detail contains a forbidden character")


def _validate_request_headers(headers: CanonicalClockHeaderPairsV9) -> None:
    expected = (
        ("accept", "application/json"),
        ("accept-encoding", "identity"),
        ("user-agent", "binance-signalbot-r4b-v2-capture/1"),
    )
    if headers != expected:
        raise ValueError("venue-clock request headers differ from the frozen public policy")


def _validate_response_headers(headers: CanonicalClockHeaderPairsV9) -> None:
    if type(headers) is not tuple or len(headers) > _MAX_RESPONSE_HEADERS:
        raise ValueError("venue-clock response headers exceed their bounded tuple")
    if tuple(sorted(headers)) != headers or len(set(headers)) != len(headers):
        raise ValueError("venue-clock response headers must be canonical and unique")
    for item in headers:
        if type(item) is not tuple or len(item) != 2:
            raise TypeError("venue-clock response header must be an exact pair")
        name, value = item
        if (
            type(name) is not str
            or type(value) is not str
            or name != name.casefold()
            or not is_allowed_rest_response_header(name)
            or not name
            or len(name) > _MAX_HEADER_NAME_LENGTH
            or len(value) > _MAX_HEADER_VALUE_LENGTH
            or value.strip() != value
            or any(character in name or character in value for character in "\r\n\x00")
        ):
            raise ValueError("venue-clock response header is not normalized and allowlisted")


def _strict_body_length(body: bytes) -> int:
    if type(body) is not bytes:
        raise TypeError("venue-clock response body must be exact bytes")
    return len(body)


def _validate_receipt(receipt: ReceiptTimestamp) -> None:
    if type(receipt) is not ReceiptTimestamp:
        raise TypeError("venue-clock completion requires an exact ReceiptTimestamp")
    _require_nonnegative_canonical_int(receipt.received_at_ms, "completion wall receipt")
    _require_nonnegative_canonical_int(
        receipt.received_monotonic_ns, "completion monotonic receipt"
    )


def _decode_canonical_document(encoded: bytes) -> dict[str, object]:
    if type(encoded) is not bytes:
        raise TypeError("venue-clock canonical attempt must be exact bytes")
    try:
        parsed = json.loads(encoded, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("venue-clock canonical attempt is invalid JSON") from exc
    if type(parsed) is not dict or canonical_json_line(parsed) != encoded:
        raise ValueError("venue-clock attempt is not exact canonical JSONL")
    return cast(dict[str, object], parsed)


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON keys are forbidden")
        result[key] = value
    return result


def _required_str(document: Mapping[str, object], key: str) -> str:
    value = document.get(key)
    if type(value) is not str:
        raise TypeError(f"{key} must be exact text")
    return value


def _optional_str(document: Mapping[str, object], key: str) -> str | None:
    value = document.get(key)
    if value is not None and type(value) is not str:
        raise TypeError(f"{key} must be exact text or null")
    return cast(str | None, value)


def _required_int(document: Mapping[str, object], key: str) -> int:
    value = document.get(key)
    if type(value) is not int:
        raise TypeError(f"{key} must be an exact integer")
    return value


def _optional_int(document: Mapping[str, object], key: str) -> int | None:
    value = document.get(key)
    if value is not None and type(value) is not int:
        raise TypeError(f"{key} must be an exact integer or null")
    return cast(int | None, value)


def _required_bool(document: Mapping[str, object], key: str) -> bool:
    value = document.get(key)
    if type(value) is not bool:
        raise TypeError(f"{key} must be an exact boolean")
    return value


def _required_pairs(
    document: Mapping[str, object],
    key: str,
) -> tuple[tuple[str, str], ...]:
    value = document.get(key)
    if type(value) is not list:
        raise TypeError(f"{key} must be a JSON array")
    result: list[tuple[str, str]] = []
    for item in value:
        if (
            type(item) is not list
            or len(item) != 2
            or type(item[0]) is not str
            or type(item[1]) is not str
        ):
            raise TypeError(f"{key} must contain exact text pairs")
        result.append((item[0], item[1]))
    return tuple(result)


def _require_identity(value: str, field_name: str) -> None:
    if (
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field_name} must be a bounded normalized identity")


def _require_sha256(value: str, field_name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")


def _require_positive_canonical_int(value: int, field_name: str) -> None:
    if type(value) is not int or not 1 <= value <= _MAX_CANONICAL_INTEGER:
        raise ValueError(f"{field_name} must be a positive canonical integer")


def _require_nonnegative_canonical_int(value: int, field_name: str) -> None:
    if type(value) is not int or not 0 <= value <= _MAX_CANONICAL_INTEGER:
        raise ValueError(f"{field_name} must be a nonnegative canonical integer")
