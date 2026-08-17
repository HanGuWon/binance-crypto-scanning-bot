from __future__ import annotations

import base64
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Final, TypeAlias

from signalbot.domain.enums import Market

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PUBLIC_REST_PATHS_BY_MARKET: Final[Mapping[Market, tuple[str, ...]]] = MappingProxyType(
    {
        Market.SPOT: (
            "/api/v3/depth",
            "/api/v3/exchangeInfo",
            "/api/v3/klines",
            "/api/v3/ticker/24hr",
            "/api/v3/time",
        ),
        Market.FUTURES: (
            "/fapi/v1/depth",
            "/fapi/v1/exchangeInfo",
            "/fapi/v1/fundingInfo",
            "/fapi/v1/fundingRate",
            "/fapi/v1/klines",
            "/fapi/v1/openInterest",
            "/fapi/v1/premiumIndex",
            "/fapi/v1/ticker/24hr",
            "/fapi/v1/time",
            "/futures/data/openInterestHist",
        ),
    }
)


class RawPayloadEncoding(StrEnum):
    TEXT = "text"
    BASE64 = "base64"


class RestErrorCategory(StrEnum):
    HTTP_STATUS = "http_status"
    NETWORK = "network"
    TIMEOUT = "timeout"
    PROTOCOL = "protocol"
    RESPONSE_READ = "response_read"
    RESPONSE_CLOSE = "response_close"
    BODY_LIMIT = "body_limit"
    CANCELLED = "cancelled"


class ConnectionState(StrEnum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECYCLED = "recycled"


class CoverageState(StrEnum):
    HEALTHY = "healthy"
    INVALID = "invalid"


class CoverageReason(StrEnum):
    STARTED = "started"
    STOPPED = "stopped"
    QUEUE_OVERFLOW = "queue_overflow"
    STORAGE_CAPACITY = "storage_capacity"
    SHORT_WRITE = "short_write"
    WRITER_ERROR = "writer_error"
    HASH_INTEGRITY = "hash_integrity"
    DOWNSTREAM_ERROR = "downstream_error"
    SERIALIZATION_ERROR = "serialization_error"


def _require_identity(value: str, field: str) -> None:
    if not value or value.strip() != value:
        raise ValueError(f"{field} must be a non-empty normalized string")


def _require_plan_sha256(value: str) -> None:
    if _SHA256_RE.fullmatch(value) is None:
        raise ValueError("plan_sha256 must be a lowercase SHA-256 digest")


def _require_nonnegative(value: int, field: str) -> None:
    if value < 0:
        raise ValueError(f"{field} must be nonnegative")


def _require_positive(value: int, field: str) -> None:
    if value < 1:
        raise ValueError(f"{field} must be positive")


def validate_public_route(market: Market, route: str) -> None:
    allowed = {"spot"} if market is Market.SPOT else {"market", "public"}
    if route not in allowed:
        raise ValueError(f"route {route!r} is not a public {market.value} market-data route")


def validate_public_rest_path(market: Market, path: str) -> None:
    if path not in PUBLIC_REST_PATHS_BY_MARKET[market]:
        raise ValueError(f"REST path {path!r} is not in the public market-data allowlist")


def is_allowed_rest_response_header(name: str) -> bool:
    return name in {
        "retry-after",
        "date",
        "content-type",
        "content-encoding",
    } or name.startswith("x-mbx-used-weight")


def _validate_canonical_query(query: tuple[tuple[str, str], ...]) -> None:
    if tuple(sorted(query)) != query:
        raise ValueError("canonical_query must be sorted")
    sensitive = {"apikey", "api_key", "listenkey", "signature", "token", "secret"}
    for name, value in query:
        if not name or name.strip() != name or any(character in name for character in "\r\n"):
            raise ValueError("canonical_query contains an invalid parameter name")
        if any(character in value for character in "\r\n"):
            raise ValueError("canonical_query contains an invalid parameter value")
        if name.casefold() in sensitive:
            raise ValueError("canonical_query contains a forbidden credential parameter")


def _validate_rest_response_headers(headers: tuple[tuple[str, str], ...]) -> None:
    if tuple(sorted(headers)) != headers:
        raise ValueError("response_headers must be sorted")
    for name, value in headers:
        if name != name.casefold() or not is_allowed_rest_response_header(name):
            raise ValueError("response_headers contains a non-allowlisted header")
        if value.strip() != value or any(character in value for character in "\r\n"):
            raise ValueError("response_headers contains an invalid value")


def _validate_safe_error_detail(detail: str) -> None:
    if not detail or detail.strip() != detail or len(detail) > 256:
        raise ValueError("error_detail must be a short normalized description")
    lowered = detail.casefold()
    forbidden = (
        "authorization",
        "x-mbx-apikey",
        "api-key",
        "apikey",
        "signature=",
        "secret=",
        "token=",
        "http://",
        "https://",
    )
    if any(token in lowered for token in forbidden) or any(
        character in detail for character in "\r\n"
    ):
        raise ValueError("error_detail may not contain credentials or request URLs")


def _require_no_private_streams(streams: tuple[str, ...]) -> None:
    forbidden = ("listenkey", "userdata", "account", "@order")
    if any(token in stream.casefold() for stream in streams for token in forbidden):
        raise ValueError("private or authenticated streams are forbidden")


@dataclass(frozen=True, slots=True)
class CaptureEnvelopeV1:
    received_at_ms: int
    received_monotonic_ns: int
    plan_sha256: str
    process_boot_id: str
    connection_id: str
    frame_seq: int
    ingest_seq: int
    market: Market
    route: str
    stream: str
    subscription_streams: tuple[str, ...]
    raw_payload: str
    raw_payload_encoding: RawPayloadEncoding = RawPayloadEncoding.TEXT
    schema_version: str = field(default="capture_envelope_v1", init=False)
    source: str = field(default="binance", init=False)
    transport: str = field(default="websocket", init=False)

    def __post_init__(self) -> None:
        _require_nonnegative(self.received_at_ms, "received_at_ms")
        _require_nonnegative(self.received_monotonic_ns, "received_monotonic_ns")
        _require_plan_sha256(self.plan_sha256)
        _require_identity(self.process_boot_id, "process_boot_id")
        _require_identity(self.connection_id, "connection_id")
        _require_positive(self.frame_seq, "frame_seq")
        _require_positive(self.ingest_seq, "ingest_seq")
        validate_public_route(self.market, self.route)
        _require_identity(self.stream, "stream")
        if not self.subscription_streams or any(not stream for stream in self.subscription_streams):
            raise ValueError("subscription_streams must contain non-empty streams")
        _require_no_private_streams(self.subscription_streams)
        if self.source != "binance" or self.transport != "websocket":
            raise ValueError("capture envelopes are restricted to Binance WebSocket data")


@dataclass(frozen=True, slots=True)
class RestEnvelopeV1:
    request_started_at_ms: int
    request_started_monotonic_ns: int
    response_received_at_ms: int
    response_received_monotonic_ns: int
    plan_sha256: str
    process_boot_id: str
    request_id: str
    attempt: int
    ingest_seq: int
    market: Market
    endpoint_path: str
    canonical_query: tuple[tuple[str, str], ...]
    response_status: int | None
    raw_payload: str
    raw_payload_encoding: RawPayloadEncoding = RawPayloadEncoding.TEXT
    error: str | None = None
    schema_version: str = field(default="rest_envelope_v1", init=False)
    source: str = field(default="binance", init=False)
    transport: str = field(default="https", init=False)

    def __post_init__(self) -> None:
        _require_nonnegative(self.request_started_at_ms, "request_started_at_ms")
        _require_nonnegative(self.response_received_at_ms, "response_received_at_ms")
        _require_nonnegative(self.request_started_monotonic_ns, "request_started_monotonic_ns")
        if self.response_received_monotonic_ns < self.request_started_monotonic_ns:
            raise ValueError("REST monotonic response time precedes request time")
        _require_plan_sha256(self.plan_sha256)
        _require_identity(self.process_boot_id, "process_boot_id")
        _require_identity(self.request_id, "request_id")
        _require_positive(self.attempt, "attempt")
        _require_positive(self.ingest_seq, "ingest_seq")
        validate_public_rest_path(self.market, self.endpoint_path)
        if tuple(sorted(self.canonical_query)) != self.canonical_query:
            raise ValueError("canonical_query must be sorted")
        if self.response_status is not None and not 100 <= self.response_status <= 599:
            raise ValueError("response_status must be an HTTP status code")
        if self.source != "binance" or self.transport != "https":
            raise ValueError("REST envelopes are restricted to Binance HTTPS data")


@dataclass(frozen=True, slots=True)
class RestEnvelopeV2:
    request_started_at_ms: int
    request_started_monotonic_ns: int
    response_first_byte_at_ms: int | None
    response_first_byte_monotonic_ns: int | None
    response_completed_at_ms: int
    response_completed_monotonic_ns: int
    plan_sha256: str
    process_boot_id: str
    request_role: str
    correlation_id: str
    attempt: int
    ingest_seq: int
    market: Market
    endpoint_path: str
    canonical_query: tuple[tuple[str, str], ...]
    response_status: int | None
    response_headers: tuple[tuple[str, str], ...]
    payload_complete: bool
    raw_payload: str
    raw_payload_encoding: RawPayloadEncoding = RawPayloadEncoding.TEXT
    error_category: RestErrorCategory | None = None
    error_detail: str | None = None
    schema_version: str = field(default="rest_envelope_v2", init=False)
    source: str = field(default="binance", init=False)
    transport: str = field(default="https", init=False)

    def __post_init__(self) -> None:
        _require_nonnegative(self.request_started_at_ms, "request_started_at_ms")
        _require_nonnegative(self.request_started_monotonic_ns, "request_started_monotonic_ns")
        _require_nonnegative(self.response_completed_at_ms, "response_completed_at_ms")
        if self.response_completed_monotonic_ns < self.request_started_monotonic_ns:
            raise ValueError("REST monotonic completion time precedes request start")
        first_wall = self.response_first_byte_at_ms
        first_monotonic = self.response_first_byte_monotonic_ns
        if (first_wall is None) != (first_monotonic is None):
            raise ValueError("REST first-byte wall and monotonic timestamps must be paired")
        if first_wall is not None and first_monotonic is not None:
            _require_nonnegative(first_wall, "response_first_byte_at_ms")
            if first_monotonic < self.request_started_monotonic_ns:
                raise ValueError("REST monotonic first-byte time precedes request start")
            if self.response_completed_monotonic_ns < first_monotonic:
                raise ValueError("REST monotonic completion time precedes first byte")
        _require_plan_sha256(self.plan_sha256)
        _require_identity(self.process_boot_id, "process_boot_id")
        _require_identity(self.request_role, "request_role")
        _require_identity(self.correlation_id, "correlation_id")
        _require_positive(self.attempt, "attempt")
        _require_positive(self.ingest_seq, "ingest_seq")
        validate_public_rest_path(self.market, self.endpoint_path)
        _validate_canonical_query(self.canonical_query)
        if isinstance(self.response_status, bool) or (
            self.response_status is not None and not 100 <= self.response_status <= 599
        ):
            raise ValueError("response_status must be an HTTP status code")
        _validate_rest_response_headers(self.response_headers)
        if not isinstance(self.payload_complete, bool):
            raise ValueError("payload_complete must be boolean")
        if first_wall is None:
            if self.response_status is not None or self.response_headers:
                raise ValueError("pre-response errors cannot contain response metadata")
            if self.payload_complete:
                raise ValueError("pre-response errors cannot have a complete payload")
        elif self.response_status is None:
            raise ValueError("response_status is required once a response is received")
        if (self.error_category is None) != (self.error_detail is None):
            raise ValueError("REST error category and detail must be paired")
        if self.error_category is not None and not isinstance(
            self.error_category, RestErrorCategory
        ):
            raise ValueError("error_category must be a supported REST category")
        if self.error_detail is not None:
            _validate_safe_error_detail(self.error_detail)
        if not self.payload_complete and self.error_category is None:
            raise ValueError("an incomplete REST payload requires an error category")
        if self.error_category is RestErrorCategory.HTTP_STATUS:
            if (
                self.response_status is None
                or 200 <= self.response_status < 300
                or not self.payload_complete
            ):
                raise ValueError("HTTP status errors require a complete non-2xx response")
        if (
            self.response_status is not None
            and not 200 <= self.response_status < 300
            and self.payload_complete
            and self.error_category is None
        ):
            raise ValueError("complete non-2xx responses require an error category")
        if self.source != "binance" or self.transport != "https":
            raise ValueError("REST envelopes are restricted to Binance HTTPS data")


@dataclass(frozen=True, slots=True)
class ConnectionTransitionV1:
    received_at_ms: int
    received_monotonic_ns: int
    plan_sha256: str
    process_boot_id: str
    connection_id: str
    ingest_seq: int
    last_frame_seq: int
    market: Market
    route: str
    streams: tuple[str, ...]
    state: ConnectionState
    reason: str
    close_code: int | None = None
    schema_version: str = field(default="connection_transition_v1", init=False)
    source: str = field(default="binance", init=False)

    def __post_init__(self) -> None:
        _require_nonnegative(self.received_at_ms, "received_at_ms")
        _require_nonnegative(self.received_monotonic_ns, "received_monotonic_ns")
        _require_plan_sha256(self.plan_sha256)
        _require_identity(self.process_boot_id, "process_boot_id")
        _require_identity(self.connection_id, "connection_id")
        _require_positive(self.ingest_seq, "ingest_seq")
        _require_nonnegative(self.last_frame_seq, "last_frame_seq")
        validate_public_route(self.market, self.route)
        if not self.streams or any(not stream for stream in self.streams):
            raise ValueError("streams must contain at least one non-empty stream")
        _require_no_private_streams(self.streams)
        _require_identity(self.reason, "reason")
        if self.source != "binance":
            raise ValueError("connection transitions are restricted to Binance")


@dataclass(frozen=True, slots=True)
class CoverageTransitionV1:
    received_at_ms: int
    received_monotonic_ns: int
    plan_sha256: str
    process_boot_id: str
    connection_id: str
    frame_seq: int
    ingest_seq: int
    market: Market
    route: str
    stream: str
    state: CoverageState
    reason: CoverageReason
    detail: str
    schema_version: str = field(default="coverage_transition_v1", init=False)
    source: str = field(default="binance", init=False)

    def __post_init__(self) -> None:
        _require_nonnegative(self.received_at_ms, "received_at_ms")
        _require_nonnegative(self.received_monotonic_ns, "received_monotonic_ns")
        _require_plan_sha256(self.plan_sha256)
        _require_identity(self.process_boot_id, "process_boot_id")
        _require_identity(self.connection_id, "connection_id")
        _require_nonnegative(self.frame_seq, "frame_seq")
        _require_positive(self.ingest_seq, "ingest_seq")
        if self.route != "rest":
            validate_public_route(self.market, self.route)
        _require_identity(self.stream, "stream")
        _require_identity(self.detail, "detail")
        if self.source != "binance":
            raise ValueError("coverage transitions are restricted to Binance")


CaptureRecord: TypeAlias = (  # noqa: UP040 - plain compileall gate may use host Python <3.12
    CaptureEnvelopeV1
    | RestEnvelopeV1
    | RestEnvelopeV2
    | ConnectionTransitionV1
    | CoverageTransitionV1
)


def payload_text(raw: str | bytes) -> tuple[str, RawPayloadEncoding]:
    """Represent WebSocket text or binary frames without losing bytes."""

    if isinstance(raw, str):
        return raw, RawPayloadEncoding.TEXT
    return base64.b64encode(raw).decode("ascii"), RawPayloadEncoding.BASE64


def payload_bytes(payload: str, encoding: RawPayloadEncoding) -> bytes:
    if encoding is RawPayloadEncoding.TEXT:
        return payload.encode("utf-8")
    return base64.b64decode(payload, validate=True)


def record_to_json_line(record: CaptureRecord) -> bytes:
    """Return one deterministic UTF-8 JSONL record."""

    return (
        json.dumps(
            asdict(record),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def invalidation_for_record(
    record: CaptureRecord,
    reason: CoverageReason,
    detail: str,
) -> CoverageTransitionV1:
    """Bind a fail-closed coverage transition to the affected source record."""

    if isinstance(record, CaptureEnvelopeV1):
        connection_id = record.connection_id
        frame_seq = record.frame_seq
        route = record.route
        stream = record.stream
        received_at_ms = record.received_at_ms
        received_monotonic_ns = record.received_monotonic_ns
    elif isinstance(record, RestEnvelopeV1):
        connection_id = f"rest:{record.request_id}"
        frame_seq = 0
        route = "rest"
        stream = record.endpoint_path
        received_at_ms = record.response_received_at_ms
        received_monotonic_ns = record.response_received_monotonic_ns
    elif isinstance(record, RestEnvelopeV2):
        connection_id = f"rest:{record.correlation_id}:{record.attempt}"
        frame_seq = 0
        route = "rest"
        stream = record.endpoint_path
        received_at_ms = record.response_completed_at_ms
        received_monotonic_ns = record.response_completed_monotonic_ns
    elif isinstance(record, ConnectionTransitionV1):
        connection_id = record.connection_id
        frame_seq = record.last_frame_seq
        route = record.route
        stream = record.streams[0]
        received_at_ms = record.received_at_ms
        received_monotonic_ns = record.received_monotonic_ns
    else:
        connection_id = record.connection_id
        frame_seq = record.frame_seq
        route = record.route
        stream = record.stream
        received_at_ms = record.received_at_ms
        received_monotonic_ns = record.received_monotonic_ns
    return CoverageTransitionV1(
        received_at_ms=received_at_ms,
        received_monotonic_ns=received_monotonic_ns,
        plan_sha256=record.plan_sha256,
        process_boot_id=record.process_boot_id,
        connection_id=connection_id,
        frame_seq=frame_seq,
        ingest_seq=record.ingest_seq,
        market=record.market,
        route=route,
        stream=stream,
        state=CoverageState.INVALID,
        reason=reason,
        detail=detail,
    )
