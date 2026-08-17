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
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.capture.plans import ProvisionalDepthRestQualificationPlanV8

PUBLIC_DEPTH_REST_BASE_URL_V8 = "https://fapi.binance.com"
PUBLIC_DEPTH_REST_ENDPOINT_V8 = "/fapi/v1/depth"
PUBLIC_DEPTH_REST_LIMIT_V8 = 1_000
PUBLIC_DEPTH_REST_REQUEST_WEIGHT_V8 = 20
PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8 = 1_048_576
PUBLIC_DEPTH_REST_MAXIMUM_LEVELS_PER_SIDE_V8 = 1_000
PUBLIC_DEPTH_REST_MAXIMUM_SYMBOL_CENSUS_V8 = 32

_METHOD = "GET"
_ROUTE_ID = "usdm_public_depth_rest"
_REQUEST_TIMEOUT_MS = 4_000
_MAXIMUM_CONCURRENCY = 4
_MAXIMUM_HTTP_ATTEMPTS = 1
_MAXIMUM_BRIDGE_ATTEMPTS = 3
_REQUEST_FIELDS = ("symbol", "limit")
_FIXED_QUERY = (("limit", "1000"),)
_AUTH_MODE = "NONE"
_REQUEST_HEADERS = (
    ("accept", "application/json"),
    ("accept-encoding", "identity"),
    ("user-agent", "binance-signalbot-r4b-v2-capture/1"),
)
_BUILT_REQUEST_HEADERS = tuple(
    sorted(
        (
            *_REQUEST_HEADERS,
            ("connection", "keep-alive"),
            ("host", "fapi.binance.com"),
        )
    )
)
_RESPONSE_HEADER_POLICY = "BINANCE_PUBLIC_MINIMAL_V1"
_RESPONSE_SCHEMA = "BINANCE_USDM_DEPTH_SNAPSHOT_RAW_ATTEMPT_V1"
_PERIODIC_CADENCE_POLICY = "UNSET_REQUIRES_INFRASTRUCTURE_QUALIFICATION_NO_PNL"
_PURPOSE = "LIQUIDITY_EXECUTION_QUALIFICATION_ONLY"
# The provisional V8 plan/API carries V9 attempt bytes after the pre-runtime
# lineage, first-buffered sequence, and built-request proof correction.
_PAYLOAD_SCHEMA_VERSION = "r4b_v2_public_depth_rest_attempt_v9"
_BODY_ENCODING = "base64"
_PLAN_HASH_DOMAIN = b"R4B_V2_PUBLIC_DEPTH_REST_PLAN_V8\0"
_ATTEMPT_HASH_DOMAIN = b"R4B_V2_PUBLIC_DEPTH_REST_ATTEMPT_V9\0"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_MAX_SIGNED_INT64 = (1 << 63) - 1
_MAX_IDENTITY_LENGTH = 256
_MAX_ERROR_DETAIL_LENGTH = 256
_MAX_RESPONSE_HEADERS = 16
_MAX_RESPONSE_HEADER_NAME_LENGTH = 128
_MAX_RESPONSE_HEADER_VALUE_LENGTH = 256
_MAX_BODY_BASE64_LENGTH = 4 * ((PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8 + 2) // 3)
_MAX_CANONICAL_PAYLOAD_BYTES = _MAX_BODY_BASE64_LENGTH + 64 * 1_024

type CanonicalPairsV8 = tuple[tuple[str, str], ...]
type PublicDepthSnapshotTriggerV8 = Literal["startup", "reconnect", "sequence_gap"]


class PublicDepthRestErrorCategoryV8(StrEnum):
    """Sanitized terminal categories for one public depth HTTP attempt."""

    HTTP_STATUS = "http_status"
    NETWORK = "network"
    TIMEOUT = "timeout"
    PROTOCOL = "protocol"
    RESPONSE_READ = "response_read"
    RESPONSE_CLOSE = "response_close"
    BODY_LIMIT = "body_limit"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class PublicDepthRestTerminalObservationV8:
    """One bounded terminal depth HTTP observation awaiting shared admission."""

    plan_sha256: str
    session_id: str
    protocol_hash: str
    connection_id: str
    method: str
    base_url: str
    endpoint: str
    symbol: str
    canonical_query: CanonicalPairsV8
    request_headers: CanonicalPairsV8
    trigger: PublicDepthSnapshotTriggerV8
    trigger_seq: int
    connection_generation: int
    first_buffered_u: int
    symbol_ordinal: int
    bridge_attempt: int
    http_attempt: int
    request_started_wall_ms: int
    request_started_monotonic_ns: int
    response_first_header_wall_ms: int | None
    response_first_header_monotonic_ns: int | None
    attempt_ended_wall_ms: int
    attempt_ended_monotonic_ns: int
    response_status: int | None
    response_headers: CanonicalPairsV8
    payload_complete: bool
    body: bytes
    admission_cancellation_requested: bool = False
    error_category: PublicDepthRestErrorCategoryV8 | None = None
    error_detail: str | None = None

    def __post_init__(self) -> None:
        _validate_attempt_common(
            plan_sha256=self.plan_sha256,
            session_id=self.session_id,
            protocol_hash=self.protocol_hash,
            connection_id=self.connection_id,
            method=self.method,
            base_url=self.base_url,
            endpoint=self.endpoint,
            symbol=self.symbol,
            canonical_query=self.canonical_query,
            request_headers=self.request_headers,
            trigger=self.trigger,
            trigger_seq=self.trigger_seq,
            connection_generation=self.connection_generation,
            first_buffered_u=self.first_buffered_u,
            symbol_ordinal=self.symbol_ordinal,
            bridge_attempt=self.bridge_attempt,
            http_attempt=self.http_attempt,
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
        plan: ProvisionalDepthRestQualificationPlanV8,
        *,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        method: str,
        base_url: str,
        endpoint: str,
        symbol: str,
        canonical_query: CanonicalPairsV8,
        request_headers: CanonicalPairsV8,
        trigger: PublicDepthSnapshotTriggerV8,
        trigger_seq: int,
        connection_generation: int,
        first_buffered_u: int,
        symbol_ordinal: int,
        bridge_attempt: int,
        request_started_wall_ms: int,
        request_started_monotonic_ns: int,
        response_first_header_wall_ms: int | None,
        response_first_header_monotonic_ns: int | None,
        attempt_ended_wall_ms: int,
        attempt_ended_monotonic_ns: int,
        response_status: int | None,
        response_headers: CanonicalPairsV8,
        payload_complete: bool,
        body: bytes,
        error_category: PublicDepthRestErrorCategoryV8 | None = None,
        error_detail: str | None = None,
    ) -> PublicDepthRestTerminalObservationV8:
        """Construct one exact symbol+limit=1000 attempt from the v8 plan."""

        validate_public_depth_rest_plan_v8(plan)
        _validate_symbol_against_plan(plan, symbol, symbol_ordinal)
        return cls(
            plan_sha256=public_depth_rest_plan_sha256_v8(plan),
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            method=method,
            base_url=base_url,
            endpoint=endpoint,
            symbol=symbol,
            canonical_query=canonical_query,
            request_headers=request_headers,
            trigger=trigger,
            trigger_seq=trigger_seq,
            connection_generation=connection_generation,
            first_buffered_u=first_buffered_u,
            symbol_ordinal=symbol_ordinal,
            bridge_attempt=bridge_attempt,
            http_attempt=1,
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
        plan: ProvisionalDepthRestQualificationPlanV8,
    ) -> None:
        validate_public_depth_rest_plan_v8(plan)
        _validate_symbol_against_plan(plan, self.symbol, self.symbol_ordinal)
        if self.plan_sha256 != public_depth_rest_plan_sha256_v8(plan):
            raise ValueError("depth REST observation plan hash differs")
        if self.trigger not in plan.snapshot_triggers:
            raise ValueError("depth REST observation trigger is outside the exact plan")

    def build_payload(
        self,
        completion_admission: ReceiptTimestamp,
    ) -> PublicDepthRestAttemptPayloadV8:
        completion_wall_ms, completion_monotonic_ns = _completion_clocks(
            completion_admission
        )
        body_sha256 = hashlib.sha256(self.body).hexdigest()
        return PublicDepthRestAttemptPayloadV8(
            plan_sha256=self.plan_sha256,
            session_id=self.session_id,
            protocol_hash=self.protocol_hash,
            connection_id=self.connection_id,
            method=self.method,
            base_url=self.base_url,
            endpoint=self.endpoint,
            symbol=self.symbol,
            canonical_query=self.canonical_query,
            request_headers=self.request_headers,
            trigger=self.trigger,
            trigger_seq=self.trigger_seq,
            connection_generation=self.connection_generation,
            first_buffered_u=self.first_buffered_u,
            symbol_ordinal=self.symbol_ordinal,
            bridge_attempt=self.bridge_attempt,
            http_attempt=self.http_attempt,
            request_started_wall_ms=self.request_started_wall_ms,
            request_started_monotonic_ns=self.request_started_monotonic_ns,
            response_first_header_wall_ms=self.response_first_header_wall_ms,
            response_first_header_monotonic_ns=self.response_first_header_monotonic_ns,
            attempt_ended_wall_ms=self.attempt_ended_wall_ms,
            attempt_ended_monotonic_ns=self.attempt_ended_monotonic_ns,
            completion_admission_wall_ms=completion_wall_ms,
            completion_admission_monotonic_ns=completion_monotonic_ns,
            response_status=self.response_status,
            response_headers=self.response_headers,
            payload_complete=self.payload_complete,
            body_encoding=_BODY_ENCODING,
            body_len=len(self.body),
            body_sha256=body_sha256,
            body_base64=base64.b64encode(self.body).decode("ascii"),
            admission_cancellation_requested=self.admission_cancellation_requested,
            error_category=self.error_category,
            error_detail=self.error_detail,
        )

    def with_admission_cancellation_v8(self) -> PublicDepthRestTerminalObservationV8:
        if self.admission_cancellation_requested:
            return self
        return replace(self, admission_cancellation_requested=True)

    def __call__(self, completion_admission: ReceiptTimestamp, /) -> bytes:
        return self.build_payload(completion_admission).canonical_bytes()


@dataclass(frozen=True, slots=True)
class PublicDepthRestAttemptPayloadV8:
    """Canonical raw evidence for one public depth snapshot HTTP attempt."""

    plan_sha256: str
    session_id: str
    protocol_hash: str
    connection_id: str
    method: str
    base_url: str
    endpoint: str
    symbol: str
    canonical_query: CanonicalPairsV8
    request_headers: CanonicalPairsV8
    trigger: PublicDepthSnapshotTriggerV8
    trigger_seq: int
    connection_generation: int
    first_buffered_u: int
    symbol_ordinal: int
    bridge_attempt: int
    http_attempt: int
    request_started_wall_ms: int
    request_started_monotonic_ns: int
    response_first_header_wall_ms: int | None
    response_first_header_monotonic_ns: int | None
    attempt_ended_wall_ms: int
    attempt_ended_monotonic_ns: int
    completion_admission_wall_ms: int
    completion_admission_monotonic_ns: int
    response_status: int | None
    response_headers: CanonicalPairsV8
    payload_complete: bool
    body_encoding: str
    body_len: int
    body_sha256: str
    body_base64: str
    error_category: PublicDepthRestErrorCategoryV8 | None = None
    error_detail: str | None = None
    admission_cancellation_requested: bool = False
    qualification_only: Literal[True] = True
    promoting: Literal[False] = False
    promotion_ready: Literal[False] = False
    wal_durability_verified: Literal[False] = False
    finality_fence_verified: Literal[False] = False
    m2_certified: Literal[False] = False
    book_bridge_certified: Literal[False] = False
    liquidity_signal_emitted: Literal[False] = False
    order_execution_enabled: Literal[False] = False
    schema_version: Literal["r4b_v2_public_depth_rest_attempt_v9"] = (
        "r4b_v2_public_depth_rest_attempt_v9"
    )

    def __post_init__(self) -> None:
        if self.schema_version != _PAYLOAD_SCHEMA_VERSION:
            raise ValueError("unsupported public depth REST attempt schema_version")
        body = _decode_retained_body(
            encoding=self.body_encoding,
            body_base64=self.body_base64,
            body_len=self.body_len,
            body_sha256=self.body_sha256,
        )
        _validate_attempt_common(
            plan_sha256=self.plan_sha256,
            session_id=self.session_id,
            protocol_hash=self.protocol_hash,
            connection_id=self.connection_id,
            method=self.method,
            base_url=self.base_url,
            endpoint=self.endpoint,
            symbol=self.symbol,
            canonical_query=self.canonical_query,
            request_headers=self.request_headers,
            trigger=self.trigger,
            trigger_seq=self.trigger_seq,
            connection_generation=self.connection_generation,
            first_buffered_u=self.first_buffered_u,
            symbol_ordinal=self.symbol_ordinal,
            bridge_attempt=self.bridge_attempt,
            http_attempt=self.http_attempt,
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
        _validate_completion_clocks(self)
        _validate_nonclaim_flags(self)

    def body_bytes(self) -> bytes:
        return _decode_retained_body(
            encoding=self.body_encoding,
            body_base64=self.body_base64,
            body_len=self.body_len,
            body_sha256=self.body_sha256,
        )

    def canonical_bytes(self) -> bytes:
        encoded = canonical_json_line(self)
        if len(encoded) > _MAX_CANONICAL_PAYLOAD_BYTES:
            raise ValueError("canonical public depth REST attempt exceeds its byte bound")
        return encoded

    def validate_against_plan(
        self,
        plan: ProvisionalDepthRestQualificationPlanV8,
    ) -> None:
        validate_public_depth_rest_plan_v8(plan)
        _validate_symbol_against_plan(plan, self.symbol, self.symbol_ordinal)
        if self.plan_sha256 != public_depth_rest_plan_sha256_v8(plan):
            raise ValueError("depth REST payload plan hash differs")
        if self.trigger not in plan.snapshot_triggers:
            raise ValueError("depth REST payload trigger is outside the exact plan")

    @classmethod
    def from_canonical_bytes(
        cls,
        encoded: bytes,
        *,
        plan: ProvisionalDepthRestQualificationPlanV8 | None = None,
    ) -> PublicDepthRestAttemptPayloadV8:
        document = _decode_canonical_document(encoded)
        if set(document) != {field.name for field in fields(cls)}:
            raise ValueError("canonical public depth REST attempt fields differ")
        error_value = _optional_str(document, "error_category")
        try:
            error_category = (
                None if error_value is None else PublicDepthRestErrorCategoryV8(error_value)
            )
        except ValueError as exc:
            raise ValueError("unsupported public depth REST error category") from exc
        trigger = cast(PublicDepthSnapshotTriggerV8, _required_str(document, "trigger"))
        schema_version = cast(
            Literal["r4b_v2_public_depth_rest_attempt_v9"],
            _required_str(document, "schema_version"),
        )
        payload = cls(
            plan_sha256=_required_str(document, "plan_sha256"),
            session_id=_required_str(document, "session_id"),
            protocol_hash=_required_str(document, "protocol_hash"),
            connection_id=_required_str(document, "connection_id"),
            method=_required_str(document, "method"),
            base_url=_required_str(document, "base_url"),
            endpoint=_required_str(document, "endpoint"),
            symbol=_required_str(document, "symbol"),
            canonical_query=_required_pairs(document, "canonical_query"),
            request_headers=_required_pairs(document, "request_headers"),
            trigger=trigger,
            trigger_seq=_required_int(document, "trigger_seq"),
            connection_generation=_required_int(document, "connection_generation"),
            first_buffered_u=_required_int(document, "first_buffered_u"),
            symbol_ordinal=_required_int(document, "symbol_ordinal"),
            bridge_attempt=_required_int(document, "bridge_attempt"),
            http_attempt=_required_int(document, "http_attempt"),
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
            body_encoding=_required_str(document, "body_encoding"),
            body_len=_required_int(document, "body_len"),
            body_sha256=_required_str(document, "body_sha256"),
            body_base64=_required_str(document, "body_base64"),
            error_category=error_category,
            error_detail=_optional_str(document, "error_detail"),
            admission_cancellation_requested=_required_bool(
                document, "admission_cancellation_requested"
            ),
            qualification_only=cast(Literal[True], _required_bool(document, "qualification_only")),
            promoting=cast(Literal[False], _required_bool(document, "promoting")),
            promotion_ready=cast(
                Literal[False], _required_bool(document, "promotion_ready")
            ),
            wal_durability_verified=cast(
                Literal[False], _required_bool(document, "wal_durability_verified")
            ),
            finality_fence_verified=cast(
                Literal[False], _required_bool(document, "finality_fence_verified")
            ),
            m2_certified=cast(Literal[False], _required_bool(document, "m2_certified")),
            book_bridge_certified=cast(
                Literal[False], _required_bool(document, "book_bridge_certified")
            ),
            liquidity_signal_emitted=cast(
                Literal[False], _required_bool(document, "liquidity_signal_emitted")
            ),
            order_execution_enabled=cast(
                Literal[False], _required_bool(document, "order_execution_enabled")
            ),
            schema_version=schema_version,
        )
        if payload.canonical_bytes() != encoded:
            raise ValueError("public depth REST attempt is not exact canonical encoding")
        if plan is not None:
            payload.validate_against_plan(plan)
        return payload


def public_depth_rest_plan_sha256_v8(
    plan: ProvisionalDepthRestQualificationPlanV8,
) -> str:
    """Hash one exact qualification-only depth REST plan independently."""

    validate_public_depth_rest_plan_v8(plan)
    document = asdict(plan)
    document["symbols"] = tuple(sorted(plan.symbols))
    return hashlib.sha256(
        _PLAN_HASH_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def public_depth_rest_attempt_payload_sha256_v8(
    payload: PublicDepthRestAttemptPayloadV8,
) -> str:
    """Return a domain-separated digest of one exact retained attempt."""

    if type(payload) is not PublicDepthRestAttemptPayloadV8:
        raise TypeError("attempt hash requires an exact public depth REST payload")
    return hashlib.sha256(
        _ATTEMPT_HASH_DOMAIN + payload.canonical_bytes()
    ).hexdigest()


def public_depth_rest_source_logical_key_v8(symbol: str) -> str:
    _validate_symbol(symbol)
    key = f"depthSnapshot:{symbol}"
    if len(key) > _MAX_IDENTITY_LENGTH:
        raise ValueError("public depth REST source key exceeds its identity bound")
    return key


def validate_public_depth_rest_plan_v8(
    plan: ProvisionalDepthRestQualificationPlanV8,
) -> None:
    """Revalidate exact v8 public/no-key/non-promoting acquisition authority."""

    if type(plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise TypeError("depth REST evidence requires the exact v8 qualification plan")
    plan.__post_init__()
    expected: Mapping[str, object] = {
        "venue": VenueV2.USDM_FUTURES,
        "route_id": _ROUTE_ID,
        "method": _METHOD,
        "base_url": PUBLIC_DEPTH_REST_BASE_URL_V8,
        "endpoint": PUBLIC_DEPTH_REST_ENDPOINT_V8,
        "request_fields": _REQUEST_FIELDS,
        "fixed_query": _FIXED_QUERY,
        "maximum_query_limit": PUBLIC_DEPTH_REST_LIMIT_V8,
        "request_weight": PUBLIC_DEPTH_REST_REQUEST_WEIGHT_V8,
        "request_timeout_ms": _REQUEST_TIMEOUT_MS,
        "maximum_body_bytes": PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8,
        "maximum_concurrency": _MAXIMUM_CONCURRENCY,
        "maximum_attempts": _MAXIMUM_HTTP_ATTEMPTS,
        "retryable_status_codes": (),
        "retryable_error_categories": (),
        "retry_backoff_ms": (),
        "retry_jitter_mode": "NONE",
        "maximum_retry_after_ms": 0,
        "request_headers": _REQUEST_HEADERS,
        "response_header_policy": _RESPONSE_HEADER_POLICY,
        "response_schema": _RESPONSE_SCHEMA,
        "maximum_symbol_census": PUBLIC_DEPTH_REST_MAXIMUM_SYMBOL_CENSUS_V8,
        "snapshot_triggers": ("startup", "reconnect", "sequence_gap"),
        "bridge_maximum_attempts": _MAXIMUM_BRIDGE_ATTEMPTS,
        "periodic_cadence_ms": None,
        "periodic_cadence_policy": _PERIODIC_CADENCE_POLICY,
        "cadence_selection_uses_pnl": False,
        "periodic_cadence_promoting": False,
        "auth_mode": _AUTH_MODE,
        "requires_api_key": False,
        "is_private": False,
        "order_execution_enabled": False,
        "purpose": _PURPOSE,
        "promotion_ready": False,
        "promoting": False,
    }
    for field_name, expected_value in expected.items():
        actual_value = getattr(plan, field_name)
        if type(actual_value) is not type(expected_value) or actual_value != expected_value:
            raise ValueError(f"depth REST v8 plan field {field_name} differs")
    symbols = plan.symbols
    if (
        type(symbols) is not tuple
        or not symbols
        or len(symbols) > PUBLIC_DEPTH_REST_MAXIMUM_SYMBOL_CENSUS_V8
        or tuple(sorted(symbols)) != symbols
        or len(set(symbols)) != len(symbols)
    ):
        raise ValueError("depth REST v8 plan symbols must be bounded unique lexicographic order")
    for symbol in symbols:
        _validate_symbol(symbol)


def _validate_attempt_common(
    *,
    plan_sha256: str,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    method: str,
    base_url: str,
    endpoint: str,
    symbol: str,
    canonical_query: CanonicalPairsV8,
    request_headers: CanonicalPairsV8,
    trigger: PublicDepthSnapshotTriggerV8,
    trigger_seq: int,
    connection_generation: int,
    first_buffered_u: int,
    symbol_ordinal: int,
    bridge_attempt: int,
    http_attempt: int,
    request_started_wall_ms: int,
    request_started_monotonic_ns: int,
    response_first_header_wall_ms: int | None,
    response_first_header_monotonic_ns: int | None,
    attempt_ended_wall_ms: int,
    attempt_ended_monotonic_ns: int,
    response_status: int | None,
    response_headers: CanonicalPairsV8,
    payload_complete: bool,
    body_len: int,
    admission_cancellation_requested: bool,
    error_category: PublicDepthRestErrorCategoryV8 | None,
    error_detail: str | None,
) -> None:
    _require_sha256(plan_sha256, "plan_sha256")
    _require_bounded_identity(session_id, "session_id")
    _require_sha256(protocol_hash, "protocol_hash")
    _require_bounded_identity(connection_id, "connection_id")
    if method != _METHOD or type(method) is not str:
        raise ValueError("public depth REST method must be exactly GET")
    if base_url != PUBLIC_DEPTH_REST_BASE_URL_V8 or type(base_url) is not str:
        raise ValueError("public depth REST base URL differs")
    if endpoint != PUBLIC_DEPTH_REST_ENDPOINT_V8 or type(endpoint) is not str:
        raise ValueError("public depth REST endpoint differs")
    _validate_symbol(symbol)
    expected_query = (("limit", "1000"), ("symbol", symbol))
    if canonical_query != expected_query or type(canonical_query) is not tuple:
        raise ValueError("public depth REST query must be exact limit=1000 plus symbol")
    if request_headers != _BUILT_REQUEST_HEADERS or type(request_headers) is not tuple:
        raise ValueError(
            "public depth REST built request headers differ from the exact keyless policy"
        )
    if trigger not in {"startup", "reconnect", "sequence_gap"}:
        raise ValueError("public depth REST trigger is unsupported")
    _require_positive_int(trigger_seq, "trigger_seq")
    _require_positive_int(connection_generation, "connection_generation")
    _require_nonnegative_int(first_buffered_u, "first_buffered_u")
    _require_nonnegative_int(symbol_ordinal, "symbol_ordinal")
    if symbol_ordinal >= PUBLIC_DEPTH_REST_MAXIMUM_SYMBOL_CENSUS_V8:
        raise ValueError("depth REST symbol ordinal exceeds its fixed census bound")
    _require_positive_int(bridge_attempt, "bridge_attempt")
    if bridge_attempt > _MAXIMUM_BRIDGE_ATTEMPTS:
        raise ValueError("depth REST bridge attempt exceeds its fixed bound")
    if type(http_attempt) is not int or http_attempt != 1:
        raise ValueError("depth REST HTTP attempt must be exactly one")
    _require_nonnegative_int(request_started_wall_ms, "request_started_wall_ms")
    _require_nonnegative_int(request_started_monotonic_ns, "request_started_monotonic_ns")
    _validate_attempt_clocks(
        request_started_wall_ms=request_started_wall_ms,
        request_started_monotonic_ns=request_started_monotonic_ns,
        first_wall_ms=response_first_header_wall_ms,
        first_monotonic_ns=response_first_header_monotonic_ns,
        attempt_ended_wall_ms=attempt_ended_wall_ms,
        attempt_ended_monotonic_ns=attempt_ended_monotonic_ns,
    )
    _validate_response(
        response_status=response_status,
        response_headers=response_headers,
        first_header_seen=response_first_header_wall_ms is not None,
    )
    if type(payload_complete) is not bool:
        raise TypeError("payload_complete must be an exact boolean")
    if type(admission_cancellation_requested) is not bool:
        raise TypeError("admission_cancellation_requested must be an exact boolean")
    _require_nonnegative_int(body_len, "body_len")
    if body_len > PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8:
        raise ValueError("retained public depth REST body exceeds its byte cap")
    _validate_error_contract(
        response_status=response_status,
        payload_complete=payload_complete,
        body_len=body_len,
        error_category=error_category,
        error_detail=error_detail,
    )


def _validate_attempt_clocks(
    *,
    request_started_wall_ms: int,
    request_started_monotonic_ns: int,
    first_wall_ms: int | None,
    first_monotonic_ns: int | None,
    attempt_ended_wall_ms: int,
    attempt_ended_monotonic_ns: int,
) -> None:
    if (first_wall_ms is None) != (first_monotonic_ns is None):
        raise ValueError("first-header wall and monotonic clocks must be paired")
    if first_wall_ms is not None and first_monotonic_ns is not None:
        _require_nonnegative_int(first_wall_ms, "response_first_header_wall_ms")
        _require_nonnegative_int(first_monotonic_ns, "response_first_header_monotonic_ns")
        if first_monotonic_ns < request_started_monotonic_ns:
            raise ValueError("depth REST first header precedes request start monotonically")
    _require_nonnegative_int(attempt_ended_wall_ms, "attempt_ended_wall_ms")
    _require_nonnegative_int(attempt_ended_monotonic_ns, "attempt_ended_monotonic_ns")
    if attempt_ended_monotonic_ns < request_started_monotonic_ns:
        raise ValueError("depth REST attempt end precedes request start monotonically")
    if first_monotonic_ns is not None and attempt_ended_monotonic_ns < first_monotonic_ns:
        raise ValueError("depth REST attempt end precedes first header monotonically")


def _validate_completion_clocks(payload: PublicDepthRestAttemptPayloadV8) -> None:
    _require_nonnegative_int(
        payload.completion_admission_wall_ms, "completion_admission_wall_ms"
    )
    _require_nonnegative_int(
        payload.completion_admission_monotonic_ns,
        "completion_admission_monotonic_ns",
    )
    if payload.completion_admission_monotonic_ns < payload.attempt_ended_monotonic_ns:
        raise ValueError("depth REST completion admission precedes attempt end monotonically")


def _validate_nonclaim_flags(payload: PublicDepthRestAttemptPayloadV8) -> None:
    if (
        payload.qualification_only is not True
        or payload.promoting is not False
        or payload.promotion_ready is not False
        or payload.wal_durability_verified is not False
        or payload.finality_fence_verified is not False
        or payload.m2_certified is not False
        or payload.book_bridge_certified is not False
        or payload.liquidity_signal_emitted is not False
        or payload.order_execution_enabled is not False
    ):
        raise ValueError("depth REST attempt contains a forbidden promotion or execution claim")


def _validate_response(
    *,
    response_status: int | None,
    response_headers: CanonicalPairsV8,
    first_header_seen: bool,
) -> None:
    if response_status is not None and (
        type(response_status) is not int or not 100 <= response_status <= 599
    ):
        raise ValueError("response_status must be an exact HTTP status")
    if first_header_seen != (response_status is not None):
        raise ValueError("response status and first-header clocks must appear together")
    if type(response_headers) is not tuple:
        raise TypeError("response_headers must be an exact tuple")
    if response_status is None and response_headers:
        raise ValueError("pre-header attempts cannot retain response headers")
    if len(response_headers) > _MAX_RESPONSE_HEADERS:
        raise ValueError("response_headers exceeds its bounded member count")
    if tuple(sorted(response_headers)) != response_headers:
        raise ValueError("response_headers must be canonically sorted")
    for item in response_headers:
        if type(item) is not tuple or len(item) != 2:
            raise TypeError("each response header must be an exact pair")
        name, value = item
        if (
            type(name) is not str
            or name != name.casefold()
            or len(name) > _MAX_RESPONSE_HEADER_NAME_LENGTH
            or not is_allowed_rest_response_header(name)
        ):
            raise ValueError("response header name is not normalized and allowlisted")
        if (
            type(value) is not str
            or value.strip() != value
            or len(value) > _MAX_RESPONSE_HEADER_VALUE_LENGTH
            or any(character in value for character in "\r\n\x00")
        ):
            raise ValueError("response header value is invalid or unbounded")


def _validate_error_contract(
    *,
    response_status: int | None,
    payload_complete: bool,
    body_len: int,
    error_category: PublicDepthRestErrorCategoryV8 | None,
    error_detail: str | None,
) -> None:
    if (error_category is None) != (error_detail is None):
        raise ValueError("depth REST error category and detail must be paired")
    if error_category is not None and type(error_category) is not PublicDepthRestErrorCategoryV8:
        raise TypeError("error_category must be an exact depth REST category")
    if error_detail is not None:
        _validate_safe_error_detail(error_detail)
    if error_category is None:
        if not payload_complete or response_status is None or not 200 <= response_status < 300:
            raise ValueError("error-free depth attempts require a complete 2xx response")
        return
    if response_status is None:
        if payload_complete or body_len != 0:
            raise ValueError("pre-header depth errors require an empty incomplete body")
        if error_category not in {
            PublicDepthRestErrorCategoryV8.NETWORK,
            PublicDepthRestErrorCategoryV8.TIMEOUT,
            PublicDepthRestErrorCategoryV8.PROTOCOL,
            PublicDepthRestErrorCategoryV8.CANCELLED,
        }:
            raise ValueError("pre-header depth error category is inconsistent")
        return
    if error_category is PublicDepthRestErrorCategoryV8.HTTP_STATUS:
        if not payload_complete or 200 <= response_status < 300:
            raise ValueError("HTTP_STATUS requires one complete non-2xx response")
        return
    if error_category is PublicDepthRestErrorCategoryV8.BODY_LIMIT:
        if payload_complete or body_len != PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8:
            raise ValueError("BODY_LIMIT requires the exact capped incomplete prefix")
        return
    if payload_complete:
        if error_category not in {
            PublicDepthRestErrorCategoryV8.CANCELLED,
            PublicDepthRestErrorCategoryV8.RESPONSE_CLOSE,
            PublicDepthRestErrorCategoryV8.TIMEOUT,
        }:
            raise ValueError("complete depth response has an inconsistent error category")
        return
    if error_category not in {
        PublicDepthRestErrorCategoryV8.TIMEOUT,
        PublicDepthRestErrorCategoryV8.RESPONSE_READ,
        PublicDepthRestErrorCategoryV8.CANCELLED,
    }:
        raise ValueError("incomplete depth response has an inconsistent error category")


def _validate_symbol_against_plan(
    plan: ProvisionalDepthRestQualificationPlanV8,
    symbol: str,
    symbol_ordinal: int,
) -> None:
    _require_nonnegative_int(symbol_ordinal, "symbol_ordinal")
    if symbol_ordinal >= len(plan.symbols) or plan.symbols[symbol_ordinal] != symbol:
        raise ValueError("depth REST symbol and zero-based plan ordinal differ")


def _validate_symbol(symbol: str) -> None:
    if (
        type(symbol) is not str
        or not 5 <= len(symbol) <= 30
        or _SYMBOL_RE.fullmatch(symbol) is None
    ):
        raise ValueError("depth REST symbol must be normalized uppercase USDT")


def _validate_safe_error_detail(detail: str) -> None:
    if (
        type(detail) is not str
        or not detail
        or detail.strip() != detail
        or len(detail) > _MAX_ERROR_DETAIL_LENGTH
        or any(character in detail for character in "\r\n\x00")
    ):
        raise ValueError("error_detail must be bounded normalized text")
    lowered = detail.casefold()
    if any(
        token in lowered
        for token in (
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
    ):
        raise ValueError("error_detail may not contain credentials or request URLs")


def _strict_body_length(body: bytes) -> int:
    if type(body) is not bytes:
        raise TypeError("public depth REST body must be immutable bytes")
    return len(body)


def _decode_retained_body(
    *,
    encoding: str,
    body_base64: str,
    body_len: int,
    body_sha256: str,
) -> bytes:
    if encoding != _BODY_ENCODING or type(encoding) is not str:
        raise ValueError("public depth REST body encoding must be base64")
    if type(body_base64) is not str or len(body_base64) > _MAX_BODY_BASE64_LENGTH:
        raise ValueError("body_base64 is not bounded exact text")
    _require_nonnegative_int(body_len, "body_len")
    if body_len > PUBLIC_DEPTH_REST_MAXIMUM_BODY_BYTES_V8:
        raise ValueError("retained public depth REST body exceeds its byte cap")
    _require_sha256(body_sha256, "body_sha256")
    try:
        body = base64.b64decode(body_base64, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("body_base64 is not valid base64") from exc
    if base64.b64encode(body).decode("ascii") != body_base64:
        raise ValueError("body_base64 is not canonical base64")
    if len(body) != body_len:
        raise ValueError("body_len differs from the retained depth body")
    if hashlib.sha256(body).hexdigest() != body_sha256:
        raise ValueError("body_sha256 differs from the retained depth body")
    return body


def _completion_clocks(completion: ReceiptTimestamp) -> tuple[int, int]:
    if type(completion) is not ReceiptTimestamp:
        raise TypeError("completion admission must be an exact ReceiptTimestamp")
    _require_nonnegative_int(completion.received_at_ms, "completion wall clock")
    _require_nonnegative_int(completion.received_monotonic_ns, "completion monotonic clock")
    return completion.received_at_ms, completion.received_monotonic_ns


def _decode_canonical_document(encoded: bytes) -> dict[str, object]:
    if type(encoded) is not bytes:
        raise TypeError("canonical depth REST attempt must be immutable bytes")
    if (
        not encoded
        or len(encoded) > _MAX_CANONICAL_PAYLOAD_BYTES
        or not encoded.endswith(b"\n")
        or encoded.count(b"\n") != 1
    ):
        raise ValueError("canonical depth REST attempt must be one bounded JSONL record")
    try:
        decoded = encoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("canonical depth REST attempt must be UTF-8") from exc

    def reject_float(value: str) -> object:
        raise ValueError(f"binary float is forbidden in depth REST JSON: {value}")

    def reject_constant(value: str) -> object:
        raise ValueError(f"non-finite value is forbidden in depth REST JSON: {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("canonical depth REST attempt contains a duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(
            decoded,
            parse_float=reject_float,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicate_keys,
        )
    except (json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("canonical depth REST attempt is invalid JSON") from exc
    if type(value) is not dict:
        raise ValueError("canonical depth REST attempt must be a JSON object")
    return cast(dict[str, object], value)


def _required_str(document: Mapping[str, object], key: str) -> str:
    value = document[key]
    if type(value) is not str:
        raise TypeError(f"{key} must be exact text")
    return value


def _optional_str(document: Mapping[str, object], key: str) -> str | None:
    value = document[key]
    if value is None:
        return None
    if type(value) is not str:
        raise TypeError(f"{key} must be exact text or null")
    return value


def _required_int(document: Mapping[str, object], key: str) -> int:
    value = document[key]
    if type(value) is not int:
        raise TypeError(f"{key} must be an exact integer")
    return value


def _optional_int(document: Mapping[str, object], key: str) -> int | None:
    value = document[key]
    if value is None:
        return None
    if type(value) is not int:
        raise TypeError(f"{key} must be an exact integer or null")
    return value


def _required_bool(document: Mapping[str, object], key: str) -> bool:
    value = document[key]
    if type(value) is not bool:
        raise TypeError(f"{key} must be an exact boolean")
    return value


def _required_pairs(document: Mapping[str, object], key: str) -> CanonicalPairsV8:
    value = document[key]
    if type(value) is not list:
        raise TypeError(f"{key} must be an exact JSON array")
    result: list[tuple[str, str]] = []
    for item in value:
        if type(item) is not list or len(item) != 2:
            raise TypeError(f"{key} members must be exact two-item arrays")
        name, member_value = item
        if type(name) is not str or type(member_value) is not str:
            raise TypeError(f"{key} names and values must be exact text")
        result.append((name, member_value))
    return tuple(result)


def _require_sha256(value: str, field_name: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")


def _require_bounded_identity(value: str, field_name: str) -> None:
    if (
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field_name} must be a bounded normalized identity")


def _require_positive_int(value: int, field_name: str) -> None:
    if type(value) is not int or not 1 <= value <= _MAX_SIGNED_INT64:
        raise ValueError(f"{field_name} must be a positive int64")


def _require_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or not 0 <= value <= _MAX_SIGNED_INT64:
        raise ValueError(f"{field_name} must be a nonnegative int64")
