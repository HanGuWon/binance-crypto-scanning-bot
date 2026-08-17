from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, fields, replace
from enum import StrEnum
from typing import Literal, Protocol, cast

from signalbot.capture.models import is_allowed_rest_response_header
from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2

PUBLIC_OI_REST_BASE_URL_V2 = "https://fapi.binance.com"
PUBLIC_OI_REST_ENDPOINT_V2 = "/fapi/v1/openInterest"
PUBLIC_OI_REST_POLL_INTERVAL_MS_V2 = 5_000
PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2 = 4_096
PUBLIC_OI_REST_MAXIMUM_ATTEMPTS_V2 = 1
PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2 = 32

_PUBLIC_OI_REST_METHOD = "GET"
_PUBLIC_OI_REST_ROUTE_ID = "usdm_public_rest"
_PUBLIC_OI_REST_SLOT_ALIGNMENT = "UTC_EPOCH_MULTIPLE"
_PUBLIC_OI_REST_REQUEST_TIMEOUT_MS = 4_000
_PUBLIC_OI_REST_MAXIMUM_CONCURRENCY = 4
_PUBLIC_OI_REST_RETRYABLE_STATUS_CODES: tuple[int, ...] = ()
_PUBLIC_OI_REST_RETRYABLE_ERROR_CATEGORIES: tuple[str, ...] = ()
_PUBLIC_OI_REST_RETRY_BACKOFF_MS: tuple[int, ...] = ()
_PUBLIC_OI_REST_RETRY_JITTER_MODE = "NONE"
_PUBLIC_OI_REST_MAXIMUM_RETRY_AFTER_MS = 0
_PUBLIC_OI_REST_REQUEST_HEADERS = (
    ("accept", "application/json"),
    ("accept-encoding", "identity"),
    ("user-agent", "binance-signalbot-r4b-v2-capture/1"),
)
_PUBLIC_OI_REST_RESPONSE_HEADER_POLICY = "BINANCE_PUBLIC_MINIMAL_V1"
_PUBLIC_OI_REST_RESPONSE_SCHEMA = "BINANCE_USDM_OPEN_INTEREST_RAW_ATTEMPT_V2"
_PUBLIC_OI_REST_SYMBOL_ORDER = "LEXICOGRAPHIC_ASC"
_PUBLIC_OI_REST_MISSED_SLOT_POLICY = "SKIP_NO_BACKFILL"
_PUBLIC_OI_REST_EXHAUSTED_ATTEMPT_POLICY = "RETAIN_AND_CONTINUE_M2_INCOMPLETE"
_PUBLIC_OI_REST_REQUEST_FIELDS = ("symbol",)
_PUBLIC_OI_REST_AUTH_MODE = "NONE"

_PAYLOAD_SCHEMA_VERSION = "r4b_v2_public_oi_rest_attempt_v2"
_BODY_ENCODING = "base64"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_MAX_IDENTITY_LENGTH = 256
_MAX_ERROR_DETAIL_LENGTH = 256
_MAX_RESPONSE_HEADERS = 16
_MAX_RESPONSE_HEADER_NAME_LENGTH = 128
_MAX_RESPONSE_HEADER_VALUE_LENGTH = 256
_MAX_CANONICAL_PAYLOAD_BYTES = 32 * 1_024
_MAX_BODY_BASE64_LENGTH = 4 * ((PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2 + 2) // 3)

CanonicalPairs = tuple[tuple[str, str], ...]


class PublicOiRestErrorCategoryV2(StrEnum):
    """Sanitized terminal categories for one public OI HTTP attempt."""

    HTTP_STATUS = "http_status"
    NETWORK = "network"
    TIMEOUT = "timeout"
    PROTOCOL = "protocol"
    RESPONSE_READ = "response_read"
    RESPONSE_CLOSE = "response_close"
    BODY_LIMIT = "body_limit"
    CANCELLED = "cancelled"


class PublicOiRestMissedSlotV2(RuntimeError):
    """Expected evidence that an OI request could not start in its assigned slot."""

    def __init__(
        self,
        *,
        symbol: str,
        poll_cycle_seq: int,
        symbol_ordinal: int,
        scheduled_slot_wall_ms: int,
        observed_request_start_wall_ms: int,
    ) -> None:
        _validate_symbol(symbol)
        _require_positive_int(poll_cycle_seq, "poll_cycle_seq")
        _require_nonnegative_int(symbol_ordinal, "symbol_ordinal")
        if symbol_ordinal >= PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2:
            raise ValueError("symbol_ordinal exceeds the frozen symbol-census bound")
        _validate_scheduled_slot(scheduled_slot_wall_ms)
        _require_nonnegative_int(
            observed_request_start_wall_ms,
            "observed_request_start_wall_ms",
        )
        if _wall_ms_is_in_slot(
            observed_request_start_wall_ms,
            scheduled_slot_wall_ms,
        ):
            raise ValueError("missed-slot observation must lie outside its assigned slot")
        self.symbol = symbol
        self.poll_cycle_seq = poll_cycle_seq
        self.symbol_ordinal = symbol_ordinal
        self.scheduled_slot_wall_ms = scheduled_slot_wall_ms
        self.observed_request_start_wall_ms = observed_request_start_wall_ms
        super().__init__(
            "public OI request missed its assigned UTC slot: "
            f"{symbol}/cycle-{poll_cycle_seq}/ordinal-{symbol_ordinal}/"
            f"slot-{scheduled_slot_wall_ms}/observed-{observed_request_start_wall_ms}"
        )


class PublicOiRestPayloadBuilderV2(Protocol):
    """Synchronous payload finalizer called while the shared ingress gate is held."""

    def __call__(self, completion_admission: ReceiptTimestamp, /) -> bytes: ...


@dataclass(frozen=True, slots=True)
class PublicOiRestTerminalObservationV2:
    """One bounded terminal HTTP observation awaiting its admission receipt.

    This object deliberately has no ingest sequence, process identity, or V1
    envelope lineage. ``SharedWebSocketIngressV2`` will eventually own the sole
    global sequence, sample ``completion_admission``, call this object
    synchronously, and retain the returned bytes inside one ``RawRecordV2``.
    """

    method: str
    base_url: str
    endpoint: str
    symbol: str
    canonical_query: CanonicalPairs
    poll_cycle_seq: int
    symbol_ordinal: int
    scheduled_slot_wall_ms: int
    attempt: int
    request_started_wall_ms: int
    request_started_monotonic_ns: int
    response_first_header_wall_ms: int | None
    response_first_header_monotonic_ns: int | None
    attempt_ended_wall_ms: int
    attempt_ended_monotonic_ns: int
    response_status: int | None
    response_headers: CanonicalPairs
    payload_complete: bool
    body: bytes
    admission_cancellation_requested: bool = False
    error_category: PublicOiRestErrorCategoryV2 | None = None
    error_detail: str | None = None

    def __post_init__(self) -> None:
        _validate_attempt_common(
            method=self.method,
            base_url=self.base_url,
            endpoint=self.endpoint,
            symbol=self.symbol,
            canonical_query=self.canonical_query,
            poll_cycle_seq=self.poll_cycle_seq,
            symbol_ordinal=self.symbol_ordinal,
            scheduled_slot_wall_ms=self.scheduled_slot_wall_ms,
            attempt=self.attempt,
            request_started_wall_ms=self.request_started_wall_ms,
            request_started_monotonic_ns=self.request_started_monotonic_ns,
            response_first_header_wall_ms=self.response_first_header_wall_ms,
            response_first_header_monotonic_ns=(
                self.response_first_header_monotonic_ns
            ),
            attempt_ended_wall_ms=self.attempt_ended_wall_ms,
            attempt_ended_monotonic_ns=self.attempt_ended_monotonic_ns,
            response_status=self.response_status,
            response_headers=self.response_headers,
            payload_complete=self.payload_complete,
            body_len=_strict_body_length(self.body),
            admission_cancellation_requested=(
                self.admission_cancellation_requested
            ),
            error_category=self.error_category,
            error_detail=self.error_detail,
        )

    @classmethod
    def for_plan(
        cls,
        plan: ProvisionalPromotingRestCapturePlanV2,
        *,
        symbol: str,
        poll_cycle_seq: int,
        symbol_ordinal: int,
        scheduled_slot_wall_ms: int,
        attempt: int,
        request_started_wall_ms: int,
        request_started_monotonic_ns: int,
        response_first_header_wall_ms: int | None,
        response_first_header_monotonic_ns: int | None,
        attempt_ended_wall_ms: int,
        attempt_ended_monotonic_ns: int,
        response_status: int | None,
        response_headers: CanonicalPairs,
        payload_complete: bool,
        body: bytes,
        error_category: PublicOiRestErrorCategoryV2 | None = None,
        error_detail: str | None = None,
    ) -> PublicOiRestTerminalObservationV2:
        """Build only when symbol identity and every frozen plan field agree."""

        _validate_plan_contract(plan)
        _validate_symbol_against_plan(plan, symbol, symbol_ordinal)
        return cls(
            method=plan.method,
            base_url=plan.base_url,
            endpoint=plan.endpoint,
            symbol=symbol,
            canonical_query=(("symbol", symbol),),
            poll_cycle_seq=poll_cycle_seq,
            symbol_ordinal=symbol_ordinal,
            scheduled_slot_wall_ms=scheduled_slot_wall_ms,
            attempt=attempt,
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
        plan: ProvisionalPromotingRestCapturePlanV2,
    ) -> None:
        _validate_plan_contract(plan)
        _validate_symbol_against_plan(plan, self.symbol, self.symbol_ordinal)

    def build_payload(
        self,
        completion_admission: ReceiptTimestamp,
    ) -> PublicOiRestAttemptPayloadV2:
        """Attach the ingress-owned completion clock without awaiting."""

        completion_wall_ms, completion_monotonic_ns = _completion_clocks(
            completion_admission
        )
        body_sha256 = hashlib.sha256(self.body).hexdigest()
        return PublicOiRestAttemptPayloadV2(
            method=self.method,
            base_url=self.base_url,
            endpoint=self.endpoint,
            symbol=self.symbol,
            canonical_query=self.canonical_query,
            poll_cycle_seq=self.poll_cycle_seq,
            symbol_ordinal=self.symbol_ordinal,
            scheduled_slot_wall_ms=self.scheduled_slot_wall_ms,
            attempt=self.attempt,
            request_started_wall_ms=self.request_started_wall_ms,
            request_started_monotonic_ns=self.request_started_monotonic_ns,
            response_first_header_wall_ms=self.response_first_header_wall_ms,
            response_first_header_monotonic_ns=(
                self.response_first_header_monotonic_ns
            ),
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
            admission_cancellation_requested=(
                self.admission_cancellation_requested
            ),
            error_category=self.error_category,
            error_detail=self.error_detail,
        )

    def with_admission_cancellation_v2(self) -> PublicOiRestTerminalObservationV2:
        """Return the exact admission-wait cancellation observation once."""

        if self.admission_cancellation_requested:
            return self
        return replace(
            self,
            admission_cancellation_requested=True,
        )

    def __call__(self, completion_admission: ReceiptTimestamp, /) -> bytes:
        return self.build_payload(completion_admission).canonical_bytes()


@dataclass(frozen=True, slots=True)
class PublicOiRestAttemptPayloadV2:
    """Canonical, self-verifying raw evidence for one public OI REST attempt."""

    method: str
    base_url: str
    endpoint: str
    symbol: str
    canonical_query: CanonicalPairs
    poll_cycle_seq: int
    symbol_ordinal: int
    scheduled_slot_wall_ms: int
    attempt: int
    request_started_wall_ms: int
    request_started_monotonic_ns: int
    response_first_header_wall_ms: int | None
    response_first_header_monotonic_ns: int | None
    attempt_ended_wall_ms: int
    attempt_ended_monotonic_ns: int
    completion_admission_wall_ms: int
    completion_admission_monotonic_ns: int
    response_status: int | None
    response_headers: CanonicalPairs
    payload_complete: bool
    body_encoding: str
    body_len: int
    body_sha256: str
    body_base64: str
    error_category: PublicOiRestErrorCategoryV2 | None = None
    error_detail: str | None = None
    admission_cancellation_requested: bool = False
    schema_version: Literal["r4b_v2_public_oi_rest_attempt_v2"] = (
        "r4b_v2_public_oi_rest_attempt_v2"
    )

    def __post_init__(self) -> None:
        if type(self.schema_version) is not str or self.schema_version != _PAYLOAD_SCHEMA_VERSION:
            raise ValueError("unsupported public OI REST attempt schema_version")
        body = _decode_retained_body(
            encoding=self.body_encoding,
            body_base64=self.body_base64,
            body_len=self.body_len,
            body_sha256=self.body_sha256,
        )
        _validate_attempt_common(
            method=self.method,
            base_url=self.base_url,
            endpoint=self.endpoint,
            symbol=self.symbol,
            canonical_query=self.canonical_query,
            poll_cycle_seq=self.poll_cycle_seq,
            symbol_ordinal=self.symbol_ordinal,
            scheduled_slot_wall_ms=self.scheduled_slot_wall_ms,
            attempt=self.attempt,
            request_started_wall_ms=self.request_started_wall_ms,
            request_started_monotonic_ns=self.request_started_monotonic_ns,
            response_first_header_wall_ms=self.response_first_header_wall_ms,
            response_first_header_monotonic_ns=(
                self.response_first_header_monotonic_ns
            ),
            attempt_ended_wall_ms=self.attempt_ended_wall_ms,
            attempt_ended_monotonic_ns=self.attempt_ended_monotonic_ns,
            response_status=self.response_status,
            response_headers=self.response_headers,
            payload_complete=self.payload_complete,
            body_len=len(body),
            admission_cancellation_requested=(
                self.admission_cancellation_requested
            ),
            error_category=self.error_category,
            error_detail=self.error_detail,
        )
        _require_nonnegative_int(
            self.completion_admission_wall_ms,
            "completion_admission_wall_ms",
        )
        _require_nonnegative_int(
            self.completion_admission_monotonic_ns,
            "completion_admission_monotonic_ns",
        )
        if self.completion_admission_monotonic_ns < self.request_started_monotonic_ns:
            raise ValueError("completion admission precedes request start monotonically")
        first_monotonic = self.response_first_header_monotonic_ns
        if (
            first_monotonic is not None
            and self.completion_admission_monotonic_ns < first_monotonic
        ):
            raise ValueError("completion admission precedes first header monotonically")
        if self.completion_admission_monotonic_ns < self.attempt_ended_monotonic_ns:
            raise ValueError("completion admission precedes attempt end monotonically")

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
            raise ValueError("canonical public OI REST attempt exceeds its byte bound")
        return encoded

    @classmethod
    def from_canonical_bytes(
        cls,
        encoded: bytes,
        *,
        plan: ProvisionalPromotingRestCapturePlanV2 | None = None,
    ) -> PublicOiRestAttemptPayloadV2:
        """Parse one exact canonical line and reject drift or byte-level tampering."""

        document = _decode_canonical_document(encoded)
        expected_keys = {field.name for field in fields(cls)}
        if set(document) != expected_keys:
            raise ValueError("canonical public OI REST attempt fields differ")
        error_value = _optional_str(document, "error_category")
        try:
            error_category = (
                None
                if error_value is None
                else PublicOiRestErrorCategoryV2(error_value)
            )
        except ValueError as exc:
            raise ValueError("unsupported public OI REST error category") from exc
        schema_version = _required_str(document, "schema_version")
        payload = cls(
            method=_required_str(document, "method"),
            base_url=_required_str(document, "base_url"),
            endpoint=_required_str(document, "endpoint"),
            symbol=_required_str(document, "symbol"),
            canonical_query=_required_pairs(document, "canonical_query"),
            poll_cycle_seq=_required_int(document, "poll_cycle_seq"),
            symbol_ordinal=_required_int(document, "symbol_ordinal"),
            scheduled_slot_wall_ms=_required_int(document, "scheduled_slot_wall_ms"),
            attempt=_required_int(document, "attempt"),
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
                document,
                "admission_cancellation_requested",
            ),
            schema_version=cast(
                Literal["r4b_v2_public_oi_rest_attempt_v2"], schema_version
            ),
        )
        if payload.canonical_bytes() != encoded:
            raise ValueError("public OI REST attempt is not the exact canonical encoding")
        if plan is not None:
            payload.validate_against_plan(plan)
        return payload

    def validate_against_plan(
        self,
        plan: ProvisionalPromotingRestCapturePlanV2,
    ) -> None:
        _validate_plan_contract(plan)
        _validate_symbol_against_plan(plan, self.symbol, self.symbol_ordinal)


def public_oi_rest_source_logical_key_v2(symbol: str) -> str:
    """Return one stable bounded health key per OI symbol, never per attempt."""

    _validate_symbol(symbol)
    key = f"openInterest:{symbol}"
    if len(key) > _MAX_IDENTITY_LENGTH:
        raise ValueError("public OI REST source logical key exceeds its identity bound")
    return key


def _validate_attempt_common(
    *,
    method: str,
    base_url: str,
    endpoint: str,
    symbol: str,
    canonical_query: CanonicalPairs,
    poll_cycle_seq: int,
    symbol_ordinal: int,
    scheduled_slot_wall_ms: int,
    attempt: int,
    request_started_wall_ms: int,
    request_started_monotonic_ns: int,
    response_first_header_wall_ms: int | None,
    response_first_header_monotonic_ns: int | None,
    attempt_ended_wall_ms: int,
    attempt_ended_monotonic_ns: int,
    response_status: int | None,
    response_headers: CanonicalPairs,
    payload_complete: bool,
    body_len: int,
    admission_cancellation_requested: bool,
    error_category: PublicOiRestErrorCategoryV2 | None,
    error_detail: str | None,
) -> None:
    if type(method) is not str or method != _PUBLIC_OI_REST_METHOD:
        raise ValueError("public OI REST attempt method must be exactly GET")
    if type(base_url) is not str or base_url != PUBLIC_OI_REST_BASE_URL_V2:
        raise ValueError("public OI REST attempt base URL differs from the exact plan")
    if type(endpoint) is not str or endpoint != PUBLIC_OI_REST_ENDPOINT_V2:
        raise ValueError("public OI REST attempt endpoint differs from the exact plan")
    _validate_symbol(symbol)
    if (
        type(canonical_query) is not tuple
        or len(canonical_query) != 1
        or type(canonical_query[0]) is not tuple
        or len(canonical_query[0]) != 2
        or any(type(value) is not str for value in canonical_query[0])
        or canonical_query != (("symbol", symbol),)
    ):
        raise ValueError("public OI REST canonical query must be exactly its symbol")
    _require_positive_int(poll_cycle_seq, "poll_cycle_seq")
    _require_nonnegative_int(symbol_ordinal, "symbol_ordinal")
    if symbol_ordinal >= PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2:
        raise ValueError("symbol_ordinal exceeds the frozen symbol-census bound")
    _validate_scheduled_slot(scheduled_slot_wall_ms)
    _require_positive_int(attempt, "attempt")
    if attempt > PUBLIC_OI_REST_MAXIMUM_ATTEMPTS_V2:
        raise ValueError("attempt exceeds the frozen public OI attempt bound")
    _require_nonnegative_int(request_started_wall_ms, "request_started_wall_ms")
    if not _wall_ms_is_in_slot(request_started_wall_ms, scheduled_slot_wall_ms):
        raise ValueError("request start must lie inside its assigned UTC slot")
    _require_nonnegative_int(
        request_started_monotonic_ns,
        "request_started_monotonic_ns",
    )
    _validate_first_header_clocks(
        request_started_wall_ms=request_started_wall_ms,
        request_started_monotonic_ns=request_started_monotonic_ns,
        first_wall_ms=response_first_header_wall_ms,
        first_monotonic_ns=response_first_header_monotonic_ns,
    )
    _validate_attempt_end_clocks(
        request_started_wall_ms=request_started_wall_ms,
        request_started_monotonic_ns=request_started_monotonic_ns,
        first_wall_ms=response_first_header_wall_ms,
        first_monotonic_ns=response_first_header_monotonic_ns,
        attempt_ended_wall_ms=attempt_ended_wall_ms,
        attempt_ended_monotonic_ns=attempt_ended_monotonic_ns,
    )
    _validate_response_status_and_headers(
        response_status=response_status,
        response_headers=response_headers,
        first_header_seen=response_first_header_wall_ms is not None,
    )
    if type(payload_complete) is not bool:
        raise TypeError("payload_complete must be an exact boolean")
    if type(admission_cancellation_requested) is not bool:
        raise TypeError("admission_cancellation_requested must be an exact boolean")
    _require_nonnegative_int(body_len, "body_len")
    if body_len > PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2:
        raise ValueError("retained public OI REST body exceeds its byte cap")
    _validate_error_contract(
        response_status=response_status,
        payload_complete=payload_complete,
        body_len=body_len,
        error_category=error_category,
        error_detail=error_detail,
    )


def _validate_first_header_clocks(
    *,
    request_started_wall_ms: int,
    request_started_monotonic_ns: int,
    first_wall_ms: int | None,
    first_monotonic_ns: int | None,
) -> None:
    if (first_wall_ms is None) != (first_monotonic_ns is None):
        raise ValueError("first-header wall and monotonic clocks must be paired")
    if first_wall_ms is None or first_monotonic_ns is None:
        return
    _require_nonnegative_int(first_wall_ms, "response_first_header_wall_ms")
    _require_nonnegative_int(
        first_monotonic_ns,
        "response_first_header_monotonic_ns",
    )
    if first_monotonic_ns < request_started_monotonic_ns:
        raise ValueError("first header precedes request start monotonically")


def _validate_attempt_end_clocks(
    *,
    request_started_wall_ms: int,
    request_started_monotonic_ns: int,
    first_wall_ms: int | None,
    first_monotonic_ns: int | None,
    attempt_ended_wall_ms: int,
    attempt_ended_monotonic_ns: int,
) -> None:
    _require_nonnegative_int(attempt_ended_wall_ms, "attempt_ended_wall_ms")
    _require_nonnegative_int(
        attempt_ended_monotonic_ns,
        "attempt_ended_monotonic_ns",
    )
    if attempt_ended_monotonic_ns < request_started_monotonic_ns:
        raise ValueError("attempt end precedes request start monotonically")
    if first_monotonic_ns is not None and attempt_ended_monotonic_ns < first_monotonic_ns:
        raise ValueError("attempt end precedes first header monotonically")


def _validate_scheduled_slot(scheduled_slot_wall_ms: int) -> None:
    _require_nonnegative_int(scheduled_slot_wall_ms, "scheduled_slot_wall_ms")
    if scheduled_slot_wall_ms % PUBLIC_OI_REST_POLL_INTERVAL_MS_V2 != 0:
        raise ValueError("scheduled slot is not a UTC epoch multiple of the poll interval")


def _wall_ms_is_in_slot(wall_ms: int, scheduled_slot_wall_ms: int) -> bool:
    return (
        scheduled_slot_wall_ms
        <= wall_ms
        < scheduled_slot_wall_ms + PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
    )


def _validate_response_status_and_headers(
    *,
    response_status: int | None,
    response_headers: CanonicalPairs,
    first_header_seen: bool,
) -> None:
    if response_status is not None:
        if type(response_status) is not int or not 100 <= response_status <= 599:
            raise ValueError("response_status must be an exact HTTP status integer")
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
            raise TypeError("each response header must be an exact name/value tuple")
        name, value = item
        if (
            type(name) is not str
            or len(name) > _MAX_RESPONSE_HEADER_NAME_LENGTH
            or name != name.casefold()
        ):
            raise ValueError("response header names must be normalized lowercase text")
        if not is_allowed_rest_response_header(name):
            raise ValueError("response_headers contains a non-allowlisted name")
        if (
            type(value) is not str
            or value.strip() != value
            or len(value) > _MAX_RESPONSE_HEADER_VALUE_LENGTH
            or any(character in value for character in "\r\n\x00")
        ):
            raise ValueError("response_headers contains an invalid bounded value")


def _validate_error_contract(
    *,
    response_status: int | None,
    payload_complete: bool,
    body_len: int,
    error_category: PublicOiRestErrorCategoryV2 | None,
    error_detail: str | None,
) -> None:
    if (error_category is None) != (error_detail is None):
        raise ValueError("public OI REST error category and detail must be paired")
    if error_category is not None and type(error_category) is not PublicOiRestErrorCategoryV2:
        raise TypeError("error_category must be an exact public OI REST category")
    if error_detail is not None:
        _validate_safe_error_detail(error_detail)
    if error_category is None:
        if not payload_complete or response_status is None or not 200 <= response_status < 300:
            raise ValueError("error-free attempts require one complete 2xx response")
        return
    if response_status is None:
        if payload_complete or body_len != 0:
            raise ValueError("pre-header errors require one empty incomplete body")
        if error_category not in {
            PublicOiRestErrorCategoryV2.NETWORK,
            PublicOiRestErrorCategoryV2.TIMEOUT,
            PublicOiRestErrorCategoryV2.PROTOCOL,
            PublicOiRestErrorCategoryV2.CANCELLED,
        }:
            raise ValueError("pre-header error category is inconsistent with no response")
        return
    if error_category is PublicOiRestErrorCategoryV2.HTTP_STATUS:
        if not payload_complete or 200 <= response_status < 300:
            raise ValueError("HTTP_STATUS requires one complete non-2xx response")
        return
    if error_category is PublicOiRestErrorCategoryV2.BODY_LIMIT:
        if payload_complete or body_len != PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2:
            raise ValueError("BODY_LIMIT requires the exact capped incomplete prefix")
        return
    if payload_complete:
        allowed_complete = {
            PublicOiRestErrorCategoryV2.CANCELLED,
            PublicOiRestErrorCategoryV2.RESPONSE_CLOSE,
            PublicOiRestErrorCategoryV2.TIMEOUT,
        }
        if error_category not in allowed_complete:
            raise ValueError("complete response has an inconsistent error category")
        if (
            not 200 <= response_status < 300
            and error_category
            not in {
                PublicOiRestErrorCategoryV2.CANCELLED,
                PublicOiRestErrorCategoryV2.TIMEOUT,
            }
        ):
            raise ValueError("complete non-2xx close failures retain HTTP_STATUS as primary")
        return
    if error_category not in {
        PublicOiRestErrorCategoryV2.TIMEOUT,
        PublicOiRestErrorCategoryV2.RESPONSE_READ,
        PublicOiRestErrorCategoryV2.CANCELLED,
    }:
        raise ValueError("incomplete response has an inconsistent error category")


def _validate_plan_contract(plan: ProvisionalPromotingRestCapturePlanV2) -> None:
    if type(plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise TypeError("public OI REST evidence requires the exact promoting REST plan")
    # The plan remains the sole acquisition-policy owner. Re-run its frozen
    # validator, then assert the payload schema's compatibility constants so a
    # future plan change cannot silently reuse this attempt schema.
    plan.__post_init__()
    if plan.venue is not VenueV2.USDM_FUTURES:
        raise ValueError("public OI REST plan requires the USD-M Futures venue")
    expected: Mapping[str, object] = {
        "route_id": _PUBLIC_OI_REST_ROUTE_ID,
        "method": _PUBLIC_OI_REST_METHOD,
        "base_url": PUBLIC_OI_REST_BASE_URL_V2,
        "endpoint": PUBLIC_OI_REST_ENDPOINT_V2,
        "request_fields": _PUBLIC_OI_REST_REQUEST_FIELDS,
        "auth_mode": _PUBLIC_OI_REST_AUTH_MODE,
        "requires_api_key": False,
        "is_private": False,
        "promoting": True,
        "promoting_families": ("A", "B", "C"),
        "poll_interval_ms": PUBLIC_OI_REST_POLL_INTERVAL_MS_V2,
        "slot_alignment": _PUBLIC_OI_REST_SLOT_ALIGNMENT,
        "request_timeout_ms": _PUBLIC_OI_REST_REQUEST_TIMEOUT_MS,
        "maximum_body_bytes": PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2,
        "maximum_concurrency": _PUBLIC_OI_REST_MAXIMUM_CONCURRENCY,
        "maximum_attempts": PUBLIC_OI_REST_MAXIMUM_ATTEMPTS_V2,
        "retryable_status_codes": _PUBLIC_OI_REST_RETRYABLE_STATUS_CODES,
        "retryable_error_categories": _PUBLIC_OI_REST_RETRYABLE_ERROR_CATEGORIES,
        "retry_backoff_ms": _PUBLIC_OI_REST_RETRY_BACKOFF_MS,
        "retry_jitter_mode": _PUBLIC_OI_REST_RETRY_JITTER_MODE,
        "maximum_retry_after_ms": _PUBLIC_OI_REST_MAXIMUM_RETRY_AFTER_MS,
        "request_headers": _PUBLIC_OI_REST_REQUEST_HEADERS,
        "response_header_policy": _PUBLIC_OI_REST_RESPONSE_HEADER_POLICY,
        "response_schema": _PUBLIC_OI_REST_RESPONSE_SCHEMA,
        "symbol_order": _PUBLIC_OI_REST_SYMBOL_ORDER,
        "maximum_symbol_census": PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2,
        "missed_slot_policy": _PUBLIC_OI_REST_MISSED_SLOT_POLICY,
        "exhausted_attempt_policy": _PUBLIC_OI_REST_EXHAUSTED_ATTEMPT_POLICY,
    }
    for field_name, expected_value in expected.items():
        try:
            actual_value = getattr(plan, field_name)
        except AttributeError as exc:
            raise TypeError(f"public OI REST plan lacks {field_name}") from exc
        if type(actual_value) is not type(expected_value) or actual_value != expected_value:
            raise ValueError(f"public OI REST plan field {field_name} differs")
    symbols = plan.symbols
    if type(symbols) is not tuple:
        raise TypeError("public OI REST plan symbols must be an exact tuple")
    if not symbols or len(symbols) > PUBLIC_OI_REST_MAXIMUM_SYMBOL_CENSUS_V2:
        raise ValueError("public OI REST plan symbol census is empty or over bound")
    if tuple(sorted(symbols)) != symbols or len(set(symbols)) != len(symbols):
        raise ValueError("public OI REST plan symbols must be unique lexicographic order")
    for symbol in symbols:
        _validate_symbol(symbol)


def _validate_symbol_against_plan(
    plan: ProvisionalPromotingRestCapturePlanV2,
    symbol: str,
    symbol_ordinal: int,
) -> None:
    _require_nonnegative_int(symbol_ordinal, "symbol_ordinal")
    if symbol_ordinal >= len(plan.symbols) or plan.symbols[symbol_ordinal] != symbol:
        raise ValueError("public OI REST symbol and zero-based plan ordinal differ")


def _validate_symbol(symbol: str) -> None:
    if (
        type(symbol) is not str
        or not 5 <= len(symbol) <= 30
        or _SYMBOL_RE.fullmatch(symbol) is None
    ):
        raise ValueError("public OI REST symbol must be normalized uppercase USDT")


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
    if any(token in lowered for token in forbidden):
        raise ValueError("error_detail may not contain credentials or request URLs")


def _strict_body_length(body: bytes) -> int:
    if type(body) is not bytes:
        raise TypeError("public OI REST body must be immutable bytes")
    return len(body)


def _decode_retained_body(
    *,
    encoding: str,
    body_base64: str,
    body_len: int,
    body_sha256: str,
) -> bytes:
    if type(encoding) is not str or encoding != _BODY_ENCODING:
        raise ValueError("public OI REST body encoding must be base64")
    if type(body_base64) is not str:
        raise TypeError("body_base64 must be exact text")
    if len(body_base64) > _MAX_BODY_BASE64_LENGTH:
        raise ValueError("body_base64 exceeds the retained-body encoding bound")
    _require_nonnegative_int(body_len, "body_len")
    if body_len > PUBLIC_OI_REST_MAXIMUM_BODY_BYTES_V2:
        raise ValueError("retained public OI REST body exceeds its byte cap")
    if type(body_sha256) is not str or _SHA256_RE.fullmatch(body_sha256) is None:
        raise ValueError("body_sha256 must be a lowercase SHA-256 digest")
    try:
        body = base64.b64decode(body_base64, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("body_base64 is not valid base64") from exc
    if base64.b64encode(body).decode("ascii") != body_base64:
        raise ValueError("body_base64 is not the canonical base64 representation")
    if len(body) != body_len:
        raise ValueError("body_len differs from the retained body")
    if hashlib.sha256(body).hexdigest() != body_sha256:
        raise ValueError("body_sha256 differs from the retained body")
    return body


def _completion_clocks(completion: ReceiptTimestamp) -> tuple[int, int]:
    if type(completion) is not ReceiptTimestamp:
        raise TypeError("completion_admission must be an exact ReceiptTimestamp")
    _require_nonnegative_int(completion.received_at_ms, "completion_admission_wall_ms")
    _require_nonnegative_int(
        completion.received_monotonic_ns,
        "completion_admission_monotonic_ns",
    )
    return completion.received_at_ms, completion.received_monotonic_ns


def _decode_canonical_document(encoded: bytes) -> dict[str, object]:
    if type(encoded) is not bytes:
        raise TypeError("canonical public OI REST attempt must be immutable bytes")
    if (
        not encoded
        or len(encoded) > _MAX_CANONICAL_PAYLOAD_BYTES
        or not encoded.endswith(b"\n")
        or encoded.count(b"\n") != 1
    ):
        raise ValueError("canonical public OI REST attempt must be one bounded JSONL record")
    try:
        decoded = encoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("canonical public OI REST attempt must be UTF-8") from exc

    def reject_float(value: str) -> object:
        raise ValueError(f"binary float is forbidden in canonical REST JSON: {value}")

    def reject_constant(value: str) -> object:
        raise ValueError(f"non-finite value is forbidden in canonical REST JSON: {value}")

    def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
        document: dict[str, object] = {}
        for key, value in pairs:
            if key in document:
                raise ValueError("canonical public OI REST attempt contains a duplicate key")
            document[key] = value
        return document

    try:
        value = json.loads(
            decoded,
            parse_float=reject_float,
            parse_constant=reject_constant,
            object_pairs_hook=reject_duplicate_keys,
        )
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError("canonical public OI REST attempt is invalid JSON") from exc
    if type(value) is not dict:
        raise ValueError("canonical public OI REST attempt must be a JSON object")
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


def _required_pairs(document: Mapping[str, object], key: str) -> CanonicalPairs:
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


def _require_positive_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def _require_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a nonnegative integer")
