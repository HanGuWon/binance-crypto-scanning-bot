from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any, cast
from urllib.parse import urlsplit

import httpx

from signalbot.capture.config import CANARY_FIXED_REQUEST_HEADERS
from signalbot.capture.models import (
    RawPayloadEncoding,
    RestEnvelopeV2,
    RestErrorCategory,
    is_allowed_rest_response_header,
    payload_text,
    validate_public_rest_path,
)
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.receipts import IngestSequencer, ReceiptClock, ReceiptTimestamp
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import (
    FUTURES_REST_BASE,
    SPOT_MARKET_DATA_REST_BASE,
)

_PLAN_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAXIMUM_BODY_BYTES = 16 * 1024 * 1024
_MAXIMUM_CONNECTIONS = 4
_MAXIMUM_TIMEOUT_SECONDS = 60.0
_FIXED_REQUEST_HEADERS = {
    **dict(CANARY_FIXED_REQUEST_HEADERS),
    "user-agent": "binance-signalbot-capture/0.1",
}
_PUBLIC_BASES = {
    Market.SPOT: SPOT_MARKET_DATA_REST_BASE,
    Market.FUTURES: FUTURES_REST_BASE,
}
_ALLOWED_REQUEST_HEADERS = frozenset({"accept"})
_SENSITIVE_QUERY_NAMES = frozenset(
    {"apikey", "api_key", "listenkey", "signature", "token", "secret"}
)
_MAXIMUM_CLOSE_CLEANUP_GRACE_SECONDS = 1.0
LOGGER = logging.getLogger(__name__)

QueryItems = Mapping[str, str] | Sequence[tuple[str, str]]


class CaptureRestCleanupFailure(RuntimeError):
    """A bounded transport cleanup could not finish safely."""


class PublicRestCaptureAdapter:
    """Own a keyless HTTPX client and preserve every public GET attempt.

    HTTPX exposes a response to callers only after it has parsed the status and
    headers. Therefore ``response_first_byte_*`` is sampled immediately when
    ``send(..., stream=True)`` returns, before the first body iteration or any
    payload parsing. It is the closest receipt seam exposed by HTTPX, and an
    upper-bound proxy for transport first-byte arrival rather than a NIC-level
    timestamp. ``response_completed_*`` is the authoritative record-admission
    receipt. On the normal response path it is sampled after bounded response
    close and immediately before the shared ingest sequence is allocated and
    the record is offered, with no intervening await. Error paths sample it at
    their own terminal admission seam. This conservative definition keeps the
    persisted global ingest order and receipt order identical even when a
    WebSocket record arrives while HTTPX is closing a completed response.

    The adapter performs one attempt and no retry. A later scheduler owns retry
    policy and passes an explicit correlation ID and attempt number each time.
    """

    def __init__(
        self,
        *,
        plan_sha256: str,
        process_boot_id: str,
        pipeline: CapturePipeline,
        clock: ReceiptClock,
        sequencer: IngestSequencer,
        maximum_body_bytes: int = _MAXIMUM_BODY_BYTES,
        timeout_seconds: float = 15.0,
        maximum_connections: int = _MAXIMUM_CONNECTIONS,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if _PLAN_SHA256.fullmatch(plan_sha256) is None:
            raise ValueError("plan_sha256 must be a lowercase SHA-256 digest")
        if not process_boot_id or process_boot_id.strip() != process_boot_id:
            raise ValueError("process_boot_id must be a normalized non-empty string")
        if (
            isinstance(maximum_body_bytes, bool)
            or maximum_body_bytes < 1
            or maximum_body_bytes > _MAXIMUM_BODY_BYTES
        ):
            raise ValueError("maximum_body_bytes must be between 1 byte and 16 MiB")
        if (
            isinstance(timeout_seconds, bool)
            or timeout_seconds <= 0
            or timeout_seconds > _MAXIMUM_TIMEOUT_SECONDS
        ):
            raise ValueError("timeout_seconds must be between 0 and 60 seconds")
        if (
            isinstance(maximum_connections, bool)
            or maximum_connections < 1
            or maximum_connections > _MAXIMUM_CONNECTIONS
        ):
            raise ValueError("maximum_connections must be between 1 and 4")
        self.plan_sha256 = plan_sha256
        self.process_boot_id = process_boot_id
        self.pipeline = pipeline
        self.clock = clock
        self.sequencer = sequencer
        self.maximum_body_bytes = maximum_body_bytes
        self.timeout_seconds = float(timeout_seconds)
        self.maximum_connections = maximum_connections
        self._pending_cleanup_tasks: set[asyncio.Task[Any]] = set()
        self._attempt_semaphore = asyncio.Semaphore(maximum_connections)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_seconds),
            follow_redirects=False,
            trust_env=False,
            headers=_FIXED_REQUEST_HEADERS,
            limits=httpx.Limits(
                max_connections=maximum_connections,
                max_keepalive_connections=maximum_connections,
            ),
            transport=transport,
        )

    async def __aenter__(self) -> PublicRestCaptureAdapter:
        if self._client.is_closed:
            raise RuntimeError("REST capture adapter is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()
        pending = tuple(self._pending_cleanup_tasks)
        if not pending:
            return
        await asyncio.wait(
            pending,
            timeout=min(
                self.timeout_seconds,
                _MAXIMUM_CLOSE_CLEANUP_GRACE_SECONDS,
            ),
        )
        for task in tuple(self._pending_cleanup_tasks):
            task.cancel()

    async def capture_attempt(
        self,
        *,
        method: str,
        market: Market,
        url: str,
        request_role: str,
        correlation_id: str,
        attempt: int,
        query: QueryItems = (),
        request_headers: Mapping[str, str] | None = None,
    ) -> RestEnvelopeV2:
        """Own one bounded connection slot for the complete attempt lifecycle."""

        async with self._attempt_semaphore:
            await self._require_cleanup_capacity()
            return await self._capture_attempt_under_slot(
                method=method,
                market=market,
                url=url,
                request_role=request_role,
                correlation_id=correlation_id,
                attempt=attempt,
                query=query,
                request_headers=request_headers,
            )

    async def _capture_attempt_under_slot(
        self,
        *,
        method: str,
        market: Market,
        url: str,
        request_role: str,
        correlation_id: str,
        attempt: int,
        query: QueryItems = (),
        request_headers: Mapping[str, str] | None = None,
    ) -> RestEnvelopeV2:
        if self._client.is_closed:
            raise RuntimeError("REST capture adapter is closed")
        endpoint_path = validate_public_rest_url(method, market, url)
        canonical_query = canonicalize_public_query(query)
        headers = normalize_public_request_headers(request_headers)
        if not request_role or request_role.strip() != request_role:
            raise ValueError("request_role must be a normalized non-empty string")
        if not correlation_id or correlation_id.strip() != correlation_id:
            raise ValueError("correlation_id must be a normalized non-empty string")
        if isinstance(attempt, bool) or attempt < 1:
            raise ValueError("attempt must be positive")

        request = self._client.build_request(
            "GET",
            url,
            params=canonical_query,
            headers=headers,
        )
        started = self.clock.capture()
        send_task = asyncio.create_task(
            self._client.send(
                request,
                stream=True,
                follow_redirects=False,
            )
        )
        try:
            send_finished = await _wait_owned_task(
                send_task,
                timeout_seconds=self.timeout_seconds,
            )
        except asyncio.CancelledError:
            completed = self.clock.capture()
            self._offer(
                started=started,
                first_byte=None,
                completed=completed,
                request_role=request_role,
                correlation_id=correlation_id,
                attempt=attempt,
                market=market,
                endpoint_path=endpoint_path,
                canonical_query=canonical_query,
                response_status=None,
                response_headers=(),
                payload_complete=False,
                body=b"",
                error_category=RestErrorCategory.CANCELLED,
                error_detail="request cancelled before response headers",
            )
            self._track_send_cleanup(send_task, cancel=True)
            raise
        if not send_finished:
            completed = self.clock.capture()
            envelope = self._offer(
                started=started,
                first_byte=None,
                completed=completed,
                request_role=request_role,
                correlation_id=correlation_id,
                attempt=attempt,
                market=market,
                endpoint_path=endpoint_path,
                canonical_query=canonical_query,
                response_status=None,
                response_headers=(),
                payload_complete=False,
                body=b"",
                error_category=RestErrorCategory.TIMEOUT,
                error_detail="request exceeded the bounded total time before response headers",
            )
            self._track_send_cleanup(send_task, cancel=True)
            return envelope
        try:
            response = send_task.result()
        except httpx.HTTPError as exc:
            completed = self.clock.capture()
            category, detail = _pre_response_error(exc)
            return self._offer(
                started=started,
                first_byte=None,
                completed=completed,
                request_role=request_role,
                correlation_id=correlation_id,
                attempt=attempt,
                market=market,
                endpoint_path=endpoint_path,
                canonical_query=canonical_query,
                response_status=None,
                response_headers=(),
                payload_complete=False,
                body=b"",
                error_category=category,
                error_detail=detail,
            )

        first_byte = self.clock.capture()
        response_headers = normalize_public_response_headers(response.headers)
        body_buffer = bytearray()
        read_task = asyncio.create_task(
            self._read_response_body(response, body_buffer)
        )
        try:
            read_finished = await _wait_owned_task(
                read_task,
                timeout_seconds=self.timeout_seconds,
            )
        except asyncio.CancelledError:
            completed = self.clock.capture()
            self._offer(
                started=started,
                first_byte=first_byte,
                completed=completed,
                request_role=request_role,
                correlation_id=correlation_id,
                attempt=attempt,
                market=market,
                endpoint_path=endpoint_path,
                canonical_query=canonical_query,
                response_status=response.status_code,
                response_headers=response_headers,
                payload_complete=False,
                body=bytes(body_buffer),
                error_category=RestErrorCategory.CANCELLED,
                error_detail="request cancelled while reading response body",
            )
            self._track_read_cleanup(read_task, response=response, cancel=True)
            raise
        if not read_finished:
            completed = self.clock.capture()
            envelope = self._offer(
                started=started,
                first_byte=first_byte,
                completed=completed,
                request_role=request_role,
                correlation_id=correlation_id,
                attempt=attempt,
                market=market,
                endpoint_path=endpoint_path,
                canonical_query=canonical_query,
                response_status=response.status_code,
                response_headers=response_headers,
                payload_complete=False,
                body=bytes(body_buffer),
                error_category=RestErrorCategory.TIMEOUT,
                error_detail="response body exceeded the bounded total attempt time",
            )
            self._track_read_cleanup(read_task, response=response, cancel=True)
            return envelope
        try:
            payload_complete, error_category, error_detail = read_task.result()
        except asyncio.CancelledError:
            completed = self.clock.capture()
            self._offer(
                started=started,
                first_byte=first_byte,
                completed=completed,
                request_role=request_role,
                correlation_id=correlation_id,
                attempt=attempt,
                market=market,
                endpoint_path=endpoint_path,
                canonical_query=canonical_query,
                response_status=response.status_code,
                response_headers=response_headers,
                payload_complete=False,
                body=bytes(body_buffer),
                error_category=RestErrorCategory.CANCELLED,
                error_detail="response body task was cancelled before completion",
            )
            raise
        except Exception as exc:
            completed = self.clock.capture()
            self._offer(
                started=started,
                first_byte=first_byte,
                completed=completed,
                request_role=request_role,
                correlation_id=correlation_id,
                attempt=attempt,
                market=market,
                endpoint_path=endpoint_path,
                canonical_query=canonical_query,
                response_status=response.status_code,
                response_headers=response_headers,
                payload_complete=False,
                body=bytes(body_buffer),
                error_category=RestErrorCategory.RESPONSE_READ,
                error_detail="unexpected response body failure after response headers",
            )
            self._schedule_response_close(response)
            raise exc
        body = bytes(body_buffer)
        if payload_complete and not 200 <= response.status_code < 300:
            error_category = RestErrorCategory.HTTP_STATUS
            error_detail = f"HTTP status {response.status_code}"

        close_task = asyncio.create_task(response.aclose())
        unexpected_close_error: Exception | None = None
        try:
            await asyncio.wait((close_task,), timeout=self.timeout_seconds)
        except asyncio.CancelledError:
            completed = self.clock.capture()
            self._offer(
                started=started,
                first_byte=first_byte,
                completed=completed,
                request_role=request_role,
                correlation_id=correlation_id,
                attempt=attempt,
                market=market,
                endpoint_path=endpoint_path,
                canonical_query=canonical_query,
                response_status=response.status_code,
                response_headers=response_headers,
                payload_complete=payload_complete,
                body=body,
                error_category=RestErrorCategory.CANCELLED,
                error_detail="request cancelled during response close",
            )
            self._track_cleanup_task(close_task, cancel=True)
            raise
        if close_task.done():
            try:
                await close_task
            except asyncio.CancelledError:
                if error_category is None:
                    error_category = RestErrorCategory.RESPONSE_CLOSE
                    error_detail = "response close was cancelled after response headers"
            except httpx.HTTPError:
                if error_category is None:
                    error_category = RestErrorCategory.RESPONSE_CLOSE
                    error_detail = "response close failed after response headers"
            except Exception as exc:
                if error_category is None:
                    error_category = RestErrorCategory.RESPONSE_CLOSE
                    error_detail = "response close failed after response headers"
                unexpected_close_error = exc
        else:
            if error_category is None:
                error_category = RestErrorCategory.RESPONSE_CLOSE
                error_detail = "response close exceeded the bounded timeout"
            self._track_cleanup_task(close_task, cancel=True)

        # Keep the authoritative receipt, global sequence allocation, and raw
        # offer in one no-await critical section. A WebSocket receipt cannot be
        # allocated between these operations on the shared event loop.
        completed = self.clock.capture()
        envelope = self._offer(
            started=started,
            first_byte=first_byte,
            completed=completed,
            request_role=request_role,
            correlation_id=correlation_id,
            attempt=attempt,
            market=market,
            endpoint_path=endpoint_path,
            canonical_query=canonical_query,
            response_status=response.status_code,
            response_headers=response_headers,
            payload_complete=payload_complete,
            body=body,
            error_category=error_category,
            error_detail=error_detail,
        )
        if unexpected_close_error is not None:
            raise unexpected_close_error
        return envelope

    def _schedule_response_close(self, response: httpx.Response) -> None:
        if len(self._pending_cleanup_tasks) >= self.maximum_connections:
            self._trip_cleanup_capacity()
            return
        self._track_cleanup_task(
            asyncio.create_task(response.aclose()),
            cancel=False,
        )

    def _track_send_cleanup(
        self,
        task: asyncio.Task[httpx.Response],
        *,
        cancel: bool,
    ) -> None:
        generic = cast(asyncio.Task[Any], task)
        if len(self._pending_cleanup_tasks) >= self.maximum_connections:
            self._trip_cleanup_capacity()
            task.cancel()
            generic.add_done_callback(_consume_untracked_cleanup_result)
            return
        self._pending_cleanup_tasks.add(generic)
        task.add_done_callback(self._send_cleanup_finished)
        if cancel:
            task.cancel()

    def _send_cleanup_finished(self, task: asyncio.Task[httpx.Response]) -> None:
        self._pending_cleanup_tasks.discard(cast(asyncio.Task[Any], task))
        try:
            response = task.result()
        except asyncio.CancelledError:
            return
        except Exception:
            LOGGER.error("asynchronous pre-header request cleanup failed")
            return
        self._schedule_response_close(response)

    def _track_read_cleanup(
        self,
        task: asyncio.Task[tuple[bool, RestErrorCategory | None, str | None]],
        *,
        response: httpx.Response,
        cancel: bool,
    ) -> None:
        generic = cast(asyncio.Task[Any], task)
        if len(self._pending_cleanup_tasks) >= self.maximum_connections:
            self._trip_cleanup_capacity()
            task.cancel()
            generic.add_done_callback(_consume_untracked_cleanup_result)
            return
        self._pending_cleanup_tasks.add(generic)
        task.add_done_callback(
            lambda completed: self._read_cleanup_finished(completed, response)
        )
        if cancel:
            task.cancel()

    def _read_cleanup_finished(
        self,
        task: asyncio.Task[tuple[bool, RestErrorCategory | None, str | None]],
        response: httpx.Response,
    ) -> None:
        self._pending_cleanup_tasks.discard(cast(asyncio.Task[Any], task))
        _consume_untracked_cleanup_result(cast(asyncio.Task[Any], task))
        self._schedule_response_close(response)

    def _track_cleanup_task(
        self,
        task: asyncio.Task[Any],
        *,
        cancel: bool,
    ) -> None:
        if len(self._pending_cleanup_tasks) >= self.maximum_connections:
            self._trip_cleanup_capacity()
            task.cancel()
            task.add_done_callback(_consume_untracked_cleanup_result)
            return
        self._pending_cleanup_tasks.add(task)
        task.add_done_callback(self._cleanup_task_finished)
        if cancel:
            task.cancel()

    def _cleanup_task_finished(self, task: asyncio.Task[Any]) -> None:
        self._pending_cleanup_tasks.discard(task)
        _consume_untracked_cleanup_result(task)

    async def _require_cleanup_capacity(self) -> None:
        if not self._pending_cleanup_tasks:
            return
        await asyncio.wait(
            tuple(self._pending_cleanup_tasks),
            timeout=min(
                self.timeout_seconds,
                _MAXIMUM_CLOSE_CLEANUP_GRACE_SECONDS,
            ),
        )
        if self._pending_cleanup_tasks:
            error = CaptureRestCleanupFailure(
                "bounded REST transport cleanup did not complete before reuse"
            )
            self.pipeline.fatal_state.trip_unbound(error)
            raise error

    def _trip_cleanup_capacity(self) -> None:
        error = CaptureRestCleanupFailure(
            "bounded REST transport cleanup capacity was exhausted"
        )
        self.pipeline.fatal_state.trip_unbound(error)
        LOGGER.error("bounded REST transport cleanup capacity was exhausted")

    async def _read_response_body(
        self,
        response: httpx.Response,
        body: bytearray,
    ) -> tuple[bool, RestErrorCategory | None, str | None]:
        if response.is_stream_consumed:
            content = response.content
            body.extend(content[: self.maximum_body_bytes])
            if len(content) > self.maximum_body_bytes:
                return (
                    False,
                    RestErrorCategory.BODY_LIMIT,
                    "response body exceeded the configured byte cap",
                )
            return True, None, None
        if not isinstance(response.stream, httpx.AsyncByteStream):
            raise RuntimeError("REST response does not expose an async byte stream")
        # Iterate the public raw stream directly so EOF and transport close are
        # separate seams. ``Response.aiter_raw`` awaits ``aclose`` before it
        # reports EOF, which can otherwise erase a completed body on shutdown.
        response.is_stream_consumed = True
        try:
            async for chunk in response.stream:
                remaining = self.maximum_body_bytes - len(body)
                if len(chunk) > remaining:
                    body.extend(chunk[:remaining])
                    return (
                        False,
                        RestErrorCategory.BODY_LIMIT,
                        "response body exceeded the configured byte cap",
                    )
                body.extend(chunk)
        except httpx.HTTPError:
            return (
                False,
                RestErrorCategory.RESPONSE_READ,
                "response body read failed after response headers",
            )
        return True, None, None

    def _offer(
        self,
        *,
        started: ReceiptTimestamp,
        first_byte: ReceiptTimestamp | None,
        completed: ReceiptTimestamp,
        request_role: str,
        correlation_id: str,
        attempt: int,
        market: Market,
        endpoint_path: str,
        canonical_query: tuple[tuple[str, str], ...],
        response_status: int | None,
        response_headers: tuple[tuple[str, str], ...],
        payload_complete: bool,
        body: bytes,
        error_category: RestErrorCategory | None,
        error_detail: str | None,
    ) -> RestEnvelopeV2:
        raw_payload, encoding = rest_payload_text(body)
        # No await may occur between the shared sequence allocation and offer.
        ingest_seq = self.sequencer.next()
        envelope = RestEnvelopeV2(
            request_started_at_ms=started.received_at_ms,
            request_started_monotonic_ns=started.received_monotonic_ns,
            response_first_byte_at_ms=(
                None if first_byte is None else first_byte.received_at_ms
            ),
            response_first_byte_monotonic_ns=(
                None if first_byte is None else first_byte.received_monotonic_ns
            ),
            response_completed_at_ms=completed.received_at_ms,
            response_completed_monotonic_ns=completed.received_monotonic_ns,
            plan_sha256=self.plan_sha256,
            process_boot_id=self.process_boot_id,
            request_role=request_role,
            correlation_id=correlation_id,
            attempt=attempt,
            ingest_seq=ingest_seq,
            market=market,
            endpoint_path=endpoint_path,
            canonical_query=canonical_query,
            response_status=response_status,
            response_headers=response_headers,
            payload_complete=payload_complete,
            raw_payload=raw_payload,
            raw_payload_encoding=encoding,
            error_category=error_category,
            error_detail=error_detail,
        )
        self.pipeline.offer(envelope)
        return envelope


def validate_public_rest_url(method: str, market: Market, url: str) -> str:
    if method != "GET":
        raise ValueError("prospective REST capture permits GET only")
    parsed = urlsplit(url)
    expected_base = urlsplit(_PUBLIC_BASES[market])
    if (
        parsed.scheme != "https"
        or parsed.hostname != expected_base.hostname
        or parsed.netloc.casefold() != expected_base.netloc.casefold()
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("REST URL is not an exact allowlisted public host/path URL")
    validate_public_rest_path(market, parsed.path)
    return parsed.path


def canonicalize_public_query(query: QueryItems) -> tuple[tuple[str, str], ...]:
    items = query.items() if isinstance(query, Mapping) else query
    canonical: list[tuple[str, str]] = []
    for name, value in items:
        if not isinstance(name, str) or not isinstance(value, str):
            raise ValueError("REST query names and values must be strings")
        if not name or name.strip() != name or any(character in name for character in "\r\n"):
            raise ValueError("REST query contains an invalid parameter name")
        if any(character in value for character in "\r\n"):
            raise ValueError("REST query contains an invalid parameter value")
        if name.casefold() in _SENSITIVE_QUERY_NAMES:
            raise ValueError("REST query contains a forbidden credential parameter")
        canonical.append((name, value))
    return tuple(sorted(canonical))


def normalize_public_request_headers(
    headers: Mapping[str, str] | None,
) -> dict[str, str]:
    if headers is None:
        return {}
    normalized: dict[str, str] = {}
    for name, value in headers.items():
        lowered = name.casefold()
        if lowered not in _ALLOWED_REQUEST_HEADERS:
            raise ValueError("REST request contains a non-allowlisted header")
        if value.strip() != value or any(character in value for character in "\r\n"):
            raise ValueError("REST request contains an invalid header value")
        normalized[lowered] = value
    return normalized


def normalize_public_response_headers(
    headers: httpx.Headers,
) -> tuple[tuple[str, str], ...]:
    retained = []
    for name, value in headers.multi_items():
        lowered = name.casefold()
        if is_allowed_rest_response_header(lowered):
            retained.append((lowered, value.strip()))
    return tuple(sorted(retained))


def rest_payload_text(body: bytes) -> tuple[str, RawPayloadEncoding]:
    try:
        return body.decode("utf-8"), RawPayloadEncoding.TEXT
    except UnicodeDecodeError:
        return payload_text(body)


def _pre_response_error(exc: httpx.HTTPError) -> tuple[RestErrorCategory, str]:
    if isinstance(exc, httpx.TimeoutException):
        return RestErrorCategory.TIMEOUT, "request timed out before response headers"
    if isinstance(exc, httpx.ProtocolError):
        return RestErrorCategory.PROTOCOL, "HTTP protocol failed before response headers"
    return RestErrorCategory.NETWORK, "network request failed before response headers"


async def _wait_owned_task(
    task: asyncio.Task[Any],
    *,
    timeout_seconds: float,
) -> bool:
    done, _pending = await asyncio.wait((task,), timeout=timeout_seconds)
    return task in done


def _consume_untracked_cleanup_result(task: asyncio.Task[Any]) -> None:
    try:
        error = task.exception()
    except asyncio.CancelledError:
        return
    if error is not None:
        LOGGER.error("asynchronous cancelled-response cleanup failed")
