from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import Coroutine
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Never, Protocol, cast

import httpx

from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp

_LOWERCASE_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_IDENTITY_LENGTH = 256
_MAX_RETAINED_RESPONSE_HEADERS = 16
_MAX_RESPONSE_HEADER_NAME_LENGTH = 128
_MAX_RESPONSE_HEADER_VALUE_LENGTH = 256


class RestCaptureAdapterClosedV2(RuntimeError):
    """The adapter stopped accepting attempts before their request-start seam."""


class RestCaptureOwnershipFailureV2(RuntimeError):
    """An owned I/O, cleanup, admission, or shutdown task escaped its bound."""


class RestCaptureFatalCoordinatorV2(Protocol):
    """Shared first-failure boundary injected into a keyless REST owner."""

    def trip_fatal(self, cause: BaseException) -> None: ...

    def raise_if_failed(self) -> None: ...


class _RestAttemptStage(StrEnum):
    BEFORE_START = "before_start"
    SEND = "send"
    READ = "read"
    CLOSE = "close"
    ADMISSION = "admission"


class _RestTransportOutcome(StrEnum):
    NETWORK = "network"
    TIMEOUT = "timeout"
    PROTOCOL = "protocol"
    RESPONSE_READ = "response_read"
    RESPONSE_CLOSE = "response_close"
    HTTP_STATUS = "http_status"
    BODY_LIMIT = "body_limit"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class _RestOwnerLimits:
    maximum_concurrency: int
    request_timeout_ms: int
    maximum_body_bytes: int
    request_headers: tuple[tuple[str, str], ...]
    task_prefix: str
    milliseconds_per_second: int = 1_000

    def __post_init__(self) -> None:
        _require_positive_int(self.maximum_concurrency, "maximum_concurrency")
        _require_positive_int(self.request_timeout_ms, "request_timeout_ms")
        _require_positive_int(self.maximum_body_bytes, "maximum_body_bytes")
        _require_positive_int(
            self.milliseconds_per_second,
            "milliseconds_per_second",
        )
        if (
            type(self.request_headers) is not tuple
            or any(
                type(item) is not tuple
                or len(item) != 2
                or type(item[0]) is not str
                or type(item[1]) is not str
                for item in self.request_headers
            )
        ):
            raise TypeError("request_headers must be an exact tuple of string pairs")
        if type(self.task_prefix) is not str or not self.task_prefix:
            raise ValueError("task_prefix must be a non-empty string")

    @property
    def timeout_seconds(self) -> float:
        return self.request_timeout_ms / self.milliseconds_per_second


@dataclass(frozen=True, slots=True)
class _RestRequestSpec:
    method: str
    url: str
    params: tuple[tuple[str, str], ...]
    headers: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class _RestRequestProof:
    """Immutable material at the controlled HTTPX transport-entry seam.

    The owner rechecks the same request after transport completion, including
    its exact byte-stream object/content and timeout-only extensions.  This is
    intentionally not a TLS-record, kernel-write, or hostile-transport
    attestation; the production HTTPX transport remains inside the trusted
    capture boundary.
    """

    method: str
    url: str
    headers: tuple[tuple[str, str], ...]
    body: bytes
    stream_body: bytes
    extensions: tuple[tuple[str, object], ...]
    request_stream: httpx.AsyncByteStream = field(repr=False, compare=False)


@dataclass(frozen=True, slots=True)
class _RestTerminalMaterial:
    request_proof: _RestRequestProof
    request_started: ReceiptTimestamp
    first_header: ReceiptTimestamp | None
    attempt_ended: ReceiptTimestamp
    response_status: int | None
    response_headers: tuple[tuple[str, str], ...]
    payload_complete: bool
    body: bytes
    transport_outcome: _RestTransportOutcome | None
    error_detail: str | None


class _RestAttemptBinding[AttemptT, ObservationT, AdmissionT](Protocol):
    """Route-specific behavior around one generic bounded REST attempt."""

    @property
    def adapter_label(self) -> str: ...

    def receipt_clock(self) -> ReceiptClock: ...

    def fatal_coordinator(self) -> RestCaptureFatalCoordinatorV2: ...

    def request_spec(self, attempt: AttemptT) -> _RestRequestSpec: ...

    def validate_request_proof(
        self,
        attempt: AttemptT,
        request_proof: _RestRequestProof,
    ) -> None: ...

    def nonfatal_request_start_failure(
        self,
        attempt: AttemptT,
        request_started: ReceiptTimestamp,
    ) -> BaseException | None: ...

    def claim_request_start(
        self,
        attempt: AttemptT,
        request_proof: _RestRequestProof,
        request_started: ReceiptTimestamp,
    ) -> None: ...

    def normalize_response_headers(
        self,
        headers: httpx.Headers,
    ) -> tuple[tuple[str, str], ...]: ...

    def task_name(
        self,
        stage: _RestAttemptStage,
        attempt: AttemptT,
        *,
        cancelled: bool,
    ) -> str: ...

    def build_observation(
        self,
        attempt: AttemptT,
        material: _RestTerminalMaterial,
    ) -> ObservationT: ...

    def prepare_admission(
        self,
        attempt: AttemptT,
        observation: ObservationT,
        cancellation_requested: asyncio.Event,
    ) -> Coroutine[Any, Any, AdmissionT]: ...


class _RestAttemptNotStarted(RuntimeError):
    def __init__(self, cause: BaseException) -> None:
        super().__init__(str(cause))
        self.cause = cause


@dataclass(slots=True)
class _RestAttemptState[AdmissionT]:
    stage: _RestAttemptStage = _RestAttemptStage.BEFORE_START
    request_started: ReceiptTimestamp | None = None
    request: httpx.Request | None = None
    request_proof: _RestRequestProof | None = None
    first_header: ReceiptTimestamp | None = None
    attempt_ended: ReceiptTimestamp | None = None
    response: httpx.Response | None = None
    response_status: int | None = None
    response_headers: tuple[tuple[str, str], ...] = ()
    body: bytearray = field(default_factory=bytearray)
    payload_complete: bool = False
    current_io_task: asyncio.Task[object] | None = None
    admission_task: asyncio.Task[AdmissionT] | None = None
    cancellation_requested: asyncio.Event = field(default_factory=asyncio.Event)
    pending_failure: BaseException | None = None
    pending_failure_cause: BaseException | None = None


class _BoundedRestAttemptOwner[AttemptT, ObservationT, AdmissionT]:
    """Own HTTP, cancellation, admission, and shutdown for one REST route.

    Route bindings own request metadata, request-start eligibility, terminal
    observation construction, and the exact ingress call. This owner never
    interprets a route body and never retries an attempt.
    """

    def __init__(
        self,
        binding: _RestAttemptBinding[AttemptT, ObservationT, AdmissionT],
        limits: _RestOwnerLimits,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        limits.__post_init__()
        _validate_binding(binding)
        _validate_receipt_clock(binding.receipt_clock())
        _validate_fatal_coordinator(binding.fatal_coordinator())
        if transport is not None and not isinstance(transport, httpx.AsyncBaseTransport):
            raise TypeError("transport must be an HTTPX AsyncBaseTransport or None")

        self.binding = binding
        self.limits = limits
        self._attempt_slots = asyncio.Semaphore(limits.maximum_concurrency)
        self._active_attempts: set[asyncio.Task[object]] = set()
        self._io_tasks: set[asyncio.Task[object]] = set()
        self._io_task_context: dict[
            asyncio.Task[object],
            tuple[_RestAttemptState[AdmissionT], _RestAttemptStage],
        ] = {}
        self._abandoned_io_tasks: set[asyncio.Task[object]] = set()
        self._admission_tasks: set[asyncio.Task[AdmissionT]] = set()
        self._fatal_cause: BaseException | None = None
        self._ownership_dirty = False
        self._closing = False
        self._close_owner: asyncio.Task[None] | None = None
        self._client_close_task: asyncio.Task[None] | None = None
        self._client = httpx.AsyncClient(
            timeout=None,
            follow_redirects=False,
            trust_env=False,
            headers=dict(limits.request_headers),
            limits=httpx.Limits(
                max_connections=limits.maximum_concurrency,
                max_keepalive_connections=limits.maximum_concurrency,
            ),
            transport=transport,
        )

    @property
    def active_attempt_count(self) -> int:
        return len(self._active_attempts)

    @property
    def owned_io_task_count(self) -> int:
        return len(self._io_tasks)

    @property
    def owned_admission_task_count(self) -> int:
        return len(self._admission_tasks)

    @property
    def closed(self) -> bool:
        return self._client.is_closed

    @property
    def closing(self) -> bool:
        return self._closing

    @property
    def accepting_attempts(self) -> bool:
        return (
            not self._closing
            and not self._client.is_closed
            and not self._ownership_dirty
            and self._fatal_cause is None
        )

    @property
    def ownership_dirty(self) -> bool:
        return self._ownership_dirty

    @property
    def pending_owner_task_count(self) -> int:
        return len(self._pending_owner_tasks())

    @property
    def fully_drained(self) -> bool:
        return not self._pending_owner_tasks()

    @property
    def cleanly_closed(self) -> bool:
        return (
            self._closing
            and self._client.is_closed
            and self.fully_drained
            and not self._ownership_dirty
            and self._fatal_cause is None
        )

    def require_open_before_start(self) -> None:
        if self._closing or self._client.is_closed:
            raise RestCaptureAdapterClosedV2(
                f"{self.binding.adapter_label} adapter is closing or closed"
            )
        self._raise_if_failed()

    def fail_ownership(
        self,
        message: str,
        *,
        cause: BaseException | None = None,
    ) -> Never:
        error = RestCaptureOwnershipFailureV2(message)
        if cause is not None:
            error.__cause__ = cause
        self._trip_fatal(error)
        if cause is not None:
            raise error from cause
        raise error

    async def capture(self, attempt: AttemptT) -> AdmissionT:
        """Run one already-authorized attempt and retain exactly one terminal row."""

        self.require_open_before_start()
        if (
            self._attempt_slots.locked()
            or len(self._active_attempts) >= self.limits.maximum_concurrency
        ):
            error = RestCaptureOwnershipFailureV2(
                f"{self.binding.adapter_label} direct-call concurrency exceeded "
                "the frozen bound"
            )
            self._trip_fatal(error)
            raise error
        owner = asyncio.current_task()
        if owner is None:
            raise RuntimeError(
                f"{self.binding.adapter_label} attempt requires an owning asyncio task"
            )
        generic_owner = cast(asyncio.Task[object], owner)
        if generic_owner in self._active_attempts:
            raise RuntimeError(
                f"{self.binding.adapter_label} attempt owner is already active"
            )
        state: _RestAttemptState[AdmissionT] = _RestAttemptState()
        acquired = False
        try:
            await self._attempt_slots.acquire()
            acquired = True
            self._active_attempts.add(generic_owner)
            if len(self._active_attempts) > self.limits.maximum_concurrency:
                raise RestCaptureOwnershipFailureV2(
                    f"{self.binding.adapter_label} active-attempt ownership exceeded "
                    "its hard bound"
                )
            self.require_open_before_start()
            self._raise_if_failed()
            return await self._capture_under_slot(state, attempt)
        except asyncio.CancelledError as cancellation:
            if state.request_started is None:
                raise
            try:
                self._harvest_completed_before_cancellation_end(state)
                if state.attempt_ended is None:
                    state.attempt_ended = self._capture_receipt(
                        "cancelled attempt end"
                    )
                await self._retain_after_cancellation(state, attempt)
            except Exception as exc:
                self._trip_fatal(exc)
                raise
            raise cancellation
        except _RestAttemptNotStarted as skipped:
            raise skipped.cause from None
        except RestCaptureAdapterClosedV2 as exc:
            if state.request_started is not None:
                error = RestCaptureOwnershipFailureV2(
                    f"{self.binding.adapter_label} adapter closed after "
                    "the request-start seam"
                )
                self._trip_fatal(error)
                raise error from exc
            raise
        except Exception as exc:
            self._trip_fatal(exc)
            raise
        finally:
            self._active_attempts.discard(generic_owner)
            if acquired:
                self._attempt_slots.release()

    async def aclose(self) -> None:
        """Stop admission, own bounded draining once, and surface fatal state."""

        close_owner = self._close_owner
        if close_owner is None:
            close_owner = asyncio.create_task(
                self._aclose_owned(),
                name=f"{self.limits.task_prefix}-rest-adapter-close",
            )
            self._close_owner = close_owner
        cancellation: asyncio.CancelledError | None = None
        while not close_owner.done():
            try:
                await asyncio.shield(close_owner)
            except asyncio.CancelledError as exc:
                if cancellation is None:
                    cancellation = exc
        if close_owner.cancelled():
            error = RestCaptureOwnershipFailureV2(
                f"{self.binding.adapter_label} adapter close owner was "
                "cancelled unexpectedly"
            )
            self._trip_fatal(error)
            raise error
        close_owner.result()
        if cancellation is not None:
            raise cancellation

    async def _capture_under_slot(
        self,
        state: _RestAttemptState[AdmissionT],
        attempt: AttemptT,
    ) -> AdmissionT:
        spec = self.binding.request_spec(attempt)
        request = self._client.build_request(
            spec.method,
            spec.url,
            params=spec.params,
            headers=dict(spec.headers),
        )
        request_proof = _capture_request_proof(request)
        self.binding.validate_request_proof(attempt, request_proof)
        request_started = self._capture_receipt("request start")
        nonfatal_failure = self.binding.nonfatal_request_start_failure(
            attempt,
            request_started,
        )
        if nonfatal_failure is not None:
            if not isinstance(nonfatal_failure, BaseException):
                raise TypeError(
                    "REST request-start failure must be an exception or None"
                )
            raise _RestAttemptNotStarted(nonfatal_failure)
        self.binding.claim_request_start(attempt, request_proof, request_started)
        state.request = request
        state.request_proof = request_proof
        state.request_started = request_started
        deadline = _loop_time() + self.limits.timeout_seconds
        state.stage = _RestAttemptStage.SEND
        send_operation = self._client.send(
            request,
            stream=True,
            follow_redirects=False,
        )
        send_task: asyncio.Task[httpx.Response] | None = None
        try:
            send_task = asyncio.create_task(
                send_operation,
                name=self.binding.task_name(
                    _RestAttemptStage.SEND,
                    attempt,
                    cancelled=False,
                ),
            )
            self._own_io_task(state, cast(asyncio.Task[object], send_task))
        except Exception as exc:
            if send_task is None:
                send_operation.close()
            else:
                send_task.cancel()
                await _wait_task_bounded(
                    cast(asyncio.Task[object], send_task),
                    timeout_seconds=self.limits.timeout_seconds,
                )
                _observe_task_exception(cast(asyncio.Task[object], send_task))
            state.attempt_ended = self._capture_receipt(
                "send-owner creation failure attempt end"
            )
            failure = RestCaptureOwnershipFailureV2(
                "HTTP send owner could not start after the request claim"
            )
            return await self._retain_then_raise(
                state,
                attempt,
                transport_outcome=_RestTransportOutcome.CANCELLED,
                error_detail="local HTTP send owner failed before transport entry",
                failure=failure,
                cause=exc,
            )
        send_resumed_after_deadline = False
        if not await _wait_until_deadline(send_task, deadline):
            state.attempt_ended = self._capture_receipt(
                "send deadline attempt end"
            )
            cleanup_failure = await self._cancel_and_reap_current_io(state)
            transport_outcome = _RestTransportOutcome.TIMEOUT
            error_detail = "request exceeded the total deadline before response headers"
            if state.response is not None:
                (
                    transport_outcome,
                    error_detail,
                    response_close_failure,
                ) = await self._close_response(
                    state,
                    deadline=deadline,
                    transport_outcome=transport_outcome,
                    error_detail=error_detail,
                )
                if cleanup_failure is None:
                    cleanup_failure = response_close_failure
            if cleanup_failure is not None:
                return await self._retain_then_raise(
                    state,
                    attempt,
                    transport_outcome=transport_outcome,
                    error_detail=error_detail,
                    failure=cleanup_failure,
                )
            return await self._admit_terminal(
                state,
                attempt,
                transport_outcome=transport_outcome,
                error_detail=error_detail,
            )
        send_resumed_after_deadline = _loop_time() > deadline
        self._release_current_io(state)
        try:
            response = send_task.result()
        except asyncio.CancelledError as exc:
            state.attempt_ended = self._capture_receipt(
                "cancelled send attempt end"
            )
            failure = RestCaptureOwnershipFailureV2(
                "HTTP send owner was cancelled without caller cancellation"
            )
            return await self._retain_then_raise(
                state,
                attempt,
                transport_outcome=_RestTransportOutcome.CANCELLED,
                error_detail="HTTP send owner was cancelled before response headers",
                failure=failure,
                cause=exc,
            )
        except httpx.HTTPError as exc:
            state.attempt_ended = self._capture_receipt(
                "pre-header failure attempt end"
            )
            category, detail = _pre_header_error(exc)
            if send_resumed_after_deadline:
                category = _RestTransportOutcome.TIMEOUT
                detail = "event loop resumed after the total send deadline"
            return await self._admit_terminal(
                state,
                attempt,
                transport_outcome=category,
                error_detail=detail,
            )
        except Exception as exc:
            state.attempt_ended = self._capture_receipt(
                "unexpected send attempt end"
            )
            return await self._retain_then_raise(
                state,
                attempt,
                transport_outcome=_RestTransportOutcome.NETWORK,
                error_detail="unexpected transport failure before response headers",
                failure=exc,
            )

        state.response = response
        state.first_header = self._capture_receipt("first response header")
        state.response_status = response.status_code
        try:
            normalized_response_headers = self.binding.normalize_response_headers(
                response.headers
            )
            _validate_bounded_response_headers(normalized_response_headers)
            state.response_headers = normalized_response_headers
        except Exception as exc:
            # Header normalization is part of the bounded retained schema, not
            # an escape hatch after the request-start/claim seam.  Preserve one
            # terminal protocol row with an empty safe header projection, close
            # the response under ownership, then surface the impossible input.
            state.response_headers = ()
            state.attempt_ended = self._capture_receipt(
                "response-header normalization attempt end"
            )
            failure = RestCaptureOwnershipFailureV2(
                f"{self.binding.adapter_label} response headers violated the "
                "bounded normalization contract"
            )
            failure.__cause__ = exc
            (
                transport_outcome,
                error_detail,
                close_failure,
            ) = await self._close_response(
                state,
                deadline=deadline,
                transport_outcome=_RestTransportOutcome.RESPONSE_READ,
                error_detail="response headers violated the bounded normalization contract",
            )
            if close_failure is not None:
                close_failure.__context__ = failure
                failure = RestCaptureOwnershipFailureV2(
                    f"{self.binding.adapter_label} response cleanup failed after "
                    "bounded header normalization failure"
                )
                failure.__cause__ = close_failure
            return await self._retain_then_raise(
                state,
                attempt,
                transport_outcome=transport_outcome,
                error_detail=error_detail,
                failure=failure,
            )
        if send_resumed_after_deadline:
            state.attempt_ended = self._capture_receipt(
                "late send-resume attempt end"
            )
            (
                transport_outcome,
                error_detail,
                close_failure,
            ) = await self._close_response(
                state,
                deadline=deadline,
                transport_outcome=_RestTransportOutcome.TIMEOUT,
                error_detail="event loop resumed after the total send deadline",
            )
            if close_failure is not None:
                return await self._retain_then_raise(
                    state,
                    attempt,
                    transport_outcome=transport_outcome,
                    error_detail=error_detail,
                    failure=close_failure,
                )
            return await self._admit_terminal(
                state,
                attempt,
                transport_outcome=transport_outcome,
                error_detail=error_detail,
            )
        state.stage = _RestAttemptStage.READ
        read_operation = _read_raw_body(
            response,
            state.body,
            maximum_body_bytes=self.limits.maximum_body_bytes,
        )
        read_task: asyncio.Task[
            tuple[bool, _RestTransportOutcome | None, str | None]
        ] | None = None
        try:
            read_task = asyncio.create_task(
                read_operation,
                name=self.binding.task_name(
                    _RestAttemptStage.READ,
                    attempt,
                    cancelled=False,
                ),
            )
            self._own_io_task(state, cast(asyncio.Task[object], read_task))
        except Exception as exc:
            cleanup_failure: BaseException | None = None
            if read_task is None:
                read_operation.close()
            elif state.current_io_task is read_task:
                cleanup_failure = await self._cancel_and_reap_current_io(state)
            else:
                read_task.cancel()
                if not await _wait_task_bounded(
                    cast(asyncio.Task[object], read_task),
                    timeout_seconds=self.limits.timeout_seconds,
                ):
                    cleanup_failure = RestCaptureOwnershipFailureV2(
                        "unowned response-body task exceeded its setup cleanup bound"
                    )
                _observe_task_exception(cast(asyncio.Task[object], read_task))
            state.attempt_ended = self._capture_receipt(
                "read-owner creation failure attempt end"
            )
            (
                transport_outcome,
                error_detail,
                close_failure,
            ) = await self._close_response(
                state,
                deadline=deadline,
                transport_outcome=_RestTransportOutcome.RESPONSE_READ,
                error_detail="local response-body owner failed before body read",
            )
            failure = RestCaptureOwnershipFailureV2(
                "HTTP response-body owner could not start after response headers"
            )
            failure.__cause__ = exc
            if cleanup_failure is not None:
                failure.__context__ = cleanup_failure
            if close_failure is not None:
                failure.__context__ = close_failure
            return await self._retain_then_raise(
                state,
                attempt,
                transport_outcome=transport_outcome,
                error_detail=error_detail,
                failure=failure,
            )
        transport_outcome: _RestTransportOutcome | None = None
        error_detail: str | None = None
        impossible_failure: BaseException | None = None
        read_resumed_after_deadline = False
        if not await _wait_until_deadline(read_task, deadline):
            state.attempt_ended = self._capture_receipt(
                "read deadline attempt end"
            )
            impossible_failure = await self._cancel_and_reap_current_io(state)
            transport_outcome = _RestTransportOutcome.TIMEOUT
            error_detail = "response body exceeded the total attempt deadline"
        else:
            read_resumed_after_deadline = _loop_time() > deadline
            self._release_current_io(state)
            try:
                (
                    state.payload_complete,
                    transport_outcome,
                    error_detail,
                ) = read_task.result()
                if transport_outcome is _RestTransportOutcome.BODY_LIMIT:
                    state.attempt_ended = self._capture_receipt(
                        "body-limit attempt end"
                    )
            except asyncio.CancelledError as exc:
                state.attempt_ended = self._capture_receipt(
                    "cancelled body-owner attempt end"
                )
                transport_outcome = _RestTransportOutcome.CANCELLED
                error_detail = "response body owner was cancelled unexpectedly"
                impossible_failure = RestCaptureOwnershipFailureV2(
                    "response body owner was cancelled without caller cancellation"
                )
                impossible_failure.__cause__ = exc
            except httpx.TimeoutException:
                state.attempt_ended = self._capture_receipt(
                    "body timeout attempt end"
                )
                transport_outcome = _RestTransportOutcome.TIMEOUT
                error_detail = "response body timed out after response headers"
            except httpx.HTTPError:
                state.attempt_ended = self._capture_receipt(
                    "body read failure attempt end"
                )
                transport_outcome = _RestTransportOutcome.RESPONSE_READ
                error_detail = "response body read failed after response headers"
            except Exception as exc:
                state.attempt_ended = self._capture_receipt(
                    "unexpected body failure attempt end"
                )
                transport_outcome = _RestTransportOutcome.RESPONSE_READ
                error_detail = (
                    "unexpected response body failure after response headers"
                )
                impossible_failure = exc

        if read_resumed_after_deadline:
            if state.attempt_ended is None:
                state.attempt_ended = self._capture_receipt(
                    "late read-resume attempt end"
                )
            transport_outcome = _RestTransportOutcome.TIMEOUT
            error_detail = "event loop resumed after the total response-body deadline"

        if (
            state.payload_complete
            and state.response_status is not None
            and not 200 <= state.response_status < 300
            and transport_outcome is None
        ):
            transport_outcome = _RestTransportOutcome.HTTP_STATUS
            error_detail = f"HTTP status {state.response_status}"
        (
            transport_outcome,
            error_detail,
            close_failure,
        ) = await self._close_response(
            state,
            deadline=deadline,
            transport_outcome=transport_outcome,
            error_detail=error_detail,
        )
        if state.attempt_ended is None:
            state.attempt_ended = self._capture_receipt(
                "response-close attempt end"
            )
        if impossible_failure is None:
            impossible_failure = close_failure
        if impossible_failure is not None:
            return await self._retain_then_raise(
                state,
                attempt,
                transport_outcome=transport_outcome,
                error_detail=error_detail,
                failure=impossible_failure,
            )
        return await self._admit_terminal(
            state,
            attempt,
            transport_outcome=transport_outcome,
            error_detail=error_detail,
        )

    async def _start_owned_response_close_task(
        self,
        state: _RestAttemptState[AdmissionT],
        response: httpx.Response,
        *,
        task_name: str,
    ) -> tuple[asyncio.Task[None], BaseException | None]:
        close_operation = response.aclose()
        task_factory_failure: BaseException | None = None
        try:
            close_task = asyncio.create_task(close_operation, name=task_name)
        except Exception as exc:
            close_operation.close()
            fallback_operation = response.aclose()
            try:
                close_task = asyncio.Task(
                    fallback_operation,
                    loop=asyncio.get_running_loop(),
                    name=task_name,
                )
            except Exception:
                fallback_operation.close()
                raise RestCaptureOwnershipFailureV2(
                    "response-close owner could not start after the request claim"
                ) from exc
            task_factory_failure = RestCaptureOwnershipFailureV2(
                "response-close task factory failed after the request claim"
            )
            task_factory_failure.__cause__ = exc
        try:
            self._own_io_task(state, cast(asyncio.Task[object], close_task))
        except Exception as exc:
            close_task.cancel()
            reaped = await _wait_task_bounded(
                cast(asyncio.Task[object], close_task),
                timeout_seconds=self.limits.timeout_seconds,
            )
            if reaped:
                _observe_task_exception(cast(asyncio.Task[object], close_task))
            failure = RestCaptureOwnershipFailureV2(
                "response-close task could not enter exact owner tracking"
            )
            if task_factory_failure is not None:
                failure.__context__ = task_factory_failure
            if not reaped:
                self._mark_ownership_dirty(failure)
            raise failure from exc
        return close_task, task_factory_failure

    async def _close_response(
        self,
        state: _RestAttemptState[AdmissionT],
        *,
        deadline: float,
        transport_outcome: _RestTransportOutcome | None,
        error_detail: str | None,
    ) -> tuple[
        _RestTransportOutcome | None,
        str | None,
        BaseException | None,
    ]:
        response = state.response
        if response is None:
            return transport_outcome, error_detail, None
        had_primary_outcome = transport_outcome is not None
        cleanup_only = state.attempt_ended is not None
        state.stage = _RestAttemptStage.CLOSE
        try:
            (
                close_task,
                task_factory_failure,
            ) = await self._start_owned_response_close_task(
                state,
                response,
                task_name=f"{self.limits.task_prefix}-response-close",
            )
        except RestCaptureOwnershipFailureV2 as exc:
            if state.attempt_ended is None:
                state.attempt_ended = self._capture_receipt(
                    "response-close owner failure attempt end"
                )
            if transport_outcome is None:
                transport_outcome = _RestTransportOutcome.RESPONSE_CLOSE
                error_detail = "local response-close owner could not start"
            return transport_outcome, error_detail, exc
        if task_factory_failure is not None and transport_outcome is None:
            transport_outcome = _RestTransportOutcome.RESPONSE_CLOSE
            error_detail = "local response-close task factory failed"
        close_deadline = deadline
        if cleanup_only:
            close_deadline = _loop_time() + self.limits.timeout_seconds
        if not await _wait_until_deadline(close_task, close_deadline):
            if state.attempt_ended is None:
                state.attempt_ended = self._capture_receipt(
                    "response-close timeout attempt end"
                )
            if transport_outcome is None:
                transport_outcome = _RestTransportOutcome.RESPONSE_CLOSE
                error_detail = "response close exceeded the total attempt deadline"
            cleanup_failure = await self._cancel_and_reap_current_io(state)
            if cleanup_failure is None:
                cleanup_failure = task_factory_failure
            if cleanup_failure is None and had_primary_outcome:
                cleanup_failure = RestCaptureOwnershipFailureV2(
                    "response close exceeded the attempt deadline after a primary outcome"
                )
            if cleanup_failure is None or close_task.done():
                state.response = None
            return transport_outcome, error_detail, cleanup_failure
        self._release_current_io(state)
        state.response = None
        try:
            close_task.result()
        except asyncio.CancelledError as exc:
            if transport_outcome is None:
                transport_outcome = _RestTransportOutcome.RESPONSE_CLOSE
                error_detail = "response close owner was cancelled unexpectedly"
            failure = RestCaptureOwnershipFailureV2(
                "response close owner was cancelled without caller cancellation"
            )
            failure.__cause__ = exc
            if task_factory_failure is not None:
                failure.__context__ = task_factory_failure
            return transport_outcome, error_detail, failure
        except httpx.HTTPError as exc:
            if transport_outcome is None:
                transport_outcome = _RestTransportOutcome.RESPONSE_CLOSE
                error_detail = "response close failed after response headers"
            elif had_primary_outcome:
                failure = RestCaptureOwnershipFailureV2(
                    "response close failed after a primary terminal outcome"
                )
                failure.__cause__ = exc
                if task_factory_failure is not None:
                    failure.__context__ = task_factory_failure
                return transport_outcome, error_detail, failure
        except Exception as exc:
            if transport_outcome is None:
                transport_outcome = _RestTransportOutcome.RESPONSE_CLOSE
                error_detail = (
                    "unexpected response close failure after response headers"
                )
            if task_factory_failure is not None:
                exc.__context__ = task_factory_failure
            return transport_outcome, error_detail, exc
        return transport_outcome, error_detail, task_factory_failure

    async def _admit_terminal(
        self,
        state: _RestAttemptState[AdmissionT],
        attempt: AttemptT,
        *,
        transport_outcome: _RestTransportOutcome | None,
        error_detail: str | None,
    ) -> AdmissionT:
        observation = self._build_observation(
            state,
            attempt,
            transport_outcome=transport_outcome,
            error_detail=error_detail,
        )
        task = self._start_admission_task(
            state,
            attempt,
            observation,
            cancelled=False,
        )
        try:
            admission = await asyncio.shield(task)
            self._raise_pending_failure(state)
            return admission
        finally:
            if task.done():
                self._admission_tasks.discard(task)

    def _build_observation(
        self,
        state: _RestAttemptState[AdmissionT],
        attempt: AttemptT,
        *,
        transport_outcome: _RestTransportOutcome | None,
        error_detail: str | None,
    ) -> ObservationT:
        started = state.request_started
        ended = state.attempt_ended
        request = state.request
        request_proof = state.request_proof
        if (
            started is None
            or ended is None
            or request is None
            or request_proof is None
        ):
            raise RestCaptureOwnershipFailureV2(
                f"terminal {self.binding.adapter_label} evidence lacks "
                "its exact request proof or start/end receipts"
            )
        try:
            current_request_proof = _capture_request_proof(request)
        except Exception as exc:
            raise RestCaptureOwnershipFailureV2(
                f"the sent {self.binding.adapter_label} request mutated before "
                "terminal evidence admission"
            ) from exc
        if (
            current_request_proof != request_proof
            or request.stream is not request_proof.request_stream
        ):
            raise RestCaptureOwnershipFailureV2(
                f"the sent {self.binding.adapter_label} request mutated before "
                "terminal evidence admission"
            )
        self.binding.validate_request_proof(attempt, request_proof)
        material = _RestTerminalMaterial(
            request_proof=request_proof,
            request_started=started,
            first_header=state.first_header,
            attempt_ended=ended,
            response_status=state.response_status,
            response_headers=state.response_headers,
            payload_complete=state.payload_complete,
            body=bytes(state.body),
            transport_outcome=transport_outcome,
            error_detail=error_detail,
        )
        return self.binding.build_observation(attempt, material)

    def _start_admission_task(
        self,
        state: _RestAttemptState[AdmissionT],
        attempt: AttemptT,
        observation: ObservationT,
        *,
        cancelled: bool,
    ) -> asyncio.Task[AdmissionT]:
        if state.admission_task is not None:
            raise RestCaptureOwnershipFailureV2(
                f"one {self.binding.adapter_label} attempt attempted duplicate "
                "ingress admission"
            )
        self._prune_done_tasks()
        if len(self._admission_tasks) >= self.limits.maximum_concurrency:
            qualifier = "cancelled " if cancelled else ""
            raise RestCaptureOwnershipFailureV2(
                f"{qualifier}{self.binding.adapter_label} admission ownership "
                "exceeded its hard bound"
            )
        state.stage = _RestAttemptStage.ADMISSION
        admission = self.binding.prepare_admission(
            attempt,
            observation,
            state.cancellation_requested,
        )
        task_name = self.binding.task_name(
            _RestAttemptStage.ADMISSION,
            attempt,
            cancelled=cancelled,
        )
        try:
            task = asyncio.create_task(admission, name=task_name)
        except Exception as exc:
            # A task-factory failure after the request claim must not strand the
            # one-shot capability. The admission coroutine has not run, so
            # close it and use one explicitly owned loop Task to retain+ACK the
            # terminal row before surfacing the local ownership failure.
            admission.close()
            fallback_admission = self.binding.prepare_admission(
                attempt,
                observation,
                state.cancellation_requested,
            )
            try:
                task = asyncio.Task(
                    fallback_admission,
                    loop=asyncio.get_running_loop(),
                    name=task_name,
                )
            except Exception:
                fallback_admission.close()
                raise RestCaptureOwnershipFailureV2(
                    f"{self.binding.adapter_label} terminal admission owner "
                    "could not start after the request claim"
                ) from exc
            if state.pending_failure is None:
                failure = RestCaptureOwnershipFailureV2(
                    f"{self.binding.adapter_label} terminal admission task "
                    "factory failed after the request claim"
                )
                state.pending_failure = failure
                state.pending_failure_cause = exc
        state.admission_task = task
        self._admission_tasks.add(task)
        task.add_done_callback(self._admission_task_done)
        return task

    async def _retain_after_cancellation(
        self,
        state: _RestAttemptState[AdmissionT],
        attempt: AttemptT,
    ) -> None:
        cancelled_stage = state.stage
        state.cancellation_requested.set()
        admission = state.admission_task
        if admission is not None:
            try:
                await _await_task_repeated_cancellation_safe(
                    admission,
                    timeout_seconds=self.limits.timeout_seconds,
                )
            except RestCaptureOwnershipFailureV2 as exc:
                self._mark_ownership_dirty(exc)
                raise
            finally:
                if admission.done():
                    self._admission_tasks.discard(admission)
            self._raise_pending_failure(state)
            return
        cleanup_failure = await self._cancel_and_reap_current_io(
            state,
            repeated_cancellation_safe=True,
        )
        if state.response is not None:
            response_close_failure = await self._close_after_cancellation(state)
            if cleanup_failure is None:
                cleanup_failure = response_close_failure
        if state.attempt_ended is None:
            raise RestCaptureOwnershipFailureV2(
                f"cancelled {self.binding.adapter_label} attempt lacks its "
                "terminal receipt"
            )
        observation = self._build_observation(
            state,
            attempt,
            transport_outcome=_RestTransportOutcome.CANCELLED,
            error_detail=_cancellation_detail(cancelled_stage),
        )
        admission = self._start_admission_task(
            state,
            attempt,
            observation,
            cancelled=True,
        )
        try:
            await _await_task_repeated_cancellation_safe(
                admission,
                timeout_seconds=self.limits.timeout_seconds,
            )
        except RestCaptureOwnershipFailureV2 as exc:
            self._mark_ownership_dirty(exc)
            raise
        finally:
            if admission.done():
                self._admission_tasks.discard(admission)
        if cleanup_failure is not None:
            self._trip_fatal(cleanup_failure)
            raise cleanup_failure
        self._raise_pending_failure(state)

    async def _close_after_cancellation(
        self,
        state: _RestAttemptState[AdmissionT],
    ) -> BaseException | None:
        response = state.response
        if response is None:
            return None
        state.stage = _RestAttemptStage.CLOSE
        try:
            (
                close_task,
                task_factory_failure,
            ) = await self._start_owned_response_close_task(
                state,
                response,
                task_name=f"{self.limits.task_prefix}-cancelled-response-close",
            )
        except RestCaptureOwnershipFailureV2 as exc:
            return exc
        try:
            await _wait_task_bounded_repeated_cancellation_safe(
                close_task,
                timeout_seconds=self.limits.timeout_seconds,
            )
        except RestCaptureOwnershipFailureV2 as exc:
            self._mark_ownership_dirty(exc)
            return exc
        finally:
            if close_task.done():
                self._release_current_io(state)
                state.response = None
        if close_task.cancelled():
            failure = RestCaptureOwnershipFailureV2(
                "cancelled response close owner was cancelled unexpectedly"
            )
            if task_factory_failure is not None:
                failure.__context__ = task_factory_failure
            return failure
        try:
            close_task.result()
        except httpx.HTTPError as exc:
            failure = RestCaptureOwnershipFailureV2(
                "cancelled response cleanup failed"
            )
            failure.__cause__ = exc
            if task_factory_failure is not None:
                failure.__context__ = task_factory_failure
            return failure
        except Exception as exc:
            failure = RestCaptureOwnershipFailureV2(
                "cancelled response cleanup failed unexpectedly"
            )
            failure.__cause__ = exc
            if task_factory_failure is not None:
                failure.__context__ = task_factory_failure
            return failure
        return task_factory_failure

    async def _retain_then_raise(
        self,
        state: _RestAttemptState[AdmissionT],
        attempt: AttemptT,
        *,
        transport_outcome: _RestTransportOutcome | None,
        error_detail: str | None,
        failure: BaseException,
        cause: BaseException | None = None,
    ) -> AdmissionT:
        state.pending_failure = failure
        state.pending_failure_cause = cause
        await self._admit_terminal(
            state,
            attempt,
            transport_outcome=transport_outcome,
            error_detail=error_detail,
        )
        self._raise_pending_failure(state)
        raise AssertionError("retained REST failure unexpectedly returned")

    async def _cancel_and_reap_current_io(
        self,
        state: _RestAttemptState[AdmissionT],
        *,
        repeated_cancellation_safe: bool = False,
    ) -> BaseException | None:
        task = state.current_io_task
        if task is None:
            return None
        task.cancel()
        if repeated_cancellation_safe:
            try:
                await _wait_task_bounded_repeated_cancellation_safe(
                    task,
                    timeout_seconds=self.limits.timeout_seconds,
                )
            except RestCaptureOwnershipFailureV2 as exc:
                self._mark_ownership_dirty(exc)
                if not task.done():
                    self._abandoned_io_tasks.add(task)
                return exc
        elif not await _wait_task_bounded(
            task,
            timeout_seconds=self.limits.timeout_seconds,
        ):
            failure = RestCaptureOwnershipFailureV2(
                "cancelled HTTP I/O owner exceeded its cleanup bound"
            )
            self._mark_ownership_dirty(failure)
            if not task.done():
                self._abandoned_io_tasks.add(task)
            return failure
        if task.done():
            self._release_current_io(state)
            self._harvest_completed_io(state, task)
        return None

    def _own_io_task(
        self,
        state: _RestAttemptState[AdmissionT],
        task: asyncio.Task[object],
    ) -> None:
        if state.current_io_task is not None:
            raise RestCaptureOwnershipFailureV2(
                f"{self.binding.adapter_label} attempt already owns one HTTP I/O task"
            )
        self._prune_done_tasks()
        if len(self._io_tasks) >= self.limits.maximum_concurrency:
            raise RestCaptureOwnershipFailureV2(
                f"{self.binding.adapter_label} HTTP task ownership exceeded "
                "its hard bound"
            )
        state.current_io_task = task
        self._io_tasks.add(task)
        self._io_task_context[task] = (state, state.stage)
        task.add_done_callback(self._io_task_done)

    def _release_current_io(
        self,
        state: _RestAttemptState[AdmissionT],
    ) -> None:
        task = state.current_io_task
        state.current_io_task = None
        if task is not None and task.done():
            self._io_tasks.discard(task)
            self._io_task_context.pop(task, None)
            self._abandoned_io_tasks.discard(task)

    def _harvest_completed_io(
        self,
        state: _RestAttemptState[AdmissionT],
        task: asyncio.Task[object],
    ) -> None:
        if task.cancelled():
            return
        try:
            result = task.result()
        except Exception:
            return
        if state.stage is _RestAttemptStage.SEND and isinstance(
            result,
            httpx.Response,
        ):
            state.response = result
        elif state.stage is _RestAttemptStage.CLOSE:
            state.response = None

    def _harvest_completed_before_cancellation_end(
        self,
        state: _RestAttemptState[AdmissionT],
    ) -> None:
        task = state.current_io_task
        if task is None or not task.done():
            return
        stage = state.stage
        self._release_current_io(state)
        if task.cancelled():
            return
        try:
            result = task.result()
        except Exception:
            return
        if stage is _RestAttemptStage.SEND and isinstance(result, httpx.Response):
            state.response = result
            state.first_header = self._capture_receipt(
                "cancelled-race first response header"
            )
            state.response_status = result.status_code
            try:
                normalized_headers = self.binding.normalize_response_headers(
                    result.headers
                )
                _validate_bounded_response_headers(normalized_headers)
                state.response_headers = normalized_headers
            except Exception as exc:
                state.response_headers = ()
                failure = RestCaptureOwnershipFailureV2(
                    f"{self.binding.adapter_label} response headers violated "
                    "the bounded normalization contract during cancellation"
                )
                if state.pending_failure is None:
                    state.pending_failure = failure
                    state.pending_failure_cause = exc
        elif stage is _RestAttemptStage.READ and isinstance(result, tuple):
            payload_complete, _category, _detail = result
            if type(payload_complete) is bool:
                state.payload_complete = payload_complete
        elif stage is _RestAttemptStage.CLOSE:
            state.response = None

    def _prune_done_tasks(self) -> None:
        done_io_tasks = tuple(task for task in self._io_tasks if task.done())
        done_admission_tasks = tuple(
            task for task in self._admission_tasks if task.done()
        )
        for task in done_io_tasks:
            self._finalize_io_task(task)
        self._admission_tasks.difference_update(done_admission_tasks)

    def _pending_owner_tasks(self) -> set[asyncio.Task[object]]:
        self._prune_done_tasks()
        owners = {task for task in self._active_attempts if not task.done()}
        owners.update(task for task in self._io_tasks if not task.done())
        owners.update(
            cast(asyncio.Task[object], task)
            for task in self._admission_tasks
            if not task.done()
        )
        close_owner = self._close_owner
        if close_owner is not None and not close_owner.done():
            owners.add(cast(asyncio.Task[object], close_owner))
        client_close_task = self._client_close_task
        if client_close_task is not None and not client_close_task.done():
            owners.add(cast(asyncio.Task[object], client_close_task))
        return owners

    def _io_task_done(self, task: asyncio.Task[object]) -> None:
        self._finalize_io_task(task)

    def _finalize_io_task(self, task: asyncio.Task[object]) -> None:
        self._io_tasks.discard(task)
        context = self._io_task_context.pop(task, None)
        abandoned = task in self._abandoned_io_tasks
        self._abandoned_io_tasks.discard(task)
        if abandoned and context is not None:
            state, owned_stage = context
            if state.current_io_task is task:
                state.current_io_task = None
            self._close_late_response_if_needed(task, owned_stage)
        _observe_task_exception(task)

    def _close_late_response_if_needed(
        self,
        task: asyncio.Task[object],
        owned_stage: _RestAttemptStage,
    ) -> None:
        if owned_stage is not _RestAttemptStage.SEND or task.cancelled():
            return
        try:
            result = task.result()
        except Exception:
            return
        if not isinstance(result, httpx.Response) or result.is_closed:
            return
        close_task = asyncio.create_task(
            result.aclose(),
            name=f"{self.limits.task_prefix}-late-response-close",
        )
        self._io_tasks.add(cast(asyncio.Task[object], close_task))
        close_task.add_done_callback(self._late_response_close_done)

    def _late_response_close_done(self, task: asyncio.Task[None]) -> None:
        self._io_tasks.discard(cast(asyncio.Task[object], task))
        if task.cancelled():
            self._trip_fatal(
                RestCaptureOwnershipFailureV2(
                    "late REST response close owner was cancelled"
                )
            )
            return
        try:
            task.result()
        except Exception as exc:
            failure = RestCaptureOwnershipFailureV2(
                "late REST response cleanup failed"
            )
            failure.__cause__ = exc
            self._trip_fatal(failure)

    def _admission_task_done(self, task: asyncio.Task[AdmissionT]) -> None:
        self._admission_tasks.discard(task)
        _observe_task_exception(cast(asyncio.Task[object], task))

    def _client_close_task_done(self, task: asyncio.Task[None]) -> None:
        _observe_task_exception(cast(asyncio.Task[object], task))

    def _capture_receipt(self, label: str) -> ReceiptTimestamp:
        try:
            receipt = self.binding.receipt_clock().capture()
        except Exception as exc:
            raise RestCaptureOwnershipFailureV2(
                f"{label} clock capture failed"
            ) from exc
        if type(receipt) is not ReceiptTimestamp:
            raise RestCaptureOwnershipFailureV2(
                f"{label} clock returned a non-exact receipt"
            )
        _require_nonnegative_int(receipt.received_at_ms, f"{label} wall time")
        _require_nonnegative_int(
            receipt.received_monotonic_ns,
            f"{label} monotonic time",
        )
        return receipt

    def _trip_fatal(self, cause: BaseException) -> None:
        if not isinstance(cause, BaseException):
            raise TypeError("REST fatal cause must be an exception")
        if self._fatal_cause is not None:
            return
        self._fatal_cause = cause
        self.binding.fatal_coordinator().trip_fatal(cause)

    def _mark_ownership_dirty(self, cause: BaseException) -> None:
        if not isinstance(cause, BaseException):
            raise TypeError("REST ownership-dirty cause must be an exception")
        self._ownership_dirty = True
        self._trip_fatal(cause)

    def _raise_pending_failure(
        self,
        state: _RestAttemptState[AdmissionT],
    ) -> None:
        failure = state.pending_failure
        if failure is None:
            return
        self._trip_fatal(failure)
        cause = state.pending_failure_cause
        if cause is not None:
            raise failure from cause
        raise failure

    def _raise_if_failed(self) -> None:
        if self._fatal_cause is not None:
            raise self._fatal_cause
        self.binding.fatal_coordinator().raise_if_failed()

    async def _aclose_owned(self) -> None:
        self._closing = True
        deadline = (
            asyncio.get_running_loop().time() + self.limits.timeout_seconds
        )
        shutdown_failure: BaseException | None = None
        active_snapshot = set(self._active_attempts)
        pending = await _wait_task_set_bounded(
            active_snapshot,
            timeout_seconds=_remaining_seconds(deadline),
        )
        if pending:
            shutdown_failure = RestCaptureOwnershipFailureV2(
                f"active {self.binding.adapter_label} attempts exceeded "
                "the graceful close bound"
            )
            self._mark_ownership_dirty(shutdown_failure)
            for task in pending:
                task.cancel()
            await _join_task_set_repeated_cancellation_safe(
                pending,
                timeout_seconds=_remaining_seconds(deadline),
            )

        self._prune_done_tasks()
        io_owners = set(self._io_tasks)
        admission_owners = set(self._admission_tasks)
        owners: set[asyncio.Task[object]] = set(io_owners)
        owners.update(cast(set[asyncio.Task[object]], admission_owners))
        pending_owners = await _wait_task_set_bounded(
            owners,
            timeout_seconds=_remaining_seconds(deadline),
        )
        if pending_owners:
            if shutdown_failure is None:
                shutdown_failure = RestCaptureOwnershipFailureV2(
                    "REST cleanup/admission owners exceeded the close bound"
                )
            self._mark_ownership_dirty(shutdown_failure)
            for task in pending_owners.intersection(io_owners):
                task.cancel()
            await _join_task_set_repeated_cancellation_safe(
                pending_owners,
                timeout_seconds=_remaining_seconds(deadline),
            )

        close_task = self._client_close_task
        if close_task is None:
            close_task = asyncio.create_task(
                self._client.aclose(),
                name=f"{self.limits.task_prefix}-httpx-client-close",
            )
            self._client_close_task = close_task
            close_task.add_done_callback(self._client_close_task_done)
        if not await _wait_task_bounded(
            close_task,
            timeout_seconds=_remaining_seconds(deadline),
        ):
            if shutdown_failure is None:
                shutdown_failure = RestCaptureOwnershipFailureV2(
                    "HTTPX client close exceeded its ownership bound"
                )
            self._mark_ownership_dirty(shutdown_failure)
            close_task.cancel()
            await _join_task_set_repeated_cancellation_safe(
                {cast(asyncio.Task[object], close_task)},
                timeout_seconds=_remaining_seconds(deadline),
            )
        if not close_task.done():
            self._ownership_dirty = True
        elif close_task.cancelled():
            if shutdown_failure is None:
                shutdown_failure = RestCaptureOwnershipFailureV2(
                    "HTTPX client close owner was cancelled"
                )
                self._mark_ownership_dirty(shutdown_failure)
        else:
            try:
                close_task.result()
            except Exception as exc:
                if shutdown_failure is None:
                    shutdown_failure = RestCaptureOwnershipFailureV2(
                        "HTTPX client close failed"
                    )
                    shutdown_failure.__cause__ = exc
                    self._mark_ownership_dirty(shutdown_failure)
        if shutdown_failure is not None:
            self._raise_if_failed()
            raise shutdown_failure
        self._raise_if_failed()


async def _read_raw_body(
    response: httpx.Response,
    body: bytearray,
    *,
    maximum_body_bytes: int,
) -> tuple[bool, _RestTransportOutcome | None, str | None]:
    if response.is_stream_consumed:
        content = response.content
        body.extend(content[:maximum_body_bytes])
        if len(content) > maximum_body_bytes:
            return (
                False,
                _RestTransportOutcome.BODY_LIMIT,
                "response body exceeded the configured byte cap",
            )
        return True, None, None
    if not isinstance(response.stream, httpx.AsyncByteStream):
        raise TypeError("public REST response must expose an HTTPX async byte stream")
    response.is_stream_consumed = True
    async for chunk in response.stream:
        if type(chunk) is not bytes:
            raise TypeError("public REST response stream yielded non-bytes content")
        remaining = maximum_body_bytes - len(body)
        if len(chunk) > remaining:
            body.extend(chunk[:remaining])
            return (
                False,
                _RestTransportOutcome.BODY_LIMIT,
                "response body exceeded the configured byte cap",
            )
        body.extend(chunk)
    return True, None, None


def _capture_request_proof(request: httpx.Request) -> _RestRequestProof:
    if type(request) is not httpx.Request:
        raise TypeError("REST request proof requires an exact HTTPX Request")
    try:
        body = request.content
    except httpx.RequestNotRead as exc:
        raise RestCaptureOwnershipFailureV2(
            "REST request body was not materialized before its request-start seam"
        ) from exc
    if type(body) is not bytes:
        raise TypeError("HTTPX request content must be exact bytes")
    if type(request.stream) is not httpx.ByteStream:
        raise RestCaptureOwnershipFailureV2(
            "public REST request must retain the exact materialized HTTPX byte stream"
        )
    stream_chunks = tuple(request.stream)
    if (
        len(stream_chunks) != 1
        or type(stream_chunks[0]) is not bytes
        or stream_chunks[0] != body
    ):
        raise RestCaptureOwnershipFailureV2(
            "public REST request byte stream differs from its materialized body"
        )
    extensions = _capture_request_extensions(request.extensions)
    return _RestRequestProof(
        method=request.method,
        url=str(request.url),
        headers=tuple(
            sorted((name.lower(), value) for name, value in request.headers.multi_items())
        ),
        body=body,
        stream_body=stream_chunks[0],
        extensions=extensions,
        request_stream=request.stream,
    )


def _capture_request_extensions(
    extensions: dict[str, Any],
) -> tuple[tuple[str, object], ...]:
    """Freeze the sole HTTPX timeout extension admitted by this public GET owner."""

    if type(extensions) is not dict or tuple(extensions) != ("timeout",):
        raise RestCaptureOwnershipFailureV2(
            "public REST request extensions differ from the exact timeout-only contract"
        )
    timeout = extensions.get("timeout")
    if type(timeout) is not dict:
        raise RestCaptureOwnershipFailureV2(
            "public REST request timeout extension must be an exact mapping"
        )
    frozen_timeout = tuple(sorted(timeout.items()))
    if frozen_timeout != (
        ("connect", None),
        ("pool", None),
        ("read", None),
        ("write", None),
    ):
        raise RestCaptureOwnershipFailureV2(
            "public REST request timeout extension differs from the no-hidden-timeout contract"
        )
    return (("timeout", frozen_timeout),)


def _validate_bounded_response_headers(
    headers: tuple[tuple[str, str], ...],
) -> None:
    """Enforce the common retained-header envelope before route construction."""

    if type(headers) is not tuple:
        raise TypeError("normalized public REST response headers must be an exact tuple")
    if len(headers) > _MAX_RETAINED_RESPONSE_HEADERS:
        raise ValueError("normalized public REST response header count exceeds its bound")
    if tuple(sorted(headers)) != headers:
        raise ValueError("normalized public REST response headers are not sorted")
    for item in headers:
        if type(item) is not tuple or len(item) != 2:
            raise TypeError("normalized public REST response header must be an exact pair")
        name, value = item
        if (
            type(name) is not str
            or not name
            or name != name.casefold()
            or len(name) > _MAX_RESPONSE_HEADER_NAME_LENGTH
            or any(character in name for character in "\r\n\x00")
        ):
            raise ValueError("normalized public REST response header name is invalid")
        if (
            type(value) is not str
            or value.strip() != value
            or len(value) > _MAX_RESPONSE_HEADER_VALUE_LENGTH
            or any(character in value for character in "\r\n\x00")
        ):
            raise ValueError("normalized public REST response header value is invalid")


def _pre_header_error(
    error: httpx.HTTPError,
) -> tuple[_RestTransportOutcome, str]:
    if isinstance(error, httpx.TimeoutException):
        return (
            _RestTransportOutcome.TIMEOUT,
            "request timed out before response headers",
        )
    if isinstance(error, httpx.ProtocolError):
        return (
            _RestTransportOutcome.PROTOCOL,
            "HTTP protocol failed before response headers",
        )
    return (
        _RestTransportOutcome.NETWORK,
        "network request failed before response headers",
    )


def _cancellation_detail(stage: _RestAttemptStage) -> str:
    if stage is _RestAttemptStage.SEND:
        return "request cancelled before response headers"
    if stage is _RestAttemptStage.READ:
        return "request cancelled while reading response body"
    if stage is _RestAttemptStage.CLOSE:
        return "request cancelled during response close"
    return "request cancelled before retained attempt admission"


def _remaining_seconds(deadline: float) -> float:
    return max(0.0, deadline - _loop_time())


def _loop_time() -> float:
    """Return the event-loop clock used by total-attempt deadline policy."""

    return asyncio.get_running_loop().time()


async def _wait_until_deadline(
    task: asyncio.Task[object],
    deadline: float,
) -> bool:
    remaining = max(0.0, deadline - _loop_time())
    done, _pending = await asyncio.wait((task,), timeout=remaining)
    return task in done


async def _wait_task_bounded(
    task: asyncio.Task[object],
    *,
    timeout_seconds: float,
) -> bool:
    done, _pending = await asyncio.wait((task,), timeout=timeout_seconds)
    return task in done


async def _wait_task_bounded_repeated_cancellation_safe(
    task: asyncio.Task[object],
    *,
    timeout_seconds: float,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while not task.done():
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise RestCaptureOwnershipFailureV2(
                "owned REST task exceeded its bounded cleanup wait"
            )
        try:
            done, _pending = await asyncio.wait((task,), timeout=remaining)
        except asyncio.CancelledError:
            continue
        if task not in done:
            raise RestCaptureOwnershipFailureV2(
                "owned REST task exceeded its bounded cleanup wait"
            )


async def _await_task_repeated_cancellation_safe[AdmissionT](
    task: asyncio.Task[AdmissionT],
    *,
    timeout_seconds: float,
) -> AdmissionT:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while not task.done():
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise RestCaptureOwnershipFailureV2(
                "REST admission owner exceeded its bounded cancellation wait"
            )
        try:
            done, _pending = await asyncio.wait((task,), timeout=remaining)
        except asyncio.CancelledError:
            continue
        if task not in done:
            raise RestCaptureOwnershipFailureV2(
                "REST admission owner exceeded its bounded cancellation wait"
            )
    if task.cancelled():
        raise RestCaptureOwnershipFailureV2(
            "shielded REST admission task was cancelled unexpectedly"
        )
    return task.result()


async def _wait_task_set_bounded(
    tasks: set[asyncio.Task[object]],
    *,
    timeout_seconds: float,
) -> set[asyncio.Task[object]]:
    if not tasks:
        return set()
    _done, pending = await asyncio.wait(tuple(tasks), timeout=timeout_seconds)
    return set(pending)


async def _join_task_set_repeated_cancellation_safe(
    tasks: set[asyncio.Task[object]],
    *,
    timeout_seconds: float,
) -> set[asyncio.Task[object]]:
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    pending = {task for task in tasks if not task.done()}
    while pending:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        try:
            _done, still_pending = await asyncio.wait(
                tuple(pending),
                timeout=remaining,
            )
        except asyncio.CancelledError:
            continue
        pending = set(still_pending)
    for task in tasks:
        _observe_task_exception(task)
    return pending


def _observe_task_exception(task: asyncio.Task[object]) -> None:
    if not task.done():
        return
    try:
        task.exception()
    except asyncio.CancelledError:
        return


def _validate_binding(binding: object) -> None:
    for method_name in (
        "receipt_clock",
        "fatal_coordinator",
        "request_spec",
        "validate_request_proof",
        "nonfatal_request_start_failure",
        "claim_request_start",
        "normalize_response_headers",
        "task_name",
        "build_observation",
    ):
        method = getattr(binding, method_name, None)
        if not callable(method):
            raise TypeError(f"REST attempt binding must expose callable {method_name}")
    for method_name in ("validate_request_proof", "claim_request_start"):
        method = getattr(binding, method_name)
        if inspect.iscoroutinefunction(method):
            raise TypeError(
                f"REST attempt binding must expose synchronous {method_name}"
            )
    adapter_label = getattr(binding, "adapter_label", None)
    if type(adapter_label) is not str or not adapter_label:
        raise TypeError("REST attempt binding must expose a non-empty adapter_label")
    prepare_admission = getattr(binding, "prepare_admission", None)
    if not callable(prepare_admission) or inspect.iscoroutinefunction(
        prepare_admission
    ):
        raise TypeError("REST attempt binding must expose synchronous prepare_admission")


def _validate_receipt_clock(clock: ReceiptClock) -> None:
    capture = getattr(clock, "capture", None)
    if not callable(capture) or inspect.iscoroutinefunction(capture):
        raise TypeError("REST receipt clock must expose synchronous capture")


def _validate_fatal_coordinator(
    coordinator: RestCaptureFatalCoordinatorV2,
) -> None:
    for method_name in ("trip_fatal", "raise_if_failed"):
        method = getattr(coordinator, method_name, None)
        if not callable(method) or inspect.iscoroutinefunction(method):
            raise TypeError(
                f"REST fatal coordinator must expose synchronous {method_name}"
            )


def _require_positive_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")


def _require_nonnegative_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a nonnegative integer")


def _require_bounded_identity(value: str, field_name: str) -> None:
    if (
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field_name} must be a bounded normalized identity")


def _require_lowercase_sha256(value: str, field_name: str) -> None:
    if type(value) is not str or _LOWERCASE_SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
