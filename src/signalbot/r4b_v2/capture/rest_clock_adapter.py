from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Never, assert_never

import httpx

from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp
from signalbot.capture.rest import normalize_public_response_headers
from signalbot.r4b_v2.capture.plans import (
    ProvisionalUsdmVenueClockRestCapturePlanV9,
)
from signalbot.r4b_v2.capture.rest_attempt_owner import (
    RestCaptureAdapterClosedV2,
    RestCaptureFatalCoordinatorV2,
    RestCaptureOwnershipFailureV2,
    _BoundedRestAttemptOwner,
    _require_bounded_identity,
    _require_lowercase_sha256,
    _require_positive_int,
    _RestAttemptStage,
    _RestOwnerLimits,
    _RestRequestProof,
    _RestRequestSpec,
    _RestTerminalMaterial,
    _RestTransportOutcome,
    _validate_fatal_coordinator,
    _validate_receipt_clock,
)
from signalbot.r4b_v2.capture.rest_clock import (
    PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
    PublicUsdmVenueClockRestErrorCategoryV9,
    PublicUsdmVenueClockRestTerminalObservationV9,
)
from signalbot.r4b_v2.capture.rest_clock_scheduler import (
    PublicUsdmVenueClockMissedSlotV9,
    PublicUsdmVenueClockScheduleAuthorityV9,
    PublicUsdmVenueClockScheduledAttemptTokenV9,
    consume_public_usdm_venue_clock_scheduled_attempt_token_v9,
    validate_public_usdm_venue_clock_schedule_authority_v9,
    validate_public_usdm_venue_clock_scheduled_attempt_token_v9,
)
from signalbot.r4b_v2.capture.websocket import (
    HttpsRestWallClockRegressionErrorV2,
    PublicUsdmVenueClockAdmissionReceiptV9,
    SharedWebSocketIngressV2,
)

_MILLISECONDS_PER_SECOND = 1_000

__all__ = (
    "PublicUsdmVenueClockRestCaptureAdapterV9",
    "RestCaptureAdapterClosedV2",
    "RestCaptureFatalCoordinatorV2",
    "RestCaptureOwnershipFailureV2",
)


@dataclass(frozen=True, slots=True)
class _UsdmVenueClockRestAttemptV9:
    token: PublicUsdmVenueClockScheduledAttemptTokenV9


class _UsdmVenueClockRestAttemptBindingV9:
    """Route-specific request proof and shared-ingress admission boundary."""

    def __init__(self, adapter: PublicUsdmVenueClockRestCaptureAdapterV9) -> None:
        self.adapter = adapter

    @property
    def adapter_label(self) -> str:
        return "USD-M venue-clock REST"

    def receipt_clock(self) -> ReceiptClock:
        return self.adapter.clock

    def fatal_coordinator(self) -> RestCaptureFatalCoordinatorV2:
        return self.adapter.fatal_coordinator

    def request_spec(self, attempt: _UsdmVenueClockRestAttemptV9) -> _RestRequestSpec:
        plan = attempt.token.plan
        return _RestRequestSpec(
            method=plan.method,
            url=f"{plan.base_url}{plan.endpoint}",
            params=plan.fixed_query,
            headers=plan.request_headers,
        )

    def validate_request_proof(
        self,
        attempt: _UsdmVenueClockRestAttemptV9,
        request_proof: _RestRequestProof,
    ) -> None:
        plan = attempt.token.plan
        url = httpx.URL(request_proof.url)
        expected_url = httpx.URL(f"{plan.base_url}{plan.endpoint}")
        expected_headers = tuple(
            sorted(
                (
                    ("host", expected_url.host),
                    ("connection", "keep-alive"),
                    *plan.request_headers,
                )
            )
        )
        if (
            request_proof.method != plan.method
            or url.scheme != expected_url.scheme
            or url.username
            or url.password
            or url.host != expected_url.host
            or url.port != expected_url.port
            or url.path != plan.endpoint
            or url.raw_path != expected_url.raw_path
            or tuple(url.params.multi_items()) != plan.fixed_query
            or url.query != expected_url.query
            or url.fragment
            or request_proof.headers != expected_headers
            or request_proof.body != b""
        ):
            raise RestCaptureOwnershipFailureV2(
                "actual venue-clock request differs from its exact public/no-key plan"
            )

    def nonfatal_request_start_failure(
        self,
        attempt: _UsdmVenueClockRestAttemptV9,
        request_started: ReceiptTimestamp,
    ) -> BaseException | None:
        token = attempt.token
        slot_end = token.scheduled_slot_wall_ms + token.plan.poll_interval_ms
        if token.scheduled_slot_wall_ms <= request_started.received_at_ms < slot_end:
            return None
        return PublicUsdmVenueClockMissedSlotV9(
            poll_cycle_seq=token.poll_cycle_seq,
            scheduled_slot_wall_ms=token.scheduled_slot_wall_ms,
            observed_request_start_wall_ms=request_started.received_at_ms,
        )

    def claim_request_start(
        self,
        attempt: _UsdmVenueClockRestAttemptV9,
        request_proof: _RestRequestProof,
        request_started: ReceiptTimestamp,
    ) -> None:
        del request_proof, request_started
        authority = self.adapter.bound_schedule_authority
        if authority is None:
            raise RestCaptureOwnershipFailureV2(
                "venue-clock request start lacks its bound schedule authority"
            )
        consume_public_usdm_venue_clock_scheduled_attempt_token_v9(
            attempt.token,
            plan=self.adapter.plan,
            schedule_authority=authority,
        )

    def normalize_response_headers(
        self,
        headers: httpx.Headers,
    ) -> tuple[tuple[str, str], ...]:
        return normalize_public_response_headers(headers)

    def task_name(
        self,
        stage: _RestAttemptStage,
        attempt: _UsdmVenueClockRestAttemptV9,
        *,
        cancelled: bool,
    ) -> str:
        if stage is _RestAttemptStage.SEND:
            operation = "send"
        elif stage is _RestAttemptStage.READ:
            operation = "read"
        elif stage is _RestAttemptStage.ADMISSION:
            operation = "admit-cancel" if cancelled else "admit"
        else:
            raise ValueError("venue-clock binding received an unsupported task stage")
        return f"r4b-v9-venue-clock-{operation}-{attempt.token.poll_cycle_seq:08d}"

    def build_observation(
        self,
        attempt: _UsdmVenueClockRestAttemptV9,
        material: _RestTerminalMaterial,
    ) -> PublicUsdmVenueClockRestTerminalObservationV9:
        first_header = material.first_header
        error_category = (
            None
            if material.transport_outcome is None
            else _clock_error_category(material.transport_outcome)
        )
        token = attempt.token
        return PublicUsdmVenueClockRestTerminalObservationV9.for_plan(
            token.plan,
            session_id=self.adapter.session_id,
            protocol_hash=self.adapter.protocol_hash,
            connection_id=self.adapter.connection_id,
            connection_generation=self.adapter.generation,
            poll_cycle_seq=token.poll_cycle_seq,
            scheduled_slot_wall_ms=token.scheduled_slot_wall_ms,
            request_started_wall_ms=material.request_started.received_at_ms,
            request_started_monotonic_ns=(material.request_started.received_monotonic_ns),
            response_first_header_wall_ms=(
                None if first_header is None else first_header.received_at_ms
            ),
            response_first_header_monotonic_ns=(
                None if first_header is None else first_header.received_monotonic_ns
            ),
            attempt_ended_wall_ms=material.attempt_ended.received_at_ms,
            attempt_ended_monotonic_ns=material.attempt_ended.received_monotonic_ns,
            response_status=material.response_status,
            response_headers=material.response_headers,
            payload_complete=material.payload_complete,
            body=material.body,
            error_category=error_category,
            error_detail=material.error_detail,
        )

    def prepare_admission(
        self,
        attempt: _UsdmVenueClockRestAttemptV9,
        observation: PublicUsdmVenueClockRestTerminalObservationV9,
        cancellation_requested: asyncio.Event,
    ) -> Coroutine[Any, Any, PublicUsdmVenueClockAdmissionReceiptV9]:
        adapter = self.adapter
        return _admit_venue_clock_attempt_v9(
            ingress=adapter.ingress,
            plan=adapter.plan,
            session_id=adapter.session_id,
            protocol_hash=adapter.protocol_hash,
            connection_id=adapter.connection_id,
            generation=adapter.generation,
            clock=adapter.clock,
            observation=observation,
            cancellation_requested=cancellation_requested,
        )


class PublicUsdmVenueClockRestCaptureAdapterV9:
    """Single-connection, keyless, bounded venue-time HTTP adapter."""

    def __init__(
        self,
        plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
        *,
        session_id: str,
        protocol_hash: str,
        connection_id: str,
        generation: int,
        clock: ReceiptClock,
        ingress: SharedWebSocketIngressV2,
        fatal_coordinator: RestCaptureFatalCoordinatorV2,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if type(plan) is not ProvisionalUsdmVenueClockRestCapturePlanV9:
            raise TypeError("venue-clock adapter requires the exact v9 plan")
        plan.__post_init__()
        _require_bounded_identity(session_id, "session_id")
        _require_lowercase_sha256(protocol_hash, "protocol_hash")
        _require_bounded_identity(connection_id, "connection_id")
        _require_positive_int(generation, "generation")
        _validate_receipt_clock(clock)
        if type(ingress) is not SharedWebSocketIngressV2:
            raise TypeError("venue-clock adapter requires exact shared ingress")
        _validate_fatal_coordinator(fatal_coordinator)
        if transport is not None and not isinstance(transport, httpx.AsyncBaseTransport):
            raise TypeError("transport must be an HTTPX AsyncBaseTransport or None")

        self.plan = plan
        self.session_id = session_id
        self.protocol_hash = protocol_hash
        self.connection_id = connection_id
        self.generation = generation
        self.clock = clock
        self.ingress = ingress
        self.fatal_coordinator = fatal_coordinator
        self._schedule_authority: PublicUsdmVenueClockScheduleAuthorityV9 | None = None
        self._owner = _BoundedRestAttemptOwner(
            _UsdmVenueClockRestAttemptBindingV9(self),
            _RestOwnerLimits(
                maximum_concurrency=plan.maximum_concurrency,
                request_timeout_ms=plan.request_timeout_ms,
                maximum_body_bytes=plan.maximum_body_bytes,
                request_headers=plan.request_headers,
                task_prefix="r4b-v9-venue-clock",
                milliseconds_per_second=_MILLISECONDS_PER_SECOND,
            ),
            transport=transport,
        )

    async def __aenter__(self) -> PublicUsdmVenueClockRestCaptureAdapterV9:
        self._owner.require_open_before_start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        await self.aclose()

    @property
    def active_attempt_count(self) -> int:
        return self._owner.active_attempt_count

    @property
    def owned_io_task_count(self) -> int:
        return self._owner.owned_io_task_count

    @property
    def owned_admission_task_count(self) -> int:
        return self._owner.owned_admission_task_count

    @property
    def pending_owner_task_count(self) -> int:
        return self._owner.pending_owner_task_count

    @property
    def accepting_attempts(self) -> bool:
        return self._owner.accepting_attempts

    @property
    def closed(self) -> bool:
        return self._owner.closed

    @property
    def ownership_dirty(self) -> bool:
        return self._owner.ownership_dirty

    @property
    def fully_drained(self) -> bool:
        return self._owner.fully_drained

    @property
    def cleanly_closed(self) -> bool:
        return self._owner.cleanly_closed

    @property
    def bound_schedule_authority(
        self,
    ) -> PublicUsdmVenueClockScheduleAuthorityV9 | None:
        return self._schedule_authority

    def bind_schedule_authority(
        self,
        authority: PublicUsdmVenueClockScheduleAuthorityV9,
        /,
    ) -> None:
        if self._schedule_authority is not None:
            self._raise_schedule_ownership_failure(
                "venue-clock adapter schedule authority was bound more than once"
            )
        if self._owner.closing or self._owner.closed:
            self._raise_schedule_ownership_failure(
                "venue-clock adapter cannot bind schedule authority after close"
            )
        try:
            validate_public_usdm_venue_clock_schedule_authority_v9(
                authority,
                plan=self.plan,
            )
        except Exception as exc:
            self._raise_schedule_ownership_failure(
                "venue-clock adapter rejected an invalid schedule authority",
                cause=exc,
            )
        self._schedule_authority = authority

    async def capture_attempt(
        self,
        token: PublicUsdmVenueClockScheduledAttemptTokenV9,
        /,
    ) -> PublicUsdmVenueClockAdmissionReceiptV9:
        self._owner.require_open_before_start()
        authority = self._schedule_authority
        if authority is None:
            self._raise_schedule_ownership_failure(
                "venue-clock adapter has no bound schedule authority"
            )
        try:
            validate_public_usdm_venue_clock_scheduled_attempt_token_v9(
                token,
                plan=self.plan,
                schedule_authority=authority,
            )
        except Exception as exc:
            self._raise_schedule_ownership_failure(
                "venue-clock adapter rejected a foreign or replayed schedule token",
                cause=exc,
            )
        return await self._owner.capture(_UsdmVenueClockRestAttemptV9(token))

    async def aclose(self) -> None:
        await self._owner.aclose()

    def _raise_schedule_ownership_failure(
        self,
        message: str,
        *,
        cause: BaseException | None = None,
    ) -> Never:
        self._owner.fail_ownership(message, cause=cause)


async def _admit_venue_clock_attempt_v9(
    *,
    ingress: SharedWebSocketIngressV2,
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    clock: ReceiptClock,
    observation: PublicUsdmVenueClockRestTerminalObservationV9,
    cancellation_requested: asyncio.Event,
) -> PublicUsdmVenueClockAdmissionReceiptV9:
    receipt = await ingress.offer_usdm_venue_clock_https_attempt_v9(
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=connection_id,
        generation=generation,
        clock=clock,
        observation=observation,
        source_logical_key=PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
        cancellation_requested=cancellation_requested,
    )
    evidence = receipt.wall_clock_regression
    if evidence is not None:
        raise HttpsRestWallClockRegressionErrorV2(
            route_id=plan.route_id,
            symbol=PUBLIC_USDM_VENUE_CLOCK_SOURCE_LOGICAL_KEY_V9,
            evidence=evidence,
        )
    return receipt


def _clock_error_category(
    outcome: _RestTransportOutcome,
) -> PublicUsdmVenueClockRestErrorCategoryV9:
    match outcome:
        case _RestTransportOutcome.NETWORK:
            return PublicUsdmVenueClockRestErrorCategoryV9.NETWORK
        case _RestTransportOutcome.TIMEOUT:
            return PublicUsdmVenueClockRestErrorCategoryV9.TIMEOUT
        case _RestTransportOutcome.PROTOCOL:
            return PublicUsdmVenueClockRestErrorCategoryV9.PROTOCOL
        case _RestTransportOutcome.RESPONSE_READ:
            return PublicUsdmVenueClockRestErrorCategoryV9.RESPONSE_READ
        case _RestTransportOutcome.RESPONSE_CLOSE:
            return PublicUsdmVenueClockRestErrorCategoryV9.RESPONSE_CLOSE
        case _RestTransportOutcome.HTTP_STATUS:
            return PublicUsdmVenueClockRestErrorCategoryV9.HTTP_STATUS
        case _RestTransportOutcome.BODY_LIMIT:
            return PublicUsdmVenueClockRestErrorCategoryV9.BODY_LIMIT
        case _RestTransportOutcome.CANCELLED:
            return PublicUsdmVenueClockRestErrorCategoryV9.CANCELLED
    assert_never(outcome)
