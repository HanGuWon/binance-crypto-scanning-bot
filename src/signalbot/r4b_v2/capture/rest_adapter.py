from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Never, assert_never

import httpx

from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp
from signalbot.capture.rest import normalize_public_response_headers
from signalbot.r4b_v2.capture.pipeline import CaptureBatchPipelineV2
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2
from signalbot.r4b_v2.capture.rest import (
    PublicOiRestErrorCategoryV2,
    PublicOiRestMissedSlotV2,
    PublicOiRestTerminalObservationV2,
    public_oi_rest_source_logical_key_v2,
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
from signalbot.r4b_v2.capture.rest_scheduler import (
    PublicOiScheduleAuthorityV2,
    PublicOiScheduledAttemptTokenV2,
    consume_public_oi_scheduled_attempt_token_v2,
    validate_public_oi_schedule_authority_v2,
)
from signalbot.r4b_v2.capture.websocket import (
    HttpsRestWallClockRegressionErrorV2,
    PublicOiAdmissionReceiptV2,
    SharedWebSocketIngressV2,
)

_MILLISECONDS_PER_SECOND = 1_000

__all__ = (
    "PipelineRestCaptureFatalCoordinatorV2",
    "PublicOpenInterestRestCaptureAdapterV2",
    "RestCaptureAdapterClosedV2",
    "RestCaptureFatalCoordinatorV2",
    "RestCaptureOwnershipFailureV2",
)


class PipelineRestCaptureFatalCoordinatorV2:
    """Production fatal bridge into the exact V2 bounded pipeline handoff."""

    def __init__(self, pipeline: CaptureBatchPipelineV2) -> None:
        if type(pipeline) is not CaptureBatchPipelineV2:
            raise TypeError("REST fatal coordinator requires exact CaptureBatchPipelineV2")
        self.pipeline = pipeline

    def trip_fatal(self, cause: BaseException) -> None:
        if not isinstance(cause, BaseException):
            raise TypeError("REST fatal cause must be an exception")
        self.pipeline.handoff.fail_consumer(cause, failing_ingest_seq=None)

    def raise_if_failed(self) -> None:
        self.pipeline.handoff.fatal_state.raise_if_failed()


@dataclass(frozen=True, slots=True)
class _OpenInterestRestAttempt:
    symbol: str
    poll_cycle_seq: int
    symbol_ordinal: int
    scheduled_slot_wall_ms: int
    attempt: int


class _OpenInterestRestAttemptBinding:
    """OI-only request, evidence, and ingress boundary for the generic owner."""

    def __init__(self, adapter: PublicOpenInterestRestCaptureAdapterV2) -> None:
        self.adapter = adapter

    @property
    def adapter_label(self) -> str:
        return "OI REST"

    def receipt_clock(self) -> ReceiptClock:
        return self.adapter.clock

    def fatal_coordinator(self) -> RestCaptureFatalCoordinatorV2:
        return self.adapter.fatal_coordinator

    def request_spec(self, attempt: _OpenInterestRestAttempt) -> _RestRequestSpec:
        plan = self.adapter.plan
        return _RestRequestSpec(
            method=plan.method,
            url=f"{plan.base_url}{plan.endpoint}",
            params=(("symbol", attempt.symbol),),
            headers=plan.request_headers,
        )

    def validate_request_proof(
        self,
        attempt: _OpenInterestRestAttempt,
        request_proof: _RestRequestProof,
    ) -> None:
        # OI retains its established plan-derived request contract. The generic
        # owner still freezes and mutation-checks the actual request material.
        del attempt, request_proof

    def nonfatal_request_start_failure(
        self,
        attempt: _OpenInterestRestAttempt,
        request_started: ReceiptTimestamp,
    ) -> BaseException | None:
        plan = self.adapter.plan
        if (
            attempt.scheduled_slot_wall_ms
            <= request_started.received_at_ms
            < attempt.scheduled_slot_wall_ms + plan.poll_interval_ms
        ):
            return None
        return PublicOiRestMissedSlotV2(
            symbol=attempt.symbol,
            poll_cycle_seq=attempt.poll_cycle_seq,
            symbol_ordinal=attempt.symbol_ordinal,
            scheduled_slot_wall_ms=attempt.scheduled_slot_wall_ms,
            observed_request_start_wall_ms=request_started.received_at_ms,
        )

    def claim_request_start(
        self,
        attempt: _OpenInterestRestAttempt,
        request_proof: _RestRequestProof,
        request_started: ReceiptTimestamp,
    ) -> None:
        # OI tokens remain consumed at its public adapter seam for exact V2
        # compatibility; only Depth moves capability claim into this hook.
        del attempt, request_proof, request_started

    def normalize_response_headers(
        self,
        headers: httpx.Headers,
    ) -> tuple[tuple[str, str], ...]:
        return normalize_public_response_headers(headers)

    def task_name(
        self,
        stage: _RestAttemptStage,
        attempt: _OpenInterestRestAttempt,
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
            raise ValueError("OI binding received an unsupported task stage")
        return (
            f"r4b-v2-oi-{operation}-{attempt.poll_cycle_seq:08d}-"
            f"{attempt.symbol_ordinal:02d}-{attempt.symbol}"
        )

    def build_observation(
        self,
        attempt: _OpenInterestRestAttempt,
        material: _RestTerminalMaterial,
    ) -> PublicOiRestTerminalObservationV2:
        first_header = material.first_header
        error_category = (
            None
            if material.transport_outcome is None
            else _oi_error_category(material.transport_outcome)
        )
        return PublicOiRestTerminalObservationV2.for_plan(
            self.adapter.plan,
            symbol=attempt.symbol,
            poll_cycle_seq=attempt.poll_cycle_seq,
            symbol_ordinal=attempt.symbol_ordinal,
            scheduled_slot_wall_ms=attempt.scheduled_slot_wall_ms,
            attempt=attempt.attempt,
            request_started_wall_ms=material.request_started.received_at_ms,
            request_started_monotonic_ns=(
                material.request_started.received_monotonic_ns
            ),
            response_first_header_wall_ms=(
                None if first_header is None else first_header.received_at_ms
            ),
            response_first_header_monotonic_ns=(
                None if first_header is None else first_header.received_monotonic_ns
            ),
            attempt_ended_wall_ms=material.attempt_ended.received_at_ms,
            attempt_ended_monotonic_ns=(
                material.attempt_ended.received_monotonic_ns
            ),
            response_status=material.response_status,
            response_headers=material.response_headers,
            payload_complete=material.payload_complete,
            body=material.body,
            error_category=error_category,
            error_detail=material.error_detail,
        )

    def prepare_admission(
        self,
        attempt: _OpenInterestRestAttempt,
        observation: PublicOiRestTerminalObservationV2,
        cancellation_requested: asyncio.Event,
    ) -> Coroutine[Any, Any, PublicOiAdmissionReceiptV2]:
        adapter = self.adapter
        return _admit_oi_attempt(
            ingress=adapter.ingress,
            plan=adapter.plan,
            session_id=adapter.session_id,
            protocol_hash=adapter.protocol_hash,
            connection_id=adapter.connection_id,
            generation=adapter.generation,
            symbol=attempt.symbol,
            clock=adapter.clock,
            observation=observation,
            source_logical_key=public_oi_rest_source_logical_key_v2(
                attempt.symbol
            ),
            cancellation_requested=cancellation_requested,
        )


async def _admit_oi_attempt(
    *,
    ingress: SharedWebSocketIngressV2,
    plan: ProvisionalPromotingRestCapturePlanV2,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    symbol: str,
    clock: ReceiptClock,
    observation: PublicOiRestTerminalObservationV2,
    source_logical_key: str,
    cancellation_requested: asyncio.Event,
) -> PublicOiAdmissionReceiptV2:
    receipt = await ingress.offer_https_attempt(
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=connection_id,
        generation=generation,
        symbol=symbol,
        clock=clock,
        observation=observation,
        source_logical_key=source_logical_key,
        cancellation_requested=cancellation_requested,
    )
    evidence = receipt.wall_clock_regression
    if evidence is not None:
        raise HttpsRestWallClockRegressionErrorV2(
            route_id=plan.route_id,
            symbol=symbol,
            evidence=evidence,
        )
    return receipt


def _oi_error_category(
    outcome: _RestTransportOutcome,
) -> PublicOiRestErrorCategoryV2:
    match outcome:
        case _RestTransportOutcome.NETWORK:
            return PublicOiRestErrorCategoryV2.NETWORK
        case _RestTransportOutcome.TIMEOUT:
            return PublicOiRestErrorCategoryV2.TIMEOUT
        case _RestTransportOutcome.PROTOCOL:
            return PublicOiRestErrorCategoryV2.PROTOCOL
        case _RestTransportOutcome.RESPONSE_READ:
            return PublicOiRestErrorCategoryV2.RESPONSE_READ
        case _RestTransportOutcome.RESPONSE_CLOSE:
            return PublicOiRestErrorCategoryV2.RESPONSE_CLOSE
        case _RestTransportOutcome.HTTP_STATUS:
            return PublicOiRestErrorCategoryV2.HTTP_STATUS
        case _RestTransportOutcome.BODY_LIMIT:
            return PublicOiRestErrorCategoryV2.BODY_LIMIT
        case _RestTransportOutcome.CANCELLED:
            return PublicOiRestErrorCategoryV2.CANCELLED
    assert_never(outcome)


class PublicOpenInterestRestCaptureAdapterV2:
    """OI facade over the sole bounded keyless REST attempt owner.

    The public type and lineage fields remain route-specific because the runtime
    verifies their exact identity. HTTP, cancellation, admission ownership, and
    bounded shutdown live in one private owner shared by future public routes.
    """

    def __init__(
        self,
        plan: ProvisionalPromotingRestCapturePlanV2,
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
        if type(plan) is not ProvisionalPromotingRestCapturePlanV2:
            raise TypeError("OI REST adapter requires the exact promoting REST plan")
        plan.__post_init__()
        _require_bounded_identity(session_id, "session_id")
        _require_lowercase_sha256(protocol_hash, "protocol_hash")
        _require_bounded_identity(connection_id, "connection_id")
        _require_positive_int(generation, "generation")
        _validate_receipt_clock(clock)
        if type(ingress) is not SharedWebSocketIngressV2:
            raise TypeError("OI REST adapter requires exact SharedWebSocketIngressV2")
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
        self._schedule_authority: PublicOiScheduleAuthorityV2 | None = None
        binding = _OpenInterestRestAttemptBinding(self)
        self._owner = _BoundedRestAttemptOwner(
            binding,
            _RestOwnerLimits(
                maximum_concurrency=plan.maximum_concurrency,
                request_timeout_ms=plan.request_timeout_ms,
                maximum_body_bytes=plan.maximum_body_bytes,
                request_headers=plan.request_headers,
                task_prefix="r4b-v2-oi",
                milliseconds_per_second=_MILLISECONDS_PER_SECOND,
            ),
            transport=transport,
        )

    async def __aenter__(self) -> PublicOpenInterestRestCaptureAdapterV2:
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
    def closed(self) -> bool:
        return self._owner.closed

    @property
    def accepting_attempts(self) -> bool:
        return self._owner.accepting_attempts

    @property
    def ownership_dirty(self) -> bool:
        return self._owner.ownership_dirty

    @property
    def pending_owner_task_count(self) -> int:
        return self._owner.pending_owner_task_count

    @property
    def fully_drained(self) -> bool:
        return self._owner.fully_drained

    @property
    def cleanly_closed(self) -> bool:
        return self._owner.cleanly_closed

    @property
    def bound_schedule_authority(self) -> PublicOiScheduleAuthorityV2 | None:
        return self._schedule_authority

    def bind_schedule_authority(
        self,
        schedule_authority: PublicOiScheduleAuthorityV2,
        /,
    ) -> None:
        if self._schedule_authority is not None:
            self._raise_schedule_ownership_failure(
                "OI REST adapter schedule authority was bound more than once"
            )
        if self._owner.closing or self._owner.closed:
            self._raise_schedule_ownership_failure(
                "OI REST adapter cannot bind schedule authority after close started"
            )
        try:
            validate_public_oi_schedule_authority_v2(
                schedule_authority,
                plan=self.plan,
            )
        except Exception as exc:
            self._raise_schedule_ownership_failure(
                "OI REST adapter rejected an invalid schedule authority",
                cause=exc,
            )
        self._schedule_authority = schedule_authority

    async def capture_attempt(
        self,
        token: PublicOiScheduledAttemptTokenV2,
        /,
    ) -> PublicOiAdmissionReceiptV2:
        self._owner.require_open_before_start()
        schedule_authority = self._schedule_authority
        if schedule_authority is None:
            self._raise_schedule_ownership_failure(
                "OI REST adapter has no bound scheduler authority"
            )
        try:
            consume_public_oi_scheduled_attempt_token_v2(
                token,
                plan=self.plan,
                schedule_authority=schedule_authority,
            )
        except Exception as exc:
            self._raise_schedule_ownership_failure(
                "OI REST adapter rejected a foreign or replayed schedule token",
                cause=exc,
            )
        return await self._owner.capture(
            _OpenInterestRestAttempt(
                symbol=token.symbol,
                poll_cycle_seq=token.poll_cycle_seq,
                symbol_ordinal=token.symbol_ordinal,
                scheduled_slot_wall_ms=token.scheduled_slot_wall_ms,
                attempt=token.attempt,
            )
        )

    async def aclose(self) -> None:
        await self._owner.aclose()

    def _raise_schedule_ownership_failure(
        self,
        message: str,
        *,
        cause: BaseException | None = None,
    ) -> Never:
        self._owner.fail_ownership(message, cause=cause)
