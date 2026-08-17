from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Never, assert_never

import httpx

from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp
from signalbot.capture.rest import normalize_public_response_headers
from signalbot.r4b_v2.capture.plans import ProvisionalDepthRestQualificationPlanV8
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
from signalbot.r4b_v2.capture.rest_depth import (
    PublicDepthRestErrorCategoryV8,
    PublicDepthRestTerminalObservationV8,
    public_depth_rest_source_logical_key_v8,
    validate_public_depth_rest_plan_v8,
)
from signalbot.r4b_v2.capture.rest_depth_scheduler import (
    PublicDepthRestScheduleAuthorityV8,
    PublicDepthRestScheduledAttemptTokenV8,
    acknowledge_public_depth_rest_terminal_admission_v8,
    consume_public_depth_rest_scheduled_attempt_token_v8,
    validate_public_depth_rest_schedule_authority_v8,
    validate_public_depth_rest_scheduled_attempt_token_v8,
)
from signalbot.r4b_v2.capture.websocket import (
    HttpsRestWallClockRegressionErrorV2,
    PublicDepthRestAdmissionReceiptV8,
    SharedWebSocketIngressV2,
    validate_public_depth_rest_admission_receipt_v8,
)

_MILLISECONDS_PER_SECOND = 1_000

__all__ = (
    "PublicDepthRestCaptureAdapterV8",
    "RestCaptureAdapterClosedV2",
    "RestCaptureOwnershipFailureV2",
)


@dataclass(frozen=True, slots=True)
class _DepthRestAttempt:
    token: PublicDepthRestScheduledAttemptTokenV8

    @property
    def symbol(self) -> str:
        return self.token.symbol


@dataclass(frozen=True, slots=True)
class _DepthRestAdapterIdentity:
    """Construction-time lineage exposed only through read-only properties."""

    plan: ProvisionalDepthRestQualificationPlanV8
    session_id: str
    protocol_hash: str
    connection_id: str
    generation: int


@dataclass(frozen=True, slots=True)
class _DepthRestAdapterDependencies:
    """Construction-time owners exposed through read-only adapter properties."""

    clock: ReceiptClock
    ingress: SharedWebSocketIngressV2
    fatal_coordinator: RestCaptureFatalCoordinatorV2


@dataclass(slots=True)
class _DepthRestTerminalAdmissionSlot:
    """One pre-reserved, process-local recovery slot for an exact token."""

    token: PublicDepthRestScheduledAttemptTokenV8
    receipt: PublicDepthRestAdmissionReceiptV8 | None = None
    recoverable: bool = False


class _DepthRestAttemptBinding:
    """Depth-only request, evidence, ingress, and terminal-ack boundary."""

    def __init__(self, adapter: PublicDepthRestCaptureAdapterV8) -> None:
        self.adapter = adapter

    @property
    def adapter_label(self) -> str:
        return "depth REST"

    def receipt_clock(self) -> ReceiptClock:
        return self.adapter.clock

    def fatal_coordinator(self) -> RestCaptureFatalCoordinatorV2:
        return self.adapter.fatal_coordinator

    def request_spec(self, attempt: _DepthRestAttempt) -> _RestRequestSpec:
        plan = attempt.token.plan
        return _RestRequestSpec(
            method=plan.method,
            url=f"{plan.base_url}{plan.endpoint}",
            params=(*plan.fixed_query, ("symbol", attempt.symbol)),
            headers=plan.request_headers,
        )

    def validate_request_proof(
        self,
        attempt: _DepthRestAttempt,
        request_proof: _RestRequestProof,
    ) -> None:
        _validated_depth_request_material(attempt, request_proof)

    def nonfatal_request_start_failure(
        self,
        attempt: _DepthRestAttempt,
        request_started: ReceiptTimestamp,
    ) -> BaseException | None:
        del attempt, request_started
        return None

    def claim_request_start(
        self,
        attempt: _DepthRestAttempt,
        request_proof: _RestRequestProof,
        request_started: ReceiptTimestamp,
    ) -> None:
        del request_proof, request_started
        adapter = self.adapter
        schedule_authority = adapter.bound_schedule_authority
        if schedule_authority is None:
            raise RestCaptureOwnershipFailureV2(
                "depth REST request start lacks its bound schedule authority"
            )
        try:
            consume_public_depth_rest_scheduled_attempt_token_v8(
                attempt.token,
                plan=adapter.plan,
                schedule_authority=schedule_authority,
            )
        except Exception as exc:
            raise RestCaptureOwnershipFailureV2(
                "depth REST adapter rejected a foreign, stale, or replayed "
                "schedule token at request start"
            ) from exc

    def normalize_response_headers(
        self,
        headers: httpx.Headers,
    ) -> tuple[tuple[str, str], ...]:
        return normalize_public_response_headers(headers)

    def task_name(
        self,
        stage: _RestAttemptStage,
        attempt: _DepthRestAttempt,
        *,
        cancelled: bool,
    ) -> str:
        token = attempt.token
        if stage is _RestAttemptStage.SEND:
            operation = "send"
        elif stage is _RestAttemptStage.READ:
            operation = "read"
        elif stage is _RestAttemptStage.ADMISSION:
            operation = "admit-cancel" if cancelled else "admit"
        else:
            raise ValueError("depth REST binding received an unsupported task stage")
        return (
            f"r4b-v2-depth-rest-{operation}-{token.trigger_seq:08d}-"
            f"{token.symbol_ordinal:02d}-{token.symbol}-b{token.bridge_attempt}"
        )

    def build_observation(
        self,
        attempt: _DepthRestAttempt,
        material: _RestTerminalMaterial,
    ) -> PublicDepthRestTerminalObservationV8:
        token = attempt.token
        first_header = material.first_header
        error_category = (
            None
            if material.transport_outcome is None
            else _depth_error_category(material.transport_outcome)
        )
        (
            method,
            base_url,
            endpoint,
            canonical_query,
            request_headers,
        ) = _validated_depth_request_material(
            attempt,
            material.request_proof,
        )
        return PublicDepthRestTerminalObservationV8.for_plan(
            token.plan,
            session_id=token.session_id,
            protocol_hash=token.protocol_hash,
            connection_id=token.connection_id,
            method=method,
            base_url=base_url,
            endpoint=endpoint,
            canonical_query=canonical_query,
            request_headers=request_headers,
            symbol=token.symbol,
            trigger=token.trigger,
            trigger_seq=token.trigger_seq,
            connection_generation=token.connection_generation,
            first_buffered_u=token.first_buffered_u,
            symbol_ordinal=token.symbol_ordinal,
            bridge_attempt=token.bridge_attempt,
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
        attempt: _DepthRestAttempt,
        observation: PublicDepthRestTerminalObservationV8,
        cancellation_requested: asyncio.Event,
    ) -> Coroutine[Any, Any, PublicDepthRestAdmissionReceiptV8]:
        adapter = self.adapter
        schedule_authority = adapter.bound_schedule_authority
        if schedule_authority is None:
            raise RestCaptureOwnershipFailureV2(
                "depth REST terminal admission lacks its bound schedule authority"
            )

        async def retain_admission_before_post_admission_checks(
        ) -> PublicDepthRestAdmissionReceiptV8:
            receipt = await _admit_depth_attempt(
                ingress=adapter.ingress,
                plan=attempt.token.plan,
                session_id=attempt.token.session_id,
                protocol_hash=attempt.token.protocol_hash,
                connection_id=attempt.token.connection_id,
                generation=attempt.token.connection_generation,
                clock=adapter.clock,
                attempt=attempt,
                observation=observation,
                cancellation_requested=cancellation_requested,
            )
            # Publish immediately after the queue side effect and before every
            # scheduler/post-admission check.  A failing ACK must not hide an
            # already-admitted terminal record from the generation owner.
            adapter._publish_terminal_admission(attempt.token, receipt)
            acknowledge_public_depth_rest_terminal_admission_v8(
                attempt.token,
                receipt,
                plan=attempt.token.plan,
                schedule_authority=schedule_authority,
            )
            evidence = receipt.wall_clock_regression
            if evidence is not None:
                raise HttpsRestWallClockRegressionErrorV2(
                    route_id=attempt.token.plan.route_id,
                    symbol=attempt.symbol,
                    evidence=evidence,
                )
            return receipt

        return retain_admission_before_post_admission_checks()


class PublicDepthRestCaptureAdapterV8:
    """One fixed-connection public depth REST adapter with no retry or orders."""

    def __init__(
        self,
        plan: ProvisionalDepthRestQualificationPlanV8,
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
        validate_public_depth_rest_plan_v8(plan)
        _require_bounded_identity(session_id, "session_id")
        _require_lowercase_sha256(protocol_hash, "protocol_hash")
        _require_bounded_identity(connection_id, "connection_id")
        _require_positive_int(generation, "generation")
        _validate_receipt_clock(clock)
        if type(ingress) is not SharedWebSocketIngressV2:
            raise TypeError("depth REST adapter requires exact SharedWebSocketIngressV2")
        _validate_fatal_coordinator(fatal_coordinator)
        if transport is not None and not isinstance(transport, httpx.AsyncBaseTransport):
            raise TypeError("transport must be an HTTPX AsyncBaseTransport or None")

        self._identity = _DepthRestAdapterIdentity(
            plan=plan,
            session_id=session_id,
            protocol_hash=protocol_hash,
            connection_id=connection_id,
            generation=generation,
        )
        self._dependencies = _DepthRestAdapterDependencies(
            clock=clock,
            ingress=ingress,
            fatal_coordinator=fatal_coordinator,
        )
        self._schedule_authority: PublicDepthRestScheduleAuthorityV8 | None = None
        self._terminal_admission_slots: dict[
            PublicDepthRestScheduledAttemptTokenV8,
            _DepthRestTerminalAdmissionSlot,
        ] = {}
        binding = _DepthRestAttemptBinding(self)
        self._owner = _BoundedRestAttemptOwner(
            binding,
            _RestOwnerLimits(
                maximum_concurrency=plan.maximum_concurrency,
                request_timeout_ms=plan.request_timeout_ms,
                maximum_body_bytes=plan.maximum_body_bytes,
                request_headers=plan.request_headers,
                task_prefix="r4b-v2-depth-rest",
                milliseconds_per_second=_MILLISECONDS_PER_SECOND,
            ),
            transport=transport,
        )

    async def __aenter__(self) -> PublicDepthRestCaptureAdapterV8:
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
    def plan(self) -> ProvisionalDepthRestQualificationPlanV8:
        return self._identity.plan

    @property
    def session_id(self) -> str:
        return self._identity.session_id

    @property
    def protocol_hash(self) -> str:
        return self._identity.protocol_hash

    @property
    def connection_id(self) -> str:
        return self._identity.connection_id

    @property
    def generation(self) -> int:
        return self._identity.generation

    @property
    def clock(self) -> ReceiptClock:
        return self._dependencies.clock

    @property
    def ingress(self) -> SharedWebSocketIngressV2:
        return self._dependencies.ingress

    @property
    def fatal_coordinator(self) -> RestCaptureFatalCoordinatorV2:
        return self._dependencies.fatal_coordinator

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
    def retained_terminal_admission_count(self) -> int:
        """Return all bounded in-flight or failed terminal recovery slots."""

        return len(self._terminal_admission_slots)

    @property
    def fully_drained(self) -> bool:
        return self._owner.fully_drained and not self._terminal_admission_slots

    @property
    def cleanly_closed(self) -> bool:
        return self._owner.cleanly_closed and not self._terminal_admission_slots

    @property
    def bound_schedule_authority(self) -> PublicDepthRestScheduleAuthorityV8 | None:
        return self._schedule_authority

    def bind_schedule_authority(
        self,
        schedule_authority: PublicDepthRestScheduleAuthorityV8,
        /,
    ) -> None:
        if self._schedule_authority is not None:
            self._raise_schedule_ownership_failure(
                "depth REST adapter schedule authority was bound more than once"
            )
        if self._owner.closing or self._owner.closed:
            self._raise_schedule_ownership_failure(
                "depth REST adapter cannot bind schedule authority after close started"
            )
        try:
            validate_public_depth_rest_schedule_authority_v8(
                schedule_authority,
                plan=self.plan,
            )
        except Exception as exc:
            self._raise_schedule_ownership_failure(
                "depth REST adapter rejected an invalid schedule authority",
                cause=exc,
            )
        self._schedule_authority = schedule_authority

    async def capture_attempt(
        self,
        token: PublicDepthRestScheduledAttemptTokenV8,
        /,
    ) -> PublicDepthRestAdmissionReceiptV8:
        self._owner.require_open_before_start()
        schedule_authority = self._schedule_authority
        if schedule_authority is None:
            self._raise_schedule_ownership_failure(
                "depth REST adapter has no bound scheduler authority"
            )
        try:
            validate_public_depth_rest_scheduled_attempt_token_v8(
                token,
                plan=self.plan,
                schedule_authority=schedule_authority,
            )
            if (
                token.session_id != self.session_id
                or token.protocol_hash != self.protocol_hash
                or token.connection_id != self.connection_id
                or token.connection_generation != self.generation
            ):
                raise RestCaptureOwnershipFailureV2(
                    "depth REST token lineage differs from the adapter connection"
                )
        except Exception as exc:
            self._raise_schedule_ownership_failure(
                "depth REST adapter rejected a foreign, stale, or replayed schedule token",
                cause=exc,
            )
        try:
            self._reserve_terminal_admission(token)
        except Exception as exc:
            self._raise_schedule_ownership_failure(
                "depth REST terminal-admission recovery capacity was violated",
                cause=exc,
            )
        try:
            receipt = await self._owner.capture(_DepthRestAttempt(token))
            return self._take_successful_terminal_admission(token, receipt)
        except BaseException:
            self._mark_terminal_admission_recoverable_after_failure(token)
            raise

    def take_terminal_admission_after_failure(
        self,
        token: PublicDepthRestScheduledAttemptTokenV8,
        /,
    ) -> PublicDepthRestAdmissionReceiptV8 | None:
        """Take one exact receipt when capture raised after shared-ingress admission.

        This deliberately uses process-local token identity instead of current
        scheduler state: a pending cycle may already have been promoted after the
        terminal ACK.  The receipt remains bounded by adapter concurrency and is
        removed exactly once.
        """

        if type(token) is not PublicDepthRestScheduledAttemptTokenV8:
            raise TypeError(
                "terminal depth REST admission recovery requires an exact token"
            )
        slot = self._terminal_admission_slots.get(token)
        if slot is None or not slot.recoverable or slot.receipt is None:
            return None
        receipt = slot.receipt
        try:
            validate_public_depth_rest_admission_receipt_v8(
                receipt,
                plan=self.plan,
            )
        except Exception as exc:
            self._raise_schedule_ownership_failure(
                "retained depth REST terminal admission lost exact provenance",
                cause=exc,
            )
        del self._terminal_admission_slots[token]
        return receipt

    async def aclose(self) -> None:
        await self._owner.aclose()

    def _reserve_terminal_admission(
        self,
        token: PublicDepthRestScheduledAttemptTokenV8,
    ) -> None:
        if token in self._terminal_admission_slots:
            raise RestCaptureOwnershipFailureV2(
                "depth REST attempt reused a terminal-admission recovery slot"
            )
        if len(self._terminal_admission_slots) >= self.plan.maximum_concurrency:
            raise RestCaptureOwnershipFailureV2(
                "depth REST terminal-admission recovery slots exceeded the "
                "concurrency bound"
            )
        self._terminal_admission_slots[token] = _DepthRestTerminalAdmissionSlot(
            token=token
        )

    def _publish_terminal_admission(
        self,
        token: PublicDepthRestScheduledAttemptTokenV8,
        receipt: PublicDepthRestAdmissionReceiptV8,
    ) -> None:
        slot = self._terminal_admission_slots.get(token)
        if slot is None or slot.token is not token:
            raise RestCaptureOwnershipFailureV2(
                "depth REST terminal admission lacks its pre-reserved exact token slot"
            )
        if slot.receipt is not None:
            raise RestCaptureOwnershipFailureV2(
                "depth REST attempt produced duplicate terminal admissions"
            )
        # Store first.  Even an impossible provenance failure after the ingress
        # side effect must leave a bounded forensic handle instead of erasing it.
        slot.receipt = receipt
        try:
            validate_public_depth_rest_admission_receipt_v8(
                receipt,
                plan=self.plan,
            )
        except Exception as exc:
            raise RestCaptureOwnershipFailureV2(
                "depth REST terminal admission lost exact ingress provenance"
            ) from exc

    def _mark_terminal_admission_recoverable_after_failure(
        self,
        token: PublicDepthRestScheduledAttemptTokenV8,
    ) -> None:
        slot = self._terminal_admission_slots.get(token)
        if slot is None:
            return
        if slot.receipt is None:
            del self._terminal_admission_slots[token]
            return
        slot.recoverable = True

    def _take_successful_terminal_admission(
        self,
        token: PublicDepthRestScheduledAttemptTokenV8,
        receipt: PublicDepthRestAdmissionReceiptV8,
    ) -> PublicDepthRestAdmissionReceiptV8:
        slot = self._terminal_admission_slots.get(token)
        if (
            slot is None
            or slot.token is not token
            or slot.receipt is not receipt
            or slot.recoverable
        ):
            self._raise_schedule_ownership_failure(
                "depth REST capture returned without its exact retained admission"
            )
        del self._terminal_admission_slots[token]
        return receipt

    def _raise_schedule_ownership_failure(
        self,
        message: str,
        *,
        cause: BaseException | None = None,
    ) -> Never:
        self._owner.fail_ownership(message, cause=cause)


async def _admit_depth_attempt(
    *,
    ingress: SharedWebSocketIngressV2,
    plan: ProvisionalDepthRestQualificationPlanV8,
    session_id: str,
    protocol_hash: str,
    connection_id: str,
    generation: int,
    clock: ReceiptClock,
    attempt: _DepthRestAttempt,
    observation: PublicDepthRestTerminalObservationV8,
    cancellation_requested: asyncio.Event,
) -> PublicDepthRestAdmissionReceiptV8:
    return await ingress.offer_depth_https_attempt_v8(
        plan=plan,
        session_id=session_id,
        protocol_hash=protocol_hash,
        connection_id=connection_id,
        generation=generation,
        symbol=attempt.symbol,
        clock=clock,
        observation=observation,
        source_logical_key=public_depth_rest_source_logical_key_v8(attempt.symbol),
        cancellation_requested=cancellation_requested,
    )


def _depth_error_category(
    outcome: _RestTransportOutcome,
) -> PublicDepthRestErrorCategoryV8:
    match outcome:
        case _RestTransportOutcome.NETWORK:
            return PublicDepthRestErrorCategoryV8.NETWORK
        case _RestTransportOutcome.TIMEOUT:
            return PublicDepthRestErrorCategoryV8.TIMEOUT
        case _RestTransportOutcome.PROTOCOL:
            return PublicDepthRestErrorCategoryV8.PROTOCOL
        case _RestTransportOutcome.RESPONSE_READ:
            return PublicDepthRestErrorCategoryV8.RESPONSE_READ
        case _RestTransportOutcome.RESPONSE_CLOSE:
            return PublicDepthRestErrorCategoryV8.RESPONSE_CLOSE
        case _RestTransportOutcome.HTTP_STATUS:
            return PublicDepthRestErrorCategoryV8.HTTP_STATUS
        case _RestTransportOutcome.BODY_LIMIT:
            return PublicDepthRestErrorCategoryV8.BODY_LIMIT
        case _RestTransportOutcome.CANCELLED:
            return PublicDepthRestErrorCategoryV8.CANCELLED
    assert_never(outcome)


def _validated_depth_request_material(
    attempt: _DepthRestAttempt,
    request_proof: _RestRequestProof,
) -> tuple[
    str,
    str,
    str,
    tuple[tuple[str, str], ...],
    tuple[tuple[str, str], ...],
]:
    plan = attempt.token.plan
    url = httpx.URL(request_proof.url)
    expected_query = (*plan.fixed_query, ("symbol", attempt.symbol))
    expected_url = httpx.URL(
        f"{plan.base_url}{plan.endpoint}",
        params=expected_query,
    )
    expected_headers = tuple(
        sorted(
            (
                ("host", expected_url.host),
                ("connection", "keep-alive"),
                *plan.request_headers,
            )
        )
    )
    actual_query = tuple(url.params.multi_items())
    if (
        request_proof.method != plan.method
        or url.scheme != expected_url.scheme
        or url.username
        or url.password
        or url.host != expected_url.host
        or url.port != expected_url.port
        or url.path != plan.endpoint
        or url.raw_path != expected_url.raw_path
        or actual_query != expected_query
        or url.query != expected_url.query
        or url.fragment
        or request_proof.headers != expected_headers
        or request_proof.body != b""
    ):
        raise RestCaptureOwnershipFailureV2(
            "actual depth REST request differs from its exact public/no-key plan proof"
        )
    return (
        request_proof.method,
        f"{url.scheme}://{url.host}",
        url.path,
        actual_query,
        request_proof.headers,
    )
