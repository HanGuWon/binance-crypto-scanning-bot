from __future__ import annotations

import asyncio
import hashlib
import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, cast

from signalbot.capture.models import ConnectionState
from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityEventV2,
    CaptureIntegrityLedgerV2,
    SourceGapCauseV2,
    SourceGapLeftBoundaryV2,
)
from signalbot.r4b_v2.capture.models import RawRecordV2, TransportV2
from signalbot.r4b_v2.capture.pipeline import CaptureFinalityFenceReceiptV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingPlanV8,
    ProvisionalPromotingRestCapturePlanV2,
    validate_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.websocket_finality import (
    WebSocketRouteStopReceiptV2,
    WebSocketRouteStopReceiptV8,
    _issue_websocket_route_stop_receipt_v2,
    _issue_websocket_route_stop_receipt_v8,
)

_OPEN_EVIDENCE_DOMAIN = b"R4B_V2_LIVE_WEBSOCKET_SOURCE_GAP_OPEN\0"
_BOUNDED_EVIDENCE_DOMAIN = b"R4B_V2_LIVE_WEBSOCKET_SOURCE_GAP_BOUNDED\0"
_MAX_IDENTITY_LENGTH = 256


class WebSocketSourceGapLifecycleError(RuntimeError):
    """Raised when owner transitions cannot preserve the sealed gap contract."""


class _FatalStateV2(Protocol):
    @property
    def stop_event(self) -> asyncio.Event: ...

    @property
    def failed(self) -> bool: ...

    def raise_if_failed(self) -> None: ...


class _FatalHandoffV2(Protocol):
    @property
    def accepting(self) -> bool: ...

    @property
    def fatal_state(self) -> _FatalStateV2: ...

    def fail_consumer(
        self,
        cause: BaseException,
        *,
        failing_ingest_seq: int | None,
    ) -> None: ...


class _FinalityPipelineV2(Protocol):
    @property
    def handoff(self) -> _FatalHandoffV2: ...

    async def finalize_through(
        self,
        requested_ingest_seq: int,
        *,
        timeout_seconds: float,
    ) -> CaptureFinalityFenceReceiptV2: ...


@dataclass(frozen=True, slots=True)
class _RetainedCursorV2:
    connection_id: str
    generation: int
    frame_seq: int
    ingest_seq: int
    wall_ms: int
    monotonic_ns: int

    @classmethod
    def from_record(cls, record: RawRecordV2) -> _RetainedCursorV2:
        if record.connection_id is None or record.generation is None or record.frame_seq is None:
            raise WebSocketSourceGapLifecycleError(
                "retained WebSocket record requires a complete source cursor"
            )
        return cls(
            connection_id=record.connection_id,
            generation=record.generation,
            frame_seq=record.frame_seq,
            ingest_seq=record.ingest_seq,
            wall_ms=record.receipt_wall_ms,
            monotonic_ns=record.receipt_monotonic_ns,
        )


class WebSocketLifecycleFatalCoordinatorV2:
    """Route-bound SOURCE_GAP state machine using one shared V2 fatal domain.

    ``record_transition`` is synchronous because the existing socket owner calls
    it immediately before attempting a connection. Consequently an OPEN ledger
    append and its fsync finish before the connector can observe the attempt.
    The adapter calls ``complete_recovery_successor`` while holding the shared
    WebSocket ingress gate.
    """

    def __init__(
        self,
        promoting_plans: Sequence[ProvisionalPromotingPlanV2],
        plan: ProvisionalPromotingCapturePlanV2,
        *,
        session_id: str,
        process_boot_id: str,
        session_started_at: ReceiptTimestamp,
        source_component: str,
        clock: ReceiptClock,
        pipeline: _FinalityPipelineV2,
        integrity_ledger: CaptureIntegrityLedgerV2,
        finality_timeout_seconds: float,
    ) -> None:
        if not isinstance(plan, ProvisionalPromotingCapturePlanV2):
            raise TypeError("V2 WebSocket lifecycle requires a promoting WebSocket plan")
        if sum(candidate == plan for candidate in promoting_plans) != 1:
            raise ValueError("selected WebSocket plan must occur exactly once in the plan bundle")
        _validate_identity(session_id, "session_id")
        _validate_identity(process_boot_id, "process_boot_id")
        _validate_identity(source_component, "source_component")
        _validate_timestamp(session_started_at, "session_started_at")
        if (
            isinstance(finality_timeout_seconds, bool)
            or not isinstance(finality_timeout_seconds, (int, float))
            or not math.isfinite(float(finality_timeout_seconds))
            or finality_timeout_seconds <= 0
        ):
            raise ValueError("finality_timeout_seconds must be finite and positive")

        self.promoting_plans = tuple(promoting_plans)
        self.plan = plan
        self.session_id = session_id
        self.process_boot_id = process_boot_id
        self.session_started_at = session_started_at
        self.source_component = source_component
        self.clock = clock
        self.pipeline = pipeline
        self.integrity_ledger = integrity_ledger
        self.finality_timeout_seconds = float(finality_timeout_seconds)
        self._pending_open: CaptureIntegrityEventV2 | None = None
        self._retained_cursor: _RetainedCursorV2 | None = None
        self._connection_id: str | None = None
        self._generation = 0
        self._normal_stop_receipt: WebSocketRouteStopReceiptV2 | None = None

    @property
    def stop_event(self) -> asyncio.Event:
        return self.pipeline.handoff.fatal_state.stop_event

    @property
    def failed(self) -> bool:
        return self.pipeline.handoff.fatal_state.failed

    @property
    def accepting(self) -> bool:
        return self.pipeline.handoff.accepting

    @property
    def pending_source_gap(self) -> bool:
        return self._pending_open is not None

    @property
    def normal_stop_receipt(self) -> WebSocketRouteStopReceiptV2 | None:
        """Return the sole receipt issued at an exact retained ``owner_stop``."""

        return self._normal_stop_receipt

    def record_transition(
        self,
        connection_id: str,
        *,
        generation: int,
        last_frame_seq: int,
        state: ConnectionState,
        reason: str,
    ) -> None:
        """Synchronously retain a lifecycle boundary or fail the shared capture."""

        try:
            self.pipeline.handoff.fatal_state.raise_if_failed()
            self._record_transition(
                connection_id,
                generation=generation,
                last_frame_seq=last_frame_seq,
                state=state,
                reason=reason,
            )
        except BaseException as exc:
            self._trip_fatal(exc, failing_ingest_seq=None)
            raise

    def trip_fatal(self, cause: BaseException) -> None:
        self._trip_fatal(cause, failing_ingest_seq=None)

    async def complete_recovery_successor(self, record: RawRecordV2) -> None:
        """Finalize and BOUND frame 1 before the shared ingress gate is released."""

        try:
            self._validate_successor(record)
            open_event = self._pending_open
            assert open_event is not None
            receipt = await self.pipeline.finalize_through(
                record.ingest_seq,
                timeout_seconds=self.finality_timeout_seconds,
            )
            if (
                receipt.requested_ingest_seq != record.ingest_seq
                or receipt.fence_ingest_seq != record.ingest_seq
            ):
                raise WebSocketSourceGapLifecycleError(
                    "finality receipt differs from the recovery successor"
                )
            bounded = self.integrity_ledger.append_source_gap_bounded(
                open_event,
                right_ingest_seq=record.ingest_seq,
                evidence_sha256=_bounded_evidence_sha256(
                    open_event=open_event,
                    record=record,
                    receipt=receipt,
                    source_component=self.source_component,
                ),
            )
            self.integrity_ledger.assert_source_gap_bounded_current_v2(bounded)
            self._retained_cursor = _RetainedCursorV2.from_record(record)
            self._pending_open = None
        except asyncio.CancelledError:
            # The owner reports owner_stop/owner_cancelled after cancelling this
            # task. The offered frame 1 remains unclaimed and the durable OPEN
            # remains unmatched; neither fact fabricates BOUNDED evidence.
            raise
        except BaseException as exc:
            self._trip_fatal(exc, failing_ingest_seq=record.ingest_seq)
            raise

    def record_retained_frame(self, record: RawRecordV2) -> None:
        """Advance the exact route cursor after a non-recovery offer succeeds."""

        try:
            self._validate_record_scope(record)
            if self._pending_open is not None:
                raise WebSocketSourceGapLifecycleError(
                    "a pending SOURCE_GAP accepts only recovery successor frame 1"
                )
            previous = self._retained_cursor
            if previous is None:
                raise WebSocketSourceGapLifecycleError(
                    "non-recovery frame arrived before a bounded successor"
                )
            if (
                record.connection_id != previous.connection_id
                or record.generation != previous.generation
                or record.frame_seq != previous.frame_seq + 1
                or record.ingest_seq <= previous.ingest_seq
            ):
                raise WebSocketSourceGapLifecycleError(
                    "retained WebSocket cursor does not advance contiguously"
                )
            self._retained_cursor = _RetainedCursorV2.from_record(record)
        except BaseException as exc:
            self._trip_fatal(exc, failing_ingest_seq=record.ingest_seq)
            raise

    def _record_transition(
        self,
        connection_id: str,
        *,
        generation: int,
        last_frame_seq: int,
        state: ConnectionState,
        reason: str,
    ) -> None:
        _validate_identity(connection_id, "connection_id")
        if type(generation) is not int or generation < 1:
            raise WebSocketSourceGapLifecycleError("generation must be positive")
        if type(last_frame_seq) is not int or last_frame_seq < 0:
            raise WebSocketSourceGapLifecycleError("last_frame_seq must be nonnegative")
        if not isinstance(state, ConnectionState):
            raise WebSocketSourceGapLifecycleError("state must be a ConnectionState")

        if state is ConnectionState.CONNECTING:
            self._record_connecting(
                connection_id,
                generation=generation,
                last_frame_seq=last_frame_seq,
                reason=reason,
            )
            return
        self._validate_current_connection(connection_id, generation)
        if state is ConnectionState.CONNECTED:
            if reason != "public_session_open" or last_frame_seq != 0:
                raise WebSocketSourceGapLifecycleError("invalid CONNECTED transition")
            return
        if state is ConnectionState.RECYCLED:
            if reason != "proactive_lifetime_rotation":
                raise WebSocketSourceGapLifecycleError("invalid RECYCLED transition")
            self._record_reconnect_boundary(
                last_frame_seq,
                SourceGapCauseV2.PROACTIVE_RECYCLE,
            )
            return
        if state is not ConnectionState.DISCONNECTED:
            raise WebSocketSourceGapLifecycleError("unsupported WebSocket transition")
        if reason == "owner_stop":
            self._validate_operator_stop_tail(last_frame_seq)
            self._issue_normal_stop_receipt(last_frame_seq)
            return
        if reason == "owner_cancelled":
            self._validate_operator_stop_tail(last_frame_seq)
            return
        if reason not in {"remote_stream_ended", "connection_failure"}:
            raise WebSocketSourceGapLifecycleError("invalid DISCONNECTED transition")
        self._record_reconnect_boundary(
            last_frame_seq,
            SourceGapCauseV2.WEBSOCKET_DISCONNECT,
        )

    def _record_connecting(
        self,
        connection_id: str,
        *,
        generation: int,
        last_frame_seq: int,
        reason: str,
    ) -> None:
        if reason != "connect_attempt" or last_frame_seq != 0:
            raise WebSocketSourceGapLifecycleError("invalid CONNECTING transition")
        if generation != self._generation + 1 or connection_id == self._connection_id:
            raise WebSocketSourceGapLifecycleError(
                "WebSocket reconnect generation must advance by exactly one"
            )
        if self._pending_open is None:
            if self._retained_cursor is not None:
                raise WebSocketSourceGapLifecycleError(
                    "reconnect attempted before a retained-frame SOURCE_GAP was opened"
                )
            detected = self.clock.capture()
            _validate_timestamp(detected, "SOURCE_GAP detection")
            self._pending_open = self._append_open(
                cause=SourceGapCauseV2.SESSION_START_PENDING,
                boundary=SourceGapLeftBoundaryV2.SESSION_START,
                detected=detected,
                connection_id=connection_id,
                generation=generation,
            )
        self._connection_id = connection_id
        self._generation = generation

    def _record_reconnect_boundary(
        self,
        last_frame_seq: int,
        cause: SourceGapCauseV2,
    ) -> None:
        if self._pending_open is not None:
            if last_frame_seq != 0:
                raise WebSocketSourceGapLifecycleError(
                    "unbounded recovery cannot report a retained successor"
                )
            return
        self._validate_reported_tail(last_frame_seq)
        retained = self._retained_cursor
        assert retained is not None
        detected = self.clock.capture()
        _validate_timestamp(detected, "SOURCE_GAP detection")
        self._pending_open = self._append_open(
            cause=cause,
            boundary=SourceGapLeftBoundaryV2.RETAINED_FRAME,
            detected=detected,
            connection_id=self._connection_id,
            generation=self._generation,
        )

    def _append_open(
        self,
        *,
        cause: SourceGapCauseV2,
        boundary: SourceGapLeftBoundaryV2,
        detected: ReceiptTimestamp,
        connection_id: str | None,
        generation: int,
    ) -> CaptureIntegrityEventV2:
        retained = self._retained_cursor
        if boundary is SourceGapLeftBoundaryV2.SESSION_START:
            left = self.session_started_at
            left_connection_id = None
            left_generation = None
            left_frame_seq = None
            left_ingest_seq = None
        else:
            assert retained is not None
            left = ReceiptTimestamp(retained.wall_ms, retained.monotonic_ns)
            left_connection_id = retained.connection_id
            left_generation = retained.generation
            left_frame_seq = retained.frame_seq
            left_ingest_seq = retained.ingest_seq
        if detected.received_monotonic_ns < left.received_monotonic_ns:
            raise WebSocketSourceGapLifecycleError(
                "SOURCE_GAP detection precedes its left boundary"
            )
        evidence = {
            "schema_version": "r4b_v2_live_websocket_source_gap_open_evidence_v1",
            "session_id": self.session_id,
            "process_boot_id": self.process_boot_id,
            "plan_id": self.plan.name,
            "route_id": self.plan.route_id,
            "cause": cause.value,
            "left_boundary_kind": boundary.value,
            "left_connection_id": left_connection_id,
            "left_generation": left_generation,
            "left_frame_seq": left_frame_seq,
            "left_ingest_seq": left_ingest_seq,
            "left_wall_ms": left.received_at_ms,
            "left_monotonic_ns": left.received_monotonic_ns,
            "detected_wall_ms": detected.received_at_ms,
            "detected_monotonic_ns": detected.received_monotonic_ns,
            "attempt_connection_id": connection_id,
            "attempt_generation": generation,
            "source_component": self.source_component,
        }
        return self._append_source_gap_open_authorized(
            cause=cause,
            boundary=boundary,
            left_connection_id=left_connection_id,
            left_generation=left_generation,
            left_frame_seq=left_frame_seq,
            left_ingest_seq=left_ingest_seq,
            left_wall_ms=left.received_at_ms,
            left_monotonic_ns=left.received_monotonic_ns,
            detected_wall_ms=detected.received_at_ms,
            detected_monotonic_ns=detected.received_monotonic_ns,
            evidence_sha256=_evidence_sha256(_OPEN_EVIDENCE_DOMAIN, evidence),
        )

    def _append_source_gap_open_authorized(
        self,
        *,
        cause: SourceGapCauseV2,
        boundary: SourceGapLeftBoundaryV2,
        left_connection_id: str | None,
        left_generation: int | None,
        left_frame_seq: int | None,
        left_ingest_seq: int | None,
        left_wall_ms: int,
        left_monotonic_ns: int,
        detected_wall_ms: int,
        detected_monotonic_ns: int,
        evidence_sha256: str,
    ) -> CaptureIntegrityEventV2:
        return self.integrity_ledger.append_source_gap_open(
            self.promoting_plans,
            self.plan,
            session_id=self.session_id,
            process_boot_id=self.process_boot_id,
            cause=cause,
            left_boundary_kind=boundary,
            left_connection_id=left_connection_id,
            left_generation=left_generation,
            left_frame_seq=left_frame_seq,
            left_ingest_seq=left_ingest_seq,
            left_wall_ms=left_wall_ms,
            left_monotonic_ns=left_monotonic_ns,
            detected_wall_ms=detected_wall_ms,
            detected_monotonic_ns=detected_monotonic_ns,
            source_component=self.source_component,
            evidence_sha256=evidence_sha256,
        )

    def _validate_successor(self, record: RawRecordV2) -> None:
        self._validate_record_scope(record)
        if self._pending_open is None:
            raise WebSocketSourceGapLifecycleError(
                "recovery successor has no durable unmatched SOURCE_GAP OPEN"
            )
        if record.frame_seq != 1:
            raise WebSocketSourceGapLifecycleError(
                "SOURCE_GAP recovery successor must be frame 1"
            )

    def _validate_record_scope(self, record: RawRecordV2) -> None:
        if not isinstance(record, RawRecordV2):
            raise WebSocketSourceGapLifecycleError("retained frame must be RawRecordV2")
        if (
            record.transport is not TransportV2.WEBSOCKET
            or record.session_id != self.session_id
            or record.plan_id != self.plan.name
            or record.venue is not self.plan.venue
            or record.route_id != self.plan.route_id
            or record.symbol is not None
            or record.connection_id != self._connection_id
            or record.generation != self._generation
        ):
            raise WebSocketSourceGapLifecycleError(
                "retained WebSocket record differs from the active route scope"
            )

    def _validate_current_connection(self, connection_id: str, generation: int) -> None:
        if connection_id != self._connection_id or generation != self._generation:
            raise WebSocketSourceGapLifecycleError(
                "WebSocket transition differs from the active connection generation"
            )

    def _validate_reported_tail(self, last_frame_seq: int) -> None:
        if self._pending_open is not None:
            if last_frame_seq != 0:
                raise WebSocketSourceGapLifecycleError(
                    "pending recovery transition must report zero retained frames"
                )
            return
        retained = self._retained_cursor
        if (
            retained is None
            or retained.connection_id != self._connection_id
            or retained.generation != self._generation
            or retained.frame_seq != last_frame_seq
        ):
            raise WebSocketSourceGapLifecycleError(
                "owner transition differs from the last retained frame"
            )

    def _validate_operator_stop_tail(self, last_frame_seq: int) -> None:
        if self._pending_open is not None:
            # Frame 1 can already have crossed the synchronous offer seam when
            # cancellation interrupts its finality wait. It is not a retained
            # recovery cursor until BOUNDED commits, so the adapter publishes 0.
            if last_frame_seq != 0:
                raise WebSocketSourceGapLifecycleError(
                    "pending recovery stop cannot publish an unbounded successor"
                )
            return
        self._validate_reported_tail(last_frame_seq)

    def _issue_normal_stop_receipt(self, last_frame_seq: int) -> None:
        """Issue only for a retained cursor with no unmatched source gap."""

        if self._pending_open is not None:
            return
        retained = self._retained_cursor
        if retained is None:
            return
        if self._normal_stop_receipt is not None:
            raise WebSocketSourceGapLifecycleError(
                "WebSocket route normal-stop receipt is write-once"
            )
        if retained.frame_seq != last_frame_seq:
            raise WebSocketSourceGapLifecycleError(
                "owner-stop receipt differs from the retained route cursor"
            )
        observed = self.clock.capture()
        _validate_timestamp(observed, "WebSocket owner-stop observation")
        if observed.received_monotonic_ns < retained.monotonic_ns:
            raise WebSocketSourceGapLifecycleError(
                "owner-stop observation precedes the retained route cursor"
            )
        self._normal_stop_receipt = _issue_websocket_route_stop_receipt_v2(
            self.promoting_plans,
            self.plan,
            session_id=self.session_id,
            process_boot_id=self.process_boot_id,
            connection_id=retained.connection_id,
            generation=retained.generation,
            last_frame_seq=retained.frame_seq,
            last_ingest_seq=retained.ingest_seq,
            last_receipt_wall_ms=retained.wall_ms,
            last_receipt_monotonic_ns=retained.monotonic_ns,
            stop_observed=observed,
        )

    def _trip_fatal(
        self,
        cause: BaseException,
        *,
        failing_ingest_seq: int | None,
    ) -> None:
        if not isinstance(cause, BaseException):
            raise TypeError("fatal cause must be an exception")
        self.pipeline.handoff.fail_consumer(
            cause,
            failing_ingest_seq=failing_ingest_seq,
        )


class WebSocketLifecycleFatalCoordinatorV8(WebSocketLifecycleFatalCoordinatorV2):
    """V8 route lifecycle bound to the exact four-plan authority object bundle."""

    def __init__(
        self,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        plan: ProvisionalPromotingCapturePlanV2,
        *,
        session_id: str,
        process_boot_id: str,
        session_started_at: ReceiptTimestamp,
        source_component: str,
        clock: ReceiptClock,
        pipeline: _FinalityPipelineV2,
        integrity_ledger: CaptureIntegrityLedgerV2,
        finality_timeout_seconds: float,
    ) -> None:
        if type(promoting_plans) is not tuple:
            raise TypeError("V8 WebSocket lifecycle requires an exact plan tuple")
        validate_provisional_promoting_capture_plans_v8(promoting_plans)
        if type(plan) is not ProvisionalPromotingCapturePlanV2:
            raise TypeError("V8 WebSocket lifecycle requires an exact WebSocket plan")
        if sum(candidate is plan for candidate in promoting_plans) != 1:
            raise ValueError("selected V8 WebSocket plan must be its authority object")
        promoting_plans_v2 = tuple(
            cast(ProvisionalPromotingPlanV2, candidate)
            for candidate in promoting_plans
            if type(candidate)
            in (
                ProvisionalPromotingCapturePlanV2,
                ProvisionalPromotingRestCapturePlanV2,
            )
        )
        super().__init__(
            promoting_plans_v2,
            plan,
            session_id=session_id,
            process_boot_id=process_boot_id,
            session_started_at=session_started_at,
            source_component=source_component,
            clock=clock,
            pipeline=pipeline,
            integrity_ledger=integrity_ledger,
            finality_timeout_seconds=finality_timeout_seconds,
        )
        self.promoting_plans_v8 = promoting_plans
        self._normal_stop_receipt_v8: WebSocketRouteStopReceiptV8 | None = None

    @property
    def normal_stop_receipt_v8(self) -> WebSocketRouteStopReceiptV8 | None:
        """Return the sole full-authority receipt issued at exact OWNER_STOP."""

        return self._normal_stop_receipt_v8

    def _append_source_gap_open_authorized(
        self,
        *,
        cause: SourceGapCauseV2,
        boundary: SourceGapLeftBoundaryV2,
        left_connection_id: str | None,
        left_generation: int | None,
        left_frame_seq: int | None,
        left_ingest_seq: int | None,
        left_wall_ms: int,
        left_monotonic_ns: int,
        detected_wall_ms: int,
        detected_monotonic_ns: int,
        evidence_sha256: str,
    ) -> CaptureIntegrityEventV2:
        return self.integrity_ledger.append_source_gap_open_v8(
            self.promoting_plans_v8,
            self.plan,
            session_id=self.session_id,
            process_boot_id=self.process_boot_id,
            cause=cause,
            left_boundary_kind=boundary,
            left_connection_id=left_connection_id,
            left_generation=left_generation,
            left_frame_seq=left_frame_seq,
            left_ingest_seq=left_ingest_seq,
            left_wall_ms=left_wall_ms,
            left_monotonic_ns=left_monotonic_ns,
            detected_wall_ms=detected_wall_ms,
            detected_monotonic_ns=detected_monotonic_ns,
            source_component=self.source_component,
            evidence_sha256=evidence_sha256,
        )

    def _issue_normal_stop_receipt(self, last_frame_seq: int) -> None:
        if self._pending_open is not None:
            return
        retained = self._retained_cursor
        if retained is None:
            return
        if self._normal_stop_receipt_v8 is not None:
            raise WebSocketSourceGapLifecycleError(
                "V8 WebSocket route normal-stop receipt is write-once"
            )
        if retained.frame_seq != last_frame_seq:
            raise WebSocketSourceGapLifecycleError(
                "V8 owner-stop receipt differs from the retained route cursor"
            )
        observed = self.clock.capture()
        _validate_timestamp(observed, "V8 WebSocket owner-stop observation")
        if observed.received_monotonic_ns < retained.monotonic_ns:
            raise WebSocketSourceGapLifecycleError(
                "V8 owner-stop observation precedes the retained route cursor"
            )
        self._normal_stop_receipt_v8 = _issue_websocket_route_stop_receipt_v8(
            self.promoting_plans_v8,
            self.plan,
            session_id=self.session_id,
            process_boot_id=self.process_boot_id,
            connection_id=retained.connection_id,
            generation=retained.generation,
            last_frame_seq=retained.frame_seq,
            last_ingest_seq=retained.ingest_seq,
            last_receipt_wall_ms=retained.wall_ms,
            last_receipt_monotonic_ns=retained.monotonic_ns,
            stop_observed=observed,
        )


def _bounded_evidence_sha256(
    *,
    open_event: CaptureIntegrityEventV2,
    record: RawRecordV2,
    receipt: CaptureFinalityFenceReceiptV2,
    source_component: str,
) -> str:
    return _evidence_sha256(
        _BOUNDED_EVIDENCE_DOMAIN,
        {
            "schema_version": "r4b_v2_live_websocket_source_gap_bounded_evidence_v1",
            "open_event_sha256": open_event.sha256,
            "right_connection_id": record.connection_id,
            "right_generation": record.generation,
            "right_frame_seq": record.frame_seq,
            "right_ingest_seq": record.ingest_seq,
            "right_wall_ms": record.receipt_wall_ms,
            "right_monotonic_ns": record.receipt_monotonic_ns,
            "finality_receipt_sha256": receipt.sha256,
            "finality_prefix_proof_sha256": receipt.prefix_proof_sha256,
            "source_component": source_component,
        },
    )


def _evidence_sha256(domain: bytes, document: dict[str, object]) -> str:
    return hashlib.sha256(domain + canonical_json_line(document)).hexdigest()


def _validate_identity(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a bounded normalized identity")


def _validate_timestamp(value: ReceiptTimestamp, field: str) -> None:
    if (
        not isinstance(value, ReceiptTimestamp)
        or type(value.received_at_ms) is not int
        or type(value.received_monotonic_ns) is not int
        or value.received_at_ms < 0
        or value.received_monotonic_ns < 0
    ):
        raise ValueError(f"{field} must be a nonnegative receipt timestamp")
