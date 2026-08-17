"""Exclusive top-level owner for one prospective R4B V2 public capture run.

This boundary composes existing owners only.  It creates no connector, ingress,
sequencer, handoff, writer, pipeline, ledger, or session-closure authority.
"""

from __future__ import annotations

import asyncio
import math
import re
from dataclasses import dataclass
from typing import cast

from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    CaptureFinalityFenceReceiptV2,
    DurableCaptureBatchWriterV2,
    verify_clean_stopped_current_tail_v2,
)
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV8,
    ProvisionalPromotingPlanV9,
    ProvisionalPromotingRestCapturePlanV2,
    ProvisionalUsdmVenueClockRestCapturePlanV9,
    provisional_promoting_plan_sha256_v2,
    provisional_promoting_plan_sha256_v8,
    provisional_promoting_plan_sha256_v9,
    validate_provisional_promoting_capture_plans_v2,
    validate_provisional_promoting_capture_plans_v8,
    validate_provisional_promoting_capture_plans_v9,
)
from signalbot.r4b_v2.capture.rest_adapter import (
    PipelineRestCaptureFatalCoordinatorV2,
    PublicOpenInterestRestCaptureAdapterV2,
)
from signalbot.r4b_v2.capture.rest_census import (
    PUBLIC_OI_REST_POLL_INTERVAL_MS_V2,
)
from signalbot.r4b_v2.capture.rest_clock_adapter import (
    PublicUsdmVenueClockRestCaptureAdapterV9,
)
from signalbot.r4b_v2.capture.rest_clock_scheduler import (
    PublicUsdmVenueClockRestSchedulerV9,
    PublicUsdmVenueClockSchedulerResultV9,
    validate_public_usdm_venue_clock_schedule_authority_v9,
)
from signalbot.r4b_v2.capture.rest_depth_bridge import (
    PublicDepthRestBridgeCoordinatorV8,
)
from signalbot.r4b_v2.capture.rest_depth_bridge_evidence import (
    DepthBridgeCoordinatorCleanCloseReceiptV8,
    validate_depth_bridge_coordinator_clean_close_receipt_v8,
)
from signalbot.r4b_v2.capture.rest_scheduler import (
    PublicOiRestCensusContextV2,
    PublicOpenInterestRestSchedulerV2,
    validate_public_oi_schedule_authority_v2,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicOiCensusAdmissionReceiptV2,
    SharedWebSocketIngressV2,
    validate_public_oi_census_admission_receipt_v2,
)
from signalbot.r4b_v2.capture.websocket_composition import (
    PublicWebSocketOwnerCompositionV2,
    PublicWebSocketOwnerCompositionV8,
    PublicWebSocketRuntimeRunTokenV2,
    PublicWebSocketRuntimeRunTokenV8,
    create_public_websocket_runtime_start_barrier_v8,
)
from signalbot.r4b_v2.capture.websocket_finality import (
    FinalizedWebSocketRouteCursorPairV2,
    FinalizedWebSocketRouteCursorPairV8,
    WebSocketRouteStopReceiptV2,
    WebSocketRouteStopReceiptV8,
    finalize_websocket_route_cursor_pair_v2,
    finalize_websocket_route_cursor_pair_v8,
    validate_finalized_websocket_route_cursor_pair_v2,
    validate_finalized_websocket_route_cursor_pair_v8,
    validate_websocket_route_stop_receipt_v2,
    validate_websocket_route_stop_receipt_v8,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EXPECTED_WEBSOCKET_ROUTES = ("usdm_market", "usdm_public")
_PRODUCER_TASK_COUNT = 3
_MAX_RUNTIME_TIMEOUT_SECONDS = 300.0
_MIN_ABNORMAL_CLEANUP_SECONDS = 1.0


class PublicCaptureRuntimeBindingErrorV2(RuntimeError):
    """The three existing producers do not share one exact capture authority."""


class PublicCaptureRuntimeStateErrorV2(RuntimeError):
    """The exclusive runtime was replayed or stopped from an invalid state."""


class PublicCaptureRuntimeShutdownErrorV2(RuntimeError):
    """A producer or owned cleanup task exceeded a bounded runtime transition."""


@dataclass(frozen=True, slots=True)
class PublicCaptureRuntimeResultV2:
    """Verified local finality result without any completeness or M2 claim."""

    normal_stop_receipt: ReceiptTimestamp
    oi_coverage_close_receipt: PublicOiCensusAdmissionReceiptV2
    websocket_route_cursors: FinalizedWebSocketRouteCursorPairV2
    finality_receipt: CaptureFinalityFenceReceiptV2
    verified_prefix_proof_sha256: str
    producer_task_count: int = _PRODUCER_TASK_COUNT
    adapter_cleanly_closed: bool = True
    oi_coverage_closed: bool = True
    websocket_local_route_cursors_finalized: bool = True
    pending_source_gap: bool = False
    fatal_state_failed: bool = False
    oi_data_completeness_claimed: bool = False
    websocket_retained_frame_parser_health_claimed: bool = False
    websocket_upstream_message_completeness_claimed: bool = False
    observed_source_completeness_claimed: bool = False
    m2_eligible: bool = False
    local_session_closure_issued: bool = False
    integrity_ledger_clean_issued: bool = False

    def __post_init__(self) -> None:
        if type(self.normal_stop_receipt) is not ReceiptTimestamp:
            raise TypeError("normal_stop_receipt must be an exact ReceiptTimestamp")
        if type(self.oi_coverage_close_receipt) is not PublicOiCensusAdmissionReceiptV2:
            raise TypeError("oi_coverage_close_receipt must be an exact census admission receipt")
        if type(self.finality_receipt) is not CaptureFinalityFenceReceiptV2:
            raise TypeError("finality_receipt must be an exact finality receipt")
        validate_finalized_websocket_route_cursor_pair_v2(
            self.websocket_route_cursors,
            finality_receipt=self.finality_receipt,
        )
        if (
            type(self.verified_prefix_proof_sha256) is not str
            or _SHA256_RE.fullmatch(self.verified_prefix_proof_sha256) is None
        ):
            raise ValueError("verified prefix proof must be a lowercase SHA-256 digest")
        if self.verified_prefix_proof_sha256 != self.finality_receipt.prefix_proof_sha256:
            raise ValueError("verified prefix proof differs from the finality receipt")
        if self.producer_task_count != _PRODUCER_TASK_COUNT:
            raise ValueError("runtime result must bind exactly three producer tasks")
        for field_name in (
            "adapter_cleanly_closed",
            "oi_coverage_closed",
            "websocket_local_route_cursors_finalized",
        ):
            if getattr(self, field_name) is not True:
                raise ValueError(f"{field_name} must be true for a normal result")
        for field_name in (
            "pending_source_gap",
            "fatal_state_failed",
            "oi_data_completeness_claimed",
            "websocket_retained_frame_parser_health_claimed",
            "websocket_upstream_message_completeness_claimed",
            "observed_source_completeness_claimed",
            "m2_eligible",
            "local_session_closure_issued",
            "integrity_ledger_clean_issued",
        ):
            if getattr(self, field_name) is not False:
                raise ValueError(f"{field_name} must remain explicitly false")


class PublicCaptureRuntimeV2:
    """Own exactly two existing WS compositions and one existing OI scheduler.

    Construction claims both WebSocket compositions.  ``run`` creates exactly
    three producer tasks after a synchronous, current-state validation.  A
    normal stop first commits the scheduler stop receipt, then signals the one
    shared WebSocket stop event.  Every abnormal path skips clean-tail finality.
    """

    def __init__(
        self,
        websocket_compositions: tuple[
            PublicWebSocketOwnerCompositionV2,
            PublicWebSocketOwnerCompositionV2,
        ],
        rest_adapter: PublicOpenInterestRestCaptureAdapterV2,
        rest_scheduler: PublicOpenInterestRestSchedulerV2,
        *,
        producer_shutdown_timeout_seconds: float,
        finality_timeout_seconds: float,
    ) -> None:
        _validate_timeout(
            producer_shutdown_timeout_seconds,
            "producer_shutdown_timeout_seconds",
        )
        _validate_timeout(finality_timeout_seconds, "finality_timeout_seconds")
        _validate_unclaimed_runtime_bindings(
            websocket_compositions=websocket_compositions,
            rest_adapter=rest_adapter,
            rest_scheduler=rest_scheduler,
        )
        self.websocket_compositions = websocket_compositions
        self.rest_adapter = rest_adapter
        self.rest_scheduler = rest_scheduler
        self.producer_shutdown_timeout_seconds = float(producer_shutdown_timeout_seconds)
        self.finality_timeout_seconds = float(finality_timeout_seconds)
        self.pipeline = cast(
            CaptureBatchPipelineV2,
            websocket_compositions[0].lifecycle_coordinator.pipeline,
        )
        self.integrity_ledger = websocket_compositions[0].lifecycle_coordinator.integrity_ledger
        self._market_token: PublicWebSocketRuntimeRunTokenV2 | None = None
        self._public_token: PublicWebSocketRuntimeRunTokenV2 | None = None
        self._producer_tasks: tuple[asyncio.Task[object], ...] = ()
        self._normal_stop_requested_future: asyncio.Future[None] | None = None
        self._normal_stop_owner: asyncio.Task[ReceiptTimestamp] | None = None
        self._adapter_close_task: asyncio.Task[None] | None = None
        self._pipeline_stop_task: asyncio.Task[None] | None = None
        self._cleanup_task: asyncio.Task[tuple[str, ...]] | None = None
        self._started_once = False
        self._running = False
        self._terminal_failure: BaseException | None = None
        self._result: PublicCaptureRuntimeResultV2 | None = None

        market, public = websocket_compositions
        market_token = market.claim_exclusive_runtime_v2(self)
        try:
            public_token = public.claim_exclusive_runtime_v2(self)
        except BaseException:
            market._release_exclusive_runtime_claim_v2(
                market_token,
                runtime_owner=self,
            )
            raise
        self._market_token = market_token
        self._public_token = public_token
        try:
            self.validate_current()
        except BaseException:
            public._release_exclusive_runtime_claim_v2(
                public_token,
                runtime_owner=self,
            )
            market._release_exclusive_runtime_claim_v2(
                market_token,
                runtime_owner=self,
            )
            self._market_token = None
            self._public_token = None
            raise

    @property
    def started_once(self) -> bool:
        return self._started_once

    @property
    def running(self) -> bool:
        return self._running

    @property
    def producer_task_count(self) -> int:
        return len(self._producer_tasks)

    @property
    def result(self) -> PublicCaptureRuntimeResultV2 | None:
        return self._result

    def validate_current(self) -> None:
        """Revalidate every shared binding without creating an asyncio task."""

        market_token = self._market_token
        public_token = self._public_token
        if market_token is None or public_token is None:
            raise PublicCaptureRuntimeBindingErrorV2(
                "runtime lacks both exact WebSocket ownership tokens"
            )
        market, public = self.websocket_compositions
        market.validate_exclusive_runtime_claim_v2(
            market_token,
            runtime_owner=self,
        )
        public.validate_exclusive_runtime_claim_v2(
            public_token,
            runtime_owner=self,
        )
        _validate_shared_runtime_bindings(
            websocket_compositions=self.websocket_compositions,
            rest_adapter=self.rest_adapter,
            rest_scheduler=self.rest_scheduler,
            allow_requested_stop=self._normal_stop_owner is not None,
        )

    async def request_normal_stop(self) -> ReceiptTimestamp:
        """Commit OI stop once, then signal the exact shared WebSocket event."""

        if self._terminal_failure is not None and self._normal_stop_owner is None:
            raise PublicCaptureRuntimeStateErrorV2(
                "failed runtime cannot begin a new normal stop"
            ) from self._terminal_failure
        stop_owner = self._normal_stop_owner
        if stop_owner is None:
            if not self._started_once or not self._running:
                raise PublicCaptureRuntimeStateErrorV2(
                    "normal stop requires the producer trio to be running"
                )
            terminal_producers = tuple(task for task in self._producer_tasks if task.done())
            if terminal_producers:
                names = ", ".join(sorted(task.get_name() for task in terminal_producers))
                raise PublicCaptureRuntimeStateErrorV2(
                    "normal stop rejects an already-terminal producer: " + names
                )
            requested_future = self._normal_stop_requested_future
            if requested_future is None or requested_future.done():
                raise PublicCaptureRuntimeStateErrorV2(
                    "runtime lacks a live write-once normal-stop signal"
                )
            stop_owner = asyncio.create_task(
                self._request_normal_stop_owned(),
                name="r4b-v2-full-runtime-normal-stop",
            )
            self._normal_stop_owner = stop_owner
            requested_future.set_result(None)
        cancellation: asyncio.CancelledError | None = None
        while not stop_owner.done():
            try:
                await asyncio.shield(stop_owner)
            except asyncio.CancelledError as exc:
                if cancellation is None:
                    cancellation = exc
        receipt = stop_owner.result()
        if cancellation is not None:
            raise cancellation
        return receipt

    async def run(self) -> PublicCaptureRuntimeResultV2:
        """Run the exact producer trio and return only verified local finality."""

        if self._started_once:
            raise PublicCaptureRuntimeStateErrorV2("capture runtime may run only once")
        self._started_once = True
        try:
            self.validate_current()
            market, public = self.websocket_compositions
            market_token = self._require_market_token()
            public_token = self._require_public_token()
            self._normal_stop_requested_future = asyncio.get_running_loop().create_future()
            # create_task does not run a coroutine until this task yields.  All
            # validation above and all three creations therefore precede the
            # first connector or REST request seam.
            self._producer_tasks = (
                cast(
                    asyncio.Task[object],
                    asyncio.create_task(
                        market.run_exclusive_runtime_v2(
                            market_token,
                            runtime_owner=self,
                        ),
                        name="r4b-v2-producer-usdm-market",
                    ),
                ),
                cast(
                    asyncio.Task[object],
                    asyncio.create_task(
                        public.run_exclusive_runtime_v2(
                            public_token,
                            runtime_owner=self,
                        ),
                        name="r4b-v2-producer-usdm-public",
                    ),
                ),
                cast(
                    asyncio.Task[object],
                    asyncio.create_task(
                        self.rest_scheduler.run(),
                        name="r4b-v2-producer-usdm-public-rest-oi",
                    ),
                ),
            )
            if len(self._producer_tasks) != _PRODUCER_TASK_COUNT:
                raise AssertionError("full runtime did not create exactly three producers")
            self._running = True
            await self._await_producer_trio()
            result = await self._complete_normal_shutdown()
            self._result = result
            return result
        except BaseException as exc:
            self._terminal_failure = exc
            cleanup_failures = await self._await_abnormal_cleanup_owned()
            if cleanup_failures:
                exc.add_note("R4B V2 abnormal cleanup: " + "; ".join(cleanup_failures))
            raise
        finally:
            self._running = False

    async def _request_normal_stop_owned(self) -> ReceiptTimestamp:
        try:
            return await self.rest_scheduler.request_normal_stop()
        finally:
            # Scheduler request_normal_stop commits its write-once receipt even
            # when its caller is cancelled.  This event is deliberately second.
            self._shared_websocket_stop_event().set()

    async def _await_producer_trio(self) -> None:
        remaining = set(self._producer_tasks)
        stop_requested = self._normal_stop_requested_future
        if stop_requested is None:
            raise AssertionError("runtime lacks its normal-stop wakeup future")
        drain_deadline: float | None = None
        loop = asyncio.get_running_loop()
        while remaining:
            normal_stop_started = self._normal_stop_owner is not None
            if normal_stop_started and drain_deadline is None:
                drain_deadline = loop.time() + self.producer_shutdown_timeout_seconds
            timeout = None if drain_deadline is None else max(0.0, drain_deadline - loop.time())
            waiters: set[asyncio.Future[object]] = {
                cast(asyncio.Future[object], task) for task in remaining
            }
            stop_waiter = cast(asyncio.Future[object], stop_requested)
            if drain_deadline is None:
                waiters.add(stop_waiter)
            done_waiters, _pending_waiters = await asyncio.wait(
                waiters,
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            stop_woke = stop_waiter in done_waiters
            if stop_woke:
                if self._normal_stop_owner is None:
                    raise PublicCaptureRuntimeStateErrorV2(
                        "normal-stop wakeup lacks its exact owner task"
                    )
                if drain_deadline is None:
                    drain_deadline = loop.time() + self.producer_shutdown_timeout_seconds
                done_waiters.remove(stop_waiter)
            done = {task for task in remaining if task in done_waiters}
            if not done and not done_waiters and drain_deadline is not None:
                if stop_woke:
                    continue
                raise PublicCaptureRuntimeShutdownErrorV2(
                    "normal producer drain exceeded its bounded timeout"
                )
            for task in done:
                if task.cancelled():
                    raise PublicCaptureRuntimeShutdownErrorV2(
                        f"producer cancelled unexpectedly: {task.get_name()}"
                    )
                failure = task.exception()
                if failure is not None:
                    raise failure
            remaining.difference_update(done)
            if remaining and self._normal_stop_owner is None:
                names = ", ".join(sorted(task.get_name() for task in done))
                raise PublicCaptureRuntimeShutdownErrorV2(
                    f"producer exited before runtime normal stop: {names}"
                )
        if self._normal_stop_owner is None:
            raise PublicCaptureRuntimeShutdownErrorV2(
                "all producers exited without a runtime normal-stop owner"
            )

    async def _complete_normal_shutdown(self) -> PublicCaptureRuntimeResultV2:
        stop_owner = self._normal_stop_owner
        if stop_owner is None or not stop_owner.done():
            raise PublicCaptureRuntimeShutdownErrorV2(
                "producer trio ended before the normal-stop owner completed"
            )
        normal_stop_receipt = stop_owner.result()
        if (
            self.rest_scheduler.normal_stop_receipt != normal_stop_receipt
            or self.rest_scheduler.running
            or not self.rest_scheduler.drained
        ):
            raise PublicCaptureRuntimeShutdownErrorV2(
                "OI scheduler did not drain at its exact normal-stop receipt"
            )
        coverage_close = self.rest_scheduler.coverage_close_receipt
        if coverage_close is None or not self.rest_scheduler.coverage_closed:
            raise PublicCaptureRuntimeShutdownErrorV2(
                "OI scheduler did not admit its exact coverage close"
            )
        close_record = validate_public_oi_census_admission_receipt_v2(coverage_close)
        if close_record.ingest_seq != coverage_close.accepted_ingest_seq:
            raise PublicCaptureRuntimeShutdownErrorV2(
                "OI coverage-close record differs from its admission sequence"
            )
        websocket_stop_receipts = self._validated_websocket_stop_receipts()

        await self._close_adapter_bounded()
        if not self.rest_adapter.cleanly_closed:
            raise PublicCaptureRuntimeShutdownErrorV2(
                "OI REST adapter lacks a clean closed-and-drained proof"
            )
        for composition in self.websocket_compositions:
            lifecycle = composition.lifecycle_coordinator
            if lifecycle.failed:
                lifecycle.pipeline.handoff.fatal_state.raise_if_failed()
                raise PublicCaptureRuntimeShutdownErrorV2(
                    "WebSocket lifecycle failed without a visible fatal cause"
                )
            if lifecycle.pending_source_gap:
                raise PublicCaptureRuntimeShutdownErrorV2(
                    f"pending WebSocket source gap remains for {composition.plan.route_id}"
                )
        self.pipeline.handoff.fatal_state.raise_if_failed()
        if not self.pipeline.handoff.accepting:
            raise PublicCaptureRuntimeShutdownErrorV2(
                "capture handoff stopped accepting before current-tail finality"
            )
        self.integrity_ledger.assert_running_healthy_and_writer_open_v2()

        finality = await self.pipeline.finalize_current_tail_and_stop(
            timeout_seconds=self.finality_timeout_seconds,
        )
        verified_prefix = verify_clean_stopped_current_tail_v2(
            finality,
            pipeline=self.pipeline,
        )
        if finality.fence_ingest_seq < coverage_close.accepted_ingest_seq:
            raise PublicCaptureRuntimeShutdownErrorV2(
                "finalized tail precedes the admitted OI coverage close"
            )
        websocket_route_cursors = finalize_websocket_route_cursor_pair_v2(
            websocket_stop_receipts,
            finality_receipt=finality,
            promoting_plans=self.websocket_compositions[0].promoting_plans,
        )
        return PublicCaptureRuntimeResultV2(
            normal_stop_receipt=normal_stop_receipt,
            oi_coverage_close_receipt=coverage_close,
            websocket_route_cursors=websocket_route_cursors,
            finality_receipt=finality,
            verified_prefix_proof_sha256=verified_prefix,
        )

    def _validated_websocket_stop_receipts(
        self,
    ) -> tuple[WebSocketRouteStopReceiptV2, WebSocketRouteStopReceiptV2]:
        if len(self._producer_tasks) != _PRODUCER_TASK_COUNT:
            raise PublicCaptureRuntimeShutdownErrorV2(
                "runtime lacks the exact producer trio at WebSocket stop validation"
            )
        receipts: list[WebSocketRouteStopReceiptV2] = []
        for task, composition in zip(
            self._producer_tasks[:2],
            self.websocket_compositions,
            strict=True,
        ):
            if not task.done() or task.cancelled():
                raise PublicCaptureRuntimeShutdownErrorV2(
                    f"WebSocket owner is not cleanly stopped for {composition.plan.route_id}"
                )
            result = task.result()
            if type(result) is not WebSocketRouteStopReceiptV2:
                raise PublicCaptureRuntimeShutdownErrorV2(
                    f"WebSocket owner lacks an exact OWNER_STOP cursor for "
                    f"{composition.plan.route_id}"
                )
            if result is not composition.lifecycle_coordinator.normal_stop_receipt:
                raise PublicCaptureRuntimeShutdownErrorV2(
                    f"WebSocket task returned a foreign OWNER_STOP cursor for "
                    f"{composition.plan.route_id}"
                )
            validate_websocket_route_stop_receipt_v2(
                result,
                promoting_plans=composition.promoting_plans,
                plan=composition.plan,
            )
            receipts.append(result)
        if tuple(receipt.route_id for receipt in receipts) != _EXPECTED_WEBSOCKET_ROUTES:
            raise PublicCaptureRuntimeShutdownErrorV2(
                "WebSocket OWNER_STOP cursors are not in canonical route order"
            )
        return receipts[0], receipts[1]

    async def _close_adapter_bounded(self) -> None:
        task = self._adapter_close_task
        if task is None:
            task = asyncio.create_task(
                self.rest_adapter.aclose(),
                name="r4b-v2-full-runtime-rest-adapter-close",
            )
            self._adapter_close_task = task
        if not await _wait_task_bounded(
            task,
            timeout_seconds=self.producer_shutdown_timeout_seconds,
        ):
            raise PublicCaptureRuntimeShutdownErrorV2(
                "OI REST adapter close exceeded the runtime bound"
            )
        if task.cancelled():
            raise PublicCaptureRuntimeShutdownErrorV2("OI REST adapter close task was cancelled")
        task.result()

    async def _await_abnormal_cleanup_owned(self) -> tuple[str, ...]:
        cleanup = self._cleanup_task
        if cleanup is None:
            cleanup = asyncio.create_task(
                self._abnormal_cleanup_owned(),
                name="r4b-v2-full-runtime-abnormal-cleanup",
            )
            self._cleanup_task = cleanup
        cancellation: asyncio.CancelledError | None = None
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError as exc:
                if cancellation is None:
                    cancellation = exc
        if cleanup.cancelled():
            failures = ("abnormal cleanup owner was cancelled",)
        else:
            failures = cleanup.result()
        if cancellation is not None:
            failures += ("caller repeated cancellation during owned cleanup",)
        return failures

    async def _abnormal_cleanup_owned(self) -> tuple[str, ...]:
        failures: list[str] = []
        self._shared_websocket_stop_event().set()
        normal_stop_owner = self._normal_stop_owner
        if normal_stop_owner is not None and not normal_stop_owner.done():
            normal_stop_owner.cancel()

        pending_producers = tuple(task for task in self._producer_tasks if not task.done())
        for task in pending_producers:
            task.cancel()
        if pending_producers:
            done, pending = await asyncio.wait(
                pending_producers,
                timeout=self.producer_shutdown_timeout_seconds,
            )
            for task in done:
                _consume_task_terminal(task)
            if pending:
                for task in pending:
                    task.cancel()
                second_done, second_pending = await asyncio.wait(
                    pending,
                    timeout=max(
                        self.producer_shutdown_timeout_seconds,
                        _MIN_ABNORMAL_CLEANUP_SECONDS,
                    ),
                )
                for task in second_done:
                    _consume_task_terminal(task)
                if second_pending:
                    failures.append(
                        "producer cancellation bound exceeded and tasks remain owned: "
                        + ", ".join(sorted(task.get_name() for task in second_pending))
                    )
        for task in self._producer_tasks:
            if task.done():
                _consume_task_terminal(task)
        if normal_stop_owner is not None:
            await _collect_control_task_failure(
                normal_stop_owner,
                label="normal-stop owner during abnormal cleanup",
                timeout_seconds=max(
                    self.producer_shutdown_timeout_seconds,
                    _MIN_ABNORMAL_CLEANUP_SECONDS,
                ),
                failures=failures,
            )

        adapter_close = self._adapter_close_task
        if adapter_close is None:
            adapter_close = asyncio.create_task(
                self.rest_adapter.aclose(),
                name="r4b-v2-full-runtime-rest-adapter-close-abnormal",
            )
            self._adapter_close_task = adapter_close
        await _collect_control_task_failure(
            adapter_close,
            label="REST adapter abnormal close",
            timeout_seconds=max(
                self.producer_shutdown_timeout_seconds,
                _MIN_ABNORMAL_CLEANUP_SECONDS,
            ),
            failures=failures,
        )

        pipeline_stop = self._pipeline_stop_task
        if pipeline_stop is None:
            pipeline_stop = asyncio.create_task(
                self.pipeline.stop(),
                name="r4b-v2-full-runtime-pipeline-stop-abnormal",
            )
            self._pipeline_stop_task = pipeline_stop
        await _collect_control_task_failure(
            pipeline_stop,
            label="pipeline abnormal stop",
            timeout_seconds=max(
                self.producer_shutdown_timeout_seconds,
                _MIN_ABNORMAL_CLEANUP_SECONDS,
            ),
            failures=failures,
        )
        return tuple(failures)

    def _shared_websocket_stop_event(self) -> asyncio.Event:
        market_event = self.websocket_compositions[0].lifecycle_coordinator.stop_event
        public_event = self.websocket_compositions[1].lifecycle_coordinator.stop_event
        if market_event is not public_event:
            raise PublicCaptureRuntimeBindingErrorV2(
                "WebSocket owners no longer share one stop event"
            )
        return market_event

    def _require_market_token(self) -> PublicWebSocketRuntimeRunTokenV2:
        token = self._market_token
        if token is None:
            raise PublicCaptureRuntimeBindingErrorV2(
                "runtime lacks the usdm_market ownership token"
            )
        return token

    def _require_public_token(self) -> PublicWebSocketRuntimeRunTokenV2:
        token = self._public_token
        if token is None:
            raise PublicCaptureRuntimeBindingErrorV2(
                "runtime lacks the usdm_public ownership token"
            )
        return token


def _validate_unclaimed_runtime_bindings(
    *,
    websocket_compositions: tuple[
        PublicWebSocketOwnerCompositionV2,
        PublicWebSocketOwnerCompositionV2,
    ],
    rest_adapter: PublicOpenInterestRestCaptureAdapterV2,
    rest_scheduler: PublicOpenInterestRestSchedulerV2,
) -> None:
    if type(websocket_compositions) is not tuple or len(websocket_compositions) != 2:
        raise TypeError("runtime requires an exact two-composition tuple")
    if any(
        type(composition) is not PublicWebSocketOwnerCompositionV2
        for composition in websocket_compositions
    ):
        raise TypeError("runtime requires exact WebSocket owner compositions")
    for composition in websocket_compositions:
        composition.validate_current()
    _validate_shared_runtime_bindings(
        websocket_compositions=websocket_compositions,
        rest_adapter=rest_adapter,
        rest_scheduler=rest_scheduler,
        allow_requested_stop=False,
    )


def _validate_shared_runtime_bindings(
    *,
    websocket_compositions: tuple[
        PublicWebSocketOwnerCompositionV2,
        PublicWebSocketOwnerCompositionV2,
    ],
    rest_adapter: PublicOpenInterestRestCaptureAdapterV2,
    rest_scheduler: PublicOpenInterestRestSchedulerV2,
    allow_requested_stop: bool,
) -> None:
    market, public = websocket_compositions
    routes = (market.plan.route_id, public.plan.route_id)
    if routes != _EXPECTED_WEBSOCKET_ROUTES:
        raise PublicCaptureRuntimeBindingErrorV2(
            "runtime requires ordered usdm_market and usdm_public compositions"
        )
    if market is public or market.owner is public.owner:
        raise PublicCaptureRuntimeBindingErrorV2(
            "runtime WebSocket compositions and owners must be distinct"
        )
    if market.session_start_authority is not public.session_start_authority:
        raise PublicCaptureRuntimeBindingErrorV2(
            "WebSocket owners differ on persisted session-start authority"
        )
    if market.writer_lease is not public.writer_lease:
        raise PublicCaptureRuntimeBindingErrorV2("WebSocket owners differ on WriterLease identity")
    if market.promoting_plans is not public.promoting_plans:
        raise PublicCaptureRuntimeBindingErrorV2(
            "WebSocket owners differ on immutable plan-bundle identity"
        )
    plans = market.promoting_plans
    validate_provisional_promoting_capture_plans_v2(plans)
    if market.plan is not plans[0] or public.plan is not plans[1]:
        raise PublicCaptureRuntimeBindingErrorV2(
            "WebSocket compositions differ from the canonical plan-bundle order"
        )
    rest_plan = plans[2]
    if type(rest_plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise PublicCaptureRuntimeBindingErrorV2(
            "third promoting plan is not the exact OI REST plan"
        )
    if (
        type(market.plan) is not ProvisionalPromotingCapturePlanV2
        or type(public.plan) is not ProvisionalPromotingCapturePlanV2
    ):
        raise PublicCaptureRuntimeBindingErrorV2(
            "first two promoting plans are not exact WebSocket plans"
        )

    if type(rest_adapter) is not PublicOpenInterestRestCaptureAdapterV2:
        raise TypeError("runtime requires the exact public OI REST adapter")
    if type(rest_scheduler) is not PublicOpenInterestRestSchedulerV2:
        raise TypeError("runtime requires the exact public OI REST scheduler")
    if rest_scheduler.adapter is not rest_adapter:
        raise PublicCaptureRuntimeBindingErrorV2(
            "OI scheduler does not own the admitted REST adapter"
        )
    if rest_scheduler.plan is not rest_plan or rest_adapter.plan is not rest_plan:
        raise PublicCaptureRuntimeBindingErrorV2(
            "OI scheduler or adapter differs from the plan bundle"
        )
    validate_public_oi_schedule_authority_v2(
        rest_scheduler.schedule_authority,
        plan=rest_plan,
    )
    if rest_adapter.bound_schedule_authority is not rest_scheduler.schedule_authority:
        raise PublicCaptureRuntimeBindingErrorV2(
            "OI adapter differs from the scheduler schedule authority"
        )
    if rest_scheduler.started_once or rest_scheduler.running:
        raise PublicCaptureRuntimeBindingErrorV2("OI scheduler already started")
    if not rest_scheduler.drained:
        raise PublicCaptureRuntimeBindingErrorV2("OI scheduler has attempts before runtime startup")
    if not rest_adapter.accepting_attempts or not rest_adapter.fully_drained:
        raise PublicCaptureRuntimeBindingErrorV2(
            "OI adapter is closed, dirty, failed, or already active"
        )
    fatal = rest_adapter.fatal_coordinator
    if type(fatal) is not PipelineRestCaptureFatalCoordinatorV2:
        raise PublicCaptureRuntimeBindingErrorV2(
            "OI adapter requires the exact pipeline fatal coordinator"
        )

    market_factory = market.frame_adapter_factory
    public_factory = public.frame_adapter_factory
    ingress = market_factory.ingress
    if type(ingress) is not SharedWebSocketIngressV2:
        raise PublicCaptureRuntimeBindingErrorV2("runtime requires the exact shared ingress")
    if public_factory.ingress is not ingress or rest_adapter.ingress is not ingress:
        raise PublicCaptureRuntimeBindingErrorV2(
            "two WebSockets and OI REST do not share one ingress"
        )
    market_lifecycle = market.lifecycle_coordinator
    public_lifecycle = public.lifecycle_coordinator
    pipeline = market_lifecycle.pipeline
    if type(pipeline) is not CaptureBatchPipelineV2:
        raise PublicCaptureRuntimeBindingErrorV2(
            "runtime requires the exact capture batch pipeline"
        )
    if (
        public_lifecycle.pipeline is not pipeline
        or ingress.pipeline is not pipeline
        or fatal.pipeline is not pipeline
    ):
        raise PublicCaptureRuntimeBindingErrorV2("all three producers must share one pipeline")
    if market_lifecycle.integrity_ledger is not public_lifecycle.integrity_ledger:
        raise PublicCaptureRuntimeBindingErrorV2(
            "WebSocket lifecycles differ on integrity-ledger identity"
        )
    if market_lifecycle.stop_event is not public_lifecycle.stop_event:
        raise PublicCaptureRuntimeBindingErrorV2(
            "WebSocket lifecycles differ on shared stop-event identity"
        )
    if pipeline.handoff.fatal_state.stop_event is not market_lifecycle.stop_event:
        raise PublicCaptureRuntimeBindingErrorV2(
            "WebSocket stop event differs from the pipeline fatal domain"
        )
    if pipeline.handoff.fatal_state.stop_event.is_set() and not allow_requested_stop:
        raise PublicCaptureRuntimeBindingErrorV2(
            "shared stop event was set outside runtime normal stop"
        )

    receipt_clock = market_lifecycle.clock
    if (
        public_lifecycle.clock is not receipt_clock
        or market_factory.clock is not receipt_clock
        or public_factory.clock is not receipt_clock
        or rest_adapter.clock is not receipt_clock
    ):
        raise PublicCaptureRuntimeBindingErrorV2("all three producers must share one receipt clock")
    authority = market.session_start_authority
    start = authority.manifest
    if (
        market_lifecycle.session_id != start.session_id
        or public_lifecycle.session_id != start.session_id
        or rest_adapter.session_id != start.session_id
    ):
        raise PublicCaptureRuntimeBindingErrorV2(
            "producer session IDs differ from persisted session start"
        )
    protocol_hash = start.wal_authority.protocol_sha256
    if (
        market_factory.protocol_hash != protocol_hash
        or public_factory.protocol_hash != protocol_hash
        or rest_adapter.protocol_hash != protocol_hash
    ):
        raise PublicCaptureRuntimeBindingErrorV2(
            "producer protocol hashes differ from persisted session start"
        )
    plan_bundle_sha256 = provisional_promoting_plan_sha256_v2(plans)
    if plan_bundle_sha256 != start.wal_authority.plan_sha256:
        raise PublicCaptureRuntimeBindingErrorV2(
            "runtime plan bundle differs from persisted WAL authority"
        )
    if (
        market.recovered_wal_tail_ingest_seq != public.recovered_wal_tail_ingest_seq
        or ingress.recovered_wal_tail_ingest_seq != market.recovered_wal_tail_ingest_seq
    ):
        raise PublicCaptureRuntimeBindingErrorV2("runtime recovered-tail bindings differ")

    context = rest_scheduler.census_context
    if type(context) is not PublicOiRestCensusContextV2:
        raise PublicCaptureRuntimeBindingErrorV2("OI scheduler lacks the exact census context")
    expected_coverage_start = start.started_wall_ms - (
        start.started_wall_ms % PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
    )
    if (
        context.plan is not rest_plan
        or context.ingress is not ingress
        or context.receipt_clock is not receipt_clock
        or context.session_id != start.session_id
        or context.session_start_manifest_sha256 != authority.manifest_sha256
        or context.plan_bundle_sha256 != plan_bundle_sha256
        or context.protocol_hash != protocol_hash
        or context.coverage_start_slot_wall_ms != expected_coverage_start
    ):
        raise PublicCaptureRuntimeBindingErrorV2(
            "OI census context differs from the exact runtime authority"
        )
    pipeline.assert_running_healthy_and_writer_open_v2()
    market_lifecycle.integrity_ledger.assert_running_healthy_and_writer_open_v2()


async def _wait_task_bounded[TaskResultT](
    task: asyncio.Task[TaskResultT],
    *,
    timeout_seconds: float,
) -> bool:
    done, _pending = await asyncio.wait({task}, timeout=timeout_seconds)
    return bool(done)


async def _collect_control_task_failure[TaskResultT](
    task: asyncio.Task[TaskResultT],
    *,
    label: str,
    timeout_seconds: float,
    failures: list[str],
) -> None:
    if not await _wait_task_bounded(task, timeout_seconds=timeout_seconds):
        task.cancel()
        if not await _wait_task_bounded(task, timeout_seconds=timeout_seconds):
            failures.append(f"{label} exceeded two bounds and remains runtime-owned")
            return
        failures.append(f"{label} exceeded its first bounded timeout")
        if task.cancelled():
            return
        failure = task.exception()
        if failure is not None:
            failures.append(f"{label} failed after cancellation: {failure}")
        return
    if task.cancelled():
        failures.append(f"{label} was cancelled")
        return
    failure = task.exception()
    if failure is not None:
        failures.append(f"{label} failed: {type(failure).__name__}: {failure}")


def _consume_task_terminal(task: asyncio.Task[object]) -> None:
    if task.cancelled():
        return
    task.exception()


def _validate_timeout(value: float, field_name: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value <= 0
        or value > _MAX_RUNTIME_TIMEOUT_SECONDS
    ):
        raise ValueError(
            f"{field_name} must be finite, positive, and at most "
            f"{_MAX_RUNTIME_TIMEOUT_SECONDS:g} seconds"
        )


class PublicCaptureRuntimeBindingErrorV8(RuntimeError):
    """The V8 producers and subordinate bridge lack one exact authority."""


class PublicCaptureRuntimeStateErrorV8(RuntimeError):
    """The exclusive V8 runtime was replayed or stopped out of order."""


class PublicCaptureRuntimeShutdownErrorV8(RuntimeError):
    """A V8 producer or owned cleanup exceeded a bounded transition."""


@dataclass(frozen=True, slots=True)
class PublicCaptureRuntimeResultV8:
    """Verified V8 local finality without completeness, CLEAN, or order claims."""

    normal_stop_receipt: ReceiptTimestamp
    oi_coverage_close_receipt: PublicOiCensusAdmissionReceiptV2
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...]
    depth_bridge_close_receipt: DepthBridgeCoordinatorCleanCloseReceiptV8
    websocket_route_cursors: FinalizedWebSocketRouteCursorPairV8
    finality_receipt: CaptureFinalityFenceReceiptV2
    verified_prefix_proof_sha256: str
    producer_task_count: int = _PRODUCER_TASK_COUNT
    adapter_cleanly_closed: bool = True
    oi_coverage_closed: bool = True
    websocket_local_route_cursors_finalized: bool = True
    pending_source_gap: bool = False
    fatal_state_failed: bool = False
    depth_bridge_complete_claimed: bool = False
    oi_data_completeness_claimed: bool = False
    websocket_retained_frame_parser_health_claimed: bool = False
    websocket_upstream_message_completeness_claimed: bool = False
    observed_source_completeness_claimed: bool = False
    m2_eligible: bool = False
    order_execution_enabled: bool = False
    production_order_execution_enabled: bool = False
    local_session_closure_issued: bool = False
    integrity_ledger_clean_issued: bool = False

    def __post_init__(self) -> None:
        if type(self.normal_stop_receipt) is not ReceiptTimestamp:
            raise TypeError("normal_stop_receipt must be an exact ReceiptTimestamp")
        if type(self.oi_coverage_close_receipt) is not PublicOiCensusAdmissionReceiptV2:
            raise TypeError("oi_coverage_close_receipt must be an exact census admission receipt")
        if type(self.promoting_plans) is not tuple:
            raise TypeError("V8 runtime result requires an exact plan tuple")
        validate_provisional_promoting_capture_plans_v8(self.promoting_plans)
        if type(self.finality_receipt) is not CaptureFinalityFenceReceiptV2:
            raise TypeError("finality_receipt must be an exact finality receipt")
        validate_finalized_websocket_route_cursor_pair_v8(
            self.websocket_route_cursors,
            finality_receipt=self.finality_receipt,
            promoting_plans=self.promoting_plans,
        )
        public_cursor = self.websocket_route_cursors[1]
        _validate_depth_bridge_close_bounds_v8(
            self.depth_bridge_close_receipt,
            promoting_plans=self.promoting_plans,
            public_stop_receipt=public_cursor.stop_receipt,
            finality_receipt=self.finality_receipt,
        )
        if (
            type(self.verified_prefix_proof_sha256) is not str
            or _SHA256_RE.fullmatch(self.verified_prefix_proof_sha256) is None
        ):
            raise ValueError("verified prefix proof must be a lowercase SHA-256 digest")
        if self.verified_prefix_proof_sha256 != self.finality_receipt.prefix_proof_sha256:
            raise ValueError("verified prefix proof differs from the finality receipt")
        if self.producer_task_count != _PRODUCER_TASK_COUNT:
            raise ValueError("V8 runtime result must bind exactly three producer tasks")
        for field_name in (
            "adapter_cleanly_closed",
            "oi_coverage_closed",
            "websocket_local_route_cursors_finalized",
        ):
            if getattr(self, field_name) is not True:
                raise ValueError(f"{field_name} must be true for a normal V8 result")
        for field_name in (
            "pending_source_gap",
            "fatal_state_failed",
            "depth_bridge_complete_claimed",
            "oi_data_completeness_claimed",
            "websocket_retained_frame_parser_health_claimed",
            "websocket_upstream_message_completeness_claimed",
            "observed_source_completeness_claimed",
            "m2_eligible",
            "order_execution_enabled",
            "production_order_execution_enabled",
            "local_session_closure_issued",
            "integrity_ledger_clean_issued",
        ):
            if getattr(self, field_name) is not False:
                raise ValueError(f"{field_name} must remain explicitly false")


class PublicCaptureRuntimeV8:
    """Own two V8 WS producers, one OI producer, and one subordinate bridge."""

    def __init__(
        self,
        websocket_compositions: tuple[
            PublicWebSocketOwnerCompositionV8,
            PublicWebSocketOwnerCompositionV8,
        ],
        rest_adapter: PublicOpenInterestRestCaptureAdapterV2,
        rest_scheduler: PublicOpenInterestRestSchedulerV2,
        depth_bridge: PublicDepthRestBridgeCoordinatorV8,
        *,
        producer_shutdown_timeout_seconds: float,
        finality_timeout_seconds: float,
    ) -> None:
        _validate_timeout(
            producer_shutdown_timeout_seconds,
            "producer_shutdown_timeout_seconds",
        )
        _validate_timeout(finality_timeout_seconds, "finality_timeout_seconds")
        _validate_unclaimed_runtime_bindings_v8(
            websocket_compositions=websocket_compositions,
            rest_adapter=rest_adapter,
            rest_scheduler=rest_scheduler,
            depth_bridge=depth_bridge,
        )
        self.websocket_compositions = websocket_compositions
        self.rest_adapter = rest_adapter
        self.rest_scheduler = rest_scheduler
        self.depth_bridge = depth_bridge
        self.producer_shutdown_timeout_seconds = float(producer_shutdown_timeout_seconds)
        self.finality_timeout_seconds = float(finality_timeout_seconds)
        self.pipeline = cast(
            CaptureBatchPipelineV2,
            websocket_compositions[0].lifecycle_coordinator.pipeline,
        )
        self.integrity_ledger = websocket_compositions[0].lifecycle_coordinator.integrity_ledger
        self._market_token: PublicWebSocketRuntimeRunTokenV8 | None = None
        self._public_token: PublicWebSocketRuntimeRunTokenV8 | None = None
        self._producer_tasks: tuple[asyncio.Task[object], ...] = ()
        self._normal_stop_requested_future: asyncio.Future[None] | None = None
        self._normal_stop_owner: asyncio.Task[ReceiptTimestamp] | None = None
        self._oi_adapter_close_task: asyncio.Task[None] | None = None
        self._bridge_close_task: (
            asyncio.Task[DepthBridgeCoordinatorCleanCloseReceiptV8 | None] | None
        ) = None
        self._bridge_abort_task: asyncio.Task[None] | None = None
        self._pipeline_stop_task: asyncio.Task[None] | None = None
        self._cleanup_task: asyncio.Task[tuple[str, ...]] | None = None
        self._started_once = False
        self._running = False
        self._terminal_failure: BaseException | None = None
        self._result: PublicCaptureRuntimeResultV8 | None = None

        market, public = websocket_compositions
        market_token = market.claim_exclusive_runtime_v8(self)
        try:
            public_token = public.claim_exclusive_runtime_v8(self)
        except BaseException:
            market._release_exclusive_runtime_claim_v8(
                market_token,
                runtime_owner=self,
            )
            raise
        self._market_token = market_token
        self._public_token = public_token
        try:
            self.validate_current()
        except BaseException:
            public._release_exclusive_runtime_claim_v8(
                public_token,
                runtime_owner=self,
            )
            market._release_exclusive_runtime_claim_v8(
                market_token,
                runtime_owner=self,
            )
            self._market_token = None
            self._public_token = None
            raise

    @property
    def started_once(self) -> bool:
        return self._started_once

    @property
    def running(self) -> bool:
        return self._running

    @property
    def producer_task_count(self) -> int:
        return len(self._producer_tasks)

    @property
    def result(self) -> PublicCaptureRuntimeResultV8 | None:
        return self._result

    def validate_current(self) -> None:
        """Revalidate every shared V8 binding without creating a task or I/O."""

        market_token = self._market_token
        public_token = self._public_token
        if market_token is None or public_token is None:
            raise PublicCaptureRuntimeBindingErrorV8(
                "V8 runtime lacks both exact WebSocket ownership tokens"
            )
        market, public = self.websocket_compositions
        market.validate_exclusive_runtime_claim_v8(
            market_token,
            runtime_owner=self,
        )
        public.validate_exclusive_runtime_claim_v8(
            public_token,
            runtime_owner=self,
        )
        _validate_shared_runtime_bindings_v8(
            websocket_compositions=self.websocket_compositions,
            rest_adapter=self.rest_adapter,
            rest_scheduler=self.rest_scheduler,
            depth_bridge=self.depth_bridge,
            allow_requested_stop=self._normal_stop_owner is not None,
        )

    async def request_normal_stop(self) -> ReceiptTimestamp:
        """Commit OI stop once, then signal the shared pair of WS owners."""

        if self._terminal_failure is not None and self._normal_stop_owner is None:
            raise PublicCaptureRuntimeStateErrorV8(
                "failed V8 runtime cannot begin a new normal stop"
            ) from self._terminal_failure
        stop_owner = self._normal_stop_owner
        if stop_owner is None:
            if not self._started_once or not self._running:
                raise PublicCaptureRuntimeStateErrorV8(
                    "normal stop requires the V8 producer trio to be running"
                )
            terminal_producers = tuple(task for task in self._producer_tasks if task.done())
            if terminal_producers:
                names = ", ".join(sorted(task.get_name() for task in terminal_producers))
                raise PublicCaptureRuntimeStateErrorV8(
                    "normal stop rejects an already-terminal producer: " + names
                )
            requested_future = self._normal_stop_requested_future
            if requested_future is None or requested_future.done():
                raise PublicCaptureRuntimeStateErrorV8(
                    "V8 runtime lacks a live write-once normal-stop signal"
                )
            stop_owner = asyncio.create_task(
                self._request_normal_stop_owned(),
                name="r4b-v8-full-runtime-normal-stop",
            )
            self._normal_stop_owner = stop_owner
            requested_future.set_result(None)
        cancellation: asyncio.CancelledError | None = None
        while not stop_owner.done():
            try:
                await asyncio.shield(stop_owner)
            except asyncio.CancelledError as exc:
                if cancellation is None:
                    cancellation = exc
        receipt = stop_owner.result()
        if cancellation is not None:
            raise cancellation
        return receipt

    async def run(self) -> PublicCaptureRuntimeResultV8:
        """Run only the producer trio and return local-finality-only evidence."""

        if self._started_once:
            raise PublicCaptureRuntimeStateErrorV8("V8 capture runtime may run only once")
        self._started_once = True
        try:
            self.validate_current()
            market, public = self.websocket_compositions
            startup_barrier = create_public_websocket_runtime_start_barrier_v8(
                self.websocket_compositions,
                runtime_owner=self,
            )
            self._normal_stop_requested_future = asyncio.get_running_loop().create_future()
            self._producer_tasks = (
                cast(
                    asyncio.Task[object],
                    asyncio.create_task(
                        market.run_exclusive_runtime_v8(
                            self._require_market_token(),
                            runtime_owner=self,
                            startup_barrier=startup_barrier,
                        ),
                        name="r4b-v8-producer-usdm-market",
                    ),
                ),
                cast(
                    asyncio.Task[object],
                    asyncio.create_task(
                        public.run_exclusive_runtime_v8(
                            self._require_public_token(),
                            runtime_owner=self,
                            startup_barrier=startup_barrier,
                        ),
                        name="r4b-v8-producer-usdm-public",
                    ),
                ),
                cast(
                    asyncio.Task[object],
                    asyncio.create_task(
                        self.rest_scheduler.run(),
                        name="r4b-v8-producer-usdm-public-rest-oi",
                    ),
                ),
            )
            if len(self._producer_tasks) != _PRODUCER_TASK_COUNT:
                raise AssertionError("V8 full runtime did not create exactly three producers")
            self._running = True
            await self._await_producer_trio()
            result = await self._complete_normal_shutdown()
            self._result = result
            return result
        except BaseException as exc:
            self._terminal_failure = exc
            cleanup_failures = await self._await_abnormal_cleanup_owned()
            if cleanup_failures:
                exc.add_note("R4B V8 abnormal cleanup: " + "; ".join(cleanup_failures))
            raise
        finally:
            self._running = False

    async def _request_normal_stop_owned(self) -> ReceiptTimestamp:
        try:
            return await self.rest_scheduler.request_normal_stop()
        finally:
            self._shared_websocket_stop_event().set()

    async def _await_producer_trio(self) -> None:
        remaining = set(self._producer_tasks)
        stop_requested = self._normal_stop_requested_future
        if stop_requested is None:
            raise AssertionError("V8 runtime lacks its normal-stop wakeup future")
        drain_deadline: float | None = None
        loop = asyncio.get_running_loop()
        while remaining:
            normal_stop_started = self._normal_stop_owner is not None
            if normal_stop_started and drain_deadline is None:
                drain_deadline = loop.time() + self.producer_shutdown_timeout_seconds
            timeout = None if drain_deadline is None else max(0.0, drain_deadline - loop.time())
            waiters: set[asyncio.Future[object]] = {
                cast(asyncio.Future[object], task) for task in remaining
            }
            stop_waiter = cast(asyncio.Future[object], stop_requested)
            if drain_deadline is None:
                waiters.add(stop_waiter)
            done_waiters, _pending_waiters = await asyncio.wait(
                waiters,
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            stop_woke = stop_waiter in done_waiters
            if stop_woke:
                if self._normal_stop_owner is None:
                    raise PublicCaptureRuntimeStateErrorV8(
                        "normal-stop wakeup lacks its exact V8 owner task"
                    )
                if drain_deadline is None:
                    drain_deadline = loop.time() + self.producer_shutdown_timeout_seconds
                done_waiters.remove(stop_waiter)
            done = {task for task in remaining if task in done_waiters}
            if not done and not done_waiters and drain_deadline is not None:
                if stop_woke:
                    continue
                raise PublicCaptureRuntimeShutdownErrorV8(
                    "normal V8 producer drain exceeded its bounded timeout"
                )
            for task in done:
                if task.cancelled():
                    raise PublicCaptureRuntimeShutdownErrorV8(
                        f"V8 producer cancelled unexpectedly: {task.get_name()}"
                    )
                failure = task.exception()
                if failure is not None:
                    raise failure
            remaining.difference_update(done)
            if remaining and self._normal_stop_owner is None:
                names = ", ".join(sorted(task.get_name() for task in done))
                raise PublicCaptureRuntimeShutdownErrorV8(
                    f"V8 producer exited before runtime normal stop: {names}"
                )
        if self._normal_stop_owner is None:
            raise PublicCaptureRuntimeShutdownErrorV8(
                "all V8 producers exited without a runtime normal-stop owner"
            )

    async def _complete_normal_shutdown(self) -> PublicCaptureRuntimeResultV8:
        stop_owner = self._normal_stop_owner
        if stop_owner is None or not stop_owner.done():
            raise PublicCaptureRuntimeShutdownErrorV8(
                "V8 producer trio ended before the normal-stop owner completed"
            )
        normal_stop_receipt = stop_owner.result()
        if (
            self.rest_scheduler.normal_stop_receipt != normal_stop_receipt
            or self.rest_scheduler.running
            or not self.rest_scheduler.drained
        ):
            raise PublicCaptureRuntimeShutdownErrorV8(
                "OI scheduler did not drain at its exact V8 normal-stop receipt"
            )
        coverage_close = self.rest_scheduler.coverage_close_receipt
        if coverage_close is None or not self.rest_scheduler.coverage_closed:
            raise PublicCaptureRuntimeShutdownErrorV8(
                "OI scheduler did not admit its exact V8 coverage close"
            )
        close_record = validate_public_oi_census_admission_receipt_v2(coverage_close)
        if close_record.ingest_seq != coverage_close.accepted_ingest_seq:
            raise PublicCaptureRuntimeShutdownErrorV8(
                "OI coverage-close record differs from its V8 admission sequence"
            )
        websocket_stop_receipts = self._validated_websocket_stop_receipts()

        # The subordinate bridge may still be writing terminal evidence after
        # the public WebSocket producer has returned.  Its bounded zero census
        # therefore precedes both OI adapter closure and pipeline finality.
        depth_bridge_close_receipt = await self._close_bridge_bounded()
        self._require_depth_bridge_zero_state(depth_bridge_close_receipt)
        _validate_depth_bridge_close_bounds_v8(
            depth_bridge_close_receipt,
            promoting_plans=self.websocket_compositions[0].promoting_plans,
            public_stop_receipt=websocket_stop_receipts[1],
            finality_receipt=None,
        )
        await self._close_oi_adapter_bounded()
        if not self.rest_adapter.cleanly_closed:
            raise PublicCaptureRuntimeShutdownErrorV8(
                "OI REST adapter lacks a clean closed-and-drained V8 proof"
            )
        for composition in self.websocket_compositions:
            lifecycle = composition.lifecycle_coordinator
            if lifecycle.failed:
                lifecycle.pipeline.handoff.fatal_state.raise_if_failed()
                raise PublicCaptureRuntimeShutdownErrorV8(
                    "V8 WebSocket lifecycle failed without a visible fatal cause"
                )
            if lifecycle.pending_source_gap:
                raise PublicCaptureRuntimeShutdownErrorV8(
                    f"pending V8 WebSocket source gap remains for {composition.plan.route_id}"
                )
        self.pipeline.handoff.fatal_state.raise_if_failed()
        if not self.pipeline.handoff.accepting:
            raise PublicCaptureRuntimeShutdownErrorV8(
                "capture handoff stopped accepting before V8 current-tail finality"
            )
        self.integrity_ledger.assert_running_healthy_and_writer_open_v2()

        finality = await self.pipeline.finalize_current_tail_and_stop(
            timeout_seconds=self.finality_timeout_seconds,
        )
        verified_prefix = verify_clean_stopped_current_tail_v2(
            finality,
            pipeline=self.pipeline,
        )
        if finality.fence_ingest_seq < coverage_close.accepted_ingest_seq:
            raise PublicCaptureRuntimeShutdownErrorV8(
                "V8 finalized tail precedes the admitted OI coverage close"
            )
        _validate_depth_bridge_close_bounds_v8(
            depth_bridge_close_receipt,
            promoting_plans=self.websocket_compositions[0].promoting_plans,
            public_stop_receipt=websocket_stop_receipts[1],
            finality_receipt=finality,
        )
        websocket_route_cursors = finalize_websocket_route_cursor_pair_v8(
            websocket_stop_receipts,
            finality_receipt=finality,
            promoting_plans=self.websocket_compositions[0].promoting_plans,
        )
        return PublicCaptureRuntimeResultV8(
            normal_stop_receipt=normal_stop_receipt,
            oi_coverage_close_receipt=coverage_close,
            promoting_plans=self.websocket_compositions[0].promoting_plans,
            depth_bridge_close_receipt=depth_bridge_close_receipt,
            websocket_route_cursors=websocket_route_cursors,
            finality_receipt=finality,
            verified_prefix_proof_sha256=verified_prefix,
        )

    def _validated_websocket_stop_receipts(
        self,
    ) -> tuple[WebSocketRouteStopReceiptV8, WebSocketRouteStopReceiptV8]:
        if len(self._producer_tasks) != _PRODUCER_TASK_COUNT:
            raise PublicCaptureRuntimeShutdownErrorV8(
                "V8 runtime lacks the exact producer trio at stop validation"
            )
        receipts: list[WebSocketRouteStopReceiptV8] = []
        for task, composition in zip(
            self._producer_tasks[:2],
            self.websocket_compositions,
            strict=True,
        ):
            if not task.done() or task.cancelled():
                raise PublicCaptureRuntimeShutdownErrorV8(
                    f"V8 WebSocket owner is not cleanly stopped for {composition.plan.route_id}"
                )
            result = task.result()
            if type(result) is not WebSocketRouteStopReceiptV8:
                raise PublicCaptureRuntimeShutdownErrorV8(
                    "V8 WebSocket owner lacks an exact OWNER_STOP cursor for "
                    f"{composition.plan.route_id}"
                )
            if result is not composition.lifecycle_coordinator.normal_stop_receipt_v8:
                raise PublicCaptureRuntimeShutdownErrorV8(
                    "V8 WebSocket task returned a foreign OWNER_STOP cursor for "
                    f"{composition.plan.route_id}"
                )
            validate_websocket_route_stop_receipt_v8(
                result,
                promoting_plans=composition.promoting_plans,
                plan=composition.plan,
            )
            receipts.append(result)
        if tuple(receipt.route_id for receipt in receipts) != _EXPECTED_WEBSOCKET_ROUTES:
            raise PublicCaptureRuntimeShutdownErrorV8(
                "V8 WebSocket OWNER_STOP cursors are not in canonical route order"
            )
        return receipts[0], receipts[1]

    async def _close_bridge_bounded(
        self,
    ) -> DepthBridgeCoordinatorCleanCloseReceiptV8:
        task = self._bridge_close_task
        if task is None:
            task = asyncio.create_task(
                self.depth_bridge.aclose(),
                name="r4b-v8-full-runtime-depth-bridge-close",
            )
            self._bridge_close_task = task
        if not await _wait_task_bounded(
            task,
            timeout_seconds=self.producer_shutdown_timeout_seconds,
        ):
            raise PublicCaptureRuntimeShutdownErrorV8(
                "depth bridge close exceeded the V8 runtime bound"
            )
        if task.cancelled():
            raise PublicCaptureRuntimeShutdownErrorV8("depth bridge close task was cancelled")
        receipt = task.result()
        if type(receipt) is not DepthBridgeCoordinatorCleanCloseReceiptV8:
            raise PublicCaptureRuntimeShutdownErrorV8(
                "depth bridge normal close lacks an exact clean-close receipt"
            )
        plans = self.websocket_compositions[0].promoting_plans
        depth_plan = plans[3]
        if type(depth_plan) is not ProvisionalDepthRestQualificationPlanV8:
            raise PublicCaptureRuntimeBindingErrorV8(
                "V8 runtime lost its exact depth plan before bridge close"
            )
        validate_depth_bridge_coordinator_clean_close_receipt_v8(
            receipt,
            promoting_plans=plans,
            depth_plan=depth_plan,
        )
        expected_protocol_hash = self.websocket_compositions[
            0
        ].session_start_authority.manifest.wal_authority.protocol_sha256
        if receipt.protocol_hash != expected_protocol_hash:
            raise PublicCaptureRuntimeShutdownErrorV8(
                "depth bridge clean-close receipt has foreign protocol authority"
            )
        return receipt

    def _require_depth_bridge_zero_state(
        self,
        receipt: DepthBridgeCoordinatorCleanCloseReceiptV8,
    ) -> None:
        self.depth_bridge.validate_current()
        authority = self.depth_bridge.schedule_authority
        if (
            not self.depth_bridge.permanently_closed
            or self.depth_bridge.clean_close_receipt is not receipt
            or self.depth_bridge.generation_open
            or self.depth_bridge.worker_count != 0
            or self.depth_bridge.permit_in_use_count != 0
            or self.depth_bridge.adapter is not None
            or authority.generation_open
            or authority.retained_registration_count != 0
            or authority.pending_registration_count != 0
            or authority.retained_token_count != 0
            or authority.claimed_token_count != 0
        ):
            raise PublicCaptureRuntimeShutdownErrorV8(
                "depth bridge lacks an exact closed zero-count V8 census"
            )

    async def _close_oi_adapter_bounded(self) -> None:
        task = self._oi_adapter_close_task
        if task is None:
            task = asyncio.create_task(
                self.rest_adapter.aclose(),
                name="r4b-v8-full-runtime-rest-adapter-close",
            )
            self._oi_adapter_close_task = task
        if not await _wait_task_bounded(
            task,
            timeout_seconds=self.producer_shutdown_timeout_seconds,
        ):
            raise PublicCaptureRuntimeShutdownErrorV8(
                "OI REST adapter close exceeded the V8 runtime bound"
            )
        if task.cancelled():
            raise PublicCaptureRuntimeShutdownErrorV8("OI REST adapter close task was cancelled")
        task.result()

    async def _await_abnormal_cleanup_owned(self) -> tuple[str, ...]:
        cleanup = self._cleanup_task
        if cleanup is None:
            cleanup = asyncio.create_task(
                self._abnormal_cleanup_owned(),
                name="r4b-v8-full-runtime-abnormal-cleanup",
            )
            self._cleanup_task = cleanup
        cancellation: asyncio.CancelledError | None = None
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError as exc:
                if cancellation is None:
                    cancellation = exc
        if cleanup.cancelled():
            failures = ("V8 abnormal cleanup owner was cancelled",)
        else:
            failures = cleanup.result()
        if cancellation is not None:
            failures += ("caller repeated cancellation during V8 owned cleanup",)
        return failures

    async def _abnormal_cleanup_owned(self) -> tuple[str, ...]:
        failures: list[str] = []
        self._shared_websocket_stop_event().set()
        normal_stop_owner = self._normal_stop_owner
        if normal_stop_owner is not None and not normal_stop_owner.done():
            normal_stop_owner.cancel()

        pending_producers = tuple(task for task in self._producer_tasks if not task.done())
        for task in pending_producers:
            task.cancel()
        if pending_producers:
            done, pending = await asyncio.wait(
                pending_producers,
                timeout=self.producer_shutdown_timeout_seconds,
            )
            for task in done:
                _consume_task_terminal(task)
            if pending:
                for task in pending:
                    task.cancel()
                second_done, second_pending = await asyncio.wait(
                    pending,
                    timeout=max(
                        self.producer_shutdown_timeout_seconds,
                        _MIN_ABNORMAL_CLEANUP_SECONDS,
                    ),
                )
                for task in second_done:
                    _consume_task_terminal(task)
                if second_pending:
                    failures.append(
                        "V8 producer cancellation bound exceeded and tasks remain "
                        "owned: " + ", ".join(sorted(task.get_name() for task in second_pending))
                    )
        for task in self._producer_tasks:
            if task.done():
                _consume_task_terminal(task)
        if normal_stop_owner is not None:
            await _collect_control_task_failure(
                normal_stop_owner,
                label="V8 normal-stop owner during abnormal cleanup",
                timeout_seconds=max(
                    self.producer_shutdown_timeout_seconds,
                    _MIN_ABNORMAL_CLEANUP_SECONDS,
                ),
                failures=failures,
            )

        bridge_close = self._bridge_close_task
        if bridge_close is not None and not bridge_close.done():
            bridge_close.cancel()
            if not await _wait_task_bounded(
                bridge_close,
                timeout_seconds=max(
                    self.producer_shutdown_timeout_seconds,
                    _MIN_ABNORMAL_CLEANUP_SECONDS,
                ),
            ):
                failures.append(
                    "normal depth-bridge close resisted cancellation during V8 abnormal cleanup"
                )
        if bridge_close is not None and bridge_close.done():
            _consume_task_terminal(cast(asyncio.Task[object], bridge_close))

        websocket_tasks = self._producer_tasks[:2]
        if any(not task.done() for task in websocket_tasks):
            failures.append(
                "depth bridge abort was withheld because a WebSocket producer remains live"
            )
        else:
            terminal_failure = self._terminal_failure
            if terminal_failure is None:
                terminal_failure = PublicCaptureRuntimeShutdownErrorV8(
                    "V8 abnormal cleanup lacks its initiating failure"
                )
            bridge_abort = self._bridge_abort_task
            if bridge_abort is None:
                bridge_abort = asyncio.create_task(
                    self.depth_bridge.abort_and_drain(terminal_failure),
                    name="r4b-v8-full-runtime-depth-bridge-abort",
                )
                self._bridge_abort_task = bridge_abort
            await _collect_control_task_failure(
                bridge_abort,
                label="depth bridge abnormal drain",
                timeout_seconds=max(
                    self.producer_shutdown_timeout_seconds,
                    _MIN_ABNORMAL_CLEANUP_SECONDS,
                ),
                failures=failures,
            )

        adapter_close = self._oi_adapter_close_task
        if adapter_close is None:
            adapter_close = asyncio.create_task(
                self.rest_adapter.aclose(),
                name="r4b-v8-full-runtime-rest-adapter-close-abnormal",
            )
            self._oi_adapter_close_task = adapter_close
        await _collect_control_task_failure(
            adapter_close,
            label="OI REST adapter abnormal V8 close",
            timeout_seconds=max(
                self.producer_shutdown_timeout_seconds,
                _MIN_ABNORMAL_CLEANUP_SECONDS,
            ),
            failures=failures,
        )

        pipeline_stop = self._pipeline_stop_task
        if pipeline_stop is None:
            pipeline_stop = asyncio.create_task(
                self.pipeline.stop(),
                name="r4b-v8-full-runtime-pipeline-stop-abnormal",
            )
            self._pipeline_stop_task = pipeline_stop
        await _collect_control_task_failure(
            pipeline_stop,
            label="pipeline abnormal V8 stop",
            timeout_seconds=max(
                self.producer_shutdown_timeout_seconds,
                _MIN_ABNORMAL_CLEANUP_SECONDS,
            ),
            failures=failures,
        )
        return tuple(failures)

    def _shared_websocket_stop_event(self) -> asyncio.Event:
        market_event = self.websocket_compositions[0].lifecycle_coordinator.stop_event
        public_event = self.websocket_compositions[1].lifecycle_coordinator.stop_event
        if market_event is not public_event:
            raise PublicCaptureRuntimeBindingErrorV8(
                "V8 WebSocket owners no longer share one stop event"
            )
        return market_event

    def _require_market_token(self) -> PublicWebSocketRuntimeRunTokenV8:
        token = self._market_token
        if token is None:
            raise PublicCaptureRuntimeBindingErrorV8(
                "V8 runtime lacks the usdm_market ownership token"
            )
        return token

    def _require_public_token(self) -> PublicWebSocketRuntimeRunTokenV8:
        token = self._public_token
        if token is None:
            raise PublicCaptureRuntimeBindingErrorV8(
                "V8 runtime lacks the usdm_public ownership token"
            )
        return token


def _validate_unclaimed_runtime_bindings_v8(
    *,
    websocket_compositions: tuple[
        PublicWebSocketOwnerCompositionV8,
        PublicWebSocketOwnerCompositionV8,
    ],
    rest_adapter: PublicOpenInterestRestCaptureAdapterV2,
    rest_scheduler: PublicOpenInterestRestSchedulerV2,
    depth_bridge: PublicDepthRestBridgeCoordinatorV8,
) -> None:
    if type(websocket_compositions) is not tuple or len(websocket_compositions) != 2:
        raise TypeError("V8 runtime requires an exact two-composition tuple")
    if any(
        type(composition) is not PublicWebSocketOwnerCompositionV8
        for composition in websocket_compositions
    ):
        raise TypeError("V8 runtime requires exact V8 WebSocket compositions")
    for composition in websocket_compositions:
        composition.validate_current()
    _validate_shared_runtime_bindings_v8(
        websocket_compositions=websocket_compositions,
        rest_adapter=rest_adapter,
        rest_scheduler=rest_scheduler,
        depth_bridge=depth_bridge,
        allow_requested_stop=False,
    )


def _validate_shared_runtime_bindings_v8(
    *,
    websocket_compositions: tuple[
        PublicWebSocketOwnerCompositionV8,
        PublicWebSocketOwnerCompositionV8,
    ],
    rest_adapter: PublicOpenInterestRestCaptureAdapterV2,
    rest_scheduler: PublicOpenInterestRestSchedulerV2,
    depth_bridge: PublicDepthRestBridgeCoordinatorV8,
    allow_requested_stop: bool,
) -> None:
    market, public = websocket_compositions
    routes = (market.plan.route_id, public.plan.route_id)
    if routes != _EXPECTED_WEBSOCKET_ROUTES:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 runtime requires ordered usdm_market and usdm_public compositions"
        )
    if market is public or market.owner is public.owner:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 WebSocket compositions and owners must be distinct"
        )
    if market.session_start_authority is not public.session_start_authority:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 WebSocket owners differ on session-start authority"
        )
    if market.writer_lease is not public.writer_lease:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 WebSocket owners differ on WriterLease identity"
        )
    if market.promoting_plans is not public.promoting_plans:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 WebSocket owners differ on four-plan bundle identity"
        )
    plans = market.promoting_plans
    validate_provisional_promoting_capture_plans_v8(plans)
    if market.plan is not plans[0] or public.plan is not plans[1]:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 compositions differ from canonical four-plan order"
        )
    rest_plan = plans[2]
    depth_plan = plans[3]
    if type(rest_plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise PublicCaptureRuntimeBindingErrorV8("third V8 plan is not the exact OI REST plan")
    if type(depth_plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise PublicCaptureRuntimeBindingErrorV8(
            "fourth V8 plan is not the exact depth qualification plan"
        )
    if (
        type(market.plan) is not ProvisionalPromotingCapturePlanV2
        or type(public.plan) is not ProvisionalPromotingCapturePlanV2
    ):
        raise PublicCaptureRuntimeBindingErrorV8("first two V8 plans are not exact WebSocket plans")

    if type(rest_adapter) is not PublicOpenInterestRestCaptureAdapterV2:
        raise TypeError("V8 runtime requires the exact public OI REST adapter")
    if type(rest_scheduler) is not PublicOpenInterestRestSchedulerV2:
        raise TypeError("V8 runtime requires the exact public OI REST scheduler")
    if type(depth_bridge) is not PublicDepthRestBridgeCoordinatorV8:
        raise TypeError("V8 runtime requires the exact public depth bridge")
    if rest_scheduler.adapter is not rest_adapter:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 OI scheduler does not own the admitted REST adapter"
        )
    if rest_scheduler.plan is not rest_plan or rest_adapter.plan is not rest_plan:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 OI scheduler or adapter differs from its plan bundle"
        )
    validate_public_oi_schedule_authority_v2(
        rest_scheduler.schedule_authority,
        plan=rest_plan,
    )
    if rest_adapter.bound_schedule_authority is not rest_scheduler.schedule_authority:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 OI adapter differs from scheduler schedule authority"
        )
    if rest_scheduler.started_once or rest_scheduler.running:
        raise PublicCaptureRuntimeBindingErrorV8("V8 OI scheduler already started")
    if not rest_scheduler.drained:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 OI scheduler has attempts before runtime startup"
        )
    if not rest_adapter.accepting_attempts or not rest_adapter.fully_drained:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 OI adapter is closed, dirty, failed, or already active"
        )
    fatal = rest_adapter.fatal_coordinator
    if type(fatal) is not PipelineRestCaptureFatalCoordinatorV2:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 OI adapter requires the exact pipeline fatal coordinator"
        )

    market_owner = market.owner
    public_owner = public.owner
    if any(
        callback is not None
        for callback in (
            market_owner.preconnecting_generation_hook,
            market_owner.retained_depth_range_callback,
            market_owner.retained_depth_resync_callback,
            market_owner.depth_range_callback,
            market_owner.depth_resync_callback,
        )
    ):
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 market owner must not carry public depth bridge callbacks"
        )
    if (
        public_owner.preconnecting_generation_hook is not depth_bridge.preconnecting_generation_hook
        or public_owner.retained_depth_range_callback
        is not depth_bridge.retained_depth_range_callback
        or public_owner.retained_depth_resync_callback
        is not depth_bridge.retained_depth_resync_callback
        or public_owner.depth_range_callback is not None
        or public_owner.depth_resync_callback is not None
    ):
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 public owner lacks the exact subordinate depth bridge callbacks"
        )
    if (
        depth_bridge.generation_open
        or depth_bridge.connection_generation != 0
        or depth_bridge.worker_count != 0
        or depth_bridge.permit_in_use_count != 0
        or depth_bridge.adapter is not None
        or depth_bridge.schedule_authority.generation_open
        or depth_bridge.schedule_authority.retained_registration_count != 0
        or depth_bridge.schedule_authority.pending_registration_count != 0
        or depth_bridge.schedule_authority.retained_token_count != 0
        or depth_bridge.schedule_authority.claimed_token_count != 0
    ):
        raise PublicCaptureRuntimeBindingErrorV8(
            "depth bridge has live state before V8 runtime startup"
        )

    market_factory = market.frame_adapter_factory
    public_factory = public.frame_adapter_factory
    ingress = market_factory.ingress
    if type(ingress) is not SharedWebSocketIngressV2:
        raise PublicCaptureRuntimeBindingErrorV8("V8 runtime requires the exact shared ingress")
    if public_factory.ingress is not ingress or rest_adapter.ingress is not ingress:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 WebSockets and OI REST do not share one ingress"
        )
    market_lifecycle = market.lifecycle_coordinator
    public_lifecycle = public.lifecycle_coordinator
    pipeline = market_lifecycle.pipeline
    if type(pipeline) is not CaptureBatchPipelineV2:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 runtime requires the exact capture batch pipeline"
        )
    if (
        public_lifecycle.pipeline is not pipeline
        or ingress.pipeline is not pipeline
        or fatal.pipeline is not pipeline
    ):
        raise PublicCaptureRuntimeBindingErrorV8(
            "all V8 producers must share one fatal pipeline domain"
        )
    ledger = market_lifecycle.integrity_ledger
    if public_lifecycle.integrity_ledger is not ledger:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 WebSocket lifecycles differ on integrity ledger"
        )
    if market_lifecycle.stop_event is not public_lifecycle.stop_event:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 WebSocket lifecycles differ on shared stop event"
        )
    if pipeline.handoff.fatal_state.stop_event is not market_lifecycle.stop_event:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 WebSocket stop event differs from the pipeline fatal domain"
        )
    if pipeline.handoff.fatal_state.stop_event.is_set() and not allow_requested_stop:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 shared stop event was set outside runtime normal stop"
        )

    receipt_clock = market_lifecycle.clock
    if (
        public_lifecycle.clock is not receipt_clock
        or market_factory.clock is not receipt_clock
        or public_factory.clock is not receipt_clock
        or rest_adapter.clock is not receipt_clock
    ):
        raise PublicCaptureRuntimeBindingErrorV8("all V8 producers must share one receipt clock")
    depth_bridge.validate_runtime_bindings(
        promoting_plans=plans,
        depth_plan=depth_plan,
        websocket_owner=public_owner,
        ingress=ingress,
        clock=receipt_clock,
        fatal_coordinator=fatal,
        ledger=ledger,
    )

    authority = market.session_start_authority
    start = authority.manifest
    if (
        market_lifecycle.session_id != start.session_id
        or public_lifecycle.session_id != start.session_id
        or rest_adapter.session_id != start.session_id
    ):
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 producer session IDs differ from persisted session start"
        )
    protocol_hash = start.wal_authority.protocol_sha256
    if (
        market_factory.protocol_hash != protocol_hash
        or public_factory.protocol_hash != protocol_hash
        or rest_adapter.protocol_hash != protocol_hash
    ):
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 producer protocol hashes differ from persisted session start"
        )
    plan_bundle_sha256 = provisional_promoting_plan_sha256_v8(plans)
    if plan_bundle_sha256 != start.wal_authority.plan_sha256:
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 four-plan bundle differs from persisted WAL authority"
        )
    if (
        market.recovered_wal_tail_ingest_seq != public.recovered_wal_tail_ingest_seq
        or ingress.recovered_wal_tail_ingest_seq != market.recovered_wal_tail_ingest_seq
    ):
        raise PublicCaptureRuntimeBindingErrorV8("V8 runtime recovered-tail bindings differ")

    context = rest_scheduler.census_context
    if type(context) is not PublicOiRestCensusContextV2:
        raise PublicCaptureRuntimeBindingErrorV8("V8 OI scheduler lacks the exact census context")
    expected_coverage_start = start.started_wall_ms - (
        start.started_wall_ms % PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
    )
    if (
        context.plan is not rest_plan
        or context.ingress is not ingress
        or context.receipt_clock is not receipt_clock
        or context.session_id != start.session_id
        or context.session_start_manifest_sha256 != authority.manifest_sha256
        or context.plan_bundle_sha256 != plan_bundle_sha256
        or context.protocol_hash != protocol_hash
        or context.coverage_start_slot_wall_ms != expected_coverage_start
    ):
        raise PublicCaptureRuntimeBindingErrorV8(
            "V8 OI census context differs from exact runtime authority"
        )
    pipeline.assert_running_healthy_and_writer_open_v2()
    ledger.assert_running_healthy_and_writer_open_v2()


def _validate_depth_bridge_close_bounds_v8(
    receipt: DepthBridgeCoordinatorCleanCloseReceiptV8,
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
    public_stop_receipt: WebSocketRouteStopReceiptV8,
    finality_receipt: CaptureFinalityFenceReceiptV2 | None,
) -> None:
    """Bind one factory receipt to the public cursor and causal shutdown clocks."""

    if type(receipt) is not DepthBridgeCoordinatorCleanCloseReceiptV8:
        raise TypeError("depth bridge close proof must be an exact clean-close receipt")
    if type(promoting_plans) is not tuple or len(promoting_plans) != 4:
        raise TypeError("depth bridge close proof requires the exact V8 plan tuple")
    depth_plan = promoting_plans[3]
    if type(depth_plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise TypeError("depth bridge close proof requires the exact depth plan")
    public_plan = promoting_plans[1]
    if type(public_plan) is not ProvisionalPromotingCapturePlanV2:
        raise TypeError("depth bridge close proof requires the exact public plan")
    if type(public_stop_receipt) is not WebSocketRouteStopReceiptV8:
        raise TypeError("depth bridge close proof requires an exact public stop receipt")
    try:
        validate_depth_bridge_coordinator_clean_close_receipt_v8(
            receipt,
            promoting_plans=promoting_plans,
            depth_plan=depth_plan,
        )
        validate_websocket_route_stop_receipt_v8(
            public_stop_receipt,
            promoting_plans=promoting_plans,
            plan=public_plan,
        )
    except (TypeError, ValueError) as exc:
        raise PublicCaptureRuntimeShutdownErrorV8(
            "depth bridge close proof failed factory or route validation"
        ) from exc

    if public_stop_receipt.route_id != "usdm_public":
        raise PublicCaptureRuntimeShutdownErrorV8(
            "depth bridge close proof is not bound to the public WebSocket route"
        )
    if receipt.session_id != public_stop_receipt.session_id:
        raise PublicCaptureRuntimeShutdownErrorV8(
            "depth bridge close receipt session differs from public OWNER_STOP"
        )
    if receipt.plan_bundle_sha256 != public_stop_receipt.plan_bundle_sha256:
        raise PublicCaptureRuntimeShutdownErrorV8(
            "depth bridge close receipt plan authority differs from public OWNER_STOP"
        )
    if receipt.last_connection_id != public_stop_receipt.connection_id:
        raise PublicCaptureRuntimeShutdownErrorV8(
            "depth bridge close receipt connection differs from public OWNER_STOP"
        )
    if receipt.last_connection_generation != public_stop_receipt.generation:
        raise PublicCaptureRuntimeShutdownErrorV8(
            "depth bridge close receipt generation differs from public OWNER_STOP"
        )
    if (
        receipt.last_generation_drained_recorded_monotonic_ns > receipt.close_monotonic_ns
        or receipt.last_generation_drained_recorded_wall_ms > receipt.close_wall_ms
    ):
        raise PublicCaptureRuntimeShutdownErrorV8(
            "depth bridge close clock precedes its persisted generation drain"
        )
    if (
        public_stop_receipt.stop_observed_monotonic_ns > receipt.close_monotonic_ns
        or public_stop_receipt.stop_observed_wall_ms > receipt.close_wall_ms
    ):
        raise PublicCaptureRuntimeShutdownErrorV8(
            "depth bridge close clock precedes public OWNER_STOP observation"
        )

    if finality_receipt is None:
        return
    if type(finality_receipt) is not CaptureFinalityFenceReceiptV2:
        raise TypeError("depth bridge close proof requires an exact finality receipt")
    # Finality exposes no wall-clock observation for its upper boundary, so the
    # bridge-to-finality causal proof is intentionally monotonic-clock-only.
    if (
        receipt.close_monotonic_ns > finality_receipt.fence_monotonic_ns
        or receipt.close_monotonic_ns > finality_receipt.writer_observed_monotonic_ns
    ):
        raise PublicCaptureRuntimeShutdownErrorV8(
            "V8 finality precedes the subordinate depth bridge close"
        )


class PublicUsdmVenueClockRuntimeBindingErrorV9(RuntimeError):
    """The V9 venue-clock producer does not share one exact capture domain."""


class PublicUsdmVenueClockRuntimeStateErrorV9(RuntimeError):
    """The one-shot V9 venue-clock runtime was used out of order."""


class PublicUsdmVenueClockRuntimeShutdownErrorV9(RuntimeError):
    """The V9 venue-clock adapter did not close within its bounded owner."""


@dataclass(frozen=True, slots=True)
class PublicUsdmVenueClockRuntimeResultV9:
    """Clean local producer shutdown without session or causal-cursor claims."""

    promoting_plans: tuple[ProvisionalPromotingPlanV9, ...]
    scheduler_result: PublicUsdmVenueClockSchedulerResultV9
    producer_task_count: int = 1
    adapter_cleanly_closed: bool = True
    scheduler_drained: bool = True
    clock_data_completeness_claimed: bool = False
    durable_session_closure_issued: bool = False
    causal_cursor_complete: bool = False
    order_execution_enabled: bool = False
    production_order_execution_enabled: bool = False

    def __post_init__(self) -> None:
        if type(self.promoting_plans) is not tuple:
            raise TypeError("venue-clock runtime result requires an exact V9 tuple")
        validate_provisional_promoting_capture_plans_v9(self.promoting_plans)
        if type(self.scheduler_result) is not PublicUsdmVenueClockSchedulerResultV9:
            raise TypeError("venue-clock runtime requires an exact scheduler result")
        self.scheduler_result.__post_init__()
        if self.producer_task_count != 1:
            raise ValueError("venue-clock runtime owns exactly one producer task")
        for field_name in ("adapter_cleanly_closed", "scheduler_drained"):
            if getattr(self, field_name) is not True:
                raise ValueError(f"{field_name} must be true for a normal result")
        for field_name in (
            "clock_data_completeness_claimed",
            "durable_session_closure_issued",
            "causal_cursor_complete",
            "order_execution_enabled",
            "production_order_execution_enabled",
        ):
            if getattr(self, field_name) is not False:
                raise ValueError(f"{field_name} must remain explicitly false")
        last_receipt = self.scheduler_result.last_admission_receipt
        if last_receipt is not None:
            clock_plan = _unique_clock_plan_v9(self.promoting_plans)
            if last_receipt.plan is not clock_plan:
                raise ValueError(
                    "venue-clock runtime result receipt differs from its V9 plan object"
                )


class PublicUsdmVenueClockRuntimeV9:
    """Bounded production owner for the additive V9 clock producer.

    This owner binds the scheduler and adapter to the exact shared ingress and
    fatal pipeline domain used by the other capture producers.  It deliberately
    does not claim V9 clean-session finality: V8 session/composition owners are
    still four-plan authorities and must not be silently relabelled as V9.
    """

    def __init__(
        self,
        promoting_plans: tuple[ProvisionalPromotingPlanV9, ...],
        ingress: SharedWebSocketIngressV2,
        adapter: PublicUsdmVenueClockRestCaptureAdapterV9,
        scheduler: PublicUsdmVenueClockRestSchedulerV9,
        *,
        adapter_shutdown_timeout_seconds: float,
    ) -> None:
        _validate_timeout(
            adapter_shutdown_timeout_seconds,
            "adapter_shutdown_timeout_seconds",
        )
        _validate_usdm_venue_clock_runtime_bindings_v9(
            promoting_plans=promoting_plans,
            ingress=ingress,
            adapter=adapter,
            scheduler=scheduler,
        )
        self.promoting_plans = promoting_plans
        self.ingress = ingress
        self.adapter = adapter
        self.scheduler = scheduler
        self.adapter_shutdown_timeout_seconds = float(adapter_shutdown_timeout_seconds)
        self._started_once = False
        self._running = False
        self._normal_stop_requested = False
        self._adapter_close_task: asyncio.Task[None] | None = None
        self._terminal_failure: BaseException | None = None
        self._result: PublicUsdmVenueClockRuntimeResultV9 | None = None

    @property
    def started_once(self) -> bool:
        return self._started_once

    @property
    def running(self) -> bool:
        return self._running

    @property
    def result(self) -> PublicUsdmVenueClockRuntimeResultV9 | None:
        return self._result

    async def request_normal_stop(self) -> None:
        """Wake the idle scheduler and let an in-flight attempt drain."""

        if not self._started_once or not self._running:
            raise PublicUsdmVenueClockRuntimeStateErrorV9(
                "venue-clock normal stop requires a running runtime"
            )
        if self._terminal_failure is not None:
            raise PublicUsdmVenueClockRuntimeStateErrorV9(
                "failed venue-clock runtime cannot begin normal stop"
            ) from self._terminal_failure
        self._normal_stop_requested = True
        self.scheduler.request_stop()

    async def run(self) -> PublicUsdmVenueClockRuntimeResultV9:
        """Run the exact clock producer once and own bounded adapter closure."""

        if self._started_once:
            raise PublicUsdmVenueClockRuntimeStateErrorV9("venue-clock runtime may run only once")
        self._started_once = True
        self._running = True
        cancellation: asyncio.CancelledError | None = None
        scheduler_result: PublicUsdmVenueClockSchedulerResultV9 | None = None
        failure: BaseException | None = None
        try:
            _validate_usdm_venue_clock_runtime_bindings_v9(
                promoting_plans=self.promoting_plans,
                ingress=self.ingress,
                adapter=self.adapter,
                scheduler=self.scheduler,
            )
            scheduler_result = await self.scheduler.run()
        except asyncio.CancelledError as exc:
            cancellation = exc
            self.scheduler.request_stop()
        except BaseException as exc:
            failure = exc
            self._terminal_failure = exc
            self.scheduler.request_stop()
        finally:
            try:
                await self._close_adapter_bounded()
            except BaseException as exc:
                if failure is None:
                    failure = exc
                    self._terminal_failure = exc
                else:
                    failure = BaseExceptionGroup(
                        "venue-clock producer and adapter cleanup both failed",
                        (failure, exc),
                    )
                    self._terminal_failure = failure
            self._running = False

        if failure is not None:
            raise failure
        if cancellation is not None:
            raise cancellation
        if scheduler_result is None:
            error = PublicUsdmVenueClockRuntimeShutdownErrorV9(
                "venue-clock scheduler returned no exact result"
            )
            self._terminal_failure = error
            raise error
        if not self._normal_stop_requested and not self.scheduler.stop_requested:
            error = PublicUsdmVenueClockRuntimeShutdownErrorV9(
                "venue-clock scheduler ended without a normal-stop request"
            )
            self._terminal_failure = error
            raise error
        result = PublicUsdmVenueClockRuntimeResultV9(
            promoting_plans=self.promoting_plans,
            scheduler_result=scheduler_result,
        )
        self._result = result
        return result

    async def _close_adapter_bounded(self) -> None:
        close_task = self._adapter_close_task
        if close_task is None:
            close_task = asyncio.create_task(
                self.adapter.aclose(),
                name="r4b-v9-venue-clock-runtime-adapter-close",
            )
            self._adapter_close_task = close_task
        if not await _wait_task_bounded(
            close_task,
            timeout_seconds=self.adapter_shutdown_timeout_seconds,
        ):
            close_task.cancel()
            if not await _wait_task_bounded(
                close_task,
                timeout_seconds=self.adapter_shutdown_timeout_seconds,
            ):
                raise PublicUsdmVenueClockRuntimeShutdownErrorV9(
                    "venue-clock adapter close exceeded two bounded waits"
                )
            raise PublicUsdmVenueClockRuntimeShutdownErrorV9(
                "venue-clock adapter close exceeded its first bounded wait"
            )
        if close_task.cancelled():
            raise PublicUsdmVenueClockRuntimeShutdownErrorV9(
                "venue-clock adapter close owner was cancelled"
            )
        close_task.result()
        if not self.adapter.cleanly_closed or not self.scheduler.drained:
            raise PublicUsdmVenueClockRuntimeShutdownErrorV9(
                "venue-clock producer lacks a clean closed-and-drained state"
            )


def _validate_usdm_venue_clock_runtime_bindings_v9(
    *,
    promoting_plans: tuple[ProvisionalPromotingPlanV9, ...],
    ingress: SharedWebSocketIngressV2,
    adapter: PublicUsdmVenueClockRestCaptureAdapterV9,
    scheduler: PublicUsdmVenueClockRestSchedulerV9,
) -> None:
    if type(promoting_plans) is not tuple:
        raise TypeError("venue-clock runtime requires an exact V9 plan tuple")
    validate_provisional_promoting_capture_plans_v9(promoting_plans)
    if type(ingress) is not SharedWebSocketIngressV2:
        raise TypeError("venue-clock runtime requires exact shared ingress")
    if type(adapter) is not PublicUsdmVenueClockRestCaptureAdapterV9:
        raise TypeError("venue-clock runtime requires the exact capture adapter")
    if type(scheduler) is not PublicUsdmVenueClockRestSchedulerV9:
        raise TypeError("venue-clock runtime requires the exact scheduler")
    clock_plan = _unique_clock_plan_v9(promoting_plans)
    if adapter.plan is not clock_plan or scheduler.plan is not clock_plan:
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "venue-clock runtime plan objects differ from its V9 bundle"
        )
    if scheduler.adapter is not adapter:
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "venue-clock scheduler does not own the admitted adapter"
        )
    if adapter.ingress is not ingress:
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "venue-clock adapter differs from the runtime shared ingress"
        )
    validate_public_usdm_venue_clock_schedule_authority_v9(
        scheduler.schedule_authority,
        plan=clock_plan,
    )
    if adapter.bound_schedule_authority is not scheduler.schedule_authority:
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "venue-clock adapter differs from scheduler authority"
        )
    if scheduler.started_once or scheduler.running or not scheduler.drained:
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "venue-clock scheduler already started before runtime ownership"
        )
    if (
        adapter.closed
        or not adapter.accepting_attempts
        or adapter.ownership_dirty
        or not adapter.fully_drained
    ):
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "venue-clock adapter is closed, dirty, failed, or already active"
        )
    if type(adapter.fatal_coordinator) is not PipelineRestCaptureFatalCoordinatorV2:
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "venue-clock runtime requires the production fatal pipeline bridge"
        )
    pipeline = adapter.fatal_coordinator.pipeline
    if type(pipeline) is not CaptureBatchPipelineV2 or ingress.pipeline is not pipeline:
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "venue-clock runtime does not share the exact capture pipeline"
        )
    pipeline.assert_live_runtime_authority_v2()
    writer = pipeline.writer
    if type(writer) is not DurableCaptureBatchWriterV2:
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "venue-clock runtime requires the exact durable capture writer"
        )
    authority = writer.wal_writer.authority
    if (
        authority.plan_sha256 != provisional_promoting_plan_sha256_v9(promoting_plans)
        or authority.protocol_sha256 != adapter.protocol_hash
    ):
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "venue-clock runtime differs from its durable V9 plan or protocol authority"
        )


def _unique_clock_plan_v9(
    plans: tuple[ProvisionalPromotingPlanV9, ...],
) -> ProvisionalUsdmVenueClockRestCapturePlanV9:
    clock_plans = tuple(
        plan for plan in plans if type(plan) is ProvisionalUsdmVenueClockRestCapturePlanV9
    )
    if len(clock_plans) != 1:
        raise PublicUsdmVenueClockRuntimeBindingErrorV9(
            "V9 runtime requires one exact venue-clock plan"
        )
    return clock_plans[0]
