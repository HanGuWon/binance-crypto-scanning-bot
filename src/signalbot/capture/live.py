from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

from signalbot.capture.config import (
    FROZEN_PROTOCOL_SHA256,
    CaptureCanaryConfig,
    load_capture_canary_config,
)
from signalbot.capture.depth_sequence import (
    DepthRangeCallback,
    DepthRangeObservation,
    DepthResyncCallback,
    DepthResyncRequest,
)
from signalbot.capture.errors import CaptureIntegrityError, CaptureStorageCapacityError
from signalbot.capture.handoff import BoundedCaptureHandoff, CaptureFatalState
from signalbot.capture.path_safety import inspect_link_free_path
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.plans import build_prospective_capture_plans
from signalbot.capture.provenance import (
    ExternalAuditRecordV1,
    ExternalAuditWrite,
    build_capture_source_manifest,
    canonical_json_bytes,
    write_external_audit_record,
)
from signalbot.capture.receipts import (
    IngestSequencer,
    ReceiptClock,
    SystemReceiptClock,
)
from signalbot.capture.rest import PublicRestCaptureAdapter
from signalbot.capture.rest_scheduler import CanaryRestScheduler, RestAttemptCapture
from signalbot.capture.session import (
    SessionClosureV1,
    SessionDocumentWrite,
    SessionStartV1,
    StopReason,
    build_session_closure,
    build_session_start,
    generate_session_id,
    write_session_closure,
    write_session_start,
)
from signalbot.capture.storage import SegmentedCaptureWriter
from signalbot.capture.ws_owner import (
    CaptureReconnectExhausted,
    PublicWebSocketCaptureOwner,
    WebSocketOwnerSettings,
)
from signalbot.exchange.binance.endpoints import WebSocketPlan

PUBLIC_NETWORK_CONFIRMATION = "R4B_PUBLIC_DATA_ONLY_NO_ORDERS"
CANARY_DURATION_SECONDS = 86_400
MINIMUM_SMOKE_SECONDS = 10
MAXIMUM_SMOKE_SECONDS = 300
_PRODUCER_SHUTDOWN_TIMEOUT_SECONDS = 60.0

CaptureRunMode = Literal["canary", "smoke"]
TerminalCause = Literal["duration", "operator", "fatal"]
DurationSleeper = Callable[[float], Awaitable[None]]


class CaptureProducer(Protocol):
    async def run(self, stop_event: asyncio.Event) -> None: ...


class RestCaptureLifecycle(RestAttemptCapture, Protocol):
    async def aclose(self) -> None: ...


class RestSchedulerLifecycle(CaptureProducer, Protocol):
    def notify_depth_range(
        self,
        observation: DepthRangeObservation,
    ) -> None: ...

    def notify_depth_resync(
        self,
        request: DepthResyncRequest,
    ) -> None: ...


class WebSocketOwnerFactory(Protocol):
    def __call__(
        self,
        plan: WebSocketPlan,
        *,
        plan_sha256: str,
        process_boot_id: str,
        pipeline: CapturePipeline,
        clock: ReceiptClock,
        sequencer: IngestSequencer,
        settings: WebSocketOwnerSettings,
        depth_resync_callback: DepthResyncCallback,
        depth_range_callback: DepthRangeCallback,
    ) -> CaptureProducer: ...


class RestAdapterFactory(Protocol):
    def __call__(
        self,
        *,
        plan_sha256: str,
        process_boot_id: str,
        pipeline: CapturePipeline,
        clock: ReceiptClock,
        sequencer: IngestSequencer,
        maximum_body_bytes: int,
        timeout_seconds: float,
        maximum_connections: int,
    ) -> RestCaptureLifecycle: ...


class RestSchedulerFactory(Protocol):
    def __call__(
        self,
        *,
        config: CaptureCanaryConfig,
        adapter: RestAttemptCapture,
        fatal_state: CaptureFatalState,
    ) -> RestSchedulerLifecycle: ...


@dataclass(frozen=True, slots=True)
class ImmutableCaptureFile:
    path: Path
    sha256: str
    byte_count: int


@dataclass(frozen=True, slots=True)
class PreparedCaptureSession:
    mode: CaptureRunMode
    duration_seconds: int
    config: CaptureCanaryConfig
    receipt_clock: ReceiptClock
    session_directory: Path
    segment_directory: Path
    source_manifest: ImmutableCaptureFile
    start: SessionStartV1
    start_document: SessionDocumentWrite
    start_audit: ExternalAuditWrite


@dataclass(frozen=True, slots=True)
class CaptureRunResult:
    mode: CaptureRunMode
    duration_seconds: int
    classification: str
    stop_reason: StopReason
    session_directory: Path
    segment_directory: Path
    source_manifest_sha256: str
    start_document_sha256: str
    start_audit_sha256: str
    closure_document: SessionDocumentWrite
    closure_audit: ExternalAuditWrite

    def to_document(self) -> dict[str, object]:
        """Return an operational result with no efficacy or trading outputs."""

        return {
            "schema_version": "capture_foreground_result_v1",
            "purpose": "infrastructure_only",
            "mode": self.mode,
            "duration_seconds": self.duration_seconds,
            "classification": self.classification,
            "stop_reason": self.stop_reason,
            "session_directory": str(self.session_directory),
            "segment_directory": str(self.segment_directory),
            "source_manifest_sha256": self.source_manifest_sha256,
            "start_document_sha256": self.start_document_sha256,
            "start_audit_sha256": self.start_audit_sha256,
            "closure_document_sha256": self.closure_document.sha256,
            "closure_audit_sha256": self.closure_audit.sha256,
        }


def prepare_capture_session(
    *,
    workspace_root: str | Path,
    config_file: str | Path,
    protocol_file: str | Path,
    output_base: str | Path,
    external_audit_root: str | Path,
    mode: CaptureRunMode,
    duration_seconds: int,
    receipt_clock: ReceiptClock | None = None,
    boot_uuid: uuid.UUID | None = None,
) -> PreparedCaptureSession:
    """Create immutable pre-network authority for one unique foreground session."""

    _validate_duration(mode, duration_seconds)
    clock = SystemReceiptClock() if receipt_clock is None else receipt_clock
    output, external = _require_existing_capture_roots(output_base, external_audit_root)
    config = load_capture_canary_config(config_file, protocol_file=protocol_file)
    if config.duration_seconds != CANARY_DURATION_SECONDS:
        raise ValueError("frozen canary configuration duration is not exactly 86400 seconds")

    first_manifest = build_capture_source_manifest(
        workspace_root,
        protocol_file=protocol_file,
        config_file=config_file,
    )
    second_manifest = build_capture_source_manifest(
        workspace_root,
        protocol_file=protocol_file,
        config_file=config_file,
    )
    if first_manifest.canonical_bytes != second_manifest.canonical_bytes:
        raise CaptureIntegrityError(
            "capture source authority changed across consecutive canonical snapshots"
        )

    started = clock.capture()
    identity = uuid.uuid4() if boot_uuid is None else boot_uuid
    if not isinstance(identity, uuid.UUID):
        raise TypeError("boot_uuid must be a UUID")
    session_id = generate_session_id(started.received_at_ms, identity)
    session_directory = output / session_id
    segment_directory = session_directory / "segments"
    session_directory.mkdir(mode=0o700, exist_ok=False)
    segment_directory.mkdir(mode=0o700, exist_ok=False)
    _fsync_directory(output)
    _fsync_directory(session_directory)

    source_manifest = _write_immutable_bytes(
        session_directory / "capture-source-manifest.json",
        first_manifest.canonical_bytes,
    )
    if source_manifest.sha256 != first_manifest.sha256:
        raise CaptureIntegrityError("stored source manifest differs from canonical authority")
    config_sha256 = _manifest_configuration_sha256(first_manifest.document)
    start = build_session_start(
        protocol_sha256=FROZEN_PROTOCOL_SHA256,
        source_manifest_sha256=source_manifest.sha256,
        config_sha256=config_sha256,
        output_root=session_directory,
        external_audit_root=external,
        started_at_ms=started.received_at_ms,
        started_monotonic_ns=started.received_monotonic_ns,
        boot_uuid=identity,
    )
    if start.session_id != session_id:
        raise CaptureIntegrityError("prepared session identity differs from start authority")
    start_document = write_session_start(start, output_root=session_directory)
    start_audit = write_external_audit_record(
        ExternalAuditRecordV1(
            schema_version="capture_external_audit_record_v1",
            purpose="infrastructure_only",
            trust_classification="SEPARATE_PATH_AUDIT_ONLY",
            phase="start",
            session_id=start.session_id,
            recorded_at_ms=start.started_at_ms,
            protocol_sha256=start.protocol_sha256,
            source_manifest_sha256=start.source_manifest_sha256,
            subject_sha256=start_document.sha256,
            previous_record_sha256=None,
        ),
        external_root=external,
        output_root=session_directory,
    )
    return PreparedCaptureSession(
        mode=mode,
        duration_seconds=duration_seconds,
        config=config,
        receipt_clock=clock,
        session_directory=session_directory,
        segment_directory=segment_directory,
        source_manifest=source_manifest,
        start=start,
        start_document=start_document,
        start_audit=start_audit,
    )


async def run_prepared_capture(
    prepared: PreparedCaptureSession,
    *,
    owner_factory: WebSocketOwnerFactory | None = None,
    rest_adapter_factory: RestAdapterFactory | None = None,
    rest_scheduler_factory: RestSchedulerFactory | None = None,
    duration_sleeper: DurationSleeper = asyncio.sleep,
    operator_stop_event: asyncio.Event | None = None,
    producer_shutdown_timeout_seconds: float = _PRODUCER_SHUTDOWN_TIMEOUT_SECONDS,
) -> CaptureRunResult:
    """Run one foreground public-data session and close its authority chain."""

    if producer_shutdown_timeout_seconds <= 0:
        raise ValueError("producer shutdown timeout must be positive")
    _validate_prepared_runtime_authority(prepared)
    owner_builder = owner_factory or _default_owner_factory
    adapter_builder = rest_adapter_factory or _default_rest_adapter_factory
    scheduler_builder = rest_scheduler_factory or _default_rest_scheduler_factory

    fatal_state = CaptureFatalState()
    handoff = BoundedCaptureHandoff(
        max_events=prepared.config.handoff.maximum_events,
        max_bytes=prepared.config.handoff.maximum_encoded_bytes,
        fatal_state=fatal_state,
    )
    writer: SegmentedCaptureWriter | None = None
    pipeline: CapturePipeline | None = None
    sequencer = IngestSequencer()
    adapter: RestCaptureLifecycle | None = None
    producer_tasks: list[asyncio.Task[None]] = []
    pipeline_started = False
    terminal_cause: TerminalCause = "fatal"

    try:
        writer = SegmentedCaptureWriter(
            prepared.segment_directory,
            plan_sha256=prepared.start.capture_plan_sha256,
            process_boot_id=prepared.start.process_boot_id,
            rotation_interval_ms=prepared.config.storage.rotation_interval_ms,
            maximum_uncompressed_bytes=(
                prepared.config.storage.maximum_segment_uncompressed_bytes
            ),
            maximum_frames=prepared.config.storage.maximum_segment_websocket_frames,
            maximum_total_bytes=prepared.config.storage.maximum_total_bytes,
            emergency_reserve_bytes=prepared.config.storage.emergency_reserve_bytes,
            recover_partials=False,
        )
        pipeline = CapturePipeline(handoff, writer)
        plans = _authorized_websocket_plans(prepared)
        settings = _websocket_owner_settings(prepared.config)
        adapter = adapter_builder(
            plan_sha256=prepared.start.capture_plan_sha256,
            process_boot_id=prepared.start.process_boot_id,
            pipeline=pipeline,
            clock=prepared.receipt_clock,
            sequencer=sequencer,
            maximum_body_bytes=prepared.config.rest.maximum_body_bytes,
            timeout_seconds=float(prepared.config.rest.timeout_seconds),
            maximum_connections=prepared.config.rest.maximum_concurrency,
        )
        scheduler = scheduler_builder(
            config=prepared.config,
            adapter=adapter,
            fatal_state=fatal_state,
        )
        depth_resync_callback: DepthResyncCallback = scheduler.notify_depth_resync
        depth_range_callback: DepthRangeCallback = scheduler.notify_depth_range
        owners = tuple(
            owner_builder(
                plan,
                plan_sha256=prepared.start.capture_plan_sha256,
                process_boot_id=prepared.start.process_boot_id,
                pipeline=pipeline,
                clock=prepared.receipt_clock,
                sequencer=sequencer,
                settings=settings,
                depth_resync_callback=depth_resync_callback,
                depth_range_callback=depth_range_callback,
            )
            for plan in plans
        )
        if len(owners) != 3:
            raise CaptureIntegrityError("the frozen canary requires exactly three owners")
        pipeline.start()
        pipeline_started = True
        producer_tasks = [
            asyncio.create_task(
                owner.run(fatal_state.stop_event),
                name=f"capture-owner-{plan.name}",
            )
            for owner, plan in zip(owners, plans, strict=True)
        ]
        producer_tasks.append(
            asyncio.create_task(
                scheduler.run(fatal_state.stop_event),
                name="capture-rest-scheduler",
            )
        )
        try:
            terminal_cause = await _wait_for_terminal(
                duration_seconds=prepared.duration_seconds,
                duration_sleeper=duration_sleeper,
                operator_stop_event=operator_stop_event,
                fatal_state=fatal_state,
                producer_tasks=producer_tasks,
            )
        except asyncio.CancelledError:
            terminal_cause = "operator"
    except asyncio.CancelledError:
        terminal_cause = "operator"
    except Exception as exc:
        fatal_state.trip_unbound(exc)
        terminal_cause = "fatal"

    if fatal_state.failed or terminal_cause == "fatal":
        await _cancel_producers(producer_tasks)
    else:
        fatal_state.stop_event.set()
        producer_error = await _drain_producers(
            producer_tasks,
            timeout_seconds=producer_shutdown_timeout_seconds,
        )
        if producer_error is not None:
            fatal_state.trip_unbound(producer_error)

    if pipeline_started and pipeline is not None:
        try:
            await pipeline.stop()
        except Exception as exc:
            fatal_state.trip_unbound(exc)
    elif writer is not None:
        try:
            await asyncio.to_thread(writer.abort)
        except Exception as exc:
            fatal_state.trip_unbound(exc)

    if adapter is not None:
        try:
            await adapter.aclose()
        except Exception as exc:
            fatal_state.trip_unbound(exc)

    if fatal_state.failed:
        await _write_failure_closure(prepared, fatal_state)
        failure = fatal_state.failure
        assert failure is not None
        raise failure.cause

    stop_reason: StopReason
    if prepared.mode == "smoke" or terminal_cause == "operator":
        stop_reason = "operator_requested"
    else:
        stop_reason = "completed_duration"
    closure, closure_document, closure_audit = await _write_complete_closure(
        prepared,
        stop_reason=stop_reason,
        fatal=False,
    )
    return CaptureRunResult(
        mode=prepared.mode,
        duration_seconds=prepared.duration_seconds,
        classification=_success_classification(prepared.mode, terminal_cause),
        stop_reason=closure.stop_reason,
        session_directory=prepared.session_directory,
        segment_directory=prepared.segment_directory,
        source_manifest_sha256=prepared.source_manifest.sha256,
        start_document_sha256=prepared.start_document.sha256,
        start_audit_sha256=prepared.start_audit.sha256,
        closure_document=closure_document,
        closure_audit=closure_audit,
    )


async def run_foreground_capture(
    *,
    workspace_root: str | Path,
    config_file: str | Path,
    protocol_file: str | Path,
    output_base: str | Path,
    external_audit_root: str | Path,
    mode: CaptureRunMode,
    duration_seconds: int,
    receipt_clock: ReceiptClock | None = None,
    owner_factory: WebSocketOwnerFactory | None = None,
    rest_adapter_factory: RestAdapterFactory | None = None,
    rest_scheduler_factory: RestSchedulerFactory | None = None,
    duration_sleeper: DurationSleeper = asyncio.sleep,
    operator_stop_event: asyncio.Event | None = None,
) -> CaptureRunResult:
    """Prepare and run one foreground session; callers must gate network consent."""

    clock = SystemReceiptClock() if receipt_clock is None else receipt_clock
    prepared = prepare_capture_session(
        workspace_root=workspace_root,
        config_file=config_file,
        protocol_file=protocol_file,
        output_base=output_base,
        external_audit_root=external_audit_root,
        mode=mode,
        duration_seconds=duration_seconds,
        receipt_clock=clock,
    )
    return await run_prepared_capture(
        prepared,
        owner_factory=owner_factory,
        rest_adapter_factory=rest_adapter_factory,
        rest_scheduler_factory=rest_scheduler_factory,
        duration_sleeper=duration_sleeper,
        operator_stop_event=operator_stop_event,
    )


async def _wait_for_terminal(
    *,
    duration_seconds: int,
    duration_sleeper: DurationSleeper,
    operator_stop_event: asyncio.Event | None,
    fatal_state: CaptureFatalState,
    producer_tasks: Sequence[asyncio.Task[None]],
) -> TerminalCause:
    duration_task = asyncio.create_task(
        _sleep_duration(duration_sleeper, duration_seconds),
        name="capture-duration",
    )
    fatal_task = asyncio.create_task(
        fatal_state.failed_event.wait(),
        name="capture-fatal-wait",
    )
    operator_task = (
        None
        if operator_stop_event is None
        else asyncio.create_task(operator_stop_event.wait(), name="capture-operator-stop")
    )
    waiters: list[asyncio.Task[object]] = [duration_task, fatal_task]
    if operator_task is not None:
        waiters.append(operator_task)
    try:
        done, _pending = await asyncio.wait(
            [*producer_tasks, *waiters],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in producer_tasks:
            if task not in done:
                continue
            if task.cancelled():
                fatal_state.trip_unbound(
                    RuntimeError(f"capture producer was cancelled unexpectedly: {task.get_name()}")
                )
                continue
            error = task.exception()
            if error is None:
                error = RuntimeError(
                    f"capture producer stopped before the lifecycle boundary: {task.get_name()}"
                )
            fatal_state.trip_unbound(error)
        if fatal_state.failed or fatal_task in done:
            return "fatal"
        if duration_task in done:
            if duration_task.cancelled():
                fatal_state.trip_unbound(
                    RuntimeError("capture duration timer was cancelled unexpectedly")
                )
                return "fatal"
            duration_error = duration_task.exception()
            if duration_error is not None:
                fatal_state.trip_unbound(duration_error)
                return "fatal"
        if operator_task is not None and operator_task in done:
            return "operator"
        return "duration"
    finally:
        for task in waiters:
            if not task.done():
                task.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)


async def _drain_producers(
    tasks: Sequence[asyncio.Task[None]],
    *,
    timeout_seconds: float,
) -> BaseException | None:
    if not tasks:
        return None
    _done, pending = await asyncio.wait(tasks, timeout=timeout_seconds)
    if pending:
        for task in pending:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        return RuntimeError("capture producers did not stop within the bounded shutdown window")
    first_error: BaseException | None = None
    for task in tasks:
        if task.cancelled():
            first_error = first_error or RuntimeError(
                f"capture producer was cancelled during normal drain: {task.get_name()}"
            )
            continue
        error = task.exception()
        if error is not None and first_error is None:
            first_error = error
    return first_error


async def _sleep_duration(
    duration_sleeper: DurationSleeper,
    duration_seconds: int,
) -> None:
    await duration_sleeper(float(duration_seconds))


async def _cancel_producers(tasks: Sequence[asyncio.Task[None]]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _write_failure_closure(
    prepared: PreparedCaptureSession,
    fatal_state: CaptureFatalState,
) -> None:
    failure = fatal_state.failure
    assert failure is not None
    stop_reason = _fatal_stop_reason(failure.cause)
    try:
        await _write_complete_closure(prepared, stop_reason=stop_reason, fatal=True)
    except Exception as closure_error:
        failure.cause.add_note(
            "fatal capture closure was not preserved because verification or durable "
            f"closure failed: {closure_error!r}"
        )


async def _write_complete_closure(
    prepared: PreparedCaptureSession,
    *,
    stop_reason: StopReason,
    fatal: bool,
) -> tuple[SessionClosureV1, SessionDocumentWrite, ExternalAuditWrite]:
    closed = prepared.receipt_clock.capture()
    closure = await asyncio.to_thread(
        build_session_closure,
        start_path=prepared.start_document.path,
        capture_directory=prepared.segment_directory,
        stop_reason=stop_reason,
        fatal=fatal,
        closed_at_ms=closed.received_at_ms,
        closed_monotonic_ns=closed.received_monotonic_ns,
    )
    closure_document = await asyncio.to_thread(
        write_session_closure,
        closure,
        start_path=prepared.start_document.path,
        capture_directory=prepared.segment_directory,
    )
    closure_audit = await asyncio.to_thread(
        write_external_audit_record,
        ExternalAuditRecordV1(
            schema_version="capture_external_audit_record_v1",
            purpose="infrastructure_only",
            trust_classification="SEPARATE_PATH_AUDIT_ONLY",
            phase="closure",
            session_id=prepared.start.session_id,
            recorded_at_ms=closure.closed_at_ms,
            protocol_sha256=prepared.start.protocol_sha256,
            source_manifest_sha256=prepared.start.source_manifest_sha256,
            subject_sha256=closure_document.sha256,
            previous_record_sha256=prepared.start_audit.sha256,
        ),
        external_root=prepared.start.external_audit_root,
        output_root=prepared.session_directory,
    )
    return closure, closure_document, closure_audit


def _authorized_websocket_plans(
    prepared: PreparedCaptureSession,
) -> tuple[WebSocketPlan, WebSocketPlan, WebSocketPlan]:
    plans = build_prospective_capture_plans(
        prepared.config.symbols,
        batch_size=prepared.config.websocket.batch_size,
    )
    if len(plans) != 3:
        raise CaptureIntegrityError("the frozen public plan must contain exactly three sockets")
    actual = tuple(
        {
            "name": plan.name,
            "market": plan.market.value,
            "route": plan.route,
            "streams": plan.streams,
            "url": plan.url,
        }
        for plan in plans
    )
    expected = tuple(
        {
            "name": plan.name,
            "market": plan.market,
            "route": plan.route,
            "streams": plan.streams,
            "url": plan.url,
        }
        for plan in prepared.start.route_plan_summary.websocket_plans
    )
    if actual != expected:
        raise CaptureIntegrityError("runtime WebSocket plans differ from session authority")
    return plans


def _websocket_owner_settings(config: CaptureCanaryConfig) -> WebSocketOwnerSettings:
    settings = config.websocket
    return WebSocketOwnerSettings(
        maximum_connection_age_seconds=float(settings.maximum_connection_age_seconds),
        connect_timeout_seconds=float(settings.connect_timeout_seconds),
        close_timeout_seconds=float(settings.close_timeout_seconds),
        heartbeat_interval_seconds=float(settings.heartbeat_interval_seconds),
        pong_timeout_seconds=float(settings.pong_timeout_seconds),
        internal_queue_frames=settings.internal_queue_frames,
        maximum_frame_bytes=settings.maximum_frame_bytes,
        maximum_reconnect_attempts=settings.maximum_reconnect_attempts,
        reconnect_delays_seconds=tuple(map(float, settings.reconnect_delays_seconds)),
    )


def _default_owner_factory(
    plan: WebSocketPlan,
    *,
    plan_sha256: str,
    process_boot_id: str,
    pipeline: CapturePipeline,
    clock: ReceiptClock,
    sequencer: IngestSequencer,
    settings: WebSocketOwnerSettings,
    depth_resync_callback: DepthResyncCallback,
    depth_range_callback: DepthRangeCallback,
) -> CaptureProducer:
    return PublicWebSocketCaptureOwner(
        plan,
        plan_sha256=plan_sha256,
        process_boot_id=process_boot_id,
        pipeline=pipeline,
        clock=clock,
        sequencer=sequencer,
        settings=settings,
        depth_resync_callback=depth_resync_callback,
        depth_range_callback=depth_range_callback,
    )


def _default_rest_adapter_factory(
    *,
    plan_sha256: str,
    process_boot_id: str,
    pipeline: CapturePipeline,
    clock: ReceiptClock,
    sequencer: IngestSequencer,
    maximum_body_bytes: int,
    timeout_seconds: float,
    maximum_connections: int,
) -> RestCaptureLifecycle:
    return PublicRestCaptureAdapter(
        plan_sha256=plan_sha256,
        process_boot_id=process_boot_id,
        pipeline=pipeline,
        clock=clock,
        sequencer=sequencer,
        maximum_body_bytes=maximum_body_bytes,
        timeout_seconds=timeout_seconds,
        maximum_connections=maximum_connections,
    )


def _default_rest_scheduler_factory(
    *,
    config: CaptureCanaryConfig,
    adapter: RestAttemptCapture,
    fatal_state: CaptureFatalState,
) -> RestSchedulerLifecycle:
    return CanaryRestScheduler(
        config=config,
        adapter=adapter,
        fatal_state=fatal_state,
    )


def _validate_duration(mode: CaptureRunMode, duration_seconds: int) -> None:
    if mode not in {"canary", "smoke"}:
        raise ValueError("capture mode must be canary or smoke")
    if type(duration_seconds) is not int:
        raise ValueError("duration_seconds must be an integer")
    if mode == "canary" and duration_seconds != CANARY_DURATION_SECONDS:
        raise ValueError("the canary duration must be exactly 86400 seconds")
    if mode == "smoke" and not MINIMUM_SMOKE_SECONDS <= duration_seconds <= MAXIMUM_SMOKE_SECONDS:
        raise ValueError("smoke duration must be between 10 and 300 seconds")


def _require_existing_capture_roots(
    output_base: str | Path,
    external_audit_root: str | Path,
) -> tuple[Path, Path]:
    output_inspection = inspect_link_free_path(output_base, "output_base")
    external_inspection = inspect_link_free_path(
        external_audit_root,
        "external_audit_root",
    )
    if output_inspection.final_status is None or not output_inspection.absolute_path.is_dir():
        raise ValueError("output_base must be an existing directory")
    if (
        external_inspection.final_status is None
        or not external_inspection.absolute_path.is_dir()
    ):
        raise ValueError("external_audit_root must be an existing directory")
    output = output_inspection.absolute_path.resolve(strict=True)
    external = external_inspection.absolute_path.resolve(strict=True)
    if output == external or output.is_relative_to(external) or external.is_relative_to(output):
        raise ValueError("output_base and external_audit_root must be distinct and non-nested")
    return output, external


def _validate_prepared_runtime_authority(prepared: PreparedCaptureSession) -> None:
    """Recheck immutable preparation evidence before any network owner is built."""

    _validate_duration(prepared.mode, prepared.duration_seconds)
    session = inspect_link_free_path(
        prepared.session_directory,
        "prepared session directory",
    ).absolute_path.resolve(strict=True)
    segments = inspect_link_free_path(
        prepared.segment_directory,
        "prepared segment directory",
    ).absolute_path.resolve(strict=True)
    if not session.is_dir() or not segments.is_dir() or segments.parent != session:
        raise CaptureIntegrityError("prepared session directories are missing or inconsistent")
    if any(segments.iterdir()):
        raise CaptureIntegrityError("a prepared capture must start with an empty segment directory")
    if prepared.start.output_root != str(session) or prepared.start.session_id != session.name:
        raise CaptureIntegrityError("prepared session path differs from start authority")

    source_path = session / "capture-source-manifest.json"
    start_path = session / f"{prepared.start.session_id}.start.session.json"
    closure_path = session / f"{prepared.start.session_id}.closure.session.json"
    if prepared.source_manifest.path.resolve(strict=True) != source_path:
        raise CaptureIntegrityError("prepared source manifest path differs from authority")
    if prepared.start_document.path.resolve(strict=True) != start_path:
        raise CaptureIntegrityError("prepared start document path differs from authority")
    if closure_path.exists():
        raise CaptureIntegrityError("prepared session already has a closure document")
    if (
        prepared.source_manifest.byte_count != source_path.stat().st_size
        or _sha256_path(source_path) != prepared.source_manifest.sha256
        or prepared.source_manifest.sha256 != prepared.start.source_manifest_sha256
    ):
        raise CaptureIntegrityError("prepared source manifest bytes changed before run")

    expected_start = canonical_json_bytes(prepared.start.model_dump(mode="json")) + b"\n"
    actual_start = start_path.read_bytes()
    if (
        actual_start != expected_start
        or hashlib.sha256(actual_start).hexdigest() != prepared.start_document.sha256
        or len(actual_start) != prepared.start_document.byte_count
    ):
        raise CaptureIntegrityError("prepared start document changed before run")

    expected_audit_path = Path(prepared.start.external_audit_root) / (
        f"{prepared.start.session_id}.start.audit-head.json"
    )
    if prepared.start_audit.path.resolve(strict=True) != expected_audit_path:
        raise CaptureIntegrityError("prepared start audit path differs from authority")
    audit_bytes = expected_audit_path.read_bytes()
    try:
        audit = ExternalAuditRecordV1.model_validate(json.loads(audit_bytes))
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as exc:
        raise CaptureIntegrityError("prepared start audit is invalid") from exc
    if audit_bytes != canonical_json_bytes(audit.model_dump(mode="json")) + b"\n":
        raise CaptureIntegrityError("prepared start audit is not canonical")
    if (
        audit.phase != "start"
        or audit.session_id != prepared.start.session_id
        or audit.subject_sha256 != prepared.start_document.sha256
        or audit.source_manifest_sha256 != prepared.start.source_manifest_sha256
        or hashlib.sha256(audit_bytes).hexdigest() != prepared.start_audit.sha256
        or len(audit_bytes) != prepared.start_audit.byte_count
    ):
        raise CaptureIntegrityError("prepared start audit changed before run")


def _write_immutable_bytes(path: Path, payload: bytes) -> ImmutableCaptureFile:
    inspect_link_free_path(path, "immutable capture file", allow_missing_tail=True)
    with path.open("xb", buffering=0) as handle:
        view = memoryview(payload)
        total = 0
        while total < len(view):
            written = handle.write(view[total:])
            if written is None or written <= 0:
                raise OSError("immutable capture file write made no progress")
            total += written
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_directory(path.parent)
    return ImmutableCaptureFile(
        path=path,
        sha256=hashlib.sha256(payload).hexdigest(),
        byte_count=len(payload),
    )


def _manifest_configuration_sha256(document: Mapping[str, object]) -> str:
    configuration = document.get("configuration")
    if not isinstance(configuration, Mapping):
        raise CaptureIntegrityError("source manifest lacks configuration authority")
    digest = configuration.get("sha256")
    if not isinstance(digest, str) or len(digest) != 64:
        raise CaptureIntegrityError("source manifest configuration hash is invalid")
    return digest


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fatal_stop_reason(cause: BaseException) -> StopReason:
    if isinstance(cause, CaptureStorageCapacityError):
        return "capacity_exhausted"
    if isinstance(cause, CaptureReconnectExhausted):
        return "network_retry_exhausted"
    if isinstance(cause, CaptureIntegrityError) and "receipt time moved backwards" in str(cause):
        return "clock_discontinuity"
    return "capture_failure"


def _success_classification(mode: CaptureRunMode, terminal: TerminalCause) -> str:
    if mode == "smoke":
        return "SMOKE_ONLY_NOT_CANARY_EVIDENCE"
    if terminal == "operator":
        return "CANARY_STOPPED_BY_OPERATOR_UNEVALUATED"
    return "CANARY_DURATION_COMPLETED_UNEVALUATED"


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
