from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path

import pytest

import signalbot.capture.cli as capture_cli
import signalbot.capture.live as capture_live
from signalbot.capture.config import CaptureCanaryConfig
from signalbot.capture.depth_sequence import (
    DepthRangeCallback,
    DepthRangeObservation,
    DepthResyncCallback,
    DepthResyncRequest,
)
from signalbot.capture.errors import CaptureIntegrityError
from signalbot.capture.handoff import CaptureFatalState
from signalbot.capture.live import (
    PUBLIC_NETWORK_CONFIRMATION,
    PreparedCaptureSession,
    prepare_capture_session,
    run_prepared_capture,
)
from signalbot.capture.models import RestEnvelopeV2
from signalbot.capture.pipeline import CapturePipeline
from signalbot.capture.receipts import IngestSequencer, ReceiptClock, ReceiptTimestamp
from signalbot.capture.rest_scheduler import RestAttemptCapture
from signalbot.capture.ws_owner import WebSocketOwnerSettings
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import WebSocketPlan

WORKSPACE = Path(__file__).parents[2]
CONFIG = WORKSPACE / "config" / "capture.r4b-canary-v1.yaml"
PROTOCOL = (
    WORKSPACE
    / "artifacts"
    / "oracle"
    / "2026-07-17"
    / "R4b_frozen_experiment_spec_v1.yaml"
)


class _Clock:
    def __init__(self) -> None:
        self._wall_ms = 1_800_000_000_000
        self._monotonic_ns = 9_000_000_000

    def capture(self) -> ReceiptTimestamp:
        self._wall_ms += 10
        self._monotonic_ns += 1_000
        return ReceiptTimestamp(self._wall_ms, self._monotonic_ns)


class _CallbackProducer:
    def __init__(
        self,
        callback: DepthResyncCallback,
        range_callback: DepthRangeCallback,
        request: DepthResyncRequest | None,
    ) -> None:
        self.callback = callback
        self.range_callback = range_callback
        self.request = request

    async def run(self, stop_event: asyncio.Event) -> None:
        if self.request is not None:
            for symbol, first_u in self.request.watermarks:
                self.range_callback(
                    DepthRangeObservation(
                        market=self.request.market,
                        symbol=symbol,
                        generation=self.request.generation,
                        U=first_u,
                        u=first_u,
                        reset=True,
                    )
                )
            self.callback(self.request)
        await stop_event.wait()


class _FailingProducer:
    async def run(self, stop_event: asyncio.Event) -> None:
        del stop_event
        raise RuntimeError("synthetic public producer failure")


class _OwnerFactory:
    def __init__(self, *, fail_first: bool = False) -> None:
        self.fail_first = fail_first
        self.plans: list[WebSocketPlan] = []
        self.clock_ids: list[int] = []
        self.sequencer_ids: list[int] = []
        self.pipeline_ids: list[int] = []
        self.callbacks: list[DepthResyncCallback] = []
        self.range_callbacks: list[DepthRangeCallback] = []

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
    ) -> _CallbackProducer | _FailingProducer:
        assert len(plan_sha256) == 64
        assert process_boot_id
        assert settings.maximum_connection_age_seconds == 86_100
        self.plans.append(plan)
        self.clock_ids.append(id(clock))
        self.sequencer_ids.append(id(sequencer))
        self.pipeline_ids.append(id(pipeline))
        self.callbacks.append(depth_resync_callback)
        self.range_callbacks.append(depth_range_callback)
        if self.fail_first and len(self.plans) == 1:
            return _FailingProducer()
        watermarks = tuple(
            sorted(
                (stream.split("@", 1)[0].upper(), 1)
                for stream in plan.streams
                if stream.endswith("@depth@100ms")
            )
        )
        return _CallbackProducer(
            depth_resync_callback,
            depth_range_callback,
            (
                DepthResyncRequest(
                    event="reconnect",
                    market=plan.market,
                    generation=1,
                    watermarks=watermarks,
                )
                if watermarks
                else None
            ),
        )


class _FakeAdapter:
    def __init__(
        self,
        *,
        pipeline: CapturePipeline,
        clock: ReceiptClock,
        sequencer: IngestSequencer,
    ) -> None:
        self.pipeline = pipeline
        self.clock = clock
        self.sequencer = sequencer
        self.closed = False

    async def capture_attempt(
        self,
        *,
        method: str,
        market: Market,
        url: str,
        request_role: str,
        correlation_id: str,
        attempt: int,
        query: Mapping[str, str] | Sequence[tuple[str, str]] = (),
        request_headers: Mapping[str, str] | None = None,
    ) -> RestEnvelopeV2:
        del (
            method,
            market,
            url,
            request_role,
            correlation_id,
            attempt,
            query,
            request_headers,
        )
        raise AssertionError("fake scheduler must not make a network attempt")

    async def aclose(self) -> None:
        self.closed = True


class _AdapterFactory:
    def __init__(self) -> None:
        self.adapter: _FakeAdapter | None = None

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
    ) -> _FakeAdapter:
        assert len(plan_sha256) == 64
        assert process_boot_id
        assert maximum_body_bytes == 16_777_216
        assert timeout_seconds == 15
        assert maximum_connections == 4
        self.adapter = _FakeAdapter(
            pipeline=pipeline,
            clock=clock,
            sequencer=sequencer,
        )
        return self.adapter


class _FakeScheduler:
    def __init__(self, fatal_state: CaptureFatalState, *, fail_callback: bool) -> None:
        self.fatal_state = fatal_state
        self.fail_callback = fail_callback
        self.depth_events: list[DepthResyncRequest] = []
        self.depth_ranges: list[DepthRangeObservation] = []

    async def run(self, stop_event: asyncio.Event) -> None:
        await stop_event.wait()

    def notify_depth_range(self, observation: DepthRangeObservation) -> None:
        self.depth_ranges.append(observation)

    def notify_depth_resync(
        self,
        request: DepthResyncRequest,
    ) -> None:
        if self.fail_callback:
            error = RuntimeError("synthetic depth resync queue overflow")
            self.fatal_state.trip_unbound(error)
            raise error
        self.depth_events.append(request)


class _SchedulerFactory:
    def __init__(self, *, fail_callback: bool = False) -> None:
        self.fail_callback = fail_callback
        self.adapter: RestAttemptCapture | None = None
        self.fatal_state: CaptureFatalState | None = None
        self.scheduler: _FakeScheduler | None = None

    def __call__(
        self,
        *,
        config: CaptureCanaryConfig,
        adapter: RestAttemptCapture,
        fatal_state: CaptureFatalState,
    ) -> _FakeScheduler:
        assert config.symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
        self.adapter = adapter
        self.fatal_state = fatal_state
        self.scheduler = _FakeScheduler(
            fatal_state,
            fail_callback=self.fail_callback,
        )
        return self.scheduler


def _roots(tmp_path: Path) -> tuple[Path, Path]:
    output = tmp_path / "capture-output"
    external = tmp_path / "external-audit"
    output.mkdir()
    external.mkdir()
    return output, external


def _prepare(
    tmp_path: Path,
    *,
    mode: str = "canary",
    duration_seconds: int = 86_400,
    clock: _Clock | None = None,
) -> PreparedCaptureSession:
    output, external = _roots(tmp_path)
    capture_mode = "smoke" if mode == "smoke" else "canary"
    return prepare_capture_session(
        workspace_root=WORKSPACE,
        config_file=CONFIG,
        protocol_file=PROTOCOL,
        output_base=output,
        external_audit_root=external,
        mode=capture_mode,
        duration_seconds=duration_seconds,
        receipt_clock=clock or _Clock(),
        boot_uuid=uuid.UUID("01234567-89ab-cdef-0123-456789abcdef"),
    )


def test_prepare_snapshots_source_twice_and_writes_exact_start_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    original = capture_live.build_capture_source_manifest

    def counted_manifest(
        workspace_root: str | Path,
        *,
        protocol_file: str | Path,
        config_file: str | Path,
    ):
        nonlocal calls
        calls += 1
        return original(
            workspace_root,
            protocol_file=protocol_file,
            config_file=config_file,
        )

    monkeypatch.setattr(capture_live, "build_capture_source_manifest", counted_manifest)
    output, external = _roots(tmp_path)
    prepared = prepare_capture_session(
        workspace_root=WORKSPACE,
        config_file=CONFIG,
        protocol_file=PROTOCOL,
        output_base=output,
        external_audit_root=external,
        mode="canary",
        duration_seconds=86_400,
        receipt_clock=_Clock(),
        boot_uuid=uuid.UUID("01234567-89ab-cdef-0123-456789abcdef"),
    )

    assert calls == 2
    assert prepared.session_directory.parent == output.resolve()
    assert prepared.session_directory.name == prepared.start.session_id
    assert prepared.segment_directory == prepared.session_directory / "segments"
    assert prepared.segment_directory.is_dir()
    source_bytes = prepared.source_manifest.path.read_bytes()
    assert hashlib.sha256(source_bytes).hexdigest() == prepared.source_manifest.sha256
    assert prepared.source_manifest.byte_count == len(source_bytes)
    assert source_bytes == original(
        WORKSPACE,
        protocol_file=PROTOCOL,
        config_file=CONFIG,
    ).canonical_bytes

    start_bytes = prepared.start_document.path.read_bytes()
    start_audit_bytes = prepared.start_audit.path.read_bytes()
    start_audit = json.loads(start_audit_bytes)
    assert hashlib.sha256(start_bytes).hexdigest() == prepared.start_document.sha256
    assert hashlib.sha256(start_audit_bytes).hexdigest() == prepared.start_audit.sha256
    assert start_audit["phase"] == "start"
    assert start_audit["subject_sha256"] == prepared.start_document.sha256
    assert start_audit["previous_record_sha256"] is None


@pytest.mark.asyncio
async def test_canary_run_uses_three_owners_one_clock_and_one_sequencer_then_closes(
    tmp_path: Path,
) -> None:
    clock = _Clock()
    prepared = _prepare(tmp_path, clock=clock)
    owners = _OwnerFactory()
    adapters = _AdapterFactory()
    schedulers = _SchedulerFactory()
    slept: list[float] = []

    async def finish_duration(seconds: float) -> None:
        slept.append(seconds)
        await asyncio.sleep(0)

    result = await run_prepared_capture(
        prepared,
        owner_factory=owners,
        rest_adapter_factory=adapters,
        rest_scheduler_factory=schedulers,
        duration_sleeper=finish_duration,
    )

    assert slept == [86_400.0]
    assert [plan.name for plan in owners.plans] == [
        "capture-spot-1",
        "capture-futures-market-1",
        "capture-futures-public-1",
    ]
    assert len(set(owners.clock_ids)) == 1
    assert owners.clock_ids == [id(clock)] * 3
    assert len(set(owners.sequencer_ids)) == 1
    assert len(set(owners.pipeline_ids)) == 1
    assert len({id(callback) for callback in owners.callbacks}) == 1
    assert len({id(callback) for callback in owners.range_callbacks}) == 1
    assert adapters.adapter is not None and adapters.adapter.closed
    assert id(adapters.adapter.clock) == id(clock)
    assert id(adapters.adapter.sequencer) == owners.sequencer_ids[0]
    assert schedulers.adapter is adapters.adapter
    assert schedulers.scheduler is not None
    assert schedulers.scheduler.depth_events == [
        DepthResyncRequest(
            event="reconnect",
            market=Market.SPOT,
            generation=1,
            watermarks=(
                ("BTCUSDT", 1),
                ("ETHUSDT", 1),
                ("SOLUSDT", 1),
            ),
        ),
        DepthResyncRequest(
            event="reconnect",
            market=Market.FUTURES,
            generation=1,
            watermarks=(
                ("BTCUSDT", 1),
                ("ETHUSDT", 1),
                ("SOLUSDT", 1),
            ),
        ),
    ]
    assert len(schedulers.scheduler.depth_ranges) == 6
    assert all(observation.reset for observation in schedulers.scheduler.depth_ranges)
    assert result.stop_reason == "completed_duration"
    assert result.classification == "CANARY_DURATION_COMPLETED_UNEVALUATED"

    closure_bytes = result.closure_document.path.read_bytes()
    closure = json.loads(closure_bytes)
    closure_audit_bytes = result.closure_audit.path.read_bytes()
    closure_audit = json.loads(closure_audit_bytes)
    assert closure["fatal"] is False
    assert closure["capture_chain"]["segment_count"] == 0
    assert closure_audit["subject_sha256"] == hashlib.sha256(closure_bytes).hexdigest()
    assert closure_audit["previous_record_sha256"] == prepared.start_audit.sha256
    assert _forbidden_result_keys(result.to_document()) == set()


@pytest.mark.asyncio
async def test_smoke_operator_stop_is_never_classified_as_canary_evidence(
    tmp_path: Path,
) -> None:
    prepared = _prepare(
        tmp_path,
        mode="smoke",
        duration_seconds=10,
    )
    operator_stop = asyncio.Event()
    operator_stop.set()

    async def wait_forever(_seconds: float) -> None:
        await asyncio.Event().wait()

    result = await run_prepared_capture(
        prepared,
        owner_factory=_OwnerFactory(),
        rest_adapter_factory=_AdapterFactory(),
        rest_scheduler_factory=_SchedulerFactory(),
        duration_sleeper=wait_forever,
        operator_stop_event=operator_stop,
    )

    assert result.stop_reason == "operator_requested"
    assert result.classification == "SMOKE_ONLY_NOT_CANARY_EVIDENCE"
    closure = json.loads(result.closure_document.path.read_bytes())
    assert closure["stop_reason"] == "operator_requested"
    assert closure["fatal"] is False
    assert "pass" not in json.dumps(result.to_document()).lower()


@pytest.mark.asyncio
async def test_producer_failure_cancels_peers_and_preserves_only_verified_fatal_closure(
    tmp_path: Path,
) -> None:
    prepared = _prepare(tmp_path)

    async def wait_forever(_seconds: float) -> None:
        await asyncio.Event().wait()

    with pytest.raises(RuntimeError, match="synthetic public producer failure"):
        await run_prepared_capture(
            prepared,
            owner_factory=_OwnerFactory(fail_first=True),
            rest_adapter_factory=_AdapterFactory(),
            rest_scheduler_factory=_SchedulerFactory(),
            duration_sleeper=wait_forever,
        )

    closure_path = prepared.session_directory / (
        f"{prepared.start.session_id}.closure.session.json"
    )
    closure = json.loads(closure_path.read_bytes())
    assert closure["fatal"] is True
    assert closure["stop_reason"] == "capture_failure"
    closure_audit_path = prepared.start_audit.path.with_name(
        f"{prepared.start.session_id}.closure.audit-head.json"
    )
    closure_audit = json.loads(closure_audit_path.read_bytes())
    assert closure_audit["previous_record_sha256"] == prepared.start_audit.sha256
    assert closure_audit["subject_sha256"] == hashlib.sha256(
        closure_path.read_bytes()
    ).hexdigest()


@pytest.mark.asyncio
async def test_depth_resync_callback_overflow_surfaces_shared_fatal_state(
    tmp_path: Path,
) -> None:
    prepared = _prepare(tmp_path)
    schedulers = _SchedulerFactory(fail_callback=True)

    async def wait_forever(_seconds: float) -> None:
        await asyncio.Event().wait()

    with pytest.raises(RuntimeError, match="synthetic depth resync queue overflow"):
        await run_prepared_capture(
            prepared,
            owner_factory=_OwnerFactory(),
            rest_adapter_factory=_AdapterFactory(),
            rest_scheduler_factory=schedulers,
            duration_sleeper=wait_forever,
        )

    assert schedulers.fatal_state is not None and schedulers.fatal_state.failed
    failure = schedulers.fatal_state.failure
    assert failure is not None
    assert str(failure.cause) == "synthetic depth resync queue overflow"
    closure_path = prepared.session_directory / (
        f"{prepared.start.session_id}.closure.session.json"
    )
    closure = json.loads(closure_path.read_bytes())
    assert closure["fatal"] is True
    assert closure["stop_reason"] == "capture_failure"


@pytest.mark.asyncio
async def test_preexisting_invalid_capture_refuses_network_and_any_closure(
    tmp_path: Path,
) -> None:
    prepared = _prepare(tmp_path)
    (prepared.segment_directory / "coverage-fatal.jsonl").write_text(
        "not a valid coverage record\n",
        encoding="utf-8",
    )

    owners = _OwnerFactory()
    adapters = _AdapterFactory()
    with pytest.raises(CaptureIntegrityError, match="empty segment directory"):
        await run_prepared_capture(
            prepared,
            owner_factory=owners,
            rest_adapter_factory=adapters,
            rest_scheduler_factory=_SchedulerFactory(),
        )

    assert owners.plans == []
    assert adapters.adapter is None
    assert not (
        prepared.session_directory / f"{prepared.start.session_id}.closure.session.json"
    ).exists()


@pytest.mark.asyncio
async def test_source_manifest_tamper_is_rejected_before_factories_or_network(
    tmp_path: Path,
) -> None:
    prepared = _prepare(tmp_path)
    with prepared.source_manifest.path.open("ab") as handle:
        handle.write(b" ")
    owners = _OwnerFactory()
    adapters = _AdapterFactory()

    with pytest.raises(CaptureIntegrityError, match="source manifest bytes changed"):
        await run_prepared_capture(
            prepared,
            owner_factory=owners,
            rest_adapter_factory=adapters,
            rest_scheduler_factory=_SchedulerFactory(),
        )

    assert owners.plans == []
    assert adapters.adapter is None
    assert not (
        prepared.session_directory / f"{prepared.start.session_id}.closure.session.json"
    ).exists()


@pytest.mark.parametrize("mode,duration", [("canary", 86_399), ("smoke", 9), ("smoke", 301)])
def test_prepare_rejects_nonfrozen_duration_boundaries(
    tmp_path: Path,
    mode: str,
    duration: int,
) -> None:
    output, external = _roots(tmp_path)
    capture_mode = "smoke" if mode == "smoke" else "canary"
    with pytest.raises(ValueError, match="duration"):
        prepare_capture_session(
            workspace_root=WORKSPACE,
            config_file=CONFIG,
            protocol_file=PROTOCOL,
            output_base=output,
            external_audit_root=external,
            mode=capture_mode,
            duration_seconds=duration,
        )
    assert list(output.iterdir()) == []
    assert list(external.iterdir()) == []


@pytest.mark.parametrize(
    "extra_arguments",
    [
        ["--confirm", PUBLIC_NETWORK_CONFIRMATION],
        ["--allow-public-network", "--confirm", "wrong-confirmation"],
    ],
)
def test_start_cli_rejects_missing_or_inexact_network_consent(
    tmp_path: Path,
    extra_arguments: list[str],
) -> None:
    output, external = _roots(tmp_path)
    arguments = [
        "start-smoke",
        "--workspace-root",
        str(WORKSPACE),
        "--config-file",
        str(CONFIG),
        "--protocol-file",
        str(PROTOCOL),
        "--output-base",
        str(output),
        "--external-audit-root",
        str(external),
        "--seconds",
        "10",
        *extra_arguments,
    ]
    with pytest.raises(SystemExit, match="2"):
        capture_cli.main(arguments)
    assert list(output.iterdir()) == []
    assert list(external.iterdir()) == []


def test_start_cli_runs_only_in_foreground_after_exact_consent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output, external = _roots(tmp_path)
    received: dict[str, object] = {}

    class _Result:
        def to_document(self) -> dict[str, object]:
            return {"schema_version": "capture_foreground_result_v1", "mode": "smoke"}

    async def fake_run_foreground_capture(**kwargs: object) -> _Result:
        received.update(kwargs)
        return _Result()

    monkeypatch.setattr(capture_cli, "run_foreground_capture", fake_run_foreground_capture)
    capture_cli.main(
        [
            "start-smoke",
            "--workspace-root",
            str(WORKSPACE),
            "--config-file",
            str(CONFIG),
            "--protocol-file",
            str(PROTOCOL),
            "--output-base",
            str(output),
            "--external-audit-root",
            str(external),
            "--seconds",
            "10",
            "--allow-public-network",
            "--confirm",
            PUBLIC_NETWORK_CONFIRMATION,
        ]
    )

    assert received["mode"] == "smoke"
    assert received["duration_seconds"] == 10
    assert json.loads(capsys.readouterr().out)["mode"] == "smoke"


def _forbidden_result_keys(value: object) -> set[str]:
    forbidden: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            lowered = key.lower()
            if any(token in lowered for token in ("signal", "pnl", "outcome", "order")):
                forbidden.add(key)
            forbidden.update(_forbidden_result_keys(item))
    elif isinstance(value, list):
        for item in value:
            forbidden.update(_forbidden_result_keys(item))
    return forbidden
