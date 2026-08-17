from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any, cast
from unittest.mock import patch

import httpx
import pytest

import signalbot.r4b_v2.capture.closed_session_owner as closed_session_owner_module
from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.capture.ws_owner import Connector, PublicWebSocketCaptureOwner
from signalbot.r4b_v2.capture.closed_session_owner import (
    PublicCaptureClosedSessionOwnerErrorV8,
    PublicCaptureClosedSessionOwnerStateErrorV8,
    PublicCaptureClosedSessionOwnerV2,
    PublicCaptureClosedSessionOwnerV8,
    PublicCaptureClosedSessionResultV8,
    canonical_public_capture_closed_session_result_v2,
    canonical_public_capture_closed_session_result_v8,
)
from signalbot.r4b_v2.capture.full_runtime import (
    PublicCaptureRuntimeBindingErrorV8,
    PublicCaptureRuntimeResultV8,
    PublicCaptureRuntimeShutdownErrorV8,
    PublicCaptureRuntimeV8,
    _validate_depth_bridge_close_bounds_v8,
)
from signalbot.r4b_v2.capture.integrity_ledger import CaptureIntegrityLedgerV2
from signalbot.r4b_v2.capture.pipeline import CaptureBatchPipelineV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV8,
    build_provisional_promoting_capture_plans_v8,
    provisional_promoting_plan_sha256_v8,
)
from signalbot.r4b_v2.capture.rest_adapter import (
    PipelineRestCaptureFatalCoordinatorV2,
)
from signalbot.r4b_v2.capture.rest_depth_bridge import (
    PublicDepthRestBridgeCoordinatorV8,
)
from signalbot.r4b_v2.capture.rest_depth_bridge_evidence import (
    DepthBridgeCoordinatorCleanCloseReceiptV8,
    DepthBridgeEvidencePayloadV8,
    DepthBridgePhaseV8,
)
from signalbot.r4b_v2.capture.session import (
    canonical_session_closure_manifest_path_v8,
)
from signalbot.r4b_v2.capture.websocket import (
    build_public_websocket_owner_plan_v2,
)
from signalbot.r4b_v2.capture.websocket_composition import (
    PublicWebSocketOwnerCompositionV8,
    PublicWebSocketRuntimeClaimErrorV8,
    PublicWebSocketRuntimeStartBarrierV8,
    create_public_websocket_frame_adapter_factory_v8,
    create_public_websocket_owner_composition_v8,
)
from signalbot.r4b_v2.capture.websocket_finality import (
    _issue_websocket_route_stop_receipt_v8,
)
from signalbot.r4b_v2.capture.websocket_lifecycle import (
    WebSocketLifecycleFatalCoordinatorV8,
)


def _load_v2_fixture_module() -> ModuleType:
    module_name = "_signalbot_test_full_runtime_fixture_v2"
    path = Path(__file__).with_name("test_full_runtime.py")
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load the adjacent V2 runtime fixture")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


v2_fixture_module = _load_v2_fixture_module()
_V2FixtureBase = cast(Any, v2_fixture_module._Fixture)
class _V8Fixture(_V2FixtureBase):  # pyright: ignore[reportInvalidTypeForm]
    def __init__(
        self,
        tmp_path,
        *,
        public_exit_gate: asyncio.Event | None = None,
        process_boot_id: str = "0123456789abcdef0123456789abcdef",
    ) -> None:
        plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT",))
        self.v8_plans = plans
        self.depth_plan = cast(ProvisionalDepthRestQualificationPlanV8, plans[3])
        self.depth_requests: list[httpx.Request] = []

        def selected_plans(
            symbols: tuple[str, ...],
        ) -> tuple[ProvisionalPromotingPlanV8, ...]:
            assert symbols == ("BTCUSDT",)
            return plans

        def selected_plan_hash(actual_plans: object) -> str:
            assert actual_plans is plans
            return provisional_promoting_plan_sha256_v8(plans)

        def shared_fatal(
            pipeline: CaptureBatchPipelineV2,
        ) -> PipelineRestCaptureFatalCoordinatorV2:
            fatal = getattr(self, "fatal_coordinator", None)
            if fatal is None:
                fatal = PipelineRestCaptureFatalCoordinatorV2(pipeline)
                self.fatal_coordinator = fatal
            assert fatal.pipeline is pipeline
            return fatal

        with (
            patch.object(
                v2_fixture_module,
                "build_provisional_promoting_capture_plans_v2",
                selected_plans,
            ),
            patch.object(
                v2_fixture_module,
                "provisional_promoting_plan_sha256_v2",
                selected_plan_hash,
            ),
            patch.object(
                v2_fixture_module,
                "PipelineRestCaptureFatalCoordinatorV2",
                shared_fatal,
            ),
        ):
            super().__init__(
                tmp_path,
                public_exit_gate=public_exit_gate,
                market_frames=(v2_fixture_module._valid_agg_trade_frame(),),
                public_frames=(v2_fixture_module._valid_depth_frame(),),
                process_boot_id=process_boot_id,
            )

    def _composition(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        plan: ProvisionalPromotingCapturePlanV2,
        connector: object,
    ) -> PublicWebSocketOwnerCompositionV8:
        fatal = getattr(self, "fatal_coordinator", None)
        if fatal is None:
            fatal = PipelineRestCaptureFatalCoordinatorV2(self.pipeline)
            self.fatal_coordinator = fatal
        lifecycle = WebSocketLifecycleFatalCoordinatorV8(
            self.v8_plans,
            plan,
            session_id=self.session_id,
            process_boot_id=self.process_boot_id,
            session_started_at=ReceiptTimestamp(
                self.started_wall_ms,
                self.started_monotonic_ns,
            ),
            source_component=f"v8-owner-{plan.route_id}",
            clock=self.receipt_clock,
            pipeline=self.pipeline,
            integrity_ledger=self.ledger,
            finality_timeout_seconds=2.0,
        )
        factory = create_public_websocket_frame_adapter_factory_v8(
            plan,
            session_id=self.session_id,
            protocol_hash=v2_fixture_module.HASH,
            clock=self.receipt_clock,
            ingress=self.ingress,
            recovery_lifecycle=lifecycle,
        )
        if plan.route_id == "usdm_public":
            bridge = PublicDepthRestBridgeCoordinatorV8(
                self.v8_plans,
                self.depth_plan,
                ingress=self.ingress,
                clock=self.receipt_clock,
                fatal_coordinator=fatal,
                ledger=self.ledger,
                transport_factory=(
                    lambda: httpx.MockTransport(self._depth_response)
                ),
            )
            owner = PublicWebSocketCaptureOwner(
                build_public_websocket_owner_plan_v2(plan),
                plan_sha256=self.authority.plan_sha256,
                process_boot_id=self.process_boot_id,
                settings=v2_fixture_module._settings(),
                connector=cast(Connector, connector),
                frame_adapter_factory=factory,
                lifecycle_coordinator=lifecycle,
                preconnecting_generation_hook=bridge,
                retained_depth_range_callback=(
                    bridge.retained_depth_range_callback
                ),
                retained_depth_resync_callback=(
                    bridge.retained_depth_resync_callback
                ),
            )
            bridge.bind_websocket_owner(owner)
            self.depth_bridge = bridge
        else:
            owner = PublicWebSocketCaptureOwner(
                build_public_websocket_owner_plan_v2(plan),
                plan_sha256=self.authority.plan_sha256,
                process_boot_id=self.process_boot_id,
                settings=v2_fixture_module._settings(),
                connector=cast(Connector, connector),
                frame_adapter_factory=factory,
                lifecycle_coordinator=lifecycle,
            )
        return create_public_websocket_owner_composition_v8(
            session_start_authority=self.start_authority,
            writer_lease=self.lease,
            promoting_plans=self.v8_plans,
            plan=plan,
            recovered_wal_tail_ingest_seq=0,
            owner=owner,
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
        )

    async def _depth_response(self, request: httpx.Request) -> httpx.Response:
        self.depth_requests.append(request)
        return httpx.Response(
            200,
            content=json.dumps(
                {
                    "lastUpdateId": 1,
                    "E": 2_000,
                    "T": 1_999,
                    "bids": [["65000", "1"]],
                    "asks": [["65001", "1"]],
                },
                separators=(",", ":"),
            ).encode(),
            request=request,
        )

    def runtime_v8(
        self,
        *,
        producer_timeout: float = 2.0,
        finality_timeout: float = 2.0,
    ) -> PublicCaptureRuntimeV8:
        compositions = cast(
            tuple[
                PublicWebSocketOwnerCompositionV8,
                PublicWebSocketOwnerCompositionV8,
            ],
            (self.market_composition, self.public_composition),
        )
        return PublicCaptureRuntimeV8(
            compositions,
            self.rest_adapter,
            self.scheduler,
            self.depth_bridge,
            producer_shutdown_timeout_seconds=producer_timeout,
            finality_timeout_seconds=finality_timeout,
        )

    async def close(self) -> None:
        try:
            await self.depth_bridge.aclose()
        except BaseException:
            pass
        await super().close()


async def _wait_until_ready(
    fixture: _V8Fixture,
    run_task: asyncio.Task[object],
) -> None:
    await v2_fixture_module._wait_until_ready(fixture, run_task)
    for _attempt in range(2_000):
        if fixture.depth_requests and fixture.depth_bridge.worker_count == 0:
            return
        await asyncio.sleep(0.001)
    raise AssertionError("V8 runtime fixture did not settle its startup bridge")


async def _run_normal_v8(
    fixture: _V8Fixture,
) -> tuple[PublicCaptureRuntimeV8, PublicCaptureRuntimeResultV8]:
    runtime = fixture.runtime_v8()
    run_task = asyncio.create_task(runtime.run())
    await _wait_until_ready(fixture, run_task)
    await runtime.request_normal_stop()
    result = await run_task
    return runtime, result


@pytest.mark.asyncio
async def test_v8_normal_stop_has_three_producers_and_bridge_before_finality(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)
    order: list[str] = []
    runtime = fixture.runtime_v8()
    original_close = PublicDepthRestBridgeCoordinatorV8.aclose
    original_finality = CaptureBatchPipelineV2.finalize_current_tail_and_stop

    async def observed_close(
        bridge: PublicDepthRestBridgeCoordinatorV8,
    ) -> DepthBridgeCoordinatorCleanCloseReceiptV8 | None:
        if bridge is fixture.depth_bridge:
            assert all(task.done() for task in runtime._producer_tasks)
            order.append("bridge")
        return await original_close(bridge)

    async def observed_finality(
        pipeline: CaptureBatchPipelineV2,
        *,
        timeout_seconds: float,
    ):
        if pipeline is fixture.pipeline:
            assert fixture.depth_bridge.generation_open is False
            assert fixture.depth_bridge.worker_count == 0
            assert fixture.depth_bridge.permit_in_use_count == 0
            order.append("finality")
        return await original_finality(
            pipeline,
            timeout_seconds=timeout_seconds,
        )

    monkeypatch.setattr(
        PublicDepthRestBridgeCoordinatorV8,
        "aclose",
        observed_close,
    )
    monkeypatch.setattr(
        CaptureBatchPipelineV2,
        "finalize_current_tail_and_stop",
        observed_finality,
    )
    try:
        assert runtime.producer_task_count == 0
        assert fixture.market_connector.urls == []
        assert fixture.public_connector.urls == []
        assert fixture.rest_requests == []
        assert fixture.depth_requests == []

        with pytest.raises(PublicWebSocketRuntimeClaimErrorV8, match="direct run"):
            await fixture.market_composition.run()

        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)
        assert runtime.producer_task_count == 3
        await runtime.request_normal_stop()
        result = await run_task

        assert order == ["bridge", "finality"]
        assert result.producer_task_count == 3
        assert result.promoting_plans is fixture.v8_plans
        receipt = result.depth_bridge_close_receipt
        assert receipt is fixture.depth_bridge.clean_close_receipt
        public_stop = result.websocket_route_cursors[1].stop_receipt
        assert receipt.session_id == public_stop.session_id
        assert receipt.last_connection_id == public_stop.connection_id
        assert receipt.last_connection_generation == public_stop.generation
        assert (
            public_stop.stop_observed_monotonic_ns
            <= receipt.close_monotonic_ns
            <= result.finality_receipt.fence_monotonic_ns
            <= result.finality_receipt.writer_observed_monotonic_ns
        )
        assert public_stop.stop_observed_wall_ms <= receipt.close_wall_ms
        assert (
            receipt.last_generation_drained_recorded_monotonic_ns
            <= receipt.close_monotonic_ns
        )
        assert (
            receipt.last_generation_drained_recorded_wall_ms
            <= receipt.close_wall_ms
        )
        assert await fixture.depth_bridge.aclose() is receipt
        assert result.depth_bridge_complete_claimed is False
        assert result.oi_data_completeness_claimed is False
        assert result.observed_source_completeness_claimed is False
        assert result.m2_eligible is False
        assert result.order_execution_enabled is False
        assert result.production_order_execution_enabled is False
        assert result.local_session_closure_issued is False
        assert result.integrity_ledger_clean_issued is False
        assert fixture.depth_bridge.generation_open is False
        assert fixture.depth_bridge.worker_count == 0
        assert fixture.depth_bridge.permit_in_use_count == 0
        assert fixture.depth_bridge.adapter is None
        assert len(fixture.market_connector.urls) == 1
        assert len(fixture.public_connector.urls) == 1
        assert len(fixture.rest_requests) == 1
        assert len(fixture.depth_requests) == 1
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_constructor_rolls_back_first_claim_and_double_claim_has_no_io(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)
    original_claim = PublicWebSocketOwnerCompositionV8.claim_exclusive_runtime_v8

    def fail_public_claim(
        composition: PublicWebSocketOwnerCompositionV8,
        runtime_owner: object,
    ):
        if composition is fixture.public_composition:
            raise RuntimeError("injected public claim failure")
        return original_claim(composition, runtime_owner)

    try:
        monkeypatch.setattr(
            PublicWebSocketOwnerCompositionV8,
            "claim_exclusive_runtime_v8",
            fail_public_claim,
        )
        with pytest.raises(RuntimeError, match="injected public claim failure"):
            fixture.runtime_v8()

        monkeypatch.setattr(
            PublicWebSocketOwnerCompositionV8,
            "claim_exclusive_runtime_v8",
            original_claim,
        )
        runtime = fixture.runtime_v8()
        with pytest.raises(PublicWebSocketRuntimeClaimErrorV8):
            fixture.runtime_v8()

        assert not runtime.started_once
        assert runtime.producer_task_count == 0
        assert fixture.market_connector.urls == []
        assert fixture.public_connector.urls == []
        assert fixture.rest_requests == []
        assert fixture.depth_requests == []
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_constructor_rejects_session_and_bridge_fatal_cross_binding(
    tmp_path,
) -> None:
    first = _V8Fixture(tmp_path / "session")
    try:
        first.rest_adapter.session_id = "foreign-session"
        with pytest.raises(PublicCaptureRuntimeBindingErrorV8, match="session IDs"):
            first.runtime_v8()
        assert first.market_connector.urls == []
        assert first.public_connector.urls == []
        assert first.rest_requests == []
        assert first.depth_requests == []
    finally:
        await first.close()

    second = _V8Fixture(tmp_path / "fatal")
    try:
        second.rest_adapter.fatal_coordinator = (
            PipelineRestCaptureFatalCoordinatorV2(second.pipeline)
        )
        with pytest.raises(ValueError, match="fatal coordinator"):
            second.runtime_v8()
        assert second.market_connector.urls == []
        assert second.public_connector.urls == []
        assert second.rest_requests == []
        assert second.depth_requests == []
    finally:
        await second.close()


@pytest.mark.asyncio
async def test_v8_producer_failure_reaps_ws_before_fatal_bridge_abort(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)
    runtime = fixture.runtime_v8()
    original_run = PublicWebSocketOwnerCompositionV8.run_exclusive_runtime_v8
    original_abort = PublicDepthRestBridgeCoordinatorV8.abort_and_drain
    abort_ws_terminal: list[bool] = []

    async def fail_market(
        composition: PublicWebSocketOwnerCompositionV8,
        token,
        *,
        runtime_owner: object,
        startup_barrier: PublicWebSocketRuntimeStartBarrierV8 | None = None,
    ):
        if composition is fixture.market_composition:
            raise RuntimeError("injected V8 producer failure")
        return await original_run(
            composition,
            token,
            runtime_owner=runtime_owner,
            startup_barrier=startup_barrier,
        )

    async def observed_abort(
        bridge: PublicDepthRestBridgeCoordinatorV8,
        cause: BaseException,
    ) -> None:
        if bridge is fixture.depth_bridge:
            abort_ws_terminal.append(
                all(task.done() for task in runtime._producer_tasks[:2])
            )
        await original_abort(bridge, cause)

    monkeypatch.setattr(
        PublicWebSocketOwnerCompositionV8,
        "run_exclusive_runtime_v8",
        fail_market,
    )
    monkeypatch.setattr(
        PublicDepthRestBridgeCoordinatorV8,
        "abort_and_drain",
        observed_abort,
    )
    try:
        with pytest.raises(RuntimeError, match="injected V8 producer failure"):
            await runtime.run()

        assert runtime.result is None
        assert abort_ws_terminal == [True]
        assert all(task.done() for task in runtime._producer_tasks)
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
        assert runtime._bridge_abort_task is not None
        assert runtime._bridge_abort_task.done()
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_depth_ledger_failure_forbids_finality_result(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)
    runtime = fixture.runtime_v8()
    original_append = CaptureIntegrityLedgerV2.append_depth_bridge_v8

    def fail_generation_start(
        ledger: CaptureIntegrityLedgerV2,
        payload: DepthBridgeEvidencePayloadV8,
        promoting_plans: tuple[ProvisionalPromotingPlanV8, ...],
        depth_plan: ProvisionalDepthRestQualificationPlanV8,
    ):
        if (
            ledger is fixture.ledger
            and payload.phase == DepthBridgePhaseV8.GENERATION_STARTED.value
        ):
            raise RuntimeError("injected V8 depth ledger failure")
        return original_append(
            ledger,
            payload,
            promoting_plans,
            depth_plan,
        )

    monkeypatch.setattr(
        CaptureIntegrityLedgerV2,
        "append_depth_bridge_v8",
        fail_generation_start,
    )
    try:
        with pytest.raises(RuntimeError, match="injected V8 depth ledger failure"):
            await runtime.run()

        assert runtime.result is None
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
        assert all(task.done() for task in runtime._producer_tasks)
        assert runtime._bridge_abort_task is not None
        assert runtime._bridge_abort_task.done()
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_cancellation_during_bridge_close_switches_to_fatal_abort(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)
    runtime = fixture.runtime_v8()
    close_entered = asyncio.Event()
    release_close = asyncio.Event()
    original_close = PublicDepthRestBridgeCoordinatorV8.aclose
    original_abort = PublicDepthRestBridgeCoordinatorV8.abort_and_drain
    abort_ws_terminal: list[bool] = []

    async def blocked_close(
        bridge: PublicDepthRestBridgeCoordinatorV8,
    ) -> DepthBridgeCoordinatorCleanCloseReceiptV8 | None:
        if bridge is fixture.depth_bridge:
            close_entered.set()
            await release_close.wait()
        return await original_close(bridge)

    async def observed_abort(
        bridge: PublicDepthRestBridgeCoordinatorV8,
        cause: BaseException,
    ) -> None:
        if bridge is fixture.depth_bridge:
            abort_ws_terminal.append(
                all(task.done() for task in runtime._producer_tasks[:2])
            )
        await original_abort(bridge, cause)

    monkeypatch.setattr(
        PublicDepthRestBridgeCoordinatorV8,
        "aclose",
        blocked_close,
    )
    monkeypatch.setattr(
        PublicDepthRestBridgeCoordinatorV8,
        "abort_and_drain",
        observed_abort,
    )
    try:
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)
        await runtime.request_normal_stop()
        await close_entered.wait()
        run_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await run_task

        assert runtime.result is None
        assert abort_ws_terminal == [True]
        assert runtime._bridge_close_task is not None
        assert runtime._bridge_close_task.cancelled()
        assert runtime._bridge_abort_task is not None
        assert runtime._bridge_abort_task.done()
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
    finally:
        release_close.set()
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_bridge_close_timeout_forbids_finality_and_aborts_after_ws(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)
    runtime = fixture.runtime_v8(producer_timeout=1.0)
    close_entered = asyncio.Event()
    release_close = asyncio.Event()
    original_close = PublicDepthRestBridgeCoordinatorV8.aclose
    original_abort = PublicDepthRestBridgeCoordinatorV8.abort_and_drain
    abort_ws_terminal: list[bool] = []

    async def blocked_close(
        bridge: PublicDepthRestBridgeCoordinatorV8,
    ) -> DepthBridgeCoordinatorCleanCloseReceiptV8 | None:
        if bridge is fixture.depth_bridge:
            close_entered.set()
            await release_close.wait()
        return await original_close(bridge)

    async def observed_abort(
        bridge: PublicDepthRestBridgeCoordinatorV8,
        cause: BaseException,
    ) -> None:
        if bridge is fixture.depth_bridge:
            abort_ws_terminal.append(
                all(task.done() for task in runtime._producer_tasks[:2])
            )
        await original_abort(bridge, cause)

    monkeypatch.setattr(
        PublicDepthRestBridgeCoordinatorV8,
        "aclose",
        blocked_close,
    )
    monkeypatch.setattr(
        PublicDepthRestBridgeCoordinatorV8,
        "abort_and_drain",
        observed_abort,
    )
    try:
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)
        await runtime.request_normal_stop()
        await close_entered.wait()

        with pytest.raises(PublicCaptureRuntimeShutdownErrorV8, match="bridge close"):
            await run_task

        assert runtime.result is None
        assert abort_ws_terminal == [True]
        assert runtime._bridge_close_task is not None
        assert runtime._bridge_close_task.cancelled()
        assert runtime._bridge_abort_task is not None
        assert runtime._bridge_abort_task.done()
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
    finally:
        release_close.set()
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_bridge_abort_failure_never_mints_finality_result(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)
    runtime = fixture.runtime_v8()
    abort_ws_terminal: list[bool] = []

    async def fail_abort(
        bridge: PublicDepthRestBridgeCoordinatorV8,
        cause: BaseException,
    ) -> None:
        del cause
        if bridge is fixture.depth_bridge:
            abort_ws_terminal.append(
                all(task.done() for task in runtime._producer_tasks[:2])
            )
        raise RuntimeError("injected V8 bridge abort failure")

    monkeypatch.setattr(
        PublicDepthRestBridgeCoordinatorV8,
        "abort_and_drain",
        fail_abort,
    )
    try:
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)
        run_task.cancel()

        with pytest.raises(asyncio.CancelledError) as cancellation:
            await run_task

        assert runtime.result is None
        assert abort_ws_terminal == [True]
        assert runtime._bridge_abort_task is not None
        assert runtime._bridge_abort_task.done()
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
        notes = getattr(cancellation.value, "__notes__", ())
        assert any("bridge abort failure" in note for note in notes)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_normal_close_without_exact_bridge_receipt_forbids_finality(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)
    runtime = fixture.runtime_v8()
    original_close = PublicDepthRestBridgeCoordinatorV8.aclose

    async def discard_receipt(
        bridge: PublicDepthRestBridgeCoordinatorV8,
    ) -> None:
        await original_close(bridge)

    monkeypatch.setattr(
        PublicDepthRestBridgeCoordinatorV8,
        "aclose",
        discard_receipt,
    )
    try:
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)
        await runtime.request_normal_stop()

        with pytest.raises(
            PublicCaptureRuntimeShutdownErrorV8,
            match="lacks an exact clean-close receipt",
        ):
            await run_task

        assert runtime.result is None
        assert fixture.depth_bridge.clean_close_receipt is not None
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_result_rejects_factory_valid_bridge_receipt_replay(
    tmp_path,
) -> None:
    first = _V8Fixture(
        tmp_path / "first",
        process_boot_id="11111111111111111111111111111111",
    )
    second = _V8Fixture(
        tmp_path / "second",
        process_boot_id="22222222222222222222222222222222",
    )
    try:
        _first_runtime, first_result = await _run_normal_v8(first)
        _second_runtime, second_result = await _run_normal_v8(second)

        with pytest.raises(
            PublicCaptureRuntimeShutdownErrorV8,
            match="receipt session differs",
        ):
            replace(
                second_result,
                depth_bridge_close_receipt=(
                    first_result.depth_bridge_close_receipt
                ),
            )
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("foreign_connection", "generation_delta", "failure_pattern"),
    (
        ("foreign-public-connection", 0, "connection differs"),
        (None, 1, "generation differs"),
    ),
)
async def test_v8_bridge_receipt_rejects_factory_valid_public_cursor_mismatch(
    tmp_path,
    foreign_connection: str | None,
    generation_delta: int,
    failure_pattern: str,
) -> None:
    fixture = _V8Fixture(tmp_path)
    try:
        _runtime, result = await _run_normal_v8(fixture)
        public_stop = result.websocket_route_cursors[1].stop_receipt
        foreign_stop = _issue_websocket_route_stop_receipt_v8(
            fixture.v8_plans,
            fixture.public_plan,
            session_id=public_stop.session_id,
            process_boot_id=public_stop.process_boot_id,
            connection_id=(
                public_stop.connection_id
                if foreign_connection is None
                else foreign_connection
            ),
            generation=public_stop.generation + generation_delta,
            last_frame_seq=public_stop.last_frame_seq,
            last_ingest_seq=public_stop.last_ingest_seq,
            last_receipt_wall_ms=public_stop.last_receipt_wall_ms,
            last_receipt_monotonic_ns=public_stop.last_receipt_monotonic_ns,
            stop_observed=ReceiptTimestamp(
                public_stop.stop_observed_wall_ms,
                public_stop.stop_observed_monotonic_ns,
            ),
        )

        with pytest.raises(
            PublicCaptureRuntimeShutdownErrorV8,
            match=failure_pattern,
        ):
            _validate_depth_bridge_close_bounds_v8(
                result.depth_bridge_close_receipt,
                promoting_plans=fixture.v8_plans,
                public_stop_receipt=foreign_stop,
                finality_receipt=result.finality_receipt,
            )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_bridge_receipt_rejects_public_stop_after_bridge_close(
    tmp_path,
) -> None:
    fixture = _V8Fixture(tmp_path)
    try:
        _runtime, result = await _run_normal_v8(fixture)
        receipt = result.depth_bridge_close_receipt
        public_stop = result.websocket_route_cursors[1].stop_receipt
        future_stop = _issue_websocket_route_stop_receipt_v8(
            fixture.v8_plans,
            fixture.public_plan,
            session_id=public_stop.session_id,
            process_boot_id=public_stop.process_boot_id,
            connection_id=public_stop.connection_id,
            generation=public_stop.generation,
            last_frame_seq=public_stop.last_frame_seq,
            last_ingest_seq=public_stop.last_ingest_seq,
            last_receipt_wall_ms=public_stop.last_receipt_wall_ms,
            last_receipt_monotonic_ns=public_stop.last_receipt_monotonic_ns,
            stop_observed=ReceiptTimestamp(
                receipt.close_wall_ms + 1,
                receipt.close_monotonic_ns + 1,
            ),
        )

        with pytest.raises(
            PublicCaptureRuntimeShutdownErrorV8,
            match="precedes public OWNER_STOP",
        ):
            _validate_depth_bridge_close_bounds_v8(
                receipt,
                promoting_plans=fixture.v8_plans,
                public_stop_receipt=future_stop,
                finality_receipt=result.finality_receipt,
            )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_bridge_receipt_rejects_finality_before_bridge_close(
    tmp_path,
) -> None:
    fixture = _V8Fixture(tmp_path)
    try:
        _runtime, result = await _run_normal_v8(fixture)
        receipt = result.depth_bridge_close_receipt
        assert (
            receipt.close_monotonic_ns
            > result.finality_receipt.target_last_receipt_monotonic_ns
        )
        premature_fence_ns = receipt.close_monotonic_ns - 1
        premature_finality = replace(
            result.finality_receipt,
            fence_monotonic_ns=premature_fence_ns,
            writer_observed_monotonic_ns=premature_fence_ns,
        )

        with pytest.raises(
            PublicCaptureRuntimeShutdownErrorV8,
            match="finality precedes",
        ):
            _validate_depth_bridge_close_bounds_v8(
                receipt,
                promoting_plans=fixture.v8_plans,
                public_stop_receipt=(
                    result.websocket_route_cursors[1].stop_receipt
                ),
                finality_receipt=premature_finality,
            )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_closed_session_owner_composes_exact_infrastructure_authorities(
    tmp_path: Path,
) -> None:
    fixture = _V8Fixture(tmp_path)
    try:
        runtime = fixture.runtime_v8()
        owner = PublicCaptureClosedSessionOwnerV8(runtime)
        run_task = asyncio.create_task(owner.run())
        await _wait_until_ready(fixture, run_task)
        await owner.request_normal_stop()
        result = await run_task

        assert type(result) is PublicCaptureClosedSessionResultV8
        assert result.runtime_result is runtime.result
        assert result.stop_reason == "OPERATOR_REQUESTED"
        assert result.integrity_ledger_clean_issued is True
        assert result.local_session_closure_issued is True
        assert result.depth_bridge_lifecycle_cleanly_closed is True
        assert result.websocket_route_cursor_finality_persisted is True
        assert result.oi_coverage_closed is True
        assert (
            result.session_closure_authority.manifest.depth_bridge_closure_entry
            is result.ledger_seal_receipt.seal.depth_bridge_closure_entry
        )
        assert (
            result.session_closure_authority.manifest.websocket_route_cursors
            == result.ledger_seal_receipt.seal.websocket_route_cursor_closure_pair
        )
        for field_name in (
            "retained_frame_parser_health_claimed",
            "retained_market_parser_health_certified",
            "websocket_retained_frame_parser_health_claimed",
            "websocket_upstream_message_completeness_claimed",
            "observed_source_completeness_claimed",
            "oi_data_completeness_claimed",
            "depth_bridge_complete_claimed",
            "book_completeness_claimed",
            "book_bridge_certified",
            "m2_certified",
            "m2_eligible",
            "strategy_ready",
            "promotion_ready",
            "probability_calibrated",
            "paper_execution_enabled",
            "paper_fok_enabled",
            "mandatory_exit_enabled",
            "efficacy_claimed",
            "pnl_or_profit_claimed",
            "order_execution_enabled",
            "private_credentials_permitted",
            "production_order_execution_enabled",
        ):
            assert getattr(result, field_name) is False
        encoded = canonical_public_capture_closed_session_result_v8(result)
        assert json.loads(encoded)["result_sha256"] == result.result_sha256
        assert owner.validate_current() == result.result_sha256

        with pytest.raises(
            PublicCaptureClosedSessionOwnerErrorV8,
            match="exact outer owner",
        ):
            replace(result)
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV8,
            match="only once",
        ):
            await owner.run()
        with pytest.raises(TypeError, match="exact PublicCaptureClosedSessionResultV2"):
            canonical_public_capture_closed_session_result_v2(result)  # pyright: ignore[reportArgumentType]

        object.__setattr__(result, "result_sha256", "f" * 64)
        with pytest.raises(
            PublicCaptureClosedSessionOwnerErrorV8,
            match="result hash differs",
        ):
            canonical_public_capture_closed_session_result_v8(result)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_closed_session_owner_rejects_v2_v8_runtime_downgrade(
    tmp_path: Path,
) -> None:
    v2_root = tmp_path / "v2"
    v8_root = tmp_path / "v8"
    v2_root.mkdir()
    v8_root.mkdir()
    v2_fixture = v2_fixture_module._Fixture(v2_root)
    v8_fixture = _V8Fixture(v8_root)
    try:
        v2_runtime = v2_fixture.runtime()
        v8_runtime = v8_fixture.runtime_v8()
        with pytest.raises(TypeError, match="exact PublicCaptureRuntimeV8"):
            PublicCaptureClosedSessionOwnerV8(v2_runtime)  # type: ignore[arg-type]
        with pytest.raises(TypeError, match="exact PublicCaptureRuntimeV2"):
            PublicCaptureClosedSessionOwnerV2(v8_runtime)  # type: ignore[arg-type]
        assert v2_runtime.started_once is False
        assert v8_runtime.started_once is False
    finally:
        await v2_fixture.close()
        await v8_fixture.close()


@pytest.mark.asyncio
async def test_v8_closed_session_owner_shields_finalization_from_caller_cancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)
    finalization_entered = asyncio.Event()
    allow_finalization = asyncio.Event()
    original_finalize = PublicCaptureClosedSessionOwnerV8._finalize_owned

    async def delayed_finalize(
        owner: PublicCaptureClosedSessionOwnerV8,
        runtime_result: PublicCaptureRuntimeResultV8,
    ) -> PublicCaptureClosedSessionResultV8:
        finalization_entered.set()
        await allow_finalization.wait()
        return await original_finalize(owner, runtime_result)

    monkeypatch.setattr(
        PublicCaptureClosedSessionOwnerV8,
        "_finalize_owned",
        delayed_finalize,
    )
    try:
        runtime = fixture.runtime_v8()
        owner = PublicCaptureClosedSessionOwnerV8(runtime)
        run_task = asyncio.create_task(owner.run())
        await _wait_until_ready(fixture, run_task)
        await owner.request_normal_stop()
        await finalization_entered.wait()

        run_task.cancel()
        await asyncio.sleep(0)
        allow_finalization.set()
        with pytest.raises(asyncio.CancelledError):
            await run_task

        result = owner.result
        assert result is not None
        assert result.integrity_ledger_clean_issued is True
        assert result.local_session_closure_issued is True
        assert result.strategy_ready is False
        assert result.paper_execution_enabled is False
        assert result.order_execution_enabled is False
        assert owner.validate_current() == result.result_sha256
    finally:
        allow_finalization.set()
        await fixture.close()


@pytest.mark.parametrize("authority_case", ["reordered", "cloned_depth", "foreign"])
@pytest.mark.asyncio
async def test_v8_closed_session_owner_rejects_mutated_plan_identity_before_seal(
    tmp_path: Path,
    authority_case: str,
) -> None:
    fixture = _V8Fixture(tmp_path)
    try:
        runtime = fixture.runtime_v8()
        owner = PublicCaptureClosedSessionOwnerV8(runtime)
        if authority_case == "reordered":
            owner.promoting_plans = (
                fixture.v8_plans[1],
                fixture.v8_plans[0],
                fixture.v8_plans[2],
                fixture.v8_plans[3],
            )
        elif authority_case == "cloned_depth":
            owner.depth_plan = replace(fixture.depth_plan)
        else:
            foreign = build_provisional_promoting_capture_plans_v8(("ETHUSDT",))
            owner.promoting_plans = foreign
            owner.depth_plan = cast(
                ProvisionalDepthRestQualificationPlanV8,
                foreign[3],
            )

        run_task = asyncio.create_task(owner.run())
        await _wait_until_ready(fixture, run_task)
        await owner.request_normal_stop()
        with pytest.raises(PublicCaptureClosedSessionOwnerErrorV8):
            await run_task

        assert owner.result is None
        assert not (
            fixture.ledger_path / "capture-clean-closure-seal.json"
        ).exists()
        assert not canonical_session_closure_manifest_path_v8(fixture.lease).exists()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV8,
            match="no current result",
        ):
            owner.validate_current()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV8,
            match="only once",
        ):
            await owner.run()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV8,
            match="fresh capture runtime",
        ):
            PublicCaptureClosedSessionOwnerV8(runtime)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_closed_session_result_rejects_same_seq_foreign_oi_receipt(
    tmp_path: Path,
) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    first = _V8Fixture(first_root)
    second = _V8Fixture(
        second_root,
        process_boot_id="fedcba9876543210fedcba9876543210",
    )
    try:
        first_owner = PublicCaptureClosedSessionOwnerV8(first.runtime_v8())
        second_owner = PublicCaptureClosedSessionOwnerV8(second.runtime_v8())
        first_task = asyncio.create_task(first_owner.run())
        second_task = asyncio.create_task(second_owner.run())
        await asyncio.gather(
            _wait_until_ready(first, first_task),
            _wait_until_ready(second, second_task),
        )
        await asyncio.gather(
            first_owner.request_normal_stop(),
            second_owner.request_normal_stop(),
        )
        first_result, second_result = await asyncio.gather(first_task, second_task)
        assert (
            first_result.runtime_result.oi_coverage_close_receipt.accepted_ingest_seq
            == second_result.runtime_result.oi_coverage_close_receipt.accepted_ingest_seq
        )

        foreign_runtime = replace(
            first_result.runtime_result,
            oi_coverage_close_receipt=(
                second_result.runtime_result.oi_coverage_close_receipt
            ),
        )
        object.__setattr__(first_result, "runtime_result", foreign_runtime)
        object.__setattr__(
            first_result,
            "runtime_result_sha256",
            closed_session_owner_module._runtime_result_sha256_v8(foreign_runtime),
        )
        with pytest.raises(
            PublicCaptureClosedSessionOwnerErrorV8,
            match="do not share one exact authority",
        ):
            canonical_public_capture_closed_session_result_v8(first_result)
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
async def test_v8_closed_session_owner_rejects_reversed_closure_clock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)
    original_capture = closed_session_owner_module._capture_receipt_v8

    def misordered_capture(clock: object, label: str) -> ReceiptTimestamp:
        observed = original_capture(clock, label)  # type: ignore[arg-type]
        if label == "V8 session closure":
            return ReceiptTimestamp(0, 0)
        return observed

    monkeypatch.setattr(
        closed_session_owner_module,
        "_capture_receipt_v8",
        misordered_capture,
    )
    try:
        runtime = fixture.runtime_v8()
        owner = PublicCaptureClosedSessionOwnerV8(runtime)
        run_task = asyncio.create_task(owner.run())
        await _wait_until_ready(fixture, run_task)
        await owner.request_normal_stop()
        with pytest.raises(ValueError, match="clock precedes"):
            await run_task

        assert owner.result is None
        assert (
            fixture.ledger_path / "capture-clean-closure-seal.json"
        ).is_file()
        assert not canonical_session_closure_manifest_path_v8(fixture.lease).exists()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV8,
            match="no current result",
        ):
            owner.validate_current()
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_v8_closed_session_owner_is_terminal_after_manifest_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _V8Fixture(tmp_path)

    def fail_closure_write(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RuntimeError("injected V8 session closure failure")

    monkeypatch.setattr(
        closed_session_owner_module,
        "write_session_closure_manifest_v8",
        fail_closure_write,
    )
    try:
        runtime = fixture.runtime_v8()
        owner = PublicCaptureClosedSessionOwnerV8(runtime)
        run_task = asyncio.create_task(owner.run())
        await _wait_until_ready(fixture, run_task)
        await owner.request_normal_stop()
        with pytest.raises(RuntimeError, match="injected V8 session closure failure"):
            await run_task

        assert owner.result is None
        assert (
            fixture.ledger_path / "capture-clean-closure-seal.json"
        ).is_file()
        assert not canonical_session_closure_manifest_path_v8(fixture.lease).exists()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV8,
            match="no current result",
        ):
            owner.validate_current()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV8,
            match="only once",
        ):
            await owner.run()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV8,
            match="fresh capture runtime",
        ):
            PublicCaptureClosedSessionOwnerV8(runtime)
    finally:
        await fixture.close()
