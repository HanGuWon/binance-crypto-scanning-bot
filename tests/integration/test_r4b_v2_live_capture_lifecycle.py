from __future__ import annotations

import asyncio
import hashlib
import threading
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from types import TracebackType

import pytest

from signalbot.capture.models import ConnectionState
from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.batching import (
    BatchPolicyV2,
    BoundedBatchHandoffV2,
)
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
    verify_grouped_blocks,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityLedgerV2,
)
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    CaptureFinalityFenceReceiptV2,
    DurableCaptureBatchWriterV2,
)
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    build_provisional_promoting_capture_plans_v2,
    provisional_promoting_plan_sha256_v2,
)
from signalbot.r4b_v2.capture.wal import (
    WalAuthorityV2,
    WalSyncPolicyV2,
    WalWriterV2,
    verify_wal_segments,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicWebSocketFrameAdapterFactoryV2,
    SharedWebSocketIngressV2,
)
from signalbot.r4b_v2.capture.websocket_lifecycle import (
    WebSocketLifecycleFatalCoordinatorV2,
)

_PROTOCOL_HASH = hashlib.sha256(b"r4b-v2-live-capture-e2e").hexdigest()
_QUALIFICATION_ID = "r4b-v2-live-e2e-q"
_STREAM_GROUP_ID = "binance-usdm-public-v2"
_SEGMENT_ID = "segment-e2e-0001"
_RAW_FRAME = (
    b'{"stream":"btcusdt@aggTrade","data":'
    b'{"e":"aggTrade","E":1,"s":"BTCUSDT"}}'
)


class _CausalClock:
    """Thread-safe deterministic receipt, ledger, handoff, and writer clock."""

    def __init__(self) -> None:
        self._wall_ms = 1_700_000_000_000
        self._monotonic_ns = 10_000_000_000
        self._lock = threading.Lock()

    def capture(self) -> ReceiptTimestamp:
        with self._lock:
            self._wall_ms += 1
            self._monotonic_ns += 100
            return ReceiptTimestamp(self._wall_ms, self._monotonic_ns)

    def wall_ms(self) -> int:
        with self._lock:
            self._wall_ms += 1
            return self._wall_ms

    def monotonic_ns(self) -> int:
        with self._lock:
            self._monotonic_ns += 100
            return self._monotonic_ns


@dataclass(slots=True)
class _CaptureStack:
    root: Path
    clock: _CausalClock
    plans: tuple[ProvisionalPromotingPlanV2, ...]
    market_plan: ProvisionalPromotingCapturePlanV2
    authority: WalAuthorityV2
    wal_policy: WalSyncPolicyV2
    block_policy: BlockPolicyV2
    signing_authority: BlockSigningAuthorityV2
    wal_writer: WalWriterV2
    block_writer: GroupedBlockWriterV2
    pipeline: CaptureBatchPipelineV2
    ledger: CaptureIntegrityLedgerV2
    ledger_max_events: int

    @property
    def wal_root(self) -> Path:
        return self.root / "wal"

    @property
    def block_root(self) -> Path:
        return self.root / "blocks"

    @property
    def ledger_root(self) -> Path:
        return self.root / "integrity-ledger"

    def reopen_ledger(self) -> CaptureIntegrityLedgerV2:
        return CaptureIntegrityLedgerV2(
            self.ledger_root,
            authority=self.authority,
            block_directory=self.block_root,
            block_root_binding=self.block_writer.root_binding,
            block_signing_authority=self.signing_authority,
            block_policy=self.block_policy,
            block_stream_group_id=_STREAM_GROUP_ID,
            block_segment_id=_SEGMENT_ID,
            maximum_total_bytes=8 * 1024 * 1024,
            emergency_reserve_bytes=128 * 1024,
            max_events=self.ledger_max_events,
            failure_domain_id="e2e-ledger",
            wall_clock_ms=self.clock.wall_ms,
            monotonic_clock_ns=self.clock.monotonic_ns,
        )


class _OneFrameThenStop:
    def __init__(self, stop_event: asyncio.Event) -> None:
        self._stop_event = stop_event

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield _RAW_FRAME
        self._stop_event.set()
        await asyncio.Event().wait()


class _OwnerConnection(AbstractAsyncContextManager[_OneFrameThenStop]):
    def __init__(self, frames: _OneFrameThenStop) -> None:
        self._frames = frames

    async def __aenter__(self) -> _OneFrameThenStop:
        return self._frames

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


class _DurableOpenCheckingConnector:
    def __init__(
        self,
        ledger: CaptureIntegrityLedgerV2,
        ledger_root: Path,
        stop_event: asyncio.Event,
    ) -> None:
        self._ledger = ledger
        self._ledger_root = ledger_root
        self._stop_event = stop_event
        self.open_event_sha256: str | None = None

    def __call__(self, url: str) -> _OwnerConnection:
        assert url.startswith("wss://fstream.binance.com/market/")
        [open_event] = self._ledger.events
        assert open_event.payload["phase"] == "OPEN"
        event_path = self._ledger_root / "integrity-event-00000001.json"
        assert event_path.read_bytes() == canonical_json_line(asdict(open_event))
        self.open_event_sha256 = open_event.sha256
        return _OwnerConnection(_OneFrameThenStop(self._stop_event))


@dataclass(slots=True)
class _BlockedFinalityPipeline:
    actual: CaptureBatchPipelineV2
    started: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    @property
    def handoff(self) -> BoundedBatchHandoffV2:
        return self.actual.handoff

    async def finalize_through(
        self,
        requested_ingest_seq: int,
        *,
        timeout_seconds: float,
    ) -> CaptureFinalityFenceReceiptV2:
        self.started.set()
        await self.release.wait()
        return await self.actual.finalize_through(
            requested_ingest_seq,
            timeout_seconds=timeout_seconds,
        )


def _build_capture_stack(root: Path) -> _CaptureStack:
    clock = _CausalClock()
    plans = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
    market_plan = next(
        plan
        for plan in plans
        if isinstance(plan, ProvisionalPromotingCapturePlanV2)
        and plan.route_id == "usdm_market"
    )
    authority = WalAuthorityV2(
        attempt_id="attempt-live-e2e",
        protocol_sha256=_PROTOCOL_HASH,
        plan_sha256=provisional_promoting_plan_sha256_v2(plans),
        source_manifest_sha256="c" * 64,
        schema_sha256="d" * 64,
        runtime_manifest_sha256="e" * 64,
    )
    wal_policy = WalSyncPolicyV2(
        qualification_id=_QUALIFICATION_ID,
        fsync_candidate_id="fsync-10ms-r10",
        interval_ms=10,
        max_unsynced_records=10,
        max_unsynced_bytes=100_000,
        max_record_bytes=20_000,
        max_segment_bytes=1_000_000,
    )
    block_policy = BlockPolicyV2(
        qualification_id=_QUALIFICATION_ID,
        codec_candidate_id="zstd-1.5.7-l9-w0-checksum-content-size",
        compression_level=9,
        max_uncompressed_bytes=4_194_304,
        max_linger_ms=1_000,
    )
    batch_policy = BatchPolicyV2(
        max_records=wal_policy.max_unsynced_records,
        max_encoded_bytes=wal_policy.max_unsynced_bytes,
        max_linger_us=wal_policy.interval_ms * 1_000,
        queue_max_events=100,
        queue_max_encoded_bytes=1_000_000,
        low_water_events=10,
        low_water_encoded_bytes=100_000,
        qualification_id=_QUALIFICATION_ID,
    )
    signer = Ed25519BlockSignerV2.from_private_key_bytes(
        key_id="live-e2e-writer",
        private_key_bytes=b"\x2a" * 32,
    )
    signing_authority = BlockSigningAuthorityV2.from_public_key_bytes(
        key_id=signer.key_id,
        public_key_bytes=signer.public_key_bytes,
    )
    wal_writer = WalWriterV2(
        root / "wal",
        authority=authority,
        policy=wal_policy,
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
        clock_ns=clock.monotonic_ns,
    )
    block_writer = GroupedBlockWriterV2(
        root / "blocks",
        authority=authority,
        policy=block_policy,
        signer=signer,
        signing_authority=signing_authority,
        stream_group_id=_STREAM_GROUP_ID,
        segment_id=_SEGMENT_ID,
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=1024,
    )
    durable_writer = DurableCaptureBatchWriterV2(
        batch_policy=batch_policy,
        wal_writer=wal_writer,
        block_builder=GroupedBlockBuilderV2(block_policy),
        block_writer=block_writer,
        clock_ns=clock.monotonic_ns,
    )
    handoff = BoundedBatchHandoffV2(
        batch_policy,
        monotonic_ns=clock.monotonic_ns,
        expected_first_ingest_seq=wal_writer.next_ingest_seq,
    )
    pipeline = CaptureBatchPipelineV2(handoff, durable_writer)
    ledger_max_events = 100
    ledger = CaptureIntegrityLedgerV2(
        root / "integrity-ledger",
        authority=authority,
        block_directory=root / "blocks",
        block_root_binding=block_writer.root_binding,
        block_signing_authority=signing_authority,
        block_policy=block_policy,
        block_stream_group_id=_STREAM_GROUP_ID,
        block_segment_id=_SEGMENT_ID,
        maximum_total_bytes=8 * 1024 * 1024,
        emergency_reserve_bytes=128 * 1024,
        max_events=ledger_max_events,
        failure_domain_id="e2e-ledger",
        wall_clock_ms=clock.wall_ms,
        monotonic_clock_ns=clock.monotonic_ns,
    )
    return _CaptureStack(
        root=root,
        clock=clock,
        plans=plans,
        market_plan=market_plan,
        authority=authority,
        wal_policy=wal_policy,
        block_policy=block_policy,
        signing_authority=signing_authority,
        wal_writer=wal_writer,
        block_writer=block_writer,
        pipeline=pipeline,
        ledger=ledger,
        ledger_max_events=ledger_max_events,
    )


def _coordinator(
    stack: _CaptureStack,
    pipeline: CaptureBatchPipelineV2 | _BlockedFinalityPipeline,
) -> WebSocketLifecycleFatalCoordinatorV2:
    return WebSocketLifecycleFatalCoordinatorV2(
        stack.plans,
        stack.market_plan,
        session_id="session-live-e2e",
        process_boot_id="boot-live-e2e",
        session_started_at=stack.clock.capture(),
        source_component="v2-owner-usdm-market-e2e",
        clock=stack.clock,
        pipeline=pipeline,
        integrity_ledger=stack.ledger,
        finality_timeout_seconds=2.0,
    )


@pytest.mark.asyncio
async def test_real_disk_lifecycle_open_finality_bounded_and_restart_assertion(
    tmp_path: Path,
) -> None:
    stack = _build_capture_stack(tmp_path)
    coordinator = _coordinator(stack, stack.pipeline)
    ingress = SharedWebSocketIngressV2(
        stack.pipeline,
        recovered_wal_tail_ingest_seq=stack.wal_writer.durable_ack_seq,
    )
    factory = PublicWebSocketFrameAdapterFactoryV2(
        stack.market_plan,
        session_id="session-live-e2e",
        protocol_hash=_PROTOCOL_HASH,
        clock=stack.clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )
    connector = _DurableOpenCheckingConnector(
        stack.ledger,
        stack.ledger_root,
        coordinator.stop_event,
    )
    stack.pipeline.start()
    connection_id = f"{stack.market_plan.name}-g000001"

    async def one_frame() -> AsyncIterator[bytes]:
        yield _RAW_FRAME

    try:
        coordinator.record_transition(
            connection_id,
            generation=1,
            last_frame_seq=0,
            state=ConnectionState.CONNECTING,
            reason="connect_attempt",
        )
        connector(factory.owner_plan.url)
        coordinator.record_transition(
            connection_id,
            generation=1,
            last_frame_seq=0,
            state=ConnectionState.CONNECTED,
            reason="public_session_open",
        )
        adapter = factory(connection_id=connection_id, generation=1)
        await adapter.consume(one_frame())
    finally:
        await stack.pipeline.stop()

    open_event, bounded_event = stack.ledger.events
    assert connector.open_event_sha256 == open_event.sha256
    assert [open_event.payload["phase"], bounded_event.payload["phase"]] == [
        "OPEN",
        "BOUNDED",
    ]
    assert bounded_event.payload["open_event_sha256"] == open_event.sha256
    assert bounded_event.payload["right_ingest_seq"] == 1
    assert bounded_event.payload["right_frame_seq"] == 1
    assert bounded_event.payload["source_message_count_known"] is False
    assert coordinator.pending_source_gap is False
    assert coordinator.failed is False
    stack.ledger.assert_source_gap_bounded_current_v2(bounded_event)

    [wal_manifest] = verify_wal_segments(
        stack.wal_root,
        authority=stack.authority,
        policy=stack.wal_policy,
    )
    [block_manifest] = verify_grouped_blocks(
        stack.block_root,
        authority=stack.authority,
        policy=stack.block_policy,
        signing_authority=stack.signing_authority,
        stream_group_id=_STREAM_GROUP_ID,
        segment_id=_SEGMENT_ID,
    )
    assert wal_manifest.last_ingest_seq == block_manifest.last_ingest_seq == 1

    restarted_ledger = stack.reopen_ledger()
    restarted_events = restarted_ledger.events
    assert [event.payload["phase"] for event in restarted_events] == [
        "OPEN",
        "BOUNDED",
    ]
    restarted_ledger.assert_source_gap_bounded_current_v2(restarted_events[-1])


@pytest.mark.asyncio
async def test_real_disk_cancelled_successor_restart_has_no_false_bounded(
    tmp_path: Path,
) -> None:
    stack = _build_capture_stack(tmp_path)
    blocked_pipeline = _BlockedFinalityPipeline(stack.pipeline)
    coordinator = _coordinator(stack, blocked_pipeline)
    ingress = SharedWebSocketIngressV2(
        stack.pipeline,
        recovered_wal_tail_ingest_seq=stack.wal_writer.durable_ack_seq,
    )
    adapter = PublicWebSocketFrameAdapterFactoryV2(
        stack.market_plan,
        session_id="session-live-e2e",
        protocol_hash=_PROTOCOL_HASH,
        clock=stack.clock,
        ingress=ingress,
        recovery_lifecycle=coordinator,
    )(connection_id="v2-usdm-market-promoting-abc-g000001", generation=1)
    coordinator.record_transition(
        adapter.connection_id,
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTING,
        reason="connect_attempt",
    )
    coordinator.record_transition(
        adapter.connection_id,
        generation=1,
        last_frame_seq=0,
        state=ConnectionState.CONNECTED,
        reason="public_session_open",
    )
    stack.pipeline.start()
    task = asyncio.create_task(adapter.consume(_one_frame()))
    await blocked_pipeline.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    coordinator.record_transition(
        adapter.connection_id,
        generation=1,
        last_frame_seq=adapter.frame_seq,
        state=ConnectionState.DISCONNECTED,
        reason="owner_cancelled",
    )
    await stack.pipeline.stop()

    assert adapter.frame_seq == 0
    assert coordinator.pending_source_gap is True
    assert coordinator.failed is False
    [open_event] = stack.ledger.events
    assert open_event.payload["phase"] == "OPEN"
    assert not list(stack.ledger_root.glob("integrity-event-00000002.json"))

    [wal_manifest] = verify_wal_segments(
        stack.wal_root,
        authority=stack.authority,
        policy=stack.wal_policy,
    )
    [block_manifest] = verify_grouped_blocks(
        stack.block_root,
        authority=stack.authority,
        policy=stack.block_policy,
        signing_authority=stack.signing_authority,
        stream_group_id=_STREAM_GROUP_ID,
        segment_id=_SEGMENT_ID,
    )
    assert wal_manifest.last_ingest_seq == block_manifest.last_ingest_seq == 1

    restarted_ledger = stack.reopen_ledger()
    [restarted_open] = restarted_ledger.events
    assert restarted_open.sha256 == open_event.sha256
    assert restarted_open.payload["phase"] == "OPEN"


async def _one_frame() -> AsyncIterator[bytes]:
    yield _RAW_FRAME
