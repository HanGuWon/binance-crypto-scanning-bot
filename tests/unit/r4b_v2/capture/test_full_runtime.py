from __future__ import annotations

import asyncio
import hashlib
import json
import pickle
import time
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock

import httpx
import pytest

import signalbot.r4b_v2.capture.blocks as blocks_module
import signalbot.r4b_v2.capture.closed_session_owner as closed_session_owner_module
import signalbot.r4b_v2.capture.integrity_ledger as integrity_ledger_module
import signalbot.r4b_v2.capture.membership as membership_module
import signalbot.r4b_v2.capture.usdm_public_prefix_health as public_prefix_health_module
from signalbot.capture.receipts import ReceiptTimestamp
from signalbot.capture.writer_lease import WriterLease, WriterLeaseNotHeldError
from signalbot.capture.ws_owner import (
    Connector,
    PublicWebSocketCaptureOwner,
    WebSocketOwnerSettings,
)
from signalbot.r4b_v2.capture.batching import BatchPolicyV2, BoundedBatchHandoffV2
from signalbot.r4b_v2.capture.block_container import (
    BlockSigningAuthorityV2,
    Ed25519BlockSignerV2,
)
from signalbot.r4b_v2.capture.blocks import (
    BlockIntegrityError,
    BlockPolicyV2,
    GroupedBlockBuilderV2,
    GroupedBlockWriterV2,
)
from signalbot.r4b_v2.capture.closed_session_owner import (
    PublicCaptureClosedSessionOwnerStateErrorV2,
    PublicCaptureClosedSessionOwnerV2,
    PublicCaptureClosedSessionResultV2,
    canonical_public_capture_closed_session_result_v2,
)
from signalbot.r4b_v2.capture.full_runtime import (
    PublicCaptureRuntimeBindingErrorV2,
    PublicCaptureRuntimeResultV2,
    PublicCaptureRuntimeShutdownErrorV2,
    PublicCaptureRuntimeStateErrorV2,
    PublicCaptureRuntimeV2,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityLedgerV2,
    PersistedCaptureCleanClosureSealReceiptV2,
)
from signalbot.r4b_v2.capture.membership import (
    CurrentVerifiedRawMembershipLeafUseV2,
    RawRecordMembershipErrorV2,
    consume_current_verified_raw_membership_prefix_v2,
    inspect_current_verified_raw_membership_leaf_v2,
)
from signalbot.r4b_v2.capture.mirrored_wal import MirroredWalWriterV2
from signalbot.r4b_v2.capture.models import TransportV2, VenueV2
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    DurableCaptureBatchWriterV2,
)
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingRestCapturePlanV2,
    build_provisional_promoting_capture_plans_v2,
    provisional_promoting_plan_sha256_v2,
)
from signalbot.r4b_v2.capture.rest_adapter import (
    PipelineRestCaptureFatalCoordinatorV2,
    PublicOpenInterestRestCaptureAdapterV2,
)
from signalbot.r4b_v2.capture.rest_scheduler import (
    PublicOpenInterestRestSchedulerV2,
    create_public_oi_rest_census_context_v2,
)
from signalbot.r4b_v2.capture.session import (
    PersistedSessionClosureAuthorityV2,
    SessionAuthorityError,
    canonical_session_closure_manifest_path_v2,
    canonical_session_start_manifest_path_v2,
    write_session_closure_manifest_v2,
    write_session_start_manifest_v2,
)
from signalbot.r4b_v2.capture.usdm_market_prefix_health import (
    RetainedUsdmMarketParserHealthCertificateV2,
    RetainedUsdmMarketParserHealthNoncertifyingV2,
    canonical_retained_usdm_market_parser_health_result_v2,
    certify_retained_usdm_market_parser_health_v2,
)
from signalbot.r4b_v2.capture.usdm_public_m1 import (
    UsdmPublicDepthM1ContractErrorV2,
    canonical_usdm_public_depth_m1_v2,
    parse_current_verified_usdm_public_depth_m1_v2,
    parse_verified_usdm_public_depth_m1_v2,
)
from signalbot.r4b_v2.capture.usdm_public_prefix_health import (
    RetainedUsdmPublicParserHealthCertificateV2,
    RetainedUsdmPublicParserHealthNoncertifyingV2,
    canonical_retained_usdm_public_parser_health_result_v2,
    certify_retained_usdm_public_parser_health_v2,
)
from signalbot.r4b_v2.capture.wal import WalAuthorityV2, WalSyncPolicyV2
from signalbot.r4b_v2.capture.wal_qualification import (
    WAL_QUALIFICATION_DURATION_MS_V2,
    WAL_RECORD_CAP_CANDIDATES_V2,
    WAL_SYNC_CANDIDATES_MS_V2,
    WalCandidateMetricsV2,
    WalCandidateQualificationV2,
    WalQualificationRunV2,
    WalSelectionReceiptV2,
    select_wal_candidate_v2,
    wal_candidate_id_v2,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicWebSocketFrameAdapterFactoryV2,
    SharedWebSocketIngressV2,
    build_public_websocket_owner_plan_v2,
)
from signalbot.r4b_v2.capture.websocket_composition import (
    PublicWebSocketOwnerCompositionV2,
    PublicWebSocketRuntimeClaimErrorV2,
)
from signalbot.r4b_v2.capture.websocket_finality import (
    WebSocketRouteFinalityErrorV2,
    finalize_websocket_route_cursor_v2,
)
from signalbot.r4b_v2.capture.websocket_lifecycle import (
    WebSocketLifecycleFatalCoordinatorV2,
)

HASH = "a" * 64
PROCESS_BOOT_ID = "0123456789abcdef0123456789abcdef"
QUALIFICATION = "full-runtime-wal-24h-grid"
WINDOW_START_MS = 2_100_000_000_000
WINDOW_END_MS = WINDOW_START_MS + WAL_QUALIFICATION_DURATION_MS_V2
H_START_MS = WINDOW_END_MS + 60_000
MAXIMUM_BYTES = 64 * 1024 * 1024
RESERVE_BYTES = 1024
POSITIVE_PATH_TIMEOUT_SECONDS = 10.0
READY_POLL_INTERVAL_SECONDS = 0.001


def _combined_frame(stream: str, data: dict[str, object]) -> bytes:
    return json.dumps(
        {"stream": stream, "data": data},
        separators=(",", ":"),
    ).encode("utf-8")


def _valid_agg_trade_frame() -> bytes:
    return _combined_frame(
        "btcusdt@aggTrade",
        {
            "e": "aggTrade",
            "E": 1_710_000_301_000,
            "a": 7_001,
            "s": "BTCUSDT",
            "p": "65000.25",
            "q": "2.500",
            "nq": "2.125",
            "f": 90_001,
            "l": 90_003,
            "T": 1_710_000_300_900,
            "m": False,
            "st": 1,
        },
    )


def _valid_kline_frame() -> bytes:
    return _combined_frame(
        "btcusdt@kline_5m",
        {
            "e": "kline",
            "E": 1_710_000_300_001,
            "s": "BTCUSDT",
            "k": {
                "t": 1_710_000_000_000,
                "T": 1_710_000_299_999,
                "s": "BTCUSDT",
                "i": "5m",
                "f": 80_001,
                "L": 80_010,
                "o": "64000.00",
                "c": "65000.00",
                "h": "65100.00",
                "l": "63900.00",
                "v": "20.0",
                "n": 10,
                "x": True,
                "q": "1290000.0",
                "V": "12.0",
                "Q": "775000.0",
                "B": "0",
            },
        },
    )


def _valid_mark_price_frame() -> bytes:
    return _combined_frame(
        "btcusdt@markPrice@1s",
        {
            "e": "markPriceUpdate",
            "E": 1_784_455_200_123,
            "s": "BTCUSDT",
            "p": "117842.37000000",
            "ap": "117836.51428571",
            "P": "0.00000000",
            "i": "117881.62956522",
            "r": "0.00009367",
            "T": 1_784_476_800_000,
            "st": 1,
        },
    )


def _all_valid_market_frames() -> tuple[bytes, ...]:
    return (
        _valid_agg_trade_frame(),
        _valid_kline_frame(),
        _valid_mark_price_frame(),
    )


def _valid_depth_frame(
    *,
    symbol: str = "BTCUSDT",
    first_update_id: int = 1,
    final_update_id: int = 1,
    previous_final_update_id: int = 0,
) -> bytes:
    return _combined_frame(
        f"{symbol.lower()}@depth@100ms",
        {
            "e": "depthUpdate",
            "E": 1_710_000_301_000 + final_update_id,
            "T": 1_710_000_300_900 + final_update_id,
            "s": symbol,
            "U": first_update_id,
            "u": final_update_id,
            "pu": previous_final_update_id,
            "b": [["65000.00", "1.250"]],
            "a": [["65000.50", "0.750"]],
            "ps": symbol,
            "st": 1,
        },
    )


def _valid_depth_frames(row_count: int) -> tuple[bytes, ...]:
    assert row_count >= 1
    return tuple(
        _valid_depth_frame(
            first_update_id=index * 2 + 1,
            final_update_id=index * 2 + 2,
            previous_final_update_id=0 if index == 0 else index * 2,
        )
        for index in range(row_count)
    )


class _ReceiptClock:
    def __init__(self, wall_ms: int, monotonic_ns: int) -> None:
        self.wall_ms = wall_ms
        self.monotonic_ns = monotonic_ns

    def capture(self) -> ReceiptTimestamp:
        return ReceiptTimestamp(self.wall_clock_ms(), self.monotonic_clock_ns())

    def wall_clock_ms(self) -> int:
        self.wall_ms += 1
        return self.wall_ms

    def monotonic_clock_ns(self) -> int:
        self.monotonic_ns = max(self.monotonic_ns + 1, time.monotonic_ns())
        return self.monotonic_ns


class _SchedulerClock:
    def __init__(self, wall_ms: int, monotonic_ns: int) -> None:
        self.wall_ms = wall_ms
        self.monotonic_value = monotonic_ns

    def utc_wall_ms(self) -> int:
        return self.wall_ms

    def monotonic_ns(self) -> int:
        return self.monotonic_value

    async def wait_until(
        self,
        stop_event: asyncio.Event,
        deadline_monotonic_ns: int,
    ) -> bool:
        assert deadline_monotonic_ns >= self.monotonic_value
        await stop_event.wait()
        return True


class _HeldFrames:
    def __init__(self, frames: bytes | tuple[bytes, ...]) -> None:
        self.frames = (frames,) if isinstance(frames, bytes) else frames
        assert self.frames
        self.first_frame_yielded = asyncio.Event()
        self.all_frames_yielded = asyncio.Event()

    async def __aiter__(self):  # type: ignore[no-untyped-def]
        for index, frame in enumerate(self.frames):
            if index == 0:
                self.first_frame_yielded.set()
            yield frame
        self.all_frames_yielded.set()
        await asyncio.Event().wait()


class _MemoryConnection(AbstractAsyncContextManager[_HeldFrames]):
    def __init__(
        self,
        frames: _HeldFrames,
        *,
        exit_gate: asyncio.Event | None = None,
    ) -> None:
        self.frames = frames
        self.exit_gate = exit_gate
        self.entered = False
        self.closed = False

    async def __aenter__(self) -> _HeldFrames:
        self.entered = True
        return self.frames

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        del exc_type, exc_value, traceback
        if self.exit_gate is not None:
            await self.exit_gate.wait()
        self.closed = True


class _MemoryConnector:
    def __init__(self, connection: _MemoryConnection) -> None:
        self.connection = connection
        self.urls: list[str] = []

    def __call__(self, url: str) -> _MemoryConnection:
        self.urls.append(url)
        if len(self.urls) > 1:
            raise AssertionError("runtime test owner unexpectedly reconnected")
        return self.connection


def _settings() -> WebSocketOwnerSettings:
    return WebSocketOwnerSettings(
        maximum_connection_age_seconds=30.0,
        connect_timeout_seconds=1.0,
        close_timeout_seconds=1.0,
        heartbeat_interval_seconds=1.0,
        pong_timeout_seconds=1.0,
        internal_queue_frames=8,
        maximum_frame_bytes=4_096,
        maximum_reconnect_attempts=1,
        reconnect_delays_seconds=(0.0,),
        healthy_reset_seconds=1.0,
    )


def _wal_policy(sync_ms: int, record_cap: int) -> WalSyncPolicyV2:
    return WalSyncPolicyV2(
        qualification_id=QUALIFICATION,
        fsync_candidate_id=wal_candidate_id_v2(
            sync_ms=sync_ms,
            record_cap=record_cap,
        ),
        interval_ms=sync_ms,
        max_unsynced_records=record_cap,
        max_unsynced_bytes=8_000_000,
        max_record_bytes=20_000,
        max_segment_bytes=16_000_000,
    )


def _candidate_metrics(*, passed: bool) -> WalCandidateMetricsV2:
    return WalCandidateMetricsV2(
        unresolved_overflow_or_drop_count=0 if passed else 1,
        p99_queue_fraction_ppm=500_000,
        maximum_queue_fraction_ppm=750_000,
        p99_enqueue_latency_ns=10_000_000,
        maximum_enqueue_latency_ns=100_000_000,
        p99_cpu_fraction_ppm=700_000,
        maximum_cpu_fraction_ppm=850_000,
        p99_fsync_latency_ns=100_000_000,
        maximum_fsync_latency_ns=500_000_000,
        service_rate_over_p99_ingress_ppm=2_000_000,
        service_rate_over_peak_1s_ingress_ppm=1_250_000,
        crash_replay_root_equality=True,
    )


def _selection_receipt() -> WalSelectionReceiptV2:
    selected = (10, 256)
    candidates = tuple(
        WalCandidateQualificationV2(
            policy=_wal_policy(sync_ms, record_cap),
            metrics=_candidate_metrics(passed=(sync_ms, record_cap) == selected),
            measurement_root_sha256=hashlib.sha256(
                f"{sync_ms}:{record_cap}".encode()
            ).hexdigest(),
        )
        for sync_ms in WAL_SYNC_CANDIDATES_MS_V2
        for record_cap in WAL_RECORD_CAP_CANDIDATES_V2
    )
    qualification = WalQualificationRunV2(
        qualification_id=QUALIFICATION,
        window_start_wall_ms=WINDOW_START_MS,
        window_end_wall_ms=WINDOW_END_MS,
        actual_final_panel_sha256="1" * 64,
        final_codec_sha256="2" * 64,
        source_manifest_sha256="3" * 64,
        runtime_manifest_sha256="4" * 64,
        independent_failure_domain_evidence_sha256="5" * 64,
        actual_final_panel_passed=True,
        final_codec_passed=True,
        independent_failure_domains_passed=True,
        engineering_only=True,
        strategy_or_outcome_data_accessed=False,
        candidates=candidates,
    )
    return select_wal_candidate_v2(
        qualification,
        selection_wall_ms=WINDOW_END_MS,
        h_start_wall_ms=H_START_MS,
    )


def _websocket_plan(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
    route_id: str,
) -> ProvisionalPromotingCapturePlanV2:
    return next(
        plan
        for plan in plans
        if type(plan) is ProvisionalPromotingCapturePlanV2
        and plan.route_id == route_id
    )


def _rest_plan(
    plans: tuple[ProvisionalPromotingPlanV2, ...],
) -> ProvisionalPromotingRestCapturePlanV2:
    return next(
        plan
        for plan in plans
        if type(plan) is ProvisionalPromotingRestCapturePlanV2
    )


class _Fixture:
    def __init__(
        self,
        tmp_path: Path,
        *,
        public_exit_gate: asyncio.Event | None = None,
        market_frames: tuple[bytes, ...] | None = None,
        public_frames: tuple[bytes, ...] | None = None,
        symbols: tuple[str, ...] = ("BTCUSDT",),
        process_boot_id: str = PROCESS_BOOT_ID,
    ) -> None:
        self.scope = tmp_path / "scope"
        self.scope.mkdir(parents=True)
        self.plans = build_provisional_promoting_capture_plans_v2(symbols)
        self.market_plan = _websocket_plan(self.plans, "usdm_market")
        self.public_plan = _websocket_plan(self.plans, "usdm_public")
        self.rest_plan = _rest_plan(self.plans)
        self.lease = WriterLease.acquire(self.scope)
        next_slot = (
            self.lease.acquired_wall_ms
            - (self.lease.acquired_wall_ms % 5_000)
            + 5_000
        )
        self.started_wall_ms = next_slot + 100
        self.started_monotonic_ns = self.lease.acquired_monotonic_ns + 1
        self.receipt_clock = _ReceiptClock(
            self.started_wall_ms + 10,
            self.started_monotonic_ns + 10,
        )
        self.primary_path = self.scope / "wal-primary"
        self.mirror_path = self.scope / "wal-mirror"
        self.block_path = self.scope / "blocks"
        self.ledger_path = self.scope / "ledger"
        self.authority = WalAuthorityV2(
            attempt_id="attempt-full-runtime",
            protocol_sha256=HASH,
            plan_sha256=provisional_promoting_plan_sha256_v2(self.plans),
            source_manifest_sha256="b" * 64,
            schema_sha256="c" * 64,
            runtime_manifest_sha256="d" * 64,
        )
        selection = _selection_receipt()
        policy = selection.selected_policy
        assert policy is not None
        self.wal_writer = MirroredWalWriterV2(
            self.primary_path,
            self.mirror_path,
            authority=self.authority,
            policy=policy,
            selection_receipt=selection,
            primary_maximum_total_bytes=MAXIMUM_BYTES,
            mirror_maximum_total_bytes=MAXIMUM_BYTES,
            primary_emergency_reserve_bytes=RESERVE_BYTES,
            mirror_emergency_reserve_bytes=RESERVE_BYTES,
            primary_failure_domain_id="runtime-primary",
            mirror_failure_domain_id="runtime-mirror",
        )
        signer = Ed25519BlockSignerV2.from_private_key_bytes(
            key_id="runtime-test-writer",
            private_key_bytes=bytes(range(32)),
        )
        signing_authority = BlockSigningAuthorityV2.from_public_key_bytes(
            key_id=signer.key_id,
            public_key_bytes=signer.public_key_bytes,
        )
        block_policy = BlockPolicyV2(
            qualification_id=QUALIFICATION,
            codec_candidate_id="runtime-zstd-candidate",
            compression_level=9,
            max_uncompressed_bytes=4 * 1024 * 1024,
            max_linger_ms=1_000,
        )
        self.block_writer = GroupedBlockWriterV2(
            self.block_path,
            authority=self.authority,
            policy=block_policy,
            signer=signer,
            signing_authority=signing_authority,
            stream_group_id="binance-usdm-public-v2",
            segment_id="segment-runtime-0001",
            maximum_total_bytes=MAXIMUM_BYTES,
            emergency_reserve_bytes=RESERVE_BYTES,
            failure_domain_id="runtime-block",
        )
        self.ledger = CaptureIntegrityLedgerV2(
            self.ledger_path,
            authority=self.authority,
            block_directory=self.block_path,
            block_root_binding=self.block_writer.root_binding,
            block_signing_authority=signing_authority,
            block_policy=block_policy,
            block_stream_group_id="binance-usdm-public-v2",
            block_segment_id="segment-runtime-0001",
            maximum_total_bytes=MAXIMUM_BYTES,
            emergency_reserve_bytes=RESERVE_BYTES,
            max_events=10_000,
            failure_domain_id="runtime-ledger",
            writer_lease=self.lease,
            wall_clock_ms=self.receipt_clock.wall_clock_ms,
            monotonic_clock_ns=self.receipt_clock.monotonic_clock_ns,
        )
        batch_policy = BatchPolicyV2(
            max_records=policy.max_unsynced_records,
            max_encoded_bytes=policy.max_unsynced_bytes,
            max_linger_us=policy.interval_ms * 1_000,
            queue_max_events=512,
            queue_max_encoded_bytes=16_000_000,
            low_water_events=128,
            low_water_encoded_bytes=4_000_000,
            qualification_id=QUALIFICATION,
        )
        durable_writer = DurableCaptureBatchWriterV2(
            batch_policy=batch_policy,
            wal_writer=self.wal_writer,
            block_builder=GroupedBlockBuilderV2(block_policy),
            block_writer=self.block_writer,
            writer_lease=self.lease,
        )
        self.pipeline = CaptureBatchPipelineV2(
            BoundedBatchHandoffV2(
                batch_policy,
                monotonic_ns=self.receipt_clock.monotonic_clock_ns,
                expected_first_ingest_seq=1,
            ),
            durable_writer,
        )
        self.pipeline.start()
        self.process_boot_id = process_boot_id
        self.session_id = f"{self.started_wall_ms}-{self.process_boot_id}"
        self.start_authority = write_session_start_manifest_v2(
            canonical_session_start_manifest_path_v2(self.lease),
            lease=self.lease,
            session_id=self.session_id,
            process_boot_id=self.process_boot_id,
            started_wall_ms=self.started_wall_ms,
            started_monotonic_ns=self.started_monotonic_ns,
            wal_authority=self.authority,
            wal_durability_binding=self.wal_writer.durability_binding,
            block_policy=block_policy,
            block_signing_authority=signing_authority,
            stream_group_id="binance-usdm-public-v2",
            segment_id="segment-runtime-0001",
            integrity_ledger_max_events=10_000,
            storage_root_directories=(
                self.primary_path,
                self.mirror_path,
                self.block_path,
                self.ledger_path,
            ),
            grouped_block_root_binding=self.block_writer.root_binding,
            integrity_ledger_root_binding=self.ledger.root_binding,
        )
        coverage_start = self.started_wall_ms - (self.started_wall_ms % 5_000)
        self.scheduler_clock = _SchedulerClock(
            coverage_start + 200,
            self.receipt_clock.monotonic_clock_ns(),
        )
        self.ingress = SharedWebSocketIngressV2(
            self.pipeline,
            recovered_wal_tail_ingest_seq=0,
        )
        self.market_frames = _HeldFrames(
            b'{"stream":"btcusdt@aggTrade","data":{}}'
            if market_frames is None
            else market_frames
        )
        self.public_frames = _HeldFrames(
            b'{"stream":"btcusdt@depth@100ms","data":'
            b'{"e":"depthUpdate","s":"BTCUSDT","ps":"BTCUSDT",'
            b'"st":1,"U":1,"u":1,"pu":0}}'
            if public_frames is None
            else public_frames
        )
        self.market_connection = _MemoryConnection(self.market_frames)
        self.public_connection = _MemoryConnection(
            self.public_frames,
            exit_gate=public_exit_gate,
        )
        self.market_connector = _MemoryConnector(self.market_connection)
        self.public_connector = _MemoryConnector(self.public_connection)
        self.market_composition = self._composition(
            self.market_plan,
            self.market_connector,
        )
        self.public_composition = self._composition(
            self.public_plan,
            self.public_connector,
        )
        self.rest_requests: list[httpx.Request] = []
        transport = httpx.MockTransport(self._rest_response)
        self.rest_adapter = PublicOpenInterestRestCaptureAdapterV2(
            self.rest_plan,
            session_id=self.session_id,
            protocol_hash=HASH,
            connection_id="oi-rest-runtime",
            generation=1,
            clock=self.receipt_clock,
            ingress=self.ingress,
            fatal_coordinator=PipelineRestCaptureFatalCoordinatorV2(self.pipeline),
            transport=transport,
        )
        context = create_public_oi_rest_census_context_v2(
            self.rest_plan,
            session_id=self.session_id,
            session_start_manifest_sha256=self.start_authority.manifest_sha256,
            plan_bundle_sha256=provisional_promoting_plan_sha256_v2(self.plans),
            protocol_hash=HASH,
            coverage_start_slot_wall_ms=coverage_start,
            ingress=self.ingress,
            receipt_clock=self.receipt_clock,
        )
        self.scheduler = PublicOpenInterestRestSchedulerV2(
            self.rest_plan,
            self.rest_adapter,
            census_context=context,
            clock=self.scheduler_clock,
        )

    def _composition(
        self,
        plan: ProvisionalPromotingCapturePlanV2,
        connector: _MemoryConnector,
    ) -> PublicWebSocketOwnerCompositionV2:
        lifecycle = WebSocketLifecycleFatalCoordinatorV2(
            self.plans,
            plan,
            session_id=self.session_id,
            process_boot_id=self.process_boot_id,
            session_started_at=ReceiptTimestamp(
                self.started_wall_ms,
                self.started_monotonic_ns,
            ),
            source_component=f"v2-owner-{plan.route_id}",
            clock=self.receipt_clock,
            pipeline=self.pipeline,
            integrity_ledger=self.ledger,
            finality_timeout_seconds=POSITIVE_PATH_TIMEOUT_SECONDS,
        )
        factory = PublicWebSocketFrameAdapterFactoryV2(
            plan,
            session_id=self.session_id,
            protocol_hash=HASH,
            clock=self.receipt_clock,
            ingress=self.ingress,
            recovery_lifecycle=lifecycle,
        )
        owner = PublicWebSocketCaptureOwner(
            build_public_websocket_owner_plan_v2(plan),
            plan_sha256=self.authority.plan_sha256,
            process_boot_id=self.process_boot_id,
            settings=_settings(),
            connector=cast(Connector, connector),
            depth_resync_callback=lambda _request: None,
            depth_range_callback=lambda _observation: None,
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
            requires_preconnect_admission=True,
        )
        return PublicWebSocketOwnerCompositionV2(
            session_start_authority=self.start_authority,
            writer_lease=self.lease,
            promoting_plans=self.plans,
            plan=plan,
            recovered_wal_tail_ingest_seq=0,
            owner=owner,
            frame_adapter_factory=factory,
            lifecycle_coordinator=lifecycle,
        )

    def _rest_response(self, request: httpx.Request) -> httpx.Response:
        self.rest_requests.append(request)
        return httpx.Response(
            200,
            content=(
                b'{"openInterest":"1.0","symbol":"BTCUSDT",'
                b'"time":2100000000000}'
            ),
            request=request,
        )

    def runtime(
        self,
        *,
        producer_timeout: float = POSITIVE_PATH_TIMEOUT_SECONDS,
        finality_timeout: float = POSITIVE_PATH_TIMEOUT_SECONDS,
    ) -> PublicCaptureRuntimeV2:
        return PublicCaptureRuntimeV2(
            (self.market_composition, self.public_composition),
            self.rest_adapter,
            self.scheduler,
            producer_shutdown_timeout_seconds=producer_timeout,
            finality_timeout_seconds=finality_timeout,
        )

    async def close(self) -> None:
        try:
            await self.rest_adapter.aclose()
        except BaseException:
            pass
        try:
            await self.pipeline.stop()
        except BaseException:
            pass
        try:
            self.lease.assert_held()
        except WriterLeaseNotHeldError:
            return
        self.lease.release()


async def _wait_until_ready(
    fixture: _Fixture,
    run_task: asyncio.Task[object],
) -> None:
    async def ready() -> bool:
        base_ready = (
            fixture.market_connection.entered
            and fixture.public_connection.entered
            and fixture.market_frames.first_frame_yielded.is_set()
            and fixture.public_frames.first_frame_yielded.is_set()
            and fixture.scheduler.last_completed_poll_cycle_seq == 1
        )
        return (
            base_ready
            and fixture.market_frames.all_frames_yielded.is_set()
            and fixture.public_frames.all_frames_yielded.is_set()
            and not fixture.market_composition.lifecycle_coordinator.pending_source_gap
            and not fixture.public_composition.lifecycle_coordinator.pending_source_gap
        )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + POSITIVE_PATH_TIMEOUT_SECONDS
    while True:
        if run_task.done():
            run_task.result()
            raise AssertionError("runtime task completed before its first retained source set")
        if await ready():
            return
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise AssertionError(
                "runtime fixture did not reach its first retained source set within "
                f"{POSITIVE_PATH_TIMEOUT_SECONDS:.1f}s; "
                f"market_entered={fixture.market_connection.entered}, "
                f"public_entered={fixture.public_connection.entered}, "
                f"market_first={fixture.market_frames.first_frame_yielded.is_set()}, "
                f"public_first={fixture.public_frames.first_frame_yielded.is_set()}, "
                "scheduler_cycle="
                f"{fixture.scheduler.last_completed_poll_cycle_seq}, "
                "market_gap="
                f"{fixture.market_composition.lifecycle_coordinator.pending_source_gap}, "
                "public_gap="
                f"{fixture.public_composition.lifecycle_coordinator.pending_source_gap}"
            )
        await asyncio.sleep(min(READY_POLL_INTERVAL_SECONDS, remaining))


async def _finish_clean_capture(
    fixture: _Fixture,
) -> tuple[
    PublicCaptureRuntimeResultV2,
    PersistedCaptureCleanClosureSealReceiptV2,
    PersistedSessionClosureAuthorityV2,
]:
    runtime = fixture.runtime()
    run_task = asyncio.create_task(runtime.run())
    await _wait_until_ready(fixture, run_task)
    await runtime.request_normal_stop()
    result = await run_task
    ledger_seal = fixture.ledger.seal_clean_closure_v2(
        promoting_plans=fixture.plans,
        finality_receipt=result.finality_receipt,
        wal_writer=fixture.wal_writer,
        block_writer=fixture.block_writer,
        session_id=fixture.session_id,
        process_boot_id=fixture.process_boot_id,
        seal_wall_ms=fixture.receipt_clock.wall_clock_ms(),
        seal_monotonic_ns=fixture.receipt_clock.monotonic_clock_ns(),
    )
    closure = write_session_closure_manifest_v2(
        canonical_session_closure_manifest_path_v2(fixture.lease),
        lease=fixture.lease,
        session_start_authority=fixture.start_authority,
        promoting_plans=fixture.plans,
        finality_receipt=result.finality_receipt,
        pipeline=fixture.pipeline,
        ledger_seal_receipt=ledger_seal,
        ledger=fixture.ledger,
        stop_reason="OPERATOR_REQUESTED",
        closed_wall_ms=fixture.receipt_clock.wall_clock_ms(),
        closed_monotonic_ns=fixture.receipt_clock.monotonic_clock_ns(),
        finalized_websocket_route_cursors=result.websocket_route_cursors,
    )
    return result, ledger_seal, closure


def _market_parser_health(
    fixture: _Fixture,
    *,
    ledger_seal: PersistedCaptureCleanClosureSealReceiptV2,
    closure: PersistedSessionClosureAuthorityV2,
) -> (
    RetainedUsdmMarketParserHealthCertificateV2
    | RetainedUsdmMarketParserHealthNoncertifyingV2
):
    return certify_retained_usdm_market_parser_health_v2(
        closure,
        lease=fixture.lease,
        session_start_authority=fixture.start_authority,
        promoting_plans=fixture.plans,
        pipeline=fixture.pipeline,
        ledger_seal_receipt=ledger_seal,
        integrity_ledger=fixture.ledger,
        block_writer=fixture.block_writer,
    )


def _public_parser_health(
    fixture: _Fixture,
    *,
    ledger_seal: PersistedCaptureCleanClosureSealReceiptV2,
    closure: PersistedSessionClosureAuthorityV2,
) -> (
    RetainedUsdmPublicParserHealthCertificateV2
    | RetainedUsdmPublicParserHealthNoncertifyingV2
):
    return certify_retained_usdm_public_parser_health_v2(
        closure,
        lease=fixture.lease,
        session_start_authority=fixture.start_authority,
        promoting_plans=fixture.plans,
        pipeline=fixture.pipeline,
        ledger_seal_receipt=ledger_seal,
        integrity_ledger=fixture.ledger,
        block_writer=fixture.block_writer,
    )


@pytest.mark.asyncio
async def test_normal_stop_owns_exact_trio_and_returns_only_local_finality(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)

        first, second = await asyncio.gather(
            runtime.request_normal_stop(),
            runtime.request_normal_stop(),
        )
        result = await run_task

        assert first == second == result.normal_stop_receipt
        assert runtime.producer_task_count == 3
        assert result.producer_task_count == 3
        assert result.adapter_cleanly_closed
        assert result.oi_coverage_closed
        assert result.websocket_local_route_cursors_finalized
        assert not result.pending_source_gap
        assert not result.fatal_state_failed
        assert not result.oi_data_completeness_claimed
        assert not result.websocket_retained_frame_parser_health_claimed
        assert not result.websocket_upstream_message_completeness_claimed
        assert not result.observed_source_completeness_claimed
        assert not result.m2_eligible
        assert not result.local_session_closure_issued
        assert not result.integrity_ledger_clean_issued
        assert (
            result.oi_coverage_close_receipt.accepted_ingest_seq
            <= result.finality_receipt.fence_ingest_seq
        )
        assert result.verified_prefix_proof_sha256 == (
            result.finality_receipt.prefix_proof_sha256
        )
        assert tuple(
            cursor.stop_receipt.route_id
            for cursor in result.websocket_route_cursors
        ) == ("usdm_market", "usdm_public")
        assert all(
            cursor.stop_receipt.last_ingest_seq
            <= result.finality_receipt.fence_ingest_seq
            for cursor in result.websocket_route_cursors
        )
        assert all(
            cursor.retained_frame_parser_health_claimed is False
            and cursor.upstream_message_completeness_claimed is False
            and cursor.m2_certified is False
            for cursor in result.websocket_route_cursors
        )
        assert fixture.rest_adapter.cleanly_closed
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is not None
        assert len(fixture.market_connector.urls) == 1
        assert len(fixture.public_connector.urls) == 1
        assert len(fixture.rest_requests) == 1
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_runtime_result_rejects_swapped_duplicate_foreign_and_early_finality(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)
        await runtime.request_normal_stop()
        result = await run_task
        market, public = result.websocket_route_cursors

        with pytest.raises(TypeError, match="factory-sealed"):
            replace(market)

        with pytest.raises(
            WebSocketRouteFinalityErrorV2,
            match="canonical route order",
        ):
            replace(result, websocket_route_cursors=(public, market))
        with pytest.raises(
            WebSocketRouteFinalityErrorV2,
            match="canonical route order",
        ):
            replace(result, websocket_route_cursors=(market, market))

        foreign_finality = replace(
            result.finality_receipt,
            exact_prefix_sha256="f" * 64,
        )
        with pytest.raises(
            WebSocketRouteFinalityErrorV2,
            match="supplied finality receipt",
        ):
            PublicCaptureRuntimeResultV2(
                normal_stop_receipt=result.normal_stop_receipt,
                oi_coverage_close_receipt=result.oi_coverage_close_receipt,
                websocket_route_cursors=result.websocket_route_cursors,
                finality_receipt=foreign_finality,
                verified_prefix_proof_sha256=foreign_finality.prefix_proof_sha256,
            )

        latest = max(
            result.websocket_route_cursors,
            key=lambda cursor: cursor.stop_receipt.last_ingest_seq,
        )
        early_tail = latest.stop_receipt.last_ingest_seq - 1
        assert early_tail >= 1
        early_finality = replace(
            result.finality_receipt,
            requested_ingest_seq=early_tail,
            fence_ingest_seq=early_tail,
            wal_durable_ack_seq=early_tail,
            finalized_block_tail_ingest_seq=early_tail,
            durable_record_count=early_tail,
            final_block_sequence=1,
        )
        with pytest.raises(
            WebSocketRouteFinalityErrorV2,
            match="precedes the WebSocket route stop cursor",
        ):
            finalize_websocket_route_cursor_v2(
                latest.stop_receipt,
                early_finality,
            )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_clean_session_closure_persists_canonical_websocket_cursor_pair(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)
        await runtime.request_normal_stop()
        result = await run_task
        ledger_seal = fixture.ledger.seal_clean_closure_v2(
            promoting_plans=fixture.plans,
            finality_receipt=result.finality_receipt,
            wal_writer=fixture.wal_writer,
            block_writer=fixture.block_writer,
            session_id=fixture.session_id,
            process_boot_id=PROCESS_BOOT_ID,
            seal_wall_ms=fixture.receipt_clock.wall_clock_ms(),
            seal_monotonic_ns=fixture.receipt_clock.monotonic_clock_ns(),
        )
        closure = write_session_closure_manifest_v2(
            canonical_session_closure_manifest_path_v2(fixture.lease),
            lease=fixture.lease,
            session_start_authority=fixture.start_authority,
            promoting_plans=fixture.plans,
            finality_receipt=result.finality_receipt,
            pipeline=fixture.pipeline,
            ledger_seal_receipt=ledger_seal,
            ledger=fixture.ledger,
            stop_reason="OPERATOR_REQUESTED",
            closed_wall_ms=fixture.receipt_clock.wall_clock_ms(),
            closed_monotonic_ns=fixture.receipt_clock.monotonic_clock_ns(),
            finalized_websocket_route_cursors=result.websocket_route_cursors,
        )
        manifest = closure.manifest

        assert manifest.websocket_route_cursor_finality_persisted is True
        assert manifest.websocket_route_cursors_sha256 is not None
        assert tuple(
            entry.route_id for entry in manifest.websocket_route_cursors
        ) == ("usdm_market", "usdm_public")
        assert all(
            entry.retained_frame_parser_health_claimed is False
            and entry.upstream_message_completeness_claimed is False
            and entry.m2_certified is False
            for entry in manifest.websocket_route_cursors
        )
        with pytest.raises(
            WebSocketRouteFinalityErrorV2,
            match="stop receipt digest differs",
        ):
            replace(
                manifest.websocket_route_cursors[0],
                last_frame_seq=(
                    manifest.websocket_route_cursors[0].last_frame_seq + 1
                ),
            )
        with pytest.raises(ValueError, match="pair hash differs"):
            replace(
                manifest,
                websocket_route_cursors_sha256="0" * 64,
            )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_clean_retained_market_prefix_certifies_all_three_strict_m1_streams(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path, market_frames=_all_valid_market_frames())
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)

        result = _market_parser_health(
            fixture,
            ledger_seal=ledger_seal,
            closure=closure,
        )

        assert type(result) is RetainedUsdmMarketParserHealthCertificateV2
        assert result.market_record_count == result.successful_parse_count == 3
        assert result.agg_trade_success_count == 1
        assert result.kline_5m_success_count == 1
        assert result.mark_price_1s_success_count == 1
        assert result.stream_count == len(result.stream_scans) == 3
        assert all(scan.successful_parse_count == 1 for scan in result.stream_scans)
        assert result.current_storage_reverified
        assert result.retained_market_prefix_parser_health_certified
        assert not result.upstream_message_losslessness_claimed
        assert not result.required_source_completeness_claimed
        assert not result.oi_schedule_completeness_claimed
        assert not result.oi_freshness_claimed
        assert not result.m2_certified
        assert not result.strategy_ready
        assert not result.pnl_or_order_authority
        encoded = canonical_retained_usdm_market_parser_health_result_v2(result)
        assert json.loads(encoded)["certificate_sha256"] == result.certificate_sha256
        with pytest.raises(TypeError, match="factory-sealed"):
            replace(result)
        object.__setattr__(result, "_factory_seal", object())
        with pytest.raises(
            RuntimeError,
            match="factory seal differs",
        ):
            canonical_retained_usdm_market_parser_health_result_v2(result)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_valid_but_incomplete_market_stream_census_is_noncertifying(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path, market_frames=(_valid_agg_trade_frame(),))
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)

        result = _market_parser_health(
            fixture,
            ledger_seal=ledger_seal,
            closure=closure,
        )

        assert type(result) is RetainedUsdmMarketParserHealthNoncertifyingV2
        assert result.market_record_count == result.successful_parse_count == 1
        assert result.parse_failure_count == 0
        assert result.unknown_stream_count == 0
        assert result.missing_planned_stream_count == 2
        assert result.issue_codes == ("MISSING_PLANNED_STREAM",)
        assert not result.retained_market_prefix_parser_health_certified
        encoded = canonical_retained_usdm_market_parser_health_result_v2(result)
        assert json.loads(encoded)["result_sha256"] == result.result_sha256
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_clean_retained_public_prefix_certifies_strict_depth_m1_and_pu_checks(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(
        tmp_path,
        public_frames=(
            _valid_depth_frame(first_update_id=1, final_update_id=2),
            _valid_depth_frame(
                first_update_id=3,
                final_update_id=4,
                previous_final_update_id=2,
            ),
        ),
    )
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)

        result = _public_parser_health(
            fixture,
            ledger_seal=ledger_seal,
            closure=closure,
        )

        assert type(result) is RetainedUsdmPublicParserHealthCertificateV2
        assert result.public_record_count == result.successful_parse_count == 2
        assert result.depth_100ms_success_count == 2
        assert result.stream_count == len(result.stream_scans) == 1
        assert result.stream_scans[0].successful_parse_count == 2
        assert result.observed_pu_transition_check_count == 1
        assert result.depth_pu_discontinuity_count == 0
        assert result.bounded_public_source_gap_count >= 1
        assert result.current_storage_reverified
        assert result.retained_public_prefix_parser_health_certified
        assert not result.depth_sequence_continuity_claimed
        assert not result.snapshot_bridge_claimed
        assert not result.local_book_reconstructed
        assert not result.upstream_message_losslessness_claimed
        assert not result.required_source_completeness_claimed
        assert not result.oi_schedule_completeness_claimed
        assert not result.oi_freshness_claimed
        assert not result.m2_certified
        assert not result.strategy_ready
        assert not result.paper_ready
        assert not result.pnl_authority
        assert not result.production_order_authority
        encoded = canonical_retained_usdm_public_parser_health_result_v2(result)
        assert json.loads(encoded)["certificate_sha256"] == result.certificate_sha256
        with pytest.raises(TypeError, match="factory-sealed"):
            replace(result)
        object.__setattr__(result, "_factory_seal", object())
        with pytest.raises(RuntimeError, match="factory seal differs"):
            canonical_retained_usdm_public_parser_health_result_v2(result)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_signed_public_parse_failure_is_noncertifying(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(
        tmp_path,
        public_frames=(
            b'{"stream":"btcusdt@depth@100ms","data":'
            b'{"e":"depthUpdate","s":"BTCUSDT","ps":"BTCUSDT",'
            b'"st":1,"U":1,"u":1,"pu":0}}',
        ),
    )
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)

        result = _public_parser_health(
            fixture,
            ledger_seal=ledger_seal,
            closure=closure,
        )

        assert type(result) is RetainedUsdmPublicParserHealthNoncertifyingV2
        assert result.public_record_count == result.parse_failure_count == 1
        assert result.successful_parse_count == 0
        assert result.unknown_stream_count == 0
        assert result.missing_planned_stream_count == 1
        assert result.issue_codes[0] == "STRICT_M1_PARSE_FAILURE"
        assert not result.retained_public_prefix_parser_health_certified
        assert not result.m2_certified
        encoded = canonical_retained_usdm_public_parser_health_result_v2(result)
        assert json.loads(encoded)["result_sha256"] == result.result_sha256
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_signed_public_unknown_stream_diagnostic_is_noncertifying(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _Fixture(
        tmp_path,
        public_frames=(
            b'{"stream":"btcusdt@depth@100ms","data":'
            b'{"e":"depthUpdate","s":"BTCUSDT","ps":"BTCUSDT",'
            b'"st":1,"U":1,"u":1,"pu":0}}',
        ),
    )
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)
        monkeypatch.setattr(
            public_prefix_health_module,
            "_diagnostic_stream",
            lambda _payload: "ethusdt@depth@100ms",
        )

        result = _public_parser_health(
            fixture,
            ledger_seal=ledger_seal,
            closure=closure,
        )

        assert type(result) is RetainedUsdmPublicParserHealthNoncertifyingV2
        assert result.public_record_count == result.parse_failure_count == 1
        assert result.unknown_stream_count == 1
        assert result.missing_planned_stream_count == 1
        assert result.issue_codes[0] == "UNKNOWN_OR_UNPLANNED_STREAM"
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_valid_public_depth_missing_one_planned_stream_is_noncertifying(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(
        tmp_path,
        symbols=("BTCUSDT", "ETHUSDT"),
        public_frames=(_valid_depth_frame(),),
    )
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)

        result = _public_parser_health(
            fixture,
            ledger_seal=ledger_seal,
            closure=closure,
        )

        assert type(result) is RetainedUsdmPublicParserHealthNoncertifyingV2
        assert result.public_record_count == result.successful_parse_count == 1
        assert result.parse_failure_count == 0
        assert result.missing_planned_stream_count == 1
        assert result.issue_codes == ("MISSING_PLANNED_STREAM",)
        assert tuple(scan.stream for scan in result.stream_scans) == (
            "btcusdt@depth@100ms",
            "ethusdt@depth@100ms",
        )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_observed_same_generation_depth_pu_discontinuity_is_noncertifying(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(
        tmp_path,
        public_frames=(
            _valid_depth_frame(first_update_id=1, final_update_id=2),
            _valid_depth_frame(
                first_update_id=3,
                final_update_id=4,
                previous_final_update_id=999,
            ),
        ),
    )
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)

        result = _public_parser_health(
            fixture,
            ledger_seal=ledger_seal,
            closure=closure,
        )

        assert type(result) is RetainedUsdmPublicParserHealthNoncertifyingV2
        assert result.successful_parse_count == 2
        assert result.parse_failure_count == 0
        assert result.observed_pu_transition_check_count == 1
        assert result.depth_pu_discontinuity_count == 1
        assert result.issue_codes == ("DEPTH_PU_DISCONTINUITY",)
        assert not result.depth_sequence_continuity_claimed
        assert not result.snapshot_bridge_claimed
        assert not result.local_book_reconstructed
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_public_prefix_reports_after_cursor_and_terminal_mismatch_defensively(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _Fixture(
        tmp_path,
        public_frames=(
            _valid_depth_frame(first_update_id=1, final_update_id=2),
            _valid_depth_frame(
                first_update_id=3,
                final_update_id=4,
                previous_final_update_id=2,
            ),
        ),
    )
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)
        public_entry = closure.manifest.websocket_route_cursors[1]
        assert public_entry.last_ingest_seq > 1
        shortened = SimpleNamespace(
            connection_id=public_entry.connection_id,
            generation=public_entry.generation,
            last_frame_seq=public_entry.last_frame_seq,
            last_ingest_seq=public_entry.last_ingest_seq - 1,
            last_receipt_wall_ms=public_entry.last_receipt_wall_ms,
            last_receipt_monotonic_ns=public_entry.last_receipt_monotonic_ns,
            sha256="f" * 64,
            stop_receipt_sha256=public_entry.stop_receipt_sha256,
            finalized_route_cursor_sha256=(
                public_entry.finalized_route_cursor_sha256
            ),
        )
        monkeypatch.setattr(
            public_prefix_health_module,
            "_public_cursor_entry",
            lambda _entries: shortened,
        )

        result = _public_parser_health(
            fixture,
            ledger_seal=ledger_seal,
            closure=closure,
        )

        assert type(result) is RetainedUsdmPublicParserHealthNoncertifyingV2
        assert result.public_record_after_finalized_cursor_count >= 1
        assert result.terminal_cursor_mismatch_count == 1
        assert "PUBLIC_RECORD_AFTER_FINALIZED_CURSOR" in result.issue_codes
        assert "TERMINAL_CURSOR_MISMATCH" in result.issue_codes
    finally:
        await fixture.close()


def test_public_local_cursor_transition_boundary_is_exact() -> None:
    transition = cast(
        Callable[
            [
                tuple[str, int, int, int, int, int] | None,
                tuple[str, int, int, int, int, int],
                str,
            ],
            bool,
        ],
        public_prefix_health_module._cursor_transition_is_exact,
    )
    plan_id = "v2-usdm-public-promoting-abc"
    first = (f"{plan_id}-g000001", 1, 1, 10, 100, 1_000)
    second = (f"{plan_id}-g000001", 1, 2, 11, 100, 1_001)
    reconnect = (f"{plan_id}-g000002", 2, 1, 12, 101, 1_002)

    assert transition(None, first, plan_id)
    assert transition(first, second, plan_id)
    assert transition(second, reconnect, plan_id)
    assert not transition(first, replace_cursor(second, frame_seq=3), plan_id)
    assert not transition(first, replace_cursor(second, ingest_seq=10), plan_id)
    assert not transition(first, replace_cursor(second, wall_ms=99), plan_id)
    assert not transition(
        first,
        replace_cursor(second, connection_id="foreign-g000001"),
        plan_id,
    )


def test_public_pu_check_resets_at_generation_boundary() -> None:
    check = cast(
        Callable[..., bool | None],
        public_prefix_health_module._observed_pu_transition_is_consistent,
    )
    previous = (1, 10, 200)

    assert check(previous, generation=1, previous_final_update_id=200) is True
    assert check(previous, generation=1, previous_final_update_id=201) is False
    assert check(previous, generation=2, previous_final_update_id=999) is None
    assert check(None, generation=1, previous_final_update_id=999) is None


def replace_cursor(
    cursor: tuple[str, int, int, int, int, int],
    *,
    connection_id: str | None = None,
    frame_seq: int | None = None,
    ingest_seq: int | None = None,
    wall_ms: int | None = None,
) -> tuple[str, int, int, int, int, int]:
    return (
        cursor[0] if connection_id is None else connection_id,
        cursor[1],
        cursor[2] if frame_seq is None else frame_seq,
        cursor[3] if ingest_seq is None else ingest_seq,
        cursor[4] if wall_ms is None else wall_ms,
        cursor[5],
    )


@pytest.mark.asyncio
async def test_public_prefix_rejects_cross_session_authority_splice(
    tmp_path: Path,
) -> None:
    left = _Fixture(
        tmp_path / "left",
        public_frames=(_valid_depth_frame(),),
    )
    right = _Fixture(
        tmp_path / "right",
        public_frames=(_valid_depth_frame(),),
        process_boot_id="fedcba9876543210fedcba9876543210",
    )
    try:
        _left_result, _left_seal, left_closure = await _finish_clean_capture(left)
        _right_result, right_seal, _right_closure = await _finish_clean_capture(right)

        with pytest.raises(SessionAuthorityError):
            certify_retained_usdm_public_parser_health_v2(
                left_closure,
                lease=right.lease,
                session_start_authority=right.start_authority,
                promoting_plans=right.plans,
                pipeline=right.pipeline,
                ledger_seal_receipt=right_seal,
                integrity_ledger=right.ledger,
                block_writer=right.block_writer,
            )
    finally:
        await left.close()
        await right.close()


@pytest.mark.asyncio
async def test_retained_public_health_fails_closed_after_signed_block_tamper(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path, public_frames=(_valid_depth_frame(),))
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)
        manifest_path = next(fixture.block_path.glob("block-*.manifest.json"))
        manifest_document = json.loads(manifest_path.read_bytes())
        data_path = fixture.block_path / cast(str, manifest_document["data_file"])
        encoded = data_path.read_bytes()
        data_path.write_bytes(encoded[:-1] + bytes((encoded[-1] ^ 1,)))

        with pytest.raises(BlockIntegrityError):
            _public_parser_health(
                fixture,
                ledger_seal=ledger_seal,
                closure=closure,
            )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_callback_scoped_public_m0_use_is_one_shot_and_parser_parity_holds(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path, public_frames=(_valid_depth_frame(),))
    captured_uses: list[CurrentVerifiedRawMembershipLeafUseV2] = []
    captured_leaves = []
    current_rows = []
    try:
        _runtime_result, _ledger_seal, _closure = await _finish_clean_capture(fixture)

        def consume(
            _ingest_seq: int,
            _encoded_line: bytes,
            current_use: CurrentVerifiedRawMembershipLeafUseV2 | None,
        ) -> None:
            if current_use is None:
                return
            captured_uses.append(current_use)
            captured_leaves.append(
                inspect_current_verified_raw_membership_leaf_v2(current_use)
            )
            current_rows.append(
                parse_current_verified_usdm_public_depth_m1_v2(
                    current_use,
                    promoting_plans=fixture.plans,
                )
            )

        delivered, manifests = consume_current_verified_raw_membership_prefix_v2(
            fixture.block_writer,
            integrity_ledger=fixture.ledger,
            expected_transport=TransportV2.WEBSOCKET,
            expected_venue=VenueV2.USDM_FUTURES,
            expected_route_id="usdm_public",
            expected_symbol=None,
            consume=consume,
        )

        assert delivered == manifests[-1].last_ingest_seq
        assert len(captured_uses) == len(captured_leaves) == len(current_rows) == 1
        current_use = captured_uses[0]
        leaf = captured_leaves[0]
        current_row = current_rows[0]
        durable_row = parse_verified_usdm_public_depth_m1_v2(
            leaf,
            promoting_plans=fixture.plans,
            block_directory=fixture.block_writer.directory,
            block_root_binding=fixture.block_writer.root_binding,
            authority=fixture.block_writer.authority,
            policy=fixture.block_writer.policy,
            signing_authority=fixture.block_writer.signing_authority,
            stream_group_id=fixture.block_writer.stream_group_id,
            segment_id=fixture.block_writer.segment_id,
            integrity_ledger=fixture.ledger,
        )
        assert current_row.m1_payload_sha256 == durable_row.m1_payload_sha256
        assert canonical_usdm_public_depth_m1_v2(
            current_row
        ) == canonical_usdm_public_depth_m1_v2(durable_row)
        assert current_use.consumed
        assert not current_use.active
        with pytest.raises(RawRecordMembershipErrorV2, match="outside its streaming"):
            parse_current_verified_usdm_public_depth_m1_v2(
                current_use,
                promoting_plans=fixture.plans,
            )
        with pytest.raises(TypeError, match="not serializable"):
            pickle.dumps(current_use)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_callback_scoped_public_m0_tamper_fails_self_integrity(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path, public_frames=(_valid_depth_frame(),))
    captured_uses: list[CurrentVerifiedRawMembershipLeafUseV2] = []
    try:
        _runtime_result, _ledger_seal, _closure = await _finish_clean_capture(fixture)

        def consume(
            _ingest_seq: int,
            _encoded_line: bytes,
            current_use: CurrentVerifiedRawMembershipLeafUseV2 | None,
        ) -> None:
            if current_use is None:
                return
            captured_uses.append(current_use)
            leaf = inspect_current_verified_raw_membership_leaf_v2(current_use)
            object.__setattr__(leaf, "leaf_sha256", "0" * 64)
            with pytest.raises(
                RawRecordMembershipErrorV2,
                match="verified raw membership leaf is invalid",
            ):
                parse_current_verified_usdm_public_depth_m1_v2(
                    current_use,
                    promoting_plans=fixture.plans,
                )

        consume_current_verified_raw_membership_prefix_v2(
            fixture.block_writer,
            integrity_ledger=fixture.ledger,
            expected_transport=TransportV2.WEBSOCKET,
            expected_venue=VenueV2.USDM_FUTURES,
            expected_route_id="usdm_public",
            expected_symbol=None,
            consume=consume,
        )

        assert len(captured_uses) == 1
        assert captured_uses[0].consumed
        assert not captured_uses[0].active
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_strict_public_parser_error_still_consumes_and_revokes_current_use(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(
        tmp_path,
        public_frames=(
            b'{"stream":"btcusdt@depth@100ms","data":'
            b'{"e":"depthUpdate","s":"BTCUSDT","ps":"BTCUSDT",'
            b'"st":1,"U":1,"u":1,"pu":0}}',
        ),
    )
    captured_uses: list[CurrentVerifiedRawMembershipLeafUseV2] = []
    try:
        _runtime_result, _ledger_seal, _closure = await _finish_clean_capture(fixture)

        def consume(
            _ingest_seq: int,
            _encoded_line: bytes,
            current_use: CurrentVerifiedRawMembershipLeafUseV2 | None,
        ) -> None:
            if current_use is None:
                return
            captured_uses.append(current_use)
            with pytest.raises(UsdmPublicDepthM1ContractErrorV2):
                parse_current_verified_usdm_public_depth_m1_v2(
                    current_use,
                    promoting_plans=fixture.plans,
                )

        consume_current_verified_raw_membership_prefix_v2(
            fixture.block_writer,
            integrity_ledger=fixture.ledger,
            expected_transport=TransportV2.WEBSOCKET,
            expected_venue=VenueV2.USDM_FUTURES,
            expected_route_id="usdm_public",
            expected_symbol=None,
            consume=consume,
        )

        assert len(captured_uses) == 1
        assert captured_uses[0].consumed
        assert not captured_uses[0].active
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_public_prefix_whole_scan_count_is_constant_for_one_and_twenty_five_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_counts: list[tuple[int, int, int, int]] = []
    for row_count in (1, 25):
        fixture = _Fixture(
            tmp_path / f"rows-{row_count}",
            public_frames=_valid_depth_frames(row_count),
        )
        try:
            _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)
            exact_line_spy = Mock(wraps=membership_module._read_exact_ingest_line)
            stream_spy = Mock(wraps=blocks_module.consume_verified_grouped_records_v2)
            attestation_verify_spy = Mock(
                wraps=integrity_ledger_module.verify_grouped_blocks
            )
            block_verify_spy = Mock(wraps=blocks_module.verify_grouped_blocks)
            with monkeypatch.context() as patch_context:
                patch_context.setattr(
                    membership_module,
                    "_read_exact_ingest_line",
                    exact_line_spy,
                )
                patch_context.setattr(
                    blocks_module,
                    "consume_verified_grouped_records_v2",
                    stream_spy,
                )
                patch_context.setattr(
                    integrity_ledger_module,
                    "verify_grouped_blocks",
                    attestation_verify_spy,
                )
                patch_context.setattr(
                    blocks_module,
                    "verify_grouped_blocks",
                    block_verify_spy,
                )

                result = _public_parser_health(
                    fixture,
                    ledger_seal=ledger_seal,
                    closure=closure,
                )

            assert type(result) is RetainedUsdmPublicParserHealthCertificateV2
            assert result.public_record_count == row_count
            observed_counts.append(
                (
                    exact_line_spy.call_count,
                    stream_spy.call_count,
                    attestation_verify_spy.call_count,
                    block_verify_spy.call_count,
                )
            )
        finally:
            await fixture.close()

    assert observed_counts[0] == observed_counts[1]
    assert observed_counts[0][0] == 0
    assert observed_counts[0][1] == 3
    assert observed_counts[0][2] == 3


@pytest.mark.asyncio
async def test_closed_session_owner_composes_exact_local_authorities_only(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path, market_frames=_all_valid_market_frames())
    try:
        runtime = fixture.runtime()
        owner = PublicCaptureClosedSessionOwnerV2(runtime)
        run_task = asyncio.create_task(owner.run())
        await _wait_until_ready(fixture, run_task)

        await owner.request_normal_stop()
        result = await run_task

        assert result.runtime_result is runtime.result
        assert result.stop_reason == "OPERATOR_REQUESTED"
        assert result.integrity_ledger_clean_issued
        assert result.local_session_closure_issued
        assert result.retained_market_parser_health_certified
        assert type(result.parser_health_result) is (
            RetainedUsdmMarketParserHealthCertificateV2
        )
        assert result.session_closure_authority.manifest.stop_reason == (
            "OPERATOR_REQUESTED"
        )
        assert result.session_closure_authority.manifest_sha256 == (
            result.parser_health_result.session_closure_manifest_sha256
        )
        assert result.ledger_seal_receipt.sha256 == (
            result.parser_health_result.ledger_clean_closure_receipt_sha256
        )
        assert not result.observed_source_completeness_claimed
        assert not result.m2_eligible
        assert not result.strategy_ready
        assert not result.probability_calibrated
        assert not result.paper_fok_enabled
        assert not result.mandatory_exit_enabled
        assert not result.efficacy_claimed
        assert not result.pnl_or_profit_claimed
        assert not result.production_order_execution_enabled
        encoded = canonical_public_capture_closed_session_result_v2(result)
        assert json.loads(encoded)["result_sha256"] == result.result_sha256
        assert owner.validate_current() == result.result_sha256

        with pytest.raises(RuntimeError, match="exact outer owner"):
            replace(result)
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV2,
            match="only once",
        ):
            await owner.run()

        mutated_runtime = replace(
            result.runtime_result,
            normal_stop_receipt=ReceiptTimestamp(
                result.runtime_result.normal_stop_receipt.received_at_ms + 1,
                result.runtime_result.normal_stop_receipt.received_monotonic_ns,
            ),
        )
        object.__setattr__(result, "runtime_result", mutated_runtime)
        with pytest.raises(RuntimeError, match="runtime result hash differs"):
            canonical_public_capture_closed_session_result_v2(result)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_closed_session_owner_keeps_incomplete_parser_census_noncertifying(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path, market_frames=(_valid_agg_trade_frame(),))
    try:
        runtime = fixture.runtime()
        owner = PublicCaptureClosedSessionOwnerV2(runtime)
        run_task = asyncio.create_task(owner.run())
        await _wait_until_ready(fixture, run_task)

        await owner.request_normal_stop()
        result = await run_task

        assert result.stop_reason == "OPERATOR_REQUESTED"
        assert type(result.parser_health_result) is (
            RetainedUsdmMarketParserHealthNoncertifyingV2
        )
        assert result.parser_health_result.issue_codes == (
            "MISSING_PLANNED_STREAM",
        )
        assert not result.retained_market_parser_health_certified
        assert not result.observed_source_completeness_claimed
        assert not result.m2_eligible
        assert not result.strategy_ready
        assert not result.paper_fok_enabled
        assert not result.mandatory_exit_enabled
        assert not result.efficacy_claimed
        assert not result.pnl_or_profit_claimed
        assert owner.validate_current() == result.result_sha256
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_closed_session_owner_rejects_abnormal_stop_reason(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()
        constructor = cast(
            Callable[..., PublicCaptureClosedSessionOwnerV2],
            PublicCaptureClosedSessionOwnerV2,
        )

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            constructor(
                runtime,
                stop_reason="COMPLETED_DURATION",
            )

        assert not runtime.started_once
        assert fixture.market_connector.urls == []
        assert fixture.public_connector.urls == []
        assert fixture.rest_requests == []
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_closed_session_result_rejects_cross_session_runtime_splice(
    tmp_path: Path,
) -> None:
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first_root.mkdir()
    second_root.mkdir()
    first = _Fixture(first_root, market_frames=_all_valid_market_frames())
    second = _Fixture(
        second_root,
        market_frames=_all_valid_market_frames(),
        process_boot_id="fedcba9876543210fedcba9876543210",
    )
    try:
        first_owner = PublicCaptureClosedSessionOwnerV2(first.runtime())
        second_owner = PublicCaptureClosedSessionOwnerV2(second.runtime())
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
        first_result, second_result = await asyncio.gather(
            first_task,
            second_task,
        )
        assert first_result.session_closure_authority.manifest.session_id != (
            second_result.session_closure_authority.manifest.session_id
        )

        object.__setattr__(
            first_result,
            "runtime_result",
            second_result.runtime_result,
        )
        object.__setattr__(
            first_result,
            "runtime_result_sha256",
            second_result.runtime_result_sha256,
        )

        with pytest.raises(RuntimeError, match="do not share one authority"):
            canonical_public_capture_closed_session_result_v2(first_result)
    finally:
        await first.close()
        await second.close()


@pytest.mark.asyncio
async def test_closed_session_owner_shields_irreversible_finalization_from_cancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _Fixture(tmp_path, market_frames=_all_valid_market_frames())
    finalization_entered = asyncio.Event()
    allow_finalization = asyncio.Event()
    original_finalize = PublicCaptureClosedSessionOwnerV2._finalize_owned

    async def delayed_finalize(
        owner: PublicCaptureClosedSessionOwnerV2,
        runtime_result: PublicCaptureRuntimeResultV2,
    ) -> PublicCaptureClosedSessionResultV2:
        finalization_entered.set()
        await allow_finalization.wait()
        return await original_finalize(owner, runtime_result)

    monkeypatch.setattr(
        PublicCaptureClosedSessionOwnerV2,
        "_finalize_owned",
        delayed_finalize,
    )
    try:
        runtime = fixture.runtime()
        owner = PublicCaptureClosedSessionOwnerV2(runtime)
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
        assert result.integrity_ledger_clean_issued
        assert result.local_session_closure_issued
        assert not result.strategy_ready
        assert not result.paper_fok_enabled
        assert not result.production_order_execution_enabled
        assert owner.validate_current() == result.result_sha256
    finally:
        allow_finalization.set()
        await fixture.close()


@pytest.mark.asyncio
async def test_closed_session_owner_fails_closed_after_seal_before_closure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _Fixture(tmp_path, market_frames=_all_valid_market_frames())

    def fail_closure_write(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RuntimeError("injected session closure failure")

    monkeypatch.setattr(
        closed_session_owner_module,
        "write_session_closure_manifest_v2",
        fail_closure_write,
    )
    try:
        runtime = fixture.runtime()
        owner = PublicCaptureClosedSessionOwnerV2(runtime)
        run_task = asyncio.create_task(owner.run())
        await _wait_until_ready(fixture, run_task)
        await owner.request_normal_stop()

        with pytest.raises(RuntimeError, match="injected session closure failure"):
            await run_task

        assert owner.result is None
        assert (
            fixture.ledger_path / "capture-clean-closure-seal.json"
        ).is_file()
        assert not canonical_session_closure_manifest_path_v2(
            fixture.lease
        ).exists()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV2,
            match="no current result",
        ):
            owner.validate_current()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV2,
            match="only once",
        ):
            await owner.run()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV2,
            match="fresh capture runtime",
        ):
            PublicCaptureClosedSessionOwnerV2(runtime)
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_closed_session_owner_surfaces_post_closure_parser_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _Fixture(tmp_path, market_frames=_all_valid_market_frames())

    def fail_parser_health(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise RuntimeError("injected retained parser failure")

    monkeypatch.setattr(
        closed_session_owner_module,
        "certify_retained_usdm_market_parser_health_v2",
        fail_parser_health,
    )
    try:
        runtime = fixture.runtime()
        owner = PublicCaptureClosedSessionOwnerV2(runtime)
        run_task = asyncio.create_task(owner.run())
        await _wait_until_ready(fixture, run_task)
        await owner.request_normal_stop()

        with pytest.raises(RuntimeError, match="injected retained parser failure"):
            await run_task

        assert owner.result is None
        assert (
            fixture.ledger_path / "capture-clean-closure-seal.json"
        ).is_file()
        assert canonical_session_closure_manifest_path_v2(
            fixture.lease
        ).is_file()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV2,
            match="no current result",
        ):
            owner.validate_current()
        with pytest.raises(
            PublicCaptureClosedSessionOwnerStateErrorV2,
            match="only once",
        ):
            await owner.run()
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_retained_market_health_fails_closed_after_signed_block_tamper(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path, market_frames=(_valid_agg_trade_frame(),))
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)
        manifest_path = next(fixture.block_path.glob("block-*.manifest.json"))
        manifest_document = json.loads(manifest_path.read_bytes())
        data_path = fixture.block_path / cast(str, manifest_document["data_file"])
        encoded = data_path.read_bytes()
        data_path.write_bytes(encoded[:-1] + bytes((encoded[-1] ^ 1,)))

        with pytest.raises(BlockIntegrityError):
            _market_parser_health(
                fixture,
                ledger_seal=ledger_seal,
                closure=closure,
            )
    finally:
        await fixture.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("frame", "unknown_stream_count", "first_issue"),
    (
        (
            b'{"stream":"btcusdt@aggTrade","data":{}}',
            0,
            "STRICT_M1_PARSE_FAILURE",
        ),
        (
            b'{"stream":"btcusdt@liquidation","data":{}}',
            1,
            "UNKNOWN_OR_UNPLANNED_STREAM",
        ),
    ),
)
async def test_signed_parse_failure_or_unknown_stream_is_noncertifying(
    tmp_path: Path,
    frame: bytes,
    unknown_stream_count: int,
    first_issue: str,
) -> None:
    fixture = _Fixture(tmp_path, market_frames=(frame,))
    try:
        _runtime_result, ledger_seal, closure = await _finish_clean_capture(fixture)

        result = _market_parser_health(
            fixture,
            ledger_seal=ledger_seal,
            closure=closure,
        )

        assert type(result) is RetainedUsdmMarketParserHealthNoncertifyingV2
        assert result.market_record_count == result.parse_failure_count == 1
        assert result.successful_parse_count == 0
        assert result.unknown_stream_count == unknown_stream_count
        assert result.missing_planned_stream_count == 3
        assert result.issue_codes[0] == first_issue
        assert not result.retained_market_prefix_parser_health_certified
        assert not result.m2_certified
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_runtime_run_is_one_shot_under_concurrent_callers(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()
        first = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, first)

        with pytest.raises(PublicCaptureRuntimeStateErrorV2, match="only once"):
            await runtime.run()

        await runtime.request_normal_stop()
        await first
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_prestart_normal_stop_rejects_without_task_or_io(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()

        with pytest.raises(PublicCaptureRuntimeStateErrorV2, match="trio"):
            await runtime.request_normal_stop()

        assert runtime.producer_task_count == 0
        assert fixture.scheduler.normal_stop_receipt is None
        assert fixture.market_connector.urls == []
        assert fixture.public_connector.urls == []
        assert fixture.rest_requests == []
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_normal_stop_rejects_producer_that_won_the_terminal_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _Fixture(tmp_path)
    observe_gate = asyncio.Event()
    observer_entered = asyncio.Event()
    original_observer = PublicCaptureRuntimeV2._await_producer_trio

    async def delayed_observer(runtime: PublicCaptureRuntimeV2) -> None:
        observer_entered.set()
        await observe_gate.wait()
        await original_observer(runtime)

    monkeypatch.setattr(
        PublicCaptureRuntimeV2,
        "_await_producer_trio",
        delayed_observer,
    )
    try:
        runtime = fixture.runtime()
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)
        await observer_entered.wait()

        fixture.market_composition.lifecycle_coordinator.stop_event.set()
        for _attempt in range(2_000):
            if any(task.done() for task in runtime._producer_tasks):
                break
            await asyncio.sleep(0.001)
        else:
            raise AssertionError("no producer reached the terminal race seam")

        with pytest.raises(PublicCaptureRuntimeStateErrorV2, match="terminal producer"):
            await runtime.request_normal_stop()

        assert runtime._normal_stop_owner is None
        assert fixture.scheduler.normal_stop_receipt is None
        observe_gate.set()
        with pytest.raises(
            PublicCaptureRuntimeShutdownErrorV2,
            match="before runtime normal stop",
        ):
            await run_task

        assert runtime.result is None
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
        assert all(task.done() for task in runtime._producer_tasks)
    finally:
        observe_gate.set()
        await fixture.close()


@pytest.mark.asyncio
async def test_cancelled_stop_caller_cannot_lose_committed_stop(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)
        stop_task = asyncio.create_task(runtime.request_normal_stop())
        await asyncio.sleep(0)
        stop_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await stop_task
        result = await run_task

        assert fixture.scheduler.normal_stop_receipt == result.normal_stop_receipt
        assert fixture.scheduler.coverage_closed
        assert result.oi_coverage_closed
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_constructor_claim_blocks_direct_composition_and_owner_bypass(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        fixture.runtime()

        with pytest.raises(PublicWebSocketRuntimeClaimErrorV2, match="direct run"):
            await fixture.market_composition.run()
        with pytest.raises(PublicWebSocketRuntimeClaimErrorV2, match="foreign task"):
            await fixture.market_composition.owner.run(
                fixture.market_composition.lifecycle_coordinator.stop_event
            )
        assert fixture.market_composition.owner.generation == 0
        assert fixture.market_connector.urls == []
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_concurrent_direct_owner_task_is_rejected_and_fatal_visible(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)

        with pytest.raises(PublicWebSocketRuntimeClaimErrorV2, match="foreign task"):
            await fixture.market_composition.owner.run(
                fixture.market_composition.lifecycle_coordinator.stop_event
            )
        with pytest.raises(PublicWebSocketRuntimeClaimErrorV2, match="foreign task"):
            await run_task

        assert len(fixture.market_connector.urls) == 1
        assert fixture.market_composition.owner.generation == 1
        assert runtime.result is None
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_run_revalidates_drift_before_any_producer_task_or_connector(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()
        fixture.rest_adapter.session_id = "drifted-session"

        with pytest.raises(PublicCaptureRuntimeBindingErrorV2, match="session IDs"):
            await runtime.run()

        assert runtime.producer_task_count == 0
        assert fixture.market_connector.urls == []
        assert fixture.public_connector.urls == []
        assert fixture.rest_requests == []
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_caller_cancellation_drains_without_clean_tail_finality(
    tmp_path: Path,
) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)

        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task

        assert runtime.result is None
        assert fixture.rest_adapter.closed
        assert fixture.rest_adapter.fully_drained
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
        assert all(task.done() for task in runtime._producer_tasks), (
            [(task.get_name(), task.done(), task.cancelling()) for task in runtime._producer_tasks],
            getattr(run_task.exception(), "__notes__", ()),
        )
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_fatal_stop_never_promotes_to_clean_tail_result(tmp_path: Path) -> None:
    fixture = _Fixture(tmp_path)
    try:
        runtime = fixture.runtime()
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)
        failure = RuntimeError("injected runtime fatal")
        fixture.pipeline.handoff.fail_consumer(failure, failing_ingest_seq=None)

        with pytest.raises(RuntimeError, match="injected runtime fatal"):
            await run_task

        assert runtime.result is None
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
        assert fixture.rest_adapter.closed
        assert all(task.done() for task in runtime._producer_tasks), (
            [(task.get_name(), task.done(), task.cancelling()) for task in runtime._producer_tasks],
            getattr(run_task.exception(), "__notes__", ()),
        )
        assert runtime._adapter_close_task is not None
        assert runtime._adapter_close_task.done()
    finally:
        await fixture.close()


@pytest.mark.asyncio
async def test_normal_producer_drain_timeout_skips_finality(tmp_path: Path) -> None:
    exit_gate = asyncio.Event()
    fixture = _Fixture(tmp_path, public_exit_gate=exit_gate)
    try:
        runtime = fixture.runtime(producer_timeout=0.01)
        run_task = asyncio.create_task(runtime.run())
        await _wait_until_ready(fixture, run_task)

        async def release_exit_gate() -> None:
            await asyncio.sleep(0.05)
            exit_gate.set()

        release_task = asyncio.create_task(release_exit_gate())
        await runtime.request_normal_stop()

        with pytest.raises(PublicCaptureRuntimeShutdownErrorV2, match="drain"):
            await run_task

        assert runtime.result is None
        assert fixture.pipeline.handoff.clean_tail_shutdown_request is None
        assert fixture.rest_adapter.closed
        assert all(task.done() for task in runtime._producer_tasks), (
            [(task.get_name(), task.done(), task.cancelling()) for task in runtime._producer_tasks],
            getattr(run_task.exception(), "__notes__", ()),
        )
        assert runtime._adapter_close_task is not None
        assert runtime._adapter_close_task.done()
        assert runtime._normal_stop_owner is not None
        assert runtime._normal_stop_owner.done()
        assert runtime._pipeline_stop_task is not None
        assert runtime._pipeline_stop_task.done()
        await release_task
    finally:
        exit_gate.set()
        await fixture.close()
