"""Owned CLEAN-session finalization for one prospective public capture run.

This sibling composes existing capture authorities only.  It does not parse a
strategy decision, enable PAPER execution, claim M2 completeness, or emit an
alert/order.  The two durable authorities remain the existing integrity-ledger
CLEAN seal and fixed-path session-closure manifest; parser health is a
deterministic current-storage result that can be recomputed from that closure.
There is deliberately no process-restart resume/reopen API: a crash between
irreversible stages leaves only auditable, non-promoting artifacts for an
offline operator decision.
"""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import InitVar, dataclass, field
from typing import Final, Literal, cast

from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp
from signalbot.capture.writer_lease import WriterLease
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.blocks import GroupedBlockWriterV2
from signalbot.r4b_v2.capture.full_runtime import (
    PublicCaptureRuntimeResultV2,
    PublicCaptureRuntimeResultV8,
    PublicCaptureRuntimeV2,
    PublicCaptureRuntimeV8,
)
from signalbot.r4b_v2.capture.integrity_ledger import (
    CaptureIntegrityLedgerV2,
    PersistedCaptureCleanClosureSealReceiptV2,
    PersistedCaptureCleanClosureSealReceiptV8,
)
from signalbot.r4b_v2.capture.mirrored_wal import MirroredWalWriterV2
from signalbot.r4b_v2.capture.pipeline import (
    CaptureBatchPipelineV2,
    DurableCaptureBatchWriterV2,
)
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingPlanV2,
    ProvisionalPromotingPlanV8,
    provisional_promoting_plan_sha256_v8,
    validate_provisional_promoting_capture_plans_v8,
)
from signalbot.r4b_v2.capture.rest_census import PublicOiRestCoverageCloseV2
from signalbot.r4b_v2.capture.rest_depth import public_depth_rest_plan_sha256_v8
from signalbot.r4b_v2.capture.rest_depth_bridge_evidence import (
    depth_bridge_coordinator_closure_entry_v8,
    validate_depth_bridge_coordinator_clean_close_receipt_v8,
)
from signalbot.r4b_v2.capture.session import (
    PersistedSessionClosureAuthorityV2,
    PersistedSessionClosureAuthorityV8,
    PersistedSessionStartAuthorityV2,
    assert_persisted_session_closure_authority_current_v2,
    assert_persisted_session_closure_authority_current_v8,
    canonical_session_closure_manifest_path_v2,
    canonical_session_closure_manifest_path_v8,
    write_session_closure_manifest_v2,
    write_session_closure_manifest_v8,
)
from signalbot.r4b_v2.capture.usdm_market_prefix_health import (
    RetainedUsdmMarketParserHealthCertificateV2,
    RetainedUsdmMarketParserHealthNoncertifyingV2,
    RetainedUsdmMarketParserHealthResultV2,
    canonical_retained_usdm_market_parser_health_result_v2,
    certify_retained_usdm_market_parser_health_v2,
)
from signalbot.r4b_v2.capture.websocket import (
    validate_public_oi_census_admission_receipt_v2,
)

type PublicCaptureNormalStopReasonV2 = Literal["OPERATOR_REQUESTED"]
type PublicCaptureNormalStopReasonV8 = Literal["OPERATOR_REQUESTED"]

PUBLIC_CAPTURE_CLOSED_SESSION_ROLE_V2: Final = (
    "PUBLIC_CAPTURE_LOCAL_CLEAN_CLOSURE_AND_RETAINED_MARKET_PARSER_HEALTH"
)
PUBLIC_CAPTURE_CLOSED_SESSION_ROLE_V8: Final = (
    "PUBLIC_CAPTURE_V8_LOCAL_INFRASTRUCTURE_CLEAN_CLOSURE_ONLY"
)

_RESULT_SCHEMA: Final = "r4b_v2_public_capture_closed_session_result_v1"
_RESULT_DOMAIN: Final = b"R4B_V2_PUBLIC_CAPTURE_CLOSED_SESSION_RESULT\0"
_RUNTIME_RESULT_DOMAIN: Final = b"R4B_V2_PUBLIC_CAPTURE_RUNTIME_RESULT\0"
_OI_CENSUS_RECORD_DOMAIN: Final = b"R4B_V2_PUBLIC_OI_CENSUS_RECORD\0"
_RESULT_FACTORY_TOKEN: Final = object()
_RESULT_SCHEMA_V8: Final = "r4b_v2_public_capture_closed_session_result_v8"
_RESULT_DOMAIN_V8: Final = b"R4B_V2_PUBLIC_CAPTURE_CLOSED_SESSION_RESULT_V8\0"
_RUNTIME_RESULT_DOMAIN_V8: Final = b"R4B_V2_PUBLIC_CAPTURE_RUNTIME_RESULT_V8\0"
_OI_CENSUS_RECORD_DOMAIN_V8: Final = b"R4B_V2_OI_COVERAGE_CLOSE_RECORD_V8\0"
_RESULT_FACTORY_TOKEN_V8: Final = object()


class PublicCaptureClosedSessionOwnerErrorV2(RuntimeError):
    """The exact runtime or one of its persisted closure owners is invalid."""


class PublicCaptureClosedSessionOwnerStateErrorV2(RuntimeError):
    """The one-shot outer owner was replayed or queried before completion."""


class PublicCaptureClosedSessionOwnerErrorV8(RuntimeError):
    """The exact V8 runtime or one of its persisted closure owners is invalid."""


class PublicCaptureClosedSessionOwnerStateErrorV8(RuntimeError):
    """The one-shot V8 outer owner was replayed or queried before completion."""


@dataclass(frozen=True, slots=True)
class PublicCaptureClosedSessionResultV2:
    """Factory result for local CLEAN closure plus retained parser health.

    A certifying parser result proves strict parsing of the retained local
    market prefix only.  Every completeness, strategy, execution, efficacy,
    probability, PnL, profit, and production-order claim remains false.
    """

    runtime_result: PublicCaptureRuntimeResultV2 = field(repr=False)
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV2 = field(
        repr=False
    )
    session_closure_authority: PersistedSessionClosureAuthorityV2 = field(
        repr=False
    )
    parser_health_result: RetainedUsdmMarketParserHealthResultV2 = field(
        repr=False
    )
    stop_reason: PublicCaptureNormalStopReasonV2
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)
    runtime_result_sha256: str = field(init=False)
    result_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_RESULT_SCHEMA)
    role: str = field(init=False, default=PUBLIC_CAPTURE_CLOSED_SESSION_ROLE_V2)
    integrity_ledger_clean_issued: Literal[True] = field(init=False, default=True)
    local_session_closure_issued: Literal[True] = field(init=False, default=True)
    retained_market_parser_health_certified: bool = field(init=False)
    observed_source_completeness_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    m2_eligible: Literal[False] = field(init=False, default=False)
    strategy_ready: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    paper_fok_enabled: Literal[False] = field(init=False, default=False)
    mandatory_exit_enabled: Literal[False] = field(init=False, default=False)
    efficacy_claimed: Literal[False] = field(init=False, default=False)
    pnl_or_profit_claimed: Literal[False] = field(init=False, default=False)
    production_order_execution_enabled: Literal[False] = field(
        init=False,
        default=False,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _RESULT_FACTORY_TOKEN:
            raise PublicCaptureClosedSessionOwnerErrorV2(
                "closed-session results require the exact outer owner"
            )
        object.__setattr__(self, "_factory_seal", _RESULT_FACTORY_TOKEN)
        object.__setattr__(
            self,
            "retained_market_parser_health_certified",
            type(self.parser_health_result)
            is RetainedUsdmMarketParserHealthCertificateV2,
        )
        object.__setattr__(
            self,
            "runtime_result_sha256",
            _runtime_result_sha256(self.runtime_result),
        )
        _validate_result_material(self)
        object.__setattr__(
            self,
            "result_sha256",
            _result_sha256(self),
        )


@dataclass(frozen=True, slots=True)
class PublicCaptureClosedSessionResultV8:
    """Factory result for exact V8 infrastructure closure without strategy claims."""

    runtime_result: PublicCaptureRuntimeResultV8 = field(repr=False)
    ledger_seal_receipt: PersistedCaptureCleanClosureSealReceiptV8 = field(
        repr=False
    )
    session_closure_authority: PersistedSessionClosureAuthorityV8 = field(
        repr=False
    )
    stop_reason: PublicCaptureNormalStopReasonV8
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)
    runtime_result_sha256: str = field(init=False)
    result_sha256: str = field(init=False)
    schema_version: str = field(init=False, default=_RESULT_SCHEMA_V8)
    role: str = field(init=False, default=PUBLIC_CAPTURE_CLOSED_SESSION_ROLE_V8)
    integrity_ledger_clean_issued: Literal[True] = field(init=False, default=True)
    local_session_closure_issued: Literal[True] = field(init=False, default=True)
    depth_bridge_lifecycle_cleanly_closed: Literal[True] = field(
        init=False,
        default=True,
    )
    websocket_route_cursor_finality_persisted: Literal[True] = field(
        init=False,
        default=True,
    )
    oi_coverage_closed: Literal[True] = field(init=False, default=True)
    retained_frame_parser_health_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    retained_market_parser_health_certified: Literal[False] = field(
        init=False,
        default=False,
    )
    websocket_retained_frame_parser_health_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    websocket_upstream_message_completeness_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    observed_source_completeness_claimed: Literal[False] = field(
        init=False,
        default=False,
    )
    oi_data_completeness_claimed: Literal[False] = field(init=False, default=False)
    depth_bridge_complete_claimed: Literal[False] = field(init=False, default=False)
    book_completeness_claimed: Literal[False] = field(init=False, default=False)
    book_bridge_certified: Literal[False] = field(init=False, default=False)
    m2_certified: Literal[False] = field(init=False, default=False)
    m2_eligible: Literal[False] = field(init=False, default=False)
    strategy_ready: Literal[False] = field(init=False, default=False)
    promotion_ready: Literal[False] = field(init=False, default=False)
    probability_calibrated: Literal[False] = field(init=False, default=False)
    paper_execution_enabled: Literal[False] = field(init=False, default=False)
    paper_fok_enabled: Literal[False] = field(init=False, default=False)
    mandatory_exit_enabled: Literal[False] = field(init=False, default=False)
    efficacy_claimed: Literal[False] = field(init=False, default=False)
    pnl_or_profit_claimed: Literal[False] = field(init=False, default=False)
    order_execution_enabled: Literal[False] = field(init=False, default=False)
    private_credentials_permitted: Literal[False] = field(init=False, default=False)
    production_order_execution_enabled: Literal[False] = field(
        init=False,
        default=False,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _RESULT_FACTORY_TOKEN_V8:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 closed-session results require the exact outer owner"
            )
        object.__setattr__(self, "_factory_seal", _RESULT_FACTORY_TOKEN_V8)
        object.__setattr__(
            self,
            "runtime_result_sha256",
            _runtime_result_sha256_v8(self.runtime_result),
        )
        _validate_result_material_v8(self)
        object.__setattr__(self, "result_sha256", _result_sha256_v8(self))


class PublicCaptureClosedSessionOwnerV2:
    """Run one existing public capture runtime, then own CLEAN finalization.

    The irreversible finalization task is cancellation-shielded once the inner
    runtime has returned its verified local finality result.  A failure after a
    durable CLEAN seal is never repaired or silently retried by this owner; the
    partially finalized session remains non-promoting and must be audited.  A
    new process cannot resume or reopen this owner; restart recovery is outside
    this deliberately narrow slice.
    """

    def __init__(
        self,
        runtime: PublicCaptureRuntimeV2,
    ) -> None:
        if type(runtime) is not PublicCaptureRuntimeV2:
            raise TypeError("runtime must be an exact PublicCaptureRuntimeV2")
        if runtime.started_once or runtime.running or runtime.result is not None:
            raise PublicCaptureClosedSessionOwnerStateErrorV2(
                "closed-session owner requires a fresh capture runtime"
            )
        runtime.validate_current()

        market = runtime.websocket_compositions[0]
        writer = runtime.pipeline.writer
        if type(writer) is not DurableCaptureBatchWriterV2:
            raise PublicCaptureClosedSessionOwnerErrorV2(
                "closed-session owner requires the exact durable batch writer"
            )
        durable_writer = cast(DurableCaptureBatchWriterV2, writer)
        if type(durable_writer.wal_writer) is not MirroredWalWriterV2:
            raise PublicCaptureClosedSessionOwnerErrorV2(
                "CLEAN closure requires the exact mirrored WAL owner"
            )
        if type(durable_writer.block_writer) is not GroupedBlockWriterV2:
            raise PublicCaptureClosedSessionOwnerErrorV2(
                "CLEAN closure requires the exact grouped-block owner"
            )
        if type(market.writer_lease) is not WriterLease:
            raise PublicCaptureClosedSessionOwnerErrorV2(
                "CLEAN closure requires the exact held WriterLease"
            )
        if (
            durable_writer.writer_lease is not market.writer_lease
            or runtime.integrity_ledger.writer_lease is not market.writer_lease
        ):
            raise PublicCaptureClosedSessionOwnerErrorV2(
                "runtime storage owners differ on WriterLease identity"
            )
        if type(market.session_start_authority) is not PersistedSessionStartAuthorityV2:
            raise PublicCaptureClosedSessionOwnerErrorV2(
                "runtime lacks the exact persisted session-start authority"
            )

        self.runtime = runtime
        self.stop_reason: PublicCaptureNormalStopReasonV2 = "OPERATOR_REQUESTED"
        self.pipeline = cast(CaptureBatchPipelineV2, runtime.pipeline)
        self.integrity_ledger = cast(
            CaptureIntegrityLedgerV2,
            runtime.integrity_ledger,
        )
        self.wal_writer = cast(MirroredWalWriterV2, durable_writer.wal_writer)
        self.block_writer = durable_writer.block_writer
        self.writer_lease = market.writer_lease
        self.session_start_authority = market.session_start_authority
        self.promoting_plans = cast(
            tuple[ProvisionalPromotingPlanV2, ...],
            market.promoting_plans,
        )
        self.receipt_clock = cast(ReceiptClock, market.lifecycle_coordinator.clock)
        self._started_once = False
        self._running = False
        self._finalization_task: (
            asyncio.Task[PublicCaptureClosedSessionResultV2] | None
        ) = None
        self._terminal_failure: BaseException | None = None
        self._result: PublicCaptureClosedSessionResultV2 | None = None

    @property
    def started_once(self) -> bool:
        return self._started_once

    @property
    def running(self) -> bool:
        return self._running

    @property
    def result(self) -> PublicCaptureClosedSessionResultV2 | None:
        return self._result

    async def request_normal_stop(self) -> ReceiptTimestamp:
        """Delegate the write-once normal-stop request to the exact inner owner."""

        return await self.runtime.request_normal_stop()

    async def run(self) -> PublicCaptureClosedSessionResultV2:
        """Run capture once and complete the existing durable closure sequence."""

        if self._started_once:
            raise PublicCaptureClosedSessionOwnerStateErrorV2(
                "closed-session owner may run only once"
            )
        self._started_once = True
        self._running = True
        try:
            runtime_result = await self.runtime.run()
            finalization = asyncio.create_task(
                self._finalize_owned(runtime_result),
                name="r4b-v2-public-capture-closed-session-finalization",
            )
            self._finalization_task = finalization
            cancellation: asyncio.CancelledError | None = None
            while not finalization.done():
                try:
                    await asyncio.shield(finalization)
                except asyncio.CancelledError as exc:
                    if cancellation is None:
                        cancellation = exc
            result = finalization.result()
            self._result = result
            if cancellation is not None:
                raise cancellation
            return result
        except asyncio.CancelledError as exc:
            if self._result is None:
                self._terminal_failure = exc
            raise
        except BaseException as exc:
            self._terminal_failure = exc
            raise
        finally:
            self._running = False

    def validate_current(self) -> str:
        """Reprove both durable authorities and recompute parser health now."""

        result = self._result
        if result is None:
            if self._terminal_failure is not None:
                raise PublicCaptureClosedSessionOwnerStateErrorV2(
                    "failed closed-session owner has no current result"
                ) from self._terminal_failure
            raise PublicCaptureClosedSessionOwnerStateErrorV2(
                "closed-session result is not available"
            )
        if self.runtime.result is not result.runtime_result:
            raise PublicCaptureClosedSessionOwnerErrorV2(
                "inner runtime result identity differs from the closed-session result"
            )
        canonical_public_capture_closed_session_result_v2(result)
        assert_persisted_session_closure_authority_current_v2(
            result.session_closure_authority,
            lease=self.writer_lease,
            session_start_authority=self.session_start_authority,
            promoting_plans=self.promoting_plans,
            finality_receipt=result.runtime_result.finality_receipt,
            pipeline=self.pipeline,
            ledger_seal_receipt=result.ledger_seal_receipt,
            ledger=self.integrity_ledger,
        )
        refreshed = certify_retained_usdm_market_parser_health_v2(
            result.session_closure_authority,
            lease=self.writer_lease,
            session_start_authority=self.session_start_authority,
            promoting_plans=self.promoting_plans,
            pipeline=self.pipeline,
            ledger_seal_receipt=result.ledger_seal_receipt,
            integrity_ledger=self.integrity_ledger,
            block_writer=self.block_writer,
        )
        if (
            canonical_retained_usdm_market_parser_health_result_v2(refreshed)
            != canonical_retained_usdm_market_parser_health_result_v2(
                result.parser_health_result
            )
        ):
            raise PublicCaptureClosedSessionOwnerErrorV2(
                "current retained parser health differs from the issued result"
            )
        return result.result_sha256

    async def _finalize_owned(
        self,
        runtime_result: PublicCaptureRuntimeResultV2,
    ) -> PublicCaptureClosedSessionResultV2:
        if self.runtime.result is not runtime_result:
            raise PublicCaptureClosedSessionOwnerErrorV2(
                "outer finalization received a foreign runtime result"
            )
        seal_receipt = _capture_receipt(self.receipt_clock, "ledger CLEAN seal")
        ledger_seal = self.integrity_ledger.seal_clean_closure_v2(
            promoting_plans=self.promoting_plans,
            finality_receipt=runtime_result.finality_receipt,
            wal_writer=self.wal_writer,
            block_writer=self.block_writer,
            session_id=self.session_start_authority.manifest.session_id,
            process_boot_id=self.session_start_authority.manifest.process_boot_id,
            seal_wall_ms=seal_receipt.received_at_ms,
            seal_monotonic_ns=seal_receipt.received_monotonic_ns,
        )
        closed_receipt = _capture_receipt(self.receipt_clock, "session closure")
        closure = write_session_closure_manifest_v2(
            canonical_session_closure_manifest_path_v2(self.writer_lease),
            lease=self.writer_lease,
            session_start_authority=self.session_start_authority,
            promoting_plans=self.promoting_plans,
            finality_receipt=runtime_result.finality_receipt,
            pipeline=self.pipeline,
            ledger_seal_receipt=ledger_seal,
            ledger=self.integrity_ledger,
            stop_reason=self.stop_reason,
            closed_wall_ms=closed_receipt.received_at_ms,
            closed_monotonic_ns=closed_receipt.received_monotonic_ns,
            finalized_websocket_route_cursors=runtime_result.websocket_route_cursors,
        )
        parser_health = certify_retained_usdm_market_parser_health_v2(
            closure,
            lease=self.writer_lease,
            session_start_authority=self.session_start_authority,
            promoting_plans=self.promoting_plans,
            pipeline=self.pipeline,
            ledger_seal_receipt=ledger_seal,
            integrity_ledger=self.integrity_ledger,
            block_writer=self.block_writer,
        )
        return PublicCaptureClosedSessionResultV2(
            runtime_result=runtime_result,
            ledger_seal_receipt=ledger_seal,
            session_closure_authority=closure,
            parser_health_result=parser_health,
            stop_reason=self.stop_reason,
            _factory_token=_RESULT_FACTORY_TOKEN,
        )


class PublicCaptureClosedSessionOwnerV8:
    """Run one fresh V8 runtime and own its irreversible local CLEAN closure."""

    def __init__(self, runtime: PublicCaptureRuntimeV8) -> None:
        if type(runtime) is not PublicCaptureRuntimeV8:
            raise TypeError("runtime must be an exact PublicCaptureRuntimeV8")
        if runtime.started_once or runtime.running or runtime.result is not None:
            raise PublicCaptureClosedSessionOwnerStateErrorV8(
                "V8 closed-session owner requires a fresh capture runtime"
            )
        runtime.validate_current()

        market, public = runtime.websocket_compositions
        pipeline = runtime.pipeline
        if type(pipeline) is not CaptureBatchPipelineV2:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 closed-session owner requires the exact capture pipeline"
            )
        writer = pipeline.writer
        if type(writer) is not DurableCaptureBatchWriterV2:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 closed-session owner requires the exact durable batch writer"
            )
        durable_writer = cast(DurableCaptureBatchWriterV2, writer)
        if type(durable_writer.wal_writer) is not MirroredWalWriterV2:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 CLEAN closure requires the exact mirrored WAL owner"
            )
        if type(durable_writer.block_writer) is not GroupedBlockWriterV2:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 CLEAN closure requires the exact grouped-block owner"
            )
        if type(runtime.integrity_ledger) is not CaptureIntegrityLedgerV2:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 CLEAN closure requires the exact integrity ledger"
            )
        if type(market.writer_lease) is not WriterLease:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 CLEAN closure requires the exact held WriterLease"
            )
        writer_lease = market.writer_lease
        writer_lease.assert_held()
        if (
            public.writer_lease is not writer_lease
            or durable_writer.writer_lease is not writer_lease
            or runtime.integrity_ledger.writer_lease is not writer_lease
        ):
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 runtime storage owners differ on WriterLease identity"
            )
        if type(market.session_start_authority) is not PersistedSessionStartAuthorityV2:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 runtime lacks the exact persisted session-start authority"
            )
        if public.session_start_authority is not market.session_start_authority:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 runtime owners differ on session-start authority identity"
            )
        promoting_plans = market.promoting_plans
        if type(promoting_plans) is not tuple:
            raise TypeError("V8 closed-session owner requires an exact plan tuple")
        validate_provisional_promoting_capture_plans_v8(promoting_plans)
        if (
            promoting_plans is not public.promoting_plans
            or tuple(plan.route_id for plan in promoting_plans)
            != (
                "usdm_market",
                "usdm_public",
                "usdm_public_rest",
                "usdm_public_depth_rest",
            )
        ):
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 closed-session owner requires canonical four-plan identity"
            )
        depth_plan = promoting_plans[3]
        if type(depth_plan) is not ProvisionalDepthRestQualificationPlanV8:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 closed-session owner requires the exact depth plan member"
            )

        self.runtime = runtime
        self.stop_reason: PublicCaptureNormalStopReasonV8 = "OPERATOR_REQUESTED"
        self.pipeline = pipeline
        self.integrity_ledger = runtime.integrity_ledger
        self.wal_writer = cast(MirroredWalWriterV2, durable_writer.wal_writer)
        self.block_writer = durable_writer.block_writer
        self.writer_lease = writer_lease
        self.session_start_authority = market.session_start_authority
        self.promoting_plans = cast(
            tuple[ProvisionalPromotingPlanV8, ...],
            promoting_plans,
        )
        self.depth_plan = depth_plan
        self.receipt_clock = cast(ReceiptClock, market.lifecycle_coordinator.clock)
        self._started_once = False
        self._running = False
        self._finalization_task: (
            asyncio.Task[PublicCaptureClosedSessionResultV8] | None
        ) = None
        self._terminal_failure: BaseException | None = None
        self._result: PublicCaptureClosedSessionResultV8 | None = None

    @property
    def started_once(self) -> bool:
        return self._started_once

    @property
    def running(self) -> bool:
        return self._running

    @property
    def result(self) -> PublicCaptureClosedSessionResultV8 | None:
        return self._result

    async def request_normal_stop(self) -> ReceiptTimestamp:
        """Delegate the write-once normal-stop request to the exact V8 runtime."""

        return await self.runtime.request_normal_stop()

    async def run(self) -> PublicCaptureClosedSessionResultV8:
        """Run capture once and shield irreversible V8 finalization from cancellation."""

        if self._started_once:
            raise PublicCaptureClosedSessionOwnerStateErrorV8(
                "V8 closed-session owner may run only once"
            )
        self._started_once = True
        self._running = True
        try:
            runtime_result = await self.runtime.run()
            finalization = asyncio.create_task(
                self._finalize_owned(runtime_result),
                name="r4b-v8-public-capture-closed-session-finalization",
            )
            self._finalization_task = finalization
            cancellation: asyncio.CancelledError | None = None
            while not finalization.done():
                try:
                    await asyncio.shield(finalization)
                except asyncio.CancelledError as exc:
                    if cancellation is None:
                        cancellation = exc
            result = finalization.result()
            self._result = result
            if cancellation is not None:
                raise cancellation
            return result
        except asyncio.CancelledError as exc:
            if self._result is None:
                self._terminal_failure = exc
            raise
        except BaseException as exc:
            self._terminal_failure = exc
            raise
        finally:
            self._running = False

    def validate_current(self) -> str:
        """Reprove the exact V8 ledger seal and session authority from current files."""

        result = self._result
        if result is None:
            if self._terminal_failure is not None:
                raise PublicCaptureClosedSessionOwnerStateErrorV8(
                    "failed V8 closed-session owner has no current result"
                ) from self._terminal_failure
            raise PublicCaptureClosedSessionOwnerStateErrorV8(
                "V8 closed-session result is not available"
            )
        if self.runtime.result is not result.runtime_result:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "inner V8 runtime result identity differs from the closed-session result"
            )
        canonical_public_capture_closed_session_result_v8(result)
        start = self.session_start_authority.manifest
        current_ledger = self.integrity_ledger.verify_current_clean_closure_seal_v8(
            promoting_plans=self.promoting_plans,
            depth_plan=self.depth_plan,
            wal_writer=self.wal_writer,
            block_writer=self.block_writer,
            session_id=start.session_id,
            process_boot_id=start.process_boot_id,
        )
        if current_ledger is not result.ledger_seal_receipt:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "current V8 ledger receipt identity differs from the issued result"
            )
        bridge_entry = result.ledger_seal_receipt.seal.depth_bridge_closure_entry
        assert_persisted_session_closure_authority_current_v8(
            result.session_closure_authority,
            lease=self.writer_lease,
            session_start_authority=self.session_start_authority,
            promoting_plans=self.promoting_plans,
            depth_plan=self.depth_plan,
            finality_receipt=result.runtime_result.finality_receipt,
            pipeline=self.pipeline,
            ledger_seal_receipt=result.ledger_seal_receipt,
            ledger=self.integrity_ledger,
            depth_bridge_closure_entry=bridge_entry,
            finalized_websocket_route_cursors=(
                result.runtime_result.websocket_route_cursors
            ),
            oi_coverage_close_receipt=(
                result.runtime_result.oi_coverage_close_receipt
            ),
        )
        canonical_public_capture_closed_session_result_v8(result)
        return result.result_sha256

    async def _finalize_owned(
        self,
        runtime_result: PublicCaptureRuntimeResultV8,
    ) -> PublicCaptureClosedSessionResultV8:
        if self.runtime.result is not runtime_result:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "outer V8 finalization received a foreign runtime result"
            )
        if type(runtime_result) is not PublicCaptureRuntimeResultV8:
            raise TypeError("runtime_result must be an exact PublicCaptureRuntimeResultV8")
        runtime_result.__post_init__()
        if runtime_result.promoting_plans is not self.promoting_plans:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 runtime result differs from the owned four-plan tuple"
            )
        if self.promoting_plans[3] is not self.depth_plan:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 outer owner lost exact depth-plan member identity"
            )

        seal_time = _capture_receipt_v8(self.receipt_clock, "V8 ledger CLEAN seal")
        ledger_seal = self.integrity_ledger.seal_clean_closure_v8(
            promoting_plans=self.promoting_plans,
            depth_plan=self.depth_plan,
            depth_bridge_close_receipt=runtime_result.depth_bridge_close_receipt,
            finalized_websocket_cursor_pair=runtime_result.websocket_route_cursors,
            finality_receipt=runtime_result.finality_receipt,
            wal_writer=self.wal_writer,
            block_writer=self.block_writer,
            session_id=self.session_start_authority.manifest.session_id,
            process_boot_id=self.session_start_authority.manifest.process_boot_id,
            seal_wall_ms=seal_time.received_at_ms,
            seal_monotonic_ns=seal_time.received_monotonic_ns,
        )
        bridge_entry = ledger_seal.seal.depth_bridge_closure_entry
        close_time = _capture_receipt_v8(self.receipt_clock, "V8 session closure")
        closure = write_session_closure_manifest_v8(
            canonical_session_closure_manifest_path_v8(self.writer_lease),
            lease=self.writer_lease,
            session_start_authority=self.session_start_authority,
            promoting_plans=self.promoting_plans,
            depth_plan=self.depth_plan,
            finality_receipt=runtime_result.finality_receipt,
            pipeline=self.pipeline,
            ledger_seal_receipt=ledger_seal,
            ledger=self.integrity_ledger,
            depth_bridge_close_receipt=runtime_result.depth_bridge_close_receipt,
            depth_bridge_closure_entry=bridge_entry,
            finalized_websocket_route_cursors=runtime_result.websocket_route_cursors,
            oi_coverage_close_receipt=runtime_result.oi_coverage_close_receipt,
            stop_reason=self.stop_reason,
            closed_wall_ms=close_time.received_at_ms,
            closed_monotonic_ns=close_time.received_monotonic_ns,
        )
        return PublicCaptureClosedSessionResultV8(
            runtime_result=runtime_result,
            ledger_seal_receipt=ledger_seal,
            session_closure_authority=closure,
            stop_reason=self.stop_reason,
            _factory_token=_RESULT_FACTORY_TOKEN_V8,
        )


def canonical_public_capture_closed_session_result_v2(
    result: PublicCaptureClosedSessionResultV2,
) -> bytes:
    """Return the compact canonical hash binding for one completed outer run."""

    if type(result) is not PublicCaptureClosedSessionResultV2:
        raise TypeError("result must be an exact PublicCaptureClosedSessionResultV2")
    _validate_result_material(result)
    if result.result_sha256 != _result_sha256(result):
        raise PublicCaptureClosedSessionOwnerErrorV2(
            "closed-session result hash differs from canonical content"
        )
    return canonical_json_line(_result_document(result, include_result_hash=True))


def _validate_result_material(result: PublicCaptureClosedSessionResultV2) -> None:
    if getattr(result, "_factory_seal", None) is not _RESULT_FACTORY_TOKEN:
        raise PublicCaptureClosedSessionOwnerErrorV2(
            "closed-session result lacks exact outer-owner provenance"
        )
    if type(result.runtime_result) is not PublicCaptureRuntimeResultV2:
        raise TypeError("runtime_result must be exact")
    result.runtime_result.__post_init__()
    expected_runtime_hash = _runtime_result_sha256(result.runtime_result)
    if result.runtime_result_sha256 != expected_runtime_hash:
        raise PublicCaptureClosedSessionOwnerErrorV2(
            "closed-session runtime result hash differs from canonical content"
        )
    if type(result.ledger_seal_receipt) is not (
        PersistedCaptureCleanClosureSealReceiptV2
    ):
        raise TypeError("ledger_seal_receipt must be exact")
    result.ledger_seal_receipt.__post_init__()
    if type(result.session_closure_authority) is not (
        PersistedSessionClosureAuthorityV2
    ):
        raise TypeError("session_closure_authority must be exact")
    result.session_closure_authority.__post_init__()
    if type(result.parser_health_result) not in {
        RetainedUsdmMarketParserHealthCertificateV2,
        RetainedUsdmMarketParserHealthNoncertifyingV2,
    }:
        raise TypeError("parser_health_result must be an exact retained result")
    canonical_retained_usdm_market_parser_health_result_v2(
        result.parser_health_result
    )
    _validate_stop_reason(result.stop_reason)
    finality = result.runtime_result.finality_receipt
    closure = result.session_closure_authority.manifest
    parser = result.parser_health_result
    runtime_cursor_join = tuple(
        (
            cursor.stop_receipt.session_id,
            cursor.stop_receipt.process_boot_id,
            cursor.stop_receipt.plan_bundle_sha256,
            cursor.stop_receipt.route_id,
            cursor.stop_receipt.receipt_sha256,
            cursor.cursor_sha256,
        )
        for cursor in result.runtime_result.websocket_route_cursors
    )
    closure_cursor_join = tuple(
        (
            entry.session_id,
            entry.process_boot_id,
            entry.plan_bundle_sha256,
            entry.route_id,
            entry.stop_receipt_sha256,
            entry.finalized_route_cursor_sha256,
        )
        for entry in closure.websocket_route_cursors
    )
    if (
        result.ledger_seal_receipt.seal.finality_receipt != finality
        or closure.finality_receipt != finality
        or closure.ledger_clean_closure_receipt_sha256
        != result.ledger_seal_receipt.sha256
        or closure.stop_reason != result.stop_reason
        or parser.session_id != closure.session_id
        or parser.finality_receipt_sha256 != finality.sha256
        or parser.session_closure_manifest_sha256
        != result.session_closure_authority.manifest_sha256
        or parser.ledger_clean_closure_receipt_sha256
        != result.ledger_seal_receipt.sha256
        or runtime_cursor_join != closure_cursor_join
    ):
        raise PublicCaptureClosedSessionOwnerErrorV2(
            "closed-session result components do not share one authority"
        )
    expected_certified = (
        type(result.parser_health_result)
        is RetainedUsdmMarketParserHealthCertificateV2
    )
    if hasattr(result, "retained_market_parser_health_certified") and (
        result.retained_market_parser_health_certified is not expected_certified
    ):
        raise PublicCaptureClosedSessionOwnerErrorV2(
            "parser-health certification flag differs from the exact result type"
        )
    for field_name in (
        "integrity_ledger_clean_issued",
        "local_session_closure_issued",
    ):
        if hasattr(result, field_name) and getattr(result, field_name) is not True:
            raise PublicCaptureClosedSessionOwnerErrorV2(
                f"{field_name} must be true after local CLEAN closure"
            )
    for field_name in (
        "observed_source_completeness_claimed",
        "m2_eligible",
        "strategy_ready",
        "probability_calibrated",
        "paper_fok_enabled",
        "mandatory_exit_enabled",
        "efficacy_claimed",
        "pnl_or_profit_claimed",
        "production_order_execution_enabled",
    ):
        if hasattr(result, field_name) and getattr(result, field_name) is not False:
            raise PublicCaptureClosedSessionOwnerErrorV2(
                f"{field_name} must remain explicitly false"
            )


def _result_sha256(result: PublicCaptureClosedSessionResultV2) -> str:
    return hashlib.sha256(
        _RESULT_DOMAIN
        + canonical_json_line(_result_document(result, include_result_hash=False))
    ).hexdigest()


def _result_document(
    result: PublicCaptureClosedSessionResultV2,
    *,
    include_result_hash: bool,
) -> dict[str, object]:
    parser_bytes = canonical_retained_usdm_market_parser_health_result_v2(
        result.parser_health_result
    )
    document: dict[str, object] = {
        "attempt_id": result.session_closure_authority.manifest.attempt_id,
        "efficacy_claimed": result.efficacy_claimed,
        "finality_prefix_proof_sha256": (
            result.runtime_result.verified_prefix_proof_sha256
        ),
        "finality_receipt_sha256": result.runtime_result.finality_receipt.sha256,
        "integrity_ledger_clean_issued": result.integrity_ledger_clean_issued,
        "ledger_clean_closure_receipt_sha256": (
            result.ledger_seal_receipt.sha256
        ),
        "ledger_clean_closure_seal_sha256": (
            result.ledger_seal_receipt.seal_sha256
        ),
        "local_session_closure_issued": result.local_session_closure_issued,
        "m2_eligible": result.m2_eligible,
        "mandatory_exit_enabled": result.mandatory_exit_enabled,
        "observed_source_completeness_claimed": (
            result.observed_source_completeness_claimed
        ),
        "paper_fok_enabled": result.paper_fok_enabled,
        "parser_health_canonical_sha256": hashlib.sha256(parser_bytes).hexdigest(),
        "pnl_or_profit_claimed": result.pnl_or_profit_claimed,
        "probability_calibrated": result.probability_calibrated,
        "production_order_execution_enabled": (
            result.production_order_execution_enabled
        ),
        "retained_market_parser_health_certified": (
            result.retained_market_parser_health_certified
        ),
        "role": result.role,
        "runtime_result_sha256": result.runtime_result_sha256,
        "schema_version": result.schema_version,
        "session_closure_manifest_sha256": (
            result.session_closure_authority.manifest_sha256
        ),
        "session_id": result.session_closure_authority.manifest.session_id,
        "stop_reason": result.stop_reason,
        "strategy_ready": result.strategy_ready,
    }
    if include_result_hash:
        document["result_sha256"] = result.result_sha256
    return document


def _runtime_result_sha256(result: PublicCaptureRuntimeResultV2) -> str:
    result.__post_init__()
    coverage_record = validate_public_oi_census_admission_receipt_v2(
        result.oi_coverage_close_receipt
    )
    if result.oi_coverage_close_receipt.accepted_ingest_seq > (
        result.finality_receipt.fence_ingest_seq
    ):
        raise PublicCaptureClosedSessionOwnerErrorV2(
            "runtime finality precedes its OI coverage-close admission"
        )
    normal_stop = result.normal_stop_receipt
    _validate_receipt(normal_stop, "runtime normal stop")
    coverage_record_sha256 = hashlib.sha256(
        _OI_CENSUS_RECORD_DOMAIN + canonical_json_line(coverage_record)
    ).hexdigest()
    document = {
        "adapter_cleanly_closed": result.adapter_cleanly_closed,
        "fatal_state_failed": result.fatal_state_failed,
        "finality_receipt_sha256": result.finality_receipt.sha256,
        "integrity_ledger_clean_issued": (
            result.integrity_ledger_clean_issued
        ),
        "local_session_closure_issued": result.local_session_closure_issued,
        "m2_eligible": result.m2_eligible,
        "normal_stop_received_at_ms": normal_stop.received_at_ms,
        "normal_stop_received_monotonic_ns": (
            normal_stop.received_monotonic_ns
        ),
        "observed_source_completeness_claimed": (
            result.observed_source_completeness_claimed
        ),
        "oi_coverage_close_accepted_ingest_seq": (
            result.oi_coverage_close_receipt.accepted_ingest_seq
        ),
        "oi_coverage_close_record_sha256": coverage_record_sha256,
        "oi_coverage_closed": result.oi_coverage_closed,
        "oi_data_completeness_claimed": result.oi_data_completeness_claimed,
        "pending_source_gap": result.pending_source_gap,
        "producer_task_count": result.producer_task_count,
        "verified_prefix_proof_sha256": (
            result.verified_prefix_proof_sha256
        ),
        "websocket_local_route_cursors_finalized": (
            result.websocket_local_route_cursors_finalized
        ),
        "websocket_retained_frame_parser_health_claimed": (
            result.websocket_retained_frame_parser_health_claimed
        ),
        "websocket_route_cursors": tuple(
            {
                "cursor_sha256": cursor.cursor_sha256,
                "route_id": cursor.stop_receipt.route_id,
                "stop_receipt_sha256": cursor.stop_receipt.receipt_sha256,
            }
            for cursor in result.websocket_route_cursors
        ),
        "websocket_upstream_message_completeness_claimed": (
            result.websocket_upstream_message_completeness_claimed
        ),
    }
    return hashlib.sha256(
        _RUNTIME_RESULT_DOMAIN + canonical_json_line(document)
    ).hexdigest()


def _capture_receipt(clock: ReceiptClock, label: str) -> ReceiptTimestamp:
    capture = getattr(clock, "capture", None)
    if not callable(capture):
        raise TypeError(f"{label} requires a synchronous ReceiptClock")
    receipt = capture()
    if type(receipt) is not ReceiptTimestamp:
        raise TypeError(f"{label} clock returned a foreign receipt type")
    _validate_receipt(receipt, label)
    return receipt


def _validate_receipt(receipt: ReceiptTimestamp, label: str) -> None:
    if (
        type(receipt.received_at_ms) is not int
        or receipt.received_at_ms < 0
        or type(receipt.received_monotonic_ns) is not int
        or receipt.received_monotonic_ns < 0
    ):
        raise PublicCaptureClosedSessionOwnerErrorV2(
            f"{label} has invalid timestamp material"
        )


def _validate_stop_reason(value: object) -> None:
    if value != "OPERATOR_REQUESTED":
        raise ValueError("stop_reason must be OPERATOR_REQUESTED")


def canonical_public_capture_closed_session_result_v8(
    result: PublicCaptureClosedSessionResultV8,
) -> bytes:
    """Return the canonical hash binding for one completed exact V8 outer run."""

    if type(result) is not PublicCaptureClosedSessionResultV8:
        raise TypeError("result must be an exact PublicCaptureClosedSessionResultV8")
    _validate_result_material_v8(result)
    if result.result_sha256 != _result_sha256_v8(result):
        raise PublicCaptureClosedSessionOwnerErrorV8(
            "V8 closed-session result hash differs from canonical content"
        )
    return canonical_json_line(_result_document_v8(result, include_result_hash=True))


def _validate_result_material_v8(result: PublicCaptureClosedSessionResultV8) -> None:
    if getattr(result, "_factory_seal", None) is not _RESULT_FACTORY_TOKEN_V8:
        raise PublicCaptureClosedSessionOwnerErrorV8(
            "V8 closed-session result lacks exact outer-owner provenance"
        )
    if result.schema_version != _RESULT_SCHEMA_V8:
        raise PublicCaptureClosedSessionOwnerErrorV8(
            "V8 closed-session result has an unsupported schema"
        )
    if result.role != PUBLIC_CAPTURE_CLOSED_SESSION_ROLE_V8:
        raise PublicCaptureClosedSessionOwnerErrorV8(
            "V8 closed-session result has a foreign role"
        )
    if type(result.runtime_result) is not PublicCaptureRuntimeResultV8:
        raise TypeError("runtime_result must be an exact PublicCaptureRuntimeResultV8")
    result.runtime_result.__post_init__()
    expected_runtime_hash = _runtime_result_sha256_v8(result.runtime_result)
    if result.runtime_result_sha256 != expected_runtime_hash:
        raise PublicCaptureClosedSessionOwnerErrorV8(
            "V8 closed-session runtime result hash differs from canonical content"
        )
    if type(result.ledger_seal_receipt) is not (
        PersistedCaptureCleanClosureSealReceiptV8
    ):
        raise TypeError("ledger_seal_receipt must be an exact V8 receipt")
    result.ledger_seal_receipt.__post_init__()
    if type(result.session_closure_authority) is not PersistedSessionClosureAuthorityV8:
        raise TypeError("session_closure_authority must be an exact V8 authority")
    result.session_closure_authority.__post_init__()
    _validate_stop_reason(result.stop_reason)

    runtime = result.runtime_result
    plans, depth_plan = _exact_v8_result_plans(runtime)
    seal_receipt = result.ledger_seal_receipt
    seal = seal_receipt.seal
    authority = result.session_closure_authority
    manifest = authority.manifest
    bridge_entry = seal.depth_bridge_closure_entry
    validate_depth_bridge_coordinator_clean_close_receipt_v8(
        runtime.depth_bridge_close_receipt,
        promoting_plans=plans,
        depth_plan=depth_plan,
    )
    projected_bridge = depth_bridge_coordinator_closure_entry_v8(
        runtime.depth_bridge_close_receipt,
        promoting_plans=plans,
        depth_plan=depth_plan,
    )
    runtime_cursor_join = tuple(
        (
            cursor.stop_receipt.session_id,
            cursor.stop_receipt.process_boot_id,
            cursor.stop_receipt.plan_bundle_sha256,
            cursor.stop_receipt.route_id,
            cursor.stop_receipt.receipt_sha256,
            cursor.cursor_sha256,
        )
        for cursor in runtime.websocket_route_cursors
    )
    persisted_cursor_join = tuple(
        (
            entry.session_id,
            entry.process_boot_id,
            entry.plan_bundle_sha256,
            entry.route_id,
            entry.stop_receipt_sha256,
            entry.finalized_route_cursor_sha256,
        )
        for entry in seal.websocket_route_cursor_closure_pair
    )
    oi_record = validate_public_oi_census_admission_receipt_v2(
        runtime.oi_coverage_close_receipt
    )
    oi_coverage = PublicOiRestCoverageCloseV2.from_canonical_bytes(
        oi_record.payload_bytes()
    )
    oi_record_sha256 = hashlib.sha256(
        _OI_CENSUS_RECORD_DOMAIN_V8 + canonical_json_line(oi_record)
    ).hexdigest()
    if (
        runtime.finality_receipt is not seal.finality_receipt
        or manifest.finality_receipt is not runtime.finality_receipt
        or runtime.promoting_plans is not plans
        or seal.plan_bundle_sha256 != provisional_promoting_plan_sha256_v8(plans)
        or seal.depth_plan_sha256 != public_depth_rest_plan_sha256_v8(depth_plan)
        or projected_bridge != bridge_entry
        or manifest.depth_bridge_closure_entry is not bridge_entry
        or manifest.ledger_clean_closure_receipt_sha256 != seal_receipt.sha256
        or manifest.ledger_clean_closure_seal_sha256 != seal_receipt.seal_sha256
        or manifest.plan_bundle_sha256 != seal.plan_bundle_sha256
        or manifest.depth_plan_sha256 != seal.depth_plan_sha256
        or manifest.oi_coverage_close != oi_coverage
        or manifest.oi_coverage_close_sha256 != oi_coverage.sha256
        or manifest.oi_coverage_close_record_sha256 != oi_record_sha256
        or manifest.oi_coverage_close_accepted_ingest_seq
        != runtime.oi_coverage_close_receipt.accepted_ingest_seq
        or manifest.oi_coverage_close_receipt_wall_ms != oi_record.receipt_wall_ms
        or manifest.oi_coverage_close_receipt_monotonic_ns
        != oi_record.receipt_monotonic_ns
        or runtime_cursor_join != persisted_cursor_join
        or manifest.websocket_route_cursors
        != seal.websocket_route_cursor_closure_pair
        or manifest.stop_reason != result.stop_reason
    ):
        raise PublicCaptureClosedSessionOwnerErrorV8(
            "V8 closed-session result components do not share one exact authority"
        )
    for field_name in (
        "integrity_ledger_clean_issued",
        "local_session_closure_issued",
        "depth_bridge_lifecycle_cleanly_closed",
        "websocket_route_cursor_finality_persisted",
        "oi_coverage_closed",
    ):
        if getattr(result, field_name) is not True:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                f"{field_name} must be true after V8 local CLEAN closure"
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
        if getattr(result, field_name) is not False:
            raise PublicCaptureClosedSessionOwnerErrorV8(
                f"{field_name} must remain explicitly false"
            )
    for owner, field_names in (
        (
            manifest,
            (
                "retained_frame_parser_health_claimed",
                "observed_source_completeness_claimed",
                "book_completeness_claimed",
                "m2_certified",
                "paper_execution_enabled",
                "promotion_ready",
                "production_order_execution_enabled",
            ),
        ),
        (
            seal,
            (
                "qualification_complete_claimed",
                "promoting",
                "book_bridge_certified",
                "m2_certified",
                "order_execution_enabled",
            ),
        ),
    ):
        if any(getattr(owner, field_name) is not False for field_name in field_names):
            raise PublicCaptureClosedSessionOwnerErrorV8(
                "V8 durable closure asserted forbidden strategy authority"
            )


def _exact_v8_result_plans(
    result: PublicCaptureRuntimeResultV8,
) -> tuple[
    tuple[ProvisionalPromotingPlanV8, ...],
    ProvisionalDepthRestQualificationPlanV8,
]:
    plans = result.promoting_plans
    if type(plans) is not tuple:
        raise TypeError("V8 runtime result requires an exact plan tuple")
    validate_provisional_promoting_capture_plans_v8(plans)
    if tuple(plan.route_id for plan in plans) != (
        "usdm_market",
        "usdm_public",
        "usdm_public_rest",
        "usdm_public_depth_rest",
    ):
        raise PublicCaptureClosedSessionOwnerErrorV8(
            "V8 runtime result plans are not in canonical route order"
        )
    depth_plan = plans[3]
    if type(depth_plan) is not ProvisionalDepthRestQualificationPlanV8:
        raise TypeError("V8 runtime result lacks the exact depth-plan member")
    return plans, depth_plan


def _runtime_result_sha256_v8(result: PublicCaptureRuntimeResultV8) -> str:
    if type(result) is not PublicCaptureRuntimeResultV8:
        raise TypeError("result must be an exact PublicCaptureRuntimeResultV8")
    result.__post_init__()
    plans, depth_plan = _exact_v8_result_plans(result)
    coverage_record = validate_public_oi_census_admission_receipt_v2(
        result.oi_coverage_close_receipt
    )
    if result.oi_coverage_close_receipt.accepted_ingest_seq > (
        result.finality_receipt.fence_ingest_seq
    ):
        raise PublicCaptureClosedSessionOwnerErrorV8(
            "V8 runtime finality precedes its OI coverage-close admission"
        )
    validate_depth_bridge_coordinator_clean_close_receipt_v8(
        result.depth_bridge_close_receipt,
        promoting_plans=plans,
        depth_plan=depth_plan,
    )
    _validate_receipt_v8(result.normal_stop_receipt, "V8 runtime normal stop")
    coverage_record_sha256 = hashlib.sha256(
        _OI_CENSUS_RECORD_DOMAIN_V8 + canonical_json_line(coverage_record)
    ).hexdigest()
    document = {
        "adapter_cleanly_closed": result.adapter_cleanly_closed,
        "depth_bridge_close_receipt_sha256": (
            result.depth_bridge_close_receipt.receipt_sha256
        ),
        "depth_bridge_complete_claimed": result.depth_bridge_complete_claimed,
        "depth_plan_sha256": public_depth_rest_plan_sha256_v8(depth_plan),
        "fatal_state_failed": result.fatal_state_failed,
        "finality_receipt_sha256": result.finality_receipt.sha256,
        "integrity_ledger_clean_issued": result.integrity_ledger_clean_issued,
        "local_session_closure_issued": result.local_session_closure_issued,
        "m2_eligible": result.m2_eligible,
        "normal_stop_received_at_ms": result.normal_stop_receipt.received_at_ms,
        "normal_stop_received_monotonic_ns": (
            result.normal_stop_receipt.received_monotonic_ns
        ),
        "observed_source_completeness_claimed": (
            result.observed_source_completeness_claimed
        ),
        "oi_coverage_close_accepted_ingest_seq": (
            result.oi_coverage_close_receipt.accepted_ingest_seq
        ),
        "oi_coverage_close_record_sha256": coverage_record_sha256,
        "oi_coverage_closed": result.oi_coverage_closed,
        "oi_data_completeness_claimed": result.oi_data_completeness_claimed,
        "order_execution_enabled": result.order_execution_enabled,
        "pending_source_gap": result.pending_source_gap,
        "plan_bundle_sha256": provisional_promoting_plan_sha256_v8(plans),
        "producer_task_count": result.producer_task_count,
        "production_order_execution_enabled": (
            result.production_order_execution_enabled
        ),
        "verified_prefix_proof_sha256": result.verified_prefix_proof_sha256,
        "websocket_local_route_cursors_finalized": (
            result.websocket_local_route_cursors_finalized
        ),
        "websocket_retained_frame_parser_health_claimed": (
            result.websocket_retained_frame_parser_health_claimed
        ),
        "websocket_route_cursors": tuple(
            {
                "cursor_sha256": cursor.cursor_sha256,
                "route_id": cursor.stop_receipt.route_id,
                "stop_receipt_sha256": cursor.stop_receipt.receipt_sha256,
            }
            for cursor in result.websocket_route_cursors
        ),
        "websocket_upstream_message_completeness_claimed": (
            result.websocket_upstream_message_completeness_claimed
        ),
    }
    return hashlib.sha256(
        _RUNTIME_RESULT_DOMAIN_V8 + canonical_json_line(document)
    ).hexdigest()


def _result_sha256_v8(result: PublicCaptureClosedSessionResultV8) -> str:
    return hashlib.sha256(
        _RESULT_DOMAIN_V8
        + canonical_json_line(_result_document_v8(result, include_result_hash=False))
    ).hexdigest()


def _result_document_v8(
    result: PublicCaptureClosedSessionResultV8,
    *,
    include_result_hash: bool,
) -> dict[str, object]:
    manifest = result.session_closure_authority.manifest
    seal_receipt = result.ledger_seal_receipt
    document: dict[str, object] = {
        "attempt_id": manifest.attempt_id,
        "book_completeness_claimed": result.book_completeness_claimed,
        "book_bridge_certified": result.book_bridge_certified,
        "depth_bridge_complete_claimed": result.depth_bridge_complete_claimed,
        "depth_bridge_closure_entry_sha256": (
            seal_receipt.seal.depth_bridge_closure_entry_sha256
        ),
        "depth_bridge_lifecycle_cleanly_closed": (
            result.depth_bridge_lifecycle_cleanly_closed
        ),
        "depth_plan_sha256": manifest.depth_plan_sha256,
        "efficacy_claimed": result.efficacy_claimed,
        "finality_prefix_proof_sha256": (
            result.runtime_result.verified_prefix_proof_sha256
        ),
        "finality_receipt_sha256": result.runtime_result.finality_receipt.sha256,
        "integrity_ledger_clean_issued": result.integrity_ledger_clean_issued,
        "ledger_clean_closure_receipt_sha256": seal_receipt.sha256,
        "ledger_clean_closure_seal_sha256": seal_receipt.seal_sha256,
        "local_session_closure_issued": result.local_session_closure_issued,
        "m2_certified": result.m2_certified,
        "m2_eligible": result.m2_eligible,
        "mandatory_exit_enabled": result.mandatory_exit_enabled,
        "observed_source_completeness_claimed": (
            result.observed_source_completeness_claimed
        ),
        "oi_coverage_close_accepted_ingest_seq": (
            result.runtime_result.oi_coverage_close_receipt.accepted_ingest_seq
        ),
        "oi_coverage_closed": result.oi_coverage_closed,
        "oi_data_completeness_claimed": result.oi_data_completeness_claimed,
        "order_execution_enabled": result.order_execution_enabled,
        "paper_execution_enabled": result.paper_execution_enabled,
        "paper_fok_enabled": result.paper_fok_enabled,
        "plan_bundle_sha256": manifest.plan_bundle_sha256,
        "pnl_or_profit_claimed": result.pnl_or_profit_claimed,
        "probability_calibrated": result.probability_calibrated,
        "private_credentials_permitted": result.private_credentials_permitted,
        "production_order_execution_enabled": (
            result.production_order_execution_enabled
        ),
        "promotion_ready": result.promotion_ready,
        "retained_frame_parser_health_claimed": (
            result.retained_frame_parser_health_claimed
        ),
        "retained_market_parser_health_certified": (
            result.retained_market_parser_health_certified
        ),
        "role": result.role,
        "runtime_result_sha256": result.runtime_result_sha256,
        "schema_version": result.schema_version,
        "session_closure_manifest_sha256": (
            result.session_closure_authority.manifest_sha256
        ),
        "session_id": manifest.session_id,
        "stop_reason": result.stop_reason,
        "strategy_ready": result.strategy_ready,
        "websocket_retained_frame_parser_health_claimed": (
            result.websocket_retained_frame_parser_health_claimed
        ),
        "websocket_route_cursor_finality_persisted": (
            result.websocket_route_cursor_finality_persisted
        ),
        "websocket_route_cursors_sha256": manifest.websocket_route_cursors_sha256,
        "websocket_upstream_message_completeness_claimed": (
            result.websocket_upstream_message_completeness_claimed
        ),
    }
    if include_result_hash:
        document["result_sha256"] = result.result_sha256
    return document


def _capture_receipt_v8(clock: ReceiptClock, label: str) -> ReceiptTimestamp:
    capture = getattr(clock, "capture", None)
    if not callable(capture):
        raise TypeError(f"{label} requires a synchronous ReceiptClock")
    receipt = capture()
    if type(receipt) is not ReceiptTimestamp:
        raise TypeError(f"{label} clock returned a foreign receipt type")
    _validate_receipt_v8(receipt, label)
    return receipt


def _validate_receipt_v8(receipt: ReceiptTimestamp, label: str) -> None:
    if (
        type(receipt.received_at_ms) is not int
        or receipt.received_at_ms < 0
        or type(receipt.received_monotonic_ns) is not int
        or receipt.received_monotonic_ns < 0
    ):
        raise PublicCaptureClosedSessionOwnerErrorV8(
            f"{label} has invalid timestamp material"
        )
