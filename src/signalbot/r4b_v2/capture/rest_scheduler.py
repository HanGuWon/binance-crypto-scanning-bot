from __future__ import annotations

import asyncio
import inspect
import re
import time
from dataclasses import InitVar, dataclass, field
from typing import Protocol

from signalbot.capture.receipts import ReceiptClock, ReceiptTimestamp
from signalbot.r4b_v2.capture.models import TransportV2
from signalbot.r4b_v2.capture.plans import ProvisionalPromotingRestCapturePlanV2
from signalbot.r4b_v2.capture.rest import (
    PublicOiRestAttemptPayloadV2,
    PublicOiRestMissedSlotV2,
    public_oi_rest_source_logical_key_v2,
)
from signalbot.r4b_v2.capture.rest_census import (
    PUBLIC_OI_REST_POLL_INTERVAL_MS_V2,
    PublicOiRestCellOutcomeV2,
    PublicOiRestCoverageCloseV2,
    PublicOiRestForwardGapRangeV2,
    PublicOiRestSlotCensusEntryV2,
    PublicOiRestSlotCensusV2,
    public_oi_rest_attempt_record_sha256_v2,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicOiAdmissionReceiptV2,
    PublicOiCensusAdmissionReceiptV2,
    SharedWebSocketIngressV2,
    validate_public_oi_admission_receipt_v2,
    validate_public_oi_census_admission_receipt_v2,
)

_PUBLIC_OI_SCHEDULED_ATTEMPT_FACTORY_TOKEN = object()
_PUBLIC_OI_SCHEDULE_AUTHORITY_FACTORY_TOKEN = object()
_PUBLIC_OI_CENSUS_CONTEXT_FACTORY_TOKEN = object()
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_IDENTITY_LENGTH = 256


class PublicOiRestAttemptSelfCancelledV2(RuntimeError):
    """An adapter child cancelled itself outside scheduler cancellation."""


class PublicOiScheduledAttemptOwnershipErrorV2(RuntimeError):
    """A schedule capability was unbound, foreign, unconsumed, or replayed."""


class PublicOiRestNormalStopBoundaryRaceV2(RuntimeError):
    """A same-wall-millisecond stop lost arbitration to slot selection."""


@dataclass(slots=True)
class _PublicOiRestCensusContextClaimV2:
    schedule_authority: object | None = None

    def bind(self, schedule_authority: PublicOiScheduleAuthorityV2) -> None:
        if self.schedule_authority is not None:
            raise PublicOiScheduledAttemptOwnershipErrorV2(
                "public OI census context was already bound to a scheduler"
            )
        self.schedule_authority = schedule_authority


@dataclass(frozen=True, slots=True)
class PublicOiRestCensusContextV2:
    """Factory-sealed lineage, ingress, clock, and half-open coverage authority."""

    plan: ProvisionalPromotingRestCapturePlanV2 = field(repr=False)
    session_id: str
    session_start_manifest_sha256: str
    plan_bundle_sha256: str
    protocol_hash: str
    coverage_start_slot_wall_ms: int
    ingress: SharedWebSocketIngressV2 = field(repr=False, compare=False)
    receipt_clock: ReceiptClock = field(repr=False, compare=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)
    _claim: _PublicOiRestCensusContextClaimV2 = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PUBLIC_OI_CENSUS_CONTEXT_FACTORY_TOKEN:
            raise TypeError(
                "PublicOiRestCensusContextV2 must be created by its exact factory"
            )
        _validate_census_context_material_v2(self)
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_OI_CENSUS_CONTEXT_FACTORY_TOKEN,
        )
        object.__setattr__(self, "_claim", _PublicOiRestCensusContextClaimV2())


def create_public_oi_rest_census_context_v2(
    plan: ProvisionalPromotingRestCapturePlanV2,
    *,
    session_id: str,
    session_start_manifest_sha256: str,
    plan_bundle_sha256: str,
    protocol_hash: str,
    coverage_start_slot_wall_ms: int,
    ingress: SharedWebSocketIngressV2,
    receipt_clock: ReceiptClock,
) -> PublicOiRestCensusContextV2:
    """Create the one process-local census authority accepted by a scheduler."""

    return PublicOiRestCensusContextV2(
        plan=plan,
        session_id=session_id,
        session_start_manifest_sha256=session_start_manifest_sha256,
        plan_bundle_sha256=plan_bundle_sha256,
        protocol_hash=protocol_hash,
        coverage_start_slot_wall_ms=coverage_start_slot_wall_ms,
        ingress=ingress,
        receipt_clock=receipt_clock,
        _factory_token=_PUBLIC_OI_CENSUS_CONTEXT_FACTORY_TOKEN,
    )


def _validate_census_context_for_binding_v2(
    context: PublicOiRestCensusContextV2,
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> _PublicOiRestCensusContextClaimV2:
    if type(context) is not PublicOiRestCensusContextV2:
        raise TypeError("OI scheduler requires an exact public OI census context")
    if (
        getattr(context, "_factory_seal", None)
        is not _PUBLIC_OI_CENSUS_CONTEXT_FACTORY_TOKEN
    ):
        raise PublicOiScheduledAttemptOwnershipErrorV2(
            "public OI census context lacks factory provenance"
        )
    _validate_census_context_material_v2(context)
    if context.plan is not plan:
        raise PublicOiScheduledAttemptOwnershipErrorV2(
            "public OI census context belongs to a different REST plan"
        )
    claim = getattr(context, "_claim", None)
    if type(claim) is not _PublicOiRestCensusContextClaimV2:
        raise PublicOiScheduledAttemptOwnershipErrorV2(
            "public OI census context lacks its scheduler claim"
        )
    if claim.schedule_authority is not None:
        raise PublicOiScheduledAttemptOwnershipErrorV2(
            "public OI census context was already bound to a scheduler"
        )
    return claim


def _validate_census_context_material_v2(
    context: PublicOiRestCensusContextV2,
) -> None:
    if type(context.plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise TypeError("public OI census context requires the exact REST plan")
    context.plan.__post_init__()
    _require_identity(context.session_id, "session_id")
    _require_sha256(
        context.session_start_manifest_sha256,
        "session_start_manifest_sha256",
    )
    _require_sha256(context.plan_bundle_sha256, "plan_bundle_sha256")
    _require_sha256(context.protocol_hash, "protocol_hash")
    _require_aligned_slot(
        context.coverage_start_slot_wall_ms,
        "coverage_start_slot_wall_ms",
    )
    if type(context.ingress) is not SharedWebSocketIngressV2:
        raise TypeError("public OI census context requires the exact shared ingress")
    _validate_receipt_clock(context.receipt_clock)


@dataclass(frozen=True, slots=True)
class PublicOiScheduleAuthorityV2:
    """Factory-sealed, process-local issuer identity for one scheduler instance."""

    plan: ProvisionalPromotingRestCapturePlanV2 = field(repr=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PUBLIC_OI_SCHEDULE_AUTHORITY_FACTORY_TOKEN:
            raise TypeError(
                "PublicOiScheduleAuthorityV2 can only be created by the OI scheduler"
            )
        if type(self.plan) is not ProvisionalPromotingRestCapturePlanV2:
            raise TypeError("OI schedule authority requires the exact promoting REST plan")
        self.plan.__post_init__()
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_OI_SCHEDULE_AUTHORITY_FACTORY_TOKEN,
        )


@dataclass(slots=True)
class _PublicOiScheduledAttemptClaimV2:
    consumed: bool = False

    def consume(self) -> None:
        if self.consumed:
            raise PublicOiScheduledAttemptOwnershipErrorV2(
                "public OI scheduled-attempt token was already consumed"
            )
        self.consumed = True


@dataclass(frozen=True, slots=True)
class PublicOiScheduledAttemptTokenV2:
    """Factory-sealed authority for one scheduler-selected OI attempt identity."""

    plan: ProvisionalPromotingRestCapturePlanV2 = field(repr=False)
    schedule_authority: PublicOiScheduleAuthorityV2 = field(
        repr=False,
        compare=False,
    )
    symbol: str
    poll_cycle_seq: int
    symbol_ordinal: int
    scheduled_slot_wall_ms: int
    attempt: int
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)
    _claim: _PublicOiScheduledAttemptClaimV2 = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _PUBLIC_OI_SCHEDULED_ATTEMPT_FACTORY_TOKEN:
            raise TypeError(
                "PublicOiScheduledAttemptTokenV2 can only be created by the OI scheduler"
            )
        object.__setattr__(
            self,
            "_factory_seal",
            _PUBLIC_OI_SCHEDULED_ATTEMPT_FACTORY_TOKEN,
        )
        object.__setattr__(self, "_claim", _PublicOiScheduledAttemptClaimV2())
        _validate_scheduled_attempt_material_v2(self)


def validate_public_oi_scheduled_attempt_token_v2(
    token: PublicOiScheduledAttemptTokenV2,
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    schedule_authority: PublicOiScheduleAuthorityV2,
) -> None:
    """Fail unless this exact plan's scheduler factory minted the token."""

    if type(token) is not PublicOiScheduledAttemptTokenV2:
        raise TypeError("OI REST attempt requires an exact scheduled-attempt token")
    if (
        getattr(token, "_factory_seal", None)
        is not _PUBLIC_OI_SCHEDULED_ATTEMPT_FACTORY_TOKEN
    ):
        raise ValueError("public OI scheduled-attempt token lacks scheduler provenance")
    if token.plan is not plan:
        raise ValueError("public OI scheduled-attempt token belongs to a different plan")
    validate_public_oi_schedule_authority_v2(
        schedule_authority,
        plan=plan,
    )
    if token.schedule_authority is not schedule_authority:
        raise PublicOiScheduledAttemptOwnershipErrorV2(
            "public OI scheduled-attempt token belongs to a different scheduler issuer"
        )
    _validate_scheduled_attempt_material_v2(token)


def validate_public_oi_schedule_authority_v2(
    schedule_authority: PublicOiScheduleAuthorityV2,
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> None:
    if type(schedule_authority) is not PublicOiScheduleAuthorityV2:
        raise TypeError("OI scheduler binding requires an exact schedule authority")
    if (
        getattr(schedule_authority, "_factory_seal", None)
        is not _PUBLIC_OI_SCHEDULE_AUTHORITY_FACTORY_TOKEN
    ):
        raise PublicOiScheduledAttemptOwnershipErrorV2(
            "public OI schedule authority lacks scheduler provenance"
        )
    if schedule_authority.plan is not plan:
        raise PublicOiScheduledAttemptOwnershipErrorV2(
            "public OI schedule authority belongs to a different plan"
        )
    schedule_authority.plan.__post_init__()


def consume_public_oi_scheduled_attempt_token_v2(
    token: PublicOiScheduledAttemptTokenV2,
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    schedule_authority: PublicOiScheduleAuthorityV2,
) -> None:
    """Atomically claim one token without a registry or await point."""

    validate_public_oi_scheduled_attempt_token_v2(
        token,
        plan=plan,
        schedule_authority=schedule_authority,
    )
    claim = getattr(token, "_claim", None)
    if type(claim) is not _PublicOiScheduledAttemptClaimV2:
        raise PublicOiScheduledAttemptOwnershipErrorV2(
            "public OI scheduled-attempt token lacks its one-shot claim"
        )
    claim.consume()


def assert_public_oi_scheduled_attempt_token_consumed_v2(
    token: PublicOiScheduledAttemptTokenV2,
    *,
    plan: ProvisionalPromotingRestCapturePlanV2,
    schedule_authority: PublicOiScheduleAuthorityV2,
) -> None:
    validate_public_oi_scheduled_attempt_token_v2(
        token,
        plan=plan,
        schedule_authority=schedule_authority,
    )
    claim = getattr(token, "_claim", None)
    if type(claim) is not _PublicOiScheduledAttemptClaimV2 or not claim.consumed:
        raise PublicOiScheduledAttemptOwnershipErrorV2(
            "OI adapter returned before consuming its scheduled-attempt token"
        )


def _validate_scheduled_attempt_material_v2(
    token: PublicOiScheduledAttemptTokenV2,
) -> None:
    plan = token.plan
    if type(plan) is not ProvisionalPromotingRestCapturePlanV2:
        raise TypeError("scheduled-attempt token requires the exact promoting REST plan")
    plan.__post_init__()
    validate_public_oi_schedule_authority_v2(
        token.schedule_authority,
        plan=plan,
    )
    _require_positive_int(token.poll_cycle_seq, "poll_cycle_seq")
    _require_nonnegative_int(token.symbol_ordinal, "symbol_ordinal")
    if (
        token.symbol_ordinal >= len(plan.symbols)
        or plan.symbols[token.symbol_ordinal] != token.symbol
    ):
        raise ValueError("scheduled-attempt token symbol differs from its plan ordinal")
    _require_nonnegative_int(token.scheduled_slot_wall_ms, "scheduled_slot_wall_ms")
    if token.scheduled_slot_wall_ms % plan.poll_interval_ms != 0:
        raise ValueError("scheduled-attempt token slot is not UTC-epoch aligned")
    if type(token.attempt) is not int or token.attempt != 1:
        raise ValueError("scheduled-attempt token permits exactly the frozen first attempt")


def _mint_public_oi_scheduled_attempt_token_v2(
    plan: ProvisionalPromotingRestCapturePlanV2,
    *,
    schedule_authority: PublicOiScheduleAuthorityV2,
    symbol: str,
    poll_cycle_seq: int,
    symbol_ordinal: int,
    scheduled_slot_wall_ms: int,
) -> PublicOiScheduledAttemptTokenV2:
    return PublicOiScheduledAttemptTokenV2(
        plan=plan,
        schedule_authority=schedule_authority,
        symbol=symbol,
        poll_cycle_seq=poll_cycle_seq,
        symbol_ordinal=symbol_ordinal,
        scheduled_slot_wall_ms=scheduled_slot_wall_ms,
        attempt=1,
        _factory_token=_PUBLIC_OI_SCHEDULED_ATTEMPT_FACTORY_TOKEN,
    )


class OpenInterestRestAttemptAdapterV2(Protocol):
    """One bounded public OI attempt; transport and retention stay downstream."""

    def bind_schedule_authority(
        self,
        schedule_authority: PublicOiScheduleAuthorityV2,
        /,
    ) -> None: ...

    async def capture_attempt(
        self,
        token: PublicOiScheduledAttemptTokenV2,
        /,
    ) -> PublicOiAdmissionReceiptV2: ...


class OpenInterestSchedulerClockV2(Protocol):
    """Wall/monotonic clock plus a stop-aware monotonic deadline wait."""

    def utc_wall_ms(self) -> int: ...

    def monotonic_ns(self) -> int: ...

    async def wait_until(
        self,
        stop_event: asyncio.Event,
        deadline_monotonic_ns: int,
    ) -> bool:
        """Return true only when stop wins before the deadline."""

        ...


class SystemOpenInterestSchedulerClockV2:
    """Production UTC/monotonic clock with a cancellation-transparent wait."""

    def utc_wall_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def monotonic_ns(self) -> int:
        return time.monotonic_ns()

    async def wait_until(
        self,
        stop_event: asyncio.Event,
        deadline_monotonic_ns: int,
    ) -> bool:
        if type(stop_event) is not asyncio.Event:
            raise TypeError("stop_event must be an exact asyncio.Event")
        _require_nonnegative_int(deadline_monotonic_ns, "deadline_monotonic_ns")
        if stop_event.is_set():
            return True
        remaining_ns = deadline_monotonic_ns - time.monotonic_ns()
        if remaining_ns <= 0:
            return stop_event.is_set()
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=remaining_ns / 1_000_000_000,
            )
        except TimeoutError:
            return False
        return True


class PublicOpenInterestRestSchedulerV2:
    """One-run, bounded, no-retry scheduler for the exact public OI plan."""

    def __init__(
        self,
        plan: ProvisionalPromotingRestCapturePlanV2,
        adapter: OpenInterestRestAttemptAdapterV2,
        *,
        census_context: PublicOiRestCensusContextV2 | None = None,
        clock: OpenInterestSchedulerClockV2 | None = None,
    ) -> None:
        if type(plan) is not ProvisionalPromotingRestCapturePlanV2:
            raise TypeError("OI scheduler requires an exact promoting REST plan")
        plan.__post_init__()
        capture_attempt = getattr(adapter, "capture_attempt", None)
        if not callable(capture_attempt) or not inspect.iscoroutinefunction(
            capture_attempt
        ):
            raise TypeError("OI scheduler adapter must expose async capture_attempt")
        bind_schedule_authority = getattr(adapter, "bind_schedule_authority", None)
        if not callable(bind_schedule_authority) or inspect.iscoroutinefunction(
            bind_schedule_authority
        ):
            raise TypeError(
                "OI scheduler adapter must expose synchronous bind_schedule_authority"
            )
        selected_clock = (
            SystemOpenInterestSchedulerClockV2() if clock is None else clock
        )
        _validate_clock(selected_clock)
        schedule_authority = PublicOiScheduleAuthorityV2(
            plan=plan,
            _factory_token=_PUBLIC_OI_SCHEDULE_AUTHORITY_FACTORY_TOKEN,
        )
        census_claim = (
            None
            if census_context is None
            else _validate_census_context_for_binding_v2(census_context, plan=plan)
        )
        bind_result = bind_schedule_authority(schedule_authority)
        if bind_result is not None:
            raise TypeError("OI scheduler authority binding must return None")
        if census_claim is not None:
            census_claim.bind(schedule_authority)

        self.plan = plan
        self.adapter = adapter
        self.clock = selected_clock
        self.schedule_authority = schedule_authority
        self.census_context = census_context
        self._started_once = False
        self._running = False
        self._last_started_poll_cycle_seq = 0
        self._last_completed_poll_cycle_seq = 0
        self._last_started_slot_wall_ms: int | None = None
        self._last_observed_wall_ms: int | None = None
        self._last_observed_monotonic_ns: int | None = None
        self._in_flight: set[asyncio.Task[PublicOiAdmissionReceiptV2]] = set()
        self._control_lock = asyncio.Lock()
        self._normal_stop_event = asyncio.Event()
        self._normal_stop_candidate_receipt: ReceiptTimestamp | None = None
        self._normal_stop_receipt: ReceiptTimestamp | None = None
        self._normal_stop_boundary_failure: (
            PublicOiRestNormalStopBoundaryRaceV2 | None
        ) = None
        self._active_slot_wall_ms: int | None = None
        self._active_slot_attempts_launched = False
        self._coverage_cursor_slot_wall_ms = (
            None
            if census_context is None
            else census_context.coverage_start_slot_wall_ms
        )
        self._last_census_ingest_seq: int | None = None
        self._coverage_close_receipt: PublicOiCensusAdmissionReceiptV2 | None = None
        self._normal_close_started = False

    @property
    def started_once(self) -> bool:
        return self._started_once

    @property
    def running(self) -> bool:
        return self._running

    @property
    def drained(self) -> bool:
        return not self._in_flight

    @property
    def in_flight_attempt_count(self) -> int:
        return len(self._in_flight)

    @property
    def last_started_poll_cycle_seq(self) -> int:
        return self._last_started_poll_cycle_seq

    @property
    def last_completed_poll_cycle_seq(self) -> int:
        return self._last_completed_poll_cycle_seq

    @property
    def normal_stop_receipt(self) -> ReceiptTimestamp | None:
        return self._normal_stop_receipt

    @property
    def last_census_ingest_seq(self) -> int | None:
        return self._last_census_ingest_seq

    @property
    def coverage_close_receipt(self) -> PublicOiCensusAdmissionReceiptV2 | None:
        return self._coverage_close_receipt

    @property
    def coverage_closed(self) -> bool:
        return self._coverage_close_receipt is not None

    @property
    def normal_stop_boundary_failure(
        self,
    ) -> PublicOiRestNormalStopBoundaryRaceV2 | None:
        return self._normal_stop_boundary_failure

    async def request_normal_stop(self) -> ReceiptTimestamp:
        """Reserve and commit one exact stop timestamp without losing cancellation.

        The first caller samples the receipt before waiting for the control lock.
        The reservation itself has no await point, so every concurrent caller uses
        that same timestamp.  A cancellation while the lock is held elsewhere is
        remembered and re-raised only after this task has committed the reserved
        boundary; no fifth/background task is created for the authority seam.
        """

        context = self._require_census_context()
        existing = self._normal_stop_receipt
        if existing is not None:
            self._raise_normal_stop_boundary_failure()
            return existing
        candidate = self._normal_stop_candidate_receipt
        if candidate is None:
            candidate = _capture_receipt(context.receipt_clock, "normal stop")
            if candidate.received_at_ms < context.coverage_start_slot_wall_ms:
                raise ValueError("normal stop receipt precedes census coverage start")
            self._normal_stop_candidate_receipt = candidate

        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                await self._control_lock.acquire()
                break
            except asyncio.CancelledError as exc:
                cancellation = exc
        try:
            receipt = self._commit_pending_normal_stop_unlocked()
            if receipt is None:
                raise AssertionError("reserved OI normal stop was not committed")
            boundary_failure = self._normal_stop_boundary_failure
        finally:
            self._control_lock.release()
        if cancellation is not None:
            raise cancellation
        if boundary_failure is not None:
            raise boundary_failure
        return receipt

    async def run(self) -> None:
        """Run UTC slots until the scheduler-owned exact stop receipt wins."""

        self._require_census_context()
        if self._started_once:
            raise RuntimeError("OI scheduler may run only once")
        self._started_once = True
        self._running = True
        try:
            interval_ms = self.plan.poll_interval_ms
            while True:
                forward_gap: tuple[int, int] | None = None
                scheduled_slot_wall_ms: int | None = None
                poll_cycle_seq: int | None = None
                async with self._control_lock:
                    self._commit_pending_normal_stop_unlocked()
                    self._raise_normal_stop_boundary_failure()
                    should_close = self._normal_stop_receipt is not None
                    if not should_close:
                        wall_ms = self._read_wall_ms()
                        scheduled_slot_wall_ms = wall_ms - (wall_ms % interval_ms)
                        cursor = self._require_coverage_cursor()
                        if scheduled_slot_wall_ms < cursor:
                            raise RuntimeError(
                                "OI scheduler UTC slot did not advance after its "
                                "monotonic due"
                            )
                        if scheduled_slot_wall_ms > cursor:
                            forward_gap = (cursor, scheduled_slot_wall_ms)
                if should_close:
                    await self._emit_normal_close()
                    return
                if scheduled_slot_wall_ms is None:
                    raise AssertionError("live OI scheduler iteration lacks a UTC slot")
                if forward_gap is not None:
                    await self._emit_forward_gap_unlocked(
                        first_slot_wall_ms=forward_gap[0],
                        end_slot_exclusive_wall_ms=forward_gap[1],
                    )
                async with self._control_lock:
                    self._commit_pending_normal_stop_unlocked()
                    self._raise_normal_stop_boundary_failure()
                    should_close = self._normal_stop_receipt is not None
                    if not should_close:
                        previous_slot = self._last_started_slot_wall_ms
                        if (
                            previous_slot is not None
                            and scheduled_slot_wall_ms <= previous_slot
                        ):
                            raise RuntimeError(
                                "OI scheduler UTC slot did not advance after its "
                                "monotonic due"
                            )
                        if scheduled_slot_wall_ms != self._require_coverage_cursor():
                            raise RuntimeError(
                                "OI scheduler slot selection is not contiguous with "
                                "census coverage"
                            )
                        self._last_started_slot_wall_ms = scheduled_slot_wall_ms
                        self._active_slot_wall_ms = scheduled_slot_wall_ms
                        self._last_started_poll_cycle_seq += 1
                        poll_cycle_seq = self._last_started_poll_cycle_seq
                if should_close:
                    await self._emit_normal_close()
                    return
                if poll_cycle_seq is None:
                    raise AssertionError("selected OI slot lacks a poll-cycle sequence")
                try:
                    completed = await self._run_cycle(
                        poll_cycle_seq=poll_cycle_seq,
                        scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                    )
                    if completed:
                        self._last_completed_poll_cycle_seq = poll_cycle_seq
                    async with self._control_lock:
                        self._commit_pending_normal_stop_unlocked()
                        self._raise_normal_stop_boundary_failure()
                finally:
                    self._active_slot_wall_ms = None
                    self._active_slot_attempts_launched = False
                if self._normal_stop_receipt is not None:
                    await self._emit_normal_close()
                    return

                next_due_monotonic_ns = self._next_due_monotonic_ns(
                    scheduled_slot_wall_ms
                )
                stopped = await self.clock.wait_until(
                    self._normal_stop_event,
                    next_due_monotonic_ns,
                )
                if type(stopped) is not bool:
                    raise TypeError("OI scheduler clock wait must return a boolean")
                if stopped and not self._normal_stop_event.is_set():
                    raise RuntimeError(
                        "OI scheduler wait reported stop without its exact receipt"
                    )
        finally:
            self._running = False

    async def _run_cycle(
        self,
        *,
        poll_cycle_seq: int,
        scheduled_slot_wall_ms: int,
    ) -> bool:
        indexed_symbols = tuple(enumerate(self.plan.symbols))
        chunk_size = self.plan.maximum_concurrency
        entries: list[PublicOiRestSlotCensusEntryV2 | None] = [
            None for _symbol in self.plan.symbols
        ]
        for offset in range(0, len(indexed_symbols), chunk_size):
            early_entries: tuple[PublicOiRestSlotCensusEntryV2, ...] | None = None
            early_receipt: ReceiptTimestamp | None = None
            chunk: tuple[tuple[int, str], ...] = ()
            tokens: tuple[PublicOiScheduledAttemptTokenV2, ...] = ()
            tasks: tuple[asyncio.Task[PublicOiAdmissionReceiptV2], ...] = ()
            async with self._control_lock:
                self._commit_pending_normal_stop_unlocked()
                self._raise_normal_stop_boundary_failure()
                stop_receipt = self._normal_stop_receipt
                if stop_receipt is not None:
                    if (
                        stop_receipt.received_at_ms == scheduled_slot_wall_ms
                        and not any(entry is not None for entry in entries)
                    ):
                        return False
                    outcome = _unstarted_outcome_at_stop(
                        stop_receipt,
                        scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                    )
                    _fill_unstarted_entries(
                        self,
                        entries,
                        scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                        outcome=outcome,
                    )
                    early_entries = _complete_entries(entries)
                    early_receipt = stop_receipt
                else:
                    observed_wall_ms = self._read_wall_ms()
                    if observed_wall_ms < scheduled_slot_wall_ms:
                        raise RuntimeError(
                            "OI scheduler wall clock precedes its chosen UTC slot"
                        )
                    if observed_wall_ms >= (
                        scheduled_slot_wall_ms + self.plan.poll_interval_ms
                    ):
                        _fill_unstarted_entries(
                            self,
                            entries,
                            scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                            outcome=(
                                PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED
                            ),
                        )
                        early_entries = _complete_entries(entries)
                        early_receipt = self._capture_census_receipt("expired slot")
                    else:
                        chunk = indexed_symbols[offset : offset + chunk_size]
                        tokens = tuple(
                            _mint_public_oi_scheduled_attempt_token_v2(
                                self.plan,
                                schedule_authority=self.schedule_authority,
                                symbol=symbol,
                                poll_cycle_seq=poll_cycle_seq,
                                symbol_ordinal=symbol_ordinal,
                                scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                            )
                            for symbol_ordinal, symbol in chunk
                        )
                        tasks = tuple(
                            asyncio.create_task(
                                self.adapter.capture_attempt(token),
                                name=(
                                    f"r4b-v2-oi-{poll_cycle_seq:08d}-"
                                    f"{token.symbol_ordinal:02d}-{token.symbol}"
                                ),
                            )
                            for token in tokens
                        )
                        if len(tasks) > self.plan.maximum_concurrency:
                            raise AssertionError(
                                "OI scheduler exceeded its concurrency bound"
                            )
                        self._in_flight.update(tasks)
                        self._active_slot_attempts_launched = True
            if early_entries is not None:
                if early_receipt is None:
                    raise AssertionError("early OI slot census lacks its terminal receipt")
                await self._emit_slot_census_unlocked(
                    scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                    entries=early_entries,
                    terminal_receipt=early_receipt,
                )
                return False
            if not tasks or not tokens:
                raise AssertionError("OI scheduler selected no tasks for a live slot chunk")
            try:
                outcomes = await _await_attempt_chunk(
                    self.plan,
                    tasks,
                    tokens=tokens,
                    schedule_authority=self.schedule_authority,
                )
            finally:
                self._in_flight.difference_update(tasks)
            missed = False
            for token, outcome in zip(tokens, outcomes, strict=True):
                if type(outcome) is PublicOiAdmissionReceiptV2:
                    entries[token.symbol_ordinal] = _retained_entry(
                        self,
                        token=token,
                        receipt=outcome,
                    )
                elif type(outcome) is PublicOiRestMissedSlotV2:
                    missed = True
                    entries[token.symbol_ordinal] = self._unstarted_entry(
                        symbol_ordinal=token.symbol_ordinal,
                        scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                        outcome=PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED,
                    )
                else:
                    raise AssertionError("OI chunk returned an unsupported exact outcome")

            final_chunk = offset + len(chunk) >= len(indexed_symbols)
            terminal_entries: tuple[PublicOiRestSlotCensusEntryV2, ...] | None = None
            terminal_receipt: ReceiptTimestamp | None = None
            cycle_completed: bool | None = None
            async with self._control_lock:
                self._commit_pending_normal_stop_unlocked()
                self._raise_normal_stop_boundary_failure()
                stop_receipt = self._normal_stop_receipt
                if missed:
                    _fill_unstarted_entries(
                        self,
                        entries,
                        scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                        outcome=PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED,
                    )
                    terminal_entries = _complete_entries(entries)
                    terminal_receipt = self._capture_census_receipt("missed slot")
                    cycle_completed = False
                elif stop_receipt is not None and not final_chunk:
                    _fill_unstarted_entries(
                        self,
                        entries,
                        scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                        outcome=_unstarted_outcome_at_stop(
                            stop_receipt,
                            scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                        ),
                    )
                    terminal_entries = _complete_entries(entries)
                    terminal_receipt = stop_receipt
                    cycle_completed = False
                elif final_chunk:
                    terminal_entries = _complete_entries(entries)
                    terminal_receipt = self._capture_census_receipt(
                        "completed slot census"
                    )
                    cycle_completed = True
            if terminal_entries is not None:
                if terminal_receipt is None or cycle_completed is None:
                    raise AssertionError(
                        "terminal OI slot census lacks an exact terminal decision"
                    )
                await self._emit_slot_census_unlocked(
                    scheduled_slot_wall_ms=scheduled_slot_wall_ms,
                    entries=terminal_entries,
                    terminal_receipt=terminal_receipt,
                )
                return cycle_completed
        raise AssertionError("public OI cycle did not emit its exact slot vector")

    def _require_census_context(self) -> PublicOiRestCensusContextV2:
        context = self.census_context
        if context is None:
            raise RuntimeError(
                "OI scheduler cannot run without its exact census context"
            )
        if type(context) is not PublicOiRestCensusContextV2:
            raise TypeError("OI scheduler census context changed type after binding")
        _validate_census_context_material_v2(context)
        if context.plan is not self.plan:
            raise PublicOiScheduledAttemptOwnershipErrorV2(
                "OI scheduler census context changed REST plan after binding"
            )
        claim = getattr(context, "_claim", None)
        if (
            type(claim) is not _PublicOiRestCensusContextClaimV2
            or claim.schedule_authority is not self.schedule_authority
        ):
            raise PublicOiScheduledAttemptOwnershipErrorV2(
                "OI scheduler lacks the exact census-context ownership claim"
            )
        return context

    def _commit_pending_normal_stop_unlocked(self) -> ReceiptTimestamp | None:
        receipt = self._normal_stop_receipt
        if receipt is not None:
            return receipt
        candidate = self._normal_stop_candidate_receipt
        if candidate is None:
            return None
        self._normal_stop_receipt = candidate
        self._normal_stop_event.set()
        active_slot = self._active_slot_wall_ms
        if (
            active_slot is not None
            and self._active_slot_attempts_launched
            and candidate.received_at_ms == active_slot
        ):
            self._normal_stop_boundary_failure = (
                PublicOiRestNormalStopBoundaryRaceV2(
                    "normal stop at an already-launched slot's exact wall boundary "
                    "cannot satisfy half-open coverage"
                )
            )
        return candidate

    def _raise_normal_stop_boundary_failure(self) -> None:
        failure = self._normal_stop_boundary_failure
        if failure is not None:
            raise failure

    def _require_coverage_cursor(self) -> int:
        cursor = self._coverage_cursor_slot_wall_ms
        if type(cursor) is not int:
            raise RuntimeError("OI scheduler lacks a census coverage cursor")
        _require_aligned_slot(cursor, "coverage cursor")
        return cursor

    def _capture_census_receipt(self, label: str) -> ReceiptTimestamp:
        return _capture_receipt(self._require_census_context().receipt_clock, label)

    def _unstarted_entry(
        self,
        *,
        symbol_ordinal: int,
        scheduled_slot_wall_ms: int,
        outcome: PublicOiRestCellOutcomeV2,
    ) -> PublicOiRestSlotCensusEntryV2:
        context = self._require_census_context()
        return PublicOiRestSlotCensusEntryV2.for_plan(
            self.plan,
            session_start_manifest_sha256=context.session_start_manifest_sha256,
            plan_bundle_sha256=context.plan_bundle_sha256,
            symbol_ordinal=symbol_ordinal,
            scheduled_slot_wall_ms=scheduled_slot_wall_ms,
            outcome=outcome,
        )

    async def _offer_census_unlocked(
        self,
        payload: (
            PublicOiRestSlotCensusV2
            | PublicOiRestForwardGapRangeV2
            | PublicOiRestCoverageCloseV2
        ),
    ) -> PublicOiCensusAdmissionReceiptV2:
        context = self._require_census_context()
        receipt = await context.ingress.offer_https_census(
            plan=self.plan,
            session_id=context.session_id,
            protocol_hash=context.protocol_hash,
            clock=context.receipt_clock,
            payload=payload,
        )
        record = validate_public_oi_census_admission_receipt_v2(receipt)
        if (
            record.session_id != context.session_id
            or record.plan_id != self.plan.name
            or record.protocol_hash != context.protocol_hash
            or record.ingest_seq != receipt.accepted_ingest_seq
        ):
            raise ValueError("OI census admission differs from its sealed context")
        return receipt

    async def _emit_slot_census_unlocked(
        self,
        *,
        scheduled_slot_wall_ms: int,
        entries: tuple[PublicOiRestSlotCensusEntryV2, ...],
        terminal_receipt: ReceiptTimestamp,
    ) -> None:
        cursor = self._require_coverage_cursor()
        if scheduled_slot_wall_ms != cursor:
            raise RuntimeError("OI slot census is not contiguous with its coverage cursor")
        context = self._require_census_context()
        payload = PublicOiRestSlotCensusV2.for_plan(
            self.plan,
            session_id=context.session_id,
            session_start_manifest_sha256=context.session_start_manifest_sha256,
            plan_bundle_sha256=context.plan_bundle_sha256,
            scheduled_slot_wall_ms=scheduled_slot_wall_ms,
            entries=entries,
            closed_wall_ms=terminal_receipt.received_at_ms,
            closed_monotonic_ns=terminal_receipt.received_monotonic_ns,
        )
        admission = await self._offer_census_unlocked(payload)
        self._last_census_ingest_seq = admission.accepted_ingest_seq
        self._coverage_cursor_slot_wall_ms = (
            scheduled_slot_wall_ms + self.plan.poll_interval_ms
        )

    async def _emit_forward_gap_unlocked(
        self,
        *,
        first_slot_wall_ms: int,
        end_slot_exclusive_wall_ms: int,
    ) -> None:
        if first_slot_wall_ms != self._require_coverage_cursor():
            raise RuntimeError("OI forward gap is not contiguous with its coverage cursor")
        if end_slot_exclusive_wall_ms <= first_slot_wall_ms:
            raise ValueError("OI forward gap must cover at least one whole slot")
        context = self._require_census_context()
        observation = self._capture_census_receipt("forward clock gap")
        payload = PublicOiRestForwardGapRangeV2.for_plan(
            self.plan,
            session_id=context.session_id,
            session_start_manifest_sha256=context.session_start_manifest_sha256,
            plan_bundle_sha256=context.plan_bundle_sha256,
            first_slot_wall_ms=first_slot_wall_ms,
            end_slot_exclusive_wall_ms=end_slot_exclusive_wall_ms,
            observed_wall_ms=observation.received_at_ms,
            observed_monotonic_ns=observation.received_monotonic_ns,
        )
        admission = await self._offer_census_unlocked(payload)
        self._last_census_ingest_seq = admission.accepted_ingest_seq
        self._coverage_cursor_slot_wall_ms = end_slot_exclusive_wall_ms

    async def _emit_normal_close(self) -> None:
        async with self._control_lock:
            self._commit_pending_normal_stop_unlocked()
            self._raise_normal_stop_boundary_failure()
            if self._coverage_close_receipt is not None:
                return
            if self._normal_close_started:
                raise RuntimeError("OI normal close was already started without completion")
            if self._normal_stop_receipt is None:
                raise RuntimeError("OI normal close requires the exact stop receipt")
            self._normal_close_started = True
        await self._emit_normal_close_unlocked()

    async def _emit_normal_close_unlocked(self) -> None:
        if self._coverage_close_receipt is not None:
            return
        stop_receipt = self._normal_stop_receipt
        if stop_receipt is None:
            raise RuntimeError("OI normal close requires the exact stop receipt")
        context = self._require_census_context()
        coverage_end = _ceil_slot_exclusive(stop_receipt.received_at_ms)
        cursor = self._require_coverage_cursor()
        if cursor > coverage_end:
            raise PublicOiRestNormalStopBoundaryRaceV2(
                "OI stop boundary precedes already admitted census coverage"
            )
        stop_slot = stop_receipt.received_at_ms - (
            stop_receipt.received_at_ms % self.plan.poll_interval_ms
        )
        if cursor < stop_slot:
            await self._emit_forward_gap_unlocked(
                first_slot_wall_ms=cursor,
                end_slot_exclusive_wall_ms=stop_slot,
            )
            cursor = self._require_coverage_cursor()
        if coverage_end > stop_slot and cursor == stop_slot:
            entries = tuple(
                self._unstarted_entry(
                    symbol_ordinal=ordinal,
                    scheduled_slot_wall_ms=stop_slot,
                    outcome=PublicOiRestCellOutcomeV2.UNSTARTED_NORMAL_STOP,
                )
                for ordinal in range(len(self.plan.symbols))
            )
            await self._emit_slot_census_unlocked(
                scheduled_slot_wall_ms=stop_slot,
                entries=entries,
                terminal_receipt=stop_receipt,
            )
        if self._require_coverage_cursor() != coverage_end:
            raise RuntimeError("OI normal stop could not close contiguous slot coverage")
        previous_census_ingest_seq = self._last_census_ingest_seq
        payload = PublicOiRestCoverageCloseV2.for_plan(
            self.plan,
            session_id=context.session_id,
            session_start_manifest_sha256=context.session_start_manifest_sha256,
            plan_bundle_sha256=context.plan_bundle_sha256,
            coverage_start_slot_wall_ms=context.coverage_start_slot_wall_ms,
            stop_requested_wall_ms=stop_receipt.received_at_ms,
            stop_requested_monotonic_ns=stop_receipt.received_monotonic_ns,
            last_census_ingest_seq=previous_census_ingest_seq,
        )
        admission = await self._offer_census_unlocked(payload)
        self._coverage_close_receipt = admission
        self._last_census_ingest_seq = admission.accepted_ingest_seq

    def _read_wall_ms(self) -> int:
        value = self.clock.utc_wall_ms()
        _require_nonnegative_int(value, "scheduler UTC wall time")
        previous = self._last_observed_wall_ms
        if previous is not None and value < previous:
            raise ValueError("OI scheduler UTC wall clock moved backwards")
        self._last_observed_wall_ms = value
        return value

    def _read_monotonic_ns(self) -> int:
        value = self.clock.monotonic_ns()
        _require_nonnegative_int(value, "scheduler monotonic time")
        previous = self._last_observed_monotonic_ns
        if previous is not None and value < previous:
            raise ValueError("OI scheduler monotonic clock moved backwards")
        self._last_observed_monotonic_ns = value
        return value

    def _next_due_monotonic_ns(self, started_slot_wall_ms: int) -> int:
        wall_ms = self._read_wall_ms()
        monotonic_ns = self._read_monotonic_ns()
        current_slot_wall_ms = wall_ms - (wall_ms % self.plan.poll_interval_ms)
        if current_slot_wall_ms > started_slot_wall_ms:
            return monotonic_ns
        next_boundary_wall_ms = started_slot_wall_ms + self.plan.poll_interval_ms
        delay_ms = next_boundary_wall_ms - wall_ms
        if delay_ms < 0:
            raise RuntimeError("OI scheduler derived a negative UTC boundary delay")
        return monotonic_ns + delay_ms * 1_000_000


async def _await_attempt_chunk(
    plan: ProvisionalPromotingRestCapturePlanV2,
    tasks: tuple[asyncio.Task[PublicOiAdmissionReceiptV2], ...],
    *,
    tokens: tuple[PublicOiScheduledAttemptTokenV2, ...],
    schedule_authority: PublicOiScheduleAuthorityV2,
) -> tuple[PublicOiAdmissionReceiptV2 | PublicOiRestMissedSlotV2, ...]:
    if len(tasks) != len(tokens):
        raise ValueError("OI scheduled tokens and attempt tasks must be one-to-one")
    pending = set(tasks)
    outcomes: list[PublicOiAdmissionReceiptV2 | PublicOiRestMissedSlotV2 | None] = [
        None for _task in tasks
    ]
    try:
        while pending:
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for index, task in enumerate(tasks):
                if task not in done:
                    continue
                if task.cancelled():
                    await _cancel_and_join_attempts(tasks)
                    raise PublicOiRestAttemptSelfCancelledV2(
                        "OI adapter task cancelled itself unexpectedly: "
                        f"{task.get_name()}"
                    )
                failure = task.exception()
                if failure is not None:
                    if type(failure) is PublicOiRestMissedSlotV2:
                        token = tokens[index]
                        if _missed_slot_matches(
                            failure,
                            token=token,
                            plan=plan,
                            schedule_authority=schedule_authority,
                        ):
                            outcomes[index] = failure
                            continue
                        await _cancel_and_join_attempts(tasks)
                        raise RuntimeError(
                            "OI adapter missed-slot identity differs from its scheduled call"
                        ) from failure
                    await _cancel_and_join_attempts(tasks)
                    raise failure
                try:
                    receipt = task.result()
                    _validate_attempt_result(
                        plan,
                        receipt,
                        token=tokens[index],
                        schedule_authority=schedule_authority,
                    )
                    outcomes[index] = receipt
                except BaseException:
                    await _cancel_and_join_attempts(tasks)
                    raise
        if any(outcome is None for outcome in outcomes):
            raise AssertionError("OI attempt chunk ended without one outcome per token")
        return tuple(
            outcome
            for outcome in outcomes
            if outcome is not None
        )
    except asyncio.CancelledError:
        await _cancel_and_join_attempts(tasks)
        raise


async def _cancel_and_join_attempts(
    tasks: tuple[asyncio.Task[PublicOiAdmissionReceiptV2], ...],
) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def _missed_slot_matches(
    failure: PublicOiRestMissedSlotV2,
    *,
    token: PublicOiScheduledAttemptTokenV2,
    plan: ProvisionalPromotingRestCapturePlanV2,
    schedule_authority: PublicOiScheduleAuthorityV2,
) -> bool:
    validate_public_oi_scheduled_attempt_token_v2(
        token,
        plan=plan,
        schedule_authority=schedule_authority,
    )
    return (
        failure.symbol == token.symbol
        and failure.poll_cycle_seq == token.poll_cycle_seq
        and failure.symbol_ordinal == token.symbol_ordinal
        and failure.scheduled_slot_wall_ms == token.scheduled_slot_wall_ms
        and failure.observed_request_start_wall_ms
        >= token.scheduled_slot_wall_ms + plan.poll_interval_ms
    )


def _validate_attempt_result(
    plan: ProvisionalPromotingRestCapturePlanV2,
    receipt: PublicOiAdmissionReceiptV2,
    *,
    token: PublicOiScheduledAttemptTokenV2,
    schedule_authority: PublicOiScheduleAuthorityV2,
) -> None:
    assert_public_oi_scheduled_attempt_token_consumed_v2(
        token,
        plan=plan,
        schedule_authority=schedule_authority,
    )
    record = validate_public_oi_admission_receipt_v2(receipt)
    record.__post_init__()
    expected_source_key = public_oi_rest_source_logical_key_v2(token.symbol)
    if (
        record.plan_id != plan.name
        or record.transport is not TransportV2.HTTPS
        or record.venue is not plan.venue
        or record.route_id != plan.route_id
        or record.symbol != token.symbol
        or record.source_logical_key != expected_source_key
        or record.frame_seq is not None
    ):
        raise ValueError("OI adapter RawRecordV2 outer identity differs from its schedule")
    payload = PublicOiRestAttemptPayloadV2.from_canonical_bytes(
        record.payload_bytes(),
        plan=plan,
    )
    if (
        payload.symbol != token.symbol
        or payload.poll_cycle_seq != token.poll_cycle_seq
        or payload.symbol_ordinal != token.symbol_ordinal
        or payload.scheduled_slot_wall_ms != token.scheduled_slot_wall_ms
        or payload.attempt != token.attempt
    ):
        raise ValueError("OI adapter payload identity differs from its schedule")
    if (
        record.receipt_wall_ms != payload.completion_admission_wall_ms
        or record.receipt_monotonic_ns != payload.completion_admission_monotonic_ns
    ):
        raise ValueError("OI adapter outer receipt differs from inner admission completion")


def _retained_entry(
    scheduler: PublicOpenInterestRestSchedulerV2,
    *,
    token: PublicOiScheduledAttemptTokenV2,
    receipt: PublicOiAdmissionReceiptV2,
) -> PublicOiRestSlotCensusEntryV2:
    record = validate_public_oi_admission_receipt_v2(receipt)
    exact_hash = public_oi_rest_attempt_record_sha256_v2(record)
    if receipt.queued_record.encoded_sha256 != exact_hash:
        raise ValueError("OI attempt admission hash differs from its exact raw record")
    context = scheduler._require_census_context()
    return PublicOiRestSlotCensusEntryV2.for_plan(
        scheduler.plan,
        session_start_manifest_sha256=context.session_start_manifest_sha256,
        plan_bundle_sha256=context.plan_bundle_sha256,
        symbol_ordinal=token.symbol_ordinal,
        scheduled_slot_wall_ms=token.scheduled_slot_wall_ms,
        outcome=PublicOiRestCellOutcomeV2.ATTEMPT_RETAINED,
        attempt_ingest_seq=receipt.accepted_ingest_seq,
        attempt_record_sha256=exact_hash,
    )


def _fill_unstarted_entries(
    scheduler: PublicOpenInterestRestSchedulerV2,
    entries: list[PublicOiRestSlotCensusEntryV2 | None],
    *,
    scheduled_slot_wall_ms: int,
    outcome: PublicOiRestCellOutcomeV2,
) -> None:
    for ordinal, entry in enumerate(entries):
        if entry is not None:
            continue
        entries[ordinal] = scheduler._unstarted_entry(
            symbol_ordinal=ordinal,
            scheduled_slot_wall_ms=scheduled_slot_wall_ms,
            outcome=outcome,
        )


def _complete_entries(
    entries: list[PublicOiRestSlotCensusEntryV2 | None],
) -> tuple[PublicOiRestSlotCensusEntryV2, ...]:
    if any(entry is None for entry in entries):
        raise AssertionError("OI slot census does not contain one entry per symbol")
    return tuple(entry for entry in entries if entry is not None)


def _unstarted_outcome_at_stop(
    stop_receipt: ReceiptTimestamp,
    *,
    scheduled_slot_wall_ms: int,
) -> PublicOiRestCellOutcomeV2:
    _validate_receipt_timestamp(stop_receipt, "normal stop")
    slot_end = scheduled_slot_wall_ms + PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
    if stop_receipt.received_at_ms < slot_end:
        return PublicOiRestCellOutcomeV2.UNSTARTED_NORMAL_STOP
    return PublicOiRestCellOutcomeV2.UNSTARTED_SLOT_EXPIRED


def _validate_clock(clock: OpenInterestSchedulerClockV2) -> None:
    for method_name in ("utc_wall_ms", "monotonic_ns"):
        method = getattr(clock, method_name, None)
        if not callable(method) or inspect.iscoroutinefunction(method):
            raise TypeError(f"OI scheduler clock requires synchronous {method_name}")
    wait_until = getattr(clock, "wait_until", None)
    if not callable(wait_until) or not inspect.iscoroutinefunction(wait_until):
        raise TypeError("OI scheduler clock requires async wait_until")


def _validate_receipt_clock(clock: ReceiptClock) -> None:
    capture = getattr(clock, "capture", None)
    if not callable(capture) or inspect.iscoroutinefunction(capture):
        raise TypeError("OI census context requires a synchronous receipt clock")


def _capture_receipt(clock: ReceiptClock, label: str) -> ReceiptTimestamp:
    receipt = clock.capture()
    _validate_receipt_timestamp(receipt, label)
    return receipt


def _validate_receipt_timestamp(receipt: ReceiptTimestamp, label: str) -> None:
    if type(receipt) is not ReceiptTimestamp:
        raise TypeError(f"{label} clock must return an exact ReceiptTimestamp")
    _require_nonnegative_int(receipt.received_at_ms, f"{label} wall time")
    _require_nonnegative_int(
        receipt.received_monotonic_ns,
        f"{label} monotonic time",
    )


def _ceil_slot_exclusive(wall_ms: int) -> int:
    _require_nonnegative_int(wall_ms, "normal stop wall time")
    remainder = wall_ms % PUBLIC_OI_REST_POLL_INTERVAL_MS_V2
    if remainder == 0:
        return wall_ms
    return wall_ms + PUBLIC_OI_REST_POLL_INTERVAL_MS_V2 - remainder


def _require_aligned_slot(value: int, field: str) -> None:
    _require_nonnegative_int(value, field)
    if value % PUBLIC_OI_REST_POLL_INTERVAL_MS_V2 != 0:
        raise ValueError(f"{field} must be a 5-second UTC epoch multiple")


def _require_identity(value: str, field: str) -> None:
    if (
        type(value) is not str
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a bounded normalized identity")


def _require_sha256(value: str, field: str) -> None:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")


def _require_nonnegative_int(value: int, field: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a nonnegative integer")


def _require_positive_int(value: int, field: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field} must be a positive integer")
