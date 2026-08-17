from __future__ import annotations

import asyncio
import inspect
from dataclasses import InitVar, dataclass, field
from typing import Protocol

from signalbot.r4b_v2.capture.plans import (
    ProvisionalUsdmVenueClockRestCapturePlanV9,
)
from signalbot.r4b_v2.capture.rest_scheduler import (
    OpenInterestSchedulerClockV2,
    SystemOpenInterestSchedulerClockV2,
)
from signalbot.r4b_v2.capture.websocket import (
    PublicUsdmVenueClockAdmissionReceiptV9,
    validate_public_usdm_venue_clock_admission_receipt_v9,
)

_SCHEDULE_AUTHORITY_FACTORY_TOKEN = object()
_SCHEDULED_ATTEMPT_FACTORY_TOKEN = object()
_MAX_SIGNED_INT64 = (1 << 63) - 1


class PublicUsdmVenueClockScheduleOwnershipErrorV9(RuntimeError):
    """A clock schedule capability was foreign, stale, or replayed."""


class PublicUsdmVenueClockMissedSlotV9(RuntimeError):
    """The local request-start receipt fell outside its selected UTC slot."""

    def __init__(
        self,
        *,
        poll_cycle_seq: int,
        scheduled_slot_wall_ms: int,
        observed_request_start_wall_ms: int,
    ) -> None:
        self.poll_cycle_seq = poll_cycle_seq
        self.scheduled_slot_wall_ms = scheduled_slot_wall_ms
        self.observed_request_start_wall_ms = observed_request_start_wall_ms
        super().__init__(
            "USD-M venue-clock request missed its selected UTC slot: "
            f"cycle={poll_cycle_seq} slot={scheduled_slot_wall_ms} "
            f"observed={observed_request_start_wall_ms}"
        )


@dataclass(frozen=True, slots=True, eq=False)
class PublicUsdmVenueClockScheduleAuthorityV9:
    """Process-local issuer for one exact public venue-clock scheduler."""

    plan: ProvisionalUsdmVenueClockRestCapturePlanV9 = field(repr=False)
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _SCHEDULE_AUTHORITY_FACTORY_TOKEN:
            raise TypeError("venue-clock schedule authority must be created by its scheduler")
        if type(self.plan) is not ProvisionalUsdmVenueClockRestCapturePlanV9:
            raise TypeError("venue-clock schedule authority requires the exact v9 plan")
        self.plan.__post_init__()
        object.__setattr__(
            self,
            "_factory_seal",
            _SCHEDULE_AUTHORITY_FACTORY_TOKEN,
        )


@dataclass(slots=True)
class _ScheduledAttemptClaimV9:
    consumed: bool = False

    def consume(self) -> None:
        if self.consumed:
            raise PublicUsdmVenueClockScheduleOwnershipErrorV9(
                "venue-clock scheduled attempt was already consumed"
            )
        self.consumed = True


@dataclass(frozen=True, slots=True, eq=False)
class PublicUsdmVenueClockScheduledAttemptTokenV9:
    """One-shot authority for one epoch-aligned, no-retry HTTP attempt."""

    plan: ProvisionalUsdmVenueClockRestCapturePlanV9 = field(repr=False)
    schedule_authority: PublicUsdmVenueClockScheduleAuthorityV9 = field(
        repr=False,
        compare=False,
    )
    poll_cycle_seq: int
    scheduled_slot_wall_ms: int
    http_attempt: int
    _factory_token: InitVar[object | None] = None
    _factory_seal: object = field(init=False, repr=False, compare=False)
    _claim: _ScheduledAttemptClaimV9 = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self, _factory_token: object | None) -> None:
        if _factory_token is not _SCHEDULED_ATTEMPT_FACTORY_TOKEN:
            raise TypeError("venue-clock scheduled attempts must be created by their scheduler")
        object.__setattr__(
            self,
            "_factory_seal",
            _SCHEDULED_ATTEMPT_FACTORY_TOKEN,
        )
        object.__setattr__(self, "_claim", _ScheduledAttemptClaimV9())
        _validate_scheduled_attempt_material_v9(self)


def validate_public_usdm_venue_clock_schedule_authority_v9(
    authority: PublicUsdmVenueClockScheduleAuthorityV9,
    *,
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
) -> None:
    """Fail unless the exact scheduler factory issued this authority."""

    if type(authority) is not PublicUsdmVenueClockScheduleAuthorityV9:
        raise TypeError("venue-clock scheduler binding requires an exact authority")
    if getattr(authority, "_factory_seal", None) is not _SCHEDULE_AUTHORITY_FACTORY_TOKEN:
        raise PublicUsdmVenueClockScheduleOwnershipErrorV9(
            "venue-clock schedule authority lacks factory provenance"
        )
    if authority.plan is not plan:
        raise PublicUsdmVenueClockScheduleOwnershipErrorV9(
            "venue-clock schedule authority belongs to a different plan object"
        )
    plan.__post_init__()


def validate_public_usdm_venue_clock_scheduled_attempt_token_v9(
    token: PublicUsdmVenueClockScheduledAttemptTokenV9,
    *,
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    schedule_authority: PublicUsdmVenueClockScheduleAuthorityV9,
) -> None:
    """Revalidate one exact, scheduler-issued attempt without consuming it."""

    if type(token) is not PublicUsdmVenueClockScheduledAttemptTokenV9:
        raise TypeError("venue-clock REST requires an exact scheduled-attempt token")
    if getattr(token, "_factory_seal", None) is not _SCHEDULED_ATTEMPT_FACTORY_TOKEN:
        raise PublicUsdmVenueClockScheduleOwnershipErrorV9(
            "venue-clock scheduled attempt lacks scheduler provenance"
        )
    validate_public_usdm_venue_clock_schedule_authority_v9(
        schedule_authority,
        plan=plan,
    )
    if token.plan is not plan or token.schedule_authority is not schedule_authority:
        raise PublicUsdmVenueClockScheduleOwnershipErrorV9(
            "venue-clock scheduled attempt belongs to a different scheduler"
        )
    _validate_scheduled_attempt_material_v9(token)


def consume_public_usdm_venue_clock_scheduled_attempt_token_v9(
    token: PublicUsdmVenueClockScheduledAttemptTokenV9,
    *,
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    schedule_authority: PublicUsdmVenueClockScheduleAuthorityV9,
) -> None:
    """Atomically claim one attempt at the actual request-start seam."""

    validate_public_usdm_venue_clock_scheduled_attempt_token_v9(
        token,
        plan=plan,
        schedule_authority=schedule_authority,
    )
    claim = getattr(token, "_claim", None)
    if type(claim) is not _ScheduledAttemptClaimV9:
        raise PublicUsdmVenueClockScheduleOwnershipErrorV9(
            "venue-clock scheduled attempt lacks its one-shot claim"
        )
    claim.consume()


def assert_public_usdm_venue_clock_scheduled_attempt_token_consumed_v9(
    token: PublicUsdmVenueClockScheduledAttemptTokenV9,
    *,
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
    schedule_authority: PublicUsdmVenueClockScheduleAuthorityV9,
) -> None:
    """Fail if an adapter returned without claiming the issued attempt."""

    validate_public_usdm_venue_clock_scheduled_attempt_token_v9(
        token,
        plan=plan,
        schedule_authority=schedule_authority,
    )
    claim = getattr(token, "_claim", None)
    if type(claim) is not _ScheduledAttemptClaimV9 or not claim.consumed:
        raise PublicUsdmVenueClockScheduleOwnershipErrorV9(
            "venue-clock adapter returned before consuming its scheduled attempt"
        )


class UsdmVenueClockRestAttemptAdapterV9(Protocol):
    """The exact adapter surface owned by the clock scheduler."""

    def bind_schedule_authority(
        self,
        authority: PublicUsdmVenueClockScheduleAuthorityV9,
        /,
    ) -> None: ...

    async def capture_attempt(
        self,
        token: PublicUsdmVenueClockScheduledAttemptTokenV9,
        /,
    ) -> PublicUsdmVenueClockAdmissionReceiptV9: ...


@dataclass(frozen=True, slots=True)
class PublicUsdmVenueClockSchedulerResultV9:
    """Bounded local scheduler result; it is not a coverage claim."""

    attempted_cycle_count: int
    last_poll_cycle_seq: int
    last_scheduled_slot_wall_ms: int | None
    last_admission_receipt: PublicUsdmVenueClockAdmissionReceiptV9 | None = field(
        repr=False,
    )
    retries_performed: int = 0
    observed_clock_completeness_claimed: bool = False
    causal_cursor_complete: bool = False
    order_execution_enabled: bool = False

    def __post_init__(self) -> None:
        _require_nonnegative_int(self.attempted_cycle_count, "attempted_cycle_count")
        _require_nonnegative_int(self.last_poll_cycle_seq, "last_poll_cycle_seq")
        if self.attempted_cycle_count != self.last_poll_cycle_seq:
            raise ValueError("venue-clock attempted count and cycle sequence differ")
        if self.attempted_cycle_count == 0:
            if (
                self.last_scheduled_slot_wall_ms is not None
                or self.last_admission_receipt is not None
            ):
                raise ValueError("an empty clock result cannot retain a last attempt")
        else:
            _require_nonnegative_int(
                self.last_scheduled_slot_wall_ms,
                "last_scheduled_slot_wall_ms",
            )
            if type(self.last_admission_receipt) is not PublicUsdmVenueClockAdmissionReceiptV9:
                raise TypeError("a non-empty clock result requires an exact receipt")
        if self.retries_performed != 0:
            raise ValueError("the frozen venue-clock plan permits no retry")
        for field_name in (
            "observed_clock_completeness_claimed",
            "causal_cursor_complete",
            "order_execution_enabled",
        ):
            if getattr(self, field_name) is not False:
                raise ValueError(f"{field_name} must remain explicitly false")


class PublicUsdmVenueClockRestSchedulerV9:
    """One-run, one-in-flight, epoch-aligned public venue-time scheduler.

    The frozen plan permits one HTTP attempt per selected 30-second slot and
    zero retries.  If the process is late, skipped slots are never backfilled.
    Normal stop wakes an idle deadline wait and lets an already-started attempt
    drain under the adapter's two-second total deadline.
    """

    def __init__(
        self,
        plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
        adapter: UsdmVenueClockRestAttemptAdapterV9,
        *,
        clock: OpenInterestSchedulerClockV2 | None = None,
    ) -> None:
        if type(plan) is not ProvisionalUsdmVenueClockRestCapturePlanV9:
            raise TypeError("venue-clock scheduler requires the exact v9 plan")
        plan.__post_init__()
        capture_attempt = getattr(adapter, "capture_attempt", None)
        if not callable(capture_attempt) or not inspect.iscoroutinefunction(capture_attempt):
            raise TypeError("venue-clock scheduler adapter requires async capture_attempt")
        bind = getattr(adapter, "bind_schedule_authority", None)
        if not callable(bind) or inspect.iscoroutinefunction(bind):
            raise TypeError("venue-clock scheduler adapter requires synchronous authority binding")
        selected_clock = SystemOpenInterestSchedulerClockV2() if clock is None else clock
        _validate_scheduler_clock(selected_clock)
        authority = PublicUsdmVenueClockScheduleAuthorityV9(
            plan=plan,
            _factory_token=_SCHEDULE_AUTHORITY_FACTORY_TOKEN,
        )
        result = bind(authority)
        if result is not None:
            raise TypeError("venue-clock schedule authority binding must return None")

        self.plan = plan
        self.adapter = adapter
        self.clock = selected_clock
        self.schedule_authority = authority
        self._stop_event = asyncio.Event()
        self._started_once = False
        self._running = False
        self._in_flight = False
        self._attempted_cycle_count = 0
        self._last_scheduled_slot_wall_ms: int | None = None
        self._last_admission_receipt: PublicUsdmVenueClockAdmissionReceiptV9 | None = None
        self._last_observed_wall_ms: int | None = None
        self._last_observed_monotonic_ns: int | None = None

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
    def attempted_cycle_count(self) -> int:
        return self._attempted_cycle_count

    @property
    def stop_requested(self) -> bool:
        return self._stop_event.is_set()

    def request_stop(self) -> None:
        """Wake the scheduler without cancelling an admitted in-flight attempt."""

        self._stop_event.set()

    async def run(self) -> PublicUsdmVenueClockSchedulerResultV9:
        if self._started_once:
            raise RuntimeError("venue-clock scheduler may run only once")
        self._started_once = True
        self._running = True
        try:
            next_slot = _next_slot_at_or_after(
                self._read_wall_ms(),
                self.plan.poll_interval_ms,
            )
            while not self._stop_event.is_set():
                next_slot = await self._wait_for_selected_slot(next_slot)
                if self._stop_event.is_set():
                    break
                if self._attempted_cycle_count == _MAX_SIGNED_INT64:
                    raise OverflowError("venue-clock poll-cycle sequence is exhausted")
                poll_cycle_seq = self._attempted_cycle_count + 1
                token = PublicUsdmVenueClockScheduledAttemptTokenV9(
                    plan=self.plan,
                    schedule_authority=self.schedule_authority,
                    poll_cycle_seq=poll_cycle_seq,
                    scheduled_slot_wall_ms=next_slot,
                    http_attempt=1,
                    _factory_token=_SCHEDULED_ATTEMPT_FACTORY_TOKEN,
                )
                self._in_flight = True
                try:
                    receipt = await self.adapter.capture_attempt(token)
                finally:
                    self._in_flight = False
                assert_public_usdm_venue_clock_scheduled_attempt_token_consumed_v9(
                    token,
                    plan=self.plan,
                    schedule_authority=self.schedule_authority,
                )
                validate_public_usdm_venue_clock_admission_receipt_v9(
                    receipt,
                    plan=self.plan,
                )
                self._attempted_cycle_count = poll_cycle_seq
                self._last_scheduled_slot_wall_ms = next_slot
                self._last_admission_receipt = receipt
                next_slot += self.plan.poll_interval_ms
                observed_wall_ms = self._read_wall_ms()
                if observed_wall_ms >= next_slot + self.plan.poll_interval_ms:
                    next_slot = _next_slot_at_or_after(
                        observed_wall_ms,
                        self.plan.poll_interval_ms,
                    )
        finally:
            self._running = False
            self._in_flight = False
        return PublicUsdmVenueClockSchedulerResultV9(
            attempted_cycle_count=self._attempted_cycle_count,
            last_poll_cycle_seq=self._attempted_cycle_count,
            last_scheduled_slot_wall_ms=self._last_scheduled_slot_wall_ms,
            last_admission_receipt=self._last_admission_receipt,
        )

    async def _wait_for_selected_slot(self, selected_slot_wall_ms: int) -> int:
        while True:
            observed_wall_ms = self._read_wall_ms()
            if observed_wall_ms >= selected_slot_wall_ms:
                if observed_wall_ms >= (selected_slot_wall_ms + self.plan.poll_interval_ms):
                    return _next_slot_at_or_after(
                        observed_wall_ms,
                        self.plan.poll_interval_ms,
                    )
                return selected_slot_wall_ms
            observed_monotonic_ns = self._read_monotonic_ns()
            wait_ns = (selected_slot_wall_ms - observed_wall_ms) * 1_000_000
            stopped = await self.clock.wait_until(
                self._stop_event,
                observed_monotonic_ns + wait_ns,
            )
            if type(stopped) is not bool:
                raise TypeError("venue-clock scheduler wait must return a boolean")
            if stopped:
                if not self._stop_event.is_set():
                    raise RuntimeError("venue-clock scheduler wait reported an unrequested stop")
                return selected_slot_wall_ms

    def _read_wall_ms(self) -> int:
        value = self.clock.utc_wall_ms()
        _require_nonnegative_int(value, "scheduler utc_wall_ms")
        previous = self._last_observed_wall_ms
        if previous is not None and value < previous:
            raise ValueError("venue-clock scheduler UTC wall clock moved backwards")
        self._last_observed_wall_ms = value
        return value

    def _read_monotonic_ns(self) -> int:
        value = self.clock.monotonic_ns()
        _require_nonnegative_int(value, "scheduler monotonic_ns")
        previous = self._last_observed_monotonic_ns
        if previous is not None and value < previous:
            raise ValueError("venue-clock scheduler monotonic clock moved backwards")
        self._last_observed_monotonic_ns = value
        return value


def _validate_scheduled_attempt_material_v9(
    token: PublicUsdmVenueClockScheduledAttemptTokenV9,
) -> None:
    if type(token.plan) is not ProvisionalUsdmVenueClockRestCapturePlanV9:
        raise TypeError("venue-clock scheduled attempt requires the exact v9 plan")
    token.plan.__post_init__()
    validate_public_usdm_venue_clock_schedule_authority_v9(
        token.schedule_authority,
        plan=token.plan,
    )
    _require_positive_int(token.poll_cycle_seq, "poll_cycle_seq")
    _require_nonnegative_int(token.scheduled_slot_wall_ms, "scheduled_slot_wall_ms")
    if token.scheduled_slot_wall_ms % token.plan.poll_interval_ms:
        raise ValueError("venue-clock scheduled slot is not UTC-epoch aligned")
    if token.http_attempt != 1 or token.plan.maximum_attempts != 1:
        raise ValueError("venue-clock schedule permits exactly one HTTP attempt")


def _next_slot_at_or_after(wall_ms: int, interval_ms: int) -> int:
    _require_nonnegative_int(wall_ms, "wall_ms")
    _require_positive_int(interval_ms, "interval_ms")
    quotient, remainder = divmod(wall_ms, interval_ms)
    return quotient * interval_ms if remainder == 0 else (quotient + 1) * interval_ms


def _validate_scheduler_clock(clock: OpenInterestSchedulerClockV2) -> None:
    for method_name in ("utc_wall_ms", "monotonic_ns"):
        method = getattr(clock, method_name, None)
        if not callable(method) or inspect.iscoroutinefunction(method):
            raise TypeError(f"scheduler clock requires synchronous {method_name}")
    wait_until = getattr(clock, "wait_until", None)
    if not callable(wait_until) or not inspect.iscoroutinefunction(wait_until):
        raise TypeError("scheduler clock requires async wait_until")


def _require_nonnegative_int(value: object, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field_name} must be a nonnegative integer")


def _require_positive_int(value: object, field_name: str) -> None:
    if type(value) is not int or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
