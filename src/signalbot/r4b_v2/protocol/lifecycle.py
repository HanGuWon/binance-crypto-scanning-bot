from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from fractions import Fraction

MILLISECONDS_PER_MINUTE_V2 = 60_000
MILLISECONDS_PER_DAY_V2 = 24 * 60 * MILLISECONDS_PER_MINUTE_V2
EFFICACY_CALENDAR_DAYS_V2 = 365
FINAL_ADMISSION_TAIL_MS_V2 = 75 * MILLISECONDS_PER_MINUTE_V2
CONFIRMATION_GRACE_MS_V2 = MILLISECONDS_PER_DAY_V2

FIRST_ATTEMPT_ALPHA_V2 = Fraction(1, 40)
LIFETIME_ALPHA_LIMIT_V2 = Fraction(1, 20)
V1_EFFICACY_ALPHA_CONSUMED_V2 = Fraction(0, 1)


class ProtocolLifecycleError(ValueError):
    """Raised when a V2 attempt or fixed-horizon invariant is violated."""


class AttemptTerminalStatusV2(StrEnum):
    """Terminal labels that cannot refund or otherwise alter consumed alpha."""

    PASS = "PASS"
    FAIL = "FAIL"
    VOID = "VOID"
    INCONCLUSIVE_COVERAGE = "INCONCLUSIVE_COVERAGE"
    INCONCLUSIVE_DATA = "INCONCLUSIVE_DATA"


class HorizonPhaseV2(StrEnum):
    """Time-only lifecycle phases; no member authorizes outcome computation."""

    PRE_H_START = "PRE_H_START"
    EFFICACY = "EFFICACY"
    CONFIRMATION_ONLY = "CONFIRMATION_ONLY"
    CLOSED = "CLOSED"


def attempt_alpha_v2(attempt_index: int) -> Fraction:
    """Return exact one-sided alpha_v = 0.025 * 2**(-(v-1))."""

    _require_positive_int(attempt_index, "attempt_index")
    return FIRST_ATTEMPT_ALPHA_V2 / (1 << (attempt_index - 1))


@dataclass(frozen=True, slots=True)
class FixedHorizonV2:
    """Fixed UTC-millisecond boundaries for one 365-calendar-day attempt.

    The efficacy interval is ``[h_start_ms, h_max_ms)``. The interval from
    ``h_max_ms`` through ``confirmation_grace_end_ms`` is inclusive and may be
    used only to confirm pre-existing metadata or integrity facts.
    """

    h_start_ms: int

    def __post_init__(self) -> None:
        _require_nonnegative_ms(self.h_start_ms, "h_start_ms")
        if self.h_start_ms % MILLISECONDS_PER_DAY_V2 != 0:
            raise ProtocolLifecycleError("H_start must be aligned to UTC midnight")

    @property
    def h_max_ms(self) -> int:
        """Return the immutable exclusive end of the efficacy interval."""

        return self.h_start_ms + EFFICACY_CALENDAR_DAYS_V2 * MILLISECONDS_PER_DAY_V2

    @property
    def admission_stop_ms(self) -> int:
        """Return the exclusive decision-admission cutoff 75 minutes before H_max."""

        return self.h_max_ms - FINAL_ADMISSION_TAIL_MS_V2

    @property
    def confirmation_grace_end_ms(self) -> int:
        """Return inclusive G, exactly 24 hours after H_max."""

        return self.h_max_ms + CONFIRMATION_GRACE_MS_V2

    def phase_at(self, wall_ms: int) -> HorizonPhaseV2:
        """Classify a wall-clock instant without reading signals or outcomes."""

        _require_nonnegative_ms(wall_ms, "wall_ms")
        if wall_ms < self.h_start_ms:
            return HorizonPhaseV2.PRE_H_START
        if wall_ms < self.h_max_ms:
            return HorizonPhaseV2.EFFICACY
        if wall_ms <= self.confirmation_grace_end_ms:
            return HorizonPhaseV2.CONFIRMATION_ONLY
        return HorizonPhaseV2.CLOSED

    def admits_decision(self, decision_cutoff_ms: int) -> bool:
        """Return whether D is inside efficacy and strictly before the final tail."""

        _require_nonnegative_ms(decision_cutoff_ms, "decision_cutoff_ms")
        return self.h_start_ms <= decision_cutoff_ms < self.admission_stop_ms

    def accepts_confirmation_completion(self, completion_wall_ms: int) -> bool:
        """Return whether completion falls inside the inclusive confirmation grace."""

        return self.phase_at(completion_wall_ms) is HorizonPhaseV2.CONFIRMATION_ONLY


@dataclass(frozen=True, slots=True)
class ProspectiveAttemptV2:
    """One registered V2 attempt and its complete qualification-to-grace interval."""

    attempt_index: int
    qualification_start_ms: int
    horizon: FixedHorizonV2
    terminal_status: AttemptTerminalStatusV2 | None = None

    def __post_init__(self) -> None:
        _require_positive_int(self.attempt_index, "attempt_index")
        _require_nonnegative_ms(self.qualification_start_ms, "qualification_start_ms")
        if not isinstance(self.horizon, FixedHorizonV2):
            raise ProtocolLifecycleError("horizon must be a FixedHorizonV2")
        if self.qualification_start_ms >= self.horizon.h_start_ms:
            raise ProtocolLifecycleError(
                "qualification_start_ms must be strictly earlier than H_start"
            )
        if self.terminal_status is not None and not isinstance(
            self.terminal_status, AttemptTerminalStatusV2
        ):
            raise ProtocolLifecycleError(
                "terminal_status must be an AttemptTerminalStatusV2 or None"
            )

    @property
    def nominal_alpha(self) -> Fraction:
        """Return the exact alpha consumed once this attempt reaches H_start."""

        return attempt_alpha_v2(self.attempt_index)

    @property
    def interval_end_inclusive_ms(self) -> int:
        """Return G, the inclusive end of this attempt's finalization interval."""

        return self.horizon.confirmation_grace_end_ms


@dataclass(frozen=True, slots=True)
class AttemptAlphaRegistryV2:
    """Immutable registry for prospective V2 alpha and non-overlap invariants.

    Registered future attempts consume no alpha before their H_start. At exact
    H_start their nominal alpha is consumed permanently, independent of any
    eventual terminal status. V1 is deliberately outside this registry and
    consumes zero efficacy alpha.
    """

    attempts: tuple[ProspectiveAttemptV2, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.attempts, tuple):
            raise ProtocolLifecycleError("attempts must be an immutable tuple")
        previous: ProspectiveAttemptV2 | None = None
        for expected_index, attempt in enumerate(self.attempts, start=1):
            if not isinstance(attempt, ProspectiveAttemptV2):
                raise ProtocolLifecycleError(
                    "attempts must contain only ProspectiveAttemptV2 values"
                )
            if attempt.attempt_index != expected_index:
                raise ProtocolLifecycleError(
                    "attempt indices must be consecutive and begin at one"
                )
            if (
                previous is not None
                and attempt.qualification_start_ms
                <= previous.interval_end_inclusive_ms
            ):
                raise ProtocolLifecycleError(
                    "successor qualification and attempt intervals must be strictly later "
                    "and non-overlapping"
                )
            previous = attempt

    @property
    def v1_consumed_alpha(self) -> Fraction:
        """Return the controlling V1 efficacy-alpha consumption: exactly zero."""

        return V1_EFFICACY_ALPHA_CONSUMED_V2

    @property
    def next_attempt_index(self) -> int:
        """Return the one-based index for the next actual prospective attempt."""

        return len(self.attempts) + 1

    @property
    def next_attempt_alpha(self) -> Fraction:
        """Return the exact alpha assigned to the next actual attempt."""

        return attempt_alpha_v2(self.next_attempt_index)

    @property
    def registered_nominal_alpha(self) -> Fraction:
        """Return alpha assigned to every registered attempt, including future starts."""

        return sum(
            (attempt.nominal_alpha for attempt in self.attempts),
            start=Fraction(0, 1),
        )

    def alpha_consumed_at(self, wall_ms: int) -> Fraction:
        """Return alpha consumed by H_starts reached at or before ``wall_ms``."""

        _require_nonnegative_ms(wall_ms, "wall_ms")
        return sum(
            (
                attempt.nominal_alpha
                for attempt in self.attempts
                if attempt.horizon.h_start_ms <= wall_ms
            ),
            start=V1_EFFICACY_ALPHA_CONSUMED_V2,
        )

    def register_attempt(
        self,
        *,
        qualification_start_ms: int,
        h_start_ms: int,
        terminal_status: AttemptTerminalStatusV2 | None = None,
    ) -> AttemptAlphaRegistryV2:
        """Return a new registry with one consecutively indexed attempt appended."""

        attempt = ProspectiveAttemptV2(
            attempt_index=self.next_attempt_index,
            qualification_start_ms=qualification_start_ms,
            horizon=FixedHorizonV2(h_start_ms=h_start_ms),
            terminal_status=terminal_status,
        )
        return AttemptAlphaRegistryV2(attempts=(*self.attempts, attempt))


def _require_positive_int(value: int, field_name: str) -> None:
    if type(value) is not int or value < 1:
        raise ProtocolLifecycleError(f"{field_name} must be a positive integer")


def _require_nonnegative_ms(value: int, field_name: str) -> None:
    if type(value) is not int or value < 0:
        raise ProtocolLifecycleError(
            f"{field_name} must be a nonnegative Unix-millisecond integer"
        )
