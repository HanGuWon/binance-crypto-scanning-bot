from __future__ import annotations

from fractions import Fraction

import pytest
from hypothesis import given
from hypothesis import strategies as st

from signalbot.r4b_v2.protocol.lifecycle import (
    CONFIRMATION_GRACE_MS_V2,
    EFFICACY_CALENDAR_DAYS_V2,
    FINAL_ADMISSION_TAIL_MS_V2,
    LIFETIME_ALPHA_LIMIT_V2,
    MILLISECONDS_PER_DAY_V2,
    V1_EFFICACY_ALPHA_CONSUMED_V2,
    AttemptAlphaRegistryV2,
    AttemptTerminalStatusV2,
    FixedHorizonV2,
    HorizonPhaseV2,
    ProspectiveAttemptV2,
    ProtocolLifecycleError,
    attempt_alpha_v2,
)

DAY_MS = MILLISECONDS_PER_DAY_V2
H_START_MS = 2_000_160_000_000


def _first_utc_midnight_at_or_after(wall_ms: int) -> int:
    return ((wall_ms + DAY_MS - 1) // DAY_MS) * DAY_MS


def test_positive_exact_alpha_formula_and_v1_baseline() -> None:
    registry = AttemptAlphaRegistryV2()

    assert V1_EFFICACY_ALPHA_CONSUMED_V2 == Fraction(0, 1)
    assert registry.v1_consumed_alpha == Fraction(0, 1)
    assert attempt_alpha_v2(1) == Fraction(1, 40)
    assert attempt_alpha_v2(2) == Fraction(1, 80)
    assert attempt_alpha_v2(5) == Fraction(1, 640)
    assert registry.next_attempt_index == 1
    assert registry.next_attempt_alpha == Fraction(1, 40)


@pytest.mark.parametrize("terminal_status", [None, *tuple(AttemptTerminalStatusV2)])
def test_equality_h_start_consumes_alpha_regardless_of_terminal_status(
    terminal_status: AttemptTerminalStatusV2 | None,
) -> None:
    registry = AttemptAlphaRegistryV2().register_attempt(
        qualification_start_ms=H_START_MS - 30 * DAY_MS,
        h_start_ms=H_START_MS,
        terminal_status=terminal_status,
    )

    assert registry.alpha_consumed_at(H_START_MS - 1) == Fraction(0, 1)
    assert registry.alpha_consumed_at(H_START_MS) == Fraction(1, 40)
    assert registry.alpha_consumed_at(H_START_MS + 1) == Fraction(1, 40)


def test_positive_strictly_later_successor_interval_is_accepted() -> None:
    first = AttemptAlphaRegistryV2().register_attempt(
        qualification_start_ms=H_START_MS - 30 * DAY_MS,
        h_start_ms=H_START_MS,
        terminal_status=AttemptTerminalStatusV2.FAIL,
    )
    first_grace_end = first.attempts[0].interval_end_inclusive_ms
    successor_qualification_start = first_grace_end + 1
    successor_h_start = (
        _first_utc_midnight_at_or_after(successor_qualification_start)
        + 30 * DAY_MS
    )

    registry = first.register_attempt(
        qualification_start_ms=successor_qualification_start,
        h_start_ms=successor_h_start,
        terminal_status=AttemptTerminalStatusV2.PASS,
    )

    assert registry.next_attempt_index == 3
    assert registry.registered_nominal_alpha == Fraction(3, 80)
    assert registry.alpha_consumed_at(successor_h_start - 1) == Fraction(1, 40)
    assert registry.alpha_consumed_at(successor_h_start) == Fraction(3, 80)


def test_negative_reused_or_touching_successor_interval_is_rejected() -> None:
    first = AttemptAlphaRegistryV2().register_attempt(
        qualification_start_ms=H_START_MS - 30 * DAY_MS,
        h_start_ms=H_START_MS,
    )
    first_grace_end = first.attempts[0].interval_end_inclusive_ms

    for invalid_start in (H_START_MS, first_grace_end - DAY_MS, first_grace_end):
        with pytest.raises(ProtocolLifecycleError, match="strictly later"):
            first.register_attempt(
                qualification_start_ms=invalid_start,
                h_start_ms=first_grace_end + 30 * DAY_MS,
            )


def test_equality_fixed_horizon_admission_and_confirmation_boundaries() -> None:
    horizon = FixedHorizonV2(h_start_ms=H_START_MS)

    assert horizon.h_max_ms == H_START_MS + EFFICACY_CALENDAR_DAYS_V2 * DAY_MS
    assert horizon.admission_stop_ms == horizon.h_max_ms - FINAL_ADMISSION_TAIL_MS_V2
    assert horizon.confirmation_grace_end_ms == (
        horizon.h_max_ms + CONFIRMATION_GRACE_MS_V2
    )
    assert horizon.phase_at(H_START_MS - 1) is HorizonPhaseV2.PRE_H_START
    assert horizon.phase_at(H_START_MS) is HorizonPhaseV2.EFFICACY
    assert horizon.phase_at(horizon.h_max_ms - 1) is HorizonPhaseV2.EFFICACY
    assert horizon.phase_at(horizon.h_max_ms) is HorizonPhaseV2.CONFIRMATION_ONLY
    assert (
        horizon.phase_at(horizon.confirmation_grace_end_ms)
        is HorizonPhaseV2.CONFIRMATION_ONLY
    )
    assert (
        horizon.phase_at(horizon.confirmation_grace_end_ms + 1)
        is HorizonPhaseV2.CLOSED
    )

    assert horizon.admits_decision(H_START_MS)
    assert horizon.admits_decision(horizon.admission_stop_ms - 1)
    assert not horizon.admits_decision(horizon.admission_stop_ms)
    assert not horizon.admits_decision(horizon.h_max_ms)
    assert horizon.accepts_confirmation_completion(horizon.confirmation_grace_end_ms)
    assert not horizon.accepts_confirmation_completion(
        horizon.confirmation_grace_end_ms + 1
    )


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (lambda: attempt_alpha_v2(0), "attempt_index"),
        (lambda: attempt_alpha_v2(True), "attempt_index"),
        (lambda: FixedHorizonV2(h_start_ms=-1), "h_start_ms"),
        (
            lambda: ProspectiveAttemptV2(
                attempt_index=1,
                qualification_start_ms=H_START_MS + 1,
                horizon=FixedHorizonV2(H_START_MS),
            ),
            "qualification_start_ms",
        ),
        (
            lambda: FixedHorizonV2(h_start_ms=H_START_MS + 1),
            "UTC midnight",
        ),
        (
            lambda: AttemptAlphaRegistryV2(
                attempts=(
                        ProspectiveAttemptV2(
                            attempt_index=2,
                            qualification_start_ms=H_START_MS - 30 * DAY_MS,
                            horizon=FixedHorizonV2(H_START_MS),
                        ),
                )
            ),
            "consecutive",
        ),
    ],
)
def test_negative_invalid_lifecycle_models_fail_closed(
    factory: object,
    message: str,
) -> None:
    callable_factory = factory
    assert callable(callable_factory)
    with pytest.raises(ProtocolLifecycleError, match=message):
        callable_factory()


def test_negative_invalid_times_do_not_silently_classify_or_admit() -> None:
    horizon = FixedHorizonV2(H_START_MS)
    registry = AttemptAlphaRegistryV2()

    with pytest.raises(ProtocolLifecycleError, match="wall_ms"):
        horizon.phase_at(-1)
    with pytest.raises(ProtocolLifecycleError, match="decision_cutoff_ms"):
        horizon.admits_decision(True)
    with pytest.raises(ProtocolLifecycleError, match="wall_ms"):
        registry.alpha_consumed_at(-1)


@given(st.integers(min_value=1, max_value=512))
def test_property_finite_alpha_sequence_is_exact_and_below_lifetime_limit(
    attempt_count: int,
) -> None:
    observed = sum(
        (attempt_alpha_v2(index) for index in range(1, attempt_count + 1)),
        start=Fraction(0, 1),
    )
    expected = LIFETIME_ALPHA_LIMIT_V2 * (
        Fraction(1, 1) - Fraction(1, 2**attempt_count)
    )

    assert observed == expected
    assert observed < LIFETIME_ALPHA_LIMIT_V2


@given(
    st.lists(
        st.sampled_from(tuple(AttemptTerminalStatusV2)),
        min_size=1,
        max_size=24,
    )
)
def test_property_terminal_status_permutations_cannot_change_consumed_alpha(
    statuses: list[AttemptTerminalStatusV2],
) -> None:
    registry = AttemptAlphaRegistryV2()
    next_qualification_start = H_START_MS - 30 * DAY_MS
    last_h_start = H_START_MS
    for terminal_status in statuses:
        last_h_start = (
            _first_utc_midnight_at_or_after(next_qualification_start)
            + 30 * DAY_MS
        )
        registry = registry.register_attempt(
            qualification_start_ms=next_qualification_start,
            h_start_ms=last_h_start,
            terminal_status=terminal_status,
        )
        next_qualification_start = (
            registry.attempts[-1].interval_end_inclusive_ms + 1
        )

    assert registry.alpha_consumed_at(last_h_start) == registry.registered_nominal_alpha
    assert registry.registered_nominal_alpha < LIFETIME_ALPHA_LIMIT_V2


@given(
    h_start_day=st.integers(
        min_value=0,
        max_value=4_000_000_000_000 // DAY_MS,
    ),
    offset_ms=st.integers(
        min_value=-DAY_MS,
        max_value=EFFICACY_CALENDAR_DAYS_V2 * DAY_MS + CONFIRMATION_GRACE_MS_V2 + DAY_MS,
    ),
)
def test_property_horizon_boundaries_are_fixed_and_path_independent(
    h_start_day: int,
    offset_ms: int,
) -> None:
    h_start_ms = h_start_day * DAY_MS
    horizon = FixedHorizonV2(h_start_ms)
    wall_ms = h_start_ms + offset_ms

    assert horizon.h_max_ms - horizon.h_start_ms == (
        EFFICACY_CALENDAR_DAYS_V2 * DAY_MS
    )
    assert horizon.confirmation_grace_end_ms - horizon.h_max_ms == (
        CONFIRMATION_GRACE_MS_V2
    )
    if wall_ms >= 0:
        assert horizon.admits_decision(wall_ms) == (
            horizon.h_start_ms <= wall_ms < horizon.admission_stop_ms
        )
