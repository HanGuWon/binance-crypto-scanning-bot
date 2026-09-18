"""WP5: impossible timestamp orderings must be rejected.

created < finished, activation > prereg, seal before run_end, and materially
future-dated timestamps are all invalid for evidence artifacts.
"""

from __future__ import annotations

import pytest


def _validate_ordering(
    *,
    created_ms: int,
    finished_ms: int,
    prereg_ms: int,
    activation_ms: int,
    run_end_ms: int,
    seal_ms: int,
    now_ms: int,
) -> None:
    if created_ms > now_ms + 60_000:
        raise ValueError("materially future-dated created_at_ms is invalid")
    if created_ms >= finished_ms:
        raise ValueError("created_at_ms must precede finished_at_ms")
    if activation_ms <= prereg_ms:
        raise ValueError("activation_ms must strictly follow prereg_ms")
    if seal_ms < run_end_ms:
        raise ValueError("seal_ms must not precede run_end_ms")


def test_valid_ordering_accepted() -> None:
    _validate_ordering(
        created_ms=1_000,
        finished_ms=2_000,
        prereg_ms=3_000,
        activation_ms=4_000,
        run_end_ms=5_000,
        seal_ms=6_000,
        now_ms=10_000,
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("created_ms", 3_000),   # created >= finished
        ("prereg_ms", 5_000),    # activation <= prereg
        ("seal_ms", 1_000),      # seal < run_end
    ],
)
def test_impossible_orderings_rejected(field: str, value: int) -> None:
    kwargs = dict(
        created_ms=1_000,
        finished_ms=2_000,
        prereg_ms=3_000,
        activation_ms=4_000,
        run_end_ms=5_000,
        seal_ms=6_000,
        now_ms=10_000,
    )
    kwargs[field] = value
    with pytest.raises(ValueError):
        _validate_ordering(**kwargs)


def test_materially_future_timestamp_rejected() -> None:
    with pytest.raises(ValueError, match="future"):
        _validate_ordering(
            created_ms=99_999_999,
            finished_ms=2_000,
            prereg_ms=3_000,
            activation_ms=4_000,
            run_end_ms=5_000,
            seal_ms=6_000,
            now_ms=10_000,
        )
