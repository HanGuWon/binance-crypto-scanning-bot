from __future__ import annotations

import re
from dataclasses import replace

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalUsdmVenueClockRestCapturePlanV9,
    build_provisional_promoting_capture_plans_v8,
    build_provisional_promoting_capture_plans_v9,
    provisional_promoting_plan_sha256_v8,
    provisional_promoting_plan_sha256_v9,
    validate_provisional_promoting_capture_plans_v9,
)


def _clock_plan() -> ProvisionalUsdmVenueClockRestCapturePlanV9:
    plans = build_provisional_promoting_capture_plans_v9(("BTCUSDT",))
    [plan] = [
        item
        for item in plans
        if type(item) is ProvisionalUsdmVenueClockRestCapturePlanV9
    ]
    return plan


def test_v9_adds_exact_public_queryless_usdm_clock_role_without_changing_v8() -> None:
    symbols = ("BTCUSDT", "ETHUSDT")
    v8 = build_provisional_promoting_capture_plans_v8(symbols)
    v9 = build_provisional_promoting_capture_plans_v9(symbols)

    assert v9[:4] == v8
    assert provisional_promoting_plan_sha256_v8(v8) == (
        provisional_promoting_plan_sha256_v8(v9[:4])  # type: ignore[arg-type]
    )
    plan = _clock_plan()
    assert plan.venue is VenueV2.USDM_FUTURES
    assert plan.route_id == "usdm_venue_clock_rest"
    assert plan.method == "GET"
    assert plan.endpoint == "/fapi/v1/time"
    assert plan.base_url == "https://fapi.binance.com"
    assert plan.request_fields == () and plan.fixed_query == ()
    assert plan.poll_interval_ms == 30_000
    assert plan.request_timeout_ms == plan.maximum_header_rtt_ms == 2_000
    assert plan.maximum_sample_age_ms == 60_000
    assert plan.maximum_wall_monotonic_residual_ms == 2
    assert plan.maximum_rate_error_ppm == 1_000
    assert plan.auth_mode == "NONE" and not plan.requires_api_key
    assert not plan.is_private and not plan.promoting
    assert not plan.order_execution_enabled and not plan.causal_cursor_complete


def test_v9_plan_hash_is_permutation_stable_and_distinct_from_v8() -> None:
    symbols = ("BTCUSDT", "ETHUSDT")
    plans = build_provisional_promoting_capture_plans_v9(symbols)
    digest = provisional_promoting_plan_sha256_v9(plans)

    assert re.fullmatch(r"[0-9a-f]{64}", digest)
    assert digest == "4d5bfe0a4fe53c61013e4ad210ea1884eaccbd1196c7fc52da57d9deb1d9d1e3"
    assert provisional_promoting_plan_sha256_v9(tuple(reversed(plans))) == digest
    assert digest != provisional_promoting_plan_sha256_v8(
        build_provisional_promoting_capture_plans_v8(symbols)
    )


@pytest.mark.parametrize(
    ("field", "drift", "message"),
    (
        ("endpoint", "/fapi/v1/time/private", "exact public USD-M"),
        ("request_fields", ("timestamp",), "no query or private"),
        ("fixed_query", (("symbol", "BTCUSDT"),), "no query or private"),
        ("auth_mode", "API_KEY", "public and unauthenticated"),
        ("requires_api_key", True, "public and unauthenticated"),
        ("promoting", True, "not a causal cursor"),
        ("causal_cursor_complete", True, "not a causal cursor"),
    ),
)
def test_v9_clock_route_auth_and_nonclaim_drift_fail_closed(
    field: str,
    drift: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(_clock_plan(), **{field: drift})


@pytest.mark.parametrize(
    ("field", "lower", "upper"),
    (
        ("request_timeout_ms", 1_999, 2_001),
        ("maximum_header_rtt_ms", 1_999, 2_001),
        ("maximum_sample_age_ms", 59_999, 60_001),
        ("maximum_wall_monotonic_residual_ms", 1, 3),
        ("maximum_rate_error_ppm", 999, 1_001),
    ),
)
def test_v9_clock_numeric_bounds_reject_nearest_drifts(
    field: str,
    lower: int,
    upper: int,
) -> None:
    for drift in (lower, upper):
        with pytest.raises(ValueError):
            replace(_clock_plan(), **{field: drift})


def test_v9_requires_exactly_one_clock_role() -> None:
    plans = build_provisional_promoting_capture_plans_v9(("BTCUSDT",))
    with pytest.raises(ValueError, match="unchanged v8 roles"):
        validate_provisional_promoting_capture_plans_v9(plans[:4])
    with pytest.raises(ValueError, match="unchanged v8 roles"):
        validate_provisional_promoting_capture_plans_v9((*plans, plans[-1]))
