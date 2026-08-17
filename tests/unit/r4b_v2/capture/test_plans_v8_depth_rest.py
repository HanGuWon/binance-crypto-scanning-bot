from __future__ import annotations

import re
from dataclasses import replace

import pytest

from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalDepthRestQualificationPlanV8,
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingRestCapturePlanV2,
    build_provisional_promoting_capture_plans_v2,
    build_provisional_promoting_capture_plans_v8,
    provisional_promoting_plan_sha256_v2,
    provisional_promoting_plan_sha256_v8,
    validate_provisional_promoting_capture_plans_v8,
)


def _depth_plan(
    symbols: tuple[str, ...] = ("BTCUSDT",),
) -> ProvisionalDepthRestQualificationPlanV8:
    plans = build_provisional_promoting_capture_plans_v8(symbols)
    depth_plans = tuple(
        plan for plan in plans if type(plan) is ProvisionalDepthRestQualificationPlanV8
    )
    assert len(depth_plans) == 1
    return depth_plans[0]


def test_v8_adds_exact_public_depth_rest_role_without_changing_v7() -> None:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    v7_plans = build_provisional_promoting_capture_plans_v2(symbols)
    plans = build_provisional_promoting_capture_plans_v8(symbols)

    assert len(plans) == 4
    assert plans[:3] == v7_plans
    assert provisional_promoting_plan_sha256_v2(v7_plans) == (
        "bea99162fa15c4366b250844404cdfc8e8de6302ae5a6de922efdc101b161429"
    )
    assert sum(type(plan) is ProvisionalPromotingCapturePlanV2 for plan in plans) == 2
    assert sum(type(plan) is ProvisionalPromotingRestCapturePlanV2 for plan in plans) == 1

    depth_plan = _depth_plan(symbols)
    assert depth_plan.venue is VenueV2.USDM_FUTURES
    assert depth_plan.route_id == "usdm_public_depth_rest"
    assert depth_plan.method == "GET"
    assert depth_plan.endpoint == "/fapi/v1/depth"
    assert depth_plan.symbols == symbols
    assert depth_plan.base_url == "https://fapi.binance.com"
    assert depth_plan.request_fields == ("symbol", "limit")
    assert depth_plan.fixed_query == (("limit", "1000"),)
    assert depth_plan.maximum_query_limit == 1_000
    assert depth_plan.request_weight == 20
    assert depth_plan.request_timeout_ms == 4_000
    assert depth_plan.maximum_body_bytes == 1_048_576
    assert depth_plan.maximum_concurrency == 4
    assert depth_plan.maximum_attempts == 1
    assert depth_plan.retryable_status_codes == ()
    assert depth_plan.retryable_error_categories == ()
    assert depth_plan.retry_backoff_ms == ()
    assert depth_plan.retry_jitter_mode == "NONE"
    assert depth_plan.maximum_retry_after_ms == 0
    assert depth_plan.snapshot_triggers == ("startup", "reconnect", "sequence_gap")
    assert depth_plan.bridge_maximum_attempts == 3
    assert depth_plan.bridge_wait_timeout_ms == 2_000
    assert depth_plan.periodic_cadence_ms is None
    assert depth_plan.periodic_cadence_policy.endswith("NO_PNL")
    assert depth_plan.cadence_selection_uses_pnl is False
    assert depth_plan.periodic_cadence_promoting is False
    assert depth_plan.auth_mode == "NONE"
    assert depth_plan.requires_api_key is False
    assert depth_plan.is_private is False
    assert depth_plan.order_execution_enabled is False
    assert depth_plan.promotion_ready is False
    assert depth_plan.promoting is False
    assert not hasattr(depth_plan, "api_key")
    assert not hasattr(depth_plan, "secret")


@pytest.mark.parametrize(
    ("field", "drift", "message"),
    (
        ("route_id", "usdm_public_rest", "usdm_public_depth_rest"),
        ("method", "POST", "exactly GET"),
        ("endpoint", "/fapi/v1/depth/private", "exactly /fapi/v1/depth"),
        ("base_url", "https://fapi.binance.com/", "request authority"),
        ("request_fields", ("symbol",), "fixed limit=1000"),
        ("request_fields", ("symbol", "limit", "signature"), "fixed limit=1000"),
        ("fixed_query", (("limit", "999"),), "fixed limit=1000"),
        ("maximum_query_limit", 999, "fixed limit=1000"),
        ("auth_mode", "API_KEY", "public and unauthenticated"),
        ("requires_api_key", True, "public and unauthenticated"),
        ("is_private", True, "public and unauthenticated"),
    ),
)
def test_depth_rest_rejects_route_request_limit_and_auth_drift(
    field: str,
    drift: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(_depth_plan(), **{field: drift})


@pytest.mark.parametrize(
    ("field", "lower", "upper"),
    (
        ("maximum_query_limit", 999, 1_001),
        ("request_weight", 19, 21),
        ("request_timeout_ms", 3_999, 4_001),
        ("maximum_body_bytes", 1_048_575, 1_048_577),
        ("maximum_concurrency", 3, 5),
        ("maximum_attempts", 0, 2),
        ("maximum_retry_after_ms", -1, 1),
        ("maximum_symbol_census", 31, 33),
        ("bridge_maximum_attempts", 2, 4),
        ("bridge_wait_timeout_ms", 1_999, 2_001),
    ),
)
def test_depth_rest_numeric_bounds_reject_both_nearest_drifts(
    field: str,
    lower: int,
    upper: int,
) -> None:
    for drift in (lower, upper):
        with pytest.raises(ValueError):
            replace(_depth_plan(), **{field: drift})


@pytest.mark.parametrize(
    ("field", "drift"),
    (
        ("maximum_query_limit", 1000.0),
        ("request_weight", 20.0),
        ("request_timeout_ms", 4000.0),
        ("maximum_body_bytes", 1048576.0),
        ("maximum_concurrency", 4.0),
        ("maximum_attempts", True),
        ("maximum_retry_after_ms", False),
        ("maximum_symbol_census", 32.0),
        ("bridge_maximum_attempts", 3.0),
        ("bridge_wait_timeout_ms", 2000.0),
    ),
)
def test_depth_rest_integer_policy_rejects_equal_bool_and_float_values(
    field: str,
    drift: object,
) -> None:
    with pytest.raises(ValueError, match="exact integers"):
        replace(_depth_plan(), **{field: drift})


@pytest.mark.parametrize(
    ("field", "drift", "message"),
    (
        ("retryable_status_codes", (429,), "no-retry"),
        ("retryable_error_categories", ("TIMEOUT",), "no-retry"),
        ("retry_backoff_ms", (100,), "no-retry"),
        ("retry_jitter_mode", "FULL", "no-retry"),
        ("snapshot_triggers", ("startup", "sequence_gap"), "bridge policy"),
        ("periodic_cadence_ms", 5_000, "qualification-selected and unset"),
        ("periodic_cadence_policy", "PNL_SELECTED", "qualification-selected and unset"),
        ("cadence_selection_uses_pnl", True, "qualification-selected and unset"),
        ("periodic_cadence_promoting", True, "qualification-selected and unset"),
        ("order_execution_enabled", True, "qualification-only and non-promoting"),
        ("promotion_ready", True, "qualification-only and non-promoting"),
        ("promoting", True, "qualification-only and non-promoting"),
    ),
)
def test_depth_rest_retry_cadence_and_nonpromotion_policy_fail_closed(
    field: str,
    drift: object,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(_depth_plan(), **{field: drift})


def test_v8_requires_exact_four_roles_and_one_symbol_census() -> None:
    plans = build_provisional_promoting_capture_plans_v8(("BTCUSDT", "ETHUSDT"))
    market_plan, public_plan, oi_plan, depth_plan = plans
    assert isinstance(oi_plan, ProvisionalPromotingRestCapturePlanV2)
    assert isinstance(depth_plan, ProvisionalDepthRestQualificationPlanV8)

    with pytest.raises(ValueError, match="exactly two WebSocket, one OI REST"):
        validate_provisional_promoting_capture_plans_v8(plans[:3])
    with pytest.raises(ValueError, match="exactly two WebSocket, one OI REST"):
        validate_provisional_promoting_capture_plans_v8((*plans, depth_plan))

    missing = replace(depth_plan, symbols=("BTCUSDT",))
    extra = replace(depth_plan, symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    with pytest.raises(ValueError, match="symbol censuses must match exactly"):
        validate_provisional_promoting_capture_plans_v8(
            (market_plan, public_plan, oi_plan, missing)
        )
    with pytest.raises(ValueError, match="symbol censuses must match exactly"):
        validate_provisional_promoting_capture_plans_v8(
            (market_plan, public_plan, oi_plan, extra)
        )
    with pytest.raises(ValueError, match="symbols must be unique"):
        replace(depth_plan, symbols=("BTCUSDT", "BTCUSDT"))


def test_v8_hash_is_frozen_and_permutation_stable() -> None:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    plans = build_provisional_promoting_capture_plans_v8(symbols)
    baseline = provisional_promoting_plan_sha256_v8(plans)

    assert re.fullmatch(r"[0-9a-f]{64}", baseline)
    assert baseline == "53407aed8a6a6a8ce382d9c0bc551b3e8f7f6f3f32ebf5f587018009237528f9"
    assert provisional_promoting_plan_sha256_v8(tuple(reversed(plans))) == baseline
    assert provisional_promoting_plan_sha256_v8(
        build_provisional_promoting_capture_plans_v8(tuple(reversed(symbols)))
    ) == baseline
    assert baseline != provisional_promoting_plan_sha256_v2(
        build_provisional_promoting_capture_plans_v2(symbols)
    )

    market_plan, public_plan, oi_plan, depth_plan = plans
    assert isinstance(market_plan, ProvisionalPromotingCapturePlanV2)
    assert isinstance(public_plan, ProvisionalPromotingCapturePlanV2)
    stream_permutation = (
        replace(market_plan, streams=tuple(reversed(market_plan.streams))),
        replace(public_plan, streams=tuple(reversed(public_plan.streams))),
        oi_plan,
        depth_plan,
    )
    assert provisional_promoting_plan_sha256_v8(stream_permutation) == baseline
