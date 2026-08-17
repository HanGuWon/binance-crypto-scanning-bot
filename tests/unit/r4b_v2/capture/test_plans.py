from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import asdict, replace

import pytest

from signalbot.capture.plans import build_prospective_capture_plans
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.capture.plans import (
    ProvisionalPromotingCapturePlanV2,
    ProvisionalPromotingRestCapturePlanV2,
    ProvisionalSpotDiagnosticCapturePlanV2,
    build_provisional_promoting_capture_plans_v2,
    build_provisional_spot_diagnostic_capture_plans_v2,
    provisional_promoting_plan_sha256_v2,
    provisional_promoting_stream_census_sha256_v2,
    provisional_spot_diagnostic_plan_sha256_v2,
    validate_provisional_promoting_capture_plans_v2,
    validate_provisional_spot_diagnostic_capture_plans_v2,
)

_REST_ACQUISITION_POLICY_CASES: tuple[tuple[str, object, object], ...] = (
    ("base_url", "https://fapi.binance.com", "https://fapi.binance.com/"),
    ("poll_interval_ms", 5_000, 4_999),
    ("slot_alignment", "UTC_EPOCH_MULTIPLE", "UTC_EPOCH_OFFSET"),
    ("request_timeout_ms", 4_000, 3_999),
    ("maximum_body_bytes", 4_096, 4_095),
    ("maximum_concurrency", 4, 3),
    ("maximum_attempts", 1, 0),
    ("retryable_status_codes", (), (429,)),
    ("retryable_error_categories", (), ("TIMEOUT",)),
    ("retry_backoff_ms", (), (1,)),
    ("retry_jitter_mode", "NONE", "FULL"),
    ("maximum_retry_after_ms", 0, 1),
    (
        "request_headers",
        (
            ("accept", "application/json"),
            ("accept-encoding", "identity"),
            ("user-agent", "binance-signalbot-r4b-v2-capture/1"),
        ),
        (("accept", "application/json"),),
    ),
    ("response_header_policy", "BINANCE_PUBLIC_MINIMAL_V1", "BINANCE_PUBLIC_MINIMAL_V2"),
    (
        "response_schema",
        "BINANCE_USDM_OPEN_INTEREST_RAW_ATTEMPT_V2",
        "BINANCE_USDM_OPEN_INTEREST_RAW_ATTEMPT_V1",
    ),
    ("symbol_order", "LEXICOGRAPHIC_ASC", "INPUT_ORDER"),
    ("maximum_symbol_census", 32, 31),
    ("missed_slot_policy", "SKIP_NO_BACKFILL", "BACKFILL"),
    (
        "exhausted_attempt_policy",
        "RETAIN_AND_CONTINUE_M2_INCOMPLETE",
        "FAIL_SESSION",
    ),
)


def _hash_promoting_policy_document(
    plans: Sequence[ProvisionalPromotingCapturePlanV2 | ProvisionalPromotingRestCapturePlanV2],
    *,
    rest_override_field: str | None = None,
    rest_override_value: object = None,
) -> str:
    """Rebuild the public canonical contract to prove each REST field is hash-bound."""

    documents: list[dict[str, object]] = []
    for plan in sorted(plans, key=lambda value: (value.route_id, value.name)):
        document: dict[str, object] = asdict(plan)
        if isinstance(plan, ProvisionalPromotingCapturePlanV2):
            document["streams"] = tuple(sorted(plan.streams))
        else:
            document["symbols"] = tuple(sorted(plan.symbols))
            if rest_override_field is not None:
                document[rest_override_field] = rest_override_value
        documents.append(document)
    payload = {
        "schema_version": "r4b_v2_provisional_promoting_plan_v7_usdm_combined_ws_rest_oi",
        "plans": documents,
    }
    return hashlib.sha256(canonical_json_line(payload)).hexdigest()


def test_provisional_promoting_plan_removes_book_ticker_and_binds_provenance() -> None:
    plans = build_provisional_promoting_capture_plans_v2(("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    websocket_plans = tuple(
        plan for plan in plans if isinstance(plan, ProvisionalPromotingCapturePlanV2)
    )
    [rest_plan] = [
        plan for plan in plans if isinstance(plan, ProvisionalPromotingRestCapturePlanV2)
    ]

    assert len(plans) == 3
    assert [len(plan.streams) for plan in websocket_plans] == [9, 3]
    assert sum(len(plan.streams) for plan in websocket_plans) == 12
    assert all(plan.venue is VenueV2.USDM_FUTURES for plan in plans)
    assert all(plan.promoting for plan in plans)
    assert all(plan.access_mode == "COMBINED_QUERY" for plan in websocket_plans)
    assert {plan.combined_base_url for plan in websocket_plans} == {
        "wss://fstream.binance.com/market/stream?streams=",
        "wss://fstream.binance.com/public/stream?streams=",
    }
    assert all(plan.promoting_families == ("A", "B", "C") for plan in plans)
    assert all(
        "bookticker" not in stream.casefold() for plan in websocket_plans for stream in plan.streams
    )
    assert {
        (plan.venue, stream.split("@", 1)[0])
        for plan in websocket_plans
        for stream in plan.streams
        if stream.endswith("@depth@100ms")
    } == {(VenueV2.USDM_FUTURES, symbol.lower()) for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT")}
    assert rest_plan.route_id == "usdm_public_rest"
    assert rest_plan.method == "GET"
    assert rest_plan.endpoint == "/fapi/v1/openInterest"
    assert rest_plan.symbols == ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    assert rest_plan.base_url == "https://fapi.binance.com"
    assert rest_plan.poll_interval_ms == 5_000
    assert rest_plan.slot_alignment == "UTC_EPOCH_MULTIPLE"
    assert rest_plan.request_timeout_ms == 4_000
    assert rest_plan.maximum_body_bytes == 4_096
    assert rest_plan.maximum_concurrency == 4
    assert rest_plan.maximum_attempts == 1
    assert rest_plan.retryable_status_codes == ()
    assert rest_plan.retryable_error_categories == ()
    assert rest_plan.retry_backoff_ms == ()
    assert rest_plan.retry_jitter_mode == "NONE"
    assert rest_plan.maximum_retry_after_ms == 0
    assert rest_plan.request_headers == (
        ("accept", "application/json"),
        ("accept-encoding", "identity"),
        ("user-agent", "binance-signalbot-r4b-v2-capture/1"),
    )
    assert rest_plan.response_header_policy == "BINANCE_PUBLIC_MINIMAL_V1"
    assert rest_plan.response_schema == "BINANCE_USDM_OPEN_INTEREST_RAW_ATTEMPT_V2"
    assert rest_plan.symbol_order == "LEXICOGRAPHIC_ASC"
    assert rest_plan.maximum_symbol_census == 32
    assert rest_plan.missed_slot_policy == "SKIP_NO_BACKFILL"
    assert rest_plan.exhausted_attempt_policy == "RETAIN_AND_CONTINUE_M2_INCOMPLETE"
    assert rest_plan.request_fields == ("symbol",)
    assert rest_plan.auth_mode == "NONE"
    assert rest_plan.requires_api_key is False
    assert rest_plan.is_private is False
    first = provisional_promoting_plan_sha256_v2(plans)
    second = provisional_promoting_plan_sha256_v2(
        build_provisional_promoting_capture_plans_v2(("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    )
    assert first == second
    assert re.fullmatch(r"[0-9a-f]{64}", first)
    assert all(not hasattr(plan, "url") for plan in plans)


def test_book_ticker_drift_and_missing_depth_fail_closed() -> None:
    book_ticker = ProvisionalSpotDiagnosticCapturePlanV2(
        name="bad-spot-diagnostic",
        venue=VenueV2.SPOT,
        route_id="spot_market",
        streams=("btcusdt@bookTicker", "btcusdt@depth@100ms"),
    )
    with pytest.raises(ValueError, match="bookTicker"):
        validate_provisional_spot_diagnostic_capture_plans_v2((book_ticker,))

    market_plan, _, rest_plan = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
    assert isinstance(market_plan, ProvisionalPromotingCapturePlanV2)
    assert isinstance(rest_plan, ProvisionalPromotingRestCapturePlanV2)
    wrong_depth_census = ProvisionalPromotingCapturePlanV2(
        name="bad-futures-depth",
        venue=VenueV2.USDM_FUTURES,
        route_id="usdm_public",
        streams=("ethusdt@depth@100ms",),
        combined_base_url="wss://fstream.binance.com/public/stream?streams=",
    )
    with pytest.raises(ValueError, match="requires standard depth"):
        validate_provisional_promoting_capture_plans_v2(
            (market_plan, wrong_depth_census, rest_plan)
        )

    nonallowlisted = ProvisionalSpotDiagnosticCapturePlanV2(
        name="bad-extra-stream",
        venue=VenueV2.SPOT,
        route_id="spot_market",
        streams=("btcusdt@depth@100ms", "btcusdt@ticker"),
    )
    with pytest.raises(ValueError, match="not allowlisted"):
        validate_provisional_spot_diagnostic_capture_plans_v2((nonallowlisted,))


def test_spot_capture_is_separate_non_promoting_diagnostic_contract() -> None:
    plans = build_provisional_spot_diagnostic_capture_plans_v2(("BTCUSDT", "ETHUSDT", "SOLUSDT"))

    assert len(plans) == 1
    [plan] = plans
    assert plan.venue is VenueV2.SPOT
    assert not plan.promoting
    assert len(plan.streams) == 12
    assert sum(stream.endswith("@blockTrade") for stream in plan.streams) == 3
    assert all("bookticker" not in stream.casefold() for stream in plan.streams)
    assert plan.diagnostic_estimands == (
        "SPOT_LONG_DIAGNOSTIC",
        "SPOT_EXIT_RISK_INCREMENTAL_UTILITY_DIAGNOSTIC",
        "SPOT_AGGTRADE_FLOW_DIAGNOSTIC",
        "SPOT_BLOCKTRADE_DIAGNOSTIC",
    )
    assert plan.empirical_no_auth_rest_diagnostics == ("historicalBlockTrades",)
    first = provisional_spot_diagnostic_plan_sha256_v2(plans)
    second = provisional_spot_diagnostic_plan_sha256_v2(
        build_provisional_spot_diagnostic_capture_plans_v2(("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    )
    assert first == second
    assert first != provisional_promoting_plan_sha256_v2(
        build_provisional_promoting_capture_plans_v2(("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    )


def test_cross_venue_promotion_and_diagnostic_role_drift_fail_closed() -> None:
    with pytest.raises(ValueError, match="only USD-M Futures"):
        ProvisionalPromotingCapturePlanV2(
            name="spot-cannot-promote",
            venue=VenueV2.SPOT,  # type: ignore[arg-type]
            route_id="usdm_public",
            streams=("btcusdt@depth@100ms",),
            combined_base_url="wss://fstream.binance.com/public/stream?streams=",
        )

    rest_plan = next(
        plan
        for plan in build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
        if isinstance(plan, ProvisionalPromotingRestCapturePlanV2)
    )
    with pytest.raises(ValueError, match="USD-M Futures"):
        replace(rest_plan, venue=VenueV2.SPOT)

    with pytest.raises(ValueError, match="Spot venue"):
        ProvisionalSpotDiagnosticCapturePlanV2(
            name="futures-cannot-be-spot-diagnostic",
            venue=VenueV2.USDM_FUTURES,
            route_id="spot_market",
            streams=("btcusdt@depth@100ms",),
        )

    with pytest.raises(ValueError, match="frozen four roles"):
        ProvisionalSpotDiagnosticCapturePlanV2(
            name="missing-diagnostic-role",
            venue=VenueV2.SPOT,
            route_id="spot_market",
            streams=("btcusdt@depth@100ms",),
            diagnostic_estimands=("SPOT_LONG_DIAGNOSTIC",),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "symbols",
    [(), ("BTCUSDT", "BTCUSDT"), ("btcusdt",), ("BTCUSD",)],
)
def test_symbol_contract_rejects_empty_duplicates_and_non_usdt(
    symbols: tuple[str, ...],
) -> None:
    with pytest.raises(ValueError):
        build_provisional_promoting_capture_plans_v2(symbols)


def test_historical_v1_plan_contract_remains_unmodified_and_quarantined() -> None:
    legacy = build_prospective_capture_plans(("BTCUSDT",), batch_size=25)
    assert any("bookTicker" in stream for plan in legacy for stream in plan.streams)
    v2 = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
    assert all(
        "bookTicker" not in stream
        for plan in v2
        if isinstance(plan, ProvisionalPromotingCapturePlanV2)
        for stream in plan.streams
    )
    spot_diagnostic = build_provisional_spot_diagnostic_capture_plans_v2(("BTCUSDT",))
    assert all(not plan.promoting for plan in spot_diagnostic)


def test_promoting_bundle_requires_exactly_two_websocket_and_one_oi_rest_plan() -> None:
    plans = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
    market_plan, depth_plan, rest_plan = plans
    assert isinstance(market_plan, ProvisionalPromotingCapturePlanV2)
    assert isinstance(depth_plan, ProvisionalPromotingCapturePlanV2)
    assert isinstance(rest_plan, ProvisionalPromotingRestCapturePlanV2)
    assert provisional_promoting_plan_sha256_v2(plans) == (
        "2dda970f7771ac7c72f7284bfa302631dfef5490b3061861a7c8908649d370b2"
    )

    with pytest.raises(ValueError, match="exactly two WebSocket plans and one OI REST"):
        validate_provisional_promoting_capture_plans_v2((market_plan, depth_plan))
    with pytest.raises(ValueError, match="exactly two WebSocket plans and one OI REST"):
        validate_provisional_promoting_capture_plans_v2((*plans, rest_plan))
    with pytest.raises(ValueError, match="one usdm_market and one usdm_public"):
        validate_provisional_promoting_capture_plans_v2(
            (market_plan, replace(market_plan, name="duplicate-market-role"), rest_plan)
        )


def test_oi_rest_contract_rejects_method_endpoint_route_private_fields_and_auth() -> None:
    rest_plan = next(
        plan
        for plan in build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
        if isinstance(plan, ProvisionalPromotingRestCapturePlanV2)
    )

    with pytest.raises(ValueError, match="exactly GET"):
        replace(rest_plan, method="POST")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="exactly /fapi/v1/openInterest"):
        replace(rest_plan, endpoint="/fapi/v1/openInterest/private")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="requires usdm_public_rest"):
        replace(rest_plan, route_id="usdm_public")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="private fields are forbidden"):
        replace(rest_plan, request_fields=("symbol", "signature"))
    with pytest.raises(ValueError, match="public and unauthenticated"):
        replace(rest_plan, auth_mode="API_KEY")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="public and unauthenticated"):
        replace(rest_plan, requires_api_key=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="public and unauthenticated"):
        replace(rest_plan, is_private=True)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("poll_interval_ms", 5000.0),
        ("request_timeout_ms", 4000.0),
        ("maximum_body_bytes", 4096.0),
        ("maximum_concurrency", 4.0),
        ("maximum_attempts", True),
        ("maximum_retry_after_ms", False),
        ("maximum_symbol_census", 32.0),
    ],
)
def test_oi_rest_integer_policy_rejects_equal_bool_and_float_values(
    field: str,
    value: object,
) -> None:
    rest_plan = next(
        plan
        for plan in build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
        if isinstance(plan, ProvisionalPromotingRestCapturePlanV2)
    )

    with pytest.raises(ValueError, match="exact integers"):
        replace(rest_plan, **{field: value})


@pytest.mark.parametrize(
    ("field_name", "expected_value", "drifted_value"),
    _REST_ACQUISITION_POLICY_CASES,
)
def test_oi_rest_acquisition_policy_defaults_and_nearest_boundary_drift(
    field_name: str,
    expected_value: object,
    drifted_value: object,
) -> None:
    rest_plan = next(
        plan
        for plan in build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
        if isinstance(plan, ProvisionalPromotingRestCapturePlanV2)
    )

    assert getattr(rest_plan, field_name) == expected_value
    with pytest.raises(ValueError, match="frozen"):
        replace(rest_plan, **{field_name: drifted_value})


@pytest.mark.parametrize(
    ("field_name", "upper_boundary_drift"),
    (
        ("poll_interval_ms", 5_001),
        ("request_timeout_ms", 4_001),
        ("maximum_body_bytes", 4_097),
        ("maximum_concurrency", 5),
        ("maximum_attempts", 2),
        ("maximum_retry_after_ms", -1),
        ("maximum_symbol_census", 33),
    ),
)
def test_oi_rest_numeric_policy_rejects_opposite_boundary_drift(
    field_name: str,
    upper_boundary_drift: int,
) -> None:
    rest_plan = next(
        plan
        for plan in build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
        if isinstance(plan, ProvisionalPromotingRestCapturePlanV2)
    )

    with pytest.raises(ValueError, match="frozen"):
        replace(rest_plan, **{field_name: upper_boundary_drift})


@pytest.mark.parametrize(
    ("field_name", "_expected_value", "drifted_value"),
    _REST_ACQUISITION_POLICY_CASES,
)
def test_every_oi_rest_acquisition_policy_field_changes_the_v6_hash_document(
    field_name: str,
    _expected_value: object,
    drifted_value: object,
) -> None:
    plans = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
    baseline = provisional_promoting_plan_sha256_v2(plans)

    assert _hash_promoting_policy_document(plans) == baseline
    assert (
        _hash_promoting_policy_document(
            plans,
            rest_override_field=field_name,
            rest_override_value=drifted_value,
        )
        != baseline
    )


def test_oi_rest_symbol_order_and_maximum_census_boundaries() -> None:
    maximum_symbols = tuple(f"S{index:02d}USDT" for index in range(32))
    plans = build_provisional_promoting_capture_plans_v2(tuple(reversed(maximum_symbols)))
    rest_plan = next(
        plan for plan in plans if isinstance(plan, ProvisionalPromotingRestCapturePlanV2)
    )
    assert rest_plan.symbols == maximum_symbols
    assert len(rest_plan.symbols) == rest_plan.maximum_symbol_census

    with pytest.raises(ValueError, match="lexicographically sorted"):
        replace(rest_plan, symbols=tuple(reversed(rest_plan.symbols)))

    too_many_symbols = tuple(f"S{index:02d}USDT" for index in range(33))
    with pytest.raises(ValueError, match="maximum of 32"):
        build_provisional_promoting_capture_plans_v2(too_many_symbols)


def test_promoting_ws_contract_seals_combined_mode_and_current_routed_paths() -> None:
    market_plan, public_plan, _ = build_provisional_promoting_capture_plans_v2(("BTCUSDT",))
    assert isinstance(market_plan, ProvisionalPromotingCapturePlanV2)
    assert isinstance(public_plan, ProvisionalPromotingCapturePlanV2)

    with pytest.raises(ValueError, match="combined query mode"):
        replace(market_plan, access_mode="RAW")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="routed base URL differs"):
        replace(
            market_plan,
            combined_base_url="wss://fstream.binance.com/stream?streams=",
        )
    with pytest.raises(ValueError, match="routed base URL differs"):
        replace(
            public_plan,
            combined_base_url="wss://fstream.binance.com/ws/",
        )


def test_oi_rest_symbol_census_missing_extra_and_duplicates_fail_closed() -> None:
    plans = build_provisional_promoting_capture_plans_v2(("BTCUSDT", "ETHUSDT"))
    market_plan, depth_plan, rest_plan = plans
    assert isinstance(market_plan, ProvisionalPromotingCapturePlanV2)
    assert isinstance(depth_plan, ProvisionalPromotingCapturePlanV2)
    assert isinstance(rest_plan, ProvisionalPromotingRestCapturePlanV2)

    missing = replace(rest_plan, symbols=("BTCUSDT",))
    extra = replace(rest_plan, symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"))
    with pytest.raises(ValueError, match="symbol censuses must match exactly"):
        validate_provisional_promoting_capture_plans_v2((market_plan, depth_plan, missing))
    with pytest.raises(ValueError, match="symbol censuses must match exactly"):
        validate_provisional_promoting_capture_plans_v2((market_plan, depth_plan, extra))
    with pytest.raises(ValueError, match="symbols must be unique"):
        replace(rest_plan, symbols=("BTCUSDT", "BTCUSDT"))


def test_promoting_hash_canonicalizes_symbol_stream_and_plan_permutations() -> None:
    symbols = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
    plans = build_provisional_promoting_capture_plans_v2(symbols)
    symbol_permutation = build_provisional_promoting_capture_plans_v2(tuple(reversed(symbols)))
    assert plans == symbol_permutation

    permuted_plans = tuple(reversed(plans))
    assert provisional_promoting_plan_sha256_v2(permuted_plans) == (
        provisional_promoting_plan_sha256_v2(plans)
    )

    market_plan, depth_plan, rest_plan = plans
    assert isinstance(market_plan, ProvisionalPromotingCapturePlanV2)
    assert isinstance(depth_plan, ProvisionalPromotingCapturePlanV2)
    assert isinstance(rest_plan, ProvisionalPromotingRestCapturePlanV2)
    stream_permutation = (
        replace(market_plan, streams=tuple(reversed(market_plan.streams))),
        replace(depth_plan, streams=tuple(reversed(depth_plan.streams))),
        rest_plan,
    )
    assert provisional_promoting_plan_sha256_v2(stream_permutation) == (
        provisional_promoting_plan_sha256_v2(plans)
    )
    assert provisional_promoting_stream_census_sha256_v2(market_plan) == (
        provisional_promoting_stream_census_sha256_v2(stream_permutation[0])
    )
    assert provisional_promoting_stream_census_sha256_v2(market_plan) != (
        provisional_promoting_stream_census_sha256_v2(depth_plan)
    )
    with pytest.raises(TypeError, match="WebSocket plan"):
        provisional_promoting_stream_census_sha256_v2(rest_plan)  # type: ignore[arg-type]
