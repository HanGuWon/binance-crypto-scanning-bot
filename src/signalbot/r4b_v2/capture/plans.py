from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import Literal, cast

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.models import VenueV2

_SYMBOL_RE = re.compile(r"^[A-Z0-9]+USDT$")
_STREAM_RE = re.compile(r"^(?P<symbol>[a-z0-9]+usdt)@(?P<suffix>[^@]+(?:@[^@]+)?)$")
_PROMOTING_ALLOWED_SUFFIXES_BY_ROUTE = {
    "usdm_market": frozenset({"kline_5m", "aggTrade", "markPrice@1s"}),
    "usdm_public": frozenset({"depth@100ms"}),
}
_PROMOTING_COMBINED_BASE_BY_ROUTE = {
    "usdm_market": "wss://fstream.binance.com/market/stream?streams=",
    "usdm_public": "wss://fstream.binance.com/public/stream?streams=",
}
_PROMOTING_REST_ROUTE_ID = "usdm_public_rest"
_PROMOTING_REST_METHOD = "GET"
_PROMOTING_REST_ENDPOINT = "/fapi/v1/openInterest"
_PROMOTING_REST_REQUEST_FIELDS = ("symbol",)
_PROMOTING_REST_AUTH_MODE = "NONE"
_PROMOTING_REST_BASE_URL = "https://fapi.binance.com"
_PROMOTING_REST_POLL_INTERVAL_MS = 5_000
_PROMOTING_REST_SLOT_ALIGNMENT = "UTC_EPOCH_MULTIPLE"
_PROMOTING_REST_REQUEST_TIMEOUT_MS = 4_000
_PROMOTING_REST_MAXIMUM_BODY_BYTES = 4_096
_PROMOTING_REST_MAXIMUM_CONCURRENCY = 4
_PROMOTING_REST_MAXIMUM_ATTEMPTS = 1
_PROMOTING_REST_RETRYABLE_STATUS_CODES: tuple[int, ...] = ()
_PROMOTING_REST_RETRYABLE_ERROR_CATEGORIES: tuple[str, ...] = ()
_PROMOTING_REST_RETRY_BACKOFF_MS: tuple[int, ...] = ()
_PROMOTING_REST_RETRY_JITTER_MODE = "NONE"
_PROMOTING_REST_MAXIMUM_RETRY_AFTER_MS = 0
_PROMOTING_REST_REQUEST_HEADERS = (
    ("accept", "application/json"),
    ("accept-encoding", "identity"),
    ("user-agent", "binance-signalbot-r4b-v2-capture/1"),
)
_PROMOTING_REST_RESPONSE_HEADER_POLICY = "BINANCE_PUBLIC_MINIMAL_V1"
_PROMOTING_REST_RESPONSE_SCHEMA = "BINANCE_USDM_OPEN_INTEREST_RAW_ATTEMPT_V2"
_PROMOTING_REST_SYMBOL_ORDER = "LEXICOGRAPHIC_ASC"
_PROMOTING_REST_MAXIMUM_SYMBOL_CENSUS = 32
_PROMOTING_REST_MISSED_SLOT_POLICY = "SKIP_NO_BACKFILL"
_PROMOTING_REST_EXHAUSTED_ATTEMPT_POLICY = "RETAIN_AND_CONTINUE_M2_INCOMPLETE"
_DEPTH_REST_ROUTE_ID = "usdm_public_depth_rest"
_DEPTH_REST_METHOD = "GET"
_DEPTH_REST_ENDPOINT = "/fapi/v1/depth"
_DEPTH_REST_REQUEST_FIELDS = ("symbol", "limit")
_DEPTH_REST_FIXED_QUERY = (("limit", "1000"),)
_DEPTH_REST_LIMIT = 1_000
_DEPTH_REST_AUTH_MODE = "NONE"
_DEPTH_REST_BASE_URL = "https://fapi.binance.com"
_DEPTH_REST_REQUEST_WEIGHT = 20
_DEPTH_REST_REQUEST_TIMEOUT_MS = 4_000
_DEPTH_REST_MAXIMUM_BODY_BYTES = 1_048_576
_DEPTH_REST_MAXIMUM_CONCURRENCY = 4
_DEPTH_REST_MAXIMUM_ATTEMPTS = 1
_DEPTH_REST_RETRYABLE_STATUS_CODES: tuple[int, ...] = ()
_DEPTH_REST_RETRYABLE_ERROR_CATEGORIES: tuple[str, ...] = ()
_DEPTH_REST_RETRY_BACKOFF_MS: tuple[int, ...] = ()
_DEPTH_REST_RETRY_JITTER_MODE = "NONE"
_DEPTH_REST_MAXIMUM_RETRY_AFTER_MS = 0
_DEPTH_REST_REQUEST_HEADERS = _PROMOTING_REST_REQUEST_HEADERS
_DEPTH_REST_RESPONSE_HEADER_POLICY = "BINANCE_PUBLIC_MINIMAL_V1"
_DEPTH_REST_RESPONSE_SCHEMA = "BINANCE_USDM_DEPTH_SNAPSHOT_RAW_ATTEMPT_V1"
_DEPTH_REST_SYMBOL_ORDER = "LEXICOGRAPHIC_ASC"
_DEPTH_REST_MAXIMUM_SYMBOL_CENSUS = 32
_DEPTH_REST_SNAPSHOT_TRIGGERS = ("startup", "reconnect", "sequence_gap")
_DEPTH_REST_BRIDGE_MAXIMUM_ATTEMPTS = 3
_DEPTH_REST_BRIDGE_WAIT_TIMEOUT_MS = 2_000
_DEPTH_REST_PERIODIC_CADENCE_POLICY = (
    "UNSET_REQUIRES_INFRASTRUCTURE_QUALIFICATION_NO_PNL"
)
_DEPTH_REST_PURPOSE = "LIQUIDITY_EXECUTION_QUALIFICATION_ONLY"
_VENUE_CLOCK_REST_ROUTE_ID = "usdm_venue_clock_rest"
_VENUE_CLOCK_REST_METHOD = "GET"
_VENUE_CLOCK_REST_ENDPOINT = "/fapi/v1/time"
_VENUE_CLOCK_REST_BASE_URL = "https://fapi.binance.com"
_VENUE_CLOCK_REST_REQUEST_FIELDS: tuple[str, ...] = ()
_VENUE_CLOCK_REST_FIXED_QUERY: tuple[tuple[str, str], ...] = ()
_VENUE_CLOCK_REST_POLL_INTERVAL_MS = 30_000
_VENUE_CLOCK_REST_SLOT_ALIGNMENT = "UTC_EPOCH_MULTIPLE"
_VENUE_CLOCK_REST_REQUEST_TIMEOUT_MS = 2_000
_VENUE_CLOCK_REST_MAXIMUM_BODY_BYTES = 4_096
_VENUE_CLOCK_REST_MAXIMUM_CONCURRENCY = 1
_VENUE_CLOCK_REST_MAXIMUM_ATTEMPTS = 1
_VENUE_CLOCK_REST_REQUEST_HEADERS = _PROMOTING_REST_REQUEST_HEADERS
_VENUE_CLOCK_REST_RESPONSE_HEADER_POLICY = "BINANCE_PUBLIC_MINIMAL_V1"
_VENUE_CLOCK_REST_RESPONSE_SCHEMA = "BINANCE_USDM_VENUE_TIME_RAW_ATTEMPT_V1"
_VENUE_CLOCK_REST_AUTH_MODE = "NONE"
_VENUE_CLOCK_REST_PURPOSE = "INFRASTRUCTURE_CLOCK_EVIDENCE_ONLY"
_VENUE_CLOCK_REST_MAXIMUM_HEADER_RTT_MS = 2_000
_VENUE_CLOCK_REST_MAXIMUM_SAMPLE_AGE_MS = 60_000
_VENUE_CLOCK_REST_MAXIMUM_RESIDUAL_MS = 2
_VENUE_CLOCK_REST_MAXIMUM_RATE_ERROR_PPM = 1_000
_STREAM_CENSUS_DOMAIN = b"R4B_V2_PROMOTING_STREAM_CENSUS\0"
_SPOT_DIAGNOSTIC_ALLOWED_SUFFIXES_BY_ROUTE = {
    "spot_market": frozenset({"kline_5m", "aggTrade", "blockTrade", "depth@100ms"}),
}

type FuturesPromotingFamilyV2 = Literal["A", "B", "C"]
type SpotDiagnosticEstimandV2 = Literal[
    "SPOT_LONG_DIAGNOSTIC",
    "SPOT_EXIT_RISK_INCREMENTAL_UTILITY_DIAGNOSTIC",
    "SPOT_AGGTRADE_FLOW_DIAGNOSTIC",
    "SPOT_BLOCKTRADE_DIAGNOSTIC",
]
type SpotEmpiricalNoAuthRestDiagnosticV2 = Literal["historicalBlockTrades"]

_FUTURES_PROMOTING_FAMILIES: tuple[FuturesPromotingFamilyV2, ...] = ("A", "B", "C")
_SPOT_DIAGNOSTIC_ESTIMANDS: tuple[SpotDiagnosticEstimandV2, ...] = (
    "SPOT_LONG_DIAGNOSTIC",
    "SPOT_EXIT_RISK_INCREMENTAL_UTILITY_DIAGNOSTIC",
    "SPOT_AGGTRADE_FLOW_DIAGNOSTIC",
    "SPOT_BLOCKTRADE_DIAGNOSTIC",
)
_SPOT_EMPIRICAL_NO_AUTH_REST_DIAGNOSTICS: tuple[SpotEmpiricalNoAuthRestDiagnosticV2, ...] = (
    "historicalBlockTrades",
)


@dataclass(frozen=True, slots=True)
class ProvisionalPromotingCapturePlanV2:
    """Disconnected USD-M-only source contract for promoting A/B/C evidence."""

    name: str
    venue: VenueV2
    route_id: Literal["usdm_market", "usdm_public"]
    streams: tuple[str, ...]
    combined_base_url: str
    access_mode: Literal["COMBINED_QUERY"] = "COMBINED_QUERY"
    promoting: Literal[True] = True
    promoting_families: tuple[FuturesPromotingFamilyV2, ...] = _FUTURES_PROMOTING_FAMILIES

    def __post_init__(self) -> None:
        _validate_plan_identity(self.name, self.streams, label="promoting")
        if self.venue is not VenueV2.USDM_FUTURES:
            raise ValueError("only USD-M Futures capture may be promoting")
        if self.route_id not in _PROMOTING_ALLOWED_SUFFIXES_BY_ROUTE:
            raise ValueError("promoting capture requires a public USD-M route ID")
        if self.access_mode != "COMBINED_QUERY":
            raise ValueError("promoting USD-M WebSocket access must be combined query mode")
        if self.combined_base_url != _PROMOTING_COMBINED_BASE_BY_ROUTE[self.route_id]:
            raise ValueError("promoting USD-M WebSocket routed base URL differs")
        if self.promoting is not True:
            raise ValueError("Futures A/B/C capture must be explicitly promoting")
        if self.promoting_families != _FUTURES_PROMOTING_FAMILIES:
            raise ValueError("promoting capture families must be exactly A/B/C")


@dataclass(frozen=True, slots=True)
class ProvisionalPromotingRestCapturePlanV2:
    """Exact hash-bound public USD-M OI acquisition authority."""

    name: str
    venue: VenueV2
    route_id: Literal["usdm_public_rest"]
    method: Literal["GET"]
    endpoint: Literal["/fapi/v1/openInterest"]
    symbols: tuple[str, ...]
    base_url: Literal["https://fapi.binance.com"] = _PROMOTING_REST_BASE_URL
    poll_interval_ms: Literal[5000] = _PROMOTING_REST_POLL_INTERVAL_MS
    slot_alignment: Literal["UTC_EPOCH_MULTIPLE"] = _PROMOTING_REST_SLOT_ALIGNMENT
    request_timeout_ms: Literal[4000] = _PROMOTING_REST_REQUEST_TIMEOUT_MS
    maximum_body_bytes: Literal[4096] = _PROMOTING_REST_MAXIMUM_BODY_BYTES
    maximum_concurrency: Literal[4] = _PROMOTING_REST_MAXIMUM_CONCURRENCY
    maximum_attempts: Literal[1] = _PROMOTING_REST_MAXIMUM_ATTEMPTS
    retryable_status_codes: tuple[int, ...] = _PROMOTING_REST_RETRYABLE_STATUS_CODES
    retryable_error_categories: tuple[str, ...] = _PROMOTING_REST_RETRYABLE_ERROR_CATEGORIES
    retry_backoff_ms: tuple[int, ...] = _PROMOTING_REST_RETRY_BACKOFF_MS
    retry_jitter_mode: Literal["NONE"] = _PROMOTING_REST_RETRY_JITTER_MODE
    maximum_retry_after_ms: Literal[0] = _PROMOTING_REST_MAXIMUM_RETRY_AFTER_MS
    request_headers: tuple[tuple[str, str], ...] = _PROMOTING_REST_REQUEST_HEADERS
    response_header_policy: Literal["BINANCE_PUBLIC_MINIMAL_V1"] = (
        _PROMOTING_REST_RESPONSE_HEADER_POLICY
    )
    response_schema: Literal["BINANCE_USDM_OPEN_INTEREST_RAW_ATTEMPT_V2"] = (
        _PROMOTING_REST_RESPONSE_SCHEMA
    )
    symbol_order: Literal["LEXICOGRAPHIC_ASC"] = _PROMOTING_REST_SYMBOL_ORDER
    maximum_symbol_census: Literal[32] = _PROMOTING_REST_MAXIMUM_SYMBOL_CENSUS
    missed_slot_policy: Literal["SKIP_NO_BACKFILL"] = _PROMOTING_REST_MISSED_SLOT_POLICY
    exhausted_attempt_policy: Literal["RETAIN_AND_CONTINUE_M2_INCOMPLETE"] = (
        _PROMOTING_REST_EXHAUSTED_ATTEMPT_POLICY
    )
    request_fields: tuple[str, ...] = _PROMOTING_REST_REQUEST_FIELDS
    auth_mode: Literal["NONE"] = _PROMOTING_REST_AUTH_MODE
    requires_api_key: Literal[False] = False
    is_private: Literal[False] = False
    promoting: Literal[True] = True
    promoting_families: tuple[FuturesPromotingFamilyV2, ...] = _FUTURES_PROMOTING_FAMILIES

    def __post_init__(self) -> None:
        _validate_promoting_rest_contract(self)


@dataclass(frozen=True, slots=True)
class ProvisionalDepthRestQualificationPlanV8:
    """Hash-bound public USD-M depth snapshot authority, not yet promoting."""

    name: str
    venue: VenueV2
    route_id: Literal["usdm_public_depth_rest"]
    method: Literal["GET"]
    endpoint: Literal["/fapi/v1/depth"]
    symbols: tuple[str, ...]
    base_url: Literal["https://fapi.binance.com"] = _DEPTH_REST_BASE_URL
    request_fields: tuple[str, ...] = _DEPTH_REST_REQUEST_FIELDS
    fixed_query: tuple[tuple[str, str], ...] = _DEPTH_REST_FIXED_QUERY
    maximum_query_limit: Literal[1000] = _DEPTH_REST_LIMIT
    request_weight: Literal[20] = _DEPTH_REST_REQUEST_WEIGHT
    request_timeout_ms: Literal[4000] = _DEPTH_REST_REQUEST_TIMEOUT_MS
    maximum_body_bytes: Literal[1048576] = _DEPTH_REST_MAXIMUM_BODY_BYTES
    maximum_concurrency: Literal[4] = _DEPTH_REST_MAXIMUM_CONCURRENCY
    maximum_attempts: Literal[1] = _DEPTH_REST_MAXIMUM_ATTEMPTS
    retryable_status_codes: tuple[int, ...] = _DEPTH_REST_RETRYABLE_STATUS_CODES
    retryable_error_categories: tuple[str, ...] = _DEPTH_REST_RETRYABLE_ERROR_CATEGORIES
    retry_backoff_ms: tuple[int, ...] = _DEPTH_REST_RETRY_BACKOFF_MS
    retry_jitter_mode: Literal["NONE"] = _DEPTH_REST_RETRY_JITTER_MODE
    maximum_retry_after_ms: Literal[0] = _DEPTH_REST_MAXIMUM_RETRY_AFTER_MS
    request_headers: tuple[tuple[str, str], ...] = _DEPTH_REST_REQUEST_HEADERS
    response_header_policy: Literal["BINANCE_PUBLIC_MINIMAL_V1"] = (
        _DEPTH_REST_RESPONSE_HEADER_POLICY
    )
    response_schema: Literal["BINANCE_USDM_DEPTH_SNAPSHOT_RAW_ATTEMPT_V1"] = (
        _DEPTH_REST_RESPONSE_SCHEMA
    )
    symbol_order: Literal["LEXICOGRAPHIC_ASC"] = _DEPTH_REST_SYMBOL_ORDER
    maximum_symbol_census: Literal[32] = _DEPTH_REST_MAXIMUM_SYMBOL_CENSUS
    snapshot_triggers: tuple[
        Literal["startup"],
        Literal["reconnect"],
        Literal["sequence_gap"],
    ] = _DEPTH_REST_SNAPSHOT_TRIGGERS
    bridge_maximum_attempts: Literal[3] = _DEPTH_REST_BRIDGE_MAXIMUM_ATTEMPTS
    bridge_wait_timeout_ms: Literal[2000] = _DEPTH_REST_BRIDGE_WAIT_TIMEOUT_MS
    periodic_cadence_ms: None = None
    periodic_cadence_policy: Literal[
        "UNSET_REQUIRES_INFRASTRUCTURE_QUALIFICATION_NO_PNL"
    ] = _DEPTH_REST_PERIODIC_CADENCE_POLICY
    cadence_selection_uses_pnl: Literal[False] = False
    periodic_cadence_promoting: Literal[False] = False
    auth_mode: Literal["NONE"] = _DEPTH_REST_AUTH_MODE
    requires_api_key: Literal[False] = False
    is_private: Literal[False] = False
    order_execution_enabled: Literal[False] = False
    purpose: Literal["LIQUIDITY_EXECUTION_QUALIFICATION_ONLY"] = _DEPTH_REST_PURPOSE
    promotion_ready: Literal[False] = False
    promoting: Literal[False] = False

    def __post_init__(self) -> None:
        _validate_depth_rest_qualification_contract_v8(self)


@dataclass(frozen=True, slots=True)
class ProvisionalUsdmVenueClockRestCapturePlanV9:
    """Hash-bound public USD-M venue-time evidence role, never a causal cursor."""

    name: str
    venue: VenueV2
    route_id: Literal["usdm_venue_clock_rest"]
    method: Literal["GET"]
    endpoint: Literal["/fapi/v1/time"]
    base_url: Literal["https://fapi.binance.com"] = _VENUE_CLOCK_REST_BASE_URL
    request_fields: tuple[str, ...] = _VENUE_CLOCK_REST_REQUEST_FIELDS
    fixed_query: tuple[tuple[str, str], ...] = _VENUE_CLOCK_REST_FIXED_QUERY
    poll_interval_ms: Literal[30000] = _VENUE_CLOCK_REST_POLL_INTERVAL_MS
    slot_alignment: Literal["UTC_EPOCH_MULTIPLE"] = _VENUE_CLOCK_REST_SLOT_ALIGNMENT
    request_timeout_ms: Literal[2000] = _VENUE_CLOCK_REST_REQUEST_TIMEOUT_MS
    maximum_body_bytes: Literal[4096] = _VENUE_CLOCK_REST_MAXIMUM_BODY_BYTES
    maximum_concurrency: Literal[1] = _VENUE_CLOCK_REST_MAXIMUM_CONCURRENCY
    maximum_attempts: Literal[1] = _VENUE_CLOCK_REST_MAXIMUM_ATTEMPTS
    retryable_status_codes: tuple[int, ...] = ()
    retryable_error_categories: tuple[str, ...] = ()
    retry_backoff_ms: tuple[int, ...] = ()
    retry_jitter_mode: Literal["NONE"] = "NONE"
    maximum_retry_after_ms: Literal[0] = 0
    request_headers: tuple[tuple[str, str], ...] = _VENUE_CLOCK_REST_REQUEST_HEADERS
    response_header_policy: Literal["BINANCE_PUBLIC_MINIMAL_V1"] = (
        _VENUE_CLOCK_REST_RESPONSE_HEADER_POLICY
    )
    response_schema: Literal["BINANCE_USDM_VENUE_TIME_RAW_ATTEMPT_V1"] = (
        _VENUE_CLOCK_REST_RESPONSE_SCHEMA
    )
    maximum_header_rtt_ms: Literal[2000] = _VENUE_CLOCK_REST_MAXIMUM_HEADER_RTT_MS
    maximum_sample_age_ms: Literal[60000] = _VENUE_CLOCK_REST_MAXIMUM_SAMPLE_AGE_MS
    maximum_wall_monotonic_residual_ms: Literal[2] = (
        _VENUE_CLOCK_REST_MAXIMUM_RESIDUAL_MS
    )
    maximum_rate_error_ppm: Literal[1000] = _VENUE_CLOCK_REST_MAXIMUM_RATE_ERROR_PPM
    auth_mode: Literal["NONE"] = _VENUE_CLOCK_REST_AUTH_MODE
    requires_api_key: Literal[False] = False
    is_private: Literal[False] = False
    order_execution_enabled: Literal[False] = False
    purpose: Literal["INFRASTRUCTURE_CLOCK_EVIDENCE_ONLY"] = _VENUE_CLOCK_REST_PURPOSE
    promoting: Literal[False] = False
    causal_cursor_complete: Literal[False] = False

    def __post_init__(self) -> None:
        _validate_usdm_venue_clock_rest_contract_v9(self)


@dataclass(frozen=True, slots=True)
class ProvisionalSpotDiagnosticCapturePlanV2:
    """Disconnected Spot-only contract whose evidence can never promote."""

    name: str
    venue: VenueV2
    route_id: Literal["spot_market"]
    streams: tuple[str, ...]
    promoting: Literal[False] = False
    diagnostic_estimands: tuple[SpotDiagnosticEstimandV2, ...] = _SPOT_DIAGNOSTIC_ESTIMANDS
    empirical_no_auth_rest_diagnostics: tuple[SpotEmpiricalNoAuthRestDiagnosticV2, ...] = (
        _SPOT_EMPIRICAL_NO_AUTH_REST_DIAGNOSTICS
    )

    def __post_init__(self) -> None:
        _validate_plan_identity(self.name, self.streams, label="Spot diagnostic")
        if self.venue is not VenueV2.SPOT:
            raise ValueError("Spot diagnostic capture requires the Spot venue")
        if self.route_id != "spot_market":
            raise ValueError("Spot diagnostic capture requires the spot_market route ID")
        if self.promoting is not False:
            raise ValueError("Spot capture must remain explicitly non-promoting")
        if self.diagnostic_estimands != _SPOT_DIAGNOSTIC_ESTIMANDS:
            raise ValueError("Spot diagnostic estimands differ from the frozen four roles")
        if self.empirical_no_auth_rest_diagnostics != _SPOT_EMPIRICAL_NO_AUTH_REST_DIAGNOSTICS:
            raise ValueError(
                "Spot REST diagnostics must retain only historicalBlockTrades "
                "with empirical no-auth classification"
            )


type ProvisionalPromotingPlanV2 = (
    ProvisionalPromotingCapturePlanV2 | ProvisionalPromotingRestCapturePlanV2
)
type ProvisionalPromotingPlanV8 = (
    ProvisionalPromotingPlanV2 | ProvisionalDepthRestQualificationPlanV8
)
type ProvisionalPromotingPlanV9 = (
    ProvisionalPromotingPlanV8 | ProvisionalUsdmVenueClockRestCapturePlanV9
)
type ProvisionalStreamCapturePlanV2 = (
    ProvisionalPromotingCapturePlanV2 | ProvisionalSpotDiagnosticCapturePlanV2
)
type ProvisionalCapturePlanV2 = ProvisionalPromotingPlanV2 | ProvisionalSpotDiagnosticCapturePlanV2


def build_provisional_promoting_capture_plans_v2(
    symbols: tuple[str, ...],
) -> tuple[ProvisionalPromotingPlanV2, ...]:
    """Build one atomic USD-M WebSocket plus public OI REST authority bundle."""

    _validate_symbols(symbols)
    canonical_symbols = tuple(sorted(symbols))
    lowered = tuple(symbol.lower() for symbol in canonical_symbols)
    plans = (
        ProvisionalPromotingCapturePlanV2(
            name="v2-usdm-market-promoting-abc",
            venue=VenueV2.USDM_FUTURES,
            route_id="usdm_market",
            combined_base_url=_PROMOTING_COMBINED_BASE_BY_ROUTE["usdm_market"],
            streams=tuple(
                stream
                for symbol in lowered
                for stream in (
                    f"{symbol}@kline_5m",
                    f"{symbol}@aggTrade",
                    f"{symbol}@markPrice@1s",
                )
            ),
        ),
        ProvisionalPromotingCapturePlanV2(
            name="v2-usdm-public-promoting-abc",
            venue=VenueV2.USDM_FUTURES,
            route_id="usdm_public",
            combined_base_url=_PROMOTING_COMBINED_BASE_BY_ROUTE["usdm_public"],
            streams=tuple(f"{symbol}@depth@100ms" for symbol in lowered),
        ),
        ProvisionalPromotingRestCapturePlanV2(
            name="v2-usdm-public-rest-oi-promoting-abc",
            venue=VenueV2.USDM_FUTURES,
            route_id="usdm_public_rest",
            method="GET",
            endpoint="/fapi/v1/openInterest",
            symbols=canonical_symbols,
        ),
    )
    validate_provisional_promoting_capture_plans_v2(plans)
    return plans


def build_provisional_promoting_capture_plans_v8(
    symbols: tuple[str, ...],
) -> tuple[ProvisionalPromotingPlanV8, ...]:
    """Add one non-promoting depth REST qualification role to unchanged v7."""

    v7_plans = build_provisional_promoting_capture_plans_v2(symbols)
    canonical_symbols = tuple(sorted(symbols))
    plans: tuple[ProvisionalPromotingPlanV8, ...] = (
        *v7_plans,
        ProvisionalDepthRestQualificationPlanV8(
            name="v8-usdm-public-rest-depth-liquidity-qualification",
            venue=VenueV2.USDM_FUTURES,
            route_id="usdm_public_depth_rest",
            method="GET",
            endpoint="/fapi/v1/depth",
            symbols=canonical_symbols,
        ),
    )
    validate_provisional_promoting_capture_plans_v8(plans)
    return plans


def build_provisional_promoting_capture_plans_v9(
    symbols: tuple[str, ...],
) -> tuple[ProvisionalPromotingPlanV9, ...]:
    """Add one non-promoting USD-M venue-time role to unchanged v8 authority."""

    v8_plans = build_provisional_promoting_capture_plans_v8(symbols)
    plans: tuple[ProvisionalPromotingPlanV9, ...] = (
        *v8_plans,
        ProvisionalUsdmVenueClockRestCapturePlanV9(
            name="v9-usdm-public-rest-venue-clock",
            venue=VenueV2.USDM_FUTURES,
            route_id="usdm_venue_clock_rest",
            method="GET",
            endpoint="/fapi/v1/time",
        ),
    )
    validate_provisional_promoting_capture_plans_v9(plans)
    return plans


def build_provisional_spot_diagnostic_capture_plans_v2(
    symbols: tuple[str, ...],
) -> tuple[ProvisionalSpotDiagnosticCapturePlanV2, ...]:
    """Build Spot LONG/EXIT_RISK/flow/blockTrade diagnostic classification."""

    _validate_symbols(symbols)
    lowered = tuple(symbol.lower() for symbol in symbols)
    plans = (
        ProvisionalSpotDiagnosticCapturePlanV2(
            name="v2-spot-non-promoting-diagnostics",
            venue=VenueV2.SPOT,
            route_id="spot_market",
            streams=tuple(
                stream
                for symbol in lowered
                for stream in (
                    f"{symbol}@kline_5m",
                    f"{symbol}@aggTrade",
                    f"{symbol}@blockTrade",
                    f"{symbol}@depth@100ms",
                )
            ),
        ),
    )
    validate_provisional_spot_diagnostic_capture_plans_v2(plans)
    return plans


def validate_provisional_promoting_capture_plans_v2(
    plans: Sequence[ProvisionalPromotingPlanV2],
) -> None:
    """Require exactly two public USD-M WS plans and one public OI REST plan."""

    if len(plans) != 3:
        raise ValueError(
            "promoting authority requires exactly two WebSocket plans and one OI REST plan"
        )
    if any(
        not isinstance(
            plan,
            (ProvisionalPromotingCapturePlanV2, ProvisionalPromotingRestCapturePlanV2),
        )
        for plan in plans
    ):
        raise ValueError("promoting validation rejects diagnostic plan types")

    websocket_plans = tuple(
        plan for plan in plans if isinstance(plan, ProvisionalPromotingCapturePlanV2)
    )
    rest_plans = tuple(
        plan for plan in plans if isinstance(plan, ProvisionalPromotingRestCapturePlanV2)
    )
    if len(websocket_plans) != 2 or len(rest_plans) != 1:
        raise ValueError(
            "promoting authority requires exactly two WebSocket plans and one OI REST plan"
        )

    websocket_route_ids = tuple(plan.route_id for plan in websocket_plans)
    if len(set(websocket_route_ids)) != 2 or set(websocket_route_ids) != set(
        _PROMOTING_ALLOWED_SUFFIXES_BY_ROUTE
    ):
        raise ValueError(
            "promoting WebSocket authority requires one usdm_market and one usdm_public plan"
        )

    for plan in plans:
        if plan.venue is not VenueV2.USDM_FUTURES or plan.promoting is not True:
            raise ValueError("only USD-M Futures A/B/C plans may promote")
        if plan.promoting_families != _FUTURES_PROMOTING_FAMILIES:
            raise ValueError("promoting capture families must be exactly A/B/C")
        if isinstance(plan, ProvisionalPromotingRestCapturePlanV2):
            _validate_promoting_rest_contract(plan)

    _validate_stream_contract(
        websocket_plans,
        allowed_suffixes_by_route=_PROMOTING_ALLOWED_SUFFIXES_BY_ROUTE,
        label="promoting USD-M",
    )
    websocket_symbol_censuses = _validate_exact_promoting_websocket_contract(websocket_plans)
    rest_symbol_census = {symbol.casefold() for symbol in rest_plans[0].symbols}
    if any(
        symbol_census != rest_symbol_census for symbol_census in websocket_symbol_censuses.values()
    ):
        raise ValueError("promoting WebSocket and OI REST symbol censuses must match exactly")


def validate_provisional_promoting_capture_plans_v8(
    plans: Sequence[ProvisionalPromotingPlanV8],
) -> None:
    """Require unchanged v7 authority plus one exact depth REST qualification role."""

    if len(plans) != 4:
        raise ValueError(
            "v8 authority requires exactly two WebSocket, one OI REST, and one depth REST plan"
        )
    allowed_types = (
        ProvisionalPromotingCapturePlanV2,
        ProvisionalPromotingRestCapturePlanV2,
        ProvisionalDepthRestQualificationPlanV8,
    )
    if any(type(plan) not in allowed_types for plan in plans):
        raise ValueError("v8 authority rejects non-exact or diagnostic plan types")

    websocket_plans = tuple(
        plan for plan in plans if type(plan) is ProvisionalPromotingCapturePlanV2
    )
    oi_rest_plans = tuple(
        plan for plan in plans if type(plan) is ProvisionalPromotingRestCapturePlanV2
    )
    depth_rest_plans = tuple(
        plan for plan in plans if type(plan) is ProvisionalDepthRestQualificationPlanV8
    )
    if len(websocket_plans) != 2 or len(oi_rest_plans) != 1 or len(depth_rest_plans) != 1:
        raise ValueError(
            "v8 authority requires exactly two WebSocket, one OI REST, and one depth REST plan"
        )

    v7_plans: tuple[ProvisionalPromotingPlanV2, ...] = (
        websocket_plans[0],
        websocket_plans[1],
        oi_rest_plans[0],
    )
    validate_provisional_promoting_capture_plans_v2(v7_plans)
    depth_plan = depth_rest_plans[0]
    _validate_depth_rest_qualification_contract_v8(depth_plan)
    if depth_plan.symbols != oi_rest_plans[0].symbols:
        raise ValueError("v8 WebSocket, OI REST, and depth REST symbol censuses must match exactly")


def validate_provisional_promoting_capture_plans_v9(
    plans: Sequence[ProvisionalPromotingPlanV9],
) -> None:
    """Require unchanged v8 roles plus one exact unauthenticated clock role."""

    if len(plans) != 5:
        raise ValueError("v9 authority requires unchanged v8 roles and one venue-clock REST plan")
    allowed_types = (
        ProvisionalPromotingCapturePlanV2,
        ProvisionalPromotingRestCapturePlanV2,
        ProvisionalDepthRestQualificationPlanV8,
        ProvisionalUsdmVenueClockRestCapturePlanV9,
    )
    if any(type(plan) not in allowed_types for plan in plans):
        raise ValueError("v9 authority rejects non-exact or diagnostic plan types")
    clock_plans = tuple(
        plan
        for plan in plans
        if type(plan) is ProvisionalUsdmVenueClockRestCapturePlanV9
    )
    if len(clock_plans) != 1:
        raise ValueError("v9 authority requires exactly one venue-clock REST plan")
    v8_plans = tuple(
        plan
        for plan in plans
        if type(plan) is not ProvisionalUsdmVenueClockRestCapturePlanV9
    )
    validate_provisional_promoting_capture_plans_v8(v8_plans)  # type: ignore[arg-type]
    _validate_usdm_venue_clock_rest_contract_v9(clock_plans[0])


def validate_provisional_spot_diagnostic_capture_plans_v2(
    plans: Sequence[ProvisionalSpotDiagnosticCapturePlanV2],
) -> None:
    """Fail closed unless Spot sources remain the exact diagnostic contract."""

    if not plans:
        raise ValueError("provisional Spot diagnostic capture plans cannot be empty")
    for plan in plans:
        if not isinstance(plan, ProvisionalSpotDiagnosticCapturePlanV2):
            raise ValueError("Spot diagnostic validation rejects promoting plan types")
        if plan.venue is not VenueV2.SPOT or plan.promoting is not False:
            raise ValueError("Spot capture must remain explicitly non-promoting")
        if plan.route_id not in _SPOT_DIAGNOSTIC_ALLOWED_SUFFIXES_BY_ROUTE:
            raise ValueError("Spot diagnostics require the public spot_market route ID")
        if _spot_diagnostic_contract_differs(plan):
            raise ValueError("Spot diagnostic role contract differs")
    _validate_stream_contract(
        plans,
        allowed_suffixes_by_route=_SPOT_DIAGNOSTIC_ALLOWED_SUFFIXES_BY_ROUTE,
        label="non-promoting Spot diagnostic",
    )


def provisional_promoting_plan_sha256_v2(
    plans: Sequence[ProvisionalPromotingPlanV2],
) -> str:
    """Bind the atomic USD-M WS/REST authority with permutation-stable ordering."""

    validate_provisional_promoting_capture_plans_v2(plans)
    document = {
        "schema_version": ("r4b_v2_provisional_promoting_plan_v7_usdm_combined_ws_rest_oi"),
        "plans": [
            _canonical_promoting_plan_document(plan)
            for plan in sorted(plans, key=lambda value: (value.route_id, value.name))
        ],
    }
    return hashlib.sha256(canonical_json_line(document)).hexdigest()


def provisional_promoting_plan_sha256_v8(
    plans: Sequence[ProvisionalPromotingPlanV8],
) -> str:
    """Bind unchanged v7 roles plus the depth REST qualification authority."""

    validate_provisional_promoting_capture_plans_v8(plans)
    document = {
        "schema_version": (
            "r4b_v2_provisional_promoting_plan_v8_usdm_combined_ws_rest_oi_depth_qualification"
        ),
        "plans": [
            _canonical_promoting_plan_document_v8(plan)
            for plan in sorted(plans, key=lambda value: (value.route_id, value.name))
        ],
    }
    return hashlib.sha256(canonical_json_line(document)).hexdigest()


def provisional_promoting_plan_sha256_v9(
    plans: Sequence[ProvisionalPromotingPlanV9],
) -> str:
    """Bind unchanged v8 roles plus the USD-M venue-time evidence role."""

    validate_provisional_promoting_capture_plans_v9(plans)
    document = {
        "schema_version": (
            "r4b_v2_provisional_promoting_plan_v9_usdm_ws_oi_depth_clock_evidence"
        ),
        "plans": [
            _canonical_promoting_plan_document_v9(plan)
            for plan in sorted(plans, key=lambda value: (value.route_id, value.name))
        ],
    }
    return hashlib.sha256(canonical_json_line(document)).hexdigest()


def provisional_promoting_stream_census_sha256_v2(
    plan: ProvisionalPromotingCapturePlanV2,
) -> str:
    """Hash one exact logical WS stream census with a versioned domain."""

    if not isinstance(plan, ProvisionalPromotingCapturePlanV2):
        raise TypeError("stream census requires a promoting WebSocket plan")
    document = {
        "schema_version": "r4b_v2_promoting_stream_census_v1",
        "plan_id": plan.name,
        "venue": plan.venue.value,
        "route_id": plan.route_id,
        "streams": tuple(sorted(plan.streams)),
    }
    return hashlib.sha256(_STREAM_CENSUS_DOMAIN + canonical_json_line(document)).hexdigest()


def provisional_spot_diagnostic_plan_sha256_v2(
    plans: Sequence[ProvisionalSpotDiagnosticCapturePlanV2],
) -> str:
    """Bind the non-promoting Spot diagnostic contract independently."""

    validate_provisional_spot_diagnostic_capture_plans_v2(plans)
    document = {
        "schema_version": "r4b_v2_provisional_spot_diagnostic_plan_v1",
        "plans": [asdict(plan) for plan in plans],
    }
    return hashlib.sha256(canonical_json_line(document)).hexdigest()


def _spot_diagnostic_contract_differs(
    plan: ProvisionalSpotDiagnosticCapturePlanV2,
) -> bool:
    """Return whether a Spot plan drifted from its frozen non-promoting roles."""

    return (
        plan.diagnostic_estimands != _SPOT_DIAGNOSTIC_ESTIMANDS
        or plan.empirical_no_auth_rest_diagnostics != _SPOT_EMPIRICAL_NO_AUTH_REST_DIAGNOSTICS
    )


def _validate_promoting_rest_contract(
    plan: ProvisionalPromotingRestCapturePlanV2,
) -> None:
    """Fail closed unless the plan is the exact unauthenticated public OI request."""

    _validate_plan_name(plan.name)
    integer_policy_fields = (
        plan.poll_interval_ms,
        plan.request_timeout_ms,
        plan.maximum_body_bytes,
        plan.maximum_concurrency,
        plan.maximum_attempts,
        plan.maximum_retry_after_ms,
        plan.maximum_symbol_census,
    )
    if any(type(value) is not int for value in integer_policy_fields):
        raise ValueError("promoting OI REST integer policy fields require exact integers")
    if plan.venue is not VenueV2.USDM_FUTURES:
        raise ValueError("promoting OI REST capture requires USD-M Futures")
    if plan.route_id != _PROMOTING_REST_ROUTE_ID:
        raise ValueError("promoting OI REST capture requires usdm_public_rest")
    if plan.method != _PROMOTING_REST_METHOD:
        raise ValueError("promoting OI REST method must be exactly GET")
    if plan.endpoint != _PROMOTING_REST_ENDPOINT:
        raise ValueError("promoting OI REST endpoint must be exactly /fapi/v1/openInterest")
    _validate_symbols(plan.symbols)
    if plan.base_url != _PROMOTING_REST_BASE_URL:
        raise ValueError("promoting OI REST base URL differs from the frozen public host")
    if (
        plan.poll_interval_ms != _PROMOTING_REST_POLL_INTERVAL_MS
        or plan.slot_alignment != _PROMOTING_REST_SLOT_ALIGNMENT
    ):
        raise ValueError("promoting OI REST polling schedule differs from the frozen policy")
    if (
        plan.request_timeout_ms != _PROMOTING_REST_REQUEST_TIMEOUT_MS
        or plan.maximum_body_bytes != _PROMOTING_REST_MAXIMUM_BODY_BYTES
        or plan.maximum_concurrency != _PROMOTING_REST_MAXIMUM_CONCURRENCY
    ):
        raise ValueError("promoting OI REST request bounds differ from the frozen policy")
    if (
        plan.maximum_attempts != _PROMOTING_REST_MAXIMUM_ATTEMPTS
        or plan.retryable_status_codes != _PROMOTING_REST_RETRYABLE_STATUS_CODES
        or plan.retryable_error_categories != _PROMOTING_REST_RETRYABLE_ERROR_CATEGORIES
        or plan.retry_backoff_ms != _PROMOTING_REST_RETRY_BACKOFF_MS
        or plan.retry_jitter_mode != _PROMOTING_REST_RETRY_JITTER_MODE
        or plan.maximum_retry_after_ms != _PROMOTING_REST_MAXIMUM_RETRY_AFTER_MS
    ):
        raise ValueError("promoting OI REST retry policy differs from the frozen no-retry policy")
    if plan.request_headers != _PROMOTING_REST_REQUEST_HEADERS:
        raise ValueError("promoting OI REST request headers differ from the frozen public policy")
    if (
        plan.response_header_policy != _PROMOTING_REST_RESPONSE_HEADER_POLICY
        or plan.response_schema != _PROMOTING_REST_RESPONSE_SCHEMA
    ):
        raise ValueError("promoting OI REST response policy differs from the frozen raw schema")
    if (
        plan.symbol_order != _PROMOTING_REST_SYMBOL_ORDER
        or plan.maximum_symbol_census != _PROMOTING_REST_MAXIMUM_SYMBOL_CENSUS
    ):
        raise ValueError("promoting OI REST symbol policy differs from the frozen policy")
    if plan.symbols != tuple(sorted(plan.symbols)):
        raise ValueError("promoting OI REST symbols must be lexicographically sorted")
    if len(plan.symbols) > _PROMOTING_REST_MAXIMUM_SYMBOL_CENSUS:
        raise ValueError("promoting OI REST symbol census exceeds the maximum of 32")
    if (
        plan.missed_slot_policy != _PROMOTING_REST_MISSED_SLOT_POLICY
        or plan.exhausted_attempt_policy != _PROMOTING_REST_EXHAUSTED_ATTEMPT_POLICY
    ):
        raise ValueError("promoting OI REST completion policy differs from the frozen policy")
    if plan.request_fields != _PROMOTING_REST_REQUEST_FIELDS:
        raise ValueError(
            "promoting OI REST request fields must be exactly symbol; private fields are forbidden"
        )
    if (
        plan.auth_mode != _PROMOTING_REST_AUTH_MODE
        or plan.requires_api_key is not False
        or plan.is_private is not False
    ):
        raise ValueError("promoting OI REST capture must be public and unauthenticated")
    if plan.promoting is not True:
        raise ValueError("Futures A/B/C capture must be explicitly promoting")
    if plan.promoting_families != _FUTURES_PROMOTING_FAMILIES:
        raise ValueError("promoting capture families must be exactly A/B/C")


def _validate_depth_rest_qualification_contract_v8(
    plan: ProvisionalDepthRestQualificationPlanV8,
) -> None:
    """Fail closed unless depth authority is exact, public, and non-promoting."""

    _validate_plan_name(plan.name)
    integer_policy_fields = (
        plan.maximum_query_limit,
        plan.request_weight,
        plan.request_timeout_ms,
        plan.maximum_body_bytes,
        plan.maximum_concurrency,
        plan.maximum_attempts,
        plan.maximum_retry_after_ms,
        plan.maximum_symbol_census,
        plan.bridge_maximum_attempts,
        plan.bridge_wait_timeout_ms,
    )
    if any(type(value) is not int for value in integer_policy_fields):
        raise ValueError("depth REST integer policy fields require exact integers")
    if plan.venue is not VenueV2.USDM_FUTURES:
        raise ValueError("depth REST qualification requires USD-M Futures")
    if plan.route_id != _DEPTH_REST_ROUTE_ID:
        raise ValueError("depth REST qualification requires usdm_public_depth_rest")
    if plan.method != _DEPTH_REST_METHOD:
        raise ValueError("depth REST method must be exactly GET")
    if plan.endpoint != _DEPTH_REST_ENDPOINT:
        raise ValueError("depth REST endpoint must be exactly /fapi/v1/depth")
    _validate_symbols(plan.symbols)
    if plan.symbols != tuple(sorted(plan.symbols)):
        raise ValueError("depth REST symbols must be lexicographically sorted")
    if len(plan.symbols) > _DEPTH_REST_MAXIMUM_SYMBOL_CENSUS:
        raise ValueError("depth REST symbol census exceeds the maximum of 32")
    if (
        plan.base_url != _DEPTH_REST_BASE_URL
        or plan.request_fields != _DEPTH_REST_REQUEST_FIELDS
        or plan.fixed_query != _DEPTH_REST_FIXED_QUERY
        or plan.maximum_query_limit != _DEPTH_REST_LIMIT
    ):
        raise ValueError("depth REST request authority differs from symbol plus fixed limit=1000")
    if (
        plan.request_weight != _DEPTH_REST_REQUEST_WEIGHT
        or plan.request_timeout_ms != _DEPTH_REST_REQUEST_TIMEOUT_MS
        or plan.maximum_body_bytes != _DEPTH_REST_MAXIMUM_BODY_BYTES
        or plan.maximum_concurrency != _DEPTH_REST_MAXIMUM_CONCURRENCY
    ):
        raise ValueError("depth REST request bounds differ from the frozen policy")
    if (
        plan.maximum_attempts != _DEPTH_REST_MAXIMUM_ATTEMPTS
        or plan.retryable_status_codes != _DEPTH_REST_RETRYABLE_STATUS_CODES
        or plan.retryable_error_categories != _DEPTH_REST_RETRYABLE_ERROR_CATEGORIES
        or plan.retry_backoff_ms != _DEPTH_REST_RETRY_BACKOFF_MS
        or plan.retry_jitter_mode != _DEPTH_REST_RETRY_JITTER_MODE
        or plan.maximum_retry_after_ms != _DEPTH_REST_MAXIMUM_RETRY_AFTER_MS
    ):
        raise ValueError("depth REST retry policy differs from the frozen no-retry policy")
    if plan.request_headers != _DEPTH_REST_REQUEST_HEADERS:
        raise ValueError("depth REST request headers differ from the frozen public policy")
    if (
        plan.response_header_policy != _DEPTH_REST_RESPONSE_HEADER_POLICY
        or plan.response_schema != _DEPTH_REST_RESPONSE_SCHEMA
    ):
        raise ValueError("depth REST response policy differs from the frozen raw schema")
    if (
        plan.symbol_order != _DEPTH_REST_SYMBOL_ORDER
        or plan.maximum_symbol_census != _DEPTH_REST_MAXIMUM_SYMBOL_CENSUS
    ):
        raise ValueError("depth REST symbol policy differs from the frozen policy")
    if (
        plan.snapshot_triggers != _DEPTH_REST_SNAPSHOT_TRIGGERS
        or plan.bridge_maximum_attempts != _DEPTH_REST_BRIDGE_MAXIMUM_ATTEMPTS
        or plan.bridge_wait_timeout_ms != _DEPTH_REST_BRIDGE_WAIT_TIMEOUT_MS
    ):
        raise ValueError("depth REST bridge policy differs from the frozen bounded policy")
    if (
        plan.periodic_cadence_ms is not None
        or plan.periodic_cadence_policy != _DEPTH_REST_PERIODIC_CADENCE_POLICY
        or plan.cadence_selection_uses_pnl is not False
        or plan.periodic_cadence_promoting is not False
    ):
        raise ValueError("depth REST periodic cadence must remain qualification-selected and unset")
    if (
        plan.auth_mode != _DEPTH_REST_AUTH_MODE
        or plan.requires_api_key is not False
        or plan.is_private is not False
    ):
        raise ValueError("depth REST qualification must be public and unauthenticated")
    if (
        plan.order_execution_enabled is not False
        or plan.purpose != _DEPTH_REST_PURPOSE
        or plan.promotion_ready is not False
        or plan.promoting is not False
    ):
        raise ValueError("depth REST authority must remain qualification-only and non-promoting")


def _validate_usdm_venue_clock_rest_contract_v9(
    plan: ProvisionalUsdmVenueClockRestCapturePlanV9,
) -> None:
    """Fail closed unless the clock role is exact, public, and non-promoting."""

    _validate_plan_name(plan.name)
    integers = (
        plan.poll_interval_ms,
        plan.request_timeout_ms,
        plan.maximum_body_bytes,
        plan.maximum_concurrency,
        plan.maximum_attempts,
        plan.maximum_retry_after_ms,
        plan.maximum_header_rtt_ms,
        plan.maximum_sample_age_ms,
        plan.maximum_wall_monotonic_residual_ms,
        plan.maximum_rate_error_ppm,
    )
    if any(type(value) is not int for value in integers):
        raise ValueError("venue-clock REST integer policy fields require exact integers")
    if (
        plan.venue is not VenueV2.USDM_FUTURES
        or plan.route_id != _VENUE_CLOCK_REST_ROUTE_ID
        or plan.method != _VENUE_CLOCK_REST_METHOD
        or plan.endpoint != _VENUE_CLOCK_REST_ENDPOINT
        or plan.base_url != _VENUE_CLOCK_REST_BASE_URL
    ):
        raise ValueError("venue-clock REST route must be exact public USD-M /fapi/v1/time")
    if plan.request_fields != () or plan.fixed_query != ():
        raise ValueError("venue-clock REST request must have no query or private fields")
    if (
        plan.poll_interval_ms != _VENUE_CLOCK_REST_POLL_INTERVAL_MS
        or plan.slot_alignment != _VENUE_CLOCK_REST_SLOT_ALIGNMENT
        or plan.request_timeout_ms != _VENUE_CLOCK_REST_REQUEST_TIMEOUT_MS
        or plan.maximum_body_bytes != _VENUE_CLOCK_REST_MAXIMUM_BODY_BYTES
        or plan.maximum_concurrency != _VENUE_CLOCK_REST_MAXIMUM_CONCURRENCY
        or plan.maximum_attempts != _VENUE_CLOCK_REST_MAXIMUM_ATTEMPTS
    ):
        raise ValueError("venue-clock REST schedule or request bounds differ")
    if (
        plan.retryable_status_codes != ()
        or plan.retryable_error_categories != ()
        or plan.retry_backoff_ms != ()
        or plan.retry_jitter_mode != "NONE"
        or plan.maximum_retry_after_ms != 0
    ):
        raise ValueError("venue-clock REST must retain the frozen single no-retry attempt")
    if (
        plan.request_headers != _VENUE_CLOCK_REST_REQUEST_HEADERS
        or plan.response_header_policy != _VENUE_CLOCK_REST_RESPONSE_HEADER_POLICY
        or plan.response_schema != _VENUE_CLOCK_REST_RESPONSE_SCHEMA
    ):
        raise ValueError("venue-clock REST raw request or response policy differs")
    if (
        plan.maximum_header_rtt_ms != _VENUE_CLOCK_REST_MAXIMUM_HEADER_RTT_MS
        or plan.maximum_sample_age_ms != _VENUE_CLOCK_REST_MAXIMUM_SAMPLE_AGE_MS
        or plan.maximum_wall_monotonic_residual_ms
        != _VENUE_CLOCK_REST_MAXIMUM_RESIDUAL_MS
        or plan.maximum_rate_error_ppm != _VENUE_CLOCK_REST_MAXIMUM_RATE_ERROR_PPM
    ):
        raise ValueError("venue-clock REST health bounds differ from the frozen policy")
    if (
        plan.auth_mode != _VENUE_CLOCK_REST_AUTH_MODE
        or plan.requires_api_key is not False
        or plan.is_private is not False
    ):
        raise ValueError("venue-clock REST must be public and unauthenticated")
    if (
        plan.order_execution_enabled is not False
        or plan.purpose != _VENUE_CLOCK_REST_PURPOSE
        or plan.promoting is not False
        or plan.causal_cursor_complete is not False
    ):
        raise ValueError("venue-clock REST is infrastructure-only and not a causal cursor")


def _validate_exact_promoting_websocket_contract(
    plans: Sequence[ProvisionalPromotingCapturePlanV2],
) -> dict[str, set[str]]:
    """Require every symbol to have every allowlisted stream for its WS route."""

    symbols_by_route: dict[str, set[str]] = {}
    for plan in plans:
        suffixes_by_symbol: dict[str, set[str]] = defaultdict(set)
        for stream in plan.streams:
            match = _STREAM_RE.fullmatch(stream)
            if match is None:
                raise ValueError(f"non-normalized V2 stream: {stream!r}")
            suffixes_by_symbol[match.group("symbol")].add(match.group("suffix"))
        expected_suffixes = _PROMOTING_ALLOWED_SUFFIXES_BY_ROUTE[plan.route_id]
        if any(
            observed_suffixes != expected_suffixes
            for observed_suffixes in suffixes_by_symbol.values()
        ):
            raise ValueError(
                f"every {plan.route_id} symbol requires its exact promoting stream set"
            )
        symbols_by_route[plan.route_id] = set(suffixes_by_symbol)
    return symbols_by_route


def _canonical_promoting_plan_document(
    plan: ProvisionalPromotingPlanV2,
) -> dict[str, object]:
    """Canonicalize plan and symbol/stream permutations before hashing."""

    document: dict[str, object] = asdict(plan)
    if isinstance(plan, ProvisionalPromotingCapturePlanV2):
        document["streams"] = tuple(sorted(plan.streams))
    else:
        document["symbols"] = tuple(sorted(plan.symbols))
    return document


def _canonical_promoting_plan_document_v8(
    plan: ProvisionalPromotingPlanV8,
) -> dict[str, object]:
    """Canonicalize the additive v8 authority without changing v7 hashing."""

    if isinstance(plan, ProvisionalDepthRestQualificationPlanV8):
        document: dict[str, object] = asdict(plan)
        document["symbols"] = tuple(sorted(plan.symbols))
        return document
    return _canonical_promoting_plan_document(plan)


def _canonical_promoting_plan_document_v9(
    plan: ProvisionalPromotingPlanV9,
) -> dict[str, object]:
    """Canonicalize additive v9 authority without changing earlier hashes."""

    if type(plan) is ProvisionalUsdmVenueClockRestCapturePlanV9:
        return asdict(plan)
    return _canonical_promoting_plan_document_v8(
        cast(ProvisionalPromotingPlanV8, plan)
    )


def _validate_stream_contract(
    plans: Sequence[ProvisionalStreamCapturePlanV2],
    *,
    allowed_suffixes_by_route: dict[str, frozenset[str]],
    label: str,
) -> None:
    seen_streams: set[tuple[VenueV2, str]] = set()
    depth_symbols: dict[VenueV2, set[str]] = defaultdict(set)
    all_symbols: dict[VenueV2, set[str]] = defaultdict(set)
    for plan in plans:
        for stream in plan.streams:
            match = _STREAM_RE.fullmatch(stream)
            if match is None:
                raise ValueError(f"non-normalized V2 stream: {stream!r}")
            suffix = match.group("suffix")
            lowered_suffix = suffix.casefold()
            if lowered_suffix == "bookticker":
                raise ValueError("bookTicker is forbidden in every provisional V2 plan")
            if suffix not in allowed_suffixes_by_route[plan.route_id]:
                raise ValueError(f"stream suffix is not allowlisted for {label}")
            if any(
                token in stream.casefold()
                for token in ("listenkey", "userdata", "account", "@order")
            ):
                raise ValueError("private or order streams are forbidden")
            key = (plan.venue, stream)
            if key in seen_streams:
                raise ValueError(f"{label} plans contain a duplicate venue stream")
            seen_streams.add(key)
            symbol = match.group("symbol")
            all_symbols[plan.venue].add(symbol)
            if lowered_suffix == "depth@100ms":
                depth_symbols[plan.venue].add(symbol)
    for venue, symbols in all_symbols.items():
        missing = symbols - depth_symbols[venue]
        if missing:
            raise ValueError(f"every {venue.value} {label} symbol requires standard depth@100ms")


def _validate_plan_identity(name: str, streams: tuple[str, ...], *, label: str) -> None:
    _validate_plan_name(name)
    if type(streams) is not tuple or not streams:
        raise ValueError(f"provisional {label} plan requires streams")
    if any(not isinstance(stream, str) for stream in streams):
        raise ValueError(f"provisional {label} streams must be strings")


def _validate_plan_name(name: str) -> None:
    if not isinstance(name, str) or not name or name.strip() != name or len(name) > 256:
        raise ValueError("plan name must be normalized")


def _validate_symbols(symbols: tuple[str, ...]) -> None:
    if type(symbols) is not tuple or not symbols:
        raise ValueError("provisional V2 capture requires at least one symbol")
    if len(set(symbols)) != len(symbols):
        raise ValueError("provisional V2 capture symbols must be unique")
    if any(_SYMBOL_RE.fullmatch(symbol) is None for symbol in symbols):
        raise ValueError("provisional V2 symbols must be normalized USDT symbols")
