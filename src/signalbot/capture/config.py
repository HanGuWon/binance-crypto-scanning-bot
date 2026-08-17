from __future__ import annotations

import hashlib
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Final, Literal, get_args, get_origin

import yaml
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from signalbot.capture.models import (
    PUBLIC_REST_PATHS_BY_MARKET,
    validate_public_rest_path,
)
from signalbot.capture.path_safety import inspect_link_free_path
from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import (
    FUTURES_REST_BASE,
    FUTURES_WS_MARKET,
    FUTURES_WS_PUBLIC,
    SPOT_MARKET_DATA_REST_BASE,
    SPOT_WS_MARKET_DATA_ONLY,
)

FROZEN_PROTOCOL_SHA256 = "9c925f5988e65a1371e8859dd00ea6a61db0c3b9ea34622432e9a28a3bab297b"
CANARY_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
SPOT_EXCHANGE_INFO_SYMBOLS_QUERY: Final = '["BTCUSDT","ETHUSDT","SOLUSDT"]'
SEPARATE_PATH_AUDIT_ONLY = "SEPARATE_PATH_AUDIT_ONLY"
CANARY_FIXED_REQUEST_HEADERS = (("accept-encoding", "identity"),)
SPOT_REQUEST_WEIGHT_LIMIT_PER_MINUTE: Final = 6_000
SPOT_USED_WEIGHT_QUARANTINE_THRESHOLD: Final = 5_000
SPOT_DEPTH_SNAPSHOT_MINIMUM_ADMISSION_INTERVAL_SECONDS: Final = 3.2
SPOT_DEPTH_SNAPSHOT_LIMIT: Final = 5_000
SPOT_DEPTH_SNAPSHOT_REQUEST_WEIGHT: Final = 250
FUTURES_DEPTH_SNAPSHOT_LIMIT: Final = 1_000
FUTURES_DEPTH_SNAPSHOT_REQUEST_WEIGHT: Final = 20


def _yaml_list_to_tuple(value: object) -> object:
    return tuple(value) if isinstance(value, list) else value


def _yaml_pairs_to_tuple(value: object) -> object:
    if not isinstance(value, list):
        return value
    return tuple(tuple(item) if isinstance(item, list) else item for item in value)


def _scalar_types(annotation: object) -> tuple[type[object], ...]:
    if annotation in (str, int, float, bool):
        assert isinstance(annotation, type)
        return (annotation,)
    if get_origin(annotation) is Literal:
        return tuple(dict.fromkeys(type(item) for item in get_args(annotation)))
    return ()


class _StrictFrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="before")
    @classmethod
    def reject_cross_type_scalars(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            return value
        for name, field in cls.model_fields.items():
            if name not in value:
                continue
            expected = _scalar_types(field.annotation)
            actual = value[name]
            if expected and actual is not None and type(actual) not in expected:
                raise ValueError(f"{name} must use its exact scalar type")
        return value


class CanaryWebSocketSettings(_StrictFrozenModel):
    batch_size: Literal[25]
    maximum_connection_age_seconds: Literal[86_100]
    heartbeat_interval_seconds: Literal[60]
    pong_timeout_seconds: Literal[10]
    connect_timeout_seconds: Literal[20]
    close_timeout_seconds: Literal[10]
    internal_queue_frames: Literal[2_048]
    maximum_frame_bytes: Literal[8_388_608]
    maximum_reconnect_attempts: Literal[8]
    reconnect_delays_seconds: tuple[int, int, int, int, int, int, int, int]
    depth_stream_variant: Literal["standard"]

    @field_validator("reconnect_delays_seconds", mode="before")
    @classmethod
    def convert_yaml_reconnect_list(cls, value: object) -> object:
        return _yaml_list_to_tuple(value)

    @model_validator(mode="after")
    def require_frozen_reconnect_schedule(self) -> CanaryWebSocketSettings:
        if self.reconnect_delays_seconds != (1, 2, 4, 8, 16, 30, 30, 30):
            raise ValueError("reconnect_delays_seconds differs from the frozen canary")
        return self


class CanaryHandoffSettings(_StrictFrozenModel):
    maximum_events: Literal[100_000]
    maximum_encoded_bytes: Literal[268_435_456]


class CanaryStorageSettings(_StrictFrozenModel):
    maximum_total_bytes: Literal[107_374_182_400]
    emergency_reserve_bytes: Literal[536_870_912]
    rotation_interval_ms: Literal[300_000]
    maximum_segment_uncompressed_bytes: Literal[268_435_456]
    maximum_segment_websocket_frames: Literal[1_000_000]


class CanaryRestSettings(_StrictFrozenModel):
    method: Literal["GET"]
    maximum_concurrency: Literal[4]
    timeout_seconds: Literal[15]
    maximum_body_bytes: Literal[16_777_216]
    maximum_attempts: Literal[2]
    retry_delays_seconds: tuple[float]
    maximum_retry_after_seconds: Literal[30]
    follow_redirects: Literal[False]
    trust_environment: Literal[False]

    @field_validator("retry_delays_seconds", mode="before")
    @classmethod
    def convert_yaml_retry_list(cls, value: object) -> object:
        return _yaml_list_to_tuple(value)

    @model_validator(mode="after")
    def require_frozen_retry_schedule(self) -> CanaryRestSettings:
        if self.retry_delays_seconds != (1.0,):
            raise ValueError("retry_delays_seconds differs from the frozen canary")
        return self


class CanaryPollingSettings(_StrictFrozenModel):
    venue_time_seconds: Literal[30]
    exchange_info_seconds: Literal[60]
    exchange_info_hash_on_change: Literal[True]
    spot_depth_snapshot_limit: Literal[5_000]
    futures_depth_snapshot_limit: Literal[1_000]
    depth_snapshot_bridge_maximum_attempts: Literal[3]
    depth_snapshot_bridge_wait_seconds: Literal[2]
    depth_snapshot_triggers: tuple[
        Literal["startup"],
        Literal["reconnect"],
        Literal["sequence_gap"],
    ]
    futures_open_interest_seconds: Literal[5]
    futures_open_interest_history_period: Literal["5m"]
    futures_open_interest_history_bar_alignment_seconds: Literal[300]
    futures_open_interest_history_delay_seconds: Literal[15]
    futures_open_interest_history_data_role: Literal["cross_check_non_primary"]
    futures_premium_index_seconds: Literal[30]
    futures_premium_index_data_role: Literal["cross_check_non_primary"]
    futures_funding_rate_delay_seconds: Literal[15]
    futures_funding_rate_maximum_attempts: Literal[2]
    futures_funding_info_seconds: Literal[300]
    futures_funding_info_on_exchange_info_change: Literal[True]

    @field_validator("depth_snapshot_triggers", mode="before")
    @classmethod
    def convert_yaml_depth_trigger_list(cls, value: object) -> object:
        return _yaml_list_to_tuple(value)


class CaptureCanaryConfig(_StrictFrozenModel):
    schema_version: Literal["capture_canary_config_v1"]
    purpose: Literal["infrastructure_only"]
    efficacy_outputs_enabled: Literal[False]
    order_execution_enabled: Literal[False]
    api_credentials_enabled: Literal[False]
    protocol_sha256: Literal["9c925f5988e65a1371e8859dd00ea6a61db0c3b9ea34622432e9a28a3bab297b"]
    symbols: tuple[str, str, str]
    duration_seconds: Literal[86_400]
    websocket: CanaryWebSocketSettings
    handoff: CanaryHandoffSettings
    storage: CanaryStorageSettings
    rest: CanaryRestSettings
    polling: CanaryPollingSettings

    @field_validator("symbols", mode="before")
    @classmethod
    def convert_yaml_symbol_list(cls, value: object) -> object:
        return _yaml_list_to_tuple(value)

    @model_validator(mode="after")
    def require_frozen_symbols(self) -> CaptureCanaryConfig:
        if self.symbols != CANARY_SYMBOLS:
            raise ValueError(f"symbols must be exactly {CANARY_SYMBOLS!r}")
        return self


class CanaryRestAllowedQueryValues(_StrictFrozenModel):
    key: str
    values: tuple[str, ...]

    @field_validator("values", mode="before")
    @classmethod
    def convert_value_list(cls, value: object) -> object:
        return _yaml_list_to_tuple(value)


class CanaryRestRequestPlanEntry(_StrictFrozenModel):
    role: Literal[
        "spot_venue_time",
        "spot_exchange_info",
        "spot_depth_snapshot",
        "futures_venue_time",
        "futures_exchange_info",
        "futures_depth_snapshot",
        "futures_open_interest",
        "futures_open_interest_history",
        "futures_premium_index",
        "futures_funding_rate_confirmation",
        "futures_funding_info",
    ]
    method: Literal["GET"]
    market: Market
    rest_base: str
    path: str
    fixed_request_headers: tuple[tuple[str, str], ...]
    fixed_query: tuple[tuple[str, str], ...]
    allowed_query_keys: tuple[str, ...]
    allowed_query_values: tuple[CanaryRestAllowedQueryValues, ...]
    maximum_query_limit: Literal[1, 1_000, 5_000] | None
    trigger: Literal[
        "interval",
        "depth_resync_event_only",
        "utc_bar_close",
        "next_funding_time",
        "interval_or_exchange_info_hash_change",
    ]
    interval_seconds: int | None
    delay_seconds: int | None
    hash_on_change: bool
    trigger_events: tuple[str, ...]
    maximum_attempts: Literal[1, 2]
    data_role: Literal["primary_capture", "cross_check_non_primary"]

    @field_validator(
        "allowed_query_keys",
        "allowed_query_values",
        "trigger_events",
        mode="before",
    )
    @classmethod
    def convert_plan_lists(cls, value: object) -> object:
        return _yaml_list_to_tuple(value)

    @field_validator("fixed_request_headers", "fixed_query", mode="before")
    @classmethod
    def convert_fixed_query_pairs(cls, value: object) -> object:
        return _yaml_pairs_to_tuple(value)

    @model_validator(mode="after")
    def require_public_bounded_request(self) -> CanaryRestRequestPlanEntry:
        expected_base = (
            SPOT_MARKET_DATA_REST_BASE if self.market is Market.SPOT else FUTURES_REST_BASE
        )
        if self.rest_base != expected_base:
            raise ValueError("request plan base differs from its public market-data base")
        validate_public_rest_path(self.market, self.path)
        if self.fixed_request_headers != CANARY_FIXED_REQUEST_HEADERS:
            raise ValueError("request plan must force identity response encoding")
        if tuple(sorted(set(self.allowed_query_keys))) != self.allowed_query_keys:
            raise ValueError("allowed_query_keys must be unique and sorted")
        if tuple(sorted(self.fixed_query)) != self.fixed_query:
            raise ValueError("fixed_query must be sorted")
        value_keys = tuple(item.key for item in self.allowed_query_values)
        if value_keys != self.allowed_query_keys:
            raise ValueError("allowed_query_values must cover the exact allowed query keys")
        allowed = {item.key: item.values for item in self.allowed_query_values}
        if any(value not in allowed[key] for key, value in self.fixed_query):
            raise ValueError("fixed_query contains a value outside its allowlist")
        fixed_limit = dict(self.fixed_query).get("limit")
        if fixed_limit is None:
            if self.maximum_query_limit is not None:
                raise ValueError("maximum_query_limit requires a fixed limit query")
        elif self.maximum_query_limit != int(fixed_limit):
            raise ValueError("maximum_query_limit differs from the fixed limit")
        depth_contracts = {
            "spot_depth_snapshot": (
                Market.SPOT,
                "/api/v3/depth",
                str(SPOT_DEPTH_SNAPSHOT_LIMIT),
                SPOT_DEPTH_SNAPSHOT_LIMIT,
            ),
            "futures_depth_snapshot": (
                Market.FUTURES,
                "/fapi/v1/depth",
                str(FUTURES_DEPTH_SNAPSHOT_LIMIT),
                FUTURES_DEPTH_SNAPSHOT_LIMIT,
            ),
        }
        expected_depth = depth_contracts.get(self.role)
        if expected_depth is not None:
            expected_market, expected_path, expected_limit, expected_maximum = expected_depth
            if (
                self.market is not expected_market
                or self.path != expected_path
                or self.fixed_query != (("limit", expected_limit),)
                or self.maximum_query_limit != expected_maximum
            ):
                raise ValueError("depth snapshot request differs from its venue-specific contract")
        if self.role == "spot_exchange_info":
            exact_values = (
                CanaryRestAllowedQueryValues(
                    key="symbols",
                    values=(SPOT_EXCHANGE_INFO_SYMBOLS_QUERY,),
                ),
            )
            if (
                self.market is not Market.SPOT
                or self.path != "/api/v3/exchangeInfo"
                or self.fixed_query
                != (("symbols", SPOT_EXCHANGE_INFO_SYMBOLS_QUERY),)
                or self.allowed_query_keys != ("symbols",)
                or self.allowed_query_values != exact_values
                or self.maximum_query_limit is not None
            ):
                raise ValueError(
                    "Spot exchangeInfo request differs from its exact canary-symbol contract"
                )
        return self


def load_capture_canary_config(
    config_file: str | Path,
    *,
    protocol_file: str | Path,
) -> CaptureCanaryConfig:
    """Load the strict non-efficacy canary contract and bind its protocol bytes."""

    config_path = _require_regular_non_symlink_file(config_file, "config_file")
    protocol_path = _require_regular_non_symlink_file(protocol_file, "protocol_file")
    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError("capture canary configuration root must be a mapping")
    settings = CaptureCanaryConfig.model_validate(raw)
    actual_protocol_sha256 = sha256_file(protocol_path)
    if actual_protocol_sha256 != settings.protocol_sha256:
        raise ValueError("protocol_file SHA-256 differs from the frozen capture canary contract")
    validate_capture_route_registry()
    return settings


def capture_rest_request_plan() -> tuple[CanaryRestRequestPlanEntry, ...]:
    """Return the exact public REST calls authorized for the three-symbol canary."""

    symbols = CanaryRestAllowedQueryValues(key="symbol", values=CANARY_SYMBOLS)
    spot_depth_values = (
        CanaryRestAllowedQueryValues(key="limit", values=(str(SPOT_DEPTH_SNAPSHOT_LIMIT),)),
        symbols,
    )
    futures_depth_values = (
        CanaryRestAllowedQueryValues(key="limit", values=(str(FUTURES_DEPTH_SNAPSHOT_LIMIT),)),
        symbols,
    )
    one_row_values = (
        CanaryRestAllowedQueryValues(key="limit", values=("1",)),
        symbols,
    )
    history_values = (
        CanaryRestAllowedQueryValues(key="limit", values=("1",)),
        CanaryRestAllowedQueryValues(key="period", values=("5m",)),
        symbols,
    )
    spot_exchange_info_values = (
        CanaryRestAllowedQueryValues(
            key="symbols",
            values=(SPOT_EXCHANGE_INFO_SYMBOLS_QUERY,),
        ),
    )
    return (
        _request(
            role="spot_venue_time",
            market=Market.SPOT,
            path="/api/v3/time",
            trigger="interval",
            interval_seconds=30,
        ),
        _request(
            role="spot_exchange_info",
            market=Market.SPOT,
            path="/api/v3/exchangeInfo",
            fixed_query=(("symbols", SPOT_EXCHANGE_INFO_SYMBOLS_QUERY),),
            allowed_query_keys=("symbols",),
            allowed_query_values=spot_exchange_info_values,
            trigger="interval",
            interval_seconds=60,
            hash_on_change=True,
        ),
        _request(
            role="spot_depth_snapshot",
            market=Market.SPOT,
            path="/api/v3/depth",
            fixed_query=(("limit", str(SPOT_DEPTH_SNAPSHOT_LIMIT)),),
            allowed_query_keys=("limit", "symbol"),
            allowed_query_values=spot_depth_values,
            maximum_query_limit=SPOT_DEPTH_SNAPSHOT_LIMIT,
            trigger="depth_resync_event_only",
            trigger_events=("startup", "reconnect", "sequence_gap"),
        ),
        _request(
            role="futures_venue_time",
            market=Market.FUTURES,
            path="/fapi/v1/time",
            trigger="interval",
            interval_seconds=30,
        ),
        _request(
            role="futures_exchange_info",
            market=Market.FUTURES,
            path="/fapi/v1/exchangeInfo",
            trigger="interval",
            interval_seconds=60,
            hash_on_change=True,
        ),
        _request(
            role="futures_depth_snapshot",
            market=Market.FUTURES,
            path="/fapi/v1/depth",
            fixed_query=(("limit", str(FUTURES_DEPTH_SNAPSHOT_LIMIT)),),
            allowed_query_keys=("limit", "symbol"),
            allowed_query_values=futures_depth_values,
            maximum_query_limit=FUTURES_DEPTH_SNAPSHOT_LIMIT,
            trigger="depth_resync_event_only",
            trigger_events=("startup", "reconnect", "sequence_gap"),
        ),
        _request(
            role="futures_open_interest",
            market=Market.FUTURES,
            path="/fapi/v1/openInterest",
            allowed_query_keys=("symbol",),
            allowed_query_values=(symbols,),
            trigger="interval",
            interval_seconds=5,
        ),
        _request(
            role="futures_open_interest_history",
            market=Market.FUTURES,
            path="/futures/data/openInterestHist",
            fixed_query=(("limit", "1"), ("period", "5m")),
            allowed_query_keys=("limit", "period", "symbol"),
            allowed_query_values=history_values,
            maximum_query_limit=1,
            trigger="utc_bar_close",
            interval_seconds=300,
            delay_seconds=15,
            data_role="cross_check_non_primary",
        ),
        _request(
            role="futures_premium_index",
            market=Market.FUTURES,
            path="/fapi/v1/premiumIndex",
            allowed_query_keys=("symbol",),
            allowed_query_values=(symbols,),
            trigger="interval",
            interval_seconds=30,
            data_role="cross_check_non_primary",
        ),
        _request(
            role="futures_funding_rate_confirmation",
            market=Market.FUTURES,
            path="/fapi/v1/fundingRate",
            fixed_query=(("limit", "1"),),
            allowed_query_keys=("limit", "symbol"),
            allowed_query_values=one_row_values,
            maximum_query_limit=1,
            trigger="next_funding_time",
            delay_seconds=15,
            maximum_attempts=2,
        ),
        _request(
            role="futures_funding_info",
            market=Market.FUTURES,
            path="/fapi/v1/fundingInfo",
            trigger="interval_or_exchange_info_hash_change",
            interval_seconds=300,
            trigger_events=("exchange_info_hash_change",),
        ),
    )


def capture_route_registry() -> dict[str, object]:
    """Serialize transport allowlists separately from the frozen request plan."""

    return {
        "transport_public_allowlist": {
            "spot": {
                "rest_base": SPOT_MARKET_DATA_REST_BASE,
                "rest_paths": list(PUBLIC_REST_PATHS_BY_MARKET[Market.SPOT]),
                "websocket_base": SPOT_WS_MARKET_DATA_ONLY,
                "stream_suffixes": [
                    "@aggTrade",
                    "@bookTicker",
                    "@depth@100ms",
                    "@kline_5m",
                ],
            },
            "futures": {
                "rest_base": FUTURES_REST_BASE,
                "rest_paths": list(PUBLIC_REST_PATHS_BY_MARKET[Market.FUTURES]),
                "websocket_market_base": FUTURES_WS_MARKET,
                "websocket_market_suffixes": [
                    "@aggTrade",
                    "@kline_5m",
                    "@markPrice@1s",
                ],
                "websocket_public_base": FUTURES_WS_PUBLIC,
                "websocket_public_suffixes": ["@bookTicker", "@depth@100ms"],
            },
        },
        "frozen_canary_rest_request_plan": [
            entry.model_dump(mode="json") for entry in capture_rest_request_plan()
        ],
    }


def validate_capture_route_registry() -> None:
    registry = capture_route_registry()
    transport = registry.get("transport_public_allowlist")
    if not isinstance(transport, dict):
        raise ValueError("capture route registry lacks its transport allowlist")
    if (
        SPOT_MARKET_DATA_REST_BASE != "https://data-api.binance.vision"
        or SPOT_WS_MARKET_DATA_ONLY != "wss://data-stream.binance.vision:443/stream?streams="
        or FUTURES_REST_BASE != "https://fapi.binance.com"
        or FUTURES_WS_MARKET != "wss://fstream.binance.com/market/stream?streams="
        or FUTURES_WS_PUBLIC != "wss://fstream.binance.com/public/stream?streams="
    ):
        raise ValueError("capture route registry differs from the frozen public-only routes")
    spot = transport.get("spot")
    futures = transport.get("futures")
    if not isinstance(spot, dict) or not isinstance(futures, dict):
        raise ValueError("capture route registry differs from the frozen public-only routes")
    if spot.get("rest_paths") != list(PUBLIC_REST_PATHS_BY_MARKET[Market.SPOT]):
        raise ValueError("capture route registry differs from the REST path owner")
    if futures.get("rest_paths") != list(PUBLIC_REST_PATHS_BY_MARKET[Market.FUTURES]):
        raise ValueError("capture route registry differs from the REST path owner")
    plan = capture_rest_request_plan()
    expected_roles = (
        "spot_venue_time",
        "spot_exchange_info",
        "spot_depth_snapshot",
        "futures_venue_time",
        "futures_exchange_info",
        "futures_depth_snapshot",
        "futures_open_interest",
        "futures_open_interest_history",
        "futures_premium_index",
        "futures_funding_rate_confirmation",
        "futures_funding_info",
    )
    if tuple(entry.role for entry in plan) != expected_roles:
        raise ValueError("capture REST request plan differs from the frozen canary")
    depth_by_role = {entry.role: entry for entry in plan if "depth_snapshot" in entry.role}
    expected_depth_limits = {
        "spot_depth_snapshot": SPOT_DEPTH_SNAPSHOT_LIMIT,
        "futures_depth_snapshot": FUTURES_DEPTH_SNAPSHOT_LIMIT,
    }
    if {
        role: (entry.fixed_query, entry.maximum_query_limit)
        for role, entry in depth_by_role.items()
    } != {role: ((("limit", str(limit)),), limit) for role, limit in expected_depth_limits.items()}:
        raise ValueError("capture REST depth limits differ from the venue-specific contract")
    serialized_plan = registry.get("frozen_canary_rest_request_plan")
    if serialized_plan != [entry.model_dump(mode="json") for entry in plan]:
        raise ValueError("capture REST request plan serialization drifted")


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _request(
    *,
    role: str,
    market: Market,
    path: str,
    fixed_query: tuple[tuple[str, str], ...] = (),
    allowed_query_keys: tuple[str, ...] = (),
    allowed_query_values: tuple[CanaryRestAllowedQueryValues, ...] = (),
    maximum_query_limit: Literal[1, 1_000, 5_000] | None = None,
    trigger: str,
    interval_seconds: int | None = None,
    delay_seconds: int | None = None,
    hash_on_change: bool = False,
    trigger_events: tuple[str, ...] = (),
    maximum_attempts: int = 1,
    data_role: str = "primary_capture",
) -> CanaryRestRequestPlanEntry:
    rest_base = SPOT_MARKET_DATA_REST_BASE if market is Market.SPOT else FUTURES_REST_BASE
    return CanaryRestRequestPlanEntry.model_validate(
        {
            "role": role,
            "method": "GET",
            "market": market,
            "rest_base": rest_base,
            "path": path,
            "fixed_request_headers": CANARY_FIXED_REQUEST_HEADERS,
            "fixed_query": fixed_query,
            "allowed_query_keys": allowed_query_keys,
            "allowed_query_values": allowed_query_values,
            "maximum_query_limit": maximum_query_limit,
            "trigger": trigger,
            "interval_seconds": interval_seconds,
            "delay_seconds": delay_seconds,
            "hash_on_change": hash_on_change,
            "trigger_events": trigger_events,
            "maximum_attempts": maximum_attempts,
            "data_role": data_role,
        }
    )


def _require_regular_non_symlink_file(path: str | Path, field: str) -> Path:
    inspection = inspect_link_free_path(path, field)
    status = inspection.final_status
    if status is None or not stat.S_ISREG(status.st_mode):
        raise ValueError(f"{field} must be an existing regular file")
    return inspection.absolute_path
