from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

_SYMBOL = re.compile(r"^[A-Z0-9]{2,20}USDT$")
_CONFIG_KEYS = frozenset(
    {
        "schema_version",
        "enabled",
        "data_directory",
        "maximum_total_bytes",
        "minimum_free_bytes",
        "maximum_event_symbols",
        "event_lifetime_ms",
        "event_return_percent",
        "event_minimum_quote_volume_usdt",
        "rest_weight_budget_per_minute",
        "event_depth_enabled",
    }
)


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """Validated non-secret settings for the isolated capture process."""

    enabled: bool
    data_directory: Path
    maximum_total_bytes: int
    minimum_free_bytes: int
    maximum_event_symbols: int
    event_lifetime_ms: int
    event_return_percent: Decimal
    event_minimum_quote_volume_usdt: Decimal
    rest_weight_budget_per_minute: int
    event_depth_enabled: bool


DEFAULT_DATA_MOUNT = Path("/var/lib/binance-bot-2/prospective-r")


def load_collector_config(path: str | Path) -> CollectorConfig:
    """Load strict JSON from a local config boundary; unknown fields fail closed."""

    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or set(raw) != _CONFIG_KEYS:
        raise ValueError("collector config must contain exactly the declared fields")
    if raw["schema_version"] != "pump_fade_v3_collector_v1":
        raise ValueError("unsupported collector config schema_version")
    for key in ("enabled", "event_depth_enabled"):
        if type(raw[key]) is not bool:
            raise ValueError(f"{key} must be a boolean")
    integer_fields = (
        "maximum_total_bytes",
        "minimum_free_bytes",
        "maximum_event_symbols",
        "event_lifetime_ms",
        "rest_weight_budget_per_minute",
    )
    if any(type(raw[key]) is not int or raw[key] <= 0 for key in integer_fields):
        raise ValueError("collector integer limits must be positive integers")
    if raw["maximum_event_symbols"] > 25:
        raise ValueError("maximum_event_symbols cannot exceed 25")
    if raw["rest_weight_budget_per_minute"] > 600:
        raise ValueError("collector REST budget cannot exceed 600 weight/minute")
    if raw["minimum_free_bytes"] >= raw["maximum_total_bytes"]:
        raise ValueError("free-space floor must be below the hard data cap")
    for key in ("event_return_percent", "event_minimum_quote_volume_usdt"):
        if type(raw[key]) not in (int, float) or raw[key] <= 0:
            raise ValueError(f"{key} must be a positive finite number")
        try:
            if not Decimal(str(raw[key])).is_finite():
                raise ValueError(f"{key} must be finite")
        except InvalidOperation as exc:
            raise ValueError(f"{key} must be a positive finite number") from exc
    if not Path(raw["data_directory"]).is_absolute() and not raw[
        "data_directory"
    ].startswith("/"):
        raise ValueError("data_directory must be an absolute path")
    return CollectorConfig(
        enabled=raw["enabled"],
        data_directory=Path(raw["data_directory"]),
        maximum_total_bytes=raw["maximum_total_bytes"],
        minimum_free_bytes=raw["minimum_free_bytes"],
        maximum_event_symbols=raw["maximum_event_symbols"],
        event_lifetime_ms=raw["event_lifetime_ms"],
        event_return_percent=Decimal(str(raw["event_return_percent"])),
        event_minimum_quote_volume_usdt=Decimal(
            str(raw["event_minimum_quote_volume_usdt"])
        ),
        rest_weight_budget_per_minute=raw["rest_weight_budget_per_minute"],
        event_depth_enabled=raw["event_depth_enabled"],
    )


def validate_usdt_symbol(symbol: str) -> None:
    """Reject non-normalized or non-USDT symbols at an external-data boundary."""

    if not isinstance(symbol, str) or _SYMBOL.fullmatch(symbol) is None:
        raise ValueError("symbol must be a normalized USDⓈ-M USDT symbol")


def parse_ticker_event(raw: Any) -> tuple[str, Decimal, Decimal, int]:
    """Parse an exchange mini-ticker/24h ticker row into trigger inputs.

    The caller must first select USDⓈ-M rows (`st == 1`) from post-migration
    combined streams. The returned change is percentage points, not a fraction.
    """

    if not isinstance(raw, dict):
        raise ValueError("ticker event must be a JSON object")
    symbol = raw.get("s")
    if not isinstance(symbol, str):
        raise ValueError("ticker symbol must be a string")
    validate_usdt_symbol(symbol)
    event_ms = raw.get("E")
    if type(event_ms) is not int or event_ms < 0:
        raise ValueError("ticker event time must be non-negative Unix milliseconds")
    try:
        quote_volume = Decimal(str(raw["q"]))
        if "P" in raw:
            change_percent = Decimal(str(raw["P"]))
        else:
            open_price = Decimal(str(raw["o"]))
            close_price = Decimal(str(raw["c"]))
            if open_price <= 0:
                raise ValueError("ticker open price must be positive")
            change_percent = (close_price / open_price - 1) * Decimal(100)
    except (KeyError, TypeError, ValueError, InvalidOperation, ZeroDivisionError) as exc:
        raise ValueError("ticker event lacks numeric 24h change/quote volume") from exc
    if not change_percent.is_finite() or not quote_volume.is_finite():
        raise ValueError("ticker event values must be finite")
    if quote_volume < 0:
        raise ValueError("ticker quote volume cannot be negative")
    return symbol, change_percent, quote_volume, event_ms
