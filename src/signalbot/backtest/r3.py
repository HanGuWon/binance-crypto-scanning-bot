from __future__ import annotations

import csv
import hashlib
import json
import math
import random
import statistics
import struct
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from signalbot.backtest.config import BacktestSpec, load_backtest_spec
from signalbot.backtest.labels import classify_kline_proxy_outcome

_DAY_MS = 86_400_000
_INTERVAL_MS = 300_000
_HORIZONS = (3, 6, 12)
_BLOCK_DAYS = (7, 14, 28)
_PRIMARY_HORIZON = 12
_PRIMARY_BLOCK_DAYS = 7
_PROTOCOL_VERSION = "r3_exposed_kline_proxy_diagnostic_v1"
_RULE_VERSION = "v3.2.0-r3-c0-causal-labels"
_BOOTSTRAP_SAMPLES = 50_000
_BOOTSTRAP_SEED = 20_260_716
_FROZEN_SPEC_SHA256 = "7cf0849a517641003d437203388889f193075ca71acfd745ecc00e7d54ed8fed"
_FROZEN_PLAN_SHA256 = "097ca77669e4aa13f4e6d618c2799681c954aceddc55d602c2a890ba34d26706"
_FROZEN_SETTINGS_SHA256 = "0e30408376f8bc6832794dbc6ef92796b7b25ebe472ed319b0c1acde253b8c5c"
_FROZEN_INPUT_LEDGER_SHA256 = (
    "f382cbe8af0f5c70127984a2fe84766bbc9a3f9b52804dba8444d27971f37045"
)
_FROZEN_INPUT_LEDGER_PATH = Path(
    "artifacts/backtest/2026-07-17-r3/input_panel.sha256.json"
)
_ASSETS = ("BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "SUI", "WIF")
_MARKET_DIRECTION = {"spot": "long", "futures": "short"}
_MARKET_LABEL = {
    "spot": "KLINE_PROXY_LONG",
    "futures": "KLINE_PROXY_SHORT",
}
_LABELS = (
    "KLINE_PROXY_LONG",
    "KLINE_PROXY_FLAT",
    "KLINE_PROXY_SHORT",
)
_RUN_OUTPUTS = ("opportunities.csv", "trades.csv", "results.json", "report.md")
_R3_RAW_ARTIFACT_ROLE = "RAW_REPLAY_INPUT_NOT_FINAL_R3_ANALYSIS"
_R3_RAW_REPLAY_CONTRACT: dict[str, Any] = {
    "sequential_t72_ledger": {
        "independent_episodes": False,
        "analysis_role": "SECONDARY_NON_PRIMARY",
    },
    "legacy_bootstrap": {
        "is_frozen_r3_shared_utc_day_mbb": False,
        "analysis_role": "DESCRIPTIVE_NOT_FINAL_R3_INFERENCE",
    },
    "r2_c0_t72_status": "PROTOCOL_MISMATCH",
    "final_r3_analysis": {
        "source": "SEPARATE_OPPORTUNITY_BASED_R3_ANALYZER",
        "provides_primary_efficacy": True,
        "provides_status_axes": True,
    },
}

R3Efficacy = Literal[
    "EXPLORATORY_SCREEN_PASS",
    "EXPLORATORY_FAIL",
    "INCONCLUSIVE_LOW_INFORMATION",
]


@dataclass(frozen=True, slots=True)
class R3Opportunity:
    opportunity_id: str
    protocol_version: str
    rule_version: str
    asset: str
    cohort: str
    market: str
    symbol: str
    direction: str
    family: str
    decision_time_ms: int
    next_open_time_ms: int
    reasons: str
    invalidation: float
    eligible: bool
    gate_passed: bool
    execution_observed: bool
    full_r2_eligible: bool | None
    split: str
    regime: str
    btc_trend: str
    breadth_ratio: float
    analysis_eligible: bool
    analysis_exclusion: str
    analysis_eligible_by_horizon: tuple[bool, ...]
    analysis_exclusion_by_horizon: tuple[str, ...]
    analysis_eligible_72: bool
    analysis_exclusion_72: str
    forward_returns: tuple[float | None, ...]
    long_net_returns: tuple[float | None, ...]
    short_net_returns: tuple[float | None, ...]
    outcome_labels: tuple[str | None, ...]
    signal_gross_returns: tuple[float | None, ...]
    signal_fee_returns: tuple[float | None, ...]
    signal_slippage_returns: tuple[float | None, ...]
    signal_funding_returns: tuple[float | None, ...]
    signal_net_returns: tuple[float | None, ...]
    f60_execution_model: str
    f60_components: tuple[float | None, ...]

    @property
    def utc_day(self) -> int:
        return self.decision_time_ms // _DAY_MS

    def horizon_index(self, horizon: int) -> int:
        try:
            return _HORIZONS.index(horizon)
        except ValueError as exc:
            raise ValueError(f"unsupported R3 horizon: {horizon}") from exc


@dataclass(frozen=True, slots=True)
class R3TechnicalTrade:
    trade_id: str
    opportunity_id: str
    protocol_version: str
    rule_version: str
    asset: str
    cohort: str
    market: str
    symbol: str
    direction: str
    family: str
    split: str
    split_contained: bool
    entry_signal_id: str
    decision_time_ms: int
    entry_time_ms: int
    exit_time_ms: int
    entry_price: float
    exit_price: float
    entry_execution_price: float
    exit_execution_price: float
    exit_reason: str
    bars_held: int
    gross_return: float
    slippage_return: float
    fee_return: float
    funding_return: float
    net_return: float


def _strict_bool(value: object, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
    raise ValueError(f"{field} must be true or false")


def _finite_float(value: object, field: str) -> float:
    parsed = float(str(value))
    if not math.isfinite(parsed):
        raise ValueError(f"{field} must be finite")
    return parsed


def _optional_finite_float(value: object, field: str) -> float | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return _finite_float(value, field)


def _present_float(value: float | None, field: str) -> float:
    if value is None:
        raise ValueError(f"{field} must be present")
    return value


def _required_text(value: object, field: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field} must not be empty")
    return text


def _read_rows(
    path: str | Path,
    required_fields: frozenset[str],
    *,
    exact_fields: frozenset[str] | None = None,
) -> list[dict[str, str]]:
    source = Path(path)
    with source.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames
        if fields is None:
            raise ValueError(f"CSV header is missing: {source}")
        if len(fields) != len(set(fields)):
            raise ValueError(f"CSV header contains duplicate fields: {source}")
        missing = sorted(required_fields.difference(fields))
        if missing:
            raise ValueError(f"CSV is missing required fields {missing}: {source}")
        if exact_fields is not None and set(fields) != exact_fields:
            unexpected = sorted(set(fields).difference(exact_fields))
            absent = sorted(exact_fields.difference(fields))
            raise ValueError(
                f"CSV schema drift (unexpected={unexpected}, absent={absent}): {source}"
            )
        rows: list[dict[str, str]] = []
        for line_number, row in enumerate(reader, start=2):
            if None in row:
                raise ValueError(f"unexpected extra CSV values on line {line_number}: {source}")
            rows.append({key: value or "" for key, value in row.items()})
    return rows


_OPPORTUNITY_FIELDS = frozenset(
    {
        "opportunity_id",
        "protocol_version",
        "rule_version",
        "asset",
        "cohort",
        "market",
        "symbol",
        "direction",
        "family",
        "decision_time_ms",
        "next_open_time_ms",
        "reasons",
        "invalidation",
        "eligible",
        "gate_passed",
        "execution_observed",
        "full_r2_eligible",
        "split",
        "regime",
        "btc_trend",
        "breadth_ratio",
        "analysis_eligible",
        "analysis_exclusion",
        "f60_execution_model",
        "f60_gross_return",
        "f60_fee_return",
        "f60_slippage_return",
        "f60_funding_return",
        "f60_net_return",
    }
    | {
        f"{prefix}_{horizon}"
        for horizon in _HORIZONS
        for prefix in (
            "analysis_eligible",
            "analysis_exclusion",
            "forward_return",
            "long_net_return",
            "short_net_return",
            "outcome_label",
            "signal_gross_return",
            "signal_fee_return",
            "signal_slippage_return",
            "signal_funding_return",
            "signal_net_return",
        )
    }
)

_OPPORTUNITY_SCHEMA_FIELDS = frozenset(
    {
        "opportunity_id",
        "protocol_version",
        "rule_version",
        "volume_feature_set",
        "asset",
        "cohort",
        "market",
        "symbol",
        "direction",
        "family",
        "decision_time_ms",
        "next_open_time_ms",
        "setup_strength",
        "reasons",
        "invalidation",
        "eligible",
        "gate_passed",
        "gate_failures",
        "htf_filter_accepted",
        "htf_filter_failures",
        "execution_observed",
        "full_r2_eligible",
        "split",
        "regime",
        "btc_trend",
        "breadth_ratio",
        "analysis_eligible",
        "analysis_exclusion",
        "analysis_eligible_72",
        "analysis_exclusion_72",
        "volume_feature_available",
        "volume_feature_unavailable_reason",
        "taker_delta_3",
        "taker_delta_12",
        "normalized_vpci",
        "normalized_vpci_signal",
        "normalized_vpci_slope_3",
        "forward_return_72",
        "f60_execution_model",
        "f60_gross_return",
        "f60_fee_return",
        "f60_slippage_return",
        "f60_funding_return",
        "f60_net_return",
        "mfe_72",
        "mae_72",
    }
    | {
        f"{prefix}_{horizon}"
        for horizon in _HORIZONS
        for prefix in (
            "analysis_eligible",
            "analysis_exclusion",
            "forward_return",
            "long_net_return",
            "short_net_return",
            "outcome_label",
            "signal_gross_return",
            "signal_fee_return",
            "signal_slippage_return",
            "signal_funding_return",
            "signal_net_return",
        )
    }
)


def read_r3_opportunities(path: str | Path) -> tuple[R3Opportunity, ...]:
    """Read the fail-closed R3 opportunity artifact."""

    output: list[R3Opportunity] = []
    rows = _read_rows(
        path,
        _OPPORTUNITY_FIELDS,
        exact_fields=_OPPORTUNITY_SCHEMA_FIELDS,
    )
    for line_number, row in enumerate(rows, start=2):
        try:
            invalidation = _finite_float(row["invalidation"], "invalidation")
            if invalidation <= 0:
                raise ValueError("invalidation must be positive")
            output.append(
                R3Opportunity(
                    opportunity_id=_required_text(row["opportunity_id"], "opportunity_id"),
                    protocol_version=_required_text(row["protocol_version"], "protocol_version"),
                    rule_version=_required_text(row["rule_version"], "rule_version"),
                    asset=_required_text(row["asset"], "asset").upper(),
                    cohort=_required_text(row["cohort"], "cohort").lower(),
                    market=_required_text(row["market"], "market").lower(),
                    symbol=_required_text(row["symbol"], "symbol").upper(),
                    direction=_required_text(row["direction"], "direction").lower(),
                    family=_required_text(row["family"], "family").lower(),
                    decision_time_ms=int(row["decision_time_ms"]),
                    next_open_time_ms=int(row["next_open_time_ms"]),
                    reasons=_required_text(row["reasons"], "reasons"),
                    invalidation=invalidation,
                    eligible=_strict_bool(row["eligible"], "eligible"),
                    gate_passed=_strict_bool(row["gate_passed"], "gate_passed"),
                    execution_observed=_strict_bool(
                        row["execution_observed"], "execution_observed"
                    ),
                    full_r2_eligible=(
                        None
                        if not row["full_r2_eligible"].strip()
                        else _strict_bool(row["full_r2_eligible"], "full_r2_eligible")
                    ),
                    split=_required_text(row["split"], "split"),
                    regime=_required_text(row["regime"], "regime").lower(),
                    btc_trend=_required_text(row["btc_trend"], "btc_trend").lower(),
                    breadth_ratio=_finite_float(row["breadth_ratio"], "breadth_ratio"),
                    analysis_eligible=_strict_bool(
                        row["analysis_eligible"], "analysis_eligible"
                    ),
                    analysis_exclusion=row["analysis_exclusion"].strip(),
                    analysis_eligible_by_horizon=tuple(
                        _strict_bool(
                            row[f"analysis_eligible_{horizon}"],
                            f"analysis_eligible_{horizon}",
                        )
                        for horizon in _HORIZONS
                    ),
                    analysis_exclusion_by_horizon=tuple(
                        row[f"analysis_exclusion_{horizon}"].strip()
                        for horizon in _HORIZONS
                    ),
                    analysis_eligible_72=_strict_bool(
                        row["analysis_eligible_72"], "analysis_eligible_72"
                    ),
                    analysis_exclusion_72=row["analysis_exclusion_72"].strip(),
                    forward_returns=tuple(
                        _optional_finite_float(
                            row[f"forward_return_{horizon}"],
                            f"forward_return_{horizon}",
                        )
                        for horizon in _HORIZONS
                    ),
                    long_net_returns=tuple(
                        _optional_finite_float(
                            row[f"long_net_return_{horizon}"],
                            f"long_net_return_{horizon}",
                        )
                        for horizon in _HORIZONS
                    ),
                    short_net_returns=tuple(
                        _optional_finite_float(
                            row[f"short_net_return_{horizon}"],
                            f"short_net_return_{horizon}",
                        )
                        for horizon in _HORIZONS
                    ),
                    outcome_labels=tuple(
                        row[f"outcome_label_{horizon}"].strip() or None
                        for horizon in _HORIZONS
                    ),
                    signal_gross_returns=tuple(
                        _optional_finite_float(
                            row[f"signal_gross_return_{horizon}"],
                            f"signal_gross_return_{horizon}",
                        )
                        for horizon in _HORIZONS
                    ),
                    signal_fee_returns=tuple(
                        _optional_finite_float(
                            row[f"signal_fee_return_{horizon}"],
                            f"signal_fee_return_{horizon}",
                        )
                        for horizon in _HORIZONS
                    ),
                    signal_slippage_returns=tuple(
                        _optional_finite_float(
                            row[f"signal_slippage_return_{horizon}"],
                            f"signal_slippage_return_{horizon}",
                        )
                        for horizon in _HORIZONS
                    ),
                    signal_funding_returns=tuple(
                        _optional_finite_float(
                            row[f"signal_funding_return_{horizon}"],
                            f"signal_funding_return_{horizon}",
                        )
                        for horizon in _HORIZONS
                    ),
                    signal_net_returns=tuple(
                        _optional_finite_float(
                            row[f"signal_net_return_{horizon}"],
                            f"signal_net_return_{horizon}",
                        )
                        for horizon in _HORIZONS
                    ),
                    f60_execution_model=_required_text(
                        row["f60_execution_model"], "f60_execution_model"
                    ),
                    f60_components=tuple(
                        _optional_finite_float(row[field], field)
                        for field in (
                            "f60_gross_return",
                            "f60_fee_return",
                            "f60_slippage_return",
                            "f60_funding_return",
                            "f60_net_return",
                        )
                    ),
                )
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid R3 opportunity on CSV line {line_number}") from exc
    return tuple(output)


_TRADE_FIELDS = frozenset(
    {
        "trade_id",
        "opportunity_id",
        "protocol_version",
        "rule_version",
        "asset",
        "cohort",
        "market",
        "symbol",
        "direction",
        "family",
        "split",
        "split_contained",
        "entry_signal_id",
        "entry_signal_time_ms",
        "entry_time_ms",
        "exit_time_ms",
        "exit_reason",
        "bars_held",
        "gross_return",
        "slippage_return",
        "fee_return",
        "funding_return",
        "net_return",
    }
)

_TRADE_SCHEMA_FIELDS = frozenset(
    {
        "trade_id",
        "opportunity_id",
        "protocol_version",
        "rule_version",
        "asset",
        "cohort",
        "market",
        "symbol",
        "direction",
        "family",
        "score",
        "split",
        "split_contained",
        "regime",
        "entry_signal_id",
        "entry_signal_time_ms",
        "entry_time_ms",
        "exit_time_ms",
        "entry_price",
        "exit_price",
        "entry_execution_price",
        "exit_execution_price",
        "initial_stop",
        "exit_reason",
        "bars_held",
        "gross_return",
        "slippage_return",
        "fee_return",
        "funding_return",
        "net_return",
        "gross_pnl_usdt",
        "slippage_usdt",
        "fees_usdt",
        "funding_pnl_usdt",
        "net_pnl_usdt",
        "mfe",
        "mae",
        "net_r_multiple",
    }
)


def read_r3_technical_trades(path: str | Path) -> tuple[R3TechnicalTrade, ...]:
    """Read the provenance-complete, sequential T72 ledger."""

    output: list[R3TechnicalTrade] = []
    rows = _read_rows(path, _TRADE_FIELDS, exact_fields=_TRADE_SCHEMA_FIELDS)
    for line_number, row in enumerate(rows, start=2):
        try:
            output.append(
                R3TechnicalTrade(
                    trade_id=_required_text(row["trade_id"], "trade_id"),
                    opportunity_id=_required_text(row["opportunity_id"], "opportunity_id"),
                    protocol_version=_required_text(row["protocol_version"], "protocol_version"),
                    rule_version=_required_text(row["rule_version"], "rule_version"),
                    asset=_required_text(row["asset"], "asset").upper(),
                    cohort=_required_text(row["cohort"], "cohort").lower(),
                    market=_required_text(row["market"], "market").lower(),
                    symbol=_required_text(row["symbol"], "symbol").upper(),
                    direction=_required_text(row["direction"], "direction").lower(),
                    family=_required_text(row["family"], "family").lower(),
                    split=_required_text(row["split"], "split"),
                    split_contained=_strict_bool(
                        row["split_contained"], "split_contained"
                    ),
                    entry_signal_id=_required_text(
                        row["entry_signal_id"], "entry_signal_id"
                    ),
                    decision_time_ms=int(row["entry_signal_time_ms"]),
                    entry_time_ms=int(row["entry_time_ms"]),
                    exit_time_ms=int(row["exit_time_ms"]),
                    entry_price=_finite_float(row["entry_price"], "entry_price"),
                    exit_price=_finite_float(row["exit_price"], "exit_price"),
                    entry_execution_price=_finite_float(
                        row["entry_execution_price"], "entry_execution_price"
                    ),
                    exit_execution_price=_finite_float(
                        row["exit_execution_price"], "exit_execution_price"
                    ),
                    exit_reason=_required_text(row["exit_reason"], "exit_reason"),
                    bars_held=int(row["bars_held"]),
                    gross_return=_finite_float(row["gross_return"], "gross_return"),
                    slippage_return=_finite_float(
                        row["slippage_return"], "slippage_return"
                    ),
                    fee_return=_finite_float(row["fee_return"], "fee_return"),
                    funding_return=_finite_float(
                        row["funding_return"], "funding_return"
                    ),
                    net_return=_finite_float(row["net_return"], "net_return"),
                )
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid R3 technical trade on CSV line {line_number}") from exc
    return tuple(output)


def _close(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-10, abs_tol=5e-12)


@dataclass(frozen=True, slots=True)
class _ExpectedExecution:
    entry_execution_price: float
    exit_execution_price: float
    gross_return: float
    slippage_return: float
    fee_return: float
    net_before_funding: float


def _expected_execution(
    direction: str,
    entry_price: float,
    exit_price: float,
    fee_bps: float,
    slippage_bps: float,
) -> _ExpectedExecution:
    if direction not in {"long", "short"}:
        raise ValueError("execution direction must be long or short")
    if entry_price <= 0 or exit_price <= 0:
        raise ValueError("execution prices must be positive")
    fee_rate = fee_bps / 10_000
    slippage_rate = slippage_bps / 10_000
    direction_sign = 1.0 if direction == "long" else -1.0
    gross = direction_sign * (exit_price - entry_price) / entry_price
    if direction == "long":
        entry_execution = entry_price * (1 + slippage_rate)
        exit_execution = exit_price * (1 - slippage_rate)
    else:
        entry_execution = entry_price * (1 - slippage_rate)
        exit_execution = exit_price * (1 + slippage_rate)
    execution_return = (
        direction_sign * (exit_execution - entry_execution) / entry_price
    )
    slippage = max(0.0, gross - execution_return)
    fee = fee_rate * (entry_execution + exit_execution) / entry_price
    return _ExpectedExecution(
        entry_execution_price=entry_execution,
        exit_execution_price=exit_execution,
        gross_return=gross,
        slippage_return=slippage,
        fee_return=fee,
        net_before_funding=execution_return - fee,
    )


def _frozen_cost_bps(
    spec: BacktestSpec, market: str, cohort: str
) -> tuple[float, float]:
    if market == "spot":
        return spec.costs.spot_fee_bps, spec.costs.spot_slippage_bps[cohort]
    return spec.costs.futures_fee_bps, spec.costs.futures_slippage_bps[cohort]


def _deterministic_opportunity_id(item: R3Opportunity) -> str:
    identity = "|".join(
        (item.market, item.symbol, item.family, str(item.decision_time_ms))
    )
    return hashlib.sha256(identity.encode()).hexdigest()[:24]


def _validate_opportunity(
    item: R3Opportunity,
    spec: BacktestSpec,
    asset_contract: Mapping[str, tuple[str, str, str]],
) -> None:
    identity = item.opportunity_id
    if item.protocol_version != spec.protocol_version or item.rule_version != spec.rule_version:
        raise ValueError(f"{identity} has protocol/rule version drift")
    if item.opportunity_id != _deterministic_opportunity_id(item):
        raise ValueError(f"{identity} has a non-deterministic opportunity_id")
    if item.asset not in asset_contract:
        raise ValueError(f"{identity} has unknown asset {item.asset}")
    cohort, spot_symbol, futures_symbol = asset_contract[item.asset]
    if item.market not in _MARKET_DIRECTION:
        raise ValueError(f"{identity} has unsupported market {item.market}")
    expected_symbol = spot_symbol if item.market == "spot" else futures_symbol
    expected_family = "breakout_long" if item.market == "spot" else "breakdown_short"
    if (
        item.cohort != cohort
        or item.symbol != expected_symbol
        or item.direction != _MARKET_DIRECTION[item.market]
        or item.family != expected_family
    ):
        raise ValueError(f"{identity} violates the frozen market/asset contract")
    if not item.eligible:
        raise ValueError(f"{identity} is not an eligible frozen C0 trigger")
    if item.execution_observed or item.full_r2_eligible is not None:
        raise ValueError(f"{identity} contradicts the no-historical-BBO R3 boundary")
    if item.decision_time_ms % _INTERVAL_MS != _INTERVAL_MS - 1:
        raise ValueError(f"{identity} decision is not a closed 5-minute candle")
    if item.next_open_time_ms != item.decision_time_ms + 1:
        raise ValueError(f"{identity} does not enter at the next 5-minute open")
    if spec.split_name(item.next_open_time_ms) != item.split:
        raise ValueError(f"{identity} split provenance does not match next-open time")
    if item.regime not in {"risk_on", "neutral", "risk_off"}:
        raise ValueError(f"{identity} has an invalid market regime")
    if item.btc_trend not in {"bullish", "neutral", "bearish"}:
        raise ValueError(f"{identity} has an invalid BTC trend")
    if not 0 <= item.breadth_ratio <= 1:
        raise ValueError(f"{identity} has breadth outside [0, 1]")
    if item.analysis_eligible != item.analysis_eligible_by_horizon[2]:
        raise ValueError(f"{identity} legacy/common eligibility does not match H12")
    if item.analysis_exclusion != item.analysis_exclusion_by_horizon[2]:
        raise ValueError(f"{identity} legacy/common exclusion does not match H12")
    if len(set(item.analysis_eligible_by_horizon)) != 1:
        raise ValueError(f"{identity} is not on the common 12-bar panel")
    if len(set(item.analysis_exclusion_by_horizon)) != 1:
        raise ValueError(f"{identity} horizon exclusions drift on the common panel")
    if item.analysis_eligible_72 == bool(item.analysis_exclusion_72):
        raise ValueError(f"{identity} has inconsistent T72 eligibility/exclusion provenance")
    containing_split = next(split for split in spec.splits if split.name == item.split)
    split_start_ms = int(containing_split.start.timestamp() * 1000)
    split_end_ms = int(containing_split.end.timestamp() * 1000)
    expected_72_eligible = (
        item.next_open_time_ms >= split_start_ms + 72 * _INTERVAL_MS
        and item.next_open_time_ms + 72 * _INTERVAL_MS < split_end_ms
    )
    if item.analysis_eligible_72 != expected_72_eligible:
        raise ValueError(
            f"{identity} T72 eligibility disagrees with the frozen gap-free time bounds"
        )
    if item.analysis_eligible:
        if item.next_open_time_ms < split_start_ms + 12 * _INTERVAL_MS:
            raise ValueError(f"{identity} violates the 12-bar split-start embargo")
        if item.next_open_time_ms + 12 * _INTERVAL_MS - 1 >= split_end_ms:
            raise ValueError(f"{identity} eligible H12 outcome crosses a split")

    for index, horizon in enumerate(_HORIZONS):
        eligible = item.analysis_eligible_by_horizon[index]
        values = (
            item.forward_returns[index],
            item.long_net_returns[index],
            item.short_net_returns[index],
            item.signal_gross_returns[index],
            item.signal_fee_returns[index],
            item.signal_slippage_returns[index],
            item.signal_funding_returns[index],
            item.signal_net_returns[index],
        )
        label = item.outcome_labels[index]
        exclusion = item.analysis_exclusion_by_horizon[index]
        if not eligible:
            if not exclusion or label is not None or any(value is not None for value in values):
                raise ValueError(f"{identity} has fail-open ineligible H{horizon} outcomes")
            continue
        if exclusion or label is None or any(value is None for value in values):
            raise ValueError(f"{identity} has incomplete eligible H{horizon} outcomes")
        forward, long_net, short_net, gross, fee, slippage, funding, net = (
            _present_float(value, f"H{horizon} outcome") for value in values
        )
        assert label is not None
        expected_label = classify_kline_proxy_outcome(
            long_net,
            short_net,
            spec.outcome_edge_margin_bps / 10_000,
        ).value
        if label != expected_label:
            raise ValueError(f"{identity} has an incorrect H{horizon} outcome label")
        if not _close(forward, gross):
            raise ValueError(f"{identity} H{horizon} forward/gross parity failed")
        if fee < 0 or slippage < 0:
            raise ValueError(f"{identity} H{horizon} has a negative execution cost")
        expected_net = gross - fee - slippage + funding
        if not _close(net, expected_net):
            raise ValueError(f"{identity} H{horizon} net-return arithmetic failed")
        allowed_net = long_net if item.market == "spot" else short_net
        if not _close(net, allowed_net):
            raise ValueError(f"{identity} H{horizon} allowed-direction parity failed")

        underlying_return = gross if item.market == "spot" else -gross
        if 1 + underlying_return <= 0:
            raise ValueError(f"{identity} H{horizon} implies a nonpositive exit price")
        fee_bps, slippage_bps = _frozen_cost_bps(
            spec, item.market, item.cohort
        )
        long_execution = _expected_execution(
            "long", 1.0, 1 + underlying_return, fee_bps, slippage_bps
        )
        short_execution = _expected_execution(
            "short", 1.0, 1 + underlying_return, fee_bps, slippage_bps
        )
        signal_execution = (
            long_execution if item.market == "spot" else short_execution
        )
        if item.market == "spot":
            if not _close(funding, 0.0):
                raise ValueError(f"{identity} Spot H{horizon} has nonzero funding")
            long_funding = 0.0
            short_funding = 0.0
        else:
            short_funding = funding
            long_funding = -funding
        expected_long_net = long_execution.net_before_funding + long_funding
        expected_short_net = short_execution.net_before_funding + short_funding
        parity = (
            (gross, signal_execution.gross_return, "gross"),
            (fee, signal_execution.fee_return, "fee"),
            (slippage, signal_execution.slippage_return, "slippage"),
            (net, signal_execution.net_before_funding + funding, "net"),
            (long_net, expected_long_net, "long counterfactual net"),
            (short_net, expected_short_net, "short counterfactual net"),
        )
        for observed, expected, field in parity:
            if not _close(observed, expected):
                raise ValueError(
                    f"{identity} H{horizon} frozen {field} cost parity failed"
                )

    expected_f60 = (
        item.signal_gross_returns[2],
        item.signal_fee_returns[2],
        item.signal_slippage_returns[2],
        item.signal_funding_returns[2],
        item.signal_net_returns[2],
    )
    if item.f60_execution_model != "next_5m_open_to_12th_close_kline_proxy":
        raise ValueError(f"{identity} has an unexpected F60 execution model")
    for legacy, current in zip(item.f60_components, expected_f60, strict=True):
        if legacy is None or current is None:
            if legacy is not current:
                raise ValueError(f"{identity} F60/H12 missing-value parity failed")
        elif not _close(legacy, current):
            raise ValueError(f"{identity} F60/H12 numeric parity failed")


def _validate_trade(
    item: R3TechnicalTrade,
    opportunity: R3Opportunity,
    spec: BacktestSpec,
) -> None:
    identity = item.trade_id
    if item.protocol_version != spec.protocol_version or item.rule_version != spec.rule_version:
        raise ValueError(f"{identity} has protocol/rule version drift")
    if (
        item.asset != opportunity.asset
        or item.cohort != opportunity.cohort
        or item.market != opportunity.market
        or item.symbol != opportunity.symbol
        or item.direction != opportunity.direction
        or item.family != opportunity.family
        or item.decision_time_ms != opportunity.decision_time_ms
        or item.split != opportunity.split
    ):
        raise ValueError(f"{identity} does not match its originating opportunity")
    expected_trade_id = hashlib.sha256(
        "|".join(
            (
                item.protocol_version,
                item.entry_signal_id,
                str(item.entry_time_ms),
                str(item.exit_time_ms),
                item.exit_reason,
            )
        ).encode()
    ).hexdigest()[:24]
    if item.trade_id != expected_trade_id:
        raise ValueError(f"{identity} has a non-deterministic trade_id")
    if item.entry_time_ms != opportunity.next_open_time_ms:
        raise ValueError(f"{identity} did not enter at its frozen next open")
    if not opportunity.analysis_eligible_72:
        raise ValueError(f"{identity} originates from an ineligible T72 opportunity")
    if item.exit_time_ms < item.entry_time_ms:
        raise ValueError(f"{identity} exits before entry")
    if not 1 <= item.bars_held <= 72:
        raise ValueError(f"{identity} bars_held is outside the realizable 1..72 range")
    if item.entry_time_ms % _INTERVAL_MS:
        raise ValueError(f"{identity} entry is not on a 5-minute open")
    exit_clock = item.exit_time_ms % _INTERVAL_MS
    exit_on_open = exit_clock == 0
    exit_on_close = exit_clock == _INTERVAL_MS - 1
    if not (exit_on_open or exit_on_close):
        raise ValueError(f"{identity} exit is neither a 5-minute open nor close")
    allowed_reasons = {
        "initial_stop",
        "trailing_stop",
        "opposite_signal",
        "trend_failure",
        "time_exit",
    }
    if item.exit_reason not in allowed_reasons:
        raise ValueError(f"{identity} has an unknown technical exit reason")
    open_only = {
        "opposite_signal",
        "trend_failure",
        "time_exit",
    }
    if item.exit_reason in open_only and not exit_on_open:
        raise ValueError(f"{identity} exit reason requires next-open execution")
    adjusted_exit_ms = item.exit_time_ms + int(exit_on_close)
    elapsed_ms = adjusted_exit_ms - item.entry_time_ms
    if elapsed_ms < 0 or elapsed_ms % _INTERVAL_MS:
        raise ValueError(f"{identity} has an invalid 5-minute elapsed clock")
    expected_elapsed_ms = item.bars_held * _INTERVAL_MS
    if elapsed_ms != expected_elapsed_ms:
        raise ValueError(f"{identity} bars_held disagrees with its 5-minute exit clock")
    if item.exit_reason == "time_exit" and item.bars_held != 72:
        raise ValueError(f"{identity} time_exit must occur after exactly 72 held bars")

    if (
        item.entry_price <= 0
        or item.exit_price <= 0
        or item.entry_execution_price <= 0
        or item.exit_execution_price <= 0
    ):
        raise ValueError(f"{identity} has a nonpositive execution price")
    if item.fee_return < 0 or item.slippage_return < 0:
        raise ValueError(f"{identity} has a negative execution cost")
    fee_bps, slippage_bps = _frozen_cost_bps(spec, item.market, item.cohort)
    execution = _expected_execution(
        item.direction,
        item.entry_price,
        item.exit_price,
        fee_bps,
        slippage_bps,
    )
    execution_parity = (
        (item.entry_execution_price, execution.entry_execution_price, "entry execution"),
        (item.exit_execution_price, execution.exit_execution_price, "exit execution"),
        (item.gross_return, execution.gross_return, "gross return"),
        (item.slippage_return, execution.slippage_return, "slippage"),
        (item.fee_return, execution.fee_return, "fee"),
    )
    for observed, expected, field in execution_parity:
        if not _close(observed, expected):
            raise ValueError(f"{identity} frozen technical {field} parity failed")
    if item.market == "spot" and not _close(item.funding_return, 0.0):
        raise ValueError(f"{identity} Spot technical trade has nonzero funding")
    expected_net = execution.net_before_funding + item.funding_return
    if not _close(item.net_return, expected_net):
        raise ValueError(f"{identity} net-return arithmetic/cost parity failed")

    entry_split = next(split for split in spec.splits if split.name == item.split)
    split_end_ms = int(entry_split.end.timestamp() * 1000)
    if not item.split_contained:
        raise ValueError(f"{identity} has a non-contained R3 technical exit")
    if item.exit_time_ms >= split_end_ms:
        raise ValueError(f"{identity} claims containment past its split")


def validate_r3_integrity(
    opportunities: Sequence[R3Opportunity],
    trades: Sequence[R3TechnicalTrade],
    spec: BacktestSpec,
) -> dict[str, Any]:
    """Validate identities, causal-panel outputs, arithmetic, and T72 provenance."""

    if not opportunities:
        raise ValueError("R3 opportunity artifact is empty")
    asset_contract = {
        asset.asset: (asset.cohort, asset.spot_symbol, asset.futures_symbol)
        for asset in spec.assets
    }
    ids: set[str] = set()
    by_id: dict[str, R3Opportunity] = {}
    for item in opportunities:
        if item.opportunity_id in ids:
            raise ValueError(f"duplicate opportunity_id {item.opportunity_id}")
        ids.add(item.opportunity_id)
        _validate_opportunity(item, spec, asset_contract)
        by_id[item.opportunity_id] = item

    observed_panel = {(item.market, item.asset) for item in opportunities}
    expected_panel = {
        (market, asset.asset)
        for market in _MARKET_DIRECTION
        for asset in spec.assets
    }
    if observed_panel != expected_panel:
        raise ValueError("R3 opportunity artifact does not contain the full asset/market panel")

    trade_ids: set[str] = set()
    traded_opportunities: set[str] = set()
    for trade in trades:
        if trade.trade_id in trade_ids:
            raise ValueError(f"duplicate trade_id {trade.trade_id}")
        if trade.opportunity_id in traded_opportunities:
            raise ValueError(f"multiple sequential trades map to {trade.opportunity_id}")
        opportunity = by_id.get(trade.opportunity_id)
        if opportunity is None:
            raise ValueError(f"trade {trade.trade_id} maps to an unknown opportunity_id")
        trade_ids.add(trade.trade_id)
        traded_opportunities.add(trade.opportunity_id)
        _validate_trade(trade, opportunity, spec)
    return {
        "valid": True,
        "opportunities": len(opportunities),
        "technical_trades": len(trades),
        "unique_opportunity_ids": len(ids),
        "unique_trade_ids": len(trade_ids),
        "asset_market_cells": len(observed_panel),
    }


def _profit_factor(values: Sequence[float]) -> tuple[float | None, str]:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses > 0:
        return gains / losses, "FINITE"
    if gains > 0:
        return None, "POSITIVE_WITH_NO_LOSSES"
    return None, "UNDEFINED_NO_GAINS_OR_LOSSES"


def _summary_from_rows(
    rows: Sequence[R3Opportunity],
    horizon: int,
    *,
    market: str,
    notional_usdt: float,
) -> dict[str, Any]:
    index = _HORIZONS.index(horizon)
    values = [
        _present_float(item.signal_net_returns[index], "signal_net_return")
        for item in rows
    ]
    gross = [
        _present_float(item.signal_gross_returns[index], "signal_gross_return")
        for item in rows
    ]
    fees = [
        _present_float(item.signal_fee_returns[index], "signal_fee_return")
        for item in rows
    ]
    slippage = [
        _present_float(item.signal_slippage_returns[index], "signal_slippage_return")
        for item in rows
    ]
    funding = [
        _present_float(item.signal_funding_returns[index], "signal_funding_return")
        for item in rows
    ]
    long_net = [
        _present_float(item.long_net_returns[index], "long_net_return") for item in rows
    ]
    short_net = [
        _present_float(item.short_net_returns[index], "short_net_return")
        for item in rows
    ]
    labels = Counter(str(item.outcome_labels[index]) for item in rows)
    count = len(rows)
    total = sum(values)
    pf, pf_state = _profit_factor(values)
    zero_slippage = [g - f + u for g, f, u in zip(gross, fees, funding, strict=True)]
    double_slippage = [
        g - f - 2 * s + u
        for g, f, s, u in zip(gross, fees, slippage, funding, strict=True)
    ]
    label_counts = {label: labels[label] for label in _LABELS}
    allowed_label = _MARKET_LABEL[market]
    opposite_label = (
        "KLINE_PROXY_SHORT" if market == "spot" else "KLINE_PROXY_LONG"
    )
    return {
        "horizon_bars": horizon,
        "horizon_minutes": horizon * 5,
        "eligible_opportunities": count,
        "represented_utc_days": len({item.utc_day for item in rows}),
        "total_net_return": total,
        "total_net_bps": total * 10_000,
        "mean_net_return": total / count if count else None,
        "mean_net_bps": total * 10_000 / count if count else None,
        "fixed_notional_pnl_usdt": total * notional_usdt,
        "profit_factor": pf,
        "profit_factor_state": pf_state,
        "win_rate": sum(value > 0 for value in values) / count if count else None,
        "loss_rate": sum(value < 0 for value in values) / count if count else None,
        "cost_decomposition": {
            "summed_gross_return": sum(gross),
            "summed_fee_cost": sum(fees),
            "summed_adverse_slippage_cost": sum(slippage),
            "summed_funding_pnl": sum(funding),
            "summed_gross_pnl_usdt": sum(gross) * notional_usdt,
            "summed_fee_cost_usdt": sum(fees) * notional_usdt,
            "summed_adverse_slippage_cost_usdt": sum(slippage) * notional_usdt,
            "summed_funding_pnl_usdt": sum(funding) * notional_usdt,
            "mean_gross_bps": statistics.fmean(gross) * 10_000 if count else None,
            "mean_fee_bps": statistics.fmean(fees) * 10_000 if count else None,
            "mean_adverse_slippage_bps": (
                statistics.fmean(slippage) * 10_000 if count else None
            ),
            "mean_funding_bps": statistics.fmean(funding) * 10_000 if count else None,
        },
        "slippage_sensitivity": {
            "zero_x_mean_net_return": statistics.fmean(zero_slippage) if count else None,
            "zero_x_mean_net_bps": (
                statistics.fmean(zero_slippage) * 10_000 if count else None
            ),
            "one_x_mean_net_return": total / count if count else None,
            "one_x_mean_net_bps": total * 10_000 / count if count else None,
            "two_x_mean_net_return": statistics.fmean(double_slippage) if count else None,
            "two_x_mean_net_bps": (
                statistics.fmean(double_slippage) * 10_000 if count else None
            ),
        },
        "research_counterfactuals": {
            "spot_short_semantics": (
                "decline/no-new-Spot-long or existing-holding exit warning; "
                "never a new Spot short order"
            ),
            "summed_long_net_return": sum(long_net),
            "long_fixed_notional_pnl_usdt": sum(long_net) * notional_usdt,
            "mean_long_net_return": statistics.fmean(long_net) if count else None,
            "mean_long_net_bps": statistics.fmean(long_net) * 10_000 if count else None,
            "summed_short_net_return": sum(short_net),
            "short_fixed_notional_pnl_usdt": sum(short_net) * notional_usdt,
            "mean_short_net_return": statistics.fmean(short_net) if count else None,
            "mean_short_net_bps": statistics.fmean(short_net) * 10_000 if count else None,
        },
        "label_counts": label_counts,
        "label_prevalence": {
            label: label_counts[label] / count if count else None for label in _LABELS
        },
        "directional_label_rates": {
            "allowed_direction_label": allowed_label,
            "directional_hit_rate": label_counts[allowed_label] / count if count else None,
            "abstention_rate": label_counts["KLINE_PROXY_FLAT"] / count if count else None,
            "opposite_direction_rate": (
                label_counts[opposite_label] / count if count else None
            ),
        },
    }


def _eligible_rows(
    opportunities: Sequence[R3Opportunity], market: str, horizon: int
) -> list[R3Opportunity]:
    index = _HORIZONS.index(horizon)
    return [
        item
        for item in opportunities
        if item.market == market and item.analysis_eligible_by_horizon[index]
    ]


def _grouped_summaries(
    rows: Sequence[R3Opportunity],
    horizon: int,
    field: Callable[[R3Opportunity], str],
    *,
    market: str,
    notional_usdt: float,
    expected_values: Sequence[str] = (),
) -> list[dict[str, Any]]:
    grouped: defaultdict[str, list[R3Opportunity]] = defaultdict(list)
    for value in expected_values:
        grouped[value]
    for row in rows:
        grouped[field(row)].append(row)
    return [
        {
            "value": value,
            **_summary_from_rows(
                group,
                horizon,
                market=market,
                notional_usdt=notional_usdt,
            ),
        }
        for value, group in sorted(grouped.items())
    ]


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("cannot calculate a quantile from an empty sample")
    ordered = sorted(values)
    position = min(1.0, max(0.0, probability)) * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _rolling_circular_sum(values: Sequence[float | int], length: int) -> list[float]:
    count = len(values)
    return [
        float(sum(values[(start + offset) % count] for offset in range(length)))
        for start in range(count)
    ]


def _calendar_arrays(
    rows: Sequence[R3Opportunity], start_day: int, day_count: int
) -> tuple[list[float], list[int]]:
    sums = [0.0] * day_count
    counts = [0] * day_count
    for row in rows:
        offset = row.utc_day - start_day
        if offset < 0 or offset >= day_count:
            raise ValueError("eligible opportunity lies outside the evaluation calendar")
        value = row.signal_net_returns[2]
        if value is None:
            raise ValueError("eligible primary opportunity has no signal net return")
        sums[offset] += value
        counts[offset] += 1
    return sums, counts


def shared_calendar_moving_block_bootstrap(
    primary_rows: Mapping[str, Sequence[R3Opportunity]],
    *,
    calendar_start_day: int,
    calendar_day_count: int,
    samples: int,
    seed: int,
    block_days: int,
) -> dict[str, Any]:
    """Bootstrap UTC days with one shared circular draw schedule for both markets."""

    if set(primary_rows) != set(_MARKET_DIRECTION):
        raise ValueError("shared bootstrap requires exactly Spot and Futures panels")
    if calendar_day_count <= 0 or samples <= 0 or block_days <= 0:
        raise ValueError("calendar, samples, and block length must be positive")
    arrays = {
        market: _calendar_arrays(rows, calendar_start_day, calendar_day_count)
        for market, rows in primary_rows.items()
    }
    full_blocks, remainder = divmod(calendar_day_count, block_days)
    block_lengths = [block_days] * full_blocks + ([remainder] if remainder else [])
    aggregates: dict[str, dict[int, tuple[list[float], list[float]]]] = {}
    for market, (daily_sums, daily_counts) in arrays.items():
        aggregates[market] = {
            length: (
                _rolling_circular_sum(daily_sums, length),
                _rolling_circular_sum(daily_counts, length),
            )
            for length in set(block_lengths)
        }

    rng = random.Random(seed)
    estimates: dict[str, list[float]] = {market: [] for market in _MARKET_DIRECTION}
    invalid = Counter[str]()
    digest = hashlib.sha256()
    packer = struct.Struct(f"<{len(block_lengths)}H")
    for _ in range(samples):
        starts = [rng.randrange(calendar_day_count) for _ in block_lengths]
        digest.update(packer.pack(*starts))
        for market in _MARKET_DIRECTION:
            total_sum = 0.0
            total_count = 0.0
            for start, length in zip(starts, block_lengths, strict=True):
                block_sums, block_counts = aggregates[market][length]
                total_sum += block_sums[start]
                total_count += block_counts[start]
            if total_count <= 0:
                invalid[market] += 1
            else:
                estimates[market].append(total_sum / total_count)

    result: dict[str, Any] = {
        "block_days": block_days,
        "samples": samples,
        "seed": seed,
        "two_sided_interval_method": "percentile_moving_block_bootstrap",
        "one_sided_lower_method": "basic_centered_moving_block_bootstrap",
        "one_sided_p_value_method": "centered_bootstrap_error_tail",
        "calendar_days": calendar_day_count,
        "zero_days_by_market": {
            market: sum(count == 0 for count in arrays[market][1])
            for market in _MARKET_DIRECTION
        },
        "shared_draw_schedule_sha256": digest.hexdigest(),
        "markets": {},
    }
    for market in _MARKET_DIRECTION:
        point_values = [
            _present_float(item.signal_net_returns[2], "signal_net_return_12")
            for item in primary_rows[market]
        ]
        point = statistics.fmean(point_values) if point_values else None
        valid = estimates[market]
        invalid_count = invalid[market]
        market_result: dict[str, Any] = {
            "point_estimate": point,
            "valid_replicates": len(valid),
            "invalid_replicates": invalid_count,
            "invalid_rate": invalid_count / samples,
            "monte_carlo_resolution": 1 / (len(valid) + 1) if valid else None,
            "two_sided_95_interval": None,
            "one_sided_basic_95_lower": None,
            "one_sided_p_value": None,
        }
        if point is not None and valid:
            market_result.update(
                {
                    "two_sided_95_interval": [
                        _quantile(valid, 0.025),
                        _quantile(valid, 0.975),
                    ],
                    "one_sided_basic_95_lower": 2 * point - _quantile(valid, 0.95),
                    "one_sided_p_value": (
                        1
                        if point <= 0
                        else (
                            1
                            + sum(value - point >= point for value in valid)
                        )
                        / (len(valid) + 1)
                    ),
                }
            )
        result["markets"][market] = market_result
    return result


def _holm_two(p_values: Mapping[str, float]) -> dict[str, dict[str, Any]]:
    if set(p_values) != set(_MARKET_DIRECTION):
        raise ValueError("Holm family must contain exactly the two primary markets")
    ordered = sorted(p_values.items(), key=lambda item: (item[1], item[0]))
    running = 0.0
    chain = True
    output: dict[str, dict[str, Any]] = {}
    for index, (market, value) in enumerate(ordered):
        remaining = len(ordered) - index
        local_alpha = 0.05 / remaining
        rejected = chain and value <= local_alpha
        if not rejected:
            chain = False
        running = max(running, min(1.0, remaining * value))
        output[market] = {
            "rank": index + 1,
            "raw_p_value": value,
            "adjusted_p_value": running,
            "local_alpha": local_alpha,
            "rejected": rejected,
        }
    return {market: output[market] for market in sorted(output)}


def _profit_factor_pass(summary: Mapping[str, Any]) -> bool:
    value = summary["profit_factor"]
    return bool(
        (isinstance(value, (float, int)) and value > 1.05)
        or summary["profit_factor_state"] == "POSITIVE_WITH_NO_LOSSES"
    )


def evaluate_frozen_r3_screen(
    primary: Mapping[str, Mapping[str, Any]],
    uncertainty: Mapping[str, Mapping[str, Any]],
    asset_concentration: Mapping[str, Mapping[str, Any]],
) -> tuple[R3Efficacy, dict[str, Any]]:
    """Apply the frozen R3 screen without promoting a secondary horizon."""

    market_criteria: dict[str, dict[str, bool]] = {}
    any_adverse = False
    information_ok = True
    invalid_bootstrap = False
    for market in _MARKET_DIRECTION:
        summary = primary[market]
        boot = uncertainty[market]
        concentration = asset_concentration[market]
        mean = summary["mean_net_return"]
        lower = boot["one_sided_basic_95_lower"]
        two_x = summary["slippage_sensitivity"]["two_x_mean_net_return"]
        criteria = {
            "eligible_opportunities_at_least_500": summary["eligible_opportunities"] >= 500,
            "represented_utc_days_at_least_120": summary["represented_utc_days"] >= 120,
            "mean_net_return_greater_than_5_bps": mean is not None and mean > 0.0005,
            "seven_day_basic_lower_greater_than_zero": lower is not None and lower > 0,
            "profit_factor_greater_than_1_05": _profit_factor_pass(summary),
            "two_x_slippage_mean_nonnegative": two_x is not None and two_x >= 0,
            "positive_assets_at_least_6_of_8": concentration["positive_assets"] >= 6,
            "maximum_positive_asset_concentration_at_most_35_percent": (
                concentration["maximum_positive_concentration"] is not None
                and concentration["maximum_positive_concentration"] <= 0.35
            ),
        }
        market_criteria[market] = criteria
        information_ok = information_ok and criteria[
            "eligible_opportunities_at_least_500"
        ] and criteria["represented_utc_days_at_least_120"]
        invalid_bootstrap = invalid_bootstrap or boot["invalid_rate"] > 0.001
        any_adverse = any_adverse or (mean is not None and mean <= 0)

    all_pass = all(all(values.values()) for values in market_criteria.values())
    if any_adverse:
        status: R3Efficacy = "EXPLORATORY_FAIL"
        reason = "at least one primary market has nonpositive mean net expectancy"
    elif invalid_bootstrap or not information_ok:
        status = "INCONCLUSIVE_LOW_INFORMATION"
        reason = "bootstrap validity or frozen information thresholds are insufficient"
    elif all_pass:
        status = "EXPLORATORY_SCREEN_PASS"
        reason = "both primary markets meet every frozen exposed-sample screen criterion"
    else:
        status = "EXPLORATORY_FAIL"
        reason = "adequately sized exposed sample fails at least one frozen screen criterion"
    return status, {
        "status_reason": reason,
        "markets": market_criteria,
        "both_markets_all_criteria": all_pass,
        "information_thresholds_met": information_ok,
        "bootstrap_invalidity_within_0_1_percent": not invalid_bootstrap,
    }


def _asset_concentration(
    rows: Sequence[R3Opportunity], expected_assets: Sequence[str]
) -> dict[str, Any]:
    contributions = {asset: 0.0 for asset in expected_assets}
    for row in rows:
        value = row.signal_net_returns[2]
        if value is not None:
            contributions[row.asset] += value
    positive = {asset: value for asset, value in contributions.items() if value > 0}
    total_positive = sum(positive.values())
    return {
        "summed_net_contribution_by_asset": dict(sorted(contributions.items())),
        "positive_assets": len(positive),
        "total_positive_contribution": total_positive,
        "maximum_positive_concentration": (
            max(positive.values()) / total_positive if total_positive > 0 else None
        ),
    }


def _technical_summary(
    trades: Sequence[R3TechnicalTrade], market: str, notional_usdt: float
) -> dict[str, Any]:
    rows = [trade for trade in trades if trade.market == market]
    values = [trade.net_return for trade in rows]
    pf, pf_state = _profit_factor(values)
    count = len(rows)
    return {
        "market": market,
        "estimand": "sequential_non_independent_T72_technical_exit",
        "role": "SECONDARY_DESCRIPTIVE_ONLY",
        "trades": count,
        "total_net_return": sum(values),
        "mean_net_return": statistics.fmean(values) if count else None,
        "mean_net_bps": statistics.fmean(values) * 10_000 if count else None,
        "fixed_notional_pnl_usdt": sum(values) * notional_usdt,
        "profit_factor": pf,
        "profit_factor_state": pf_state,
        "win_rate": sum(value > 0 for value in values) / count if count else None,
        "mean_bars_held": statistics.fmean(trade.bars_held for trade in rows) if count else None,
        "exit_reason_counts": dict(sorted(Counter(trade.exit_reason for trade in rows).items())),
    }


def _integrity_failure(reason: str) -> dict[str, Any]:
    return {
        "protocol_version": _PROTOCOL_VERSION,
        "scientific_scope": "EXPOSED_SAMPLE_DIAGNOSTIC_NOT_HOLDOUT",
        "status_axes": {
            "data_integrity": "FAIL",
            "kline_proxy_efficacy": "INCONCLUSIVE_LOW_INFORMATION",
            "execution_validity": "INCONCLUSIVE_NO_HISTORICAL_BBO",
            "generalization": "INCONCLUSIVE_NO_UNTOUCHED_OOS",
            "deployment": "NOT_APPROVED",
        },
        "integrity": {"valid": False, "reason": reason},
        "primary_60m": {},
        "market_horizon_summaries": [],
        "breakdowns": {},
        "bootstrap": {},
        "technical_exit_72": [],
        "screen": {},
    }


def _analyze_r3_diagnostic(
    opportunities: Sequence[R3Opportunity],
    trades: Sequence[R3TechnicalTrade],
    spec: BacktestSpec,
    *,
    bootstrap_samples: int,
    bootstrap_seed: int,
) -> dict[str, Any]:
    """Internal analysis core; tests may supply small deterministic resample counts."""

    try:
        integrity = validate_r3_integrity(opportunities, trades, spec)
    except ValueError as exc:
        return _integrity_failure(str(exc))

    samples = bootstrap_samples
    seed = bootstrap_seed
    notional = spec.costs.notional_usdt
    market_horizon: list[dict[str, Any]] = []
    breakdowns: dict[str, Any] = {}
    rows_by_market_horizon: dict[tuple[str, int], list[R3Opportunity]] = {}
    for market in _MARKET_DIRECTION:
        for horizon in _HORIZONS:
            rows = _eligible_rows(opportunities, market, horizon)
            rows_by_market_horizon[(market, horizon)] = rows
            summary = _summary_from_rows(
                rows,
                horizon,
                market=market,
                notional_usdt=notional,
            )
            market_horizon.append({"market": market, **summary})
            key = f"{market}_{horizon}"
            breakdowns[key] = {
                "asset": _grouped_summaries(
                    rows,
                    horizon,
                    lambda item: item.asset,
                    market=market,
                    notional_usdt=notional,
                    expected_values=[asset.asset for asset in spec.assets],
                ),
                "split": _grouped_summaries(
                    rows,
                    horizon,
                    lambda item: item.split,
                    market=market,
                    notional_usdt=notional,
                    expected_values=[split.name for split in spec.splits],
                ),
                "regime": _grouped_summaries(
                    rows,
                    horizon,
                    lambda item: item.regime,
                    market=market,
                    notional_usdt=notional,
                    expected_values=("neutral", "risk_off", "risk_on"),
                ),
                "btc_trend": _grouped_summaries(
                    rows,
                    horizon,
                    lambda item: item.btc_trend,
                    market=market,
                    notional_usdt=notional,
                    expected_values=("bearish", "neutral", "bullish"),
                ),
            }

    primary = {
        market: next(
            summary
            for summary in market_horizon
            if summary["market"] == market and summary["horizon_bars"] == 12
        )
        for market in _MARKET_DIRECTION
    }
    evaluation_start_ms = int(spec.evaluation_start.timestamp() * 1000)
    evaluation_end_ms = int(spec.evaluation_end.timestamp() * 1000)
    if evaluation_start_ms % _DAY_MS or evaluation_end_ms % _DAY_MS:
        return _integrity_failure("R3 bootstrap calendar boundaries must be UTC midnights")
    calendar_start_day = evaluation_start_ms // _DAY_MS
    calendar_day_count = (evaluation_end_ms - evaluation_start_ms) // _DAY_MS
    bootstrap: dict[str, Any] = {}
    for block_days in _BLOCK_DAYS:
        bootstrap[str(block_days)] = shared_calendar_moving_block_bootstrap(
            {
                market: rows_by_market_horizon[(market, _PRIMARY_HORIZON)]
                for market in _MARKET_DIRECTION
            },
            calendar_start_day=calendar_start_day,
            calendar_day_count=calendar_day_count,
            samples=samples,
            seed=seed,
            block_days=block_days,
        )
    primary_uncertainty = bootstrap[str(_PRIMARY_BLOCK_DAYS)]["markets"]
    raw_p = {
        market: primary_uncertainty[market]["one_sided_p_value"]
        for market in _MARKET_DIRECTION
    }
    if all(isinstance(value, (float, int)) for value in raw_p.values()):
        holm = _holm_two({market: float(value) for market, value in raw_p.items()})
        for market in _MARKET_DIRECTION:
            primary_uncertainty[market]["holm"] = holm[market]

    concentration = {
        market: _asset_concentration(
            rows_by_market_horizon[(market, 12)],
            [asset.asset for asset in spec.assets],
        )
        for market in _MARKET_DIRECTION
    }
    efficacy, screen = evaluate_frozen_r3_screen(
        primary, primary_uncertainty, concentration
    )
    fragility = {
        market: {
            str(block): (
                bootstrap[str(block)]["markets"][market]["one_sided_basic_95_lower"]
                is None
                or bootstrap[str(block)]["markets"][market]["one_sided_basic_95_lower"]
                <= 0
            )
            for block in (14, 28)
        }
        for market in _MARKET_DIRECTION
    }
    screen["asset_concentration"] = concentration
    screen["long_block_fragility_flags"] = fragility
    return {
        "protocol_version": spec.protocol_version,
        "rule_version": spec.rule_version,
        "scientific_scope": "EXPOSED_SAMPLE_DIAGNOSTIC_NOT_HOLDOUT",
        "status_axes": {
            "data_integrity": "PASS",
            "kline_proxy_efficacy": efficacy,
            "execution_validity": "INCONCLUSIVE_NO_HISTORICAL_BBO",
            "generalization": "INCONCLUSIVE_NO_UNTOUCHED_OOS",
            "deployment": "NOT_APPROVED",
        },
        "integrity": integrity,
        "analysis_contract": {
            "primary_horizon_bars": 12,
            "primary_horizon_minutes": 60,
            "secondary_horizons_bars": [3, 6],
            "bootstrap_samples": samples,
            "bootstrap_seed": seed,
            "bootstrap_block_days": list(_BLOCK_DAYS),
            "calendar_start_utc": spec.evaluation_start.isoformat(),
            "calendar_end_exclusive_utc": spec.evaluation_end.isoformat(),
            "calendar_days_including_zero_opportunity_days": calendar_day_count,
            "fixed_notional_usdt_per_standalone_opportunity_counterfactual": notional,
            "funding_provenance_scope": (
                "aggregate signed return bound to frozen per-symbol input-file hashes; "
                "row ledger does not store per-event funding IDs or digests"
            ),
        },
        "primary_60m": primary,
        "market_horizon_summaries": market_horizon,
        "breakdowns": breakdowns,
        "bootstrap": bootstrap,
        "technical_exit_72": [
            _technical_summary(trades, market, notional) for market in _MARKET_DIRECTION
        ],
        "screen": screen,
        "semantic_boundaries": {
            "report_precedence": (
                "r3_final_report_ko.md supersedes the engine's raw replay report.md"
            ),
            "spot_short_label": (
                "decline/no-new-Spot-long or existing-holding exit warning; "
                "never a new Spot short order"
            ),
            "futures_short": "USDⓈ-M Futures research short direction",
            "technical_exit_72": "sequential, non-independent, secondary descriptive output",
            "funding": (
                "aggregate funding is reproducible from frozen input/source hashes, but the "
                "row ledger does not independently identify each included funding event"
            ),
        },
    }


def analyze_r3_diagnostic(
    opportunities: Sequence[R3Opportunity],
    trades: Sequence[R3TechnicalTrade],
    spec: BacktestSpec,
) -> dict[str, Any]:
    """Run the immutable official R3 diagnostic (50,000 draws; seed 20260716)."""

    try:
        _validate_frozen_spec(spec)
    except ValueError as exc:
        return _integrity_failure(str(exc))
    return _analyze_r3_diagnostic(
        opportunities,
        trades,
        spec,
        bootstrap_samples=_BOOTSTRAP_SAMPLES,
        bootstrap_seed=_BOOTSTRAP_SEED,
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _require_digest(value: object, field: str) -> str:
    text = str(value).lower()
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise ValueError(f"{field} is not a SHA-256 digest")
    return text


def _validate_raw_results_contract(path: str | Path) -> dict[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("raw replay results root must be an object")
    if raw.get("artifact_role") != _R3_RAW_ARTIFACT_ROLE:
        raise ValueError("results.json lacks the frozen R3 raw-artifact role")
    if raw.get("r3_raw_replay_contract") != _R3_RAW_REPLAY_CONTRACT:
        raise ValueError("results.json has a tampered R3 raw-replay interpretation contract")
    return {
        "artifact_role": _R3_RAW_ARTIFACT_ROLE,
        "r3_raw_replay_contract": _R3_RAW_REPLAY_CONTRACT,
    }


def _validate_source_code_digest(declared: object, workspace: Path) -> str:
    from signalbot.backtest.runner import source_code_digest

    declared_digest = _require_digest(declared, "code_sha256")
    actual_digest = source_code_digest(workspace)
    if declared_digest != actual_digest:
        raise ValueError("run source code differs from the source used by the R3 analyzer")
    return actual_digest


def _verify_input_panel_ledger(
    ledger_path: str | Path,
    *,
    data_root: str | Path,
    manifest_inputs: Mapping[str, object],
    expected_paths: frozenset[str],
    expected_protocol: str,
    expected_data_root_label: str,
    expected_ledger_sha256: str,
) -> dict[str, Any]:
    ledger_source = Path(ledger_path)
    actual_ledger_sha = _sha256_file(ledger_source)
    if actual_ledger_sha != expected_ledger_sha256:
        raise ValueError("frozen input-panel ledger hash mismatch")
    raw = json.loads(ledger_source.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or set(raw) != {
        "data_root",
        "files",
        "protocol_version",
    }:
        raise ValueError("input-panel ledger root/schema drift")
    if raw["protocol_version"] != expected_protocol:
        raise ValueError("input-panel ledger protocol mismatch")
    if raw["data_root"] != expected_data_root_label:
        raise ValueError("input-panel ledger data_root mismatch")
    files = raw["files"]
    if not isinstance(files, dict) or set(files) != expected_paths:
        raise ValueError("input-panel ledger file set differs from the frozen panel")
    normalized_files = {
        str(name): _require_digest(digest, f"input ledger files[{name}]")
        for name, digest in files.items()
    }
    normalized_manifest = {
        str(name): _require_digest(digest, f"manifest inputs[{name}]")
        for name, digest in manifest_inputs.items()
    }
    if normalized_manifest != normalized_files:
        raise ValueError("run manifest inputs differ from the frozen input-panel ledger")

    root = Path(data_root).resolve()
    verified = 0
    for relative_path, expected_sha in sorted(normalized_files.items()):
        source = (root / relative_path).resolve()
        try:
            source.relative_to(root)
        except ValueError as exc:
            raise ValueError("input-panel ledger path escapes its data_root") from exc
        if not source.is_file():
            raise ValueError(f"frozen input-panel file is missing: {relative_path}")
        if _sha256_file(source) != expected_sha:
            raise ValueError(f"frozen input-panel file hash mismatch: {relative_path}")
        verified += 1
    return {
        "ledger_sha256": actual_ledger_sha,
        "data_root": expected_data_root_label,
        "verified_actual_input_files": verified,
        "files": normalized_files,
    }


def _validate_frozen_spec(spec: BacktestSpec) -> None:
    if (
        spec.protocol_version != _PROTOCOL_VERSION
        or spec.rule_version != _RULE_VERSION
        or spec.interval != "5m"
        or spec.strategy_mode != "pit_breakout_volume"
        or spec.candidate_policy != "c0_frozen"
        or spec.confirmation_mode != "explicit_trigger"
        or spec.opportunity_panel_horizon_bars != 12
        or spec.outcome_edge_margin_bps != 0
        or spec.minimum_age_days != 90
        or spec.minimum_history_bars != 210
        or spec.historical_spread_proxy_bps != 11.25
        or spec.entry_score != 80
        or spec.gate_use_participation
        or spec.gate_use_crowding
        or spec.gate_use_higher_timeframes
        or spec.trend_gate != 0
        or spec.participation_gate != 60
        or spec.crowding_risk_cap != 75
        or spec.execution_gate != 65
        or spec.completeness_gate != 70
        or spec.bootstrap.samples != _BOOTSTRAP_SAMPLES
        or spec.bootstrap.block_days != _PRIMARY_BLOCK_DAYS
        or spec.bootstrap.seed != _BOOTSTRAP_SEED
        or spec.exits.trend_failure_bars != 3
        or spec.exits.trailing_activation_r != 1.0
        or spec.exits.trailing_atr_multiple != 2.0
        or spec.exits.max_holding_bars != 72
        or tuple(asset.asset for asset in spec.assets) != _ASSETS
        or spec.include_rsi_reversals
        or spec.volume_feature_set != "none"
        or spec.costs.notional_usdt != 100.0
        or spec.costs.spot_fee_bps != 10.0
        or spec.costs.futures_fee_bps != 5.0
        or spec.costs.spot_slippage_bps
        != {"anchor": 5.0, "major": 5.0, "volatile": 10.0}
        or spec.costs.futures_slippage_bps
        != {"anchor": 3.0, "major": 3.0, "volatile": 8.0}
        or not spec.costs.include_funding
    ):
        raise ValueError("specification drifts from the frozen R3 protocol")
    if spec.data_start != datetime(2024, 3, 1, tzinfo=UTC):
        raise ValueError("R3 data-start date drifts from the frozen protocol")
    expected_asset_contract = (
        ("BTC", "anchor", "BTCUSDT", "BTCUSDT"),
        ("ETH", "anchor", "ETHUSDT", "ETHUSDT"),
        ("BNB", "major", "BNBUSDT", "BNBUSDT"),
        ("SOL", "major", "SOLUSDT", "SOLUSDT"),
        ("XRP", "major", "XRPUSDT", "XRPUSDT"),
        ("DOGE", "major", "DOGEUSDT", "DOGEUSDT"),
        ("SUI", "volatile", "SUIUSDT", "SUIUSDT"),
        ("WIF", "volatile", "WIFUSDT", "WIFUSDT"),
    )
    actual_asset_contract = tuple(
        (asset.asset, asset.cohort, asset.spot_symbol, asset.futures_symbol)
        for asset in spec.assets
    )
    if actual_asset_contract != expected_asset_contract:
        raise ValueError("R3 asset/cohort/symbol contract drifts from the frozen protocol")
    expected_dates = (
        datetime(2024, 7, 1, tzinfo=UTC),
        datetime(2026, 7, 1, tzinfo=UTC),
    )
    if (spec.evaluation_start, spec.evaluation_end) != expected_dates:
        raise ValueError("R3 evaluation window drifts from the frozen protocol")
    expected_splits = (
        ("development", datetime(2024, 7, 1, tzinfo=UTC), datetime(2025, 3, 1, tzinfo=UTC)),
        ("validation", datetime(2025, 3, 1, tzinfo=UTC), datetime(2025, 11, 1, tzinfo=UTC)),
        (
            "retrospective_test",
            datetime(2025, 11, 1, tzinfo=UTC),
            datetime(2026, 7, 1, tzinfo=UTC),
        ),
    )
    if tuple((split.name, split.start, split.end) for split in spec.splits) != expected_splits:
        raise ValueError("R3 split definitions drift from the frozen protocol")


def validate_r3_run_provenance(
    run_dir: str | Path,
    spec_path: str | Path,
    *,
    workspace_root: str | Path,
) -> dict[str, Any]:
    """Verify frozen spec/plan, input panel, and all engine artifact hashes."""

    run_root = Path(run_dir)
    workspace = Path(workspace_root).resolve()
    spec_source = Path(spec_path).resolve()
    spec = load_backtest_spec(spec_source)
    _validate_frozen_spec(spec)
    if _sha256_file(spec_source) != _FROZEN_SPEC_SHA256:
        raise ValueError("specification bytes differ from the pre-replay frozen file")
    manifest_path = run_root / "run_manifest.json"
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("run manifest root must be an object")
    if (
        raw.get("protocol_version") != spec.protocol_version
        or raw.get("rule_version") != spec.rule_version
    ):
        raise ValueError("run manifest protocol/rule version mismatch")
    if _require_digest(raw.get("spec_sha256"), "spec_sha256") != _sha256_file(spec_source):
        raise ValueError("run manifest specification hash mismatch")
    _validate_source_code_digest(raw.get("code_sha256"), workspace)
    for field in ("effective_settings_sha256", "config_input_sha256"):
        _require_digest(raw.get(field), field)
    if raw.get("config_input_sha256") != _FROZEN_SETTINGS_SHA256:
        raise ValueError(
            "settings input drifted from the frozen file containing breakout_lookback=20"
        )
    if raw.get("config_input_path") != "config/settings.example.yaml":
        raise ValueError("run did not use the frozen settings.example.yaml input")
    if _sha256_file(workspace / "config" / "settings.example.yaml") != _FROZEN_SETTINGS_SHA256:
        raise ValueError("local frozen settings file no longer matches the replay")
    environment = raw.get("environment")
    if not isinstance(environment, dict):
        raise ValueError("run manifest environment is missing")
    uv_lock_sha = _require_digest(
        environment.get("uv_lock_sha256"), "environment.uv_lock_sha256"
    )
    if uv_lock_sha != _sha256_file(workspace / "uv.lock"):
        raise ValueError("uv.lock differs from the dependency lock used by the replay")

    plan_path = workspace / str(spec.experiment_plan_path)
    plan_hash = _sha256_file(plan_path)
    if plan_hash != _FROZEN_PLAN_SHA256:
        raise ValueError("experiment-plan bytes differ from the pre-replay frozen file")
    if _require_digest(raw.get("experiment_plan_sha256"), "experiment_plan_sha256") != plan_hash:
        raise ValueError("run manifest experiment-plan hash mismatch")
    contract = raw.get("backtest_contract")
    if not isinstance(contract, dict) or contract != {
        "candidate_policy": "c0_frozen",
        "confirmation_mode": "explicit_trigger",
        "interval": "5m",
        "max_holding_bars": 72,
        "opportunity_panel_horizon_bars": 12,
        "outcome_edge_margin_bps": 0.0,
        "prediction_horizons_bars": [3, 6, 12],
        "prediction_entry": "next_contiguous_5m_open",
        "prediction_exit": "decision_index_plus_h_close",
        "outcome_labels": list(_LABELS),
    }:
        raise ValueError("run manifest backtest contract mismatch")

    expected_inputs = {
        f"{market}/{asset.asset}__{symbol}__5m.csv.gz"
        for asset in spec.assets
        for market, symbol in (
            ("spot", asset.spot_symbol),
            ("futures", asset.futures_symbol),
            ("funding", asset.futures_symbol),
        )
    }
    inputs = raw.get("inputs")
    if not isinstance(inputs, dict) or set(inputs) != expected_inputs:
        raise ValueError("run manifest does not contain the frozen 24-file input panel")
    input_ledger = _verify_input_panel_ledger(
        workspace / _FROZEN_INPUT_LEDGER_PATH,
        data_root=workspace / "data" / "backtest",
        manifest_inputs=inputs,
        expected_paths=frozenset(expected_inputs),
        expected_protocol=_PROTOCOL_VERSION,
        expected_data_root_label="data/backtest",
        expected_ledger_sha256=_FROZEN_INPUT_LEDGER_SHA256,
    )
    outputs = raw.get("outputs")
    if not isinstance(outputs, dict):
        raise ValueError("run manifest outputs are missing")
    for name in _RUN_OUTPUTS:
        declared = _require_digest(outputs.get(name), f"outputs[{name}]")
        if declared != _sha256_file(run_root / name):
            raise ValueError(f"run artifact hash mismatch: {name}")
    raw_results_contract = _validate_raw_results_contract(run_root / "results.json")
    return {
        "valid": True,
        "run_manifest_sha256": _sha256_file(manifest_path),
        "spec_sha256": _sha256_file(spec_source),
        "experiment_plan_sha256": plan_hash,
        "input_files": len(inputs),
        "input_panel_ledger_sha256": input_ledger["ledger_sha256"],
        "verified_actual_input_files": input_ledger[
            "verified_actual_input_files"
        ],
        "verified_engine_outputs": list(_RUN_OUTPUTS),
        "raw_results_contract": raw_results_contract,
    }


def _format_optional_number(value: object, *, digits: int = 4) -> str:
    if not isinstance(value, (float, int)) or isinstance(value, bool):
        return "NA"
    return f"{value:.{digits}f}"


def _format_profit_factor(summary: Mapping[str, Any]) -> str:
    value = summary["profit_factor"]
    if isinstance(value, (float, int)) and not isinstance(value, bool):
        return f"{value:.4f}"
    return str(summary["profit_factor_state"])


def _format_optional_percent(value: object) -> str:
    if not isinstance(value, (float, int)) or isinstance(value, bool):
        return "NA"
    return f"{value * 100:.2f}%"


def render_r3_report_ko(result: Mapping[str, Any]) -> str:
    """Render a compact Korean decision report; JSON retains all breakdown detail."""

    status = result["status_axes"]
    lines = [
        "# R3 5분봉 노출표본 진단 보고서",
        "",
        "> 이미 관찰된 2024-07-01~2026-07-01 자료의 설명적 진단이다. "
        "미관찰 OOS, 실거래 체결 검증, 투자 권고가 아니다.",
        "> 이 최종 R3 분석은 엔진의 원시 replay 입력 보고서 `report.md`보다 우선한다.",
        "",
        "## 상태 축",
        "",
        "| 축 | 결과 |",
        "|---|---|",
        f"| 데이터 무결성 | {status['data_integrity']} |",
        f"| 60분 kline proxy 효율성 | {status['kline_proxy_efficacy']} |",
        f"| 과거 체결 유효성 | {status['execution_validity']} |",
        f"| 일반화 | {status['generalization']} |",
        f"| 배포 | {status['deployment']} |",
        "",
    ]
    if status["data_integrity"] != "PASS":
        lines.extend(
            [
                "## 무결성 실패",
                "",
                str(result["integrity"]["reason"]),
                "",
            ]
        )
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "## 60분 1차 결과",
            "",
            "| 시장/허용 방향 | 기회 | 일수 | 평균 순수익(bp) | "
            "총 고정명목 P&L(USDT) | PF | 0x/2x 슬리피지(bp) |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for market in _MARKET_DIRECTION:
        row = result["primary_60m"][market]
        pf_text = _format_profit_factor(row)
        lines.append(
            "| "
            f"{market}/{_MARKET_DIRECTION[market]} | {row['eligible_opportunities']} | "
            f"{row['represented_utc_days']} | "
            f"{_format_optional_number(row['mean_net_bps'])} | "
            f"{row['fixed_notional_pnl_usdt']:.2f} | {pf_text} | "
            f"{_format_optional_number(row['slippage_sensitivity']['zero_x_mean_net_bps'])}/"
            f"{_format_optional_number(row['slippage_sensitivity']['two_x_mean_net_bps'])} |"
        )

    lines.extend(
        [
            "",
            "## 공유 UTC 일자 moving-block bootstrap",
            "",
            "| 시장 | 블록(일) | 95% 양측 구간(bp) | 단측 basic 하한(bp) | "
            "단측 p/Holm p | MC 해상도 | 무효/전체 |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for block in _BLOCK_DAYS:
        for market in _MARKET_DIRECTION:
            row = result["bootstrap"][str(block)]["markets"][market]
            interval = row["two_sided_95_interval"]
            interval_text = (
                "NA"
                if interval is None
                else f"[{interval[0] * 10_000:.4f}, {interval[1] * 10_000:.4f}]"
            )
            lower = row["one_sided_basic_95_lower"]
            raw_p = row["one_sided_p_value"]
            holm = row.get("holm")
            holm_p = None if holm is None else holm["adjusted_p_value"]
            lines.append(
                f"| {market} | {block} | {interval_text} | "
                f"{'NA' if lower is None else f'{lower * 10_000:.4f}'} | "
                f"{'NA' if raw_p is None else f'{raw_p:.6f}'}/"
                f"{'NA' if holm_p is None else f'{holm_p:.6f}'} | "
                f"{_format_optional_number(row['monte_carlo_resolution'], digits=8)} | "
                f"{row['invalid_replicates']}/{result['bootstrap'][str(block)]['samples']} |"
            )

    lines.extend(
        [
            "",
            "## 사전 고정 탐색 스크린",
            "",
            "| 시장 | 500건 | 120일 | 평균>5bp | 7일 하한>0 | PF>1.05 | "
            "2x>=0 | 양(+) 자산>=6 | 집중도<=35% |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    criterion_names = (
        "eligible_opportunities_at_least_500",
        "represented_utc_days_at_least_120",
        "mean_net_return_greater_than_5_bps",
        "seven_day_basic_lower_greater_than_zero",
        "profit_factor_greater_than_1_05",
        "two_x_slippage_mean_nonnegative",
        "positive_assets_at_least_6_of_8",
        "maximum_positive_asset_concentration_at_most_35_percent",
    )
    for market in _MARKET_DIRECTION:
        criteria = result["screen"]["markets"][market]
        flags = ["PASS" if criteria[name] else "FAIL" for name in criterion_names]
        lines.append(f"| {market} | {' | '.join(flags)} |")

    lines.extend(
        [
            "",
            "## 15·30·60분 비용·민감도·방향 적중률",
            "",
            "| 시장 | 분 | N | gross | fee | slip | funding | net | 0x | 2x | hit/flat/opposite |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in result["market_horizon_summaries"]:
        costs = row["cost_decomposition"]
        sensitivity = row["slippage_sensitivity"]
        rates = row["directional_label_rates"]
        lines.append(
            f"| {row['market']} | {row['horizon_minutes']} | "
            f"{row['eligible_opportunities']} | "
            f"{_format_optional_number(costs['mean_gross_bps'])} | "
            f"{_format_optional_number(costs['mean_fee_bps'])} | "
            f"{_format_optional_number(costs['mean_adverse_slippage_bps'])} | "
            f"{_format_optional_number(costs['mean_funding_bps'])} | "
            f"{_format_optional_number(row['mean_net_bps'])} | "
            f"{_format_optional_number(sensitivity['zero_x_mean_net_bps'])} | "
            f"{_format_optional_number(sensitivity['two_x_mean_net_bps'])} | "
            f"{_format_optional_percent(rates['directional_hit_rate'])}/"
            f"{_format_optional_percent(rates['abstention_rate'])}/"
            f"{_format_optional_percent(rates['opposite_direction_rate'])} |"
        )

    lines.extend(
        [
            "",
            "## 15·30·60분 long/short 연구 반사실",
            "",
            "| 시장 | 분 | 허용방향(bp) | long(bp/P&L) | short(bp/P&L) | LONG/FLAT/SHORT |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in result["market_horizon_summaries"]:
        counterfactual = row["research_counterfactuals"]
        counts = row["label_counts"]
        lines.append(
            f"| {row['market']} | {row['horizon_minutes']} | "
            f"{_format_optional_number(row['mean_net_bps'])} | "
            f"{_format_optional_number(counterfactual['mean_long_net_bps'])}/"
            f"{counterfactual['long_fixed_notional_pnl_usdt']:.2f} | "
            f"{_format_optional_number(counterfactual['mean_short_net_bps'])}/"
            f"{counterfactual['short_fixed_notional_pnl_usdt']:.2f} | "
            f"{counts['KLINE_PROXY_LONG']}/{counts['KLINE_PROXY_FLAT']}/"
            f"{counts['KLINE_PROXY_SHORT']} |"
        )

    lines.extend(
        [
            "",
            "## 60분 자산별 결과",
            "",
            "| 시장 | 자산 | N | net(bp) | P&L | PF | long(bp) | short(bp) |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for market in _MARKET_DIRECTION:
        for row in result["breakdowns"][f"{market}_12"]["asset"]:
            counterfactual = row["research_counterfactuals"]
            lines.append(
                f"| {market} | {row['value']} | {row['eligible_opportunities']} | "
                f"{_format_optional_number(row['mean_net_bps'])} | "
                f"{row['fixed_notional_pnl_usdt']:.2f} | "
                f"{_format_profit_factor(row)} | "
                f"{_format_optional_number(counterfactual['mean_long_net_bps'])} | "
                f"{_format_optional_number(counterfactual['mean_short_net_bps'])} |"
            )

    lines.extend(
        [
            "",
            "## 60분 split·regime·BTC trend 분해",
            "",
            "| 차원 | 시장 | 값 | N | net(bp) | P&L | PF |",
            "|---|---|---|---:|---:|---:|---:|",
        ]
    )
    dimension_labels = {
        "split": "split",
        "regime": "regime",
        "btc_trend": "BTC trend",
    }
    for dimension, label in dimension_labels.items():
        for market in _MARKET_DIRECTION:
            for row in result["breakdowns"][f"{market}_12"][dimension]:
                lines.append(
                    f"| {label} | {market} | {row['value']} | "
                    f"{row['eligible_opportunities']} | "
                    f"{_format_optional_number(row['mean_net_bps'])} | "
                    f"{row['fixed_notional_pnl_usdt']:.2f} | "
                    f"{_format_profit_factor(row)} |"
                )

    lines.extend(
        [
            "",
            "Spot의 SHORT 라벨은 하락/신규 Spot-long 보류 또는 기존 보유분의 청산 경고 뜻이며, "
            "신규 Spot 공매도 주문이 아니다.",
            "",
            "## T72 기술적 종료(2차·비독립·설명 전용)",
            "",
            "| 시장 | 거래 | 평균 순수익(bp) | P&L(USDT) | PF |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in result["technical_exit_72"]:
        mean = row["mean_net_bps"]
        lines.append(
            f"| {row['market']} | {row['trades']} | "
            f"{'NA' if mean is None else f'{mean:.4f}'} | "
            f"{row['fixed_notional_pnl_usdt']:.2f} | {_format_profit_factor(row)} |"
        )
    lines.extend(
        [
            "",
            "### T72 종료 사유",
            "",
            "| 시장 | 종료 사유 | 건수 |",
            "|---|---|---:|",
        ]
    )
    for row in result["technical_exit_72"]:
        reasons = row["exit_reason_counts"]
        if not reasons:
            lines.append(f"| {row['market']} | (없음) | 0 |")
        for reason, count in reasons.items():
            lines.append(f"| {row['market']} | {reason} | {count} |")
    lines.extend(
        [
            "",
            "## 해석 제한",
            "",
            "- 7일 블록이 사전 고정 1차 불확실성 추정이며, 14·28일 하한의 0 교차는 취약성 표시다.",
            "- 과거 decision-time BBO가 없어 kline proxy가 좋아도 체결 유효성은 결론 불가다.",
            "- funding 합계는 동결된 종목별 입력 파일과 소스 해시에 묶여 있지만, "
            "행 원장에는 포함 이벤트별 ID·digest가 없어 event-level 독립 검증을 주장하지 않는다.",
            "- 전 기간이 이미 노출되어 일반화와 배포 승인을 주장할 수 없다.",
            "- 자산·split·regime·BTC trend 상세표와 비용 분해는 JSON 산출물에 보존한다.",
            "",
        ]
    )
    return "\n".join(lines)


def write_r3_analysis(
    result: Mapping[str, Any], output_dir: str | Path
) -> dict[str, str]:
    """Write strict JSON, Korean Markdown, and their deterministic hashes."""

    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "r3_analysis.json"
    report_path = root / "r3_final_report_ko.md"
    json_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    report_path.write_text(render_r3_report_ko(result), encoding="utf-8")
    manifest = {
        "r3_analysis.json": _sha256_file(json_path),
        "r3_final_report_ko.md": _sha256_file(report_path),
    }
    manifest_path = root / "r3_analysis_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {
        "json": str(json_path),
        "report": str(report_path),
        "manifest": str(manifest_path),
    }


def analyze_r3_run(
    run_dir: str | Path,
    spec_path: str | Path,
    *,
    workspace_root: str | Path,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Verify one frozen R3 run, analyze it, and write durable result artifacts."""

    run_root = Path(run_dir)
    target = run_root if output_dir is None else Path(output_dir)
    try:
        provenance = validate_r3_run_provenance(
            run_root, spec_path, workspace_root=workspace_root
        )
        spec = load_backtest_spec(spec_path)
        opportunities = read_r3_opportunities(run_root / "opportunities.csv")
        trades = read_r3_technical_trades(run_root / "trades.csv")
        result = analyze_r3_diagnostic(opportunities, trades, spec)
        result["provenance"] = provenance
    except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        result = _integrity_failure(str(exc))
    write_r3_analysis(result, target)
    return result
