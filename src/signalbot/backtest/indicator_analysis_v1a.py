"""Fail-closed input authority for the seven-asset indicator V1A experiment.

This sibling deliberately does not modify the exposed V1 analyzer.  It binds a
pre-replay freeze, three independently replayed chronological splits, the full
five-horizon ledger, and the exact information-only pullback population before
any feature fitting or outcome analysis is permitted.
"""

from __future__ import annotations

import argparse
import csv
import errno
import hashlib
import io
import json
import math
import os
import random
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final, Literal, cast

import yaml
from pydantic import ValidationError

from signalbot.backtest.alert_replay import (
    RecommendationEvent,
    RecommendationOutcomeRow,
)
from signalbot.backtest.config import BacktestSpec, load_backtest_spec
from signalbot.backtest.engine import calculate_execution_returns
from signalbot.backtest.indicator_analysis import (
    FEATURE_COLUMNS,
    IndicatorObservation,
    PercentileCompositeModel,
    fit_percentile_composite,
    score_observations,
)
from signalbot.backtest.runner import source_code_digest
from signalbot.config import Settings
from signalbot.domain.enums import Direction

V1A_SCHEMA_VERSION: Final = 1
V1A_FREEZE_STATUS: Final = "FROZEN_BEFORE_AMENDMENT_1_REPLAY"
V1A_EXPOSURE_STATUS: Final = (
    "EXPOSED_V1A_VALIDATOR_BOUNDARY_ONLY_NO_EFFICACY_METRICS_"
    "AMENDMENT_1_HISTORICAL_ONLY"
)
V1A_ASSETS: Final = ("BONK", "ENA", "WIF", "FLOKI", "ARB", "OP", "SEI")
V1A_SPLITS: Final = (
    "development",
    "validation",
    "retrospective_test",
)
V1A_SPLIT_RANGES_MS: Final = {
    "development": (1_719_792_000_000, 1_740_787_200_000),
    "validation": (1_740_787_200_000, 1_761_955_200_000),
    "retrospective_test": (1_761_955_200_000, 1_782_864_000_000),
}
V1A_SPLIT_RANGES_UTC: Final = {
    "development": (
        "2024-07-01T00:00:00+00:00",
        "2025-03-01T00:00:00+00:00",
    ),
    "validation": (
        "2025-03-01T00:00:00+00:00",
        "2025-11-01T00:00:00+00:00",
    ),
    "retrospective_test": (
        "2025-11-01T00:00:00+00:00",
        "2026-07-01T00:00:00+00:00",
    ),
}
V1A_MARKETS: Final = ("spot", "futures")
V1A_INTERVAL: Final = "5m"
V1A_HORIZONS_BARS: Final = (1, 3, 6, 12, 72)
V1A_PRIMARY_HORIZON_BARS: Final = 12
V1A_REPLAY_PROTOCOL_VERSION: Final = (
    "alert_replay_v3_2026-07-20_indicator_discriminator"
)
V1A_RULE_VERSION: Final = "v4.3.0-causal-structure-diagnostics"
V1A_FEATURE_POLICY: Final = (
    "STRICT_8_FEATURE_COMPLETE_CASE_ELSE_FEATURE_NOT_READY_NO_CALL"
)
V1A_POPULATION_FAMILIES: Final = ("pullback_long", "pullback_short")
V1A_EXPECTED_INPUT_COUNT: Final = 21
V1A_SPEC_PROTOCOL_VERSION: Final = (
    "bt_5m_indicator_discriminator_v1a_7asset_2026-07-20"
)
V1A_BOOTSTRAP_BLOCK_DAYS: Final = 7
V1A_BOOTSTRAP_SAMPLES: Final = 10_000
V1A_BOOTSTRAP_SEED: Final = 20_260_720
V1A_MAX_RECOMMENDATIONS_PER_REPLAY: Final = 2_000_000
V1A_MAX_OUTCOMES_PER_REPLAY: Final = (
    V1A_MAX_RECOMMENDATIONS_PER_REPLAY * len(V1A_HORIZONS_BARS)
)
_JCS_SAFE_INTEGER_MAX: Final = 9_007_199_254_740_991
_RETURN_MICRO_SCALE: Final = Decimal(1_000_000)
_DAY_MS: Final = 86_400_000
_INTERVAL_MS: Final = 300_000
_SPLIT_START_EMBARGO_BARS: Final = 72
_OUTCOME_FLOAT_ABS_TOLERANCE: Final = 1e-12
_OUTCOME_FLOAT_REL_TOLERANCE: Final = 1e-12
_V1A_EXECUTION_COST_BPS: Final = {
    ("spot", "volatile"): (10.0, 10.0),
    ("futures", "volatile"): (5.0, 8.0),
}
_OUTCOME_BASE_EXCLUSION_REASONS: Final = frozenset(
    {
        "data_gap_in_horizon",
        "outside_declared_split",
        "split_start_embargo",
        "horizon_crosses_split",
        "next_bar_unavailable",
        "next_bar_not_contiguous",
        "insufficient_horizon",
    }
)

V1A_EXPECTED_DATA_INPUT_KEYS: Final = frozenset(
    {
        "spot/BONK__BONKUSDT__5m.csv.gz",
        "spot/ENA__ENAUSDT__5m.csv.gz",
        "spot/WIF__WIFUSDT__5m.csv.gz",
        "spot/FLOKI__FLOKIUSDT__5m.csv.gz",
        "spot/ARB__ARBUSDT__5m.csv.gz",
        "spot/OP__OPUSDT__5m.csv.gz",
        "spot/SEI__SEIUSDT__5m.csv.gz",
        "futures/BONK__1000BONKUSDT__5m.csv.gz",
        "futures/ENA__ENAUSDT__5m.csv.gz",
        "futures/WIF__WIFUSDT__5m.csv.gz",
        "futures/FLOKI__1000FLOKIUSDT__5m.csv.gz",
        "futures/ARB__ARBUSDT__5m.csv.gz",
        "futures/OP__OPUSDT__5m.csv.gz",
        "futures/SEI__SEIUSDT__5m.csv.gz",
        "funding/BONK__1000BONKUSDT__5m.csv.gz",
        "funding/ENA__ENAUSDT__5m.csv.gz",
        "funding/WIF__WIFUSDT__5m.csv.gz",
        "funding/FLOKI__1000FLOKIUSDT__5m.csv.gz",
        "funding/ARB__ARBUSDT__5m.csv.gz",
        "funding/OP__OPUSDT__5m.csv.gz",
        "funding/SEI__SEIUSDT__5m.csv.gz",
    }
)

V1A_REQUIRED_FROZEN_FILE_PATHS: Final = frozenset(
    {
        ".python-version",
        "pyproject.toml",
        "uv.lock",
        "config/backtest.5m.indicator-discriminator-v1a-7asset.yaml",
        "config/settings.example.yaml",
        (
            "artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/"
            "experiment_contract.md"
        ),
        (
            "artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/"
            "amendment_1.md"
        ),
        (
            "artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/"
            "freeze_manifest.json"
        ),
        (
            "artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/"
            "replay-development/run_manifest.json"
        ),
        (
            "artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/"
            "replay-validation/run_manifest.json"
        ),
        (
            "artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/"
            "replay-retrospective/run_manifest.json"
        ),
        "src/signalbot/backtest/indicator_analysis_v1a.py",
        "tests/unit/test_indicator_analysis_v1a.py",
        "src/signalbot/backtest/indicator_analysis.py",
        "tests/unit/test_indicator_analysis.py",
        "src/signalbot/backtest/alert_filter.py",
        "tests/unit/test_backtest_alert_filter.py",
        "src/signalbot/backtest/alert_replay.py",
        "tests/unit/test_alert_replay.py",
        (
            "artifacts/backtest/2026-07-20-indicator-discriminator-v1/"
            "freeze_manifest.json"
        ),
        (
            "artifacts/backtest/2026-07-20-indicator-discriminator-v1/"
            "amendment_1.md"
        ),
        (
            "artifacts/backtest/2026-07-20-indicator-discriminator-v1/"
            "amendment_1_freeze.json"
        ),
        "config/backtest.5m.indicator-discriminator-v1-development-amendment.yaml",
    }
)

_REPLAY_OUTPUT_NAMES: Final = frozenset(
    {"recommendations.csv", "outcomes.csv", "results.json", "report_ko.md"}
)
_ANALYSIS_PROTOCOL: Final = "indicator_discriminator_v1a_seven_asset_after_cost"
_ANALYSIS_PAYLOAD_NAMES: Final = (
    "fitted_score.json",
    "results.json",
    "report_ko.md",
)
_ANALYSIS_MANIFEST_NAME: Final = "analysis_manifest.json"
_ANALYSIS_OUTPUT_NAMES: Final = frozenset(
    (*_ANALYSIS_PAYLOAD_NAMES, _ANALYSIS_MANIFEST_NAME)
)
_RECOMMENDATION_EXACT_COLUMNS: Final = tuple(
    field.name for field in fields(RecommendationEvent)
)
_OUTCOME_EXACT_COLUMNS: Final = tuple(
    field.name for field in fields(RecommendationOutcomeRow)
)
_V1A_ASSET_CONTRACT: Final = (
    ("BONK", "volatile", "BONKUSDT", "1000BONKUSDT"),
    ("ENA", "volatile", "ENAUSDT", "ENAUSDT"),
    ("WIF", "volatile", "WIFUSDT", "WIFUSDT"),
    ("FLOKI", "volatile", "FLOKIUSDT", "1000FLOKIUSDT"),
    ("ARB", "volatile", "ARBUSDT", "ARBUSDT"),
    ("OP", "volatile", "OPUSDT", "OPUSDT"),
    ("SEI", "volatile", "SEIUSDT", "SEIUSDT"),
)
_V1A_SYMBOL_BY_PAIR: Final = {
    (asset, "spot"): spot_symbol
    for asset, _, spot_symbol, _ in _V1A_ASSET_CONTRACT
} | {
    (asset, "futures"): futures_symbol
    for asset, _, _, futures_symbol in _V1A_ASSET_CONTRACT
}
_V1A_EXPECTED_PAIR_ORDER: Final = tuple(
    (market, asset) for market in V1A_MARKETS for asset in V1A_ASSETS
)


class IndicatorV1AContractError(ValueError):
    """Raised when frozen V1A authority or replay material is not exact."""


@dataclass(frozen=True, slots=True)
class FrozenAuthorityV1A:
    """Validated pre-replay authority and current workspace binding."""

    manifest_path: Path
    manifest_sha256: str
    created_at_utc: str
    created_at_ms: int
    created_at_datetime: datetime
    spec_sha256: str
    spec_semantics_sha256: str
    config_sha256: str
    settings_semantics_sha256: str
    source_code_sha256: str
    data_input_sha256: tuple[tuple[str, str], ...]
    file_sha256: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class NetOutcomeV1A:
    """One exact after-cost horizon outcome; missing is never zero."""

    horizon_bars: int
    evaluable: bool
    net_return_micros: int | None


@dataclass(frozen=True, slots=True)
class IntendedPopulationRowV1A:
    """One pre-outcome intended-population row before spot-priority dedup."""

    event_id: str
    asset: str
    market: Literal["spot", "futures"]
    direction: Literal["long", "short"]
    decision_time_ms: int
    split: str
    features: tuple[float | None, ...]


@dataclass(frozen=True, slots=True)
class AnalyzableEventV1A:
    """One complete-case, deduplicated event with all frozen horizons."""

    event_id: str
    asset: str
    market: Literal["spot", "futures"]
    direction: Literal["long", "short"]
    decision_time_ms: int
    split: str
    features: tuple[float, ...]
    outcomes: tuple[NetOutcomeV1A, ...]


@dataclass(frozen=True, slots=True)
class FeatureNotReadyV1A:
    """Explicit no-call retained in the population denominator."""

    event_id: str
    asset: str
    market: Literal["spot", "futures"]
    direction: Literal["long", "short"]
    decision_time_ms: int
    split: str
    missing_features: tuple[str, ...]
    outcomes: tuple[NetOutcomeV1A, ...] = ()


@dataclass(frozen=True, slots=True)
class ReplayInputAuditV1A:
    """Structural and population audit for one chronological replay."""

    split: str
    replay_dir: Path
    run_manifest_sha256: str
    recommendations_sha256: str
    outcomes_sha256: str
    results_sha256: str
    report_sha256: str
    recommendation_rows: int
    outcome_rows: int
    intended_population_rows: int
    intended_population_sha256: str
    deduplicated_population_rows: int
    retained_population_sha256: str
    duplicate_rows_dropped: int
    dropped_population_sha256: str
    complete_case_rows: int
    feature_not_ready_rows: int
    feature_not_ready_sha256: str


@dataclass(frozen=True, slots=True)
class LoadedIndicatorV1A:
    """Non-promoting, outcome-complete V1A input bundle."""

    authority: FrozenAuthorityV1A
    events: tuple[AnalyzableEventV1A, ...]
    feature_not_ready: tuple[FeatureNotReadyV1A, ...]
    audits: tuple[ReplayInputAuditV1A, ...]
    exposure_status: str = V1A_EXPOSURE_STATUS
    independently_validated: bool = False
    deployment_approved: bool = False
    probability_calibrated: bool = False


@dataclass(frozen=True, slots=True)
class ScoredEventV1A:
    """One complete-case event scored by the frozen development-only model."""

    event: AnalyzableEventV1A
    axis_scores: tuple[float, ...]
    composite_score: float
    selected: bool


@dataclass(frozen=True, slots=True)
class OutcomeMetricsV1A:
    """Exact-micros point metrics for one after-cost event population."""

    population_events: int
    evaluable_events: int
    missing_events: int
    sum_net_return_micros: int
    mean_net_return_micros: float | None
    median_net_return_micros: float | None
    strict_hits: int
    strict_hit_rate: float | None
    gross_profit_micros: int
    gross_loss_abs_micros: int
    profit_factor: float | None
    profit_factor_status: str

    def artifact(self) -> dict[str, object]:
        """Return a deterministic JSON-safe representation."""

        return {
            "population_events": self.population_events,
            "evaluable_events": self.evaluable_events,
            "missing_events": self.missing_events,
            "sum_net_return_micros": self.sum_net_return_micros,
            "mean_net_return_micros": self.mean_net_return_micros,
            "median_net_return_micros": self.median_net_return_micros,
            "strict_hits": self.strict_hits,
            "strict_hit_rate": self.strict_hit_rate,
            "gross_profit_micros": self.gross_profit_micros,
            "gross_loss_abs_micros": self.gross_loss_abs_micros,
            "profit_factor": self.profit_factor,
            "profit_factor_status": self.profit_factor_status,
        }


@dataclass(frozen=True, slots=True)
class ValidationGateEvidenceV1A:
    """Exact evidence used by the sole frozen historical validation gate."""

    selected_sum_micros: int
    selected_evaluable: int
    selected_mean_lower_micros: float | None
    baseline_sum_micros: int
    baseline_evaluable: int
    uplift_mean_lower_micros: float | None
    selected_gross_profit_micros: int
    selected_gross_loss_abs_micros: int
    selected_median_micros: float | None
    long_selected_sum_micros: int
    long_selected_evaluable: int
    short_selected_sum_micros: int
    short_selected_evaluable: int
    selected_events: int
    intended_population_events: int
    complete_case_events: int
    positive_asset_uplifts: int
    asset_count: int
    selected_mean_valid_replicates: int
    uplift_mean_valid_replicates: int
    bootstrap_samples: int


@dataclass(frozen=True, slots=True)
class SharedBootstrapScheduleV1A:
    """One frozen draw schedule shared by every cell in a chronological split."""

    split: str
    calendar_start_ms: int
    calendar_end_ms: int
    calendar_days: int
    block_lengths: tuple[int, ...]
    samples: int
    seed: int
    starts: tuple[tuple[int, ...], ...]
    schedule_sha256: str

    def artifact(self) -> dict[str, object]:
        """Return schedule authority without materializing all draws in JSON."""

        return {
            "split": self.split,
            "calendar_start_ms": self.calendar_start_ms,
            "calendar_end_ms": self.calendar_end_ms,
            "calendar_days": self.calendar_days,
            "calendar_includes_zero_alert_days": True,
            "circular": True,
            "requested_block_days": V1A_BOOTSTRAP_BLOCK_DAYS,
            "block_lengths": list(self.block_lengths),
            "samples": self.samples,
            "seed": self.seed,
            "shared_across_all_horizons_selections_directions_assets": True,
            "schedule_sha256": self.schedule_sha256,
        }


@dataclass(frozen=True, slots=True)
class _ReplayIdentityV1A:
    spec_sha256: str
    config_sha256: str
    code_sha256: str
    protocol_version: str
    rule_version: str
    inputs: tuple[tuple[str, str], ...]
    costs_json: str


@dataclass(frozen=True, slots=True)
class _ParsedReplayV1A:
    identity: _ReplayIdentityV1A
    full_event_ids: frozenset[str]
    events: tuple[AnalyzableEventV1A, ...]
    feature_not_ready: tuple[FeatureNotReadyV1A, ...]
    audit: ReplayInputAuditV1A


@dataclass(frozen=True, slots=True)
class _RecommendationMetadataV1A:
    """Authenticated recommendation fields required by every outcome row."""

    event_id: str
    asset: str
    cohort: str
    market: Literal["spot", "futures"]
    symbol: str
    family: str
    direction: Literal["long", "short"]
    stage: str
    information_only: bool
    decision_time_ms: int
    split: str
    recommendation_price: float
    invalidation_price: float | None


@dataclass(frozen=True, slots=True)
class _RecommendationsV1A:
    event_ids: frozenset[str]
    metadata_by_event_id: Mapping[str, _RecommendationMetadataV1A]
    population_rows: tuple[IntendedPopulationRowV1A, ...]
    pair_counts: tuple[tuple[str, str, int], ...]
    row_count: int


@dataclass(frozen=True, slots=True)
class _OutcomesV1A:
    by_event_id: Mapping[str, tuple[NetOutcomeV1A, ...]]
    row_count: int


def sha256_bytes(value: bytes) -> str:
    """Return the lowercase SHA-256 of exact bytes."""

    return hashlib.sha256(value).hexdigest()


def _runtime_workspace_root() -> Path:
    """Return the repository that owns this executing V1A module."""

    return Path(__file__).resolve().parents[3]


def _parse_canonical_utc(value: object, label: str) -> tuple[str, datetime]:
    text = _require_string(value, label)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise IndicatorV1AContractError(
            f"{label} must be a canonical ISO-8601 UTC timestamp"
        ) from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() != UTC.utcoffset(parsed)
        or parsed.isoformat() != text
    ):
        raise IndicatorV1AContractError(
            f"{label} must be canonical ISO-8601 with an explicit +00:00 offset"
        )
    return text, parsed


def _semantic_sha256(value: object) -> str:
    return sha256_bytes(_canonical_json_bytes(value))


def _load_settings_without_environment(path: Path) -> Settings:
    """Parse the frozen settings bytes without applying process environment."""

    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle) or {}
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise IndicatorV1AContractError(
            "frozen settings are not readable UTF-8 YAML"
        ) from exc
    if type(raw) is not dict:
        raise IndicatorV1AContractError(
            "frozen settings root must be an exact YAML mapping"
        )
    try:
        return Settings.model_validate(raw)
    except ValidationError as exc:
        raise IndicatorV1AContractError(
            "frozen settings fail semantic validation"
        ) from exc


def _load_and_validate_v1a_spec(path: Path) -> BacktestSpec:
    try:
        spec = load_backtest_spec(path)
    except (OSError, UnicodeError, ValueError, yaml.YAMLError, ValidationError) as exc:
        raise IndicatorV1AContractError(
            "frozen V1A backtest spec fails semantic validation"
        ) from exc
    actual_assets = tuple(
        (asset.asset, asset.cohort, asset.spot_symbol, asset.futures_symbol)
        for asset in spec.assets
    )
    actual_splits = tuple(
        (
            split.name,
            int(split.start.timestamp() * 1000),
            int(split.end.timestamp() * 1000),
        )
        for split in spec.splits
    )
    expected_splits = tuple(
        (name, *V1A_SPLIT_RANGES_MS[name]) for name in V1A_SPLITS
    )
    exact_scalars = (
        spec.protocol_version == V1A_SPEC_PROTOCOL_VERSION,
        spec.rule_version == V1A_RULE_VERSION,
        spec.experiment_plan_path
        == (
            "artifacts/backtest/2026-07-20-indicator-discriminator-v1a-7asset/"
            "experiment_contract.md"
        ),
        spec.interval == V1A_INTERVAL,
        spec.data_start == datetime(2024, 3, 1, tzinfo=UTC),
        spec.evaluation_start == datetime(2024, 7, 1, tzinfo=UTC),
        spec.evaluation_end == datetime(2026, 7, 1, tzinfo=UTC),
        spec.minimum_age_days == 90,
        spec.minimum_history_bars == 210,
        spec.historical_spread_proxy_bps == 11.25,
        spec.entry_score == 80,
        spec.strategy_mode == "gate_v2",
        spec.candidate_policy is None,
        spec.opportunity_panel_horizon_bars == 72,
        spec.outcome_edge_margin_bps == 0.0,
        spec.confirmation_mode is None,
        spec.gate_use_participation is True,
        spec.gate_use_crowding is True,
        spec.gate_use_higher_timeframes is True,
        spec.include_rsi_reversals is False,
        spec.trend_gate == 60,
        spec.participation_gate == 60,
        spec.crowding_risk_cap == 75,
        spec.execution_gate == 65,
        spec.completeness_gate == 95,
        spec.volume_feature_set == "none",
        spec.exits.model_dump(mode="json")
        == {
            "trend_failure_bars": 3,
            "trailing_activation_r": 1.0,
            "trailing_atr_multiple": 2.0,
            "max_holding_bars": 72,
        },
        spec.costs.model_dump(mode="json")
        == {
            "notional_usdt": 100.0,
            "spot_fee_bps": 10.0,
            "futures_fee_bps": 5.0,
            "spot_slippage_bps": {
                "anchor": 5.0,
                "major": 5.0,
                "volatile": 10.0,
            },
            "futures_slippage_bps": {
                "anchor": 3.0,
                "major": 3.0,
                "volatile": 8.0,
            },
            "include_funding": True,
        },
        spec.bootstrap.model_dump(mode="json")
        == {
            "samples": V1A_BOOTSTRAP_SAMPLES,
            "block_days": V1A_BOOTSTRAP_BLOCK_DAYS,
            "seed": V1A_BOOTSTRAP_SEED,
        },
    )
    if (
        not all(exact_scalars)
        or actual_assets != _V1A_ASSET_CONTRACT
        or actual_splits != expected_splits
    ):
        raise IndicatorV1AContractError(
            "frozen V1A backtest spec differs from the pre-declared exact semantics"
        )
    return spec


def _validate_v1a_settings(settings: Settings) -> None:
    document = settings.model_dump(mode="json")
    binance = cast(dict[str, Any], document["binance"])
    signals = cast(dict[str, Any], document["signals"])
    technical_exit = cast(dict[str, Any], signals["technical_exit"])
    alerts = cast(dict[str, Any], document["alerts"])
    if not (
        document["app_name"] == "binance-signal-bot"
        and document["log_level"] == "INFO"
        and document["rule_version"] == V1A_RULE_VERSION
        and binance["markets"] == ["spot", "futures"]
        and binance["quote_asset"] == "USDT"
        and binance["primary_interval"] == V1A_INTERVAL
        and binance["intervals"] == ["1m", "5m", "15m", "1h", "4h"]
        and signals["pullback_alert_mode"] == "informational"
        and signals["pullback_intervals"] == ["5m", "15m", "1h", "4h"]
        and signals["confirmation_mode"] == "explicit_trigger"
        and signals["volume_feature_set"] == "none"
        and technical_exit
        == {
            "enabled": True,
            "trend_failure_bars": 3,
            "trailing_activation_r": 1.0,
            "trailing_atr_multiple": 2.0,
            "max_holding_bars": 72,
        }
        and alerts["discord_enabled"] is False
        and alerts["discord_webhook_url"] is None
    ):
        raise IndicatorV1AContractError(
            "frozen settings differ from the exact env-disabled V1A replay semantics"
        )


def _require_dict(value: object, label: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise IndicatorV1AContractError(f"{label} must be an exact JSON object")
    return cast(dict[str, Any], value)


def _require_list(value: object, label: str) -> list[Any]:
    if type(value) is not list:
        raise IndicatorV1AContractError(f"{label} must be an exact JSON array")
    return cast(list[Any], value)


def _require_string(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise IndicatorV1AContractError(f"{label} must be a non-empty string")
    return value


def _require_bool(value: object, expected: bool, label: str) -> None:
    if type(value) is not bool or value is not expected:
        raise IndicatorV1AContractError(f"{label} must be {expected!r}")


def _require_int(value: object, label: str) -> int:
    if type(value) is not int or not 0 <= value <= _JCS_SAFE_INTEGER_MAX:
        raise IndicatorV1AContractError(
            f"{label} must be a nonnegative JCS-safe integer"
        )
    return value


def _require_sha256(value: object, label: str) -> str:
    text = _require_string(value, label)
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        raise IndicatorV1AContractError(f"{label} must be lowercase SHA-256")
    return text


def _json_from_bytes(raw: bytes, label: str) -> dict[str, Any]:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        document: dict[str, Any] = {}
        for key, value in pairs:
            if key in document:
                raise IndicatorV1AContractError(
                    f"{label} contains duplicate JSON key: {key}"
                )
            document[key] = value
        return document

    try:
        decoded = json.loads(raw, object_pairs_hook=reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IndicatorV1AContractError(f"{label} is not valid UTF-8 JSON") from exc
    return _require_dict(decoded, label)


def _canonical_mapping(
    value: object,
    *,
    label: str,
    expected_count: int | None = None,
) -> tuple[tuple[str, str], ...]:
    document = _require_dict(value, label)
    if expected_count is not None and len(document) != expected_count:
        raise IndicatorV1AContractError(
            f"{label} must contain exactly {expected_count} entries"
        )
    pairs: list[tuple[str, str]] = []
    for key, digest in document.items():
        if type(key) is not str or not key or "\\" in key:
            raise IndicatorV1AContractError(
                f"{label} keys must be non-empty POSIX paths"
            )
        pairs.append((key, _require_sha256(digest, f"{label}[{key!r}]")))
    return tuple(sorted(pairs))


def _hash_file(path: Path) -> str:
    if not path.is_file():
        raise IndicatorV1AContractError(f"required frozen file is missing: {path}")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise IndicatorV1AContractError(
            f"required frozen file cannot be read: {path}"
        ) from exc
    return digest.hexdigest()


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    except (TypeError, ValueError) as exc:
        raise IndicatorV1AContractError("value is not canonical-JSON encodable") from exc


def _parse_csv_bool(value: str | None, label: str) -> bool:
    if value == "True":
        return True
    if value == "False":
        return False
    raise IndicatorV1AContractError(f"{label} must be exact True or False")


def _parse_csv_int(value: str | None, label: str) -> int:
    if value is None or not value or not value.isascii() or not value.isdigit():
        raise IndicatorV1AContractError(f"{label} must be an unsigned decimal integer")
    parsed = int(value)
    if parsed > _JCS_SAFE_INTEGER_MAX:
        raise IndicatorV1AContractError(f"{label} exceeds the JCS-safe integer range")
    return parsed


def _parse_decimal(value: str | None, label: str) -> Decimal:
    if value is None or not value.strip():
        raise IndicatorV1AContractError(f"{label} must be a finite decimal")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise IndicatorV1AContractError(f"{label} must be a finite decimal") from exc
    if not parsed.is_finite():
        raise IndicatorV1AContractError(f"{label} must be a finite decimal")
    return parsed


def _parse_optional_float_v1a(value: str | None, label: str) -> float | None:
    if value == "":
        return None
    parsed = float(_parse_decimal(value, label))
    if not math.isfinite(parsed):
        raise IndicatorV1AContractError(f"{label} overflows float")
    return parsed


def _parse_required_float_v1a(value: str | None, label: str) -> float:
    parsed = _parse_optional_float_v1a(value, label)
    if parsed is None:
        raise IndicatorV1AContractError(f"{label} must be present")
    return parsed


def _parse_optional_csv_int_v1a(value: str | None, label: str) -> int | None:
    if value == "":
        return None
    return _parse_csv_int(value, label)


def _require_float_close_v1a(actual: float, expected: float, label: str) -> None:
    if not math.isclose(
        actual,
        expected,
        rel_tol=_OUTCOME_FLOAT_REL_TOLERANCE,
        abs_tol=_OUTCOME_FLOAT_ABS_TOLERANCE,
    ):
        raise IndicatorV1AContractError(f"{label} does not reconcile")


def _hit_status_v1a(net_return: float, margin_bps: float) -> str:
    margin = margin_bps / 10_000
    if net_return > margin:
        return "hit"
    if net_return < -margin:
        return "miss"
    return "ambiguous"


def net_return_micros_v1a(value: str) -> int:
    """Quantize exact CSV decimal text to signed return micros, half-even."""

    parsed = _parse_decimal(value, "net_return")
    scaled = (parsed * _RETURN_MICRO_SCALE).to_integral_value(
        rounding=ROUND_HALF_EVEN
    )
    result = int(scaled)
    if abs(result) > _JCS_SAFE_INTEGER_MAX:
        raise IndicatorV1AContractError("net_return micros exceeds JCS-safe range")
    return result


def _csv_reader(raw: bytes, *, label: str) -> csv.DictReader[str]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IndicatorV1AContractError(f"{label} is not valid UTF-8") from exc
    return csv.DictReader(io.StringIO(text, newline=""))


def _require_csv_columns(
    reader: csv.DictReader[str],
    expected: tuple[str, ...],
    *,
    label: str,
) -> None:
    fields = reader.fieldnames
    if fields is None:
        raise IndicatorV1AContractError(f"{label} has no header")
    if len(fields) != len(set(fields)):
        raise IndicatorV1AContractError(f"{label} contains a duplicate column")
    if tuple(fields) != expected:
        raise IndicatorV1AContractError(
            f"{label} header must exactly match the producer dataclass order"
        )


def _require_no_surplus_csv_cells(
    row: Mapping[str | None, str | list[str] | None],
    *,
    label: str,
) -> None:
    if None in row:
        raise IndicatorV1AContractError(f"{label} contains surplus CSV cells")


def _parse_recommendations(
    raw: bytes,
    *,
    expected_split: str,
) -> _RecommendationsV1A:
    reader = _csv_reader(raw, label="recommendations.csv")
    _require_csv_columns(
        reader,
        _RECOMMENDATION_EXACT_COLUMNS,
        label="recommendations.csv",
    )
    event_ids: set[str] = set()
    metadata_by_event_id: dict[str, _RecommendationMetadataV1A] = {}
    population: list[IntendedPopulationRowV1A] = []
    pair_counts: dict[tuple[str, str], int] = defaultdict(int)
    start_ms, end_ms = V1A_SPLIT_RANGES_MS[expected_split]
    for line_number, row in enumerate(reader, start=2):
        _require_no_surplus_csv_cells(
            row,
            label=f"recommendations line {line_number}",
        )
        if len(event_ids) >= V1A_MAX_RECOMMENDATIONS_PER_REPLAY:
            raise IndicatorV1AContractError("recommendation row cap exceeded")
        event_id = (row.get("event_id") or "").strip()
        if not event_id:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} event_id is empty"
            )
        if event_id in event_ids:
            raise IndicatorV1AContractError(
                f"duplicate recommendation event_id: {event_id}"
            )
        event_ids.add(event_id)
        if row.get("protocol_version") != V1A_REPLAY_PROTOCOL_VERSION:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} protocol drift"
            )
        if row.get("rule_version") != V1A_RULE_VERSION:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} rule drift"
            )
        asset = (row.get("asset") or "").strip()
        if asset not in V1A_ASSETS:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} has unknown asset"
            )
        market_raw = (row.get("market") or "").strip()
        if market_raw not in V1A_MARKETS:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} has unknown market"
            )
        market = cast(Literal["spot", "futures"], market_raw)
        pair_counts[(market, asset)] += 1
        if row.get("cohort") != "volatile":
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} cohort drift"
            )
        if row.get("symbol") != _V1A_SYMBOL_BY_PAIR[(asset, market)]:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} symbol drift"
            )
        split = (row.get("split") or "").strip()
        if split != expected_split:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} belongs to the wrong split"
            )
        decision_time_ms = _parse_csv_int(
            row.get("decision_time_ms"),
            f"recommendations line {line_number} decision_time_ms",
        )
        if not start_ms <= decision_time_ms < end_ms:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} lies outside its frozen split"
            )
        if decision_time_ms % 300_000 != 299_999:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} is not a closed 5m candle time"
            )
        direction_raw = (row.get("direction") or "").strip()
        if direction_raw not in {"long", "short"}:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} has unknown direction"
            )
        direction = cast(Literal["long", "short"], direction_raw)
        information_only = _parse_csv_bool(
            row.get("information_only"),
            f"recommendations line {line_number} information_only",
        )
        stage = (row.get("stage") or "").strip()
        family = (row.get("family") or "").strip()
        score = _parse_decimal(
            row.get("score"), f"recommendations line {line_number} score"
        )
        price = _parse_decimal(
            row.get("price"), f"recommendations line {line_number} price"
        )
        if price <= 0:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} price is not positive"
            )
        recommendation_price = float(price)
        if not math.isfinite(recommendation_price):
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} price overflows float"
            )
        invalidation_price = _parse_optional_float_v1a(
            row.get("invalidation"),
            f"recommendations line {line_number} invalidation",
        )
        metadata_by_event_id[event_id] = _RecommendationMetadataV1A(
            event_id=event_id,
            asset=asset,
            cohort="volatile",
            market=market,
            symbol=_V1A_SYMBOL_BY_PAIR[(asset, market)],
            family=family,
            direction=direction,
            stage=stage,
            information_only=information_only,
            decision_time_ms=decision_time_ms,
            split=split,
            recommendation_price=recommendation_price,
            invalidation_price=invalidation_price,
        )
        is_population = (
            information_only
            and stage == "setup"
            and family in V1A_POPULATION_FAMILIES
            and score == Decimal(100)
        )
        if not is_population:
            continue
        expected_direction = "long" if family == "pullback_long" else "short"
        if direction != expected_direction:
            raise IndicatorV1AContractError(
                f"recommendations line {line_number} family-direction conflict"
            )
        features: list[float | None] = []
        for feature_name in FEATURE_COLUMNS:
            raw_feature = row.get(feature_name)
            if raw_feature is None or not raw_feature.strip():
                features.append(None)
                continue
            decimal_feature = _parse_decimal(
                raw_feature,
                f"recommendations line {line_number} {feature_name}",
            )
            feature = float(decimal_feature)
            if not math.isfinite(feature):
                raise IndicatorV1AContractError(
                    f"recommendations line {line_number} {feature_name} overflows float"
                )
            features.append(feature)
        population.append(
            IntendedPopulationRowV1A(
                event_id=event_id,
                asset=asset,
                market=market,
                direction=direction,
                decision_time_ms=decision_time_ms,
                split=split,
                features=tuple(features),
            )
        )
    return _RecommendationsV1A(
        event_ids=frozenset(event_ids),
        metadata_by_event_id=metadata_by_event_id,
        population_rows=tuple(population),
        pair_counts=tuple(
            (market, asset, pair_counts[(market, asset)])
            for market, asset in _V1A_EXPECTED_PAIR_ORDER
        ),
        row_count=len(event_ids),
    )


def _outcome_exclusion_reason_v1a(
    row: Mapping[str, str | None],
    *,
    metadata: _RecommendationMetadataV1A,
    horizon: int,
    line_number: int,
) -> None:
    label = f"outcomes line {line_number}"
    reason = row.get("exclusion_reason")
    valid_reasons = _OUTCOME_BASE_EXCLUSION_REASONS | {
        f"insufficient_{horizon}_bar_horizon"
    }
    if reason not in valid_reasons:
        raise IndicatorV1AContractError(f"{label} has invalid exclusion reason")
    numeric_and_path_fields = (
        "entry_time_ms",
        "exit_time_ms",
        "entry_price",
        "exit_price",
        "raw_close_return",
        "maximum_rise",
        "maximum_drop",
        "gross_return",
        "slippage_return",
        "fee_return",
        "funding_return",
        "net_return",
        "mfe",
        "mae",
        "one_r_target_price",
        "one_r_risk_fraction",
        "observed_until_ms",
    )
    if any(row.get(field) != "" for field in numeric_and_path_fields):
        raise IndicatorV1AContractError(
            f"{label} gives numeric or path data to an unevaluable outcome"
        )
    if any(
        row.get(field) != "unevaluable"
        for field in (
            "hit_status_5bps",
            "hit_status_0bps",
            "hit_status_10bps",
            "hit_status_25bps",
            "one_r_path_status",
        )
    ):
        raise IndicatorV1AContractError(
            f"{label} has non-unevaluable status for excluded outcome"
        )
    start_ms, end_ms = V1A_SPLIT_RANGES_MS[metadata.split]
    expected_entry_ms = metadata.decision_time_ms + 1
    expected_exit_ms = expected_entry_ms + horizon * _INTERVAL_MS - 1
    embargo_end_ms = start_ms + _SPLIT_START_EMBARGO_BARS * _INTERVAL_MS
    if expected_entry_ms < embargo_end_ms and reason != "split_start_embargo":
        raise IndicatorV1AContractError(
            f"{label} must retain deterministic split_start_embargo reason"
        )
    if expected_entry_ms >= embargo_end_ms and expected_exit_ms >= end_ms:
        end_reasons = {
            "horizon_crosses_split",
            f"insufficient_{horizon}_bar_horizon",
            "data_gap_in_horizon",
        }
        if expected_entry_ms >= end_ms:
            end_reasons |= {"split_start_embargo", "outside_declared_split"}
        if reason not in end_reasons:
            raise IndicatorV1AContractError(
                f"{label} lacks a producer-reachable split-end exclusion"
            )


def _one_r_excursion_relation_v1a(
    excursion: float,
    expected_risk_fraction: float,
) -> Literal[-1, 0, 1]:
    """Compare one summarized excursion without inventing a boundary bit.

    The producer compares raw float prices.  A serialized excursion ratio can
    land just above or below the equivalent risk ratio because the two values
    are formed by different float expressions.  An isclose boundary therefore
    supports either producer result; values outside it remain ordered.
    """

    if math.isclose(
        excursion,
        expected_risk_fraction,
        rel_tol=_OUTCOME_FLOAT_REL_TOLERANCE,
        abs_tol=_OUTCOME_FLOAT_ABS_TOLERANCE,
    ):
        return 0
    return 1 if excursion > expected_risk_fraction else -1


def _one_r_touch_relations_v1a(
    direction: Literal["long", "short"],
    maximum_rise: float,
    maximum_drop: float,
    expected_risk_fraction: float,
) -> tuple[Literal[-1, 0, 1], Literal[-1, 0, 1]]:
    if direction == "long":
        target_excursion = maximum_rise
        invalidation_excursion = -maximum_drop
    else:
        target_excursion = -maximum_drop
        invalidation_excursion = maximum_rise
    return (
        _one_r_excursion_relation_v1a(
            target_excursion,
            expected_risk_fraction,
        ),
        _one_r_excursion_relation_v1a(
            invalidation_excursion,
            expected_risk_fraction,
        ),
    )


def _validate_one_r_v1a(
    row: Mapping[str, str | None],
    *,
    metadata: _RecommendationMetadataV1A,
    entry_price: float,
    maximum_rise: float,
    maximum_drop: float,
    entry_time_ms: int,
    exit_time_ms: int,
    line_number: int,
) -> None:
    label = f"outcomes line {line_number} one-R"
    status = row.get("one_r_path_status")
    observed = _parse_optional_csv_int_v1a(
        row.get("observed_until_ms"),
        f"{label} observed_until_ms",
    )
    target = _parse_optional_float_v1a(
        row.get("one_r_target_price"),
        f"{label} target_price",
    )
    risk_fraction = _parse_optional_float_v1a(
        row.get("one_r_risk_fraction"),
        f"{label} risk_fraction",
    )
    invalidation = metadata.invalidation_price
    if metadata.direction == "long":
        valid_invalidation = (
            invalidation is not None and 0 < invalidation < entry_price
        )
        risk = 0.0 if invalidation is None else entry_price - invalidation
        expected_target = entry_price + risk
    else:
        valid_invalidation = invalidation is not None and invalidation > entry_price
        risk = 0.0 if invalidation is None else invalidation - entry_price
        expected_target = entry_price - risk
        valid_invalidation = valid_invalidation and expected_target > 0
    if not valid_invalidation:
        if (
            status != "invalid_invalidation"
            or target is not None
            or risk_fraction is not None
            or observed != exit_time_ms
        ):
            raise IndicatorV1AContractError(
                f"{label} invalid-invalidation fields are inconsistent"
            )
        return
    if status not in {
        "target_first",
        "invalidation_first",
        "collision",
        "timeout",
    }:
        raise IndicatorV1AContractError(f"{label} status is inconsistent")
    if target is None or risk_fraction is None or observed is None:
        raise IndicatorV1AContractError(f"{label} valid path fields must be present")
    expected_risk_fraction = risk / entry_price
    _require_float_close_v1a(target, expected_target, f"{label} target_price")
    _require_float_close_v1a(
        risk_fraction,
        expected_risk_fraction,
        f"{label} risk_fraction",
    )
    if not entry_time_ms <= observed <= exit_time_ms:
        raise IndicatorV1AContractError(f"{label} observed time is outside path")
    if observed % _INTERVAL_MS not in {0, _INTERVAL_MS - 1}:
        raise IndicatorV1AContractError(
            f"{label} observed time is not a 5m open or close"
        )
    target_relation, invalidation_relation = _one_r_touch_relations_v1a(
        metadata.direction,
        maximum_rise,
        maximum_drop,
        expected_risk_fraction,
    )
    if status == "target_first" and target_relation < 0:
        raise IndicatorV1AContractError(f"{label} target status lacks path support")
    if status == "invalidation_first" and invalidation_relation < 0:
        raise IndicatorV1AContractError(
            f"{label} invalidation status lacks path support"
        )
    if status == "collision" and (
        target_relation < 0
        or invalidation_relation < 0
        or observed % _INTERVAL_MS != _INTERVAL_MS - 1
    ):
        raise IndicatorV1AContractError(f"{label} collision fields are inconsistent")
    if status == "timeout" and (
        target_relation > 0
        or invalidation_relation > 0
        or observed != exit_time_ms
    ):
        raise IndicatorV1AContractError(f"{label} timeout fields are inconsistent")


def _evaluable_outcome_micros_v1a(
    row: Mapping[str, str | None],
    *,
    metadata: _RecommendationMetadataV1A,
    horizon: int,
    line_number: int,
) -> int:
    label = f"outcomes line {line_number}"
    if row.get("exclusion_reason") != "":
        raise IndicatorV1AContractError(
            f"{label} gives an exclusion reason to an evaluable outcome"
        )
    entry_time_ms = _parse_csv_int(row.get("entry_time_ms"), f"{label} entry_time_ms")
    exit_time_ms = _parse_csv_int(row.get("exit_time_ms"), f"{label} exit_time_ms")
    expected_entry_ms = metadata.decision_time_ms + 1
    expected_exit_ms = expected_entry_ms + horizon * _INTERVAL_MS - 1
    if entry_time_ms != expected_entry_ms or exit_time_ms != expected_exit_ms:
        raise IndicatorV1AContractError(
            f"{label} entry/exit times differ from the frozen 5m horizon"
        )
    start_ms, end_ms = V1A_SPLIT_RANGES_MS[metadata.split]
    if (
        entry_time_ms < start_ms + _SPLIT_START_EMBARGO_BARS * _INTERVAL_MS
        or not start_ms <= entry_time_ms < end_ms
        or not entry_time_ms <= exit_time_ms < end_ms
    ):
        raise IndicatorV1AContractError(
            f"{label} evaluable path violates split boundaries or embargo"
        )
    entry_price = _parse_required_float_v1a(
        row.get("entry_price"), f"{label} entry_price"
    )
    exit_price = _parse_required_float_v1a(
        row.get("exit_price"), f"{label} exit_price"
    )
    if entry_price <= 0 or exit_price <= 0:
        raise IndicatorV1AContractError(f"{label} prices must be positive")
    raw_close_return = _parse_required_float_v1a(
        row.get("raw_close_return"), f"{label} raw_close_return"
    )
    maximum_rise = _parse_required_float_v1a(
        row.get("maximum_rise"), f"{label} maximum_rise"
    )
    maximum_drop = _parse_required_float_v1a(
        row.get("maximum_drop"), f"{label} maximum_drop"
    )
    gross_return = _parse_required_float_v1a(
        row.get("gross_return"), f"{label} gross_return"
    )
    slippage_return = _parse_required_float_v1a(
        row.get("slippage_return"), f"{label} slippage_return"
    )
    fee_return = _parse_required_float_v1a(
        row.get("fee_return"), f"{label} fee_return"
    )
    funding_return = _parse_required_float_v1a(
        row.get("funding_return"), f"{label} funding_return"
    )
    net_return = _parse_required_float_v1a(
        row.get("net_return"), f"{label} net_return"
    )
    mfe = _parse_required_float_v1a(row.get("mfe"), f"{label} mfe")
    mae = _parse_required_float_v1a(row.get("mae"), f"{label} mae")
    expected_raw = exit_price / entry_price - 1
    _require_float_close_v1a(
        raw_close_return,
        expected_raw,
        f"{label} raw_close_return",
    )
    path_tolerance = max(
        _OUTCOME_FLOAT_ABS_TOLERANCE,
        abs(raw_close_return) * _OUTCOME_FLOAT_REL_TOLERANCE,
    )
    if (
        maximum_rise < max(0.0, raw_close_return) - path_tolerance
        or maximum_drop > min(0.0, raw_close_return) + path_tolerance
    ):
        raise IndicatorV1AContractError(
            f"{label} maximum rise/drop does not contain open-to-close path"
        )
    direction = Direction.LONG if metadata.direction == "long" else Direction.SHORT
    fee_bps, slippage_bps = _V1A_EXECUTION_COST_BPS[
        (metadata.market, metadata.cohort)
    ]
    execution = calculate_execution_returns(
        direction,
        entry_price,
        exit_price,
        fee_bps,
        slippage_bps,
    )
    _require_float_close_v1a(
        gross_return,
        execution.gross_return,
        f"{label} gross_return",
    )
    _require_float_close_v1a(
        slippage_return,
        execution.slippage_return,
        f"{label} slippage_return",
    )
    _require_float_close_v1a(
        fee_return,
        execution.fee_return,
        f"{label} fee_return",
    )
    if metadata.market == "spot" and funding_return != 0.0:
        raise IndicatorV1AContractError(f"{label} Spot funding_return must be zero")
    _require_float_close_v1a(
        net_return,
        execution.net_before_funding + funding_return,
        f"{label} net_return",
    )
    expected_mfe = maximum_rise if metadata.direction == "long" else -maximum_drop
    expected_mae = maximum_drop if metadata.direction == "long" else -maximum_rise
    _require_float_close_v1a(mfe, expected_mfe, f"{label} mfe")
    _require_float_close_v1a(mae, expected_mae, f"{label} mae")
    for field, margin_bps in (
        ("hit_status_0bps", 0.0),
        ("hit_status_5bps", 5.0),
        ("hit_status_10bps", 10.0),
        ("hit_status_25bps", 25.0),
    ):
        if row.get(field) != _hit_status_v1a(net_return, margin_bps):
            raise IndicatorV1AContractError(f"{label} {field} does not reconcile")
    _validate_one_r_v1a(
        row,
        metadata=metadata,
        entry_price=entry_price,
        maximum_rise=maximum_rise,
        maximum_drop=maximum_drop,
        entry_time_ms=entry_time_ms,
        exit_time_ms=exit_time_ms,
        line_number=line_number,
    )
    return net_return_micros_v1a(cast(str, row.get("net_return")))


def _parse_outcomes(
    raw: bytes,
    *,
    recommendations: _RecommendationsV1A,
) -> _OutcomesV1A:
    reader = _csv_reader(raw, label="outcomes.csv")
    _require_csv_columns(reader, _OUTCOME_EXACT_COLUMNS, label="outcomes.csv")
    mutable: dict[str, dict[int, NetOutcomeV1A]] = defaultdict(dict)
    row_count = 0
    for line_number, row in enumerate(reader, start=2):
        _require_no_surplus_csv_cells(row, label=f"outcomes line {line_number}")
        if row_count >= V1A_MAX_OUTCOMES_PER_REPLAY:
            raise IndicatorV1AContractError("outcome row cap exceeded")
        row_count += 1
        event_id = (row.get("event_id") or "").strip()
        if event_id not in recommendations.event_ids:
            raise IndicatorV1AContractError(
                f"outcomes line {line_number} has orphan event_id"
            )
        horizon = _parse_csv_int(
            row.get("horizon_bars"), f"outcomes line {line_number} horizon_bars"
        )
        if horizon not in V1A_HORIZONS_BARS:
            raise IndicatorV1AContractError(
                f"outcomes line {line_number} has unknown horizon"
            )
        minutes = _parse_csv_int(
            row.get("horizon_minutes"),
            f"outcomes line {line_number} horizon_minutes",
        )
        if minutes != horizon * 5:
            raise IndicatorV1AContractError(
                f"outcomes line {line_number} horizon minutes conflict"
            )
        if horizon in mutable[event_id]:
            raise IndicatorV1AContractError(
                f"duplicate outcome horizon for event_id: {event_id}"
            )
        evaluable = _parse_csv_bool(
            row.get("evaluable"), f"outcomes line {line_number} evaluable"
        )
        metadata = recommendations.metadata_by_event_id[event_id]
        if evaluable:
            net_micros: int | None = _evaluable_outcome_micros_v1a(
                row,
                metadata=metadata,
                horizon=horizon,
                line_number=line_number,
            )
        else:
            _outcome_exclusion_reason_v1a(
                row,
                metadata=metadata,
                horizon=horizon,
                line_number=line_number,
            )
            net_micros = None
        mutable[event_id][horizon] = NetOutcomeV1A(
            horizon_bars=horizon,
            evaluable=evaluable,
            net_return_micros=net_micros,
        )
    expected_rows = len(recommendations.event_ids) * len(V1A_HORIZONS_BARS)
    if row_count != expected_rows:
        raise IndicatorV1AContractError(
            "outcome cardinality differs from events times five horizons"
        )
    if set(mutable) != set(recommendations.event_ids):
        raise IndicatorV1AContractError("one or more recommendation events lack outcomes")
    expected_horizons = set(V1A_HORIZONS_BARS)
    by_event: dict[str, tuple[NetOutcomeV1A, ...]] = {}
    for event_id, values in mutable.items():
        if set(values) != expected_horizons:
            raise IndicatorV1AContractError(
                f"event_id does not have the exact five horizons: {event_id}"
            )
        by_event[event_id] = tuple(values[horizon] for horizon in V1A_HORIZONS_BARS)
    return _OutcomesV1A(by_event_id=by_event, row_count=row_count)


def deduplicate_spot_priority_v1a(
    rows: Sequence[IntendedPopulationRowV1A],
) -> tuple[tuple[IntendedPopulationRowV1A, ...], int]:
    """Apply the frozen deterministic spot-over-futures population key."""

    retained, dropped = _deduplicate_spot_priority_with_dropped_v1a(rows)
    return retained, len(dropped)


def _population_sort_key_v1a(
    row: IntendedPopulationRowV1A,
) -> tuple[int, int, int, str, int, str]:
    return (
        V1A_SPLITS.index(row.split),
        row.decision_time_ms,
        V1A_ASSETS.index(row.asset),
        row.direction,
        V1A_MARKETS.index(row.market),
        row.event_id,
    )


def _deduplicate_spot_priority_with_dropped_v1a(
    rows: Sequence[IntendedPopulationRowV1A],
) -> tuple[
    tuple[IntendedPopulationRowV1A, ...],
    tuple[IntendedPopulationRowV1A, ...],
]:

    groups: dict[
        tuple[str, str, int],
        list[IntendedPopulationRowV1A],
    ] = defaultdict(list)
    for row in rows:
        if type(row) is not IntendedPopulationRowV1A:
            raise TypeError("population row must be exact IntendedPopulationRowV1A")
        groups[(row.asset, row.direction, row.decision_time_ms)].append(row)
    retained: list[IntendedPopulationRowV1A] = []
    dropped: list[IntendedPopulationRowV1A] = []
    for key, values in groups.items():
        markets = [value.market for value in values]
        if len(values) > 2 or len(markets) != len(set(markets)):
            raise IndicatorV1AContractError(
                f"ambiguous duplicate population group: {key}"
            )
        chosen = next((value for value in values if value.market == "spot"), values[0])
        retained.append(chosen)
        dropped.extend(value for value in values if value is not chosen)
    retained.sort(key=_population_sort_key_v1a)
    dropped.sort(key=_population_sort_key_v1a)
    return tuple(retained), tuple(dropped)


def _population_hash_v1a(rows: Sequence[IntendedPopulationRowV1A]) -> str:
    document = [
        {
            "asset": row.asset,
            "decision_time_ms": row.decision_time_ms,
            "direction": row.direction,
            "event_id": row.event_id,
            "features": [
                value.hex() if value is not None else None for value in row.features
            ],
            "market": row.market,
            "split": row.split,
        }
        for row in sorted(rows, key=_population_sort_key_v1a)
    ]
    return sha256_bytes(_canonical_json_bytes(document))


def _verified_replay_outputs(
    replay_dir: Path,
    manifest: dict[str, Any],
) -> tuple[dict[str, bytes], dict[str, str]]:
    outputs = _canonical_mapping(
        manifest.get("outputs"),
        label="run manifest outputs",
        expected_count=len(_REPLAY_OUTPUT_NAMES),
    )
    if {name for name, _ in outputs} != _REPLAY_OUTPUT_NAMES:
        raise IndicatorV1AContractError("run manifest output file set is not exact")
    raw_by_name: dict[str, bytes] = {}
    sha_by_name: dict[str, str] = {}
    for name, expected in outputs:
        path = replay_dir / name
        if not path.is_file():
            raise IndicatorV1AContractError(f"replay output is missing: {path}")
        raw = path.read_bytes()
        actual = sha256_bytes(raw)
        if actual != expected:
            raise IndicatorV1AContractError(f"replay output hash mismatch: {name}")
        raw_by_name[name] = raw
        sha_by_name[name] = actual
    return raw_by_name, sha_by_name


def _validate_results_document(
    results: dict[str, Any],
    *,
    expected_split: str,
    recommendation_rows: int,
    outcome_rows: int,
    recommendation_pair_counts: tuple[tuple[str, str, int], ...],
) -> str:
    if results.get("protocol_version") != V1A_REPLAY_PROTOCOL_VERSION:
        raise IndicatorV1AContractError("results replay protocol version is not exact")
    if results.get("rule_version") != V1A_RULE_VERSION:
        raise IndicatorV1AContractError("results rule version is not exact")
    if results.get("events") != recommendation_rows:
        raise IndicatorV1AContractError("results event count differs from recommendations")
    if results.get("outcome_rows") != outcome_rows:
        raise IndicatorV1AContractError("results outcome count differs from outcomes CSV")
    per_symbol = _require_list(results.get("per_symbol"), "results per_symbol")
    if len(per_symbol) != len(_V1A_EXPECTED_PAIR_ORDER):
        raise IndicatorV1AContractError(
            "results per_symbol must contain exactly 14 ordered market/asset rows"
        )
    expected_counts = {
        (market, asset): count
        for market, asset, count in recommendation_pair_counts
    }
    summed_events = 0
    summed_outcomes = 0
    for index, (expected_market, expected_asset) in enumerate(
        _V1A_EXPECTED_PAIR_ORDER
    ):
        row = _require_dict(per_symbol[index], f"results per_symbol[{index}]")
        if set(row) != {
            "market",
            "asset",
            "candles",
            "events",
            "outcome_rows",
            "duration_seconds",
        }:
            raise IndicatorV1AContractError(
                "results per_symbol row fields are not exact"
            )
        if (
            row.get("market") != expected_market
            or row.get("asset") != expected_asset
        ):
            raise IndicatorV1AContractError(
                "results per_symbol market/asset order is not exact"
            )
        candles = _require_int(
            row.get("candles"), f"results per_symbol[{index}] candles"
        )
        events = _require_int(
            row.get("events"), f"results per_symbol[{index}] events"
        )
        pair_outcomes = _require_int(
            row.get("outcome_rows"),
            f"results per_symbol[{index}] outcome_rows",
        )
        duration = row.get("duration_seconds")
        if (
            candles == 0
            or type(duration) not in {int, float}
            or not math.isfinite(float(cast(int | float, duration)))
            or float(cast(int | float, duration)) < 0
            or events != expected_counts[(expected_market, expected_asset)]
            or pair_outcomes != events * len(V1A_HORIZONS_BARS)
        ):
            raise IndicatorV1AContractError(
                "results per_symbol counts or duration do not reconcile"
            )
        summed_events += events
        summed_outcomes += pair_outcomes
    if summed_events != recommendation_rows or summed_outcomes != outcome_rows:
        raise IndicatorV1AContractError(
            "results per_symbol totals do not reconcile with replay CSV totals"
        )
    selection = _require_dict(results.get("selection"), "results selection")
    expected_start, expected_end = V1A_SPLIT_RANGES_UTC[expected_split]
    expected_selection = {
        "start_utc": expected_start,
        "end_utc": expected_end,
        "splits": [expected_split],
        "interval": V1A_INTERVAL,
        "assets": list(V1A_ASSETS),
        "markets": list(V1A_MARKETS),
    }
    for key, expected in expected_selection.items():
        if selection.get(key) != expected:
            raise IndicatorV1AContractError(f"results selection {key} is not exact")
    if selection.get("universe_mode") != "fixed_backtest_spec_assets_not_live_dynamic_top_n":
        raise IndicatorV1AContractError("results universe authority is not exact")
    evaluation = _require_dict(
        results.get("evaluation_contract"), "results evaluation_contract"
    )
    if evaluation.get("horizons_bars") != list(V1A_HORIZONS_BARS):
        raise IndicatorV1AContractError("results horizons are not exact")
    if evaluation.get("primary_horizon_bars") != V1A_PRIMARY_HORIZON_BARS:
        raise IndicatorV1AContractError("results primary horizon is not exact")
    costs = _require_dict(evaluation.get("costs"), "results costs")
    status = _require_dict(results.get("status"), "results status")
    _require_bool(
        status.get("independently_validated"),
        False,
        "results independently_validated",
    )
    _require_bool(
        status.get("deployment_approved"), False, "results deployment_approved"
    )
    return _canonical_json_bytes(costs).decode()


def _feature_not_ready_hash(rows: Sequence[FeatureNotReadyV1A]) -> str:
    document = [
        {
            "asset": row.asset,
            "decision_time_ms": row.decision_time_ms,
            "direction": row.direction,
            "event_id": row.event_id,
            "market": row.market,
            "missing_features": list(row.missing_features),
            "reason": "FEATURE_NOT_READY",
            "split": row.split,
        }
        for row in rows
    ]
    return sha256_bytes(_canonical_json_bytes(document))


def _parse_replay_dir_v1a(
    replay_dir: str | Path,
    *,
    authority: FrozenAuthorityV1A,
) -> _ParsedReplayV1A:
    root = Path(replay_dir).resolve()
    manifest_path = root / "run_manifest.json"
    if not manifest_path.is_file():
        raise IndicatorV1AContractError(f"run manifest is missing: {manifest_path}")
    manifest_raw = manifest_path.read_bytes()
    manifest = _json_from_bytes(manifest_raw, "run_manifest.json")
    if manifest.get("protocol_version") != V1A_REPLAY_PROTOCOL_VERSION:
        raise IndicatorV1AContractError("run manifest replay protocol is not exact")
    if manifest.get("rule_version") != V1A_RULE_VERSION:
        raise IndicatorV1AContractError("run manifest rule version is not exact")
    _, started_at = _parse_canonical_utc(
        manifest.get("started_at_utc"), "run manifest started_at_utc"
    )
    _, completed_at = _parse_canonical_utc(
        manifest.get("completed_at_utc"), "run manifest completed_at_utc"
    )
    if not authority.created_at_datetime < started_at <= completed_at:
        raise IndicatorV1AContractError(
            "run manifest time ordering must be freeze < start <= completion"
        )
    duration_value = manifest.get("duration_seconds")
    if type(duration_value) not in {int, float}:
        raise IndicatorV1AContractError(
            "run manifest duration_seconds must be a finite nonnegative number"
        )
    duration_seconds = float(cast(int | float, duration_value))
    wall_seconds = (completed_at - started_at).total_seconds()
    if (
        not math.isfinite(duration_seconds)
        or duration_seconds < 0
        or abs(duration_seconds - wall_seconds) > max(1.0, wall_seconds * 0.05)
    ):
        raise IndicatorV1AContractError(
            "run manifest duration_seconds is inconsistent with canonical UTC times"
        )
    runtime_workspace = _runtime_workspace_root()
    expected_spec_path = (
        runtime_workspace
        / "config/backtest.5m.indicator-discriminator-v1a-7asset.yaml"
    )
    expected_config_path = runtime_workspace / "config/settings.example.yaml"
    if (
        Path(_require_string(manifest.get("spec_path"), "run manifest spec_path"))
        .resolve()
        != expected_spec_path
        or Path(
            _require_string(manifest.get("config_path"), "run manifest config_path")
        ).resolve()
        != expected_config_path
    ):
        raise IndicatorV1AContractError(
            "run manifest spec/config paths are not bound to the runtime workspace"
        )
    spec_sha256 = _require_sha256(
        manifest.get("spec_sha256"), "run manifest spec_sha256"
    )
    config_sha256 = _require_sha256(
        manifest.get("config_sha256"), "run manifest config_sha256"
    )
    code_sha256 = _require_sha256(
        manifest.get("code_sha256"), "run manifest code_sha256"
    )
    if (
        spec_sha256 != authority.spec_sha256
        or config_sha256 != authority.config_sha256
        or code_sha256 != authority.source_code_sha256
    ):
        raise IndicatorV1AContractError(
            "run manifest spec/config/code differs from pre-replay freeze"
        )
    inputs = _canonical_mapping(
        manifest.get("inputs"),
        label="run manifest inputs",
        expected_count=V1A_EXPECTED_INPUT_COUNT,
    )
    if inputs != authority.data_input_sha256:
        raise IndicatorV1AContractError(
            "run manifest inputs differ from pre-replay frozen data"
        )
    raw_outputs, output_hashes = _verified_replay_outputs(root, manifest)
    results = _json_from_bytes(raw_outputs["results.json"], "results.json")
    selection = _require_dict(results.get("selection"), "results selection")
    selected_splits = _require_list(selection.get("splits"), "results selection splits")
    if len(selected_splits) != 1 or selected_splits[0] not in V1A_SPLITS:
        raise IndicatorV1AContractError(
            "each V1A replay must contain exactly one frozen split"
        )
    expected_split = cast(str, selected_splits[0])
    recommendations = _parse_recommendations(
        raw_outputs["recommendations.csv"],
        expected_split=expected_split,
    )
    deduplicated, dropped_rows = _deduplicate_spot_priority_with_dropped_v1a(
        recommendations.population_rows
    )
    if (
        len(recommendations.population_rows)
        != len(deduplicated) + len(dropped_rows)
    ):
        raise IndicatorV1AContractError(
            "intended population does not reconcile with retained and dropped rows"
        )
    complete_rows: list[IntendedPopulationRowV1A] = []
    not_ready: list[FeatureNotReadyV1A] = []
    for row in deduplicated:
        missing = tuple(
            name
            for name, value in zip(FEATURE_COLUMNS, row.features, strict=True)
            if value is None
        )
        if missing:
            not_ready.append(
                FeatureNotReadyV1A(
                    event_id=row.event_id,
                    asset=row.asset,
                    market=row.market,
                    direction=row.direction,
                    decision_time_ms=row.decision_time_ms,
                    split=row.split,
                    missing_features=missing,
                )
            )
        else:
            complete_rows.append(row)
    # Outcome bytes were authenticated above, but no label or return value is
    # parsed until the outcome-blind population, deduplication, and complete-
    # case eligibility decisions are immutable.
    outcomes = _parse_outcomes(
        raw_outputs["outcomes.csv"],
        recommendations=recommendations,
    )
    not_ready = [
        FeatureNotReadyV1A(
            event_id=row.event_id,
            asset=row.asset,
            market=row.market,
            direction=row.direction,
            decision_time_ms=row.decision_time_ms,
            split=row.split,
            missing_features=row.missing_features,
            outcomes=outcomes.by_event_id[row.event_id],
        )
        for row in not_ready
    ]
    costs_json = _validate_results_document(
        results,
        expected_split=expected_split,
        recommendation_rows=recommendations.row_count,
        outcome_rows=outcomes.row_count,
        recommendation_pair_counts=recommendations.pair_counts,
    )
    analyzable: list[AnalyzableEventV1A] = []
    for row in complete_rows:
        complete_features = cast(tuple[float, ...], row.features)
        analyzable.append(
            AnalyzableEventV1A(
                event_id=row.event_id,
                asset=row.asset,
                market=row.market,
                direction=row.direction,
                decision_time_ms=row.decision_time_ms,
                split=row.split,
                features=complete_features,
                outcomes=outcomes.by_event_id[row.event_id],
            )
        )
    not_ready.sort(
        key=lambda row: (
            row.decision_time_ms,
            V1A_ASSETS.index(row.asset),
            row.direction,
            row.event_id,
        )
    )
    analyzable.sort(
        key=lambda row: (
            row.decision_time_ms,
            V1A_ASSETS.index(row.asset),
            row.direction,
            row.event_id,
        )
    )
    not_ready_hash = _feature_not_ready_hash(not_ready)
    identity = _ReplayIdentityV1A(
        spec_sha256=spec_sha256,
        config_sha256=config_sha256,
        code_sha256=code_sha256,
        protocol_version=V1A_REPLAY_PROTOCOL_VERSION,
        rule_version=V1A_RULE_VERSION,
        inputs=inputs,
        costs_json=costs_json,
    )
    audit = ReplayInputAuditV1A(
        split=expected_split,
        replay_dir=root,
        run_manifest_sha256=sha256_bytes(manifest_raw),
        recommendations_sha256=output_hashes["recommendations.csv"],
        outcomes_sha256=output_hashes["outcomes.csv"],
        results_sha256=output_hashes["results.json"],
        report_sha256=output_hashes["report_ko.md"],
        recommendation_rows=recommendations.row_count,
        outcome_rows=outcomes.row_count,
        intended_population_rows=len(recommendations.population_rows),
        intended_population_sha256=_population_hash_v1a(
            recommendations.population_rows
        ),
        deduplicated_population_rows=len(deduplicated),
        retained_population_sha256=_population_hash_v1a(deduplicated),
        duplicate_rows_dropped=len(dropped_rows),
        dropped_population_sha256=_population_hash_v1a(dropped_rows),
        complete_case_rows=len(analyzable),
        feature_not_ready_rows=len(not_ready),
        feature_not_ready_sha256=not_ready_hash,
    )
    return _ParsedReplayV1A(
        identity=identity,
        full_event_ids=recommendations.event_ids,
        events=tuple(analyzable),
        feature_not_ready=tuple(not_ready),
        audit=audit,
    )


def load_indicator_v1a_inputs(
    *,
    freeze_manifest_path: str | Path,
    replay_dirs: Sequence[str | Path],
    workspace_root: str | Path,
) -> LoadedIndicatorV1A:
    """Load exactly three frozen V1A replays without making an efficacy claim."""

    if type(replay_dirs) not in {tuple, list} or len(replay_dirs) != len(V1A_SPLITS):
        raise IndicatorV1AContractError("V1A requires exactly three replay directories")
    resolved = tuple(Path(value).resolve() for value in replay_dirs)
    if len(set(resolved)) != len(resolved):
        raise IndicatorV1AContractError("V1A replay directories must be distinct")
    authority = load_frozen_authority_v1a(
        freeze_manifest_path,
        workspace_root=workspace_root,
    )
    parsed = tuple(
        _parse_replay_dir_v1a(replay_dir, authority=authority)
        for replay_dir in resolved
    )
    by_split = {value.audit.split: value for value in parsed}
    if set(by_split) != set(V1A_SPLITS) or len(by_split) != len(parsed):
        raise IndicatorV1AContractError(
            "V1A replay directories must map one-to-one to frozen splits"
        )
    identities = {value.identity for value in parsed}
    if len(identities) != 1:
        raise IndicatorV1AContractError(
            "V1A replay identity or execution cost contract differs across splits"
        )
    seen_event_ids: set[str] = set()
    events: list[AnalyzableEventV1A] = []
    not_ready: list[FeatureNotReadyV1A] = []
    audits: list[ReplayInputAuditV1A] = []
    for split in V1A_SPLITS:
        value = by_split[split]
        duplicate = seen_event_ids & value.full_event_ids
        if duplicate:
            raise IndicatorV1AContractError(
                f"event_id repeats across splits: {min(duplicate)}"
            )
        seen_event_ids.update(value.full_event_ids)
        events.extend(value.events)
        not_ready.extend(value.feature_not_ready)
        audits.append(value.audit)
    return LoadedIndicatorV1A(
        authority=authority,
        events=tuple(events),
        feature_not_ready=tuple(not_ready),
        audits=tuple(audits),
    )


def load_frozen_authority_v1a(
    manifest_path: str | Path,
    *,
    workspace_root: str | Path,
) -> FrozenAuthorityV1A:
    """Validate the exact pre-replay freeze against the current workspace."""

    path = Path(manifest_path).resolve()
    workspace = Path(workspace_root).resolve()
    runtime_workspace = _runtime_workspace_root()
    if workspace != runtime_workspace:
        raise IndicatorV1AContractError(
            "workspace_root must be the repository owning the executing V1A module"
        )
    raw = path.read_bytes() if path.is_file() else b""
    if not raw:
        raise IndicatorV1AContractError("V1A freeze manifest is missing or empty")
    document = _json_from_bytes(raw, "V1A freeze manifest")
    if document.get("schema_version") != V1A_SCHEMA_VERSION:
        raise IndicatorV1AContractError("V1A freeze schema version is not exact")
    if document.get("status") != V1A_FREEZE_STATUS:
        raise IndicatorV1AContractError("V1A freeze is not sealed before replay")
    created_at_utc, created_at = _parse_canonical_utc(
        document.get("created_at_utc"), "freeze created_at_utc"
    )
    _require_bool(
        document.get("external_anchor"),
        False,
        "freeze external_anchor",
    )
    _require_bool(
        document.get("historical_only"),
        True,
        "freeze historical_only",
    )
    _require_bool(
        document.get("independent_validation_claim_allowed"),
        False,
        "freeze independent_validation_claim_allowed",
    )
    _require_bool(
        document.get("deployment_approved"),
        False,
        "freeze deployment_approved",
    )
    _require_bool(
        document.get("probability_calibrated"),
        False,
        "freeze probability_calibrated",
    )
    if document.get("exposure_status") != V1A_EXPOSURE_STATUS:
        raise IndicatorV1AContractError("V1A exposure status is not exact")

    contract = _require_dict(document.get("experiment_contract"), "experiment_contract")
    if contract.get("ordered_assets") != list(V1A_ASSETS):
        raise IndicatorV1AContractError("freeze ordered asset universe is not exact")
    if contract.get("markets") != list(V1A_MARKETS):
        raise IndicatorV1AContractError("freeze market order is not exact")
    if contract.get("interval") != V1A_INTERVAL:
        raise IndicatorV1AContractError("freeze interval is not 5m")
    if contract.get("horizons_bars") != list(V1A_HORIZONS_BARS):
        raise IndicatorV1AContractError("freeze horizon contract is not exact")
    if contract.get("primary_horizon_bars") != V1A_PRIMARY_HORIZON_BARS:
        raise IndicatorV1AContractError("freeze primary horizon is not exact")
    if contract.get("replay_protocol_version") != V1A_REPLAY_PROTOCOL_VERSION:
        raise IndicatorV1AContractError("freeze replay protocol version is not exact")
    if contract.get("rule_version") != V1A_RULE_VERSION:
        raise IndicatorV1AContractError("freeze rule version is not exact")
    if contract.get("feature_policy") != V1A_FEATURE_POLICY:
        raise IndicatorV1AContractError("freeze feature policy is not exact")

    splits = _require_dict(contract.get("splits"), "freeze splits")
    if set(splits) != set(V1A_SPLITS):
        raise IndicatorV1AContractError("freeze split names are not exact")
    for split in V1A_SPLITS:
        value = _require_dict(splits[split], f"freeze split {split}")
        if set(value) != {"start_ms", "end_ms"}:
            raise IndicatorV1AContractError(f"freeze split {split} fields are not exact")
        expected_start, expected_end = V1A_SPLIT_RANGES_MS[split]
        if (
            _require_int(value.get("start_ms"), f"freeze {split} start_ms")
            != expected_start
            or _require_int(value.get("end_ms"), f"freeze {split} end_ms")
            != expected_end
        ):
            raise IndicatorV1AContractError(f"freeze split {split} range is not exact")

    population = _require_dict(contract.get("population"), "freeze population")
    if set(population) != {"information_only", "stage", "families", "score"}:
        raise IndicatorV1AContractError("freeze population fields are not exact")
    _require_bool(
        population.get("information_only"),
        True,
        "freeze population information_only",
    )
    if (
        population.get("stage") != "setup"
        or population.get("families") != list(V1A_POPULATION_FAMILIES)
        or population.get("score") != "100"
    ):
        raise IndicatorV1AContractError("freeze population predicate is not exact")

    deduplication = _require_dict(
        contract.get("deduplication"), "freeze deduplication"
    )
    if deduplication != {
        "key": ["asset", "direction", "decision_time_ms"],
        "priority": ["spot", "futures"],
    }:
        raise IndicatorV1AContractError("freeze deduplication contract is not exact")
    bootstrap = _require_dict(contract.get("bootstrap"), "freeze bootstrap")
    if bootstrap != {
        "block_days": V1A_BOOTSTRAP_BLOCK_DAYS,
        "samples": V1A_BOOTSTRAP_SAMPLES,
        "seed": V1A_BOOTSTRAP_SEED,
    }:
        raise IndicatorV1AContractError("freeze bootstrap contract is not exact")

    spec_sha256 = _require_sha256(document.get("spec_sha256"), "freeze spec_sha256")
    spec_semantics_sha256 = _require_sha256(
        document.get("spec_semantics_sha256"),
        "freeze spec_semantics_sha256",
    )
    config_sha256 = _require_sha256(
        document.get("config_sha256"), "freeze config_sha256"
    )
    settings_semantics_sha256 = _require_sha256(
        document.get("settings_semantics_sha256"),
        "freeze settings_semantics_sha256",
    )
    source_sha256 = _require_sha256(
        document.get("source_code_sha256"), "freeze source_code_sha256"
    )
    if document.get("expected_input_count") != V1A_EXPECTED_INPUT_COUNT:
        raise IndicatorV1AContractError("freeze expected_input_count is not exact")
    data_inputs = _canonical_mapping(
        document.get("data_input_sha256"),
        label="freeze data_input_sha256",
        expected_count=V1A_EXPECTED_INPUT_COUNT,
    )
    if {key for key, _ in data_inputs} != V1A_EXPECTED_DATA_INPUT_KEYS:
        raise IndicatorV1AContractError("freeze data input key set is not exact")
    for relative, expected in data_inputs:
        actual = _hash_file(workspace / "data" / "backtest" / Path(relative))
        if actual != expected:
            raise IndicatorV1AContractError(
                f"frozen data input hash mismatch: {relative}"
            )

    frozen_files = _canonical_mapping(
        document.get("file_sha256"),
        label="freeze file_sha256",
        expected_count=len(V1A_REQUIRED_FROZEN_FILE_PATHS),
    )
    if {key for key, _ in frozen_files} != V1A_REQUIRED_FROZEN_FILE_PATHS:
        raise IndicatorV1AContractError("freeze file path set is not exact")
    for relative, expected in frozen_files:
        actual = _hash_file(workspace / Path(relative))
        if actual != expected:
            raise IndicatorV1AContractError(
                f"frozen workspace file hash mismatch: {relative}"
            )
    file_map = dict(frozen_files)
    spec_path = "config/backtest.5m.indicator-discriminator-v1a-7asset.yaml"
    config_path = "config/settings.example.yaml"
    if file_map[spec_path] != spec_sha256 or file_map[config_path] != config_sha256:
        raise IndicatorV1AContractError(
            "freeze top-level spec/config hashes differ from frozen file hashes"
        )
    spec = _load_and_validate_v1a_spec(workspace / spec_path)
    actual_spec_semantics_sha256 = _semantic_sha256(spec.model_dump(mode="json"))
    if actual_spec_semantics_sha256 != spec_semantics_sha256:
        raise IndicatorV1AContractError(
            "freeze spec semantic hash differs from the env-independent parsed spec"
        )
    settings = _load_settings_without_environment(workspace / config_path)
    _validate_v1a_settings(settings)
    actual_settings_semantics_sha256 = _semantic_sha256(
        settings.model_dump(mode="json")
    )
    if actual_settings_semantics_sha256 != settings_semantics_sha256:
        raise IndicatorV1AContractError(
            "freeze settings semantic hash differs from env-disabled settings"
        )
    current_source_sha256 = source_code_digest(workspace)
    if current_source_sha256 != source_sha256:
        raise IndicatorV1AContractError(
            "current Python source digest differs from the pre-replay freeze"
        )
    return FrozenAuthorityV1A(
        manifest_path=path,
        manifest_sha256=sha256_bytes(raw),
        created_at_utc=created_at_utc,
        created_at_ms=int(created_at.timestamp() * 1000),
        created_at_datetime=created_at,
        spec_sha256=spec_sha256,
        spec_semantics_sha256=spec_semantics_sha256,
        config_sha256=config_sha256,
        settings_semantics_sha256=settings_semantics_sha256,
        source_code_sha256=source_sha256,
        data_input_sha256=data_inputs,
        file_sha256=frozen_files,
    )


def _legacy_observation_v1a(event: AnalyzableEventV1A) -> IndicatorObservation:
    """Adapt feature-only V1A input to the unchanged V1 score implementation."""

    return IndicatorObservation(
        event_id=event.event_id,
        asset=event.asset,
        cohort="V1A_FROZEN_SEVEN_ASSET",
        market=event.market,
        direction=event.direction,
        decision_time_ms=event.decision_time_ms,
        split=event.split,
        features=event.features,
        recommendation_price=1.0,
        outcome_available=False,
        evaluable=False,
        future_close=None,
    )


def fit_and_score_indicator_v1a(
    loaded: LoadedIndicatorV1A,
) -> tuple[PercentileCompositeModel, tuple[ScoredEventV1A, ...]]:
    """Reuse V1's label-free ECDF, four axes, and type-7 top-quartile fit.

    Only complete-case development features enter the fit.  Outcomes are not
    adapted into the V1 object and therefore cannot affect either the ECDFs or
    the cutoff.
    """

    if type(loaded) is not LoadedIndicatorV1A:
        raise TypeError("loaded input must be exact LoadedIndicatorV1A")
    development = tuple(
        _legacy_observation_v1a(event)
        for event in loaded.events
        if event.split == "development"
    )
    if not development:
        raise IndicatorV1AContractError(
            "V1A development split has no complete-case event to fit"
        )
    try:
        model = fit_percentile_composite(development)
        scored_legacy = score_observations(
            tuple(_legacy_observation_v1a(event) for event in loaded.events),
            model,
        )
    except ValueError as exc:
        raise IndicatorV1AContractError("V1A frozen score fitting failed") from exc
    event_by_id = {event.event_id: event for event in loaded.events}
    if len(event_by_id) != len(loaded.events):
        raise IndicatorV1AContractError("V1A complete-case event IDs are not unique")
    scored: list[ScoredEventV1A] = []
    for value in scored_legacy:
        if value.composite_score is None or any(
            axis is None for axis in value.axis_scores
        ):
            raise IndicatorV1AContractError(
                "complete-case V1A event did not receive all four axis scores"
            )
        scored.append(
            ScoredEventV1A(
                event=event_by_id[value.observation.event_id],
                axis_scores=cast(tuple[float, ...], value.axis_scores),
                composite_score=value.composite_score,
                selected=value.composite_score >= model.top_quartile_cutoff,
            )
        )
    return model, tuple(scored)


def _outcome_micros_v1a(
    row: AnalyzableEventV1A | FeatureNotReadyV1A,
    horizon_bars: int,
) -> int | None:
    if horizon_bars not in V1A_HORIZONS_BARS:
        raise IndicatorV1AContractError("unknown V1A outcome horizon")
    matching = tuple(
        value for value in row.outcomes if value.horizon_bars == horizon_bars
    )
    if len(matching) != 1:
        raise IndicatorV1AContractError(
            "V1A event does not retain exactly one requested horizon"
        )
    outcome = matching[0]
    if outcome.evaluable != (outcome.net_return_micros is not None):
        raise IndicatorV1AContractError("V1A outcome evaluability is inconsistent")
    return outcome.net_return_micros


def summarize_outcomes_v1a(
    rows: Sequence[AnalyzableEventV1A | FeatureNotReadyV1A],
    *,
    horizon_bars: int,
) -> OutcomeMetricsV1A:
    """Summarize one population using signed after-cost return micros."""

    values = sorted(
        value
        for row in rows
        if (value := _outcome_micros_v1a(row, horizon_bars)) is not None
    )
    count = len(values)
    total = sum(values)
    mean = total / count if count else None
    if not values:
        median = None
    elif count % 2:
        median = float(values[count // 2])
    else:
        median = (values[count // 2 - 1] + values[count // 2]) / 2
    hits = sum(value > 0 for value in values)
    gross_profit = sum(value for value in values if value > 0)
    gross_loss = -sum(value for value in values if value < 0)
    if gross_loss:
        profit_factor = gross_profit / gross_loss
        profit_factor_status = "FINITE"
    elif gross_profit:
        profit_factor = None
        profit_factor_status = "POSITIVE_INFINITY_NO_LOSSES"
    else:
        profit_factor = None
        profit_factor_status = "UNDEFINED_NO_PROFIT_OR_LOSS"
    return OutcomeMetricsV1A(
        population_events=len(rows),
        evaluable_events=count,
        missing_events=len(rows) - count,
        sum_net_return_micros=total,
        mean_net_return_micros=mean,
        median_net_return_micros=median,
        strict_hits=hits,
        strict_hit_rate=hits / count if count else None,
        gross_profit_micros=gross_profit,
        gross_loss_abs_micros=gross_loss,
        profit_factor=profit_factor,
        profit_factor_status=profit_factor_status,
    )


def _ratio_difference_positive_v1a(
    left_sum: int,
    left_count: int,
    right_sum: int,
    right_count: int,
) -> bool:
    return (
        left_count > 0
        and right_count > 0
        and left_sum * right_count > right_sum * left_count
    )


def evaluate_validation_gate_v1a(
    evidence: ValidationGateEvidenceV1A,
) -> dict[str, object]:
    """Apply every frozen validation-h12 threshold with exact boundaries."""

    if type(evidence) is not ValidationGateEvidenceV1A:
        raise TypeError("gate evidence must be exact ValidationGateEvidenceV1A")
    if evidence.asset_count != len(V1A_ASSETS):
        raise IndicatorV1AContractError("validation gate requires all seven assets")
    nonnegative = (
        evidence.selected_evaluable,
        evidence.baseline_evaluable,
        evidence.selected_gross_profit_micros,
        evidence.selected_gross_loss_abs_micros,
        evidence.long_selected_evaluable,
        evidence.short_selected_evaluable,
        evidence.selected_events,
        evidence.intended_population_events,
        evidence.complete_case_events,
        evidence.positive_asset_uplifts,
        evidence.selected_mean_valid_replicates,
        evidence.uplift_mean_valid_replicates,
        evidence.bootstrap_samples,
    )
    if any(value < 0 for value in nonnegative):
        raise IndicatorV1AContractError("validation gate counts must be nonnegative")
    if evidence.selected_events > evidence.complete_case_events:
        raise IndicatorV1AContractError(
            "selected events cannot exceed complete-case events"
        )
    if evidence.complete_case_events > evidence.intended_population_events:
        raise IndicatorV1AContractError(
            "complete-case events cannot exceed intended population"
        )
    if not 0 <= evidence.positive_asset_uplifts <= evidence.asset_count:
        raise IndicatorV1AContractError("positive asset uplift count is impossible")
    if evidence.selected_evaluable > evidence.selected_events:
        raise IndicatorV1AContractError(
            "selected evaluable count cannot exceed selected events"
        )
    if evidence.baseline_evaluable > evidence.intended_population_events:
        raise IndicatorV1AContractError(
            "baseline evaluable count cannot exceed intended population"
        )
    if evidence.baseline_evaluable < evidence.selected_evaluable:
        raise IndicatorV1AContractError(
            "baseline evaluable count cannot be smaller than selected evaluable"
        )
    if (
        evidence.long_selected_evaluable + evidence.short_selected_evaluable
        != evidence.selected_evaluable
    ):
        raise IndicatorV1AContractError(
            "direction evaluable counts must partition selected evaluable"
        )
    if (
        evidence.long_selected_sum_micros + evidence.short_selected_sum_micros
        != evidence.selected_sum_micros
    ):
        raise IndicatorV1AContractError(
            "direction sums must partition selected return micros"
        )
    if (
        evidence.selected_gross_profit_micros
        - evidence.selected_gross_loss_abs_micros
        != evidence.selected_sum_micros
    ):
        raise IndicatorV1AContractError(
            "gross profit and loss must reconcile to selected return micros"
        )
    if evidence.bootstrap_samples != V1A_BOOTSTRAP_SAMPLES:
        raise IndicatorV1AContractError(
            "confirmatory bootstrap sample count is not frozen"
        )
    if (
        evidence.selected_mean_valid_replicates > evidence.bootstrap_samples
        or evidence.uplift_mean_valid_replicates > evidence.bootstrap_samples
    ):
        raise IndicatorV1AContractError(
            "confirmatory valid replicates cannot exceed requested samples"
        )

    criteria = {
        "selected_mean_net_return_strictly_positive": (
            evidence.selected_evaluable > 0 and evidence.selected_sum_micros > 0
        ),
        "selected_mean_one_sided_basic_95_lower_strictly_positive": (
            evidence.selected_mean_lower_micros is not None
            and math.isfinite(evidence.selected_mean_lower_micros)
            and evidence.selected_mean_lower_micros > 0
        ),
        "selected_minus_baseline_mean_strictly_positive": (
            _ratio_difference_positive_v1a(
                evidence.selected_sum_micros,
                evidence.selected_evaluable,
                evidence.baseline_sum_micros,
                evidence.baseline_evaluable,
            )
        ),
        "selected_minus_baseline_one_sided_basic_95_lower_strictly_positive": (
            evidence.uplift_mean_lower_micros is not None
            and math.isfinite(evidence.uplift_mean_lower_micros)
            and evidence.uplift_mean_lower_micros > 0
        ),
        "selected_profit_factor_strictly_greater_than_one": (
            evidence.selected_gross_profit_micros
            > evidence.selected_gross_loss_abs_micros
        ),
        "selected_median_net_return_strictly_positive": (
            evidence.selected_median_micros is not None
            and math.isfinite(evidence.selected_median_micros)
            and evidence.selected_median_micros > 0
        ),
        "long_selected_mean_strictly_positive": (
            evidence.long_selected_evaluable > 0
            and evidence.long_selected_sum_micros > 0
        ),
        "short_selected_mean_strictly_positive": (
            evidence.short_selected_evaluable > 0
            and evidence.short_selected_sum_micros > 0
        ),
        "selected_retention_at_least_20_percent": (
            evidence.intended_population_events > 0
            and evidence.selected_events * 5
            >= evidence.intended_population_events
        ),
        "selected_evaluable_at_least_300": evidence.selected_evaluable >= 300,
        "positive_asset_uplift_at_least_6_of_7": (
            evidence.positive_asset_uplifts >= 6
        ),
        "complete_case_coverage_at_least_99_percent": (
            evidence.intended_population_events > 0
            and evidence.complete_case_events * 100
            >= evidence.intended_population_events * 99
        ),
        "confirmatory_mean_bootstrap_all_10000_replicates_valid": (
            evidence.selected_mean_valid_replicates == evidence.bootstrap_samples
            and evidence.uplift_mean_valid_replicates == evidence.bootstrap_samples
        ),
    }
    return {
        "overall_pass": all(criteria.values()),
        "criteria": criteria,
        "confirmatory_split": "validation",
        "confirmatory_horizon_bars": V1A_PRIMARY_HORIZON_BARS,
        "historical_only": True,
        "independently_validated": False,
        "probability_calibrated": False,
        "deployment_approved": False,
    }


def _matches_dimension_v1a(
    row: AnalyzableEventV1A | FeatureNotReadyV1A,
    dimension: str,
    value: str,
) -> bool:
    if dimension == "overall":
        return value == "all"
    if dimension == "direction":
        return row.direction == value
    if dimension == "asset":
        return row.asset == value
    raise IndicatorV1AContractError("unknown V1A evaluation dimension")


def _point_difference_v1a(
    selected: OutcomeMetricsV1A,
    baseline: OutcomeMetricsV1A,
    *,
    metric: Literal["mean", "hit"],
) -> float | None:
    if metric == "mean":
        left = selected.mean_net_return_micros
        right = baseline.mean_net_return_micros
    else:
        left = selected.strict_hit_rate
        right = baseline.strict_hit_rate
    if left is None or right is None:
        return None
    return left - right


def build_point_evaluations_v1a(
    loaded: LoadedIndicatorV1A,
    scored: Sequence[ScoredEventV1A],
) -> list[dict[str, object]]:
    """Build all frozen after-cost baseline/selected point-metric cells.

    `FEATURE_NOT_READY` rows remain in every applicable baseline population and
    retention denominator.  They can never enter the selected population.
    """

    if type(loaded) is not LoadedIndicatorV1A:
        raise TypeError("loaded input must be exact LoadedIndicatorV1A")
    scored_by_id: dict[str, ScoredEventV1A] = {}
    for value in scored:
        if type(value) is not ScoredEventV1A:
            raise TypeError("scored row must be exact ScoredEventV1A")
        if value.event.event_id in scored_by_id:
            raise IndicatorV1AContractError("duplicate scored V1A event ID")
        scored_by_id[value.event.event_id] = value
    expected_ids = {event.event_id for event in loaded.events}
    if set(scored_by_id) != expected_ids:
        raise IndicatorV1AContractError(
            "scored V1A event set differs from complete-case input"
        )

    dimensions = (
        ("overall", ("all",)),
        ("direction", ("long", "short")),
        ("asset", V1A_ASSETS),
    )
    rows: list[dict[str, object]] = []
    for split in V1A_SPLITS:
        split_complete = tuple(event for event in loaded.events if event.split == split)
        split_not_ready = tuple(
            event for event in loaded.feature_not_ready if event.split == split
        )
        split_baseline: tuple[AnalyzableEventV1A | FeatureNotReadyV1A, ...] = (
            *split_complete,
            *split_not_ready,
        )
        for horizon in V1A_HORIZONS_BARS:
            for dimension, values in dimensions:
                for value in values:
                    baseline = tuple(
                        event
                        for event in split_baseline
                        if _matches_dimension_v1a(event, dimension, value)
                    )
                    complete = tuple(
                        event
                        for event in split_complete
                        if _matches_dimension_v1a(event, dimension, value)
                    )
                    selected = tuple(
                        event
                        for event in complete
                        if scored_by_id[event.event_id].selected
                    )
                    baseline_metrics = summarize_outcomes_v1a(
                        baseline,
                        horizon_bars=horizon,
                    )
                    selected_metrics = summarize_outcomes_v1a(
                        selected,
                        horizon_bars=horizon,
                    )
                    baseline_count = len(baseline)
                    rows.append(
                        {
                            "split": split,
                            "horizon_bars": horizon,
                            "horizon_minutes": horizon * 5,
                            "dimension": dimension,
                            "value": value,
                            "confirmatory": (
                                split == "validation"
                                and horizon == V1A_PRIMARY_HORIZON_BARS
                                and dimension == "overall"
                            ),
                            "baseline_includes_feature_not_ready": True,
                            "complete_case_events": len(complete),
                            "feature_not_ready_events": baseline_count - len(complete),
                            "complete_case_coverage": (
                                len(complete) / baseline_count if baseline_count else None
                            ),
                            "selected_events": len(selected),
                            "retention": (
                                len(selected) / baseline_count if baseline_count else None
                            ),
                            "baseline": baseline_metrics.artifact(),
                            "selected": selected_metrics.artifact(),
                            "selected_minus_baseline": {
                                "mean_net_return_micros": _point_difference_v1a(
                                    selected_metrics,
                                    baseline_metrics,
                                    metric="mean",
                                ),
                                "strict_hit_rate": _point_difference_v1a(
                                    selected_metrics,
                                    baseline_metrics,
                                    metric="hit",
                                ),
                            },
                            "secondary_descriptive_only": not (
                                split == "validation"
                                and horizon == V1A_PRIMARY_HORIZON_BARS
                                and dimension == "overall"
                            ),
                        }
                    )
    return rows


def build_shared_bootstrap_schedule_v1a(
    split: str,
    *,
    samples: int = V1A_BOOTSTRAP_SAMPLES,
    seed: int = V1A_BOOTSTRAP_SEED,
) -> SharedBootstrapScheduleV1A:
    """Build the one frozen full-calendar circular schedule for a split."""

    if split not in V1A_SPLITS:
        raise IndicatorV1AContractError("bootstrap split is not frozen")
    if samples != V1A_BOOTSTRAP_SAMPLES or seed != V1A_BOOTSTRAP_SEED:
        raise IndicatorV1AContractError(
            "V1A bootstrap requires exactly 10000 samples and seed 20260720"
        )
    start_ms, end_ms = V1A_SPLIT_RANGES_MS[split]
    if start_ms % _DAY_MS or end_ms % _DAY_MS or end_ms <= start_ms:
        raise IndicatorV1AContractError(
            "V1A bootstrap boundaries must be increasing UTC midnights"
        )
    day_count = (end_ms - start_ms) // _DAY_MS
    full_blocks, remainder = divmod(day_count, V1A_BOOTSTRAP_BLOCK_DAYS)
    block_lengths = (V1A_BOOTSTRAP_BLOCK_DAYS,) * full_blocks
    if remainder:
        block_lengths = (*block_lengths, remainder)
    header = {
        "block_lengths": list(block_lengths),
        "calendar_end_ms_exclusive": end_ms,
        "calendar_start_ms_inclusive": start_ms,
        "method": "circular_moving_utc_calendar_day_blocks",
        "samples": samples,
        "seed": seed,
        "split": split,
    }
    digest = hashlib.sha256(_canonical_json_bytes(header) + b"\n")
    rng = random.Random(seed)
    schedules: list[tuple[int, ...]] = []
    for _ in range(samples):
        starts = tuple(rng.randrange(day_count) for _ in block_lengths)
        schedules.append(starts)
        digest.update(",".join(str(value) for value in starts).encode("ascii"))
        digest.update(b"\n")
    return SharedBootstrapScheduleV1A(
        split=split,
        calendar_start_ms=start_ms,
        calendar_end_ms=end_ms,
        calendar_days=day_count,
        block_lengths=block_lengths,
        samples=samples,
        seed=seed,
        starts=tuple(schedules),
        schedule_sha256=digest.hexdigest(),
    )


def _circular_sums_v1a(values: Sequence[int], length: int) -> tuple[int, ...]:
    size = len(values)
    if size == 0 or not 1 <= length <= size:
        raise IndicatorV1AContractError("invalid circular block series")
    doubled = (*values, *values)
    prefix = [0]
    for value in doubled:
        prefix.append(prefix[-1] + value)
    return tuple(prefix[start + length] - prefix[start] for start in range(size))


def _resampled_totals_v1a(
    daily: Sequence[int],
    schedule: SharedBootstrapScheduleV1A,
) -> tuple[int, ...]:
    if len(daily) != schedule.calendar_days:
        raise IndicatorV1AContractError("daily series differs from bootstrap calendar")
    by_length = {
        length: _circular_sums_v1a(daily, length)
        for length in set(schedule.block_lengths)
    }
    totals: list[int] = []
    for starts in schedule.starts:
        totals.append(
            sum(
                by_length[length][start]
                for start, length in zip(
                    starts,
                    schedule.block_lengths,
                    strict=True,
                )
            )
        )
    return tuple(totals)


def _daily_metric_series_v1a(
    rows: Sequence[AnalyzableEventV1A | FeatureNotReadyV1A],
    *,
    horizon_bars: int,
    schedule: SharedBootstrapScheduleV1A,
) -> tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...]]:
    totals = [0] * schedule.calendar_days
    counts = [0] * schedule.calendar_days
    hits = [0] * schedule.calendar_days
    for row in rows:
        if row.split != schedule.split:
            raise IndicatorV1AContractError("bootstrap cell crosses frozen splits")
        offset = (row.decision_time_ms - schedule.calendar_start_ms) // _DAY_MS
        if not 0 <= offset < schedule.calendar_days:
            raise IndicatorV1AContractError(
                "bootstrap event lies outside the full frozen calendar"
            )
        value = _outcome_micros_v1a(row, horizon_bars)
        if value is None:
            continue
        totals[offset] += value
        counts[offset] += 1
        hits[offset] += int(value > 0)
    return tuple(totals), tuple(counts), tuple(hits)


def _type7_quantile_v1a(values: Sequence[float], probability: float) -> float:
    if not values:
        raise IndicatorV1AContractError("bootstrap quantile has no valid replicate")
    if not 0 <= probability <= 1:
        raise IndicatorV1AContractError("bootstrap quantile probability is invalid")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _bootstrap_endpoint_v1a(
    values: Sequence[float],
    *,
    point_estimate: float | None,
    samples: int,
    unit: str,
) -> dict[str, object]:
    if point_estimate is None or not values:
        interval: list[float | None] = [None, None]
        lower = None
    else:
        interval = [
            _type7_quantile_v1a(values, 0.025),
            _type7_quantile_v1a(values, 0.975),
        ]
        lower = 2 * point_estimate - _type7_quantile_v1a(values, 0.95)
    return {
        "unit": unit,
        "point_estimate": point_estimate,
        "two_sided_percentile_95_interval": interval,
        "one_sided_basic_95_lower": lower,
        "valid_replicates": len(values),
        "invalid_replicates": samples - len(values),
    }


def bootstrap_evaluation_cell_v1a(
    baseline_rows: Sequence[AnalyzableEventV1A | FeatureNotReadyV1A],
    selected_rows: Sequence[AnalyzableEventV1A],
    *,
    horizon_bars: int,
    schedule: SharedBootstrapScheduleV1A,
) -> dict[str, object]:
    """Bootstrap selected and paired-uplift mean/hit endpoints for one cell."""

    baseline_daily = _daily_metric_series_v1a(
        baseline_rows,
        horizon_bars=horizon_bars,
        schedule=schedule,
    )
    selected_daily = _daily_metric_series_v1a(
        selected_rows,
        horizon_bars=horizon_bars,
        schedule=schedule,
    )
    baseline_totals = tuple(
        _resampled_totals_v1a(values, schedule) for values in baseline_daily
    )
    selected_totals = tuple(
        _resampled_totals_v1a(values, schedule) for values in selected_daily
    )
    selected_means: list[float] = []
    selected_hits: list[float] = []
    mean_uplifts: list[float] = []
    hit_uplifts: list[float] = []
    for index in range(schedule.samples):
        baseline_sum = baseline_totals[0][index]
        baseline_count = baseline_totals[1][index]
        baseline_hit = baseline_totals[2][index]
        selected_sum = selected_totals[0][index]
        selected_count = selected_totals[1][index]
        selected_hit = selected_totals[2][index]
        if selected_count == 0:
            continue
        selected_mean = selected_sum / selected_count
        selected_rate = selected_hit / selected_count
        selected_means.append(selected_mean)
        selected_hits.append(selected_rate)
        if baseline_count == 0:
            continue
        mean_uplifts.append(selected_mean - baseline_sum / baseline_count)
        hit_uplifts.append(selected_rate - baseline_hit / baseline_count)

    baseline_point = summarize_outcomes_v1a(
        baseline_rows,
        horizon_bars=horizon_bars,
    )
    selected_point = summarize_outcomes_v1a(
        selected_rows,
        horizon_bars=horizon_bars,
    )
    return {
        "method": "shared_circular_utc_calendar_moving_block_bootstrap",
        "block_days": V1A_BOOTSTRAP_BLOCK_DAYS,
        "samples": schedule.samples,
        "seed": schedule.seed,
        "full_frozen_calendar": True,
        "zero_alert_days_retained": True,
        "schedule_sha256": schedule.schedule_sha256,
        "endpoints": {
            "selected_mean_net_return_micros": _bootstrap_endpoint_v1a(
                selected_means,
                point_estimate=selected_point.mean_net_return_micros,
                samples=schedule.samples,
                unit="return_micros",
            ),
            "selected_strict_hit_rate": _bootstrap_endpoint_v1a(
                selected_hits,
                point_estimate=selected_point.strict_hit_rate,
                samples=schedule.samples,
                unit="fraction",
            ),
            "selected_minus_baseline_mean_net_return_micros": (
                _bootstrap_endpoint_v1a(
                    mean_uplifts,
                    point_estimate=_point_difference_v1a(
                        selected_point,
                        baseline_point,
                        metric="mean",
                    ),
                    samples=schedule.samples,
                    unit="return_micros",
                )
            ),
            "selected_minus_baseline_strict_hit_rate": _bootstrap_endpoint_v1a(
                hit_uplifts,
                point_estimate=_point_difference_v1a(
                    selected_point,
                    baseline_point,
                    metric="hit",
                ),
                samples=schedule.samples,
                unit="fraction",
            ),
        },
    }


def build_bootstrapped_evaluations_v1a(
    loaded: LoadedIndicatorV1A,
    scored: Sequence[ScoredEventV1A],
) -> tuple[list[dict[str, object]], tuple[SharedBootstrapScheduleV1A, ...]]:
    """Attach one split-shared frozen schedule to all 150 evaluation cells."""

    point_rows = build_point_evaluations_v1a(loaded, scored)
    selected_ids = {row.event.event_id for row in scored if row.selected}
    schedules = tuple(
        build_shared_bootstrap_schedule_v1a(split) for split in V1A_SPLITS
    )
    schedule_by_split = {schedule.split: schedule for schedule in schedules}
    enriched: list[dict[str, object]] = []
    for point in point_rows:
        split = cast(str, point["split"])
        horizon = cast(int, point["horizon_bars"])
        dimension = cast(str, point["dimension"])
        value = cast(str, point["value"])
        baseline = tuple(
            row
            for row in (*loaded.events, *loaded.feature_not_ready)
            if row.split == split and _matches_dimension_v1a(row, dimension, value)
        )
        selected = tuple(
            row
            for row in loaded.events
            if row.split == split
            and row.event_id in selected_ids
            and _matches_dimension_v1a(row, dimension, value)
        )
        output = dict(point)
        output["bootstrap"] = bootstrap_evaluation_cell_v1a(
            baseline,
            selected,
            horizon_bars=horizon,
            schedule=schedule_by_split[split],
        )
        enriched.append(output)
    return enriched, schedules


def _find_evaluation_v1a(
    evaluations: Sequence[Mapping[str, object]],
    *,
    split: str,
    horizon_bars: int,
    dimension: str,
    value: str,
) -> Mapping[str, object]:
    matching = tuple(
        row
        for row in evaluations
        if row.get("split") == split
        and row.get("horizon_bars") == horizon_bars
        and row.get("dimension") == dimension
        and row.get("value") == value
    )
    if len(matching) != 1:
        raise IndicatorV1AContractError("evaluation cell is missing or duplicated")
    return matching[0]


def _optional_finite_number_v1a(value: object, label: str) -> float | None:
    if value is None:
        return None
    if type(value) not in {int, float}:
        raise IndicatorV1AContractError(f"{label} must be numeric or null")
    output = float(cast(int | float, value))
    if not math.isfinite(output):
        raise IndicatorV1AContractError(f"{label} must be finite")
    return output


def build_historical_validation_gate_v1a(
    loaded: LoadedIndicatorV1A,
    scored: Sequence[ScoredEventV1A],
    evaluations: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    """Bind the exact point/uncertainty evidence to the validation-h12 gate."""

    split = "validation"
    horizon = V1A_PRIMARY_HORIZON_BARS
    selected_ids = {row.event.event_id for row in scored if row.selected}
    complete = tuple(row for row in loaded.events if row.split == split)
    not_ready = tuple(row for row in loaded.feature_not_ready if row.split == split)
    baseline: tuple[AnalyzableEventV1A | FeatureNotReadyV1A, ...] = (
        *complete,
        *not_ready,
    )
    selected = tuple(row for row in complete if row.event_id in selected_ids)
    baseline_metrics = summarize_outcomes_v1a(baseline, horizon_bars=horizon)
    selected_metrics = summarize_outcomes_v1a(selected, horizon_bars=horizon)
    by_direction = {
        direction: summarize_outcomes_v1a(
            tuple(row for row in selected if row.direction == direction),
            horizon_bars=horizon,
        )
        for direction in ("long", "short")
    }
    positive_asset_uplifts = 0
    for asset in V1A_ASSETS:
        asset_baseline = summarize_outcomes_v1a(
            tuple(row for row in baseline if row.asset == asset),
            horizon_bars=horizon,
        )
        asset_selected = summarize_outcomes_v1a(
            tuple(row for row in selected if row.asset == asset),
            horizon_bars=horizon,
        )
        positive_asset_uplifts += int(
            _ratio_difference_positive_v1a(
                asset_selected.sum_net_return_micros,
                asset_selected.evaluable_events,
                asset_baseline.sum_net_return_micros,
                asset_baseline.evaluable_events,
            )
        )
    overall = _find_evaluation_v1a(
        evaluations,
        split=split,
        horizon_bars=horizon,
        dimension="overall",
        value="all",
    )
    bootstrap = _require_dict(overall.get("bootstrap"), "validation bootstrap")
    endpoints = _require_dict(
        bootstrap.get("endpoints"),
        "validation bootstrap endpoints",
    )
    selected_mean_endpoint = _require_dict(
        endpoints.get("selected_mean_net_return_micros"),
        "validation selected mean endpoint",
    )
    uplift_endpoint = _require_dict(
        endpoints.get("selected_minus_baseline_mean_net_return_micros"),
        "validation uplift mean endpoint",
    )
    evidence = ValidationGateEvidenceV1A(
        selected_sum_micros=selected_metrics.sum_net_return_micros,
        selected_evaluable=selected_metrics.evaluable_events,
        selected_mean_lower_micros=_optional_finite_number_v1a(
            selected_mean_endpoint.get("one_sided_basic_95_lower"),
            "validation selected mean lower",
        ),
        baseline_sum_micros=baseline_metrics.sum_net_return_micros,
        baseline_evaluable=baseline_metrics.evaluable_events,
        uplift_mean_lower_micros=_optional_finite_number_v1a(
            uplift_endpoint.get("one_sided_basic_95_lower"),
            "validation uplift mean lower",
        ),
        selected_gross_profit_micros=selected_metrics.gross_profit_micros,
        selected_gross_loss_abs_micros=selected_metrics.gross_loss_abs_micros,
        selected_median_micros=selected_metrics.median_net_return_micros,
        long_selected_sum_micros=by_direction["long"].sum_net_return_micros,
        long_selected_evaluable=by_direction["long"].evaluable_events,
        short_selected_sum_micros=by_direction["short"].sum_net_return_micros,
        short_selected_evaluable=by_direction["short"].evaluable_events,
        selected_events=len(selected),
        intended_population_events=len(baseline),
        complete_case_events=len(complete),
        positive_asset_uplifts=positive_asset_uplifts,
        asset_count=len(V1A_ASSETS),
        selected_mean_valid_replicates=_require_int(
            selected_mean_endpoint.get("valid_replicates"),
            "validation selected mean valid_replicates",
        ),
        uplift_mean_valid_replicates=_require_int(
            uplift_endpoint.get("valid_replicates"),
            "validation uplift mean valid_replicates",
        ),
        bootstrap_samples=_require_int(
            bootstrap.get("samples"),
            "validation bootstrap samples",
        ),
    )
    decision = evaluate_validation_gate_v1a(evidence)
    decision["evidence"] = {
        "selected": selected_metrics.artifact(),
        "baseline": baseline_metrics.artifact(),
        "selected_mean_one_sided_basic_95_lower_micros": (
            evidence.selected_mean_lower_micros
        ),
        "selected_minus_baseline_mean_one_sided_basic_95_lower_micros": (
            evidence.uplift_mean_lower_micros
        ),
        "long_selected": by_direction["long"].artifact(),
        "short_selected": by_direction["short"].artifact(),
        "selected_events": evidence.selected_events,
        "intended_population_events": evidence.intended_population_events,
        "retention": (
            evidence.selected_events / evidence.intended_population_events
            if evidence.intended_population_events
            else None
        ),
        "complete_case_events": evidence.complete_case_events,
        "complete_case_coverage": (
            evidence.complete_case_events / evidence.intended_population_events
            if evidence.intended_population_events
            else None
        ),
        "positive_asset_uplifts": evidence.positive_asset_uplifts,
        "asset_count": evidence.asset_count,
        "selected_mean_valid_replicates": (
            evidence.selected_mean_valid_replicates
        ),
        "uplift_mean_valid_replicates": evidence.uplift_mean_valid_replicates,
        "bootstrap_samples": evidence.bootstrap_samples,
    }
    return decision


def score_quartile_v1a(
    score: float,
    *,
    q25: float,
    q50: float,
    q75: float,
) -> Literal["Q1", "Q2", "Q3", "Q4"]:
    """Assign one score to the predeclared left-closed development quartiles."""

    values = (score, q25, q50, q75)
    if any(not math.isfinite(value) for value in values):
        raise IndicatorV1AContractError("score quartile inputs must be finite")
    if not 0 <= q25 <= q50 <= q75 <= 100 or not 0 <= score <= 100:
        raise IndicatorV1AContractError("score quartile boundaries are invalid")
    if score < q25:
        return "Q1"
    if score < q50:
        return "Q2"
    if score < q75:
        return "Q3"
    return "Q4"


def build_descriptive_score_gradient_v1a(
    model: PercentileCompositeModel,
    scored: Sequence[ScoredEventV1A],
) -> dict[str, object]:
    """Describe after-cost outcomes by frozen score quartile without inference."""

    development_scores = tuple(
        row.composite_score for row in scored if row.event.split == "development"
    )
    if not development_scores:
        raise IndicatorV1AContractError(
            "score gradient requires complete-case development scores"
        )
    q25 = _type7_quantile_v1a(development_scores, 0.25)
    q50 = _type7_quantile_v1a(development_scores, 0.50)
    q75 = _type7_quantile_v1a(development_scores, 0.75)
    if q75 != model.top_quartile_cutoff:
        raise IndicatorV1AContractError(
            "score gradient q75 differs from frozen selection cutoff"
        )
    quartiles = ("Q1", "Q2", "Q3", "Q4")
    rows: list[dict[str, object]] = []
    for split in V1A_SPLITS:
        for horizon in V1A_HORIZONS_BARS:
            for quartile in quartiles:
                events = tuple(
                    row.event
                    for row in scored
                    if row.event.split == split
                    and score_quartile_v1a(
                        row.composite_score,
                        q25=q25,
                        q50=q50,
                        q75=q75,
                    )
                    == quartile
                )
                metrics = summarize_outcomes_v1a(
                    events,
                    horizon_bars=horizon,
                )
                rows.append(
                    {
                        "split": split,
                        "horizon_bars": horizon,
                        "horizon_minutes": horizon * 5,
                        "quartile": quartile,
                        "after_cost_metrics": metrics.artifact(),
                        "empty_bin": len(events) == 0,
                        "label": "DESCRIPTIVE_ONLY",
                        "score_is_probability": False,
                        "inferential_claim_allowed": False,
                        "promotion_allowed": False,
                    }
                )
    return {
        "label": "DESCRIPTIVE_ONLY",
        "purpose": (
            "check whether stronger multi-indicator scores have a monotone "
            "after-cost outcome gradient"
        ),
        "fit_split": "development",
        "cutoff_fit_uses_outcomes": False,
        "development_type7_cutoffs": {"q25": q25, "q50": q50, "q75": q75},
        "q75_equals_frozen_selection_cutoff": True,
        "bin_contract": {
            "Q1": "score < q25",
            "Q2": "q25 <= score < q50",
            "Q3": "q50 <= score < q75",
            "Q4": "score >= q75",
        },
        "feature_not_ready_policy": (
            "excluded_from score bins; retained separately in baseline/retention"
        ),
        "score_is_probability": False,
        "included_in_confirmatory_gate": False,
        "inferential_bootstrap_applied": False,
        "promotion_allowed": False,
        "rows": rows,
    }


def _score_audit_sha256_v1a(scored: Sequence[ScoredEventV1A]) -> str:
    artifact = [
        {
            "axis_scores_hex": [value.hex() for value in row.axis_scores],
            "composite_score_hex": row.composite_score.hex(),
            "event_id": row.event.event_id,
            "selected": row.selected,
            "split": row.event.split,
        }
        for row in scored
    ]
    return sha256_bytes(_canonical_json_bytes(artifact))


def _execution_cost_authority_v1a(
    loaded: LoadedIndicatorV1A,
) -> tuple[str, dict[str, Any]]:
    contracts: dict[str, dict[str, Any]] = {}
    for audit in loaded.audits:
        path = audit.replay_dir / "results.json"
        raw = path.read_bytes() if path.is_file() else b""
        if sha256_bytes(raw) != audit.results_sha256:
            raise IndicatorV1AContractError(
                "authenticated replay results changed during V1A analysis"
            )
        document = _json_from_bytes(raw, "authenticated results.json")
        evaluation = _require_dict(
            document.get("evaluation_contract"),
            "authenticated evaluation_contract",
        )
        costs = _require_dict(
            evaluation.get("costs"),
            "authenticated execution costs",
        )
        canonical = _canonical_json_bytes(costs)
        contracts[sha256_bytes(canonical)] = costs
    if len(contracts) != 1:
        raise IndicatorV1AContractError(
            "V1A execution cost authority differs across replays"
        )
    return next(iter(contracts.items()))


def _model_artifact_v1a(model: PercentileCompositeModel) -> dict[str, Any]:
    artifact = model.artifact()
    artifact.update(
        {
            "protocol": "indicator_discriminator_v1a_seven_asset_after_cost",
            "fit_split": "development",
            "fit_population": "STRICT_8_FEATURE_COMPLETE_CASE",
            "missing_value_policy": (
                "strict eight-feature complete-case; any missing feature is "
                "FEATURE_NOT_READY no-call"
            ),
            "v1a_missing_value_policy": "FEATURE_NOT_READY_NO_CALL",
            "validation_or_retrospective_refit": False,
            "outcomes_used_for_fit_or_cutoff": False,
            "selection_rule": "composite_score >= development_type7_p75",
            "score_is_probability": False,
        }
    )
    return artifact


def _audit_artifact_v1a(audit: ReplayInputAuditV1A) -> dict[str, object]:
    return {
        "split": audit.split,
        "replay_dir": str(audit.replay_dir),
        "run_manifest_sha256": audit.run_manifest_sha256,
        "recommendations_sha256": audit.recommendations_sha256,
        "outcomes_sha256": audit.outcomes_sha256,
        "results_sha256": audit.results_sha256,
        "report_sha256": audit.report_sha256,
        "recommendation_rows": audit.recommendation_rows,
        "outcome_rows": audit.outcome_rows,
        "intended_population_rows": audit.intended_population_rows,
        "intended_population_sha256": audit.intended_population_sha256,
        "deduplicated_population_rows": audit.deduplicated_population_rows,
        "retained_population_sha256": audit.retained_population_sha256,
        "duplicate_rows_dropped": audit.duplicate_rows_dropped,
        "dropped_population_sha256": audit.dropped_population_sha256,
        "complete_case_rows": audit.complete_case_rows,
        "feature_not_ready_rows": audit.feature_not_ready_rows,
        "feature_not_ready_sha256": audit.feature_not_ready_sha256,
    }


def _json_text_v1a(value: object) -> str:
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            + "\n"
        )
    except (TypeError, ValueError) as exc:
        raise IndicatorV1AContractError(
            "V1A result is not deterministic JSON-safe"
        ) from exc


def _resolved_fresh_analysis_target_v1a(output_dir: str | Path) -> Path:
    target = Path(output_dir).resolve()
    if os.path.lexists(target):
        raise IndicatorV1AContractError(
            "V1A analysis output target must not already exist"
        )
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise IndicatorV1AContractError(
            "V1A analysis output parent cannot be created"
        ) from exc
    if os.path.lexists(target):
        raise IndicatorV1AContractError(
            "V1A analysis output target appeared during fresh-target validation"
        )
    return target


def _write_fsynced_bytes_v1a(path: Path, payload: bytes) -> None:
    if type(payload) is not bytes:
        raise TypeError("analysis payload must be exact bytes")
    try:
        payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IndicatorV1AContractError(
            f"analysis payload is not UTF-8: {path.name}"
        ) from exc
    if b"\r" in payload:
        raise IndicatorV1AContractError(
            f"analysis payload must use LF line endings: {path.name}"
        )
    try:
        with path.open("xb") as handle:
            written = handle.write(payload)
            if written != len(payload):
                raise OSError("short analysis artifact write")
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except OSError as exc:
                unsupported = {
                    errno.EINVAL,
                    getattr(errno, "ENOTSUP", errno.EINVAL),
                    getattr(errno, "EOPNOTSUPP", errno.EINVAL),
                }
                if exc.errno not in unsupported:
                    raise
    except OSError as exc:
        raise IndicatorV1AContractError(
            f"analysis artifact write failed: {path.name}"
        ) from exc


def _remove_owned_analysis_directory_v1a(path: Path, *, parent: Path) -> None:
    if path.parent.resolve() != parent.resolve():
        raise IndicatorV1AContractError(
            "refusing to clean an analysis directory outside its intended parent"
        )
    if not os.path.lexists(path):
        return
    try:
        if path.is_symlink():
            path.unlink()
        else:
            shutil.rmtree(path)
    except OSError as exc:
        raise IndicatorV1AContractError(
            f"failed to clean incomplete analysis directory: {path}"
        ) from exc


def _analysis_manifest_v1a(
    *,
    freeze_manifest_sha256: str,
    input_authority: dict[str, Any],
    payload_sha256: Mapping[str, str],
) -> dict[str, object]:
    if set(payload_sha256) != set(_ANALYSIS_PAYLOAD_NAMES):
        raise IndicatorV1AContractError(
            "analysis manifest requires the exact three payload hashes"
        )
    completed_at_utc = datetime.now(UTC).isoformat()
    _parse_canonical_utc(completed_at_utc, "analysis completed_at_utc")
    return {
        "schema_version": 1,
        "protocol": _ANALYSIS_PROTOCOL,
        "completed_at_utc": completed_at_utc,
        "freeze_manifest_sha256": _require_sha256(
            freeze_manifest_sha256,
            "analysis freeze_manifest_sha256",
        ),
        "input_authority_sha256": _semantic_sha256(input_authority),
        "outputs": {
            name: _require_sha256(
                payload_sha256[name], f"analysis output hash {name}"
            )
            for name in _ANALYSIS_PAYLOAD_NAMES
        },
        "historical_only": True,
        "external_anchor": False,
        "deployment_approved": False,
        "probability_calibrated": False,
    }


def _verify_analysis_directory_v1a(
    root: Path,
    *,
    expected_manifest: Mapping[str, object],
    expected_payload_sha256: Mapping[str, str],
) -> None:
    try:
        entries = tuple(root.iterdir())
    except OSError as exc:
        raise IndicatorV1AContractError(
            "published analysis directory cannot be inspected"
        ) from exc
    if (
        {entry.name for entry in entries} != _ANALYSIS_OUTPUT_NAMES
        or any(not entry.is_file() or entry.is_symlink() for entry in entries)
    ):
        raise IndicatorV1AContractError(
            "analysis output file set is not the exact four-file contract"
        )
    for name in _ANALYSIS_PAYLOAD_NAMES:
        path = root / name
        actual = _hash_file(path)
        if actual != expected_payload_sha256[name]:
            raise IndicatorV1AContractError(
                f"published analysis payload hash mismatch: {name}"
            )
        raw = path.read_bytes()
        try:
            raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise IndicatorV1AContractError(
                f"published analysis payload is not UTF-8: {name}"
            ) from exc
        if b"\r" in raw:
            raise IndicatorV1AContractError(
                f"published analysis payload is not LF-only: {name}"
            )
    manifest_raw = (root / _ANALYSIS_MANIFEST_NAME).read_bytes()
    if b"\r" in manifest_raw:
        raise IndicatorV1AContractError("analysis manifest is not LF-only")
    manifest = _json_from_bytes(manifest_raw, _ANALYSIS_MANIFEST_NAME)
    if manifest != expected_manifest:
        raise IndicatorV1AContractError(
            "published analysis manifest differs from the authenticated manifest"
        )
    if set(manifest) != {
        "schema_version",
        "protocol",
        "completed_at_utc",
        "freeze_manifest_sha256",
        "input_authority_sha256",
        "outputs",
        "historical_only",
        "external_anchor",
        "deployment_approved",
        "probability_calibrated",
    }:
        raise IndicatorV1AContractError("analysis manifest fields are not exact")
    _parse_canonical_utc(
        manifest.get("completed_at_utc"),
        "analysis manifest completed_at_utc",
    )
    outputs = _canonical_mapping(
        manifest.get("outputs"),
        label="analysis manifest outputs",
        expected_count=len(_ANALYSIS_PAYLOAD_NAMES),
    )
    if (
        {name for name, _ in outputs} != set(_ANALYSIS_PAYLOAD_NAMES)
        or _ANALYSIS_MANIFEST_NAME in dict(outputs)
        or dict(outputs) != dict(expected_payload_sha256)
    ):
        raise IndicatorV1AContractError(
            "analysis manifest output hashes are not exact"
        )


def _publish_analysis_artifacts_v1a(
    *,
    target: Path,
    payloads: Mapping[str, bytes],
    freeze_manifest_sha256: str,
    input_authority: dict[str, Any],
) -> dict[str, object]:
    if target != target.resolve() or os.path.lexists(target):
        raise IndicatorV1AContractError(
            "V1A analysis publication requires a fresh resolved target"
        )
    if set(payloads) != set(_ANALYSIS_PAYLOAD_NAMES):
        raise IndicatorV1AContractError(
            "V1A analysis publication requires exactly three payloads"
        )
    payload_sha256 = {
        name: sha256_bytes(payloads[name]) for name in _ANALYSIS_PAYLOAD_NAMES
    }
    manifest = _analysis_manifest_v1a(
        freeze_manifest_sha256=freeze_manifest_sha256,
        input_authority=input_authority,
        payload_sha256=payload_sha256,
    )
    manifest_raw = _json_text_v1a(manifest).encode("utf-8")
    prefix = f".{target.name}.tmp-"
    try:
        temporary = Path(tempfile.mkdtemp(prefix=prefix, dir=target.parent)).resolve()
    except OSError as exc:
        raise IndicatorV1AContractError(
            "failed to create same-filesystem analysis temporary directory"
        ) from exc
    if temporary.parent != target.parent or not temporary.name.startswith(prefix):
        _remove_owned_analysis_directory_v1a(temporary, parent=target.parent)
        raise IndicatorV1AContractError(
            "analysis temporary directory escaped its intended sibling location"
        )
    published = False
    try:
        for name in _ANALYSIS_PAYLOAD_NAMES:
            _write_fsynced_bytes_v1a(temporary / name, payloads[name])
        _write_fsynced_bytes_v1a(
            temporary / _ANALYSIS_MANIFEST_NAME,
            manifest_raw,
        )
        _verify_analysis_directory_v1a(
            temporary,
            expected_manifest=manifest,
            expected_payload_sha256=payload_sha256,
        )
        if os.path.lexists(target):
            raise IndicatorV1AContractError(
                "V1A analysis output target appeared before atomic publication"
            )
        os.rename(temporary, target)
        published = True
        _verify_analysis_directory_v1a(
            target,
            expected_manifest=manifest,
            expected_payload_sha256=payload_sha256,
        )
    except Exception as exc:
        incomplete = target if published else temporary
        _remove_owned_analysis_directory_v1a(incomplete, parent=target.parent)
        if isinstance(exc, IndicatorV1AContractError):
            raise
        raise IndicatorV1AContractError(
            "atomic V1A analysis publication failed"
        ) from exc
    return manifest


def _format_micros_v1a(value: object) -> str:
    if value is None:
        return "n/a"
    if type(value) not in {int, float}:
        raise IndicatorV1AContractError("report return metric is not numeric")
    return f"{float(cast(int | float, value)):.2f} micros"


def render_indicator_v1a_report_ko(results: Mapping[str, object]) -> str:
    """Render the intentionally minimal, non-promoting Korean readout."""

    gate = _require_dict(results.get("historical_validation_gate"), "report gate")
    evidence = _require_dict(gate.get("evidence"), "report gate evidence")
    selected = _require_dict(evidence.get("selected"), "report selected evidence")
    baseline = _require_dict(evidence.get("baseline"), "report baseline evidence")
    criteria = _require_dict(gate.get("criteria"), "report gate criteria")
    passed = gate.get("overall_pass") is True
    lines = [
        "# Indicator Discriminator V1A 역사적 비용 후 분석",
        "",
        f"- Validation 60분 교집합 게이트: **{'PASS' if passed else 'FAIL'}**",
        (
            "- 선택 평균 순수익: "
            + _format_micros_v1a(selected.get("mean_net_return_micros"))
        ),
        (
            "- 기준선 평균 순수익: "
            + _format_micros_v1a(baseline.get("mean_net_return_micros"))
        ),
        (
            "- 선택 평균의 단측 basic 95% 하한: "
            + _format_micros_v1a(
                evidence.get("selected_mean_one_sided_basic_95_lower_micros")
            )
        ),
        (
            "- 선택-기준선 평균 차이의 단측 basic 95% 하한: "
            + _format_micros_v1a(
                evidence.get(
                    "selected_minus_baseline_mean_one_sided_basic_95_lower_micros"
                )
            )
        ),
        f"- 선택/전체 보유율: {evidence.get('selected_events')}/"
        f"{evidence.get('intended_population_events')}",
        f"- 자산별 양(+)의 평균 uplift: {evidence.get('positive_asset_uplifts')}/7",
        "- 점수 Q1~Q4 성과 기울기는 별도 기술 통계이며 확률·게이트가 아니다.",
        "",
        "## 동결 게이트",
        "",
    ]
    for name, value in criteria.items():
        lines.append(f"- `{name}`: {value}")
    lines.extend(
        [
            "",
            "## 해석 제한",
            "",
            "- 모든 수익률은 replay가 기록한 수수료·슬리피지·펀딩 반영 후 net return이다.",
            "- FEATURE_NOT_READY는 기준선과 보유율 분모에 남고 선택 신호가 되지 않는다.",
            "- outcome 검사는 인증 행 내부 정합성 감사이며 원시 캔들·펀딩 독립 재계산이 아니다.",
            "- 7일 블록은 각 split의 전체 UTC 달력을 사용하며 무알림 날짜도 포함한다.",
            "- PASS여도 노출된 역사적 진단일 뿐 독립 검증·확률 보정·배포 승인이 아니다.",
            "- 실주문 기능은 포함하지 않는다.",
            "",
        ]
    )
    return "\n".join(lines)


def run_indicator_v1a_analysis(
    *,
    freeze_manifest_path: str | Path,
    replay_dirs: Sequence[str | Path],
    output_dir: str | Path,
    workspace_root: str | Path,
    samples: int = V1A_BOOTSTRAP_SAMPLES,
    seed: int = V1A_BOOTSTRAP_SEED,
) -> dict[str, object]:
    """Run the frozen seven-asset historical analysis and write its artifacts."""

    if samples != V1A_BOOTSTRAP_SAMPLES or seed != V1A_BOOTSTRAP_SEED:
        raise IndicatorV1AContractError(
            "V1A analysis requires exactly 10000 samples and seed 20260720"
        )
    output = _resolved_fresh_analysis_target_v1a(output_dir)
    loaded = load_indicator_v1a_inputs(
        freeze_manifest_path=freeze_manifest_path,
        replay_dirs=replay_dirs,
        workspace_root=workspace_root,
    )
    model, scored = fit_and_score_indicator_v1a(loaded)
    evaluations, schedules = build_bootstrapped_evaluations_v1a(loaded, scored)
    gate = build_historical_validation_gate_v1a(loaded, scored, evaluations)
    score_gradient = build_descriptive_score_gradient_v1a(model, scored)
    cost_sha256, cost_contract = _execution_cost_authority_v1a(loaded)
    model_artifact = _model_artifact_v1a(model)
    model_raw = _json_text_v1a(model_artifact).encode()
    results: dict[str, object] = {
        "schema_version": 1,
        "protocol": _ANALYSIS_PROTOCOL,
        "exposure_status": V1A_EXPOSURE_STATUS,
        "assets_in_frozen_order": list(V1A_ASSETS),
        "splits_in_chronological_order": list(V1A_SPLITS),
        "horizons_bars": list(V1A_HORIZONS_BARS),
        "primary_endpoint": {
            "split": "validation",
            "horizon_bars": V1A_PRIMARY_HORIZON_BARS,
            "dimension": "overall",
            "metric": "selected mean after-cost net return micros",
            "confirmatory_endpoints": 1,
        },
        "multiplicity": {
            "secondary_horizons_and_subgroups": "DESCRIPTIVE_ONLY",
            "direction_and_asset_requirements": "INTERSECTION_GATE_NOT_DISCOVERIES",
            "secondary_inferential_claims_allowed": False,
        },
        "score_authority": {
            "fitted_score_sha256": sha256_bytes(model_raw),
            "scored_event_selection_sha256": _score_audit_sha256_v1a(scored),
            "complete_case_scored_events": len(scored),
            "selected_events": sum(row.selected for row in scored),
            "fit_uses_outcomes": False,
        },
        "population": {
            "complete_case_events": len(loaded.events),
            "feature_not_ready_events": len(loaded.feature_not_ready),
            "feature_not_ready_retained_in_baseline_and_retention_denominator": True,
            "feature_not_ready_by_split_sha256": {
                audit.split: audit.feature_not_ready_sha256 for audit in loaded.audits
            },
        },
        "execution_cost_contract": cost_contract,
        "execution_cost_contract_sha256": cost_sha256,
        "outcome_integrity": {
            "rows_joined_to_authenticated_recommendations": sum(
                audit.outcome_rows for audit in loaded.audits
            ),
            "all_recommendations_joined_before_population_filtering": True,
            "decision_and_horizon_clock_reconciled": True,
            "split_start_embargo_bars": _SPLIT_START_EMBARGO_BARS,
            "execution_returns_recomputed_with_frozen_costs": True,
            "path_extrema_and_one_r_internal_consistency_checked": True,
            "hit_status_margins_bps": [0, 5, 10, 25],
            "float_relative_tolerance": _OUTCOME_FLOAT_REL_TOLERANCE,
            "float_absolute_tolerance": _OUTCOME_FLOAT_ABS_TOLERANCE,
            "raw_candles_reparsed": False,
            "raw_funding_reparsed": False,
            "independent_outcome_recomputation_claimed": False,
        },
        "bootstrap_schedules": [schedule.artifact() for schedule in schedules],
        "evaluations": evaluations,
        "descriptive_score_gradient": score_gradient,
        "historical_validation_gate": gate,
        "input_authority": {
            "freeze_manifest_path": str(loaded.authority.manifest_path),
            "freeze_manifest_sha256": loaded.authority.manifest_sha256,
            "freeze_created_at_utc": loaded.authority.created_at_utc,
            "spec_sha256": loaded.authority.spec_sha256,
            "spec_semantics_sha256": loaded.authority.spec_semantics_sha256,
            "config_sha256": loaded.authority.config_sha256,
            "settings_semantics_sha256": (
                loaded.authority.settings_semantics_sha256
            ),
            "source_code_sha256": loaded.authority.source_code_sha256,
            "data_input_sha256": dict(loaded.authority.data_input_sha256),
            "replays": [_audit_artifact_v1a(audit) for audit in loaded.audits],
        },
        "status": {
            "historical_candidate_gate_pass": gate["overall_pass"],
            "historical_only": True,
            "independently_validated": False,
            "prospective_paper_bbo_validated": False,
            "probability_calibrated": False,
            "deployment_approved": False,
            "production_order_execution": False,
        },
    }
    results_text = _json_text_v1a(results)
    report_text = render_indicator_v1a_report_ko(results)
    input_authority = _require_dict(
        results.get("input_authority"),
        "analysis results input_authority",
    )
    _publish_analysis_artifacts_v1a(
        target=output,
        payloads={
            "fitted_score.json": model_raw,
            "results.json": results_text.encode("utf-8"),
            "report_ko.md": report_text.encode("utf-8"),
        },
        freeze_manifest_sha256=loaded.authority.manifest_sha256,
        input_authority=input_authority,
    )
    return results


def _parser_v1a() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the frozen seven-asset V1A after-cost historical analysis."
    )
    parser.add_argument("--freeze-manifest", type=Path, required=True)
    parser.add_argument(
        "--replay-dir",
        type=Path,
        action="append",
        required=True,
        help="Repeat exactly three times, once for each frozen split.",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--workspace-root", type=Path, default=Path.cwd())
    parser.add_argument("--samples", type=int, default=V1A_BOOTSTRAP_SAMPLES)
    parser.add_argument("--seed", type=int, default=V1A_BOOTSTRAP_SEED)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point with no unfrozen or partial-input escape hatch."""

    parser = _parser_v1a()
    args = parser.parse_args(argv)
    if len(args.replay_dir) != len(V1A_SPLITS):
        parser.error("--replay-dir must be supplied exactly three times")
    try:
        run_indicator_v1a_analysis(
            freeze_manifest_path=args.freeze_manifest,
            replay_dirs=tuple(args.replay_dir),
            output_dir=args.output_dir,
            workspace_root=args.workspace_root,
            samples=args.samples,
            seed=args.seed,
        )
    except IndicatorV1AContractError as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
