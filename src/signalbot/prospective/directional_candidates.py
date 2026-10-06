"""Contracts for researching and promoting Futures directional candidates.

This module is deliberately evidence-blind. It validates a preregistered
candidate, fingerprints a replay fixture, and adjudicates supplied receipts,
but it cannot manufacture a historical or prospective result.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import Field, field_validator, model_validator

from signalbot.config import StrictModel
from signalbot.domain.enums import Direction, Market, SignalFamily

PROTOCOL_VERSION = "futures_bidirectional_candidate_v1"


class PromotionVerdict(StrEnum):
    WAITING_FOR_AUTHORITY = "WAITING_FOR_AUTHORITY"
    NO_QUALIFIED_CANDIDATE = "NO_QUALIFIED_CANDIDATE"
    CONTINUE_OBSERVING = "CONTINUE_OBSERVING"
    PROMOTE = "PROMOTE"
    REJECT = "REJECT"


class HistoricalScreenStatus(StrEnum):
    WAITING_FOR_AUTHORITY = "WAITING_FOR_AUTHORITY"
    PASS = "PASS"
    NO_QUALIFIED_CANDIDATE = "NO_QUALIFIED_CANDIDATE"


class ProspectiveStatus(StrEnum):
    NOT_STARTED = "NOT_STARTED"
    ACTIVE = "ACTIVE"
    COMPLETE = "COMPLETE"


class DirectionalSplit(StrictModel):
    name: str
    start: str
    end: str

    @field_validator("start", "end")
    @classmethod
    def require_utc_timestamp(cls, value: str) -> str:
        if not value.endswith("Z"):
            raise ValueError("split timestamps must be UTC and end with Z")
        return value

    @model_validator(mode="after")
    def validate_order(self) -> DirectionalSplit:
        if self.start >= self.end:
            raise ValueError("split start must be before split end")
        return self


class DirectionalUniverseAsset(StrictModel):
    asset: str
    symbol: str
    cohort: Literal["anchor", "major", "volatile"]

    @field_validator("asset", "symbol")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        return value.upper()


class DirectionalMinimumEvidence(StrictModel):
    min_samples_per_direction: int = Field(ge=1)
    min_symbols_per_direction: int = Field(ge=1)
    min_regimes_per_direction: int = Field(ge=1)
    max_censor_fraction: float = Field(gt=0, lt=1)
    require_independent_review: bool = True


class DirectionalPreregistration(StrictModel):
    protocol_version: Literal["futures_bidirectional_candidate_v1"] = PROTOCOL_VERSION
    candidate_version: str
    rule_version: str
    market: Literal[Market.FUTURES] = Market.FUTURES
    interval: Literal["5m"] = "5m"
    long_family: Literal[SignalFamily.BREAKOUT_LONG] = SignalFamily.BREAKOUT_LONG
    short_family: Literal[SignalFamily.BREAKDOWN_SHORT] = SignalFamily.BREAKDOWN_SHORT
    higher_timeframes: tuple[Literal["15m", "1h"], Literal["15m", "1h"]] = (
        "15m",
        "1h",
    )
    require_fully_closed_candles: bool = True
    require_strict_prior_higher_timeframes: bool = True
    require_observed_bbo_for_live_gate: bool = True
    historical_bbo_status: Literal["UNAVAILABLE", "AVAILABLE"] = "UNAVAILABLE"
    entry_timing: Literal["NEXT_BAR_OPEN"] = "NEXT_BAR_OPEN"
    outcome_horizons_bars: tuple[Literal[1, 3, 6, 12], ...] = (1, 3, 6, 12)
    technical_exit_max_holding_bars: int = Field(ge=1, le=10_000)
    neutral_band_bps: float = Field(ge=0)
    research_round_trip_cost_bps: float = Field(gt=0)
    universe: tuple[DirectionalUniverseAsset, ...] = Field(min_length=1)
    splits: tuple[DirectionalSplit, ...] = Field(min_length=2)
    minimum_evidence: DirectionalMinimumEvidence
    authority_status: Literal["WAITING_FOR_AUTHORITY"] = "WAITING_FOR_AUTHORITY"

    @field_validator("candidate_version", "rule_version")
    @classmethod
    def require_nonempty_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("candidate and rule versions must be non-empty")
        return value

    @model_validator(mode="after")
    def validate_preregistration(self) -> DirectionalPreregistration:
        if self.higher_timeframes != ("15m", "1h"):
            raise ValueError("strict prior higher timeframes must be exactly 15m and 1h")
        if len(set(self.outcome_horizons_bars)) != len(self.outcome_horizons_bars):
            raise ValueError("outcome horizons must be unique")
        split_names = [split.name for split in self.splits]
        if len(set(split_names)) != len(split_names):
            raise ValueError("split names must be unique")
        for previous, current in zip(self.splits, self.splits[1:], strict=False):
            if previous.end != current.start:
                raise ValueError("splits must be contiguous and non-overlapping")
        symbols = [asset.symbol for asset in self.universe]
        if len(set(symbols)) != len(symbols):
            raise ValueError("universe symbols must be unique")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @property
    def config_sha256(self) -> str:
        return canonical_sha256(self.canonical_payload())


class DirectionalResearchManifest(StrictModel):
    protocol_version: Literal["futures_bidirectional_candidate_v1"] = PROTOCOL_VERSION
    candidate_version: str
    config_sha256: str
    source_identity: str
    data_authority_sha256: str
    universe_manifest_sha256: str
    trial_registry_sha256: str

    @field_validator(
        "candidate_version",
        "config_sha256",
        "source_identity",
        "data_authority_sha256",
        "universe_manifest_sha256",
        "trial_registry_sha256",
    )
    @classmethod
    def require_manifest_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("manifest identities must be non-empty")
        return value

    def manifest_sha256(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class DirectionalReceipt(StrictModel):
    """One direction's evidence receipt; it contains no performance result."""

    direction: Literal[Direction.LONG, Direction.SHORT]
    candidate_version: str
    config_sha256: str
    sample_count: int = Field(ge=0)
    symbol_count: int = Field(ge=0)
    regime_count: int = Field(ge=0)
    censor_fraction: float = Field(ge=0, le=1)
    data_quality_passed: bool
    operational_health_passed: bool
    independent_review_passed: bool


class PromotionEvidence(StrictModel):
    """Externally produced receipts used by the fail-closed promotion gate."""

    candidate_version: str
    config_sha256: str
    data_authority_sha256: str
    trial_registry_sha256: str
    authority_released: bool = False
    historical_screen: HistoricalScreenStatus = HistoricalScreenStatus.WAITING_FOR_AUTHORITY
    prospective_status: ProspectiveStatus = ProspectiveStatus.NOT_STARTED
    independent_review_passed: bool = False
    directions: tuple[DirectionalReceipt, ...] = ()

    @model_validator(mode="after")
    def validate_direction_receipts(self) -> PromotionEvidence:
        directions = [receipt.direction for receipt in self.directions]
        if len(set(directions)) != len(directions):
            raise ValueError("promotion evidence cannot contain duplicate directions")
        return self


class FixtureReplayRow(StrictModel):
    """A small recorded row for deterministic adapter/parity testing."""

    opportunity_id: str
    candidate_version: str
    market: Literal[Market.FUTURES] = Market.FUTURES
    symbol: str
    direction: Literal[Direction.LONG, Direction.SHORT]
    decision_time_ms: int = Field(ge=0)
    candle_close_time_ms: int = Field(ge=0)
    higher_timeframe_time_ms: int = Field(ge=0)
    entry_open_time_ms: int = Field(ge=0)
    entry_price: float = Field(gt=0)
    exit_price: float = Field(gt=0)
    funding_return: float
    candle_closed: bool = True

    @field_validator("opportunity_id", "candidate_version", "symbol")
    @classmethod
    def require_row_identity(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("fixture row identity must be non-empty")
        return value


class ReplayReceipt(StrictModel):
    protocol_version: Literal["futures_bidirectional_candidate_v1"] = PROTOCOL_VERSION
    candidate_version: str
    input_rows: int = Field(ge=0)
    unique_rows: int = Field(ge=0)
    exact_duplicates_removed: int = Field(ge=0)
    replay_sha256: str


def canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def load_preregistration(path: str | Path) -> DirectionalPreregistration:
    """Load a successor preregistration without touching existing configs."""

    with Path(path).open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError("directional preregistration must be a YAML mapping")
    return DirectionalPreregistration.model_validate(_normalize_yaml_timestamps(payload))


def _normalize_yaml_timestamps(value: Any) -> Any:
    """Keep YAML timestamp parsing from changing the canonical config identity."""

    if isinstance(value, datetime):
        normalized = value
        if normalized.tzinfo is None:
            normalized = normalized.replace(tzinfo=UTC)
        return normalized.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {key: _normalize_yaml_timestamps(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_yaml_timestamps(item) for item in value]
    return value


def build_research_manifest(
    preregistration: DirectionalPreregistration,
    *,
    source_identity: str,
    data_authority_sha256: str,
    universe_manifest_sha256: str,
    trial_registry_sha256: str,
) -> DirectionalResearchManifest:
    return DirectionalResearchManifest(
        candidate_version=preregistration.candidate_version,
        config_sha256=preregistration.config_sha256,
        source_identity=source_identity,
        data_authority_sha256=data_authority_sha256,
        universe_manifest_sha256=universe_manifest_sha256,
        trial_registry_sha256=trial_registry_sha256,
    )


def evaluate_promotion(
    preregistration: DirectionalPreregistration,
    evidence: PromotionEvidence,
    *,
    manifest: DirectionalResearchManifest | None = None,
) -> PromotionVerdict:
    """Return a per-candidate verdict without changing any runtime registry."""

    if not evidence.authority_released:
        return PromotionVerdict.WAITING_FOR_AUTHORITY
    if (
        evidence.candidate_version != preregistration.candidate_version
        or evidence.config_sha256 != preregistration.config_sha256
    ):
        return PromotionVerdict.REJECT
    if manifest is None:
        return PromotionVerdict.WAITING_FOR_AUTHORITY
    if (
        manifest.candidate_version != preregistration.candidate_version
        or manifest.config_sha256 != preregistration.config_sha256
        or evidence.data_authority_sha256 != manifest.data_authority_sha256
        or evidence.trial_registry_sha256 != manifest.trial_registry_sha256
    ):
        return PromotionVerdict.REJECT
    if evidence.historical_screen == HistoricalScreenStatus.NO_QUALIFIED_CANDIDATE:
        return PromotionVerdict.NO_QUALIFIED_CANDIDATE
    if evidence.historical_screen != HistoricalScreenStatus.PASS:
        return PromotionVerdict.CONTINUE_OBSERVING
    if evidence.prospective_status != ProspectiveStatus.COMPLETE:
        return PromotionVerdict.CONTINUE_OBSERVING
    if not evidence.independent_review_passed:
        return PromotionVerdict.CONTINUE_OBSERVING

    by_direction = {receipt.direction: receipt for receipt in evidence.directions}
    minimum = preregistration.minimum_evidence
    for direction in (Direction.LONG, Direction.SHORT):
        receipt = by_direction.get(direction)
        if receipt is None:
            return PromotionVerdict.CONTINUE_OBSERVING
        if (
            receipt.candidate_version != preregistration.candidate_version
            or receipt.config_sha256 != preregistration.config_sha256
            or receipt.sample_count < minimum.min_samples_per_direction
            or receipt.symbol_count < minimum.min_symbols_per_direction
            or receipt.regime_count < minimum.min_regimes_per_direction
            or receipt.censor_fraction > minimum.max_censor_fraction
            or not receipt.data_quality_passed
            or not receipt.operational_health_passed
            or not receipt.independent_review_passed
        ):
            return PromotionVerdict.CONTINUE_OBSERVING
    return PromotionVerdict.PROMOTE


def replay_fixture(
    rows: list[FixtureReplayRow],
    *,
    preregistration: DirectionalPreregistration,
) -> ReplayReceipt:
    """Validate and fingerprint a bounded replay fixture.

    Exact duplicate rows are idempotently removed. A changed payload sharing an
    opportunity ID is rejected, as are open/future-context/same-bar rows.
    """

    unique: dict[str, FixtureReplayRow] = {}
    for row in rows:
        if row.candidate_version != preregistration.candidate_version:
            raise ValueError("fixture row candidate version does not match preregistration")
        if row.candle_close_time_ms != row.decision_time_ms:
            raise ValueError("fixture decision must use the fully closed candle timestamp")
        if not row.candle_closed:
            raise ValueError("fixture contains an unclosed candle")
        if row.higher_timeframe_time_ms >= row.decision_time_ms:
            raise ValueError("higher-timeframe context must be strictly prior")
        if row.entry_open_time_ms <= row.decision_time_ms:
            raise ValueError("entry must use the next bar open")
        previous = unique.get(row.opportunity_id)
        if previous is not None and previous != row:
            raise ValueError("opportunity ID was replayed with a changed payload")
        unique[row.opportunity_id] = row

    canonical_rows = [
        row.model_dump(mode="json") for _, row in sorted(unique.items(), key=lambda item: item[0])
    ]
    return ReplayReceipt(
        candidate_version=preregistration.candidate_version,
        input_rows=len(rows),
        unique_rows=len(canonical_rows),
        exact_duplicates_removed=len(rows) - len(canonical_rows),
        replay_sha256=canonical_sha256(canonical_rows),
    )
