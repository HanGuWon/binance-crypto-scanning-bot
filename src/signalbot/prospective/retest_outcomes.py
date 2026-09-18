"""Frozen, offline outcomes for causal-retest prospective references.

The evaluator consumes immutable references and already-closed primary bars.
It never infers intrabar ordering, forward-fills gaps, or treats a candle close
as an executable fill.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.enums import Direction, Market
from signalbot.domain.models import Candle, ObservedBboSnapshot
from signalbot.prospective.retest import RetestReadySnapshot

OUTCOME_SCHEMA_VERSION = "retest_outcome_v1"
PRIMARY_INTERVAL = "5m"
HORIZONS_BARS = (1, 3, 6, 12)


class OutcomeStatus(StrEnum):
    COMPLETE = "COMPLETE"
    CENSORED = "CENSORED"
    EXCLUDED = "EXCLUDED"


@dataclass(frozen=True, slots=True)
class RetestOutcomePolicy:
    schema_version: str = OUTCOME_SCHEMA_VERSION
    primary_interval: str = PRIMARY_INTERVAL
    horizons_bars: tuple[int, ...] = HORIZONS_BARS
    direction_return_convention: str = "long=price/ref-1;short=1-price/ref"
    mfe_mae_convention: str = "max/min directional return over candle high/low"
    time_to_extreme_convention: str = "first completed bar index; intrabar order unknown"
    gap_semantics: str = "strict contiguous completed bars; no fill/interpolation"
    reference_price_semantics: str = "signal close plus executable BBO when recorded"
    cost_semantics: str = "descriptive and 26bps round-trip research adjustment only"
    path_start_semantics: str = (
        "first full 5m bar whose open is strictly after effective reference "
        "availability; BBO receipt time participates when executable BBO is recorded"
    )

    def __post_init__(self) -> None:
        if self.schema_version != OUTCOME_SCHEMA_VERSION:
            raise ValueError("unsupported retest outcome schema")
        if self.primary_interval != PRIMARY_INTERVAL:
            raise ValueError("retest outcomes require primary interval 5m")
        if self.horizons_bars != HORIZONS_BARS:
            raise ValueError("retest outcome horizons are frozen at 1/3/6/12 bars")

    @property
    def sha256(self) -> str:
        payload = {
            "schema_version": self.schema_version,
            "primary_interval": self.primary_interval,
            "horizons_bars": list(self.horizons_bars),
            "direction_return_convention": self.direction_return_convention,
            "mfe_mae_convention": self.mfe_mae_convention,
            "time_to_extreme_convention": self.time_to_extreme_convention,
            "gap_semantics": self.gap_semantics,
            "reference_price_semantics": self.reference_price_semantics,
            "cost_semantics": self.cost_semantics,
            "path_start_semantics": self.path_start_semantics,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class RetestReference:
    opportunity_id: str
    campaign_id: str
    campaign_manifest_sha256: str
    retest_policy_sha256: str
    reference_kind: str
    direction: Direction
    market: Market
    symbol: str
    reference_decision_time_ms: int
    signal_close_reference_price: float
    executable_bbo_reference_price: float | None
    observed_bbo: ObservedBboSnapshot | None = None


@dataclass(frozen=True, slots=True)
class RetestOutcome:
    opportunity_id: str
    campaign_id: str
    campaign_manifest_sha256: str
    retest_policy_sha256: str
    outcome_policy_sha256: str
    reference_kind: str
    direction: Direction
    reference_decision_time_ms: int
    effective_reference_time_ms: int
    signal_close_reference_price: float
    executable_bbo_reference_price: float | None
    observed_bbo_identity: str | None
    horizon_bars: int
    directional_mfe: float | None
    directional_mae: float | None
    terminal_directional_return: float | None
    descriptive_terminal_return: float | None
    bbo_entry_terminal_return: float | None
    cost_model_adjusted_research_return: float | None
    time_to_mfe_bars: int | None
    time_to_mae_bars: int | None
    observed_until_ms: int | None
    status: OutcomeStatus
    reason: str | None = None
    bbo_unavailable_reason: str | None = None


def _directional(direction: Direction, price: float, reference: float) -> float:
    if not math.isfinite(price) or not math.isfinite(reference) or reference <= 0:
        raise ValueError("outcome prices must be finite and positive")
    return price / reference - 1.0 if direction is Direction.LONG else 1.0 - price / reference


def _bbo_identity(value: ObservedBboSnapshot | None) -> str | None:
    if value is None:
        return None
    payload = value.model_dump(mode="json")
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _effective_reference_time(reference: RetestReference) -> tuple[int, str | None]:
    """Return causal path start and an explicit missing-clock diagnostic."""

    observed = reference.observed_bbo
    if observed is None:
        return reference.reference_decision_time_ms, None
    if observed.receipt_time_ms is None:
        return reference.reference_decision_time_ms, "BBO_RECEIPT_TIME_UNAVAILABLE"
    return (
        max(reference.reference_decision_time_ms, observed.receipt_time_ms),
        None,
    )


def evaluate_reference(
    reference: RetestReference,
    candles: Iterable[Candle],
    *,
    policy: RetestOutcomePolicy | None = None,
    cost_bps: float = 26.0,
) -> tuple[RetestOutcome, ...]:
    """Evaluate all frozen horizons for one reference and exact candle path."""

    policy = policy or RetestOutcomePolicy()
    if policy.primary_interval != PRIMARY_INTERVAL:
        raise ValueError("causal retest outcomes require primary interval 5m")
    if reference.retest_policy_sha256 == "":
        raise ValueError("outcome reference requires retest policy provenance")
    if cost_bps < 0 or not math.isfinite(cost_bps):
        raise ValueError("cost_bps must be finite and non-negative")
    effective_reference_time_ms, bbo_unavailable_reason = _effective_reference_time(
        reference
    )
    step = interval_to_milliseconds(policy.primary_interval)
    ordered = sorted(
        (
            candle
            for candle in candles
            if candle.market is reference.market
            and candle.symbol == reference.symbol
            and candle.interval == policy.primary_interval
            and candle.is_closed
            and candle.open_time_ms > effective_reference_time_ms
        ),
        key=lambda candle: candle.open_time_ms,
    )
    results: list[RetestOutcome] = []
    for horizon in policy.horizons_bars:
        path = ordered[:horizon]
        status = OutcomeStatus.COMPLETE
        reason: str | None = None
        expected_start = ((effective_reference_time_ms // step) + 1) * step
        has_gap = bool(path) and (
            path[0].open_time_ms != expected_start
            or any(
                candle.open_time_ms != path[index - 1].open_time_ms + step
                for index, candle in enumerate(path)
                if index > 0
            )
        )
        if has_gap:
            status = OutcomeStatus.EXCLUDED
            reason = "DATA_GAP"
        elif len(path) < horizon:
            status = OutcomeStatus.CENSORED
            reason = "INSUFFICIENT_HORIZON"
        if status is not OutcomeStatus.COMPLETE:
            results.append(
                _row(
                    reference,
                    policy,
                    horizon,
                    status,
                    reason,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    effective_reference_time_ms=effective_reference_time_ms,
                    bbo_unavailable_reason=bbo_unavailable_reason,
                )
            )
            continue
        descriptive_ref = reference.signal_close_reference_price
        executable_ref = reference.executable_bbo_reference_price
        if descriptive_ref <= 0 or not math.isfinite(descriptive_ref):
            raise ValueError("signal close reference must be finite and positive")
        directional_values = [
            _directional(reference.direction, float(candle.high), descriptive_ref)
            for candle in path
        ]
        adverse_values = [
            _directional(reference.direction, float(candle.low), descriptive_ref)
            for candle in path
        ]
        # For SHORT, the adverse/favorable extrema swap high/low.
        if reference.direction is Direction.SHORT:
            directional_values, adverse_values = (
                [
                    _directional(reference.direction, float(candle.low), descriptive_ref)
                    for candle in path
                ],
                [
                    _directional(reference.direction, float(candle.high), descriptive_ref)
                    for candle in path
                ],
            )
        mfe = max(directional_values)
        mae = min(adverse_values)
        terminal = _directional(reference.direction, float(path[-1].close), descriptive_ref)
        bbo_terminal = (
            None
            if executable_ref is None or bbo_unavailable_reason is not None
            else _directional(reference.direction, float(path[-1].close), executable_ref)
        )
        adjusted = None if bbo_terminal is None else bbo_terminal - cost_bps / 10_000
        results.append(
            _row(
                reference,
                policy,
                horizon,
                status,
                None,
                mfe,
                mae,
                terminal,
                terminal,
                bbo_terminal,
                adjusted,
                directional_values.index(mfe) + 1,
                adverse_values.index(mae) + 1,
                observed_until_ms=path[-1].close_time_ms,
                effective_reference_time_ms=effective_reference_time_ms,
                bbo_unavailable_reason=bbo_unavailable_reason,
            )
        )
    return tuple(results)


def reference_from_raw_observation(
    payload: dict[str, object],
    *,
    campaign_id: str,
    campaign_manifest_sha256: str,
    retest_policy_sha256: str,
) -> RetestReference:
    """Build a RAW_C0 reference without treating v1 rows as v2 evidence."""

    provenance = payload.get("provenance")
    if not isinstance(provenance, dict):
        raise ValueError("observation is missing provenance")
    schema = provenance.get("observation_schema_version")
    if schema != "shadow_observation_v2":
        raise ValueError("exact raw BBO requires shadow_observation_v2")
    common = payload.get("common_causal_input")
    execution = payload.get("execution_evidence")
    shadow = payload.get("shadow")
    if (
        not isinstance(common, dict)
        or not isinstance(execution, dict)
        or not isinstance(shadow, dict)
    ):
        raise ValueError("observation is missing causal or execution evidence")
    raw = execution.get("observed_bbo")
    observed = None if raw is None else ObservedBboSnapshot.model_validate(raw)
    direction = Direction(str(shadow.get("direction", "LONG")))
    executable = execution.get("executable_bbo_reference_price")
    return RetestReference(
        opportunity_id=str(shadow["opportunity_id"]),
        campaign_id=campaign_id,
        campaign_manifest_sha256=campaign_manifest_sha256,
        retest_policy_sha256=retest_policy_sha256,
        reference_kind="RAW_C0",
        direction=direction,
        market=Market(str(common.get("market", "SPOT"))),
        symbol=str(common.get("symbol", "")).upper(),
        reference_decision_time_ms=int(common["event_time_ms"]),
        signal_close_reference_price=float(
            execution.get("decision_close_price", common["price"])
        ),
        executable_bbo_reference_price=(None if executable is None else float(executable)),
        observed_bbo=observed,
    )


def reference_from_ready_snapshot(
    snapshot: RetestReadySnapshot,
    *,
    retest_policy_sha256: str,
) -> RetestReference:
    """Build the READY-time reference for the same opportunity identity."""

    return RetestReference(
        opportunity_id=snapshot.opportunity_id,
        campaign_id=snapshot.campaign_id,
        campaign_manifest_sha256=snapshot.campaign_manifest_sha256,
        retest_policy_sha256=retest_policy_sha256,
        reference_kind="RETEST_READY",
        direction=snapshot.direction,
        market=snapshot.market,
        symbol=snapshot.symbol,
        reference_decision_time_ms=snapshot.decision_time_ms,
        signal_close_reference_price=(
            snapshot.signal_close_reference_price or snapshot.price
        ),
        executable_bbo_reference_price=snapshot.executable_bbo_reference_price,
        observed_bbo=snapshot.bbo.raw_bbo,
    )


def _row(
    reference: RetestReference,
    policy: RetestOutcomePolicy,
    horizon: int,
    status: OutcomeStatus,
    reason: str | None,
    mfe: float | None,
    mae: float | None,
    terminal: float | None,
    descriptive: float | None,
    bbo_terminal: float | None,
    adjusted: float | None,
    time_mfe: int | None,
    time_mae: int | None,
    *,
    observed_until_ms: int | None = None,
    effective_reference_time_ms: int,
    bbo_unavailable_reason: str | None = None,
) -> RetestOutcome:
    return RetestOutcome(
        opportunity_id=reference.opportunity_id,
        campaign_id=reference.campaign_id,
        campaign_manifest_sha256=reference.campaign_manifest_sha256,
        retest_policy_sha256=reference.retest_policy_sha256,
        outcome_policy_sha256=policy.sha256,
        reference_kind=reference.reference_kind,
        direction=reference.direction,
        reference_decision_time_ms=reference.reference_decision_time_ms,
        effective_reference_time_ms=effective_reference_time_ms,
        signal_close_reference_price=reference.signal_close_reference_price,
        executable_bbo_reference_price=reference.executable_bbo_reference_price,
        observed_bbo_identity=_bbo_identity(reference.observed_bbo),
        horizon_bars=horizon,
        directional_mfe=mfe,
        directional_mae=mae,
        terminal_directional_return=terminal,
        descriptive_terminal_return=descriptive,
        bbo_entry_terminal_return=bbo_terminal,
        cost_model_adjusted_research_return=adjusted,
        time_to_mfe_bars=time_mfe,
        time_to_mae_bars=time_mae,
        observed_until_ms=observed_until_ms,
        status=status,
        reason=reason,
        bbo_unavailable_reason=bbo_unavailable_reason,
    )
