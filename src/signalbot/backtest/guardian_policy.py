from __future__ import annotations

import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext
from enum import StrEnum
from math import ceil
from pathlib import Path
from typing import Any

from signalbot.backtest.config import BacktestSpec, load_backtest_spec
from signalbot.backtest.dataset import (
    KlineDatasetRequest,
    build_dataset_manifest,
    read_kline_csv,
    verify_dataset_manifest,
)
from signalbot.backtest.engine import FundingRate, ResearchBacktester, Trade, build_market_regimes
from signalbot.backtest.funding import funding_sha256, verify_funding_dataset
from signalbot.backtest.runner import dataset_path, funding_path
from signalbot.config import Settings, load_settings
from signalbot.data.candles import interval_to_milliseconds
from signalbot.data.microstructure import OrderFlowSnapshot
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import Candle, FeatureSnapshot, MarketRegime, SignalDecision
from signalbot.signals.position_management import (
    ManagedPositionSnapshot,
    ProtectiveStopPlanner,
    ProtectiveStopPolicy,
)
from signalbot.signals.positions import (
    ExitReason,
    PaperPosition,
    TechnicalExitEngine,
    calculate_trailing_stop_candidate,
)
from signalbot.signals.protection_context import ProtectionContext

FROZEN_L60_02_V2_CONTRACT_SHA256 = (
    "23140ebd342ccf5e2b6c1ba9a6f8b180ece420cf8277db7fe944c0b3790fefb7"
)
FROZEN_L60_02_PARENT_CONTRACT_SHA256 = (
    "2e19fd7a597dd78a0372753da50fd54dcfacceea6e9482bb34aac606c507a923"
)
_CONTRACT_DOMAIN = b"GUARDIAN_POLICY_SELECTION_CONTRACT_V2\0"
_SUPPORTED_MOMENTUM_RULE_ID = "technical_exit_one_bar_trend_failure_v1"
_POLICY_IDS = (
    "delayed_atr_trail_v1",
    "initial_stop_only_v1",
    "confirmed_swing_atr_trail_v1",
    "weakening_sensitive_adaptive_trail_v1",
)
_BASELINE_POLICY_ID = "delayed_atr_trail_v1"
_DECIMAL_CONTEXT = Context(prec=34, rounding=ROUND_HALF_EVEN)
_ONE_DAY_MS = 86_400_000


class GuardianPolicyContractError(RuntimeError):
    """Raised when L60-03 cannot prove that frozen preregistration still applies."""


class GuardianExitPhase(StrEnum):
    OPEN = "OPEN"
    INTRABAR = "INTRABAR"
    CLOSE = "CLOSE"


@dataclass(frozen=True, slots=True)
class GuardianEntryRow:
    position_id: str
    asset: str
    cohort: str
    symbol: str
    direction: Direction
    family: SignalFamily
    score: int
    split: str
    regime: str
    rule_version: str
    entry_signal_id: str
    entry_signal_time_ms: int
    entry_time_ms: int
    entry_index: int
    entry_price: Decimal
    entry_execution_price: Decimal
    initial_stop: Decimal
    notional_usdt: Decimal
    quantity: Decimal
    trend_state: str
    entry_atr_percent: Decimal | None = None


@dataclass(frozen=True, slots=True)
class GuardianBarContext:
    candle_close_time_ms: int
    atr: Decimal
    confirmed_swing_support: Decimal | None = None
    confirmed_swing_resistance: Decimal | None = None
    momentum_weakened_long: bool = False
    momentum_weakened_short: bool = False
    context_ready: bool = True

    def structure_stop(self, direction: Direction) -> Decimal | None:
        if direction is Direction.LONG:
            return self.confirmed_swing_support
        if direction is Direction.SHORT:
            return self.confirmed_swing_resistance
        return None

    def momentum_weakened(self, direction: Direction) -> bool:
        if direction is Direction.LONG:
            return self.momentum_weakened_long
        if direction is Direction.SHORT:
            return self.momentum_weakened_short
        return False


@dataclass(frozen=True, slots=True)
class GuardianPolicyEpisode:
    position_id: str
    policy_id: str
    symbol: str
    direction: Direction
    entry_time_ms: int
    exit_time_ms: int | None
    exit_bar_index: int | None
    exit_phase: GuardianExitPhase | None
    exit_price: Decimal | None
    exit_atr: Decimal | None
    bars_held: int
    valid_primary: bool
    censor_reason: str | None
    stop_update_count: int
    gap_through_slippage_bps: Decimal
    same_bar_ambiguity_count: int
    directional_return_bps: Decimal | None
    realized_signed_funding_bps: Decimal | None
    after_cost_return_bps: Decimal | None
    mfe_bps: Decimal | None
    mfe_giveback_fraction: Decimal | None

    @property
    def exposure_ms(self) -> int | None:
        if self.exit_time_ms is None:
            return None
        return self.exit_time_ms - self.entry_time_ms


@dataclass(frozen=True, slots=True)
class GuardianPairMetrics:
    challenger_policy_id: str
    admitted_positions: int
    paired_valid_positions: int
    censor_fraction: Decimal
    mean_after_cost_delta_bps: Decimal | None
    candidate_maximum_drawdown: Decimal | None
    baseline_maximum_drawdown: Decimal | None
    candidate_cvar_5_return_bps: Decimal | None
    baseline_cvar_5_return_bps: Decimal | None
    candidate_premature_stop_rate: Decimal | None
    baseline_premature_stop_rate: Decimal | None
    candidate_mean_mfe_giveback_fraction: Decimal | None
    baseline_mean_mfe_giveback_fraction: Decimal | None
    candidate_mean_stop_updates_per_24h_exposure: Decimal | None
    baseline_mean_stop_updates_per_24h_exposure: Decimal | None
    candidate_total_stop_update_count: int
    baseline_total_stop_update_count: int
    candidate_mean_gap_through_slippage_bps: Decimal | None
    baseline_mean_gap_through_slippage_bps: Decimal | None
    candidate_same_bar_ambiguity_count: int
    baseline_same_bar_ambiguity_count: int
    evidence_total_positions: int
    evidence_calendar_days: int
    evidence_positions_by_direction: dict[str, int]
    evidence_symbols_by_direction: dict[str, int]
    evidence_trend_states: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class GuardianStratumMetrics:
    challenger_policy_id: str
    dimension: str
    stratum: str
    paired_valid_positions: int
    candidate_mean_after_cost_return_bps: Decimal | None
    baseline_mean_after_cost_return_bps: Decimal | None
    mean_after_cost_delta_bps: Decimal | None
    candidate_premature_stop_rate: Decimal | None
    candidate_mean_mfe_giveback_fraction: Decimal | None
    baseline_mean_mfe_giveback_fraction: Decimal | None
    candidate_mean_stop_updates_per_24h_exposure: Decimal | None
    baseline_mean_stop_updates_per_24h_exposure: Decimal | None


def _decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _canonical_contract_sha256(value: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(_CONTRACT_DOMAIN + canonical).hexdigest()


def _canonical_text_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def _canonical_settings_sha256(settings: Settings) -> str:
    canonical = json.dumps(
        settings.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _git_blob_sha256(workspace: Path, commit: str, source_path: str) -> str:
    try:
        blob = subprocess.check_output(
            ["git", "-C", str(workspace), "cat-file", "blob", f"{commit}:{source_path}"],
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise GuardianPolicyContractError(
            f"cannot verify frozen Git source authority: {source_path}"
        ) from exc
    return hashlib.sha256(blob).hexdigest()


def _require_mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GuardianPolicyContractError(f"{label} must be an object")
    return value


def load_guardian_policy_contract(
    path: str | Path,
    *,
    workspace_root: str | Path,
) -> dict[str, Any]:
    """Load and authenticate the frozen L60-02 v2 contract and source identities."""

    contract_path = Path(path)
    try:
        raw = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise GuardianPolicyContractError("cannot read Guardian policy contract") from exc
    contract = _require_mapping(raw, "Guardian policy contract")
    actual_sha = _canonical_contract_sha256(contract)
    if actual_sha != FROZEN_L60_02_V2_CONTRACT_SHA256:
        raise GuardianPolicyContractError(
            "Guardian policy contract semantic SHA-256 does not match frozen L60-02 v2"
        )
    if contract.get("contract_version") != "guardian-policy-selection-v2":
        raise GuardianPolicyContractError("Guardian policy contract version is not frozen v2")
    if contract.get("status") != "PREREGISTERED_OUTCOME_BLIND":
        raise GuardianPolicyContractError("Guardian policy contract is not outcome-blind")
    if contract.get("bound_text_source_hash_rule") is None:
        raise GuardianPolicyContractError("bound text-source hash rule is missing")
    parent = _require_mapping(contract.get("parent_contract"), "parent_contract")
    if (
        parent.get("contract_version") != "guardian-policy-selection-v1"
        or parent.get("semantic_sha256") != FROZEN_L60_02_PARENT_CONTRACT_SHA256
    ):
        raise GuardianPolicyContractError("Guardian v2 parent contract provenance drifted")

    workspace = Path(workspace_root)
    cohort = _require_mapping(contract.get("cohort_authority"), "cohort_authority")
    historical = _require_mapping(cohort.get("historical_harness"), "historical_harness")
    funding = _require_mapping(contract.get("funding_authority"), "funding_authority")
    invariants = _require_mapping(
        contract.get("policy_common_invariants"), "policy_common_invariants"
    )
    momentum = _require_mapping(
        contract.get("historical_momentum_weakening_authority"),
        "historical_momentum_weakening_authority",
    )
    bound_sources = (
        (historical.get("source_path"), historical.get("source_sha256")),
        (
            cohort.get("protection_context_source_path"),
            cohort.get("protection_context_source_sha256"),
        ),
        (funding.get("historical_source_path"), funding.get("historical_source_sha256")),
        (invariants.get("l60_01_source_path"), invariants.get("l60_01_source_sha256")),
        (momentum.get("source_path"), momentum.get("source_sha256")),
    )
    for source_path, expected_sha in bound_sources:
        if not isinstance(source_path, str) or not isinstance(expected_sha, str):
            raise GuardianPolicyContractError("bound source path/hash is malformed")
        source = workspace / source_path
        if not source.is_file():
            raise GuardianPolicyContractError(f"bound source is missing: {source_path}")
        if _canonical_text_sha256(source) != expected_sha:
            raise GuardianPolicyContractError(f"bound source identity drifted: {source_path}")

    producer = _require_mapping(
        contract.get("historical_entry_producer_authority"),
        "historical_entry_producer_authority",
    )
    commit = producer.get("git_commit")
    producer_sources = _require_mapping(producer.get("source_sha256"), "producer source_sha256")
    if not isinstance(commit, str) or len(commit) != 40:
        raise GuardianPolicyContractError("historical producer Git commit is malformed")
    for source_path, expected_sha in sorted(producer_sources.items()):
        if not isinstance(source_path, str) or not isinstance(expected_sha, str):
            raise GuardianPolicyContractError("historical producer source binding is malformed")
        source = workspace / source_path
        if not source.is_file():
            raise GuardianPolicyContractError(
                f"historical producer source is missing: {source_path}"
            )
        if _canonical_text_sha256(source) != expected_sha:
            raise GuardianPolicyContractError(
                f"historical producer worktree identity drifted: {source_path}"
            )
        if _git_blob_sha256(workspace, commit, source_path) != expected_sha:
            raise GuardianPolicyContractError(
                f"historical producer Git identity drifted: {source_path}"
            )

    policies = contract.get("policies")
    if (
        not isinstance(policies, list)
        or tuple(item.get("policy_id") for item in policies if isinstance(item, dict))
        != _POLICY_IDS
    ):
        raise GuardianPolicyContractError("frozen Guardian policy set changed")
    return contract


def validate_guardian_historical_settings(
    contract: Mapping[str, Any],
    settings: Settings,
    *,
    config_path: str | Path | None,
    workspace_root: str | Path,
) -> None:
    """Fail closed on config bytes, parsed Settings, or operator path drift before data reads."""

    workspace = Path(workspace_root).resolve()
    cohort = _require_mapping(contract.get("cohort_authority"), "cohort_authority")
    historical = _require_mapping(cohort.get("historical_harness"), "historical_harness")
    bound_path = historical.get("settings_path")
    bound_sha = historical.get("settings_sha256")
    effective_sha = historical.get("effective_settings_sha256")
    if not all(isinstance(value, str) for value in (bound_path, bound_sha, effective_sha)):
        raise GuardianPolicyContractError("historical Settings authority is malformed")
    assert isinstance(bound_path, str)
    assert isinstance(bound_sha, str)
    assert isinstance(effective_sha, str)
    expected_path = (workspace / bound_path).resolve()
    if config_path is None or Path(config_path).resolve() != expected_path:
        raise GuardianPolicyContractError(
            f"historical config path must be the frozen Settings authority: {bound_path}"
        )
    if not expected_path.is_file() or _canonical_text_sha256(expected_path) != bound_sha:
        raise GuardianPolicyContractError("historical Settings input identity drifted")
    try:
        bound_settings = load_settings(expected_path)
    except (OSError, UnicodeError, ValueError) as exc:
        raise GuardianPolicyContractError("cannot load frozen historical Settings") from exc
    if _canonical_settings_sha256(bound_settings) != effective_sha:
        raise GuardianPolicyContractError("effective historical Settings identity drifted")
    if settings.model_dump(mode="json") != bound_settings.model_dump(mode="json"):
        raise GuardianPolicyContractError(
            "supplied Settings differ from frozen historical Settings"
        )


def derive_guardian_historical_spec(
    contract: Mapping[str, Any],
    *,
    workspace_root: str | Path,
) -> BacktestSpec:
    """Load the bound source spec and apply only the preregistered direction override."""

    cohort = _require_mapping(contract.get("cohort_authority"), "cohort_authority")
    historical = _require_mapping(cohort.get("historical_harness"), "historical_harness")
    source_path = historical.get("source_path")
    if not isinstance(source_path, str):
        raise GuardianPolicyContractError("historical source_path is missing")
    source = Path(workspace_root) / source_path
    base = load_backtest_spec(source)
    override = historical.get("derived_override")
    if override != {"direction_scope": "futures_bidirectional"}:
        raise GuardianPolicyContractError("historical derived override is not frozen")
    derived = base.model_copy(update=override)
    base_values = base.model_dump(mode="python")
    derived_values = derived.model_dump(mode="python")
    changed = sorted(key for key in base_values if base_values.get(key) != derived_values.get(key))
    if changed != ["direction_scope"]:
        raise GuardianPolicyContractError(
            f"historical derived spec changed unexpected fields: {changed}"
        )
    if derived.direction_scope != "futures_bidirectional":
        raise GuardianPolicyContractError("historical direction scope is not bidirectional")
    return derived


def _require_historical_momentum_rule(contract: Mapping[str, Any]) -> str:
    """Fail before outcomes until a versioned preregistration binds the boolean owner."""

    authority = contract.get("historical_momentum_weakening_authority")
    if not isinstance(authority, dict):
        raise GuardianPolicyContractError(
            "L60-03 real outcomes are blocked: L60-02 does not bind a historical "
            "momentum_weakened authority"
        )
    rule_id = authority.get("rule_id")
    if rule_id != _SUPPORTED_MOMENTUM_RULE_ID:
        raise GuardianPolicyContractError(
            "historical momentum_weakened authority is unsupported or unreviewed"
        )
    return rule_id


def _historical_momentum_weakened(feature: FeatureSnapshot, direction: Direction) -> bool:
    """Existing TechnicalExitEngine one-bar trend-failure predicate, when contract-bound."""

    if direction is Direction.LONG:
        return feature.price < feature.ema20 and feature.macd_histogram < 0
    if direction is Direction.SHORT:
        return feature.price > feature.ema20 and feature.macd_histogram > 0
    return False


def _position_id_from_trade(trade: Trade) -> str:
    identity = "|".join(
        (
            "guardian-entry-v1",
            trade.protocol_version,
            trade.rule_version,
            trade.asset,
            trade.cohort,
            trade.market,
            trade.symbol,
            trade.direction,
            trade.family,
            trade.entry_signal_id,
            str(trade.entry_signal_time_ms),
            str(trade.entry_time_ms),
            format(trade.entry_price, ".17g"),
            format(trade.initial_stop, ".17g"),
        )
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def freeze_guardian_entry_row(
    trade: Trade,
    *,
    entry_index: int,
    notional_usdt: Decimal,
    trend_state: str,
    entry_atr_percent: Decimal | None = None,
) -> GuardianEntryRow:
    """Drop all source-exit fields and freeze a policy-independent entry identity."""

    if trade.market != Market.FUTURES.value:
        raise GuardianPolicyContractError("Guardian historical entries must be Futures")
    direction = Direction(trade.direction)
    if direction not in {Direction.LONG, Direction.SHORT}:
        raise GuardianPolicyContractError("Guardian entry direction must be long or short")
    entry_price = _decimal(trade.entry_price)
    entry_execution_price = _decimal(trade.entry_execution_price)
    initial_stop = _decimal(trade.initial_stop)
    if entry_execution_price <= 0:
        raise GuardianPolicyContractError("Guardian entry execution price must be positive")
    with localcontext(_DECIMAL_CONTEXT):
        quantity = notional_usdt / entry_price
    return GuardianEntryRow(
        position_id=_position_id_from_trade(trade),
        asset=trade.asset,
        cohort=trade.cohort,
        symbol=trade.symbol,
        direction=direction,
        family=SignalFamily(trade.family),
        score=trade.score,
        split=trade.split,
        regime=trade.regime,
        rule_version=trade.rule_version,
        entry_signal_id=trade.entry_signal_id,
        entry_signal_time_ms=trade.entry_signal_time_ms,
        entry_time_ms=trade.entry_time_ms,
        entry_index=entry_index,
        entry_price=entry_price,
        entry_execution_price=entry_execution_price,
        initial_stop=initial_stop,
        notional_usdt=notional_usdt,
        quantity=quantity,
        trend_state=trend_state,
        entry_atr_percent=entry_atr_percent,
    )


def _paper_position(entry: GuardianEntryRow) -> PaperPosition:
    decision = SignalDecision(
        event_id=entry.entry_signal_id,
        market=Market.FUTURES,
        symbol=entry.symbol,
        family=entry.family,
        stage=SignalStage.CONFIRMED,
        direction=entry.direction,
        timeframe="5m",
        event_time_ms=entry.entry_signal_time_ms,
        score=entry.score,
        price=entry.entry_price,
        invalidation=entry.initial_stop,
        regime=MarketRegime(label=entry.regime),
        rule_version=entry.rule_version,
    )
    return PaperPosition(
        decision=decision,
        entry_index=entry.entry_index,
        entry_time_ms=entry.entry_time_ms,
        entry_price=float(entry.entry_price),
        initial_stop=float(entry.initial_stop),
        active_stop=float(entry.initial_stop),
        active_stop_reason=ExitReason.INITIAL_STOP,
        highest_price=float(entry.entry_price),
        lowest_price=float(entry.entry_price),
    )


def _planner(policy_id: str) -> ProtectiveStopPlanner:
    if policy_id not in _POLICY_IDS:
        raise GuardianPolicyContractError(f"unknown Guardian policy_id={policy_id}")
    return ProtectiveStopPlanner(
        ProtectiveStopPolicy(
            policy_version=f"guardian-backtest:{policy_id}",
            trailing_activation_r=1.0,
            trailing_atr_multiple=2.0,
            manual_activation_atr=1.0,
            minimum_price_gap_bps=5.0,
            minimum_improvement_bps=1.0,
        )
    )


def _apply_common_stop_gates(
    snapshot: ManagedPositionSnapshot,
    *,
    raw_candidate: float,
    minimum_price_gap_bps: float = 5.0,
    minimum_improvement_bps: float = 1.0,
) -> float | None:
    """Apply the bound L60-01 common invariants to an already-derived candidate."""

    if snapshot.direction is Direction.LONG and raw_candidate >= snapshot.reference_price:
        return None
    if snapshot.direction is Direction.SHORT and raw_candidate <= snapshot.reference_price:
        return None
    price_gap = snapshot.reference_price * minimum_price_gap_bps / 10_000
    if snapshot.direction is Direction.LONG:
        proposed = max(raw_candidate, snapshot.protection_floor or raw_candidate)
        proposed = min(proposed, snapshot.reference_price - price_gap)
        improvement = proposed - snapshot.active_stop
        if snapshot.protection_floor is not None and proposed < snapshot.protection_floor:
            return None
        if proposed >= snapshot.reference_price:
            return None
    else:
        proposed = min(raw_candidate, snapshot.protection_floor or raw_candidate)
        proposed = max(proposed, snapshot.reference_price + price_gap)
        improvement = snapshot.active_stop - proposed
        if snapshot.protection_floor is not None and proposed > snapshot.protection_floor:
            return None
        if proposed <= snapshot.reference_price:
            return None
    minimum_improvement = snapshot.reference_price * minimum_improvement_bps / 10_000
    if improvement <= 0 or improvement < minimum_improvement:
        return None
    return proposed


def plan_guardian_stop(
    policy_id: str,
    position: PaperPosition,
    candle: Candle,
    context: GuardianBarContext,
) -> float | None:
    """Return a close-time stop amendment; caller makes it active next candle only."""

    if policy_id == "initial_stop_only_v1" or not context.context_ready:
        return None
    snapshot = ManagedPositionSnapshot(
        position_ref=position.decision.event_id,
        market=Market.FUTURES,
        symbol=position.decision.symbol,
        direction=position.direction,
        entry_price=position.entry_price,
        active_stop=position.active_stop,
        reference_price=float(candle.close),
        highest_price=position.highest_price,
        lowest_price=position.lowest_price,
        observed_at_ms=candle.close_time_ms,
        original_risk_stop=position.initial_stop,
        protection_floor=position.initial_stop,
    )
    atr = float(context.atr)
    structure = context.structure_stop(position.direction)
    structure_float = None if structure is None else float(structure)
    if policy_id == "delayed_atr_trail_v1":
        intent = _planner(policy_id).plan(snapshot, atr=atr)
        return None if intent is None else intent.proposed_stop
    if policy_id == "weakening_sensitive_adaptive_trail_v1":
        intent = _planner(policy_id).plan(
            snapshot,
            atr=atr,
            confirmed_structure_stop=structure_float,
            momentum_weakened=context.momentum_weakened(position.direction),
        )
        return None if intent is None else intent.proposed_stop
    if policy_id == "confirmed_swing_atr_trail_v1":
        atr_candidate = calculate_trailing_stop_candidate(
            direction=position.direction,
            entry_price=position.entry_price,
            initial_stop=position.initial_stop,
            active_stop=position.active_stop,
            highest_price=position.highest_price,
            lowest_price=position.lowest_price,
            atr=atr,
            activation_r=1.0,
            atr_multiple=2.0,
        )
        if atr_candidate is None:
            return None
        raw_candidate = atr_candidate
        if structure_float is not None:
            if position.direction is Direction.LONG:
                raw_candidate = max(raw_candidate, structure_float)
            else:
                raw_candidate = min(raw_candidate, structure_float)
        return _apply_common_stop_gates(snapshot, raw_candidate=raw_candidate)
    raise GuardianPolicyContractError(f"unknown Guardian policy_id={policy_id}")


def _favorable_bps(
    direction: Direction,
    entry_execution_price: Decimal,
    prices: Sequence[Decimal],
) -> Decimal:
    if not prices:
        return Decimal("0")
    with localcontext(_DECIMAL_CONTEXT):
        if direction is Direction.LONG:
            favorable = max(prices) - entry_execution_price
        else:
            favorable = entry_execution_price - min(prices)
        return max(
            Decimal("0"),
            favorable / entry_execution_price * Decimal("10000"),
        )


def _funding_bps(
    entry: GuardianEntryRow,
    exit_time_ms: int,
    funding: Sequence[FundingRate],
    *,
    authority_complete: bool,
) -> Decimal | None:
    if not authority_complete:
        return None
    if any(item.funding_time_ms in {entry.entry_time_ms, exit_time_ms} for item in funding):
        return None
    direction_sign = Decimal("1") if entry.direction is Direction.LONG else Decimal("-1")
    with localcontext(_DECIMAL_CONTEXT):
        total = Decimal("0")
        for item in funding:
            if not entry.entry_time_ms < item.funding_time_ms < exit_time_ms:
                continue
            rate = _decimal(item.rate)
            mark = (
                _decimal(item.mark_price)
                if item.mark_price is not None and item.mark_price > 0
                else entry.entry_price
            )
            total += -direction_sign * rate * mark / entry.entry_price
        return Decimal("10000") * total


def _gap_through_bps(
    direction: Direction,
    entry_price: Decimal,
    active_stop: Decimal,
    open_price: Decimal,
) -> Decimal:
    with localcontext(_DECIMAL_CONTEXT):
        adverse = (
            max(Decimal("0"), active_stop - open_price)
            if direction is Direction.LONG
            else max(Decimal("0"), open_price - active_stop)
        )
        return adverse / entry_price * Decimal("10000")


def _episode(
    entry: GuardianEntryRow,
    *,
    policy_id: str,
    exit_time_ms: int | None,
    exit_bar_index: int | None,
    exit_phase: GuardianExitPhase | None,
    exit_price: Decimal | None,
    exit_atr: Decimal | None,
    bars_held: int,
    censor_reason: str | None,
    stop_update_count: int,
    gap_through_slippage_bps: Decimal,
    same_bar_ambiguity_count: int,
    mfe_bps: Decimal | None,
    funding: Sequence[FundingRate],
    funding_authority_complete: bool,
    contract: Mapping[str, Any],
) -> GuardianPolicyEpisode:
    if censor_reason is not None or exit_time_ms is None or exit_price is None:
        return GuardianPolicyEpisode(
            position_id=entry.position_id,
            policy_id=policy_id,
            symbol=entry.symbol,
            direction=entry.direction,
            entry_time_ms=entry.entry_time_ms,
            exit_time_ms=exit_time_ms,
            exit_bar_index=exit_bar_index,
            exit_phase=exit_phase,
            exit_price=exit_price,
            exit_atr=exit_atr,
            bars_held=bars_held,
            valid_primary=False,
            censor_reason=censor_reason or "missing_exit",
            stop_update_count=stop_update_count,
            gap_through_slippage_bps=gap_through_slippage_bps,
            same_bar_ambiguity_count=same_bar_ambiguity_count,
            directional_return_bps=None,
            realized_signed_funding_bps=None,
            after_cost_return_bps=None,
            mfe_bps=mfe_bps,
            mfe_giveback_fraction=None,
        )
    funding_bps = _funding_bps(
        entry,
        exit_time_ms,
        funding,
        authority_complete=funding_authority_complete,
    )
    if funding_bps is None:
        return GuardianPolicyEpisode(
            position_id=entry.position_id,
            policy_id=policy_id,
            symbol=entry.symbol,
            direction=entry.direction,
            entry_time_ms=entry.entry_time_ms,
            exit_time_ms=exit_time_ms,
            exit_bar_index=exit_bar_index,
            exit_phase=exit_phase,
            exit_price=exit_price,
            exit_atr=exit_atr,
            bars_held=bars_held,
            valid_primary=False,
            censor_reason="funding_boundary_or_authority",
            stop_update_count=stop_update_count,
            gap_through_slippage_bps=gap_through_slippage_bps,
            same_bar_ambiguity_count=same_bar_ambiguity_count,
            directional_return_bps=None,
            realized_signed_funding_bps=None,
            after_cost_return_bps=None,
            mfe_bps=mfe_bps,
            mfe_giveback_fraction=None,
        )
    cost_model = _require_mapping(contract.get("cost_model"), "cost_model")
    round_trip = _require_mapping(cost_model.get("round_trip_execution"), "round_trip_execution")
    entry_slippage = _require_mapping(
        round_trip.get("entry_slippage_bps_by_cohort"), "entry_slippage_bps_by_cohort"
    )
    exit_slippage = _require_mapping(
        round_trip.get("exit_slippage_bps_by_cohort"), "exit_slippage_bps_by_cohort"
    )
    with localcontext(_DECIMAL_CONTEXT):
        sign = Decimal("1") if entry.direction is Direction.LONG else Decimal("-1")
        directional_bps = (
            sign * (exit_price - entry.entry_price) / entry.entry_price * Decimal("10000")
        )
        mfe_realized_directional_bps = (
            sign
            * (exit_price - entry.entry_execution_price)
            / entry.entry_execution_price
            * Decimal("10000")
        )
        after_cost = (
            directional_bps
            - _decimal(round_trip["entry_fee_bps"])
            - _decimal(entry_slippage[entry.cohort])
            - _decimal(round_trip["exit_fee_bps"])
            - _decimal(exit_slippage[entry.cohort])
            + funding_bps
            - _decimal(cost_model["modeled_stop_update_cost_bps"]) * stop_update_count
        )
        giveback: Decimal | None = None
        if mfe_bps is not None and mfe_bps > 0:
            captured = min(
                max(mfe_realized_directional_bps, Decimal("0")),
                mfe_bps,
            )
            giveback = Decimal("1") - captured / mfe_bps
    return GuardianPolicyEpisode(
        position_id=entry.position_id,
        policy_id=policy_id,
        symbol=entry.symbol,
        direction=entry.direction,
        entry_time_ms=entry.entry_time_ms,
        exit_time_ms=exit_time_ms,
        exit_bar_index=exit_bar_index,
        exit_phase=exit_phase,
        exit_price=exit_price,
        exit_atr=exit_atr,
        bars_held=bars_held,
        valid_primary=True,
        censor_reason=None,
        stop_update_count=stop_update_count,
        gap_through_slippage_bps=gap_through_slippage_bps,
        same_bar_ambiguity_count=same_bar_ambiguity_count,
        directional_return_bps=directional_bps,
        realized_signed_funding_bps=funding_bps,
        after_cost_return_bps=after_cost,
        mfe_bps=mfe_bps,
        mfe_giveback_fraction=giveback,
    )


def _candidate_exit_atr(
    candles: Sequence[Candle],
    *,
    exit_bar_index: int,
    contexts_by_close: Mapping[int, GuardianBarContext],
) -> Decimal | None:
    """Return the immediately-prior ready positive closed-candle ATR for a stop exit."""

    if exit_bar_index <= 0 or exit_bar_index >= len(candles):
        return None
    current = candles[exit_bar_index]
    previous = candles[exit_bar_index - 1]
    step_ms = interval_to_milliseconds(current.interval)
    if current.open_time_ms - previous.open_time_ms != step_ms:
        return None
    context = contexts_by_close.get(previous.close_time_ms)
    if context is None or not context.context_ready or context.atr <= 0:
        return None
    return context.atr


def replay_guardian_episode(
    entry: GuardianEntryRow,
    *,
    policy_id: str,
    candles: Sequence[Candle],
    contexts_by_close: Mapping[int, GuardianBarContext],
    funding: Sequence[FundingRate],
    contract: Mapping[str, Any],
    funding_authority_complete: bool = True,
) -> GuardianPolicyEpisode:
    """Replay one frozen entry independently with next-candle-only stop amendments."""

    if policy_id not in _POLICY_IDS:
        raise GuardianPolicyContractError(f"unknown Guardian policy_id={policy_id}")
    if entry.entry_index < 0 or entry.entry_index >= len(candles):
        raise GuardianPolicyContractError("entry index lies outside candle tape")
    entry_candle = candles[entry.entry_index]
    if entry_candle.open_time_ms != entry.entry_time_ms:
        raise GuardianPolicyContractError("entry index/time does not align with candle tape")
    if entry_candle.market is not Market.FUTURES or entry_candle.symbol != entry.symbol:
        raise GuardianPolicyContractError("entry candle identity does not match entry row")
    if any(not candle.is_closed for candle in candles[entry.entry_index :]):
        raise GuardianPolicyContractError("Guardian replay requires closed candles only")

    outcome = _require_mapping(contract.get("outcome_semantics"), "outcome_semantics")
    maximum_holding_bars = int(outcome.get("maximum_holding_bars", 0))
    if maximum_holding_bars != 72:
        raise GuardianPolicyContractError("maximum holding period differs from frozen 72 bars")
    step_ms = interval_to_milliseconds(entry_candle.interval)
    position = _paper_position(entry)
    pending_stop: float | None = None
    stop_updates = 0
    ambiguity_count = 0
    gap_slippage = Decimal("0")
    mfe_bps = Decimal("0")
    target_distance = abs(entry.entry_price - entry.initial_stop)
    target = (
        entry.entry_price + target_distance
        if entry.direction is Direction.LONG
        else entry.entry_price - target_distance
    )

    for index in range(entry.entry_index, len(candles)):
        candle = candles[index]
        if candle.symbol != entry.symbol or candle.market is not Market.FUTURES:
            raise GuardianPolicyContractError("mixed symbol/market candle tape")
        if index > entry.entry_index:
            previous = candles[index - 1]
            if candle.open_time_ms - previous.open_time_ms != step_ms:
                return _episode(
                    entry,
                    policy_id=policy_id,
                    exit_time_ms=None,
                    exit_bar_index=None,
                    exit_phase=None,
                    exit_price=None,
                    exit_atr=None,
                    bars_held=index - entry.entry_index,
                    censor_reason="data_gap",
                    stop_update_count=stop_updates,
                    gap_through_slippage_bps=gap_slippage,
                    same_bar_ambiguity_count=ambiguity_count,
                    mfe_bps=mfe_bps,
                    funding=funding,
                    funding_authority_complete=funding_authority_complete,
                    contract=contract,
                )
            if pending_stop is not None:
                position.active_stop = pending_stop
                position.active_stop_reason = ExitReason.TRAILING_STOP
                pending_stop = None
                stop_updates += 1

        open_price = _decimal(candle.open)
        active_stop = _decimal(position.active_stop)
        stop_at_open = TechnicalExitEngine.stop_at_open(position, float(candle.open))
        if stop_at_open is not None:
            gap_slippage += _gap_through_bps(
                entry.direction,
                entry.entry_price,
                active_stop,
                open_price,
            )
            mfe_bps = max(
                mfe_bps,
                _favorable_bps(
                    entry.direction,
                    entry.entry_execution_price,
                    [open_price],
                ),
            )
            exit_atr = _candidate_exit_atr(
                candles,
                exit_bar_index=index,
                contexts_by_close=contexts_by_close,
            )
            if policy_id != _BASELINE_POLICY_ID and exit_atr is None:
                return _episode(
                    entry,
                    policy_id=policy_id,
                    exit_time_ms=candle.open_time_ms,
                    exit_bar_index=index,
                    exit_phase=GuardianExitPhase.OPEN,
                    exit_price=open_price,
                    exit_atr=None,
                    bars_held=index - entry.entry_index,
                    censor_reason="missing_candidate_exit_atr",
                    stop_update_count=stop_updates,
                    gap_through_slippage_bps=gap_slippage,
                    same_bar_ambiguity_count=ambiguity_count,
                    mfe_bps=mfe_bps,
                    funding=funding,
                    funding_authority_complete=funding_authority_complete,
                    contract=contract,
                )
            return _episode(
                entry,
                policy_id=policy_id,
                exit_time_ms=candle.open_time_ms,
                exit_bar_index=index,
                exit_phase=GuardianExitPhase.OPEN,
                exit_price=open_price,
                exit_atr=exit_atr,
                bars_held=index - entry.entry_index,
                censor_reason=None,
                stop_update_count=stop_updates,
                gap_through_slippage_bps=gap_slippage,
                same_bar_ambiguity_count=ambiguity_count,
                mfe_bps=mfe_bps,
                funding=funding,
                funding_authority_complete=funding_authority_complete,
                contract=contract,
            )

        low = _decimal(candle.low)
        high = _decimal(candle.high)
        target_touched = low <= target <= high
        stop_touched = low <= active_stop <= high
        if target_touched and stop_touched:
            ambiguity_count += 1
        stop_in_bar = TechnicalExitEngine.stop_in_bar(position, candle)
        if stop_in_bar is not None:
            stop_price = _decimal(stop_in_bar.price)
            mfe_bps = max(
                mfe_bps,
                _favorable_bps(
                    entry.direction,
                    entry.entry_execution_price,
                    [open_price, stop_price],
                ),
            )
            exit_atr = _candidate_exit_atr(
                candles,
                exit_bar_index=index,
                contexts_by_close=contexts_by_close,
            )
            if policy_id != _BASELINE_POLICY_ID and exit_atr is None:
                return _episode(
                    entry,
                    policy_id=policy_id,
                    exit_time_ms=candle.close_time_ms,
                    exit_bar_index=index,
                    exit_phase=GuardianExitPhase.INTRABAR,
                    exit_price=stop_price,
                    exit_atr=None,
                    bars_held=index - entry.entry_index + 1,
                    censor_reason="missing_candidate_exit_atr",
                    stop_update_count=stop_updates,
                    gap_through_slippage_bps=gap_slippage,
                    same_bar_ambiguity_count=ambiguity_count,
                    mfe_bps=mfe_bps,
                    funding=funding,
                    funding_authority_complete=funding_authority_complete,
                    contract=contract,
                )
            return _episode(
                entry,
                policy_id=policy_id,
                exit_time_ms=candle.close_time_ms,
                exit_bar_index=index,
                exit_phase=GuardianExitPhase.INTRABAR,
                exit_price=stop_price,
                exit_atr=exit_atr,
                bars_held=index - entry.entry_index + 1,
                censor_reason=None,
                stop_update_count=stop_updates,
                gap_through_slippage_bps=gap_slippage,
                same_bar_ambiguity_count=ambiguity_count,
                mfe_bps=mfe_bps,
                funding=funding,
                funding_authority_complete=funding_authority_complete,
                contract=contract,
            )

        held_bars = index - entry.entry_index + 1
        context = contexts_by_close.get(candle.close_time_ms)
        if held_bars >= maximum_holding_bars:
            close_price = _decimal(candle.close)
            mfe_bps = max(
                mfe_bps,
                _favorable_bps(
                    entry.direction,
                    entry.entry_execution_price,
                    [open_price, close_price],
                ),
            )
            return _episode(
                entry,
                policy_id=policy_id,
                exit_time_ms=candle.close_time_ms,
                exit_bar_index=index,
                exit_phase=GuardianExitPhase.CLOSE,
                exit_price=close_price,
                exit_atr=None if context is None else context.atr,
                bars_held=held_bars,
                censor_reason=None,
                stop_update_count=stop_updates,
                gap_through_slippage_bps=gap_slippage,
                same_bar_ambiguity_count=ambiguity_count,
                mfe_bps=mfe_bps,
                funding=funding,
                funding_authority_complete=funding_authority_complete,
                contract=contract,
            )

        mfe_bps = max(
            mfe_bps,
            _favorable_bps(
                entry.direction,
                entry.entry_execution_price,
                [low, high],
            ),
        )
        position.highest_price = max(position.highest_price, float(candle.high))
        position.lowest_price = min(position.lowest_price, float(candle.low))
        if context is not None:
            pending_stop = plan_guardian_stop(policy_id, position, candle, context)

    return _episode(
        entry,
        policy_id=policy_id,
        exit_time_ms=None,
        exit_bar_index=None,
        exit_phase=None,
        exit_price=None,
        exit_atr=None,
        bars_held=max(0, len(candles) - entry.entry_index),
        censor_reason="end_of_data_before_terminal",
        stop_update_count=stop_updates,
        gap_through_slippage_bps=gap_slippage,
        same_bar_ambiguity_count=ambiguity_count,
        mfe_bps=mfe_bps,
        funding=funding,
        funding_authority_complete=funding_authority_complete,
        contract=contract,
    )


def _exit_order(episode: GuardianPolicyEpisode) -> tuple[int, int] | None:
    if episode.exit_bar_index is None or episode.exit_phase is None:
        return None
    phase_order = {
        GuardianExitPhase.OPEN: 0,
        GuardianExitPhase.INTRABAR: 1,
        GuardianExitPhase.CLOSE: 2,
    }
    return episode.exit_bar_index, phase_order[episode.exit_phase]


def is_premature_stop(
    candidate: GuardianPolicyEpisode,
    baseline: GuardianPolicyEpisode,
    *,
    candles: Sequence[Candle],
) -> bool:
    if not candidate.valid_primary or not baseline.valid_primary:
        return False
    candidate_order = _exit_order(candidate)
    baseline_order = _exit_order(baseline)
    if candidate_order is None or baseline_order is None:
        return False
    if (
        candidate.exit_bar_index == baseline.exit_bar_index
        and candidate.exit_phase is GuardianExitPhase.INTRABAR
        and baseline.exit_phase is GuardianExitPhase.INTRABAR
    ):
        return False
    if candidate_order >= baseline_order:
        return False
    if candidate.exit_price is None:
        return False
    if candidate.exit_phase in {GuardianExitPhase.OPEN, GuardianExitPhase.INTRABAR} and (
        candidate.exit_atr is None or candidate.exit_atr <= 0
    ):
        raise GuardianPolicyContractError(
            "valid candidate stop exit is missing the frozen candidate-exit ATR"
        )
    if candidate.exit_atr is None or candidate.exit_atr <= 0:
        return False
    assert candidate.exit_bar_index is not None
    assert baseline.exit_bar_index is not None
    start = candidate.exit_bar_index + 1
    stop = min(baseline.exit_bar_index, start + 12)
    for candle in candles[start:stop]:
        if candidate.direction is Direction.LONG:
            favorable = _decimal(candle.high) - candidate.exit_price
        else:
            favorable = candidate.exit_price - _decimal(candle.low)
        if favorable >= candidate.exit_atr:
            return True
    return False


def _mean(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    with localcontext(_DECIMAL_CONTEXT):
        return sum(values, Decimal("0")) / len(values)


def _cvar_5(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    count = ceil(Decimal("0.05") * len(values))
    return _mean(sorted(values)[:count])


def _maximum_drawdown(episodes: Sequence[GuardianPolicyEpisode]) -> Decimal | None:
    valid = [
        episode
        for episode in episodes
        if episode.valid_primary
        and episode.after_cost_return_bps is not None
        and episode.exit_time_ms is not None
    ]
    if not valid:
        return None
    symbols = sorted({episode.symbol for episode in valid})
    sleeves = {symbol: Decimal("1") for symbol in symbols}
    peak = Decimal("1")
    maximum = Decimal("0")
    ordered = sorted(valid, key=lambda item: (item.exit_time_ms or 0, item.position_id))
    with localcontext(_DECIMAL_CONTEXT):
        for episode in ordered:
            assert episode.after_cost_return_bps is not None
            sleeves[episode.symbol] *= max(
                Decimal("0"), Decimal("1") + episode.after_cost_return_bps / Decimal("10000")
            )
            equity = sum(sleeves.values(), Decimal("0")) / len(sleeves)
            if equity > peak:
                peak = equity
            drawdown = max(Decimal("0"), Decimal("1") - equity / peak)
            maximum = max(maximum, drawdown)
    return maximum


def _update_frequency(episodes: Sequence[GuardianPolicyEpisode]) -> Decimal | None:
    exposures = [episode.exposure_ms for episode in episodes]
    if any(value is None or value <= 0 for value in exposures):
        return None
    exposure_ms = sum(int(value) for value in exposures if value is not None)
    if exposure_ms <= 0:
        return None
    with localcontext(_DECIMAL_CONTEXT):
        return Decimal(sum(episode.stop_update_count for episode in episodes)) / (
            Decimal(exposure_ms) / Decimal(_ONE_DAY_MS)
        )


def compute_guardian_pair_metrics(
    entries: Sequence[GuardianEntryRow],
    episodes: Mapping[tuple[str, str], GuardianPolicyEpisode],
    *,
    challenger_policy_id: str,
    candles_by_symbol: Mapping[str, Sequence[Candle]],
) -> GuardianPairMetrics:
    if challenger_policy_id == _BASELINE_POLICY_ID or challenger_policy_id not in _POLICY_IDS:
        raise GuardianPolicyContractError("pair metrics require one frozen challenger policy")
    paired: list[tuple[GuardianEntryRow, GuardianPolicyEpisode, GuardianPolicyEpisode]] = []
    censored = 0
    for entry in entries:
        candidate = episodes.get((entry.position_id, challenger_policy_id))
        baseline = episodes.get((entry.position_id, _BASELINE_POLICY_ID))
        if (
            candidate is None
            or baseline is None
            or not candidate.valid_primary
            or not baseline.valid_primary
            or candidate.after_cost_return_bps is None
            or baseline.after_cost_return_bps is None
        ):
            censored += 1
            continue
        paired.append((entry, candidate, baseline))
    admitted = len(entries)
    with localcontext(_DECIMAL_CONTEXT):
        censor_fraction = Decimal(censored) / admitted if admitted else Decimal("1")
    candidate_episodes = [candidate for _, candidate, _ in paired]
    baseline_episodes = [baseline for _, _, baseline in paired]
    deltas = [
        candidate.after_cost_return_bps - baseline.after_cost_return_bps
        for _, candidate, baseline in paired
        if candidate.after_cost_return_bps is not None
        and baseline.after_cost_return_bps is not None
    ]
    candidate_returns = [
        episode.after_cost_return_bps
        for episode in candidate_episodes
        if episode.after_cost_return_bps is not None
    ]
    baseline_returns = [
        episode.after_cost_return_bps
        for episode in baseline_episodes
        if episode.after_cost_return_bps is not None
    ]
    candidate_givebacks = [
        episode.mfe_giveback_fraction
        for episode in candidate_episodes
        if episode.mfe_giveback_fraction is not None
    ]
    baseline_givebacks = [
        episode.mfe_giveback_fraction
        for episode in baseline_episodes
        if episode.mfe_giveback_fraction is not None
    ]
    premature_count = 0
    for entry, candidate, baseline in paired:
        candles = candles_by_symbol.get(entry.symbol)
        if candles is None:
            raise GuardianPolicyContractError(f"missing candle tape for {entry.symbol}")
        premature_count += int(is_premature_stop(candidate, baseline, candles=candles))
    with localcontext(_DECIMAL_CONTEXT):
        premature_rate = Decimal(premature_count) / len(paired) if paired else None
    direction_counts = Counter(entry.direction.value for entry, _, _ in paired)
    symbols_by_direction: dict[str, set[str]] = defaultdict(set)
    for entry, _, _ in paired:
        symbols_by_direction[entry.direction.value].add(entry.symbol)
    calendar_days = {entry.entry_time_ms // _ONE_DAY_MS for entry, _, _ in paired}
    trend_states = tuple(sorted({entry.trend_state for entry, _, _ in paired}))
    return GuardianPairMetrics(
        challenger_policy_id=challenger_policy_id,
        admitted_positions=admitted,
        paired_valid_positions=len(paired),
        censor_fraction=censor_fraction,
        mean_after_cost_delta_bps=_mean(deltas),
        candidate_maximum_drawdown=_maximum_drawdown(candidate_episodes),
        baseline_maximum_drawdown=_maximum_drawdown(baseline_episodes),
        candidate_cvar_5_return_bps=_cvar_5(candidate_returns),
        baseline_cvar_5_return_bps=_cvar_5(baseline_returns),
        candidate_premature_stop_rate=premature_rate,
        baseline_premature_stop_rate=Decimal("0") if paired else None,
        candidate_mean_mfe_giveback_fraction=_mean(candidate_givebacks),
        baseline_mean_mfe_giveback_fraction=_mean(baseline_givebacks),
        candidate_mean_stop_updates_per_24h_exposure=_update_frequency(candidate_episodes),
        baseline_mean_stop_updates_per_24h_exposure=_update_frequency(baseline_episodes),
        candidate_total_stop_update_count=sum(
            episode.stop_update_count for episode in candidate_episodes
        ),
        baseline_total_stop_update_count=sum(
            episode.stop_update_count for episode in baseline_episodes
        ),
        candidate_mean_gap_through_slippage_bps=_mean(
            [episode.gap_through_slippage_bps for episode in candidate_episodes]
        ),
        baseline_mean_gap_through_slippage_bps=_mean(
            [episode.gap_through_slippage_bps for episode in baseline_episodes]
        ),
        candidate_same_bar_ambiguity_count=sum(
            episode.same_bar_ambiguity_count for episode in candidate_episodes
        ),
        baseline_same_bar_ambiguity_count=sum(
            episode.same_bar_ambiguity_count for episode in baseline_episodes
        ),
        evidence_total_positions=len(paired),
        evidence_calendar_days=len(calendar_days),
        evidence_positions_by_direction=dict(sorted(direction_counts.items())),
        evidence_symbols_by_direction={
            direction: len(symbols) for direction, symbols in sorted(symbols_by_direction.items())
        },
        evidence_trend_states=trend_states,
    )


def _historical_stratum(entry: GuardianEntryRow, dimension: str) -> str:
    if dimension == "direction":
        return entry.direction.name
    if dimension == "regime":
        return (
            entry.regime
            if entry.regime in {"risk_on", "neutral", "risk_off"}
            else "stratum_unavailable"
        )
    if dimension == "time_of_day_utc":
        hour = (entry.entry_time_ms // 3_600_000) % 24
        if hour < 8:
            return "utc_00_08"
        if hour < 16:
            return "utc_08_16"
        return "utc_16_24"
    if dimension == "volatility_atr_percent":
        value = entry.entry_atr_percent
        if value is None or not value.is_finite():
            return "stratum_unavailable"
        if value < Decimal("0.50"):
            return "low_lt_0_50"
        if value < Decimal("1.00"):
            return "medium_0_50_to_1_00"
        return "high_ge_1_00"
    raise GuardianPolicyContractError(f"unsupported historical stratum dimension: {dimension}")


def compute_guardian_stratified_metrics(
    entries: Sequence[GuardianEntryRow],
    episodes: Mapping[tuple[str, str], GuardianPolicyEpisode],
    *,
    challenger_policy_id: str,
    candles_by_symbol: Mapping[str, Sequence[Candle]],
) -> list[GuardianStratumMetrics]:
    """Report preregistered strata on the unchanged paired-valid comparison population."""

    if challenger_policy_id == _BASELINE_POLICY_ID or challenger_policy_id not in _POLICY_IDS:
        raise GuardianPolicyContractError("stratified metrics require one frozen challenger policy")
    paired: list[tuple[GuardianEntryRow, GuardianPolicyEpisode, GuardianPolicyEpisode]] = []
    for entry in entries:
        candidate = episodes.get((entry.position_id, challenger_policy_id))
        baseline = episodes.get((entry.position_id, _BASELINE_POLICY_ID))
        if (
            candidate is None
            or baseline is None
            or not candidate.valid_primary
            or not baseline.valid_primary
            or candidate.after_cost_return_bps is None
            or baseline.after_cost_return_bps is None
        ):
            continue
        paired.append((entry, candidate, baseline))

    rows: list[GuardianStratumMetrics] = []
    for dimension in ("direction", "regime", "time_of_day_utc", "volatility_atr_percent"):
        labels = sorted({_historical_stratum(entry, dimension) for entry, _, _ in paired})
        for label in labels:
            subset = [row for row in paired if _historical_stratum(row[0], dimension) == label]
            candidates = [candidate for _, candidate, _ in subset]
            baselines = [baseline for _, _, baseline in subset]
            candidate_returns = [
                episode.after_cost_return_bps
                for episode in candidates
                if episode.after_cost_return_bps is not None
            ]
            baseline_returns = [
                episode.after_cost_return_bps
                for episode in baselines
                if episode.after_cost_return_bps is not None
            ]
            deltas = [
                candidate.after_cost_return_bps - baseline.after_cost_return_bps
                for _, candidate, baseline in subset
                if candidate.after_cost_return_bps is not None
                and baseline.after_cost_return_bps is not None
            ]
            candidate_givebacks = [
                episode.mfe_giveback_fraction
                for episode in candidates
                if episode.mfe_giveback_fraction is not None
            ]
            baseline_givebacks = [
                episode.mfe_giveback_fraction
                for episode in baselines
                if episode.mfe_giveback_fraction is not None
            ]
            premature = 0
            for entry, candidate, baseline in subset:
                candles = candles_by_symbol.get(entry.symbol)
                if candles is None:
                    raise GuardianPolicyContractError(f"missing candle tape for {entry.symbol}")
                premature += int(is_premature_stop(candidate, baseline, candles=candles))
            with localcontext(_DECIMAL_CONTEXT):
                premature_rate = Decimal(premature) / len(subset) if subset else None
            rows.append(
                GuardianStratumMetrics(
                    challenger_policy_id=challenger_policy_id,
                    dimension=dimension,
                    stratum=label,
                    paired_valid_positions=len(subset),
                    candidate_mean_after_cost_return_bps=_mean(candidate_returns),
                    baseline_mean_after_cost_return_bps=_mean(baseline_returns),
                    mean_after_cost_delta_bps=_mean(deltas),
                    candidate_premature_stop_rate=premature_rate,
                    candidate_mean_mfe_giveback_fraction=_mean(candidate_givebacks),
                    baseline_mean_mfe_giveback_fraction=_mean(baseline_givebacks),
                    candidate_mean_stop_updates_per_24h_exposure=_update_frequency(candidates),
                    baseline_mean_stop_updates_per_24h_exposure=_update_frequency(baselines),
                )
            )
    return rows


def bootstrap_draw_day_indices(
    *,
    calendar_day_count: int,
    seed: int,
    replicate_ordinal: int,
    block_days: int = 7,
) -> tuple[int, ...]:
    if calendar_day_count <= 0 or block_days <= 0:
        raise ValueError("bootstrap calendar and block size must be positive")
    if seed < 0 or replicate_ordinal < 0:
        raise ValueError("bootstrap seed and replicate ordinal must be non-negative")
    sampled: list[int] = []
    block_ordinal = 0
    while len(sampled) < calendar_day_count:
        payload = (
            f"GUARDIAN_POLICY_BOOTSTRAP_V1|{seed}|{replicate_ordinal}|{block_ordinal}"
        ).encode("ascii")
        start = int.from_bytes(hashlib.sha256(payload).digest()[:8], "big") % calendar_day_count
        sampled.extend((start + offset) % calendar_day_count for offset in range(block_days))
        block_ordinal += 1
    return tuple(sampled[:calendar_day_count])


def familywise_guardian_bootstrap(
    entries: Sequence[GuardianEntryRow],
    episodes: Mapping[tuple[str, str], GuardianPolicyEpisode],
    *,
    samples: int = 10_000,
    seed: int = 20_260_921,
    block_days: int = 7,
    minimum_valid_replicates: int = 8_000,
) -> dict[str, Any]:
    challengers = [policy for policy in _POLICY_IDS if policy != _BASELINE_POLICY_ID]
    if not entries:
        raise GuardianPolicyContractError("bootstrap requires admitted Guardian entries")
    first_day = min(entry.entry_time_ms // _ONE_DAY_MS for entry in entries)
    last_day = max(entry.entry_time_ms // _ONE_DAY_MS for entry in entries)
    day_count = last_day - first_day + 1
    paired_values: dict[str, list[tuple[int, Decimal]]] = {}
    point_estimates: dict[str, Decimal] = {}
    for challenger in challengers:
        rows: list[tuple[int, Decimal]] = []
        for entry in entries:
            candidate = episodes.get((entry.position_id, challenger))
            baseline = episodes.get((entry.position_id, _BASELINE_POLICY_ID))
            if (
                candidate is None
                or baseline is None
                or not candidate.valid_primary
                or not baseline.valid_primary
                or candidate.after_cost_return_bps is None
                or baseline.after_cost_return_bps is None
            ):
                continue
            day_index = entry.entry_time_ms // _ONE_DAY_MS - first_day
            rows.append(
                (day_index, candidate.after_cost_return_bps - baseline.after_cost_return_bps)
            )
        if not rows:
            raise GuardianPolicyContractError(f"bootstrap has no paired rows for {challenger}")
        paired_values[challenger] = rows
        point = _mean([value for _, value in rows])
        assert point is not None
        point_estimates[challenger] = point

    maximum_errors: list[Decimal] = []
    valid_replicates = 0
    for replicate in range(samples):
        draw = bootstrap_draw_day_indices(
            calendar_day_count=day_count,
            seed=seed,
            replicate_ordinal=replicate,
            block_days=block_days,
        )
        multiplicities = Counter(draw)
        errors: list[Decimal] = []
        globally_valid = True
        for challenger in challengers:
            weighted_sum = Decimal("0")
            weight = 0
            with localcontext(_DECIMAL_CONTEXT):
                for day_index, delta in paired_values[challenger]:
                    multiple = multiplicities.get(day_index, 0)
                    if multiple:
                        weighted_sum += delta * multiple
                        weight += multiple
                if weight == 0:
                    globally_valid = False
                    break
                replicate_mean = weighted_sum / weight
                errors.append(replicate_mean - point_estimates[challenger])
        if not globally_valid:
            continue
        valid_replicates += 1
        maximum_errors.append(max(errors))
    if valid_replicates < minimum_valid_replicates:
        raise GuardianPolicyContractError(
            f"bootstrap valid replicates {valid_replicates} < {minimum_valid_replicates}"
        )
    maximum_errors.sort()
    rank = ceil(Decimal("0.95") * valid_replicates) - 1
    critical = maximum_errors[rank]
    lower_bounds = {
        challenger: point_estimates[challenger] - critical for challenger in challengers
    }
    return {
        "method": "shared_circular_calendar_block_bootstrap",
        "calendar_first_utc_day": first_day,
        "calendar_last_utc_day": last_day,
        "calendar_day_count": day_count,
        "block_days": block_days,
        "samples": samples,
        "seed": seed,
        "valid_replicates": valid_replicates,
        "critical_95": critical,
        "point_estimates_bps": point_estimates,
        "simultaneous_lower_bps": lower_bounds,
    }


def _json_ready(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_json_ready(item) for item in value]
    return value


def _write_jsonl(
    path: Path,
    rows: Sequence[GuardianEntryRow | GuardianPolicyEpisode],
) -> None:
    path.write_text(
        "".join(
            json.dumps(_json_ready(asdict(row)), sort_keys=True, separators=(",", ":")) + "\n"
            for row in rows
        ),
        encoding="utf-8",
        newline="\n",
    )


def _build_context_tape(
    candles: Sequence[Candle],
    features: Sequence[FeatureSnapshot | None],
    *,
    momentum_rule_id: str,
) -> dict[int, GuardianBarContext]:
    if momentum_rule_id != _SUPPORTED_MOMENTUM_RULE_ID:
        raise GuardianPolicyContractError("historical momentum rule is not supported")
    contexts: dict[int, GuardianBarContext] = {}
    for candle, feature in zip(candles, features, strict=True):
        if feature is None:
            continue
        structure = feature.chart_structure
        contexts[candle.close_time_ms] = GuardianBarContext(
            candle_close_time_ms=candle.close_time_ms,
            atr=_decimal(feature.atr),
            confirmed_swing_support=(
                None if structure.latest_swing_low is None else _decimal(structure.latest_swing_low)
            ),
            confirmed_swing_resistance=(
                None
                if structure.latest_swing_high is None
                else _decimal(structure.latest_swing_high)
            ),
            momentum_weakened_long=_historical_momentum_weakened(feature, Direction.LONG),
            momentum_weakened_short=_historical_momentum_weakened(feature, Direction.SHORT),
        )
    return contexts


def run_guardian_policy_backtest(
    settings: Settings,
    contract_path: str | Path,
    data_dir: str | Path,
    output_dir: str | Path,
    *,
    workspace_root: str | Path,
    config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Run L60-03 only after every outcome-affecting historical input is preregistered."""

    workspace = Path(workspace_root)
    contract = load_guardian_policy_contract(contract_path, workspace_root=workspace)
    validate_guardian_historical_settings(
        contract,
        settings,
        config_path=config_path,
        workspace_root=workspace,
    )
    momentum_rule_id = _require_historical_momentum_rule(contract)
    spec = derive_guardian_historical_spec(contract, workspace_root=workspace)
    data_root = Path(data_dir)
    output_root = Path(output_dir)
    start_ms = int(spec.data_start.timestamp() * 1000)
    end_ms = int(spec.evaluation_end.timestamp() * 1000) - 1
    candles_by_asset: dict[str, list[Candle]] = {}
    funding_by_asset: dict[str, list[FundingRate]] = {}
    input_hashes: dict[str, str] = {}
    for asset in spec.assets:
        path = dataset_path(
            data_root, Market.FUTURES, asset.asset, asset.futures_symbol, spec.interval
        )
        request = KlineDatasetRequest(
            market=Market.FUTURES,
            symbol=asset.futures_symbol,
            alias=asset.asset,
            interval=spec.interval,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
        )
        verify_dataset_manifest(
            path,
            path.with_suffix(path.suffix + ".manifest.json"),
            expected_request=request,
        )
        dataset = read_kline_csv(path)
        candles_by_asset[asset.asset] = list(dataset.candles)
        input_hashes[path.relative_to(data_root).as_posix()] = build_dataset_manifest(path).sha256
        funding_file = funding_path(data_root, asset.asset, asset.futures_symbol, spec.interval)
        funding_dataset = verify_funding_dataset(
            funding_file,
            expected_symbol=asset.futures_symbol,
            expected_start_time_ms=start_ms,
            expected_end_time_ms=end_ms,
        )
        funding_by_asset[asset.asset] = list(funding_dataset.rates)
        input_hashes[funding_file.relative_to(data_root).as_posix()] = funding_sha256(funding_file)

    regimes_by_asset = build_market_regimes(candles_by_asset)
    backtester = ResearchBacktester(settings, spec)
    entries: list[GuardianEntryRow] = []
    episodes: dict[tuple[str, str], GuardianPolicyEpisode] = {}
    candles_by_symbol: dict[str, Sequence[Candle]] = {}
    for asset in spec.assets:
        candles = candles_by_asset[asset.asset]
        regimes = regimes_by_asset[asset.asset]
        funding = funding_by_asset[asset.asset]
        source_run = backtester.run_symbol(
            asset,
            Market.FUTURES,
            candles,
            regimes,
            funding,
        )
        flows = [OrderFlowSnapshot() for _ in candles]
        features = backtester._continuous_features(candles, flows, regimes)
        contexts = _build_context_tape(
            candles,
            features,
            momentum_rule_id=momentum_rule_id,
        )
        index_by_open = {candle.open_time_ms: index for index, candle in enumerate(candles)}
        candles_by_symbol[asset.futures_symbol] = candles
        for trade in source_run.trades:
            entry_index = index_by_open.get(trade.entry_time_ms)
            if entry_index is None or entry_index <= 0:
                raise GuardianPolicyContractError("source trade entry is not on the candle tape")
            decision_candle = candles[entry_index - 1]
            if decision_candle.close_time_ms != trade.entry_signal_time_ms:
                raise GuardianPolicyContractError(
                    "source entry signal is not the strict prior closed candle"
                )
            decision_feature = features[entry_index - 1]
            if decision_feature is None:
                raise GuardianPolicyContractError("source entry has no causal decision feature")
            protection_context = ProtectionContext.from_closed_candle(
                candle=decision_candle,
                feature=decision_feature,
                higher_timeframe_contexts={},
                source_decision_clock_id=f"guardian-historical:{trade.entry_signal_id}",
            )
            entry = freeze_guardian_entry_row(
                trade,
                entry_index=entry_index,
                notional_usdt=_decimal(spec.costs.notional_usdt),
                trend_state=protection_context.trend_state.value,
                entry_atr_percent=_decimal(decision_feature.atr_percent),
            )
            entries.append(entry)
            for policy_id in _POLICY_IDS:
                episode = replay_guardian_episode(
                    entry,
                    policy_id=policy_id,
                    candles=candles,
                    contexts_by_close=contexts,
                    funding=funding,
                    contract=contract,
                    funding_authority_complete=True,
                )
                episodes[(entry.position_id, policy_id)] = episode

    if len({entry.position_id for entry in entries}) != len(entries):
        raise GuardianPolicyContractError("frozen source entries contain duplicate position IDs")
    pair_metrics = [
        compute_guardian_pair_metrics(
            entries,
            episodes,
            challenger_policy_id=policy_id,
            candles_by_symbol=candles_by_symbol,
        )
        for policy_id in _POLICY_IDS
        if policy_id != _BASELINE_POLICY_ID
    ]
    stratified_metrics = [
        metric
        for policy_id in _POLICY_IDS
        if policy_id != _BASELINE_POLICY_ID
        for metric in compute_guardian_stratified_metrics(
            entries,
            episodes,
            challenger_policy_id=policy_id,
            candles_by_symbol=candles_by_symbol,
        )
    ]
    bootstrap = familywise_guardian_bootstrap(entries, episodes)
    output_root.mkdir(parents=True, exist_ok=True)
    entry_path = output_root / "entry_rows.jsonl"
    episode_path = output_root / "policy_episodes.jsonl"
    metrics_path = output_root / "historical_metrics.json"
    manifest_path = output_root / "run_manifest.json"
    report_path = output_root / "report.md"
    _write_jsonl(entry_path, entries)
    episode_rows: list[GuardianPolicyEpisode] = sorted(
        list(episodes.values()), key=lambda row: (row.position_id, row.policy_id)
    )
    _write_jsonl(
        episode_path,
        episode_rows,
    )
    metrics_payload = {
        "contract_sha256": FROZEN_L60_02_V2_CONTRACT_SHA256,
        "parent_contract_sha256": FROZEN_L60_02_PARENT_CONTRACT_SHA256,
        "population": "historical_harness",
        "pair_metrics": [asdict(metric) for metric in pair_metrics],
        "stratified_metrics": [asdict(metric) for metric in stratified_metrics],
        "bootstrap": bootstrap,
        "selection_performed": False,
    }
    metrics_path.write_text(
        json.dumps(_json_ready(metrics_payload), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    output_hashes = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (entry_path, episode_path, metrics_path)
    }
    manifest = {
        "schema_version": "guardian_policy_backtest_manifest_v1",
        "contract_sha256": FROZEN_L60_02_V2_CONTRACT_SHA256,
        "parent_contract_sha256": FROZEN_L60_02_PARENT_CONTRACT_SHA256,
        "source_spec": contract["cohort_authority"]["historical_harness"],
        "config_path": contract["cohort_authority"]["historical_harness"]["settings_path"],
        "effective_settings_sha256": contract["cohort_authority"]["historical_harness"][
            "effective_settings_sha256"
        ],
        "historical_entry_producer_authority": contract["historical_entry_producer_authority"],
        "guardian_policy_source_sha256": _canonical_text_sha256(
            workspace / "src" / "signalbot" / "backtest" / "guardian_policy.py"
        ),
        "inputs": dict(sorted(input_hashes.items())),
        "outputs": output_hashes,
        "selection_performed": False,
    }
    manifest_path.write_text(
        json.dumps(_json_ready(manifest), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    report_path.write_text(
        "# Guardian policy historical evaluation\n\n"
        f"- Contract SHA-256: `{FROZEN_L60_02_V2_CONTRACT_SHA256}`\n"
        f"- Frozen entries: {len(entries)}\n"
        f"- Policy episodes: {len(episodes)}\n"
        f"- Report-only stratum rows: {len(stratified_metrics)}\n"
        "- Selection performed: no\n"
        "- This L60-03 artifact is evidence only; promotion belongs to L60-08.\n",
        encoding="utf-8",
        newline="\n",
    )
    return {
        "status": "L60_03_HISTORICAL_EVIDENCE_WRITTEN",
        "contract_sha256": FROZEN_L60_02_V2_CONTRACT_SHA256,
        "entries": len(entries),
        "episodes": len(episodes),
        "output_dir": str(output_root),
    }
