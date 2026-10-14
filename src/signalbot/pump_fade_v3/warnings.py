"""UNVALIDATED, offline L7 warning decisions; intentionally no delivery adapters."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from signalbot.pump_fade_v3.position_tracker import Episode, RecordSink, Tranche

WarningId = Literal[
    "THESIS-BROKEN",
    "ADD-BLOCKED",
    "ADD-DETECTED",
    "LIQ-PROXIMITY",
    "ADVERSE-CONTINUATION",
    "RUNNER-HOLD",
    "RUNNER-EXIT",
]

_GROUPS = {
    "T1": "price",
    "T2": "price",
    "T3": "price",
    "T4": "oi",
    "T5": "liquidation_flow",
    "T6": "funding",
}


@dataclass(frozen=True)
class WarningPolicy:
    policy_hash: str
    liq_thresholds_percent: tuple[Decimal, ...]
    max_adds: int
    legacy_adds: int
    max_add_fraction_q0: Decimal
    legacy_spacing_percent: Decimal
    reserve_rate: Decimal
    liq_rearm_margin_percent: Decimal = Decimal(2)
    oi_expansion_rate: Decimal = Decimal("0.10")
    funding_cap_near: Decimal = Decimal("0.0001")
    add_reason_step_limit_per_candle: int = 1

    def __post_init__(self) -> None:
        if not self.policy_hash:
            raise ValueError("policy_hash is required")
        if tuple(sorted(self.liq_thresholds_percent, reverse=True)) != self.liq_thresholds_percent:
            raise ValueError("liquidation thresholds must descend")
        if any(threshold <= 0 for threshold in self.liq_thresholds_percent):
            raise ValueError("liquidation thresholds must be positive")
        if self.max_adds < 0 or self.legacy_adds < 0:
            raise ValueError("add limits must be non-negative")
        if self.liq_rearm_margin_percent < 0:
            raise ValueError("liquidation rearm margin must be non-negative")
        if self.oi_expansion_rate < 0 or self.funding_cap_near < 0:
            raise ValueError("live-only trigger thresholds must be non-negative")


def load_warning_policy(policy_path: Path) -> WarningPolicy:
    raw_bytes = policy_path.read_bytes()
    policy = json.loads(raw_bytes)
    if not isinstance(policy, dict):
        raise ValueError("policy must be an object")
    ladder = policy.get("ladder")
    replay = policy.get("replay")
    wait = policy.get("wait")
    release = policy.get("release")
    funding = release.get("R5") if isinstance(release, dict) else None
    if (
        not isinstance(ladder, dict)
        or not isinstance(replay, dict)
        or not isinstance(wait, dict)
        or not isinstance(funding, dict)
    ):
        raise ValueError("policy ladder, wait, release, and replay sections are required")
    distances = replay.get("liquidation_distance_percent")
    if not isinstance(distances, list) or not distances:
        raise ValueError("liquidation distance policy must be a non-empty array")
    return WarningPolicy(
        policy_hash=hashlib.sha256(raw_bytes).hexdigest(),
        liq_thresholds_percent=tuple(
            sorted((Decimal(str(value)) for value in distances), reverse=True)
        ),
        max_adds=int(ladder["primary_max_adds"]),
        legacy_adds=int(ladder["legacy_adds"]),
        max_add_fraction_q0=Decimal(str(ladder["primary_max_each_add_q0"])),
        legacy_spacing_percent=Decimal(str(ladder["legacy_add_spacing_percent"])) / Decimal(100),
        reserve_rate=Decimal(str(ladder["risk_cost_reserve_roundtrip_bps"])) / Decimal(10_000),
        oi_expansion_rate=(
            Decimal(str(wait["continuation_risk_oi_increase_gte_percent"])) / Decimal(100)
        ),
        funding_cap_near=Decimal(str(funding["cap_near_distance"])),
    )


@dataclass(frozen=True)
class ThesisInputs:
    triggers: frozenset[str]
    source_values: dict[str, object]
    unavailable_reasons: tuple[str, ...] = ()


@dataclass(frozen=True)
class ThesisEvidence:
    now_ms: int
    invalidation: Decimal | None
    closed_mark_high: Decimal | None
    lagged_oi: Decimal | None
    oi_baseline: Decimal | None
    oi_usable_at_ms: int | None
    forced_buy_times_ms: tuple[int, ...]
    funding_rate: Decimal | None
    funding_cap: Decimal | None
    funding_interval_shortened: bool


def evaluate_available_thesis(evidence: ThesisEvidence, policy: WarningPolicy) -> ThesisInputs:
    if evidence.now_ms < 0:
        raise ValueError("thesis clock must be non-negative")
    triggers: set[str] = set()
    unavailable: list[str] = []
    if evidence.invalidation is not None and evidence.closed_mark_high is not None:
        if evidence.closed_mark_high >= evidence.invalidation:
            triggers.add("T1")
    else:
        unavailable.append("T1_INVALIDATION_OR_CLOSED_MARK_HIGH_MISSING")
    if (
        evidence.lagged_oi is not None
        and evidence.oi_baseline is not None
        and evidence.oi_baseline > 0
        and evidence.oi_usable_at_ms is not None
        and evidence.oi_usable_at_ms <= evidence.now_ms
        and evidence.now_ms - evidence.oi_usable_at_ms <= 15 * 60_000
    ):
        if evidence.lagged_oi >= evidence.oi_baseline * (Decimal(1) + policy.oi_expansion_rate):
            triggers.add("T4")
    else:
        unavailable.append("T4_LAGGED_OI_OR_BASELINE_MISSING_OR_STALE")
    if any(
        evidence.now_ms - 300_000 < time_ms <= evidence.now_ms
        for time_ms in evidence.forced_buy_times_ms
    ):
        triggers.add("T5")
    if evidence.funding_interval_shortened:
        triggers.add("T6")
    elif evidence.funding_rate is not None and evidence.funding_cap is not None:
        if evidence.funding_rate >= evidence.funding_cap - policy.funding_cap_near:
            triggers.add("T6")
    else:
        unavailable.append("T6_FUNDING_CAP_UNKNOWN")
    sources = {
        "closed_mark_high": str(evidence.closed_mark_high)
        if evidence.closed_mark_high is not None
        else None,
        "invalidation": str(evidence.invalidation) if evidence.invalidation is not None else None,
        "lagged_oi": str(evidence.lagged_oi) if evidence.lagged_oi is not None else None,
        "oi_baseline": str(evidence.oi_baseline) if evidence.oi_baseline is not None else None,
        "oi_usable_at_ms": evidence.oi_usable_at_ms,
        "forced_buy_times_ms": list(evidence.forced_buy_times_ms),
        "funding_rate": str(evidence.funding_rate) if evidence.funding_rate is not None else None,
        "funding_cap": str(evidence.funding_cap) if evidence.funding_cap is not None else None,
        "funding_interval_shortened": evidence.funding_interval_shortened,
    }
    return ThesisInputs(frozenset(triggers), sources, tuple(unavailable))


@dataclass(frozen=True)
class AddBlockInputs:
    closed_mark: Decimal
    closed_candle_ms: int
    rule_state: Literal["WAIT", "SKIP", "CONT", "EXPIRED", "UNAVAILABLE", "RELEASED"]
    confirmed: bool
    next_quantity: Decimal
    r_live: Decimal | None


@dataclass
class _EpisodeWarningState:
    thesis_groups: frozenset[str] = frozenset()
    thesis_active: bool = False
    thesis_cleared_at: int | None = None
    liq_armed: dict[Decimal, bool] = field(default_factory=dict)
    add_reasons: frozenset[str] | None = None
    add_last_candle_ms: int | None = None
    add_price_stages: set[tuple[str, int]] = field(default_factory=set)
    state_record_values: dict[str, str] = field(default_factory=dict)


class WarningEngine:
    """Pure-data decision engine. Records are stored locally, never sent to Discord."""

    def __init__(
        self, policy: WarningPolicy, sink: RecordSink, *, state_capacity: int = 4096
    ) -> None:
        if state_capacity < 1:
            raise ValueError("state_capacity must be positive")
        self._policy = policy
        self._sink = sink
        self._states: dict[str, _EpisodeWarningState] = {}
        self._state_capacity = state_capacity
        self._health: Literal["HEALTHY", "DEGRADED"] = "DEGRADED"
        self._degraded_reason: str | None = "ACCOUNT_MODE_UNKNOWN"
        self._account_mode_supported = False
        self._missed_during_degraded = 0

    def mark_degraded(
        self, reason: str, occurred_ms: int, episode: Episode | None = None
    ) -> dict[str, object]:
        if not reason:
            raise ValueError("degraded reason must be non-empty")
        self._health = "DEGRADED"
        self._degraded_reason = reason
        return self._state_record(
            "HEALTH", episode, occurred_ms, {"state": "DEGRADED", "reason": reason}
        )

    def mark_recovered(self, occurred_ms: int, episode: Episode | None = None) -> dict[str, object]:
        details = {
            "state": "RECOVERED",
            "previous_reason": self._degraded_reason,
            "missed_during_degraded": self._missed_during_degraded,
        }
        self._health = "HEALTHY"
        self._degraded_reason = None
        self._missed_during_degraded = 0
        return self._state_record("HEALTH", episode, occurred_ms, details)

    def observe_account_config(
        self,
        config: dict[str, object],
        occurred_ms: int,
        *,
        portfolio_margin: bool | None,
        episode: Episode | None = None,
    ) -> bool:
        dual_side = config.get("dualSidePosition")
        multi_assets = config.get("multiAssetsMargin")
        supported = (
            isinstance(dual_side, bool)
            and isinstance(multi_assets, bool)
            and dual_side is False
            and multi_assets is False
            and portfolio_margin is False
        )
        if not supported:
            self._account_mode_supported = False
            reason = (
                "UNSUPPORTED_ACCOUNT_MODE"
                if dual_side is True or multi_assets is True or portfolio_margin is True
                else "ACCOUNT_MODE_UNKNOWN"
            )
            self.mark_degraded(reason, occurred_ms, episode)
            return False
        self._account_mode_supported = True
        if self._degraded_reason in {"ACCOUNT_MODE_UNKNOWN", "UNSUPPORTED_ACCOUNT_MODE"}:
            self.mark_recovered(occurred_ms, episode)
        return True

    def position_status(
        self,
        episode: Episode,
        occurred_ms: int,
        *,
        unrealized_pnl: Decimal | None,
        break_even_price: Decimal | None,
        cumulative_funding: Decimal | None,
        next_funding_ms: int | None,
        funding_rate: Decimal | None,
        liquidation_distance_percent: Decimal | None,
        remaining_budget_r: Decimal | None,
        adl: int | None,
    ) -> dict[str, object]:
        return self._state_record(
            "POSITION-STATUS",
            episode,
            occurred_ms,
            {
                "unrealized_pnl": str(unrealized_pnl) if unrealized_pnl is not None else None,
                "break_even_price": str(break_even_price) if break_even_price is not None else None,
                "cumulative_funding": str(cumulative_funding)
                if cumulative_funding is not None
                else None,
                "next_funding_ms": next_funding_ms,
                "funding_rate": str(funding_rate) if funding_rate is not None else None,
                "liquidation_distance_percent": str(liquidation_distance_percent)
                if liquidation_distance_percent is not None
                else None,
                "remaining_budget_r": str(remaining_budget_r)
                if remaining_budget_r is not None
                else None,
                "rule_state": "UNKNOWN",
                "adl": adl,
            },
        )

    def thesis_broken(
        self, episode: Episode, inputs: ThesisInputs, occurred_ms: int
    ) -> dict[str, object] | None:
        if not self._can_warn():
            return None
        if episode.side != "SHORT":
            return self._transition_state_record(
                "THESIS_NOT_APPLICABLE",
                episode,
                occurred_ms,
                {},
                state_key="thesis_applicability",
                state_value="NOT_APPLICABLE",
            )
        triggers = frozenset(trigger for trigger in inputs.triggers if trigger in _GROUPS)
        groups = frozenset(_GROUPS[trigger] for trigger in triggers)
        validated_groups = frozenset(_GROUPS[t] for t in triggers if t in {"T1", "T2", "T3", "T4"})
        issuable = "T1" in triggers or (
            len(groups) >= 2 and bool(validated_groups) and ("price" in groups or "oi" in groups)
        )
        state = self._state(episode)
        if not issuable:
            if state.thesis_active and not triggers:
                if state.thesis_cleared_at is None:
                    state.thesis_cleared_at = occurred_ms
                elif occurred_ms - state.thesis_cleared_at >= 3_600_000:
                    state.thesis_active = False
                    state.thesis_groups = frozenset()
                    return self._state_record(
                        "THESIS_RECHECK",
                        episode,
                        occurred_ms,
                        {"triggers": [], "unavailable_reasons": list(inputs.unavailable_reasons)},
                    )
            elif triggers:
                state.thesis_cleared_at = None
            return None
        state.thesis_cleared_at = None
        if state.thesis_active and groups <= state.thesis_groups:
            return None
        state.thesis_active = True
        state.thesis_groups = groups
        return self._warning(
            "THESIS-BROKEN",
            episode,
            occurred_ms,
            {
                "triggers": sorted(triggers),
                "independent_group_count": len(groups),
                "validated_group_count": len(validated_groups),
                "live_only_trigger": bool(triggers & {"T5", "T6"}),
                "source_values": inputs.source_values,
                "unavailable_reasons": list(inputs.unavailable_reasons),
                "l8_a3_qualifies": "T1" in triggers or len(validated_groups) >= 2,
            },
            invalidation="All active triggers false for 60 minutes; recheck before reissue.",
        )

    def liquidation_proximity(
        self,
        episode: Episode,
        *,
        liquidation_price: Decimal | None,
        snapshot_received_ms: int,
        mark: Decimal | None,
        mark_update_ms: int,
        now_ms: int,
        account_supported: bool,
        cross_margin: bool,
        margin_call: bool = False,
    ) -> dict[str, object] | None:
        """Evaluate LIQ freshness using local REST response receipt time.

        `snapshot_received_ms` is the client clock when the positionRisk response
        arrived; Binance position `updateTime` is a position-change time and must
        not be used as the snapshot freshness clock.
        """
        if not self._can_warn():
            return None
        if (
            liquidation_price is None
            or liquidation_price <= 0
            or mark is None
            or mark <= 0
            or snapshot_received_ms > now_ms
            or mark_update_ms > now_ms
            or now_ms - snapshot_received_ms > 15_000
            or now_ms - mark_update_ms > 5_000
            or not account_supported
        ):
            return self._transition_state_record(
                "LIQ_PROXIMITY_UNKNOWN",
                episode,
                now_ms,
                {"reason": "STALE_OR_UNAVAILABLE_SOURCE"},
                state_key="liq_proximity",
                state_value="UNKNOWN:STALE_OR_UNAVAILABLE_SOURCE",
            )
        sign = Decimal(-1) if episode.side == "SHORT" else Decimal(1)
        distance = sign * (mark - liquidation_price) / mark * Decimal(100)
        if distance < 0:
            return self._transition_state_record(
                "LIQ_PROXIMITY_UNKNOWN",
                episode,
                now_ms,
                {"reason": "LIQUIDATION_PRICE_NOT_ADVERSE", "distance_percent": str(distance)},
                state_key="liq_proximity",
                state_value="UNKNOWN:LIQUIDATION_PRICE_NOT_ADVERSE",
            )
        state = self._state(episode)
        state.state_record_values.pop("liq_proximity", None)
        for threshold in self._policy.liq_thresholds_percent:
            if distance >= threshold + self._policy.liq_rearm_margin_percent:
                state.liq_armed[threshold] = True
        armed_thresholds = [
            threshold
            for threshold in self._policy.liq_thresholds_percent
            if distance <= threshold and state.liq_armed.get(threshold, True)
        ]
        if armed_thresholds:
            threshold = min(armed_thresholds)
            for armed_threshold in self._policy.liq_thresholds_percent:
                if armed_threshold >= threshold:
                    state.liq_armed[armed_threshold] = False
            return self._warning(
                "LIQ-PROXIMITY",
                episode,
                now_ms,
                {
                    "distance_percent": str(distance),
                    "threshold_percent": str(threshold),
                    "liq_basis": "CROSS_ACCOUNT_DEPENDENT" if cross_margin else "ISOLATED",
                    "l8_a3_liq_condition": distance <= self._policy.liq_thresholds_percent[-1],
                    "margin_call": margin_call,
                    "source_values": {
                        "liquidation_price": str(liquidation_price),
                        "snapshot_received_ms": snapshot_received_ms,
                        "mark": str(mark),
                        "mark_update_ms": mark_update_ms,
                    },
                },
                invalidation=(
                    f"Distance recovers above "
                    f"{threshold + self._policy.liq_rearm_margin_percent}%."
                ),
            )
        return None

    def add_blocked(self, episode: Episode, inputs: AddBlockInputs) -> dict[str, object] | None:
        if not self._can_warn():
            return None
        if episode.side != "SHORT":
            return self._transition_state_record(
                "ADD_NOT_APPLICABLE",
                episode,
                inputs.closed_candle_ms,
                {},
                state_key="add_applicability",
                state_value="NOT_APPLICABLE",
            )
        invalidation = episode.invalidation
        reasons: set[str] = set()
        if inputs.rule_state != "RELEASED":
            reasons.add(f"RULE_STATE_{inputs.rule_state}")
        if not inputs.confirmed:
            reasons.add("NO_UPDATED_CONFIRMATION")
        if episode.add_count >= self._policy.max_adds:
            reasons.add("PRIMARY_ADD_LIMIT")
        if episode.total_fill_count >= self._policy.legacy_adds:
            reasons.add("LEGACY_FILL_LIMIT_REACHED")
        if inputs.next_quantity > self._policy.max_add_fraction_q0 * episode.q0:
            reasons.add("ADD_SIZE_LIMIT")
        if invalidation is None:
            reasons.add("INVALIDATION_UNKNOWN")
        elif inputs.closed_mark >= invalidation:
            reasons.add("PRICE_AT_OR_ABOVE_INVALIDATION")
        current_risk = _position_risk(episode, invalidation, self._policy.reserve_rate)
        proposed_risk = (
            inputs.next_quantity
            * (
                abs(invalidation - inputs.closed_mark)
                + self._policy.reserve_rate * inputs.closed_mark
            )
            if invalidation is not None
            else None
        )
        if inputs.r_live is None or current_risk is None or proposed_risk is None:
            reasons.add("RISK_BUDGET_UNKNOWN")
        elif current_risk + proposed_risk > inputs.r_live:
            reasons.add("RISK_BUDGET_EXCEEDED")
        reason_set = frozenset(reasons)
        if not reason_set:
            self._state(episode).add_reasons = reason_set
            return None
        state = self._state(episode)
        previous = state.add_reasons
        candle_fresh = state.add_last_candle_ms != inputs.closed_candle_ms
        changed = previous is None or reason_set != previous
        stage = _price_stage(
            episode.last_entry_price, inputs.closed_mark, self._policy.legacy_spacing_percent
        )
        stage_key = (
            str(episode.last_entry_price) if episode.last_entry_price is not None else "UNKNOWN",
            stage,
        )
        staged = stage > 0 and stage_key not in state.add_price_stages
        if not ((changed and candle_fresh) or staged):
            state.add_reasons = reason_set
            return None
        state.add_reasons = reason_set
        state.add_last_candle_ms = inputs.closed_candle_ms
        if staged:
            state.add_price_stages.add(stage_key)
        remaining_r = (
            (inputs.r_live - current_risk) / inputs.r_live
            if inputs.r_live is not None and current_risk is not None and inputs.r_live > 0
            else None
        )
        return self._warning(
            "ADD-BLOCKED",
            episode,
            inputs.closed_candle_ms,
            {
                "reasons": sorted(reason_set),
                "remaining_budget_r": str(remaining_r) if remaining_r is not None else None,
                "next_price_stage": stage if staged else None,
                "rule_state": inputs.rule_state,
                "price_source": "CLOSED_5M_MARK_CLOSE",
                "source_values": {
                    "closed_mark": str(inputs.closed_mark),
                    "candle_close_ms": inputs.closed_candle_ms,
                },
            },
            invalidation="Next fully closed 5m candle or all blocking reasons clear.",
            expires_ms=inputs.closed_candle_ms + 300_000,
        )

    def add_detected(
        self,
        episode: Episode,
        occurred_ms: int,
        *,
        stage: Literal["PARTIAL", "FINAL"],
        tranche: Tranche | None = None,
        break_even_price: Decimal | None = None,
    ) -> dict[str, object] | None:
        if not self._can_warn() or episode.add_count == 0:
            return None
        live_r = _live_risk_budget(episode, self._policy.reserve_rate)
        total_risk = _position_risk(episode, episode.invalidation, self._policy.reserve_rate)
        remaining_budget = (
            live_r - total_risk if live_r is not None and total_risk is not None else None
        )
        return self._warning(
            "ADD-DETECTED",
            episode,
            occurred_ms,
            {
                "stage": stage,
                "add_number": episode.add_count,
                "total_fills": episode.total_fill_count,
                "quantity": str(tranche.quantity) if tranche is not None else None,
                "average_price": (
                    str(tranche.average_price)
                    if tranche is not None and tranche.average_price is not None
                    else None
                ),
                "q0": str(episode.q0),
                "invalidation": str(episode.invalidation)
                if episode.invalidation is not None
                else None,
                "rules_flag": "UNKNOWN",
                "ledger_quality": episode.ledger_quality,
                "policy_max_adds": self._policy.max_adds,
                "legacy_adds": self._policy.legacy_adds,
                "total_risk": str(total_risk) if total_risk is not None else None,
                "r_live": str(live_r) if live_r is not None else None,
                "remaining_budget": str(remaining_budget) if remaining_budget is not None else None,
                "weighted_average_entry": (
                    str(episode.average_entry_price)
                    if episode.average_entry_price is not None
                    else None
                ),
                "break_even_price": (
                    str(break_even_price) if break_even_price is not None else None
                ),
                "plan_status": "PROVISIONAL" if stage == "PARTIAL" else "FROZEN",
                "compliance": {
                    "state": "UNKNOWN",
                    "confirmation": "UNKNOWN",
                    "size": (
                        "UNKNOWN"
                        if tranche is None or episode.q0 <= 0
                        else tranche.quantity <= self._policy.max_add_fraction_q0 * episode.q0
                    ),
                    "count": episode.add_count <= self._policy.max_adds,
                    "budget": (
                        "UNKNOWN" if total_risk is None or live_r is None else total_risk <= live_r
                    ),
                    "price_below_invalidation": (
                        "UNKNOWN"
                        if episode.invalidation is None
                        else episode.last_entry_price is not None
                        and episode.last_entry_price < episode.invalidation
                    ),
                },
            },
            invalidation="Informational fill record; it does not block an exchange action.",
        )

    def _warning(
        self,
        warning_id: WarningId,
        episode: Episode,
        occurred_ms: int,
        details: dict[str, object],
        *,
        invalidation: str,
        expires_ms: int | None = None,
    ) -> dict[str, object]:
        record = {
            "warning_id": warning_id,
            "episode_id": episode.episode_id,
            "symbol": episode.symbol,
            "side": episode.side,
            "occurred_ms": occurred_ms,
            "occurred_utc": datetime.fromtimestamp(occurred_ms / 1000, UTC).isoformat(),
            "occurred_seoul": datetime.fromtimestamp(occurred_ms / 1000, UTC)
            .astimezone(ZoneInfo("Asia/Seoul"))
            .isoformat(),
            "reasons": details.get("reasons")
            or details.get("triggers")
            or [f"{warning_id} condition observed"],
            "details": details,
            "invalidation": invalidation,
            "expires_ms": expires_ms,
            "rule_version": "L7-G1-UNAVAILABLE",
            "validity": "UNVALIDATED",
            "policy_hash": self._policy.policy_hash,
            "data_health": "HEALTHY",
        }
        record["record_id"] = hashlib.sha256(_stable_bytes(record)).hexdigest()
        self._sink.append(record)
        return record

    def _state_record(
        self, event_type: str, episode: Episode | None, occurred_ms: int, details: dict[str, object]
    ) -> dict[str, object]:
        record = {
            "event_type": event_type,
            "episode_id": episode.episode_id if episode is not None else None,
            "symbol": episode.symbol if episode is not None else None,
            "occurred_ms": occurred_ms,
            "occurred_utc": datetime.fromtimestamp(occurred_ms / 1000, UTC).isoformat(),
            "occurred_seoul": datetime.fromtimestamp(occurred_ms / 1000, UTC)
            .astimezone(ZoneInfo("Asia/Seoul"))
            .isoformat(),
            "details": details,
            "policy_hash": self._policy.policy_hash,
        }
        record["record_id"] = hashlib.sha256(_stable_bytes(record)).hexdigest()
        self._sink.append(record)
        return record

    def _transition_state_record(
        self,
        event_type: str,
        episode: Episode,
        occurred_ms: int,
        details: dict[str, object],
        *,
        state_key: str,
        state_value: str,
    ) -> dict[str, object] | None:
        state = self._state(episode)
        if state.state_record_values.get(state_key) == state_value:
            return None
        state.state_record_values[state_key] = state_value
        return self._state_record(event_type, episode, occurred_ms, details)

    def _state(self, episode: Episode) -> _EpisodeWarningState:
        state = self._states.get(episode.episode_id)
        if state is None:
            if len(self._states) >= self._state_capacity:
                self._states.pop(next(iter(self._states)))
            state = _EpisodeWarningState()
            self._states[episode.episode_id] = state
        return state

    def _can_warn(self) -> bool:
        if self._health == "DEGRADED" or not self._account_mode_supported:
            self._missed_during_degraded += 1
            return False
        return True


def _position_risk(
    episode: Episode, invalidation: Decimal | None, reserve_rate: Decimal
) -> Decimal | None:
    if invalidation is None:
        return None
    result = Decimal(0)
    for tranche in episode.tranches.values():
        if tranche.is_entry and tranche.average_price is not None:
            result += tranche.quantity * (
                abs(invalidation - tranche.average_price) + reserve_rate * tranche.average_price
            )
    return result


def _live_risk_budget(episode: Episode, reserve_rate: Decimal) -> Decimal | None:
    if episode.invalidation is None or episode.p0 is None:
        return None
    return (
        Decimal(2)
        * episode.q0
        * (abs(episode.invalidation - episode.p0) + reserve_rate * episode.p0)
    )


def _price_stage(last_entry: Decimal | None, price: Decimal, spacing: Decimal) -> int:
    if (
        last_entry is None
        or last_entry <= 0
        or spacing <= 0
        or price < last_entry * (Decimal(1) + spacing)
    ):
        return 0
    return int(((price / last_entry) - 1) / spacing)


def _stable_bytes(record: dict[str, object]) -> bytes:
    return json.dumps(record, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
