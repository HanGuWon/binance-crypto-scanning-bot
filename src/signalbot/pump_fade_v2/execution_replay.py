"""Deterministic, offline comparison of executable ladder scenarios.

Inputs are caller-supplied, receipt-stamped quote paths and add decisions. This
module does not fetch data, inspect private exports, or place orders.

Funding accounting (counterfactual, per arm)
--------------------------------------------
Funding is never copied between arms and is never taken from an unbound account
cash amount. Each arm's funding is recomputed from the arm's own open position at
every scheduled settlement::

    cash = quantity_open * mark_price * rate        (received by shorts when rate > 0)

with ``quantity_open`` the sum of that arm's accepted fills whose ``at_ms`` is
strictly before the settlement, and zero when the arm's simulated exit is strictly
before the settlement. Equality convention (declared before outcomes were inspected
and identical to ``labels.label_outcome``'s ``origin < settlement <= expiry``): a
fill at exactly the settlement instant is not yet exposed, and a position exiting at
exactly the settlement instant still settles. Funding is available for an arm only
if the verified coverage interval spans [first fill, exit] and every scheduled
settlement the arm was exposed to has an observed rate and valuation; otherwise the
arm's funding and net result are ``None`` (never zero).
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from enum import StrEnum
from itertools import pairwise

from signalbot.pump_fade_v2.ladder import Fill, FrozenLadder, Side, assess_ladder
from signalbot.pump_fade_v2.state import State

BPS = 10_000.0
PRIMARY_MAX_ADDITIONS = 2
SENSITIVITY_MAX_ADDITIONS = 3
LEGACY_MAX_ADDITIONS = 8
PRIMARY_MAX_TRANCHE_FRACTION = 0.5
LEGACY_ADVERSE_SPACING_PCT = 5.0
DEFAULT_ROUND_TRIP_COST_BPS = 30.0
BOOTSTRAP_REPLICATES = 10_000
BOOTSTRAP_SEED = 20261007
NONINFERIORITY_MARGIN_BPS = 10.0
MIN_CLUSTERED_PAIRED_PARENTS = 100
MIN_CLUSTERED_UTC_DAYS = 20
FUNDING_MAX_ABS_RATE = 1.0
FUNDING_VERIFIED = "VERIFIED"
FUNDING_UNAVAILABLE_NO_COVERAGE = "UNAVAILABLE_NO_VERIFIED_COVERAGE_FOR_HOLDING_WINDOW"
FUNDING_UNAVAILABLE_MISSING_SETTLEMENT = "UNAVAILABLE_EXPOSED_SETTLEMENT_WITHOUT_RATE_AND_VALUATION"
FUNDING_NOT_APPLICABLE = "NOT_APPLICABLE_NO_EXECUTABLE_EXIT"


class ExposureClass(StrEnum):
    PUBLIC_SYNTHETIC = "PUBLIC_SYNTHETIC"
    PRIVATE_IN_SAMPLE = "PRIVATE_IN_SAMPLE"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class ExecutableQuote:
    at_ms: int
    received_ms: int
    bid: float
    ask: float

    def __post_init__(self) -> None:
        if (self.at_ms < 0 or self.received_ms < self.at_ms or self.bid <= 0
                or self.ask < self.bid or not math.isfinite(self.bid + self.ask)):
            raise ValueError("invalid or noncausal executable quote")


@dataclass(frozen=True, slots=True)
class PathBar:
    close_ms: int
    received_ms: int
    high: float
    low: float

    def __post_init__(self) -> None:
        if (self.close_ms < 0 or self.received_ms <= self.close_ms
                or self.low <= 0 or self.high < self.low
                or not math.isfinite(self.high + self.low)):
            raise ValueError("invalid closed mark path bar")


@dataclass(frozen=True, slots=True)
class AddOpportunity:
    fill: Fill
    available_at_ms: int
    quote: ExecutableQuote
    candidate_state: State
    continuation_risk: bool
    renewed_confirmation: bool

    def __post_init__(self) -> None:
        if self.available_at_ms < 0 or self.quote.received_ms > self.available_at_ms:
            raise ValueError("add quote must be available before its decision")
        if self.fill.at_ms != self.quote.at_ms:
            raise ValueError("add fill time must match its executable quote")
        if self.fill.price not in (self.quote.bid, self.quote.ask):
            raise ValueError("add fill price must equal an executable bid or ask")


@dataclass(frozen=True, slots=True)
class FundingSettlement:
    """One observed settlement: the rate and valuation to apply to any open position.

    ``rate`` is the signed fractional rate for the settlement (positive: longs pay
    shorts). ``mark_price`` is the mark price used to value the position at that
    instant. Neither is an account cash amount.
    """

    settled_at_ms: int
    received_ms: int
    rate: float
    mark_price: float

    def __post_init__(self) -> None:
        if (self.settled_at_ms < 0 or self.received_ms < self.settled_at_ms
                or not math.isfinite(self.rate + self.mark_price)
                or abs(self.rate) > FUNDING_MAX_ABS_RATE or self.mark_price <= 0):
            raise ValueError("funding must be a finite, receipt-stamped rate and valuation")


@dataclass(frozen=True, slots=True)
class FundingCoverage:
    """Verified-complete settlement schedule for ``[start_ms, end_ms]`` (inclusive).

    ``scheduled_settlements_ms`` lists every settlement instant inside the interval
    (empty means verified that none occurred), strictly ascending.
    """

    start_ms: int
    end_ms: int
    scheduled_settlements_ms: tuple[int, ...]

    def __post_init__(self) -> None:
        schedule = self.scheduled_settlements_ms
        if self.start_ms < 0 or self.end_ms < self.start_ms:
            raise ValueError("funding coverage interval is invalid")
        if any(b <= a for a, b in pairwise(schedule)):
            raise ValueError("scheduled settlements must be strictly ascending")
        if any(not self.start_ms <= value <= self.end_ms for value in schedule):
            raise ValueError("scheduled settlement lies outside the verified coverage")


@dataclass(frozen=True, slots=True)
class ParentPath:
    parent_id: str
    event_id: str
    event_at_ms: int
    decision_available_ms: int
    exposure_class: ExposureClass
    ladder: FrozenLadder
    entry_quote: ExecutableQuote | None
    quotes: tuple[ExecutableQuote, ...]
    bars: tuple[PathBar, ...]
    additions: tuple[AddOpportunity, ...]
    funding: tuple[FundingSettlement, ...]
    funding_coverage: FundingCoverage | None
    horizon_end_ms: int
    asof_ms: int

    def __post_init__(self) -> None:
        if not self.parent_id or not self.event_id:
            raise ValueError("parent and event identities are required")
        if not self.event_at_ms <= self.decision_available_ms <= self.horizon_end_ms:
            raise ValueError("parent decision/horizon chronology is invalid")
        if self.asof_ms < self.horizon_end_ms:
            raise ValueError("path cannot be evaluated before the full horizon is observed")
        if self.entry_quote is not None and (
            self.entry_quote.received_ms > self.asof_ms
            or self.entry_quote.at_ms < self.decision_available_ms
        ):
            raise ValueError("entry quote is not causally executable after decision")
        if tuple(sorted(self.quotes, key=lambda q: (q.at_ms, q.received_ms))) != self.quotes:
            raise ValueError("quotes must be ordered")
        if tuple(sorted(self.bars, key=lambda b: b.close_ms)) != self.bars:
            raise ValueError("closed path bars must be ordered")
        if tuple(sorted(self.additions, key=lambda a: a.available_at_ms)) != self.additions:
            raise ValueError("add opportunities must be ordered")
        if any(q.received_ms > self.asof_ms or q.at_ms > self.horizon_end_ms
               for q in self.quotes):
            raise ValueError("future or post-horizon quote in path")
        if any(bar.received_ms > self.asof_ms or bar.close_ms > self.horizon_end_ms
               for bar in self.bars):
            raise ValueError("future or post-horizon mark bar in path")
        if any(a.available_at_ms > self.asof_ms for a in self.additions):
            raise ValueError("future add decision in path")
        if any(x.received_ms > self.asof_ms or x.settled_at_ms > self.horizon_end_ms
               for x in self.funding):
            raise ValueError("future or post-horizon funding settlement")
        settled = [x.settled_at_ms for x in self.funding]
        if len(settled) != len(set(settled)):
            raise ValueError("duplicate funding settlement instant")
        if self.funding_coverage is not None and (
            self.funding_coverage.end_ms > self.horizon_end_ms
        ):
            raise ValueError("funding coverage extends beyond the observed horizon")
        if self.funding and self.funding_coverage is None:
            raise ValueError("observed funding requires an explicit verified coverage")
        if self.funding_coverage is not None and any(
            value not in self.funding_coverage.scheduled_settlements_ms for value in settled
        ):
            raise ValueError("funding settlement is not in the verified schedule")


@dataclass(frozen=True, slots=True)
class Arm:
    name: str
    max_additions: int
    max_tranche_fraction: float
    legacy_spacing_pct: float = 0.0


ARMS = (
    Arm("primary_two_add", PRIMARY_MAX_ADDITIONS, PRIMARY_MAX_TRANCHE_FRACTION),
    Arm("first_fill_only", 0, 0.0),
    Arm("legacy_eight_add_comparator", LEGACY_MAX_ADDITIONS, 1.0,
        LEGACY_ADVERSE_SPACING_PCT),
    Arm("three_add_sensitivity", SENSITIVITY_MAX_ADDITIONS,
        PRIMARY_MAX_TRANCHE_FRACTION),
)


@dataclass(frozen=True, slots=True)
class ParentArmResult:
    parent_id: str
    event_id: str
    event_day_utc: int
    initial_notional_usdt: float
    arm: str
    exposure_class: ExposureClass
    status: str
    parent_in_denominator: bool
    abstained: bool
    accepted_additions: int
    filled_quantity: float
    gross_pnl_usdt: float | None
    costs_usdt: float | None
    funding_usdt: float | None
    net_pnl_usdt: float | None
    loss_r: float | None
    funding_status: str = FUNDING_NOT_APPLICABLE


def _quote_exit(path: ParentPath, stop_bar: PathBar | None) -> ExecutableQuote | None:
    # A stop is only actionable once its closed bar has been received.
    after = (max(stop_bar.close_ms, stop_bar.received_ms) if stop_bar is not None
             else path.horizon_end_ms)
    return next((quote for quote in path.quotes
                 if quote.at_ms >= after and quote.received_ms <= path.asof_ms), None)


def _stopped(path: ParentPath) -> PathBar | None:
    stop = path.ladder.invalidation_price
    for bar in path.bars:
        if bar.close_ms < path.decision_available_ms:
            continue
        crossed = bar.high >= stop if path.ladder.side is Side.SHORT else bar.low <= stop
        if crossed:
            return bar
    return None


def _adverse_spacing(side: Side, prior: float, candidate: float) -> float:
    return ((candidate / prior - 1) if side is Side.SHORT
            else (prior / candidate - 1)) * 100


def _risk_usdt(side: Side, stop: float, fill: Fill, cost_bps: float) -> float:
    adverse = stop - fill.price if side is Side.SHORT else fill.price - stop
    if adverse <= 0:
        raise ValueError("invalidation is not adverse to every accepted fill")
    return fill.quantity * (adverse + fill.price * cost_bps / BPS)


def _arm_funding(
    path: ParentPath, fills: list[Fill], exit_quote: ExecutableQuote,
) -> tuple[float | None, str]:
    """Funding cash (positive = received) from this arm's own exposure, or unavailable."""

    coverage = path.funding_coverage
    first_fill_ms = min(fill.at_ms for fill in fills)
    if (coverage is None or coverage.start_ms > first_fill_ms
            or coverage.end_ms < exit_quote.at_ms):
        return None, FUNDING_UNAVAILABLE_NO_COVERAGE
    observed = {item.settled_at_ms: item for item in path.funding}
    side_sign = 1.0 if path.ladder.side is Side.SHORT else -1.0
    total = 0.0
    for settled_at_ms in coverage.scheduled_settlements_ms:
        if settled_at_ms > exit_quote.at_ms:
            continue  # the arm had already exited
        quantity = sum(fill.quantity for fill in fills if fill.at_ms < settled_at_ms)
        if quantity <= 0:
            continue  # nothing was open yet
        settlement = observed.get(settled_at_ms)
        if settlement is None:
            return None, FUNDING_UNAVAILABLE_MISSING_SETTLEMENT
        total += side_sign * quantity * settlement.mark_price * settlement.rate
    return total, FUNDING_VERIFIED


def _simulate_arm(path: ParentPath, arm: Arm, *, cost_bps: float) -> ParentArmResult:
    parent_day = path.event_at_ms // 86_400_000
    if path.entry_quote is None:
        return ParentArmResult(path.parent_id, path.event_id, parent_day, 0.0, arm.name,
                               path.exposure_class, "MISSING_ENTRY_QUOTE", True, True,
                               0, 0.0, None, None, None, None, None)
    initial = path.ladder.initial_fill
    executable_entry = (path.entry_quote.bid if path.ladder.side is Side.SHORT
                        else path.entry_quote.ask)
    if initial.at_ms != path.entry_quote.at_ms or initial.price != executable_entry:
        raise ValueError("initial fill must use the causal executable entry quote")
    if path.entry_quote.at_ms < path.decision_available_ms:
        raise ValueError("entry quote predates the causal decision")

    stop_bar = _stopped(path)
    exit_quote = _quote_exit(path, stop_bar)
    accepted = [initial]
    accepted_tranches: dict[str, float] = {}
    accepted_count = 0
    used_risk = _risk_usdt(path.ladder.side, path.ladder.invalidation_price,
                           initial, cost_bps)
    last_fill_price = initial.price
    for opportunity in path.additions:
        if stop_bar is not None and opportunity.available_at_ms >= stop_bar.close_ms:
            continue
        if opportunity.available_at_ms < path.decision_available_ms:
            continue
        if exit_quote is not None and opportunity.fill.at_ms >= exit_quote.at_ms:
            continue  # no addition can fill at or after the simulated exit
        if opportunity.candidate_state is not State.FADE_CANDIDATE or (
            opportunity.continuation_risk or not opportunity.renewed_confirmation
        ):
            continue
        executable_add = (opportunity.quote.bid if path.ladder.side is Side.SHORT
                          else opportunity.quote.ask)
        if opportunity.fill.price != executable_add:
            raise ValueError("addition fill does not use the side-correct executable quote")
        is_new_tranche = opportunity.fill.tranche_id not in accepted_tranches
        if is_new_tranche and accepted_count >= arm.max_additions:
            continue
        tranche_total = (accepted_tranches.get(opportunity.fill.tranche_id, 0.0)
                         + opportunity.fill.quantity)
        if tranche_total > arm.max_tranche_fraction * initial.quantity + 1e-12:
            continue
        if (arm.legacy_spacing_pct and _adverse_spacing(
            path.ladder.side, last_fill_price, opportunity.fill.price
        ) < arm.legacy_spacing_pct):
            continue
        if (path.ladder.side is Side.SHORT
                and opportunity.fill.price >= path.ladder.invalidation_price):
            continue
        if (path.ladder.side is Side.LONG
                and opportunity.fill.price <= path.ladder.invalidation_price):
            continue
        candidate_risk = _risk_usdt(path.ladder.side, path.ladder.invalidation_price,
                                    opportunity.fill, cost_bps)
        if used_risk + candidate_risk > path.ladder.max_loss_usdt + 1e-10:
            continue
        if arm.name == "primary_two_add":
            assessment = assess_ladder(
                path.ladder, tuple(accepted[1:]), proposed=opportunity.fill,
                candidate_state=opportunity.candidate_state,
                continuation_risk=opportunity.continuation_risk,
                renewed_confirmation=opportunity.renewed_confirmation,
            )
            if not assessment.allowed:
                continue
        accepted.append(opportunity.fill)
        accepted_tranches[opportunity.fill.tranche_id] = tranche_total
        accepted_count += int(is_new_tranche)
        used_risk += candidate_risk
        last_fill_price = opportunity.fill.price

    if exit_quote is None:
        return ParentArmResult(path.parent_id, path.event_id, parent_day,
                               initial.quantity * initial.price, arm.name, path.exposure_class,
                               "CENSORED_NO_EXECUTABLE_EXIT", True,
                               len(accepted) == 1, accepted_count,
                               sum(fill.quantity for fill in accepted), None, None, None,
                               None, None)
    quantity = sum(fill.quantity for fill in accepted)
    entry_value = sum(fill.quantity * fill.price for fill in accepted)
    exit_price = exit_quote.ask if path.ladder.side is Side.SHORT else exit_quote.bid
    exit_value = quantity * exit_price
    gross = entry_value - exit_value if path.ladder.side is Side.SHORT else exit_value - entry_value
    costs = (entry_value + exit_value) * cost_bps / (2 * BPS)
    funding, funding_status = _arm_funding(path, accepted, exit_quote)
    net = gross - costs + funding if funding is not None else None
    status = "COMPLETE" if net is not None else "COMPLETE_FUNDING_UNAVAILABLE"
    return ParentArmResult(path.parent_id, path.event_id, parent_day,
                           initial.quantity * initial.price, arm.name, path.exposure_class,
                           status, True, len(accepted) == 1,
                           accepted_count, quantity, gross, costs, funding, net,
                           (max(0.0, -net) / path.ladder.max_loss_usdt
                            if net is not None else None), funding_status)


def compare_parent_paths(
    parents: tuple[ParentPath, ...], *, cost_bps: float = DEFAULT_ROUND_TRIP_COST_BPS,
) -> tuple[ParentArmResult, ...]:
    """Evaluate all declared arms on the same parent cohort and executable paths."""

    if not math.isfinite(cost_bps) or cost_bps < 0 or cost_bps > BPS:
        raise ValueError("cost scenario must be finite and within [0, 10000] bps")
    ids = [parent.parent_id for parent in parents]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate parent identity in comparison cohort")
    return tuple(_simulate_arm(parent, arm, cost_bps=cost_bps)
                 for parent in parents for arm in ARMS)


def summarize_arm_results(results: tuple[ParentArmResult, ...]) -> dict[str, object]:
    """Summarize support without treating absent/censored paths as zero returns."""

    names = sorted({row.arm for row in results})
    output: dict[str, object] = {}
    arm_summaries: dict[str, dict[str, object]] = {}
    for name in names:
        rows = [row for row in results if row.arm == name]
        complete = [row for row in rows if row.status.startswith("COMPLETE")]
        net_observed = [row.net_pnl_usdt for row in complete
                        if row.net_pnl_usdt is not None]
        losses = sorted(row.loss_r for row in complete
                        if row.loss_r is not None and row.loss_r > 0)
        # A complete executable path without verified funding has no net result. Such
        # rows must not be silently dropped from net statistics: any of them makes the
        # arm's net and tail summaries unavailable rather than partial.
        funding_unavailable = len(complete) - len(net_observed)
        net_complete = bool(complete) and funding_unavailable == 0
        tail_ok = len(losses) >= 100 and funding_unavailable == 0
        arm_summary: dict[str, object] = {
            "parent_denominator": len(rows),
            "complete_executable_paths": len(complete),
            "net_complete_paths": len(net_observed),
            "funding_unavailable_paths": funding_unavailable,
            "censored_or_missing": len(rows) - len(complete),
            "abstentions": sum(row.abstained for row in rows),
            "net_inference_status": (
                "COMPLETE" if net_complete
                else "NO_EXECUTABLE_PATHS" if not complete
                else "UNAVAILABLE_FUNDING_COVERAGE_INCOMPLETE"
            ),
            "mean_net_pnl_usdt": (sum(net_observed) / len(net_observed)
                                  if net_complete else None),
            "q99_loss_r": (losses[math.ceil(0.99 * len(losses)) - 1] if tail_ok else None),
            "q99_status": (
                "OBSERVED_POSITIVE_LOSS_TAIL_N_GE_100" if tail_ok
                else "UNAVAILABLE_FUNDING_COVERAGE_INCOMPLETE" if funding_unavailable
                else "UNAVAILABLE_INSUFFICIENT_OBSERVED_TAIL"
            ),
        }
        output[name] = arm_summary
        arm_summaries[name] = arm_summary
    comparisons: dict[str, dict[str, object]] = {}
    baseline = {row.parent_id: row for row in results
                if row.arm == "first_fill_only" and row.net_pnl_usdt is not None}
    p_values: dict[str, float] = {}
    paired_losses: dict[str, tuple[list[float], list[float]]] = {}
    for name in names:
        if name == "first_fill_only":
            continue
        challenger = {row.parent_id: row for row in results
                      if row.arm == name and row.net_pnl_usdt is not None}
        pairs = []
        for parent_id in sorted(baseline.keys() & challenger.keys()):
            control = baseline[parent_id]
            treatment = challenger[parent_id]
            if (control.event_id != treatment.event_id
                    or control.exposure_class is not treatment.exposure_class
                    or control.initial_notional_usdt <= 0
                    or treatment.initial_notional_usdt != control.initial_notional_usdt
                    or treatment.net_pnl_usdt is None
                    or control.net_pnl_usdt is None):
                continue
            delta_bps = ((treatment.net_pnl_usdt - control.net_pnl_usdt)
                         / control.initial_notional_usdt * BPS)
            pairs.append((control.event_day_utc, delta_bps))
        every_control = {row.parent_id: row for row in results if row.arm == "first_fill_only"}
        every_treatment = {row.parent_id: row for row in results if row.arm == name}
        funding_dropped = sum(
            1 for parent_id in every_control.keys() & every_treatment.keys()
            if every_control[parent_id].status.startswith("COMPLETE")
            and every_treatment[parent_id].status.startswith("COMPLETE")
            and (every_control[parent_id].net_pnl_usdt is None
                 or every_treatment[parent_id].net_pnl_usdt is None)
        )
        day_clusters: dict[int, list[float]] = {}
        for day, delta in pairs:
            day_clusters.setdefault(day, []).append(delta)
        supported = (len(pairs) >= MIN_CLUSTERED_PAIRED_PARENTS
                     and len(day_clusters) >= MIN_CLUSTERED_UTC_DAYS
                     and funding_dropped == 0)
        lower: float | None = None
        mean_delta: float | None = None
        raw_p: float | None = None
        if supported:
            mean_delta = sum(delta for _, delta in pairs) / len(pairs)
            days = tuple(sorted(day_clusters))
            rng = random.Random(BOOTSTRAP_SEED)
            boot: list[float] = []
            for _ in range(BOOTSTRAP_REPLICATES):
                sampled = [rng.choice(days) for _ in days]
                values = [delta for day in sampled for delta in day_clusters[day]]
                boot.append(sum(values) / len(values))
            boot.sort()
            lower = boot[math.floor(0.05 * (BOOTSTRAP_REPLICATES - 1))]
            raw_p = sum(value <= -NONINFERIORITY_MARGIN_BPS for value in boot) / len(boot)
            p_values[name] = raw_p
        baseline_loss = []
        challenger_loss = []
        by_challenger = {row.parent_id: row for row in results if row.arm == name}
        for parent_id in sorted(baseline.keys() & by_challenger.keys()):
            control = baseline[parent_id]
            treatment = by_challenger[parent_id]
            if (control.loss_r is not None and treatment.loss_r is not None
                    and control.loss_r > 0 and treatment.loss_r > 0):
                baseline_loss.append(control.loss_r)
                challenger_loss.append(treatment.loss_r)
        paired_losses[name] = (baseline_loss, challenger_loss)
        q99_reduction = None
        if (len(baseline_loss) >= 100 and len(challenger_loss) >= 100
                and funding_dropped == 0):
            control_q99 = sorted(baseline_loss)[math.ceil(0.99 * len(baseline_loss)) - 1]
            treatment_q99 = sorted(challenger_loss)[math.ceil(0.99 * len(challenger_loss)) - 1]
            if control_q99 > 0:
                q99_reduction = (control_q99 - treatment_q99) / control_q99
        comparisons[name] = {
            "paired_net_parent_n": len(pairs),
            "paired_funding_unavailable_n": funding_dropped,
            "utc_day_clusters_n": len(day_clusters),
            "mean_net_difference_bps": mean_delta,
            "one_sided_95pct_cluster_bootstrap_lower_bps": lower,
            "bootstrap_replicates": BOOTSTRAP_REPLICATES if supported else 0,
            "net_noninferiority_margin_bps": NONINFERIORITY_MARGIN_BPS,
            "net_noninferior": (lower > -NONINFERIORITY_MARGIN_BPS
                                 if lower is not None else None),
            "net_noninferiority_status": (
                "CLUSTERED_BOOTSTRAP_SUPPORTED" if supported
                else "UNAVAILABLE_FUNDING_COVERAGE_INCOMPLETE_FOR_PAIRED_PARENTS"
                if funding_dropped
                else "UNAVAILABLE_REQUIRES_100_PAIRED_PARENTS_AND_20_UTC_DAYS"
            ),
            "raw_one_sided_bootstrap_p": raw_p,
            "holm_adjusted_p": None,
            "paired_positive_loss_n": len(baseline_loss),
            "q99_loss_reduction_fraction": q99_reduction,
            "q99_status": ("PAIRED_OBSERVED_LOSS_TAIL_N_GE_100" if q99_reduction is not None
                           else "UNAVAILABLE_INSUFFICIENT_PAIRED_OBSERVED_LOSSES"),
        }
    ordered_p = sorted(p_values.items(), key=lambda item: item[1])
    previous_adjusted = 0.0
    for index, (name, p_value) in enumerate(ordered_p):
        adjusted = min(1.0, (len(ordered_p) - index) * p_value)
        previous_adjusted = max(previous_adjusted, adjusted)
        comparisons[name]["holm_adjusted_p"] = previous_adjusted
    for name in names:
        if name != "first_fill_only":
            arm_summaries[name]["net_noninferiority_10bp"] = comparisons[name][
                "net_noninferiority_status"
            ]
    output["paired_comparisons_vs_first_fill_only"] = comparisons
    return output
