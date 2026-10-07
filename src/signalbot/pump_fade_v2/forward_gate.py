"""Pure forward-evaluation eligibility checks; never starts a campaign."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ForwardGate:
    duration_gate: bool
    parent_count_gate: bool
    clustered_power_gate: bool
    positive_lower_bound_gate: bool
    eligible_for_human_review: bool


def evaluate_forward_gate(
    *, days_since_eligible_start: int, valid_parent_alerts: int,
    clustered_power_passed: bool, net_mean_bps: float,
    one_sided_cluster_lower_bound_bps: float,
    minimum_days: int = 56, minimum_valid_parent_alerts: int = 150,
) -> ForwardGate:
    """Require time, count, power, and a strictly positive clustered lower bound."""

    if min(days_since_eligible_start, valid_parent_alerts, minimum_days,
           minimum_valid_parent_alerts) < 0:
        raise ValueError("forward gate counts cannot be negative")
    duration = days_since_eligible_start >= minimum_days
    count = valid_parent_alerts >= minimum_valid_parent_alerts
    lower_bound = one_sided_cluster_lower_bound_bps > 0
    # Mean is accepted as an input for explicit audit, never as promotion evidence.
    _ = net_mean_bps
    return ForwardGate(duration, count, clustered_power_passed, lower_bound,
                       duration and count and clustered_power_passed and lower_bound)
