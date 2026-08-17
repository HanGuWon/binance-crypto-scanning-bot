"""Outcome-blind preregistration for the 365-day prospective efficacy gate.

This module freezes what a later inference engine must evaluate.  It reads no
outcomes and cannot issue a PASS verdict.  Family hypotheses are multiplicity-
controlled with Holm; fee and size stress cells form an intersection-union
test, so a family may promote only when every frozen economic cell passes.
"""

from __future__ import annotations

import hashlib
from typing import Final

from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.paper_sizing import (
    PAPER_CAPACITY_QUOTE_NOTIONAL_USDT_V2,
    PAPER_FIXED_QUOTE_NOTIONAL_USDT_V2,
)
from signalbot.r4b_v2.protocol.lifecycle import (
    EFFICACY_CALENDAR_DAYS_V2,
    ProspectiveAttemptV2,
)

PROSPECTIVE_EFFICACY_GATE_SCHEMA_V2: Final = (
    "r4b_v2_prospective_efficacy_gate_contract_v2"
)
PROSPECTIVE_EFFICACY_GATE_STATUS_V2: Final = (
    "PREREGISTERED_OUTCOME_BLIND_NO_VERDICT"
)
_GATE_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_EFFICACY_GATE_CONTRACT_V2\0"


class ProspectiveEfficacyContractErrorV2(ValueError):
    """Raised when a gate is requested for an ineligible attempt."""


def current_prospective_efficacy_gate_document_v2(
    attempt: ProspectiveAttemptV2,
) -> dict[str, object]:
    """Return the exact outcome-blind gate document for one attempt."""

    if type(attempt) is not ProspectiveAttemptV2:
        raise TypeError("attempt must be exact ProspectiveAttemptV2")
    if attempt.terminal_status is not None:
        raise ProspectiveEfficacyContractErrorV2(
            "cannot preregister an already terminal attempt"
        )
    alpha = attempt.nominal_alpha
    return {
        "attempt": {
            "attempt_index": attempt.attempt_index,
            "h_max_ms": attempt.horizon.h_max_ms,
            "h_start_ms": attempt.horizon.h_start_ms,
            "one_sided_alpha_denominator": alpha.denominator,
            "one_sided_alpha_numerator": alpha.numerator,
        },
        "coverage_gates": {
            "actionability_rate_min": "0.99",
            "expected_cell_disposition_rate": "1.0",
            "required_field_availability_min": "0.999",
            "unresolved_schema_shift_count_max": 0,
            "unresolved_sequence_gap_count_max": 0,
        },
        "economic_cells": {
            "combination_rule": "INTERSECTION_UNION_ALL_CELLS_MUST_PASS",
            "fee_multipliers": ("1.0", "1.5"),
            "quote_notional_usdt": (
                str(PAPER_FIXED_QUOTE_NOTIONAL_USDT_V2),
                str(PAPER_CAPACITY_QUOTE_NOTIONAL_USDT_V2),
            ),
        },
        "economic_gates_per_family_and_cell": {
            "cumulative_net_pnl_strictly_gt_usdt": "0",
            "mean_net_bps_min": "5.0",
            "one_sided_block_bootstrap_mean_net_bps_lower_strictly_gt": "0",
            "profit_factor_block_bootstrap_lower_min": "1.05",
            "profit_factor_point_min": "1.20",
        },
        "family_hypotheses": {
            "families": tuple(family.value for family in PromotingFamilyV2),
            "family_null": "MEAN_AFTER_COST_EXECUTED_EPISODE_RETURN_LE_ZERO",
            "multiplicity": "HOLM_STEP_DOWN_ACROSS_A_B_C",
            "promotion_rule": (
                "PROMOTE_ONLY_HOLM_REJECTED_FAMILIES_PASSING_EVERY_OTHER_GATE"
            ),
            "unrejected_family_action": "DISABLE_WITH_NO_EFFICACY_CLAIM",
        },
        "inference": {
            "bootstrap_resamples": 20_000,
            "calendar_block_days": 7,
            "iid_trade_tests": "FORBIDDEN",
            "p_value_method": "SYNCHRONIZED_CALENDAR_BLOCK_BOOTSTRAP_ONE_SIDED",
            "random_seed": 2_607_210_001,
            "resampling_unit": "UTC_DAY_SHARED_ACROSS_SYMBOLS_AND_FAMILIES",
        },
        "outcome_accounting": {
            "entry": "FACTORY_VERIFIED_PAPER_FOK_FULL_FILL_ONLY",
            "fee": "CAUSAL_PUBLIC_USDM_TAKER_BOTH_ENTRY_AND_EXIT",
            "funding": "EXACT_CONFIRMED_REALIZED_CASHFLOW",
            "inconclusive": "COVERAGE_FAILURE_NOT_ZERO_RETURN",
            "mandatory_exit": "FACTORY_VERIFIED_TERMINAL_WITH_ZERO_RESIDUAL",
            "no_fill": "REPORTED_IN_FILL_RATE_NOT_AN_EXECUTED_EPISODE",
            "return_denominator": "ENTRY_EXECUTABLE_NOTIONAL",
        },
        "sample_gates_per_family": {
            "active_utc_days_min": 45,
            "calendar_days_exact": EFFICACY_CALENDAR_DAYS_V2,
            "complete_calendar_quarters_min": 4,
            "executed_episodes_min": 500,
            "non_overlapping_episodes_min": 150,
        },
        "schema_version": PROSPECTIVE_EFFICACY_GATE_SCHEMA_V2,
        "stability_gates_per_family": {
            "max_positive_pnl_share_single_quarter": "0.35",
            "max_positive_pnl_share_single_symbol": "0.20",
            "net_positive_after_removing_top_symbols": 3,
            "net_positive_after_removing_top_trades": 10,
            "positive_calendar_quarters_min": 3,
            "positive_event_symbol_fraction_min": "0.60",
        },
        "status": PROSPECTIVE_EFFICACY_GATE_STATUS_V2,
        "verdict_authority": {
            "profit_guarantee": False,
            "production_order_authorized": False,
            "single_evaluation_after_h_max": True,
        },
    }


def current_prospective_efficacy_gate_sha256_v2(
    attempt: ProspectiveAttemptV2,
) -> str:
    """Return the domain-separated hash of the frozen attempt-specific gate."""

    return hashlib.sha256(
        _GATE_DOMAIN
        + canonical_json_line(current_prospective_efficacy_gate_document_v2(attempt))
    ).hexdigest()
