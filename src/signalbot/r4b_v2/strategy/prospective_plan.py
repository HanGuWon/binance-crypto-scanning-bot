"""Frozen current-code bindings for a prospective efficacy census.

This module is the strategy-layer factory for the generic census model.  It
summarizes the exact public timing and execution constants that affect PAPER
admission or returns, and it binds the three current family rule versions.
It performs no I/O and does not choose or register ``H_start``.
"""

from __future__ import annotations

import hashlib
from typing import Final

from signalbot.r4b_v2.alerts.actionability import (
    ACTIONABILITY_LATE_GRACE_MS_V2,
    ACTIONABILITY_ROLE_V2,
    ACTIONABILITY_RULE_VERSION_V2,
    ACTIONABILITY_THRESHOLD_DENOMINATOR_V2,
    ACTIONABILITY_THRESHOLD_NUMERATOR_V2,
    PromotingFamilyV2,
)
from signalbot.r4b_v2.alerts.actionability import (
    PRIMARY_PAPER_TARGET_DELAY_MS_V2 as ACTIONABILITY_TARGET_DELAY_MS_V2,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.fees import (
    FEE_POLL_CADENCE_MS_V2,
    FEE_RULE_VERSION_V2,
    PUBLIC_FEE_SCENARIO_V2,
    USDM_OFFICIAL_FEE_URL_V2,
    USDM_PUBLIC_TAKER_RATE_V2,
)
from signalbot.r4b_v2.execution.funding import (
    FUNDING_CONFIRMATION_MAX_DELAY_MS_V2,
    FUNDING_ENDPOINT_PATH_V2,
    FUNDING_HORIZON_GRACE_MS_V2,
    FUNDING_ROUTE_ID_V2,
    FUNDING_RULE_VERSION_V2,
)
from signalbot.r4b_v2.execution.mandatory_exit import (
    EXIT_ACK_TARGET_DELAY_MS_V2,
    EXIT_MISSING_ACK_EMERGENCY_DELAY_MS_V2,
    EXIT_RETRY_WINDOW_MS_V2,
    MANDATORY_EXIT_RULE_VERSION_V2,
    PRIMARY_EXIT_DEPTH_HAIRCUT_V2,
)
from signalbot.r4b_v2.execution.paper_fok import (
    MARK_PRICE_MAX_STALENESS_MS_V2,
    PAPER_FOK_RULE_VERSION_V2,
    PAPER_PRICE_CAP_RATE_V2,
    PRIMARY_DEPTH_HAIRCUT_V2,
    PRIMARY_PAPER_TARGET_DELAY_MS_V2,
)
from signalbot.r4b_v2.execution.paper_sizing import (
    PAPER_CAPACITY_QUOTE_NOTIONAL_USDT_V2,
    PAPER_FIXED_QUOTE_NOTIONAL_USDT_V2,
    PAPER_SIZING_RULE_VERSION_V2,
)
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusContractErrorV2,
    ProspectiveCensusPlanV2,
    ProspectiveFamilyRuleBindingV2,
)
from signalbot.r4b_v2.execution.prospective_decision_payload import (
    PROSPECTIVE_DECISION_PAYLOAD_RULE_VERSION_V2,
)
from signalbot.r4b_v2.execution.prospective_outcome_wal_record import (
    PROSPECTIVE_OUTCOME_WAL_RECORD_SCHEMA_V2,
    PROSPECTIVE_OUTCOME_WAL_RULE_VERSION_V2,
    ProspectiveOutcomeWalRecordKindV2,
)
from signalbot.r4b_v2.execution.prospective_terminal_contract import (
    PROSPECTIVE_PAPER_TERMINAL_RULE_VERSION_V2,
)
from signalbot.r4b_v2.execution.prospective_wal_record import (
    PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
)
from signalbot.r4b_v2.protocol.decimal_context import (
    PROTOCOL_DECIMAL_PRECISION_V2,
)
from signalbot.r4b_v2.protocol.decision_clock import (
    DECISION_DELAY_MS_V2,
    FIVE_MINUTE_MS_V2,
)
from signalbot.r4b_v2.protocol.lifecycle import (
    CONFIRMATION_GRACE_MS_V2,
    EFFICACY_CALENDAR_DAYS_V2,
    FINAL_ADMISSION_TAIL_MS_V2,
    ProspectiveAttemptV2,
)
from signalbot.r4b_v2.protocol.prospective_code_freeze import (
    ProspectiveCodeFreezeReceiptV2,
)
from signalbot.r4b_v2.research.prospective_efficacy_contract import (
    current_prospective_efficacy_gate_sha256_v2,
)
from signalbot.r4b_v2.strategy.family_a import (
    FAMILY_A_HARD_HORIZON_BARS_V2,
    FAMILY_A_PAPER_TARGET_DELAY_MS_V2,
    FAMILY_A_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.family_b import (
    FAMILY_B_HARD_HORIZON_BARS_V2,
    FAMILY_B_RULE_VERSION_V2,
)
from signalbot.r4b_v2.strategy.family_c import (
    FAMILY_C_HARD_HORIZON_BARS_V2,
    FAMILY_C_MINIMUM_MEMBERS_V2,
    FAMILY_C_PRIOR_WINDOW_V2,
    FAMILY_C_RULE_VERSION_V2,
)

PROSPECTIVE_EXECUTION_CONTRACT_SCHEMA_V2: Final = "r4b_v2_prospective_execution_contract_v2"
_EXECUTION_CONTRACT_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_EXECUTION_CONTRACT_V2\0"


def current_prospective_family_rules_v2() -> tuple[ProspectiveFamilyRuleBindingV2, ...]:
    """Return all promoting families in the census's canonical order."""

    return (
        ProspectiveFamilyRuleBindingV2(
            family=PromotingFamilyV2.A,
            rule_version=FAMILY_A_RULE_VERSION_V2,
        ),
        ProspectiveFamilyRuleBindingV2(
            family=PromotingFamilyV2.B,
            rule_version=FAMILY_B_RULE_VERSION_V2,
        ),
        ProspectiveFamilyRuleBindingV2(
            family=PromotingFamilyV2.C,
            rule_version=FAMILY_C_RULE_VERSION_V2,
        ),
    )


def current_prospective_execution_contract_document_v2() -> dict[str, object]:
    """Return the canonical semantic summary of PAPER outcome-bearing rules."""

    if not (
        ACTIONABILITY_TARGET_DELAY_MS_V2
        == PRIMARY_PAPER_TARGET_DELAY_MS_V2
        == FAMILY_A_PAPER_TARGET_DELAY_MS_V2
    ):
        raise ProspectiveCensusContractErrorV2(
            "family A, actionability, and PAPER target delays must remain identical"
        )
    return {
        "actionability": {
            "late_grace_ms": ACTIONABILITY_LATE_GRACE_MS_V2,
            "role": ACTIONABILITY_ROLE_V2,
            "rule_version": ACTIONABILITY_RULE_VERSION_V2,
            "target_delay_ms": ACTIONABILITY_TARGET_DELAY_MS_V2,
            "threshold_denominator": ACTIONABILITY_THRESHOLD_DENOMINATOR_V2,
            "threshold_numerator": ACTIONABILITY_THRESHOLD_NUMERATOR_V2,
        },
        "authority_scope": {
            "market_data": "PUBLIC_BINANCE_ONLY",
            "order_placement": False,
            "paper_only": True,
            "venue": "USDM_FUTURES",
        },
        "decision_clock": {
            "decision_delay_ms": DECISION_DELAY_MS_V2,
            "five_minute_ms": FIVE_MINUTE_MS_V2,
        },
        "decision_wal": {
            "crash_reconciliation_authoritative": False,
            "daily_wal_typed_replay_authoritative": False,
            "commit_order": (
                "PREVIEW",
                "DURABLE_PREPARE",
                "RECEIPT",
                "STATE_COMMIT",
                "DURABLE_DISPOSITION",
            ),
            "fail_stop_on_ambiguous_disposition": True,
            "inconclusive_distinct_from_no_signal": True,
            "production_order_placement": False,
            "rule_version": PROSPECTIVE_DECISION_PAYLOAD_RULE_VERSION_V2,
            "suppressed_distinct_from_no_signal": True,
            "typed_payload_parser_available": True,
        },
        "decimal_arithmetic": {
            "precision": PROTOCOL_DECIMAL_PRECISION_V2,
            "rounding": "ROUND_HALF_EVEN",
            "trapped_signals": (
                "DivisionByZero",
                "InvalidOperation",
                "Overflow",
                "Underflow",
            ),
        },
        "family_lifecycle": {
            "family_a_hard_horizon_bars": FAMILY_A_HARD_HORIZON_BARS_V2,
            "family_b_hard_horizon_bars": FAMILY_B_HARD_HORIZON_BARS_V2,
            "family_c_hard_horizon_bars": FAMILY_C_HARD_HORIZON_BARS_V2,
            "family_c_minimum_members": FAMILY_C_MINIMUM_MEMBERS_V2,
            "family_c_prior_window_bars": FAMILY_C_PRIOR_WINDOW_V2,
        },
        "fee": {
            "official_url": USDM_OFFICIAL_FEE_URL_V2,
            "poll_cadence_ms": FEE_POLL_CADENCE_MS_V2,
            "public_taker_rate": str(USDM_PUBLIC_TAKER_RATE_V2),
            "rule_version": FEE_RULE_VERSION_V2,
            "scenario": PUBLIC_FEE_SCENARIO_V2,
        },
        "funding": {
            "confirmation_max_delay_ms": FUNDING_CONFIRMATION_MAX_DELAY_MS_V2,
            "endpoint_path": FUNDING_ENDPOINT_PATH_V2,
            "horizon_grace_ms": FUNDING_HORIZON_GRACE_MS_V2,
            "route_id": FUNDING_ROUTE_ID_V2,
            "rule_version": FUNDING_RULE_VERSION_V2,
        },
        "lifecycle": {
            "confirmation_grace_ms": CONFIRMATION_GRACE_MS_V2,
            "efficacy_calendar_days": EFFICACY_CALENDAR_DAYS_V2,
            "final_admission_tail_ms": FINAL_ADMISSION_TAIL_MS_V2,
        },
        "mandatory_exit": {
            "ack_target_delay_ms": EXIT_ACK_TARGET_DELAY_MS_V2,
            "depth_haircut": str(PRIMARY_EXIT_DEPTH_HAIRCUT_V2),
            "missing_ack_emergency_delay_ms": (EXIT_MISSING_ACK_EMERGENCY_DELAY_MS_V2),
            "retry_window_ms": EXIT_RETRY_WINDOW_MS_V2,
            "rule_version": MANDATORY_EXIT_RULE_VERSION_V2,
        },
        "outcome_wal": {
            "position_terminal_typed": False,
            "production_order_placement": False,
            "record_kinds": tuple(kind.value for kind in ProspectiveOutcomeWalRecordKindV2),
            "record_schema_version": PROSPECTIVE_OUTCOME_WAL_RECORD_SCHEMA_V2,
            "rule_version": PROSPECTIVE_OUTCOME_WAL_RULE_VERSION_V2,
            "scope": "ATTEMPT_WIDE_ORIGIN_CELL_REFERENCED",
        },
        "paper_fok": {
            "depth_haircut": str(PRIMARY_DEPTH_HAIRCUT_V2),
            "mark_price_max_staleness_ms": MARK_PRICE_MAX_STALENESS_MS_V2,
            "price_cap_rate": str(PAPER_PRICE_CAP_RATE_V2),
            "rule_version": PAPER_FOK_RULE_VERSION_V2,
            "target_delay_ms": PRIMARY_PAPER_TARGET_DELAY_MS_V2,
        },
        "paper_sizing": {
            "quote_notional_cells_usdt": (
                str(PAPER_FIXED_QUOTE_NOTIONAL_USDT_V2),
                str(PAPER_CAPACITY_QUOTE_NOTIONAL_USDT_V2),
            ),
            "reference": "CAUSAL_TARGET_MARK_PRICE",
            "rounding": "FLOOR_TO_COMMON_LOT_AND_MARKET_LOT_GRID",
            "rule_version": PAPER_SIZING_RULE_VERSION_V2,
        },
        "paper_terminal": {
            "entry_terminal_only": True,
            "position_terminal": False,
            "production_order_placement": False,
            "rule_version": PROSPECTIVE_PAPER_TERMINAL_RULE_VERSION_V2,
            "schema_version": PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
        },
        "schema_version": PROSPECTIVE_EXECUTION_CONTRACT_SCHEMA_V2,
    }


def current_prospective_execution_contract_sha256_v2() -> str:
    """Hash the current semantic execution contract with domain separation."""

    return hashlib.sha256(
        _EXECUTION_CONTRACT_DOMAIN
        + canonical_json_line(current_prospective_execution_contract_document_v2())
    ).hexdigest()


def build_current_prospective_census_plan_v2(
    *,
    attempt_id: str,
    attempt: ProspectiveAttemptV2,
    promoting_plan_sha256: str,
    code_freeze_receipt: ProspectiveCodeFreezeReceiptV2,
    symbols: tuple[str, ...],
    context_symbols: tuple[str, ...],
    created_at_ms: int,
) -> ProspectiveCensusPlanV2:
    """Build a census bound to the current strategy and PAPER rule constants.

    The factory accepts only the policy-validated, factory-sealed pre-``H_start``
    code-freeze receipt and rechecks all three upstream bindings.
    """

    if type(context_symbols) is not tuple:
        raise ProspectiveCensusContractErrorV2("context_symbols must be an immutable tuple")
    if len(set(context_symbols)) < FAMILY_C_MINIMUM_MEMBERS_V2:
        raise ProspectiveCensusContractErrorV2(
            "Family C requires at least 20 distinct context symbols"
        )
    if type(code_freeze_receipt) is not ProspectiveCodeFreezeReceiptV2:
        raise TypeError("code_freeze_receipt must be exact ProspectiveCodeFreezeReceiptV2")
    execution_contract_sha256 = current_prospective_execution_contract_sha256_v2()
    efficacy_gate_contract_sha256 = current_prospective_efficacy_gate_sha256_v2(attempt)
    expected_freeze_bindings = (
        ("promoting_plan", promoting_plan_sha256),
        ("prospective_efficacy_gate", efficacy_gate_contract_sha256),
        ("prospective_execution_contract", execution_contract_sha256),
    )
    if (
        code_freeze_receipt.h_start_ms != attempt.horizon.h_start_ms
        or code_freeze_receipt.upstream_sha256 != expected_freeze_bindings
    ):
        raise ProspectiveCensusContractErrorV2(
            "code-freeze receipt differs from the attempt or current upstream contracts"
        )
    return ProspectiveCensusPlanV2(
        attempt_id=attempt_id,
        attempt=attempt,
        promoting_plan_sha256=promoting_plan_sha256,
        symbols=symbols,
        context_symbols=context_symbols,
        family_rules=current_prospective_family_rules_v2(),
        paper_fok_rule_version=PAPER_FOK_RULE_VERSION_V2,
        execution_contract_sha256=execution_contract_sha256,
        efficacy_gate_contract_sha256=efficacy_gate_contract_sha256,
        strategy_code_freeze_manifest_sha256=(code_freeze_receipt.manifest_sha256),
        created_at_ms=created_at_ms,
    )
