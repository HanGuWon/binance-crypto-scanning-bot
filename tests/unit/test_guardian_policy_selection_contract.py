from __future__ import annotations

import hashlib
import json
from pathlib import Path

CONTRACT_PATH = Path("config/guardian-policy-selection.v1.json")
DOMAIN_SEPARATOR = b"GUARDIAN_POLICY_SELECTION_CONTRACT_V1\0"
EXPECTED_CONTRACT_SHA256 = "2e19fd7a597dd78a0372753da50fd54dcfacceea6e9482bb34aac606c507a923"


def _contract() -> dict[str, object]:
    value = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _contract_sha256(value: dict[str, object]) -> str:
    canonical = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(DOMAIN_SEPARATOR + canonical).hexdigest()


def _canonical_text_sha256(path: str) -> str:
    value = Path(path).read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(value).hexdigest()


def test_guardian_policy_selection_contract_identity_is_frozen() -> None:
    contract = _contract()

    assert contract["schema_version"] == "guardian_policy_selection_preregistration_v1"
    assert contract["contract_version"] == "guardian-policy-selection-v1"
    assert contract["status"] == "PREREGISTERED_OUTCOME_BLIND"
    assert _contract_sha256(contract) == EXPECTED_CONTRACT_SHA256


def test_guardian_policy_selection_contract_freezes_required_decision_surface() -> None:
    contract = _contract()
    cohort = contract["cohort_authority"]
    assert isinstance(cohort, dict)
    historical = cohort["historical_harness"]
    assert isinstance(historical, dict)
    assert historical["source_sha256"] == (
        "2dce99a243c4f94c446cf48a0edcb103093db9cecf0e6da261c35b45fa235c7f"
    )
    assert historical["derived_override"] == {"direction_scope": "futures_bidirectional"}
    assert "same fully closed decision candle" in historical["admission_stratum_rule"]
    assert historical["symbol_cohort_map"] == {
        "BNBUSDT": "major",
        "BTCUSDT": "anchor",
        "DOGEUSDT": "major",
        "ETHUSDT": "anchor",
        "SOLUSDT": "major",
        "SUIUSDT": "volatile",
        "WIFUSDT": "volatile",
        "XRPUSDT": "major",
    }

    policies = contract["policies"]
    assert isinstance(policies, list)
    assert [policy["policy_id"] for policy in policies] == [
        "delayed_atr_trail_v1",
        "initial_stop_only_v1",
        "confirmed_swing_atr_trail_v1",
        "weakening_sensitive_adaptive_trail_v1",
    ]

    primary = contract["primary_metric"]
    assert isinstance(primary, dict)
    assert primary["baseline_policy_id"] == "delayed_atr_trail_v1"
    assert primary["metric"] == "paired_mean_after_cost_return_delta_bps"

    guardrails = contract["hard_guardrails"]
    assert isinstance(guardrails, dict)
    assert guardrails == {
        "maximum_censor_fraction": "0.10",
        "maximum_drawdown_worsening_percentage_points_vs_baseline": "2.0",
        "maximum_mean_mfe_giveback_fraction": "0.60",
        "maximum_mean_mfe_giveback_worsening_vs_baseline": "0.05",
        "maximum_mean_stop_updates_per_24h_exposure": "24.0",
        "maximum_premature_stop_rate": "0.15",
        "maximum_premature_stop_rate_worsening_vs_baseline": "0.05",
        "maximum_tail_loss_cvar_5_worsening_bps_vs_baseline": "25.0",
        "require_primary_bootstrap_lower_bound_strictly_positive": True,
        "require_primary_point_estimate_strictly_positive": True,
    }

    bootstrap = contract["bootstrap"]
    assert isinstance(bootstrap, dict)
    assert bootstrap["block_days"] == 7
    assert "all admitted cohort positions before policy-specific censoring" in bootstrap[
        "calendar_span_rule"
    ]
    assert "every challenger in that population uses this identical calendar" in bootstrap[
        "calendar_span_rule"
    ]
    assert bootstrap["calendar_key"] == "entry_utc_day"
    assert bootstrap["include_zero_entry_calendar_days"] is True
    assert bootstrap["minimum_valid_replicates"] == 8000
    assert bootstrap["method"] == "shared_circular_calendar_block_bootstrap"
    assert bootstrap["samples"] == 10000
    assert bootstrap["seed"] == 20260921
    assert "max_error=max_i" in str(bootstrap["familywise_rule"])
    assert "nearest_rank" in str(bootstrap["quantile_rule"])
    assert "zero-based 0..samples-1" in str(bootstrap["draw_algorithm"])
    assert "block ordinal b is zero-based" in str(bootstrap["draw_algorithm"])
    assert "no trailing newline" in str(bootstrap["draw_algorithm"])
    assert "unsigned base-10 ASCII integers" in str(bootstrap["draw_algorithm"])


def test_guardian_policy_selection_contract_is_outcome_blind_and_fail_closed() -> None:
    contract = _contract()
    encoded = json.dumps(contract, sort_keys=True)

    for forbidden in (
        "observed_return",
        "realized_result",
        "winning_policy",
        "selected_policy_id",
        "promotion_passed",
    ):
        assert forbidden not in encoded

    selection = contract["selection_rule"]
    assert isinstance(selection, dict)
    assert selection["all_hard_guardrails_must_pass"] is True
    assert selection["tie_break_population"] == "external_shadow"
    assert "strictly positive primary point estimate" in selection["external_shadow_gate"]
    assert "exact same paired-valid position intersection" in selection[
        "guardrail_population_rule"
    ]
    assert "not promotion gates" in selection["historical_harness_gate"]
    assert selection["no_qualifying_challenger_terminal"] == "NO_POLICY_PROMOTION"
    assert selection["tie_break_order"] == [
        "higher_paired_mean_after_cost_return_delta_bps",
        "lower_maximum_drawdown",
        "less_negative_cvar_5_return",
        "lower_premature_stop_rate",
        "lower_mean_mfe_giveback_fraction",
        "lower_mean_stop_updates_per_24h_exposure",
        "lexicographically_smaller_policy_id",
    ]

    semantics = contract["outcome_semantics"]
    assert isinstance(semantics, dict)
    assert semantics["new_stop_effective_from"] == "NEXT_CANDLE_OPEN"
    assert "fill at that bar open" in str(semantics["contiguous_price_gap_rule"])
    assert semantics["unresolved_same_bar_ambiguity_allowed"] is False
    assert semantics["profit_target"].startswith("NO_TARGET_EXIT")
    assert "never label the row a target win" in str(
        semantics["same_bar_target_diagnostic"]
    )
    assert "entry_time_ms < funding_time_ms < exit_time_ms" in str(
        semantics["funding_boundary_rule"]
    )
    assert "present and positive" in str(semantics["funding_return_rule"])
    assert "realized_signed_funding_bps=10000*funding_return_decimal" in str(
        semantics["funding_return_rule"]
    )
    assert semantics["terminal_precedence"] == [
        "contiguous_bar_gap_through_active_stop_at_open",
        "intrabar_active_stop_at_stop_price",
        "shared_max_holding_exit_at_close",
    ]
    premature = semantics["premature_stop_definition"]
    assert "OPEN < INTRABAR < CLOSE" in premature["exit_order_rule"]
    assert "next 12 eligible fully subsequent bars" in premature["rule"]
    assert "same OHLC bar" in premature["same_bar_unordered"]
    assert "strictly less than the baseline exit bar_index" in premature["rebound_bar_rule"]

    metrics = contract["metric_definitions"]
    assert isinstance(metrics, dict)
    assert set(metrics) == {
        "after_cost_return_bps",
        "censor_fraction",
        "cvar_5_return_bps",
        "maximum_drawdown",
        "mean_mfe_giveback_fraction",
        "mean_stop_updates_per_24h_exposure",
        "premature_stop_rate",
        "same_bar_ambiguity_count",
    }
    assert "/10000" in str(metrics["maximum_drawdown"])
    assert "positive drawdown magnitude fraction" in str(metrics["maximum_drawdown"])
    assert "realized_signed_funding_bps=10000*" in str(metrics["after_cost_return_bps"])
    assert "exclude that bar's favorable high/low" in str(
        metrics["mean_mfe_giveback_fraction"]
    )

    evidence = contract["minimum_evidence"]
    assert isinstance(evidence, dict)
    assert "paired-valid primary-position intersection" in evidence["application_rule"]
    assert "distinct UTC entry dates" in evidence["calendar_day_count_rule"]
    assert evidence["selection_requires_both_populations"] is True
    historical_evidence = evidence["historical_harness"]
    external_evidence = evidence["external_shadow"]
    assert historical_evidence["minimum_total_positions"] == 300
    assert historical_evidence["required_context_trend_states"] == [
        "bullish",
        "mixed",
        "bearish",
    ]
    assert external_evidence["minimum_total_positions"] == 30
    assert external_evidence["required_context_trend_states"] == [
        "bullish",
        "mixed",
        "bearish",
    ]

    costs = contract["cost_model"]
    assert isinstance(costs, dict)
    assert costs["execution_cost_source"] == (
        "config/backtest.5m.r2-c0-corrected.yaml:costs"
    )
    assert costs["modeled_stop_update_cost_bps"] == "0.10"
    assert costs["funding"] == "include_realized_usdm_funding_for_each_policy_holding_interval"

    policies = contract["policies"]
    assert isinstance(policies, list)
    confirmed_swing = policies[2]
    assert confirmed_swing["rules"]["atr_candidate_not_active"].startswith("no_stop_update")
    weakening = policies[3]
    assert "even if the normal ATR/R activation threshold" in weakening["rules"]["weakening_state"]

    invariants = contract["policy_common_invariants"]
    assert isinstance(invariants, dict)
    assert invariants["l60_01_source_sha256"] == (
        "5de30eb8406d534ca3033edc0ac0f2376311d6a8d2f96e0bd8e5d4d6c433f018"
    )
    assert invariants["stale_or_uncertain_context"] == "no_stop_update"

    cohort = contract["cohort_authority"]
    assert isinstance(cohort, dict)
    external = cohort["external_shadow"]
    assert isinstance(external, dict)
    assert external["campaign_duration_days"] == 60
    assert "activation_ms+60*86400000" in external["campaign_terminal_rule"]
    assert "fixed 21600000 ms observation tail" in external["campaign_terminal_rule"]
    assert "greatest candle_close_time_ms strictly less than" in external[
        "admission_stratum_rule"
    ]
    assert "transition_ms-candle_close_time_ms<=600000" in external[
        "admission_stratum_rule"
    ]

    precision = contract["numeric_precision"]
    assert isinstance(precision, dict)
    assert precision["decimal_context_precision_digits"] == 34
    assert precision["rounding_mode"] == "ROUND_HALF_EVEN"
    assert precision["intermediate_quantization"] == "none"
    assert "strictly-positive gates require value>Decimal('0')" in precision[
        "pass_fail_comparison"
    ]

    funding = contract["funding_authority"]
    assert isinstance(funding, dict)
    assert funding["endpoint_path"] == "/fapi/v1/fundingRate"
    assert "verify_funding_dataset" in funding["historical_harness_source"]
    assert "do not substitute zero" in funding["missing_or_unverified_rule"]
    assert "complete for the full policy holding interval" in funding["zero_event_rule"]


def test_guardian_policy_selection_bound_source_hashes_match_files() -> None:
    contract = _contract()
    hash_rule = contract["bound_text_source_hash_rule"]
    assert isinstance(hash_rule, str)
    assert "replace each CRLF byte pair with LF" in hash_rule
    cohort = contract["cohort_authority"]
    assert isinstance(cohort, dict)
    historical = cohort["historical_harness"]
    assert isinstance(historical, dict)

    assert _canonical_text_sha256(str(historical["source_path"])) == historical["source_sha256"]
    assert _canonical_text_sha256(str(cohort["protection_context_source_path"])) == (
        cohort["protection_context_source_sha256"]
    )

    funding = contract["funding_authority"]
    assert isinstance(funding, dict)
    assert _canonical_text_sha256(str(funding["historical_source_path"])) == (
        funding["historical_source_sha256"]
    )

    invariants = contract["policy_common_invariants"]
    assert isinstance(invariants, dict)
    assert _canonical_text_sha256(str(invariants["l60_01_source_path"])) == (
        invariants["l60_01_source_sha256"]
    )
