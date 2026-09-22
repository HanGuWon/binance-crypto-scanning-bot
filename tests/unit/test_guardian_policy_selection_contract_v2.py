from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

from signalbot.config import load_settings

V1_PATH = Path("config/guardian-policy-selection.v1.json")
V2_PATH = Path("config/guardian-policy-selection.v2.json")
DOMAIN_SEPARATOR = b"GUARDIAN_POLICY_SELECTION_CONTRACT_V2\0"
EXPECTED_CONTRACT_SHA256 = "23140ebd342ccf5e2b6c1ba9a6f8b180ece420cf8277db7fe944c0b3790fefb7"
POSITIONS_SOURCE = Path("src/signalbot/signals/positions.py")
SETTINGS_SOURCE = Path("config/settings.example.yaml")
EXPECTED_POSITIONS_SHA256 = "fab4a5198b1ebd065f7ab4848c3898c7f5fa35b421580d697d74461d5f1d0beb"
EXPECTED_SETTINGS_SHA256 = "2889de620152b22821281d031e7532afef885897321850721fd28bd15da25843"
EXPECTED_EFFECTIVE_SETTINGS_SHA256 = (
    "b01aa1f2434feb03f1fce70866059c21dcca613235f2e764874739f36ba31839"
)
ENTRY_PRODUCER_GIT_COMMIT = "2bb0d1eb075a6cf34d094cacfd902abb939d7bf1"
EXPECTED_ENTRY_PRODUCER_SHA256 = {
    "src/signalbot/backtest/config.py": (
        "6844f58a25b6686b464c8106264213c78b9e82bb557fdbde6cb1969bc13d21e2"
    ),
    "src/signalbot/backtest/engine.py": (
        "76fc746916d52c9a6e3cdf61e9b60ce494e27273a28714128e5cbe4a35a1f963"
    ),
    "src/signalbot/data/microstructure.py": (
        "bf76b8206b154b0ed2be39848e6f3abed2719d891f3a0228e73d87980f24d0f8"
    ),
    "src/signalbot/indicators/core.py": (
        "7ac200b898bcf7afc8834500cec34b620c4666318ff6d093d5060cac03e9fd6e"
    ),
    "src/signalbot/indicators/structure.py": (
        "fc29b1e15bc6e2a077293f924379d8188fa532491732da27a6f1577d5e737d05"
    ),
    "src/signalbot/signals/gates.py": (
        "b34da1259d5e8c1c1201d1e0b19c471704554244cd370405fb911e572f9d0924"
    ),
    "src/signalbot/signals/rules.py": (
        "cfd7eb9c95094e259984ad7649dd28d1f0ea8f10d78c5641140758bb7595a72d"
    ),
    "src/signalbot/signals/state_machine.py": (
        "a0b8a792088bf057c7c543eed2d71f9256c815e517282129bdd5a08b222b7bc7"
    ),
}


def _load(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
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


def _canonical_text_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def test_v2_contract_identity_is_frozen_after_review() -> None:
    contract = _load(V2_PATH)

    assert contract["schema_version"] == "guardian_policy_selection_preregistration_v2"
    assert contract["contract_version"] == "guardian-policy-selection-v2"
    assert contract["status"] == "PREREGISTERED_OUTCOME_BLIND"
    assert _contract_sha256(contract) == EXPECTED_CONTRACT_SHA256


def test_v2_changes_only_explicit_historical_authorities_from_frozen_v1() -> None:
    v1 = _load(V1_PATH)
    v2 = _load(V2_PATH)

    assert v2["parent_contract"] == {
        "contract_version": "guardian-policy-selection-v1",
        "semantic_sha256": "2e19fd7a597dd78a0372753da50fd54dcfacceea6e9482bb34aac606c507a923",
    }
    amendment_scope = str(v2["amendment_scope"])
    assert "historical producer authority" in amendment_scope
    assert "exact historical Settings authority" in amendment_scope
    assert "deterministic L60-03 historical execution conventions" in amendment_scope

    producer = v2["historical_entry_producer_authority"]
    assert isinstance(producer, dict)
    assert producer["git_commit"] == ENTRY_PRODUCER_GIT_COMMIT
    assert producer["source_sha256"] == EXPECTED_ENTRY_PRODUCER_SHA256

    authority = v2["historical_momentum_weakening_authority"]
    assert isinstance(authority, dict)
    assert authority == {
        "rule_id": "technical_exit_one_bar_trend_failure_v1",
        "owner": "signalbot.signals.positions.TechnicalExitEngine.after_close",
        "source_path": "src/signalbot/signals/positions.py",
        "source_sha256": EXPECTED_POSITIONS_SHA256,
        "clock": (
            "evaluate only the FeatureSnapshot for the fully closed 5m candle whose "
            "close-time policy update can first become active at the next contiguous "
            "5m candle open"
        ),
        "long_rule": "feature.price < feature.ema20 and feature.macd_histogram < 0",
        "short_rule": "feature.price > feature.ema20 and feature.macd_histogram > 0",
        "state_rule": (
            "this is the one-bar failed boolean from TechnicalExitEngine.after_close; "
            "do not require trend_failure_count>=trend_failure_bars and do not include "
            "opposite-signal logic"
        ),
        "structure_pairing": (
            "confirmed structure and momentum_weakened must come from the same fully "
            "closed causal FeatureSnapshot used for that close-time policy update"
        ),
        "missing_feature_rule": (
            "if the causal FeatureSnapshot needed for the close-time update is "
            "unavailable, mark Guardian context not ready and create no stop update; "
            "do not coerce unavailable to false"
        ),
        "future_data_forbidden": True,
    }

    cohort = v2["cohort_authority"]
    assert isinstance(cohort, dict)
    historical = cohort["historical_harness"]
    assert isinstance(historical, dict)
    assert historical["settings_path"] == "config/settings.example.yaml"
    assert historical["settings_sha256"] == EXPECTED_SETTINGS_SHA256
    assert historical["effective_settings_sha256"] == EXPECTED_EFFECTIVE_SETTINGS_SHA256
    assert "before any historical market-data read" in str(historical["settings_authority_rule"])

    normalized_v2 = json.loads(json.dumps(v2))
    normalized_v2["schema_version"] = v1["schema_version"]
    normalized_v2["contract_version"] = v1["contract_version"]
    del normalized_v2["parent_contract"]
    del normalized_v2["amendment_scope"]
    del normalized_v2["historical_momentum_weakening_authority"]
    del normalized_v2["historical_entry_producer_authority"]
    del normalized_v2["historical_diagnostic_strata"]
    del normalized_v2["historical_execution_semantics"]
    normalized_historical = normalized_v2["cohort_authority"]["historical_harness"]
    del normalized_historical["settings_authority_rule"]
    del normalized_historical["settings_path"]
    del normalized_historical["settings_sha256"]
    del normalized_historical["effective_settings_hash_rule"]
    del normalized_historical["effective_settings_sha256"]
    normalized_premature = normalized_v2["outcome_semantics"]["premature_stop_definition"]
    del normalized_premature["candidate_exit_atr_rule"]
    del normalized_premature["missing_candidate_exit_atr_rule"]
    assert normalized_v2 == v1


def test_v2_freezes_historical_execution_conventions_without_threshold_changes() -> None:
    contract = _load(V2_PATH)
    semantics = contract["historical_execution_semantics"]
    assert isinstance(semantics, dict)

    assert "Trade.entry_execution_price" in str(semantics["common_entry_execution_rule"])
    assert "raw_entry_price" in str(semantics["entry_quantity_rule"])
    assert "common_entry_execution_price" in str(semantics["mfe_bps_rule"])
    assert "policy_exit_price" in str(semantics["mfe_realized_directional_return_rule"])
    assert semantics["exit_timestamp_rule"] == (
        "OPEN exit => candle.open_time_ms; INTRABAR stop exit => candle.close_time_ms; "
        "CLOSE/max-holding exit => candle.close_time_ms; this deterministic timestamp "
        "is used for strict-interior funding, exposure duration, and drawdown ordering, "
        "while exit phase separately preserves OPEN<INTRABAR<CLOSE diagnostic ordering"
    )


def test_v2_momentum_source_identity_matches_worktree_and_git_lf_blob() -> None:
    contract = _load(V2_PATH)
    authority = contract["historical_momentum_weakening_authority"]
    assert isinstance(authority, dict)

    assert _canonical_text_sha256(POSITIONS_SOURCE) == EXPECTED_POSITIONS_SHA256
    git_blob = subprocess.check_output(
        ["git", "cat-file", "blob", "HEAD:src/signalbot/signals/positions.py"]
    )
    assert hashlib.sha256(git_blob).hexdigest() == EXPECTED_POSITIONS_SHA256
    assert authority["source_sha256"] == EXPECTED_POSITIONS_SHA256


def test_v2_settings_identity_matches_worktree_and_git_lf_blob() -> None:
    contract = _load(V2_PATH)
    cohort = contract["cohort_authority"]
    assert isinstance(cohort, dict)
    historical = cohort["historical_harness"]
    assert isinstance(historical, dict)

    assert _canonical_text_sha256(SETTINGS_SOURCE) == EXPECTED_SETTINGS_SHA256
    git_blob = subprocess.check_output(
        ["git", "cat-file", "blob", "HEAD:config/settings.example.yaml"]
    )
    assert hashlib.sha256(git_blob).hexdigest() == EXPECTED_SETTINGS_SHA256
    assert historical["settings_sha256"] == EXPECTED_SETTINGS_SHA256

    settings = load_settings(SETTINGS_SOURCE)
    canonical = json.dumps(
        settings.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    assert hashlib.sha256(canonical).hexdigest() == EXPECTED_EFFECTIVE_SETTINGS_SHA256
    assert historical["effective_settings_sha256"] == EXPECTED_EFFECTIVE_SETTINGS_SHA256


def test_v2_entry_producer_sources_match_worktree_and_frozen_git_blobs() -> None:
    contract = _load(V2_PATH)
    producer = contract["historical_entry_producer_authority"]
    assert isinstance(producer, dict)
    assert producer["git_commit"] == ENTRY_PRODUCER_GIT_COMMIT
    assert producer["source_sha256"] == EXPECTED_ENTRY_PRODUCER_SHA256

    for source_path, expected_sha in EXPECTED_ENTRY_PRODUCER_SHA256.items():
        assert _canonical_text_sha256(Path(source_path)) == expected_sha
        git_blob = subprocess.check_output(
            ["git", "cat-file", "blob", f"{ENTRY_PRODUCER_GIT_COMMIT}:{source_path}"]
        )
        assert hashlib.sha256(git_blob).hexdigest() == expected_sha


def test_v2_freezes_causal_candidate_exit_atr_and_missing_context_behavior() -> None:
    contract = _load(V2_PATH)
    outcome = contract["outcome_semantics"]
    assert isinstance(outcome, dict)
    premature = outcome["premature_stop_definition"]
    assert isinstance(premature, dict)

    assert "strictly before the candidate exit bar" in str(premature["candidate_exit_atr_rule"])
    assert "primary-censor" in str(premature["missing_candidate_exit_atr_rule"])
    assert "never carry an older stale ATR" in str(premature["missing_candidate_exit_atr_rule"])


def test_v2_freezes_report_only_historical_strata_before_outcomes() -> None:
    contract = _load(V2_PATH)
    strata = contract["historical_diagnostic_strata"]
    assert isinstance(strata, dict)
    assert strata["role"] == "REPORT_ONLY_NOT_SELECTION_OR_GUARDRAIL"
    dimensions = strata["dimensions"]
    assert isinstance(dimensions, dict)
    assert set(dimensions) == {
        "direction",
        "regime",
        "time_of_day_utc",
        "volatility_atr_percent",
    }
    assert "low_lt_0_50" in str(dimensions["volatility_atr_percent"])
    assert "utc_00_08" in str(dimensions["time_of_day_utc"])
    assert "never admit, remove, or reweight" in str(strata["population_rule"])
    assert "do not change bins" in str(strata["anti_tuning_rule"])


def test_v2_momentum_rules_match_existing_technical_exit_predicate_text() -> None:
    source = POSITIONS_SOURCE.read_text(encoding="utf-8")

    assert "feature.price < feature.ema20 and feature.macd_histogram < 0" in source
    assert "feature.price > feature.ema20 and feature.macd_histogram > 0" in source
    assert (
        "position.trend_failure_count = position.trend_failure_count + 1 if failed else 0" in source
    )
