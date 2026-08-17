from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.paper_fok import PAPER_FOK_RULE_VERSION_V2
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusContractErrorV2,
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
from signalbot.r4b_v2.protocol.lifecycle import (
    MILLISECONDS_PER_DAY_V2,
    FixedHorizonV2,
    ProspectiveAttemptV2,
)
from signalbot.r4b_v2.protocol.prospective_code_freeze import (
    ProspectiveCodeFreezeReceiptV2,
    create_prospective_code_freeze_v2,
)
from signalbot.r4b_v2.research.prospective_efficacy_contract import (
    current_prospective_efficacy_gate_sha256_v2,
)
from signalbot.r4b_v2.strategy.family_a import FAMILY_A_RULE_VERSION_V2
from signalbot.r4b_v2.strategy.family_b import FAMILY_B_RULE_VERSION_V2
from signalbot.r4b_v2.strategy.family_c import FAMILY_C_RULE_VERSION_V2
from signalbot.r4b_v2.strategy.prospective_plan import (
    PROSPECTIVE_EXECUTION_CONTRACT_SCHEMA_V2,
    build_current_prospective_census_plan_v2,
    current_prospective_execution_contract_document_v2,
    current_prospective_execution_contract_sha256_v2,
    current_prospective_family_rules_v2,
)

DAY_MS = MILLISECONDS_PER_DAY_V2
H_START_MS = 30_000 * DAY_MS


def _attempt() -> ProspectiveAttemptV2:
    return ProspectiveAttemptV2(
        attempt_index=1,
        qualification_start_ms=H_START_MS - 30 * DAY_MS,
        horizon=FixedHorizonV2(h_start_ms=H_START_MS),
    )


def _code_freeze(
    tmp_path: Path,
    attempt: ProspectiveAttemptV2,
) -> ProspectiveCodeFreezeReceiptV2:
    (tmp_path / "src" / "signalbot").mkdir(parents=True)
    (tmp_path / "tests").mkdir()
    (tmp_path / "src" / "signalbot" / "runtime.py").write_text(
        "VALUE = 1\n",
        encoding="utf-8",
    )
    (tmp_path / "tests" / "test_runtime.py").write_text(
        "def test_runtime(): assert True\n",
        encoding="utf-8",
    )
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    return create_prospective_code_freeze_v2(
        workspace_root=tmp_path,
        manifest_path="artifacts/freeze.json",
        h_start_ms=attempt.horizon.h_start_ms,
        promoting_plan_sha256="a" * 64,
        prospective_execution_contract_sha256=(current_prospective_execution_contract_sha256_v2()),
        prospective_efficacy_gate_sha256=(current_prospective_efficacy_gate_sha256_v2(attempt)),
        created_at_utc=datetime(2026, 7, 21, tzinfo=UTC),
    )


def test_current_family_bindings_are_exact_and_canonical() -> None:
    rules = current_prospective_family_rules_v2()

    assert tuple((rule.family, rule.rule_version) for rule in rules) == (
        (PromotingFamilyV2.A, FAMILY_A_RULE_VERSION_V2),
        (PromotingFamilyV2.B, FAMILY_B_RULE_VERSION_V2),
        (PromotingFamilyV2.C, FAMILY_C_RULE_VERSION_V2),
    )


def test_execution_contract_hash_is_canonical_and_semantically_complete() -> None:
    document = current_prospective_execution_contract_document_v2()
    digest = current_prospective_execution_contract_sha256_v2()

    assert document["schema_version"] == PROSPECTIVE_EXECUTION_CONTRACT_SCHEMA_V2
    assert set(document) == {
        "actionability",
        "authority_scope",
        "decision_clock",
        "decision_wal",
        "decimal_arithmetic",
        "family_lifecycle",
        "fee",
        "funding",
        "lifecycle",
        "mandatory_exit",
        "outcome_wal",
        "paper_fok",
        "paper_sizing",
        "paper_terminal",
        "schema_version",
    }
    assert document["paper_terminal"] == {
        "entry_terminal_only": True,
        "position_terminal": False,
        "production_order_placement": False,
        "rule_version": PROSPECTIVE_PAPER_TERMINAL_RULE_VERSION_V2,
        "schema_version": PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
    }
    assert document["outcome_wal"] == {
        "position_terminal_typed": False,
        "production_order_placement": False,
        "record_kinds": tuple(kind.value for kind in ProspectiveOutcomeWalRecordKindV2),
        "record_schema_version": PROSPECTIVE_OUTCOME_WAL_RECORD_SCHEMA_V2,
        "rule_version": PROSPECTIVE_OUTCOME_WAL_RULE_VERSION_V2,
        "scope": "ATTEMPT_WIDE_ORIGIN_CELL_REFERENCED",
    }
    assert document["decision_wal"] == {
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
        "rule_version": "R4B_CAUSAL_V2.4.0_PROSPECTIVE_DECISION_WAL",
        "suppressed_distinct_from_no_signal": True,
        "typed_payload_parser_available": True,
    }
    assert (
        digest
        == hashlib.sha256(
            b"R4B_V2_PROSPECTIVE_EXECUTION_CONTRACT_V2\0" + canonical_json_line(document)
        ).hexdigest()
    )


def test_builder_binds_code_freeze_current_rules_and_canonical_symbol_set(
    tmp_path: Path,
) -> None:
    attempt = _attempt()
    freeze = _code_freeze(tmp_path, attempt)
    forward = build_current_prospective_census_plan_v2(
        attempt_id="attempt-001",
        attempt=attempt,
        promoting_plan_sha256="a" * 64,
        code_freeze_receipt=freeze,
        symbols=("BTCUSDT", "ETHUSDT", "SOLUSDT"),
        context_symbols=(
            *(f"C{index:02d}USDT" for index in range(20)),
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
        ),
        created_at_ms=H_START_MS - 1,
    )
    reversed_order = build_current_prospective_census_plan_v2(
        attempt_id="attempt-001",
        attempt=attempt,
        promoting_plan_sha256="a" * 64,
        code_freeze_receipt=freeze,
        symbols=("SOLUSDT", "ETHUSDT", "BTCUSDT"),
        context_symbols=(
            "SOLUSDT",
            "ETHUSDT",
            "BTCUSDT",
            *(f"C{index:02d}USDT" for index in reversed(range(20))),
        ),
        created_at_ms=H_START_MS - 1,
    )

    assert forward.plan_sha256 == reversed_order.plan_sha256
    assert forward.family_rules == current_prospective_family_rules_v2()
    assert forward.paper_fok_rule_version == PAPER_FOK_RULE_VERSION_V2
    assert forward.execution_contract_sha256 == current_prospective_execution_contract_sha256_v2()
    assert forward.strategy_code_freeze_manifest_sha256 == freeze.manifest_sha256
    assert forward.efficacy_gate_contract_sha256 == (
        current_prospective_efficacy_gate_sha256_v2(forward.attempt)
    )


def test_builder_rejects_too_small_family_c_context_universe(
    tmp_path: Path,
) -> None:
    attempt = _attempt()
    with pytest.raises(ProspectiveCensusContractErrorV2, match="at least 20"):
        build_current_prospective_census_plan_v2(
            attempt_id="attempt-001",
            attempt=attempt,
            promoting_plan_sha256="a" * 64,
            code_freeze_receipt=_code_freeze(tmp_path, attempt),
            symbols=("BTCUSDT", "ETHUSDT"),
            context_symbols=("BTCUSDT", "ETHUSDT"),
            created_at_ms=H_START_MS - 1,
        )
