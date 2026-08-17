from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

import signalbot.r4b_v2.execution.prospective_attempt_ledger as ledger_module
from signalbot.r4b_v2.alerts.actionability import (
    PRIMARY_PAPER_TARGET_DELAY_MS_V2,
)
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.execution.prospective_attempt_ledger import (
    PROSPECTIVE_ATTEMPT_LEDGER_AUTHORITY_V1,
    PROSPECTIVE_ATTEMPT_LEDGER_NONCLAIMS_V1,
    ProspectiveAttemptLedgerCapacityErrorV1,
    ProspectiveAttemptLedgerCheckpointV1,
    ProspectiveAttemptLedgerConflictErrorV1,
    ProspectiveAttemptLedgerIntegrityErrorV1,
    ProspectiveAttemptLedgerV1,
    ProspectiveAttemptLedgerWriteErrorV1,
    _new_record_v1,
    prospective_decision_cell_v1,
)
from signalbot.r4b_v2.protocol.decision_clock import DECISION_DELAY_MS_V2

CLOSE_MS = 299_999
SOURCE_ROOT = "a" * 64
OTHER_SOURCE_ROOT = "b" * 64
EXECUTION_CONTRACT = "c" * 64
SIGNAL_PAYLOAD = "d" * 64
EVIDENCE_ROOT = "e" * 64


def _ledger(
    tmp_path: Path,
    *,
    max_cells: int = 8,
    expected_checkpoint: ProspectiveAttemptLedgerCheckpointV1 | None = None,
    fault_hook=None,
):
    return ProspectiveAttemptLedgerV1(
        tmp_path / "prospective-attempts.jsonl",
        max_cells=max_cells,
        expected_checkpoint=expected_checkpoint,
        fault_hook=fault_hook,
    )


def _record_decision(
    ledger: ProspectiveAttemptLedgerV1,
    *,
    symbol: str = "BTCUSDT",
    close_ms: int = CLOSE_MS,
    decision: str = "SIGNAL_LONG",
    source_root: str = SOURCE_ROOT,
):
    signal = decision != "NO_SIGNAL"
    return ledger.record_decision(
        attempt_id="attempt-prospective-001",
        rule_version="rule-v1",
        universe_version="universe-v1",
        symbol=symbol,
        closed_5m_close_ms=close_ms,
        decision_cutoff_ms=close_ms + DECISION_DELAY_MS_V2,
        decision=decision,  # type: ignore[arg-type]
        source_root_sha256=source_root,
        execution_contract_sha256=EXECUTION_CONTRACT,
        decision_event_id=f"event-{symbol}-{close_ms}" if signal else None,
        payload_sha256=SIGNAL_PAYLOAD if signal else None,
    )


def _record_valid_terminal(
    ledger: ProspectiveAttemptLedgerV1,
    *,
    close_ms: int = CLOSE_MS,
    outcome: str = "FULL_FILL",
    reason: str = "first post-target executable BBO admitted",
):
    target_ms = (
        close_ms + DECISION_DELAY_MS_V2 + PRIMARY_PAPER_TARGET_DELAY_MS_V2
    )
    exchange_event_ms = target_ms + 5
    evaluation_wall_ms = target_ms + 15
    return ledger.record_execution_terminal(
        attempt_id="attempt-prospective-001",
        rule_version="rule-v1",
        universe_version="universe-v1",
        symbol="BTCUSDT",
        closed_5m_close_ms=close_ms,
        target_ms=target_ms,
        outcome=outcome,  # type: ignore[arg-type]
        evidence_state="VALID",
        evidence_locator=f"bbo://BTCUSDT/{target_ms}",
        evidence_root_sha256=EVIDENCE_ROOT,
        evidence_exchange_event_ms=exchange_event_ms,
        evidence_receipt_wall_ms=target_ms + 8,
        evidence_receipt_monotonic_ns=1_000_000,
        evidence_evaluation_wall_ms=evaluation_wall_ms,
        evidence_age_ms=evaluation_wall_ms - exchange_event_ms,
        freshness_limit_ms=10,
        reason=reason,
    )


def test_decision_census_is_canonical_hash_chained_and_nonexecuting(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    long_cell = _record_decision(ledger)
    short_cell = _record_decision(
        ledger,
        symbol="ETHUSDT",
        close_ms=CLOSE_MS + 300_000,
        decision="SIGNAL_SHORT",
    )
    no_signal = _record_decision(
        ledger,
        symbol="SOLUSDT",
        close_ms=CLOSE_MS + 600_000,
        decision="NO_SIGNAL",
    )

    assert ledger.cell_count == 3
    assert ledger.record_count == 3
    assert ledger.execution_terminal_count == 0
    assert ledger.pending_signal_count == 2
    assert long_cell.cell_id != short_cell.cell_id != no_signal.cell_id
    assert no_signal.decision_event_id is None
    assert no_signal.payload_sha256 is None
    assert ledger.records[0].previous_record_sha256 is None
    assert ledger.records[1].previous_record_sha256 == ledger.records[0].record_sha256
    assert ledger.tip_sha256 == ledger.records[-1].record_sha256
    for record in ledger.records:
        assert record.encoded_line == canonical_json_line(
            ledger_module._record_document_v1(record, include_hash=True)
        )
        assert record.payload.paper_fok_invoked is False
        assert record.payload.pnl_computed is False
        assert record.payload.network_accessed is False
        assert record.payload.order_execution_enabled is False
    assert PROSPECTIVE_ATTEMPT_LEDGER_NONCLAIMS_V1 == (
        "NO_PAPER_FOK_NO_PNL_NO_NETWORK_NO_ORDER"
    )
    assert PROSPECTIVE_ATTEMPT_LEDGER_AUTHORITY_V1 == (
        "NONAUTHORITATIVE_SERIALIZATION_PROTOTYPE"
    )


def test_decision_requires_closed_boundary_time_and_exact_signal_fields(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    common: dict[str, Any] = {
        "attempt_id": "attempt-prospective-001",
        "rule_version": "rule-v1",
        "universe_version": "universe-v1",
        "symbol": "BTCUSDT",
        "closed_5m_close_ms": CLOSE_MS,
        "decision_cutoff_ms": CLOSE_MS + DECISION_DELAY_MS_V2,
        "decision": "SIGNAL_LONG",
        "source_root_sha256": SOURCE_ROOT,
        "execution_contract_sha256": EXECUTION_CONTRACT,
        "decision_event_id": "event-btc-long",
        "payload_sha256": SIGNAL_PAYLOAD,
    }
    with pytest.raises(ValueError, match="five-minute boundary"):
        ledger.record_decision(**{**common, "closed_5m_close_ms": CLOSE_MS - 1})
    with pytest.raises(ValueError, match="must equal"):
        ledger.record_decision(
            **{**common, "decision_cutoff_ms": CLOSE_MS + DECISION_DELAY_MS_V2 + 1}
        )
    with pytest.raises(ValueError, match="decision_event_id"):
        ledger.record_decision(**{**common, "decision_event_id": None})
    with pytest.raises(ValueError, match="NO_SIGNAL forbids"):
        ledger.record_decision(**{**common, "decision": "NO_SIGNAL"})
    assert ledger.record_count == 0


def test_exact_decision_replay_is_noop_conflict_rejects_and_cap_never_prunes(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path, max_cells=1)
    first = _record_decision(ledger)
    original = ledger.journal_path.read_bytes()
    replay = _record_decision(ledger)
    assert replay is first
    assert ledger.journal_path.read_bytes() == original
    with pytest.raises(ProspectiveAttemptLedgerConflictErrorV1, match="different facts"):
        _record_decision(ledger, source_root=OTHER_SOURCE_ROOT)
    with pytest.raises(ProspectiveAttemptLedgerCapacityErrorV1, match="bound"):
        _record_decision(
            ledger,
            symbol="ETHUSDT",
            close_ms=CLOSE_MS + 300_000,
        )
    assert ledger.cell_count == 1
    assert ledger.record_count == 1
    assert ledger.journal_path.read_bytes() == original


def test_signal_has_exactly_one_terminal_with_boundary_fresh_evidence(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    decision = _record_decision(ledger)
    terminal = _record_valid_terminal(ledger)
    original = ledger.journal_path.read_bytes()
    replay = _record_valid_terminal(ledger)

    assert replay is terminal
    assert terminal.cell_id == decision.cell_id
    assert terminal.decision_sha256 == decision.decision_sha256
    assert terminal.evidence_age_ms == terminal.freshness_limit_ms
    assert ledger.record_count == 2
    assert ledger.execution_terminal_count == 1
    assert ledger.pending_signal_count == 0
    assert ledger.journal_path.read_bytes() == original
    with pytest.raises(ProspectiveAttemptLedgerConflictErrorV1, match="different facts"):
        _record_valid_terminal(ledger, reason="conflicting replay")


def test_unknown_and_no_signal_cells_forbid_execution_terminal(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    with pytest.raises(ProspectiveAttemptLedgerConflictErrorV1, match="prior exact"):
        _record_valid_terminal(ledger)
    _record_decision(ledger, decision="NO_SIGNAL")
    with pytest.raises(ProspectiveAttemptLedgerConflictErrorV1, match="NO_SIGNAL"):
        _record_valid_terminal(ledger)
    assert ledger.execution_terminal_count == 0


@pytest.mark.parametrize("evidence_state", ["MISSING", "GAP"])
def test_missing_and_gap_force_inconclusive_without_valid_locator(
    tmp_path: Path,
    evidence_state: str,
) -> None:
    ledger = _ledger(tmp_path)
    _record_decision(ledger)
    target_ms = (
        CLOSE_MS + DECISION_DELAY_MS_V2 + PRIMARY_PAPER_TARGET_DELAY_MS_V2
    )
    evidence_locator = (
        "bbo://BTCUSDT/gap" if evidence_state == "GAP" else None
    )
    evidence_root = EVIDENCE_ROOT if evidence_state == "GAP" else None
    arguments: dict[str, Any] = {
        "attempt_id": "attempt-prospective-001",
        "rule_version": "rule-v1",
        "universe_version": "universe-v1",
        "symbol": "BTCUSDT",
        "closed_5m_close_ms": CLOSE_MS,
        "target_ms": target_ms,
        "outcome": "FULL_FILL",
        "evidence_state": evidence_state,
        "evidence_locator": evidence_locator,
        "evidence_root_sha256": evidence_root,
        "evidence_exchange_event_ms": None,
        "evidence_receipt_wall_ms": None,
        "evidence_receipt_monotonic_ns": None,
        "evidence_evaluation_wall_ms": None,
        "evidence_age_ms": None,
        "freshness_limit_ms": 250,
        "reason": f"{evidence_state.casefold()} first-post-target evidence",
    }
    with pytest.raises(ValueError, match="forces INCONCLUSIVE"):
        ledger.record_execution_terminal(**arguments)
    terminal = ledger.record_execution_terminal(
        **{**arguments, "outcome": "INCONCLUSIVE"}
    )
    assert terminal.outcome == "INCONCLUSIVE"
    assert terminal.evidence_locator == evidence_locator


def test_stale_evidence_forces_inconclusive_and_requires_true_staleness(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    _record_decision(ledger)
    target_ms = (
        CLOSE_MS + DECISION_DELAY_MS_V2 + PRIMARY_PAPER_TARGET_DELAY_MS_V2
    )
    exchange_event_ms = target_ms + 1
    arguments: dict[str, Any] = {
        "attempt_id": "attempt-prospective-001",
        "rule_version": "rule-v1",
        "universe_version": "universe-v1",
        "symbol": "BTCUSDT",
        "closed_5m_close_ms": CLOSE_MS,
        "target_ms": target_ms,
        "outcome": "FULL_FILL",
        "evidence_state": "STALE",
        "evidence_locator": "bbo://BTCUSDT/stale",
        "evidence_root_sha256": EVIDENCE_ROOT,
        "evidence_exchange_event_ms": exchange_event_ms,
        "evidence_receipt_wall_ms": target_ms + 2,
        "evidence_receipt_monotonic_ns": 1_000_000,
        "evidence_evaluation_wall_ms": exchange_event_ms + 11,
        "evidence_age_ms": 11,
        "freshness_limit_ms": 10,
        "reason": "first post-target BBO exceeded freshness",
    }
    with pytest.raises(ValueError, match="forces INCONCLUSIVE"):
        ledger.record_execution_terminal(**arguments)
    terminal = ledger.record_execution_terminal(
        **{**arguments, "outcome": "INCONCLUSIVE"}
    )
    assert terminal.evidence_state == "STALE"

    boundary_path = tmp_path / "boundary"
    boundary_path.mkdir()
    other = _ledger(boundary_path)
    _record_decision(other)
    with pytest.raises(ValueError, match="does not exceed"):
        other.record_execution_terminal(
            **{
                **arguments,
                "outcome": "INCONCLUSIVE",
                "evidence_evaluation_wall_ms": exchange_event_ms + 10,
                "evidence_age_ms": 10,
            }
        )


def test_terminal_rejects_pre_target_and_inconsistent_evidence_clocks(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    _record_decision(ledger)
    with pytest.raises(ValueError, match="target_ms must equal"):
        ledger.record_execution_terminal(
            attempt_id="attempt-prospective-001",
            rule_version="rule-v1",
            universe_version="universe-v1",
            symbol="BTCUSDT",
            closed_5m_close_ms=CLOSE_MS,
            target_ms=CLOSE_MS + DECISION_DELAY_MS_V2,
            outcome="INCONCLUSIVE",
            evidence_state="MISSING",
            evidence_locator=None,
            evidence_root_sha256=None,
            evidence_exchange_event_ms=None,
            evidence_receipt_wall_ms=None,
            evidence_receipt_monotonic_ns=None,
            evidence_evaluation_wall_ms=None,
            evidence_age_ms=None,
            freshness_limit_ms=10,
            reason="target boundary invalid",
        )
    target = CLOSE_MS + DECISION_DELAY_MS_V2 + PRIMARY_PAPER_TARGET_DELAY_MS_V2
    with pytest.raises(ValueError, match="evidence_age_ms differs"):
        ledger.record_execution_terminal(
            attempt_id="attempt-prospective-001",
            rule_version="rule-v1",
            universe_version="universe-v1",
            symbol="BTCUSDT",
            closed_5m_close_ms=CLOSE_MS,
            target_ms=target,
            outcome="FULL_FILL",
            evidence_state="VALID",
            evidence_locator="bbo://BTCUSDT/age-mismatch",
            evidence_root_sha256=EVIDENCE_ROOT,
            evidence_exchange_event_ms=target + 1,
            evidence_receipt_wall_ms=target + 2,
            evidence_receipt_monotonic_ns=1,
            evidence_evaluation_wall_ms=target + 6,
            evidence_age_ms=4,
            freshness_limit_ms=10,
            reason="age mismatch",
        )


def test_restart_replays_exact_chain_pending_state_and_enforces_new_bound(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path, max_cells=3)
    _record_decision(ledger)
    _record_valid_terminal(ledger, outcome="NO_FILL")
    _record_decision(
        ledger,
        symbol="ETHUSDT",
        close_ms=CLOSE_MS + 300_000,
        decision="SIGNAL_SHORT",
    )
    expected_records = ledger.records
    expected_tip = ledger.tip_sha256
    checkpoint = ledger.checkpoint
    assert checkpoint is not None

    reopened = _ledger(
        tmp_path,
        max_cells=3,
        expected_checkpoint=checkpoint,
    )
    assert reopened.records == expected_records
    assert reopened.tip_sha256 == expected_tip
    assert reopened.cell_count == 2
    assert reopened.execution_terminal_count == 1
    assert reopened.pending_signal_count == 1
    with pytest.raises(ProspectiveAttemptLedgerCapacityErrorV1, match="max_cells"):
        _ledger(
            tmp_path,
            max_cells=1,
            expected_checkpoint=checkpoint,
        )


@pytest.mark.parametrize(
    "mutation",
    ["truncate", "full_suffix", "noncanonical", "tamper", "reorder"],
)
def test_reload_rejects_truncation_noncanonical_tamper_and_reorder(
    tmp_path: Path,
    mutation: str,
) -> None:
    ledger = _ledger(tmp_path)
    _record_decision(ledger)
    _record_decision(
        ledger,
        symbol="ETHUSDT",
        close_ms=CLOSE_MS + 300_000,
    )
    checkpoint = ledger.checkpoint
    assert checkpoint is not None
    lines = ledger.journal_path.read_bytes().splitlines(keepends=True)
    if mutation == "truncate":
        encoded = b"".join(lines)[:-1]
    elif mutation == "full_suffix":
        encoded = lines[0]
    elif mutation == "noncanonical":
        encoded = b" " + lines[0] + lines[1]
    elif mutation == "tamper":
        encoded = b"".join(lines).replace(b"BTCUSDT", b"XRPUSDT", 1)
    else:
        encoded = lines[1] + lines[0]
    ledger.journal_path.write_bytes(encoded)
    with pytest.raises(ProspectiveAttemptLedgerIntegrityErrorV1):
        _ledger(tmp_path, expected_checkpoint=checkpoint)


@pytest.mark.parametrize("conflicting", [False, True])
def test_reload_rejects_recomputed_duplicate_or_conflicting_cell(
    tmp_path: Path,
    conflicting: bool,
) -> None:
    ledger = _ledger(tmp_path)
    decision = _record_decision(ledger)
    checkpoint = ledger.checkpoint
    assert checkpoint is not None
    payload = (
        prospective_decision_cell_v1(
            attempt_id=decision.attempt_id,
            rule_version=decision.rule_version,
            universe_version=decision.universe_version,
            symbol=decision.symbol,
            closed_5m_close_ms=decision.closed_5m_close_ms,
            decision_cutoff_ms=decision.decision_cutoff_ms,
            decision=decision.decision,
            source_root_sha256=OTHER_SOURCE_ROOT,
            execution_contract_sha256=decision.execution_contract_sha256,
            decision_event_id=decision.decision_event_id,
            payload_sha256=decision.payload_sha256,
        )
        if conflicting
        else decision
    )
    duplicate = _new_record_v1(
        sequence=2,
        previous_record_sha256=ledger.tip_sha256,
        payload=payload,
    )
    with ledger.journal_path.open("ab") as handle:
        handle.write(duplicate.encoded_line)
        handle.flush()
        os.fsync(handle.fileno())
    with pytest.raises(
        ProspectiveAttemptLedgerIntegrityErrorV1,
        match="duplicate/conflicting decision",
    ):
        _ledger(tmp_path, expected_checkpoint=checkpoint)


def test_append_fault_occurs_before_memory_mutation_and_restart_replays_disk(
    tmp_path: Path,
) -> None:
    def fault(point: str) -> None:
        if point == "after_fsync":
            raise RuntimeError("injected post-fsync crash")

    ledger = _ledger(tmp_path, fault_hook=fault)
    with pytest.raises(ProspectiveAttemptLedgerWriteErrorV1, match="made durable"):
        _record_decision(ledger)
    assert ledger.cell_count == 0
    assert ledger.record_count == 0
    with pytest.raises(ProspectiveAttemptLedgerWriteErrorV1, match="poisoned"):
        _record_decision(ledger)

    with pytest.raises(
        ProspectiveAttemptLedgerIntegrityErrorV1,
        match="requires its checkpoint",
    ):
        _ledger(tmp_path)


def test_short_write_poisoning_leaves_no_admitted_memory_and_rejects_empty_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def short_write(_handle: object, encoded: bytes) -> int:
        return len(encoded) - 1

    monkeypatch.setattr(ledger_module, "_write_exact", short_write)
    ledger = _ledger(tmp_path)
    with pytest.raises(ProspectiveAttemptLedgerWriteErrorV1, match="short"):
        _record_decision(ledger)
    assert ledger.cell_count == 0
    assert ledger.record_count == 0
    with pytest.raises(
        ProspectiveAttemptLedgerIntegrityErrorV1,
        match="empty or truncated",
    ):
        _ledger(tmp_path)


def test_existing_hardlink_is_rejected_before_replay(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _record_decision(ledger)
    checkpoint = ledger.checkpoint
    assert checkpoint is not None
    alias = tmp_path / "journal-alias.jsonl"
    try:
        os.link(ledger.journal_path, alias)
    except OSError as exc:
        pytest.skip(f"hard links are unavailable: {exc}")
    with pytest.raises(ProspectiveAttemptLedgerIntegrityErrorV1, match="one hard link"):
        _ledger(tmp_path, expected_checkpoint=checkpoint)
