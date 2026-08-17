from __future__ import annotations

import hashlib
from typing import cast

import pytest

from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.protocol.lifecycle import (
    MILLISECONDS_PER_DAY_V2,
    AttemptTerminalStatusV2,
    FixedHorizonV2,
    ProspectiveAttemptV2,
)
from signalbot.r4b_v2.research.prospective_efficacy_contract import (
    PROSPECTIVE_EFFICACY_GATE_SCHEMA_V2,
    PROSPECTIVE_EFFICACY_GATE_STATUS_V2,
    ProspectiveEfficacyContractErrorV2,
    current_prospective_efficacy_gate_document_v2,
    current_prospective_efficacy_gate_sha256_v2,
)

DAY_MS = MILLISECONDS_PER_DAY_V2
H_START_MS = 40_000 * DAY_MS


def _attempt(
    *,
    attempt_index: int = 1,
    terminal_status: AttemptTerminalStatusV2 | None = None,
) -> ProspectiveAttemptV2:
    return ProspectiveAttemptV2(
        attempt_index=attempt_index,
        qualification_start_ms=H_START_MS - 30 * DAY_MS,
        horizon=FixedHorizonV2(h_start_ms=H_START_MS),
        terminal_status=terminal_status,
    )


def test_gate_contract_is_exact_outcome_blind_and_canonically_hashed() -> None:
    attempt = _attempt()
    document = current_prospective_efficacy_gate_document_v2(attempt)
    family_hypotheses = cast(dict[str, object], document["family_hypotheses"])

    assert document["schema_version"] == PROSPECTIVE_EFFICACY_GATE_SCHEMA_V2
    assert document["status"] == PROSPECTIVE_EFFICACY_GATE_STATUS_V2
    assert document["economic_cells"] == {
        "combination_rule": "INTERSECTION_UNION_ALL_CELLS_MUST_PASS",
        "fee_multipliers": ("1.0", "1.5"),
        "quote_notional_usdt": ("100", "1000"),
    }
    assert family_hypotheses["multiplicity"] == (
        "HOLM_STEP_DOWN_ACROSS_A_B_C"
    )
    assert current_prospective_efficacy_gate_sha256_v2(attempt) == hashlib.sha256(
        b"R4B_V2_PROSPECTIVE_EFFICACY_GATE_CONTRACT_V2\0"
        + canonical_json_line(document)
    ).hexdigest()


def test_alpha_spending_changes_attempt_specific_gate_hash() -> None:
    first = _attempt(attempt_index=1)
    second = _attempt(attempt_index=2)
    first_attempt = cast(
        dict[str, object],
        current_prospective_efficacy_gate_document_v2(first)["attempt"],
    )
    second_attempt = cast(
        dict[str, object],
        current_prospective_efficacy_gate_document_v2(second)["attempt"],
    )

    assert current_prospective_efficacy_gate_sha256_v2(first) != (
        current_prospective_efficacy_gate_sha256_v2(second)
    )
    assert first_attempt["one_sided_alpha_denominator"] == 40
    assert second_attempt["one_sided_alpha_denominator"] == 80


def test_terminal_attempt_cannot_receive_a_new_gate_contract() -> None:
    with pytest.raises(ProspectiveEfficacyContractErrorV2, match="terminal"):
        current_prospective_efficacy_gate_document_v2(
            _attempt(terminal_status=AttemptTerminalStatusV2.FAIL)
        )
