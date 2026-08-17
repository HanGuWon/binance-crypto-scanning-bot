from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from signalbot.backtest.downstream_code_freeze import (
    create_downstream_code_freeze_v1,
)
from signalbot.r4b_v2.protocol.prospective_code_freeze import (
    PROSPECTIVE_CODE_FREEZE_PURPOSE_V2,
    ProspectiveCodeFreezeContractErrorV2,
    ProspectiveCodeFreezeReceiptV2,
    canonical_prospective_code_freeze_receipt_v2,
    create_prospective_code_freeze_v2,
    load_prospective_code_freeze_v2,
    validate_prospective_code_freeze_authority_v2,
)

PROMOTING = "a" * 64
EXECUTION = "b" * 64
EFFICACY = "c" * 64
H_START_MS = 2_000_000_000_000
CREATED = datetime(2026, 7, 21, 1, 2, 3, 456000, tzinfo=UTC)


def _workspace(tmp_path: Path) -> Path:
    (tmp_path / "src" / "signalbot").mkdir(parents=True)
    (tmp_path / "tests").mkdir()
    (tmp_path / "src" / "signalbot" / "runtime.py").write_text(
        "VALUE = 1\n",
        encoding="utf-8",
    )
    (tmp_path / "tests" / "test_runtime.py").write_text(
        "def test_value(): assert True\n",
        encoding="utf-8",
    )
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    return tmp_path


def _create(workspace: Path):
    return create_prospective_code_freeze_v2(
        workspace_root=workspace,
        manifest_path="artifacts/prospective-code-freeze.json",
        h_start_ms=H_START_MS,
        promoting_plan_sha256=PROMOTING,
        prospective_execution_contract_sha256=EXECUTION,
        prospective_efficacy_gate_sha256=EFFICACY,
        created_at_utc=CREATED,
    )


def test_exact_broad_freeze_round_trips_and_detects_code_drift(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    receipt = _create(workspace)

    loaded = load_prospective_code_freeze_v2(
        "artifacts/prospective-code-freeze.json",
        workspace_root=workspace,
        expected_manifest_sha256=receipt.manifest_sha256,
        h_start_ms=H_START_MS,
        promoting_plan_sha256=PROMOTING,
        prospective_execution_contract_sha256=EXECUTION,
        prospective_efficacy_gate_sha256=EFFICACY,
    )
    assert loaded == receipt
    assert canonical_prospective_code_freeze_receipt_v2(receipt).endswith(b"\n")

    (workspace / "src" / "signalbot" / "runtime.py").write_text(
        "VALUE = 2\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="hash drift"):
        load_prospective_code_freeze_v2(
            "artifacts/prospective-code-freeze.json",
            workspace_root=workspace,
            expected_manifest_sha256=receipt.manifest_sha256,
            h_start_ms=H_START_MS,
            promoting_plan_sha256=PROMOTING,
            prospective_execution_contract_sha256=EXECUTION,
            prospective_efficacy_gate_sha256=EFFICACY,
        )


def test_narrow_generic_freeze_cannot_be_upgraded_to_prospective(
    tmp_path: Path,
) -> None:
    workspace = _workspace(tmp_path)
    authority = create_downstream_code_freeze_v1(
        workspace_root=workspace,
        manifest_path="artifacts/narrow.json",
        purpose=PROSPECTIVE_CODE_FREEZE_PURPOSE_V2,
        include_trees=("src/signalbot",),
        include_files=("pyproject.toml", "uv.lock"),
        included_suffixes=(".py",),
        upstream_sha256={
            "promoting_plan": PROMOTING,
            "prospective_efficacy_gate": EFFICACY,
            "prospective_execution_contract": EXECUTION,
        },
        created_at_utc=CREATED,
    )

    with pytest.raises(ProspectiveCodeFreezeContractErrorV2, match="policy"):
        validate_prospective_code_freeze_authority_v2(
            authority,
            h_start_ms=H_START_MS,
            promoting_plan_sha256=PROMOTING,
            prospective_execution_contract_sha256=EXECUTION,
            prospective_efficacy_gate_sha256=EFFICACY,
        )


def test_manifest_time_must_precede_h_start(tmp_path: Path) -> None:
    workspace = _workspace(tmp_path)
    created_ms = int(CREATED.timestamp() * 1_000)

    with pytest.raises(ProspectiveCodeFreezeContractErrorV2, match="before H_start"):
        create_prospective_code_freeze_v2(
            workspace_root=workspace,
            manifest_path="artifacts/late.json",
            h_start_ms=created_ms,
            promoting_plan_sha256=PROMOTING,
            prospective_execution_contract_sha256=EXECUTION,
            prospective_efficacy_gate_sha256=EFFICACY,
            created_at_utc=CREATED,
        )


def test_code_freeze_receipt_cannot_be_directly_forged() -> None:
    with pytest.raises(ProspectiveCodeFreezeContractErrorV2, match="validator"):
        ProspectiveCodeFreezeReceiptV2(
            manifest_sha256="d" * 64,
            manifest_created_at_ms=1,
            h_start_ms=2,
            upstream_sha256=(
                ("promoting_plan", PROMOTING),
                ("prospective_efficacy_gate", EFFICACY),
                ("prospective_execution_contract", EXECUTION),
            ),
            _factory_token=object(),
        )
