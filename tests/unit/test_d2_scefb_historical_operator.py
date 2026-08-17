from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from signalbot.backtest import d1_scefb_historical_attempt_wal as attempt_wal
from signalbot.backtest import d2_scefb_historical_development as development
from signalbot.backtest import d2_scefb_historical_operator as subject


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _configure_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(subject, "D2_OPERATOR_INPUT_AUTHORITY_DIR_V0", "authority")
    monkeypatch.setattr(
        subject,
        "D2_OPERATOR_FREEZE_MANIFEST_V0",
        "freeze/freeze_manifest.json",
    )
    monkeypatch.setattr(subject, "D2_OPERATOR_ATTEMPT_DIR_V0", "attempt")
    monkeypatch.setattr(subject, "D2_OPERATOR_OUTPUT_DIR_V0", "output")
    monkeypatch.setattr(subject, "D2_OPERATOR_FAILURE_RECEIPT_DIR_V0", "failure")
    monkeypatch.setattr(subject, "_validate_metadata_projection_sources_v0", lambda _root: None)
    monkeypatch.setattr(subject, "_validate_protocol_documents_v0", lambda _root: None)
    assert tmp_path.is_absolute()


def _freeze(authority_sha256: str) -> development.D2HistoricalDevelopmentFreezeV0:
    return development.D2HistoricalDevelopmentFreezeV0(
        manifest_sha256="a" * 64,
        manifest_created_at_ms=0,
        input_authority_sha256=authority_sha256,
        frozen_file_count=3,
        _factory_token=development._FREEZE_FACTORY_TOKEN,
    )


def _prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    subject.D2HistoricalInputAuthorityArtifactsV0,
    development.D2HistoricalDevelopmentFreezeV0,
]:
    _configure_paths(tmp_path, monkeypatch)
    bundle = subject.create_d2_historical_input_authority_artifacts_v0(
        workspace_root=tmp_path
    )
    freeze = _freeze(bundle.authority.authority_sha256)
    monkeypatch.setattr(
        subject,
        "load_d2_historical_development_freeze_v0",
        lambda *_args, **_kwargs: freeze,
    )
    return bundle, freeze


def _prepare_and_arm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    subject.D2HistoricalInputAuthorityArtifactsV0,
    development.D2HistoricalDevelopmentFreezeV0,
]:
    bundle, freeze = _prepare(tmp_path, monkeypatch)
    subject.arm_d2_historical_development_attempt_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    return bundle, freeze


def _start(tmp_path: Path) -> attempt_wal.D1AppendStartedResultV0:
    armed = attempt_wal.load_attempt_wal_v0(tmp_path / "attempt")
    return attempt_wal.append_started_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=armed.prefix,
        started_at_ms=armed.records[0].observed_at_ms,
    )


def _publish_failure(
    tmp_path: Path,
    started: attempt_wal.D1AppendStartedResultV0,
) -> subject.D2HistoricalFailureReceiptPublicationV0:
    receipt = subject._build_failure_receipt_v0(
        start=started.snapshot.records[1],
        grant=started.outcome_access_grant,
        phase="OUTCOME_REPLAY",
        error_code="D2_OUTCOME_REPLAY_FAILED",
        output_protocol_state="PROVEN_ABSENT",
        observed_at_ms=started.snapshot.records[1].observed_at_ms,
    )
    return subject._publish_failure_receipt_v0(
        target=tmp_path / "failure",
        receipt=receipt,
    )


def _write_placeholder_completed_output(tmp_path: Path) -> Path:
    output = tmp_path / "output"
    output.mkdir()
    for name in sorted(subject._D2_COMPLETED_OUTPUT_FILE_NAMES_V0):
        (output / name).write_bytes(f"placeholder:{name}\n".encode())
    return output


def test_literal_authority_is_exact_and_contains_no_native_hour_binding() -> None:
    authority = subject._fixed_input_authority_v0()
    raw = subject.canonical_d2_historical_input_authority_v0(authority)
    document = json.loads(raw)

    assert authority.authority_sha256 == subject.D2_OPERATOR_EXPECTED_INPUT_AUTHORITY_SHA256_V0
    assert _sha(raw) == subject.D2_OPERATOR_EXPECTED_INPUT_AUTHORITY_FILE_SHA256_V0
    assert len(raw) == subject.D2_OPERATOR_EXPECTED_INPUT_AUTHORITY_SIZE_BYTES_V0
    assert all(row["interval"] == "5m" for row in document["five_minute_manifests"])
    assert all(
        "__1h" not in row["relative_manifest_path"]
        for row in document["five_minute_manifests"]
    )
    assert subject.D2_HISTORICAL_FIXED_INPUT_PROJECTION_SHA256_V0 == (
        "fa3f9c4c4ccfdf086348abe7f9277bf369531d18ac07b763d86ceb5727dc7472"
    )
    assert subject.D2_HISTORICAL_SOURCE_POLICY_SHA256_V0 == (
        "52a83f2a4e2e6c28a33ebfac7a0fa8726d80db0c93798088c9d92af2c3e79b19"
    )


def test_metadata_projection_validator_never_opens_a_gzip_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    predecessor = b"predecessor metadata\n"
    predecessor_path = (
        tmp_path
        / "artifacts/backtest/2026-07-21-d1-scefb-v0-input-authority/input_authority.jsonl"
    )
    predecessor_path.parent.mkdir(parents=True)
    predecessor_path.write_bytes(predecessor)
    monkeypatch.setattr(
        subject,
        "D1_PREDECESSOR_INPUT_AUTHORITY_FILE_SHA256_V0",
        _sha(predecessor),
    )

    funding = b"funding metadata only\n"
    funding_relative = "metadata/funding-authority.jsonl"
    funding_path = tmp_path / "metadata/funding-authority.jsonl"
    funding_path.parent.mkdir(parents=True)
    funding_path.write_bytes(funding)
    monkeypatch.setattr(
        subject,
        "D2_HISTORICAL_FIXED_FUNDING_MANIFEST_RELATIVE_PATH_V0",
        funding_relative,
    )
    monkeypatch.setattr(
        subject,
        "D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0",
        _sha(funding),
    )

    manifests: list[tuple[str, str, str]] = []
    for index, (symbol, _relative, _digest) in enumerate(
        subject.D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0
    ):
        relative = f"metadata/{index}__{symbol}__5m.csv.gz.manifest.json"
        raw = f"recorded sidecar {symbol}\n".encode()
        path = tmp_path.joinpath(*relative.split("/"))
        path.write_bytes(raw)
        manifests.append((symbol, relative, _sha(raw)))
    monkeypatch.setattr(
        subject,
        "D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0",
        tuple(manifests),
    )

    original = subject._read_stable_regular_file
    opened: list[Path] = []

    def record_read(path: Path, label: str, *, maximum_bytes: int) -> bytes:
        opened.append(path)
        return original(path, label, maximum_bytes=maximum_bytes)

    monkeypatch.setattr(subject, "_read_stable_regular_file", record_read)
    subject._validate_metadata_projection_sources_v0(tmp_path)

    assert len(opened) == 12
    assert not any(path.name.endswith(".csv.gz") for path in opened)
    assert sum(path.name.endswith(".csv.gz.manifest.json") for path in opened) == 10


def test_workspace_member_rejects_an_existing_reparse_ancestor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ancestor = tmp_path / "junction"
    ancestor.mkdir()
    ancestor_identity = ancestor.stat(follow_symlinks=False).st_ino
    original = subject._is_link_or_reparse

    def mark_ancestor_as_reparse(metadata: os.stat_result) -> bool:
        return metadata.st_ino == ancestor_identity or original(metadata)

    monkeypatch.setattr(subject, "_is_link_or_reparse", mark_ancestor_as_reparse)
    with pytest.raises(ValueError, match="reparse ancestor"):
        subject._workspace_member_v0(tmp_path, "junction/child.jsonl")


def test_prepare_freeze_and_arm_are_row_blind_and_bind_exact_upstream(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_paths(tmp_path, monkeypatch)
    row_reads = {"five_minute": 0, "funding": 0}

    def forbidden_five_minute(**_kwargs):
        row_reads["five_minute"] += 1
        raise AssertionError("5m outcome loader ran before START")

    def forbidden_funding(**_kwargs):
        row_reads["funding"] += 1
        raise AssertionError("funding outcome loader ran before START")

    monkeypatch.setattr(
        development,
        "load_d1_historical_authenticated_five_minute_v0",
        forbidden_five_minute,
    )
    monkeypatch.setattr(
        development,
        "load_d1_historical_authenticated_funding_bindings_v0",
        forbidden_funding,
    )
    bundle = subject.create_d2_historical_input_authority_artifacts_v0(
        workspace_root=tmp_path
    )
    loaded = subject.load_d2_historical_input_authority_artifacts_v0(
        workspace_root=tmp_path
    )
    assert loaded == bundle

    freeze = _freeze(bundle.authority.authority_sha256)
    captured: dict[str, object] = {}

    def fake_create(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(manifest_sha256=freeze.manifest_sha256)

    monkeypatch.setattr(subject, "create_downstream_code_freeze_v1", fake_create)
    monkeypatch.setattr(
        subject,
        "load_d2_historical_development_freeze_v0",
        lambda *_args, **_kwargs: freeze,
    )
    assert (
        subject.create_d2_historical_development_freeze_v0(workspace_root=tmp_path)
        == freeze
    )
    upstream = captured["upstream_sha256"]
    assert isinstance(upstream, dict)
    assert upstream["d2_input_authority"] == bundle.authority.authority_sha256
    assert upstream["d2_source_policy"] == subject.D2_HISTORICAL_SOURCE_POLICY_SHA256_V0
    assert upstream["d2_operator_correction_a1"] == (
        subject.D2_OPERATOR_EXPECTED_FAILURE_CORRECTION_SHA256_V0
    )
    assert captured["purpose"] == development.D2_DEVELOPMENT_FREEZE_PURPOSE_V0

    arm = subject.arm_d2_historical_development_attempt_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    snapshot = attempt_wal.load_attempt_wal_v0(arm.attempt_dir)
    assert snapshot.last_state == "ARMED"
    assert snapshot.bindings.input_authority_sha256 == bundle.authority.authority_sha256
    assert snapshot.bindings.funding_authority_file_sha256 == (
        subject.D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0
    )
    assert row_reads == {"five_minute": 0, "funding": 0}


def test_authority_publication_is_no_replace_and_tamper_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_paths(tmp_path, monkeypatch)
    subject.create_d2_historical_input_authority_artifacts_v0(workspace_root=tmp_path)
    with pytest.raises(
        subject.D2HistoricalOperatorErrorV0,
        match="D2_INPUT_AUTHORITY_PREPARATION_FAILED",
    ):
        subject.create_d2_historical_input_authority_artifacts_v0(workspace_root=tmp_path)

    path = tmp_path / "authority/input_authority.jsonl"
    path.write_bytes(path.read_bytes() + b"tamper")
    with pytest.raises(
        subject.D2HistoricalOperatorErrorV0,
        match="D2_INPUT_AUTHORITY_VERIFICATION_FAILED",
    ):
        subject.load_d2_historical_input_authority_artifacts_v0(workspace_root=tmp_path)


def test_run_passes_one_fresh_grant_directly_and_runner_consumes_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    observed: dict[str, object] = {}

    def fake_run(**kwargs):
        grant = kwargs["outcome_access_grant"]
        assert type(grant) is attempt_wal.D1OutcomeAccessGrantV0
        assert grant.consumed is False
        observed["grant"] = grant
        return grant.consume_once_v0(lambda: SimpleNamespace(result_sha256="b" * 64))

    monkeypatch.setattr(subject, "run_d2_historical_development_v0", fake_run)
    monkeypatch.setattr(
        subject,
        "write_d2_historical_development_artifacts_v0",
        lambda **_kwargs: SimpleNamespace(
            result_sha256="b" * 64,
            manifest_sha256="c" * 64,
        ),
    )
    monkeypatch.setattr(
        subject,
        "verify_d2_historical_serialized_artifacts_v0",
        lambda **_kwargs: SimpleNamespace(result_sha256="b" * 64),
    )
    completed = SimpleNamespace(status="COMPLETED")
    monkeypatch.setattr(
        subject,
        "verify_d2_historical_development_publication_v0",
        lambda **_kwargs: completed,
    )

    assert subject.run_and_publish_d2_historical_development_once_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    ) is completed
    grant = observed["grant"]
    assert isinstance(grant, attempt_wal.D1OutcomeAccessGrantV0)
    assert grant.consumed is True
    with pytest.raises(attempt_wal.D1HistoricalAttemptWalStateErrorV0, match="already consumed"):
        grant.consume_once_v0(lambda: None)


def test_post_start_runner_failure_preserves_cause_and_binds_receipt_before_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    observed: dict[str, object] = {}

    def failing_run(**kwargs):
        grant = kwargs["outcome_access_grant"]
        observed["grant"] = grant

        def fail_after_consumption():
            raise RuntimeError("unserialized sensitive diagnostic")

        return grant.consume_once_v0(fail_after_consumption)

    monkeypatch.setattr(subject, "run_d2_historical_development_v0", failing_run)
    with pytest.raises(subject.D2HistoricalOperatorErrorV0) as captured:
        subject.run_and_publish_d2_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )

    assert captured.value.phase == "OUTCOME_REPLAY"
    assert captured.value.code == "D2_OUTCOME_REPLAY_FAILED"
    assert captured.value.verification_status == "FAILED"
    assert isinstance(captured.value.__cause__, RuntimeError)
    assert "sensitive diagnostic" in str(captured.value.__cause__)
    grant = observed["grant"]
    assert isinstance(grant, attempt_wal.D1OutcomeAccessGrantV0)
    assert grant.consumed is True

    snapshot = attempt_wal.load_attempt_wal_v0(tmp_path / "attempt")
    assert tuple(record.state for record in snapshot.records) == (
        "ARMED",
        "STARTED_BEFORE_OUTCOME_ACCESS",
        "FAILED",
    )
    match = subject._FAILURE_DETAIL_RE.fullmatch(snapshot.records[-1].detail_code or "")
    assert match is not None
    assert match.group(1).lower() == captured.value.failure_receipt_sha256
    receipt_raw = (tmp_path / "failure/failure-receipt.jsonl").read_bytes()
    assert b"sensitive diagnostic" not in receipt_raw


def test_post_start_clock_rollback_is_clamped_and_still_publishes_typed_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    armed = attempt_wal.load_attempt_wal_v0(tmp_path / "attempt")
    start_ms = armed.records[0].observed_at_ms
    clock_values = iter((start_ms, start_ms - 1))
    monkeypatch.setattr(subject, "_now_ms_v0", lambda: next(clock_values))

    def failing_run(**kwargs):
        grant = kwargs["outcome_access_grant"]

        def fail_after_consumption():
            raise RuntimeError("clock rollback trigger")

        return grant.consume_once_v0(fail_after_consumption)

    monkeypatch.setattr(subject, "run_d2_historical_development_v0", failing_run)
    with pytest.raises(subject.D2HistoricalOperatorErrorV0) as captured:
        subject.run_and_publish_d2_historical_development_once_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )

    assert captured.value.verification_status == "FAILED"
    snapshot = attempt_wal.load_attempt_wal_v0(tmp_path / "attempt")
    start = snapshot.records[1]
    terminal = snapshot.records[-1]
    assert terminal.observed_at_ms == start.observed_at_ms == start_ms
    observed = subject._observe_failure_receipt_v0(tmp_path / "failure")
    assert observed.state == "VALID"
    assert observed.publication is not None
    receipt = observed.publication.receipt
    assert receipt.observed_at_ms == start_ms
    assert receipt.context["clock_clamped_to_start"] is True


def test_exact_failure_receipt_binds_file_hash_and_verifies_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    publication = _publish_failure(tmp_path, started)
    raw = (tmp_path / "failure/failure-receipt.jsonl").read_bytes()
    assert publication.receipt.receipt_body_sha256 != publication.receipt_file_sha256
    assert _sha(raw) == publication.receipt_file_sha256
    detail = subject._failure_detail_code_v0(publication.receipt_file_sha256)
    terminal = attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="FAILED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        detail_code=detail,
    )
    assert terminal.records[-1].detail_code == detail

    verified = subject.verify_d2_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verified.status == "FAILED"
    assert verified.failure_receipt_sha256 == publication.receipt_file_sha256
    assert verified.result_sha256 is None


def test_missing_receipt_after_failed_terminal_is_ambiguous_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="FAILED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        detail_code=subject._failure_detail_code_v0("e" * 64),
    )

    verified = subject.verify_d2_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verified.status == "AMBIGUOUS_FAILURE_EVIDENCE"
    assert verified.reason == "MISSING_OR_INVALID_FAILURE_RECEIPT"


def test_receipt_tamper_after_failed_terminal_is_ambiguous_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    publication = _publish_failure(tmp_path, started)
    attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="FAILED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        detail_code=subject._failure_detail_code_v0(publication.receipt_file_sha256),
    )
    receipt_path = tmp_path / "failure/failure-receipt.jsonl"
    receipt_path.write_bytes(receipt_path.read_bytes() + b"tamper")

    verified = subject.verify_d2_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verified.status == "AMBIGUOUS_FAILURE_EVIDENCE"
    assert verified.reason == "MISSING_OR_INVALID_FAILURE_RECEIPT"


def test_valid_receipt_without_terminal_is_incomplete_failure_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    publication = _publish_failure(tmp_path, started)

    verified = subject.verify_d2_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verified.status == "AMBIGUOUS_FAILURE_EVIDENCE"
    assert verified.reason == "INCOMPLETE_FAILURE_BINDING"
    assert verified.failure_receipt_sha256 == publication.receipt_file_sha256


def test_receipt_detail_hash_mismatch_is_ambiguous_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    _publish_failure(tmp_path, started)
    attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="FAILED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        detail_code=subject._failure_detail_code_v0("f" * 64),
    )
    verified = subject.verify_d2_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verified.status == "AMBIGUOUS_FAILURE_EVIDENCE"
    assert verified.reason == "TERMINAL_FAILURE_FILE_HASH_MISMATCH"


def test_phase_error_code_mismatch_is_invalid_even_with_recomputed_receipt_hashes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    receipt = subject._build_failure_receipt_v0(
        start=started.snapshot.records[1],
        grant=started.outcome_access_grant,
        phase="OUTCOME_REPLAY",
        error_code="D2_OUTCOME_REPLAY_FAILED",
        output_protocol_state="PROVEN_ABSENT",
        observed_at_ms=started.snapshot.records[1].observed_at_ms,
    )
    document = json.loads(subject.canonical_d2_historical_failure_receipt_v0(receipt))
    document["error_code"] = "D2_ARTIFACT_PUBLICATION_FAILED"
    body = dict(document)
    del body["receipt_body_sha256"]
    document["receipt_body_sha256"] = _sha(
        subject._FAILURE_RECEIPT_BODY_HASH_DOMAIN
        + subject.canonical_json_line(body)
    )
    raw = subject.canonical_json_line(document)
    (tmp_path / "failure").mkdir()
    (tmp_path / "failure/failure-receipt.jsonl").write_bytes(raw)
    attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="FAILED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        detail_code=subject._failure_detail_code_v0(_sha(raw)),
    )

    verified = subject.verify_d2_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert verified.status == "AMBIGUOUS_FAILURE_EVIDENCE"
    assert verified.reason == "MISSING_OR_INVALID_FAILURE_RECEIPT"


def test_ambiguous_output_terminal_requires_exact_failure_receipt_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    (tmp_path / "output").mkdir()
    receipt = subject._build_failure_receipt_v0(
        start=started.snapshot.records[1],
        grant=started.outcome_access_grant,
        phase="ARTIFACT_PUBLICATION",
        error_code="D2_ARTIFACT_PUBLICATION_FAILED",
        output_protocol_state="PRESENT_OR_UNCERTAIN",
        observed_at_ms=started.snapshot.records[1].observed_at_ms,
    )
    publication = subject._publish_failure_receipt_v0(
        target=tmp_path / "failure",
        receipt=receipt,
    )
    attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="AMBIGUOUS_OUTPUT",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        detail_code=subject._failure_detail_code_v0(publication.receipt_file_sha256),
    )
    exact = subject.verify_d2_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert exact.status == "AMBIGUOUS_OUTPUT"
    assert exact.reason == "TERMINAL_AMBIGUOUS_OUTPUT_VALID_FAILURE_BINDING"

    path = tmp_path / "failure/failure-receipt.jsonl"
    path.write_bytes(path.read_bytes() + b"tamper")
    tampered = subject.verify_d2_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )
    assert tampered.status == "AMBIGUOUS_OUTPUT"
    assert tampered.reason == "AMBIGUOUS_OUTPUT_MISSING_OR_INVALID_FAILURE_RECEIPT"


def test_orphan_receipt_before_start_is_typed_operational_corruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    body = subject._failure_receipt_body_document_v0(
        run_id=subject.D2_OPERATOR_RUN_ID_V0,
        phase="OUTCOME_REPLAY",
        error_code="D2_OUTCOME_REPLAY_FAILED",
        context={
            "clock_clamped_to_start": False,
            "native_1h_outcome_opened": False,
            "outcome_access_grant_consumed": False,
            "source_policy_sha256": subject.D2_HISTORICAL_SOURCE_POLICY_SHA256_V0,
        },
        start_record_sha256="1" * 64,
        bindings_sha256="2" * 64,
        attempt_directory_sha256="3" * 64,
        planned_terminal_state="FAILED",
        output_protocol_state="PROVEN_ABSENT",
        observed_at_ms=0,
    )
    context = body["context"]
    assert isinstance(context, dict)
    receipt = subject.D2HistoricalFailureReceiptV0(
        run_id=subject.D2_OPERATOR_RUN_ID_V0,
        phase="OUTCOME_REPLAY",
        error_code="D2_OUTCOME_REPLAY_FAILED",
        context=context,
        start_record_sha256="1" * 64,
        bindings_sha256="2" * 64,
        attempt_directory_sha256="3" * 64,
        planned_terminal_state="FAILED",
        output_protocol_state="PROVEN_ABSENT",
        observed_at_ms=0,
        receipt_body_sha256=_sha(
            subject._FAILURE_RECEIPT_BODY_HASH_DOMAIN
            + subject.canonical_json_line(body)
        ),
    )
    subject._publish_failure_receipt_v0(target=tmp_path / "failure", receipt=receipt)

    with pytest.raises(subject.D2HistoricalOperatorErrorV0) as captured:
        subject.verify_d2_historical_development_publication_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert captured.value.code == "ORPHAN_FAILURE_RECEIPT_BEFORE_START"
    assert captured.value.verification_status == "OPERATIONAL_ERROR"


def test_completed_restart_uses_independent_serialized_verifier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    result_sha256 = "b" * 64
    manifest_sha256 = "c" * 64
    terminal = attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="COMPLETED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        result_sha256=result_sha256,
        artifact_manifest_sha256=manifest_sha256,
    )
    _write_placeholder_completed_output(tmp_path)
    captured: dict[str, object] = {}

    def independent_verify(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            result_sha256=result_sha256,
            artifact_manifest_sha256=manifest_sha256,
        )

    monkeypatch.setattr(
        subject,
        "verify_d2_historical_published_artifact_bundle_v0",
        independent_verify,
    )
    verified = subject.verify_d2_historical_development_publication_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )

    assert verified.status == "COMPLETED"
    assert verified.terminal_receipt_sha256 == terminal.records[-1].record_sha256
    assert captured["expected_result_sha256"] == result_sha256
    assert captured["expected_manifest_sha256"] == manifest_sha256
    assert captured["expected_start_record_sha256"] == (
        started.snapshot.records[1].record_sha256
    )
    assert captured["expected_attempt_bindings_sha256"] == (
        started.snapshot.records[1].bindings_sha256
    )


def test_completed_restart_rejects_ambiguous_wal_append_during_artifact_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    result_sha256 = "b" * 64
    manifest_sha256 = "c" * 64
    completed = attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="COMPLETED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        result_sha256=result_sha256,
        artifact_manifest_sha256=manifest_sha256,
    )
    _write_placeholder_completed_output(tmp_path)

    def append_ambiguity(**_kwargs):
        attempt_wal.append_terminal_v0(
            attempt_dir=tmp_path / "attempt",
            expected_prefix=completed.prefix,
            state="AMBIGUOUS_OUTPUT",
            terminal_at_ms=started.snapshot.records[1].observed_at_ms,
            detail_code="POST_COMPLETION_VERIFICATION_FAILED",
            result_sha256=result_sha256,
            artifact_manifest_sha256=manifest_sha256,
        )
        return SimpleNamespace(
            result_sha256=result_sha256,
            artifact_manifest_sha256=manifest_sha256,
        )

    monkeypatch.setattr(
        subject,
        "verify_d2_historical_published_artifact_bundle_v0",
        append_ambiguity,
    )
    with pytest.raises(subject.D2HistoricalOperatorErrorV0) as captured:
        subject.verify_d2_historical_development_publication_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )

    assert captured.value.code == (
        "D2_PUBLICATION_PROTOCOL_CHANGED_DURING_VERIFICATION"
    )
    assert captured.value.verification_status == "AMBIGUOUS_OUTPUT"
    assert attempt_wal.load_attempt_wal_v0(tmp_path / "attempt").last_state == (
        "AMBIGUOUS_OUTPUT"
    )


def test_completed_restart_rejects_receipt_created_during_artifact_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    result_sha256 = "b" * 64
    manifest_sha256 = "c" * 64
    attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="COMPLETED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        result_sha256=result_sha256,
        artifact_manifest_sha256=manifest_sha256,
    )
    _write_placeholder_completed_output(tmp_path)
    receipt_sha256: str | None = None

    def publish_receipt(**_kwargs):
        nonlocal receipt_sha256
        receipt_sha256 = _publish_failure(
            tmp_path,
            started,
        ).receipt_file_sha256
        return SimpleNamespace(
            result_sha256=result_sha256,
            artifact_manifest_sha256=manifest_sha256,
        )

    monkeypatch.setattr(
        subject,
        "verify_d2_historical_published_artifact_bundle_v0",
        publish_receipt,
    )
    with pytest.raises(subject.D2HistoricalOperatorErrorV0) as captured:
        subject.verify_d2_historical_development_publication_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )

    assert captured.value.code == (
        "D2_PUBLICATION_PROTOCOL_CHANGED_DURING_VERIFICATION"
    )
    assert captured.value.verification_status == "AMBIGUOUS_OUTPUT"
    assert receipt_sha256 is not None
    assert captured.value.failure_receipt_sha256 == receipt_sha256


def test_completed_restart_rejects_output_member_mutated_after_artifact_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    result_sha256 = "b" * 64
    manifest_sha256 = "c" * 64
    attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="COMPLETED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        result_sha256=result_sha256,
        artifact_manifest_sha256=manifest_sha256,
    )
    output = _write_placeholder_completed_output(tmp_path)

    def mutate_member(**_kwargs):
        verification = SimpleNamespace(
            result_sha256=result_sha256,
            artifact_manifest_sha256=manifest_sha256,
        )
        report = output / "report.md"
        report.write_bytes(report.read_bytes() + b"in-place mutation")
        return verification

    monkeypatch.setattr(
        subject,
        "verify_d2_historical_published_artifact_bundle_v0",
        mutate_member,
    )
    with pytest.raises(subject.D2HistoricalOperatorErrorV0) as captured:
        subject.verify_d2_historical_development_publication_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )

    assert captured.value.code == (
        "D2_PUBLICATION_PROTOCOL_CHANGED_DURING_VERIFICATION"
    )
    assert captured.value.verification_status == "AMBIGUOUS_OUTPUT"


def test_completed_restart_rejects_authority_mutated_after_artifact_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    result_sha256 = "b" * 64
    manifest_sha256 = "c" * 64
    attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="COMPLETED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        result_sha256=result_sha256,
        artifact_manifest_sha256=manifest_sha256,
    )
    _write_placeholder_completed_output(tmp_path)

    def mutate_authority(**_kwargs):
        verification = SimpleNamespace(
            result_sha256=result_sha256,
            artifact_manifest_sha256=manifest_sha256,
        )
        authority = tmp_path / "authority/input_authority.jsonl"
        authority.write_bytes(authority.read_bytes() + b"in-place mutation")
        return verification

    monkeypatch.setattr(
        subject,
        "verify_d2_historical_published_artifact_bundle_v0",
        mutate_authority,
    )
    with pytest.raises(subject.D2HistoricalOperatorErrorV0) as captured:
        subject.verify_d2_historical_development_publication_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )

    assert captured.value.code == (
        "D2_PUBLICATION_PROTOCOL_CHANGED_DURING_VERIFICATION"
    )
    assert captured.value.verification_status == "AMBIGUOUS_OUTPUT"


def test_completed_restart_requires_all_nine_exact_output_members(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="COMPLETED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        result_sha256="b" * 64,
        artifact_manifest_sha256="c" * 64,
    )
    output = _write_placeholder_completed_output(tmp_path)
    (output / "report.md").unlink()
    verifier_called = False

    def forbidden_verifier(**_kwargs):
        nonlocal verifier_called
        verifier_called = True
        raise AssertionError("serialized verifier ran on an incomplete bundle")

    monkeypatch.setattr(
        subject,
        "verify_d2_historical_published_artifact_bundle_v0",
        forbidden_verifier,
    )
    with pytest.raises(subject.D2HistoricalOperatorErrorV0) as captured:
        subject.verify_d2_historical_development_publication_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )

    assert captured.value.code == "D2_OUTPUT_PROTOCOL_OBSERVATION_FAILED"
    assert captured.value.verification_status == "AMBIGUOUS_OUTPUT"
    assert verifier_called is False


def test_reproduction_hook_requires_completed_and_never_mutates_primary_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    result_sha256 = "b" * 64
    manifest_sha256 = "c" * 64
    completed = attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="COMPLETED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        result_sha256=result_sha256,
        artifact_manifest_sha256=manifest_sha256,
    )
    (tmp_path / "output").mkdir()
    marker = tmp_path / "output/marker.bin"
    marker.write_bytes(b"immutable published marker")
    primary = subject.D2HistoricalDevelopmentPublicationVerificationV0(
        status="COMPLETED",
        reason=None,
        run_id=subject.D2_OPERATOR_RUN_ID_V0,
        attempt_dir=tmp_path / "attempt",
        output_dir=tmp_path / "output",
        start_receipt_sha256=started.snapshot.records[1].record_sha256,
        terminal_receipt_sha256=completed.records[-1].record_sha256,
        result_sha256=result_sha256,
        artifact_manifest_sha256=manifest_sha256,
        failure_receipt_sha256=None,
    )
    monkeypatch.setattr(
        subject,
        "verify_d2_historical_development_publication_v0",
        lambda **_kwargs: primary,
    )
    reproduced = SimpleNamespace(
        run_id=subject.D2_OPERATOR_RUN_ID_V0,
        run_started_at_ms=started.snapshot.records[1].observed_at_ms,
        start_record_sha256=started.snapshot.records[1].record_sha256,
        completed_record_sha256=completed.records[-1].record_sha256,
        result_sha256=result_sha256,
        artifact_manifest_sha256=manifest_sha256,
        summary_sha256="d" * 64,
        derived_manifest_sequence_root_sha256="e" * 64,
        episode_sequence_root_sha256="f" * 64,
        censor_sequence_root_sha256="1" * 64,
        episode_count=1,
        censor_count=2,
        raw_replay_performed=True,
        published_artifacts_modified=False,
        production_order_placement=False,
    )
    captured: dict[str, object] = {}

    def fake_reproduce(**kwargs):
        captured.update(kwargs)
        return reproduced

    monkeypatch.setattr(
        subject,
        "reproduce_d2_historical_published_artifact_bundle_v0",
        fake_reproduce,
    )
    wal_before = (tmp_path / "attempt/attempt.wal").read_bytes()
    seal_before = (tmp_path / "attempt/start.seal").read_bytes()
    marker_before = marker.read_bytes()

    result = subject.verify_d2_historical_development_reproduction_v0(
        workspace_root=tmp_path,
        expected_freeze_manifest_sha256=freeze.manifest_sha256,
    )

    assert result is reproduced
    assert captured["expected_attempt_bindings"] == subject._attempt_wal_bindings_v0(
        bundle=bundle,
        freeze=freeze,
    )
    assert (tmp_path / "attempt/attempt.wal").read_bytes() == wal_before
    assert (tmp_path / "attempt/start.seal").read_bytes() == seal_before
    assert marker.read_bytes() == marker_before


def test_reproduction_hook_refuses_noncompleted_primary_before_raw_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    incomplete = SimpleNamespace(
        status="INCOMPLETE",
        failure_receipt_sha256=None,
    )
    monkeypatch.setattr(
        subject,
        "verify_d2_historical_development_publication_v0",
        lambda **_kwargs: incomplete,
    )
    called = False

    def forbidden_reproduction(**_kwargs):
        nonlocal called
        called = True
        raise AssertionError("raw reproduction ran without COMPLETED evidence")

    monkeypatch.setattr(
        subject,
        "reproduce_d2_historical_published_artifact_bundle_v0",
        forbidden_reproduction,
    )
    with pytest.raises(subject.D2HistoricalOperatorErrorV0) as captured:
        subject.verify_d2_historical_development_reproduction_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    assert captured.value.code == (
        "D2_REPRODUCTION_REQUIRES_EXACT_COMPLETED_PUBLICATION"
    )
    assert called is False


def test_reproduction_hook_refuses_canonical_failure_receipt_created_during_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _bundle, freeze = _prepare_and_arm(tmp_path, monkeypatch)
    started = _start(tmp_path)
    result_sha256 = "b" * 64
    manifest_sha256 = "c" * 64
    completed = attempt_wal.append_terminal_v0(
        attempt_dir=tmp_path / "attempt",
        expected_prefix=started.snapshot.prefix,
        state="COMPLETED",
        terminal_at_ms=started.snapshot.records[1].observed_at_ms,
        result_sha256=result_sha256,
        artifact_manifest_sha256=manifest_sha256,
    )
    (tmp_path / "output").mkdir()
    primary = subject.D2HistoricalDevelopmentPublicationVerificationV0(
        status="COMPLETED",
        reason=None,
        run_id=subject.D2_OPERATOR_RUN_ID_V0,
        attempt_dir=tmp_path / "attempt",
        output_dir=tmp_path / "output",
        start_receipt_sha256=started.snapshot.records[1].record_sha256,
        terminal_receipt_sha256=completed.records[-1].record_sha256,
        result_sha256=result_sha256,
        artifact_manifest_sha256=manifest_sha256,
        failure_receipt_sha256=None,
    )
    verification_calls = 0

    def verify_primary(**_kwargs):
        nonlocal verification_calls
        verification_calls += 1
        if verification_calls == 1:
            return primary
        observed = subject._observe_failure_receipt_v0(tmp_path / "failure")
        assert observed.state == "VALID"
        assert observed.publication is not None
        return replace(
            primary,
            status="AMBIGUOUS_OUTPUT",
            reason="POST_COMPLETED_FAILURE_EVIDENCE_PRESENT",
            failure_receipt_sha256=observed.publication.receipt_file_sha256,
        )

    monkeypatch.setattr(
        subject,
        "verify_d2_historical_development_publication_v0",
        verify_primary,
    )

    def mutate_during_replay(**_kwargs):
        _publish_failure(tmp_path, started)
        return SimpleNamespace(
            run_id=subject.D2_OPERATOR_RUN_ID_V0,
            run_started_at_ms=started.snapshot.records[1].observed_at_ms,
            start_record_sha256=started.snapshot.records[1].record_sha256,
            completed_record_sha256=completed.records[-1].record_sha256,
            result_sha256=result_sha256,
            artifact_manifest_sha256=manifest_sha256,
            summary_sha256="d" * 64,
            derived_manifest_sequence_root_sha256="e" * 64,
            episode_sequence_root_sha256="f" * 64,
            censor_sequence_root_sha256="1" * 64,
            episode_count=1,
            censor_count=2,
            raw_replay_performed=True,
            published_artifacts_modified=False,
            production_order_placement=False,
        )

    monkeypatch.setattr(
        subject,
        "reproduce_d2_historical_published_artifact_bundle_v0",
        mutate_during_replay,
    )
    with pytest.raises(subject.D2HistoricalOperatorErrorV0) as captured:
        subject.verify_d2_historical_development_reproduction_v0(
            workspace_root=tmp_path,
            expected_freeze_manifest_sha256=freeze.manifest_sha256,
        )

    assert captured.value.code == "D2_REPRODUCTION_PRIMARY_CHANGED_DURING_REPLAY"
    assert captured.value.verification_status == "AMBIGUOUS_OUTPUT"
    assert captured.value.failure_receipt_sha256 is not None
    assert verification_calls == 2


def test_publication_verification_rejects_impossible_status_combinations(
    tmp_path: Path,
) -> None:
    common = {
        "run_id": subject.D2_OPERATOR_RUN_ID_V0,
        "attempt_dir": tmp_path,
        "output_dir": tmp_path,
        "start_receipt_sha256": "1" * 64,
        "terminal_receipt_sha256": "2" * 64,
    }
    completed = subject.D2HistoricalDevelopmentPublicationVerificationV0(
        status="COMPLETED",
        reason=None,
        result_sha256="3" * 64,
        artifact_manifest_sha256="4" * 64,
        failure_receipt_sha256=None,
        **common,
    )
    with pytest.raises(ValueError):
        replace(completed, result_sha256=None)
    failed = subject.D2HistoricalDevelopmentPublicationVerificationV0(
        status="FAILED",
        reason=None,
        result_sha256=None,
        artifact_manifest_sha256=None,
        failure_receipt_sha256="5" * 64,
        **common,
    )
    with pytest.raises(ValueError):
        replace(failed, failure_receipt_sha256=None)
    incomplete = subject.D2HistoricalDevelopmentPublicationVerificationV0(
        status="INCOMPLETE",
        reason="FIXED_REASON",
        terminal_receipt_sha256=None,
        result_sha256=None,
        artifact_manifest_sha256=None,
        failure_receipt_sha256=None,
        **{key: value for key, value in common.items() if key != "terminal_receipt_sha256"},
    )
    with pytest.raises(ValueError):
        replace(incomplete, terminal_receipt_sha256="4" * 64)
    ambiguous = subject.D2HistoricalDevelopmentPublicationVerificationV0(
        status="AMBIGUOUS_FAILURE_EVIDENCE",
        reason="FIXED_REASON",
        result_sha256=None,
        artifact_manifest_sha256=None,
        failure_receipt_sha256="5" * 64,
        **common,
    )
    with pytest.raises(ValueError):
        replace(ambiguous, start_receipt_sha256=None)


def test_cli_emits_canonical_sanitized_operational_error_without_argparse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    digest = "d" * 64

    def fail(**_kwargs):
        try:
            raise RuntimeError("secret arbitrary exception text")
        except RuntimeError as cause:
            raise subject.D2HistoricalOperatorErrorV0(
                phase="OUTCOME_REPLAY",
                code="D2_OUTCOME_REPLAY_FAILED",
                verification_status="FAILED",
                failure_receipt_sha256=digest,
            ) from cause

    monkeypatch.setattr(
        subject,
        "verify_d2_historical_development_publication_v0",
        fail,
    )
    exit_code = subject.main(
        (
            "verify-development-publication",
            "--workspace-root",
            str(tmp_path),
            "--expected-freeze-manifest-sha256",
            "a" * 64,
        )
    )
    raw = capsys.readouterr().out.encode()
    document = json.loads(raw)

    assert exit_code == 1
    assert raw == subject.canonical_json_line(document)
    assert document["status"] == "FAILED"
    assert document["phase"] == "OUTCOME_REPLAY"
    assert document["code"] == "D2_OUTCOME_REPLAY_FAILED"
    assert document["failure_receipt_sha256"] == digest
    assert b"secret arbitrary" not in raw
    assert document["production_order_placement"] is False


def test_cli_invalid_freeze_digest_is_a_canonical_operational_error(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = subject.main(
        (
            "verify-development-publication",
            "--workspace-root",
            str(tmp_path),
            "--expected-freeze-manifest-sha256",
            "not-a-digest",
        )
    )
    raw = capsys.readouterr().out.encode()
    document = json.loads(raw)
    assert exit_code == 1
    assert raw == subject.canonical_json_line(document)
    assert document["status"] == "OPERATIONAL_ERROR"
    assert document["code"] == "D2_EXPECTED_FREEZE_SHA256_INVALID"


def test_cli_fixed_path_failure_is_typed_and_does_not_leak_exception_text(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail_member(_root: Path, _relative: str) -> Path:
        raise subject.D1HistoricalOperatorErrorV0("secret fixed-path detail")

    monkeypatch.setattr(subject, "_workspace_member_v0", fail_member)
    exit_code = subject.main(
        (
            "verify-development-publication",
            "--workspace-root",
            str(tmp_path),
            "--expected-freeze-manifest-sha256",
            "a" * 64,
        )
    )
    raw = capsys.readouterr().out.encode()
    document = json.loads(raw)
    assert exit_code == 1
    assert raw == subject.canonical_json_line(document)
    assert document["code"] == "D2_FIXED_WORKSPACE_MEMBER_INVALID"
    assert b"secret fixed-path" not in raw
