"""One-shot operator for the post-D1 D2 derived-hour development diagnostic.

The operator is deliberately thin.  It reuses the D1 append-only attempt WAL,
the shared downstream-freeze owner, and the D2 replay/artifact owner.  Before a
durable START it may inspect metadata files only; no gzip file is opened.  A
fresh START grant is passed directly to the D2 runner, which is the sole owner
allowed to consume it and open authenticated outcome rows.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Final, Literal, Never, cast

from signalbot.backtest.d1_scefb_historical_attempt_wal import (
    D1_ATTEMPT_START_SEAL_FILE_V0,
    D1_ATTEMPT_WAL_FILE_V0,
    D1AttemptWalBindingsV0,
    D1AttemptWalRecordV0,
    D1AttemptWalSnapshotV0,
    D1HistoricalAttemptWalErrorV0,
    D1OutcomeAccessGrantV0,
    _DirectoryIdentityV0,
    _file_metadata,
    _FileMetadataV0,
    _lstat_or_none,
    _metadata_from_path,
    _require_attempt_directory,
    append_started_v0,
    append_terminal_v0,
    create_armed_wal_v0,
    load_attempt_wal_v0,
)
from signalbot.backtest.d1_scefb_historical_development import (
    D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0,
    D1HistoricalFundingFileBindingV0,
    D1HistoricalKlineManifestBindingV0,
)
from signalbot.backtest.d1_scefb_historical_operator import (
    D1HistoricalOperatorErrorV0,
    _canonical_output_target_kind,
    _hash_stable_regular_file,
    _is_link_or_reparse,
    _output_protocol_has_orphans,
    _output_protocol_is_proven_absent,
    _publish_fresh_directory,
    _read_stable_regular_file,
    _real_directory_identity,
    _require_exact_directory,
    _workspace_root,
)
from signalbot.backtest.d1_scefb_historical_operator import (
    _workspace_member as _d1_workspace_member,
)
from signalbot.backtest.d2_scefb_derived_hourly_historical import (
    D1_PREDECESSOR_FAILURE_EVIDENCE_ARCHIVE_SHA256_V0,
    D1_PREDECESSOR_FAILURE_EVIDENCE_MANIFEST_SHA256_V0,
    D1_PREDECESSOR_FREEZE_SHA256_V0,
    D1_PREDECESSOR_INPUT_AUTHORITY_FILE_SHA256_V0,
    D1_PREDECESSOR_PREREGISTRATION_SHA256_V0,
    D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0,
    D2_HISTORICAL_FIXED_FUNDING_FILES_V0,
    D2_HISTORICAL_FIXED_FUNDING_MANIFEST_RELATIVE_PATH_V0,
    D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0,
    D2_HISTORICAL_FIXED_INPUT_PROJECTION_SHA256_V0,
    D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
    D2_HISTORICAL_SOURCE_POLICY_SHA256_V0,
    D2HistoricalInputAuthorityV0,
    build_d2_historical_input_authority_v0,
    canonical_d2_historical_input_authority_v0,
)
from signalbot.backtest.d2_scefb_historical_development import (
    D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0,
    D2_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0,
    D2_DEVELOPMENT_FREEZE_PURPOSE_V0,
    D2_DEVELOPMENT_FREEZE_SUFFIXES_V0,
    D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0,
    D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0,
    D2HistoricalDevelopmentFreezeV0,
    D2HistoricalReproductionVerificationV0,
    d2_historical_development_freeze_upstream_v0,
    load_d2_historical_development_freeze_v0,
    reproduce_d2_historical_published_artifact_bundle_v0,
    run_d2_historical_development_v0,
    verify_d2_historical_published_artifact_bundle_v0,
    verify_d2_historical_serialized_artifacts_v0,
    write_d2_historical_development_artifacts_v0,
)
from signalbot.backtest.downstream_code_freeze import create_downstream_code_freeze_v1
from signalbot.r4b_v2.canonical import canonical_json_line

D2_OPERATOR_INPUT_AUTHORITY_DIR_V0: Final = (
    "artifacts/backtest/2026-07-21-d2-scefb-derived-1h-v0-input-authority"
)
D2_OPERATOR_INPUT_AUTHORITY_FILE_V0: Final = "input_authority.jsonl"
D2_OPERATOR_FREEZE_MANIFEST_V0: Final = (
    "artifacts/backtest/2026-07-21-d2-scefb-derived-1h-v0-development-freeze-001/"
    "freeze_manifest.json"
)
D2_OPERATOR_ATTEMPT_DIR_V0: Final = (
    "artifacts/backtest/2026-07-21-d2-scefb-derived-1h-v0-development-run-001-attempt"
)
D2_OPERATOR_OUTPUT_DIR_V0: Final = (
    "artifacts/backtest/2026-07-21-d2-scefb-derived-1h-v0-development-run-001"
)
D2_OPERATOR_FAILURE_RECEIPT_DIR_V0: Final = (
    "artifacts/backtest/2026-07-21-d2-scefb-derived-1h-v0-development-run-001-"
    "failure-receipt"
)
D2_OPERATOR_FAILURE_RECEIPT_FILE_V0: Final = "failure-receipt.jsonl"
D2_OPERATOR_RUN_ID_V0: Final = "d2-scefb-derived-1h-v0-development-run-001"
D2_OPERATOR_PREREGISTRATION_FILE_V0: Final = (
    "docs/r4b-v2-d2-scefb-derived-hourly-development-preregistration-v0.md"
)
D2_OPERATOR_FAILURE_AMENDMENT_FILE_V0: Final = (
    "docs/r4b-v2-d2-scefb-operator-failure-receipt-amendment-a0.md"
)
D2_OPERATOR_FAILURE_CORRECTION_FILE_V0: Final = (
    "docs/r4b-v2-d2-scefb-operator-failure-receipt-correction-a1.md"
)

D2_OPERATOR_EXPECTED_INPUT_AUTHORITY_SHA256_V0: Final = (
    "b5e40e83112317c6878e549dc2f745f9c63a08837aacacfcb259338adb734f5c"
)
D2_OPERATOR_EXPECTED_INPUT_AUTHORITY_FILE_SHA256_V0: Final = (
    "dcbf84c637465851db1c03649962260aca5c50a9470c1ffbe3d66840f724f1bf"
)
D2_OPERATOR_EXPECTED_INPUT_AUTHORITY_SIZE_BYTES_V0: Final = 5_266
D2_OPERATOR_EXPECTED_FAILURE_CORRECTION_SHA256_V0: Final = (
    "c29137ab1f307092137bdc30b678f2f78aa8964a88f6387c293eff92e88ec865"
)

D2OperatorPhaseV0 = Literal[
    "AUTHORITY_PREPARATION",
    "AUTHORITY_VERIFICATION",
    "FREEZE_CREATION",
    "FREEZE_VERIFICATION",
    "ATTEMPT_ARM",
    "START_APPEND",
    "OUTCOME_REPLAY",
    "ARTIFACT_PUBLICATION",
    "SERIALIZED_ARTIFACT_VERIFICATION",
    "COMPLETED_WAL_APPEND",
    "POST_COMPLETION_VERIFICATION",
    "FAILURE_RECEIPT_PUBLICATION",
    "TERMINAL_WAL_APPEND",
    "RECOVERY_VERIFICATION",
    "REPRODUCTION_VERIFICATION",
]
D2OperatorVerificationStatusV0 = Literal[
    "COMPLETED",
    "FAILED",
    "INCOMPLETE",
    "AMBIGUOUS_OUTPUT",
    "AMBIGUOUS_FAILURE_EVIDENCE",
    "OPERATIONAL_ERROR",
]
D2FailureTerminalStateV0 = Literal["FAILED", "AMBIGUOUS_OUTPUT"]
D2OutputProtocolStateV0 = Literal["PROVEN_ABSENT", "PRESENT_OR_UNCERTAIN"]

_AUTHORITY_FILE_NAMES: Final = frozenset({D2_OPERATOR_INPUT_AUTHORITY_FILE_V0})
_FAILURE_RECEIPT_FILE_NAMES: Final = frozenset({D2_OPERATOR_FAILURE_RECEIPT_FILE_V0})
_MAX_METADATA_BYTES: Final = 1024 * 1024
_MAX_FROZEN_EVIDENCE_BYTES: Final = 16 * 1024 * 1024
_MAX_FAILURE_RECEIPT_BYTES: Final = 32 * 1024
_MAX_OUTPUT_PROTOCOL_ENTRIES: Final = 32
_D2_COMPLETED_OUTPUT_FILE_NAMES_V0: Final = frozenset(
    {
        "censors.jsonl",
        "code-freeze-receipt.jsonl",
        "derived-hourly-manifests.jsonl",
        "episodes.jsonl",
        "input-authority.jsonl",
        "manifest.jsonl",
        "report.md",
        "result-index.jsonl",
        "summary.jsonl",
    }
)
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_ERROR_CODE_RE: Final = re.compile(r"^[A-Z0-9][A-Z0-9_]{0,95}$")
_FAILURE_DETAIL_RE: Final = re.compile(r"^D2_FAILURE_RECEIPT_SHA256_([0-9A-F]{64})$")
_FAILURE_RECEIPT_BODY_HASH_DOMAIN: Final = (
    b"D2_HISTORICAL_FAILURE_RECEIPT_BODY_V0\0"
)
_OUTPUT_PATH_HASH_DOMAIN: Final = b"D2_HISTORICAL_OPERATOR_OUTPUT_PATH_V0\0"
_FAILURE_RECEIPT_SCHEMA_V0: Final = "d2_historical_failure_receipt_v0"


class D2HistoricalOperatorErrorV0(ValueError):
    """Typed, sanitized operational failure exposed by the D2 boundary."""

    phase: D2OperatorPhaseV0
    code: str
    verification_status: D2OperatorVerificationStatusV0
    failure_receipt_sha256: str | None

    def __init__(
        self,
        *,
        phase: D2OperatorPhaseV0,
        code: str,
        verification_status: D2OperatorVerificationStatusV0 = "OPERATIONAL_ERROR",
        failure_receipt_sha256: str | None = None,
    ) -> None:
        if phase not in _D2_PHASE_VALUES:
            raise ValueError("D2 operator phase is unsupported")
        if _ERROR_CODE_RE.fullmatch(code) is None:
            raise ValueError("D2 operator error code is not a fixed uppercase token")
        if verification_status not in _D2_VERIFICATION_STATUS_VALUES:
            raise ValueError("D2 operator verification status is unsupported")
        if failure_receipt_sha256 is not None:
            _require_sha256(failure_receipt_sha256, "failure_receipt_sha256")
        self.phase = phase
        self.code = code
        self.verification_status = verification_status
        self.failure_receipt_sha256 = failure_receipt_sha256
        super().__init__(f"{phase}:{code}")


@dataclass(frozen=True, slots=True)
class D2HistoricalInputAuthorityArtifactsV0:
    output_dir: Path
    authority: D2HistoricalInputAuthorityV0
    input_authority_file_sha256: str
    total_size_bytes: int

    def __post_init__(self) -> None:
        if not self.output_dir.is_absolute():
            raise ValueError("D2 authority output_dir must be absolute")
        _require_sha256(self.input_authority_file_sha256, "input_authority_file_sha256")
        if self.total_size_bytes != D2_OPERATOR_EXPECTED_INPUT_AUTHORITY_SIZE_BYTES_V0:
            raise ValueError("D2 authority byte size differs from the fixed identity")


@dataclass(frozen=True, slots=True)
class D2HistoricalDevelopmentAttemptArmV0:
    attempt_dir: Path
    armed_record_sha256: str
    code_freeze_manifest_sha256: str
    historical_bbo_available: bool = False
    paper_fill_claim: bool = False
    execution_conclusive: bool = False
    probability_claim: bool = False
    efficacy_claim: bool = False
    promoting: bool = False
    prospective: bool = False
    production_order_placement: bool = False

    def __post_init__(self) -> None:
        if not self.attempt_dir.is_absolute():
            raise ValueError("D2 attempt_dir must be absolute")
        _require_sha256(self.armed_record_sha256, "armed_record_sha256")
        _require_sha256(self.code_freeze_manifest_sha256, "code_freeze_manifest_sha256")
        if any(asdict(self)[name] for name in _FALSE_CLAIM_FIELDS):
            raise ValueError("D2 ARMED attempt cannot make outcome claims")


@dataclass(frozen=True, slots=True)
class D2HistoricalFailureReceiptV0:
    run_id: str
    phase: D2OperatorPhaseV0
    error_code: str
    context: Mapping[str, object]
    start_record_sha256: str
    bindings_sha256: str
    attempt_directory_sha256: str
    planned_terminal_state: D2FailureTerminalStateV0
    output_protocol_state: D2OutputProtocolStateV0
    observed_at_ms: int
    receipt_body_sha256: str
    production_order_placement: Literal[False] = False
    schema_version: Literal["d2_historical_failure_receipt_v0"] = (
        _FAILURE_RECEIPT_SCHEMA_V0
    )

    def __post_init__(self) -> None:
        if self.run_id != D2_OPERATOR_RUN_ID_V0:
            raise ValueError("failure receipt run_id is not fixed")
        if _ERROR_CODE_RE.fullmatch(self.error_code) is None:
            raise ValueError("failure receipt error_code is not fixed")
        if self.error_code != _failure_code_for_phase_v0(self.phase):
            raise ValueError("failure receipt phase and error_code differ")
        if set(self.context) != {
            "clock_clamped_to_start",
            "native_1h_outcome_opened",
            "outcome_access_grant_consumed",
            "source_policy_sha256",
        }:
            raise ValueError("failure receipt context fields differ")
        if type(self.context.get("clock_clamped_to_start")) is not bool:
            raise ValueError("failure receipt clock-clamped flag must be boolean")
        if self.context.get("native_1h_outcome_opened") is not False:
            raise ValueError("failure receipt cannot claim native 1h access")
        if type(self.context.get("outcome_access_grant_consumed")) is not bool:
            raise ValueError("failure receipt grant-consumed flag must be boolean")
        if self.context.get("source_policy_sha256") != D2_HISTORICAL_SOURCE_POLICY_SHA256_V0:
            raise ValueError("failure receipt source-policy binding differs")
        for value, label in (
            (self.start_record_sha256, "start_record_sha256"),
            (self.bindings_sha256, "bindings_sha256"),
            (self.attempt_directory_sha256, "attempt_directory_sha256"),
            (self.receipt_body_sha256, "receipt_body_sha256"),
        ):
            _require_sha256(value, label)
        if self.planned_terminal_state not in {"FAILED", "AMBIGUOUS_OUTPUT"}:
            raise ValueError("failure receipt terminal state is unsupported")
        if self.output_protocol_state not in {
            "PROVEN_ABSENT",
            "PRESENT_OR_UNCERTAIN",
        }:
            raise ValueError("failure receipt output state is unsupported")
        if (
            (self.planned_terminal_state == "FAILED")
            != (self.output_protocol_state == "PROVEN_ABSENT")
        ):
            raise ValueError("failure receipt output and terminal states differ")
        if type(self.observed_at_ms) is not int or self.observed_at_ms < 0:
            raise ValueError("failure receipt observed_at_ms is invalid")
        if self.observed_at_ms > (1 << 53) - 1:
            raise ValueError("failure receipt observed_at_ms is not JSON-safe")
        if self.production_order_placement is not False:
            raise ValueError("failure receipt cannot authorize production orders")
        if self.schema_version != _FAILURE_RECEIPT_SCHEMA_V0:
            raise ValueError("failure receipt schema is unsupported")


@dataclass(frozen=True, slots=True)
class D2HistoricalFailureReceiptPublicationV0:
    receipt: D2HistoricalFailureReceiptV0
    receipt_file_sha256: str

    def __post_init__(self) -> None:
        _require_sha256(self.receipt_file_sha256, "receipt_file_sha256")
        if self.receipt_file_sha256 != hashlib.sha256(
            canonical_d2_historical_failure_receipt_v0(self.receipt)
        ).hexdigest():
            raise ValueError("failure receipt publication file hash differs")


@dataclass(frozen=True, slots=True)
class D2HistoricalDevelopmentPublicationVerificationV0:
    status: D2OperatorVerificationStatusV0
    reason: str | None
    run_id: str
    attempt_dir: Path
    output_dir: Path
    start_receipt_sha256: str | None
    terminal_receipt_sha256: str | None
    result_sha256: str | None
    artifact_manifest_sha256: str | None
    failure_receipt_sha256: str | None
    historical_bbo_available: bool = False
    paper_fill_claim: bool = False
    execution_conclusive: bool = False
    probability_claim: bool = False
    efficacy_claim: bool = False
    promoting: bool = False
    prospective: bool = False
    production_order_placement: bool = False

    def __post_init__(self) -> None:
        if self.status not in _D2_VERIFICATION_STATUS_VALUES:
            raise ValueError("D2 publication verification status is unsupported")
        if self.run_id != D2_OPERATOR_RUN_ID_V0:
            raise ValueError("D2 verification run_id is not fixed")
        if not self.attempt_dir.is_absolute() or not self.output_dir.is_absolute():
            raise ValueError("D2 verification paths must be absolute")
        for value, label in (
            (self.start_receipt_sha256, "start_receipt_sha256"),
            (self.terminal_receipt_sha256, "terminal_receipt_sha256"),
            (self.result_sha256, "result_sha256"),
            (self.artifact_manifest_sha256, "artifact_manifest_sha256"),
            (self.failure_receipt_sha256, "failure_receipt_sha256"),
        ):
            if value is not None:
                _require_sha256(value, label)
        if (self.result_sha256 is None) != (self.artifact_manifest_sha256 is None):
            raise ValueError("D2 result and artifact hashes must be paired")
        if any(asdict(self)[name] for name in _FALSE_CLAIM_FIELDS):
            raise ValueError("D2 historical verification cannot make outcome claims")
        if self.status == "OPERATIONAL_ERROR":
            raise ValueError("operational errors are not publication verification values")
        if self.status in {"COMPLETED", "FAILED"}:
            if self.reason is not None:
                raise ValueError("terminal exact verification cannot include a reason")
        elif not isinstance(self.reason, str) or _ERROR_CODE_RE.fullmatch(self.reason) is None:
            raise ValueError("non-exact D2 verification requires a fixed reason token")
        if self.status == "COMPLETED":
            if any(
                value is None
                for value in (
                    self.start_receipt_sha256,
                    self.terminal_receipt_sha256,
                    self.result_sha256,
                    self.artifact_manifest_sha256,
                )
            ) or self.failure_receipt_sha256 is not None:
                raise ValueError("completed D2 verification fields differ")
        if self.status == "FAILED":
            if (
                self.start_receipt_sha256 is None
                or self.terminal_receipt_sha256 is None
                or self.failure_receipt_sha256 is None
                or self.result_sha256 is not None
                or self.artifact_manifest_sha256 is not None
            ):
                raise ValueError("failed D2 verification fields differ")
        if self.status == "INCOMPLETE" and (
            self.terminal_receipt_sha256 is not None
            or self.result_sha256 is not None
            or self.failure_receipt_sha256 is not None
        ):
            raise ValueError("incomplete D2 verification cannot claim terminal evidence")
        if self.status == "AMBIGUOUS_FAILURE_EVIDENCE" and (
            self.start_receipt_sha256 is None or self.result_sha256 is not None
        ):
            raise ValueError("ambiguous failure evidence requires START and no result")


@dataclass(frozen=True, slots=True)
class _FailureReceiptObservationV0:
    state: Literal["ABSENT", "VALID", "INVALID"]
    publication: D2HistoricalFailureReceiptPublicationV0 | None

    def __post_init__(self) -> None:
        if (self.state == "VALID") != (self.publication is not None):
            raise ValueError("failure-receipt observation is inconsistent")


@dataclass(frozen=True, slots=True)
class _AttemptWalProtocolIdentityV0:
    directory_identity: _DirectoryIdentityV0
    wal_identity: _FileMetadataV0
    start_seal_identity: _FileMetadataV0 | None


@dataclass(frozen=True, slots=True)
class _OutputProtocolObservationV0:
    target_kind: Literal["ABSENT", "REAL_DIRECTORY", "INVALID_PRESENT"]
    protocol_orphans: bool
    target_identity: tuple[int, int, int, int, int] | None
    members: tuple[
        tuple[str, tuple[int, int, int, int, int, int], str | None], ...
    ]

    def __post_init__(self) -> None:
        if (self.target_kind == "REAL_DIRECTORY") != (
            self.target_identity is not None
        ):
            raise ValueError("output protocol observation identity differs")
        if self.target_kind != "REAL_DIRECTORY" and self.members:
            raise ValueError("non-directory output observation cannot contain members")


_FALSE_CLAIM_FIELDS: Final = (
    "historical_bbo_available",
    "paper_fill_claim",
    "execution_conclusive",
    "probability_claim",
    "efficacy_claim",
    "promoting",
    "prospective",
    "production_order_placement",
)


def create_d2_historical_input_authority_artifacts_v0(
    *,
    workspace_root: str | Path,
) -> D2HistoricalInputAuthorityArtifactsV0:
    """Publish the exact D2 authority from metadata only; never open gzip bytes."""

    try:
        root = _workspace_root(workspace_root)
        target = _workspace_member_v0(root, D2_OPERATOR_INPUT_AUTHORITY_DIR_V0)
        if not _output_protocol_is_proven_absent(target):
            raise ValueError("D2 input-authority publication protocol is not fresh")
        authority = _fixed_input_authority_v0()
        _validate_metadata_projection_sources_v0(root)
        raw = canonical_d2_historical_input_authority_v0(authority)
        _require_pinned_authority_v0(authority, raw)
        _publish_fresh_directory(
            target=target,
            files={D2_OPERATOR_INPUT_AUTHORITY_FILE_V0: raw},
        )
        return load_d2_historical_input_authority_artifacts_v0(workspace_root=root)
    except D2HistoricalOperatorErrorV0:
        raise
    except (D1HistoricalOperatorErrorV0, OSError, ValueError) as error:
        raise D2HistoricalOperatorErrorV0(
            phase="AUTHORITY_PREPARATION",
            code="D2_INPUT_AUTHORITY_PREPARATION_FAILED",
        ) from error


def load_d2_historical_input_authority_artifacts_v0(
    *,
    workspace_root: str | Path,
) -> D2HistoricalInputAuthorityArtifactsV0:
    """Reload one exact canonical D2 authority without inspecting outcome files."""

    try:
        root = _workspace_root(workspace_root)
        directory = _workspace_member_v0(root, D2_OPERATOR_INPUT_AUTHORITY_DIR_V0)
        _require_exact_directory(directory, _AUTHORITY_FILE_NAMES, "D2 input authority")
        raw = _read_stable_regular_file(
            directory / D2_OPERATOR_INPUT_AUTHORITY_FILE_V0,
            "D2 input authority",
            maximum_bytes=_MAX_METADATA_BYTES,
        )
        authority = _fixed_input_authority_v0()
        _require_pinned_authority_v0(authority, raw)
        return D2HistoricalInputAuthorityArtifactsV0(
            output_dir=directory,
            authority=authority,
            input_authority_file_sha256=hashlib.sha256(raw).hexdigest(),
            total_size_bytes=len(raw),
        )
    except D2HistoricalOperatorErrorV0:
        raise
    except (D1HistoricalOperatorErrorV0, OSError, ValueError) as error:
        raise D2HistoricalOperatorErrorV0(
            phase="AUTHORITY_VERIFICATION",
            code="D2_INPUT_AUTHORITY_VERIFICATION_FAILED",
        ) from error


def create_d2_historical_development_freeze_v0(
    *,
    workspace_root: str | Path,
) -> D2HistoricalDevelopmentFreezeV0:
    """Create and policy-check the exact broad D2 freeze once."""

    try:
        root = _workspace_root(workspace_root)
        bundle = load_d2_historical_input_authority_artifacts_v0(workspace_root=root)
        _validate_protocol_documents_v0(root)
        downstream = create_downstream_code_freeze_v1(
            workspace_root=root,
            manifest_path=D2_OPERATOR_FREEZE_MANIFEST_V0,
            purpose=D2_DEVELOPMENT_FREEZE_PURPOSE_V0,
            include_trees=D2_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0,
            include_files=D2_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0,
            included_suffixes=D2_DEVELOPMENT_FREEZE_SUFFIXES_V0,
            upstream_sha256=d2_historical_development_freeze_upstream_v0(
                bundle.authority.authority_sha256
            ),
        )
        return load_d2_historical_development_freeze_v0(
            D2_OPERATOR_FREEZE_MANIFEST_V0,
            workspace_root=root,
            expected_manifest_sha256=downstream.manifest_sha256,
            input_authority=bundle.authority,
        )
    except D2HistoricalOperatorErrorV0:
        raise
    except (OSError, ValueError) as error:
        raise D2HistoricalOperatorErrorV0(
            phase="FREEZE_CREATION",
            code="D2_CODE_FREEZE_CREATION_FAILED",
        ) from error


def arm_d2_historical_development_attempt_v0(
    *,
    workspace_root: str | Path,
    expected_freeze_manifest_sha256: str,
) -> D2HistoricalDevelopmentAttemptArmV0:
    """Validate metadata/freeze and durably create one outcome-blind ARMED WAL."""

    expected_freeze = _validated_expected_freeze_sha256_v0(
        expected_freeze_manifest_sha256,
        phase="FREEZE_VERIFICATION",
    )
    try:
        root = _workspace_root(workspace_root)
        attempt_dir = _workspace_member_v0(root, D2_OPERATOR_ATTEMPT_DIR_V0)
        output_dir = _workspace_member_v0(root, D2_OPERATOR_OUTPUT_DIR_V0)
        failure_dir = _workspace_member_v0(root, D2_OPERATOR_FAILURE_RECEIPT_DIR_V0)
        if attempt_dir.exists() or attempt_dir.is_symlink():
            raise ValueError("D2 attempt reservation is not fresh")
        if not _output_protocol_is_proven_absent(output_dir):
            raise ValueError("D2 output publication protocol is not fresh")
        if not _output_protocol_is_proven_absent(failure_dir):
            raise ValueError("D2 failure-receipt publication protocol is not fresh")
        bundle = load_d2_historical_input_authority_artifacts_v0(workspace_root=root)
        _validate_protocol_documents_v0(root)
        freeze = load_d2_historical_development_freeze_v0(
            D2_OPERATOR_FREEZE_MANIFEST_V0,
            workspace_root=root,
            expected_manifest_sha256=expected_freeze,
            input_authority=bundle.authority,
        )
        if attempt_dir.exists() or attempt_dir.is_symlink():
            raise ValueError("D2 attempt reservation changed while arming")
        if not _output_protocol_is_proven_absent(output_dir):
            raise ValueError("D2 output protocol changed while arming")
        if not _output_protocol_is_proven_absent(failure_dir):
            raise ValueError("D2 failure protocol changed while arming")
        armed_at_ms = _now_ms_v0()
        if armed_at_ms < freeze.manifest_created_at_ms:
            raise ValueError("D2 arm clock precedes the code freeze")
        snapshot = create_armed_wal_v0(
            attempt_dir=attempt_dir,
            bindings=_attempt_wal_bindings_v0(bundle=bundle, freeze=freeze),
            armed_at_ms=armed_at_ms,
        )
        _require_exact_armed_snapshot_v0(snapshot)
        return D2HistoricalDevelopmentAttemptArmV0(
            attempt_dir=attempt_dir,
            armed_record_sha256=snapshot.records[0].record_sha256,
            code_freeze_manifest_sha256=freeze.manifest_sha256,
        )
    except D2HistoricalOperatorErrorV0:
        raise
    except (
        D1HistoricalAttemptWalErrorV0,
        D1HistoricalOperatorErrorV0,
        OSError,
        ValueError,
    ) as error:
        raise D2HistoricalOperatorErrorV0(
            phase="ATTEMPT_ARM",
            code="D2_ATTEMPT_ARM_FAILED",
        ) from error


def run_and_publish_d2_historical_development_once_v0(
    *,
    workspace_root: str | Path,
    expected_freeze_manifest_sha256: str,
) -> D2HistoricalDevelopmentPublicationVerificationV0:
    """Cross START once, pass the fresh grant directly, publish, and verify."""

    expected_freeze = _validated_expected_freeze_sha256_v0(
        expected_freeze_manifest_sha256,
        phase="RECOVERY_VERIFICATION",
    )
    root = _safe_workspace_root_v0(workspace_root, phase="RECOVERY_VERIFICATION")
    attempt_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_ATTEMPT_DIR_V0,
        phase="RECOVERY_VERIFICATION",
    )
    output_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_OUTPUT_DIR_V0,
        phase="RECOVERY_VERIFICATION",
    )
    failure_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_FAILURE_RECEIPT_DIR_V0,
        phase="RECOVERY_VERIFICATION",
    )

    # The permanent attempt gate intentionally precedes authority/freeze/data reads.
    try:
        armed = load_attempt_wal_v0(attempt_dir)
        _require_exact_armed_snapshot_v0(armed)
        if not _output_protocol_is_proven_absent(output_dir):
            raise ValueError("D2 ARMED attempt has output protocol state")
        if not _output_protocol_is_proven_absent(failure_dir):
            raise ValueError("D2 ARMED attempt has failure-receipt protocol state")
    except (
        D1HistoricalAttemptWalErrorV0,
        D1HistoricalOperatorErrorV0,
        OSError,
        ValueError,
    ) as error:
        raise D2HistoricalOperatorErrorV0(
            phase="RECOVERY_VERIFICATION",
            code="D2_ATTEMPT_NOT_SOLE_ARMED",
        ) from error

    bundle, freeze = _load_bound_authority_and_freeze_v0(
        root=root,
        expected_freeze_manifest_sha256=expected_freeze,
    )
    bindings = _attempt_wal_bindings_v0(bundle=bundle, freeze=freeze)
    if armed.bindings != bindings:
        raise D2HistoricalOperatorErrorV0(
            phase="FREEZE_VERIFICATION",
            code="D2_ARMED_BINDINGS_MISMATCH",
        )
    run_started_at_ms = _now_ms_v0()
    if run_started_at_ms < max(
        freeze.manifest_created_at_ms,
        armed.records[0].observed_at_ms,
    ):
        raise D2HistoricalOperatorErrorV0(
            phase="START_APPEND",
            code="D2_START_CLOCK_PRECEDES_AUTHORITY",
        )
    try:
        start_result = append_started_v0(
            attempt_dir=attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=run_started_at_ms,
        )
    except D1HistoricalAttemptWalErrorV0 as error:
        raise D2HistoricalOperatorErrorV0(
            phase="START_APPEND",
            code="D2_START_APPEND_FAILED_OR_UNCERTAIN_NO_RETRY",
        ) from error

    started = start_result.snapshot
    grant = start_result.outcome_access_grant
    phase: D2OperatorPhaseV0 = "OUTCOME_REPLAY"
    try:
        result = run_d2_historical_development_v0(
            data_root=root,
            input_authority=bundle.authority,
            code_freeze=freeze,
            outcome_access_grant=grant,
            run_id=D2_OPERATOR_RUN_ID_V0,
            run_started_at_ms=run_started_at_ms,
        )
        phase = "ARTIFACT_PUBLICATION"
        artifacts = write_d2_historical_development_artifacts_v0(
            result=result,
            input_authority=bundle.authority,
            code_freeze=freeze,
            output_dir=output_dir,
        )
        phase = "SERIALIZED_ARTIFACT_VERIFICATION"
        serialized = verify_d2_historical_serialized_artifacts_v0(
            output_dir=output_dir,
            expected_result=result,
            expected_input_authority=bundle.authority,
            expected_code_freeze=freeze,
        )
        if serialized.result_sha256 != artifacts.result_sha256:
            raise ValueError("D2 in-memory and serialized result identities differ")
    except Exception as error:
        _raise_after_post_start_failure_v0(
            root=root,
            started=started,
            grant=grant,
            phase=phase,
            original_error=error,
            run_started_at_ms=run_started_at_ms,
        )

    phase = "COMPLETED_WAL_APPEND"
    try:
        append_terminal_v0(
            attempt_dir=attempt_dir,
            expected_prefix=started.prefix,
            state="COMPLETED",
            terminal_at_ms=_terminal_time_ms_v0(run_started_at_ms),
            result_sha256=artifacts.result_sha256,
            artifact_manifest_sha256=artifacts.manifest_sha256,
        )
    except Exception as error:
        _raise_after_post_start_failure_v0(
            root=root,
            started=started,
            grant=grant,
            phase=phase,
            original_error=error,
            run_started_at_ms=run_started_at_ms,
        )

    try:
        verification = verify_d2_historical_development_publication_v0(
            workspace_root=root,
            expected_freeze_manifest_sha256=expected_freeze,
        )
        if verification.status != "COMPLETED":
            raise ValueError("post-COMPLETED D2 verification is not COMPLETED")
        return verification
    except Exception as error:
        _raise_after_post_start_failure_v0(
            root=root,
            started=started,
            grant=grant,
            phase="POST_COMPLETION_VERIFICATION",
            original_error=error,
            run_started_at_ms=run_started_at_ms,
        )


def _observe_attempt_wal_protocol_identity_v0(
    attempt_dir: Path,
) -> _AttemptWalProtocolIdentityV0:
    directory_before = _require_attempt_directory(attempt_dir, require_wal=True)
    wal_identity = _metadata_from_path(attempt_dir / D1_ATTEMPT_WAL_FILE_V0)
    seal_metadata = _lstat_or_none(attempt_dir / D1_ATTEMPT_START_SEAL_FILE_V0)
    seal_identity = None if seal_metadata is None else _file_metadata(seal_metadata)
    directory_after = _require_attempt_directory(attempt_dir, require_wal=True)
    if directory_after != directory_before:
        raise ValueError("attempt WAL directory identity changed during observation")
    return _AttemptWalProtocolIdentityV0(
        directory_identity=directory_after,
        wal_identity=wal_identity,
        start_seal_identity=seal_identity,
    )


def _load_attempt_protocol_snapshot_v0(
    *,
    attempt_dir: Path,
    expected_bindings: D1AttemptWalBindingsV0,
) -> tuple[D1AttemptWalSnapshotV0, _AttemptWalProtocolIdentityV0]:
    identity_before = _observe_attempt_wal_protocol_identity_v0(attempt_dir)
    snapshot = load_attempt_wal_v0(
        attempt_dir,
        expected_bindings=expected_bindings,
    )
    identity_after = _observe_attempt_wal_protocol_identity_v0(attempt_dir)
    if identity_after != identity_before:
        raise ValueError("attempt WAL tree or member identity changed during load")
    return snapshot, identity_after


def _output_member_identity_v0(
    metadata: os.stat_result,
) -> tuple[int, int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
        getattr(metadata, "st_file_attributes", 0),
    )


def _observe_output_protocol_v0(
    output_dir: Path,
    *,
    require_exact_bundle: bool,
) -> _OutputProtocolObservationV0:
    target_kind = _canonical_output_target_kind(output_dir)
    protocol_orphans = _output_protocol_has_orphans(output_dir)
    target_identity = (
        _real_directory_identity(output_dir, "D2 publication output")
        if target_kind == "REAL_DIRECTORY"
        else None
    )
    members: list[
        tuple[str, tuple[int, int, int, int, int, int], str | None]
    ] = []
    if target_kind == "REAL_DIRECTORY":
        try:
            with os.scandir(output_dir) as entries:
                snapshot_entries = tuple(entries)
        except OSError as error:
            raise ValueError("output membership cannot be observed") from error
        if len(snapshot_entries) > _MAX_OUTPUT_PROTOCOL_ENTRIES:
            raise ValueError("output membership exceeds its observation cap")
        observed_names = frozenset(entry.name for entry in snapshot_entries)
        if (
            require_exact_bundle
            and observed_names != _D2_COMPLETED_OUTPUT_FILE_NAMES_V0
        ):
            raise ValueError("COMPLETED output membership differs")
        total_regular_bytes = 0
        for entry in sorted(snapshot_entries, key=lambda value: value.name):
            try:
                before = (output_dir / entry.name).stat(follow_symlinks=False)
            except OSError as error:
                raise ValueError("output member cannot be inspected") from error
            is_exact_regular = not _is_link_or_reparse(before) and stat.S_ISREG(
                before.st_mode
            )
            if require_exact_bundle and not is_exact_regular:
                raise ValueError("COMPLETED output member is not an exact regular file")
            digest: str | None = None
            if is_exact_regular and require_exact_bundle:
                if before.st_size < 0:
                    raise ValueError("COMPLETED output member size is invalid")
                total_regular_bytes += before.st_size
                if total_regular_bytes > D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0:
                    raise ValueError("COMPLETED output exceeds its aggregate byte cap")
                digest, observed_size = _hash_stable_regular_file(
                    output_dir / entry.name,
                    f"D2 publication output member {entry.name}",
                    maximum_bytes=D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0,
                )
                if observed_size != before.st_size:
                    raise ValueError("COMPLETED output member size changed")
            try:
                after = (output_dir / entry.name).stat(follow_symlinks=False)
            except OSError as error:
                raise ValueError("output member disappeared during observation") from error
            before_identity = _output_member_identity_v0(before)
            if _output_member_identity_v0(after) != before_identity:
                raise ValueError("output member identity changed during observation")
            members.append((entry.name, before_identity, digest))
        try:
            with os.scandir(output_dir) as entries:
                final_names = frozenset(entry.name for entry in entries)
        except OSError as error:
            raise ValueError("output membership cannot be reobserved") from error
        if final_names != observed_names:
            raise ValueError("output membership changed during observation")
    target_identity_after = (
        _real_directory_identity(output_dir, "D2 publication output")
        if target_kind == "REAL_DIRECTORY"
        else None
    )
    if (
        _canonical_output_target_kind(output_dir) != target_kind
        or target_identity_after != target_identity
    ):
        raise ValueError("output target kind changed during observation")
    return _OutputProtocolObservationV0(
        target_kind=target_kind,
        protocol_orphans=protocol_orphans,
        target_identity=target_identity,
        members=tuple(members),
    )


def verify_d2_historical_development_publication_v0(
    *,
    workspace_root: str | Path,
    expected_freeze_manifest_sha256: str,
) -> D2HistoricalDevelopmentPublicationVerificationV0:
    """Read-only recovery classification with independent serialized verification."""

    expected_freeze = _validated_expected_freeze_sha256_v0(
        expected_freeze_manifest_sha256,
        phase="RECOVERY_VERIFICATION",
    )
    root = _safe_workspace_root_v0(workspace_root, phase="RECOVERY_VERIFICATION")
    attempt_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_ATTEMPT_DIR_V0,
        phase="RECOVERY_VERIFICATION",
    )
    output_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_OUTPUT_DIR_V0,
        phase="RECOVERY_VERIFICATION",
    )
    failure_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_FAILURE_RECEIPT_DIR_V0,
        phase="RECOVERY_VERIFICATION",
    )
    bundle, freeze = _load_bound_authority_and_freeze_v0(
        root=root,
        expected_freeze_manifest_sha256=expected_freeze,
    )
    expected_bindings = _attempt_wal_bindings_v0(bundle=bundle, freeze=freeze)
    try:
        snapshot, attempt_identity = _load_attempt_protocol_snapshot_v0(
            attempt_dir=attempt_dir,
            expected_bindings=expected_bindings,
        )
    except (D1HistoricalAttemptWalErrorV0, OSError, ValueError) as error:
        raise D2HistoricalOperatorErrorV0(
            phase="RECOVERY_VERIFICATION",
            code="D2_ATTEMPT_WAL_VERIFICATION_FAILED",
        ) from error
    receipt = _observe_failure_receipt_v0(failure_dir)
    try:
        output_protocol = _observe_output_protocol_v0(
            output_dir,
            require_exact_bundle=snapshot.last_state == "COMPLETED",
        )
    except (D1HistoricalOperatorErrorV0, OSError, ValueError) as error:
        raise D2HistoricalOperatorErrorV0(
            phase="RECOVERY_VERIFICATION",
            code="D2_OUTPUT_PROTOCOL_OBSERVATION_FAILED",
            verification_status=(
                "AMBIGUOUS_OUTPUT"
                if snapshot.last_state == "COMPLETED"
                else "OPERATIONAL_ERROR"
            ),
        ) from error
    start = _start_record_v0(snapshot)
    if start is None and receipt.state != "ABSENT":
        raise D2HistoricalOperatorErrorV0(
            phase="RECOVERY_VERIFICATION",
            code="ORPHAN_FAILURE_RECEIPT_BEFORE_START",
        )
    try:
        verification = _verification_from_snapshot_v0(
            snapshot=snapshot,
            start=start,
            attempt_dir=attempt_dir,
            output_dir=output_dir,
            failure_receipt=receipt,
            bundle=bundle,
            freeze=freeze,
        )
        if verification.status == "COMPLETED":
            try:
                final_snapshot, final_attempt_identity = (
                    _load_attempt_protocol_snapshot_v0(
                        attempt_dir=attempt_dir,
                        expected_bindings=expected_bindings,
                    )
                )
                final_receipt = _observe_failure_receipt_v0(failure_dir)
                final_output_protocol = _observe_output_protocol_v0(
                    output_dir,
                    require_exact_bundle=True,
                )
                final_bundle, final_freeze = _load_bound_authority_and_freeze_v0(
                    root=root,
                    expected_freeze_manifest_sha256=expected_freeze,
                )
            except (
                D1HistoricalAttemptWalErrorV0,
                D1HistoricalOperatorErrorV0,
                OSError,
                ValueError,
            ) as error:
                raise D2HistoricalOperatorErrorV0(
                    phase="RECOVERY_VERIFICATION",
                    code="D2_PUBLICATION_PROTOCOL_CHANGED_DURING_VERIFICATION",
                    verification_status="AMBIGUOUS_OUTPUT",
                ) from error
            if (
                final_snapshot != snapshot
                or final_attempt_identity != attempt_identity
                or final_receipt != receipt
                or final_output_protocol != output_protocol
                or final_bundle != bundle
                or final_freeze != freeze
            ):
                failure_receipt_sha256 = (
                    final_receipt.publication.receipt_file_sha256
                    if final_receipt.state == "VALID"
                    and final_receipt.publication is not None
                    else None
                )
                raise D2HistoricalOperatorErrorV0(
                    phase="RECOVERY_VERIFICATION",
                    code="D2_PUBLICATION_PROTOCOL_CHANGED_DURING_VERIFICATION",
                    verification_status="AMBIGUOUS_OUTPUT",
                    failure_receipt_sha256=failure_receipt_sha256,
                )
        return verification
    except D2HistoricalOperatorErrorV0:
        raise
    except (D1HistoricalOperatorErrorV0, OSError, ValueError) as error:
        raise D2HistoricalOperatorErrorV0(
            phase="RECOVERY_VERIFICATION",
            code="D2_PUBLICATION_RECOVERY_VERIFICATION_FAILED",
        ) from error


def verify_d2_historical_development_reproduction_v0(
    *,
    workspace_root: str | Path,
    expected_freeze_manifest_sha256: str,
) -> D2HistoricalReproductionVerificationV0:
    """Re-run raw inputs read-only only after an exact COMPLETED publication."""

    expected_freeze = _validated_expected_freeze_sha256_v0(
        expected_freeze_manifest_sha256,
        phase="REPRODUCTION_VERIFICATION",
    )
    root = _safe_workspace_root_v0(
        workspace_root,
        phase="REPRODUCTION_VERIFICATION",
    )
    primary = verify_d2_historical_development_publication_v0(
        workspace_root=root,
        expected_freeze_manifest_sha256=expected_freeze,
    )
    if primary.status != "COMPLETED":
        raise D2HistoricalOperatorErrorV0(
            phase="REPRODUCTION_VERIFICATION",
            code="D2_REPRODUCTION_REQUIRES_EXACT_COMPLETED_PUBLICATION",
            verification_status=primary.status,
            failure_receipt_sha256=primary.failure_receipt_sha256,
        )
    attempt_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_ATTEMPT_DIR_V0,
        phase="REPRODUCTION_VERIFICATION",
    )
    output_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_OUTPUT_DIR_V0,
        phase="REPRODUCTION_VERIFICATION",
    )
    bundle, freeze = _load_bound_authority_and_freeze_v0(
        root=root,
        expected_freeze_manifest_sha256=expected_freeze,
    )
    bindings = _attempt_wal_bindings_v0(bundle=bundle, freeze=freeze)
    try:
        reproduced = reproduce_d2_historical_published_artifact_bundle_v0(
            data_root=root,
            attempt_dir=attempt_dir,
            output_dir=output_dir,
            expected_attempt_bindings=bindings,
            expected_input_authority=bundle.authority,
            expected_code_freeze=freeze,
        )
    except (D1HistoricalAttemptWalErrorV0, OSError, ValueError) as error:
        raise D2HistoricalOperatorErrorV0(
            phase="REPRODUCTION_VERIFICATION",
            code="D2_RAW_REPRODUCTION_FAILED",
        ) from error
    try:
        post_replay_primary = verify_d2_historical_development_publication_v0(
            workspace_root=root,
            expected_freeze_manifest_sha256=expected_freeze,
        )
    except D2HistoricalOperatorErrorV0 as error:
        raise D2HistoricalOperatorErrorV0(
            phase="REPRODUCTION_VERIFICATION",
            code="D2_REPRODUCTION_POST_REPLAY_PUBLICATION_VERIFICATION_FAILED",
            verification_status=error.verification_status,
            failure_receipt_sha256=error.failure_receipt_sha256,
        ) from error
    if post_replay_primary != primary:
        raise D2HistoricalOperatorErrorV0(
            phase="REPRODUCTION_VERIFICATION",
            code="D2_REPRODUCTION_PRIMARY_CHANGED_DURING_REPLAY",
            verification_status=post_replay_primary.status,
            failure_receipt_sha256=post_replay_primary.failure_receipt_sha256,
        )
    if not (
        reproduced.run_id == D2_OPERATOR_RUN_ID_V0
        and reproduced.start_record_sha256 == primary.start_receipt_sha256
        and reproduced.completed_record_sha256 == primary.terminal_receipt_sha256
        and reproduced.result_sha256 == primary.result_sha256
        and reproduced.artifact_manifest_sha256 == primary.artifact_manifest_sha256
        and reproduced.raw_replay_performed is True
        and reproduced.published_artifacts_modified is False
        and reproduced.production_order_placement is False
    ):
        raise D2HistoricalOperatorErrorV0(
            phase="REPRODUCTION_VERIFICATION",
            code="D2_REPRODUCTION_RECEIPT_DIFFERS_FROM_COMPLETED_PUBLICATION",
        )
    return reproduced


def _fixed_input_authority_v0() -> D2HistoricalInputAuthorityV0:
    five_minute = tuple(
        D1HistoricalKlineManifestBindingV0(
            symbol=symbol,
            interval="5m",
            relative_manifest_path=relative_path,
            manifest_sha256=manifest_sha256,
        )
        for symbol, relative_path, manifest_sha256 in (
            D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0
        )
    )
    funding = tuple(
        D1HistoricalFundingFileBindingV0(
            symbol=symbol,
            relative_path=relative_path,
            sha256=sha256,
        )
        for symbol, relative_path, sha256 in D2_HISTORICAL_FIXED_FUNDING_FILES_V0
    )
    return build_d2_historical_input_authority_v0(
        five_minute_manifests=five_minute,
        funding_manifest_relative_path=(
            D2_HISTORICAL_FIXED_FUNDING_MANIFEST_RELATIVE_PATH_V0
        ),
        funding_manifest_sha256=D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0,
        funding_files=funding,
    )


def _validate_metadata_projection_sources_v0(root: Path) -> None:
    """Verify only metadata identities; every ``.csv.gz`` path stays unopened."""

    d1_authority = _read_stable_regular_file(
        _workspace_member_v0(
            root,
            "artifacts/backtest/2026-07-21-d1-scefb-v0-input-authority/"
            "input_authority.jsonl",
        ),
        "fixed predecessor D1 input authority",
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    if hashlib.sha256(d1_authority).hexdigest() != D1_PREDECESSOR_INPUT_AUTHORITY_FILE_SHA256_V0:
        raise ValueError("fixed predecessor D1 authority file differs")
    funding = _read_stable_regular_file(
        _workspace_member_v0(
            root,
            D2_HISTORICAL_FIXED_FUNDING_MANIFEST_RELATIVE_PATH_V0,
        ),
        "fixed funding metadata authority",
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    if hashlib.sha256(funding).hexdigest() != D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0:
        raise ValueError("fixed funding metadata authority differs")
    for symbol, relative_path, expected_sha256 in (
        D2_HISTORICAL_FIXED_FIVE_MINUTE_MANIFESTS_V0
    ):
        if not relative_path.endswith("__5m.csv.gz.manifest.json") or "__1h" in relative_path:
            raise ValueError("D2 metadata projection admitted a native 1h path")
        raw = _read_stable_regular_file(
            _workspace_member_v0(root, relative_path),
            f"{symbol} fixed 5m sidecar metadata",
            maximum_bytes=_MAX_METADATA_BYTES,
        )
        if hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise ValueError("fixed 5m sidecar metadata differs")


def _require_pinned_authority_v0(
    authority: D2HistoricalInputAuthorityV0,
    raw: bytes,
) -> None:
    expected_raw = canonical_d2_historical_input_authority_v0(authority)
    if (
        raw != expected_raw
        or authority.authority_sha256 != D2_OPERATOR_EXPECTED_INPUT_AUTHORITY_SHA256_V0
        or hashlib.sha256(raw).hexdigest()
        != D2_OPERATOR_EXPECTED_INPUT_AUTHORITY_FILE_SHA256_V0
        or len(raw) != D2_OPERATOR_EXPECTED_INPUT_AUTHORITY_SIZE_BYTES_V0
        or D2_HISTORICAL_FIXED_INPUT_PROJECTION_SHA256_V0
        != "fa3f9c4c4ccfdf086348abe7f9277bf369531d18ac07b763d86ceb5727dc7472"
        or D2_HISTORICAL_SOURCE_POLICY_SHA256_V0
        != "52a83f2a4e2e6c28a33ebfac7a0fa8726d80db0c93798088c9d92af2c3e79b19"
    ):
        raise ValueError("D2 authority differs from its fixed metadata-only identity")


def _validate_protocol_documents_v0(root: Path) -> None:
    for relative_path, expected, label in (
        (
            D2_OPERATOR_PREREGISTRATION_FILE_V0,
            D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
            "D2 preregistration",
        ),
        (
            D2_OPERATOR_FAILURE_AMENDMENT_FILE_V0,
            D2_HISTORICAL_OPERATOR_AMENDMENT_SHA256_V0,
            "D2 failure-receipt amendment",
        ),
        (
            D2_OPERATOR_FAILURE_CORRECTION_FILE_V0,
            D2_OPERATOR_EXPECTED_FAILURE_CORRECTION_SHA256_V0,
            "D2 failure-receipt correction",
        ),
        (
            "docs/r4b-v2-d1-scefb-5m-preregistration-v0.md",
            D1_PREDECESSOR_PREREGISTRATION_SHA256_V0,
            "D1 economic preregistration",
        ),
        (
            "artifacts/backtest/2026-07-21-d1-scefb-v0-development-freeze-002/"
            "freeze_manifest.json",
            D1_PREDECESSOR_FREEZE_SHA256_V0,
            "D1 predecessor freeze",
        ),
        (
            "artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-"
            "failure-evidence/evidence-manifest.jsonl",
            D1_PREDECESSOR_FAILURE_EVIDENCE_MANIFEST_SHA256_V0,
            "D1 failure-evidence manifest",
        ),
    ):
        raw = _read_stable_regular_file(
            _workspace_member_v0(root, relative_path),
            label,
            maximum_bytes=_MAX_METADATA_BYTES,
        )
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError(f"{label} hash differs")
    archive_sha256, _archive_size = _hash_stable_regular_file(
        _workspace_member_v0(
            root,
            "artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-"
            "failure-evidence/frozen-failure-evidence.zip",
        ),
        "D1 frozen failure-evidence archive",
        maximum_bytes=_MAX_FROZEN_EVIDENCE_BYTES,
    )
    if archive_sha256 != D1_PREDECESSOR_FAILURE_EVIDENCE_ARCHIVE_SHA256_V0:
        raise ValueError("D1 frozen failure-evidence archive hash differs")
    if (
        D2_HISTORICAL_OPERATOR_CORRECTION_A1_SHA256_V0
        != D2_OPERATOR_EXPECTED_FAILURE_CORRECTION_SHA256_V0
    ):
        raise ValueError("D2 failure-receipt correction constant differs")


def _load_bound_authority_and_freeze_v0(
    *,
    root: Path,
    expected_freeze_manifest_sha256: str,
) -> tuple[D2HistoricalInputAuthorityArtifactsV0, D2HistoricalDevelopmentFreezeV0]:
    try:
        bundle = load_d2_historical_input_authority_artifacts_v0(workspace_root=root)
        _validate_protocol_documents_v0(root)
        freeze = load_d2_historical_development_freeze_v0(
            D2_OPERATOR_FREEZE_MANIFEST_V0,
            workspace_root=root,
            expected_manifest_sha256=expected_freeze_manifest_sha256,
            input_authority=bundle.authority,
        )
        return bundle, freeze
    except D2HistoricalOperatorErrorV0:
        raise
    except (D1HistoricalOperatorErrorV0, OSError, ValueError) as error:
        raise D2HistoricalOperatorErrorV0(
            phase="FREEZE_VERIFICATION",
            code="D2_BOUND_AUTHORITY_OR_FREEZE_INVALID",
        ) from error


def _attempt_wal_bindings_v0(
    *,
    bundle: D2HistoricalInputAuthorityArtifactsV0,
    freeze: D2HistoricalDevelopmentFreezeV0,
) -> D1AttemptWalBindingsV0:
    output_path_sha256 = hashlib.sha256(
        _OUTPUT_PATH_HASH_DOMAIN + D2_OPERATOR_OUTPUT_DIR_V0.encode("utf-8")
    ).hexdigest()
    return D1AttemptWalBindingsV0(
        run_id=D2_OPERATOR_RUN_ID_V0,
        code_freeze_manifest_sha256=freeze.manifest_sha256,
        input_authority_sha256=bundle.authority.authority_sha256,
        input_authority_file_sha256=bundle.input_authority_file_sha256,
        funding_authority_file_sha256=(
            D2_HISTORICAL_FIXED_FUNDING_MANIFEST_SHA256_V0
        ),
        preregistration_sha256=D2_HISTORICAL_PREREGISTRATION_SHA256_V0,
        output_path_sha256=output_path_sha256,
    )


def _require_exact_armed_snapshot_v0(snapshot: D1AttemptWalSnapshotV0) -> None:
    if (
        snapshot.torn_tail is not None
        or len(snapshot.records) != 1
        or snapshot.last_state != "ARMED"
    ):
        raise ValueError("D2 attempt is not the sole intact ARMED state")


def _start_record_v0(snapshot: D1AttemptWalSnapshotV0) -> D1AttemptWalRecordV0 | None:
    if len(snapshot.records) < 2:
        return None
    start = snapshot.records[1]
    if start.state != "STARTED_BEFORE_OUTCOME_ACCESS":
        raise D2HistoricalOperatorErrorV0(
            phase="RECOVERY_VERIFICATION",
            code="D2_ATTEMPT_START_SEQUENCE_INVALID",
        )
    return start


def _build_failure_receipt_v0(
    *,
    start: D1AttemptWalRecordV0,
    grant: D1OutcomeAccessGrantV0,
    phase: D2OperatorPhaseV0,
    error_code: str,
    output_protocol_state: D2OutputProtocolStateV0,
    observed_at_ms: int,
    clock_clamped_to_start: bool = False,
) -> D2HistoricalFailureReceiptV0:
    terminal: D2FailureTerminalStateV0 = (
        "FAILED" if output_protocol_state == "PROVEN_ABSENT" else "AMBIGUOUS_OUTPUT"
    )
    body = _failure_receipt_body_document_v0(
        run_id=D2_OPERATOR_RUN_ID_V0,
        phase=phase,
        error_code=error_code,
        context={
            "clock_clamped_to_start": clock_clamped_to_start,
            "native_1h_outcome_opened": False,
            "outcome_access_grant_consumed": grant.consumed,
            "source_policy_sha256": D2_HISTORICAL_SOURCE_POLICY_SHA256_V0,
        },
        start_record_sha256=start.record_sha256,
        bindings_sha256=start.bindings_sha256,
        attempt_directory_sha256=start.attempt_directory_sha256,
        planned_terminal_state=terminal,
        output_protocol_state=output_protocol_state,
        observed_at_ms=observed_at_ms,
    )
    body_sha256 = hashlib.sha256(
        _FAILURE_RECEIPT_BODY_HASH_DOMAIN + canonical_json_line(body)
    ).hexdigest()
    return D2HistoricalFailureReceiptV0(
        run_id=D2_OPERATOR_RUN_ID_V0,
        phase=phase,
        error_code=error_code,
        context=cast(Mapping[str, object], body["context"]),
        start_record_sha256=start.record_sha256,
        bindings_sha256=start.bindings_sha256,
        attempt_directory_sha256=start.attempt_directory_sha256,
        planned_terminal_state=terminal,
        output_protocol_state=output_protocol_state,
        observed_at_ms=observed_at_ms,
        receipt_body_sha256=body_sha256,
    )


def canonical_d2_historical_failure_receipt_v0(
    receipt: D2HistoricalFailureReceiptV0,
) -> bytes:
    """Serialize the A1 two-layer receipt after revalidating its body hash."""

    if type(receipt) is not D2HistoricalFailureReceiptV0:
        raise ValueError("failure receipt must be exact D2HistoricalFailureReceiptV0")
    body = _failure_receipt_body_document_v0(
        run_id=receipt.run_id,
        phase=receipt.phase,
        error_code=receipt.error_code,
        context=receipt.context,
        start_record_sha256=receipt.start_record_sha256,
        bindings_sha256=receipt.bindings_sha256,
        attempt_directory_sha256=receipt.attempt_directory_sha256,
        planned_terminal_state=receipt.planned_terminal_state,
        output_protocol_state=receipt.output_protocol_state,
        observed_at_ms=receipt.observed_at_ms,
    )
    expected = hashlib.sha256(
        _FAILURE_RECEIPT_BODY_HASH_DOMAIN + canonical_json_line(body)
    ).hexdigest()
    if receipt.receipt_body_sha256 != expected:
        raise ValueError("failure receipt body hash differs")
    return canonical_json_line({**body, "receipt_body_sha256": expected})


def _failure_receipt_body_document_v0(
    *,
    run_id: str,
    phase: D2OperatorPhaseV0,
    error_code: str,
    context: Mapping[str, object],
    start_record_sha256: str,
    bindings_sha256: str,
    attempt_directory_sha256: str,
    planned_terminal_state: D2FailureTerminalStateV0,
    output_protocol_state: D2OutputProtocolStateV0,
    observed_at_ms: int,
) -> dict[str, object]:
    return {
        "attempt_directory_sha256": attempt_directory_sha256,
        "bindings_sha256": bindings_sha256,
        "context": dict(context),
        "error_code": error_code,
        "observed_at_ms": observed_at_ms,
        "output_protocol_state": output_protocol_state,
        "phase": phase,
        "planned_terminal_state": planned_terminal_state,
        "production_order_placement": False,
        "run_id": run_id,
        "schema_version": _FAILURE_RECEIPT_SCHEMA_V0,
        "start_record_sha256": start_record_sha256,
    }


def _publish_failure_receipt_v0(
    *,
    target: Path,
    receipt: D2HistoricalFailureReceiptV0,
) -> D2HistoricalFailureReceiptPublicationV0:
    raw = canonical_d2_historical_failure_receipt_v0(receipt)
    if len(raw) > _MAX_FAILURE_RECEIPT_BYTES:
        raise ValueError("D2 failure receipt exceeds its byte cap")
    _publish_fresh_directory(
        target=target,
        files={D2_OPERATOR_FAILURE_RECEIPT_FILE_V0: raw},
    )
    observed = _observe_failure_receipt_v0(target)
    if observed.state != "VALID" or observed.publication is None:
        raise ValueError("published D2 failure receipt cannot be revalidated")
    if observed.publication.receipt != receipt:
        raise ValueError("published D2 failure receipt differs")
    return observed.publication


def _observe_failure_receipt_v0(target: Path) -> _FailureReceiptObservationV0:
    try:
        kind = _canonical_output_target_kind(target)
        if kind == "ABSENT":
            return _FailureReceiptObservationV0(state="ABSENT", publication=None)
        if kind != "REAL_DIRECTORY" or _output_protocol_has_orphans(target):
            return _FailureReceiptObservationV0(state="INVALID", publication=None)
        _require_exact_directory(target, _FAILURE_RECEIPT_FILE_NAMES, "D2 failure receipt")
        raw = _read_stable_regular_file(
            target / D2_OPERATOR_FAILURE_RECEIPT_FILE_V0,
            "D2 failure receipt",
            maximum_bytes=_MAX_FAILURE_RECEIPT_BYTES,
        )
        receipt = _parse_failure_receipt_v0(raw)
        if canonical_d2_historical_failure_receipt_v0(receipt) != raw:
            return _FailureReceiptObservationV0(state="INVALID", publication=None)
        return _FailureReceiptObservationV0(
            state="VALID",
            publication=D2HistoricalFailureReceiptPublicationV0(
                receipt=receipt,
                receipt_file_sha256=hashlib.sha256(raw).hexdigest(),
            ),
        )
    except (D1HistoricalOperatorErrorV0, OSError, ValueError):
        return _FailureReceiptObservationV0(state="INVALID", publication=None)


def _parse_failure_receipt_v0(raw: bytes) -> D2HistoricalFailureReceiptV0:
    document = _decode_canonical_object_v0(raw)
    expected_keys = {
        "attempt_directory_sha256",
        "bindings_sha256",
        "context",
        "error_code",
        "observed_at_ms",
        "output_protocol_state",
        "phase",
        "planned_terminal_state",
        "production_order_placement",
        "receipt_body_sha256",
        "run_id",
        "schema_version",
        "start_record_sha256",
    }
    if set(document) != expected_keys:
        raise ValueError("failure receipt fields differ")
    phase = document.get("phase")
    terminal = document.get("planned_terminal_state")
    output_state = document.get("output_protocol_state")
    context = document.get("context")
    if phase not in _D2_PHASE_VALUES or terminal not in {"FAILED", "AMBIGUOUS_OUTPUT"}:
        raise ValueError("failure receipt enum field differs")
    if output_state not in {"PROVEN_ABSENT", "PRESENT_OR_UNCERTAIN"}:
        raise ValueError("failure receipt output enum differs")
    if not isinstance(context, dict):
        raise ValueError("failure receipt context must be an object")
    return D2HistoricalFailureReceiptV0(
        run_id=_text_field_v0(document, "run_id"),
        phase=cast(D2OperatorPhaseV0, phase),
        error_code=_text_field_v0(document, "error_code"),
        context=cast(dict[str, object], context),
        start_record_sha256=_text_field_v0(document, "start_record_sha256"),
        bindings_sha256=_text_field_v0(document, "bindings_sha256"),
        attempt_directory_sha256=_text_field_v0(
            document,
            "attempt_directory_sha256",
        ),
        planned_terminal_state=cast(D2FailureTerminalStateV0, terminal),
        output_protocol_state=cast(D2OutputProtocolStateV0, output_state),
        observed_at_ms=_int_field_v0(document, "observed_at_ms"),
        receipt_body_sha256=_text_field_v0(document, "receipt_body_sha256"),
        production_order_placement=cast(
            Literal[False],
            document.get("production_order_placement"),
        ),
        schema_version=cast(
            Literal["d2_historical_failure_receipt_v0"],
            _text_field_v0(document, "schema_version"),
        ),
    )


def _decode_canonical_object_v0(raw: bytes) -> dict[str, object]:
    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def reject_number(value: str) -> object:
        raise ValueError(f"non-integer JSON number is forbidden: {value}")

    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_float=reject_number,
            parse_constant=reject_number,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise ValueError("failure receipt is invalid JSON") from error
    if not isinstance(value, dict) or canonical_json_line(value) != raw:
        raise ValueError("failure receipt is not one canonical JSONL object")
    return cast(dict[str, object], value)


def _raise_after_post_start_failure_v0(
    *,
    root: Path,
    started: D1AttemptWalSnapshotV0,
    grant: D1OutcomeAccessGrantV0,
    phase: D2OperatorPhaseV0,
    original_error: BaseException,
    run_started_at_ms: int,
) -> Never:
    attempt_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_ATTEMPT_DIR_V0,
        phase="RECOVERY_VERIFICATION",
    )
    output_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_OUTPUT_DIR_V0,
        phase="RECOVERY_VERIFICATION",
    )
    failure_dir = _safe_workspace_member_v0(
        root,
        D2_OPERATOR_FAILURE_RECEIPT_DIR_V0,
        phase="RECOVERY_VERIFICATION",
    )
    start = _start_record_v0(started)
    if start is None:
        raise D2HistoricalOperatorErrorV0(
            phase="RECOVERY_VERIFICATION",
            code="D2_POST_START_HANDLER_HAS_NO_START",
            verification_status="INCOMPLETE",
        ) from original_error
    output_state: D2OutputProtocolStateV0 = (
        "PROVEN_ABSENT"
        if _output_protocol_is_proven_absent(output_dir)
        else "PRESENT_OR_UNCERTAIN"
    )
    failure_at_ms, clock_clamped = _terminal_observation_v0(run_started_at_ms)
    receipt = _build_failure_receipt_v0(
        start=start,
        grant=grant,
        phase=phase,
        error_code=_failure_code_for_phase_v0(phase),
        output_protocol_state=output_state,
        observed_at_ms=failure_at_ms,
        clock_clamped_to_start=clock_clamped,
    )
    try:
        publication = _publish_failure_receipt_v0(target=failure_dir, receipt=receipt)
    except Exception as receipt_error:
        observed = _observe_failure_receipt_v0(failure_dir)
        digest = (
            observed.publication.receipt_file_sha256
            if observed.publication is not None
            else None
        )
        status: D2OperatorVerificationStatusV0 = (
            "AMBIGUOUS_OUTPUT"
            if output_state == "PRESENT_OR_UNCERTAIN"
            else (
                "AMBIGUOUS_FAILURE_EVIDENCE"
                if observed.state != "ABSENT"
                else "INCOMPLETE"
            )
        )
        raise D2HistoricalOperatorErrorV0(
            phase="FAILURE_RECEIPT_PUBLICATION",
            code="D2_FAILURE_RECEIPT_PUBLICATION_FAILED_OR_UNCERTAIN",
            verification_status=status,
            failure_receipt_sha256=digest,
        ) from receipt_error
    detail = _failure_detail_code_v0(publication.receipt_file_sha256)
    terminal_state = receipt.planned_terminal_state
    try:
        current = load_attempt_wal_v0(attempt_dir)
        if current.last_state == "COMPLETED":
            prefix = current.prefix
            terminal_result_sha256 = current.records[-1].result_sha256
            terminal_manifest_sha256 = current.records[-1].artifact_manifest_sha256
        elif current.last_state == "STARTED_BEFORE_OUTCOME_ACCESS":
            prefix = started.prefix
            terminal_result_sha256 = None
            terminal_manifest_sha256 = None
        else:
            raise ValueError("D2 failure terminal cannot follow current WAL state")
        append_terminal_v0(
            attempt_dir=attempt_dir,
            expected_prefix=prefix,
            state=terminal_state,
            terminal_at_ms=failure_at_ms,
            detail_code=detail,
            result_sha256=terminal_result_sha256,
            artifact_manifest_sha256=terminal_manifest_sha256,
        )
    except Exception as terminal_error:
        raise D2HistoricalOperatorErrorV0(
            phase="TERMINAL_WAL_APPEND",
            code="D2_FAILURE_TERMINAL_APPEND_FAILED_OR_UNCERTAIN",
            verification_status=(
                "AMBIGUOUS_OUTPUT"
                if output_state == "PRESENT_OR_UNCERTAIN"
                else "AMBIGUOUS_FAILURE_EVIDENCE"
            ),
            failure_receipt_sha256=publication.receipt_file_sha256,
        ) from terminal_error
    try:
        verification = verify_d2_historical_development_publication_v0(
            workspace_root=root,
            expected_freeze_manifest_sha256=started.bindings.code_freeze_manifest_sha256,
        )
    except Exception as verify_error:
        raise D2HistoricalOperatorErrorV0(
            phase="RECOVERY_VERIFICATION",
            code="D2_FAILURE_BINDING_VERIFICATION_FAILED",
            verification_status=(
                "AMBIGUOUS_OUTPUT"
                if output_state == "PRESENT_OR_UNCERTAIN"
                else "AMBIGUOUS_FAILURE_EVIDENCE"
            ),
            failure_receipt_sha256=publication.receipt_file_sha256,
        ) from verify_error
    raise D2HistoricalOperatorErrorV0(
        phase=phase,
        code=receipt.error_code,
        verification_status=verification.status,
        failure_receipt_sha256=publication.receipt_file_sha256,
    ) from original_error


def _verification_from_snapshot_v0(
    *,
    snapshot: D1AttemptWalSnapshotV0,
    start: D1AttemptWalRecordV0 | None,
    attempt_dir: Path,
    output_dir: Path,
    failure_receipt: _FailureReceiptObservationV0,
    bundle: D2HistoricalInputAuthorityArtifactsV0,
    freeze: D2HistoricalDevelopmentFreezeV0,
) -> D2HistoricalDevelopmentPublicationVerificationV0:
    if snapshot.records[0].observed_at_ms < freeze.manifest_created_at_ms:
        raise D2HistoricalOperatorErrorV0(
            phase="RECOVERY_VERIFICATION",
            code="D2_ARMED_TIMESTAMP_PRECEDES_FREEZE",
        )
    output_absent = _output_protocol_is_proven_absent(output_dir)
    target_kind = _canonical_output_target_kind(output_dir)
    protocol_orphans = _output_protocol_has_orphans(output_dir)
    output_uncertain = (
        not output_absent
        or target_kind == "INVALID_PRESENT"
        or protocol_orphans
    )
    start_sha = None if start is None else start.record_sha256
    last = snapshot.records[-1]
    terminal_sha = (
        last.record_sha256
        if last.state in {"COMPLETED", "FAILED", "AMBIGUOUS_OUTPUT"}
        else None
    )
    failure_sha = (
        None
        if failure_receipt.publication is None
        else failure_receipt.publication.receipt_file_sha256
    )
    base = {
        "run_id": D2_OPERATOR_RUN_ID_V0,
        "attempt_dir": attempt_dir,
        "output_dir": output_dir,
        "start_receipt_sha256": start_sha,
        "terminal_receipt_sha256": terminal_sha,
        "result_sha256": last.result_sha256,
        "artifact_manifest_sha256": last.artifact_manifest_sha256,
        "failure_receipt_sha256": failure_sha,
    }
    if last.state == "AMBIGUOUS_OUTPUT":
        binding_reason = _validate_terminal_receipt_binding_v0(
            start=start,
            terminal=last,
            failure_receipt=failure_receipt,
            expected_terminal_state="AMBIGUOUS_OUTPUT",
            expected_output_state="PRESENT_OR_UNCERTAIN",
        )
        return D2HistoricalDevelopmentPublicationVerificationV0(
            status="AMBIGUOUS_OUTPUT",
            reason=(
                "TERMINAL_AMBIGUOUS_OUTPUT_VALID_FAILURE_BINDING"
                if binding_reason is None
                else f"AMBIGUOUS_OUTPUT_{binding_reason}"
            ),
            **base,
        )
    if last.state == "COMPLETED" and (
        snapshot.torn_tail is not None
        or (start is not None and not snapshot.start_seal_valid)
    ):
        return D2HistoricalDevelopmentPublicationVerificationV0(
            status="AMBIGUOUS_OUTPUT",
            reason="COMPLETED_WITH_INCOMPLETE_WAL_PROTOCOL",
            **base,
        )
    if output_uncertain and last.state != "COMPLETED":
        return D2HistoricalDevelopmentPublicationVerificationV0(
            status="AMBIGUOUS_OUTPUT",
            reason="OUTPUT_PROTOCOL_PRESENT_OR_UNCERTAIN",
            **base,
        )
    if snapshot.torn_tail is not None or (start is not None and not snapshot.start_seal_valid):
        if failure_receipt.state != "ABSENT":
            return D2HistoricalDevelopmentPublicationVerificationV0(
                status="AMBIGUOUS_FAILURE_EVIDENCE",
                reason="INCOMPLETE_OR_TORN_WAL_BINDING",
                **base,
            )
        return D2HistoricalDevelopmentPublicationVerificationV0(
            status="INCOMPLETE",
            reason="INCOMPLETE_OR_TORN_START_PROTOCOL",
            **base,
        )
    if last.state in {"ARMED", "STARTED_BEFORE_OUTCOME_ACCESS"}:
        if failure_receipt.state != "ABSENT":
            return D2HistoricalDevelopmentPublicationVerificationV0(
                status="AMBIGUOUS_FAILURE_EVIDENCE",
                reason=(
                    "INCOMPLETE_FAILURE_BINDING"
                    if failure_receipt.state == "VALID"
                    else "INVALID_FAILURE_RECEIPT"
                ),
                **base,
            )
        return D2HistoricalDevelopmentPublicationVerificationV0(
            status="INCOMPLETE",
            reason="NO_TERMINAL_RECORD",
            **base,
        )
    if last.state == "FAILED":
        reason = _validate_terminal_failure_binding_v0(
            start=start,
            terminal=last,
            failure_receipt=failure_receipt,
        )
        if reason is not None:
            return D2HistoricalDevelopmentPublicationVerificationV0(
                status="AMBIGUOUS_FAILURE_EVIDENCE",
                reason=reason,
                **base,
            )
        return D2HistoricalDevelopmentPublicationVerificationV0(
            status="FAILED",
            reason=None,
            **base,
        )
    if last.state != "COMPLETED" or start is None:
        raise D2HistoricalOperatorErrorV0(
            phase="RECOVERY_VERIFICATION",
            code="D2_TERMINAL_SEQUENCE_UNSUPPORTED",
        )
    if failure_receipt.state != "ABSENT":
        return D2HistoricalDevelopmentPublicationVerificationV0(
            status="AMBIGUOUS_OUTPUT",
            reason="POST_COMPLETED_FAILURE_EVIDENCE_PRESENT",
            **base,
        )
    if target_kind != "REAL_DIRECTORY" or protocol_orphans:
        return D2HistoricalDevelopmentPublicationVerificationV0(
            status="AMBIGUOUS_OUTPUT",
            reason="COMPLETED_OUTPUT_PROTOCOL_NOT_EXACT",
            **base,
        )
    try:
        verified = verify_d2_historical_published_artifact_bundle_v0(
            output_dir=output_dir,
            expected_result_sha256=_require_sha256(last.result_sha256, "result_sha256"),
            expected_manifest_sha256=_require_sha256(
                last.artifact_manifest_sha256,
                "artifact_manifest_sha256",
            ),
            expected_input_authority=bundle.authority,
            expected_code_freeze=freeze,
            expected_run_id=D2_OPERATOR_RUN_ID_V0,
            expected_run_started_at_ms=start.observed_at_ms,
            expected_start_record_sha256=start.record_sha256,
            expected_attempt_directory_sha256=start.attempt_directory_sha256,
            expected_attempt_bindings_sha256=start.bindings_sha256,
        )
        if (
            verified.result_sha256 != last.result_sha256
            or verified.artifact_manifest_sha256 != last.artifact_manifest_sha256
        ):
            raise ValueError("independent D2 artifact verification hashes differ")
    except (OSError, ValueError):
        return D2HistoricalDevelopmentPublicationVerificationV0(
            status="AMBIGUOUS_OUTPUT",
            reason="INDEPENDENT_SERIALIZED_VERIFICATION_FAILED",
            **base,
        )
    return D2HistoricalDevelopmentPublicationVerificationV0(
        status="COMPLETED",
        reason=None,
        **base,
    )


def _validate_terminal_failure_binding_v0(
    *,
    start: D1AttemptWalRecordV0 | None,
    terminal: D1AttemptWalRecordV0,
    failure_receipt: _FailureReceiptObservationV0,
) -> str | None:
    return _validate_terminal_receipt_binding_v0(
        start=start,
        terminal=terminal,
        failure_receipt=failure_receipt,
        expected_terminal_state="FAILED",
        expected_output_state="PROVEN_ABSENT",
    )


def _validate_terminal_receipt_binding_v0(
    *,
    start: D1AttemptWalRecordV0 | None,
    terminal: D1AttemptWalRecordV0,
    failure_receipt: _FailureReceiptObservationV0,
    expected_terminal_state: D2FailureTerminalStateV0,
    expected_output_state: D2OutputProtocolStateV0,
) -> str | None:
    if start is None:
        return "TERMINAL_WITHOUT_START"
    if terminal.state != expected_terminal_state:
        return "TERMINAL_STATE_MISMATCH"
    if (terminal.result_sha256 is None) != (terminal.artifact_manifest_sha256 is None):
        return "TERMINAL_RESULT_HASH_PAIR_MISMATCH"
    if expected_terminal_state == "FAILED" and (
        terminal.result_sha256 is not None
        or terminal.artifact_manifest_sha256 is not None
    ):
        return "FAILED_TERMINAL_HAS_RESULT_HASHES"
    if failure_receipt.state != "VALID" or failure_receipt.publication is None:
        return "MISSING_OR_INVALID_FAILURE_RECEIPT"
    match = (
        None
        if terminal.detail_code is None
        else _FAILURE_DETAIL_RE.fullmatch(terminal.detail_code)
    )
    if match is None:
        return "TERMINAL_FAILURE_DETAIL_CODE_INVALID"
    publication = failure_receipt.publication
    receipt = publication.receipt
    if match.group(1).lower() != publication.receipt_file_sha256:
        return "TERMINAL_FAILURE_FILE_HASH_MISMATCH"
    try:
        expected_error_code = _failure_code_for_phase_v0(receipt.phase)
    except ValueError:
        return "FAILURE_RECEIPT_PHASE_NOT_ELIGIBLE"
    if (
        receipt.run_id != D2_OPERATOR_RUN_ID_V0
        or receipt.start_record_sha256 != start.record_sha256
        or receipt.bindings_sha256 != start.bindings_sha256
        or receipt.attempt_directory_sha256 != start.attempt_directory_sha256
        or receipt.error_code != expected_error_code
        or receipt.planned_terminal_state != expected_terminal_state
        or receipt.output_protocol_state != expected_output_state
        or receipt.observed_at_ms < start.observed_at_ms
        or receipt.observed_at_ms > terminal.observed_at_ms
    ):
        return "TERMINAL_FAILURE_RECEIPT_BINDINGS_MISMATCH"
    return None


def _failure_detail_code_v0(receipt_file_sha256: str) -> str:
    return f"D2_FAILURE_RECEIPT_SHA256_{_require_sha256(receipt_file_sha256, 'digest').upper()}"


def _failure_code_for_phase_v0(phase: D2OperatorPhaseV0) -> str:
    values = {
        "OUTCOME_REPLAY": "D2_OUTCOME_REPLAY_FAILED",
        "ARTIFACT_PUBLICATION": "D2_ARTIFACT_PUBLICATION_FAILED",
        "SERIALIZED_ARTIFACT_VERIFICATION": "D2_SERIALIZED_VERIFICATION_FAILED",
        "COMPLETED_WAL_APPEND": "D2_COMPLETED_WAL_APPEND_FAILED_OR_UNCERTAIN",
        "POST_COMPLETION_VERIFICATION": "D2_POST_COMPLETION_VERIFICATION_FAILED",
    }
    try:
        return values[phase]
    except KeyError as error:
        raise ValueError("post-START failure phase is not receipt-eligible") from error


def _false_claims_v0() -> dict[str, bool]:
    return {name: False for name in _FALSE_CLAIM_FIELDS}


def _safe_workspace_root_v0(
    value: str | Path,
    *,
    phase: D2OperatorPhaseV0,
) -> Path:
    try:
        return _workspace_root(value)
    except (D1HistoricalOperatorErrorV0, OSError, ValueError) as error:
        raise D2HistoricalOperatorErrorV0(
            phase=phase,
            code="D2_WORKSPACE_ROOT_INVALID",
        ) from error


def _workspace_member_v0(root: Path, relative: str) -> Path:
    """Resolve one fixed member and reject every existing reparse ancestor."""

    candidate = _d1_workspace_member(root, relative)
    current = root
    for part in candidate.relative_to(root).parts:
        current /= part
        try:
            metadata = current.stat(follow_symlinks=False)
        except FileNotFoundError:
            break
        except OSError as error:
            raise ValueError("D2 workspace member ancestor cannot be inspected") from error
        if _is_link_or_reparse(metadata):
            raise ValueError("D2 workspace member contains a symlink or reparse ancestor")
    return candidate


def _safe_workspace_member_v0(
    root: Path,
    relative: str,
    *,
    phase: D2OperatorPhaseV0,
) -> Path:
    try:
        return _workspace_member_v0(root, relative)
    except (D1HistoricalOperatorErrorV0, OSError, ValueError) as error:
        raise D2HistoricalOperatorErrorV0(
            phase=phase,
            code="D2_FIXED_WORKSPACE_MEMBER_INVALID",
        ) from error


def _now_ms_v0() -> int:
    value = time.time_ns() // 1_000_000
    if value < 0:
        raise ValueError("system clock returned a negative timestamp")
    return value


def _terminal_time_ms_v0(run_started_at_ms: int) -> int:
    value, _clamped = _terminal_observation_v0(run_started_at_ms)
    return value


def _terminal_observation_v0(run_started_at_ms: int) -> tuple[int, bool]:
    if type(run_started_at_ms) is not int or run_started_at_ms < 0:
        raise ValueError("D2 START timestamp is invalid")
    try:
        value = _now_ms_v0()
    except Exception:
        return run_started_at_ms, True
    if value < run_started_at_ms:
        return run_started_at_ms, True
    return value, False


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return value


def _validated_expected_freeze_sha256_v0(
    value: object,
    *,
    phase: D2OperatorPhaseV0,
) -> str:
    try:
        return _require_sha256(value, "expected_freeze_manifest_sha256")
    except ValueError as error:
        raise D2HistoricalOperatorErrorV0(
            phase=phase,
            code="D2_EXPECTED_FREEZE_SHA256_INVALID",
        ) from error


def _text_field_v0(document: Mapping[str, object], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str):
        raise ValueError(f"failure receipt field {key} must be text")
    return value


def _int_field_v0(document: Mapping[str, object], key: str) -> int:
    value = document.get(key)
    if type(value) is not int:
        raise ValueError(f"failure receipt field {key} must be an integer")
    return value


_D2_PHASE_VALUES: Final = frozenset(
    {
        "AUTHORITY_PREPARATION",
        "AUTHORITY_VERIFICATION",
        "FREEZE_CREATION",
        "FREEZE_VERIFICATION",
        "ATTEMPT_ARM",
        "START_APPEND",
        "OUTCOME_REPLAY",
        "ARTIFACT_PUBLICATION",
        "SERIALIZED_ARTIFACT_VERIFICATION",
        "COMPLETED_WAL_APPEND",
        "POST_COMPLETION_VERIFICATION",
        "FAILURE_RECEIPT_PUBLICATION",
        "TERMINAL_WAL_APPEND",
        "RECOVERY_VERIFICATION",
        "REPRODUCTION_VERIFICATION",
    }
)
_D2_VERIFICATION_STATUS_VALUES: Final = frozenset(
    {
        "COMPLETED",
        "FAILED",
        "INCOMPLETE",
        "AMBIGUOUS_OUTPUT",
        "AMBIGUOUS_FAILURE_EVIDENCE",
        "OPERATIONAL_ERROR",
    }
)


def _parser_v0() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operate the one-shot frozen D2 run.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("prepare-input-authority", "create-freeze"):
        child = subparsers.add_parser(command)
        child.add_argument("--workspace-root", type=Path, default=Path.cwd())
    for command in (
        "arm-development-attempt",
        "run-development-once",
        "verify-development-publication",
        "verify-development-reproduction",
    ):
        child = subparsers.add_parser(command)
        child.add_argument("--workspace-root", type=Path, default=Path.cwd())
        child.add_argument("--expected-freeze-manifest-sha256", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Emit one canonical JSONL object; operational errors never use argparse."""

    args = _parser_v0().parse_args(argv)
    try:
        if args.command == "prepare-input-authority":
            bundle = create_d2_historical_input_authority_artifacts_v0(
                workspace_root=args.workspace_root
            )
            output: dict[str, object] = {
                **_false_claims_v0(),
                "input_authority_file_sha256": bundle.input_authority_file_sha256,
                "input_authority_sha256": bundle.authority.authority_sha256,
                "run_id": D2_OPERATOR_RUN_ID_V0,
                "schema_version": "d2_historical_operator_prepare_receipt_v0",
                "total_size_bytes": bundle.total_size_bytes,
            }
        elif args.command == "create-freeze":
            freeze = create_d2_historical_development_freeze_v0(
                workspace_root=args.workspace_root
            )
            output = {
                **_false_claims_v0(),
                "manifest_sha256": freeze.manifest_sha256,
                "receipt_sha256": freeze.receipt_sha256,
                "run_id": D2_OPERATOR_RUN_ID_V0,
                "schema_version": "d2_historical_operator_freeze_receipt_v0",
            }
        elif args.command == "arm-development-attempt":
            arm = arm_d2_historical_development_attempt_v0(
                workspace_root=args.workspace_root,
                expected_freeze_manifest_sha256=args.expected_freeze_manifest_sha256,
            )
            output = {
                **_false_claims_v0(),
                "armed_record_sha256": arm.armed_record_sha256,
                "code_freeze_manifest_sha256": arm.code_freeze_manifest_sha256,
                "run_id": D2_OPERATOR_RUN_ID_V0,
                "schema_version": "d2_historical_operator_arm_receipt_v0",
                "status": "ARMED",
            }
        elif args.command == "verify-development-reproduction":
            reproduction = verify_d2_historical_development_reproduction_v0(
                workspace_root=args.workspace_root,
                expected_freeze_manifest_sha256=args.expected_freeze_manifest_sha256,
            )
            output = {
                **_false_claims_v0(),
                "artifact_manifest_sha256": reproduction.artifact_manifest_sha256,
                "censor_count": reproduction.censor_count,
                "censor_sequence_root_sha256": (
                    reproduction.censor_sequence_root_sha256
                ),
                "completed_record_sha256": reproduction.completed_record_sha256,
                "derived_manifest_sequence_root_sha256": (
                    reproduction.derived_manifest_sequence_root_sha256
                ),
                "episode_count": reproduction.episode_count,
                "episode_sequence_root_sha256": (
                    reproduction.episode_sequence_root_sha256
                ),
                "published_artifacts_modified": (
                    reproduction.published_artifacts_modified
                ),
                "raw_replay_performed": reproduction.raw_replay_performed,
                "result_sha256": reproduction.result_sha256,
                "run_id": reproduction.run_id,
                "run_started_at_ms": reproduction.run_started_at_ms,
                "schema_version": "d2_historical_operator_reproduction_receipt_v0",
                "start_record_sha256": reproduction.start_record_sha256,
                "status": "REPRODUCED_EXACT",
                "summary_sha256": reproduction.summary_sha256,
            }
        else:
            operation = (
                run_and_publish_d2_historical_development_once_v0
                if args.command == "run-development-once"
                else verify_d2_historical_development_publication_v0
            )
            verification = operation(
                workspace_root=args.workspace_root,
                expected_freeze_manifest_sha256=args.expected_freeze_manifest_sha256,
            )
            output = {
                **_false_claims_v0(),
                "artifact_manifest_sha256": verification.artifact_manifest_sha256,
                "failure_receipt_sha256": verification.failure_receipt_sha256,
                "reason": verification.reason,
                "result_sha256": verification.result_sha256,
                "run_id": verification.run_id,
                "schema_version": "d2_historical_operator_publication_receipt_v0",
                "start_receipt_sha256": verification.start_receipt_sha256,
                "status": verification.status,
                "terminal_receipt_sha256": verification.terminal_receipt_sha256,
            }
    except D2HistoricalOperatorErrorV0 as error:
        output = {
            **_false_claims_v0(),
            "code": error.code,
            "failure_receipt_sha256": error.failure_receipt_sha256,
            "phase": error.phase,
            "run_id": D2_OPERATOR_RUN_ID_V0,
            "schema_version": "d2_historical_operator_operational_error_v0",
            "status": error.verification_status,
        }
        sys.stdout.buffer.write(canonical_json_line(output))
        return 1
    except Exception:
        output = {
            **_false_claims_v0(),
            "code": "D2_UNCLASSIFIED_OPERATIONAL_FAILURE",
            "failure_receipt_sha256": None,
            "phase": "RECOVERY_VERIFICATION",
            "run_id": D2_OPERATOR_RUN_ID_V0,
            "schema_version": "d2_historical_operator_operational_error_v0",
            "status": "OPERATIONAL_ERROR",
        }
        sys.stdout.buffer.write(canonical_json_line(output))
        return 1
    sys.stdout.buffer.write(canonical_json_line(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
