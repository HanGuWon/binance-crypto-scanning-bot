"""One-shot, outcome-blind operator boundary for the frozen D1 development run.

Preparation hashes funding gzip files as opaque bytes only.  It never
decompresses a funding or kline row.  A separate arm command validates the
literal code freeze and creates a fixed append-only WAL without outcome access.
The run command appends and fsyncs ``STARTED_BEFORE_OUTCOME_ACCESS`` before
calling the historical runner.  Once any START append is attempted the run ID
may never be attempted again, regardless of success, failure, or process death.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import os
import re
import shutil
import stat
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Final, Literal, cast

from signalbot.backtest.d1_scefb_historical_attempt_wal import (
    D1AttemptWalBindingsV0,
    D1AttemptWalRecordV0,
    D1AttemptWalSnapshotV0,
    D1HistoricalAttemptWalErrorV0,
    append_started_v0,
    append_terminal_v0,
    create_armed_wal_v0,
    load_attempt_wal_v0,
)
from signalbot.backtest.d1_scefb_historical_development import (
    D1_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0,
    D1_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0,
    D1_DEVELOPMENT_FREEZE_PURPOSE_V0,
    D1_DEVELOPMENT_FREEZE_SUFFIXES_V0,
    D1_HISTORICAL_ALIAS_BY_SYMBOL_V0,
    D1_HISTORICAL_DEVELOPMENT_RULE_V0,
    D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0,
    D1_HISTORICAL_RESULT_STATUS_V0,
    D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_SHA256_V0,
    D1_HISTORICAL_UNIVERSE_V0,
    D1HistoricalDevelopmentFreezeV0,
    D1HistoricalFundingFileBindingV0,
    D1HistoricalInputAuthorityV0,
    D1HistoricalKlineManifestBindingV0,
    build_d1_historical_input_authority_v0,
    canonical_d1_historical_development_freeze_v0,
    canonical_d1_historical_funding_authority_manifest_v0,
    canonical_d1_historical_input_authority_v0,
    d1_historical_artifact_durability_contract_v0,
    load_d1_historical_development_freeze_v0,
    run_d1_historical_development_v0,
    verify_d1_historical_serialized_artifacts_v0,
    write_d1_historical_development_artifacts_v0,
)
from signalbot.backtest.downstream_code_freeze import (
    create_downstream_code_freeze_v1,
)
from signalbot.r4b_v2.canonical import canonical_json_line

D1_OPERATOR_INPUT_AUTHORITY_DIR_V0: Final = (
    "artifacts/backtest/2026-07-21-d1-scefb-v0-input-authority"
)
D1_OPERATOR_FUNDING_AUTHORITY_FILE_V0: Final = "funding_authority.jsonl"
D1_OPERATOR_INPUT_AUTHORITY_FILE_V0: Final = "input_authority.jsonl"
D1_OPERATOR_FREEZE_MANIFEST_V0: Final = (
    "artifacts/backtest/2026-07-21-d1-scefb-v0-development-freeze-002/"
    "freeze_manifest.json"
)
D1_OPERATOR_ATTEMPT_DIR_V0: Final = (
    "artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-attempt"
)
D1_OPERATOR_OUTPUT_DIR_V0: Final = (
    "artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002"
)
D1_OPERATOR_RUN_ID_V0: Final = "d1-scefb-v0-development-run-002"
D1_OPERATOR_PREREGISTRATION_FILE_V0: Final = (
    "docs/r4b-v2-d1-scefb-5m-preregistration-v0.md"
)

D1_OPERATOR_EXPECTED_PREREGISTRATION_SHA256_V0: Final = (
    "af69c262282144432e6adbf1e01406c7334e37176dd83ce6f9666adc49b6899d"
)
D1_OPERATOR_EXPECTED_FUNDING_AUTHORITY_FILE_SHA256_V0: Final = (
    "b128bf30c6f23141e638248e47352eee4b6532317e5c8379cc04a262228fb4e8"
)
D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_SHA256_V0: Final = (
    "c33a77f4223dcf2b90fbf79853beb4818af105ccb65bf248daa273a3a4089f62"
)
D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_FILE_SHA256_V0: Final = (
    "f22655f7a3327ed176c5bdcffb565914fe0807586338f688253208a7ea7cabd5"
)
D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_SIZE_BYTES_V0: Final = 4_550

_AUTHORITY_FILE_NAMES: Final = frozenset(
    {
        D1_OPERATOR_FUNDING_AUTHORITY_FILE_V0,
        D1_OPERATOR_INPUT_AUTHORITY_FILE_V0,
    }
)
_OUTPUT_FILE_NAMES: Final = frozenset(
    {
        "censors.jsonl",
        "code-freeze-receipt.jsonl",
        "episodes.jsonl",
        "input-authority.jsonl",
        "manifest.jsonl",
        "report.md",
        "result-index.jsonl",
        "summary.jsonl",
    }
)
_MANIFEST_OUTPUT_FILE_NAMES: Final = _OUTPUT_FILE_NAMES - {"manifest.jsonl"}
_SHA256_RE: Final = re.compile(r"^[0-9a-f]{64}$")
_MAX_AUTHORITY_BYTES: Final = 1024 * 1024
_MAX_FUNDING_COMPRESSED_BYTES: Final = 16 * 1024 * 1024
_READ_CHUNK_BYTES: Final = 1024 * 1024
_OUTPUT_PATH_HASH_DOMAIN: Final = b"D1_HISTORICAL_OPERATOR_OUTPUT_PATH_V0\0"
_DETAIL_RUN_OR_PUBLICATION_FAILED: Final = "RUN_OR_PUBLICATION_FAILED_NO_RETRY"
_DETAIL_OUTPUT_PRESENT_OR_UNCERTAIN: Final = "OUTPUT_PRESENT_OR_UNCERTAIN_NO_RETRY"
_DETAIL_POST_COMPLETION_VERIFY_FAILED: Final = "POST_COMPLETION_VERIFY_FAILED_NO_RETRY"


class D1HistoricalOperatorErrorV0(ValueError):
    """Raised when the one-shot D1 operator contract is not exact."""


class _WindowsByHandleFileInformationV0(ctypes.Structure):
    _fields_ = (
        ("file_attributes", ctypes.c_uint32),
        ("creation_time_low", ctypes.c_uint32),
        ("creation_time_high", ctypes.c_uint32),
        ("last_access_time_low", ctypes.c_uint32),
        ("last_access_time_high", ctypes.c_uint32),
        ("last_write_time_low", ctypes.c_uint32),
        ("last_write_time_high", ctypes.c_uint32),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    )


@dataclass(frozen=True, slots=True)
class D1HistoricalInputAuthorityArtifactsV0:
    output_dir: Path
    authority: D1HistoricalInputAuthorityV0
    funding_manifest_sha256: str
    input_authority_file_sha256: str
    total_size_bytes: int

    def __post_init__(self) -> None:
        if not self.output_dir.is_absolute():
            raise D1HistoricalOperatorErrorV0("authority output_dir must be absolute")
        _require_sha256(self.funding_manifest_sha256, "funding_manifest_sha256")
        _require_sha256(
            self.input_authority_file_sha256,
            "input_authority_file_sha256",
        )
        if type(self.total_size_bytes) is not int or self.total_size_bytes <= 0:
            raise D1HistoricalOperatorErrorV0("authority total_size_bytes must be positive")


@dataclass(frozen=True, slots=True)
class D1HistoricalDevelopmentAttemptArmV0:
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
            raise D1HistoricalOperatorErrorV0("attempt_dir must be absolute")
        _require_sha256(self.armed_record_sha256, "armed_record_sha256")
        _require_sha256(
            self.code_freeze_manifest_sha256,
            "code_freeze_manifest_sha256",
        )
        if any(
            (
                self.historical_bbo_available,
                self.paper_fill_claim,
                self.execution_conclusive,
                self.probability_claim,
                self.efficacy_claim,
                self.promoting,
                self.prospective,
                self.production_order_placement,
            )
        ):
            raise D1HistoricalOperatorErrorV0("ARMED attempt cannot make outcome claims")


@dataclass(frozen=True, slots=True)
class D1HistoricalDevelopmentPublicationVerificationV0:
    status: Literal["COMPLETED", "FAILED", "INCOMPLETE", "AMBIGUOUS_OUTPUT"]
    run_id: str
    attempt_dir: Path
    output_dir: Path
    start_receipt_sha256: str | None
    terminal_receipt_sha256: str | None
    result_sha256: str | None
    artifact_manifest_sha256: str | None
    historical_bbo_available: bool = False
    paper_fill_claim: bool = False
    execution_conclusive: bool = False
    probability_claim: bool = False
    efficacy_claim: bool = False
    promoting: bool = False
    prospective: bool = False
    production_order_placement: bool = False

    def __post_init__(self) -> None:
        if self.status not in {"COMPLETED", "FAILED", "INCOMPLETE", "AMBIGUOUS_OUTPUT"}:
            raise D1HistoricalOperatorErrorV0("publication status is unsupported")
        if self.run_id != D1_OPERATOR_RUN_ID_V0:
            raise D1HistoricalOperatorErrorV0("publication run_id is not fixed")
        if not self.attempt_dir.is_absolute() or not self.output_dir.is_absolute():
            raise D1HistoricalOperatorErrorV0("publication paths must be absolute")
        if any(
            (
                self.historical_bbo_available,
                self.paper_fill_claim,
                self.execution_conclusive,
                self.probability_claim,
                self.efficacy_claim,
                self.promoting,
                self.prospective,
                self.production_order_placement,
            )
        ):
            raise D1HistoricalOperatorErrorV0(
                "historical publication cannot make outcome claims"
            )
        if self.status == "COMPLETED":
            _require_sha256(self.start_receipt_sha256, "start_receipt_sha256")
            _require_sha256(self.terminal_receipt_sha256, "terminal_receipt_sha256")
            _require_sha256(self.result_sha256, "result_sha256")
            _require_sha256(
                self.artifact_manifest_sha256,
                "artifact_manifest_sha256",
            )
        elif self.status == "FAILED":
            _require_sha256(self.start_receipt_sha256, "start_receipt_sha256")
            _require_sha256(self.terminal_receipt_sha256, "terminal_receipt_sha256")
            if self.result_sha256 is not None or self.artifact_manifest_sha256 is not None:
                raise D1HistoricalOperatorErrorV0(
                    "failed publication cannot claim result artifacts"
                )
        elif (self.result_sha256 is None) != (self.artifact_manifest_sha256 is None):
            raise D1HistoricalOperatorErrorV0(
                "ambiguous publication artifact hashes must be both present or absent"
            )
        else:
            if self.start_receipt_sha256 is not None:
                _require_sha256(self.start_receipt_sha256, "start_receipt_sha256")
            if self.terminal_receipt_sha256 is not None:
                _require_sha256(self.terminal_receipt_sha256, "terminal_receipt_sha256")
            if self.result_sha256 is not None:
                _require_sha256(self.result_sha256, "result_sha256")
                _require_sha256(
                    self.artifact_manifest_sha256,
                    "artifact_manifest_sha256",
                )


def create_d1_historical_input_authority_artifacts_v0(
    *,
    workspace_root: str | Path,
) -> D1HistoricalInputAuthorityArtifactsV0:
    """Publish the exact outcome-blind authority bundle once, without row access."""

    root = _workspace_root(workspace_root)
    target = _workspace_member(root, D1_OPERATOR_INPUT_AUTHORITY_DIR_V0)
    _require_absent(target, "input authority target")

    funding_bindings: list[D1HistoricalFundingFileBindingV0] = []
    for symbol in D1_HISTORICAL_UNIVERSE_V0:
        relative = _funding_relative_path(symbol)
        digest, _size = _hash_stable_regular_file(
            _workspace_member(root, relative),
            f"{symbol} funding input",
            maximum_bytes=_MAX_FUNDING_COMPRESSED_BYTES,
        )
        funding_bindings.append(
            D1HistoricalFundingFileBindingV0(
                symbol=symbol,
                relative_path=relative,
                sha256=digest,
            )
        )
    funding_raw = canonical_d1_historical_funding_authority_manifest_v0(
        tuple(funding_bindings)
    )
    funding_sha256 = hashlib.sha256(funding_raw).hexdigest()
    if funding_sha256 != D1_OPERATOR_EXPECTED_FUNDING_AUTHORITY_FILE_SHA256_V0:
        raise D1HistoricalOperatorErrorV0(
            "funding authority differs from its outcome-blind pinned identity"
        )

    kline_bindings: list[D1HistoricalKlineManifestBindingV0] = []
    for symbol in D1_HISTORICAL_UNIVERSE_V0:
        for interval in ("5m", "1h"):
            relative = _kline_manifest_relative_path(symbol, interval)
            raw = _read_stable_regular_file(
                _workspace_member(root, relative),
                f"{symbol} {interval} kline manifest",
                maximum_bytes=_MAX_AUTHORITY_BYTES,
            )
            kline_bindings.append(
                D1HistoricalKlineManifestBindingV0(
                    symbol=symbol,
                    interval=cast(Literal["5m", "1h"], interval),
                    relative_manifest_path=relative,
                    manifest_sha256=hashlib.sha256(raw).hexdigest(),
                )
            )
    authority = build_d1_historical_input_authority_v0(
        kline_manifests=tuple(kline_bindings),
        funding_manifest_relative_path=(
            f"{D1_OPERATOR_INPUT_AUTHORITY_DIR_V0}/"
            f"{D1_OPERATOR_FUNDING_AUTHORITY_FILE_V0}"
        ),
        funding_manifest_sha256=funding_sha256,
    )
    input_raw = canonical_d1_historical_input_authority_v0(authority)
    input_file_sha256 = hashlib.sha256(input_raw).hexdigest()
    if (
        authority.authority_sha256 != D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_SHA256_V0
        or input_file_sha256 != D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_FILE_SHA256_V0
        or len(input_raw) != D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_SIZE_BYTES_V0
    ):
        raise D1HistoricalOperatorErrorV0(
            "input authority differs from its outcome-blind pinned identity"
        )
    _publish_fresh_directory(
        target=target,
        files={
            D1_OPERATOR_FUNDING_AUTHORITY_FILE_V0: funding_raw,
            D1_OPERATOR_INPUT_AUTHORITY_FILE_V0: input_raw,
        },
    )
    return load_d1_historical_input_authority_artifacts_v0(workspace_root=root)


def load_d1_historical_input_authority_artifacts_v0(
    *,
    workspace_root: str | Path,
) -> D1HistoricalInputAuthorityArtifactsV0:
    """Canonically reload the fixed authority bundle without opening data rows."""

    root = _workspace_root(workspace_root)
    directory = _workspace_member(root, D1_OPERATOR_INPUT_AUTHORITY_DIR_V0)
    _require_exact_directory(directory, _AUTHORITY_FILE_NAMES, "input authority bundle")
    funding_raw = _read_stable_regular_file(
        directory / D1_OPERATOR_FUNDING_AUTHORITY_FILE_V0,
        "funding authority manifest",
        maximum_bytes=_MAX_AUTHORITY_BYTES,
    )
    input_raw = _read_stable_regular_file(
        directory / D1_OPERATOR_INPUT_AUTHORITY_FILE_V0,
        "input authority manifest",
        maximum_bytes=_MAX_AUTHORITY_BYTES,
    )
    funding_sha256 = hashlib.sha256(funding_raw).hexdigest()
    input_file_sha256 = hashlib.sha256(input_raw).hexdigest()
    if funding_sha256 != D1_OPERATOR_EXPECTED_FUNDING_AUTHORITY_FILE_SHA256_V0:
        raise D1HistoricalOperatorErrorV0("funding authority file hash differs")
    if (
        input_file_sha256 != D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_FILE_SHA256_V0
        or len(input_raw) != D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_SIZE_BYTES_V0
    ):
        raise D1HistoricalOperatorErrorV0("input authority file identity differs")
    _parse_funding_authority(funding_raw)
    authority = _parse_input_authority(input_raw)
    if (
        authority.authority_sha256 != D1_OPERATOR_EXPECTED_INPUT_AUTHORITY_SHA256_V0
        or authority.funding_manifest_sha256 != funding_sha256
    ):
        raise D1HistoricalOperatorErrorV0("input authority domain binding differs")
    return D1HistoricalInputAuthorityArtifactsV0(
        output_dir=directory,
        authority=authority,
        funding_manifest_sha256=funding_sha256,
        input_authority_file_sha256=input_file_sha256,
        total_size_bytes=len(funding_raw) + len(input_raw),
    )


def create_d1_historical_development_freeze_v0(
    *,
    workspace_root: str | Path,
) -> D1HistoricalDevelopmentFreezeV0:
    """Create the fixed broad D1 freeze exactly once and policy-check it."""

    root = _workspace_root(workspace_root)
    bundle = load_d1_historical_input_authority_artifacts_v0(workspace_root=root)
    preregistration_sha256 = _validated_preregistration_sha256(root)
    downstream = create_downstream_code_freeze_v1(
        workspace_root=root,
        manifest_path=D1_OPERATOR_FREEZE_MANIFEST_V0,
        purpose=D1_DEVELOPMENT_FREEZE_PURPOSE_V0,
        include_trees=D1_DEVELOPMENT_FREEZE_INCLUDE_TREES_V0,
        include_files=D1_DEVELOPMENT_FREEZE_INCLUDE_FILES_V0,
        included_suffixes=D1_DEVELOPMENT_FREEZE_SUFFIXES_V0,
        upstream_sha256={
            "d1_input_authority": bundle.authority.authority_sha256,
            "d1_predecessor_freeze_001": (
                D1_HISTORICAL_RETIRED_FREEZE_001_MANIFEST_SHA256_V0
            ),
            "d1_preregistration": preregistration_sha256,
        },
    )
    return load_d1_historical_development_freeze_v0(
        D1_OPERATOR_FREEZE_MANIFEST_V0,
        workspace_root=root,
        expected_manifest_sha256=downstream.manifest_sha256,
        input_authority=bundle.authority,
        preregistration_sha256=preregistration_sha256,
    )


def arm_d1_historical_development_attempt_v0(
    *,
    workspace_root: str | Path,
    expected_freeze_manifest_sha256: str,
) -> D1HistoricalDevelopmentAttemptArmV0:
    """Validate fixed authorities and durably arm one run without outcome access."""

    expected_freeze = _require_sha256(
        expected_freeze_manifest_sha256,
        "expected_freeze_manifest_sha256",
    )
    root = _workspace_root(workspace_root)
    attempt_dir = _workspace_member(root, D1_OPERATOR_ATTEMPT_DIR_V0)
    output_dir = _workspace_member(root, D1_OPERATOR_OUTPUT_DIR_V0)
    _require_absent(attempt_dir, "historical attempt reservation")
    if not _output_protocol_is_proven_absent(output_dir):
        raise D1HistoricalOperatorErrorV0(
            "historical output target, staging, and publication lock must be absent"
        )
    bundle = load_d1_historical_input_authority_artifacts_v0(workspace_root=root)
    preregistration_sha256 = _validated_preregistration_sha256(root)
    freeze = load_d1_historical_development_freeze_v0(
        D1_OPERATOR_FREEZE_MANIFEST_V0,
        workspace_root=root,
        expected_manifest_sha256=expected_freeze,
        input_authority=bundle.authority,
        preregistration_sha256=preregistration_sha256,
    )
    # Recheck after all read-only validation and immediately before ARMED creation.
    _require_absent(attempt_dir, "historical attempt reservation")
    if not _output_protocol_is_proven_absent(output_dir):
        raise D1HistoricalOperatorErrorV0(
            "historical output protocol changed while arming"
        )
    armed_at_ms = time.time_ns() // 1_000_000
    if armed_at_ms < freeze.manifest_created_at_ms:
        raise D1HistoricalOperatorErrorV0("arm clock precedes the pinned code freeze")
    bindings = _attempt_wal_bindings(bundle=bundle, freeze=freeze)
    try:
        snapshot = create_armed_wal_v0(
            attempt_dir=attempt_dir,
            bindings=bindings,
            armed_at_ms=armed_at_ms,
        )
    except D1HistoricalAttemptWalErrorV0 as error:
        raise D1HistoricalOperatorErrorV0(
            "cannot durably create the fixed ARMED attempt WAL"
        ) from error
    _require_exact_armed_snapshot(snapshot)
    return D1HistoricalDevelopmentAttemptArmV0(
        attempt_dir=attempt_dir,
        armed_record_sha256=snapshot.records[0].record_sha256,
        code_freeze_manifest_sha256=freeze.manifest_sha256,
    )


def run_and_publish_d1_historical_development_once_v0(
    *,
    workspace_root: str | Path,
    expected_freeze_manifest_sha256: str,
) -> D1HistoricalDevelopmentPublicationVerificationV0:
    """Consume one exact ARMED WAL, execute once, and never resume or retry."""

    expected_freeze = _require_sha256(
        expected_freeze_manifest_sha256,
        "expected_freeze_manifest_sha256",
    )
    root = _workspace_root(workspace_root)
    attempt_dir = _workspace_member(root, D1_OPERATOR_ATTEMPT_DIR_V0)
    output_dir = _workspace_member(root, D1_OPERATOR_OUTPUT_DIR_V0)

    # This permanent state gate intentionally precedes every authority/data read.
    try:
        armed = load_attempt_wal_v0(attempt_dir)
    except D1HistoricalAttemptWalErrorV0 as error:
        raise D1HistoricalOperatorErrorV0(
            "fixed attempt WAL is unavailable or invalid; run is not allowed"
        ) from error
    _require_exact_armed_snapshot(armed)
    if not _output_protocol_is_proven_absent(output_dir):
        raise D1HistoricalOperatorErrorV0(
            "ARMED attempt has pre-existing output protocol state"
        )

    bundle = load_d1_historical_input_authority_artifacts_v0(workspace_root=root)
    preregistration_sha256 = _validated_preregistration_sha256(root)
    freeze = load_d1_historical_development_freeze_v0(
        D1_OPERATOR_FREEZE_MANIFEST_V0,
        workspace_root=root,
        expected_manifest_sha256=expected_freeze,
        input_authority=bundle.authority,
        preregistration_sha256=preregistration_sha256,
    )
    bindings = _attempt_wal_bindings(bundle=bundle, freeze=freeze)
    if armed.bindings != bindings:
        raise D1HistoricalOperatorErrorV0(
            "ARMED attempt bindings differ from the literal freeze and authority"
        )
    run_started_at_ms = time.time_ns() // 1_000_000
    if run_started_at_ms < max(freeze.manifest_created_at_ms, armed.records[0].observed_at_ms):
        raise D1HistoricalOperatorErrorV0("run clock precedes ARMED or the code freeze")
    try:
        start_result = append_started_v0(
            attempt_dir=attempt_dir,
            expected_prefix=armed.prefix,
            started_at_ms=run_started_at_ms,
        )
    except D1HistoricalAttemptWalErrorV0 as error:
        raise D1HistoricalOperatorErrorV0(
            "START append is uncertain or failed; no outcome access and no retry are allowed"
        ) from error
    started = start_result.snapshot

    try:
        result = start_result.outcome_access_grant.consume_once_v0(
            lambda: run_d1_historical_development_v0(
                data_root=root,
                input_authority=bundle.authority,
                code_freeze=freeze,
                run_id=D1_OPERATOR_RUN_ID_V0,
                run_started_at_ms=run_started_at_ms,
            )
        )
        artifacts = write_d1_historical_development_artifacts_v0(
            result=result,
            input_authority=bundle.authority,
            code_freeze=freeze,
            output_dir=output_dir,
        )
        completed_claims = _completed_output_claims(
            start_record=started.records[-1],
            result_sha256=artifacts.result_sha256,
            artifact_manifest_sha256=artifacts.manifest_sha256,
        )
        _verify_output_directory(
            output_dir=output_dir,
            bundle=bundle,
            freeze=freeze,
            completed=completed_claims,
        )
    except Exception as error:
        output_absent = _output_protocol_is_proven_absent(output_dir)
        terminal_state: Literal["FAILED", "AMBIGUOUS_OUTPUT"] = (
            "FAILED" if output_absent else "AMBIGUOUS_OUTPUT"
        )
        detail_code = (
            _DETAIL_RUN_OR_PUBLICATION_FAILED
            if output_absent
            else _DETAIL_OUTPUT_PRESENT_OR_UNCERTAIN
        )
        try:
            append_terminal_v0(
                attempt_dir=attempt_dir,
                expected_prefix=started.prefix,
                state=terminal_state,
                terminal_at_ms=_terminal_time_ms(run_started_at_ms),
                detail_code=detail_code,
            )
        except Exception as terminal_error:
            raise D1HistoricalOperatorErrorV0(
                "post-START failure could not append a terminal WAL record; "
                "the immutable WAL must be audited and the run may not be retried"
            ) from terminal_error
        raise D1HistoricalOperatorErrorV0(
            f"D1 historical run ended as {terminal_state} after permanent START"
        ) from error

    try:
        completed = append_terminal_v0(
            attempt_dir=attempt_dir,
            expected_prefix=started.prefix,
            state="COMPLETED",
            terminal_at_ms=_terminal_time_ms(run_started_at_ms),
            result_sha256=artifacts.result_sha256,
            artifact_manifest_sha256=artifacts.manifest_sha256,
        )
    except Exception as error:
        raise D1HistoricalOperatorErrorV0(
            "COMPLETED WAL append is uncertain; immutable recovery verification is required"
        ) from error

    try:
        verification = verify_d1_historical_development_publication_v0(
            workspace_root=root,
            expected_freeze_manifest_sha256=expected_freeze,
        )
        if verification.status != "COMPLETED":
            raise D1HistoricalOperatorErrorV0(
                "post-COMPLETED verification did not return COMPLETED"
            )
        return verification
    except Exception as error:
        try:
            append_terminal_v0(
                attempt_dir=attempt_dir,
                expected_prefix=completed.prefix,
                state="AMBIGUOUS_OUTPUT",
                terminal_at_ms=_terminal_time_ms(run_started_at_ms),
                detail_code=_DETAIL_POST_COMPLETION_VERIFY_FAILED,
                result_sha256=artifacts.result_sha256,
                artifact_manifest_sha256=artifacts.manifest_sha256,
            )
        except Exception as terminal_error:
            raise D1HistoricalOperatorErrorV0(
                "post-COMPLETED verification and ambiguity append both failed; "
                "the immutable WAL must be audited"
            ) from terminal_error
        raise D1HistoricalOperatorErrorV0(
            "post-COMPLETED verification failed and the WAL is AMBIGUOUS_OUTPUT"
        ) from error


def verify_d1_historical_development_publication_v0(
    *,
    workspace_root: str | Path,
    expected_freeze_manifest_sha256: str,
) -> D1HistoricalDevelopmentPublicationVerificationV0:
    """Read-only, typed verification of the attempt WAL and output publication."""

    expected_freeze = _require_sha256(
        expected_freeze_manifest_sha256,
        "expected_freeze_manifest_sha256",
    )
    root = _workspace_root(workspace_root)
    attempt_dir = _workspace_member(root, D1_OPERATOR_ATTEMPT_DIR_V0)
    output_dir = _workspace_member(root, D1_OPERATOR_OUTPUT_DIR_V0)
    try:
        snapshot = load_attempt_wal_v0(attempt_dir)
    except D1HistoricalAttemptWalErrorV0 as error:
        raise D1HistoricalOperatorErrorV0("attempt WAL verification failed") from error
    bundle = load_d1_historical_input_authority_artifacts_v0(workspace_root=root)
    preregistration_sha256 = _validated_preregistration_sha256(root)
    freeze = load_d1_historical_development_freeze_v0(
        D1_OPERATOR_FREEZE_MANIFEST_V0,
        workspace_root=root,
        expected_manifest_sha256=expected_freeze,
        input_authority=bundle.authority,
        preregistration_sha256=preregistration_sha256,
    )
    expected_bindings = _attempt_wal_bindings(bundle=bundle, freeze=freeze)
    if snapshot.bindings != expected_bindings:
        raise D1HistoricalOperatorErrorV0(
            "attempt WAL bindings differ from the literal freeze and authority"
        )
    return _verification_from_wal_snapshot(
        snapshot=snapshot,
        attempt_dir=attempt_dir,
        output_dir=output_dir,
        bundle=bundle,
        freeze=freeze,
    )


def _parse_funding_authority(raw: bytes) -> tuple[D1HistoricalFundingFileBindingV0, ...]:
    document = _decode_canonical_object(raw, "funding authority")
    if set(document) != {"files", "historical_only", "protocol", "schema_version"}:
        raise D1HistoricalOperatorErrorV0("funding authority fields are not exact")
    files = document.get("files")
    if not isinstance(files, list):
        raise D1HistoricalOperatorErrorV0("funding authority files must be a list")
    bindings: list[D1HistoricalFundingFileBindingV0] = []
    for item in files:
        if not isinstance(item, dict) or set(item) != {"relative_path", "sha256", "symbol"}:
            raise D1HistoricalOperatorErrorV0("funding authority binding fields differ")
        symbol = item.get("symbol")
        relative_path = item.get("relative_path")
        digest = item.get("sha256")
        if not isinstance(symbol, str) or not isinstance(relative_path, str) or not isinstance(
            digest, str
        ):
            raise D1HistoricalOperatorErrorV0("funding authority binding types differ")
        bindings.append(
            D1HistoricalFundingFileBindingV0(
                symbol=symbol,
                relative_path=relative_path,
                sha256=digest,
            )
        )
    snapshot = tuple(bindings)
    if tuple(value.relative_path for value in snapshot) != tuple(
        _funding_relative_path(symbol) for symbol in D1_HISTORICAL_UNIVERSE_V0
    ):
        raise D1HistoricalOperatorErrorV0("funding authority paths differ from fixed inputs")
    if canonical_d1_historical_funding_authority_manifest_v0(snapshot) != raw:
        raise D1HistoricalOperatorErrorV0("funding authority is not canonical")
    return snapshot


def _parse_input_authority(raw: bytes) -> D1HistoricalInputAuthorityV0:
    document = _decode_canonical_object(raw, "input authority")
    if set(document) != {
        "authority_sha256",
        "funding_manifest_relative_path",
        "funding_manifest_sha256",
        "kline_manifests",
        "schema_version",
    }:
        raise D1HistoricalOperatorErrorV0("input authority fields are not exact")
    rows = document.get("kline_manifests")
    if not isinstance(rows, list):
        raise D1HistoricalOperatorErrorV0("input authority kline_manifests must be a list")
    bindings: list[D1HistoricalKlineManifestBindingV0] = []
    for item in rows:
        if not isinstance(item, dict) or set(item) != {
            "interval",
            "manifest_sha256",
            "relative_manifest_path",
            "symbol",
        }:
            raise D1HistoricalOperatorErrorV0("input authority kline fields differ")
        symbol = item.get("symbol")
        interval = item.get("interval")
        relative = item.get("relative_manifest_path")
        digest = item.get("manifest_sha256")
        if (
            not isinstance(symbol, str)
            or interval not in {"5m", "1h"}
            or not isinstance(relative, str)
            or not isinstance(digest, str)
        ):
            raise D1HistoricalOperatorErrorV0("input authority kline types differ")
        bindings.append(
            D1HistoricalKlineManifestBindingV0(
                symbol=symbol,
                interval=cast(Literal["5m", "1h"], interval),
                relative_manifest_path=relative,
                manifest_sha256=digest,
            )
        )
    funding_path = document.get("funding_manifest_relative_path")
    funding_sha256 = document.get("funding_manifest_sha256")
    if not isinstance(funding_path, str) or not isinstance(funding_sha256, str):
        raise D1HistoricalOperatorErrorV0("input authority funding binding types differ")
    authority = build_d1_historical_input_authority_v0(
        kline_manifests=tuple(bindings),
        funding_manifest_relative_path=funding_path,
        funding_manifest_sha256=funding_sha256,
    )
    expected_paths = tuple(
        _kline_manifest_relative_path(symbol, interval)
        for symbol in D1_HISTORICAL_UNIVERSE_V0
        for interval in ("5m", "1h")
    )
    if tuple(value.relative_manifest_path for value in authority.kline_manifests) != expected_paths:
        raise D1HistoricalOperatorErrorV0("input authority kline paths differ from fixed inputs")
    if funding_path != (
        f"{D1_OPERATOR_INPUT_AUTHORITY_DIR_V0}/"
        f"{D1_OPERATOR_FUNDING_AUTHORITY_FILE_V0}"
    ):
        raise D1HistoricalOperatorErrorV0("input authority funding path differs")
    if document.get("authority_sha256") != authority.authority_sha256:
        raise D1HistoricalOperatorErrorV0("input authority self-hash differs")
    if canonical_d1_historical_input_authority_v0(authority) != raw:
        raise D1HistoricalOperatorErrorV0("input authority is not canonical")
    return authority


def _validated_preregistration_sha256(root: Path) -> str:
    raw = _read_stable_regular_file(
        _workspace_member(root, D1_OPERATOR_PREREGISTRATION_FILE_V0),
        "D1 preregistration",
        maximum_bytes=_MAX_AUTHORITY_BYTES,
    )
    digest = hashlib.sha256(raw).hexdigest()
    if digest != D1_OPERATOR_EXPECTED_PREREGISTRATION_SHA256_V0:
        raise D1HistoricalOperatorErrorV0("D1 preregistration hash differs")
    return digest


def _funding_relative_path(symbol: str) -> str:
    alias = D1_HISTORICAL_ALIAS_BY_SYMBOL_V0[symbol]
    return f"data/backtest/funding/{alias}__{symbol}__5m.csv.gz"


def _kline_manifest_relative_path(symbol: str, interval: str) -> str:
    alias = D1_HISTORICAL_ALIAS_BY_SYMBOL_V0[symbol]
    return f"data/backtest/futures/{alias}__{symbol}__{interval}.csv.gz.manifest.json"


def _false_claims() -> dict[str, bool]:
    return {
        "efficacy_claim": False,
        "execution_conclusive": False,
        "historical_bbo_available": False,
        "paper_fill_claim": False,
        "probability_claim": False,
        "production_order_placement": False,
        "promoting": False,
        "prospective": False,
    }


def _attempt_wal_bindings(
    *,
    bundle: D1HistoricalInputAuthorityArtifactsV0,
    freeze: D1HistoricalDevelopmentFreezeV0,
) -> D1AttemptWalBindingsV0:
    output_path_sha256 = hashlib.sha256(
        _OUTPUT_PATH_HASH_DOMAIN + D1_OPERATOR_OUTPUT_DIR_V0.encode("utf-8")
    ).hexdigest()
    return D1AttemptWalBindingsV0(
        run_id=D1_OPERATOR_RUN_ID_V0,
        code_freeze_manifest_sha256=freeze.manifest_sha256,
        input_authority_sha256=bundle.authority.authority_sha256,
        input_authority_file_sha256=bundle.input_authority_file_sha256,
        funding_authority_file_sha256=bundle.funding_manifest_sha256,
        preregistration_sha256=freeze.preregistration_sha256,
        output_path_sha256=output_path_sha256,
    )


def _require_exact_armed_snapshot(snapshot: D1AttemptWalSnapshotV0) -> None:
    if (
        snapshot.torn_tail is not None
        or len(snapshot.records) != 1
        or snapshot.last_state != "ARMED"
    ):
        raise D1HistoricalOperatorErrorV0(
            "attempt is not the sole intact ARMED state; retry is permanently forbidden"
        )


def _start_record(snapshot: D1AttemptWalSnapshotV0) -> D1AttemptWalRecordV0 | None:
    if len(snapshot.records) < 2:
        return None
    record = snapshot.records[1]
    if record.state != "STARTED_BEFORE_OUTCOME_ACCESS":
        raise D1HistoricalOperatorErrorV0("attempt WAL has no exact START record")
    return record


def _completed_output_claims(
    *,
    start_record: D1AttemptWalRecordV0,
    result_sha256: str,
    artifact_manifest_sha256: str,
) -> dict[str, object]:
    if start_record.state != "STARTED_BEFORE_OUTCOME_ACCESS":
        raise D1HistoricalOperatorErrorV0("completed claims require the exact START record")
    return {
        "artifact_manifest_sha256": _require_sha256(
            artifact_manifest_sha256,
            "artifact_manifest_sha256",
        ),
        "result_sha256": _require_sha256(result_sha256, "result_sha256"),
        "run_started_at_ms": start_record.observed_at_ms,
    }


def _verify_existing_output_without_trusting_terminal(
    *,
    output_dir: Path,
    start_record: D1AttemptWalRecordV0,
    terminal_record: D1AttemptWalRecordV0,
    bundle: D1HistoricalInputAuthorityArtifactsV0,
    freeze: D1HistoricalDevelopmentFreezeV0,
) -> tuple[str, str]:
    result_sha256 = terminal_record.result_sha256
    artifact_manifest_sha256 = terminal_record.artifact_manifest_sha256
    if result_sha256 is None or artifact_manifest_sha256 is None:
        manifest_raw = _read_stable_regular_file(
            output_dir / "manifest.jsonl",
            "unclaimed historical output manifest",
            maximum_bytes=D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0,
        )
        manifest = _decode_canonical_object(
            manifest_raw,
            "unclaimed historical output manifest",
        )
        result_sha256 = _require_sha256(
            manifest.get("result_sha256"),
            "unclaimed output result_sha256",
        )
        artifact_manifest_sha256 = hashlib.sha256(manifest_raw).hexdigest()
    claims = _completed_output_claims(
        start_record=start_record,
        result_sha256=result_sha256,
        artifact_manifest_sha256=artifact_manifest_sha256,
    )
    _verify_output_directory(
        output_dir=output_dir,
        bundle=bundle,
        freeze=freeze,
        completed=claims,
    )
    return result_sha256, artifact_manifest_sha256


def _verification_from_wal_snapshot(
    *,
    snapshot: D1AttemptWalSnapshotV0,
    attempt_dir: Path,
    output_dir: Path,
    bundle: D1HistoricalInputAuthorityArtifactsV0,
    freeze: D1HistoricalDevelopmentFreezeV0,
) -> D1HistoricalDevelopmentPublicationVerificationV0:
    if snapshot.records[0].observed_at_ms < freeze.manifest_created_at_ms:
        raise D1HistoricalOperatorErrorV0("ARMED timestamp precedes the literal freeze")
    start = _start_record(snapshot)
    start_sha256 = None if start is None else start.record_sha256
    last = snapshot.records[-1]
    is_terminal = last.state in {"COMPLETED", "FAILED", "AMBIGUOUS_OUTPUT"}
    terminal_sha256 = last.record_sha256 if is_terminal else None
    target_kind = _canonical_output_target_kind(output_dir)
    if target_kind == "INVALID_PRESENT":
        raise D1HistoricalOperatorErrorV0(
            "canonical historical output target is a symlink, reparse point, or non-directory"
        )
    protocol_orphans = _output_protocol_has_orphans(output_dir)
    output_absent = _output_protocol_is_proven_absent(output_dir)
    observed_result_sha256 = last.result_sha256
    observed_manifest_sha256 = last.artifact_manifest_sha256
    if target_kind == "REAL_DIRECTORY":
        if start is None:
            _require_exact_directory(
                output_dir,
                _OUTPUT_FILE_NAMES,
                "historical output bundle",
            )
        else:
            observed_result_sha256, observed_manifest_sha256 = (
                _verify_existing_output_without_trusting_terminal(
                    output_dir=output_dir,
                    start_record=start,
                    terminal_record=last,
                    bundle=bundle,
                    freeze=freeze,
                )
            )

    if snapshot.torn_tail is not None:
        torn_status: Literal["INCOMPLETE", "AMBIGUOUS_OUTPUT"] = (
            "AMBIGUOUS_OUTPUT"
            if is_terminal or not output_absent
            else "INCOMPLETE"
        )
        return D1HistoricalDevelopmentPublicationVerificationV0(
            status=torn_status,
            run_id=D1_OPERATOR_RUN_ID_V0,
            attempt_dir=attempt_dir,
            output_dir=output_dir,
            start_receipt_sha256=start_sha256,
            terminal_receipt_sha256=terminal_sha256,
            result_sha256=observed_result_sha256,
            artifact_manifest_sha256=observed_manifest_sha256,
        )

    seal_incomplete = start is not None and not snapshot.start_seal_valid
    if seal_incomplete:
        seal_status: Literal["INCOMPLETE", "AMBIGUOUS_OUTPUT"] = (
            "AMBIGUOUS_OUTPUT"
            if is_terminal or not output_absent
            else "INCOMPLETE"
        )
        return D1HistoricalDevelopmentPublicationVerificationV0(
            status=seal_status,
            run_id=D1_OPERATOR_RUN_ID_V0,
            attempt_dir=attempt_dir,
            output_dir=output_dir,
            start_receipt_sha256=start_sha256,
            terminal_receipt_sha256=terminal_sha256,
            result_sha256=observed_result_sha256,
            artifact_manifest_sha256=observed_manifest_sha256,
        )

    if last.state in {"ARMED", "STARTED_BEFORE_OUTCOME_ACCESS"}:
        incomplete_status: Literal["INCOMPLETE", "AMBIGUOUS_OUTPUT"] = (
            "INCOMPLETE" if output_absent else "AMBIGUOUS_OUTPUT"
        )
        return D1HistoricalDevelopmentPublicationVerificationV0(
            status=incomplete_status,
            run_id=D1_OPERATOR_RUN_ID_V0,
            attempt_dir=attempt_dir,
            output_dir=output_dir,
            start_receipt_sha256=start_sha256,
            terminal_receipt_sha256=None,
            result_sha256=None,
            artifact_manifest_sha256=None,
        )

    if last.state == "FAILED":
        failed_status: Literal["FAILED", "AMBIGUOUS_OUTPUT"] = (
            "FAILED" if output_absent else "AMBIGUOUS_OUTPUT"
        )
        return D1HistoricalDevelopmentPublicationVerificationV0(
            status=failed_status,
            run_id=D1_OPERATOR_RUN_ID_V0,
            attempt_dir=attempt_dir,
            output_dir=output_dir,
            start_receipt_sha256=start_sha256,
            terminal_receipt_sha256=last.record_sha256,
            result_sha256=None,
            artifact_manifest_sha256=None,
        )

    if last.state == "AMBIGUOUS_OUTPUT":
        return D1HistoricalDevelopmentPublicationVerificationV0(
            status="AMBIGUOUS_OUTPUT",
            run_id=D1_OPERATOR_RUN_ID_V0,
            attempt_dir=attempt_dir,
            output_dir=output_dir,
            start_receipt_sha256=start_sha256,
            terminal_receipt_sha256=last.record_sha256,
            result_sha256=observed_result_sha256,
            artifact_manifest_sha256=observed_manifest_sha256,
        )

    if last.state != "COMPLETED" or start is None:
        raise D1HistoricalOperatorErrorV0("attempt WAL terminal sequence is unsupported")
    if target_kind == "ABSENT" or protocol_orphans:
        return D1HistoricalDevelopmentPublicationVerificationV0(
            status="AMBIGUOUS_OUTPUT",
            run_id=D1_OPERATOR_RUN_ID_V0,
            attempt_dir=attempt_dir,
            output_dir=output_dir,
            start_receipt_sha256=start.record_sha256,
            terminal_receipt_sha256=last.record_sha256,
            result_sha256=last.result_sha256,
            artifact_manifest_sha256=last.artifact_manifest_sha256,
        )
    return D1HistoricalDevelopmentPublicationVerificationV0(
        status="COMPLETED",
        run_id=D1_OPERATOR_RUN_ID_V0,
        attempt_dir=attempt_dir,
        output_dir=output_dir,
        start_receipt_sha256=start.record_sha256,
        terminal_receipt_sha256=last.record_sha256,
        result_sha256=last.result_sha256,
        artifact_manifest_sha256=last.artifact_manifest_sha256,
    )


def _terminal_time_ms(run_started_at_ms: int) -> int:
    value = time.time_ns() // 1_000_000
    if value < run_started_at_ms:
        raise D1HistoricalOperatorErrorV0("terminal clock precedes run start")
    return value


def _verify_output_directory(
    *,
    output_dir: Path,
    bundle: D1HistoricalInputAuthorityArtifactsV0,
    freeze: D1HistoricalDevelopmentFreezeV0,
    completed: Mapping[str, object],
) -> None:
    directory_before = _real_directory_identity(
        output_dir,
        "historical output bundle",
    )
    _require_exact_directory(output_dir, _OUTPUT_FILE_NAMES, "historical output bundle")
    actual_hashes: dict[str, str] = {}
    actual_sizes: dict[str, int] = {}
    raw_by_name: dict[str, bytes] = {}
    observed_total_size = 0
    for name in sorted(_OUTPUT_FILE_NAMES):
        raw = _read_stable_regular_file(
            output_dir / name,
            f"historical output {name}",
            maximum_bytes=D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0,
        )
        digest = hashlib.sha256(raw).hexdigest()
        size = len(raw)
        actual_hashes[name] = digest
        actual_sizes[name] = size
        raw_by_name[name] = raw
        observed_total_size = _bounded_artifact_total(observed_total_size, size)
    if completed.get("artifact_manifest_sha256") != actual_hashes["manifest.jsonl"]:
        raise D1HistoricalOperatorErrorV0("artifact manifest hash differs")

    authority_raw = raw_by_name["input-authority.jsonl"]
    if authority_raw != canonical_d1_historical_input_authority_v0(bundle.authority):
        raise D1HistoricalOperatorErrorV0("published input authority differs")
    freeze_raw = raw_by_name["code-freeze-receipt.jsonl"]
    if freeze_raw != canonical_d1_historical_development_freeze_v0(freeze):
        raise D1HistoricalOperatorErrorV0("published freeze receipt differs")

    manifest = _decode_canonical_object(
        raw_by_name["manifest.jsonl"],
        "published artifact manifest",
    )
    if set(manifest) != {
        "durability_contract",
        "efficacy_claim",
        "execution_conclusive",
        "historical_bbo_available",
        "input_authority_sha256",
        "outputs",
        "probability_claim",
        "production_order_placement",
        "promoting",
        "prospective",
        "protocol",
        "result_sha256",
        "schema_version",
        "status",
    }:
        raise D1HistoricalOperatorErrorV0("artifact manifest fields are not exact")
    for name in (
        "efficacy_claim",
        "execution_conclusive",
        "historical_bbo_available",
        "probability_claim",
        "production_order_placement",
        "promoting",
        "prospective",
    ):
        if manifest.get(name) is not False:
            raise D1HistoricalOperatorErrorV0(f"artifact manifest claim is true: {name}")
    if (
        manifest.get("input_authority_sha256") != bundle.authority.authority_sha256
        or manifest.get("durability_contract")
        != d1_historical_artifact_durability_contract_v0()
        or manifest.get("protocol") != D1_HISTORICAL_DEVELOPMENT_RULE_V0
        or manifest.get("status") != D1_HISTORICAL_RESULT_STATUS_V0
        or manifest.get("schema_version") != 1
        or manifest.get("result_sha256") != completed.get("result_sha256")
    ):
        raise D1HistoricalOperatorErrorV0("artifact manifest bindings differ")
    outputs = manifest.get("outputs")
    if not isinstance(outputs, dict) or set(outputs) != _MANIFEST_OUTPUT_FILE_NAMES:
        raise D1HistoricalOperatorErrorV0("artifact manifest outputs differ")
    for name in _MANIFEST_OUTPUT_FILE_NAMES:
        value = outputs.get(name)
        if not isinstance(value, dict) or value != {
            "sha256": actual_hashes[name],
            "size_bytes": actual_sizes[name],
        }:
            raise D1HistoricalOperatorErrorV0(f"artifact output binding differs: {name}")

    serialized = verify_d1_historical_serialized_artifacts_v0(
        episode_lines=_exact_jsonl_lines(raw_by_name["episodes.jsonl"], "episodes"),
        censor_lines=_exact_jsonl_lines(raw_by_name["censors.jsonl"], "censors"),
        summary_raw=raw_by_name["summary.jsonl"],
        result_index_raw=raw_by_name["result-index.jsonl"],
        expected_run_id=D1_OPERATOR_RUN_ID_V0,
        expected_run_started_at_ms=cast(int, completed["run_started_at_ms"]),
        expected_input_authority_sha256=bundle.authority.authority_sha256,
        expected_code_freeze_manifest_sha256=freeze.manifest_sha256,
        expected_code_freeze_receipt_sha256=freeze.receipt_sha256,
        expected_preregistration_sha256=freeze.preregistration_sha256,
    )
    if serialized.result_sha256 != completed.get("result_sha256"):
        raise D1HistoricalOperatorErrorV0("serialized result identity differs")
    _require_exact_directory(output_dir, _OUTPUT_FILE_NAMES, "historical output bundle")
    directory_after = _real_directory_identity(
        output_dir,
        "historical output bundle",
    )
    if directory_after != directory_before:
        raise D1HistoricalOperatorErrorV0(
            "historical output directory changed during verification"
        )


def _exact_jsonl_lines(raw: bytes, label: str) -> tuple[bytes, ...]:
    if not raw:
        return ()
    lines = tuple(raw.splitlines(keepends=True))
    if b"".join(lines) != raw or any(not line.endswith(b"\n") for line in lines):
        raise D1HistoricalOperatorErrorV0(f"{label} JSONL framing is torn")
    return lines


def _bounded_artifact_total(current: int, increment: int) -> int:
    if type(current) is not int or current < 0 or type(increment) is not int or increment < 0:
        raise D1HistoricalOperatorErrorV0("artifact byte accounting must be nonnegative")
    total = current + increment
    if total > D1_HISTORICAL_MAX_ARTIFACT_BYTES_V0:
        raise D1HistoricalOperatorErrorV0("historical output exceeds its aggregate byte cap")
    return total


def _decode_canonical_object(raw: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as error:
        raise D1HistoricalOperatorErrorV0(f"{label} is not valid UTF-8 JSON") from error
    if not isinstance(value, dict):
        raise D1HistoricalOperatorErrorV0(f"{label} root must be an object")
    try:
        canonical = canonical_json_line(value)
    except (TypeError, ValueError) as error:
        raise D1HistoricalOperatorErrorV0(f"{label} contains unsupported JSON") from error
    if canonical != raw:
        raise D1HistoricalOperatorErrorV0(f"{label} is not canonical JSONL")
    return cast(dict[str, object], value)


def _publish_fresh_directory(*, target: Path, files: Mapping[str, bytes]) -> None:
    if not files or any(
        not isinstance(name, str)
        or not name
        or Path(name).name != name
        or type(raw) is not bytes
        for name, raw in files.items()
    ):
        raise D1HistoricalOperatorErrorV0("publication files are invalid")
    _prepare_real_parent(target)
    _require_publication_volume_supported(target.parent)
    _require_fresh_publication_protocol_absent(target)
    try:
        staging = Path(
            tempfile.mkdtemp(prefix=f".{target.name}.tmp-", dir=target.parent)
        )
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(
            "cannot create fresh publication staging directory"
        ) from error
    staging_identity = _directory_object_identity(
        _require_real_publication_directory(
            staging,
            "fresh publication staging directory",
        )
    )
    try:
        for name, raw in sorted(files.items()):
            _write_new_file(staging / name, raw)
        _verify_fresh_publication_directory(
            directory=staging,
            files=files,
            label="staged publication",
            expected_directory_identity=staging_identity,
        )
        _flush_publication_directory(staging)
    except Exception:
        _remove_unpublished_staging(staging, expected_identity=staging_identity)
        raise
    # No cleanup is allowed after the commit boundary begins.  A rename call
    # may have succeeded even if its wrapper raises, so target and staging are
    # audit evidence and every later error is permanently non-retryable.
    _publish_staging_directory_no_replace(
        staging=staging,
        target=target,
        files=files,
        expected_staging_identity=staging_identity,
    )


def _publish_staging_directory_no_replace(
    *,
    staging: Path,
    target: Path,
    files: Mapping[str, bytes],
    expected_staging_identity: tuple[int, int, int],
) -> None:
    lock = target.parent / f".{target.name}.publish.lock"
    descriptor: int | None = None
    lock_identity: tuple[int, int, int, int, int] | None = None
    commit_attempted = False
    try:
        descriptor = os.open(
            lock,
            os.O_CREAT
            | os.O_EXCL
            | os.O_WRONLY
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        lock_metadata = os.fstat(descriptor)
        if not stat.S_ISREG(lock_metadata.st_mode) or lock_metadata.st_nlink != 1:
            raise D1HistoricalOperatorErrorV0(
                "publication lock must be one regular file"
            )
        lock_identity = _identity(lock_metadata)
        os.fsync(descriptor)
        _flush_publication_directory(target.parent)
        _require_absent(target, "fresh publication target")
        commit_attempted = True
        _rename_directory_no_replace(staging=staging, target=target)
        published_identity = _directory_object_identity(
            _require_real_publication_directory(
                target,
                "published input authority",
            )
        )
        if published_identity != expected_staging_identity:
            raise D1HistoricalOperatorErrorV0(
                "published directory is not the exact staged directory object"
            )
        _flush_publication_directory(target)
        _flush_publication_directory(target.parent)
        _verify_fresh_publication_directory(
            directory=target,
            files=files,
            label="published input authority",
            expected_directory_identity=expected_staging_identity,
        )
    except FileExistsError as error:
        if commit_attempted:
            raise D1HistoricalOperatorErrorV0(
                "fresh directory publication is durability-ambiguous after commit began; "
                "do not retry, delete, or replace target or staging; inspect read-only"
            ) from error
        raise D1HistoricalOperatorErrorV0(
            "publication target or exclusive lock already exists"
        ) from error
    except Exception as error:
        if commit_attempted:
            raise D1HistoricalOperatorErrorV0(
                "fresh directory publication is durability-ambiguous after commit began; "
                "do not retry, delete, or replace target or staging; inspect read-only"
            ) from error
        if isinstance(error, D1HistoricalOperatorErrorV0):
            raise
        raise D1HistoricalOperatorErrorV0("cannot publish fresh directory") from error
    finally:
        cleanup_error: Exception | None = None
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as error:
                cleanup_error = error
        if lock_identity is not None:
            try:
                observed_lock = lock.stat(follow_symlinks=False)
                if (
                    _is_link_or_reparse(observed_lock)
                    or not stat.S_ISREG(observed_lock.st_mode)
                    or _identity(observed_lock) != lock_identity
                ):
                    raise D1HistoricalOperatorErrorV0(
                        "publication lock pathname identity changed"
                    )
                lock.unlink()
                _flush_publication_directory(target.parent)
            except Exception as error:
                if cleanup_error is None:
                    cleanup_error = error
        if cleanup_error is not None:
            raise D1HistoricalOperatorErrorV0(
                "publication may have succeeded but durable lock cleanup failed; "
                "do not retry or delete publication state"
            ) from cleanup_error


def _rename_directory_no_replace(*, staging: Path, target: Path) -> None:
    if os.name == "nt":
        try:
            os.rename(staging, target)
        except FileExistsError as error:
            raise D1HistoricalOperatorErrorV0(
                "publication target appeared during commit"
            ) from error
        return
    if not sys.platform.startswith("linux"):
        raise D1HistoricalOperatorErrorV0(
            "atomic directory no-replace is unsupported on this platform"
        )
    try:
        renameat2 = ctypes.CDLL(None, use_errno=True).renameat2
    except AttributeError as error:
        raise D1HistoricalOperatorErrorV0("Linux renameat2 is unavailable") from error
    renameat2.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    renameat2.restype = ctypes.c_int
    result = renameat2(-100, os.fsencode(staging), -100, os.fsencode(target), 1)
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise D1HistoricalOperatorErrorV0(
            "publication target appeared during commit"
        )
    raise D1HistoricalOperatorErrorV0(
        f"atomic directory publication failed with errno {error_number}"
    )


def _flush_publication_directory(path: Path) -> None:
    """Fail closed unless one exact real directory entry is durably flushed."""

    if os.name == "nt":
        _windows_flush_publication_directory(path)
        return
    if os.name != "posix":
        raise D1HistoricalOperatorErrorV0(
            "publication directory durability is unsupported on this platform"
        )
    before = _require_real_publication_directory(path, "POSIX publication flush target")
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or _directory_object_identity(opened) != _directory_object_identity(before)
        ):
            raise D1HistoricalOperatorErrorV0(
                "POSIX publication directory handle differs from its pathname"
            )
        os.fsync(descriptor)
        after = os.fstat(descriptor)
        path_after = _require_real_publication_directory(
            path,
            "POSIX publication flush target",
        )
        if (
            _directory_object_identity(after) != _directory_object_identity(opened)
            or _directory_object_identity(path_after)
            != _directory_object_identity(opened)
        ):
            raise D1HistoricalOperatorErrorV0(
                "POSIX publication directory identity changed during flush"
            )
    except D1HistoricalOperatorErrorV0:
        raise
    except OSError as error:
        raise D1HistoricalOperatorErrorV0("cannot fsync publication directory") from error
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as error:
                raise D1HistoricalOperatorErrorV0(
                    "publication directory descriptor close failed"
                ) from error


def _require_publication_volume_supported(path: Path) -> None:
    if os.name == "nt":
        _windows_local_publication_volume_identity(path)
        return
    if os.name != "posix":
        raise D1HistoricalOperatorErrorV0(
            "fresh directory publication is unsupported on this platform"
        )


def _require_fresh_publication_protocol_absent(target: Path) -> None:
    _require_absent(target, "fresh publication target")
    parent_before = _real_directory_identity(
        target.parent,
        "fresh publication parent",
    )
    staging_prefix = f".{target.name}.tmp-"
    lock_name = f".{target.name}.publish.lock"
    try:
        with os.scandir(target.parent) as entries:
            for entry in entries:
                if entry.name == lock_name or entry.name.startswith(staging_prefix):
                    raise D1HistoricalOperatorErrorV0(
                        "fresh publication has prior staging or lock evidence; retry is forbidden"
                    )
    except D1HistoricalOperatorErrorV0:
        raise
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(
            "fresh publication parent cannot be inspected"
        ) from error
    parent_after = _real_directory_identity(
        target.parent,
        "fresh publication parent",
    )
    if parent_after != parent_before:
        raise D1HistoricalOperatorErrorV0(
            "fresh publication parent changed during protocol inspection"
        )


def _remove_unpublished_staging(
    staging: Path,
    *,
    expected_identity: tuple[int, int, int],
) -> None:
    try:
        observed = staging.stat(follow_symlinks=False)
    except FileNotFoundError:
        return
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(
            "failed publication staging cannot be inspected for cleanup"
        ) from error
    if (
        _is_link_or_reparse(observed)
        or not stat.S_ISDIR(observed.st_mode)
        or _directory_object_identity(observed) != expected_identity
    ):
        raise D1HistoricalOperatorErrorV0(
            "failed publication staging identity changed; cleanup is forbidden"
        )
    try:
        shutil.rmtree(staging)
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(
            "publication failed and staging cleanup also failed"
        ) from error
    _flush_publication_directory(staging.parent)


def _verify_fresh_publication_directory(
    *,
    directory: Path,
    files: Mapping[str, bytes],
    label: str,
    expected_directory_identity: tuple[int, int, int],
) -> None:
    directory_before = _real_directory_identity(directory, label)
    if directory_before[:3] != expected_directory_identity:
        raise D1HistoricalOperatorErrorV0(
            f"{label} is not the expected directory object"
        )
    expected_names = frozenset(files)
    _require_exact_directory(directory, expected_names, label)
    for name in sorted(expected_names):
        _verify_published_file_same_descriptor(
            directory / name,
            expected=files[name],
            label=f"{label} {name}",
        )
    _require_exact_directory(directory, expected_names, label)
    directory_after = _real_directory_identity(directory, label)
    if (
        directory_after != directory_before
        or directory_after[:3] != expected_directory_identity
    ):
        raise D1HistoricalOperatorErrorV0(
            f"{label} directory identity changed during exact verification"
        )


def _verify_published_file_same_descriptor(
    path: Path,
    *,
    expected: bytes,
    label: str,
) -> None:
    handle, opened = _open_stable_regular(path, label)
    digest = hashlib.sha256()
    total = 0
    try:
        if opened.st_nlink != 1:
            raise D1HistoricalOperatorErrorV0(
                f"{label} must have exactly one filesystem link"
            )
        while True:
            chunk = handle.read(min(_READ_CHUNK_BYTES, len(expected) - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > len(expected):
                raise D1HistoricalOperatorErrorV0(f"{label} exceeds its expected size")
            digest.update(chunk)
        _verify_stable_regular(path, handle, opened, label)
        if (
            total != opened.st_size
            or total != len(expected)
            or digest.hexdigest() != hashlib.sha256(expected).hexdigest()
        ):
            raise D1HistoricalOperatorErrorV0(
                f"{label} same-descriptor size or hash differs"
            )
    finally:
        try:
            handle.close()
        except OSError as error:
            raise D1HistoricalOperatorErrorV0(
                f"{label} same-descriptor close failed"
            ) from error


def _require_real_publication_directory(path: Path, label: str) -> os.stat_result:
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(f"{label} is unavailable") from error
    if _is_link_or_reparse(metadata) or not stat.S_ISDIR(metadata.st_mode):
        raise D1HistoricalOperatorErrorV0(
            f"{label} must be one real non-reparse directory"
        )
    return metadata


def _directory_object_identity(metadata: os.stat_result) -> tuple[int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_mode


def _windows_publication_kernel32():
    loader = getattr(ctypes, "WinDLL", None)
    if loader is None:
        raise D1HistoricalOperatorErrorV0(
            "Win32 publication durability APIs are unavailable"
        )
    try:
        return loader("kernel32", use_last_error=True)
    except (OSError, TypeError) as error:
        raise D1HistoricalOperatorErrorV0(
            "Win32 publication durability APIs are unavailable"
        ) from error


def _windows_publication_api(name: str):
    try:
        return getattr(_windows_publication_kernel32(), name)
    except AttributeError as error:
        raise D1HistoricalOperatorErrorV0(
            f"Win32 publication durability API {name} is unavailable"
        ) from error


def _windows_publication_last_error() -> int:
    getter = getattr(ctypes, "get_last_error", None)
    return 0 if getter is None else int(getter())


def _windows_open_publication_directory_handle(path: Path) -> int:
    from ctypes import wintypes

    create_file = _windows_publication_api("CreateFileW")
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    handle = create_file(
        os.fspath(path),
        0x40000000,
        0x00000001 | 0x00000002 | 0x00000004,
        None,
        3,
        0x02000000 | 0x00200000 | 0x80000000,
        None,
    )
    value = handle if isinstance(handle, int) else getattr(handle, "value", None)
    if value is None or value == ctypes.c_void_p(-1).value:
        error_number = _windows_publication_last_error()
        raise D1HistoricalOperatorErrorV0(
            f"CreateFileW publication directory failed with error {error_number}"
        )
    return int(value)


def _windows_publication_file_information(
    handle: int,
) -> _WindowsByHandleFileInformationV0:
    from ctypes import wintypes

    get_information = _windows_publication_api("GetFileInformationByHandle")
    get_information.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(_WindowsByHandleFileInformationV0),
    )
    get_information.restype = wintypes.BOOL
    information = _WindowsByHandleFileInformationV0()
    if not get_information(wintypes.HANDLE(handle), ctypes.byref(information)):
        error_number = _windows_publication_last_error()
        raise D1HistoricalOperatorErrorV0(
            "GetFileInformationByHandle publication directory failed "
            f"with error {error_number}"
        )
    return information


def _windows_publication_directory_handle_identity(
    information: _WindowsByHandleFileInformationV0,
) -> tuple[int, int]:
    file_index = (int(information.file_index_high) << 32) | int(
        information.file_index_low
    )
    return int(information.volume_serial_number), file_index


def _require_windows_real_publication_directory_handle(
    information: _WindowsByHandleFileInformationV0,
) -> None:
    attributes = int(information.file_attributes)
    if not attributes & 0x00000010 or attributes & 0x00000400:
        raise D1HistoricalOperatorErrorV0(
            "Win32 publication handle must name one real non-reparse directory"
        )


def _windows_flush_publication_directory_handle(handle: int) -> None:
    from ctypes import wintypes

    flush = _windows_publication_api("FlushFileBuffers")
    flush.argtypes = (wintypes.HANDLE,)
    flush.restype = wintypes.BOOL
    if not flush(wintypes.HANDLE(handle)):
        error_number = _windows_publication_last_error()
        raise D1HistoricalOperatorErrorV0(
            f"FlushFileBuffers publication directory failed with error {error_number}"
        )


def _windows_close_publication_handle(handle: int) -> None:
    from ctypes import wintypes

    close = _windows_publication_api("CloseHandle")
    close.argtypes = (wintypes.HANDLE,)
    close.restype = wintypes.BOOL
    if not close(wintypes.HANDLE(handle)):
        error_number = _windows_publication_last_error()
        raise D1HistoricalOperatorErrorV0(
            f"CloseHandle publication directory failed with error {error_number}"
        )


def _windows_local_publication_volume_identity(path: Path) -> tuple[str, int]:
    from ctypes import wintypes

    volume_path = ctypes.create_unicode_buffer(261)
    get_volume_path = _windows_publication_api("GetVolumePathNameW")
    get_volume_path.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
    get_volume_path.restype = wintypes.BOOL
    if not get_volume_path(os.fspath(path), volume_path, len(volume_path)):
        error_number = _windows_publication_last_error()
        raise D1HistoricalOperatorErrorV0(
            f"GetVolumePathNameW publication failed with error {error_number}"
        )
    root = volume_path.value
    get_drive_type = _windows_publication_api("GetDriveTypeW")
    get_drive_type.argtypes = (wintypes.LPCWSTR,)
    get_drive_type.restype = wintypes.UINT
    if int(get_drive_type(root)) != 3:
        raise D1HistoricalOperatorErrorV0(
            "fresh publication requires a local fixed Windows volume"
        )

    serial = wintypes.DWORD()
    filesystem = ctypes.create_unicode_buffer(64)
    get_volume_information = _windows_publication_api("GetVolumeInformationW")
    get_volume_information.argtypes = (
        wintypes.LPCWSTR,
        wintypes.LPWSTR,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        ctypes.POINTER(wintypes.DWORD),
        wintypes.LPWSTR,
        wintypes.DWORD,
    )
    get_volume_information.restype = wintypes.BOOL
    if not get_volume_information(
        root,
        None,
        0,
        ctypes.byref(serial),
        None,
        None,
        filesystem,
        len(filesystem),
    ):
        error_number = _windows_publication_last_error()
        raise D1HistoricalOperatorErrorV0(
            f"GetVolumeInformationW publication failed with error {error_number}"
        )
    filesystem_name = filesystem.value.upper()
    if filesystem_name != "NTFS":
        raise D1HistoricalOperatorErrorV0(
            "fresh publication requires a local fixed NTFS volume"
        )
    serial_value = int(serial.value)
    return (
        f"{os.path.normcase(root)}|{filesystem_name}|{serial_value:08x}",
        serial_value,
    )


def _windows_flush_publication_directory(path: Path) -> None:
    before = _require_real_publication_directory(
        path,
        "Windows publication flush target",
    )
    _volume_identity, expected_serial = _windows_local_publication_volume_identity(path)
    handle = _windows_open_publication_directory_handle(path)
    try:
        opened = _windows_publication_file_information(handle)
        _require_windows_real_publication_directory_handle(opened)
        opened_identity = _windows_publication_directory_handle_identity(opened)
        if opened_identity[0] != expected_serial:
            raise D1HistoricalOperatorErrorV0(
                "Win32 publication directory differs from its qualified volume"
            )
        _windows_flush_publication_directory_handle(handle)
        after_flush = _windows_publication_file_information(handle)
        _require_windows_real_publication_directory_handle(after_flush)
        if _windows_publication_directory_handle_identity(after_flush) != opened_identity:
            raise D1HistoricalOperatorErrorV0(
                "Win32 publication directory changed during flush"
            )
        path_handle = _windows_open_publication_directory_handle(path)
        try:
            path_information = _windows_publication_file_information(path_handle)
            _require_windows_real_publication_directory_handle(path_information)
            if (
                _windows_publication_directory_handle_identity(path_information)
                != opened_identity
            ):
                raise D1HistoricalOperatorErrorV0(
                    "Win32 publication pathname identity changed during flush"
                )
        finally:
            _windows_close_publication_handle(path_handle)
        after = _require_real_publication_directory(
            path,
            "Windows publication flush target",
        )
        if _directory_object_identity(after) != _directory_object_identity(before):
            raise D1HistoricalOperatorErrorV0(
                "Windows publication directory pathname changed during flush"
            )
    finally:
        _windows_close_publication_handle(handle)


def _write_new_file(path: Path, raw: bytes) -> None:
    try:
        with path.open("xb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as error:
        raise D1HistoricalOperatorErrorV0("immutable publication file already exists") from error
    except OSError as error:
        raise D1HistoricalOperatorErrorV0("cannot write immutable publication file") from error


def _workspace_root(value: str | Path) -> Path:
    candidate = Path(value)
    if candidate.is_symlink():
        raise D1HistoricalOperatorErrorV0("workspace_root must not be a symlink")
    try:
        resolved = candidate.resolve(strict=True)
        metadata = resolved.stat(follow_symlinks=False)
    except OSError as error:
        raise D1HistoricalOperatorErrorV0("workspace_root is unavailable") from error
    if _is_link_or_reparse(metadata) or not stat.S_ISDIR(metadata.st_mode):
        raise D1HistoricalOperatorErrorV0("workspace_root must be a real directory")
    return resolved


def _workspace_member(root: Path, relative: str) -> Path:
    if (
        not isinstance(relative, str)
        or not relative
        or relative.startswith("/")
        or "\\" in relative
        or any(part in {"", ".", ".."} for part in relative.split("/"))
    ):
        raise D1HistoricalOperatorErrorV0("operator path must be normalized relative POSIX text")
    candidate = root.joinpath(*relative.split("/"))
    current = root
    for part in candidate.relative_to(root).parts:
        current /= part
        if current.is_symlink():
            raise D1HistoricalOperatorErrorV0("operator path contains a symlink component")
    return candidate


def _prepare_real_parent(target: Path) -> None:
    missing: list[Path] = []
    current = target.parent
    existing_metadata: os.stat_result | None = None
    while True:
        try:
            existing_metadata = current.stat(follow_symlinks=False)
        except FileNotFoundError as error:
            if current == current.parent:
                raise D1HistoricalOperatorErrorV0(
                    "publication parent chain has no existing durable anchor"
                ) from error
            missing.append(current)
            current = current.parent
            continue
        except OSError as error:
            raise D1HistoricalOperatorErrorV0(
                "publication parent chain cannot be inspected before creation"
            ) from error
        if _is_link_or_reparse(existing_metadata) or not stat.S_ISDIR(
            existing_metadata.st_mode
        ):
            raise D1HistoricalOperatorErrorV0(
                "publication parent chain must contain only real directories"
            )
        break
    if existing_metadata is None:
        raise D1HistoricalOperatorErrorV0(
            "publication parent chain has no existing durable anchor"
        )
    existing_identity = _directory_object_identity(existing_metadata)
    _require_publication_volume_supported(current)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise D1HistoricalOperatorErrorV0("publication parent is unavailable") from error
    existing_after = _require_real_publication_directory(
        current,
        "publication parent durable anchor",
    )
    if _directory_object_identity(existing_after) != existing_identity:
        raise D1HistoricalOperatorErrorV0(
            "publication parent durable anchor changed during creation"
        )
    for directory in (*missing, current):
        _require_real_publication_directory(directory, "publication parent chain")
        _flush_publication_directory(directory)


def _require_absent(path: Path, label: str) -> None:
    if path.exists() or path.is_symlink():
        raise D1HistoricalOperatorErrorV0(f"{label} must be absent")


def _canonical_output_target_kind(
    output_dir: Path,
) -> Literal["ABSENT", "REAL_DIRECTORY", "INVALID_PRESENT"]:
    try:
        metadata = os.lstat(output_dir)
    except FileNotFoundError:
        return "ABSENT"
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(
            "canonical historical output target cannot be inspected"
        ) from error
    attributes = getattr(metadata, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    if (
        stat.S_ISLNK(metadata.st_mode)
        or bool(attributes & reparse_flag)
        or not stat.S_ISDIR(metadata.st_mode)
    ):
        return "INVALID_PRESENT"
    return "REAL_DIRECTORY"


def _output_protocol_has_orphans(output_dir: Path) -> bool:
    parent = output_dir.parent
    staging_prefix = f".{output_dir.name}.tmp-"
    lock_name = f".{output_dir.name}.publish.lock"

    def has_orphan() -> bool:
        with os.scandir(parent) as entries:
            return any(
                entry.name == lock_name or entry.name.startswith(staging_prefix)
                for entry in entries
            )

    try:
        before = parent.stat(follow_symlinks=False)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
            return True
        first = has_orphan()
        second = has_orphan()
        after = parent.stat(follow_symlinks=False)
    except OSError:
        return True
    return first or second or _identity(before) != _identity(after)


def _output_protocol_is_proven_absent(output_dir: Path) -> bool:
    """Conservatively prove no target, staging directory, or publish lock exists."""

    parent = output_dir.parent
    staging_prefix = f".{output_dir.name}.tmp-"
    lock_name = f".{output_dir.name}.publish.lock"

    def has_relevant_name() -> bool:
        with os.scandir(parent) as entries:
            return any(
                entry.name == output_dir.name
                or entry.name == lock_name
                or entry.name.startswith(staging_prefix)
                for entry in entries
            )

    try:
        before = parent.stat(follow_symlinks=False)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
            return False
        first = has_relevant_name()
        second = has_relevant_name()
        after = parent.stat(follow_symlinks=False)
    except OSError:
        return False
    return not first and not second and _identity(before) == _identity(after)


def _real_directory_identity(path: Path, label: str) -> tuple[int, int, int, int, int]:
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(f"{label} is unavailable") from error
    if _is_link_or_reparse(metadata) or not stat.S_ISDIR(metadata.st_mode):
        raise D1HistoricalOperatorErrorV0(f"{label} must be a real directory")
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def _require_exact_directory(path: Path, expected: frozenset[str], label: str) -> None:
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(f"{label} is unavailable") from error
    if _is_link_or_reparse(metadata) or not stat.S_ISDIR(metadata.st_mode):
        raise D1HistoricalOperatorErrorV0(f"{label} must be a real directory")
    observed: set[str] = set()
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                if entry.name not in expected or entry.name in observed:
                    raise D1HistoricalOperatorErrorV0(
                        f"{label} file membership differs"
                    )
                observed.add(entry.name)
                if len(observed) > len(expected):
                    raise D1HistoricalOperatorErrorV0(
                        f"{label} exceeds its bounded membership cap"
                    )
                try:
                    entry_metadata = entry.stat(follow_symlinks=False)
                except OSError as error:
                    raise D1HistoricalOperatorErrorV0(
                        f"{label} entry cannot be inspected"
                    ) from error
                if _is_link_or_reparse(entry_metadata) or not stat.S_ISREG(
                    entry_metadata.st_mode
                ):
                    raise D1HistoricalOperatorErrorV0(
                        f"{label} may contain regular non-symlink files only"
                    )
    except D1HistoricalOperatorErrorV0:
        raise
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(f"{label} cannot be listed") from error
    if frozenset(observed) != expected:
        raise D1HistoricalOperatorErrorV0(f"{label} file membership differs")


def _identity(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def _is_link_or_reparse(metadata: os.stat_result) -> bool:
    if stat.S_ISLNK(metadata.st_mode):
        return True
    attributes = getattr(metadata, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(attributes & reparse_flag)


def _open_stable_regular(path: Path, label: str) -> tuple[BinaryIO, os.stat_result]:
    descriptor: int | None = None
    try:
        before = path.stat(follow_symlinks=False)
        if _is_link_or_reparse(before) or not stat.S_ISREG(before.st_mode):
            raise D1HistoricalOperatorErrorV0(
                f"{label} must be a regular non-symlink file"
            )
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or _identity(before) != _identity(opened):
            raise D1HistoricalOperatorErrorV0(f"{label} identity changed while opening")
        handle = os.fdopen(descriptor, "rb", buffering=0)
        descriptor = None
        return handle, opened
    except D1HistoricalOperatorErrorV0:
        raise
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(f"{label} is unavailable") from error
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _verify_stable_regular(
    path: Path,
    handle: BinaryIO,
    opened: os.stat_result,
    label: str,
) -> None:
    try:
        descriptor_after = os.fstat(handle.fileno())
        path_after = path.stat(follow_symlinks=False)
    except OSError as error:
        raise D1HistoricalOperatorErrorV0(f"{label} cannot be revalidated") from error
    expected = _identity(opened)
    if (
        _is_link_or_reparse(path_after)
        or not stat.S_ISREG(path_after.st_mode)
        or _identity(descriptor_after) != expected
        or _identity(path_after) != expected
    ):
        raise D1HistoricalOperatorErrorV0(f"{label} changed during read")


def _read_stable_regular_file(path: Path, label: str, *, maximum_bytes: int) -> bytes:
    if type(maximum_bytes) is not int or maximum_bytes <= 0:
        raise D1HistoricalOperatorErrorV0("stable-read byte cap must be positive")
    handle, opened = _open_stable_regular(path, label)
    try:
        if opened.st_size > maximum_bytes:
            raise D1HistoricalOperatorErrorV0(f"{label} exceeds its byte cap")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = handle.read(min(_READ_CHUNK_BYTES, maximum_bytes - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                raise D1HistoricalOperatorErrorV0(f"{label} exceeds its byte cap")
            chunks.append(chunk)
        _verify_stable_regular(path, handle, opened, label)
        if total != opened.st_size:
            raise D1HistoricalOperatorErrorV0(
                f"{label} bytes read differ from its opened size"
            )
        return b"".join(chunks)
    finally:
        handle.close()


def _hash_stable_regular_file(
    path: Path,
    label: str,
    *,
    maximum_bytes: int,
) -> tuple[str, int]:
    if type(maximum_bytes) is not int or maximum_bytes <= 0:
        raise D1HistoricalOperatorErrorV0("stable-hash byte cap must be positive")
    handle, opened = _open_stable_regular(path, label)
    digest = hashlib.sha256()
    size = 0
    try:
        while True:
            chunk = handle.read(min(_READ_CHUNK_BYTES, maximum_bytes - size + 1))
            if not chunk:
                break
            size += len(chunk)
            if size > maximum_bytes:
                raise D1HistoricalOperatorErrorV0(f"{label} exceeds its byte cap")
            digest.update(chunk)
        _verify_stable_regular(path, handle, opened, label)
        if size != opened.st_size:
            raise D1HistoricalOperatorErrorV0(
                f"{label} bytes hashed differ from its opened size"
            )
    finally:
        handle.close()
    return digest.hexdigest(), size


def _require_sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise D1HistoricalOperatorErrorV0(f"{label} must be a lowercase SHA-256 digest")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Operate the one-shot frozen D1 development run.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("prepare-input-authority", "create-freeze"):
        child = subparsers.add_parser(command)
        child.add_argument("--workspace-root", type=Path, default=Path.cwd())
    for command in (
        "arm-development-attempt",
        "run-development-once",
        "verify-development-publication",
    ):
        child = subparsers.add_parser(command)
        child.add_argument("--workspace-root", type=Path, default=Path.cwd())
        child.add_argument("--expected-freeze-manifest-sha256", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point; stdout is one canonical, non-promoting JSONL receipt."""

    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare-input-authority":
            bundle = create_d1_historical_input_authority_artifacts_v0(
                workspace_root=args.workspace_root
            )
            output = {
                **_false_claims(),
                "funding_manifest_sha256": bundle.funding_manifest_sha256,
                "input_authority_file_sha256": bundle.input_authority_file_sha256,
                "input_authority_sha256": bundle.authority.authority_sha256,
                "schema_version": "d1_historical_operator_prepare_receipt_v0",
                "total_size_bytes": bundle.total_size_bytes,
            }
        elif args.command == "create-freeze":
            freeze = create_d1_historical_development_freeze_v0(
                workspace_root=args.workspace_root
            )
            output = {
                **_false_claims(),
                "manifest_sha256": freeze.manifest_sha256,
                "receipt_sha256": freeze.receipt_sha256,
                "schema_version": "d1_historical_operator_freeze_receipt_v0",
            }
        elif args.command == "arm-development-attempt":
            arm = arm_d1_historical_development_attempt_v0(
                workspace_root=args.workspace_root,
                expected_freeze_manifest_sha256=args.expected_freeze_manifest_sha256,
            )
            output = {
                **_false_claims(),
                "armed_record_sha256": arm.armed_record_sha256,
                "code_freeze_manifest_sha256": arm.code_freeze_manifest_sha256,
                "run_id": D1_OPERATOR_RUN_ID_V0,
                "schema_version": "d1_historical_operator_arm_receipt_v0",
                "status": "ARMED",
            }
        else:
            operation = (
                run_and_publish_d1_historical_development_once_v0
                if args.command == "run-development-once"
                else verify_d1_historical_development_publication_v0
            )
            verification = operation(
                workspace_root=args.workspace_root,
                expected_freeze_manifest_sha256=args.expected_freeze_manifest_sha256,
            )
            output = {
                **_false_claims(),
                "artifact_manifest_sha256": verification.artifact_manifest_sha256,
                "result_sha256": verification.result_sha256,
                "run_id": verification.run_id,
                "schema_version": "d1_historical_operator_publication_receipt_v0",
                "start_receipt_sha256": verification.start_receipt_sha256,
                "status": verification.status,
                "terminal_receipt_sha256": verification.terminal_receipt_sha256,
            }
    except ValueError as error:
        parser.error(str(error))
    sys.stdout.buffer.write(canonical_json_line(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
