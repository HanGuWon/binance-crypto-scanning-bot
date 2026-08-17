# pyright: reportPrivateUsage=false

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Final, cast

from signalbot.backtest.d1_scefb_historical_attempt_wal import load_attempt_wal_v0
from signalbot.backtest.d1_scefb_historical_operator import (
    D1_OPERATOR_ATTEMPT_DIR_V0,
    D1_OPERATOR_FREEZE_MANIFEST_V0,
    D1_OPERATOR_FUNDING_AUTHORITY_FILE_V0,
    D1_OPERATOR_INPUT_AUTHORITY_DIR_V0,
    D1_OPERATOR_INPUT_AUTHORITY_FILE_V0,
    D1_OPERATOR_OUTPUT_DIR_V0,
    D1_OPERATOR_RUN_ID_V0,
    _output_protocol_is_proven_absent,
    _publish_fresh_directory,
    _read_stable_regular_file,
    _workspace_member,
    _workspace_root,
    load_d1_historical_input_authority_artifacts_v0,
    verify_d1_historical_development_publication_v0,
)
from signalbot.r4b_v2.canonical import canonical_json_line

_EXPECTED_FREEZE_SHA256: Final = (
    "bdf6f495762371281a137c32d57066602578a47598303d2ce4830d5e977b161a"
)
_EXPECTED_START_RECEIPT_SHA256: Final = (
    "1eb5d24f79c43bbdb80e7fdcb479a606fa92be6aa76e95c657f09509ecbe4c5d"
)
_EXPECTED_TERMINAL_RECEIPT_SHA256: Final = (
    "81948df00e0a11812d9088239712d145ba8ce0daa21fffefe4ab06573626b369"
)
_EVIDENCE_DIR: Final = (
    "artifacts/backtest/2026-07-21-d1-scefb-v0-development-run-002-failure-evidence"
)
_ARCHIVE_NAME: Final = "frozen-failure-evidence.zip"
_MANIFEST_NAME: Final = "evidence-manifest.jsonl"
_SEAL_NAME: Final = "evidence.seal"
_MAX_EVIDENCE_MEMBER_BYTES: Final = 64 * 1024 * 1024
_MAX_ARCHIVE_BYTES: Final = 64 * 1024 * 1024


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _read_workspace_file(root: Path, relative: str, label: str) -> bytes:
    return _read_stable_regular_file(
        _workspace_member(root, relative),
        label,
        maximum_bytes=_MAX_EVIDENCE_MEMBER_BYTES,
    )


def _require_object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be an object with string keys")
    return cast(dict[str, object], value)


def _require_string_map(value: object, label: str) -> dict[str, str]:
    document = _require_object(value, label)
    if any(not isinstance(item, str) for item in document.values()):
        raise ValueError(f"{label} values must be strings")
    return cast(dict[str, str], document)


def _require_size_map(value: object, label: str) -> dict[str, int]:
    document = _require_object(value, label)
    if any(type(item) is not int or item < 0 for item in document.values()):
        raise ValueError(f"{label} values must be nonnegative integers")
    return cast(dict[str, int], document)


def _add_evidence_member(
    members: dict[str, bytes],
    roles: dict[str, set[str]],
    *,
    relative: str,
    raw: bytes,
    role: str,
) -> None:
    if (
        not relative
        or relative.startswith("/")
        or "\\" in relative
        or any(part in {"", ".", ".."} for part in relative.split("/"))
    ):
        raise ValueError("evidence member path must be normalized relative POSIX text")
    existing = members.get(relative)
    if existing is not None and existing != raw:
        raise ValueError("duplicate evidence member has conflicting bytes")
    members[relative] = raw
    roles.setdefault(relative, set()).add(role)


def _deterministic_zip(members: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_STORED) as archive:
        for relative, raw in sorted(members.items()):
            info = zipfile.ZipInfo(f"workspace/{relative}")
            info.date_time = (1980, 1, 1, 0, 0, 0)
            info.compress_type = zipfile.ZIP_STORED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, raw)
    result = buffer.getvalue()
    if len(result) > _MAX_ARCHIVE_BYTES:
        raise ValueError("failed-run evidence archive exceeds its fixed byte cap")
    return result


def _evidence_payloads(root: Path) -> tuple[dict[str, bytes], dict[str, object]]:
    verification = verify_d1_historical_development_publication_v0(
        workspace_root=root,
        expected_freeze_manifest_sha256=_EXPECTED_FREEZE_SHA256,
    )
    if (
        verification.status != "FAILED"
        or verification.run_id != D1_OPERATOR_RUN_ID_V0
        or verification.start_receipt_sha256 != _EXPECTED_START_RECEIPT_SHA256
        or verification.terminal_receipt_sha256 != _EXPECTED_TERMINAL_RECEIPT_SHA256
        or verification.result_sha256 is not None
        or verification.artifact_manifest_sha256 is not None
    ):
        raise ValueError("D1 failed-run verification differs from the fixed evidence state")
    output_dir = _workspace_member(root, D1_OPERATOR_OUTPUT_DIR_V0)
    if not _output_protocol_is_proven_absent(output_dir):
        raise ValueError("D1 output protocol is not provably absent")

    freeze_raw = _read_workspace_file(
        root,
        D1_OPERATOR_FREEZE_MANIFEST_V0,
        "D1 freeze-002 manifest",
    )
    if _sha256(freeze_raw) != _EXPECTED_FREEZE_SHA256:
        raise ValueError("D1 freeze-002 manifest hash differs")
    freeze_document = _require_object(json.loads(freeze_raw), "D1 freeze-002 manifest")
    frozen_sha256 = _require_string_map(
        freeze_document.get("file_sha256"),
        "D1 freeze file_sha256",
    )
    frozen_sizes = _require_size_map(
        freeze_document.get("file_size_bytes"),
        "D1 freeze file_size_bytes",
    )
    if set(frozen_sha256) != set(frozen_sizes):
        raise ValueError("D1 freeze hash and size membership differ")

    members: dict[str, bytes] = {}
    roles: dict[str, set[str]] = {}
    for relative in sorted(frozen_sha256):
        raw = _read_workspace_file(root, relative, f"frozen source {relative}")
        if len(raw) != frozen_sizes[relative] or _sha256(raw) != frozen_sha256[relative]:
            raise ValueError(f"frozen source differs: {relative}")
        _add_evidence_member(
            members,
            roles,
            relative=relative,
            raw=raw,
            role="FROZEN_WORKSPACE_MEMBER",
        )

    bundle = load_d1_historical_input_authority_artifacts_v0(workspace_root=root)
    protocol_paths = {
        D1_OPERATOR_FREEZE_MANIFEST_V0: "FREEZE_002_MANIFEST",
        f"{D1_OPERATOR_ATTEMPT_DIR_V0}/attempt.wal": "IMMUTABLE_ATTEMPT_WAL",
        f"{D1_OPERATOR_ATTEMPT_DIR_V0}/start.seal": "START_ACCESS_SEAL",
        (
            f"{D1_OPERATOR_INPUT_AUTHORITY_DIR_V0}/"
            f"{D1_OPERATOR_FUNDING_AUTHORITY_FILE_V0}"
        ): "FUNDING_AUTHORITY",
        (
            f"{D1_OPERATOR_INPUT_AUTHORITY_DIR_V0}/"
            f"{D1_OPERATOR_INPUT_AUTHORITY_FILE_V0}"
        ): "INPUT_AUTHORITY",
    }
    for relative, role in sorted(protocol_paths.items()):
        _add_evidence_member(
            members,
            roles,
            relative=relative,
            raw=_read_workspace_file(root, relative, role),
            role=role,
        )
    for binding in bundle.authority.kline_manifests:
        relative = binding.relative_manifest_path
        raw = _read_workspace_file(
            root,
            relative,
            f"{binding.symbol} {binding.interval} kline sidecar",
        )
        if _sha256(raw) != binding.manifest_sha256:
            raise ValueError("kline sidecar differs from the input authority")
        _add_evidence_member(
            members,
            roles,
            relative=relative,
            raw=raw,
            role="KLINE_SIDECAR_MANIFEST",
        )

    tool_relative = Path(__file__).resolve(strict=True).relative_to(root).as_posix()
    _add_evidence_member(
        members,
        roles,
        relative=tool_relative,
        raw=_read_workspace_file(root, tool_relative, "evidence preservation tool"),
        role="EVIDENCE_PRESERVATION_TOOL",
    )

    attempt_wal_relative = f"{D1_OPERATOR_ATTEMPT_DIR_V0}/attempt.wal"
    start_seal_relative = f"{D1_OPERATOR_ATTEMPT_DIR_V0}/start.seal"
    attempt_snapshot = load_attempt_wal_v0(_workspace_member(root, D1_OPERATOR_ATTEMPT_DIR_V0))
    if tuple(record.state for record in attempt_snapshot.records) != (
        "ARMED",
        "STARTED_BEFORE_OUTCOME_ACCESS",
        "FAILED",
    ):
        raise ValueError("D1 WAL state chain differs from the fixed failure state")

    archive_raw = _deterministic_zip(members)
    file_records = [
        {
            "archive_member": f"workspace/{relative}",
            "relative_path": relative,
            "roles": sorted(roles[relative]),
            "sha256": _sha256(raw),
            "size_bytes": len(raw),
        }
        for relative, raw in sorted(members.items())
    ]
    manifest_document: dict[str, object] = {
        "archive": {
            "filename": _ARCHIVE_NAME,
            "format": "ZIP_STORED_FIXED_1980_TIMESTAMP_POSIX_REGULAR_V0",
            "sha256": _sha256(archive_raw),
            "size_bytes": len(archive_raw),
        },
        "claims": {
            "efficacy_claim": False,
            "execution_conclusive": False,
            "paper_fill_claim": False,
            "probability_claim": False,
            "production_order_placement": False,
            "profitability_claim": False,
            "promoting": False,
            "prospective": False,
        },
        "failure": {
            "artifact_manifest_sha256": None,
            "detail_code": "RUN_OR_PUBLICATION_FAILED_NO_RETRY",
            "output_protocol_proven_absent": True,
            "result_sha256": None,
            "start_receipt_sha256": verification.start_receipt_sha256,
            "status": verification.status,
            "terminal_receipt_sha256": verification.terminal_receipt_sha256,
        },
        "files": file_records,
        "freeze_manifest_sha256": _EXPECTED_FREEZE_SHA256,
        "input_authority_file_sha256": bundle.input_authority_file_sha256,
        "input_authority_sha256": bundle.authority.authority_sha256,
        "outcome_data_gzip_included": False,
        "rerunnable_outcome_bundle": False,
        "run_id": D1_OPERATOR_RUN_ID_V0,
        "schema_version": "d1_failed_run_evidence_manifest_v0",
        "start_seal_sha256": _sha256(members[start_seal_relative]),
        "wal_sha256": _sha256(members[attempt_wal_relative]),
    }
    manifest_raw = canonical_json_line(manifest_document)
    seal_raw = canonical_json_line(
        {
            "archive_sha256": _sha256(archive_raw),
            "evidence_manifest_sha256": _sha256(manifest_raw),
            "production_order_placement": False,
            "run_id": D1_OPERATOR_RUN_ID_V0,
            "schema_version": "d1_failed_run_evidence_seal_v0",
        }
    )
    payloads = {
        _ARCHIVE_NAME: archive_raw,
        _MANIFEST_NAME: manifest_raw,
        _SEAL_NAME: seal_raw,
    }
    receipt = {
        "archive_sha256": _sha256(archive_raw),
        "evidence_manifest_sha256": _sha256(manifest_raw),
        "evidence_seal_sha256": _sha256(seal_raw),
        "file_count": len(file_records),
        "production_order_placement": False,
        "run_id": D1_OPERATOR_RUN_ID_V0,
        "schema_version": "d1_failed_run_evidence_publication_receipt_v0",
        "status": "FAILED_RUN_EVIDENCE_ONLY",
    }
    return payloads, receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preserve immutable D1 failed-run evidence")
    parser.add_argument("--workspace-root", default=".")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = _workspace_root(args.workspace_root)
    payloads, receipt = _evidence_payloads(root)
    if args.dry_run:
        print(canonical_json_line({**receipt, "dry_run": True}).decode("utf-8"), end="")
        return 0

    target = _workspace_member(root, _EVIDENCE_DIR)
    _publish_fresh_directory(target=target, files=payloads)
    for name, expected in payloads.items():
        observed = _read_stable_regular_file(
            target / name,
            f"published evidence {name}",
            maximum_bytes=max(len(expected), 1),
        )
        if observed != expected:
            raise ValueError("published evidence differs from its staged bytes")
    print(canonical_json_line({**receipt, "dry_run": False}).decode("utf-8"), end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
