"""Exact prospective-policy wrapper around the shared code-freeze engine.

The shared engine owns recursive membership and byte-hash validation.  This
wrapper removes caller discretion over scope, purpose, and upstream bindings.
Its timestamp is declarative; a later writer-lease receipt must still prove the
plan was durably persisted before ``H_start``.
"""

from __future__ import annotations

import hashlib
from dataclasses import InitVar, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Final

from signalbot.backtest.downstream_code_freeze import (
    DownstreamCodeFreezeAuthorityV1,
    create_downstream_code_freeze_v1,
    load_downstream_code_freeze_v1,
)
from signalbot.r4b_v2.canonical import canonical_json_line

PROSPECTIVE_CODE_FREEZE_PURPOSE_V2: Final = (
    "R4B_V2_PROSPECTIVE_EFFICACY_PRE_H_START"
)
PROSPECTIVE_CODE_FREEZE_STATUS_V2: Final = (
    "POLICY_VALIDATED_DECLARATIVE_TIME_REQUIRES_DURABLE_PRE_H_START_RECEIPT"
)
PROSPECTIVE_CODE_FREEZE_SCHEMA_V2: Final = (
    "r4b_v2_prospective_code_freeze_receipt_v2"
)
PROSPECTIVE_CODE_FREEZE_INCLUDE_TREES_V2: Final = (
    "src/signalbot",
    "tests",
)
PROSPECTIVE_CODE_FREEZE_INCLUDE_FILES_V2: Final = (
    "pyproject.toml",
    "uv.lock",
)
PROSPECTIVE_CODE_FREEZE_SUFFIXES_V2: Final = (".py",)

_UPSTREAM_NAMES: Final = (
    "promoting_plan",
    "prospective_efficacy_gate",
    "prospective_execution_contract",
)
_RECEIPT_DOMAIN: Final = b"R4B_V2_PROSPECTIVE_CODE_FREEZE_RECEIPT_V2\0"
_RECEIPT_FACTORY_TOKEN: Final = object()


class ProspectiveCodeFreezeContractErrorV2(ValueError):
    """Raised when a generic freeze is too weak for prospective use."""


@dataclass(frozen=True, slots=True)
class ProspectiveCodeFreezeReceiptV2:
    manifest_sha256: str
    manifest_created_at_ms: int
    h_start_ms: int
    upstream_sha256: tuple[tuple[str, str], ...]
    _factory_token: InitVar[object]
    status: str = field(init=False, default=PROSPECTIVE_CODE_FREEZE_STATUS_V2)
    schema_version: str = field(init=False, default=PROSPECTIVE_CODE_FREEZE_SCHEMA_V2)
    receipt_sha256: str = field(init=False)

    def __post_init__(self, _factory_token: object) -> None:
        if _factory_token is not _RECEIPT_FACTORY_TOKEN:
            raise ProspectiveCodeFreezeContractErrorV2(
                "prospective code-freeze receipt requires the policy validator"
            )
        if self.manifest_created_at_ms >= self.h_start_ms:
            raise ProspectiveCodeFreezeContractErrorV2(
                "code-freeze manifest must declare a time before H_start"
            )
        object.__setattr__(
            self,
            "receipt_sha256",
            hashlib.sha256(
                _RECEIPT_DOMAIN + canonical_json_line(_receipt_document(self))
            ).hexdigest(),
        )


def create_prospective_code_freeze_v2(
    *,
    workspace_root: str | Path,
    manifest_path: str | Path,
    h_start_ms: int,
    promoting_plan_sha256: str,
    prospective_execution_contract_sha256: str,
    prospective_efficacy_gate_sha256: str,
    created_at_utc: datetime | None = None,
) -> ProspectiveCodeFreezeReceiptV2:
    """Create and policy-validate the exact broad prospective code freeze."""

    timestamp = created_at_utc or datetime.now(UTC)
    timestamp = timestamp.replace(microsecond=(timestamp.microsecond // 1_000) * 1_000)
    authority = create_downstream_code_freeze_v1(
        workspace_root=workspace_root,
        manifest_path=manifest_path,
        purpose=PROSPECTIVE_CODE_FREEZE_PURPOSE_V2,
        include_trees=PROSPECTIVE_CODE_FREEZE_INCLUDE_TREES_V2,
        include_files=PROSPECTIVE_CODE_FREEZE_INCLUDE_FILES_V2,
        included_suffixes=PROSPECTIVE_CODE_FREEZE_SUFFIXES_V2,
        upstream_sha256=_upstream_bindings(
            promoting_plan_sha256=promoting_plan_sha256,
            prospective_execution_contract_sha256=(
                prospective_execution_contract_sha256
            ),
            prospective_efficacy_gate_sha256=prospective_efficacy_gate_sha256,
        ),
        created_at_utc=timestamp,
    )
    return validate_prospective_code_freeze_authority_v2(
        authority,
        h_start_ms=h_start_ms,
        promoting_plan_sha256=promoting_plan_sha256,
        prospective_execution_contract_sha256=(
            prospective_execution_contract_sha256
        ),
        prospective_efficacy_gate_sha256=prospective_efficacy_gate_sha256,
    )


def load_prospective_code_freeze_v2(
    manifest_path: str | Path,
    *,
    workspace_root: str | Path,
    expected_manifest_sha256: str,
    h_start_ms: int,
    promoting_plan_sha256: str,
    prospective_execution_contract_sha256: str,
    prospective_efficacy_gate_sha256: str,
) -> ProspectiveCodeFreezeReceiptV2:
    """Revalidate exact current workspace bytes and prospective policy."""

    bindings = _upstream_bindings(
        promoting_plan_sha256=promoting_plan_sha256,
        prospective_execution_contract_sha256=(
            prospective_execution_contract_sha256
        ),
        prospective_efficacy_gate_sha256=prospective_efficacy_gate_sha256,
    )
    authority = load_downstream_code_freeze_v1(
        manifest_path,
        workspace_root=workspace_root,
        expected_manifest_sha256=expected_manifest_sha256,
        required_upstream_sha256=bindings,
    )
    return validate_prospective_code_freeze_authority_v2(
        authority,
        h_start_ms=h_start_ms,
        promoting_plan_sha256=promoting_plan_sha256,
        prospective_execution_contract_sha256=(
            prospective_execution_contract_sha256
        ),
        prospective_efficacy_gate_sha256=prospective_efficacy_gate_sha256,
    )


def validate_prospective_code_freeze_authority_v2(
    authority: DownstreamCodeFreezeAuthorityV1,
    *,
    h_start_ms: int,
    promoting_plan_sha256: str,
    prospective_execution_contract_sha256: str,
    prospective_efficacy_gate_sha256: str,
) -> ProspectiveCodeFreezeReceiptV2:
    """Reject any valid generic manifest with a narrower prospective scope."""

    if type(authority) is not DownstreamCodeFreezeAuthorityV1:
        raise TypeError("authority must be exact DownstreamCodeFreezeAuthorityV1")
    if type(h_start_ms) is not int or h_start_ms < 0:
        raise ProspectiveCodeFreezeContractErrorV2(
            "h_start_ms must be a nonnegative Unix-millisecond integer"
        )
    expected_bindings = _upstream_bindings(
        promoting_plan_sha256=promoting_plan_sha256,
        prospective_execution_contract_sha256=(
            prospective_execution_contract_sha256
        ),
        prospective_efficacy_gate_sha256=prospective_efficacy_gate_sha256,
    )
    expected_policy = (
        authority.purpose == PROSPECTIVE_CODE_FREEZE_PURPOSE_V2
        and authority.include_trees == PROSPECTIVE_CODE_FREEZE_INCLUDE_TREES_V2
        and authority.include_files == PROSPECTIVE_CODE_FREEZE_INCLUDE_FILES_V2
        and authority.included_suffixes == PROSPECTIVE_CODE_FREEZE_SUFFIXES_V2
        and dict(authority.upstream_sha256) == expected_bindings
    )
    if not expected_policy:
        raise ProspectiveCodeFreezeContractErrorV2(
            "generic code-freeze authority differs from the exact prospective policy"
        )
    created_at = datetime.fromisoformat(authority.created_at_utc)
    if created_at.tzinfo is None or created_at.utcoffset() != UTC.utcoffset(created_at):
        raise ProspectiveCodeFreezeContractErrorV2(
            "manifest created_at_utc must be timezone-aware UTC"
        )
    if created_at.microsecond % 1_000:
        raise ProspectiveCodeFreezeContractErrorV2(
            "manifest created_at_utc must have exact millisecond precision"
        )
    created_at_ms = int(created_at.timestamp() * 1_000)
    return ProspectiveCodeFreezeReceiptV2(
        manifest_sha256=authority.manifest_sha256,
        manifest_created_at_ms=created_at_ms,
        h_start_ms=h_start_ms,
        upstream_sha256=tuple(sorted(expected_bindings.items())),
        _factory_token=_RECEIPT_FACTORY_TOKEN,
    )


def canonical_prospective_code_freeze_receipt_v2(
    receipt: ProspectiveCodeFreezeReceiptV2,
) -> bytes:
    """Return canonical receipt bytes after rechecking the content hash."""

    if type(receipt) is not ProspectiveCodeFreezeReceiptV2:
        raise TypeError("receipt must be exact ProspectiveCodeFreezeReceiptV2")
    document = _receipt_document(receipt)
    expected = hashlib.sha256(
        _RECEIPT_DOMAIN + canonical_json_line(document)
    ).hexdigest()
    if receipt.receipt_sha256 != expected:
        raise ProspectiveCodeFreezeContractErrorV2(
            "prospective code-freeze receipt hash differs"
        )
    return canonical_json_line({**document, "receipt_sha256": receipt.receipt_sha256})


def _upstream_bindings(
    *,
    promoting_plan_sha256: str,
    prospective_execution_contract_sha256: str,
    prospective_efficacy_gate_sha256: str,
) -> dict[str, str]:
    values = (
        promoting_plan_sha256,
        prospective_efficacy_gate_sha256,
        prospective_execution_contract_sha256,
    )
    for value, name in zip(values, _UPSTREAM_NAMES, strict=True):
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ProspectiveCodeFreezeContractErrorV2(
                f"{name} must be lowercase SHA-256 hex"
            )
    return dict(zip(_UPSTREAM_NAMES, values, strict=True))


def _receipt_document(receipt: ProspectiveCodeFreezeReceiptV2) -> dict[str, object]:
    return {
        "h_start_ms": receipt.h_start_ms,
        "manifest_created_at_ms": receipt.manifest_created_at_ms,
        "manifest_sha256": receipt.manifest_sha256,
        "schema_version": receipt.schema_version,
        "status": receipt.status,
        "upstream_sha256": dict(receipt.upstream_sha256),
    }
