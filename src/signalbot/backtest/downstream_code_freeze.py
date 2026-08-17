"""Exact post-census code-freeze authority for outcome-bearing research.

The census freeze must remain immutable after the outcome-blind census.  This
module creates a distinct, later authority over the code, contracts, and gate
tests that are allowed to read outcomes.  It hashes bytes only; it never opens
market data, census rows, or outcome artifacts.

Publication fsyncs both the staged file and the linked target file.  POSIX
fsyncs every created parent through the workspace plus the publication
directory after adding the target name and removing the staging name.  On
supported Windows hosts, the same directory boundaries are flushed through
writeable ``CreateFileW`` handles on local fixed NTFS volumes.  ReFS is outside
this contract because ``BY_HANDLE_FILE_INFORMATION`` exposes only a 64-bit file
index, which is not a unique ReFS file identity.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, cast

from signalbot.r4b_v2.canonical import canonical_json_line

DOWNSTREAM_CODE_FREEZE_SCHEMA_V1: Final = (
    "historical_three_family_downstream_code_freeze_v1"
)
DOWNSTREAM_CODE_FREEZE_STATUS_V1: Final = "FROZEN_BEFORE_OUTCOME_ACCESS"
DOWNSTREAM_CODE_FREEZE_MAX_FILE_BYTES_V1: Final = 64 * 1024 * 1024
DOWNSTREAM_CODE_FREEZE_POSIX_DURABILITY_CONTRACT_V1: Final = (
    "POSIX_FILE_CREATED_PARENT_CHAIN_AND_PUBLICATION_DIRECTORY_FSYNC_"
    "HARDLINK_NOREPLACE_V1"
)
DOWNSTREAM_CODE_FREEZE_WINDOWS_DURABILITY_CONTRACT_V1: Final = (
    "WINDOWS_LOCAL_FIXED_NTFS_FILE_CREATED_PARENT_CHAIN_AND_PUBLICATION_"
    "DIRECTORY_FLUSH_HARDLINK_NOREPLACE_V1"
)
_REGULAR_FILE_READ_CHUNK_BYTES_V1: Final = 1024 * 1024
_SHA256_RE: Final = re.compile(r"[0-9a-f]{64}")
_BINDING_NAME_RE: Final = re.compile(r"[a-z][a-z0-9_]{0,63}")
_MANIFEST_KEYS: Final = frozenset(
    {
        "schema_version",
        "status",
        "created_at_utc",
        "purpose",
        "include_trees",
        "include_files",
        "included_suffixes",
        "upstream_sha256",
        "file_count",
        "file_sha256",
        "file_size_bytes",
    }
)


class DownstreamCodeFreezeErrorV1(ValueError):
    """Raised when a downstream code-freeze authority is not exact."""


class DownstreamCodeFreezeDurabilityErrorV1(DownstreamCodeFreezeErrorV1):
    """Raised when the platform cannot prove the required publication boundary."""


class _WindowsByHandleFileInformationV1(ctypes.Structure):
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
class DownstreamCodeFreezeAuthorityV1:
    """Validated manifest identity and its exact current workspace bindings."""

    manifest_path: Path
    manifest_sha256: str
    created_at_utc: str
    purpose: str
    include_trees: tuple[str, ...]
    include_files: tuple[str, ...]
    included_suffixes: tuple[str, ...]
    upstream_sha256: Mapping[str, str]
    file_sha256: Mapping[str, str]
    file_size_bytes: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class _CollectedFileV1:
    sha256: str
    size_bytes: int


def downstream_code_freeze_durability_contract_v1() -> str:
    """Return the exact host contract required for a successful publication."""

    if os.name == "nt":
        return DOWNSTREAM_CODE_FREEZE_WINDOWS_DURABILITY_CONTRACT_V1
    if os.name == "posix":
        return DOWNSTREAM_CODE_FREEZE_POSIX_DURABILITY_CONTRACT_V1
    raise DownstreamCodeFreezeDurabilityErrorV1(
        "downstream freeze directory durability is unsupported on this platform"
    )


def create_downstream_code_freeze_v1(
    *,
    workspace_root: str | Path,
    manifest_path: str | Path,
    purpose: str,
    include_trees: Sequence[str],
    include_files: Sequence[str] = (),
    included_suffixes: Sequence[str] = (".py",),
    upstream_sha256: Mapping[str, str],
    created_at_utc: datetime | None = None,
) -> DownstreamCodeFreezeAuthorityV1:
    """Create and immediately validate one canonical downstream freeze.

    Tree scopes are recursive.  Only regular files with one of
    ``included_suffixes`` are members; every symlink encountered in a scoped
    tree is rejected.  Explicit files are included regardless of suffix.
    """

    workspace = _workspace_root(workspace_root)
    manifest = _manifest_target(workspace, manifest_path)
    trees = _normalized_unique_paths(include_trees, "include_trees")
    files = _normalized_unique_paths(include_files, "include_files")
    suffixes = _normalized_suffixes(included_suffixes)
    bindings = _validated_bindings(upstream_sha256, "upstream_sha256")
    purpose_text = _nonempty_string(purpose, "purpose")
    _reject_manifest_self_reference(manifest, workspace, trees, files)
    collected = _collect_files(workspace, trees, files, suffixes)
    timestamp = created_at_utc or datetime.now(UTC)
    created_text = _canonical_utc(timestamp, "created_at_utc")
    document: dict[str, object] = {
        "schema_version": DOWNSTREAM_CODE_FREEZE_SCHEMA_V1,
        "status": DOWNSTREAM_CODE_FREEZE_STATUS_V1,
        "created_at_utc": created_text,
        "purpose": purpose_text,
        "include_trees": list(trees),
        "include_files": list(files),
        "included_suffixes": list(suffixes),
        "upstream_sha256": bindings,
        "file_count": len(collected),
        "file_sha256": {
            path: value.sha256 for path, value in collected.items()
        },
        "file_size_bytes": {
            path: value.size_bytes for path, value in collected.items()
        },
    }
    raw = canonical_json_line(document)
    _write_new_atomic(workspace, manifest, raw)
    return load_downstream_code_freeze_v1(
        manifest,
        workspace_root=workspace,
        required_upstream_sha256=bindings,
    )


def load_downstream_code_freeze_v1(
    manifest_path: str | Path,
    *,
    workspace_root: str | Path,
    expected_manifest_sha256: str | None = None,
    required_upstream_sha256: Mapping[str, str] | None = None,
    forbidden_manifest_sha256: Sequence[str] = (),
) -> DownstreamCodeFreezeAuthorityV1:
    """Validate canonical bytes, exact membership, hashes, and bindings."""

    workspace = _workspace_root(workspace_root)
    manifest = _manifest_target(workspace, manifest_path)
    raw = _read_regular_file(manifest, "downstream code-freeze manifest")
    document = _decode_canonical_object(raw)
    if set(document) != _MANIFEST_KEYS:
        missing = sorted(_MANIFEST_KEYS - set(document))
        extra = sorted(set(document) - _MANIFEST_KEYS)
        raise DownstreamCodeFreezeErrorV1(
            f"manifest fields are not exact (missing={missing}, extra={extra})"
        )
    if document["schema_version"] != DOWNSTREAM_CODE_FREEZE_SCHEMA_V1:
        raise DownstreamCodeFreezeErrorV1("manifest schema_version is not exact")
    if document["status"] != DOWNSTREAM_CODE_FREEZE_STATUS_V1:
        raise DownstreamCodeFreezeErrorV1("manifest is not frozen before outcome access")
    created_text = _parse_canonical_utc(document["created_at_utc"])
    purpose = _nonempty_string(document["purpose"], "purpose")
    trees = _string_list(document["include_trees"], "include_trees")
    files = _string_list(document["include_files"], "include_files")
    suffixes = _string_list(document["included_suffixes"], "included_suffixes")
    if trees != _normalized_unique_paths(trees, "include_trees"):
        raise DownstreamCodeFreezeErrorV1("include_trees must be sorted and unique")
    if files != _normalized_unique_paths(files, "include_files"):
        raise DownstreamCodeFreezeErrorV1("include_files must be sorted and unique")
    if suffixes != _normalized_suffixes(suffixes):
        raise DownstreamCodeFreezeErrorV1(
            "included_suffixes must be sorted, unique, normalized suffixes"
        )
    bindings = _validated_bindings(
        _object(document["upstream_sha256"], "upstream_sha256"),
        "upstream_sha256",
    )
    required = _validated_bindings(
        required_upstream_sha256 or {}, "required_upstream_sha256"
    )
    for name, expected in required.items():
        if bindings.get(name) != expected:
            raise DownstreamCodeFreezeErrorV1(
                f"required upstream binding differs: {name}"
            )
    expected_hashes = _hash_map(document["file_sha256"], "file_sha256")
    expected_sizes = _size_map(document["file_size_bytes"], "file_size_bytes")
    count = document["file_count"]
    if type(count) is not int or count <= 0:
        raise DownstreamCodeFreezeErrorV1("file_count must be a positive integer")
    if set(expected_hashes) != set(expected_sizes) or count != len(expected_hashes):
        raise DownstreamCodeFreezeErrorV1(
            "file_count, file_sha256, and file_size_bytes do not reconcile"
        )
    _reject_manifest_self_reference(manifest, workspace, trees, files)
    actual = _collect_files(workspace, trees, files, suffixes)
    if set(actual) != set(expected_hashes):
        missing = sorted(set(expected_hashes) - set(actual))
        extra = sorted(set(actual) - set(expected_hashes))
        raise DownstreamCodeFreezeErrorV1(
            f"frozen file membership drift (missing={missing}, extra={extra})"
        )
    for path, value in actual.items():
        if expected_hashes[path] != value.sha256:
            raise DownstreamCodeFreezeErrorV1(f"frozen file hash drift: {path}")
        if expected_sizes[path] != value.size_bytes:
            raise DownstreamCodeFreezeErrorV1(f"frozen file size drift: {path}")
    manifest_sha256 = _sha256(raw)
    if expected_manifest_sha256 is not None and manifest_sha256 != _sha256_text(
        expected_manifest_sha256, "expected_manifest_sha256"
    ):
        raise DownstreamCodeFreezeErrorV1(
            "downstream manifest SHA-256 differs from its frozen authority"
        )
    forbidden = tuple(
        _sha256_text(value, "forbidden_manifest_sha256")
        for value in forbidden_manifest_sha256
    )
    if manifest_sha256 in forbidden:
        raise DownstreamCodeFreezeErrorV1(
            "downstream manifest must be distinct from a forbidden authority"
        )
    return DownstreamCodeFreezeAuthorityV1(
        manifest_path=manifest,
        manifest_sha256=manifest_sha256,
        created_at_utc=created_text,
        purpose=purpose,
        include_trees=trees,
        include_files=files,
        included_suffixes=suffixes,
        upstream_sha256=bindings,
        file_sha256=expected_hashes,
        file_size_bytes=expected_sizes,
    )


def _collect_files(
    workspace: Path,
    trees: tuple[str, ...],
    files: tuple[str, ...],
    suffixes: tuple[str, ...],
) -> dict[str, _CollectedFileV1]:
    collected: dict[str, _CollectedFileV1] = {}
    for tree in trees:
        root = _workspace_member(workspace, tree)
        _require_real_directory(root, f"included tree {tree}")
        for current, directory_names, file_names in os.walk(
            root, topdown=True, followlinks=False
        ):
            directory_names.sort()
            file_names.sort()
            current_path = Path(current)
            for name in directory_names:
                candidate = current_path / name
                if candidate.is_symlink():
                    relative = candidate.relative_to(workspace).as_posix()
                    raise DownstreamCodeFreezeErrorV1(
                        f"symlink is forbidden in included tree: {relative}"
                    )
            for name in file_names:
                candidate = current_path / name
                relative = candidate.relative_to(workspace).as_posix()
                if candidate.is_symlink():
                    raise DownstreamCodeFreezeErrorV1(
                        f"symlink is forbidden in included tree: {relative}"
                    )
                if not name.endswith(suffixes):
                    continue
                _add_file(collected, candidate, relative)
    for relative in files:
        _add_file(collected, _workspace_member(workspace, relative), relative)
    if not collected:
        raise DownstreamCodeFreezeErrorV1("freeze scope contains no regular files")
    return dict(sorted(collected.items()))


def _add_file(
    collected: dict[str, _CollectedFileV1], candidate: Path, relative: str
) -> None:
    if relative in collected:
        raise DownstreamCodeFreezeErrorV1(
            f"file belongs to more than one freeze scope: {relative}"
        )
    raw = _read_regular_file(candidate, f"included file {relative}")
    collected[relative] = _CollectedFileV1(
        sha256=_sha256(raw), size_bytes=len(raw)
    )


def _workspace_root(value: str | Path) -> Path:
    candidate = Path(value)
    try:
        candidate_metadata = candidate.stat(follow_symlinks=False)
    except OSError as exc:
        raise DownstreamCodeFreezeErrorV1("workspace_root is missing") from exc
    if _is_link_or_reparse_v1(candidate_metadata):
        raise DownstreamCodeFreezeErrorV1(
            "workspace_root must not be a symlink or reparse point"
        )
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise DownstreamCodeFreezeErrorV1("workspace_root is missing") from exc
    _require_real_directory(resolved, "workspace_root")
    return resolved


def _manifest_target(workspace: Path, value: str | Path) -> Path:
    raw = Path(value)
    candidate = raw if raw.is_absolute() else workspace / raw
    absolute = Path(os.path.abspath(candidate))
    try:
        absolute.relative_to(workspace)
    except ValueError as exc:
        raise DownstreamCodeFreezeErrorV1(
            "manifest_path must remain inside workspace_root"
        ) from exc
    _reject_existing_symlink_components(workspace, absolute)
    if absolute == workspace:
        raise DownstreamCodeFreezeErrorV1("manifest_path cannot be workspace_root")
    return absolute


def _workspace_member(workspace: Path, relative: str) -> Path:
    candidate = workspace.joinpath(*relative.split("/"))
    _reject_existing_symlink_components(workspace, candidate)
    return candidate


def _reject_existing_symlink_components(workspace: Path, candidate: Path) -> None:
    relative = candidate.relative_to(workspace)
    current = workspace
    for part in relative.parts:
        current /= part
        try:
            metadata = current.stat(follow_symlinks=False)
        except FileNotFoundError:
            break
        except OSError as exc:
            raise DownstreamCodeFreezeErrorV1(
                f"path component is unavailable: {current}"
            ) from exc
        if _is_link_or_reparse_v1(metadata):
            raise DownstreamCodeFreezeErrorV1(
                f"symlink or reparse path component is forbidden: {current}"
            )


def _reject_manifest_self_reference(
    manifest: Path,
    workspace: Path,
    trees: tuple[str, ...],
    files: tuple[str, ...],
) -> None:
    relative = manifest.relative_to(workspace).as_posix()
    if relative in files:
        raise DownstreamCodeFreezeErrorV1(
            "manifest cannot include itself as an explicit file"
        )
    for tree in trees:
        tree_path = _workspace_member(workspace, tree)
        if manifest == tree_path or tree_path in manifest.parents:
            raise DownstreamCodeFreezeErrorV1(
                "manifest cannot be stored inside an included tree"
            )


def _read_regular_file(
    path: Path,
    label: str,
    *,
    expected_metadata: os.stat_result | None = None,
) -> bytes:
    try:
        path_before = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise DownstreamCodeFreezeErrorV1(f"{label} is missing or unreadable") from exc
    if _is_link_or_reparse_v1(path_before) or not stat.S_ISREG(path_before.st_mode):
        raise DownstreamCodeFreezeErrorV1(
            f"{label} must be an exact regular non-symlink file"
        )
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise DownstreamCodeFreezeErrorV1(
            f"{label} is missing, unreadable, or not a regular non-symlink file"
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or _regular_file_object_identity(path_before)
            != _regular_file_object_identity(opened)
            or _regular_file_common_state(path_before)
            != _regular_file_common_state(opened)
        ):
            raise DownstreamCodeFreezeErrorV1(
                f"{label} identity changed while opening"
            )
        if expected_metadata is not None and (
            _regular_file_object_identity(opened)
            != _regular_file_object_identity(expected_metadata)
            or _regular_file_common_state(opened)
            != _regular_file_common_state(expected_metadata)
        ):
            raise DownstreamCodeFreezeErrorV1(
                f"{label} differs from the exact staged file object"
            )
        if opened.st_size > DOWNSTREAM_CODE_FREEZE_MAX_FILE_BYTES_V1:
            raise DownstreamCodeFreezeErrorV1(
                f"{label} exceeds the frozen per-file byte cap"
            )
        chunks: list[bytes] = []
        retained_bytes = 0
        while retained_bytes <= DOWNSTREAM_CODE_FREEZE_MAX_FILE_BYTES_V1:
            remaining = (
                DOWNSTREAM_CODE_FREEZE_MAX_FILE_BYTES_V1 + 1 - retained_bytes
            )
            chunk = os.read(
                descriptor,
                min(_REGULAR_FILE_READ_CHUNK_BYTES_V1, remaining),
            )
            if not chunk:
                break
            chunks.append(chunk)
            retained_bytes += len(chunk)
        if retained_bytes > DOWNSTREAM_CODE_FREEZE_MAX_FILE_BYTES_V1:
            raise DownstreamCodeFreezeErrorV1(
                f"{label} exceeds the frozen per-file byte cap"
            )
        try:
            descriptor_after = os.fstat(descriptor)
            path_after = path.stat(follow_symlinks=False)
        except OSError as exc:
            raise DownstreamCodeFreezeErrorV1(
                f"{label} identity cannot be revalidated after reading"
            ) from exc
        if (
            _is_link_or_reparse_v1(path_after)
            or not stat.S_ISREG(path_after.st_mode)
            or _regular_file_descriptor_state(descriptor_after)
            != _regular_file_descriptor_state(opened)
            or _regular_file_object_identity(path_after)
            != _regular_file_object_identity(opened)
            or _regular_file_common_state(path_after)
            != _regular_file_common_state(opened)
            or retained_bytes != opened.st_size
        ):
            raise DownstreamCodeFreezeErrorV1(
                f"{label} identity or size changed during reading"
            )
        return b"".join(chunks)
    except DownstreamCodeFreezeErrorV1:
        raise
    except OSError as exc:
        raise DownstreamCodeFreezeErrorV1(f"{label} is unreadable") from exc
    finally:
        try:
            os.close(descriptor)
        except OSError as exc:
            raise DownstreamCodeFreezeErrorV1(
                f"{label} descriptor could not be closed"
            ) from exc


def _regular_file_object_identity(
    metadata: os.stat_result,
) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


def _is_link_or_reparse_v1(metadata: os.stat_result) -> bool:
    if stat.S_ISLNK(metadata.st_mode):
        return True
    attributes = getattr(metadata, "st_file_attributes", 0)
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    return bool(attributes & reparse_flag)


def _regular_file_common_state(
    metadata: os.stat_result,
) -> tuple[int, int, int]:
    """Return metadata represented consistently by path stat and descriptor stat."""

    return (
        metadata.st_mode,
        metadata.st_size,
        metadata.st_mtime_ns,
    )


def _regular_file_descriptor_state(
    metadata: os.stat_result,
) -> tuple[int, int, int, int, int, int]:
    return (
        *_regular_file_object_identity(metadata),
        *_regular_file_common_state(metadata),
        metadata.st_ctime_ns,
    )


def _require_real_directory(path: Path, label: str) -> os.stat_result:
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise DownstreamCodeFreezeErrorV1(f"{label} is missing") from exc
    if _is_link_or_reparse_v1(metadata) or not stat.S_ISDIR(metadata.st_mode):
        raise DownstreamCodeFreezeErrorV1(
            f"{label} must be an exact real non-reparse directory"
        )
    return metadata


def _normalized_unique_paths(values: Sequence[str], label: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise DownstreamCodeFreezeErrorV1(f"{label} must be a sequence of paths")
    normalized = tuple(_relative_posix_path(value, label) for value in values)
    if len(set(normalized)) != len(normalized):
        raise DownstreamCodeFreezeErrorV1(f"{label} contains duplicate paths")
    return tuple(sorted(normalized))


def _relative_posix_path(value: object, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.startswith("/")
        or "\\" in value
        or "//" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise DownstreamCodeFreezeErrorV1(
            f"{label} must contain normalized relative POSIX paths"
        )
    return value


def _normalized_suffixes(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise DownstreamCodeFreezeErrorV1(
            "included_suffixes must be a sequence"
        )
    suffixes = tuple(values)
    if (
        not suffixes
        or any(
            not isinstance(value, str)
            or not value.startswith(".")
            or len(value) < 2
            or not value[1:].replace("_", "").isalnum()
            for value in suffixes
        )
        or len(set(suffixes)) != len(suffixes)
    ):
        raise DownstreamCodeFreezeErrorV1(
            "included_suffixes must be unique dot-prefixed filename suffixes"
        )
    return tuple(sorted(suffixes))


def _validated_bindings(
    value: Mapping[str, object], label: str
) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise DownstreamCodeFreezeErrorV1(f"{label} must be an object")
    result: dict[str, str] = {}
    for name, digest in value.items():
        if not isinstance(name, str) or _BINDING_NAME_RE.fullmatch(name) is None:
            raise DownstreamCodeFreezeErrorV1(
                f"{label} names must be normalized lowercase identifiers"
            )
        result[name] = _sha256_text(digest, f"{label}.{name}")
    return dict(sorted(result.items()))


def _hash_map(value: object, label: str) -> dict[str, str]:
    document = _object(value, label)
    result: dict[str, str] = {}
    for path, digest in document.items():
        relative = _relative_posix_path(path, label)
        result[relative] = _sha256_text(digest, f"{label}.{relative}")
    if list(document) != sorted(document):
        raise DownstreamCodeFreezeErrorV1(f"{label} keys must be sorted")
    return result


def _size_map(value: object, label: str) -> dict[str, int]:
    document = _object(value, label)
    result: dict[str, int] = {}
    for path, size in document.items():
        relative = _relative_posix_path(path, label)
        if type(size) is not int or size < 0:
            raise DownstreamCodeFreezeErrorV1(
                f"{label}.{relative} must be a nonnegative integer"
            )
        result[relative] = size
    if list(document) != sorted(document):
        raise DownstreamCodeFreezeErrorV1(f"{label} keys must be sorted")
    return result


def _string_list(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise DownstreamCodeFreezeErrorV1(f"{label} must be an array of strings")
    return tuple(cast(list[str], value))


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise DownstreamCodeFreezeErrorV1(f"{label} must be an object")
    return cast(dict[str, object], value)


def _decode_canonical_object(raw: bytes) -> dict[str, object]:
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise DownstreamCodeFreezeErrorV1(
            "manifest is not valid UTF-8 JSON"
        ) from exc
    document = _object(value, "manifest")
    try:
        canonical = canonical_json_line(document)
    except (TypeError, ValueError) as exc:
        raise DownstreamCodeFreezeErrorV1(
            "manifest contains unsupported protocol JSON"
        ) from exc
    if raw != canonical:
        raise DownstreamCodeFreezeErrorV1(
            "manifest must be canonical RFC 8785 JSONL"
        )
    return document


def _canonical_utc(value: datetime, label: str) -> str:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise DownstreamCodeFreezeErrorV1(f"{label} must be timezone-aware UTC")
    return value.isoformat()


def _parse_canonical_utc(value: object) -> str:
    text = _nonempty_string(value, "created_at_utc")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise DownstreamCodeFreezeErrorV1(
            "created_at_utc must be canonical ISO-8601 UTC"
        ) from exc
    if (
        parsed.tzinfo is None
        or parsed.utcoffset() != UTC.utcoffset(parsed)
        or parsed.isoformat() != text
        or not text.endswith("+00:00")
    ):
        raise DownstreamCodeFreezeErrorV1(
            "created_at_utc must be canonical with explicit +00:00"
        )
    return text


def _nonempty_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise DownstreamCodeFreezeErrorV1(
            f"{label} must be a nonempty trimmed string"
        )
    return value


def _sha256_text(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise DownstreamCodeFreezeErrorV1(
            f"{label} must be a lowercase SHA-256 digest"
        )
    return value


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_new_atomic(workspace: Path, path: Path, raw: bytes) -> None:
    """Publish one immutable freeze manifest without replacing an existing one.

    Once the hard link succeeds, any later failure is durability-ambiguous: the
    target is deliberately retained and the caller is instructed not to retry,
    delete, or replace it.  A read-only audit is the only safe next operation.
    """

    _create_and_flush_manifest_parent_chain_v1(
        workspace=workspace,
        parent=path.parent,
    )
    _reject_existing_symlink_components(workspace, path)
    temporary_path: Path | None = None
    target_linked = False
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as handle:
            temporary_path = Path(handle.name)
            written = handle.write(raw)
            if written != len(raw):
                raise DownstreamCodeFreezeErrorV1(
                    "temporary freeze manifest write was short"
                )
            handle.flush()
            os.fsync(handle.fileno())
            staged_metadata = os.fstat(handle.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError as exc:
            target_linked = _target_may_be_staged_file_v1(path, staged_metadata)
            raise DownstreamCodeFreezeErrorV1(
                "freeze manifest already exists and cannot be replaced"
            ) from exc
        except OSError:
            # A filesystem call may commit the hard link and still report an
            # error.  Prove absence/difference before treating the operation
            # as pre-publication; an uninspectable matching candidate is
            # conservatively durability-ambiguous.
            target_linked = _target_may_be_staged_file_v1(path, staged_metadata)
            raise
        target_linked = True
        _fsync_linked_regular_file(path, staged_metadata)
        _fsync_publication_directory(path)
        temporary_path.unlink()
        temporary_path = None
        _fsync_publication_directory(path)
        observed = _read_regular_file(
            path,
            "published freeze manifest",
            expected_metadata=staged_metadata,
        )
        if observed != raw:
            raise DownstreamCodeFreezeErrorV1(
                "published freeze manifest bytes differ from staged bytes"
            )
    except (DownstreamCodeFreezeErrorV1, OSError) as exc:
        cleanup_error: OSError | None = None
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError as cleanup_exc:
                cleanup_error = cleanup_exc
        if target_linked:
            detail = (
                "freeze manifest publication is durability-ambiguous after the "
                "no-replace target link succeeded; do not retry, delete, or "
                "replace the target; inspect it read-only"
            )
            if cleanup_error is not None:
                detail += "; temporary-name cleanup also failed"
            raise DownstreamCodeFreezeErrorV1(detail) from cleanup_error or exc
        if cleanup_error is not None:
            raise DownstreamCodeFreezeErrorV1(
                "cannot publish or clean up temporary freeze manifest"
            ) from cleanup_error
        if isinstance(exc, DownstreamCodeFreezeErrorV1):
            raise
        raise DownstreamCodeFreezeErrorV1("cannot publish freeze manifest") from exc


def _fsync_linked_regular_file(
    path: Path,
    staged_metadata: os.stat_result,
) -> None:
    """Flush the linked file handle and prove it is the staged file object."""

    try:
        path_before = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "linked freeze manifest pathname cannot be inspected"
        ) from exc
    if (
        _is_link_or_reparse_v1(path_before)
        or not stat.S_ISREG(path_before.st_mode)
        or _regular_file_object_identity(path_before)
        != _regular_file_object_identity(staged_metadata)
        or _regular_file_common_state(path_before)
        != _regular_file_common_state(staged_metadata)
    ):
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "linked freeze manifest pathname differs from the staged file object"
        )
    flags = (
        os.O_RDWR
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "linked freeze manifest cannot be opened for durability flush"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or _regular_file_object_identity(before)
            != _regular_file_object_identity(staged_metadata)
            or _regular_file_common_state(before)
            != _regular_file_common_state(staged_metadata)
            or _regular_file_object_identity(path_before)
            != _regular_file_object_identity(before)
            or _regular_file_common_state(path_before)
            != _regular_file_common_state(before)
        ):
            raise DownstreamCodeFreezeDurabilityErrorV1(
                "linked freeze manifest differs from the staged file object"
            )
        os.fsync(descriptor)
        after = os.fstat(descriptor)
        path_after = path.stat(follow_symlinks=False)
        if (
            _is_link_or_reparse_v1(path_after)
            or not stat.S_ISREG(path_after.st_mode)
            or _regular_file_descriptor_state(after)
            != _regular_file_descriptor_state(before)
            or _regular_file_object_identity(path_after)
            != _regular_file_object_identity(after)
            or _regular_file_common_state(path_after)
            != _regular_file_common_state(after)
        ):
            raise DownstreamCodeFreezeDurabilityErrorV1(
                "linked freeze manifest changed while being flushed"
            )
    except DownstreamCodeFreezeErrorV1:
        raise
    except OSError as exc:
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "linked freeze manifest durability flush failed"
        ) from exc
    finally:
        try:
            os.close(descriptor)
        except OSError as exc:
            raise DownstreamCodeFreezeDurabilityErrorV1(
                "linked freeze manifest descriptor close failed"
            ) from exc


def _target_may_be_staged_file_v1(
    path: Path,
    staged_metadata: os.stat_result,
) -> bool:
    """Return whether a failed link call may already have published the stage."""

    try:
        observed = path.stat(follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError:
        return True
    if _is_link_or_reparse_v1(observed) or not stat.S_ISREG(observed.st_mode):
        return False
    return bool(
        _regular_file_object_identity(observed)
        == _regular_file_object_identity(staged_metadata)
        and _regular_file_common_state(observed)
        == _regular_file_common_state(staged_metadata)
    )


def _create_and_flush_manifest_parent_chain_v1(
    *,
    workspace: Path,
    parent: Path,
) -> None:
    try:
        parent.relative_to(workspace)
    except ValueError as exc:
        raise DownstreamCodeFreezeErrorV1(
            "manifest parent must remain inside workspace_root"
        ) from exc
    downstream_code_freeze_durability_contract_v1()
    _reject_existing_symlink_components(workspace, parent)
    if os.name == "nt":
        # Qualify the already-existing workspace before mkdir mutates any
        # directory.  Containment plus the no-reparse rule keeps descendants
        # on this qualified volume.
        _windows_local_volume_identity_v1(workspace)
    flush_chain = _manifest_parent_flush_chain_v1(
        workspace=workspace,
        parent=parent,
    )
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise DownstreamCodeFreezeErrorV1(
            "manifest parent directory cannot be created"
        ) from exc
    _reject_existing_symlink_components(workspace, parent)
    for directory in flush_chain:
        _require_real_directory(directory, "manifest parent chain")
        _flush_directory_entry_v1(directory)


def _manifest_parent_flush_chain_v1(
    *,
    workspace: Path,
    parent: Path,
) -> tuple[Path, ...]:
    """Capture missing parents deepest-first plus their first existing parent."""

    missing: list[Path] = []
    current = parent
    while True:
        try:
            metadata = current.stat(follow_symlinks=False)
        except FileNotFoundError as exc:
            if current == workspace:
                raise DownstreamCodeFreezeDurabilityErrorV1(
                    "workspace disappeared before manifest parent creation"
                ) from exc
            missing.append(current)
            current = current.parent
            continue
        except OSError as exc:
            raise DownstreamCodeFreezeDurabilityErrorV1(
                "manifest parent chain cannot be inspected before creation"
            ) from exc
        if _is_link_or_reparse_v1(metadata) or not stat.S_ISDIR(metadata.st_mode):
            raise DownstreamCodeFreezeDurabilityErrorV1(
                "manifest parent chain must contain only real directories"
            )
        return (*missing, current)


def _fsync_publication_directory(path: Path) -> None:
    """Flush the exact directory containing one publication pathname."""

    _flush_directory_entry_v1(path.parent)


def _flush_directory_entry_v1(path: Path) -> None:
    if os.name == "nt":
        _windows_flush_directory_entry_v1(path)
        return
    if os.name != "posix":
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "directory durability is unsupported on this platform"
        )
    _posix_flush_directory_entry_v1(path)


def _posix_flush_directory_entry_v1(path: Path) -> None:
    before = _require_real_directory(path, "POSIX directory flush target")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor: int | None = None
    try:
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or _directory_identity_v1(opened) != _directory_identity_v1(before)
        ):
            raise DownstreamCodeFreezeDurabilityErrorV1(
                "POSIX directory flush handle differs from its pathname"
            )
        os.fsync(descriptor)
        descriptor_after = os.fstat(descriptor)
        path_after = _require_real_directory(path, "POSIX directory flush target")
        if (
            _directory_identity_v1(descriptor_after)
            != _directory_identity_v1(opened)
            or _directory_identity_v1(path_after) != _directory_identity_v1(opened)
        ):
            raise DownstreamCodeFreezeDurabilityErrorV1(
                "POSIX directory identity changed while being flushed"
            )
    except DownstreamCodeFreezeErrorV1:
        raise
    except OSError as exc:
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "POSIX directory fsync is required by the durability contract"
        ) from exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as exc:
                raise DownstreamCodeFreezeDurabilityErrorV1(
                    "POSIX directory descriptor close failed"
                ) from exc


def _windows_kernel32_v1():
    loader = getattr(ctypes, "WinDLL", None)
    if loader is None:
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "Win32 downstream freeze durability APIs are unavailable"
        )
    try:
        return loader("kernel32", use_last_error=True)
    except (OSError, TypeError) as exc:
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "Win32 downstream freeze durability APIs are unavailable"
        ) from exc


def _windows_api_v1(name: str):
    try:
        return getattr(_windows_kernel32_v1(), name)
    except AttributeError as exc:
        raise DownstreamCodeFreezeDurabilityErrorV1(
            f"Win32 downstream freeze durability API {name} is unavailable"
        ) from exc


def _windows_last_error_v1() -> int:
    getter = getattr(ctypes, "get_last_error", None)
    return 0 if getter is None else int(getter())


def _windows_open_directory_handle_v1(path: Path) -> int:
    from ctypes import wintypes

    create_file = _windows_api_v1("CreateFileW")
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
        error_number = _windows_last_error_v1()
        raise DownstreamCodeFreezeDurabilityErrorV1(
            f"CreateFileW downstream freeze directory failed with error {error_number}"
        )
    return int(value)


def _windows_file_information_v1(
    handle: int,
) -> _WindowsByHandleFileInformationV1:
    from ctypes import wintypes

    get_information = _windows_api_v1("GetFileInformationByHandle")
    get_information.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(_WindowsByHandleFileInformationV1),
    )
    get_information.restype = wintypes.BOOL
    information = _WindowsByHandleFileInformationV1()
    if not get_information(wintypes.HANDLE(handle), ctypes.byref(information)):
        error_number = _windows_last_error_v1()
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "GetFileInformationByHandle downstream freeze directory failed "
            f"with error {error_number}"
        )
    return information


def _windows_directory_handle_identity_v1(
    information: _WindowsByHandleFileInformationV1,
) -> tuple[int, int]:
    file_index = (int(information.file_index_high) << 32) | int(
        information.file_index_low
    )
    return int(information.volume_serial_number), file_index


def _require_windows_real_directory_handle_v1(
    information: _WindowsByHandleFileInformationV1,
) -> None:
    attributes = int(information.file_attributes)
    if not attributes & 0x00000010 or attributes & 0x00000400:
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "Win32 downstream freeze handle must name one real directory"
        )


def _windows_flush_directory_handle_v1(handle: int) -> None:
    from ctypes import wintypes

    flush = _windows_api_v1("FlushFileBuffers")
    flush.argtypes = (wintypes.HANDLE,)
    flush.restype = wintypes.BOOL
    if not flush(wintypes.HANDLE(handle)):
        error_number = _windows_last_error_v1()
        raise DownstreamCodeFreezeDurabilityErrorV1(
            f"FlushFileBuffers downstream freeze directory failed with error {error_number}"
        )


def _windows_close_handle_v1(handle: int) -> None:
    from ctypes import wintypes

    close = _windows_api_v1("CloseHandle")
    close.argtypes = (wintypes.HANDLE,)
    close.restype = wintypes.BOOL
    if not close(wintypes.HANDLE(handle)):
        error_number = _windows_last_error_v1()
        raise DownstreamCodeFreezeDurabilityErrorV1(
            f"CloseHandle downstream freeze directory failed with error {error_number}"
        )


def _windows_local_volume_identity_v1(path: Path) -> tuple[str, int]:
    """Qualify fixed NTFS for the 64-bit directory-identity contract.

    ReFS is deliberately rejected: its 128-bit file IDs are not represented
    uniquely by ``BY_HANDLE_FILE_INFORMATION``'s 64-bit file index.
    """

    from ctypes import wintypes

    volume_path = ctypes.create_unicode_buffer(261)
    get_volume_path = _windows_api_v1("GetVolumePathNameW")
    get_volume_path.argtypes = (wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD)
    get_volume_path.restype = wintypes.BOOL
    if not get_volume_path(os.fspath(path), volume_path, len(volume_path)):
        error_number = _windows_last_error_v1()
        raise DownstreamCodeFreezeDurabilityErrorV1(
            f"GetVolumePathNameW downstream freeze failed with error {error_number}"
        )
    root = volume_path.value
    get_drive_type = _windows_api_v1("GetDriveTypeW")
    get_drive_type.argtypes = (wintypes.LPCWSTR,)
    get_drive_type.restype = wintypes.UINT
    if int(get_drive_type(root)) != 3:
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "downstream freeze requires a local fixed Windows volume"
        )

    serial = wintypes.DWORD()
    filesystem = ctypes.create_unicode_buffer(64)
    get_volume_information = _windows_api_v1("GetVolumeInformationW")
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
        error_number = _windows_last_error_v1()
        raise DownstreamCodeFreezeDurabilityErrorV1(
            f"GetVolumeInformationW downstream freeze failed with error {error_number}"
        )
    filesystem_name = filesystem.value.upper()
    if filesystem_name != "NTFS":
        raise DownstreamCodeFreezeDurabilityErrorV1(
            "downstream freeze requires local fixed NTFS; ReFS and other "
            "filesystems are unsupported by the 64-bit directory identity contract"
        )
    serial_value = int(serial.value)
    return (
        f"{os.path.normcase(root)}|{filesystem_name}|{serial_value:08x}",
        serial_value,
    )


def _windows_flush_directory_entry_v1(path: Path) -> None:
    before = _require_real_directory(path, "Windows directory flush target")
    _volume_identity, expected_volume_serial = _windows_local_volume_identity_v1(path)
    handle = _windows_open_directory_handle_v1(path)
    try:
        opened = _windows_file_information_v1(handle)
        _require_windows_real_directory_handle_v1(opened)
        opened_identity = _windows_directory_handle_identity_v1(opened)
        if opened_identity[0] != expected_volume_serial:
            raise DownstreamCodeFreezeDurabilityErrorV1(
                "Win32 downstream freeze directory differs from its qualified volume"
            )
        _windows_flush_directory_handle_v1(handle)
        after_flush = _windows_file_information_v1(handle)
        _require_windows_real_directory_handle_v1(after_flush)
        if _windows_directory_handle_identity_v1(after_flush) != opened_identity:
            raise DownstreamCodeFreezeDurabilityErrorV1(
                "Win32 downstream freeze directory changed during flush"
            )
        path_handle = _windows_open_directory_handle_v1(path)
        try:
            path_information = _windows_file_information_v1(path_handle)
            _require_windows_real_directory_handle_v1(path_information)
            if _windows_directory_handle_identity_v1(path_information) != opened_identity:
                raise DownstreamCodeFreezeDurabilityErrorV1(
                    "Win32 downstream freeze pathname identity changed during flush"
                )
        finally:
            _windows_close_handle_v1(path_handle)
        after = _require_real_directory(path, "Windows directory flush target")
        if _directory_identity_v1(after) != _directory_identity_v1(before):
            raise DownstreamCodeFreezeDurabilityErrorV1(
                "Windows downstream freeze directory pathname changed during flush"
            )
    finally:
        _windows_close_handle_v1(handle)


def _directory_identity_v1(metadata: os.stat_result) -> tuple[int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_mode


def _binding_arguments(values: Sequence[str], label: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        name, separator, digest = value.partition("=")
        if not separator or name in result:
            raise DownstreamCodeFreezeErrorV1(
                f"{label} must contain unique NAME=SHA256 values"
            )
        result[name] = digest
    return _validated_bindings(result, label)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create or validate an exact downstream research code freeze."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create")
    create.add_argument("--workspace-root", type=Path, default=Path.cwd())
    create.add_argument("--manifest", type=Path, required=True)
    create.add_argument("--purpose", required=True)
    create.add_argument("--include-tree", action="append", default=[])
    create.add_argument("--include-file", action="append", default=[])
    create.add_argument("--suffix", action="append", default=[])
    create.add_argument("--binding", action="append", default=[])
    create.add_argument("--created-at-utc")
    validate = subparsers.add_parser("validate")
    validate.add_argument("--workspace-root", type=Path, default=Path.cwd())
    validate.add_argument("--manifest", type=Path, required=True)
    validate.add_argument("--expected-manifest-sha256")
    validate.add_argument("--require-binding", action="append", default=[])
    validate.add_argument(
        "--forbid-manifest-sha256", action="append", default=[]
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point; prints only the validated manifest SHA-256."""

    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "create":
            created_at = (
                None
                if args.created_at_utc is None
                else datetime.fromisoformat(args.created_at_utc)
            )
            authority = create_downstream_code_freeze_v1(
                workspace_root=args.workspace_root,
                manifest_path=args.manifest,
                purpose=args.purpose,
                include_trees=args.include_tree,
                include_files=args.include_file,
                included_suffixes=args.suffix or (".py",),
                upstream_sha256=_binding_arguments(args.binding, "binding"),
                created_at_utc=created_at,
            )
        else:
            authority = load_downstream_code_freeze_v1(
                args.manifest,
                workspace_root=args.workspace_root,
                expected_manifest_sha256=args.expected_manifest_sha256,
                required_upstream_sha256=_binding_arguments(
                    args.require_binding, "require_binding"
                ),
                forbidden_manifest_sha256=args.forbid_manifest_sha256,
            )
    except (DownstreamCodeFreezeErrorV1, ValueError) as exc:
        parser.error(str(exc))
    print(authority.manifest_sha256)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
