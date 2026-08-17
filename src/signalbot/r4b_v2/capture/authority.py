from __future__ import annotations

import hashlib
import os
import re
import stat
from dataclasses import asdict, dataclass
from pathlib import Path

from signalbot.capture.path_safety import inspect_link_free_path
from signalbot.r4b_v2.canonical import canonical_json_line

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_IDENTITY_LENGTH = 256
_BINDING_FILE = "storage-root-binding.json"


class StorageRootBindingError(RuntimeError):
    """Raised when a storage directory cannot prove its original authority."""


@dataclass(frozen=True, slots=True)
class StorageRootBindingV2:
    storage_kind: str
    root_role: str
    failure_domain_id: str
    authority_sha256: str
    contract_sha256: str
    schema_version: str = "r4b_v2_storage_root_binding_v1"


@dataclass(frozen=True, slots=True)
class StorageRootOpenedIdentityV2:
    canonical_path: str
    root_device: int
    root_inode: int
    binding_device: int
    binding_inode: int
    schema_version: str = "r4b_v2_storage_root_opened_identity_v1"

    def __post_init__(self) -> None:
        if self.schema_version != "r4b_v2_storage_root_opened_identity_v1":
            raise ValueError("unsupported storage-root opened identity schema")
        if self.canonical_path != os.path.normcase(os.path.abspath(self.canonical_path)):
            raise ValueError("storage-root opened path must be canonical and absolute")
        for field in (
            "root_device",
            "root_inode",
            "binding_device",
            "binding_inode",
        ):
            value = getattr(self, field)
            if type(value) is not int or value < 0:
                raise ValueError(f"{field} must be a nonnegative integer")


def assert_storage_root_binding_v2(
    directory: str | Path,
    expected: StorageRootBindingV2,
) -> None:
    """Re-read and exactly match the current canonical root-binding bytes."""

    if not isinstance(expected, StorageRootBindingV2):
        raise TypeError("expected must be a StorageRootBindingV2")
    binding_path = Path(directory) / _BINDING_FILE
    try:
        observed = binding_path.read_bytes()
    except FileNotFoundError as exc:
        raise StorageRootBindingError("storage root binding is missing") from exc
    except OSError as exc:
        raise StorageRootBindingError("storage root binding is unreadable") from exc
    if observed != canonical_json_line(asdict(expected)):
        raise StorageRootBindingError(
            "storage root binding differs from its expected current bytes"
        )


def inspect_storage_root_opened_identity_v2(
    directory: str | Path,
    expected: StorageRootBindingV2,
) -> StorageRootOpenedIdentityV2:
    """Capture one link-free root and binding-file pathname identity."""

    try:
        root_inspection = inspect_link_free_path(directory, "storage root")
    except ValueError as exc:
        raise StorageRootBindingError(str(exc)) from exc
    root_status = root_inspection.final_status
    if root_status is None or not stat.S_ISDIR(root_status.st_mode):
        raise StorageRootBindingError("storage root must be an existing directory")
    binding_path = root_inspection.absolute_path / _BINDING_FILE
    try:
        binding_inspection = inspect_link_free_path(
            binding_path,
            "storage root binding",
        )
    except ValueError as exc:
        if not binding_path.exists():
            raise StorageRootBindingError("storage root binding is missing") from exc
        raise StorageRootBindingError(str(exc)) from exc
    binding_status = binding_inspection.final_status
    if binding_status is None or not stat.S_ISREG(binding_status.st_mode):
        raise StorageRootBindingError(
            "storage root binding must be an existing regular file"
        )
    assert_storage_root_binding_v2(root_inspection.absolute_path, expected)
    try:
        root_after = inspect_link_free_path(directory, "storage root").final_status
        binding_after = inspect_link_free_path(
            binding_path,
            "storage root binding",
        ).final_status
    except ValueError as exc:
        raise StorageRootBindingError(str(exc)) from exc
    if (
        root_after is None
        or binding_after is None
        or (int(root_after.st_dev), int(root_after.st_ino))
        != (int(root_status.st_dev), int(root_status.st_ino))
        or (int(binding_after.st_dev), int(binding_after.st_ino))
        != (int(binding_status.st_dev), int(binding_status.st_ino))
    ):
        raise StorageRootBindingError(
            "storage root pathname identity changed during validation"
        )
    return StorageRootOpenedIdentityV2(
        canonical_path=os.path.normcase(
            os.path.abspath(os.fspath(root_inspection.absolute_path))
        ),
        root_device=int(root_status.st_dev),
        root_inode=int(root_status.st_ino),
        binding_device=int(binding_status.st_dev),
        binding_inode=int(binding_status.st_ino),
    )


def bind_storage_root_v2(
    directory: str | Path,
    *,
    storage_kind: str,
    root_role: str,
    failure_domain_id: str,
    authority_sha256: str,
    contract: dict[str, object],
) -> StorageRootBindingV2:
    """Create once or exactly verify an immutable storage-root authority binding."""

    root = Path(directory)
    _validate_identity(storage_kind, "storage_kind")
    _validate_identity(root_role, "root_role")
    _validate_identity(failure_domain_id, "failure_domain_id")
    if _SHA256_RE.fullmatch(authority_sha256) is None:
        raise ValueError("authority_sha256 must be a lowercase SHA-256 digest")
    contract_bytes = canonical_json_line(contract)
    expected = StorageRootBindingV2(
        storage_kind=storage_kind,
        root_role=root_role,
        failure_domain_id=failure_domain_id,
        authority_sha256=authority_sha256,
        contract_sha256=hashlib.sha256(contract_bytes).hexdigest(),
    )
    expected_bytes = canonical_json_line(asdict(expected))
    binding_path = root / _BINDING_FILE
    if binding_path.exists():
        try:
            observed = binding_path.read_bytes()
        except OSError as exc:
            raise StorageRootBindingError("storage root binding is unreadable") from exc
        if observed != expected_bytes:
            raise StorageRootBindingError(
                "storage root binding differs from the requested authority contract"
            )
        return expected

    residues = tuple(root.iterdir())
    if residues:
        raise StorageRootBindingError(
            "non-empty storage root without its immutable authority binding"
        )
    try:
        with binding_path.open("xb", buffering=0) as handle:
            written = handle.write(expected_bytes)
            if written != len(expected_bytes):
                raise StorageRootBindingError("storage root binding short write")
            os.fsync(handle.fileno())
        _fsync_parent(binding_path)
    except FileExistsError:
        raise StorageRootBindingError("storage root binding raced with another writer") from None
    except OSError as exc:
        raise StorageRootBindingError("storage root binding could not be made durable") from exc
    return expected


def _validate_identity(value: str, field: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > _MAX_IDENTITY_LENGTH
        or any(character in value for character in "\r\n\x00")
    ):
        raise ValueError(f"{field} must be a bounded normalized identity")


def _fsync_parent(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
