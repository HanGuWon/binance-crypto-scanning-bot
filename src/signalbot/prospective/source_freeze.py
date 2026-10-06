"""Deterministic source-freeze identity for prospective campaigns.

This module fingerprints only source/configuration inputs that can affect the
runtime or scientific policy.  It deliberately rejects symlinks and Windows
reparse points: a campaign must never silently hash content outside its source
root.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

SOURCE_FREEZE_SCHEMA_VERSION = "worktree-source-v1"
REQUIRED_ROOT_FILES = ("pyproject.toml", "uv.lock")


@dataclass(frozen=True, slots=True)
class FrozenSourceFile:
    path: str
    sha256: str


@dataclass(frozen=True, slots=True)
class SourceFreeze:
    schema_version: str
    git_head: str | None
    files: tuple[FrozenSourceFile, ...]
    source_root_sha256: str

    @property
    def source_identity(self) -> str:
        return f"{self.schema_version}:{self.source_root_sha256}"

    def as_dict(self) -> dict[str, object]:
        return {
            "source_freeze_schema_version": self.schema_version,
            "git_head": self.git_head,
            "files": [
                {"path": item.path, "sha256": item.sha256} for item in self.files
            ],
            "source_root_sha256": self.source_root_sha256,
        }


def _is_reparse_or_symlink(path: Path) -> bool:
    if path.is_symlink():
        return True
    try:
        attributes = os.stat(path, follow_symlinks=False).st_file_attributes  # pyright: ignore[reportAttributeAccessIssue]
    except (AttributeError, OSError):
        return False
    return bool(attributes & getattr(os, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _regular_file(path: Path, root: Path) -> None:
    if _is_reparse_or_symlink(path):
        raise ValueError(f"source freeze rejects symlink/reparse path: {path}")
    if not path.is_file():
        raise FileNotFoundError(f"required source file is not a regular file: {path}")
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"source path escapes root: {path}") from exc
    parent = path.parent
    while True:
        if _is_reparse_or_symlink(parent):
            raise ValueError(f"source freeze rejects symlink/reparse parent: {parent}")
        if parent == root:
            break
        try:
            parent.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"source path parent escapes root: {parent}") from exc
        parent = parent.parent


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _git_head(root: Path) -> str | None:
    head = root / ".git" / "HEAD"
    if _is_reparse_or_symlink(head) or not head.is_file():
        return None
    value = head.read_text(encoding="utf-8").strip()
    if value.startswith("ref: "):
        ref_path = root / ".git" / value[5:]
        if ref_path.is_file() and not _is_reparse_or_symlink(ref_path):
            return ref_path.read_text(encoding="utf-8").strip() or None
    return value or None


def default_source_root() -> Path:
    """Return the source checkout root that owns this installed module.

    Prospective ``worktree-source-v1`` campaigns are intentionally tied to a
    source checkout containing ``pyproject.toml``, ``uv.lock`` and
    ``src/signalbot``. A wheel/site-packages installation without those files
    is therefore not silently accepted as the frozen worktree.
    """

    return Path(__file__).resolve().parents[3]


def freeze_source(root: str | Path) -> SourceFreeze:
    """Hash the exact runtime/scientific source set in deterministic order."""

    requested_root = Path(root).absolute()
    if _is_reparse_or_symlink(requested_root):
        raise ValueError(
            f"source freeze rejects symlink/reparse root: {requested_root}"
        )
    parent = requested_root.parent
    while True:
        if _is_reparse_or_symlink(parent):
            raise ValueError(f"source freeze rejects symlink/reparse parent: {parent}")
        if parent.parent == parent:
            break
        parent = parent.parent
    source_root = requested_root.resolve()
    files: list[Path] = []
    for relative in REQUIRED_ROOT_FILES:
        files.append(source_root / relative)
    source_dir = source_root / "src" / "signalbot"
    if not source_dir.is_dir() or _is_reparse_or_symlink(source_dir):
        raise FileNotFoundError("required runtime source directory is missing or unsafe")
    files.extend(sorted(source_dir.rglob("*.py"), key=lambda item: item.as_posix()))
    if not any(path.name == "__init__.py" for path in files):
        raise FileNotFoundError("required signalbot runtime source is missing")

    entries: list[FrozenSourceFile] = []
    for path in files:
        _regular_file(path, source_root)
        relative = path.relative_to(source_root).as_posix()
        entries.append(FrozenSourceFile(relative, _sha256_file(path)))
    entries.sort(key=lambda item: item.path)
    canonical = json.dumps(
        [{"path": item.path, "sha256": item.sha256} for item in entries],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return SourceFreeze(
        schema_version=SOURCE_FREEZE_SCHEMA_VERSION,
        git_head=_git_head(source_root),
        files=tuple(entries),
        source_root_sha256=hashlib.sha256(canonical).hexdigest(),
    )
