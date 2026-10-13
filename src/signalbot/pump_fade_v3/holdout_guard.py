"""Explicit, independently pinned L1 authorization for one-shot sealed evaluation.

No sealed path is accepted or examined by ``validate_unseal_token``.
The external seal and trusted pin are outside the sealed dataset directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SHA = re.compile(r"[0-9a-f]{40}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_FILES = (
    "config/pump_fade_v3/freeze_manifest_v3.json",
    "config/pump_fade_v3/policy_v3.json",
    "docs/pump_fade_v3/PREREGISTRATION_V3.md",
    "src/signalbot/pump_fade_v3/holdout_guard.py",
)


class HoldoutAuthorizationError(RuntimeError):
    """Holdout must remain sealed because its authority is unverified."""


@dataclass(frozen=True, slots=True)
class TrustedFreeze:
    """Independent operator-pinned values, not loaded from the external seal."""

    commit_sha: str
    file_sha256: dict[str, str]


class AuthorizedHoldout:
    """Capability minted only by successful validation; constructor is private."""

    __slots__ = ("_key", "commit_sha")
    _factory_key = object()

    def __init__(self, key: object, commit_sha: str) -> None:
        if key is not self._factory_key:
            raise HoldoutAuthorizationError("capability cannot be forged")
        self._key = key
        self.commit_sha = commit_sha


class ClaimedHoldout:
    """A one-shot claim acquired atomically outside the sealed directory."""

    __slots__ = ("_key", "commit_sha")

    def __init__(self, key: object, commit_sha: str) -> None:
        if key is not AuthorizedHoldout._factory_key:
            raise HoldoutAuthorizationError("claimed capability cannot be forged")
        self._key = key
        self.commit_sha = commit_sha


def add_holdout_unseal_argument(parser: argparse.ArgumentParser) -> None:
    """Register the mandatory L5 unseal token without accepting a data path."""

    parser.add_argument(
        "--holdout-unseal",
        required=True,
        dest="holdout_unseal",
        metavar="L1_FREEZE_COMMIT_SHA",
        help="exact lowercase SHA-1 of the committed L1 freeze",
    )


def _git(repo: Path, *args: str) -> bytes:
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, check=False,
        timeout=15,
    )
    if result.returncode:
        raise HoldoutAuthorizationError("immutable L1 Git evidence unavailable")
    return result.stdout


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise HoldoutAuthorizationError("invalid evidence object")
    return value


def validate_unseal_token(
    token: str | None, *, repo_root: Path, seal_file: Path,
    trusted: TrustedFreeze,
) -> AuthorizedHoldout:
    """Validate token, Git blobs and externally pinned seal before sealed IO.

    The API deliberately has no holdout-directory parameter. ``trusted`` must
    originate from independent operator-controlled configuration, not the seal.
    """
    if not isinstance(token, str) or _SHA.fullmatch(token) is None:
        raise HoldoutAuthorizationError("missing or malformed L1 freeze SHA")
    if _SHA.fullmatch(trusted.commit_sha) is None or token != trusted.commit_sha:
        raise HoldoutAuthorizationError("wrong or stale L1 freeze SHA")
    if set(trusted.file_sha256) != set(_FILES) or any(
        not isinstance(value, str) or _DIGEST.fullmatch(value) is None
        for value in trusted.file_sha256.values()
    ):
        raise HoldoutAuthorizationError("incomplete independent frozen file pins")
    if _git(repo_root, "cat-file", "-t", token).strip() != b"commit":
        raise HoldoutAuthorizationError("freeze identity is not a commit")
    _git(repo_root, "merge-base", "--is-ancestor", token, "HEAD")
    frozen: dict[str, bytes] = {}
    for path in _FILES:
        data = _git(repo_root, "show", f"{token}:{path}")
        if _sha(data) != trusted.file_sha256[path]:
            raise HoldoutAuthorizationError("frozen Git blob differs from independent pin")
        frozen[path] = data
    manifest = _object(json.loads(frozen[_FILES[0]]))
    if "freeze_commit_sha" in manifest or "commit_sha" in manifest:
        raise HoldoutAuthorizationError("self-referential freeze commit field forbidden")
    bindings = {
        "policy_sha256": trusted.file_sha256[_FILES[1]],
        "preregistration_sha256": trusted.file_sha256[_FILES[2]],
        "guard_sha256": trusted.file_sha256[_FILES[3]],
    }
    if any(manifest.get(key) != digest for key, digest in bindings.items()):
        raise HoldoutAuthorizationError("freeze manifest dependency hashes mismatch")
    # External trusted receipt is a separate path; the caller must never place
    # it under the sealed root. Seal is only read after all Git checks.
    seal = _object(json.loads(seal_file.read_bytes()))
    if seal.get("schema_version") != "pump_fade_v3_holdout_seal_v1":
        raise HoldoutAuthorizationError("unsupported seal schema")
    if seal.get("freeze_commit_sha") != token or seal.get(
        "freeze_manifest_sha256"
    ) != trusted.file_sha256[_FILES[0]]:
        raise HoldoutAuthorizationError("seal does not bind independent freeze")
    if any(seal.get(key) != digest for key, digest in bindings.items()):
        raise HoldoutAuthorizationError("seal dependency hashes mismatch")
    if seal.get("holdout_spec") != {
        "start_utc": "2026-07-01T00:00:00Z",
        "end_utc": "2026-10-06T23:59:59Z",
        "universe": "binance_usdm_usdt_perpetual",
        "include_delisted": True,
    }:
        raise HoldoutAuthorizationError("holdout specification mismatch")
    return AuthorizedHoldout(AuthorizedHoldout._factory_key, token)


def claim_one_shot(
    authorization: AuthorizedHoldout, *, one_shot_receipt: Path,
) -> ClaimedHoldout:
    """Atomically claim evaluation before any sealed directory is supplied.

    Use a dedicated, trusted non-sealed receipt directory. Never retry an
    attempted evaluation after the exclusive receipt has been created.
    """
    if (type(authorization) is not AuthorizedHoldout
            or authorization._key is not AuthorizedHoldout._factory_key):
        raise HoldoutAuthorizationError("invalid capability")
    marker = json.dumps({"freeze_commit_sha": authorization.commit_sha,
                         "status": "EVALUATION_CLAIMED"}).encode()
    try:
        fd = os.open(one_shot_receipt, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise HoldoutAuthorizationError("one-shot evaluation already claimed") from exc
    with os.fdopen(fd, "wb") as stream:
        stream.write(marker)
        stream.flush()
        os.fsync(stream.fileno())
    return ClaimedHoldout(AuthorizedHoldout._factory_key, authorization.commit_sha)


def _safe_relative(name: str) -> list[str]:
    if (not isinstance(name, str) or not name or name.startswith(("/", "\\"))
            or "\\" in name or ":" in name
            or any(part in ("", ".", "..") for part in name.split("/"))):
        raise HoldoutAuthorizationError("unsafe relative sealed filename")
    return name.split("/")


def read_sealed_file(
    authorization: ClaimedHoldout, *, sealed_root: Path,
    relative_path: str, expected_sha256: str,
) -> bytes:
    """Read one acquired file by an independently supplied expected digest."""
    if (type(authorization) is not ClaimedHoldout
            or authorization._key is not AuthorizedHoldout._factory_key):
        raise HoldoutAuthorizationError("one-shot claim required")
    parts = _safe_relative(relative_path)
    if not isinstance(expected_sha256, str) or _DIGEST.fullmatch(expected_sha256) is None:
        raise HoldoutAuthorizationError("invalid expected digest")
    if os.name == "nt":
        # Check every existing path component for Windows junction/reparse aliases.
        # The archive must be exclusively controlled to prevent a replacement race.
        from signalbot.capture.path_safety import inspect_link_free_path

        target = sealed_root.joinpath(*parts)
        inspected = inspect_link_free_path(target, "sealed holdout")
        if inspected.final_status is None:
            raise HoldoutAuthorizationError("sealed file missing")
        info = target.stat(follow_symlinks=False)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise HoldoutAuthorizationError("unsafe sealed file type")
        fd = os.open(target, os.O_RDONLY | os.O_BINARY)
        try:
            opened = os.fstat(fd)
            if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
                raise HoldoutAuthorizationError("unsafe opened sealed file")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                payload = stream.read()
        finally:
            os.close(fd)
        if _sha(payload) != expected_sha256:
            raise HoldoutAuthorizationError("sealed file digest mismatch")
        return payload
    dirflags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    root_fd = os.open(sealed_root, dirflags)
    try:
        parent_fd = os.dup(root_fd)
        try:
            for directory in parts[:-1]:
                next_fd = os.open(directory, dirflags, dir_fd=parent_fd)
                os.close(parent_fd)
                parent_fd = next_fd
            data_fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                              dir_fd=parent_fd)
            try:
                info = os.fstat(data_fd)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise HoldoutAuthorizationError("unsafe sealed file type")
                with os.fdopen(data_fd, "rb", closefd=False) as stream:
                    payload = stream.read()
            finally:
                os.close(data_fd)
        finally:
            os.close(parent_fd)
    finally:
        os.close(root_fd)
    if _sha(payload) != expected_sha256:
        raise HoldoutAuthorizationError("sealed file digest mismatch")
    return payload
