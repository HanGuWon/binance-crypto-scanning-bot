"""Install an OCI-only, drive.file-scoped rclone token received on stdin."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

CONFIG_PATH = Path("/etc/pump-fade-v3/rclone.conf")


class InstallError(RuntimeError):
    """Raised when the narrow OAuth config cannot be installed safely."""


def _validated_token(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise InstallError("invalid rclone token")
    access_token = value.get("access_token")
    refresh_token = value.get("refresh_token")
    token_type = value.get("token_type")
    if not all(
        isinstance(item, str) and item
        for item in (access_token, refresh_token, token_type)
    ):
        raise InstallError("invalid rclone token")
    safe: dict[str, Any] = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": token_type,
    }
    expiry = value.get("expiry")
    if isinstance(expiry, str):
        safe["expiry"] = expiry
    return safe


def _config_bytes(token: dict[str, Any]) -> bytes:
    serialized = json.dumps(token, separators=(",", ":"), ensure_ascii=True)
    return f"[ocidrive]\ntype = drive\nscope = drive.file\ntoken = {serialized}\n".encode()


def write_config(
    token: object,
    path: Path,
    *,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> None:
    safe_token = _validated_token(token)
    parent = path.parent
    if parent.is_symlink():
        raise InstallError("unsafe config directory")
    if not parent.exists():
        parent.mkdir(mode=0o750, parents=False)
        if owner_gid is not None:
            os.chown(parent, 0, owner_gid)
        parent.chmod(0o750)
    elif owner_uid is not None and owner_gid is not None:
        parent_info = parent.stat(follow_symlinks=False)
        if (
            not parent.is_dir()
            or parent_info.st_uid != 0
            or parent_info.st_gid != owner_gid
            or parent_info.st_mode & 0o777 != 0o750
        ):
            raise InstallError("existing config directory has unsafe ownership or mode")
    if path.exists() or path.is_symlink():
        raise InstallError("rclone config already exists")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".rclone.conf.", dir=parent)
    temporary = Path(temporary_name)
    try:
        if owner_uid is not None and owner_gid is not None:
            os.chown(temporary, owner_uid, owner_gid)
        os.chmod(temporary, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(_config_bytes(safe_token))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except OSError:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _probe_as_service_user() -> bool:
    try:
        result = subprocess.run(
            [
                "runuser",
                "-u",
                "pumpfadev3",
                "--",
                "rclone",
                "--config",
                str(CONFIG_PATH),
                "lsd",
                "ocidrive:",
            ],
            capture_output=True,
            check=False,
            timeout=90,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def main() -> int:
    try:
        import pwd

        token = json.load(sys.stdin)
        service_user = pwd.getpwnam("pumpfadev3")
        write_config(
            token,
            CONFIG_PATH,
            owner_uid=service_user.pw_uid,
            owner_gid=service_user.pw_gid,
        )
        if not _probe_as_service_user():
            CONFIG_PATH.unlink(missing_ok=True)
            print("FAIL")
            return 1
    except (OSError, ValueError, InstallError):
        print("FAIL")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
