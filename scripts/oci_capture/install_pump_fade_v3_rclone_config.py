"""Install an OCI-only, drive.file-scoped rclone token received on stdin."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

CONFIG_PATH = Path("/etc/pump-fade-v3/rclone.conf")
_TOKEN_VALUE_PATTERN = re.compile(
    r"([\"']?(?:access_token|refresh_token)[\"']?\s*[:=]\s*)"
    r'("[^"\r\n]*"|\'[^\'\r\n]*\'|[^,\s}\]]+)',
    re.IGNORECASE,
)


class InstallError(RuntimeError):
    """Raised when the narrow OAuth config cannot be installed safely."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _validated_token(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise InstallError("token-invalid")
    access_token = value.get("access_token")
    refresh_token = value.get("refresh_token")
    token_type = value.get("token_type")
    if not all(
        isinstance(item, str) and item
        for item in (access_token, refresh_token, token_type)
    ):
        raise InstallError("token-invalid")
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
        raise InstallError("dir-unsafe")
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
            raise InstallError("dir-unsafe")
    if path.exists() or path.is_symlink():
        raise InstallError("exists")
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


def _redact_probe_stderr(stderr: bytes) -> str:
    text = stderr.decode("utf-8", errors="replace")
    last_lines = text.splitlines()[-3:]
    return "\n".join(
        _TOKEN_VALUE_PATTERN.sub(r"\1[REDACTED]", line) for line in last_lines
    )


def _probe_as_service_user() -> tuple[bool, bytes]:
    environment = os.environ.copy()
    environment["HOME"] = "/nonexistent"
    environment["XDG_CACHE_HOME"] = "/nonexistent"
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
            env=environment,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False, b""
    return result.returncode == 0, result.stderr


def _fail(reason: str, *, probe_stderr: bytes = b"") -> int:
    print(f"reason={reason}", file=sys.stderr)
    if probe_stderr:
        redacted = _redact_probe_stderr(probe_stderr)
        if redacted:
            print(redacted, file=sys.stderr)
    print("FAIL")
    return 1


def main() -> int:
    try:
        import pwd

        token = json.load(sys.stdin)
    except (ImportError, OSError, ValueError):
        return _fail("token-invalid")

    try:
        service_user = pwd.getpwnam("pumpfadev3")
    except (KeyError, OSError):
        return _fail("write")

    try:
        write_config(
            token,
            CONFIG_PATH,
            owner_uid=service_user.pw_uid,
            owner_gid=service_user.pw_gid,
        )
    except InstallError as error:
        return _fail(error.reason)
    except OSError:
        return _fail("write")

    probe_ok, probe_stderr = _probe_as_service_user()
    if not probe_ok:
        CONFIG_PATH.unlink(missing_ok=True)
        return _fail("probe", probe_stderr=probe_stderr)

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
