#!/usr/bin/python3
"""Install an OCI-only, drive.file-scoped rclone token received on stdin."""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

CONFIG_PATH = Path("/etc/pump-fade-v3/rclone.conf")
CLIENT_ID_PATTERN = re.compile(r"^[0-9]+-[a-z0-9]+\.apps\.googleusercontent\.com$")
_TOKEN_VALUE_PATTERN = re.compile(
    r"([\"']?(?:access_token|refresh_token|client_secret)[\"']?\s*[:=]\s*)"
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
        isinstance(item, str) and item for item in (access_token, refresh_token, token_type)
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


def _validated_oauth_input(value: object) -> tuple[str, str, dict[str, Any]]:
    if not isinstance(value, dict):
        raise InstallError("token-invalid")
    client_id = value.get("client_id")
    client_secret = value.get("client_secret")
    if not isinstance(client_id, str) or not client_id:
        raise InstallError("token-invalid")
    if not isinstance(client_id, str) or not CLIENT_ID_PATTERN.fullmatch(client_id):
        raise InstallError("client-id-invalid")
    if (
        not isinstance(client_secret, str)
        or not client_secret.strip()
        or "\n" in client_secret
        or "\r" in client_secret
    ):
        raise InstallError("token-invalid")
    token = _validated_token(value.get("token"))
    return client_id, client_secret.strip(), token


def _config_bytes(
    client_id: str,
    client_secret: str,
    token: dict[str, Any],
) -> bytes:
    serialized = json.dumps(token, separators=(",", ":"), ensure_ascii=True)
    return (
        "[ocidrive]\n"
        "type = drive\n"
        f"client_id = {client_id}\n"
        f"client_secret = {client_secret}\n"
        "scope = drive.file\n"
        f"token = {serialized}\n"
    ).encode()


def write_config(
    oauth_input: object,
    path: Path,
    *,
    owner_uid: int | None = None,
    owner_gid: int | None = None,
) -> Path:
    client_id, client_secret, safe_token = _validated_oauth_input(oauth_input)
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
    if path.is_symlink():
        raise InstallError("exists")
    if path.exists():
        path_info = path.stat(follow_symlinks=False)
        if not stat.S_ISREG(path_info.st_mode) or path_info.st_nlink != 1:
            raise InstallError("exists")
        if (
            owner_uid is not None
            and owner_gid is not None
            and (
                path_info.st_uid != owner_uid
                or path_info.st_gid != owner_gid
                or path_info.st_mode & 0o777 != 0o600
            )
        ):
            raise InstallError("exists")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".rclone.conf.", dir=parent)
    temporary = Path(temporary_name)
    try:
        if owner_uid is not None and owner_gid is not None:
            os.chown(temporary, owner_uid, owner_gid)
        os.chmod(temporary, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(_config_bytes(client_id, client_secret, safe_token))
            stream.flush()
            os.fsync(stream.fileno())
        return temporary
    except OSError:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _redact_probe_stderr(stderr: bytes) -> str:
    text = stderr.decode("utf-8", errors="replace")
    last_lines = text.splitlines()[-3:]
    return "\n".join(_TOKEN_VALUE_PATTERN.sub(r"\1[REDACTED]", line) for line in last_lines)


def _probe_as_service_user(config_path: Path) -> tuple[bool, bytes]:
    environment = os.environ.copy()
    environment["HOME"] = "/nonexistent"
    environment["XDG_CACHE_HOME"] = "/nonexistent"
    command = [
        "runuser",
        "-u",
        "pumpfadev3",
        "--",
        "rclone",
        "--tpslimit",
        "2",
        "--config",
        str(config_path),
        "lsd",
        "ocidrive:",
    ]
    for attempt in range(3):
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                check=False,
                timeout=90,
                env=environment,
            )
        except (OSError, subprocess.TimeoutExpired):
            return False, b""
        if result.returncode == 0:
            return True, result.stderr
        stderr = result.stderr or b""
        lower_stderr = stderr.lower()
        rate_limited = (
            b"ratelimitexceeded" in lower_stderr
            or b"userratelimitexceeded" in lower_stderr
        )
        if not rate_limited or attempt == 2:
            return False, stderr
        time.sleep(2**attempt)
    return False, b""


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

    temporary: Path | None = None
    try:
        temporary = write_config(
            token,
            CONFIG_PATH,
            owner_uid=service_user.pw_uid,
            owner_gid=service_user.pw_gid,
        )
    except InstallError as error:
        return _fail(error.reason)
    except OSError:
        return _fail("write")

    if temporary is None:
        return _fail("write")
    probe_ok, probe_stderr = _probe_as_service_user(temporary)
    if not probe_ok:
        temporary.unlink(missing_ok=True)
        return _fail("probe", probe_stderr=probe_stderr)

    try:
        os.replace(temporary, CONFIG_PATH)
    except OSError:
        temporary.unlink(missing_ok=True)
        return _fail("write")

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
