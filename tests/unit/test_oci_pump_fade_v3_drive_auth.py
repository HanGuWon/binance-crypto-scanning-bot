from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
INSTALLER_PATH = ROOT / "scripts" / "oci_capture" / "install_pump_fade_v3_rclone_config.py"
POWERSHELL_PATH = ROOT / "scripts" / "oci_capture" / "authorize_drive_for_oci.ps1"
SPEC = importlib.util.spec_from_file_location("drive_config_installer", INSTALLER_PATH)
assert SPEC is not None and SPEC.loader is not None
INSTALLER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = INSTALLER
SPEC.loader.exec_module(INSTALLER)


def test_config_writer_scopes_remote_and_sets_restrictive_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "rclone.conf"
    modes: list[int] = []
    real_chmod = os.chmod

    def record_mode(path: Path, mode: int) -> None:
        modes.append(mode)
        real_chmod(path, mode)

    monkeypatch.setattr(INSTALLER.os, "chmod", record_mode)
    INSTALLER.write_config(
        {
            "access_token": "access-secret",
            "refresh_token": "refresh-secret",
            "token_type": "Bearer",
            "expiry": "2026-10-10T00:00:00Z",
            "untrusted_extra": "discard-me",
        },
        path,
    )

    contents = path.read_text(encoding="utf-8")
    assert contents.startswith("[ocidrive]\ntype = drive\nscope = drive.file\n")
    assert '"access_token":"access-secret"' in contents
    assert "untrusted_extra" not in contents
    assert modes == [0o600]


def test_config_writer_refuses_overwrite_and_invalid_token(tmp_path: Path) -> None:
    path = tmp_path / "rclone.conf"
    path.write_text("existing", encoding="utf-8")
    with pytest.raises(INSTALLER.InstallError) as existing_error:
        INSTALLER.write_config(
            {"access_token": "a", "refresh_token": "r", "token_type": "Bearer"}, path
        )
    assert existing_error.value.reason == "exists"
    with pytest.raises(INSTALLER.InstallError) as token_error:
        INSTALLER.write_config({"access_token": "a"}, tmp_path / "other.conf")
    assert token_error.value.reason == "token-invalid"
    assert path.read_text(encoding="utf-8") == "existing"


def test_config_writer_classifies_unsafe_directory(tmp_path: Path) -> None:
    with pytest.raises(INSTALLER.InstallError) as error:
        INSTALLER.write_config(
            {"access_token": "a", "refresh_token": "r", "token_type": "Bearer"},
            tmp_path / "rclone.conf",
            owner_uid=100,
            owner_gid=100,
        )

    assert error.value.reason == "dir-unsafe"


def test_probe_sets_nonexistent_home_and_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run(*args: object, **kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(returncode=0, stderr=b"")

    monkeypatch.setattr(INSTALLER.subprocess, "run", fake_run)

    assert INSTALLER._probe_as_service_user() == (True, b"")
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert environment["HOME"] == "/nonexistent"
    assert environment["XDG_CACHE_HOME"] == "/nonexistent"


def test_probe_failure_reports_only_redacted_last_three_lines(
    capsys: pytest.CaptureFixture[str],
) -> None:
    stderr = (
        b"discard this line\n"
        b'{"access_token":"access-value","refresh_token":"refresh-value"}\n'
        b"access_token = another-access-value\n"
        b"probe ended\n"
    )

    assert INSTALLER._fail("probe", probe_stderr=stderr) == 1
    output = capsys.readouterr()
    assert output.out == "FAIL\n"
    assert output.err.startswith("reason=probe\n")
    assert "discard this line" not in output.err
    assert "access-value" not in output.err
    assert "refresh-value" not in output.err
    assert "another-access-value" not in output.err
    assert output.err.count("[REDACTED]") == 3
    assert "probe ended" in output.err


def _write_auth_stubs(directory: Path) -> tuple[Path, dict[str, str]]:
    directory.mkdir()
    response_path = directory / "rclone-response.json"
    response_path.write_text(
        '{"access_token":"access-secret","refresh_token":"refresh-secret",'
        '"token_type":"Bearer","expiry":"2026-10-10T00:00:00Z"}',
        encoding="utf-8",
    )
    rclone_args = directory / "rclone-args.txt"
    ssh_args = directory / "ssh-args.txt"
    ssh_input = directory / "ssh-stdin.json"
    ssh_stub = directory / "ssh_stub.py"
    ssh_stub.write_text(
        "import os, pathlib, sys\n"
        "args = ' '.join(sys.argv[1:])\n"
        "with open(os.environ['PFV3_TEST_SSH_ARGS'], 'a', encoding='utf-8') as f:\n"
        "    f.write(args + '\\n')\n"
        "if 'echo CONNECTED;' in args:\n"
        "    print('CONNECTED\\nREADY')\n"
        "    raise SystemExit(0)\n"
        "if 'install_pump_fade_v3_rclone_config' in args:\n"
        "    pathlib.Path(os.environ['PFV3_TEST_SSH_INPUT']).write_bytes(sys.stdin.buffer.read())\n"
        "    result = os.environ.get('PFV3_TEST_SSH_RESULT', 'OK')\n"
        "    print(result)\n"
        "    raise SystemExit(0 if result == 'OK' else 1)\n",
        encoding="utf-8",
    )
    (directory / "rclone.cmd").write_text(
        '@echo off\r\n'
        '> "%PFV3_TEST_RCLONE_ARGS%" echo %*\r\n'
        'type "%PFV3_TEST_RCLONE_OUTPUT%"\r\n'
        'exit /b 0\r\n',
        encoding="ascii",
    )
    (directory / "ssh.cmd").write_text(
        '@echo off\r\n'
        '"%PFV3_TEST_PYTHON%" "%PFV3_TEST_SSH_STUB%" %*\r\n'
        'exit /b %ERRORLEVEL%\r\n',
        encoding="ascii",
    )
    environment = {
        "PFV3_TEST_RCLONE_OUTPUT": str(response_path),
        "PFV3_TEST_RCLONE_ARGS": str(rclone_args),
        "PFV3_TEST_SSH_ARGS": str(ssh_args),
        "PFV3_TEST_SSH_INPUT": str(ssh_input),
        "PFV3_TEST_SSH_STUB": str(ssh_stub),
        "PFV3_TEST_PYTHON": sys.executable,
        "PATH": str(directory) + os.pathsep + os.environ["PATH"],
    }
    return directory, environment


@pytest.mark.skipif(
    os.name != "nt" or shutil.which("powershell.exe") is None,
    reason="Windows PowerShell required",
)
@pytest.mark.parametrize("remote_result", ["OK", "FAIL"])
def test_powershell_authorization_uses_ssh_stdin_and_never_prints_token(
    tmp_path: Path, remote_result: str
) -> None:
    stub_dir, extra_environment = _write_auth_stubs(tmp_path / "stubs")
    environment = os.environ.copy()
    environment.update(extra_environment)
    environment["PFV3_TEST_SSH_RESULT"] = remote_result
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(POWERSHELL_PATH),
            "-SshHost",
            "trendguard-osaka-a1",
        ],
        capture_output=True,
        check=False,
        text=True,
        encoding="utf-8",
        env=environment,
        timeout=30,
    )

    assert result.returncode == (0 if remote_result == "OK" else 1)
    if remote_result == "OK":
        assert "OCI Drive 인증 완료" in result.stdout
    else:
        assert "FAIL [remote-install]" in result.stdout
        assert "exit 1: FAIL" in result.stdout
    assert "access-secret" not in result.stdout + result.stderr
    assert "refresh-secret" not in result.stdout + result.stderr
    ssh_args = (stub_dir / "ssh-args.txt").read_text(encoding="utf-8")
    assert "access-secret" not in ssh_args and "refresh-secret" not in ssh_args
    assert "install_pump_fade_v3_rclone_config" in ssh_args
    rclone_args = (stub_dir / "rclone-args.txt").read_text(encoding="utf-8")
    assert "authorize drive" in rclone_args
    assert "access-secret" not in rclone_args and "refresh-secret" not in rclone_args
    payload = json.loads((stub_dir / "ssh-stdin.json").read_text(encoding="utf-8"))
    assert payload["access_token"] == "access-secret"
    assert payload["refresh_token"] == "refresh-secret"

