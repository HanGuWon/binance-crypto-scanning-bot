from __future__ import annotations

import importlib.util
import io
import json
import os
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
    temporary = INSTALLER.write_config(
        {
            "client_id": "123-abc.apps.googleusercontent.com",
            "client_secret": "test-client-secret",
            "token": {
                "access_token": "access-value",
                "refresh_token": "refresh-value",
                "token_type": "Bearer",
                "expiry": "2026-10-10T00:00:00Z",
                "untrusted_extra": "discard-me",
            },
        },
        path,
    )

    contents = temporary.read_text(encoding="utf-8")
    assert not path.exists()
    assert contents.startswith(
        "[ocidrive]\ntype = drive\nclient_id = 123-abc.apps.googleusercontent.com\n"
        "client_secret = test-client-secret\nscope = drive.file\n"
    )
    assert '"access_token":"access-value"' in contents
    assert "untrusted_extra" not in contents
    assert modes == [0o600]
    temporary.unlink()


def test_config_writer_stages_replacement_and_rejects_invalid_input(tmp_path: Path) -> None:
    path = tmp_path / "rclone.conf"
    path.write_text("existing", encoding="utf-8")
    temporary = INSTALLER.write_config(
        {
            "client_id": "123-abc.apps.googleusercontent.com",
            "client_secret": "test-client-secret",
            "token": {"access_token": "a", "refresh_token": "r", "token_type": "Bearer"},
        },
        path,
    )
    assert temporary.exists()
    assert path.read_text(encoding="utf-8") == "existing"
    temporary.unlink()
    with pytest.raises(INSTALLER.InstallError) as token_error:
        INSTALLER.write_config({"token": {"access_token": "a"}}, tmp_path / "other.conf")
    assert token_error.value.reason == "client-id-invalid"
    with pytest.raises(INSTALLER.InstallError) as client_error:
        INSTALLER.write_config(
            {
                "client_id": "not-a-google-client-id",
                "client_secret": "test-client-secret",
                "token": {"access_token": "a", "refresh_token": "r", "token_type": "Bearer"},
            },
            tmp_path / "invalid.conf",
        )
    assert client_error.value.reason == "client-id-invalid"
    assert path.read_text(encoding="utf-8") == "existing"


def test_config_writer_classifies_unsafe_directory(tmp_path: Path) -> None:
    with pytest.raises(INSTALLER.InstallError) as error:
        INSTALLER.write_config(
            {
                "client_id": "123-abc.apps.googleusercontent.com",
                "client_secret": "test-client-secret",
                "token": {"access_token": "a", "refresh_token": "r", "token_type": "Bearer"},
            },
            tmp_path / "rclone.conf",
            owner_uid=100,
            owner_gid=100,
        )

    assert error.value.reason == "dir-unsafe"


def test_probe_sets_nonexistent_home_and_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run(args: list[str], **kwargs: object) -> SimpleNamespace:
        captured["command"] = args
        captured.update(kwargs)
        return SimpleNamespace(returncode=0, stderr=b"")

    monkeypatch.setattr(INSTALLER.subprocess, "run", fake_run)

    assert INSTALLER._probe_as_service_user(tmp_path / ".rclone.conf.candidate") == (
        True,
        b"",
    )
    environment = captured["env"]
    assert isinstance(environment, dict)
    assert environment["HOME"] == "/nonexistent"
    assert environment["XDG_CACHE_HOME"] == "/nonexistent"
    assert isinstance(captured["command"], list)
    assert captured["command"][captured["command"].index("--tpslimit") + 1] == "2"


def test_probe_retries_rate_limit_with_exponential_delays(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    responses = iter(
        [
            SimpleNamespace(returncode=1, stderr=b"rateLimitExceeded"),
            SimpleNamespace(returncode=1, stderr=b"userRateLimitExceeded"),
            SimpleNamespace(returncode=0, stderr=b""),
        ]
    )
    attempts: list[list[str]] = []
    delays: list[int] = []

    def fake_run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        attempts.append(command)
        return next(responses)

    monkeypatch.setattr(INSTALLER.subprocess, "run", fake_run)
    monkeypatch.setattr(INSTALLER.time, "sleep", delays.append)

    assert INSTALLER._probe_as_service_user(tmp_path / ".candidate") == (True, b"")
    assert len(attempts) == 3
    assert delays == [1, 2]


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


@pytest.mark.parametrize("probe_ok", [False, True])
def test_installer_commits_only_after_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    probe_ok: bool,
) -> None:
    path = tmp_path / "rclone.conf"
    path.write_text("old-config-marker", encoding="utf-8")
    candidate = tmp_path / ".rclone.conf.candidate"
    monkeypatch.setattr(INSTALLER, "CONFIG_PATH", path)
    monkeypatch.setitem(
        sys.modules,
        "pwd",
        SimpleNamespace(getpwnam=lambda _: SimpleNamespace(pw_uid=123, pw_gid=456)),
    )
    monkeypatch.setattr(
        INSTALLER.sys,
        "stdin",
        io.StringIO(
            json.dumps(
                {
                    "client_id": "123-abc.apps.googleusercontent.com",
                    "client_secret": "test-client-secret",
                    "token": {
                        "access_token": "test-access-value",
                        "refresh_token": "test-refresh-value",
                        "token_type": "Bearer",
                    },
                }
            )
        ),
    )

    def stage_config(*args: object, **kwargs: object) -> Path:
        candidate.write_text("new-config-marker", encoding="utf-8")
        return candidate

    monkeypatch.setattr(INSTALLER, "write_config", stage_config)
    monkeypatch.setattr(
        INSTALLER,
        "_probe_as_service_user",
        lambda candidate_path: (probe_ok, b'client_secret="hidden-test-value"'),
    )

    result = INSTALLER.main()
    output = capsys.readouterr()
    assert "hidden-test-value" not in output.out + output.err
    if probe_ok:
        assert result == 0
        assert output.out == "OK\n"
        assert output.err == ""
        assert path.read_text(encoding="utf-8") == "new-config-marker"
    else:
        assert result == 1
        assert output.out == "FAIL\n"
        assert output.err.startswith("reason=probe\nclient_secret=[REDACTED]")
        assert path.read_text(encoding="utf-8") == "old-config-marker"
        assert not candidate.exists()


def test_powershell_auth_uses_expected_narrow_install_contract() -> None:
    script = POWERSHELL_PATH.read_text(encoding="utf-8")
    assert "https://www.googleapis.com/auth/drive.file" in script
    assert "client_id" in script and "client_secret" in script
    assert "install_pump_fade_v3_rclone_config" in script
    assert "rclone authorize" not in script
    assert "Start-Process -ArgumentList" not in script
    assert "client_secret = $clientSecret" in script
    assert "code_challenge_method=S256" in script
    assert "code_verifier = $codeVerifier" in script
    assert "$payload | & ssh" in script
    assert "enable pump-fade-v3-uploader.timer" not in script
