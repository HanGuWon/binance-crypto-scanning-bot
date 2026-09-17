from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pytest

ROOT = str(Path(__file__).resolve().parents[2])
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from tools.oci_phase_p_live_audit import (  # noqa: E402
    PROBE_BODIES,
    PROBE_SHA256,
    RemoteCommand,
    _ssh_argv,
    process_command,
    run_remote,
    static_commands,
)


def test_probe_bodies_match_reviewed_hashes() -> None:
    assert {
        name: hashlib.sha256(body.encode()).hexdigest()
        for name, body in PROBE_BODIES.items()
    } == PROBE_SHA256


def test_static_templates_have_no_generic_or_mutating_escape() -> None:
    commands = static_commands()
    assert len({item.template_id for item in commands}) == len(commands)
    forbidden = {
        "start",
        "stop",
        "restart",
        "reload",
        "enable",
        "disable",
        "reset-failed",
        "tee",
        "touch",
        "mkdir",
        "cp",
        "mv",
        "rm",
        "install",
        "chmod",
        "chown",
        "truncate",
        "find",
        "sqlite3",
    }
    for command in commands:
        assert not forbidden.intersection(command.argv)
        remote = _ssh_argv(command)[-1]
        assert all(token not in remote for token in (" >", " >>", " | ", ";"))
        output_args = [arg for arg in command.argv if arg.startswith("--output")]
        if command.template_id == "journal":
            assert output_args == ["--output=short-iso-precise"]
        elif command.template_id == "filesystem_root":
            assert output_args == [
                "--output=source,fstype,size,used,avail,itotal,iused,iavail,"
                "pcent,target"
            ]
        else:
            assert output_args == []


def test_initial_health_template_has_no_remote_output_argument() -> None:
    command = next(
        item for item in static_commands() if item.template_id == "initial_health"
    )
    assert "--output" not in command.argv
    assert command.argv[-2:] == (
        "--service-unit",
        "binance-bot-2-prospective.service",
    )


def test_process_template_accepts_digits_only_positive_pid() -> None:
    assert process_command(123).argv[-1] == "123"
    with pytest.raises(ValueError):
        process_command(0)
    with pytest.raises(TypeError):
        process_command("1;touch /tmp/x")  # type: ignore[arg-type]


def test_altered_or_arbitrary_command_is_rejected_before_subprocess(monkeypatch) -> None:
    called = False

    def fail_run(*args, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal called
        called = True
        raise AssertionError("subprocess must not be reached")

    monkeypatch.setattr("subprocess.run", fail_run)
    with pytest.raises(ValueError, match="not an approved"):
        run_remote(RemoteCommand("journal", ("journalctl", "--vacuum-size=1")))
    with pytest.raises(ValueError, match="not an approved"):
        run_remote(RemoteCommand("arbitrary", ("python", "-c", "print(1)")))
    assert called is False


def test_altered_probe_body_is_rejected() -> None:
    approved = next(
        item
        for item in static_commands()
        if item.template_id == "freeze_and_identities"
    )
    altered = RemoteCommand(
        approved.template_id,
        (*approved.argv[:-1], approved.argv[-1] + "\nprint('changed')"),
        approved.probe_sha256,
    )
    with pytest.raises(ValueError, match="not an approved"):
        run_remote(altered)


def test_timeout_is_captured_as_unavailable(monkeypatch) -> None:
    command = next(
        item for item in static_commands() if item.template_id == "filesystem_root"
    )

    def timeout_run(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise __import__("subprocess").TimeoutExpired(args[0], 3)

    monkeypatch.setattr("subprocess.run", timeout_run)
    capture = run_remote(command, timeout=3)
    assert capture.exit_code == 124
    assert capture.stdout == ""
    assert capture.stderr.startswith("TIMEOUT after 3s")
