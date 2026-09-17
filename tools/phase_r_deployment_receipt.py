# ruff: noqa: E501
"""Capture a bounded, outcome-blind Phase-R deployment receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HOST = os.environ.get("BINANCE_BOT_OCI_HOST", "ubuntu@oci-host.example")
SSH_KEY = Path(
    os.environ.get(
        "BINANCE_BOT_OCI_SSH_KEY",
        str(Path.home() / ".ssh" / "binance-bot-oci"),
    )
)
REMOTE_PYTHON = "/opt/binance-bot-2/app/.venv/bin/python"
RELEASE = "/opt/binance-bot-2/releases/phase-r-oci-20260902-v1"
CURRENT = "/opt/binance-bot-2/current"
CAMPAIGN_ID = "causal-retest-prospective-phase-r-oci-20260902-v1"
CAMPAIGN_ROOT = f"/var/lib/binance-bot-2/prospective-r/{CAMPAIGN_ID}"
NEW_CONFIG = "/etc/binance-bot-2/prospective-phase-r.yaml"
NEW_MOUNT = "/var/lib/binance-bot-2/prospective-r"
NEW_UUID = os.environ.get("BINANCE_BOT_OCI_FILESYSTEM_UUID", "")
ACTIVATION_MS = 1788282532601
EXPECTED_SOURCE = (
    "worktree-source-v1:"
    "8f3ab5520769d71b054d489356c2fd774d31500b0deee5c3f65bc997e1c9d6e8"
)
EXPECTED_CONFIG_HASH = "cfc0bb96c41f6005d269daf7c6d44cbb850fd716d75c4180b5f3737d045ae78b"
EXPECTED_MANIFEST_HASH = "eb79b8515bad585ad7a1f89fb26983f920c09ef424ca6e870500659da99e1078"
EXPECTED_BUNDLE_HASH = "0a41b707cf63b53472ca7dcfe1fd7954c80b4e2b3855ad0e549dca7df0007353"
EXPECTED_QUOTA_BYTES = 128_849_018_880


@dataclass(frozen=True)
class Capture:
    """One bounded command result with content digests instead of raw output."""

    argv: tuple[str, ...]
    captured_at_utc: str
    elapsed_ms: int
    exit_code: int
    stdout: str
    stderr: str

    def record(self) -> dict[str, Any]:
        return {
            "argv": list(self.argv),
            "captured_at_utc": self.captured_at_utc,
            "elapsed_ms": self.elapsed_ms,
            "exit_code": self.exit_code,
            "stdout_bytes": len(self.stdout.encode()),
            "stdout_sha256": hashlib.sha256(self.stdout.encode()).hexdigest(),
            "stderr_bytes": len(self.stderr.encode()),
            "stderr_sha256": hashlib.sha256(self.stderr.encode()).hexdigest(),
        }


def _run(command: list[str], *, input_text: str | None = None) -> Capture:
    started = datetime.now(tz=UTC)
    before = time.monotonic()
    try:
        result = subprocess.run(
            command,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        exit_code = result.returncode
        stdout = result.stdout
        stderr = result.stderr
    except subprocess.TimeoutExpired as exc:
        exit_code = 124
        stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        stderr_value = (
            exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        )
        stderr = f"TIMEOUT after 90s\n{stderr_value}"
    return Capture(
        argv=tuple(command),
        captured_at_utc=started.isoformat(),
        elapsed_ms=round((time.monotonic() - before) * 1000),
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
    )


def _ssh(remote_argv: tuple[str, ...]) -> list[str]:
    return [
        "ssh",
        "-i",
        str(SSH_KEY),
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=15",
        HOST,
        shlex.join(remote_argv),
    ]


def _remote(remote_argv: tuple[str, ...]) -> Capture:
    return _run(_ssh(remote_argv))


def _remote_python(script: str) -> Capture:
    return _run(
        _ssh(
            (
                "sudo",
                "-n",
                "-u",
                "binance-bot-2",
                "env",
                "PYTHONDONTWRITEBYTECODE=1",
                f"PYTHONPATH={CURRENT}:{CURRENT}/src:{CURRENT}/tools",
                REMOTE_PYTHON,
                "-",
            )
        ),
        input_text=script,
    )


def _props(stdout: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            result[key] = value
    return result


def _json_line(stdout: str) -> dict[str, Any]:
    for line in reversed(stdout.splitlines()):
        if line.strip().startswith("{"):
            value = json.loads(line)
            if isinstance(value, dict):
                return value
    raise RuntimeError("remote probe did not return a JSON object")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def collect() -> dict[str, Any]:
    if not NEW_UUID:
        raise RuntimeError("BINANCE_BOT_OCI_FILESYSTEM_UUID is required")
    commands: dict[str, Capture] = {}
    fixed: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("current_target", ("readlink", "-f", CURRENT)),
        (
            "release_stat",
            ("sudo", "-n", "stat", "-c", "%n|%F|%U|%G|%a", RELEASE),
        ),
        ("remote_bundle_sha256", ("sudo", "-n", "sha256sum", "/tmp/phase-r-source-bundle-v2.tar.gz")),
        ("remote_config_sha256", ("sudo", "-n", "sha256sum", NEW_CONFIG)),
        (
            "remote_manifest_sha256",
            ("sudo", "-n", "sha256sum", f"{CAMPAIGN_ROOT}/preregistration.json"),
        ),
        (
            "phase_r_mount",
            ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,UUID,TARGET", NEW_MOUNT),
        ),
        (
            "campaign_root_stat",
            ("sudo", "-n", "stat", "-c", "%n|%F|%U|%G|%a|%s", CAMPAIGN_ROOT),
        ),
        (
            "campaign_db_stat",
            ("sudo", "-n", "stat", "-c", "%n|%F|%U|%G|%a|%s", f"{CAMPAIGN_ROOT}/campaign.db"),
        ),
        (
            "raw_events_stat",
            ("sudo", "-n", "stat", "-c", "%n|%F|%U|%G|%a|%s", f"{CAMPAIGN_ROOT}/raw-events"),
        ),
        (
            "collector_service",
            (
                "sudo",
                "-n",
                "systemctl",
                "show",
                "binance-bot-2-prospective.service",
                "--property=ActiveState,SubState,UnitFileState,MainPID,NRestarts,ExecMainStartTimestamp,ExecMainStatus,Result,ExecStart,WorkingDirectory,Environment,DropInPaths",
                "--no-pager",
            ),
        ),
        (
            "fast_health_service",
            (
                "sudo",
                "-n",
                "systemctl",
                "show",
                "binance-bot-2-health.service",
                "--property=ActiveState,SubState,UnitFileState,ExecMainStatus,Result,ExecMainStartTimestamp,ExecMainExitTimestamp",
                "--no-pager",
            ),
        ),
        (
            "deep_health_service",
            (
                "sudo",
                "-n",
                "systemctl",
                "show",
                "binance-bot-2-deep-health.service",
                "--property=ActiveState,SubState,UnitFileState,ExecMainStatus,Result,ExecMainStartTimestamp,ExecMainExitTimestamp",
                "--no-pager",
            ),
        ),
        (
            "fast_health_timer",
            (
                "sudo",
                "-n",
                "systemctl",
                "show",
                "binance-bot-2-health.timer",
                "--property=ActiveState,SubState,UnitFileState,LastTriggerUSec,NextElapseUSecRealtime",
                "--no-pager",
            ),
        ),
        (
            "deep_health_timer",
            (
                "sudo",
                "-n",
                "systemctl",
                "show",
                "binance-bot-2-deep-health.timer",
                "--property=ActiveState,SubState,UnitFileState,LastTriggerUSec,NextElapseUSecRealtime",
                "--no-pager",
            ),
        ),
        (
            "systemd_verify",
            (
                "sudo",
                "-n",
                "systemd-analyze",
                "verify",
                "binance-bot-2-prospective.service",
                "binance-bot-2-health.service",
                "binance-bot-2-deep-health.service",
                "binance-bot-2-health.timer",
                "binance-bot-2-deep-health.timer",
            ),
        ),
        ("old_app_exists", ("sudo", "-n", "test", "-d", "/opt/binance-bot-2/app")),
        ("old_config_exists", ("sudo", "-n", "test", "-f", "/etc/binance-bot-2/prospective.yaml")),
        (
            "old_campaign_root_exists",
            ("sudo", "-n", "test", "-d", "/var/lib/binance-bot-2/prospective/causal-retest-prospective-phase-o-oci-20260827-v1"),
        ),
    )
    for name, argv in fixed:
        commands[name] = _remote(argv)

    commands["source_import_probe"] = _remote_python(
        f"""import json
from signalbot.config import load_settings
from signalbot.prospective.source_freeze import default_source_root, freeze_source

freeze = freeze_source(default_source_root())
settings = load_settings({NEW_CONFIG!r})
for module_name in ("signalbot", "oci_operational_health", "oci_preregistration"):
    __import__(module_name)
print(json.dumps({{
    "source_identity": freeze.source_identity,
    "source_file_count": len(freeze.files),
    "campaign_id": settings.shadow.campaign_id,
    "activation_ms": settings.shadow.activation_ms,
    "source_identity_in_config": settings.shadow.source_identity,
    "storage_url": settings.storage.url,
    "raw_event_directory": settings.runtime.raw_event_directory,
    "quota_bytes": settings.runtime.raw_event_max_bytes,
    "imports": ["signalbot", "oci_operational_health", "oci_preregistration"],
}}, sort_keys=True))
"""
    )

    service = _props(commands["collector_service"].stdout)
    fast_service = _props(commands["fast_health_service"].stdout)
    deep_service = _props(commands["deep_health_service"].stdout)
    fast_timer = _props(commands["fast_health_timer"].stdout)
    deep_timer = _props(commands["deep_health_timer"].stdout)
    probe = _json_line(commands["source_import_probe"].stdout)

    _assert(commands["current_target"].exit_code == 0, "current symlink target is unavailable")
    _assert(commands["current_target"].stdout.strip() == RELEASE, "current symlink target mismatch")
    _assert(commands["remote_bundle_sha256"].stdout.startswith(EXPECTED_BUNDLE_HASH), "remote v2 bundle hash mismatch")
    _assert(commands["remote_config_sha256"].stdout.startswith(EXPECTED_CONFIG_HASH), "remote Phase-R config hash mismatch")
    _assert(commands["remote_manifest_sha256"].stdout.startswith(EXPECTED_MANIFEST_HASH), "remote Phase-R manifest hash mismatch")
    _assert(commands["phase_r_mount"].exit_code == 0, "Phase-R volume mount is unavailable")
    _assert(f"{NEW_UUID} {NEW_MOUNT}" in commands["phase_r_mount"].stdout, "Phase-R UUID mount mismatch")
    _assert(commands["campaign_db_stat"].exit_code == 0, "Phase-R campaign DB is missing")
    _assert(commands["raw_events_stat"].exit_code == 0, "Phase-R raw-events directory is missing")
    _assert(probe["source_identity"] == EXPECTED_SOURCE, "remote Phase-R source identity mismatch")
    _assert(probe["source_file_count"] == 239, "remote frozen source file count mismatch")
    _assert(probe["campaign_id"] == CAMPAIGN_ID, "remote Phase-R campaign mismatch")
    _assert(probe["source_identity_in_config"] == EXPECTED_SOURCE, "remote config source mismatch")
    _assert(probe["quota_bytes"] == EXPECTED_QUOTA_BYTES, "remote quota mismatch")
    _assert(service.get("ActiveState") == "active", "Phase-R collector is not active")
    _assert(service.get("SubState") == "running", "Phase-R collector is not running")
    _assert(service.get("NRestarts") == "0", "Phase-R collector restarted unexpectedly")
    _assert("prospective-phase-r.yaml" in service.get("ExecStart", ""), "collector config path mismatch")
    _assert(service.get("WorkingDirectory") == CURRENT, "collector working directory mismatch")
    _assert(CURRENT in service.get("Environment", ""), "collector PYTHONPATH does not bind current release")
    _assert(fast_timer.get("ActiveState") == "active", "FAST health timer is not active")
    _assert(fast_timer.get("UnitFileState") == "enabled", "FAST health timer is not enabled")
    _assert(deep_timer.get("ActiveState") == "active", "DEEP health timer is not active")
    _assert(deep_timer.get("UnitFileState") == "enabled", "DEEP health timer is not enabled")
    _assert(commands["systemd_verify"].exit_code == 0, "systemd unit verification failed")
    _assert(commands["old_app_exists"].exit_code == 0, "old rollback source is missing")
    _assert(commands["old_config_exists"].exit_code == 0, "old rollback config is missing")
    _assert(commands["old_campaign_root_exists"].exit_code == 0, "old campaign root was not preserved")
    _assert(fast_service.get("Result") in {"exec-condition", "success"}, "FAST health state is unexpected")
    _assert(deep_service.get("Result") in {"", "exec-condition", "success"}, "DEEP health state is unexpected")

    local_bundle = Path("artifacts/evidence/phase-r-source-bundle-v2.tar.gz")
    local_config = Path("config/prospective.causal-retest.phase-r.yaml")
    local_manifest = Path(
        "artifacts/evidence/_phase-r-preregistration-staging/"
        f"{CAMPAIGN_ID}/preregistration.json"
    )
    _assert(local_bundle.is_file(), "local v2 bundle is missing")
    _assert(local_config.is_file(), "local Phase-R config is missing")
    _assert(local_manifest.is_file(), "local Phase-R manifest is missing")
    local_bundle_hash = _sha256(local_bundle)
    local_config_hash = _sha256(local_config)
    local_manifest_hash = _sha256(local_manifest)
    _assert(local_bundle_hash == EXPECTED_BUNDLE_HASH, "local v2 bundle hash changed")
    _assert(local_config_hash == EXPECTED_CONFIG_HASH, "local Phase-R config hash changed")
    _assert(local_manifest_hash == EXPECTED_MANIFEST_HASH, "local Phase-R manifest hash changed")

    captured_at = datetime.now(tz=UTC)
    captured_at_ms = round(captured_at.timestamp() * 1000)
    records = {name: capture.record() for name, capture in commands.items()}
    return {
        "schema_contract": "phase-r-deployment-receipt-v1",
        "schema_version": "phase_r_deployment_receipt_v1",
        "captured_at_utc": captured_at.isoformat(),
        "captured_at_ms": captured_at_ms,
        "work_phase_id": "r8",
        "campaign_id": CAMPAIGN_ID,
        "activation_ms": ACTIVATION_MS,
        "capture_before_activation": captured_at_ms < ACTIVATION_MS,
        "source_and_import_gate": {
            "release_path": RELEASE,
            "current_path": CURRENT,
            "current_target": commands["current_target"].stdout.strip(),
            "source_identity": probe["source_identity"],
            "source_file_count": probe["source_file_count"],
            "config_source_identity": probe["source_identity_in_config"],
            "imports": probe["imports"],
            "bundle_sha256": local_bundle_hash,
            "bundle_bytes": local_bundle.stat().st_size,
        },
        "config_and_storage": {
            "config_path": NEW_CONFIG,
            "config_sha256": local_config_hash,
            "manifest_path": f"{CAMPAIGN_ROOT}/preregistration.json",
            "manifest_sha256": local_manifest_hash,
            "mount_path": NEW_MOUNT,
            "mount_uuid": NEW_UUID,
            "campaign_root": CAMPAIGN_ROOT,
            "database_path": f"{CAMPAIGN_ROOT}/campaign.db",
            "raw_events_path": f"{CAMPAIGN_ROOT}/raw-events",
            "quota_bytes": probe["quota_bytes"],
        },
        "cutover": {
            "collector_unit": "binance-bot-2-prospective.service",
            "active": service.get("ActiveState") == "active",
            "sub_state": service.get("SubState"),
            "main_pid": service.get("MainPID"),
            "restart_count": service.get("NRestarts"),
            "working_directory": service.get("WorkingDirectory"),
            "exec_start_bound_to_phase_r": "prospective-phase-r.yaml" in service.get("ExecStart", ""),
            "drop_ins": service.get("DropInPaths", "").split(),
            "atomic_current_switch": True,
            "old_collector_started_after_new_cutover": False,
        },
        "health_services": {
            "fast": fast_service,
            "deep": deep_service,
            "fast_timer": fast_timer,
            "deep_timer": deep_timer,
            "pre_activation_guard": fast_service.get("Result") == "exec-condition",
            "fast_service_failure_is_absent": fast_service.get("Result") != "exit-code",
            "deep_service_failure_is_absent": deep_service.get("Result") not in {"exit-code", "failed"},
        },
        "preservation": {
            "old_source_path": "/opt/binance-bot-2/app",
            "old_source_preserved": commands["old_app_exists"].exit_code == 0,
            "old_config_path": "/etc/binance-bot-2/prospective.yaml",
            "old_config_preserved": commands["old_config_exists"].exit_code == 0,
            "old_campaign_root": "/var/lib/binance-bot-2/prospective/causal-retest-prospective-phase-o-oci-20260827-v1",
            "old_campaign_preserved": commands["old_campaign_root_exists"].exit_code == 0,
            "data_deleted": False,
        },
        "rollback_coordinates": {
            "stop_unit": "sudo systemctl stop binance-bot-2-prospective.service",
            "remove_phase_r_collector_dropin": "/etc/systemd/system/binance-bot-2-prospective.service.d/phase-r-deployment.conf",
            "remove_phase_r_storage_dropin": "/etc/systemd/system/binance-bot-2-prospective.service.d/phase-r-storage.conf",
            "restore_source_path": "/opt/binance-bot-2/app",
            "restore_config_path": "/etc/binance-bot-2/prospective.yaml",
            "restore_campaign_root": "/var/lib/binance-bot-2/prospective/causal-retest-prospective-phase-o-oci-20260827-v1",
            "restore_health_unit_sources": [
                "deployment/binance-bot-2-health.service",
                "deployment/binance-bot-2-deep-health.service",
                "deployment/binance-bot-2-health.timer",
                "deployment/binance-bot-2-deep-health.timer",
            ],
            "rollback_rehearsed": False,
        },
        "commands": records,
        "governance": {
            "new_oci_resource": False,
            "public_binance_market_data_only": True,
            "private_binance_api_or_orders": False,
            "strategy_policy_r2_shadow_retest_outcome_or_discord_semantics_change": False,
            "commit": False,
            "push": False,
            "deploy": True,
            "outcome_or_profitability_inspection": False,
        },
        "verdict": "PHASE_R_DEPLOYED_RUNNING_WITH_ROLLBACK_COORDINATES",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts/evidence/phase-r-deployment-receipt-v1.json"))
    args = parser.parse_args()
    output = args.output.resolve()
    workspace = Path.cwd().resolve()
    if not output.is_relative_to(workspace / "artifacts" / "evidence"):
        raise SystemExit("output must stay under artifacts/evidence")
    if output.exists():
        raise SystemExit("refusing to overwrite existing deployment receipt")
    receipt = collect()
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "verdict": receipt["verdict"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
