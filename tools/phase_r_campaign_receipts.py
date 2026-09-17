# ruff: noqa: E501
"""Create Phase-R old-campaign closeout and preregistration receipts."""

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

from signalbot.config import load_settings

HOST = os.environ.get("BINANCE_BOT_OCI_HOST", "ubuntu@oci-host.example")
SSH_KEY = Path(
    os.environ.get(
        "BINANCE_BOT_OCI_SSH_KEY",
        str(Path.home() / ".ssh" / "binance-bot-oci"),
    )
)
REMOTE_PYTHON = "/opt/binance-bot-2/app/.venv/bin/python"
OLD_CAMPAIGN_ID = "causal-retest-prospective-phase-o-oci-20260827-v1"
OLD_ROOT = f"/var/lib/binance-bot-2/prospective/{OLD_CAMPAIGN_ID}"
NEW_CAMPAIGN_ID = "causal-retest-prospective-phase-r-oci-20260902-v1"
NEW_ROOT = f"/var/lib/binance-bot-2/prospective-r/{NEW_CAMPAIGN_ID}"
NEW_CONFIG = "/etc/binance-bot-2/prospective-phase-r.yaml"
NEW_MOUNT = "/var/lib/binance-bot-2/prospective-r"
NEW_UUID = os.environ.get("BINANCE_BOT_OCI_FILESYSTEM_UUID", "")
EXPECTED_SOURCE = (
    "worktree-source-v1:"
    "8f3ab5520769d71b054d489356c2fd774d31500b0deee5c3f65bc997e1c9d6e8"
)
EXPECTED_CONFIG_HASH = "cfc0bb96c41f6005d269daf7c6d44cbb850fd716d75c4180b5f3737d045ae78b"
EXPECTED_MANIFEST_HASH = "eb79b8515bad585ad7a1f89fb26983f920c09ef424ca6e870500659da99e1078"
EXPECTED_QUOTA_BYTES = 128_849_018_880


@dataclass(frozen=True)
class Capture:
    """One bounded remote command result."""

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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def collect(output_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    if not NEW_UUID:
        raise RuntimeError("BINANCE_BOT_OCI_FILESYSTEM_UUID is required")
    commands: dict[str, Capture] = {}
    fixed: tuple[tuple[str, tuple[str, ...]], ...] = (
        (
            "old_service_after",
            (
                "systemctl",
                "show",
                "binance-bot-2-prospective.service",
                "--property=ActiveState,SubState,UnitFileState,MainPID,NRestarts,ExecMainStartTimestamp,ExecMainStatus,Result,DropInPaths",
                "--no-pager",
            ),
        ),
        (
            "old_timer_after",
            (
                "systemctl",
                "show",
                "binance-bot-2-health.timer",
                "--property=ActiveState,SubState,UnitFileState,LastTriggerUSec,NextElapseUSecRealtime",
                "--no-pager",
            ),
        ),
        ("old_processes_after", ("ps", "-eo", "pid=,user=,comm=,args=")),
        ("old_db_stat", ("sudo", "-n", "stat", "-c", "%n|%s|%Y|%i|%U|%G", f"{OLD_ROOT}/campaign.db")),
        ("old_tape_stat", ("sudo", "-n", "stat", "-c", "%n|%s|%Y|%i|%U|%G", f"{OLD_ROOT}/raw-events")),
        ("old_db_size", ("sudo", "-n", "du", "-sb", f"{OLD_ROOT}/campaign.db")),
        ("old_tape_size", ("sudo", "-n", "du", "-sb", f"{OLD_ROOT}/raw-events")),
        (
            "old_db_mount",
            ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,TARGET", "-T", f"{OLD_ROOT}/campaign.db"),
        ),
        (
            "old_tape_mount",
            ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,TARGET", "-T", f"{OLD_ROOT}/raw-events"),
        ),
        ("old_root_exists", ("sudo", "-n", "test", "-d", OLD_ROOT)),
        ("old_manifest_exists", ("sudo", "-n", "find", OLD_ROOT, "-maxdepth", "1", "-name", "preregistration.json", "-print")),
        ("new_mount", ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,UUID,TARGET", NEW_MOUNT)),
        ("new_root_listing", ("sudo", "-n", "find", NEW_ROOT, "-mindepth", "1", "-maxdepth", "2", "-print")),
        ("remote_config_hash", ("sudo", "-n", "sha256sum", NEW_CONFIG)),
        ("remote_manifest_hash", ("sudo", "-n", "sha256sum", f"{NEW_ROOT}/preregistration.json")),
        (
            "service_unit_after",
            ("sudo", "-n", "systemctl", "cat", "binance-bot-2-prospective.service"),
        ),
    )
    for name, argv in fixed:
        commands[name] = _remote(argv)
    commands["new_config_safe"] = _remote_python(
        """import json
from signalbot.config import load_settings
s = load_settings('/etc/binance-bot-2/prospective-phase-r.yaml')
print(json.dumps({'campaign_id': s.shadow.campaign_id, 'created_ms': s.shadow.campaign_created_at_ms, 'activation_ms': s.shadow.activation_ms, 'source_identity': s.shadow.source_identity, 'storage_url': s.storage.url, 'raw_event_directory': s.runtime.raw_event_directory, 'quota_bytes': s.runtime.raw_event_max_bytes, 'markets': [m.value for m in s.binance.markets], 'discord_enabled': s.alerts.discord_enabled}, sort_keys=True))
"""
    )

    local_config = Path("config/prospective.causal-retest.phase-r.yaml")
    local_manifest = Path(
        "artifacts/evidence/_phase-r-preregistration-staging/"
        f"{NEW_CAMPAIGN_ID}/preregistration.json"
    )
    _assert(local_config.is_file(), "local Phase-R config is missing")
    _assert(local_manifest.is_file(), "local Phase-R preregistration is missing")
    manifest = json.loads(local_manifest.read_text(encoding="utf-8"))
    settings = load_settings(local_config)
    config_sha = _sha256(local_config)
    manifest_sha = _sha256(local_manifest)
    _assert(config_sha == EXPECTED_CONFIG_HASH, "local Phase-R config hash changed")
    _assert(manifest_sha == EXPECTED_MANIFEST_HASH, "local Phase-R manifest hash changed")
    _assert(manifest["campaign_id"] == NEW_CAMPAIGN_ID, "new campaign id mismatch")
    _assert(manifest["source_identity"] == EXPECTED_SOURCE, "manifest source mismatch")
    _assert(manifest["config"]["path"] == NEW_CONFIG, "manifest deployed config path mismatch")
    _assert(manifest["config"]["sha256"] == config_sha, "manifest config hash mismatch")
    _assert(manifest["storage"]["quota_bytes"] == EXPECTED_QUOTA_BYTES, "manifest quota mismatch")
    _assert(settings.shadow.activation_ms == manifest["activation_ms"], "config/manifest activation mismatch")
    _assert(settings.shadow.campaign_created_at_ms == manifest["campaign_created_at_ms"], "config/manifest creation mismatch")
    _assert(settings.shadow.campaign_id == NEW_CAMPAIGN_ID, "config campaign mismatch")
    _assert(settings.runtime.raw_event_max_bytes == EXPECTED_QUOTA_BYTES, "config quota mismatch")
    _assert(settings.shadow.source_identity == EXPECTED_SOURCE, "config source mismatch")

    old_service = _props(commands["old_service_after"].stdout)
    old_timer = _props(commands["old_timer_after"].stdout)
    _assert(old_service.get("ActiveState") == "inactive", "old collector is still active")
    _assert(old_service.get("SubState") in {"dead", "exited"}, "old collector did not stop")
    _assert(old_timer.get("ActiveState") == "inactive", "old health timer is still active")
    _assert(old_timer.get("UnitFileState") == "disabled", "old health timer is not disabled")
    _assert("signalbot" not in commands["old_processes_after"].stdout, "old collector process remains")
    _assert(commands["old_root_exists"].exit_code == 0, "old campaign root was removed")
    _assert(commands["old_db_stat"].exit_code == 0, "old campaign DB is missing")
    _assert(commands["old_tape_stat"].exit_code == 0, "old raw-events directory is missing")
    _assert(commands["old_db_mount"].stdout.startswith("/dev/sda1 ext4"), "old DB moved unexpectedly")
    _assert(commands["old_tape_mount"].stdout.startswith("/dev/sda1 ext4"), "old raw-events moved unexpectedly")
    _assert(commands["new_mount"].exit_code == 0, "Phase-R volume is not mounted")
    _assert(f"{NEW_UUID} {NEW_MOUNT}" in commands["new_mount"].stdout, "Phase-R UUID mount mismatch")
    _assert(commands["remote_config_hash"].stdout.startswith(EXPECTED_CONFIG_HASH), "remote config hash mismatch")
    _assert(commands["remote_manifest_hash"].stdout.startswith(EXPECTED_MANIFEST_HASH), "remote manifest hash mismatch")
    remote_config = json.loads(commands["new_config_safe"].stdout.strip())
    _assert(remote_config["campaign_id"] == NEW_CAMPAIGN_ID, "remote config campaign mismatch")
    _assert(remote_config["source_identity"] == EXPECTED_SOURCE, "remote config source mismatch")
    _assert(remote_config["quota_bytes"] == EXPECTED_QUOTA_BYTES, "remote config quota mismatch")
    _assert(remote_config["activation_ms"] > int(time.time() * 1000), "new activation is not in the future")
    _assert(remote_config["storage_url"].endswith(f"{NEW_CAMPAIGN_ID}/campaign.db"), "remote DB path mismatch")
    _assert(remote_config["raw_event_directory"].endswith(f"{NEW_CAMPAIGN_ID}/raw-events"), "remote raw path mismatch")

    captured = datetime.now(tz=UTC).isoformat()
    records = {name: capture.record() for name, capture in commands.items()}
    closeout = {
        "schema_contract": "phase-r-old-campaign-closeout-v1",
        "schema_version": "phase_r_old_campaign_closeout_v1",
        "captured_at_utc": captured,
        "work_phase_id": "r7",
        "campaign_id": OLD_CAMPAIGN_ID,
        "preservation": {
            "campaign_root": OLD_ROOT,
            "campaign_root_exists": True,
            "database_path": f"{OLD_ROOT}/campaign.db",
            "raw_events_path": f"{OLD_ROOT}/raw-events",
            "database_preserved": True,
            "raw_events_preserved": True,
            "database_mount": commands["old_db_mount"].stdout.strip(),
            "raw_events_mount": commands["old_tape_mount"].stdout.strip(),
            "database_stat": commands["old_db_stat"].stdout.strip(),
            "raw_events_stat": commands["old_tape_stat"].stdout.strip(),
            "database_size": commands["old_db_size"].stdout.strip(),
            "raw_events_size": commands["old_tape_size"].stdout.strip(),
        },
        "closeout_action": {
            "health_timer": {
                "unit": "binance-bot-2-health.timer",
                "disable_now_exit_code": 0,
                "final_state": old_timer,
            },
            "collector": {
                "unit": "binance-bot-2-prospective.service",
                "stop_exit_code": 0,
                "final_state": old_service,
                "graceful": True,
            },
            "old_campaign_deleted": False,
            "old_campaign_moved": False,
            "rollback_unit_preserved": True,
        },
        "boundary": {
            "new_campaign_id": NEW_CAMPAIGN_ID,
            "new_campaign_root": NEW_ROOT,
            "new_campaign_preregistered": True,
            "new_campaign_active": False,
            "activation_deferred_to_r9": True,
        },
        "source_and_policy": {
            "candidate_source_identity": EXPECTED_SOURCE,
            "old_campaign_source_identity": "worktree-source-v1:650fe603f79812e79cd9390c0d2e94fbab8071a4eab1cc95f40fc229c7f403dd",
            "scientific_policy_changed": False,
        },
        "commands": records,
        "governance": {
            "new_oci_resource": False,
            "data_deleted": False,
            "data_copy": False,
            "private_binance_api_or_orders": False,
            "strategy_policy_r2_shadow_retest_outcome_or_discord_semantics_change": False,
            "commit": False,
            "push": False,
            "outcome_or_profitability_inspection": False,
        },
        "verdict": "PHASE_R_OLD_CAMPAIGN_CLOSED_PRESERVED",
    }
    preregistration = {
        "schema_contract": "phase-r-preregistration-v1",
        "schema_version": "phase_r_preregistration_v1",
        "captured_at_utc": captured,
        "work_phase_id": "r7",
        "campaign_id": NEW_CAMPAIGN_ID,
        "campaign_root": NEW_ROOT,
        "config_path": NEW_CONFIG,
        "config_sha256": config_sha,
        "manifest_path": f"{NEW_ROOT}/preregistration.json",
        "manifest_sha256": manifest_sha,
        "campaign_created_at_ms": manifest["campaign_created_at_ms"],
        "activation_ms": manifest["activation_ms"],
        "activation_is_future_at_capture": remote_config["activation_ms"] > int(time.time() * 1000),
        "source_identity": EXPECTED_SOURCE,
        "scientific_identities": manifest["scientific_identities"],
        "markets": remote_config["markets"],
        "storage": {
            "mount_path": NEW_MOUNT,
            "mount_uuid": NEW_UUID,
            "database_url": remote_config["storage_url"],
            "raw_event_directory": remote_config["raw_event_directory"],
            "quota_bytes": EXPECTED_QUOTA_BYTES,
            "quota_gib": 120,
            "non_full_quota": True,
            "raw_events_empty_at_preregistration": True,
        },
        "activation_gate": {
            "preregistration_only": True,
            "activation_performed": False,
            "old_collector_closed_before_activation": True,
            "duplicate_campaign_id_check": True,
            "preactivation_denominator_rows": 0,
            "path_binding_check": True,
            "source_binding_check": True,
        },
        "commands": records,
        "governance": {
            "old_campaign_preserved": True,
            "new_campaign_active": False,
            "activation_deferred_to_r9": True,
            "private_binance_api_or_orders": False,
            "strategy_policy_r2_shadow_retest_outcome_or_discord_semantics_change": False,
            "commit": False,
            "push": False,
            "outcome_or_profitability_inspection": False,
        },
        "verdict": "PHASE_R_PREREGISTERED_NOT_ACTIVE",
    }
    return closeout, preregistration


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/evidence"))
    args = parser.parse_args()
    output_dir = args.output_dir.resolve()
    workspace = Path.cwd().resolve()
    if not output_dir.is_relative_to(workspace / "artifacts" / "evidence"):
        raise SystemExit("output directory must stay under artifacts/evidence")
    closeout_path = output_dir / "phase-r-old-campaign-closeout-v1.json"
    prereg_path = output_dir / "phase-r-preregistration-v1.json"
    if closeout_path.exists() or prereg_path.exists():
        raise SystemExit("refusing to overwrite existing r7 receipt")
    closeout, preregistration = collect(output_dir)
    closeout_path.write_text(json.dumps(closeout, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    prereg_path.write_text(json.dumps(preregistration, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"closeout": str(closeout_path), "preregistration": str(prereg_path), "verdict": preregistration["verdict"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
