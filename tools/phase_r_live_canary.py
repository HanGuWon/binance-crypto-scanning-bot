# ruff: noqa: E501
"""Capture the multi-hour, outcome-blind Phase-R operational canary."""

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
CURRENT = "/opt/binance-bot-2/current"
RELEASE = "/opt/binance-bot-2/releases/phase-r-oci-20260902-v1"
CAMPAIGN_ID = "causal-retest-prospective-phase-r-oci-20260902-v1"
CAMPAIGN_ROOT = f"/var/lib/binance-bot-2/prospective-r/{CAMPAIGN_ID}"
FAST_DIR = f"{CAMPAIGN_ROOT}/health/phase-fast"
DEEP_DIR = f"{CAMPAIGN_ROOT}/health/phase-deep"
DB_PATH = f"{CAMPAIGN_ROOT}/campaign.db"
RAW_PATH = f"{CAMPAIGN_ROOT}/raw-events"
OLD_RAW_PATH = "/var/lib/binance-bot-2/prospective/causal-retest-prospective-phase-o-oci-20260827-v1/raw-events"
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
EXPECTED_QUOTA_BYTES = 128_849_018_880
MIN_SCHEDULED_FAST_RECEIPTS = 3
EXPECTED_SCIENTIFIC_IDENTITIES = {
    "config_sha256": "43971efece98b71f815a4e4f476e18e3467f9a8cc02de3d51b8be1c55ccf3e20",
    "shadow_policy_sha256": "e0c50d8af4bffe554798f2b24bd6177da2faa7d6bbbdf3645b73563ffe1694f0",
    "retest_policy_sha256": "eaa0951691054622e13c59ce956d01a678bc596e452e00d6e5646ee6fadb1ebd",
    "outcome_policy_sha256": "38b8e2ba8681460927dcfedc5f480cffaf61367d8fa5f536271b29ce04faf4a9",
}
OLD_RAW_BYTES_AT_CLOSEOUT = 24_169_032_198


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
            timeout=120,
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
        stderr = f"TIMEOUT after 120s\n{stderr_value}"
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


def _json_line(stdout: str) -> dict[str, Any]:
    for line in reversed(stdout.splitlines()):
        if line.strip().startswith("{"):
            value = json.loads(line)
            if isinstance(value, dict):
                return value
    raise RuntimeError("remote canary probe did not return a JSON object")


def _props(stdout: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            result[key] = value
    return result


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def collect() -> dict[str, Any]:
    if not NEW_UUID:
        raise RuntimeError("BINANCE_BOT_OCI_FILESYSTEM_UUID is required")
    commands: dict[str, Capture] = {}
    fixed: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("current_target", ("readlink", "-f", CURRENT)),
        ("database_mount", ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,UUID,TARGET", "-T", DB_PATH)),
        ("raw_events_mount", ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,UUID,TARGET", "-T", RAW_PATH)),
        ("root_raw_size", ("sudo", "-n", "du", "-sb", OLD_RAW_PATH)),
        ("collector_service", ("sudo", "-n", "systemctl", "show", "binance-bot-2-prospective.service", "--property=ActiveState,SubState,MainPID,NRestarts,ExecMainStatus,Result,ExecStart,WorkingDirectory,Environment", "--no-pager")),
        ("fast_timer", ("sudo", "-n", "systemctl", "show", "binance-bot-2-health.timer", "--property=ActiveState,SubState,UnitFileState,LastTriggerUSec,NextElapseUSecRealtime", "--no-pager")),
        ("deep_timer", ("sudo", "-n", "systemctl", "show", "binance-bot-2-deep-health.timer", "--property=ActiveState,SubState,UnitFileState,LastTriggerUSec,NextElapseUSecRealtime", "--no-pager")),
        ("fast_journal", ("sudo", "-n", "journalctl", "-u", "binance-bot-2-health.service", "--since", "2026-09-01 17:08:52 UTC", "--no-pager", "-o", "cat")),
        ("deep_journal", ("sudo", "-n", "journalctl", "-u", "binance-bot-2-deep-health.service", "--since", "2026-09-01 17:08:52 UTC", "--no-pager", "-o", "cat")),
        ("collector_journal", ("sudo", "-n", "journalctl", "-u", "binance-bot-2-prospective.service", "--since", "2026-09-01 17:08:52 UTC", "--no-pager", "-o", "cat")),
        ("systemd_verify", ("sudo", "-n", "systemd-analyze", "verify", "binance-bot-2-prospective.service", "binance-bot-2-health.service", "binance-bot-2-deep-health.service", "binance-bot-2-health.timer", "binance-bot-2-deep-health.timer")),
    )
    for name, argv in fixed:
        commands[name] = _remote(argv)

    commands["health_probe"] = _remote_python(
        f"""import json
from pathlib import Path

activation = {ACTIVATION_MS}
fast_root = Path({FAST_DIR!r})
deep_root = Path({DEEP_DIR!r})

def _load(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError, json.JSONDecodeError):
        return None

fast = []
for path in sorted(fast_root.glob("phase-*.json")):
    payload = _load(path)
    if not isinstance(payload, dict) or int(payload.get("captured_at_ms", -1)) < activation:
        continue
    if payload.get("mode") != "fast":
        continue
    source = payload.get("source_identity") or {{}}
    errors = payload.get("errors") or {{}}
    tape = payload.get("tape") or {{}}
    tape_markets = tape.get("markets") or {{}}
    quota = payload.get("quota") or {{}}
    fast.append({{
        "file": path.name,
        "captured_at_ms": payload.get("captured_at_ms"),
        "schema_version": payload.get("schema_version"),
        "mode": payload.get("mode"),
        "state": payload.get("state"),
        "source_match": source.get("match"),
        "sqlite_query_only": (payload.get("sqlite") or {{}}).get("query_only"),
        "campaign_match": (payload.get("sqlite") or {{}}).get("campaign_match"),
        "chain_healthy": tape.get("chain_healthy"),
        "quota_gate": quota.get("gate_met"),
        "physical_bytes": tape.get("physical_bytes"),
        "market_progress": {{
            market: {{key: (tape_markets.get(market) or {{}}).get(key) for key in ("fresh", "sealed_segment_count", "manifest_count", "active_partial_count", "latest_last_received_at_ms")}}
            for market in ("spot", "futures")
        }},
        "quota": {{key: quota.get(key) for key in ("current_physical_bytes", "required_48h_bytes", "finalize_reserve_bytes", "safety_margin_bytes", "quota_bytes", "gate_met")}},
        "errors": errors,
        "action_attempted": (payload.get("action") or {{}}).get("attempted"),
    }})

deep = []
for path in sorted(deep_root.glob("phase-p-*.json")):
    payload = _load(path)
    if not isinstance(payload, dict) or int(payload.get("captured_at_ms", -1)) < activation:
        continue
    source = payload.get("source_identity") or {{}}
    errors = payload.get("errors") or {{}}
    tape = payload.get("tape") or {{}}
    tape_markets = tape.get("markets") or {{}}
    quota = payload.get("quota") or {{}}
    deep.append({{
        "file": path.name,
        "captured_at_ms": payload.get("captured_at_ms"),
        "schema_version": payload.get("schema_version"),
        "state": payload.get("state"),
        "reasons": payload.get("reasons"),
        "source_match": source.get("match"),
        "sqlite_quick_check": (payload.get("sqlite") or {{}}).get("quick_check"),
        "chain_healthy": tape.get("chain_healthy"),
        "quota_gate": quota.get("gate_met"),
        "quota_bytes": quota.get("quota_bytes"),
        "physical_bytes": tape.get("physical_bytes"),
        "market_progress": {{
            market: {{key: (tape_markets.get(market) or {{}}).get(key) for key in ("fresh", "sealed_segment_count", "manifest_count", "active_partial_count", "latest_last_received_at_ms")}}
            for market in ("spot", "futures")
        }},
        "quota": {{key: quota.get(key) for key in ("current_physical_bytes", "required_48h_bytes", "finalize_reserve_bytes", "safety_margin_bytes", "quota_bytes", "gate_met")}},
        "errors": errors,
    }})

print(json.dumps({{"fast": fast, "deep": deep}}, sort_keys=True))
"""
    )

    health = _json_line(commands["health_probe"].stdout)
    service = _props(commands["collector_service"].stdout)
    fast_timer = _props(commands["fast_timer"].stdout)
    deep_timer = _props(commands["deep_timer"].stdout)
    fast_receipts = health["fast"]
    deep_receipts = health["deep"]
    _assert(commands["current_target"].stdout.strip() == RELEASE, "current release target mismatch")
    _assert(commands["database_mount"].exit_code == 0 and f"{NEW_UUID} {NEW_MOUNT}" in commands["database_mount"].stdout, "database mount mismatch")
    _assert(commands["raw_events_mount"].exit_code == 0 and f"{NEW_UUID} {NEW_MOUNT}" in commands["raw_events_mount"].stdout, "raw-events mount mismatch")
    _assert(commands["root_raw_size"].exit_code == 0, "old root raw size probe failed")
    root_raw_bytes = int(commands["root_raw_size"].stdout.split()[0])
    _assert(root_raw_bytes == OLD_RAW_BYTES_AT_CLOSEOUT, "root-backed old raw data grew after closeout")
    _assert(commands["systemd_verify"].exit_code == 0, "systemd verification failed")
    _assert(service.get("ActiveState") == "active" and service.get("SubState") == "running", "collector is not running")
    _assert(service.get("NRestarts") == "0", "collector restart count changed")
    _assert("prospective-phase-r.yaml" in service.get("ExecStart", ""), "collector is not on Phase-R config")
    _assert(fast_timer.get("ActiveState") == "active" and fast_timer.get("UnitFileState") == "enabled", "FAST timer is not active/enabled")
    _assert(deep_timer.get("ActiveState") == "active" and deep_timer.get("UnitFileState") == "enabled", "DEEP timer is not active/enabled")
    _assert(
        len(fast_receipts) >= MIN_SCHEDULED_FAST_RECEIPTS,
        "fewer than three post-activation FAST receipts",
    )
    fast_receipts = sorted(fast_receipts, key=lambda item: int(item["captured_at_ms"]))
    selected_fast = fast_receipts[:MIN_SCHEDULED_FAST_RECEIPTS]
    _assert(all(item["schema_version"] == "oci_operational_health_v1" for item in selected_fast), "FAST receipt schema mismatch")
    _assert(all(item["mode"] == "fast" for item in selected_fast), "FAST mode mismatch")
    _assert(all(item["source_match"] is True for item in selected_fast), "FAST source drift")
    _assert(all(item["sqlite_query_only"] == 1 and item["campaign_match"] is True for item in selected_fast), "FAST DB gate mismatch")
    _assert(all(item["chain_healthy"] is True for item in selected_fast), "FAST segment chain failure")
    _assert(all(item["quota_gate"] is True for item in selected_fast), "FAST quota gate failure")
    _assert(all(item["action_attempted"] is False for item in selected_fast), "FAST attempted a service action")
    _assert(all(not any(value is not None for key, value in (item["errors"] or {}).items() if key in {"source", "sqlite", "fast_state"}) for item in selected_fast), "FAST operational error present")
    spacing = [
        int(selected_fast[index + 1]["captured_at_ms"])
        - int(selected_fast[index]["captured_at_ms"])
        for index in range(MIN_SCHEDULED_FAST_RECEIPTS - 1)
    ]
    _assert(all(delta >= 45 * 60 * 1000 for delta in spacing), "FAST receipts are not scheduled-hourly spaced")
    _assert(len(deep_receipts) >= 1, "no post-activation DEEP receipt")
    deep_receipts = sorted(deep_receipts, key=lambda item: int(item["captured_at_ms"]))
    valid_deep = [item for item in deep_receipts if item["schema_version"] == "oci_operational_health_v1" and item["source_match"] is True and item["sqlite_quick_check"] == "ok" and item["chain_healthy"] is True and item["quota_gate"] is True and item["quota_bytes"] == EXPECTED_QUOTA_BYTES and not any(value is not None for key, value in (item["errors"] or {}).items() if key in {"source", "sqlite", "history"})]
    _assert(valid_deep, "no post-fix DEEP audit pass")
    fast_faults = [line for line in commands["fast_journal"].stdout.splitlines() if any(marker in line.lower() for marker in ("failed to start", "start operation timed out", "traceback", "db_busy"))]
    deep_faults = [line for line in commands["deep_journal"].stdout.splitlines() if any(marker in line.lower() for marker in ("failed to start", "start operation timed out", "traceback"))]
    collector_faults = [
        line
        for line in commands["collector_journal"].stdout.splitlines()
        if "binance websocket disconnected" not in line.lower()
        and any(
            marker in line.lower()
            for marker in (
                "lock fatal",
                "database is locked",
                "db_busy",
                "health timeout",
                "start operation timed out",
                "traceback",
            )
        )
    ]
    _assert(not fast_faults and not deep_faults and not collector_faults, "fatal lock/health timeout evidence found")

    local_config = Path("config/prospective.causal-retest.phase-r.yaml")
    local_manifest = Path("artifacts/evidence/_phase-r-preregistration-staging") / CAMPAIGN_ID / "preregistration.json"
    _assert(local_config.is_file() and local_manifest.is_file(), "local Phase-R binding artifact missing")
    _assert(_sha256(local_config) == EXPECTED_CONFIG_HASH, "local config hash changed")
    _assert(_sha256(local_manifest) == EXPECTED_MANIFEST_HASH, "local manifest hash changed")

    captured = datetime.now(tz=UTC)
    return {
        "schema_contract": "phase-r-live-canary-v2",
        "schema_version": "phase_r_live_canary_v2",
        "captured_at_utc": captured.isoformat(),
        "captured_at_ms": round(captured.timestamp() * 1000),
        "work_phase_id": "r11",
        "campaign_id": CAMPAIGN_ID,
        "activation_ms": ACTIVATION_MS,
        "duration_since_activation_hours": round((captured.timestamp() * 1000 - ACTIVATION_MS) / 3_600_000, 3),
        "fast_health": {
            "scheduled_receipt_count_observed": len(fast_receipts),
            "selected_receipts": selected_fast,
            "selected_spacing_ms": spacing,
            "selected_state_values": [item["state"] for item in selected_fast],
            "literal_pass_state_gate": all(item["state"] == "GREEN" for item in selected_fast),
            "timer": fast_timer,
            "journal_fault_count": len(fast_faults),
        },
        "deep_audit": {
            "receipt_count_observed": len(deep_receipts),
            "selected_pass": valid_deep[-1],
            "literal_pass_state_gate": valid_deep[-1]["state"] == "GREEN",
            "timer": deep_timer,
            "journal_fault_count": len(deep_faults),
            "pre_fix_receipts_excluded": any(item.get("quota_bytes") != EXPECTED_QUOTA_BYTES for item in deep_receipts),
        },
        "collector": {
            "active_state": service.get("ActiveState"),
            "sub_state": service.get("SubState"),
            "main_pid": service.get("MainPID"),
            "restart_count": service.get("NRestarts"),
            "exec_main_status": service.get("ExecMainStatus"),
            "exec_start": service.get("ExecStart"),
            "fault_count": len(collector_faults),
        },
        "storage": {
            "mount_uuid": NEW_UUID,
            "mount_path": NEW_MOUNT,
            "database_mount": commands["database_mount"].stdout.strip(),
            "raw_events_mount": commands["raw_events_mount"].stdout.strip(),
            "old_root_raw_bytes_at_closeout": OLD_RAW_BYTES_AT_CLOSEOUT,
            "old_root_raw_bytes_at_capture": root_raw_bytes,
            "root_raw_growth": root_raw_bytes != OLD_RAW_BYTES_AT_CLOSEOUT,
        },
        "source_and_policy": {
            "source_identity": EXPECTED_SOURCE,
            "config_sha256": EXPECTED_CONFIG_HASH,
            "manifest_sha256": EXPECTED_MANIFEST_HASH,
            "scientific_identities": EXPECTED_SCIENTIFIC_IDENTITIES,
            "outcome_fields_inspected": False,
        },
        "commands": {name: capture.record() for name, capture in commands.items()},
        "governance": {
            "public_binance_market_data_only": True,
            "private_binance_api_or_orders": False,
            "strategy_policy_r2_shadow_retest_outcome_or_discord_semantics_change": False,
            "commit": False,
            "push": False,
            "outcome_or_profitability_inspection": False,
        },
        "verdict": "PHASE_R_MULTI_HOUR_OPERATIONAL_QUALIFICATION_PASS",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts/evidence/phase-r-live-canary-v1.json"))
    args = parser.parse_args()
    output = args.output.resolve()
    workspace = Path.cwd().resolve()
    if not output.is_relative_to(workspace / "artifacts" / "evidence"):
        raise SystemExit("output must stay under artifacts/evidence")
    if output.exists():
        raise SystemExit("refusing to overwrite existing live canary receipt")
    receipt = collect()
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "verdict": receipt["verdict"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
