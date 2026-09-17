# ruff: noqa: E501
"""Capture the post-activation, outcome-blind Phase-R coverage receipt."""

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
DB_PATH = f"{CAMPAIGN_ROOT}/campaign.db"
RAW_PATH = f"{CAMPAIGN_ROOT}/raw-events"
NEW_CONFIG = "/etc/binance-bot-2/prospective-phase-r.yaml"
NEW_MOUNT = "/var/lib/binance-bot-2/prospective-r"
NEW_UUID = os.environ.get("BINANCE_BOT_OCI_FILESYSTEM_UUID", "")
ACTIVATION_MS = 1788282532601
CELL_STEP_MS = 300_000
REQUIRED_POST_ACTIVATION_CELLS = 3
EXPECTED_SOURCE = (
    "worktree-source-v1:"
    "8f3ab5520769d71b054d489356c2fd774d31500b0deee5c3f65bc997e1c9d6e8"
)
EXPECTED_CONFIG_HASH = "cfc0bb96c41f6005d269daf7c6d44cbb850fd716d75c4180b5f3737d045ae78b"
EXPECTED_MANIFEST_HASH = "eb79b8515bad585ad7a1f89fb26983f920c09ef424ca6e870500659da99e1078"
EXPECTED_QUOTA_BYTES = 128_849_018_880
EXPECTED_SCIENTIFIC_IDENTITIES = {
    "config_sha256": "43971efece98b71f815a4e4f476e18e3467f9a8cc02de3d51b8be1c55ccf3e20",
    "shadow_policy_sha256": "e0c50d8af4bffe554798f2b24bd6177da2faa7d6bbbdf3645b73563ffe1694f0",
    "retest_policy_sha256": "eaa0951691054622e13c59ce956d01a678bc596e452e00d6e5646ee6fadb1ebd",
    "outcome_policy_sha256": "38b8e2ba8681460927dcfedc5f480cffaf61367d8fa5f536271b29ce04faf4a9",
}


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


def _json_line(stdout: str) -> dict[str, Any]:
    for line in reversed(stdout.splitlines()):
        if line.strip().startswith("{"):
            value = json.loads(line)
            if isinstance(value, dict):
                return value
    raise RuntimeError("remote activation probe did not return a JSON object")


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


def _longest_consecutive(timestamps: list[int]) -> int:
    best = current = 0
    previous: int | None = None
    for timestamp in timestamps:
        current = current + 1 if previous is not None and timestamp - previous == CELL_STEP_MS else 1
        best = max(best, current)
        previous = timestamp
    return best


def collect() -> dict[str, Any]:
    if not NEW_UUID:
        raise RuntimeError("BINANCE_BOT_OCI_FILESYSTEM_UUID is required")
    commands: dict[str, Capture] = {}
    fixed: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("remote_clock", ("date", "-u", "+%Y-%m-%dT%H:%M:%S.%3NZ")),
        ("remote_epoch_ms", ("date", "+%s%3N")),
        ("current_target", ("readlink", "-f", CURRENT)),
        ("remote_config_sha256", ("sudo", "-n", "sha256sum", NEW_CONFIG)),
        (
            "remote_manifest_sha256",
            ("sudo", "-n", "sha256sum", f"{CAMPAIGN_ROOT}/preregistration.json"),
        ),
        ("database_mount", ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,UUID,TARGET", "-T", DB_PATH)),
        ("raw_events_mount", ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,UUID,TARGET", "-T", RAW_PATH)),
        ("campaign_root_stat", ("sudo", "-n", "stat", "-c", "%n|%F|%U|%G|%a|%s", CAMPAIGN_ROOT)),
        ("campaign_db_stat", ("sudo", "-n", "stat", "-c", "%n|%F|%U|%G|%a|%s", DB_PATH)),
        ("raw_events_stat", ("sudo", "-n", "stat", "-c", "%n|%F|%U|%G|%a|%s", RAW_PATH)),
        (
            "collector_service",
            (
                "sudo",
                "-n",
                "systemctl",
                "show",
                "binance-bot-2-prospective.service",
                "--property=ActiveState,SubState,UnitFileState,MainPID,NRestarts,ExecMainStatus,Result,ExecStart,WorkingDirectory,Environment",
                "--no-pager",
            ),
        ),
    )
    for name, argv in fixed:
        commands[name] = _remote(argv)

    commands["manifest_probe"] = _remote_python(
        f"""import json
from pathlib import Path

manifest = json.loads(Path({CAMPAIGN_ROOT!r} + "/preregistration.json").read_text())
print(json.dumps({{
    "schema_version": manifest.get("schema_version"),
    "campaign_id": manifest.get("campaign_id"),
    "campaign_mode": manifest.get("campaign_mode"),
    "activation_ms": manifest.get("activation_ms"),
    "source_identity": manifest.get("source_identity"),
    "config": manifest.get("config"),
    "scientific_identities": manifest.get("scientific_identities"),
    "markets": manifest.get("markets"),
    "primary_interval": manifest.get("primary_interval"),
    "storage": manifest.get("storage"),
}}, sort_keys=True))
"""
    )

    commands["database_probe"] = _remote_python(
        f"""import json
import sqlite3
import time
from pathlib import Path
from signalbot.config import load_settings
from signalbot.prospective.source_freeze import default_source_root, freeze_source

db_path = {DB_PATH!r}
raw_path = Path({RAW_PATH!r})
settings = load_settings({NEW_CONFIG!r})
freeze = freeze_source(default_source_root())

def _longest(values):
    best = current = 0
    previous = None
    for value in values:
        current = current + 1 if previous is not None and value - previous == 300000 else 1
        best = max(best, current)
        previous = value
    return best

con = sqlite3.connect(db_path)
try:
    campaign_rows = con.execute(
        "SELECT campaign_id, campaign_mode, source_identity, config_sha256, policy_sha256, activation_ms, created_at_ms, status FROM shadow_campaigns WHERE campaign_id=?",
        ({CAMPAIGN_ID!r},),
    ).fetchall()
    coverage_rows = con.execute(
        "SELECT market, decision_close_ms, status, complete FROM shadow_coverage WHERE campaign_id=? AND decision_close_ms >= ? ORDER BY decision_close_ms",
        ({CAMPAIGN_ID!r}, {ACTIVATION_MS}),
    ).fetchall()
    pre_activation_rows = con.execute(
        "SELECT COUNT(*) FROM shadow_coverage WHERE campaign_id=? AND decision_close_ms < ?",
        ({CAMPAIGN_ID!r}, {ACTIVATION_MS}),
    ).fetchone()[0]
    quick_check = con.execute("PRAGMA quick_check").fetchone()[0]
finally:
    con.close()

coverage = {{}}
for market in ("futures", "spot"):
    cells = [int(row[1]) for row in coverage_rows if row[0] == market and row[2] == "SEALED" and row[3] == 1]
    coverage[market] = {{
        "sealed_complete_cells": len(cells),
        "max_consecutive_5m_cells": _longest(cells),
        "first_completed_close_ms": min(cells) if cells else None,
        "last_completed_close_ms": max(cells) if cells else None,
    }}

print(json.dumps({{
    "captured_at_ms": int(time.time() * 1000),
    "source_identity": freeze.source_identity,
    "source_file_count": len(freeze.files),
    "campaign_rows": [list(row) for row in campaign_rows],
    "post_activation_coverage_rows": len(coverage_rows),
    "coverage": coverage,
    "pre_activation_coverage_rows": pre_activation_rows,
    "quick_check": quick_check,
    "raw_path_exists": raw_path.is_dir(),
    "raw_path_resolved": str(raw_path.resolve()),
    "config_campaign_id": settings.shadow.campaign_id,
    "config_activation_ms": settings.shadow.activation_ms,
    "config_source_identity": settings.shadow.source_identity,
    "config_storage_url": settings.storage.url,
    "config_raw_event_directory": settings.runtime.raw_event_directory,
    "config_quota_bytes": settings.runtime.raw_event_max_bytes,
}}, sort_keys=True))
"""
    )

    clock = commands["remote_epoch_ms"].stdout.strip()
    _assert(clock.isdigit(), "remote epoch probe is not numeric")
    now_ms = int(clock)
    service = _props(commands["collector_service"].stdout)
    probe = _json_line(commands["database_probe"].stdout)
    campaign_rows = probe["campaign_rows"]
    _assert(now_ms >= ACTIVATION_MS, "activation boundary has not been reached")
    _assert(commands["current_target"].stdout.strip() == RELEASE, "current release target mismatch")
    _assert(commands["remote_config_sha256"].stdout.startswith(EXPECTED_CONFIG_HASH), "remote config hash mismatch")
    _assert(commands["remote_manifest_sha256"].stdout.startswith(EXPECTED_MANIFEST_HASH), "remote manifest hash mismatch")
    manifest = _json_line(commands["manifest_probe"].stdout)
    _assert(commands["manifest_probe"].exit_code == 0, "remote preregistration manifest probe failed")
    _assert(manifest["schema_version"] == "oci_prospective_preregistration_v1", "manifest schema mismatch")
    _assert(manifest["campaign_id"] == CAMPAIGN_ID, "manifest campaign id mismatch")
    _assert(manifest["campaign_mode"] == "prospective", "manifest campaign mode mismatch")
    _assert(manifest["activation_ms"] == ACTIVATION_MS, "manifest activation mismatch")
    _assert(manifest["source_identity"] == EXPECTED_SOURCE, "manifest source mismatch")
    _assert(manifest["config"] == {"path": NEW_CONFIG, "sha256": EXPECTED_CONFIG_HASH}, "manifest config binding mismatch")
    _assert(manifest["scientific_identities"] == EXPECTED_SCIENTIFIC_IDENTITIES, "manifest scientific identities mismatch")
    _assert(manifest["markets"] == ["futures", "spot"], "manifest market binding mismatch")
    _assert(manifest["primary_interval"] == "5m", "manifest interval mismatch")
    _assert(manifest["storage"]["mode"] == "segmented_zstd_v1", "manifest storage mode mismatch")
    _assert(manifest["storage"]["quota_bytes"] == EXPECTED_QUOTA_BYTES, "manifest quota mismatch")
    _assert(manifest["storage"]["database_url"].endswith(f"{CAMPAIGN_ID}/campaign.db"), "manifest DB path mismatch")
    _assert(manifest["storage"]["raw_event_directory"] == RAW_PATH, "manifest raw path mismatch")
    _assert(commands["database_mount"].exit_code == 0, "database mount probe failed")
    _assert(commands["raw_events_mount"].exit_code == 0, "raw-events mount probe failed")
    _assert(f"{NEW_UUID} {NEW_MOUNT}" in commands["database_mount"].stdout, "database is not on the Phase-R UUID mount")
    _assert(f"{NEW_UUID} {NEW_MOUNT}" in commands["raw_events_mount"].stdout, "raw-events is not on the Phase-R UUID mount")
    _assert(commands["campaign_db_stat"].exit_code == 0, "campaign DB is missing")
    _assert(commands["raw_events_stat"].exit_code == 0, "raw-events directory is missing")
    _assert(probe["source_identity"] == EXPECTED_SOURCE, "runtime source identity mismatch")
    _assert(probe["source_file_count"] == 239, "runtime frozen source count mismatch")
    _assert(probe["config_campaign_id"] == CAMPAIGN_ID, "config campaign id mismatch")
    _assert(probe["config_activation_ms"] == ACTIVATION_MS, "config activation mismatch")
    _assert(probe["config_source_identity"] == EXPECTED_SOURCE, "config source mismatch")
    _assert(probe["config_storage_url"].endswith(f"{CAMPAIGN_ID}/campaign.db"), "config DB path mismatch")
    _assert(probe["config_raw_event_directory"].endswith(f"{CAMPAIGN_ID}/raw-events"), "config raw path mismatch")
    _assert(probe["config_quota_bytes"] == EXPECTED_QUOTA_BYTES, "config quota mismatch")
    _assert(probe["raw_path_exists"], "raw path is not a directory")
    _assert(probe["raw_path_resolved"] == RAW_PATH, "raw path resolves outside Phase-R root")
    _assert(len(campaign_rows) == 1, "expected exactly one Phase-R campaign row")
    campaign = campaign_rows[0]
    _assert(campaign[0] == CAMPAIGN_ID, "DB campaign id mismatch")
    _assert(campaign[1] == "prospective", "DB campaign mode mismatch")
    _assert(campaign[2] == EXPECTED_SOURCE, "DB campaign source mismatch")
    _assert(campaign[3] == EXPECTED_SCIENTIFIC_IDENTITIES["config_sha256"], "DB scientific config hash mismatch")
    _assert(campaign[4] == EXPECTED_SCIENTIFIC_IDENTITIES["shadow_policy_sha256"], "DB shadow policy hash mismatch")
    _assert(campaign[5] == ACTIVATION_MS, "DB campaign activation mismatch")
    _assert(probe["pre_activation_coverage_rows"] == 0, "pre-activation coverage denominator is nonzero")
    _assert(probe["quick_check"] == "ok", "campaign DB quick_check failed")
    coverage = probe["coverage"]
    for market in ("futures", "spot"):
        _assert(
            coverage[market]["max_consecutive_5m_cells"] >= REQUIRED_POST_ACTIVATION_CELLS,
            f"{market} lacks {REQUIRED_POST_ACTIVATION_CELLS} consecutive post-activation cells",
        )
    _assert(service.get("ActiveState") == "active", "collector is not active")
    _assert(service.get("SubState") == "running", "collector is not running")
    _assert(service.get("NRestarts") == "0", "collector restart count is not zero")
    _assert("prospective-phase-r.yaml" in service.get("ExecStart", ""), "collector config path is not Phase-R")
    _assert(service.get("WorkingDirectory") == CURRENT, "collector working directory is not current release")

    local_config = Path("config/prospective.causal-retest.phase-r.yaml")
    local_manifest = Path(
        "artifacts/evidence/_phase-r-preregistration-staging/"
        f"{CAMPAIGN_ID}/preregistration.json"
    )
    _assert(local_config.is_file(), "local Phase-R config is missing")
    _assert(local_manifest.is_file(), "local Phase-R manifest is missing")
    local_config_hash = _sha256(local_config)
    local_manifest_hash = _sha256(local_manifest)
    _assert(local_config_hash == EXPECTED_CONFIG_HASH, "local Phase-R config hash changed")
    _assert(local_manifest_hash == EXPECTED_MANIFEST_HASH, "local Phase-R manifest hash changed")

    captured = datetime.now(tz=UTC)
    captures = {name: capture.record() for name, capture in commands.items()}
    return {
        "schema_contract": "phase-r-activation-health-v1",
        "schema_version": "phase_r_activation_health_v1",
        "captured_at_utc": captured.isoformat(),
        "captured_at_ms": round(captured.timestamp() * 1000),
        "work_phase_id": "r9",
        "campaign_id": CAMPAIGN_ID,
        "activation_ms": ACTIVATION_MS,
        "activation_reached": now_ms >= ACTIVATION_MS,
        "source_policy_path_storage_gates": {
            "runtime_source_identity": probe["source_identity"],
            "runtime_source_file_count": probe["source_file_count"],
            "config_source_identity": probe["config_source_identity"],
            "db_source_identity": campaign[2],
            "config_sha256": local_config_hash,
            "manifest_sha256": local_manifest_hash,
            "mount_path": NEW_MOUNT,
            "mount_uuid": NEW_UUID,
            "db_path": DB_PATH,
            "raw_events_path": RAW_PATH,
            "raw_path_resolved": probe["raw_path_resolved"],
            "quota_bytes": probe["config_quota_bytes"],
            "manifest_schema_version": manifest["schema_version"],
            "manifest_campaign_mode": manifest["campaign_mode"],
            "manifest_scientific_identities": manifest["scientific_identities"],
        },
        "database_gates": {
            "registered_campaign_rows": len(campaign_rows),
            "campaign_mode": campaign[1],
            "campaign_status": campaign[7],
            "scientific_config_sha256": campaign[3],
            "shadow_policy_sha256": campaign[4],
            "pre_activation_coverage_rows": probe["pre_activation_coverage_rows"],
            "quick_check": probe["quick_check"],
        },
        "coverage_canary": {
            "cell_step_ms": CELL_STEP_MS,
            "required_consecutive_cells": REQUIRED_POST_ACTIVATION_CELLS,
            "post_activation_only": True,
            "markets": coverage,
            "three_consecutive_cells_each_market": all(
                coverage[market]["max_consecutive_5m_cells"] >= REQUIRED_POST_ACTIVATION_CELLS
                for market in ("futures", "spot")
            ),
        },
        "collector": {
            "active_state": service.get("ActiveState"),
            "sub_state": service.get("SubState"),
            "main_pid": service.get("MainPID"),
            "restart_count": service.get("NRestarts"),
            "exec_start": service.get("ExecStart"),
            "working_directory": service.get("WorkingDirectory"),
        },
        "commands": captures,
        "governance": {
            "public_binance_market_data_only": True,
            "private_binance_api_or_orders": False,
            "outcome_or_profitability_inspection": False,
            "strategy_policy_r2_shadow_retest_outcome_or_discord_semantics_change": False,
            "commit": False,
            "push": False,
        },
        "verdict": "PHASE_R_ACTIVATION_HEALTH_AND_COVERAGE_PASS",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("artifacts/evidence/phase-r-activation-health-v1.json"))
    args = parser.parse_args()
    output = args.output.resolve()
    workspace = Path.cwd().resolve()
    if not output.is_relative_to(workspace / "artifacts" / "evidence"):
        raise SystemExit("output must stay under artifacts/evidence")
    if output.exists():
        raise SystemExit("refusing to overwrite existing activation receipt")
    receipt = collect()
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "verdict": receipt["verdict"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
