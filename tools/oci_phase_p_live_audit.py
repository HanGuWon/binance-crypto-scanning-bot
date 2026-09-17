# ruff: noqa: E501
"""Fixed-template, stdout-only SSH collector for the Phase-P OCI audit."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import time
from dataclasses import asdict, dataclass
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
APP_ROOT = "/opt/binance-bot-2/app"
PYTHON = f"{APP_ROOT}/.venv/bin/python"
CAMPAIGN_ID = "causal-retest-prospective-phase-o-oci-20260827-v1"
CAMPAIGN_ROOT = f"/var/lib/binance-bot-2/prospective/{CAMPAIGN_ID}"
DB_PATH = f"{CAMPAIGN_ROOT}/campaign.db"
TAPE_ROOT = f"{CAMPAIGN_ROOT}/raw-events"
CONFIG_PATH = "/etc/binance-bot-2/prospective.yaml"
ACTIVATION_MS = 1_787_832_063_819
EXPECTED_SOURCE = (
    "worktree-source-v1:"
    "650fe603f79812e79cd9390c0d2e94fbab8071a4eab1cc95f40fc229c7f403dd"
)
UNITS = ("binance-bot-2-prospective.service", "signalbot.service")

FREEZE_AND_IDENTITIES = """
import json, sys
sys.path.insert(0, "/opt/binance-bot-2/app/tools")
from oci_preregistration import _scientific_identities
from signalbot.config import load_settings
from signalbot.prospective.source_freeze import default_source_root, freeze_source
freeze = freeze_source(default_source_root())
settings = load_settings("/etc/binance-bot-2/prospective.yaml")
print(json.dumps({"freeze": freeze.as_dict(), "source_identity": freeze.source_identity, "scientific_identities": _scientific_identities(settings)}, sort_keys=True))
""".strip()

SQLITE_READONLY = """
import json, sqlite3
db = "/var/lib/binance-bot-2/prospective/causal-retest-prospective-phase-o-oci-20260827-v1/campaign.db"
conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
conn.execute("PRAGMA query_only=ON")
quick = conn.execute("PRAGMA quick_check").fetchone()[0]
query_only = conn.execute("PRAGMA query_only").fetchone()[0]
campaign = conn.execute("SELECT campaign_id, source_identity FROM shadow_campaigns WHERE campaign_id=?", ("causal-retest-prospective-phase-o-oci-20260827-v1",)).fetchone()
coverage = conn.execute("SELECT market, status, complete, COUNT(*), MIN(decision_close_ms), MAX(decision_close_ms) FROM shadow_coverage WHERE campaign_id=? GROUP BY market, status, complete ORDER BY market, status, complete", ("causal-retest-prospective-phase-o-oci-20260827-v1",)).fetchall()
open_rows = conn.execute("SELECT market, COUNT(*) FROM shadow_coverage WHERE campaign_id=? AND (status != 'SEALED' OR complete != 1) GROUP BY market ORDER BY market", ("causal-retest-prospective-phase-o-oci-20260827-v1",)).fetchall()
conn.close()
print(json.dumps({"quick_check": quick, "query_only": query_only, "campaign": campaign, "coverage": coverage, "open_or_incomplete": open_rows}, sort_keys=True))
""".strip()

SEGMENT_INVENTORY = """
import hashlib, json
from pathlib import Path
root = Path("/var/lib/binance-bot-2/prospective/causal-retest-prospective-phase-o-oci-20260827-v1/raw-events")
result = {"markets": {}}
for market in ("futures", "spot"):
    directory = root / market
    files = []
    for path in sorted(directory.iterdir()):
        if path.is_file():
            stat = path.stat()
            files.append({"name": path.name, "size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "inode": stat.st_ino})
    manifests = []
    previous = None
    errors = []
    for manifest_path in sorted(directory.glob("*.manifest.json")):
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        data_path = directory / manifest_path.name.replace(".manifest.json", ".jsonl.zst")
        actual = hashlib.sha256(data_path.read_bytes()).hexdigest() if data_path.is_file() else None
        declared = payload.get("compressed_file_sha256")
        if actual != declared:
            errors.append({"manifest": manifest_path.name, "error": "data_hash", "actual": actual, "declared": declared})
        if payload.get("previous_segment_sha256") != previous:
            errors.append({"manifest": manifest_path.name, "error": "previous_hash", "actual": payload.get("previous_segment_sha256"), "expected": previous})
        previous = actual
        manifests.append({"name": manifest_path.name, "manifest_bytes": manifest_path.stat().st_size, "bucket_start_ms": payload.get("bucket_start_ms"), "segment_sequence": payload.get("segment_sequence"), "first_received_at_ms": payload.get("first_received_at_ms"), "last_received_at_ms": payload.get("last_received_at_ms"), "compressed_bytes": payload.get("compressed_bytes"), "uncompressed_bytes": payload.get("uncompressed_bytes"), "record_count": payload.get("record_count"), "storage_schema_version": payload.get("storage_schema_version"), "source_identity": payload.get("source_identity"), "campaign_id": payload.get("campaign_id"), "market": payload.get("market")})
    result["markets"][market] = {"files": files, "manifests": manifests, "chain_errors": errors}
print(json.dumps(result, sort_keys=True))
""".strip()

PROBE_BODIES = {
    "freeze_and_identities": FREEZE_AND_IDENTITIES,
    "sqlite_readonly": SQLITE_READONLY,
    "segment_inventory": SEGMENT_INVENTORY,
}

# Updated only when the reviewed constant body changes.
PROBE_SHA256 = {
    "freeze_and_identities": "313b064e9f6df7f18285beaf52043a5f1900b9b1b5c90dcc7a10c23cbcfdae72",
    "sqlite_readonly": "820e1033991c20ffe57a8bd0e80b6e700651889c996a9885e7c0042fb25001d0",
    "segment_inventory": "e0a81f81308a447c7d89d1f45136b81d37f901df5d6147cfdfb9924219bd0cff",
}


@dataclass(frozen=True)
class RemoteCommand:
    """One immutable remote command selected by template id."""

    template_id: str
    argv: tuple[str, ...]
    probe_sha256: str | None = None


@dataclass(frozen=True)
class Capture:
    """Local capture metadata and stdout from one read-only command."""

    template_id: str
    argv: tuple[str, ...]
    probe_sha256: str | None
    started_at_utc: str
    elapsed_ms: int
    exit_code: int
    stdout_sha256: str
    stdout_bytes: int
    stdout: str
    stderr: str


def _probe_command(name: str) -> RemoteCommand:
    body = PROBE_BODIES[name]
    actual = hashlib.sha256(body.encode()).hexdigest()
    expected = PROBE_SHA256[name]
    if actual != expected:
        raise RuntimeError(f"probe body hash mismatch: {name} {actual} != {expected}")
    return RemoteCommand(
        template_id=name,
        argv=(
            "sudo",
            "-n",
            "-u",
            "binance-bot-2",
            "env",
            "PYTHONDONTWRITEBYTECODE=1",
            PYTHON,
            "-c",
            body,
        ),
        probe_sha256=actual,
    )


def static_commands() -> tuple[RemoteCommand, ...]:
    """Return the complete fixed command set that does not depend on MainPID."""

    commands: list[RemoteCommand] = []
    for unit in UNITS:
        commands.extend(
            (
                RemoteCommand(
                    f"systemd_show_{unit}",
                    (
                        "systemctl",
                        "show",
                        unit,
                        "--property=ActiveState,UnitFileState,MainPID,NRestarts,"
                        "ExecMainStartTimestamp,ExecMainStatus,Result",
                        "--no-pager",
                    ),
                ),
                RemoteCommand(
                    f"systemd_active_{unit}", ("systemctl", "is-active", unit)
                ),
                RemoteCommand(
                    f"systemd_enabled_{unit}", ("systemctl", "is-enabled", unit)
                ),
            )
        )
    commands.append(
        RemoteCommand(
            "filesystem_root",
            (
                "df",
                "-B1",
                "--output=source,fstype,size,used,avail,itotal,iused,iavail,"
                "pcent,target",
                "/",
            ),
        )
    )
    stat_paths = (
        APP_ROOT,
        CONFIG_PATH,
        CAMPAIGN_ROOT,
        DB_PATH,
        TAPE_ROOT,
        "/opt/signalbot",
        "/etc/signalbot",
        "/var/lib/signalbot",
        "/var/lib/signalbot/signalbot.db",
    )
    for index, path in enumerate(stat_paths):
        commands.append(
            RemoteCommand(
                f"stat_{index}",
                (
                    "sudo",
                    "-n",
                    "stat",
                    "-c",
                    "%n|%F|%s|%Y|%d|%i|%a|%U|%G",
                    "--",
                    path,
                ),
            )
        )
    du_paths = (
        DB_PATH,
        f"{DB_PATH}-wal",
        f"{DB_PATH}-shm",
        TAPE_ROOT,
        f"{CAMPAIGN_ROOT}/health",
        f"{CAMPAIGN_ROOT}/logs",
        "/var/log/journal",
    )
    for index, path in enumerate(du_paths):
        commands.append(
            RemoteCommand(
                f"du_{index}", ("sudo", "-n", "du", "-sb", "--", path)
            )
        )
    commands.extend(
        (
            RemoteCommand(
                "journal",
                (
                    "journalctl",
                    "-u",
                    "binance-bot-2-prospective.service",
                    "--since",
                    "@1787832063",
                    "--no-pager",
                    "--output=short-iso-precise",
                ),
            ),
            RemoteCommand(
                "initial_health",
                (
                    "sudo",
                    "-n",
                    "-u",
                    "binance-bot-2",
                    "env",
                    "PYTHONDONTWRITEBYTECODE=1",
                    PYTHON,
                    f"{APP_ROOT}/tools/oci_initial_health_check.py",
                    "--db",
                    DB_PATH,
                    "--campaign-id",
                    CAMPAIGN_ID,
                    "--tape-root",
                    TAPE_ROOT,
                    "--activation-ms",
                    str(ACTIVATION_MS),
                    "--expected-source",
                    EXPECTED_SOURCE,
                    "--service-unit",
                    "binance-bot-2-prospective.service",
                ),
            ),
            _probe_command("freeze_and_identities"),
            _probe_command("sqlite_readonly"),
            _probe_command("segment_inventory"),
        )
    )
    return tuple(commands)


def process_command(pid: int) -> RemoteCommand:
    """Create the sole PID-dependent command after digits-only validation."""

    if pid <= 0 or not str(pid).isdigit():
        raise ValueError("PID must be a positive integer")
    return RemoteCommand(
        "process",
        (
            "ps",
            "-o",
            "pid=,ppid=,etimes=,%cpu=,%mem=,rss=,args=",
            "-p",
            str(pid),
        ),
    )


def _ssh_argv(command: RemoteCommand) -> list[str]:
    remote = shlex.join(command.argv)
    return [
        "ssh",
        "-i",
        str(SSH_KEY),
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=15",
        HOST,
        remote,
    ]


def run_remote(command: RemoteCommand, *, timeout: int = 300) -> Capture:
    """Execute one reviewed command and capture stdout only on the local host."""

    allowed = {item.template_id: item for item in static_commands()}
    if command.template_id == "process":
        if command != process_command(int(command.argv[-1])):
            raise ValueError("altered process command")
    elif allowed.get(command.template_id) != command:
        raise ValueError(f"command is not an approved immutable template: {command.template_id}")
    started = datetime.now(tz=UTC)
    before = time.monotonic()
    try:
        result = subprocess.run(
            _ssh_argv(command),
            capture_output=True,
            text=True,
            timeout=timeout,
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
        stderr = f"TIMEOUT after {timeout}s\n{stderr_value}"
    elapsed_ms = round((time.monotonic() - before) * 1000)
    return Capture(
        template_id=command.template_id,
        argv=command.argv,
        probe_sha256=command.probe_sha256,
        started_at_utc=started.isoformat(),
        elapsed_ms=elapsed_ms,
        exit_code=exit_code,
        stdout_sha256=hashlib.sha256(stdout.encode()).hexdigest(),
        stdout_bytes=len(stdout.encode()),
        stdout=stdout,
        stderr=stderr,
    )


def _main_pid(captures: list[Capture]) -> int:
    target = next(
        item
        for item in captures
        if item.template_id == "systemd_show_binance-bot-2-prospective.service"
    )
    match = re.search(r"^MainPID=(\d+)$", target.stdout, flags=re.MULTILINE)
    if match is None:
        raise RuntimeError("successor MainPID missing")
    return int(match.group(1))


def collect_snapshot() -> dict[str, Any]:
    """Collect one complete fixed-template read-only snapshot."""

    captures = [
        run_remote(command, timeout=60 if command.template_id == "initial_health" else 300)
        for command in static_commands()
    ]
    captures.append(run_remote(process_command(_main_pid(captures))))
    return {
        "schema_version": "phase_p_oci_readonly_snapshot_v1",
        "captured_at_utc": datetime.now(tz=UTC).isoformat(),
        "host": HOST,
        "commands": [asdict(item) for item in captures],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    workspace = Path.cwd().resolve()
    allowed_roots = (
        workspace / "artifacts" / "_phase_p_capture",
        workspace / "health" / "_dev",
    )
    if not any(output.is_relative_to(root) for root in allowed_roots):
        raise SystemExit("local output must stay under artifacts/_phase_p_capture or health/_dev")
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise SystemExit(f"refusing to overwrite capture: {output}")
    payload = collect_snapshot()
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
