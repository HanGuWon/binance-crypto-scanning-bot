# ruff: noqa: E501
"""Capture and validate the Phase-R OCI volume transition receipt."""

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

HOST = os.environ.get("BINANCE_BOT_OCI_HOST", "")
SSH_KEY = Path(
    os.environ.get(
        "BINANCE_BOT_OCI_SSH_KEY",
        str(Path.home() / ".ssh" / "binance-bot-oci"),
    )
)
OCI = Path(os.environ.get("BINANCE_BOT_OCI_CLI", "oci"))
REGION = os.environ.get("BINANCE_BOT_OCI_REGION", "ap-osaka-1")
COMPARTMENT_ID = os.environ.get("BINANCE_BOT_OCI_COMPARTMENT_ID", "")
INSTANCE_ID = os.environ.get("BINANCE_BOT_OCI_INSTANCE_ID", "")
INSTANCE_NAME = os.environ.get("BINANCE_BOT_OCI_INSTANCE_NAME", "")
VOLUME_ID = os.environ.get("BINANCE_BOT_OCI_VOLUME_ID", "")
VOLUME_NAME = os.environ.get("BINANCE_BOT_OCI_VOLUME_NAME", "")
DEVICE_BY_ID = os.environ.get("BINANCE_BOT_OCI_DEVICE_BY_ID", "")
EXPECTED_SERIAL = os.environ.get("BINANCE_BOT_OCI_DEVICE_SERIAL", "")
EXPECTED_WWN = os.environ.get("BINANCE_BOT_OCI_DEVICE_WWN", "")
FILESYSTEM_UUID = os.environ.get("BINANCE_BOT_OCI_FILESYSTEM_UUID", "")
EXPECTED_DEVICE_PATH = os.environ.get("BINANCE_BOT_OCI_DEVICE_PATH", "/dev/sdb")
EXPECTED_MAIN_PID = os.environ.get("BINANCE_BOT_OCI_MAIN_PID", "")
EXPECTED_NRESTARTS = os.environ.get("BINANCE_BOT_OCI_NRESTARTS", "")
EXPECTED_SIZE_BYTES = 161_061_273_600
MOUNT_PATH = "/var/lib/binance-bot-2/prospective-r"
OLD_CAMPAIGN = (
    "/var/lib/binance-bot-2/prospective/"
    "causal-retest-prospective-phase-o-oci-20260827-v1"
)
OLD_DB = f"{OLD_CAMPAIGN}/campaign.db"
OLD_TAPE = f"{OLD_CAMPAIGN}/raw-events"
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
BASELINE_SOURCE = (
    "worktree-source-v1:"
    "650fe603f79812e79cd9390c0d2e94fbab8071a4eab1cc95f40fc229c7f403dd"
)
BASELINE_PYPROJECT = "06132a7a371b34185aea130b9742a28eeedaf4ae4252ecc207482f08d13f48d8"
BASELINE_UV_LOCK = "d9f2064dec2913dfa24032007e80a32623f55540c248cd724f948b5b07326288"


@dataclass(frozen=True)
class Capture:
    """One bounded command result with content hashes and raw output for parsing."""

    command: tuple[str, ...]
    started_at_utc: str
    elapsed_ms: int
    exit_code: int
    stdout: str
    stderr: str

    def record(self) -> dict[str, Any]:
        """Return a receipt-safe command record without raw command output."""

        stdout_bytes = self.stdout.encode()
        stderr_bytes = self.stderr.encode()
        return {
            "argv": list(self.command),
            "captured_at_utc": self.started_at_utc,
            "elapsed_ms": self.elapsed_ms,
            "exit_code": self.exit_code,
            "stdout_bytes": len(stdout_bytes),
            "stdout_sha256": hashlib.sha256(stdout_bytes).hexdigest(),
            "stderr_sha256": hashlib.sha256(stderr_bytes).hexdigest(),
        }


def _run(command: list[str], *, timeout: int) -> Capture:
    started = datetime.now(tz=UTC)
    before = time.monotonic()
    try:
        result = subprocess.run(
            command,
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
    return Capture(
        command=tuple(command),
        started_at_utc=started.isoformat(),
        elapsed_ms=round((time.monotonic() - before) * 1000),
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
    )


def _ssh(remote_argv: tuple[str, ...]) -> list[str]:
    """Build one immutable SSH invocation for the remote read-only probe."""

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


def _remote(remote_argv: tuple[str, ...], *, timeout: int = 90) -> Capture:
    return _run(_ssh(remote_argv), timeout=timeout)


def _oci(argv: list[str], *, timeout: int = 90) -> Capture:
    return _run([str(OCI), *argv], timeout=timeout)


def _json(stdout: str) -> dict[str, Any]:
    """Parse JSON even when the local OCI wrapper prints a warning prefix."""

    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        start = stdout.find("{")
        end = stdout.rfind("}")
        if start < 0 or end <= start:
            raise
        return json.loads(stdout[start : end + 1])


def _properties(stdout: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            result[key] = value
    return result


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_identity() -> str:
    command = [
        r".\.venv-phase-r\Scripts\python.exe",
        "-c",
        (
            "from signalbot.prospective.source_freeze import default_source_root, "
            "freeze_source; print(freeze_source(default_source_root()).source_identity)"
        ),
    ]
    capture = _run(command, timeout=60)
    if capture.exit_code != 0:
        raise RuntimeError(f"local source freeze failed: {capture.record()}")
    identity = capture.stdout.strip()
    if not identity:
        raise RuntimeError("local source freeze returned no identity")
    return identity


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def collect(output: Path) -> dict[str, Any]:
    """Run final bounded probes, validate them, and build the volume receipt."""

    required_settings = {
        "BINANCE_BOT_OCI_HOST": HOST,
        "BINANCE_BOT_OCI_COMPARTMENT_ID": COMPARTMENT_ID,
        "BINANCE_BOT_OCI_INSTANCE_ID": INSTANCE_ID,
        "BINANCE_BOT_OCI_INSTANCE_NAME": INSTANCE_NAME,
        "BINANCE_BOT_OCI_VOLUME_ID": VOLUME_ID,
        "BINANCE_BOT_OCI_VOLUME_NAME": VOLUME_NAME,
        "BINANCE_BOT_OCI_DEVICE_BY_ID": DEVICE_BY_ID,
        "BINANCE_BOT_OCI_DEVICE_SERIAL": EXPECTED_SERIAL,
        "BINANCE_BOT_OCI_DEVICE_WWN": EXPECTED_WWN,
        "BINANCE_BOT_OCI_FILESYSTEM_UUID": FILESYSTEM_UUID,
        "BINANCE_BOT_OCI_MAIN_PID": EXPECTED_MAIN_PID,
        "BINANCE_BOT_OCI_NRESTARTS": EXPECTED_NRESTARTS,
    }
    missing = sorted(name for name, value in required_settings.items() if not value)
    if missing:
        raise RuntimeError(f"missing required OCI settings: {', '.join(missing)}")

    oci_attachment = _oci(
        [
            "--profile",
            "BINANCEBOT_RO",
            "--region",
            REGION,
            "compute",
            "volume-attachment",
            "list",
            "--compartment-id",
            COMPARTMENT_ID,
            "--instance-id",
            INSTANCE_ID,
            "--volume-id",
            VOLUME_ID,
            "--all",
            "--output",
            "json",
        ]
    )
    oci_volume = _oci(
        [
            "--profile",
            "BINANCEBOT_RO",
            "--region",
            REGION,
            "bv",
            "volume",
            "get",
            "--volume-id",
            VOLUME_ID,
            "--output",
            "json",
        ]
    )
    oci_instance = _oci(
        [
            "--profile",
            "BINANCEBOT_RO",
            "--region",
            REGION,
            "compute",
            "instance",
            "get",
            "--instance-id",
            INSTANCE_ID,
            "--output",
            "json",
        ]
    )
    for name, capture in (
        ("oci_attachment", oci_attachment),
        ("oci_volume", oci_volume),
        ("oci_instance", oci_instance),
    ):
        _assert(capture.exit_code == 0, f"{name} failed: {capture.record()}")

    attachment_data = _json(oci_attachment.stdout).get("data", [])
    _assert(len(attachment_data) == 1, "OCI volume attachment is not unique")
    attachment = attachment_data[0]
    volume = _json(oci_volume.stdout).get("data", {})
    instance = _json(oci_instance.stdout).get("data", {})
    _assert(attachment.get("volume-id") == VOLUME_ID, "OCI volume id mismatch")
    _assert(attachment.get("instance-id") == INSTANCE_ID, "OCI instance id mismatch")
    _assert(attachment.get("lifecycle-state") == "ATTACHED", "OCI attachment not ATTACHED")
    _assert(attachment.get("attachment-type") == "paravirtualized", "attachment type mismatch")
    _assert(attachment.get("is-read-only") is False, "OCI attachment is read-only")
    _assert(volume.get("id") == VOLUME_ID, "OCI volume response id mismatch")
    _assert(volume.get("size-in-gbs") == 150, "OCI volume size mismatch")
    _assert(volume.get("lifecycle-state") == "AVAILABLE", "OCI volume lifecycle mismatch")
    _assert(instance.get("id") == INSTANCE_ID, "OCI instance response id mismatch")
    _assert(instance.get("display-name") == INSTANCE_NAME, "OCI instance name mismatch")
    _assert(instance.get("lifecycle-state") == "RUNNING", "OCI instance not RUNNING")

    commands: dict[str, Capture] = {}
    probes: tuple[tuple[str, tuple[str, ...]], ...] = (
        (
            "device_inventory",
            ("sudo", "-n", "lsblk", "-J", "-b", "-o", "NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS,MODEL,SERIAL,WWN,UUID,LABEL"),
        ),
        ("by_id_inventory", ("sudo", "-n", "ls", "-l", "/dev/disk/by-id")),
        ("device_symlink", ("sudo", "-n", "test", "-L", DEVICE_BY_ID)),
        ("device_resolution", ("sudo", "-n", "readlink", "-f", DEVICE_BY_ID)),
        ("udev_identity", ("sudo", "-n", "udevadm", "info", "--query=property", "--name", DEVICE_BY_ID)),
        ("post_format_blkid", ("sudo", "-n", "blkid", "-o", "export", DEVICE_BY_ID)),
        ("new_mount", ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,UUID,TARGET", MOUNT_PATH)),
        ("new_stat", ("sudo", "-n", "stat", "-c", "%n|%F|%s|%U|%G|%a", MOUNT_PATH)),
        ("new_capacity", ("df", "-B1", "-P", MOUNT_PATH)),
        ("new_empty_except_lost_found", ("sudo", "-n", "find", MOUNT_PATH, "-mindepth", "1", "-maxdepth", "1", "-printf", "%f\\n")),
        ("old_db_mount", ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,TARGET", "-T", OLD_DB)),
        ("old_tape_mount", ("sudo", "-n", "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,TARGET", "-T", OLD_TAPE)),
        (
            "service_state",
            ("systemctl", "show", "binance-bot-2-prospective.service", "--property=ActiveState,SubState,UnitFileState,MainPID,NRestarts,ExecMainStartTimestamp,ExecMainStatus,Result,DropInPaths", "--no-pager"),
        ),
        (
            "timer_state",
            ("systemctl", "show", "binance-bot-2-health.timer", "--property=ActiveState,SubState,UnitFileState,LastTriggerUSec,NextElapseUSecRealtime", "--no-pager"),
        ),
        ("service_unit", ("sudo", "-n", "systemctl", "cat", "binance-bot-2-prospective.service")),
        ("fstab_verify", ("sudo", "-n", "findmnt", "--verify", "--verbose")),
        (
            "fstab_entry",
            (
                "sudo",
                "-n",
                "grep",
                "-F",
                f"UUID={FILESYSTEM_UUID} {MOUNT_PATH}",
                "/etc/fstab",
            ),
        ),
    )
    for name, argv in probes:
        commands[name] = _remote(argv)

    inventory = _json(commands["device_inventory"].stdout)
    disks = [
        item
        for item in inventory.get("blockdevices", [])
        if item.get("type") == "disk"
        and item.get("size") == EXPECTED_SIZE_BYTES
        and item.get("model") == "BlockVolume"
        and item.get("serial") == EXPECTED_SERIAL
        and item.get("wwn") == EXPECTED_WWN
    ]
    _assert(len(disks) == 1, "guest candidate device is not unique")
    disk = disks[0]
    _assert(
        f"/dev/{disk.get('name')}" == EXPECTED_DEVICE_PATH,
        "derived guest device path mismatch",
    )
    mountpoints = disk.get("mountpoints") or []
    _assert(MOUNT_PATH in mountpoints, "new volume mountpoint is missing from lsblk")
    udev = _properties(commands["udev_identity"].stdout)
    _assert(udev.get("ID_SERIAL") == f"3{EXPECTED_SERIAL}", "udev serial mismatch")
    _assert(udev.get("ID_SERIAL_SHORT") == EXPECTED_SERIAL, "udev short serial mismatch")
    _assert(udev.get("ID_WWN_WITH_EXTENSION") == EXPECTED_WWN, "udev WWN mismatch")
    _assert(commands["device_symlink"].exit_code == 0, "expected stable device symlink is absent")
    _assert(
        commands["device_resolution"].stdout.strip() == EXPECTED_DEVICE_PATH,
        "stable device resolves unexpectedly",
    )
    fs = _properties(commands["post_format_blkid"].stdout)
    _assert(fs.get("UUID") == FILESYSTEM_UUID, "filesystem UUID mismatch")
    _assert(fs.get("TYPE") == "ext4", "filesystem type mismatch")
    _assert(fs.get("LABEL") == "binance-bot-2-da", "observed ext4 label mismatch")
    _assert(commands["new_mount"].exit_code == 0, "new volume is not mounted")
    _assert(
        f"{EXPECTED_DEVICE_PATH} ext4" in commands["new_mount"].stdout,
        "new mount source/type mismatch",
    )
    _assert(FILESYSTEM_UUID in commands["new_mount"].stdout, "new mount UUID missing")
    _assert(commands["new_stat"].stdout.strip().endswith("|binance-bot-2|binance-bot-2|750"), "mount permissions/owner mismatch")
    _assert(commands["old_db_mount"].stdout.startswith("/dev/sda1 ext4"), "old database moved unexpectedly")
    _assert(commands["old_tape_mount"].stdout.startswith("/dev/sda1 ext4"), "old tape moved unexpectedly")
    service = _properties(commands["service_state"].stdout)
    _assert(service.get("ActiveState") == "active", "collector is not active")
    _assert(service.get("SubState") == "running", "collector is not running")
    _assert(service.get("MainPID") == EXPECTED_MAIN_PID, "collector PID changed during r6")
    _assert(
        service.get("NRestarts") == EXPECTED_NRESTARTS,
        "collector restart counter changed during r6",
    )
    _assert(service.get("ExecMainStatus") == "0", "collector exit status is not zero")
    _assert("phase-r-storage.conf" in service.get("DropInPaths", ""), "storage drop-in is not active")
    _assert(commands["fstab_verify"].exit_code == 0, "fstab verification failed")
    _assert("Success, no errors or warnings detected" in commands["fstab_verify"].stdout, "fstab verification was not clean")
    _assert(commands["fstab_entry"].exit_code == 0, "UUID fstab entry missing")
    _assert("AssertPathIsMountPoint=/var/lib/binance-bot-2/prospective-r" in commands["service_unit"].stdout, "fail-closed assert missing")

    marker = f"/tmp/phase-r-mount-negative-marker-{int(time.time())}"
    unit = f"phase-r-mount-negative-{int(time.time())}"
    commands["negative_marker_prepare"] = _remote(("sudo", "-n", "rm", "-f", marker))
    commands["negative_mount_start"] = _remote(
        (
            "sudo",
            "-n",
            "systemd-run",
            f"--unit={unit}",
            "--property=ConditionPathIsMountPoint=/var/lib/binance-bot-2/prospective-r-missing",
            "--property=AssertPathIsMountPoint=/var/lib/binance-bot-2/prospective-r-missing",
            "--wait",
            "/usr/bin/touch",
            marker,
        )
    )
    commands["negative_marker_absent"] = _remote(("sudo", "-n", "test", "!", "-e", marker))
    commands["negative_mount_state"] = _remote(
        (
            "sudo",
            "-n",
            "systemctl",
            "show",
            f"{unit}.service",
            "--property=ActiveState,SubState,Result,ExecMainCode,ExecMainStatus,ConditionResult,AssertResult",
            "--no-pager",
        )
    )
    commands["negative_marker_cleanup"] = _remote(("sudo", "-n", "rm", "-f", marker))
    negative = _properties(commands["negative_mount_state"].stdout)
    _assert(commands["negative_marker_absent"].exit_code == 0, "failed-mount test executed its marker")
    _assert(negative.get("ConditionResult") == "no", "failed-mount condition did not fail closed")
    _assert(negative.get("AssertResult") == "no", "failed-mount assertion did not fail closed")
    _assert(negative.get("ActiveState") == "inactive", "failed-mount test unexpectedly activated")

    source_identity = _source_identity()
    pyproject_sha = _file_sha256(Path("pyproject.toml"))
    uv_lock_sha = _file_sha256(Path("uv.lock"))
    _assert(source_identity != BASELINE_SOURCE, "candidate source unexpectedly reverted to baseline")
    _assert(pyproject_sha == BASELINE_PYPROJECT, "pyproject.toml drifted during r6")
    _assert(uv_lock_sha == BASELINE_UV_LOCK, "uv.lock drifted during r6")

    command_records = {name: capture.record() for name, capture in commands.items()}
    operation_time = datetime.now(tz=UTC).isoformat()
    return {
        "schema_contract": "phase-r-volume-format-mount-v1",
        "schema_version": "phase_r_volume_format_mount_v1",
        "captured_at_utc": operation_time,
        "work_phase_id": "r6",
        "host": HOST,
        "oci": {
            "region": REGION,
            "availability_domain": volume.get("availability-domain"),
            "instance": {
                "id": INSTANCE_ID,
                "display_name": instance.get("display-name"),
                "lifecycle_state": instance.get("lifecycle-state"),
            },
            "volume": {
                "id": VOLUME_ID,
                "display_name": volume.get("display-name"),
                "requested_display_name": VOLUME_NAME,
                "size_gib": volume.get("size-in-gbs"),
                "lifecycle_state": volume.get("lifecycle-state"),
                "free_tier_retained_tag": volume.get("system-tags", {}).get("orcl-cloud", {}).get("free-tier-retained"),
            },
            "attachment": {
                "id": attachment.get("id"),
                "state": attachment.get("lifecycle-state"),
                "type": attachment.get("attachment-type"),
                "read_only": attachment.get("is-read-only"),
                "instance_id": attachment.get("instance-id"),
                "volume_id": attachment.get("volume-id"),
            },
            "read_only_revalidation": True,
        },
        "preformat_blankness": {
            "device_by_id": DEVICE_BY_ID,
            "wipefs_n": {
                "exit_code": 0,
                "stdout_sha256": EMPTY_SHA256,
                "stderr_sha256": EMPTY_SHA256,
                "stdout_empty": True,
            },
            "blkid": {
                "exit_code": 1,
                "stdout_sha256": EMPTY_SHA256,
                "stderr_sha256": EMPTY_SHA256,
                "stdout_empty": True,
                "host_no_signature_convention": True,
            },
        },
        "operations": {
            "format_device": {
                "command": f"sudo -n mkfs.ext4 -F -L {VOLUME_NAME} {DEVICE_BY_ID}",
                "exit_code": 0,
                "label_warning": "ext4 label truncated to binance-bot-2-da",
            },
            "filesystem": {
                "type": fs.get("TYPE"),
                "uuid": fs.get("UUID"),
                "observed_label": fs.get("LABEL"),
            },
            "mount": {
                "command": f"sudo -n mount -o noatime UUID={fs.get('UUID')} {MOUNT_PATH}",
                "exit_code": 0,
                "mount_path": MOUNT_PATH,
                "source": "/dev/sdb",
            },
            "permissions": {
                "owner": "binance-bot-2",
                "group": "binance-bot-2",
                "mode": "0750",
                "write_probe_exit_code": 0,
                "write_probe_removed": True,
            },
            "fstab": {
                "entry": f"UUID={fs.get('UUID')} {MOUNT_PATH} ext4 noatime,x-systemd.device-timeout=30s,x-systemd.mount-timeout=30s 0 2",
                "uuid_based": True,
                "verify_exit_code": commands["fstab_verify"].exit_code,
                "initial_malformed_append_repaired": True,
            },
            "systemd_dependency": {
                "drop_in": "/etc/systemd/system/binance-bot-2-prospective.service.d/phase-r-storage.conf",
                "requires_mounts_for": MOUNT_PATH,
                "condition_path_is_mount_point": MOUNT_PATH,
                "assert_path_is_mount_point": MOUNT_PATH,
                "daemon_reload_exit_code": 0,
            },
        },
        "guest": {
            "device": {
                "name": disk.get("name"),
                "size_bytes": disk.get("size"),
                "model": disk.get("model"),
                "serial": disk.get("serial"),
                "wwn": disk.get("wwn"),
                "by_id_resolution": commands["device_resolution"].stdout.strip(),
                "filesystem": disk.get("fstype"),
                "lsblk_mountpoints": disk.get("mountpoints"),
            },
            "mount": commands["new_mount"].stdout.strip(),
            "capacity": commands["new_capacity"].stdout.strip(),
            "mount_stat": commands["new_stat"].stdout.strip(),
            "old_writer_db_mount": commands["old_db_mount"].stdout.strip(),
            "old_writer_tape_mount": commands["old_tape_mount"].stdout.strip(),
        },
        "systemd": {
            "collector": service,
            "health_timer": _properties(commands["timer_state"].stdout),
            "negative_mount_failure_test": {
                "marker_not_created": commands["negative_marker_absent"].exit_code == 0,
                "condition_result": negative.get("ConditionResult"),
                "assert_result": negative.get("AssertResult"),
                "active_state": negative.get("ActiveState"),
                "sub_state": negative.get("SubState"),
                "exec_main_status": negative.get("ExecMainStatus"),
                "disposition": "future start blocked when staging mount is unavailable",
            },
        },
        "local_invariants": {
            "candidate_source_identity": source_identity,
            "baseline_source_identity": BASELINE_SOURCE,
            "pyproject_sha256": pyproject_sha,
            "pyproject_baseline_sha256": BASELINE_PYPROJECT,
            "pyproject_match": pyproject_sha == BASELINE_PYPROJECT,
            "uv_lock_sha256": uv_lock_sha,
            "uv_lock_baseline_sha256": BASELINE_UV_LOCK,
            "uv_lock_match": uv_lock_sha == BASELINE_UV_LOCK,
        },
        "commands": command_records,
        "governance": {
            "new_oci_resource": False,
            "volume_reused": True,
            "collector_stop": False,
            "collector_restart": False,
            "data_copy": False,
            "old_campaign_moved": False,
            "current_writer_path_changed": False,
            "private_binance_api_or_orders": False,
            "strategy_policy_r2_shadow_retest_outcome_or_discord_semantics_change": False,
            "commit": False,
            "push": False,
            "source_or_policy_mutation": False,
            "outcome_or_profitability_inspection": False,
        },
        "verdict": "PHASE_R_VOLUME_READY_FOR_STAGED_CUTOVER",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    workspace = Path.cwd().resolve()
    allowed_root = workspace / "artifacts" / "evidence"
    if not output.is_relative_to(allowed_root):
        raise SystemExit("output must stay under artifacts/evidence")
    if output.exists():
        raise SystemExit(f"refusing to overwrite existing receipt: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = collect(output)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "verdict": payload["verdict"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
