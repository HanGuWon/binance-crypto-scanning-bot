"""Upload only closed, hash-verified Pump Fade v3 generations to Google Drive."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import stat
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("pump_fade_v3_uploader")
GENERATION_RE = re.compile(r"^capture-[0-9a-f]{32}-g[0-9]{4,}$")
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
CAPTURE_ROOT = Path("/var/lib/binance-bot-2/prospective-r/pump-fade-v3/capture")
RECEIPT_ROOT = Path("/var/lib/binance-bot-2/prospective-r/pump-fade-v3/archive-receipts")
REMOTE_ROOT = "TradingResearch/BINANCE_BOT_2_ARCHIVE/pump-fade-v3-capture"
KEEP_DAYS = 7


class UploaderError(RuntimeError):
    """Raised when a generation or remote verification cannot be trusted."""


@dataclass(frozen=True)
class UploadConfig:
    capture_root: Path
    receipt_root: Path
    rclone_config: Path
    remote: str
    host: str
    prune_enabled: bool
    keep_days: int = KEEP_DAYS


def _sha256(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise UploaderError(f"unsafe generation file: {path.name}") from exc
    digest = hashlib.sha256()
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise UploaderError(f"unsafe generation file: {path.name}")
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _raise_walk_error(error: OSError) -> None:
    raise error


def _inventory(directory: Path) -> list[dict[str, object]]:
    if directory.is_symlink() or not directory.is_dir():
        raise UploaderError("generation path is not a real directory")
    if (directory / ".prune-staging").exists():
        raise UploaderError("generation has a pending prune quarantine")
    root_device = directory.stat(follow_symlinks=False).st_dev
    entries: list[dict[str, object]] = []
    for root, dirs, files in os.walk(
        directory,
        topdown=True,
        onerror=_raise_walk_error,
        followlinks=False,
    ):
        current = Path(root)
        for name in list(dirs):
            path = current / name
            info = path.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_dev != root_device:
                raise UploaderError("unsafe directory in generation")
        for name in files:
            path = current / name
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_dev != root_device:
                raise UploaderError("unsafe file in generation")
            relative = path.relative_to(directory).as_posix()
            entries.append({"path": relative, "size": info.st_size, "sha256": _sha256(path)})
    return sorted(entries, key=lambda item: str(item["path"]))


def _read_json(path: Path, limit: int = 1024 * 1024) -> tuple[dict[str, Any], bytes]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise UploaderError(f"unsafe or unreadable receipt: {path.name}") from exc
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
            raise UploaderError(f"unsafe or oversized receipt: {path.name}")
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise UploaderError(f"unsafe or oversized receipt: {path.name}")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise UploaderError(f"invalid receipt: {path.name}") from exc
    if not isinstance(value, dict):
        raise UploaderError(f"invalid receipt object: {path.name}")
    return value, raw


def _clean_terminal(directory: Path) -> tuple[dict[str, Any], str, str]:
    name = directory.name
    if not GENERATION_RE.fullmatch(name):
        raise UploaderError("unsafe generation name")
    terminal, _ = _read_json(directory / "generation-terminal.json")
    start_sha = _sha256(directory / "generation-receipt.json")
    terminal_sha = _sha256(directory / "generation-terminal.json")
    if (
        terminal.get("schema") != "pump_fade_v3_generation_terminal_v1"
        or terminal.get("status") != "CLEAN_CLOSED"
        or terminal.get("recovered_crash") is not False
        or terminal.get("archive_clean") is not True
        or terminal.get("generation_directory") != name
        or terminal.get("start_sha256") != start_sha
        or not SHA256_RE.fullmatch(str(terminal.get("plan_sha256", "")))
        or not isinstance(terminal.get("process_boot_id"), str)
        or type(terminal.get("terminal_at_ms")) is not int
        or not isinstance(terminal.get("compressed_bytes_by_stream"), dict)
    ):
        raise UploaderError("generation terminal receipt is not clean or identity-bound")
    return terminal, start_sha, terminal_sha


def _canonical_json(value: object) -> bytes:
    serialized = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return (serialized + "\n").encode()


def build_generation_manifest(directory: Path) -> bytes:
    terminal, start_sha, terminal_sha = _clean_terminal(directory)
    return _canonical_json(
        {
            "schema": "pump_fade_v3_drive_manifest_v1",
            "generation": directory.name,
            "terminal_at_ms": terminal["terminal_at_ms"],
            "generation_receipt_sha256": start_sha,
            "generation_terminal_sha256": terminal_sha,
            "files": _inventory(directory),
        }
    )


def _atomic_write(path: Path, data: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.exists() and path.is_symlink():
        raise UploaderError("receipt path is a symlink")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    except OSError:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _rclone(config: UploadConfig, *args: str, timeout: int = 3600) -> bytes:
    command = [
        "rclone",
        "--config",
        str(config.rclone_config),
        "--transfers",
        "2",
        "--checkers",
        "2",
        "--bwlimit",
        "4M",
        "--retries",
        "3",
        "--low-level-retries",
        "5",
        "--timeout",
        "10m",
        "--contimeout",
        "30s",
        *args,
    ]
    try:
        result = subprocess.run(command, capture_output=True, check=False, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise UploaderError("rclone operation could not complete") from exc
    if result.returncode != 0:
        raise UploaderError("rclone operation failed")
    return result.stdout


def _remote_generation(config: UploadConfig, generation: str) -> str:
    if not GENERATION_RE.fullmatch(generation):
        raise UploaderError("unsafe remote generation path")
    return f"{config.remote}:{REMOTE_ROOT}/{config.host}/{generation}"


def _remote_sha256(config: UploadConfig, remote_path: str) -> str:
    contents = _rclone(config, "cat", remote_path, timeout=120)
    return hashlib.sha256(contents).hexdigest()


def _save_deterministic(path: Path, data: bytes) -> None:
    if path.exists():
        if path.is_symlink() or path.read_bytes() != data:
            raise UploaderError(f"existing receipt differs: {path.name}")
        return
    _atomic_write(path, data)


def _verify_and_receipt(config: UploadConfig, directory: Path, manifest: bytes) -> dict[str, Any]:
    remote = _remote_generation(config, directory.name)
    manifest_path = config.receipt_root / "manifests" / f"{directory.name}.json"
    _save_deterministic(manifest_path, manifest)
    _rclone(config, "copy", str(directory), remote, "--immutable", timeout=6 * 3600)
    _rclone(config, "check", str(directory), remote, "--checksum", "--one-way", timeout=6 * 3600)
    manifest_remote = f"{remote}/SHA256SUMS.json"
    _rclone(config, "copyto", str(manifest_path), manifest_remote, "--immutable", timeout=600)
    manifest_sha = hashlib.sha256(manifest).hexdigest()
    if _remote_sha256(config, manifest_remote) != manifest_sha:
        raise UploaderError("uploaded SHA-256 manifest readback mismatch")
    terminal, start_sha, terminal_sha = _clean_terminal(directory)
    receipt: dict[str, Any] = {
        "schema": "pump_fade_v3_drive_upload_receipt_v1",
        "generation": directory.name,
        "host": config.host,
        "remote_path": remote,
        "verified_on_drive": True,
        "rclone_check": "PASS",
        "manifest_sha256": manifest_sha,
        "generation_receipt_sha256": start_sha,
        "generation_terminal_sha256": terminal_sha,
        "terminal_at_ms": terminal["terminal_at_ms"],
        "verified_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    receipt_path = config.receipt_root / f"{directory.name}.json"
    if receipt_path.exists():
        existing, existing_raw = _read_json(receipt_path)
        if (
            existing.get("manifest_sha256") != manifest_sha
            or existing.get("verified_on_drive") is not True
            or existing.get("remote_path") != remote
            or existing.get("generation") != directory.name
            or existing.get("host") != config.host
        ):
            raise UploaderError("local upload receipt conflicts with generation")
        receipt = existing
        receipt_bytes = existing_raw
    else:
        receipt_bytes = _canonical_json(receipt)
        _atomic_write(receipt_path, receipt_bytes)
    remote_receipt = f"{remote}/UPLOAD_RECEIPT.json"
    _rclone(config, "copyto", str(receipt_path), remote_receipt, "--immutable", timeout=600)
    if _remote_sha256(config, remote_receipt) != hashlib.sha256(receipt_bytes).hexdigest():
        raise UploaderError("uploaded receipt readback mismatch")
    return receipt


def _safe_prune(
    config: UploadConfig,
    directory: Path,
    manifest: bytes,
    receipt: dict[str, Any],
) -> None:
    manifest_value, _ = _read_json(config.receipt_root / "manifests" / f"{directory.name}.json")
    terminal, _, _ = _clean_terminal(directory)
    cutoff_ms = int((time.time() - config.keep_days * 86400) * 1000)
    if int(terminal["terminal_at_ms"]) >= cutoff_ms:
        return
    if (
        receipt.get("verified_on_drive") is not True
        or receipt.get("rclone_check") != "PASS"
        or receipt.get("manifest_sha256") != hashlib.sha256(manifest).hexdigest()
        or manifest_value.get("generation") != directory.name
    ):
        raise UploaderError("prune lacks matching verified Drive receipt")
    root_device = directory.parent.stat(follow_symlinks=False).st_dev
    quarantine_root = directory.parent / ".prune-staging"
    quarantine_root.mkdir(mode=0o700, exist_ok=True)
    if (
        quarantine_root.is_symlink()
        or quarantine_root.stat(follow_symlinks=False).st_dev != root_device
    ):
        raise UploaderError("unsafe prune quarantine")
    quarantine = quarantine_root / directory.name
    if quarantine.exists():
        raise UploaderError("prune quarantine already exists")
    before = _inventory(directory)
    os.rename(directory, quarantine)
    after = _inventory(quarantine)
    if before != after or after != manifest_value.get("files"):
        raise UploaderError("quarantined generation changed; preserved for recovery")
    shutil.rmtree(quarantine)


def _record_failure(receipt_root: Path, failed: bool) -> None:
    state_path = receipt_root / "uploader-state.json"
    try:
        state, _ = _read_json(state_path, limit=65536)
    except FileNotFoundError:
        state = {"consecutive_failed_days": 0}
    count = int(state.get("consecutive_failed_days", 0)) + 1 if failed else 0
    _atomic_write(state_path, _canonical_json({"consecutive_failed_days": count}), mode=0o600)
    if count >= 3:
        LOGGER.critical(
            "Drive upload failed for at least three consecutive daily runs; data retained"
        )


def load_config(path: Path) -> UploadConfig:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise UploaderError("uploader config is unreadable") from exc
    if not isinstance(raw, dict):
        raise UploaderError("uploader config must be an object")
    host = raw.get("host")
    if host != "trendguard-osaka-a1":
        raise UploaderError("uploader host does not match the verified A1 VM")
    capture_root = Path(str(raw.get("capture_root", CAPTURE_ROOT)))
    receipt_root = Path(str(raw.get("receipt_root", RECEIPT_ROOT)))
    rclone_config = Path(str(raw.get("rclone_config", "/etc/pump-fade-v3/rclone.conf")))
    if capture_root != CAPTURE_ROOT or receipt_root != RECEIPT_ROOT:
        raise UploaderError("uploader paths differ from the dedicated DATA paths")
    if rclone_config != Path("/etc/pump-fade-v3/rclone.conf"):
        raise UploaderError("unexpected rclone config path")
    try:
        config_info = rclone_config.lstat()
    except OSError as exc:
        raise UploaderError("rclone config is unavailable") from exc
    if (
        not stat.S_ISREG(config_info.st_mode)
        or config_info.st_nlink != 1
        or config_info.st_uid != os.geteuid()
        or stat.S_IMODE(config_info.st_mode) != 0o600
    ):
        raise UploaderError("rclone config ownership or mode is unsafe")
    return UploadConfig(
        capture_root=capture_root,
        receipt_root=receipt_root,
        rclone_config=rclone_config,
        remote=str(raw.get("remote", "ocidrive")),
        host=host,
        prune_enabled=raw.get("prune_enabled") is True,
        keep_days=int(raw.get("keep_days", KEEP_DAYS)),
    )


def validate_data_mount(capture_root: Path) -> None:
    if capture_root != CAPTURE_ROOT or capture_root.is_symlink() or not capture_root.is_dir():
        raise UploaderError("capture root is unavailable or unsafe")
    result = subprocess.run(
        ["findmnt", "-T", str(capture_root), "-n", "-o", "TARGET"],
        capture_output=True,
        check=False,
        text=True,
        timeout=10,
    )
    if result.returncode != 0 or result.stdout.strip() != "/var/lib/binance-bot-2/prospective-r":
        raise UploaderError("capture root is not on the DATA filesystem")


def run_once(config: UploadConfig) -> tuple[int, int]:
    if config.remote != "ocidrive" or config.keep_days != KEEP_DAYS:
        raise UploaderError("unsafe remote or retention setting")
    if (
        config.receipt_root.is_symlink()
        or config.receipt_root.resolve(strict=False) != RECEIPT_ROOT
    ):
        raise UploaderError("receipt root is unsafe")
    config.receipt_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    uploaded = 0
    failures = 0
    for directory in sorted(config.capture_root.iterdir()):
        if not directory.is_dir() or not GENERATION_RE.fullmatch(directory.name):
            continue
        try:
            manifest = build_generation_manifest(directory)
        except (OSError, UploaderError):
            terminal_path = directory / "generation-terminal.json"
            if terminal_path.exists():
                LOGGER.error("generation is not eligible for upload: %s", directory.name)
            continue
        try:
            receipt = _verify_and_receipt(config, directory, manifest)
            if config.prune_enabled:
                _safe_prune(config, directory, manifest, receipt)
            uploaded += 1
        except (OSError, UploaderError):
            failures += 1
            LOGGER.error("generation upload or verification failed: %s", directory.name)
    _record_failure(config.receipt_root, failures > 0)
    return uploaded, failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        config = load_config(args.config)
        validate_data_mount(config.capture_root)
        uploaded, failures = run_once(config)
    except (OSError, UploaderError, subprocess.TimeoutExpired):
        LOGGER.error("uploader preflight failed")
        return 2
    LOGGER.info("run complete: verified_generations=%d failures=%d", uploaded, failures)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
