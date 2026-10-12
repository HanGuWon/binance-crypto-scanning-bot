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
import tarfile
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
RATE_LIMIT_ATTEMPTS = 3
RATE_LIMIT_BACKOFF_SECONDS = 2


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


def _set_file_mode(descriptor: int, path: Path, mode: int) -> None:
    if hasattr(os, "fchmod"):
        os.fchmod(descriptor, mode)
    else:
        os.chmod(path, mode)


def _generation_record(directory: Path, utc_date: str) -> dict[str, Any]:
    terminal, start_sha, terminal_sha = _clean_terminal(directory)
    terminal_date = datetime.fromtimestamp(int(terminal["terminal_at_ms"]) / 1000, UTC)
    if terminal_date.date().isoformat() != utc_date:
        raise UploaderError("generation is outside the requested UTC day")
    return {
        "generation": directory.name,
        "terminal_at_ms": terminal["terminal_at_ms"],
        "generation_receipt_sha256": start_sha,
        "generation_terminal_sha256": terminal_sha,
        "files": _inventory(directory),
    }


def build_daily_manifest(directories: list[Path], *, utc_date: str, host: str) -> bytes:
    try:
        if datetime.strptime(utc_date, "%Y-%m-%d").date().isoformat() != utc_date:
            raise ValueError
    except ValueError as exc:
        raise UploaderError("invalid UTC bundle date") from exc
    records = [_generation_record(path, utc_date) for path in sorted(directories)]
    file_count = sum(len(record["files"]) for record in records)
    source_bytes = sum(int(item["size"]) for record in records for item in record["files"])
    return _canonical_json(
        {
            "schema": "pump_fade_v3_drive_daily_manifest_v1",
            "utc_date": utc_date,
            "host": host,
            "generation_count": len(records),
            "file_count": file_count,
            "source_bytes": source_bytes,
            "generations": records,
        }
    )


def _create_daily_tar(config: UploadConfig, *, utc_date: str, manifest: bytes) -> Path:
    manifest_value = json.loads(manifest)
    staging = config.receipt_root / "staging"
    staging.mkdir(mode=0o700, parents=True, exist_ok=True)
    if staging.is_symlink() or not staging.is_dir():
        raise UploaderError("daily staging directory is unsafe")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{utc_date}.", suffix=".tar.tmp", dir=staging
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            with tarfile.open(fileobj=output, mode="w", format=tarfile.PAX_FORMAT) as archive:
                for record in manifest_value["generations"]:
                    directory = config.capture_root / record["generation"]
                    for entry in record["files"]:
                        relative = Path(str(entry["path"]))
                        if relative.is_absolute() or ".." in relative.parts:
                            raise UploaderError("unsafe path in daily manifest")
                        source = directory / relative
                        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                        file_descriptor = os.open(source, flags)
                        with os.fdopen(file_descriptor, "rb") as input_stream:
                            info = os.fstat(input_stream.fileno())
                            if (
                                not stat.S_ISREG(info.st_mode)
                                or info.st_nlink != 1
                                or info.st_size != int(entry["size"])
                                or info.st_dev != config.capture_root.stat().st_dev
                            ):
                                raise UploaderError("unsafe or changed file in daily bundle")
                            if _sha256(source) != entry["sha256"]:
                                raise UploaderError("generation changed while bundling")
                            tar_info = tarfile.TarInfo(
                                f"{record['generation']}/{relative.as_posix()}"
                            )
                            tar_info.size = info.st_size
                            # Keep tar bytes stable when only filesystem metadata changes.
                            tar_info.mode = 0o644
                            tar_info.mtime = 0
                            archive.addfile(tar_info, input_stream)
                            if _sha256(source) != entry["sha256"]:
                                raise UploaderError("generation changed while bundling")
            output.flush()
            os.fsync(output.fileno())
        return temporary
    except (OSError, tarfile.TarError, UploaderError):
        temporary.unlink(missing_ok=True)
        raise


def _atomic_write(path: Path, data: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.parent.is_symlink() or not path.parent.is_dir():
        raise UploaderError("receipt directory is unsafe")
    if path.is_symlink():
        raise UploaderError("receipt path is a symlink")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        _set_file_mode(descriptor, Path(temporary_name), mode)
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
        "--tpslimit",
        "2",
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
    operation = args[0] if args else "unknown"
    for attempt in range(RATE_LIMIT_ATTEMPTS):
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                check=False,
                timeout=timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise UploaderError("rclone operation could not complete") from exc
        if result.returncode == 0:
            return result.stdout
        stderr = (result.stderr or b"").lower()
        rate_limited = b"ratelimitexceeded" in stderr or b"userratelimitexceeded" in stderr
        if not rate_limited or attempt + 1 == RATE_LIMIT_ATTEMPTS:
            reason = (
                "rclone rate limit retries exhausted"
                if rate_limited
                else "rclone operation failed"
            )
            if rate_limited:
                LOGGER.critical(
                    "rclone rateLimitExceeded persisted after %d attempts; source data retained",
                    RATE_LIMIT_ATTEMPTS,
                )
            raise UploaderError(reason)
        delay = RATE_LIMIT_BACKOFF_SECONDS * (2**attempt)
        LOGGER.warning(
            "rclone rate limit; retrying operation=%s attempt=%d delay_seconds=%d",
            operation,
            attempt + 1,
            delay,
        )
        time.sleep(delay)
    raise UploaderError("rclone operation failed")


def _remote_daily_object(config: UploadConfig, utc_date: str, suffix: str) -> str:
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", utc_date):
        raise UploaderError("unsafe daily archive date")
    return f"{config.remote}:{REMOTE_ROOT}/{config.host}/daily/{utc_date}.{suffix}"


def _remote_manifest_sha256(config: UploadConfig, remote_path: str) -> str:
    contents = _rclone(config, "cat", remote_path, timeout=120)
    return hashlib.sha256(contents).hexdigest()


def _daily_paths(receipt_root: Path, utc_date: str) -> tuple[Path, Path]:
    daily_root = receipt_root / "daily"
    return (
        daily_root / f"{utc_date}.manifest.json",
        daily_root / f"{utc_date}.receipt.json",
    )


def _load_verified_daily_bundle(
    config: UploadConfig, utc_date: str
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    manifest_path, receipt_path = _daily_paths(config.receipt_root, utc_date)
    if not manifest_path.exists() and receipt_path.exists():
        raise UploaderError("daily bundle receipt exists without its manifest")
    if not manifest_path.exists():
        return None
    manifest, manifest_bytes = _read_json(manifest_path, limit=64 * 1024 * 1024)
    if not receipt_path.exists():
        # A prior attempt may have saved the deterministic manifest before
        # failing. _upload_daily_bundle will reuse it only if current inputs match.
        return None
    receipt, _ = _read_json(receipt_path, limit=1024 * 1024)
    expected_manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    if (
        manifest.get("schema") != "pump_fade_v3_drive_daily_manifest_v1"
        or manifest.get("utc_date") != utc_date
        or manifest.get("host") != config.host
        or receipt.get("schema") != "pump_fade_v3_drive_daily_receipt_v1"
        or receipt.get("utc_date") != utc_date
        or receipt.get("host") != config.host
        or receipt.get("verified_on_drive") is not True
        or receipt.get("rclone_check") != "PASS"
        or receipt.get("manifest_sha256") != expected_manifest_sha
        or receipt.get("remote_tar") != _remote_daily_object(config, utc_date, "tar")
        or receipt.get("remote_manifest") != _remote_daily_object(config, utc_date, "manifest.json")
        or receipt.get("remote_receipt") != _remote_daily_object(config, utc_date, "receipt.json")
        or not isinstance(manifest.get("generations"), list)
        or type(manifest.get("generation_count")) is not int
        or manifest.get("generation_count") != len(manifest["generations"])
    ):
        raise UploaderError("daily bundle receipt is not verified")
    for record in manifest["generations"]:
        if (
            not isinstance(record, dict)
            or not isinstance(record.get("generation"), str)
            or not isinstance(record.get("files"), list)
        ):
            raise UploaderError("daily bundle manifest structure is invalid")
    return manifest, receipt


def _upload_daily_bundle(
    config: UploadConfig, utc_date: str, directories: list[Path]
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest_bytes = build_daily_manifest(directories, utc_date=utc_date, host=config.host)
    manifest_value = json.loads(manifest_bytes)
    manifest_path, receipt_path = _daily_paths(config.receipt_root, utc_date)
    _save_deterministic(manifest_path, manifest_bytes)
    tar_path = _create_daily_tar(config, utc_date=utc_date, manifest=manifest_bytes)
    try:
        tar_sha = _sha256(tar_path)
        tar_size = tar_path.stat(follow_symlinks=False).st_size
        remote_tar = _remote_daily_object(config, utc_date, "tar")
        remote_manifest = _remote_daily_object(config, utc_date, "manifest.json")
        remote_receipt = _remote_daily_object(config, utc_date, "receipt.json")
        _rclone(config, "copyto", str(tar_path), remote_tar, "--immutable", timeout=6 * 3600)
        _rclone(
            config,
            "check",
            str(tar_path),
            remote_tar,
            "--checksum",
            "--one-way",
            timeout=6 * 3600,
        )
        _rclone(
            config,
            "copyto",
            str(manifest_path),
            remote_manifest,
            "--immutable",
            timeout=1800,
        )
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
        if _remote_manifest_sha256(config, remote_manifest) != manifest_sha:
            raise UploaderError("uploaded daily manifest readback mismatch")
        remote_stat = json.loads(_rclone(config, "lsjson", "--stat", remote_tar, timeout=120))
        if isinstance(remote_stat, list):
            if len(remote_stat) != 1:
                raise UploaderError("remote daily tar size is unavailable")
            remote_stat = remote_stat[0]
        if not isinstance(remote_stat, dict) or int(remote_stat.get("Size", -1)) != tar_size:
            raise UploaderError("remote daily tar size mismatch")
        receipt: dict[str, Any] = {
            "schema": "pump_fade_v3_drive_daily_receipt_v1",
            "utc_date": utc_date,
            "host": config.host,
            "remote_tar": remote_tar,
            "remote_manifest": remote_manifest,
            "remote_receipt": remote_receipt,
            "tar_sha256": tar_sha,
            "tar_size_bytes": tar_size,
            "manifest_sha256": manifest_sha,
            "generation_count": manifest_value["generation_count"],
            "file_count": manifest_value["file_count"],
            "verified_on_drive": True,
            "rclone_check": "PASS",
        }
        receipt_bytes = _canonical_json(receipt)
        candidate_fd, candidate_name = tempfile.mkstemp(
            prefix=f".{utc_date}.receipt.", dir=receipt_path.parent
        )
        candidate = Path(candidate_name)
        try:
            _set_file_mode(candidate_fd, candidate, 0o600)
            with os.fdopen(candidate_fd, "wb") as stream:
                stream.write(receipt_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            _rclone(
                config,
                "copyto",
                str(candidate),
                remote_receipt,
                "--immutable",
                timeout=1800,
            )
            _save_deterministic(receipt_path, receipt_bytes)
        except (OSError, UploaderError):
            candidate.unlink(missing_ok=True)
            raise
        finally:
            candidate.unlink(missing_ok=True)
        return manifest_value, receipt
    finally:
        tar_path.unlink(missing_ok=True)


def _save_deterministic(path: Path, data: bytes) -> None:
    if path.exists():
        if path.is_symlink() or path.read_bytes() != data:
            raise UploaderError(f"existing receipt differs: {path.name}")
        return
    _atomic_write(path, data)


def _safe_prune(
    config: UploadConfig,
    directory: Path,
    manifest: dict[str, Any],
    receipt: dict[str, Any],
    *,
    now_ms: int | None = None,
) -> bool:
    terminal, _, _ = _clean_terminal(directory)
    current_ms = int(time.time() * 1000) if now_ms is None else now_ms
    cutoff_ms = current_ms - config.keep_days * 86_400_000
    if int(terminal["terminal_at_ms"]) > cutoff_ms:
        return False
    terminal_at = datetime.fromtimestamp(
        int(terminal["terminal_at_ms"]) / 1000, UTC
    )
    utc_date = terminal_at.date().isoformat()
    generation_record = next(
        (
            item
            for item in manifest.get("generations", [])
            if item.get("generation") == directory.name
        ),
        None,
    )
    if (
        manifest.get("schema") != "pump_fade_v3_drive_daily_manifest_v1"
        or manifest.get("utc_date") != utc_date
        or manifest.get("host") != config.host
        or receipt.get("schema") != "pump_fade_v3_drive_daily_receipt_v1"
        or receipt.get("utc_date") != utc_date
        or receipt.get("host") != config.host
        or receipt.get("verified_on_drive") is not True
        or receipt.get("rclone_check") != "PASS"
        or receipt.get("manifest_sha256") != hashlib.sha256(_canonical_json(manifest)).hexdigest()
        or receipt.get("remote_tar") != _remote_daily_object(config, utc_date, "tar")
        or receipt.get("remote_manifest") != _remote_daily_object(config, utc_date, "manifest.json")
        or receipt.get("remote_receipt") != _remote_daily_object(config, utc_date, "receipt.json")
        or generation_record is None
        or generation_record.get("files") != _inventory(directory)
    ):
        raise UploaderError("prune lacks matching verified daily bundle receipt")
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
    if before != after or after != generation_record.get("files"):
        raise UploaderError("quarantined generation changed; preserved for recovery")
    shutil.rmtree(quarantine)
    LOGGER.info("pruned verified daily generation: %s", directory.name)
    return True


def _record_failure(receipt_root: Path, failed: bool) -> None:
    state_path = receipt_root / "uploader-state.json"
    try:
        state, _ = _read_json(state_path, limit=65536)
    except UploaderError:
        if state_path.exists() or state_path.is_symlink():
            raise
        state = {"consecutive_failed_runs": 0}
    prior_count = int(state.get("consecutive_failed_runs", state.get("consecutive_failed_days", 0)))
    count = prior_count + 1 if failed else 0
    _atomic_write(state_path, _canonical_json({"consecutive_failed_runs": count}), mode=0o600)
    if count >= 3:
        LOGGER.critical(
            "Drive upload failed for at least three consecutive runs; source data retained"
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
    today = datetime.now(UTC).date()
    by_date: dict[str, list[Path]] = {}
    failures = 0
    for directory in sorted(config.capture_root.iterdir()):
        if not directory.is_dir() or not GENERATION_RE.fullmatch(directory.name):
            continue
        terminal_path = directory / "generation-terminal.json"
        if not terminal_path.exists():
            continue
        try:
            terminal, _, _ = _clean_terminal(directory)
            generation_date = datetime.fromtimestamp(
                int(terminal["terminal_at_ms"]) / 1000, UTC
            ).date()
            if generation_date < today:
                by_date.setdefault(generation_date.isoformat(), []).append(directory)
        except (OSError, UploaderError):
            # Dirty or incomplete generations are never included in daily bundles.
            LOGGER.warning("generation excluded from daily archive: %s", directory.name)

    completed_bundles = 0
    for utc_date, directories in sorted(by_date.items()):
        try:
            verified = _load_verified_daily_bundle(config, utc_date)
            if verified is None:
                manifest, receipt = _upload_daily_bundle(config, utc_date, directories)
            else:
                manifest, receipt = verified
                # Refuse to prune if new clean generations appeared after the receipt.
                current_manifest = json.loads(
                    build_daily_manifest(directories, utc_date=utc_date, host=config.host)
                )
                if current_manifest != manifest:
                    raise UploaderError(
                        "verified daily bundle no longer matches source generations"
                    )
            completed_bundles += 1
            if config.prune_enabled:
                for directory in directories:
                    _safe_prune(config, directory, manifest, receipt)
        except (OSError, UploaderError):
            failures += 1
            LOGGER.error("daily bundle upload or verification failed: %s", utc_date)
    _record_failure(config.receipt_root, failures > 0)
    return completed_bundles, failures


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
    LOGGER.info("run complete: verified_daily_bundles=%d failures=%d", uploaded, failures)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
