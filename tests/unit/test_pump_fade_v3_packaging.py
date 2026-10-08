"""Static contract tests: packaging is inert until a separate operator stage."""

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_embedded_prune_executes_clean_and_fails_closed(tmp_path: Path) -> None:
    script = (ROOT / "scripts/oci_capture/pull_verified_segments.ps1").read_text(
        encoding="utf-8"
    )
    match = re.search(r"(?s)\$pruneHelper = @'\n(.*?)\n'@", script)
    assert match is not None
    helper = match.group(1)
    compile(helper, "remote-prune", "exec")
    if os.name != "posix":
        pytest.skip("remote prune uses Linux openat/O_NOFOLLOW directory-fd semantics")
    # Execution uses a temp root; do not replace the production root in the script.
    helper = helper.replace(
        'assert root == "/var/lib/binance-bot-2/prospective-r/pump-fade-v3/capture"',
        f'assert root == {str(tmp_path)!r}',
    )
    generation = "capture-testboot-g0001"
    folder = tmp_path / generation
    folder.mkdir()
    name = "segment.jsonl.gz"
    raw = b"immutable compressed segment fixture"
    (folder / name).write_bytes(raw)
    sha = hashlib.sha256(raw).hexdigest()
    manifest = {
        "data_file": name,
        "sha256": sha,
        "last_received_at_ms": 1,
    }
    manifest_name = name + ".manifest.json"
    manifest_raw = json.dumps(manifest).encode()
    (folder / manifest_name).write_bytes(manifest_raw)
    manifest_sha = hashlib.sha256(manifest_raw).hexdigest()
    start_raw = b'{"schema":"pump_fade_v3_generation_v1"}'
    (folder / "generation-receipt.json").write_bytes(start_raw)
    start_sha = hashlib.sha256(start_raw).hexdigest()
    terminal = {
        "schema": "pump_fade_v3_generation_terminal_v1",
        "status": "CLEAN_CLOSED",
        "recovered_crash": False,
        "archive_clean": True,
        "generation_directory": generation,
        "start_sha256": start_sha,
        "plan_sha256": "a" * 64,
        "process_boot_id": "testboot-g1",
        "terminal_at_ms": 2,
        "compressed_bytes_by_stream": {},
    }
    terminal_path = folder / "generation-terminal.json"

    def invoke() -> subprocess.CompletedProcess[str]:
        terminal_raw = json.dumps(terminal).encode()
        terminal_path.write_bytes(terminal_raw)
        args = [
            sys.executable, "-c", helper, str(tmp_path), generation, name,
            manifest_name, sha, manifest_sha, start_sha,
            hashlib.sha256(terminal_raw).hexdigest(),
        ]
        return subprocess.run(args, capture_output=True, text=True, timeout=10, check=False)

    terminal["archive_clean"] = False
    rejected = invoke()
    assert rejected.returncode != 0
    assert (folder / name).exists()
    assert (folder / manifest_name).exists()
    terminal["archive_clean"] = True
    passed = invoke()
    assert passed.returncode == 0, passed.stderr
    assert passed.stdout.strip() == "PRUNED_VERIFIED"
    assert not (folder / name).exists()
    assert not (folder / manifest_name).exists()
    assert (folder / "generation-terminal.json").exists()


def test_service_isolated_and_not_installable() -> None:
    service = (ROOT / "deploy/systemd/pump-fade-v3-collector.service").read_text(
        encoding="utf-8"
    )
    for declaration in (
        "User=pumpfadev3",
        "Group=pumpfadev3",
        "MemoryMax=2G",
        "CPUQuota=60%",
        "Nice=10",
        "IOWeight=50",
        "Restart=no",
        "ReadWritePaths=/var/lib/binance-bot-2/prospective-r/pump-fade-v3",
        "ProtectSystem=strict",
        "WorkingDirectory=/opt/pump-fade-v3",
        "ExecStart=/opt/pump-fade-v3/.venv/bin/python -m signalbot.pump_fade_v3.collector "
        "--config config/pump_fade_v3/collector.json --enable "
        "--data-mount /var/lib/binance-bot-2/prospective-r",
    ):
        assert declaration in service
    assert "[Install]" not in service
    assert "WantedBy=" not in service
    assert "systemctl" not in service
    assert "signalbot.service" not in service


def test_pc_pull_is_opt_in_and_hash_gated() -> None:
    script = (ROOT / "scripts/oci_capture/pull_verified_segments.ps1").read_text(
        encoding="utf-8"
    )
    assert "[switch]$Execute" in script
    assert "[switch]$PruneVerified" in script
    assert "if ($PruneVerified -and -not $Execute)" in script
    assert r"D:\Binance-trade-analysis\oci-capture" in script
    assert "last_received_at_ms" in script
    assert "$cutoffMs" in script
    assert "Get-RemoteDigest" in script
    assert script.count("Get-FileHash") >= 4
    assert 'Get-RemoteDigest "$generation/$segmentName"' in script
    assert "Invoke-RemoteVerifiedPrune" in script
    assert "os.rename(data_name" in script
    assert "os.rename(manifest_name" in script
    assert "digest_file(quarantine_fd, data_name)" in script
    assert "post-rename hash mismatch; quarantine retained" in script
    assert "os.O_NOFOLLOW" in script
    assert "generation-terminal.json" in script
    assert "generation-receipt.json" in script
    assert "CLEAN_CLOSED" in script
    assert "recovered_crash" in script
    assert "generationDestination" in script
    assert "Get-RemoteDigest \"$generation/$segmentName\"" in script
    assert "src_dir_fd=generation_fd" in script
    assert "os.unlink(data_name, dir_fd=quarantine_fd)" in script
    assert "rm -- '$segmentName' '$name'" not in script
    assert "test ! -L" in script
    assert "ReparsePoint" in script
    assert "-maxdepth 1" in script
    assert "SKIP incomplete/live generation" in script
    assert "SKIP invalid generation terminal" in script
    assert "Local prune evidence changed after initial verification." in script
    assert "@($localData, $remoteDataHash)" in script
    assert "@($localManifest, $remoteManifestHash)" in script
    assert "@($localStart, $startHash)" in script
    assert "@($localTerminal, $terminalHash)" in script


def test_runbook_documents_staging_gates() -> None:
    runbook = (ROOT / "docs/pump_fade_v3/COLLECTOR_RUNBOOK.md").read_text(
        encoding="utf-8"
    )
    for required in (
        "two-hour",
        "22.77%",
        "24 hours",
        "2.0 GB/day",
        "depth5@500ms",
        "Billing",
        "reclaim",
        "dry-run",
        "run_standalone",
        "generation-terminal.json",
        "pump_fade_v3_generation_terminal_v1",
        "RECOVERED_CRASH_INCOMPLETE",
        "CLEAN_CLOSED",
        "512 MiB",
        "permanently trips",
    ):
        assert required in runbook
    assert "lacks the terminal receipt producer" not in runbook
    assert "no documented standalone executable runtime entrypoint" not in runbook


def test_collector_service_config_and_data_mount_agree() -> None:
    from signalbot.pump_fade_v3.config import DEFAULT_DATA_MOUNT, load_collector_config

    relative_config = Path("config/pump_fade_v3/collector.json")
    config = load_collector_config(ROOT / relative_config)
    service = (ROOT / "deploy/systemd/pump-fade-v3-collector.service").read_text(
        encoding="utf-8"
    )
    assert config.enabled is False
    assert config.data_directory == DEFAULT_DATA_MOUNT / "pump-fade-v3"
    assert f"--config {relative_config.as_posix()} --enable" in service
    assert f"--data-mount {DEFAULT_DATA_MOUNT.as_posix()}" in service
    assert f"ReadWritePaths={config.data_directory.as_posix()}" in service
    assert "WorkingDirectory=/opt/pump-fade-v3" in service
