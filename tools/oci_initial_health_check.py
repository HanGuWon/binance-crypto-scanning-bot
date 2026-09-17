"""Read-only post-activation health gate for the OCI prospective collector."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any

from prospective_health_report_v2 import build_report

from signalbot.prospective.segment_set import build_market_segment_root
from signalbot.prospective.segmented_replay import iter_segmented_zstd

EXPECTED_EXEC = (
    "/opt/binance-bot-2/app/.venv/bin/python "
    "/opt/binance-bot-2/app/.venv/bin/signalbot run "
    "--config /etc/binance-bot-2/prospective.yaml"
)
MARKETS = ("futures", "spot")
CELL_STEP_MS = 300_000
MIN_SEGMENTS = 3
MIN_COVERAGE_CELLS = 3
MAX_LOAD_1 = 2.0
MAX_RSS_KIB = 1_572_864
MIN_MEMORY_AVAILABLE_BYTES = 2 * 1024 * 1024 * 1024
MIN_DISK_FREE_BYTES = 16 * 1024 * 1024 * 1024


def _command(*args: str) -> str:
    result = subprocess.run(args, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(args)}")
    return result.stdout.strip()


def _command_output(*args: str) -> str:
    """Return stdout even when a state query uses a non-zero status code."""
    result = subprocess.run(args, capture_output=True, text=True, check=False)
    return result.stdout.strip()


def _read_rss_kib(pid: int) -> int:
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    raise RuntimeError(f"VmRSS missing for pid {pid}")


def _matching_processes(expected: str) -> list[int]:
    matches: list[int] = []
    for proc in Path("/proc").glob("[0-9]*"):
        try:
            pid = int(proc.name)
            command = proc.joinpath("cmdline").read_bytes().replace(b"\0", b" ").decode().strip()
        except (OSError, UnicodeDecodeError, ValueError):
            continue
        if command == expected:
            matches.append(pid)
    return matches


def _coverage(db_path: Path, campaign_id: str, activation_ms: int) -> dict[str, dict[str, Any]]:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT market, decision_close_ms, status, complete "
        "FROM shadow_coverage WHERE campaign_id=? ORDER BY market, decision_close_ms",
        (campaign_id,),
    ).fetchall()
    conn.close()
    result: dict[str, dict[str, Any]] = {}
    for market in MARKETS:
        market_rows = [row for row in rows if row[0] == market]
        complete = sorted(
            row[1] for row in market_rows if row[2] == "SEALED" and row[3] == 1
        )
        post_activation = [
            timestamp for timestamp in complete if timestamp >= activation_ms
        ]
        gaps = [
            [post_activation[index - 1], post_activation[index]]
            for index in range(1, len(post_activation))
            if post_activation[index] - post_activation[index - 1] != CELL_STEP_MS
        ]
        result[market] = {
            "total_rows": len(market_rows),
            "complete_sealed_cells": len(complete),
            "post_activation_complete_sealed_cells": len(post_activation),
            "latest_decision_close_ms": complete[-1] if complete else None,
            "coverage_gap_count": len(gaps),
            "coverage_gaps": gaps,
            "pass": bool(
                len(post_activation) >= MIN_COVERAGE_CELLS
                and not gaps
                and post_activation[-1] >= activation_ms
            ),
        }
    return result


def _segment_checks(tape_root: Path, activation_ms: int) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for market in MARKETS:
        market_dir = tape_root / market
        manifests = sorted(market_dir.glob("*.manifest.json"))
        pairs: list[str] = []
        pair_errors: list[str] = []
        data_bytes = 0
        latest = None
        for manifest_path in manifests:
            data_path = market_dir / manifest_path.name.replace(
                ".manifest.json", ".jsonl.zst"
            )
            if not data_path.is_file():
                pair_errors.append(f"missing data: {data_path.name}")
                continue
            if data_path.stat().st_size <= 0:
                pair_errors.append(f"empty data: {data_path.name}")
                continue
            pairs.append(manifest_path.name)
            data_bytes += data_path.stat().st_size
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            received = payload.get("last_received_at_ms")
            if isinstance(received, int) and (latest is None or received > latest):
                latest = received
        replayed = 0
        replay_error: str | None = None
        try:
            replayed = sum(1 for _ in iter_segmented_zstd(market_dir, allow_partials=True))
            root = build_market_segment_root(market_dir)
        except Exception as exc:
            replay_error = repr(exc)
            root = None
        result[market] = {
            "manifest_count": len(manifests),
            "data_pair_count": len(pairs),
            "compressed_data_bytes": data_bytes,
            "pair_errors": pair_errors,
            "replayed_record_count": replayed,
            "replay_error": replay_error,
            "segment_root": root,
            "latest_last_received_at_ms": latest,
            "sealed_segment_pass": bool(
                len(manifests) >= MIN_SEGMENTS
                and len(pairs) == len(manifests)
                and data_bytes > 0
                and replay_error is None
                and latest is not None
                and latest >= activation_ms
            ),
        }
    return result


def build_health(
    *,
    db_path: Path,
    campaign_id: str,
    tape_root: Path,
    activation_ms: int,
    expected_source: str,
    service_unit: str,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    if now_ms < activation_ms + 900_000:
        raise RuntimeError("O4 boundary is not reached: activation_ms + 900000")
    report = build_report(
        db_path=db_path,
        campaign_id=campaign_id,
        tape_root=tape_root,
        quota_bytes=30_064_771_072,
        reserve_bytes=4_294_967_296,
    )
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    integrity = conn.execute("PRAGMA quick_check").fetchone()[0]
    campaign = conn.execute(
        "SELECT source_identity FROM shadow_campaigns WHERE campaign_id=?",
        (campaign_id,),
    ).fetchone()
    conn.close()
    segments = _segment_checks(tape_root, activation_ms)
    coverage = _coverage(db_path, campaign_id, activation_ms)
    stale_files = sorted(
        str(path.relative_to(tape_root))
        for path in tape_root.rglob("*")
        if path.is_file() and (path.name.endswith(".tmp") or path.name.endswith(".partial.tmp"))
    )
    marker_files = sorted(
        str(path.relative_to(tape_root))
        for path in tape_root.rglob("*.evidence-failure")
    )
    journal = _command(
        "journalctl",
        "-u",
        service_unit,
        "--since",
        f"@{activation_ms // 1000}",
        "--no-pager",
    )
    forbidden_markers = ["FATAL", "overflow", "recorder failure", "uncaught"]
    journal_marker_hits = [marker for marker in forbidden_markers if marker in journal]
    main_pid_text = _command_output(
        "systemctl", "show", service_unit, "-p", "MainPID", "--value"
    )
    main_pid = int(main_pid_text) if main_pid_text.isdigit() else 0
    command_matches = _matching_processes(EXPECTED_EXEC)
    load_1 = float(Path("/proc/loadavg").read_text().split()[0])
    mem_available_bytes = next(
        int(line.split()[1]) * 1024
        for line in Path("/proc/meminfo").read_text().splitlines()
        if line.startswith("MemAvailable:")
    )
    disk_free_bytes = os.statvfs("/").f_bavail * os.statvfs("/").f_frsize
    old_pid_text = _command_output(
        "systemctl", "show", "signalbot.service", "-p", "MainPID", "--value"
    )
    old_pid = int(old_pid_text) if old_pid_text.isdigit() else -1
    rss_kib = _read_rss_kib(main_pid) if main_pid > 0 else 0
    checks = {
        "post_activation_boundary": now_ms >= activation_ms + 900_000,
        "database_integrity": integrity == "ok",
        "source_identity": campaign is not None and campaign[0] == expected_source,
        "segments_both_markets": all(item["sealed_segment_pass"] for item in segments.values()),
        "coverage_both_markets": all(item["pass"] for item in coverage.values()),
        "no_stale_tmp": not stale_files,
        "no_failure_markers": not marker_files,
        "no_forbidden_journal_markers": not journal_marker_hits,
        "successor_active": _command_output("systemctl", "is-active", service_unit) == "active",
        "successor_enabled": _command_output("systemctl", "is-enabled", service_unit) == "enabled",
        "successor_exactly_one_process": command_matches == [main_pid],
        "old_service_inactive": (
            _command_output("systemctl", "is-active", "signalbot.service") == "inactive"
        ),
        "old_service_disabled": (
            _command_output("systemctl", "is-enabled", "signalbot.service") == "disabled"
        ),
        "old_main_pid_zero": old_pid == 0,
        "load_below_two": load_1 < MAX_LOAD_1,
        "rss_below_1_5_gib": rss_kib < MAX_RSS_KIB,
        "memory_available_above_2_gib": mem_available_bytes >= MIN_MEMORY_AVAILABLE_BYTES,
        "root_free_above_16_gib": disk_free_bytes >= MIN_DISK_FREE_BYTES,
    }
    return {
        "schema_version": "phase_o_oci_initial_health_v1",
        "captured_at_ms": now_ms,
        "campaign_id": campaign_id,
        "activation_ms": activation_ms,
        "elapsed_since_activation_ms": now_ms - activation_ms,
        "source_identity": campaign[0] if campaign else None,
        "checks": checks,
        "all_checks_pass": all(checks.values()),
        "segments": segments,
        "coverage": coverage,
        "database": {"quick_check": integrity, "health_report": report},
        "journal": {"forbidden_marker_hits": journal_marker_hits},
        "stale_files": stale_files,
        "failure_markers": marker_files,
        "resources": {
            "main_pid": main_pid,
            "matching_processes": command_matches,
            "load_1": load_1,
            "rss_kib": rss_kib,
            "memory_available_bytes": mem_available_bytes,
            "root_free_bytes": disk_free_bytes,
        },
        "verdict": "PASS" if all(checks.values()) else "FAIL",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--tape-root", type=Path, required=True)
    parser.add_argument("--activation-ms", type=int, required=True)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument("--service-unit", default="binance-bot-2-prospective.service")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = build_health(
        db_path=args.db,
        campaign_id=args.campaign_id,
        tape_root=args.tape_root,
        activation_ms=args.activation_ms,
        expected_source=args.expected_source,
        service_unit=args.service_unit,
    )
    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    print(text, end="")
    return 0 if result["all_checks_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
