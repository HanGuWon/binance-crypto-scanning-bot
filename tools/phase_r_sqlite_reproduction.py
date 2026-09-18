"""Deterministic, scratch-only SQLite contention reproduction for Phase R.

The harness never opens a repository or campaign database.  Every scenario
uses a private temporary database and synchronizes the competing operations
with Events, so the result does not depend on sleep-based timing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sqlite3
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ITERATIONS = 40
SHORT_TIMEOUT_SECONDS = 0.05
NORMAL_TIMEOUT_SECONDS = 0.25
WAIT_TIMEOUT_SECONDS = 3.0
SCAN_SIZES = (0, 500, 1000)


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _connect(path: Path, *, timeout: float) -> sqlite3.Connection:
    return sqlite3.connect(str(path), timeout=timeout, isolation_level=None)


def _new_database(directory: Path, name: str) -> Path:
    path = directory / f"{name}.db"
    connection = sqlite3.connect(str(path), timeout=NORMAL_TIMEOUT_SECONDS)
    try:
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.executescript(
            """
            CREATE TABLE candles (
                id INTEGER PRIMARY KEY,
                market TEXT NOT NULL,
                value INTEGER NOT NULL
            );
            CREATE TABLE shadow_coverage (
                id INTEGER PRIMARY KEY,
                market TEXT NOT NULL,
                complete INTEGER NOT NULL
            );
            """
        )
        connection.executemany(
            "INSERT INTO candles(market, value) VALUES (?, ?)",
            (("futures" if index % 2 else "spot", index) for index in range(1000)),
        )
        connection.executemany(
            "INSERT INTO shadow_coverage(market, complete) VALUES (?, ?)",
            (("futures" if index % 2 else "spot", 1) for index in range(200)),
        )
        connection.commit()
    finally:
        connection.close()
    return path


def _duration_summary(durations: list[float]) -> dict[str, float | int | None]:
    if not durations:
        return {"count": 0, "min_ms": None, "p50_ms": None, "p95_ms": None, "max_ms": None}
    ordered = sorted(durations)

    def percentile(percent: float) -> float:
        index = min(len(ordered) - 1, int((len(ordered) - 1) * percent))
        return round(ordered[index] * 1000, 3)

    return {
        "count": len(ordered),
        "min_ms": round(ordered[0] * 1000, 3),
        "p50_ms": percentile(0.50),
        "p95_ms": percentile(0.95),
        "max_ms": round(ordered[-1] * 1000, 3),
    }


def _is_busy(error: BaseException) -> bool:
    return isinstance(error, sqlite3.OperationalError) and "locked" in str(error).lower()


def _scenario_result(
    name: str,
    *,
    operations: int,
    successes: int,
    busy_errors: int,
    durations: list[float],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "scenario": name,
        "operations": operations,
        "successes": successes,
        "busy_errors": busy_errors,
        "other_errors": max(0, operations - successes - busy_errors),
        "latency": _duration_summary(durations),
    }
    if extra:
        result.update(extra)
    return result


def _run_writer_only(directory: Path) -> dict[str, Any]:
    path = _new_database(directory, "writer-only")
    durations: list[float] = []
    successes = 0
    busy_errors = 0
    connection = _connect(path, timeout=NORMAL_TIMEOUT_SECONDS)
    try:
        for index in range(ITERATIONS):
            started = time.perf_counter()
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO candles(market, value) VALUES (?, ?)",
                    ("futures", 1000 + index),
                )
                connection.commit()
                successes += 1
            except sqlite3.Error as error:
                if _is_busy(error):
                    busy_errors += 1
                connection.rollback()
            durations.append(time.perf_counter() - started)
    finally:
        connection.close()
    return _scenario_result(
        "writer_only",
        operations=ITERATIONS,
        successes=successes,
        busy_errors=busy_errors,
        durations=durations,
        extra={"database": path.name, "journal_mode": "delete"},
    )


def _run_writer_with_reader(directory: Path, *, quick_check: bool) -> dict[str, Any]:
    label = "writer_plus_quick_check" if quick_check else "writer_plus_light_read"
    path = _new_database(directory, label)
    ready_events = [threading.Event() for _ in range(ITERATIONS)]
    release_events = [threading.Event() for _ in range(ITERATIONS)]
    writer_durations: list[float] = []
    reader_durations: list[float] = []
    writer_successes = 0
    reader_successes = 0
    writer_busy = 0
    reader_busy = 0
    errors: list[str] = []

    def writer() -> None:
        nonlocal writer_successes, writer_busy
        connection = _connect(path, timeout=NORMAL_TIMEOUT_SECONDS)
        try:
            for index in range(ITERATIONS):
                started = time.perf_counter()
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "INSERT INTO candles(market, value) VALUES (?, ?)",
                        ("spot", 2000 + index),
                    )
                    ready_events[index].set()
                    if not release_events[index].wait(WAIT_TIMEOUT_SECONDS):
                        raise RuntimeError("reader did not release writer")
                    connection.commit()
                    writer_successes += 1
                except RuntimeError as error:
                    errors.append(f"writer:{error}")
                    connection.rollback()
                    ready_events[index].set()
                except sqlite3.Error as error:
                    if _is_busy(error):
                        writer_busy += 1
                    connection.rollback()
                    ready_events[index].set()
                writer_durations.append(time.perf_counter() - started)
        finally:
            connection.close()

    def reader() -> None:
        nonlocal reader_successes, reader_busy
        connection = _connect(path, timeout=SHORT_TIMEOUT_SECONDS)
        try:
            for index in range(ITERATIONS):
                if not ready_events[index].wait(WAIT_TIMEOUT_SECONDS):
                    errors.append("reader:writer did not become ready")
                    release_events[index].set()
                    continue
                started = time.perf_counter()
                try:
                    if quick_check:
                        value = connection.execute("PRAGMA quick_check").fetchone()[0]
                        if value != "ok":
                            errors.append(f"reader:quick_check={value}")
                    else:
                        connection.execute(
                            "SELECT COUNT(*) FROM shadow_coverage WHERE complete=1"
                        ).fetchone()
                    connection.rollback()
                    reader_successes += 1
                except sqlite3.Error as error:
                    if _is_busy(error):
                        reader_busy += 1
                    connection.rollback()
                finally:
                    release_events[index].set()
                    reader_durations.append(time.perf_counter() - started)
        finally:
            connection.close()

    writer_thread = threading.Thread(target=writer, name=f"{label}-writer")
    reader_thread = threading.Thread(target=reader, name=f"{label}-reader")
    writer_thread.start()
    reader_thread.start()
    writer_thread.join(WAIT_TIMEOUT_SECONDS * 2)
    reader_thread.join(WAIT_TIMEOUT_SECONDS * 2)
    if writer_thread.is_alive() or reader_thread.is_alive():
        raise RuntimeError(f"{label} thread did not finish within the bounded deadline")
    if errors:
        raise RuntimeError(f"{label} orchestration errors: {'; '.join(errors)}")
    if writer_successes + writer_busy != ITERATIONS:
        raise RuntimeError(f"{label} writer accounting is incomplete")
    if reader_successes + reader_busy != ITERATIONS:
        raise RuntimeError(f"{label} reader accounting is incomplete")
    return _scenario_result(
        label,
        operations=ITERATIONS,
        successes=writer_successes,
        busy_errors=writer_busy,
        durations=writer_durations,
        extra={
            "database": path.name,
            "reader_successes": reader_successes,
            "reader_busy_errors": reader_busy,
            "reader_latency": _duration_summary(reader_durations),
            "thread_errors": errors,
            "writer_holds_reserved_transaction": True,
            "reader_connection_closed_transaction_each_iteration": True,
        },
    )


def _create_segments(directory: Path, count: int) -> Path:
    segment_directory = directory / f"segments-{count}"
    segment_directory.mkdir()
    for index in range(count):
        (segment_directory / f"{index:08d}.jsonl.zst").write_bytes(
            f"segment-{index}\n".encode() * 16
        )
    return segment_directory


def _hash_segments(directory: Path) -> int:
    total = 0
    for path in sorted(directory.glob("*.jsonl.zst")):
        digest = hashlib.sha256(path.read_bytes()).digest()
        total += len(digest)
    return total


def _run_old_health_once(directory: Path, segment_count: int) -> dict[str, Any]:
    path = _new_database(directory, f"old-health-like-{segment_count}")
    segment_directory = _create_segments(directory, segment_count)
    writer_ready = threading.Event()
    health_db_started = threading.Event()
    db_closed = threading.Event()
    writer_release = threading.Event()
    writer_durations: list[float] = []
    writer_errors: list[str] = []

    def writer() -> None:
        connection = _connect(path, timeout=NORMAL_TIMEOUT_SECONDS)
        try:
            started = time.perf_counter()
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO candles(market, value) VALUES (?, ?)",
                ("futures", 3000 + segment_count),
            )
            writer_ready.set()
            if not health_db_started.wait(WAIT_TIMEOUT_SECONDS):
                writer_errors.append("health did not start")
            if not db_closed.wait(WAIT_TIMEOUT_SECONDS):
                writer_errors.append("health did not close DB connection")
            if not writer_release.wait(WAIT_TIMEOUT_SECONDS):
                writer_errors.append("writer release was not signaled")
            connection.commit()
            writer_durations.append(time.perf_counter() - started)
        except sqlite3.Error as error:
            writer_errors.append(type(error).__name__ + ":" + str(error))
            connection.rollback()
        finally:
            connection.close()

    writer_thread = threading.Thread(target=writer, name="old-health-like-writer")
    writer_thread.start()
    if not writer_ready.wait(WAIT_TIMEOUT_SECONDS):
        raise RuntimeError("old-health-like writer did not become ready")

    health_started = time.perf_counter()
    health_db_started.set()
    db_started = time.perf_counter()
    health_connection = _connect(path, timeout=SHORT_TIMEOUT_SECONDS)
    db_busy = 0
    db_errors: list[str] = []
    try:
        try:
            health_connection.execute("PRAGMA query_only=ON")
            quick_check = health_connection.execute("PRAGMA quick_check").fetchone()[0]
            health_connection.execute(
                "SELECT market, COUNT(*) FROM shadow_coverage GROUP BY market"
            ).fetchall()
        except sqlite3.Error as error:
            if _is_busy(error):
                db_busy += 1
            db_errors.append(type(error).__name__ + ":" + str(error))
            quick_check = None
    finally:
        health_connection.close()
    db_closed.set()
    db_elapsed = time.perf_counter() - db_started

    scan_started = time.perf_counter()
    hashed_bytes = _hash_segments(segment_directory)
    scan_elapsed = time.perf_counter() - scan_started
    writer_release.set()
    writer_thread.join(WAIT_TIMEOUT_SECONDS * 2)
    if writer_thread.is_alive():
        raise RuntimeError(f"old-health-like writer did not finish for {segment_count}")
    if writer_errors:
        raise RuntimeError(
            f"old-health-like writer orchestration errors: {'; '.join(writer_errors)}"
        )

    return {
        "segment_count": segment_count,
        "database": path.name,
        "quick_check": quick_check,
        "database_busy_errors": db_busy,
        "database_errors": db_errors,
        "db_read_latency_ms": round(db_elapsed * 1000, 3),
        "segment_scan_latency_ms": round(scan_elapsed * 1000, 3),
        "total_health_latency_ms": round((time.perf_counter() - health_started) * 1000, 3),
        "hashed_digest_bytes": hashed_bytes,
        "writer_commit_latency": _duration_summary(writer_durations),
        "writer_errors": writer_errors,
        "sqlite_connection_closed_before_segment_scan": True,
    }


def _run_old_health_like(directory: Path) -> dict[str, Any]:
    scale = [_run_old_health_once(directory, count) for count in SCAN_SIZES]
    return {
        "scenario": "old_health_like",
        "operations": len(scale),
        "successes": sum(item["quick_check"] == "ok" for item in scale),
        "busy_errors": sum(item["database_busy_errors"] for item in scale),
        "scale": scale,
        "health_ordering": "live DB quick_check/coverage, close DB, then full segment hash scan",
    }


def _run_overlapping_writers(directory: Path) -> dict[str, Any]:
    path = _new_database(directory, "overlapping-writers")
    first_ready = threading.Event()
    release_first = threading.Event()
    second_attempted = threading.Event()
    first_durations: list[float] = []
    second_durations: list[float] = []
    first_errors: list[str] = []
    second_errors: list[str] = []
    second_successes = 0
    second_busy = 0

    def first_writer() -> None:
        connection = _connect(path, timeout=NORMAL_TIMEOUT_SECONDS)
        try:
            started = time.perf_counter()
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO candles(market, value) VALUES (?, ?)",
                ("futures", 4000),
            )
            first_ready.set()
            if not release_first.wait(WAIT_TIMEOUT_SECONDS):
                first_errors.append("second writer did not attempt")
            connection.commit()
            first_durations.append(time.perf_counter() - started)
        except sqlite3.Error as error:
            first_errors.append(type(error).__name__ + ":" + str(error))
            connection.rollback()
        finally:
            connection.close()

    def second_writer() -> None:
        nonlocal second_successes, second_busy
        if not first_ready.wait(WAIT_TIMEOUT_SECONDS):
            second_errors.append("first writer did not become ready")
            second_attempted.set()
            release_first.set()
            return
        connection = _connect(path, timeout=SHORT_TIMEOUT_SECONDS)
        try:
            started = time.perf_counter()
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO candles(market, value) VALUES (?, ?)",
                    ("spot", 4001),
                )
                connection.commit()
                second_successes += 1
            except sqlite3.Error as error:
                if _is_busy(error):
                    second_busy += 1
                second_errors.append(type(error).__name__ + ":" + str(error))
                connection.rollback()
            second_durations.append(time.perf_counter() - started)
        finally:
            second_attempted.set()
            release_first.set()
            connection.close()

    first_thread = threading.Thread(target=first_writer, name="overlap-first")
    second_thread = threading.Thread(target=second_writer, name="overlap-second")
    first_thread.start()
    second_thread.start()
    first_thread.join(WAIT_TIMEOUT_SECONDS * 2)
    second_thread.join(WAIT_TIMEOUT_SECONDS * 2)
    if first_thread.is_alive() or second_thread.is_alive():
        raise RuntimeError("overlapping writers did not finish within the bounded deadline")
    if first_errors or not second_attempted.is_set():
        raise RuntimeError(
            "overlapping writer orchestration errors: "
            + "; ".join(first_errors or ["second attempt was not observed"])
        )
    return {
        "scenario": "overlapping_writers",
        "operations": 2,
        "successes": 1 + second_successes,
        "busy_errors": second_busy,
        "first_writer_latency": _duration_summary(first_durations),
        "second_writer_latency": _duration_summary(second_durations),
        "second_writer_errors": second_errors,
        "first_writer_errors": first_errors,
        "forced_overlap": True,
        "short_timeout_seconds": SHORT_TIMEOUT_SECONDS,
        "second_attempt_observed": second_attempted.is_set(),
    }


def _adjudicate(results: dict[str, Any]) -> list[dict[str, Any]]:
    light = results["writer_plus_light_read"]
    quick = results["writer_plus_quick_check"]
    health_scale = results["old_health_like"]["scale"]
    overlap = results["overlapping_writers"]
    scan_durations = [item["segment_scan_latency_ms"] for item in health_scale]
    db_durations = [item["db_read_latency_ms"] for item in health_scale]
    scan_grows = scan_durations[-1] >= scan_durations[0]
    return [
        {
            "id": "H1",
            "hypothesis": (
                "Health quick_check/coverage can overlap a writer and create the observed "
                "lock failure."
            ),
            "result": {
                "light_read_busy_errors": light["busy_errors"] + light["reader_busy_errors"],
                "quick_check_busy_errors": quick["busy_errors"] + quick["reader_busy_errors"],
            },
            "verdict": "NOT_REPRODUCED_IN_SHORT_RESERVED_HOLD"
            if quick["busy_errors"] + quick["reader_busy_errors"] == 0
            else "RETAINED",
            "falsifier_or_limit": (
                "A clean short controlled quick_check overlap does not rule out longer/full-health "
                "interaction on the OCI database; process-level lock tracing remains absent."
            ),
        },
        {
            "id": "H2",
            "hypothesis": (
                "The old health-like full-history scan is an independent timeout risk after the "
                "SQLite connection closes."
            ),
            "result": {
                "scan_latency_ms_by_manifest_count": scan_durations,
                "db_read_latency_ms_by_manifest_count": db_durations,
                "scan_latency_non_decreasing_in_scale_probe": scan_grows,
            },
            "verdict": "RETAINED_AS_TIMEOUT_RISK" if scan_grows else "NOT_SUPPORTED_BY_SCALE_PROBE",
            "falsifier_or_limit": (
                "The scale is intentionally small and scratch-only; r4 must test 5000+/10000+ "
                "manifests and enforce a real health budget."
            ),
        },
        {
            "id": "H3",
            "hypothesis": (
                "Overlapping file-backed SQLite writers need a finite busy policy; the current "
                "engine has no explicit file timeout connect_args."
            ),
            "result": {
                "forced_overlap_busy_errors": overlap["busy_errors"],
                "writer_only_busy_errors": results["writer_only"]["busy_errors"],
            },
            "verdict": "RETAINED" if overlap["busy_errors"] > 0 else "NOT_REPRODUCED",
            "falsifier_or_limit": (
                "The forced overlap uses a 50ms scratch timeout; r3 must prove the selected finite "
                "timeout against measured lock duration and idempotency tests."
            ),
        },
        {
            "id": "H4",
            "hypothesis": (
                "A writer-only transaction or filesystem latency alone explains the failure."
            ),
            "result": {
                "writer_only_busy_errors": results["writer_only"]["busy_errors"],
                "writer_only_successes": results["writer_only"]["successes"],
            },
            "verdict": "WEAKENED_BY_WRITER_ONLY_CONTROL"
            if results["writer_only"]["busy_errors"] == 0
            else "RETAINED",
            "falsifier_or_limit": (
                "This is not a production filesystem trace; it only falsifies the isolated scratch "
                "control under the same SQLite journal mode."
            ),
        },
    ]


def run_reproduction() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="phase-r-sqlite-") as temporary:
        directory = Path(temporary)
        raw_results: dict[str, Any] = {
            "writer_only": _run_writer_only(directory),
            "writer_plus_light_read": _run_writer_with_reader(directory, quick_check=False),
            "writer_plus_quick_check": _run_writer_with_reader(directory, quick_check=True),
            "old_health_like": _run_old_health_like(directory),
            "overlapping_writers": _run_overlapping_writers(directory),
        }
    return {
        "schema_contract": "phase-r-sqlite-reproduction-v1",
        "schema_version": "phase_r_sqlite_reproduction_v1",
        "captured_at_utc": _utc_now(),
        "work_phase_id": "r2",
        "method": {
            "scratch_only": True,
            "temporary_directory_deleted_after_run": True,
            "journal_mode": "delete",
            "iterations": ITERATIONS,
            "short_timeout_seconds": SHORT_TIMEOUT_SECONDS,
            "normal_timeout_seconds": NORMAL_TIMEOUT_SECONDS,
            "synchronization": (
                "threading.Event handshakes; no sleep-based synchronization; all joins bounded"
            ),
            "live_oci_database_opened": False,
            "production_source_edited": False,
        },
        "runtime": {
            "python": sys.version.split()[0],
            "sqlite_runtime": sqlite3.sqlite_version,
            "sqlite_module": sqlite3.version,
            "platform": platform.platform(),
        },
        "scenarios": raw_results,
        "hypothesis_adjudication": _adjudicate(raw_results),
        "remedy_selection_basis": {
            "supported_mechanism": (
                "Add finite SQLite busy handling/timeout at the file-backed connection boundary "
                "and keep transactions short; expose failure after the bound "
                "instead of blind retry."
            ),
            "health_mechanism": (
                "Remove hourly full-history work from FAST HEALTH; keep low-frequency DEEP AUDIT "
                "separate after the connection is closed or on a safe snapshot."
            ),
            "wal_decision": (
                "Do not enable WAL for the current live DB from this reproduction; journal-mode "
                "change requires a separate justified new-campaign decision."
            ),
            "remaining_proof": (
                "r3 must implement only after this receipt and prove lock handling, idempotency, "
                "and bounded failure with deterministic tests."
            ),
        },
        "governance": {
            "outcome_blind": True,
            "strategy_or_policy_change": False,
            "private_api_or_orders": False,
            "live_campaign_mutation": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    payload = run_reproduction()
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output is None:
        print(encoded, end="")
    else:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
        print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
