"""Deterministic T+24h operational receipt composer (Phase N).

Composes the COMPLETE T+24h operational receipt required by objective
sections 9/11/15, which the base checkpoint tool alone does not contain.
Read-only, outcome-blind. Writes only to --output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, os.getcwd())

from src.signalbot.prospective.source_freeze import (
    default_source_root,
    freeze_source,
)
from tools.prospective_operational_checkpoint import (
    build_checkpoint,
)

CONFIGURED_SOURCE_IDENTITY = (
    "worktree-source-v1:650fe603f79812e79cd9390c0d2e94fbab8071a4eab1cc95f40fc229c7f403dd"
)
DEFAULT_RECOVERY_BOUNDARY_MS = 1787738846103
DEFAULT_ACTIVATION_MS = 1787734450558


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


def _measure_physical_rate(tape_root: Path, activation_ms: int, recovery_boundary_ms: int) -> dict:
    rows: list[tuple[str, int, int]] = []
    for market in ("futures", "spot"):
        market_dir = tape_root / market
        if not market_dir.is_dir():
            continue
        for mf in sorted(market_dir.glob("*.manifest.json")):
            try:
                man = json.loads(mf.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            for ext in (".jsonl.zst", ".zst"):
                zst = mf.with_name(mf.stem.replace(".manifest", "") + ext)
                if zst.exists():
                    break
            else:
                continue
            byte_count = zst.stat().st_size
            start_ms = None
            for cand in (
                "segment_start_ms",
                "start_ms",
                "first_received_at_ms",
                "period_start_ms",
            ):
                if cand in man and isinstance(man[cand], int):
                    start_ms = man[cand]
                    break
            if start_ms is None:
                name = zst.name[: zst.name.rfind(".")]
                try:
                    start_ms = int(name.split("-")[0])
                except (ValueError, IndexError):
                    continue
            rows.append((market, start_ms, byte_count))

    healthy_rows = [r for r in rows if r[1] >= recovery_boundary_ms]
    whole_rows = [r for r in rows if r[1] >= activation_ms]
    active_partial_bytes = 0
    for market in ("futures", "spot"):
        market_dir = tape_root / market
        if market_dir.is_dir():
            active_partial_bytes += sum(
                p.stat().st_size for p in market_dir.glob("*.jsonl.zst.partial")
            )

    now_ms = int(time.time() * 1000)
    elapsed_h = (now_ms - activation_ms) / 3600000.0
    total_whole_bytes = sum(r[2] for r in whole_rows)
    total_phys = total_whole_bytes + active_partial_bytes
    wall_rate = total_phys / (1024**3) / elapsed_h if elapsed_h > 0 else 0.0

    if healthy_rows:
        win_start = min(r[1] for r in healthy_rows)
        win_end = max(r[1] for r in healthy_rows) + 1800000
        healthy_h = (win_end - win_start) / 3600000.0
        healthy_rate = (
            (sum(r[2] for r in healthy_rows) / (1024**3) / healthy_h) if healthy_h > 0 else 0.0
        )
        seg_rates = [(r[2] / (1024**3)) / (5.0 / 60.0) for r in healthy_rows]
        p95 = statistics.quantiles(seg_rates, n=20)[18] if len(seg_rates) >= 20 else max(seg_rates)
    else:
        healthy_h = 0.0
        healthy_rate = 0.0
        p95 = 0.0

    return {
        "elapsed_since_activation_hours": round(elapsed_h, 4),
        "sealed_segments_total": len(rows),
        "healthy_window_segments": len(healthy_rows),
        "physical_bytes_total_incl_partials": total_phys,
        "physical_gib_total_incl_partials": round(total_phys / (1024**3), 4),
        "active_partial_bytes": active_partial_bytes,
        "wall_clock_rate_gib_per_h": round(wall_rate, 5),
        "healthy_window_rate_gib_per_h": round(healthy_rate, 5),
        "p95_conservative_rate_gib_per_h": round(p95, 5),
    }


def _count_log_levels(log_path: Path) -> dict:
    # Count ERROR/CRITICAL/FATAL lines in a JSON-lines log file.
    if not log_path.is_file():
        return {
            "available": False,
            "log_path": str(log_path),
            "total_lines": None,
            "error_count": None,
            "critical_count": None,
            "fatal_count": None,
        }
    total = 0
    error_count = 0
    critical_count = 0
    fatal_count = 0
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                record = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            level = str(record.get("level", "")).upper()
            if level == "ERROR":
                error_count += 1
            elif level == "CRITICAL":
                critical_count += 1
            elif level == "FATAL":
                fatal_count += 1
    return {
        "available": True,
        "log_path": str(log_path),
        "total_lines": total,
        "error_count": error_count,
        "critical_count": critical_count,
        "fatal_count": fatal_count,
    }


def _collect_retest_denominator(db_path: Path, campaign_id: str) -> dict:
    # Count raw-C0 observations and retest lifecycle stages read-only.
    from sqlite3 import connect

    try:
        conn = connect("file:" + str(db_path) + "?mode=ro", uri=True)
        cur = conn.cursor()
    except Exception:
        return {"available": False}
    try:
        raw_c0 = cur.execute(
            "SELECT COUNT(*) FROM shadow_observations WHERE campaign_id=?",
            (campaign_id,),
        ).fetchone()[0]
        stages_rows = cur.execute(
            "SELECT stage, COUNT(*) FROM retest_lifecycles WHERE campaign_id=? GROUP BY stage",
            (campaign_id,),
        ).fetchall()
        lifecycles = cur.execute(
            "SELECT COUNT(*) FROM retest_lifecycles WHERE campaign_id=?",
            (campaign_id,),
        ).fetchone()[0]
    except Exception:
        conn.close()
        return {"available": False}
    conn.close()
    stages = dict(stages_rows)
    return {
        "available": True,
        "raw_c0_observations": raw_c0,
        "lifecycle_total": lifecycles,
        "stage_counts": stages,
    }


def _collect_segment_chain(tape_root: Path) -> dict:
    # Per-market segment chain verification from manifest files.
    out = {}
    for market in ("futures", "spot"):
        mdir = tape_root / market
        if not mdir.is_dir():
            out[market] = {"available": False}
            continue
        manifests = []
        for mf in sorted(mdir.glob("*.manifest.json")):
            try:
                data = json.loads(mf.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            data["_manifest_file"] = mf.name
            manifests.append(data)
        manifests.sort(key=lambda m: m.get("segment_sequence", 0))
        sealed = [m for m in manifests if not str(m["_manifest_file"]).startswith("_")]
        zst_files = list(mdir.glob("*.jsonl.zst"))
        partials = list(mdir.glob("*.jsonl.zst.partial"))
        total_compressed = sum(f.stat().st_size for f in zst_files)
        partial_bytes = sum(p.stat().st_size for p in partials)
        gaps = []
        prev_last = None
        prev_seq = None
        for m in sealed:
            seq = m.get("segment_sequence")
            first = m.get("first_received_at_ms")
            last = m.get("last_received_at_ms")
            if prev_last is not None and first is not None and first - prev_last >= 60000:
                gaps.append(
                    {
                        "after_segment": prev_seq,
                        "before_first_ms": prev_last,
                        "until_first_ms": first,
                        "gap_ms": first - prev_last,
                    }
                )
            prev_last = last
            prev_seq = seq
        latest_receipt = (
            max((m.get("last_received_at_ms") or 0) for m in sealed) if sealed else None
        )
        out[market] = {
            "available": True,
            "sealed_segments": len(sealed),
            "active_partials": len(partials),
            "total_compressed_bytes": total_compressed,
            "active_partial_bytes": partial_bytes,
            "latest_last_received_at_ms": latest_receipt,
            "gap_count": len(gaps),
            "gaps": gaps,
        }
    return out


def compose_t24h_receipt(
    *,
    db_path: Path,
    campaign_id: str,
    tape_root: Path,
    prereg_path: Path,
    activation_ms: int,
    quota_bytes: int,
    gate_hours: float,
    transient_reserve_gib: float,
    safety_margin_gib: float,
    decision: str,
    recovery_boundary_ms: int = DEFAULT_RECOVERY_BOUNDARY_MS,
    log_path: Path | None = None,
) -> dict:
    base = build_checkpoint(
        db_path=db_path,
        campaign_id=campaign_id,
        tape_root=tape_root,
        activation_ms=activation_ms,
    )

    try:
        freeze = freeze_source(default_source_root())
        source_identity = freeze.source_identity
        source_git_head = freeze.git_head
    except Exception as exc:
        source_identity = f"LIVE_SOURCE_DRIFT({exc})"
        source_git_head = None

    retest_denominator = _collect_retest_denominator(db_path, campaign_id)
    segment_chain = _collect_segment_chain(tape_root)
    prereg_sha = _sha256_file(prereg_path) if prereg_path.exists() else None
    rate = _measure_physical_rate(tape_root, activation_ms, recovery_boundary_ms)
    if log_path is not None:
        log_health = _count_log_levels(log_path)
    else:
        log_health = {
            "available": False,
            "log_path": None,
            "total_lines": None,
            "error_count": None,
            "critical_count": None,
            "fatal_count": None,
        }

    used_bytes = sum(f.stat().st_size for f in tape_root.rglob("*") if f.is_file())
    quota_gib = quota_bytes / (1024**3)
    used_gib = used_bytes / (1024**3)
    remaining_gib = quota_gib - used_gib
    conservative_rate = rate["p95_conservative_rate_gib_per_h"]
    required_gate_gib = conservative_rate * gate_hours
    total_needed_gib = required_gate_gib + transient_reserve_gib + safety_margin_gib
    gate_met = remaining_gib >= total_needed_gib

    if decision in ("CONTINUE_PHASE_M", "PLANNED_PHASE_N_ROLLOVER"):
        rollover_decision = decision
    else:
        rollover_decision = "CONTINUE_PHASE_M" if gate_met else "PLANNED_PHASE_N_ROLLOVER"

    receipt = {
        "receipt_schema_version": "phase_n_t24h_operational_receipt_v1",
        "artifact_id": "phase-n-tplus24h-operational-receipt",
        "campaign_id": campaign_id,
        **base,
        "source_identity": {
            "configured": CONFIGURED_SOURCE_IDENTITY,
            "recomputed": source_identity,
            "source_drift": source_identity != CONFIGURED_SOURCE_IDENTITY,
            "git_head": source_git_head,
        },
        "preregistration": {
            "path": str(prereg_path),
            "sha256": prereg_sha,
            "activation_ms": activation_ms,
            "activation_utc": _iso_utc(activation_ms),
        },
        "retest_denominator": retest_denominator,
        "segment_chain": segment_chain,
        "log_health": log_health,
        "physical_storage": {
            **rate,
            "quota_bytes": quota_bytes,
            "quota_gib": round(quota_gib, 4),
            "used_bytes_on_disk": used_bytes,
            "used_gib": round(used_gib, 4),
            "remaining_quota_gib": round(remaining_gib, 4),
        },
        "rollover_gate": {
            "formula": (
                "remaining_quota_gib >= conservative_rate_gib_per_h * gate_hours "
                "+ transient_finalize_reserve_gib + safety_margin_gib"
            ),
            "conservative_rate_gib_per_h": conservative_rate,
            "gate_hours": gate_hours,
            "required_gate_gib": round(required_gate_gib, 4),
            "transient_finalize_reserve_gib": transient_reserve_gib,
            "safety_margin_gib": safety_margin_gib,
            "total_needed_gib": round(total_needed_gib, 4),
            "gate_met": gate_met,
            "decision": rollover_decision,
        },
    }
    return receipt


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--tape-root", required=True)
    parser.add_argument("--prereg", required=True)
    parser.add_argument("--activation-ms", type=int, required=True)
    parser.add_argument("--quota-bytes", type=int, required=True)
    parser.add_argument("--gate-hours", type=float, default=48.0)
    parser.add_argument("--transient-reserve-gib", type=float, default=0.02)
    parser.add_argument("--safety-margin-gib", type=float, default=0.5)
    parser.add_argument("--decision", default="AUTO")
    parser.add_argument(
        "--recovery-boundary-ms",
        type=int,
        default=DEFAULT_RECOVERY_BOUNDARY_MS,
    )
    parser.add_argument("--log-path", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args(argv)

    receipt = compose_t24h_receipt(
        db_path=Path(args.db),
        campaign_id=args.campaign_id,
        tape_root=Path(args.tape_root),
        prereg_path=Path(args.prereg),
        activation_ms=args.activation_ms,
        quota_bytes=args.quota_bytes,
        gate_hours=args.gate_hours,
        transient_reserve_gib=args.transient_reserve_gib,
        safety_margin_gib=args.safety_margin_gib,
        decision=args.decision,
        recovery_boundary_ms=args.recovery_boundary_ms,
        log_path=Path(args.log_path) if args.log_path else None,
    )
    text_out = json.dumps(receipt, indent=2)
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text_out + chr(10), encoding="utf-8")
        print(f"T+24h receipt written to {out_path}")
    else:
        print(text_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
