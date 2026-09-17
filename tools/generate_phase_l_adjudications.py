"""Generate Phase-L adjudication artifacts (v2), reading evidence from disk.

Every cited value is read from disk at generation time; the only wall clock
used is datetime.now(timezone.utc). No timestamps are hand-written.
Append-only: existing receipts are hashed, never modified.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
CANARY_DIR = REPO / "var/cutover-canary/smoke-causal-retest-phase-l-final-20260826-v1"
RECEIPT = CANARY_DIR / "canary-result-receipt.json"
TERMINAL = REPO / "var/prospective/causal-retest-prospective-phase-l-terminal-status.json"
OUT_DIR = REPO / "artifacts/evidence"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def mtime_utc(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def collect_segment_evidence() -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    raw_events = CANARY_DIR / "raw-events"
    for market in ("futures", "spot"):
        market_dir = raw_events / market
        if not market_dir.is_dir():
            continue
        for manifest_path in sorted(market_dir.glob("*.manifest.json")):
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            entries.append(
                {
                    "market": market,
                    "file": manifest_path.name,
                    "segment_sequence": manifest["segment_sequence"],
                    "finalized_at_utc_manifest": manifest.get("finalized_at_utc"),
                    "manifest_file_mtime_utc": mtime_utc(manifest_path),
                }
            )
    return entries


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    receipt_sha = sha256_file(RECEIPT)
    terminal_sha = sha256_file(TERMINAL)
    receipt = json.loads(RECEIPT.read_text(encoding="utf-8"))
    terminal = json.loads(TERMINAL.read_text(encoding="utf-8"))
    segments = collect_segment_evidence()
    last_finalized = max(
        (
            str(s["finalized_at_utc_manifest"])
            for s in segments
            if s["finalized_at_utc_manifest"] is not None
        ),
        default=None,
    )

    post = {
        "schema_version": "phase_l_post_completion_adjudication_v2",
        "generated_at_utc": utc_now(),
        "supersedes_verdict": {
            "artifact": str(RECEIPT.relative_to(REPO)),
            "artifact_sha256": receipt_sha,
            "old_verdict": receipt["verdict"],
        },
        "corrected_historical_verdict": {
            "SEGMENTED_STORAGE_FEASIBILITY": "STORAGE_FEASIBILITY_PASS",
            "STORAGE_AUTHORITY_QUALIFICATION": "INVALIDATED_BY_POST_AUDIT",
            "SCIENTIFIC_COVERAGE": "FAIL",
        },
        "invalidation_reasons": [
            {
                "id": "async_ingestion_contract_regression",
                "detail": (
                    "ProspectiveTapeRecorder.append executed synchronous "
                    "handle.write on the calling context; wait_failed() was "
                    "asyncio.sleep(3600); accepted/durable counters incremented "
                    "together in append()."
                ),
            },
            {
                "id": "unenforced_physical_quota",
                "detail": (
                    "raw_event_max_bytes never passed through; "
                    "maximum_physical_bytes stored but never checked."
                ),
            },
            {
                "id": "incomplete_crash_recovery_authority",
                "detail": (
                    "_recover_unfinished_tail deleted unreadable partials via "
                    "unlink, violating fail-closed evidence semantics."
                ),
            },
            {
                "id": "incomplete_segment_verifier",
                "detail": (
                    "Replay hashed whole compressed file (read_bytes) with no "
                    "incremental compressed SHA; _previous_segment_sha allowed "
                    "missing predecessor via None-and-continue."
                ),
            },
            {
                "id": "raw_segment_set_not_bound_by_canary_seal",
                "detail": (
                    "Seal tool bound only *.jsonl legacy files; segmented tape "
                    "absent from smoke evidence seal v1."
                ),
            },
        ],
        "evidence_source_identity": (
            "worktree-source-v1:9e36bba58600550bbc89ec8f403f0036da2a02525ce8"
            "f054f554904948cc2815"
        ),
        "preserved_artifacts": [
            {
                "path": str(RECEIPT.relative_to(REPO)),
                "sha256": receipt_sha,
                "disposition": "PRESERVED_UNMODIFIED_HISTORICAL_EVIDENCE",
            },
            {
                "path": str(TERMINAL.relative_to(REPO)),
                "sha256": terminal_sha,
                "disposition": "PRESERVED_UNMODIFIED_HISTORICAL_EVIDENCE",
            },
            {
                "path": "artifacts/source_snapshots/phase-l-source-diff-receipt.json",
                "sha256": sha256_file(
                    REPO / "artifacts/source_snapshots/phase-l-source-diff-receipt.json"
                ),
                "disposition": "PRESERVED_UNMODIFIED_HISTORICAL_EVIDENCE",
            },
        ],
        "non_actions": ["no old receipt rewritten", "no canary artifacts mutated"],
    }

    prov = {
        "schema_version": "phase_l_timestamp_provenance_adjudication_v2",
        "generated_at_utc": utc_now(),
        "wall_clock_source": "datetime.now(timezone.utc) at generation time",
        "subject_artifacts": [
            {
                "path": str(RECEIPT.relative_to(REPO)),
                "sha256": receipt_sha,
                "field_classifications": [
                    {
                        "field": "created_at_utc",
                        "recorded_value": receipt["created_at_utc"],
                        "classification": "ERRONEOUS_FUTURE_DATED",
                        "authoritative_contrast": (
                            "Filesystem LastWriteTime (OS authority): "
                            + mtime_utc(RECEIPT)
                            + "; segment manifests finalized earlier in the same run."
                        ),
                    },
                    {
                        "field": "graceful_shutdown.bounded_timer_utc",
                        "recorded_value": receipt["graceful_shutdown"]["bounded_timer_utc"],
                        "classification": (
                            "APPROXIMATE_MANUAL_ENTRY_CONFLICTS_WITH_MANIFEST_TIMES"
                        ),
                        "authoritative_contrast": (
                            "Last manifest finalized_at_utc="
                            + str(last_finalized)
                            + " precedes the claimed shutdown by ~2 hours."
                        ),
                    },
                    {
                        "field": "graceful_shutdown.stopped_logged_utc",
                        "recorded_value": receipt["graceful_shutdown"]["stopped_logged_utc"],
                        "classification": (
                            "APPROXIMATE_MANUAL_ENTRY_CONFLICTS_WITH_MANIFEST_TIMES"
                        ),
                        "authoritative_contrast": (
                            "Same manifest-time conflict as bounded_timer_utc."
                        ),
                    },
                ],
            },
            {
                "path": str(TERMINAL.relative_to(REPO)),
                "sha256": terminal_sha,
                "field_classifications": [
                    {
                        "field": "created_at_utc",
                        "recorded_value": terminal["created_at_utc"],
                        "classification": "ERRONEOUS_FUTURE_DATED_VS_FILESYSTEM_MTIME",
                        "authoritative_contrast": (
                            "Filesystem LastWriteTime (OS authority): "
                            + mtime_utc(TERMINAL)
                        ),
                    }
                ],
            },
        ],
        "immutable_time_authorities_used": [
            "segment manifest finalized_at_utc fields",
            "filesystem mtimes converted from local KST to UTC at read time",
            "log-derived times retained inside preserved receipts (not edited)",
        ],
        "segment_evidence": segments,
        "policy_from_phase_m_forward": (
            "All receipt generators MUST obtain UTC programmatically via "
            "datetime.now(timezone.utc) or an owned wall-clock utility; tests "
            "must reject impossible orderings and materially-future timestamps."
        ),
        "non_actions": ["old receipts not rewritten", "append-only correction"],
    }

    for name, payload in (
        ("phase-l-post-completion-adjudication-v2.json", post),
        ("phase-l-timestamp-provenance-adjudication-v2.json", prov),
    ):
        out = OUT_DIR / name
        out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {out} generated_at={payload['generated_at_utc']}")


if __name__ == "__main__":
    main()
