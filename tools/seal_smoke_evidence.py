"""Seal final smoke evidence into an immutable manifest (read-only inputs)."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main(argv=None):
    import argparse as ap

    parser = ap.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--db", required=True)
    parser.add_argument("--stdout-log", required=True)
    parser.add_argument("--stderr-log", required=True)
    parser.add_argument("--raw-tape-root", required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)

    entries = []
    tape_root = Path(args.raw_tape_root)
    if tape_root.is_dir():
        for fp in sorted(tape_root.rglob("*.jsonl")):
            entries.append({
                "path": fp.as_posix(),
                "sha256": _sha256_file(fp),
                "bytes": fp.stat().st_size,
            })
    tape_root_sha = hashlib.sha256(
        json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest() if entries else None

    # DB integrity + row counts snapshot (read-only URI).
    conn = sqlite3.connect("file:" + str(Path(args.db)).replace("\\", "/") + "?mode=ro", uri=True)
    cur = conn.cursor()
    integrity = cur.execute("PRAGMA integrity_check").fetchone()[0]
    coverage_counts = dict(
        cur.execute("SELECT market, COUNT(*) FROM shadow_coverage GROUP BY market").fetchall()
    )
    conn.close()

    manifest = {
        "evidence_manifest_schema_version": "smoke_evidence_seal_v1",
        "campaign_id": args.campaign_id,
        "files": {
            "config_sha256": _sha256_file(Path(args.config)),
            "db_sha256": _sha256_file(Path(args.db)),
            "stdout_log_sha256": _sha256_file(Path(args.stdout_log)),
            "stderr_log_sha256": _sha256_file(Path(args.stderr_log)),
        },
        "database_integrity_check": integrity,
        "coverage_row_counts": coverage_counts,
        "raw_tape_files": entries,
        "raw_tape_root_sha256": tape_root_sha,
    }
    inner = {k: v for k, v in manifest.items()}
    manifest["evidence_manifest_sha256"] = hashlib.sha256(
        json.dumps(inner, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(manifest, indent=2, sort_keys=True) + chr(10), encoding="utf-8")
    print(json.dumps({
        "output": str(out),
        "evidence_manifest_sha256": manifest["evidence_manifest_sha256"],
        "raw_tape_root_sha256": tape_root_sha,
        "integrity": integrity,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
