"""Create the Phase R candidate source freeze and parity/diff receipts."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from signalbot.config import load_settings
from signalbot.prospective.observer import shadow_config_sha256
from signalbot.prospective.retest import retest_policy_for_horizon
from signalbot.prospective.retest_outcomes import RetestOutcomePolicy
from signalbot.prospective.source_freeze import default_source_root, freeze_source
from signalbot.signals.shadow_policy import shadow_policy_identity

ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = ROOT / "health" / "_dev" / "_tmp-frozen-baseline.json"
CANDIDATE_FREEZE_PATH = ROOT / "health" / "_dev" / "phase-r-source-freeze-v1.json"
DIFF_RECEIPT_PATH = ROOT / "artifacts" / "evidence" / "phase-r-source-diff-receipt-v1.json"
EXPECTED_BASELINE = (
    "worktree-source-v1:650fe603f79812e79cd9390c0d2e94fbab8071a4eab1cc95f40fc229c7f403dd"
)
EXPECTED_IDENTITIES = {
    "config_sha256": "43971efece98b71f815a4e4f476e18e3467f9a8cc02de3d51b8be1c55ccf3e20",
    "shadow_policy_sha256": "e0c50d8af4bffe554798f2b24bd6177da2faa7d6bbbdf3645b73563ffe1694f0",
    "retest_policy_sha256": "eaa0951691054622e13c59ce956d01a678bc596e452e00d6e5646ee6fadb1ebd",
    "outcome_policy_sha256": "38b8e2ba8681460927dcfedc5f480cffaf61367d8fa5f536271b29ce04faf4a9",
}
ALLOWED_FROZEN_MODIFIED = {"src/signalbot/persistence/repository.py"}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    if baseline.get("schema_version") != "worktree-source-v1":
        raise SystemExit("unexpected baseline schema")
    baseline_identity = f"{baseline['schema_version']}:{baseline['source_root_sha256']}"
    if baseline_identity != EXPECTED_BASELINE:
        raise SystemExit(f"unexpected baseline identity: {baseline_identity}")
    frozen = freeze_source(default_source_root())
    current = {item.path: item.sha256 for item in frozen.files}
    before = dict(baseline["files"])
    added = sorted(set(current) - set(before))
    removed = sorted(set(before) - set(current))
    modified = sorted(
        path for path in set(current) & set(before) if current[path] != before[path]
    )

    settings_path = (
        ROOT
        / "var"
        / "prospective"
        / "causal-retest-prospective-phase-m-corrective-20260826-v1"
        / "campaign.yaml"
    )
    settings = load_settings(settings_path)
    identities = {
        "config_sha256": shadow_config_sha256(settings),
        "shadow_policy_sha256": shadow_policy_identity(settings.shadow, settings.signals),
        "retest_policy_sha256": retest_policy_for_horizon(
            settings.shadow.retest_horizon_bars
        ).sha256,
        "outcome_policy_sha256": RetestOutcomePolicy().sha256,
    }
    parity = {
        name: {
            "expected": expected,
            "observed": identities[name],
            "match": identities[name] == expected,
        }
        for name, expected in EXPECTED_IDENTITIES.items()
    }
    candidate_payload = {
        **frozen.as_dict(),
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_identity": frozen.source_identity,
    }
    _write_json(CANDIDATE_FREEZE_PATH, candidate_payload)
    root_file_parity = {
        path: {
            "baseline_sha256": before[path],
            "candidate_sha256": current[path],
            "match": before[path] == current[path],
        }
        for path in ("pyproject.toml", "uv.lock")
    }
    receipt = {
        "schema_version": "phase_r_source_diff_receipt_v1",
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "baseline": {
            "identity": baseline_identity,
            "manifest_path": str(BASELINE_PATH.relative_to(ROOT)).replace("\\", "/"),
            "manifest_sha256": _sha256_file(BASELINE_PATH),
            "file_count": len(before),
        },
        "candidate": {
            "identity": frozen.source_identity,
            "manifest_path": str(CANDIDATE_FREEZE_PATH.relative_to(ROOT)).replace("\\", "/"),
            "file_count": len(current),
            "git_head": frozen.git_head,
        },
        "frozen_delta": {
            "added": added,
            "removed": removed,
            "modified": modified,
            "allowed_modified": sorted(ALLOWED_FROZEN_MODIFIED),
            "unexpected_modified": sorted(set(modified) - ALLOWED_FROZEN_MODIFIED),
            "delta_gate_pass": (
                not added and not removed and set(modified) <= ALLOWED_FROZEN_MODIFIED
            ),
        },
        "root_file_parity": root_file_parity,
        "scientific_parity": parity,
        "governance": {
            "strategy_policy_r2_shadow_retest_outcome_unchanged": all(
                item["match"] for item in parity.values()
            ),
            "oci_mutation": False,
            "commit_push": False,
            "private_api_or_orders": False,
        },
    }
    _write_json(DIFF_RECEIPT_PATH, receipt)
    print(
        json.dumps(
            {
                "candidate_identity": frozen.source_identity,
                "modified": modified,
                "unexpected_modified": receipt["frozen_delta"]["unexpected_modified"],
                "root_file_parity": all(item["match"] for item in root_file_parity.values()),
                "scientific_parity": all(item["match"] for item in parity.values()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
