"""Read-only independent review of a directional validation receipt set."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from signalbot.prospective.directional_candidates import (
    DirectionalPreregistration,
    PromotionEvidence,
    canonical_sha256,
    load_preregistration,
)
from signalbot.prospective.source_freeze import freeze_source

REVIEW_PROTOCOL_VERSION = "futures_bidirectional_independent_review_v1"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"receipt must be a JSON object: {path}")
    return payload


def review_directional_validation(
    preregistration_path: str | Path,
    receipt_dir: str | Path,
    *,
    workspace_root: str | Path,
) -> dict[str, Any]:
    """Recompute receipt integrity and report unresolved promotion blockers."""

    preregistration: DirectionalPreregistration = load_preregistration(preregistration_path)
    root = Path(receipt_dir)
    manifest_payload = _load_object(root / "validation-manifest.json")
    recorded_source = str(manifest_payload.get("source_identity", ""))
    current_source = freeze_source(workspace_root).source_identity
    checks: dict[str, bool] = {
        "source_identity_matches": current_source == recorded_source,
        "candidate_matches": manifest_payload.get("candidate_version")
        == preregistration.candidate_version,
        "config_matches": manifest_payload.get("config_sha256")
        == preregistration.config_sha256,
    }
    output_hashes = manifest_payload.get("outputs")
    if not isinstance(output_hashes, dict) or not output_hashes:
        raise ValueError("validation manifest outputs are missing")
    output_mismatches: list[str] = []
    for name, expected in sorted(output_hashes.items()):
        path = root / str(name)
        if not path.is_file() or _sha256_file(path) != expected:
            output_mismatches.append(str(name))
    checks["output_hashes_match"] = not output_mismatches

    evidence = PromotionEvidence.model_validate(_load_object(root / "promotion-evidence.json"))
    historical = _load_object(root / "historical-summary.json")
    receipts = {item.direction.value for item in evidence.directions}
    checks["both_directions_present"] = receipts == {"long", "short"}
    checks["historical_identity_matches"] = (
        historical.get("candidate_version") == evidence.candidate_version
        and historical.get("config_sha256") == evidence.config_sha256
        and historical.get("data_authority_sha256") == evidence.data_authority_sha256
    )

    blockers: list[str] = []
    if not all(checks.values()):
        blockers.append("receipt integrity or identity check failed")
    if manifest_payload.get("forward_shadow", {}).get("status") != "COMPLETE":
        blockers.append("forward shadow evidence is incomplete")
    if manifest_payload.get("freqtrade", {}).get("status") == "NOT_INSTALLED":
        blockers.append("Freqtrade sidecar was not installed or executed")
    if not evidence.independent_review_passed:
        blockers.append("promotion evidence does not yet contain an independent pass")
    status = "PASS" if not blockers else "BLOCKED"
    payload: dict[str, Any] = {
        "protocol_version": REVIEW_PROTOCOL_VERSION,
        "status": status,
        "candidate_version": preregistration.candidate_version,
        "source_identity_recorded": recorded_source,
        "source_identity_current": current_source,
        "checks": checks,
        "output_hash_mismatches": output_mismatches,
        "blockers": blockers,
        "historical_screen": evidence.historical_screen.value,
        "forward_shadow_status": manifest_payload.get("forward_shadow", {}).get("status"),
        "freqtrade_status": manifest_payload.get("freqtrade", {}).get("status"),
    }
    payload["review_sha256"] = canonical_sha256(payload)
    output = root / "independent-review.json"
    output.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return payload
