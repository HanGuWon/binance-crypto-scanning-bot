"""Generate the immutable preregistration manifest for the OCI successor.

This is an operational migration tool.  It derives scientific identities from
the installed application and refuses to overwrite an existing manifest or
accept an activation time that is no longer in the future. The campaign root
may already contain only the three directories created by O2 transport
preflight: ``raw-events``, ``manifests``, and ``logs``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from signalbot.config import load_settings
from signalbot.prospective.observer import shadow_config_sha256
from signalbot.prospective.retest import retest_policy_for_horizon
from signalbot.prospective.retest_outcomes import RetestOutcomePolicy
from signalbot.prospective.source_freeze import default_source_root, freeze_source
from signalbot.signals.shadow_policy import shadow_policy_identity

EXPECTED_IDENTITIES = {
    "shadow_policy_sha256": "e0c50d8af4bffe554798f2b24bd6177da2faa7d6bbbdf3645b73563ffe1694f0",
    "config_sha256": "43971efece98b71f815a4e4f476e18e3467f9a8cc02de3d51b8be1c55ccf3e20",
    "retest_policy_sha256": "eaa0951691054622e13c59ce956d01a678bc596e452e00d6e5646ee6fadb1ebd",
    "outcome_policy_sha256": "38b8e2ba8681460927dcfedc5f480cffaf61367d8fa5f536271b29ce04faf4a9",
}
EXPECTED_CAMPAIGN_PREFIX = "causal-retest-prospective-phase-o-oci-"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _utc_iso(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).isoformat()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--hostname", required=True)
    parser.add_argument("--expected-source", required=True)
    parser.add_argument(
        "--campaign-prefix",
        default=EXPECTED_CAMPAIGN_PREFIX,
        help="campaign namespace prefix permitted for this registration",
    )
    parser.add_argument(
        "--manifest-config-path",
        help="path recorded in the manifest when it differs from the local input path",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="verify an existing manifest instead of creating one",
    )
    return parser.parse_args()


def _scientific_identities(settings: Any) -> dict[str, str]:
    identities = {
        "source_identity": freeze_source(default_source_root()).source_identity,
        "shadow_policy_sha256": shadow_policy_identity(
            settings.shadow, settings.signals
        ),
        "config_sha256": shadow_config_sha256(settings),
        "retest_policy_sha256": retest_policy_for_horizon(
            settings.shadow.retest_horizon_bars
        ).sha256,
        "outcome_policy_sha256": RetestOutcomePolicy().sha256,
    }
    for name, expected in EXPECTED_IDENTITIES.items():
        if identities[name] != expected:
            raise SystemExit(
                f"scientific identity mismatch: {name} "
                f"{identities[name]} != {expected}"
            )
    return identities


def build_preregistration(
    *,
    config_path: Path,
    output_path: Path,
    hostname: str,
    expected_source: str,
    campaign_prefix: str = EXPECTED_CAMPAIGN_PREFIX,
    manifest_config_path: str | None = None,
) -> dict[str, Any]:
    settings = load_settings(config_path)
    shadow = settings.shadow
    campaign_id = shadow.campaign_id
    created_ms = shadow.campaign_created_at_ms
    activation_ms = shadow.activation_ms
    if campaign_id is None or created_ms is None or activation_ms is None:
        raise SystemExit("prospective campaign metadata is incomplete")
    if not campaign_id.startswith(campaign_prefix):
        raise SystemExit(f"unexpected campaign namespace: {campaign_id}")
    if created_ms >= activation_ms:
        raise SystemExit("campaign_created_at_ms must precede activation_ms")
    now_ms = int(datetime.now(tz=UTC).timestamp() * 1000)
    if activation_ms <= now_ms:
        raise SystemExit("activation_ms must be in the future at preregistration")
    if output_path.name != "preregistration.json" or output_path.parent.name != campaign_id:
        raise SystemExit("output path must be the campaign's preregistration.json")
    if output_path.exists():
        raise SystemExit(f"preregistration already exists: {output_path}")
    if output_path.parent.exists():
        allowed_preflight_entries = {"raw-events", "manifests", "logs"}
        unexpected = {
            path.name
            for path in output_path.parent.iterdir()
            if path.name not in allowed_preflight_entries
        }
        if unexpected:
            raise SystemExit(
                "campaign path contains unexpected entries: "
                + ", ".join(sorted(unexpected))
            )
    prospective_root = output_path.parent.parent
    matching_campaign_roots = [
        path for path in prospective_root.iterdir() if path.name == campaign_id
    ]
    if matching_campaign_roots != [output_path.parent]:
        raise SystemExit(f"campaign id is not unique under {prospective_root}")

    identities = _scientific_identities(settings)
    if identities["source_identity"] != expected_source:
        raise SystemExit(
            f"source identity mismatch: {identities['source_identity']} != {expected_source}"
        )
    if not output_path.parent.exists():
        output_path.parent.mkdir(parents=False)
    manifest: dict[str, Any] = {
        "schema_version": "oci_prospective_preregistration_v1",
        "campaign_id": campaign_id,
        "campaign_mode": shadow.campaign_mode,
        "host": hostname,
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "campaign_created_at_ms": created_ms,
        "campaign_created_at_utc": _utc_iso(created_ms),
        "activation_ms": activation_ms,
        "activation_utc": _utc_iso(activation_ms),
        "source_identity": identities["source_identity"],
        "scientific_identities": {
            "shadow_policy_sha256": identities["shadow_policy_sha256"],
            "config_sha256": identities["config_sha256"],
            "retest_policy_sha256": identities["retest_policy_sha256"],
            "outcome_policy_sha256": identities["outcome_policy_sha256"],
        },
        "config": {
            "path": manifest_config_path or str(config_path),
            "sha256": _sha256_file(config_path),
        },
        "markets": sorted(item.value for item in settings.binance.markets),
        "primary_interval": settings.binance.primary_interval,
        "storage": {
            "mode": settings.runtime.storage_mode,
            "database_url": settings.storage.url,
            "raw_event_directory": settings.runtime.raw_event_directory,
            "quota_bytes": settings.runtime.raw_event_max_bytes,
        },
        "alerts": {"discord_enabled": settings.alerts.discord_enabled},
        "activation_contract": "prospective_public_market_data_only_no_orders",
    }
    output_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest


def verify_preregistration(
    *,
    config_path: Path,
    output_path: Path,
    hostname: str,
    expected_source: str,
    campaign_prefix: str = EXPECTED_CAMPAIGN_PREFIX,
    manifest_config_path: str | None = None,
) -> dict[str, Any]:
    if not output_path.is_file():
        raise SystemExit(f"preregistration manifest is missing: {output_path}")
    try:
        manifest = json.loads(output_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"preregistration manifest is not valid JSON: {output_path}") from exc
    if not isinstance(manifest, dict):
        raise SystemExit("preregistration manifest must be a JSON object")
    settings = load_settings(config_path)
    campaign_id = settings.shadow.campaign_id
    if campaign_id is None or manifest.get("campaign_id") != campaign_id:
        raise SystemExit("manifest campaign_id does not match the config")
    if not campaign_id.startswith(campaign_prefix):
        raise SystemExit("manifest campaign namespace does not match the expected prefix")
    if manifest.get("host") != hostname:
        raise SystemExit("manifest hostname does not match the expected host")
    if manifest.get("source_identity") != expected_source:
        raise SystemExit("manifest source identity does not match the expected source")
    identities = _scientific_identities(settings)
    if manifest.get("scientific_identities") != {
        name: identities[name]
        for name in EXPECTED_IDENTITIES
    }:
        raise SystemExit("manifest scientific identities do not match the frozen source")
    if manifest.get("config", {}).get("sha256") != _sha256_file(config_path):
        raise SystemExit("manifest config hash does not match the config file")
    if (
        manifest_config_path is not None
        and manifest.get("config", {}).get("path") != manifest_config_path
    ):
        raise SystemExit("manifest config path does not match the expected deployed path")
    if manifest.get("campaign_created_at_ms") != settings.shadow.campaign_created_at_ms:
        raise SystemExit("manifest creation timestamp does not match the config")
    if manifest.get("activation_ms") != settings.shadow.activation_ms:
        raise SystemExit("manifest activation timestamp does not match the config")
    if output_path.parent.name != campaign_id:
        raise SystemExit("manifest is not stored under its campaign directory")
    created_ms = manifest.get("campaign_created_at_ms")
    activation_ms = manifest.get("activation_ms")
    if not isinstance(created_ms, int) or not isinstance(activation_ms, int):
        raise SystemExit("manifest timestamps must be integers")
    if created_ms >= activation_ms:
        raise SystemExit("manifest campaign_created_at_ms must precede activation_ms")
    if activation_ms <= int(datetime.now(tz=UTC).timestamp() * 1000):
        raise SystemExit("manifest activation_ms is no longer in the future")
    siblings = [path for path in output_path.parent.parent.iterdir() if path.name == campaign_id]
    if siblings != [output_path.parent]:
        raise SystemExit("campaign id is not unique under the prospective root")
    print(
        json.dumps(
            {
                "campaign_id": campaign_id,
                "output": str(output_path),
                "source_identity": expected_source,
                "status": "verified",
            },
            sort_keys=True,
        )
    )
    return manifest


def main() -> int:
    args = _parse_args()
    kwargs = {
        "config_path": args.config.resolve(),
        "output_path": args.output.resolve(),
        "hostname": args.hostname,
        "expected_source": args.expected_source,
        "campaign_prefix": args.campaign_prefix,
        "manifest_config_path": args.manifest_config_path,
    }
    if args.verify:
        verify_preregistration(**kwargs)
        return 0
    manifest = build_preregistration(**kwargs)
    print(
        json.dumps(
            {
                "campaign_id": manifest["campaign_id"],
                "activation_ms": manifest["activation_ms"],
                "activation_utc": manifest["activation_utc"],
                "source_identity": manifest["source_identity"],
                "output": str(args.output.resolve()),
                "status": "preregistered",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
