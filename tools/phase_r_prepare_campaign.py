"""Prepare a non-active Phase-R campaign config and empty preregistration root."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

from signalbot.config import load_settings

OLD_CAMPAIGN_ID = "causal-retest-prospective-phase-m-corrective-20260826-v1"
EXPECTED_CAMPAIGN_PREFIX = "causal-retest-prospective-phase-r-oci-"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _replace_line(text: str, pattern: str, replacement: str) -> str:
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.MULTILINE)
    if count != 1:
        raise ValueError(f"expected one config line matching {pattern!r}")
    return updated


def prepare(
    *,
    template: Path,
    config_output: Path,
    staging_root: Path,
    campaign_id: str,
    created_ms: int,
    activation_ms: int,
    source_identity: str,
    quota_bytes: int,
) -> dict[str, object]:
    if not campaign_id.startswith(EXPECTED_CAMPAIGN_PREFIX):
        raise ValueError("campaign id is outside the Phase-R namespace")
    if created_ms >= activation_ms:
        raise ValueError("campaign creation must precede activation")
    if template.resolve() == config_output.resolve():
        raise ValueError("template and output must be distinct")
    if config_output.exists():
        raise FileExistsError(config_output)
    campaign_root = staging_root / campaign_id
    if campaign_root.exists():
        raise FileExistsError(campaign_root)
    text = template.read_text(encoding="utf-8")
    if text.count(OLD_CAMPAIGN_ID) < 3:
        raise ValueError("template does not contain the expected Phase-M paths")
    text = text.replace(OLD_CAMPAIGN_ID, campaign_id)
    text = text.replace(
        "worktree-source-v1:650fe603f79812e79cd9390c0d2e94fbab8071a4eab1cc95f40fc229c7f403dd",
        source_identity,
    )
    text = text.replace(
        "app_name: binance-signalbot-prospective-phase-m",
        "app_name: binance-signalbot-prospective-phase-r",
    )
    text = _replace_line(
        text, r"^  campaign_created_at_ms:.*$", f"  campaign_created_at_ms: {created_ms}"
    )
    text = _replace_line(text, r"^  activation_ms:.*$", f"  activation_ms: {activation_ms}")
    text = _replace_line(
        text,
        r"^  url: sqlite:///\.\/var/prospective/.*?/campaign\.db$",
        f"  url: sqlite:////var/lib/binance-bot-2/prospective-r/{campaign_id}/campaign.db",
    )
    text = _replace_line(
        text,
        r"^  raw_event_directory: \.\/var/prospective/.*?/raw-events$",
        f"  raw_event_directory: /var/lib/binance-bot-2/prospective-r/{campaign_id}/raw-events",
    )
    text = _replace_line(
        text, r"^  raw_event_max_bytes:.*$", f"  raw_event_max_bytes: {quota_bytes}"
    )
    config_output.parent.mkdir(parents=True, exist_ok=True)
    config_output.write_text(text, encoding="utf-8")
    settings = load_settings(config_output)
    if settings.shadow.campaign_id != campaign_id:
        raise ValueError("generated config campaign id mismatch")
    if settings.shadow.campaign_created_at_ms != created_ms:
        raise ValueError("generated config creation timestamp mismatch")
    if settings.shadow.activation_ms != activation_ms:
        raise ValueError("generated config activation timestamp mismatch")
    if settings.runtime.raw_event_max_bytes != quota_bytes:
        raise ValueError("generated config quota mismatch")
    for name in ("raw-events", "manifests", "logs"):
        (campaign_root / name).mkdir(parents=True)
    return {
        "campaign_id": campaign_id,
        "campaign_root": str(campaign_root),
        "config": str(config_output),
        "config_sha256": _sha256(config_output),
        "created_ms": created_ms,
        "activation_ms": activation_ms,
        "source_identity": source_identity,
        "quota_bytes": quota_bytes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", type=Path, required=True)
    parser.add_argument("--config-output", type=Path, required=True)
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--created-ms", type=int, required=True)
    parser.add_argument("--activation-ms", type=int, required=True)
    parser.add_argument("--source-identity", required=True)
    parser.add_argument("--quota-bytes", type=int, required=True)
    args = parser.parse_args()
    payload = prepare(
        template=args.template,
        config_output=args.config_output,
        staging_root=args.staging_root,
        campaign_id=args.campaign_id,
        created_ms=args.created_ms,
        activation_ms=args.activation_ms,
        source_identity=args.source_identity,
        quota_bytes=args.quota_bytes,
    )
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
