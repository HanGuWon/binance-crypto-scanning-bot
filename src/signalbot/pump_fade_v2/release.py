"""Build a deterministic, disabled local Pump-fade package and evidence receipt."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from signalbot.pump_fade_v2 import POLICY_VERSION
from signalbot.pump_fade_v2.shadow import preview_payload
from signalbot.pump_fade_v2.state import Decision, State

SOURCE_PLAN_SHA256 = "bb0504050b157c0456fe309731a4ea2954cbbb9a1d2e4773d21edbbab522e986"
PACKAGE_MODULE_PREFIX = "signalbot.pump_fade_v2"
EVIDENCE_INPUTS = (
    "docs/pump_fade_v2/SOURCE_VERIFICATION_20261007.json",
    "docs/pump_fade_v2/API_SOURCE_VERIFICATION_20261007.md",
    "docs/pump_fade_v2/PREREGISTRATION_V2.md",
    "docs/pump_fade_v2/PREREGISTRATION_AMENDMENT_B.md",
    "docs/pump_fade_v2/PREREGISTRATION_REMEDIATION_C.md",
    "docs/pump_fade_v2/OFFLINE_RESULTS_V2C.md",
    "docs/pump_fade_v2/offline_public_kline_proxy_v2c_20261007/results.json",
    "docs/pump_fade_v2/offline_public_kline_proxy_v2c_20261007/proxy_events.jsonl",
    "docs/pump_fade_v2/offline_public_kline_proxy_v2c_20261007/data_manifest.json",
    "docs/pump_fade_v2/REMEDIATION_ACCEPTANCE_MATRIX.md",
    "docs/pump_fade_v2/PREREGISTRATION_REMEDIATION_D.md",
    "docs/pump_fade_v2/IMPLEMENTATION_REPORT_V2D.md",
    "docs/pump_fade_v2/OCI_RELEASE_RUNBOOK_V2D.md",
    "docs/pump_fade_v2/FORWARD_EVALUATION_RUNBOOK_V2D.md",
    "docs/pump_fade_v2/SAMPLE_ALERTS_V2D.md",
)
V2D_CONFIGURATION = (
    "config/pump_fade_v2/policy_v2d.json",
    "config/pump_fade_v2/freeze_manifest_v2d.json",
    "config/pump_fade_v2/forward_evaluation_v2d.json",
    "docs/pump_fade_v2/PREREGISTRATION_REMEDIATION_D.md",
)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(path: Path) -> bytes:
    """Text input bytes with CRLF normalized to LF.

    Git (``core.autocrlf``) checks the same blob out as CRLF on Windows and LF on
    Linux. Identities are taken over the LF form, which equals the Git blob, so a
    rebuild from a clean checkout on any platform reproduces the same digests.
    """

    return path.read_bytes().replace(b"\r\n", b"\n")


def _sha256_text_file(path: Path) -> str:
    return hashlib.sha256(_canonical_bytes(path)).hexdigest()


def _identity(checksums: dict[str, str]) -> str:
    canonical = json.dumps(checksums, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


def _module_path(root: Path, module: str) -> Path | None:
    source_root = root / "src"
    candidate = source_root.joinpath(*module.split("."))
    module_file = candidate.with_suffix(".py")
    package_file = candidate / "__init__.py"
    if module_file.is_file():
        return module_file
    if package_file.is_file():
        return package_file
    return None


def _local_imports(path: Path, module_name: str, root: Path) -> set[Path]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    is_package = path.name == "__init__.py"
    package_name = module_name if is_package else module_name.rpartition(".")[0]
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = package_name
            if node.level:
                for _ in range(node.level - 1):
                    base = base.rpartition(".")[0]
                if node.module:
                    base = f"{base}.{node.module}" if base else node.module
            elif node.module:
                base = node.module
            if base:
                imports.add(base)
                imports.update(f"{base}.{alias.name}" for alias in node.names)
    result: set[Path] = set()
    for imported in imports:
        if imported == "signalbot" or imported.startswith("signalbot."):
            resolved = _module_path(root, imported)
            if resolved is None:
                parent = imported.rpartition(".")[0]
                if parent and _module_path(root, parent) is not None:
                    continue  # imported symbol, not a submodule
                raise ValueError(f"unresolved local execution dependency: {imported}")
            result.add(resolved)
    return result


def source_dependency_closure(root: Path) -> tuple[Path, ...]:
    """Resolve all package modules and transitive local Python imports from AST."""

    root = root.resolve()
    package_dir = root / "src/signalbot/pump_fade_v2"
    modules = sorted(package_dir.glob("*.py"))
    if not modules:
        raise FileNotFoundError("Pump-fade package source is missing")
    pending: list[tuple[Path, str]] = []
    for path in modules:
        name = PACKAGE_MODULE_PREFIX if path.name == "__init__.py" else (
            f"{PACKAGE_MODULE_PREFIX}.{path.stem}"
        )
        pending.append((path, name))
    visited: dict[Path, str] = {}
    while pending:
        path, module_name = pending.pop()
        resolved = path.resolve()
        if resolved in visited:
            continue
        if not resolved.is_relative_to(root / "src"):
            raise ValueError("execution dependency escapes source root")
        visited[resolved] = module_name
        package_dir = resolved.parent
        while package_dir != root / "src":
            initializer = package_dir / "__init__.py"
            if initializer.is_file():
                relative_parts = initializer.relative_to(root / "src").parent.parts
                pending.append((initializer, ".".join(relative_parts)))
            package_dir = package_dir.parent
        for dependency in _local_imports(resolved, module_name, root):
            name = dependency.relative_to(root / "src").with_suffix("")
            parts = list(name.parts)
            if parts[-1] == "__init__":
                parts.pop()
            pending.append((dependency, ".".join(parts)))
    return tuple(sorted(visited, key=lambda path: path.relative_to(root).as_posix()))


def source_closure_manifest(root: Path) -> dict[str, str]:
    """Map every resolved local execution dependency to its content digest."""

    root = root.resolve()
    return {
        path.relative_to(root).as_posix(): _sha256_text_file(path)
        for path in source_dependency_closure(root)
    }


def _write_deterministic_package(root: Path, files: tuple[Path, ...], target: Path) -> str:
    """Write a stable zip containing source closure, policy, and locked inputs."""

    package_files = (*files, root / "pyproject.toml", root / "uv.lock",
                     *(root / name for name in V2D_CONFIGURATION))
    unique = {path.resolve() for path in package_files}
    if any(not path.is_file() or not path.resolve().is_relative_to(root.resolve())
           for path in unique):
        raise ValueError("release package contains a missing or escaping input")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".partial")
    if temporary.exists():
        raise FileExistsError("partial package requires operator review")
    with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED,
                         compresslevel=9) as archive:
        for path in sorted(unique, key=lambda item: item.relative_to(root).as_posix()):
            relative = path.relative_to(root).as_posix()
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, _canonical_bytes(path), compress_type=zipfile.ZIP_DEFLATED,
                             compresslevel=9)
    if target.exists():
        if target.read_bytes() != temporary.read_bytes():
            temporary.unlink()
            raise FileExistsError("immutable package path exists with different bytes")
        temporary.unlink()
    else:
        temporary.replace(target)
    return _sha256_file(target)


def _write_immutable_json(path: Path, value: dict[str, Any]) -> None:
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    if path.exists():
        if path.read_bytes() != payload:
            raise FileExistsError("immutable release output exists with different bytes")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _package_name(revision: str) -> str:
    if revision and not all(ch.isalnum() or ch in "-_." for ch in revision):
        raise ValueError("release revision may contain only letters, digits, '-', '_' and '.'")
    return f"pump_fade_v2d{revision}_package.zip"


def build_release_evidence(
    root: Path, output_dir: Path, *, revision: str = "",
) -> dict[str, Any]:
    """Hash the complete executable closure separately from research evidence.

    ``revision`` (for example ``"-r1"``) names a rebuilt candidate after a technical
    fix to the same registered v2d contract; the default keeps the original name.
    """

    root = root.resolve()
    freeze_path = root / "config/pump_fade_v2/freeze_manifest_v2d.json"
    policy_path = root / "config/pump_fade_v2/policy_v2d.json"
    contract_path = root / "docs/pump_fade_v2/PREREGISTRATION_REMEDIATION_D.md"
    freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
    if freeze["policy_version"] != POLICY_VERSION:
        raise ValueError("frozen policy differs from active release package")
    if freeze["source_plan_sha256"] != SOURCE_PLAN_SHA256:
        raise ValueError("v2d freeze source-plan authority is not verified")
    policy_hash = _sha256_text_file(policy_path)
    contract_hash = _sha256_text_file(contract_path)
    if policy_hash != freeze["policy_config_sha256"]:
        raise ValueError("v2d policy differs from immutable engineering freeze")
    if contract_hash != freeze["remediation_contract_sha256"]:
        raise ValueError("v2d remediation contract differs from engineering freeze")
    verification = json.loads(
        (root / "docs/pump_fade_v2/SOURCE_VERIFICATION_20261007.json")
        .read_text(encoding="utf-8")
    )
    if verification["strategy_plan"]["sha256"] != SOURCE_PLAN_SHA256:
        raise ValueError("source verification receipt differs from source authority")

    closure = source_dependency_closure(root)
    source_hashes = source_closure_manifest(root)
    lock_hashes = {
        name: _sha256_text_file(root / name) for name in ("pyproject.toml", "uv.lock")
    }
    config_hashes = {name: _sha256_text_file(root / name) for name in V2D_CONFIGURATION}
    executable_inputs = {**source_hashes, **lock_hashes, **config_hashes}
    executable_identity = _identity(executable_inputs)
    package_name = _package_name(revision)
    package_hash = _write_deterministic_package(root, closure, output_dir / package_name)
    evidence_hashes: dict[str, str] = {}
    for name in EVIDENCE_INPUTS:
        path = root / name
        if not path.is_file() or not path.resolve().is_relative_to(root):
            raise ValueError(f"missing or unapproved evidence input: {name}")
        evidence_hashes[name] = _sha256_text_file(path)
    evidence_identity = _identity(evidence_hashes)
    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    if policy["evaluation"]["squeeze_adverse_mark_excursion_pct"] != 15.0:
        raise ValueError("v2d primary squeeze estimand must remain 15 percent")

    sample = Decision(
        event_id="SYNTHETIC_EXAMPLE_ONLY_DO_NOT_TRADE", policy_version=POLICY_VERSION,
        state=State.FADE_CANDIDATE, decision_at_ms=1_799_282_976_000,
        reasons=("60 minutes without a new fully closed five-minute high",),
        releases=("R1_NO_HIGH_60M",), continuation_risk=False,
        invalidation_price=130.0, expires_at_ms=1_799_369_376_000,
        metrics=(("spread_bps", 10.0), ("trailing_1h_return_pct", 1.0)),
        validation_tier="EXPLORATORY",
    )
    result: dict[str, Any] = {
        "schema_version": "pump_fade_v2_disabled_release_manifest_v2",
        "status": "LOCAL_PREPARED_NO_DEPLOYMENT",
        "policy_version": POLICY_VERSION,
        "freeze_utc": freeze["engineering_freeze_utc"],
        "parent_git_commit": freeze["source_parent_commit"],
        "executable_source_closure": source_hashes,
        "executable_source_closure_sha256": _identity(source_hashes),
        "runtime_lock_inputs_sha256": lock_hashes,
        "policy_and_freeze_inputs_sha256": config_hashes,
        "executable_tree_sha256": executable_identity,
        "executable_package_path": package_name,
        "release_revision": revision or "initial",
        "hash_canonicalization": "sha256 over file bytes with CRLF normalized to LF",
        "executable_package_sha256": package_hash,
        "research_evidence_inputs_sha256": evidence_hashes,
        "research_evidence_tree_sha256": evidence_identity,
        "source_plan_verified": verification["strategy_plan"][
            "matches_user_supplied_snapshot"
        ],
        "source_plan_sha256": SOURCE_PLAN_SHA256,
        "historical_outcomes_exposed_before_v2d": True,
        "confirmatory_historical_claims": False,
        "primary_squeeze_adverse_mark_excursion_pct": 15.0,
        "historical_efficacy": "UNAVAILABLE_OFFICIAL_PIT_EXECUTION_AND_FUNDING_DATA",
        "forward_eligible": False,
        "capture_enabled": False,
        "shadow_discord_enabled": False,
        "production_orders_enabled": False,
        "production_scanner_changes_applied": False,
        "independent_full_suite": "REQUIRED_ON_COMPATIBLE_AUTHORIZED_WINDOWS_RUNNER",
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_immutable_json(output_dir / "release_manifest.json", result)
    _write_immutable_json(
        output_dir / "sample_shadow_payload.json",
        preview_payload(sample, spread_bps=10.0, cost_scenario_bps=30.0),
    )
    return result


def main(argv: Sequence[str] | None = None) -> None:
    """Write deterministic package and manifest to a separate v2d directory."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--revision", default="", help="for example -r1; default: none")
    args = parser.parse_args(argv)
    result = build_release_evidence(args.repo_root, args.output_dir, revision=args.revision)
    print(json.dumps({
        "status": result["status"],
        "executable_tree_sha256": result["executable_tree_sha256"],
        "executable_package_sha256": result["executable_package_sha256"],
        "deployment_enabled": False,
    }, indent=2))


if __name__ == "__main__":
    main()
