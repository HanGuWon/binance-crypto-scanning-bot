"""Build a source-bound, secret-free Phase-R deployment bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import tarfile
from pathlib import Path

EXPECTED_SOURCE = (
    "worktree-source-v1:"
    "8f3ab5520769d71b054d489356c2fd774d31500b0deee5c3f65bc997e1c9d6e8"
)
MANIFEST_PATH = Path("health/_dev/phase-r-source-freeze-v1.json")
EXTRA_FILES = (
    "README.md",
    ".python-version",
    "tools/binance_ws_transport_preflight.py",
    "tools/oci_initial_health_check.py",
    "tools/oci_phase_p_storage_math.py",
    "tools/oci_operational_health.py",
    "tools/oci_preregistration.py",
    "tools/oci_storage_health.py",
    "tools/prospective_health_report_v2.py",
    "tools/prospective_operational_checkpoint.py",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build(output: Path) -> dict[str, object]:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    if manifest.get("source_identity") != EXPECTED_SOURCE:
        raise ValueError("source freeze is not the expected Phase-R candidate")
    frozen = [item["path"] for item in manifest["files"]]
    relative_paths = list(dict.fromkeys([*frozen, *EXTRA_FILES]))
    paths: list[tuple[Path, str]] = []
    for relative in relative_paths:
        path = Path(relative)
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"bundle input is missing or unsafe: {relative}")
        paths.append((path, path.as_posix()))
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise FileExistsError(output)
    with tarfile.open(output, mode="w:gz") as archive:
        for path, arcname in paths:
            archive.add(path, arcname=arcname, recursive=False)
    return {
        "schema_version": "phase_r_source_bundle_v1",
        "source_identity": EXPECTED_SOURCE,
        "frozen_file_count": len(frozen),
        "bundle_file_count": len(paths),
        "extra_files": list(EXTRA_FILES),
        "output": str(output),
        "sha256": _sha256(output),
        "bytes": output.stat().st_size,
        "excluded": [".venv", "var", "artifacts", "health", "devlog", "secrets", ".git"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    workspace = Path.cwd().resolve()
    if not output.is_relative_to(workspace / "artifacts" / "evidence"):
        raise SystemExit("bundle output must stay under artifacts/evidence")
    print(json.dumps(build(output), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
