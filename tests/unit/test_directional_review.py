import json
import shutil
from pathlib import Path

import pytest

from signalbot.prospective.directional_review import review_directional_validation


TRACKED_RECEIPT_DIR = Path("artifacts/prospective/futures-bidirectional-v1")


def _receipt_copy(tmp_path: Path) -> Path:
    """Run the review against a copy: it writes independent-review.json."""

    destination = tmp_path / "futures-bidirectional-v1"
    shutil.copytree(TRACKED_RECEIPT_DIR, destination)
    return destination


def test_review_rejects_missing_validation_manifest(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        review_directional_validation(
            "config/research.futures-bidirectional.v1.yaml",
            tmp_path,
            workspace_root=Path.cwd(),
        )


def test_review_writes_blockers_for_current_not_started_receipt(tmp_path: Path) -> None:
    receipt_dir = _receipt_copy(tmp_path)
    result = review_directional_validation(
        "config/research.futures-bidirectional.v1.yaml",
        receipt_dir,
        workspace_root=Path.cwd(),
    )

    assert result["status"] == "BLOCKED"
    assert "forward shadow evidence is incomplete" in result["blockers"]
    assert "Freqtrade sidecar was not installed or executed" in result["blockers"]
    written = json.loads((receipt_dir / "independent-review.json").read_text())
    assert written["review_sha256"] == result["review_sha256"]


def test_review_does_not_require_itself_in_validation_output_hashes(tmp_path: Path) -> None:
    receipt_dir = _receipt_copy(tmp_path)
    result = review_directional_validation(
        "config/research.futures-bidirectional.v1.yaml",
        receipt_dir,
        workspace_root=Path.cwd(),
    )

    assert "independent-review.json" not in result["output_hash_mismatches"]
