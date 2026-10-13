"""Temporary Git fixtures only: no real holdout is inspected."""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from signalbot.pump_fade_v3.holdout_guard import (
    HoldoutAuthorizationError,
    TrustedFreeze,
    add_holdout_unseal_argument,
    claim_one_shot,
    read_sealed_file,
    validate_unseal_token,
)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def test_cli_requires_explicit_holdout_unseal_token() -> None:
    parser = argparse.ArgumentParser()
    add_holdout_unseal_argument(parser)
    with pytest.raises(SystemExit):
        parser.parse_args([])
    token = "a" * 40
    assert parser.parse_args(["--holdout-unseal", token]).holdout_unseal == token


@pytest.fixture
def freeze(tmp_path: Path) -> tuple[Path, Path, TrustedFreeze, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-q")
    git(repo, "config", "user.email", "fixture@example.invalid")
    git(repo, "config", "user.name", "Fixture")
    files = {
        "config/pump_fade_v3/policy_v3.json": b'{}',
        "docs/pump_fade_v3/PREREGISTRATION_V3.md": b"frozen registration\n",
        "src/signalbot/pump_fade_v3/holdout_guard.py": b"frozen guard\n",
    }
    bindings = {
        "policy_sha256": digest(files["config/pump_fade_v3/policy_v3.json"]),
        "preregistration_sha256": digest(files["docs/pump_fade_v3/PREREGISTRATION_V3.md"]),
        "guard_sha256": digest(files["src/signalbot/pump_fade_v3/holdout_guard.py"]),
    }
    files["config/pump_fade_v3/freeze_manifest_v3.json"] = json.dumps(bindings).encode()
    for name, data in files.items():
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    git(repo, "add", ".")
    git(repo, "commit", "-qm", "freeze")
    commit = git(repo, "rev-parse", "HEAD")
    trusted = TrustedFreeze(commit, {name: digest(data) for name, data in files.items()})
    seal = tmp_path / "HOLDOUT_SEAL.json"
    seal.write_text(json.dumps({
        "schema_version": "pump_fade_v3_holdout_seal_v1",
        "freeze_commit_sha": commit,
        "freeze_manifest_sha256": trusted.file_sha256[
            "config/pump_fade_v3/freeze_manifest_v3.json"
        ],
        **bindings,
        "holdout_spec": {
            "start_utc": "2026-07-01T00:00:00Z",
            "end_utc": "2026-10-06T23:59:59Z",
            "universe": "binance_usdm_usdt_perpetual",
            "include_delisted": True,
        },
    }))
    return repo, seal, trusted, commit


@pytest.mark.parametrize("token", [None, "", "0" * 39, "0" * 41, "G" * 40,
                                      "A" * 40, " " + "0" * 40, "f" * 40])
def test_reject_before_any_sealed_access(
    freeze: tuple[Path, Path, TrustedFreeze, str], token: str | None,
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, seal, trusted, commit = freeze
    sealed = tmp_path / "sealed"
    # Spy on Path methods without requiring writable instance attributes.
    original_stat = type(sealed).stat
    original_open = type(sealed).open
    original_iterdir = type(sealed).iterdir

    def guarded_stat(self: Path, *args: object, **kwargs: object) -> object:
        if self == sealed:
            raise AssertionError("sealed path stat before authorization")
        return original_stat(self, *args, **kwargs)

    def guarded_open(self: Path, *args: object, **kwargs: object) -> object:
        if self == sealed:
            raise AssertionError("sealed path open before authorization")
        opener: Any = original_open
        return opener(self, *args, **kwargs)

    def guarded_iterdir(self: Path) -> object:
        if self == sealed:
            raise AssertionError("sealed path iteration before authorization")
        return original_iterdir(self)

    monkeypatch.setattr(type(sealed), "stat", guarded_stat)
    monkeypatch.setattr(type(sealed), "open", guarded_open)
    monkeypatch.setattr(type(sealed), "iterdir", guarded_iterdir)
    with pytest.raises(HoldoutAuthorizationError):
        validate_unseal_token(token if token != commit else "0" * 40,
                              repo_root=repo, seal_file=seal, trusted=trusted)


def test_verified_exact_commit_and_one_shot(
    freeze: tuple[Path, Path, TrustedFreeze, str], tmp_path: Path,
) -> None:
    repo, seal, trusted, commit = freeze
    capability = validate_unseal_token(commit, repo_root=repo, seal_file=seal,
                                       trusted=trusted)
    assert capability.commit_sha == commit
    sealed = tmp_path / "sealed"
    sealed.mkdir()
    (sealed / "data.bin").write_bytes(b"fixture")
    receipt = tmp_path / "claim.json"
    claimed = claim_one_shot(capability, one_shot_receipt=receipt)
    assert read_sealed_file(claimed, sealed_root=sealed, relative_path="data.bin",
                            expected_sha256=digest(b"fixture")) == b"fixture"
    with pytest.raises(HoldoutAuthorizationError, match="already claimed"):
        claim_one_shot(capability, one_shot_receipt=receipt)
    with pytest.raises(HoldoutAuthorizationError, match="digest mismatch"):
        read_sealed_file(claimed, sealed_root=sealed, relative_path="data.bin",
                         expected_sha256="a" * 64)
    with pytest.raises(HoldoutAuthorizationError, match="unsafe relative"):
        read_sealed_file(claimed, sealed_root=sealed, relative_path="../data.bin",
                         expected_sha256=digest(b"fixture"))
    with pytest.raises(HoldoutAuthorizationError, match="one-shot claim required"):
        read_sealed_file(capability, sealed_root=sealed, relative_path="data.bin",  # type: ignore[arg-type]
                         expected_sha256=digest(b"fixture"))


def test_tampered_frozen_blob_and_seal_rejected(
    freeze: tuple[Path, Path, TrustedFreeze, str],
) -> None:
    repo, seal, trusted, commit = freeze
    wrong_pin = TrustedFreeze(commit, {**trusted.file_sha256,
        "config/pump_fade_v3/policy_v3.json": "a" * 64})
    with pytest.raises(HoldoutAuthorizationError, match="blob"):
        validate_unseal_token(commit, repo_root=repo, seal_file=seal, trusted=wrong_pin)
    payload = json.loads(seal.read_text())
    payload["freeze_commit_sha"] = "b" * 40
    seal.write_text(json.dumps(payload))
    with pytest.raises(HoldoutAuthorizationError, match="seal"):
        validate_unseal_token(commit, repo_root=repo, seal_file=seal, trusted=trusted)


def test_freeze_commit_not_self_referential(
    freeze: tuple[Path, Path, TrustedFreeze, str],
) -> None:
    repo, _seal, _trusted, commit = freeze
    manifest = json.loads(git(
        repo, "show", f"{commit}:config/pump_fade_v3/freeze_manifest_v3.json",
    ))
    assert "freeze_commit_sha" not in manifest
    assert "commit_sha" not in manifest


def test_real_but_stale_commit_rejected_before_sealed_access(
    freeze: tuple[Path, Path, TrustedFreeze, str], tmp_path: Path,
) -> None:
    repo, seal, trusted, commit = freeze
    (repo / "later.txt").write_text("later unrelated commit")
    git(repo, "add", "later.txt")
    git(repo, "commit", "-qm", "later")
    other_valid_commit = git(repo, "rev-parse", "HEAD")
    assert other_valid_commit != commit
    # No sealed directory exists: a stale authorization must be rejected before
    # any code can receive its path, including when the SHA is a real Git commit.
    assert not (tmp_path / "sealed").exists()
    with pytest.raises(HoldoutAuthorizationError, match="wrong or stale"):
        validate_unseal_token(other_valid_commit, repo_root=repo,
                              seal_file=seal, trusted=trusted)


def test_repository_freeze_manifest_binds_current_l1_inputs_without_self_sha() -> None:
    root = Path(__file__).resolve().parents[2]
    manifest = json.loads(
        (root / "config/pump_fade_v3/freeze_manifest_v3.json").read_text(encoding="utf-8")
    )
    assert "freeze_commit_sha" not in manifest
    assert "commit_sha" not in manifest
    bindings = {
        "policy_sha256": root / "config/pump_fade_v3/policy_v3.json",
        "preregistration_sha256": root / "docs/pump_fade_v3/PREREGISTRATION_V3.md",
        "guard_sha256": root / "src/signalbot/pump_fade_v3/holdout_guard.py",
    }
    for key, path in bindings.items():
        assert manifest[key] == digest(path.read_bytes())
    assert manifest["holdout_spec"] == {
        "start_utc": "2026-07-01T00:00:00Z",
        "end_utc": "2026-10-06T23:59:59Z",
        "universe": "binance_usdm_usdt_perpetual",
        "include_delisted": True,
    }
