import hashlib
import json
import sqlite3
from pathlib import Path

import pytest
from tools.oci_directional_closeout import WRITER_UNITS, _assert_no_other_writers, closeout


def test_writer_scan_includes_timer_target_units() -> None:
    assert "binance-bot-2-health.service" in WRITER_UNITS
    assert "binance-bot-2-deep-health.service" in WRITER_UNITS


@pytest.fixture(autouse=True)
def _mock_writer_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "tools.oci_directional_closeout._assert_no_other_writers", lambda _root: None
    )


def _campaign(
    root: Path,
    campaign_id: str = "futures-bidirectional-test",
    source: str = "worktree-source-v1:" + "a" * 64,
) -> None:
    root.mkdir()
    with sqlite3.connect(root / "campaign.db") as connection:
        connection.execute("CREATE TABLE observations (id TEXT)")
        connection.execute("INSERT INTO observations VALUES ('one')")
        connection.execute(
            "CREATE TABLE shadow_campaigns (campaign_id TEXT, source_identity TEXT, "
            "rule_version TEXT, policy_sha256 TEXT, config_sha256 TEXT)"
        )
        connection.execute(
            "INSERT INTO shadow_campaigns VALUES (?, ?, 'v1', 'policy', 'config')",
            (campaign_id, source),
        )
    (root / "events.jsonl").write_text('{"ok":true}\n', encoding="utf-8")


def test_closeout_writes_payload_manifest_outside_campaign_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign = tmp_path / "campaign"
    _campaign(campaign)
    monkeypatch.setattr(
        "tools.oci_directional_closeout._systemd_state",
        lambda _service: {"ActiveState": "inactive", "MainPID": "0"},
    )

    receipt = closeout(
        service="directional.service",
        campaign_root=campaign,
        control_dir=tmp_path / "control",
        payload_manifest=tmp_path / "control" / "payload-manifest.json",
        expected_campaign="futures-bidirectional-test",
        expected_source="worktree-source-v1:" + "a" * 64,
    )

    manifest = json.loads(
        (tmp_path / "control" / "payload-manifest.json").read_text(encoding="utf-8")
    )
    assert receipt["database"]["quick_check"] == "ok"
    assert manifest["root_name"] == "campaign"
    assert all(not item["path"].startswith("../") for item in manifest["files"])


def test_closeout_rejects_unfinished_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign = tmp_path / "campaign"
    _campaign(campaign)
    (campaign / "open.partial").write_bytes(b"partial")
    monkeypatch.setattr(
        "tools.oci_directional_closeout._systemd_state",
        lambda _service: {"ActiveState": "inactive", "MainPID": "0"},
    )

    with pytest.raises(RuntimeError, match="unfinished files"):
        closeout(
            service="directional.service",
            campaign_root=campaign,
            control_dir=tmp_path / "control",
            payload_manifest=tmp_path / "control" / "payload-manifest.json",
            expected_campaign="futures-bidirectional-test",
            expected_source="worktree-source-v1:" + "a" * 64,
        )


def _segment(root: Path, market: str, sequence: int, predecessor: str | None) -> str:
    directory = root / "raw-events" / market
    directory.mkdir(parents=True, exist_ok=True)
    data = directory / f"0000000000000-{sequence:08d}.jsonl.zst"
    data.write_bytes(f"{market}-{sequence}".encode())
    digest = hashlib.sha256(data.read_bytes()).hexdigest()
    manifest = data.with_name(data.name.removesuffix(".jsonl.zst") + ".manifest.json")
    manifest.write_text(
        json.dumps({
            "campaign_id": "camp",
            "source_identity": "source",
            "market": market,
            "segment_sequence": sequence,
            "bucket_start_ms": 0,
            "compressed_bytes": data.stat().st_size,
            "compressed_file_sha256": digest,
            "previous_segment_sha256": predecessor,
        }),
        encoding="utf-8",
    )
    return digest


def test_closeout_accepts_independent_spot_and_futures_chains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign = tmp_path / "campaign"
    _campaign(campaign, "camp", "source")
    for market in ("spot", "futures"):
        previous = _segment(campaign, market, 1, None)
        _segment(campaign, market, 2, previous)
    monkeypatch.setattr(
        "tools.oci_directional_closeout._systemd_state",
        lambda _service: {"ActiveState": "inactive", "MainPID": "0"},
    )
    receipt = closeout(
        service="collector.service", campaign_root=campaign,
        control_dir=tmp_path / "control",
        payload_manifest=tmp_path / "control" / "payload-manifest.json",
        expected_campaign="camp", expected_source="source",
    )
    assert receipt["segments"]["manifest_count"] == 4
    assert receipt["segments"]["sequence_bounds_by_market"] == {
        "spot": [1, 2], "futures": [1, 2]
    }


@pytest.mark.parametrize(
    "corruption", ["data", "predecessor", "orphan", "prefix", "filename"]
)
def test_closeout_rejects_segment_corruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, corruption: str
) -> None:
    campaign = tmp_path / "campaign"
    _campaign(campaign, "camp", "source")
    previous = _segment(campaign, "futures", 1, None)
    _segment(campaign, "futures", 2, previous)
    directory = campaign / "raw-events" / "futures"
    if corruption == "data":
        (directory / "0000000000000-00000002.jsonl.zst").write_bytes(b"tampered!!")
    elif corruption == "predecessor":
        manifest = directory / "0000000000000-00000002.manifest.json"
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        payload["previous_segment_sha256"] = "0" * 64
        manifest.write_text(json.dumps(payload), encoding="utf-8")
    elif corruption == "prefix":
        (directory / "0000000000000-00000001.jsonl.zst").unlink()
        (directory / "0000000000000-00000001.manifest.json").unlink()
    elif corruption == "filename":
        for suffix in ("jsonl.zst", "manifest.json"):
            (directory / f"0000000000000-00000002.{suffix}").rename(
                directory / f"0000000000001-00000002.{suffix}"
            )
    else:
        (directory / "orphan.jsonl.zst").write_bytes(b"orphan")
    monkeypatch.setattr(
        "tools.oci_directional_closeout._systemd_state",
        lambda _service: {"ActiveState": "inactive", "MainPID": "0"},
    )
    with pytest.raises(RuntimeError, match=r"segment|predecessor"):
        closeout(
            service="collector.service", campaign_root=campaign,
            control_dir=tmp_path / "control",
            payload_manifest=tmp_path / "control" / "payload-manifest.json",
            expected_campaign="camp", expected_source="source",
        )


def test_closeout_rejects_wrong_database_campaign(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    campaign = tmp_path / "campaign"
    _campaign(campaign, "other-campaign", "source")
    monkeypatch.setattr(
        "tools.oci_directional_closeout._systemd_state",
        lambda _service: {"ActiveState": "inactive", "MainPID": "0"},
    )
    with pytest.raises(RuntimeError, match="SQLite campaign/source identity mismatch"):
        closeout(
            service="collector.service", campaign_root=campaign,
            control_dir=tmp_path / "control",
            payload_manifest=tmp_path / "control" / "payload-manifest.json",
            expected_campaign="camp", expected_source="source",
        )


def test_writer_check_rejects_active_health_timer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from subprocess import CompletedProcess

    monkeypatch.setattr(
        "tools.oci_directional_closeout.subprocess.run",
        lambda *_args, **_kwargs: CompletedProcess([], 0, "ActiveState=active\nMainPID=0\n", ""),
    )
    with pytest.raises(RuntimeError, match="writer unit remains active"):
        _assert_no_other_writers(tmp_path)
