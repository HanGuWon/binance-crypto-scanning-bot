from __future__ import annotations

import copy
from typing import Any

import pytest

from conftest import make_decision
from signalbot.alerts.embeds import PRESENTATION_VERSION, build_discord_payload
from signalbot.persistence.repository import EventIdConflictError, SqlRepository


def _repo() -> SqlRepository:
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    return repo


def _legacy_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Payload as produced before the presentation version existed (version 1)."""

    legacy = copy.deepcopy(payload)
    embed = legacy["embeds"][0]
    embed["title"] = "BTCUSDT · 🟢 추천: 상승 예상 · LONG 후보"
    embed["footer"]["text"] = embed["footer"]["text"].split(" · view v")[0]
    embed["fields"] = [f for f in embed["fields"] if f["name"] != "검증 상태"]
    return legacy


def _persist(repo: SqlRepository, decision: Any, payload: dict[str, Any]) -> bool:
    return repo.save_signal_and_enqueue(
        decision, payload, 1_000, delivery_enabled=True, maximum_active_items=100
    )


def test_legacy_stored_payload_survives_a_newer_presentation_repersist() -> None:
    repo = _repo()
    old = make_decision()
    old_payload = _legacy_payload(build_discord_payload(old, "Bot"))
    assert _persist(repo, old, old_payload) is True
    claimed = repo.claim_outbox(old.event_id, 2_000)
    assert claimed is not None
    stored_json = claimed.payload_json

    new = make_decision(
        metadata={"entry_policy": "legacy_gates", "unevaluated_gates": []},
    )
    new_payload = build_discord_payload(new, "Bot")
    assert new_payload != old_payload
    assert _persist(repo, new, new_payload) is False  # tolerated no-op
    item = repo.get_outbox(old.event_id)
    assert item is not None
    assert item.payload_json == stored_json  # stored (possibly sent) intent is kept
    assert item.status == "sending"
    repo.close()


def test_presentation_only_metadata_difference_does_not_conflict_for_save_signal() -> None:
    repo = _repo()
    assert repo.save_signal(make_decision()) is True
    extended = make_decision(
        metadata={"entry_policy": "r2_pit_htf_exec", "unevaluated_gates": ["crowding"]}
    )
    assert repo.save_signal(extended) is False
    repo.close()


def test_signal_semantic_change_still_conflicts_even_with_older_presentation() -> None:
    repo = _repo()
    old = make_decision()
    assert _persist(repo, old, _legacy_payload(build_discord_payload(old, "Bot"))) is True
    changed = make_decision(score=60, metadata={"entry_policy": "legacy_gates"})
    with pytest.raises(EventIdConflictError, match="signal payloads"):
        _persist(repo, changed, build_discord_payload(changed, "Bot"))
    repo.close()


def test_semantic_reason_change_still_conflicts_for_save_signal() -> None:
    repo = _repo()
    repo.save_signal(make_decision())
    with pytest.raises(EventIdConflictError):
        repo.save_signal(make_decision(reasons=("different evidence",)))
    repo.close()


def test_same_version_payload_difference_still_conflicts() -> None:
    repo = _repo()
    decision = make_decision()
    payload = build_discord_payload(decision, "Bot")
    assert _persist(repo, decision, payload) is True
    tampered = copy.deepcopy(payload)
    tampered["embeds"][0]["title"] = "[SPOT] tampered"  # type: ignore[index]
    with pytest.raises(EventIdConflictError, match="alert payloads"):
        _persist(repo, decision, tampered)
    repo.close()


def test_identical_repersist_is_still_an_idempotent_noop() -> None:
    repo = _repo()
    decision = make_decision()
    payload = build_discord_payload(decision, "Bot")
    assert _persist(repo, decision, payload) is True
    assert _persist(repo, decision, payload) is False
    repo.close()


def test_stored_newer_presentation_is_not_overwritten_by_an_older_one() -> None:
    repo = _repo()
    decision = make_decision()
    payload = build_discord_payload(decision, "Bot")
    assert _persist(repo, decision, payload) is True
    older = _legacy_payload(payload)
    with pytest.raises(EventIdConflictError, match="alert payloads"):
        _persist(repo, decision, older)
    repo.close()


def test_current_payload_records_the_presentation_version_in_the_footer() -> None:
    payload = build_discord_payload(make_decision(), "Bot")
    footer = payload["embeds"][0]["footer"]["text"]  # type: ignore[index]
    assert footer.endswith(f"view v{PRESENTATION_VERSION}")
