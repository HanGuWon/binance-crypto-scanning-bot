"""Phase 2 durability contracts: append-only retest transitions and the
durable current lifecycle row, both campaign/manifest-gated and
conflict-loud."""

from __future__ import annotations

import hashlib
from typing import Any, cast

import pytest

from signalbot.persistence.repository import EventIdConflictError, SqlRepository
from signalbot.prospective.retest import retest_policy_for_horizon


def _register(repo, campaign_id="cret"):
    repo.register_shadow_campaign(
        campaign_schema_version="v1",
        campaign_id=campaign_id,
        campaign_mode="prospective",
        source_identity="src",
        rule_version="r2",
        policy_name="p",
        policy_version="pv1",
        policy_sha256="psh",
        config_sha256="csh",
        observation_schema_version="osv",
        primary_interval="5m",
        markets=["SPOT"],
        families={"SPOT": ["BREAKOUT_LONG"]},
        activation_ms=1_700_000_000_000,
        created_at_ms=1_700_000_000_000,
        status="registered",
    )
    return repo.get_shadow_campaign(campaign_id)["manifest_sha256"]


def _lifecycle(stage="ARMED", bars=0, opportunity_id="opp-1"):
    return {
        "arm": {
            "opportunity_id": opportunity_id,
            "breakout_level": 100.0,
            "retest_policy_sha256": retest_policy_for_horizon(72).sha256,
        },
        "stage": stage,
        "elapsed_bars": bars,
    }


def _save_base_observation(repo, manifest, opportunity_id, observation_id):
    return repo.save_shadow_observation(
        observation_id=observation_id,
        campaign_id="cret",
        campaign_manifest_sha256=manifest,
        opportunity_id=opportunity_id,
        market="SPOT",
        symbol="BTCUSDT",
        family="BREAKOUT_LONG",
        direction="LONG",
        decision_time_ms=100,
        primary_interval="5m",
        payload={"opportunity_id": opportunity_id, "observation_id": observation_id},
        policy_sha256="psh",
        created_at_ms=100,
    )


def test_begin_then_transition_persists_durable_history():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        opp = "opp-1"
        assert repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id=opp, protocol_version="causal_retest_v1",
            stage="ARMED", lifecycle=_lifecycle("ARMED"), updated_at_ms=100,
        ) is True
        assert repo.transition_retest(
            transition_id="t1", campaign_id="cret",
            campaign_manifest_sha256=manifest, opportunity_id=opp,
            protocol_version="causal_retest_v1", from_stage="ARMED",
            to_stage="READY", decision_time_ms=900, bar_close_ms=1000,
            lifecycle=_lifecycle("READY", bars=1), persisted_at_ms=1001,
        ) is True
        current = repo.load_retest_lifecycle(
            campaign_id="cret", opportunity_id=opp
        )
        assert current is not None
        assert current["stage"] == "READY"
        assert current["lifecycle"]["stage"] == "READY"
        history = repo.list_retest_transitions(
            campaign_id="cret", opportunity_id=opp
        )
        assert [t["transition_id"] for t in history] == ["t1"]
        assert history[0]["from_stage"] == "ARMED"
        assert history[0]["to_stage"] == "READY"
        assert history[0]["payload_json"]
        assert hashlib.sha256(
            history[0]["payload_json"].encode("utf-8")
        ).hexdigest() == history[0]["payload_sha256"]
        assert current["lifecycle_sha256"] != ""
    finally:
        repo.close()


def test_identical_replay_is_idempotent_noop():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        opp = "opp-2"
        repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id=opp, protocol_version="causal_retest_v1",
            stage="ARMED", lifecycle=_lifecycle("ARMED"), updated_at_ms=100,
        )
        args = dict(
            transition_id="t1", campaign_id="cret",
            campaign_manifest_sha256=manifest, opportunity_id=opp,
            protocol_version="causal_retest_v1", from_stage="ARMED",
            to_stage="READY", decision_time_ms=900, bar_close_ms=1000,
            lifecycle=_lifecycle("READY", bars=1), persisted_at_ms=1001,
        )
        assert repo.transition_retest(**cast(dict[str, Any], args)) is True
        assert repo.transition_retest(**cast(dict[str, Any], args)) is False
        history = repo.list_retest_transitions(
            campaign_id="cret", opportunity_id=opp
        )
        assert len(history) == 1
    finally:
        repo.close()


def test_reusing_transition_id_with_different_content_conflicts():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        opp = "opp-3"
        repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id=opp, protocol_version="causal_retest_v1",
            stage="ARMED", lifecycle=_lifecycle("ARMED"), updated_at_ms=100,
        )
        repo.transition_retest(
            transition_id="t1", campaign_id="cret",
            campaign_manifest_sha256=manifest, opportunity_id=opp,
            protocol_version="causal_retest_v1", from_stage="ARMED",
            to_stage="READY", decision_time_ms=900, bar_close_ms=1000,
            lifecycle=_lifecycle("READY", bars=1), persisted_at_ms=1001,
        )
        with pytest.raises(EventIdConflictError):
            repo.transition_retest(
                transition_id="t1", campaign_id="cret",
                campaign_manifest_sha256=manifest, opportunity_id=opp,
                protocol_version="causal_retest_v1", from_stage="ARMED",
                to_stage="INVALID", decision_time_ms=900, bar_close_ms=1000,
                lifecycle=_lifecycle("INVALID", bars=1), persisted_at_ms=1001,
            )
    finally:
        repo.close()


def test_begin_requires_registered_campaign_with_matching_manifest():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        with pytest.raises(EventIdConflictError):
            repo.begin_retest_lifecycle(
                campaign_id="unregistered", campaign_manifest_sha256=manifest,
                opportunity_id="o", protocol_version="causal_retest_v1",
                stage="ARMED", lifecycle=_lifecycle(), updated_at_ms=1,
            )
        with pytest.raises(EventIdConflictError):
            repo.begin_retest_lifecycle(
                campaign_id="cret", campaign_manifest_sha256="wrong",
                opportunity_id="o", protocol_version="causal_retest_v1",
                stage="ARMED", lifecycle=_lifecycle(), updated_at_ms=1,
            )
    finally:
        repo.close()


def test_transition_without_begin_conflicts():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        with pytest.raises(EventIdConflictError):
            repo.transition_retest(
                transition_id="t1", campaign_id="cret",
                campaign_manifest_sha256=manifest, opportunity_id="o",
                protocol_version="causal_retest_v1", from_stage="ARMED",
                to_stage="READY", decision_time_ms=900, bar_close_ms=1000,
                lifecycle=_lifecycle("READY"), persisted_at_ms=1001,
            )
    finally:
        repo.close()


def test_begin_reuse_with_different_content_conflicts():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        opp = "opp-4"
        assert repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id=opp, protocol_version="causal_retest_v1",
            stage="ARMED", lifecycle=_lifecycle("ARMED"), updated_at_ms=100,
        ) is True
        assert repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id=opp, protocol_version="causal_retest_v1",
            stage="ARMED", lifecycle=_lifecycle("ARMED"), updated_at_ms=100,
        ) is False
        with pytest.raises(EventIdConflictError):
            repo.begin_retest_lifecycle(
                campaign_id="cret", campaign_manifest_sha256=manifest,
                opportunity_id=opp, protocol_version="causal_retest_v1",
                stage="READY", lifecycle=_lifecycle("READY"), updated_at_ms=200,
            )
    finally:
        repo.close()


def test_load_missing_lifecycle_returns_none():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        assert repo.load_retest_lifecycle(
            campaign_id="cret", opportunity_id="nope"
        ) is None
    finally:
        repo.close()


def test_transition_requires_current_source_stage_and_matching_payload_stage():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id="opp-stage", protocol_version="causal_retest_v1",
            stage="ARMED", lifecycle=_lifecycle("ARMED"), updated_at_ms=100,
        )
        with pytest.raises(EventIdConflictError, match="source stage"):
            repo.transition_retest(
                transition_id="bad-source", campaign_id="cret",
                campaign_manifest_sha256=manifest, opportunity_id="opp-stage",
                protocol_version="causal_retest_v1", from_stage="RETEST_TOUCH",
                to_stage="READY", decision_time_ms=900, bar_close_ms=1000,
                lifecycle=_lifecycle("READY", bars=1), persisted_at_ms=1001,
            )
        with pytest.raises(EventIdConflictError, match="payload stage"):
            repo.transition_retest(
                transition_id="bad-payload", campaign_id="cret",
                campaign_manifest_sha256=manifest, opportunity_id="opp-stage",
                protocol_version="causal_retest_v1", from_stage="ARMED",
                to_stage="READY", decision_time_ms=900, bar_close_ms=1000,
                lifecycle=_lifecycle("RETEST_TOUCH", bars=1), persisted_at_ms=1001,
            )
    finally:
        repo.close()


def test_persistence_time_does_not_change_logical_transition_identity():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id="opp-binding", protocol_version="causal_retest_v1",
            stage="ARMED", lifecycle=_lifecycle("ARMED"), updated_at_ms=100,
        )
        args = dict(
            transition_id="bound", campaign_id="cret",
            campaign_manifest_sha256=manifest, opportunity_id="opp-binding",
            protocol_version="causal_retest_v1", from_stage="ARMED",
            to_stage="READY", decision_time_ms=900, bar_close_ms=1000,
            lifecycle=_lifecycle("READY", bars=1), persisted_at_ms=1001,
        )
        assert repo.transition_retest(**cast(dict[str, Any], args)) is True
        assert repo.transition_retest(**{**args, "persisted_at_ms": 1002}) is False
        changed_lifecycle = _lifecycle("READY", bars=1)
        changed_lifecycle["arm"]["breakout_level"] = 101.0
        with pytest.raises(EventIdConflictError):
            repo.transition_retest(**{**args, "lifecycle": changed_lifecycle})
    finally:
        repo.close()


def test_terminal_current_row_cannot_receive_later_transition():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id="opp-terminal", protocol_version="causal_retest_v1",
            stage="ARMED", lifecycle=_lifecycle("ARMED"), updated_at_ms=100,
        )
        first = dict(
            transition_id="terminal", campaign_id="cret",
            campaign_manifest_sha256=manifest, opportunity_id="opp-terminal",
            protocol_version="causal_retest_v1", from_stage="ARMED",
            to_stage="TIMEOUT", decision_time_ms=900, bar_close_ms=1000,
            lifecycle=_lifecycle("TIMEOUT", bars=1), persisted_at_ms=1001,
        )
        assert repo.transition_retest(**cast(dict[str, Any], first)) is True
        with pytest.raises(EventIdConflictError, match="terminal"):
            repo.transition_retest(
                **{
                    **first,
                    "transition_id": "after-terminal",
                    "from_stage": "TIMEOUT",
                    "to_stage": "READY",
                    "lifecycle": _lifecycle("READY", bars=2),
                    "persisted_at_ms": 1002,
                }
            )
    finally:
        repo.close()


def test_stale_expected_lifecycle_sha_is_rejected():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id="opp-cas", protocol_version="causal_retest_v1",
            stage="ARMED", lifecycle=_lifecycle("ARMED"), updated_at_ms=100,
        )
        with pytest.raises(EventIdConflictError, match="stale retest lifecycle"):
            repo.transition_retest(
                campaign_id="cret", campaign_manifest_sha256=manifest,
                opportunity_id="opp-cas", protocol_version="causal_retest_v1",
                from_stage="ARMED", to_stage="READY",
                decision_time_ms=900, bar_close_ms=1000,
                lifecycle=_lifecycle("READY", bars=1), persisted_at_ms=1001,
                expected_previous_lifecycle_sha256="0" * 64,
            )
    finally:
        repo.close()


def test_corrupt_lifecycle_payload_fails_closed_on_load():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id="opp-corrupt", protocol_version="causal_retest_v1",
            stage="ARMED", lifecycle=_lifecycle("ARMED"), updated_at_ms=100,
        )
        with repo.engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE retest_lifecycles SET lifecycle_json = ? "
                "WHERE campaign_id = ? AND opportunity_id = ?",
                ('{"stage":"ARMED","arm":{}}', "cret", "opp-corrupt"),
            )
        with pytest.raises(EventIdConflictError, match="content hash mismatch"):
            repo.load_retest_lifecycle(
                campaign_id="cret", opportunity_id="opp-corrupt"
            )
    finally:
        repo.close()


def test_retest_lifecycle_counts_expose_active_touched_and_each_terminal():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        stages = (
            ("a", "ARMED"),
            ("b", "RETEST_TOUCH"),
            ("c", "READY"),
            ("d", "INVALID"),
            ("e", "TIMEOUT"),
            ("f", "CENSORED"),
        )
        for opp, stage in stages:
            repo.begin_retest_lifecycle(
                campaign_id="cret", campaign_manifest_sha256=manifest,
                opportunity_id=opp, protocol_version="causal_retest_v1",
                stage=stage, lifecycle=_lifecycle(stage), updated_at_ms=100,
            )
        counts = repo.retest_lifecycle_counts(
            campaign_id="cret", campaign_manifest_sha256=manifest
        )
        assert counts == {
            "active": 1,
            "touched": 1,
            "READY": 1,
            "INVALID": 1,
            "TIMEOUT": 1,
            "CENSORED": 1,
            "admitted": 6,
            "terminal": 4,
        }
    finally:
        repo.close()


def test_retest_denominator_audit_uses_base_observations_and_detects_gaps():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        policy = retest_policy_for_horizon(72).sha256
        for index, opportunity_id in enumerate(("a", "b", "c")):
            _save_base_observation(repo, manifest, opportunity_id, f"obs-{index}")
        repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id="a", protocol_version="causal_retest_v1",
            retest_policy_sha256=policy, stage="ARMED",
            lifecycle=_lifecycle("ARMED", opportunity_id="a"), updated_at_ms=100,
        )
        repo.begin_retest_lifecycle(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            opportunity_id="unexpected", protocol_version="causal_retest_v1",
            retest_policy_sha256=policy, stage="CENSORED",
            lifecycle=_lifecycle("CENSORED", opportunity_id="unexpected"),
            updated_at_ms=100,
        )
        audit = repo.audit_retest_denominator(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            retest_policy_sha256=policy,
        )
        assert audit["expected"] == 3
        assert audit["lifecycle_rows"] == 2
        assert audit["missing_opportunity_ids"] == ["b", "c"]
        assert audit["unexpected_opportunity_ids"] == ["unexpected"]
        assert audit["active"] == 1
        assert audit["CENSORED"] == 1
    finally:
        repo.close()


def test_retest_denominator_audit_detects_duplicate_expected_evidence():
    repo = SqlRepository("sqlite:///:memory:")
    repo.initialize()
    try:
        manifest = _register(repo)
        _save_base_observation(repo, manifest, "dup", "obs-1")
        _save_base_observation(repo, manifest, "dup", "obs-2")
        audit = repo.audit_retest_denominator(
            campaign_id="cret", campaign_manifest_sha256=manifest,
            retest_policy_sha256=retest_policy_for_horizon(72).sha256,
        )
        assert audit["expected"] == 1
        assert audit["duplicate_expected_ids"] == ["dup"]
    finally:
        repo.close()
