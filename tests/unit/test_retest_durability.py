"""Phase 2 durability contracts: append-only retest transitions and the
durable current lifecycle row, both campaign/manifest-gated and
conflict-loud."""

from __future__ import annotations

import pytest

from signalbot.persistence.repository import EventIdConflictError, SqlRepository


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


def _lifecycle(stage="ARMED", bars=0):
    return {
        "arm": {"opportunity_id": "opp-1", "breakout_level": 100.0},
        "stage": stage,
        "elapsed_bars": bars,
    }


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
        assert current["stage"] == "READY"
        assert current["lifecycle"]["stage"] == "READY"
        history = repo.list_retest_transitions(
            campaign_id="cret", opportunity_id=opp
        )
        assert [t["transition_id"] for t in history] == ["t1"]
        assert history[0]["from_stage"] == "ARMED"
        assert history[0]["to_stage"] == "READY"
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
        assert repo.transition_retest(**args) is True
        assert repo.transition_retest(**args) is False
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


def test_transition_id_binds_manifest_and_persisted_time():
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
        assert repo.transition_retest(**args) is True
        with pytest.raises(EventIdConflictError):
            repo.transition_retest(**{**args, "persisted_at_ms": 1002})
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
        assert repo.transition_retest(**first) is True
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
