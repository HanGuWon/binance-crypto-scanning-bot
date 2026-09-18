"""Independent adapters for prospective causal-retest parity.

The live-like side reads the durable runtime result. The replay side rebuilds
the lifecycle from persisted causal features and closed bars. Neither adapter
uses the other adapter's output, and the parity harness still refuses cases
without recorded decision-time BBO.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from signalbot.config import Settings
from signalbot.domain.models import ComparatorCandidate, FeatureSnapshot
from signalbot.persistence.repository import SqlRepository
from signalbot.prospective.retest import (
    RetestStage,
    arm_from_candidate,
    build_ready_snapshot,
    on_completed_bar,
    restore_lifecycle,
)
from signalbot.prospective.retest_parity import (
    RetestParityCase,
    RetestParityOutput,
)


@dataclass(frozen=True, slots=True)
class ReplayBar:
    feature: FeatureSnapshot
    contexts: Mapping[str, FeatureSnapshot]
    bar_close_ms: int


@dataclass(frozen=True, slots=True)
class CausalRetestReplayInput:
    candidate: ComparatorCandidate
    arm_feature: FeatureSnapshot
    bars: tuple[ReplayBar, ...]
    settings: Settings
    campaign_id: str
    campaign_manifest_sha256: str
    retest_horizon_bars: int


class RepositoryLiveLikeAdapter:
    """Read the live-like result from the durable lifecycle repository.

    This adapter deliberately does not evaluate bars or call the replay
    state machine.  It represents the result that a running observer wrote to
    the append-only/current lifecycle store and is therefore suitable for a
    software parity check against an independently reconstructed replay.
    """

    def __init__(
        self,
        repository: SqlRepository,
        *,
        campaign_id: str,
        campaign_manifest_sha256: str,
    ) -> None:
        self._repository = repository
        self._campaign_id = campaign_id
        self._campaign_manifest_sha256 = campaign_manifest_sha256

    def evaluate(self, case: RetestParityCase) -> RetestParityOutput:
        durable = self._repository.load_retest_lifecycle(
            campaign_id=self._campaign_id,
            opportunity_id=case.opportunity_id,
        )
        if durable is None:
            raise ValueError(
                f"live-like durable result missing {case.opportunity_id}"
            )
        if durable["campaign_manifest_sha256"] != self._campaign_manifest_sha256:
            raise ValueError(
                f"live-like durable result has conflicting campaign provenance "
                f"for {case.opportunity_id}"
            )
        lifecycle = restore_lifecycle(durable["lifecycle"])
        snapshot_sha = (
            None
            if lifecycle.ready_snapshot is None
            else lifecycle.ready_snapshot.content_sha256
        )
        return RetestParityOutput(
            stage=lifecycle.stage.value,
            terminal_reason=lifecycle.terminal_reason,
            ready_snapshot_sha256=snapshot_sha,
        )


class SyntheticOutputAdapter:
    """Test-only fixed-output adapter; never use as runtime evidence."""

    def __init__(self, outputs: Mapping[str, RetestParityOutput]) -> None:
        self._outputs = dict(outputs)

    def evaluate(self, case: RetestParityCase) -> RetestParityOutput:
        try:
            return self._outputs[case.opportunity_id]
        except KeyError as exc:
            raise ValueError(
                f"synthetic parity result missing {case.opportunity_id}"
            ) from exc


# Backward-compatible name for older test imports.  New parity tests must use
# RepositoryLiveLikeAdapter so a replay result cannot be copied into the live
# side by construction.
DurableLiveLikeAdapter = SyntheticOutputAdapter


class CausalRetestReplayAdapter:
    """Reconstruct causal_retest_v1 from closed feature/bar evidence."""

    def __init__(self, inputs: Mapping[str, CausalRetestReplayInput]) -> None:
        self._inputs = dict(inputs)

    def evaluate(self, case: RetestParityCase) -> RetestParityOutput:
        try:
            item = self._inputs[case.opportunity_id]
        except KeyError as exc:
            raise ValueError(
                f"replay evidence missing {case.opportunity_id}"
            ) from exc
        lifecycle = arm_from_candidate(
            item.candidate,
            item.arm_feature,
            campaign_id=item.campaign_id,
            campaign_manifest_sha256=item.campaign_manifest_sha256,
            opportunity_id=case.opportunity_id,
            retest_horizon_bars=item.retest_horizon_bars,
        )
        for bar in item.bars:
            ready_snapshot = None
            if lifecycle.stage is RetestStage.RETEST_TOUCH:
                recovers = (
                    bar.feature.price > lifecycle.arm.breakout_level
                    if lifecycle.arm.direction.value == "long"
                    else bar.feature.price < lifecycle.arm.breakout_level
                )
                if recovers:
                    try:
                        ready_snapshot = build_ready_snapshot(
                            lifecycle,
                            bar.feature,
                            bar.contexts,
                            item.settings,
                            bar_close_ms=bar.bar_close_ms,
                        )
                    except (ValueError, RuntimeError):
                        ready_snapshot = None
            on_completed_bar(
                lifecycle,
                decision_time_ms=bar.feature.event_time_ms,
                bar_close_ms=bar.bar_close_ms,
                close=bar.feature.price,
                ready_snapshot=ready_snapshot,
            )
            if lifecycle.terminal:
                break
        snapshot_sha = (
            None
            if lifecycle.ready_snapshot is None
            else lifecycle.ready_snapshot.content_sha256
        )
        return RetestParityOutput(
            stage=lifecycle.stage.value,
            terminal_reason=lifecycle.terminal_reason,
            ready_snapshot_sha256=snapshot_sha,
        )
