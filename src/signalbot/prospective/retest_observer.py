"""Failure-isolated prospective integration for ``causal_retest_v1``.

This adapter is research-only.  It consumes the same comparator opportunity
and completed primary features as the existing shadow observer, while keeping
all persistence failures outside the production R2/PAPER/Discord path.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping

from signalbot.clock import Clock
from signalbot.config import Settings
from signalbot.domain.enums import Direction
from signalbot.domain.models import ComparatorCandidate, FeatureSnapshot
from signalbot.persistence.repository import SqlRepository
from signalbot.prospective.retest import (
    RetestCensorReason,
    RetestLifecycle,
    RetestStage,
    arm_from_candidate,
    build_ready_snapshot,
    censor_lifecycle,
    on_completed_bar,
    restore_lifecycle,
    retest_policy_for_horizon,
    serialize_lifecycle,
)

LOGGER = logging.getLogger(__name__)


class CausalRetestObserver:
    """Advance one durable retest lifecycle per admitted raw-C0 opportunity."""

    def __init__(
        self,
        settings: Settings,
        repository: SqlRepository,
        *,
        campaign_id: str,
        campaign_manifest_sha256: str,
        clock: Clock,
        continuity_proven: bool = False,
    ) -> None:
        if not settings.shadow.retest_observation_enabled:
            raise ValueError("causal retest observer requires explicit enablement")
        self.settings = settings
        self.repository = repository
        self.campaign_id = campaign_id
        self.campaign_manifest_sha256 = campaign_manifest_sha256
        self.clock = clock
        self.policy = retest_policy_for_horizon(settings.shadow.retest_horizon_bars)
        self._active: dict[str, RetestLifecycle] = {}
        self._active_by_symbol: dict[tuple[str, str], set[str]] = {}
        self._restore_active(continuity_proven=continuity_proven)

    @property
    def policy_sha256(self) -> str:
        return self.policy.sha256

    def advance(
        self,
        feature: FeatureSnapshot,
        contexts: Mapping[str, FeatureSnapshot],
    ) -> None:
        """Advance only existing lifecycles for one later completed bar."""

        self._advance_matching(feature, contexts)

    def arm(
        self,
        candidate: ComparatorCandidate,
        feature: FeatureSnapshot,
        *,
        opportunity_id: str,
    ) -> None:
        """Arm one raw-C0 only after durable base-observation admission."""

        if not candidate.raw_c0_triggered:
            raise ValueError("causal retest arm requires raw C0")
        if not self.repository.has_shadow_observation(
            campaign_id=self.campaign_id,
            campaign_manifest_sha256=self.campaign_manifest_sha256,
            opportunity_id=opportunity_id,
        ):
            raise RuntimeError(
                "causal retest arm requires a durable base shadow observation"
            )
        if opportunity_id in self._active:
            return
        existing = self.repository.load_retest_lifecycle(
            campaign_id=self.campaign_id,
            opportunity_id=opportunity_id,
        )
        if existing is not None:
            if (
                existing["campaign_manifest_sha256"]
                != self.campaign_manifest_sha256
                or existing.get("retest_policy_sha256") != self.policy.sha256
            ):
                raise RuntimeError("causal retest duplicate has conflicting provenance")
            return
        lifecycle = arm_from_candidate(
            candidate,
            feature,
            campaign_id=self.campaign_id,
            campaign_manifest_sha256=self.campaign_manifest_sha256,
            opportunity_id=opportunity_id,
            retest_horizon_bars=self.policy.retest_horizon_bars,
        )
        self.repository.begin_retest_lifecycle(
            campaign_id=self.campaign_id,
            campaign_manifest_sha256=self.campaign_manifest_sha256,
            opportunity_id=opportunity_id,
            protocol_version=self.policy.protocol_version,
            retest_policy_sha256=self.policy.sha256,
            stage=lifecycle.stage.value,
            lifecycle=serialize_lifecycle(lifecycle),
            updated_at_ms=self.clock.now_ms(),
        )
        self._active[opportunity_id] = lifecycle
        self._index_active(opportunity_id, lifecycle)

    def observe(
        self,
        candidate: ComparatorCandidate,
        feature: FeatureSnapshot,
        contexts: Mapping[str, FeatureSnapshot],
        *,
        opportunity_id: str,
        base_observation_durable: bool = False,
    ) -> None:
        """Compatibility boundary: advance, then conditionally arm."""

        self.advance(feature, contexts)
        if candidate.raw_c0_triggered and base_observation_durable:
            self.arm(candidate, feature, opportunity_id=opportunity_id)

    def _advance_matching(
        self,
        feature: FeatureSnapshot,
        contexts: Mapping[str, FeatureSnapshot],
    ) -> None:
        active_ids = tuple(
            self._active_by_symbol.get((feature.market.value, feature.symbol), ())
        )
        for opportunity_id in active_ids:
            lifecycle = self._active.get(opportunity_id)
            if lifecycle is None:
                continue
            arm = lifecycle.arm
            if feature.event_time_ms <= arm.arm_decision_time_ms:
                continue
            before = serialize_lifecycle(lifecycle)
            from_stage = lifecycle.stage.value
            ready_snapshot = None
            if lifecycle.stage is RetestStage.RETEST_TOUCH:
                recovers = (
                    feature.price > arm.breakout_level
                    if arm.direction is Direction.LONG
                    else feature.price < arm.breakout_level
                )
                if recovers:
                    try:
                        ready_snapshot = build_ready_snapshot(
                            lifecycle,
                            feature,
                            contexts,
                            self.settings,
                            bar_close_ms=feature.event_time_ms,
                        )
                    except (ValueError, RuntimeError):
                        # A recovery without causal READY evidence is censored
                        # by the state machine; no proxy context is fabricated.
                        ready_snapshot = None
            on_completed_bar(
                lifecycle,
                decision_time_ms=feature.event_time_ms,
                bar_close_ms=feature.event_time_ms,
                close=feature.price,
                ready_snapshot=ready_snapshot,
            )
            after = serialize_lifecycle(lifecycle)
            if before == after:
                continue
            durable = self.repository.load_retest_lifecycle(
                campaign_id=self.campaign_id,
                opportunity_id=opportunity_id,
            )
            if durable is None:
                raise RuntimeError("active retest lifecycle disappeared from storage")
            self.repository.transition_retest(
                campaign_id=self.campaign_id,
                campaign_manifest_sha256=self.campaign_manifest_sha256,
                opportunity_id=opportunity_id,
                protocol_version=self.policy.protocol_version,
                retest_policy_sha256=self.policy.sha256,
                from_stage=from_stage,
                to_stage=lifecycle.stage.value,
                decision_time_ms=feature.event_time_ms,
                bar_close_ms=feature.event_time_ms,
                transition_time_ms=feature.event_time_ms,
                expected_previous_lifecycle_sha256=durable["lifecycle_sha256"],
                lifecycle=after,
                persisted_at_ms=self.clock.now_ms(),
            )
            if lifecycle.terminal:
                self._active.pop(opportunity_id, None)
                self._unindex_active(opportunity_id, lifecycle)

    def censor_symbols(
        self,
        symbols: set[str] | frozenset[str],
        *,
        decision_time_ms: int,
    ) -> None:
        """Durably censor active lifecycles leaving a confirmed universe."""

        normalized = {symbol.upper() for symbol in symbols}
        for opportunity_id, lifecycle in list(self._active.items()):
            if lifecycle.arm.symbol.upper() not in normalized or lifecycle.terminal:
                continue
            from_stage = lifecycle.stage.value
            censor_lifecycle(
                lifecycle,
                reason=RetestCensorReason.UNIVERSE_MEMBERSHIP_LOSS,
                decision_time_ms=decision_time_ms,
            )
            durable = self.repository.load_retest_lifecycle(
                campaign_id=self.campaign_id,
                opportunity_id=opportunity_id,
            )
            if durable is None:
                raise RuntimeError("active retest lifecycle disappeared from storage")
            try:
                self.repository.transition_retest(
                    campaign_id=self.campaign_id,
                    campaign_manifest_sha256=self.campaign_manifest_sha256,
                    opportunity_id=opportunity_id,
                    protocol_version=self.policy.protocol_version,
                    retest_policy_sha256=self.policy.sha256,
                    from_stage=from_stage,
                    to_stage=lifecycle.stage.value,
                    decision_time_ms=decision_time_ms,
                    bar_close_ms=None,
                    transition_time_ms=decision_time_ms,
                    expected_previous_lifecycle_sha256=durable["lifecycle_sha256"],
                    lifecycle=serialize_lifecycle(lifecycle),
                    persisted_at_ms=self.clock.now_ms(),
                )
            except Exception:
                # Keep the in-memory lifecycle so a transient research-store
                # failure cannot silently erase an admitted observation.
                lifecycle.stage = RetestStage(from_stage)
                lifecycle.terminal_reason = None
                lifecycle.terminal_time_ms = None
                raise
            self._active.pop(opportunity_id, None)
            self._unindex_active(opportunity_id, lifecycle)

    def flush(self, *, decision_time_ms: int) -> None:
        """Censor unresolved rows at controlled shutdown."""

        for opportunity_id, lifecycle in list(self._active.items()):
            if lifecycle.terminal:
                continue
            from_stage = lifecycle.stage.value
            censor_lifecycle(
                lifecycle,
                reason=RetestCensorReason.CAMPAIGN_SHUTDOWN,
                decision_time_ms=decision_time_ms,
            )
            durable = self.repository.load_retest_lifecycle(
                campaign_id=self.campaign_id,
                opportunity_id=opportunity_id,
            )
            if durable is None:
                raise RuntimeError("active retest lifecycle disappeared from storage")
            self.repository.transition_retest(
                campaign_id=self.campaign_id,
                campaign_manifest_sha256=self.campaign_manifest_sha256,
                opportunity_id=opportunity_id,
                protocol_version=self.policy.protocol_version,
                retest_policy_sha256=self.policy.sha256,
                from_stage=from_stage,
                to_stage=lifecycle.stage.value,
                decision_time_ms=decision_time_ms,
                bar_close_ms=None,
                transition_time_ms=decision_time_ms,
                expected_previous_lifecycle_sha256=durable["lifecycle_sha256"],
                lifecycle=serialize_lifecycle(lifecycle),
                persisted_at_ms=self.clock.now_ms(),
            )
            self._active.pop(opportunity_id, None)
            self._unindex_active(opportunity_id, lifecycle)

    def _restore_active(self, *, continuity_proven: bool) -> None:
        rows = self.repository.list_retest_lifecycles(
            campaign_id=self.campaign_id,
            campaign_manifest_sha256=self.campaign_manifest_sha256,
            retest_policy_sha256=self.policy.sha256,
            active_only=True,
        )
        for row in rows:
            lifecycle = restore_lifecycle(row["lifecycle"])
            if continuity_proven:
                self._active[row["opportunity_id"]] = lifecycle
                self._index_active(row["opportunity_id"], lifecycle)
                continue
            from_stage = lifecycle.stage.value
            censor_lifecycle(
                lifecycle,
                reason=RetestCensorReason.RESTART_GAP,
                decision_time_ms=max(
                    self.clock.now_ms(), lifecycle.arm.arm_decision_time_ms + 1
                ),
            )
            self.repository.transition_retest(
                campaign_id=self.campaign_id,
                campaign_manifest_sha256=self.campaign_manifest_sha256,
                opportunity_id=row["opportunity_id"],
                protocol_version=self.policy.protocol_version,
                retest_policy_sha256=self.policy.sha256,
                from_stage=from_stage,
                to_stage=lifecycle.stage.value,
                decision_time_ms=lifecycle.terminal_time_ms,
                bar_close_ms=None,
                transition_time_ms=lifecycle.terminal_time_ms,
                expected_previous_lifecycle_sha256=row["lifecycle_sha256"],
                lifecycle=serialize_lifecycle(lifecycle),
                persisted_at_ms=self.clock.now_ms(),
            )

    def _index_active(self, opportunity_id: str, lifecycle: RetestLifecycle) -> None:
        key = (lifecycle.arm.market.value, lifecycle.arm.symbol)
        self._active_by_symbol.setdefault(key, set()).add(opportunity_id)

    def _unindex_active(self, opportunity_id: str, lifecycle: RetestLifecycle) -> None:
        key = (lifecycle.arm.market.value, lifecycle.arm.symbol)
        ids = self._active_by_symbol.get(key)
        if ids is None:
            return
        ids.discard(opportunity_id)
        if not ids:
            self._active_by_symbol.pop(key, None)
