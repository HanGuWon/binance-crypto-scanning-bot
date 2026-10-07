"""Explicit-clock orchestration for recorded capture, outcomes and archive safety."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from signalbot.pump_fade_v2 import POLICY_VERSION
from signalbot.pump_fade_v2.labels import (
    AppendOnlyOutcomeStore,
    ExecutableQuote,
    FundingSettlement,
    MarkBar,
    Outcome,
    label_outcome,
    load_primary_squeeze_threshold,
    outcome_receipt_identity,
)
from signalbot.pump_fade_v2.materializer import (
    PumpInputMaterializer,
    replay_capture_directory,
)
from signalbot.pump_fade_v2.shadow import ArchiveReceipt, ShadowEpisodeEngine


class OfflinePumpLifecycle:
    """Join verified recorded inputs to durable decisions/outcomes without I/O."""

    def __init__(
        self,
        shadow: ShadowEpisodeEngine,
        outcomes: AppendOnlyOutcomeStore,
        *,
        clock_ms: Callable[[], int],
    ) -> None:
        self.shadow = shadow
        self.outcomes = outcomes
        self.clock_ms = clock_ms

    def replay_capture(
        self, capture_directory: str | Path, materializer: PumpInputMaterializer,
    ) -> dict[str, object]:
        """Verify segments, causally materialize and persist local decision receipts."""

        if materializer.shadow is not self.shadow:
            raise ValueError("offline materializer must use this lifecycle's shadow store")
        asof_ms = self.clock_ms()
        if asof_ms < 0:
            raise ValueError("offline clock must be a nonnegative Unix millisecond value")
        receipt = replay_capture_directory(capture_directory, materializer)
        return {**receipt, "orchestration_asof_ms": asof_ms,
                "outcome_store": str(self.outcomes.path)}

    def persist_outcome(self, outcome: Outcome) -> bool:
        """Append one immutable outcome snapshot and acknowledge it for archive gating."""

        if outcome.policy_version != POLICY_VERSION:
            raise ValueError("outcome policy differs from active Pump-fade package")
        if self.clock_ms() < outcome.labeled_at_ms:
            raise ValueError("offline clock precedes outcome receipt time")
        identity, digest, _ = outcome_receipt_identity(outcome)
        inserted = self.outcomes.append(outcome)
        self.shadow.record_outcome_receipt(
            outcome.event_id, origin=outcome.origin, origin_ms=outcome.origin_ms,
            horizon_hours=outcome.horizon_hours, outcome_identity=identity,
            payload_sha256=digest, status=outcome.status.value,
        )
        return inserted

    def label_horizon(
        self,
        *,
        event_id: str,
        origin: str,
        origin_ms: int,
        origin_mark: float,
        horizon_hours: int,
        marks: tuple[MarkBar, ...],
        entry_quote: ExecutableQuote | None = None,
        exit_quote: ExecutableQuote | None = None,
        settlements: tuple[FundingSettlement, ...] = (),
        verified_funding_coverage: bool = False,
    ) -> Outcome:
        """Label one event/alert horizon at the injected clock and persist it."""

        decision_now_ms = self.clock_ms()
        if decision_now_ms < 0:
            raise ValueError("offline clock must be a nonnegative Unix millisecond value")
        policy_path = Path(__file__).resolve().parents[3] / "config/pump_fade_v2" / (
            f"policy_v2{POLICY_VERSION.rsplit('-', 1)[-1]}.json"
        )
        outcome = label_outcome(
            event_id=event_id, origin=origin, origin_ms=origin_ms,
            origin_mark=origin_mark, horizon_hours=horizon_hours,
            decision_now_ms=decision_now_ms, marks=marks, entry_quote=entry_quote,
            exit_quote=exit_quote, settlements=settlements,
            verified_funding_coverage=verified_funding_coverage,
            squeeze_adverse_pct=load_primary_squeeze_threshold(policy_path),
        )
        self.persist_outcome(outcome)
        return outcome

    def archive_terminal(self, path: str | Path) -> ArchiveReceipt:
        """Prune only terminal episodes with outcome-complete hash-acked archives."""

        return self.shadow.archive_terminal(path)
