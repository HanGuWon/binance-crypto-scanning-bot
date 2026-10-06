"""One-shot PAPER tracking-reset notices after a process restart.

``PaperPositionLifecycle`` keeps its state in memory only (frozen contract; this
module deliberately does not touch it). After a restart, open PAPER entries stop
producing exit alerts silently. At startup this module finds persisted CONFIRMED
entries that are still inside their maximum holding window and have no
TECHNICAL_EXIT row, and persists exactly one notice-only decision per entry
through the normal signal + outbox path.

The notice is not an exit, not a recommendation, and never becomes a PAPER
position: it is a ``TECHNICAL_EXIT`` family decision flagged ``notice_only`` that
the lifecycle's entry qualification rejects (``TECHNICAL_EXIT`` is not an entry
family).
"""

from __future__ import annotations

import hashlib
import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from signalbot.alerts.embeds import TRACKING_RESET_NOTICE_TEXT
from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.enums import Direction, Market, SignalFamily, SignalStage
from signalbot.domain.models import SignalDecision
from signalbot.persistence.repository import EventIdConflictError, SqlRepository

if TYPE_CHECKING:
    from signalbot.runtime import MarketRuntime

__all__ = ["TRACKING_RESET_NOTICE_TEXT", "emit_tracking_reset_notices"]

LOGGER = logging.getLogger(__name__)

TRACKING_RESET_REASON = "tracking_reset"
MAX_TRACKING_RESET_ENTRIES = 500

_NON_ENTRY_FAMILIES = frozenset(
    {SignalFamily.PUMP_RISK, SignalFamily.CRASH_RISK, SignalFamily.TECHNICAL_EXIT}
)


def tracking_reset_event_id(
    market: Market, symbol: str, entry_event_id: str, rule_version: str
) -> str:
    """Deterministic ID: at most one notice per entry across any number of restarts."""

    identity = "|".join(
        (
            market.value,
            symbol,
            SignalFamily.TECHNICAL_EXIT.value,
            entry_event_id,
            TRACKING_RESET_REASON,
            rule_version,
        )
    )
    return hashlib.sha256(identity.encode()).hexdigest()[:24]


def is_trackable_paper_entry(decision: SignalDecision, primary_interval: str) -> bool:
    """Mirror the lifecycle's entry qualification without needing a candle."""

    if (
        decision.stage is not SignalStage.CONFIRMED
        or decision.timeframe != primary_interval
        or decision.family in _NON_ENTRY_FAMILIES
        or decision.metadata.get("informational_only") is True
        or decision.invalidation is None
        or decision.invalidation <= 0
    ):
        return False
    if decision.market is Market.SPOT and decision.direction is not Direction.LONG:
        return False
    if decision.direction is Direction.LONG:
        return decision.invalidation < decision.price
    if decision.direction is Direction.SHORT:
        return decision.invalidation > decision.price
    return False


def build_tracking_reset_notice(
    entry: SignalDecision, *, rule_version: str, now_ms: int
) -> SignalDecision:
    return SignalDecision(
        event_id=tracking_reset_event_id(
            entry.market, entry.symbol, entry.event_id, rule_version
        ),
        market=entry.market,
        symbol=entry.symbol,
        family=SignalFamily.TECHNICAL_EXIT,
        stage=SignalStage.CONFIRMED,
        direction=entry.direction,
        timeframe=entry.timeframe,
        event_time_ms=now_ms,
        score=0,
        price=entry.price,
        reasons=(
            "PAPER lifecycle state is in memory only and was lost at process restart",
            "no further PAPER exit alerts will be sent for this entry",
            "notice only: not an exit recommendation; no exchange order was placed",
        ),
        invalidation=entry.invalidation if isinstance(entry.invalidation, Decimal) else None,
        regime=entry.regime,
        rule_version=rule_version,
        metadata={
            "paper_only": True,
            "order_placed": False,
            "exit_reason": TRACKING_RESET_REASON,
            "notice_only": True,
            "entry_event_id": entry.event_id,
            "entry_family": entry.family.value,
            "entry_time_ms": entry.event_time_ms,
        },
    )


def pending_tracking_resets(
    repository: SqlRepository,
    *,
    market: Market,
    primary_interval: str,
    max_holding_bars: int,
    now_ms: int,
    limit: int = MAX_TRACKING_RESET_ENTRIES,
) -> list[SignalDecision]:
    """Entries in their holding window that have no TECHNICAL_EXIT row (bounded)."""

    window_ms = max_holding_bars * interval_to_milliseconds(primary_interval)
    since_ms = now_ms - window_ms
    entries = repository.list_signals_since(
        market=market,
        timeframe=primary_interval,
        since_ms=since_ms,
        exclude_families=_NON_ENTRY_FAMILIES,
        stage=SignalStage.CONFIRMED,
        limit=limit,
    )
    exited = repository.entry_ids_with_exit_since(
        market=market, since_ms=since_ms, limit=limit * 4
    )
    return [
        entry
        for entry in entries
        if entry.event_id not in exited
        and is_trackable_paper_entry(entry, primary_interval)
        and entry.event_time_ms <= now_ms
    ]


def emit_tracking_reset_notices(runtime: MarketRuntime, now_ms: int) -> int:
    """Persist one notice per orphaned entry; return how many were newly created."""

    settings = runtime.settings
    exit_settings = settings.signals.technical_exit
    if not exit_settings.enabled:
        return 0
    entries = pending_tracking_resets(
        runtime.repository,
        market=runtime.market,
        primary_interval=settings.binance.primary_interval,
        max_holding_bars=exit_settings.max_holding_bars,
        now_ms=now_ms,
    )
    created = 0
    for entry in entries:
        notice = build_tracking_reset_notice(
            entry, rule_version=settings.rule_version, now_ms=now_ms
        )
        try:
            if runtime.persist_notice(notice):
                created += 1
        except EventIdConflictError:
            # A notice for this entry already exists with different bytes (for
            # example persisted at an earlier restart time): it was emitted once.
            LOGGER.warning(
                "tracking-reset notice already exists for entry; not re-emitting",
                extra={"market": runtime.market.value, "event_id": notice.event_id},
            )
    if created:
        LOGGER.info(
            "PAPER tracking-reset notices persisted",
            extra={"market": runtime.market.value},
        )
    return created
