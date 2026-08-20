"""causal_retest_v1 - research-only post-raw-C0 retest lifecycle.

Scientific ceiling: SHADOW / RESEARCH ONLY. This module never changes
production R2 eligibility, SignalStateMachine, PAPER, Discord, or any order
path. It observes a causal breakout/breakdown and a possible retest, returning
only descriptive state transitions for future prospective study.

The frozen horizon bounds the entire ARM -> READY lifecycle. READY must occur
within ``retest_horizon_bars`` completed primary bars after the arm. A touch on
the final allowed bar is observable, but cannot recover on a later bar without
timing out. This is a protocol rule, not a tuned parameter.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from enum import StrEnum

from signalbot.config import Settings
from signalbot.domain.enums import Direction, Market, SignalFamily
from signalbot.domain.models import ComparatorCandidate, FeatureSnapshot
from signalbot.prospective.research_context import (
    RESEARCH_CONTEXT_VERSION,
    build_research_context,
)
from signalbot.signals.gates import (
    BboExecutionEvidence,
    evaluate_bbo_execution_evidence,
)

RETEST_PROTOCOL_VERSION = "causal_retest_v1"


class RetestStage(StrEnum):
    RAW_C0 = "RAW_C0"
    ARMED = "ARMED"
    RETEST_TOUCH = "RETEST_TOUCH"
    READY = "READY"
    INVALID = "INVALID"
    TIMEOUT = "TIMEOUT"
    CENSORED = "CENSORED"


class RetestCensorReason(StrEnum):
    CONTINUITY_LOSS = "CONTINUITY_LOSS"
    CAUSAL_CONTEXT_UNAVAILABLE = "CAUSAL_CONTEXT_UNAVAILABLE"
    CAMPAIGN_SHUTDOWN = "CAMPAIGN_SHUTDOWN"
    UNIVERSE_MEMBERSHIP_LOSS = "UNIVERSE_MEMBERSHIP_LOSS"
    RESTART_GAP = "RESTART_GAP"


TERMINAL_STAGES = frozenset(
    {
        RetestStage.READY,
        RetestStage.INVALID,
        RetestStage.TIMEOUT,
        RetestStage.CENSORED,
    }
)


class RetestOutOfOrderError(RuntimeError):
    pass


class RetestConflictError(RuntimeError):
    """Raised when one causal bar identity maps to different scientific content."""


@dataclass(frozen=True, slots=True)
class RetestArm:
    opportunity_id: str
    campaign_id: str
    campaign_manifest_sha256: str
    market: Market
    symbol: str
    family: SignalFamily
    direction: Direction
    primary_interval: str
    breakout_level: float
    arm_price: float
    arm_atr: float
    arm_decision_time_ms: int
    retest_horizon_bars: int
    protocol_version: str = RETEST_PROTOCOL_VERSION


@dataclass(frozen=True, slots=True)
class RetestReadySnapshot:
    """Immutable READY-time evidence; never substitutes arm-time evidence."""

    campaign_id: str
    campaign_manifest_sha256: str
    opportunity_id: str
    market: Market
    symbol: str
    family: SignalFamily
    direction: Direction
    decision_time_ms: int
    bar_close_ms: int
    price: float
    breakout_level: float
    bbo: BboExecutionEvidence
    research_context_version: str
    research_context_json: str
    research_context_sha256: str
    content_sha256: str


@dataclass(slots=True)
class RetestLifecycle:
    arm: RetestArm
    stage: RetestStage = RetestStage.RAW_C0
    elapsed_bars: int = 0
    last_bar_close_ms: int | None = None
    last_decision_time_ms: int | None = None
    last_close: float | None = None
    last_bar_fingerprint: str | None = None
    touch_bar_close_ms: int | None = None
    touch_price: float | None = None
    ready_time_ms: int | None = None
    ready_price: float | None = None
    ready_snapshot: RetestReadySnapshot | None = None
    terminal_time_ms: int | None = None
    terminal_reason: str | None = None

    @property
    def terminal(self) -> bool:
        return self.stage in TERMINAL_STAGES


def _require_positive_finite(name: str, value: float) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _bbo_payload(value: BboExecutionEvidence) -> dict[str, object]:
    payload = asdict(value)
    payload["failures"] = list(value.failures)
    return payload


def _bar_fingerprint(
    *,
    decision_time_ms: int,
    bar_close_ms: int,
    close: float,
    ready_snapshot: RetestReadySnapshot | None,
) -> str:
    return _sha256(
        _canonical_json(
            {
                "decision_time_ms": decision_time_ms,
                "bar_close_ms": bar_close_ms,
                "close": close,
                "ready_snapshot_sha256": (
                    None if ready_snapshot is None else ready_snapshot.content_sha256
                ),
            }
        )
    )


def arm_from_raw_c0(arm: RetestArm) -> RetestLifecycle:
    if arm.direction not in (Direction.LONG, Direction.SHORT):
        raise ValueError("retest direction must be LONG or SHORT")
    _require_positive_finite("breakout_level", arm.breakout_level)
    _require_positive_finite("arm_price", arm.arm_price)
    _require_positive_finite("arm_atr", arm.arm_atr)
    if arm.retest_horizon_bars <= 0:
        raise ValueError("retest horizon must be positive")
    if not arm.opportunity_id or not arm.campaign_id:
        raise ValueError("retest arm requires opportunity/campaign identity")
    if not arm.campaign_manifest_sha256:
        raise ValueError("retest arm requires campaign manifest provenance")
    return RetestLifecycle(arm=arm, stage=RetestStage.ARMED)


def arm_from_candidate(
    candidate: ComparatorCandidate,
    feature: FeatureSnapshot,
    *,
    campaign_id: str,
    campaign_manifest_sha256: str,
    opportunity_id: str,
    retest_horizon_bars: int,
) -> RetestLifecycle:
    """Build the only supported RetestArm from the causal raw-C0 snapshot."""

    if not candidate.raw_c0_triggered:
        raise ValueError("causal retest requires a raw-C0 opportunity")
    if (
        candidate.market is not feature.market
        or candidate.symbol != feature.symbol
        or candidate.decision_time_ms != feature.event_time_ms
        or candidate.primary_interval != feature.interval
    ):
        raise ValueError("candidate and feature must describe the same causal close")

    if candidate.market is Market.SPOT:
        if (
            candidate.family is not SignalFamily.BREAKOUT_LONG
            or candidate.direction is not Direction.LONG
        ):
            raise ValueError("Spot causal retest is locked to BREAKOUT_LONG/LONG")
        breakout_level = feature.recent_high
    elif candidate.market is Market.FUTURES:
        if (
            candidate.family is not SignalFamily.BREAKDOWN_SHORT
            or candidate.direction is not Direction.SHORT
        ):
            raise ValueError("Futures causal retest is locked to BREAKDOWN_SHORT/SHORT")
        breakout_level = feature.recent_low
    else:  # pragma: no cover - Market is currently exhaustive
        raise ValueError("unsupported market for causal retest")

    return arm_from_raw_c0(
        RetestArm(
            opportunity_id=opportunity_id,
            campaign_id=campaign_id,
            campaign_manifest_sha256=campaign_manifest_sha256,
            market=candidate.market,
            symbol=candidate.symbol,
            family=candidate.family,
            direction=candidate.direction,
            primary_interval=candidate.primary_interval,
            breakout_level=breakout_level,
            arm_price=feature.price,
            arm_atr=feature.atr,
            arm_decision_time_ms=candidate.decision_time_ms,
            retest_horizon_bars=retest_horizon_bars,
        )
    )


def build_ready_snapshot(
    lifecycle: RetestLifecycle,
    feature: FeatureSnapshot,
    contexts: Mapping[str, FeatureSnapshot],
    settings: Settings,
    *,
    bar_close_ms: int,
) -> RetestReadySnapshot:
    """Freeze READY-time BBO + research context using existing shared owners."""

    arm = lifecycle.arm
    if (
        feature.market is not arm.market
        or feature.symbol != arm.symbol
        or feature.interval != arm.primary_interval
    ):
        raise ValueError("READY feature does not match retest arm identity")
    if feature.event_time_ms <= arm.arm_decision_time_ms:
        raise RetestOutOfOrderError("READY feature must be strictly after the arm")
    _require_positive_finite("READY price", feature.price)

    bbo = evaluate_bbo_execution_evidence(
        feature,
        arm.direction,
        settings.signals,
    )
    research_context = build_research_context(
        feature,
        contexts,
        execution_available=bbo.eligible,
    )
    research_json = _canonical_json(research_context)
    research_sha256 = _sha256(research_json)
    canonical = {
        "campaign_id": arm.campaign_id,
        "campaign_manifest_sha256": arm.campaign_manifest_sha256,
        "opportunity_id": arm.opportunity_id,
        "market": arm.market.value,
        "symbol": arm.symbol,
        "family": arm.family.value,
        "direction": arm.direction.value,
        "decision_time_ms": feature.event_time_ms,
        "bar_close_ms": bar_close_ms,
        "price": feature.price,
        "breakout_level": arm.breakout_level,
        "bbo": _bbo_payload(bbo),
        "research_context_version": RESEARCH_CONTEXT_VERSION,
        "research_context_sha256": research_sha256,
        "retest_protocol_version": arm.protocol_version,
    }
    return RetestReadySnapshot(
        campaign_id=arm.campaign_id,
        campaign_manifest_sha256=arm.campaign_manifest_sha256,
        opportunity_id=arm.opportunity_id,
        market=arm.market,
        symbol=arm.symbol,
        family=arm.family,
        direction=arm.direction,
        decision_time_ms=feature.event_time_ms,
        bar_close_ms=bar_close_ms,
        price=feature.price,
        breakout_level=arm.breakout_level,
        bbo=bbo,
        research_context_version=RESEARCH_CONTEXT_VERSION,
        research_context_json=research_json,
        research_context_sha256=research_sha256,
        content_sha256=_sha256(_canonical_json(canonical)),
    )


def _touches(arm: RetestArm, close: float) -> bool:
    if arm.direction is Direction.LONG:
        return close <= arm.breakout_level
    return close >= arm.breakout_level


def _recovers(arm: RetestArm, close: float) -> bool:
    if arm.direction is Direction.LONG:
        return close > arm.breakout_level
    return close < arm.breakout_level


def censor_lifecycle(
    lifecycle: RetestLifecycle,
    *,
    reason: RetestCensorReason,
    decision_time_ms: int,
) -> None:
    """Terminalize unresolved evidence without turning absence into success."""

    if lifecycle.terminal:
        return
    if decision_time_ms <= lifecycle.arm.arm_decision_time_ms:
        raise RetestOutOfOrderError("censor time must be strictly after the arm")
    if (
        lifecycle.last_decision_time_ms is not None
        and decision_time_ms < lifecycle.last_decision_time_ms
    ):
        raise RetestOutOfOrderError("censor time cannot regress")
    lifecycle.stage = RetestStage.CENSORED
    lifecycle.terminal_reason = reason.value
    lifecycle.terminal_time_ms = decision_time_ms


def on_completed_bar(
    lifecycle: RetestLifecycle,
    *,
    decision_time_ms: int,
    bar_close_ms: int,
    close: float,
    ready_snapshot: RetestReadySnapshot | None = None,
) -> None:
    fingerprint = _bar_fingerprint(
        decision_time_ms=decision_time_ms,
        bar_close_ms=bar_close_ms,
        close=close,
        ready_snapshot=ready_snapshot,
    )
    if (
        lifecycle.last_bar_close_ms is not None
        and bar_close_ms == lifecycle.last_bar_close_ms
    ):
        if lifecycle.last_bar_fingerprint == fingerprint:
            return
        raise RetestConflictError(
            "same retest bar identity maps to different scientific content"
        )
    if lifecycle.terminal:
        return
    if not math.isfinite(close) or close <= 0:
        lifecycle.stage = RetestStage.INVALID
        lifecycle.terminal_reason = "non-positive or non-finite retest close"
        lifecycle.terminal_time_ms = decision_time_ms
        return
    if decision_time_ms <= lifecycle.arm.arm_decision_time_ms:
        raise RetestOutOfOrderError(
            "retest bar must be strictly after the armed raw-C0 decision time"
        )
    if bar_close_ms <= lifecycle.arm.arm_decision_time_ms:
        raise RetestOutOfOrderError(
            "retest bar close must be strictly after the armed raw-C0 decision time"
        )
    if (
        lifecycle.last_bar_close_ms is not None
        and bar_close_ms < lifecycle.last_bar_close_ms
    ):
        raise RetestOutOfOrderError(
            "retest bar close time must advance monotonically"
        )
    if (
        lifecycle.last_decision_time_ms is not None
        and decision_time_ms <= lifecycle.last_decision_time_ms
    ):
        raise RetestOutOfOrderError("retest decision time must advance monotonically")
    if ready_snapshot is not None:
        if (
            ready_snapshot.campaign_id != lifecycle.arm.campaign_id
            or ready_snapshot.campaign_manifest_sha256
            != lifecycle.arm.campaign_manifest_sha256
            or ready_snapshot.opportunity_id != lifecycle.arm.opportunity_id
            or ready_snapshot.decision_time_ms != decision_time_ms
            or ready_snapshot.bar_close_ms != bar_close_ms
            or ready_snapshot.price != close
            or ready_snapshot.breakout_level != lifecycle.arm.breakout_level
        ):
            raise RetestConflictError(
                "READY snapshot does not match retest bar/arm identity"
            )

    lifecycle.last_bar_close_ms = bar_close_ms
    lifecycle.last_decision_time_ms = decision_time_ms
    lifecycle.last_close = close
    lifecycle.last_bar_fingerprint = fingerprint
    lifecycle.elapsed_bars += 1

    if lifecycle.elapsed_bars > lifecycle.arm.retest_horizon_bars:
        lifecycle.stage = RetestStage.TIMEOUT
        lifecycle.terminal_reason = (
            "no READY retest within the frozen lifecycle horizon"
        )
        lifecycle.terminal_time_ms = decision_time_ms
        return
    if lifecycle.stage == RetestStage.ARMED and _touches(lifecycle.arm, close):
        lifecycle.stage = RetestStage.RETEST_TOUCH
        lifecycle.touch_bar_close_ms = bar_close_ms
        lifecycle.touch_price = close
        return
    if (
        lifecycle.stage == RetestStage.RETEST_TOUCH
        and _recovers(lifecycle.arm, close)
    ):
        if ready_snapshot is None:
            lifecycle.stage = RetestStage.CENSORED
            lifecycle.terminal_reason = (
                RetestCensorReason.CAUSAL_CONTEXT_UNAVAILABLE.value
            )
            lifecycle.terminal_time_ms = decision_time_ms
            return
        lifecycle.stage = RetestStage.READY
        lifecycle.ready_time_ms = decision_time_ms
        lifecycle.ready_price = close
        lifecycle.ready_snapshot = ready_snapshot
        lifecycle.terminal_time_ms = decision_time_ms


def _arm_to_dict(arm: RetestArm) -> dict:
    return {
        "opportunity_id": arm.opportunity_id,
        "campaign_id": arm.campaign_id,
        "campaign_manifest_sha256": arm.campaign_manifest_sha256,
        "market": arm.market.value,
        "symbol": arm.symbol,
        "family": arm.family.value,
        "direction": arm.direction.value,
        "primary_interval": arm.primary_interval,
        "breakout_level": arm.breakout_level,
        "arm_price": arm.arm_price,
        "arm_atr": arm.arm_atr,
        "arm_decision_time_ms": arm.arm_decision_time_ms,
        "retest_horizon_bars": arm.retest_horizon_bars,
        "protocol_version": arm.protocol_version,
    }


def _arm_from_dict(d: dict) -> RetestArm:
    return RetestArm(
        opportunity_id=d["opportunity_id"],
        campaign_id=d["campaign_id"],
        campaign_manifest_sha256=d["campaign_manifest_sha256"],
        market=Market(d["market"]),
        symbol=d["symbol"],
        family=SignalFamily(d["family"]),
        direction=Direction(d["direction"]),
        primary_interval=d["primary_interval"],
        breakout_level=d["breakout_level"],
        arm_price=d["arm_price"],
        arm_atr=d["arm_atr"],
        arm_decision_time_ms=d["arm_decision_time_ms"],
        retest_horizon_bars=d["retest_horizon_bars"],
        protocol_version=d.get("protocol_version", RETEST_PROTOCOL_VERSION),
    )


def serialize_lifecycle(lifecycle: RetestLifecycle) -> dict:
    """Canonical JSON-safe snapshot of the full lifecycle for durable storage."""

    ready = lifecycle.ready_snapshot
    return {
        "arm": _arm_to_dict(lifecycle.arm),
        "stage": lifecycle.stage.value,
        "elapsed_bars": lifecycle.elapsed_bars,
        "last_bar_close_ms": lifecycle.last_bar_close_ms,
        "last_decision_time_ms": lifecycle.last_decision_time_ms,
        "last_close": lifecycle.last_close,
        "last_bar_fingerprint": lifecycle.last_bar_fingerprint,
        "touch_bar_close_ms": lifecycle.touch_bar_close_ms,
        "touch_price": lifecycle.touch_price,
        "ready_time_ms": lifecycle.ready_time_ms,
        "ready_price": lifecycle.ready_price,
        "terminal_time_ms": lifecycle.terminal_time_ms,
        "terminal_reason": lifecycle.terminal_reason,
        "ready_snapshot": None if ready is None else {
            "campaign_id": ready.campaign_id,
            "campaign_manifest_sha256": ready.campaign_manifest_sha256,
            "opportunity_id": ready.opportunity_id,
            "market": ready.market.value,
            "symbol": ready.symbol,
            "family": ready.family.value,
            "direction": ready.direction.value,
            "decision_time_ms": ready.decision_time_ms,
            "bar_close_ms": ready.bar_close_ms,
            "price": ready.price,
            "breakout_level": ready.breakout_level,
            "bbo": asdict(ready.bbo),
            "research_context_version": ready.research_context_version,
            "research_context_json": ready.research_context_json,
            "research_context_sha256": ready.research_context_sha256,
            "content_sha256": ready.content_sha256,
        },
    }


def restore_lifecycle(data: dict) -> RetestLifecycle:
    """Restore a RetestLifecycle from a durable canonical snapshot."""

    arm = _arm_from_dict(data["arm"])
    lifecycle = RetestLifecycle(arm=arm)
    lifecycle.stage = RetestStage(data["stage"])
    lifecycle.elapsed_bars = data.get("elapsed_bars", 0)
    lifecycle.last_bar_close_ms = data.get("last_bar_close_ms")
    lifecycle.last_decision_time_ms = data.get("last_decision_time_ms")
    lifecycle.last_close = data.get("last_close")
    lifecycle.last_bar_fingerprint = data.get("last_bar_fingerprint")
    lifecycle.touch_bar_close_ms = data.get("touch_bar_close_ms")
    lifecycle.touch_price = data.get("touch_price")
    lifecycle.ready_time_ms = data.get("ready_time_ms")
    lifecycle.ready_price = data.get("ready_price")
    lifecycle.terminal_time_ms = data.get("terminal_time_ms")
    lifecycle.terminal_reason = data.get("terminal_reason")
    ready = data.get("ready_snapshot")
    if ready is not None:
        bbo = BboExecutionEvidence(**ready["bbo"])
        lifecycle.ready_snapshot = RetestReadySnapshot(
            campaign_id=ready["campaign_id"],
            campaign_manifest_sha256=ready["campaign_manifest_sha256"],
            opportunity_id=ready["opportunity_id"],
            market=Market(ready["market"]),
            symbol=ready["symbol"],
            family=SignalFamily(ready["family"]),
            direction=Direction(ready["direction"]),
            decision_time_ms=ready["decision_time_ms"],
            bar_close_ms=ready["bar_close_ms"],
            price=ready["price"],
            breakout_level=ready["breakout_level"],
            bbo=bbo,
            research_context_version=ready["research_context_version"],
            research_context_json=ready["research_context_json"],
            research_context_sha256=ready["research_context_sha256"],
            content_sha256=ready["content_sha256"],
        )
    return lifecycle


def recover_after_restart(
    data: dict,
    *,
    continuity_proven: bool,
    resume_decision_time_ms: int,
) -> RetestLifecycle:
    """Restore a durable retest lifecycle after a process restart.

    ARMED / RETEST_TOUCH are restored exactly so the lifecycle can continue.
    A terminal lifecycle is returned untouched. If the lifecycle was
    mid-flight (non-terminal) but completed-bar continuity cannot be proven,
    it is censored with RESTART_GAP instead of silently resuming from an
    assumed ARMED state. A RAW_C0 durable row is never silently promoted to
    ARMED.
    """
    lifecycle = restore_lifecycle(data)
    if lifecycle.terminal:
        return lifecycle
    if lifecycle.stage is RetestStage.RAW_C0:
        lifecycle.stage = RetestStage.CENSORED
        lifecycle.terminal_reason = RetestCensorReason.RESTART_GAP.value
        lifecycle.terminal_time_ms = resume_decision_time_ms
        return lifecycle
    if not continuity_proven:
        censor_lifecycle(
            lifecycle,
            reason=RetestCensorReason.RESTART_GAP,
            decision_time_ms=resume_decision_time_ms,
        )
    return lifecycle
