"""Point-in-time pump opportunity, risk overlay and release state machine.

Consumers must pass only observations available by ``decision_at_ms``. Inputs are
immutable so the same replay prefix always produces the same transition.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, replace
from enum import StrEnum
from itertools import pairwise

from signalbot.pump_fade_v2 import POLICY_VERSION

MINUTE_MS = 60_000
FIVE_MINUTES = 5 * MINUTE_MS
DAY_MS = 24 * 60 * MINUTE_MS


class State(StrEnum):
    WAIT = "WAIT"
    WAIT_UNAVAILABLE = "WAIT_UNAVAILABLE"
    FADE_CANDIDATE = "FADE-CANDIDATE"
    SKIP = "SKIP"
    EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class ClosedBar:
    """Closed Binance OHLCV; receipt is when this value became available."""

    close_ms: int
    received_ms: int
    open: float
    high: float
    low: float
    close: float
    base_volume: float
    quote_volume: float

    def __post_init__(self) -> None:
        if self.received_ms <= self.close_ms:
            raise ValueError("closed bar requires receipt after exchange close time")
        if not (0 < self.low <= min(self.open, self.close) <= max(self.open, self.close)
                <= self.high):
            raise ValueError("invalid closed candle OHLC")
        if self.base_volume < 0 or self.quote_volume < 0:
            raise ValueError("volumes cannot be negative")


@dataclass(frozen=True, slots=True)
class Observation:
    """Scalar observed at receipt time; event time cannot be in the future."""

    event_ms: int
    received_ms: int
    value: float

    def __post_init__(self) -> None:
        if self.event_ms > self.received_ms or not math.isfinite(self.value):
            raise ValueError("noncausal or nonfinite observation")


@dataclass(frozen=True, slots=True)
class FundingCap:
    observed_ms: int
    cap: float
    interval_hours: int
    floor: float | None = None

    def __post_init__(self) -> None:
        if (self.observed_ms < 0 or not math.isfinite(self.cap) or not 0 < self.cap <= 1
                or self.interval_hours <= 0):
            raise ValueError("invalid observed funding metadata")
        if self.floor is not None and (
            not math.isfinite(self.floor) or self.floor < -1 or self.floor >= self.cap
        ):
            raise ValueError("observed funding floor must be below cap")


@dataclass(frozen=True, slots=True)
class BookQuality:
    received_ms: int
    bid: float
    ask: float
    bid_depth_50bps_usdt: float
    ask_depth_50bps_usdt: float
    depth_received_ms: int

    def __post_init__(self) -> None:
        if min(self.bid, self.ask) <= 0 or self.ask < self.bid:
            raise ValueError("crossed or nonpositive book")
        if min(self.bid_depth_50bps_usdt, self.ask_depth_50bps_usdt) < 0:
            raise ValueError("negative depth")


@dataclass(frozen=True, slots=True)
class LiquidationSample:
    event_ms: int
    received_ms: int
    symbol: str
    forced_order_side: str

    @property
    def is_short_liquidation(self) -> bool:
        """A forced BUY is exchange-side buy-to-close of a short."""

        return self.forced_order_side == "BUY"


@dataclass(frozen=True, slots=True)
class Event:
    event_id: str
    symbol: str
    event_ms: int
    event_price: float
    event_high: float
    event_low: float
    listing_observed_ms: int
    listing_open_ms: int
    policy_version: str = POLICY_VERSION


@dataclass(frozen=True, slots=True)
class Episode:
    event: Event
    state: State
    peak: float
    peak_at_ms: int
    trough: float
    evaluated_at_ms: int


@dataclass(frozen=True, slots=True)
class InputPanel:
    """Receipt-ordered observations. Unknown mandatory values block release."""

    five_minute: tuple[ClosedBar, ...]
    fifteen_minute: tuple[ClosedBar, ...]
    oi: tuple[Observation, ...]
    predicted_funding: tuple[Observation, ...]
    caps: tuple[FundingCap, ...]
    book: BookQuality | None
    liquidations: tuple[LiquidationSample, ...] = ()


@dataclass(frozen=True, slots=True)
class Decision:
    event_id: str
    policy_version: str
    state: State
    decision_at_ms: int
    reasons: tuple[str, ...]
    releases: tuple[str, ...]
    continuation_risk: bool
    invalidation_price: float
    expires_at_ms: int
    metrics: tuple[tuple[str, float], ...] = ()
    validation_tier: str = "EXPLORATORY"


def _bars_available(bars: tuple[ClosedBar, ...], now_ms: int) -> tuple[ClosedBar, ...]:
    return tuple(bar for bar in bars if bar.received_ms <= now_ms)


def _continuous(bars: tuple[ClosedBar, ...], spacing_ms: int) -> bool:
    return all(b.close_ms - a.close_ms == spacing_ms for a, b in pairwise(bars))


def derive_fifteen_minute(bars: tuple[ClosedBar, ...], now_ms: int) -> tuple[ClosedBar, ...]:
    """Group three consecutive available closed 5m candles, never partial 15m."""

    eligible = _bars_available(bars, now_ms)
    output: list[ClosedBar] = []
    for end in range(2, len(eligible)):
        batch = eligible[end - 2:end + 1]
        last = batch[-1]
        if (last.close_ms + 1) % (15 * MINUTE_MS) or not _continuous(batch, FIVE_MINUTES):
            continue
        output.append(ClosedBar(
            close_ms=last.close_ms, received_ms=max(b.received_ms for b in batch),
            open=batch[0].open, high=max(b.high for b in batch),
            low=min(b.low for b in batch), close=last.close,
            base_volume=sum(b.base_volume for b in batch),
            quote_volume=sum(b.quote_volume for b in batch),
        ))
    return tuple(output)


def admit_pump(
    symbol: str,
    bars: tuple[ClosedBar, ...],
    *,
    now_ms: int,
    latest_admitted_event_ms: int | None,
    listing_open_ms: int | None,
    listing_observed_ms: int | None,
) -> Event | None:
    """Admit first +30%/24h, 2m quote-volume event with 1h same-symbol cooldown.

    Requires 289 *continuous* closed 5m bars and PIT listing evidence.
    """

    available = _bars_available(bars, now_ms)
    if len(available) < 289 or listing_open_ms is None or listing_observed_ms is None:
        return None
    if listing_observed_ms > now_ms:
        return None
    window = available[-289:]
    if not _continuous(window, FIVE_MINUTES):
        return None
    current = window[-1]
    if latest_admitted_event_ms is not None and (
        current.close_ms - latest_admitted_event_ms < 60 * MINUTE_MS
    ):
        return None
    if current.close / window[0].close - 1 < 0.30:
        return None
    if sum(bar.quote_volume for bar in window[1:]) < 2_000_000:
        return None
    digest = hashlib.sha256(
        f"{POLICY_VERSION}|USD-M|{symbol}|{current.close_ms}".encode()
    ).hexdigest()
    return Event(
        event_id=digest,
        symbol=symbol,
        event_ms=current.close_ms,
        event_price=current.close,
        event_high=current.high,
        event_low=current.low,
        listing_observed_ms=listing_observed_ms,
        listing_open_ms=listing_open_ms,
    )


def start_episode(event: Event) -> Episode:
    """Create the immutable initial WAIT state at the admitted event."""

    # Same-bar high/low order is unknown; the first causal post-peak trough is
    # the closed event price, not the low of the event candle.
    return Episode(event, State.WAIT, event.event_high, event.event_ms,
                   event.event_price, event.event_ms)


def _safe_lookback(bars: tuple[ClosedBar, ...], now_ms: int) -> bool:
    if len(bars) < 157 or not _continuous(bars[-157:], FIVE_MINUTES):
        return False
    return all(bar.received_ms <= now_ms for bar in bars[-157:])


def _funding_cap_at(caps: tuple[FundingCap, ...], asof_ms: int) -> FundingCap | None:
    eligible = [cap for cap in caps if cap.observed_ms <= asof_ms]
    return max(eligible, key=lambda x: x.observed_ms) if eligible else None


def _funding_wait(event: Event, caps: tuple[FundingCap, ...],
                  rates: tuple[Observation, ...], now_ms: int) -> bool:
    nearby = tuple(rate for rate in rates if now_ms - DAY_MS <= rate.received_ms
                   <= now_ms)
    hit = any(
        (cap := _funding_cap_at(caps, rate.received_ms)) is not None
        and rate.value >= cap.cap - 0.0001
        for rate in nearby
    )
    # Cap-risk clearance is independent of the event-specific R5 confirmation.
    # Pre-admission normal samples may clear a cap hit, but cannot earn R5.
    if hit and _funding_cap_risk_cleared(caps, nearby):
        hit = False
    ordered = sorted((c for c in caps if now_ms - DAY_MS <= c.observed_ms <= now_ms),
                     key=lambda c: c.observed_ms)
    pending_restore: int | None = None
    for previous, current in pairwise(ordered):
        if current.interval_hours < previous.interval_hours:
            if pending_restore is None:
                pending_restore = previous.interval_hours
        elif pending_restore is not None and current.interval_hours >= pending_restore:
            pending_restore = None
    return hit or pending_restore is not None


def _release_retest(fifteen: tuple[ClosedBar, ...], peak: float,
                    event_ms: int) -> bool:
    fifteen = tuple(bar for bar in fifteen if bar.close_ms > event_ms)
    for index in range(2, len(fifteen)):
        bar = fifteen[index]
        floor = min(fifteen[index - 1].low, fifteen[index - 2].low)
        if bar.close >= floor:
            continue
        for retest in fifteen[index + 1: index + 5]:
            if (floor * 0.9975 <= retest.high < peak
                    and retest.close < floor):
                return True
    return False


def _release_avwap(event: Event, bars: tuple[ClosedBar, ...],
                   fifteen: tuple[ClosedBar, ...]) -> bool:
    if not fifteen or fifteen[-1].close_ms <= event.event_ms:
        return False
    qualifying = tuple(bar for bar in bars if event.event_ms < bar.close_ms
                       <= fifteen[-1].close_ms)
    if (not qualifying or qualifying[0].close_ms != event.event_ms + FIVE_MINUTES
            or not _continuous(qualifying, FIVE_MINUTES)):
        return False
    volume = sum(bar.base_volume for bar in qualifying)
    if len(qualifying) < 3 or volume <= 0:
        return False
    avwap = sum((bar.high + bar.low + bar.close) / 3 * bar.base_volume
                for bar in qualifying) / volume
    return fifteen[-1].close < avwap


def _funding_normalizations_after_hit(
    caps: tuple[FundingCap, ...], rates: tuple[Observation, ...], *,
    not_before_ms: int | None,
) -> bool:
    """Return whether a cap hit has three spaced normal samples after it.

    ``not_before_ms`` constrains event-specific R5 evidence. Passing ``None``
    evaluates the cap-risk streak independent of the event admission time.
    """

    ordered = sorted(rates, key=lambda r: r.received_ms)
    had_hit = False
    normal: list[Observation] = []
    for sample in ordered:
        cap = _funding_cap_at(caps, sample.received_ms)
        if cap is None:
            continue
        if sample.value >= cap.cap - 0.0001:
            had_hit = True
            normal.clear()
        elif (had_hit and (not_before_ms is None or sample.received_ms >= not_before_ms)
              and sample.value <= cap.cap - 0.001):
            if not normal or sample.received_ms - normal[-1].received_ms >= MINUTE_MS:
                normal.append(sample)
    return had_hit and len(normal) >= 3


def _funding_cap_risk_cleared(
    caps: tuple[FundingCap, ...], rates: tuple[Observation, ...],
) -> bool:
    """Three observed normalizations after the latest in-window cap hit."""

    return _funding_normalizations_after_hit(caps, rates, not_before_ms=None)


def _release_funding(event: Event, caps: tuple[FundingCap, ...],
                     rates: tuple[Observation, ...]) -> bool:
    """R5 requires its own three post-event normalizations after a cap hit."""

    return _funding_normalizations_after_hit(
        caps, rates, not_before_ms=event.event_ms,
    )


def step_episode(episode: Episode, panel: InputPanel, *, now_ms: int) -> tuple[Episode, Decision]:
    """Apply terminal, missing-input, WAIT and release precedence without future rows."""

    event = episode.event
    if now_ms < episode.evaluated_at_ms:
        raise ValueError("decision clock cannot move backwards")
    bars = _bars_available(panel.five_minute, now_ms)
    event_bars = tuple(b for b in bars if b.close_ms >= event.event_ms)
    has_event_continuity = (
        bool(event_bars)
        and event_bars[0].close_ms == event.event_ms
        and _continuous(event_bars, FIVE_MINUTES)
        and 0 <= now_ms - event_bars[-1].close_ms <= FIVE_MINUTES + 5_000
    )
    fifteen = (_bars_available(panel.fifteen_minute, now_ms)
               if panel.fifteen_minute else derive_fifteen_minute(bars, now_ms))
    future = any(sample.received_ms > now_ms for sample in panel.oi + panel.predicted_funding)
    # Unavailable future receipts are filtered; they never evaluate as false risk.
    oi = tuple(sorted((x for x in panel.oi if x.received_ms <= now_ms),
                      key=lambda x: x.received_ms))
    initial_oi = next((x for x in oi if event.event_ms <= x.received_ms
                       <= event.event_ms + FIVE_MINUTES), None)
    rates = tuple(sorted((x for x in panel.predicted_funding if x.received_ms <= now_ms),
                         key=lambda x: x.received_ms))
    caps = tuple(sorted((x for x in panel.caps if x.observed_ms <= now_ms),
                        key=lambda x: x.observed_ms))
    observed = tuple(bar for bar in bars if episode.evaluated_at_ms < bar.close_ms <= now_ms)
    updated_peak, peak_at, trough = episode.peak, episode.peak_at_ms, episode.trough
    for bar in observed:
        if bar.high > updated_peak:
            updated_peak, peak_at, trough = bar.high, bar.close_ms, bar.close
        else:
            trough = min(trough, bar.low)
    spread = None
    metrics: dict[str, float] = {}
    if panel.book is not None:
        spread = (panel.book.ask - panel.book.bid) / (
            (panel.book.ask + panel.book.bid) / 2) * 10_000
        metrics["spread_bps"] = spread
        metrics["book_age_ms"] = float(now_ms - panel.book.received_ms)
        metrics["depth_age_ms"] = float(now_ms - panel.book.depth_received_ms)
    if oi:
        metrics["oi_age_ms"] = float(now_ms - oi[-1].received_ms)
    if rates:
        metrics["predicted_funding_age_ms"] = float(now_ms - rates[-1].received_ms)
    if caps:
        metrics["funding_metadata_age_ms"] = float(now_ms - caps[-1].observed_ms)
    metrics["post_peak_drawdown_pct"] = (updated_peak - trough) / updated_peak * 100
    if bars:
        metrics["post_trough_rebound_pct"] = (bars[-1].close - trough) / trough * 100
    if oi and initial_oi is not None and initial_oi.value > 0:
        metrics["oi_change_from_first_post_event_pct"] = (
            oi[-1].value / initial_oi.value - 1) * 100
    if rates:
        metrics["predicted_funding"] = rates[-1].value
    reasons: list[str] = []
    release: list[str] = []
    if episode.state in (State.SKIP, State.EXPIRED):
        state = episode.state
        reasons.append("terminal_state_absorbing")
    elif now_ms >= event.event_ms + DAY_MS:
        state = State.EXPIRED
        reasons.append("event_ttl_24h")
    elif event.listing_observed_ms > event.event_ms:
        state = State.WAIT_UNAVAILABLE
        reasons.append("listing_metadata_not_observed_at_event")
    elif now_ms - event.listing_open_ms < 7 * DAY_MS:
        state = State.SKIP
        reasons.append("new_market_under_seven_days")
    elif panel.book is not None and (
        (spread is not None and spread > 30)
        or panel.book.bid_depth_50bps_usdt < 100_000
        or panel.book.ask_depth_50bps_usdt < 100_000
    ) and (0 <= now_ms - panel.book.received_ms <= 5_000
           and 0 <= now_ms - panel.book.depth_received_ms <= 5_000):
        state = State.SKIP
        reasons.append("unacceptable_observed_spread_or_depth")
    elif (updated_peak - trough) / updated_peak >= 0.30 and (
        bars and (bars[-1].close - trough) / trough >= 0.10
    ):
        state = State.SKIP
        reasons.append("primary_joint_post_crash_rebound_30_10")
    elif (not has_event_continuity or not _safe_lookback(bars, now_ms)
          or not caps or not oi or not rates
          or initial_oi is None
          or panel.book is None
          or not 0 <= now_ms - panel.book.received_ms <= 5_000
          or not 0 <= now_ms - panel.book.depth_received_ms <= 5_000
          or not 0 <= now_ms - caps[-1].observed_ms <= 300_000
          or not 0 <= now_ms - oi[-1].received_ms <= 900_000
          or not 0 <= now_ms - rates[-1].received_ms <= 300_000
          or event.listing_observed_ms > event.event_ms):
        state = State.WAIT_UNAVAILABLE
        reasons.append("mandatory_point_in_time_inputs_unavailable_or_stale")
        if future:
            reasons.append("future_receipts_excluded")
    else:
        trailing_one = bars[-1].close / bars[-13].close - 1
        trailing_four = bars[-1].close / bars[-49].close - 1
        hour_quote = sum(bar.quote_volume for bar in bars[-12:])
        hours = [sum(bar.quote_volume for bar in bars[-12 - 12 * (j + 1):
                                                          -12 - 12 * j])
                 for j in range(12)]
        baseline = sorted(hours)[len(hours) // 2 - 1:len(hours) // 2 + 1]
        median = sum(baseline) / 2
        metrics["trailing_1h_return_pct"] = trailing_one * 100
        metrics["trailing_4h_return_pct"] = trailing_four * 100
        metrics["hour_volume_to_prior_median"] = (
            hour_quote / median if median > 0 else float("nan")
        )
        wait = []
        if bars[-1].close > bars[-49].close * 1.15:
            wait.append("4h_return_gt_15pct")
        if bars[-1].close > bars[-13].close * 1.05:
            wait.append("1h_return_gt_5pct")
        if median > 0 and hour_quote >= 2 * median:
            wait.append("1h_volume_at_least_2x_prior12h_median")
        if _funding_wait(event, caps, rates, now_ms):
            wait.append("observed_funding_cap_or_interval_risk")
        if wait:
            state = State.WAIT
            reasons.extend(wait)
        else:
            if now_ms - peak_at >= 60 * MINUTE_MS:
                release.append("R1_NO_HIGH_60M")
            fifteen_fresh = bool(fifteen and 0 <= now_ms - fifteen[-1].received_ms
                                 <= 15 * MINUTE_MS)
            if fifteen_fresh and _release_retest(fifteen, updated_peak, event.event_ms):
                release.append("R2_15M_BREAK_FAILED_RETEST")
            lagged_oi = oi[:-1]  # last row is deliberately excluded even if already received
            post = tuple(s for s in lagged_oi if s.received_ms >= event.event_ms)
            if len(post) > 1:
                peak_oi = max(s.value for s in post)
                if (peak_oi > 0 and post[-1].value <= peak_oi * 0.95
                        and now_ms - post[-1].received_ms <= 900_000):
                    release.append("R3_OI_FALL_5PCT_LAG")
            if fifteen_fresh and _release_avwap(event, bars, fifteen):
                release.append("R4_15M_CLOSE_BELOW_AVWAP")
            if _release_funding(event, caps, rates):
                release.append("R5_PREDICTED_FUNDING_NORMALIZED")
            state = State.FADE_CANDIDATE if release else State.WAIT
            if not release:
                reasons.append("release_not_confirmed")
    liquidations = any(sample.is_short_liquidation and sample.received_ms <= now_ms
                       and event.event_ms <= sample.event_ms <= now_ms
                       and now_ms - sample.received_ms <= FIVE_MINUTES
                       and sample.symbol == event.symbol for sample in panel.liquidations)
    oi_rising = bool(initial_oi is not None and initial_oi.value > 0
                     and oi and oi[-1].value >= 1.10 * initial_oi.value)
    broke_high = updated_peak > episode.peak
    continuation = state is State.WAIT and (liquidations or oi_rising or broke_high)
    if continuation:
        reasons.append("continuation_risk_overlay_new_high_oi_or_forced_buy")
    replacement = replace(episode, state=state, peak=updated_peak,
                          peak_at_ms=peak_at, trough=trough, evaluated_at_ms=now_ms)
    decision = Decision(event.event_id, event.policy_version, state, now_ms,
                        tuple(reasons), tuple(release) if state is State.FADE_CANDIDATE else (),
                        continuation, updated_peak, event.event_ms + DAY_MS,
                        tuple(sorted((k, v) for k, v in metrics.items() if math.isfinite(v))))
    return replacement, decision
