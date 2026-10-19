"""Outcome-only adapter. Never imported by scanner/runtime or used to generate signals."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from typing import Literal

from signalbot.domain.models import Candle

BAR_MS = 300_000
EVALUATION_VERSION = "public-shadow-outcomes-v1"
Direction = Literal["long", "short"]


@dataclass(frozen=True)
class ObservedQuote:
    event_ms: int
    observed_ms: int
    bid: float
    ask: float


@dataclass(frozen=True)
class FundingSettlement:
    event_ms: int
    rate: float | None


@dataclass(frozen=True)
class Outcome:
    kind: str
    status: str
    entry_ms: int | None
    exit_ms: int | None
    gross_bps: float | None
    net_bps: float | None
    reason: str


def candle_diagnostic(
    candles: Sequence[Candle], *, event_ms: int, as_of_ms: int,
    direction: Direction | None, horizon_bars: int = 3, cost_bps: float = 26,
) -> Outcome:
    """Next 5m open to horizon close; descriptive event-time label, never execution proof."""
    if horizon_bars not in (1, 3, 6, 12) or cost_bps < 0 or not math.isfinite(cost_bps):
        raise ValueError("invalid diagnostic contract")
    start = event_ms + 1
    if start % BAR_MS:
        raise ValueError("event must be an official closed 5m boundary")
    end = start + horizon_bars * BAR_MS - 1
    if as_of_ms <= end:
        return Outcome("EVENT_TIME_DIAGNOSTIC", "IMMATURE", None, None, None, None,
                       "horizon_not_closed")
    rows = [c for c in candles if start <= c.open_time_ms <= end]
    if (len(rows) != horizon_bars or
            [c.open_time_ms for c in rows] != list(range(start, end + 1, BAR_MS)) or
            any(not c.is_closed or c.interval != "5m" or
                c.close_time_ms != c.open_time_ms + BAR_MS - 1 for c in rows) or
            len({(c.market, c.symbol) for c in rows}) != 1):
        return Outcome("EVENT_TIME_DIAGNOSTIC", "UNAVAILABLE", None, None, None, None,
                       "missing_conflicting_or_unclosed_candles")
    if direction is None:
        return Outcome("EVENT_TIME_DIAGNOSTIC", "NO_ENTRY", start, end, 0, 0, "abstention")
    entry, exit_price = float(rows[0].open), float(rows[-1].close)
    if entry <= 0 or exit_price <= 0:
        raise ValueError("invalid candle price")
    sign = 1 if direction == "long" else -1
    gross = sign * (exit_price / entry - 1) * 10_000
    return Outcome("EVENT_TIME_DIAGNOSTIC", "MATURE", start, end, gross,
                   gross - cost_bps, "candle_proxy_not_executable")


def executable_outcome(
    quotes: Sequence[ObservedQuote], *, available_ms: int, as_of_ms: int,
    direction: Direction | None, funding: Sequence[FundingSettlement],
    funding_schedule_complete: bool, latency_ms: int = 1000,
    horizon_bars: int = 3, maximum_age_ms: int = 2000, residual_cost_bps: float = 26,
) -> Outcome:
    """Observed bid/ask execution; residual costs exclude spread already paid by fills."""
    if (latency_ms < 0 or maximum_age_ms < 0 or horizon_bars not in (1, 3, 6, 12)
            or residual_cost_bps < 0 or not math.isfinite(residual_cost_bps)):
        raise ValueError("invalid execution contract")
    times = [q.observed_ms for q in quotes]
    if times != sorted(set(times)):
        raise ValueError("quote observations must be strictly ordered and unique")
    if any(not math.isfinite(q.bid) or not math.isfinite(q.ask) or q.bid <= 0 or
           q.ask < q.bid or q.event_ms > q.observed_ms for q in quotes):
        raise ValueError("invalid observed quote")
    earliest = available_ms + latency_ms
    eligible = [q for q in quotes if earliest <= q.observed_ms <= as_of_ms
                and q.observed_ms - q.event_ms <= maximum_age_ms]
    if not eligible:
        return Outcome("OBSERVED_BBO", "UNAVAILABLE", None, None, None, None,
                       "no_valid_quote_after_availability")
    entry = eligible[0]
    target = entry.observed_ms + horizon_bars * BAR_MS
    exits = [q for q in eligible if q.observed_ms >= target]
    if not exits:
        return Outcome("OBSERVED_BBO", "IMMATURE" if as_of_ms < target else "UNAVAILABLE",
                       entry.observed_ms, None, None, None, "exit_quote_not_observed")
    exit_quote = exits[0]
    if direction is None:
        return Outcome("OBSERVED_BBO", "NO_ENTRY", entry.observed_ms,
                       exit_quote.observed_ms, 0, 0, "abstention")
    crossed = [f for f in funding if entry.observed_ms < f.event_ms <= exit_quote.observed_ms]
    if (not funding_schedule_complete or any(f.rate is None for f in crossed)):
        return Outcome("OBSERVED_BBO", "UNAVAILABLE", entry.observed_ms,
                       exit_quote.observed_ms, None, None, "funding_coverage_missing")
    if len({f.event_ms for f in crossed}) != len(crossed):
        raise ValueError("duplicate funding settlement")
    rates = [float(f.rate) for f in crossed if f.rate is not None]
    if any(not math.isfinite(rate) for rate in rates):
        raise ValueError("invalid funding rate")
    sign = 1 if direction == "long" else -1
    entry_price = entry.ask if direction == "long" else entry.bid
    exit_price = exit_quote.bid if direction == "long" else exit_quote.ask
    gross = sign * (exit_price / entry_price - 1) * 10_000
    funding_bps = sign * sum(rates) * 10_000
    return Outcome("OBSERVED_BBO", "MATURE", entry.observed_ms, exit_quote.observed_ms,
                   gross, gross - residual_cost_bps - funding_bps,
                   "observed_spread_in_fills_plus_residual_cost")


def first_available_at(connection: sqlite3.Connection, observation_id: str, now_ms: int) -> int:
    """Conservative first evaluator read time; never backdate availability to candle close."""
    connection.execute("CREATE TABLE IF NOT EXISTS availability "
                       "(observation_id TEXT PRIMARY KEY, first_read_ms INTEGER NOT NULL)")
    connection.execute("INSERT OR IGNORE INTO availability VALUES (?,?)",
                       (observation_id, now_ms))
    connection.commit()
    row = connection.execute("SELECT first_read_ms FROM availability WHERE observation_id=?",
                             (observation_id,)).fetchone()
    assert row is not None
    return int(row[0])


def append_mature_outcome(
    connection: sqlite3.Connection, *, observation_id: str, candidate_id: str,
    horizon_bars: int, cost_bps: float, outcome: Outcome, input_sha256: str,
) -> bool:
    """Separate evaluation DB only. Exact replay is idempotent; changed content conflicts."""
    if outcome.status not in ("MATURE", "NO_ENTRY"):
        return False
    identity = json.dumps([EVALUATION_VERSION, observation_id, candidate_id,
                           horizon_bars, cost_bps, outcome.kind], separators=(",", ":"))
    oid = hashlib.sha256(identity.encode()).hexdigest()
    payload = json.dumps({"identity": json.loads(identity), "input_sha256": input_sha256,
                          "outcome": asdict(outcome)}, sort_keys=True, allow_nan=False)
    connection.execute("CREATE TABLE IF NOT EXISTS labels "
                       "(outcome_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)")
    previous = connection.execute("SELECT payload_json FROM labels WHERE outcome_id=?",
                                  (oid,)).fetchone()
    if previous:
        if previous[0] != payload:
            raise ValueError("immutable outcome conflict")
        return False
    connection.execute("INSERT INTO labels VALUES (?,?)", (oid, payload))
    connection.commit()
    return True
