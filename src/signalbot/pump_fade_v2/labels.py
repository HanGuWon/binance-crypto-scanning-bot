"""Versioned, append-only event/alert-origin outcome labels with explicit censoring."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from decimal import Decimal
from enum import StrEnum
from pathlib import Path

from signalbot.pump_fade_v2 import POLICY_VERSION

FIVE_MIN_MS = 300_000


def load_primary_squeeze_threshold(policy_path: str | Path) -> float:
    """Read the active policy's primary squeeze threshold; reject other versions."""

    policy = json.loads(Path(policy_path).read_text(encoding="utf-8"))
    if policy.get("policy_version") != POLICY_VERSION:
        raise ValueError("label policy version differs from the active Pump-fade package")
    threshold = float(policy["evaluation"]["squeeze_adverse_mark_excursion_pct"])
    if not math.isfinite(threshold) or threshold <= 0:
        raise ValueError("primary squeeze threshold must be finite and positive")
    return threshold


class LabelStatus(StrEnum):
    PENDING = "PENDING"
    CENSORED = "CENSORED"
    COMPLETE = "COMPLETE"


@dataclass(frozen=True, slots=True)
class MarkBar:
    """Completed mark-price OHLC bar, used only for path/tail risk."""

    close_ms: int
    received_ms: int
    high: float
    low: float
    close: float

    def __post_init__(self) -> None:
        if self.received_ms <= self.close_ms or not 0 < self.low <= self.close <= self.high:
            raise ValueError("mark bar must be closed, ordered and valid")


@dataclass(frozen=True, slots=True)
class ExecutableQuote:
    at_ms: int
    received_ms: int
    bid: float
    ask: float

    def __post_init__(self) -> None:
        if not 0 < self.bid <= self.ask or self.received_ms < self.at_ms:
            raise ValueError("invalid quote")


@dataclass(frozen=True, slots=True)
class FundingSettlement:
    at_ms: int
    received_ms: int
    rate: float  # positive rate credits a short at timestamped settlement


@dataclass(frozen=True, slots=True)
class Outcome:
    policy_version: str
    event_id: str
    origin: str
    origin_ms: int
    labeled_at_ms: int
    horizon_hours: int
    status: LabelStatus
    squeeze_primary: bool | None
    squeeze_primary_threshold_pct: float
    mark_mae_pct: float | None
    mark_mfe_pct: float | None
    near_liquidation_scenarios_pct: dict[str, bool] | None
    triple_barrier_first: str | None
    net_short_bps: float | None
    net_data_status: str
    sample_count: int


def label_outcome(
    *,
    event_id: str,
    origin: str,
    origin_ms: int,
    origin_mark: float,
    horizon_hours: int,
    decision_now_ms: int,
    marks: tuple[MarkBar, ...],
    entry_quote: ExecutableQuote | None = None,
    exit_quote: ExecutableQuote | None = None,
    settlements: tuple[FundingSettlement, ...] = (),
    verified_funding_coverage: bool = False,
    taker_fee_each_side_bps: float = 6.0,
    slippage_each_side_bps: float = 5.0,
    squeeze_adverse_pct: float,
    stop_up_pct: float = 20.0,
    target_down_pct: float = 10.0,
) -> Outcome:
    """Label a fixed parent opportunity at 4/24h without future receipts.

    Mark bars represent mark-price path, never executable fill prices. Net value
    requires a contemporaneous executable bid/ask at both endpoints and verified
    cash-flow coverage; absent inputs are UNAVAILABLE rather than zero.
    """

    if origin not in ("event", "alert") or horizon_hours not in (4, 24):
        raise ValueError("unknown origin or outcome horizon")
    if origin_mark <= 0 or min(stop_up_pct, target_down_pct, squeeze_adverse_pct) <= 0:
        raise ValueError("invalid origin price or triple barriers")
    expiry = origin_ms + horizon_hours * 3_600_000
    in_window = tuple(sorted((bar for bar in marks if origin_ms < bar.close_ms <= expiry
                              and bar.received_ms <= decision_now_ms),
                             key=lambda item: item.close_ms))
    expected = horizon_hours * 12
    complete_time = decision_now_ms > expiry
    continuous = (len(in_window) == expected and
                  all(bar.close_ms == origin_ms + (i + 1) * FIVE_MIN_MS
                      for i, bar in enumerate(in_window)))
    status = (LabelStatus.PENDING if not complete_time else
              LabelStatus.COMPLETE if continuous else LabelStatus.CENSORED)
    mae: float | None = None
    mfe: float | None = None
    squeeze: bool | None = None
    scenarios: dict[str, bool] | None = None
    barrier: str | None = None
    net_bps: float | None = None
    net_status = "UNAVAILABLE_EXECUTION_OR_FUNDING_INPUTS"
    if status is LabelStatus.COMPLETE:
        mae = (max(bar.high for bar in in_window) / origin_mark - 1) * 100
        mfe = (1 - min(bar.low for bar in in_window) / origin_mark) * 100
        threshold = Decimal(str(squeeze_adverse_pct)) / Decimal(100)
        squeeze = any(
            Decimal(str(bar.high)) / Decimal(str(origin_mark)) - Decimal(1) >= threshold
            for bar in in_window
        )
        scenarios = {str(x): mae >= x for x in (14, 20, 25, 33)}
        barrier = "TIME"
        for bar in in_window:
            stop = bar.high >= origin_mark * (1 + stop_up_pct / 100)
            target = bar.low <= origin_mark * (1 - target_down_pct / 100)
            if stop and target:
                barrier = "AMBIGUOUS_SAME_BAR"
                break
            if stop or target:
                barrier = "STOP" if stop else "TARGET"
                break
        quotes_valid = (entry_quote is not None and exit_quote is not None
                        and 0 <= entry_quote.received_ms - origin_ms <= 5_000
                        and 0 <= exit_quote.received_ms - expiry <= 5_000
                        and entry_quote.bid > 0 and exit_quote.ask > 0)
        if quotes_valid and verified_funding_coverage:
            assert entry_quote is not None and exit_quote is not None
            funding = sum(s.rate for s in settlements if origin_ms < s.at_ms <= expiry
                          and s.received_ms <= decision_now_ms)
            net_bps = ((entry_quote.bid - exit_quote.ask) / entry_quote.bid * 10_000
                       + funding * 10_000 - 2 * (taker_fee_each_side_bps
                                                   + slippage_each_side_bps))
            net_status = "EXECUTABLE_QUOTE_SCENARIO_NOT_CONFIRMED_FILLS"
    return Outcome(POLICY_VERSION, event_id, origin, origin_ms, decision_now_ms,
                   horizon_hours, status, squeeze, squeeze_adverse_pct,
                   mae, mfe, scenarios, barrier, net_bps, net_status, len(in_window))


class AppendOnlyOutcomeStore:
    """Research-only local SQLite; rejects conflicting retries and historical edits."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as connection:
            connection.execute("""CREATE TABLE IF NOT EXISTS pump_v2_outcomes (
                identity TEXT PRIMARY KEY, sha256 TEXT NOT NULL, payload_json TEXT NOT NULL
            )""")

    def append(self, outcome: Outcome) -> bool:
        """Insert an immutable outcome once; false means exact idempotent replay."""

        key, digest, payload = outcome_receipt_identity(outcome)
        with sqlite3.connect(self.path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT sha256 FROM pump_v2_outcomes WHERE identity=?",
                               (key,)).fetchone()
            if row is not None:
                if row[0] != digest:
                    raise ValueError("immutable outcome identity conflict")
                return False
            conn.execute("INSERT INTO pump_v2_outcomes VALUES (?,?,?)", (key, digest, payload))
            return True


def outcome_receipt_identity(outcome: Outcome) -> tuple[str, str, str]:
    """Return stable append-only key, digest and canonical bytes for one snapshot."""

    payload = json.dumps(asdict(outcome), sort_keys=True, separators=(",", ":"))
    key = (f"{outcome.policy_version}|{outcome.event_id}|{outcome.origin}|"
           f"{outcome.origin_ms}|{outcome.horizon_hours}|{outcome.labeled_at_ms}")
    digest = hashlib.sha256(payload.encode()).hexdigest()
    return key, digest, payload
