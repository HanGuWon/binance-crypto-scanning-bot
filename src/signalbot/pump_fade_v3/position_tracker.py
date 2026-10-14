"""Offline position ledger and deterministic append-only shadow records."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Literal, Protocol, cast

from position_guardian.exchange.binance_user_stream import BoundedUserEventBuffer, RestResyncSession

Side = Literal["SHORT", "LONG"]
OrderTerminal = Literal["FILLED", "CANCELED", "EXPIRED"]
RESYNC_BACKOFF_SECONDS = (1.0, 2.0, 5.0)
MAX_SHADOW_RECORD_BYTES = 1_048_576


class RecordSink(Protocol):
    def append(self, record: dict[str, object]) -> bool: ...

    def contains_trade_id(self, trade_id: str) -> bool: ...

    def remember_trade_id(self, trade_id: str) -> None: ...

    def iter_records(self) -> tuple[dict[str, object], ...]: ...


class AccountConfigSource(Protocol):
    async def account_config(self) -> dict[str, object]: ...


@dataclass(frozen=True)
class RecoverySnapshot:
    position_amount: Decimal
    update_ms: int
    snapshot_id: str
    cursor: str


class PositionRecoverySource(Protocol):
    async def position_risk(self, symbol: str) -> RecoverySnapshot: ...

    async def user_trades(self, symbol: str, from_trade_id: str | None) -> tuple[Fill, ...]: ...


@dataclass(frozen=True)
class RecoveryResult:
    status: Literal["CERTAIN", "ADJUSTED", "DEGRADED"]
    fenced_event_ids: tuple[str, ...]
    replayed_trades: int


class KlineBackfillSource(Protocol):
    async def klines(self, symbol: str, start_ms: int, end_ms: int) -> tuple[ClosedCandle, ...]: ...

    async def mark_price_klines(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> tuple[ClosedCandle, ...]: ...


class PositionStore:
    """Append-only SQLite ledger mirrored to deterministic daily JSONL files."""

    def __init__(self, database_path: Path, jsonl_directory: Path) -> None:
        self._database_path = database_path
        self._jsonl_directory = jsonl_directory
        database_path.parent.mkdir(parents=True, exist_ok=True)
        jsonl_directory.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS shadow_records "
                "(record_id TEXT PRIMARY KEY, occurred_ms INTEGER NOT NULL, payload BLOB NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS shadow_trade_ids (trade_id TEXT PRIMARY KEY)"
            )

    def contains_trade_id(self, trade_id: str) -> bool:
        with sqlite3.connect(self._database_path) as connection:
            return (
                connection.execute(
                    "SELECT 1 FROM shadow_trade_ids WHERE trade_id = ?", (trade_id,)
                ).fetchone()
                is not None
            )

    def remember_trade_id(self, trade_id: str) -> None:
        with sqlite3.connect(self._database_path) as connection:
            connection.execute(
                "INSERT OR IGNORE INTO shadow_trade_ids(trade_id) VALUES (?)", (trade_id,)
            )

    def iter_records(self) -> tuple[dict[str, object], ...]:
        with sqlite3.connect(self._database_path) as connection:
            rows = connection.execute(
                "SELECT payload FROM shadow_records ORDER BY rowid"
            ).fetchall()
        records: list[dict[str, object]] = []
        for (payload,) in rows:
            decoded = json.loads(bytes(payload))
            if isinstance(decoded, dict):
                records.append(decoded)
        return tuple(records)

    def append(self, record: dict[str, object]) -> bool:
        payload = _canonical_json(record)
        if len(payload) > MAX_SHADOW_RECORD_BYTES:
            raise ValueError("shadow record exceeds the configured size bound")
        record_id = record.get("record_id")
        occurred_ms = record.get("occurred_ms")
        if not isinstance(record_id, str) or not isinstance(occurred_ms, int):
            raise ValueError("shadow records require record_id and occurred_ms")
        with sqlite3.connect(self._database_path) as connection:
            existing = connection.execute(
                "SELECT payload FROM shadow_records WHERE record_id = ?", (record_id,)
            ).fetchone()
            if existing is None:
                connection.execute(
                    "INSERT INTO shadow_records(record_id, occurred_ms, payload) VALUES (?, ?, ?)",
                    (record_id, occurred_ms, payload),
                )
            elif bytes(existing[0]) != payload:
                raise ValueError("record_id was reused with different shadow content")
        day = datetime.fromtimestamp(occurred_ms / 1000, UTC).strftime("%Y-%m-%d")
        daily_path = self._jsonl_directory / f"{day}.jsonl"
        line = payload + b"\n"
        already_present = False
        if daily_path.exists():
            with daily_path.open("rb") as input_file:
                already_present = any(
                    existing_line.rstrip(b"\r\n") == payload for existing_line in input_file
                )
        if not already_present:
            with daily_path.open("ab") as output:
                output.write(line)
                output.flush()
        return existing is None


@dataclass(frozen=True)
class Fill:
    trade_id: str
    order_id: str
    symbol: str
    side: Literal["BUY", "SELL"]
    quantity: Decimal
    price: Decimal
    occurred_ms: int


@dataclass(frozen=True)
class ClosedCandle:
    high: Decimal
    close_ms: int


@dataclass
class Tranche:
    order_id: str
    quantity: Decimal = Decimal(0)
    notional: Decimal = Decimal(0)
    fill_count: int = 0
    terminal_status: OrderTerminal | None = None
    is_entry: bool = True

    @property
    def average_price(self) -> Decimal | None:
        return self.notional / self.quantity if self.quantity else None


@dataclass
class Episode:
    episode_id: str
    symbol: str
    side: Side
    first_trade_id: str
    first_order_id: str
    opened_ms: int
    last_trade_ms: int = 0
    signed_quantity: Decimal = Decimal(0)
    initial_quantity: Decimal = Decimal(0)
    initial_notional: Decimal = Decimal(0)
    tranches: dict[str, Tranche] = field(default_factory=dict)
    close_reason: str | None = None
    invalidation: Decimal | None = None
    manual_plan: bool = False
    ledger_quality: Literal["CERTAIN", "ADJUSTED"] = "CERTAIN"
    fills_count: int = 0

    @property
    def q0(self) -> Decimal:
        return self.initial_quantity

    @property
    def p0(self) -> Decimal | None:
        return self.initial_notional / self.initial_quantity if self.initial_quantity else None

    @property
    def last_entry_price(self) -> Decimal | None:
        entries = [tranche for tranche in self.tranches.values() if tranche.is_entry]
        return entries[-1].average_price if entries else None

    @property
    def average_entry_price(self) -> Decimal | None:
        entries = [tranche for tranche in self.tranches.values() if tranche.is_entry]
        quantity = sum((tranche.quantity for tranche in entries), Decimal(0))
        notional = sum((tranche.notional for tranche in entries), Decimal(0))
        return notional / quantity if quantity else None

    @property
    def add_count(self) -> int:
        return max(0, sum(tranche.is_entry for tranche in self.tranches.values()) - 1)

    @property
    def total_fill_count(self) -> int:
        return self.fills_count


class PositionLedger:
    """Trade-ID deduped signed-position ledger for deterministic fixture replay."""

    def __init__(
        self,
        account_alias: str,
        sink: RecordSink,
        *,
        dedupe_capacity: int = 4096,
        max_open_symbols: int = 32,
        max_episode_cache: int = 4096,
        risk_reserve_rate: Decimal = Decimal("0.003"),
    ) -> None:
        if not account_alias.strip():
            raise ValueError("account_alias must be non-empty")
        if dedupe_capacity < 1:
            raise ValueError("dedupe_capacity must be positive")
        if max_open_symbols < 1 or max_episode_cache < 1:
            raise ValueError("position ledger limits must be positive")
        if risk_reserve_rate < 0:
            raise ValueError("risk_reserve_rate must be non-negative")
        self._account_alias = account_alias
        self._sink = sink
        self._episodes: dict[str, Episode] = {}
        self._active_by_symbol: dict[str, Episode] = {}
        self._trade_ids: set[str] = set()
        self._trade_id_order: deque[str] = deque()
        self._dedupe_capacity = dedupe_capacity
        self._max_open_symbols = max_open_symbols
        self._max_episode_cache = max_episode_cache
        self._risk_reserve_rate = risk_reserve_rate
        self._mismatch_counts: dict[str, int] = {}
        self._last_trade_id: dict[str, str] = {}
        self._restore_from_records()

    @property
    def episodes(self) -> tuple[Episode, ...]:
        return tuple(self._episodes.values())

    def apply_fill(self, fill: Fill) -> tuple[Episode, ...]:
        _validate_fill(fill)
        if fill.trade_id in self._trade_ids or self._sink.contains_trade_id(fill.trade_id):
            return ()
        changed: list[Episode] = []
        current = self._active_by_symbol.get(fill.symbol)
        delta = fill.quantity if fill.side == "BUY" else -fill.quantity
        if current is None:
            current = self._open_episode(fill.symbol, delta, fill)
            self._apply_piece(current, fill, fill.quantity)
            changed.append(current)
        elif current.signed_quantity == 0 or current.signed_quantity * delta > 0:
            self._apply_piece(current, fill, fill.quantity)
            changed.append(current)
        else:
            previous_quantity = abs(current.signed_quantity)
            close_quantity = min(previous_quantity, fill.quantity)
            remainder = fill.quantity - close_quantity
            reason = (
                "CLOSED_BY_FLIP"
                if remainder > 0
                else "CLOSED"
                if close_quantity == previous_quantity
                else "PARTIAL_CLOSE"
            )
            self._emit_ledger_event("CLOSE_FILL", fill, current, close_quantity, reason)
            current.signed_quantity += (
                close_quantity if current.side == "SHORT" else -close_quantity
            )
            self._reduce_open_tranches(current, close_quantity)
            current.fills_count += 1
            if current.signed_quantity == 0:
                current.close_reason = "CLOSED_BY_FLIP" if remainder > 0 else "CLOSED"
                self._active_by_symbol.pop(fill.symbol, None)
            if remainder:
                new_side_delta = remainder if fill.side == "BUY" else -remainder
                next_episode = self._open_episode(fill.symbol, new_side_delta, fill)
                self._apply_piece(next_episode, fill, remainder, trade_id=f"{fill.trade_id}:flip")
                changed.append(next_episode)
            changed.append(current)
        self._last_trade_id.pop(fill.symbol, None)
        self._last_trade_id[fill.symbol] = fill.trade_id
        while len(self._last_trade_id) > self._max_open_symbols * 2:
            self._last_trade_id.pop(next(iter(self._last_trade_id)))
        self._remember_trade_id(fill.trade_id)
        self._sink.remember_trade_id(fill.trade_id)
        return tuple(changed)

    def close_tranche(
        self, symbol: str, order_id: str, status: OrderTerminal, occurred_ms: int
    ) -> None:
        episode = self._active_by_symbol.get(symbol)
        if episode is None:
            return
        tranche = episode.tranches.get(order_id)
        if tranche is None:
            return
        self._sink.append(
            _record(
                "TRANCHE_CLOSED",
                occurred_ms,
                episode.episode_id,
                {"symbol": symbol, "order_id": order_id, "status": status},
            )
        )
        tranche.terminal_status = status

    def apply_account_update(self, payload: dict[str, object], occurred_ms: int) -> bool:
        """Apply isolated position deltas; cross funding (B+S without P) is not a fill."""
        account = payload.get("a")
        if isinstance(account, dict):
            payload = account
        if payload.get("m") == "FUNDING_FEE":
            balances = payload.get("B")
            symbol = payload.get("S")
            funding_amount: object = None
            if isinstance(balances, list):
                funding_row = next(
                    (balance for balance in balances if isinstance(balance, dict)), None
                )
                if funding_row is not None:
                    symbol = symbol if symbol is not None else funding_row.get("S")
                    funding_amount = funding_row.get("bc")
            self._sink.append(
                _record(
                    "FUNDING_OBSERVED",
                    occurred_ms,
                    "",
                    {"symbol": symbol, "amount": funding_amount},
                )
            )
            if not isinstance(payload.get("P"), list):
                return False
        raw_positions = payload.get("P")
        if not isinstance(raw_positions, list):
            return False
        for raw_position in raw_positions:
            if not isinstance(raw_position, dict) or not isinstance(raw_position.get("s"), str):
                continue
            symbol = str(raw_position["s"])
            amount = _decimal(raw_position.get("pa"))
            episode = self._active_by_symbol.get(symbol)
            ledger_amount = episode.signed_quantity if episode is not None else Decimal(0)
            self._sink.append(
                _record(
                    "POSITION_OBSERVED"
                    if amount == ledger_amount
                    else "ACCOUNT_POSITION_DISCREPANCY",
                    occurred_ms,
                    episode.episode_id if episode is not None else "",
                    {
                        "symbol": symbol,
                        "position_amount": str(amount),
                        "ledger_amount": str(ledger_amount),
                    },
                )
            )
            if amount != ledger_amount:
                return False
        return True

    def apply_order_trade_update(
        self, payload: dict[str, object], occurred_ms: int
    ) -> tuple[Episode, ...]:
        order = payload.get("o")
        if not isinstance(order, dict):
            return ()
        changed: tuple[Episode, ...] = ()
        if order.get("x") == "TRADE":
            side = order.get("S")
            symbol = order.get("s")
            if side not in {"BUY", "SELL"} or not isinstance(symbol, str):
                raise ValueError("trade update is missing symbol or side")
            trade_id = order.get("t")
            order_id = order.get("i")
            if (
                side not in {"BUY", "SELL"}
                or not isinstance(symbol, str)
                or not isinstance(trade_id, (int, str))
                or isinstance(trade_id, bool)
                or not isinstance(order_id, (int, str))
                or isinstance(order_id, bool)
            ):
                raise ValueError("trade update is missing symbol, side, or identity")
            fill = Fill(
                str(trade_id),
                str(order_id),
                symbol,
                side,
                _decimal(order.get("l")),  # type: ignore[arg-type]
                _decimal(order.get("L")),  # type: ignore[arg-type]
                occurred_ms,
            )
            changed = self.apply_fill(fill)
        status = order.get("X")
        if status in {"FILLED", "CANCELED", "EXPIRED"} and isinstance(order.get("s"), str):
            self.close_tranche(
                str(order["s"]),
                str(order.get("i")),
                status,  # type: ignore[arg-type]
                occurred_ms,
            )
        return changed

    def set_plan(self, symbol: str, invalidation: Decimal, occurred_ms: int) -> Decimal | None:
        episode = self._active_by_symbol.get(symbol)
        if episode is None or invalidation <= 0:
            raise ValueError("an open position and positive invalidation are required")
        self._sink.append(
            _record(
                "POSITION_PLAN_SET",
                occurred_ms,
                episode.episode_id,
                {"symbol": symbol, "invalidation": str(invalidation), "source": "MANUAL"},
            )
        )
        episode.invalidation = invalidation
        episode.manual_plan = True
        return self._risk_budget(episode, self._risk_reserve_rate)

    def manual_add(
        self, symbol: str, side: Side, quantity: Decimal, price: Decimal, occurred_ms: int
    ) -> Episode:
        active = self._active_by_symbol.get(symbol)
        if active is not None and active.side != side:
            raise ValueError("manual add direction must match the open position")
        sequence = active.total_fill_count + 1 if active is not None else 1
        fill = Fill(
            f"manual:{symbol}:{occurred_ms}:{sequence}",
            f"manual:{symbol}:{sequence}",
            symbol,
            "BUY" if side == "LONG" else "SELL",
            quantity,
            price,
            occurred_ms,
        )
        self.apply_fill(fill)
        episode = self._active_by_symbol[symbol]
        episode.manual_plan = True
        return episode

    def manual_close(
        self, symbol: str, quantity: Decimal, price: Decimal, occurred_ms: int
    ) -> Episode:
        active = self._active_by_symbol.get(symbol)
        if active is None or quantity <= 0 or quantity > abs(active.signed_quantity):
            raise ValueError("manual close requires an open position and valid quantity")
        fill = Fill(
            f"manual-close:{symbol}:{occurred_ms}",
            f"manual-close:{symbol}:{occurred_ms}",
            symbol,
            "SELL" if active.side == "LONG" else "BUY",
            quantity,
            price,
            occurred_ms,
        )
        self.apply_fill(fill)
        return active

    def reconcile(
        self,
        symbol: str,
        rest_amount: Decimal,
        rest_update_ms: int,
        now_ms: int,
        step_size: Decimal,
        consecutive_mismatches: int | None = None,
    ) -> Literal["MATCH", "DEFERRED", "MISMATCH", "DEGRADED"]:
        episode = self._active_by_symbol.get(symbol)
        ledger_amount = episode.signed_quantity if episode is not None else Decimal(0)
        last_trade_ms = episode.last_trade_ms if episode is not None else 0
        if rest_update_ms < last_trade_ms or now_ms - last_trade_ms < 2_000:
            return "DEFERRED"
        if abs(rest_amount - ledger_amount) <= step_size / 2:
            self._mismatch_counts.pop(symbol, None)
            return "MATCH"
        if (
            symbol not in self._mismatch_counts
            and len(self._mismatch_counts) >= self._max_open_symbols * 2
        ):
            self._mismatch_counts.pop(next(iter(self._mismatch_counts)))
        observed_mismatches = self._mismatch_counts.get(symbol, 0) + 1
        self._mismatch_counts[symbol] = observed_mismatches
        if consecutive_mismatches is not None:
            observed_mismatches = max(observed_mismatches, consecutive_mismatches)
        if observed_mismatches < 2:
            return "MISMATCH"
        return "DEGRADED"

    def last_trade_id(self, symbol: str) -> str | None:
        return self._last_trade_id.get(symbol)

    def replay_recovered_trades(self, trades: tuple[Fill, ...]) -> int:
        applied = 0
        for fill in sorted(trades, key=lambda item: (item.occurred_ms, item.trade_id)):
            if self.apply_fill(fill):
                applied += 1
        return applied

    def active_episode(self, symbol: str) -> Episode | None:
        return self._active_by_symbol.get(symbol)

    def _restore_from_records(self) -> None:
        for record in self._sink.iter_records():
            event_type = record.get("event_type")
            occurred_ms = record.get("occurred_ms")
            episode_id = record.get("episode_id")
            details = record.get("details")
            if (
                not isinstance(event_type, str)
                or not isinstance(occurred_ms, int)
                or isinstance(occurred_ms, bool)
                or not isinstance(details, dict)
            ):
                continue
            if event_type == "FILL":
                self._restore_fill(episode_id, occurred_ms, details)
            elif event_type == "CLOSE_FILL":
                self._restore_close(episode_id, occurred_ms, details)
            elif event_type == "TRANCHE_CLOSED":
                self._restore_tranche_close(episode_id, details)
            elif event_type == "POSITION_PLAN_SET":
                episode = self._episodes.get(episode_id) if isinstance(episode_id, str) else None
                invalidation = details.get("invalidation")
                if episode is not None and invalidation is not None:
                    episode.invalidation = _decimal(invalidation)
                    episode.manual_plan = True
            elif event_type == "RESYNC_ADJUSTMENT":
                self._restore_adjustment(episode_id, occurred_ms, details)

    def _restore_fill(
        self, episode_id: object, occurred_ms: int, details: dict[str, object]
    ) -> None:
        symbol = details.get("symbol")
        trade_id = details.get("trade_id")
        order_id = details.get("order_id")
        if (
            not isinstance(episode_id, str)
            or not episode_id
            or not isinstance(symbol, str)
            or not symbol
            or not isinstance(trade_id, str)
            or not trade_id
            or not isinstance(order_id, str)
            or not order_id
        ):
            return
        side = details.get("side")
        if side != "BUY" and side != "SELL":
            return
        fill = Fill(
            trade_id,
            order_id,
            symbol,
            cast(Literal["BUY", "SELL"], side),
            _decimal(details.get("quantity")),
            _decimal(details.get("price")),
            occurred_ms,
        )
        episode = self._episodes.get(episode_id)
        if episode is None:
            first_trade_id = details.get("episode_first_trade_id", trade_id)
            first_order_id = details.get("episode_first_order_id", order_id)
            if not isinstance(first_trade_id, str) or not isinstance(first_order_id, str):
                return
            self._ensure_episode_capacity(symbol)
            episode = Episode(
                episode_id,
                symbol,
                "LONG" if side == "BUY" else "SHORT",
                first_trade_id,
                first_order_id,
                occurred_ms,
                last_trade_ms=occurred_ms,
            )
            self._episodes[episode_id] = episode
            self._active_by_symbol[symbol] = episode
        tranche = episode.tranches.setdefault(order_id, Tranche(order_id))
        episode.signed_quantity += fill.quantity if side == "BUY" else -fill.quantity
        episode.fills_count += 1
        episode.last_trade_ms = occurred_ms
        tranche.quantity += fill.quantity
        tranche.notional += fill.quantity * fill.price
        tranche.fill_count += 1
        if order_id == episode.first_order_id:
            episode.initial_quantity += fill.quantity
            episode.initial_notional += fill.quantity * fill.price
        self._restore_trade_identity(symbol, trade_id)

    def _restore_close(
        self, episode_id: object, occurred_ms: int, details: dict[str, object]
    ) -> None:
        episode = self._episodes.get(episode_id) if isinstance(episode_id, str) else None
        if episode is None:
            return
        quantity = _decimal(details.get("quantity"))
        side = details.get("side")
        if quantity <= 0 or (side != "BUY" and side != "SELL"):
            return
        episode.signed_quantity += quantity if side == "BUY" else -quantity
        self._reduce_open_tranches(episode, quantity)
        episode.fills_count += 1
        episode.last_trade_ms = occurred_ms
        reason = details.get("reason")
        if episode.signed_quantity == 0:
            episode.close_reason = reason if isinstance(reason, str) else "CLOSED"
            self._active_by_symbol.pop(episode.symbol, None)
        trade_id = details.get("trade_id")
        if isinstance(trade_id, str):
            self._restore_trade_identity(episode.symbol, trade_id)

    def _restore_tranche_close(self, episode_id: object, details: dict[str, object]) -> None:
        episode = self._episodes.get(episode_id) if isinstance(episode_id, str) else None
        order_id, status = details.get("order_id"), details.get("status")
        if (
            episode is not None
            and isinstance(order_id, str)
            and status in {"FILLED", "CANCELED", "EXPIRED"}
        ):
            tranche = episode.tranches.get(order_id)
            if tranche is not None:
                tranche.terminal_status = cast(OrderTerminal, status)

    def _restore_adjustment(
        self, episode_id: object, occurred_ms: int, details: dict[str, object]
    ) -> None:
        symbol = details.get("symbol")
        target = details.get("rest_amount")
        if not isinstance(symbol, str) or target is None:
            return
        amount = _decimal(target)
        episode = self._episodes.get(episode_id) if isinstance(episode_id, str) else None
        if episode is None and amount != 0:
            new_episode_id = episode_id if isinstance(episode_id, str) else ""
            if not new_episode_id:
                return
            resync_id = details.get("resync_id")
            if not isinstance(resync_id, str):
                return
            self._ensure_episode_capacity(symbol)
            episode = Episode(
                new_episode_id,
                symbol,
                "LONG" if amount > 0 else "SHORT",
                resync_id,
                resync_id,
                occurred_ms,
                last_trade_ms=occurred_ms,
                signed_quantity=amount,
                ledger_quality="ADJUSTED",
            )
            self._episodes[new_episode_id] = episode
            self._active_by_symbol[symbol] = episode
            self._add_adjustment_tranche(episode, details)
            return
        if episode is None:
            return
        new_episode_id = details.get("new_episode_id")
        if isinstance(new_episode_id, str) and new_episode_id:
            episode.signed_quantity = Decimal(0)
            episode.close_reason = "RESYNC_SIDE_FLIP"
            self._active_by_symbol.pop(symbol, None)
            resync_id = details.get("resync_id")
            if not isinstance(resync_id, str):
                return
            side = "LONG" if amount > 0 else "SHORT"
            self._ensure_episode_capacity(symbol)
            replacement = Episode(
                new_episode_id,
                symbol,
                side,
                resync_id,
                resync_id,
                occurred_ms,
                last_trade_ms=occurred_ms,
                signed_quantity=amount,
                ledger_quality="ADJUSTED",
            )
            self._episodes[new_episode_id] = replacement
            self._active_by_symbol[symbol] = replacement
            self._add_adjustment_tranche(replacement, details)
        elif amount == 0:
            episode.signed_quantity = Decimal(0)
            episode.close_reason = "RESYNC_ZERO"
            self._active_by_symbol.pop(symbol, None)
        else:
            episode.signed_quantity = amount
            episode.ledger_quality = "ADJUSTED"
            self._add_adjustment_tranche(episode, details)
        episode.last_trade_ms = occurred_ms

    def _add_adjustment_tranche(self, episode: Episode, details: dict[str, object]) -> None:
        order_id = details.get("resync_id")
        delta = details.get("delta")
        if not isinstance(order_id, str) or delta is None:
            return
        tranche = episode.tranches.setdefault(order_id, Tranche(order_id, is_entry=False))
        quantity = details.get("adjustment_quantity", delta)
        tranche.quantity += abs(_decimal(quantity))
        tranche.fill_count += 1

    def _restore_trade_identity(self, symbol: str, trade_id: str) -> None:
        if trade_id not in self._trade_ids:
            self._remember_trade_id(trade_id)
        self._sink.remember_trade_id(trade_id)
        self._last_trade_id[symbol] = trade_id

    def _ensure_episode_capacity(self, symbol: str) -> None:
        if (
            symbol not in self._active_by_symbol
            and len(self._active_by_symbol) >= self._max_open_symbols
        ):
            raise RuntimeError("position ledger open-symbol capacity reached")
        while len(self._episodes) >= self._max_episode_cache:
            closed_id = next(
                (episode_id for episode_id, item in self._episodes.items() if item.close_reason),
                None,
            )
            if closed_id is None:
                raise RuntimeError("position ledger episode cache capacity reached")
            self._episodes.pop(closed_id)

    def _remember_trade_id(self, trade_id: str) -> None:
        self._trade_ids.add(trade_id)
        self._trade_id_order.append(trade_id)
        while len(self._trade_id_order) > self._dedupe_capacity:
            evicted = self._trade_id_order.popleft()
            self._trade_ids.remove(evicted)

    @staticmethod
    def _reduce_open_tranches(episode: Episode, quantity: Decimal) -> None:
        remaining = quantity
        for tranche in episode.tranches.values():
            if not tranche.is_entry or tranche.quantity == 0:
                continue
            cut = min(remaining, tranche.quantity)
            average = tranche.average_price
            tranche.quantity -= cut
            if average is not None:
                tranche.notional -= average * cut
            remaining -= cut
            if remaining == 0:
                return

    def recover_adjustment(
        self, symbol: str, rest_amount: Decimal, occurred_ms: int
    ) -> Episode | None:
        episode = self._active_by_symbol.get(symbol)
        if episode is None:
            if rest_amount == 0:
                return None
        delta = rest_amount - (episode.signed_quantity if episode is not None else Decimal(0))
        if delta == 0:
            return episode
        resync_id = f"resync:{symbol}:{occurred_ms}:{delta}"
        new_episode_id: str | None = None
        if episode is None:
            side: Side = "LONG" if rest_amount > 0 else "SHORT"
            new_episode_id = hashlib.sha256(
                f"{self._account_alias}|{symbol}|{side}|{resync_id}".encode()
            ).hexdigest()
        elif rest_amount != 0 and episode.signed_quantity * rest_amount < 0:
            side = "LONG" if rest_amount > 0 else "SHORT"
            new_episode_id = hashlib.sha256(
                f"{self._account_alias}|{symbol}|{side}|{resync_id}".encode()
            ).hexdigest()
        record_episode_id = new_episode_id if episode is None else episode.episode_id
        self._sink.append(
            _record(
                "RESYNC_ADJUSTMENT",
                occurred_ms,
                record_episode_id or "",
                {
                    "symbol": symbol,
                    "delta": str(delta),
                    "rest_amount": str(rest_amount),
                    "ledger_quality": "ADJUSTED",
                    "resync_id": resync_id,
                    "adjustment_quantity": str(
                        abs(rest_amount)
                        if episode is None or new_episode_id is not None
                        else abs(delta)
                    ),
                    "new_episode_id": new_episode_id
                    if episode is not None and new_episode_id is not None
                    else None,
                },
            )
        )
        if episode is None:
            self._ensure_episode_capacity(symbol)
            side = "LONG" if rest_amount > 0 else "SHORT"
            episode = Episode(
                record_episode_id or "",
                symbol,
                side,
                resync_id,
                resync_id,
                occurred_ms,
                last_trade_ms=occurred_ms,
                signed_quantity=rest_amount,
                ledger_quality="ADJUSTED",
            )
            self._episodes[episode.episode_id] = episode
            self._active_by_symbol[symbol] = episode
            self._add_adjustment_tranche(
                episode,
                {
                    "resync_id": resync_id,
                    "delta": str(delta),
                    "adjustment_quantity": str(abs(rest_amount)),
                },
            )
            return episode
        if new_episode_id is not None:
            episode.signed_quantity = Decimal(0)
            episode.close_reason = "RESYNC_SIDE_FLIP"
            self._active_by_symbol.pop(symbol, None)
            self._ensure_episode_capacity(symbol)
            side = "LONG" if rest_amount > 0 else "SHORT"
            replacement = Episode(
                new_episode_id,
                symbol,
                side,
                resync_id,
                resync_id,
                occurred_ms,
                last_trade_ms=occurred_ms,
                signed_quantity=rest_amount,
                ledger_quality="ADJUSTED",
            )
            self._episodes[new_episode_id] = replacement
            self._active_by_symbol[symbol] = replacement
            self._add_adjustment_tranche(
                replacement,
                {
                    "resync_id": resync_id,
                    "delta": str(delta),
                    "adjustment_quantity": str(abs(rest_amount)),
                },
            )
            return replacement
        episode.last_trade_ms = occurred_ms
        episode.signed_quantity = rest_amount
        episode.ledger_quality = "ADJUSTED"
        if rest_amount == 0:
            episode.close_reason = "RESYNC_ZERO"
            self._active_by_symbol.pop(symbol, None)
        else:
            self._add_adjustment_tranche(episode, {"resync_id": resync_id, "delta": str(delta)})
        return episode

    def _open_episode(self, symbol: str, signed_quantity: Decimal, fill: Fill) -> Episode:
        self._ensure_episode_capacity(symbol)
        side: Side = "LONG" if signed_quantity > 0 else "SHORT"
        episode_id = hashlib.sha256(
            f"{self._account_alias}|{symbol}|{side}|{fill.trade_id}".encode()
        ).hexdigest()
        episode = Episode(
            episode_id,
            symbol,
            side,
            fill.trade_id,
            fill.order_id,
            fill.occurred_ms,
            last_trade_ms=fill.occurred_ms,
        )
        self._episodes[episode_id] = episode
        self._active_by_symbol[symbol] = episode
        return episode

    def _apply_piece(
        self, episode: Episode, fill: Fill, quantity: Decimal, *, trade_id: str | None = None
    ) -> None:
        signed = quantity if fill.side == "BUY" else -quantity
        tranche = episode.tranches.get(fill.order_id)
        if tranche is None:
            if len(episode.tranches) >= 4096:
                raise RuntimeError("position episode tranche capacity reached")
            tranche = Tranche(fill.order_id)
            episode.tranches[fill.order_id] = tranche
        self._sink.append(
            _record(
                "FILL",
                fill.occurred_ms,
                episode.episode_id,
                {
                    "symbol": fill.symbol,
                    "trade_id": fill.trade_id,
                    "leg_id": trade_id or fill.trade_id,
                    "order_id": fill.order_id,
                    "side": fill.side,
                    "quantity": str(quantity),
                    "price": str(fill.price),
                    "episode_first_trade_id": episode.first_trade_id,
                    "episode_first_order_id": episode.first_order_id,
                },
            )
        )
        episode.signed_quantity += signed
        episode.fills_count += 1
        episode.last_trade_ms = fill.occurred_ms
        tranche.quantity += quantity
        tranche.notional += quantity * fill.price
        tranche.fill_count += 1
        if fill.order_id == episode.first_order_id:
            episode.initial_quantity += quantity
            episode.initial_notional += quantity * fill.price

    def _emit_ledger_event(
        self, event_type: str, fill: Fill, episode: Episode, quantity: Decimal, reason: str
    ) -> None:
        self._sink.append(
            _record(
                event_type,
                fill.occurred_ms,
                episode.episode_id,
                {
                    "symbol": fill.symbol,
                    "quantity": str(quantity),
                    "reason": reason,
                    "trade_id": fill.trade_id,
                    "order_id": fill.order_id,
                    "side": fill.side,
                    "price": str(fill.price),
                },
            )
        )

    @staticmethod
    def _risk_budget(episode: Episode, reserve_rate: Decimal) -> Decimal | None:
        if episode.invalidation is None or episode.p0 is None:
            return None
        return (
            Decimal(2)
            * episode.q0
            * (abs(episode.invalidation - episode.p0) + reserve_rate * episode.p0)
        )


def _validate_fill(fill: Fill) -> None:
    if not fill.trade_id or not fill.order_id or not fill.symbol:
        raise ValueError("fill identities must be non-empty")
    if fill.quantity <= 0 or fill.price <= 0 or fill.occurred_ms < 0:
        raise ValueError("fill values must be positive and timestamp non-negative")


def derive_invalidation(
    candles: tuple[ClosedCandle, ...],
    *,
    event_start_ms: int,
    first_fill_ms: int,
    multiplier: Decimal,
) -> Decimal | None:
    if multiplier <= 0 or event_start_ms < 0 or first_fill_ms < event_start_ms:
        raise ValueError("invalidation window and multiplier are invalid")
    eligible = [
        candle.high
        for candle in candles
        if event_start_ms <= candle.close_ms < first_fill_ms and candle.high > 0
    ]
    return max(eligible) * (Decimal(1) + multiplier) if eligible else None


def estimate_clock_offset_ms(
    server_time_ms: int, request_start_ms: int, response_end_ms: int
) -> int:
    if (
        min(server_time_ms, request_start_ms, response_end_ms) < 0
        or response_end_ms < request_start_ms
    ):
        raise ValueError("clock samples must be non-negative and ordered")
    midpoint = request_start_ms + (response_end_ms - request_start_ms) // 2
    return server_time_ms - midpoint


def missing_position_amount(
    snapshots: tuple[object, ...], symbol: str, *, amount_attribute: str = "position_amount"
) -> Decimal:
    for snapshot in snapshots:
        if getattr(snapshot, "symbol", None) == symbol:
            return _decimal(getattr(snapshot, amount_attribute))
    return Decimal(0)


def position_poll_interval_ms(has_position: bool) -> int:
    return 10_000 if has_position else 60_000


def position_resync_delay_ms(
    event_type: str, *, now_ms: int, last_request_ms: int | None
) -> int | None:
    if event_type == "MARGIN_CALL":
        return 0
    if event_type not in {"ACCOUNT_UPDATE", "ORDER_TRADE_UPDATE", "ACCOUNT_CONFIG_UPDATE"}:
        return None
    minimum_gap = 3_000 if event_type == "ACCOUNT_CONFIG_UPDATE" else 1_000
    if last_request_ms is None:
        return minimum_gap
    return max(0, minimum_gap - (now_ms - last_request_ms))


def recover_fenced_trades(
    buffer: BoundedUserEventBuffer,
    session: RestResyncSession,
    ledger: PositionLedger,
    trades: tuple[Fill, ...],
) -> tuple[tuple[str, ...], int]:
    fenced_event_ids = buffer.fence_buffered_events(session)
    return fenced_event_ids, ledger.replay_recovered_trades(trades)


async def recover_user_stream_gap(
    buffer: BoundedUserEventBuffer,
    ledger: PositionLedger,
    source: PositionRecoverySource,
    symbol: str,
    *,
    now_ms: Callable[[], int],
    step_size: Decimal,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> RecoveryResult:
    """Retry fenced REST snapshots, replay user trades, and reconcile a stream gap."""
    fenced: list[str] = []
    for _attempt, delay in enumerate((0.0, *RESYNC_BACKOFF_SECONDS)):
        if delay:
            await sleep(delay)
        session = buffer.begin_rest_resync()
        try:
            snapshot = await source.position_risk(symbol)
            completed = buffer.complete_rest_resync(
                session,
                snapshot_id=snapshot.snapshot_id,
                snapshot_cursor=snapshot.cursor,
                defer_health_recovery=True,
            )
        except Exception:
            fenced.extend(buffer.fence_buffered_events(session))
            raise
        if completed.status != "CERTAIN":
            fenced.extend(buffer.fence_buffered_events(session))
            continue
        try:
            trades = await source.user_trades(symbol, ledger.last_trade_id(symbol))
            replayed = ledger.replay_recovered_trades(trades)
            reconciliation = ledger.reconcile(
                symbol,
                snapshot.position_amount,
                snapshot.update_ms,
                now_ms(),
                step_size,
                consecutive_mismatches=2,
            )
        except Exception:
            fenced.extend(buffer.fence_buffered_events(session))
            raise
        if reconciliation == "DEFERRED":
            fenced.extend(buffer.fence_buffered_events(session))
            continue
        if reconciliation != "MATCH":
            stable, crossed = buffer.validate_rest_resync(session)
            fenced.extend(crossed)
            if not stable:
                continue
            try:
                adjusted = ledger.recover_adjustment(
                    symbol, snapshot.position_amount, now_ms()
                )
            except Exception:
                fenced.extend(buffer.fence_buffered_events(session))
                raise
            if adjusted is None:
                fenced.extend(buffer.fence_buffered_events(session))
                continue
            recovered, crossed = buffer.finalize_rest_resync(session)
            fenced.extend(crossed)
            if recovered:
                return RecoveryResult("ADJUSTED", tuple(fenced), replayed)
            continue
        recovered, crossed = buffer.finalize_rest_resync(session)
        fenced.extend(crossed)
        if recovered:
            return RecoveryResult("CERTAIN", tuple(fenced), replayed)
    return RecoveryResult("DEGRADED", tuple(fenced), 0)


async def replace_user_stream_make_before_break[T](
    open_replacement: Callable[[], Awaitable[T]],
    overlap_and_reconcile: Callable[[T], Awaitable[None]],
    close_previous: Callable[[], Awaitable[None]],
) -> T:
    """Open and reconcile a replacement stream before closing the old stream."""
    replacement = await open_replacement()
    await overlap_and_reconcile(replacement)
    await close_previous()
    return replacement


async def backfill_closed_candles(
    source: KlineBackfillSource,
    symbol: str,
    expected_close_times_ms: tuple[int, ...],
) -> tuple[ClosedCandle, ...] | None:
    if not expected_close_times_ms:
        return ()
    start_ms = min(expected_close_times_ms)
    end_ms = max(expected_close_times_ms)
    ordinary = await source.klines(symbol, start_ms, end_ms)
    mark = await source.mark_price_klines(symbol, start_ms, end_ms)
    ordinary_times = {candle.close_ms for candle in ordinary}
    mark_by_time = {candle.close_ms: candle for candle in mark}
    if any(
        close_time not in ordinary_times or close_time not in mark_by_time
        for close_time in expected_close_times_ms
    ):
        return None
    return tuple(mark_by_time[close_time] for close_time in sorted(expected_close_times_ms))


def _decimal(value: object) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("position amount must be decimal") from exc
    if not parsed.is_finite():
        raise ValueError("position amount must be finite")
    return parsed


def _record(
    event_type: str, occurred_ms: int, episode_id: str, details: dict[str, object]
) -> dict[str, object]:
    content: dict[str, object] = {
        "event_type": event_type,
        "occurred_ms": occurred_ms,
        "episode_id": episode_id,
        "details": details,
    }
    content["record_id"] = hashlib.sha256(_canonical_json(content)).hexdigest()
    return content


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
