from __future__ import annotations

import ast
import inspect
import json
import re
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from pydantic import SecretStr

from position_guardian.exchange.binance_user_stream import (
    BoundedUserEventBuffer,
    parse_user_stream_event,
)
from signalbot.pump_fade_v3.listen_key import LISTEN_KEY_PATH, ListenKeyClient
from signalbot.pump_fade_v3.position_tracker import (
    ClosedCandle,
    Episode,
    Fill,
    PositionLedger,
    PositionStore,
    RecoverySnapshot,
    backfill_closed_candles,
    derive_invalidation,
    estimate_clock_offset_ms,
    position_poll_interval_ms,
    position_resync_delay_ms,
    recover_fenced_trades,
    recover_user_stream_gap,
    replace_user_stream_make_before_break,
)
from signalbot.pump_fade_v3.tracker_config import load_position_tracker_config
from signalbot.pump_fade_v3.warnings import (
    AddBlockInputs,
    ThesisEvidence,
    ThesisInputs,
    WarningEngine,
    WarningPolicy,
    evaluate_available_thesis,
    load_warning_policy,
)

ROOT = Path(__file__).parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "pump_fade_v3"


class MemorySink:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []
        self.trade_ids: set[tuple[str, str]] = set()

    def append(self, record: dict[str, object]) -> bool:
        self.records.append(record)
        return True

    def contains_trade_id(self, symbol: str, trade_id: str) -> bool:
        return (symbol, trade_id) in self.trade_ids

    def remember_trade_id(self, symbol: str, trade_id: str) -> None:
        self.trade_ids.add((symbol, trade_id))

    def iter_records(self) -> tuple[dict[str, object], ...]:
        return tuple(self.records)


def _fill(
    trade_id: str,
    order_id: str,
    side: str,
    quantity: str,
    price: str,
    occurred_ms: int = 1_700_000_000_000,
) -> Fill:
    return Fill(
        trade_id,
        order_id,
        "SYNTHUSDT",
        side,  # type: ignore[arg-type]
        Decimal(quantity),
        Decimal(price),
        occurred_ms,
    )


def _short_episode(
    sink: MemorySink | PositionStore | None = None,
) -> tuple[PositionLedger, Episode]:
    target = sink or MemorySink()
    ledger = PositionLedger("synthetic-account", target)
    episode = ledger.apply_fill(_fill("1", "10", "SELL", "1", "100"))[0]
    return ledger, episode


def _policy() -> WarningPolicy:
    return WarningPolicy(
        policy_hash="synthetic-policy-hash",
        liq_thresholds_percent=tuple(map(Decimal, ("33", "25", "20", "14"))),
        max_adds=2,
        legacy_adds=8,
        max_add_fraction_q0=Decimal("0.5"),
        legacy_spacing_percent=Decimal("0.05"),
        reserve_rate=Decimal("0.003"),
    )


def _block_input(price: str, candle_ms: int) -> AddBlockInputs:
    return AddBlockInputs(
        Decimal(price), candle_ms, "UNAVAILABLE", False, Decimal("0.5"), Decimal("40")
    )


def _warning_engine(sink: MemorySink) -> WarningEngine:
    engine = WarningEngine(_policy(), sink)
    assert engine.observe_account_config(
        {"dualSidePosition": False, "multiAssetsMargin": False},
        1_700_000_000_000,
        portfolio_margin=False,
    )
    return engine


def test_partial_fills_share_a_tranche_and_trade_ids_are_deduplicated() -> None:
    sink = MemorySink()
    ledger = PositionLedger("synthetic-account", sink)
    first = _fill("1", "10", "SELL", "0.4", "100")
    second = _fill("2", "10", "SELL", "0.6", "110", first.occurred_ms + 1)
    episode = ledger.apply_fill(first)[0]
    assert ledger.apply_fill(first) == ()
    assert ledger.apply_fill(second) == (episode,)
    assert episode.q0 == Decimal("1.0")
    assert episode.p0 == Decimal("106")
    assert episode.tranches["10"].fill_count == 2
    ledger.close_tranche("SYNTHUSDT", "10", "CANCELED", second.occurred_ms + 1)
    assert episode.tranches["10"].terminal_status == "CANCELED"


def test_same_trade_id_on_distinct_symbols_is_not_deduplicated(tmp_path: Path) -> None:
    store = PositionStore(tmp_path / "positions.sqlite", tmp_path / "daily")
    ledger = PositionLedger("synthetic-account", store)
    first = Fill("42", "order-a", "AAAUSDT", "SELL", Decimal("1"), Decimal("10"), 1000)
    second = Fill("42", "order-b", "BBBUSDT", "BUY", Decimal("2"), Decimal("20"), 1001)

    first_episode = ledger.apply_fill(first)[0]
    second_episode = ledger.apply_fill(second)[0]

    assert first_episode.symbol == "AAAUSDT" and first_episode.signed_quantity == Decimal("-1")
    assert second_episode.symbol == "BBBUSDT" and second_episode.signed_quantity == Decimal("2")
    assert ledger.apply_fill(first) == ()
    with sqlite3.connect(tmp_path / "positions.sqlite") as connection:
        rows = connection.execute(
            "SELECT symbol, trade_id FROM shadow_trade_ids ORDER BY symbol"
        ).fetchall()
    assert rows == [("AAAUSDT", "42"), ("BBBUSDT", "42")]


def test_legacy_trade_ids_migrate_with_symbol_from_shadow_records(tmp_path: Path) -> None:
    database = tmp_path / "positions.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE shadow_records "
            "(record_id TEXT PRIMARY KEY, occurred_ms INTEGER NOT NULL, payload BLOB NOT NULL)"
        )
        connection.execute("CREATE TABLE shadow_trade_ids (trade_id TEXT PRIMARY KEY)")
        connection.execute("INSERT INTO shadow_trade_ids(trade_id) VALUES ('42')")
        record = {
            "record_id": "old-fill",
            "occurred_ms": 1000,
            "details": {"symbol": "AAAUSDT", "trade_id": "42"},
        }
        connection.execute(
            "INSERT INTO shadow_records(record_id, occurred_ms, payload) VALUES (?, ?, ?)",
            ("old-fill", 1000, json.dumps(record).encode()),
        )

    store = PositionStore(database, tmp_path / "daily")

    assert store.contains_trade_id("AAAUSDT", "42")
    assert not store.contains_trade_id("BBBUSDT", "42")


@pytest.mark.parametrize("chunks", [("15",), ("8", "7")])
def test_side_flip_splits_close_and_new_episode_open(chunks: tuple[str, ...]) -> None:
    sink = MemorySink()
    ledger = PositionLedger("synthetic-account", sink)
    old = ledger.apply_fill(_fill("1", "short", "SELL", "10", "100"))[0]
    when = 1_700_000_001_000
    created = ()
    for index, chunk in enumerate(chunks):
        created = ledger.apply_fill(
            _fill(str(index + 2), f"buy-{index}", "BUY", chunk, "110", when + index)
        )
    new = next(episode for episode in created if episode.side == "LONG")
    assert new.q0 == Decimal("5")
    assert new.signed_quantity == Decimal("5")
    assert old.close_reason == "CLOSED_BY_FLIP"
    assert old.signed_quantity == 0


def test_account_funding_updates_do_not_change_position_quantity() -> None:
    ledger, episode = _short_episode()
    before = episode.signed_quantity
    cross = json.loads((FIXTURES / "account_update_cross_funding.json").read_text())
    assert ledger.apply_account_update(cast(dict[str, object], cross), 1_700_000_001_000) is False
    isolated = {"a": {"m": "FUNDING_FEE", "P": [{"s": "SYNTHUSDT", "pa": "-1"}]}}
    assert ledger.apply_account_update(cast(dict[str, object], isolated), 1_700_000_002_000) is True
    assert episode.signed_quantity == before
    assert sum(record["event_type"] == "FUNDING_OBSERVED" for record in ledger._sink.records) == 2  # type: ignore[attr-defined]


def test_manual_plan_uses_first_filled_quantity_and_risk_equation() -> None:
    ledger, _episode = _short_episode()
    episode = ledger.manual_add(
        "SYNTHUSDT", "SHORT", Decimal("1"), Decimal("100"), 1_700_000_001_000
    )
    budget = ledger.set_plan("SYNTHUSDT", Decimal("120"), 1_700_000_002_000)
    assert episode.q0 == Decimal("1")
    assert budget == Decimal("40.600")
    assert episode.manual_plan is True


def test_invalidation_uses_only_closed_event_candles_before_first_fill() -> None:
    candles = (
        ClosedCandle(Decimal("100"), 10_000),
        ClosedCandle(Decimal("120"), 19_999),
        ClosedCandle(Decimal("1000"), 20_000),
    )
    assert derive_invalidation(
        candles,
        event_start_ms=10_000,
        first_fill_ms=20_000,
        multiplier=Decimal("0.15"),
    ) == Decimal("138.00")
    assert (
        derive_invalidation(
            (), event_start_ms=10_000, first_fill_ms=20_000, multiplier=Decimal("0.15")
        )
        is None
    )


def test_rest_missing_symbol_is_zero_and_mismatch_needs_two_observations() -> None:
    ledger, _episode = _short_episode()
    assert ledger.reconcile("EMPTYUSDT", Decimal(0), 2_000, 3_000, Decimal("0.1"), 1) == "MATCH"
    assert (
        ledger.reconcile(
            "SYNTHUSDT", Decimal(-2), 1_700_000_000_000, 1_700_000_003_000, Decimal("0.1"), 1
        )
        == "MISMATCH"
    )
    assert (
        ledger.reconcile(
            "SYNTHUSDT", Decimal(-2), 1_700_000_000_000, 1_700_000_003_000, Decimal("0.1"), 2
        )
        == "DEGRADED"
    )


def test_rest_race_is_deferred_then_resync_adjustment_is_recorded() -> None:
    sink = MemorySink()
    ledger, episode = _short_episode(sink)
    assert (
        ledger.reconcile("SYNTHUSDT", Decimal(-2), 1, 1_700_000_000_500, Decimal("0.1"), 2)
        == "DEFERRED"
    )
    adjusted = ledger.recover_adjustment("SYNTHUSDT", Decimal(-2), 1_700_000_003_000)
    assert adjusted is not None
    assert adjusted is episode
    assert adjusted.ledger_quality == "ADJUSTED"
    assert sink.records[-1]["event_type"] == "RESYNC_ADJUSTMENT"


def test_resync_zero_and_sign_flip_replace_active_episode() -> None:
    sink = MemorySink()
    ledger, original = _short_episode(sink)
    assert ledger.recover_adjustment("SYNTHUSDT", Decimal(0), 1_700_000_003_000) is original
    assert ledger.active_episode("SYNTHUSDT") is None
    assert original.signed_quantity == 0 and original.close_reason == "RESYNC_ZERO"

    ledger.apply_fill(_fill("2", "11", "SELL", "1", "100", 1_700_000_004_000))
    short = ledger.active_episode("SYNTHUSDT")
    assert short is not None and short.side == "SHORT"
    replacement = ledger.recover_adjustment("SYNTHUSDT", Decimal("0.25"), 1_700_000_007_000)
    assert replacement is not None and replacement.side == "LONG"
    assert replacement is not short
    assert short.close_reason == "RESYNC_SIDE_FLIP" and short.signed_quantity == 0
    assert replacement.signed_quantity == Decimal("0.25")
    assert next(iter(replacement.tranches.values())).quantity == Decimal("0.25")


def test_restart_replay_has_stable_bytes_and_no_duplicate_records(tmp_path: Path) -> None:
    database = tmp_path / "positions.sqlite"
    store = PositionStore(database, tmp_path / "daily")
    ledger = PositionLedger("synthetic-account", store)
    ledger.apply_fill(_fill("1", "10", "SELL", "1", "100"))
    first_line = (tmp_path / "daily" / "2023-11-14.jsonl").read_bytes().splitlines()[0]
    restarted = PositionStore(database, tmp_path / "daily")
    replay = PositionLedger("synthetic-account", restarted)
    restored = replay.active_episode("SYNTHUSDT")
    assert restored is not None
    assert restored.signed_quantity == Decimal("-1")
    assert restored.tranches["10"].average_price == Decimal("100")
    assert replay.last_trade_id("SYNTHUSDT") == "1"
    replay.apply_fill(_fill("1", "10", "SELL", "1", "100"))
    lines = (tmp_path / "daily" / "2023-11-14.jsonl").read_bytes().splitlines()
    with sqlite3.connect(database) as connection:
        count = connection.execute("SELECT COUNT(*) FROM shadow_records").fetchone()[0]
    assert count == 1
    assert lines == [first_line]


def test_same_time_close_fills_keep_distinct_durable_ids(tmp_path: Path) -> None:
    store = PositionStore(tmp_path / "positions.sqlite", tmp_path / "daily")
    ledger = PositionLedger("synthetic-account", store)
    ledger.apply_fill(_fill("entry", "entry-order", "BUY", "2", "100"))
    ledger.apply_fill(_fill("close-a", "close-order-a", "SELL", "0.5", "101"))
    ledger.apply_fill(_fill("close-b", "close-order-b", "SELL", "0.5", "101"))
    closes = [record for record in store.iter_records() if record["event_type"] == "CLOSE_FILL"]
    assert len(closes) == 2
    assert {record["details"]["trade_id"] for record in closes} == {"close-a", "close-b"}  # type: ignore[index]


def test_duplicate_store_append_uses_sqlite_without_reading_jsonl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = PositionStore(tmp_path / "positions.sqlite", tmp_path / "daily")
    record: dict[str, object] = {"record_id": "synthetic-record", "occurred_ms": 1_700_000_000_000}
    assert store.append(record)
    path = tmp_path / "daily" / "2023-11-14.jsonl"
    original_open = Path.open
    modes: list[str] = []

    def tracked_open(
        self: Path, mode: str = "r", *args: Any, **kwargs: Any
    ) -> Any:
        modes.append(mode)
        return original_open(self, mode, *args, **kwargs)

    with monkeypatch.context() as patcher:
        patcher.setattr(Path, "open", cast(Any, tracked_open))
        assert not store.append(record)
    assert modes == []
    assert path.read_bytes().count(b"\n") == 1


def test_api_key_never_enters_shadow_records(tmp_path: Path) -> None:
    store = PositionStore(tmp_path / "positions.sqlite", tmp_path / "daily")
    ledger = PositionLedger("synthetic-account", store)
    ledger.apply_fill(_fill("1", "10", "SELL", "1", "100"))
    all_record_bytes = b"".join(path.read_bytes() for path in (tmp_path / "daily").glob("*.jsonl"))
    assert b"test-key" not in all_record_bytes
    assert b"api_key" not in all_record_bytes


def test_stream_fence_flush_and_trade_replay_deduplicate_make_before_break() -> None:
    buffer = BoundedUserEventBuffer()
    event = parse_user_stream_event('{"e":"ACCOUNT_UPDATE","E":10,"a":{"m":"ORDER"}}')
    buffer.ingest(event)
    session = buffer.begin_rest_resync()
    assert (
        buffer.complete_rest_resync(session, snapshot_id="snap", snapshot_cursor="10").status
        == "DEGRADED"
    )
    ledger, _episode = _short_episode()
    fill = _fill("2", "11", "SELL", "1", "100")
    replayed_ids, replayed_count = recover_fenced_trades(buffer, session, ledger, (fill,))
    assert replayed_ids == (event.event_id,)
    assert replayed_count == 1
    assert ledger.apply_fill(fill) == ()
    next_session = buffer.begin_rest_resync()
    result = buffer.complete_rest_resync(next_session, snapshot_id="snap2", snapshot_cursor="11")
    assert result.status == "CERTAIN"


@pytest.mark.asyncio
async def test_gap_recovery_retries_rest_then_fetches_trades_from_cursor() -> None:
    class Source:
        def __init__(self) -> None:
            self.snapshot_calls = 0
            self.trade_cursors: list[str | None] = []
            self.trade_calls = 0

        async def position_risk(self, symbol: str) -> RecoverySnapshot:
            self.snapshot_calls += 1
            if self.snapshot_calls == 1:
                pending = parse_user_stream_event(
                    '{"e":"ACCOUNT_UPDATE","E":10,"a":{"m":"ORDER"}}'
                )
                buffer.ingest(pending)
            return RecoverySnapshot(
                Decimal("-1"),
                1_700_000_100_000,
                f"snap-{self.snapshot_calls}",
                str(self.snapshot_calls),
            )

        async def user_trades(
            self, symbol: str, from_trade_id: str | None
        ) -> tuple[Fill, ...]:
            self.trade_cursors.append(from_trade_id)
            self.trade_calls += 1
            if self.trade_calls == 1:
                pending = parse_user_stream_event(
                    '{"e":"ACCOUNT_UPDATE","E":11,"a":{"m":"ORDER"}}'
                )
                buffer.ingest(pending)
            return (_fill("gap-fill", "gap-order", "SELL", "0.5", "100"),)

    buffer = BoundedUserEventBuffer()
    ledger = PositionLedger("synthetic-account", MemorySink())
    ledger.apply_fill(_fill("cursor-0", "seed-order", "SELL", "0.5", "100"))
    source = Source()
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    result = await recover_user_stream_gap(
        buffer,
        ledger,
        source,
        "SYNTHUSDT",
        now_ms=lambda: 1_700_000_200_000,
        step_size=Decimal("0.1"),
        sleep=sleep,
    )
    assert result.status == "CERTAIN"
    assert result.fenced_event_ids
    assert source.snapshot_calls == 3
    assert source.trade_cursors == ["cursor-0", "gap-fill"]
    assert delays == [1.0, 2.0]
    assert ledger.active_episode("SYNTHUSDT") is not None
    assert ledger.active_episode("SYNTHUSDT").signed_quantity == Decimal("-1")  # type: ignore[union-attr]
    assert ledger.last_trade_id("SYNTHUSDT") == "gap-fill"
    assert buffer.health == "HEALTHY"


@pytest.mark.asyncio
async def test_missing_trade_cursor_skips_user_trades_and_uses_rest_adjustment() -> None:
    class Source:
        def __init__(self) -> None:
            self.trade_calls = 0

        async def position_risk(self, symbol: str) -> RecoverySnapshot:
            return RecoverySnapshot(Decimal("-1"), 1_700_000_100_000, "snap", "cursor")

        async def user_trades(
            self, symbol: str, from_trade_id: str | None
        ) -> tuple[Fill, ...]:
            self.trade_calls += 1
            return (_fill("old-close", "old-order", "BUY", "1", "100"),)

    source = Source()
    sink = MemorySink()
    ledger = PositionLedger("synthetic-account", sink)
    result = await recover_user_stream_gap(
        BoundedUserEventBuffer(),
        ledger,
        source,
        "SYNTHUSDT",
        now_ms=lambda: 1_700_000_200_000,
        step_size=Decimal("0.1"),
    )
    assert result.status == "ADJUSTED" and result.replayed_trades == 0
    assert source.trade_calls == 0
    assert ledger.active_episode("SYNTHUSDT") is not None
    assert ledger.active_episode("SYNTHUSDT").signed_quantity == Decimal("-1")  # type: ignore[union-attr]
    assert sink.records[0]["event_type"] == "RESYNC_ADJUSTMENT"


@pytest.mark.asyncio
async def test_user_trades_page_limit_marks_recovery_adjusted() -> None:
    class Source:
        async def position_risk(self, symbol: str) -> RecoverySnapshot:
            return RecoverySnapshot(Decimal("-1"), 1_700_000_100_000, "snap", "cursor")

        async def user_trades(
            self, symbol: str, from_trade_id: str | None
        ) -> tuple[Fill, ...]:
            return tuple(
                _fill(str(index), f"order-{index}", "SELL", "0.0018", "100")
                for index in range(2, 502)
            )

    sink = MemorySink()
    ledger = PositionLedger("synthetic-account", sink)
    ledger.apply_fill(_fill("1", "seed", "SELL", "0.1", "100"))
    result = await recover_user_stream_gap(
        BoundedUserEventBuffer(),
        ledger,
        Source(),
        "SYNTHUSDT",
        now_ms=lambda: 1_700_000_200_000,
        step_size=Decimal("0.001"),
    )
    episode = ledger.active_episode("SYNTHUSDT")
    assert result.status == "ADJUSTED" and result.replayed_trades == 500
    assert episode is not None and episode.ledger_quality == "ADJUSTED"
    assert any(
        isinstance(details := record.get("details"), dict)
        and details.get("reason") == "USER_TRADES_PAGE_LIMIT_REACHED"
        for record in sink.records
    )


@pytest.mark.asyncio
async def test_stream_replacement_overlaps_before_closing_old_stream() -> None:
    calls: list[str] = []

    async def open_replacement() -> str:
        calls.append("open")
        return "new-stream"

    async def overlap(stream: str) -> None:
        calls.append(f"overlap:{stream}")

    async def close_old() -> None:
        calls.append("close-old")

    result = await replace_user_stream_make_before_break(open_replacement, overlap, close_old)
    assert result == "new-stream"
    assert calls == ["open", "overlap:new-stream", "close-old"]


@pytest.mark.asyncio
async def test_trade_recovery_failure_leaves_stream_degraded() -> None:
    class Source:
        async def position_risk(self, symbol: str) -> RecoverySnapshot:
            return RecoverySnapshot(Decimal(0), 1_700_000_100_000, "snap", "cursor")

        async def user_trades(
            self, symbol: str, from_trade_id: str | None
        ) -> tuple[Fill, ...]:
            raise RuntimeError("synthetic recovery failure")

    buffer = BoundedUserEventBuffer()
    ledger = PositionLedger("synthetic-account", MemorySink())
    ledger.apply_fill(_fill("seed-trade", "seed-order", "SELL", "0.5", "100"))
    with pytest.raises(RuntimeError, match="synthetic recovery failure"):
        await recover_user_stream_gap(
            buffer,
            ledger,
            Source(),
            "SYNTHUSDT",
            now_ms=lambda: 1_700_000_200_000,
            step_size=Decimal("0.1"),
        )
    assert buffer.health == "DEGRADED"


@pytest.mark.asyncio
async def test_stale_snapshot_does_not_adjust_ledger_after_stream_crossing() -> None:
    class Source:
        def __init__(self) -> None:
            self.snapshot_calls = 0
            self.trade_calls = 0

        async def position_risk(self, symbol: str) -> RecoverySnapshot:
            self.snapshot_calls += 1
            return RecoverySnapshot(
                Decimal("-2"),
                1_700_000_100_000,
                f"snapshot-{self.snapshot_calls}",
                str(self.snapshot_calls),
            )

        async def user_trades(
            self, symbol: str, from_trade_id: str | None
        ) -> tuple[Fill, ...]:
            self.trade_calls += 1
            if self.trade_calls == 1:
                buffer.ingest(
                    parse_user_stream_event(
                        '{"e":"ACCOUNT_UPDATE","E":12,"a":{"m":"ORDER"}}'
                    )
                )
            return ()

    buffer = BoundedUserEventBuffer()
    sink = MemorySink()
    ledger = PositionLedger("synthetic-account", sink)
    ledger.apply_fill(_fill("seed-trade", "seed-order", "SELL", "0.5", "100"))
    result = await recover_user_stream_gap(
        buffer,
        ledger,
        Source(),
        "SYNTHUSDT",
        now_ms=lambda: 1_700_000_200_000,
        step_size=Decimal("0.1"),
    )
    assert result.status == "ADJUSTED"
    assert result.fenced_event_ids
    adjustments = [
        record for record in sink.records if record["event_type"] == "RESYNC_ADJUSTMENT"
    ]
    assert len(adjustments) == 1
    assert ledger.active_episode("SYNTHUSDT") is not None
    assert ledger.active_episode("SYNTHUSDT").signed_quantity == Decimal("-2")  # type: ignore[union-attr]
    assert buffer.health == "HEALTHY"


def test_refresh_debounce_and_periods_match_the_read_budget() -> None:
    assert position_poll_interval_ms(True) == 10_000
    assert position_poll_interval_ms(False) == 60_000
    assert position_resync_delay_ms("MARGIN_CALL", now_ms=10_000, last_request_ms=9_000) == 0
    assert (
        position_resync_delay_ms("ACCOUNT_CONFIG_UPDATE", now_ms=10_000, last_request_ms=8_000)
        == 1_000
    )
    assert (
        position_resync_delay_ms("ORDER_TRADE_UPDATE", now_ms=10_000, last_request_ms=9_500) == 500
    )


@pytest.mark.asyncio
async def test_missing_kline_backfill_requires_both_rest_sources() -> None:
    class Source:
        async def klines(
            self, symbol: str, start_ms: int, end_ms: int
        ) -> tuple[ClosedCandle, ...]:
            _ = (symbol, start_ms, end_ms)
            return (ClosedCandle(Decimal("110"), 300_000),)

        async def mark_price_klines(
            self, symbol: str, start_ms: int, end_ms: int
        ) -> tuple[ClosedCandle, ...]:
            _ = (symbol, start_ms, end_ms)
            return (ClosedCandle(Decimal("111"), 300_000),)

    candles = await backfill_closed_candles(Source(), "SYNTHUSDT", (300_000,))
    assert candles == (ClosedCandle(Decimal("111"), 300_000),)
    assert await backfill_closed_candles(Source(), "SYNTHUSDT", (300_000, 600_000)) is None


def test_thesis_requires_t1_or_validated_group_plus_distinct_group() -> None:
    _ledger, episode = _short_episode()
    engine = _warning_engine(MemorySink())
    first = engine.thesis_broken(episode, ThesisInputs(frozenset({"T1"}), {"high": "120"}), 1_000)
    assert first is not None
    assert first["details"]["validated_group_count"] == 1  # type: ignore[index]
    assert first["details"]["l8_a3_qualifies"] is True  # type: ignore[index]
    assert engine.thesis_broken(episode, ThesisInputs(frozenset({"T1", "T2"}), {}), 2_000) is None
    mixed = engine.thesis_broken(episode, ThesisInputs(frozenset({"T1", "T4"}), {}), 3_000)
    assert mixed is not None
    assert mixed["details"]["validated_group_count"] == 2  # type: ignore[index]
    assert mixed["details"]["l8_a3_qualifies"] is True  # type: ignore[index]


def test_t5_plus_t6_alone_do_not_fire_but_t4_plus_t6_does() -> None:
    _ledger, episode = _short_episode()
    engine = _warning_engine(MemorySink())
    assert engine.thesis_broken(episode, ThesisInputs(frozenset({"T5", "T6"}), {}), 1_000) is None
    result = engine.thesis_broken(episode, ThesisInputs(frozenset({"T4", "T6"}), {}), 2_000)
    assert result is not None
    assert result["details"]["validated_group_count"] == 1  # type: ignore[index]
    assert result["details"]["l8_a3_qualifies"] is False  # type: ignore[index]


def test_thesis_clears_after_an_hour_then_can_reissue() -> None:
    _ledger, episode = _short_episode()
    engine = _warning_engine(MemorySink())
    inputs = ThesisInputs(frozenset({"T1"}), {"high": "120"})
    assert engine.thesis_broken(episode, inputs, 1_000) is not None
    assert engine.thesis_broken(episode, ThesisInputs(frozenset(), {}), 2_000) is None
    recheck = engine.thesis_broken(episode, ThesisInputs(frozenset(), {}), 3_602_000)
    assert recheck is not None and recheck["event_type"] == "THESIS_RECHECK"
    assert engine.thesis_broken(episode, inputs, 3_603_000) is not None


def test_long_thesis_is_not_applicable_and_liquidation_sign_is_directional() -> None:
    sink = MemorySink()
    ledger = PositionLedger("synthetic-account", sink)
    long = ledger.apply_fill(_fill("long", "long-order", "BUY", "1", "100"))[0]
    engine = _warning_engine(sink)
    thesis = engine.thesis_broken(long, ThesisInputs(frozenset({"T1"}), {}), 20_000)
    assert thesis is not None and thesis["event_type"] == "THESIS_NOT_APPLICABLE"
    liq = engine.liquidation_proximity(
        long,
        liquidation_price=Decimal("95"),
        snapshot_received_ms=20_000,
        mark=Decimal("100"),
        mark_update_ms=20_000,
        now_ms=20_000,
        account_supported=True,
        cross_margin=False,
    )
    assert liq is not None and liq["warning_id"] == "LIQ-PROXIMITY"
    assert liq["details"]["distance_percent"] == "5.00"  # type: ignore[index]


def test_liquidation_freshness_uses_snapshot_receive_time() -> None:
    _ledger, episode = _short_episode()
    engine = _warning_engine(MemorySink())
    now_ms = 1_700_000_000_000
    position_update_time_ms = now_ms - 3_600_000
    snapshot_received_ms = now_ms - 1_000
    result = engine.liquidation_proximity(
        episode,
        liquidation_price=Decimal("110"),
        snapshot_received_ms=snapshot_received_ms,
        mark=Decimal("100"),
        mark_update_ms=now_ms - 500,
        now_ms=now_ms,
        account_supported=True,
        cross_margin=False,
    )
    assert position_update_time_ms < now_ms - 15_000
    assert result is not None and result["warning_id"] == "LIQ-PROXIMITY"


@pytest.mark.parametrize(
    "liq,rest_ms,mark,mark_ms,supported",
    [
        (Decimal("0.00000000"), 20_000, Decimal("100"), 20_000, True),
        (Decimal("90"), 20_000, Decimal("100"), 20_000, True),
        (Decimal("130"), 1_000, Decimal("100"), 20_000, True),
        (Decimal("130"), 20_000, Decimal("100"), 1_000, True),
        (Decimal("130"), 20_000, Decimal("100"), 20_000, False),
    ],
)
def test_liquidation_unknown_inputs_fail_closed(
    liq: Decimal, rest_ms: int, mark: Decimal, mark_ms: int, supported: bool
) -> None:
    _ledger, episode = _short_episode()
    engine = _warning_engine(MemorySink())
    result = engine.liquidation_proximity(
        episode,
        liquidation_price=liq,
        snapshot_received_ms=rest_ms,
        mark=mark,
        mark_update_ms=mark_ms,
        now_ms=20_000,
        account_supported=supported,
        cross_margin=True,
    )
    assert result is not None and result["event_type"] == "LIQ_PROXIMITY_UNKNOWN"


def test_unknown_and_not_applicable_records_emit_only_on_state_changes() -> None:
    sink = MemorySink()
    ledger, short = _short_episode(sink)
    long = ledger.apply_fill(
        Fill("long-1", "long-order", "LONGUSDT", "BUY", Decimal("1"), Decimal("100"), 1000)
    )[0]
    engine = _warning_engine(sink)

    first_unknown = engine.liquidation_proximity(
        short,
        liquidation_price=None,
        snapshot_received_ms=20_000,
        mark=Decimal("100"),
        mark_update_ms=20_000,
        now_ms=20_000,
        account_supported=True,
        cross_margin=False,
    )
    assert first_unknown is not None
    assert engine.liquidation_proximity(
        short,
        liquidation_price=None,
        snapshot_received_ms=20_001,
        mark=Decimal("100"),
        mark_update_ms=20_001,
        now_ms=20_001,
        account_supported=True,
        cross_margin=False,
    ) is None

    thesis = ThesisInputs(frozenset(), {})
    assert engine.thesis_broken(long, thesis, 20_000) is not None
    assert engine.thesis_broken(long, thesis, 20_001) is None
    add_input = _block_input("100", 20_000)
    assert engine.add_blocked(long, add_input) is not None
    assert engine.add_blocked(long, AddBlockInputs(
        add_input.closed_mark,
        add_input.closed_candle_ms + 1,
        add_input.rule_state,
        add_input.confirmed,
        add_input.next_quantity,
        add_input.r_live,
    )) is None


def test_liquidation_threshold_hysteresis_and_cross_basis() -> None:
    _ledger, episode = _short_episode()
    engine = _warning_engine(MemorySink())

    def check(liquidation_price: Decimal) -> dict[str, object] | None:
        return engine.liquidation_proximity(
            episode,
            liquidation_price=liquidation_price,
            snapshot_received_ms=20_000,
            mark=Decimal("100"),
            mark_update_ms=20_000,
            now_ms=20_000,
            account_supported=True,
            cross_margin=True,
        )

    first = check(Decimal("130"))
    assert first is not None and first["details"]["threshold_percent"] == "33"  # type: ignore[index]
    assert first["details"]["liq_basis"] == "CROSS_ACCOUNT_DEPENDENT"  # type: ignore[index]
    assert check(Decimal("130")) is None
    assert check(Decimal("136")) is None
    again = check(Decimal("132"))
    assert again is not None


def test_liquidation_jump_emits_only_the_lowest_breached_threshold() -> None:
    _ledger, episode = _short_episode()
    engine = _warning_engine(MemorySink())
    result = engine.liquidation_proximity(
        episode,
        liquidation_price=Decimal("110"),
        snapshot_received_ms=20_000,
        mark=Decimal("100"),
        mark_update_ms=20_000,
        now_ms=20_000,
        account_supported=True,
        cross_margin=False,
    )
    assert result is not None
    assert result["details"]["distance_percent"] == "10.0"  # type: ignore[index]
    assert result["details"]["threshold_percent"] == "14"  # type: ignore[index]
    assert engine.liquidation_proximity(
        episode,
        liquidation_price=Decimal("110"),
        snapshot_received_ms=20_000,
        mark=Decimal("100"),
        mark_update_ms=20_000,
        now_ms=20_001,
        account_supported=True,
        cross_margin=False,
    ) is None


def test_add_blocked_uses_closed_candle_reasons_and_five_percent_steps() -> None:
    _ledger, episode = _short_episode()
    episode.invalidation = Decimal("120")
    engine = _warning_engine(MemorySink())
    first = engine.add_blocked(episode, _block_input("105", 100))
    assert first is not None and "NO_UPDATED_CONFIRMATION" in first["details"]["reasons"]  # type: ignore[index,operator]
    assert first["details"]["price_source"] == "CLOSED_5M_MARK_CLOSE"  # type: ignore[index]
    assert engine.add_blocked(episode, _block_input("105", 100)) is None
    assert engine.add_blocked(episode, _block_input("105", 200)) is None
    step = engine.add_blocked(episode, _block_input("110", 300))
    assert step is not None and step["details"]["next_price_stage"] == 2  # type: ignore[index]
    invalidated = engine.add_blocked(
        episode,
        AddBlockInputs(Decimal("120"), 400, "UNAVAILABLE", False, Decimal("0.5"), Decimal("40")),
    )
    assert invalidated is not None
    assert "PRICE_AT_OR_ABOVE_INVALIDATION" in invalidated["details"]["reasons"]  # type: ignore[index,operator]


def test_add_blocked_rearms_price_stage_after_entry_reference_changes() -> None:
    ledger, episode = _short_episode()
    episode.invalidation = Decimal("150")
    engine = _warning_engine(MemorySink())
    first = engine.add_blocked(episode, _block_input("105", 100))
    assert first is not None and first["details"]["next_price_stage"] == 1  # type: ignore[index]

    ledger.apply_fill(_fill("2", "add-1", "SELL", "0.25", "110", 101))
    second = engine.add_blocked(episode, _block_input("115.5", 200))
    assert second is not None and second["details"]["next_price_stage"] == 1  # type: ignore[index]


def test_degraded_health_suppresses_warnings_and_counts_missed_conditions() -> None:
    sink = MemorySink()
    _ledger, episode = _short_episode(sink)
    engine = WarningEngine(_policy(), sink)
    engine.mark_degraded("STREAM_GAP", 10, episode)
    assert engine.thesis_broken(episode, ThesisInputs(frozenset({"T1"}), {}), 20) is None
    assert engine.add_detected(episode, 21, stage="PARTIAL") is None
    recovered = engine.mark_recovered(30, episode)
    assert recovered["details"]["missed_during_degraded"] == 2  # type: ignore[index]
    assert recovered["details"]["state"] == "RECOVERED"  # type: ignore[index]


def test_warnings_wait_for_verified_one_way_account_mode() -> None:
    _ledger, episode = _short_episode()
    engine = WarningEngine(_policy(), MemorySink())
    inputs = ThesisInputs(frozenset({"T1"}), {})
    assert engine.thesis_broken(episode, inputs, 10) is None
    assert engine.observe_account_config(
        {"dualSidePosition": False, "multiAssetsMargin": False},
        20,
        portfolio_margin=False,
        episode=episode,
    )
    assert engine.thesis_broken(episode, inputs, 30) is not None


def test_add_detected_records_partial_then_final_and_unknown_rule_flags() -> None:
    ledger, episode = _short_episode()
    episode.invalidation = Decimal("130")
    add = _fill("2", "11", "SELL", "0.25", "110", 1_700_000_001_000)
    ledger.apply_fill(add)
    tranche = episode.tranches["11"]
    engine = _warning_engine(MemorySink())
    partial = engine.add_detected(episode, add.occurred_ms, stage="PARTIAL", tranche=tranche)
    assert partial is not None
    assert partial["details"]["plan_status"] == "PROVISIONAL"  # type: ignore[index]
    assert partial["details"]["compliance"]["size"] is True  # type: ignore[index]
    ledger.apply_fill(_fill("3", "11", "SELL", "0.25", "112", add.occurred_ms + 1))
    final = engine.add_detected(
        episode,
        add.occurred_ms + 2,
        stage="FINAL",
        tranche=tranche,
        break_even_price=Decimal("105"),
    )
    assert final is not None
    assert final["details"]["plan_status"] == "FROZEN"  # type: ignore[index]
    assert final["details"]["rules_flag"] == "UNKNOWN"  # type: ignore[index]
    assert final["details"]["remaining_budget"] is not None  # type: ignore[index]


def test_account_update_position_mismatch_is_recorded_as_discrepancy() -> None:
    sink = MemorySink()
    ledger, _episode = _short_episode(sink)
    payload: dict[str, object] = {
        "a": {"m": "ORDER", "P": [{"s": "SYNTHUSDT", "pa": "-2"}]}
    }
    assert ledger.apply_account_update(payload, 1_700_000_001_000) is False
    assert sink.records[-1]["event_type"] == "ACCOUNT_POSITION_DISCREPANCY"


def test_tracker_config_fails_closed_for_unhashed_activation(tmp_path: Path) -> None:
    config = json.loads((ROOT / "config" / "pump_fade_v3" / "positions.json").read_text())
    assert (
        load_position_tracker_config(ROOT / "config" / "pump_fade_v3" / "positions.json").enabled
        is False
    )
    config["activation"]["runner"] = {"enabled": True, "rule_hash": None}
    path = tmp_path / "positions.json"
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError, match="promoted arm hash"):
        load_position_tracker_config(path)


def test_warning_policy_uses_registered_values_and_clock_offset_corrects_rtt() -> None:
    policy = load_warning_policy(ROOT / "config" / "pump_fade_v3" / "policy_v3.json")
    assert policy.max_adds == 2
    assert policy.legacy_adds == 8
    assert policy.liq_thresholds_percent == tuple(map(Decimal, ("33", "25", "20", "14")))
    assert estimate_clock_offset_ms(1050, 1000, 1100) == 0


def test_unavailable_rule_context_still_derives_t1_t4_t5_t6_from_sources() -> None:
    policy = load_warning_policy(ROOT / "config" / "pump_fade_v3" / "policy_v3.json")
    evidence = ThesisEvidence(
        now_ms=1_000_000,
        invalidation=Decimal("120"),
        closed_mark_high=Decimal("120"),
        lagged_oi=Decimal("110"),
        oi_baseline=Decimal("100"),
        oi_usable_at_ms=900_000,
        forced_buy_times_ms=(999_999,),
        funding_rate=Decimal("0.0099"),
        funding_cap=Decimal("0.01"),
        funding_interval_shortened=False,
    )
    derived = evaluate_available_thesis(evidence, policy)
    assert derived.triggers == frozenset({"T1", "T4", "T5", "T6"})
    assert derived.unavailable_reasons == ()


def test_funding_limit_without_funding_info_stays_unknown() -> None:
    policy = _policy()
    evidence = ThesisEvidence(
        now_ms=1_000_000,
        invalidation=None,
        closed_mark_high=Decimal("100"),
        lagged_oi=None,
        oi_baseline=None,
        oi_usable_at_ms=None,
        forced_buy_times_ms=(),
        funding_rate=Decimal("0.01"),
        funding_cap=None,
        funding_interval_shortened=False,
    )
    derived = evaluate_available_thesis(evidence, policy)
    assert "T6" not in derived.triggers
    assert "T6_FUNDING_CAP_UNKNOWN" in derived.unavailable_reasons


def test_account_mode_degrades_and_status_records_keep_funding_and_adl() -> None:
    sink = MemorySink()
    _ledger, episode = _short_episode(sink)
    engine = WarningEngine(_policy(), sink)
    assert not engine.observe_account_config(
        {"dualSidePosition": True, "multiAssetsMargin": False},
        1_700_000_000_100,
        portfolio_margin=False,
        episode=episode,
    )
    assert sink.records[-1]["details"]["reason"] == "UNSUPPORTED_ACCOUNT_MODE"  # type: ignore[index]
    engine.mark_recovered(1_700_000_000_200, episode)
    record = engine.position_status(
        episode,
        1_700_000_000_300,
        unrealized_pnl=Decimal("-2"),
        break_even_price=Decimal("101"),
        cumulative_funding=Decimal("-0.2"),
        next_funding_ms=1_700_003_600_000,
        funding_rate=Decimal("0.0001"),
        liquidation_distance_percent=Decimal("25"),
        remaining_budget_r=Decimal("0.4"),
        adl=2,
    )
    assert record["event_type"] == "POSITION-STATUS"
    assert record["details"]["rule_state"] == "UNKNOWN"  # type: ignore[index]
    assert record["details"]["adl"] == 2  # type: ignore[index]
    assert record["occurred_seoul"].endswith("+09:00")  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_listen_key_client_uses_one_path_api_key_only_and_masks_key() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "POST":
            return httpx.Response(200, json={"listenKey": "synthetic-listen-key"})
        return httpx.Response(204)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    key_client = ListenKeyClient(SecretStr("test-key"), client)
    async with client:
        listen_key = await key_client.create()
        await key_client.keep_alive()
        await key_client.close()
    assert listen_key.get_secret_value() == "synthetic-listen-key"
    assert "synthetic-listen-key" not in str(listen_key)
    assert calls == [
        ("POST", LISTEN_KEY_PATH),
        ("PUT", LISTEN_KEY_PATH),
        ("DELETE", LISTEN_KEY_PATH),
    ]
    assert "api_secret" not in inspect.signature(ListenKeyClient).parameters


def test_l7_package_has_no_other_http_routes_or_discord_imports() -> None:
    package = ROOT / "src" / "signalbot" / "pump_fade_v3"
    path_pattern = re.compile(r"/(?:fapi|dapi)/[^\"']+")
    forbidden = re.compile(
        r"/(?:[^\"']*/)?(?:order|algoOrder|batchOrders|leverage|marginType|positionSide)(?:/|\?|\"|')",
        re.I,
    )
    found_paths: list[str] = []
    non_get_calls: list[tuple[str, str]] = []
    for source in package.glob("*.py"):
        text = source.read_text(encoding="utf-8")
        tree = ast.parse(text)
        assert not any(
            (
                isinstance(node, ast.Import)
                and any(
                    alias.name == "discord" or alias.name.startswith("discord.")
                    for alias in node.names
                )
            )
            or (
                isinstance(node, ast.ImportFrom)
                and node.module is not None
                and node.module.startswith("discord")
            )
            for node in ast.walk(tree)
        )
        found_paths.extend(path_pattern.findall(text))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr in {"post", "put", "delete", "patch"}:
                non_get_calls.append((source.name, node.func.attr.upper()))
            if node.func.attr == "request":
                assert source.name == "listen_key.py"
                assert isinstance(node.args[0], ast.Name) and node.args[0].id == "method"
    assert non_get_calls == []
    assert (
        sum(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "request"
            for source in package.glob("*.py")
            for node in ast.walk(ast.parse(source.read_text(encoding="utf-8")))
        )
        == 1
    )
    assert "/fapi/v1/listenKey" in found_paths
    assert not any(forbidden.search(path) for path in found_paths)
