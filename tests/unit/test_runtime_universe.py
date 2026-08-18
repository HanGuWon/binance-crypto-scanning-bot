from decimal import Decimal
from typing import Any

import pytest

from conftest import make_candle, make_feature
from signalbot.clock import ReplayClock
from signalbot.config import Settings
from signalbot.data.funding import FundingRatePoint
from signalbot.domain.enums import Market
from signalbot.domain.models import AggTrade, BookTicker
from signalbot.persistence.repository import SqlRepository
from signalbot.runtime import MarketRuntime


def _settings() -> Settings:
    return Settings.model_validate(
        {
            "binance": {
                "markets": ["spot"],
                "top_n": 2,
                "surveillance_n": 3,
                "intervals": ["5m"],
                "primary_interval": "5m",
            },
            "storage": {"url": "sqlite:///:memory:"},
            "runtime": {"persist_candles": False},
        }
    )


def _runtime(settings: Settings) -> tuple[MarketRuntime, SqlRepository]:
    repository = SqlRepository(settings.storage.url)
    repository.initialize()

    async def discard(_decision: Any) -> None:
        return None

    return (
        MarketRuntime(Market.SPOT, settings, repository, ReplayClock(), discard),
        repository,
    )


@pytest.mark.asyncio
async def test_non_tradable_candles_books_and_trades_are_rejected() -> None:
    runtime, repository = _runtime(_settings())
    runtime.set_active_symbols(
        frozenset({"BTCUSDT"}),
        frozenset({"BTCUSDT", "DOGEUSDT"}),
    )
    await runtime.handle_event(make_candle(0, symbol="DOGEUSDT"))
    await runtime.handle_event(
        BookTicker(
            market=Market.SPOT,
            symbol="DOGEUSDT",
            event_time_ms=1_000,
            bid_price=Decimal("99.9"),
            bid_quantity=Decimal("1"),
            ask_price=Decimal("100.1"),
            ask_quantity=Decimal("1"),
        )
    )
    await runtime.handle_event(
        AggTrade(
            market=Market.SPOT,
            symbol="DOGEUSDT",
            event_time_ms=1_000,
            trade_time_ms=1_000,
            price=Decimal("100"),
            quantity=Decimal("1"),
            is_buyer_maker=False,
            aggregate_trade_id=1,
        )
    )

    assert runtime.candles.size(Market.SPOT, "DOGEUSDT", "5m") == 0
    assert runtime.books.spread_bps(
        Market.SPOT, "DOGEUSDT", as_of_ms=1_000, maximum_age_ms=0
    ) is None
    assert not runtime.order_flow.snapshot(
        Market.SPOT, "DOGEUSDT", 1_000
    ).available
    repository.close()


@pytest.mark.asyncio
async def test_context_symbol_keeps_market_data_without_execution_state_or_signals() -> None:
    runtime, repository = _runtime(_settings())
    runtime.set_active_symbols(
        frozenset({"ETHUSDT"}),
        frozenset({"ETHUSDT"}),
        frozenset({"BTCUSDT"}),
    )

    assert runtime.bootstrap([make_candle(0, symbol="BTCUSDT")]) == 1
    await runtime.handle_event(make_candle(1, symbol="BTCUSDT"))
    await runtime.handle_event(
        BookTicker(
            market=Market.SPOT,
            symbol="BTCUSDT",
            event_time_ms=1_000,
            bid_price=Decimal("99.9"),
            bid_quantity=Decimal("1"),
            ask_price=Decimal("100.1"),
            ask_quantity=Decimal("1"),
        )
    )
    await runtime.handle_event(
        AggTrade(
            market=Market.SPOT,
            symbol="BTCUSDT",
            event_time_ms=1_000,
            trade_time_ms=1_000,
            price=Decimal("100"),
            quantity=Decimal("1"),
            is_buyer_maker=False,
            aggregate_trade_id=1,
        )
    )

    assert runtime.candles.size(Market.SPOT, "BTCUSDT", "5m") == 2
    assert runtime.books.spread_bps(
        Market.SPOT, "BTCUSDT", as_of_ms=1_000, maximum_age_ms=0
    ) is None
    assert not runtime.order_flow.snapshot(
        Market.SPOT, "BTCUSDT", 1_000
    ).available
    assert runtime.decision_count == 0

    runtime.set_active_symbols(
        frozenset({"ETHUSDT"}),
        frozenset({"ETHUSDT"}),
    )
    assert runtime.candles.size(Market.SPOT, "BTCUSDT", "5m") == 0
    repository.close()


@pytest.mark.asyncio
async def test_universe_rotation_prunes_every_runtime_owned_symbol_store() -> None:
    runtime, repository = _runtime(_settings())
    runtime.set_active_symbols(
        frozenset({"BTCUSDT", "ETHUSDT"}),
        frozenset({"BTCUSDT", "ETHUSDT", "DOGEUSDT"}),
    )
    runtime.bootstrap([make_candle(0, symbol="BTCUSDT")])
    await runtime.handle_event(
        BookTicker(
            market=Market.SPOT,
            symbol="BTCUSDT",
            event_time_ms=1_000,
            bid_price=Decimal("99.9"),
            bid_quantity=Decimal("1"),
            ask_price=Decimal("100.1"),
            ask_quantity=Decimal("1"),
        )
    )
    await runtime.handle_event(
        AggTrade(
            market=Market.SPOT,
            symbol="BTCUSDT",
            event_time_ms=1_000,
            trade_time_ms=1_000,
            price=Decimal("100"),
            quantity=Decimal("1"),
            is_buyer_maker=False,
            aggregate_trade_id=1,
        )
    )
    runtime.funding.update(FundingRatePoint("BTCUSDT", 1_000, Decimal("0.001")))
    runtime._store_feature(
        make_feature(
            market=Market.SPOT,
            symbol="BTCUSDT",
            interval="5m",
            event_time_ms=299_999,
        )
    )

    runtime.set_active_symbols(
        frozenset({"ETHUSDT"}),
        frozenset({"ETHUSDT", "DOGEUSDT"}),
    )
    assert runtime.candles.size(Market.SPOT, "BTCUSDT", "5m") == 0
    assert runtime.books.spread_bps(
        Market.SPOT, "BTCUSDT", as_of_ms=1_000, maximum_age_ms=0
    ) is None
    assert not runtime.order_flow.snapshot(Market.SPOT, "BTCUSDT", 1_000).available
    assert runtime.funding.latest_time_ms("BTCUSDT") is None
    assert ("BTCUSDT", "5m") not in runtime._features
    assert ("BTCUSDT", "5m") not in runtime._feature_history

    await runtime.handle_event(make_candle(1, symbol="BTCUSDT"))
    await runtime.handle_event(make_candle(1, symbol="ETHUSDT"))
    assert runtime.candles.size(Market.SPOT, "BTCUSDT", "5m") == 0
    assert runtime.candles.size(Market.SPOT, "ETHUSDT", "5m") == 1
    repository.close()


def test_universe_contract_rejects_capacity_and_subset_violations() -> None:
    settings = _settings()
    runtime, repository = _runtime(settings)
    with pytest.raises(ValueError, match="top_n"):
        runtime.set_active_symbols(
            frozenset({"AUSDT", "BUSDT", "CUSDT"}),
            frozenset({"AUSDT", "BUSDT", "CUSDT"}),
        )
    with pytest.raises(ValueError, match="surveillance_n"):
        runtime.set_active_symbols(
            frozenset({"AUSDT"}),
            frozenset({"AUSDT", "BUSDT", "CUSDT", "DUSDT"}),
        )
    with pytest.raises(ValueError, match="subset"):
        runtime.set_active_symbols(
            frozenset({"AUSDT"}),
            frozenset({"BUSDT"}),
        )
    repository.close()


@pytest.mark.asyncio
async def test_conflicting_book_ticker_cursor_is_discarded_without_raising() -> None:
    runtime, repository = _runtime(_settings())
    runtime.set_active_symbols(frozenset({"BTCUSDT"}), frozenset({"BTCUSDT"}))
    first = BookTicker(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=1_000,
        bid_price=Decimal("99.9"),
        bid_quantity=Decimal("1"),
        ask_price=Decimal("100.1"),
        ask_quantity=Decimal("1"),
    )
    conflicting = BookTicker(
        market=Market.SPOT,
        symbol="BTCUSDT",
        event_time_ms=1_000,
        bid_price=Decimal("98.0"),
        bid_quantity=Decimal("1"),
        ask_price=Decimal("100.1"),
        ask_quantity=Decimal("1"),
    )

    await runtime.handle_event(first)
    await runtime.handle_event(conflicting)

    snapshot = runtime.books.snapshot(
        Market.SPOT, "BTCUSDT", as_of_ms=1_000, maximum_age_ms=0
    )
    assert snapshot is not None
    assert snapshot.bid_quote_capacity == float(Decimal("99.9"))
    repository.close()
