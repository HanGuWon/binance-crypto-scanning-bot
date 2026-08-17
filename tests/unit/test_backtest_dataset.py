from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from conftest import make_candle
from signalbot.backtest.dataset import (
    DatasetDownloadError,
    DatasetValidationError,
    KlineDataset,
    KlineDatasetRequest,
    build_dataset_manifest,
    download_kline_dataset,
    find_kline_gaps,
    read_dataset_manifest,
    read_kline_csv,
    sha256_file,
    verify_dataset_manifest,
    write_dataset_manifest,
    write_kline_csv,
)
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.exchange.binance.rest import BinanceRestClient


class FakeRest:
    def __init__(self, market: Market, candles: list[Candle]) -> None:
        self.market = market
        self.candles = candles
        self.start_times: list[int | None] = []

    async def klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        now_ms: int | None = None,
    ) -> list[Candle]:
        del now_ms
        self.start_times.append(start_time_ms)
        start = 0 if start_time_ms is None else start_time_ms
        end = 2**63 - 1 if end_time_ms is None else end_time_ms
        return [
            candle
            for candle in self.candles
            if candle.symbol == symbol.upper()
            and candle.interval == interval
            and candle.open_time_ms >= start
            and candle.close_time_ms <= end
        ][:limit]


class StalledRest(FakeRest):
    async def klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        now_ms: int | None = None,
    ) -> list[Candle]:
        del symbol, interval, limit, start_time_ms, end_time_ms, now_ms
        return self.candles[:1]


def futures_candles(count: int = 5) -> list[Candle]:
    return [make_candle(index, market=Market.FUTURES) for index in range(count)]


@pytest.mark.asyncio
async def test_download_paginates_with_closed_range_and_preserves_alias() -> None:
    candles = futures_candles()
    rest = FakeRest(Market.FUTURES, candles)
    request = KlineDatasetRequest(
        market=Market.FUTURES,
        symbol="btcusdt",
        alias="btc/perp,서울\n별칭",
        interval="5m",
        start_time_ms=0,
        end_time_ms=candles[-1].close_time_ms,
    )

    dataset = await download_kline_dataset(
        cast(BinanceRestClient, rest), request, page_limit=2
    )

    assert dataset.candles == tuple(candles)
    assert dataset.request.symbol == "BTCUSDT"
    assert dataset.request.dataset_alias == "btc/perp,서울\n별칭"
    assert rest.start_times == [0, 600_000, 1_200_000]


@pytest.mark.asyncio
async def test_download_rejects_wrong_market_and_stalled_pagination() -> None:
    candles = futures_candles(3)
    request = KlineDatasetRequest(
        Market.FUTURES, "BTCUSDT", "5m", 0, candles[-1].close_time_ms
    )
    wrong_market = FakeRest(Market.SPOT, candles)
    with pytest.raises(DatasetDownloadError, match="client market"):
        await download_kline_dataset(cast(BinanceRestClient, wrong_market), request)

    stalled = StalledRest(Market.FUTURES, candles)
    with pytest.raises(DatasetDownloadError, match="no progress"):
        await download_kline_dataset(cast(BinanceRestClient, stalled), request, page_limit=1)


@pytest.mark.asyncio
async def test_download_excludes_partial_listing_bar() -> None:
    candles = futures_candles(3)
    partial = candles[0].model_copy(update={"close_time_ms": 100_000})
    rest = FakeRest(Market.FUTURES, [partial, *candles[1:]])
    request = KlineDatasetRequest(
        Market.FUTURES, "BTCUSDT", "5m", 0, candles[-1].close_time_ms
    )

    dataset = await download_kline_dataset(
        cast(BinanceRestClient, rest), request, page_limit=3
    )

    assert dataset.candles == tuple(candles[1:])


def test_gzip_csv_is_byte_deterministic_and_round_trips(tmp_path: Path) -> None:
    candles = futures_candles()
    request = KlineDatasetRequest(
        Market.FUTURES,
        "BTCUSDT",
        "5m",
        0,
        candles[-1].close_time_ms,
        alias="bitcoin, perpetual\n서울",
    )
    dataset = KlineDataset(request, tuple(candles))
    first = write_kline_csv(dataset, tmp_path / "first.csv.gz")
    second = write_kline_csv(dataset, tmp_path / "second.csv.gz")

    assert first.read_bytes() == second.read_bytes()
    assert sha256_file(first) == sha256_file(second)
    assert read_kline_csv(first) == dataset


def test_validation_accepts_ohlc_equality_boundary_and_reports_gap() -> None:
    candles = futures_candles(3)
    flat = candles[0].model_copy(
        update={
            "open": Decimal("100"),
            "high": Decimal("100"),
            "low": Decimal("100"),
            "close": Decimal("100"),
        }
    )
    request = KlineDatasetRequest(
        Market.FUTURES, "BTCUSDT", "5m", 0, candles[-1].close_time_ms
    )
    KlineDataset(request, (flat, candles[1], candles[2]))

    gapped = KlineDataset(request, (candles[0], candles[2]))
    gaps = find_kline_gaps(gapped)
    assert len(gaps) == 1
    assert gaps[0].missing_intervals == 1


def test_validation_rejects_bad_ohlc_and_open_candle() -> None:
    candle = futures_candles(1)[0]
    request = KlineDatasetRequest(
        Market.FUTURES, "BTCUSDT", "5m", 0, candle.close_time_ms
    )
    bad_high = candle.model_copy(update={"high": candle.open - Decimal("0.01")})
    with pytest.raises(DatasetValidationError, match="high"):
        KlineDataset(request, (bad_high,))

    with pytest.raises(DatasetValidationError, match="not fully closed"):
        KlineDataset(request, (candle.model_copy(update={"is_closed": False}),))


def test_validation_rejects_taker_volume_above_total() -> None:
    candle = futures_candles(1)[0]
    request = KlineDatasetRequest(
        Market.FUTURES, "BTCUSDT", "5m", 0, candle.close_time_ms
    )

    bad_quote = candle.model_copy(
        update={"taker_buy_quote_volume": candle.quote_volume + Decimal("0.01")}
    )
    with pytest.raises(DatasetValidationError, match="taker-buy quote"):
        KlineDataset(request, (bad_quote,))

    bad_base = candle.model_copy(
        update={"taker_buy_base_volume": candle.volume + Decimal("0.01")}
    )
    with pytest.raises(DatasetValidationError, match="taker-buy base"):
        KlineDataset(request, (bad_base,))


def test_manifest_round_trip_and_tamper_detection(tmp_path: Path) -> None:
    candles = futures_candles(2)
    dataset = KlineDataset(
        KlineDatasetRequest(
            Market.FUTURES,
            "BTCUSDT",
            "5m",
            0,
            candles[-1].close_time_ms,
            alias="btc-future",
        ),
        tuple(candles),
    )
    data_path = write_kline_csv(dataset, tmp_path / "btc.csv.gz")
    manifest = build_dataset_manifest(data_path)
    first_manifest_path = write_dataset_manifest(manifest, tmp_path / "first.manifest.json")
    second_manifest_path = write_dataset_manifest(manifest, tmp_path / "second.manifest.json")

    assert first_manifest_path.read_bytes() == second_manifest_path.read_bytes()
    assert read_dataset_manifest(first_manifest_path) == manifest
    verify_dataset_manifest(
        data_path, first_manifest_path, expected_request=dataset.request
    )

    stale_request = KlineDatasetRequest(
        Market.FUTURES,
        "BTCUSDT",
        "5m",
        0,
        candles[-1].close_time_ms + 300_000,
        alias="btc-future",
    )
    with pytest.raises(DatasetValidationError, match="request identity/range"):
        verify_dataset_manifest(
            data_path,
            first_manifest_path,
            expected_request=stale_request,
        )

    tampered = bytearray(data_path.read_bytes())
    tampered[-1] ^= 0x01
    data_path.write_bytes(tampered)
    with pytest.raises(DatasetValidationError, match="SHA-256"):
        verify_dataset_manifest(data_path, manifest)
