from __future__ import annotations

import csv
import gzip
import hashlib
import io
import math
from dataclasses import dataclass
from pathlib import Path

from signalbot.backtest.engine import FundingRate
from signalbot.exchange.binance.rest import BinanceRestClient

_CSV_COLUMNS = (
    "symbol",
    "start_time_ms",
    "end_time_ms",
    "funding_time_ms",
    "rate",
    "mark_price",
)


class FundingValidationError(ValueError):
    """Raised when funding data does not satisfy its recorded request contract."""


@dataclass(frozen=True, slots=True)
class FundingDataset:
    symbol: str
    start_time_ms: int
    end_time_ms: int
    rates: tuple[FundingRate, ...]

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise FundingValidationError("funding symbol must not be empty")
        if self.start_time_ms < 0 or self.end_time_ms < self.start_time_ms:
            raise FundingValidationError("invalid funding request time range")
        rates = tuple(self.rates)
        previous_time: int | None = None
        for item in rates:
            if not self.start_time_ms <= item.funding_time_ms <= self.end_time_ms:
                raise FundingValidationError(
                    f"funding event {item.funding_time_ms} is outside the request range"
                )
            if previous_time is not None and item.funding_time_ms <= previous_time:
                raise FundingValidationError(
                    "funding events must be strictly ordered and unique"
                )
            if not math.isfinite(item.rate):
                raise FundingValidationError("funding rate must be finite")
            if item.mark_price is not None and (
                not math.isfinite(item.mark_price) or item.mark_price <= 0
            ):
                raise FundingValidationError("funding mark price must be finite and positive")
            previous_time = item.funding_time_ms
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "rates", rates)


async def download_funding_dataset(
    client: BinanceRestClient,
    symbol: str,
    start_time_ms: int,
    end_time_ms: int,
) -> FundingDataset:
    cursor = start_time_ms
    values: dict[int, FundingRate] = {}
    while cursor <= end_time_ms:
        page = await client.funding_rates(symbol, cursor, end_time_ms, 1000)
        if not page:
            break
        newest = -1
        for row in page:
            try:
                timestamp = int(row["fundingTime"])
                rate = float(row["fundingRate"])
                raw_mark = row.get("markPrice")
                mark = None if raw_mark is None or raw_mark == "" else float(str(raw_mark))
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"invalid funding row for {symbol}") from exc
            if start_time_ms <= timestamp <= end_time_ms:
                values[timestamp] = FundingRate(timestamp, rate, mark)
            newest = max(newest, timestamp)
        if newest < cursor:
            raise RuntimeError(f"funding download made no progress for {symbol}")
        cursor = newest + 1
        if len(page) < 1000:
            break
    return FundingDataset(
        symbol=symbol.upper(),
        start_time_ms=start_time_ms,
        end_time_ms=end_time_ms,
        rates=tuple(values[key] for key in sorted(values)),
    )


def write_funding_csv(dataset: FundingDataset, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("wb") as raw:
        with gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as compressed:
            with io.TextIOWrapper(compressed, encoding="utf-8", newline="") as text:
                writer = csv.writer(text, lineterminator="\n")
                writer.writerow(_CSV_COLUMNS)
                for item in dataset.rates:
                    writer.writerow(
                        (
                            dataset.symbol,
                            dataset.start_time_ms,
                            dataset.end_time_ms,
                            item.funding_time_ms,
                            repr(item.rate),
                            "" if item.mark_price is None else repr(item.mark_price),
                        )
                    )
    return target


def read_funding_csv(path: str | Path) -> FundingDataset:
    source = Path(path)
    try:
        with gzip.open(source, mode="rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != _CSV_COLUMNS:
                raise FundingValidationError(
                    "funding dataset CSV header does not match schema"
                )
            rows = list(reader)
    except FundingValidationError:
        raise
    except (OSError, UnicodeError, csv.Error) as exc:
        raise FundingValidationError(
            f"cannot read funding dataset CSV: {source}"
        ) from exc
    if not rows:
        raise FundingValidationError("funding dataset is empty")
    first = rows[0]
    try:
        symbol = first["symbol"]
        start = int(first["start_time_ms"])
        end = int(first["end_time_ms"])
        values = []
        for row in rows:
            if (
                row["symbol"] != symbol
                or int(row["start_time_ms"]) != start
                or int(row["end_time_ms"]) != end
            ):
                raise FundingValidationError("inconsistent funding metadata")
            mark = float(row["mark_price"]) if row["mark_price"] else None
            values.append(
                FundingRate(int(row["funding_time_ms"]), float(row["rate"]), mark)
            )
    except FundingValidationError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise FundingValidationError("invalid funding dataset values") from exc
    return FundingDataset(symbol, start, end, tuple(values))


def verify_funding_dataset(
    path: str | Path,
    *,
    expected_symbol: str,
    expected_start_time_ms: int,
    expected_end_time_ms: int,
) -> FundingDataset:
    """Read funding data and require exact symbol and request-range coverage."""

    dataset = read_funding_csv(path)
    expected = (
        expected_symbol.strip().upper(),
        expected_start_time_ms,
        expected_end_time_ms,
    )
    actual = (dataset.symbol, dataset.start_time_ms, dataset.end_time_ms)
    if actual != expected:
        raise FundingValidationError(
            "funding dataset symbol/request range does not match the expected request"
        )
    return dataset


def funding_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
