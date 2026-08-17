from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from itertools import pairwise
from pathlib import Path

from signalbot.data.candles import interval_to_milliseconds
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle
from signalbot.exchange.binance.rest import BinanceRestClient

_CSV_COLUMNS = (
    "market",
    "symbol",
    "alias",
    "interval",
    "request_start_time_ms",
    "request_end_time_ms",
    "open_time_ms",
    "close_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "is_closed",
)
_MANIFEST_SCHEMA_VERSION = 2


class DatasetError(RuntimeError):
    """Base error for historical dataset operations."""


class DatasetDownloadError(DatasetError):
    """Raised when a paginated download cannot make reliable progress."""


class DatasetValidationError(DatasetError):
    """Raised when historical data violates the dataset contract."""


@dataclass(frozen=True, slots=True)
class KlineDatasetRequest:
    market: Market
    symbol: str
    interval: str
    start_time_ms: int
    end_time_ms: int
    alias: str | None = None

    def __post_init__(self) -> None:
        symbol = self.symbol.strip().upper()
        if not symbol:
            raise ValueError("symbol must not be empty")
        if self.alias is not None and not self.alias:
            raise ValueError("alias must not be empty when provided")
        if self.start_time_ms < 0:
            raise ValueError("start_time_ms must be non-negative")
        if self.end_time_ms < self.start_time_ms:
            raise ValueError("end_time_ms must be greater than or equal to start_time_ms")
        interval_to_milliseconds(self.interval)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "alias", symbol if self.alias is None else self.alias)

    @property
    def dataset_alias(self) -> str:
        if self.alias is None:  # pragma: no cover - normalized in __post_init__
            return self.symbol
        return self.alias


@dataclass(frozen=True, slots=True)
class KlineDataset:
    request: KlineDatasetRequest
    candles: tuple[Candle, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "candles", tuple(self.candles))
        validate_kline_dataset(self)


@dataclass(frozen=True, slots=True)
class DatasetManifest:
    schema_version: int
    data_file: str
    sha256: str
    market: str
    symbol: str
    alias: str
    interval: str
    request_start_time_ms: int
    request_end_time_ms: int
    row_count: int
    first_open_time_ms: int
    last_close_time_ms: int
    gap_count: int
    missing_intervals: int


@dataclass(frozen=True, slots=True)
class KlineGap:
    previous_open_time_ms: int
    next_open_time_ms: int
    missing_intervals: int


async def download_kline_dataset(
    client: BinanceRestClient,
    request: KlineDatasetRequest,
    *,
    page_limit: int | None = None,
) -> KlineDataset:
    """Download all fully closed Binance klines in an inclusive time range."""

    if client.market is not request.market:
        raise DatasetDownloadError(
            f"client market {client.market.value} does not match request {request.market.value}"
        )
    maximum_limit = 1_000 if request.market is Market.SPOT else 1_500
    limit = maximum_limit if page_limit is None else page_limit
    if not 1 <= limit <= maximum_limit:
        raise ValueError(f"page_limit must be between 1 and {maximum_limit}")

    step_ms = interval_to_milliseconds(request.interval)
    cursor = request.start_time_ms
    by_open_time: dict[int, Candle] = {}
    while cursor <= request.end_time_ms:
        page = await client.klines(
            request.symbol,
            request.interval,
            limit,
            start_time_ms=cursor,
            end_time_ms=request.end_time_ms,
            now_ms=request.end_time_ms + 1,
        )
        if not page:
            break

        newest_open_time: int | None = None
        for candle in page:
            # A newly listed symbol can begin with a partial interval whose close
            # timestamp precedes the canonical boundary. It is closed on Binance,
            # but it is not a complete bar for an interval-based experiment.
            # Exclude it; any partial bar inside an otherwise continuous series
            # still becomes an explicit continuity failure below.
            if candle.close_time_ms != candle.open_time_ms + step_ms - 1:
                newest_open_time = max(newest_open_time or -1, candle.open_time_ms)
                continue
            if candle.open_time_ms < request.start_time_ms:
                continue
            if candle.close_time_ms > request.end_time_ms:
                continue
            existing = by_open_time.get(candle.open_time_ms)
            if existing is not None and existing != candle:
                raise DatasetDownloadError(
                    f"conflicting candle at open_time_ms={candle.open_time_ms}"
                )
            by_open_time[candle.open_time_ms] = candle
            if newest_open_time is None or candle.open_time_ms > newest_open_time:
                newest_open_time = candle.open_time_ms

        if newest_open_time is None or newest_open_time < cursor:
            raise DatasetDownloadError(f"download made no progress from startTime={cursor}")
        next_cursor = newest_open_time + step_ms
        if next_cursor <= cursor:
            raise DatasetDownloadError(f"download cursor did not advance from startTime={cursor}")
        cursor = next_cursor

    if not by_open_time:
        raise DatasetDownloadError(
            f"no closed klines returned for {request.market.value} {request.symbol}"
        )
    candles = tuple(by_open_time[key] for key in sorted(by_open_time))
    return KlineDataset(request=request, candles=candles)


def validate_kline_dataset(dataset: KlineDataset) -> None:
    """Validate identity, OHLC integrity, ordering, and interval-grid alignment."""

    if not dataset.candles:
        raise DatasetValidationError("dataset must contain at least one candle")
    request = dataset.request
    step_ms = interval_to_milliseconds(request.interval)
    previous: Candle | None = None
    for candle in dataset.candles:
        _validate_candle_identity(candle, request)
        _validate_candle_values(candle, step_ms)
        outside_request = (
            candle.open_time_ms < request.start_time_ms
            or candle.close_time_ms > request.end_time_ms
        )
        if outside_request:
            raise DatasetValidationError(
                f"candle {candle.open_time_ms} lies outside the requested time range"
            )
        if previous is not None:
            difference = candle.open_time_ms - previous.open_time_ms
            if difference <= 0 or difference % step_ms != 0:
                raise DatasetValidationError(
                    f"candle time-grid error after open_time_ms={previous.open_time_ms}: "
                    f"got {candle.open_time_ms}"
                )
        previous = candle


def find_kline_gaps(dataset: KlineDataset) -> tuple[KlineGap, ...]:
    step_ms = interval_to_milliseconds(dataset.request.interval)
    gaps = []
    for previous, current in pairwise(dataset.candles):
        difference = current.open_time_ms - previous.open_time_ms
        if difference > step_ms:
            gaps.append(
                KlineGap(
                    previous_open_time_ms=previous.open_time_ms,
                    next_open_time_ms=current.open_time_ms,
                    missing_intervals=difference // step_ms - 1,
                )
            )
    return tuple(gaps)


def write_kline_csv(dataset: KlineDataset, path: str | Path) -> Path:
    """Write a canonical gzip CSV whose bytes are stable across repeated writes."""

    validate_kline_dataset(dataset)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("wb") as raw:
        with gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as compressed:
            with io.TextIOWrapper(compressed, encoding="utf-8", newline="") as text:
                writer: csv.DictWriter[str] = csv.DictWriter(
                    text,
                    fieldnames=list(_CSV_COLUMNS),
                    lineterminator="\n",
                    extrasaction="raise",
                )
                writer.writeheader()
                for candle in dataset.candles:
                    writer.writerow(_candle_row(dataset.request, candle))
    return target


def read_kline_csv(path: str | Path) -> KlineDataset:
    """Read and validate a gzip CSV produced by :func:`write_kline_csv`."""

    source = Path(path)
    try:
        with gzip.open(source, mode="rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != _CSV_COLUMNS:
                raise DatasetValidationError("dataset CSV header does not match schema")
            rows = list(reader)
    except (OSError, UnicodeError, csv.Error) as exc:
        raise DatasetValidationError(f"cannot read dataset CSV: {source}") from exc
    if not rows:
        raise DatasetValidationError("dataset CSV contains no candles")

    request = _request_from_row(rows[0])
    candles = []
    for index, row in enumerate(rows, start=2):
        if _request_from_row(row) != request:
            raise DatasetValidationError(f"inconsistent dataset metadata on CSV line {index}")
        candles.append(_candle_from_row(row, index))
    return KlineDataset(request=request, candles=tuple(candles))


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    """Return the lowercase SHA-256 digest of a file without loading it all into memory."""

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def build_dataset_manifest(path: str | Path) -> DatasetManifest:
    """Build a deterministic manifest from an on-disk dataset."""

    source = Path(path)
    dataset = read_kline_csv(source)
    request = dataset.request
    gaps = find_kline_gaps(dataset)
    return DatasetManifest(
        schema_version=_MANIFEST_SCHEMA_VERSION,
        data_file=source.name,
        sha256=sha256_file(source),
        market=request.market.value,
        symbol=request.symbol,
        alias=request.dataset_alias,
        interval=request.interval,
        request_start_time_ms=request.start_time_ms,
        request_end_time_ms=request.end_time_ms,
        row_count=len(dataset.candles),
        first_open_time_ms=dataset.candles[0].open_time_ms,
        last_close_time_ms=dataset.candles[-1].close_time_ms,
        gap_count=len(gaps),
        missing_intervals=sum(item.missing_intervals for item in gaps),
    )


def write_dataset_manifest(manifest: DatasetManifest, path: str | Path) -> Path:
    """Write canonical JSON for a dataset manifest."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        asdict(manifest), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    target.write_text(f"{payload}\n", encoding="utf-8", newline="\n")
    return target


def read_dataset_manifest(path: str | Path) -> DatasetManifest:
    """Read a strict dataset manifest from JSON."""

    source = Path(path)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DatasetValidationError(f"cannot read dataset manifest: {source}") from exc
    if not isinstance(payload, dict):
        raise DatasetValidationError("dataset manifest root must be an object")
    try:
        manifest = DatasetManifest(**payload)
    except TypeError as exc:
        raise DatasetValidationError("dataset manifest fields do not match schema") from exc
    if manifest.schema_version != _MANIFEST_SCHEMA_VERSION:
        raise DatasetValidationError(
            f"unsupported dataset manifest schema_version={manifest.schema_version}"
        )
    return manifest


def verify_dataset_manifest(
    data_path: str | Path,
    manifest: DatasetManifest | str | Path,
    *,
    expected_request: KlineDatasetRequest | None = None,
) -> None:
    """Raise if file bytes, metadata, or the expected request differ."""

    source = Path(data_path)
    expected = read_dataset_manifest(manifest) if isinstance(manifest, (str, Path)) else manifest
    if expected.schema_version != _MANIFEST_SCHEMA_VERSION:
        raise DatasetValidationError(
            f"unsupported dataset manifest schema_version={expected.schema_version}"
        )
    if source.name != expected.data_file:
        raise DatasetValidationError(
            f"manifest expects data_file={expected.data_file}, got {source.name}"
        )
    if expected_request is not None:
        request_fields = (
            expected.market,
            expected.symbol,
            expected.alias,
            expected.interval,
            expected.request_start_time_ms,
            expected.request_end_time_ms,
        )
        required_fields = (
            expected_request.market.value,
            expected_request.symbol,
            expected_request.dataset_alias,
            expected_request.interval,
            expected_request.start_time_ms,
            expected_request.end_time_ms,
        )
        if request_fields != required_fields:
            raise DatasetValidationError(
                "dataset manifest request identity/range does not match the expected request"
            )
    actual_digest = sha256_file(source)
    if actual_digest != expected.sha256:
        raise DatasetValidationError("dataset SHA-256 does not match manifest")
    actual = build_dataset_manifest(source)
    if actual != expected:
        raise DatasetValidationError("dataset metadata does not match manifest")


def _validate_candle_identity(candle: Candle, request: KlineDatasetRequest) -> None:
    if candle.market is not request.market:
        raise DatasetValidationError(f"candle {candle.open_time_ms} has the wrong market")
    if candle.symbol != request.symbol:
        raise DatasetValidationError(f"candle {candle.open_time_ms} has the wrong symbol")
    if candle.interval != request.interval:
        raise DatasetValidationError(f"candle {candle.open_time_ms} has the wrong interval")
    if not candle.is_closed:
        raise DatasetValidationError(f"candle {candle.open_time_ms} is not fully closed")


def _validate_candle_values(candle: Candle, step_ms: int) -> None:
    expected_close = candle.open_time_ms + step_ms - 1
    if candle.close_time_ms != expected_close:
        raise DatasetValidationError(
            f"candle {candle.open_time_ms} has close_time_ms={candle.close_time_ms}, "
            f"expected {expected_close}"
        )
    if min(candle.open, candle.high, candle.low, candle.close) <= 0:
        raise DatasetValidationError(f"candle {candle.open_time_ms} has a non-positive price")
    if candle.high < max(candle.open, candle.close):
        raise DatasetValidationError(f"candle {candle.open_time_ms} high is below open/close")
    if candle.low > min(candle.open, candle.close):
        raise DatasetValidationError(f"candle {candle.open_time_ms} low is above open/close")
    if candle.low > candle.high:
        raise DatasetValidationError(f"candle {candle.open_time_ms} low exceeds high")
    if min(
        candle.volume,
        candle.quote_volume,
        candle.taker_buy_base_volume,
        candle.taker_buy_quote_volume,
    ) < 0:
        raise DatasetValidationError(f"candle {candle.open_time_ms} has negative volume")
    if candle.trade_count < 0:
        raise DatasetValidationError(f"candle {candle.open_time_ms} has negative trade_count")
    if candle.taker_buy_base_volume > candle.volume:
        raise DatasetValidationError(
            f"candle {candle.open_time_ms} taker-buy base volume exceeds base volume"
        )
    if candle.taker_buy_quote_volume > candle.quote_volume:
        raise DatasetValidationError(
            f"candle {candle.open_time_ms} taker-buy quote volume exceeds quote volume"
        )


def _candle_row(request: KlineDatasetRequest, candle: Candle) -> dict[str, str | int]:
    return {
        "market": request.market.value,
        "symbol": request.symbol,
        "alias": request.dataset_alias,
        "interval": request.interval,
        "request_start_time_ms": request.start_time_ms,
        "request_end_time_ms": request.end_time_ms,
        "open_time_ms": candle.open_time_ms,
        "close_time_ms": candle.close_time_ms,
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
        "volume": str(candle.volume),
        "quote_volume": str(candle.quote_volume),
        "trade_count": candle.trade_count,
        "taker_buy_base_volume": str(candle.taker_buy_base_volume),
        "taker_buy_quote_volume": str(candle.taker_buy_quote_volume),
        "is_closed": "true",
    }


def _request_from_row(row: dict[str, str]) -> KlineDatasetRequest:
    try:
        return KlineDatasetRequest(
            market=Market(row["market"]),
            symbol=row["symbol"],
            alias=row["alias"],
            interval=row["interval"],
            start_time_ms=int(row["request_start_time_ms"]),
            end_time_ms=int(row["request_end_time_ms"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise DatasetValidationError("invalid dataset request metadata") from exc


def _candle_from_row(row: dict[str, str], line_number: int) -> Candle:
    if row.get("is_closed") != "true":
        raise DatasetValidationError(f"invalid is_closed value on CSV line {line_number}")
    try:
        return Candle(
            market=Market(row["market"]),
            symbol=row["symbol"],
            interval=row["interval"],
            open_time_ms=int(row["open_time_ms"]),
            close_time_ms=int(row["close_time_ms"]),
            open=Decimal(row["open"]),
            high=Decimal(row["high"]),
            low=Decimal(row["low"]),
            close=Decimal(row["close"]),
            volume=Decimal(row["volume"]),
            quote_volume=Decimal(row["quote_volume"]),
            trade_count=int(row["trade_count"]),
            taker_buy_base_volume=Decimal(row["taker_buy_base_volume"]),
            taker_buy_quote_volume=Decimal(row["taker_buy_quote_volume"]),
            is_closed=True,
        )
    except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
        raise DatasetValidationError(f"invalid candle values on CSV line {line_number}") from exc
