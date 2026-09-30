import json
from dataclasses import asdict
from decimal import Decimal

import pytest

from signalbot.data.microstructure import minute_pressure_snapshot
from signalbot.domain.enums import Market
from signalbot.domain.models import Candle


def minutes(ratio: str = ".60", end: str = "100.2") -> list[Candle]:
    rows = []
    for index in range(3):
        price = Decimal(end) if index == 2 else Decimal("100")
        rows.append(
            Candle(
                market=Market.FUTURES,
                symbol="TESTUSDT",
                interval="1m",
                open_time_ms=index * 60_000,
                close_time_ms=(index + 1) * 60_000 - 1,
                open=Decimal(100),
                high=max(Decimal(101), price),
                low=min(Decimal(99), price),
                close=price,
                volume=Decimal(1),
                quote_volume=Decimal(100),
                trade_count=10,
                taker_buy_base_volume=Decimal(ratio),
                taker_buy_quote_volume=Decimal(ratio) * 100,
            )
        )
    return rows


@pytest.mark.parametrize(
    ("ratio", "end", "text"),
    [
        (".55", "100.2", "매수 체결 우세 · 가격 상승"),
        (".45", "99.8", "매도 체결 우세 · 가격 하락"),
        (".60", "99.8", "매수 체결 우세지만 가격은 상승하지 않음"),
        (".40", "100.2", "매도 체결 우세지만 가격은 하락하지 않음"),
        (".50", "100.2", "매수·매도 체결이 비슷함"),
        (".55", "100.05", "매수 체결 우세지만 가격은 상승하지 않음"),
        (".45", "99.95", "매도 체결 우세지만 가격은 하락하지 않음"),
    ],
)
def test_pressure_and_price_reaction(ratio: str, end: str, text: str) -> None:
    snapshot = minute_pressure_snapshot(minutes(ratio, end), Market.FUTURES, "TESTUSDT", 179_999)
    assert snapshot.available and snapshot.summary == text
    assert snapshot.buy_ratio == float(ratio) and snapshot.last_buy_ratio == float(ratio)
    assert snapshot.lag_ms == 0 and snapshot.start_ms == 0 and snapshot.end_ms == 179_999
    json.dumps(asdict(snapshot), allow_nan=False)
    assert "흡수 확정" not in snapshot.summary and "반전" not in snapshot.summary


def test_quote_weighting_and_last_minute_are_distinct() -> None:
    rows = minutes(".2")
    rows[0] = rows[0].model_copy(
        update={
            "quote_volume": Decimal(1000),
            "taker_buy_quote_volume": Decimal(900),
            "taker_buy_base_volume": Decimal(".9"),
        }
    )
    snapshot = minute_pressure_snapshot(rows, Market.FUTURES, "TESTUSDT", 179_999)
    assert snapshot.buy_ratio == pytest.approx(940 / 1200)
    assert snapshot.last_buy_ratio == 0.2
    assert snapshot.buy_ratio != pytest.approx((0.9 + 0.2 + 0.2) / 3)


@pytest.mark.parametrize(("lag", "available"), [(60_000, True), (60_001, False)])
def test_freshness_boundary(lag: int, available: bool) -> None:
    snapshot = minute_pressure_snapshot(minutes(), Market.FUTURES, "TESTUSDT", 179_999 + lag)
    assert snapshot.available is available
    if available:
        assert snapshot.lag_ms == lag
    else:
        assert snapshot.reason == "stale_bars" and snapshot.buy_ratio is None


@pytest.mark.parametrize("failure", ["two", "gap", "duplicate", "duration", "zero", "buy", "nan"])
def test_unavailable_is_not_neutral(failure: str) -> None:
    rows = minutes()
    if failure == "two":
        rows = rows[:2]
    elif failure == "gap":
        rows[1] = rows[1].model_copy(update={"open_time_ms": 65_000, "close_time_ms": 124_999})
    elif failure == "duplicate":
        rows[1] = rows[0]
    elif failure == "duration":
        rows[1] = rows[1].model_copy(update={"close_time_ms": 119_998})
    elif failure == "zero":
        rows[1] = rows[1].model_copy(
            update={
                "quote_volume": Decimal(0),
                "taker_buy_quote_volume": Decimal(0),
            }
        )
    elif failure == "buy":
        rows[1] = rows[1].model_copy(update={"taker_buy_quote_volume": Decimal(101)})
    else:
        rows[1] = rows[1].model_copy(update={"close": Decimal("NaN")})
    snapshot = minute_pressure_snapshot(rows, Market.FUTURES, "TESTUSDT", 179_999)
    assert not snapshot.available and snapshot.buy_ratio is None
    assert snapshot.price_change_percent is None


def test_future_unclosed_and_other_series_do_not_change_past_snapshot() -> None:
    rows = minutes()
    past = minute_pressure_snapshot(rows, Market.FUTURES, "TESTUSDT", 179_999)
    additions = [
        rows[-1].model_copy(
            update={"open_time_ms": 180_000, "close_time_ms": 239_999, "close": Decimal(99999)}
        ),
        rows[-1].model_copy(update={"is_closed": False}),
        rows[-1].model_copy(update={"market": Market.SPOT}),
        rows[-1].model_copy(update={"symbol": "OTHERUSDT"}),
        rows[-1].model_copy(update={"interval": "5m"}),
    ]
    assert minute_pressure_snapshot(rows + additions, Market.FUTURES, "TESTUSDT", 179_999) == past
