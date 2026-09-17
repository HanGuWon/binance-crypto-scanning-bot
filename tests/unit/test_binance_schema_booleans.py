from __future__ import annotations

import pytest

from signalbot.domain.enums import Market
from signalbot.exchange.binance.schemas import PayloadError, parse_payload


def _kline_payload(*, closed: object = True) -> dict[str, object]:
    return {
        "e": "kline",
        "k": {
            "s": "BTCUSDT",
            "i": "5m",
            "t": 0,
            "T": 299_999,
            "o": "100",
            "h": "101",
            "l": "99",
            "c": "100",
            "v": "1",
            "q": "100",
            "n": 1,
            "V": "0.5",
            "Q": "50",
            "x": closed,
        },
    }


@pytest.mark.parametrize("closed", [True, False])
def test_kline_boolean_is_preserved(closed: bool) -> None:
    event = parse_payload(Market.SPOT, _kline_payload(closed=closed))[0]
    assert getattr(event, "is_closed", None) is closed


@pytest.mark.parametrize("closed", ["true", "false", 1, 0, None])
def test_kline_boolean_rejects_truthy_coercions(closed: object) -> None:
    with pytest.raises(PayloadError, match="boolean field x"):
        parse_payload(Market.SPOT, _kline_payload(closed=closed))


def test_agg_trade_boolean_rejects_string_false() -> None:
    payload = {
        "e": "aggTrade",
        "E": 1,
        "T": 1,
        "s": "BTCUSDT",
        "a": 1,
        "p": "100",
        "q": "1",
        "m": "false",
    }
    with pytest.raises(PayloadError, match="boolean field m"):
        parse_payload(Market.SPOT, payload)
