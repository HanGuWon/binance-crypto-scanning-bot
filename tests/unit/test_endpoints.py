from signalbot.domain.enums import Market
from signalbot.exchange.binance.endpoints import build_websocket_plans


def test_spot_plan_uses_combined_stream_and_all_market_ticker() -> None:
    plans = build_websocket_plans(Market.SPOT, ["BTCUSDT"], ["5m"], batch_size=10)
    detailed = plans[0]
    assert detailed.url.startswith("wss://stream.binance.com:9443/stream?streams=")
    assert detailed.streams == (
        "btcusdt@kline_5m",
        "btcusdt@aggTrade",
        "btcusdt@bookTicker",
    )
    assert plans[-1].streams == ("!miniTicker@arr",)


def test_futures_plan_separates_market_and_public_routes() -> None:
    plans = build_websocket_plans(Market.FUTURES, ["BTCUSDT"], ["5m"], batch_size=10)
    by_route = {plan.route: plan for plan in plans if "all-mini" not in plan.name}
    assert by_route["market"].url.startswith("wss://fstream.binance.com/market/stream")
    assert "btcusdt@aggTrade" in by_route["market"].streams
    assert by_route["public"].url.startswith("wss://fstream.binance.com/public/stream")
    assert by_route["public"].streams == ("btcusdt@bookTicker",)


def test_plan_chunks_never_exceed_configured_batch_size() -> None:
    plans = build_websocket_plans(
        Market.SPOT,
        ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        ["1m", "5m", "15m"],
        batch_size=4,
    )
    assert all(len(plan.streams) <= 4 for plan in plans)
