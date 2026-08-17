# Verified Primary References

Verified 2026-07-14. External provider contracts may change; review fixtures and endpoint constants during upgrades.

- Binance Spot WebSocket Market Streams: https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~
- Binance USDⓈ-M WebSocket connection rules: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect
- Binance USDⓈ-M routed-endpoint migration: https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice
- Binance USDⓈ-M public high-frequency streams: https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public
- Binance USDⓈ-M market streams: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/All-Market-Mini-Tickers-Stream
- Binance Spot changelog: https://developers.binance.com/en/docs/products/spot/CHANGELOG
- Discord Webhooks: https://docs.discord.com/developers/resources/webhook
- Freqtrade lookahead analysis: https://www.freqtrade.io/en/stable/lookahead-analysis/
- Freqtrade recursive analysis: https://www.freqtrade.io/en/stable/recursive-analysis/
- Freqtrade backtesting: https://www.freqtrade.io/en/stable/backtesting/

Design implications: combined streams wrap `{stream,data}`; stream symbols are lowercase; current Futures regular and high-frequency public data use separate routed bases; Futures connections have a finite 24-hour life and 1,024-stream limit; closed-kline state gates candle decisions; Freqtrade recommends lookahead and recursive checks before dry/live use.
