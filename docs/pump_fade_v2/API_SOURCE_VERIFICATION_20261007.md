# Binance USD-M public API verification — 2026-10-07

This receipt records the current official public-interface checks used by the Pump-fade v2 collector design. It is an engineering compatibility check, not strategy evidence.

- Binance's 2026 USD-M change log records the market-stream path split introduced in March 2026: public market streams identify `/market` and `/public` URL paths, and the April update records the old WebSocket base-URL shutdown date. The repository's existing routed constants remain authoritative for connection construction; Pump-fade adds only allowlisted public stream names.
- Current USD-M REST market-data documentation exposes unauthenticated `GET /fapi/v1/exchangeInfo`; symbol records include `onboardDate` and `status`. Pump-fade stores the local response receipt as the point-in-time observation authority and never projects a current snapshot backward into a historical event.
- Current USD-M REST market-data documentation exposes `GET /fapi/v1/fundingInfo` with adjusted funding cap, adjusted floor and `fundingIntervalHours`. The documentation explicitly describes it as returning symbols whose cap/floor/interval had an adjustment. Therefore a missing symbol row is **not** converted into a historical/default-cap fact; eligibility remains unavailable without an applicable observed cap rule.
- The collector continues to use public `GET /fapi/v1/openInterest`, `GET /futures/data/openInterestHist`, `GET /fapi/v1/premiumIndex`, `GET /fapi/v1/depth`, `GET /fapi/v1/klines`, closed 5m kline, mark-price, book-ticker, depth and liquidation streams. All exchange event timestamps and local receipt/availability clocks remain distinct.
- `!forceOrder@arr` remains modeled as a **censored observed sample**, never a complete liquidation total. Stream silence is not an observed zero. A forced `BUY` is interpreted only as evidence of a short being force-closed; it is not directional proof for the next price move.
- No global/top-trader long-short ratio is used to make a directional verdict.

Official pages checked:

- `https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data`
- `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Liquidation-Order-Streams`
- `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Mark-Price-Stream`
- `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams`
- Binance derivatives change log entries dated 2026-03-05 and 2026-04-02 for the routed WebSocket migration.

No authenticated endpoint, account key, private stream, order endpoint or account-setting endpoint is part of this collector.
