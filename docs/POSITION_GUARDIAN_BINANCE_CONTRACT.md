# Position Guardian Binance USDⓈ-M read contract

Verified against the official Binance Developer Documentation on 2026-09-20.
The Guardian contract is deliberately limited to read-only REST calls. This
task adds no order placement, amendment, cancellation, or user-stream writer.

## REST environment

| Environment | Base URL |
| --- | --- |
| production | `https://fapi.binance.com` |
| testnet | `https://testnet.binancefuture.com` |

The production market-data and account documentation identifies
`https://fapi.binance.com` as the USDⓈ-M REST host. The testnet host is kept as
a configuration boundary for later testnet qualification; this package does
not contact either host during tests.

## Read endpoints

| Purpose | Method and path | Security | Parsed contract |
| --- | --- | --- | --- |
| server clock | `GET /fapi/v1/time` | NONE | integer `serverTime` in milliseconds |
| account positions | `GET /fapi/v3/positionRisk` | USER_DATA, signed | symbol, position side, amount, entry/mark price, PnL, update time |
| position mode | `GET /fapi/v1/positionSide/dual` | USER_DATA, signed | boolean `dualSidePosition`; `true` is Hedge Mode and `false` is One-way Mode |
| normal open orders | `GET /fapi/v1/openOrders` | USER_DATA, signed | open stop/protective and other order rows |
| algo open orders | `GET /fapi/v1/openAlgoOrders` | USER_DATA, signed | conditional TP/SL/trailing rows |
| symbol filters | `GET /fapi/v1/exchangeInfo` | NONE | `PRICE_FILTER.tickSize`, `LOT_SIZE.stepSize/minQty`, symbol status |

The position endpoint is treated as a REST snapshot and is intended to be
paired with `ACCOUNT_UPDATE` in the later L50-06 stream task. The open algo
order endpoint is parsed as an array, matching the current official example.

## Signing and timing

Signed requests include `timestamp` and `recvWindow` in milliseconds and send
the API key in `X-MBX-APIKEY`. The signature is HMAC-SHA256 over the exact
URL-encoded query string, with the secret as the HMAC key. Parameter order is
preserved by the implementation so the signed string is also the transmitted
query string.

The implementation uses a bounded retry schedule for transport failures,
HTTP 418/429, and HTTP 5xx responses. `asyncio.CancelledError` is propagated
without retry. Authentication failures and malformed payloads are terminal.
Binance error `-1021` is reported as temporary clock skew so the later runtime
can resynchronize before considering a position manageable.

Secrets never appear in error messages, parsed models, recorded fixtures, or
test output. Recorded JSON fixtures are synthetic and contain no account
credentials or live account data.

## Official references

- [USDⓈ-M Futures market data REST API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)
- [USDⓈ-M Futures account REST API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account)
- [USDⓈ-M Futures trade REST API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)
- [Binance REST API general information and signed endpoint security](https://developers.binance.com/en/docs/products/spot/rest-api)
