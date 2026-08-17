# Binance depth snapshot and sequence-contract revalidation - 2026-07-17

## Scope and claim boundary

This is a primary-source revalidation of the public Spot and USD-M Futures
local-order-book bootstrap contracts used by the infrastructure-only R4B canary.
It does not amend the downloaded Pro protocol, enable Spot Family B, establish an
executable local book, or provide evidence of trading efficacy.

The earlier `binance_public_capture_contract_20260717.md` remains preserved as
the original research record. Its common `limit=1000` depth plan and Spot request
weight are superseded for capture configuration by the current official endpoint
contracts recorded here; its Spot `+1` bridge remains an explicitly independent
DESIGN rule rather than a quotation of the current official bootstrap procedure.

## Primary official sources

- Spot WebSocket payload and local-book procedure:
  https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
- Spot public-market-data-only endpoints:
  https://github.com/binance/binance-spot-api-docs/blob/master/faqs/market_data_only.md
- Spot REST order book limits and request weights:
  https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market#order-book
- USD-M routed WebSocket connection contract:
  https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect
- USD-M public depth payload, including `U`, `u`, `pu`, `ps`, and `st`:
  https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public
- USD-M local-book bootstrap procedure:
  https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
- USD-M REST order book limits and request weights:
  https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#order-book

The two USD-M procedural pages reported `Last modified on July 17, 2026` when
this revalidation was performed.

## Frozen public snapshot requests for the canary

| Venue | Public REST path | Exact limit | Maximum | Request weight |
|---|---|---:|---:|---:|
| Spot | `https://data-api.binance.vision/api/v3/depth` | 5000 | 5000 | 250 |
| USD-M Futures | `https://fapi.binance.com/fapi/v1/depth` | 1000 | 1000 | 20 |

The Spot official local-book procedure explicitly bootstraps with `limit=5000`.
The Spot REST endpoint assigns weight 250 to limits 1001-5000. The Futures
endpoint accepts limits 5, 10, 20, 50, 100, 500, and 1000; limit 1000 has
weight 20. Snapshot requests remain limited to startup, reconnect, and detected
sequence-gap resynchronization, and must be scheduled within exchange rate
limits. A maximum snapshot does not prove that price levels beyond its boundary
are known.

The official Spot procedure says to fetch another snapshot while its
`lastUpdateId` is behind the first buffered `U`, but does not prescribe an
unbounded retry loop. The canary freezes a separate operational bootstrap cap of
three snapshot cycles. Reaching that cap quarantines the book; it is not an HTTP
retry and must not silently continue with an unbridged snapshot.

## Current official sequence distinctions

### Spot

- Diff-depth publishes `U` and `u`; it does not publish `pu`.
- Open the stream and buffer events before requesting the snapshot.
- If snapshot `lastUpdateId` is strictly less than the first buffered `U`, get a
  new snapshot.
- Discard buffered events with `u <= lastUpdateId`.
- The first remaining event must contain `lastUpdateId` in `[U, u]`.
- After synchronization, `u < local_id` is old; `U > local_id + 1` is a gap that
  requires discarding and rebuilding the local book.
- Spot `pu` synthesis is forbidden. The downloaded generic `U/u/pu` requirement
  therefore cannot enable Spot Family B without a venue-specific protocol
  revision and independent adjudication.

### USD-M Futures

- Diff-depth publishes `U`, `u`, and `pu`.
- Open and buffer the stream before requesting the snapshot.
- Discard buffered events with `u < lastUpdateId`.
- The first processed event must satisfy `U <= lastUpdateId <= u`.
- Each subsequent event must satisfy `current.pu == previous.u`; otherwise the
  book is invalid and must be resynchronized.
- Current post-CM-migration payloads append `ps` and `st`; `st=1` denotes UM and
  `st=2` denotes CM. USD-M capture must validate `st=1` and the intended
  symbol/pair before routing an event to a book.

## Safe protocol status

The downloaded file
`artifacts/oracle/2026-07-17/R4b_frozen_experiment_spec_v1.yaml` remains
byte-for-byte unchanged. Spot Family B remains disabled and classified
`INCONCLUSIVE_DATA`; no `pu` is synthesized. The separate Spot bridge in the
existing errata remains `DESIGN` authority and is not relabeled as official.
These snapshot corrections qualify capture infrastructure only and do not
constitute an efficacy or positive-expectancy result.
