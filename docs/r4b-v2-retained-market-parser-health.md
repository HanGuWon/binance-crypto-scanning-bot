# R4B V2 retained USD-M market parser health

`RetainedUsdmMarketParserHealthCertificateV2` is a local, current-storage M2
prerequisite. It is not an M2 certificate and it does not claim that Binance
emitted no message that the process failed to retain.

The verifier starts from one persisted CLEAN session closure containing the
canonical `usdm_market`/`usdm_public` route-cursor pair. Under the same held
writer lease it reproves the session start and closure, finality fence, exact
WAL/block prefix, CLEAN ledger seal, signed grouped-block chain, and every
BOUNDED market `SOURCE_GAP` endpoint. A `VOID`, unmatched `SOURCE_GAP OPEN`,
authority mismatch, or changed storage artifact fails closed without a result.

It then streams ingest sequence `1..finality_tail_ingest_seq`. Every retained
`usdm_market` member is attested against its current signed block, minted as a
live-reverified M0 leaf, and passed through the frozen strict M1 parser. The
certificate binds:

- the persisted market stop/finalized cursor and common finality prefix;
- the exact plan, logical-stream census, and parser-contract hashes;
- scanned prefix and grouped-block manifest bounds and rolling roots;
- aggregate and per-stream counts for `aggTrade`, `kline_5m`, and
  `markPrice@1s`;
- a rolling sequence root containing every successful M0 leaf and M1 payload
  hash; and
- the count and rolling root of currently reverified BOUNDED market gaps.

State is bounded by the frozen plan stream census, the configured integrity
ledger event cap, and constant rolling-hash state. The current membership API
rechecks signed storage for each market member, so verification cost grows with
the retained prefix; this is an offline closure verifier, not an intrabar hot
path.

Signed content that is still intact but has a strict-M1 failure, an unknown or
unplanned stream, a conflicting local cursor, a row after the persisted market
cursor, a terminal mismatch, or an unobserved planned stream returns
`RetainedUsdmMarketParserHealthNoncertifyingV2`. It retains bounded counts,
roots, and ordered issue codes but makes no parser-health claim.

Both result types structurally keep the following false: upstream message
losslessness, required-source completeness, OI schedule completeness, OI
freshness, M2 certification, strategy readiness, and PnL/order authority. A
BOUNDED gap supplies known local endpoints only; it never means the missing
upstream interval was recovered.
