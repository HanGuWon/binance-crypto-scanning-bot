# L50 phase-gate qualification — Position Guardian observer/ledger

**Terminal state:** `L50_PHASE_GATE_DETERMINISTIC_REPLAY_PASS`

This is a new append-only qualification receipt. Historical L50-03 through
L50-06 receipts were not edited.

## Repository identity and preserved state

- Branch at qualification start: `codex/public-prospective-ops-20260917`
- HEAD at qualification start: `ebd2e2080370d3a99dd2ae6aeccca8a887e76533`
- Start status: `ahead 18` of `origin/codex/public-prospective-ops-20260917`
- The pre-task status snapshot contained only the following unrelated dirty files and they were preserved:

  ```text
  M artifacts/prospective/futures-bidirectional-v1/historical-summary.json
  M artifacts/prospective/futures-bidirectional-v1/independent-review.json
  M artifacts/prospective/futures-bidirectional-v1/source-freeze.json
  M artifacts/prospective/futures-bidirectional-v1/validation-manifest.json
  M docs/DIRECTIONAL_CANDIDATE_RESEARCH_PROTOCOL.md
  M src/signalbot/config.py
  M src/signalbot/exchange/binance/universe.py
  M src/signalbot/prospective/directional_shadow.py
  M src/signalbot/scanner.py
  M tests/unit/test_directional_shadow.py
  M tests/unit/test_scanner_universe_rotation.py
  M tests/unit/test_universe.py
  ?? tests/unit/test_oci_directional_closeout.py
  ?? tests/unit/test_verify_directional_drive_archive.py
  ?? tools/oci_directional_closeout.py
  ?? tools/verify_directional_drive_archive.py
  ```

Input commit ancestry, oldest to newest:

```text
ac08ef017a7a119e839e7fc26a3613acbb81d7c2  8d181d100c72072b4f0040959f1fd1c8542f7820  Add opt-in managed position adoption
60418d2220e0e444a413fa8c844cd8fa2333e98e  ac08ef017a7a119e839e7fc26a3613acbb81d7c2  Add restart-safe Guardian ledger
ae290a82fd6bd6b9fa4c1097b45723829a655e05  60418d2220e0e444a413fa8c844cd8fa2333e98e  Add read-only Guardian reconciliation
ebd2e2080370d3a99dd2ae6aeccca8a887e76533  ae290a82fd6bd6b9fa4c1097b45723829a655e05  Add bounded Guardian user stream boundary
```

Pre-task Guardian source tree hash from tracked `src/position_guardian/**/*.py` contents:

`9fb086bc4b86b81987b696e64ea2334762bf76662deb516b5adf832a7e0747b4`

Qualified Guardian source tree hash:

`5e126d2e9bd0088ebce201b7f112b13be1f46c79ec5c1f4758c3f870842d62d9`

## Re-audit and scope

The Guardian surface was re-read before editing. Its private exchange adapter still has GET-only methods for server time, positions, position mode, open orders, algo orders, and filters. The domain, ledger, reconciliation, and stream buffer have no order-create, order-modify, order-cancel, position-create, position-increase, or position-reversal exchange path.

The pre-task Guardian suite was **60 passed** under Python 3.12.13. The historical L50-06 receipt reports **59 passed**; that historical discrepancy is preserved here rather than corrected in place. The current suite after the six new qualification tests is **66 passed**.

## Binance contract correction

Verified against the official Binance USDⓈ-M documentation on 2026-09-21:

- production REST: `https://fapi.binance.com`
- testnet REST: `https://demo-fapi.binance.com`
- production private stream: `wss://fstream.binance.com/private/ws/<listenKey>`
- no testnet/private network call was made by the replay

The Guardian read client and contract document now use the canonical demo REST host. A regression test asserts the exact host with an `httpx.MockTransport`. The implementation remains GET-only and adds no trading endpoint or order writer. References: [USDⓈ-M Futures introduction](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/Introduction), [USDⓈ-M User Data Streams](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams), and [USDⓈ-M stream subscription](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams).

## REST race boundary

`mark_rest_resync_complete()` no longer changes health. It raises and directs callers to the explicit `begin_rest_resync()` / `complete_rest_resync()` boundary. The buffer records a bounded ingest fence and requires a non-empty authoritative snapshot ID and cursor. If a buffered event, an event arriving during REST reconciliation, malformed input, or any other stream activity crosses that fence, completion returns `DEGRADED` and retains the crossed IDs. `fence_buffered_events()` is the explicit ledger-facing discard boundary; only a subsequent bounded REST attempt with no crossed activity can restore `CERTAIN`. No wall-clock comparison is used as proof of exchange ordering.

## Deterministic replay

The replay used the real Guardian repository, reconciliation code, stream buffer, and recorded JSON fixtures. It ran twice from independent temporary SQLite databases. The deterministic payload hash was identical in both runs:

- run 1: `c80b82164c6bccfcc6ad5c91f74101a6dd23aefc964e31b3a4903703e91b2b88`
- run 2: `c80b82164c6bccfcc6ad5c91f74101a6dd23aefc964e31b3a4903703e91b2b88`
- scenario count: `18`
- code commit used by replay: `ebd2e2080370d3a99dd2ae6aeccca8a887e76533`
- qualified Guardian source tree hash: `5e126d2e9bd0088ebce201b7f112b13be1f46c79ec5c1f4758c3f870842d62d9`

Every required scenario passed:

| Scenario | Disposition |
| --- | --- |
| normal startup reconciliation | PASS |
| restart with persisted ledger state | PASS |
| partial manual close | PASS |
| manual position increase | PASS |
| full close | PASS |
| side flip | PASS |
| protective-order disappearance | PASS |
| exact duplicate event | PASS |
| duplicate after bounded dedupe-cache eviction | PASS |
| reordered same-type event | PASS; DEGRADED |
| disconnect/reconnect | PASS |
| queue overflow | PASS; DEGRADED |
| listenKey expiry | PASS |
| malformed event | PASS; DEGRADED until REST success |
| REST snapshot while events are buffered | PASS; first result DEGRADED |
| event arriving during REST reconciliation | PASS; first result DEGRADED |
| stale pre-snapshot event remaining in queue | PASS; stale event cannot regress snapshot |
| ordering cannot be proven | PASS; DEGRADED |

Input fixture hashes:

```text
tests/fixtures/binance_user_stream/account_update.json       e1a387a3deb01117714f0114b4d1173c53b36c2307c91f7b9fa668e0a710902e
tests/fixtures/binance_user_stream/listen_key_expired.json   3b61ed614cbc5b4229e92b35cfeb2db509699b4cc4aee6561ece28312813cdd3
tests/fixtures/binance_user_stream/order_trade_update.json   37ea6b42af306e59eb70ed6758f460b45d346b3e37abf4a7133290df2088f1c0
```

## Exchange-write proof

The replay's AST scan found no forbidden write calls in `src/position_guardian`. It also installed an `ExchangeWriteProbe` whose write attributes raise if reached; no probe method was invoked. The exact counters are:

```text
exchange_order_create_calls = 0
exchange_order_modify_calls = 0
exchange_order_cancel_calls = 0
position_increase_calls = 0
position_reverse_calls = 0
total_exchange_trading_write_calls = 0
```

Local SQLite ledger writes occurred as expected and are not exchange writes.

## Validation

- Guardian suite: `66 passed`
- full pytest: `4349 passed, 21 skipped in 1351.30s`; exit 0
- prior L50-06 full baseline: `4343 passed, 21 skipped`
- full Ruff: PASS
- full Pyright: `0 errors, 0 warnings, 0 informations`
- Python 3.12.13 `compileall`: PASS
- replay-focused test: PASS
- `git diff --check`: PASS

The six-test increase over the baseline is the qualification's endpoint, resync, replay, and zero-write coverage. No failure was attributable to this task.

## Owned files

```text
docs/POSITION_GUARDIAN_BINANCE_CONTRACT.md
src/position_guardian/exchange/binance_read.py
src/position_guardian/exchange/binance_user_stream.py
src/position_guardian/reconcile.py
tests/guardian/test_binance_read.py
tests/guardian/test_binance_user_stream.py
tests/guardian/test_l50_phase_gate_replay.py
tools/__init__.py
tools/guardian_l50_phase_gate_replay.py
devlog/_plan/260917_luna-trading-bot-roadmap/receipts/L50-PHASE-GATE.md
```

## Remaining limitations

This gate does not implement listenKey create/keepalive/delete, production socket ownership, live or testnet order writers, autonomous entry, leverage, Freqtrade integration, or a 24-hour live soak. The resync contract is deliberately conservative when Binance cannot provide a provable stream sequence relationship. The next authorized implementation unit after this PASS is `L60-01 — Guardian policy state model extension`; L60 was not implemented here.
