# Trading mode promotion

Protection and entry automation use separate state axes. A mode on one axis
does not grant permissions on the other.

## Position protection axis

`observe -> shadow -> testnet -> live_protection`

- `observe`: public or private reads may be inspected; no stop write.
- `shadow`: Guardian computes intents and receipts without exchange writes.
- `testnet`: the exact adapter and reconciliation flow is exercised against
  the configured test environment.
- `live_protection`: stop-only writes for an explicitly armed allowlist.

## Entry automation axis

`backtest -> dry_run -> testnet -> live_entry`

- `backtest`: historical, public-data research only.
- `dry_run`: live market data may produce simulated decisions; no order write.
- `testnet`: dedicated automation is tested against the configured test
  environment.
- `live_entry`: Freqtrade may manage a dedicated account under its own
  strategy and limits.

The defaults are `observe` and `backtest`. Promotion requires passing the
relevant tests, reconciliation checks, bounded retry checks, and incident
review. A missing receipt, stale context, unresolved account mismatch, or
uncertain exchange response blocks promotion and returns the affected axis to
its previous safe mode.
