# Trading capability matrix

This repository separates market observation, position protection, and
dedicated-account automation. A recommendation is information; it is not an
exchange order.

| Process | Public market read | Private account read | Stop write | Entry/exit write | Credential owner | Data owner | Kill switch owner | Failure behavior |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Scanner | Yes | No | No | No | None | Scanner operator | Scanner operator | Stop recommendations and retain public evidence; never place an order |
| Position Guardian | Optional public context | Yes, allowlisted account and symbol only | Yes, stop-only and reduce-only | No new entry or exposure | Guardian operator | Guardian operator | Guardian operator | Fail closed to observe mode and require reconciliation |
| Freqtrade auto | Strategy market data | Dedicated trading account only | Yes, as part of its own trade lifecycle | Yes, dedicated-account entry/exit | Freqtrade operator | Freqtrade operator | Freqtrade operator | Stop the strategy and reconcile the dedicated account |

## Meaning of the outputs

- `LONG` is a directional recommendation. On Spot it can map to a buy
  candidate; on Futures it can map to a long candidate.
- `SHORT` is a Futures short recommendation only. On Spot, short-direction
  analysis is represented as a sell/exit warning (`SPOT_EXIT`), never as a
  Spot short entry.
- `NO_ENTRY` means that the current evidence does not authorize a new
  position. `PUMP_RISK` and `CRASH_RISK` are warnings, not entries.
- A Position Guardian may protect a position that was explicitly assigned to
  it. It cannot discover or adopt a position implicitly, increase size, flip
  side, or create a new exposure.
- Freqtrade automation uses a dedicated account and lifecycle. It does not
  take over manually opened positions.

The scanner has no private Binance client and no order endpoint. The current
paper-position lifecycle is an alert and research aid; its technical exits do
not create exchange orders.

## Ownership and incident rule

The process owner who holds a credential also owns its rotation and revocation.
The process that writes a database owns its migrations, backups, and replay
checks. The kill-switch owner can disable the process without changing code.
An exchange or storage outage stops the affected action path and leaves the
last observed state for reconciliation; it does not turn an uncertain result
into a fresh order.
