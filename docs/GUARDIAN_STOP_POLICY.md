# Guardian stop policy

`guardian-stop-policy-v1` is the L60-01 policy-state contract for the existing
read-only `StopUpdateIntent` path. It creates an intent only; it does not call
an exchange endpoint or place an order.

The policy keeps three values separate for an adopted position:

- `original_risk_stop` is the known stop from the original position. R-based
  activation is allowed only when this value exists.
- `protection_floor` is the least acceptable stop after adoption. A profitable
  manual position may have a floor above break-even without inventing an
  initial risk or an R value.
- `active_stop` is the stop currently observed on the position. Every new
  intent must tighten it monotonically.

The policy states are `INITIAL_RISK`, `TREND_PROGRESS`,
`PROFIT_PROTECTION`, `TREND_WEAKENING`, and `STALE_OR_UNCERTAIN`.

`INITIAL_RISK` covers the period before the known-R activation threshold. With
no original risk stop, the planner uses the configured ATR activation threshold
and never estimates R. `TREND_PROGRESS` uses the existing ATR trailing rule.
`PROFIT_PROTECTION` is selected when a known-risk position's proposed stop has
passed entry in the protective direction, or when an adopted manual position's
floor is being preserved in profit. `TREND_WEAKENING` requires both a confirmed
structure stop and an explicit momentum-weakening signal; a structure value by
itself cannot trigger this state. `STALE_OR_UNCERTAIN` produces no intent.

For LONG positions, the proposed stop is at least the protection floor, above
the active stop, and strictly below the current reference price. For SHORT
positions the inequalities are mirrored. If the floor is already at or beyond
the current reference price, the planner suppresses the intent rather than
creating an invalid amend. A stop calculated from one fully closed candle is
marked `effective_from_next_candle`; this policy layer never applies it
retroactively inside the candle that supplied the observation.

The intent ID includes the policy version, position identity, observation time,
policy state, original risk, protection floor, active stop, and proposed stop.
Repeating the same snapshot and policy inputs therefore produces the same ID.
The intent remains `reduce_only=True`, `close_position=False`, and
`order_placed=False`.
