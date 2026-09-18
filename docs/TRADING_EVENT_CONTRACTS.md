# Trading event contracts

These contracts define the boundary between the public scanner, future
position protection, and dedicated-account automation. They are schemas and
identity rules, not permission to place orders.

## Common rules

- Persist timestamps as UTC Unix milliseconds. Human-facing output includes
  both UTC and Asia/Seoul (KST).
- Prices and quantities at an exchange boundary use exact `Decimal` values;
  binary floating point is not an exchange contract.
- A nullable field must state whether `null` means **unknown** or **not
  applicable**. It must not silently mean both.
- A repeated identity with byte-equivalent payload is an idempotent replay. A
  repeated identity with different payload is a hard conflict that requires
  operator attention.
- A timeout without an exchange ID is `UNCERTAIN`, because the exchange may
  have accepted the request.

## RecommendationEnvelope

The scanner projects a `SignalDecision` into this read-only envelope:

| Field | Meaning |
| --- | --- |
| `event_id` | Deterministic ID of `source_event_id` plus projection version |
| `source_event_id` | Immutable source decision identity |
| `action` | `LONG`, `SHORT`, or `NO_ENTRY` |
| `kind` | `ENTRY_CANDIDATE`, `EXIT_WARNING`, `RISK_WARNING`, or `HOLD` |
| `market`, `symbol` | Spot/Futures and normalized symbol |
| `decision_time_ms`, `expires_at_ms` | UTC Unix milliseconds |
| `reasons` | Evidence retained from the source decision |
| `blockers` | Failed gates or reasons the action cannot be an entry |
| `invalidation` | Directional invalidation price; `null` means no invalidation was supplied |
| `evidence_strength` | Existing rule-strength score, never a probability |
| `score_kind` | `RULE_STRENGTH` or `NOT_APPLICABLE` |
| `evidence_tier` | Starts at `UNVALIDATED` until research governance promotes it |
| `rule_version` | Source rule version |

Identity example:

```text
event_id = sha256("recommendation|<source_event_id>|recommendation-projection-v1")[:24]
```

`Spot + SHORT + ENTRY_CANDIDATE` is invalid. Spot short-direction analysis
must remain an exit warning. `RISK_WARNING` is never an entry. An expired
envelope is not eligible for ranking or reuse.

## ProtectionContext

The scanner creates a protection context only from a fully closed candle. The
current wire contract is `protection-context-v1` and contains:

```text
context_id, context_version, source_decision_clock_id, market, symbol,
primary_interval, candle_open_time_ms, candle_close_time_ms,
source_candle_closed, close, atr, confirmed_swing_support,
confirmed_swing_resistance, trend_state, consecutive_trend_failure_count,
data_completeness, context_freshness_ms, higher_timeframes
```

An open candle, a future timestamp, incomplete required data, or stale
higher-timeframe context produces no stop intent. `null` structure boundary
means unknown; it is not the same as a strategy that has no boundary.

Guardian consumers must inspect `context_version` before using the payload.
Version strings have the form `protection-context-v<major>`; the current
consumer contract supports major version `1` only. Older `v0`, newer `v2+`,
missing versions, and malformed version strings are rejected as unsupported
before policy evaluation. Merely constructing a `ProtectionContext` model does
not grant compatibility with an unsupported major version.

Unknown top-level fields on a `v1` payload are transport extensions and are
ignored by a `v1` consumer. They must not change the v1 semantic payload or its
`context_id`. A field that changes identity, stop-policy meaning, required
validation, or safety semantics requires a new major version. Known required
v1 fields still undergo normal schema and deterministic-ID validation, so a
missing, malformed, or identity-conflicting known field is rejected.

## ManagedPositionSnapshot

The Guardian's observed position shape is:

```text
account_alias, symbol, position_side, quantity, entry_price,
initial_stop, active_stop, extrema, observation_cursor
```

`quantity = null` means the account response did not establish quantity;
`initial_stop = null` means no initial stop is known. A position snapshot is
not an instruction to adopt the position. Adoption requires an explicit
allowlist and an arming record.

## StopUpdateIntent

An intent contains:

```text
intent_id, prior_stop, proposed_stop, quantity, mode_semantics,
source_context, reason, policy_version
```

The ID includes the account alias, symbol, position side, observation cursor,
prior stop, proposed stop, quantity, and policy version. A stop update is
monotonic and reduce-only for the assigned side. It cannot increase quantity,
flip side, or create a position.

## ExecutionReceipt

Execution evidence separates request identity from exchange identity:

```text
request_id, intent_id, exchange_order_id, status,
before_account_snapshot, after_account_snapshot
```

`status` is terminal only when the exchange identity and resulting account
state are known. A transport timeout without `exchange_order_id` is
`UNCERTAIN`; reconciliation must query the account before any retry. A
successful response without a verifiable exchange identity is also uncertain.
