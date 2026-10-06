# Position Guardian threat model

The Guardian is a later, stop-only process. It protects explicitly assigned
positions and must fail closed when identity, context, or account state is
uncertain.

| Threat | Detect | Fail behavior | Recovery owner |
| --- | --- | --- | --- |
| Leaked API secret | Secret scan, vault audit, unexpected use | Revoke key; disable Guardian | Credential owner |
| Clock skew | Signed request time and local/exchange delta outside bound | Stop private calls; observe only | Guardian operator |
| Replay | Duplicate intent ID or observation cursor | Idempotent no-op; conflict on changed payload | Guardian operator |
| Duplicate order | Existing request/intent identity or exchange reconciliation | Do not retry; reconcile account | Guardian operator |
| Timeout after accept | Timeout without exchange ID | Mark `UNCERTAIN`; query before retry | Guardian operator |
| Partial fill | Before/after quantity differs from request | Keep protection conservative; reconcile remaining size | Guardian operator |
| Manual partial close | Account quantity drops without Guardian receipt | Recompute protection for remaining size or pause | Position owner |
| Manual size increase | Account quantity exceeds armed snapshot | Pause; do not protect the new amount automatically | Position owner |
| Side flip | Observed side differs from armed side | Pause and require re-arming | Position owner |
| Hedge/One-way mismatch | Position mode differs from allowlist | Refuse adoption and writes | Exchange/account owner |
| Stale or open context | Candle completeness, age, and strict-prior checks | Create no stop intent | Guardian operator |
| Database loss | Ledger checksum, missing cursor, restore failure | Observe only; no writes until state is rebuilt | Data owner |
| Scanner outage | Heartbeat or source cursor age | Keep last stop; do not invent a new one | Scanner and Guardian owners |
| Exchange outage | Bounded request failure and health state | Preserve last known stop; mark reconciliation required | Exchange owner |
| Min-notional/tick mismatch | Exchange filters and Decimal quantization | Reject intent; never round into a larger exposure | Guardian operator |

No secret, signature, API key, or signed request body is logged. Logs may use
redacted request IDs, exchange IDs, account aliases, and hashes of canonical
payloads. Raw credentials are accepted only through environment or secret
manager injection and are excluded from exceptions and Discord messages.

## Arming

Changing one live-mode setting cannot enable writes. Live protection requires
two independent records: an operator approval for the exact account/symbol
allowlist and a process-local arming token that is issued only after a fresh
reconciliation pass. Either record expiring returns the process to `observe`.
