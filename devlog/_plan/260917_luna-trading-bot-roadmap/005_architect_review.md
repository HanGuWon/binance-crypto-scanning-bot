# Read-only architecture review record

## Review scope

The reviewer checked the master roadmap and the directional, Guardian, Freqtrade, and auto-entry phases for dependency order, service boundaries, missing fail-closed behavior, promotion gates, and LUNA task size.

## Incorporated findings

- Canonical recommendation core now precedes Guardian context work.
- Auto-entry paper work now requires directional promotion, a selected Guardian policy, Guardian testnet qualification, and Freqtrade parity PASS.
- Guardian private event-stream reconciliation is part of the L50 gate before policy shadow work.
- Freqtrade export is bound to the exact L35 candidate/config version receipt.
- Guardian policy metrics and hard rejection thresholds must be preregistered before outcome computation.
- Guardian testnet writes reject production hosts/accounts and require a capability token.
- Standalone protective-stop cancellation is removed from the writer capability. Cancellation is limited to verified replacement, confirmed closure, or explicit release.
- Stop replacement freezes an unprotected-time deadline and a bounded recovery/escalation path before testnet execution.
- Auto-entry requires fresh execution-quality evidence; unavailable or stale evidence produces `NO_ENTRY`.
- Auto-entry freezes the exact maximum stopless interval and emergency action before testnet execution, with zero breaches required.
- Broad campaign tasks were split into harness/schema, deterministic fixture, external execution, and receipt adjudication tasks.
- Guardian shadow policy and overnight paper phases now have separate campaign execution and adjudication tasks.
- Auto-entry requires the fully wired L35-09 candidate, and L35/L90 candidate/config hashes must match exactly.
- Every Guardian write now requires a fresh reconciled position/order snapshot and a non-degraded state.

## Deliberate boundary choices

- Guardian work does not wait for all recommendation delivery tasks. It starts only after the canonical recommendation core is fixed, then proceeds in parallel with API/Discord work. This preserves the prior requirement that manual-position protection can advance without waiting for every recommendation presentation feature.
- L80 limited-live Guardian activation is not a prerequisite for auto-entry paper research because L80 itself requires separate user authorization. L70 Guardian testnet qualification is required before auto-entry paper work, so protection engineering remains ahead of new-entry execution without silently granting live-protection authority.

## Reflection result

After the changes above, the architecture keeps three independent promotions: recommendation, manual-position protection, and new-entry automation. A pass in one cannot grant permissions in another.
