# Prospective Shadow Campaign — er_context_v1

## Status

`READY_FOR_CONTINUOUS_START` + `SHADOW_SUCCESSOR_ONLY`

This campaign is **not started** from this environment. This host is not the
intended 24/7 observation host, so no prospective time has begun. Starting the
campaign requires the safe start step below on the deployment host and a fresh
activation timestamp that is strictly after the source/config/policy freeze.

## Frozen contract

- Production `entry_policy` must remain `r2_pit_htf_exec`.
- Shadow observer is enabled separately via `shadow.observation_enabled: true`
  and `observation_schema_version: shadow_observation_v1`.
- Decision clock: completed 5m primary close only.
- Families: Spot `BREAKOUT_LONG`; Futures `BREAKDOWN_SHORT`.
- Strict-prior `15m` and `1h` contexts required.
- Shadow parameters: ER20 >= 0.40, anti-chase <= 0.5 ATR, round-trip cost seed
  26 bp, cost-headroom multiple 2, BTC-opposition veto, relative-volume
  participation, shared fresh-BBO contract.
- The policy SHA-256 (computed by `shadow_policy_identity`) and the config
  SHA-256 bind each observation's provenance. A change to any frozen parameter
  forces a new policy version and a new campaign.

## What the observer does

At each mature 5m close it evaluates R2 and the shadow on the SAME causal
feature/context cutoff, persists one idempotent comparator observation per raw
C0 opportunity, and maintains a per-close coverage ledger whose invariant
(raw C0 count == comparator rows persisted) proves no silent holes. It is
informational-only and has no promotion, state-machine, PAPER, or Discord path.

## Safe start command (deployment host, alert-only)

```bash
uv run signalbot validate-config --config config/settings.yaml
uv run signalbot run --config config/settings.yaml
```

Before enabling, provide the continuous host's config with
`shadow.observation_enabled: true`, create the campaign preregistration with a
post-freeze activation timestamp, and verify at least one durable post-activation
coverage cell before marking the campaign ACTIVE.
