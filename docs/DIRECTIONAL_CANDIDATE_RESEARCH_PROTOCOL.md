# Futures bidirectional research protocol

The successor candidate is `futures-bidirectional-v1` and is defined by
`config/research.futures-bidirectional.v1.yaml`. It tests the existing causal
breakout/breakdown family in both directions while keeping the existing R2 and
Phase-R/Phase-S artifacts immutable.

The protocol freezes the candidate before results are read:

- 5m fully closed candles with strict-prior 15m and 1h context.
- `BREAKOUT_LONG` and `BREAKDOWN_SHORT` are separate directional hypotheses.
- Entry is the next bar open; technical exit is capped at 72 bars.
- Outcomes are recorded at 1, 3, 6, and 12 bars with the preregistered 26 bps
  research round-trip cost. This is an analysis cost, not an order permission.
- The universe and three contiguous time splits are copied into the successor
  contract by identity, rather than changing the frozen R2 configuration.
- Exact observed BBO is required for a live gate. Historical lack of BBO is an
  explicit limitation and produces an inconclusive research condition where it
  applies.

The implementation sequence is:

1. Freeze the preregistration and its configuration hash.
2. Build a manifest containing source, data-authority, universe, and trial
   registry identities before reading outcomes.
3. Run bounded deterministic fixture replay. Duplicate IDs are idempotent only
   when their payload is identical; changed payloads fail the replay.
4. Run the external historical campaign only after fixture parity passes.
5. Produce a cost-aware, direction-separated analysis and an independent review
   receipt. A failed screen yields `NO_QUALIFIED_CANDIDATE`.
6. Activate prospective shadow observation without orders or Discord messages,
   then produce a forward receipt after the preregistered gate is open.
7. Evaluate promotion. Both LONG and SHORT must meet the minimum evidence,
   data-quality, operational-health, and independent-review requirements.

The code in `signalbot.prospective.directional_candidates` implements the
identity, replay, and fail-closed adjudication contracts. It does not create
historical results, inspect sealed Phase-S values, change the recommendation
registry, or place orders. Only a receipt with `PROMOTE` may authorize a later
versioned recommendation wiring change, and even that does not grant order
permission.

## Executable validation command

The Phase 1 runner uses the existing manifest-verified `data/backtest` input and
does not download the Google Drive cold-archive shards:

```powershell
$env:UV_PROJECT_ENVIRONMENT = 'D:\\Binance bot-2\\.venv-codex'
uv run signalbot prospective-directional-validate `
  --config config/settings.example.yaml `
  --preregistration config/research.futures-bidirectional.v1.yaml `
  --spec config/backtest.5m.r2-c0-corrected.yaml `
  --data-dir data/backtest `
  --output-dir artifacts/prospective/futures-bidirectional-v1
```

After the receipt set is complete, run the read-only independent review:

```powershell
uv run signalbot prospective-directional-review `
  --preregistration config/research.futures-bidirectional.v1.yaml `
  --receipt-dir artifacts/prospective/futures-bidirectional-v1
```

The review writes `independent-review.json`. `BLOCKED` is an expected result
until the forward shadow window, sidecar comparison, and independent evidence
are all complete; it must not be treated as a promotion approval.

The command freezes the source tree, verifies each Futures 5m and funding
manifest, runs the two directional hypotheses with the strict-prior HTF policy,
and writes bounded JSON receipts. It does not copy raw candles into the output
directory. Historical spread/BBO remains a proxy; therefore a valid historical
screen cannot be described as a passing live observed-BBO gate.

The Freqtrade integration under `integrations/freqtrade/` remains a dry-run
sidecar. Its static-universe strategy is useful for a candle-family comparison,
but it cannot substitute for this scanner's source/data hash, fixture replay, or
forward evidence. If the `freqtrade` executable is unavailable, the campaign
receipt must say `NOT_INSTALLED` rather than implying parity.

## Successor forward shadow

The successor observer is opt-in and attaches only to the Futures runtime after
the incumbent signal, paper, and Discord paths have completed. Add these fields
to a private runtime config after computing the current source identity and
choosing a future UTC activation boundary:

```yaml
shadow:
  directional_observation_enabled: true
  directional_campaign_id: futures-bidirectional-20260918-a
  directional_source_identity: worktree-source-v1:<64-hex-source-hash>
  directional_campaign_created_at_ms: <registration-time-ms>
  directional_activation_ms: <future-boundary-ms>
```

The observer writes immutable evidence rows under the existing shadow campaign
repository with schema `directional_shadow_observation_v1`. It records both
`BREAKOUT_LONG` and `BREAKDOWN_SHORT` raw closed-candle triggers, strictly-prior
HTF acceptance, and explicit `production_entry: false`, `order_placement: false`,
and `discord_delivery: false` markers. The incumbent observer and its campaign
remain separate. A campaign cannot be called complete until the future boundary
has elapsed and the independent review recomputes the stored hashes.
