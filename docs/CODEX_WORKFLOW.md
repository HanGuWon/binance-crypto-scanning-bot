# Codex implementation workflow

`AGENTS.md` contains non-negotiable repository invariants. `PLAN.md` is the durable checklist. Work on one dependency-ordered milestone per branch and require a separate skeptical review before merging.

For implementation tasks, direct Codex to inspect the repository first, implement only the named milestone, add success/failure/boundary tests, run every verification command, and report residual risks. Never grant a task permission to introduce production order execution or Binance credentials into this repository.

For review tasks, use an independent session and ask it to try to disprove correctness, focusing on future-data leakage, unclosed candles, higher-timeframe alignment, duplicate WebSocket events, reconnect gaps, timestamp units, bounded queues, Discord idempotency, exception handling, and live/replay parity. Every finding should include a concrete failure scenario and regression test.
