# L5 — one-shot sealed holdout

Preconditions: L1 freeze commit and `HOLDOUT_SEAL.json`; successful L2–L4b development runs; exact selected model SHA or rules-only decision; current holdout guard tests pass. Unseal with the exact L1 freeze commit SHA, run once, then lock the result. No tuning, rerun to adjust, or rescue on these outcomes. Record command, environment, code/config/input SHA-256, timestamp and all prespecified metrics/CIs, including missing/failed cells and reason.

Holdout is all eligible USD-M USDT perpetuals, including delisted, `2026-07-01T00:00:00Z` through `2026-10-06T23:59:59Z`. Secondary analysis excludes operator-traded symbol-days (P3) and labels that subset as hypothesis-exposed. The sealed directory is not included in normal analysis globs, unit-test fixtures, or any read path before authorization. Any model with pretraining cutoff later than holdout start is not confirmatory for this holdout.

If guard, seal, freeze SHA, or one-shot run identity fails, report L5 not run/invalid and retain the seal; do not make a new selection from holdout outcomes.
