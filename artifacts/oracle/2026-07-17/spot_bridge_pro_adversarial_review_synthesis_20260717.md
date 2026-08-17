# GPT Pro adversarial review synthesis — Spot bridge and capture safety

## Retrieval identity

- Conversation:
  `https://chatgpt.com/g/g-p-69a9f92954288191a063fd1eea40b983-gasanghwapye-teureiding/c/6a599091-df04-83ee-a7b7-f6475d3de4bc`
- Web-AI session: `01KXQKM35YB4MZJCM6PMKY4HSQ`
- Prompt SHA-256:
  `9b89b525d059974a12bfae69c11101ff7feb2a37de48f2003907608cc6302fb5`
- Completed at: `2026-07-17T08:55:46.509Z`
- Captured response characters: `15,788`
- Provider UI mode observed: `Pro`
- User-designated model: GPT 5.6 SOL Pro. The browser transcript does not
  independently attest the provider's internal backend build identifier.
- Local transcript evidence:
  `C:/Users/user/.browser-agent/sessions/01KXQKM35YB4MZJCM6PMKY4HSQ/artifacts/transcript.md`

The review explicitly stated that it could not independently access the raw
failed-smoke records in the ChatGPT project library. Its numerical treatment of
Spot 9/9 and Futures 3/3 therefore relied on the supplied evidence summary. The
local Codex audit separately verified those raw records and hashes.

## Pro verdict

```text
REVIEW_STATUS = APPROVE_WITH_P0_AMENDMENTS
PROFITABILITY_STATUS = NOT_EVALUATED
```

The review approved the venue-specific bridge correction:

- Spot discards `u <= L` and bridges on `U <= L+1 <= u`.
- An empty retained buffer is `WAITING_FOR_BRIDGE`, not immediately stale.
- Spot `U > L+1` is stale/gapped.
- USDⓈ-M Futures remains `u < L`, `U <= L <= u`, followed by `pu`
  continuity; it must not be normalized to the Spot rule.
- The failed session must remain immutable and every patched live run must use a
  new root and authority.
- A scoped three-symbol Spot `exchangeInfo` request is appropriate for this
  fixed canary, provided the response is complete and contains the exact symbol
  set.
- A 250-weight depth-snapshot circuit breaker, proactive admission pacing,
  used-weight evidence, and 429/418 fail-closed behavior are release gates for
  a long canary.
- The smoke may establish sequence/replay/payload/rate-limit correctness only;
  it cannot establish hit rate, net expectancy, or profitability.

## Amendments applied or scheduled

Applied in this workstream:

- Spot online and offline target changed to `L+1`; discard rule retained.
- No-retained-event path remains bounded waiting; no Spot `pu` is invented.
- Spot/Futures boundary tests and failed-session empirical issue record added.
- Spot `exchangeInfo` fixed to exact canary symbols; the 16 MiB cap was not
  increased.
- `BODY_LIMIT` is a report-level hard failure and is being made an immediate
  capture quarantine after its evidence record is persisted.
- Spot snapshots are being globally paced; successful Spot responses must carry
  one canonical `x-mbx-used-weight-1m` value, with frozen high-water quarantine.
- The current exchange rate-limit contract is checked rather than silently
  retuned.

Still required before a long efficacy holdout:

- factor the shared Spot/Futures discard and bridge classification into one
  pure online/offline reducer and add state/reason parity fixtures;
- run a fresh live startup smoke long enough to cross a REQUEST_WEIGHT minute
  boundary, then the independent 24-hour non-efficacy canary;
- add venue-clock offset/discontinuity and causal availability gates;
- retain Spot Family B as disabled until its decision-time timestamp contract is
  separately frozen;
- collect untouched prospective PAPER/BBO evidence before any profitability
  conclusion.

## Deliberate project-specific choices

The Pro response suggested `showPermissionSets=false`. This was not adopted:
the exact three-symbol response was only 15,149 bytes and preserving the full
metadata semantics is safer than removing fields for another small size gain.

The Pro response also suggested using live `exchangeInfo.rateLimits` directly.
For the frozen canary, silent dynamic retuning is rejected. The current limit is
frozen and the observed `exchangeInfo` value must match it; drift quarantines
the session for an explicit protocol revision.
