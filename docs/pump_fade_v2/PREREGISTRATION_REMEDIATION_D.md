# Pump-fade v2d engineering correction record

This is a post-exposure engineering record, not a prospective preregistration or
confirmatory analysis. Historical outcomes were opened before v2c and remain
exposed. v2b and v2c policies, freezes, manifests and replay results are
immutable. v2d changes the causal separation of funding cap-risk clearance and
R5 confirmation, adds an executable offline ladder-scenario engine, gates
terminal archive pruning on event/alert outcome receipts, and hashes the complete
local executable import closure.

## Frozen policy

- Active identifier: pump-fade-v2-20261007-remediation-d.
- Primary adverse mark squeeze: 15%; inclusive >= boundary; 20% is sensitivity.
- Cap WAIT clears after three spaced, observed normalizations following the
  latest cap hit in the rolling 24-hour window. This only clears cap risk.
- R5 remains event-specific: three spaced normalized samples at/after event
  admission after a cap hit. Pre-event samples cannot earn R5.
- Interval shortening remains WAIT until observed restoration to the original
  pre-shortening interval.
- Primary ladder allows at most two risk-increasing additions, each at most half
  the frozen initial quantity, subject to the same frozen stop and USDT risk
  budget. First-fill-only is a comparator; three additions are sensitivity;
  eight additions are a legacy comparator only.
- Event and each alert origin retain independent 4h/24h outcomes. Missing marks,
  quotes or verified settlement coverage remain pending, censored or unavailable.
- Capture, Discord shadow delivery, scanner changes, production orders,
  deployment and forward evaluation remain disabled.

## Evidence status and limitations

The scenario comparator executes only against explicit receipt-stamped
synthetic/test or separately supplied paths. No private account export was read.
It does not create empirical fill, funding, q99-tail, noninferiority, Holm or
strategy efficacy evidence. The already exposed v2c public-kline proxy remains
historical development evidence and is not rerun or overwritten by this record.

The exact source-plan authority remains SHA-256
bb0504050b157c0456fe309731a4ea2954cbbb9a1d2e4773d21edbbab522e986.
The matching source-verification receipt is retained at
docs/pump_fade_v2/SOURCE_VERIFICATION_20261007.json.

## Operational status

Disabled local package only. No source changes were applied to the production
scanner or Guardian; no live capture or external messaging occurred. A
compatible authorized Windows runner must execute the full repository
verification before publication. Sonnet owns human review of the local diff and
any later commit or publication decision.
