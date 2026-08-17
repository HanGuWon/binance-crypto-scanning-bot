# R4B V2 Historical Conflicted-Majority Comparator Adapter

Status: pre-outcome, historical-only, non-promoting, uncalibrated. This
contract was frozen after the outcome-blind topology census and before this
adapter was allowed to read any forward candle, funding return, fixed-horizon
return, or technical-exit result.

## Purpose and population

The source consensus remains unchanged. Its clean `3/3` and `2 + neutral`
admission rule remains unchanged. This adapter creates a separate descriptive
comparison population from rows for which the frozen census says exactly:

- topology is `CONFLICTED_BULLISH_2_1_0` or
  `CONFLICTED_BEARISH_1_2_0`;
- two families support the source LONG/SHORT side and one opposes it;
- `conflicted_comparator_eligible=true`;
- source `admitted=false`, `clean_primary_audit_eligible=false`, and
  `conflicted_comparator_outcome_authorized=false`.

The source event ID, anchor ID, consensus payload/canonical hashes, topology
hashes, source-row hashes, three leaf calculation/source-slice hashes,
cross-peer hashes, decision clock, side, price, invalidation, ATR, and frozen
execution-contract hash are retained. The adapter never rewrites the source
event ID or sets the source authorization flag to true. Adapter authorization
is a separate hash-bound envelope.

## Authorization gate

No row is released for outcome evaluation unless all of the following are
provided and authenticated:

1. the complete outcome-blind census manifest and its externally frozen hash;
2. the original experiment contract hash;
3. the exact topology pre-outcome amendment hash;
4. this adapter contract and its externally frozen hash;
5. a canonical all-source code-freeze manifest and its externally frozen
   hash.

The code freeze binds every `src/signalbot/**/*.py` file, `.python-version`,
`pyproject.toml`, the experiment contract, the topology amendment, and this
contract. It also binds the exact census, experiment, topology, and adapter
contract hashes and states `outcome_data_read=false`.

## Frozen evaluation semantics

The only permitted fixed horizons are 1, 3, 6, 12, and 72 closed 5-minute
bars: 5, 15, 30, 60, and 360 minutes. Entry is the next contiguous 5-minute
candle open. Exit is the corresponding fully closed horizon candle. Split
boundaries and data gaps remain exclusions. Fees, slippage, funding, return
rounding, and component reconciliation must be owned by the existing public
historical three-family outcome/execution functions; an evaluator must not
reimplement or tune those rules for this population.

The separately frozen comparison is `BROAD_3_OF_3` versus
`CONFLICTED_2_VS_1`. It is reported separately from the original clean
`BROAD_3_OF_3` versus `CLEAN_2_PLUS_NEUTRAL` comparison and separately by
bullish/bearish source side and horizon. It is never pooled with clean
`2 + neutral`.

## Fixed claims

Every adapter artifact states:

- `historical_only=true`;
- `outcome_data_read=false` at adapter authorization time;
- `probability=false` and `probability_calibrated=false`;
- `promoting=false` and `order_placement=false`;
- `changes_source_admission=false` and `changes_source_event_id=false`;
- `clean_population_pooled=false`.

The label `CONFLICTED_MAJORITY_UNCALIBRATED` is evidence breadth, not a win
probability, expected return, recommendation, or order instruction. No result
from this historical comparator promotes the scanner without a later,
separately frozen prospective PAPER/BBO validation decision.
