# R4B V2 Three-Family Topology Pre-Outcome Amendment

Status: pre-outcome, historical-only, non-promoting amendment. At the time this
contract was written, no event from the new three-family consensus had been
joined to a forward return. Prior V1A results were already exposed and remain
descriptive context; they are not labels for this amendment.

This amendment does not change the frozen consensus arithmetic, event ID,
payload, or admission decision in
`r4b-v2-historical-three-family-consensus-experiment.md`. It gives every
three-leaf sign pattern an explicit, hash-bound name before any new outcome is
opened. Its executable owner is
`R4B_CAUSAL_V2.4.1_HISTORICAL_THREE_FAMILY_TOPOLOGY_V1_FROZEN`.

## Why this amendment exists

The original clean `2/3` state means two same-direction leaves and one neutral
leaf. The three historical formulas usually emit a nonzero sign when READY, so
that state may be structurally sparse. A two-versus-one disagreement is not the
same evidence pattern and must not be silently called clean `2/3`.

The three families are capped and deliberately non-duplicated. They are not
claimed to be statistically independent.

## Exhaustive topology

READY leaves are classified only by the ordered counts
`(bullish, bearish, neutral)`:

- unanimous bullish `(3,0,0)` or bearish `(0,3,0)`;
- clean two plus neutral bullish `(2,0,1)` or bearish `(0,2,1)`;
- conflicted two versus one bullish `(2,1,0)` or bearish `(1,2,0)`;
- lone bullish `(1,0,2)` or bearish `(0,1,2)`;
- balanced `(1,1,1)`;
- all neutral `(0,0,3)`.

Any unavailable required leaf is `WITHHELD`; it is unknown and never converted
to neutral. Magnitudes order observations inside a topology but cannot change
the sign-count topology. A nonzero direction with quantized zero magnitude is
still a sign under the frozen source-family contract.

## Outcome-blind feasibility census

Before forward returns are read, the census must publish and hash:

- counts by topology, split, asset, and source LONG/SHORT side;
- the ordered 27-tuple leaf sign table;
- each family's READY, bullish, bearish, and neutral rates;
- pairwise agreement, disagreement, and neutral rates;
- reconciliation to all 6,341 authenticated anchors and the original admitted
  count.

The topology artifact binds the source consensus event ID, payload hash,
canonical hash, anchor hash, all three leaf projections and hashes, topology
rule version, and fixed false claims for probability, calibration, promotion,
orders, and changes to the source decision.

## Predeclared comparisons

The original clean comparison remains:

1. unanimous `3/3` supporting the source direction;
2. clean `2 + neutral` supporting the source direction.

If either cell is empty or sparse, that comparison is inconclusive. No outcome
may be used to change its membership.

A separate descriptive breadth comparison is predeclared here, before outcome
matching:

1. unanimous `3/3` supporting the source direction;
2. conflicted `2 versus 1` whose two-family majority supports the source
   direction.

The conflicted group remains non-admitted under the original consensus and is
never pooled with clean `2 + neutral`. It may enter a separately versioned
historical outcome adapter only after the topology census, executable code,
this amendment, source consensus artifact, cost contract, horizons, and output
schema are hash-frozen. Results are reported separately by bullish/bearish
side and 5, 15, 30, 60, and 360 minutes.

## Alert wording boundary

Until untouched PAPER/BBO data support calibrated probabilities, presentation
uses evidence-breadth labels only:

- `UNANIMOUS_BREADTH_UNCALIBRATED` for `3/3`;
- `CLEAN_TWO_FAMILY_BREADTH_UNCALIBRATED` for `2 + neutral`;
- `CONFLICTED_MAJORITY_UNCALIBRATED` for `2 versus 1`;
- insufficient, no-consensus, or withheld labels for all other states.

No percentage, win probability, expected return, or trading instruction may be
inferred from those labels. Volatility, crowding, cost survival, and execution
quality remain context or veto candidates; they never become additional
directional votes.
