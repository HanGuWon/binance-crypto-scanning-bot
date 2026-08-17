# D2 SCEFB operator failure-receipt correction A1

## Status and timing

- Version: `D2_SCEFB_OPERATOR_FAILURE_RECEIPT_A1`
- Fixed: 2026-07-21, before D2 code freeze, ARMED, START, or outcome-row access
- Parent preregistration:
  `37640c48b386896edc333d83467cb89add0cedb95ffc3afa5e374bd1e580bca3`
- Corrected amendment A0:
  `94c7ee24a5be0f36e48b0f62c2c9898601dc54cf73f8d2894f6a91304b4175a7`
- Scope: failure-receipt hash layering only
- Strategy, data, universe, thresholds, cost model, and statistical gates
  changed: false

## Reason for correction

Amendment A0 required the WAL suffix to equal both a `receipt_sha256` stored
inside the canonical receipt and the SHA-256 of the complete receipt file. A
digest embedded in the bytes whose digest must equal that same value is a hash
fixed-point requirement and is not a constructible publication contract.

A0 remains immutable evidence of the pre-freeze design review. This A1
correction supersedes only that impossible equality. All other A0 state,
publication-order, recovery, sanitization, CLI, non-retry, and non-claim rules
remain in force.

## Correct two-layer hash contract

The receipt has two distinct identities:

1. `receipt_body_sha256` is the domain-separated SHA-256 of the canonical body
   document that excludes `receipt_body_sha256` itself; and
2. `receipt_file_sha256` is ordinary SHA-256 of the complete canonical JSONL
   file bytes after `receipt_body_sha256` has been inserted.

The hash domain is exactly:

```text
D2_HISTORICAL_FAILURE_RECEIPT_BODY_V0\0
```

The published canonical JSONL contains `receipt_body_sha256` and does not
contain a self-referential `receipt_file_sha256` field. The publisher computes
`receipt_file_sha256` over the complete bytes, returns it in the typed
publication receipt, and binds it into the WAL terminal detail code:

```text
D2_FAILURE_RECEIPT_SHA256_<64 UPPERCASE HEX CHARACTERS>
```

Verification must independently prove all of:

- exact canonical JSONL bytes and field set;
- recomputed domain-separated body hash equals the embedded lowercase
  `receipt_body_sha256`;
- ordinary SHA-256 of the complete file bytes equals the lowercase form of the
  WAL detail-code suffix; and
- run ID, START record, attempt bindings, attempt-directory identity, planned
  terminal state, and output-protocol state match the WAL and fixed operator
  contract.

Neither digest substitutes for the other. A mismatch at either layer is
`AMBIGUOUS_FAILURE_EVIDENCE`, never `FAILED`, and never authorizes retry.

No production order placement is authorized.
