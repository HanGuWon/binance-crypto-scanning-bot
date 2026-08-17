# D2 SCEFB operator failure-receipt amendment A0

## Status and timing

- Version: `D2_SCEFB_OPERATOR_FAILURE_RECEIPT_A0`
- Fixed: 2026-07-21, before D2 code freeze, ARMED, START, or outcome-row access
- Parent preregistration:
  `37640c48b386896edc333d83467cb89add0cedb95ffc3afa5e374bd1e580bca3`
- Scope: post-START failure evidence and recovery classification only
- Strategy, data, universe, thresholds, cost model, and statistical gates
  changed: false

This amendment resolves one operator detail left implicit in the parent D2
preregistration. It does not authorize a D2 run by itself.

## Failure receipt and WAL binding

The D2 operator reuses the already tested D1 append-only attempt WAL strictly
as a transport schema. A terminal `FAILED` WAL record must retain null
`result_sha256` and `artifact_manifest_sha256`. Therefore a separate bounded
canonical failure receipt is published and its SHA-256 is bound into the
terminal record's existing `detail_code` as:

```text
D2_FAILURE_RECEIPT_SHA256_<64 UPPERCASE HEX CHARACTERS>
```

The canonical receipt retains its own hash in lowercase. Verification extracts
the suffix from the WAL detail code, lowercases it, and requires exact equality
with both the receipt's canonical self-hash and the byte hash of the published
receipt file.

The receipt contains exactly bounded, sanitized fields for:

- run ID;
- typed phase and typed error code;
- fixed-schema sanitized context with no arbitrary exception string;
- START record, bindings, and attempt-directory hashes;
- planned terminal state and observed output-protocol state;
- UTC Unix-millisecond observation time;
- receipt self-hash; and
- `production_order_placement=false`.

The original exception remains chained in process memory for debugging, but it
is not serialized. Secrets, filesystem contents, arbitrary paths, payload
rows, and Python exception messages are forbidden from the receipt.

## Publication order and crash states

After permanent START, a failed run follows this order:

1. classify output protocol state without deleting or replacing anything;
2. build the typed canonical receipt;
3. publish it to the fixed fresh no-replace failure-receipt directory and
   revalidate its bytes;
4. append and durably sync terminal `FAILED` or `AMBIGUOUS_OUTPUT`, with the
   receipt hash embedded in `detail_code`; and
5. verify WAL-to-receipt binding read-only.

The receipt publication and WAL append are separate durability boundaries and
cannot be made power-loss atomic. Recovery classification is fixed as follows:

| Observed state | Verification status | Retry allowed |
| --- | --- | --- |
| START, no terminal, no receipt | `INCOMPLETE` | no |
| START, valid receipt, no terminal | `AMBIGUOUS_FAILURE_EVIDENCE` with reason `INCOMPLETE_FAILURE_BINDING` | no |
| START, terminal FAILED, missing/invalid/mismatched receipt | `AMBIGUOUS_FAILURE_EVIDENCE` | no |
| START, terminal FAILED, exact bound receipt, output protocol provably absent | `FAILED` | no |
| any uncertain or present result-output protocol after START | `AMBIGUOUS_OUTPUT` | no |

An orphan receipt before START is invalid state and cannot grant outcome access.
No loader may infer permission to resume from any receipt, WAL prefix, process
exit, or absence of output.

## CLI contract

Operational failures must not pass through `argparse.error()`. The command
returns exit code 1 and emits one canonical JSONL status object containing the
run ID, verification status, typed phase, typed code, failure-receipt SHA-256,
and fixed false claims. Argument syntax errors may continue to use argparse's
normal behavior before any mutation or outcome access.

No production order placement is authorized.
