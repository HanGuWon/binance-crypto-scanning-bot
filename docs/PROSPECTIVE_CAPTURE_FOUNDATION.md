# Prospective capture foundation

This package is an order-free foundation for an `R4B_CAUSAL_V1` prospective
recorder. It is not connected to `SignalApplication`, the current
`WebSocketConsumer`, the scanner runtime, or any Binance private endpoint. An
explicit foreground command can now start public-data capture, but it produces
infrastructure evidence only: it does not calculate signals, outcomes, PnL, or
an efficacy verdict.

## Producer contract

`PublicWebSocketCaptureAdapter` receives an already-connected public WebSocket
async iterator. Immediately after `async for raw` yields, it samples one UTC
Unix-millisecond receipt time plus one supplemental monotonic-nanosecond value.
It does not parse JSON. Text and binary frames, including invalid JSON, are put
into the bounded handoff losslessly with plan, process, connection, frame, and
ingest identity.

The separate prospective plan builder permits only the fixed 5-minute evidence
set: Spot and Futures kline, aggregate trade, BBO, 100 ms depth, and Futures
1-second mark/funding estimate streams on the current public routes. Scanner
plans, all-market mini-tickers, non-5m candles, user streams, API keys, and
private/order paths are outside this boundary.

Each depth owner reports a validated `U/u` range only after the corresponding
raw frame has been offered to the capture pipeline. The bounded REST scheduler
owns exactly six operational range states (Spot and Futures for BTCUSDT,
ETHUSDT, and SOLUSDT), with at most 1,024 pending ranges per book. Startup and
reconnect snapshots are requested only after the first depth range for every
authorized symbol in that connection generation has been observed. A sequence
gap resets the affected range state before its symbol-only resnapshot is
queued. Missing callbacks, callback failures, queue overflow, or range-buffer
overflow are fatal.

The operational observer runs when the adapter resumes the frame iterator for
its next item. A stop or lifetime recycle immediately after the final raw offer
can therefore omit only that generation-local operational observation; the raw
frame is still persisted, the offline replay remains authoritative, and the
next connection generation resets every operational depth state before it can
be treated as synchronized.

The online bridge follows the venue contracts separately. Spot discards
buffered ranges with `u <= lastUpdateId`; USD-M Futures discards ranges with
`u < lastUpdateId`. Under the frozen DESIGN reconciliation of Binance Spot's
off-by-one wording, the first retained Spot range must contain
`lastUpdateId + 1` in `[U, u]`; USD-M Futures must contain `lastUpdateId` and
retains its `pu` continuity rule. A snapshot that is ahead of the current
buffer may wait up to the frozen two-second bound for another persisted range.
A stale or timed-out bridge consumes one of exactly three snapshot cycles. A newer
`(generation, first U)` supersedes an older in-flight request without clearing
the newer buffer. Successful operational synchronization clears the pending
buffer, so ordinary live ranges do not accumulate.

Spot `exchangeInfo` is captured once per interval with the fixed encoded query
`symbols=["BTCUSDT","ETHUSDT","SOLUSDT"]`. This preserves the three-symbol
canary metadata in one weight-20 response and avoids admitting the unbounded
venue-wide payload, which exceeded the frozen 16 MiB body cap in the failed
2026-07-17 smoke. The cap is not raised and the request is not expanded into
three per-symbol calls.

Spot depth snapshots are admitted through one stop-aware monotonic pacer at a
minimum 3.2-second interval. The single Spot-depth lock is held through the
adapter attempt, and the next cadence begins only after that attempt terminates.
Semaphore, rate-gate, and adapter-internal connection/cleanup waits therefore
cannot consume intervals early and burst when a gate reopens. This bounds any
60-second window to at most 19 depth admissions (4,750 planned weight) before
ordinary Spot polling. Every
successful Spot response must contain exactly one canonical nonnegative
`x-mbx-used-weight-1m` header; an observed value at or above 5,000 quarantines
the session before the 6,000/minute venue limit. The targeted `exchangeInfo`
response must confirm exactly one `REQUEST_WEIGHT/MINUTE/1=6000` contract,
contain the exact canary symbol set without duplicates, and preserve the
required price/notional/lot filters. Limit drift is never used to silently
retune a running frozen canary. `BODY_LIMIT` is persisted once and immediately
quarantined without a retry. Existing 429 `Retry-After` and 418 fatal handling
remain independent fail-closed paths.

`RestEnvelopeV2.response_completed_*` is the conservative local admission
receipt used for global ordering and snapshot availability. On the normal path
it is sampled after bounded response-close handling and immediately before
`ingest_seq` allocation and pipeline offer, with no intervening await. It is
therefore not an exchange timestamp or a claim about the exact transport
last-byte instant. `response_first_byte_*` is likewise the local time at which
the HTTP response headers became available to the client, not a NIC timestamp.

This online state is an availability guard, not the evidence authority. The
offline `LocalBookMaterializer` independently replays the persisted global
ingest sequence, raw depth frames, connection transitions, and REST snapshots.
It applies exact-decimal absolute level updates, venue-specific bootstrap and
continuation rules, request-start/generation barriers, and bounded event, byte,
and level state. Replay must begin at `ingest_seq=1` and remain contiguous.
Connection or reconstruction invalidation remains unresolved until a successful
snapshot-plus-first-depth bridge; a clean `owner_stop` does not invalidate an
already synchronized book. This unresolved-reconstruction state is independent
of a remembered sequence gap and prevents a completed canary from passing. The
materializer exposes a no-sort coverage view; a sorted full-book view is only an
explicit diagnostic operation.

## Fail-closed lifecycle

The handoff reserves one control slot and is bounded by both queued/in-flight
event count and encoded bytes. `put_nowait` overflow, serialization failure,
storage quota, short write, writer failure, or integrity failure sets the shared
fatal and stop state. The first failure remains authoritative. No scanner or
runtime callback exists in this slice, so a caller cannot silently continue
from capture into alert or order behavior.

Normal shutdown order is:

1. stop every producer;
2. enqueue the ordered stop marker;
3. drain and acknowledge every queued record;
4. fsync, finalize, and close the active segment;
5. surface any background writer failure to the caller.

The foreground owner follows that order across all four producers: exactly
three separately routed WebSocket owners plus one bounded public REST
scheduler. They share one `SystemReceiptClock`, one process-local
`IngestSequencer`, one fatal state, and one bounded pipeline. At duration or
operator stop, the owner first stops and drains producers, then drains and
closes the pipeline. On failure, it cancels all producers, stops or aborts the
pipeline fail-closed, and re-raises the first cause. A fatal session closure is
retained only when the stored segment chain can still be verified.

An abnormal storage failure attempts to fsync a small `coverage-fatal.jsonl`
journal from reserved capacity. Its presence makes verification fail rather
than letting finalized earlier segments appear complete.

The newer V2 capture foundation additionally has a local clean-stop
prerequisite. It atomically freezes admission at one positive accepted tail,
queues finality and `STOP` in reserved control capacity, and proves that exact
tail against the WAL and signed-block writers before close. Pipeline stop is
owned by a cancellation-shielded task, and a stopped instance cannot restart.
A normally closed mirrored WAL remains attestable, while a distinct
verification-only reopen can later re-read the same finalized dual prefix
without creating, recovering, syncing, or appending and without itself claiming
the prior clean stop. Aborted or faulted mirrors cannot pass. Before the
integrity ledger persists a `CLEAN` seal, the grouped-block owner durably
terminalizes the exact finality tail and thereafter rejects commits. The seal
binds that terminal marker and requires no unmatched `SOURCE_GAP` and no
`VOID`. Fresh verification-only WAL, block, and ledger owners can reprove the
retained seal; new issuance still requires the live normally closed WAL. All
combined ledger paths acquire the writer lease before the ledger lock. A
separate write-once local session-closure authority then binds the start
receipt, planned source census, terminal finality proof, and ledger seal under
the held writer lease.

This is not a supported V2 end-to-end capture closure:
`SESSION_CLOSURE_SUPPORTED_V2=False`. The top-level runtime joining both public
WebSocket routes with public OI REST, an observed M2 source census/certificate
and verifier, and an independently anchored external audit/deletion tombstone
are still missing. None of these local closure artifacts evaluates signals,
efficacy, PnL, expectancy, or profit.

## Segments and recovery

Raw authority is deterministic JSONL. Each line is compressed as an independent
checksummed zstd payload and stored in outer-frame format version 1. The outer
header is fixed-width and consists of:

- 8-byte magic `SBCAPFRM`;
- 1-byte format version (`1`);
- unsigned big-endian 64-bit compressed and uncompressed lengths;
- SHA-256 of the compressed payload;
- SHA-256 of all preceding header fields.

The header digest therefore protects the magic, version, both lengths, and
payload digest. The manifest also records `frame_format_version: 1`. Despite
the historical `.jsonl.zst` suffix, a segment is an outer-frame container and
must be decoded by the capture reader rather than as a bare concatenated zstd
stream. Segments rotate before the first record crossing any of these limits:

- a 5-minute UTC receipt-time bucket;
- 256 MiB uncompressed;
- 1,000,000 WebSocket frames.

The active name ends in `.partial`. Finalization fsyncs the data, atomically
renames it, hashes it with SHA-256, and atomically writes a manifest containing
the previous segment hash. POSIX also fsyncs the parent directory; Windows has
no portable directory-fsync primitive.

Recovery can seal a contiguous unfinished `.partial` tail and discard only a
final frame whose complete, integrity-checked outer header declares more payload
bytes than are present. A partial or corrupt outer header is fatal because its
intended payload boundary cannot be proven. A fully present payload must pass
the outer compressed SHA-256, inner zstd checksum, exact uncompressed length,
and one-record JSONL invariants; failures are corruption, never torn-tail
recovery. Finalized segments and finalized orphans reject every torn or corrupt
frame. Sequence gaps, receipt reversal, unknown schemas, ambiguous recovery
temporary files, manifest/data mismatch, and a broken hash chain are also
fatal. Recovery is followed immediately by full verification. Normal finalized
verification reads fixed-width headers and compressed payloads in bounded
chunks, retains only one decoded record at a time, and aggregates manifest
metadata incrementally. It therefore uses memory proportional to one frame,
not to the whole segment. The explicit `read_segment_lines` helper and partial
tail rewrite may collect decoded records because their contracts return or
re-encode the complete prefix; authenticated declared lengths are never used
to preallocate those records.

## Validation-only command

`signalbot-capture validate-config` hashes a prospective plan and validates
symbols, public stream routing, bounded queue/storage values, and a proposed
canary duration. It performs no network call, creates no output directory, and
starts neither a writer nor live capture.

## Explicit foreground capture

Both the output base and the separate external-audit directory must already
exist, must contain no symlink/reparse-point component, and must be distinct,
non-nested paths. Each run creates a new `<UTC-ms>-<UUID-hex>` session directory
under the output base and a `segments` child. It then:

1. builds the canonical source manifest twice and refuses to continue unless
   the bytes are identical;
2. writes those exact manifest bytes once with exclusive create and fsync;
3. writes a canonical `SessionStartV1` once;
4. anchors the actual start-document SHA-256 in a separate-path start audit
   head;
5. only then constructs and starts the public capture producers;
6. verifies and writes the session closure, then anchors the actual closure
   SHA-256 to the actual start-audit SHA-256.

The separate path is classified `SEPARATE_PATH_AUDIT_ONLY`; it is deletion
evidence under a distinct operator path, not a WORM claim.

The exact 24-hour infrastructure canary is foreground-only and has no duration
override:

```powershell
signalbot-capture start-canary `
  --workspace-root "C:\path\to\Binance bot-2" `
  --config-file "C:\path\to\Binance bot-2\config\capture.r4b-canary-v1.yaml" `
  --protocol-file "C:\path\to\Binance bot-2\artifacts\oracle\2026-07-17\R4b_frozen_experiment_spec_v1.yaml" `
  --output-base "D:\capture-output" `
  --external-audit-root "E:\capture-audit" `
  --allow-public-network `
  --confirm R4B_PUBLIC_DATA_ONLY_NO_ORDERS
```

A bounded connectivity smoke run accepts only 10 through 300 seconds:

```powershell
signalbot-capture start-smoke `
  --seconds 60 `
  --workspace-root "C:\path\to\Binance bot-2" `
  --config-file "C:\path\to\Binance bot-2\config\capture.r4b-canary-v1.yaml" `
  --protocol-file "C:\path\to\Binance bot-2\artifacts\oracle\2026-07-17\R4b_frozen_experiment_spec_v1.yaml" `
  --output-base "D:\capture-output" `
  --external-audit-root "E:\capture-audit" `
  --allow-public-network `
  --confirm R4B_PUBLIC_DATA_ONLY_NO_ORDERS
```

`start-smoke` always closes as an operator-requested smoke session and is never
classified as canary evidence. `start-canary` runs exactly 86,400 seconds unless
the operator interrupts it; an interrupt closes as `operator_requested`, not as
a completed duration. Neither command runs a background process or emits a
capacity/coverage success verdict. A completed duration is still unevaluated
until the separate reporting gate audits the stored evidence.

The capacity report hard-fails on a persisted HTTP body-limit truncation even
when that role was observed and returned HTTP 200. An incomplete payload
therefore cannot satisfy the role-presence check by itself. Other bounded REST
errors remain counted separately because an operator-duration shutdown can
legitimately persist a terminal `CANCELLED` attempt.

## Closed-evidence reporting

All three reports use one public closed-evidence boundary. It verifies the canonical
session start and closure, the separate-path audit-head chain, link-free segment
and manifest paths, the full segment hash chain beginning at `ingest_seq=1`, and
strict canonical typed records. It also verifies the exact session-root
`capture-source-manifest.json` as a stable, link-free regular file no larger than
16 MiB: its bytes must be newline-free canonical JSON, its SHA-256 must equal the
session authority, and its infrastructure purpose plus protocol/configuration
hash bindings must agree with the session start. It repeats the complete
authority check immediately before and after streaming records one at a time. A
named-path or manifest change prevents a report artifact from being produced.
This proves the stored manifest binding, not that the current report runtime was
independently attested against the capture-time source manifest. Readers are
also not descriptor-pinned for the entire consume interval, so a concurrent
writer with path-mutation rights remains a swap-and-restore threat.

`build_canary_capacity_schema_report` preserves the original infrastructure
capacity/schema scope and does not interpret market payloads. The separate
`build_depth_reconstruction_coverage_report` interprets only the depth evidence
needed to reconstruct the six books. It partitions the full session window into
sequence-unavailable; fresh two-sided uncrossed; fresh crossed/locked; fresh
one-sided/empty; and sequence-valid-but-stale durations. Freshness is based only
on the latest locally received, actually applied snapshot or depth evidence:
age at or below exactly 2,000 ms is fresh and age above it is stale. The deadline
is split even if no later affected record arrives, unrelated records do not
refresh it, and later applied depth evidence can make the book fresh again.
This is not Binance `E`/`T`, exchange quote age, BBO latency, or an execution
fill claim. The 24-hour canary requires both sequence-valid and fresh
two-sided-uncrossed coverage at 995,000 ppm; 999,000 ppm is diagnostic for the
later 30-day prospective gate and is not a holdout pass here. The report fixes
`family_b_authorized=false` and `promotion_authorized=false`; efficacy and the
30-day gate are not performed, while outcomes and PnL remain unevaluated. A
depth-coverage pass is not evidence of profitability or positive expectancy.

`build_clock_health_report` is the third, separate infrastructure gate. It
interprets only the exact Spot `/api/v3/time` and USD-M Futures `/fapi/v1/time`
responses and never pools the two venue clocks. Each complete HTTP 200 sample
brackets `serverTime` between local request start and response-header
availability, widens the offset interval by one millisecond on each side for
integer timestamp quantization, and becomes causally available only at REST
response completion. Header RTT above 2,000 ms or sample age above 60,000 ms is
inconclusive. Consecutive samples must fit a separately frozen ±1,000 ppm rate
envelope. Global receipt pairs treat a wall-versus-monotonic residual at or
below 2 ms as healthy, above 2 ms but below 100 ms as inconclusive, and at or
above 100 ms as a clock discontinuity. A completed 24-hour clock pass requires
999,000 ppm valid coverage independently for Spot and Futures; a short smoke is
`INCOMPLETE`.

The pure cutoff mapper accepts only one already-complete prior clock sample;
it never interpolates from a future sample. It returns `ADMISSIBLE` only when
the entire conservative receipt-time interval is at or before the cutoff,
`LATE` only when the entire interval is after it, and otherwise
`CLOCK_INCONCLUSIVE`. This helper is not connected to the alert or PAPER
runtime. The report is covered by future exact source manifests, but the
existing closed-evidence check still does not independently attest the current
report runtime against capture-time source bytes.

The infrastructure builder deliberately does not materialize a derived clock
sample ledger. It streams the immutable raw records, retains only constant-size
per-venue diagnostics plus the first and last valid samples, and returns a
canonical summary and SHA-256 to the caller without writing into the closed raw
session. The raw `RestEnvelopeV2` records remain the complete per-sample
authority. A decision-level, hash-bound clock ledger belongs to the later
PAPER/efficacy protocol and is not implied by this infrastructure report.

## Deliberately remaining work

- Add bar-level required-stream coverage, actual fee/funding snapshots, and the
  next-BBO/full-visible-depth PAPER execution ledger before any causal efficacy
  run; connect the already-audited causal clock mapper only in that later
  protocol version.
- Keep Spot Family B disabled until its decision-time BBO timestamp contract is
  revised and frozen; the current Spot book-ticker payload does not provide the
  event-time fields required by the downloaded generic protocol. Never
  synthesize Spot `pu` or timestamps.
- Run a non-efficacy capacity canary before freezing universe and retention.
- Run the independent 24-hour depth reconstruction gate, then collect the full
  30 consecutive days at the prospective coverage requirement before opening a
  sealed efficacy result.
- Upgrade the separate-path audit head to an independently administered WORM or
  transparency boundary, attest the report runtime against the capture-time
  source manifest, and use descriptor-pinned read-and-hash if protection
  against a concurrent writer with path-mutation rights is required.
