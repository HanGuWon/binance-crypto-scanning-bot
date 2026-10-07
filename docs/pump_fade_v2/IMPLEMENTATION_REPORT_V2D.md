# Pump-fade v2d local implementation report

v2d closes the four residual engineering findings locally while preserving the
exposed v2c evidence. It is a disabled research package, not a validated trading
strategy and not permission to publish or deploy.

## F4 — funding predicates are distinct

Three spaced observed normalizations after the latest in-window funding-cap hit
can clear the cap-risk WAIT reason even when they occurred before event
admission. R5 still requires its own post-event confirmations. The regression
fixture covers cap-risk clearance and absence of R5 in the same transition;
the existing boundary cases retain repeated-hit reset, spacing and interval
restoration checks.

## F2 — comparator now executes supplied scenarios

The offline engine replays common parent paths using receipt-stamped executable
quotes, frozen initial size/stop/risk, confirmation-gated additions, partial
tranche aggregation, stop ordering, fee/slippage scenarios and verified
settlements. First-fill-only, primary two-add, three-add sensitivity and legacy
eight-add comparator share each parent. Censored or missing paths remain in the
denominator; absent funding is never treated as zero. This engineering fixture
does not substitute for private/historical inputs: no real private export was
read, and empirical q99, clustered noninferiority, Holm and efficacy remain
unavailable.

## F3 — outcomes gate terminal pruning

The offline lifecycle verifies recorded capture segments before materializing
durable decisions, stores outcome snapshots append-only, and requires separate
event- and alert-origin 4h/24h terminal receipts before archiving a terminal
episode. A failed/incomplete archive does not prune the source rows; successful
archive bytes are read back and hash-acknowledged before deletion. Missing
observations remain pending/censored/unavailable. The shadow database and preview
outbox have explicit row/episode caps; terminal JSON archives have a configurable
8 GiB default aggregate cap (bounded to 64 GiB), fail closed at capacity, and are
not automatically deleted or rotated.

## F1 — package identity covers execution closure

The release builder resolves the Pump-fade Python import closure, binds
pyproject.toml and uv.lock plus v2d policy/freeze inputs, creates a deterministic
source package, and reports executable and research-evidence hashes separately.
Missing local imports and escaping inputs fail closed. An isolated import smoke
must show that the extracted package—not a parent editable install—owns the
import.

## Evidence and limits

The 15% squeeze threshold remains primary with inclusive equality. v2c package,
policy, freeze, reports, replay outputs and release manifest remain unchanged.
The full repository pytest suite must still run on a compatible authorized
Windows runner; this host previously could not initialize the pytest-asyncio
loopback socketpair and cannot supply that final evidence. Capture, Discord,
scanner/Guardian integration, orders, forward evaluation and deployment remain
disabled. See the adjacent acceptance matrix and Sonnet handoff for exact
commands and finalization instructions.
