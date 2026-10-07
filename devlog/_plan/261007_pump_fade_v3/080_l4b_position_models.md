# L4b — in-position model/rule comparison

External development-only analysis on the same parents/entries as L3. Every closed 5m bar in-position gets two targets: (1) adverse continuation for short: further adverse mark move X primary 10%, sensitivities 5/15% before favorable Y=5%, 4h barrier; mirror longs (P3 aggregate context says 149 round trips). (2) bounce vs continuation after favorable leg using L3b labels. Use only decision-time features including adverse %, duration, adds, frozen invalidation/liquidation distance when known, new high, OI-price movement, forced BUY bursts, funding-cap proximity and 15m structure.

Evaluate rule-only warnings first, then same M0–M4 ladder discipline. Compare cut-on-warning with hold-with-adds (legacy 8-add and primary 2-add) and fixed 5–15% stops; explicitly count cut-then-reversed false alarms. Report net bp, q99 loss, liquidation proximity, false alarms per 100 positions. Promotion requires q99 reduction ≥30% vs hold-with-adds, net noninferior within 10bp, and false-alarm rate ≤20 per 100 positions (user-approved); model must beat rule-only. Preserve the user's P3 private rows outside the repo; aggregate-only summaries.

Files/tests/results in external project. L4b does not enable live warnings or order actions and does not change L1 thresholds.
