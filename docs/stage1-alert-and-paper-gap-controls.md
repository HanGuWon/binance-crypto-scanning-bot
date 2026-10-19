# Stage 1: alert wording, repeat suppression and PAPER data-gap controls

All behaviour-changing switches default to **off**. With every switch off the stored signals,
outbox payloads and PAPER exits are byte-identical to the previous release
(`tests/fixtures/stage1/*.json` are the recorded baseline; `test_stage1_alerts.py::
test_all_stage1_switches_off_reproduce_the_baseline_bytes` enforces it).

## Settings

| Setting | Default | Effect |
|---|---|---|
| `alerts.alert_wording_v2` | `false` | CONFIRMED trend alerts read as a state ("downtrend confirmed (5m low broken) - a new short needs a position check") instead of a forecast. |
| `alerts.alert_position_line` | `false` | Adds a "where in the move" field: zone ordinal, change from the 4h extreme, distance from EMA20 in ATR, RSI, and a display-only chase-risk tag. Never blocks an alert. |
| `alerts.repeat_suppression` | `false` | Inside a live trend zone only the first alert is sent to Discord. Repeats are still stored (signal row, disabled outbox row, `suppressed_repeat` in metadata) and still reach PAPER unchanged. |
| `alerts.repeat_suppression_max_hours` | `12` | A zone also ends this long after the last alert that was actually sent. |
| `runtime.paper_gap_stats_split` | `true` | A `data_gap` end is labelled a censored "tracking interrupted" track (`exit_class`, `censored` in metadata; separate wording in the alert). Reason, price, time and event ID are unchanged. |
| `runtime.paper_gap_log` | `true` | One structured log line per handled candle gap (range, REST result, duration, PAPER outcome). Secrets are masked. |
| `runtime.paper_gap_recovery` | `false` | On a primary-interval gap the bounded REST fill is finished first. If the whole range is recovered, the bars are replayed in order through the **existing** PAPER position only; otherwise the track ends as `data_gap` as before. Late exits are flagged (`late_observation`, `observed_wall_time_ms`). |
| `runtime.require_contiguous_bars` | `false` | Holds signal evaluation (PAPER keeps tracking) while the indicator input window still holds an unrecovered hole. |
| `runtime.contiguity_window_bars` | `210` | Size of the window checked for holes. |
| `runtime.contiguity_hold_limit_bars` | `48` | After this many consecutive held bars the holes seen so far are acknowledged, so a halted or delisted symbol cannot be held forever. |

## Zone definition

A zone starts with the first CONFIRMED `breakdown_short` (down) or `breakout_long` (up) of a
symbol. It ends when the 5m close recovers through EMA20, the strictly prior 1h alignment is
lost, or the maximum age since the last sent alert is reached. Zone state is in memory and is
not restored after a restart.

## Rollback

* Switch the flags above back to `false` and restart, or point the service back to the previous release.
* The settings models reject unknown keys, so remove the new keys from the configuration before
  starting a release that predates them.
* There is no schema change. Rows written with the new metadata keys stay readable by the previous release.
* A restart drops in-memory PAPER and zone state (existing behaviour of the PAPER lifecycle).
