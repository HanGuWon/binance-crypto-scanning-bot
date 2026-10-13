# Pump Fade v3 daily Drive archive

The uploader groups `CLEAN_CLOSED` generations by their terminal UTC date. It
archives only completed UTC dates, so a generation from the current UTC day is
deferred until the next run. Each date produces one uncompressed tar, one JSON
manifest containing each generation's file inventory and SHA-256 values, and one
verification receipt.

Before recording a receipt, the uploader runs `rclone check --checksum` on the
tar, reads the manifest back with `rclone cat` and compares its SHA-256, then
compares the remote tar size with the local tar size. Every rclone invocation
uses `--tpslimit 2`. Rate-limit responses receive up to three attempts with
exponential delays; three consecutive rate-limit failures log `CRITICAL` and
leave capture data in place.

Pruning is disabled by default. When enabled, a generation is eligible only if
its UTC date bundle has a local receipt that records successful Drive
verification, and its files still match that bundle's manifest. The configured
retention remains seven days. Keep `pump-fade-v3-uploader.timer` disabled and
`prune_enabled=false` until an independent review passes and a manual upload has
been verified.

## Deployment checklist

- Confirm the deployed auth installer starts with `#!/usr/bin/python3` using
  `head -1`, and run the empty-input smoke check:
  `echo '{}' | sudo -n /usr/local/libexec/install_pump_fade_v3_rclone_config`.
  Expect stdout `FAIL`, stderr `reason=token-invalid`, and exit status 1;
  confirm the existing `rclone.conf` metadata is unchanged.
- The uploader service invokes its script through `/usr/bin/python3` in
  `ExecStart`, so a script shebang is not used. Verify that invocation from the
  installed unit when deploying it.
