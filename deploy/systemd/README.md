# systemd deployment

The unit expects an immutable release symlink at `/opt/signalbot/current`, a
validated configuration at `/etc/signalbot/settings.yaml`, and persistent state
under `/var/lib/signalbot`.

Copy `signalbot.env.example` to the root-owned
`/etc/signalbot/signalbot.env`. Keep secrets out of the release tree. To enable
Discord, add `SIGNALBOT_DISCORD_WEBHOOK_URL=...` to that file and set its mode
to `0600`. Without that variable Discord stays disabled while market ingestion,
persistence, and dry-run checks remain available.

The service is alert-only. It has no exchange order path and requires no Binance
API key for public Spot and USD-M market data.
