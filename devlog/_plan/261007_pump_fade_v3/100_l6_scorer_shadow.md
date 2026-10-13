# L6 — optional model scorer shadow (prepare only)

If L4/L5 select a model, package a separate optional OCI scorer service, not part of the running recommender or collector process. Score only collector event symbols at decision times; write shadow records next to (not inside or sharing) v2d store. Discord remains disabled. Resource limits `MemoryMax=1.5G`, `CPUQuota=40%`; separate systemd unit/user/data directory; bounded queue/cache, model/config hashes, deterministic event IDs, restart idempotency, graceful shutdown. Unit stays disabled and is not installed/enabled in this task.

Planned files: NEW `src/signalbot/pump_fade_v3/scorer.py`; NEW `deploy/systemd/pump-fade-v3-scorer.service`; NEW `config/pump_fade_v3/scorer.json`; NEW tests and `docs/pump_fade_v3/SCORER_RUNBOOK.md`. If no model qualifies, record rules-only and do not create weights/runtime integration. Enabling is a separate user decision.
