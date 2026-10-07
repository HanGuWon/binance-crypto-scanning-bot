# v2c local package release preparation (not deployed)

The only candidate is the separate local `release_v2c` hash bundle. It includes the v2c rule, remediation contract, acceptance matrix, code/tests, source-plan verification receipt, and the corrected exposed proxy output. `deployment_enabled`, capture, Discord, forward evaluation, and production orders remain false. This receipt does not authorize an OCI login, deployment, timer, webhook, private API, database migration, or Guardian change.

Before any future deployment request, require fresh independent review, actual OCI source/config receipts, explicit human authorization, isolated service/DB/cgroup checks, bounded canary and rollback evidence. The old OCI campaign and incumbent scanner remain untouched. For current local completion, only verify the hash closure and preserve the resulting package; do not run a live smoke capture.
