# v2d local package review runbook

This runbook describes review of a disabled local artifact only. It does not
authorize OCI access, package upload, deployment, Discord delivery, live
capture, or orders.

1. Confirm branch and HEAD against SONNET_FINALIZATION_HANDOFF.md.
2. Verify the v2d policy and remediation-contract hashes against
   config/pump_fade_v2/freeze_manifest_v2d.json.
3. Rebuild into a fresh temporary directory and compare
   executable_tree_sha256, package SHA-256, evidence-tree SHA-256 and every
   listed input hash with docs/pump_fade_v2/release_v2d/release_manifest.json.
4. Extract the package in an isolated directory and confirm imports resolve
   from its src/ tree with no parent editable-install path.
5. Run the full repository verification on a compatible authorized Windows
   runner. Local focused checks do not replace that full gate.
6. Sonnet performs human diff review and decides whether to request the
   separately authorized publication workflow.

All activation flags are false. No live service or deployment action is part of
this procedure.
