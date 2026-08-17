# Conflicted Comparator Freeze B Runbook

The conflicted fixed-horizon consumer uses the same generic downstream
code-freeze manifest (Freeze B) as the primary fixed-horizon and TE0 runners.
There is no conflicted-only downstream freeze format.

Before any outcome-bearing runner starts, create one generic Freeze B with
`signalbot.backtest.downstream_code_freeze`. Its `upstream_sha256` must bind at
least:

- `bootstrap_schedule`;
- `census_artifact_manifest`;
- `census_code_freeze`;
- `experiment_contract`;
- `topology_amendment`;
- `funding_authority`;
- `conflicted_adapter_contract`;
- `adapter_code_freeze`;
- `conflicted_adapter_manifest`.

Pass the same manifest path and SHA-256 to primary, TE0, and conflicted
fixed-horizon commands. The conflicted CLI arguments are
`--downstream-code-freeze-manifest` and
`--expected-downstream-code-freeze-manifest-sha256`.

The adapter contract remains the already frozen file with SHA-256
`36e32da401617022c29395fdcbd570e7a8cf04a2f496679d7aa8b24cf90e4ecd`.
This runbook does not modify that contract.
