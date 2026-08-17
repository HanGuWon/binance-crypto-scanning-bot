from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from signalbot.capture.writer_lease import (
    WriterLease,
    WriterLeaseProspectiveAttemptClaimError,
)
from signalbot.r4b_v2.alerts.actionability import PromotingFamilyV2
from signalbot.r4b_v2.canonical import canonical_json_line
from signalbot.r4b_v2.capture.wal import WalSyncPolicyV2
from signalbot.r4b_v2.capture.wal_qualification import (
    WAL_QUALIFICATION_DURATION_MS_V2,
    WAL_RECORD_CAP_CANDIDATES_V2,
    WAL_SYNC_CANDIDATES_MS_V2,
    WalCandidateMetricsV2,
    WalCandidateQualificationV2,
    WalQualificationRunV2,
    select_wal_candidate_v2,
    wal_candidate_id_v2,
)
from signalbot.r4b_v2.execution.paper_sizing import PaperSizingCellV2
from signalbot.r4b_v2.execution.prospective_census import (
    ProspectiveCensusPlanV2,
    ProspectiveFamilyRuleBindingV2,
)
from signalbot.r4b_v2.execution.prospective_outcome_wal_record import (
    FAMILY_EXIT_DISPOSITION_PAYLOAD_SCHEMA_V2,
    FAMILY_EXIT_PREPARE_PAYLOAD_SCHEMA_V2,
    POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2,
    POSITION_OPEN_DISPOSITION_PAYLOAD_SCHEMA_V2,
    POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2,
    POSITION_TERMINAL_PAYLOAD_SCHEMA_V2,
    ProspectiveOutcomeWalRecordKindV2,
)
from signalbot.r4b_v2.execution.prospective_outcome_wal_store import (
    PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_FILE_V2,
    ProspectiveOutcomeLifecyclePhaseV2,
    ProspectiveOutcomeWalAppendItemV2,
    ProspectiveOutcomeWalStoreConfigV2,
    ProspectiveOutcomeWalStoreContractErrorV2,
    ProspectiveOutcomeWalStoreFactoryV2,
    ProspectiveOutcomeWalStoreFailedErrorV2,
    ProspectiveOutcomeWalStoreIntegrityErrorV2,
    canonical_prospective_outcome_wal_store_manifest_v2,
    parse_prospective_outcome_wal_store_manifest_v2,
)
from signalbot.r4b_v2.protocol.decision_clock import FIVE_MINUTE_MS_V2
from signalbot.r4b_v2.protocol.lifecycle import (
    MILLISECONDS_PER_DAY_V2,
    FixedHorizonV2,
    ProspectiveAttemptV2,
)

DAY_MS = MILLISECONDS_PER_DAY_V2
H_START_MS = 20_000 * DAY_MS
QUALIFICATION_START_MS = H_START_MS - 30 * DAY_MS
WINDOW_END_MS = H_START_MS - DAY_MS
WINDOW_START_MS = WINDOW_END_MS - WAL_QUALIFICATION_DURATION_MS_V2
QUALIFICATION_ID = "prospective-outcome-wal-final-panel-q1"
MAXIMUM_BYTES = 16 * 1_024 * 1_024
RESERVE_BYTES = 1_024
CONTEXT_FILLERS = tuple(f"C{index:02d}USDT" for index in range(20))
RULES = (
    ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.A, "family-a-v2"),
    ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.B, "family-b-v2"),
    ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.C, "family-c-v2"),
)
SCHEMA_BY_KIND = {
    ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE: (
        POSITION_OPEN_PREPARE_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION: (
        POSITION_OPEN_DISPOSITION_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_PREPARE: (
        FAMILY_EXIT_PREPARE_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_DISPOSITION: (
        FAMILY_EXIT_DISPOSITION_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW: (
        POSITION_CASHFLOW_PAYLOAD_SCHEMA_V2
    ),
    ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL: (
        POSITION_TERMINAL_PAYLOAD_SCHEMA_V2
    ),
}


class _ConstantClock:
    def __call__(self) -> int:
        return 1_000_000_000


class _RaiseOnceAfterWalFsync:
    def __init__(self) -> None:
        self.raised = False

    def __call__(self, point: str) -> None:
        if point == "after_wal_fsync" and not self.raised:
            self.raised = True
            raise RuntimeError("simulated crash after WAL fsync")


def _plan(*, attempt_id: str = "prospective-outcome-attempt-001") -> ProspectiveCensusPlanV2:
    return ProspectiveCensusPlanV2(
        attempt_id=attempt_id,
        attempt=ProspectiveAttemptV2(
            attempt_index=1,
            qualification_start_ms=QUALIFICATION_START_MS,
            horizon=FixedHorizonV2(h_start_ms=H_START_MS),
        ),
        promoting_plan_sha256="a" * 64,
        symbols=("BTCUSDT",),
        context_symbols=tuple(sorted({"BTCUSDT", *CONTEXT_FILLERS})),
        family_rules=RULES,
        paper_fok_rule_version="paper-fok-v2",
        execution_contract_sha256="b" * 64,
        efficacy_gate_contract_sha256="e" * 64,
        strategy_code_freeze_manifest_sha256="c" * 64,
        created_at_ms=H_START_MS - 1,
    )


def _policy(
    sync_ms: int,
    record_cap: int,
    *,
    max_unsynced_bytes: int = 512 * 1_024,
) -> WalSyncPolicyV2:
    return WalSyncPolicyV2(
        qualification_id=QUALIFICATION_ID,
        fsync_candidate_id=wal_candidate_id_v2(sync_ms=sync_ms, record_cap=record_cap),
        interval_ms=sync_ms,
        max_unsynced_records=record_cap,
        max_unsynced_bytes=max_unsynced_bytes,
        max_record_bytes=70 * 1_024,
        max_segment_bytes=2 * 1_024 * 1_024,
    )


def _metrics(*, passed: bool) -> WalCandidateMetricsV2:
    return WalCandidateMetricsV2(
        unresolved_overflow_or_drop_count=0 if passed else 1,
        p99_queue_fraction_ppm=500_000,
        maximum_queue_fraction_ppm=750_000,
        p99_enqueue_latency_ns=10_000_000,
        maximum_enqueue_latency_ns=100_000_000,
        p99_cpu_fraction_ppm=700_000,
        maximum_cpu_fraction_ppm=850_000,
        p99_fsync_latency_ns=100_000_000,
        maximum_fsync_latency_ns=500_000_000,
        service_rate_over_p99_ingress_ppm=2_000_000,
        service_rate_over_peak_1s_ingress_ppm=1_250_000,
        crash_replay_root_equality=True,
    )


def _selection_receipt(*, max_unsynced_bytes: int = 512 * 1_024):
    selected = (10, 256)
    candidates = tuple(
        WalCandidateQualificationV2(
            policy=_policy(
                sync_ms,
                record_cap,
                max_unsynced_bytes=max_unsynced_bytes,
            ),
            metrics=_metrics(passed=(sync_ms, record_cap) == selected),
            measurement_root_sha256=hashlib.sha256(
                f"{sync_ms}:{record_cap}:{max_unsynced_bytes}".encode()
            ).hexdigest(),
        )
        for sync_ms in WAL_SYNC_CANDIDATES_MS_V2
        for record_cap in WAL_RECORD_CAP_CANDIDATES_V2
    )
    qualification = WalQualificationRunV2(
        qualification_id=QUALIFICATION_ID,
        window_start_wall_ms=WINDOW_START_MS,
        window_end_wall_ms=WINDOW_END_MS,
        actual_final_panel_sha256="1" * 64,
        final_codec_sha256="2" * 64,
        source_manifest_sha256="3" * 64,
        runtime_manifest_sha256="4" * 64,
        independent_failure_domain_evidence_sha256="5" * 64,
        actual_final_panel_passed=True,
        final_codec_passed=True,
        independent_failure_domains_passed=True,
        engineering_only=True,
        strategy_or_outcome_data_accessed=False,
        candidates=candidates,
    )
    return select_wal_candidate_v2(
        qualification,
        selection_wall_ms=WINDOW_END_MS,
        h_start_wall_ms=H_START_MS,
    )


def _paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    scope = tmp_path / "scope"
    primary = scope / "outcome-primary"
    mirror = scope / "outcome-mirror"
    primary.mkdir(parents=True)
    mirror.mkdir()
    return scope, primary, mirror


def _config(
    plan: ProspectiveCensusPlanV2,
    primary: Path,
    mirror: Path,
    *,
    maximum_batch_records: int = 16,
    maximum_records: int = 100,
    maximum_active_outcomes: int = 10,
    max_unsynced_bytes: int = 512 * 1_024,
) -> ProspectiveOutcomeWalStoreConfigV2:
    receipt = _selection_receipt(max_unsynced_bytes=max_unsynced_bytes)
    policy = receipt.selected_policy
    assert policy is not None
    return ProspectiveOutcomeWalStoreConfigV2(
        attempt_plan_sha256=plan.plan_sha256,
        primary_directory=primary,
        mirror_directory=mirror,
        policy=policy,
        selection_receipt=receipt,
        protocol_sha256=plan.execution_contract_sha256,
        source_manifest_sha256=plan.strategy_code_freeze_manifest_sha256,
        schema_sha256="d" * 64,
        runtime_manifest_sha256=receipt.qualification.runtime_manifest_sha256,
        primary_maximum_total_bytes=MAXIMUM_BYTES,
        mirror_maximum_total_bytes=MAXIMUM_BYTES,
        primary_emergency_reserve_bytes=RESERVE_BYTES,
        mirror_emergency_reserve_bytes=RESERVE_BYTES,
        primary_failure_domain_id="declared-outcome-primary-device",
        mirror_failure_domain_id="declared-outcome-mirror-device",
        maximum_batch_records=maximum_batch_records,
        maximum_records=maximum_records,
        maximum_active_outcomes=maximum_active_outcomes,
    )


def _factory(
    config: ProspectiveOutcomeWalStoreConfigV2,
    *,
    primary_fault_hook: _RaiseOnceAfterWalFsync | None = None,
) -> ProspectiveOutcomeWalStoreFactoryV2:
    return ProspectiveOutcomeWalStoreFactoryV2(
        config=config,
        clock_ns=_ConstantClock(),
        primary_fault_hook=primary_fault_hook,
    )


def _claim_lease(lease: WriterLease, plan: ProspectiveCensusPlanV2) -> None:
    with lease.operation_guard():
        lease.claim_prospective_attempt_authority(
            attempt_plan_sha256=plan.plan_sha256
        )


def _cell(plan: ProspectiveCensusPlanV2, *, offset: int = 0):
    return plan.expected_cell(
        family=PromotingFamilyV2.A,
        symbol="BTCUSDT",
        bar_open_ms=H_START_MS + offset * FIVE_MINUTE_MS_V2,
    )


def _item(
    plan: ProspectiveCensusPlanV2,
    kind: ProspectiveOutcomeWalRecordKindV2,
    *,
    offset: int = 0,
    marker: str | None = None,
    sizing_cell: PaperSizingCellV2 = PaperSizingCellV2.NOTIONAL_100_USDT,
) -> ProspectiveOutcomeWalAppendItemV2:
    schema = SCHEMA_BY_KIND[kind]
    return ProspectiveOutcomeWalAppendItemV2(
        origin_cell=_cell(plan, offset=offset),
        sizing_cell=sizing_cell,
        kind=kind,
        canonical_payload_jsonl=canonical_json_line(
            {
                "marker": kind.value if marker is None else marker,
                "production_order_placement": False,
                "schema_version": schema,
            }
        ),
    )


def _claim_owner(store, plan, lease, owner, snapshot=None):
    with lease.operation_guard():
        return store._claim_position_lifecycle_owner_v2(  # pyright: ignore[reportPrivateUsage]
            census_plan=plan,
            writer_lease=lease,
            lifecycle_owner=owner,
            replay_snapshot=snapshot,
        )


def _cleanup(store, lease: WriterLease) -> None:
    store.abort()
    lease.release()


def test_complete_structural_lifecycle_is_durable_bounded_and_nonpromoting(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    config = _config(plan, primary, mirror)
    lease = WriterLease.acquire(scope)
    _claim_lease(lease, plan)
    store = _factory(config).open(census_plan=plan, writer_lease=lease)
    owner = object()
    claim = _claim_owner(store, plan, lease, owner)
    kinds = (
        ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE,
        ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION,
        ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW,
        ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_PREPARE,
        ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_DISPOSITION,
        ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW,
        ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_PREPARE,
        ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_DISPOSITION,
        ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL,
    )

    receipt = store.append_batch_and_sync(
        tuple(
            _item(plan, kind, marker=f"{index}:{kind.value}")
            for index, kind in enumerate(kinds)
        ),
        lifecycle_claim=claim,
    )
    snapshot = store.replay_snapshot_v2(lifecycle_claim=claim)

    assert receipt.first_ingest_seq == 1
    assert receipt.last_ingest_seq == len(kinds)
    assert receipt.durable_prefix_proof.durable_ack_seq == len(kinds)
    assert receipt.typed_payload_semantics_authoritative is False
    assert receipt.efficacy_eligible is False
    assert receipt.production_order_placement is False
    assert snapshot.active_outcome_count == 0
    assert snapshot.terminal_outcome_count == 1
    assert snapshot.outcomes[0].phase is ProspectiveOutcomeLifecyclePhaseV2.TERMINAL
    assert snapshot.outcomes[0].cashflow_count == 2
    assert snapshot.outcomes[0].completed_exit_pair_count == 2
    assert snapshot.outcomes[0].record_count == len(kinds)
    assert store.record_count == len(kinds)
    assert store.active_outcome_count == 0
    assert store.typed_payload_semantics_authoritative is False
    assert store.efficacy_eligible is False
    assert store.production_order_placement is False
    assert store.manifest.attempt_plan_sha256 == plan.plan_sha256
    assert store.manifest.selection_receipt_sha256 == config.selection_receipt.sha256
    assert store.manifest.policy_sha256 == hashlib.sha256(
        canonical_json_line(asdict(config.policy))
    ).hexdigest()
    assert store.manifest.source_manifest_sha256 == plan.strategy_code_freeze_manifest_sha256
    assert store.manifest.runtime_manifest_sha256 == (
        config.selection_receipt.qualification.runtime_manifest_sha256
    )
    assert (scope / PROSPECTIVE_OUTCOME_WAL_STORE_MANIFEST_FILE_V2).is_file()
    store.close()
    lease.release()


@pytest.mark.parametrize(
    "first_kind",
    tuple(
        kind
        for kind in ProspectiveOutcomeWalRecordKindV2
        if kind is not ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE
    ),
)
def test_lifecycle_requires_exact_open_prepare_genesis(
    tmp_path: Path,
    first_kind: ProspectiveOutcomeWalRecordKindV2,
) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    lease = WriterLease.acquire(scope)
    _claim_lease(lease, plan)
    store = _factory(_config(plan, primary, mirror)).open(
        census_plan=plan,
        writer_lease=lease,
    )
    claim = _claim_owner(store, plan, lease, object())

    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="begin"):
        store.append_and_sync(
            item=_item(plan, first_kind),
            lifecycle_claim=claim,
        )

    assert store.record_count == 0
    store.append_and_sync(
        item=_item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE),
        lifecycle_claim=claim,
    )
    _cleanup(store, lease)


def test_duplicate_conflicting_and_post_terminal_operations_fail_without_cursor_mutation(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    lease = WriterLease.acquire(scope)
    _claim_lease(lease, plan)
    store = _factory(_config(plan, primary, mirror)).open(
        census_plan=plan,
        writer_lease=lease,
    )
    claim = _claim_owner(store, plan, lease, object())
    prepare = _item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE)
    store.append_and_sync(item=prepare, lifecycle_claim=claim)

    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="duplicate"):
        store.append_and_sync(item=prepare, lifecycle_claim=claim)
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="violates"):
        store.append_and_sync(
            item=_item(plan, ProspectiveOutcomeWalRecordKindV2.FAMILY_EXIT_DISPOSITION),
            lifecycle_claim=claim,
        )
    assert store.record_count == 1
    receipt = store.append_batch_and_sync(
        (
            _item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION),
            _item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL),
        ),
        lifecycle_claim=claim,
    )
    assert receipt.first_ingest_seq == 2
    assert receipt.last_ingest_seq == 3
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="post-terminal"):
        store.append_and_sync(
            item=_item(
                plan,
                ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW,
                marker="after-terminal",
            ),
            lifecycle_claim=claim,
        )
    assert store.record_count == 3
    _cleanup(store, lease)


def test_active_outcome_and_total_record_bounds_are_exact(tmp_path: Path) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    lease = WriterLease.acquire(scope)
    _claim_lease(lease, plan)
    store = _factory(
        _config(
            plan,
            primary,
            mirror,
            maximum_active_outcomes=1,
            maximum_records=4,
        )
    ).open(census_plan=plan, writer_lease=lease)
    claim = _claim_owner(store, plan, lease, object())
    store.append_batch_and_sync(
        (
            _item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE),
            _item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION),
        ),
        lifecycle_claim=claim,
    )
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="active"):
        store.append_and_sync(
            item=_item(
                plan,
                ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE,
                offset=1,
            ),
            lifecycle_claim=claim,
        )
    store.append_and_sync(
        item=_item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL),
        lifecycle_claim=claim,
    )
    store.append_and_sync(
        item=_item(
            plan,
            ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE,
            offset=1,
        ),
        lifecycle_claim=claim,
    )
    assert store.record_count == 4
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="maximum_records"):
        store.append_and_sync(
            item=_item(
                plan,
                ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION,
                offset=1,
            ),
            lifecycle_claim=claim,
        )
    _cleanup(store, lease)


def test_batch_record_and_selected_byte_bounds_reject_before_wal_mutation(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    lease = WriterLease.acquire(scope)
    _claim_lease(lease, plan)
    store = _factory(
        _config(
            plan,
            primary,
            mirror,
            maximum_batch_records=2,
            max_unsynced_bytes=70 * 1_024,
        )
    ).open(census_plan=plan, writer_lease=lease)
    claim = _claim_owner(store, plan, lease, object())
    three = tuple(
        _item(
            plan,
            ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE,
            offset=index,
            marker=str(index),
        )
        for index in range(3)
    )
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="batch"):
        store.append_batch_and_sync(three, lifecycle_claim=claim)
    large = tuple(
        _item(
            plan,
            ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE,
            offset=index,
            marker=f"{index}:" + "x" * 39_000,
        )
        for index in range(2)
    )
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="byte bound"):
        store.append_batch_and_sync(large, lifecycle_claim=claim)
    assert store.record_count == 0
    _cleanup(store, lease)


def test_append_requires_exact_irreversible_owner_claim(tmp_path: Path) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    lease = WriterLease.acquire(scope)
    _claim_lease(lease, plan)
    store = _factory(_config(plan, primary, mirror)).open(
        census_plan=plan,
        writer_lease=lease,
    )
    item = _item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE)
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="claim"):
        store.append_and_sync(item=item, lifecycle_claim=object())
    owner = object()
    claim = _claim_owner(store, plan, lease, owner)
    with lease.operation_guard(), pytest.raises(
        ProspectiveOutcomeWalStoreContractErrorV2,
        match="already",
    ):
        store._claim_position_lifecycle_owner_v2(  # pyright: ignore[reportPrivateUsage]
            census_plan=plan,
            writer_lease=lease,
            lifecycle_owner=owner,
        )
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="claim"):
        store.append_and_sync(item=item, lifecycle_claim=object())
    store.append_and_sync(item=item, lifecycle_claim=claim)
    _cleanup(store, lease)


def test_clean_restart_requires_verified_snapshot_and_same_recovery_owner(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    config = _config(plan, primary, mirror)
    factory = _factory(config)
    first_lease = WriterLease.acquire(scope)
    _claim_lease(first_lease, plan)
    first = factory.open(census_plan=plan, writer_lease=first_lease)
    first_claim = _claim_owner(first, plan, first_lease, object())
    first.append_batch_and_sync(
        (
            _item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE),
            _item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION),
            _item(
                plan,
                ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW,
                marker="funding-1",
            ),
        ),
        lifecycle_claim=first_claim,
    )
    first.close()
    first_lease.release()

    second_lease = WriterLease.acquire(scope)
    _claim_lease(second_lease, plan)
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="reopening"):
        factory.open(census_plan=plan, writer_lease=second_lease)
    verified = factory.verify_replay_snapshot_v2(
        census_plan=plan,
        writer_lease=second_lease,
    )
    recovery_owner = object()
    resumed = factory.open(
        census_plan=plan,
        writer_lease=second_lease,
        replay_snapshot=verified,
        recovered_state_owner=recovery_owner,
    )
    with second_lease.operation_guard(), pytest.raises(
        ProspectiveOutcomeWalStoreContractErrorV2,
        match="recovery owner",
    ):
        resumed._claim_position_lifecycle_owner_v2(  # pyright: ignore[reportPrivateUsage]
            census_plan=plan,
            writer_lease=second_lease,
            lifecycle_owner=object(),
            replay_snapshot=verified,
        )
    resumed_claim = _claim_owner(
        resumed,
        plan,
        second_lease,
        recovery_owner,
        verified,
    )
    receipt = resumed.append_and_sync(
        item=_item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_TERMINAL),
        lifecycle_claim=resumed_claim,
    )
    assert receipt.first_ingest_seq == 4
    assert receipt.last_ingest_seq == 4
    assert resumed.record_count == 4
    assert resumed.active_outcome_count == 0
    resumed.close()
    second_lease.release()


def test_stale_replay_snapshot_is_rejected_after_prefix_advances(tmp_path: Path) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    factory = _factory(_config(plan, primary, mirror))
    lease = WriterLease.acquire(scope)
    _claim_lease(lease, plan)
    store = factory.open(census_plan=plan, writer_lease=lease)
    claim = _claim_owner(store, plan, lease, object())
    store.append_batch_and_sync(
        (
            _item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE),
            _item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_DISPOSITION),
        ),
        lifecycle_claim=claim,
    )
    store.close()
    lease.release()

    lease2 = WriterLease.acquire(scope)
    _claim_lease(lease2, plan)
    stale = factory.verify_replay_snapshot_v2(census_plan=plan, writer_lease=lease2)
    owner2 = object()
    resumed = factory.open(
        census_plan=plan,
        writer_lease=lease2,
        replay_snapshot=stale,
        recovered_state_owner=owner2,
    )
    claim2 = _claim_owner(resumed, plan, lease2, owner2, stale)
    resumed.append_and_sync(
        item=_item(
            plan,
            ProspectiveOutcomeWalRecordKindV2.POSITION_CASHFLOW,
            marker="new-funding",
        ),
        lifecycle_claim=claim2,
    )
    resumed.close()
    lease2.release()

    lease3 = WriterLease.acquire(scope)
    _claim_lease(lease3, plan)
    with pytest.raises(ProspectiveOutcomeWalStoreIntegrityErrorV2, match="differs"):
        factory.open(
            census_plan=plan,
            writer_lease=lease3,
            replay_snapshot=stale,
            recovered_state_owner=object(),
        )
    lease3.release()


def test_fault_after_one_wal_fsync_poison_store_and_never_issues_receipt(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    fault = _RaiseOnceAfterWalFsync()
    lease = WriterLease.acquire(scope)
    _claim_lease(lease, plan)
    store = _factory(
        _config(plan, primary, mirror),
        primary_fault_hook=fault,
    ).open(census_plan=plan, writer_lease=lease)
    claim = _claim_owner(store, plan, lease, object())

    with pytest.raises(RuntimeError, match="simulated crash"):
        store.append_and_sync(
            item=_item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE),
            lifecycle_claim=claim,
        )
    with pytest.raises(ProspectiveOutcomeWalStoreFailedErrorV2, match="failed"):
        store.append_and_sync(
            item=_item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE),
            lifecycle_claim=claim,
        )
    _cleanup(store, lease)


def test_manifest_and_receipt_and_snapshot_are_factory_sealed_and_tamper_evident(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    lease = WriterLease.acquire(scope)
    _claim_lease(lease, plan)
    store = _factory(_config(plan, primary, mirror)).open(
        census_plan=plan,
        writer_lease=lease,
    )
    claim = _claim_owner(store, plan, lease, object())
    receipt = store.append_and_sync(
        item=_item(plan, ProspectiveOutcomeWalRecordKindV2.POSITION_OPEN_PREPARE),
        lifecycle_claim=claim,
    )
    snapshot = store.replay_snapshot_v2(lifecycle_claim=claim)
    encoded_manifest = canonical_prospective_outcome_wal_store_manifest_v2(store.manifest)
    assert parse_prospective_outcome_wal_store_manifest_v2(encoded_manifest) == store.manifest
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="factory-sealed"):
        replace(store.manifest, attempt_id="forged")
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="factory-sealed"):
        replace(receipt, first_ingest_seq=2)
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="factory-sealed"):
        replace(snapshot, active_outcome_count=0)
    tampered = json.loads(encoded_manifest)
    tampered["maximum_records"] += 1
    with pytest.raises(ProspectiveOutcomeWalStoreIntegrityErrorV2, match="self-hash"):
        parse_prospective_outcome_wal_store_manifest_v2(canonical_json_line(tampered))
    _cleanup(store, lease)


def test_exact_lease_claim_and_nonoverlapping_link_free_paths_are_required(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _paths(tmp_path)
    config = _config(plan, primary, mirror)
    unclaimed = WriterLease.acquire(scope)
    with pytest.raises(WriterLeaseProspectiveAttemptClaimError, match="claim"):
        _factory(config).open(census_plan=plan, writer_lease=unclaimed)
    unclaimed.release()

    overlap_scope = tmp_path / "overlap-scope"
    overlap = overlap_scope / "root"
    nested = overlap / "nested"
    nested.mkdir(parents=True)
    overlap_lease = WriterLease.acquire(overlap_scope)
    _claim_lease(overlap_lease, plan)
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="overlap"):
        _factory(_config(plan, overlap, nested)).open(
            census_plan=plan,
            writer_lease=overlap_lease,
        )
    overlap_lease.release()


def test_authoritative_factory_forbids_torn_tail_recovery(tmp_path: Path) -> None:
    plan = _plan()
    _scope, primary, mirror = _paths(tmp_path)
    with pytest.raises(ProspectiveOutcomeWalStoreContractErrorV2, match="recover_torn_tail"):
        ProspectiveOutcomeWalStoreFactoryV2(
            config=_config(plan, primary, mirror),
            recover_torn_tail=True,
        )
