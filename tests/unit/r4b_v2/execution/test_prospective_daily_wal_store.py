from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from signalbot.capture.receipts import ReceiptTimestamp
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
from signalbot.r4b_v2.execution.prospective_daily_wal_store import (
    PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_FILE_V2,
    ProspectiveDailyWalAppendItemV2,
    ProspectiveDailyWalStoreConfigV2,
    ProspectiveDailyWalStoreContractErrorV2,
    ProspectiveDailyWalStoreFactoryV2,
    ProspectiveDailyWalStoreFailedErrorV2,
    ProspectiveDailyWalStoreIntegrityErrorV2,
    build_prospective_daily_wal_shard_plan_v2,
    canonical_prospective_daily_wal_shard_plan_v2,
    canonical_prospective_daily_wal_store_manifest_v2,
    parse_prospective_daily_wal_store_manifest_v2,
)
from signalbot.r4b_v2.execution.prospective_wal_record import (
    CELL_DISPOSITION_PAYLOAD_SCHEMA_V2,
    DECISION_PREPARE_PAYLOAD_SCHEMA_V2,
    PAPER_TERMINAL_PAYLOAD_SCHEMA_V2,
    ProspectiveWalRecordKindV2,
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
QUALIFICATION_ID = "prospective-daily-wal-final-panel-q1"
MAXIMUM_BYTES = 16 * 1024 * 1024
RESERVE_BYTES = 1_024
CONTEXT_FILLERS = tuple(f"C{index:02d}USDT" for index in range(20))
RULES = (
    ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.A, "family-a-v2"),
    ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.B, "family-b-v2"),
    ProspectiveFamilyRuleBindingV2(PromotingFamilyV2.C, "family-c-v2"),
)


class _ConstantClock:
    def __call__(self) -> int:
        return 1_000_000_000


class _ReceiptClock:
    def __init__(
        self,
        wall_ms: int = H_START_MS - 1,
        monotonic_ns: int = 900_000_000,
    ) -> None:
        self.wall_ms = wall_ms
        self.monotonic_ns = monotonic_ns
        self.capture_count = 0

    def capture(self) -> ReceiptTimestamp:
        self.capture_count += 1
        return ReceiptTimestamp(
            received_at_ms=self.wall_ms,
            received_monotonic_ns=self.monotonic_ns,
        )


class _FailReceiptClock:
    def capture(self) -> ReceiptTimestamp:
        raise AssertionError("existing manifest must not recapture a receipt")


class _RaiseOnceAfterWalFsync:
    def __init__(self) -> None:
        self.raised = False

    def __call__(self, point: str) -> None:
        if point == "after_wal_fsync" and not self.raised:
            self.raised = True
            raise RuntimeError("simulated crash after WAL fsync")


def _plan(*, attempt_id: str = "prospective-attempt-001") -> ProspectiveCensusPlanV2:
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


def _policy(sync_ms: int, record_cap: int) -> WalSyncPolicyV2:
    return WalSyncPolicyV2(
        qualification_id=QUALIFICATION_ID,
        fsync_candidate_id=wal_candidate_id_v2(
            sync_ms=sync_ms,
            record_cap=record_cap,
        ),
        interval_ms=sync_ms,
        max_unsynced_records=record_cap,
        max_unsynced_bytes=512 * 1024,
        max_record_bytes=70 * 1024,
        max_segment_bytes=2 * 1024 * 1024,
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


def _selection_receipt():
    selected = (10, 256)
    candidates = tuple(
        WalCandidateQualificationV2(
            policy=_policy(sync_ms, record_cap),
            metrics=_metrics(passed=(sync_ms, record_cap) == selected),
            measurement_root_sha256=hashlib.sha256(f"{sync_ms}:{record_cap}".encode()).hexdigest(),
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


def _storage_paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    scope = tmp_path / "scope"
    primary = scope / "primary"
    mirror = scope / "mirror"
    primary.mkdir(parents=True)
    mirror.mkdir()
    return scope, primary, mirror


def _config(
    plan: ProspectiveCensusPlanV2,
    primary: Path,
    mirror: Path,
) -> ProspectiveDailyWalStoreConfigV2:
    receipt = _selection_receipt()
    policy = receipt.selected_policy
    assert policy is not None
    return ProspectiveDailyWalStoreConfigV2(
        attempt_plan_sha256=plan.plan_sha256,
        primary_base_directory=primary,
        mirror_base_directory=mirror,
        policy=policy,
        selection_receipt=receipt,
        protocol_sha256=plan.execution_contract_sha256,
        source_manifest_sha256=plan.strategy_code_freeze_manifest_sha256,
        schema_sha256="d" * 64,
        runtime_manifest_sha256=receipt.qualification.runtime_manifest_sha256,
        primary_maximum_total_bytes_per_shard=MAXIMUM_BYTES,
        mirror_maximum_total_bytes_per_shard=MAXIMUM_BYTES,
        primary_emergency_reserve_bytes_per_shard=RESERVE_BYTES,
        mirror_emergency_reserve_bytes_per_shard=RESERVE_BYTES,
        primary_failure_domain_id="declared-primary-device",
        mirror_failure_domain_id="declared-mirror-device",
        maximum_batch_records=16,
    )


def _factory(
    config: ProspectiveDailyWalStoreConfigV2,
    *,
    primary_fault_hook: _RaiseOnceAfterWalFsync | None = None,
    receipt_clock: _ReceiptClock | _FailReceiptClock | None = None,
) -> ProspectiveDailyWalStoreFactoryV2:
    return ProspectiveDailyWalStoreFactoryV2(
        config=config,
        receipt_clock=_ReceiptClock() if receipt_clock is None else receipt_clock,
        clock_ns=_ConstantClock(),
        primary_fault_hook=primary_fault_hook,
    )


def _cell(
    plan: ProspectiveCensusPlanV2,
    *,
    day_index: int = 0,
    bar_offset: int = 0,
    family: PromotingFamilyV2 = PromotingFamilyV2.A,
):
    return plan.expected_cell(
        family=family,
        symbol="BTCUSDT",
        bar_open_ms=(H_START_MS + day_index * DAY_MS + bar_offset * FIVE_MINUTE_MS_V2),
    )


def _payload(
    kind: ProspectiveWalRecordKindV2,
    *,
    sizing_cell: PaperSizingCellV2 | None = None,
) -> bytes:
    schema = {
        ProspectiveWalRecordKindV2.DECISION_PREPARE: (DECISION_PREPARE_PAYLOAD_SCHEMA_V2),
        ProspectiveWalRecordKindV2.CELL_DISPOSITION: (CELL_DISPOSITION_PAYLOAD_SCHEMA_V2),
        ProspectiveWalRecordKindV2.PAPER_TERMINAL: (PAPER_TERMINAL_PAYLOAD_SCHEMA_V2),
    }[kind]
    document: dict[str, object] = {
        "evidence": "closed-candle-test",
        "schema_version": schema,
    }
    if sizing_cell is not None:
        document["sizing_cell"] = sizing_cell.value
    return canonical_json_line(document)


def _prepare_and_dispose(store, cell) -> None:
    store.append_and_sync(
        cell=cell,
        kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
        canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.DECISION_PREPARE),
    )
    store.append_and_sync(
        cell=cell,
        kind=ProspectiveWalRecordKindV2.CELL_DISPOSITION,
        canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.CELL_DISPOSITION),
    )


def test_shard_plan_binds_attempt_separately_from_exact_wal_authority(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = _config(plan, primary, mirror)
    first = next(plan.iter_segments())
    shard_plan = build_prospective_daily_wal_shard_plan_v2(
        census_plan=plan,
        segment=first,
        segment_index=0,
        config=config,
        scope_directory=scope.resolve(),
        store_manifest_sha256="9" * 64,
    )

    assert shard_plan.attempt_plan_sha256 == plan.plan_sha256
    assert shard_plan.store_manifest_sha256 == "9" * 64
    assert shard_plan.authority.plan_sha256 == shard_plan.shard_plan_sha256
    assert shard_plan.authority.protocol_sha256 == plan.execution_contract_sha256
    assert canonical_prospective_daily_wal_shard_plan_v2(shard_plan).endswith(b"\n")
    with pytest.raises(
        ProspectiveDailyWalStoreContractErrorV2,
        match="segment ID or index",
    ):
        build_prospective_daily_wal_shard_plan_v2(
            census_plan=plan,
            segment=first,
            segment_index=1,
            config=config,
            scope_directory=scope.resolve(),
            store_manifest_sha256="9" * 64,
        )
    with pytest.raises(
        ProspectiveDailyWalStoreContractErrorV2,
        match="attempt plan",
    ):
        build_prospective_daily_wal_shard_plan_v2(
            census_plan=plan,
            segment=first,
            segment_index=0,
            config=replace(config, attempt_plan_sha256="f" * 64),
            scope_directory=scope.resolve(),
            store_manifest_sha256="9" * 64,
        )


def test_daily_sequence_resets_previous_day_routes_and_only_two_shards_stay_open(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    lease = WriterLease.acquire(scope)
    store = _factory(_config(plan, primary, mirror)).open(
        census_plan=plan,
        writer_lease=lease,
    )
    try:
        day_zero = _cell(plan, day_index=0)
        _prepare_and_dispose(store, day_zero)
        day_one_prepare = store.append_and_sync(
            cell=_cell(plan, day_index=1),
            kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
            canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.DECISION_PREPARE),
        )
        assert day_one_prepare.first_ingest_seq == 1
        assert store.active_segment_indices == (0, 1)

        previous_terminal = store.append_and_sync(
            cell=day_zero,
            kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            canonical_payload_jsonl=_payload(
                ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            ),
        )
        assert previous_terminal.first_ingest_seq == 3
        assert store.active_shard_count == 2

        day_two_prepare = store.append_and_sync(
            cell=_cell(plan, day_index=2),
            kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
            canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.DECISION_PREPARE),
        )
        assert day_two_prepare.first_ingest_seq == 1
        assert store.active_segment_indices == (1, 2)
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="older than the previous",
        ):
            store.append_and_sync(
                cell=_cell(plan, day_index=0, bar_offset=1),
                kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
                canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.DECISION_PREPARE),
            )
        store.close()
    finally:
        if store.active_shard_count:
            store.abort()
        lease.release()


def test_same_lease_cannot_open_two_owners_but_new_lease_resumes(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    factory = _factory(_config(plan, primary, mirror))
    first_lease = WriterLease.acquire(scope)
    first = factory.open(census_plan=plan, writer_lease=first_lease)
    cell = _cell(plan)
    try:
        _prepare_and_dispose(first, cell)
        with pytest.raises(
            WriterLeaseProspectiveAttemptClaimError,
            match="already consumed",
        ):
            factory.open(census_plan=plan, writer_lease=first_lease)
        first.close()
    finally:
        if first.active_shard_count:
            first.abort()
        first_lease.release()

    second_lease = WriterLease.acquire(scope)
    second = factory.open(census_plan=plan, writer_lease=second_lease)
    try:
        receipt = second.append_and_sync(
            cell=cell,
            kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
            sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            canonical_payload_jsonl=_payload(
                ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
            ),
        )
        assert receipt.first_ingest_seq == 3
        second.close()
    finally:
        if second.active_shard_count:
            second.abort()
        second_lease.release()


def test_two_sizing_terminals_are_distinct_and_receipt_is_factory_sealed(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    lease = WriterLease.acquire(scope)
    store = _factory(_config(plan, primary, mirror)).open(
        census_plan=plan,
        writer_lease=lease,
    )
    cell = _cell(plan)
    try:
        _prepare_and_dispose(store, cell)
        sizing_cells = (
            PaperSizingCellV2.NOTIONAL_100_USDT,
            PaperSizingCellV2.NOTIONAL_1000_USDT,
        )
        receipt = store.append_batch_and_sync(
            tuple(
                ProspectiveDailyWalAppendItemV2(
                    cell=cell,
                    kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                    sizing_cell=sizing,
                    canonical_payload_jsonl=_payload(
                        ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                        sizing_cell=sizing,
                    ),
                )
                for sizing in sizing_cells
            )
        )
        assert tuple(item.sizing_cell for item in receipt.records) == sizing_cells
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="duplicate PAPER_TERMINAL",
        ):
            store.append_and_sync(
                cell=cell,
                kind=ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                canonical_payload_jsonl=_payload(
                    ProspectiveWalRecordKindV2.PAPER_TERMINAL,
                    sizing_cell=PaperSizingCellV2.NOTIONAL_100_USDT,
                ),
            )
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="factory-sealed",
        ):
            replace(receipt)
        store.close()
    finally:
        if store.active_shard_count:
            store.abort()
        lease.release()


def test_out_of_plan_and_prepare_disposition_order_are_rejected_without_write(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    lease = WriterLease.acquire(scope)
    store = _factory(_config(plan, primary, mirror)).open(
        census_plan=plan,
        writer_lease=lease,
    )
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="foreign attempt plan",
        ):
            store.append_and_sync(
                cell=_cell(_plan(attempt_id="foreign-attempt")),
                kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
                canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.DECISION_PREPARE),
            )
        cell = _cell(plan)
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="previously durable PREPARE",
        ):
            store.append_and_sync(
                cell=cell,
                kind=ProspectiveWalRecordKindV2.CELL_DISPOSITION,
                canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.CELL_DISPOSITION),
            )
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="previously durable PREPARE",
        ):
            store.append_batch_and_sync(
                (
                    ProspectiveDailyWalAppendItemV2(
                        cell=cell,
                        kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
                        canonical_payload_jsonl=_payload(
                            ProspectiveWalRecordKindV2.DECISION_PREPARE
                        ),
                    ),
                    ProspectiveDailyWalAppendItemV2(
                        cell=cell,
                        kind=ProspectiveWalRecordKindV2.CELL_DISPOSITION,
                        canonical_payload_jsonl=_payload(
                            ProspectiveWalRecordKindV2.CELL_DISPOSITION
                        ),
                    ),
                )
            )
        store.close()
    finally:
        if store.active_shard_count:
            store.abort()
        lease.release()


def test_crash_replay_makes_recovered_orphan_prepare_permanently_blocking(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = _config(plan, primary, mirror)
    fault = _RaiseOnceAfterWalFsync()
    first_lease = WriterLease.acquire(scope)
    first = _factory(config, primary_fault_hook=fault).open(
        census_plan=plan,
        writer_lease=first_lease,
    )
    orphan_cell = _cell(plan)
    try:
        with pytest.raises(RuntimeError, match="simulated crash"):
            first.append_and_sync(
                cell=orphan_cell,
                kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
                canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.DECISION_PREPARE),
            )
        first.abort()
    finally:
        first_lease.release()

    second_lease = WriterLease.acquire(scope)
    second = _factory(config).open(
        census_plan=plan,
        writer_lease=second_lease,
    )
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="orphan PREPARE permanently blocks",
        ):
            second.append_and_sync(
                cell=orphan_cell,
                kind=ProspectiveWalRecordKindV2.CELL_DISPOSITION,
                canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.CELL_DISPOSITION),
            )
        fresh = second.append_and_sync(
            cell=_cell(plan, bar_offset=1),
            kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
            canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.DECISION_PREPARE),
        )
        assert fresh.first_ingest_seq == 2
        second.close()
    finally:
        if second.active_shard_count:
            second.abort()
        second_lease.release()


def test_protocol_mismatch_and_overlapping_bases_fail_before_lease_claim(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = _config(plan, primary, mirror)
    lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="execution contract",
        ):
            _factory(replace(config, protocol_sha256="f" * 64)).open(
                census_plan=plan,
                writer_lease=lease,
            )
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="must not overlap",
        ):
            _factory(replace(config, mirror_base_directory=primary)).open(
                census_plan=plan,
                writer_lease=lease,
            )
        store = _factory(config).open(census_plan=plan, writer_lease=lease)
        store.close()
    finally:
        lease.release()


def test_mutated_census_plan_fails_self_hash_before_lease_claim(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    factory = _factory(_config(plan, primary, mirror))
    object.__setattr__(plan, "symbols", ("ETHUSDT",))
    lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="canonical self-hash",
        ):
            factory.open(census_plan=plan, writer_lease=lease)
    finally:
        lease.release()


def test_lazy_shard_open_failure_is_sticky_and_does_not_advance_routing(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = _config(plan, primary, mirror)
    segment = next(plan.iter_segments())
    shard_plan = build_prospective_daily_wal_shard_plan_v2(
        census_plan=plan,
        segment=segment,
        segment_index=0,
        config=config,
        scope_directory=scope.resolve(),
        store_manifest_sha256="9" * 64,
    )
    first_lease = WriterLease.acquire(scope)
    first = _factory(config).open(census_plan=plan, writer_lease=first_lease)
    first.close()
    first_lease.release()

    contaminated = scope / shard_plan.primary_directory_relative_to_scope
    contaminated.mkdir()
    (contaminated / "unexpected-residue").write_bytes(b"not-a-root-binding")

    lease = WriterLease.acquire(scope)
    store = _factory(config).open(census_plan=plan, writer_lease=lease)
    try:
        with pytest.raises(Exception, match="non-empty storage root"):
            store.append_and_sync(
                cell=_cell(plan),
                kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
                canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.DECISION_PREPARE),
            )
        assert store.active_segment_indices == ()
        with pytest.raises(
            ProspectiveDailyWalStoreFailedErrorV2,
            match="failed and must be aborted",
        ):
            store.append_and_sync(
                cell=_cell(plan, bar_offset=1),
                kind=ProspectiveWalRecordKindV2.DECISION_PREPARE,
                canonical_payload_jsonl=_payload(ProspectiveWalRecordKindV2.DECISION_PREPARE),
            )
        store.abort()
    finally:
        lease.release()


def test_attempt_manifest_same_plan_resume_reuses_exact_pre_h_start_receipt(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = _config(plan, primary, mirror)
    receipt_clock = _ReceiptClock(
        wall_ms=H_START_MS - 1,
        monotonic_ns=987_654_321,
    )
    first_lease = WriterLease.acquire(scope)
    first = _factory(config, receipt_clock=receipt_clock).open(
        census_plan=plan,
        writer_lease=first_lease,
    )
    manifest = first.store_manifest
    assert receipt_clock.capture_count == 1
    assert manifest.receipt_wall_ms == H_START_MS - 1
    assert manifest.receipt_monotonic_ns == 987_654_321
    assert canonical_prospective_daily_wal_store_manifest_v2(manifest).endswith(b"\n")
    first.close()
    first_lease.release()

    second_lease = WriterLease.acquire(scope)
    second = _factory(config, receipt_clock=_FailReceiptClock()).open(
        census_plan=plan,
        writer_lease=second_lease,
    )
    try:
        assert second.store_manifest == manifest
        assert second.store_manifest.manifest_sha256 == manifest.manifest_sha256
        second.close()
    finally:
        second_lease.release()


def test_fixed_attempt_manifest_rejects_foreign_plan_on_new_lease(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    first_lease = WriterLease.acquire(scope)
    first = _factory(_config(plan, primary, mirror)).open(
        census_plan=plan,
        writer_lease=first_lease,
    )
    first.close()
    first_lease.release()

    foreign = _plan(attempt_id="prospective-attempt-foreign")
    second_lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreIntegrityErrorV2,
            match="exact attempt or config",
        ):
            _factory(_config(foreign, primary, mirror)).open(
                census_plan=foreign,
                writer_lease=second_lease,
            )
    finally:
        second_lease.release()


@pytest.mark.parametrize("drift", ["schema", "path", "quota"])
def test_attempt_manifest_rejects_config_drift_across_lease_acquisitions(
    tmp_path: Path,
    drift: str,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = _config(plan, primary, mirror)
    first_lease = WriterLease.acquire(scope)
    first = _factory(config).open(census_plan=plan, writer_lease=first_lease)
    first.close()
    first_lease.release()

    if drift == "schema":
        changed = replace(config, schema_sha256="f" * 64)
    elif drift == "quota":
        changed = replace(
            config,
            primary_maximum_total_bytes_per_shard=(
                config.primary_maximum_total_bytes_per_shard + 1
            ),
        )
    else:
        other_mirror = scope / "other-mirror"
        other_mirror.mkdir()
        changed = replace(config, mirror_base_directory=other_mirror)

    second_lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreIntegrityErrorV2,
            match="exact attempt or config",
        ):
            _factory(changed).open(
                census_plan=plan,
                writer_lease=second_lease,
            )
    finally:
        second_lease.release()


@pytest.mark.parametrize("receipt_wall_ms", [H_START_MS, H_START_MS + 1])
def test_first_attempt_manifest_creation_at_or_after_h_start_fails(
    tmp_path: Path,
    receipt_wall_ms: int,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="during qualification and before H_start",
        ):
            _factory(
                _config(plan, primary, mirror),
                receipt_clock=_ReceiptClock(wall_ms=receipt_wall_ms),
            ).open(census_plan=plan, writer_lease=lease)
        final_path = scope / PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_FILE_V2
        assert not final_path.exists()
        assert not final_path.with_name(final_path.name + ".partial").exists()
    finally:
        lease.release()


def test_canonical_manifest_tamper_is_rejected_on_parse_and_reopen(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = _config(plan, primary, mirror)
    first_lease = WriterLease.acquire(scope)
    first = _factory(config).open(census_plan=plan, writer_lease=first_lease)
    encoded = canonical_prospective_daily_wal_store_manifest_v2(first.store_manifest)
    assert parse_prospective_daily_wal_store_manifest_v2(encoded) == (first.store_manifest)
    first.close()
    first_lease.release()

    document = json.loads(encoded)
    document["schema_sha256"] = "f" * 64
    tampered = canonical_json_line(document)
    with pytest.raises(
        ProspectiveDailyWalStoreIntegrityErrorV2,
        match="stored manifest SHA-256",
    ):
        parse_prospective_daily_wal_store_manifest_v2(tampered)
    manifest_path = scope / PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_FILE_V2
    manifest_path.write_bytes(tampered)

    second_lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreIntegrityErrorV2,
            match="stored manifest SHA-256",
        ):
            _factory(config).open(census_plan=plan, writer_lease=second_lease)
    finally:
        second_lease.release()


def test_exact_one_sided_manifest_partial_recovers_but_drift_does_not(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = _config(plan, primary, mirror)
    first_lease = WriterLease.acquire(scope)
    first = _factory(config).open(census_plan=plan, writer_lease=first_lease)
    manifest = first.store_manifest
    first.close()
    first_lease.release()

    final_path = scope / PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_FILE_V2
    partial_path = final_path.with_name(final_path.name + ".partial")
    final_path.replace(partial_path)
    second_lease = WriterLease.acquire(scope)
    second = _factory(config, receipt_clock=_FailReceiptClock()).open(
        census_plan=plan,
        writer_lease=second_lease,
    )
    assert second.store_manifest == manifest
    assert final_path.is_file()
    assert not partial_path.exists()
    second.close()
    second_lease.release()

    final_path.replace(partial_path)
    third_lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreIntegrityErrorV2,
            match="exact attempt or config",
        ):
            _factory(replace(config, schema_sha256="f" * 64)).open(
                census_plan=plan,
                writer_lease=third_lease,
            )
        assert partial_path.is_file()
        assert not final_path.exists()
    finally:
        third_lease.release()


def test_typed_store_rejects_implicit_tail_recovery_before_consuming_lease_claim(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = replace(
        _config(plan, primary, mirror),
        typed_decision_payloads_required=True,
    )
    lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="forbids implicit torn-tail recovery",
        ):
            _factory(config).open(census_plan=plan, writer_lease=lease)

        # The failed factory validation happens before the acquisition's sole
        # prospective-attempt claim is consumed.
        with lease.operation_guard():
            lease.claim_prospective_attempt_authority(attempt_plan_sha256=plan.plan_sha256)
            lease.assert_prospective_attempt_authority_claim(attempt_plan_sha256=plan.plan_sha256)
    finally:
        lease.release()


def test_exact_manifest_partial_is_recovered_as_resumed_typed_state(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = replace(
        _config(plan, primary, mirror),
        typed_decision_payloads_required=True,
    )
    first_lease = WriterLease.acquire(scope)
    first = ProspectiveDailyWalStoreFactoryV2(
        config=config,
        receipt_clock=_ReceiptClock(),
        clock_ns=_ConstantClock(),
        recover_torn_tail=False,
    ).open(census_plan=plan, writer_lease=first_lease)
    first.close()
    first_lease.release()

    final_path = scope / PROSPECTIVE_DAILY_WAL_STORE_MANIFEST_FILE_V2
    partial_path = final_path.with_name(final_path.name + ".partial")
    final_path.replace(partial_path)

    second_lease = WriterLease.acquire(scope)
    second = ProspectiveDailyWalStoreFactoryV2(
        config=config,
        receipt_clock=_FailReceiptClock(),
        clock_ns=_ConstantClock(),
        recover_torn_tail=False,
    ).open(census_plan=plan, writer_lease=second_lease)
    try:
        assert final_path.is_file()
        assert not partial_path.exists()
        with pytest.raises(
            ProspectiveDailyWalStoreContractErrorV2,
            match="state-recovery owner",
        ):
            with second_lease.operation_guard():
                second._claim_decision_transaction_owner_v2(  # pyright: ignore[reportPrivateUsage]
                    census_plan=plan,
                    writer_lease=second_lease,
                )
    finally:
        second.close()
        second_lease.release()


def test_typed_decision_flag_is_manifest_and_shard_authority_state(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    plain = _config(plan, primary, mirror)
    typed = replace(plain, typed_decision_payloads_required=True)
    segment = next(plan.iter_segments())
    plain_shard = build_prospective_daily_wal_shard_plan_v2(
        census_plan=plan,
        segment=segment,
        segment_index=0,
        config=plain,
        scope_directory=scope.resolve(),
        store_manifest_sha256="9" * 64,
    )
    typed_shard = build_prospective_daily_wal_shard_plan_v2(
        census_plan=plan,
        segment=segment,
        segment_index=0,
        config=typed,
        scope_directory=scope.resolve(),
        store_manifest_sha256="9" * 64,
    )
    assert plain_shard.shard_plan_sha256 != typed_shard.shard_plan_sha256
    assert plain_shard.authority != typed_shard.authority

    first_lease = WriterLease.acquire(scope)
    first = _factory(plain).open(census_plan=plan, writer_lease=first_lease)
    first.close()
    first_lease.release()

    second_lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreIntegrityErrorV2,
            match="exact attempt or config",
        ):
            ProspectiveDailyWalStoreFactoryV2(
                config=typed,
                receipt_clock=_FailReceiptClock(),
                clock_ns=_ConstantClock(),
                recover_torn_tail=False,
            ).open(census_plan=plan, writer_lease=second_lease)
    finally:
        second_lease.release()


def test_manifest_detects_base_directory_replacement_at_same_path(
    tmp_path: Path,
) -> None:
    plan = _plan()
    scope, primary, mirror = _storage_paths(tmp_path)
    config = _config(plan, primary, mirror)
    first_lease = WriterLease.acquire(scope)
    first = _factory(config).open(census_plan=plan, writer_lease=first_lease)
    first.close()
    first_lease.release()

    primary.replace(scope / "retired-primary")
    primary.mkdir()
    second_lease = WriterLease.acquire(scope)
    try:
        with pytest.raises(
            ProspectiveDailyWalStoreIntegrityErrorV2,
            match="exact attempt or config",
        ):
            _factory(config).open(census_plan=plan, writer_lease=second_lease)
    finally:
        second_lease.release()
