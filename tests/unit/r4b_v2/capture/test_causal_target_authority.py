from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from threading import Lock

import pytest

from signalbot.capture.writer_lease import (
    WriterLease,
    WriterLeaseNotHeldError,
)
from signalbot.r4b_v2.alerts.actionability import (
    AlertActionabilityContractErrorV2,
    AlertActionabilityInputV2,
    AlertTransportTimesV2,
    CausalTargetCursorV2,
    PromotingFamilyV2,
    evaluate_authorized_alert_actionability_v2,
)
from signalbot.r4b_v2.capture.blocks import GroupedBlockBuilderV2
from signalbot.r4b_v2.capture.causal_target_authority import (
    CausalTargetAuthorityErrorV2,
    CausalTargetAuthorityOwnerV2,
    CurrentCausalTargetAuthorityUseV2,
    consume_current_causal_target_authority_v2,
)
from signalbot.r4b_v2.capture.causal_target_cursor import (
    CausalTargetCursorSnapshotV2,
    derive_causal_target_cursor_snapshot_v2,
)
from signalbot.r4b_v2.capture.integrity_ledger import CaptureIntegrityLedgerV2
from signalbot.r4b_v2.capture.models import VenueV2
from signalbot.r4b_v2.execution.paper_fok import (
    PaperFokContractErrorV2,
    PaperFokEntryInputV2,
    evaluate_authorized_paper_fok_entry_v2,
)

from ..execution import test_paper_fok as paper_fok_testkit
from . import test_causal_target_cursor as cursor_testkit

_DECISION_CUTOFF_MS = cursor_testkit._BASE_WALL_MS + 2_000


@dataclass(slots=True)
class _AuthorityFixture:
    source: cursor_testkit._Fixture
    lease: WriterLease
    ledger: CaptureIntegrityLedgerV2
    snapshot: CausalTargetCursorSnapshotV2
    owner: CausalTargetAuthorityOwnerV2

    def close(self) -> None:
        try:
            self.lease.release()
        except WriterLeaseNotHeldError:
            pass


@pytest.fixture
def authority_fixture(tmp_path: Path):  # type: ignore[no-untyped-def]
    plans = cursor_testkit._plans()
    source = cursor_testkit._fixture(
        tmp_path,
        (
            cursor_testkit._clock_record(
                plans,
                ingest_seq=1,
                elapsed_ms=0,
                server_time_ms=cursor_testkit._BASE_WALL_MS,
            ),
            cursor_testkit._plain_record(ingest_seq=2, elapsed_ms=12_013),
            cursor_testkit._plain_record(ingest_seq=3, elapsed_ms=12_014),
        ),
    )
    lease = WriterLease.acquire(tmp_path)
    ledger = CaptureIntegrityLedgerV2(
        tmp_path / "ledger",
        authority=source.writer.authority,
        block_directory=source.writer.directory,
        block_root_binding=source.writer.root_binding,
        block_signing_authority=source.writer.signing_authority,
        block_policy=source.writer.policy,
        block_stream_group_id=source.writer.stream_group_id,
        block_segment_id=source.writer.segment_id,
        maximum_total_bytes=cursor_testkit._MAXIMUM_BYTES,
        emergency_reserve_bytes=cursor_testkit._RESERVE_BYTES,
        max_events=32,
        failure_domain_id="causal-target-cursor-ledger-device",
        writer_lease=lease,
    )
    with lease.operation_guard():
        snapshot = derive_causal_target_cursor_snapshot_v2(
            source.writer,
            integrity_ledger=ledger,
            promoting_plans=source.plans,
            decision_cutoff_ms=_DECISION_CUTOFF_MS,
        )
    owner = CausalTargetAuthorityOwnerV2(
        block_writer=source.writer,
        integrity_ledger=ledger,
        promoting_plans=source.plans,
        writer_lease=lease,
        maximum_authorizations=16,
    )
    fixture = _AuthorityFixture(
        source=source,
        lease=lease,
        ledger=ledger,
        snapshot=snapshot,
        owner=owner,
    )
    try:
        yield fixture
    finally:
        fixture.close()


def _cursor(snapshot: CausalTargetCursorSnapshotV2) -> CausalTargetCursorV2:
    return CausalTargetCursorV2(
        decision_cutoff_ms=snapshot.decision_cutoff_ms,
        target_venue_ms=snapshot.target_venue_ms,
        prior_local_cursor_ms=snapshot.prior_local_cursor_ms,
        prior_venue_lower_bound_ms=snapshot.prior_venue_lower_bound_ms,
        target_local_cursor_ms=snapshot.target_local_cursor_ms,
        target_venue_lower_bound_ms=snapshot.target_venue_lower_bound_ms,
        clock_segment_root_sha256=snapshot.clock_segment_root_sha256,
        contiguous_cursor_evidence=True,
    )


def _actionability_input(
    snapshot: CausalTargetCursorSnapshotV2,
    *,
    cursor: CausalTargetCursorV2 | None = None,
) -> AlertActionabilityInputV2:
    selected_cursor = _cursor(snapshot) if cursor is None else cursor
    target_local_ms = selected_cursor.target_local_cursor_ms
    return AlertActionabilityInputV2(
        attempt_id="attempt-current-causal-target",
        signal_event_id="1" * 64,
        symbol="BTCUSDT",
        venue=VenueV2.USDM_FUTURES,
        family=PromotingFamilyV2.A,
        promoting_plan_sha256=snapshot.promoting_plan_sha256,
        target_cursor=selected_cursor,
        finalized_through_ms=target_local_ms + 2,
        transport=AlertTransportTimesV2(
            durable_outbox_enqueue_ms=snapshot.decision_cutoff_ms,
            send_start_ms=snapshot.decision_cutoff_ms + 1,
            response_first_byte_ms=target_local_ms - 2,
            provider_acceptance_completion_ms=target_local_ms,
            request_completion_ms=target_local_ms + 1,
            observable_delivery_or_ack_ms=target_local_ms + 2,
        ),
    )


def _paper_input(snapshot: CausalTargetCursorSnapshotV2) -> PaperFokEntryInputV2:
    item = paper_fok_testkit._item()
    plan_sha256 = snapshot.promoting_plan_sha256
    return replace(
        item,
        lineage=replace(item.lineage, promoting_plan_sha256=plan_sha256),
        bar_open_ms=snapshot.decision_cutoff_ms - 302_000,
        bar_close_ms=snapshot.decision_cutoff_ms - 2_001,
        decision_cutoff_ms=snapshot.decision_cutoff_ms,
        target_cursor=_cursor(snapshot),
        snapshot=replace(item.snapshot, promoting_plan_sha256=plan_sha256),
        pre_target_depth_events=tuple(
            replace(event, promoting_plan_sha256=plan_sha256)
            for event in item.pre_target_depth_events
        ),
        mark=replace(item.mark, promoting_plan_sha256=plan_sha256),
        exchange_info=replace(
            item.exchange_info,
            promoting_plan_sha256=plan_sha256,
        ),
    )


def test_owner_reverifies_and_authorized_paper_and_actionability_seams_consume_once(
    authority_fixture: _AuthorityFixture,
) -> None:
    fixture = authority_fixture
    paper_item = _paper_input(fixture.snapshot)
    paper_decision = fixture.owner.with_current_authority(
        fixture.snapshot,
        consume=lambda current: evaluate_authorized_paper_fok_entry_v2(
            paper_item,
            current_target_authority=current,
        ),
    )
    alert_item = _actionability_input(fixture.snapshot)
    alert_record = fixture.owner.with_current_authority(
        fixture.snapshot,
        consume=lambda current: evaluate_authorized_alert_actionability_v2(
            alert_item,
            current_target_authority=current,
        ),
    )

    assert paper_decision.target_cursor == _cursor(fixture.snapshot)
    assert not paper_decision.production_order_placement
    assert alert_record.target_cursor == _cursor(fixture.snapshot)
    assert not alert_record.changes_paper_execution
    assert fixture.owner.authorization_count == 2


def test_runtime_seams_reject_direct_legacy_cursor(
    authority_fixture: _AuthorityFixture,
) -> None:
    fixture = authority_fixture
    cursor = _cursor(fixture.snapshot)
    with pytest.raises(PaperFokContractErrorV2, match="direct CausalTargetCursorV2"):
        evaluate_authorized_paper_fok_entry_v2(
            _paper_input(fixture.snapshot),
            current_target_authority=cursor,
        )
    with pytest.raises(
        AlertActionabilityContractErrorV2,
        match="direct CausalTargetCursorV2",
    ):
        evaluate_authorized_alert_actionability_v2(
            _actionability_input(fixture.snapshot),
            current_target_authority=cursor,
        )
    with pytest.raises(TypeError, match="direct CausalTargetCursorV2"):
        consume_current_causal_target_authority_v2(cursor)  # type: ignore[arg-type]


def test_capability_is_one_use_and_revoked_after_callback(
    authority_fixture: _AuthorityFixture,
) -> None:
    fixture = authority_fixture
    captured: list[CurrentCausalTargetAuthorityUseV2] = []

    def consume(current: CurrentCausalTargetAuthorityUseV2) -> CausalTargetCursorV2:
        captured.append(current)
        assert current.paper_input_authorized
        cursor = consume_current_causal_target_authority_v2(current)
        assert not current.paper_input_authorized
        with pytest.raises(CausalTargetAuthorityErrorV2, match="already been consumed"):
            consume_current_causal_target_authority_v2(current)
        return cursor

    assert fixture.owner.with_current_authority(
        fixture.snapshot,
        consume=consume,
    ) == _cursor(fixture.snapshot)
    assert len(captured) == 1
    assert not captured[0].active and captured[0].consumed
    with pytest.raises(CausalTargetAuthorityErrorV2, match="revoked"):
        consume_current_causal_target_authority_v2(captured[0])

    with pytest.raises(CausalTargetAuthorityErrorV2, match="not consumed"):
        fixture.owner.with_current_authority(
            fixture.snapshot,
            consume=lambda current: current.capability_id,
        )


def test_capability_factory_seal_and_owner_capacity_boundary(
    authority_fixture: _AuthorityFixture,
) -> None:
    fixture = authority_fixture
    with pytest.raises(CausalTargetAuthorityErrorV2, match="factory-sealed"):
        CurrentCausalTargetAuthorityUseV2(
            snapshot=fixture.snapshot,
            cursor=_cursor(fixture.snapshot),
            capability_id="2" * 64,
            _factory_token=object(),
        )
    with pytest.raises(ValueError, match="sealed bound"):
        CausalTargetAuthorityOwnerV2(
            block_writer=fixture.source.writer,
            integrity_ledger=fixture.ledger,
            promoting_plans=fixture.source.plans,
            writer_lease=fixture.lease,
            maximum_authorizations=0,
        )

    bounded = CausalTargetAuthorityOwnerV2(
        block_writer=fixture.source.writer,
        integrity_ledger=fixture.ledger,
        promoting_plans=fixture.source.plans,
        writer_lease=fixture.lease,
        maximum_authorizations=1,
    )
    bounded.with_current_authority(
        fixture.snapshot,
        consume=consume_current_causal_target_authority_v2,
    )
    with pytest.raises(CausalTargetAuthorityErrorV2, match="issuance cap"):
        bounded.with_current_authority(
            fixture.snapshot,
            consume=consume_current_causal_target_authority_v2,
        )


def test_snapshot_tamper_and_released_lease_revoke_authority(
    authority_fixture: _AuthorityFixture,
) -> None:
    fixture = authority_fixture
    object.__setattr__(
        fixture.snapshot,
        "target_venue_lower_bound_ms",
        fixture.snapshot.target_venue_lower_bound_ms + 1,
    )
    with pytest.raises(RuntimeError):
        fixture.owner.with_current_authority(
            fixture.snapshot,
            consume=consume_current_causal_target_authority_v2,
        )

    fresh_snapshot = derive_causal_target_cursor_snapshot_v2(
        fixture.source.writer,
        integrity_ledger=fixture.ledger,
        promoting_plans=fixture.source.plans,
        decision_cutoff_ms=_DECISION_CUTOFF_MS,
    )
    fixture.lease.release()
    with pytest.raises(WriterLeaseNotHeldError):
        fixture.owner.with_current_authority(
            fresh_snapshot,
            consume=consume_current_causal_target_authority_v2,
        )


def test_prefix_extension_is_rejected_instead_of_leaking_future_rows(
    authority_fixture: _AuthorityFixture,
) -> None:
    fixture = authority_fixture
    future = cursor_testkit._clock_record(
        fixture.source.plans,
        ingest_seq=4,
        elapsed_ms=30_000,
        server_time_ms=cursor_testkit._BASE_WALL_MS + 30_000,
        poll_cycle_seq=2,
    )
    builder = GroupedBlockBuilderV2(fixture.source.writer.policy)
    with fixture.lease.operation_guard():
        assert not builder.offer(
            future,
            now_ns=future.record.receipt_monotonic_ns + 2,
        )
        block = builder.flush_tail(now_ns=future.record.receipt_monotonic_ns + 3)
        assert block is not None
        fixture.source.writer.commit(block)

    with pytest.raises(CausalTargetAuthorityErrorV2, match="prefix differs"):
        fixture.owner.with_current_authority(
            fixture.snapshot,
            consume=consume_current_causal_target_authority_v2,
        )


def test_callback_prefix_mutation_discards_the_authorized_result(
    authority_fixture: _AuthorityFixture,
) -> None:
    fixture = authority_fixture
    future = cursor_testkit._clock_record(
        fixture.source.plans,
        ingest_seq=4,
        elapsed_ms=30_000,
        server_time_ms=cursor_testkit._BASE_WALL_MS + 30_000,
        poll_cycle_seq=2,
    )
    builder = GroupedBlockBuilderV2(fixture.source.writer.policy)
    assert not builder.offer(
        future,
        now_ns=future.record.receipt_monotonic_ns + 2,
    )
    block = builder.flush_tail(now_ns=future.record.receipt_monotonic_ns + 3)
    assert block is not None

    def consume(current: CurrentCausalTargetAuthorityUseV2) -> str:
        consume_current_causal_target_authority_v2(current)
        fixture.source.writer.commit(block)
        return current.capability_id

    with pytest.raises(CausalTargetAuthorityErrorV2, match="prefix differs"):
        fixture.owner.with_current_authority(
            fixture.snapshot,
            consume=consume,
        )


def test_owner_serializes_concurrent_authorizations(
    authority_fixture: _AuthorityFixture,
) -> None:
    fixture = authority_fixture
    state_lock = Lock()
    active = 0
    maximum_active = 0

    def invoke() -> str:
        def consume(current: CurrentCausalTargetAuthorityUseV2) -> str:
            nonlocal active, maximum_active
            with state_lock:
                active += 1
                maximum_active = max(maximum_active, active)
            try:
                time.sleep(0.02)
                consume_current_causal_target_authority_v2(current)
                return current.capability_id
            finally:
                with state_lock:
                    active -= 1

        return fixture.owner.with_current_authority(
            fixture.snapshot,
            consume=consume,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        identifiers = tuple(executor.map(lambda _index: invoke(), range(2)))

    assert len(set(identifiers)) == 2
    assert maximum_active == 1
    assert fixture.owner.authorization_count == 2
